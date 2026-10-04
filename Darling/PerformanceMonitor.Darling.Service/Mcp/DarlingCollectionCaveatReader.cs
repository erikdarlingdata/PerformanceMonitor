/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The collection caveats <c>get_collection_health</c> carries as <c>collection_caveats</c> (#4843): the data
/// families the scheduled analysis pass currently cannot read on a server, as <c>collect.analysis_collection_caveats</c>
/// (V141) holds them. The same four columns the desktop viewer's "Analysis could not read these data families" grid shows.
/// A different layer from the in-memory <c>analysis_caveats</c> block beside it: that one remembers the last few passes in
/// the serving process, this one is the store's current answer and survives a restart.
/// </summary>
internal static class DarlingCollectionCaveatReader
{
    /// <summary>The payload key. Absent, never null or empty, when the server has no caveat.</summary>
    internal const string PayloadKey = "collection_caveats";

    /// <summary>One server's caveats. $1 server_id. Schema-qualified, so a 42P01 from it can only mean this table is missing.</summary>
    public const string CaveatsSql = """
        SELECT family, reason, first_seen_utc, last_seen_utc
        FROM collect.analysis_collection_caveats
        WHERE server_id = $1
        ORDER BY family
        """;

    internal readonly record struct Caveat(string Family, string Reason, DateTime FirstSeenUtc, DateTime LastSeenUtc);

    /// <summary>
    /// The server's caveats, or none. A store below V141 has no such table, and that is "no caveats" (the pass never
    /// recorded any), so the 42P01 is answered with an empty list; a role that may not read the table (42501) is answered
    /// the same way with one warning. The state code is matched, not the message text: lc_messages is not always English.
    /// Any other failure propagates to the caller, which treats the whole health read as failed.
    /// </summary>
    public static Task<IReadOnlyList<Caveat>> ReadAsync(
        NpgsqlDataSource postgres, int serverId, Microsoft.Extensions.Logging.ILogger? logger, CancellationToken cancellationToken) =>
        ReadAsync(
            async () =>
            {
                var rows = new List<Caveat>();
                await using var command = postgres.CreateCommand(CaveatsSql);
                command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                DarlingMcpReadParameters.AddInt(command, serverId);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    rows.Add(new Caveat(
                        reader.GetString(0),
                        reader.GetString(1),
                        DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                        DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc)));
                }

                return rows;
            },
            DarlingCollectorRunTimeReader.WarnTo(logger));

    /// <summary>The state-code handling of <see cref="ReadAsync(NpgsqlDataSource,int,Microsoft.Extensions.Logging.ILogger?,CancellationToken)"/>,
    /// with the read and the warning passed in so the two refusals are testable without a store.</summary>
    internal static async Task<IReadOnlyList<Caveat>> ReadAsync(Func<Task<List<Caveat>>> read, Action<string> warn)
    {
        try
        {
            return await read();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            return Array.Empty<Caveat>();
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.InsufficientPrivilege)
        {
            warn("get_collection_health: the MCP role cannot read collect.analysis_collection_caveats (42501), so collection caveats are not shown. "
                + "Re-run provision-roles.sql to grant it SELECT.");
            return Array.Empty<Caveat>();
        }
    }

    /// <summary>
    /// Adds <c>collection_caveats</c> to a serialized health payload, and returns the text untouched when there are none.
    /// Spliced into the JSON rather than carried as a property for the reason the ledger's block is one: the MCP serializer
    /// writes nulls, so a property would put an empty key on every clean server's answer.
    /// </summary>
    public static string AttachToJson(string healthJson, IReadOnlyList<Caveat> caveats)
    {
        ArgumentNullException.ThrowIfNull(healthJson);
        if (caveats.Count == 0) return healthJson;

        var node = JsonNode.Parse(healthJson)?.AsObject()
            ?? throw new InvalidOperationException("the health payload did not parse to a JSON object");
        var array = new JsonArray();
        foreach (var c in caveats)
        {
            array.Add(new JsonObject
            {
                ["family"] = c.Family,
                ["reason"] = c.Reason,
                ["first_seen_utc"] = c.FirstSeenUtc.ToString("o"),
                ["last_seen_utc"] = c.LastSeenUtc.ToString("o"),
            });
        }

        node[PayloadKey] = array;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
