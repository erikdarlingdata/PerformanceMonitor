/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #6130: the first sentence of an <c>unavailable</c> answer from a server that has had no recent collection at all.
///
/// <para>An <c>unavailable</c> read tells the caller to check the collector, to confirm pg_stat_statements is installed, to look at
/// get_collection_health. All of that is advice for a server that IS being collected. A server whose last collection was days
/// ago has a simpler explanation that outranks every one of them: nothing is reaching the store. So the answer says that first,
/// with the last collection time.</para>
///
/// <para><b>The rule is <c>list_servers</c>'s, not a second one.</b> The server counts as offline when
/// <see cref="DarlingMcpDataTools.FreshnessStatus"/> says <c>Offline</c> for it: the same read of the newest collection-log row
/// (<see cref="DarlingDataReader.ServerListSql"/>'s per-server probe) through the same ladder, so the answer and
/// <c>list_servers</c> never disagree about which servers are offline.</para>
/// </summary>
internal static class DarlingServerOfflineNote
{
    /// <summary>The <c>list_servers</c> probe for one server: its registration instant and its newest non-cycle collection-log row.</summary>
    internal static readonly string FreshnessSql = $"""
        SELECT
            s.created_date,
            latest.collection_time AS last_collection
        FROM servers s
        LEFT JOIN LATERAL
        (
            SELECT cl.collection_time
            FROM v_collection_log cl
            WHERE cl.server_id = s.server_id
            AND   {CollectionCycleRow.NotCycleRowSql}
            ORDER BY cl.collection_time DESC
            LIMIT 1
        ) AS latest ON TRUE
        WHERE s.server_id = $1
        """;

    /// <summary>
    /// The sentence that opens an <c>unavailable</c> answer for a server that is Offline by <c>list_servers</c>'s rule, or null
    /// when the server is not offline or when the read fails: a diagnostic on a path that already found nothing must not turn
    /// that into an error.
    /// </summary>
    internal static async Task<string?> LeadAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, DateTime nowUtc, CancellationToken cancellationToken,
        DateTime? windowEndUtc = null)
    {
        if (await ReadFreshnessAsync(postgres, serverId, cancellationToken) is not var (registered, last))
        {
            return null;
        }

        /* #6155: a window that ends at or before the newest collection lies inside collected history, so an empty read there is a true
           negative even though the server has gone quiet since. Only a window that reaches into the silence is missing data. */
        if (windowEndUtc is { } windowEnd && last is { } newest && windowEnd <= newest)
        {
            return null;
        }

        return LeadFor(displayName, last, registered, nowUtc);
    }

    /// <summary>
    /// The <see cref="FreshnessSql"/> probe for one server: its registration instant and its newest collection. Null when the server is
    /// not registered or the read fails, so a caller's diagnostic never turns a finding into an error.
    /// </summary>
    internal static async Task<(DateTime? Registered, DateTime? Last)?> ReadFreshnessAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand(FreshnessSql);
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddInt(command, serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return (reader.IsDBNull(0) ? null : reader.GetDateTime(0), reader.IsDBNull(1) ? null : reader.GetDateTime(1));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>The pure half of <see cref="LeadAsync"/>, over an already-read freshness probe.</summary>
    internal static string? LeadFor(string displayName, DateTime? lastCollectionUtc, DateTime? registeredAtUtc, DateTime nowUtc)
    {
        if (DarlingMcpDataTools.FreshnessStatus(lastCollectionUtc, registeredAtUtc, nowUtc) != "Offline")
        {
            return null;
        }

        return lastCollectionUtc is { } last
            ? $"{displayName} is offline: the store has had no collection from it since "
                + $"{last.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture)} UTC (list_servers shows it as Offline), "
                + "so nothing is arriving to read, whatever the collectors would otherwise report."
            : $"{displayName} is offline: the store holds no collection from it within its retained history "
                + "(list_servers shows it as Offline), so nothing is arriving to read, whatever the collectors would otherwise report.";
    }

    /// <summary>
    /// <c>McpHelpers.Status("unavailable", message, hints)</c>, with <see cref="LeadAsync"/>'s sentence in front of the message when
    /// the server is offline.
    /// </summary>
    internal static Task<string> UnavailableAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, string message, object? hints, CancellationToken cancellationToken) =>
        WithLeadAsync(McpHelpers.Status("unavailable", message, hints), postgres, serverId, displayName, cancellationToken);

    /// <summary><see cref="UnavailableAsync(NpgsqlDataSource, int, string, string, object, CancellationToken)"/> for an answer with no hints.</summary>
    internal static Task<string> UnavailableAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, string message, CancellationToken cancellationToken) =>
        UnavailableAsync(postgres, serverId, displayName, message, null, cancellationToken);

    /// <summary>
    /// #6155: <paramref name="answer"/>, when it says <c>empty</c> (the read looked and found nothing) for a server that is Offline by
    /// <c>list_servers</c>'s rule, as an <c>unavailable</c> answer that opens with <see cref="LeadAsync"/>'s sentence. An offline server's
    /// empty read is not a true negative: nothing has been arriving, so "no dead tuples", "no replication slots" or "nothing held the
    /// xmin horizon" would be a healthy all-clear built from missing data. The original answer's own wording is dropped with its word.
    /// Any other answer, any answer for a server that is not offline, and any answer whose window ends at or before the server's newest
    /// collection (it is inside collected history, so its emptiness is a real finding), comes back unchanged.
    /// </summary>
    internal static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, int serverId, string displayName, DateTime windowEndUtc, string answer, CancellationToken cancellationToken)
    {
        if (!IsEmptyAnswer(answer))
        {
            return answer;
        }

        /* The caller's window end is the read's anchor (as_of, or now for a live read), so offline is judged as of it, not as of the
           clock: an as_of read of a past window must not answer as the server is today. */
        var lead = await LeadAsync(postgres, serverId, displayName, windowEndUtc, cancellationToken, windowEndUtc);
        return lead is null ? answer : OfflineInsteadOfEmpty(lead);
    }

    /// <summary>The <c>unavailable</c> answer that replaces an offline server's empty read.</summary>
    internal static string OfflineInsteadOfEmpty(string lead) =>
        McpHelpers.Status("unavailable", lead + " An empty result here is missing data, not a finding.", null);

    /// <summary>True when <paramref name="answer"/> is a JSON object whose <c>status</c> is <c>empty</c>.</summary>
    internal static bool IsEmptyAnswer(string answer)
    {
        try
        {
            return JsonNode.Parse(answer) is JsonObject node
                && node["status"] is JsonValue status && status.TryGetValue<string>(out var word) && word == "empty";
        }
        catch (System.Text.Json.JsonException)
        {
            return false;
        }
    }

    /// <summary>
    /// <paramref name="answer"/>, when it is an <c>unavailable</c> envelope for an offline server, with <see cref="LeadAsync"/>'s
    /// sentence put in front of its <c>message</c>. Any other answer, and any answer for a server that is not offline, comes back unchanged.
    /// </summary>
    internal static async Task<string> WithLeadAsync(
        string answer, NpgsqlDataSource postgres, int serverId, string displayName, CancellationToken cancellationToken)
    {
        JsonObject? node;
        try
        {
            node = JsonNode.Parse(answer) as JsonObject;
        }
        catch (System.Text.Json.JsonException)
        {
            return answer;
        }

        if (node is null
            || node["status"] is not JsonValue status || !status.TryGetValue<string>(out var word) || word != "unavailable"
            || node["message"] is not JsonValue message || !message.TryGetValue<string>(out var text))
        {
            return answer;
        }

        var lead = await LeadAsync(postgres, serverId, displayName, DateTime.UtcNow, cancellationToken);
        if (lead is null)
        {
            return answer;
        }

        node["message"] = lead + " " + text;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }
}
