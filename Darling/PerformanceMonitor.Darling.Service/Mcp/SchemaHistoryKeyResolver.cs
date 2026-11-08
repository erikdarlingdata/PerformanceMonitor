/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7643: what a server and a database name mean in the schema history. <see cref="Collected"/> is false when the collector never stored
/// a state row for the server. <see cref="Keys"/> are the history keys to read (one for a database; with no database, every key the server
/// writes to or reads from). <see cref="Database"/> is the name as the history spells it. <see cref="ResolvedFromAg"/> is true when a key
/// is an availability group's, so the history read is the group's and not the server's. <see cref="DatabaseUntracked"/> is true for a
/// named database with no state row and no availability group: no pass ever read it (not writable, excluded, out of scope), so a miss there
/// is not a true negative. <see cref="SessionNotRunning"/> is true when the server row of a finished pass shows no running session.
/// </summary>
internal sealed record SchemaHistoryResolution(bool Collected, IReadOnlyList<SchemaHistoryKey> Keys, string? Database, bool ResolvedFromAg)
{
    public bool DatabaseUntracked { get; init; }

    public bool SessionNotRunning { get; init; }
}

/// <summary>
/// #7643: maps (server, database) to the key the history is stored under. A database of an availability group is stored under the
/// group (<c>ag_group_id</c>), from whichever replica was the primary, so a read through a secondary has to land on the group's rows.
///
/// <para><b>Where the key comes from.</b> First <c>collect.schema_capture_state</c>: the pass stores, per server and database, the key it
/// wrote the history under (<c>history_ag_group_id</c> or <c>history_server_id</c>), a table of a row per database that is read by its
/// server id. It does not hold a database the server only ever held as a secondary (a secondary is never tracked), so the second source is the AG
/// collector's <c>collect.ag_database_replica_states</c>: the group of each database this server holds a replica of, newest sighting
/// within a week. It is the same table <c>get_ag_health</c> reads, and the group id is the one the writer takes from
/// <c>sys.availability_groups</c>. With neither, the key is the server's own.</para>
/// </summary>
internal static class SchemaHistoryKeyResolver
{
    internal static async Task<SchemaHistoryResolution> ResolveAsync(
        NpgsqlConnection connection, int serverId, string? database, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var states = await SchemaCaptureStateStore.LoadAsync(connection, serverId, cancellationToken);
        if (states.Count == 0)
        {
            return new SchemaHistoryResolution(false, [], database, false);
        }

        var databaseStates = states.Where(s => s.DatabaseName.Length > 0).ToList();

        /* A finished pass stamps the server row; with no session position on it, nor on a database row (Azure SQL Database keeps one per
           database), the session was not running when the passes ran. */
        var serverRow = states.FirstOrDefault(s => s.DatabaseName.Length == 0);
        var sessionNotRunning = serverRow is { LastSeenInScopeAt: not null, SessionCreateTime: null }
            && databaseStates.All(s => s.SessionCreateTime is null);
        if (database is not null)
        {
            var row = databaseStates.FirstOrDefault(s => string.Equals(s.DatabaseName, database, StringComparison.OrdinalIgnoreCase));
            if (row is not null)
            {
                var key = KeyOf(serverId, row);
                return new SchemaHistoryResolution(true, [key], row.DatabaseName, key.AgGroupId is not null) { SessionNotRunning = sessionNotRunning };
            }

            var memberships = await SchemaHistoryReader.ReadAgMembershipsAsync(connection, serverId, database, nowUtc, cancellationToken);
            if (memberships.Count > 0)
            {
                return new SchemaHistoryResolution(true, [new SchemaHistoryKey(null, memberships[0].GroupId, memberships[0].AgName)], memberships[0].DatabaseName, true)
                {
                    SessionNotRunning = sessionNotRunning,
                };
            }

            return new SchemaHistoryResolution(true, [new SchemaHistoryKey(serverId, null, null)], database, false) { DatabaseUntracked = true };
        }

        var keys = new List<SchemaHistoryKey> { new(serverId, null, null) };
        foreach (var key in databaseStates.Select(s => KeyOf(serverId, s)).Where(k => k.AgGroupId is not null))
        {
            AddGroup(keys, key.AgGroupId!.Value, key.AgName);
        }

        foreach (var m in await SchemaHistoryReader.ReadAgMembershipsAsync(connection, serverId, null, nowUtc, cancellationToken))
        {
            AddGroup(keys, m.GroupId, m.AgName);
        }

        return new SchemaHistoryResolution(true, keys, null, keys.Any(k => k.AgGroupId is not null)) { SessionNotRunning = sessionNotRunning };
    }

    private static SchemaHistoryKey KeyOf(int serverId, SchemaCaptureStateRow row) =>
        row.HistoryAgGroupId is { } group ? new SchemaHistoryKey(null, group, null) : new SchemaHistoryKey(row.HistoryServerId ?? serverId, null, null);

    private static void AddGroup(List<SchemaHistoryKey> keys, Guid group, string? agName)
    {
        if (keys.All(k => k.AgGroupId != group))
        {
            keys.Add(new SchemaHistoryKey(null, group, agName));
        }
    }
}
