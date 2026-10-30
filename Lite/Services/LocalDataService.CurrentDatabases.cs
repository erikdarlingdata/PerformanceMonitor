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
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>How old the newest <c>database_states</c> snapshot may be and still decide which databases are gone: six
    /// hours, the twin of Darling's <c>CurrentDatabaseScope.SnapshotHorizon</c>.</summary>
    internal static readonly TimeSpan CurrentDatabasesHorizon = TimeSpan.FromHours(6);

    /// <summary>True when <paramref name="current"/> is known and does not name <paramref name="databaseName"/> (ordinal, the twin of
    /// Darling's <c>CurrentDatabaseScope.IsDropped</c>): unknown filters nothing.</summary>
    internal static bool IsDroppedDatabase(IReadOnlyList<string>? current, string? databaseName)
        => current is not null && databaseName is not null && !current.Contains(databaseName, StringComparer.Ordinal);

    /// <summary>The shared backfill SQL names <c>database_states</c> (the hot table) after each FROM; these reads need the archive
    /// union, so the name becomes <c>v_database_states</c> (#5633 item 4). The match is the name and the WHERE after it, not the
    /// FROM, so this is no bare read: the text that runs always reads the view, which
    /// <c>ArchivableTableBareReadSweepTests</c> would otherwise count as two bare reads here.</summary>
    private static string ThroughArchiveView(string sharedSql)
        => sharedSql.Replace("database_states WHERE", "v_database_states WHERE", StringComparison.Ordinal);

    /// <summary>
    /// #5633 item 4: the databases the server's newest <c>database_states</c> snapshot names, on the same SQL text as the
    /// Query Store backfill's gone-database check (<see cref="QueryStoreBackfillState.NewestSnapshotSql"/>), read from
    /// <c>v_database_states</c> (hot plus archive). Null when that is unknown (no snapshot, as on an Azure SQL Database or a
    /// store that never collected it; one older than <see cref="CurrentDatabasesHorizon"/>; no database names; a failed
    /// read), and every caller then filters nothing. <paramref name="asOfUtc"/> (#5633 review M2) is a historical read's anchor: the
    /// newest snapshot at or before it, with the horizon measured from it, so a database dropped after the window still counts for it.
    /// </summary>
    public async Task<IReadOnlyList<string>?> GetCurrentDatabasesAsync(int serverId, DateTime? nowUtc = null, DateTime? asOfUtc = null)
    {
        try
        {
            using var connection = await OpenConnectionAsync();
            using var command = connection.CreateCommand();
            command.CommandText = ThroughArchiveView(asOfUtc is null ? QueryStoreBackfillState.NewestSnapshotSql : QueryStoreBackfillState.NewestSnapshotAtOrBeforeSql);
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = (asOfUtc ?? nowUtc ?? DateTime.UtcNow) - CurrentDatabasesHorizon });
            if (asOfUtc is { } ceiling)
            {
                command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = ceiling });
            }

            var names = new List<string>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(0))
                {
                    names.Add(reader.GetString(0));
                }
            }

            return names.Count == 0 ? null : names;
        }
        catch (Exception ex)
        {
            /* #5633 review: keep the fail-open (a monitoring read never fails on this lookup), but a SQL or ordinal mistake here turns all of
               item 4 off, so say so, once per occurrence (Darling's CurrentDatabaseScope logs the same failure at Warning). */
            AppLogger.Warn("CurrentDatabases", $"The newest database_states snapshot for server {serverId} failed, so no dropped database is left out of the read that asked: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// #6651 (#5633 item 4): the databases a desktop grid's all-databases ranking over [<paramref name="startUtc"/>, <paramref name="endUtc"/>]
    /// leaves out, the way get_top_procedures_by_cpu does: the ones the server DROPPED. Null when a database is chosen (a named database is
    /// read as asked), when nothing was dropped, or when the set is unknown (no recent snapshot). A window that ended in the past is
    /// anchored at its end, as the MCP tool's as_of read is, so a database dropped since still ranks for a window it existed in.
    /// </summary>
    public async Task<IReadOnlyList<string>?> GetDatabasesLeftOutOfRankingAsync(
        int serverId, IReadOnlyList<string>? chosenDatabases, DateTime startUtc, DateTime endUtc, DateTime? nowUtc = null)
    {
        if (chosenDatabases is { Count: > 0 })
        {
            return null;
        }

        var now = nowUtc ?? DateTime.UtcNow;
        return DatabasesLeftOut.Distinct(
            await GetDroppedDatabasesAsync(serverId, nowUtc: now, asOfUtc: endUtc < now ? endUtc : null, windowStartUtc: startUtc));
    }

    /// <summary>The longest look-back <see cref="GetDroppedDatabasesAsync"/> reaches for an earlier snapshot naming a database, the twin
    /// of Darling's <c>CurrentDatabaseScope.DropLookBack</c>: seven days.</summary>
    internal static readonly TimeSpan DroppedDatabasesLookBack = TimeSpan.FromDays(7);

    /// <summary>The oldest snapshot stamp <see cref="GetDroppedDatabasesAsync"/> reads (the twin of Darling's <c>LookBackFloor</c>): the
    /// window's start when given, never nearer than <see cref="CurrentDatabasesHorizon"/> before the anchor, never further than
    /// <see cref="DroppedDatabasesLookBack"/>.</summary>
    internal static DateTime DroppedDatabasesFloor(DateTime anchorUtc, DateTime? windowStartUtc)
    {
        var floor = anchorUtc - CurrentDatabasesHorizon;
        if (windowStartUtc is { } start && start < floor)
        {
            floor = start;
        }

        var oldest = anchorUtc - DroppedDatabasesLookBack;
        return floor < oldest ? oldest : floor;
    }

    /// <summary>The pure half of <see cref="GetDroppedDatabasesAsync"/> (the twin of Darling's <c>DroppedFrom</c>): the names whose
    /// newest snapshot stamp is older than the newest stamp overall; null when there is no stamp or the newest is not within
    /// <see cref="CurrentDatabasesHorizon"/> of the anchor.</summary>
    internal static IReadOnlyList<string>? DroppedFrom(IReadOnlyDictionary<string, DateTime> lastSeenPerDatabase, DateTime anchorUtc)
    {
        if (lastSeenPerDatabase.Count == 0)
        {
            return null;
        }

        var newest = lastSeenPerDatabase.Values.Max();
        if (newest <= anchorUtc - CurrentDatabasesHorizon)
        {
            return null;
        }

        return lastSeenPerDatabase.Where(pair => pair.Value < newest).Select(pair => pair.Key).OrderBy(name => name, StringComparer.Ordinal).ToList();
    }

    /// <summary>
    /// #5633 review: the databases the server DROPPED, the twin of Darling's <c>CurrentDatabaseScope.ReadDroppedAsync</c>: names an earlier
    /// <c>database_states</c> snapshot lists that the newest does not, so a ranking can leave them out while a database no snapshot ever
    /// lists (created after the newest snapshot) is still read. One grouped read of <c>v_database_states</c>. Null is unknown (no
    /// snapshot, the newest older than the horizon, a failed read); an empty list is "nothing dropped". <paramref name="asOfUtc"/>
    /// anchors a historical read.
    /// </summary>
    public async Task<IReadOnlyList<string>?> GetDroppedDatabasesAsync(int serverId, DateTime? nowUtc = null, DateTime? asOfUtc = null, DateTime? windowStartUtc = null)
    {
        try
        {
            var anchor = asOfUtc ?? nowUtc ?? DateTime.UtcNow;
            using var connection = await OpenConnectionAsync();
            using var command = connection.CreateCommand();
            command.CommandText = ThroughArchiveView(asOfUtc is null ? QueryStoreBackfillState.LastSeenPerDatabaseSql : QueryStoreBackfillState.LastSeenPerDatabaseAtOrBeforeSql);
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = DroppedDatabasesFloor(anchor, windowStartUtc) });
            if (asOfUtc is { } ceiling)
            {
                command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = ceiling });
            }

            var lastSeen = new Dictionary<string, DateTime>(StringComparer.Ordinal);
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(0) && !reader.IsDBNull(1))
                {
                    lastSeen[reader.GetString(0)] = reader.GetDateTime(1);
                }
            }

            return DroppedFrom(lastSeen, anchor);
        }
        catch (Exception ex)
        {
            /* #5633 review: keep the fail-open (a monitoring read never fails on this lookup), but a SQL or ordinal mistake here turns all of
               item 4 off, so say so, once per occurrence (Darling's CurrentDatabaseScope logs the same failure at Warning). */
            AppLogger.Warn("CurrentDatabases", $"The last-seen database_states read for server {serverId} failed, so no dropped database is left out of the read that asked: {ex.Message}");
            return null;
        }
    }
}
