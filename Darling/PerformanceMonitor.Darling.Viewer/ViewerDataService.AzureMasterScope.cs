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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// An Azure SQL Database <c>master</c> registration collects blocked-process reports and deadlocks for every
/// database on its logical server. When a database is also monitored as its own target, the same event would
/// show on both servers' cards and twice in the fleet totals. The Viewer has no worker and no registry
/// snapshot, so it derives the list the service uses (<see cref="AzureMasterScope"/>) from the store.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The monitored targets (identity columns only, never a credential column), one row per target.</summary>
    public const string SeparatelyMonitoredTargetsSql = @"
SELECT
    c.server_id,
    c.host,
    c.database,
    c.is_enabled,
    c.read_only_intent
FROM config_monitored_servers c";

    /// <summary>
    /// Whether server $1 is an Azure SQL Database master by the rule the service uses: <c>servers.sql_engine_edition</c>
    /// is 5 AND the newest stored <c>server_properties.engine_edition</c> is 5 (the service's
    /// <c>StoredEngineEditionSql</c>). A server that is not registered, has no properties row, or is any other
    /// edition (a managed instance is 8) is not one, so the two apps cannot disagree on the same rows. The cheap
    /// <c>servers</c> test comes first, so no other server reads the properties history.
    /// </summary>
    public const string IsAzureMasterSql = @"
SELECT 1
FROM servers s
WHERE s.server_id = $1
  AND s.sql_engine_edition = 5
  AND (SELECT sp.engine_edition FROM server_properties sp WHERE sp.server_id = s.server_id ORDER BY sp.collection_time DESC LIMIT 1) = 5";

    /// <summary>
    /// The enabled servers that are Azure SQL Database masters by the same rule as <see cref="IsAzureMasterSql"/>.
    /// </summary>
    public const string FleetMasterCandidatesSql = @"
SELECT s.server_id
FROM servers s
WHERE s.is_enabled
  AND s.sql_engine_edition = 5
  AND (SELECT sp.engine_edition FROM server_properties sp WHERE sp.server_id = s.server_id ORDER BY sp.collection_time DESC LIMIT 1) = 5";

    /// <summary>
    /// A seam for tests: called with the name of each scope read ("registry", "blocking", "deadlocks", "unscoped")
    /// just before it runs, so a test can make one throw and count how often the registry is read. Null in production.
    /// </summary>
    internal Func<string, Task>? ScopeReadHookForTests { get; set; }

    private Task ScopeReadStageAsync(string stage) => ScopeReadHookForTests?.Invoke(stage) ?? Task.CompletedTask;

    /// <summary>
    /// The note a master tab's Blocking and Deadlocks grids carry for this resolved list: the shared sentence when the
    /// list is non-empty, null (no note) when it is empty, which is every non-master target and a master with no siblings.
    /// </summary>
    public static string? SeparatelyMonitoredListNoteFor(IReadOnlyList<string> separatelyMonitored) =>
        separatelyMonitored.Count > 0 ? AzureMasterScope.SeparatelyMonitoredListNote : null;

    /// <summary>
    /// The databases a master registration should skip because they are monitored as their own targets, or an
    /// empty list (not an Azure SQL Database master, no such sibling, the server is not registered, or the
    /// lookup failed: empty always means unscoped, so a failure shows the unscoped counts and loses no event).
    /// </summary>
    public async Task<IReadOnlyList<string>> GetSeparatelyMonitoredAsync(int serverId, CancellationToken cancellationToken = default)
    {
        try
        {
            await using (var command = _dataSource.CreateCommand(IsAzureMasterSql))
            {
                command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
                if (await command.ExecuteScalarAsync(cancellationToken) is null)
                {
                    return Array.Empty<string>();
                }
            }

            var registry = await ReadScopeRegistryAsync(cancellationToken);
            return SeparatelyMonitoredFrom(serverId, registry);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ViewerLogger.Warn("ViewerDataService", $"Separately monitored lookup failed for server {serverId}; showing unscoped counts: {ex.Message}");
            return Array.Empty<string>();
        }
    }

    /// <summary>The monitored targets, read once; the master's own host and database come from the same rows.</summary>
    private async Task<List<(int Id, string Host, string? Database, bool Enabled, bool ReadOnly)>> ReadScopeRegistryAsync(
        CancellationToken cancellationToken)
    {
        await ScopeReadStageAsync("registry");
        var rows = new List<(int, string, string?, bool, bool)>();
        await using var command = _dataSource.CreateCommand(SeparatelyMonitoredTargetsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                !reader.IsDBNull(4) && reader.GetBoolean(4)));
        }

        return rows;
    }

    private static IReadOnlyList<string> SeparatelyMonitoredFrom(
        int serverId, List<(int Id, string Host, string? Database, bool Enabled, bool ReadOnly)> registry)
    {
        var id = serverId.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var targets = registry
            .Select(r => new AlertTargetIdentity(
                r.Id.ToString(System.Globalization.CultureInfo.InvariantCulture), r.Host, r.Database, r.Enabled, r.ReadOnly))
            .ToList();
        foreach (var r in registry)
        {
            if (r.Id == serverId)
            {
                return AzureMasterScope.SeparatelyMonitoredDatabases(true, id, r.Host, r.Database, targets);
            }
        }

        return Array.Empty<string>();
    }

    /// <summary>
    /// The master's windowed XE blocking count and worst wait without the separately monitored databases'
    /// events (<see cref="PgFactCollector.BlockingSqlSkippingSeparate"/>, the read the service's analysis uses).
    /// </summary>
    private async Task<(int Count, long MaxWaitMs)> ReadScopedBlockingAsync(
        int serverId, DateTime start, DateTime end, IReadOnlyList<string> separate, CancellationToken cancellationToken)
    {
        await ScopeReadStageAsync("blocking");
        await using var command = _dataSource.CreateCommand(PgFactCollector.BlockingSqlSkippingSeparate);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = separate.ToArray() });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(start) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return (0, 0);
        }

        return (
            reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
            reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)));
    }

    /// <summary>The newest blocking report of the master's own databases, over all history like the card's unscoped
    /// "Last" time: <see cref="ReadScopedBlockingAsync"/>'s database rule without the window.</summary>
    private async Task<DateTime?> ReadScopedLastBlockingAsync(
        int serverId, IReadOnlyList<string> separate, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(ScopedLastBlockingSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = separate.ToArray() });
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime t ? t : null;
    }

    private const string ScopedLastBlockingSql = @"
SELECT MAX(event_time)
FROM v_blocked_process_reports
WHERE server_id = $1
AND   (database_name IS NULL OR NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($2::text[]) x)))";

    /// <summary>The newest deadlock of the master's own databases, over all history like the card's unscoped "Last"
    /// time, by the every-process rule <see cref="ReadScopedDeadlocksAsync"/> counts with.</summary>
    private async Task<DateTime?> ReadScopedLastDeadlockAsync(
        int serverId, IReadOnlyList<string> separate, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await PgFactCollector.NewestDeadlockSkippingSeparateAsync(
            connection, serverId, new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc), DateTime.UtcNow.AddDays(1), separate,
            cancellationToken, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
    }

    /// <summary>The master's windowed deadlock count without the deadlocks wholly in the separately monitored databases.</summary>
    private async Task<long> ReadScopedDeadlocksAsync(
        int serverId, DateTime start, DateTime end, IReadOnlyList<string> separate, CancellationToken cancellationToken)
    {
        await ScopeReadStageAsync("deadlocks");
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await PgFactCollector.CountDeadlocksSkippingSeparateAsync(
            connection, PgFactCollector.DeadlockOutsideCountSql, PgFactCollector.DeadlockGraphsSql,
            serverId, start, end, separate, cancellationToken, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
    }

    private const string FleetMasterUnscopedSql = @"
SELECT
    (SELECT COUNT(*) FROM v_blocked_process_reports WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3 AND collection_time >= $4),
    (SELECT COUNT(*) FROM v_dmv_blocking_snapshots   WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3 AND collection_time >= $4),
    (SELECT COUNT(*) FROM v_deadlocks WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3 AND collection_time >= $4)";

    /// <summary>
    /// What the fleet totals over-count for the Azure SQL Database masters that have separately monitored
    /// databases: per master, its unscoped contribution (XE count when it has any XE row, else DMV; plus the
    /// deadlock count) minus its scoped contribution, the figure its own card shows.
    /// </summary>
    private async Task<(long Blocking, long Deadlocks)> ReadFleetMasterOvercountAsync(
        DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var masters = new List<int>();
        List<(int Id, string Host, string? Database, bool Enabled, bool ReadOnly)> registry;
        try
        {
            await using (var command = _dataSource.CreateCommand(FleetMasterCandidatesSql))
            {
                command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    masters.Add(reader.GetInt32(0));
                }
            }

            /* The registry is read once for the whole call, and only when there is a master to scope. */
            if (masters.Count == 0)
            {
                return (0, 0);
            }

            registry = await ReadScopeRegistryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            ViewerLogger.Warn("ViewerDataService", $"Fleet master over-count lookup failed; totals stay unscoped: {ex.Message}");
            return (0, 0);
        }

        long blocking = 0;
        long deadlocks = 0;
        foreach (var id in masters)
        {
            var separate = SeparatelyMonitoredFrom(id, registry);
            if (separate.Count == 0)
            {
                continue;
            }

            /* A master whose scoped reads fail keeps its unscoped contribution (a zero over-count), the same
               fallback its own card takes; the other masters are still scoped. */
            try
            {
                long xe, dmv, dead;
                await ScopeReadStageAsync("unscoped");
                await using (var command = _dataSource.CreateCommand(FleetMasterUnscopedSql))
                {
                    command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
                    command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
                    command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
                    command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
                    command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(startUtc) });
                    await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                    if (!await reader.ReadAsync(cancellationToken))
                    {
                        continue;
                    }

                    xe = Convert.ToInt64(reader.GetValue(0));
                    dmv = Convert.ToInt64(reader.GetValue(1));
                    dead = Convert.ToInt64(reader.GetValue(2));
                }

                var scoped = await ReadScopedBlockingAsync(id, startUtc, endUtc, separate, cancellationToken);
                var scopedDead = await ReadScopedDeadlocksAsync(id, startUtc, endUtc, separate, cancellationToken);
                /* The fleet totals' statement and these reads are separate statements, so an event landing
                   between them can make one master's term briefly off by that event (a card that fell back to
                   a larger DMV count makes it negative, which is right). The caller clamps the final total at
                   zero, so a race can shave a count by an event but never drive a total below zero. */
                blocking += (xe > 0 ? xe : dmv) - (scoped.Count > 0 ? scoped.Count : dmv);
                deadlocks += dead - scopedDead;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                ViewerLogger.Warn("ViewerDataService", $"Scoped read failed for master {id}; its fleet share stays unscoped: {ex.Message}");
            }
        }

        return (blocking, deadlocks);
    }
}
