/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
    /// <summary>
    /// The monitored targets (identity columns only, never a credential column) and the edition of server $1
    /// (<c>servers.sql_engine_edition</c>; 5 = Azure SQL Database), as one row per target.
    /// </summary>
    public const string SeparatelyMonitoredTargetsSql = @"
SELECT
    c.server_id,
    c.host,
    c.database,
    c.is_enabled,
    c.read_only_intent,
    (SELECT COALESCE(s.sql_engine_edition, 0) FROM servers s WHERE s.server_id = $1) AS self_edition
FROM config_monitored_servers c";

    private const int AzureSqlDatabaseEngineEdition = 5;

    /// <summary>
    /// The databases a master registration should skip because they are monitored as their own targets, or an
    /// empty list (not an Azure SQL Database master, no such sibling, or the server is not registered).
    /// </summary>
    public async Task<IReadOnlyList<string>> GetSeparatelyMonitoredAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var targets = new List<AlertTargetIdentity>();
        var edition = 0;
        string? host = null;
        string? database = null;

        await using (var command = _dataSource.CreateCommand(SeparatelyMonitoredTargetsSql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt32(0);
                var targetHost = reader.GetString(1);
                var targetDatabase = reader.IsDBNull(2) ? null : reader.GetString(2);
                var enabled = !reader.IsDBNull(3) && reader.GetBoolean(3);
                var readOnly = !reader.IsDBNull(4) && reader.GetBoolean(4);
                edition = reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5));
                if (id == serverId)
                {
                    host = targetHost;
                    database = targetDatabase;
                }

                targets.Add(new AlertTargetIdentity(
                    id.ToString(System.Globalization.CultureInfo.InvariantCulture), targetHost, targetDatabase, enabled, readOnly));
            }
        }

        if (host is null || edition != AzureSqlDatabaseEngineEdition)
        {
            return Array.Empty<string>();
        }

        return AzureMasterScope.SeparatelyMonitoredDatabases(
            true, serverId.ToString(System.Globalization.CultureInfo.InvariantCulture), host, database, targets);
    }

    /// <summary>
    /// The master's windowed XE blocking count and worst wait without the separately monitored databases'
    /// events (<see cref="PgFactCollector.BlockingSqlSkippingSeparate"/>, the read the service's analysis uses).
    /// </summary>
    private async Task<(int Count, long MaxWaitMs)> ReadScopedBlockingAsync(
        int serverId, DateTime start, DateTime end, IReadOnlyList<string> separate, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(PgFactCollector.BlockingSqlSkippingSeparate);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = ToArray(separate) });
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

    /// <summary>The master's windowed deadlock count without the deadlocks wholly in the separately monitored databases.</summary>
    private async Task<long> ReadScopedDeadlocksAsync(
        int serverId, DateTime start, DateTime end, IReadOnlyList<string> separate, CancellationToken cancellationToken)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        return await PgFactCollector.CountDeadlocksSkippingSeparateAsync(
            connection, PgFactCollector.DeadlockOutsideCountSql, PgFactCollector.DeadlockGraphsSql,
            serverId, start, end, separate, cancellationToken, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);
    }

    private static string[] ToArray(IReadOnlyList<string> list)
    {
        var result = new string[list.Count];
        for (var i = 0; i < result.Length; i++)
        {
            result[i] = list[i];
        }

        return result;
    }

    private const string FleetMasterUnscopedSql = @"
SELECT
    (SELECT COUNT(*) FROM v_blocked_process_reports WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3 AND collection_time >= $4),
    (SELECT COUNT(*) FROM v_dmv_blocking_snapshots   WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3 AND collection_time >= $4),
    (SELECT COUNT(*) FROM v_deadlocks WHERE server_id = $1 AND deadlock_time >= $2 AND deadlock_time <= $3 AND collection_time >= $4)";

    private const string FleetMasterCandidatesSql = @"
SELECT server_id FROM servers WHERE is_enabled AND sql_engine_edition = 5";

    /// <summary>
    /// What the fleet totals over-count for the Azure SQL Database masters that have separately monitored
    /// databases: per master, its unscoped contribution (XE count when it has any XE row, else DMV; plus the
    /// deadlock count) minus its scoped contribution, the figure its own card shows.
    /// </summary>
    private async Task<(long Blocking, long Deadlocks)> ReadFleetMasterOvercountAsync(
        DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var masters = new List<int>();
        await using (var command = _dataSource.CreateCommand(FleetMasterCandidatesSql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                masters.Add(reader.GetInt32(0));
            }
        }

        long blocking = 0;
        long deadlocks = 0;
        foreach (var id in masters)
        {
            var separate = await GetSeparatelyMonitoredAsync(id, cancellationToken);
            if (separate.Count == 0)
            {
                continue;
            }

            long xe, dmv, dead;
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
            blocking += (xe > 0 ? xe : dmv) - (scoped.Count > 0 ? scoped.Count : dmv);
            deadlocks += dead - scopedDead;
        }

        return (blocking, deadlocks);
    }
}
