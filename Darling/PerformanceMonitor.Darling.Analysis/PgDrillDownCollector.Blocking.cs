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
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgDrillDownCollector
{
    public const string TopDeadlocksSql = @"
SELECT collection_time, deadlock_time, victim_process_id,
       LEFT(victim_sql_text, 500) AS victim_sql,
       deadlock_graph_xml
FROM v_deadlocks
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
ORDER BY collection_time DESC
LIMIT 3";

    private async Task CollectTopDeadlocks(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(TopDeadlocksSql, connection);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            /* #1140: parse the involved objects from the graph for the dedup fingerprint + a readable
               Objects field. The raw graph XML is NOT surfaced (it would bloat the alert detail). */
            var objects = DeadlockObjectExtractor.FromGraphXml(reader.IsDBNull(4) ? null : reader.GetString(4));
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                deadlock_time = reader.IsDBNull(1) ? "" : reader.GetDateTime(1).ToString("o"),
                victim = reader.IsDBNull(2) ? "" : reader.GetString(2),
                victim_sql = reader.IsDBNull(3) ? "" : reader.GetString(3),
                objects = string.Join(", ", objects)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_deadlocks"] = items;
    }

    /* BPR + always-on DMV blocking snapshot, so the flat top-blocking list isn't empty when the
       blocked-process-report XE captured nothing (AWS RDS). Worst-by-wait surfaces regardless of
       source; on a box with both, each may contribute (this is a top-5 list, not a count). */
    public const string TopBlockingChainsSql = @"
SELECT collection_time, database_name, blocked_spid, blocking_spid,
       wait_time_ms, lock_mode, blocked_sql, blocking_sql, contentious_object
FROM
(
    SELECT collection_time, database_name, blocked_spid, blocking_spid,
           wait_time_ms, lock_mode,
           LEFT(blocked_sql_text, 500) AS blocked_sql,
           LEFT(blocking_sql_text, 500) AS blocking_sql,
           contentious_object
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3

    UNION ALL

    SELECT collection_time, database_name, blocked_spid, blocking_spid,
           wait_time_ms, lock_mode,
           LEFT(blocked_sql_text, 500) AS blocked_sql,
           LEFT(blocking_sql_text, 500) AS blocking_sql,
           contentious_object
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
) AS combined
ORDER BY wait_time_ms DESC
LIMIT 5";

    private async Task CollectTopBlockingChains(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(TopBlockingChainsSql, connection);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                database = reader.IsDBNull(1) ? "" : reader.GetString(1),
                blocked_spid = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                blocking_spid = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                wait_time_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                lock_mode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                blocked_sql = reader.IsDBNull(6) ? "" : reader.GetString(6),
                blocking_sql = reader.IsDBNull(7) ? "" : reader.GetString(7),
                contentious_object = reader.IsDBNull(8) ? "" : reader.GetString(8)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_blocking_chains"] = items;
    }

    // SpidFilter keeps the drill-down, fact collector, and viewer fetch in lockstep on the apex
    // (a missing blocker maps to spid 0 — see PgBlockingPairRowQuery). SQL text is truncated here
    // for the drill-down payload; the shared reader mapping is unaffected (same column order).
    public const string ReconstructedChainsSql = $@"
SELECT
    {PgBlockingPairRowQuery.LeadingColumns},
    LEFT(blocked_sql_text, 500) AS blocked_sql,
    LEFT(blocking_sql_text, 500) AS blocking_sql,
    {PgBlockingPairRowQuery.IdentityColumns},
    contentious_object,
    {PgBlockingPairRowQuery.TrailingIdentityColumns}
FROM v_blocked_process_reports
WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
{PgBlockingPairRowQuery.SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";

    /// <summary>
    /// Reconstructs blocking chains (same logic as the collector) and surfaces the top 3
    /// by magnitude — apex, depth, victim count, and the level-by-level structure that the
    /// flat top_blocking_chains list cannot show.
    /// </summary>
    private async Task CollectReconstructedBlockingChains(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(ReconstructedChainsSql, connection);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var rows = new List<BlockingPairRow>();
        using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
        {
            while (await reader.ReadAsync(context.CancellationToken))
                rows.Add(PgBlockingPairRowQuery.Read(reader));
        }

        // Always-on DMV blocking snapshot fallback. Merge BEFORE the empty check so DMV-only blocking
        // (blocked-process-report unavailable, e.g. AWS RDS) still reconstructs.
        await PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            connection.CreateCommand, rows, context.ServerId, context.TimeRangeStart, context.TimeRangeEnd,
            context.CancellationToken);

        if (rows.Count == 0) return;

        var reconstruction = BlockingChainReconstructor.Reconstruct(
            rows, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: false);

        var items = new List<object>();
        foreach (var chain in reconstruction.Chains.Take(3))
        {
            items.Add(new
            {
                apex_spid = chain.ApexSpid,
                apex_sleeping = chain.ApexSleeping,
                depth = chain.Depth,
                // Distinct sessions blocked under this apex over the window — cumulative, not peak-concurrent.
                victim_count = chain.VictimCount,
                max_wait_ms = chain.MaxWaitMs,
                levels = chain.Levels.Select(l => new
                {
                    level = l.Level,
                    blocking_spid = l.BlockingSpid,
                    blocked_spid = l.BlockedSpid,
                    lock_mode = l.LockMode,
                    wait_time_ms = l.WaitTimeMs,
                    blocking_sql = l.BlockingSqlText,
                    blocked_sql = l.BlockedSqlText
                }).ToList()
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["reconstructed_blocking_chains"] = items;
    }

    public const string LockModeBreakdownSql = @"
SELECT wait_type,
       SUM(delta_wait_time_ms)::BIGINT AS total_wait_ms,
       SUM(delta_waiting_tasks)::BIGINT AS total_count
FROM v_wait_stats
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
AND   wait_type ILIKE 'LCK%'
AND   delta_wait_time_ms > 0
GROUP BY wait_type
ORDER BY total_wait_ms DESC
LIMIT 10";

    private async Task CollectLockModeBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var cmd = new NpgsqlCommand(LockModeBreakdownSql, connection);
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            items.Add(new
            {
                lock_type = reader.IsDBNull(0) ? "" : reader.GetString(0),
                total_wait_ms = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                waiting_tasks = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["lock_mode_breakdown"] = items;
    }
}
