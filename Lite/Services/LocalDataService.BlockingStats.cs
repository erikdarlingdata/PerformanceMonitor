/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// One per-minute bucket of blocking SEVERITY (the Blocking Stats sub-tab): the count of blocked-process
/// events in the bucket plus the total / max / avg blocking DURATION (ms) over them. The Lite equivalent of
/// the Dashboard's pre-aggregated <c>collect.blocking_deadlock_stats</c> (blocking_event_count /
/// total_blocking_duration_ms / max_blocking_duration_ms / avg_blocking_duration_ms), computed on-the-fly from
/// the raw <c>blocked_process_reports</c> rows Lite already keeps, so no collector or schema change is needed.
/// The duration source is each blocked process's <c>wait_time_ms</c> — the same column the Blocked Process
/// Reports grid and the blocking-incident count trend read.
/// </summary>
public sealed record BlockingDurationStatsPoint(
    DateTime Time, int EventCount, long TotalDurationMs, long MaxDurationMs, double AvgDurationMs);

/* The DeadlockSeverityStatsPoint record moved to PerformanceMonitor.Common (#2484), because the headless
   service needed the same shape and a third identical copy is how three surfaces end up disagreeing about
   one deadlock. Only the RECORD is shared; the aggregation below stays here, since Lite parses graphs into
   DeadlockProcessDetail where Darling uses DeadlockProcessNode -- unifying those is a separate question. */

public partial class LocalDataService
{
    /// <summary>
    /// Whether ANY of the three capture paths behind the blocking-severity read has ever produced a row.
    /// <para>Both are checked because either can be off on a given server: the XE blocked-process report
    /// needs its session running, the DMV snapshot needs its collector enabled, and deadlock capture is
    /// separate from both -- the verdict gates on the blocking AND deadlock series being empty, so leaving
    /// deadlocks out would call a server clear on blocking capture alone. Probing one would report
    /// "never captured" for a server capturing fine through the other, and probing neither would let a
    /// silent capture gap read as a clean bill of health. Darling's twin is
    /// <c>DarlingDataReader.HasAnyBlockingCaptureAsync</c>.</para>
    /// </summary>
    public async Task<bool> HasAnyBlockingCaptureAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
WHERE EXISTS (SELECT 1 FROM v_blocked_process_reports WHERE server_id = $1)
OR    EXISTS (SELECT 1 FROM v_dmv_blocking_snapshots WHERE server_id = $2)
OR    EXISTS (SELECT 1 FROM v_deadlocks WHERE server_id = $3)";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Blocking-duration aggregate per minute — the SEVERITY companion to
    /// <see cref="GetBlockingTrendAsync"/>'s count-only trend, built on the SAME XE-preferred / DMV-fallback
    /// source selection so the two reconcile exactly. XE blocked-process reports
    /// (<c>v_blocked_process_reports</c>) are the primary source, bucketed on <c>event_time</c>; the always-on
    /// DMV snapshot (<c>v_dmv_blocking_snapshots</c>) contributes only when the XE source has no rows in the
    /// window (<c>WHERE NOT EXISTS</c>), so a server with both sources never double-counts (identical exclusion
    /// to the count trend). Each bucket carries COUNT(*) events and the SUM / MAX / AVG of <c>wait_time_ms</c>
    /// (the per-blocked-process block duration). Honors the #1319 database filter on both arms, exactly like the
    /// count trend.
    /// </summary>
    public async Task<List<BlockingDurationStatsPoint>> GetBlockingDurationStatsAsync(
        int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        /* BPR per-minute severity buckets, falling back to the always-on DMV snapshot only when BPR has none
           in the window (AWS RDS) — so a server with both sources never double-counts. Mirrors the blocking
           count trend's XE→DMV union so the severity chart and the count chart draw from the same rows. */
        command.CommandText = @"
WITH bpr AS (
    SELECT
        DATE_TRUNC('minute', event_time) AS bucket,
        COUNT(*) AS event_count,
        CAST(COALESCE(SUM(wait_time_ms), 0) AS BIGINT) AS total_duration_ms,
        CAST(COALESCE(MAX(wait_time_ms), 0) AS BIGINT) AS max_duration_ms,
        CAST(COALESCE(AVG(wait_time_ms), 0) AS DOUBLE) AS avg_duration_ms
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause + @"
    GROUP BY DATE_TRUNC('minute', event_time)
),
dmv AS (
    SELECT
        DATE_TRUNC('minute', event_time) AS bucket,
        COUNT(*) AS event_count,
        CAST(COALESCE(SUM(wait_time_ms), 0) AS BIGINT) AS total_duration_ms,
        CAST(COALESCE(MAX(wait_time_ms), 0) AS BIGINT) AS max_duration_ms,
        CAST(COALESCE(AVG(wait_time_ms), 0) AS DOUBLE) AS avg_duration_ms
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause + @"
    GROUP BY DATE_TRUNC('minute', event_time)
)
SELECT bucket, event_count, total_duration_ms, max_duration_ms, avg_duration_ms FROM bpr
UNION ALL
SELECT bucket, event_count, total_duration_ms, max_duration_ms, avg_duration_ms FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<BlockingDurationStatsPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new BlockingDurationStatsPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4))));
        }

        return items;
    }

    /// <summary>
    /// Deadlock-severity buckets for one server over the window (Blocking Stats sub-tab): victim count and
    /// total / max / avg deadlock wait per minute. Reads the raw graphs, then parses + aggregates them OFF the
    /// UI thread (<see cref="Task.Run"/>) — the graph walk is CPU-bound XML work, the same reason the Deadlocks
    /// grid parses off-thread (#1193). Windows on <c>collection_time</c> and reads <c>v_deadlocks</c> — the
    /// IDENTICAL row-selection predicate <see cref="GetDeadlockTrendAsync"/> uses — so the deadlock COUNT shown
    /// on this tab's summary strip and the victim/wait aggregate are drawn from the exact same set of deadlock
    /// rows and reconcile in period. No LIMIT: deadlocks are rare, and a cap would drop rows the un-capped count
    /// trend keeps (breaking reconciliation) — deliberately NOT reusing the capped
    /// <see cref="GetRecentDeadlocksAsync"/>.
    /// </summary>
    public async Task<List<DeadlockSeverityStatsPoint>> GetDeadlockSeverityStatsAsync(
        int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        var rows = new List<DeadlockRow>();

        using (var connection = await OpenConnectionAsync())
        using (var command = connection.CreateCommand())
        {
            var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

            command.CommandText = @"
SELECT
    deadlock_time,
    deadlock_graph_xml
FROM v_deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY deadlock_time";

            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rows.Add(new DeadlockRow
                {
                    DeadlockTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                    DeadlockGraphXml = reader.IsDBNull(1) ? "" : reader.GetString(1)
                });
            }
        }

        /* Parse each graph + aggregate on the thread pool (Lite's #1193 discipline): deadlocks are low-volume,
           but the sp_BlitzLock-style walk is still CPU-bound, so it never runs on the dispatcher. */
        return await Task.Run(() => AggregateDeadlockSeverity(rows));
    }

    /// <summary>
    /// Parses each deadlock graph via Lite's own <see cref="DeadlockProcessDetail.ParseFromRows"/> (the SAME
    /// parse the Deadlocks grid uses, so the two agree) and rolls the processes up into per-minute severity
    /// buckets (bucketed on <c>deadlock_time</c> truncated to the minute, matching the count trend's
    /// <c>DATE_TRUNC('minute', deadlock_time)</c>). Per bucket: <c>victim_count</c> = SUM of
    /// <see cref="DeadlockProcessDetail.IsVictim"/>, and total / max / avg deadlock wait over EVERY process's
    /// <see cref="DeadlockProcessDetail.WaitTime"/> (avg is process-weighted = total ÷ process count) — the
    /// Dashboard <c>collect.blocking_deadlock_stats</c> semantics. A per-process row with a null
    /// <c>deadlock_time</c> (unplaceable on the time axis) contributes nothing. Pure + off-WPF so a round-trip
    /// test pins it; the ORDER BY is re-applied here because the dictionary rollup does not preserve read order.
    /// </summary>
    internal static List<DeadlockSeverityStatsPoint> AggregateDeadlockSeverity(List<DeadlockRow> rows)
    {
        var details = DeadlockProcessDetail.ParseFromRows(rows);
        var buckets = new Dictionary<DateTime, DeadlockSeverityAccumulator>();

        foreach (var detail in details)
        {
            if (detail.DeadlockTime is not { } dt)
                continue;

            var bucket = TruncateToMinute(dt);
            if (!buckets.TryGetValue(bucket, out var acc))
            {
                acc = new DeadlockSeverityAccumulator();
                buckets[bucket] = acc;
            }

            if (detail.IsVictim)
                acc.VictimCount++;
            acc.TotalWaitMs += detail.WaitTime;
            if (detail.WaitTime > acc.MaxWaitMs)
                acc.MaxWaitMs = detail.WaitTime;
            acc.ProcessCount++;
        }

        return buckets
            .OrderBy(kvp => kvp.Key)
            .Select(kvp => new DeadlockSeverityStatsPoint(
                kvp.Key,
                kvp.Value.VictimCount,
                kvp.Value.TotalWaitMs,
                kvp.Value.MaxWaitMs,
                kvp.Value.ProcessCount > 0 ? (double)kvp.Value.TotalWaitMs / kvp.Value.ProcessCount : 0.0))
            .ToList();
    }

    /// <summary>Mutable per-bucket accumulator for <see cref="AggregateDeadlockSeverity"/> (a reference type so
    /// the <c>TryGetValue</c> handle mutates the stored instance in place — one alloc per populated minute,
    /// which is negligible at deadlock volumes).</summary>
    private sealed class DeadlockSeverityAccumulator
    {
        public int VictimCount;
        public long TotalWaitMs;
        public long MaxWaitMs;
        public int ProcessCount;
    }

    /// <summary>Truncates to the minute to match DuckDB <c>DATE_TRUNC('minute', …)</c> (the count trend's
    /// bucketing), preserving the value's <see cref="DateTimeKind"/> (naive UTC from the store).</summary>
    private static DateTime TruncateToMinute(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute), value.Kind);
}
