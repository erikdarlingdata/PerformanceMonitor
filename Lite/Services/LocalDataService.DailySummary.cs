/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The Performance Calendar / Daily Summary aggregate, grouped one row per day over a half-open
    /// [fromDate, toDate) window. One row is returned for every day that had ANY collection (present in
    /// the collection log or any source view); days with no collection are simply absent, which the
    /// calendar renders as <see cref="DailyHealthBand.NoData"/>. Each row's composite band is computed by
    /// the shared <see cref="DailyHealthBandCalculator"/> so it bands identically to the Darling viewer.
    ///
    /// This is the single source of truth for the daily aggregate; <see cref="GetDailySummaryAsync"/>
    /// (single day) delegates here so the calendar cell and the drilled-in day can never disagree.
    /// </summary>
    private const string DailySummaryRangeSql = @"
WITH wait_per_type AS (
    SELECT date_trunc('day', collection_time) AS d, wait_type, SUM(delta_wait_time_ms) AS ms
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3 AND delta_wait_time_ms > 0
    GROUP BY 1, 2
),
waits AS (
    SELECT d, SUM(ms) / 1000.0 AS total_wait_sec, arg_max(wait_type, ms) AS top_wait_type
    FROM wait_per_type
    GROUP BY d
),
queries AS (
    SELECT date_trunc('day', collection_time) AS d, COUNT(DISTINCT query_hash) AS c
    FROM v_query_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
deadlocks AS (
    SELECT date_trunc('day', collection_time) AS d, COUNT(*) AS c
    FROM v_deadlocks
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
bpr AS (
    SELECT date_trunc('day', collection_time) AS d, COUNT(*) AS c, MAX(wait_time_ms) AS max_wait_ms
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
dmv AS (
    SELECT date_trunc('day', collection_time) AS d, COUNT(*) AS c, MAX(wait_time_ms) AS max_wait_ms
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
cpu AS (
    /* Total host CPU = SQL + other-process (NULL on Linux -> 0), matching the alert engine and the
       Overview headline; sustained >= 80 samples drive the day's band. */
    SELECT date_trunc('day', collection_time) AS d,
           COUNT(*) FILTER (WHERE (sqlserver_cpu_utilization + COALESCE(other_process_cpu_utilization, 0)) >= 80) AS c
    FROM v_cpu_utilization_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
coll AS (
    /* Any run (all statuses) marks the day as collected -> it appears even if every metric is quiet
       (a quiet monitored day is Healthy/green, not No-Data/grey). errs feeds the Critical band. */
    SELECT date_trunc('day', collection_time) AS d,
           COUNT(*) AS runs,
           COUNT(*) FILTER (WHERE status = 'ERROR') AS errs
    FROM v_collection_log
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
mem AS (
    SELECT date_trunc('day', collection_time) AS d,
           COUNT(*) FILTER (WHERE memory_indicators_process >= 2 OR memory_indicators_system >= 2) AS pressure,
           COUNT(*) FILTER (WHERE memory_indicators_process >= 3) AS critical
    FROM v_memory_pressure_events
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
alerts AS (
    /* Actionable alerts only: exclude dismissed rows and resolution/good-news notices, mirroring
       AlertMetricClassifier.IsResolution. The suffix list must match that method exactly — it is a
       hand-maintained SQL copy, so widening one without the other silently counts recoveries
       -- Collection Resumed, Agent Restarted, AG Sync Recovered -- as actionable alerts. */
    SELECT date_trunc('day', alert_time) AS d, COUNT(*) AS c
    FROM v_config_alert_log
    WHERE server_id = $1 AND alert_time >= $2 AND alert_time < $3
      AND dismissed = false
      AND metric_name NOT LIKE '%Cleared%'
      AND metric_name NOT LIKE '%Resolved%'
      AND metric_name NOT LIKE '%Restored%'
      AND metric_name NOT LIKE '%Resumed%'
      AND metric_name NOT LIKE '%Restarted%'
      AND metric_name NOT LIKE '%Recovered%'
      AND metric_name NOT LIKE '%Reconnected%'
    GROUP BY 1
),
day_spine AS (
    SELECT d FROM waits
    UNION SELECT d FROM queries
    UNION SELECT d FROM deadlocks
    UNION SELECT d FROM bpr
    UNION SELECT d FROM dmv
    UNION SELECT d FROM cpu
    UNION SELECT d FROM coll
    UNION SELECT d FROM mem
    UNION SELECT d FROM alerts
)
SELECT
    s.d AS day,
    COALESCE(w.total_wait_sec, 0) AS total_wait_sec,
    w.top_wait_type,
    COALESCE(q.c, 0) AS unique_queries,
    COALESCE(dl.c, 0) AS deadlock_count,
    COALESCE(NULLIF(b.c, 0), dm.c, 0) AS blocking_events,
    COALESCE(cp.c, 0) AS high_cpu_events,
    COALESCE(cl.errs, 0) AS collection_errors,
    COALESCE(m.pressure, 0) AS memory_pressure_events,
    COALESCE(m.critical, 0) AS memory_critical_events,
    COALESCE(al.c, 0) AS alert_count,
    /* Peak block wait (ms) from the SAME source the blocking count came from (BPR preferred, DMV-snapshot
       fallback), so the day-detail blocking reason ('N blocking events (peak block X)') reconciles with the
       count. 0 when the blocking came from a source without a wait time. */
    COALESCE(CASE WHEN COALESCE(b.c, 0) > 0 THEN b.max_wait_ms ELSE dm.max_wait_ms END, 0) AS peak_block_wait_ms
FROM day_spine s
LEFT JOIN waits w ON w.d = s.d
LEFT JOIN queries q ON q.d = s.d
LEFT JOIN deadlocks dl ON dl.d = s.d
LEFT JOIN bpr b ON b.d = s.d
LEFT JOIN dmv dm ON dm.d = s.d
LEFT JOIN cpu cp ON cp.d = s.d
LEFT JOIN coll cl ON cl.d = s.d
LEFT JOIN mem m ON m.d = s.d
LEFT JOIN alerts al ON al.d = s.d
ORDER BY s.d";

    /// <summary>
    /// Returns one <see cref="DailySummaryRow"/> per collected day in the half-open [fromDate, toDate)
    /// window (dates normalized to their date component). Powers the Performance Calendar month grid.
    /// </summary>
    public async Task<List<DailySummaryRow>> GetDailySummaryRangeAsync(int serverId, DateTime fromDate, DateTime toDate)
    {
        using var _q = TimeQuery("GetDailySummaryRangeAsync", "daily summary range aggregation");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = DailySummaryRangeSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = fromDate.Date });
        command.Parameters.Add(new DuckDBParameter { Value = toDate.Date });

        var results = new List<DailySummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.Add(ReadDailySummaryRow(reader));
        }

        return results;
    }

    /// <summary>
    /// Gets the daily summary for a specific date (or today if null). Delegates to the range query for a
    /// single day so the single-day contract shares the one aggregate; returns a No-Data row when the day
    /// had no collection.
    /// </summary>
    public async Task<DailySummaryRow?> GetDailySummaryAsync(int serverId, DateTime? summaryDate = null)
    {
        var targetDate = summaryDate?.Date ?? DateTime.UtcNow.Date;
        var rows = await GetDailySummaryRangeAsync(serverId, targetDate, targetDate.AddDays(1));
        return rows.Count > 0
            ? rows[0]
            : new DailySummaryRow { SummaryDate = targetDate, HasData = false, HealthBand = DailyHealthBand.NoData };
    }

    private static DailySummaryRow ReadDailySummaryRow(System.Data.Common.DbDataReader reader)
    {
        var row = new DailySummaryRow
        {
            SummaryDate = reader.IsDBNull(0) ? DateTime.MinValue : Convert.ToDateTime(reader.GetValue(0)),
            TotalWaitTimeSec = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
            TopWaitType = reader.IsDBNull(2) ? "" : reader.GetString(2),
            UniqueQueries = reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
            DeadlockCount = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
            BlockingEvents = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
            HighCpuEvents = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6)),
            CollectionErrors = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7)),
            MemoryPressureEvents = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
            MemoryCriticalEvents = reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
            AlertCount = reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
            MaxBlockDurationMs = reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
            HasData = true,
        };
        row.HealthBand = DailyHealthBandCalculator.Classify(row.ToSignals());
        return row;
    }
}

public class DailySummaryRow
{
    public DateTime SummaryDate { get; set; }
    public decimal TotalWaitTimeSec { get; set; }
    public string TopWaitType { get; set; } = "";
    public long UniqueQueries { get; set; }
    public long DeadlockCount { get; set; }
    public long BlockingEvents { get; set; }
    public long HighCpuEvents { get; set; }
    public long MemoryPressureEvents { get; set; }
    public long MemoryCriticalEvents { get; set; }
    public long CollectionErrors { get; set; }
    public long AlertCount { get; set; }

    /// <summary>The day's peak/max block wait in ms (0 when no blocking, or blocking from a source without a
    /// wait time). Surfaced on the day-detail panel's blocking reason.</summary>
    public long MaxBlockDurationMs { get; set; }

    /// <summary>True when the day had any collection. False renders the calendar cell as No-Data (grey).</summary>
    public bool HasData { get; set; }

    /// <summary>The composite health band that colors this day's calendar cell.</summary>
    public DailyHealthBand HealthBand { get; set; } = DailyHealthBand.NoData;

    /// <summary>Human label for the band ("Healthy" / "Warning" / "Critical" / "No Data"), shown in the day detail.</summary>
    public string OverallHealth => DailyHealthBandCalculator.Label(HealthBand);

    public string SummaryDateFormatted => SummaryDate.ToString("yyyy-MM-dd");
    public string TotalWaitFormatted => TotalWaitTimeSec < 1000
        ? $"{TotalWaitTimeSec:N1} s"
        : $"{TotalWaitTimeSec / 60:N1} min";

    /// <summary>Multi-line hover text summarizing the day's signals, for the calendar cell tooltip.</summary>
    public string SignalsTooltip => DailyHealthBandCalculator.Describe(ToSignals());

    /// <summary>Projects this row's counts into the shared banding input.</summary>
    public DailyHealthSignals ToSignals() => new()
    {
        HasData = HasData,
        Deadlocks = DeadlockCount,
        CollectionErrors = CollectionErrors,
        HighCpuEvents = HighCpuEvents,
        BlockingEvents = BlockingEvents,
        MemoryPressureEvents = MemoryPressureEvents,
        MemoryCriticalEvents = MemoryCriticalEvents,
        AlertCount = AlertCount,
        /* #3525: a calendar day is the 24-hour window these counts were aggregated over — the denominator
           the deadlock rate bands on. */
        Window = TimeSpan.FromDays(1),
    };
}
