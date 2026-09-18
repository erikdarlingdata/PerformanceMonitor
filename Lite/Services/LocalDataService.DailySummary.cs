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
    /* Total host CPU = SQL + other-process (NULL on Linux -> 0), matching the Overview headline. The 80 is
       ServerHealthThresholds.CpuWarningPercent, the card band's Warning bar, restated as a literal because
       this is a SQL string and pinned equal by both suites (#3539 A2). Deliberately NOT the alert engine's
       configurable CPU threshold: this statement re-counts at read time, so binding it to a knob would
       recolour every past day the moment the knob moved. The count feeds a bar that scales with the window
       (DailyHealthThresholds.HighCpuCriticalSamplesFor). */
    SELECT date_trunc('day', collection_time) AS d,
           COUNT(*) FILTER (WHERE (sqlserver_cpu_utilization + COALESCE(other_process_cpu_utilization, 0)) >= 80) AS c
    FROM v_cpu_utilization_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time < $3
    GROUP BY 1
),
coll AS (
    /* Any run (all statuses) marks the day as collected -> it appears even if every metric is quiet
       (a quiet monitored day is Healthy/green, not No-Data/grey). runs is also the denominator the error
       SHARE bands on (#3539 A2); errs alone used to make the day Critical on presence. */
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
    COALESCE(CASE WHEN COALESCE(b.c, 0) > 0 THEN b.max_wait_ms ELSE dm.max_wait_ms END, 0) AS peak_block_wait_ms,
    /* Every collector run in the window (#3539 A2): the denominator that turns collection_errors into a
       share. Appended after peak_block_wait_ms so every existing ordinal read stays where it was. */
    COALESCE(cl.runs, 0) AS collection_runs,
    /* #3541 A9: how many of the seven per-signal sources hold at least one row for the day — the retention
       arm's PRESENCE fact, Darling's DailySummarySql column of the same name (its comment carries the
       reasoning). Lite's sources share one archive horizon, so the ghost is narrower here, but the reader
       judges the day the same way from the same fact. Appended LAST, after collection_runs. */
    (CASE WHEN w.d IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN q.d IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN dl.d IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN b.d IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN dm.d IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN cp.d IS NULL THEN 0 ELSE 1 END)
      + (CASE WHEN m.d IS NULL THEN 0 ELSE 1 END) AS signal_sources_present
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
    public async Task<List<DailySummaryRow>> GetDailySummaryRangeAsync(int serverId, DateTime fromDate, DateTime toDate, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetDailySummaryRangeAsync", "daily summary range aggregation");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = DailySummaryRangeSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = fromDate.Date });
        command.Parameters.Add(new DuckDBParameter { Value = toDate.Date });

        /* #3525 review: the still-forming day's window clamps against the read's own clock — the anchored
           MCP read hands its resolved window end so a backdated as_of never clamps against the process
           clock; the live calendar read leaves this null. */
        var referenceUtc = asOfUtc ?? DateTime.UtcNow;

        /* #3541 A9: the retention horizon, from the READER's wall clock and never the anchor — a purge is a
           wall-clock event and a backdated as_of cannot un-purge an archive; DailySummaryRetention.HorizonFor
           says why in full. Lite has ONE horizon for every source (RetentionService.ArchiveRetentionMonths),
           so a day past it is gone from every table together and cannot ghost the way Darling's per-signal
           horizons let a day do; the state is stamped all the same so the two SKUs publish one vocabulary
           and the same day is judged the same way on both. */
        var retentionHorizon = DailySummaryRetentionHorizon(DateTime.UtcNow);

        var results = new List<DailySummaryRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            results.Add(ReadDailySummaryRow(reader, referenceUtc, retentionHorizon));
        }

        return results;
    }

    /// <summary>
    /// The oldest UTC day every daily-summary source still holds on Lite (#3541 A9): today minus the archive
    /// retention. Month arithmetic rather than <see cref="DailySummaryRetention.HorizonFor"/>'s day count
    /// because Lite's horizon is DECLARED in months (<see cref="RetentionService.ArchiveRetentionMonths"/>)
    /// and the cleanup that enforces it subtracts months; converting to a day count here would make the two
    /// disagree by a day or two around month ends.
    /// </summary>
    internal static DateTime DailySummaryRetentionHorizon(DateTime utcNow) =>
        utcNow.AddMonths(-RetentionService.ArchiveRetentionMonths).Date;

    /// <summary>
    /// Gets the daily summary for a specific date (or today if null). Delegates to the range query for a
    /// single day so the single-day contract shares the one aggregate; returns a No-Data row when the day
    /// had no collection.
    /// </summary>
    public async Task<DailySummaryRow?> GetDailySummaryAsync(int serverId, DateTime? summaryDate = null)
    {
        var targetDate = summaryDate?.Date ?? DateTime.UtcNow.Date;
        var rows = await GetDailySummaryRangeAsync(serverId, targetDate, targetDate.AddDays(1));
        if (rows.Count > 0)
        {
            return rows[0];
        }

        /* #3541 A9: a day the spine does not hold is not "collected" either — before the horizon it is purged
           like any other, inside it simply without a run record — so the single-day tool can say which. */
        var horizon = DailySummaryRetentionHorizon(DateTime.UtcNow);
        return new DailySummaryRow
        {
            SummaryDate = targetDate,
            HasData = false,
            HealthBand = DailyHealthBand.NoData,
            DataState = DailySummaryRetention.StateFor(targetDate, 0, 0, horizon),
            RetentionHorizon = horizon,
        };
    }

    private static DailySummaryRow ReadDailySummaryRow(System.Data.Common.DbDataReader reader, DateTime referenceUtc, DateTime retentionHorizon)
    {
        var row = new DailySummaryRow
        {
            ReferenceUtc = referenceUtc,
            RetentionHorizon = retentionHorizon,
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
            /* #3539 A2: the trailing collection_runs column — the collection-error share's denominator. */
            CollectionRuns = reader.IsDBNull(12) ? 0L : Convert.ToInt64(reader.GetValue(12)),
            /* #3541 A9: the signal-presence count, after collection_runs. */
            SignalSourcesPresent = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13)),
            HasData = true,
        };
        /* #3541 A9: judged from the day, its run count, its signal presence and the horizon; ToSignals folds
           a non-collected state into HasData = false so the shared band reads NoData rather than
           measured-zero-Healthy. */
        row.DataState = DailySummaryRetention.StateFor(row.SummaryDate, row.CollectionRuns, row.SignalSourcesPresent, retentionHorizon);
        row.HealthBand = DailyHealthBandCalculator.Classify(row.ToSignals());
        return row;
    }
}

public class DailySummaryRow
{
    public DateTime SummaryDate { get; set; }

    /// <summary>The clock the still-forming day's window clamps against (#3525 review): the anchored
    /// MCP range read hands its resolved window end so a backdated as_of clamps against its own "now";
    /// the live calendar read leaves the wall-clock default.</summary>
    public DateTime ReferenceUtc { get; set; } = DateTime.UtcNow;
    public decimal TotalWaitTimeSec { get; set; }
    public string TopWaitType { get; set; } = "";
    public long UniqueQueries { get; set; }
    public long DeadlockCount { get; set; }
    public long BlockingEvents { get; set; }
    public long HighCpuEvents { get; set; }
    public long MemoryPressureEvents { get; set; }
    public long MemoryCriticalEvents { get; set; }
    public long CollectionErrors { get; set; }

    /// <summary>Collector runs of every status that day (#3539 A2) — the denominator the collection-error
    /// share bands on, read from the daily SQL's trailing <c>collection_runs</c> column.</summary>
    public long CollectionRuns { get; set; }
    public long AlertCount { get; set; }

    /// <summary>The day's peak/max block wait in ms (0 when no blocking, or blocking from a source without a
    /// wait time). Surfaced on the day-detail panel's blocking reason, and the blocking band's wait arm
    /// (#3539 A2).</summary>
    public long MaxBlockDurationMs { get; set; }

    /// <summary>True when the spine holds the day at all. Together with <see cref="DataState"/> this decides
    /// the band's HasData: a held day that is purged, past the horizon or without a run record renders the calendar cell No-Data (grey)
    /// exactly as an absent day does (#3541 A9).</summary>
    public bool HasData { get; set; }

    /// <summary>Whether this row's counts are a measurement or the shape retention left behind (#3541 A9) —
    /// see <see cref="DailySummaryDataState"/>. Defaults to Collected so a row built without the reader
    /// (the tests' hand-built rows) bands as it always did.</summary>
    public DailySummaryDataState DataState { get; set; } = DailySummaryDataState.Collected;

    /// <summary>The horizon <see cref="DataState"/> was judged against; null on a row nobody judged.</summary>
    public DateTime? RetentionHorizon { get; set; }

    /// <summary>How many of the seven per-signal sources hold at least one row for the day (#3541 A9) — the
    /// aggregate's trailing <c>signal_sources_present</c> column, the fact that tells a purged shell from a
    /// day the purge has not reached.</summary>
    public int SignalSourcesPresent { get; set; }

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
        /* #3541 A9: a purged, past-horizon or run-record-less day is a NoData day to the band, whatever the spine still holds
           for it — its COALESCEd zeros are absences, and measured-zero-Healthy was the lie. */
        HasData = HasData && DataState == DailySummaryDataState.Collected,
        Deadlocks = DeadlockCount,
        CollectionErrors = CollectionErrors,
        CollectionRuns = CollectionRuns,
        HighCpuEvents = HighCpuEvents,
        BlockingEvents = BlockingEvents,
        /* #3539 A2: the blocking band's wait arm reads the longest block, so the peak rides in the signals. */
        PeakBlockWaitMs = MaxBlockDurationMs,
        MemoryPressureEvents = MemoryPressureEvents,
        MemoryCriticalEvents = MemoryCriticalEvents,
        AlertCount = AlertCount,
        /* #3525: a finished calendar day bands over its full 24 hours; the still-forming day clamps to
           its elapsed portion so an active storm is not diluted by hours that have not happened yet
           (review finding on #3525). Anchored MCP reads hand their window end; the calendar is live. */
        Window = DailyHealthBandCalculator.CalendarDayWindow(SummaryDate, ReferenceUtc),
    };
}
