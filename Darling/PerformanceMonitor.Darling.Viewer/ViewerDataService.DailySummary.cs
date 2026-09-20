/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Performance Calendar / Daily Summary aggregate, grouped one row per day over a half-open
/// [fromDate, toDate) window and run on the <c>v_wait_stats</c> / <c>v_query_stats</c> / <c>v_deadlocks</c> /
/// <c>v_blocked_process_reports</c> (with the <c>v_dmv_blocking_snapshots</c> fallback) /
/// <c>v_cpu_utilization_stats</c> / <c>v_collection_log</c> / <c>v_memory_pressure_events</c> passthrough
/// views plus <c>config_alert_log</c>. One row is returned for every day that had any collection; days with
/// none are absent (the calendar renders them No-Data). Each row's composite band is computed by the shared
/// <see cref="DailyHealthBandCalculator"/> so it bands identically to Lite. This is the single source of
/// truth for the daily aggregate; <see cref="GetDailySummaryAsync"/> (one day) delegates here.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The daily-summary aggregate SQL. Single definition in <see cref="DailySummarySql"/>, shared with the
    /// service's <c>DarlingHealthReader</c> (the <c>get_daily_health</c> MCP tool) — the two used to keep
    /// hand-copied literals with nothing enforcing the copy (#1661).
    /// </summary>
    public const string DailySummaryRangeSql = DailySummarySql.RangeSql;

    /// <summary>The tier-routed form. See <see cref="DailySummarySql.RangeSqlFor(RetentionTier)"/>.</summary>
    public static string DailySummaryRangeSqlFor(RetentionTier tier) => DailySummarySql.RangeSqlFor(tier);

    /// <summary>The tier-routed form over a resolved hourly relation (#3653, Q12). See
    /// <see cref="DailySummarySql.RangeSqlFor(RetentionTier, string)"/>.</summary>
    public static string DailySummaryRangeSqlFor(RetentionTier tier, string hourlyRelation) => DailySummarySql.RangeSqlFor(tier, hourlyRelation);

    /// <summary>
    /// Returns one <see cref="DailySummaryRow"/> per collected day in the half-open [fromDate, toDate)
    /// window. Powers the Performance Calendar month grid.
    ///
    /// <para>#1661: routes the query-count CTE to the hourly or daily rollup when the window reaches past the raw
    /// retention horizon. Without this the calendar silently reported zero distinct queries for every day older
    /// than ~4 days once retention began dropping chunks. #1664: gated on the rollups actually existing — a
    /// plain-PostgreSQL store has none (and never drops raw, so raw is complete there); routing by age alone
    /// threw 42P01 at the user.</para>
    ///
    /// <para>#3653 (the viewer port of #3641): every returned day is judged against the store's retention
    /// horizon — <see cref="DailySummaryRetention.StateFor"/>, the ONE decision the MCP tool makes off the same
    /// SQL — and a purged or past-horizon day bands <see cref="DailyHealthBand.NoData"/> (grey) rather than
    /// Healthy. The spine names a day for as long as its longest-lived source (the collection log at 60 days,
    /// the alert log at 90) still holds it, while every signal the band reads is purged at 30; before this the
    /// calendar painted that second month green, with a tooltip saying "No issues detected." The horizon is
    /// computed by <see cref="DailySummaryHorizon"/> from the fleet retention overrides and the same constants
    /// the purge runs on, off the viewer's WALL CLOCK (a purge is a wall-clock event; the calendar's reads are
    /// always live, so there is no anchor to be tempted by).</para>
    /// </summary>
    public async Task<List<DailySummaryRow>> GetDailySummaryRangeAsync(
        int serverId, DateTime fromDate, DateTime toDate, CancellationToken cancellationToken = default)
        => (await ReadDailySummaryRangeAsync(serverId, fromDate, toDate, cancellationToken)).Rows;

    /// <summary>The range read plus the horizon its rows were judged against, so the single-day path can
    /// judge an ABSENT day without reading the horizon a second time (review note on #3661).</summary>
    private async Task<(List<DailySummaryRow> Rows, DateTime RetentionHorizon)> ReadDailySummaryRangeAsync(
        int serverId, DateTime fromDate, DateTime toDate, CancellationToken cancellationToken)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, fromDate, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        /* #3525: the deadlock-rate tiers the day band evaluates, read ONCE per range rather than per row —
           the fleet roll-up's own hoist argument: a settings save mid-read must not band some days on the
           old pair and the rest on the new one. One read per month navigation, off the Overview's existing
           settings-row read. */
        var banding = new DailyHealthThresholds
        {
            DeadlockRates = await GetDeadlockRateThresholdsAsync(cancellationToken),
        };

        /* #3653: the horizon, read once per range like the banding tiers above — a schedule save mid-read must
           not judge some days against one horizon and the rest against another. BaselineMath.BaselineWindowDays
           is the floor the purge applies to the baseline-serving raw collectors; it is passed because the
           Analysis assembly that owns it is one Storage does not reference. */
        var horizon = await ReadRetentionHorizonAsync(cancellationToken);

        /* #3653 (Q12): tier over the legacy pair above; the hourly RELATION by the supply rule — the
           interval-honest successor where it reaches as far back as the legacy for this window, so the calendar
           and get_daily_health (DarlingHealthReader, same call) count the same queries for the same day. */
        await using var command = _dataSource.CreateCommand(DailySummaryRangeSqlFor(tier, coverage.HourlyRelationFor(TimescaleSupport.QueryStatsHourlyView, fromDate)));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(fromDate.Date, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(toDate.Date, DateTimeKind.Unspecified) });

        var results = new List<DailySummaryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadDailySummaryRow(reader, banding, horizon));
        }

        return (results, horizon);
    }

    /// <summary>
    /// Daily summary for one server on a specific date (or today, UTC, when <paramref name="summaryDate"/>
    /// is null). Delegates to the range query for a single day; returns a No-Data row when the day had no
    /// collection.
    /// </summary>
    public async Task<DailySummaryRow?> GetDailySummaryAsync(int serverId, DateTime? summaryDate = null, CancellationToken cancellationToken = default)
    {
        var targetDate = summaryDate?.Date ?? DateTime.UtcNow.Date;
        var (rows, horizon) = await ReadDailySummaryRangeAsync(serverId, targetDate, targetDate.AddDays(1), cancellationToken);
        if (rows.Count > 0)
        {
            return rows[0];
        }

        /* #3653: a day the spine does not hold is not "collected" either — before the horizon it is purged
           (nothing names it any more), inside it there is simply no run record — so the absent row is judged
           too, the way the MCP single-day tool judges its absent day, against the horizon the range read
           already computed. */
        return new DailySummaryRow
        {
            SummaryDate = targetDate,
            HasData = false,
            HealthBand = DailyHealthBand.NoData,
            DataState = DailySummaryRetention.StateFor(targetDate, 0, 0, horizon),
            RetentionHorizon = horizon,
        };
    }

    /// <summary>
    /// The store's daily-summary retention horizon as of NOW (#3653): the oldest UTC day every signal source
    /// still holds, from the shortest effective fleet retention among the aggregate's sources —
    /// <see cref="DailySummaryHorizon"/>, the computation the MCP health reader runs, over
    /// <see cref="DailySummaryRetention.HorizonFor"/>'s date arithmetic.
    /// </summary>
    internal async Task<DateTime> ReadRetentionHorizonAsync(CancellationToken cancellationToken)
    {
        var fleetOverrideDays = await DailySummaryHorizon.ReadFleetRetentionOverrideDaysAsync(
            _dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken);
        var shortestRetentionDays = DailySummaryHorizon.ShortestSignalRetentionDays(fleetOverrideDays, BaselineMath.BaselineWindowDays);
        return DailySummaryRetention.HorizonFor(DateTime.UtcNow, shortestRetentionDays);
    }

    private static DailySummaryRow ReadDailySummaryRow(DbDataReader reader, DailyHealthThresholds banding, DateTime retentionHorizon)
    {
        var row = new DailySummaryRow
        {
            SummaryDate = reader.IsDBNull(0) ? DateTime.MinValue : Convert.ToDateTime(reader.GetValue(0)),
            TotalWaitTimeSec = reader.IsDBNull(1) ? 0m : Convert.ToDecimal(reader.GetValue(1)),
            TopWaitType = reader.IsDBNull(2) ? "" : reader.GetString(2),
            /* #3653 A6: a NULL unique_queries is the routed statement's "not carried at this tier" (the rollup
               never materialized this server's day while its source still holds the rows) and is KEPT as null
               — the 0L this used to substitute drew a measured-zero on the day-detail panel for a day the tier
               skipped. The MCP reader (DarlingHealthReader) makes the same read at the same ordinal. */
            UniqueQueries = reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3)),
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
            /* #3541 A9 / #3653: the aggregate's LAST column, how many of the seven signal sources hold a row for
               the day — the fact that tells a purged shell from a day the purge has not reached. */
            SignalSourcesPresent = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13)),
            HasData = true,
            RetentionHorizon = retentionHorizon,
        };
        /* Judged AFTER the counts are read, from the same three facts the MCP reader judges on: the day, its run
           count, its signal presence — against the horizon. ToSignals folds the state into HasData, so Classify
           bands a purged day NoData without knowing why. */
        row.DataState = DailySummaryRetention.StateFor(row.SummaryDate, row.CollectionRuns, row.SignalSourcesPresent, retentionHorizon);
        row.HealthBand = DailyHealthBandCalculator.Classify(row.ToSignals(), banding);
        return row;
    }
}

/// <summary>
/// The Daily Summary grid row, structurally identical to Lite's <c>DailySummaryRow</c>. The composite
/// <see cref="HealthBand"/> is computed by the shared <see cref="DailyHealthBandCalculator"/>.
/// </summary>
public class DailySummaryRow
{
    public DateTime SummaryDate { get; set; }

    /// <summary>The clock the still-forming day's window clamps against (#3525 review). The viewer's
    /// calendar reads are always live, so the wall-clock default is the correct reference.</summary>
    public DateTime ReferenceUtc { get; set; } = DateTime.UtcNow;
    public decimal TotalWaitTimeSec { get; set; }
    public string TopWaitType { get; set; } = "";

    /// <summary>Distinct queries seen that day, or <c>null</c> when the rollup tier that answered the window
    /// never carried this server's day (#3653 A6) — "not materialized", which the day-detail panel renders as
    /// such rather than as 0. Lite's row keeps <c>long</c>: it has no rollup tier, so raw is always the
    /// source, and a day inside retention is measured by construction.</summary>
    public long? UniqueQueries { get; set; }
    public long DeadlockCount { get; set; }
    public long BlockingEvents { get; set; }
    public long HighCpuEvents { get; set; }
    public long MemoryPressureEvents { get; set; }
    public long MemoryCriticalEvents { get; set; }
    public long CollectionErrors { get; set; }

    /// <summary>Collector runs of every status that day (#3539 A2) — the denominator the collection-error
    /// share bands on, read from the shared SQL's trailing <c>collection_runs</c> column.</summary>
    public long CollectionRuns { get; set; }
    public long AlertCount { get; set; }

    /// <summary>The day's peak/max block wait in ms (0 when no blocking, or blocking from a source without a
    /// wait time). Surfaced on the day-detail panel's blocking reason, and the blocking band's wait arm
    /// (#3539 A2).</summary>
    public long MaxBlockDurationMs { get; set; }

    /// <summary>True when the spine holds the day at all. Together with <see cref="DataState"/> this decides
    /// the band's HasData: a held day that is purged or past the horizon renders the calendar cell No-Data (grey)
    /// exactly as an absent day does (#3541 A9, #3653) — Lite's <c>DailySummaryRow</c> shape.</summary>
    public bool HasData { get; set; }

    /// <summary>Whether this row's counts are a measurement or the shape retention left behind (#3541 A9) —
    /// see <see cref="DailySummaryDataState"/>. Defaults to Collected so a row built without the reader (a
    /// hand-built test row) bands as it always did.</summary>
    public DailySummaryDataState DataState { get; set; } = DailySummaryDataState.Collected;

    /// <summary>The horizon <see cref="DataState"/> was judged against; null on a row nobody judged.</summary>
    public DateTime? RetentionHorizon { get; set; }

    /// <summary>How many of the seven per-signal sources hold at least one row for the day (#3541 A9) — the
    /// aggregate's trailing <c>signal_sources_present</c> column.</summary>
    public int SignalSourcesPresent { get; set; }

    /// <summary>The composite health band that colors this day's calendar cell.</summary>
    public DailyHealthBand HealthBand { get; set; } = DailyHealthBand.NoData;

    /// <summary>Human label for the band ("Healthy" / "Warning" / "Critical" / "No Data"), shown in the day detail.</summary>
    public string OverallHealth => DailyHealthBandCalculator.Label(HealthBand);

    public string SummaryDateFormatted => SummaryDate.ToString("yyyy-MM-dd");
    public string TotalWaitFormatted => TotalWaitTimeSec < 1000
        ? $"{TotalWaitTimeSec:N1} s"
        : $"{TotalWaitTimeSec / 60:N1} min";

    /// <summary>Multi-line hover text summarizing the day's signals, for the calendar cell tooltip — with the
    /// retention state spoken (#3653): a purged cell says retention took the day, not "No data collected."</summary>
    public string SignalsTooltip => DailyHealthBandCalculator.Describe(ToSignals(), DataState, RetentionHorizon, SignalSourcesPresent);

    /// <summary>Projects this row's counts into the shared banding input.</summary>
    public DailyHealthSignals ToSignals() => new()
    {
        /* #3541 A9 / #3653: a purged or past-horizon day is a NoData day to the band, whatever the spine still
           holds for it — its COALESCEd zeros may be absences, and measured-zero-Healthy was the lie. Inside
           retention (Collected, NoRunRecord) a zero IS a measurement and the band stands. The same fold Lite's
           row and the MCP reader's row make. */
        HasData = HasData && DataState is not (DailySummaryDataState.Purged or DailySummaryDataState.PastHorizon),
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
           (review finding on #3525). The viewer's reads are live, so the wall clock is the reference. */
        Window = DailyHealthBandCalculator.CalendarDayWindow(SummaryDate, ReferenceUtc),
    };
}
