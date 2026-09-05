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

    /// <summary>The tier-routed form. See <see cref="DailySummarySql.RangeSqlFor"/>.</summary>
    public static string DailySummaryRangeSqlFor(RetentionTier tier) => DailySummarySql.RangeSqlFor(tier);

    /// <summary>
    /// Returns one <see cref="DailySummaryRow"/> per collected day in the half-open [fromDate, toDate)
    /// window. Powers the Performance Calendar month grid.
    ///
    /// <para>#1661: routes the query-count CTE to the hourly or daily rollup when the window reaches past the raw
    /// retention horizon. Without this the calendar silently reported zero distinct queries for every day older
    /// than ~4 days once retention began dropping chunks. #1664: gated on the rollups actually existing — a
    /// plain-PostgreSQL store has none (and never drops raw, so raw is complete there); routing by age alone
    /// threw 42P01 at the user.</para>
    /// </summary>
    public async Task<List<DailySummaryRow>> GetDailySummaryRangeAsync(
        int serverId, DateTime fromDate, DateTime toDate, CancellationToken cancellationToken = default)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, fromDate, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        await using var command = _dataSource.CreateCommand(DailySummaryRangeSqlFor(tier));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(fromDate.Date, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(toDate.Date, DateTimeKind.Unspecified) });

        var results = new List<DailySummaryRow>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadDailySummaryRow(reader));
        }

        return results;
    }

    /// <summary>
    /// Daily summary for one server on a specific date (or today, UTC, when <paramref name="summaryDate"/>
    /// is null). Delegates to the range query for a single day; returns a No-Data row when the day had no
    /// collection.
    /// </summary>
    public async Task<DailySummaryRow?> GetDailySummaryAsync(int serverId, DateTime? summaryDate = null, CancellationToken cancellationToken = default)
    {
        var targetDate = summaryDate?.Date ?? DateTime.UtcNow.Date;
        var rows = await GetDailySummaryRangeAsync(serverId, targetDate, targetDate.AddDays(1), cancellationToken);
        return rows.Count > 0
            ? rows[0]
            : new DailySummaryRow { SummaryDate = targetDate, HasData = false, HealthBand = DailyHealthBand.NoData };
    }

    private static DailySummaryRow ReadDailySummaryRow(DbDataReader reader)
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

/// <summary>
/// The Daily Summary grid row, structurally identical to Lite's <c>DailySummaryRow</c>. The composite
/// <see cref="HealthBand"/> is computed by the shared <see cref="DailyHealthBandCalculator"/>.
/// </summary>
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
    };
}
