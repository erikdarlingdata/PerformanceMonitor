/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The health-overview MCP tools — get_server_summary (Lite's one-shot per-server health), get_daily_summary
/// (the Dashboard's daily rollup for ONE day) and get_daily_summary_range (#2484: the same rollup across a
/// span of days, which is what the Performance Calendar's month grid draws) — served over Darling's Postgres
/// store, the same names those SKUs expose. All read through <see cref="DarlingHealthReader"/> (STORED reads,
/// no live monitored-server hit). get_server_summary is the fast "is this server OK" check — current SQL CPU,
/// memory, recent blocking, recent deadlocks — before drilling in; the two daily reads fold a day's signals
/// into the SHARED composite health band (<c>DailyHealthBandCalculator</c>), so an agent gets the same
/// Healthy / Warning / Critical verdict the Performance Calendar shows.
///
/// <para><b>Why the range is a SIBLING rather than a wider get_daily_summary.</b> Four reasons, and the first
/// two are mechanical. (1) The response SHAPE is the contract: get_daily_summary returns a flat object of
/// scalars, which is what a stat tile consumes and what its own description promises; a range returns rows. A
/// single tool that returned either depending on whether a span argument arrived would make every consumer
/// branch on a parameter it may not have sent, and the web stat panel would simply stop rendering. (2) The web
/// server page must not fetch one read twice — there is a pin for it — and the Overview tab already reads
/// get_daily_summary for today's band, so the month grid beside it CANNOT be the same read. (3) They are
/// different questions with different defaults: "how was Tuesday" versus "which of the last thirty days were
/// bad", the second of which is a screening read whose answer is the shape of the month rather than one day's
/// numbers. (4) get_daily_summary is a shipped name on both SKUs; changing its payload shape would break
/// callers for no gain. The two share the ONE aggregate underneath (<c>DailySummarySql.RangeSql</c>), which is
/// what stops them ever disagreeing about a day.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpHealthTools
{
    [McpServerTool(Name = "get_server_summary"), Description("Gets a quick health overview for a SQL Server instance: current CPU %, memory usage, recent blocking count, and deadlock count. Use this for a fast health check before drilling into specific areas.")]
    public static async Task<string> GetServerSummary(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Optional if only one server is configured.")] string? server_name = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        try
        {
            var summary = await DarlingHealthReader.GetServerSummaryAsync(postgres, resolved.ServerId);
            if (summary.HasNoData)
                return McpHelpers.Status(
                    "unavailable",
                    $"No data available for {resolved.ServerName}. The collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                cpu_percent = summary.CpuPercent,
                memory_mb = summary.MemoryMb,
                blocking_count = summary.BlockingCount,
                deadlock_count = summary.DeadlockCount,
                last_collection = summary.LastCollectionTime?.ToString("o")
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_summary", ex);
        }
    }

    [McpServerTool(Name = "get_daily_summary"), Description("Gets a daily health summary: overall composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events, memory pressure (and severe memory pressure), high-CPU samples, collection errors, and actionable alert count for one day. Use this for a quick overview to decide which areas need investigation.")]
    public static async Task<string> GetDailySummary(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Summary date (yyyy-MM-dd), interpreted as a UTC day. Default is today.")] string? summary_date = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        DateTime? date = null;
        if (!string.IsNullOrEmpty(summary_date))
        {
            if (!DateTime.TryParse(summary_date, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var parsed))
                return $"Invalid date format '{summary_date}'. Use yyyy-MM-dd format (e.g., 2026-07-09).";
            date = parsed;
        }

        try
        {
            var row = await DarlingHealthReader.GetDailySummaryAsync(postgres, resolved.ServerId, date);
            if (!row.HasData)
                return McpHelpers.Status(
                    "empty",
                    $"No data collected for {resolved.ServerName} on {row.SummaryDate:yyyy-MM-dd}.",
                    new { summary_date = row.SummaryDate.ToString("yyyy-MM-dd"), overall_health = row.OverallHealth });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                overall_health = row.OverallHealth,
                health_band = row.HealthBand.ToString(),
                total_wait_time_sec = row.TotalWaitTimeSec,
                top_wait_type = row.TopWaitType,
                unique_queries = row.UniqueQueries,
                deadlock_count = row.DeadlockCount,
                blocking_events = row.BlockingEvents,
                high_cpu_events = row.HighCpuEvents,
                memory_pressure_events = row.MemoryPressureEvents,
                memory_critical_events = row.MemoryCriticalEvents,
                collection_errors = row.CollectionErrors,
                alert_count = row.AlertCount,
                max_block_duration_ms = row.MaxBlockDurationMs
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_daily_summary", ex);
        }
    }

    [McpServerTool(Name = "get_daily_summary_range"), Description("Gets the daily health summary for a SPAN of days rather than one: one row per collected day, each with its composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events with the peak block wait, high-CPU samples, memory pressure, collection errors and actionable alert count. This is what the desktop viewer's Performance Calendar month grid draws, and it is the read to use when the question is WHICH day rather than how one day went — scan the bands, then call get_daily_summary for the day that stands out. A day on which anything at all was collected appears here even if every signal was quiet (that day is Healthy, not missing), so a gap in the returned days is a gap in COLLECTION.")]
    public static async Task<string> GetDailySummaryRange(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Days of history, ending on the anchor day (inclusive). Default 30; max 366 (a year).")] int days_back = 30,
        [Description(McpHelpers.AsOfDaysDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        /* A year, not the calendar's month, because "how did last quarter look" is a real question — but
           bounded, because the aggregate underneath scans the RAW per-collection series for every signal
           except the query count (which routes to the rollup). The ceiling is SHARED with Lite so the two
           SKUs cannot accept different spans. */
        if (days_back <= 0 || days_back > McpHelpers.MaxDailySummaryDaysBack)
            return $"Invalid days_back value '{days_back}'. Must be a positive integer (1-{McpHelpers.MaxDailySummaryDaysBack}).";

        /* The anchor is the ONLY source of "now" in this body — see AsOfWindowAnchorTests, which fails a tool
           that advertises as_of and then reads the process clock anyway. (That check is a source scan and
           the rule is absolute, so this comment cannot name the property either.) The resolver returns the
           present when the caller sent nothing, which is exactly the pre-anchor behaviour. */
        var anchorError = McpHelpers.ResolveAsOf(as_of, out var windowEnd);
        if (anchorError != null) return anchorError;

        try
        {
            /* Days, not hours: the anchor names a DAY here and only its UTC date is used, because the
               aggregate buckets on date_trunc('day', ...) and a half-day is not a row it can return. The
               range is half-open [from, to) over whole days, so `days_back` days ending ON the anchor day
               means the anchor day is the last one included rather than the first one excluded. */
            var lastDay = windowEnd.Date;
            var fromDate = lastDay.AddDays(-(days_back - 1));
            var toDate = lastDay.AddDays(1);

            var rows = await DarlingHealthReader.GetDailySummaryRangeAsync(
                postgres, resolved.ServerId, fromDate, toDate);

            if (rows.Count == 0)
            {
                /*
                    Zero DAYS is two facts. The aggregate's day spine is a UNION over nine sources and one of
                    them is the collection log, where ANY run marks the day collected — that is why a quiet
                    but monitored day comes back Healthy rather than absent. So a range with no rows at all
                    cannot be "the server was quiet"; it is either a range that predates this server's
                    history, or a server nothing has ever been collected for.

                    The denominator is therefore the DATA, probed on v_collection_log — the spine member that
                    guarantees a collected day appears at all. collection_log is PERIODIC: every collector run
                    writes a row whatever it found, so its presence is proof somebody looked, and unlike an
                    edge table it cannot report a healthy server as uncollected.
                */
                var everCollected = await DarlingDataReader.HasAnyCollectionLogAsync(postgres, resolved.ServerId);
                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"No collected days for {resolved.ServerName} between {fromDate:yyyy-MM-dd} and {lastDay:yyyy-MM-dd}. A day with ANY collection appears here even when every signal was quiet, so this range is outside what the store holds for this server rather than a stretch of quiet days — widen days_back, or move as_of.",
                        new { from_date = fromDate.ToString("yyyy-MM-dd"), to_date = lastDay.ToString("yyyy-MM-dd") })
                    : McpHelpers.Status(
                        "unavailable",
                        $"No collector runs have EVER been recorded for {resolved.ServerName}, so the calendar is empty because nothing has been collected — not because those days were quiet. Check that the service is running and that the server is enabled for collection.",
                        new { from_date = fromDate.ToString("yyyy-MM-dd"), to_date = lastDay.ToString("yyyy-MM-dd") });
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                days_back,
                /* The bounds the read actually used, echoed back: with an anchor in play, a caller cannot
                   otherwise tell which days they were given from the days they got. */
                from_date = fromDate.ToString("yyyy-MM-dd"),
                to_date = lastDay.ToString("yyyy-MM-dd"),
                /* Days WITH data, not days in the span. The two differ exactly where collection has a hole,
                   and that difference is the most useful thing on this payload. */
                day_count = rows.Count,
                days = rows.Select(row => new
                {
                    summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                    overall_health = row.OverallHealth,
                    health_band = row.HealthBand.ToString(),
                    total_wait_time_sec = row.TotalWaitTimeSec,
                    top_wait_type = row.TopWaitType,
                    unique_queries = row.UniqueQueries,
                    deadlock_count = row.DeadlockCount,
                    blocking_events = row.BlockingEvents,
                    high_cpu_events = row.HighCpuEvents,
                    memory_pressure_events = row.MemoryPressureEvents,
                    memory_critical_events = row.MemoryCriticalEvents,
                    collection_errors = row.CollectionErrors,
                    alert_count = row.AlertCount,
                    max_block_duration_ms = row.MaxBlockDurationMs,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_daily_summary_range", ex);
        }
    }
}
