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
using System.Threading;
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
/// <para><b>Why the range is a SIBLING rather than a wider get_daily_summary.</b> Three reasons, and the first
/// is mechanical. (1) The response SHAPE is the contract: get_daily_summary returns a flat object of scalars,
/// which is what a stat tile consumes and what its own description promises; a range returns rows. A single
/// tool that returned either depending on whether a span argument arrived would make every consumer branch on
/// a parameter it may not have sent. (2) They are different questions with different defaults: "how was
/// Tuesday" versus "which of the last thirty days were bad", the second of which is a screening read whose
/// answer is the shape of the month rather than one day's numbers. (3) get_daily_summary is a shipped name on
/// both SKUs; changing its payload shape would break callers for no gain. The two share the ONE aggregate
/// underneath (<c>DailySummarySql.RangeSql</c>), which is what stops them ever disagreeing about a day. The web
/// server page used to fetch both, the tile beside the month grid; since #3905 it draws the tile from the
/// range's anchor-day row, one fetch for two panels, which changes neither tool.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpHealthTools
{
    /// <summary>get_server_summary's description, VERBATIM Lite's (#3541 A10); the cross-SKU census pins them
    /// equal. Names the three clocks the payload carries, because the payload used to carry one.</summary>
    internal const string ServerSummaryDescription =
        "Gets a quick health overview for a SQL Server instance: current CPU %, memory usage, recent blocking count, and deadlock count. Use this for a fast health check before drilling into specific areas. THREE CLOCKS, NAMED: cpu_percent is the newest CPU snapshot and cpu_captured_at is its instant; memory_mb is the newest memory snapshot and memory_captured_at is its instant; last_collection is the newest collection of ANY collector for this server - the store's freshness, NOT the age of the two figures above, which can be far older when their own collectors have stopped. blocking_count and deadlock_count cover the counts_window_hours ending now.";

    [McpServerTool(Name = "get_server_summary"), Description(ServerSummaryDescription)]
    public static async Task<string> GetServerSummary(
        NpgsqlDataSource postgres,
        [Description("Server name or display name. Optional if only one server is configured.")] string? server_name = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        try
        {
            var summary = await DarlingHealthReader.GetServerSummaryAsync(postgres, resolved.ServerId, cancellationToken);
            if (summary.HasNoData)
                return McpHelpers.Status(
                    "unavailable",
                    $"No data available for {resolved.ServerName}. The collector may not have run yet.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                cpu_percent = summary.CpuPercent,
                /* #3541 A10: each latest figure carries ITS OWN clock. last_collection below is the newest
                   collection of ANY collector — a live collection log beside a dead CPU collector made a
                   day-old cpu_percent read as current, because the only stamp on the payload was fresh. */
                cpu_captured_at = summary.CpuCapturedAt?.ToString("o"),
                memory_mb = summary.MemoryMb,
                memory_captured_at = summary.MemoryCapturedAt?.ToString("o"),
                blocking_count = summary.BlockingCount,
                deadlock_count = summary.DeadlockCount,
                counts_window_hours = DarlingHealthReader.ServerSummaryCountsWindowHours,
                last_collection = summary.LastCollectionTime?.ToString("o")
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_server_summary", ex);
        }
    }

    [McpServerTool(Name = "get_daily_summary"), Description("Gets one day's health band (Healthy/Warning/Critical/NoData), wait time (sec), top wait, unique queries, deadlocks, blocking, high-CPU samples, memory pressure, errors, alerts for summary_date (UTC day, default today). status empty: no row for that day. Before retention_horizon: status unavailable, data_state purged, zeros are absences, no verdict; past_horizon if a signal table still holds the day (band NoData, non-zero counts real). Inside retention (collected or no_run_record) the band stands on real zeros, no_run_record too; only its collection-error share has no denominator. <<GUIDE>> Gets a daily health summary: overall composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events, memory pressure (and severe memory pressure), high-CPU samples, collection errors, and actionable alert count for one day. Use this for a quick overview to decide which areas need investigation. A day before the store's retention_horizon (the oldest day the shortest-lived signal table still holds) returns status=unavailable with data_state=purged rather than a health band: its per-signal counts would be COALESCEd zeros, not measurements, and a zero is only a measurement inside retention. A returned day carries data_state=collected (a verdict), past_horizon (before the horizon but some signal table still holds rows - the purge has not reached it; health_band=NoData, non-zero counts real) or no_run_record (inside retention, no collector run recorded - banded on the counts as read, which are measurements there; the collection-error share has no denominator). unique_queries is null (NOT 0) when the rollup tier that answers a day older than the raw window never materialized this server's day while the rollup's source still holds the day's rows - 'not carried at this tier', not 'no queries ran' - and days_missing names that day (empty otherwise); the service repairs such a day at its next start, and the band does not read this count.")]
    public static async Task<string> GetDailySummary(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Summary date, ISO-8601 yyyy-MM-dd ONLY (e.g. 2026-07-09), interpreted as a UTC day; any other spelling is refused rather than guessed at. Default is today.")] string? summary_date = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* #3541 A9: exact ISO-8601, refused otherwise — McpHelpers.ParseSummaryDate says why the general
           parse this replaced was the wrong tool in a file that already held as_of's strict allowlist. */
        var dateError = McpHelpers.ParseSummaryDate(summary_date, out var date);
        if (dateError != null) return dateError;

        try
        {
            var row = await DarlingHealthReader.GetDailySummaryAsync(postgres, resolved.ServerId, date, cancellationToken);

            /* #3541 A9: a day before the retention horizon is "unavailable" in the miss vocabulary's own
               sense — it existed and is not retrievable now — and it is told apart from a never-collected
               day because the two send a caller to different places (nowhere useful, versus collection
               health). What the spine still holds for it rides in the hints, named for what it is. */
            if (row.DataState == DailySummaryDataState.Purged)
                return McpHelpers.Status(
                    "unavailable",
                    $"{row.SummaryDate:yyyy-MM-dd} is before {resolved.ServerName}'s retention_horizon ({row.RetentionHorizon:yyyy-MM-dd}): the per-signal tables the health band reads (deadlocks, blocking, CPU, memory, waits) have been purged for that day, so no health verdict is possible and the counts would be zeros by construction, not by measurement. Longer-lived sources may still record the day — collection_runs and alert_count below are real where non-zero.",
                    new
                    {
                        summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                        overall_health = row.HealthBand.ToString(),
                        data_state = DailySummaryRetention.Label(row.DataState),
                        retention_horizon = row.RetentionHorizon?.ToString("yyyy-MM-dd"),
                        collection_runs = row.CollectionRuns,
                        alert_count = row.AlertCount,
                    });

            if (!row.HasData)
                return McpHelpers.Status(
                    "empty",
                    $"No data collected for {resolved.ServerName} on {row.SummaryDate:yyyy-MM-dd}.",
                    new { summary_date = row.SummaryDate.ToString("yyyy-MM-dd"), overall_health = row.HealthBand.ToString() });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                /* #3653 (A15/A16, one vocabulary): overall_health and health_band are the SAME band and now
                   carry the SAME spelling — the enum token (Healthy / Warning / Critical / NoData), which is
                   what every other band on the wire is spelled as (HealthSeverity, FleetHealthBand) and what
                   the web client builds its CSS classes from. overall_health used to publish the calculator's
                   human LABEL, and the two differed on exactly one value: "No Data" beside "NoData" in one
                   object. The label ("No Data") stays on the row type for the desktop viewers' tooltips; the
                   wire gets the token. overall_health is kept as a key because it is the older of the two
                   (the Dashboard's daily-summary column name) — removing it is a wire-shape change, not a
                   spelling. */
                overall_health = row.HealthBand.ToString(),
                health_band = row.HealthBand.ToString(),
                /* #3541 A9: collected, past_horizon or no_run_record here (purged returned above); the note
                   says what the zeros are on a non-collected day, null on a collected one. */
                data_state = DailySummaryRetention.Label(row.DataState),
                data_note = row.RetentionHorizon is { } horizon ? DailySummaryRetention.Note(row.DataState, horizon, row.SignalSourcesPresent) : null,
                retention_horizon = row.RetentionHorizon?.ToString("yyyy-MM-dd"),
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
                /* #3539 A2/A3, additive: the figures the band read that the counts alone cannot show — the
                   run total the error share is a share OF, and the blocking rate over the day's window (null
                   when the window was too short to normalise). The window is the row's own clamp against
                   its ReferenceUtc, so an anchored read rates against its as_of, not the process clock. */
                collection_runs = row.CollectionRuns,
                blocking_rate_per_hour = ServerHealthClassifier.BlockingRatePerHour(row.BlockingEvents, row.ToSignals().Window),
                /* #3653 A6, additive and trailing: the ONE key name the range tool spells the same way. A
                   unique_queries of null above means the rollup tier never carried this day for this server
                   (DarlingHealthReader.DailySummaryReadRow says what that is); this names the day so a caller
                   reading the null has the disclosure beside it rather than only in the description. Empty on
                   every carried day, which is every raw-tier day. */
                days_missing = row.UniqueQueries is null ? new[] { row.SummaryDate.ToString("yyyy-MM-dd") } : Array.Empty<string>(),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_daily_summary", ex);
        }
    }

    [McpServerTool(Name = "get_daily_summary_range"), Description("Gets one row per day over a span ending at as_of (default now), same per-day fields as get_daily_summary: health band, wait time (sec), queries, deadlocks, blocking, CPU, memory, errors, alerts. A day with ANY collection appears here even if quiet (Healthy), so a gap is a COLLECTION gap. status empty: no collected day in range but the server has history elsewhere; status unavailable: nothing was ever collected. Before retention_horizon: data_state purged rows' zeros are absences; past_horizon rows are real but a zero may be either; both health_band NoData, never Healthy. <<GUIDE>> Gets the daily health summary for a SPAN of days rather than one: one row per collected day, each with its composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events with the peak block wait, high-CPU samples, memory pressure, collection errors and actionable alert count. This is what the desktop viewer's Performance Calendar month grid draws, and it is the read to use when the question is WHICH day rather than how one day went - scan the bands, then call get_daily_summary for the day that stands out. A day on which anything at all was collected appears here even if every signal was quiet (that day is Healthy, not missing), so a gap in the returned days is a gap in COLLECTION - INSIDE RETENTION. The per-signal tables age out at the store's shortest retention while the collection log and alert log live longer, so retention_horizon is the oldest day every signal can still answer for; a returned day before it carries data_state=purged (no signal table holds it) or past_horizon (some still do - the purge has not reached it), health_band=NoData and a data_note, NEVER Healthy - a purged day's zeros are absences, and days_before_horizon counts both kinds. A day inside retention with no collector run recorded is data_state=no_run_record - it keeps its band (an alert-only day is Warning), with the caveat that the error share has no denominator. Purged and past_horizon rows carry no verdict. A window older than the raw query window answers unique_queries from a rollup tier; a day that tier never materialized for this server while the rollup's source still holds the day's rows carries unique_queries=null (NOT 0 - 'not carried at this tier', not 'no queries ran') and is listed in days_missing (empty when every day was carried); the service repairs such days at its next start, and the band does not read this count.")]
    public static async Task<string> GetDailySummaryRange(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Days of history, ending on the anchor day (inclusive). Default 30; max 366 (a year).")] int days_back = 30,
        [Description(McpHelpers.AsOfDaysDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        /* A year, not the calendar's month, because "how did last quarter look" is a real question — but
           bounded, because the aggregate underneath scans the RAW per-collection series for every signal
           except the query count (which routes to the rollup). The ceiling is SHARED with Lite so the two
           SKUs cannot accept different spans. */
        var daysError = McpHelpers.ValidateDaysBack(days_back, McpHelpers.MaxDailySummaryDaysBack);
        if (daysError != null) return daysError;

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

            /* The anchor is also the clock the still-forming day's window clamps against (#3525 review):
               a backdated as_of must clamp its own "today" against ITSELF, not the process clock. */
            /* #4232 ruling item 5: only a read "as of now" (no as_of given) uses the closed-day cache — a
               caller who pinned an explicit end time gets an unmemoized read of exactly that moment every time. */
            var range = await DarlingHealthReader.GetDailySummaryRangeAsync(
                postgres, resolved.ServerId, fromDate, toDate, referenceUtc: windowEnd, asOfNow: as_of is null, cancellationToken: cancellationToken);
            var rows = range.Rows;

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
                var everCollected = await DarlingDataReader.HasAnyCollectionLogAsync(postgres, resolved.ServerId, cancellationToken);
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
                /* Days the spine holds, not days in the span. The two differ exactly where collection has a
                   hole, and that difference is the most useful thing on this payload — read it together with
                   days_before_horizon, because a held day before the horizon is a shell, not a collected day. */
                day_count = rows.Count,
                /* #3541 A9: the store's horizon (reader clock, effective retention — see the reader) and how
                   many returned days fall before it. */
                retention_horizon = range.RetentionHorizon.ToString("yyyy-MM-dd"),
                days_before_horizon = rows.Count(row => row.DataState is DailySummaryDataState.Purged or DailySummaryDataState.PastHorizon),
                purged_day_count = rows.Count(row => row.DataState == DailySummaryDataState.Purged),
                collected_day_count = rows.Count(row => row.DataState == DailySummaryDataState.Collected),
                days = rows.Select(row => new
                {
                    summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                    /* #3653: one spelling for one band — see get_daily_summary. */
                    overall_health = row.HealthBand.ToString(),
                    health_band = row.HealthBand.ToString(),
                    data_state = DailySummaryRetention.Label(row.DataState),
                    data_note = DailySummaryRetention.Note(row.DataState, range.RetentionHorizon, row.SignalSourcesPresent),
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
                    /* #3539 A2/A3, additive — see get_daily_summary's members of the same names. */
                    collection_runs = row.CollectionRuns,
                    blocking_rate_per_hour = ServerHealthClassifier.BlockingRatePerHour(row.BlockingEvents, row.ToSignals().Window),
                }),
                /* #3653 A6, additive and TRAILING: the days whose unique_queries above is null — the rollup
                   tier that answered this window never carried them for this server while its source still
                   holds their rows (DarlingHealthReader.DailySummaryRangeReadResult.DaysMissing derives the
                   list from the rows, so it cannot disagree with the nulls). Empty when every day was carried,
                   which is every raw-tier window. One key name, spelled the same on get_daily_summary. */
                days_missing = range.DaysMissing.Select(day => day.ToString("yyyy-MM-dd")),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_daily_summary_range", ex);
        }
    }
}
