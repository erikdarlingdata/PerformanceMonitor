/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_query_store_clutter</c> (#3797) — the Query Store clutter view, over rows the collectors already
/// write and nothing else. The maintainer's design intent: <i>some sort of view of Query Store clutter,
/// reachable by the MCP, web and WPF viewers; not a new query — analyse what we've already collected.</i>
/// This is the MCP surface. The reader and the composition beneath it live in
/// <c>PerformanceMonitor.Darling.Storage</c> (<see cref="DarlingQueryStoreClutterReader"/> /
/// <see cref="QueryStoreClutter"/>) beside the <c>DarlingPg*Reader</c> family and for the same reason
/// (#2530): the web server tab reads THIS tool, and the WPF Viewer's Query Store Clutter grid runs the same
/// query text and the same composition in-process, so neither surface carries a second copy of the SQL or a
/// second spelling of a verdict.
///
/// <para><b>The case that motivated it (2026-09-20, the largest production store).</b> One database out of
/// dozens carried 92–96% of the <c>query_store</c> collector's per-database read time; the plan dimension
/// on the second store class was taking ~755k new distinct plans a day; and nobody could say from the
/// product's own surfaces which databases were cluttered, why, or what it was costing the servers hosting
/// them. Every input for that answer was already in the store: the fan-out rollup on <c>collection_log</c>
/// (#2472), the plan identities on <c>query_store_stats</c>, the options row in <c>query_store_health</c>,
/// the <c>QDS_*</c> deltas in <c>wait_stats</c> and the <c>MEMORYCLERK_QUERYDISKSTORE</c> row in
/// <c>memory_clerks</c>. This tool composes them.</para>
///
/// <para><b>Honest granularity, stated on the face.</b> Clutter is per DATABASE — the Query Store catalog is
/// per database, the collector fans out per database, the options are per database — so the rows are
/// databases, each carrying three decomposed arms with raw numbers. Overhead is per SERVER — the wait types
/// and the memory clerk that measure Query Store's own cost are instance-wide — so it is ONE block, and the
/// payload never pretends to attribute it to a database.</para>
///
/// <para><b>Decomposed arms, not a score</b> (<see cref="QueryStoreClutter"/>): the per-database verdict is
/// the band canon (#3703) with the reason tokens that raised it and the thresholds published beside them,
/// and the rows are ordered by that band and then by the arms' own figures. A weighted number would have
/// needed weights nobody could audit from the row.</para>
///
/// <para><b>Replicas are excluded by architecture, with the reason on the row</b> (topology ruling,
/// 2026-09-20). Query Store on a readable secondary is READ_ONLY by design — <c>readonly_reason</c> bit 8,
/// the engine's own mechanism-agnostic flag, the one <c>QueryStoreCollector</c> gates on — so a replica's
/// clutter and configuration are its primary's. Such a database reads <c>excluded: true</c>,
/// <c>excluded_reason: qs_read_only_replica</c>, verdict Unknown, and its per-server overhead block is
/// still reported because waits and clerks are real on a replica. Its READ_ONLY is never a defect.</para>
///
/// <para><b>Capture mode is read, not stubbed.</b> V137 (#3796) added <c>query_capture_mode</c> to
/// <c>query_store_health</c> and the collector fills it hourly, so the config arm publishes the mode and
/// <c>capture_mode_known</c> says whether the newest capture carried one. A NULL is a row written before that
/// rung — never asked — and is published that way rather than as <c>NONE</c>. The churn recommendations name
/// the mode they are looking at: <c>ALL</c> beside churn is the switch-to-AUTO case, <c>AUTO</c> beside it
/// points at the workload.</para>
///
/// <para><b>What it does NOT do.</b> No Lite twin: Lite has every input and no port, which is why
/// <c>get_query_store_clutter</c> is Darling-only by decision
/// rather than by omission. The fleet reference is opt-in
/// (<c>include_fleet_median</c>): it costs a fleet-wide walk of two raw hypertables over the window, which
/// the default call keeps off so the per-server answer stays inside the read deadline (30 s client-side on
/// the shared storage reader, 15 s <c>statement_timeout</c> on the managed store's <c>mcp</c> role).</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryStoreClutterTools
{
    /// <summary>Default per-database rows. A server rarely has more Query-Store-bearing databases than this,
    /// so the default page is usually the whole list; <c>truncated</c> says when it is not.</summary>
    public const int DefaultLimit = 50;

    /// <summary>The sentence beside <c>query_capture_mode</c>, naming what a null there means. A null is a
    /// health row captured before the V137 rung (#3796) created the column — the mode was never asked for —
    /// and a reader must not read it as <c>NONE</c>, which is a mode the engine really can be in.</summary>
    public const string CaptureModeNote =
        "query_capture_mode is a query_store_health column that is filled hourly; null means the newest capture predates the column, so the mode was never asked for, and it is not NONE";

    /// <summary>#9958: the read bar, in seconds (#8570): the time a call is held to. The fleet reference gets what is left of it
    /// after the per-server reads (<see cref="FleetWait"/>).</summary>
    internal const int ReadBarSeconds = QueryStoreClutterFleetReference.KeepAboveSeconds;

    /// <summary>#9958: the least the call waits for the fleet reference, in seconds. Five statements over a small store finish well
    /// inside it, so a call whose per-server reads used the whole bar still gets a reference that is nearly done, and the wait is
    /// never zero or negative, which a timer cannot be armed with. A call that has already used the bar goes over it by at most this.</summary>
    internal const double FleetWaitFloorSeconds = 1;

    /// <summary>#9958: how long the call waits for the fleet computation: what is left of <see cref="ReadBarSeconds"/> after
    /// <paramref name="elapsed"/>, the time the call has used on its per-server reads, and never less than
    /// <see cref="FleetWaitFloorSeconds"/>. The computation is bounded by its own cap and not by this wait (#9882), so a shorter
    /// wait costs the computation nothing: it goes on, and a later call in the clock hour gets what it finds.</summary>
    internal static TimeSpan FleetWait(TimeSpan elapsed)
    {
        var left = TimeSpan.FromSeconds(ReadBarSeconds) - elapsed;
        var floor = TimeSpan.FromSeconds(FleetWaitFloorSeconds);
        return left > floor ? left : floor;
    }

    /// <summary>#9882: the fleet reference is not in this answer and its computation goes on (<see cref="QueryStoreClutterFleetReference"/>).</summary>
    internal const string FleetOverrunNote =
        "The fleet reference ran over its time budget and is not in this answer. The per-server figures above are complete. It is still being computed and, once done, is kept for the rest of the clock hour: ask again in about a minute, try a shorter hours_back, or leave include_fleet_median off.";

    /// <summary>#9882: no computation was started for this window, because the store's one running computation is for another.</summary>
    internal const string FleetBusyNote =
        "The fleet reference for this window is not in this answer and was not started: one fleet reference is computed at a time on this store, and another window's is still being computed. The per-server figures above are complete. Ask again in about a minute, or leave include_fleet_median off.";

    /// <summary>#9882: the computation for this window ended without a reference, and calls for it answer this at once until the clock hour ends.</summary>
    internal const string FleetUnfinishedNote =
        "The fleet reference for this window did not finish inside its time cap and is not tried again this clock hour. The per-server figures above are complete. Try a shorter hours_back, or leave include_fleet_median off.";

    /// <summary>Test seam (#9478): runs at the start of the fleet arm and is handed the fleet budget's source, so a test can fire the
    /// bound without waiting for it. Unset in production.</summary>
    internal static readonly AsyncLocal<Func<CancellationTokenSource, Task>?> FleetReadSeam = new();

    /// <summary>Test seam (#9958): told the wait the call gives the fleet computation, as <see cref="FleetWait"/> worked it out from the
    /// time the call had used. Unset in production.</summary>
    internal static readonly AsyncLocal<Action<TimeSpan>?> FleetWaitSeam = new();

    /// <summary>Test seam (#8570): runs at the start of each of the three reads that start together ("waits", "clerk",
    /// "discontinuities") and is handed the read's name and its token, so a test can hold the three in flight at once or
    /// fail one. Unset in production.</summary>
    internal static readonly AsyncLocal<Func<string, CancellationToken, Task>?> IndependentReadSeam = new();

    /// <summary>#8570: one of the reads that need only the server id and the window. Each takes its own pooled connection and
    /// touches no state the other reads share, so they start together and the call waits as long as the slowest rather than the sum.</summary>
    private static async Task<T> RunIndependentReadAsync<T>(string name, Func<CancellationToken, Task<T>> read, CancellationToken cancellationToken)
    {
        if (IndependentReadSeam.Value is { } seam) await seam(name, cancellationToken);
        return await read(cancellationToken);
    }

    /// <summary>#8570: stops the started reads and waits for them, so none outlives the call and none leaves an unobserved
    /// exception behind. A read already awaited is finished and costs nothing here. What a read threw is dropped: the call is
    /// either returning its own answer or already carrying a failure that came first.</summary>
    private static async Task SettleIndependentReadsAsync(CancellationTokenSource? source, params Task?[] reads)
    {
        if (source is null) return;
        try
        {
            source.Cancel();
            foreach (var read in reads)
            {
                if (read is null) continue;
                try { await read; }
                catch (Exception) { /* observed; see the summary */ }
            }
        }
        finally
        {
            source.Dispose();
        }
    }

    /// <summary>True when the fleet budget fired and the caller did not: the cancellation, or the PostgreSQL <c>57014</c> that
    /// the cancel request raises. The caller's own cancellation is not an overrun and propagates as before.</summary>
    private static bool IsFleetOverrun(Exception ex, CancellationToken caller, CancellationToken fleet)
        => !caller.IsCancellationRequested
           && fleet.IsCancellationRequested
           && (ex is OperationCanceledException || (ex is PostgresException pg && pg.SqlState == "57014"));

    /* The four arms of the memory clerk's `note`. The web page's plain-text.js rewrites the field and relation names in them, and
       WebPlainTextTests feeds these constants through it, so a reword here breaks that test (#6649). */
    internal const string ClerkNoteNoCapture =
        "No memory_clerks capture in the window: the collector wrote nothing for this server, so nothing is known about the clerk.";

    internal const string ClerkNoteNeverAppeared =
        "The clerk never appeared in the window. memory_clerks stores the top 25 clerks over 1 MB per capture, so this is a RANK, not a zero: Query Store's memory was below the collector's floor or outside its top 25 on every capture.";

    internal const string ClerkNoteInNewestCapture =
        "The clerk is in the newest capture's top 25; latest_memory_mb is its current footprint.";

    internal const string ClerkNoteDroppedOut =
        "The clerk appeared earlier in the window and has since dropped out of the collector's top 25; latest_memory_mb is its last observed footprint, not its current one.";

    /// <summary>
    /// #9382: the window note, naming each arm that holds less than the window and how many hours that arm holds. A null start means that arm
    /// covers the whole window. The web Queries tab does not show this note: each panel says its own arm's span from the window block's
    /// per-arm fields.
    /// </summary>
    private static string WindowTruncationNote(DateTime? planChurnStart, DateTime? waitStart, DateTime now)
    {
        static string Hours(DateTime start, DateTime now) =>
            Math.Round((now - start).TotalHours, 1).ToString("0.#", CultureInfo.InvariantCulture);

        var arms = new List<string>();
        var detail = new List<string>();
        if (planChurnStart is { } churn)
        {
            arms.Add("the plan-churn arm");
            detail.Add($"The plan-churn arm holds {Hours(churn, now)} hours of the window.");
        }

        if (waitStart is { } wait)
        {
            arms.Add("the wait arm");
            detail.Add($"The wait arm holds {Hours(wait, now)} hours of the window.");
        }

        return "The window reaches further back than this server's stored data for " + string.Join(" and ", arms) + ". "
            + string.Join(" ", detail)
            + " The read-cost and config arms read tables with longer retention and cover the whole window.";
    }

    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_query_store_clutter"), Description(
        "The Query Store CLUTTER view for one server, composed from already-collected rows: no new query runs. Clutter (read_cost/plan_churn/config) is per DATABASE; qs_overhead (waits/memory clerk) is per SERVER, never attributed to a database. No rows answers unavailable, never a clean bill. Verdict Unknown means unmeasured, never quietly Healthy. REPLICAS: excluded, verdict Unknown, never a defect. query_capture_mode null means never asked, never NONE. window_truncated is a retention floor, not a page cut: no limit fixes it. fleet_median is null unless include_fleet_median=true. <<GUIDE>> The Query Store CLUTTER view for one server, composed from rows already collected: no new query runs against the monitored server. Clutter is per DATABASE and overhead is per SERVER, and the payload keeps the two apart. Each database row carries three decomposed arms with raw numbers, never a bare composite. (a) read_cost, from the query_store collector's fan-out rollup on collection_log: runs_observed (fan-out runs on the server in the window), runs_slowest (runs on which THIS database was the slowest item) and runs_slowest_pct, slowest_item_ms_p50/p95 when it was, run_duration_ms_p50 for scale, slowest_share_pct (median of slowest_item_ms / duration_ms over those runs, the slowest item's share of the whole pass, get_collection_health's verdict figure), and dominance_ratio (this database's median slowest cost over the pooled median of every OTHER database's slowest cost on the same server, null when no other database was ever slowest). dominance_ratio is NOT get_collection_health's dominance (slowest x items / run): it names a different quantity. The store keeps only the slowest item of each run, so this arm ranks databases by how often and by how much they were the slowest, not by a per-database series. (b) plan_churn, from the clutter rollups plus raw query_store_stats at the edges: distinct_queries, distinct_plans (plan_id, the identity MAX_PLANS_PER_QUERY caps and the plan map keys on), plans_per_query_p95 and _max, new_plans_per_day (plans first seen after the database's first collection in the window, per day of the span its collections cover; a plan idle before the window and run again inside it counts as new, the store keeps no first-seen stamp), never_seen_twice_fraction (plans seen under exactly one collection over all plans observed; null under two collections) and collections_observed. (c) config, the newest query_store_health capture per database INSIDE the window, with its own captured_at: actual_state, desired_state, readonly_reason (decoded), storage used vs cap and pct_of_cap, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, and query_capture_mode (the DMV's query_capture_mode_desc spelling verbatim: ALL / AUTO / CUSTOM / NONE) with capture_mode_known beside it, null means the newest capture predates the column, which is NEVER ASKED and never NONE. The mode is the one option on that row that names a plan-churn factory, so the churn recommendations name it: ALL beside high churn is the 'switch to AUTO' case, AUTO beside it points at the workload. Per database, verdict is the band canon (Healthy / Warning / Critical / Unknown) with verdict_reasons naming the arm and bar that raised it; the bars are published under thresholds so you can disagree with them on the evidence, and Unknown means unmeasured, never quietly Healthy. recommendations is prose per reason and next_tools names the reads that carry the detail. Rows are ordered worst-first (band, then the read-cost share, then plans per query) and cut at limit, observed off a fetch one past it: truncated is the page cut, databases_returned the page, database_count the whole. READ-ONLY: verdict_reasons qs_read_only (Critical) means readonly_reason carries a capacity bit (65536, 131072, 262144 or 524288), the store filled up and stopped, and a capacity bit decides when a state bit is set too. qs_read_only_state means Query Store is READ_ONLY with no capacity bit and no operator choice: either the database itself is in a state Query Store cannot record in (readonly_reason 1 read-only, 2 single-user, 4 emergency), or readonly_reason is 0 with desired_state READ_WRITE, or it holds only bits this build does not recognise, where the cause is not recorded and get_query_store_health and the error log are the next read. qs_read_only_operator means readonly_reason 0 with desired_state READ_ONLY, an operator's own OPERATION_MODE. Both are reasons with no band and are not clutter; the row stays Unknown unless another arm grades it, and recommendations name the decoded reason. REPLICAS: a database whose readonly_reason carries bit 8 (the engine's readable-secondary flag) reads excluded: true, excluded_reason: qs_read_only_replica, verdict Unknown, Query Store on a readable secondary is READ_ONLY by design, its clutter and configuration are its primary's, and it is never a defect here. qs_overhead is the ONE per-server block: wait_stats lists every non-sleep QDS_* wait type in the window with wait_ms_total, waiting_tasks_total and wait_ms_per_hour (rated milliseconds over the measured seconds they accrued over, never over a cadence; null when no interval was knowable), and excluded_wait_types NAMES the four QDS_* sleep waits IgnoredWaitDefaults drops at collection (QDS_ASYNC_QUEUE, QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP, QDS_PERSIST_TASK_MAIN_LOOP_SLEEP, QDS_SHUTDOWN_QUEUE) so the proxy is not mistaken for the whole; memory_clerk is MEMORYCLERK_QUERYDISKSTORE latest and window max, with clerk_in_latest_capture saying whether it is in the collector's current top 25 (its absence is a rank, not a zero); the block ends with the window's baseline discontinuities, the same markers the trend tools publish. The window block carries the requested start and end beside the raw tier's reach. fleet_median is opt-in (include_fleet_median): discrete medians of slowest_share_pct and plans_per_query_p95 over every database on every enabled SQL Server target that is not a replica, and of the QDS wait rate over those servers, each with the population it was drawn from, off by default because it walks two raw hypertables fleet-wide over the window. The call waits for the walk for what is left of the 5 s read bar after its per-server reads, and for at least 1 s. When the walk runs over that, fleet_median is null and fleet_median_note says so, and every per-server figure is still complete. The walk then goes on without the call for up to 120 s, a reference that took over 5 s is kept for the rest of its clock hour for that hours_back, and a later call in that hour answers at once: with the kept reference and when it was computed, or with a note that it is still being computed or did not finish. One reference is computed at a time: a call for another hours_back while one runs answers at once with a note and starts nothing. include_fleet_median's own original wording, unabbreviated: If true, also compute fleet_median: the discrete median of each headline arm over every enabled SQL Server target that is not a replica, so a per-server figure has a reference. Default false: it walks query_store_stats and wait_stats fleet-wide over the window, which on a large store is the read deadline's whole budget. The wait arm has its own floor, and window gives each arm's reach." + McpHelpers.WindowTruncatedDescription)]
    public static Task<string> GetQueryStoreClutter(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24, maximum 168; window_truncated says when the plan-churn arm (rollups plus raw rows) or the wait arm holds less than the window.")] int hours_back = 24,
        [Description("Maximum database rows to return, worst first. Default 50. truncated is true when the server had more Query-Store-bearing databases than this; database_count is the whole.")] int limit = DefaultLimit,
        [Description("If true, also computes fleet_median: the discrete median of each headline arm over every enabled non-replica SQL Server target. Default false; see the reading guide for the read-cost detail.")] bool include_fleet_median = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
        => GetQueryStoreClutter(postgres, server_name, hours_back, limit, include_fleet_median, as_of, DatabaseFilter.One(database_name), logger, cancellationToken);

    /// <summary>
    /// #5244: the get_query_store_clutter read over a LIST of databases. The MCP tool passes <c>DatabaseFilter.One(database_name)</c>
    /// and the web dispatch passes the repeated keys. Only the per-database rows narrow (read cost, plan churn, config);
    /// the per-server overhead (waits, memory clerk), the window floor, the baseline discontinuities and the fleet reference
    /// stay whole. The one-name consumers are list-aware: the Query Store precondition on the empty path takes the filter,
    /// an empty answer for chosen databases is <c>empty</c> and says "for the database X" (one name) or "for the chosen databases" (two or more) (never a verdict on the
    /// server), and <c>server_is_replica</c> is read off the server's WHOLE config, not the chosen databases' rows.
    /// </summary>
    internal static Task<string> GetQueryStoreClutter(
        NpgsqlDataSource postgres,
        string? server_name,
        int hours_back,
        int limit,
        bool include_fleet_median,
        string? as_of,
        DatabaseFilter databaseFilter,
        CancellationToken cancellationToken)
        => GetQueryStoreClutter(postgres, server_name, hours_back, limit, include_fleet_median, as_of, databaseFilter, logger: null, cancellationToken);

    /// <summary>#5633 review: <see cref="GetQueryStoreClutter(NpgsqlDataSource,string,int,int,bool,string,DatabaseFilter,CancellationToken)"/>
    /// with the host's logger, so the dropped-database lookup's fail-open (CurrentDatabaseScope) is logged at Warning.</summary>
    internal static async Task<string> GetQueryStoreClutter(
        NpgsqlDataSource postgres,
        string? server_name,
        int hours_back,
        int limit,
        bool include_fleet_median,
        string? as_of,
        DatabaseFilter databaseFilter,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        /* #9958: the call's own clock, the one the fleet reference measures a computation on, so the wait for the fleet arm is what is
           left of the read bar after everything this call has read. */
        var callClock = QueryStoreClutterFleetReference.Clock.Value ?? TimeProvider.System;
        var callStarted = callClock.GetTimestamp();
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        /* #8570: the three reads that need only the server id and the window start before the per-database reads and are awaited
           where each used to run. The source is the one the finally below cancels, so a return or a throw never leaves one running. */
        var independent = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        Task? waitsRead = null;
        Task? clerkRead = null;
        Task? discontinuitiesRead = null;
        try
        {
            var now = windowEnd;
            /* Review M3: an as_of read keeps a database dropped since the window (the newest snapshot at or before the anchor decides). */
            DateTime? asOfUtc = string.IsNullOrWhiteSpace(as_of) ? null : windowEnd;
            var requestedStart = now.AddHours(-hours_back);
            var serverIds = new[] { resolved.ServerId };

            /* #5633 review (finding D): the reads omit a database the server no longer lists; the names they left out ride the envelope of
               an empty page and of a page with rows, and an empty page does not read as "Query Store is off everywhere". */
            var omitted = new HashSet<string>(StringComparer.Ordinal);

            /* #8570: waits, clerk and baseline markers share no state with each other or with the reads below (each takes its own pooled
               connection) and none touches `omitted`, which the next three reads fill one after another. */
            var independentToken = independent.Token;
            var waitsTask = RunIndependentReadAsync("waits", t => DarlingQueryStoreClutterReader.GetQdsWaitsAsync(postgres, serverIds, requestedStart, now, t), independentToken);
            waitsRead = waitsTask;
            var clerkTask = RunIndependentReadAsync("clerk", t => DarlingQueryStoreClutterReader.GetQueryStoreClerkAsync(postgres, resolved.ServerId, requestedStart, now, t), independentToken);
            clerkRead = clerkTask;
            var discontinuitiesTask = RunIndependentReadAsync("discontinuities", t => DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, requestedStart, now, t), independentToken);
            discontinuitiesRead = discontinuitiesTask;

            var readCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, serverIds, requestedStart, now, databaseFilter, asOfUtc, omitted, logger, cancellationToken);
            var planChurn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, serverIds, requestedStart, now, databaseFilter, asOfUtc, omitted, logger, cancellationToken);
            var config = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, serverIds, requestedStart, now, databaseFilter, asOfUtc, omitted, logger, cancellationToken);
            var leftOut = DatabasesLeftOut.Distinct(omitted);
            var waits = await waitsTask;
            var clerk = await clerkTask;

            if (readCost.Count == 0 && planChurn.Count == 0 && config.Count == 0)
            {
                /* #8570: the baseline markers are not part of an empty answer; stop that read now rather than after the ladder below. */
                independent.Cancel();
                /* The same ladder get_query_store_top climbs: the collector cannot run on this engine; the
                   store's recorded Query Store configuration says the databases are not recording; the
                   collector's own last run recorded a precondition; and only then the plain miss. Waits and
                   the clerk are not consulted here — a server with QDS waits and no Query Store rows is a
                   server whose Query Store this tool cannot see, which is what the miss says. */
                var unmet = McpHelpers.WithDatabase(await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, DarlingQueryStoreClutterReader.CollectorName, cancellationToken), databaseFilter.Describe())
                    ?? McpHelpers.WithDatabase(await DarlingRuntimePrecondition.QueryStoreStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, databaseFilter, cancellationToken), databaseFilter.Describe())
                    ?? McpHelpers.WithDatabase(await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.DisplayName, DarlingQueryStoreClutterReader.CollectorName, cancellationToken), databaseFilter.Describe());
                if (unmet is not null) return McpHelpers.WithDatabasesLeftOut(unmet, leftOut)!;

                /* #7010: the three answers above say their own cause and stay bare. What is left is a window with no Query Store row in it,
                   which is not a true negative when the store does not reach back to it: the hints carry the window-floor notice over
                   query_store_stats (the probe's table is the one the rows come from). A failed probe costs the hints, never the answer. */
                var noRows = await DarlingMcpWindowNotice.ReadAsync(
                    () => DarlingMcpWindowNotice.Probe(postgres, "query_store_stats", resolved.ServerName, requestedStart, now, cancellationToken),
                    requestedStart, now, "query_store_stats", emptyAnswer: true, logger: logger, cancellationToken: cancellationToken);
                var emptyAnswer = !databaseFilter.IsAll
                    ? McpHelpers.StatusForDatabase(
                        "empty",
                        $"No Query Store rows, no query_store fan-out run and no query_store_health capture{DarlingMcpBlockingTools.ForChosenDatabases(databaseFilter)} in the {hours_back}-hour window. "
                        + "A chosen database either has Query Store OFF, is not on this server, or has not completed a cycle yet. Widen the selection to read the whole server.", databaseFilter.Describe(), noRows.AsHints())
                    : leftOut is not null
                        ? McpHelpers.StatusForDatabase(
                            "empty",
                            $"No Query Store rows in the {hours_back}-hour window for a database this server still has.",
                            databaseFilter.Describe(), noRows.AsHints())
                        : McpHelpers.StatusForDatabase(
                            "unavailable",
                            $"No Query Store rows, no query_store fan-out run and no query_store_health capture for this server in the {hours_back}-hour window. "
                            + "The query_store collector runs every 5 minutes and query_store_health hourly on SQL Server 2016+; a server with none of the three either predates Query Store, has it OFF everywhere, or has not completed a cycle yet. "
                            + "The plan-churn arm reads the clutter rollups and raw rows, and the raw rows a store with the rollups armed drops at 4 days: try a shorter window before concluding nothing ran.", databaseFilter.Describe(), noRows.AsHints());
                return McpHelpers.WithDatabasesLeftOut(await DarlingMcpWindowNotice.FinishEmptyAsync(postgres, resolved.ServerId, resolved.DisplayName, windowEnd, emptyAnswer, noRows, cancellationToken, requestedStart), leftOut)!;
            }

            /* #2364 / #3653 item 17, #9382: how far back each arm that can be cut actually reached, beside what was asked for. The
               plan-churn arm reads whole days and hours from the clutter rollups and raw rows at the edges (#9148, #9173), so its coverage
               starts where its route's rollups do, or at raw's oldest row: GetPlanChurnCoverageStartAsync. The wait arm reads wait_stats
               and has its own floor. The read-cost and config arms read tables with longer retention. */
            var planChurnStart = await DarlingQueryStoreClutterReader.GetPlanChurnCoverageStartAsync(postgres, serverIds, requestedStart, now, cancellationToken: cancellationToken);
            var waitFloor = await RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.WaitStats, resolved.ServerId, requestedStart, now, McpCommandDeadlines.ReadSeconds, cancellationToken);
            /* A start before the requested one means the arm covers the window whole; the shared helper never reports a served start earlier
               than the one asked for. */
            var planChurnTruncated = RawWindowFloor.IsTruncated(planChurnStart, requestedStart);
            var waitTruncated = RawWindowFloor.IsTruncated(waitFloor, requestedStart);
            var windowTruncated = planChurnTruncated || waitTruncated;
            /* effective_start and effective_hours_back describe the first arm the note names: plan churn when it was cut, else the wait arm. */
            var effectiveStart = RawWindowFloor.EffectiveStart(planChurnTruncated ? planChurnStart : waitTruncated ? waitFloor : null, requestedStart);
            var truncationNote = windowTruncated ? WindowTruncationNote(planChurnTruncated ? planChurnStart : null, waitTruncated ? waitFloor : null, now) : null;

            var discontinuities = await discontinuitiesTask;

            /* A SERVER-level claim ("every database here is a readable secondary"), so it is read off the server's whole latest
               config, never the chosen databases' rows: one replica database picked from a mixed server must not say the server is one.
               It is a per-server read, so it comes before the fleet arm and counts in what that arm is left. */
            var serverIsReplica = QueryStoreClutter.IsReplicaServer(
                databaseFilter.IsAll
                    ? config
                    : await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, serverIds, requestedStart, now, asOfUtc: asOfUtc, cancellationToken: cancellationToken));

            var composed = QueryStoreClutter.Compose(readCost, planChurn, config);
            var (page, truncated) = McpHelpers.BoundPage(composed, limit);

            object? fleetMedian = null;
            string? fleetMedianNote = null;
            if (include_fleet_median)
            {
                /* #9478: the fleet arm has its own bound, well under the 30 s per-command deadline, so the per-server answer always
                   returns. #9882: the wait is bounded and the computation is not. It goes on past the bound under its own cap, one per
                   store, hours_back and clock hour, and a later call for the same key answers at once (QueryStoreClutterFleetReference).
                   #9958: the bound is what is left of the read bar after the per-server reads above, never under the floor. */
                var fleetWait = FleetWait(callClock.GetElapsedTime(callStarted));
                FleetWaitSeam.Value?.Invoke(fleetWait);
                using var fleetBudget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                fleetBudget.CancelAfter(fleetWait);
                try
                {
                    if (FleetReadSeam.Value is { } seam) await seam(fleetBudget);
                    var answer = await QueryStoreClutterFleetReference.GetAsync(
                        postgres, hours_back, now, asOfUtc is not null,
                        fleetToken => ComputeFleetReferenceAsync(postgres, requestedStart, now, asOfUtc, fleetToken),
                        fleetBudget, fleetWait, logger);
                    if (answer.Outcome is not { } outcome)
                    {
                        fleetMedianNote = answer.OtherWindowRunning ? FleetBusyNote : FleetOverrunNote;
                    }
                    else if (outcome.Reference is not { } reference)
                    {
                        fleetMedianNote = FleetUnfinishedNote;
                    }
                    else
                    {
                        var median = reference.Median;
                        fleetMedian = new
                        {
                            slowest_share_pct = Round(median.SlowestSharePct, 1),
                            databases_in_read_cost_median = median.DatabasesInReadCostMedian,
                            plans_per_query_p95 = median.PlansPerQueryP95,
                            databases_in_churn_median = median.DatabasesInChurnMedian,
                            qds_wait_ms_per_hour = Round(median.QdsWaitMsPerHour, 1),
                            servers_in_wait_median = median.ServersInWaitMedian,
                            servers_in_median = median.ServersInMedian,
                            replica_servers_excluded = median.ReplicaServersExcluded,
                            enabled_sql_server_targets = reference.EnabledSqlServerTargets,
                        };
                        fleetMedianNote =
                            "Discrete medians (percentile_disc 0.5) over the same window and the same arithmetic as the rows above, "
                            + "on every enabled SQL Server target whose newest query_store_health rows do not describe a readable secondary. "
                            + "The read-cost and churn medians are over DATABASES (only databases whose server fanned out over at least "
                            + QueryStoreClutter.MinFanoutItemsForReadCost.ToString(CultureInfo.InvariantCulture)
                            + " databases enter the read-cost median); the wait median is over SERVERS (each server's sum of its included QDS_* rates). "
                            + "A null median with a zero population means no server in the fleet contributed that arm in the window.";
                        if (answer.Shared)
                        {
                            /* #9882: another call computed this reference, over its own window in the same clock hour. */
                            fleetMedianNote += string.Create(
                                CultureInfo.InvariantCulture,
                                $" This reference was computed at {outcome.EndedUtc:yyyy-MM-dd HH:mm:ss} UTC over the {hours_back} hours ending {reference.WindowEndUtc:yyyy-MM-dd HH:mm:ss} UTC and took {outcome.Seconds:F1} s; one that takes over {QueryStoreClutterFleetReference.KeepAboveSeconds} s is kept for the rest of its clock hour.");
                        }
                    }
                }
                catch (Exception ex) when (IsFleetOverrun(ex, cancellationToken, fleetBudget.Token))
                {
                    fleetMedianNote = FleetOverrunNote;
                }
            }
            else
            {
                fleetMedianNote = "Not computed: pass include_fleet_median=true for the fleet reference (a fleet-wide walk of query_store_stats and wait_stats over the window).";
            }

            var includedWaits = waits.Where(w => !QueryStoreClutter.IsExcludedWaitType(w.WaitType)).ToList();
            var excludedPresent = waits.Where(w => QueryStoreClutter.IsExcludedWaitType(w.WaitType)).Select(w => w.WaitType).OrderBy(w => w, StringComparer.Ordinal).ToList();
            var databases = page.Select(r => new
            {
                database_name = r.DatabaseName,
                /* The band canon (#3703): the enum's own spelling, so it cannot drift by hand. */
                verdict = r.Verdict.ToString(),
                verdict_reasons = r.Reasons,
                excluded = r.Excluded,
                excluded_reason = r.ExcludedReason,
                read_cost = r.ReadCost is { } a
                    ? new
                    {
                        runs_observed = a.RunsObserved,
                        runs_slowest = a.RunsSlowest,
                        runs_slowest_pct = Math.Round(QueryStoreClutter.RunsSlowestPct(a), 1),
                        slowest_item_ms_p50 = a.SlowestItemMsP50,
                        slowest_item_ms_p95 = a.SlowestItemMsP95,
                        run_duration_ms_p50 = a.RunDurationMsP50,
                        slowest_share_pct = Math.Round(a.SlowestSharePctP50, 1),
                        dominance_ratio = Round(QueryStoreClutter.DominanceRatio(a), 2),
                        others_slowest_item_ms_p50 = a.OthersSlowestItemMsP50,
                        fanout_items_max = a.FanoutItemsMax,
                        last_slowest_at = Stamp(a.LastSlowestAt),
                    }
                    : null,
                plan_churn = r.PlanChurn is { } b
                    ? new
                    {
                        distinct_queries = b.DistinctQueries,
                        distinct_plans = b.DistinctPlans,
                        plans_per_query_p95 = b.PlansPerQueryP95,
                        plans_per_query_max = b.PlansPerQueryMax,
                        plans_first_seen_in_window = b.PlansFirstSeenAfterFirstCollection,
                        new_plans_per_day = Round(QueryStoreClutter.NewPlansPerDay(b), 1),
                        plans_seen_once = b.PlansSeenOnce,
                        never_seen_twice_fraction = Round(QueryStoreClutter.NeverSeenTwiceFraction(b), 3),
                        collections_observed = b.CollectionsObserved,
                        first_collection = Stamp(b.FirstCollection),
                        last_collection = Stamp(b.LastCollection),
                    }
                    : null,
                config = r.Config is { } c
                    ? new
                    {
                        captured_at = Stamp(c.CapturedAt),
                        actual_state = c.ActualState,
                        desired_state = c.DesiredState,
                        readonly_reason = c.ReadonlyReason,
                        readonly_reason_decoded = QueryStoreReadonlyReason.DecodeWithState(c.ActualState, c.ReadonlyReason) is { Length: > 0 } decoded ? decoded : null,
                        current_storage_size_mb = c.CurrentStorageMb,
                        max_storage_size_mb = c.MaxStorageMb,
                        pct_of_cap = Round(QueryStoreClutter.PctOfCap(c), 1),
                        size_based_cleanup_mode = string.IsNullOrEmpty(c.SizeBasedCleanupMode) ? null : c.SizeBasedCleanupMode,
                        stale_query_threshold_days = c.StaleQueryThresholdDays,
                        max_plans_per_query = c.MaxPlansPerQuery,
                        interval_length_minutes = c.IntervalLengthMinutes,
                        /* #3796: the column the health row does not carry yet. Null with the flag beside it,
                           never a guess — the flag flips when the rung lands and the reader gains the column. */
                        query_capture_mode = c.QueryCaptureMode,
                        capture_mode_known = c.QueryCaptureMode is { Length: > 0 },
                        capture_mode_note = CaptureModeNote,
                    }
                    : null,
                recommendations = QueryStoreClutter.Recommendations(r),
                next_tools = NextTools(resolved.DisplayName, r, hours_back),
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* #5244: which databases the rows are limited to: the name for one, "the chosen databases" for two or more, null for all. */
                database_name = databaseFilter.Describe(),
                /* #5633 review (finding D): null when the read left no database out. */
                databases_left_out = DatabasesLeftOut.Named(leftOut),
                databases_left_out_count = DatabasesLeftOut.Count(leftOut),
                databases_left_out_note = DatabasesLeftOut.Note(leftOut),
                /* The window as its own block: the requested edges and, beside them, the raw tier's reach under
                   the window floor's spelling (#2364 / #3653 item 17). Nested rather than flat because this
                   payload ALSO carries a page cut (`truncated`, below), and the two facts must not share a
                   block — a client that learned the page dialect's word must never find it beside
                   effective_hours_back (McpPayloadContractCensusTests holds the two apart). */
                window = new
                {
                    start = Stamp(requestedStart),
                    end = Stamp(now),
                    effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                    effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                    /* #9382: each arm's own reach, null when that arm covers the window, so a page panel says its own arm's span. */
                    plan_churn_effective_start = planChurnTruncated ? McpHelpers.FormatEffectiveStart(planChurnStart!.Value) : null,
                    plan_churn_effective_hours_back = planChurnTruncated ? Math.Round((now - planChurnStart!.Value).TotalHours, 1) : (double?)null,
                    wait_effective_start = waitTruncated ? McpHelpers.FormatEffectiveStart(waitFloor!.Value) : null,
                    wait_effective_hours_back = waitTruncated ? Math.Round((now - waitFloor!.Value).TotalHours, 1) : (double?)null,
                    window_truncated = windowTruncated,
                    truncation_note = truncationNote,
                },
                server_is_replica = serverIsReplica,
                server_note = serverIsReplica
                    ? "Every Query-Store-bearing database on this server is a readable secondary (readonly_reason bit 8): its Query Store is its primary's, replicated, and no per-database verdict is issued here. The qs_overhead block is real on a replica and is reported. Read the primary for the clutter."
                    : null,
                thresholds = new
                {
                    runs_slowest_gate_pct = QueryStoreClutter.RunsSlowestGatePct,
                    read_cost_warning_share_pct = QueryStoreClutter.ReadCostWarningSharePct,
                    read_cost_critical_share_pct = QueryStoreClutter.ReadCostCriticalSharePct,
                    min_fanout_items_for_read_cost = QueryStoreClutter.MinFanoutItemsForReadCost,
                    plans_per_query_p95_warning = QueryStoreClutter.PlansPerQueryP95Warning,
                    plans_per_query_p95_critical = QueryStoreClutter.PlansPerQueryP95Critical,
                    one_shot_fraction_warning = QueryStoreClutter.OneShotFractionWarning,
                    min_distinct_plans_for_one_shot = QueryStoreClutter.MinDistinctPlansForOneShot,
                    storage_near_cap_pct = QueryStoreClutter.StorageNearCapPct,
                    note = "read_cost_dominant needs runs_slowest_pct >= runs_slowest_gate_pct AND fanout_items_max >= min_fanout_items_for_read_cost; then slowest_share_pct >= the warning bar is Warning, >= the critical bar Critical. plan_churn_high is plans_per_query_p95 against its two bars. one_shot_plans is never_seen_twice_fraction >= its bar over at least min_distinct_plans_for_one_shot plans. plans_per_query_at_cap is plans_per_query_max >= max_plans_per_query. storage_near_cap is pct_of_cap >= its bar. qs_read_only is Critical only when readonly_reason carries a capacity bit (65536 storage cap, 131072 statement memory, 262144 persist backlog, 524288 disk size limit), and the capacity bit decides when a state bit is set too. READ_ONLY from the database's own state (readonly_reason 1 read-only, 2 single-user, 4 emergency) is qs_read_only_state, and READ_ONLY with reason 0 and desired_state READ_ONLY is qs_read_only_operator: both are reasons with no band, not clutter. qs_off and not_measured are reasons on an Unknown row, never a band; the other arms (read cost, plan churn, storage_near_cap) still grade a read-only database.",
                },
                /* The SERVER-level flag is the AND over the rows: it says the whole config arm can be read
                   for the mode, so a client that finds it true need not check each row, and one row still
                   on a pre-rung capture makes it false rather than being averaged away. */
                capture_mode_known = config.Count > 0 && config.TrueForAll(c => c.QueryCaptureMode is { Length: > 0 }),
                capture_mode_note = CaptureModeNote,
                database_count = composed.Count,
                databases_returned = page.Count,
                truncated,
                limit,
                databases,
                qs_overhead = new
                {
                    note = "Per SERVER, not per database: the QDS_* wait types and the Query Store memory clerk are instance-wide, and nothing here attributes them to a database. The wait figures are the collector's stored deltas summed over the window; wait_ms_per_hour divides each type's rated milliseconds by the seconds those rows measured (never by a cadence) and is null when no interval was knowable.",
                    wait_stats = new
                    {
                        included = includedWaits.Select(w => new
                        {
                            wait_type = w.WaitType,
                            wait_ms_total = w.WaitMsTotal,
                            waiting_tasks_total = w.WaitingTasksTotal,
                            wait_ms_per_hour = Round(QueryStoreClutter.WaitMsPerHour(w.RatedWaitMs, w.MeasuredSeconds), 1),
                            measured_seconds = w.MeasuredSeconds,
                            rows_observed = w.RowsObserved,
                            rows_unknowable = w.RowsUnknowable,
                            rows_without_interval = w.RowsWithoutInterval,
                            first_observed = Stamp(w.FirstObserved),
                            last_observed = Stamp(w.LastObserved),
                        }),
                        included_count = includedWaits.Count,
                        total_wait_ms = includedWaits.Sum(w => w.WaitMsTotal),
                        total_wait_ms_per_hour = includedWaits.Count == 0
                            ? null
                            : Round(includedWaits.Sum(w => QueryStoreClutter.WaitMsPerHour(w.RatedWaitMs, w.MeasuredSeconds) ?? 0), 1),
                        excluded_wait_types = QueryStoreClutter.ExcludedQdsWaitTypes,
                        excluded_wait_types_present_in_store = excludedPresent,
                        excluded_note = "The excluded types are sleep waits (Query Store's background tasks idling) dropped at collection by IgnoredWaitDefaults on both SKUs, and dropped again here by name if a row ever carried one. The proxy is the NON-sleep QDS_* waits (QDS_LOADDB, QDS_BLOCKING_TASK, QDS_DYN_VECTOR, ...) plus the memory clerk, and it is a proxy, not the whole of Query Store's cost.",
                    },
                    memory_clerk = new
                    {
                        clerk_type = DarlingQueryStoreClutterReader.QueryStoreMemoryClerk,
                        latest_memory_mb = clerk.LatestMemoryMb,
                        latest_clerk_captured_at = Stamp(clerk.LatestClerkCapture),
                        max_memory_mb_in_window = clerk.MaxMemoryMb,
                        clerk_samples = clerk.ClerkSamples,
                        captures_in_window = clerk.Captures,
                        latest_capture_at = Stamp(clerk.LatestCapture),
                        clerk_in_latest_capture = clerk.LatestCapture is { } lc && clerk.LatestClerkCapture is { } lcc && lcc == lc,
                        note = clerk.Captures == 0
                            ? ClerkNoteNoCapture
                            : clerk.ClerkSamples == 0
                                ? ClerkNoteNeverAppeared
                                : clerk.LatestClerkCapture == clerk.LatestCapture
                                    ? ClerkNoteInNewestCapture
                                    : ClerkNoteDroppedOut,
                    },
                    discontinuities = BaselineDiscontinuities.ToPayload(discontinuities),
                },
                fleet_median = fleetMedian,
                fleet_median_note = fleetMedianNote,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_clutter", ex);
        }
        finally
        {
            await SettleIndependentReadsAsync(independent, waitsRead, clerkRead, discontinuitiesRead);
        }
    }

    private const string ToolQueryStoreHealth = "get_query_store_health";
    private const string ToolCollectionHealth = "get_collection_health";
    private const string ToolQueryStoreTop = "get_query_store_top";
    private const string ToolCollectionLog = "get_collection_log";
    private const string ToolWaitStats = "get_wait_stats";
    private const string ToolMemoryClerks = "get_memory_clerks";

    /// <summary>#9882: the fleet reference over one window, from the five fleet reads. Every read takes <paramref name="fleetToken"/>,
    /// the computation's cap (<see cref="QueryStoreClutterFleetReference.CapSeconds"/>), so at the cap each statement is cancelled and
    /// its connection goes back to the pool.</summary>
    private static async Task<QueryStoreClutterFleetReference.Reference> ComputeFleetReferenceAsync(
        NpgsqlDataSource postgres, DateTime requestedStart, DateTime now, DateTime? asOfUtc, CancellationToken fleetToken)
    {
        var fleetIds = await DarlingQueryStoreClutterReader.GetEnabledSqlServerTargetsAsync(postgres, MonitoredEngineKind.SqlServer, fleetToken);
        var fleetReadCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, fleetIds, requestedStart, now, asOfUtc: asOfUtc, cancellationToken: fleetToken);
        /* #9478: the median reads only each database's p95, so the fleet arm reads the narrow churn statement. */
        var fleetChurn = await DarlingQueryStoreClutterReader.GetPlanChurnP95Async(postgres, fleetIds, requestedStart, now, asOfUtc: asOfUtc, cancellationToken: fleetToken);
        var fleetConfig = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, fleetIds, requestedStart, now, asOfUtc: asOfUtc, cancellationToken: fleetToken);
        var fleetWaits = await DarlingQueryStoreClutterReader.GetQdsWaitsAsync(postgres, fleetIds, requestedStart, now, fleetToken);
        var median = QueryStoreClutter.ComputeFleetMedian(fleetIds, fleetReadCost, fleetChurn, fleetConfig, fleetWaits);
        return new QueryStoreClutterFleetReference.Reference(median, fleetIds.Length, requestedStart, now);
    }

    /// <summary>Every tool name <see cref="NextTools"/> can put in its payload (#3898 D7) — named constants
    /// rather than a second hand-typed list, so the <c>/core</c> profile's closure computation and the emitted
    /// payload can never drift apart. Unlike the fact-keyed tables in <see cref="ToolRecommendations"/> and
    /// <see cref="PgTargetToolRecommendations"/>, this reader's <c>next_tools</c> is not keyed by fact at all —
    /// every clutter row points at (a subset of) the same six reads.</summary>
    internal static readonly string[] FixedNextToolNames =
    [
        ToolQueryStoreHealth,
        ToolCollectionHealth,
        ToolQueryStoreTop,
        ToolCollectionLog,
        ToolWaitStats,
        ToolMemoryClerks
    ];

    /// <summary>
    /// The reads that carry one database's detail — only tools this server hosts. The pair the clutter view
    /// is composed FROM (<c>get_query_store_health</c>, <c>get_collection_health</c>) come first, then the
    /// query-level reads that name the plans, then the server-level overhead reads.
    /// </summary>
    private static object[] NextTools(string serverName, QueryStoreClutter.DatabaseClutter row, int hoursBack)
    {
        var tools = new List<object>
        {
            new { tool = ToolQueryStoreHealth, reason = "the full options row and the readonly_reason decode for this database, with the newest hourly capture's stamp", suggested_params = new { server_name = serverName, database_name = row.DatabaseName } },
            new { tool = ToolCollectionHealth, reason = "the query_store collector's fanout block (items / slowest / slowest_share_pct / dominance on the window's worst run) and its run statistics", suggested_params = new { server_name = serverName } },
        };

        if (!row.Excluded)
        {
            tools.Add(new { tool = ToolQueryStoreTop, reason = "which queries and plans in this database carry the cost; query_id + plan_id feed analyze_query_store_plan", suggested_params = new { server_name = serverName, database_name = row.DatabaseName, hours_back = hoursBack } });
            tools.Add(new { tool = ToolCollectionLog, reason = "the slowest query_store runs in the window, with slowest_item naming the database each one spent its time in", suggested_params = new { server_name = serverName, collector_name = DarlingQueryStoreClutterReader.CollectorName, hours_back = hoursBack, min_duration_ms = 0 } });
        }

        tools.Add(new { tool = ToolWaitStats, reason = "the QDS_* waits in the context of every other wait type on the server", suggested_params = new { server_name = serverName, hours_back = hoursBack } });
        tools.Add(new { tool = ToolMemoryClerks, reason = "MEMORYCLERK_QUERYDISKSTORE beside the other clerks on the newest capture", suggested_params = new { server_name = serverName } });
        return tools.ToArray();
    }

    /// <summary>Rounds a nullable figure, keeping null as null — an unmeasured quantity never becomes 0. Spelled
    /// with <c>HasValue</c> rather than a property pattern so the T-SQL guard's member walk reads it whole
    /// (a <c>is { }</c> property pattern is where that walk stops).</summary>
    private static double? Round(double? value, int digits) => value.HasValue ? Math.Round(value.Value, digits) : null;

    /// <summary>A stored naive-UTC stamp as round-trip ISO-8601 with its offset, or null — the
    /// <c>get_oversized_plan_backlog</c> rendering, for its reason: the stored columns are
    /// <c>timestamp without time zone</c> holding UTC, and an offset-less rendering reads as local time.</summary>
    private static string? Stamp(DateTime? value) =>
        value.HasValue
            ? DateTime.SpecifyKind(value.Value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture)
            : null;
}
