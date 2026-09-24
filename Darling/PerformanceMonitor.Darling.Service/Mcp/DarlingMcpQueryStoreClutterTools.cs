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
using System.Threading.Tasks;
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
/// <c>get_query_store_clutter</c> sits on <c>CrossAppMcpToolInventoryPinTests.KnownLiteMissingMcpTools</c>
/// with the port written out rather than being quietly absent. The fleet reference is opt-in
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

    /// <summary>The window floor's tolerance: the raw tier's first row inside the window is allowed to sit
    /// this far after the requested start before the window is called truncated — <c>get_query_store_top</c>'s
    /// 90 minutes, the cadence slack between a request and the first collection that could have served it.</summary>
    private static readonly TimeSpan WindowFloorTolerance = TimeSpan.FromMinutes(90);

    /// <summary>The sentence beside <c>query_capture_mode</c>, naming what a null there means. A null is a
    /// health row captured before the V137 rung (#3796) created the column — the mode was never asked for —
    /// and a reader must not read it as <c>NONE</c>, which is a mode the engine really can be in.</summary>
    public const string CaptureModeNote =
        "query_capture_mode is the query_store_health column added by the V137 rung (#3796) and filled hourly; null means the newest capture predates the rung — never asked, never NONE";

    [McpServerTool(Name = "get_query_store_clutter"), Description(
        "The Query Store CLUTTER view for one server, composed from already-collected rows: no new query runs. Clutter (read_cost/plan_churn/config) is per DATABASE; qs_overhead (waits/memory clerk) is per SERVER, never attributed to a database. No rows answers unavailable, never a clean bill. Verdict Unknown means unmeasured, never quietly Healthy. REPLICAS: excluded, verdict Unknown, never a defect. query_capture_mode null means never asked, never NONE. window_truncated is a retention floor, not a page cut: no limit fixes it. fleet_median is null unless include_fleet_median=true. <<GUIDE>> The Query Store CLUTTER view for one server, composed from rows already collected — no new query runs against the monitored server. Clutter is per DATABASE and overhead is per SERVER, and the payload keeps the two apart. Each database row carries three decomposed arms with raw numbers, never a bare composite. (a) read_cost, from the query_store collector's fan-out rollup on collection_log: runs_observed (fan-out runs on the server in the window), runs_slowest (runs on which THIS database was the slowest item) and runs_slowest_pct, slowest_item_ms_p50/p95 when it was, run_duration_ms_p50 for scale, slowest_share_pct (median of slowest_item_ms / duration_ms over those runs — the slowest item's share of the whole pass, get_collection_health's verdict figure), and dominance_ratio (this database's median slowest cost over the pooled median of every OTHER database's slowest cost on the same server, null when no other database was ever slowest). dominance_ratio is NOT get_collection_health's dominance (slowest x items / run) — it names a different quantity. The store keeps only the slowest item of each run, so this arm ranks databases by how often and by how much they were the slowest, not by a per-database series. (b) plan_churn, from raw query_store_stats: distinct_queries, distinct_plans (plan_id — the identity MAX_PLANS_PER_QUERY caps and the plan map keys on), plans_per_query_p95 and _max, new_plans_per_day (plans first seen after the database's first collection in the window, per day of the span its collections cover; a plan idle before the window and run again inside it counts as new — the store keeps no first-seen stamp), never_seen_twice_fraction (plans seen under exactly one collection over all plans observed; null under two collections) and collections_observed. (c) config, the newest query_store_health capture per database INSIDE the window, with its own captured_at: actual_state, desired_state, readonly_reason (decoded), storage used vs cap and pct_of_cap, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, and query_capture_mode (the DMV's query_capture_mode_desc spelling verbatim: ALL / AUTO / CUSTOM / NONE) with capture_mode_known beside it — null means the newest capture predates the V137 rung that added the column, which is NEVER ASKED and never NONE. The mode is the one option on that row that names a plan-churn factory, so the churn recommendations name it: ALL beside high churn is the 'switch to AUTO' case, AUTO beside it points at the workload. Per database, verdict is the band canon (Healthy / Warning / Critical / Unknown) with verdict_reasons naming the arm and bar that raised it; the bars are published under thresholds so you can disagree with them on the evidence, and Unknown means unmeasured, never quietly Healthy. recommendations is prose per reason and next_tools names the reads that carry the detail. Rows are ordered worst-first (band, then the read-cost share, then plans per query) and cut at limit, observed off a fetch one past it: truncated is the page cut, databases_returned the page, database_count the whole. REPLICAS: a database whose readonly_reason carries bit 8 (the engine's readable-secondary flag) reads excluded: true, excluded_reason: qs_read_only_replica, verdict Unknown — Query Store on a readable secondary is READ_ONLY by design, its clutter and configuration are its primary's, and it is never a defect here. qs_overhead is the ONE per-server block: wait_stats lists every non-sleep QDS_* wait type in the window with wait_ms_total, waiting_tasks_total and wait_ms_per_hour (rated milliseconds over the measured seconds they accrued over, never over a cadence; null when no interval was knowable), and excluded_wait_types NAMES the four QDS_* sleep waits IgnoredWaitDefaults drops at collection (QDS_ASYNC_QUEUE, QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP, QDS_PERSIST_TASK_MAIN_LOOP_SLEEP, QDS_SHUTDOWN_QUEUE) so the proxy is not mistaken for the whole; memory_clerk is MEMORYCLERK_QUERYDISKSTORE latest and window max, with clerk_in_latest_capture saying whether it is in the collector's current top 25 (its absence is a rank, not a zero); the block ends with the window's baseline discontinuities, the same markers the trend tools publish. The window block carries the requested start and end beside the raw tier's reach. fleet_median is opt-in (include_fleet_median): discrete medians of slowest_share_pct and plans_per_query_p95 over every database on every enabled SQL Server target that is not a replica, and of the QDS wait rate over those servers, each with the population it was drawn from — off by default because it walks two raw hypertables fleet-wide over the window. include_fleet_median's own original wording, unabbreviated: If true, also compute fleet_median: the discrete median of each headline arm over every enabled SQL Server target that is not a replica, so a per-server figure has a reference. Default false — it walks query_store_stats and wait_stats fleet-wide over the window, which on a large store is the read deadline's whole budget. The plan-churn and wait arms read the raw tier only, which on a store with the rollups armed is dropped at 4 days." + McpHelpers.WindowTruncatedDescription)]
    public static async Task<string> GetQueryStoreClutter(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24, maximum 168; the plan-churn and wait arms read the raw tier, which a store with the rollups armed drops at 4 days — window_truncated says when the window reached past it.")] int hours_back = 24,
        [Description("Maximum database rows to return, worst first. Default 50. truncated is true when the server had more Query-Store-bearing databases than this; database_count is the whole.")] int limit = DefaultLimit,
        [Description("If true, also computes fleet_median: the discrete median of each headline arm over every enabled non-replica SQL Server target. Default false; see the reading guide for the read-cost detail.")] bool include_fleet_median = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var requestedStart = now.AddHours(-hours_back);
            var serverIds = new[] { resolved.ServerId };

            var readCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, serverIds, requestedStart, now);
            var planChurn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, serverIds, requestedStart, now);
            var config = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, serverIds, requestedStart, now);
            var waits = await DarlingQueryStoreClutterReader.GetQdsWaitsAsync(postgres, serverIds, requestedStart, now);
            var clerk = await DarlingQueryStoreClutterReader.GetQueryStoreClerkAsync(postgres, resolved.ServerId, requestedStart, now);

            if (readCost.Count == 0 && planChurn.Count == 0 && config.Count == 0)
            {
                /* The same ladder get_query_store_top climbs: the collector cannot run on this engine; the
                   store's recorded Query Store configuration says the databases are not recording; the
                   collector's own last run recorded a precondition; and only then the plain miss. Waits and
                   the clerk are not consulted here — a server with QDS waits and no Query Store rows is a
                   server whose Query Store this tool cannot see, which is what the miss says. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, DarlingQueryStoreClutterReader.CollectorName)
                    ?? await DarlingRuntimePrecondition.QueryStoreStatusAsync(postgres, resolved.ServerId, resolved.ServerName, null)
                    ?? await DarlingRuntimePrecondition.StatusAsync(postgres, resolved.ServerId, resolved.ServerName, DarlingQueryStoreClutterReader.CollectorName)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No Query Store rows, no query_store fan-out run and no query_store_health capture for this server in the {hours_back}-hour window. "
                        + "The query_store collector runs every 5 minutes and query_store_health hourly on SQL Server 2016+; a server with none of the three either predates Query Store, has it OFF everywhere, or has not completed a cycle yet. "
                        + "The plan-churn arm reads the raw tier, which a store with the rollups armed drops at 4 days — try a shorter window before concluding nothing ran.");
            }

            /* #2364 / #3653 item 17: what the raw tier actually held, beside what was asked for. The
               plan-churn arm is raw-only (the rollups carry no plan_id), so its floor is the window's. */
            var floor = await DarlingDataReader.GetQueryStoreWindowFloorAsync(postgres, resolved.ServerId, requestedStart, now);
            var effectiveStart = floor ?? requestedStart;
            var windowTruncated = floor is DateTime f && f > requestedStart + WindowFloorTolerance;

            var discontinuities = await DarlingTrendReader.GetBaselineDiscontinuitiesAsync(postgres, resolved.ServerId, requestedStart, now);

            var composed = QueryStoreClutter.Compose(readCost, planChurn, config);
            var (page, truncated) = McpHelpers.BoundPage(composed, limit);

            object? fleetMedian = null;
            string? fleetMedianNote = null;
            if (include_fleet_median)
            {
                var fleetIds = await DarlingQueryStoreClutterReader.GetEnabledSqlServerTargetsAsync(postgres, MonitoredEngineKind.SqlServer);
                var fleetReadCost = await DarlingQueryStoreClutterReader.GetReadCostAsync(postgres, fleetIds, requestedStart, now);
                var fleetChurn = await DarlingQueryStoreClutterReader.GetPlanChurnAsync(postgres, fleetIds, requestedStart, now);
                var fleetConfig = await DarlingQueryStoreClutterReader.GetConfigAsync(postgres, fleetIds, requestedStart, now);
                var fleetWaits = await DarlingQueryStoreClutterReader.GetQdsWaitsAsync(postgres, fleetIds, requestedStart, now);
                var median = QueryStoreClutter.ComputeFleetMedian(fleetIds, fleetReadCost, fleetChurn, fleetConfig, fleetWaits);
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
                    enabled_sql_server_targets = fleetIds.Length,
                };
                fleetMedianNote =
                    "Discrete medians (percentile_disc 0.5) over the same window and the same arithmetic as the rows above, "
                    + "on every enabled SQL Server target whose newest query_store_health rows do not describe a readable secondary. "
                    + "The read-cost and churn medians are over DATABASES (only databases whose server fanned out over at least "
                    + QueryStoreClutter.MinFanoutItemsForReadCost.ToString(CultureInfo.InvariantCulture)
                    + " databases enter the read-cost median); the wait median is over SERVERS (each server's sum of its included QDS_* rates). "
                    + "A null median with a zero population means no server in the fleet contributed that arm in the window.";
            }
            else
            {
                fleetMedianNote = "Not computed: pass include_fleet_median=true for the fleet reference (a fleet-wide walk of query_store_stats and wait_stats over the window).";
            }

            var includedWaits = waits.Where(w => !QueryStoreClutter.IsExcludedWaitType(w.WaitType)).ToList();
            var excludedPresent = waits.Where(w => QueryStoreClutter.IsExcludedWaitType(w.WaitType)).Select(w => w.WaitType).OrderBy(w => w, StringComparer.Ordinal).ToList();
            var serverIsReplica = QueryStoreClutter.IsReplicaServer(config);

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
                        readonly_reason_decoded = c.ReadonlyReason == 0 ? null : QueryStoreReadonlyReason.Decode(c.ReadonlyReason),
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
                next_tools = NextTools(resolved.ServerName, r, hours_back),
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* The window as its own block: the requested edges and, beside them, the raw tier's reach under
                   the window floor's spelling (#2364 / #3653 item 17). Nested rather than flat because this
                   payload ALSO carries a page cut (`truncated`, below), and the two facts must not share a
                   block — a client that learned the page dialect's word must never find it beside
                   effective_hours_back (McpPayloadContractCensusTests holds the two apart). */
                window = new
                {
                    start = Stamp(requestedStart),
                    end = Stamp(now),
                    effective_start = Stamp(effectiveStart),
                    effective_hours_back = Math.Round((now - effectiveStart).TotalHours, 1),
                    window_truncated = windowTruncated,
                    truncation_note = windowTruncated
                        ? "The window reaches further back than this server's raw query_store_stats retains, so the plan-churn arm (and the wait arm, which reads the same raw tier) covers effective_hours_back, not hours_back. The read-cost and config arms read tables with longer retention and cover the whole window."
                        : null,
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
                    note = "read_cost_dominant needs runs_slowest_pct >= runs_slowest_gate_pct AND fanout_items_max >= min_fanout_items_for_read_cost; then slowest_share_pct >= the warning bar is Warning, >= the critical bar Critical. plan_churn_high is plans_per_query_p95 against its two bars. one_shot_plans is never_seen_twice_fraction >= its bar over at least min_distinct_plans_for_one_shot plans. plans_per_query_at_cap is plans_per_query_max >= max_plans_per_query. storage_near_cap is pct_of_cap >= its bar. qs_read_only (not the replica bit) is Critical. qs_off and not_measured are reasons on an Unknown row, never a band.",
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
                        excluded_note = "The excluded types are sleep waits — Query Store's background tasks idling — dropped at collection by IgnoredWaitDefaults on both SKUs, and dropped again here by name if a row ever carried one. The proxy is the NON-sleep QDS_* waits (QDS_LOADDB, QDS_BLOCKING_TASK, QDS_DYN_VECTOR, ...) plus the memory clerk, and it is a proxy, not the whole of Query Store's cost.",
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
                            ? "No memory_clerks capture in the window: the collector wrote nothing for this server, so nothing is known about the clerk."
                            : clerk.ClerkSamples == 0
                                ? "The clerk never appeared in the window. memory_clerks stores the top 25 clerks over 1 MB per capture, so this is a RANK, not a zero: Query Store's memory was below the collector's floor or outside its top 25 on every capture."
                                : clerk.LatestClerkCapture == clerk.LatestCapture
                                    ? "The clerk is in the newest capture's top 25; latest_memory_mb is its current footprint."
                                    : "The clerk appeared earlier in the window and has since dropped out of the collector's top 25; latest_memory_mb is its last observed footprint, not its current one.",
                    },
                    discontinuities = BaselineDiscontinuities.ToPayload(discontinuities),
                },
                fleet_median = fleetMedian,
                fleet_median_note = fleetMedianNote,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_clutter", ex);
        }
    }

    private const string ToolQueryStoreHealth = "get_query_store_health";
    private const string ToolCollectionHealth = "get_collection_health";
    private const string ToolQueryStoreTop = "get_query_store_top";
    private const string ToolCollectionLog = "get_collection_log";
    private const string ToolWaitStats = "get_wait_stats";
    private const string ToolMemoryClerks = "get_memory_clerks";

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
    /// (<c>TsqlConventionGuardTests.KnownTruncatedRanges</c> names the <c>is { }</c> shape as where it stops).</summary>
    private static double? Round(double? value, int digits) => value.HasValue ? Math.Round(value.Value, digits) : null;

    /// <summary>A stored naive-UTC stamp as round-trip ISO-8601 with its offset, or null — the
    /// <c>get_oversized_plan_backlog</c> rendering, for its reason: the stored columns are
    /// <c>timestamp without time zone</c> holding UTC, and an offset-less rendering reads as local time.</summary>
    private static string? Stamp(DateTime? value) =>
        value.HasValue
            ? DateTime.SpecifyKind(value.Value, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture)
            : null;
}
