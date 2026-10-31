/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public static partial class DarlingWebEndpoints
{
    /// <summary>The signature every read endpoint's handler shares: bind the query string, call the tool.</summary>
    internal delegate Task<string> ReadToolHandler(HttpContext context, NpgsqlDataSource postgres, DarlingAnalysisService analysis);

    /// <summary>
    /// The EXPLICIT endpoint-name -> tool-call dispatch table (no runtime reflection): one entry per read-only
    /// tool, binding its parameters from the query string and calling the matching public static tool method. The
    /// keys ARE the <c>/api/read/*</c> surface — the parity test asserts they equal the <c>[McpServerTool]</c>
    /// catalog minus <see cref="ExcludedToolNames"/>.
    ///
    /// <para><paramref name="logger"/> rides by CLOSURE into the one entry whose tool takes a logger seat
    /// (<c>get_sweep_reports</c>) — the <see cref="ReadToolHandler"/> delegate stays three-seat, because a
    /// fourth parameter would touch every entry for the one tool that logs. <see cref="MapAll"/> builds the
    /// dispatch WITH the host service's logger; callers with no host behind them (the parity tests, the triage
    /// section runner — neither maps that entry) build it bare, and there the null is the honest value: no
    /// service log is wired to receive anything.</para>
    ///
    /// <para><paramref name="postgresConfig"/> rides the same way, into the one entry whose tool takes a
    /// config seat (<c>get_store_host</c>, #4214 part 2 — <see cref="DarlingStoreHostProfile.GatherAsync"/>
    /// needs <c>PostgresConfig</c> to resolve the managed data directory, which no other read on this surface
    /// touches). <see cref="MapAll"/> builds the dispatch with the web host's own config; a caller with none
    /// gets the tool's own "unavailable" envelope rather than a null-reference throw if that one entry is ever
    /// invoked without it.</para>
    /// <para><paramref name="readAdmission"/> (#5506 part 3) reaches only the <c>get_read_latency</c> mirror, for the same live
    /// <c>admission</c> object the MCP tool returns; a dispatch built without it answers without that object.</para>
    /// </summary>
    internal static IReadOnlyDictionary<string, ReadToolHandler> BuildReadDispatch(ILogger? logger = null, PostgresConfig? postgresConfig = null, MonitoredServerRegistryState? registryState = null, ReadAdmission? readAdmission = null)
    {
        var dispatch = new Dictionary<string, ReadToolHandler>(StringComparer.Ordinal)
        {
            /* ── analysis reads (take the DarlingAnalysisService) ── */
            ["audit_config"] = (c, pg, an) => DarlingMcpTools.AuditConfig(an, pg, Server(c), c.RequestAborted),
            ["compare_analysis"] = (c, pg, an) => DarlingMcpTools.CompareAnalysis(an, pg, Server(c), Hours(c, 4), QueryNullableInt(c, "baseline_hours_back"), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_analysis_facts"] = (c, pg, an) => DarlingMcpTools.GetAnalysisFacts(an, pg, Server(c), Hours(c, 4), Str(c, "source"), QueryDouble(c, "min_severity", 0), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            // #4198: the tool's own default (limit 18, previews) is sized for a chat caller's token budget.
            // The viewer's two "Analysis Findings" tables render no preview-cut field and never asked for a
            // budget, so this row keeps asking for what dev always returned: every chain, full text. See
            // DarlingWebEndpointsTests.GetAnalysisFindingsRow_PassesTheOldViewerDefaults_EveryChainFullText.
            ["get_analysis_findings"] = (c, pg, an) => DarlingMcpTools.GetAnalysisFindings(an, pg, Server(c), Hours(c, 24), Rows(c, "limit", MaxRowLimit), QueryBool(c, "include_drilldown", false), QueryBool(c, "full_text", true), as_of: AsOf(c), logger: logger, cancellationToken: c.RequestAborted),

            /* ── sessions ── */
            ["get_active_queries"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpSessionTools.GetActiveQueries(pg, Server(c), Hours(c, 1), databases, QueryBool(c, "blocking_only", false), Str(c, "wait_type"), Rows(c, "limit", 50), 2000, AsOf(c), logger, c.RequestAborted, tag: Str(c, "tag"), collectionTime: Str(c, "collection_time"), snapshotsOnly: QueryBool(c, "snapshots_only", false))
                : DatabaseNamesRefusal(c),
            ["get_session_stats"] = (c, pg, an) => DarlingMcpSessionTools.GetSessionStats(pg, Server(c), c.RequestAborted),
            ["get_waiting_tasks"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpSessionTools.GetWaitingTasks(pg, Server(c), Hours(c, 1), Rows(c, "limit", 30), databases, AsOf(c), logger, c.RequestAborted)
                : DatabaseNamesRefusal(c),

            /* ── alerts / mute rules ── */
            ["get_alert_history"] = (c, pg, an) => DarlingMcpAlertTools.GetAlertHistory(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), include_dismissed: QueryBool(c, "include_dismissed", false), metric_name: Str(c, "metric_name"), family: Str(c, "family"), include_streak: QueryBool(c, "include_streak", false), cancellationToken: c.RequestAborted),
            ["get_alert_details"] = (c, pg, an) => First(c, "server_id") is null ? MissingParam("server_id")
                : !RequireText(c, "metric_name", out var detailsMetric) ? MissingParam("metric_name")
                : !RequireText(c, "alert_time", out var detailsTime) ? MissingParam("alert_time")
                : int.TryParse(First(c, "server_id"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var detailsServerId)
                    ? DarlingMcpAlertTools.GetAlertDetails(pg, detailsServerId, detailsMetric, detailsTime, c.RequestAborted)
                    : DarlingMcpAlertTools.GetAlertDetails(pg, 0, "", detailsTime, c.RequestAborted),
            ["get_alert_settings"] = (c, pg, an) => DarlingMcpAlertTools.GetAlertSettings(pg, Server(c), c.RequestAborted),
            ["get_mute_rules"] = (c, pg, an) => DarlingMcpAlertTools.GetMuteRules(pg, QueryBool(c, "enabled_only", true), c.RequestAborted),
            ["get_notification_routes"] = (c, pg, an) => DarlingMcpAlertTools.GetNotificationRoutes(pg, c.RequestAborted),
            ["list_investigation_requests"] = (c, pg, an) => DarlingMcpInvestigationTools.ListInvestigationRequests(pg, Str(c, "state"), Server(c), Str(c, "family"), QueryInt(c, "limit", null, 20), c.RequestAborted),
            ["get_investigation_reports"] = (c, pg, an) => DarlingMcpInvestigationTools.ReportsAsync(pg, McpDataDetail.Full, Server(c), Str(c, "request_id"), Hours(c, 24), c.RequestAborted),

            /* ── fleet sweep reports (#3466 lane 4) ── the tool mirror beside the dedicated /api/sweeps
               routes, the /api/fleet + /api/read/get_fleet_overview coexistence: the page reads its own
               routes, and the 1:1 read surface carries the tool like every other read. The captured
               logger is the tool's logger seat — the web host's SERVICE logger when MapAll built this
               dispatch, the same instance the MCP host injects with AddSingleton<ILogger> (#3473
               review) — so on a store fault the mirror's child reads throw (#4315), the tool's own
               catch logs the exception once and answers the error envelope, and this entry's
               ToHttpResult classifies that envelope and answers the fixed body through
               ServerErrorResult, instead of the hardcoded null this entry carried while the
               dashboard app's provider-less factory was the only alternative. Closure, not a fourth
               ReadToolHandler seat: widening the shared delegate would touch every entry in this
               table for the one tool that logs. */
            ["get_sweep_reports"] = (c, pg, an) => DarlingMcpFleetSweepTools.GetSweepReports(pg, logger, Hours(c, 1), AsOf(c), Str(c, "sweep_id"), Str(c, "watch_state"), c.RequestAborted),

            /* ── blocking / deadlocks ── */
            ["get_blocked_process_xml"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpBlockingTools.GetBlockedProcessXml(pg, Server(c), Hours(c, 24), Rows(c, "limit", 5), AsOf(c), databases, logger, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #4198: the internal overload, not the MCP tool wrapper — pins the OLD row limit (30) and the
               OLD 2000-char text cap (WebSqlTextPreviewLength) explicitly, so this page does not change even
               though the tool's own MCP defaults (limit 15, 150-char preview) did. Same shape #3897's trend
               tools use to pass TrendBudget.Chart here instead of their own MCP point budget. */
            ["get_blocking"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpBlockingTools.GetBlocking(pg, Server(c), Hours(c, 24), Rows(c, "limit", 30), null, false, AsOf(c), databases, DarlingMcpBlockingTools.WebSqlTextPreviewLength, registryState, logger, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #6292: the web-only Block chain read. A key that is present but unreadable is refused, never defaulted; an omitted ecid is 0, as the
               grid carries it. The tree is the Viewer's own, built by the shared reconstruction. */
            ["get_block_chain"] = (c, pg, an) => !RequireText(c, "event_time", out var chainEventTime)
                ? MissingParam("event_time")
                : !OptionalInt(c, "blocked_spid", out var chainSpid) ? UnparseableParam("blocked_spid")
                : chainSpid is null ? MissingParam("blocked_spid")
                : !OptionalInt(c, "blocked_ecid", out var chainEcid) ? UnparseableParam("blocked_ecid")
                : DarlingWebBlockChain.GetBlockChain(pg, Server(c), chainEventTime, chainSpid.Value, chainEcid ?? 0, c.RequestAborted),
            ["get_blocking_trend"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpBlockingTools.GetBlockingTrend(pg, Server(c), Hours(c, 24), AsOf(c), databases, cancellationToken: c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #4254: full_graph defaults false on the MCP signature (a preview keeps a busy production
               store's tools/list-driven call under the shared response budget), but the web viewer has
               always shown the whole graph. The row pins its OWN default to true so #4198's MCP-side
               budget cut does not silently shrink what the viewer renders. */
            /* #5246: the drawn graph is added here for the web only, so the MCP tool's payload, response budget and Lite parity do not change. */
            ["get_deadlock_detail"] = async (c, pg, an) => DarlingWebDeadlockGraph.AddGraphs(await DarlingMcpBlockingTools.GetDeadlockDetail(pg, Server(c), Hours(c, 24), Rows(c, "limit", 5), full_graph: QueryBool(c, "full_graph", true), as_of: AsOf(c), logger: logger, cancellationToken: c.RequestAborted)),
            ["get_deadlock_trend"] = (c, pg, an) => DarlingMcpBlockingTools.GetDeadlockTrend(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_deadlocks"] = (c, pg, an) => DarlingMcpBlockingTools.GetDeadlocks(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), registryState: registryState, logger: logger, cancellationToken: c.RequestAborted),
            ["get_lock_wait_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpBlockingTools.GetLockWaitTrend(pg, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),

            /* ── automatic plan correction (#2028) ── */
            /* #4257: full_text defaults false on the MCP signature (a 150-character query_text preview
               keeps a busy production server's default call under the shared response budget), but the
               web viewer has always shown the whole query text. The row pins its OWN default to true so
               #4198's MCP-side budget cut does not silently truncate what the viewer renders. */
            ["get_plan_corrections"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpPlanCorrectionTools.GetPlanCorrections(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), AsOf(c), QueryBool(c, "full_text", true), databases, null, c.RequestAborted, collapse: true)
                : DatabaseNamesRefusal(c),

            /* ── config (current + history) ── */
            ["get_database_config"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpConfigTools.GetDatabaseConfig(pg, Server(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_server_config"] = (c, pg, an) => DarlingMcpConfigTools.GetServerConfig(pg, Server(c), c.RequestAborted),
            ["get_trace_flags"] = (c, pg, an) => DarlingMcpConfigTools.GetTraceFlags(pg, Server(c), c.RequestAborted),
            ["get_database_protection"] = (c, pg, an) => DarlingMcpDatabaseProtectionTools.GetDatabaseProtection(pg, Server(c), Hours(c, 24), AsOf(c), QueryBool(c, "overdue_only", false), c.RequestAborted),
            ["get_identity_capacity"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpIdentityCapacityTools.GetIdentityCapacity(pg, Server(c), Hours(c, DarlingMcpIdentityCapacityTools.DefaultHoursBack), AsOf(c), databases, Rows(c, "limit", DarlingMcpIdentityCapacityTools.MaxListed), c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_database_config_changes"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpConfigHistoryTools.GetDatabaseConfigChanges(pg, Server(c), Hours(c, 168), databases, AsOf(c), null, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_database_scoped_config"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpConfigHistoryTools.GetDatabaseScopedConfig(pg, Server(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_query_store_health"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpConfigHistoryTools.GetQueryStoreHealth(pg, Server(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_server_config_changes"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetServerConfigChanges(pg, Server(c), Hours(c, 168), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_known_patterns"] = (c, pg, an) => DarlingMcpKnownPatternTools.GetKnownPatterns(pg, Server(c), Str(c, "metric_name"), cancellationToken: c.RequestAborted),
            ["get_change_watches"] = (c, pg, an) => DarlingMcpChangeWatchTools.GetChangeWatches(pg, Server(c), Str(c, "state"), Rows(c, "limit", 20), Str(c, "watch_id"), c.RequestAborted),
            ["get_trace_flag_changes"] = (c, pg, an) => DarlingMcpConfigHistoryTools.GetTraceFlagChanges(pg, Server(c), Hours(c, 168), as_of: AsOf(c), cancellationToken: c.RequestAborted),

            /* ── core data reads ── */
            /* #4198: full_detail=true keeps the web viewer's payload exactly what it was before the default
               cut — every field on every collector row, never the compact shape a boring-healthy row gets
               by default. */
            ["get_collection_health"] = (c, pg, an) => DarlingMcpDataTools.GetCollectionHealth(pg, Server(c), full_detail: true, collector_name: Str(c, "collector_name"), hours_back: Hours(c, 24), cancellationToken: c.RequestAborted),
            /* #4198: full_text: true, because error_message carried no preview cap before this PR — the
               web viewer keeps that behavior (an operator reading the Collection Log grid gets the whole
               error, the same way get_deadlock_detail's row passes TrendBudget.Chart-style overrides to
               hold its OWN pre-existing behavior steady). limit stays explicit at the pre-#4198 200, also
               unaffected by the new lower MCP default. */

            ["get_collection_log"] = (c, pg, an) => OptionalDouble(c, "min_duration_ms", out var minDurationMs)
                ? DarlingMcpDataTools.GetCollectionLog(pg, Server(c), Hours(c, 24), Rows(c, "limit", 200), AsOf(c), Str(c, "collector_name"), minDurationMs, status: Str(c, "status"), full_text: true, cancellationToken: c.RequestAborted)
                : UnparseableParam("min_duration_ms"),
            ["get_current_waits_trend"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDataTools.GetCurrentWaitsTrend(pg, Server(c), Hours(c, 4), databases, AsOf(c), c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_client_timeouts"] = (c, pg, an) => DarlingMcpClientTimeoutTools.GetClientTimeouts(pg, Server(c), Hours(c, 1), Rows(c, "limit", 20), AsOf(c), logger, c.RequestAborted),
            ["get_blocking_stats"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDataTools.GetBlockingStats(pg, Server(c), Hours(c, 24), databases, AsOf(c), logger, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #3960: the core trends a page charts take the CHART budget and bind bucket_minutes, as the trends
               below do (#3897). */
            ["get_cpu_utilization"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpDataTools.GetCpuUtilization(pg, Server(c), Hours(c, 4), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_file_io_stats"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDataTools.GetFileIoStats(pg, Server(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_memory_clerks"] = (c, pg, an) => DarlingMcpDataTools.GetMemoryClerks(pg, Server(c), c.RequestAborted),
            ["get_memory_stats"] = (c, pg, an) => DarlingMcpDataTools.GetMemoryStats(pg, Server(c), c.RequestAborted),
            ["get_perfmon_stats"] = (c, pg, an) => DarlingMcpDataTools.GetPerfmonStats(pg, Server(c), Str(c, "counter_name"), Str(c, "instance_name"), c.RequestAborted),
            ["get_query_heatmap"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpQueryHeatmapTools.GetQueryHeatmap(pg, Server(c), Hours(c, 24), Str(c, "metric"), databases, QueryInt(c, "bucket_minutes", null, 5), Rows(c, "limit", 500), as_of: AsOf(c), full_text: false, logger: logger, cancellationToken: c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #4198: full_text defaults false on the MCP signature (a 240-character preview keeps a busy
               production store's default call under the shared response budget), but the web viewer has
               always shown the whole query text. The row pins its OWN default to true so the MCP-side
               budget cut does not silently shrink what the viewer renders. */
            ["get_query_store_regressions"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(pg, Server(c), Hours(c, 24), databases, Rows(c, "limit", 50), full_text: QueryBool(c, "full_text", true), as_of: AsOf(c), logger: logger, cancellationToken: c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_query_store_clutter"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpQueryStoreClutterTools.GetQueryStoreClutter(pg, Server(c), Hours(c, 24), Rows(c, "limit", DarlingMcpQueryStoreClutterTools.DefaultLimit), QueryBool(c, "include_fleet_median", false), AsOf(c), databases, logger, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #4198: query_text already had a 2000-character cap before this tool had a full_text opt-in at
               all, so the viewer keeps that exact number through the previewLength overload -- QueryBool
               still lets an operator ask for the whole statement via ?full_text=true, but the default (no
               query override) reproduces the page exactly as it always rendered. */
            /* #5234: the History button's read. The same refusal ladder as get_query_store_plan_xml: a key that is present
               but unreadable is refused, never defaulted. */
            ["get_resource_use_by_tag"] = (c, pg, an) => DarlingMcpQueryTagRollupTools.GetResourceUseByTag(pg, Server(c), Hours(c, 24), Str(c, "tag"), Rows(c, "limit", DarlingMcpQueryTagRollupTools.DefaultLimit), as_of: AsOf(c), logger: logger, cancellationToken: c.RequestAborted),
            ["get_query_store_query_history"] = (c, pg, an) => !RequireText(c, "database_name", out var qshDatabase)
                ? MissingParam("database_name")
                : !OptionalLong(c, "query_id", out var qshQueryId) ? UnparseableParam("query_id")
                : qshQueryId is null ? MissingParam("query_id")
                : DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory(pg, qshDatabase, qshQueryId.Value, Server(c), Hours(c, 24), AsOf(c), c.RequestAborted),
            ["get_forced_plans"] = (c, pg, an) => DatabaseNames(c) is { } fpDatabases
                ? DarlingMcpForcedPlanTools.GetForcedPlans(pg, Server(c), Hours(c, 24), AsOf(c), fpDatabases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_query_store_hints"] = (c, pg, an) => DatabaseNames(c) is { } qshHintDatabases
                ? DarlingMcpForcedPlanTools.GetQueryStoreHints(pg, Server(c), Hours(c, 24), AsOf(c), qshHintDatabases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_plan_force_precheck"] = (c, pg, an) => !RequireText(c, "database_name", out var pfDatabase)
                ? MissingParam("database_name")
                : !OptionalLong(c, "query_id", out var pfQueryId) ? UnparseableParam("query_id")
                : pfQueryId is null ? MissingParam("query_id")
                : !OptionalLong(c, "plan_id", out var pfPlanId) ? UnparseableParam("plan_id")
                : pfPlanId is null ? MissingParam("plan_id")
                : !OptionalLong(c, "compare_plan_id", out var pfCompareId) ? UnparseableParam("compare_plan_id")
                : DarlingMcpPlanForcePrecheckTools.GetPlanForcePrecheck(pg, pfDatabase, pfQueryId.Value, pfPlanId.Value, Server(c), pfCompareId, Hours(c, 24), AsOf(c), logger, c.RequestAborted),
            ["get_query_store_aborts"] = (c, pg, an) => DarlingMcpQueryStoreAbortsTools.GetQueryStoreAborts(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), DatabaseFilter.One(Str(c, "database_name")), AsOf(c), QueryBool(c, "full_text", false), 2000, c.RequestAborted),
            ["get_cpu_by_database"] = (c, pg, an) => DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), AsOf(c), c.RequestAborted),
            ["get_load_by_caller"] = (c, pg, an) => DarlingMcpWhoDroveItTools.GetLoadByCallerCore(pg, Server(c), Hours(c, 1), Rows(c, "top", 20), Rows(c, "statements", 3), AsOf(c), c.RequestAborted),
            ["get_query_pileups"] = (c, pg, an) => DarlingMcpWhoDroveItTools.GetQueryPileupsCore(pg, Server(c), Hours(c, 24), Rows(c, "min_sessions", 5), Rows(c, "top", 10), AsOf(c), c.RequestAborted),
            ["get_query_store_top"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDataTools.GetQueryStoreTop(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), databases, as_of: AsOf(c), execution_type: Str(c, "execution_type"), module_name: Str(c, "module_name"), full_text: QueryBool(c, "full_text", false), previewLength: 2000, cancellationToken: c.RequestAborted, tag: Str(c, "tag"), responseByteBudget: null)
                : DatabaseNamesRefusal(c),
            ["get_long_query_completions"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpLongQueryTools.GetLongQueryCompletions(pg, Server(c), Hours(c, 24), Rows(c, "limit", 30), AsOf(c), databases, logger, c.RequestAborted, tag: Str(c, "tag"))
                : DatabaseNamesRefusal(c),
            ["get_server_properties"] = (c, pg, an) => DarlingMcpDataTools.GetServerProperties(pg, Server(c), c.RequestAborted),
            ["get_tempdb_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpDataTools.GetTempDbTrend(pg, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_top_procedures_by_cpu"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDataTools.GetTopProceduresRanked(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), databases, as_of: AsOf(c), detail: Str(c, "detail") ?? "summary", order_by: Str(c, "order_by"), cancellationToken: c.RequestAborted, logger: logger, tag: Str(c, "tag"))
                : DatabaseNamesRefusal(c),
            ["get_top_queries_by_cpu"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDataTools.GetTopQueriesRanked(pg, Server(c), Hours(c, 24), Rows(c, "top", 20), databases, QueryBool(c, "parallel_only", false), QueryInt(c, "min_dop", null, 0), group_by: "query_hash", as_of: AsOf(c), detail: Str(c, "detail") ?? "summary", order_by: Str(c, "order_by"), cancellationToken: c.RequestAborted, tag: Str(c, "tag"))
                : DatabaseNamesRefusal(c),
            ["get_pg_top_queries"] = (c, pg, an) => DarlingMcpPgStatementTools.GetPgTopQueries(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), full_text: QueryBool(c, "full_text", true), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            /* query_id arrives as TEXT and is passed through as text (#2548): a queryid that made a
               round trip through a JSON number has already been rounded, and the tool rejects one it
               cannot parse exactly rather than silently matching nothing. */
            ["get_pg_plans"] = (c, pg, an) => DarlingMcpPgPlanTools.GetPgPlans(pg, Server(c), Hours(c, 24), Rows(c, "limit", 10), Str(c, "query_id"), AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_plan_capture_readiness"] = (c, pg, an) => DarlingMcpPgPlanTools.GetPgPlanCaptureReadiness(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_logging_audit"] = (c, pg, an) => DarlingMcpPgLoggingAuditTools.GetPgLoggingAudit(pg, Server(c), cancellationToken: c.RequestAborted),
            ["get_pg_wraparound_risk"] = (c, pg, an) => DarlingMcpPgWraparoundTools.GetPgWraparoundRisk(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_xmin_horizon"] = (c, pg, an) => DarlingMcpPgXminTools.GetPgXminHorizon(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_replication_slots"] = (c, pg, an) => DarlingMcpPgSlotTools.GetPgReplicationSlots(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_autovacuum_health"] = (c, pg, an) => DarlingMcpPgAutovacuumTools.GetPgAutovacuumHealth(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_io_stats"] = (c, pg, an) => DarlingMcpPgIoTools.GetPgIoStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_wait_stats"] = (c, pg, an) => DarlingMcpPgWaitTools.GetPgWaitStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_cpu_utilization"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpPgCpuUtilizationTools.GetPgCpuUtilization(pg, Server(c), Hours(c, 4), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_pg_wait_sampling"] = (c, pg, an) => DarlingMcpPgWaitSamplingTools.GetPgWaitSampling(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_kernel_stats"] = (c, pg, an) => DarlingMcpPgKernelStatsTools.GetPgKernelStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_predicate_stats"] = (c, pg, an) => DarlingMcpPgPredicateTools.GetPgPredicateStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_index_bloat"] = (c, pg, an) => DarlingMcpPgIndexTools.GetPgIndexBloat(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), QueryBool(c, "answered_only", false), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_column_stats"] = (c, pg, an) => DarlingMcpPgIndexTools.GetPgColumnStats(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_buffer_usage"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgBufferUsage(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_extensions"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgExtensions(pg, Server(c), Hours(c, 168), Rows(c, "limit", 50), Str(c, "database_name"), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_lock_stats"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgLockStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_write_stats"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgWriteStats(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_server_config"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgServerConfig(pg, Server(c), Rows(c, "limit", 100), QueryBool(c, "include_defaults", false), cancellationToken: c.RequestAborted),
            ["get_pg_server_config_changes"] = (c, pg, an) => DarlingMcpPgServerStateTools.GetPgServerConfigChanges(pg, Server(c), Hours(c, 168), Rows(c, "limit", 100), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_deadlocks"] = (c, pg, an) => DarlingMcpPgDeadlockTools.GetPgDeadlocks(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_deadlock_detail"] = (c, pg, an) => DarlingMcpPgDeadlockTools.GetPgDeadlockDetail(pg, Server(c), Str(c, "deadlock_hash"), Rows(c, "limit", 5), cancellationToken: c.RequestAborted),
            ["get_pg_log_events"] = (c, pg, an) => DarlingMcpPgLogEventTools.GetPgLogEvents(pg, Server(c), Hours(c, 24), Str(c, "family"), Str(c, "min_severity"), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_wait_trend"] = (c, pg, an) => DarlingMcpPgTrendTools.GetPgWaitTrend(pg, Server(c), Str(c, "wait_event"), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_query_duration_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpPgTrendTools.GetPgQueryDurationTrend(pg, Server(c), Str(c, "queryid"), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_pg_io_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpPgTrendTools.GetPgIoTrend(pg, Server(c), Str(c, "backend_type"), Str(c, "context"), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_pg_database_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpPgTrendTools.GetPgDatabaseTrend(pg, Server(c), Str(c, "database"), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_pg_replication_stats"] = (c, pg, an) => DarlingMcpPgReplicationStatsTools.GetPgReplicationStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_blocking"] = (c, pg, an) => DarlingMcpPgBlockingTools.GetPgBlocking(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_database_stats"] = (c, pg, an) => DarlingMcpPgDatabaseTools.GetPgDatabaseStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_index_usage"] = (c, pg, an) => DarlingMcpPgIndexUsageTools.GetPgIndexUsage(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_table_bloat"] = (c, pg, an) => DarlingMcpPgTableBloatTools.GetPgTableBloat(pg, Server(c), Hours(c, 168), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_pg_session_states"] = (c, pg, an) => DarlingMcpPgSessionStatesTools.GetPgSessionStates(pg, Server(c), Hours(c, 24), Rows(c, "limit", 25), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_wait_stats"] = (c, pg, an) => DarlingMcpDataTools.GetWaitStats(pg, Server(c), Hours(c, 24), Rows(c, "limit", 20), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_wait_trend"] = (c, pg, an) => RequireText(c, "wait_type", out var waitType)
                ? (OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                    ? DarlingMcpDataTools.GetWaitTrend(pg, waitType, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                    : UnparseableParam("bucket_minutes"))
                : MissingParam("wait_type"),
            ["get_wait_types"] = (c, pg, an) => DarlingMcpDataTools.GetWaitTypes(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["find_database"] = (c, pg, an) => !RequireText(c, "database_name", out var fdDatabase)
                ? MissingParam("database_name")
                : DarlingMcpFindDatabaseTools.FindDatabase(pg, fdDatabase, c.RequestAborted),
            ["list_servers"] = (c, pg, an) => DarlingMcpDataTools.ListServers(pg, c.RequestAborted),
            ["list_query_tags"] = (c, pg, an) => DarlingMcpQueryTagTools.ListQueryTags(pg, c.RequestAborted),

            /* ── trends ── */
            /* #3897: the trends a page charts pass the CHART budget — a browser draws far more points than a model
               should read — and bind bucket_minutes, refusing a value that is not a number rather than quietly
               sizing the points itself (the OptionalDouble rule). */
            ["get_file_io_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? (DatabaseNames(c) is { } databases
                    ? DarlingMcpTrendTools.GetFileIoTrend(pg, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, databases, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                    : DatabaseNamesRefusal(c))
                : UnparseableParam("bucket_minutes"),
            ["get_memory_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpTrendTools.GetMemoryTrend(pg, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_server_trend"] = (c, pg, an) => RequireText(c, "metric", out var serverTrendMetric)
                ? (OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                    ? DarlingMcpServerTrendTools.GetServerTrend(pg, serverTrendMetric, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, Str(c, "clerk_types"), Str(c, "names"), TrendBudget.Chart, cancellationToken: c.RequestAborted)
                    : UnparseableParam("bucket_minutes"))
                : MissingParam("metric"),
            ["get_perfmon_trend"] = (c, pg, an) => RequireText(c, "counter_name", out var counter)
                ? (OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                    ? DarlingMcpTrendTools.GetPerfmonTrend(pg, counter, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                    : UnparseableParam("bucket_minutes"))
                : MissingParam("counter_name"),
            ["get_procedure_duration_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? (DatabaseNames(c) is { } databases
                    ? DarlingMcpTrendTools.GetProcedureDurationTrend(pg, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, databases, TrendBudget.Chart, Str(c, "procedure_name"), c.RequestAborted)
                    : DatabaseNamesRefusal(c))
                : UnparseableParam("bucket_minutes"),
            ["get_query_duration_trend"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? (DatabaseNames(c) is { } databases
                    ? DarlingMcpTrendTools.GetQueryDurationTrend(pg, Server(c), Hours(c, 24), AsOf(c), bucketMinutes, databases, TrendBudget.Chart, cancellationToken: c.RequestAborted)
                    : DatabaseNamesRefusal(c))
                : UnparseableParam("bucket_minutes"),
            ["get_query_store_duration_trend"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpTrendTools.GetQueryStoreDurationTrend(pg, Server(c), Hours(c, 24), AsOf(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_query_trend"] = (c, pg, an) => RequireText(c, "query_hash", out var queryHash)
                ? (RequireText(c, "database_name", out var db)
                    ? DarlingMcpTrendTools.GetQueryTrend(pg, queryHash, db, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted)
                    : MissingParam("database_name"))
                : MissingParam("query_hash"),

            /* ── health / overview ── */
            ["get_server_summary"] = (c, pg, an) => DarlingMcpHealthTools.GetServerSummary(pg, Server(c), registryState, logger, c.RequestAborted),
            /* #6291: the web-only Overview baselines read. A key that is present but unreadable is refused, never defaulted. */
            ["get_overview_baselines"] = (c, pg, an) => !RequireText(c, "window_start", out var baselineWindowStart)
                ? MissingParam("window_start")
                : DarlingWebOverviewBaselines.GetOverviewBaselines(pg, an, Server(c), baselineWindowStart, c.RequestAborted),
            ["get_daily_summary"] = (c, pg, an) => DarlingMcpHealthTools.GetDailySummary(pg, Server(c), Str(c, "summary_date"), registryState, logger, c.RequestAborted),
            ["get_daily_summary_range"] = (c, pg, an) => DarlingMcpHealthTools.GetDailySummaryRange(pg, Server(c), QueryInt(c, "days_back", null, 30), AsOf(c), registryState, logger, c.RequestAborted),
            ["get_fleet_overview"] = (c, pg, an) => DarlingMcpFleetTools.GetFleetOverview(pg, Hours(c, DefaultFleetHours), Str(c, "detail") ?? "summary", QueryBool(c, "worst_only", false), Str(c, "band"), registryState: registryState, logger: logger, cancellationToken: c.RequestAborted),
            ["get_ag_health"] = (c, pg, an) => DarlingMcpAgTools.GetAgHealth(pg, Server(c), Rows(c, "limit", DarlingMcpAgTools.DefaultGroupLimit), c.RequestAborted),
            ["get_store_metrics"] = (c, pg, an) => DarlingMcpStoreMetricsTools.GetStoreMetrics(pg, QueryInt(c, "days_back", null, 30), Str(c, "object_kind"), Str(c, "object_name"), Rows(c, "limit", DarlingMcpStoreMetricsTools.DefaultLimit), c.RequestAborted),
            ["get_store_log"] = (c, pg, an) => DarlingMcpStoreLogTools.GetStoreLog(pg, Hours(c, 24), Rows(c, "limit", DarlingMcpStoreLogTools.DefaultRetainedLimit), AsOf(c), c.RequestAborted),
            ["get_store_query_history"] = (c, pg, an) => DarlingMcpStoreQueryHistoryTools.GetStoreQueryHistory(pg, Str(c, "query_id"), Str(c, "role"), Hours(c, DarlingMcpStoreQueryHistoryTools.DefaultHours), Rows(c, "top", DarlingMcpStoreQueryHistoryTools.DefaultTop), logger: logger, cancellationToken: c.RequestAborted),
            ["get_store_query_stats"] = (c, pg, an) => DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(pg, Str(c, "role"), Str(c, "order_by") ?? "total_time", Rows(c, "top", DarlingMcpStoreQueryStatsTools.DefaultTop), QueryBool(c, "full_text", false), c.RequestAborted),
            /* #4214 part 2: postgresConfig rides by closure (this method's own doc comment), the same way
               logger does for get_sweep_reports two screens up. StoreHostProfileCache.Shared as a direct
               static reference, not a new BuildReadDispatch parameter (round-1 review, Medium 2) — unlike
               postgresConfig, the cache instance never varies by caller, so no threading is needed. #4203:
               c.RequestAborted now reaches the gather via GetOrGatherAsync's linked token. */
            ["get_store_host"] = (c, pg, an) => DarlingMcpStoreHostTools.GetStoreHost(pg, postgresConfig, StoreHostProfileCache.Shared, c.RequestAborted),
            ["get_collector_cost"] = (c, pg, an) => DarlingMcpCollectorCostTools.GetCollectorCost(pg, QueryInt(c, "days_back", null, 7), Str(c, "collector_name"), c.RequestAborted),
            ["get_collector_stall_probes"] = (c, pg, an) => DarlingMcpStallProbeTools.GetCollectorStallProbes(pg, Server(c), QueryInt(c, "days_back", null, 7), Rows(c, "limit", DarlingMcpStallProbeTools.DefaultLimit), c.RequestAborted),
            ["get_oversized_plan_backlog"] = (c, pg, an) => DarlingMcpOversizedPlanBacklogTools.GetOversizedPlanBacklog(pg, Server(c), QueryBool(c, "include_rows", false), Rows(c, "limit", DarlingMcpOversizedPlanBacklogTools.DefaultLimit), c.RequestAborted),
            ["get_slow_reads"] = (c, pg, an) => DarlingMcpSlowReadTools.GetSlowReads(pg, Hours(c, DarlingMcpSlowReadTools.DefaultHours), Server(c), Str(c, "surface"), Str(c, "route"), Rows(c, "limit", DarlingMcpSlowReadTools.DefaultLimit), false, c.RequestAborted),
            ["get_read_latency"] = (c, pg, an) => DarlingMcpReadLatencyTools.GetReadLatency(pg, Hours(c, DarlingMcpReadLatencyTools.DefaultHours), Str(c, "surface"), Str(c, "route"), Rows(c, "limit", DarlingMcpReadLatencyTools.DefaultLimit), readAdmission, c.RequestAborted),

            /* ── latch / spinlock ── */
            ["get_latch_stats"] = (c, pg, an) => DarlingMcpLatchSpinlockTools.GetLatchStats(pg, Server(c), Hours(c, 24), Rows(c, "top", 10), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_spinlock_stats"] = (c, pg, an) => DarlingMcpLatchSpinlockTools.GetSpinlockStats(pg, Server(c), Hours(c, 24), Rows(c, "top", 10), as_of: AsOf(c), cancellationToken: c.RequestAborted),

            /* ── memory grants ── */
            ["get_memory_grants"] = (c, pg, an) => OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                ? DarlingMcpMemoryGrantTools.GetMemoryGrants(pg, Server(c), Hours(c, 1), AsOf(c), QueryBool(c, "chart", false), bucketMinutes, TrendBudget.Chart, c.RequestAborted)
                : UnparseableParam("bucket_minutes"),
            ["get_memory_pressure_events"] = (c, pg, an) => DarlingMcpMemoryGrantTools.GetMemoryPressureEvents(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_resource_semaphore"] = (c, pg, an) => DarlingMcpMemoryGrantTools.GetResourceSemaphore(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),

            /* ── object / index stats ── */
            ["get_database_sizes"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpObjectStatsTools.GetDatabaseSizes(pg, Server(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_pvs_stats"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpPvsTools.GetPvsStats(pg, Server(c), QueryInt(c, "trend_hours_back", null, 0), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_index_usage"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpObjectStatsTools.GetIndexUsage(pg, Server(c), databases, Rows(c, "limit", 200), c.RequestAborted)
                : DatabaseNamesRefusal(c),
            /* #4258: limit defaults to 75 on the MCP signature now (was an uncapped-looking 200-row hard
               fetch with no parameter at all), sized under the shared response budget. The web viewer has
               always effectively received that old 200-row fetch (there was no smaller cap anywhere in the
               path), so the row pins its OWN limit at 200 - the same value get_index_usage's row above pins
               for the identical reason - rather than silently dropping to the new MCP default. 200 is well
               under both McpHelpers.MaxTop and MaxRowLimit (1000 each), so the value is never refused or
               reclamped by either validation layer. */
            ["get_object_locking"] = (c, pg, an) => HasObjectLockingSelector(c) ? ObjectLockingDetail(c, pg)
                : DatabaseNames(c) is { } databases
                    ? DarlingMcpObjectStatsTools.GetObjectLockingWithHeatAsync(pg, Server(c), Rows(c, "limit", 200), registryState, databases, c.RequestAborted)
                    : DatabaseNamesRefusal(c),
            ["get_table_index_sizes"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpObjectStatsTools.GetTableIndexSizes(pg, Server(c), databases, c.RequestAborted)
                : DatabaseNamesRefusal(c),

            /* ── plan cache / scheduler ── */
            ["get_cpu_scheduler_pressure"] = (c, pg, an) => DarlingMcpPlanCacheSchedulerTools.GetCpuSchedulerPressure(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_plan_cache_bloat"] = (c, pg, an) => DarlingMcpPlanCacheSchedulerTools.GetPlanCacheBloat(pg, Server(c), Hours(c, 24), as_of: AsOf(c), cancellationToken: c.RequestAborted),

            /* ── jobs ── */
            ["get_running_jobs"] = (c, pg, an) => DarlingMcpJobTools.GetRunningJobs(pg, Server(c), c.RequestAborted),
            ["get_agent_alerts"] = (c, pg, an) => DarlingMcpAgentAlertTools.GetAgentAlerts(pg, Server(c), Hours(c, 24), AsOf(c), c.RequestAborted),
            ["get_job_history"] = (c, pg, an) => OptionalInt(c, "min_duration_seconds", out var minDurationSeconds)
                ? DarlingMcpJobTools.GetJobHistory(pg, Server(c), Hours(c, 24), Str(c, "job_name"), Str(c, "status"), Str(c, "category"), Rows(c, "limit", 100), as_of: AsOf(c), min_duration_seconds: minDurationSeconds, cancellationToken: c.RequestAborted)
                : UnparseableParam("min_duration_seconds"),

            /* ── stored plan XML (READ; the analyze_*_plan compute family stays excluded) ── */
            ["get_plan_xml"] = (c, pg, an) => RequireText(c, "query_hash", out var queryHash)
                ? WrapPlanXmlAsync(DarlingMcpPlanTools.GetPlanXml(pg, queryHash, Server(c), Str(c, "database_name"), c.RequestAborted), queryHash, Str(c, "database_name"))
                : MissingParam("query_hash"),

            /* #5228: the grid-row plan reads. A key that is present but unreadable is REFUSED, never defaulted — a
               plan fetched for the wrong row is worse than none. */
            ["get_query_store_plan_xml"] = (c, pg, an) => !RequireText(c, "database_name", out var qsDatabase)
                ? MissingParam("database_name")
                : !OptionalLong(c, "query_id", out var qsQueryId) ? UnparseableParam("query_id")
                : qsQueryId is null ? MissingParam("query_id")
                : !OptionalLong(c, "plan_id", out var qsPlanId) ? UnparseableParam("plan_id")
                : WrapPlanXmlAsync(DarlingMcpPlanTools.GetQueryStorePlanXml(pg, qsDatabase, qsQueryId.Value, Server(c), qsPlanId, c.RequestAborted),
                    PlanIdentity(("database_name", qsDatabase), ("query_id", qsQueryId), ("plan_id", qsPlanId))),
            ["get_procedure_plan_xml"] = (c, pg, an) => !RequireText(c, "sql_handle", out var procSqlHandle)
                ? MissingParam("sql_handle")
                : !DarlingMcpPlanTools.IsSqlHandle(procSqlHandle) ? UnparseableParam("sql_handle", "Expected a hex sql_handle such as 0x0300...")
                : WrapPlanXmlAsync(DarlingMcpPlanTools.GetProcedurePlanXml(pg, procSqlHandle, Server(c), c.RequestAborted),
                    PlanIdentity(("sql_handle", procSqlHandle))),
            ["get_active_query_plan_xml"] = (c, pg, an) => !RequireText(c, "collection_time", out var snapTime)
                ? MissingParam("collection_time")
                : !OptionalInt(c, "session_id", out var snapSession) ? UnparseableParam("session_id")
                : snapSession is null ? MissingParam("session_id")
                : !OptionalInt(c, "request_id", out var snapRequest) ? UnparseableParam("request_id")
                : !DarlingMcpPlanTools.TryParseCollectionTime(snapTime, out _) ? UnparseableParam("collection_time", "Expected the collection_time exactly as get_active_queries returned it (ISO 8601).")
                : First(c, "live") is { } liveText && !bool.TryParse(liveText, out _) ? UnparseableParam("live", "Expected true or false.")
                : WrapPlanXmlAsync(DarlingMcpPlanTools.GetActiveQueryPlanXml(pg, snapTime, snapSession.Value, Server(c), snapRequest ?? 0, QueryBool(c, "live", false), c.RequestAborted),
                    PlanIdentity(("collection_time", snapTime), ("session_id", snapSession), ("request_id", snapRequest ?? 0), ("live", QueryBool(c, "live", false)))),
            /* #5233: the repro script. A key that is present but unreadable is refused, never defaulted. The JSON passes
               through unwrapped. */
            ["get_query_repro_script"] = (c, pg, an) => !RequireText(c, "kind", out var reproKind)
                ? MissingParam("kind")
                : !OptionalLong(c, "query_id", out var reproQueryId) ? UnparseableParam("query_id")
                : !OptionalLong(c, "plan_id", out var reproPlanId) ? UnparseableParam("plan_id")
                : !OptionalInt(c, "session_id", out var reproSession) ? UnparseableParam("session_id")
                : !OptionalInt(c, "request_id", out var reproRequest) ? UnparseableParam("request_id")
                : DarlingMcpPlanTools.GetQueryReproScript(pg, reproKind, Server(c), Str(c, "database_name"), Str(c, "query_hash"),
                    reproQueryId, reproPlanId, Str(c, "collection_time"), reproSession, request_id: reproRequest ?? 0, cancellationToken: c.RequestAborted),

            /* #5236: the Blocking and Deadlocks grids' plan reads, bound by the same rule: a key that is present but
               unreadable is REFUSED, never defaulted. Both times are the row's strings and are matched for equality, so
               they are checked against the tool's own parse here and sent on untouched. An omitted ecid is 0, as the grid
               carries it, and an omitted side is the blocked side; the tool owns the refusal of any other side, so the web
               and MCP surfaces cannot disagree about what one means. The identity echoes the side as the tool read it. The
               row's database_name travels on both reads, so two databases that share every other part of the key (an Azure
               master target collects several under one server) each read their own plan; a row without one sends none. */
            ["get_blocking_plan_xml"] = (c, pg, an) => !RequireText(c, "event_time", out var blockEventTime)
                ? MissingParam("event_time")
                : !OptionalInt(c, "blocked_spid", out var blockedSpid) ? UnparseableParam("blocked_spid")
                : blockedSpid is null ? MissingParam("blocked_spid")
                : !OptionalInt(c, "blocking_spid", out var blockingSpid) ? UnparseableParam("blocking_spid")
                : blockingSpid is null ? MissingParam("blocking_spid")
                : !OptionalInt(c, "blocked_ecid", out var blockedEcid) ? UnparseableParam("blocked_ecid")
                : !OptionalInt(c, "blocking_ecid", out var blockingEcid) ? UnparseableParam("blocking_ecid")
                : !DarlingMcpPlanTools.TryParseCollectionTime(blockEventTime, out _) ? UnparseableParam("event_time", "Expected the event_time exactly as get_blocking returned it (ISO 8601).")
                : WrapPlanXmlAsync(DarlingMcpPlanTools.GetBlockingPlanXml(pg, blockEventTime, blockedSpid.Value, blockingSpid.Value, Server(c), blockedEcid ?? 0, blockingEcid ?? 0, Str(c, "side"), Str(c, "database_name"), c.RequestAborted),
                    PlanIdentity(("event_time", blockEventTime), ("blocked_spid", blockedSpid), ("blocked_ecid", blockedEcid ?? 0), ("blocking_spid", blockingSpid), ("blocking_ecid", blockingEcid ?? 0),
                        ("side", string.Equals(Str(c, "side"), "blocking", StringComparison.OrdinalIgnoreCase) ? "blocking" : "blocked"), ("database_name", Str(c, "database_name")))),
            ["get_deadlock_plan_xml"] = (c, pg, an) => !RequireText(c, "collection_time", out var deadlockCollection)
                ? MissingParam("collection_time")
                : !RequireText(c, "deadlock_time", out var deadlockTime) ? MissingParam("deadlock_time")
                : !DarlingMcpPlanTools.TryParseCollectionTime(deadlockCollection, out _) ? UnparseableParam("collection_time", "Expected the collection_time exactly as get_deadlocks returned it (ISO 8601).")
                : !DarlingMcpPlanTools.TryParseCollectionTime(deadlockTime, out _) ? UnparseableParam("deadlock_time", "Expected the deadlock_time exactly as get_deadlocks returned it (ISO 8601).")
                : WrapPlanXmlAsync(DarlingMcpPlanTools.GetDeadlockPlanXml(pg, deadlockCollection, deadlockTime, Server(c), Str(c, "victim_process_id"), Str(c, "database_name"), c.RequestAborted),
                    PlanIdentity(("collection_time", deadlockCollection), ("deadlock_time", deadlockTime), ("victim_process_id", Str(c, "victim_process_id")), ("database_name", Str(c, "database_name")))),

            /* ── default trace ── */
            ["get_default_trace_events"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpDefaultTraceTools.GetDefaultTraceEvents(pg, Server(c), Hours(c, 24), Rows(c, "limit", 100), databases, AsOf(c), null, c.RequestAborted)
                : DatabaseNamesRefusal(c),

            /* ── system_health parse-on-read family ── */
            ["get_health_parser_cpu_tasks"] = (c, pg, an) => DarlingMcpHealthParserTools.GetCPUTasks(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_io_issues"] = (c, pg, an) => DarlingMcpHealthParserTools.GetIOIssues(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_memory_broker"] = (c, pg, an) => DarlingMcpHealthParserTools.GetMemoryBroker(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_memory_conditions"] = (c, pg, an) => DarlingMcpHealthParserTools.GetMemoryConditions(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_memory_node_oom"] = (c, pg, an) => DarlingMcpHealthParserTools.GetMemoryNodeOOM(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_scheduler_issues"] = (c, pg, an) => DarlingMcpHealthParserTools.GetSchedulerIssues(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_severe_errors"] = (c, pg, an) => DatabaseNames(c) is { } databases
                ? DarlingMcpHealthParserTools.GetSevereErrors(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), databases, AsOf(c), null, c.RequestAborted)
                : DatabaseNamesRefusal(c),
            ["get_health_parser_significant_waits"] = (c, pg, an) => DarlingMcpHealthParserTools.GetSignificantWaits(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted),
            ["get_health_parser_system_health"] = (c, pg, an) => !QueryBool(c, "chart", false)
                ? DarlingMcpHealthParserTools.GetSystemHealth(pg, Server(c), Hours(c, 24), Rows(c, "limit", 50), as_of: AsOf(c), cancellationToken: c.RequestAborted)
                : OptionalInt(c, "bucket_minutes", out var bucketMinutes)
                    ? DarlingMcpHealthParserTools.GetSystemHealthChart(pg, Server(c), Hours(c, 24), bucketMinutes, AsOf(c), cancellationToken: c.RequestAborted)
                    : UnparseableParam("bucket_minutes"),
            // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
            ["get_finops_inventory"] = (c, pg, an) => DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(pg, First(c, "view") ?? "", QueryInt(c, "limit", null, DarlingMcpFinOpsInventoryTools.DefaultLimit), QueryBool(c, "include_removed", false), c.RequestAborted),
            ["get_finops_recommendations"] = (c, pg, an) => DarlingMcpFinOpsRecommendationsTools.GetFinOpsRecommendations(pg, Server(c), logger, c.RequestAborted),
            // FinOps web parity (#4843), set A ends.
            // Each set belongs to one series of changes. Append to your own set only,
            // so the two series never edit the same lines of this read list.
            // Entries keep the read list's existing order and form.
            // Set A and set B are separated on purpose: keep this gap.
            //
            //
            //
            // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
            ["get_finops"] = (c, pg, an) => DarlingMcpFinOpsTools.GetFinOps(pg, First(c, "view") ?? "", Server(c), Hours(c, 24), QueryInt(c, "limit", null, 10), Str(c, "database_name"), QueryBool(c, "full_text", false), Str(c, "object_name"), logger, c.RequestAborted),
            // FinOps web parity (#4843), set B ends.
        };

        /* #4442: the test-only extra entry, added ONLY when a test set it -- never in a production
           run, since TestOnlyExtraDispatchEntry stays null unless Darling.Tests assigns it. #4782: it is
           per async flow, so only the flow that set it gets the extra key; a test running at the same time
           in another flow builds its dispatch without it. */
        if (TestOnlyExtraDispatchEntry is { } extra)
        {
            dispatch[extra.Name] = extra.Handler;
        }

        return dispatch;
    }

    /* ─────────────────────────── response mapping ─────────────────────────── */

    /// <summary>#5506 part 3: one web read's answer: what the caller gets, the text the read cache may store (null when
    /// it may not), and the outcome a caller that joined the read records (null when the read recorded nothing).</summary>
    private sealed record WebReadAnswer(IResult Answer, string? Text, ReadOutcome? Outcome);

    /// <summary>How a tool's returned string maps to an HTTP outcome — the four kinds of outcome a tool has
    /// (data or a miss; a failure; a refusal; a bare string nothing shapes any more), each with its code.</summary>
    internal enum ToolResponseKind
    {
        /// <summary>A serialized JSON object/array (data, or the {"status", ...} miss envelope) — 200 passthrough.</summary>
        JsonPassthrough,

        /// <summary>The tool caught an exception — the <c>{"status":"error", ...}</c> envelope
        /// <c>McpHelpers.FormatError</c> builds (#3653 Q11), or the bare "Error during ..." sentence it built
        /// before that ruling — HTTP 500.</summary>
        ServerError,

        /// <summary>The tool REFUSED the request as given — the <c>{"status":"invalid", ...}</c> envelope
        /// <c>McpHelpers.Refusal</c> builds (#3739: a parameter it cannot honor, a missing required one, a server
        /// name that resolves to nothing) or a write tool's <c>Outcome("invalid", …)</c> for a body that will not
        /// parse — a client-correctable HTTP 400 with the envelope itself as the body, on the read surface exactly
        /// as the mute-rule write routes have always answered <c>invalid</c>.</summary>
        Refusal,

        /// <summary>Any other bare string — a client-correctable HTTP 400, wrapped as <c>{"error": ...}</c>.
        /// Since #3739 the validators and resolvers no longer produce one; what still can is a store fault the
        /// resolver reports as a sentence, and the two miss sentences the <c>list_servers</c> tools return on an
        /// empty registry (#3703's lane).</summary>
        ClientError,
    }

    /// <summary>
    /// Classifies a tool's returned string. The two status-word envelopes are tested FIRST, because both begin
    /// with <c>{</c> and the passthrough sniff would otherwise answer 200 over them — which is exactly what it
    /// did for the PostgreSQL tools before this ordering existed: the error envelope
    /// (<see cref="McpHelpers.IsErrorEnvelope"/> — <c>{"status":"error", ...}</c>, what every tool catch returns
    /// since #3653 Q11) is the tool's caught-exception shape (HTTP 500); the refusal envelope
    /// (<see cref="McpHelpers.IsRefusalEnvelope"/> — <c>{"status":"invalid", ...}</c>, what every validator and
    /// both resolvers return since #3739, and what the write tools have always returned for a body that will
    /// not parse) is the client-correctable shape (HTTP 400). Any other leading <c>{</c> or <c>[</c> (after any
    /// whitespace) is a serialized object/array and passes through; the bare <c>"Error during ..."</c> sentence
    /// the pre-#3653 helper built is kept as a server error so an un-migrated producer still maps to 500 rather
    /// than to a client-correctable 400; anything else is a bare string and maps to 400.
    ///
    /// <para><b>The ruling this encodes (#3739).</b> #3719 pinned that a hand-built <c>Status("error", …)</c>
    /// refusal answered 500 here, and said the word that tells a refusal from a failure was a ruling it did
    /// not make. The ruling is <c>invalid</c>, widened from the write surface's "a body that will not parse" to
    /// "the request as given cannot be served", and this arm is where it lands: a refusal is 400 (not the 500 a
    /// fault gets, not the 200 the <c>{</c>-sniff would give it), the envelope is the body, and the read and
    /// write surfaces read it by ONE rule — <see cref="MuteRuleEnvelopeStatus"/> goes through this same
    /// classifier. Pure so the whole mapping is unit-testable.</para>
    /// </summary>
    internal static ToolResponseKind ClassifyToolResponse(string result)
    {
        if (McpHelpers.IsErrorEnvelope(result))
        {
            return ToolResponseKind.ServerError;
        }

        if (McpHelpers.IsRefusalEnvelope(result))
        {
            return ToolResponseKind.Refusal;
        }

        var trimmed = result.AsSpan().TrimStart();
        if (trimmed.Length > 0 && (trimmed[0] == '{' || trimmed[0] == '['))
        {
            return ToolResponseKind.JsonPassthrough;
        }

        if (result.StartsWith("Error during ", StringComparison.Ordinal))
        {
            return ToolResponseKind.ServerError;
        }

        if (result.StartsWith(Mcp.DarlingServerResolver.RegistryReadFaultPrefix, StringComparison.Ordinal))
        {
            return ToolResponseKind.ServerError;
        }

        return ToolResponseKind.ClientError;
    }

    /// <summary>The read surface's HTTP answer. A refusal is the envelope itself as the body under 400 — the
    /// shape the mute-rule write routes have always answered <c>invalid</c> with, so <c>status</c>, <c>message</c>
    /// and <c>hints.parameter</c> reach a web client exactly as they reach an MCP client (#3739).
    ///
    /// <para><b>#4283: the ServerError arm never puts the tool's caught-exception text on the wire.</b> A
    /// tool's own catch turns <c>ex.Message</c> into <c>McpHelpers.ErrorMessageOf(result)</c> — a PostgreSQL
    /// error starting with its SQLSTATE and the server's message, or a failed store connection naming its host
    /// and port — before this method ever sees it. The sentence is logged ONCE through
    /// <see cref="DarlingWebFailureLog.Report(ILogger,string,long,string,System.Threading.CancellationToken)"/> (so the service log still names
    /// what failed) and the body becomes <see cref="DarlingWebFailureLog.Body(string)"/>'s fixed message, under
    /// <see cref="DarlingWebFailureLog.StatusCode(string)"/> — 503 when the sentence carries a caught
    /// statement_timeout's 57014 token, 500 otherwise, the same split #4281's backstop gives an UNCAUGHT one.
    /// The bare <c>"Error during "</c> arm <see cref="ClassifyToolResponse"/> keeps for an un-migrated producer
    /// maps here too, for the same reason.</para>
    /// </summary>
    internal static IResult ToHttpResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        SensitiveStatements.JudgeBudget? sweepBudget = null)
    {
        /* #6341 round 1: classified from the tool's own result BEFORE the sweep. The sweep can withhold the status word
           or the whole sentence (it fails closed when the judge runs out of time), and a classification read off that
           placeholder answered a store fault 200 or 400 with no log line. The status and the one log line never depend on
           the sweep. A ServerError is answered with a fixed body and writes none of the tool's text, so it is not swept. */
        var kind = ClassifyToolResponse(result);
        if (kind == ToolResponseKind.ServerError)
        {
            return ServerErrorResult(McpHelpers.ErrorMessageOf(result), route, logger, elapsedMs, requestAborted);
        }

        /* #4348: the statement filter's web sweep, over the body that is written; a body it cannot read is never written. */
        var swept = DarlingWebStatementSweep.Apply(result, sweepBudget);
        if (swept.Refused) return DarlingWebStatementSweep.Refusal(route, logger, elapsedMs, requestAborted);

        return kind switch
        {
            ToolResponseKind.JsonPassthrough => Results.Text(swept.Text, "application/json"),
            ToolResponseKind.Refusal => Results.Text(swept.Text, "application/json", statusCode: StatusCodes.Status400BadRequest),
            _ => Results.Json(new { error = swept.Text }, statusCode: StatusCodes.Status400BadRequest),
        };
    }

    /// <summary>
    /// #5678: ends the request the way #4276 does when the browser has left and a tool's own caught failure is about to be
    /// logged for it. A tool that catches its own exception hands back an <c>Error during ...</c> sentence and returns
    /// normally, so the <c>OperationCanceledException</c> catch never sees it; when the cause was the caller going away (a
    /// cancelled token can surface as a connect timeout rather than a cancellation), logging it as a read failure blames the
    /// service for a request nobody is waiting on. Throwing <c>OperationCanceledException(requestAborted)</c> hands it to the
    /// top-of-pipeline backstop (<see cref="DarlingWebFailureObserver"/>), which writes no line and no body for it. Only a
    /// result that classifies as <see cref="ToolResponseKind.ServerError"/> is touched: a payload or a client-correctable
    /// refusal is answered as before. Call it AFTER any latency sample, which is already right for a gone caller
    /// (<see cref="ReadOutcomeClassifier.ClassifySentence"/> says Cancelled).
    /// </summary>
    internal static void ThrowIfCallerGone(string result, CancellationToken requestAborted)
    {
        if (requestAborted.IsCancellationRequested && ClassifyToolResponse(result) == ToolResponseKind.ServerError)
        {
            throw new OperationCanceledException(requestAborted);
        }
    }

    /// <summary>The ServerError arm's body, factored out so both <see cref="ToHttpResult"/> and the
    /// <c>/api/read/*</c> loop's binding-layer catch (which holds the real <see cref="Exception"/>, not just
    /// its sentence) answer with the identical fixed shape.</summary>
    internal static IResult ServerErrorResult(string sentence, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted)
    {
        /* A write the store refused for privilege is an answer, not a fault: the 403 sentence, no failure line. One place for every route
           whose core caught the store's refusal as text. */
        if (DarlingWebFailureLog.TryWriteRefusal(sentence, out var refusal))
        {
            return ErrorResult(refusal, StatusCodes.Status403Forbidden);
        }

        DarlingWebFailureLog.Report(logger, route, elapsedMs, sentence, requestAborted);
        return Results.Json(DarlingWebFailureLog.Body(sentence), statusCode: DarlingWebFailureLog.StatusCode(sentence));
    }

    /* ─────────────────────────── query-string binding ─────────────────────────── */

    /// <summary>The server name from <c>?server=</c> (or the tool's own <c>?server_name=</c>); null when absent,
    /// which lets a tool auto-select a sole configured server exactly as the MCP surface does.</summary>
    private static string? Server(HttpContext context) => First(context, "server") ?? First(context, "server_name");

    /* ── the Locking page's index detail selector (#5311): web-only, never an MCP parameter ── */

    private static readonly string[] ObjectLockingSelectorKeys = ["detail_database", "detail_schema", "detail_table", "detail_index"];

    /// <summary>True when the request carries any <c>detail_*</c> selector key: the read then answers ONE index's four
    /// counters (<see cref="ObjectLockingDetail"/>) instead of the list. Without one the list read is untouched.</summary>
    private static bool HasObjectLockingSelector(HttpContext context) =>
        ObjectLockingSelectorKeys.Any(key => context.Request.Query.ContainsKey(key));

    /// <summary>
    /// #5311: the detail read of <c>get_object_locking</c>. The index is named exactly by <c>detail_database</c>,
    /// <c>detail_schema</c> and <c>detail_table</c> (each required) and <c>detail_index</c> (absent names a heap); each
    /// goes to the store as a bound parameter. A missing or blank name, a name over a <c>sysname</c>'s 128 characters or
    /// one holding NUL is refused with the usual <c>invalid</c> envelope before the store is touched; a blank database
    /// would otherwise be the "every database" filter, and a selector never widens.
    /// </summary>
    private static Task<string> ObjectLockingDetail(HttpContext context, NpgsqlDataSource postgres)
    {
        foreach (var key in new[] { "detail_database", "detail_schema", "detail_table" })
        {
            var v = First(context, key);
            if (v is null || string.IsNullOrWhiteSpace(v)) return MissingParam(key);
        }

        foreach (var key in ObjectLockingSelectorKeys)
        {
            var v = First(context, key);
            if (v is null) continue;
            if (v.Contains('\0'))
                return Task.FromResult(McpHelpers.Refusal(key, $"A {key} value holds a character an object name cannot hold."));
            if (v.Length > MaxDatabaseNameLength)
                return Task.FromResult(McpHelpers.Refusal(key, $"A {key} value is {v.Length} characters long; an object name is at most {MaxDatabaseNameLength} characters."));
        }

        return DarlingMcpObjectStatsTools.GetObjectLockingDetailAsync(
            postgres, Server(context), First(context, "detail_database")!, First(context, "detail_schema")!,
            First(context, "detail_table")!, First(context, "detail_index"), context.RequestAborted);
    }

    /// <summary>The hours-back window from <c>?hours=</c> (or the tool's own <c>?hours_back=</c>), else the tool's default.</summary>
    private static int Hours(HttpContext context, int def) => QueryInt(context, "hours", "hours_back", def);

    /// <summary>
    /// The window ANCHOR from <c>?as_of=</c>; null when absent, which is what makes the window end at now.
    /// Passed through UNVALIDATED on purpose — the tool owns the parse and the refusal message, so the web and
    /// MCP surfaces cannot disagree about what a bad anchor means, and the refusal envelope reaches the same
    /// <see cref="ToHttpResult"/> 400 mapping every other client-correctable outcome does.
    /// </summary>
    private static string? AsOf(HttpContext context) => First(context, "as_of");

    /// <summary>An optional text parameter; null when absent or empty (so the tool sees its own default).</summary>
    private static string? Str(HttpContext context, string key) => First(context, key);

    /* ── the database list (#5245): repeated ?database_name= keys, one per chosen database ── */

    /// <summary>The query key the picker's database list travels under, once per chosen database.</summary>
    private const string DatabaseNameKey = "database_name";

    /// <summary>The most databases one request may name, counted AFTER de-duplication.</summary>
    internal const int MaxDatabaseNames = 50;

    /// <summary>The longest database name a request may carry: a <c>sysname</c> is 128 characters.</summary>
    internal const int MaxDatabaseNameLength = 128;

    /// <summary>
    /// The most encoded bytes the <c>database_name</c> part of a query may take: <see cref="DatabaseKeyBytes"/>
    /// plus <c>encodeURIComponent</c>'s length of each name, summed over the distinct names. The picker (a later
    /// part of #5245) refuses a check that would pass it, so a hand-built request is the only way to reach this
    /// refusal, and 4,096 keeps a request well inside Kestrel's 8 KB request line.
    /// </summary>
    internal const int MaxDatabaseQueryBytes = 4096;

    /// <summary>The bytes <c>&amp;database_name=</c> takes in front of every name (1 + 13 + 1).</summary>
    internal const int DatabaseKeyBytes = 15;

    /// <summary>
    /// The databases a read is asked about, from the REPEATED <c>?database_name=</c> keys, as a
    /// <see cref="DatabaseFilter"/>; <c>null</c> when the request has to be REFUSED. Spell it
    /// <c>DatabaseNames(c) is { } databases ? tool(..., databases) : DatabaseNamesRefusal(c)</c>, so a refused
    /// request cannot reach a read as "all databases" (the type will not let a null through as a filter).
    ///
    /// <para>No <c>database_name</c> key at all is <see cref="DatabaseFilter.All"/>. Otherwise the values are
    /// ITERATED, one per key; a value is never split on a comma, so <c>A,B</c> is one database called
    /// <c>A,B</c>. Blank values (empty or whitespace-only) are dropped, a kept value is never trimmed or
    /// case-folded, and repeats are kept once. The refusals, each the existing <c>invalid</c> envelope for the
    /// <c>database_name</c> parameter:</para>
    /// <list type="bullet">
    /// <item>keys were sent but none survived the blank rule. A caller who asked for a database and got
    /// "all databases" would read an unfiltered page as a filtered one, so the request is refused and never
    /// widened;</item>
    /// <item>more than <see cref="MaxDatabaseNames"/> distinct names;</item>
    /// <item>a name over <see cref="MaxDatabaseNameLength"/> characters;</item>
    /// <item>distinct names whose encoded query part is over <see cref="MaxDatabaseQueryBytes"/> bytes.</item>
    /// </list>
    /// Both caps count the DISTINCT names, so a request that repeats one name a hundred times is one name.
    /// </summary>
    internal static DatabaseFilter? DatabaseNames(HttpContext context) =>
        TryBindDatabaseNames(context.Request.Query[DatabaseNameKey], out var databases, out _) ? databases : null;

    /// <summary>
    /// The refusal for a request <see cref="DatabaseNames"/> answered <c>null</c> for, as the dispatch entry's own
    /// result: the <c>invalid</c> envelope (a 400 with the envelope as the body), the same shape
    /// <see cref="MissingParam"/> and <see cref="UnparseableParam"/> hand back, and returned BEFORE the store is
    /// touched. Calling it for a request that is fine is a bug, so it throws.
    /// </summary>
    internal static Task<string> DatabaseNamesRefusal(HttpContext context) =>
        TryBindDatabaseNames(context.Request.Query[DatabaseNameKey], out _, out var refusal)
            ? throw new InvalidOperationException("The request's database_name values are acceptable: there is nothing to refuse.")
            : Task.FromResult(refusal!);

    /// <summary>
    /// PURE <see cref="DatabaseNames"/>: <paramref name="sent"/> is every value of the <c>database_name</c> key,
    /// one element per key the request carried (empty when it carried none). True with the filter when the
    /// request is acceptable; false with the refusal envelope when it is not.
    /// </summary>
    internal static bool TryBindDatabaseNames(IReadOnlyList<string?> sent, out DatabaseFilter databases, out string? refusal)
    {
        databases = default;
        refusal = null;
        if (sent.Count == 0)
        {
            return true;
        }

        // The blank rule and the de-duplication are DatabaseFilter's one copy; the caps below count what is left.
        var chosen = DatabaseFilter.Of(sent);
        if (chosen.IsAll)
        {
            refusal = McpHelpers.Refusal(DatabaseNameKey,
                "database_name was sent, but every value was empty or only spaces. Name at least one database, or leave database_name out to cover them all.");
            return false;
        }

        var names = chosen.Names;
        if (names.Count > MaxDatabaseNames)
        {
            refusal = McpHelpers.Refusal(DatabaseNameKey,
                $"database_name names {names.Count} different databases; at most {MaxDatabaseNames} are accepted. Name fewer, or leave database_name out to cover them all.");
            return false;
        }

        foreach (var name in names)
        {
            /* PostgreSQL refuses a text value holding 0x00 (SQLSTATE 22021) and no stored database name can hold one, so the
               name is refused here with the same envelope, rather than costing the read a 500. The sentence echoes no name. */
            if (name.Contains('\0'))
            {
                refusal = McpHelpers.Refusal(DatabaseNameKey,
                    "A database_name value holds a character a database name cannot hold.");
                return false;
            }

            if (name.Length > MaxDatabaseNameLength)
            {
                refusal = McpHelpers.Refusal(DatabaseNameKey,
                    $"A database_name value is {name.Length} characters long; a database name is at most {MaxDatabaseNameLength} characters.");
                return false;
            }
        }

        var bytes = 0;
        foreach (var name in names)
        {
            bytes += DatabaseKeyBytes + EncodedQueryLength(name);
        }

        if (bytes > MaxDatabaseQueryBytes)
        {
            refusal = McpHelpers.Refusal(DatabaseNameKey,
                $"The database_name values come to {bytes} encoded bytes; at most {MaxDatabaseQueryBytes} are accepted. Name fewer databases, or leave database_name out to cover them all.");
            return false;
        }

        databases = chosen;
        return true;
    }

    /// <summary>
    /// PURE: the length <c>encodeURIComponent</c> gives <paramref name="value"/>, which is what the page's
    /// <c>buildQuery</c> puts on the wire for each name: the UTF-8 bytes, one character for each of
    /// <c>A-Z a-z 0-9 - _ . ! ~ * ' ( )</c> and three (<c>%XX</c>) for every other byte.
    /// </summary>
    internal static int EncodedQueryLength(string value)
    {
        var length = 0;
        foreach (var b in System.Text.Encoding.UTF8.GetBytes(value))
        {
            length += IsLeftAloneByEncodeUriComponent(b) ? 1 : 3;
        }

        return length;
    }

    private static bool IsLeftAloneByEncodeUriComponent(byte b) =>
        b is (>= (byte)'A' and <= (byte)'Z') or (>= (byte)'a' and <= (byte)'z') or (>= (byte)'0' and <= (byte)'9')
            or (byte)'-' or (byte)'_' or (byte)'.' or (byte)'!' or (byte)'~' or (byte)'*' or (byte)'\'' or (byte)'(' or (byte)')';

    /// <summary>A required text parameter — true with the value when present, false when absent/empty.</summary>
    private static bool RequireText(HttpContext context, string key, out string value)
    {
        value = First(context, key) ?? "";
        return value.Length > 0;
    }

    /// <summary>The dispatch layer's own refusal for a required text parameter the caller did not send — the
    /// <c>invalid</c> envelope (<see cref="McpHelpers.Refusal"/>, #3739) rather than the bare sentence it was, so
    /// a missing <c>wait_type</c> answers with the same shape and the same 400 a tool's own refusal does.</summary>
    private const string PlanTruncatedMarker = "... (truncated)";

    private static async Task<string> WrapPlanXmlAsync(Task<string> read, string queryHash, string? databaseName)
        => WrapPlanXml(await read, queryHash, databaseName);

    private static async Task<string> WrapPlanXmlAsync(Task<string> read, IReadOnlyDictionary<string, object?> identity)
        => WrapPlanXml(await read, identity);

    /// <summary>The identity a plan answer echoes back: the key the caller read by, in the order given.</summary>
    private static Dictionary<string, object?> PlanIdentity(params (string Key, object? Value)[] pairs)
    {
        var identity = new Dictionary<string, object?>(pairs.Length, StringComparer.Ordinal);
        foreach (var (key, value) in pairs) identity[key] = value;
        return identity;
    }

    /// <summary>The web answer for <c>get_plan_xml</c>. The tool returns the stored showplan XML as bare text, which
    /// the read route's classifier would file as a 400, so a plan is wrapped as JSON here (the MCP tool's own output
    /// is unchanged). A status or error envelope, which starts with <c>{</c>, passes through untouched. A plan cut
    /// by <see cref="McpHelpers.Truncate"/> ends with its marker (a whole plan ends with a closing tag); the marker
    /// is removed and <c>truncated</c> says so.</summary>
    internal static string WrapPlanXml(string result, string queryHash, string? databaseName)
        => WrapPlanXml(result, new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["query_hash"] = queryHash,
            ["database_name"] = databaseName,
        });

    /// <summary>The same wrap for any plan read (#5228): <paramref name="identity"/> is echoed first, then
    /// <c>plan_xml</c> and <c>truncated</c>.</summary>
    internal static string WrapPlanXml(string result, IReadOnlyDictionary<string, object?> identity)
    {
        if (!result.AsSpan().TrimStart().StartsWith("<", StringComparison.Ordinal)) return result;

        var truncated = result.EndsWith(PlanTruncatedMarker, StringComparison.Ordinal);
        var xml = truncated ? result[..^PlanTruncatedMarker.Length] : result;
        var payload = new Dictionary<string, object?>(identity.Count + 2, StringComparer.Ordinal);
        foreach (var pair in identity) payload[pair.Key] = pair.Value;
        payload["plan_xml"] = xml;
        payload["truncated"] = truncated;
        return System.Text.Json.JsonSerializer.Serialize(payload);
    }

    private static Task<string> MissingParam(string key) => Task.FromResult(McpHelpers.Refusal(key, $"Missing required parameter '{key}'."));

    /// <summary>
    /// An OPTIONAL numeric parameter: true with null when the key is absent, true with the value when it
    /// parses, and FALSE when it is present and cannot be read as a number.
    ///
    /// <para>The false arm is the whole reason this is not <see cref="QueryDouble"/>. Every other optional
    /// knob on this dispatch falls back to its default on a value it cannot parse, so <c>?hours=abc</c>
    /// quietly means 24 — and for a FILTER that same fallback means the filter does not apply and the caller
    /// receives a complete-looking UNFILTERED page. That is exactly the silently-dropped-parameter failure
    /// the filter was added to remove (#3287), so an unreadable filter is refused rather than ignored.</para>
    /// </summary>
    private static bool OptionalDouble(HttpContext context, string key, out double? value) =>
        TryParseOptionalDouble(First(context, key), out value);

    /// <summary>PURE optional-number binding — what <see cref="OptionalDouble"/> is without an HttpContext,
    /// so the three outcomes are pinnable the way <see cref="ParseDouble"/> and <see cref="ClampRows"/>
    /// are.</summary>
    internal static bool TryParseOptionalDouble(string? raw, out double? value)
    {
        if (raw is null)
        {
            value = null;
            return true;
        }

        var parsed = double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var number);
        value = parsed ? number : null;
        return parsed;
    }

    /// <summary>
    /// An OPTIONAL integer parameter — <see cref="OptionalDouble"/>'s rule for an int: true with null when the key
    /// is absent, true with the value when it parses, FALSE when it is present and not an integer. For a knob
    /// whose absence means "let the read choose" (<c>bucket_minutes</c>, #3897), an unreadable value falling back
    /// to that choice would answer a different request than the one sent.
    /// </summary>
    private static bool OptionalInt(HttpContext context, string key, out int? value) =>
        TryParseOptionalInt(First(context, key), out value);

    /// <summary>An OPTIONAL 64-bit integer parameter (a Query Store <c>query_id</c> / <c>plan_id</c>), with
    /// <see cref="OptionalInt"/>'s three outcomes.</summary>
    private static bool OptionalLong(HttpContext context, string key, out long? value) =>
        TryParseOptionalLong(First(context, key), out value);

    /// <summary>PURE optional-long binding — <see cref="TryParseOptionalInt"/>'s 64-bit twin.</summary>
    internal static bool TryParseOptionalLong(string? raw, out long? value)
    {
        if (raw is null)
        {
            value = null;
            return true;
        }

        var parsed = long.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number);
        value = parsed ? number : null;
        return parsed;
    }

    /// <summary>PURE optional-integer binding — <see cref="TryParseOptionalDouble"/>'s twin.</summary>
    internal static bool TryParseOptionalInt(string? raw, out int? value)
    {
        if (raw is null)
        {
            value = null;
            return true;
        }

        var parsed = int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var number);
        value = parsed ? number : null;
        return parsed;
    }

    /// <summary>The dispatch layer's refusal for a filter value it cannot read as a number — the <c>invalid</c>
    /// envelope, for the reason <see cref="MissingParam"/> is.</summary>
    private static Task<string> UnparseableParam(string key, string? expected = null) =>
        Task.FromResult(McpHelpers.Refusal(key, $"Invalid value for parameter '{key}'. {expected ?? "Expected a number."}"));


    /// <summary>
    /// Every query-string knob the dispatch reads as a whole number through <see cref="QueryInt"/> / <see cref="Rows"/> /
    /// <see cref="Hours"/> (#5644 W22). <see cref="ParseInt"/> reads an unreadable one as the default, so a window or row
    /// count the caller got wrong was answered, with a 200, for a different one than the one asked for.
    /// </summary>
    internal static readonly string[] IntegerQueryKeys =
    [
        "hours", "hours_back", "days_back", "trend_hours_back", "baseline_hours_back", "limit", "top",
        "bucket_minutes", "min_dop", "worst_count", "statements", "min_sessions"
    ];

    /// <summary>
    /// The 400 envelope for the first <see cref="IntegerQueryKeys"/> knob that is present but not a whole number
    /// (<c>?hours=abc</c>, <c>?hours=1.5</c>), else null. An absent or empty knob still takes the tool's default, and a number out of
    /// range is left to the tool, which owns that refusal. Pure over the lookup, so a test pins it without a server.
    /// </summary>
    internal static string? NonIntegerParamRefusal(Func<string, string?> lookup)
    {
        foreach (var key in IntegerQueryKeys)
        {
            var raw = lookup(key);
            if (!string.IsNullOrEmpty(raw) && !int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out _))
            {
                return McpHelpers.Refusal(key, $"Invalid {key} value '{McpHelpers.Truncate(raw, 40)}'. Must be a whole number.");
            }
        }

        return null;
    }

    /// <summary>
    /// #5644 W22: the window refusal (<c>hints.parameter</c> <c>hours_back</c>) a web caller got for <c>?hours=</c> out of range, said in
    /// the parameter the caller sent. The tools name <c>hours_back</c> (the MCP parameter, kept as it is for MCP callers); the web
    /// accepts both spellings, and a request that sent only <c>hours</c> was told off for a parameter it never used. A request that
    /// sent <c>hours_back</c> (alone or beside <c>hours</c>), a refusal for any other parameter, and a result that is not a refusal
    /// envelope come back unchanged. Only the standalone word is renamed: <c>baseline_hours_back</c> stays.
    /// </summary>
    internal static string NameWindowParameterTheCallerSent(string result, Func<string, string?> lookup)
    {
        if (!McpHelpers.IsRefusalEnvelope(result) || string.IsNullOrEmpty(lookup("hours")) || !string.IsNullOrEmpty(lookup("hours_back")))
        {
            return result;
        }

        try
        {
            if (JsonNode.Parse(result) is not JsonObject envelope
                || envelope["hints"] is not JsonObject hints
                || hints["parameter"]?.GetValue<string>() != "hours_back")
            {
                return result;
            }

            hints["parameter"] = "hours";
            if (envelope["message"]?.GetValue<string>() is { } message)
            {
                envelope["message"] = Regex.Replace(message, @"(?<![A-Za-z0-9_])hours_back(?![A-Za-z0-9_])", "hours");
            }

            return envelope.ToJsonString();
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
        {
            return result;
        }
    }

    /// <summary>
    /// #5644: the refusal envelope for an out-of-range <c>/api/fleet</c> window (<paramref name="hours"/> outside 1..168) or a
    /// negative <paramref name="worstCount"/>, else null. Same envelope and sentences as the MCP tool's
    /// <see cref="McpHelpers.ValidateHoursBack(int)"/>, with the window parameter named as the caller sent it
    /// (<see cref="NameWindowParameterTheCallerSent"/>). Pure over the lookup, so a test pins it without a server.
    /// </summary>
    internal static string? FleetParamRefusal(int hours, int worstCount, Func<string, string?> lookup)
    {
        /* The note this makes is inert today (#5814): /api/fleet calls this before its ReadScope opens, so there is no scope
           to note on, and the route is never offered to the slow-read log. A route that starts offering it a row must note
           its window inside the scope. */
        var window = DarlingReadWindow.ValidateHoursBack(hours);
        if (window is not null)
        {
            return NameWindowParameterTheCallerSent(window, lookup);
        }

        return worstCount < 0
            ? McpHelpers.Refusal("worst_count", $"Invalid worst_count value '{worstCount}'. Must be zero or a positive integer.")
            : null;
    }

    private static string? NonIntegerParamRefusal(HttpContext context) =>
        NonIntegerParamRefusal(key => First(context, key));

    private static int QueryInt(HttpContext context, string key, string? aliasKey, int def) =>
        ParseInt(First(context, key) ?? (aliasKey is null ? null : First(context, aliasKey)), def);

    /// <summary>A knob whose omitted value is derived from another knob (#5633 item 12: compare_analysis's baseline follows
    /// hours_back), so a missing or unparseable value is null, not a fixed default.</summary>
    private static int? QueryNullableInt(HttpContext context, string key) =>
        int.TryParse(First(context, key), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : null;

    /// <summary>
    /// A row-count knob (<c>?limit=</c> / <c>?top=</c>), clamped to [1, <see cref="MaxRowLimit"/>] at the
    /// dispatch layer so an authenticated/loopback caller can't request an unbounded result set from a reader
    /// that binds the value straight into <c>LIMIT $N</c> (the /api/fleet window is refused out of range instead, #5644).
    /// </summary>
    private static int Rows(HttpContext context, string key, int def) =>
        ClampRows(QueryInt(context, key, null, def));

    /// <summary>The dispatch-layer ceiling on any caller-supplied row count — generous for real use, bounded against abuse.</summary>
    internal const int MaxRowLimit = 1000;

    /// <summary>PURE row-count clamp to [1, <see cref="MaxRowLimit"/>] — the abuse bound the row-knob binding applies.</summary>
    internal static int ClampRows(int requested) => Math.Clamp(requested, 1, MaxRowLimit);

    private static bool QueryBool(HttpContext context, string key, bool def) => ParseBool(First(context, key), def);

    private static double QueryDouble(HttpContext context, string key, double def) => ParseDouble(First(context, key), def);

    /// <summary>
    /// The first non-empty value for a query key, or null. This is the ONE binding rule for a single-value
    /// parameter: the other three web surfaces' query helpers (<c>DarlingFleetSweepEndpoints.Query</c>,
    /// <c>AlertNotebookEndpoint.Query</c>, <c>DarlingTriageEndpoint.Query</c>) call it rather than restate it.
    ///
    /// <para>A key sent twice (<c>?server=A&amp;server=B</c>) is its FIRST value. It used to be both values joined
    /// with a comma (<c>A,B</c>, which is what turning ASP.NET's <c>StringValues</c> into a string does), which
    /// no server, database or collector is called, so a repeated key silently matched nothing (#5245). A list is
    /// read by <see cref="DatabaseNames"/>, which iterates the values and never splits on a comma.</para>
    /// </summary>
    internal static string? First(HttpContext context, string key)
    {
        foreach (var value in context.Request.Query[key])
        {
            if (!string.IsNullOrEmpty(value))
            {
                return value;
            }
        }

        return null;
    }

    /// <summary>
    /// The ONE reader for a query key that must carry exactly one value (a confirm flag, the access token, the
    /// sign-in callback's <c>code</c>/<c>state</c>/<c>error</c>/<c>return</c>). Returns false when the key is
    /// sent more than once (<c>?code=a&amp;code=b</c>): RFC 6749 section 3.1 says such a parameter MUST NOT be
    /// included more than once, and reading it as <c>Query[key].ToString()</c> joins the copies into
    /// <c>a,b</c>, which is a value nobody sent (#5245). An absent key is true with an empty
    /// <paramref name="value"/>, exactly what <c>Query[key].ToString()</c> gave, so a request that sends each
    /// key once behaves as it always did.
    /// </summary>
    internal static bool TrySingleQueryValue(IQueryCollection query, string key, out string value)
    {
        var values = query[key];
        if (values.Count > 1)
        {
            value = string.Empty;
            return false;
        }

        value = values.ToString();
        return true;
    }

    /// <summary>The <c>confirm</c> flag of a delete: absent is false, <c>true</c> is true, anything else (including
    /// a key sent twice) is a refusal sentence for the 400 (#5245). Pure, so the rule pins without a host.</summary>
    internal static (bool Confirm, string? Refusal) ParseConfirm(IQueryCollection query)
    {
        if (!TrySingleQueryValue(query, "confirm", out var confirmText))
        {
            return (false, "confirm must be given once, as true, or omitted.");
        }

        if (confirmText.Length > 0 && !string.Equals(confirmText, "true", StringComparison.Ordinal))
        {
            return (false, "confirm must be true, or omitted.");
        }

        return (confirmText.Length > 0, null);
    }

    /* Pure parse helpers (invariant culture) — the binding logic the tests pin without an HttpContext. */

    internal static int ParseInt(string? raw, int def) =>
        int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : def;

    internal static bool ParseBool(string? raw, bool def) =>
        bool.TryParse(raw, out var value) ? value : def;

    internal static double ParseDouble(string? raw, double def) =>
        double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) ? value : def;
}
