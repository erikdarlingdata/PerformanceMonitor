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

/// <summary>
/// The web dashboard's HTTP API surface (#1562). <see cref="DarlingWebHostService"/> owns the host, the
/// pipeline order, and the auth gate; this class owns the ROUTES. The two are split so the host (Builder 1's
/// foundation) and the endpoints (Builder 2) evolve independently against the stable <see cref="MapAll"/> seam.
///
/// <para><b>The read surface.</b> <c>GET /api/read/{tool_name}</c> is 1:1 with the READ-ONLY MCP tool catalog:
/// each endpoint calls the SAME public static tool method the MCP server exposes (the <c>[McpServerTool]</c>
/// attributes are inert — the tool bodies are plain statics), so there is ZERO SQL / projection drift between the
/// two surfaces. Query-string values bind to the tool's parameters (<c>server</c>/<c>server_name</c>,
/// <c>hours</c>/<c>hours_back</c>, <c>as_of</c>, <c>top</c>, <c>limit</c>, ...); missing optional parameters fall back to the
/// method's own defaults. The excluded tools are the ones that are not read-only-over-the-store:
/// <c>analyze_server</c> (a live monitored-server touch), <c>mute_analysis_finding</c> (a write), and the
/// <c>analyze_*_plan</c> compute family (phase 2). A reflection test pins the endpoint set == the tool catalog
/// minus exactly those exclusions, so a future tool cannot be silently missed.</para>
///
/// <para><b>Response mapping.</b> The tools always return a string: a serialized JSON object/array for data (and
/// for the <c>{"status", ...}</c> miss envelope), the <c>{"status":"error", ...}</c> envelope
/// <c>McpHelpers.FormatError</c> builds when the body's try/catch caught an exception (#3653 Q11 — before that
/// ruling the caught exception was a bare <c>"Error during ..."</c> string), the <c>{"status":"invalid", ...}</c>
/// envelope <c>McpHelpers.Refusal</c> builds when the tool refuses the request as given (#3739 — before that
/// ruling a refusal was the validator's bare sentence), or, rarely now, a bare string. The error envelope maps
/// to 500; the refusal envelope maps to 400 and is passed through AS the body (<c>application/json</c>), the
/// shape the mute-rule write routes have always given <c>invalid</c>; any other leading <c>{</c> or <c>[</c>
/// passes through verbatim as <c>application/json</c> (200); any bare (non-JSON) string maps to 400. The two
/// error arms answer <c>{"error": "..."}</c> with the SENTENCE — the envelope's <c>message</c>, not the
/// envelope — so the web surface keeps the one error body it has always had and a web client that reads
/// <c>.error</c> is not handed JSON inside a string. The mapping is where the PostgreSQL tools' failures, which
/// were already the envelope and therefore passed through as 200, became the 500 they always were, and where
/// nine PostgreSQL refusals that borrowed the <c>error</c> word — and so answered 500 for a bad <c>limit</c>
/// between #3719 and #3739 — became the 400 they always deserved.</para>
///
/// <para><b>The pre-banded fleet.</b> <c>GET /api/fleet</c> and the <c>get_fleet_overview</c> MCP tool both read
/// through <see cref="DarlingFleetReader"/> — the enriched per-server cards and the cross-server rollup, banded
/// once by the shared <c>ServerHealthClassifier</c>. <c>GET /api/ag</c> and <c>get_ag_health</c> pair the same way
/// over <see cref="DarlingAgReader"/> for the Availability Group topology (#991).</para>
/// </summary>
public static partial class DarlingWebEndpoints
{
    /// <summary>#4442, test-only: one extra <c>/api/read/*</c> dispatch entry a test can register so a
    /// real <see cref="PostgresException"/> with SqlState 57014 travels through the SAME dispatch loop
    /// every other route uses, rather than a hand-called <c>Record</c> standing in for the wiring. <c>internal</c>
    /// and set ONLY from <c>Darling.Tests</c> (grep proves no production caller ever assigns it); null in every
    /// production run, so <see cref="BuildReadDispatch"/> adds nothing extra unless a test opted in.
    /// #4782: held per async flow, not process-wide. Only the async flow that set the entry (and what that flow
    /// starts or awaits) sees it, so a test class running at the same time in another flow builds its own
    /// dispatch without the extra route. A plain static was seen by all of them, and a test that compares the
    /// dispatch keys with the Custom Views catalog failed on the route it did not expect.</summary>
    private static readonly AsyncLocal<(string Name, ReadToolHandler Handler)?> s_testOnlyExtraDispatchEntry = new();

    /// <summary>The test-only extra dispatch entry (#4442, #4782). Reads and writes the current async flow's
    /// value only; see the note on the backing field.</summary>
    internal static (string Name, ReadToolHandler Handler)? TestOnlyExtraDispatchEntry
    {
        get => s_testOnlyExtraDispatchEntry.Value;
        set => s_testOnlyExtraDispatchEntry.Value = value;
    }

    /// <summary>Reads served on <c>/api/read/*</c> that are NOT an MCP tool (#5241): the web page's own keyed reads.
    /// <c>get_alert_details</c> returns one alert's advice when its row is opened, so the Alert History poll does not
    /// carry every row's advice. They are the only dispatch keys with no <c>[McpServerTool]</c> behind them; the
    /// parity test adds exactly this set to the tool catalog.</summary>
    internal static readonly IReadOnlySet<string> WebOnlyReadNames = new HashSet<string>(StringComparer.Ordinal)
    {
        "get_alert_details",
        "get_overview_baselines",
        "get_block_chain",
    };

    /// <summary>The tool names deliberately absent from the <c>/api/read/*</c> 1:1 read surface. <c>analyze_server</c>
    /// makes a live monitored-server connection; <c>mute_analysis_finding</c> and <c>unmute_analysis_finding</c> (#6383) write; the <c>analyze_*_plan</c> family
    /// is the compute-heavy plan-analysis phase-2 work; the Custom Views tools (#1599) are served by their OWN
    /// richer web endpoints (<c>/api/views</c> CRUD + <c>/api/compose/run</c> + the <c>/api/catalog</c> compose
    /// vocabulary that <c>describe_custom_view_catalog</c> mirrors), not a <c>/api/read/{tool}</c> query-string mirror;
    /// the four mute-rule write verbs (<c>create_mute_rule</c> / <c>update_mute_rule</c> / <c>delete_mute_rule</c> /
    /// <c>set_mute_rule_enabled</c>) are the Custom Views disposition since #3450 — served by their OWN dedicated
    /// endpoints (<c>/api/mute-rules</c>, see <see cref="MapMuteRules"/>), never a query-string mirror of a write;
    /// <c>update_alert_settings</c> WRITEs the alert config with no web surface at all, as do the two notification-route
    /// writes (<c>set_notification_route_enabled</c> / <c>delete_notification_route</c>, #3598 — routes are authored in
    /// the Viewer's Settings window, and the web host's <c>viewer</c> role holds no write on the routes table), and the server-onboarding tools
    /// (<c>add_servers</c> / <c>remove_server</c>) WRITE the
    /// monitored-server registry, so — like <c>mute_analysis_finding</c> — they have no read endpoint. The
    /// custom-alert-rule tools (#3285) are the same disposition as the Custom Views tools: <c>create</c> /
    /// <c>update</c> / <c>delete</c> write <c>config.custom_alert_rules</c> and <c>get</c> / <c>list</c> /
    /// <c>validate_custom_alert_rule</c> read/validate against the compose catalog, none a <c>/api/read/{tool}</c>
    /// mirror. <c>get_tool_guide</c> (#3898) reads no data at all: it serves the MCP tools' own reading guides out of
    /// the registration-time catalog only the MCP host builds, so a web read of it has nothing to mirror.
    /// <c>wait_for_investigation_requests</c> (#5822) is an agent's long poll: it holds the call up to 25 seconds waiting
    /// for a request to open, which is no page read, and the web page reads the requests through
    /// <c>list_investigation_requests</c>. <c>backtest_custom_alert_rule</c> (#5911) replays a rule over up to 7 days of
    /// history in up to 2,880 queries and 60 seconds, which is no page read: it is served over MCP only.</summary>
    public static readonly IReadOnlySet<string> ExcludedToolNames = new HashSet<string>(StringComparer.Ordinal)
    {
        "analyze_server",
        "mute_analysis_finding",
        "unmute_analysis_finding",
        "analyze_query_plan",
        "analyze_procedure_plan",
        "analyze_query_store_plan",
        "analyze_plan_xml",
        "describe_custom_view_catalog",
        "list_custom_views",
        "get_custom_view",
        "validate_custom_view",
        "create_custom_view",
        "update_custom_view",
        "delete_custom_view",
        "run_custom_view_panel",
        "update_alert_settings",
        /* #5916: a threshold override is set over MCP; the web grid has its own admin-login route, not a generic read. */
        "set_alert_threshold_override",
        /* #6006: a signature merge is an MCP action through the agent gate; the web has no route for it. */
        "merge_deadlock_signatures",
        /* #5632 step 7: the plan force tools change a monitored SQL Server through the server action gate; the web has no route for them. */
        "force_query_store_plan",
        "unforce_query_store_plan",
        /* #5632 step 10: an eviction changes a monitored SQL Server through the server action gate; the web has no route for it. */
        "evict_cached_plan",
        /* #5632 step 12: a kill ends a session on a monitored SQL Server through the server action gate; the web has no route for it. */
        "kill_session",
        /* #6569: a module recompile runs sp_recompile on a monitored SQL Server through the server action gate; the web has no route for it. */
        "recompile_module",
        /* #6569: a plan guide create runs sys.sp_create_plan_guide on a monitored SQL Server through the server action gate; the web has no route for it (a person undoes it from Agent changes). */
        "create_plan_guide",
        /* #6569, slice 2b: a plan guide drop runs sys.sp_control_plan_guide on a monitored SQL Server through the server action gate; the web has no route for it (a person undoes it from Agent changes). */
        "drop_plan_guide",
        /* #5632 step 10: an agent's read of what compiled after its eviction, served over MCP by journal id; no page read mirrors it. */
        "get_compiles_after_evict",
        /* #6230 part B: the incident actions queue a command to the service over MCP; the web has its own routes for them, none a generic read.
           get_command_result reads a queue row by id and get_agent_changes is the MCP twin of GET /api/agent-changes. */
        "test_connection",
        "analyze_now",
        "purge_now",
        "undo_plan_change",
        /* #7041: the manual send of the Daily Health Report goes through the alert pipeline over MCP; the web has no route for it (its Preview
           is the read get_daily_health_report, which sends nothing). */
        "send_daily_health_report_now",
        /* #6401: the global agent switch and the undo of a non-plan agent change over MCP; the web has its own PUT /api/agent-changes/switch and
           PUT /api/agent-changes/{id}/undo, neither a generic read. */
        "set_agent_actions_enabled",
        "undo_agent_change",
        "get_command_result",
        "get_agent_changes",
        "set_notification_route_enabled",
        "set_notification_route_fallback",
        "create_notification_route",
        "update_notification_route",
        "generate_webhook_signing_key",
        "remove_webhook_signing_key",
        "update_notification_channels",
        "dismiss_alert_history",
        "restore_alert_history",
        "set_quiet_window",
        "clear_quiet_window",
        /* #6397: the Database State expectations are edited in the Viewer's override editor; the web has no route for them. */
        "get_database_state_expectations",
        "set_database_state_expected",
        /* #6400: the web has its own routes for these seven (PUT /api/investigation-notes, .../approve, .../reject,
           /api/investigation-requests/{id}/close, /api/investigation-practice-requests/{id}, /api/environment-profiles and
           /api/investigation-feedback/{id}); the MCP tools are not dispatched through /api/read. */
        "write_investigation_note",
        "approve_investigation_note",
        "reject_investigation_note",
        "close_investigation_request",
        "start_practice_request",
        "save_environment_profile",
        "record_investigation_feedback",
        /* #5632 Phase A: claim and submit write investigation state for an agent over MCP; the web serves the two
           investigation reads only (/api/read). */
        "claim_investigation_request",
        "submit_investigation_report",
        "delete_notification_route",
        "create_mute_rule",
        "update_mute_rule",
        "delete_mute_rule",
        "purge_expired_mute_rules",
        "set_mute_rule_enabled",
        "add_servers",
        "edit_server",
        "remove_server",
        "create_custom_alert_rule",
        "get_custom_alert_rule",
        "list_custom_alert_rules",
        "update_custom_alert_rule",
        "delete_custom_alert_rule",
        "validate_custom_alert_rule",
        "test_custom_alert_rule",
        "backtest_custom_alert_rule",
        "list_custom_alert_templates",
        "create_server_tag",
        "update_server_tag",
        "delete_server_tag",
        "assign_server_tag",
        "unassign_server_tag",
        "create_query_tag",
        "update_query_tag",
        "delete_query_tag",
        "assign_query_tag",
        "unassign_query_tag",
        "create_query_tag_rule",
        "update_query_tag_rule",
        "delete_query_tag_rule",
        /* #5982: the rule reads are not on /api/read. test_query_tag_rule runs the shared resolver over up to 10,000 stored sightings,
           which is no page read. The rules page (#5986) has its own routes, MapQueryTags: GET /api/query-tag-rules (the list) and
           GET /api/query-tag-rules/test (the dry run, admitted and recorded under this name). */
        "list_query_tag_rules",
        "test_query_tag_rule",
        "run_collectors_now",
        /* #6230: the collection-state writes (pause, resume, disable and enable a server, a server's excluded databases, reload) are MCP-only:
           the Viewer and the command plane already carry them, and the web host's viewer role holds no write on config_service or
           config_monitored_servers. Their read, get_collection_state, is MCP-only too: the Viewer's Manage Servers and Settings windows show the same state. */
        "get_collection_state",
        "pause_collection",
        "resume_collection",
        "disable_server",
        "enable_server",
        "set_server_excluded_databases",
        "reload_config",
        /* #6227: the five collector schedule writes are MCP-only (the Viewer's schedule editor is the page for a person), and the
           get_collector_schedules read is for an agent checking its own change before and after; no page read mirrors it. */
        "disable_collector",
        "enable_collector",
        "set_collector_frequency",
        "set_collector_run_at",
        "set_collector_retention",
        /* #6396: the database scope and the reset to defaults are MCP-only too, for the same reason. */
        "set_collector_databases",
        "reset_collector_schedules",
        "get_collector_schedules",
        /* #6398: the known-pattern writes are MCP-only; the web editor has its own routes (POST/PUT/DELETE /api/known-patterns, see DarlingKnownPatternRoutes). */
        "create_known_pattern",
        "update_known_pattern",
        "delete_known_pattern",
        /* #6393: update_service_flags is MCP-only; the Viewer's Settings window is the page for a person. */
        "update_service_flags",
        /* #6269: the server action switches have their own web routes (GET /api/server-actions and PUT /api/servers/{id}/actions/{action}, admin role); the MCP write is under mcp.allowServerAdmin. */
        "set_server_action",
        "get_server_actions",
        /* #7107: the people directory read. The Admin People tab has its own web routes (GET /api/people and PUT /api/people/{command}). */
        "get_people",
        /* #6402: the read of the fields edit_server changes; the web edit form has its own read (GET /api/admin/servers/{id}), so no /api/read page mirrors it. */
        "get_server_settings",
        "get_tool_guide",
        "wait_for_investigation_requests",
        /* #6033: an agent's read of one claimed request's frozen evidence, served by claim; no page read mirrors it. */
        "get_investigation_evidence",
    };

    /// <summary>
    /// #4203: every read-only tool NOT yet converted to observe cancellation all the way to Npgsql — the
    /// ratchet <c>WebReadCancellationPinTests</c> holds in both directions.
    /// A name here may only be
    /// REMOVED, on the PR that finishes threading its <see cref="ReadToolHandler"/> entry AND its
    /// <c>[McpServerTool]</c> method's own <c>CancellationToken</c> parameter down to every store call — never
    /// added, because every tool on the <c>/api/read/*</c> surface is in scope for the same fix and none is
    /// meant to stay uncancellable. The list is now empty: every tool on the <c>/api/read/*</c> surface has been
    /// converted, and #4203 is closed.
    /// </summary>
internal static readonly IReadOnlySet<string> CancellationAllowlist = new HashSet<string>(StringComparer.Ordinal)
    {
    };

    /// <summary>The window (hours) the fleet card blocking / deadlock counts default to — the WPF Overview's window.</summary>
    private const int DefaultFleetHours = 1;

    /// <summary>
    /// What <c>GET /api/ping</c> answers, as an object rather than a literal (#2953). <c>HttpStatus</c> is not
    /// serialized — it is the same verdict expressed in the one field a load balancer or uptime check reads
    /// without parsing a body.
    /// </summary>
    /// <param name="HttpStatus">200 while nothing is known to be wrong with collection, 503 once something is.</param>
    /// <param name="Status">The machine-readable state: <c>ok</c>, <c>starting</c>, <c>degraded</c> or <c>stopped</c>.</param>
    /// <param name="Collecting">The same answer as one bit, for a check that wants no string comparison at all.</param>
    /// <param name="Step">Which startup step failed (<c>configuration</c>, <c>managed_store</c>, <c>store</c>); omitted otherwise.</param>
    /// <param name="Attempt">The retry in flight and the cap it counts against; both omitted outside <c>degraded</c>.</param>
    /// <param name="Attempts">The attempt cap the retry budget allows; omitted once the fast budget is spent
    /// and the service is retrying every 60 seconds with no cap (#4508), rather than showing a spent cap as if
    /// it still bounded anything.</param>
    /// <param name="Detail">The failure message, as the service's own log line reports it; omitted when there is none.</param>
    /// <param name="SinceUtc">When this state began — collection start, or when the failure was last observed.</param>
    /// <param name="Sustained">True once the step has spent its 120 s fast budget and retries with no cap (#4508); omitted otherwise.</param>
    /// <param name="RetryEverySeconds">The sustained retry interval, <c>StartupFailureTriage.SustainedRetryDelay</c>; omitted outside that state.</param>
    internal sealed record PingReport(
        [property: JsonIgnore] int HttpStatus,
        [property: JsonPropertyName("status")] string Status,
        [property: JsonPropertyName("collecting")] bool Collecting,
        [property: JsonPropertyName("step")] string? Step,
        [property: JsonPropertyName("attempt")] int? Attempt,
        [property: JsonPropertyName("attempts")] int? Attempts,
        [property: JsonPropertyName("detail")] string? Detail,
        [property: JsonPropertyName("since")] DateTime? SinceUtc,
        [property: JsonPropertyName("sustained")] bool? Sustained,
        [property: JsonPropertyName("retryEverySeconds")] int? RetryEverySeconds);

    /// <summary>Omits the null members so each ping state carries only the fields that mean something in it —
    /// a <c>degraded</c> body has an attempt count and an <c>ok</c> body does not, rather than every body
    /// carrying nulls a check has to know to ignore.</summary>
    internal static readonly JsonSerializerOptions PingJsonOptions = new()
    {
        WriteIndented = false,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    /// <summary>
    /// Turns the collector's published verdict into the ping response (#2953). Pure over the snapshot so the
    /// whole four-state table is unit-pinned without a server or a store, exactly as
    /// <c>DarlingWebHostService.DecideWebAction</c> pins the supervisor's.
    ///
    /// <para><b>The four states, and how an external monitor tells them apart.</b> The status CODE separates
    /// "collection is not a known problem" from "collection is not running", because that is the only field a
    /// load balancer or an uptime check reads without parsing JSON; the <c>status</c> string then separates the
    /// four:</para>
    /// <list type="bullet">
    /// <item><description><b>200 <c>ok</c></b> — the collection loop started. Byte-compatible with what this
    /// route always answered for the healthy case, so an existing check that asserts <c>status: ok</c> keeps
    /// passing and only stops passing when something is genuinely wrong.</description></item>
    /// <item><description><b>200 <c>starting</c></b> — the worker has not reached a verdict yet. 200 rather than
    /// 503 deliberately: this is the ordinary state for the first seconds of every service start, and a check
    /// that alarmed here would alarm on every restart. It is bounded — the worker publishes one of the other
    /// three, and a terminal stand-down publishes <c>stopped</c> before it returns, so this cannot be where a
    /// dead collector comes to rest.</description></item>
    /// <item><description><b>503 <c>degraded</c></b> — a collection-blocking startup step failed transiently and
    /// is being retried. 503 because collection is not happening; self-clearing because the failure was
    /// classified retryable, so within <c>StartupFailureTriage.RetryBudget</c> this becomes <c>ok</c> or
    /// <c>stopped</c>.</description></item>
    /// <item><description><b>503 <c>stopped</c></b> — a collection-blocking startup step failed terminally.
    /// Collection does not start for the life of this process; <c>detail</c> carries what the critical log line
    /// says, and the fix is to correct that and restart. This is the state the whole issue is about: it used to
    /// answer <c>200 ok</c>.</description></item>
    /// </list>
    ///
    /// <para><b>Why 503 and not 200-with-a-body.</b> The reporting failure is that automation was told the
    /// service was fine — and automation reads the code. A body-only signal would leave every existing uptime
    /// check exactly as wrong as it is now. The cost is that a load balancer health-checking this route pulls
    /// the host from rotation during a store outage, which is the correct verdict for an API whose data all
    /// comes from that store; the dashboard's static surface and its other routes are untouched, so a human can
    /// still open the UI directly and read the reason out of this body.</para>
    /// </summary>
    internal static PingReport DescribePing(CollectorRuntimeState.Snapshot? snapshot)
    {
        if (snapshot is null)
        {
            return new PingReport(StatusCodes.Status200OK, "starting", false, null, null, null, null, null, null, null);
        }

        return snapshot.Phase switch
        {
            CollectorRuntimeState.CollectorPhase.Collecting =>
                new PingReport(StatusCodes.Status200OK, "ok", true, null, null, null, null, snapshot.AsOfUtc, null, null),

            CollectorRuntimeState.CollectorPhase.Retrying =>
                new PingReport(
                    StatusCodes.Status503ServiceUnavailable, "degraded", false, DescribeStartupStep(snapshot.Step),
                    snapshot.Attempt, snapshot.Sustained ? null : snapshot.Attempts, snapshot.Detail, snapshot.AsOfUtc,
                    snapshot.Sustained ? true : null,
                    snapshot.Sustained ? (int)StartupFailureTriage.SustainedRetryDelay.TotalSeconds : null),

            CollectorRuntimeState.CollectorPhase.Stopped =>
                new PingReport(
                    StatusCodes.Status503ServiceUnavailable, "stopped", false, DescribeStartupStep(snapshot.Step),
                    null, null, snapshot.Detail, snapshot.AsOfUtc, null, null),

            /* Default-deny to the loudest answer: a phase this method does not know about is a phase whose
               health it cannot vouch for, and the whole point of the route is that it does not report healthy
               on a state it has not reasoned about. */
            _ => new PingReport(
                StatusCodes.Status503ServiceUnavailable, "stopped", false, DescribeStartupStep(snapshot.Step),
                null, null, snapshot.Detail, snapshot.AsOfUtc, null, null),
        };
    }

    /// <summary>The wire name for a startup step — snake_case like the store's own identifiers, and an explicit
    /// map rather than the enum name so renaming the enum cannot silently change a public response field.</summary>
    private static string? DescribeStartupStep(CollectorRuntimeState.StartupStep? step)
        => step switch
        {
            CollectorRuntimeState.StartupStep.Configuration => "configuration",
            CollectorRuntimeState.StartupStep.ManagedStore => "managed_store",
            CollectorRuntimeState.StartupStep.Store => "store",
            _ => null,
        };

    /// <summary>
    /// Maps the web dashboard's HTTP endpoints onto <paramref name="app"/>, reading from <paramref name="postgres"/>
    /// (the VIEWER-role store pool). Called ONCE from the web host's pipeline, after the auth middleware and before
    /// the static files. Every route lives under <c>/api/*</c> so the SPA's static surface never collides.
    ///
    /// <para><paramref name="logger"/> is the HOST SERVICE's logger — the seat every log-and-degrade route below
    /// writes through. Deliberately NOT <c>app.Logger</c>: the host clears the dashboard app's logging providers
    /// (both halves of that decision are stated at its ClearProviders site), so the app's own factory writes
    /// nowhere, and a degradation line logged through it would vanish.</para>
    ///
    /// <para><paramref name="baselineCache"/> is the process's shared baseline tier (#3941) — the one the worker's passes
    /// and the MCP host's analysis fill — so compare_analysis' banding here reads a series the store was already asked
    /// for this analysis hour from memory. Null keeps the analysis service's baselines private to it.</para>
    /// </summary>
    public static void MapAll(WebApplication app, NpgsqlDataSource postgres, CollectorRuntimeState collector, ILogger logger, BaselineCache? baselineCache = null, PostgresConfig? postgresConfig = null, ReadLatencyAccumulator? readLatency = null, PerformanceMonitor.PlanAnalysis.AnalyzerConfig? analyzerConfig = null, MonitoredServerRegistryState? registryState = null, SlowReadLog? slowReads = null, ReadAdmission? readAdmission = null, StoreOwnerSource? ownerSource = null, McpDataDetail dataDetail = McpDataDetail.MetadataOnly, IPseudonymKeySource? pseudonymKeys = null, InvestigationExportGate? investigationExportGate = null, IPasswordKeyRing? passwordKeys = null, WebAdminArm? adminArm = null, bool allowWindowsAuthOverUnverifiedTls = false, NpgsqlDataSource? composeReadPostgres = null)
    {
        /* #4442 scope 2, #4782: the read-latency seat THIS call's routes record into -- the accumulator and
           logger this call was given, held in a per-call object that the two record sites close over: the
           /api/read/* loop below, and the /api/compose/run route, which hands it to the shared
           RunComposedPanelAsync (the MCP run_custom_view_panel tool hands that runner its own host's seat).
           They used to be process-wide statics that every MapAll call overwrote, so a second server set up in
           the same process (six test classes call MapAll, and xUnit runs classes in parallel) took the samples
           of a server built before it. Production calls MapAll once, so nothing changes there. A caller with
           no accumulator (a test that maps the routes directly) records nothing. */
        var readLatencyRecorder = new ReadLatencyRecorder(readLatency, logger, slowReads, readAdmission);

        /* #6445: the Admin writes the web's store login cannot make answer a 403 whose advice depends on the store. A managed store refuses
           postgres.webConnectionString, so its sentences say the dashboard lists only and the Viewer app makes the change. */
        var managedStore = DarlingWebStoreWrites.IsManaged(postgresConfig);

        /* Liveness AND collection state (#2953). The one health surface that does not read the store, which
           makes it the only one that can answer when the store IS the problem — so it reports the collector's
           actual verdict instead of a hardcoded "ok". See DescribePing for the four states and their status
           codes, and CollectorRuntimeState for why that verdict is an in-process field rather than a row. */
        app.MapGet("/api/ping", () =>
        {
            var report = DescribePing(collector.Read());
            return Results.Json(report, PingJsonOptions, statusCode: report.HttpStatus);
        });

        /* The four analysis-READ tools take a DarlingAnalysisService; the web host does not register one, so
           build it once here from the same VIEWER-role pool (its read methods — fact collection, period compare,
           persisted-finding read — need only the store; the optional plan fetcher is for the excluded
           analyze/drill path, but the logger is also the analysis service's own logger (#4316)). Shared across
           requests, like the MCP host's singleton. */
        var analysis = new DarlingAnalysisService(postgres, logger: logger, baselineCache: baselineCache, analyzerConfig: analyzerConfig)
        {
            SeparatelyMonitoredResolver = registryState is null
                ? null
                : (serverId, ct) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(serverId, registryState.Read(), postgres, ct)
        };

        /* The pre-banded fleet roll-up (also surfaced as the get_fleet_overview MCP tool). */
        app.MapGet("/api/fleet", async (HttpContext context) =>
        {
            /* #5644 W22: a present, non-integer window or count is refused like an out-of-range one. */
            if (NonIntegerParamRefusal(context) is { } fleetRefusal)
            {
                return ToHttpResult(fleetRefusal, DarlingWebFailureLog.RouteOf(context.Request), logger, 0, context.RequestAborted);
            }

            /* #5644 (review of W22): an out-of-range window or count is refused with the {"status":"invalid"} 400
               the read loop answers, never clamped to a different window and answered 200 (REVIEW-TRAPS: counts
               are refused, not clamped). The refusal names the spelling the caller sent (hours or hours_back).
               The in-product callers send neither (apiGetFleet asks for the bare /api/fleet, the one-hour default). */
            var hours = QueryInt(context, "hours_back", "hours", DefaultFleetHours);
            var worstCount = QueryInt(context, "worst_count", null, DarlingFleetReader.DefaultWorstCount);
            if (FleetParamRefusal(hours, worstCount, key => First(context, key)) is { } fleetRangeRefusal)
            {
                return ToHttpResult(fleetRangeRefusal, DarlingWebFailureLog.RouteOf(context.Request), logger, 0, context.RequestAborted);
            }

            /* #5506 part 3: admitted, and recorded, under its MCP twin's name and window band, so the long-read table learns
               this route from its own samples. */
            using var readScope = ReadScope.Open(logger);
            var fleetBand = ReadWindowBand.Of(hours);
            using var ticket = await AdmitWebRouteAsync(readAdmission, "get_fleet_overview", fleetBand, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, "get_fleet_overview", fleetBand, context, readScope.Scope);

            var now = DateTime.UtcNow;
            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-hours), now, now, worstCount,
                separatelyMonitored: registryState is null
                    ? null
                    : (serverId, ct) => DarlingWorker.AnalysisSeparatelyMonitoredDatabasesAsync(serverId, registryState.Read(), postgres, ct),
                logger: logger,
                cancellationToken: context.RequestAborted);
            run.Complete(ReadOutcome.Ok);
            return Results.Json(result, DarlingFleetReader.JsonOptions);
        });

        /* The Availability Group topology (#991, also surfaced as the get_ag_health MCP tool). Fleet-wide, exactly
           like /api/fleet — the per-server view is reachable through the /api/read/get_ag_health mirror, which
           owns the name-resolution error path. Unlike that mirror this returns the DTO directly: an AG-less store
           answers with an empty groups array rather than the tool's {status:"empty"} envelope, so the page renders
           its own empty state and the nav gate can read the count off the same response. #5042: this page renders
           its own cards, not an MCP client's result buffer, so it passes a null byte budget and never loses a view
           to the 32 KB cap (the response is compressed on the wire, #4188); only get_ag_health keeps that cap. */
        app.MapGet("/api/ag", async (HttpContext context) =>
        {
            /* #5506 part 3 (round 1, M3): admitted, and recorded, under its MCP twin get_ag_health and its window band, like /api/fleet (D8). */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, AgHealthRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, AgHealthRoute, ReadWindowBand.None, context, readScope.Scope);
            var result = await DarlingAgReader.GetAgHealthAsync(
                postgres, null, DateTime.UtcNow, responseByteBudget: null, cancellationToken: context.RequestAborted);
            run.Complete(ReadOutcome.Ok);
            return Results.Json(result, DarlingAgReader.JsonOptions);
        });

        /* The nav-gate probe (#4189): refreshAgNav's only question is "is this nonzero", and answering it from
           /api/ag cost every page load a 140-266 KB, 1.5-2.6s topology read, repeated every 60s poll for as
           long as the answer stayed no. Same distinct-group definition, one aggregate query instead. */
        app.MapGet("/api/ag/count", async (HttpContext context) =>
        {
            var count = await DarlingAgReader.GetAvailabilityGroupCountAsync(
                postgres, null, context.RequestAborted);
            return Results.Json(new AvailabilityGroupCountResult { AvailabilityGroupCount = count }, DarlingAgReader.JsonOptions);
        });

        /* One GET per read-only tool, calling the tool method directly (no SQL/projection re-implementation). */
        foreach (var (name, handler) in BuildReadDispatch(logger, postgresConfig, registryState, readAdmission))
        {
            app.MapGet("/api/read/" + name, async (HttpContext context) =>
            {
                var stopwatch = Stopwatch.StartNew();
                using var readScope = ReadScope.Open(logger);
                readScope.Scope.CaptureStatements = true;

                /* #5506 part 3: admission, after the scope and the band. A refused read answers 429 and records busy; a
                   browser that leaves the queue records cancelled; a queued read records its wait as one queue_wait
                   sample. The recorded elapsed time starts after admission (D1). */
                /* #5644 W22: ?hours=abc / ?hours=1.5 (or any other integer knob) was read as the default and answered
                   200 for a different window than the one asked for, while ?hours=500 got a 400. Refused up front now,
                   before any tool runs, in the same envelope the range refusals use; and (#5506 part 3, round 1 L5) before
                   the cache and admission, so a malformed request on a heavy route is never queued or refused busy. */
                if (NonIntegerParamRefusal(context) is { } nonIntegerRefusal)
                {
                    return ToHttpResult(nonIntegerRefusal, DarlingWebFailureLog.RouteOf(context.Request), logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                var admissionBand = WebWindowBand(name, key => First(context, key));
                /* #5506 part 3: a heavy cacheable read is answered from the read cache, joins an identical read running now,
                   or runs once and is stored (the text before the statement sweep, which ToHttpResult runs on every answer).
                   The key is the query string as sent, before the read below adds its own as_of. A hit skips admission. */
                /* #6800: a whole-fleet read of get_resource_use_by_tag (no server) over a window of 24 hours or more is cached for its own TTL. */
                var fleetRead = ReadAdmission.IsFleetRead(name, Server(context), WebWindowHours(name, key => First(context, key)));
                if (readAdmission is { } admission && admission.IsCached(ReadSurface.Web, name, admissionBand, fleetRead))
                {
                    var cache = admission.Cache;
                    var ttl = ReadResultCache.TtlFor(name, admissionBand, fleetRead);
                    /* Round 2, L4: each value of a repeated name is passed apart (StringValues.ToString() joins them with a comma,
                       which ?database=x%2Cy also gives); the key escapes every name and value. */
                    var key = ReadCacheKey.ForWeb(name,
                        context.Request.Query.SelectMany(q => q.Value.Select(v => new KeyValuePair<string, string?>(q.Key, v))),
                        cache.Generation, cache, ttl, heldFromStore: ReadAdmission.FleetTtlApplies(name, fleetRead));
                    if (cache.TryGet(key, out var stored))
                    {
                        /* Round 2, L3: a hit runs no read, so it does not count as one in flight. */
                        readScope.Scope.Uncount();
                        admission.NoteCacheHit(ReadSurface.Web, name);
                        RecordWebReadLatency(readLatencyRecorder, name, ReadOutcome.CacheHit, stopwatch.ElapsedMilliseconds, context, readScope.Scope);
                        return ToHttpResult(stored, DarlingWebFailureLog.RouteOf(context.Request), logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                    }

                    /* D6: the read runs once, under the flight's token, which is cancelled only when every caller waiting for it
                       has left; each caller waits with its own. It runs on a detached copy of the first caller's request
                       (round 1, C3), never by swapping that request's RequestAborted: the first caller's own abort must
                       still end its own request (the #4276 backstop reads RequestAborted to tell a browser that left from a
                       failure), and the read must not touch a request that has ended. */
                    var callerAborted = context.RequestAborted;
                    var joinedAt = Stopwatch.StartNew();
                    var flight = cache.Join(key, ttl, token => RunWebReadAsync(DetachedReadContext(context, token)), read => read.Text);
                    if (flight.IsLead)
                    {
                        admission.NoteCacheMiss(ReadSurface.Web, name);
                        return (await flight.WaitAsync(callerAborted)).Answer;
                    }

                    /* Round 2, L3: a joined caller runs no read while it waits for the shared one. Round 2, M1: it records only its
                       own outcome (busy, cancelled, or cache_hit for the shared answer), never the read's, which the lead records
                       once: N joiners of one timed-out read are not N + 1 timeouts, and their shorter waits never reach the
                       percentiles or the hourly p90 table. */
                    readScope.Scope.Uncount();
                    try
                    {
                        var shared = await flight.WaitAsync(callerAborted);
                        var joinedOutcome = ReadResultCache.JoinedOutcome(shared.Outcome, callerAborted);
                        if (joinedOutcome == ReadOutcome.CacheHit)
                        {
                            admission.NoteCacheHit(ReadSurface.Web, name);
                        }

                        RecordWebReadLatency(readLatencyRecorder, name, joinedOutcome, joinedAt.ElapsedMilliseconds, context, readScope.Scope);
                        return shared.Text is { } text
                            ? ToHttpResult(text, DarlingWebFailureLog.RouteOf(context.Request), logger, joinedAt.ElapsedMilliseconds, callerAborted)
                            : shared.Answer;
                    }
                    catch (Exception ex)
                    {
                        var thrown = ReadResultCache.JoinedOutcome(
                            ReadOutcomeClassifier.Classify(ex, callerAborted) == ReadOutcome.Cancelled ? ReadOutcome.Cancelled : null, callerAborted);
                        if (thrown == ReadOutcome.CacheHit)
                        {
                            admission.NoteCacheHit(ReadSurface.Web, name);
                        }

                        RecordWebReadLatency(readLatencyRecorder, name, thrown, joinedAt.ElapsedMilliseconds, context, readScope.Scope);
                        throw;
                    }
                }

                return (await RunWebReadAsync(context)).Answer;

                /* The read itself, on the request it is given: the caller's own, or a detached copy for a shared read. */
                async Task<WebReadAnswer> RunWebReadAsync(HttpContext context)
                {
                    string result;
                    using var ticket = readAdmission is null || !ReadAdmission.IsAdmitted(name)
                        ? ReadAdmissionTicket.Passed
                        : await ReadScope.AdmitAsync(readAdmission, ReadSurface.Web, name, admissionBand, context.RequestAborted);
                    if (ticket.Refusal is { } refusal)
                    {
                        RecordWebReadLatency(readLatencyRecorder, name, ReadOutcome.Busy, ticket.QueueMs ?? 0, context, readScope.Scope);
                        return new WebReadAnswer(BusyResult(refusal), null, ReadOutcome.Busy);
                    }

                    if (ticket.CallerLeft)
                    {
                        RecordWebReadLatency(readLatencyRecorder, name, ReadOutcome.Cancelled, ticket.QueueMs ?? 0, context, readScope.Scope);
                        context.RequestAborted.ThrowIfCancellationRequested();
                    }

                    if (ticket.QueueMs is { } queueMs)
                    {
                        RecordWebReadLatency(readLatencyRecorder, name, ReadOutcome.QueueWait, queueMs, context, readScope.Scope);
                    }

                    stopwatch.Restart();

                    /* #6795: a read the browser abandoned is still a read that held the store. It is recorded as one cancelled
                       read_latency sample, the way the MCP filter's cancelled arm does, so get_read_latency and get_slow_reads
                       agree about it. Over the slow-read threshold it is also offered, with its route, the time it ran and the
                       statements it had run, as cancelled; under the threshold the log declines it. Both happen here, before the
                       rethrow the #4276 backstop needs, because that rethrow is the end of this read. */
                    void RecordAndOfferCancelledWebRead()
                    {
                        RecordWebReadLatency(readLatencyRecorder, name, ReadOutcome.Cancelled, stopwatch.ElapsedMilliseconds, context, readScope.Scope);
                        OfferWebSlowRead(readLatencyRecorder, readScope.Scope, name, ReadOutcome.Cancelled, stopwatch.ElapsedMilliseconds, context, null);
                    }

                    try
                    {
                        /* A newest-first capped read is judged against the window it read: with no anchor sent, the end is
                           taken once here and handed to both the read and the note check. A request with its own anchor
                           is left as sent. */
                        var askedAsOf = AsOf(context);
                        if (askedAsOf is null && WebDataStartNote.CappedByRead.ContainsKey(name))
                        {
                            askedAsOf = WebDataStartNote.FormatWindowEnd(DateTime.UtcNow);
                            context.Request.QueryString = context.Request.QueryString.Add("as_of", askedAsOf);
                        }

                        result = await handler(context, postgres, analysis);

                        /* #5644 W22: the tools' own window parameter is hours_back, but a web caller sends hours; a refusal for an
                           out-of-range one (?hours=-5, ?hours=500) is worded in the caller's own parameter. */
                        result = NameWindowParameterTheCallerSent(result, key => First(context, key));

                        /* #4966: a grid over a window says where its table's data starts when that is after the window's
                           start. Added to the web mirror only, never to the tool, so the MCP payloads are unchanged. A
                           failed probe returns the tool's answer as it was. */
                        var askedHours = QueryInt(context, "hours", "hours_back", 0);
                        result = await WebDataStartNote.AddAsync(
                            postgres, name, Server(context), askedHours > 0 ? askedHours : null, askedAsOf, result, logger, context.RequestAborted);
                    }
                    catch (OperationCanceledException) when (context.RequestAborted.IsCancellationRequested)
                    {
                        /* #4276: the browser left. Not a failure — no log line, and rethrown so the #4276
                           top-of-pipeline backstop (which already owns this exact classification) sees the same
                           exception rather than this catch turning it into a written body for a caller who is gone. */
                        RecordAndOfferCancelledWebRead();
                        throw;
                    }
                    catch (Exception ex)
                    {
                        /* #5678: the browser left and the store's connect timed out before it could raise a cancellation
                           (Npgsql can do that for an already-cancelled token): still the #4276 not-a-failure case. */
                        if (context.RequestAborted.IsCancellationRequested)
                        {
                            RecordAndOfferCancelledWebRead();
                        }

                        context.RequestAborted.ThrowIfCancellationRequested();

                        /* #4276/#4283: a binding-layer throw (the tools normally swallow their own into
                           McpHelpers.FormatError's envelope and return normally, so this is only a backstop).
                           Reported and answered directly from the real Exception here — ServerErrorResult's
                           sentence-based classifier is for the FAR more common case below, where the exception is
                           already gone by the time a tool's own catch handed back its envelope. Answering here
                           too, rather than falling through to ToHttpResult, keeps the ONE log line #4276 added:
                           routing this through FormatError first would make ToHttpResult's classifier re-derive
                           from text what this catch already knows structurally, and log it a second time. */
                        DarlingWebFailureLog.Report(logger, DarlingWebFailureLog.RouteOf(context.Request), stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                        var thrownOutcome = ReadScope.Resolve(ReadOutcomeClassifier.Classify(ex, context.RequestAborted), readScope.Fallback);
                        RecordWebReadLatency(readLatencyRecorder, name, thrownOutcome, stopwatch.ElapsedMilliseconds, context, readScope.Scope);
                        OfferWebSlowRead(readLatencyRecorder, readScope.Scope, name, thrownOutcome, stopwatch.ElapsedMilliseconds, context, SlowReadLog.ErrorClassOf(ex));
                        return new WebReadAnswer(Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex)), null, thrownOutcome);
                    }

                    /* #4442 scope 2: recorded on BOTH arms of this loop -- here for a tool result that made it
                       back as a string (success, or a tool's own caught-and-formatted failure), above for a
                       binding-layer throw the try/catch above answers directly. ClassifyToolResponse's
                       ServerError arm is the only shape that can carry a 57014 sentence (a tool that caught its
                       own statement_timeout and formatted it); everything else that reaches here is Ok. */
                    var webOutcome = ClassifyToolResponse(result) == ToolResponseKind.ServerError
                        ? ReadOutcomeClassifier.ClassifySentence(McpHelpers.ErrorMessageOf(result), context.RequestAborted)
                        : ReadOutcome.Ok;
                    var resolvedOutcome = ReadScope.Resolve(webOutcome, readScope.Fallback);
                    RecordWebReadLatency(readLatencyRecorder, name, resolvedOutcome, stopwatch.ElapsedMilliseconds, context, readScope.Scope);
                    OfferWebSlowRead(readLatencyRecorder, readScope.Scope, name, resolvedOutcome, stopwatch.ElapsedMilliseconds, context,
                        resolvedOutcome == ReadOutcome.Error ? "tool_error" : SlowReadLog.ErrorClassOf(resolvedOutcome));

                    /* #5678: a tool-caught sentence for a browser that has left is not a failure either (see ThrowIfCallerGone). */
                    ThrowIfCallerGone(result, context.RequestAborted);
                    var cacheable = resolvedOutcome == ReadOutcome.Ok && ClassifyToolResponse(result) == ToolResponseKind.JsonPassthrough
                        && ReadResultCache.IsCacheableText(result, gcf: false);
                    return new WebReadAnswer(ToHttpResult(result, DarlingWebFailureLog.RouteOf(context.Request), logger, stopwatch.ElapsedMilliseconds, context.RequestAborted),
                        cacheable ? result : null, resolvedOutcome);
                }
            });
        }

        MapCustomViews(app, postgres, logger, readLatencyRecorder, readAdmission, managedStore, adminArm, composeReadPostgres);
        MapCustomAlerts(app, postgres, logger, readAdmission, readLatencyRecorder);
        MapMuteRules(app, postgres, logger);
        DarlingKnownPatternRoutes.Map(app, postgres, logger);
        MapInvestigationNotes(app, postgres, new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds), logger, managedStore, adminArm);
        MapThresholdOverrides(app, postgres, logger, managedStore, adminArm);
        MapAgentSetup(app, postgres, logger, passwordKeys, managedStore, adminArm);
        MapAgentChanges(app, postgres, new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds), logger, ownerSource, managedStore, adminArm);
        MapServerTags(app, postgres, logger);
        MapServerActions(app, postgres, new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds), logger, ownerSource, managedStore, adminArm);
        MapPeople(app, postgres, new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds), logger, ownerSource, managedStore, adminArm);
        MapAdminServers(app, postgres);
        MapQueryTags(app, postgres, logger, readAdmission, readLatencyRecorder);
        MapCollectorRuns(app, new PostgresSnapshotNowStore(postgres), logger, async (name, ct) => { var (r, err) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, name, ct, NameClash.Refuse); return (r.ServerId, err); });
        MapActiveQueriesLive(app, new PostgresActiveQueriesLiveStore(postgres), logger, async (name, ct) => { var (r, err) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, name, ct, NameClash.Refuse); return (r.ServerId, err); }, managedStore, readAdmission: readAdmission, readLatencyRecorder: readLatencyRecorder);
        MapAnalyzeNow(app, new PostgresAnalyzeNowStore(postgres), postgres, logger, async (name, ct) => { var (r, err) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, name, ct, NameClash.Refuse); return (r.ServerId, err); }, managedStore);
        MapServers(app, postgres, logger, allowWindowsAuthOverUnverifiedTls: allowWindowsAuthOverUnverifiedTls);
        MapAlertHistoryDismiss(app, postgres, logger);
        MapInvestigationFeedback(app, postgres, logger, managedStore, adminArm);
        MapInvestigationExport(
            app, postgres, logger, dataDetail, pseudonymKeys, readAdmission, readLatencyRecorder,
            investigationExportGate ?? new InvestigationExportGate(InvestigationExportGate.FolderFor(
                DarlingManagedPostgres.ResolveDataDirectory(postgresConfig ?? new PostgresConfig()))),
            managedStore: managedStore, adminArm: adminArm);
        MapInvestigationPractice(app, postgres, logger);
        MapInvestigationPersonWrites(app, postgres, logger, managedStore, adminArm);
        MapWebhookSigningStatus(app, collector);

        /* The fleet sweep feed (#3466 lane 3): dedicated read routes like /api/fleet, over the same
           FleetSweepStore presentation reads lane 4's get_sweep_reports tool will serve — see
           DarlingFleetSweepEndpoints for the span discipline, the seat posture, and why its logger
           seat takes the service logger threaded here rather than app.Logger. */
        DarlingFleetSweepEndpoints.Map(app, postgres, logger, readAdmission, readLatencyRecorder);

        /* The per-alert triage page's assembly endpoint (#2710): everything it serves is already reachable
           through the /api/read mirror above — it adds assembly (alert match + anchored sections), not reach. */
        DarlingTriageEndpoint.Map(app, postgres, analysis, logger, readAdmission, readLatencyRecorder);

        /* The alert-notebook binding endpoint (#4222 slice C): the mechanical conversion of a firing into a
           read-only notebook definition. Same reach as the triage page it sits beside - everything it
           serves is already reachable through /api/read/*. */
        AlertNotebookEndpoint.Map(app, postgres, analysis, logger, readAdmission, readLatencyRecorder);
        DarlingServerDatabasesEndpoint.Map(app, postgres, logger, readAdmission, readLatencyRecorder); // #5245: the database picker's list
    }
}
