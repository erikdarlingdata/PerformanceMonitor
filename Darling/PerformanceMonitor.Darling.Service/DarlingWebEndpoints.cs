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
public static class DarlingWebEndpoints
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
    public static void MapAll(WebApplication app, NpgsqlDataSource postgres, CollectorRuntimeState collector, ILogger logger, BaselineCache? baselineCache = null, PostgresConfig? postgresConfig = null, ReadLatencyAccumulator? readLatency = null, PerformanceMonitor.PlanAnalysis.AnalyzerConfig? analyzerConfig = null, MonitoredServerRegistryState? registryState = null, SlowReadLog? slowReads = null, ReadAdmission? readAdmission = null, StoreOwnerSource? ownerSource = null, McpDataDetail dataDetail = McpDataDetail.MetadataOnly, IPseudonymKeySource? pseudonymKeys = null, InvestigationExportGate? investigationExportGate = null, IPasswordKeyRing? passwordKeys = null, WebAdminArm? adminArm = null, bool allowWindowsAuthOverUnverifiedTls = false)
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

        MapCustomViews(app, postgres, logger, readLatencyRecorder, readAdmission, managedStore, adminArm);
        MapCustomAlerts(app, postgres, logger, readAdmission, readLatencyRecorder);
        MapMuteRules(app, postgres, logger);
        DarlingKnownPatternRoutes.Map(app, postgres, logger);
        MapInvestigationNotes(app, postgres, new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds), logger, managedStore, adminArm);
        MapThresholdOverrides(app, postgres, logger, managedStore, adminArm);
        MapAgentSetup(app, postgres, logger, passwordKeys, managedStore, adminArm);
        MapAgentChanges(app, postgres, new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds), logger, ownerSource, managedStore, adminArm);
        MapServerTags(app, postgres, logger);
        MapServerActions(app, postgres, new AgentChangeUndo(postgres, McpCommandDeadlines.ReadSeconds), logger, ownerSource, managedStore, adminArm);
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

    /* ─────────────────────────── #1563 custom views: session, catalog, CRUD ─────────────────────────── */

    /// <summary>
    /// Maps the custom-views surface: the session capability probe, the read catalog the composer builds its
    /// picker from, and the CRUD routes. Editing is available to any AUTHENTICATED seat — the same reach as the
    /// rest of the web surface: over the network a caller has already passed the token→cookie + CIDR gate (the
    /// auth middleware runs before these routes), and on loopback the operator is on the host. The mutations are
    /// NOT restricted to loopback (network operation is the normal mode). Cross-site forgery is blocked by the
    /// always-on Host-header allowlist (anti-rebind), the SameSite=Strict session cookie, and the
    /// <c>application/json</c> content-type requirement below. Definitions are validated by
    /// <see cref="ValidateDefinition"/> (the authority) before any write; the store adds optimistic concurrency +
    /// duplicate-name conflict detection. Error bodies are always <c>{"error": "..."}</c>, matching the read surface.
    /// </summary>
    internal static void MapCustomViews(WebApplication app, NpgsqlDataSource postgres, ILogger logger, ReadLatencyRecorder readLatencyRecorder,
        ReadAdmission? readAdmission, bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var store = new CustomViewStore(postgres, McpCommandDeadlines.ReadSeconds);

        /* A request that reaches this endpoint has already cleared the host's auth gate (network: token→cookie +
           CIDR; loopback: on the host), so it holds a seat. WHICH seat decides the answer: the shared token and
           an OIDC admin may edit, an OIDC viewer may not. Reporting the request's own seat rather than a
           constant is what makes the SPA's read-only rendering reachable — it hides every edit affordance on
           can_edit: false, and the middleware's group-level write gate refuses the mutation regardless of what a
           client sends, so this is the affordance layer, never the enforcement. A failed probe fails closed to
           read-only in the UI. */
        app.MapGet("/api/session", (HttpContext context) =>
        {
            /* #6214: can_own is whether this session can own a personal view (it holds an owner key: an OIDC sign-in made after owner keys
               existed). The page decides the Personal checkbox from it, and "My views" and read-only from each view's own `mine`. Neither the
               subject nor the key is returned: a name decides nothing, and a key is never shown. */
            var seat = DarlingWebSeat.FromContext(context);
            return JsonNodeResult(new JsonObject { ["can_edit"] = seat.CanEdit, ["can_own"] = seat.OwnerKey is not null, ["signed_in_admin"] = DarlingWebSeat.IsSignedInAdmin(context), ["managed_store"] = managedStore, ["admin_writes_off"] = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm) });
        });

        /* The read catalog: input truth (read names + their WIRE query keys) the composer binds params from. */
        /* #5562: ?server= makes the collector intervals the SERVER's own (a per-server schedule row wins over the
           fleet-wide one); without it they are the fleet-wide effective interval. Either way the read's reach
           (max_hours) is the same: it belongs to the read, not the server. An unknown server is answered with the
           resolver's refusal, not a silent fall back to the fleet's numbers. */
        app.MapGet("/api/catalog", async (HttpContext context) =>
        {
            /* #5506 part 3 (round 2, L5): it reads the store (the schedule overrides, and the server resolver for ?server=), so
               it is admitted, and recorded, under its MCP twin describe_custom_view_catalog: a light read passes at once. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, CatalogRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, CatalogRoute, ReadWindowBand.None, context, readScope.Scope);
            var serverName = context.Request.Query["server"].ToString();
            if (string.IsNullOrWhiteSpace(serverName))
            {
                var fleetSchedules = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, null, context.RequestAborted);
                run.Complete(ReadOutcome.Ok);
                return JsonNodeResult(BuildCatalogNode(null, fleetSchedules));
            }

            var (resolved, resolveError) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, context.RequestAborted);
            if (resolveError != null)
            {
                run.Complete(ReadOutcome.Ok);
                return JsonNodeResult(JsonNode.Parse(resolveError)!.AsObject());
            }

            var schedules = await DarlingDataReader.ReadScheduleOverridesAsync(postgres, resolved.ServerId, context.RequestAborted);
            run.Complete(ReadOutcome.Ok);
            return JsonNodeResult(BuildCatalogNode(resolved.ServerId, schedules));
        });

        /* List — a bare array of summaries (no definition); [] when none. Every shared view, plus the caller's OWN personal views
           (#6214): a personal view is listed for its owner only. That is a list filter, not access control: GET by id below still returns
           any view, so a shared link keeps working. */
        app.MapGet("/api/views", async (HttpContext context) =>
        {
            var views = await store.ListAsync(callerOwnerKey: DarlingWebSeat.FromContext(context).OwnerKey, cancellationToken: context.RequestAborted);
            return JsonNodeResult(BuildSummariesNode(views));
        });

        /* Get one — full, including the definition; 404 when missing. */
        app.MapGet("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            var result = await store.GetAsync(id, DarlingWebSeat.FromContext(context).OwnerKey, context.RequestAborted);
            return result is CustomViewResult.Ok ok
                ? JsonNodeResult(BuildFullViewNode(ok.View!))
                : NotFoundResult();
        });

        /* Create — 201 + Location; 400 on a bad body/definition, 409 on a duplicate name. Any authenticated seat
           (the auth middleware already gated the request); application/json required (CSRF defense). */
        app.MapPost("/api/views", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseViewBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var validation = ValidateDefinition(request.DefinitionJson);
            if (!validation.IsValid)
            {
                return ErrorResult(validation.Error!, StatusCodes.Status400BadRequest);
            }

            /* #6214: a personal view is owned by the signed-in person's owner key, so a session that has none cannot make one: the shared
               token and the tokenless loopback operator have no person, and a cookie from before owner keys existed names a person but
               carries no key (they sign in again). Every create from a session that has a key stamps it, personal or not, so the creator
               can later make their own shared view personal. */
            var seat = DarlingWebSeat.FromContext(context);
            var personal = request.Personal ?? false;
            if (personal && seat.OwnerKey is null)
            {
                return ErrorResult(NoOwnerKeyMessage(seat), StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — an unexpected throw here (never ex.Message on the wire) is exactly what
               the #4281 top-of-pipeline backstop exists to answer, and MapAll wires this route after it. */
            var result = await store.CreateAsync(
                request.Name, request.Description, request.DefinitionJson,
                updatedBy: seat.EditorPrincipal, folder: request.Folder, personal: personal, ownerKey: seat.OwnerKey,
                cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomViewResult.Ok ok => CreatedResult(context, $"/api/views/{ok.View!.Id}", BuildFullViewNode(ok.View)),
                CustomViewResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomViewResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not create the view.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Update — 200 on success; 400 bad body/definition, 404 gone, 409 stale-version OR duplicate name.
           Any authenticated seat; application/json required. The client sends the version it edited; a mismatch
           is a 409, not a silent clobber. */
        app.MapPut("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseViewBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            if (request.Version is not { } expectedVersion)
            {
                return ErrorResult("'version' is required for an update (optimistic concurrency).", StatusCodes.Status400BadRequest);
            }

            var validation = ValidateDefinition(request.DefinitionJson);
            if (!validation.IsValid)
            {
                return ErrorResult(validation.Error!, StatusCodes.Status400BadRequest);
            }

            /* #6214: an omitted folder or personal flag keeps the stored value. A personal view is its owner's alone to change (the
               store's UPDATE holds the same rule, so a race cannot slip past this read). */
            var seat = DarlingWebSeat.FromContext(context);
            var current = await store.GetAsync(id, seat.OwnerKey, context.RequestAborted);
            if (current is not CustomViewResult.Ok { View: { } stored })
            {
                return NotFoundResult();
            }

            if (stored.Personal && !stored.Mine)
            {
                return ErrorResult(CustomViewStore.NotOwnerMessage, StatusCodes.Status403Forbidden);
            }

            var personal = request.Personal ?? stored.Personal;
            if (personal && seat.OwnerKey is null)
            {
                return ErrorResult(NoOwnerKeyMessage(seat), StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await store.UpdateAsync(
                id, request.Name, request.Description, request.DefinitionJson, expectedVersion,
                updatedBy: seat.EditorPrincipal, folder: request.FolderGiven ? request.Folder : stored.Folder, personal: personal,
                actorKey: seat.OwnerKey, cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomViewResult.Ok ok => JsonNodeResult(BuildFullViewNode(ok.View!)),
                CustomViewResult.NotFound => NotFoundResult(),
                CustomViewResult.Forbidden forbidden => ErrorResult(forbidden.Message, StatusCodes.Status403Forbidden),
                CustomViewResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomViewResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not update the view.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Delete — 204 on success, 404 when missing, 403 for somebody else's personal view (#6214). Any authenticated seat;
           application/json required. */
        app.MapDelete("/api/views/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await store.DeleteAsync(id, DarlingWebSeat.FromContext(context).OwnerKey, cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomViewResult.Ok => Results.NoContent(),
                CustomViewResult.Forbidden forbidden => ErrorResult(forbidden.Message, StatusCodes.Status403Forbidden),
                _ => NotFoundResult(),
            };
        });

        /* Compile-and-run a single composed panel spec (Custom Views v2, #1563) against the least-privilege
           viewer pool — the composer's live preview + the "view compiled SQL" transparency. application/json
           required (the same CSRF gate as the mutations). The panel is validated by the SAME
           ComposeSpec.TryParsePanel authority the write path uses, so a panel that previews here is a panel that
           stores — and the compiler emits only catalog identifiers, schema-qualified collect.*, every value
           bound; the viewer role's statement_timeout is the runaway backstop. */
        app.MapPost("/api/compose/run", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            JsonNode? root;
            try
            {
                root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
            }
            catch (JsonException)
            {
                return ErrorResult("Request body is not valid JSON.", StatusCodes.Status400BadRequest);
            }

            if (root is not JsonObject body)
            {
                return ErrorResult("Request body must be a JSON object with a 'panel'.", StatusCodes.Status400BadRequest);
            }

            /* The compile-and-run itself lives in the shared RunComposedPanelAsync (the ONE runner behind both
               this endpoint and the MCP run_custom_view_panel tool); this route only adds the web concerns —
               content-type gate, stream parse — then maps the discriminated outcome onto HTTP status.
               RunComposedPanelAsync's outward text is UNCHANGED by #4283: its outcome.Error keeps carrying the
               real "Error running query: {ex.Message}" sentence, because run_custom_view_panel's MCP caller
               (DarlingMcpCustomViewTools) reads that same field and #4283 does not touch what an MCP client
               sees. Only THIS web mapping (see ComposeRunFailureResult) stops putting a STORE fault's text on
               the wire (M1's outcome.Fault, checked before outcome.Error is ever read for the 400/500 split). */
            var stopwatch = Stopwatch.StartNew();
            var outcome = await RunComposedPanelAsync(postgres, body, context.RequestAborted, readLatencyRecorder, ComposeClientDeadlineHeadroomSeconds, remapClientTimeout: true, includeDataStartFields: true,
                caller: WebReadCaller(context), cacheHost: ReadSurface.Web);
            if (outcome.PayloadText is { } payloadText)
            {
                return Results.Text(payloadText, "application/json");
            }

            if (outcome.Payload is not null)
            {
                return JsonNodeResult(outcome.Payload);
            }

            /* #5506 part 3: the runner refused the panel at admission. */
            if (outcome.Busy is { } busy)
            {
                return BusyResult(busy);
            }

            /* #5678: any failed run for a browser that has left is not logged: a store fault, the generic catch's sentence, and a
               panel author's own query error (ComposeRunFailureResult's 400 Warning) all end the request instead. A 404 is
               answered as before. */
            if (outcome.Payload is null && !outcome.IsNotFound)
            {
                context.RequestAborted.ThrowIfCancellationRequested();
            }

            return ComposeRunFailureResult(outcome, "/api/compose/run", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>
    /// The custom-alert-rule web API (#3285, Component 7's backend) — the SAME surface the
    /// <see cref="Mcp.DarlingMcpCustomAlertTools"/> MCP tools expose, mirrored onto HTTP so a web editor (its JS
    /// deferred) can list / read / create / update / delete / validate / test the user-authored alert rules and
    /// browse the starter templates. Deliberately a THIN wrapper, never a second implementation: persistence is
    /// the same <see cref="CustomAlertRuleStore"/>, definition validation the same
    /// <see cref="CustomAlertRuleDefinition.TryParse"/> authority, delete the same
    /// <see cref="CustomAlertEvaluator.ResolveAndDeleteRuleAsync"/> (so an open incident is force-resolved before
    /// the FK cascade drops its state, #3305), templates the same <see cref="CustomAlertTemplates"/>, and
    /// evaluate-now the same <see cref="Mcp.DarlingMcpCustomAlertTools.TestCustomAlertRule"/> seam — so the web
    /// and MCP surfaces cannot drift. The routes shadow <see cref="MapCustomViews"/> exactly: the reads are open
    /// to any seat; the writes (POST/PUT/DELETE, plus the two evaluate POSTs) require <c>application/json</c> (the
    /// CSRF gate) and are born gated by the host's method-based write gate
    /// (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/> refuses a read-only seat everything unsafe except
    /// <c>POST /api/compose/run</c>), so a viewer seat gets a 403 on every one of them without this method
    /// naming the gate. The store/evaluator run on the host's least-privilege VIEWER pool (the same
    /// <paramref name="postgres"/> the reads and <c>/api/compose/run</c> use — never the owner pool).
    /// </summary>
    private static void MapCustomAlerts(WebApplication app, NpgsqlDataSource postgres, ILogger logger, ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        var store = new CustomAlertRuleStore(postgres, McpCommandDeadlines.ReadSeconds);

        /* List — a bare array of summaries (no definition), carrying 'enabled'; [] when none. */
        app.MapGet("/api/alerts", async (HttpContext context) =>
        {
            var rules = await store.ListAsync(context.RequestAborted);
            return JsonNodeResult(BuildRuleSummariesNode(rules));
        });

        /* Get one — full, including the definition and 'enabled'; 404 when missing. */
        app.MapGet("/api/alerts/{id:long}", async (HttpContext context, long id) =>
        {
            var result = await store.GetAsync(id, context.RequestAborted);
            return result is CustomAlertRuleResult.Ok ok && ok.Rule is not null
                ? JsonNodeResult(BuildFullRuleNode(ok.Rule))
                : AlertNotFoundResult();
        });

        /* Create — 201 + Location; 400 on a bad body/definition, 409 on a duplicate name. The definition is
           VALIDATED (CustomAlertRuleDefinition.TryParse — the SAME authority the evaluator applies on load)
           BEFORE any store hit, so a stored rule always parses. application/json required (CSRF defense). */
        app.MapPost("/api/alerts", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseAlertBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var (parsed, definitionError) = CustomAlertRuleDefinition.TryParse(request.DefinitionJson);
            if (parsed is null || definitionError is not null)
            {
                return ErrorResult(definitionError ?? "definition is invalid.", StatusCodes.Status400BadRequest);
            }

            var (createExpiry, createExpiryError) = Mcp.DarlingMcpCustomAlertTools.ResolveCreateExpiry(request.ExpiresAtText);
            if (createExpiryError is not null)
            {
                return ErrorResult(createExpiryError, StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — an unexpected throw here (never ex.Message on the wire) is exactly what
               the #4281 top-of-pipeline backstop exists to answer, and MapAll wires this route after it. */
            var result = await store.CreateAsync(
                request.Name, request.Description, request.DefinitionJson, request.Enabled,
                updatedBy: DarlingWebSeat.FromContext(context).EditorPrincipal, expiresAtUtc: createExpiry,
                cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => CreatedResult(context, $"/api/alerts/{ok.Rule!.Id}", BuildFullRuleNode(ok.Rule)),
                CustomAlertRuleResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomAlertRuleResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not create the alert rule.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Update — 200 on success; 400 bad body/definition, 404 gone, 409 stale-version OR duplicate name. A
           full replace (the client sends the whole rule it edited), exactly like PUT /api/views: 'version' is
           required, and a mismatch is a 409, not a silent clobber. application/json required. */
        app.MapPut("/api/alerts/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (request, bodyError) = await ParseAlertBodyAsync(context);
            if (request is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            if (request.Version is not { } expectedVersion)
            {
                return ErrorResult("'version' is required for an update (optimistic concurrency).", StatusCodes.Status400BadRequest);
            }

            var (parsed, definitionError) = CustomAlertRuleDefinition.TryParse(request.DefinitionJson);
            if (parsed is null || definitionError is not null)
            {
                return ErrorResult(definitionError ?? "definition is invalid.", StatusCodes.Status400BadRequest);
            }

            /* #6235: a full replace, so an absent expiry clears it. A value that is in the past is refused only when it is a CHANGE: an
               editor that re-saves an already-expired rule sends its old expiry back, and that must still save. */
            var existing = await store.GetAsync(id, context.RequestAborted);
            if (existing is not CustomAlertRuleResult.Ok { Rule: { } existingRule })
            {
                return AlertNotFoundResult();
            }

            var (updateExpiry, updateExpiryError) = Mcp.DarlingMcpCustomAlertTools.ResolveUpdateExpiry(
                request.ExpiresAtText ?? string.Empty, existingRule.ExpiresAtUtc);
            if (updateExpiryError is not null)
            {
                return ErrorResult(updateExpiryError, StatusCodes.Status400BadRequest);
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await store.UpdateAsync(
                id, request.Name, request.Description, request.DefinitionJson, request.Enabled, expectedVersion,
                updatedBy: DarlingWebSeat.FromContext(context).EditorPrincipal, expiresAtUtc: updateExpiry,
                cancellationToken: context.RequestAborted);
            return result switch
            {
                CustomAlertRuleResult.Ok ok => JsonNodeResult(BuildFullRuleNode(ok.Rule!)),
                CustomAlertRuleResult.NotFound => AlertNotFoundResult(),
                CustomAlertRuleResult.Conflict conflict => ErrorResult(conflict.Message, StatusCodes.Status409Conflict),
                CustomAlertRuleResult.Invalid invalid => ErrorResult(invalid.Message, StatusCodes.Status400BadRequest),
                _ => ErrorResult("Could not update the alert rule.", StatusCodes.Status500InternalServerError),
            };
        });

        /* Delete — 204 on success, 404 when missing. application/json required. Routes through
           CustomAlertEvaluator.ResolveAndDeleteRuleAsync (NOT store.DeleteAsync): any OPEN incident is
           force-resolved — a recovery row written to alert history — BEFORE the delete's FK cascade drops the
           per-server state that says which (rule, server) pairs were firing (#3305), so nothing is left showing
           as firing forever. The resolve is best-effort inside that method (its own failure isolation), so a
           resolve blip never blocks the delete the operator asked for. Same call the MCP delete tool makes;
           logger null here as there (the recovery row still writes; only the service-log line is skipped). */
        app.MapDelete("/api/alerts/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            /* #4283: no local catch — see the create route's comment above. */
            var result = await CustomAlertEvaluator.ResolveAndDeleteRuleAsync(postgres, id, logger: null, context.RequestAborted);
            return result is CustomAlertRuleResult.Ok ? Results.NoContent() : AlertNotFoundResult();
        });

        /* The starter templates (#3325): the SAME code-defined set the list_custom_alert_templates MCP tool
           returns, byte-identical (wraps that tool), mapped through the shared ToHttpResult exactly as the
           /api/read/* tool endpoints map their string results. Read-only, touches no store; open to any seat. */
        app.MapGet("/api/alert-templates", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpCustomAlertTools.ListCustomAlertTemplates();
            return ToHttpResult(result, "/api/alert-templates", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        /* Validate — dry-run a definition WITHOUT persisting; returns {valid, error}. The definition rides as an
           embedded JSON object (same as create/update bodies), validated by the SAME
           CustomAlertRuleDefinition.TryParse authority the MCP validate_custom_alert_rule tool uses, so the two
           verdicts cannot diverge. A POST (application/json required); born gated like the mutations — an
           evaluate/validate is an editor affordance, not a read the viewer seat is granted. */
        app.MapPost("/api/alerts/validate", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (definitionJson, bodyError) = await ParseDefinitionBodyAsync(context);
            if (definitionJson is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var (parsed, validationError) = CustomAlertRuleDefinition.TryParse(definitionJson);
            return JsonNodeResult(new JsonObject
            {
                ["valid"] = parsed is not null && validationError is null,
                ["error"] = validationError,
            });
        });

        /* Test (evaluate-now) — for a SAVED rule (rule_id) OR a supplied definition, read the metric's CURRENT
           value on each in-scope server and report whether it WOULD breach right now, delivering/persisting
           nothing. Wraps the MCP test_custom_alert_rule tool for zero divergence: that method resolves the
           in-scope servers, runs the SHARED EvaluateScalarNowAsync per-server scalar seam, and applies
           ClassifyTestValue — all on the pool it is handed, which here is the host's VIEWER pool (exactly as
           /api/compose/run runs the composer — never the owner pool). rule_id XOR definition is enforced inside
           the tool; its result is mapped through the shared ToHttpResult like every /api/read/* tool string — the
           {status,...} envelope (invalid / not_found / no_in_scope_servers) passes through as 200, an unexpected
           tool error becomes 500. A POST (application/json required); born gated like the mutations. */
        app.MapPost("/api/alerts/test", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (ruleId, definitionJson, bodyError) = await ParseAlertTestBodyAsync(context);
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            /* #5506 part 3: a rule test reads collected data like any read, admitted and recorded under its MCP twin's name. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, "test_custom_alert_rule", ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, "test_custom_alert_rule", ReadWindowBand.None, context, readScope.Scope);
            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpCustomAlertTools.TestCustomAlertRule(postgres, ruleId, definitionJson);
            run.Complete(ToolResultOutcome(result, context.RequestAborted));
            return ToHttpResult(result, "/api/alerts/test", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /* ─────────────────────── #3450 mute rules: the dedicated write endpoints ─────────────────────── */

    /// <summary>
    /// The pending agent notes screen's routes (#5903): <c>GET /api/investigation-notes/pending</c> lists the notes an agent
    /// proposed that nobody has decided, and <c>PUT /api/investigation-notes/{id}/approve</c> and <c>.../reject</c> decide
    /// one. The write routes answer as the mute-rule writes do: <c>application/json</c> required (the same CSRF discipline),
    /// born gated by the host's write gate (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/> refuses every unsafe
    /// method to a read-only seat), 404 when the note is gone or already decided, 403 with <see cref="NoteDecisionRefusedMessage"/>
    /// when the store refuses the write for privilege (SQLSTATE 42501: deciding a note is an <c>admin</c> role write, and the web
    /// host runs on whatever login it was given, the <c>viewer</c> role on a managed or compose store), 400 when the person
    /// rule refuses the seat's name (<see cref="InvestigationPersonWrites.ValidateActor"/>), a fixed-sentence 500 or 503
    /// for any other store failure. The decision is stamped as every web write is: the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>
    /// (the signed-in subject, else <c>web</c>) shaped as <c>person:&lt;name&gt;</c>, as the Viewer stamps a decision. The list body goes through the statement sweep (#4348) like every other body that carries text an agent wrote.
    /// </summary>
    private static void MapInvestigationNotes(WebApplication app, NpgsqlDataSource postgres, InvestigationRequestStore notes, ILogger logger, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* #6600: a decision is written as the login the seat's admin writes use; the pending list stays on the web's own pool. */
        InvestigationRequestStore DecisionStore(HttpContext context) =>
            adminArm is null ? notes : new InvestigationRequestStore(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/investigation-notes/pending", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                /* One row past the cap tells a full page from a cut one (#5903). */
                var pending = await notes.ListPendingNotesAsync(PendingNotesLimit + 1, context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    PendingNotesBody(pending), "/api/investigation-notes/pending", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-notes/pending", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-notes/{id:long}/approve", async (HttpContext context, long id) =>
        {
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var decidedBy = NoteDecider(DarlingWebSeat.FromContext(context));
            if (InvestigationPersonWrites.ValidateActor(decidedBy) is not null)
            {
                return PersonActorRefusedResult();
            }

            try
            {
                if (!await DecisionStore(context).ApproveNoteAsync(id, decidedBy, context.RequestAborted))
                {
                    return ErrorResult("That note is not pending: it was already decided, or it is gone.", StatusCodes.Status404NotFound);
                }

                return DarlingWebStatementSweep.JsonText(
                    NoteDecisionBody("approved", id, decidedBy), "/api/investigation-notes/{id}/approve", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return NoteDecisionRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-notes/{id}/approve", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-notes/{id:long}/reject", async (HttpContext context, long id) =>
        {
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var decidedBy = NoteDecider(DarlingWebSeat.FromContext(context));
            if (InvestigationPersonWrites.ValidateActor(decidedBy) is not null)
            {
                return PersonActorRefusedResult();
            }

            try
            {
                if (!await DecisionStore(context).RejectNoteAsync(id, decidedBy, context.RequestAborted))
                {
                    return ErrorResult("That note is not pending: it was already decided, or it is gone.", StatusCodes.Status404NotFound);
                }

                return DarlingWebStatementSweep.JsonText(
                    NoteDecisionBody("rejected", id, decidedBy), "/api/investigation-notes/{id}/reject", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return NoteDecisionRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-notes/{id}/reject", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// The threshold-overrides grid's routes (#5916 part 4): <c>GET /api/threshold-overrides</c> lists the overrides and
    /// <c>PUT /api/threshold-overrides</c> sets or clears one (the body and the answers are <see cref="DarlingWebThresholdOverrides"/>'s).
    /// The write answers as the note decisions do (#5903): <c>application/json</c> required, born gated by the host's write gate,
    /// 403 with <see cref="DarlingWebThresholdOverrides.RefusedMessage"/> when the store login holds no write grant on the table
    /// (the <c>viewer</c> role of a managed or compose store), 400 for a refused key, value or server. Every body goes through the
    /// statement sweep (#4348): <c>set_by</c> on an agent's override is text an agent named.
    /// </summary>
    private static void MapThresholdOverrides(WebApplication app, NpgsqlDataSource postgres, ILogger logger, bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        app.MapGet("/api/threshold-overrides", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                return DarlingWebStatementSweep.JsonText(
                    await DarlingWebThresholdOverrides.ListBodyAsync(postgres, context.RequestAborted, WebAdminArm.SourceFor(adminArm, context, postgres)),
                    "/api/threshold-overrides", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/threshold-overrides", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/threshold-overrides", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            JsonObject? body;
            try
            {
                body = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted) as JsonObject;
            }
            catch (JsonException)
            {
                body = null;
            }

            if (body is null)
            {
                return ErrorResult("The body must be a JSON object: {server_name, key, value, database_name?, clear?}.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var (status, answer) = await DarlingWebThresholdOverrides.ChangeAsync(
                    WebAdminArm.SourceFor(adminArm, context, postgres), logger, body, DarlingWebSeat.FromContext(context).EditorPrincipal,
                    context.RequestAborted, managedStore, adminWritesOff);
                return DarlingWebStatementSweep.JsonText(
                    answer, "/api/threshold-overrides", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, status);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/threshold-overrides", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// The agent setup routes (#6264), level with the desktop Viewer: <c>GET /api/agent-setup/access</c> says whether this login can write;
    /// <c>POST /api/notification-routes</c>, <c>PUT /api/notification-routes/{id}</c> and <c>DELETE /api/notification-routes/{id}</c> create, edit and
    /// delete a route's agent channel, match, scope, enabled flag, resolution override, fallback and destinations (webhook URLs and routing key sealed
    /// as the Viewer and the MCP route tools seal them, never answered; email recipients); <c>GET /api/environment-profiles</c> lists the profiles and <c>PUT /api/environment-profiles</c> saves or removes one.
    /// The writes need <c>application/json</c>, are born gated by the host's write gate (a read-only seat is refused every unsafe method), answer
    /// 403 when the store refuses for privilege (SQLSTATE 42501: the <c>viewer</c> role of a managed or compose store), 404 for a route that is
    /// gone and 400 with the shared rule's sentence for a refused value. The work is <see cref="DarlingWebAgentSetup"/>'s.
    /// </summary>
    private static void MapAgentSetup(WebApplication app, NpgsqlDataSource postgres, ILogger logger, IPasswordKeyRing? passwordKeys, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        app.MapGet("/api/agent-setup/access", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                return Results.Json(DarlingWebAgentSetup.ForSeat(
                    await DarlingWebAgentSetup.AccessAsync(WebAdminArm.SourceFor(adminArm, context, postgres), context.RequestAborted), DarlingWebSeat.IsSignedInAdmin(context)));
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-setup/access", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapGet("/api/environment-profiles", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                return DarlingWebStatementSweep.JsonText(
                    DarlingWebAgentSetup.ForSeat(
                        await DarlingWebAgentSetup.ProfilesBodyAsync(postgres, context.RequestAborted, WebAdminArm.SourceFor(adminArm, context, postgres)),
                        DarlingWebSeat.IsSignedInAdmin(context)),
                    "/api/environment-profiles", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/environment-profiles", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/environment-profiles", async Task<IResult> (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/environment-profiles",
                (body, editor, ct) => DarlingWebAgentSetup.SaveProfileAsync(WebAdminArm.SourceFor(adminArm, context, postgres), body, editor, ct, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/environment-profiles", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        app.MapPost("/api/notification-routes", async Task<IResult> (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/notification-routes",
                (body, editor, ct) => DarlingWebAgentSetup.SaveRouteAsync(WebAdminArm.SourceFor(adminArm, context, postgres), logger, null, body, editor, passwordKeys ?? DarlingPasswordKey.Current, ct, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/notification-routes", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        app.MapPut("/api/notification-routes/{id:int}", async Task<IResult> (HttpContext context, int id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var run = await AgentSetupRunAsync(context, logger, "/api/notification-routes/{id}",
                (body, editor, ct) => DarlingWebAgentSetup.SaveRouteAsync(WebAdminArm.SourceFor(adminArm, context, postgres), logger, id, body, editor, passwordKeys ?? DarlingPasswordKey.Current, ct, managedStore, adminWritesOff));
            return DarlingWebStatementSweep.JsonText(
                run.Body, "/api/notification-routes/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, run.Status);
        });

        app.MapDelete("/api/notification-routes/{id:int}", async (HttpContext context, int id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["error"] = InvestigationWriteSignInMessage }, "/api/notification-routes/{id}", logger,
                    stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }

            /* #6265: the person is named as a note decision names them; a signed-in name the journal cannot hold is refused before anything runs. */
            if (PersonWriteActor(context) is null)
            {
                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["error"] = PersonWriteActorRefusal }, "/api/notification-routes/{id}", logger,
                    stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                var (status, answer) = await DarlingWebAgentSetup.DeleteRouteAsync(
                    WebAdminArm.SourceFor(adminArm, context, postgres), logger, id, DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted,
                    managedStore, adminWritesOff);
                return DarlingWebStatementSweep.JsonText(
                    answer, "/api/notification-routes/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, status);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/notification-routes/{id}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>One agent setup write: <c>application/json</c> required (the same CSRF discipline as the other web writes), the body a JSON
    /// object, the seat's <see cref="DarlingWebSeat.EditorPrincipal"/> as the editor. Always answers a status and a body for the route to send
    /// through the statement sweep: a refusal's sentence, or the fixed-sentence 500 or 503 of a failure.</summary>
    private static async Task<(int Status, JsonNode Body)> AgentSetupRunAsync(
        HttpContext context, ILogger logger, string route,
        Func<JsonObject, string, CancellationToken, Task<(int Status, JsonNode Body)>> write)
    {
        /* #6274: every agent setup write needs a signed-in admin. A dashboard with no sign-in stores no seat, so the default shared-token
           seat that FromContext hands back would let any process on the host change routes and profiles with no credential. */
        if (!DarlingWebSeat.IsSignedInAdmin(context))
        {
            return (StatusCodes.Status403Forbidden, new JsonObject { ["error"] = InvestigationWriteSignInMessage });
        }

        if (!IsJsonContentType(context.Request.ContentType))
        {
            return (StatusCodes.Status415UnsupportedMediaType, new JsonObject { ["error"] = "Content-Type must be application/json." });
        }

        JsonObject? body;
        try
        {
            body = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted) as JsonObject;
        }
        catch (JsonException)
        {
            body = null;
        }

        if (body is null)
        {
            return (StatusCodes.Status400BadRequest, new JsonObject { ["error"] = "The body must be a JSON object." });
        }

        /* #6265: the person is named as a note decision names them (InvestigationPersonWrites.PersonActor); a signed-in name the journal
           cannot hold is refused with the same fixed sentence, before anything runs. */
        if (PersonWriteActor(context) is null)
        {
            return (StatusCodes.Status400BadRequest, new JsonObject { ["error"] = PersonWriteActorRefusal });
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            return await write(body, DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
            return (DarlingWebFailureLog.StatusCode(ex), DarlingWebFailureLog.Body(ex));
        }
    }

    /// <summary>The 403 sentence a note decision answers when the store refuses the write (SQLSTATE 42501): the web host runs on
    /// whatever login <c>postgres.webConnectionString</c> names, and deciding a note is an <c>admin</c> role write (#5903).</summary>
    internal static string NoteDecisionRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("decide notes", managedStore, "Decide them in the Viewer app", adminWritesOff: adminWritesOff);

    /// <summary>The sentence a person-made investigation change answers with 403 when the request holds no signed-in admin seat (#6265):
    /// a dashboard with no sign-in configured has no caller to name, so the change is made in the Viewer or after signing in.</summary>
    internal const string InvestigationWriteSignInMessage = "Sign in to the web as an admin to make this change, or use the Viewer.";

    /// <summary>The 403 answered to a change that needs a signed-in admin: <see cref="InvestigationWriteSignInMessage"/>, nothing written.</summary>
    private static IResult SignInRequiredResult() => ErrorResult(InvestigationWriteSignInMessage, StatusCodes.Status403Forbidden);

    /// <summary>The 403 a refused note decision answers: the fixed <see cref="NoteDecisionRefusedMessage"/> sentence in the mute-rule error envelope.</summary>
    private static IResult NoteDecisionRefusedResult(bool managedStore, bool adminWritesOff = false) =>
        ErrorResult(NoteDecisionRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);

    /// <summary>True when the store refused the statement for privilege (SQLSTATE 42501).</summary>
    internal static bool IsInsufficientPrivilege(Exception ex) => ex is PostgresException { SqlState: "42501" };

    /// <summary>The most pending notes one list answers. A fleet that proposes more than this has a backlog the screen cannot
    /// show whole; the list says so (<c>truncated</c>), and deciding the oldest shows the rest.</summary>
    internal const int PendingNotesLimit = 200;

    /// <summary>Who a note decision is stamped as: <c>person:&lt;signed-in name&gt;</c>, the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>
    /// shaped by <see cref="InvestigationPersonWrites.PersonActor"/> as the Viewer and the web's own person writes record a person (#6265).
    /// The signed-in subject, else <c>web</c> for the shared token, so that token's decision is <c>person:web</c>.</summary>
    internal static string NoteDecider(DarlingWebSeat seat) => InvestigationPersonWrites.PersonActor(seat.EditorPrincipal);

    /// <summary>The list body: <c>{notes_returned, truncated, notes: [{note_id, server_id, server_name, metric_name, family,
    /// story_path_hash, note_text, author, source_request_id, proposed_at_utc, expires_at_utc}]}</c>, oldest proposal first.
    /// <paramref name="pending"/> is read with <see cref="PendingNotesLimit"/> + 1 rows: the first <see cref="PendingNotesLimit"/>
    /// are returned and <c>truncated</c> is true when the extra row exists.</summary>
    internal static JsonObject PendingNotesBody(IReadOnlyList<PendingInvestigationNote> pending)
    {
        var array = new JsonArray();
        foreach (var note in pending.Take(PendingNotesLimit))
        {
            array.Add(new JsonObject
            {
                ["note_id"] = note.NoteId,
                ["server_id"] = note.ServerId,
                ["server_name"] = note.ServerName,
                ["metric_name"] = note.MetricName,
                ["family"] = note.Family,
                ["story_path_hash"] = note.StoryPathHash,
                ["note_text"] = note.NoteText,
                ["author"] = note.Author,
                ["source_request_id"] = note.SourceRequestId?.ToString(),
                ["proposed_at_utc"] = note.ProposedAtUtc.ToString("o", CultureInfo.InvariantCulture),
                ["expires_at_utc"] = note.ExpiresAtUtc.ToString("o", CultureInfo.InvariantCulture),
            });
        }

        return new JsonObject { ["notes_returned"] = array.Count, ["truncated"] = pending.Count > PendingNotesLimit, ["notes"] = array };
    }

    internal static JsonObject NoteDecisionBody(string status, long noteId, string decidedBy) =>
        new() { ["status"] = status, ["note_id"] = noteId, ["decided_by"] = decidedBy };

    /* ─────────────────────── #5917 agent changes: the list, one-click undo and the switch ─────────────────────── */

    /// <summary>The 403 sentence an undo or a switch answers when the store refuses the write for privilege (SQLSTATE 42501): the web host
    /// runs on whatever login <c>postgres.webConnectionString</c> names, the <c>viewer</c> role on a managed or compose store (#5917).</summary>
    internal static string AgentChangeRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("undo or switch off agent changes", managedStore, adminWritesOff: adminWritesOff);

    /// <summary>The 403 a refused undo or switch answers: the fixed <see cref="AgentChangeRefusedMessage"/> sentence in the error envelope.</summary>
    private static IResult AgentChangeRefusedResult(bool managedStore, bool adminWritesOff = false) =>
        ErrorResult(AgentChangeRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);

    /// <summary>
    /// The Agent changes routes (#5917): <c>GET /api/agent-changes</c> lists the calls (with their result, or "unknown"), the refusals,
    /// the switch and <c>can_decide</c> (false under the viewer role: the page then lists only); <c>GET /api/agent-changes/{id}/muted-alerts</c>
    /// lists the alerts muted since a mute change; <c>PUT /api/agent-changes/{id}/undo</c> undoes one change and <c>PUT /api/agent-changes/switch</c>
    /// turns every agent change on or off. The writes need <c>application/json</c>, are gated by the host's write gate, answer 403 with
    /// <see cref="AgentChangeRefusedMessage"/> when the store refuses for privilege, 404 when the call is not in the journal, 409 with the
    /// refusal ("changed since", name taken, already undone, no undo) and 400 for a bad body or login. Journal text goes through
    /// <c>InvestigationFieldMasking.JournalDetail</c> at the web's level (full, as the other web reads of the journal).
    /// </summary>
    private static void MapAgentChanges(WebApplication app, NpgsqlDataSource postgres, AgentChangeUndo changes, ILogger logger, StoreOwnerSource? ownerSource = null,
        bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* #6600: the access check, the undo and the switch run as the login the seat's admin writes use (a plan undo journals through it too);
           the list and the muted alerts stay on the web's own pool. */
        AgentChangeUndo AdminChanges(HttpContext context) =>
            adminArm is null ? changes : new AgentChangeUndo(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/agent-changes", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var read = await changes.ReadAsync(AgentChangesLimit, context.RequestAborted);
                var canDecide = await AdminChanges(context).CanDecideAsync(context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    AgentChangesBody(read, canDecide, DateTime.UtcNow), "/api/agent-changes", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapGet("/api/agent-changes/{id:long}/muted-alerts", async (HttpContext context, long id) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var alerts = await changes.ReadMutedAlertsAsync(id, 200, context.RequestAborted);
                var array = new JsonArray();
                foreach (var a in alerts)
                {
                    array.Add(new JsonObject
                    {
                        ["alert_time_utc"] = a.AlertTimeUtc.ToString("o", CultureInfo.InvariantCulture), ["server_id"] = a.ServerId,
                        ["server_name"] = a.ServerName, ["metric_name"] = a.MetricName, ["current_value"] = a.CurrentValue, ["threshold_value"] = a.ThresholdValue,
                    });
                }

                return DarlingWebStatementSweep.JsonText(new JsonObject { ["alerts_returned"] = array.Count, ["alerts"] = array },
                    "/api/agent-changes/{id}/muted-alerts", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes/{id}/muted-alerts", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/agent-changes/{id:long}/undo", async (HttpContext context, long id) =>
        {
            /* #6265: an undo is a person's act, so it needs a signed-in admin before anything is read or written. */
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var login = DarlingWebSeat.FromContext(context).EditorPrincipal;
            try
            {
                /* #5632 step 9: a plan force or unforce is undone by the service's plan force code, which reads the server first. The first request
                   (no confirm) answers the statement and the state and writes nothing; {"confirm": true, "statement": "<shown>"} writes. */
                var writeChanges = AdminChanges(context);
                if ((await changes.GetAsync(id, context.RequestAborted))?.IsPlanChange == true)
                {
                    /* A plan undo writes to the monitored server, so it needs a signed-in admin, not just the web's store login (#6269). */
                    if (!DarlingWebSeat.IsSignedInAdmin(context))
                    {
                        return SignInRequiredResult();
                    }

                    if (!await writeChanges.CanDecideAsync(context.RequestAborted))
                    {
                        return AgentChangeRefusedResult(managedStore, adminWritesOff);
                    }

                    var (planBody, planStatus) = await UndoPlanChangeAsync(context, writeChanges, ownerSource, logger, id, login);
                    return DarlingWebStatementSweep.JsonText(planBody, "/api/agent-changes/{id}/undo", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, planStatus);
                }

                var result = await writeChanges.UndoAsync(id, login, context.RequestAborted);
                if (result.Undone)
                {
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["status"] = "undone", ["called_id"] = id, ["undone_by"] = AgentChangeUndo.PersonActor(login), ["message"] = result.Words },
                        "/api/agent-changes/{id}/undo", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = result.Words, ["code"] = result.Code },
                    "/api/agent-changes/{id}/undo", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted,
                    result.Code == "not_found" ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict);
            }
            catch (ArgumentException)
            {
                return ErrorResult("The signed-in name cannot be recorded as the person who undid this change.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return AgentChangeRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes/{id}/undo", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/agent-changes/switch", async (HttpContext context) =>
        {
            /* #6265: the service-wide agent switch is a person's act, so it needs a signed-in admin before anything is read or written. */
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var login = DarlingWebSeat.FromContext(context).EditorPrincipal;
            try
            {
                if (JsonNode.Parse(await ReadBodyAsync(context)) is not JsonObject body || body["enabled"] is not JsonValue flag || !flag.TryGetValue(out bool enabled))
                {
                    return ErrorResult("Send {\"enabled\": true} or {\"enabled\": false}.", StatusCodes.Status400BadRequest);
                }

                if (!await AdminChanges(context).SetEnabledAsync(enabled, login, context.RequestAborted))
                {
                    /* #6401: no service row yet, so nothing was written; the switch never creates the row. */
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = AgentChangeUndo.SwitchStoreNotSetUpMessage, ["code"] = "store_not_set_up" },
                        "/api/agent-changes/switch", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status409Conflict);
                }

                return DarlingWebStatementSweep.JsonText(new JsonObject { ["status"] = enabled ? "on" : "off", ["enabled"] = enabled },
                    "/api/agent-changes/switch", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is JsonException or ArgumentException)
            {
                return ErrorResult("The request body is not valid, or the signed-in name cannot be recorded.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return AgentChangeRefusedResult(managedStore, adminWritesOff);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/agent-changes/switch", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>
    /// A person's undo of a plan force or unforce (#5632, step 9). 200 <c>{status: "confirm", statement, state, message}</c> without a confirm
    /// (nothing written); 200 <c>{status: "undone", ...}</c> with <c>{"confirm": true}</c>; a refusal is 409 <c>{error, code}</c> (404 for
    /// <c>not_found</c>); a server that took no write is 502. A viewer-role store is 403 before anything else, as the store kinds' undo answers it
    /// (the route checks that, then sweeps what this returns).
    /// </summary>
    internal static async Task<(JsonObject Body, int Status)> UndoPlanChangeAsync(HttpContext context, AgentChangeUndo changes, StoreOwnerSource? ownerSource, ILogger logger,
        long id, string login)
    {
        var confirm = false;
        string? shown = null;
        var raw = await ReadBodyAsync(context);
        if (!string.IsNullOrWhiteSpace(raw))
        {
            try
            {
                if (JsonNode.Parse(raw) is not JsonObject body)
                {
                    return (new JsonObject { ["error"] = "Send {} or {\"confirm\": true, \"statement\": \"<the statement shown>\"}." }, StatusCodes.Status400BadRequest);
                }

                confirm = body["confirm"] is JsonValue flag && flag.TryGetValue(out bool yes) && yes;
                shown = body["statement"] is JsonValue text && text.TryGetValue(out string? sentence) ? sentence : null;
            }
            catch (JsonException)
            {
                return (new JsonObject { ["error"] = "The request body is not valid JSON." }, StatusCodes.Status400BadRequest);
            }
        }

        /* #6569: an undo whose reply is lost journals a result-unknown action_called row, which only the store owner may write, so the plan undo runs on the owner source, as the
           MCP undo does. The gate above (IsSignedInAdmin, CanDecide) stays on the web login: it is the web's own, not the journal writer's. */
        var owner = ownerSource?.Read();
        var planChanges = owner is null ? changes : new AgentChangeUndo(owner, McpCommandDeadlines.ReadSeconds);
        var answer = await Mcp.DarlingMcpPlanForceTools.UndoPlanChangeAsync(planChanges, owner, logger, id, login, confirm, shown, context.RequestAborted);
        if (answer.Succeeded)
        {
            return (answer.ToJson(), StatusCodes.Status200OK);
        }

        return UndoRefusalBody(answer);
    }

    /// <summary>The words the browser gets when a plan undo does not succeed.</summary>
    internal const string UndoFailedWords =
        "The undo did not complete on the server. The Darling service log has the server's own words.";

    /// <summary>
    /// The body and status of a plan undo that was refused or failed (#5632, step 9). A refusal carries this code's own sentence. A failure
    /// carries the monitored server's exception text (the executor scrubs the secret from it, but it can still name hosts and addresses), so
    /// the browser gets <see cref="UndoFailedWords"/> and the code instead (#4283). The executor already logged the server's words.
    /// </summary>
    internal static (JsonObject Body, int Status) UndoRefusalBody(PlanUndoAnswer undoRefusal)
    {
        var failed = undoRefusal.Status == PlanUndoAnswer.Failed;
        var refused = new JsonObject
        {
            ["error"] = failed ? UndoFailedWords : undoRefusal.Message,
            ["code"] = undoRefusal.Code,
            ["status"] = undoRefusal.Status,
        };
        if (undoRefusal.State is not null)
        {
            refused["state"] = undoRefusal.State.DeepClone();
        }

        return (refused, undoRefusal.Code == "not_found" ? StatusCodes.Status404NotFound
            : undoRefusal.Code == "bad_login" ? StatusCodes.Status400BadRequest
            : failed ? StatusCodes.Status502BadGateway
            : StatusCodes.Status409Conflict);
    }

    /// <summary>The most calls and refusals the Agent changes list answers.</summary>
    internal const int AgentChangesLimit = 200;

    /// <summary>The list body: <c>{enabled, can_decide, changes: [{called_id, called_at_utc, request_id, server_id, server_name (the registry's name, or "Server N (removed)"), actor, reason, tool, kind, detail,
    /// result (done, failed or unknown), result_detail, undone_at_utc, undone_by, muted_alerts, can_undo}], refusals: [...]}</c>. A call with no
    /// result row is <c>unknown</c>. <c>detail</c> goes through the journal masking helper at the web's level.</summary>
    internal static JsonObject AgentChangesBody(AgentChangesRead read, bool canDecide, DateTime nowUtc)
    {
        var masker = McpDataDetailMasker.ForAnswer(McpDataDetail.Full);
        var calls = new JsonArray();
        foreach (var c in read.Changes)
        {
            calls.Add(new JsonObject
            {
                ["called_id"] = c.CalledId,
                ["called_at_utc"] = c.CalledAtUtc.ToString("o", CultureInfo.InvariantCulture),
                ["request_id"] = c.RequestId?.ToString(),
                ["server_id"] = c.ServerId,
                ["server_name"] = c.ServerName, ["server_removed"] = c.ServerId is not null && c.ServerName is null,
                ["actor"] = c.Actor,
                ["reason"] = c.Reason,
                ["tool"] = c.Tool,
                ["kind"] = c.Kind,
                ["detail"] = InvestigationFieldMasking.JournalDetail(c.DetailJson, masker),
                ["result"] = c.Result.ToString().ToLowerInvariant(),
                ["result_detail"] = c.ResultJson is null ? null : InvestigationFieldMasking.JournalDetail(c.ResultJson, masker),
                ["undone_at_utc"] = c.UndoneAtUtc?.ToString("o", CultureInfo.InvariantCulture),
                ["undone_by"] = c.UndoneBy,
                ["muted_alerts"] = c.MutedAlerts,
                ["force_review"] = c.ForceReview?.Words,
                ["force_checks_opened"] = c.ForceReview?.Opened,
                ["force_checks_answered"] = c.ForceReview?.Answered,
                ["can_undo"] = c.CanTryUndo(nowUtc),
            });
        }

        var refusals = new JsonArray();
        foreach (var r in read.Refusals)
        {
            refusals.Add(new JsonObject
            {
                ["journal_id"] = r.JournalId, ["at_utc"] = r.AtUtc.ToString("o", CultureInfo.InvariantCulture), ["request_id"] = r.RequestId?.ToString(),
                ["server_id"] = r.ServerId, ["server_name"] = r.ServerName, ["server_removed"] = r.ServerId is not null && r.ServerName is null,
                ["actor"] = r.Actor, ["code"] = r.Code, ["tool"] = r.Tool,
            });
        }

        return new JsonObject { ["enabled"] = read.Enabled, ["can_decide"] = canDecide, ["changes"] = calls, ["refusals"] = refusals };
    }

    /// <summary>
    /// The mute-rule WRITE surface (#3450): create / update / set-enabled / delete, the web twins of the four
    /// MCP alert-tuning verbs. <see cref="ExcludedToolNames"/> was never a trust boundary — only the list of
    /// tools with no <i>generic</i> <c>/api/read</c> mirror — so, exactly as the Custom Views tools got
    /// <c>/api/views</c>, the mute verbs get dedicated routes here; the read half already ships as
    /// <c>/api/read/get_mute_rules</c>.
    ///
    /// <para><b>Who may call these.</b> Nothing here names a seat, and that is the design: every unsafe method
    /// is born gated by the host's group-level write gate (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/>
    /// refuses a read-only seat everything non-GET except <c>POST /api/compose/run</c>), so an OIDC viewer seat
    /// gets 403 on all four routes while the admin and shared-token seats — and the tokenless loopback operator —
    /// pass, the same enforcement <c>/api/views</c> and <c>/api/alerts</c> ride. <c>application/json</c> is
    /// required on every route (the CSRF defense — see <see cref="IsJsonContentType"/> for why a non-simple
    /// Content-Type closes the simple-request forgery hole), the DELETE included, matching the views surface.</para>
    ///
    /// <para><b>Zero drift with MCP, by construction.</b> Each route is a thin HTTP mapping over the SAME core
    /// the matching MCP verb runs — <see cref="Mcp.DarlingMcpAlertTools.CreateMuteRuleCore"/> /
    /// <see cref="Mcp.DarlingMcpAlertTools.UpdateMuteRuleCore"/> /
    /// <see cref="Mcp.DarlingMcpAlertTools.SetMuteRuleEnabledCore"/> /
    /// <see cref="Mcp.DarlingMcpAlertTools.DeleteMuteRuleCore"/> — over the host's least-privilege VIEWER pool
    /// (whose narrow floor — <c>config_mute_rules</c> plus the two <c>config_service</c> beacon columns the
    /// SECURITY-INVOKER bump trigger writes as the caller — is provisioned beside the mcp role's in
    /// <c>DarlingManagedRoles</c>). The response BODY is the verb's own <c>{status, ...}</c> envelope, verbatim;
    /// only the HTTP status is added (<see cref="MuteRuleEnvelopeStatus"/>), so a web client and an MCP client
    /// read one truth. That buys the cores' invariants unduplicated: PARTIAL update with explicit-null clears
    /// through the store's own full-row <c>UpdateAsync</c>, an <c>unchanged</c> answer for a write that would
    /// change nothing, a re-read after every write so the reported rule is the STORED one, and
    /// <c>created_at_utc</c> never moving through any path — the #3306 clock the whole verb family exists to
    /// preserve. One cosmetic consequence is accepted with it: a refusal's prose speaks the MCP verbs' names
    /// ("use set_mute_rule_enabled"), which is the shared parser's vocabulary and maps 1:1 onto these routes.</para>
    ///
    /// <para><b>The operational caveat these endpoints inherit.</b> Alert rows spell <c>server_name</c> two ways
    /// — the monitor's own self-alert family uses the display-short name, engine alerts use the registry name —
    /// so a rule created or edited from ANY surface can go inert by matching the wrong spelling while looking
    /// scoped. Copy the spelling off the alert rows being muted (<c>/api/read/get_alert_history</c>) rather than
    /// retyping it; the update core's own description carries the same warning on the MCP side.</para>
    ///
    /// <para><b>Route shapes.</b> Create is a POST returning 201 with the stored rule — no <c>Location</c>
    /// header, deliberately: there is no per-rule GET (the read is the list, <c>/api/read/get_mute_rules</c>),
    /// and a Location that 404s is worse than none. Update is a PATCH — not the views surface's PUT — because
    /// the body is a partial document (send only the fields to change; an explicit null clears one) and calling
    /// that a full replace would misstate the one semantic the verb exists for. Set-enabled is a PUT of the tiny
    /// <c>{"enabled": bool}</c> sub-resource, the reversible flag flip that never touches another field. The id
    /// is the rule's GUID string from <c>get_mute_rules</c> / create's response.</para>
    /// </summary>
    private static void MapMuteRules(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new PgMuteRuleStore(postgres);

        /* A server_id in a create or update body must name a registered server, the same check the MCP tools
           make, so the web cannot write a rule keyed on an id that mutes nothing. */
        Func<int, Task<string?>> serverNameLookup = id => Mcp.DarlingMcpAlertTools.MonitoredServerDisplayNameAsync(postgres, id);

        /* Create — 201 with the STORED rule (re-read after the insert); 400 on a bad body/field/expiry; 409 with
           status already_exists (and the existing rule's id) when an enabled, unexpired rule already has the same
           scope, patterns and expiry, so a client retry leaves one rule (#4734). The
           body is one JSON object of the get_mute_rules field shape; {} is legal and creates a rule that mutes
           EVERY alert (the same whole-fleet silence an argument-less create_mute_rule builds — scope fields
           narrow, they are not required). application/json required. */
        app.MapPost("/api/mute-rules", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.CreateMuteRuleCore(store, await ReadBodyAsync(context), serverNameLookup);
            return MuteRuleToolResult(result, "/api/mute-rules", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        /* Update — PARTIAL, the merged update_mute_rule semantics verbatim: the body carries ONLY the fields to
           change, an explicit JSON null clears one, a field not sent does not move, 'enabled' is refused toward
           the flag route, and created_at_utc is structurally out of reach. 200 updated/unchanged, 404 unknown
           id, 400 bad field/value with nothing written. The raw body text IS the core's changes_json — no
           web-side re-parse to drift. application/json required. */
        app.MapPatch("/api/mute-rules/{id}", async (HttpContext context, string id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, await ReadBodyAsync(context), serverNameLookup);
            return MuteRuleToolResult(result, "/api/mute-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        /* Set-enabled — the reversible flag flip that keeps the rule's id, scope, reason and creation date (the
           reason it exists; delete-and-recreate resets the #3306 clock and delivers the suppressed alerts in
           between). 200 updated/unchanged, 404 unknown id. The body is exactly {"enabled": true|false}: strict,
           because the ONE field this route may move is the one field the update route refuses — a stray sibling
           key here is a caller who wanted PATCH. application/json required. */
        app.MapPut("/api/mute-rules/{id}/enabled", async (HttpContext context, string id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            JsonNode? root;
            try
            {
                root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
            }
            catch (JsonException)
            {
                return ErrorResult("Request body is not valid JSON.", StatusCodes.Status400BadRequest);
            }

            if (root is not JsonObject body || body["enabled"] is not JsonValue enabledValue
                || !enabledValue.TryGetValue<bool>(out var enabled) || body.Count != 1)
            {
                return ErrorResult(
                    "Request body must be exactly {\"enabled\": true|false}; any other field belongs to PATCH /api/mute-rules/{id}.",
                    StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, id, enabled);
            return MuteRuleToolResult(result, "/api/mute-rules/{id}/enabled", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        /* Delete — 200 with the verb's {status:"deleted", rule_id} envelope rather than the views surface's
           204, deliberately: the envelope is the ONE shape both surfaces answer with, and it names the id the
           caller should stop citing. 404 unknown id. Permanent — prefer the enabled route for anything
           reversible. application/json required (the same CSRF discipline as the bodyless views DELETE). */
        app.MapDelete("/api/mute-rules/{id}", async (HttpContext context, string id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpAlertTools.DeleteMuteRuleCore(store, id);
            return MuteRuleToolResult(result, "/api/mute-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>How long one <c>POST /api/servers</c> request may hold the one-at-a-time slot.</summary>
    internal static readonly TimeSpan ServerAddSlotTimeout = TimeSpan.FromSeconds(120);

    internal const string ServerAddTimedOutText = "Adding servers took too long; check the server list before retrying. If every later add is refused as busy, restart the service.";

    /// <summary>The most servers one <c>POST /api/servers</c> request may carry.</summary>
    internal const int MaxServersPerAddRequest = 20;

    /// <summary>The most request-body bytes the add route reads before it answers 400 (twenty entries are a few KB).</summary>
    internal const int MaxServerAddBodyBytes = 64 * 1024;

    /// <summary>What a redacted secret is replaced with in any text the add route puts on the wire or in a log.</summary>
    private const string RedactedSecret = "[redacted]";

    /// <summary>
    /// <c>POST /api/servers</c>: adds monitored servers (the web twin of the MCP <c>add_servers</c> tool). The body
    /// is the same JSON ARRAY of server objects the tool takes, and the route hands it to the SAME core
    /// (<see cref="DarlingMcpServerAdminTools.AddServersAsync(NpgsqlDataSource, string, DarlingMcpServerAdminTools.ServerProbe, CancellationToken)"/>),
    /// so the web surface accepts exactly what the tool accepts, probes the server from the service before saving,
    /// and answers with the core's own <c>{requested, added, skipped, collided, failed, results}</c> envelope. The
    /// core's whole-request refusal (<c>{"status":"invalid"}</c>) is a 400; every other envelope is a 200, with
    /// each entry's outcome in <c>results</c>.
    ///
    /// <para><b>Gates.</b> A read-only seat is refused by the host's group-level write gate
    /// (<see cref="DarlingWebSeat.IsRequestAllowed"/>) and again here, because this is the one route that carries a
    /// credential. <c>application/json</c> is required (415 otherwise, the CSRF defense the mute routes use). At most
    /// <see cref="MaxServersPerAddRequest"/> servers per request (400 above that). One add runs at a time per host
    /// process: a second request that arrives while one is running answers 429, because each entry is probed over
    /// the network and a caller must not be able to queue unbounded probes from the service. The write runs on the
    /// host's <c>viewer</c> pool, whose <c>INSERT</c> on <c>config_monitored_servers</c> is the floor beneath the
    /// seat gate; <c>encrypted_password</c> stays SELECT-carved from it.</para>
    ///
    /// <para><b>Credentials.</b> A password or client secret travels only in the request body. The route never echoes
    /// the body, never logs it, and every 400 it composes is a fixed sentence (a parse failure names no part of the
    /// body, because a JSON parser's message can quote the character it stopped at). Any submitted secret that
    /// appears in the core's answer is replaced with <see cref="RedactedSecret"/> before it is written, as a
    /// backstop: the core's answers carry none today. The core's caught-exception envelope never reaches the wire
    /// either: it answers through <see cref="ServerErrorResult"/>'s fixed body, like every other write route.</para>
    ///
    /// <para><b>Audit.</b> Who added a server is not stored on the row, so one Information line per added server is
    /// the record: the editor principal, the server name and the auth mode, never the secret.</para>
    /// </summary>
    internal static void MapServers(
        WebApplication app, NpgsqlDataSource postgres, ILogger logger,
        Func<string, Task<string>>? addServers = null,
        TimeSpan? addTimeout = null,
        Func<int, string, Task<string>>? editServer = null,
        TimeSpan? editTimeout = null,
        Func<int, Task<DarlingMcpServerAdminTools.ServerEditRow?>>? readServer = null,
        bool allowWindowsAuthOverUnverifiedTls = false)
    {
        var slotTimeout = addTimeout ?? ServerAddSlotTimeout;
        /* web.allowWindowsAuthOverUnverifiedTls: the add and the edit refuse Windows authentication over a connection whose server
           identity is not verified unless it is on, the rule MCP applies under mcp.allowWindowsAuthOverUnverifiedTls. */
        var windowsAuthScreen = DarlingUnverifiedWindowsAuthScreen.ForWeb(allowWindowsAuthOverUnverifiedTls);
        var editSlotTimeout = editTimeout ?? ServerEditSlotTimeout;

        /* One gate per host: MapAll runs once per process. ADD and EDIT share it (#5240): both probe over the network
           and both read-then-write the same identity set, so one server write runs at a time per host process. */
        var serverWriteInFlight = new SemaphoreSlim(1, 1);

        /* The seam a test stands a stub probe or a held add in through; production runs the core itself. */
        addServers ??= body => DarlingMcpServerAdminTools.AddServers(postgres, body, logger, windowsAuthScreen);

        /* The edit core and the by-id read, with the same seam. The edit core gets NO audit logger: the route writes the one
           audit line itself, with the signed-in principal. It gets the logger for a failed connection test's driver
           text alone (probeLogger), which the reply to the browser does not carry. */
        editServer ??= (id, body) => DarlingMcpServerAdminTools.EditServerByIdAsync(postgres, id, body, null, CancellationToken.None, probeLogger: logger, webScreen: windowsAuthScreen);
        readServer ??= id => new DarlingMcpServerAdminTools.PostgresServerEditStore(postgres).ReadRowAsync(id, CancellationToken.None);

        app.MapPost("/api/servers", async (HttpContext context) =>
        {
            if (!DarlingWebSeat.FromContext(context).CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxServerAddBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            if (!TryReadServerAddBody(body, out var entries, out var refusal))
            {
                return ErrorResult(refusal!, StatusCodes.Status400BadRequest);
            }

            /* Both are read BEFORE the slot call: an add that outlives the wait commits after the request is over, and
               its audit line is written then, from these two, never from the HttpContext. */
            var principal = DarlingWebSeat.FromContext(context).EditorPrincipal;
            var secrets = SubmittedSecrets(entries);

            var ran = await RunInServerWriteSlotAsync(
                serverWriteInFlight, () => addServers(body), slotTimeout, AddSlotLabels, logger,
                auditLateAnswer: late =>
                {
                    var lateAnswer = RedactAddAnswer(late, secrets);
                    LogServerAddFailures(logger, principal, lateAnswer);
                    LogServerAdds(logger, principal, entries, lateAnswer);
                });
            if (ran.Early is not null)
            {
                return ran.Early;
            }

            var result = ran.Answer!;
            var stopwatch = ran.Stopwatch!;

            if (ClassifyToolResponse(result) is ToolResponseKind.ServerError)
            {
                return ServerErrorResult(RedactSecrets(McpHelpers.ErrorMessageOf(result), secrets), "/api/servers", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            var answer = RedactAddAnswer(result, secrets);
            LogServerAddFailures(logger, principal, answer);
            LogServerAdds(logger, principal, entries, answer);
            return Results.Text(answer, "application/json", statusCode: MuteRuleEnvelopeStatus(answer));
        });

        /* #5240: edit one server in place. Gates in add's order: the sign-in's edit right (403), JSON content type (415), a bounded
           body (400), a JSON object without duplicate keys (400), the token (400), then the shared slot (429) and the
           core with a timeout (503). The host's group-level method gate has already refused a read-only sign-in
           every unsafe method. */
        app.MapPatch("/api/servers/{id:int}", async (HttpContext context, int id) =>
        {
            if (!DarlingWebSeat.FromContext(context).CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxServerEditBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            if (!TryReadServerEditBody(body, out var changes, out var refusal))
            {
                return ErrorResult(refusal!, StatusCodes.Status400BadRequest);
            }

            /* Both are read BEFORE the slot call: an edit that outlives the wait commits after the request is over, and
               its audit line is written then, from these two, never from the HttpContext. */
            var principal = DarlingWebSeat.FromContext(context).EditorPrincipal;
            var secrets = new List<string>();
            if (TryGetString(changes, "password") is { Length: > 0 } secret)
            {
                secrets.Add(secret);
            }

            /* #5452: the typed AWS external ID is redacted wherever a password is. */
            if (TryGetString(changes, "aws_external_id") is { Length: > 0 } externalId)
            {
                secrets.Add(externalId);
                if (externalId.Trim() is { Length: > 0 } trimmedExternalId && trimmedExternalId != externalId)
                {
                    secrets.Add(trimmedExternalId);
                }
            }

            var ran = await RunInServerWriteSlotAsync(
                serverWriteInFlight, () => editServer(id, body), editSlotTimeout, EditSlotLabels, logger,
                auditLateAnswer: late => LogServerEdit(logger, principal, id, RedactEditAnswer(late, secrets)));
            if (ran.Early is not null)
            {
                return ran.Early;
            }

            var result = ran.Answer!;
            if (ClassifyToolResponse(result) is ToolResponseKind.ServerError)
            {
                var sentence = RedactSecrets(McpHelpers.ErrorMessageOf(result), secrets);
                if (sentence.Contains(DarlingMcpServerAdminTools.EditStoreNeedsRolesText, StringComparison.Ordinal))
                {
                    /* A store whose roles predate the edit function (#5240): still a 500, but the body says what to do
                       instead of the generic sentence. The text is ours; PostgreSQL's own error never reaches this far. */
                    DarlingWebFailureLog.Report(logger, "/api/servers/{id}", ran.Stopwatch!.ElapsedMilliseconds, sentence, context.RequestAborted);
                    return ErrorResult(DarlingMcpServerAdminTools.EditStoreNeedsRolesText, StatusCodes.Status500InternalServerError);
                }

                return ServerErrorResult(sentence, "/api/servers/{id}", logger, ran.Stopwatch!.ElapsedMilliseconds, context.RequestAborted);
            }

            var answer = RedactEditAnswer(result, secrets);
            LogServerEdit(logger, principal, id, answer);
            return Results.Text(answer, "application/json", statusCode: ServerEditEnvelopeStatus(answer));
        });

        /* The edit form's read (#5240): the editable NON-secret values plus modified_at, an opaque string. Web-only
           (no MCP tool). It cannot say whether a secret is stored: the viewer role cannot even evaluate
           encrypted_password. The host's group gate lets every GET through for a read-only seat, so the route
           checks the edit right itself. */
        app.MapGet("/api/admin/servers/{id:int}", async (HttpContext context, int id) =>
        {
            /* The form's read pre-fills username and the TLS posture, which the Manage Servers list
               (DarlingAdminServersReader) withholds from every seat. Only a seat that can submit the edit may read it. */
            if (!DarlingWebSeat.FromContext(context).CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            var stopwatch = Stopwatch.StartNew();
            DarlingMcpServerAdminTools.ServerEditRow? row;
            try
            {
                row = await readServer(id);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"admin server read failed ({ex.GetType().Name})", "/api/admin/servers/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (row is null)
            {
                return ErrorResult("This server's definition no longer exists.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            return Results.Text(JsonSerializer.Serialize(DarlingMcpServerAdminTools.CurrentValuesOf(row), McpHelpers.JsonOptions), "application/json");
        });
    }

    /// <summary>The most request-body bytes the edit route reads (a partial change of a dozen scalar fields).</summary>
    internal const int MaxServerEditBodyBytes = 8 * 1024;

    /// <summary>
    /// PURE: parses the edit body, or names why it is not acceptable with a fixed sentence that quotes no part of the
    /// body. It must be a JSON object with no duplicate key, and the web route ALWAYS requires
    /// <c>expected_modified_at</c> (a non-empty string): a form that did not read the row first cannot edit it.
    /// </summary>
    internal static bool TryReadServerEditBody(string body, out JsonObject changes, out string? refusal)
    {
        changes = new JsonObject();
        refusal = null;
        try
        {
            if (JsonNode.Parse(body) is not JsonObject parsed)
            {
                refusal = "Request body must be a JSON object.";
                return false;
            }

            /* A duplicate property name throws ArgumentException on the first enumeration; force it here. */
            _ = parsed.Count;
            if (TryGetString(parsed, "expected_modified_at") is not { Length: > 0 })
            {
                refusal = "expected_modified_at is required: send the modified_at the edit form read.";
                return false;
            }

            changes = parsed;
            return true;
        }
        catch (JsonException)
        {
            refusal = "Request body is not valid JSON.";
            return false;
        }
        catch (ArgumentException)
        {
            refusal = "Request body has a duplicate field.";
            return false;
        }
    }

    /// <summary>
    /// The HTTP status an edit answer maps to: <see cref="ServerTagEnvelopeStatus"/> plus <c>collides</c> as 409, and
    /// <c>connection_failed</c> as 200 with the status in the body (add answers a failed probe the same way), so the
    /// page branches on <c>status</c> and no new convention is invented. Pure.
    /// </summary>
    internal static int ServerEditEnvelopeStatus(string result)
    {
        if (ClassifyToolResponse(result) is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal)
        {
            try
            {
                switch (JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null)
                {
                    case "collides":
                        return StatusCodes.Status409Conflict;
                    case "connection_failed":
                        return StatusCodes.Status200OK;
                }
            }
            catch (JsonException)
            {
                /* Not a shape the core produces; the tag mapping below answers it. */
            }
        }

        return ServerTagEnvelopeStatus(result);
    }

    /// <summary>The edit answer with every occurrence of the submitted secret removed from its one free-text field,
    /// <c>message</c> (the only field a driver's text can reach: connection_failed, collides). Structured fields
    /// (engine, username, host, server, modified_at, numbers, keys) are never rewritten, so a short or common secret
    /// cannot corrupt them. An answer that does not parse is replaced by a fixed body, never passed through.</summary>
    internal static string RedactEditAnswer(string answer, IReadOnlyList<string> secrets)
    {
        if (secrets.Count == 0)
        {
            return answer;
        }

        try
        {
            if (JsonNode.Parse(answer) is not JsonObject envelope)
            {
                return RedactSecrets(answer, secrets);
            }

            RedactField(envelope, "message", secrets);
            return envelope.ToJsonString();
        }
        catch (JsonException)
        {
            return "{\"status\":\"error\",\"message\":\"The answer could not be read.\"}";
        }
    }

    /// <summary>One Information line per saved edit: who, the server id and the field NAMES. Never a value, never the
    /// secret. A refused probe gets a line with the core's sanitized detail (redacted already).</summary>
    private static void LogServerEdit(ILogger logger, string principal, int id, string answer)
    {
        try
        {
            if (JsonNode.Parse(answer) is not JsonObject envelope)
            {
                return;
            }

            switch (TryGetString(envelope, "status"))
            {
                case "updated":
                    var fields = envelope["changed"] is JsonArray changed
                        ? string.Join(",", changed.Select(n => n is JsonValue v && v.TryGetValue<string>(out var f) ? f : "?"))
                        : "";
                    logger.LogInformation(
                        "Server edited by {Principal}: id {ServerId}, fields {Fields}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256), id, DarlingHttpRefusalLog.Sanitize(fields, 256));
                    if (fields.Split(',').Contains("aws_role_arn", StringComparer.Ordinal))
                    {
                        /* #5452: old and new role ARN with the principal. The external ID is never in this line. */
                        logger.LogInformation(
                            "AWS role changed by {Principal}: id {ServerId}, from {OldRole} to {NewRole}",
                            DarlingHttpRefusalLog.Sanitize(principal, 256), id,
                            DarlingHttpRefusalLog.Sanitize(TryGetString(envelope, "old_aws_role_arn") ?? "(none)", 2200),
                            DarlingHttpRefusalLog.Sanitize(TryGetString(envelope, "new_aws_role_arn") ?? "(none)", 2200));
                    }
                    break;
                case "connection_failed":
                    logger.LogInformation(
                        "Server edit failed for {Principal}: id {ServerId}, connection_failed: {Detail}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256), id, DarlingHttpRefusalLog.Sanitize(TryGetString(envelope, "message") ?? "", 1024));
                    break;
            }
        }
        catch (JsonException)
        {
            /* An unreadable answer has nothing to log. */
        }
    }

    /// <summary>What <see cref="RunInServerWriteSlotAsync"/> says for one server-write route.</summary>
    internal sealed record ServerWriteSlotLabels(string BusyText, string TimedOutText, string ToolName, string Route, string LogVerb);

    internal static readonly ServerWriteSlotLabels AddSlotLabels = new(
        "A server add or edit is already running. Wait for it to finish, then try again.",
        ServerAddTimedOutText, "add_servers", "/api/servers", "POST /api/servers: the add");

    internal static readonly ServerWriteSlotLabels EditSlotLabels = new(
        "A server add or edit is already running. Wait for it to finish, then try again.",
        ServerEditTimedOutText, "edit_server", "/api/servers/{id}", "PATCH /api/servers/{id}: the edit");

    /// <summary>How long one <c>PATCH /api/servers/{id}</c> request may hold the slot (one probe, not twenty).</summary>
    internal static readonly TimeSpan ServerEditSlotTimeout = TimeSpan.FromSeconds(60);

    internal const string ServerEditTimedOutText = "Editing the server took too long; check the server's settings before retrying. If every later add or edit is refused as busy, restart the service.";

    /// <summary>The outcome of <see cref="RunInServerWriteSlotAsync"/>: either an <see cref="Early"/> answer (busy,
    /// timed out, or faulted) or the core's <see cref="Answer"/> and how long it ran.</summary>
    internal sealed record ServerWriteRun(IResult? Early, string? Answer, Stopwatch? Stopwatch);

    /// <summary>
    /// The ONE place the server-write slot is taken, held and freed (#5268's fix, shared by add and edit so the next
    /// server-write route cannot reintroduce the race). <c>Wait(0)</c> or 429; the core starts; two exits can free the
    /// slot, the handler when the core finished inside the wait and the core's own continuation when it outlives the
    /// wait, and both go through one <see cref="SingleReleaseSlot"/> so it is freed at most once; the handler frees it
    /// in <c>finally</c> BEFORE the caller writes any answer, so a client that sends again the moment it reads the
    /// answer finds the slot free. A core that outlives <paramref name="timeout"/> answers 503 and keeps the slot
    /// until it finishes, and a core that outlives it can still commit: <paramref name="auditLateAnswer"/> receives
    /// its answer when it finishes, so the route's audit line is written for a write the client was told nothing
    /// about. It runs after the request is over, so it must not touch the HttpContext. Only the exception TYPE is
    /// ever logged or answered: its message could quote a value the request carried.
    /// </summary>
    private static async Task<ServerWriteRun> RunInServerWriteSlotAsync(
        SemaphoreSlim gate, Func<Task<string>> start, TimeSpan timeout, ServerWriteSlotLabels labels, ILogger logger,
        Action<string>? auditLateAnswer = null)
    {
        if (!gate.Wait(0))
        {
            return new ServerWriteRun(ErrorResult(labels.BusyText, StatusCodes.Status429TooManyRequests), null, null);
        }

        var stopwatch = Stopwatch.StartNew();
        var slot = new SingleReleaseSlot(gate);

        /* The core takes no request token, like the tool: a client that disconnects mid-write must not leave it
           half-attempted and unaudited. */
        Task<string> running;
        try
        {
            running = start();
        }
        catch (Exception ex)
        {
            /* start threw before returning a task: nothing is running, so free the slot here. EVERY synchronous throw
               is caught, cancellation included: the slot is not released in a finally, so a type that escaped this
               catch would hold it for the life of the process and answer 429 forever. */
            slot.Release();
            return new ServerWriteRun(
                ServerErrorResult($"{labels.ToolName} failed ({ex.GetType().Name})", labels.Route, logger, stopwatch.ElapsedMilliseconds, CancellationToken.None), null, null);
        }

        /* The slot is freed by the core itself, once, when it finishes (completed or faulted) - never by the wait's
           timeout, so a core that outlives the timeout still holds the slot. */
        _ = running.ContinueWith(_ => slot.Release(), TaskScheduler.Default);

        try
        {
            var answer = await running.WaitAsync(timeout);
            return new ServerWriteRun(null, answer, stopwatch);
        }
        catch (TimeoutException)
        {
            _ = running.ContinueWith(t => _ = t.Exception, TaskContinuationOptions.OnlyOnFaulted);
            if (auditLateAnswer is not null)
            {
                /* The core still commits after the 503: write its audit line when it finishes. Never touch HttpContext here. */
                _ = running.ContinueWith(
                    t => auditLateAnswer(t.Result), CancellationToken.None, TaskContinuationOptions.OnlyOnRanToCompletion, TaskScheduler.Default);
            }

            logger.LogWarning("{Verb} did not finish within {Seconds} s; the slot stays held until it does", labels.LogVerb, (int)timeout.TotalSeconds);
            return new ServerWriteRun(ErrorResult(labels.TimedOutText, StatusCodes.Status503ServiceUnavailable), null, null);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The cores catch their own faults, so this is a backstop; only the TYPE is logged. */
            return new ServerWriteRun(
                ServerErrorResult($"{labels.ToolName} failed ({ex.GetType().Name})", labels.Route, logger, stopwatch.ElapsedMilliseconds, CancellationToken.None), null, null);
        }
        finally
        {
            /* A core that finished inside the wait frees the slot here, before any response is produced. One still
               running (the timeout path) keeps it: only its own continuation frees it. */
            if (running.IsCompleted)
            {
                slot.Release();
            }
        }
    }

    /// <summary>
    /// Frees one acquired slot of a one-slot gate at most once, however many exits call <see cref="Release"/> and in
    /// whatever order. A plain second <c>Release</c> is not harmless: if another add has already taken the slot in
    /// between, the extra release frees the slot that add holds, and a third add runs beside it.
    /// </summary>
    internal sealed class SingleReleaseSlot
    {
        private readonly SemaphoreSlim gate;
        private readonly object sync = new();
        private readonly Action? beforeFree;
        private bool released;

        /// <param name="gate">The one-slot gate to free.</param>
        /// <param name="beforeFree">A test seam only: runs inside the first release, after it claimed the slot and before
        /// it frees the gate, so a test can stand a second caller in that window. Production passes nothing.</param>
        internal SingleReleaseSlot(SemaphoreSlim gate, Action? beforeFree = null)
        {
            this.gate = gate;
            this.beforeFree = beforeFree;
        }

        /// <summary>
        /// Frees the slot on the first call; every later call does nothing, but it does not RETURN until the first
        /// call has really freed the gate (#6440). A flag flipped with Interlocked.Exchange before gate.Release() let a
        /// second caller see "already released" and return while the first caller, preempted between the flag and
        /// the Release, had not freed the gate yet: the handler then sent its answer, the client sent its next
        /// edit, and that edit was refused with 429. The lock makes the second caller wait that moment out. Nothing
        /// inside it blocks or awaits.
        /// </summary>
        internal void Release()
        {
            lock (sync)
            {
                if (released)
                {
                    return;
                }

                released = true;
                beforeFree?.Invoke();
                gate.Release();
            }
        }
    }

    /// <summary>
    /// PURE: parses the add body into its entries. False with a fixed, caller-facing <paramref name="refusal"/>
    /// when the body is not a JSON array of 1 to <see cref="MaxServersPerAddRequest"/> objects. The refusal names
    /// no part of the body.
    /// </summary>
    internal static bool TryReadServerAddBody(string body, out List<JsonObject> entries, out string? refusal)
    {
        entries = new List<JsonObject>();
        refusal = null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(body);
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
            refusal = "Request body is not valid JSON.";
            return false;
        }

        if (root is not JsonArray array)
        {
            refusal = "Request body must be a JSON array of server objects.";
            return false;
        }

        if (array.Count == 0)
        {
            refusal = "Request body must hold at least one server object.";
            return false;
        }

        if (array.Count > MaxServersPerAddRequest)
        {
            refusal = $"A request may add at most {MaxServersPerAddRequest} servers; send the rest in another request.";
            return false;
        }

        foreach (var element in array)
        {
            if (element is not JsonObject entry)
            {
                refusal = "Every entry must be a JSON object.";
                return false;
            }

            try
            {
                /* A duplicate property name throws ArgumentException on the first enumeration; force it here so it
                   answers 400, like the dismiss route. */
                _ = entry.Count;
            }
            catch (ArgumentException)
            {
                refusal = "An entry has a duplicate field.";
                return false;
            }

            entries.Add(entry);
        }

        return true;
    }

    /// <summary>The non-empty <c>password</c> values the request carried (a SQL password or a service-principal client secret),
    /// and the typed <c>aws_external_id</c> (#5452), redacted the same way.</summary>
    private static List<string> SubmittedSecrets(List<JsonObject> entries)
    {
        var secrets = new List<string>();
        foreach (var entry in entries)
        {
            if (TryGetString(entry, "password") is { Length: > 0 } secret)
            {
                secrets.Add(secret);
            }

            if (TryGetString(entry, "aws_external_id") is { Length: > 0 } externalId)
            {
                secrets.Add(externalId);
                if (externalId.Trim() is { Length: > 0 } trimmedExternalId && trimmedExternalId != externalId)
                {
                    secrets.Add(trimmedExternalId);
                }
            }
        }

        return secrets;
    }

    /// <summary>PURE: <paramref name="text"/> with every occurrence of every secret replaced.</summary>
    internal static string RedactSecrets(string text, IReadOnlyList<string> secrets)
    {
        foreach (var secret in secrets)
        {
            text = text.Replace(secret, RedactedSecret, StringComparison.Ordinal);
        }

        return text;
    }

    /// <summary>The core's envelope with any submitted secret removed from its free-text fields (<c>detail</c>,
    /// <c>message</c>, <c>server</c>). The envelope keeps its shape; an envelope that does not parse is replaced
    /// by a fixed refusal rather than passed through unredacted.</summary>
    internal static string RedactAddAnswer(string answer, IReadOnlyList<string> secrets)
    {
        if (secrets.Count == 0)
        {
            return answer;
        }

        try
        {
            if (JsonNode.Parse(answer) is not JsonObject envelope)
            {
                return RedactSecrets(answer, secrets);
            }

            RedactField(envelope, "message", secrets);
            if (envelope["results"] is JsonArray results)
            {
                foreach (var row in results.OfType<JsonObject>())
                {
                    RedactField(row, "detail", secrets);
                    RedactField(row, "server", secrets);
                }
            }

            return envelope.ToJsonString();
        }
        catch (JsonException)
        {
            return "{\"status\":\"error\",\"message\":\"The answer could not be read.\"}";
        }
    }

    private static void RedactField(JsonObject node, string key, IReadOnlyList<string> secrets)
    {
        if (node[key] is JsonValue value && value.TryGetValue<string>(out var text))
        {
            node[key] = RedactSecrets(text, secrets);
        }
    }

    /// <summary>The auth mode an add entry names, in the words the tool documents; absent means Windows.</summary>
    private static string AuthModeOf(JsonObject entry) => TryGetString(entry, "auth")?.Trim().ToLowerInvariant() switch
    {
        null or "" or "windows" => "Windows",
        "sql" => "SQL",
        "serviceprincipal" => "ServicePrincipal",
        "managedidentity" => "ManagedIdentity",
        _ => "other",
    };

    /// <summary>One Information line per failed row with the core's full detail (secrets already redacted), so the
    /// operator keeps what the web answer no longer shows.</summary>
    private static void LogServerAddFailures(ILogger logger, string principal, string answer)
    {
        try
        {
            if (JsonNode.Parse(answer) is JsonObject envelope && envelope["results"] is JsonArray results)
            {
                foreach (var row in results.OfType<JsonObject>())
                {
                    var status = TryGetString(row, "status");
                    if (status != DarlingMcpServerAdminTools.AddStatus.ConnectionFailed && status != DarlingMcpServerAdminTools.AddStatus.NotSaved)
                    {
                        continue;
                    }

                    logger.LogInformation(
                        "Server add failed for {Principal}: {Server}, {Status}: {Detail}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256),
                        DarlingHttpRefusalLog.Sanitize(TryGetString(row, "server") ?? "", 256),
                        status,
                        DarlingHttpRefusalLog.Sanitize(TryGetString(row, "detail") ?? "", 1024));
                }
            }
        }
        catch (JsonException)
        {
            /* An unreadable answer has no rows to log. */
        }
    }

    /// <summary>One Information line per server this request ADDED: who, which server and its auth mode. Request
    /// text is sanitized before it reaches the log; the secret is never read here.</summary>
    private static void LogServerAdds(ILogger logger, string principal, List<JsonObject> entries, string answer)
    {
        var authByName = new Dictionary<string, string>(StringComparer.Ordinal);
        var roleByName = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            var host = TryGetString(entry, "host")?.Trim();
            var name = TryGetString(entry, "display_name") is { Length: > 0 } displayName ? displayName.Trim() : host;
            if (!string.IsNullOrEmpty(name))
            {
                authByName.TryAdd(name, AuthModeOf(entry));
                if (TryGetString(entry, "aws_role_arn")?.Trim() is { Length: > 0 } awsRole)
                {
                    roleByName.TryAdd(name, awsRole);
                }
            }
        }

        try
        {
            if (JsonNode.Parse(answer) is JsonObject { } envelope && envelope["results"] is JsonArray results)
            {
                foreach (var row in results.OfType<JsonObject>())
                {
                    if (TryGetString(row, "status") != DarlingMcpServerAdminTools.AddStatus.Added)
                    {
                        continue;
                    }

                    var server = TryGetString(row, "server") ?? "";
                    logger.LogInformation(
                        "Server added by {Principal}: {Server}, auth {AuthMode}",
                        DarlingHttpRefusalLog.Sanitize(principal, 256), DarlingHttpRefusalLog.Sanitize(server, 256), authByName.GetValueOrDefault(server, "other"));
                    if (roleByName.TryGetValue(server, out var addedRole))
                    {
                        /* #5452: the role an add set, with the principal. The external ID is never logged. */
                        logger.LogInformation(
                            "AWS role set by {Principal} on add: {Server}, role {NewRole}",
                            DarlingHttpRefusalLog.Sanitize(principal, 256), DarlingHttpRefusalLog.Sanitize(server, 256), DarlingHttpRefusalLog.Sanitize(addedRole, 2200));
                    }
                }
            }
        }
        catch (JsonException)
        {
            /* The answer was written by the core; an unreadable one has no added rows to audit. */
        }
    }

    /// <summary>The most alert keys one dismiss request may carry.</summary>
    internal const int MaxDismissKeys = 1000;

    /// <summary>The most request-body bytes the dismiss route reads before it answers 400.</summary>
    internal const int MaxDismissBodyBytes = 256 * 1024;

    /// <summary>The longest metric name a dismiss key may carry; real metric names are far shorter.</summary>
    internal const int MaxDismissMetricNameLength = 512;

    /// <summary>
    /// <c>POST /api/alert-history/dismiss</c>: marks the listed Alert History rows dismissed (the web twin of the
    /// Viewer's Dismiss Selected; Dismiss All is the client sending every visible key, so there is no
    /// dismiss-everything verb). The body is <c>{"alerts":[{"alert_time":"…","server_id":1,"metric_name":"…"}]}</c>,
    /// the identity the Viewer's dismiss keys on, taken from the row <c>get_alert_history</c> returned.
    /// <c>application/json</c> is required (415 otherwise, the CSRF defense the mute routes use); a read-only
    /// seat is refused by the host's group-level write gate before this handler runs
    /// (<see cref="Hosting.DarlingWebSeat.IsRequestAllowed"/> allows only GET/HEAD/OPTIONS for it). The write
    /// runs on the host's <c>viewer</c> pool, whose only privilege here is the column-level
    /// <c>UPDATE (dismissed)</c> on <c>config_alert_log</c>. Keys are deduped; a key that names no alert row is
    /// counted in <c>unknown</c>, not an error. 200 answers <c>{requested, dismissed, already_dismissed, unknown}</c>.
    /// </summary>
    private static void MapAlertHistoryDismiss(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        app.MapPost("/api/alert-history/dismiss", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxDismissBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var parsed = ParseDismissBody(body, out var keys, out var refusal);
            if (!parsed)
            {
                return ErrorResult(refusal!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                var outcome = await AlertDismissStore.DismissKeysAsync(
                    postgres, keys, McpCommandDeadlines.ReadSeconds, context.RequestAborted);

                /* Who dismissed is not stored on the row; this line is the only record. Counts only, no alert text. */
                logger.LogInformation(
                    "Alert dismiss by {Principal}: requested {Requested}, dismissed {Dismissed}, known {Known}",
                    DarlingWebSeat.FromContext(context).EditorPrincipal, outcome.Requested, outcome.Dismissed, outcome.Known);

                return JsonNodeResult(new JsonObject
                {
                    ["requested"] = outcome.Requested,
                    ["dismissed"] = outcome.Dismissed,
                    ["already_dismissed"] = Math.Max(0, outcome.Known - outcome.Dismissed),
                    ["unknown"] = Math.Max(0, outcome.Requested - outcome.Known),
                });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-history/dismiss", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /* ─────────────────────── #5913 feedback on investigation reports ─────────────────────── */

    /// <summary>The longest feedback request body the route reads (two 4000-character fields plus the rest, with room for escapes).</summary>
    internal const int MaxFeedbackBodyBytes = 64 * 1024;

    /// <summary>The most reported requests one feedback list answers (the Viewer's list holds the same number).</summary>
    internal const int FeedbackListLimit = 200;

    /// <summary>The 403 sentence a feedback record answers when the store refuses the write (SQLSTATE 42501): the web host runs on
    /// whatever login <c>postgres.webConnectionString</c> names, and recording feedback is an <c>admin</c> role write (#5913).</summary>
    internal static string FeedbackRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("record feedback", managedStore, "Give feedback in the Viewer app", adminWritesOff: adminWritesOff);

    /* ─────────────────────── #5918 practice scores ─────────────────────── */

    /// <summary>
    /// The web's list of practice scores (#5918, #5632 Phase B): <c>GET /api/investigation-practice-scores</c> answers the score per agent
    /// over the practice requests opened in the last <c>hours_back</c> hours (default 720, at most 8760): <c>{hours_back, agents: [{agent_id,
    /// cases, scored, matched, unscored, points, possible, percent, missed_step_cases}]}</c>, agents in name order, <c>percent</c> null for an
    /// agent with nothing scored yet. This route only reads: the score is advisory, so nothing here decides anything or writes
    /// (a practice request is started by <see cref="MapInvestigationPersonWrites"/>).
    /// </summary>
    internal static void MapInvestigationPractice(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new InvestigationPracticeStore(postgres, McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/investigation-practice-scores", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var hours = 720;
            if (context.Request.Query.TryGetValue("hours_back", out var hoursText)
                && (!int.TryParse(hoursText.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out hours) || hours < 1 || hours > 8760))
            {
                return ErrorResult("hours_back must be a whole number from 1 to 8760.", StatusCodes.Status400BadRequest);
            }

            try
            {
                var scores = await store.ReadScoresAsync(DateTime.UtcNow.AddHours(-hours), context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    PracticeScoresBody(scores, hours), "/api/investigation-practice-scores", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-practice-scores", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /* ─────────────────────── #6265 close a request, write a note, start a practice request ─────────────────────── */

    /// <summary>The 403 sentence these three writes answer when the store refuses them for privilege (SQLSTATE 42501): the web host runs on
    /// whatever login <c>postgres.webConnectionString</c> names, the <c>viewer</c> role on a managed or compose store (#6265).</summary>
    internal static string InvestigationWriteRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("close a request, write a note or start a practice request", managedStore, "Do it in the Viewer app", adminWritesOff: adminWritesOff);

    /// <summary>The most bytes one of these write bodies may carry (a note is at most 2,000 characters).</summary>
    internal const int MaxInvestigationWriteBodyBytes = 64 * 1024;

    /// <summary>
    /// The three investigation writes a person makes (#6265, #5632), the web's twins of the desktop Viewer's: <c>PUT /api/investigation-requests/{id}/close</c>
    /// (body <c>{reason}</c>) closes an open, claimed or reported request; <c>PUT /api/investigation-notes</c> (body <c>{note_text, server_id?, metric_name?, days?}</c>)
    /// writes a note a person stands behind, approved as written (an agent's proposed note still waits for Approve); <c>PUT /api/investigation-practice-requests/{id}</c>
    /// opens a practice request replaying one judged case. They call the same storage code the Viewer does
    /// (<see cref="InvestigationRequestStore.CloseRequestAsync"/>, <see cref="InvestigationRequestStore.WriteNoteAsync"/>,
    /// <see cref="InvestigationPracticeStore.OpenAsync"/>), so the states, the refusals and the journal rows are the same: <c>closed</c>,
    /// <c>note_written</c> and <c>opened</c>, each by <c>person:&lt;signed-in name&gt;</c>. Born gated as the other web writes are: <c>application/json</c>
    /// required, a read-only seat refused by the host's write gate, a store login without the admin role answered 403 with
    /// <see cref="InvestigationWriteRefusedMessage"/>. Each route needs a signed-in admin
    /// (<see cref="DarlingWebSeat.IsSignedInAdmin"/>): a dashboard with no sign-in configured has no caller to name, so it answers 403 with
    /// <see cref="InvestigationWriteSignInMessage"/> and writes nothing. An agent's MCP token is not a web sign-in either.
    /// 400 for a value the rules refuse (in plain words), 404 for an unknown request or case, 409 for a request that cannot be closed or a case that
    /// cannot be replayed, a fixed-sentence 500 or 503 for any other store failure.
    /// </summary>
    internal static void MapInvestigationPersonWrites(WebApplication app, NpgsqlDataSource postgres, ILogger logger, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        var requests = new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);

        /* #6600: the three writes run as the login the seat's admin writes use; the registered-server check stays on the web's own pool. */
        InvestigationRequestStore WriteRequests(HttpContext context) =>
            adminArm is null ? requests : new InvestigationRequestStore(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);
        InvestigationPracticeStore WritePractice(HttpContext context) =>
            new(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapPut("/api/investigation-requests/{requestId}/close", async (HttpContext context, string requestId) =>
        {
            const string route = "/api/investigation-requests/{requestId}/close";
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            if (!Guid.TryParse(requestId, out var request))
            {
                return ErrorResult("The request id is not a request id.", StatusCodes.Status400BadRequest);
            }

            var (body, bodyRefusal) = await ReadInvestigationWriteBodyAsync(context);
            if (body is null)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = bodyRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            var reason = body["reason"] is JsonValue reasonValue && reasonValue.TryGetValue<string>(out var reasonText) ? reasonText : null;
            if (InvestigationPersonWrites.ValidateCloseReason(reason) is { } reasonRefusal)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = reasonRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            if (PersonWriteActor(context) is not { } actor)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = PersonWriteActorRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                if (!await WriteRequests(context).CloseRequestAsync(request, actor, reason!, context.RequestAborted))
                {
                    return ErrorResult("That request is not open: it is already closed or expired, or it is gone.", StatusCodes.Status409Conflict);
                }

                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["status"] = "closed", ["request_id"] = request.ToString(), ["closed_by"] = actor },
                    route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationWriteRefusedMessage(managedStore, adminWritesOff) }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-notes", async (HttpContext context) =>
        {
            const string route = "/api/investigation-notes";
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var (body, bodyRefusal) = await ReadInvestigationWriteBodyAsync(context);
            if (body is null)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = bodyRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            var text = body["note_text"] is JsonValue textValue && textValue.TryGetValue<string>(out var noteText) ? noteText : null;
            var metric = body["metric_name"] is JsonValue metricValue && metricValue.TryGetValue<string>(out var metricText) ? metricText : null;
            int? serverId = null;
            if (body["server_id"] is { } serverNode)
            {
                if (serverNode is not JsonValue serverValue || !serverValue.TryGetValue<int>(out var id) || id < 1)
                {
                    return ErrorResult("server_id must be a whole number, or left out for a fleet note.", StatusCodes.Status400BadRequest);
                }

                serverId = id;
            }

            var days = InvestigationPersonWrites.NoteDefaultDays;
            if (body["days"] is { } daysNode && (daysNode is not JsonValue daysValue || !daysValue.TryGetValue<int>(out days)))
            {
                return ErrorResult("days must be a whole number.", StatusCodes.Status400BadRequest);
            }

            if (InvestigationPersonWrites.ValidateNote(text, days, metric) is { } noteRefusal)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = noteRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            if (PersonWriteActor(context) is not { } actor)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = PersonWriteActorRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                if (serverId is { } named && !await requests.ServerRegisteredAsync(named, context.RequestAborted))
                {
                    return ErrorResult("That server is not registered.", StatusCodes.Status404NotFound);
                }

                var noteId = await WriteRequests(context).WriteNoteAsync(serverId, metric, text!, actor, days, context.RequestAborted);
                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["status"] = "approved", ["note_id"] = noteId, ["author"] = actor, ["server_id"] = serverId, ["days"] = days },
                    route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationWriteRefusedMessage(managedStore, adminWritesOff) }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-practice-requests/{requestId}", async (HttpContext context, string requestId) =>
        {
            const string route = "/api/investigation-practice-requests/{requestId}";
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return SignInRequiredResult();
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            if (!Guid.TryParse(requestId, out var source))
            {
                return ErrorResult("The request id is not a request id.", StatusCodes.Status400BadRequest);
            }

            if (PersonWriteActor(context) is not { } actor)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = PersonWriteActorRefusal }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                var (outcome, practiceId, storeSentence) = await WritePractice(context).OpenAsync(source, actor, context.RequestAborted);
                if (outcome != InvestigationPracticeOutcome.Opened)
                {
                    /* The store's own sentence says why the case was not replayed (never an exception's text). */
                    return DarlingWebStatementSweep.JsonText(
                        new JsonObject { ["error"] = storeSentence ?? "The practice request was not opened.", ["outcome"] = outcome.ToString(), ["request_id"] = practiceId?.ToString() },
                        route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted,
                        outcome == InvestigationPracticeOutcome.NotFound ? StatusCodes.Status404NotFound : StatusCodes.Status409Conflict);
                }

                return DarlingWebStatementSweep.JsonText(
                    new JsonObject { ["status"] = "opened", ["request_id"] = practiceId?.ToString(), ["source_request_id"] = source.ToString(), ["opened_by"] = actor },
                    route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsInsufficientPrivilege(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationWriteRefusedMessage(managedStore, adminWritesOff) }, route, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, route, stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The journal actor of a person's investigation write: <c>person:&lt;signed-in name&gt;</c> (the seat's
    /// <see cref="DarlingWebSeat.EditorPrincipal"/>), or null when the built actor is not one the journal may hold: a name the undo path
    /// refuses (invisible, format or bidi characters, whitespace, over 64 characters) or an actor longer than 128.</summary>
    private static string? PersonWriteActor(HttpContext context)
    {
        var actor = InvestigationPersonWrites.PersonActor(DarlingWebSeat.FromContext(context).EditorPrincipal);
        return InvestigationPersonWrites.ValidateActor(actor) is null ? actor : null;
    }

    private const string PersonWriteActorRefusal = "The signed-in name cannot be recorded as the person who made this change.";

    /// <summary>The 400 a person-made change answers when the signed-in name cannot be recorded: the fixed <see cref="PersonWriteActorRefusal"/> sentence, never the name.</summary>
    private static IResult PersonActorRefusedResult() => ErrorResult(PersonWriteActorRefusal, StatusCodes.Status400BadRequest);

    /// <summary>Reads a bounded JSON object body for the investigation writes: the object, or the plain sentence that refuses it.</summary>
    private static async Task<(JsonObject? Body, string? Refusal)> ReadInvestigationWriteBodyAsync(HttpContext context)
    {
        string text;
        try
        {
            text = await ReadBoundedBodyAsync(context, MaxInvestigationWriteBodyBytes);
        }
        catch (InvalidDataException)
        {
            return (null, "Request body is too large.");
        }

        try
        {
            if (JsonNode.Parse(text) is JsonObject body)
            {
                /* A JsonObject reads its properties lazily, so a duplicate key surfaces as an ArgumentException on first access, outside
                   this try. Touch Count here so the duplicate is refused as a body that is not a JSON object, not an unhandled 500. */
                _ = body.Count;
                return (body, null);
            }
        }
        catch (Exception ex) when (ex is JsonException or ArgumentException)
        {
        }

        return (null, "The body must be a JSON object.");
    }

    /// <summary>PURE: the practice scores body (see <see cref="MapInvestigationPractice"/>).</summary>
    internal static JsonObject PracticeScoresBody(IReadOnlyList<InvestigationPracticeScore> scores, int hoursBack)
    {
        var agents = new JsonArray();
        foreach (var s in scores)
        {
            agents.Add(new JsonObject
            {
                ["agent_id"] = s.AgentId,
                ["cases"] = s.Cases,
                ["scored"] = s.Scored,
                ["matched"] = s.Matched,
                ["unscored"] = s.Unscored,
                ["points"] = s.Points,
                ["possible"] = s.Possible,
                ["percent"] = s.Percent is { } p ? Math.Round(p * 100, 1) : null,
                ["missed_step_cases"] = s.MissedStepCases,
            });
        }

        return new JsonObject { ["hours_back"] = hoursBack, ["agents"] = agents };
    }

    /// <summary>
    /// <c>GET /api/webhook-signing</c> (#5998): whether the settings row's generic webhook and each route that has its own key holds
    /// a signing key, whether a rotation is in its window and when it ends. A status page only: the web has no write surface for
    /// notifications, and the answer is served from what the service read when it applied the store's configuration, so it holds no
    /// key text and does not depend on the web login being able to read the key columns.
    /// </summary>
    internal static void MapWebhookSigningStatus(WebApplication app, CollectorRuntimeState collector, Func<DateTime>? utcNow = null)
    {
        var clock = utcNow ?? (() => DateTime.UtcNow);
        app.MapGet("/api/webhook-signing", () => Results.Text(collector.SigningStatus.Answer(clock()).ToJsonString(), "application/json"));
    }

    /// <summary>
    /// The feedback routes (#5913): <c>GET /api/investigation-feedback</c> lists the reported requests of the last
    /// <c>hours_back</c> hours (default 168, at most 2160) with the feedback people gave each, and the false-positive counts per alert
    /// family; <c>PUT /api/investigation-feedback/{requestId}</c> records the signed-in person's feedback on one report.
    /// The write answers as the mute-rule writes do: <c>application/json</c> required, a read-only seat refused by the host's write gate,
    /// 400 for a value the rules refuse (<see cref="InvestigationFeedbackRules"/>, in plain words), 404 for an unknown request, 409 for
    /// a request with no report, 403 with <see cref="FeedbackRefusedMessage"/> when the store refuses the write for privilege (the web
    /// decides only under an admin store login, as the notes screen does), a fixed-sentence 500 or 503 for any other store failure.
    /// The author is the seat's <see cref="DarlingWebSeat.EditorPrincipal"/> shaped by <see cref="InvestigationFeedbackRules.AuthorFromIdentity"/>.
    /// A second record by the same author replaces the first. Bodies go through the statement sweep (#4348): they carry text an agent
    /// and a person wrote.
    /// </summary>
    internal static void MapInvestigationFeedback(WebApplication app, NpgsqlDataSource postgres, ILogger logger, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        var store = new InvestigationFeedbackStore(postgres, McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/investigation-feedback", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var hours = 168;
            if (context.Request.Query.TryGetValue("hours_back", out var hoursText)
                && (!int.TryParse(hoursText.ToString(), NumberStyles.None, CultureInfo.InvariantCulture, out hours) || hours < 1 || hours > 2160))
            {
                return ErrorResult("hours_back must be a whole number from 1 to 2160.", StatusCodes.Status400BadRequest);
            }

            try
            {
                var since = DateTime.UtcNow.AddHours(-hours);
                /* One connection for the list, the counts and the capability probe (#5913). */
                await using var connection = await postgres.OpenConnectionAsync(context.RequestAborted);
                var (rows, truncated) = await store.ListReportedAsync(connection, since, null, FeedbackListLimit, context.RequestAborted);
                var counts = await store.FamilyCountsAsync(connection, since, null, context.RequestAborted);
                /* #6600: the capability probe runs as the login the write would use. */
                var recordSource = WebAdminArm.SourceFor(adminArm, context, postgres);
                bool canRecord;
                if (ReferenceEquals(recordSource, postgres))
                {
                    canRecord = await store.CanRecordAsync(connection, context.RequestAborted);
                }
                else
                {
                    await using var recordConnection = await recordSource.OpenConnectionAsync(context.RequestAborted);
                    canRecord = await store.CanRecordAsync(recordConnection, context.RequestAborted);
                }

                return DarlingWebStatementSweep.JsonText(
                    FeedbackListBody(rows, truncated, counts, hours, canRecord), "/api/investigation-feedback", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-feedback", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/investigation-feedback/{requestId}", async (HttpContext context, string requestId) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            if (!Guid.TryParse(requestId, out var request))
            {
                return ErrorResult("The request id is not a request id.", StatusCodes.Status400BadRequest);
            }

            string text;
            try
            {
                text = await ReadBoundedBodyAsync(context, MaxFeedbackBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            if (!TryParseFeedbackBody(text, request, DarlingWebSeat.FromContext(context).EditorPrincipal, out var submission, out var refusal))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = refusal! }, "/api/investigation-feedback/{requestId}",
                    logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            try
            {
                var writeStore = new InvestigationFeedbackStore(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);
                var result = await writeStore.RecordAsync(submission!, context.RequestAborted);
                /* The store's own sentence for a refused, unknown or unreported request (never an exception's text). */
                var (outcome, sentence) = result;
                if (outcome is not (InvestigationFeedbackOutcome.Recorded or InvestigationFeedbackOutcome.Replaced))
                {
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = sentence! },"/api/investigation-feedback/{requestId}",
                        logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, FeedbackRefusalStatus(outcome));
                }

                var receipt = FeedbackReceipt(outcome, request, submission!.Author);
                return DarlingWebStatementSweep.JsonText(
                    receipt, "/api/investigation-feedback/{requestId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (IsFeedbackPrivilegeRefusal(ex))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = FeedbackRefusedMessage(managedStore, adminWritesOff) }, "/api/investigation-feedback/{requestId}",
                    logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-feedback/{requestId}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    /// <summary>The 403 sentence the export answers a login that may not record feedback (#6236).</summary>
    internal static string InvestigationExportRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("export investigation cases", managedStore, "Run --export-investigations on the host", adminWritesOff: adminWritesOff);

    /// <summary>The 403 sentence the export answers a request that is not a signed-in admin session (#6236).</summary>
    internal const string InvestigationExportSeatMessage =
        "Exporting investigation cases needs a signed-in dashboard account with the admin role. A viewer account, and a dashboard with no sign-in "
        + "configured, cannot: run --export-investigations on the host instead.";

    /// <summary>The route name the export is admitted and recorded under (#6236).</summary>
    internal const string InvestigationExportRoute = "investigation_export";

    /// <summary>The seconds a second export is told to wait when one is already running (#6236).</summary>
    internal const int InvestigationExportRetryAfterSeconds = 30;

    /// <summary>
    /// The download of the investigation cases that have feedback (#6236): <c>GET /api/investigation-export?since=&amp;until=&amp;server=&amp;verdict=</c>
    /// answers JSON lines (<c>application/x-ndjson</c>, one case per line, the same bytes <c>--export-investigations</c> writes) as an attachment.
    /// Two gates, both required: the request is a signed-in dashboard session whose account holds the admin role
    /// (<see cref="DarlingWebSeat.IsSignedInAdmin"/>; a viewer account and a dashboard with no sign-in are refused), and the web login holds INSERT and
    /// UPDATE on the feedback table (<see cref="InvestigationFeedbackStore.CanRecordAsync"/>). The route is heavy from its first call (<see cref="ReadAdmission.AlwaysHeavyRoutes"/>), only one
    /// export runs at a time (a second answers 429 with Retry-After), and the read-admission slot is released before the file is sent.
    /// Text is masked at the <c>mcp.dataDetail</c> the service started with, with the store's pseudonym key, by <see cref="InvestigationFeedbackExport"/>,
    /// the same code the command line runs. The cases are written one at a time to a work file in the service's own data folder and the file is then sent
    /// through the same handle (deleted when it closes), so a refused case refuses the whole download and no case is held in memory. 400 for a date, verdict or server it cannot use.
    /// </summary>
    internal static void MapInvestigationExport(
        WebApplication app, NpgsqlDataSource postgres, ILogger logger, McpDataDetail dataDetail, IPseudonymKeySource? pseudonymKeys,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null, InvestigationExportGate? exportGate = null,
        bool managedStore = false, WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);
        exportGate ??= new InvestigationExportGate(InvestigationExportGate.FolderFor(DarlingManagedPostgres.ResolveDataDirectory(new PostgresConfig())));
        var feedbackStore = new InvestigationFeedbackStore(postgres, McpCommandDeadlines.ReadSeconds);
        var requestStore = new InvestigationRequestStore(postgres, McpCommandDeadlines.ReadSeconds, () => DateTime.UtcNow);

        app.MapGet("/api/investigation-export", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            if (!DarlingWebSeat.IsSignedInAdmin(context))
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationExportSeatMessage }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }

            var since = ExportDate(context, "since", out var badSince);
            var until = ExportDate(context, "until", out var badUntil);
            if (badSince || badUntil)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = "since and until are dates such as 2026-10-01 or 2026-10-01T06:00:00Z." }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            var verdict = context.Request.Query.TryGetValue("verdict", out var verdictText) ? verdictText.ToString() : null;
            if (InvestigationFeedbackExport.FilterRefusal(since, until, verdict) is { } filterRefusal)
            {
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = filterRefusal }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
            }

            /* One export runs at a time, from before its work file exists until the file is gone: a second request answers busy and
               creates nothing. */
            if (!exportGate.TryEnter())
            {
                return BusyResult(new ReadAdmissionRefusal(
                    "An investigation export is already running. Try again when it has finished.", InvestigationExportRetryAfterSeconds, 1, 0));
            }

            /* Up to 2000 cases are read, so the route is heavy from its first call (ReadAdmission.AlwaysHeavyRoutes). The slot covers the
               store read into the work file only: it is released before the file is sent. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, InvestigationExportRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                exportGate.Exit();
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, InvestigationExportRoute, ReadWindowBand.None, context, readScope.Scope);
            try
            {
                /* #6600: the access probe runs as the login a feedback record would use; the cases are still read on the web's own pool. */
                await using (var connection = await WebAdminArm.SourceFor(adminArm, context, postgres).OpenConnectionAsync(context.RequestAborted))
                {
                    if (!await feedbackStore.CanRecordAsync(connection, context.RequestAborted))
                    {
                        run.Complete(ReadOutcome.GateFailed);
                        return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationExportRefusedMessage(managedStore, adminWritesOff) }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
                    }
                }

                var (serverId, serverLabel, serverError) = await InvestigationFeedbackExport.ResolveServerAsync(postgres, context.Request.Query.TryGetValue("server", out var serverText) ? serverText.ToString() : null, context.RequestAborted);
                if (serverError is not null)
                {
                    run.Complete(ReadOutcome.Ok);
                    return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = serverError }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status400BadRequest);
                }

                var key = await PseudonymKeySources.KeyForAsync(pseudonymKeys, dataDetail, context.RequestAborted);
                /* One handle for the whole life of the file: written, rewound and sent through the same handle, never reopened by name. */
                await using var file = InvestigationExportFile.CreateScratch(exportGate.Folder);
                var outcome = await InvestigationFeedbackExport.WriteAsync(
                    requestStore, since, until, serverId, verdict, dataDetail, key, file, context.RequestAborted);
                await file.FlushAsync(context.RequestAborted);
                file.Position = 0;

                if (outcome.Refused)
                {
                    return DarlingWebStatementSweep.Refusal("/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                context.Response.ContentType = "application/x-ndjson; charset=utf-8";
                context.Response.Headers.ContentDisposition = "attachment; filename=\"investigation-cases.jsonl\"";
                if (outcome.Truncated)
                {
                    context.Response.Headers["X-Export-Truncated"] = InvestigationRequestStore.FeedbackExportLimit.ToString(CultureInfo.InvariantCulture);
                }

                if (outcome.HeldBack > 0)
                {
                    context.Response.Headers["X-Export-Held-Back"] = outcome.HeldBack.ToString(CultureInfo.InvariantCulture);
                }

                if (serverLabel is not null)
                {
                    context.Response.Headers["X-Export-Server"] = Uri.EscapeDataString(serverLabel);
                }

                /* The store work is done: the slot and the route's timing end here, so a slow download holds neither. The one-export gate
                   stays held until the file is gone. */
                run.Complete(ReadOutcome.Ok);
                run.Dispose();
                ticket.Dispose();
                if (exportGate.BeforeSend is { } beforeSend)
                {
                    await beforeSend();
                }

                context.Response.ContentLength = file.Length;
                await file.CopyToAsync(context.Response.Body, context.RequestAborted);
                return Results.Empty;
            }
            catch (Exception ex) when (IsFeedbackPrivilegeRefusal(ex))
            {
                run.Complete(ReadOutcome.GateFailed);
                return DarlingWebStatementSweep.JsonText(new JsonObject { ["error"] = InvestigationExportRefusedMessage(managedStore, adminWritesOff) }, "/api/investigation-export", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status403Forbidden);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/investigation-export", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
            finally
            {
                exportGate.Exit();
            }
        });
    }

    /// <summary>One date query parameter of the export (#6236): null when absent, with <paramref name="bad"/> set when it is not a date.</summary>
    private static DateTime? ExportDate(HttpContext context, string name, out bool bad)
    {
        bad = false;
        if (!context.Request.Query.TryGetValue(name, out var text) || string.IsNullOrWhiteSpace(text))
            return null;
        if (DateTimeOffset.TryParse(text.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var when))
            return when.UtcDateTime;
        bad = true;
        return null;
    }

    internal static JsonObject FeedbackReceipt(InvestigationFeedbackOutcome outcome, Guid request, string author) => new()
    {
        ["status"] = outcome == InvestigationFeedbackOutcome.Recorded ? "recorded" : "replaced",
        ["request_id"] = request.ToString(),
        ["author"] = author,
    };

    /// <summary>The status a refused feedback record answers: 404 for an unknown request, 409 for one with no report, else 400.</summary>
    private static int FeedbackRefusalStatus(InvestigationFeedbackOutcome outcome) => outcome switch
    {
        InvestigationFeedbackOutcome.NotFound => StatusCodes.Status404NotFound,
        InvestigationFeedbackOutcome.NoReport => StatusCodes.Status409Conflict,
        _ => StatusCodes.Status400BadRequest,
    };

    /// <summary>True when the store refused the feedback write for privilege (SQLSTATE 42501).</summary>
    private static bool IsFeedbackPrivilegeRefusal(Exception ex) => ex is PostgresException { SqlState: "42501" };

    /// <summary>
    /// PURE: parses a feedback body, <c>{"verdict": "right|partly|wrong", "corrected_verdict": null|"real"|"false_positive"|"needs_person",
    /// "missed_step": null|"...", "comment": null|"..."}</c>, into the submission for <paramref name="request"/> by <paramref name="identity"/>.
    /// False with a caller-facing <paramref name="refusal"/> when the body is not a JSON object, a field is not text, or a value fails
    /// <see cref="InvestigationFeedbackRules"/>. Unknown fields are ignored.
    /// </summary>
    internal static bool TryParseFeedbackBody(string body, Guid request, string identity, out InvestigationFeedbackSubmission? submission, out string? refusal)
    {
        submission = null;
        refusal = null;
        JsonNode? parsed;
        try
        {
            parsed = JsonNode.Parse(body);
        }
        catch (JsonException)
        {
            refusal = "The body is not JSON.";
            return false;
        }

        if (parsed is not JsonObject obj)
        {
            refusal = "The body must be a JSON object with a verdict.";
            return false;
        }

        string? Field(string name, out bool bad)
        {
            bad = false;
            if (!obj.TryGetPropertyValue(name, out var node) || node is null)
            {
                return null;
            }

            if (node is JsonValue value && value.TryGetValue<string>(out var text))
            {
                return text;
            }

            bad = true;
            return null;
        }

        var verdict = Field("verdict", out var badVerdict);
        var corrected = Field("corrected_verdict", out var badCorrected);
        var missed = Field("missed_step", out var badMissed);
        var comment = Field("comment", out var badComment);
        if (badVerdict || badCorrected || badMissed || badComment)
        {
            refusal = "verdict, corrected_verdict, missed_step and comment must each be text or null.";
            return false;
        }

        /* A sign-in name the table's author rule cannot hold is shaped, not refused: DOMAIN\erik reads DOMAIN_erik-1a2b3c4d (a hash suffix keeps two such sign-ins apart). */
        var author = InvestigationFeedbackRules.AuthorFromIdentity(identity) ?? "web";
        var candidate = new InvestigationFeedbackSubmission(
            request, verdict ?? "", string.IsNullOrWhiteSpace(corrected) ? null : corrected,
            string.IsNullOrWhiteSpace(missed) ? null : missed, string.IsNullOrWhiteSpace(comment) ? null : comment, author);
        var problems = InvestigationFeedbackRules.Validate(candidate);
        if (problems.Count > 0)
        {
            refusal = InvestigationFeedbackRules.RefusalMessage(problems);
            return false;
        }

        submission = candidate;
        return true;
    }

    /// <summary>The list body: <c>{hours_back, can_record, requests_returned, truncated, families: [{family, reported, with_feedback, marked_wrong,
    /// agent_false_positives, false_positives}], requests: [{request_id, server_name, family, first_metric, opened_at_utc, reported_at_utc,
    /// agent_id, report_verdict, root_cause, summary, feedback: [{verdict, corrected_verdict, missed_step, comment, author, recorded_at_utc}]}]}</c>,
    /// newest report first. <c>can_record</c> is whether the web dashboard's own store login may write feedback (false on managed and
    /// compose stores, where it connects as the viewer role): the page shows its form only when it is true (#5913).</summary>
    internal static JsonObject FeedbackListBody(
        IReadOnlyList<InvestigationFeedbackListRow> rows, bool truncated, IReadOnlyList<InvestigationFamilyCounts> counts, int hoursBack,
        bool canRecord)
    {
        static string Iso(DateTime t) => DateTime.SpecifyKind(t, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture);
        var requests = new JsonArray();
        foreach (var row in rows)
        {
            var feedback = new JsonArray();
            foreach (var f in row.Feedback)
            {
                feedback.Add(new JsonObject
                {
                    ["verdict"] = f.Verdict,
                    ["corrected_verdict"] = f.CorrectedVerdict,
                    ["missed_step"] = f.MissedStep,
                    ["comment"] = f.Comment,
                    ["author"] = f.Author,
                    ["recorded_at_utc"] = Iso(f.CreatedAtUtc),
                });
            }

            requests.Add(new JsonObject
            {
                ["request_id"] = row.RequestId.ToString(),
                ["server_name"] = row.ServerName,
                ["family"] = row.Family,
                ["first_metric"] = row.FirstMetric,
                ["opened_at_utc"] = Iso(row.OpenedAtUtc),
                ["reported_at_utc"] = Iso(row.ReportedAtUtc),
                ["agent_id"] = row.AgentId,
                ["report_verdict"] = row.ReportVerdict,
                ["root_cause"] = row.RootCause,
                ["summary"] = row.Summary,
                ["feedback"] = feedback,
            });
        }

        return new JsonObject
        {
            ["hours_back"] = hoursBack,
            ["can_record"] = canRecord,
            ["requests_returned"] = requests.Count,
            ["truncated"] = truncated,
            ["families"] = InvestigationToolCore.FamilyCountsPayload(counts),
            ["requests"] = requests,
        };
    }

    /// <summary>
    /// PURE: parses a dismiss body into its distinct keys, in first-seen order. False with a caller-facing
    /// <paramref name="refusal"/> when the body is not a JSON object with a non-empty <c>alerts</c> array of at
    /// most <see cref="MaxDismissKeys"/> objects, each with an ISO-8601 <c>alert_time</c>, an integer
    /// <c>server_id</c> and a non-empty <c>metric_name</c>. A time that carries an offset is converted to UTC; a
    /// bare time is taken as the store's naive UTC, which is how <c>get_alert_history</c> spells it.
    /// </summary>
    internal static bool ParseDismissBody(string body, out List<AlertDismissKey> keys, out string? refusal)
    {
        keys = new List<AlertDismissKey>();
        refusal = null;

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(body);
        }
        catch (JsonException)
        {
            refusal = "Request body must be a JSON object.";
            return false;
        }

        /* A duplicate property name makes the JsonObject indexer throw ArgumentException; that is a caller's
           malformed body (400), not a server fault. */
        JsonNode? alertsNode = null;
        try
        {
            alertsNode = (root as JsonObject)?["alerts"];
        }
        catch (ArgumentException)
        {
            refusal = "Request body must be a JSON object with one alerts array.";
            return false;
        }

        if (root is not JsonObject || alertsNode is not JsonArray alerts)
        {
            refusal = "Request body must be {\"alerts\": [{\"alert_time\", \"server_id\", \"metric_name\"}, ...]}.";
            return false;
        }

        if (alerts.Count == 0)
        {
            refusal = "alerts must name at least one alert.";
            return false;
        }

        if (alerts.Count > MaxDismissKeys)
        {
            refusal = $"alerts may name at most {MaxDismissKeys} alerts per request.";
            return false;
        }

        var seen = new HashSet<AlertDismissKey>();
        foreach (var item in alerts)
        {
            if (item is not JsonObject entry
                || !TryReadString(entry["alert_time"], out var timeText)
                || !TryReadString(entry["metric_name"], out var metric)
                || metric.Length == 0 || metric.Length > MaxDismissMetricNameLength
                || entry["server_id"] is not JsonValue idValue || !idValue.TryGetValue<int>(out var serverId)
                || !DateTime.TryParse(
                    timeText, CultureInfo.InvariantCulture,
                    DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var time))
            {
                refusal = "Each alert needs an ISO-8601 alert_time, an integer server_id and a non-empty metric_name.";
                return false;
            }

            var key = new AlertDismissKey(DateTime.SpecifyKind(time, DateTimeKind.Unspecified), serverId, metric);
            if (seen.Add(key))
            {
                keys.Add(key);
            }
        }

        return true;
    }

    private static bool TryReadString(JsonNode? node, out string value)
    {
        value = "";
        if (node is JsonValue v && v.TryGetValue<string>(out var text) && text is not null)
        {
            value = text;
            return true;
        }

        return false;
    }

    /// <summary>Reads the request body, refusing (<see cref="InvalidDataException"/>) past <paramref name="maxBytes"/>.</summary>
    private static async Task<string> ReadBoundedBodyAsync(HttpContext context, int maxBytes)
    {
        using var buffer = new MemoryStream();
        var chunk = new byte[8192];
        int read;
        while ((read = await context.Request.Body.ReadAsync(chunk, context.RequestAborted)) > 0)
        {
            if (buffer.Length + read > maxBytes)
            {
                throw new InvalidDataException("Request body is too large.");
            }

            buffer.Write(chunk, 0, read);
        }

        return System.Text.Encoding.UTF8.GetString(buffer.GetBuffer(), 0, (int)buffer.Length);
    }

    /// <summary>
    /// <c>GET /api/admin/servers</c> (#5239): the Admin page's Manage Servers grid - every configured server,
    /// enabled or disabled, with its authentication mode, monthly cost and added time (see
    /// <see cref="DarlingAdminServersReader"/>). It is a web-only route: <c>list_servers</c> keeps its
    /// enabled-only answer for every other consumer, and a route here adds nothing to the MCP <c>tools/list</c>.
    /// The gate is the one every web route sits behind (the sign-in gate, then the seat's method gate, which
    /// lets a read-only seat make a GET); the read runs on the viewer-role pool, whose column grant on
    /// <c>config_monitored_servers</c> leaves out the credential columns, and the statement names none of them.
    /// </summary>
    /// <summary>The most request-body bytes the run-collectors route reads (a list of collector names).</summary>
    internal const int MaxCollectorRunBodyBytes = 16 * 1024;

    /// <summary>
    /// The run-collectors-now routes (#5941). <c>POST /api/collectors/run</c> (body: the server name, optional collectors) queues a <c>snapshot_now</c> for
    /// one server: an empty body runs every enabled collector, <c>{"collectors":["name", ...]}</c> runs only those, each
    /// even when disabled. It needs the edit seat (a read-only seat gets 403) and goes through <see cref="SnapshotNowRequests"/>,
    /// the same guard the MCP tool uses: 202 queued, 409 when a run for the server is queued or going, 429 (with Retry-After)
    /// within 60 seconds of the last one, 400 for an unknown collector name, 404 for an unknown server. The page then polls
    /// <c>GET /api/collectors/run/{commandId}</c> for the result the service wrote: the collectors run,
    /// the rows, and the ones it skipped with the reason.
    /// </summary>
    internal static void MapCollectorRuns(
        WebApplication app, ISnapshotNowStore store, ILogger logger,
        Func<string, CancellationToken, Task<(int ServerId, string? Error)>> resolveServer, TimeProvider? clock = null)
    {
        var requests = new SnapshotNowRequests(store, clock);

        app.MapPost("/api/collectors/run", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            if (!seat.CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxCollectorRunBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            string? serverName;
            IReadOnlyList<string>? collectors = null;
            try
            {
                var parsedBody = JsonNode.Parse(body) as JsonObject;
                serverName = parsedBody?["server"]?.GetValue<string>();
                if (parsedBody?["collectors"] is JsonNode list)
                {
                    if (!SnapshotNowArgs.TryParse(new JsonObject { ["collectors"] = list.DeepClone() }.ToJsonString(), out var parsed, out var parseError))
                    {
                        return ErrorResult(parseError!, StatusCodes.Status400BadRequest);
                    }

                    collectors = parsed;
                }
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
            {
                return ErrorResult("The body must be a JSON object with a \"server\" name and an optional \"collectors\" list.", StatusCodes.Status400BadRequest);
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                return ErrorResult("The body must name the server.", StatusCodes.Status400BadRequest);
            }

            SnapshotRequestOutcome outcome;
            var stopwatch = Stopwatch.StartNew();
            int id;
            try
            {
                var (resolvedId, resolveError) = await resolveServer(serverName, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                id = resolvedId;
                outcome = await requests.RequestAsync(id, collectors, seat.EditorPrincipal ?? "web", context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"run collectors request failed ({ex.GetType().Name})", "/api/collectors/run", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (outcome.Accepted)
            {
                logger.LogInformation("Web: {Principal} queued a collector run for server {ServerId} ({Scope}).",
                    seat.EditorPrincipal, id, collectors is { Count: > 0 } ? string.Join(",", collectors) : "all enabled");
                return JsonNodeResult(
                    new JsonObject { ["status"] = "queued", ["command_id"] = outcome.CommandId }, StatusCodes.Status202Accepted);
            }

            var refusal = new JsonObject { ["error"] = outcome.Text, ["code"] = outcome.Code };
            if (outcome.NextAllowedUtc is { } next)
            {
                refusal["next_allowed_utc"] = next.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
                var wait = (int)Math.Ceiling(Math.Max(1, (next - (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime).TotalSeconds));
                context.Response.Headers["Retry-After"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
            }

            var status = outcome.Code switch
            {
                SnapshotRequestOutcome.InFlightCode => StatusCodes.Status409Conflict,
                SnapshotRequestOutcome.RateLimitedCode => StatusCodes.Status429TooManyRequests,
                SnapshotRequestOutcome.UnknownServerCode => StatusCodes.Status404NotFound,
                _ => StatusCodes.Status400BadRequest,
            };
            return JsonNodeResult(refusal, status);
        });

        app.MapGet("/api/collectors/run/{commandId:long}", async (HttpContext context, long commandId) =>
        {
            SnapshotCommandState? state;
            var stopwatch = Stopwatch.StartNew();
            try
            {
                state = await store.ReadStateAsync(commandId, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"run collectors read failed ({ex.GetType().Name})", "/api/collectors/run/{commandId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (state is null)
            {
                return ErrorResult("No such collector run.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            var node = new JsonObject
            {
                ["status"] = state.Status,
                ["finished"] = state.Status is "succeeded" or "failed",
                ["result_status"] = state.ResultStatus,
            };
            if (state.ResultJson is not null)
            {
                try
                {
                    node["result"] = JsonNode.Parse(state.ResultJson);
                }
                catch (JsonException)
                {
                    /* The service wrote it; if it is not JSON the page shows the status alone. */
                }
            }

            return JsonNodeResult(node);
        });
    }

    /// <summary>The most request-body bytes the live Active Queries route reads (a server name).</summary>
    internal const int MaxActiveQueriesLiveBodyBytes = 4 * 1024;

    /// <summary>
    /// The live Active Queries routes (#6386), built like the run-collectors-now routes. <c>POST /api/active-queries/live</c> (body: the
    /// server name) queues the same argument-less <c>fetch_active_queries</c> command the Viewer's Current Active Queries queues, through
    /// <c>config.request_active_queries_live</c> (the viewer role has no INSERT on the command queue). It needs the edit seat (403
    /// otherwise): 202 queued, 409 when a read for the server is queued or going, 429 (with Retry-After) within 10 seconds of the last,
    /// 404 for an unknown server, 403 when the web's store login cannot execute the function (a store provisioned before this release).
    /// <c>GET /api/active-queries/live/{commandId}</c> returns the command's state and the service's <c>result_json</c> (the running
    /// requests, one object per request, PascalCase as the service serializes them). <c>GET /api/active-queries/live/available</c>
    /// says whether this seat and store login can queue one, so the page draws the control or the note.
    /// </summary>
    internal static void MapActiveQueriesLive(
        WebApplication app, IActiveQueriesLiveStore store, ILogger logger,
        Func<string, CancellationToken, Task<(int ServerId, string? Error)>> resolveServer, bool managedStore, TimeProvider? clock = null,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        app.MapGet("/api/active-queries/live/available", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var storeCan = seat.CanEdit && await store.CanQueueAsync(context.RequestAborted);
                context.Response.Headers.CacheControl = "no-store";
                return JsonNodeResult(new JsonObject { ["can_queue"] = storeCan, ["can_edit"] = seat.CanEdit, ["managed_store"] = managedStore });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"live active queries availability failed ({ex.GetType().Name})", "/api/active-queries/live/available", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
        });

        app.MapPost("/api/active-queries/live", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            if (!seat.CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxActiveQueriesLiveBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            string? serverName;
            try
            {
                serverName = (JsonNode.Parse(body) as JsonObject)?["server"]?.GetValue<string>();
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
            {
                return ErrorResult("The body must be a JSON object with a \"server\" name.", StatusCodes.Status400BadRequest);
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                return ErrorResult("The body must name the server.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            int id;
            SnapshotRequestOutcome outcome;
            try
            {
                var (resolvedId, resolveError) = await resolveServer(serverName, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                id = resolvedId;
                outcome = await ActiveQueriesLiveRequests.RequestAsync(store, id, seat.EditorPrincipal ?? "web", context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"live active queries request failed ({ex.GetType().Name})", "/api/active-queries/live", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (!outcome.Accepted)
            {
                var refusal = new JsonObject { ["error"] = outcome.Text, ["code"] = outcome.Code };
                if (outcome.NextAllowedUtc is { } next)
                {
                    refusal["next_allowed_utc"] = next.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
                    var wait = (int)Math.Ceiling(Math.Max(1, (next - (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime).TotalSeconds));
                    context.Response.Headers["Retry-After"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
                }

                return JsonNodeResult(refusal, outcome.Code switch
                {
                    SnapshotRequestOutcome.InFlightCode => StatusCodes.Status409Conflict,
                    SnapshotRequestOutcome.RateLimitedCode => StatusCodes.Status429TooManyRequests,
                    SnapshotRequestOutcome.UnknownServerCode => StatusCodes.Status404NotFound,
                    ActiveQueriesLiveRequests.StoreRefusedCode => StatusCodes.Status403Forbidden,
                    _ => StatusCodes.Status400BadRequest,
                });
            }

            logger.LogInformation("Web: {Principal} queued a live active-queries read for server {ServerId}.", seat.EditorPrincipal, id);
            return JsonNodeResult(new JsonObject { ["status"] = "queued", ["command_id"] = outcome.CommandId }, StatusCodes.Status202Accepted);
        });

        app.MapGet("/api/active-queries/live/{commandId:long}", async (HttpContext context, long commandId) =>
        {
            SnapshotCommandState? state;
            var stopwatch = Stopwatch.StartNew();
            /* The result read carries the plan XML a live read stored, so it is admitted and recorded like the other web reads. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, ActiveQueriesLiveResultRoute, ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, ActiveQueriesLiveResultRoute, ReadWindowBand.None, context, readScope.Scope);
            try
            {
                state = await store.ReadStateAsync(commandId, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"live active queries read failed ({ex.GetType().Name})", "/api/active-queries/live/{commandId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (state is null)
            {
                run.Complete(ReadOutcome.Ok);
                return ErrorResult("No such live read.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            var node = new JsonObject
            {
                ["status"] = state.Status,
                ["finished"] = state.Status is "succeeded" or "failed",
                ["result_status"] = state.ResultStatus,
            };
            if (state.ResultJson is not null)
            {
                try
                {
                    node["result"] = JsonNode.Parse(state.ResultJson);
                }
                catch (JsonException)
                {
                    /* The service wrote it; if it is not JSON the page shows the status alone. */
                }
            }

            run.Complete(ReadOutcome.Ok);
            return JsonNodeResult(node);
        });
    }

    /// <summary>The most request-body bytes the analyze-now route reads (a server name).</summary>
    internal const int MaxAnalyzeNowBodyBytes = 4 * 1024;

    /// <summary>The longest finding title the Ask AI route builds a prompt around (a headline is a sentence).</summary>
    internal const int MaxAskAiTitleLength = 500;

    /// <summary>
    /// The Recommendations tab's routes (#6389). <c>POST /api/analyze-now</c> (body: the server name) queues the same argument-less
    /// <c>analyze_now</c> command the Viewer's Generate now queues, through <c>config.request_analyze_now</c> (the viewer role has no INSERT
    /// on the command queue). Built like <see cref="MapActiveQueriesLive"/>: edit seat only (403 otherwise), 202 queued, 409 when a run for
    /// the server is queued or going, 429 (with Retry-After) within 15 seconds of the last, 404 for an unknown server, 403 when the web's
    /// store login cannot execute the function. <c>GET /api/analyze-now/{commandId}</c> is the command's state (the page polls it and
    /// re-reads the findings when it finishes); <c>GET /api/analyze-now/available</c> says whether this seat and store login can queue a
    /// run. <c>GET /api/analysis/ask-ai-prompt</c> returns the text the Viewer's Ask AI copies, from the one shared builder: no key and no
    /// model call, only a prompt for the reader's own MCP client.
    /// </summary>
    internal static void MapAnalyzeNow(
        WebApplication app, IAnalyzeNowStore store, NpgsqlDataSource postgres, ILogger logger,
        Func<string, CancellationToken, Task<(int ServerId, string? Error)>> resolveServer, bool managedStore, TimeProvider? clock = null)
    {
        app.MapGet("/api/analyze-now/available", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var storeCan = seat.CanEdit && await store.CanQueueAsync(context.RequestAborted);
                context.Response.Headers.CacheControl = "no-store";
                return JsonNodeResult(new JsonObject { ["can_queue"] = storeCan, ["can_edit"] = seat.CanEdit, ["managed_store"] = managedStore });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"analyze now availability failed ({ex.GetType().Name})", "/api/analyze-now/available", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
        });

        app.MapPost("/api/analyze-now", async (HttpContext context) =>
        {
            var seat = DarlingWebSeat.FromContext(context);
            if (!seat.CanEdit)
            {
                return ErrorResult("This account has read-only access.", StatusCodes.Status403Forbidden);
            }

            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string body;
            try
            {
                body = await ReadBoundedBodyAsync(context, MaxAnalyzeNowBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            string? serverName;
            try
            {
                serverName = (JsonNode.Parse(body) as JsonObject)?["server"]?.GetValue<string>();
            }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException or FormatException)
            {
                return ErrorResult("The body must be a JSON object with a \"server\" name.", StatusCodes.Status400BadRequest);
            }

            if (string.IsNullOrWhiteSpace(serverName))
            {
                return ErrorResult("The body must name the server.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            int id;
            SnapshotRequestOutcome outcome;
            try
            {
                var (resolvedId, resolveError) = await resolveServer(serverName, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                id = resolvedId;
                outcome = await AnalyzeNowRequests.RequestAsync(store, id, seat.EditorPrincipal ?? "web", context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"analyze now request failed ({ex.GetType().Name})", "/api/analyze-now", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (!outcome.Accepted)
            {
                var refusal = new JsonObject { ["error"] = outcome.Text, ["code"] = outcome.Code };
                if (outcome.NextAllowedUtc is { } next)
                {
                    refusal["next_allowed_utc"] = next.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
                    var wait = (int)Math.Ceiling(Math.Max(1, (next - (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime).TotalSeconds));
                    context.Response.Headers["Retry-After"] = wait.ToString(System.Globalization.CultureInfo.InvariantCulture);
                }

                return JsonNodeResult(refusal, outcome.Code switch
                {
                    SnapshotRequestOutcome.InFlightCode => StatusCodes.Status409Conflict,
                    SnapshotRequestOutcome.RateLimitedCode => StatusCodes.Status429TooManyRequests,
                    SnapshotRequestOutcome.UnknownServerCode => StatusCodes.Status404NotFound,
                    AnalyzeNowRequests.StoreRefusedCode => StatusCodes.Status403Forbidden,
                    _ => StatusCodes.Status400BadRequest,
                });
            }

            logger.LogInformation("Web: {Principal} queued an analysis run for server {ServerId}.", seat.EditorPrincipal, id);
            return JsonNodeResult(new JsonObject { ["status"] = "queued", ["command_id"] = outcome.CommandId }, StatusCodes.Status202Accepted);
        });

        app.MapGet("/api/analyze-now/{commandId:long}", async (HttpContext context, long commandId) =>
        {
            var stopwatch = Stopwatch.StartNew();
            SnapshotCommandState? state;
            try
            {
                state = await store.ReadStateAsync(commandId, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"analyze now read failed ({ex.GetType().Name})", "/api/analyze-now/{commandId}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }

            if (state is null)
            {
                return ErrorResult("No such analysis run.", StatusCodes.Status404NotFound);
            }

            context.Response.Headers.CacheControl = "no-store";
            return JsonNodeResult(new JsonObject
            {
                ["status"] = state.Status,
                ["finished"] = state.Status is "succeeded" or "failed",
                ["succeeded"] = state.Status == "succeeded",
                ["result_status"] = state.ResultStatus,
            });
        });

        app.MapGet("/api/analysis/ask-ai-prompt", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            var server = context.Request.Query["server"].ToString();
            var title = context.Request.Query["title"].ToString();
            if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(title) || title.Length > MaxAskAiTitleLength)
            {
                return ErrorResult("The request must name the server and a finding title (at most " + MaxAskAiTitleLength + " characters).", StatusCodes.Status400BadRequest);
            }

            DateTime? start = null;
            DateTime? end = null;
            var startText = context.Request.Query["start"].ToString();
            var endText = context.Request.Query["end"].ToString();
            if (startText.Length > 0 || endText.Length > 0)
            {
                const System.Globalization.DateTimeStyles utc = System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal;
                if (!DateTime.TryParse(startText, System.Globalization.CultureInfo.InvariantCulture, utc, out var parsedStart)
                    || !DateTime.TryParse(endText, System.Globalization.CultureInfo.InvariantCulture, utc, out var parsedEnd))
                {
                    return ErrorResult("start and end must be UTC timestamps.", StatusCodes.Status400BadRequest);
                }

                start = parsedStart;
                end = parsedEnd;
            }

            try
            {
                var (serverId, resolveError) = await resolveServer(server, context.RequestAborted);
                if (resolveError is not null)
                {
                    return CollectorRunResolveFailure(resolveError, logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
                }

                var clocks = await DarlingServerClocksReader.GetAsync(postgres, serverId, McpCommandDeadlines.ReadSeconds, context.RequestAborted);
                var prompt = PerformanceMonitor.Analysis.RecommendationAskAiPrompt.BuildFor(
                    server, title, start, end, DarlingServerClocksReader.ClockFor(clocks, serverId), (clock ?? TimeProvider.System).GetUtcNow().UtcDateTime);
                context.Response.Headers.CacheControl = "no-store";
                return JsonNodeResult(new JsonObject { ["prompt"] = prompt });
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                return ServerErrorResult($"ask ai prompt failed ({ex.GetType().Name})", "/api/analysis/ask-ai-prompt", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
        });
    }

    /// <summary>
    /// What the run route answers when the server resolver hands back an error string (#5941 review). The resolver has three
    /// shapes and each gets its own answer: a registry read fault (a store fault, not the caller's request) is logged and answers
    /// the fixed 500/503 body; a name that matches no server ("Could not resolve server. ...") is a 404 with the envelope's plain
    /// message; any other refusal envelope (a name that matches several servers) is a 400 with its plain message. The raw
    /// envelope JSON is never the body.
    /// </summary>
    internal static IResult CollectorRunResolveFailure(string error, ILogger logger, long elapsedMs, CancellationToken requestAborted)
    {
        if (ClassifyToolResponse(error) == ToolResponseKind.ServerError)
        {
            return ServerErrorResult(error, "/api/collectors/run", logger, elapsedMs, requestAborted);
        }

        var message = McpHelpers.ErrorMessageOf(error);
        var noMatch = !McpHelpers.IsRefusalEnvelope(error)
            || message.StartsWith("Could not resolve server", StringComparison.Ordinal);
        return ErrorResult(message, noMatch ? StatusCodes.Status404NotFound : StatusCodes.Status400BadRequest);
    }

    /// <summary>The 403 a refused switch change answers when the web signs in to the store as the viewer role (#6269).</summary>
    internal static string ServerActionRefusedMessage(bool managedStore, bool adminWritesOff = false) =>
        DarlingWebStoreWrites.Refused("change server action switches", managedStore, adminWritesOff: adminWritesOff);

    /// <summary>
    /// The server action switch routes (#6269). <c>GET /api/server-actions</c> lists every server with a state for each action
    /// (<c>{can_decide, servers_returned, servers: [{server_id, server_name, engine, engine_kind, actions: [{action, valid, enabled, set_by,
    /// updated_at_utc}]}]}</c>; <c>can_decide</c> is false under the viewer role, which lists only). <c>PUT /api/servers/{id}/actions/{action}</c>
    /// with <c>{"enabled": true|false}</c> sets one switch: 200 <c>{status: set|unchanged, ...}</c>; 400 for a bad body, an unknown action or a
    /// signed-in name that cannot be recorded; 403 under the viewer role (and for a read-only seat, which the host's method gate refuses first);
    /// 404 for an unknown server; 409 when the server's engine cannot run the action; 415 without <c>application/json</c>; 503 before the service
    /// has published its store owner connection. <b>The write runs on that owner connection</b>, because no other role can write the table; the
    /// admin check is the web login's own privileges (<see cref="AgentChangeUndo.CanDecideAsync"/>) and the seat's method gate. The change is
    /// journaled by <c>person:&lt;login&gt;</c>, surface <c>web</c>.
    /// </summary>
    internal static void MapServerActions(WebApplication app, NpgsqlDataSource postgres, AgentChangeUndo changes, ILogger logger, StoreOwnerSource? ownerSource, bool managedStore = false,
        WebAdminArm? adminArm = null)
    {
        var adminWritesOff = DarlingWebStoreWrites.AdminWritesOff(managedStore, adminArm);

        /* #6600: the access check runs as the login the switch change is allowed under; the write itself stays on the owner connection. */
        AgentChangeUndo DecideProbe(HttpContext context) =>
            adminArm is null ? changes : new AgentChangeUndo(WebAdminArm.SourceFor(adminArm, context, postgres), McpCommandDeadlines.ReadSeconds);

        app.MapGet("/api/server-actions", async (HttpContext context) =>
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var rows = await ServerActionSwitches.ListAsync(postgres, context.RequestAborted);
                var body = Mcp.DarlingMcpServerActionTools.ListToJson(rows);
                body["can_decide"] = DarlingWebSeat.IsSignedInAdmin(context) && await DecideProbe(context).CanDecideAsync(context.RequestAborted);
                body["signed_in_admin"] = DarlingWebSeat.IsSignedInAdmin(context);
                return DarlingWebStatementSweep.JsonText(body, "/api/server-actions", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/server-actions", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });

        app.MapPut("/api/servers/{id:int}/actions/{action}", async (HttpContext context, int id, string action) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (JsonNode.Parse(await ReadBodyAsync(context)) is not JsonObject body || body["enabled"] is not JsonValue flag || !flag.TryGetValue(out bool enabled))
                {
                    return ErrorResult("Send {\"enabled\": true} or {\"enabled\": false}.", StatusCodes.Status400BadRequest);
                }

                /* A signed-in admin comes first: the store login the web runs on says nothing about who is calling (#6269). */
                if (!DarlingWebSeat.IsSignedInAdmin(context))
                {
                    return SignInRequiredResult();
                }

                /* The same actor every web person write records: ONE person: prefix on the signed-in name (NoteDecider), held to the shared 256-unit name rule. */
                var actor = NoteDecider(DarlingWebSeat.FromContext(context));
                if (InvestigationPersonWrites.ValidateActor(actor) is not null)
                {
                    return ErrorResult("The signed-in name cannot be recorded as the person who changed this switch.", StatusCodes.Status400BadRequest);
                }

                /* The admin check comes before anything is read or written. */
                if (!await DecideProbe(context).CanDecideAsync(context.RequestAborted))
                {
                    return ErrorResult(ServerActionRefusedMessage(managedStore, adminWritesOff), StatusCodes.Status403Forbidden);
                }

                var owner = ownerSource?.Read();
                if (owner is null)
                {
                    return ErrorResult("The service has not published its store owner connection yet, so the switch cannot be written. Try again in a moment.",
                        StatusCodes.Status503ServiceUnavailable);
                }

                var result = await ServerActionSwitches.SetAsync(owner, id, action, enabled, actor, ServerActionSurfaces.Web, context.RequestAborted);
                var status = result.Succeeded ? StatusCodes.Status200OK
                    : result.Code == "unknown_server" ? StatusCodes.Status404NotFound
                    : result.Code is "not_for_engine" or "switch_busy" ? StatusCodes.Status409Conflict
                    : StatusCodes.Status400BadRequest;
                return DarlingWebStatementSweep.JsonText(result.ToJson(), "/api/servers/{id}/actions/{action}", logger, stopwatch.ElapsedMilliseconds,
                    context.RequestAborted, status);
            }
            catch (Exception ex) when (ex is JsonException)
            {
                return ErrorResult("The request body is not valid.", StatusCodes.Status400BadRequest);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/servers/{id}/actions/{action}", stopwatch.ElapsedMilliseconds, ex, context.RequestAborted);
                return Results.Json(DarlingWebFailureLog.Body(ex), statusCode: DarlingWebFailureLog.StatusCode(ex));
            }
        });
    }

    internal static void MapAdminServers(WebApplication app, NpgsqlDataSource postgres)
    {
        app.MapGet(DarlingAdminServersReader.Route, async (HttpContext context) =>
        {
            var rows = await DarlingAdminServersReader.ReadAsync(postgres, context.RequestAborted);
            return Results.Text(DarlingAdminServersReader.Render(rows, DateTime.UtcNow), "application/json");
        });
    }

    /// <summary>
    /// The fleet server-tag write routes (#5085), the same shape as <see cref="MapMuteRules"/>: each route parses
    /// its JSON body and hands it to the matching <c>DarlingMcpServerTagTools.*Core</c> over a
    /// <see cref="ServerTagStore"/> on the viewer-role pool, so the web surface and the MCP tools share one set of
    /// rules (names, colours, depth, cycles, sibling uniqueness) and one set of answers. <c>application/json</c> is
    /// required on every route, the same CSRF discipline as the mute-rule routes. The admin gate is the seat's
    /// method gate (<see cref="DarlingWebSeat.IsRequestAllowed"/> refuses every non-GET from a read-only seat);
    /// the viewer role's two single-table grants are only the floor beneath it.
    ///
    /// <para><b>Routes.</b> <c>POST /api/server-tags</c> (<c>{name, parent_id?, colour?}</c>, 201 on
    /// <c>created</c>); <c>PATCH /api/server-tags/{id}</c> (the body IS the partial <c>changes_json</c>, or the single field <c>{"move":"up"|"down"}</c> to reorder siblings, #6094);
    /// <c>DELETE /api/server-tags/{id}?confirm=true</c> (the flag rides the query string, so the DELETE stays
    /// bodyless like <c>DELETE /api/mute-rules/{id}</c>); <c>POST /api/server-tags/{id}/servers</c> and
    /// <c>DELETE /api/server-tags/{id}/servers</c> (<c>{server_ids: [...]}</c> in the body).</para>
    ///
    /// <para><b>Statuses.</b> See <see cref="ServerTagEnvelopeStatus"/>: the mute-rule mapping plus
    /// <c>conflict</c> and <c>confirm_required</c> as 409, the latter with its full envelope (the
    /// <c>affected_rules</c> a delete would orphan) as the body.</para>
    /// </summary>
    internal static void MapServerTags(WebApplication app, NpgsqlDataSource postgres, ILogger logger)
    {
        var store = new ServerTagStore(postgres, DarlingMcpServerTagTools.WriteCommandSeconds);

        app.MapPost("/api/server-tags", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            if (!TryReadCreateBody(body, out var name, out var parentId, out var colour, out var createError))
            {
                return ErrorResult(createError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.CreateServerTagCore(store, name, parentId, colour, context.RequestAborted);
            LogServerTagWrite(logger, context, "create", null, result, 0);
            return ServerTagToolResult(result, "/api/server-tags", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        app.MapPatch("/api/server-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string changes;
            try
            {
                changes = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.UpdateServerTagCore(store, id, changes, context.RequestAborted);
            LogServerTagWrite(logger, context, "update", id, result, 0);
            return ServerTagToolResult(result, "/api/server-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/server-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (confirm, confirmRefusal) = ParseConfirm(context.Request.Query);
            if (confirmRefusal is not null)
            {
                return ErrorResult(confirmRefusal, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.DeleteServerTagCore(store, id, confirm, context.RequestAborted);
            LogServerTagWrite(logger, context, "delete", id, result, 0);
            return ServerTagToolResult(result, "/api/server-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapPost("/api/server-tags/{id:int}/servers", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (serverIds, idsError) = await ReadServerIdsAsync(context);
            if (serverIds is null)
            {
                return ErrorResult(idsError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.AssignServerTagCore(store, id, serverIds, context.RequestAborted);
            LogServerTagWrite(logger, context, "assign", id, result, serverIds.Count);
            return ServerTagToolResult(result, "/api/server-tags/{id}/servers", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/server-tags/{id:int}/servers", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (serverIds, idsError) = await ReadServerIdsAsync(context);
            if (serverIds is null)
            {
                return ErrorResult(idsError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpServerTagTools.UnassignServerTagCore(store, id, serverIds, context.RequestAborted);
            LogServerTagWrite(logger, context, "unassign", id, result, serverIds.Count);
            return ServerTagToolResult(result, "/api/server-tags/{id}/servers", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>
    /// The query-tag web routes (#5986), the same shape as <see cref="MapServerTags"/>: each write route parses its JSON
    /// body and hands it to the matching <c>DarlingMcpQueryTagTools.*Core</c> or <c>DarlingMcpQueryTagRuleTools.*Core</c> over
    /// a store on the viewer-role pool, so the page and the MCP tools share one set of rules and one set of answers.
    /// <c>application/json</c> is required on every write (the CSRF discipline of the other write routes). The admin gate is
    /// the seat's method gate (<see cref="DarlingWebSeat.IsRequestAllowed"/> refuses every non-GET from a read-only seat);
    /// the viewer role's grants on the three tag tables are only the floor beneath it. The writer's name is stamped on a
    /// new assignment or rule as <c>created_by</c> (the seat's <see cref="DarlingWebSeat.EditorPrincipal"/>), not <c>mcp</c>.
    ///
    /// <para><b>Tag routes.</b> <c>POST /api/query-tags</c> (<c>{name, parent_id?, colour?}</c>, 201);
    /// <c>PATCH /api/query-tags/{id}</c> (the body IS the partial <c>changes_json</c>);
    /// <c>DELETE /api/query-tags/{id}?confirm=true</c> (409 <c>confirm_required</c> with the counts until the flag rides along);
    /// <c>POST /api/query-tags/{id}/assignments</c> and <c>DELETE /api/query-tags/{id}/assignments</c>
    /// (<c>{kind, query_hash?, database_name?, schema_name?, object_name?, server_id?}</c>; assign also takes <c>query_id?</c>).
    /// Unassign is keyed by the assignment's key and server scope, as the MCP tool is: the tag id in the path is not part of
    /// the key.</para>
    ///
    /// <para><b>Rule routes.</b> <c>POST /api/query-tag-rules</c> (<c>{field, pattern?, tag_id?, priority?, enabled?, server_id?}</c>,
    /// 201); <c>PATCH /api/query-tag-rules/{id}</c> (the partial <c>changes_json</c>); <c>DELETE /api/query-tag-rules/{id}</c>;
    /// <c>GET /api/query-tag-rules[?server_id=]</c> (the list, in the order the rules apply) and
    /// <c>GET /api/query-tag-rules/test?...</c> (the dry run: <c>rule_id</c> or a draft's <c>field</c>, <c>pattern</c>,
    /// <c>tag_id</c>, <c>priority</c>, plus <c>server_id</c> and <c>window_hours</c>). The test is a GET, so a read-only seat
    /// can run it: it writes nothing, and it is admitted like any other read. Both reads are swept (#4348) because a rule's
    /// pattern and a sample's matched value are text.</para>
    /// </summary>
    internal static void MapQueryTags(WebApplication app, NpgsqlDataSource postgres, ILogger logger,
        ReadAdmission? readAdmission = null, ReadLatencyRecorder? readLatencyRecorder = null)
    {
        var tags = new QueryTagStore(postgres, Mcp.DarlingMcpQueryTagTools.WriteCommandSeconds);
        var rules = new QueryTagRuleStore(postgres, Mcp.DarlingMcpQueryTagRuleTools.CommandSeconds);

        app.MapPost("/api/query-tags", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "A query tag", "name", "parent_id", "colour");
            var name = reader.String("name");
            var parentId = reader.Int("parent_id");
            var colour = reader.String("colour");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.CreateQueryTagCore(tags, name, parentId, colour, context.RequestAborted);
            LogQueryTagWrite(logger, context, "create", null, result);
            return QueryTagToolResult(result, "/api/query-tags", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        app.MapPatch("/api/query-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string changes;
            try
            {
                changes = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.UpdateQueryTagCore(tags, id, changes, context.RequestAborted);
            LogQueryTagWrite(logger, context, "update", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/query-tags/{id:int}", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (confirm, bodyError) = ParseConfirm(context.Request.Query);
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.DeleteQueryTagCore(tags, id, confirm, context.RequestAborted);
            LogQueryTagWrite(logger, context, "delete", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapPost("/api/query-tags/{id:int}/assignments", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "An assignment", "kind", "query_hash", "database_name", "schema_name", "object_name", "server_id", "query_id");
            var kind = reader.String("kind");
            var queryHash = reader.String("query_hash");
            var databaseName = reader.String("database_name");
            var schemaName = reader.String("schema_name");
            var objectName = reader.String("object_name");
            var serverId = reader.Int("server_id");
            var queryId = reader.Long("query_id");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.AssignQueryTagCore(
                tags, id, kind, queryHash, databaseName, schemaName, objectName, serverId, queryId,
                DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted);
            LogQueryTagWrite(logger, context, "assign", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}/assignments", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/query-tags/{id:int}/assignments", async (HttpContext context, int id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "An unassignment", "kind", "query_hash", "database_name", "schema_name", "object_name", "server_id");
            var kind = reader.String("kind");
            var queryHash = reader.String("query_hash");
            var databaseName = reader.String("database_name");
            var schemaName = reader.String("schema_name");
            var objectName = reader.String("object_name");
            var serverId = reader.Int("server_id");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagTools.UnassignQueryTagCore(
                tags, kind, queryHash, databaseName, schemaName, objectName, serverId, context.RequestAborted);
            LogQueryTagWrite(logger, context, "unassign", id, result);
            return QueryTagToolResult(result, "/api/query-tags/{id}/assignments", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapPost("/api/query-tag-rules", async (HttpContext context) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var (body, bodyError) = await ReadJsonObjectAsync(context);
            if (body is null)
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var reader = new QueryTagBodyReader(body, "A query tag rule", "field", "pattern", "tag_id", "priority", "enabled", "server_id");
            var field = reader.String("field");
            var pattern = reader.String("pattern");
            var tagId = reader.Int("tag_id");
            var priority = reader.Int("priority");
            var enabled = reader.Bool("enabled");
            var serverId = reader.Int("server_id");
            bodyError = reader.Error;
            if (bodyError is not null)
            {
                return ErrorResult(bodyError, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.CreateCore(
                rules, field, pattern, tagId, priority ?? Mcp.DarlingMcpQueryTagRuleTools.DefaultPriority, enabled ?? true, serverId,
                DarlingWebSeat.FromContext(context).EditorPrincipal, context.RequestAborted);
            LogQueryTagWrite(logger, context, "rule-create", null, result);
            return QueryTagToolResult(result, "/api/query-tag-rules", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted, StatusCodes.Status201Created);
        });

        app.MapPatch("/api/query-tag-rules/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            string changes;
            try
            {
                changes = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
            }
            catch (InvalidDataException)
            {
                return ErrorResult("Request body is too large.", StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.UpdateCore(rules, id, changes, context.RequestAborted);
            LogQueryTagWrite(logger, context, "rule-update", id, result);
            return QueryTagToolResult(result, "/api/query-tag-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapDelete("/api/query-tag-rules/{id:long}", async (HttpContext context, long id) =>
        {
            if (!IsJsonContentType(context.Request.ContentType))
            {
                return UnsupportedMediaTypeResult();
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.DeleteCore(rules, id, context.RequestAborted);
            LogQueryTagWrite(logger, context, "rule-delete", id, result);
            return QueryTagToolResult(result, "/api/query-tag-rules/{id}", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapGet("/api/query-tag-rules", async (HttpContext context) =>
        {
            if (!TryReadQueryTagInt(context.Request.Query, "server_id", out var serverId, out var bodyError))
            {
                return ErrorResult(bodyError!, StatusCodes.Status400BadRequest);
            }

            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.ListCore(rules, serverId, context.RequestAborted);
            return ToHttpResult(result, "/api/query-tag-rules", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });

        app.MapGet("/api/query-tag-rules/test", async (HttpContext context) =>
        {
            var query = context.Request.Query;
            if (!TryReadQueryTagInt(query, "rule_id", out var ruleId, out var bodyError)
                || !TryReadQueryTagInt(query, "tag_id", out var tagId, out bodyError)
                || !TryReadQueryTagInt(query, "priority", out var priority, out bodyError)
                || !TryReadQueryTagInt(query, "server_id", out var serverId, out bodyError)
                || !TryReadQueryTagInt(query, "window_hours", out var windowHours, out bodyError)
                || !TrySingleQueryValue(query, "field", out var field)
                || !TrySingleQueryValue(query, "pattern", out var pattern))
            {
                return ErrorResult(bodyError ?? "Each parameter may be given once.", StatusCodes.Status400BadRequest);
            }

            /* The dry run reads up to 10,000 stored sightings like any collected-data read: admitted and recorded under its MCP twin's name. */
            using var readScope = ReadScope.Open(logger);
            using var ticket = await AdmitWebRouteAsync(readAdmission, "test_query_tag_rule", ReadWindowBand.None, context, readLatencyRecorder, readScope.Scope);
            if (ticket.Refusal is { } refusal)
            {
                return BusyResult(refusal);
            }

            using var run = new WebRouteRun(readLatencyRecorder, "test_query_tag_rule", ReadWindowBand.None, context, readScope.Scope);
            var stopwatch = Stopwatch.StartNew();
            var result = await Mcp.DarlingMcpQueryTagRuleTools.TestCore(
                rules, ruleId, field.Length == 0 ? null : field, pattern.Length == 0 ? null : pattern, tagId,
                priority ?? Mcp.DarlingMcpQueryTagRuleTools.DefaultPriority, serverId,
                windowHours ?? Mcp.DarlingMcpQueryTagRuleTools.DefaultWindowHours, TimeProvider.System, context.RequestAborted);
            run.Complete(ToolResultOutcome(result, context.RequestAborted));
            return ToHttpResult(result, "/api/query-tag-rules/test", logger, stopwatch.ElapsedMilliseconds, context.RequestAborted);
        });
    }

    /// <summary>An optional whole-number query parameter: absent or blank is null, given more than once or not a number is a refusal sentence.</summary>
    private static bool TryReadQueryTagInt(IQueryCollection query, string key, out int? value, out string? error)
    {
        value = null;
        error = null;
        if (!TrySingleQueryValue(query, key, out var text))
        {
            error = $"{key} must be given once.";
            return false;
        }

        if (text.Length == 0)
        {
            return true;
        }

        if (!int.TryParse(text, System.Globalization.NumberStyles.Integer, System.Globalization.CultureInfo.InvariantCulture, out var parsed))
        {
            error = $"{key} must be a whole number.";
            return false;
        }

        value = parsed;
        return true;
    }

    /// <summary>Who changed a query tag, assignment or rule is on the row only for assignments and rules; this line records the verb for
    /// every successful write. Verb and id only, never a name, pattern or body text.</summary>
    private static void LogQueryTagWrite(ILogger logger, HttpContext context, string verb, long? id, string result)
    {
        if (ServerTagEnvelopeStatus(result) is < 200 or >= 300)
        {
            return;
        }

        logger.LogInformation("Query tag {Verb} by {Principal}: id {Id}", verb, DarlingWebSeat.FromContext(context).EditorPrincipal, id);
    }

    /// <summary>The envelope pass-through the query-tag write routes share: the answer is swept first (#4348, a pattern or a
    /// name echoes text a caller typed), then answered exactly as <see cref="ServerTagToolResult"/> answers.</summary>
    internal static IResult QueryTagToolResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        int successStatus = StatusCodes.Status200OK)
    {
        /* #6341 round 1: a ServerError is answered with a fixed body and is not swept; the status and the one log line come from
           the tool's own result, never from the swept text. */
        if (ClassifyToolResponse(result) is ToolResponseKind.ServerError)
        {
            return ServerTagToolResult(result, route, logger, elapsedMs, requestAborted, successStatus);
        }

        var swept = DarlingWebStatementSweep.Apply(result);
        if (swept.Refused) return DarlingWebStatementSweep.Refusal(route, logger, elapsedMs, requestAborted);
        return ServerTagToolResult(result, route, logger, elapsedMs, requestAborted, successStatus, swept.Text);
    }

    /// <summary>Parses the request body as one JSON object, or names why it is not.</summary>
    private static async Task<(JsonObject? Body, string? Error)> ReadJsonObjectAsync(HttpContext context)
    {
        string text;
        try
        {
            text = await ReadBoundedBodyAsync(context, MaxServerTagBodyBytes);
        }
        catch (InvalidDataException)
        {
            return (null, "Request body is too large.");
        }

        try
        {
            var root = JsonNode.Parse(text);
            if (root is not JsonObject body)
            {
                return (null, "Request body must be a JSON object.");
            }

            /* A duplicate property name throws ArgumentException on the first enumeration; force it here so it
               answers 400 (a caller's malformed body), like the dismiss route. */
            _ = body.Count;
            return (body, null);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }
        catch (ArgumentException)
        {
            return (null, "Request body has a duplicate field.");
        }
    }

    /// <summary>The most request-body bytes a server-tag write route reads before it answers 400 (1000 ids is about 12 KB).</summary>
    internal const int MaxServerTagBodyBytes = 64 * 1024;

    /// <summary>Who changed tag scope is not stored on the row; this line is the only record. Verb, tag id and
    /// counts only, never a tag name or body text. Logged for a successful write (not invalid / refused).</summary>
    private static void LogServerTagWrite(ILogger logger, HttpContext context, string verb, int? tagId, string result, int serverCount)
    {
        if (ServerTagEnvelopeStatus(result) is < 200 or >= 300)
        {
            return;
        }

        logger.LogInformation(
            "Server tag {Verb} by {Principal}: tag {TagId}, servers {ServerCount}",
            verb, DarlingWebSeat.FromContext(context).EditorPrincipal, tagId, serverCount);
    }

    /// <summary>Reads <c>{name, parent_id?, colour?}</c>: only those keys, with their JSON types, so a stray or
    /// mistyped field is a 400 here rather than a silently dropped one.</summary>
    internal static bool TryReadCreateBody(JsonObject body, out string? name, out int? parentId, out string? colour, out string? error)
    {
        name = null;
        parentId = null;
        colour = null;
        error = null;

        foreach (var property in body)
        {
            if (property.Key is not ("name" or "parent_id" or "colour"))
            {
                error = $"Unknown field '{property.Key}'. A server tag takes name, parent_id and colour.";
                return false;
            }
        }

        if (body["name"] is JsonValue nameValue && nameValue.TryGetValue<string>(out var nameText))
        {
            name = nameText;
        }
        else if (body["name"] is not null)
        {
            error = "name must be a string.";
            return false;
        }

        if (body["parent_id"] is JsonNode parentNode)
        {
            if (parentNode is JsonValue parentValue && parentValue.TryGetValue<int>(out var parent))
            {
                parentId = parent;
            }
            else
            {
                error = "parent_id must be a whole number, or null for a root tag.";
                return false;
            }
        }

        if (body["colour"] is JsonNode colourNode)
        {
            if (colourNode is JsonValue colourValue && colourValue.TryGetValue<string>(out var colourText))
            {
                colour = colourText;
            }
            else
            {
                error = "colour must be a #RRGGBB string, or null for the palette colour.";
                return false;
            }
        }

        return true;
    }

    /// <summary>Reads <c>{server_ids: [int, ...]}</c> from the body, or names why it cannot.</summary>
    private static async Task<(List<int>? ServerIds, string? Error)> ReadServerIdsAsync(HttpContext context)
    {
        var (body, error) = await ReadJsonObjectAsync(context);
        if (body is null)
        {
            return (null, error);
        }

        if (body.Count != 1 || body["server_ids"] is not JsonArray array)
        {
            return (null, "Request body must be exactly {\"server_ids\": [whole numbers]}.");
        }

        var ids = new List<int>(array.Count);
        foreach (var element in array)
        {
            if (element is JsonValue value && value.TryGetValue<int>(out var id))
            {
                ids.Add(id);
            }
            else
            {
                return (null, "server_ids must hold only whole numbers.");
            }
        }

        return (ids, null);
    }

    /// <summary>
    /// The HTTP status a server-tag verb's returned string maps to: the mute-rule mapping
    /// (<see cref="MuteRuleEnvelopeStatus"/>: <c>invalid</c> 400, <c>not_found</c> 404, the caught-exception
    /// envelope 500, any other envelope the success status) plus the two statuses the tag verbs add —
    /// <c>conflict</c> (a sibling already holds the name) and <c>confirm_required</c> (a delete that would orphan
    /// custom alert rules until repeated with <c>confirm=true</c>) — both 409. The body is untouched either way,
    /// so <c>affected_rules</c> reaches the caller.
    /// </summary>
    internal static int ServerTagEnvelopeStatus(string result, int successStatus = StatusCodes.Status200OK)
    {
        if (ClassifyToolResponse(result) is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal)
        {
            try
            {
                var status = JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null;
                if (status is "conflict" or "confirm_required")
                {
                    return StatusCodes.Status409Conflict;
                }
            }
            catch (JsonException)
            {
                /* Not a shape the cores produce; the mute-rule mapping below answers it. */
            }
        }

        var mapped = MuteRuleEnvelopeStatus(result, successStatus);
        if (mapped != successStatus)
        {
            return mapped;
        }

        /* Allow-list: only a known success status answers 2xx; a status a future verb adds is a 500 until it is
           mapped here, never a silent 200. */
        try
        {
            var status = JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null;
            return status is "created" or "updated" or "unchanged" or "deleted" or "assigned" or "unassigned" or "moved"
                ? successStatus
                : StatusCodes.Status500InternalServerError;
        }
        catch (JsonException)
        {
            return StatusCodes.Status500InternalServerError;
        }
    }

    /// <summary>The envelope pass-through the server-tag routes share: <see cref="MuteRuleToolResult"/>'s error
    /// handling (the caught-exception sentence never reaches the wire) with <see cref="ServerTagEnvelopeStatus"/>'s
    /// status.</summary>
    internal static IResult ServerTagToolResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        int successStatus = StatusCodes.Status200OK, string? writtenBody = null)
    {
        var kind = ClassifyToolResponse(result);
        if (kind is ToolResponseKind.ServerError)
        {
            return ServerErrorResult(McpHelpers.ErrorMessageOf(result), route, logger, elapsedMs, requestAborted);
        }

        /* #6341 round 1: the status comes from the tool's own result; writtenBody (the swept text) only replaces what is written. */
        var httpStatus = ServerTagEnvelopeStatus(result, successStatus);
        var body = writtenBody ?? result;
        return kind is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal
            ? Results.Text(body, "application/json", statusCode: httpStatus)
            : ErrorResult(McpHelpers.ErrorMessageOf(body), httpStatus);
    }

    /// <summary>Reads the raw request body text — what the mute-rule cores parse themselves, so the web layer
    /// adds no parse of its own to drift from theirs.</summary>
    private static async Task<string> ReadBodyAsync(HttpContext context)
    {
        using var reader = new StreamReader(context.Request.Body);
        return await reader.ReadToEndAsync(context.RequestAborted);
    }

    /// <summary>
    /// Maps a mute-rule verb's returned string onto the HTTP status the web surface answers with, leaving the
    /// body untouched: <c>invalid</c> → 400, <c>not_found</c> → 404, <c>already_exists</c> → 409 (#4734: a create
    /// that repeats a rule already in force is a conflict, the status the views and custom-rule routes give theirs;
    /// the body is still the verb's envelope, carrying the existing rule's id), any other envelope (created /
    /// updated / unchanged / deleted) → <paramref name="successStatus"/>; the cores' caught-exception envelope
    /// (<c>McpHelpers.FormatError</c>, <c>{"status":"error", ...}</c>) → 500 (classified by
    /// <see cref="ClassifyToolResponse"/>, like the read surface, and BEFORE the status switch below so the
    /// failure word is never read as a verb outcome); any other bare string is a shape the cores do not produce and maps to the client-correctable
    /// 400 for the reason the read surface's mapping does. <c>unchanged</c> deliberately shares the success
    /// code: it is the retry-safe "already so" answer, and the envelope's own <c>status</c> field carries the
    /// distinction a caller might act on. Pure, so the whole table pins without a server.
    ///
    /// <para>Since #3739 the <c>invalid</c> → 400 arm is answered by the classifier's <c>Refusal</c> kind — the
    /// same recognizer the read surface uses, so the two surfaces cannot disagree about the word — and the
    /// <c>"invalid"</c> case in the parsed switch below is kept as the belt-and-braces for an envelope that was
    /// serialized some other way (whitespace, a different key order) and so missed the prefix test.</para>
    /// </summary>
    internal static int MuteRuleEnvelopeStatus(string result, int successStatus = StatusCodes.Status200OK)
    {
        switch (ClassifyToolResponse(result))
        {
            case ToolResponseKind.ServerError:
                return StatusCodes.Status500InternalServerError;
            case ToolResponseKind.Refusal:
            case ToolResponseKind.ClientError:
                return StatusCodes.Status400BadRequest;
        }

        try
        {
            var status = JsonNode.Parse(result) is JsonObject envelope ? TryGetString(envelope, "status") : null;
            return status switch
            {
                "invalid" => StatusCodes.Status400BadRequest,
                "not_found" => StatusCodes.Status404NotFound,
                "already_exists" => StatusCodes.Status409Conflict,
                _ => successStatus,
            };
        }
        catch (JsonException)
        {
            /* A '{'-leading string that does not parse is not a shape the cores produce — refuse to claim
               success over a body nobody can read. */
            return StatusCodes.Status500InternalServerError;
        }
    }

    /// <summary>The envelope pass-through the mute-rule routes share: the verb's own body, verbatim, under the
    /// status <see cref="MuteRuleEnvelopeStatus"/> assigns — a refusal (<c>invalid</c>) included, which is the
    /// shape the read surface adopted from here in #3739.
    ///
    /// <para><b>#4283: the ServerError arm never puts the tool's caught-exception text on the wire.</b> Classified
    /// FIRST, before <see cref="MuteRuleEnvelopeStatus"/> ever runs, and answered through
    /// <see cref="ServerErrorResult"/> exactly as <see cref="ToHttpResult"/> answers the read surface's — logged
    /// ONCE, the body a fixed message, 503 when the sentence carries a caught statement_timeout's 57014 token,
    /// 500 otherwise. Any other error (a bare non-JSON string) is still wrapped as <c>{"error": sentence}</c>.</para>
    /// </summary>
    internal static IResult MuteRuleToolResult(string result, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted,
        int successStatus = StatusCodes.Status200OK, SensitiveStatements.JudgeBudget? sweepBudget = null)
    {
        /* #6341 round 1: classified, and the status read, from the tool's own result BEFORE the sweep, like ToHttpResult: the
           sweep can withhold the status word and must not decide the status or whether the fault is logged. */
        var kind = ClassifyToolResponse(result);
        if (kind is ToolResponseKind.ServerError)
        {
            return ServerErrorResult(McpHelpers.ErrorMessageOf(result), route, logger, elapsedMs, requestAborted);
        }

        var httpStatus = MuteRuleEnvelopeStatus(result, successStatus);

        /* #4348: swept over the body that is written. A mute rule echoes the pattern it was given, which can be a statement. */
        var swept = DarlingWebStatementSweep.Apply(result, sweepBudget);
        if (swept.Refused) return DarlingWebStatementSweep.Refusal(route, logger, elapsedMs, requestAborted);

        return kind is ToolResponseKind.JsonPassthrough or ToolResponseKind.Refusal
            ? Results.Text(swept.Text, "application/json", statusCode: httpStatus)
            : ErrorResult(McpHelpers.ErrorMessageOf(swept.Text), httpStatus);
    }

    /// <summary>#6390 S2c: what the service gives the composed-panel run core, which lives in Storage: its own server-name resolver
    /// (with the refusals and peer lookups), registry lookup, store-setting deadline read and the web data-start fields.</summary>
    internal static readonly ComposeRunHost ComposeHost = new(
        ComposeServerScope.ResolveNamesAsync,
        ComposeServerScope.FindUnregisteredAsync,
        McpCommandDeadlines.ResolveComposedQuerySecondsAsync,
        WebDataStartNote.AddComposedPanelInstants);

    /// <summary>Seconds of client-deadline headroom the web compose path adds; the value and its reasoning live with the run core (#6390 S2c).</summary>
    internal const int ComposeClientDeadlineHeadroomSeconds = ComposePanelRunner.ComposeClientDeadlineHeadroomSeconds;

    /// <summary>The schema-version probe the run core reads; the text lives with the run core (#6390 S2c).</summary>
    internal const string QueryStoreWideSchemaVersionSql = ComposePanelRunner.QueryStoreWideSchemaVersionSql;

    /// <summary>Default compose-run window (hours) when the request omits one; the value lives with the run core (#6390 S2c).</summary>
    internal const int DefaultComposeHours = ComposePanelRunner.DefaultComposeHours;

    /// <summary>
    /// Compile-and-run a single composed panel spec (Custom Views v2, #1563) against <paramref name="postgres"/>
    /// — the ONE runner shared by the web <c>/api/compose/run</c> endpoint and the MCP <c>run_custom_view_panel</c>
    /// tool, so the two surfaces can never diverge on validation, compilation, or query shape. Takes an
    /// already-parsed request body <c>{panel, variables?, values?, server?, hours?}</c> and returns a
    /// <see cref="ComposeRunOutcome"/> the caller maps to its surface. The panel is validated by the SAME
    /// <see cref="ComposeSpec.TryParsePanel"/> authority the write path uses (so a panel that runs here is a
    /// panel that stores), the compiler emits only catalog identifiers — schema-qualified <c>collect.*</c>, every
    /// value bound — and the query runs under the pool's role <c>statement_timeout</c> backstop. A cancellation
    /// (<see cref="OperationCanceledException"/>) is deliberately NOT caught: it propagates to the caller as a
    /// client-abort, exactly as the endpoint has always done.
    ///
    /// <para><paramref name="readLatency"/> (#4782) is the read-latency seat this run is recorded into: the web
    /// route passes the one its own <see cref="MapAll"/> call built, the MCP tool the one its host registered.
    /// Null records nothing (a test calling the runner directly).</para>
    ///
    /// <para><paramref name="includeDataStartFields"/> adds the data-start sentence and the instants it names beside
    /// <c>notice</c> (<see cref="WebDataStartNote.AddComposedPanelInstants"/>), so the page can write the sentence again
    /// in the browser's zone. Only the web route sets it; the MCP tool leaves it off, so its client reads the sentence
    /// once, in <c>notice</c>.</para>
    /// </summary>
    internal static async Task<ComposeRunOutcome> RunComposedPanelAsync(
        NpgsqlDataSource postgres, JsonObject body, System.Threading.CancellationToken cancellationToken,
        ReadLatencyRecorder? readLatency = null, int clientDeadlineHeadroomSeconds = 0, bool remapClientTimeout = false, bool includeDataStartFields = false,
        Action<Exception>? onRunException = null, DateTime? nowUtc = null, string? caller = null, ReadSurface? cacheHost = null)
    {
        /* #5506 part 3 (round 1, C1): a heavy panel is answered from the read cache, joins an identical run in flight, or
           runs once and is stored, as /api/read and the MCP reads are. The host that runs it (cacheHost: web or mcp) is
           part of the key (D4): the two serialize the payload differently and the web adds its data-start fields, so
           their entries are never shared. What is stored is the exact text the host answers (PayloadText), so a hit is
           byte-identical to the run that stored it. A hit skips admission. */
        var admission = readLatency?.Admission;
        if (cacheHost is { } host && admission is not null
            && admission.IsCached(ReadSurface.Compose, ComposeRouteLabel(body), ComposeWindowBand(body)))
        {
            return await RunComposedPanelCachedAsync(postgres, body, readLatency!, admission, host,
                clientDeadlineHeadroomSeconds, remapClientTimeout, includeDataStartFields, onRunException, nowUtc, caller, cancellationToken);
        }

        return await RunComposedPanelAdmittedAsync(postgres, body, readLatency, clientDeadlineHeadroomSeconds,
            remapClientTimeout, includeDataStartFields, onRunException, nowUtc, caller, cacheHost, cancellationToken);
    }

    private static async Task<ComposeRunOutcome> RunComposedPanelCachedAsync(
        NpgsqlDataSource postgres, JsonObject body, ReadLatencyRecorder readLatency,
        ReadAdmission admission, ReadSurface host, int clientDeadlineHeadroomSeconds, bool remapClientTimeout, bool includeDataStartFields,
        Action<Exception>? onRunException, DateTime? nowUtc, string? caller, System.Threading.CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();
        using var readScope = ReadScope.Open(readLatency.Logger);
        var composeRoute = ComposeRouteLabel(body);
        var composeBand = ComposeWindowBand(body);
        var recordCaller = caller ?? ReadCaller.AmbientMcpCaller;
        var cache = admission.Cache;
        var ttl = ReadResultCache.TtlOf(composeBand);
        var key = ReadCacheKey.ForCompose(host, composeRoute, body.ToJsonString(), cache.Generation, cache, ttl);
        if (cache.TryGet(key, out var stored))
        {
            /* Round 2, L3: a hit runs no read, so it does not count as one in flight. */
            readScope.Scope.Uncount();
            admission.NoteCacheHit(ReadSurface.Compose, composeRoute);
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.CacheHit, stopwatch.ElapsedMilliseconds, composeBand, readScope.Scope, recordCaller);
            return ComposeRunOutcome.FromCache(stored);
        }

        /* D6: the run is shared; the caller that starts it records its real outcome inside the run. Round 2, M1: a caller
           that joined records only its own outcome with its own elapsed time (busy, cancelled, or cache_hit for the shared
           answer), never the run's, so one run counts once. */
        var flight = cache.Join(key, ttl,
            token => RunComposedPanelAdmittedAsync(postgres, body, readLatency, clientDeadlineHeadroomSeconds, remapClientTimeout,
                includeDataStartFields, onRunException, nowUtc, caller, host, token),
            run => run.PayloadText is { } text && ReadResultCache.IsCacheableText(text, gcf: false) ? text : null);
        if (flight.IsLead)
        {
            admission.NoteCacheMiss(ReadSurface.Compose, composeRoute);
            return await flight.WaitAsync(cancellationToken);
        }

        /* Round 2, L3: a joined caller runs no read while it waits for the shared one. */
        readScope.Scope.Uncount();
        try
        {
            var shared = await flight.WaitAsync(cancellationToken);
            var joinedOutcome = ReadResultCache.JoinedOutcome(shared.Busy is not null ? ReadOutcome.Busy : null, cancellationToken);
            if (joinedOutcome == ReadOutcome.CacheHit)
            {
                admission.NoteCacheHit(ReadSurface.Compose, composeRoute);
            }

            RecordAdmissionSample(readLatency, composeRoute, joinedOutcome, stopwatch.ElapsedMilliseconds, composeBand, readScope.Scope, recordCaller);
            return shared;
        }
        catch (Exception ex)
        {
            var thrown = ReadResultCache.JoinedOutcome(
                ReadOutcomeClassifier.Classify(ex, cancellationToken) == ReadOutcome.Cancelled ? ReadOutcome.Cancelled : null, cancellationToken);
            if (thrown == ReadOutcome.CacheHit)
            {
                admission.NoteCacheHit(ReadSurface.Compose, composeRoute);
            }

            RecordAdmissionSample(readLatency, composeRoute, thrown, stopwatch.ElapsedMilliseconds, composeBand, readScope.Scope, recordCaller);
            throw;
        }
    }

    private static async Task<ComposeRunOutcome> RunComposedPanelAdmittedAsync(
        NpgsqlDataSource postgres, JsonObject body,
        ReadLatencyRecorder? readLatency, int clientDeadlineHeadroomSeconds, bool remapClientTimeout, bool includeDataStartFields,
        Action<Exception>? onRunException, DateTime? nowUtc, string? caller, ReadSurface? cacheHost, System.Threading.CancellationToken cancellationToken)
    {
        /* #4442 scope 2: recorded ONCE per call, here, so the web /api/compose/run route and the MCP
           run_custom_view_panel tool -- both of which call this ONE runner -- contribute exactly one
           'compose' sample each, never two. The route label is the panel's source measure key when the body
           parses far enough to have one, else the fixed label below; both are bounded-cardinality (the
           catalog's own measure keys), never the caller's raw JSON. Recording never throws into the caller:
           a bucket update is the only work in the try, and any failure there is swallowed and logged at
           Debug, exactly like the web loop's own recording. */
        var stopwatch = Stopwatch.StartNew();
        using var readScope = ReadScope.Open(readLatency?.Logger);
        readScope.Scope.CaptureStatements = true;

        /* #5506 part 3: admission, after the scope and the band, under the same route label and band the run records.
           A refusal records busy, a caller that leaves the queue records cancelled, and a queued run records its wait as
           one queue_wait sample; the recorded elapsed time starts after admission (D1). */
        var admission = readLatency?.Admission;
        var composeRoute = ComposeRouteLabel(body);
        var composeBand = ComposeWindowBand(body);
        using var ticket = admission is null
            ? ReadAdmissionTicket.Passed
            : await ReadScope.AdmitAsync(admission, ReadSurface.Compose, composeRoute, composeBand, cancellationToken);
        if (ticket.Refusal is { } refusal)
        {
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.Busy, ticket.QueueMs ?? 0, composeBand, readScope.Scope, caller ?? ReadCaller.AmbientMcpCaller);
            return ComposeRunOutcome.Refused(refusal);
        }

        if (ticket.CallerLeft)
        {
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.Cancelled, ticket.QueueMs ?? 0, composeBand, readScope.Scope, caller ?? ReadCaller.AmbientMcpCaller);
            cancellationToken.ThrowIfCancellationRequested();
        }

        if (ticket.QueueMs is { } queueMs)
        {
            RecordAdmissionSample(readLatency, composeRoute, ReadOutcome.QueueWait, queueMs, composeBand, readScope.Scope, caller ?? ReadCaller.AmbientMcpCaller);
        }

        stopwatch.Restart();
        var outcome = await ComposePanelRunner.RunComposedPanelCoreAsync(ComposeHost, postgres, body, clientDeadlineHeadroomSeconds, remapClientTimeout, includeDataStartFields, cancellationToken, readLatency?.Logger, onRunException, nowUtc);
        /* Round 1, C1: the text the host answers, taken once here so a cached copy of it is the same bytes. */
        if (cacheHost is { } host && outcome.Payload is { } payload)
        {
            outcome = outcome with { PayloadText = host == ReadSurface.Mcp ? payload.ToJsonString(McpHelpers.JsonOptions) : payload.ToJsonString() };
        }

        /* #5793: the web route passes its session hash; the MCP tool passes none, and the tool filter's ambient caller
           names the client instead. */
        RecordComposeLatency(readLatency, body, outcome, stopwatch.ElapsedMilliseconds, cancellationToken, readScope.Fallback, readScope.Scope,
            caller ?? ReadCaller.AmbientMcpCaller);
        return outcome;
    }

    /// <summary>The composed-panel route label recorded against <see cref="ReadLatencyAccumulator"/>: the
    /// panel's own measure key when the body parsed that far, else a fixed catch-all -- bounded cardinality
    /// either way (the compose catalog's own measure keys, never the caller's free text).</summary>
    private const string ComposeUnknownRouteLabel = "compose:unknown";

    /// <summary>#5506 part 3 (round 2, L5): the MCP twin <c>/api/catalog</c> is admitted and recorded under.</summary>
    internal const string CatalogRoute = "describe_custom_view_catalog";

    /// <summary>The route name the live Active Queries result read is admitted and recorded under (#6386).</summary>
    internal const string ActiveQueriesLiveResultRoute = "active_queries_live_result";

    /// <summary>#5506 part 3 (round 1, M3): the name <c>/api/ag</c> is admitted and recorded under, its MCP twin's.</summary>
    internal const string AgHealthRoute = "get_ag_health";

    /// <summary>#5506 part 3: the web answer to a refused read: 429, a <c>Retry-After</c> header, and <c>{"error": sentence}</c>,
    /// which the page already shows (<c>wwwroot/js/util.js</c> reads <c>body.error</c>).</summary>
    internal static IResult BusyResult(ReadAdmissionRefusal refusal) =>
        new RetryAfterResult(ErrorResult(refusal.Sentence, StatusCodes.Status429TooManyRequests), refusal.RetryAfterSeconds);

    /// <summary>An answer with a <c>Retry-After</c> header, set on the response it is written to (round 1, C3): a busy
    /// answer made by a shared read reaches every caller of that read with the header, not only the first.</summary>
    private sealed class RetryAfterResult(IResult inner, int retryAfterSeconds) : IResult
    {
        public Task ExecuteAsync(HttpContext httpContext)
        {
            httpContext.Response.Headers.RetryAfter = retryAfterSeconds.ToString(CultureInfo.InvariantCulture);
            return inner.ExecuteAsync(httpContext);
        }
    }

    /// <summary>
    /// #5506 part 3 (round 1, C3): a copy of a read request for a read several callers share. It carries what a read
    /// handler and the recording read (method, path, query string, headers and cookies, user, services, remote address)
    /// and <paramref name="readToken"/> as its <c>RequestAborted</c>, so the shared read runs under the flight's token while
    /// the caller's own request keeps its own, and nothing the read does (its <c>as_of</c> added to the query string)
    /// changes the caller's request.
    /// </summary>
    internal static HttpContext DetachedReadContext(HttpContext source, CancellationToken readToken)
    {
        ArgumentNullException.ThrowIfNull(source);
        var copy = new DefaultHttpContext
        {
            RequestAborted = readToken,
            User = source.User,
            RequestServices = source.RequestServices,
        };
        var from = source.Request;
        var to = copy.Request;
        to.Method = from.Method;
        to.Scheme = from.Scheme;
        to.Host = from.Host;
        to.PathBase = from.PathBase;
        to.Path = from.Path;
        to.QueryString = from.QueryString;
        foreach (var header in from.Headers)
        {
            to.Headers[header.Key] = header.Value;
        }

        copy.Connection.RemoteIpAddress = source.Connection.RemoteIpAddress;
        return copy;
    }

    /// <summary>#5506 part 3: admits a dedicated web read route (<c>/api/fleet</c>, <c>/api/triage</c>,
    /// <c>/api/alerts/test</c>) and, given a recorder and the route's open scope, records its admission sample under the
    /// <c>web</c> surface and <paramref name="route"/> as the read loop does: <c>busy</c> when refused, <c>cancelled</c> when
    /// the browser left the queue, <c>queue_wait</c> when it waited. The run itself is recorded by <see cref="WebRouteRun"/>.
    /// A browser that leaves the queue ends the request, as the read loop's #4276 arm does.</summary>
    internal static Task<ReadAdmissionTicket> AdmitWebRouteAsync(ReadAdmission? admission, string route, string band, HttpContext context,
        ReadLatencyRecorder? recorder = null, ReadScope? scope = null)
    {
        if (admission is null)
        {
            return Task.FromResult(ReadAdmissionTicket.Passed);
        }

        /* Not async (round 1, L4): AdmitAsync sets the nesting marker in the CALLER's flow, and an async method undoes
           every AsyncLocal it sets when it returns, so a read nested under this route would take a second slot (a
           deadlock at a limit of 1). Only the recording below runs in an async continuation; it sets no AsyncLocal. */
        return RecordAsync(ReadScope.AdmitAsync(admission, ReadSurface.Web, route, band, context.RequestAborted));

        async Task<ReadAdmissionTicket> RecordAsync(Task<ReadAdmissionTicket> admitting)
        {
            var ticket = await admitting;
            if (ticket.Refusal is not null)
            {
                RecordWebRouteSample(recorder, route, ReadOutcome.Busy, ticket.QueueMs ?? 0, band, scope, context);
            }
            else if (ticket.CallerLeft)
            {
                RecordWebRouteSample(recorder, route, ReadOutcome.Cancelled, ticket.QueueMs ?? 0, band, scope, context);
                ticket.Dispose();
                context.RequestAborted.ThrowIfCancellationRequested();
            }
            else if (ticket.QueueMs is { } queueMs)
            {
                RecordWebRouteSample(recorder, route, ReadOutcome.QueueWait, queueMs, band, scope, context);
            }

            return ticket;
        }
    }

    /// <summary>#5506 part 3: one sample of a dedicated web read route under the <c>web</c> surface. Never throws into the
    /// request; a missing recorder or scope records nothing.</summary>
    internal static void RecordWebRouteSample(ReadLatencyRecorder? recorder, string route, ReadOutcome outcome, long elapsedMs, string band, ReadScope? scope, HttpContext context)
    {
        if (recorder is null || scope is null)
        {
            return;
        }

        try
        {
            recorder.Accumulator?.Record(ReadSurface.Web, route, outcome, elapsedMs, band, scope.InFlightAtStart, scope.WaitMs, WebReadCaller(context));
        }
        catch (Exception ex)
        {
            recorder.Logger?.LogDebug(ex, "Read-latency recording failed for the web route {Route}.", route);
        }
    }

    /// <summary>The outcome of a tool answer, as the read loop classifies it: a server-error envelope by its sentence
    /// (a caught 57014 is a timeout), anything else <c>ok</c>.</summary>
    internal static ReadOutcome ToolResultOutcome(string result, System.Threading.CancellationToken requestAborted) =>
        ClassifyToolResponse(result) == ToolResponseKind.ServerError
            ? ReadOutcomeClassifier.ClassifySentence(McpHelpers.ErrorMessageOf(result), requestAborted)
            : ReadOutcome.Ok;

    /// <summary>
    /// #5506 part 3: the run sample of a dedicated web read route (<c>/api/fleet</c>, <c>/api/triage</c>,
    /// <c>/api/alerts/test</c>), so the long-read table (<see cref="ReadAdmission"/>) learns these routes from their own
    /// p90 instead of the window-size rule alone. Created once the read may run, so the elapsed time starts after
    /// admission (D1). Disposing records once: the outcome given to <see cref="Complete"/>, a fallback the scope noted on an
    /// <c>ok</c>, or, when the route threw before completing, <c>cancelled</c> for a browser that left and <c>error</c>
    /// otherwise.
    /// </summary>
    internal sealed class WebRouteRun : IDisposable
    {
        private readonly ReadLatencyRecorder? _recorder;
        private readonly string _route;
        private readonly string _band;
        private readonly HttpContext _context;
        private readonly ReadScope _scope;
        private readonly Stopwatch _stopwatch = Stopwatch.StartNew();
        private ReadOutcome? _outcome;
        private int _recorded;

        internal WebRouteRun(ReadLatencyRecorder? recorder, string route, string band, HttpContext context, ReadScope scope)
        {
            _recorder = recorder;
            _route = route;
            _band = band;
            _context = context;
            _scope = scope;
        }

        /// <summary>The outcome the route answered with.</summary>
        internal void Complete(ReadOutcome outcome) => _outcome = outcome;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _recorded, 1) != 0)
            {
                return;
            }

            var outcome = _outcome ?? (_context.RequestAborted.IsCancellationRequested ? ReadOutcome.Cancelled : ReadOutcome.Error);
            RecordWebRouteSample(_recorder, _route, ReadScope.Resolve(outcome, _scope.Fallback), _stopwatch.ElapsedMilliseconds, _band, _scope, _context);
        }
    }

    /// <summary>#5506 part 3: one admission sample of a composed-panel run (busy, cancelled in the queue, or queue_wait).</summary>
    private static void RecordAdmissionSample(ReadLatencyRecorder? recorder, string route, ReadOutcome outcome, long elapsedMs, string band, ReadScope scope, string? caller)
    {
        try
        {
            recorder?.Accumulator?.Record(ReadSurface.Compose, route, outcome, elapsedMs, band, scope.InFlightAtStart, 0, caller);
        }
        catch (Exception ex)
        {
            recorder?.Logger?.LogDebug(ex, "Read-latency recording failed for a composed-panel admission.");
        }
    }

    /// <summary>Records one <c>/api/read/&lt;name&gt;</c> sample under the <c>web</c> surface -- <paramref
    /// name="name"/> is already a bounded-cardinality route label (the dispatch table's own tool name, never
    /// caller-supplied text). Never throws into the request: swallowed and logged at Debug, exactly like the
    /// compose path's own recording.</summary>
    private static void RecordWebReadLatency(ReadLatencyRecorder recorder, string name, ReadOutcome outcome, long elapsedMs, HttpContext context, ReadScope scope)
    {
        try
        {
            /* #5506: the band of the window the page asked for (hours, hours_back or days_back on the query string). A
               request that sends none reads the read's own default window, which the catalog row declares (PHours(def),
               PInt("days_back", def)), so it records that window's band; only a read that takes no window records none. */
            var band = WebWindowBand(name, key => First(context, key));
            recorder.Accumulator?.Record(ReadSurface.Web, name, outcome, elapsedMs, band, scope.InFlightAtStart, scope.WaitMs, WebReadCaller(context));
        }
        catch (Exception ex)
        {
            recorder.Logger?.LogDebug(ex, "Read-latency recording failed for /api/read/{Route}.", name);
        }
    }

    /// <summary>The caller of a web or composed-panel read (#5793): <see cref="ReadCaller.OfWebSession(string?)"/> over the
    /// request's session key, a salted hash. Only the hash leaves this method; the key itself is never recorded.</summary>
    internal static string WebReadCaller(HttpContext context)
        => ReadCaller.OfWebSession(context.Request.Cookies[Mcp.DarlingWebHostService.SessionCookieName]);

    /// <summary>The window band of one <c>/api/read/&lt;name&gt;</c> request (#5506): the window the query string sent, else
    /// the default <see cref="CatalogDescriptors"/> declares for that read, else <c>none</c>. Internal so a test can pin a
    /// request that sends no window against the catalog's default.</summary>
    internal static string WebWindowBand(string name, Func<string, string?> sent) =>
        ReadWindowBand.OrFixed(ReadWindowBand.Of(WebWindowHours(name, sent)), name);

    /// <summary>The window in hours of web read <paramref name="name"/>: the one the query string sent, else the default the catalog
    /// row declares (the number <see cref="WebWindowBand"/> bands); null when there is none.</summary>
    internal static double? WebWindowHours(string name, Func<string, string?> sent) =>
        ReadWindowBand.HoursOf(sent, key => CatalogDefaultText(name, key));

    /// <summary>The default the catalog row of read <paramref name="name"/> declares for parameter <paramref name="key"/>,
    /// as text; null when the read is not in the catalog, does not take that parameter, or declares no default.</summary>
    private static string? CatalogDefaultText(string name, string key)
    {
        if (!CatalogDescriptors.TryGetValue(name, out var read))
        {
            return null;
        }

        foreach (var parameter in read.Params)
        {
            if (string.Equals(parameter.Name, key, StringComparison.Ordinal))
            {
                return ReadWindowBand.DefaultText(parameter.Default);
            }
        }

        return null;
    }

    /// <summary>The query string as slow-read arguments. A key name is caller text: only plain identifier characters
    /// ([A-Za-z0-9_.-]) are stored, capped at 64; any other key is dropped and counted under <c>_dropped_keys</c>. A key that
    /// repeats is stored as a JSON array of its values in the order sent; a key sent once is a plain value.</summary>
    internal static JsonObject WebQueryArguments(IEnumerable<KeyValuePair<string, string?>> query)
    {
        var arguments = new JsonObject();
        var droppedKeys = 0;
        foreach (var (key, value) in query)
        {
            if (!key.All(c => c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '_' or '.' or '-'))
            {
                droppedKeys++;
                continue;
            }

            var stored = key.Length > 64 ? key[..64] : key;
            if (!arguments.TryGetPropertyValue(stored, out var existing))
            {
                arguments[stored] = value;
            }
            else if (existing is JsonArray repeated)
            {
                repeated.Add(value);
            }
            else
            {
                /* A key sent more than once records every value, in the order sent, so the record shows the request as it
                   was made (#5245); a key sent once stays a plain value. */
                arguments[stored] = new JsonArray(existing?.DeepClone(), JsonValue.Create(value));
            }
        }

        if (droppedKeys > 0)
        {
            arguments["_dropped_keys"] = droppedKeys;
        }

        return arguments;
    }

    /// <summary>The composed-panel route label: <c>compose:</c> plus the panel's source when the catalog knows it,
    /// else <c>compose:unknown</c>, held to 120 characters.</summary>
    internal static string ComposeRouteLabel(JsonObject body)
    {
        var route = body["panel"] is JsonObject panel && panel["source"] is JsonValue sourceValue
            && sourceValue.TryGetValue<string>(out var source) && MeasureCatalog.IsKnownSource(source)
            ? "compose:" + source
            : ComposeUnknownRouteLabel;
        return route.Length > 120 ? route[..120] : route;
    }

    /// <summary>Offers a slow or failed <c>/api/read/*</c> call to the slow-read record (#5097): the query string, as sent,
    /// becomes the arguments. Never throws into the request.</summary>
    private static void OfferWebSlowRead(ReadLatencyRecorder recorder, ReadScope scope, string name, ReadOutcome outcome, long elapsedMs, HttpContext context, string? errorClass)
    {
        if (recorder.SlowReads is null || !recorder.SlowReads.ShouldRecord(outcome, elapsedMs))
        {
            return;
        }

        try
        {
            /* One pair per value, so a key sent more than once keeps every value (WebQueryArguments folds them into an array);
               a key with no value at all (a bare ?key) is one null pair, as before. */
            var arguments = WebQueryArguments(context.Request.Query.SelectMany(q => q.Value.Count > 0
                ? q.Value.Select(v => new KeyValuePair<string, string?>(q.Key, v))
                : [new KeyValuePair<string, string?>(q.Key, null)]));

            recorder.SlowReads.Offer(scope, ReadSurface.Web, name, outcome, elapsedMs, arguments, errorClass, recorder.Logger, WebReadCaller(context));
        }
        catch (Exception ex)
        {
            recorder.Logger?.LogDebug(ex, "Slow-read recording failed for /api/read/{Route}.", name);
        }
    }

    private static void RecordComposeLatency(ReadLatencyRecorder? recorder, JsonObject body, ComposeRunOutcome outcome, long elapsedMs, System.Threading.CancellationToken cancellationToken, ReadFallback? noted = null, ReadScope? scope = null, string? caller = null)
    {
        try
        {
            var measureKey = ComposeRouteLabel(body);

            /* Payload => Ok. outcome.Fault carries the real PostgresException for a store fault the panel
               author could not have caused -- classified the same way the web loop classifies any exception.
               outcome.AuthorSqlState is set only for an author-actionable PostgresException (#4283 M1/#4293
               R2), and that allow-list includes 57014 -- a panel query hitting the store's own
               statement_timeout is a Timeout sample even though it answers the caller at 400, not 500 -- and
               (#4605) 53400, the viewer/mcp role's temp_file_limit refusing the panel's own spill, which is a
               Limit sample for the same reason. Anything else that did not produce a payload (a validation
               BadRequest with no exception at all, or the generic-Exception ServerError arm) is Error, unless
               the caller's own token already explains it. */
            var readOutcome = outcome.Payload is not null
                ? ReadOutcome.Ok
                : outcome.AuthorSqlState == CollectorFaultCancelOrigin.QueryCanceled
                    ? ReadOutcome.Timeout
                    : outcome.AuthorSqlState == ReadOutcomeClassifier.ConfigurationLimitExceeded
                        ? ReadOutcome.Limit
                        : outcome.Fault is not null
                            ? ReadOutcomeClassifier.Classify(outcome.Fault, cancellationToken)
                            : cancellationToken.IsCancellationRequested
                                ? ReadOutcome.Cancelled
                                : ReadOutcome.Error;

            var resolved = ReadScope.Resolve(readOutcome, noted);
            recorder?.Accumulator?.Record(ReadSurface.Compose, measureKey, resolved, elapsedMs, ComposeWindowBand(body),
                scope?.InFlightAtStart ?? 0, scope?.WaitMs ?? 0, caller);

            /* #5097: a slow or failed run also becomes a slow-read row. The panel body is the arguments. */
            if (scope is not null && recorder?.SlowReads is { } slowReads && slowReads.ShouldRecord(resolved, elapsedMs))
            {
                var errorClass = outcome.Fault is not null ? SlowReadLog.ErrorClassOf(outcome.Fault)
                    : outcome.AuthorSqlState is { Length: > 0 } sqlState ? sqlState
                    : resolved == ReadOutcome.Error ? "compose_error" : null;
                slowReads.Offer(scope, ReadSurface.Compose, measureKey, resolved, elapsedMs, body.DeepClone() as JsonObject, errorClass, recorder.Logger, caller);
            }
        }
        catch (Exception ex)
        {
            recorder?.Logger?.LogDebug(ex, "Read-latency recording failed for a composed-panel run.");
        }
    }

    /// <summary>The window band of a composed-panel run (#5506): the windowStart/windowEnd pair when both parse, else the
    /// body's hours, else the default window the runner reads (<see cref="DefaultComposeHours"/>).</summary>
    internal static string ComposeWindowBand(JsonObject body)
    {
        if (ComposeSpec.TryParseUtcInstant(body["windowStart"], out var start) && ComposeSpec.TryParseUtcInstant(body["windowEnd"], out var end))
        {
            return ReadWindowBand.Of(start, end);
        }

        return ReadWindowBand.Of(body["hours"] is JsonValue hoursValue && hoursValue.TryGetValue<int>(out var hours) ? hours : DefaultComposeHours);
    }

    /// <summary>Maps a failed (non-<see cref="ComposeRunOutcome.Payload"/>) <see cref="ComposeRunOutcome"/> onto
    /// its HTTP answer — factored out of the <c>/api/compose/run</c> route (the <see cref="ToHttpResult"/> /
    /// <see cref="MuteRuleToolResult"/> shape) so the three arms are directly testable without a live Postgres
    /// round trip. <see cref="ComposeRunOutcome.Fault"/> (#4283 M1) is checked first: a STORE fault the panel
    /// author could not have caused answers through the same fixed-body backstop #4276 gives an uncaught
    /// exception, logged once, no role/host text on the wire — even though the outcome that carried it is
    /// itself a BadRequest shape (<see cref="ComposeRunOutcome.IsServerError"/> false), it still needs the
    /// 500-class backstop treatment, not a 400. Absent a Fault, <see cref="ComposeRunOutcome.IsServerError"/>
    /// false is a bad spec or #4283's allow-listed "Query failed: {ex.MessageText}" (a Custom Views author
    /// needs that syntax error, statement_timeout cancel included, to fix their own panel) — that text STAYS;
    /// true is the generic-Exception catch's "Error running query: {ex.Message}", reclassified here exactly as
    /// <see cref="ToHttpResult"/>'s ServerError arm reclassifies a tool's envelope, since by this point the
    /// real Exception is equally gone — only the sentence survived the round trip through
    /// <see cref="ComposeRunOutcome"/>.</summary>
    internal static IResult ComposeRunFailureResult(ComposeRunOutcome outcome, string route, ILogger logger, long elapsedMs, CancellationToken requestAborted)
    {
        /* W13: an unknown server is a plain 404 with its message, never a quiet empty answer. */
        if (outcome.IsNotFound)
        {
            return ErrorResult(outcome.Error!, StatusCodes.Status404NotFound);
        }

        if (outcome.Fault is not null)
        {
            DarlingWebFailureLog.Report(logger, route, elapsedMs, outcome.Fault, requestAborted);
            return Results.Json(DarlingWebFailureLog.Body(outcome.Fault), statusCode: DarlingWebFailureLog.StatusCode(outcome.Fault));
        }

        if (!outcome.IsServerError && outcome.AuthorSqlState is { } sqlState)
        {
            /* #4293 round 2 (R2-L1): a panel's own query error still answers its text at 400 so the author can fix
               the panel, but it is logged once too, so store drift (42P01/42703 after a migration that did not
               finish) reaches the service log, not only one author's browser. */
            logger.LogWarning(
                "{Route} answered a Custom View panel's query error at 400 after {ElapsedMs} ms (SQLSTATE {SqlState}): {Error}",
                route, elapsedMs, sqlState, outcome.Error);
        }

        return outcome.IsServerError
            ? ServerErrorResult(outcome.Error!, route, logger, elapsedMs, requestAborted)
            : ErrorResult(outcome.Error!, StatusCodes.Status400BadRequest);
    }

    internal static JsonValue? DbValueToJson(object value) => ComposePanelRunner.DbValueToJson(value);

    /// <summary>
    /// The <c>updated_by</c> stamp for a web edit made by a seat with NO subject — the shared token and the
    /// tokenless loopback operator. A named OIDC seat stamps its own subject instead
    /// (<see cref="DarlingWebSeat.EditorPrincipal"/> is the one place that choice is made), so this is the
    /// fallback, not the web stamp.
    ///
    /// <para><b>The column is therefore heterogeneous, deliberately.</b> A row reads either as a person or as
    /// the surface that wrote it, and a reader cannot tell which by shape alone — a directory that stamps an
    /// opaque <c>sub</c> GUID produces values no more person-like than this one. The alternative was a second
    /// column separating identity from provenance, rejected because the only consumer is a rendered byline
    /// ("updated by X") that wants exactly one value, and splitting it would make every reader join two columns
    /// to recover the string they already display. Nothing filters, sorts or groups on <c>updated_by</c>, so
    /// heterogeneity costs nothing a query has to cope with; if something ever does, that is the moment to
    /// revisit this, not before.</para>
    /// </summary>
    internal const string WebEditorPrincipal = "web";

    /// <summary>
    /// The <c>updated_by</c> stamp for a custom view created/updated over MCP (the <c>create_custom_view</c> /
    /// <c>update_custom_view</c> tools). Unlike <see cref="WebEditorPrincipal"/> this stays an unconditional
    /// constant, and the asymmetry is chosen rather than left over: MCP authenticates a CLIENT on its own
    /// network block with its own shared token and has no sign-in flow to carry a person through, so there is no
    /// subject to prefer. Stamping <c>mcp</c> always is the honest answer for a surface where per-user identity
    /// does not exist, and MCP is such a surface: it has no OIDC path, so a seat-derived stamp here would have
    /// nothing to derive from. Giving MCP identity is a separate piece of work on a separate credential model,
    /// not a line change here.
    /// </summary>
    internal const string McpEditorPrincipal = "mcp";

    /* ── loopback determination (the web host's tokenless-loopback auth arm) ── */

    /// <summary>
    /// PURE: whether a request's remote address is loopback — the CIDR-exemption arm of the web host's auth
    /// gate (<see cref="DarlingWebHostService.DecideWebAuth"/> calls this). A loopback request skips the CIDR
    /// test, because 127.0.0.1 is not inside a LAN CIDR and would otherwise 403 the operator's own browser; it
    /// still has to present a session cookie or token (#1649). Unwraps an IPv4-mapped-IPv6 address
    /// (<c>::ffff:127.0.0.1</c>) then defers to <see cref="IPAddress.IsLoopback"/>; a null/unverifiable remote
    /// is NOT loopback (fail-closed).
    /// </summary>
    internal static bool IsLoopbackRemote(IPAddress? remoteIp)
    {
        if (remoteIp is null)
        {
            return false;
        }

        var ip = remoteIp.IsIPv4MappedToIPv6 ? remoteIp.MapToIPv4() : remoteIp;
        return IPAddress.IsLoopback(ip);
    }

    /* ── Content-Type gate (the CSRF defense on the mutation routes) ── */

    /// <summary>
    /// PURE: whether a mutation request's Content-Type is <c>application/json</c> (a trailing <c>; charset=…</c>
    /// parameter is ignored; the match is case-insensitive). The POST/PUT/DELETE routes require this and return
    /// 415 otherwise. Requiring a non-simple Content-Type forces the browser to CORS-preflight ANY cross-origin
    /// write, and the dashboard answers no preflight — so a simple-request CSRF (which can only send
    /// <c>text/plain</c> / <c>application/x-www-form-urlencoded</c> / <c>multipart/form-data</c>) can never reach
    /// a write. A null/empty or non-json Content-Type is rejected. The test itself lives in
    /// <c>PerformanceMonitor.Common.JsonContentType</c>, shared with Lite's MCP host.
    /// </summary>
    internal static bool IsJsonContentType(string? contentType)
        => PerformanceMonitor.Common.JsonContentType.IsJson(contentType);

    /* ── definition validation (the authority: a bad doc can never be stored) ── */

    /* ── definition validation: the authority lives in Storage (CustomViewValidation, #6390 S2b-B); these forward ── */

    /// <summary>The visualization vocabulary (see <see cref="CustomViewValidation.KnownVizList"/>).</summary>
    internal static IReadOnlyList<string> KnownVizList => CustomViewValidation.KnownVizList;

    /// <summary>Set form of <see cref="KnownVizList"/>.</summary>
    internal static IReadOnlySet<string> KnownViz => CustomViewValidation.KnownViz;

    internal const int MaxDefinitionBytes = CustomViewValidation.MaxDefinitionBytes;

    internal const int MaxPanelCount = CustomViewValidation.MaxPanelCount;

    internal const int MaxNotebookCells = CustomViewValidation.MaxNotebookCells;

    internal const int MaxMarkdownCellBytes = CustomViewValidation.MaxMarkdownCellBytes;

    internal static string[] RefreshChoices => CustomViewValidation.RefreshChoices;

    /// <summary>The declared parameters of one read from <see cref="CatalogDescriptors"/>, null when it has no entry.</summary>
    internal static IReadOnlyList<ReadPanelParam>? ReadPanelParams(string read) =>
        CatalogDescriptors.TryGetValue(read, out var descriptor)
            ? descriptor.Params.Select(p => new ReadPanelParam(p.Name, p.Required)).ToList()
            : null;

    /// <summary>Validates a stored definition; the body lives in <see cref="CustomViewValidation.ValidateDefinition"/>.</summary>
    internal static CustomViewValidation.DefinitionValidation ValidateDefinition(string? definitionJson) =>
        CustomViewValidation.ValidateDefinition(definitionJson, ReadPanelParams);

    /// <summary>Validates a notebook definition; the body lives in <see cref="CustomViewValidation.ValidateNotebookDefinition"/>.</summary>
    internal static CustomViewValidation.DefinitionValidation ValidateNotebookDefinition(JsonObject rootObject) =>
        CustomViewValidation.ValidateNotebookDefinition(rootObject, ReadPanelParams);

    internal static bool IsHexColor(string? color) => CustomViewValidation.IsHexColor(color);

    private static string? TryGetString(JsonObject obj, string key) =>
        obj[key] is JsonValue value && value.TryGetValue<string>(out var s) ? s : null;

    /* ── the read catalog (input truth): read names from BuildReadDispatch, param metadata hand-authored ── */

    /// <summary>One catalog parameter: its WIRE query-string key (NOT the C# method param name), its
    /// <see cref="Type"/> (server/int/text/bool/double), whether it is <see cref="Required"/>, and its
    /// <see cref="Default"/> (null for text/required).</summary>
    internal sealed record CatalogParam(string Name, string Type, bool Required, object? Default);

    /// <summary>One catalog read: its display <see cref="Category"/>, a short <see cref="Description"/>, and its
    /// <see cref="Params"/> (the exact query keys its dispatch lambda binds).</summary>
    internal sealed record CatalogRead(string Category, string Description, IReadOnlyList<CatalogParam> Params);

    private const string TypeServer = "server";
    private const string TypeInt = "int";
    private const string TypeText = "text";
    private const string TypeBool = "bool";
    private const string TypeDouble = "double";

    private const string CatAnalysis = "Analysis";
    private const string CatSessions = "Sessions";
    private const string CatAlerts = "Alerts";
    private const string CatBlocking = "Blocking & Deadlocks";
    private const string CatConfig = "Configuration";
    private const string CatData = "Data";
    private const string CatTrends = "Trends";
    private const string CatOverview = "Overview";
    private const string CatLatch = "Latch & Spinlock";
    private const string CatMemoryGrants = "Memory Grants";
    private const string CatObjects = "Objects & Indexes";
    private const string CatPlanCache = "Plan Cache & Scheduler";
    private const string CatJobs = "Jobs";
    private const string CatPlans = "Plans";
    private const string CatDefaultTrace = "Default Trace";
    private const string CatSystemHealth = "System Health";
    private const string CatFinOps = "FinOps";

    private static CatalogParam PServer() => new("server", TypeServer, false, null);
    private static CatalogParam PHours(int def) => new("hours", TypeInt, false, def);

    /// <summary>
    /// The window ANCHOR (#2495): <c>?as_of=</c> moves the END of a <see cref="PHours"/> window off "now" so a
    /// caller can ask about a past incident. Typed <c>text</c> rather than a new catalog type because it IS a
    /// string on the wire — an ISO-8601 instant the tool parses and REFUSES if it cannot, which is where the
    /// error message belongs; a new type would only teach the picker to render a box it already renders.
    /// </summary>
    private static CatalogParam PAsOf() => new("as_of", TypeText, false, null);
    private static CatalogParam PLimit(int def) => new("limit", TypeInt, false, def);
    private static CatalogParam PTop(int def) => new("top", TypeInt, false, def);
    private static CatalogParam PText(string name) => new(name, TypeText, false, null);

    /// <summary>
    /// #5245: the catalog row of a read that takes the picker's database list. It is the same
    /// <see cref="CatalogParam"/> as <c>PText("database_name")</c> (text, optional, no default), so the served
    /// catalog keeps its shape and a client that sends one <c>database_name</c> is unchanged. The helper exists so
    /// a census can tell a read that takes a LIST (a <c>PDatabases(</c> row, whose dispatch entry calls
    /// <see cref="DatabaseNames"/>) from a read that takes one name.
    /// </summary>
    internal static CatalogParam PDatabases() => PText("database_name");

    /// <summary>A text param with a real default, unlike <see cref="PText(string)"/>'s always-null one — e.g.
    /// get_fleet_overview's detail (#4198), whose default "summary" is part of the contract, not an absence.</summary>
    private static CatalogParam PTextDefault(string name, string def) => new(name, TypeText, false, def);
    private static CatalogParam PReqText(string name) => new(name, TypeText, true, null);

    /// <summary>A REQUIRED integer with no default — a point read's key (a Query Store <c>query_id</c>, a snapshot
    /// <c>session_id</c>), where there is no sensible value to fall back to.</summary>
    private static CatalogParam PReqInt(string name) => new(name, TypeInt, true, null);
    private static CatalogParam PInt(string name, int def) => new(name, TypeInt, false, def);

    /// <summary>
    /// An optional integer with NO default — <see cref="PDouble(string)"/>'s shape for an int. Absent is a third
    /// state, not a number: <c>bucket_minutes</c> absent means the read sizes its points to the window (#3897),
    /// and advertising a default would tell a catalog consumer that sending nothing and sending that width are
    /// the same request.
    /// </summary>
    private static CatalogParam PInt(string name) => new(name, TypeInt, false, null);
    private static CatalogParam PBool(string name, bool def) => new(name, TypeBool, false, def);
    private static CatalogParam PDouble(string name, double def) => new(name, TypeDouble, false, def);

    /// <summary>
    /// A numeric FILTER, which carries no default — the <see cref="PText"/> shape rather than
    /// <see cref="PDouble(string, double)"/>'s.
    ///
    /// <para>The distinction is load-bearing for <c>min_duration_ms</c>: <c>0</c> is a real value there (it
    /// admits every row AND ranks the page by duration), so advertising <c>0</c> as the default would tell a
    /// catalog consumer that sending nothing and sending zero are the same request. Absent means no filter,
    /// which is a third state and not a number.</para>
    /// </summary>
    private static CatalogParam PDouble(string name) => new(name, TypeDouble, false, null);

    private static CatalogRead R(string category, string description, params CatalogParam[] parameters) =>
        new(category, description, parameters);

    /// <summary>
    /// The hand-authored per-read param + category metadata, keyed by read name. There is NO declarative param
    /// source (the dispatch lambdas bind STRING-LITERAL query keys imperatively, and those keys ≠ the C# method
    /// param names — e.g. <c>hours_back</c> ≠ <c>hoursBack</c> — so reflection would send params under the wrong
    /// keys). Each entry's <see cref="CatalogParam.Name"/> is therefore the ACTUAL query-string key its
    /// <see cref="BuildReadDispatch"/> lambda reads. A test pins <c>CatalogDescriptors.Keys ==
    /// BuildReadDispatch().Keys</c>, so a new read forces a catalog entry (it cannot silently ship key-less).
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, CatalogRead> CatalogDescriptors =
        new Dictionary<string, CatalogRead>(StringComparer.Ordinal)
        {
            /* ── analysis reads (AuditConfig/CompareAnalysis/GetAnalysisFacts/GetAnalysisFindings) ── */
            ["audit_config"] = R(CatAnalysis, "Configuration-audit findings for a server.", PServer()),
            ["compare_analysis"] = R(CatAnalysis, "Compare a window's analysis facts against an earlier baseline.", PServer(), PHours(4), PInt("baseline_hours_back"), PAsOf()),
            ["get_analysis_facts"] = R(CatAnalysis, "Raw analysis facts for a window, filtered by source and minimum severity.", PServer(), PHours(4), PText("source"), PDouble("min_severity", 0), PAsOf()),
            ["get_analysis_findings"] = R(CatAnalysis, "Persisted analysis findings for a server.", PServer(), PHours(24), PAsOf(), PLimit(MaxRowLimit), PBool("include_drilldown", false), PBool("full_text", true)),

            /* ── sessions (DarlingMcpSessionTools) ── */
            ["get_active_queries"] = R(CatSessions, "Currently-active queries, optionally blocking-only.", PServer(), PHours(1), PDatabases(), PBool("blocking_only", false), PText("wait_type"), PLimit(50), PAsOf(), PText("tag"), PText("collection_time"), PBool("snapshots_only", false)),
            ["get_session_stats"] = R(CatSessions, "Session-level summary counters for a server.", PServer()),
            ["get_waiting_tasks"] = R(CatSessions, "Tasks currently waiting, with wait type and duration.", PServer(), PHours(1), PLimit(30), PDatabases(), PAsOf()),

            /* ── alerts / mute rules (DarlingMcpAlertTools) ── */
            ["get_alert_history"] = R(CatAlerts, "Recent fired-alert history for a server, newest first and bounded by limit. Excludes operator-dismissed alerts unless include_dismissed is true (dismissed_excluded_count says how many the default hid). include_streak adds streak_days to each row: days in a row this metric fired at this time of day before the alert, muted fires counting.", PServer(), PHours(24), PLimit(50), PAsOf(), PBool("include_dismissed", false), PText("metric_name"), PText("family"), PBool("include_streak", false)),
            ["get_alert_settings"] = R(CatAlerts, "The current alert-settings configuration."),
            /* #5241: a web-only read (no MCP tool behind it, see WebOnlyReadNames): one alert's advice and fix script,
               fetched when its Analysis row is first expanded. The key is the page's own row identity. */
            ["get_alert_details"] = R(CatAlerts, "One fired alert's advice: the heading, fields, advice text and fix script the desktop's Alert Detail shows (never the Apply payload), found by server_id, metric_name and alert_time exactly as get_alert_history reports them. An alert with no advice, or no match, answers an empty details list.", new CatalogParam("server_id", TypeInt, true, null), PReqText("metric_name"), PReqText("alert_time")),
            ["list_investigation_requests"] = R(CatAlerts, "Level 1 investigation requests alerts opened for an agent, newest first, with the state computed from the lease and expiry.",
                PText("state"), PServer(), PText("family"), PLimit(20)),
            ["get_investigation_reports"] = R(CatAlerts, "Investigation requests with their agent reports and journal: one request by id (with its brief), or those opened in the window.",
                PServer(), PText("request_id"), PHours(24)),
            ["get_mute_rules"] = R(CatAlerts, "The alert mute rules (enabled-only by default).", PBool("enabled_only", true)),
            /* #3598: a read the web host's viewer role can serve — the tool selects only the non-secret carve
               (route_id, metric_match, the GENERATED configured_channels presence column, smtp_recipients,
               enabled), never a destination URL. */
            ["get_notification_routes"] = R(CatAlerts, "The alert-family taxonomy (self-monitor / reports / agent-jobs / performance, with every metric each owns) and the notification routes layered over the parent channels — which channels each route sets, never the destination values."),

            /* ── blocking / deadlocks (DarlingMcpBlockingTools) ── */
            ["get_blocked_process_xml"] = R(CatBlocking, "Blocked-process-report XML captures.", PServer(), PHours(24), PLimit(5), PDatabases(), PAsOf()),
            /* #6292: a web-only read (no MCP tool behind it, see WebOnlyReadNames): the lead-blocker tree of one blocked-process row,
               fetched when the row's Block chain action is used. The key is the page's own row identity. */
            ["get_block_chain"] = R(CatBlocking, "The lead-blocker tree of one blocked-process row: the lead blocker at the root and the sessions it blocks under it, built from the blocked-process reports and DMV blocking snapshots within five minutes of the row, exactly as the desktop viewer builds it.", PServer(), PReqText("event_time"), PReqInt("blocked_spid"), PInt("blocked_ecid", 0)),
            ["get_blocking"] = R(CatBlocking, "Blocking chains observed in the window.", PServer(), PHours(24), PLimit(30), PDatabases(), PAsOf()),
            ["get_blocking_trend"] = R(CatBlocking, "Blocking-event counts over time.", PServer(), PHours(24), PDatabases(), PAsOf()),
            ["get_deadlock_detail"] = R(CatBlocking, "Deadlock graph detail for recent deadlocks.", PServer(), PHours(24), PLimit(5), PBool("full_graph", true), PAsOf()),
            ["get_deadlock_trend"] = R(CatBlocking, "Deadlock counts over time.", PServer(), PHours(24), PAsOf()),
            ["get_deadlocks"] = R(CatBlocking, "Recent deadlocks with victim/resource summary.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_lock_wait_trend"] = R(CatBlocking, "The LCK% family's summed wait ms/sec over time - the aggregate lock-wait lane - with a legend of the types that waited.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),

            /* ── automatic plan correction (DarlingMcpPlanCorrectionTools, #2028) ── */
            ["get_plan_corrections"] = R(CatAnalysis, "Automatic plan correction activity + per-database FORCE_LAST_GOOD_PLAN state.", PServer(), PHours(24), PLimit(50), PAsOf(), PBool("full_text", true), PDatabases()),

            /* ── config: current + history (DarlingMcpConfigTools / DarlingMcpConfigHistoryTools) ── */
            ["get_database_config"] = R(CatConfig, "Database-level configuration for a server.", PServer(), PDatabases()),
            ["get_server_config"] = R(CatConfig, "Server-level configuration (sp_configure) for a server.", PServer()),
            ["get_trace_flags"] = R(CatConfig, "Active trace flags for a server.", PServer()),
            ["get_database_protection"] = R(CatConfig, "Per database, the last good CHECKDB, the last full backup and the last log backup with their ages, and which databases are overdue (full backup 7 days, log backup 60 minutes, CHECKDB 14 days). Azure SQL Database and Managed Instance show the age and are never overdue. RDS shows the backup ages and is never overdue on them, but its CHECKDB is judged.", PServer(), PHours(24), PAsOf(), PBool("overdue_only", false)),
            ["get_identity_capacity"] = R(CatConfig, "How full each identity column is and how long until it runs out, per scanned database (the fullest columns above 10 percent used).", PServer(), PHours(168), PAsOf(), PDatabases(), PLimit(200)),
            ["get_database_config_changes"] = R(CatConfig, "Database-configuration changes over time.", PServer(), PHours(168), PAsOf(), PDatabases()),
            ["get_database_scoped_config"] = R(CatConfig, "Database-scoped configuration for a database.", PServer(), PDatabases()),
            ["get_query_store_health"] = R(CatConfig, "Per-database Query Store health: actual vs desired state, readonly_reason, storage vs cap, and the two capture modes (query_capture_mode ALL / AUTO / CUSTOM / NONE, wait_stats_capture_mode ON / OFF; null on a pre-rung row or a pre-2017 engine).", PServer(), PDatabases()),
            ["get_server_config_changes"] = R(CatConfig, "Server-configuration changes over time.", PServer(), PHours(168), PAsOf()),
            ["get_trace_flag_changes"] = R(CatConfig, "Trace-flag changes over time.", PServer(), PHours(168), PAsOf()),
            ["get_known_patterns"] = R(CatConfig, "The operator-written known-pattern entries (a note on a recurring alert pattern); get_alert_history rows carry the matches.", PServer(), PText("metric_name")),
            ["get_change_watches"] = R(CatConfig, "A server's change watches, open and closed: each recorded change opens a 60 minute watch, with its changes, measures and result. watch_id reads one watch in any state.", PServer(), PText("state"), PLimit(20), PText("watch_id")),

            /* ── core data reads (DarlingMcpDataTools + long-query / fleet tools) ── */
            ["get_collection_health"] = R(CatData, "Per-collector collection health for a server. server=* with collector_name answers for that one collector on every enabled server (hours: default 24).", PServer(), PText("collector_name")),
            ["get_collection_log"] = R(CatData, "Raw per-run collector log for a server, newest first — or slowest first when min_duration_ms is supplied.", PServer(), PHours(24), PLimit(200), PAsOf(), PText("collector_name"), PDouble("min_duration_ms"), PText("status")),
            ["get_current_waits_trend"] = R(CatData, "Waiting-task and blocked-session series over time.", PServer(), PHours(4), PDatabases(), PAsOf()),
            ["get_client_timeouts"] = R(CatData, "Client-timeout (attention) counts, rate and top databases, applications and statements over the window, with the newest sampled events (#5640).", PServer(), PHours(1), PLimit(20), PAsOf()),
            ["get_blocking_stats"] = R(CatData, "Blocking duration and deadlock severity per minute.", PServer(), PHours(24), PDatabases(), PAsOf()),
            ["get_cpu_utilization"] = R(CatData, "CPU utilization over time.", PServer(), PHours(4), PAsOf(), PInt("bucket_minutes")),
            ["get_file_io_stats"] = R(CatData, "Per-file IO stall/throughput stats.", PServer(), PDatabases()),
            ["get_memory_clerks"] = R(CatData, "Top memory clerks by allocation.", PServer()),
            ["get_memory_stats"] = R(CatData, "Server memory summary counters.", PServer()),
            ["get_perfmon_stats"] = R(CatData, "Perfmon counter values, filtered by counter/instance.", PServer(), PText("counter_name"), PText("instance_name")),
            ["get_query_heatmap"] = R(CatData, "Query counts per (time bin x log-magnitude bucket) - the viewer's Query Heatmap as a table.", PServer(), PHours(24), PText("metric"), PDatabases(), PInt("bucket_minutes", 5), PLimit(500), PAsOf()),
            ["get_query_store_regressions"] = R(CatData, "Queries whose Query Store performance got WORSE vs their baseline.", PServer(), PHours(24), PDatabases(), PLimit(50), PBool("full_text", true), PAsOf()),
            ["get_query_store_clutter"] = R(CatData, "The Query Store CLUTTER view: per database the collector's read cost (how often and by how much it was the slowest fan-out item), plan churn (plans per query, arrivals per day, one-shot plans) and the options row, each with raw numbers and a decomposed verdict; ONE per-server overhead block (non-sleep QDS_* wait deltas with the excluded sleep waits named, and the Query Store memory clerk). Replicas excluded by architecture with the reason on the row; query_capture_mode (ALL / AUTO / CUSTOM / NONE) carried with capture_mode_known beside it, null meaning a capture older than the V137 rung rather than NONE. window_truncated says the raw tier did not hold the whole window. Composed from collected rows - no new query against the server.", PServer(), PHours(24), PLimit(DarlingMcpQueryStoreClutterTools.DefaultLimit), PBool("include_fleet_median", false), PAsOf(), PDatabases()),
            ["get_resource_use_by_tag"] = R(CatData, "Resource use rolled up by query tag, one section per source (plan cache, Query Store, procedures, blocking, deadlocks); sections are never summed. Omit the server for the fleet.", PServer(), PHours(24), PText("tag"), PLimit(DarlingMcpQueryTagRollupTools.DefaultLimit), PAsOf()),
            ["get_query_store_query_history"] = R(CatData, "One Query Store query's executions, duration and CPU per plan over the window (requires database_name, query_id).", PReqText("database_name"), PReqInt("query_id"), PServer(), PHours(24), PAsOf()),
            ["get_query_store_hints"] = R(CatData, "Which Query Store hints are in place: one entry per hint in its database's newest capture, with the query, the hint text, its source, whether the engine records a failure (FAILING or NO_FAILURE) and the query text (database_name optional). SQL Server 2022 and later.", PServer(), PHours(24), PAsOf(), PDatabases()),
            ["get_forced_plans"] = R(CatData, "Whether each Query Store force works: one entry per plan forced in its database's newest capture, with a state (WORKING, NOT_EXERCISED, IGNORED, FAILING, UNKNOWN_PLAN_NOT_COLLECTED or REMOVED) and the evidence (database_name optional).", PServer(), PHours(24), PAsOf(), PDatabases()),
            ["get_plan_force_precheck"] = R(CatData, "Reads one Query Store plan before anyone forces it: verdict blocked (plan_not_found or plan_already_forced, a force that cannot work) or not_blocked (never safe), the warnings with stable codes for the agent's own instructions to weigh, what a person should read first, and the checks the store could not answer (requires database_name, query_id, plan_id; compare_plan_id optional).", PReqText("database_name"), PReqInt("query_id"), PReqInt("plan_id"), PInt("compare_plan_id"), PServer(), PHours(24), PAsOf()),
            ["get_query_store_aborts"] = R(CatData, "Queries ranked by Query Store runs that ended aborted (a client timeout or cancel) or with an exception, with each query's share of its own runs, the average aborted duration and the newest failure time; window_truncated says the store did not hold the whole window.", PServer(), PHours(24), PTop(20), PText("database_name"), PAsOf(), PBool("full_text", false)),
            ["get_cpu_by_database"] = R(CatData, "Databases ranked by CPU over the window with each one's share of the server total, its heaviest statement and the procedure stats' CPU beside it (never added to it).", PServer(), PHours(24), PTop(20), PAsOf()),
            ["get_load_by_caller"] = R(CatSessions, "Active-query snapshots grouped by host, program and login, ranked by CPU, with each group's share and top statements.", PServer(), PHours(1), PTop(20), PInt("statements", 3), PAsOf()),
            ["get_query_pileups"] = R(CatSessions, "Capture times when min_sessions or more sessions ran the same query_hash at once, with the main waits and the head blocker from the nearest blocking snapshot.", PServer(), PHours(24), PInt("min_sessions", 5), PTop(10), PAsOf()),
            ["get_query_store_top"] = R(CatData, "Top Query Store queries in the window, optionally filtered by execution outcome or to one exact module before ranking; window_truncated says the raw tier did not hold the whole window (effective_hours_back how far it reached).", PServer(), PHours(24), PTop(20), PDatabases(), PAsOf(), PText("execution_type"), PText("module_name"), PBool("full_text", false), PText("tag")),
            ["get_long_query_completions"] = R(CatData, "Completed long-running queries captured by the XE trace.", PServer(), PHours(24), PLimit(30), PAsOf(), PDatabases(), PText("tag")),
            ["get_server_properties"] = R(CatData, "Server properties/inventory for a server.", PServer()),
            ["get_tempdb_trend"] = R(CatData, "tempdb space usage over time.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_top_procedures_by_cpu"] = R(CatData, "Top stored procedures, ranked by CPU (the default), duration, reads or executions (order_by).", PServer(), PHours(24), PTop(20), PDatabases(), PTextDefault("order_by", "cpu"), PAsOf(), PTextDefault("detail", "summary"), PText("tag")),
            ["get_top_queries_by_cpu"] = R(CatData, "Top queries, ranked by CPU (the default), duration, reads or executions (order_by), optionally parallel-only / min-DOP.", PServer(), PHours(24), PTop(20), PDatabases(), PBool("parallel_only", false), PInt("min_dop", 0), PTextDefault("order_by", "cpu"), PAsOf(), PTextDefault("detail", "summary"), PText("tag")),
            ["get_pg_top_queries"] = R(CatData, "Top PostgreSQL query shapes by total execution time (Aurora targets).", PServer(), PHours(24), PLimit(20), PBool("full_text", true), PAsOf()),
            ["get_pg_plans"] = R(CatData, "Captured PostgreSQL execution plans, grouped by shape. Plans are redacted at collection.", PServer(), PHours(24), PLimit(10), PText("query_id"), PAsOf()),
            ["get_pg_plan_capture_readiness"] = R(CatData, "Whether a PostgreSQL target can capture execution plans at all, facet by facet, with the remedy for each step that is not in place. Read this when a plan or target-log read is empty.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_logging_audit"] = R(CatData, "Whether a PostgreSQL target's logging settings (log_min_duration_statement, log_lock_waits, log_temp_files, log_autovacuum_min_duration, log_checkpoints, log_connections, log_disconnections) are producing the lines they could, per setting: verdict, what it unlocks, the recommended value with its cost, and the remedy in the hosting flavour's syntax. Judged from the newest stored pg_server_config snapshot, not the live server; plan capture's own settings are listed and pointed at get_pg_plan_capture_readiness.", PServer()),
            ["get_pg_wraparound_risk"] = R(CatData, "PostgreSQL XID/MultiXact freeze headroom per database.", PServer(), PHours(24), PAsOf()),
            ["get_pg_xmin_horizon"] = R(CatData, "What is holding back the PostgreSQL xmin horizon, by cause.", PServer(), PHours(24), PAsOf()),
            ["get_pg_replication_slots"] = R(CatData, "PostgreSQL replication slot health, including whether retained WAL is still growing.", PServer(), PHours(24), PAsOf()),
            ["get_pg_autovacuum_health"] = R(CatData, "PostgreSQL tables behind on vacuum or analyze, ranked by how far past each table's own threshold.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_io_stats"] = R(CatData, "PostgreSQL I/O by backend type, object and context, differenced across the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_wait_stats"] = R(CatData, "Top PostgreSQL wait events in the window (Aurora targets).", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_cpu_utilization"] = R(CatData, "Instance CPU utilization over time from AWS Performance Insights (Aurora/RDS targets).", PServer(), PHours(4), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_wait_sampling"] = R(CatData, "Sampled PostgreSQL waits by query shape, from pg_wait_sampling - the stock-PostgreSQL counterpart of get_pg_wait_stats. Sample counts, not measured durations; event_type CPU means running rather than waiting.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_kernel_stats"] = R(CatData, "Per-query OS CPU (user and system), device bytes and major faults, from pg_stat_kcache. The CPU half of the elapsed time get_pg_top_queries reports.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_predicate_stats"] = R(CatData, "Which columns queries actually filter on and how selectively, from pg_qualstats. SAMPLED counts - the evidence behind an index recommendation.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_index_bloat"] = R(CatData, "ESTIMATED PostgreSQL index bloat from catalog statistics: reclaimable bytes, the modelled inputs behind them, and a reason instead of an answer where the statistics cannot support one. Answerless rows sort FIRST by design, so pass answered_only for the ranking on a server whose answerless population exceeds the row cap — read the reach field, which says whether raising the limit would help at all. The complete btree census, no size floor, so it counts MORE indexes than get_pg_index_usage (which floors at 64 kB).", PServer(), PHours(168), PLimit(25), PBool("answered_only", false), PAsOf()),
            ["get_pg_column_stats"] = R(CatData, "Per-column distribution statistics the PLANNER uses: n_distinct, null fraction, correlation and top-value frequency.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_buffer_usage"] = R(CatData, "What is resident in the shared buffer pool per relation, from pg_buffercache. Residency, not read volume.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_extensions"] = R(CatData, "Which PostgreSQL extensions are installed, outdated, available or absent, per database. Usually the reason another read is empty. Rows are databases x extension names, so a multi-database host outgrows any row cap: read install_census for what is installed where (aggregated, limit-independent), or pass database_name to complete one database at a time.", PServer(), PHours(168), PLimit(50), PText("database_name"), PAsOf()),
            ["get_pg_lock_stats"] = R(CatData, "Sampled PostgreSQL lock activity by type, mode and relation. A sample of pg_locks, not an event log; for who blocks whom use get_pg_blocking.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_write_stats"] = R(CatData, "Checkpoint and WAL write activity across the window: timed versus requested checkpoints, buffers written by whom, and WAL volume.", PServer(), PHours(24), PAsOf()),
            ["get_pg_server_config"] = R(CatData, "The PostgreSQL server's configuration from pg_settings, non-default first, saying where each value came from and whether changing it needs a restart. Reports pending_restart, where the file and the running server disagree. Per-database and per-role overrides (ALTER DATABASE / ALTER ROLE ... SET) are a separate section, present only when the cluster has any: those are what sessions in that database or as that role actually run with.", PServer(), PLimit(100), PBool("include_defaults", false)),
            ["get_pg_server_config_changes"] = R(CatData, "PostgreSQL configuration parameters whose value CHANGED in the window, old beside new - per-database and per-role overrides included, as changed, set or reset. Nothing else can reconstruct this after the fact.", PServer(), PHours(168), PLimit(100), PAsOf()),
            ["get_pg_deadlocks"] = R(CatData, "PostgreSQL deadlocks reported in the window, with the victim, the lock modes and resources, and the victim's statement. Needs nothing configured on the target.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_deadlock_detail"] = R(CatData, "PostgreSQL deadlock graphs in full: the whole wait graph as the server wrote it, and every participant's statement with its literals normalized. Newest first, or one by deadlock_hash.", PServer(), PText("deadlock_hash"), PLimit(5)),
            ["get_pg_log_events"] = R(CatData, "PostgreSQL server-log events in the window, classified by family (error, connection, lock_wait, temp_file, autovacuum, checkpoint), newest first, SQL in them normalized. The 'check the error log' read. Filter by family and min_severity; the page says what bounded it.", PServer(), PHours(24), PText("family"), PText("min_severity"), PLimit(50), PAsOf()),
            ["get_pg_wait_trend"] = R(CatTrends, "One PostgreSQL wait event over time, per second. Omit wait_event to follow whichever dominates. Estimates from a sampling profiler, so the shape is the finding.", PServer(), PText("wait_event"), PHours(24), PAsOf()),
            ["get_pg_query_duration_trend"] = R(CatTrends, "One PostgreSQL statement over time by queryid: what a single execution cost in each interval. Omit queryid for the busiest statement. The regression read.", PServer(), PText("queryid"), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_io_trend"] = R(CatTrends, "One PostgreSQL (backend_type, context) pair over time: I/O rates per second, the hit ratio per point, and latency where the server measures it. Omit both to follow whichever pair moved the most I/O.", PServer(), PText("backend_type"), PText("context"), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_database_trend"] = R(CatTrends, "One PostgreSQL database over time: temp-file spills, the cache hit ratio per point with its worst interval, deadlocks and the rollback share. Omit database for the biggest spiller. The cumulative ratio is a lifetime average that hides a cliff.", PServer(), PText("database"), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_pg_replication_stats"] = R(CatData, "Health of CONNECTED replicas from pg_stat_replication, with the worst lag in the window beside the latest. Counterpart of get_pg_replication_slots.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_pg_blocking"] = R(CatData, "PostgreSQL blocking chains that were sampled, with the root blocker attributed. A sample, not an event log.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_pg_database_stats"] = R(CatData, "PostgreSQL per-database temp-file spills, cache hit ratio, deadlocks and commit/rollback split, differenced across the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_pg_index_usage"] = R(CatData, "PostgreSQL per-index scan counts and size, with the constraint, replica-identity and validity facts that decide whether an unused index can actually be dropped. Reports indexes of at least 64 kB plus any invalid index at any size, so it counts FEWER indexes than get_pg_index_bloat (the complete btree census) — that floor is the whole difference.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_table_bloat"] = R(CatData, "PostgreSQL per-table bloat ESTIMATE with its measured sizes and dead-tuple counts. The estimate is suppressed, not captioned, when its statistics cannot be trusted.", PServer(), PHours(168), PLimit(25), PAsOf()),
            ["get_pg_session_states"] = R(CatData, "PostgreSQL sessions holding a transaction open, and whether each one actually pins the xmin horizon - which is not the same question as how long it has been idle in transaction.", PServer(), PHours(24), PLimit(25), PAsOf()),
            ["get_wait_stats"] = R(CatData, "Top wait statistics in the window.", PServer(), PHours(24), PLimit(20), PAsOf()),
            ["get_wait_trend"] = R(CatData, "One wait type's totals over time (requires wait_type).", PReqText("wait_type"), PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_wait_types"] = R(CatData, "The wait types observed in the window.", PServer(), PHours(24), PAsOf()),
            ["find_database"] = R(CatData, "Which monitored server holds a database: exact, case-insensitive name match against each server's newest database list, with the registry and display names, whether the server is still monitored and when the list was last seen (requires database_name).", PReqText("database_name")),
            ["list_servers"] = R(CatData, "The monitored servers known to the store."),
            ["list_query_tags"] = R(CatData, "The query tag tree with each tag's hand assignments, their precedence over rules, and whether the tag map has caught up."),

            /* ── trends (DarlingMcpTrendTools) ── */
            ["get_file_io_trend"] = R(CatTrends, "File I/O read and write latency over time per database and file type, heaviest stall first; database_name charts one database per file.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PDatabases()),
            ["get_memory_trend"] = R(CatTrends, "Memory usage over time.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            ["get_server_trend"] = R(CatTrends, "One instance trend over time, picked by metric: total_waits, cpu_scheduler, memory_clerks, plan_cache, latch, spinlock, session_stats, collector_duration, tempdb_file_io, tempdb_size or file_io_throughput.", PReqText("metric"), PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PText("clerk_types"), PText("names")),
            ["get_perfmon_trend"] = R(CatTrends, "One perfmon counter over time (requires counter_name).", PReqText("counter_name"), PServer(), PHours(24), PAsOf(), PInt("bucket_minutes")),
            /* #3653 item 17: the four reads below disclose the WINDOW floor as window_truncated (beside
               effective_start / effective_hours_back) — not the page dialect's truncated, which they never had. */
            ["get_procedure_duration_trend"] = R(CatTrends, "Stored-procedure elapsed ms/sec + executions/sec over time, or one procedure's with procedure_name (its full_name from the Top Procedures grid, plus one database_name); window_truncated says the tier did not hold the whole window.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PDatabases(), PText("procedure_name")),
            ["get_query_duration_trend"] = R(CatTrends, "Query elapsed ms/sec + executions/sec over time (per-interval rates, not percentiles); window_truncated says the tier did not hold the whole window.", PServer(), PHours(24), PAsOf(), PInt("bucket_minutes"), PDatabases()),
            ["get_query_store_duration_trend"] = R(CatTrends, "Query Store duration ms/sec + executions/sec over time; window_truncated says the tier did not hold the whole window.", PServer(), PHours(24), PAsOf(), PDatabases()),
            ["get_query_trend"] = R(CatTrends, "One query's metrics over time (requires query_hash + database_name); window_truncated says the tier did not hold the whole window.", PReqText("query_hash"), PReqText("database_name"), PServer(), PHours(24), PAsOf()),

            /* ── health / overview (DarlingMcpHealthTools / DarlingMcpFleetTools) ── */
            ["get_server_summary"] = R(CatOverview, "A one-shot health summary for a server.", PServer()),
            /* #6291: a web-only read (no MCP tool behind it, see WebOnlyReadNames): the four baselines (CPU, waits per second, I/O latency, blocking)
               at the window start that the Overview strips draw their bands and anomaly markers from. Keyed by the window start the page draws. */
            ["get_overview_baselines"] = R(CatOverview, "The four baselines the Overview strips draw their band and anomaly markers from (CPU, waits per second, I/O latency, blocking): each bucket's mean and effective standard deviation at the window start, null where there is not enough history.", PServer(), PReqText("window_start")),
            ["get_daily_summary"] = R(CatOverview, "The daily health summary (optionally for a specific date).", PServer(), PText("summary_date")),
            ["get_daily_summary_range"] = R(CatOverview, "One daily health summary per collected day over a span of days - the Performance Calendar's month grid.", PServer(), PInt("days_back", 30), PAsOf()),
            ["get_fleet_overview"] = R(CatOverview, "The banded cross-server fleet roll-up.", PHours(DefaultFleetHours), PTextDefault("detail", "summary"), PBool("worst_only", false), PText("band")),
            ["get_sweep_reports"] = R(CatOverview, "The scheduled Fleet Sweep Reports: the sweep timeline for the window, the newest sweep in full, and the watch-item worklist - or one sweep by sweep_id (a string; the ids do not survive a JSON number round trip).", PHours(1), PAsOf(), PText("sweep_id"), PText("watch_state")),
            ["get_ag_health"] = R(CatOverview, "Availability Group topology: replicas and per-database secondary state.", PServer(), PLimit(DarlingMcpAgTools.DefaultGroupLimit)),
            ["get_store_metrics"] = R(CatOverview, "The monitoring store's own size/compression/growth (self-metrics): a summary by default, object_kind to list one kind, an exact object_name for one object's daily series.", PInt("days_back", 30), PText("object_kind"), PText("object_name"), PLimit(DarlingMcpStoreMetricsTools.DefaultLimit)),
            ["get_store_log"] = R(CatOverview, "What the monitoring store's OWN PostgreSQL server log recorded - a per-class census with the capture denominator beside it, not the lines. Deliberately unbanded.", PHours(24), PLimit(DarlingMcpStoreLogTools.DefaultRetainedLimit), PAsOf()),
            ["get_store_query_history"] = R(CatOverview, "The monitoring store's OWN SQL statements hour by hour (pg_stat_statements deltas the service snapshots every hour): the top statements over the window by time spent, or one statement's hourly series with query_id.", PText("query_id"), PText("role"), PHours(DarlingMcpStoreQueryHistoryTools.DefaultHours), PTop(DarlingMcpStoreQueryHistoryTools.DefaultTop)),
            ["get_store_query_stats"] = R(CatOverview, "The monitoring store's OWN SQL statements ranked by server-side cost (pg_stat_statements), split by the role that ran them (on a managed store: the web viewer, MCP tools, the Darling Viewer, or the service itself).", PText("role"), PText("order_by"), PTop(DarlingMcpStoreQueryStatsTools.DefaultTop), PBool("full_text", false)),
            ["get_store_host"] = R(CatOverview, "The monitoring store's own HOST profile: platform/RAM/data volume, PostgreSQL/TimescaleDB facts, and a verdict per sizing-relevant setting against what this host would derive today - is the store sized right. No parameters; a snapshot."),
            ["get_collector_cost"] = R(CatOverview, "The monitoring tool's OWN per-collector cost on the monitored servers (self-monitoring) - which of our collectors is the most expensive to run. Pass collector_name for that one collector's daily trend instead of the ranked list.", PInt("days_back", 7), PText("collector_name")),
            ["get_collector_stall_probes"] = R(CatOverview, "The out-of-band server-wide wait samples taken while one of OUR collectors was stalled mid-read - what the monitored instance was doing inside the window the sequential sweep records nothing in. Carries the outcome census beside the samples, deliberately unbanded.", PServer(), PInt("days_back", 7), PLimit(DarlingMcpStallProbeTools.DefaultLimit)),
            ["get_oversized_plan_backlog"] = R(CatOverview, "The cached plans this tool measured as too large to capture inline, and what the out-of-band sweep has done about each one: per server the three verdict buckets (pending/captured/expired, a strict partition), the attempt figures on still-pending rows, the newest capture and expiry instants, and observed_bytes min/median/max, with the per-collector census beside them. Takes no window - a worklist updated in place, not a series. Pass server_name with include_rows for the claim keys.", PServer(), PBool("include_rows", false), PLimit(DarlingMcpOversizedPlanBacklogTools.DefaultLimit)),
            ["get_slow_reads"] = R(CatOverview, "The monitoring tool's OWN slow or failed reads, newest first - each with the arguments it was asked with, the source that answered and its slowest statements. Recorded when a read took 5 seconds or more or ended in a timeout, error or limit. Optional server, surface (web/compose/mcp) and route filter.", PHours(DarlingMcpSlowReadTools.DefaultHours), PServer(), PText("surface"), PText("route"), PLimit(DarlingMcpSlowReadTools.DefaultLimit)),
            ["get_read_latency"] = R(CatOverview, "The monitoring tool's OWN read-latency history (self-monitoring) - which of the web dashboard's or MCP server's own reads is really slow, and how often it times out. p50/p95/p99 are bucket upper-bound estimates, per (surface, route, window band: 1h, 24h, 7d, 7d+ or none), so one route can return a row per band; sorted p95 desc. window_band is the width of the window the read asked for (its default window when it sent none). max_in_flight is the most other reads running when one started; wait_total_ms is the summed wait for a store connection. Optional surface (web/compose/mcp) and route filter. fallbacks and gate_failures count reads that fell back to raw.", PHours(DarlingMcpReadLatencyTools.DefaultHours), PText("surface"), PText("route"), PLimit(DarlingMcpReadLatencyTools.DefaultLimit)),

            /* ── latch / spinlock (DarlingMcpLatchSpinlockTools) ── */
            ["get_latch_stats"] = R(CatLatch, "Top latch waits in the window.", PServer(), PHours(24), PTop(10), PAsOf()),
            ["get_spinlock_stats"] = R(CatLatch, "Top spinlock activity in the window.", PServer(), PHours(24), PTop(10), PAsOf()),

            /* ── memory grants (DarlingMcpMemoryGrantTools) ── */
            ["get_memory_grants"] = R(CatMemoryGrants, "Active/recent memory grants. chart=true answers the whole window as one point per bucket per resource pool instead of grants[] and window[]; bucket_minutes picks the width.", PServer(), PHours(1), PAsOf(), PBool("chart", false), PInt("bucket_minutes")),
            ["get_memory_pressure_events"] = R(CatMemoryGrants, "Memory-pressure events in the window.", PServer(), PHours(24), PAsOf()),
            ["get_resource_semaphore"] = R(CatMemoryGrants, "Resource-semaphore state over time.", PServer(), PHours(24), PAsOf()),

            /* ── object / index stats (DarlingMcpObjectStatsTools) ── */
            ["get_database_sizes"] = R(CatObjects, "Per-database size breakdown.", PServer(), PDatabases()),
            ["get_pvs_stats"] = R(CatObjects, "ADR persistent version store state per database, with an optional top-5 size trend.", PServer(), PInt("trend_hours_back", 0), PDatabases()),
            ["get_index_usage"] = R(CatObjects, "Index usage (seeks/scans/updates) per index. Unused-first, so pass database_name unless you want a server-wide sweep; the answer carries matching_index_count and truncated.", PServer(), PDatabases(), PLimit(200)),
            ["get_object_locking"] = R(CatObjects, "Per-object locking/contention stats.", PServer(), PLimit(200), PDatabases(), PText("detail_database"), PText("detail_schema"), PText("detail_table"), PText("detail_index")),
            ["get_table_index_sizes"] = R(CatObjects, "Per-table/index size breakdown.", PServer(), PDatabases()),

            /* ── plan cache / scheduler (DarlingMcpPlanCacheSchedulerTools) ── */
            ["get_cpu_scheduler_pressure"] = R(CatPlanCache, "CPU scheduler pressure indicators from the newest snapshot within the window.", PServer(), PHours(24), PAsOf()),
            ["get_plan_cache_bloat"] = R(CatPlanCache, "Plan-cache bloat / single-use plan indicators.", PServer(), PHours(24), PAsOf()),

            /* ── jobs (DarlingMcpJobTools) ── */
            ["get_running_jobs"] = R(CatJobs, "Currently-running SQL Agent jobs.", PServer()),
            ["get_agent_alerts"] = R(CatJobs, "The SQL Server Agent alerts defined on an instance, and the gaps in them (no alert for severities 19 to 25 or errors 823 to 825, an alert that notifies no one, a disabled alert, an alert that fired and told no one).", PServer(), PHours(24), PAsOf()),
            ["get_job_history"] = R(CatJobs, "Retained SQL Agent job runs, newest first; omit server for every server.", PServer(), PHours(24), PText("job_name"), PText("status"), PText("category"), PLimit(100), PInt("min_duration_seconds"), PAsOf()),

            /* ── stored plan XML (DarlingMcpPlanTools; the analyze_*_plan compute family stays excluded) ── */
            ["get_plan_xml"] = R(CatPlans, "The stored execution-plan XML for a query (requires query_hash).", PReqText("query_hash"), PServer(), PText("database_name")),
            /* #5228: the three grid-row plan reads — point reads by a row's own key, so no window (no PHours/PAsOf). */
            ["get_query_store_plan_xml"] = R(CatPlans, "The stored Query Store plan XML for a query (requires database_name, query_id).", PReqText("database_name"), PReqInt("query_id"), PServer(), PInt("plan_id")),
            ["get_procedure_plan_xml"] = R(CatPlans, "The stored plan XML for a procedure (requires sql_handle).", PReqText("sql_handle"), PServer()),
            ["get_active_query_plan_xml"] = R(CatPlans, "The plan captured with one Active Queries row (requires collection_time, session_id).", PReqText("collection_time"), PReqInt("session_id"), PServer(), PInt("request_id", 0), PBool("live", false)),
            /* #5236: the Blocking and Deadlocks grids' plan reads, the same point-read shape: no window (no PHours/PAsOf). */
            ["get_blocking_plan_xml"] = R(CatPlans, "The plan stored with one Blocking row (requires event_time, blocked_spid, blocking_spid); side=blocking for the blocker's plan.", PReqText("event_time"), PReqInt("blocked_spid"), PReqInt("blocking_spid"), PServer(), PInt("blocked_ecid", 0), PInt("blocking_ecid", 0), PTextDefault("side", "blocked"), PText("database_name")),
            ["get_deadlock_plan_xml"] = R(CatPlans, "The victim's plan stored with one Deadlocks row (requires collection_time, deadlock_time).", PReqText("collection_time"), PReqText("deadlock_time"), PServer(), PText("victim_process_id"), PText("database_name")),
            /* #5233: a repro script built from the stored text and plan — store-only, nothing runs. */
            ["get_query_repro_script"] = R(CatPlans, "A T-SQL repro script built from a stored query's text and plan (requires kind and its key).", PReqText("kind"), PServer(), PText("database_name"), PText("query_hash"), PInt("query_id"), PInt("plan_id"), PText("collection_time"), PInt("session_id"), PInt("request_id", 0)),

            /* ── default trace (DarlingMcpDefaultTraceTools) ── */
            ["get_default_trace_events"] = R(CatDefaultTrace, "Default-trace events (file growth, DDL, security).", PServer(), PHours(24), PLimit(100), PAsOf(), PDatabases()),

            /* ── system_health parse-on-read family (DarlingMcpHealthParserTools) ── */
            ["get_health_parser_cpu_tasks"] = R(CatSystemHealth, "system_health: CPU-bound task snapshots.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_io_issues"] = R(CatSystemHealth, "system_health: IO-latency issues.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_broker"] = R(CatSystemHealth, "system_health: memory-broker ring-buffer entries.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_conditions"] = R(CatSystemHealth, "system_health: resource-monitor memory conditions.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_memory_node_oom"] = R(CatSystemHealth, "system_health: per-node out-of-memory events.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_scheduler_issues"] = R(CatSystemHealth, "system_health: non-yielding scheduler issues.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_severe_errors"] = R(CatSystemHealth, "system_health: severe (sev >= 19) errors.", PServer(), PHours(24), PLimit(50), PAsOf(), PDatabases()),
            ["get_health_parser_significant_waits"] = R(CatSystemHealth, "system_health: individual 500 ms+ waits with their statement.", PServer(), PHours(24), PLimit(50), PAsOf()),
            ["get_health_parser_system_health"] = R(CatSystemHealth, "system_health: the raw parsed session records. chart=true answers one row per time bucket (mean CPU) across the whole window instead of the newest limit rows; bucket_minutes picks the width.", PServer(), PHours(24), PLimit(50), PAsOf(), PBool("chart", false), PInt("bucket_minutes")),
            // FinOps web parity (#4843), set A: append new FinOps entries below this line only.
            ["get_finops_inventory"] = R(CatFinOps, "FinOps Server Inventory for the whole fleet, one row per configured server. view is server_inventory; limit caps servers returned; include_removed adds servers removed from the configuration.", PText("view"), PInt("limit", DarlingMcpFinOpsInventoryTools.DefaultLimit), PBool("include_removed", false)),
            ["get_finops_recommendations"] = R(CatFinOps, "FinOps recommendations for one server, High severity first; est_savings_usd_month is null without a monthly cost.", PServer()),
            // FinOps web parity (#4843), set A ends.
            // Each set belongs to one series of changes. Append to your own set only,
            // so the two series never edit the same lines of this catalog.
            // Entries keep the catalog's existing order and form.
            // Set A and set B are separated on purpose: keep this gap.
            //
            //
            //
            // FinOps web parity (#4843), set B: append new FinOps entries below this line only.
            ["get_finops"] = R(CatFinOps, "FinOps views for one server, picked by view. view is one of a closed set (today: " + DarlingMcpFinOpsTools.SetAValid + DarlingMcpFinOpsTools.SetBValid + "); an unknown view is refused with the valid list. Windowed over hours; limit caps rows per list.", PServer(), PText("view"), PHours(24), PInt("limit", 10), PText("database_name"), PBool("full_text", false), PText("object_name")),
            // FinOps web parity (#4843), set B ends.
        };

    /// <summary>Builds the <c>/api/catalog</c> body: the reads (names taken from <see cref="BuildReadDispatch"/>,
    /// each enriched with its <see cref="CatalogDescriptors"/> metadata) and the viz vocabulary. Iterating the
    /// dispatch keys guarantees the catalog only advertises actually-dispatchable reads.</summary>
    internal static JsonObject BuildCatalogNode() => BuildCatalogNode(null, Array.Empty<ScheduleOverride>());

    /// <summary>
    /// The catalog with each windowed read's reach (#5562): its <c>hours</c> param carries <c>max_hours</c> (the longest
    /// window the read's validator accepts, from <see cref="WebReadReach"/>), <c>shape</c>, and the main collector's
    /// <c>collector</c> id, <c>collector_interval_minutes</c> (the schedule in force: <paramref name="schedules"/> over
    /// the shipped default) and <c>collector_default_interval_minutes</c>, so the picker can grey out a longer period
    /// with its reason and say "collected every N minutes" under a range too short to hold three samples.
    /// </summary>
    internal static JsonObject BuildCatalogNode(int? serverId, IReadOnlyList<ScheduleOverride> schedules)
    {
        var reads = new JsonArray();
        foreach (var name in BuildReadDispatch().Keys)
        {
            var descriptor = CatalogDescriptors[name];
            var parameters = new JsonArray();
            foreach (var p in descriptor.Params)
            {
                var param = new JsonObject
                {
                    ["name"] = p.Name,
                    ["type"] = p.Type,
                    ["required"] = p.Required,
                    ["default"] = ToJsonValue(p.Default),
                };
                if (p.Name == "hours")
                {
                    AddReach(param, name, serverId, schedules);
                }

                parameters.Add(param);
            }

            reads.Add(new JsonObject
            {
                ["name"] = name,
                ["category"] = descriptor.Category,
                ["description"] = descriptor.Description,
                ["params"] = parameters,
            });
        }

        var viz = new JsonArray();
        foreach (var v in KnownVizList)
        {
            viz.Add(v);
        }

        /* v1 keys (reads/viz) unchanged; the v2 composed-view catalog rides alongside under "compose". */
        return new JsonObject { ["reads"] = reads, ["viz"] = viz, ["compose"] = BuildComposeCatalogNode() };
    }

    /// <summary>Adds a windowed read's reach to its <c>hours</c> catalog param (#5562). A read with no row in
    /// <see cref="WebReadReach.All"/> is advertised at the default reach with no collector; a census keeps that case
    /// from shipping.</summary>
    private static void AddReach(JsonObject param, string read, int? serverId, IReadOnlyList<ScheduleOverride> schedules)
    {
        WebReadReach.All.TryGetValue(read, out var reach);
        var shape = reach?.Shape switch
        {
            ReadShape.BucketedTrend => "bucketed_trend",
            ReadShape.LatestSnapshot => "latest_snapshot",
            _ => "list",
        };
        var collector = reach?.Collector;
        param["max_hours"] = reach?.MaxHours ?? WebReadReach.DefaultHours;
        if (WebReadReach.ViewMaxHours.TryGetValue(read, out var views))
        {
            var viewReach = new JsonObject();
            foreach (var (view, hours) in views)
            {
                viewReach[view] = hours;
            }

            param["view_max_hours"] = viewReach;
        }

        param["shape"] = shape;
        param["collector"] = collector;
        param["collector_interval_minutes"] = WebReadReach.CollectorIntervalMinutes(collector, serverId, schedules);
        param["collector_default_interval_minutes"] = collector is not null && CollectorScheduleDefaults.All.TryGetValue(collector, out var def)
            ? CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(def.FrequencyMinutes)
            : null;
    }

    /// <summary>
    /// The v2 (Custom Views composer) catalog: the measure/dimension/unit-family/aggregate/time-bucket/filter-op
    /// vocabularies the composer picks from, straight off <see cref="MeasureCatalog"/> (the SOLE identifier
    /// source the compiler emits). Served under <c>/api/catalog</c>'s <c>compose</c> key so a single fetch backs
    /// both the v1 read picker and the v2 composer.
    /// </summary>
    internal static JsonObject BuildComposeCatalogNode()
    {
        var measures = new JsonArray();
        foreach (var m in MeasureCatalog.Measures)
        {
            var validAggregates = new JsonArray();
            foreach (var a in m.ValidAggs)
            {
                validAggregates.Add(MeasureCatalog.WireName(a));
            }

            var allowedDimensions = new JsonArray();
            foreach (var d in m.AllowedDimensions)
            {
                allowedDimensions.Add(d);
            }

            string? defaultAggregate = m.Kind == MeasureKind.Ratio ? null : MeasureCatalog.WireName(m.DefaultTimeAgg);

            var node = new JsonObject
            {
                ["key"] = m.Key,
                ["displayName"] = m.DisplayName,
                ["category"] = m.Category,
                ["source"] = m.SourceTable,
                ["kind"] = m.Kind == MeasureKind.Ratio ? "ratio" : "scalar",
                ["archetype"] = m.Archetype.ToString(),
                ["nativeUnit"] = m.NativeUnit,
                ["defaultUnit"] = m.DefaultUnit,
                ["unitFamily"] = m.UnitFamily,
                ["defaultAggregate"] = defaultAggregate,
                ["validAggregates"] = validAggregates,
                ["allowedDimensions"] = allowedDimensions,
                /* Per-server-type availability (design D4), off the owning collector's AppliesTo gate — so the
                   composer can grey a measure a given target can't collect (e.g. Agent measures need msdb). */
                ["appliesTo"] = BuildAppliesToNode(m.SourceTable),
            };

            /* #4653: only a measure that really divides carries a suffix; omitted otherwise, so the payload grows
               by the ratio measures alone (editor.js reads `labelSuffix || ""`, so an absent field renders the same). */
            var labelSuffix = MeasureCatalog.LabelSuffix(m);
            if (labelSuffix.Length > 0)
            {
                node["labelSuffix"] = labelSuffix;
            }

            measures.Add(node);
        }

        var dimensions = new JsonArray();
        foreach (var d in MeasureCatalog.Dimensions)
        {
            dimensions.Add(new JsonObject
            {
                ["source"] = d.SourceTable,
                ["name"] = d.Name,
                ["likeable"] = d.Likeable,
                ["viaModuleJoin"] = d.ViaModuleJoin,
            });
        }

        var unitFamilies = new JsonArray();
        foreach (var family in MeasureCatalog.UnitFamilies)
        {
            var units = new JsonArray();
            foreach (var unit in family.Units)
            {
                units.Add(new JsonObject { ["name"] = unit.Name, ["baseFactor"] = unit.BaseFactor });
            }

            unitFamilies.Add(new JsonObject { ["name"] = family.Name, ["units"] = units });
        }

        /* Event-annotation sources (design D5): the marker overlays the composer offers on a time-series panel.
           Carries appliesTo off the owning collector's gate (like measures, D4) so the composer can grey a source
           a given target can't collect — e.g. default_trace_events / system_health differ by platform. */
        var annotationSources = new JsonArray();
        foreach (var a in MeasureCatalog.AnnotationSources)
        {
            annotationSources.Add(new JsonObject
            {
                ["key"] = a.Key,
                ["displayName"] = a.DisplayName,
                ["category"] = a.Category,
                ["appliesTo"] = BuildAppliesToNode(a.SourceTable),
            });
        }

        return new JsonObject
        {
            ["measures"] = measures,
            ["dimensions"] = dimensions,
            ["annotationSources"] = annotationSources,
            /* server is a virtual dimension present on every source (the fleet axis), not in the per-source
               dimensions list — surfaced here so the composer offers it on every measure. */
            ["universalDimensions"] = ToJsonStringArray(new[] { MeasureCatalog.ServerDimensionName }),
            ["unitFamilies"] = unitFamilies,
            ["aggregates"] = ToJsonStringArray(MeasureCatalog.AggregateWireNames),
            ["timeBuckets"] = ToJsonStringArray(MeasureCatalog.TimeBucketWireNames),
            ["filterOps"] = ToJsonStringArray(MeasureCatalog.FilterOpWireNames),
            /* the v2 composed-panel viz vocabulary (bar/pie/stacked/stacked-bar/area added over v1's KnownViz). */
            ["viz"] = ToJsonStringArray(ComposeSpec.ComposeVizList),
        };
    }

    /// <summary>
    /// The MCP <c>describe_custom_view_catalog</c> tool's DEFAULT shape (#4198 — the full catalog measured 98,173
    /// bytes at default arguments, three times the tool's 32 KB response budget). Re-groups
    /// <see cref="BuildComposeCatalogNode"/>'s flat <c>measures</c> by <c>source</c> (collector table) and keeps
    /// only the fields an author needs to pick a measure and know its legal vocabulary: <c>key</c>,
    /// <c>displayName</c> (the one-line purpose), <c>kind</c>, <c>unitFamily</c>, <c>validAggregates</c> (this one
    /// VARIES within a source — e.g. a ratio measure's is empty where its source's scalars allow sum/avg/min/max —
    /// so, unlike <c>allowedDimensions</c>, it cannot be hoisted to the source level without hiding a real
    /// restriction). Left out: <c>category</c>/<c>archetype</c> (neither is part of the vocabulary a panel spec
    /// names — <see cref="ComposeSpec"/>'s required panel fields are measure/aggregate/unit, never these),
    /// <c>nativeUnit</c>/<c>defaultUnit</c>/<c>defaultAggregate</c> (suggestions, not requirements — any unit in
    /// the measure's family and any aggregate in its validAggregates is legal), <c>allowedDimensions</c> (uniform
    /// across every measure of a source — pinned by <c>DarlingComposeTests</c> — so it is the source's dimension
    /// list, one <see cref="FilterComposeCatalogNodeBySource"/> call away), and <c>appliesTo</c> (server-type
    /// availability is a UI greying hint for the web composer, per design D4 — it gates nothing at compose time).
    /// Every dropped field is reachable per-source via <c>source=&lt;name&gt;</c>
    /// (<see cref="FilterComposeCatalogNodeBySource"/>) or for the whole catalog via <c>full_detail=true</c>
    /// (this method's own return, unfiltered).
    ///
    /// <para>Takes the ALREADY-BUILT full node so the compact view is a pure re-shape of it, never a second read
    /// of <see cref="MeasureCatalog"/> — it can't drift from what <c>full_detail=true</c> and <c>/api/catalog</c>
    /// (which never compacts; the web Custom Views editor needs the fields this trims) both serve.</para>
    /// </summary>
    internal static JsonObject BuildComposeCatalogCompactNode(JsonObject full)
    {
        var sources = new JsonArray();
        foreach (var sourceGroup in full["measures"]!.AsArray()
                     .Select(m => m!.AsObject())
                     .GroupBy(m => m["source"]!.GetValue<string>(), StringComparer.Ordinal))
        {
            var measures = new JsonArray();
            foreach (var m in sourceGroup)
            {
                measures.Add(new JsonObject
                {
                    ["key"] = m["key"]!.GetValue<string>(),
                    ["displayName"] = m["displayName"]!.GetValue<string>(),
                    ["kind"] = m["kind"]!.GetValue<string>(),
                    ["unitFamily"] = m["unitFamily"]!.GetValue<string>(),
                    ["validAggregates"] = JsonNode.Parse(m["validAggregates"]!.ToJsonString()),
                });
            }

            sources.Add(new JsonObject { ["source"] = sourceGroup.Key, ["measures"] = measures });
        }

        var annotationSources = new JsonArray();
        foreach (var a in full["annotationSources"]!.AsArray())
        {
            var ao = a!.AsObject();
            annotationSources.Add(new JsonObject
            {
                ["key"] = ao["key"]!.GetValue<string>(),
                ["displayName"] = ao["displayName"]!.GetValue<string>(),
                ["category"] = ao["category"]!.GetValue<string>(),
            });
        }

        return new JsonObject
        {
            ["sources"] = sources,
            ["annotationSources"] = annotationSources,
            ["universalDimensions"] = JsonNode.Parse(full["universalDimensions"]!.ToJsonString()),
            ["unitFamilies"] = JsonNode.Parse(full["unitFamilies"]!.ToJsonString()),
            ["aggregates"] = JsonNode.Parse(full["aggregates"]!.ToJsonString()),
            ["timeBuckets"] = JsonNode.Parse(full["timeBuckets"]!.ToJsonString()),
            ["filterOps"] = JsonNode.Parse(full["filterOps"]!.ToJsonString()),
            ["viz"] = JsonNode.Parse(full["viz"]!.ToJsonString()),
            ["compact"] = true,
            ["note"] = "Compact by default (#4198): each source lists its measures' key/displayName/kind/unitFamily/validAggregates " +
                "only. Call describe_custom_view_catalog(source=\"<name>\") for that source's FULL per-measure detail " +
                "(category, archetype, nativeUnit, defaultUnit, defaultAggregate, allowedDimensions, appliesTo) plus its own " +
                "dimensions, or full_detail=true for the complete catalog (measures/dimensions/annotationSources as flat arrays, " +
                "every field, exactly like this tool returned before #4198).",
        };
    }

    /// <summary>
    /// The MCP <c>describe_custom_view_catalog</c> tool's <c>source=&lt;name&gt;</c> drill-down (#4198): the FULL
    /// per-entry detail <see cref="BuildComposeCatalogNode"/> serves, filtered to one source (collector table)'s
    /// measures and dimensions. <c>annotationSources</c> rides along whole either way — five entries total, cheap
    /// regardless of the filter, and its underlying source table is not itself a served field to filter on. An
    /// unmatched <paramref name="source"/> comes back with empty <c>measures</c>/<c>dimensions</c> and a
    /// <c>note</c> pointing at the default (unfiltered) call for the real source names, rather than an error
    /// envelope — this tool has never returned one, and a typo should not change the shape the caller parses.
    /// </summary>
    internal static JsonObject FilterComposeCatalogNodeBySource(JsonObject full, string source)
    {
        var measures = new JsonArray(full["measures"]!.AsArray()
            .Where(m => string.Equals(m!["source"]!.GetValue<string>(), source, StringComparison.Ordinal))
            .Select(m => JsonNode.Parse(m!.ToJsonString())!)
            .ToArray());

        var dimensions = new JsonArray(full["dimensions"]!.AsArray()
            .Where(d => string.Equals(d!["source"]!.GetValue<string>(), source, StringComparison.Ordinal))
            .Select(d => JsonNode.Parse(d!.ToJsonString())!)
            .ToArray());

        var result = new JsonObject
        {
            ["source"] = source,
            ["measures"] = measures,
            ["dimensions"] = dimensions,
            ["annotationSources"] = JsonNode.Parse(full["annotationSources"]!.ToJsonString()),
            ["universalDimensions"] = JsonNode.Parse(full["universalDimensions"]!.ToJsonString()),
            ["unitFamilies"] = JsonNode.Parse(full["unitFamilies"]!.ToJsonString()),
            ["aggregates"] = JsonNode.Parse(full["aggregates"]!.ToJsonString()),
            ["timeBuckets"] = JsonNode.Parse(full["timeBuckets"]!.ToJsonString()),
            ["filterOps"] = JsonNode.Parse(full["filterOps"]!.ToJsonString()),
            ["viz"] = JsonNode.Parse(full["viz"]!.ToJsonString()),
        };

        if (measures.Count == 0)
        {
            result["note"] = $"No measures found for source '{source}'. Call describe_custom_view_catalog() with no arguments to list the valid source names.";
        }

        return result;
    }

    /// <summary>Collector definition by its destination table, for the per-measure availability lookup (a
    /// measure's <c>SourceTable</c> is a collector <c>TargetTable</c>, pinned by <c>DarlingComposeTests</c>).</summary>
    private static readonly Dictionary<string, ICollectorSchemaInfo> s_collectorByTable =
        CollectorCatalog.All.ToDictionary(c => c.TargetTable, StringComparer.Ordinal);

    /* Representative target profiles a measure's collector AppliesTo gate is evaluated against (design D4).
       SqlMajorVersion is pinned to a supported major (16 = SQL 2022) so the version-gated collectors
       (query_stats/query_store, 2016+) read as available. */
    private static readonly CollectorTargetInfo s_onPremTarget = new() { SqlMajorVersion = 16, HasMsdbAccess = true };

    /* The SQL-Agent collectors, NAMED rather than probed. This used to be detected by evaluating the gate
       against an msdb-less profile - "available with msdb, gated without it" - which stopped working when
       #2559 removed HasMsdbAccess from those gates. The dependency itself did not go away: these three read
       msdb.dbo.sysjobs and friends, so a panel built on them still needs the grant. What changed is what its
       absence produces - a PERMISSIONS row rather than no dispatch - which arguably makes the badge MORE
       useful, since the data starts flowing the moment the grant lands with no reconnect.

       Honest limit: this cannot notice a NEW collector that reads msdb. DarlingComposeTests pins that the set
       is exactly these three and that each one's query really does reference msdb, which catches a rename or
       a collector that stopped reading it - not an addition. */
    private static readonly HashSet<string> s_msdbBackedTables =
        new(StringComparer.Ordinal) { "running_jobs", "job_history", "agent_status" };
    private static readonly CollectorTargetInfo s_azureSqlDbTarget = new() { IsAzureSqlDb = true, SqlMajorVersion = 16, HasMsdbAccess = false };
    private static readonly CollectorTargetInfo s_azureMiTarget = new() { IsAzureManagedInstance = true, SqlMajorVersion = 16, HasMsdbAccess = true };
    private static readonly CollectorTargetInfo s_awsRdsTarget = new() { IsAwsRds = true, SqlMajorVersion = 16, HasMsdbAccess = true };

    /* PostgreSQL target profiles (#3285). Engine = PostgreSql so the dispatch gate's engine half
       (CollectorCatalog.EngineMatches) selects the PostgreSQL collectors and excludes every SQL Server one -
       and excludes these PG collectors from the four SQL profiles above. The major/version are pinned modern
       so the version-gated PG collectors (pg_stat_io PG16+, pg_write_stats PG14+) read as available, and
       IsInRecovery defaults false so the writer-only collectors (autovacuum, index/table stats) do too.
       s_postgresTarget is stock PostgreSQL: IsAurora is false, so the Aurora-only collectors (pg_wait_stats,
       pg_cpu_utilization) correctly read unavailable there; s_auroraTarget flips IsAurora on. */
    private static readonly CollectorTargetInfo s_postgresTarget =
        new() { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, PostgresVersionNum = 170_005 };
    private static readonly CollectorTargetInfo s_auroraTarget =
        new() { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true, PostgresMajorVersion = 17, PostgresVersionNum = 170_005 };

    /// <summary>The per-server-type availability of a measure (design D4), derived from its owning collector's
    /// target gate so the composer can label/grey a measure a given server type can't collect. Each flag is
    /// the FULL dispatch gate <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>
    /// (<c>EngineMatches AND definition.AppliesTo</c>), NOT the raw <see cref="ICollectorDefinition{TRow}.AppliesTo"/>
    /// override: the engine half is what makes a PostgreSQL measure read unavailable on the SQL Server profiles
    /// and a SQL Server measure unavailable on the PostgreSQL profiles. <c>postgres</c>/<c>aurora</c> were added
    /// with the #3285 PostgreSQL measures; <c>needsMsdb</c> is the SQL-Agent dependency (job/agent measures),
    /// taken from the named SQL-Agent set rather than probed from the gate - see the note there on why #2559
    /// made probing impossible. Returns null only if a measure's source has no collector (impossible, pinned by test).</summary>
    private static JsonObject? BuildAppliesToNode(string sourceTable)
    {
        if (!s_collectorByTable.TryGetValue(sourceTable, out var collector))
        {
            return null;
        }

        return new JsonObject
        {
            /* CollectorCatalog.AppliesTo, NOT the raw collector.AppliesTo: the former is
               EngineMatches && definition.AppliesTo, so a PostgreSQL collector (including one gated `=> true`)
               correctly reads FALSE on the SQL Server profiles and a SQL Server collector reads FALSE on the
               PostgreSQL profiles. The raw override skips the engine half and reported a `=> true` PG collector
               as available on every SQL Server target (#3285). */
            ["onPrem"] = CollectorCatalog.AppliesTo(collector, s_onPremTarget),
            ["azureSqlDb"] = CollectorCatalog.AppliesTo(collector, s_azureSqlDbTarget),
            ["azureMi"] = CollectorCatalog.AppliesTo(collector, s_azureMiTarget),
            ["awsRds"] = CollectorCatalog.AppliesTo(collector, s_awsRdsTarget),
            ["postgres"] = CollectorCatalog.AppliesTo(collector, s_postgresTarget),
            ["aurora"] = CollectorCatalog.AppliesTo(collector, s_auroraTarget),
            ["needsMsdb"] = s_msdbBackedTables.Contains(sourceTable),
        };
    }

    private static JsonArray ToJsonStringArray(IReadOnlyList<string> values)
    {
        var array = new JsonArray();
        foreach (var value in values)
        {
            array.Add(value);
        }

        return array;
    }

    private static JsonValue? ToJsonValue(object? value) => value switch
    {
        null => null,
        int i => JsonValue.Create(i),
        bool b => JsonValue.Create(b),
        double d => JsonValue.Create(d),
        _ => JsonValue.Create(value.ToString()),
    };

    /* ── request body parsing + response building ── */

    /// <summary>A parsed create/update body: name (required, trimmed), description (optional), the definition as
    /// raw JSON text (re-serialized from the parsed node), version (present only on updates), and (#6214) the folder
    /// (<see cref="FolderGiven"/> tells an omitted key from an explicit null, which means the top level) and the personal flag
    /// (null when omitted).</summary>
    private sealed record ViewWriteRequest(
        string Name, string? Description, string DefinitionJson, int? Version, bool FolderGiven = false, string? Folder = null, bool? Personal = null);

    /// <summary>Parses the request body into a <see cref="ViewWriteRequest"/>, or returns a caller-facing error
    /// (the definition's STRUCTURE is validated separately by <see cref="ValidateDefinition"/>).</summary>
    private static async Task<(ViewWriteRequest? Request, string? Error)> ParseViewBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var name = TryGetString(obj, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return (null, "'name' is required.");
        }

        var description = TryGetString(obj, "description");

        var definitionNode = obj["definition"];
        if (definitionNode is null)
        {
            return (null, "'definition' is required.");
        }

        var definitionJson = definitionNode.ToJsonString();

        int? version = null;
        if (obj["version"] is JsonValue versionValue && versionValue.TryGetValue<int>(out var v))
        {
            version = v;
        }

        var folderGiven = obj.ContainsKey("folder");
        string? folder = null;
        if (folderGiven && obj["folder"] is { } folderNode)
        {
            if (folderNode is not JsonValue folderValue || !folderValue.TryGetValue<string>(out var folderText))
            {
                return (null, "'folder' must be a string, or null for the top level.");
            }

            folder = CustomViewStore.NormalizeFolder(folderText, out var folderError);
            if (folderError is not null)
            {
                return (null, folderError);
            }
        }

        bool? personal = null;
        if (obj["personal"] is { } personalNode)
        {
            if (personalNode is not JsonValue personalValue || !personalValue.TryGetValue<bool>(out var personalFlag))
            {
                return (null, "'personal' must be true or false.");
            }

            personal = personalFlag;
        }

        return (new ViewWriteRequest(name.Trim(), description, definitionJson, version, folderGiven, folder, personal), null);
    }

    /// <summary>The full single-view wire shape (definition embedded as JSON, NOT an escaped string). Shared by
    /// the web <c>GET/POST/PUT /api/views</c> responses and the MCP <c>get_custom_view</c> / <c>create_custom_view</c>
    /// / <c>update_custom_view</c> tools, so the two surfaces return an identical view shape.</summary>
    internal static JsonObject BuildFullViewNode(CustomView view) => new()
    {
        ["id"] = view.Id,
        ["name"] = view.Name,
        ["description"] = view.Description,
        ["definition"] = JsonNode.Parse(view.DefinitionJson),
        ["version"] = view.Version,
        ["created_at"] = view.CreatedAt,
        ["updated_at"] = view.UpdatedAt,
        ["updated_by"] = view.UpdatedBy,
        ["folder"] = view.Folder,
        ["personal"] = view.Personal,
        ["created_by"] = view.CreatedBy,
        ["mine"] = view.Mine,
    };

    /// <summary>The 400 sentence for a personal view a session cannot own (#6214): a session that names a person (a cookie from before
    /// owner keys existed) signs in again; one with no person at all has none to give.</summary>
    internal static string NoOwnerKeyMessage(DarlingWebSeat seat) =>
        seat.Subject is null ? CustomViewStore.NoIdentityMessage : CustomViewStore.SignInAgainMessage;

    /// <summary>The bare-array list wire shape (no definition body). Carries the view <c>kind</c> — always
    /// concrete (<c>"dashboard"</c> when the stored definition declares none) so the list page/sidebar can badge
    /// and route a notebook vs a dashboard without fetching each full definition.</summary>
    internal static JsonArray BuildSummariesNode(IReadOnlyList<CustomViewSummary> views)
    {
        var array = new JsonArray();
        foreach (var view in views)
        {
            array.Add(new JsonObject
            {
                ["id"] = view.Id,
                ["name"] = view.Name,
                ["description"] = view.Description,
                ["version"] = view.Version,
                ["updated_at"] = view.UpdatedAt,
                ["updated_by"] = view.UpdatedBy,
                ["kind"] = view.Kind ?? "dashboard",
                ["folder"] = view.Folder,
                ["personal"] = view.Personal,
                ["created_by"] = view.CreatedBy,
                ["mine"] = view.Mine,
            });
        }

        return array;
    }

    /* ── custom-alert-rule body parsing + response building (#3285) ── */

    /// <summary>A parsed alert create/update body: name (required, trimmed), description (optional), the
    /// definition as raw JSON text (re-serialized from the parsed node), <c>enabled</c> (defaults true when the
    /// key is absent, matching create's default and a full-replace PUT), and version (present only on updates).
    /// The definition's STRUCTURE is validated separately by <see cref="CustomAlertRuleDefinition.TryParse"/>.</summary>
    private sealed record AlertWriteRequest(string Name, string? Description, string DefinitionJson, bool Enabled, int? Version, string? ExpiresAtText = null);

    /// <summary>Parses an alert create/update body into an <see cref="AlertWriteRequest"/>, or a caller-facing
    /// error. Mirrors <see cref="ParseViewBodyAsync"/>, adding the <c>enabled</c> field alert rules carry.</summary>
    private static async Task<(AlertWriteRequest? Request, string? Error)> ParseAlertBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var name = TryGetString(obj, "name");
        if (string.IsNullOrWhiteSpace(name))
        {
            return (null, "'name' is required.");
        }

        var description = TryGetString(obj, "description");

        var definitionNode = obj["definition"];
        if (definitionNode is null)
        {
            return (null, "'definition' is required.");
        }

        var definitionJson = definitionNode.ToJsonString();

        /* Absent 'enabled' defaults true: create's default, and a full-replace PUT of a rule that names no
           enabled state is an enabled rule (the real full-document editor always sends the field). */
        var enabled = true;
        if (obj["enabled"] is JsonValue enabledValue && enabledValue.TryGetValue<bool>(out var e))
        {
            enabled = e;
        }

        int? version = null;
        if (obj["version"] is JsonValue versionValue && versionValue.TryGetValue<int>(out var v))
        {
            version = v;
        }

        /* #6235: the optional expiry. Absent, null or empty means the rule does not expire (a full-replace PUT clears it, like every
           other field the editor sends); anything else is parsed by the store call below, so a bad value is a 400, not a throw. */
        string? expiresText = null;
        if (obj["expires_at_utc"] is JsonValue expiresValue)
        {
            if (!expiresValue.TryGetValue<string>(out var expiresString))
            {
                return (null, $"'expires_at_utc' must be {CustomAlertExpiry.Spelling}, or null for a rule that does not expire.");
            }

            expiresText = string.IsNullOrWhiteSpace(expiresString) ? null : expiresString;
        }

        return (new AlertWriteRequest(name.Trim(), description, definitionJson, enabled, version, expiresText), null);
    }

    /// <summary>Extracts just the embedded <c>definition</c> object from a body as raw JSON text — the
    /// <c>POST /api/alerts/validate</c> shape — or a caller-facing error. The definition rides as a JSON object
    /// (not an escaped string), the same way the create/update bodies carry it.</summary>
    private static async Task<(string? DefinitionJson, string? Error)> ParseDefinitionBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, "Request body must be a JSON object.");
        }

        var definitionNode = obj["definition"];
        return definitionNode is null
            ? (null, "'definition' is required.")
            : (definitionNode.ToJsonString(), null);
    }

    /// <summary>Parses a <c>POST /api/alerts/test</c> body into <c>(rule_id, definition)</c> — both optional here
    /// (the wrapped <see cref="Mcp.DarlingMcpCustomAlertTools.TestCustomAlertRule"/> enforces the rule_id XOR
    /// definition rule and reports a violation in its envelope). Errors only on a body that is not a JSON object.
    /// The definition rides as an embedded JSON object, re-serialized to the text the tool parses.</summary>
    private static async Task<(long? RuleId, string? DefinitionJson, string? Error)> ParseAlertTestBodyAsync(HttpContext context)
    {
        JsonNode? root;
        try
        {
            root = await JsonNode.ParseAsync(context.Request.Body, cancellationToken: context.RequestAborted);
        }
        catch (JsonException)
        {
            return (null, null, "Request body is not valid JSON.");
        }

        if (root is not JsonObject obj)
        {
            return (null, null, "Request body must be a JSON object.");
        }

        long? ruleId = null;
        if (obj["rule_id"] is JsonValue ruleIdValue && ruleIdValue.TryGetValue<long>(out var r))
        {
            ruleId = r;
        }

        var definitionNode = obj["definition"];
        var definitionJson = definitionNode?.ToJsonString();

        return (ruleId, definitionJson, null);
    }

    /// <summary>The full single-rule wire shape — IDENTICAL to
    /// <see cref="Mcp.DarlingMcpCustomAlertTools"/>'s (definition embedded as JSON, plus the <c>enabled</c>
    /// column alert rules carry), so the web GET/POST/PUT and the MCP get/create/update return the same rule
    /// shape.</summary>
    internal static JsonObject BuildFullRuleNode(CustomAlertRule rule) =>
        JsonSerializer.SerializeToNode(new CustomAlertRuleImage(rule.Id, rule.Name, rule.Description, JsonNode.Parse(rule.DefinitionJson),
            rule.Enabled, rule.Version, rule.CreatedAt, rule.UpdatedAt, rule.UpdatedBy,
            CustomAlertExpiry.Format(rule.ExpiresAtUtc), rule.IsExpiredAt(DateTime.UtcNow)))!.AsObject();

    /// <summary>The bare-array list wire shape (no definition body), carrying <c>enabled</c> so the list can
    /// badge a paused rule without fetching each full definition — plus <c>last_fired</c> (#3360), the naive-UTC
    /// ISO-8601 instant the rule most recently fired (JSON <c>null</c> when it never has) so a card can show
    /// "last fired …" / "never fired" without a per-rule read. Mirrors the MCP list surface (they share this
    /// builder AND <see cref="CustomAlertRuleSummary"/>, so the field reaches both surfaces identically).</summary>
    internal static JsonArray BuildRuleSummariesNode(IReadOnlyList<CustomAlertRuleSummary> rules)
    {
        var array = new JsonArray();
        foreach (var rule in rules)
        {
            array.Add(new JsonObject
            {
                ["id"] = rule.Id,
                ["name"] = rule.Name,
                ["description"] = rule.Description,
                ["enabled"] = rule.Enabled,
                ["version"] = rule.Version,
                ["updated_at"] = rule.UpdatedAt,
                ["updated_by"] = rule.UpdatedBy,
                ["last_fired"] = rule.LastFired,
                ["expires_at_utc"] = CustomAlertExpiry.Format(rule.ExpiresAtUtc),
                ["expired"] = rule.IsExpiredAt(DateTime.UtcNow),
            });
        }

        return array;
    }

    /* ── result helpers (JSON body written verbatim, bypassing any serializer naming policy) ── */

    private static IResult JsonNodeResult(JsonNode node, int statusCode = StatusCodes.Status200OK) =>
        Results.Text(node.ToJsonString(), "application/json", statusCode: statusCode);

    private static IResult CreatedResult(HttpContext context, string location, JsonNode node)
    {
        context.Response.Headers.Location = location;
        return Results.Text(node.ToJsonString(), "application/json", statusCode: StatusCodes.Status201Created);
    }

    private static IResult ErrorResult(string message, int statusCode) =>
        JsonNodeResult(new JsonObject { ["error"] = message }, statusCode);

    private static IResult NotFoundResult() =>
        ErrorResult("View not found.", StatusCodes.Status404NotFound);

    private static IResult AlertNotFoundResult() =>
        ErrorResult("Alert rule not found.", StatusCodes.Status404NotFound);

    private static IResult UnsupportedMediaTypeResult() =>
        ErrorResult("Content-Type must be application/json.", StatusCodes.Status415UnsupportedMediaType);

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
