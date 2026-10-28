/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Storage;
using System.Text.Json.Serialization;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The service's configuration file (darling.json): the Postgres store and the monitored
/// servers. Headless plan D-M2-1 — deliberately minimal: no schedule knobs (the shared
/// CollectorScheduleDefaults apply; defaults over speculative config), integrated auth
/// recommended, SQL-auth passwords DPAPI-protected via the service's --encrypt-password verb.
/// Resolution order: explicit path → DARLING_CONFIG environment variable → darling.json next
/// to the service binary.
/// </summary>
public sealed class DarlingConfig
{
    private static readonly JsonSerializerOptions s_jsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    [JsonPropertyName("postgres")]
    public PostgresConfig Postgres { get; set; } = new();

    /// <summary>
    /// #4535: the plan analyzer's per-rule disable/severity-override config, read from an optional
    /// <c>analyzer</c> section in darling.json — the same shape as erikdarlingdata/PerformanceStudio's
    /// <c>.planview.json</c> root (<see cref="PerformanceMonitor.PlanAnalysis.AnalyzerConfig"/>). Null
    /// (the section omitted entirely) behaves exactly like an explicit <c>AnalyzerConfig.Default</c>:
    /// no rule disabled, no severity overridden. Not auto-discovered from any other file — PM does not
    /// couple to PlanViewer's <c>~/.planview.json</c> convention.
    /// </summary>
    [JsonPropertyName("analyzer")]
    public PerformanceMonitor.PlanAnalysis.AnalyzerConfig? Analyzer { get; set; }

    [JsonPropertyName("servers")]
    public List<MonitoredServer> Servers { get; set; } = new();

    /// <summary>
    /// Which AWS roles the web and MCP may set for an Amazon RDS or Aurora target (#5452): IAM role ARNs, or 12-digit
    /// AWS account ids that allow every role in the account (a bare id is an account in the <c>aws</c> partition;
    /// <c>aws-cn:123456789012</c> and <c>aws-us-gov:123456789012</c> name the others). The service assumes a saved role only if it is listed
    /// here or a <c>servers[]</c> entry in this file names it in <c>awsRoleArn</c>. Read at start, so an edit applies
    /// on restart. Empty by default: only the roles the file's own servers name run.
    /// </summary>
    [JsonPropertyName("allowedAwsRoles")]
    public List<string> AllowedAwsRoles { get; set; } = new();

    /// <summary>
    /// Capture execution-plan text. Default TRUE for Darling. Since #1767 a query_stats plan is
    /// stored ONCE per distinct plan in <c>query_plan_dim</c> and the fact row carries only its
    /// content digest (<c>query_plan_digest</c>), so re-collecting the same cached plan every cycle
    /// costs a hash rather than another copy of the XML — which is what makes keeping plans cheap.
    /// (query_store_stats.query_plan_text is still stored inline; it is not deduplicated yet.)
    /// Unlike Lite, which stores to DuckDB/parquet and deliberately never captures plans. Set false
    /// to skip plan capture entirely. Feeds <see cref="CollectorContext.CapturePlanXml"/> in the
    /// shared query_stats / query_store collectors.
    /// </summary>
    [JsonPropertyName("capturePlans")]
    public bool CapturePlans { get; set; } = true;

    /// <summary>
    /// Whether the query_store backfill loop runs at all (#2167). Store-backed (config_service, V58) and
    /// read live by the worker's loop, so an operator can stop a runaway drain (a freshly restored catalog
    /// against a cross-region server) without a restart and without touching plan capture. Default on.
    /// </summary>
    [JsonPropertyName("queryStoreBackfillEnabled")]
    public bool QueryStoreBackfillEnabled { get; set; } = true;

    /// <summary>
    /// Per-database text byte budget for the query_store collector, in MEGABYTES (#2164). Store-backed
    /// (config_service, V59), clamped [4,256] on read, default 64 = the previous compile-time constant.
    /// Lower it when the monitored fleet is a network hop away: the budget bounds memory, but it also
    /// sets how long one collector query holds the monitored server open draining to this client, which
    /// over a cross-region link is the tenant-visible cost. A cut is always resumable (#1960), so a
    /// smaller budget trades catch-up latency for shorter statements — never data.
    /// </summary>
    [JsonPropertyName("queryStoreTextBudgetMb")]
    public int QueryStoreTextBudgetMb { get; set; } = 64;

    /// <summary>
    /// #6211: how many plans per database-hour, on each of seven metrics (CPU, duration, physical reads, writes, memory grant, tempdb,
    /// executions), the Query Store collector fetches the statement text and plan XML of. Forced plans, the other plans of a query with a
    /// forced plan, and plans named by plan correction are fetched whatever their rank. The rest keep their runtime statistics and are fetched
    /// by a later run if they rank then. Seeds <c>config_service.query_store_fetch_top_n</c> (V209) on a fresh store; the store wins after
    /// that, and a change there reaches the next cycle with no restart. Default 100. 0 or less fetches every new plan (the behaviour before this).
    /// </summary>
    [JsonPropertyName("queryStoreFetchTopN")]
    public int QueryStoreFetchTopN { get; set; } = 100;

    /// <summary>
    /// #2316: how many days a stored plan XML outlives its last sighting before the dimension GC may
    /// take it — the bound the fact-coupled horizon cannot provide on a store younger than the fact
    /// retention (measured: 127 GB of parameter-sniffing plan churn in the dim's first 22 days, with
    /// the coupled GC unable to fire until a month after projected disk-full). Facts keep their full
    /// retention; a plan older than this renders as a missing plan, which every reader handles.
    /// 0 disables (fact-coupled horizon alone); enabled values clamp to [7,365] on read.
    /// </summary>
    [JsonPropertyName("planContentRetentionDays")]
    public int PlanContentRetentionDays { get; set; } = 21;

    /// <summary>
    /// The per-session <c>statement_timeout</c> applied to the viewer and mcp roles — the hard backstop a
    /// composed query can never exceed (#2357). Seeds <c>config_service.compose_statement_timeout_seconds</c>;
    /// the store is authoritative afterwards, and since #2918 a reload delivers a change like every other
    /// knob here — on a managed store. The mechanism is still unlike the others, and the difference shows
    /// at the edges.
    ///
    /// <para><b>It lives on the roles, not in a query.</b> The ceiling is
    /// <c>ALTER ROLE viewer/mcp SET statement_timeout</c>, written by startup provisioning and — since
    /// #2918 — re-asserted by a control-plane reload when the value changed
    /// (<see cref="!:DarlingManagedRoles.ReassertComposeStatementTimeoutAsync"/>). Three consequences a
    /// reader of this property should not have to rediscover:</para>
    ///
    /// <para>1. <b>Re-assertion happens only where the service provisioned the roles</b> — managed mode on
    /// Windows, and the compose distribution's own store (#3914) — mirroring the gate on provisioning, which is
    /// where these roles get CREATED. Any other BYO store provisions them out-of-band through
    /// <c>tools/provision-roles.sql</c> and names them itself, so there the old restart-scoped caveat still
    /// holds — and an operator has to re-run that script by hand.</para>
    ///
    /// <para>2. <b>A role <c>SET</c> only takes on that role's NEXT session.</b> Lowering the ceiling bounds
    /// the next runaway, not the one already running; an already-connected viewer keeps the old value until
    /// it reconnects. This is not a kill switch.</para>
    ///
    /// <para>3. Reading this property tells you the store's <i>desired</i> value. It is kept in sync with
    /// what was last successfully written to the roles on the paths above, but a failed re-assertion leaves
    /// the roles behind until the next reload or start converges them.</para>
    ///
    /// <para>60 preserves the constant it replaces (raised from a shipped 15 by #4442, whose migration rung
    /// moves an existing store's shipped-15 value to 60 too). It is a judgement about store size and disk
    /// speed, which this product cannot make for someone else's deployment — a fleet-wide aggregate over a
    /// wide window on a large store can exceed 60s with nothing wrong.</para>
    /// </summary>
    public int ComposeStatementTimeoutSeconds { get; set; } = 60;

    /// <summary>
    /// The plan-XML storage codec (#2171). Store-backed (config_service, V62), normalized to 'gzip' or
    /// 'none' on read. 'gzip' (default, unchanged): plans live as gzip bytes in query_plan_gz - 14.0x
    /// measured, readable only through the apps/MCP. 'none': plans written as plain text into
    /// query_plan_xml - lz4 TOAST compresses ~8.9x, and anything reading the store directly over SQL
    /// (Grafana, report tooling) gets plan XML back with no extension and no UDF. Flipping it affects
    /// NEW rows only; the readers' text-first-else-gz resolution covers every mix. The dimension is
    /// content-addressed either way, so the digest and dedup are codec-independent.
    /// </summary>
    [JsonPropertyName("planXmlCompression")]
    public string PlanXmlCompression { get; set; } = "gzip";

    /// <summary>
    /// How many per-server collection bodies may hold a SQL connection at once (#2170) — the #1553 fleet
    /// gate, previously hardcoded to 4. Store-backed (config_service, V59), clamped [1,16], default 4.
    /// Raise it on a host with headroom watching a large fleet, where 4-wide serialization is what makes
    /// sweeps queue and the Fleet Health screen report staleness while every collector is healthy.
    /// Peak transient memory is roughly this × <see cref="QueryStoreTextBudgetMb"/>.
    /// </summary>
    [JsonPropertyName("maxConcurrentSweeps")]
    public int MaxConcurrentSweeps { get; set; } = 4;

    /// <summary>
    /// Whether the default_trace_events collector records Object:Created/Altered/Deleted schema-change
    /// (DDL) events. Default TRUE (today's behavior). Set false on a noisy or benchmark box where a
    /// create/drop-happy workload floods the viewer's System Events &gt; Default Trace tab — e.g. HammerDB's
    /// TPC-H Query 15 creates and drops a <c>revenue</c> view thousands of times, and the collector
    /// faithfully records every create/delete. Only the Object DDL slice is suppressed; the file-growth,
    /// ErrorLog, and security-audit categories are still collected. Feeds
    /// <see cref="CollectorContext.CollectSchemaChangeEvents"/> — the shared collector's equivalent of the
    /// full Dashboard proc's <c>@include_object_events</c>. A file-only knob (not seeded into the
    /// control-plane store), so an edit takes effect on the next restart.
    /// </summary>
    [JsonPropertyName("collectSchemaChangeEvents")]
    public bool CollectSchemaChangeEvents { get; set; } = true;

    /// <summary>
    /// Whether the raw hypertable chunk-interval reconcile runs at all (#4211). Default TRUE. Rides the daily
    /// retention purge tick (first pass after startup, then every 24h) and, for every raw hypertable, applies
    /// <see cref="PerformanceMonitor.Darling.Storage.RawChunkIntervalPlanner"/>'s decisions with
    /// <c>set_chunk_time_interval</c>. Set false to keep every table's interval exactly where it is — an
    /// operator override survives restarts, and nothing is applied or recorded to
    /// <c>collect.raw_chunk_interval_rung_history</c> while this is off. A file-only knob (not seeded into the
    /// control-plane store, unlike <see cref="QueryStoreBackfillEnabled"/>), so an edit takes effect on the
    /// next restart.
    /// </summary>
    [JsonPropertyName("rawChunkIntervalReconcileEnabled")]
    public bool RawChunkIntervalReconcileEnabled { get; set; } = true;

    /// <summary>
    /// #2862: how many collection cycles pass between plan-XML captures for <c>procedure_stats</c> — 1 is
    /// every cycle (the pre-#2862 collector, byte-identical), 4 is one cycle in four. Clamped to [1,60] on
    /// read. Only <c>procedure_stats</c> is gated; every other plan-capturing collector is untouched.
    ///
    /// <para><b>Why the knob exists.</b> procedure_stats is the most expensive collector on the production
    /// us-east-1 store, and a controlled decomposition attributed 73.8% of its read loop to RENDERING plan
    /// XML and a further 26.0% to shipping it, against 0.2% for the same query with no plan apply at all.
    /// The render happens inside <c>sys.dm_exec_text_query_plan</c>, a server-side TVF, so it is CPU burned
    /// on the MONITORED server — which is why the lever is cadence and not a dedup key: hashing at the
    /// source would remove the transfer and leave the render on customer hardware.</para>
    ///
    /// <para>The cost paid is plan-data granularity: a captured plan is up to this many cycles old. 4 keeps
    /// the worst-case plan age inside the collector's own ten-minute <c>last_execution_time</c> candidate
    /// window, so a module busy enough to be among the first MaxPlansPerRun (150) kept rows is still busy when its plan is next
    /// rendered. Runtime statistics are unaffected — they are collected at full resolution every cycle.</para>
    ///
    /// <para>A file-only knob (not seeded into the control-plane store), exactly like
    /// <see cref="CollectSchemaChangeEvents"/> above, so an edit takes effect on the next restart and this
    /// needs no schema rung. It is read through a live provider, so promoting it to a store column later is
    /// a change to the source only and not to the collector seam.</para>
    /// </summary>
    [JsonPropertyName("procedureStatsPlanCycleInterval")]
    public int ProcedureStatsPlanCycleInterval { get; set; } = 4;

    /// <summary>
    /// #5158: whether <c>query_stats</c> fetches plan XML only for statement plans this host has not already
    /// committed. On (the default), the main query carries no plan and a second target query renders just the
    /// plans the host has not stored; rows for plans it has stored carry the existing digest, so every reader
    /// sees what it saw before. Off restores the inline capture, which renders and ships every row's plan every run.
    ///
    /// <para><b>Why.</b> On one large store the inline capture rendered 54 GB of plan XML an hour on the monitored
    /// servers and shipped 29 GB of it, against 1.1 GB an hour of distinct plans: about 47 times the plans there
    /// are, and the reason the slowest <c>query_stats</c> runs are almost entirely time spent draining plan text.</para>
    ///
    /// <para>A file-only knob like <see cref="ProcedureStatsPlanCycleInterval"/>: an edit takes effect on the next
    /// restart, and it needs no schema rung. Only meaningful while <see cref="CapturePlans"/> is on.</para>
    /// </summary>
    [JsonPropertyName("queryStatsDeferredPlanFetch")]
    public bool QueryStatsDeferredPlanFetch { get; set; } = true;

    /// <summary>
    /// #5158: whether <c>procedure_stats</c> fetches plan XML only for module plans this host has not already
    /// committed. <c>on</c> (the default since 3.10) skips the render for a recognized plan. <c>off</c> keeps the inline capture. <c>shadow</c> keeps the inline capture and every
    /// stored row exactly as <c>off</c> writes it, and in addition recognizes each module plan by its identity (the
    /// plan handle, its cached time and a fingerprint of its statements) and checks that identity against the plans
    /// this host has committed, recording in the run's collection-log note how many it would have skipped and how
    /// many of those the identity got wrong (<c>deferred_would_hit</c>, <c>deferred_false_hit</c> when the plan's shape changed;
    /// a render whose shape held, in the field only the memory grant moved, counts as <c>deferred_same_shape</c>, not a false hit). <c>on</c> skips
    /// the render for a recognized plan, sends its stored digest instead, and renders only the rest, at most 150 a run.
    /// Set <c>off</c> to restore the inline capture.
    /// An unrecognized value logs a warning and means <c>off</c>.
    ///
    /// <para><b>Why a shadow mode.</b> A module plan changes in place when one statement recompiles, with the same
    /// plan handle and cached time, so an identity that missed that would send a stale plan's digest. Shadow measures
    /// the identity against the plans it just rendered before <c>on</c> trusts it. The cadence of
    /// <see cref="ProcedureStatsPlanCycleInterval"/> still decides which runs may render: an <c>on</c> run between
    /// captures sends digests for recognized plans and renders nothing.</para>
    ///
    /// <para>A file-only knob like <see cref="QueryStatsDeferredPlanFetch"/>: an edit takes effect on the next restart,
    /// and it needs no schema rung. Because it cannot change while the service runs, the plan cache starts empty under
    /// whichever mode the service started in, so nothing needs clearing on a change. Only meaningful while
    /// <see cref="CapturePlans"/> is on; Azure SQL Database stays <c>off</c> whatever this says.</para>
    /// </summary>
    [JsonPropertyName("procedureStatsDeferredPlanFetch")]
    public string ProcedureStatsDeferredPlanFetch { get; set; } = "on";

    /// <summary>
    /// The shared alert engine's enabled flags and thresholds (Phase-5 slice D). Every default
    /// mirrors Lite's <c>App.*</c> alert defaults exactly, so an empty section alerts like a
    /// fresh Lite install. Optional — omit it entirely for the defaults.
    /// </summary>
    [JsonPropertyName("alerts")]
    public AlertsConfig Alerts { get; set; } = new();

    /// <summary>
    /// SMTP delivery for fired alerts. Delivery is enabled when host + from are set (no separate
    /// flag — defaults over speculative config); <c>to</c> is the default recipient list, which a
    /// notification route can stand in for (#4751). The password uses the same DPAPI
    /// --encrypt-password pattern as SQL auth. Optional.
    /// </summary>
    [JsonPropertyName("smtp")]
    public SmtpConfig Smtp { get; set; } = new();

    /// <summary>
    /// Teams/Slack incoming-webhook delivery for fired alerts. A channel is enabled when its
    /// URL is set. Optional.
    /// </summary>
    [JsonPropertyName("webhooks")]
    public WebhooksConfig Webhooks { get; set; } = new();

    /// <summary>
    /// The sparse notification routes layered over <see cref="Smtp"/> and <see cref="Webhooks"/> (#3598,
    /// V131 <c>config.config_notification_routes</c>): a family or exact-metric match with a destination
    /// per channel, empty meaning "inherit". STORE-ONLY, like mute rules — <c>JsonIgnore</c> because a
    /// route is an operator's live tuning through the Viewer's Settings grid, not deployment plumbing, and a
    /// darling.json copy would be overwritten by the first store reload exactly as <c>Alerts</c> is
    /// (<c>ApplyToConfig</c> swaps it wholesale). Empty until the store reload delivers a list, which is
    /// the pre-routes fan-out exactly.
    /// </summary>
    [JsonIgnore]
    public IReadOnlyList<PerformanceMonitor.Notifications.NotificationRoute> NotificationRoutes { get; set; } =
        Array.Empty<PerformanceMonitor.Notifications.NotificationRoute>();

    /// <summary>
    /// The scheduled-analysis cadence + delivery knobs (Phase-5 AN3 / control-plane Stage 1). Every
    /// default mirrors Lite's <c>App.*</c> analysis defaults (enabled, 30-minute interval,
    /// notify-severity 1.5) EXCEPT <see cref="AnalysisConfig.NotificationsEnabled"/>, which defaults
    /// TRUE to preserve Darling's shipped behavior (the service gated analysis-finding delivery on the
    /// master <c>alerts.enabled</c> switch, default on) rather than Lite's App default of false. Seeded
    /// into <c>config_alert_settings</c>' analysis columns and read back live so the store is
    /// authoritative after the first run. Optional — omit the section entirely for the defaults.
    /// </summary>
    [JsonPropertyName("analysis")]
    public AnalysisConfig Analysis { get; set; } = new();

    /// <summary>
    /// The auto force-plan bot (#2138 phase 1). <c>enabled: false</c> (the default) means the bot
    /// evaluates nothing; enabled with <c>dryRun: true</c> (also the default) journals every
    /// would-force/blocked decision to <c>collect.plan_force_actions</c> in the MONITORING store.
    /// Optional — omit the section entirely and the bot stays off.
    ///
    /// <para>Phase 1 has no write path at all: no build artifact here can force or unforce a plan on
    /// a monitored server (see <see cref="PlanForceBot"/>), so these knobs govern what gets JUDGED
    /// and JOURNALED. They are the same knobs the write path (#2731) will gate on — including the
    /// third gate, the per-server <c>config_monitored_servers.plan_force_bot_enabled</c> opt-in that
    /// nothing in this file can set — so a shadow-mode ledger scored now is a faithful rehearsal of
    /// what an armed bot would have done.</para>
    /// </summary>
    [JsonPropertyName("forcePlanBot")]
    public ForcePlanBotFileConfig ForcePlanBot { get; set; } = new();

    /// <summary>
    /// The embedded MCP server (analysis slice AN4): the same analysis + data-read tool surface Lite
    /// and the Dashboard expose (the analysis class plus the plan-analysis and ~60 STORED data-read
    /// tools), over Streamable HTTP. Default OFF: a headless service should not open a port unless the
    /// operator asks for it (both apps default their MCP servers off too). Loopback-only by default, and it
    /// needs a token there too (<see cref="McpConfig.AllowTokenlessLoopback"/> is the opt-out); an opt-in <see cref="McpConfig.Network"/> block exposes it on the LAN behind a
    /// required bearer token + an in-app CIDR check (managed-mode only). Optional — omit the section entirely.
    /// </summary>
    [JsonPropertyName("mcp")]
    public McpConfig Mcp { get; set; } = new();

    /// <summary>
    /// The embedded web dashboard (#1562): a second, independent Kestrel host serving a
    /// browser-based view of the monitoring store over HTTP — a SEPARATE port + kill switch + exposure block
    /// from the MCP server (they gate different blast radii). Default OFF: a headless service should not open a
    /// port unless the operator asks. Loopback-only by default; an opt-in <see cref="WebConfig.Network"/> block
    /// exposes it on the LAN behind a required token (once) → an HMAC session cookie + an in-app CIDR check
    /// (managed-mode only), optionally over TLS (<see cref="WebTlsConfig"/>, #2562). While LAN-exposed EVERY
    /// request authenticates, loopback included — loopback is exempt from the CIDR test only (#1649).
    /// Optional — omit the section entirely.
    /// </summary>
    [JsonPropertyName("web")]
    public WebConfig Web { get; set; } = new();

    /// <summary>
    /// Declared PEER STORES (#2339): what this store covers, and which sibling Darling stores cover the
    /// rest of the fleet. Pure disclosure — see <see cref="PeersConfig"/>. Optional; omit it entirely on a
    /// single-store deployment and every surface behaves exactly as it did before.
    /// </summary>
    [JsonPropertyName("peers")]
    public PeersConfig Peers { get; set; } = new();

    /// <summary>
    /// The outbound heartbeat (#5450): a GET to a dead-man's-switch URL while the service is collecting, so a check
    /// outside the box alerts when the process stops or stops collecting. File-only like <see cref="Peers"/>, and its URL
    /// is a secret that no read surface returns. Off unless <c>heartbeat.url</c> is set. See <see cref="HeartbeatConfig"/>.
    /// </summary>
    [JsonPropertyName("heartbeat")]
    public HeartbeatConfig Heartbeat
    {
        get => _heartbeat;

        /* "heartbeat": null means off, the same as an absent block (#5460): the JSON options here do not honor nullable
           annotations, so a null would otherwise land in this property and fault the loop at shutdown. */
        set => _heartbeat = value ?? new();
    }

    private HeartbeatConfig _heartbeat = new();

    /// <summary>
    /// Read admission (#5506): how many long reads the MCP and web hosts run at once. File-only, like <see cref="Peers"/>;
    /// see <see cref="ReadsConfig"/>. Omit it for the default.
    /// </summary>
    [JsonPropertyName("reads")]
    public ReadsConfig Reads
    {
        get => _reads;

        /* "reads": null means the defaults, the same as an absent block (the #5460 heartbeat rule). */
        set => _reads = value ?? new();
    }

    private ReadsConfig _reads = new();

    /// <summary>
    /// The front door (#6840): one web address and one MCP endpoint that routes to several Darling stores, each of which keeps
    /// its own monitoring database. File-only, like <see cref="Peers"/>, and absent or <c>"enabled": false</c> is today's
    /// behavior. In front-door mode this process holds no store of its own and runs no collector. See <see cref="FrontDoorConfig"/>.
    /// </summary>
    [JsonPropertyName("frontDoor")]
    public FrontDoorConfig FrontDoor
    {
        get => _frontDoor;

        /* "frontDoor": null means off, the same as an absent block (the #5460 heartbeat rule). */
        set => _frontDoor = value ?? new();
    }

    private FrontDoorConfig _frontDoor = new();

    /// <summary>True when this process is a front door (#6840): <c>frontDoor.enabled</c>.</summary>
    [JsonIgnore]
    public bool IsFrontDoor => FrontDoor.Enabled;

    public static string ResolveConfigPath(string? explicitPath = null) =>
        ResolveConfigPath(explicitPath, Environment.GetEnvironmentVariable);

    /// <summary><see cref="ResolveConfigPath(string?)"/> reading the environment through <paramref name="environment"/>, so a
    /// test can supply <c>DARLING_CONFIG</c> without setting it for the whole process.</summary>
    internal static string ResolveConfigPath(string? explicitPath, Func<string, string?> environment)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath))
        {
            return explicitPath;
        }

        var fromEnvironment = environment("DARLING_CONFIG");
        if (!string.IsNullOrWhiteSpace(fromEnvironment))
        {
            return fromEnvironment;
        }

        return Path.Combine(AppContext.BaseDirectory, "darling.json");
    }

    /// <summary>Every env:/file: reference found in the deserialized config, exactly as written, captured
    /// in <see cref="Parse"/> before any resolution. Feeds <see cref="DarlingOwnedSecrets"/>.</summary>
    [JsonIgnore]
    internal List<string> SecretReferencesAsWritten { get; set; } = new();

    /// <summary>#5307: the paths (never the values) of keys under <c>web.network</c> and <c>mcp.network</c>, their
    /// <c>tls</c> blocks and <c>web.network.oidc</c> that no config class declares, e.g. <c>mcp.network.ssl</c>,
    /// found by <see cref="DarlingUnknownKeys"/> in <see cref="Parse"/>. Internal and <c>[JsonIgnore]</c>, so a
    /// serialized config (the diagnostics bundle) never carries it.</summary>
    [JsonIgnore]
    internal List<string> UnknownNetworkKeys { get; set; } = new();

    public static DarlingConfig Load(string? explicitPath = null)
    {
        var path = ResolveConfigPath(explicitPath);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"Configuration file not found: {path}. Copy darling.sample.json to darling.json and edit it.", path);
        }

        try
        {
            var loaded = Parse(File.ReadAllText(path));
            DarlingOwnedSecrets.Set(DarlingOwnedSecrets.Compute(loaded, path));
            return loaded;
        }
        catch (UnauthorizedAccessException ex)
        {
            /* The file is restricted on purpose (it holds every stored secret), so a refusal is the expected
               first experience of a new operator: say which group to join instead of a bare access error. */
            throw new UnauthorizedAccessException($"{DarlingOperatorGroup.AccessDeniedAdvice(path)} ({ex.Message})", ex);
        }

    }

    public static DarlingConfig Parse(string json)
    {
        var config = JsonSerializer.Deserialize<DarlingConfig>(json, s_jsonOptions);
        if (config is null)
        {
            throw new InvalidDataException("Configuration file parsed to null.");
        }

        /* #6468: a file with no "servers" key, or "servers": null, is the same as an empty list (a start with no servers yet). */
        config.Servers ??= new List<MonitoredServer>();

        /* Capture every env:/file: reference AS WRITTEN, before the resolution below overwrites any slot
           with the secret it points at. */
        config.SecretReferencesAsWritten = DarlingOwnedSecrets.CollectReferences(config);
        config.UnknownNetworkKeys = DarlingUnknownKeys.Find(json);

        /* #5240: a reference in a server's own secret slot is one the file declares for that server, so the entry read from
           the file may resolve it even when it is owned. The flags are set here and nowhere from a stored value. */
        foreach (var server in config.Servers ?? new List<MonitoredServer>())
        {
            if (server is null)
            {
                continue;
            }

            server.EncryptedPasswordDeclaredByFile = DarlingSecrets.DeclaresSecretText(server.EncryptedPassword);
            server.RemediationEncryptedPasswordDeclaredByFile = DarlingSecrets.DeclaresSecretText(server.RemediationEncryptedPassword);
            server.HypotheticalIndexEncryptedPasswordDeclaredByFile = DarlingSecrets.DeclaresSecretText(server.HypotheticalIndexEncryptedPassword);
        }

        if (config.Smtp is not null)
        {
            config.Smtp.EncryptedPasswordDeclaredByFile = DarlingSecrets.DeclaresSecretText(config.Smtp.EncryptedPassword);
        }

        /* #1804: postgres.connectionString also takes an env:/file: reference — for the WHOLE string,
           since the password lives inside it and per-field indirection can't reach it. Resolved ONCE
           here at the parse seam so every consumer (worker, MCP/web hosts, CLI verbs) sees the real
           string; the compose distribution's darling.json stays secret-free. A literal passes through
           untouched, so every existing config is byte-for-byte unaffected. */
        if (config.Postgres?.ConnectionString is { } connectionString && DarlingSecretSource.IsReference(connectionString))
        {
            config.Postgres.ConnectionString = DarlingSecretSource.Resolve(connectionString, "postgres.connectionString");
        }

        return config;
    }

    /// <summary>
    /// Validates the configuration; returns human-readable problems (empty = valid).
    /// Plaintext passwords are accepted (dev convenience) but reported as warnings by the caller.
    /// </summary>
    public IReadOnlyList<string> Validate()
    {
        var problems = new List<string>();

        if (IsFrontDoor)
        {
            /* #6840: a front door has no store of its own, so no postgres section is required of it (and none is read).
               The frontDoor block itself is checked below, with the peers block. */
        }
        else if (Postgres is null)
        {
            problems.Add("postgres section is required.");
        }
        else if (Postgres.Managed)
        {
            /* Managed mode DERIVES the connection string (localhost + port + the generated
               DPAPI-protected credential — see DarlingManagedPostgres); a hand-set string
               would silently win or silently lose depending on code order, so both together
               is a hard config error, not a precedence rule. */
            if (!string.IsNullOrWhiteSpace(Postgres.ConnectionString))
            {
                problems.Add("postgres.managed is true AND postgres.connectionString is set — pick one: " +
                    "managed mode derives the connection string itself (remove connectionString), or remove " +
                    "\"managed\" to use your own PostgreSQL via connectionString.");
            }

            if (Postgres.Port is < 1 or > 65535)
            {
                problems.Add($"postgres.port must be between 1 and 65535 (got {Postgres.Port}).");
            }

            /* #3914: same reasoning as connectionString above — managed mode connects the web dashboard and the
               MCP server as its own viewer and mcp roles, so a hand-set login would silently lose. */
            foreach (var (setting, value) in new[]
            {
                ("postgres.webConnectionString", Postgres.WebConnectionString),
                ("postgres.mcpConnectionString", Postgres.McpConnectionString),
            })
            {
                if (!string.IsNullOrWhiteSpace(value))
                {
                    problems.Add($"postgres.managed is true AND {setting} is set — managed mode connects the web " +
                        "dashboard and the MCP server as its own viewer and mcp roles; remove it, or remove \"managed\" " +
                        "to use your own PostgreSQL.");
                }
            }
        }
        else if (string.IsNullOrWhiteSpace(Postgres.ConnectionString))
        {
            problems.Add("postgres.connectionString is required (or set postgres.managed = true to run the bundled server).");
        }

        /* Peer-disclosure problems are checked BEFORE the servers early-return so a broken peers block is
           reported even on a config that has no servers yet. */
        /* The AWS role allowlist (#5452): an entry is a role ARN or a 12-digit account id. Fatal like the server
           entries: a typo here would silently leave a role off the list. */
        for (var i = 0; i < (AllowedAwsRoles?.Count ?? 0); i++)
        {
            var entryProblem = Targets.AwsRoleAllowlist.ValidateEntry(AllowedAwsRoles![i]);
            if (entryProblem is not null)
            {
                problems.Add($"allowedAwsRoles[{i}]: {entryProblem}");
            }
        }

        problems.AddRange(PeersConfig.Validate(Peers));
        problems.AddRange(FrontDoorConfig.Validate(FrontDoor));
        /* #5450: the heartbeat block, for the same reason (a broken one is reported even with no servers yet). */
        problems.AddRange(HeartbeatConfig.Validate(Heartbeat));

        /* #6468: no servers is a valid configuration. A first install starts with an empty list and the person adds the
           first server in the Viewer or the web dashboard, so an empty list is not a problem to report here. */
        Servers ??= new List<MonitoredServer>();

        for (int i = 0; i < Servers.Count; i++)
        {
            var server = Servers[i];
            var label = string.IsNullOrWhiteSpace(server.Name) ? $"servers[{i}]" : $"server '{server.Name}'";

            if (string.IsNullOrWhiteSpace(server.Host))
            {
                problems.Add($"{label}: host is required.");
            }

            if (server.UsesSqlAuth || server.UsesServicePrincipal)
            {
                /* Service principal carries the SAME two fields as SQL auth — the client id in username, the
                   client secret in encryptedPassword — so the requirement is identical; only the wording
                   changes so the error names what the operator actually has to supply. */
                if (string.IsNullOrWhiteSpace(server.Username))
                {
                    problems.Add(server.UsesServicePrincipal
                        ? $"{label}: service principal auth requires username (the Entra application/client id)."
                        : $"{label}: sql auth requires username.");
                }

                if (string.IsNullOrWhiteSpace(server.EncryptedPassword) && string.IsNullOrWhiteSpace(server.Password))
                {
                    problems.Add(server.UsesServicePrincipal
                        ? $"{label}: service principal auth requires encryptedPassword (the client secret; preferred, see --encrypt-password) or password."
                        : $"{label}: sql auth requires encryptedPassword (preferred; see --encrypt-password) or password.");
                }
            }
            else if (server.UsesManagedIdentity)
            {
                /* Managed identity has no mandatory field: a system-assigned identity needs nothing, a
                   user-assigned one puts its client id in username. No secret is ever stored. */
            }
            else if (!string.Equals(server.Auth, "integrated", StringComparison.OrdinalIgnoreCase))
            {
                problems.Add($"{label}: auth must be 'integrated', 'sql', 'serviceprincipal', or 'managedidentity' " +
                    $"(got '{server.Auth}'). The interactive Microsoft Entra modes are not supported for a headless collector.");
            }

            /* Caught here, in the pre-flight, rather than only where the connection string is built.
               MonitoredServerConnection throws on this too — it has to, since it is what actually
               knows the driver can't honour it — but that throw happens at first connect, which for a
               service means the misconfiguration surfaces in a log after deployment instead of in
               --test-connection before it. */
            if (server.IsPostgres && !server.UsesSqlAuth)
            {
                problems.Add(
                    $"{label}: a PostgreSQL target requires auth 'sql' with a username and password " +
                    "(integrated/Kerberos auth is not supported for PostgreSQL targets).");
            }

            if (server.Port is not 0 && server.Port is < 1 or > 65535)
            {
                problems.Add($"{label}: port must be between 1 and 65535 (got {server.Port}).");
            }

            /* The AWS role (#5452): each value's format, an external ID needs a role, and a role needs a
               PostgreSQL target. The texts come from the one validator every surface shares. */
            var awsRole = AwsRoleSettings.Normalize(server.AwsRoleArn);
            var awsProblem = AwsRoleSettings.ValidatePair(awsRole, AwsRoleSettings.Normalize(server.AwsExternalId));
            if (awsProblem is not null)
            {
                problems.Add($"{label}: {awsProblem}");
            }
            else if (awsRole is not null && !server.IsPostgres)
            {
                problems.Add($"{label}: {AwsRoleSettings.RoleNeedsPostgresMessage}");
            }
        }

        return problems;
    }
}

/// <summary>
/// The Postgres store — two modes. Unmanaged (default): <see cref="ConnectionString"/> points at
/// an existing PostgreSQL and the service never touches its lifecycle. Managed (the shipped
/// zero-admin default in darling.sample.json): <see cref="Managed"/> = true and the service
/// unpacks, initializes, starts, and stops its own bundled PostgreSQL + TimescaleDB via
/// <see cref="DarlingManagedPostgres"/>; the connection string is DERIVED
/// (localhost + <see cref="Port"/> + the generated DPAPI-protected credential), so setting
/// <see cref="ConnectionString"/> too is a validation error.
/// </summary>
public sealed class PostgresConfig
{
    [JsonPropertyName("connectionString")]
    public string ConnectionString { get; set; } = "";

    /// <summary>
    /// The web dashboard's store login on a store the service does not manage (#3914), normally the <c>viewer</c>
    /// role <c>tools/provision-roles.sql</c> creates. Same forms as <see cref="ConnectionString"/>: a literal, or an
    /// <c>env:NAME</c> / <c>file:/path</c> reference (#1804). Resolved when the web host starts, not at parse, so an
    /// unreadable reference keeps the dashboard down with the reason logged instead of stopping collection. Unset:
    /// the compose distribution's own store uses the <c>viewer</c> role the service provisions there, and any other
    /// store the owner <see cref="ConnectionString"/>, with a startup warning naming what that gives up. Managed mode
    /// derives its own; setting this there is a validation error.
    /// </summary>
    [JsonPropertyName("webConnectionString")]
    public string? WebConnectionString { get; set; }

    /// <summary>
    /// The MCP server's store login on a store the service does not manage (#3914), normally the <c>mcp</c> role
    /// <c>tools/provision-roles.sql</c> creates. Everything <see cref="WebConnectionString"/> says, for the MCP host.
    /// </summary>
    [JsonPropertyName("mcpConnectionString")]
    public string? McpConnectionString { get; set; }

    /// <summary>Run the bundled, service-managed PostgreSQL instead of pointing at an existing one.</summary>
    [JsonPropertyName("managed")]
    public bool Managed { get; set; }

    /// <summary>
    /// The managed server's loopback port. 5641 deliberately avoids PostgreSQL's default 5432 so
    /// the bundled instance can coexist with a PostgreSQL the machine already runs.
    /// </summary>
    [JsonPropertyName("port")]
    public int Port { get; set; } = 5641;

    /// <summary>
    /// The managed server's data directory; null (the default) means
    /// %ProgramData%\PerformanceMonitorDarling\pg — a machine-wide, service-account-writable
    /// convention created with inherited ACLs.
    /// </summary>
    [JsonPropertyName("dataDirectory")]
    public string? DataDirectory { get; set; }

    /// <summary>
    /// Which least-privilege role the interactive Viewer connects as in managed mode (V8 security
    /// hardening): <c>"admin"</c> (default — reads both schemas + writes the config tables the mute /
    /// alert-dismiss / analysis-mute surfaces need) or <c>"viewer"</c> (read-only, for a locked-down
    /// deployment where the Viewer's write actions degrade gracefully). Selects the role + credential
    /// file the Viewer's managed derivation uses; ignored by the service (which always connects as the
    /// owner) and in BYO mode (the operator's connectionString picks the role). Unknown values fall
    /// back to <c>admin</c>.
    /// </summary>
    [JsonPropertyName("connectAs")]
    public string ConnectAs { get; set; } = "admin";

    /// <summary>
    /// Opt-in network exposure for the managed store (darling-network-endpoints). Omit for the secure
    /// default = loopback-only (byte-for-byte today's behavior). Managed-mode only; ignored in BYO with
    /// a caller warning (the operator's own PostgreSQL governs BYO exposure). Any invalid/incomplete
    /// field degrades the store to loopback + LogCritical rather than failing validation — the exposure
    /// rules live at the point of use, NEVER in the all-fatal <see cref="DarlingConfig.Validate"/>
    /// (D-validate). See <see cref="PostgresNetworkConfig"/>.
    /// </summary>
    [JsonPropertyName("network")]
    public PostgresNetworkConfig? Network { get; set; }
}

/// <summary>
/// The alert engine's config section. Defaults mirror Lite's <c>App.xaml.cs</c> alert defaults
/// member-for-member: cpu 80% (Total mode), blocking 1, deadlock 1, poison 500 ms, long-running
/// query 30 min, tempdb 80%, low disk 10% / 5 GB, job multiplier 3x, failed-job lookback 60 min,
/// cooldown 5 min. The long-running-query read shape (max results + the five noise filters) is
/// control-plane-honored since V20 (was hardcoded); its defaults (5 / all on) still match Lite's.
/// </summary>
public sealed class AlertsConfig
{
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; } = true;

    /// <summary>
    /// Whether the service delivers the Server-Unreachable / Server-Restored connect-edge alerts (the headless
    /// twin of Lite's connection-change tray toasts, fired by <see cref="!:DarlingSelfAlertEvaluator"/>). Default
    /// true (matches Lite's App.NotifyConnectionChanges). Independent of the per-alert toggles — the connection
    /// edge is a service-health signal, gated together with the master <c>alerts.enabled</c> switch. Stored as
    /// <c>config_alert_settings.notify_connection_changes</c> (V20).
    /// </summary>
    [JsonPropertyName("notifyConnectionChanges")]
    public bool NotifyConnectionChanges { get; set; } = true;

    /// <summary>#1659 opt-in (V33): announce a server that is already down on its first-ever connect attempt
    /// (a service started mid-outage otherwise never alerts — there was no edge). Default false: the classic
    /// edge-only behavior.</summary>
    public bool NotifyConnectionDownAtStartup { get; set; }

    /// <summary>#1659 opt-in (V33): re-announce a standing outage every N minutes (0 = off). Re-fires deliver
    /// under the SAME "Server Unreachable" metric name so webhook-driven automation keyed on it re-triggers.</summary>
    public int ConnectionRefireMinutes { get; set; }

    /// <summary>#991 (V35): the master switch for the Availability Group alert family — failover, replica
    /// disconnect/reconnect, sync fell behind, database suspended. Default true, matching the sibling
    /// <see cref="NotifyConnectionChanges"/> gate: a fleet with no AGs collects no AG rows, so the alerts are
    /// silent anyway, and an operator who DOES run AGs should not have to discover a switch to be told about a
    /// failover. Turning it off also skips the per-server AG store read entirely.</summary>
    [JsonPropertyName("notifyAgHealth")]
    public bool NotifyAgHealth { get; set; } = true;

    /// <summary>#991 (V35): "AG Sync Fell Behind" fires when a secondary's <c>secondary_lag_seconds</c> reaches
    /// this (0 = off). Default 300 — five minutes of lag is well past a healthy synchronous or asynchronous
    /// replica on a working network, and short enough to matter before a failover decision.</summary>
    [JsonPropertyName("agLagAlertSeconds")]
    public int AgLagAlertSeconds { get; set; } = 300;

    /// <summary>#991 (V35): the second, independent "AG Sync Fell Behind" trigger — the secondary's
    /// <c>redo_queue_size</c> in KILOBYTES (0 = off, the default). Off by default because a healthy redo queue
    /// size is entirely workload-dependent: it is the knob to reach for once you know your own baseline, and a
    /// shipped guess would page half the fleet on day one.</summary>
    [JsonPropertyName("agRedoQueueAlertKb")]
    public long AgRedoQueueAlertKb { get; set; }

    /// <summary>#1696 (V37): re-announce a still-disconnected AG replica every N minutes (0 = off, the
    /// default). "AG Replica Disconnected" was a pure edge, so a replica down for a week announced it once.
    /// Re-fires deliver under the SAME metric name so webhook automation keyed on it re-triggers.</summary>
    [JsonPropertyName("agDisconnectRefireMinutes")]
    public int AgDisconnectRefireMinutes { get; set; }

    [JsonPropertyName("cpuEnabled")]
    public bool CpuEnabled { get; set; } = true;

    [JsonPropertyName("cpuThresholdPercent")]
    public int CpuThresholdPercent { get; set; } = 80;

    /// <summary>"total" (Lite's default: SQL + other-process CPU) or "sql" (SQL process only).</summary>
    [JsonPropertyName("cpuMode")]
    public string CpuMode { get; set; } = "total";

    [JsonPropertyName("blockingEnabled")]
    public bool BlockingEnabled { get; set; } = true;

    [JsonPropertyName("blockingCountThreshold")]
    public int BlockingCountThreshold { get; set; } = 1;

    /// <summary>
    /// #1839: fire when the latest blocking snapshot's TOTAL blocked wait reaches this many seconds.
    /// 0 = off, and off is the shipped default — a count threshold cannot tell one session blocked for
    /// an hour from one blocked for a second, but enabling it for everyone would change what existing
    /// deployments alert about.
    /// </summary>
    [JsonPropertyName("blockingWaitSecondsThreshold")]
    public int BlockingWaitSecondsThreshold { get; set; }

    [JsonPropertyName("deadlockEnabled")]
    public bool DeadlockEnabled { get; set; } = true;

    [JsonPropertyName("deadlockCountThreshold")]
    public int DeadlockCountThreshold { get; set; } = 1;

    [JsonPropertyName("poisonWaitEnabled")]
    public bool PoisonWaitEnabled { get; set; } = true;

    /// <summary>Read and reported but no longer consulted by the alert engine since #3539 A4 — see
    /// <see cref="IAlertEngineSettings.PoisonWaitThresholdMs"/>. Kept so an existing darling.json and the
    /// store's control-plane row keep round-tripping.</summary>
    [JsonPropertyName("poisonWaitThresholdMs")]
    public int PoisonWaitThresholdMs { get; set; } = 500;

    /// <summary>The Runnable Queue alert (#5641) switch: runnable tasks per visible online scheduler, averaged over
    /// <see cref="RunnableQueueEvaluator.WindowMinutes"/> minutes. ON by default, like Poison Wait. Persisted as the
    /// store column <c>runnable_queue_enabled</c> (V177).</summary>
    [JsonPropertyName("runnableQueueEnabled")]
    public bool RunnableQueueEnabled { get; set; } = true;

    /// <summary>Runnable Queue Warning bar (#5641), runnable tasks per scheduler, window average. Shipped 8 (the
    /// measurement is on <see cref="RunnableQueueEvaluator"/>). Read through <see cref="RunnableQueueEvaluator.ClampBars"/>.
    /// Store column <c>runnable_queue_warning_per_scheduler</c> (V177).</summary>
    [JsonPropertyName("runnableQueueWarningPerScheduler")]
    public int RunnableQueueWarningPerScheduler { get; set; } = RunnableQueueEvaluator.DefaultWarningPerScheduler;

    /// <summary>Runnable Queue Critical bar (#5641). Shipped 16. Never read below the Warning bar. Store column
    /// <c>runnable_queue_critical_per_scheduler</c> (V177).</summary>
    [JsonPropertyName("runnableQueueCriticalPerScheduler")]
    public int RunnableQueueCriticalPerScheduler { get; set; } = RunnableQueueEvaluator.DefaultCriticalPerScheduler;

    /// <summary>The Client Timeouts alert (#5640) switch: client attentions (a client cancelled or timed out a query) per
    /// minute, from the attention collector. ON by default since #5736 (V216): the bars are relative to each server's own normal rate, and a
    /// 7-day replay of them over a 42-server production store would have paged twice, both at Warning. Persisted as the store column
    /// <c>client_timeouts_enabled</c> (V179, default moved to TRUE by V216).</summary>
    [JsonPropertyName("clientTimeoutsEnabled")]
    public bool ClientTimeoutsEnabled { get; set; } = true;

    /// <summary>Client Timeouts Warning FLOOR (#5640, #5736), attentions per minute over the covered span. Shipped 10. The bar is the higher of this and
    /// the Warning multiple times the server's own baseline. Read through
    /// <see cref="AttentionEvaluator.ClampBars"/>. Store column <c>client_timeouts_warning_per_minute</c> (V179).</summary>
    [JsonPropertyName("clientTimeoutsWarningPerMinute")]
    public int ClientTimeoutsWarningPerMinute { get; set; } = AttentionEvaluator.DefaultWarningPerMinute;

    /// <summary>Client Timeouts Critical FLOOR (#5640, #5736). Shipped 60. Never read below the Warning floor. Store column
    /// <c>client_timeouts_critical_per_minute</c> (V179).</summary>
    [JsonPropertyName("clientTimeoutsCriticalPerMinute")]
    public int ClientTimeoutsCriticalPerMinute { get; set; } = AttentionEvaluator.DefaultCriticalPerMinute;

    /// <summary>Client Timeouts Warning multiple (#5736). The Warning bar is the higher of <see cref="ClientTimeoutsWarningPerMinute"/> (the floor) and
    /// this many times the server's own baseline, the p99.9 of its 3-minute average attention rate over the last 7 days. Shipped 3. Store column
    /// <c>client_timeouts_warning_multiple</c> (V211).</summary>
    [JsonPropertyName("clientTimeoutsWarningMultiple")]
    public int ClientTimeoutsWarningMultiple { get; set; } = AttentionBaselineMath.DefaultWarningMultiple;

    /// <summary>Client Timeouts Critical multiple (#5736). Shipped 10. Never read below the Warning multiple. Store column
    /// <c>client_timeouts_critical_multiple</c> (V211).</summary>
    [JsonPropertyName("clientTimeoutsCriticalMultiple")]
    public int ClientTimeoutsCriticalMultiple { get; set; } = AttentionBaselineMath.DefaultCriticalMultiple;

    /// <summary>The "Identity Capacity" alert (#6875): fires when an identity column is at or past the warning bar (or the critical bar) of
    /// its type's capacity. Ships OFF, like the other alerts that start firing on upgrade. Store column <c>identity_capacity_enabled</c> (V222).</summary>
    [JsonPropertyName("identityCapacityEnabled")]
    public bool IdentityCapacityEnabled { get; set; }

    /// <summary>Percent used at which an identity column warns (default 70). Store column <c>identity_capacity_warning_percent</c> (V222). A per-server or
    /// per-database threshold override (<c>identity_capacity.warning_percent</c>) replaces it.</summary>
    [JsonPropertyName("identityCapacityWarningPercent")]
    public int IdentityCapacityWarningPercent { get; set; } = 70;

    /// <summary>Percent used at which an identity column is critical (default 90). Store column <c>identity_capacity_critical_percent</c> (V222).</summary>
    [JsonPropertyName("identityCapacityCriticalPercent")]
    public int IdentityCapacityCriticalPercent { get; set; } = 90;

    [JsonPropertyName("longRunningQueryEnabled")]
    public bool LongRunningQueryEnabled { get; set; } = true;

    [JsonPropertyName("longRunningQueryThresholdMinutes")]
    public int LongRunningQueryThresholdMinutes { get; set; } = 30;

    [JsonPropertyName("tempDbSpaceEnabled")]
    public bool TempDbSpaceEnabled { get; set; } = true;

    [JsonPropertyName("tempDbSpaceThresholdPercent")]
    public int TempDbSpaceThresholdPercent { get; set; } = 80;

    [JsonPropertyName("lowDiskEnabled")]
    public bool LowDiskEnabled { get; set; } = true;

    /// <summary>Alert when a volume's free space &lt; X% (0 disables this dimension).</summary>
    [JsonPropertyName("lowDiskThresholdPercent")]
    public int LowDiskThresholdPercent { get; set; } = 10;

    /// <summary>Alert when a volume's free space &lt; X GB (0 disables this dimension).</summary>
    [JsonPropertyName("lowDiskThresholdGb")]
    public int LowDiskThresholdGb { get; set; } = 5;

    [JsonPropertyName("pvsEnabled")]
    public bool PvsEnabled { get; set; } = true;

    /// <summary>Alert when an ADR database's PVS reaches X% of its data files (0 disables the check) (#1984).</summary>
    [JsonPropertyName("pvsThresholdPercent")]
    public int PvsThresholdPercent { get; set; } = 40;

    /// <summary>A PVS breach additionally requires at least X GB of PVS — an AND qualifier so small
    /// databases at a high percent never page (0 removes the floor) (#1984).</summary>
    [JsonPropertyName("pvsFloorGb")]
    public int PvsFloorGb { get; set; } = 1;

    /* #2349: the database file-growth alert. Ships OFF -- a new alert that starts firing on upgrade is a bad
       citizen, and the right thresholds are a property of the fleet rather than of the product. */
    public bool FileGrowthEnabled { get; set; }
    /* #3539 A8c: MB per HOUR, averaged over FileGrowthLookbackMinutes. Same column (file_growth_rise_mb), same
       integer, one meaning on every lookback -- the engine scales it to the window at comparison time. */
    public int FileGrowthRiseMb { get; set; } = 10240;
    public int FileGrowthVolumePercent { get; set; } = 60;
    /* The window the rise RATE is averaged over (#3539 A8c) -- it does not rescale the threshold. */
    public int FileGrowthLookbackMinutes { get; set; } = 60;

    /// <summary>#2107: the store volume's self-alert warning percent (was a compile-time 10.0;
    /// 0 disables the check).</summary>
    [JsonPropertyName("selfDiskFreeWarnPercent")]
    public int SelfDiskFreeWarnPercent { get; set; } = 10;

    /// <summary>#3528: the store warning's GB floor — the percent above additionally requires free space
    /// below this many GB, an AND qualifier so a large volume at a low percent never pages (0 removes the
    /// floor). The PVS-floor composition, not the target pair's OR. 50 puts the crossover at a 500 GB
    /// store volume: below that the percent governs exactly as before; above it, 50 GB free is the line —
    /// which is what stops 400 GB free on a 4 TB volume reading as "act now".</summary>
    [JsonPropertyName("selfDiskFreeWarnGb")]
    public int SelfDiskFreeWarnGb { get; set; } = 50;

    /// <summary>#2107: how long collection may go quiet before Collection Stopped / Agent Not
    /// Running fire (was a compile-time 30 minutes). Defaults to the shared constant behind the
    /// display's Offline band, so the two definitions of "collection stopped" agree out of the
    /// box (#2794); widening it here widens only the ALERT window.</summary>
    [JsonPropertyName("collectionStaleMinutes")]
    public int CollectionStaleMinutes { get; set; } = ServerHealthThresholds.CollectionStoppedMinutesDefault;

    /// <summary>#2107: the Collection Stopped fast path — consecutive failures with zero successes
    /// that fire without waiting out the staleness window (was a compile-time 10).</summary>
    [JsonPropertyName("collectionFailureThreshold")]
    public int CollectionFailureThreshold { get; set; } = 10;

    /// <summary>#2107: the low-disk CRITICAL severity tier's percent floor (#1136 — grades the
    /// target-volume alert; was a compile-time 3.0).</summary>
    [JsonPropertyName("diskCriticalFreePercent")]
    public int DiskCriticalFreePercent { get; set; } = 3;

    /// <summary>#2107: the low-disk CRITICAL severity tier's GB floor (was a compile-time 2.0).</summary>
    [JsonPropertyName("diskCriticalFreeGb")]
    public int DiskCriticalFreeGb { get; set; } = 2;

    /// <summary>#2107: the analysis notification cooldown — the shared engine clamps [30, 10080]
    /// and Lite always passed a configured value through; Darling hardcoded 360.</summary>
    [JsonPropertyName("analysisNotifyCooldownMinutes")]
    public int AnalysisNotifyCooldownMinutes { get; set; } = 360;

    /// <summary>#2136: the Store Job Over Cadence warning threshold — a store background job whose
    /// last run reaches this percent of its own schedule interval fires the Warning tier. The
    /// Critical tier is fixed at 100 (a job outrunning its cadence compounds refresh lag).
    ///
    /// <para>#3060: the default is <see cref="TimescaleSupport.RefreshSlotPercentOfHourlyCadence"/>, named
    /// rather than restated, so this seed and that constant cannot disagree. The figure is 25, and #3174
    /// broke the derivation that used to produce it while leaving the value where it is — see that constant
    /// for why a re-derived grid has no single slot for a percent of cadence to mean, and why V57's
    /// already-applied column default is what the figure is anchored on instead. DarlingSelfAlertTests pins
    /// this seed equal to the rung text and holds the knob firing at or before the heaviest refresh's
    /// window.</para></summary>
    [JsonPropertyName("storeJobCadenceWarnPercent")]
    public int StoreJobCadenceWarnPercent { get; set; } = TimescaleSupport.RefreshSlotPercentOfHourlyCadence;

    /// <summary>#3297 (V119): the Retention Held WARNING tier — how many times its own configured horizon a
    /// retention policy held by the rollup-coverage gate must be holding before the hold is reported. Was a
    /// compile-time constant, which made this the one alert an operator could not tune (#3296).
    ///
    /// <para>The seed is <see cref="TimescaleSupport.RetentionHoldWarnRatioDefault"/>, named rather than
    /// restated, so this and the V119 column default cannot disagree — the #3060 discipline the cadence knob
    /// above already follows. <c>DarlingAlertSettings</c> clamps it on read.</para></summary>
    [JsonPropertyName("retentionHoldWarnRatio")]
    public double RetentionHoldWarnRatio { get; set; } = TimescaleSupport.RetentionHoldWarnRatioDefault;

    /// <summary>#3297 (V119): the Retention Held CRITICAL tier, seeded from
    /// <see cref="TimescaleSupport.RetentionHoldCriticalRatioDefault"/> for the reason its warning sibling
    /// above is. A value below the warning tier is not invalid — it means every fire is Critical, which is
    /// what setting it there asks for.</summary>
    [JsonPropertyName("retentionHoldCriticalRatio")]
    public double RetentionHoldCriticalRatio { get; set; } = TimescaleSupport.RetentionHoldCriticalRatioDefault;

    /// <summary>#3444 (V122): how many distinct deadlocks inside the rolling window fire the PostgreSQL
    /// Deadlocks alert. Was a compile-time constant in <c>DarlingWorker</c> with no settings path at all,
    /// while SQL Server's <see cref="DeadlockCountThreshold"/> has been settable since V1.
    ///
    /// <para>The seed is <see cref="PostgresAlertEvaluator.DeadlockCountThresholdDefault"/>, named rather
    /// than restated, so this and the V122 column default cannot disagree — the #3060 discipline the
    /// retention and deadlock-band knobs above already follow. <c>DarlingAlertSettings</c> clamps it on
    /// read.</para>
    ///
    /// <para><b>Separate from <see cref="DeadlockCountThreshold"/> deliberately</b> — see the default
    /// constant: the two engines count with different instruments (captured graphs vs deadlocks parsed
    /// from the server log), and an operator tuning one should not silently move the other. The V122 rung's
    /// second reason — that a PostgreSQL server had no deadlock band to agree with — ended with #3539, which
    /// bands the PostgreSQL card's own counter difference through the shared tiers. The <c>enabled</c>
    /// switch IS shared: <see cref="DeadlockEnabled"/> governs both engines.</para></summary>
    [JsonPropertyName("pgDeadlockCountThreshold")]
    public int PgDeadlockCountThreshold { get; set; } = PostgresAlertEvaluator.DeadlockCountThresholdDefault;

    /// <summary>#3444 (V122): how many distinct ROOT BLOCKERS inside the rolling window fire the PostgreSQL
    /// Blocking alert, seeded from <see cref="PostgresAlertEvaluator.BlockingCountThresholdDefault"/> for
    /// the reason its deadlock sibling above is.
    ///
    /// <para>Separate from <see cref="BlockingCountThreshold"/> because the two count different things: that
    /// one counts engine-recorded blocked-process reports, this one counts roots found in a periodic SAMPLE
    /// of <c>pg_stat_activity</c>. <see cref="BlockingEnabled"/> is shared.</para></summary>
    [JsonPropertyName("pgBlockingCountThreshold")]
    public int PgBlockingCountThreshold { get; set; } = PostgresAlertEvaluator.BlockingCountThresholdDefault;

    /// <summary>#3466 (V124): whether the scheduled fleet sweep runs at all. NOT the alert master
    /// switch's business, deliberately: sweeps under <c>alerts.enabled: false</c> are the muted-mode
    /// contract's whole point (the sweep keeps publishing and carries the would-have-paged ledger),
    /// so this is the sweep's OWN switch, the way <c>analysis.enabled</c> is the analysis pipeline's.
    /// Default true — see the V124 rung for why the feature does not ship dark.</summary>
    [JsonPropertyName("fleetSweepEnabled")]
    public bool FleetSweepEnabled { get; set; } = true;

    /// <summary>#3466 (V124): how often the fleet sweep runs, in minutes. Seeded from
    /// <see cref="FleetSweepCadence.DefaultIntervalMinutes"/>, named rather than restated, so this and
    /// the V124 column default cannot disagree — the #3060 discipline the knobs above follow. The
    /// worker clamps it on read to <see cref="FleetSweepCadence.IntervalMinutesFloor"/>–
    /// <see cref="FleetSweepCadence.IntervalMinutesCeiling"/>, the same bounds
    /// <c>update_alert_settings</c> and the Viewer's save gate enforce on the way in.</summary>
    [JsonPropertyName("fleetSweepIntervalMinutes")]
    public int FleetSweepIntervalMinutes { get; set; } = FleetSweepCadence.DefaultIntervalMinutes;

    /// <summary>#3368 (V120): the deadlock health band's WARNING tier, in deadlocks per hour normalised over
    /// the window a card counted them in. The band was <c>count &gt; 0</c>, so one resolved deadlock made a
    /// server Critical; the right rate differs per workload, which is #3297's argument for the control plane.
    ///
    /// <para>The seed is <see cref="ServerHealthThresholds.DeadlockWarnPerHourDefault"/>, named rather than
    /// restated, so this and the V120 column default cannot disagree — the #3060 discipline the retention
    /// knobs above already follow. <c>DeadlockRateThresholds</c> clamps it on read.</para></summary>
    [JsonPropertyName("deadlockWarnPerHour")]
    public double DeadlockWarnPerHour { get; set; } = ServerHealthThresholds.DeadlockWarnPerHourDefault;

    /// <summary>#3368 (V120): the deadlock health band's CRITICAL tier, seeded from
    /// <see cref="ServerHealthThresholds.DeadlockCriticalPerHourDefault"/> for the reason its warning sibling
    /// above is. A value below the warning tier is not invalid — it means every banded rate is Critical,
    /// which is what setting it there asks for.</summary>
    [JsonPropertyName("deadlockCriticalPerHour")]
    public double DeadlockCriticalPerHour { get; set; } = ServerHealthThresholds.DeadlockCriticalPerHourDefault;

    [JsonPropertyName("longRunningJobEnabled")]
    public bool LongRunningJobEnabled { get; set; } = true;

    [JsonPropertyName("longRunningJobMultiplier")]
    public int LongRunningJobMultiplier { get; set; } = 3;

    [JsonPropertyName("failedJobEnabled")]
    public bool FailedJobEnabled { get; set; } = true;

    [JsonPropertyName("failedJobLookbackMinutes")]
    public int FailedJobLookbackMinutes { get; set; } = 60;

    /// <summary>Master switch for the database-state alert (fire when a database's current state
    /// deviates from its expected baseline/override state). Per-database expected states live in the
    /// config.database_state_expected table, not here.</summary>
    [JsonPropertyName("databaseStateEnabled")]
    public bool DatabaseStateEnabled { get; set; } = true;

    /// <summary>Minimum minutes between repeated notifications for the same alert condition.</summary>
    [JsonPropertyName("cooldownMinutes")]
    public int CooldownMinutes { get; set; } = 5;

    /// <summary>Databases excluded from blocking/deadlock/long-running-query alert evaluation.</summary>
    [JsonPropertyName("excludedDatabases")]
    public List<string> ExcludedDatabases { get; set; } = new();

    /// <summary>Mount points the Volume Free Space alert ignores on every server (#6098), stored in
    /// <c>config_alert_settings.excluded_volumes</c> (V200) beside <c>excluded_databases</c>, and the store row wins
    /// over this file after the first seed. Matched by <see cref="PerformanceMonitor.Alerting.VolumeExclusions"/>:
    /// the whole mount point, case-insensitive, a trailing <c>\</c> or <c>/</c> on either side ignored.</summary>
    [JsonPropertyName("excludedVolumes")]
    public List<string> ExcludedVolumes { get; set; } = new();

    /// <summary>
    /// How deadlock/blocking alerts are delivered (#1141): <see cref="AlertNotificationMode.Summary"/>
    /// (one batched card per cycle — the default, matching Lite) or <see cref="AlertNotificationMode.PerEvent"/>
    /// (one notification per distinct incident, capped at <see cref="PerEventMax"/> with a trailing "+N more").
    /// A per-server override (<see cref="MonitoredServer.AlertDeliveryModeOverride"/>) wins over this global
    /// default via the shared <c>AlertDeliveryModeResolver</c>. Serialized as its name ("Summary"/"PerEvent")
    /// in darling.json and stored as <c>config_alert_settings.delivery_mode</c>.
    /// </summary>
    [JsonPropertyName("deliveryMode")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public AlertNotificationMode DeliveryMode { get; set; } = AlertNotificationMode.Summary;

    /// <summary>Per-event mode's cap on incidents delivered individually per cycle before the trailing
    /// "+N more" batch (#1141); clamped 1–100 when applied. Stored as <c>config_alert_settings.per_event_max</c>.</summary>
    [JsonPropertyName("perEventMax")]
    public int PerEventMax { get; set; } = 5;

    /* The long-running-query read shape (V20 control-plane knobs) — the row cap + the five noise-filter
       opt-outs the shared AlertEngine forwards to GetLongRunningQueriesAsync. Defaults match Lite's App.*
       (5 rows, every filter on) so an un-customized store suppresses the same noise it always did; stored as
       config_alert_settings.long_running_query_*. */

    /// <summary>Row cap for the long-running-query read (clamped 1–1000 when applied; default 5).</summary>
    [JsonPropertyName("longRunningQueryMaxResults")]
    public int LongRunningQueryMaxResults { get; set; } = 5;

    /// <summary>Exclude sessions waiting on SP_SERVER_DIAGNOSTICS (default true).</summary>
    [JsonPropertyName("longRunningQueryExcludeSpServerDiagnostics")]
    public bool LongRunningQueryExcludeSpServerDiagnostics { get; set; } = true;

    /// <summary>Exclude WAITFOR / BROKER_RECEIVE_WAITFOR sessions (default true).</summary>
    [JsonPropertyName("longRunningQueryExcludeWaitFor")]
    public bool LongRunningQueryExcludeWaitFor { get; set; } = true;

    /// <summary>Exclude BACKUPTHREAD / BACKUPIO sessions (default true).</summary>
    [JsonPropertyName("longRunningQueryExcludeBackups")]
    public bool LongRunningQueryExcludeBackups { get; set; } = true;

    /// <summary>Exclude XE_LIVE_TARGET_TVF sessions (default true).</summary>
    [JsonPropertyName("longRunningQueryExcludeMiscWaits")]
    public bool LongRunningQueryExcludeMiscWaits { get; set; } = true;

    /// <summary>Exclude CDC capture sessions (default true).</summary>
    [JsonPropertyName("longRunningQueryExcludeCdc")]
    public bool LongRunningQueryExcludeCdc { get; set; } = true;

    /// <summary>
    /// The Long-Running Query OPT-OUT knob's <c>program_name</c> arm (#3653 A5, ruling Q5): sessions whose
    /// program name STARTS WITH an entry are not evaluated by the alert at all — not read into the decision,
    /// not counted, not fingerprinted; the opposite of a mute rule. Case-insensitive prefix, no wildcard grammar
    /// — <c>LongRunningQueryExclusions</c> in the Alerting library is the one rule both SKUs apply, IN the read
    /// and ahead of its row cap (a post-read drop would let permanent background requests fill the cap; the type
    /// says why). Stored as <c>config_alert_settings.long_running_query_excluded_program_name_prefixes</c>
    /// (<c>text[]</c>, V135), the same shape as <c>excludedDatabases</c>, and the store row wins over this file
    /// like every other alert knob.
    ///
    /// <para><b>Seeded default</b>: <c>SQLAgent - TSQL JobStep</c> — class 1 of the 7-day read of one large
    /// production store, whose long-running population was (1) SQL Agent job steps, ~460 sessions a week across
    /// 11 jobs, medians 35–62 min (the program name embeds the job id, hence a PREFIX); (2) the
    /// <c>NT AUTHORITY\SYSTEM</c> / <c>NT AUTHORITY\NETWORK SERVICE</c> logins, the multi-day background — see
    /// <see cref="LongRunningQueryExcludedLogins"/>; (3) the application's admin login — NOT excluded, because it
    /// carries the job wave AND real ad-hoc long-runners, and this prefix already removes its share; (4) named
    /// humans — never excluded. The seed is a DEFAULT: this initialiser and the rung's column default agree, so a
    /// fresh file and a pre-rung row read the same; an operator clearing the list stores an empty array, which
    /// excludes nothing on this arm.</para>
    /// </summary>
    [JsonPropertyName("longRunningQueryExcludedProgramNamePrefixes")]
    public List<string> LongRunningQueryExcludedProgramNamePrefixes { get; set; } = LongRunningQueryExclusions.DefaultProgramNamePrefixes.ToList();

    /// <summary>The knob's <c>login_name</c> arm (#3653 A5, Q5) — EXACT, case-insensitive (a prefix would let
    /// <c>svc</c> swallow <c>svc_owner</c>), for the service principals whose program name is a generic driver
    /// string; a session is excluded when EITHER arm matches and counted once, under the prefix. Stored as
    /// <c>config_alert_settings.long_running_query_excluded_logins</c> (V135). <b>Seeded default</b>:
    /// <c>NT AUTHORITY\SYSTEM</c> and <c>NT AUTHORITY\NETWORK SERVICE</c> — class 2 of the production read, ~70
    /// sessions across 42 servers with medians of 4.8–8.6 DAYS, CDC-capture shaped. The application's admin login
    /// is deliberately absent (see <see cref="LongRunningQueryExcludedProgramNamePrefixes"/>).</summary>
    [JsonPropertyName("longRunningQueryExcludedLogins")]
    public List<string> LongRunningQueryExcludedLogins { get; set; } = LongRunningQueryExclusions.DefaultLogins.ToList();
}

/// <summary>
/// The scheduled-analysis cadence + delivery knobs (control-plane Stage 1). These live in
/// <c>config_alert_settings</c>' analysis columns in the store; before this section existed the
/// service hardcoded the interval (30 min) and gated finding delivery on <c>alerts.enabled</c>.
/// Defaults mirror Lite's <c>App.*</c> analysis defaults (enabled, 30-minute interval clamped to
/// 5–360, notify-severity 1.5 clamped to 0–2) except <see cref="NotificationsEnabled"/>, which is
/// TRUE to keep Darling's shipped notify-on default (Lite's App default is false).
/// </summary>
public sealed class AnalysisConfig
{
    /// <summary>Whether the scheduled analysis pipeline runs (findings still require notify gating).</summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; } = true;

    /// <summary>How often scheduled analysis runs, per server (clamped 5–360 when applied).</summary>
    [JsonPropertyName("intervalMinutes")]
    public int IntervalMinutes { get; set; } = 30;

    /// <summary>Whether findings are delivered to the notification channels (analysis still runs + persists).</summary>
    [JsonPropertyName("notificationsEnabled")]
    public bool NotificationsEnabled { get; set; } = true;

    /// <summary>Minimum finding severity (0.0–2.0) to notify on — the shared AnalysisNotificationService floor.</summary>
    [JsonPropertyName("notifySeverity")]
    public double NotifySeverity { get; set; } = 1.5;

    /// <summary>
    /// Where a notify-worthy but UNCORROBORATED finding goes (#3712) — <c>digest</c> (default) or <c>page</c>;
    /// see <c>FindingRouting</c>. A string rather than the enum so a hand-edited value that is neither parses
    /// to "no opinion" (<c>DarlingAlertSettings</c> falls through it) instead of failing the whole config load.
    ///
    /// <para><b>The FILE half of a two-source knob.</b> This member is darling.json's value and nothing else;
    /// the store's half is <see cref="StoreUncorroboratedRoute"/>, the V137
    /// <c>config_alert_settings.analysis_uncorroborated_route</c> column, and the precedence
    /// <c>DarlingAlertSettings.UncorroboratedFindingRoute</c> applies is <b>store non-NULL wins over file</b>:
    /// a route set in the Viewer's Settings window or through <c>update_alert_settings</c> governs from the next
    /// reload beacon, and only a NULL column defers to this value, and only an unparseable value here defers
    /// to the shipped <c>digest</c>. The knob shipped FILE-LEVEL in #3732 because two rungs were in flight; the
    /// column landed in V137 and the code half in the same lane as this paragraph.</para>
    ///
    /// <para><b>Why it stays a distinct member rather than being overwritten by the column on load.</b> It
    /// lives inside this section for the JSON shape an operator expects (<c>analysis.uncorroboratedRoute</c>
    /// beside <c>analysis.notifySeverity</c>), and <c>StoreConfigProvider.ApplyToConfig</c> swaps
    /// <c>config.Analysis</c> WHOLESALE on every reload, so <c>StoreConfigProvider.LoadViewAsync</c> CARRIES
    /// this value across the swap (the <c>BuildServerFromRow</c> backfill-from-bootstrap shape) and reads the
    /// column into the sibling. Folding the column INTO this member would lose the file's value the moment the
    /// store held one — and then an operator clearing the store column back to NULL ("let the file govern
    /// again") would find nothing left to govern. Two members, one resolver, and the MCP read can say which
    /// one decided.</para>
    /// </summary>
    [JsonPropertyName("uncorroboratedRoute")]
    public string UncorroboratedRoute { get; set; } = PerformanceMonitor.Notifications.FindingRouting.DigestText;

    /// <summary>
    /// The STORE half of the #3712 route knob: <c>config.config_alert_settings.analysis_uncorroborated_route</c>
    /// (V137, nullable <c>text</c> under a CHECK admitting only <c>digest</c> / <c>page</c>), read by
    /// <c>StoreConfigProvider.ReadAlertSettingsAsync</c> like every appended knob and swapped in with the
    /// section on every reload beacon. NULL is a VALUE here — "not set in the store; the file-level
    /// <see cref="UncorroboratedRoute"/> governs" — which is what every store reads the morning after the
    /// V137 upgrade and what a fresh seed leaves (the seed deliberately does not copy the file's value in,
    /// or the tri-state's third state would be unreachable on every install). Non-NULL wins over the file.
    ///
    /// <para><see cref="JsonIgnoreAttribute">JsonIgnore</see> on purpose: darling.json has no key for it and
    /// must not grow one by accident — a store value that could also be typed into the file is a value with
    /// two authors and no tiebreak, the #3314 by-halves shape this member exists to avoid. It is populated by
    /// exactly one writer, the store read, and a value that is neither spelling (impossible under the CHECK;
    /// possible on a store whose CHECK was dropped by hand) is treated by the resolver as NULL and logged
    /// once per reload by <c>LoadViewAsync</c>, never silently turned into a page or a digest.</para>
    /// </summary>
    [JsonIgnore]
    public string? StoreUncorroboratedRoute { get; set; }
}

/// <summary>
/// The darling.json face of <see cref="PerformanceMonitor.Analysis.ForcePlanBotSettings"/> (#2138).
/// File-level and NOT control-plane-backed in phase 1 (no <c>config_service</c> columns yet — a
/// follow-up adds the store override + viewer surface), so these values are authoritative from the
/// file at startup and a store reload does not change them. Kept as a separate JSON-shaped class
/// rather than reusing the settings record so the wire names stay stable if the policy record grows.
/// Every default is the safe state — see the settings record for what each knob means and why
/// dry-run mirrors live mode exactly.
///
/// <para>The self-review knobs (<c>firstReviewMinutes</c>, <c>finalReviewMinutes</c>,
/// <c>minReviewExecutions</c>, <c>netBenefitRatio</c>) are carried in full even though phase 1 places
/// no force to review: they are the inputs to
/// <see cref="PerformanceMonitor.Analysis.ForcePlanSelfReview"/>, whose whole verdict table is
/// specced here so the rules a live force would be judged by are settled and reviewable BEFORE the
/// write path exists to apply them.</para>
/// </summary>
public sealed class ForcePlanBotFileConfig
{
    /// <summary>Global gate 1 — false (default) means the bot evaluates nothing at all.</summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>Global gate 2 — true (default) journals decisions without touching any server.</summary>
    [JsonPropertyName("dryRun")]
    public bool DryRun { get; set; } = true;

    /// <summary>The bot's own action floor on regression_factor (detection fires at 2; this can be raised independently).</summary>
    [JsonPropertyName("minRegressionFactor")]
    public double MinRegressionFactor { get; set; } = 2.0;

    /// <summary>One journaled decision per (server, database, query) per this window.</summary>
    [JsonPropertyName("queryCooldownHours")]
    public int QueryCooldownHours { get; set; } = 24;

    /// <summary>Rolling 24h cap on actionable decisions per server.</summary>
    [JsonPropertyName("maxActionsPerServerPerDay")]
    public int MaxActionsPerServerPerDay { get; set; } = 3;

    /// <summary>Failed forces within the cooldown window that block a query from further forcing.</summary>
    [JsonPropertyName("failedForceThreshold")]
    public int FailedForceThreshold { get; set; } = 2;

    /// <summary>The failure-memory window in hours (a sliding window, not a permanent flag).</summary>
    [JsonPropertyName("failedForceCooldownHours")]
    public int FailedForceCooldownHours { get; set; } = 168;

    /// <summary>First self-review checkpoint after a live force, in minutes. Read by the review
    /// state machine, which the write path (#2731) drives — nothing in phase 1 places a force.</summary>
    [JsonPropertyName("firstReviewMinutes")]
    public int FirstReviewMinutes { get; set; } = 60;

    /// <summary>Final (terminal) self-review checkpoint, in minutes.</summary>
    [JsonPropertyName("finalReviewMinutes")]
    public int FinalReviewMinutes { get; set; } = 1440;

    /// <summary>Executions required before a checkpoint judges cost, and the executions limb of the
    /// operator flow's post-eviction observation window.</summary>
    [JsonPropertyName("minReviewExecutions")]
    public int MinReviewExecutions { get; set; } = 25;

    /// <summary>The elapsed limb of the post-eviction observation window, in minutes — the operator flow
    /// observes until whichever of the two limbs fires first.</summary>
    [JsonPropertyName("observationWindowMinutes")]
    public int ObservationWindowMinutes { get; set; } = 30;

    /// <summary>Post-force cpu/exec must be at or below this fraction of the baseline, or the review unforces.</summary>
    [JsonPropertyName("netBenefitRatio")]
    public double NetBenefitRatio { get; set; } = 0.75;

    /// <summary>The clamped policy settings this file section resolves to.</summary>
    public PerformanceMonitor.Analysis.ForcePlanBotSettings ToSettings() =>
        new PerformanceMonitor.Analysis.ForcePlanBotSettings
        {
            Enabled = Enabled,
            DryRun = DryRun,
            MinRegressionFactor = MinRegressionFactor,
            QueryCooldownHours = QueryCooldownHours,
            MaxActionsPerServerPerDay = MaxActionsPerServerPerDay,
            FailedForceThreshold = FailedForceThreshold,
            FailedForceCooldownHours = FailedForceCooldownHours,
            FirstReviewMinutes = FirstReviewMinutes,
            FinalReviewMinutes = FinalReviewMinutes,
            MinReviewExecutions = MinReviewExecutions,
            ObservationWindowMinutes = ObservationWindowMinutes,
            NetBenefitRatio = NetBenefitRatio,
        }.Normalize();
}

/// <summary>
/// SMTP alert delivery. Port/SSL/cooldown defaults mirror Lite's (587 / SSL on / 15 minutes).
/// </summary>
public sealed class SmtpConfig
{
    [JsonPropertyName("host")]
    public string Host { get; set; } = "";

    [JsonPropertyName("port")]
    public int Port { get; set; } = 587;

    [JsonPropertyName("useSsl")]
    public bool UseSsl { get; set; } = true;

    [JsonPropertyName("username")]
    public string? Username { get; set; }

    /// <summary>DPAPI-LocalMachine-protected SMTP password, base64 — produced by --encrypt-password.</summary>
    [JsonPropertyName("encryptedPassword")]
    public string? EncryptedPassword { get; set; }

    /// <summary>Whether darling.json itself declares <see cref="EncryptedPassword"/> as it stands (the
    /// <see cref="MonitoredServer.EncryptedPasswordDeclaredByFile"/> rule, for the SMTP slot, #5366).</summary>
    [JsonIgnore]
    internal bool EncryptedPasswordDeclaredByFile { get; set; }

    /// <summary>The pin taken at upgrade for the SMTP slot, when the store holds one (#5366).</summary>
    [JsonIgnore]
    internal LegacyPin? SecretPin { get; set; }

    /// <summary>The binding a sealed SMTP password must have been sealed for.</summary>
    [JsonIgnore]
    internal PasswordBinding SecretBinding => PasswordBinding.ForSmtp(Host, Port, UseSsl, Username);

    /// <summary>
    /// The SMTP password as a literal or an <c>env:</c>/<c>file:</c> reference (#1804 —
    /// <see cref="DarlingSecretSource"/>). Before this, SMTP had ONLY the DPAPI field, so non-Windows
    /// hosts had no email-alerting path at all. <see cref="EncryptedPassword"/> stays preferred where
    /// both are set (Windows); a reference is the supported non-Windows shape and does not count as
    /// plaintext-in-config.
    /// </summary>
    [JsonPropertyName("password")]
    public string? Password { get; set; }

    [JsonPropertyName("from")]
    public string From { get; set; } = "";

    /// <summary>Comma-separated recipient list.</summary>
    [JsonPropertyName("to")]
    public string To { get; set; } = "";

    /// <summary>Email/webhook channel cooldown between repeated alerts (Lite's default 15).</summary>
    [JsonPropertyName("emailCooldownMinutes")]
    public int EmailCooldownMinutes { get; set; } = 15;

    /// <summary>
    /// Opt-in (V187, #5825, <c>config_notification.email_send_resolutions</c>): when an alert clears, email the
    /// resolution to the recipients its fire reached. Off by default. A notification route can override it for its
    /// own destinations (<see cref="NotificationRouteResolutions"/>).
    /// </summary>
    [JsonPropertyName("emailSendResolutions")]
    public bool SendResolutions { get; set; } = false;
}

/// <summary>
/// The embedded MCP server's config. Port 5152 keeps the product's local MCP family
/// non-colliding on one machine (Dashboard 5150, Lite 5151, Darling 5152). Loopback-only by default,
/// with a bearer token required there too unless <see cref="AllowTokenlessLoopback"/> is on; the optional <see cref="Network"/> block opts into LAN exposure behind a
/// required bearer token + an in-app CIDR check (darling-network-endpoints, managed-mode only).
/// </summary>
public sealed class McpConfig
{
    /// <summary>Default OFF — the headless twin of both apps' mcp_enabled=false default.
    /// <para>A first-run SEED only (#2389): once the store is seeded, <c>config.config_service.mcp_enabled</c>
    /// is authoritative and this value is never read again except before the worker's first publish. Editing it
    /// on a seeded box changes nothing — use <c>--enable-mcp</c>/<c>--disable-mcp</c> or the Viewer's Settings.
    /// <see cref="Network"/>, sitting right beside it, is the OPPOSITE: file-only with no store equivalent.
    /// The supervisor reports the disagreement when the two planes differ.</para></summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>A first-run SEED only, like <see cref="Enabled"/> — <c>config.config_service.mcp_port</c> wins
    /// on a seeded store.</summary>
    [JsonPropertyName("port")]
    public int Port { get; set; } = 5152;

    /// <summary>
    /// Default OFF. Offers the MCP tools that change monitoring configuration (alert settings and routes, mute
    /// rules, custom alert rules, Custom Views, the analysis mute). File-only and restart-only, like
    /// <see cref="Network"/>: no store equivalent. Writes are only offered when a bearer token also resolves
    /// (<see cref="McpNetworkConfig.EncryptedToken"/> or <see cref="McpNetworkConfig.Token"/>), and every
    /// request must then present it, loopback included. See <c>DarlingMcpWritePolicy</c>.
    /// </summary>
    [JsonPropertyName("allowWrites")]
    public bool AllowWrites { get; set; }

    /// <summary>
    /// Default OFF. Additionally offers <c>add_servers</c>, <c>edit_server</c>, <c>remove_server</c> and
    /// <c>set_server_action</c>. The first three change the servers this service connects to as its own
    /// account; <c>set_server_action</c> turns one server's switch for an agent action (<c>force_plan</c>,
    /// <c>evict_plan</c>, <c>kill_session</c>) on or off. Has no effect unless <see cref="AllowWrites"/> is on.
    /// </summary>
    [JsonPropertyName("allowServerAdmin")]
    public bool AllowServerAdmin { get; set; }

    /// <summary>
    /// Default OFF (#5632 Phase A, ruling R2). Offers <c>claim_investigation_request</c> and
    /// <c>submit_investigation_report</c>, which write investigation state, and the 13 Level 2 agent action tools. Every
    /// action call needs an <c>agent_action</c> argument and a live claim, and the actions also need
    /// <c>agent_actions_enabled</c> (default on). The 9 store actions need a Level 2 route; the 4 server actions (session
    /// kill, plan force, unforce and evict) need a Level 3 request, the server's own action switch (default off) and a
    /// remediation login reference. Served only when a bearer token
    /// resolves AND <see cref="AllowWrites"/> is off: one token cannot tell an agent from a person, so with both on the
    /// investigation tools stay off and the start logs one Critical line. File-only and restart-only, like
    /// <see cref="AllowWrites"/>. The two investigation reads are ordinary reads and need no setting.
    /// </summary>
    [JsonPropertyName("allowInvestigations")]
    public bool AllowInvestigations { get; set; }

    /// <summary>
    /// Default OFF. A loopback-only MCP server needs a bearer token like an exposed one: with no
    /// <see cref="McpNetworkConfig.EncryptedToken"/> or <see cref="McpNetworkConfig.Token"/>, an enabled MCP server does not
    /// start, and the log names the settings to add. On, a loopback-only server with no token serves local clients
    /// without one, with the write and investigation tools off (they need the token), and every start logs one line
    /// saying so. A configured token always gates, whatever this says. File-only and restart-only.
    /// </summary>
    [JsonPropertyName("allowTokenlessLoopback")]
    public bool AllowTokenlessLoopback { get; set; }

    /// <summary>
    /// Default OFF. <c>add_servers</c> defaults to Windows authentication, where this service presents its own
    /// computer account to the host the caller names. Off, it refuses a Windows-authentication entry that also
    /// sets <c>trust_server_certificate</c> or an <c>encrypt_mode</c> other than Mandatory or Strict, because the
    /// service could not confirm which server it reached. On, it accepts them. SQL authentication and the Entra
    /// modes are not affected. Applies to the MCP tool only: the <c>--add-server</c> command is the operator's own
    /// and is not gated. File-only and restart-only, like <see cref="AllowWrites"/>. Has no effect unless
    /// <see cref="AllowServerAdmin"/> is on, since <c>add_servers</c> is not served otherwise.
    /// </summary>
    [JsonPropertyName("allowWindowsAuthOverUnverifiedTls")]
    public bool AllowWindowsAuthOverUnverifiedTls { get; set; }

    /// <summary>
    /// How much collected data the MCP tools hand out (#5632): <c>full</c> (the default, everything as read),
    /// <c>maskLiterals</c> (statement text, error text and plans keep their shape with every literal replaced;
    /// parameter values replaced) or <c>metadataOnly</c> (statement text, error text, plans, input buffers and
    /// parameter values withheld; logins and host names masked). Object and database names, counts, durations, waits,
    /// times and program names are kept at every level. Applied to every tool answer on this listener by
    /// <c>McpDataDetailFilter</c>, never inside a reader. An unrecognized value runs as <c>metadataOnly</c> and logs one
    /// warning at start. File-only and restart-only, like <see cref="AllowWrites"/>.
    /// </summary>
    [JsonPropertyName("dataDetail")]
    public string DataDetail { get; set; } = McpDataDetailModes.DefaultValue;

    /// <summary>
    /// Opt-in network exposure for the MCP server (darling-network-endpoints). Omit <c>listen</c> for the
    /// default = loopback-only HTTP; the block's token gates that listener too. Managed-mode only; ignored in BYO
    /// with a caller warning. Any missing precondition (token / valid allowFrom / managed, and a usable
    /// certificate when <c>tls</c> is set, #5288) keeps MCP loopback-only + LogCritical — enforced in the MCP
    /// host, NEVER in the all-fatal <see cref="DarlingConfig.Validate"/> (D-validate). See
    /// <see cref="McpNetworkConfig"/>.
    /// </summary>
    [JsonPropertyName("network")]
    public McpNetworkConfig? Network { get; set; }

    /// <summary>
    /// Named bearer tokens, each with a role (#6542), so one service serves an operator and its investigation agents at once.
    /// Null or empty keeps the legacy single token (<see cref="McpNetworkConfig.Token"/> or <see cref="McpNetworkConfig.EncryptedToken"/>)
    /// exactly as before. With entries, the token on each request picks the tool set: <c>operator</c> gets the reads and the write
    /// tools (never the claim or submit tools), <c>agent</c> gets the reads, the investigation tools and the Level 2 action tools
    /// (never a tool in <c>DarlingMcpWritePolicy.NeverAgentTools</c>), and an agent's identity is the entry's name. A legacy token
    /// set beside them keeps its role from <see cref="AllowWrites"/> and <see cref="AllowInvestigations"/>. File-only and restart-only.
    /// </summary>
    [JsonPropertyName("tokens")]
    public List<McpTokenEntry>? Tokens { get; set; }

    /// <summary>True when <see cref="Tokens"/> has an entry. An entry that cannot be served (no value, a bad name or role) still counts:
    /// named tokens were asked for, so a start must not fall back to serving without credentials.</summary>
    [JsonIgnore]
    public bool HasNamedTokens => Tokens is { Count: > 0 } && Tokens.Any(t => t is not null);
}

/// <summary>
/// One named MCP bearer token (#6542): a <see cref="Name"/> (1 to 64 of <c>A-Za-z0-9._:-</c>), the token itself
/// (<see cref="EncryptedToken"/> or <see cref="Token"/>, the same rules as <c>mcp.network</c>'s), and a <see cref="Role"/>,
/// <c>operator</c> or <c>agent</c>. The name is what logs and the journal record; the value is never logged.
/// </summary>
public sealed class McpTokenEntry
{
    /// <summary>The <c>operator</c> role.</summary>
    public const string OperatorRole = "operator";

    /// <summary>The <c>agent</c> role.</summary>
    public const string AgentRole = "agent";

    [JsonPropertyName("name")]
    public string? Name { get; set; }

    /// <summary>The DPAPI-encrypted token (Windows), as <c>--encrypt-password</c> writes it. Preferred over <see cref="Token"/>.</summary>
    [JsonPropertyName("encryptedToken")]
    public string? EncryptedToken { get; set; }

    /// <summary>The plaintext token, or an <c>env:</c>/<c>file:</c> reference.</summary>
    [JsonPropertyName("token")]
    public string? Token { get; set; }

    [JsonPropertyName("role")]
    public string? Role { get; set; }

    /// <summary>The token, preferring <see cref="EncryptedToken"/> over <see cref="Token"/>; null when neither is set. Throws when an
    /// encrypted token cannot be read.</summary>
    public string? ResolveToken()
    {
        /* #6542: the error names this entry's own setting, not mcp.network.token, so an operator fixes the right line. */
        var setting = $"mcp.tokens[{Name}]";
        if (!string.IsNullOrWhiteSpace(EncryptedToken))
        {
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    setting + ".encryptedToken requires Windows (DPAPI); use the plaintext \"token\" on other platforms.");
            }

            return DarlingSecrets.Unprotect(EncryptedToken);
        }

        return string.IsNullOrWhiteSpace(Token) ? null : DarlingSecretSource.Resolve(Token, setting + ".token");
    }
}

/// <summary>
/// Opt-in NETWORK exposure for the managed store (darling-network-endpoints). Omit the whole
/// <c>network</c> object for the secure default = today's loopback-only behavior (no pg_hba network
/// rule, no firewall rule, ssl=off). When present with a non-loopback <see cref="Listen"/>, the
/// service reconciles it on every start (managed mode only): listen_addresses gains the bind IP, a
/// self-signed TLS cert is generated for verify-full, a marked
/// <c>hostssl darling &lt;role&gt; &lt;allowFrom&gt; scram-sha-256</c> pg_hba block is written +
/// reloaded, and the scoped firewall rule (created by the elevated installer, #1771) is checked.
/// Reconciliation is symmetric (removing the
/// block closes the box) and fail-closed: any invalid/incomplete field degrades the store to
/// loopback + LogCritical, never exposed. These rules are enforced at the point of use, NOT in the
/// all-fatal <see cref="DarlingConfig.Validate"/> (D-validate).
/// </summary>
public sealed class PostgresNetworkConfig
{
    /// <summary>
    /// Bind IP for the store's network listener and the generated cert's iPAddress SAN. A specific IP
    /// (e.g. <c>192.168.1.205</c>) is preferred; <c>0.0.0.0</c> = all interfaces (connect by a cert SAN
    /// name under verify-full). Absent or an IPv4 loopback (<c>127.0.0.0/8</c>) = the default
    /// loopback-only store.
    /// </summary>
    [JsonPropertyName("listen")]
    public string? Listen { get; set; }

    /// <summary>
    /// The CIDR the pg_hba rule and firewall rule allow (e.g. <c>192.168.1.0/24</c>); its address family
    /// must match <see cref="Listen"/>. Required when exposed — a missing/invalid CIDR degrades the store
    /// to loopback.
    /// </summary>
    [JsonPropertyName("allowFrom")]
    public string? AllowFrom { get; set; }

    /// <summary>
    /// The least-privilege login role the network pg_hba rule names: <c>viewer</c> (default, read-only
    /// remote — the secure default) or <c>admin</c> (full remote writes; the caller warns because admin
    /// holds the <c>config_command</c>/<c>config_monitored_servers</c>/<c>config_notification</c>
    /// service-credential pivot). Never the superuser <c>darling</c>. Distinct from the local-loopback
    /// <see cref="PostgresConfig.ConnectAs"/> (default admin) — the sample/README document the difference.
    /// An unknown value degrades the store to loopback.
    /// </summary>
    [JsonPropertyName("role")]
    public string? Role { get; set; }

    /// <summary>
    /// True when any field is set — used only for the BYO "network.* is ignored" caller warning (D-BYO).
    /// It does NOT mean "exposed"; use <see cref="DarlingNetwork.IsExposedListenAddress"/> for that.
    /// </summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Listen)
        || !string.IsNullOrWhiteSpace(AllowFrom)
        || !string.IsNullOrWhiteSpace(Role);
}

/// <summary>
/// Opt-in NETWORK exposure for the embedded MCP server (darling-network-endpoints). Omit the whole
/// <c>listen</c> for the default = loopback-only HTTP, gated by this block's token. When present
/// with a non-loopback <see cref="Listen"/> AND managed mode AND a token AND a valid
/// <see cref="AllowFrom"/>, the MCP host binds the network interface behind a required bearer token +
/// an in-app CIDR check (D3); any missing precondition keeps MCP loopback-only + LogCritical
/// (fail-closed, enforced in the MCP host).
///
/// <para><b>TLS is opt-in via <see cref="Tls"/> (#5288).</b> Without it the network listener is plain HTTP, and
/// the bearer token and every tool result cross the segment in the clear — the MCP host warns about exactly
/// that at every exposed start, and a TLS-terminating reverse proxy in front of the port is the other way to
/// close the gap. <see cref="HostName"/> names the one DNS name clients reach the endpoint by. Both are
/// file-only and restart-only, like the rest of this block, and both are null by default.</para>
/// </summary>
public sealed class McpNetworkConfig
{
    /// <summary>
    /// Bind IP for the MCP network listener (a specific IP preferred; <c>0.0.0.0</c> = all interfaces).
    /// Absent or an IPv4 loopback = the default loopback-only MCP server.
    /// </summary>
    [JsonPropertyName("listen")]
    public string? Listen { get; set; }

    /// <summary>
    /// The CIDR(s) the in-app <c>RemoteIpAddress</c> check and the firewall rule allow (e.g.
    /// <c>192.168.1.0/24</c>); loopback is always allowed regardless. Required when exposed.
    ///
    /// <para>#5288: a JSON string holding one CIDR, or several separated by commas
    /// (<c>"10.8.0.0/16,192.168.1.5/32"</c>), or a JSON array of CIDR strings
    /// (<c>["10.8.0.0/16", "192.168.1.5/32"]</c>). The property stays a string: the converter joins an array
    /// with commas, and <see cref="Hosting.CidrAllowList"/> is the one parser. Every entry must be CIDR form
    /// (<c>/32</c> for one address) and of the SAME address family as <see cref="Listen"/> (a <c>::</c> listen
    /// takes IPv6 entries only); host bits are masked rather than refused. One bad entry degrades the whole
    /// listener to loopback-only with a Critical line — an entry is never dropped quietly.</para>
    /// </summary>
    [JsonPropertyName("allowFrom")]
    [JsonConverter(typeof(Hosting.CidrListJsonConverter))]
    public string? AllowFrom { get; set; }

    /// <summary>
    /// DPAPI-LocalMachine-protected bearer token, base64 — produced by <c>--encrypt-password</c>
    /// (preferred over the plaintext <see cref="Token"/>). Read via <see cref="ResolveToken"/>.
    /// </summary>
    [JsonPropertyName("encryptedToken")]
    public string? EncryptedToken { get; set; }

    /// <summary>
    /// The bearer token as a literal (dev convenience only; the caller warns) or an
    /// <c>env:</c>/<c>file:</c> reference (#1804 — <see cref="DarlingSecretSource"/>, which does not
    /// count as plaintext-in-config). Prefer <see cref="EncryptedToken"/> on Windows.
    /// </summary>
    [JsonPropertyName("token")]
    public string? Token { get; set; }

    /// <summary>
    /// Opt-in TLS for the MCP network listener (#5288). Omit for plain HTTP, which is the zero-config default and
    /// the right answer on loopback; supply a certificate before exposing MCP on a segment where the bearer
    /// token and the tool results crossing in the clear matter. The SAME type as <c>web.network.tls</c>
    /// (<see cref="WebTlsConfig"/>), but its OWN block with its own fail-closed decision: no shared block and no
    /// precedence rule between the two listeners. To serve one certificate on both, point both blocks at the
    /// same files. A bad, expired or not-yet-valid certificate keeps MCP loopback-only with a Critical line,
    /// never plain HTTP on the LAN. File-only and restart-only: a renewed certificate file is served after a
    /// service restart.
    /// </summary>
    [JsonPropertyName("tls")]
    public WebTlsConfig? Tls { get; set; }

    /// <summary>
    /// The ONE DNS name clients reach the MCP endpoint by (#5288), e.g. <c>mcp.corp.example</c>: a bare name with
    /// no scheme, port, path, wildcard or IP address (see <see cref="NormalizeHostName"/> for the exact rules).
    /// In NETWORK mode the Host-header guard admits that one exact name, compared ignoring case, beside the
    /// names it always admits; it never admits a name the operator did not write here. In loopback-only mode,
    /// and in every mode that degrades to it (an unreadable token, a refused certificate), the guard does NOT
    /// admit it: the name exists for the network listener's clients, and a loopback-only listener has no use for it. With
    /// <see cref="Tls"/> set it is also the name the certificate should carry as a dNSName SAN, and the host
    /// warns (never refuses) when it does not. A value that is set but is not a bare DNS name is ignored with one
    /// Warning at start. A name written in Unicode is admitted as the ASCII (punycode) name a client sends in
    /// its Host header; a punycode (<c>xn--</c>) label that does not decode, such as <c>xn--a</c>, is refused the
    /// same way. Unlike <c>web.publicBaseUrl</c> (admitted in both modes) this is a bare host rather than
    /// a URL, because MCP has no link builder: a scheme, port and path would be unused fields that could
    /// disagree with the listener. File-only and restart-only.
    /// </summary>
    [JsonPropertyName("hostName")]
    public string? HostName { get; set; }

    /// <summary>
    /// True when any field is set — used only for the BYO "network.* is ignored" caller warning (D-BYO);
    /// NOT the same as "exposed".
    /// </summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Listen)
        || !string.IsNullOrWhiteSpace(AllowFrom)
        || !string.IsNullOrWhiteSpace(EncryptedToken)
        || !string.IsNullOrWhiteSpace(Token)
        || !string.IsNullOrWhiteSpace(HostName)
        || (Tls?.IsConfigured ?? false);

    /// <summary>
    /// The bare DNS name a raw <see cref="HostName"/> stands for, or null when there is none to use (#5288). PURE:
    /// no DNS lookup, no config, no logger. The rules, in this order:
    ///
    /// <list type="number">
    /// <item>A null, empty or blank value is "not set": null.</item>
    /// <item>Surrounding whitespace is trimmed.</item>
    /// <item>ONE trailing dot is stripped (<c>mcp.corp.example.</c> is the same name written fully qualified).
    /// A name that still ends in a dot after that (<c>host..</c>) is refused: an empty label is a typo, not a
    /// name.</item>
    /// <item>A name with any non-ASCII character is an internationalized name. A client sends its ASCII
    /// (punycode) form in the Host header, never the Unicode form, so a Unicode name could never match: it is
    /// mapped with <c>IdnMapping.GetAscii</c> (<c>b&#252;cher.example</c> becomes <c>xn--bcher-kva.example</c>,
    /// folded to lower case as IDNA does), and every rule below runs on that ASCII form. A name the mapping
    /// refuses (an empty label, a label over 63 characters once encoded, a lone surrogate) is refused: null. So
    /// is a mapped name that ends in a dot, which the mapping can produce from an ideographic full stop. An
    /// all-ASCII name is never mapped, so it keeps its spelling, and its case too unless it has an <c>xn--</c>
    /// label (see the last rule).</item>
    /// <item>Only a DNS name survives: <c>Uri.CheckHostName</c> must say <c>Dns</c>, on the ASCII form. An IP
    /// address (the guard admits the listen IP by itself, and a fullwidth <c>10.1.2.3</c> maps to one), a port
    /// (<c>host:5152</c>), a scheme or path (<c>https://host/</c>), a wildcard (<c>*.corp.example</c>) and
    /// anything with whitespace inside are refused: null.</item>
    /// <item>A name with an <c>xn--</c> label must decode: <c>IdnMapping.GetUnicode</c> must not throw for it. The
    /// MCP host decodes the name the same way before the Host-header guard compares it, and a malformed label
    /// (<c>xn--a</c>) is a valid DNS name to <c>Uri.CheckHostName</c> yet makes that decode throw, so it is refused
    /// here: null. A name that decodes is never rewritten to Unicode, but it is folded to lower case: IDNA names
    /// are case-insensitive, and the framework decodes a Host header's <c>xn--</c> labels only when the prefix is
    /// lower case (the form a client sends), so every spelling of one punycode name has to reach the guard in that
    /// one form.</item>
    /// </list>
    ///
    /// <para>An ASCII name keeps its case as written, because the Host-header guard compares ignoring case; the one
    /// exception is a name with an <c>xn--</c> label, which is folded to lower case (the last rule above). A
    /// caller tells "not set" from "set but refused" by testing <see cref="HostName"/> for blank first: a refused
    /// value is ignored with one Warning at start, never an error, and never a reason to degrade a listener.</para>
    /// </summary>
    /// <param name="value">The raw <see cref="HostName"/>.</param>
    /// <returns>The trimmed name without its trailing dot (its ASCII form when it was written in Unicode, folded to
    /// lower case when it has an <c>xn--</c> label), or null when unset or refused.</returns>
    public static string? NormalizeHostName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var name = value.Trim();
        if (name.EndsWith('.'))
        {
            name = name[..^1];
        }

        /* "host.." had two dots: stripping one leaves an empty label that CheckHostName may let through, and no
           client sends it as a Host. A bare "." strips to nothing. Both are refused. */
        if (name.Length == 0 || name.EndsWith('.'))
        {
            return null;
        }

        /* A client sends the punycode (xn--) form of an internationalized name in its Host header, so a Unicode
           value could never match what the guard compares it with. Map it to that form first; the DNS test below
           then runs on the ASCII form, which is also what refuses a fullwidth "10.1.2.3" (it maps to an IPv4
           address) or a "host:5152" the mapping let through. Only a name with a non-ASCII character is mapped:
           an ASCII name is left exactly as written (an xn-- label folds the case further down). The mapping throws ArgumentException for a
           name that is not a valid IDN (an empty label, a label too long once encoded, a lone surrogate): refused,
           never an exception out of a start-up config read. */
        if (!System.Text.Ascii.IsValid(name))
        {
            try
            {
                name = new System.Globalization.IdnMapping().GetAscii(name);
            }
            catch (ArgumentException)
            {
                return null;
            }

            /* The mapping folds the ideographic full stops (U+3002, U+FF0E, U+FF61) into dots, so a name can end in
               a dot only now: refused, as the same typo written with an ASCII dot is above. */
            if (name.Length == 0 || name.EndsWith('.'))
            {
                return null;
            }
        }

        if (Uri.CheckHostName(name) != UriHostNameType.Dns)
        {
            return null;
        }

        /* #5288: the MCP host runs this name through HostString.FromUriComponent (it decodes every xn-- label with
           IdnMapping.GetUnicode) so the guard compares it in the form the framework gives the middleware. A
           malformed label such as "xn--a" is ASCII letters and a hyphen, so it passes the DNS test above, then makes
           that decode throw ArgumentException, which stopped MCP from starting. A name the framework cannot decode
           could never equal a decoded Host anyway: it is refused here, so the host logs its one "set but refused"
           Warning and admits nothing for it (fail closed). Only the check runs, on the ASCII form: the decoded
           result is dropped, so the name is never rewritten to Unicode. A name with no xn-- label is never decoded. */
        if (name.Contains("xn--", StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                _ = new System.Globalization.IdnMapping().GetUnicode(name);
            }
            catch (ArgumentException)
            {
                return null;
            }

            /* IDNA names are case-insensitive, and the framework decodes a Host header's xn-- labels only when the
               prefix is lower case, which is the form a client sends. The guard decodes this name the same way, so
               an upper-case spelling would stay undecoded while the client's header is decoded, and the two would
               not compare equal. A name with an xn-- label is therefore folded to lower case here (it is all ASCII
               by now), which is also the form the mapping above gives a Unicode name. A name with no xn-- label
               keeps its case as written. */
            if (Array.Exists(name.Split('.'), label => label.StartsWith("xn--", StringComparison.OrdinalIgnoreCase)))
            {
                name = name.ToLowerInvariant();
            }
        }

        return name;
    }

    /// <summary>
    /// The bearer token, preferring <see cref="EncryptedToken"/> (DPAPI-decrypted; Windows-only) over
    /// the plaintext <see cref="Token"/>. <paramref name="usedPlaintext"/> is true when the plaintext
    /// fallback is used, so the caller can warn — the same shape as
    /// <see cref="DarlingSecrets.ResolvePassword"/>. Returns null when neither is set.
    /// </summary>
    public string? ResolveToken(out bool usedPlaintext)
    {
        usedPlaintext = false;

        if (!string.IsNullOrWhiteSpace(EncryptedToken))
        {
            /* DarlingSecrets.Unprotect is DPAPI (Windows-only). The network path is managed-mode-only,
               which is itself Windows-gated, so this only runs on Windows; the guard keeps the analyzer
               (CA1416) honest and gives a clear failure if someone points a non-Windows BYO client here. */
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    "mcp.network.encryptedToken requires Windows (DPAPI); use the plaintext \"token\" on other platforms.");
            }

            return DarlingSecrets.Unprotect(EncryptedToken);
        }

        if (!string.IsNullOrWhiteSpace(Token))
        {
            /* An env:/file: reference (#1804) is not plaintext-in-config — no warning for it. */
            usedPlaintext = !DarlingSecretSource.IsReference(Token);
            return DarlingSecretSource.Resolve(Token, "mcp.network.token");
        }

        return null;
    }
}

/// <summary>
/// The embedded web dashboard's config (#1562). Port 5153 keeps the product's local port family
/// non-colliding on one machine (Dashboard MCP 5150, Lite MCP 5151, Darling MCP 5152, Darling Web 5153).
/// Loopback-only by default; the optional <see cref="Network"/> block opts into LAN exposure behind a
/// required token → HMAC session cookie + an in-app CIDR check (darling-network-endpoints, managed-mode only).
/// A SEPARATE surface from <see cref="McpConfig"/> — its own enable flag, port, token, and exposure block —
/// because the two gate different blast radii (MCP's token guards analyze_server's live outbound SQL
/// connections; the web dashboard works on the collected store and changes only monitoring settings).
/// </summary>
public sealed class WebConfig
{
    /// <summary>Default OFF — a headless service should not open the dashboard port unless asked.</summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    [JsonPropertyName("port")]
    public int Port { get; set; } = 5153;

    /// <summary>
    /// The externally reachable base URL of this dashboard (#2710) — e.g. <c>http://10.0.0.5:5153</c> — used
    /// only to build the triage-page link alert webhooks carry (<c>TriageLink.Build</c>). Empty (the default)
    /// means alerts carry no link. It is configuration rather than derived from <see cref="Network"/>'s bind
    /// address, because what the service binds is not necessarily what recipients can reach (a proxy, a DNS
    /// name, TLS on a different host name). Deliberately FILE-authoritative like the rest of this block —
    /// deployment plumbing, not an alert-tuning knob — so a store config reload (which overwrites only
    /// <see cref="Enabled"/>/<see cref="Port"/> here) never resets it. An invalid value (not absolute
    /// http/https) is treated as unset by the link builder rather than shipping a dead href.
    /// </summary>
    [JsonPropertyName("publicBaseUrl")]
    public string PublicBaseUrl { get; set; } = "";

    /// <summary>
    /// Default OFF. A loopback-only dashboard needs a token like an exposed one: with no
    /// <see cref="WebNetworkConfig.EncryptedToken"/> or <see cref="WebNetworkConfig.Token"/>, an enabled dashboard does not
    /// start, and the log names the settings to add. On, a loopback-only dashboard with no token serves local browsers
    /// without one, for reads only (a change is refused with a message naming the token setting), and every start logs
    /// one line saying so. A configured token always gates, whatever this says. File-only and restart-only.
    /// </summary>
    [JsonPropertyName("allowTokenlessLoopback")]
    public bool AllowTokenlessLoopback { get; set; }

    /// <summary>
    /// Default OFF; a darling.json without the key keeps it off, so an upgrade from 3.11 keeps that release's web scope until the
    /// operator turns it on. The files this product writes for a NEW install (the MSI's minimal darling.json and darling.sample.json) set
    /// it to true explicitly (#6600). On the managed store the dashboard signs in as the read-only <c>viewer</c> role, so
    /// its Admin writes were list-only. On, the dashboard opens a second, small connection pool that signs in as the managed <c>admin</c> role
    /// (the Viewer's login) and uses it only for those writes and their access checks, and only for a person signed in as an admin; every read
    /// stays on <c>viewer</c>. Off keeps the dashboard list-only on the managed store. A store you bring yourself is unchanged either way.
    /// File-only and restart-only.
    /// </summary>
    [JsonPropertyName("allowAdminWrites")]
    public bool AllowAdminWrites { get; set; }

    /// <summary>
    /// Default OFF; a darling.json without the key keeps it off. Adding or editing a server in the web dashboard defaults to Windows
    /// authentication, where this service presents its own computer account to the host the form names. Off, the web add and edit refuse a
    /// Windows-authentication server that also sets <c>trust_server_certificate</c> or an <c>encrypt_mode</c> other than Mandatory or
    /// Strict, because the service could not confirm which server it reached. An edit is judged only when it changes the machine the entry
    /// names or its sign-in and certificate settings (the host, the port, the auth type, <c>trust_server_certificate</c>,
    /// <c>encrypt_mode</c> or the Query Store replica), on the server as it would be saved; <c>read_only_intent</c> and
    /// <c>multi_subnet_failover</c> do not change the host the entry names, so they are not judged. On, the web accepts
    /// them. SQL authentication and the Entra modes are not affected. The same rule as <see cref="McpConfig.AllowWindowsAuthOverUnverifiedTls"/>
    /// for MCP; the <c>--add-server</c> command is the operator's own and is not gated. File-only and restart-only.
    /// </summary>
    [JsonPropertyName("allowWindowsAuthOverUnverifiedTls")]
    public bool AllowWindowsAuthOverUnverifiedTls { get; set; }

    /// <summary>
    /// Opt-in network exposure for the web dashboard (darling-network-endpoints). Omit <c>listen</c> for the
    /// default = loopback-only HTTP; the block's token gates that listener too. Managed-mode only; ignored in BYO with a caller warning. Any missing
    /// precondition (token / valid allowFrom / managed) keeps the dashboard loopback-only + LogCritical —
    /// enforced in the web host, NEVER in the all-fatal <see cref="DarlingConfig.Validate"/> (D-validate).
    /// See <see cref="WebNetworkConfig"/>.
    /// </summary>
    [JsonPropertyName("network")]
    public WebNetworkConfig? Network { get; set; }
}

/// <summary>
/// Opt-in NETWORK exposure for the embedded web dashboard (#1562), the twin of <see cref="McpNetworkConfig"/>.
/// Omit the whole <c>network</c> object for the secure default = loopback-only HTTP. When present with a
/// non-loopback <see cref="Listen"/> AND managed mode AND a token AND a valid <see cref="AllowFrom"/>, the web
/// host binds the network interface behind an in-app CIDR check and a token gate: a browser presents the token
/// once in the sign-in form, which is exchanged for an HMAC-signed session cookie. While exposed EVERY request
/// authenticates, loopback included — loopback is exempt from the CIDR test only, never from the credential
/// (#1649). Any missing precondition keeps the dashboard loopback-only + LogCritical (fail-closed, enforced in
/// the web host).
///
/// <para><b>TLS is opt-in via <see cref="Tls"/> (#2562).</b> Without it the network listener is plain HTTP and
/// the token and session cookie cross the segment in the clear — the web host warns about exactly that at
/// every exposed start. The MCP endpoint has its own <c>mcp.network.tls</c> block of the same type (#5288);
/// the two listeners never share a block.</para>
/// </summary>
public sealed class WebNetworkConfig
{
    /// <summary>
    /// Bind IP for the web network listener (a specific IP preferred; <c>0.0.0.0</c> = all interfaces).
    /// Absent or an IPv4 loopback = the default loopback-only dashboard.
    /// </summary>
    [JsonPropertyName("listen")]
    public string? Listen { get; set; }

    /// <summary>
    /// The CIDR(s) the in-app <c>RemoteIpAddress</c> check and the firewall rule allow (e.g.
    /// <c>192.168.1.0/24</c>); loopback is always allowed regardless. Required when exposed.
    ///
    /// <para>#5288: a JSON string holding one CIDR, or several separated by commas
    /// (<c>"10.8.0.0/16,192.168.1.5/32"</c>), or a JSON array of CIDR strings
    /// (<c>["10.8.0.0/16", "192.168.1.5/32"]</c>). The property stays a string: the converter joins an array
    /// with commas, and <see cref="Hosting.CidrAllowList"/> is the one parser. Every entry must be CIDR form
    /// (<c>/32</c> for one address) and of the SAME address family as <see cref="Listen"/> (a <c>::</c> listen
    /// takes IPv6 entries only); host bits are masked rather than refused. One bad entry degrades the whole
    /// listener to loopback-only with a Critical line — an entry is never dropped quietly.</para>
    /// </summary>
    [JsonPropertyName("allowFrom")]
    [JsonConverter(typeof(Hosting.CidrListJsonConverter))]
    public string? AllowFrom { get; set; }

    /// <summary>
    /// DPAPI-LocalMachine-protected access token, base64 — produced by <c>--encrypt-password</c>
    /// (preferred over the plaintext <see cref="Token"/>). Read via <see cref="ResolveToken"/>.
    /// </summary>
    [JsonPropertyName("encryptedToken")]
    public string? EncryptedToken { get; set; }

    /// <summary>
    /// The access token as a literal (dev convenience only; the caller warns) or an <c>env:</c>/<c>file:</c>
    /// reference (#1804 — <see cref="DarlingSecretSource"/>, which does not count as plaintext-in-config, and
    /// is how the compose distribution mounts it). Prefer <see cref="EncryptedToken"/> on Windows.
    /// </summary>
    [JsonPropertyName("token")]
    public string? Token { get; set; }

    /// <summary>
    /// Opt-in TLS for the network listener (#2562). Omit for plain HTTP, which is the zero-config default and
    /// the right answer on loopback; supply a certificate before exposing the dashboard on a segment where the
    /// access token crossing in the clear matters. See <see cref="WebTlsConfig"/>.
    /// </summary>
    [JsonPropertyName("tls")]
    public WebTlsConfig? Tls { get; set; }

    /// <summary>
    /// Opt-in per-user OIDC sign-in for the LAN-exposed dashboard (#2550). Omit for the existing behavior —
    /// the shared token is the only credential. When configured, the login page additionally offers an SSO
    /// sign-in (authorization code + PKCE); the shared token KEEPS working beside it as the scripted-caller /
    /// break-glass fallback. See <see cref="WebOidcConfig"/>.
    /// </summary>
    [JsonPropertyName("oidc")]
    public WebOidcConfig? Oidc { get; set; }

    /// <summary>
    /// The reverse proxies in front of the dashboard whose <c>X-Forwarded-For</c> / <c>X-Forwarded-Proto</c> /
    /// <c>X-Forwarded-Host</c> headers the host honors — each entry an IP or a CIDR. Omit (the default) and
    /// the headers are ignored from every source. Needed behind a TLS-terminating proxy: the connection to
    /// this host is plain HTTP there, so without it the session cookie is not marked Secure and the OIDC
    /// redirect URI is built as <c>http://</c>. With it, <see cref="AllowFrom"/> is checked against the
    /// forwarded client address rather than the proxy's, so it has to cover the clients. Parsed by
    /// <c>DarlingHostBinding.TryParseTrustedProxies</c>; an entry that does not parse keeps the dashboard
    /// loopback-only with a Critical line.
    /// </summary>
    [JsonPropertyName("trustedProxies")]
    public string[]? TrustedProxies { get; set; }

    /// <summary>
    /// True to send <c>Strict-Transport-Security</c> (one year, no subdomains) on every response to an HTTPS
    /// request. Off by default: a browser that has seen it uses HTTPS for every port of this host name for a
    /// year and cannot be told otherwise, including for other services on that name that speak plain HTTP.
    /// Turn it on once the dashboard is reached over HTTPS only.
    /// </summary>
    [JsonPropertyName("hsts")]
    public bool Hsts { get; set; }

    /// <summary>
    /// True when any field is set — used only for the BYO "network.* is ignored" caller warning (D-BYO);
    /// NOT the same as "exposed".
    /// </summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Listen)
        || !string.IsNullOrWhiteSpace(AllowFrom)
        || !string.IsNullOrWhiteSpace(EncryptedToken)
        || !string.IsNullOrWhiteSpace(Token)
        || (Tls?.IsConfigured ?? false)
        || (Oidc?.IsConfigured ?? false)
        || (TrustedProxies?.Length ?? 0) > 0;

    /// <summary>
    /// The access token, preferring <see cref="EncryptedToken"/> (DPAPI-decrypted; Windows-only) over the
    /// plaintext <see cref="Token"/>. <paramref name="usedPlaintext"/> is true when the plaintext fallback is
    /// used, so the caller can warn — the same shape as <see cref="McpNetworkConfig.ResolveToken"/>. Returns
    /// null when neither is set.
    /// </summary>
    public string? ResolveToken(out bool usedPlaintext)
    {
        usedPlaintext = false;

        if (!string.IsNullOrWhiteSpace(EncryptedToken))
        {
            /* DarlingSecrets.Unprotect is DPAPI (Windows-only). The network path is managed-mode-only, which is
               itself Windows-gated, so this only runs on Windows; the guard keeps the analyzer (CA1416) honest. */
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    "web.network.encryptedToken requires Windows (DPAPI); use the plaintext \"token\" on other platforms.");
            }

            return DarlingSecrets.Unprotect(EncryptedToken);
        }

        if (!string.IsNullOrWhiteSpace(Token))
        {
            /* An env:/file: reference (#1804) is not plaintext-in-config — no warning for it. */
            usedPlaintext = !DarlingSecretSource.IsReference(Token);
            return DarlingSecretSource.Resolve(Token, "web.network.token");
        }

        return null;
    }
}

/// <summary>
/// Opt-in per-user OIDC sign-in for the LAN-exposed web dashboard (#2550) — authorization code + PKCE against
/// a standard OpenID Connect provider, resolved via its discovery document. Entirely OFF when this block is
/// absent: the shared token → cookie exchange is byte-for-byte unchanged, and it KEEPS working beside OIDC when
/// this is configured (the scripted-caller and break-glass path — an IdP outage must not lock the operator out
/// of their own monitoring). Lives in darling.json rather than the store because the client secret needs the
/// same DPAPI treatment <see cref="WebNetworkConfig.EncryptedToken"/> gets, and the host has to be able to bind
/// before the store is necessarily reachable — the same reasoning as the token. File-defined, restart-only,
/// like the rest of the network block.
///
/// <para><b>Role mapping.</b> With <see cref="RoleClaim"/> unset, every signed-in user gets the same reach the
/// shared token grants (edit) — parity, not a new privilege. With it set, membership decides: a claim value in
/// <see cref="AdminRoles"/> ⇒ edit; else a value in <see cref="ViewerRoles"/> ⇒ read-only; else the sign-in is
/// REFUSED — an authenticated-at-the-IdP user with no mapped role gets nothing, which is per-user revocation
/// doing its job. Matching is case-sensitive ordinal: role values are identifiers (Entra emits group GUIDs),
/// not prose.</para>
/// </summary>
public sealed class WebOidcConfig
{
    /// <summary>
    /// The provider's issuer/authority URL (e.g. <c>https://login.microsoftonline.com/{tenant}/v2.0</c> or
    /// <c>https://{org}.okta.com/oauth2/default</c>). Discovery is fetched from
    /// <c>{authority}/.well-known/openid-configuration</c>. Must be <c>https://</c> — plain <c>http://</c> is
    /// accepted only for a loopback host (a local test IdP), because over HTTP the code exchange and the
    /// tokens it returns would cross the wire in the clear.
    /// </summary>
    [JsonPropertyName("authority")]
    public string? Authority { get; set; }

    /// <summary>The registered client (application) id. Also the required ID-token audience — an ID token is
    /// minted FOR a client, so there is no separate audience knob to misconfigure.</summary>
    [JsonPropertyName("clientId")]
    public string? ClientId { get; set; }

    /// <summary>
    /// DPAPI-LocalMachine-protected client secret, base64 — produced by <c>--encrypt-password</c>
    /// (preferred over the plaintext <see cref="ClientSecret"/>). Read via <see cref="ResolveClientSecret"/>.
    /// </summary>
    [JsonPropertyName("encryptedClientSecret")]
    public string? EncryptedClientSecret { get; set; }

    /// <summary>
    /// The client secret as a literal (dev convenience only; the caller warns) or an <c>env:</c>/<c>file:</c>
    /// reference (#1804). Optional even in combination with <see cref="EncryptedClientSecret"/> absent: with
    /// no secret at all the exchange runs as a PUBLIC client on PKCE alone, which some providers permit;
    /// a confidential client with a secret is the recommended registration.
    /// </summary>
    [JsonPropertyName("clientSecret")]
    public string? ClientSecret { get; set; }

    /// <summary>Space-separated scopes. Default <c>openid profile email</c>; <c>openid</c> is always included
    /// even when this names a set without it, because without it there is no ID token and no sign-in.</summary>
    [JsonPropertyName("scopes")]
    public string? Scopes { get; set; }

    /// <summary>
    /// The claim that becomes the seat's identity (the <c>updated_by</c> stamp). Unset = the useful-name chain:
    /// <c>preferred_username</c> (Entra's UPN), then <c>email</c>, then <c>sub</c>. When SET, that exact claim
    /// is required — a token without it REFUSES the sign-in rather than silently falling back, because "the
    /// operator asked for X" and "we stamped something else" must not coexist quietly.
    /// </summary>
    [JsonPropertyName("subjectClaim")]
    public string? SubjectClaim { get; set; }

    /// <summary>The ID-token claim carrying role/group membership (e.g. <c>roles</c>, <c>groups</c>). String
    /// or array-of-strings both work. Required whenever <see cref="AdminRoles"/>/<see cref="ViewerRoles"/> are
    /// set, and pointless without them — either half alone is refused as a misconfiguration.</summary>
    [JsonPropertyName("roleClaim")]
    public string? RoleClaim { get; set; }

    /// <summary>Claim values granting the EDIT seat (custom-view CRUD and any future write surface).</summary>
    [JsonPropertyName("adminRoles")]
    public string[]? AdminRoles { get; set; }

    /// <summary>Claim values granting the READ-ONLY seat: the whole read surface plus running composed panels,
    /// no mutations. The SPA already renders this seat (it hides edit affordances when <c>/api/session</c>
    /// reports <c>can_edit: false</c>); the server refuses the writes regardless of what a client sends.</summary>
    [JsonPropertyName("viewerRoles")]
    public string[]? ViewerRoles { get; set; }

    /// <summary>True when any field is set — the "does the operator want OIDC" gate.</summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Authority)
        || !string.IsNullOrWhiteSpace(ClientId)
        || !string.IsNullOrWhiteSpace(EncryptedClientSecret)
        || !string.IsNullOrWhiteSpace(ClientSecret)
        || !string.IsNullOrWhiteSpace(Scopes)
        || !string.IsNullOrWhiteSpace(SubjectClaim)
        || !string.IsNullOrWhiteSpace(RoleClaim)
        || AdminRoles is { Length: > 0 }
        || ViewerRoles is { Length: > 0 };

    /// <summary>
    /// The client secret, preferring <see cref="EncryptedClientSecret"/> (DPAPI-decrypted; Windows-only) over
    /// the plaintext/reference <see cref="ClientSecret"/> — the same shape as
    /// <see cref="WebNetworkConfig.ResolveToken"/>. Returns null when neither is set (public-client PKCE-only
    /// exchange).
    /// </summary>
    public string? ResolveClientSecret(out bool usedPlaintext)
    {
        usedPlaintext = false;

        if (!string.IsNullOrWhiteSpace(EncryptedClientSecret))
        {
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    "web.network.oidc.encryptedClientSecret requires Windows (DPAPI); use \"clientSecret\" with an env:/file: reference on other platforms.");
            }

            return DarlingSecrets.Unprotect(EncryptedClientSecret);
        }

        if (!string.IsNullOrWhiteSpace(ClientSecret))
        {
            /* An env:/file: reference (#1804) is not plaintext-in-config — no warning for it. */
            usedPlaintext = !DarlingSecretSource.IsReference(ClientSecret);
            return DarlingSecretSource.Resolve(ClientSecret, "web.network.oidc.clientSecret");
        }

        return null;
    }
}

/// <summary>
/// Opt-in TLS for a network listener (#2562, #5288). This one type backs BOTH <c>web.network.tls</c> (the web
/// dashboard) and <c>mcp.network.tls</c> (the MCP endpoint): two separate blocks, each its own fail-closed
/// decision, with no shared block and no precedence rule. To serve one certificate on both, point both blocks at
/// the same files. Omit the whole <c>tls</c> object for plain HTTP — the zero-config default, and the correct
/// one for a loopback-only listener, which has nothing to encrypt. Supply a certificate to close the gap the
/// exposure block otherwise leaves open: the access token (and, on the web, the HMAC session cookie it is
/// exchanged for; on MCP, every tool result) crosses the segment in the clear over HTTP, and the in-app CIDR
/// check bounds who can ROUTE to the port, never what an on-path attacker can read off the wire.
///
/// <para><b>Two forms, exactly one at a time.</b> A PKCS#12 bundle (<see cref="PfxPath"/>, with the password
/// in whichever of the three slots suits the platform) or a PEM pair (<see cref="CertPath"/> +
/// <see cref="KeyPath"/>, which is what a container mounts). Configuring both is refused rather than resolved
/// by precedence — see <see cref="Hosting.DarlingWebTls.Describe"/>.</para>
///
/// <para><b>The product consumes a certificate; it does not manage a PKI.</b> No issuance, no ACME, no
/// self-signed fallback: a certificate the product minted itself would buy encryption without authentication
/// and train the operator to click through a browser warning, which is a worse habit than the plain HTTP it
/// replaces. An internal CA is the normal answer on the LAN this feature is for.</para>
///
/// <para><b>Fail-closed, like every other exposure precondition.</b> A missing, unreadable, mismatched or
/// EXPIRED certificate keeps the listener loopback-only and logs Critical. It never falls back to serving
/// the LAN over HTTP: an operator who configured TLS and silently got cleartext would be in precisely the
/// state this block exists to prevent.</para>
/// </summary>
public sealed class WebTlsConfig
{
    /// <summary>
    /// Path to a PKCS#12 (<c>.pfx</c>/<c>.p12</c>) bundle holding the certificate AND its private key.
    /// Mutually exclusive with <see cref="CertPath"/>/<see cref="KeyPath"/>.
    /// </summary>
    [JsonPropertyName("pfxPath")]
    public string? PfxPath { get; set; }

    /// <summary>
    /// DPAPI-LocalMachine-protected password for <see cref="PfxPath"/>, base64 — produced by
    /// <c>--encrypt-password</c>, and the preferred slot on Windows. Read via
    /// <see cref="ResolvePfxPassword"/>.
    /// </summary>
    [JsonPropertyName("encryptedPfxPassword")]
    public string? EncryptedPfxPassword { get; set; }

    /// <summary>
    /// The PKCS#12 password as a literal (dev convenience only; the caller warns) or an
    /// <c>env:</c>/<c>file:</c> reference (#1804 — <see cref="DarlingSecretSource"/>, which does not count as
    /// plaintext-in-config). Omit entirely for a bundle that has no password.
    /// </summary>
    [JsonPropertyName("pfxPassword")]
    public string? PfxPassword { get; set; }

    /// <summary>
    /// Path to a PEM-encoded certificate (the leaf first; a chain may follow). Requires
    /// <see cref="KeyPath"/>, and is mutually exclusive with <see cref="PfxPath"/>.
    /// </summary>
    [JsonPropertyName("certPath")]
    public string? CertPath { get; set; }

    /// <summary>
    /// Path to the PEM-encoded PRIVATE KEY for <see cref="CertPath"/>. A separate file rather than a combined
    /// PEM because that is how both Docker secrets and every certificate tool in this space emit them, and
    /// because the two want different file permissions.
    /// </summary>
    [JsonPropertyName("keyPath")]
    public string? KeyPath { get; set; }

    /// <summary>True when any field is set. Drives the "this block is configured" half of the decision — a
    /// block set but unusable must FAIL rather than read as "TLS was never asked for".</summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(PfxPath)
        || !string.IsNullOrWhiteSpace(EncryptedPfxPassword)
        || !string.IsNullOrWhiteSpace(PfxPassword)
        || !string.IsNullOrWhiteSpace(CertPath)
        || !string.IsNullOrWhiteSpace(KeyPath);

    /// <summary>
    /// The PKCS#12 password, preferring <see cref="EncryptedPfxPassword"/> (DPAPI-decrypted; Windows-only)
    /// over <see cref="PfxPassword"/> — the same shape as <see cref="WebNetworkConfig.ResolveToken"/>.
    /// Returns null when neither is set, which is correct for a bundle with no password rather than an error.
    /// </summary>
    /// <param name="usedPlaintext">True when the plaintext <see cref="PfxPassword"/> literal was used (an
    /// <c>env:</c>/<c>file:</c> reference is not plaintext), so the caller can warn.</param>
    /// <param name="section">Which listener's block this is, <c>"web"</c> (the default) or <c>"mcp"</c>; it only
    /// names the setting in the messages (<c>{section}.network.tls.pfxPassword</c>).</param>
    public string? ResolvePfxPassword(out bool usedPlaintext, string section = "web")
    {
        usedPlaintext = false;

        if (!string.IsNullOrWhiteSpace(EncryptedPfxPassword))
        {
            /* DarlingSecrets.Unprotect is DPAPI (Windows-only). Unlike the token slots this one is reachable
               from the container path too (exposure is honored in a container since #1804), so the guard is a
               real branch here, not just analyzer bookkeeping: say which slot to use instead. */
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    $"{section}.network.tls.encryptedPfxPassword requires Windows (DPAPI); use \"pfxPassword\" with a "
                    + "file:/env: reference on other platforms.");
            }

            return DarlingSecrets.Unprotect(EncryptedPfxPassword);
        }

        if (!string.IsNullOrWhiteSpace(PfxPassword))
        {
            /* An env:/file: reference (#1804) is not plaintext-in-config — no warning for it. */
            usedPlaintext = !DarlingSecretSource.IsReference(PfxPassword);
            return DarlingSecretSource.Resolve(PfxPassword, $"{section}.network.tls.pfxPassword");
        }

        return null;
    }
}

/// <summary>
/// Shared network-exposure helpers for the opt-in store/MCP endpoints (darling-network-endpoints).
/// These pure functions are the single source of truth for "is this listen value a network bind?"
/// and "what pg_hba role?" — used by the store bootstrap (<see cref="DarlingManagedPostgres"/>), the
/// worker's startup warnings (<see cref="DarlingWorker"/>), and the MCP host's bind resolution. Kept
/// deliberately OUT of the all-fatal <see cref="DarlingConfig.Validate"/> (one entry there kills
/// collection): a typo in an optional, default-off endpoint must degrade to loopback + LogCritical
/// and keep collecting (D-validate).
/// </summary>
public static class DarlingNetwork
{
    /// <summary>
    /// Is <paramref name="listen"/> a NETWORK (non-loopback) bind — i.e. anything other than the safe
    /// IPv4 loopback range <c>127.0.0.0/8</c>? Absent/blank is NOT exposed (the default = today's
    /// loopback-only behavior). Everything else is treated as exposed, and therefore subject to the full
    /// exposure validation: a real IP; <c>0.0.0.0</c>/<c>::</c> (all interfaces); the IPv6 loopback
    /// <c>::1</c> (the store's loopback handling is IPv4-<c>127</c> only); and any value that is not even
    /// an IP — <c>localhost</c>, <c>*</c>, a hostname — which the store bind (it needs
    /// <see cref="IPAddress.Parse"/>) then rejects by degrading to loopback. Fail-safe: unknown ⇒
    /// exposed ⇒ validated, never silently bound.
    /// </summary>
    public static bool IsExposedListenAddress(string? listen)
    {
        if (string.IsNullOrWhiteSpace(listen))
        {
            return false;
        }

        if (IPAddress.TryParse(listen.Trim(), out var ip))
        {
            /* The ONLY non-exposed value is an IPv4 address in 127.0.0.0/8 (canonical loopback). */
            return !(ip.AddressFamily == AddressFamily.InterNetwork && ip.GetAddressBytes()[0] == 127);
        }

        /* Not an IP at all (localhost / hostname / "*") — treat as exposed; the store bind degrades to
           loopback because it cannot IPAddress.Parse it. */
        return true;
    }

    /// <summary>
    /// Normalizes <c>postgres.network.role</c> to the pg_hba login role: absent/blank ⇒ the secure
    /// default <c>viewer</c> (read-only remote, D7); <c>viewer</c>/<c>admin</c> pass through
    /// (case-insensitively); anything else ⇒ null (invalid — the store degrades to loopback). NEVER
    /// returns <c>darling</c> (the superuser/owner is service-only).
    ///
    /// <para>Kept for the callers that ask a yes/no question about the exposure — "is this store reachable
    /// as admin" (see <c>DarlingWorker</c>'s startup warning). Where BOTH roles are admitted it answers
    /// <c>admin</c>, because the question those callers are really asking is whether write-capable access
    /// is reachable from the network, and it is.</para>
    /// </summary>
    public static string? NormalizeNetworkRole(string? role)
    {
        var roles = NormalizeNetworkRoles(role);

        /* Count == 0 cannot happen today — the plural returns null or a non-empty list — but this is the one
           caller that would INDEX the result, and a future "return the ones we understood" would turn that
           invariant into an IndexOutOfRangeException at service startup rather than a degrade. Checked here
           because every other caller already checks it. */
        if (roles is null || roles.Count == 0)
        {
            return null;
        }

        return roles.Contains("admin", StringComparer.Ordinal) ? "admin" : roles[0];
    }

    /// <summary>
    /// Every pg_hba login role <c>postgres.network.role</c> admits, in a stable order (#2665). One rule is
    /// written per role, so a team can reach the same store with an <c>admin</c> Viewer and a set of
    /// read-only <c>viewer</c> ones — which the roles have always supported and the single generated
    /// <c>hostssl</c> line did not.
    ///
    /// <para>Accepts <c>"admin"</c>, <c>"viewer"</c> and <c>"sources"</c> (#5654: every external alert source login, through
    /// the <c>darling_alert_sources</c> group), alone or several in one string separated by a comma, a plus or
    /// whitespace (<c>"admin,viewer"</c>, <c>"viewer+sources"</c>). <c>sources</c> is a token, not a role name: the pg_hba
    /// line writes it as <c>+darling_alert_sources</c> (<c>DarlingManagedPostgres.PgHbaRoleField</c>). A JSON array would be the tidier surface
    /// and is not worth a breaking change to a field that has shipped: everything already written stays
    /// valid, and a list is expressible without it.</para>
    ///
    /// <para><b>Absent/blank still means <c>viewer</c> alone.</b> A field learning to take a list must not
    /// become a way for an existing configuration to quietly gain admin-capable network access.</para>
    ///
    /// <para>Returns null when ANY element is unrecognised, rather than silently keeping the ones it
    /// understood — the store then degrades to loopback with the reason named. A typo in one of two roles
    /// should not open the store with the other and leave somebody believing both are reachable.</para>
    /// </summary>
    public static IReadOnlyList<string>? NormalizeNetworkRoles(string? role)
    {
        /* Literals rather than DarlingManagedPostgres.ViewerRoleName/AdminRoleName: originally forced
           (the type carried [SupportedOSPlatform("windows")] until #3241 moved the attribute onto its
           Windows-only members, so referencing the consts raised CA1416 here), kept so this classifier
           stays decoupled from the managed store. The names mirror those consts (pinned equal by test). */
        if (string.IsNullOrWhiteSpace(role))
        {
            return new[] { "viewer" };
        }

        var parts = role.Split(
            new[] { ',', '+', ' ', '\t', '\r', '\n' },
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        /* Separators and nothing else (",", "+") is NOT the blank case: the operator wrote a value and none
           of it names a role, so it takes the same fail-closed path as a typo rather than being read as the
           viewer default. Guessing here would expose a store off the back of a malformed field. */
        if (parts.Length == 0)
        {
            return null;
        }

        var seen = new List<string>();

        foreach (var part in parts)
        {
            var normalized = part.ToLowerInvariant();

            if (normalized is not ("viewer" or "admin" or "sources"))
            {
                return null;
            }

            if (!seen.Contains(normalized, StringComparer.Ordinal))
            {
                seen.Add(normalized);
            }
        }

        /* Stable order regardless of how it was written, so the generated pg_hba block is identical for
           "admin,viewer" and "viewer,admin" — otherwise the reconciler rewrites the file and reloads the
           server every time somebody reorders the field. Ordinal order: admin, sources, viewer. */
        seen.Sort(StringComparer.Ordinal);
        return seen;
    }
}

/// <summary>Teams/Slack incoming-webhook alert delivery; a channel is enabled by a non-empty URL.</summary>
public sealed class WebhooksConfig
{
    [JsonPropertyName("teamsUrl")]
    public string TeamsUrl { get; set; } = "";

    [JsonPropertyName("teamsProxy")]
    public string TeamsProxy { get; set; } = "";

    [JsonPropertyName("slackUrl")]
    public string SlackUrl { get; set; } = "";

    [JsonPropertyName("slackProxy")]
    public string SlackProxy { get; set; } = "";

    /* Generic webhook (#1506): POSTs an operator-authored JSON body to any endpoint, so an alert can drive
       automation we ship no adapter for (PagerDuty, Opsgenie, n8n, a GitHub repository_dispatch that re-runs
       a workflow). Enabled by a non-empty URL, like the sibling channels. */

    [JsonPropertyName("genericUrl")]
    public string GenericUrl { get; set; } = "";

    /// <summary>
    /// A JSON object of request headers, e.g. <c>{"Authorization":"Bearer ghp_..."}</c>. A bearer secret —
    /// carved out of the read-only viewer role's column grants alongside <c>generic_url</c>
    /// (<c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>).
    /// </summary>
    [JsonPropertyName("genericHeaders")]
    public string GenericHeaders { get; set; } = "";

    /// <summary>The JSON body, with <c>{{metric}}</c>/<c>{{server}}</c>/… placeholders; empty uses the shared default.
    /// <c>{{investigation_json}}</c> (#5821) is the investigation request the alert opened or joined, empty when it opened none.
    /// <c>{{status}}</c> (#6718) is <c>firing</c> on a fire and <c>resolved</c> on a resolution send.</summary>
    [JsonPropertyName("genericBodyTemplate")]
    public string GenericBodyTemplate { get; set; } = "";

    [JsonPropertyName("genericProxy")]
    public string GenericProxy { get; set; } = "";

    /// <summary>
    /// The generic URL's signing keys (#5998, V193 <c>signing_key</c>, <c>previous_signing_key</c> and
    /// <c>previous_signing_key_until</c>): sealed as the store holds them, opened by <see cref="DarlingWebhookSecrets.Open"/>.
    /// Store only: Darling generates the key, so darling.json never carries one and this is never written to JSON.
    /// </summary>
    [JsonIgnore]
    public WebhookSigningKeys? GenericSigning { get; set; }

    /* PagerDuty webhook — Events API v2. The routing key is a bearer-secret-like opaque token (comparable
       to the Teams/Slack/Generic webhook URLs), so it is stored as a plaintext column in Postgres with a
       column-level REVOKE from the read-only viewer role (DarlingManagedRoles.ViewerRestrictedConfigTables).
       Enabled by a non-empty routing key, like the sibling channels. */

    [JsonPropertyName("pagerDutyRoutingKey")]
    public string PagerDutyRoutingKey { get; set; } = "";

    [JsonPropertyName("pagerDutyUseEuRegion")]
    public bool PagerDutyUseEuRegion { get; set; } = false;

    [JsonPropertyName("pagerDutyProxy")]
    public string PagerDutyProxy { get; set; } = "";

    /// <summary>
    /// Opt-in (V166, <c>config_notification.pagerduty_auto_resolve</c>): the closing edge of an edge-type
    /// alert pair is delivered to PagerDuty as a <c>resolve</c> event that CLOSES the incident its firing
    /// edge's trigger opened, instead of the shipped info-severity trigger that leaves it open. Stored on
    /// the singleton notification row (authoritative in the store; the Viewer's Settings window authors it),
    /// so a store reload applies the operator's choice live like the sibling knobs on this row.
    /// </summary>
    [JsonPropertyName("pagerDutyAutoResolve")]
    public bool PagerDutyAutoResolve { get; set; } = false;

    /* V187 (#5825): send an alert's resolution to the channels its fire reached, one opt-in per channel, all off by
       default. Stored on the singleton notification row beside the destinations they govern. A route can override
       each for its own destinations; NotificationRouteResolutions holds that rule. */

    /// <summary>Send resolutions to Slack (<c>config_notification.slack_send_resolutions</c>).</summary>
    [JsonPropertyName("slackSendResolutions")]
    public bool SlackSendResolutions { get; set; } = false;

    /// <summary>Send resolutions to Teams (<c>config_notification.teams_send_resolutions</c>).</summary>
    [JsonPropertyName("teamsSendResolutions")]
    public bool TeamsSendResolutions { get; set; } = false;

    /// <summary>Send resolutions to the generic webhook (<c>config_notification.generic_send_resolutions</c>).</summary>
    [JsonPropertyName("genericSendResolutions")]
    public bool GenericSendResolutions { get; set; } = false;
}

public sealed class MonitoredServer
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    /// <summary>
    /// Which database engine this target runs: <c>"sqlserver"</c> (default) or <c>"postgres"</c>
    /// (accepted spellings: postgres, postgresql, pg, aurora-postgresql).
    /// <para>This is configuration rather than something probed, because it has to be known BEFORE
    /// connecting — it decides which driver builds the connection string and which detection query
    /// runs. An omitted or unrecognized value means SQL Server, so every existing darling.json keeps
    /// its exact present behaviour.</para>
    /// </summary>
    [JsonPropertyName("engine")]
    public string Engine { get; set; } = "sqlserver";

    [JsonPropertyName("host")]
    public string Host { get; set; } = "";

    /// <summary>
    /// TCP port, for PostgreSQL targets on a non-default port. <c>0</c> (the default) means "use the
    /// driver's default", which is 5432.
    /// <para>Unused for SQL Server, which carries a non-default port in the host itself as
    /// <c>host,1433</c> — that convention is left alone rather than migrated. A SQL Server entry with a non-zero
    /// <c>port</c> still starts, but <c>--validate-config</c> and the service log warn that it is ignored (#6583), and
    /// <c>add_servers</c> and <c>edit_server</c> refuse it.</para>
    /// </summary>
    [JsonPropertyName("port")]
    public int Port { get; set; }

    /// <summary>Azure SQL Database: the one database this entry monitors (feeds the storage-name identity).</summary>
    [JsonPropertyName("database")]
    public string? Database { get; set; }

    /// <summary>"integrated" (default, recommended for a service account) or "sql".</summary>
    [JsonPropertyName("auth")]
    public string Auth { get; set; } = "integrated";

    [JsonPropertyName("username")]
    public string? Username { get; set; }

    /// <summary>DPAPI-LocalMachine-protected password, base64 — produced by --encrypt-password.</summary>
    [JsonPropertyName("encryptedPassword")]
    public string? EncryptedPassword { get; set; }

    /// <summary>Plaintext password — dev convenience only; the service logs a warning when used.</summary>
    [JsonPropertyName("password")]
    public string? Password { get; set; }

    [JsonPropertyName("readOnlyIntent")]
    public bool ReadOnlyIntent { get; set; }

    [JsonPropertyName("trustServerCertificate")]
    public bool TrustServerCertificate { get; set; }

    /// <summary>Mandatory (default) | Strict | Optional — unknown values fail closed to Mandatory.</summary>
    [JsonPropertyName("encryptMode")]
    public string EncryptMode { get; set; } = "Mandatory";

    [JsonPropertyName("multiSubnetFailover")]
    public bool MultiSubnetFailover { get; set; }

    /// <summary>
    /// #6254: where Query Store is read from instead of this server, a host (<c>host,port</c> allowed) or an availability group
    /// listener name. Empty, the default, reads it from the server itself. SQL Server only; the replica connection is
    /// <see cref="MonitoredServerConnection.BuildQueryStoreReplicaConnectionString"/>. It is
    /// the last field of <see cref="ConnectionIdentity"/>: a change of it is a change of how the server is reached, and it is bound
    /// into a sealed password (<see cref="SecretBinding"/>) only when it is set, so a change of it needs the password again and no
    /// stored password is sent to a replica host nobody entered it for.
    /// </summary>
    [JsonPropertyName("queryStoreReplica")]
    public string? QueryStoreReplica { get; set; }

    [JsonPropertyName("excludedDatabases")]
    public List<string> ExcludedDatabases { get; set; } = new();

    /// <summary>
    /// The server's monthly cost/budget in USD — the twin of Lite's <c>ServerConnection.MonthlyCostUsd</c>.
    /// Pure user-input config (not collected data): every FinOps cost figure is proportional math on this
    /// single number (e.g. a database's share = its size fraction × this budget). 0 (the default) hides the
    /// cost columns/cards in the viewer's FinOps tab, exactly like Lite. Upserted into the servers registry
    /// so the headless viewer can read it.
    /// </summary>
    [JsonPropertyName("monthlyCostUsd")]
    public decimal MonthlyCostUsd { get; set; }

    /// <summary>
    /// Per-server override of the deadlock/blocking alert delivery mode (#1236); null (the default) inherits
    /// the global <see cref="AlertsConfig.DeliveryMode"/>. Forces Summary or Per-event for THIS server only
    /// (e.g. Per-event for one noisy prod box while the fleet default stays Summary). Populated from
    /// <c>config_monitored_servers.alert_delivery_mode_override</c> at read time so the deliverer can resolve
    /// it per fired alert via the shared <c>AlertDeliveryModeResolver</c>; serialized as its name in darling.json.
    /// </summary>
    [JsonPropertyName("alertDeliveryModeOverride")]
    [JsonConverter(typeof(JsonStringEnumConverter))]
    public AlertNotificationMode? AlertDeliveryModeOverride { get; set; }

    /// <summary>
    /// #6771: this server's own plan-capture setting, read from <c>config_monitored_servers.capture_plans</c>. Null means "use the
    /// service-wide setting" (<see cref="DarlingConfig.CapturePlans"/>), true keeps plan XML for this server whatever the service-wide
    /// setting says, false collects this server's statistics without plan XML. Store-only: darling.json has no per-server counterpart,
    /// so it is not read from or written to the file.
    /// </summary>
    [JsonIgnore]
    public bool? CapturePlans { get; set; }

    [JsonIgnore]
    public bool UsesSqlAuth => string.Equals(Auth, "sql", StringComparison.OrdinalIgnoreCase);

    /// <summary>Microsoft Entra service principal (application/client id + client secret) — non-interactive,
    /// so it fits a headless collector. The client id is carried in <see cref="Username"/> and the secret in
    /// <see cref="EncryptedPassword"/> (same DPAPI shape as a SQL password), so it needs no new config field.
    /// #3484.</summary>
    [JsonIgnore]
    public bool UsesServicePrincipal => string.Equals(Auth, "serviceprincipal", StringComparison.OrdinalIgnoreCase);

    /// <summary>Azure managed identity (system- or user-assigned) — non-interactive and secret-less. A
    /// user-assigned identity names its client id in <see cref="Username"/>; a system-assigned one leaves it
    /// blank. #3484.</summary>
    [JsonIgnore]
    public bool UsesManagedIdentity => string.Equals(Auth, "managedidentity", StringComparison.OrdinalIgnoreCase);

    /// <summary>Either of the two non-interactive Microsoft Entra modes. The INTERACTIVE Entra modes (MFA,
    /// device-code, default-credential) are deliberately not supported for an unattended service — they need a
    /// broker or a signed-in user; Lite offers them, the headless collector does not.</summary>
    [JsonIgnore]
    public bool UsesEntra => UsesServicePrincipal || UsesManagedIdentity;

    /// <summary>True when the connect path must have a resolved secret in hand. SQL auth (password) and
    /// service principal (client secret) both carry one in <see cref="EncryptedPassword"/>/<see cref="Password"/>;
    /// integrated and managed identity carry none.</summary>
    [JsonIgnore]
    public bool RequiresResolvedSecret => UsesSqlAuth || UsesServicePrincipal;

    /// <summary>
    /// <see cref="Engine"/> parsed. Anything unrecognized resolves to
    /// <see cref="CollectorTargetEngine.SqlServer"/> rather than throwing: a typo in one server entry
    /// must not stop the service from starting and monitoring everything else. The mismatch surfaces
    /// immediately anyway — the SQL Server detection query fails against a Postgres target.
    /// </summary>
    [JsonIgnore]
    public CollectorTargetEngine TargetEngine => MonitoredEngineKind.ParseConfiguredEngine(Engine);

    /// <summary>True when this entry targets PostgreSQL, so the SQL Server-only config is inapplicable.</summary>
    [JsonIgnore]
    public bool IsPostgres => TargetEngine == CollectorTargetEngine.PostgreSql;

    /// <summary>Display name falls back to the host.</summary>
    [JsonIgnore]
    public string DisplayName => string.IsNullOrWhiteSpace(Name) ? Host : Name;

    /// <summary>
    /// The canonical storage identity (<c>host[:database][:pg][:port][:RO]</c>) — hashed to server_id via the
    /// shared ServerIdHelper, so this Darling entry derives the same id Lite would for the same server.
    ///
    /// <para>#2218: engine and port are passed so a PostgreSQL instance cannot collide with a SQL Server on the
    /// same host, and two PostgreSQL instances on one host cannot collide with each other. Both are inert for a
    /// SQL Server entry and the resulting name is byte-identical to what it was before — <c>Engine</c> folds to
    /// no token for SQL Server, and <c>Port</c> is a PostgreSQL-only field that stays 0 there, because SQL
    /// Server carries a non-default port inside <c>Host</c> as <c>host,1433</c> and is therefore already
    /// discriminated by the host string. That is why no conditional is needed here: the defaults ARE the
    /// backwards-compatible case.</para>
    /// </summary>
    [JsonIgnore]
    public string StorageName => PerformanceMonitor.Common.ServerIdHelper.BuildStorageName(
        Host, Database, ReadOnlyIntent, Engine, Port);

    /// <summary>
    /// <c>config_monitored_servers.server_id</c> as READ FROM THE STORE, or null for an entry that has no
    /// store row yet — a <c>darling.json</c> bootstrap entry before the first seed.
    ///
    /// <para><b>Not settable from the file</b> (<see cref="JsonIgnore"/>) on purpose. The registry is
    /// authoritative for identity once seeded, so letting an operator pin a <c>server_id</c> in
    /// <c>darling.json</c> would create a second authority that could disagree with it — and disagree
    /// silently, since nothing downstream re-checks.</para>
    /// </summary>
    [JsonIgnore]
    public int? StoredServerId { get; set; }

    /// <summary>
    /// <c>config_monitored_servers.plan_force_bot_enabled</c> as READ FROM THE STORE — write-gate 2
    /// of the #2138 two-gate contract. A live force on this server requires the global
    /// <c>forcePlanBot</c> gates AND this flag.
    ///
    /// <para><b>Not settable from the file</b> (<see cref="JsonIgnore"/>) on purpose, the
    /// <see cref="StoredServerId"/> reasoning applied to a WRITE authorization: the registry is
    /// authoritative once seeded, so a darling.json knob would be a silent no-op on every seeded box
    /// (#2254) — and a knob that silently does nothing is worst of all when what it claims to gate is
    /// a write to a production server. The seed always writes FALSE; opting a server in is a store
    /// write (viewer surface to follow).</para>
    /// </summary>
    [JsonIgnore]
    public bool PlanForceBotEnabled { get; set; }

    /// <summary>
    /// The REMEDIATION credential's login name (<c>config_monitored_servers.remediation_username</c>) — the
    /// second, per-server, opt-in identity a #2138 phase-1 action runs as.
    ///
    /// <para><b>The monitoring credential is never used for a write, ever.</b> That promise is stated in
    /// both READMEs and in the MCP instructions, and operators grant against it, so the write travels on
    /// its own identity or it does not travel. There is no fallback: null here means this server has no
    /// phase-1 surface at all, which is the whole arming model — see
    /// <see cref="PerformanceMonitor.Analysis.OperatorRemediationGate.SurfaceFor"/> for why the absence is
    /// a null credential rather than an <c>enabled</c> flag.</para>
    ///
    /// <para><b>Presence IS the auth mode.</b> Deliberately no <c>remediationAuth</c> sibling: an
    /// integrated remediation identity would be the service account, which is the monitoring identity,
    /// which is exactly what this exists to keep read-only. So a remediation credential is always SQL auth
    /// when set, and there is no third state to resolve wrongly.</para>
    ///
    /// <para><b>Settable from the file</b>, unlike <see cref="PlanForceBotEnabled"/> — and the difference is
    /// deliberate. That flag is an ARM STATE, so a file knob would be a silent no-op on a seeded box
    /// (#2254). This is a CREDENTIAL, and the container/compose deploy has no viewer to type one into; the
    /// <c>env:</c>/<c>file:</c> reference path is the only way to arm a Linux install at all. Unlike every other
    /// per-server field here it is NOT only a seed value (#6262): when BOTH keys are present,
    /// <c>StoreConfigProvider.SyncRemediationLoginsAsync</c> sets or replaces the registered server's login on every start, only on the
    /// server that pairs with this entry and connects the same way. Absent keys never clear it, and one key alone changes nothing (it is
    /// logged): removing the keys disarms nothing. To stop agent actions on a server, turn off its action switches, or turn off agent
    /// changes service-wide; the switches cover agent actions only, not operator remediation from the Viewer. To remove a server's
    /// remediation login entirely, so neither agents nor operators can use it, take both keys out of the file and run, as the store owner,
    /// <c>UPDATE config_monitored_servers SET remediation_username = NULL, remediation_encrypted_password = NULL,
    /// modified_at = now() AT TIME ZONE 'UTC' WHERE name = 'alpha-sql-01'</c>. Services that share one
    /// store should declare the same logins, because each copies its own file on start.</para>
    /// </summary>
    [JsonPropertyName("remediationUsername")]
    public string? RemediationUsername { get; set; }

    /// <summary>
    /// The remediation credential's secret: an <c>env:</c>/<c>file:</c> reference (resolved by the same
    /// <c>DarlingSecretSource</c>; a Level 3 server action, #5632, refuses any other form as <c>credential_not_reference</c>) or a DPAPI-LocalMachine blob, base64, produced by the same
    /// <c>--encrypt-password</c> as <see cref="EncryptedPassword"/>. Only a reference works for every action (#6262), and a plain password (none of a reference, a sealed value or a DPAPI blob) is not copied into the store:
    /// a blob opens for operator remediation (#2138), but the agent server actions (plan force, plan eviction, session
    /// kill) accept only a reference and <c>--validate-config</c> and the service warn about a blob. There is deliberately no
    /// plaintext sibling of this one (no counterpart to <see cref="Password"/>): the dev-convenience
    /// plaintext slot exists because a wrong monitoring password fails a read, and a wrong remediation
    /// password fails a write to a production server.
    /// </summary>
    [JsonPropertyName("remediationEncryptedPassword")]
    public string? RemediationEncryptedPassword { get; set; }

    /// <summary>
    /// The login the PostgreSQL hypothetical-index check runs under (<c>config_monitored_servers.hypothetical_index_username</c>, #6170),
    /// a third identity beside the monitoring login and the remediation login, set up the way the remediation login is.
    ///
    /// <para>The check plans statements taken from <c>pg_stat_statements</c>, and planning can evaluate a function a
    /// database role created when its arguments are constants, under the roles of the login that plans. So the check never runs under the
    /// monitoring login, which usually holds <c>pg_monitor</c> or <c>pg_read_all_stats</c>. It runs under this login, which needs only
    /// CONNECT, USAGE on the schemas and SELECT on the tables of the statements to test. Null here means the check is refused on this
    /// server with a message that says what to set up. There is no fallback.</para>
    ///
    /// <para>Presence is the auth mode: always SQL auth. Read at seed time, and (#6262) when BOTH keys are in the file they are also applied
    /// to the registered server on every start (<c>StoreConfigProvider.SyncHypotheticalIndexLoginsAsync</c>): set, or replaced, only on
    /// the server that pairs with this entry and connects the same way. The file never clears the slot (the same rule as the remediation
    /// login), because an owner's UPDATE on the store row is a documented way to set it too; only an <c>env:</c> or <c>file:</c>
    /// reference is written.</para>
    /// </summary>
    [JsonPropertyName("hypotheticalIndexUsername")]
    public string? HypotheticalIndexUsername { get; set; }

    /// <summary>
    /// The password of <see cref="HypotheticalIndexUsername"/> as an <c>env:NAME</c> or <c>file:path</c> reference, read on the Darling
    /// host when the check runs. Only a reference is accepted: a stored password is refused, as it is for the remediation login.
    /// </summary>
    [JsonPropertyName("hypotheticalIndexEncryptedPassword")]
    public string? HypotheticalIndexEncryptedPassword { get; set; }

    /// <summary>
    /// The AWS IAM role Darling assumes to reach this Amazon RDS or Aurora target (#5452), as a role ARN such as
    /// <c>arn:aws:iam::123456789012:role/darling-monitor</c>. Unset (the default) means the process's own AWS
    /// credentials, exactly as before. PostgreSQL targets only.
    ///
    /// <para><b>darling.json seeds only a NEW row.</b> The store is authoritative once a server is registered
    /// (the seed is <c>ON CONFLICT DO NOTHING</c>, <c>StoreConfigProvider</c>), so changing this in the file
    /// does not move a server that is already stored: edit it from the web, the MCP tool or the desktop
    /// viewer.</para>
    /// </summary>
    [JsonPropertyName("awsRoleArn")]
    public string? AwsRoleArn { get; set; }

    /// <summary>
    /// The external ID sent with the AssumeRole call for <see cref="AwsRoleArn"/> (#5452), when the role's trust policy
    /// asks for one. Needs a role. Seeds a new row only, like the role.
    /// </summary>
    [JsonPropertyName("awsExternalId")]
    public string? AwsExternalId { get; set; }

    /// <summary>
    /// The role and external ID as one value, trimmed, or null when no role is set (an external ID with no role is
    /// never used). Two servers with the same key share one assumed-role session.
    /// </summary>
    [JsonIgnore]
    public Targets.AwsRoleKey? AwsRoleKey
    {
        get
        {
            var role = AwsRoleSettings.Normalize(AwsRoleArn);
            return role is null ? null : new Targets.AwsRoleKey(role, AwsRoleSettings.Normalize(AwsExternalId));
        }
    }

    /// <summary>
    /// Whether this server is armed for operator-initiated remediation: BOTH halves of the credential are
    /// present. A one-sided credential is not a weaker arm, it is a misconfiguration — so it reads as
    /// unarmed rather than as something to attempt and fail at against a production server.
    /// </summary>
    [JsonIgnore]
    public bool HasRemediationCredential =>
        !string.IsNullOrWhiteSpace(RemediationUsername) &&
        !string.IsNullOrWhiteSpace(RemediationEncryptedPassword);

    /// <summary>
    /// Whether darling.json itself declares <see cref="EncryptedPassword"/> as it stands: the same <c>env:</c>/<c>file:</c>
    /// reference or old-format (DPAPI) value, in this same slot, for this same server id (#5240, #5366). It is the one thing
    /// that lets such a reference resolve when it names one of the service's own configuration files or secrets
    /// (<see cref="DarlingSecrets.ResolvePassword(MonitoredServer, out bool, IPasswordKeyRing?)"/>), and lets an old-format
    /// value open without a pin; every other owned reference is refused where it resolves, and an old-format value with no
    /// pin is not opened. A sealed value is never marked: it opens through the key ring only.
    ///
    /// <para>Set in two places, and never from a stored value. <see cref="DarlingConfig.Parse"/> sets it on an entry read from
    /// the file, since the file declares what it holds. <c>StoreConfigProvider.BuildServerFromRow</c> sets it on a server
    /// built from a store row only when a file entry with the same server id declares the same text in this slot, so a store
    /// row that names another server's reference, or whose slot was changed, is not marked. <see cref="JsonIgnore"/>:
    /// the file cannot set it, and it is not stored.</para>
    /// </summary>
    [JsonIgnore]
    internal bool EncryptedPasswordDeclaredByFile { get; set; }

    /// <summary><see cref="EncryptedPasswordDeclaredByFile"/> for <see cref="RemediationEncryptedPassword"/>.</summary>
    [JsonIgnore]
    internal bool RemediationEncryptedPasswordDeclaredByFile { get; set; }

    /// <summary><see cref="EncryptedPasswordDeclaredByFile"/> for <see cref="HypotheticalIndexEncryptedPassword"/> (#6170).</summary>
    [JsonIgnore]
    internal bool HypotheticalIndexEncryptedPasswordDeclaredByFile { get; set; }

    /// <summary>
    /// The connection as the store row holds it, read once by <see cref="ServerConnectionIdentity.FromStoredColumns"/> from the raw
    /// columns (null text and a null port are not defaulted any further). Null for an entry read from darling.json, whose
    /// own properties are the connection (#5366).
    /// </summary>
    [JsonIgnore]
    internal ServerConnectionIdentity? StoredIdentity { get; set; }

    /// <summary>The pin taken at upgrade for the <c>encryptedPassword</c> slot, when the store holds one (#5366).</summary>
    [JsonIgnore]
    internal LegacyPin? SecretPin { get; set; }

    /// <summary>The pin taken at upgrade for the remediation slot, when the store holds one (#5366).</summary>
    [JsonIgnore]
    internal LegacyPin? RemediationPin { get; set; }

    /// <summary>
    /// The connection settings this server's saved password is bound to (#5366). Throws <see cref="ArgumentException"/> when a
    /// field is not valid text; the resolver treats that as the saved password not matching this connection.
    /// </summary>
    [JsonIgnore]
    internal ServerConnectionIdentity ConnectionIdentity => StoredIdentity ?? ServerConnectionIdentity.FromStoredColumns(
        Host, Port, Engine, Database, ReadOnlyIntent, Auth, Username, EncryptMode, TrustServerCertificate, MultiSubnetFailover,
        QueryStoreReplica);

    /// <summary>The binding a sealed <c>encryptedPassword</c> must have been sealed for.</summary>
    [JsonIgnore]
    internal PasswordBinding SecretBinding => PasswordBinding.ForServer(ConnectionIdentity);

    /// <summary>The binding a sealed remediation password must have been sealed for.</summary>
    [JsonIgnore]
    internal PasswordBinding RemediationBinding => PasswordBinding.ForRemediation(ConnectionIdentity, RemediationUsername);

    /// <summary>
    /// This server's <c>server_id</c>: the stored value when there is one, otherwise derived from
    /// <see cref="StorageName"/>.
    ///
    /// <para><b>This is the single place a monitored server's identity is decided</b> (#2218, #2158). It used
    /// to be recomputed at twelve call sites — every operator-command lookup, the reconcile, the self-alert
    /// stamps, the schedule resolution — which is what makes identity-derived-from-mutable-config expensive
    /// to change: a stored surrogate is only useful if nothing re-derives it behind the store's back.</para>
    ///
    /// <para><b>Today the two are always equal</b>, because the seed and the Viewer both write exactly this
    /// hash, so reading the stored value changes no behaviour and no data moves. The point is that the
    /// FALLBACK is now the only derivation: when identity stops being derivable, this property is what
    /// changes, and the twelve call sites do not.</para>
    ///
    /// <para>The store is preferred over the derivation rather than merely agreeing with it, because that is
    /// the ordering that makes a stored id which no longer matches its host keep working — which is the
    /// whole point of storing it.</para>
    /// </summary>
    [JsonIgnore]
    public int ServerId =>
        StoredServerId ?? PerformanceMonitor.Common.ServerIdHelper.GetDeterministicHashCode(StorageName);
}

/// <summary>
/// Declared peer stores (#2339, tier 1) — the <c>peers</c> block. A fleet split across several boxes gives
/// every box a Darling store that knows only its own slice, so an agent asking the wrong endpoint about a
/// server gets a not-found that is indistinguishable from "nobody monitors it". Declaring the siblings here
/// makes the split legible in the MCP instructions, in <c>list_servers</c>, and in the server-resolution
/// miss message.
///
/// <para><b>Disclosure only — there are no credentials in this block and there is no connectivity behind
/// it.</b> A peer is a NAME and a sentence; this service never contacts one, cannot read one's data, and
/// cannot tell whether one is even running. Deliberately so: cross-store reads (auth between stores,
/// latency, partial failures) are a much larger surface and may never be worth building if disclosure alone
/// makes the split legible. <see cref="Validate"/> therefore REFUSES text that looks like a connection
/// string or credential, because everything here is sent verbatim to every connected MCP client.</para>
///
/// <para>A file-only block (not seeded into the control-plane store): it describes the DEPLOYMENT TOPOLOGY of
/// the box this config sits on, which is exactly the kind of thing that must not be editable from a peer's
/// Viewer. An edit takes effect on the next service restart.</para>
/// </summary>
public sealed class PeersConfig
{
    /// <summary>
    /// One sentence naming what THIS store monitors — the anchor everything else is relative to
    /// ("the 42 us-east-1 SQL Server primaries"). Optional; omit it and the peer list is still disclosed.
    /// </summary>
    [JsonPropertyName("thisStoreCovers")]
    public string ThisStoreCovers { get; set; } = "";

    /// <summary>
    /// A short LABEL naming this store itself (#3500) — the box name, "the use1 store" — where
    /// <see cref="ThisStoreCovers"/> stays the sentence it is. When set, every fleet-level store self-alert
    /// (Store Disk Pressure, Retention Held, the cost digest, the sweep rollup, all twelve families and their
    /// resolution edges) fires under this label instead of the constant "Monitor Store", so on a multi-store
    /// estate two stores' identical self-alerts stop being indistinguishable: the Teams card names the host,
    /// and the delivery fingerprint downstream automation dedups on becomes distinct per store instead of
    /// colliding two hosts into one work item.
    ///
    /// <para><b>OPT-IN, and setting it IS accepting a re-key.</b> The label is part of the self-alerts'
    /// delivery fingerprint, so changing it orphans every open downstream work item keyed on the old one and
    /// starts fresh ones beside them. Unset (the default), every self-alert is byte-identical to before this
    /// field existed. The reporter's suggested machine-hostname fallback is deliberately NOT taken for
    /// exactly that reason: it would re-key every existing install's self-alert fingerprints on upgrade,
    /// silently — the precise harm the opt-in exists to prevent. Only setting this field opts in.</para>
    ///
    /// <para><b>Mute rules match the alert row's spelling.</b> A rule scoped to server "Monitor Store" stops
    /// matching this store's self-alerts the moment this is set; re-scope such rules to the label (copy the
    /// spelling off the new alert rows, the same advice the mute tools already give). And a label that
    /// equals a monitored server's display name is NOT policed — the registry is store-authoritative after
    /// seeding, so config validation cannot see it — but it would make the two indistinguishable on every
    /// shared surface, including the triage page, which treats the label as fleet-level. Do not reuse one.</para>
    ///
    /// <para>File-only like its siblings (deployment identity, not a peer's Viewer's to edit); trimmed;
    /// blank/whitespace means unset; an edit takes effect on the next service restart.</para>
    /// </summary>
    [JsonPropertyName("storeName")]
    public string StoreName { get; set; } = "";

    /// <summary>The sibling Darling stores. Empty (the default) = nothing declared, and every surface behaves as before.</summary>
    [JsonPropertyName("stores")]
    public List<PeerStoreConfig> Stores { get; set; } = new();

    /* Substrings that mean the operator pasted a credential or a whole connection string into a field whose
       entire purpose is to be broadcast. Matched case-insensitively against every peer string. Deliberately
       a short, high-signal list rather than a secret detector: it catches the realistic mistake (copying a
       peer's connectionString in as its description) without pretending to be a scanner. */
    private static readonly string[] CredentialShapedTokens =
    {
        "password=", "pwd=", "connectionstring", "integrated security=", "accountkey=", "secretaccesskey",
    };

    /// <summary>
    /// Validates a peers block; returns human-readable problems (empty = valid). Fatal, like the rest of
    /// <see cref="DarlingConfig.Validate"/>, for exactly two shapes: a peer that cannot be NAMED (an agent
    /// told "some other store has it" with no name to point a human at is no better off than before), and
    /// any peer text that looks like it carries a secret (failing open there would broadcast it).
    /// A peer with a name but no <c>covers</c> sentence is allowed — half a disclosure still names an
    /// endpoint — and renders as just the name.
    /// </summary>
    public static IReadOnlyList<string> Validate(PeersConfig? peers)
    {
        var problems = new List<string>();
        if (peers is null)
        {
            return problems;
        }

        /* thisStoreCovers is checked FIRST, before anything can short-circuit. It used to live after the
           per-peer loop behind an early `peers?.Stores is null` return, which meant an explicit
           `"stores": null` in the JSON (System.Text.Json assigns null over the property initializer, unlike
           an omitted key) skipped the credential guard on the one field that is still disclosed in that
           config — instructions, list_servers' this_store_covers, and every resolution miss. Caught in
           review on #2339. Ordering, not an extra check, is the fix: an unconditional guard cannot be
           bypassed by a shape nobody thought to enumerate. */
        var selfText = peers.ThisStoreCovers ?? "";
        var selfOffending = CredentialShapedTokens.FirstOrDefault(
            t => selfText.Contains(t, StringComparison.OrdinalIgnoreCase));

        if (selfOffending is not null)
        {
            problems.Add(
                $"peers.thisStoreCovers contains '{selfOffending}'. The peers block is DISCLOSURE ONLY — its text " +
                "is sent verbatim to every connected MCP client — so it must carry no connection string and no " +
                "credential.");
        }

        /* storeName gets the same unconditional guard, in the same pre-early-return position, for a stronger
           version of the same reason: it is not only disclosed to MCP clients, it rides OUT on every
           self-alert delivery channel (#3500) — Teams, Slack, PagerDuty, the generic webhook, email. */
        var storeNameText = peers.StoreName ?? "";
        var storeNameOffending = CredentialShapedTokens.FirstOrDefault(
            t => storeNameText.Contains(t, StringComparison.OrdinalIgnoreCase));

        if (storeNameOffending is not null)
        {
            problems.Add(
                $"peers.storeName contains '{storeNameOffending}'. The store label is sent verbatim on every " +
                "self-alert delivery channel and to every connected MCP client, so it must carry no connection " +
                "string and no credential. Name the box, not how to reach it.");
        }

        if (peers.Stores is null)
        {
            return problems;
        }

        for (int i = 0; i < peers.Stores.Count; i++)
        {
            var peer = peers.Stores[i];
            if (peer is null)
            {
                continue;
            }

            var label = string.IsNullOrWhiteSpace(peer.Name) ? $"peers.stores[{i}]" : $"peer '{peer.Name.Trim()}'";

            if (string.IsNullOrWhiteSpace(peer.Name))
            {
                problems.Add(
                    $"{label}: name is required — it is what an agent tells its human to point at, so a peer " +
                    "with only a description cannot be acted on.");
            }

            foreach (var value in PeerStrings(peer))
            {
                var offending = CredentialShapedTokens.FirstOrDefault(
                    t => value.Contains(t, StringComparison.OrdinalIgnoreCase));

                if (offending is not null)
                {
                    /* #4316 round 1 (M3): index, never label — label can BE the offending text (peer.Name is
                       exactly the field this check inspects), and a config-validation problem list is the kind
                       of thing that ends up pasted into a support ticket or a log. */
                    problems.Add(
                        $"peers.stores[{i}]: peer text contains '{offending}'. The peers block is DISCLOSURE ONLY — " +
                        "its text is sent verbatim to every connected MCP client — so it must carry no connection " +
                        "string and no credential. Describe what the peer monitors, not how to reach it.");
                    break;
                }
            }
        }

        return problems;
    }

    /// <summary>The credential-shaped substring <paramref name="text"/> carries, or null. The front door's store names use the
    /// same list, because a store name goes into a header and a page address.</summary>
    internal static string? FindCredentialShapedToken(string text)
        => CredentialShapedTokens.FirstOrDefault(t => text.Contains(t, StringComparison.OrdinalIgnoreCase));

    private static IEnumerable<string> PeerStrings(PeerStoreConfig peer)
    {
        yield return peer.Name ?? "";
        yield return peer.Covers ?? "";

        foreach (var match in peer.Matches ?? new List<string>())
        {
            yield return match ?? "";
        }
    }
}

/// <summary>
/// One declared peer store: what to call it, what it covers in prose, and optionally which server names it
/// owns. No address and no credential by design — see <see cref="PeersConfig"/>.
/// </summary>
public sealed class PeerStoreConfig
{
    /// <summary>
    /// What to call this store — whatever an operator would recognize (the box name, "the use1 store").
    /// Required: it is the actionable half of the disclosure.
    /// </summary>
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    /// <summary>
    /// A short sentence naming what that store monitors ("the readable replicas of the use1 primaries, from
    /// us-east-2"). Human prose — never parsed, only shown.
    /// </summary>
    [JsonPropertyName("covers")]
    public string Covers { get; set; } = "";

    /// <summary>
    /// Optional server-name substrings this peer declares it monitors (<c>"use1"</c>, <c>"-replica"</c>),
    /// matched case-insensitively. This is the ONLY machine-checked field: it is what lets a resolution miss
    /// name the specific peer that owns the server instead of listing them all. Blank entries are dropped —
    /// an empty substring matches every name, which would make one peer claim the whole fleet. No globbing
    /// and no regex on purpose: a pattern language is a config surface with its own failure modes, and a
    /// substring answers the question actually being asked (which region/role does this name belong to?).
    /// A peer that declares none is still disclosed everywhere; it just cannot be singled out on a miss.
    /// </summary>
    [JsonPropertyName("matches")]
    public List<string> Matches { get; set; } = new();
}

/// <summary>
/// Read admission (#5506). A read whose route took 2 seconds or more at its window size is a long read; at most
/// <see cref="MaxHeavyReads"/> of them run at once, up to 16 more wait for up to 45 seconds, and past that a read is
/// refused as busy. Short reads never wait. The limit is per service process: the MCP and web hosts share it, and the
/// WPF viewer, which reads the store directly, is not counted. File-only; read when each host starts.
/// </summary>
public sealed class ReadsConfig
{
    /// <summary>How many long reads run at once (default 2). 0 turns the limit off.</summary>
    [JsonPropertyName("maxHeavyReads")]
    public int MaxHeavyReads { get; set; } = ReadAdmission.DefaultMaxHeavyReads;
}

/// <summary>
/// The <c>frontDoor</c> block of darling.json (#6840): this process is a front door that holds no monitoring database of its
/// own and forwards the MCP and web surfaces to the Darling stores listed in <see cref="Stores"/>.
///
/// <para><b>File-only, and its own block.</b> It is not part of <see cref="PeersConfig"/>: that text goes verbatim to every MCP
/// client and its validator refuses secrets, while this block holds every peer's access token. No read surface returns it.
/// Each token is an <c>*EncryptedToken</c> (DPAPI), a plain <c>*Token</c>, or an <c>env:</c>/<c>file:</c> reference, resolved the
/// way <see cref="McpTokenEntry.ResolveToken"/> resolves its own. An edit takes effect on the next service restart.</para>
/// </summary>
public sealed class FrontDoorConfig
{
    /// <summary>Seconds between directory refreshes when <c>refreshSeconds</c> is omitted.</summary>
    public const int DefaultRefreshSeconds = 300;

    /// <summary>The shortest refresh a config may ask for.</summary>
    public const int MinRefreshSeconds = 30;

    /// <summary>The longest refresh a config may ask for.</summary>
    public const int MaxRefreshSeconds = 3600;

    /// <summary>Seconds a fan-out waits for one store when <c>peerTimeoutSeconds</c> is omitted.</summary>
    public const int DefaultPeerTimeoutSeconds = 150;

    /// <summary>The shortest peer timeout a config may ask for.</summary>
    public const int MinPeerTimeoutSeconds = 30;

    /// <summary>The longest peer timeout a config may ask for.</summary>
    public const int MaxPeerTimeoutSeconds = 600;

    /// <summary>Off unless set: an absent block, or <c>"enabled": false</c>, is today's single-store behavior.</summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    /// <summary>Seconds between reads of each store's server list.</summary>
    [JsonPropertyName("refreshSeconds")]
    public int RefreshSeconds { get; set; } = DefaultRefreshSeconds;

    /// <summary>Seconds one forwarded call to a store may take before the front door gives up on it.</summary>
    [JsonPropertyName("peerTimeoutSeconds")]
    public int PeerTimeoutSeconds { get; set; } = DefaultPeerTimeoutSeconds;

    /// <summary>The directory refresh interval a caller uses: <see cref="RefreshSeconds"/> held between the limits. <c>Validate</c> does
    /// not run in the processes that host the front door's directory and MCP router, so a 0 here would refresh without a pause and a
    /// negative value would stop the host (#6840).</summary>
    internal TimeSpan EffectiveRefreshInterval() =>
        TimeSpan.FromSeconds(Math.Clamp(RefreshSeconds, MinRefreshSeconds, MaxRefreshSeconds));

    /// <summary>The per-call deadline a caller uses: <see cref="PeerTimeoutSeconds"/> held between the limits, for the same reason.</summary>
    internal TimeSpan EffectivePeerTimeout() =>
        TimeSpan.FromSeconds(Math.Clamp(PeerTimeoutSeconds, MinPeerTimeoutSeconds, MaxPeerTimeoutSeconds));

    /// <summary>The Darling stores this front door routes to.</summary>
    [JsonPropertyName("stores")]
    public List<FrontDoorStoreConfig> Stores { get; set; } = new();

    /// <summary>The valid store entries as routing records, in file order. An entry that fails validation is left out, so a
    /// caller that has not validated never sees a half-built store.</summary>
    /* The same name twice (names compare ignoring case) is one store, the first entry: this host does not validate the file, Validate
       refuses the duplicate, and a second entry would otherwise break the peer client's name table at start and make two rows share one
       entry's health and server list. */
    internal IReadOnlyList<FrontDoorStore> ToStores()
        => (Stores ?? new()).Select(s => s?.ToStore()).Where(s => s is not null).Select(s => s!)
            .GroupBy(s => s.Name, StringComparer.OrdinalIgnoreCase).Select(g => g.First()).ToList();

    /// <summary>Notes for the log that are not refusals (D3): a plain-http address to a store that is not on this machine sends
    /// that store's token in clear text. Guidance only, like the MCP listener without TLS. Names the store, never an address.</summary>
    public IReadOnlyList<string> Warnings()
    {
        var warnings = new List<string>();
        foreach (var store in Stores ?? new())
        {
            if (store is null || string.IsNullOrWhiteSpace(store.Name))
            {
                continue;
            }

            foreach (var (setting, url) in new[] { ("mcpUrl", store.McpUrl), ("webUrl", store.WebUrl) })
            {
                if (FrontDoorStoreConfig.TryParseUrl(url, out var uri) && uri.Scheme == Uri.UriSchemeHttp && !uri.IsLoopback)
                {
                    warnings.Add($"frontDoor.stores['{store.Name.Trim()}'].{setting} is plain http to a host that is not this machine, " +
                        "so the store's access token crosses the network in clear text. Use https if the store serves it.");
                }
            }
        }

        return warnings;
    }

    /// <summary>
    /// Validates a front-door block; returns human-readable problems (empty = valid). Nothing is checked when the block is off.
    /// A problem never carries a URL's text or a token.
    /// </summary>
    public static IReadOnlyList<string> Validate(FrontDoorConfig? frontDoor)
    {
        var problems = new List<string>();
        if (frontDoor is null || !frontDoor.Enabled)
        {
            return problems;
        }

        if (frontDoor.RefreshSeconds is < MinRefreshSeconds or > MaxRefreshSeconds)
        {
            problems.Add($"frontDoor.refreshSeconds must be between {MinRefreshSeconds} and {MaxRefreshSeconds} (got {frontDoor.RefreshSeconds}).");
        }

        if (frontDoor.PeerTimeoutSeconds is < MinPeerTimeoutSeconds or > MaxPeerTimeoutSeconds)
        {
            problems.Add($"frontDoor.peerTimeoutSeconds must be between {MinPeerTimeoutSeconds} and {MaxPeerTimeoutSeconds} (got {frontDoor.PeerTimeoutSeconds}).");
        }

        if (frontDoor.Stores is not { Count: > 0 })
        {
            problems.Add("frontDoor.stores needs at least one store when frontDoor.enabled is true.");
            return problems;
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < frontDoor.Stores.Count; i++)
        {
            var store = frontDoor.Stores[i];
            if (store is null)
            {
                problems.Add($"frontDoor.stores[{i}] is empty.");
                continue;
            }

            /* The index names the entry, never the name: a name that fails the credential check can BE the offending text. */
            var label = $"frontDoor.stores[{i}]";
            var name = store.Name?.Trim() ?? "";
            if (!FrontDoorStoreConfig.NamePattern.IsMatch(name))
            {
                problems.Add($"{label}: name is required, 1 to 64 characters of A-Z, a-z, 0-9, '.', '_' and '-'. It goes into a request header and a page address.");
            }
            else if (PeersConfig.FindCredentialShapedToken(name) is not null)
            {
                problems.Add($"{label}: name looks like it carries a credential. A store name is shown on every page and sent in a header, so name the store, not how to reach it.");
            }
            else if (!seen.Add(name))
            {
                problems.Add($"{label}: name is used by another store (names are compared ignoring case).");
            }

            if (string.IsNullOrWhiteSpace(store.McpUrl))
            {
                problems.Add($"{label}: mcpUrl is required.");
            }
            else if (FrontDoorStoreConfig.UrlProblem(store.McpUrl) is { } mcpProblem)
            {
                problems.Add($"{label}: mcpUrl {mcpProblem}");
            }

            if (!string.IsNullOrWhiteSpace(store.WebUrl) && FrontDoorStoreConfig.UrlProblem(store.WebUrl) is { } webProblem)
            {
                problems.Add($"{label}: webUrl {webProblem}");
            }

            if (string.IsNullOrWhiteSpace(store.McpEncryptedToken) && string.IsNullOrWhiteSpace(store.McpToken))
            {
                problems.Add($"{label}: an MCP token is required (mcpEncryptedToken, or mcpToken as text or an env:/file: reference).");
            }

            /* A store with a web address and no web token would answer 502 on every web request at run time. */
            if (!string.IsNullOrWhiteSpace(store.WebUrl)
                && string.IsNullOrWhiteSpace(store.WebEncryptedToken) && string.IsNullOrWhiteSpace(store.WebToken))
            {
                problems.Add($"{label}: a web token is required when webUrl is set (webEncryptedToken, or webToken as text or an env:/file: reference).");
            }

            if (!string.IsNullOrWhiteSpace(store.CertificateSha256) && FrontDoorStoreConfig.NormalizePin(store.CertificateSha256) is null)
            {
                problems.Add($"{label}: certificateSha256 must be the certificate's SHA-256 thumbprint, 64 hex characters (colons and spaces allowed).");
            }
        }

        return problems;
    }
}

/// <summary>One store a front door routes to: a name, the address of its MCP endpoint and (optionally) its web dashboard, the
/// access tokens, and an optional certificate pin. See <see cref="FrontDoorConfig"/>.</summary>
public sealed class FrontDoorStoreConfig
{
    internal static readonly System.Text.RegularExpressions.Regex NamePattern =
        new("^[A-Za-z0-9._-]{1,64}$", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// <summary>The store's name: 1 to 64 of <c>A-Za-z0-9._-</c>. It is the <c>X-Darling-Store</c> header value and the page address's store.</summary>
    [JsonPropertyName("name")]
    public string? Name { get; set; }

    /// <summary>The store's MCP endpoint, an absolute http or https URL with no query. Required.</summary>
    [JsonPropertyName("mcpUrl")]
    public string? McpUrl { get; set; }

    /// <summary>The store's operator-role MCP token, DPAPI-encrypted. Preferred over <see cref="McpToken"/>.</summary>
    [JsonPropertyName("mcpEncryptedToken")]
    public string? McpEncryptedToken { get; set; }

    /// <summary>The store's operator-role MCP token as text, or an <c>env:</c>/<c>file:</c> reference.</summary>
    [JsonPropertyName("mcpToken")]
    public string? McpToken { get; set; }

    /// <summary>The store's agent-role MCP token, DPAPI-encrypted. Preferred over <see cref="McpAgentToken"/>.</summary>
    [JsonPropertyName("mcpEncryptedAgentToken")]
    public string? McpEncryptedAgentToken { get; set; }

    /// <summary>The store's agent-role MCP token as text, or an <c>env:</c>/<c>file:</c> reference.</summary>
    [JsonPropertyName("mcpAgentToken")]
    public string? McpAgentToken { get; set; }

    /// <summary>The store's web dashboard, an absolute http or https URL with no query. Optional: a store without it has no web pages through the front door.</summary>
    [JsonPropertyName("webUrl")]
    public string? WebUrl { get; set; }

    /// <summary>The token the front door signs in to the store's web dashboard with, DPAPI-encrypted. Preferred over <see cref="WebToken"/>.</summary>
    [JsonPropertyName("webEncryptedToken")]
    public string? WebEncryptedToken { get; set; }

    /// <summary>The web sign-in token as text, or an <c>env:</c>/<c>file:</c> reference.</summary>
    [JsonPropertyName("webToken")]
    public string? WebToken { get; set; }

    /// <summary>The SHA-256 of the store's certificate (64 hex characters). When set, only that certificate is accepted and the
    /// chain is not consulted, which is how a store with a self-signed certificate is reached. Absent: the system's trust decides.</summary>
    [JsonPropertyName("certificateSha256")]
    public string? CertificateSha256 { get; set; }

    internal static bool TryParseUrl(string? text, out Uri uri)
    {
        if (!string.IsNullOrWhiteSpace(text)
            && Uri.TryCreate(text.Trim(), UriKind.Absolute, out var parsed)
            && (parsed.Scheme == Uri.UriSchemeHttp || parsed.Scheme == Uri.UriSchemeHttps))
        {
            uri = parsed;
            return true;
        }

        uri = null!;
        return false;
    }

    /// <summary>What is wrong with a store address, as the tail of a sentence, or null when it is usable. Never echoes the address.</summary>
    internal static string? UrlProblem(string text)
    {
        if (!TryParseUrl(text, out var uri))
        {
            return "must be an absolute http or https URL.";
        }

        if (uri.Query.Length > 0)
        {
            return "must not have a query string.";
        }

        if (uri.UserInfo.Length > 0)
        {
            return "must not carry a user name or password.";
        }

        return null;
    }

    /// <summary>A pin as 64 upper-case hex characters (colons and spaces dropped), or null when it is not one.</summary>
    internal static string? NormalizePin(string? pin)
    {
        var hex = new string((pin ?? "").Where(c => c != ':' && !char.IsWhiteSpace(c)).ToArray());
        return hex.Length == 64 && hex.All(Uri.IsHexDigit) ? hex.ToUpperInvariant() : null;
    }

    /// <summary>The routing record for this entry, or null when it is not usable (no valid name or MCP address, or a pin that is not 64 hex characters).</summary>
    internal FrontDoorStore? ToStore()
    {
        var name = Name?.Trim() ?? "";
        if (!NamePattern.IsMatch(name) || !TryParseUrl(McpUrl, out var mcp))
        {
            return null;
        }

        /* A pin that is set and is not a pin is not "no pin": dropping it would reach the store under the system's chain
           trust, which is what the pin was written to exclude, and only the worker validates this file. */
        var pin = NormalizePin(CertificateSha256);
        if (!string.IsNullOrWhiteSpace(CertificateSha256) && pin is null)
        {
            return null;
        }

        var web = TryParseUrl(WebUrl, out var webUri) ? webUri : null;
        return new FrontDoorStore(name, mcp, web, pin);
    }

    /// <summary>The MCP token for the role, preferring the encrypted form; null when none is set. Throws when an encrypted token
    /// cannot be read or a reference cannot be resolved.</summary>
    internal string? ResolveMcpToken(bool agentRole)
        => agentRole
            ? ResolveSecret(McpEncryptedAgentToken, McpAgentToken, "mcpEncryptedAgentToken", "mcpAgentToken")
            : ResolveSecret(McpEncryptedToken, McpToken, "mcpEncryptedToken", "mcpToken");

    /// <summary>The web sign-in token, preferring the encrypted form; null when none is set.</summary>
    internal string? ResolveWebToken()
        => ResolveSecret(WebEncryptedToken, WebToken, "webEncryptedToken", "webToken");

    /* The same resolution as McpTokenEntry.ResolveToken, with this entry's own setting in each error. */
    private string? ResolveSecret(string? encrypted, string? plain, string encryptedKey, string plainKey)
    {
        var setting = $"frontDoor.stores[{Name}]";
        if (!string.IsNullOrWhiteSpace(encrypted))
        {
            if (!OperatingSystem.IsWindows())
            {
                throw new PlatformNotSupportedException(
                    $"{setting}.{encryptedKey} requires Windows (DPAPI); use the plaintext \"{plainKey}\" on other platforms.");
            }

            return DarlingSecrets.Unprotect(encrypted);
        }

        return string.IsNullOrWhiteSpace(plain) ? null : DarlingSecretSource.Resolve(plain, $"{setting}.{plainKey}");
    }
}
