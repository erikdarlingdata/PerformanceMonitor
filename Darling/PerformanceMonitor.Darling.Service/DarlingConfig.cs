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
using PerformanceMonitor.Collectors;
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

    [JsonPropertyName("servers")]
    public List<MonitoredServer> Servers { get; set; } = new();

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
    /// the store is authoritative afterwards, like every other value here.
    ///
    /// <para>15 preserves the constant it replaces. It is a judgement about store size and disk speed, which
    /// this product cannot make for someone else's deployment — a fleet-wide aggregate over a wide window on a
    /// large store can exceed 15s with nothing wrong.</para>
    /// </summary>
    public int ComposeStatementTimeoutSeconds { get; set; } = 15;

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
    /// The shared alert engine's enabled flags and thresholds (Phase-5 slice D). Every default
    /// mirrors Lite's <c>App.*</c> alert defaults exactly, so an empty section alerts like a
    /// fresh Lite install. Optional — omit it entirely for the defaults.
    /// </summary>
    [JsonPropertyName("alerts")]
    public AlertsConfig Alerts { get; set; } = new();

    /// <summary>
    /// SMTP delivery for fired alerts. Delivery is enabled when host + from + to are all set
    /// (no separate flag — defaults over speculative config); the password uses the same DPAPI
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
    /// The embedded MCP server (analysis slice AN4): the same analysis + data-read tool surface Lite
    /// and the Dashboard expose (the analysis class plus the plan-analysis and ~60 STORED data-read
    /// tools), over Streamable HTTP. Default OFF: a headless service should not open a port unless the
    /// operator asks for it (both apps default their MCP servers off too). Loopback-only and tokenless
    /// by default; an opt-in <see cref="McpConfig.Network"/> block exposes it on the LAN behind a
    /// required bearer token + an in-app CIDR check (managed-mode only). Optional — omit the section entirely.
    /// </summary>
    [JsonPropertyName("mcp")]
    public McpConfig Mcp { get; set; } = new();

    /// <summary>
    /// The embedded web dashboard (#1562): a second, independent Kestrel host serving a read-only,
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

    public static string ResolveConfigPath(string? explicitPath = null)
    {
        if (!string.IsNullOrWhiteSpace(explicitPath))
        {
            return explicitPath;
        }

        var fromEnvironment = Environment.GetEnvironmentVariable("DARLING_CONFIG");
        if (!string.IsNullOrWhiteSpace(fromEnvironment))
        {
            return fromEnvironment;
        }

        return Path.Combine(AppContext.BaseDirectory, "darling.json");
    }

    public static DarlingConfig Load(string? explicitPath = null)
    {
        var path = ResolveConfigPath(explicitPath);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException(
                $"Configuration file not found: {path}. Copy darling.sample.json to darling.json and edit it.", path);
        }

        return Parse(File.ReadAllText(path));
    }

    public static DarlingConfig Parse(string json)
    {
        var config = JsonSerializer.Deserialize<DarlingConfig>(json, s_jsonOptions);
        if (config is null)
        {
            throw new InvalidDataException("Configuration file parsed to null.");
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

        if (Postgres is null)
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
        }
        else if (string.IsNullOrWhiteSpace(Postgres.ConnectionString))
        {
            problems.Add("postgres.connectionString is required (or set postgres.managed = true to run the bundled server).");
        }

        /* Peer-disclosure problems are checked BEFORE the servers early-return so a broken peers block is
           reported even on a config that has no servers yet. */
        problems.AddRange(PeersConfig.Validate(Peers));

        if (Servers is null || Servers.Count == 0)
        {
            problems.Add("servers must contain at least one entry.");
            return problems;
        }

        for (int i = 0; i < Servers.Count; i++)
        {
            var server = Servers[i];
            var label = string.IsNullOrWhiteSpace(server.Name) ? $"servers[{i}]" : $"server '{server.Name}'";

            if (string.IsNullOrWhiteSpace(server.Host))
            {
                problems.Add($"{label}: host is required.");
            }

            if (server.UsesSqlAuth)
            {
                if (string.IsNullOrWhiteSpace(server.Username))
                {
                    problems.Add($"{label}: sql auth requires username.");
                }

                if (string.IsNullOrWhiteSpace(server.EncryptedPassword) && string.IsNullOrWhiteSpace(server.Password))
                {
                    problems.Add($"{label}: sql auth requires encryptedPassword (preferred; see --encrypt-password) or password.");
                }
            }
            else if (!string.Equals(server.Auth, "integrated", StringComparison.OrdinalIgnoreCase))
            {
                problems.Add($"{label}: auth must be 'integrated' or 'sql' (got '{server.Auth}').");
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

    [JsonPropertyName("poisonWaitThresholdMs")]
    public int PoisonWaitThresholdMs { get; set; } = 500;

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
    public int FileGrowthRiseMb { get; set; } = 10240;
    public int FileGrowthVolumePercent { get; set; } = 60;
    public int FileGrowthLookbackMinutes { get; set; } = 60;

    /// <summary>#2107: the store volume's self-alert warning percent (was a compile-time 10.0;
    /// 0 disables the check — percent is its only trigger).</summary>
    [JsonPropertyName("selfDiskFreeWarnPercent")]
    public int SelfDiskFreeWarnPercent { get; set; } = 10;

    /// <summary>#2107: how long collection may go quiet before Collection Stopped / Agent Not
    /// Running fire (was a compile-time 30 minutes).</summary>
    [JsonPropertyName("collectionStaleMinutes")]
    public int CollectionStaleMinutes { get; set; } = 30;

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
    /// Critical tier is fixed at 100 (a job outrunning its cadence compounds refresh lag).</summary>
    [JsonPropertyName("storeJobCadenceWarnPercent")]
    public int StoreJobCadenceWarnPercent { get; set; } = 25;

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
}

/// <summary>
/// The embedded MCP server's config. Port 5152 keeps the product's local MCP family
/// non-colliding on one machine (Dashboard 5150, Lite 5151, Darling 5152). Loopback-only and
/// tokenless by default; the optional <see cref="Network"/> block opts into LAN exposure behind a
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
    /// Opt-in network exposure for the MCP server (darling-network-endpoints). Omit for the secure
    /// default = loopback-only, tokenless HTTP (today's behavior). Managed-mode only; ignored in BYO
    /// with a caller warning. Any missing precondition (token / valid allowFrom / managed) keeps MCP
    /// loopback-only + LogCritical — enforced in the MCP host, NEVER in the all-fatal
    /// <see cref="DarlingConfig.Validate"/> (D-validate). See <see cref="McpNetworkConfig"/>.
    /// </summary>
    [JsonPropertyName("network")]
    public McpNetworkConfig? Network { get; set; }
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
/// <c>network</c> object for the secure default = today's loopback-only, tokenless HTTP. When present
/// with a non-loopback <see cref="Listen"/> AND managed mode AND a token AND a valid
/// <see cref="AllowFrom"/>, the MCP host binds the network interface behind a required bearer token +
/// an in-app CIDR check (D3); any missing precondition keeps MCP loopback-only + LogCritical
/// (fail-closed, enforced in the MCP host). No TLS on MCP (a self-signed cert breaks real clients; the
/// named MITM control is a TLS reverse proxy in front of the endpoint).
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
    /// The CIDR the in-app <c>RemoteIpAddress</c> check and the firewall rule allow (e.g.
    /// <c>192.168.1.0/24</c>); loopback is always allowed regardless. Required when exposed.
    /// </summary>
    [JsonPropertyName("allowFrom")]
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
    /// True when any field is set — used only for the BYO "network.* is ignored" caller warning (D-BYO);
    /// NOT the same as "exposed".
    /// </summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Listen)
        || !string.IsNullOrWhiteSpace(AllowFrom)
        || !string.IsNullOrWhiteSpace(EncryptedToken)
        || !string.IsNullOrWhiteSpace(Token);

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
/// connections; the web dashboard is read-only over the store).
/// </summary>
public sealed class WebConfig
{
    /// <summary>Default OFF — a headless service should not open the dashboard port unless asked.</summary>
    [JsonPropertyName("enabled")]
    public bool Enabled { get; set; }

    [JsonPropertyName("port")]
    public int Port { get; set; } = 5153;

    /// <summary>
    /// Opt-in network exposure for the web dashboard (darling-network-endpoints). Omit for the secure
    /// default = loopback-only HTTP. Managed-mode only; ignored in BYO with a caller warning. Any missing
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
/// once via <c>?token=</c>, which is exchanged for an HMAC-signed session cookie. While exposed EVERY request
/// authenticates, loopback included — loopback is exempt from the CIDR test only, never from the credential
/// (#1649). Any missing precondition keeps the dashboard loopback-only + LogCritical (fail-closed, enforced in
/// the web host).
///
/// <para><b>TLS is opt-in via <see cref="Tls"/> (#2562).</b> Without it the network listener is plain HTTP and
/// the token and session cookie cross the segment in the clear — the web host warns about exactly that at
/// every exposed start. MCP still has no TLS of its own on the older rationale that a self-signed certificate
/// breaks real MCP clients; that argument is about MCP clients rather than about the wire, and it does not
/// carry to a surface whose only client is a browser.</para>
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
    /// The CIDR the in-app <c>RemoteIpAddress</c> check and the firewall rule allow (e.g.
    /// <c>192.168.1.0/24</c>); loopback is always allowed regardless. Required when exposed.
    /// </summary>
    [JsonPropertyName("allowFrom")]
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
    /// True when any field is set — used only for the BYO "network.* is ignored" caller warning (D-BYO);
    /// NOT the same as "exposed".
    /// </summary>
    [JsonIgnore]
    public bool IsConfigured =>
        !string.IsNullOrWhiteSpace(Listen)
        || !string.IsNullOrWhiteSpace(AllowFrom)
        || !string.IsNullOrWhiteSpace(EncryptedToken)
        || !string.IsNullOrWhiteSpace(Token)
        || (Tls?.IsConfigured ?? false);

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
/// Opt-in TLS for the web dashboard's network listener (#2562). Omit the whole <c>tls</c> object for plain
/// HTTP — the zero-config default, and the correct one for a loopback-only dashboard, which has nothing to
/// encrypt. Supply a certificate to close the gap the exposure block otherwise leaves open: the access token
/// and the HMAC session cookie it is exchanged for both cross the segment in the clear over HTTP, and the
/// in-app CIDR check bounds who can ROUTE to the port, never what an on-path attacker can read off the wire.
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
/// EXPIRED certificate keeps the dashboard loopback-only and logs Critical. It never falls back to serving
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
    public string? ResolvePfxPassword(out bool usedPlaintext)
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
                    "web.network.tls.encryptedPfxPassword requires Windows (DPAPI); use \"pfxPassword\" with a "
                    + "file:/env: reference on other platforms.");
            }

            return DarlingSecrets.Unprotect(EncryptedPfxPassword);
        }

        if (!string.IsNullOrWhiteSpace(PfxPassword))
        {
            /* An env:/file: reference (#1804) is not plaintext-in-config — no warning for it. */
            usedPlaintext = !DarlingSecretSource.IsReference(PfxPassword);
            return DarlingSecretSource.Resolve(PfxPassword, "web.network.tls.pfxPassword");
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
    /// <para>Accepts <c>"admin"</c>, <c>"viewer"</c>, or both in one string separated by a comma, a plus or
    /// whitespace (<c>"admin,viewer"</c>, <c>"admin+viewer"</c>). A JSON array would be the tidier surface
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
        /* Literals rather than DarlingManagedPostgres.ViewerRoleName/AdminRoleName: that type is
           [SupportedOSPlatform("windows")] and this classifier is platform-neutral, so referencing its
           consts would raise CA1416 here. The names mirror those consts (pinned equal by test). */
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

            if (normalized is not ("viewer" or "admin"))
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
           server every time somebody reorders the field. */
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

    /// <summary>The JSON body, with <c>{{metric}}</c>/<c>{{server}}</c>/… placeholders; empty uses the shared default.</summary>
    [JsonPropertyName("genericBodyTemplate")]
    public string GenericBodyTemplate { get; set; } = "";

    [JsonPropertyName("genericProxy")]
    public string GenericProxy { get; set; } = "";

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
    /// <c>host,1433</c> — that convention is left alone rather than migrated.</para>
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

    [JsonIgnore]
    public bool UsesSqlAuth => string.Equals(Auth, "sql", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// <see cref="Engine"/> parsed. Anything unrecognized resolves to
    /// <see cref="CollectorTargetEngine.SqlServer"/> rather than throwing: a typo in one server entry
    /// must not stop the service from starting and monitoring everything else. The mismatch surfaces
    /// immediately anyway — the SQL Server detection query fails against a Postgres target.
    /// </summary>
    [JsonIgnore]
    public CollectorTargetEngine TargetEngine => Engine?.Trim().ToLowerInvariant() switch
    {
        "postgres" or "postgresql" or "pg" or "aurora-postgresql" or "aurora" => CollectorTargetEngine.PostgreSql,
        _ => CollectorTargetEngine.SqlServer,
    };

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
                    problems.Add(
                        $"{label}: peer text contains '{offending}'. The peers block is DISCLOSURE ONLY — its " +
                        "text is sent verbatim to every connected MCP client — so it must carry no connection " +
                        "string and no credential. Describe what the peer monitors, not how to reach it.");
                    break;
                }
            }
        }

        return problems;
    }

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
