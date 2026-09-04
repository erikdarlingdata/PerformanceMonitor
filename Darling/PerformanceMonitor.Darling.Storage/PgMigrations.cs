/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Darling's versioned schema migrations — plain SQL scripts the service applies on startup
/// (headless plan: no migration framework). Each script runs once, inside its own transaction,
/// tracked in darling_schema_version. V1 is generated from the collector definitions
/// (<see cref="PgSchemaGenerator.GenerateFullSchema"/>); later versions are appended, never
/// edited. Migrations stay engine-plain on purpose: TimescaleDB hypertable conversion is
/// RUNTIME setup (<see cref="TimescaleSupport"/>), applied by the service only when the
/// extension is detected — the same store must work on plain PostgreSQL.
/// </summary>
public static class PgMigrations
{
    public sealed class Migration
    {
        public Migration(int version, string name, string sql)
        {
            Version = version;
            Name = name;
            Sql = sql;
        }

        public int Version { get; }

        public string Name { get; }

        public string Sql { get; }
    }

    public static IReadOnlyList<Migration> Scripts { get; } = new[]
    {
        new Migration(1, "collector-tables", PgSchemaGenerator.GenerateFullSchema()),
        new Migration(2, "server-registry-and-collection-log", V2Sql),
        new Migration(3, "alerting-stores", V3Sql),
        new Migration(4, "analysis-tables", V4Sql),
        new Migration(5, "viewer-passthrough-views", V5Sql),
        new Migration(6, "memory-tab-passthrough-views", V6Sql),
        new Migration(7, "viewer-plan-capture-columns", V7Sql),
        new Migration(8, "schema-split-collect-config", PgSchemaGenerator.GenerateV8Move()),
        new Migration(9, "server-inventory-cost-fields", V9Sql),
        new Migration(10, "latch-spinlock-collectors", PgSchemaGenerator.GenerateV10AddLatchSpinlock()),
        new Migration(11, "cpu-scheduler-plan-cache-collectors", PgSchemaGenerator.GenerateV11AddCpuSchedulerPlanCache()),
        new Migration(12, "session-summary-collector", PgSchemaGenerator.GenerateV12AddSessionSummary()),
        new Migration(13, "system-health-events-collector", PgSchemaGenerator.GenerateV13AddSystemHealthEvents()),
        new Migration(14, "refresh-passthrough-views", PgSchemaGenerator.GenerateV14RefreshViews()),
        new Migration(15, "index-metadata-columns", V15Sql),
        new Migration(16, "server-utc-offset", V16Sql),
        new Migration(17, "config-control-plane", V17Sql),
        new Migration(18, "alert-delivery-mode", V18Sql),
        new Migration(19, "analysis-state-marker", V19Sql),
        new Migration(20, "alert-tuning-knobs", V20Sql),
        new Migration(21, "default-trace-events-collector", V21Sql),
        new Migration(22, "index-object-stats-latest-index", V22Sql),
        new Migration(23, "collection-log-hypertable", V23Sql),
        new Migration(24, "job-history-collector", V24Sql),
        new Migration(25, "agent-status-collector", V25Sql),
        new Migration(26, "generic-webhook-channel", V26Sql),
        new Migration(27, "deadlocks-database-name", V27Sql),
        new Migration(28, "query-store-replica-role", V28Sql),
        new Migration(29, "long-query-completions-collector", V29Sql),
        new Migration(30, "web-dashboard-config", V30Sql),
        new Migration(31, "custom-views-table", V31Sql),
        new Migration(32, "server-tags", V32Sql),
        new Migration(33, "connection-alert-refire", V33Sql),
        new Migration(34, "availability-group-collectors", V34Sql),
        new Migration(35, "availability-group-alerts", V35Sql),
        new Migration(36, "ag-latency-columns", V36Sql),
        new Migration(37, "ag-local-replica-and-disconnect-refire", V37Sql),
        new Migration(38, "query-payload-dimensions", PgSchemaGenerator.GenerateV38PayloadDimensions()),
        new Migration(39, "dim-feeding-fact-floor-indexes", V39Sql),
        new Migration(40, "blocking-wait-threshold", V40Sql),
        new Migration(41, "query-store-interval-identity", V41Sql),
        new Migration(42, "pagerduty-webhook", V42Sql),
        new Migration(43, "pagerduty-proxy", V43Sql),
        new Migration(44, "collector-state", V44Sql),
        /* 45 is permanently absent. It was reserved for the #1951 lane while that lane was still
           unmerged, but #1952 landed first and took 46, and the runner applies only versions ABOVE
           the store's MAX(version) — so a store already stamped 46 would have skipped a late-arriving
           45 forever, leaving upgraded stores without a table fresh stores get from V1. #1951 was
           renumbered to 47 rather than shipped into that hole. Version numbers only have to be unique
           and ascending: a gap costs nothing and a collision would cost a store. */
        new Migration(46, "plan-correction-collector", V46Sql),
        new Migration(47, "pvs-stats", V47Sql),
        new Migration(48, "pvs-pressure-alert", V48Sql),
        new Migration(49, "database-state-alert", V49Sql),
        new Migration(50, "server-tag-colour", V50Sql),
        /* #2119: V54Sql is PREPENDED ahead of the generated view. This rung's view SQL comes from the
           LIVE generator, which since #2069 emits the V54 gz column — a ≤V50 store replaying this rung
           on current code referenced a column three rungs before the ALTER that adds it (42703, the
           ladder halts, every 3.3.0→3.4.0 upgrade failed). V54's ALTERs are idempotent, so pre-adding
           here costs a fresh-through-this-rung store nothing and rung 54's own copy no-ops. This is
           the standing hazard of generator-built rungs: any LATER column the generator learns must be
           pre-added in EVERY earlier rung that re-emits generated SQL over existing tables — pinned by
           MigrationLadderPins so the next collision fails in CI, not on an operator's store. */
        new Migration(51, "query-stats-host-object", V51Sql + "\n" + V54Sql + "\n" + PgSchemaGenerator.GenerateQueryStatsResolvingView()),
        new Migration(52, "finding-drilldown-json", V52Sql),
        new Migration(53, "store-self-metrics", V53Sql),
        new Migration(54, "plan-dim-gzip", V54Sql + "\n" + PgSchemaGenerator.GenerateQueryStatsResolvingView()),
        new Migration(55, "self-alert-knobs", V55Sql),
        new Migration(56, "store-metrics-background-jobs", V56Sql),
        new Migration(57, "store-job-cadence-knob", V57Sql),
        new Migration(58, "qs-backfill-switch", V58Sql),
        new Migration(59, "collector-memory-knobs", V59Sql),
        new Migration(60, "database-state-edge-memory", V60Sql),
        new Migration(61, "incident-occurrence-counters", V61Sql),
        new Migration(62, "plan-xml-compression-knob", V62Sql),
        new Migration(63, "pg-wait-stats", V63Sql),
        new Migration(64, "pg-statement-stats", V64Sql),
        new Migration(65, "pg-wraparound-stats", V65Sql),
        new Migration(66, "pg-xmin-horizon", V66Sql),
        new Migration(67, "pg-replication-slots", V67Sql),
        new Migration(68, "pg-autovacuum-stats", V68Sql),
        new Migration(69, "pg-io-stats", V69Sql),
        new Migration(70, "monitored-server-engine", V70Sql),
        new Migration(71, "pg-blocking-edges", V71Sql),
        new Migration(72, "query-store-plan-map", V72Sql),
        new Migration(73, "pg-statement-text", V73Sql),
        new Migration(74, "query-store-text", V74Sql),
        new Migration(75, "plan-content-retention-knob", V75Sql),
        new Migration(76, "query-store-health", V76Sql),
        new Migration(77, "activity-driven-plan-fetch", V77Sql),
        new Migration(78, "compose-statement-timeout", V78Sql),
        new Migration(79, "file-growth-alert", V79Sql),
        new Migration(80, "collection-log-fanout-rollup", V80Sql),
        new Migration(81, "tempdb-max-size", V81Sql),
        new Migration(82, "server-engine-kind", V82Sql),
        new Migration(83, "pg-database-stats", V83Sql),
        new Migration(84, "pg-index-usage-stats", V84Sql),
        new Migration(85, "pg-table-bloat-stats", V85Sql),
        new Migration(86, "pg-session-states", V86Sql),
        new Migration(87, "pg-plan-capture-readiness", V87Sql),
        new Migration(88, "pg-write-stats", V88Sql),
        new Migration(89, "pg-extension-availability", V89Sql),
        new Migration(90, "pg-lock-stats", V90Sql),
        new Migration(91, "pg-column-stats", V91Sql),
        new Migration(92, "pg-replication-stats", V92Sql),
        new Migration(93, "pg-buffer-usage", V93Sql),
        new Migration(94, "pg-index-bloat", V94Sql),
        new Migration(95, "pg-per-database-attribution", V95Sql),
        new Migration(96, "pg-wait-sampling", V96Sql),
        new Migration(97, "pg-kernel-stats", V97Sql),
        new Migration(98, "pg-predicate-stats", V98Sql),
        new Migration(99, "pg-plan-capture", V99Sql),
        new Migration(100, "pg-major-version", V100Sql),
        new Migration(101, "pg18-io-bytes", V101Sql),
        new Migration(102, "pg-server-config", V102Sql),
        new Migration(103, "pg-deadlocks", V103Sql),
        new Migration(104, "pg-deadlock-identity-index", V104Sql),
        new Migration(105, "collector-cost", V105Sql),
        new Migration(106, "pg-cpu-utilization", V106Sql),
        new Migration(107, "plan-force-actions", V107Sql),
        new Migration(108, "collection-log-phase-split", V108Sql),
        new Migration(109, "collection-log-drain-forensics", V109Sql),
        new Migration(110, "collection-log-fetch-phase-sums", V110Sql),
    };

    /// <summary>
    /// V2 — the service's observability store: the servers registry (upserted on every
    /// successful connect) and the per-run collection_log. Column names deliberately mirror
    /// Lite's DuckDB schema so viewer/analysis SQL can twin across stores — including
    /// duckdb_duration_ms, which in Darling records the Postgres storage phase. Lite's servers
    /// table also carries auth columns (use_windows_auth/username); Darling deliberately omits
    /// them because auth lives in darling.json. servers keeps its PRIMARY KEY (a registry, not
    /// a hypertable candidate); collection_log has none, same reasoning as the collector tables.
    /// </summary>
    private const string V2Sql = @"
CREATE TABLE IF NOT EXISTS servers (
    server_id integer NOT NULL PRIMARY KEY,
    server_name text NOT NULL,
    display_name text,
    is_enabled boolean NOT NULL DEFAULT TRUE,
    sql_engine_edition integer,
    sql_major_version integer,
    created_date timestamp,
    modified_date timestamp
);

CREATE TABLE IF NOT EXISTS collection_log (
    log_id bigint NOT NULL,
    server_id integer NOT NULL,
    server_name text,
    collector_name text NOT NULL,
    collection_time timestamp NOT NULL,
    duration_ms integer,
    status text NOT NULL,
    error_message text,
    rows_collected integer,
    sql_duration_ms integer,
    duckdb_duration_ms integer
);

CREATE INDEX IF NOT EXISTS idx_collection_log_time ON collection_log(server_id, collection_time);";

    /// <summary>
    /// V3 — the alerting stores behind the Phase-5 shared alert engine (slice D), each mirroring
    /// its Lite DuckDB twin column-for-column so viewer/analysis SQL can twin across stores:
    /// <c>config_alert_log</c> (one combined history row per fired alert — Lite's Schema.cs
    /// CreateAlertLogTable), <c>config_edge_trigger_watermarks</c> (the #1091 rolling-count
    /// watermarks + the time-based failed-job watermark, #1145 restart survival — Lite's
    /// CreateEdgeTriggerWatermarksTable, including its (server_id, metric_name) primary key the
    /// upserts conflict on), and <c>config_mute_rules</c> (Lite's CreateMuteRulesTable). The
    /// alert-log index serves the per-(server, metric) MAX(alert_time) cooldown seeds.
    /// </summary>
    private const string V3Sql = @"
CREATE TABLE IF NOT EXISTS config_alert_log (
    alert_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    metric_name text NOT NULL,
    current_value double precision NOT NULL,
    threshold_value double precision NOT NULL,
    alert_sent boolean NOT NULL DEFAULT FALSE,
    notification_type text NOT NULL DEFAULT 'tray',
    send_error text,
    dismissed boolean NOT NULL DEFAULT FALSE,
    muted boolean NOT NULL DEFAULT FALSE,
    detail_text text,
    context_json text
);

CREATE INDEX IF NOT EXISTS idx_config_alert_log_time ON config_alert_log(server_id, metric_name, alert_time);

CREATE TABLE IF NOT EXISTS config_edge_trigger_watermarks (
    server_id integer NOT NULL,
    metric_name text NOT NULL,
    watermark integer NOT NULL,
    watermark_time timestamp,
    updated_at timestamp NOT NULL,
    PRIMARY KEY (server_id, metric_name)
);

CREATE TABLE IF NOT EXISTS config_mute_rules (
    id text NOT NULL PRIMARY KEY,
    enabled boolean NOT NULL DEFAULT TRUE,
    created_at_utc timestamp NOT NULL,
    expires_at_utc timestamp,
    reason text,
    server_name text,
    metric_name text,
    database_pattern text,
    query_text_pattern text,
    wait_type_pattern text,
    job_name_pattern text
);";

    /// <summary>
    /// V4 — the analysis-engine stores (Phase-5 analysis slice AN1) plus the passthrough views
    /// the ported analysis SQL reads. <c>analysis_findings</c> is Lite's AnalysisSchema v3 shape
    /// column-for-column PLUS the Dashboard's <c>remediation_action_json</c> (recommendations
    /// rebuild D2: the BUILT RemediationAction persisted on the row, serialized by the shared
    /// AlertContextSerializer) — no primary key, same hypertable/COPY reasoning as the collector
    /// tables (TimescaleDB hypertables require the partition column in any unique constraint, and
    /// bulk ingest doesn't want one); finding_id stays a NOT NULL bigint, and the two Lite index
    /// column sets carry over. <c>analysis_muted</c> is a small mute registry and KEEPS its
    /// primary key (like V2's servers); Lite's DuckDB <c>DEFAULT CURRENT_TIMESTAMP</c> on
    /// muted_date is deliberately NOT carried — in Postgres that default would stamp the PG
    /// server's LOCAL clock, and the store always supplies naive-UTC explicitly.
    ///
    /// The <c>v_&lt;table&gt;</c> views exist so analysis SQL ports VERBATIM (AN2/AN3): in Lite
    /// they union the hot DuckDB table with the parquet archive; Darling has no parquet tier, so
    /// they are plain passthroughs. Seventeen views — the fourteen the fact collectors read
    /// (wait/query/query-store/cpu/memory-grant/memory/perfmon/session/file-io stats,
    /// blocked_process_reports, deadlocks, dmv_blocking_snapshots, index_object_stats,
    /// database_size_stats) plus the three Lite's drill-down/storage collectors also read
    /// (v_tempdb_stats in DuckDbFactCollector.Storage + DrillDownCollector.Storage,
    /// v_query_snapshots in DrillDownCollector.Queries, v_database_config in
    /// DrillDownCollector.Config). Every view targets a V1 collector table, pinned by test.
    /// </summary>
    private const string V4Sql = @"
CREATE TABLE IF NOT EXISTS analysis_findings (
    finding_id bigint NOT NULL,
    analysis_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    time_range_start timestamp,
    time_range_end timestamp,
    severity double precision NOT NULL,
    confidence double precision NOT NULL,
    category text NOT NULL,
    story_path text NOT NULL,
    story_path_hash text NOT NULL,
    story_text text NOT NULL,
    root_fact_key text NOT NULL,
    root_fact_value double precision,
    leaf_fact_key text,
    leaf_fact_value double precision,
    fact_count integer NOT NULL,
    incident_id text,
    remediation_action_json text
);

CREATE INDEX IF NOT EXISTS idx_analysis_findings_time ON analysis_findings(server_id, analysis_time);
CREATE INDEX IF NOT EXISTS idx_analysis_findings_hash ON analysis_findings(story_path_hash);

CREATE TABLE IF NOT EXISTS analysis_muted (
    mute_id bigint NOT NULL PRIMARY KEY,
    server_id integer,
    database_name text,
    story_path_hash text NOT NULL,
    story_path text NOT NULL,
    muted_date timestamp NOT NULL,
    reason text
);

CREATE INDEX IF NOT EXISTS idx_analysis_muted_hash ON analysis_muted(story_path_hash);

CREATE OR REPLACE VIEW v_wait_stats AS SELECT * FROM wait_stats;
CREATE OR REPLACE VIEW v_query_stats AS SELECT * FROM query_stats;
CREATE OR REPLACE VIEW v_query_store_stats AS SELECT * FROM query_store_stats;
CREATE OR REPLACE VIEW v_cpu_utilization_stats AS SELECT * FROM cpu_utilization_stats;
CREATE OR REPLACE VIEW v_memory_grant_stats AS SELECT * FROM memory_grant_stats;
CREATE OR REPLACE VIEW v_memory_stats AS SELECT * FROM memory_stats;
CREATE OR REPLACE VIEW v_perfmon_stats AS SELECT * FROM perfmon_stats;
CREATE OR REPLACE VIEW v_session_stats AS SELECT * FROM session_stats;
CREATE OR REPLACE VIEW v_file_io_stats AS SELECT * FROM file_io_stats;
CREATE OR REPLACE VIEW v_blocked_process_reports AS SELECT * FROM blocked_process_reports;
CREATE OR REPLACE VIEW v_deadlocks AS SELECT * FROM deadlocks;
CREATE OR REPLACE VIEW v_dmv_blocking_snapshots AS SELECT * FROM dmv_blocking_snapshots;
CREATE OR REPLACE VIEW v_index_object_stats AS SELECT * FROM index_object_stats;
CREATE OR REPLACE VIEW v_database_size_stats AS SELECT * FROM database_size_stats;
CREATE OR REPLACE VIEW v_tempdb_stats AS SELECT * FROM tempdb_stats;
CREATE OR REPLACE VIEW v_query_snapshots AS SELECT * FROM query_snapshots;
CREATE OR REPLACE VIEW v_database_config AS SELECT * FROM database_config;";

    /* V5 — the five passthrough views V4 left out, completing the v_* twin of Lite's DuckDB
       view layer so every ported viewer query stays byte-identical to Lite's (the copy-parity
       program's tail tabs read these: Running Jobs, Configuration ×3, Daily Summary /
       Collection Health). Tables all exist since V1/V2; views only. */
    private const string V5Sql = @"
CREATE OR REPLACE VIEW v_running_jobs AS SELECT * FROM running_jobs;
CREATE OR REPLACE VIEW v_server_config AS SELECT * FROM server_config;
CREATE OR REPLACE VIEW v_database_scoped_config AS SELECT * FROM database_scoped_config;
CREATE OR REPLACE VIEW v_trace_flags AS SELECT * FROM trace_flags;
CREATE OR REPLACE VIEW v_collection_log AS SELECT * FROM collection_log;";

    /* V6 — the two memory passthrough views V4/V5 left out, needed by the Memory tab port (W1j): the
       Memory Clerks + Memory Pressure Events sub-tabs read v_memory_clerks / v_memory_pressure_events,
       mirroring Lite, so their ported SQL stays byte-identical. Tables exist since V1; views only. */
    private const string V6Sql = @"
CREATE OR REPLACE VIEW v_memory_clerks AS SELECT * FROM memory_clerks;
CREATE OR REPLACE VIEW v_memory_pressure_events AS SELECT * FROM memory_pressure_events;";

    /* V7 — the deferred-plan-capture columns the viewer's blocked-process/deadlock/procedure "View
       Plan" surfaces read (PR #1262, extending the #1349 pattern to three more collectors). All are
       nullable text appended to existing collector tables, so a store already at V1's pre-plan shape
       comes up to shape with one ADD COLUMN each; a fresh install already has them (V1 is generated
       from the current collector definitions, which now include these columns), and ADD COLUMN IF NOT
       EXISTS makes that a harmless no-op. Darling captures the plans because DarlingConfig.CapturePlans
       defaults true (CollectorContext.CapturePlanXml) — no config change; Lite never sets the flag and
       always writes NULL here. Appended (not inserted) so a fresh V1 store and an upgraded V7 store keep
       an identical physical column order. */
    private const string V7Sql = @"
ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS query_plan_xml text;
ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocked_query_plan_xml text;
ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocking_query_plan_xml text;
ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS victim_query_plan_xml text;";

    /* V27 — deadlocks.database_name: the victim process's currentdbname, keying the Azure SQL DB
       per-database watermark (#1535: capture is one database-scoped session per monitored database).
       Nullable text appended at the end (identical physical column order for the binary COPY whether
       fresh — V1 is generated from the current collector definition, which now includes it — or
       upgraded; ADD COLUMN IF NOT EXISTS no-ops on fresh). blocked_process_reports already had
       database_name. The trailing CREATE OR REPLACE VIEW re-expands v_deadlocks' pinned SELECT *
       (Postgres freezes it at CREATE; append-only ADDs keep the refresh legal), mirroring V15.
       Runs after V8, so the bare names resolve through search_path = collect, config, public. */
    private const string V27Sql = @"
ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS database_name text;
CREATE OR REPLACE VIEW v_deadlocks AS SELECT * FROM deadlocks;";

    /* V28 — query_store_stats.replica_role: the replica role SQL Server 2022+ attributed each
       runtime-stats row to (sys.query_store_replicas.replica_name, LEFT JOINed by replica_group_id).
       With "Query Store for secondary replicas" enabled an AG keeps ONE shared Query Store on the
       PRIMARY holding every replica's rows, so the primary's numbers silently blend in secondary
       workload unless split by this column. Nullable text appended at the end (identical physical
       column order for the binary COPY whether fresh — V1 is generated from the current collector
       definition, which now includes it — or upgraded; ADD COLUMN IF NOT EXISTS no-ops on fresh).
       The trailing CREATE OR REPLACE VIEW re-expands v_query_store_stats' pinned SELECT * (Postgres
       freezes it at CREATE; append-only ADDs keep the refresh legal), mirroring V15/V27. Runs after
       V8, so the bare names resolve through search_path = collect, config, public. */
    private const string V28Sql = @"
ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS replica_role text;
CREATE OR REPLACE VIEW v_query_store_stats AS SELECT * FROM query_store_stats;";

    /// <summary>
    /// V46 — the <c>plan_correction</c> collector table (#1952): what the engine's own automatic plan
    /// correction is doing per database — the FORCE_LAST_GOOD_PLAN enablement state from
    /// <c>sys.database_automatic_tuning_options</c>, and the live recommendation set from
    /// <c>sys.dm_db_tuning_recommendations</c> with its details JSON shredded into typed columns and
    /// the regressed query's text resolved through Query Store at collection time. Added additively
    /// exactly like V29/V34 — a fresh store already has it (V1's
    /// <see cref="PgSchemaGenerator.GenerateFullSchema"/> walks the collector catalog), so
    /// <c>CREATE TABLE IF NOT EXISTS</c> is a no-op on fresh and the real create on upgrade. Column
    /// order/types are exactly <see cref="PgSchemaGenerator.CreateTable"/>'s output for the
    /// <see cref="PlanCorrectionCollector"/> catalog entry (prefix NOT NULL, payload nullable), which
    /// <c>DarlingPlanCorrectionMigrationTests</c> asserts rather than leaving to this comment — the one
    /// thing a hand-written literal can get wrong that a generated body cannot. No <c>v_*</c>
    /// passthrough view (a post-V14 collector); the viewer reads the base table directly. Hypertable /
    /// compression / 30-day retention flow from the catalog at runtime.
    /// </summary>
    private const string V46Sql = @"
CREATE TABLE IF NOT EXISTS collect.plan_correction (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    force_last_good_plan_desired_state text,
    force_last_good_plan_actual_state text,
    force_last_good_plan_reason text,
    create_index_actual_state text,
    drop_index_actual_state text,
    recommendation_name text,
    recommendation_type text,
    recommendation_state text,
    recommendation_state_reason text,
    recommendation_reason text,
    valid_since timestamp,
    last_refresh timestamp,
    score integer,
    query_id bigint,
    query_text text,
    regressed_plan_id bigint,
    last_good_plan_id bigint,
    last_good_plan_forcing_type text,
    last_good_plan_is_forced boolean,
    last_good_plan_force_failure_reason text,
    regressed_plan_execution_count bigint,
    regressed_plan_cpu_time_average_ms double precision,
    regressed_plan_error_count bigint,
    last_good_plan_execution_count bigint,
    last_good_plan_cpu_time_average_ms double precision,
    last_good_plan_error_count bigint,
    estimated_gain_seconds double precision,
    is_executable_action boolean,
    is_revertable_action boolean,
    execute_action_initiated_by text,
    execute_action_initiated_time timestamp,
    execute_action_start_time timestamp,
    execute_action_duration_seconds double precision,
    revert_action_initiated_by text,
    revert_action_initiated_time timestamp,
    revert_action_start_time timestamp,
    revert_action_duration_seconds double precision,
    implementation_script text
);

CREATE INDEX IF NOT EXISTS idx_plan_correction_time ON collect.plan_correction(server_id, collection_time);";

    /// <summary>
    /// V29 — the <c>long_query_completions</c> collector table (#1496): long-running query completions
    /// (rpc_completed / sql_batch_completed >= duration threshold, plus attention/cancels) from a
    /// dedicated, OPT-IN XE ring-buffer session. Added additively exactly like V21/V24/V25 — a fresh
    /// store already has it (V1's <see cref="PgSchemaGenerator.GenerateFullSchema"/> walks the collector
    /// and V8 moved it to <c>collect</c>), so <c>CREATE TABLE IF NOT EXISTS</c> is a no-op on fresh and
    /// the real create on upgrade. Column order/types are exactly <see cref="PgSchemaGenerator.CreateTable"/>'s
    /// output for the <see cref="LongQueryCompletionsCollector"/> catalog entry (prefix NOT NULL, payload
    /// nullable). No <c>v_*</c> passthrough view (a post-V14 collector); the viewer reads the base table
    /// directly. Hypertable / compression / 30-day retention flow from the catalog at runtime.
    /// </summary>
    private const string V29Sql = @"
CREATE TABLE IF NOT EXISTS collect.long_query_completions (
    long_query_completion_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    event_time timestamp,
    event_type text,
    database_id integer,
    database_name text,
    session_id integer,
    client_app_name text,
    client_pid integer,
    nt_username text,
    server_principal_name text,
    query_hash text,
    event_sequence bigint,
    duration_microseconds bigint,
    cpu_time_microseconds bigint,
    physical_reads bigint,
    logical_reads bigint,
    writes bigint,
    row_count bigint,
    result text,
    statement_text text,
    object_name text
);

CREATE INDEX IF NOT EXISTS idx_long_query_completions_time ON collect.long_query_completions(server_id, collection_time);";

    /// <summary>
    /// V30 — the web dashboard's live toggle (#1562): config_service gains web_enabled + web_port, the twins of
    /// mcp_enabled/mcp_port, so the viewer's Settings toggle drives the second Kestrel host through the same
    /// config_version reload beacon (the BEFORE-UPDATE self-bump trigger already covers the new columns). Additive
    /// ALTERs, schema-qualified config.* exactly like V18 (the migrate session's search_path resolves a bare name
    /// to collect — the wrong schema/ACL); IF NOT EXISTS matches the file's ALTER idiom so a re-run is a no-op. Both
    /// columns are NOT NULL with the shipped defaults (off / 5153) so the single seeded row comes up honoring them.
    /// config_service has no v_* passthrough view, so nothing to refresh. Role grants are table-wide SELECT for
    /// viewer/mcp and table-wide writes for admin (config_service is NOT column-carved in DarlingManagedRoles), so
    /// the new columns inherit the grants automatically — no grant change needed.
    /// </summary>
    private const string V30Sql = @"
ALTER TABLE config.config_service ADD COLUMN IF NOT EXISTS web_enabled boolean NOT NULL DEFAULT FALSE;
ALTER TABLE config.config_service ADD COLUMN IF NOT EXISTS web_port integer NOT NULL DEFAULT 5153;";

    /// <summary>
    /// V31 — the web dashboard's user-authored CUSTOM VIEWS store (#1563): the saved dashboard/notebook
    /// definitions the web renderer composes from the existing read allowlist. One row per named view; the
    /// <c>definition</c> is the view JSON (<c>{panels:[{read,params,viz,span,...}]}</c>) validated app-side
    /// before storage. A NEW config-plane table (the web renderer reads it; the Viewer's composer writes it),
    /// added additively exactly like V17's control-plane tables.
    ///
    /// <para><b>Schema — <c>config</c>, qualified.</b> V31 CREATEs directly in <c>config</c>, and the migrate
    /// session runs under <c>search_path = collect, config, public</c> (<see cref="PgSchemaGenerator.SearchPath"/>),
    /// so an UNQUALIFIED <c>CREATE TABLE custom_views</c> would resolve to <c>collect</c> (first in the path) —
    /// the wrong schema and the wrong ACL (config is the admin-writable surface). Qualifying <c>config.</c> pins
    /// it into the admin-writable schema, mirroring V17/V18/V30. <c>id</c> is <c>GENERATED ALWAYS AS IDENTITY</c>
    /// (not <c>serial</c>) so INSERTs need no sequence USAGE grant — the same reasoning as V17's
    /// <c>config_command</c>. <c>name</c> is UNIQUE (a duplicate insert/rename surfaces as a 409 to the caller);
    /// <c>definition</c> is <c>jsonb NOT NULL</c> (the engine rejects malformed JSON as a second line of defense
    /// behind the app-side validator). Timestamps store naive-UTC (<c>now() AT TIME ZONE 'UTC'</c>) to match the
    /// store-wide convention (Npgsql rejects Kind=Utc against a bare <c>timestamp</c>).
    ///
    /// <para><b>Versioned = an IN-PLACE <c>version</c> column + optimistic concurrency</b> (the store's UPDATE
    /// carries <c>WHERE id = $ AND version = $expected</c>; a 0-rowcount is a stale-write 409), NOT append-only
    /// history rows — a <c>custom_view_history</c> audit trail is a clean later add if it is ever wanted.</para>
    ///
    /// <para><b>NO <c>config_bump_version</c> trigger</b> (deliberately, unlike V17's four desired-state tables):
    /// custom_views feeds the WEB RENDERER, never the collector/service loop, so a write here has nothing for the
    /// service to reload — bumping the <c>config_version</c> reload beacon would only trigger a needless service
    /// reload on every view save. The write grant for the least-privilege viewer role (the web dashboard's
    /// identity) is an EXPLICIT single-table grant in <see cref="!:DarlingManagedRoles.BuildProvisioningSql"/>
    /// (and BYO <c>tools/provision-roles.sql</c>), NOT an <c>ALTER DEFAULT PRIVILEGES</c> widening — custom_views
    /// has no secret columns, so it needs no <c>ViewerRestrictedConfigTables</c> carve.</para>
    /// </summary>
    private const string V31Sql = @"
CREATE TABLE IF NOT EXISTS config.custom_views (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL UNIQUE,
    definition jsonb NOT NULL,
    description text,
    version integer NOT NULL DEFAULT 1,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_by text
);";

    /// <summary>
    /// V32 — the Viewer's fleet TAGS, the user-authored visual organization of a large server list
    /// (~100 servers at the motivating field site). Two NEW config-plane tables, added additively exactly
    /// like V31: <c>server_tags</c> is the tag tree, <c>server_tag_map</c> is the many-to-many assignment.
    ///
    /// <para><b>Tags, not exclusive groups.</b> A server may carry any number of tags, so the composite
    /// PK <c>(server_id, tag_id)</c> on the map is the whole membership rule — deliberately NOT a unique
    /// constraint on <c>server_id</c>, which is what an exclusive-group model would have needed.</para>
    ///
    /// <para><b>Membership lives in its own table, never as a column on
    /// <c>config_monitored_servers</c>.</b> A column there would have required all four of: an edit to the
    /// fail-closed <c>ViewerRestrictedConfigTables</c> column ACL (an unlisted column is invisible, which
    /// breaks the read-only viewer's ENTIRE sidebar read, not just the new field); a matching hand edit to
    /// BYO <c>tools/provision-roles.sql</c>; a silent widening of the network-reachable <c>mcp</c> role,
    /// whose INSERT/UPDATE/DELETE on that table is TABLE-level and so would cover a new column it cannot
    /// even SELECT (a blind write); and — because <c>trg_bump_monitored_servers</c> is a STATEMENT-level
    /// trigger — a full service <c>ReloadFromStoreAsync</c> per tag write, up to one per server on a bulk
    /// tag of 100. A side table avoids all four. It also survives the server-identity move
    /// (<c>server_id</c> is derived from host+database+read_only_intent, so editing any of those
    /// upserts-new-then-deletes-old), which a column on the server row would not.</para>
    ///
    /// <para><b>Nesting.</b> <c>parent_id</c> is a self-reference; NULL = a root tag. Depth is capped
    /// app-side at 4 levels — Postgres cannot express that without a trigger, and the tag table is tiny
    /// (dozens of rows), so the Viewer loads it whole, builds the tree in memory, and checks both the cap
    /// and cycle-freedom there. No recursive CTE, no ltree extension, no closure table.
    /// <c>ON DELETE CASCADE</c> on the self-reference means deleting a tag removes its whole subtree and
    /// (via the map's own cascade) those assignments — the folder mental model. It can never reach
    /// <c>config_monitored_servers</c>: there is no FK to it, so no server row, credential blob, or
    /// collected history is touched. The Viewer still warns with the descendant + assignment counts.</para>
    ///
    /// <para><b>Uniqueness is per-parent</b> so <c>prod/primaries</c> and <c>staging/primaries</c> coexist.
    /// It is a UNIQUE INDEX over <c>COALESCE(parent_id, 0)</c> rather than a plain <c>UNIQUE
    /// (parent_id, name)</c>, because Postgres treats NULLs as DISTINCT — two ROOT tags both named
    /// 'Prod' would otherwise both be accepted. Identity starts at 1, so 0 is never a real id.
    /// <c>lower(name)</c> makes it case-insensitive, matching how the rest of the product treats
    /// operator-entered names.</para>
    ///
    /// <para><b>NO <c>config_bump_version</c> trigger</b>, same reasoning as V31: tags feed the VIEWER's
    /// sidebar, never the collector/service loop, so there is nothing for the service to reload and a
    /// beacon bump would only cost a needless fleet reconcile. <c>id</c> is
    /// <c>GENERATED ALWAYS AS IDENTITY</c> so INSERTs need no sequence USAGE grant. No explicit grant is
    /// added: both tables are picked up by the blanket <c>GRANT ... ON ALL TABLES IN SCHEMA config</c>
    /// statements that provisioning re-runs on EVERY service start. Note it is those, not
    /// <c>ALTER DEFAULT PRIVILEGES</c>, that cover a table introduced by a migration — ADP only applies to
    /// objects created after it runs, and provisioning runs AFTER the migration pass. No
    /// <c>ViewerRestrictedConfigTables</c> carve is needed either, because neither table has a secret
    /// column — but note the corollary: a table in <c>config</c> is readable by the network-reachable
    /// <c>mcp</c> role by default, so if tag names may carry customer identifiers the REVOKE must be
    /// emitted inside provisioning AFTER that blanket grant (and mirrored into BYO
    /// <c>tools/provision-roles.sql</c>), or the next service start silently re-grants it.</para>
    /// </summary>
    private const string V32Sql = @"
CREATE TABLE IF NOT EXISTS config.server_tags (
    id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    parent_id integer REFERENCES config.server_tags(id) ON DELETE CASCADE,
    sort_order integer NOT NULL DEFAULT 0,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_server_tags_parent_name
    ON config.server_tags (COALESCE(parent_id, 0), lower(name));

CREATE TABLE IF NOT EXISTS config.server_tag_map (
    server_id integer NOT NULL,
    tag_id integer NOT NULL REFERENCES config.server_tags(id) ON DELETE CASCADE,
    PRIMARY KEY (server_id, tag_id)
);

CREATE INDEX IF NOT EXISTS idx_server_tag_map_tag
    ON config.server_tag_map (tag_id);";

    /// <summary>
    /// V33 — the #1659 connection-alert opt-ins on the singleton config_alert_settings row: announce a
    /// server already down at first sight, and re-announce a standing outage every N minutes (0 = off).
    /// Both default OFF, preserving the classic edge-only behavior byte-for-byte. ADD COLUMN IF NOT EXISTS
    /// keeps the migration idempotent; NOT NULL DEFAULT means a pre-V33 row reads correctly with no backfill.
    /// No ACL/provisioning change: config_alert_settings carries table-level grants (no column carve).
    /// </summary>
    private const string V33Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS notify_connection_down_at_startup boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS connection_refire_minutes integer NOT NULL DEFAULT 0;";

    /// <summary>
    /// V34 — the two Availability Group collector tables (#991): <c>ag_replica_states</c> (replica grain)
    /// and <c>ag_database_replica_states</c> (database grain, carrying the send/redo queues, rates and
    /// secondary lag). Added additively exactly like V29 — a fresh store already has both (V1's
    /// <see cref="PgSchemaGenerator.GenerateFullSchema"/> walks the collector catalog), so
    /// <c>CREATE TABLE IF NOT EXISTS</c> is a no-op on fresh and the real create on upgrade. Column
    /// order/types are exactly <see cref="PgSchemaGenerator.CreateTable"/>'s output for the
    /// <see cref="AgReplicaStatesCollector"/> / <see cref="AgDatabaseReplicaStatesCollector"/> catalog
    /// entries (prefix NOT NULL, payload nullable). No <c>v_*</c> passthrough views (post-V14 collectors);
    /// readers use the base tables. Hypertable / compression / retention flow from the catalog at runtime.
    /// </summary>
    private const string V34Sql = @"
CREATE TABLE IF NOT EXISTS collect.ag_replica_states (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    ag_name text,
    replica_server_name text,
    role_desc text,
    operational_state_desc text,
    connected_state_desc text,
    recovery_health_desc text,
    synchronization_health_desc text,
    availability_mode_desc text,
    failover_mode_desc text,
    endpoint_url text
);

CREATE INDEX IF NOT EXISTS idx_ag_replica_states_time ON collect.ag_replica_states(server_id, collection_time);

CREATE TABLE IF NOT EXISTS collect.ag_database_replica_states (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    ag_name text,
    database_name text,
    replica_server_name text,
    is_local boolean,
    synchronization_state_desc text,
    last_hardened_lsn text,
    last_commit_lsn text,
    log_send_queue_size bigint,
    redo_queue_size bigint,
    log_send_rate bigint,
    redo_rate bigint,
    is_suspended boolean,
    suspend_reason_desc text,
    availability_mode_desc text,
    secondary_lag_seconds bigint
);

CREATE INDEX IF NOT EXISTS idx_ag_database_replica_states_time ON collect.ag_database_replica_states(server_id, collection_time);";

    /// <summary>
    /// V35 — the #991 Availability Group alert knobs on the singleton config_alert_settings row: the master
    /// switch for the AG alert family plus the two "AG Sync Fell Behind" triggers. The lag trigger ships ON at
    /// 300 seconds (a secondary five minutes behind is worth knowing about on any AG); the redo-queue trigger
    /// ships OFF at 0, because a healthy queue size is workload-specific and a shipped guess would page half
    /// the fleet. Same shape as V33: ADD COLUMN IF NOT EXISTS keeps it idempotent, and NOT NULL DEFAULT means a
    /// pre-V35 row reads correctly with no backfill. No ACL/provisioning change — config_alert_settings carries
    /// table-level grants (no column carve).
    /// </summary>
    private const string V35Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS notify_ag_health boolean NOT NULL DEFAULT true,
    ADD COLUMN IF NOT EXISTS ag_lag_alert_seconds integer NOT NULL DEFAULT 300,
    ADD COLUMN IF NOT EXISTS ag_redo_queue_alert_kb bigint NOT NULL DEFAULT 0;";

    /// <summary>
    /// V36 — the AG latency columns (#991 addendum): the four commit/hardened/redone/received timestamps
    /// the reference project's query skips, plus the two server-computed drain-time estimates
    /// (queue ÷ rate ÷ 60, guarded against BIGINT integer division and a zero rate).
    /// <para>APPENDED, never inserted, and deliberately a SEPARATE migration rather than an edit to V34:
    /// V34's <c>CREATE TABLE IF NOT EXISTS</c> is a no-op on a store that already ran it, so widening V34
    /// in place would silently leave every already-migrated store (the field box included) six columns
    /// short while fresh installs got them — the exact drift the fresh-vs-upgraded shape pin exists to
    /// catch. <c>ADD COLUMN IF NOT EXISTS</c> appends physically, matching the order
    /// <see cref="PgSchemaGenerator.CreateTable"/> emits for a fresh store, so both provenances end up
    /// column-for-column identical (pinned by PgSchemaGeneratorTests, which reconstructs the current
    /// shape from V34 + V36 and compares it to the generator).</para>
    /// <para>Version 36 because 35 was taken by the concurrent AG-alerts work; both are now on dev, so the
    /// ladder is dense again.</para>
    /// </summary>
    private const string V36Sql = @"
ALTER TABLE collect.ag_database_replica_states
    ADD COLUMN IF NOT EXISTS last_commit_time timestamp,
    ADD COLUMN IF NOT EXISTS last_hardened_time timestamp,
    ADD COLUMN IF NOT EXISTS last_redone_time timestamp,
    ADD COLUMN IF NOT EXISTS last_received_time timestamp,
    ADD COLUMN IF NOT EXISTS est_redo_completion_time_min double precision,
    ADD COLUMN IF NOT EXISTS est_send_drain_time_min double precision;";

    /// <summary>
    /// V37 — two additive columns for #1696, in ONE migration because they ship together.
    ///
    /// <para><c>ag_replica_states.is_local</c> marks the row describing the replica the collector was
    /// connected to. Every replica in an AG is visible from every node, so a fully-monitored 3-node AG
    /// collects the same replica's state three times and previously reported one failover three times. The
    /// alert path uses this to pick one node's view. NULLABLE, unlike the settings column below: rows
    /// collected before this migration genuinely do not know, and a NULL must read as "unknown" rather than
    /// as "not local" — de-duplicating on a false negative would drop a real alert. The de-dup treats a
    /// snapshot with no known-local row as un-de-duplicable and keeps every row, which is the safe direction.</para>
    ///
    /// <para><c>config_alert_settings.ag_disconnect_refire_minutes</c> is the #1659 re-fire treatment for
    /// "AG Replica Disconnected", which until now was a pure edge: a replica that stayed disconnected for a
    /// week announced it once. NOT NULL DEFAULT 0 = off, so the shipped behavior is byte-for-byte the old
    /// edge-only one and nothing starts re-alerting on upgrade.</para>
    ///
    /// ADD COLUMN IF NOT EXISTS throughout, per the file's additive idiom; config.-qualified because the
    /// migrate session runs under search_path = collect, config, public.
    /// </summary>
    private const string V37Sql = @"
ALTER TABLE collect.ag_replica_states
    ADD COLUMN IF NOT EXISTS is_local boolean;

ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS ag_disconnect_refire_minutes integer NOT NULL DEFAULT 0;";

    /// <summary>
    /// V39 — the partial indexes behind the dimension GC's MEASURED bound (#1795). The GC prunes
    /// dimension content against the oldest SURVIVING digest-carrying fact row rather than an assumed
    /// horizon, which requires a <c>min(collection_time)</c> probe per dim-feeding table every sweep.
    /// Unindexed, that is an ordered walk from each hypertable's oldest chunk filtering out NULL-digest
    /// rows (every pre-#1767 row); with a partial index whose predicate EXACTLY matches the probe's
    /// WHERE clause, it is an index-edge read. One index per dim-feeding fact table, predicate =
    /// "carries any digest" (the OR of that table's digest columns from <c>PayloadDimensions.All</c> —
    /// pinned against the map by test, since a new dimension column would silently fall out of both the
    /// index and the probe together or neither). TimescaleDB propagates the index to existing and
    /// future chunks; <c>IF NOT EXISTS</c> keeps the migration idempotent; plain-PostgreSQL stores take
    /// the same DDL unchanged. Bare names resolve through the migrate session's
    /// <c>search_path = collect, config, public</c>.
    /// </summary>
    private const string V39Sql = @"
CREATE INDEX IF NOT EXISTS ix_query_stats_digest_floor
    ON query_stats (collection_time)
    WHERE query_text_digest IS NOT NULL OR query_plan_digest IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_procedure_stats_digest_floor
    ON procedure_stats (collection_time)
    WHERE query_plan_digest IS NOT NULL;";

    /// <summary>
    /// V40 — <c>config_alert_settings.blocking_wait_seconds_threshold</c>, the #1839 total-blocked-wait
    /// gate. A second, independent blocking threshold beside the existing count one, because a count
    /// cannot distinguish one session blocked for an hour from one blocked for a second.
    /// <para>NOT NULL DEFAULT 0 = OFF, so an upgraded store alerts byte-for-byte as before and nobody
    /// starts getting a new alert because they took an update — the same shipped-behavior-preserving
    /// choice V37's re-fire column made. <c>ADD COLUMN IF NOT EXISTS</c> per the file's additive idiom;
    /// config.-qualified because the migrate session runs under
    /// <c>search_path = collect, config, public</c>.</para>
    /// </summary>
    private const string V40Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS blocking_wait_seconds_threshold integer NOT NULL DEFAULT 0;";

    /// <summary>
    /// V41 — the REAL Query Store interval identity on <c>query_store_stats</c> (#1841 tier 2):
    /// <c>runtime_stats_interval_id</c> (<c>sys.query_store_runtime_stats</c>' own interval key) and
    /// <c>interval_start_time_utc</c> (<c>sys.query_store_runtime_stats_interval.start_time</c>, converted
    /// to UTC at collection).
    /// <para>Query Store rows are CUMULATIVE per-interval snapshots and the collector re-fetches the OPEN
    /// interval every cycle, so every aggregate read must collapse an interval to its latest snapshot
    /// before summing. Tier 1 (#1845) did that on the <c>first_execution_time</c> PROXY because the schema
    /// exposed no interval key; this is the real one, and it is also the only honest x-axis for "when the
    /// work ran" — <c>collection_time</c> dates an interval to the cycle that last FETCHED it, one bucket
    /// late on Query Store's default 60-minute interval.</para>
    /// <para>Both nullable and appended (identical physical column order for the binary COPY whether fresh —
    /// V1 is generated from the current collector definition, which now includes them — or upgraded;
    /// <c>ADD COLUMN IF NOT EXISTS</c> no-ops on fresh). Deliberately NOT backfilled: rows already stored
    /// were collected without the identity and nothing can reconstruct it, so readers key on the real id
    /// only when present and fall back to the tier-1 proxy otherwise. The trailing
    /// <c>CREATE OR REPLACE VIEW</c> re-expands <c>v_query_store_stats</c>' pinned <c>SELECT *</c>
    /// (Postgres freezes it at CREATE; append-only ADDs keep the refresh legal), mirroring V15/V27/V28.
    /// Runs after V8, so the bare names resolve through <c>search_path = collect, config, public</c>.</para>
    /// </summary>
    private const string V41Sql = @"
ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS runtime_stats_interval_id bigint;
ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS interval_start_time_utc timestamp;
CREATE OR REPLACE VIEW v_query_store_stats AS SELECT * FROM query_store_stats;";

    /// <summary>
    /// V42 — PagerDuty webhook channel (#1943). Adds two columns to <c>config.config_notification</c>:
    /// <c>pagerduty_routing_key</c> (the 32-character Events API v2 integration key, stored as plaintext
    /// but column-REVOKEd from the read-only viewer role — same secret tier as Teams/Slack/Generic webhook
    /// URLs) and <c>pagerduty_use_eu_region</c> (boolean flag for EU data center endpoint). Both are
    /// non-null with safe defaults (empty string / false) so existing rows get valid values without a data
    /// migration. The routing key is carved from the viewer role's SELECT grant in
    /// <c>DarlingManagedRoles.ViewerRestrictedConfigTables</c> and <c>Darling/tools/provision-roles.sql</c>;
    /// the EU flag is non-secret and stays in the viewer's grant.
    /// </summary>
    private const string V42Sql = @"
ALTER TABLE config.config_notification
    ADD COLUMN IF NOT EXISTS pagerduty_routing_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS pagerduty_use_eu_region boolean NOT NULL DEFAULT FALSE;";

    /// <summary>
    /// V43 - PagerDuty proxy (#1945). The channel was the only webhook that could not route through a
    /// proxy, and its endpoints are fixed public URLs, so a locked-down network that proxies webhook
    /// egress could not use it at all. Non-secret like the sibling proxy columns (teams_proxy et al.),
    /// so it stays in the viewer role's SELECT grant.
    /// </summary>
    private const string V43Sql = @"
ALTER TABLE config.config_notification
    ADD COLUMN IF NOT EXISTS pagerduty_proxy text NOT NULL DEFAULT '';";

    /// <summary>
    /// V44 — per-server collector state that is NOT derivable from the collected rows, so it cannot be a
    /// MAX() over the collector's own table the way the <c>event_time</c> / <c>instance_id</c> watermarks
    /// are (#1962). One collector declares state today: <c>default_trace_events</c> records the trace FILE
    /// it read, and compares it next cycle to decide whether it can read only the current rollover file
    /// (the measured 5.0x steady-state saving) or must re-read the whole set because the trace rolled.
    /// The state cannot ride on the payload precisely because the cycles that need it most collect zero
    /// rows — a server whose default trace churns through 20 MB files without producing any CURATED event
    /// would never record a rollover, and so would never leave the expensive fallback.
    ///
    /// <para><b>Schema — <c>collect</c>, qualified.</b> Service-written state the operator never mutates,
    /// so it belongs in <c>collect</c> beside <c>analysis_state</c> (V19) rather than the <c>config</c>
    /// control plane; qualifying is belt-and-suspenders over the migrate session's
    /// <c>search_path = collect, config, public</c>, exactly like V19. NOT a hypertable: it is a keyed
    /// registry with one short row per (server, collector, key), and
    /// <see cref="!:TimescaleSupport.HypertableTables"/> is catalog-driven, so a non-collector table is
    /// excluded automatically. No <c>v_*</c> passthrough view — nothing outside the collector runner reads
    /// it. <c>CREATE TABLE IF NOT EXISTS</c>, so a re-run is a no-op; a fresh store reaches it in
    /// migration order like every other post-V8 addition. Lite's twin is <c>Schema.CreateCollectorStateTable</c>
    /// — same columns, same key.</para>
    /// </summary>
    private const string V44Sql = @"
CREATE TABLE IF NOT EXISTS collect.collector_state (
    server_id integer NOT NULL,
    collector_name text NOT NULL,
    state_key text NOT NULL,
    state_value text NOT NULL,
    updated_at timestamp NOT NULL,
    PRIMARY KEY (server_id, collector_name, state_key)
);";

    /// <summary>
    /// V47 — the #1951 ADR persistent version store table for stores that already exist. A fresh store
    /// gets this table from V1's <see cref="PgSchemaGenerator.GenerateFullSchema"/> (catalog-driven, so
    /// registering <c>PvsStatsCollector</c> in <c>CollectorCatalog.All</c> is the whole fresh-install
    /// story); an UPGRADED store ran V1 before the collector existed and would otherwise never get the
    /// table, so it is spelled out here column-for-column in the generator's emission order. The
    /// fresh-vs-upgraded shape pin in PgSchemaGeneratorTests compares the two, which is what keeps this
    /// block honest if the payload ever changes.
    ///
    /// <para>Column types are the generator's mapping of the definition's 23 <c>PayloadColumns</c>: Varchar →
    /// <c>text</c>, Integer → <c>integer</c>, SmallInt → <c>smallint</c>, BigInt → <c>bigint</c>, Boolean →
    /// <c>boolean</c>, Timestamp → <c>timestamp</c>, Decimal(19,2) → <c>numeric(19,2)</c>, behind the four
    /// standard prefix columns. Bare names resolve through the migrate session's
    /// <c>search_path = collect, config, public</c> (V8); the table is <c>collect</c>-qualified anyway,
    /// matching V44 and V34.</para>
    ///
    /// <para>The view statement is deliberately UNQUALIFIED while the table is <c>collect.</c>-qualified,
    /// which looks inconsistent and is not: it resolves through the migrate session's
    /// <c>search_path = collect, config, public</c> exactly like V10-V13's view statements, and the
    /// drift guard in DarlingObservabilityTests scans every migration for the bare
    /// <c>CREATE OR REPLACE VIEW v_x AS SELECT * FROM x</c> form. A <c>collect.</c>-qualified view would
    /// be INVISIBLE to that guard — the guard that exists so a collector view can never be added without
    /// its V14 refresh — and would drop out of <c>PgSchemaGenerator.AllPassthroughViews</c>, which the MCP
    /// reader consults to answer "does this table have a v_* view".</para>
    ///
    /// <para>The <c>v_pvs_stats</c> passthrough view is what keeps the two viewers' SQL byte-identical.
    /// Lite's <c>v_*</c> views are load-bearing there — they UNION the hot DuckDB table with the parquet
    /// archive — and Darling's are the passthrough twin created for exactly that reason (V4/V5:
    /// "completing the v_* twin of Lite's DuckDB view layer so every ported viewer query stays
    /// byte-identical to Lite's"). Without it the Darling FinOps read would have to name the base table
    /// and the two front ends would diverge in their first line. The AG collectors read base tables
    /// directly and are the exception, not the pattern to copy for a grid twinned with Lite's.</para>
    ///
    /// <para>Everything else is automatic and deliberately NOT written here: the hypertable conversion,
    /// compression policy and retention come from <see cref="!:TimescaleSupport"/>, which enumerates
    /// <c>CollectorCatalog.All</c>, so a new collector table is converted on the next service start
    /// without a hand-written <c>create_hypertable</c> — the same path ag_replica_states took in V34.</para>
    /// </summary>
    private const string V47Sql = @"
CREATE TABLE IF NOT EXISTS collect.pvs_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    database_id integer,
    is_accelerated_database_recovery_on boolean,
    pvs_filegroup_id smallint,
    persistent_version_store_size_mb numeric(19,2),
    online_index_version_store_size_mb numeric(19,2),
    database_data_size_mb numeric(19,2),
    current_aborted_transaction_count bigint,
    oldest_active_transaction_id bigint,
    oldest_aborted_transaction_id bigint,
    min_transaction_timestamp bigint,
    online_index_min_transaction_timestamp bigint,
    secondary_low_water_mark bigint,
    offrow_version_cleaner_start_time timestamp,
    offrow_version_cleaner_end_time timestamp,
    aborted_version_cleaner_start_time timestamp,
    aborted_version_cleaner_end_time timestamp,
    pvs_off_row_page_skipped_low_water_mark bigint,
    pvs_off_row_page_skipped_transaction_not_cleaned bigint,
    pvs_off_row_page_skipped_oldest_active_xdesid bigint,
    pvs_off_row_page_skipped_min_useful_xts bigint,
    pvs_off_row_page_skipped_oldest_snapshot bigint,
    pvs_off_row_page_skipped_oldest_aborted_xdesid bigint
);

CREATE INDEX IF NOT EXISTS idx_pvs_stats_time ON collect.pvs_stats(server_id, collection_time);

CREATE OR REPLACE VIEW v_pvs_stats AS SELECT * FROM pvs_stats;";

    /// <summary>
    /// V48 — the #1984 PVS-pressure alert knobs on the singleton config_alert_settings row: an enable,
    /// the percent-of-database trigger, and the GB floor qualifier (AND semantics — see AlertsConfig).
    /// <para>Defaults ON at 40% / 1 GB, matching darling.json's <c>AlertsConfig</c> defaults — the
    /// #754 shipped-on precedent for a brand-new percent-based alert, NOT V40's default-off (that was a
    /// second gate on an EXISTING alert, where a non-zero default would have changed behavior someone had
    /// already tuned). 40 warns meaningfully before MS's "close to 50% of the database size" = large.
    /// <c>ADD COLUMN IF NOT EXISTS</c> per the file's additive idiom; config.-qualified because the
    /// migrate session runs under <c>search_path = collect, config, public</c>.</para>
    /// </summary>
    private const string V48Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS pvs_enabled boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS pvs_threshold_percent integer NOT NULL DEFAULT 40;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS pvs_floor_gb integer NOT NULL DEFAULT 1;";

    /// <summary>
    /// V49 — the database-state alert. Two tables plus a settings column:
    /// <para><c>collect.database_states</c> — the periodic per-database <c>state_desc</c> time series
    /// the alert compares against. Added additively exactly like V34/V44: a fresh store already has it
    /// (V1's <see cref="PgSchemaGenerator.GenerateFullSchema"/> walks the collector catalog, which now
    /// includes <see cref="PerformanceMonitor.Collectors.DatabaseStateCollector"/>), so
    /// <c>CREATE TABLE IF NOT EXISTS</c> is a no-op on fresh and the real create on upgrade. Column
    /// order/types are exactly <see cref="PgSchemaGenerator.CreateTable"/>'s output for that catalog
    /// entry (prefix NOT NULL, payload nullable) so the fresh-vs-upgraded shape pin holds. It gets the
    /// default retrieval index. No <c>v_*</c> passthrough view (post-V14 collectors read the base table).</para>
    /// <para><c>config.database_state_expected</c> — the per-(server, database) expected state the
    /// baseline-deviation alert compares the current state against: auto-seeded from first observation
    /// (non-critical states only — a critical first observation stays pending and alerts) and
    /// user-editable (the override), with the <c>(ignore)</c> sentinel opting a database out. Lives in the
    /// config control plane; the viewer role's SELECT grant is added in
    /// <c>Darling/tools/provision-roles.sql</c> / <c>DarlingManagedRoles</c>.</para>
    /// <para><c>config.config_alert_settings.database_state_enabled</c> — the master toggle, NOT NULL
    /// DEFAULT true so an upgraded store enables it without a data migration.</para>
    /// </summary>
    private const string V49Sql = @"
CREATE TABLE IF NOT EXISTS collect.database_states (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    database_id integer,
    state_desc text,
    is_in_standby boolean
);

CREATE INDEX IF NOT EXISTS idx_database_states_time ON collect.database_states(server_id, collection_time);

CREATE TABLE IF NOT EXISTS config.database_state_expected (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    expected_state text NOT NULL,
    is_user_override boolean NOT NULL DEFAULT false,
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (server_id, database_name)
);

ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS database_state_enabled boolean NOT NULL DEFAULT true;";

    /// <summary>
    /// V50 — a nullable colour for each server tag (#2008 stage 2a). Additive exactly like V33/V48: one
    /// <c>ADD COLUMN IF NOT EXISTS</c> on <c>config.server_tags</c> (created back in V32), so it is a no-op
    /// on a store already carrying it and the real add on an upgrade. There is no config-schema generator
    /// for server_tags — the table exists only because V32 created it — so a fresh store reaches this column
    /// by running V32 then V50 in order, the same path an upgraded store takes; nothing else needs editing.
    /// <para>Deliberately nullable with NO backfill: existing tags stay NULL and render as a neutral pill
    /// until a user picks a colour, while newly-created tags get a palette colour assigned at creation time
    /// (rotated by tag id, in the viewer). Stored as <c>#RRGGBB</c> text — the viewer's only concern, the
    /// service never reads server_tags — so no CHECK constraint is imposed here; the viewer writes only
    /// palette values or a user pick.</para>
    /// </summary>
    private const string V50Sql = @"
ALTER TABLE config.server_tags
    ADD COLUMN IF NOT EXISTS colour text;";

    /// <summary>
    /// V51 — the statement's HOST OBJECT on query_stats (#2012 stage 2): <c>sys.dm_exec_sql_text.objectid</c>
    /// resolved to schema.name at collection, NULL for ad-hoc/prepared text. This is what lets the
    /// hash-grouped readers split <c>INSERT...EXEC</c> callers that share a <c>query_hash</c> (the hash
    /// normalizes the callee away — reproduced and mis-attributed in live triage) while leaving the ad-hoc
    /// literal-collapse behavior untouched (NULLs group as one). Appended LAST to match the collector's
    /// append-only payload; nullable, no backfill — history rows stay NULL and read as "unknown host",
    /// aging out with raw retention. TimescaleDB accepts a nullable ADD COLUMN on a compressed hypertable.
    /// <para><c>v_query_stats</c> is NOT a passthrough on a V38+ store — it is the #1767 payload-RESOLVING
    /// view (<see cref="PgSchemaGenerator.GenerateQueryStatsResolvingView"/>), so it must be rebuilt from
    /// the generator, not replaced with <c>SELECT *</c> (which would silently return NULL query_text for
    /// every digest-era row). And because the generator emits payload columns BEFORE the trailing digest
    /// columns, the new column lands mid-list — an alteration <c>CREATE OR REPLACE VIEW</c> refuses
    /// (append-at-end only) — hence DROP + recreate. Plain DROP, no CASCADE: nothing persistent depends
    /// on the view; readers reference it per-query. The migration entry concatenates the regenerated view
    /// after this constant, the V38 idiom, so the definition can never go stale here.</para>
    /// </summary>
    private const string V51Sql = @"
ALTER TABLE query_stats
    ADD COLUMN IF NOT EXISTS host_object_name text;
DROP VIEW IF EXISTS v_query_stats;";

    /// <summary>
    /// V52 — the persisted finding drill-down (#2060): the evidence rows behind a finding
    /// (parameter-sensitive plans, top spill queries, blocking chains) previously existed only on
    /// the write path, so <c>get_analysis_findings</c> could say "13 plans showed the pattern"
    /// while no surface could enumerate them. Additive nullable text column carrying the CAPPED
    /// JSON (<c>DrillDownSerializer</c>: rows-per-section + total-size caps, explicit truncation
    /// note — never a silent cap), written beside <c>remediation_action_json</c> with the same D2
    /// rationale and read back into the finding. No backfill: pre-upgrade findings honestly return
    /// no drill-down and age out with finding retention. A fresh store's V4 creates the table
    /// without the column and this ALTER adds it later in the same ladder run — the V9 idiom.
    /// </summary>
    private const string V52Sql = @"
ALTER TABLE analysis_findings
    ADD COLUMN IF NOT EXISTS drill_down_json text;";

    /// <summary>
    /// V53 — the store self-metrics table (#2068): the hourly fleet-level sweep
    /// (<see cref="StoreSelfMetrics"/>) persists the store's OWN size/compression/growth series here — one
    /// row per hypertable (total / pre- / post-compression bytes, chunk count), one per payload dimension
    /// table (total bytes, row count), and one whole-store summary row (pg_database_size + the
    /// enabled-server count) per run — so capacity forecasting is a stored query instead of ad-hoc
    /// archaeology over a chunk catalog whose raw window is 4 days.
    /// <para>Deliberately a PLAIN table, and deliberately NOT a collector: it is not in
    /// <c>CollectorCatalog.All</c>, so <see cref="TimescaleSupport"/>'s catalog-driven hypertable
    /// conversion and DarlingRetention's catalog purge can never recurse onto the table that measures them
    /// (pinned by test). At ~30 narrow rows/hour it needs neither chunks nor compression; its retention is
    /// the sweep's own bounded DELETE (<see cref="StoreSelfMetrics.RetentionDays"/> days), no policy
    /// machinery. No <c>v_*</c> passthrough view — not a collector table, nothing twins with Lite (a
    /// single-server edition has no central store to measure). Fresh stores get the table from this
    /// migration too (V1's generator walks the collector catalog, which this is not in — the V49
    /// <c>config.database_state_expected</c> precedent), so fresh and upgraded stores take the same path.
    /// <c>collect.</c>-qualified like V44/V47/V49; the (metric_time) index serves both the read surface's
    /// windowed scans and the retention DELETE.</para>
    /// </summary>

    private const string V53Sql = @"
CREATE TABLE IF NOT EXISTS collect.store_metrics (
    metric_time timestamp NOT NULL,
    object_name text NOT NULL,
    object_kind text NOT NULL,
    total_bytes bigint,
    compressed_before_bytes bigint,
    compressed_after_bytes bigint,
    chunk_count integer,
    row_count bigint,
    enabled_server_count integer
);

CREATE INDEX IF NOT EXISTS idx_store_metrics_time ON collect.store_metrics(metric_time);";

    /// <summary>
    /// V54 — gzip-compressed plan-dimension content (#2069). The plan dim was 101 GB (69%) of the
    /// production store under lz4 TOAST (8.9× on plan XML); app-level gzip measured 14.0× on the
    /// same live content, and PG 18 has no zstd TOAST to do it in-engine. Additive bytea column:
    /// new rows carry gzip bytes and NULL text, pre-upgrade rows keep text, readers take
    /// gz-else-text and inflate in C#, and the dim's own GC turnover (~9 days) converts the store
    /// with no rewrite. The existing V51-era text column becomes effectively nullable-by-use; the
    /// NOT NULL constraint is dropped so gz-only rows can insert. The resolving view is re-emitted
    /// from the generator with the gz column APPENDED (a legal CREATE OR REPLACE — additions at the
    /// end only), and V38's generated body pre-adds the column for stores upgrading from below it.
    /// </summary>
    private const string V54Sql = @"
ALTER TABLE query_plan_dim
    ADD COLUMN IF NOT EXISTS query_plan_gz bytea;
ALTER TABLE query_plan_dim
    ALTER COLUMN query_plan_xml DROP NOT NULL;";

    /// <summary>
    /// V55 — the #2107 alert-threshold knobs, all previously compile-time constants: the store
    /// volume's self-alert warning percent, the Collection Stopped staleness window and
    /// consecutive-failure fast path, the low-disk CRITICAL severity tier's two floors (#1136 —
    /// these grade the shared target-volume alert, not just the self-alert), and the analysis
    /// notification cooldown Lite already passed through while Darling hardcoded 360. Defaults are
    /// the constants they replace, NOT NULL so pre-V55 rows read cleanly at the appended ordinals.
    /// </summary>
    private const string V55Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS self_disk_free_warn_percent integer NOT NULL DEFAULT 10;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS collection_stale_minutes integer NOT NULL DEFAULT 30;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS collection_failure_threshold integer NOT NULL DEFAULT 10;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS disk_critical_free_percent integer NOT NULL DEFAULT 3;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS disk_critical_free_gb integer NOT NULL DEFAULT 2;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS analysis_notify_cooldown_minutes integer NOT NULL DEFAULT 360;";

    /// <summary>
    /// V56 — background-job telemetry columns on the #2068 self-metrics series (#2136): the store's own
    /// TimescaleDB background jobs (CAGG refreshes, compression, retention) are its heaviest recurring
    /// work — measured on the production store, the four most expensive jobs are all the
    /// query_store_stats family (compression 157s, interval_hourly refresh 96s) — and their runtimes
    /// scale serially with raw volume, so an onboarding wave moves them first. Job rows ride the same
    /// hourly sweep under <c>object_kind = 'background_job'</c>. All nullable, appended (the V55/#1984
    /// ordinal rule); non-job rows simply leave them NULL.
    /// </summary>
    private const string V56Sql = @"
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS last_run_duration_ms bigint;
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS schedule_interval_ms bigint;
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS total_runs bigint;
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS total_failures bigint;";

    /// <summary>
    /// V57 — the Store Job Over Cadence warning knob (#2136, the alert half of the V56 job telemetry):
    /// a background job whose last run reaches this percent of its own schedule interval fires the
    /// Warning tier of the new self-alert (the Critical tier is fixed at 100 — a job outrunning its
    /// cadence is compounding refresh lag, which is the failure the telemetry exists to catch).
    /// Store-backed like the V55 knobs (#2107 pattern): the column is the control plane, the C# default
    /// remains only the shipped seed. Default 25: the production 52-server store's worst job runs at
    /// ~7% of cadence, so 25 sits 3.5x above the observed ceiling but far ahead of real compounding.
    /// </summary>
    private const string V57Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS store_job_cadence_warn_percent integer NOT NULL DEFAULT 25;";

    /// <summary>
    /// V58 — the Query Store backfill off switch (#2167): a service-wide toggle the worker's backfill loop
    /// reads live (store reload, no restart), because the #2058 backfill previously ran unconditionally —
    /// during the 2026-08-10 consolidation a freshly restored catalog put it into sustained 64MB drains
    /// against a cross-region production primary with no way to stop it short of gutting plan capture
    /// fleet-wide. Default TRUE preserves today's behavior; the column rides <c>config_service</c> so the
    /// existing config_version trigger makes a flip visible to the service's next reload poll.
    /// </summary>
    private const string V58Sql = @"
ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS query_store_backfill_enabled boolean NOT NULL DEFAULT TRUE;";

    /// <summary>
    /// V59 — the two collector memory knobs that were compile-time constants (#2164 + #2170). They ride
    /// ONE rung deliberately: peak transient memory is approximately
    /// <c>max_concurrent_sweeps × query_store_text_budget_mb</c>, so an operator who moves one needs the
    /// other in front of them, and shipping them together keeps the documented product of the two honest.
    ///
    /// <para>Defaults reproduce today's hardcoded behavior exactly (64 MB budget from
    /// QueryStoreCollector.MaxTextBytesPerDatabase, 4-wide sweep from the #1553 gate), so an upgraded
    /// store changes nothing until someone turns a dial. Both are clamped on READ (budget [4,256] MB,
    /// sweeps [1,16]) rather than by CHECK constraints, matching the sibling knobs' posture: a bad value
    /// degrades to a sane one instead of failing the service's config load.</para>
    /// </summary>
    private const string V59Sql = @"
ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS query_store_text_budget_mb integer NOT NULL DEFAULT 64;
ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS max_concurrent_sweeps integer NOT NULL DEFAULT 4;";

    /// <summary>
    /// V60 — restart-surviving edge memory for the database-state alert (#2166). Two nullable columns on
    /// <c>config.database_state_expected</c>, which is already keyed per (server, database) and already
    /// exists for this alert, so the memory lives beside the config it belongs with rather than in a new
    /// table or smuggled into <c>config_edge_trigger_watermarks</c>' metric_name (that column feeds alert
    /// history and mute matching; a compound key hidden in a label is a trap).
    ///
    /// <para>Why it must persist: the reporter's case is a database deliberately parked OFFLINE for a
    /// month. Edge-triggering on in-memory state would re-fire every parked database on every service
    /// restart — worse than the cooldown-repeat it replaces. NULL means never alerted, so an upgraded
    /// store's first evaluation fires once per deviating database and then goes quiet.</para>
    /// </summary>
    private const string V60Sql = @"
ALTER TABLE config.database_state_expected
    ADD COLUMN IF NOT EXISTS last_alerted_state text;
ALTER TABLE config.database_state_expected
    ADD COLUMN IF NOT EXISTS last_alerted_at timestamp;";

    /// <summary>
    /// V61 — the monotonic per-fingerprint occurrence counters (#2216). The rolling-window count that rides
    /// on an alert incident is a GAUGE: it rises as events arrive and falls as they age out of the groupers'
    /// read window, so a consumer that only sees throttled deliveries (one per #1154 per-fingerprint
    /// cooldown) cannot recover how many events actually happened between two of them. This table is the
    /// accumulator's memory, keyed by the #1140 dedup fingerprint.
    ///
    /// <para>A NEW table rather than columns on <c>config_edge_trigger_watermarks</c>, for two independent
    /// reasons. The key is wrong: watermarks are per (server, metric) while occurrences are per (server,
    /// metric, FINGERPRINT) — a deadlock on one table and a deadlock on another are separate incidents with
    /// separate totals, and folding them into one row would report their sum under both. And the cross-store
    /// twin cannot take the columns: Lite writes that same row with <c>INSERT OR REPLACE</c> and a PARTIAL
    /// column list, so any column added there is silently reset to its default every time an alert fires —
    /// the counter would zero itself precisely when it was being read.</para>
    ///
    /// <para><c>config</c> schema because it joins the alert-coordination family (the V8 remarks put the
    /// edge-trigger watermarks there for the same reason): service-written, operator-visible, keyed by
    /// server. Schema-qualified per the V17 rule — the migrate session's search_path would otherwise resolve
    /// a bare name into <c>collect</c>. No per-table grant (provisioning re-runs
    /// <c>GRANT … ON ALL TABLES IN SCHEMA config</c> after the migration pass) and no
    /// <c>ViewerRestrictedConfigTables</c> carve: the only identity stored is the fingerprint HASH, never
    /// the involved object names it was computed from, so there is nothing here for the network-reachable
    /// <c>mcp</c> role to read that it should not.</para>
    ///
    /// <para>NO <c>config_bump_version</c> trigger, per the V32 precedent: this is the service's own
    /// coordination state, written on the alert path, and nothing reloads on it. A beacon bump would make
    /// every delivered alert trigger a needless fleet reconcile.</para>
    ///
    /// <para><c>last_observed_at</c> is not display data — it is what makes a row's staleness decidable. The
    /// service deletes a fingerprint's row when its incident ends, but a host that dies mid-incident leaves
    /// one behind, and a stranded row trusted on the fingerprint's NEXT incident would decay its
    /// already-counted mark to the new window count, read the recurrence as nothing new, and report a stale
    /// total under a stale start time. The accumulator therefore ignores rows older than its read window.
    /// Row growth needs no separate GC: each delivery REPLACES the set for its (server, metric), so the
    /// table holds the live fingerprints plus whatever a crash stranded until that metric next fires.</para>
    /// </summary>
    private const string V61Sql = @"
CREATE TABLE IF NOT EXISTS config.incident_occurrences (
    server_id integer NOT NULL,
    metric_name text NOT NULL,
    dedup_key text NOT NULL,
    total_occurrences bigint NOT NULL,
    observed_window_count integer NOT NULL,
    incident_started_at timestamp NOT NULL,
    last_observed_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (server_id, metric_name, dedup_key)
);";

    /// <summary>
    /// V62 — the #2171 plan-XML codec knob for direct-SQL store consumers. 'gzip' (default) keeps
    /// today's write path; 'none' makes the dim writer store plain text in query_plan_xml (lz4 TOAST
    /// compresses, ~8.9x measured vs gzip's 14.0x) so Grafana-class readers get plans back with plain
    /// SQL — PostgreSQL exposes no inflate, so gzip bytes are unreadable without an untrusted-language
    /// UDF, which is the contract failure #2171 reports. Rides config_service like V58/V59 so the
    /// config_version trigger makes a flip visible to the next reload poll. The CHECK mirrors the
    /// provider's normalization; both fail toward 'gzip'. Rides directly above #2216's V61 — the
    /// merge-order gate this PR carried (never land 62 over a vacant 61; ascent-only applier) was
    /// satisfied when that rung merged; the #2227 density pin now enforces the rule mechanically.
    /// </summary>
    private const string V62Sql = @"
ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS plan_xml_compression text NOT NULL DEFAULT 'gzip';
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'config_service_plan_xml_compression_check'
    ) THEN
        ALTER TABLE config.config_service
            ADD CONSTRAINT config_service_plan_xml_compression_check
            CHECK (plan_xml_compression IN ('gzip', 'none'));
    END IF;
END $$;";

    /// <summary>
    /// V63 — <c>pg_wait_stats</c>, the first PostgreSQL collector table: cumulative Aurora wait
    /// counters with deltas computed on write, the Postgres counterpart of <c>wait_stats</c>.
    /// <para>Columns are spelled out here in the generator's exact emission order (the four standard
    /// prefix columns, then <see cref="PerformanceMonitor.Collectors.PgWaitStatsCollector"/>'s payload
    /// in its declared order) so a fresh store — where V1 generates this from the catalog — and an
    /// upgraded store, where this rung creates it, end up with an identical physical column order for
    /// the binary COPY. No PRIMARY KEY, and the <c>(server_id, collection_time)</c> index, per the
    /// convention every collector table follows.</para>
    /// <para><c>wait_time_us</c> is MICROSECONDS. The AWS documentation contradicts itself on the unit
    /// — microseconds for <c>aurora_stat_system_waits</c>, milliseconds for
    /// <c>aurora_stat_backend_waits</c> — so it was settled by measurement instead: read as
    /// milliseconds, the observed totals imply tens of thousands of concurrently waiting sessions
    /// against a <c>max_connections</c> of 5,000, which is impossible. The name carries the unit so a
    /// reader never has to relitigate it.</para>
    /// <para>Both the numeric id and the decoded name are stored. The name is what an operator reads,
    /// but the id is the stable key: wait-event name casing differs between Aurora majors
    /// (<c>AutoVacuumMain</c> on 16.11 versus <c>AutovacuumMain</c> on 17.7), so anything keyed on the
    /// name breaks its own history across an upgrade. Nullable because the type/event lookups are LEFT
    /// JOINed — an event Aurora reports but does not name is still recorded.</para>
    /// </summary>
    private const string V63Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_wait_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    wait_type_id integer,
    wait_event_id bigint,
    wait_type text,
    wait_event text,
    waits bigint,
    wait_time_us bigint,
    delta_waits bigint,
    delta_wait_time_us bigint
);

CREATE INDEX IF NOT EXISTS idx_pg_wait_stats_time
    ON collect.pg_wait_stats(server_id, collection_time);";

    /// <summary>
    /// V64 — <c>pg_statement_stats</c>, per-query-shape execution statistics from Aurora's extended
    /// <c>aurora_stat_statements()</c>: the Postgres counterpart of <c>query_stats</c>.
    /// <para>Two column groups exist nowhere on the SQL Server side. The Aurora I/O source split
    /// (<c>storage_blks_read</c> / <c>orcache_blks_hit</c> and their times) decomposes what is otherwise
    /// an opaque block read into "came from the storage volume" versus "hit the local NVMe tier" — which
    /// is why a cache-hit ratio computed the community way is arithmetically misleading on Aurora. And
    /// <c>total_exec_peakmem_bytes</c> / <c>max_exec_peakmem_bytes</c> are the nearest thing PostgreSQL
    /// has to memory-grant data, which core PostgreSQL has no concept of at all. Per-query
    /// <c>wal_bytes</c> likewise has no SQL Server DMV equivalent.</para>
    /// <para><b>No query text column, deliberately.</b> Text belongs in the shared
    /// <c>query_text_dim</c> rather than inline — inline payload was 94% of a 250 GB field store — but
    /// registering a new dim-feeding table cannot be done from a rung this late: V38 is GENERATED from
    /// <c>PayloadDimensions.All</c>, so adding an entry makes V38 emit
    /// <c>ALTER TABLE pg_statement_stats ADD COLUMN query_text_digest</c>, and on an upgraded store V38
    /// runs long before this rung creates the table — the ALTER would hit a nonexistent table and fail
    /// the entire migration. Retrofitting a dim-feeding table therefore needs either a
    /// existence-guarded V38 or a rung-aware dimension registry, which is a design change and not a
    /// drive-by. Until then <c>queryid</c> is the identity, which is the join key anyway, and text
    /// arrives with a dedicated low-cadence text collector that stores each statement once instead of
    /// once per snapshot.</para>
    /// </summary>
    private const string V64Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_statement_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    queryid bigint,
    database_id bigint,
    user_id bigint,
    toplevel boolean,
    calls bigint,
    total_exec_time_ms double precision,
    min_exec_time_ms double precision,
    max_exec_time_ms double precision,
    mean_exec_time_ms double precision,
    rows_returned bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    shared_blks_dirtied bigint,
    shared_blks_written bigint,
    temp_blks_read bigint,
    temp_blks_written bigint,
    blk_read_time_ms double precision,
    blk_write_time_ms double precision,
    storage_blks_read bigint,
    orcache_blks_hit bigint,
    storage_blk_read_time_ms double precision,
    orcache_blk_read_time_ms double precision,
    wal_records bigint,
    wal_fpi bigint,
    wal_bytes bigint,
    total_exec_peakmem_bytes bigint,
    max_exec_peakmem_bytes bigint,
    delta_calls bigint,
    delta_total_exec_time_ms bigint,
    delta_rows bigint
);

CREATE INDEX IF NOT EXISTS idx_pg_statement_stats_time
    ON collect.pg_statement_stats(server_id, collection_time);";

    /// <summary>
    /// V65 — <c>pg_wraparound_stats</c>: transaction id and MultiXact id freeze headroom per database.
    /// <para>Both counters are stored, because they are independent and each is separately fatal.
    /// MultiXact exhaustion is the one almost nobody monitors: ids are consumed when a row is locked by
    /// several transactions at once, so a <c>SELECT FOR UPDATE</c>-heavy or foreign-key-heavy workload
    /// burns them much faster than plain transaction ids, and a server can look comfortable on XID age
    /// while being in trouble on MultiXact age.</para>
    /// <para>The percentages are STORED rather than derived on read because their denominators are
    /// per-server settings. Recomputing later against whatever <c>autovacuum_freeze_max_age</c> happens
    /// to be then would silently rewrite history the moment someone tunes it; a stored percentage stays
    /// true to the configuration in force when it was measured.</para>
    /// <para>The first PostgreSQL collector table that is not Aurora-specific — it reads only core
    /// catalog surfaces, so it populates on any PostgreSQL target.</para>
    /// </summary>
    private const string V65Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_wraparound_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    frozen_xid_age bigint,
    min_multixid_age bigint,
    autovacuum_freeze_max_age bigint,
    autovacuum_multixact_freeze_max_age bigint,
    pct_toward_emergency_vacuum double precision,
    pct_toward_wraparound double precision,
    pct_toward_multixact_emergency double precision,
    pct_toward_multixact_wraparound double precision,
    xids_remaining bigint,
    multixids_remaining bigint,
    allows_connections boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_wraparound_stats_time
    ON collect.pg_wraparound_stats(server_id, collection_time);";

    /// <summary>
    /// V66 — <c>pg_xmin_horizon</c>: what is holding back the xmin horizon, attributed by cause.
    /// <para>Four unrelated causes produce an identical picture — dead tuples accumulate, autovacuum
    /// runs and reports success, nothing shrinks — and the fix differs completely for each: kill a
    /// session, drop a replication slot, disable standby feedback, or resolve an orphaned prepared
    /// transaction. That is why this table stores one row per SOURCE with the oldest holder for that
    /// source, plus an <c>is_winner</c> flag, rather than a single horizon age. Attribution is the whole
    /// value; an aggregate would leave a reader exactly where they started.</para>
    /// <para><c>is_winner</c> is stamped at collection rather than derived on read, so a stored row names
    /// the winner as of the moment it was measured — deriving it later would depend on which rows a
    /// query happened to select, and a filtered read could crown a holder that never held the horizon.</para>
    /// <para>Zero rows is the HEALTHY state and must never be read as a collection failure. Note also
    /// that <c>standby_feedback</c> is expected to be absent on Aurora, whose replicas read the same
    /// storage volume instead of streaming WAL.</para>
    /// </summary>
    private const string V66Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_xmin_horizon (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    source text,
    xmin_age bigint,
    holder text,
    detail text,
    is_winner boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_xmin_horizon_time
    ON collect.pg_xmin_horizon(server_id, collection_time);";

    /// <summary>
    /// V67 — <c>collect.pg_replication_slot_stats</c>: slot state, including the two independent ways an abandoned
    /// slot can take a server down.
    /// <para><c>retained_wal_bytes</c> is the disk-exhaustion measure and is COMPUTED rather than read,
    /// because the column that would answer it directly — <c>safe_wal_size</c> — is NULL whenever
    /// <c>max_slot_wal_keep_size</c> is <c>-1</c>, which is the default. Reading only that column would
    /// mean reporting nothing on a stock server, exactly where retention is unbounded. <c>-1</c> is
    /// stored as the not-applicable sentinel so a consumer cannot mistake "no limit configured" for "no
    /// data collected".</para>
    /// <para><c>inactive_since</c> and <c>invalidation_reason</c> are PostgreSQL 17+ and
    /// <c>conflicting</c> is 16+; on older majors the collector substitutes NULL/false so the table shape
    /// stays constant across a mixed-version fleet and a chart does not change shape at an upgrade.
    /// <c>inactive_since</c> is the column that distinguishes a consumer between polls from a slot
    /// orphaned three weeks ago.</para>
    /// </summary>
    private const string V67Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_replication_slot_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    slot_name text,
    slot_type text,
    plugin text,
    database_name text,
    is_active boolean,
    active_pid bigint,
    is_temporary boolean,
    two_phase boolean,
    wal_status text,
    safe_wal_size_bytes bigint,
    retained_wal_bytes bigint,
    xmin_age bigint,
    catalog_xmin_age bigint,
    inactive_since timestamp,
    invalidation_reason text,
    conflicting boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_replication_slot_stats_time
    ON collect.pg_replication_slot_stats(server_id, collection_time);";

    /// <summary>
    /// V68 — <c>collect.pg_autovacuum_stats</c>, per-table autovacuum state, and the first PostgreSQL
    /// collector on the per-database fan-out path.
    /// <para>The threshold columns are what make the table worth having. Dead-tuple counts alone are not
    /// actionable — autovacuum fires at <c>autovacuum_vacuum_threshold + scale_factor * reltuples</c>, so
    /// the same count is routine on a large table and urgent on a small one. The collector computes each
    /// table's OWN threshold, honouring per-table <c>reloptions</c> overrides rather than only the GUCs,
    /// because those overrides are common on exactly the big hot tables where the global default is
    /// wrong.</para>
    /// <para><c>inserts_since_vacuum</c> / <c>insert_vacuum_threshold</c> are PostgreSQL 13+ and carry
    /// <c>-1</c> on older majors so the table shape stays constant across a mixed-version fleet. They
    /// cover the append-only case, which has no dead tuples at all and is therefore invisible to the
    /// dead-tuple rule — and an append-only table that is never vacuumed is never frozen either.</para>
    /// <para><c>database_name</c> comes from the per-database loop's connection rather than the result
    /// set: <c>pg_stat_user_tables</c> shows only the connected database, so the connection IS the
    /// authoritative answer. Additive and view-less exactly like V63–V67 — a fresh store gets the table
    /// from V1's generated schema, and this rung is what an already-existing store gets.</para>
    /// </summary>
    private const string V68Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_autovacuum_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    live_tuples bigint,
    dead_tuples bigint,
    mods_since_analyze bigint,
    inserts_since_vacuum bigint,
    vacuum_threshold bigint,
    insert_vacuum_threshold bigint,
    analyze_threshold bigint,
    autovacuum_disabled boolean,
    total_bytes bigint,
    last_vacuum timestamp,
    last_autovacuum timestamp,
    last_analyze timestamp,
    last_autoanalyze timestamp,
    vacuum_count bigint,
    autovacuum_count bigint,
    analyze_count bigint,
    autoanalyze_count bigint
);

CREATE INDEX IF NOT EXISTS idx_pg_autovacuum_stats_time
    ON collect.pg_autovacuum_stats(server_id, collection_time);";

    /// <summary>
    /// V69 — <c>collect.pg_io_stats</c>, I/O attributed to a (backend_type, object, context) triple rather
    /// than to a file, from <c>pg_stat_io</c> (PostgreSQL 16+).
    /// <para>Every counter column is NULLABLE and that is load-bearing, not incidental. PostgreSQL uses
    /// NULL for "this counter does not apply to this combination" — the checkpointer performs no reads,
    /// <c>bulkread</c> never extends, the <c>normal</c> context has no ring buffer to reuse — and on Aurora
    /// the entire write side is NULL because backends there do not write data files. A NOT NULL column with
    /// a 0 default would claim measurements that were never taken, and a consumer averaging write latency
    /// would divide by them.</para>
    /// <para>Cumulative counters stored raw, with the windowed change computed at read time. Additive and
    /// view-less exactly like V63-V68: a fresh store gets the table from V1's generated schema, and this
    /// rung is what an already-existing store gets.</para>
    /// </summary>
    private const string V69Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_io_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    backend_type text,
    object_type text,
    context text,
    reads bigint,
    read_time_ms double precision,
    writes bigint,
    write_time_ms double precision,
    writebacks bigint,
    writeback_time_ms double precision,
    extends bigint,
    extend_time_ms double precision,
    op_bytes bigint,
    hits bigint,
    evictions bigint,
    reuses bigint,
    fsyncs bigint,
    fsync_time_ms double precision,
    stats_reset timestamp,
    read_bytes numeric(28,0),
    write_bytes numeric(28,0),
    extend_bytes numeric(28,0)
);

CREATE INDEX IF NOT EXISTS idx_pg_io_stats_time
    ON collect.pg_io_stats(server_id, collection_time);";

    /// <summary>
    /// V70 — <c>config.config_monitored_servers.engine</c> and <c>.port</c>, the two columns without which a
    /// PostgreSQL target cannot survive its own registration.
    /// <para>The registry is store-authoritative after the first seed: darling.json seeds it once, and from
    /// then on the worker's server list comes from this table. Every other <c>MonitoredServer</c> field had a
    /// column here; these two did not, so a PostgreSQL entry round-tripped through the store as
    /// <c>"sqlserver"</c> on the driver's default port (both property defaults) and the service then opened a
    /// <c>SqlConnection</c> to it. That happens on the FIRST start, not a later one, because the seed is
    /// immediately followed by the load that replaces the file's list with the store's.</para>
    /// <para>Both defaults are what make this safe on an existing store. Every row already there is a SQL
    /// Server target, and <c>port</c> is consumed only by the PostgreSQL connection builder (the SQL Server
    /// path carries a port in the host string), so <c>0</c> means "the driver's default" exactly as the
    /// property does. The SQL-Server-only writers — the Viewer's Add / Manage Servers dialogs — keep
    /// inserting without naming either column and keep meaning the same thing.</para>
    /// </summary>
    private const string V70Sql = @"
ALTER TABLE config.config_monitored_servers
    ADD COLUMN IF NOT EXISTS engine text NOT NULL DEFAULT 'sqlserver';

ALTER TABLE config.config_monitored_servers
    ADD COLUMN IF NOT EXISTS port integer NOT NULL DEFAULT 0;";

    /// <summary>
    /// V71 — <c>collect.pg_blocking_edges</c>: who is blocked, by whom, and what state each side was in.
    /// <para>An EDGE LIST, which is why the table is named for edges rather than for chains. One row per
    /// (blocked, blocking) pair, so a chain of four is four rows and a blocker with thirty victims is thirty.
    /// Storing a rendered tree instead would bake in one traversal and make root-blocker, depth, and fan-out
    /// queries string work; from edges they are ordinary SQL.</para>
    /// <para>Both sides carry their own state because the remedy depends on it: a chain rooted in
    /// <c>idle in transaction</c> is an application defect, one rooted in a long-running query is a tuning
    /// problem, and the pid alone does not distinguish them. That doubling of columns is the point of the
    /// table.</para>
    /// <para><b>Reader beware — sparse by design.</b> This table is empty on a healthy instance, and unlike
    /// SQL Server's <c>blocked_process_report</c> there is no engine-side recorder behind it: PostgreSQL
    /// materialises nothing unless something asks, so a gap means "not sampled", not "not blocked". A count
    /// over this table measures how often blocking was CAUGHT.</para>
    /// <para>Additive and view-less exactly like V63-V69: a fresh store gets the table from V1's generated
    /// schema, and this rung is what an already-existing store gets.</para>
    /// </summary>
    private const string V71Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_blocking_edges (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    blocked_backend_id bigint,
    blocked_pid integer,
    blocking_backend_id bigint,
    blocking_pid integer,
    database_name text,
    blocked_username text,
    blocked_application_name text,
    blocked_client_addr text,
    blocked_state text,
    blocked_wait_event_type text,
    blocked_wait_event text,
    blocked_query text,
    blocked_xact_duration_ms bigint,
    blocked_query_duration_ms bigint,
    blocking_username text,
    blocking_application_name text,
    blocking_client_addr text,
    blocking_state text,
    blocking_wait_event_type text,
    blocking_wait_event text,
    blocking_query text,
    blocking_xact_duration_ms bigint,
    blocking_query_duration_ms bigint,
    blocked_pid_count integer,
    blocking_is_idle_in_transaction boolean,
    query_text_may_be_truncated boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_blocking_edges_time
    ON collect.pg_blocking_edges(server_id, collection_time);";

    /// <summary>
    /// V73 — <c>collect.pg_statement_text</c>: one row per <c>(server_id, queryid)</c> holding the statement text
    /// for a PostgreSQL target, so <c>get_pg_top_queries</c> can return something a human can read (#2219).
    ///
    /// <para><b>The gap this closes.</b> <c>pg_statement_stats</c> identifies queries by <c>queryid</c> and stores
    /// no text, because <c>aurora_stat_statements</c>'s <c>showtext</c> costs real money per collection and
    /// normalized text is highly repetitive. But <c>queryid</c> is NOT stable across a major version upgrade, so
    /// after one the stored history joins to nothing readable — a list of integers that used to be your slowest
    /// queries. Keying text on <c>(server_id, queryid)</c> is what preserves the OLD ids' text when the live view
    /// re-keys, which is the whole point: no live fetch can recover it afterwards.</para>
    ///
    /// <para><b>Inline text, not a <c>query_text_dim</c> digest, and that is a deliberate reversal of what V64's
    /// comment promised.</b> The dimension route is blocked and would stay expensive to unblock: V38 is GENERATED
    /// from <c>PayloadDimensions.All</c>, so registering <c>pg_statement_stats</c> makes V38 emit an
    /// <c>ALTER TABLE</c> against a table it has not created yet on every upgraded store, and it would also break
    /// V64's own ladder diff, which asserts each rung equals the generated schema. More importantly the dimension
    /// needs the liveness interlock <see cref="QueryStorePlanMap"/> documents at length — the GC sweeps on
    /// <c>last_seen</c> rather than counting references, so a dim row can be collected while live facts still
    /// point at it, and the failure mode is SILENTLY missing text. Inline cannot dangle. It costs cross-server
    /// dedup — one row per server per queryid rather than one per distinct text — which on a 52-server fleet of
    /// <c>pg_stat_statements.max = 5000</c> is a few hundred MB against a store whose Query Store plan XML alone
    /// measured 43 GB. Paying that to make a silent-loss mode impossible is the trade.</para>
    ///
    /// <para>Not a hypertable and not a collector table: one row per statement per server, near-static once a
    /// workload is warm, so it is dimension-shaped and pruned on <c>last_seen</c> rather than by
    /// <c>drop_chunks</c>. That also keeps it out of the generated-schema ladder diff, exactly as V72's
    /// <c>query_store_plan_map</c> is — the established shape for content keyed to facts rather than collected
    /// as facts.</para>
    ///
    /// <para><c>first_seen</c> is kept alongside <c>last_seen</c> because they answer different questions: when a
    /// statement shape first appeared on this server (which survives the upgrade re-key and is the only record of
    /// it) versus whether the text is still live enough to keep. NUMBERED <c>max(dev) + 1</c> without a gap, for
    /// the reason V72's comment gives — a gap is skipped silently on every upgraded store.</para>
    /// </summary>
    private const string V73Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_statement_text (
    server_id integer NOT NULL,
    queryid bigint NOT NULL,
    query_text text NOT NULL,
    first_seen timestamp NOT NULL,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, queryid)
);
CREATE INDEX IF NOT EXISTS idx_pg_statement_text_last_seen
    ON collect.pg_statement_text(last_seen);";

    /// <summary>
    /// V82 — the target-engine discriminator on the <c>collect.servers</c> registry (#2530). The registry
    /// recorded <c>sql_engine_edition</c> and <c>sql_major_version</c> and nothing that says a target is
    /// PostgreSQL, so a PostgreSQL server landed with edition <c>0</c> — byte-identical to a SQL Server that
    /// has never completed a connect. That is not a fact that can be inferred, and everything the
    /// PostgreSQL-parity work needs sits on top of being able to state it: the fleet card, both UIs' tab
    /// choice, and the engine-KIND half of the #2511 capability answer.
    ///
    /// <para><b>A token, not a boolean.</b> <c>is_postgres</c> loses the Aurora distinction, which the
    /// collectors already gate on (<c>aurora_stat_system_waits()</c> and the rest of the proprietary surface
    /// core PostgreSQL has in no version), and a boolean cannot grow a third value without a second column
    /// that means something only in combination with the first. The vocabulary lives in C#
    /// (<c>MonitoredEngineKind</c>) rather than in a CHECK constraint on purpose: a constraint would make
    /// every new token its own migration rung, and a store written by a NEWER service would then fail to
    /// insert rather than merely being described conservatively by an older reader.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> — like V80 and V81, and here the reason is semantic as
    /// well as operational. Defaulting to <c>'sqlserver'</c> would have every PostgreSQL target assert it is
    /// SQL Server for the window between the migration and its next connect, which is precisely the wrong
    /// claim to make by default; NULL says "no connect has stamped this yet", and the readers treat it as no
    /// claim. The registry upsert runs on every successful connect, so the column populates itself within one
    /// collection cycle for every live server without a backfill statement.</para>
    ///
    /// <para>No view refresh: <c>collect.servers</c> is a registry, not a hypertable, and has no
    /// <c>v_</c> passthrough for a <c>SELECT *</c> column list to be frozen in.</para>
    /// </summary>
    private const string V82Sql = @"
ALTER TABLE collect.servers
    ADD COLUMN IF NOT EXISTS engine_kind text;";

    /// <summary>
    /// V83 — <c>collect.pg_database_stats</c>, the <c>pg_stat_database</c> counters (#2539): temp-file
    /// spills, the buffer-cache hit ratio, deadlocks, and the commit/rollback split. Four questions nothing
    /// collected, off one cluster-wide view that needs no extension and nothing configured on the target.
    ///
    /// <para><b>Per-database rows, and the grain is the point.</b> <c>stats_reset</c> is per database
    /// (<c>pg_stat_reset()</c> resets the database it is connected to), so an aggregate would have to pick
    /// one reset timestamp for a set of rows that legitimately disagree — and a single database's reset
    /// would then corrupt the cluster's delta with nothing left in the data to say so. The full argument is
    /// on <c>PgDatabaseStatsCollector</c>.</para>
    ///
    /// <para><b>Every counter column is NULLABLE</b>, matching V69's reasoning: these are cumulative
    /// counters that get differenced at read time, and a NOT NULL 0 default would turn "not reported" into
    /// a measurement. <c>database_name</c> is nullable because PostgreSQL genuinely emits a NULL-named row
    /// for shared relations, and <c>stats_reset</c> is nullable because it is NULL until the first reset —
    /// the common case, and it means exactly "never reset".</para>
    ///
    /// <para>Additive and view-less exactly like V63-V69 and V71: a fresh store gets the table from V1's
    /// generated schema, and this rung is what an already-existing store gets. A store monitoring no
    /// PostgreSQL target carries one more empty table and nothing else changes.</para>
    /// </summary>
    private const string V83Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_database_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    xact_commit bigint,
    xact_rollback bigint,
    blks_read bigint,
    blks_hit bigint,
    temp_files bigint,
    temp_bytes bigint,
    deadlocks bigint,
    stats_reset timestamp
);

CREATE INDEX IF NOT EXISTS idx_pg_database_stats_time
    ON collect.pg_database_stats(server_id, collection_time);";

    /// <summary>
    /// V84 — <c>collect.pg_index_usage_stats</c> (#2541), the per-index scan counts, sizes and
    /// droppability facts behind <c>get_pg_index_usage</c>.
    ///
    /// <para>Most of the width is the second half of the question. <c>index_scans</c> answers "does anything
    /// use this"; <c>is_unique</c>, <c>is_primary_key</c>, <c>supports_constraint</c>,
    /// <c>is_replica_identity</c>, <c>is_partial</c>, <c>is_expression</c>, <c>is_valid</c> and
    /// <c>index_definition</c> answer "and can it therefore go", which is a different question with a much
    /// worse failure mode. They are stored rather than looked up on demand because the MCP has no ad-hoc
    /// path back to a monitored server: whatever is not captured here cannot be recovered later.</para>
    ///
    /// <para><c>last_scan</c> is nullable and NOT defaulted, because NULL is a real answer with two
    /// meanings the read distinguishes — PostgreSQL 15 and below do not record it at all, and on 16+ it is
    /// NULL for an index never scanned since the counters were reset. <c>stats_reset</c> is nullable for the
    /// ordinary reason: it is NULL until a database's statistics are first reset, which is the common state
    /// and means the counters run back to the beginning rather than that the value is unknown.</para>
    /// </summary>
    private const string V84Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_index_usage_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    index_name text,
    index_scans bigint,
    tuples_read bigint,
    tuples_fetched bigint,
    blocks_read bigint,
    blocks_hit bigint,
    index_bytes bigint,
    table_bytes bigint,
    is_unique boolean,
    is_primary_key boolean,
    is_valid boolean,
    is_ready boolean,
    is_replica_identity boolean,
    is_partial boolean,
    is_expression boolean,
    supports_constraint boolean,
    index_method text,
    column_count integer,
    index_definition text,
    last_scan timestamp,
    stats_reset timestamp
);

CREATE INDEX IF NOT EXISTS idx_pg_index_usage_stats_time
    ON collect.pg_index_usage_stats(server_id, collection_time);";

    /// <summary>
    /// V85 — <c>collect.pg_table_bloat_stats</c> (#2542), the statistics-based per-table bloat estimate and
    /// the counter-based dead-tuple pair beside it.
    ///
    /// <para><b>Three tiers of certainty share this table, and the column names carry the difference</b>
    /// rather than leaving it to a doc. <c>heap_bytes</c> / <c>toast_bytes</c> / <c>index_bytes</c> are
    /// MEASURED — <c>pg_relation_size</c> asks the filesystem. <c>live_tuples</c> / <c>dead_tuples</c> are
    /// the server's own maintained counters. <c>bloat_bytes_estimate</c> and <c>bloat_pct_estimate</c> are
    /// ARITHMETIC over column-width statistics, and are suffixed <c>_estimate</c> in the store so the
    /// qualifier cannot be lost between here and a screen.</para>
    ///
    /// <para><b><c>estimate_unavailable</c> is load-bearing, not advisory.</b> It is TRUE when the estimate
    /// has no basis — most commonly because the monitoring login lacks SELECT on the table, so
    /// <c>pg_stats</c> filtered every row out. Measured against a <c>pg_monitor</c>-only role on a live
    /// target, the estimator did not fail in that state: it reported 88.59% bloat for a table whose true
    /// figure is 0.50%. A reader that renders the percentage while ignoring this flag ships an argument for
    /// rewriting every table on the instance.</para>
    ///
    /// <para><c>estimated_tuple_bytes</c>, <c>estimated_heap_pages</c>, <c>fillfactor</c>,
    /// <c>alignment_bytes</c> and <c>mods_since_analyze</c> are the estimator's own inputs, stored so the
    /// number can be argued with rather than only believed — and <c>mods_since_analyze</c> specifically
    /// because stale width statistics are the failure that produced an 81-percentage-point error in
    /// testing, and modifications are the only way those statistics can go stale.</para>
    /// </summary>
    private const string V85Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_table_bloat_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    heap_bytes bigint,
    heap_pages bigint,
    toast_bytes bigint,
    index_bytes bigint,
    live_tuples bigint,
    dead_tuples bigint,
    mods_since_analyze bigint,
    last_analyzed timestamp,
    estimated_tuple_bytes double precision,
    estimated_heap_pages bigint,
    fillfactor integer,
    bloat_bytes_estimate bigint,
    bloat_pct_estimate numeric(5,2),
    estimate_unavailable boolean,
    alignment_bytes integer,
    pgstattuple_available boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_table_bloat_stats_time
    ON collect.pg_table_bloat_stats(server_id, collection_time);";

    /// <summary>
    /// V86 — <c>collect.pg_session_states</c> (#2540), the session side of the xmin horizon: which sessions
    /// are holding a transaction open, for how long, and whether that transaction is what is pinning it.
    ///
    /// <para><b><c>horizon_age</c> is the column this table exists for.</b> <c>pg_xmin_horizon</c> already
    /// names the CLASS of holder; this names the session and, crucially, is able to say that a session is
    /// NOT one. Measured on a live PostgreSQL 16.15 instance, two of four idle-in-transaction shapes pin
    /// nothing at all — a READ COMMITTED transaction that only read, and one whose UPDATE matched zero rows,
    /// both report NULL for <c>backend_xmin</c> and <c>backend_xid</c>. <c>horizon_age</c> is <c>-1</c>
    /// there, not <c>0</c>: 0 would read as "holding the newest possible xid", which is the opposite
    /// finding, and confusing the two is how a read ends up telling someone to kill a session that is
    /// costing them nothing.</para>
    ///
    /// <para><b>No query text column, deliberately</b>, and the absence is the design rather than an
    /// oversight. <c>pg_stat_activity.query</c> carries literal parameter values inline — verified on the
    /// live rig. <c>pg_blocking_edges</c> stores it because blocking is exceptional and the text IS the
    /// finding; this table fills on a duration floor an ordinary application can cross, so the same column
    /// would mean routinely accumulating user data to answer a question that does not need it.
    /// <c>query_id</c> is PostgreSQL's own normalised fingerprint and joins to
    /// <c>pg_statement_stats</c>, whose text is already normalised to <c>$1</c> placeholders;
    /// <c>command_tag</c> is a whitelisted SQL keyword. It is nullable for three separate reasons the read
    /// must keep apart — PostgreSQL 13 has no such column, 14+ reports NULL when <c>compute_query_id</c> is
    /// off, and a redacted row reports NULL along with everything else privileged.</para>
    ///
    /// <para><b><c>state_is_redacted</c> exists because this feature fails silently without
    /// <c>pg_monitor</c>.</b> Measured against a least-privileged role: PostgreSQL does not refuse the read,
    /// it returns every row for every backend the login does not own with all but SIX columns NULL. What
    /// survives is <c>pid</c>, <c>application_name</c>, <c>datname</c>, <c>usename</c>, <c>backend_xid</c>
    /// and <c>backend_xmin</c>; <c>state</c>, <c>state_change</c>, <c>xact_start</c>, <c>query_start</c>,
    /// <c>backend_start</c>, <c>wait_event_type</c>, <c>wait_event</c>, <c>backend_type</c>,
    /// <c>client_addr</c>, <c>leader_pid</c> and <c>query_id</c> do not, and the query text is replaced by
    /// an insufficient-privilege literal — leaving <c>backend_xmin</c> and <c>backend_xid</c> visible. So the horizon still reads as pinned and
    /// every column that could explain it is gone. On the same live capture the privileged role saw four
    /// idle-in-transaction sessions and the unprivileged one saw zero, out of the same nine backends. The
    /// flag is derived from the privilege literal rather than from a NULL state because background workers
    /// legitimately report NULL state under full privilege.</para>
    ///
    /// <para><c>total_sessions</c>, <c>active_sessions</c>, <c>idle_in_transaction_sessions</c> and
    /// <c>reportable_sessions</c> repeat per row on purpose: a read that filters to the idle-in-transaction
    /// rows still owes the reader "out of how many", and <c>reportable_sessions</c> above the stored row
    /// count is what a capture truncated by the collector's row cap looks like.</para>
    /// </summary>
    private const string V86Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_session_states (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    backend_id bigint,
    pid integer,
    database_name text,
    username text,
    application_name text,
    client_addr text,
    backend_type text,
    state text,
    wait_event_type text,
    wait_event text,
    command_tag text,
    query_id bigint,
    state_duration_ms bigint,
    xact_duration_ms bigint,
    query_duration_ms bigint,
    backend_duration_ms bigint,
    xmin_age bigint,
    xid_age bigint,
    horizon_age bigint,
    is_idle_in_transaction boolean,
    is_horizon_holder boolean,
    state_is_redacted boolean,
    total_sessions integer,
    active_sessions integer,
    idle_in_transaction_sessions integer,
    reportable_sessions integer
);

CREATE INDEX IF NOT EXISTS idx_pg_session_states_time
    ON collect.pg_session_states(server_id, collection_time);";

    /// <summary>
    /// V87 — <c>collect.pg_plan_capture_readiness</c> (#2564): whether a PostgreSQL target could capture
    /// execution plans at all, and if not, which step is missing.
    ///
    /// <para>One row PER FACET rather than one wide row per capture, because each facet has a different
    /// remedy and the whole point is to say WHICH one is missing. A wide row would collapse "the library was
    /// never loaded" and "the library is loaded and its threshold is -1" into the same shape, and those are a
    /// parameter-group change plus a reboot versus a single setting.</para>
    ///
    /// <para><c>observed</c> is TEXT and stores what the server answered verbatim, not a parsed value. GUCs
    /// render with their units on some settings and not others, and a store column typed to a number is how a
    /// collector starts failing on one major version and not another — the raw answer keeps the row honest
    /// and lets interpretation change without a migration.</para>
    ///
    /// <para><c>detail</c> is stored rather than derived on read for the same reason the collector frames it:
    /// the remedy is specific to the facet AND to the platform, and a read that reconstructed it would drift
    /// from what the collector actually observed.</para>
    /// </summary>
    private const string V87Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_plan_capture_readiness (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    facet text,
    is_satisfied boolean,
    observed text,
    detail text
);

CREATE INDEX IF NOT EXISTS idx_pg_plan_capture_readiness_time
    ON collect.pg_plan_capture_readiness(server_id, collection_time);";

    /// <summary>
    /// V88 — <c>collect.pg_write_stats</c> (#2544): the write side of the cluster — checkpoints, background
    /// writing and WAL, from <c>pg_stat_checkpointer</c>, <c>pg_stat_bgwriter</c> and <c>pg_stat_wal</c>.
    ///
    /// <para>ONE wide row, unlike every other recent PostgreSQL table here, because all three source views
    /// are cluster-wide singletons — a snapshot is genuinely one row, and splitting three views that answer
    /// one question would put the numerator and denominator of every useful ratio in different tables.</para>
    ///
    /// <para><b>The column set is the UNION across majors, and that is the point.</b> The source shape moves
    /// twice: 17 took seven of <c>pg_stat_bgwriter</c>'s eleven columns (five renamed into the new
    /// <c>pg_stat_checkpointer</c>, and <c>buffers_backend</c>/<c>buffers_backend_fsync</c> into
    /// <c>pg_stat_io</c> instead), and 18 removed the four WAL timing columns while adding
    /// <c>num_done</c>/<c>slru_written</c>. Every column is therefore NULLABLE and a major that does not
    /// supply one stores NULL, so a store holding a 16 and an 18 target means the same thing in both rows.
    /// The fleet this was written for is an even 26/26 split across the 16→17 break, so there is no majority
    /// version to write against and fix up later.</para>
    ///
    /// <para><c>wal_bytes</c> is <c>numeric(38,0)</c> and not <c>bigint</c>: upstream types it
    /// <c>numeric</c> precisely because cumulative WAL volume is allowed to exceed 2^63 over a long
    /// uptime.</para>
    ///
    /// <para>All THREE <c>stats_reset</c> stamps are stored. <c>pg_stat_reset_shared</c> takes a target, so
    /// they can be reset independently — a read differencing across a reset it could not see would report a
    /// negative interval as an enormous positive one.</para>
    /// </summary>
    private const string V88Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_write_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    num_timed bigint,
    num_requested bigint,
    num_done bigint,
    restartpoints_timed bigint,
    restartpoints_req bigint,
    restartpoints_done bigint,
    checkpoint_write_time_ms double precision,
    checkpoint_sync_time_ms double precision,
    buffers_written_checkpoint bigint,
    slru_written bigint,
    checkpointer_stats_reset timestamp,
    buffers_clean bigint,
    maxwritten_clean bigint,
    buffers_alloc bigint,
    buffers_backend bigint,
    buffers_backend_fsync bigint,
    bgwriter_stats_reset timestamp,
    wal_records bigint,
    wal_fpi bigint,
    wal_bytes numeric(38,0),
    wal_buffers_full bigint,
    wal_write bigint,
    wal_sync bigint,
    wal_write_time_ms double precision,
    wal_sync_time_ms double precision,
    wal_stats_reset timestamp
);

CREATE INDEX IF NOT EXISTS idx_pg_write_stats_time
    ON collect.pg_write_stats(server_id, collection_time);";

    /// <summary>
    /// V89 — <c>collect.pg_extension_availability</c> (#2545): which extensions this target has, could
    /// have, or cannot have — the third capability axis after engine kind and engine edition, and the only
    /// one whose answer a customer can act on.
    ///
    /// <para>Four states rather than a boolean: <c>installed</c>, <c>outdated</c> (a newer
    /// <c>default_version</c> exists, which matters because a stale extension can be missing columns a
    /// collector reads), <c>available</c> (one <c>CREATE EXTENSION</c> away — the actionable one), and
    /// <c>absent</c> (the server does not offer it at all).</para>
    ///
    /// <para><b>Scope warning carried in the column names.</b> <c>installed_version</c> is a claim about the
    /// CONNECTED DATABASE only — <c>pg_extension</c> is per-database while <c>pg_available_extensions</c> is
    /// cluster-wide, measured on one cluster reporting an extension installed in one database and not in
    /// another. <c>default_version</c> is the cluster-wide half. A read that treats the two as the same
    /// scope will report "not installed" about an extension living in the application database.</para>
    ///
    /// <para>Both version columns are TEXT and are compared for equality only, never ordered: extension
    /// versions are free-form strings and deciding that 1.10 is newer than 1.9 needs a parser this has no
    /// business carrying, when the server already names the default.</para>
    /// </summary>
    private const string V89Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_extension_availability (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    extension_name text,
    state text,
    installed_version text,
    default_version text,
    is_monitoring_relevant boolean,
    comment text
);

CREATE INDEX IF NOT EXISTS idx_pg_extension_availability_time
    ON collect.pg_extension_availability(server_id, collection_time);";

    /// <summary>
    /// V90 — <c>collect.pg_lock_stats</c> (#2544, the locks slice): lock state by mode, type and relation.
    ///
    /// <para>Does NOT duplicate <c>pg_blocking_edges</c>, which reads <c>pg_blocking_pids()</c> and stores
    /// blocked/blocker PAIRS. That answers "who is stuck behind whom" and cannot answer "what lock, in what
    /// mode, on which relation" — which is the half that decides the remedy. An ungranted
    /// <c>AccessExclusiveLock</c> is a DDL queue; <c>RowExclusiveLock</c> contention is ordinary write
    /// traffic; the pair shape is identical and the advice is opposite.</para>
    ///
    /// <para>Aggregated by <c>(database, locktype, mode, granted, relation)</c> rather than one row per
    /// lock, so the row count is bounded by CONTENTION rather than by concurrency — a busy server holds
    /// thousands of lock rows and that is not the grain anyone reasons at.</para>
    ///
    /// <para><b>Both relation columns exist because of a scope mismatch.</b> <c>pg_locks</c> is cluster-wide
    /// while <c>pg_class</c> is per-database, so a lock on a relation in another database resolves to an OID
    /// with no name. <c>relation_oid</c> is stored regardless and <c>relation_name</c> is left NULL, because
    /// dropping the row would hide real contention and naming it from the connected database's catalog would
    /// name the wrong table. <c>relation_oid</c> is <c>bigint</c>, not <c>integer</c>: OIDs are unsigned
    /// 32-bit and one past 2^31 lands negative in a signed int.</para>
    ///
    /// <para><c>oldest_wait_ms</c> is NULL on granted rows rather than 0 — zero would read as "granted
    /// instantly", which is a measurement, where NULL is the absence of one.</para>
    /// </summary>
    private const string V90Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_lock_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    lock_type text,
    mode text,
    granted boolean,
    relation_oid bigint,
    relation_name text,
    backend_count bigint,
    oldest_wait_ms double precision
);

CREATE INDEX IF NOT EXISTS idx_pg_lock_stats_time
    ON collect.pg_lock_stats(server_id, collection_time);";

    /// <summary>
    /// V91 — <c>collect.pg_column_stats</c> (#2543): the per-column planner statistics that explain WHY a
    /// plan was chosen, as opposed to what it did.
    ///
    /// <para><b>The value-bearing columns are deliberately absent.</b> <c>most_common_vals</c> and
    /// <c>histogram_bounds</c> hold raw column values — measured on a realistic table, they returned
    /// customer names, an identifier fragment and live email addresses. Collecting them would copy customer
    /// data into the monitoring store under our retention, the same exposure as the <c>auto_explain</c>
    /// literals on #2538. Only <c>most_common_freqs[1]</c> survives, as
    /// <c>top_value_frequency</c>: frequency carries the entire parameter-sensitivity signal (a top value at
    /// 0.60 means one value covers 60% of the table) and carries no value itself.
    /// <c>histogram_bounds</c> was also the largest column by bytes, so dropping both is cheaper as well as
    /// safer.</para>
    ///
    /// <para><c>n_distinct</c> is <c>double precision</c> and NOT an integer count: negatives are a RATIO of
    /// the row count, so <c>-1</c> means "distinct ≈ every row". An integer column would let a read render
    /// "-1 distinct values".</para>
    ///
    /// <para>Per-database, because <c>pg_stats</c> describes the connected database only — and its rows are
    /// filtered by <c>has_column_privilege</c>, so a monitoring role without SELECT sees nothing for a table
    /// that exists. Zero rows therefore has two causes and the read must not report either as an absence of
    /// problems.</para>
    /// </summary>
    private const string V91Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_column_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    column_name text,
    n_distinct double precision,
    null_frac double precision,
    avg_width integer,
    correlation double precision,
    top_value_frequency double precision,
    common_value_count integer
);

CREATE INDEX IF NOT EXISTS idx_pg_column_stats_time
    ON collect.pg_column_stats(server_id, collection_time);";

    /// <summary>
    /// V92 — <c>collect.pg_replication_stats</c> (#2544, the replication slice): connected standbys and how
    /// far behind each one is.
    ///
    /// <para>Distinct from <c>pg_replication_slots</c>, which records a promise to RETAIN WAL that exists
    /// whether or not anybody is attached. This is the live connection. A server can have a slot with no
    /// standby, a standby with no slot, or both.</para>
    ///
    /// <para><b>Four byte distances AND three time lags, because they answer differently.</b> Measured
    /// against a real standby holding <c>pg_wal_replay_pause()</c>: <c>sent</c>, <c>write</c> and
    /// <c>flush</c> were all ZERO while <c>replay</c> was <b>33.7 MB</b> behind — so the WAL had been
    /// shipped, written and fsynced and the fault was purely apply, which no single column would have shown.
    /// Meanwhile <c>state</c> still read <c>streaming</c>. The time lag moved to only 2.8 seconds for that
    /// same 33.7 MB, because it times the round-trip of the most recently replayed record rather than
    /// measuring the backlog; the byte distance is the proportionate one and the one to alert on.</para>
    ///
    /// <para>Every distance is measured from <c>pg_current_wal_lsn()</c> rather than from <c>sent_lsn</c>,
    /// so a sender that has itself fallen behind is visible instead of being used as the baseline.</para>
    /// </summary>
    private const string V92Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_replication_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    application_name text,
    client_addr text,
    state text,
    sync_state text,
    sync_priority integer,
    sent_bytes_behind bigint,
    write_bytes_behind bigint,
    flush_bytes_behind bigint,
    replay_bytes_behind bigint,
    write_lag_ms double precision,
    flush_lag_ms double precision,
    replay_lag_ms double precision,
    backend_start timestamp
);

CREATE INDEX IF NOT EXISTS idx_pg_replication_stats_time
    ON collect.pg_replication_stats(server_id, collection_time);";

    /// <summary>
    /// V93 — <c>collect.pg_buffer_usage</c> (#2544, the buffers slice): what is resident in shared buffers,
    /// by relation. A hit ratio says how often the pool worked; this says what is IN it.
    ///
    /// <para>Needs the <c>pg_buffercache</c> extension, so a server without it records an
    /// <c>ObjectMissing</c> outcome — now actionable, because <c>pg_extension_availability</c> (#2545)
    /// reports whether it is one <c>CREATE EXTENSION</c> away.</para>
    ///
    /// <para><b><c>relation_name</c> is NULL for two different reasons and both are honest.</b> A buffer
    /// belonging to ANOTHER database cannot be named from here — the pool is cluster-wide while
    /// <c>pg_class</c> is per-database, and a filenode from elsewhere can collide with a local OID and
    /// resolve to the WRONG name, measured. Those rows keep their database name and a NULL relation rather
    /// than being dropped, because dropping them would understate how full the pool is. Shared catalogs have
    /// no database at all, so both columns are NULL there.</para>
    ///
    /// <para>The collector joins on <c>pg_relation_filenode(oid)</c> and never on <c>oid</c> or the raw
    /// <c>relfilenode</c> column: measured, after one <c>VACUUM FULL</c> the naive OID join reported ZERO
    /// buffers for a table holding 6,667, and mapped catalogs carry <c>relfilenode = 0</c>.</para>
    ///
    /// <para><c>pool_buffers_total</c> and <c>pool_buffers_used</c> repeat on every row so a share is
    /// computable without a second read against a pool that has since moved.</para>
    /// </summary>
    private const string V93Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_buffer_usage (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    relation_name text,
    relation_kind text,
    buffers bigint,
    dirty_buffers bigint,
    avg_usage_count double precision,
    pool_buffers_total bigint,
    pool_buffers_used bigint
);

CREATE INDEX IF NOT EXISTS idx_pg_buffer_usage_time
    ON collect.pg_buffer_usage(server_id, collection_time);";

    /// <summary>
    /// V107 — the auto force-plan bot's journal (#2138 phase 1) plus the per-server opt-in column.
    ///
    /// <para><c>collect.plan_force_actions</c> is the bot's audit trail and ledger, and it is a first-class
    /// deliverable rather than logging: every decision the bot takes about writing to a monitored SQL Server
    /// — would-force (dry run), blocked-with-reasons, live force, self-review checkpoint, unforce — lands
    /// here with the evidence numbers that justified it. NOT a collector (no CollectorCatalog entry, so no
    /// generator-parity pin applies): it is written by the service's post-analysis bot pass, the
    /// store_metrics/collector_cost pattern. APPEND-ONLY by design — reviews and outcomes are their own rows
    /// pointing back via <c>related_action_id</c>, never UPDATEs, so the trail cannot be rewritten by the
    /// thing it audits. Deliberately NOT enrolled in retention: an audit of writes to production servers is
    /// the one series that should outlive the metrics that motivated it, and its volume is bounded by the
    /// bot's own cooldowns (at most one decision per query per cooldown window).</para>
    ///
    /// <para><c>reasons</c> is a comma-joined text of the named gate/blocker reasons (the same strings the
    /// MCP <c>structured_remediation</c> blockers carry) rather than <c>text[]</c>, so a future Lite twin
    /// (DuckDB) can share the exact column shape. Identity PK because review rows reference their force row;
    /// GENERATED ALWAYS so INSERTs need no sequence USAGE grant (the V64 reasoning).</para>
    ///
    /// <para><c>config.config_monitored_servers.plan_force_bot_enabled</c> is write-gate 2 of #2138's
    /// two-gate contract (gate 1 is the global <c>forcePlanBot.enabled</c> + <c>dryRun</c> pair in
    /// darling.json): a live write to a monitored server requires the global gates AND this row-level
    /// opt-in, which defaults FALSE for every existing and future row. Like <c>capture_plans</c>, it has NO
    /// darling.json counterpart on purpose — the registry is authoritative after seeding, so a file knob
    /// would be a silent no-op on every seeded box (#2254); opting a server in is a store write (viewer
    /// surface to follow), never a file edit.</para>
    /// </summary>
    private const string V107Sql = @"
CREATE TABLE IF NOT EXISTS collect.plan_force_actions
(
    action_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    action_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text NOT NULL,
    query_id bigint NOT NULL,
    plan_id bigint NOT NULL,
    action text NOT NULL,
    mode text NOT NULL,
    decision text NOT NULL,
    reasons text NOT NULL DEFAULT '',
    regression_factor numeric(19,2) NOT NULL DEFAULT 0,
    latest_cpu_per_exec_us numeric(19,2) NOT NULL DEFAULT 0,
    best_cpu_per_exec_us numeric(19,2) NOT NULL DEFAULT 0,
    replica_role text,
    parameter_sensitivity_cofired boolean NOT NULL DEFAULT FALSE,
    outcome text NOT NULL,
    detail text,
    related_action_id bigint
);

CREATE INDEX IF NOT EXISTS idx_plan_force_actions_time
    ON collect.plan_force_actions(server_id, action_time);

CREATE INDEX IF NOT EXISTS idx_plan_force_actions_query
    ON collect.plan_force_actions(server_id, database_name, query_id, action_time);

ALTER TABLE config.config_monitored_servers
    ADD COLUMN IF NOT EXISTS plan_force_bot_enabled boolean NOT NULL DEFAULT FALSE;";

    /// <summary>
    /// V108 - the server-scoped phase split on <c>collection_log</c> (#2851 made queryable).
    ///
    /// <para><b>The gap.</b> #2851 decomposes a server-scoped collector's <c>sql_duration_ms</c> into
    /// <c>open:</c> (the <c>ExecuteReaderAsync</c>) and <c>drain:</c> (the <c>ReadAsync</c> loop), and reports
    /// the store-side watermark read beside them. All of it existed ONLY as an app-log line, so the one
    /// question the split was built to answer - which phase owns a collector's cost, across servers and over
    /// time - needed an SSM session onto the box and a log scrape per server. It could not be aggregated,
    /// trended, or read through the MCP surface at all. On 2026-09-03 an investigation into where
    /// <c>procedure_stats</c>' 4,724 ms drain goes stalled on exactly that: AWS SSO began returning
    /// InternalServerException, SSM went unavailable, and the numbers were unreachable even though the store
    /// itself was answering. Three columns turn that whole class of question into a store query.</para>
    ///
    /// <para><b>Why columns and not a jsonb blob.</b> The phase set on this path is FIXED - open, drain, and
    /// the watermark - so jsonb's flexibility buys nothing and costs the cheap aggregation that is the entire
    /// point (<c>avg(sql_drain_ms)</c> against <c>avg((phases-&gt;&gt;'drain')::bigint)</c>, the latter
    /// unindexable and materially slower over a 30-day hypertable). The shape that genuinely DOES vary - the
    /// #2811 fetch split, with its per-chunk and per-id counts - is not a candidate for this table in either
    /// encoding: it is emitted once per DATABASE while <c>collection_log</c> holds one row per RUN, so it is
    /// N:1 here and needs a rollup decision of its own, exactly as the V80 fan-out did. Filed separately
    /// rather than half-answered here.</para>
    ///
    /// <para><b>Why <c>other:</c> is NOT stored.</b> It is a computed residual -
    /// <c>Math.Max(0, SqlMs - open - drain)</c> - and storing it would let it drift from the parent it is
    /// defined against, which is the one property that makes it meaningful (the terms SUM to
    /// <c>sql_duration_ms</c> by construction, so a large residual is itself the finding rather than a
    /// rounding artifact). Readers derive it the same way the C# property does. A stored residual can go
    /// stale; a derived one cannot.</para>
    ///
    /// <para><b>Why the watermark keeps its own column and no <c>sql_</c> prefix.</b> On this path it is
    /// genuinely outside <c>sql_duration_ms</c> - it runs before that stopwatch starts - so folding it into
    /// the decomposition would print a permanent zero and teach every reader that a store read #2796 clocked
    /// at 50 s cold is free. The naming carries the semantic: <c>sql_open_ms</c> and <c>sql_drain_ms</c>
    /// decompose <c>sql_duration_ms</c>; <c>watermark_ms</c> deliberately does not.</para>
    ///
    /// <para><b>No Lite twin, deliberately.</b> The two-store parity rule exists so that state added to one
    /// store does not read as permanently empty on the other, and it does not bind here because the SOURCE of
    /// these figures is Darling-only: the open/drain split is stamped by <c>DarlingCollectorRunner</c>'s
    /// server-scoped path, and Lite's <c>RemoteCollectorService</c> runner has no equivalent phase to report.
    /// A DuckDB twin would therefore be three columns that are NULL on every row Lite will ever write - which
    /// is the exact outcome the parity rule is meant to PREVENT, not produce. Said out loud here rather than
    /// left to inference, because this rung otherwise looks precisely like the shape that rule catches.</para>
    ///
    /// <para>Nullable with no DEFAULT and no backfill, the V80 reasoning exactly: a catalog-only change that
    /// stays instant on a large compressed hypertable, where adding a column WITH a default is the shape
    /// TimescaleDB has historically refused. A row written before this rung does not know its phases, and
    /// NULL says so where 0 would claim a measured instant open. All three are written together or not at
    /// all, gated on the MEASURED flag rather than on a value being non-zero - the distinction #2851 added
    /// the flag for, so a genuinely instant open records as 0 rather than vanishing.</para>
    /// </summary>
    private const string V108Sql = @"
ALTER TABLE collect.collection_log
    ADD COLUMN IF NOT EXISTS sql_open_ms integer,
    ADD COLUMN IF NOT EXISTS sql_drain_ms integer,
    ADD COLUMN IF NOT EXISTS watermark_ms integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so without this refresh the passthrough every
   read goes through would keep serving the pre-V108 column list forever and the new columns would be
   invisible to an UPGRADED store while working fine on a fresh one - the V14 lesson, and V80's too. */
CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;";

    /// <summary>
    /// V109 - what an abandoned collection cycle was DOING, not merely that it stopped (#2864).
    ///
    /// <para><b>The gap.</b> A cycle the #2673 wall-clock budget abandons records <c>ABANDONED</c> and
    /// <c>rows_collected = 0</c> - and that zero is rows STORED, which an abandoned cycle never does by
    /// definition. So the stored row could not distinguish a target that sent no rows at all from one that
    /// sent 149 and then went silent: a stalled target and a stalled stream, which want unrelated fixes.
    /// V108 made the phase split queryable and the first production capture read
    /// <c>open:104ms drain:119,945ms rows=0</c> - proving the time was in the drain and unable to say
    /// whether the drain was slow or simply empty. These columns end that.</para>
    ///
    /// <para><b><c>drain_last_read_ms</c> is the one that carries the diagnosis</b>, not the row count. A
    /// count alone still cannot separate "streaming steadily but slowly" from "delivered everything then
    /// hung" - both end at the budget with a positive count. Subtract this from <c>sql_drain_ms</c> and you
    /// have the time the reader sat with nothing arriving. NULL means no row ever arrived, which is
    /// NULL beside a 0 count says nothing came, and 0 is left free to mean what it honestly means - row 1
    /// arrived instantly.</para>
    ///
    /// <para><b>NULL means NOT RECORDED and nothing more.</b> It does NOT identify a pre-rung row, and
    /// saying so would be false in two reachable ways: an abandon firing inside <c>ExecuteReaderAsync</c>
    /// never constructs the counting reader, so all three guard to NULL on a genuine V109 row; and no
    /// per-database ENUMERATED collector sets the measured flag at all, so <c>query_store</c> and every
    /// <c>Pg*Stats</c> collector read NULL here forever on a fully current store. A dashboard that treated
    /// NULL as 'old row' would silently misclassify both an open-stall and whole collector families.</para>
    ///
    /// <para><b><c>drain_bytes_read</c> is the string payload, and the column name is the honest one.</b> No
    /// <c>DbDataReader</c> exposes wire size, so this counts UTF-16 bytes off the string and binary getters
    /// and excludes numerics and protocol framing. That is the useful scope as well as the truthful one: the
    /// collectors this rung exists for are dominated by one large text column - plan XML at a 260 KB mean -
    /// so string bytes ARE the payload to within a rounding error, and a number pretending to be the wire
    /// size would be a worse measurement wearing a better name.</para>
    ///
    /// <para><b><c>target_session_id</c> is what makes a stalled run joinable.</b> <c>waiting_tasks</c>,
    /// <c>dmv_blocking_snapshot</c> and <c>query_snapshots</c> all carry a session id, so without ours the
    /// question "what was our OWN stalled session waiting on" cannot be asked even for a window where the
    /// answering snapshot was captured. Read off the open connection as a client property, never with
    /// <c>SELECT @@SPID</c>: a round trip per collector per server per cycle is ~25,000 extra queries an
    /// hour against the fleet to learn a number the client already holds.</para>
    ///
    /// <para><b><c>sweep_peer_max_ms</c> separates the two populations automatically.</b> The slowest
    /// NON-budgeted collector already completed in the same sweep body. A genuinely large query runs beside
    /// peers at or below baseline (one measured sweep: <c>wait_stats</c> 1 ms, <c>latch_stats</c> 1 ms,
    /// alongside 71,977 ms for 12,557 rows); sweep-wide degradation shows those same light collectors at
    /// 34-47x baseline BEFORE the heavy ones burn their budget. Identical stored shape, opposite causes, and
    /// telling them apart previously meant cross-referencing neighbouring rows by hand. Budgeted collectors
    /// are excluded because they are the heavies being explained. Written on EVERY row rather than only
    /// abandoned ones, because a ratio needs a denominator and the baseline has to come from the same column
    /// on ordinary bodies - recording it only on failures would rebuild the very cross-referencing this
    /// removes.</para>
    ///
    /// <para><b>No Lite twin, and for V108's reason restated.</b> The parity rule exists so state added to
    /// one store does not read as permanently empty on the other. The source here is Darling-only: the
    /// counting reader is installed by <c>DarlingCollectorRunner</c>'s server-scoped path and the peer mark
    /// by <c>DarlingWorker</c>'s sweep body, neither of which Lite's runner has. A DuckDB twin would be five
    /// forever-NULL columns - precisely the outcome the rule prevents rather than the one it demands.</para>
    ///
    /// <para>Nullable, no DEFAULT, no backfill - the V80/V108 reasoning: a catalog-only change that stays
    /// instant on a large compressed hypertable. A row written before this rung does not know any of this,
    /// and NULL says so where 0 would claim a measured drain that delivered nothing.</para>
    /// </summary>
    private const string V109Sql = @"
ALTER TABLE collect.collection_log
    ADD COLUMN IF NOT EXISTS drain_rows_read bigint,
    ADD COLUMN IF NOT EXISTS drain_bytes_read bigint,
    ADD COLUMN IF NOT EXISTS drain_last_read_ms integer,
    ADD COLUMN IF NOT EXISTS target_session_id integer,
    ADD COLUMN IF NOT EXISTS sweep_peer_max_ms integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so without this refresh the passthrough every
   read goes through would keep serving the pre-V109 column list forever - invisible on an UPGRADED store
   while working fine on a fresh one. The V14 lesson, V80's, and V108's. */
CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;";

    /// <summary>
    /// V110 — the PER-DATABASE fetch split, summed across the fan-out and persisted on the run's row (#2860).
    /// V108's twin one path over: V108 decomposed a SERVER-scoped collector's <c>sql_duration_ms</c>, and this
    /// decomposes the deferred plan/text fetch that only the ENUMERATED path performs.
    ///
    /// <para><b>The gap.</b> #2811's sub-split reports <c>plan_fetch:Nms = probe: + target: + write: +
    /// other:</c> per database, and every bit of it lived only in an app-log line. Measured over 38.2 h on 42
    /// members, the store <c>probe:</c> is the LARGEST single term — 55.4% of <c>plan_fetch</c> and 80.6% of
    /// <c>text_fetch</c>, with the target second at 41.1% / 18.2% and <c>write:</c> a rounding error at 3.5% /
    /// 1.1%. That inverts the illustrative shape #2811/#2812 were written against, and it is the single fact
    /// most likely to be misread, so it needs to be queryable rather than scraped per server over SSM.</para>
    ///
    /// <para><b>Why SUMS, which was not one of the three options the issue offered.</b> The split is emitted
    /// once per DATABASE while <c>collection_log</c> holds one row per RUN, so it is N:1 — N &gt; 1 on 68.8% of
    /// <c>query_store</c> runs (mean 2.7 databases, max 7). The issue offered a slowest-database rollup, a
    /// per-item table, and jsonb; sums dissolve the N:1 problem instead of trading against it, because the two
    /// questions are already separated across two mechanisms. WHICH DATABASE is answered by V80's
    /// <c>fanout_item_count</c> / <c>slowest_item</c> / <c>slowest_item_ms</c> — 1:1, persisted, and the fetch
    /// is the dominant term inside a <c>query_store</c> item, so V80 already fingers the right one. WHICH PHASE
    /// is what this split is for, and a sum answers it EXACTLY rather than by proxy.</para>
    ///
    /// <para><b>Why the slowest-database rollup was declined, stated as the measurement that decided it.</b>
    /// It is not that it loses information — it is that its residual error points one way. The slowest database
    /// alone names the same winning phase as the run's true argmax on 92.8% of <c>plan_fetch</c> runs (89.5%
    /// among the multi-database ones). Of the 199 disagreements, 170 read the truth <c>probe</c> as
    /// <c>target</c> and only 26 the reverse: a <b>~6.5 : 1 bias toward indicting the monitored target when the
    /// cost was actually the store probe</b>. That is precisely the misreading this instrumentation family
    /// exists to end, so a 90%-right instrument whose 10% error is systematically anti-target is the wrong
    /// instrument for the one question anyone asks of this split. Sums carry no such bias.</para>
    ///
    /// <para><b>The blind spot, plainly.</b> Sums cannot say whether ONE database inside a run was pathological
    /// while its siblings were fine — the ~10.5% of multi-database runs whose slowest database's phase mix
    /// differs from the run's aggregate. Nothing here recovers that; the columns trade per-database resolution
    /// for an unbiased per-run answer, and if the per-database question ever becomes live the increment is three
    /// more columns for the slowest database's mix, an addition on top rather than a different shape.</para>
    ///
    /// <para><b>Row cost: zero.</b> These are columns on a row that is written anyway. The split fires on only
    /// 22.2% of <c>query_store</c> runs (a fetch has to actually run), so they are NULL on ~98% of
    /// <c>collection_log</c> — the V80/V108/V109 trade, very nearly free on a hypertable compressed and
    /// segmented by <c>server_id</c>. For contrast, the per-item table the issue offered would have been
    /// ~167 rows/day/server against ~11,955 <c>collection_log</c> rows/day/server.</para>
    ///
    /// <para><b>Why the counters are here after all, when #2860 §7 argued for leaving them out.</b> That
    /// section's own condition was "add them if a question actually needs them", and #2902 supplied one: the
    /// fetch carryover has NO eviction — no count cap, no byte cap, no age-out — and narrowing its key to
    /// include the collector dropped drain opportunities ~52%. If <c>query_store</c>'s candidate cap cannot
    /// cover what its probe finds missing, the backlog grows silently, and #2902 concluded twice that
    /// <c>ids attempted</c> versus <c>probe ids</c> per run is the only instrument that would see it. They also
    /// turn the durations into RATES, which is where the phase question stops being descriptive: <c>target</c>
    /// ÷ <c>ids_attempted</c> measured 31.65 ms/id on production's cold plans against the ~1.6 ms/id #2806's
    /// controlled A/B saw on hot ones — a ~20x gap that bears directly on that open issue — and <c>probe</c> ÷
    /// <c>probe_ids</c> reproduces the documented ~0.61 ms/reference, which is what calibrated the parse.
    /// <c>chunks</c> is still left out: it is a batching artifact no question has asked for.</para>
    ///
    /// <para><b>Deliberately NOT generalised to #2855's Azure per-database split.</b> Two reasons that both
    /// survive being checked. The row-volume profile is OPPOSITE: #2893's line is explicitly not gated on
    /// <c>batch.Count &gt; 0</c> ("the connect is paid on a quiet database exactly as on a busy one"), so its
    /// emissions are one per database per cycle — the ungated shape V80 priced at ~10% and declined, and the
    /// cheapness above comes entirely from this rung's gating and does not transfer. And the NAMES collide: a
    /// persisted per-database <c>open_ms</c> / <c>drain_ms</c> would sit beside V108's <c>sql_open_ms</c> /
    /// <c>sql_drain_ms</c>, which mean the SERVER-scoped open and drain. jsonb is the only shape that would
    /// generalise, and it costs the cheap aggregation that is the entire reason to persist any of this.</para>
    ///
    /// <para><b>No <c>other:</c> column, and no stored parent — #2859's rule with one honest consequence.</b>
    /// <c>other:</c> is a residual (<c>fetch - probe - target - write</c>) and a stored copy could drift from
    /// the parent it completes. But unlike V108, the parent here is NOT already a column: there is no
    /// <c>plan_fetch_ms</c>, so <c>other:</c> is not derivable from the store at all — it is simply not
    /// recorded. That is a real loss and it is accepted rather than overlooked, because <c>other:</c> measured
    /// 0.1% of both fetches fleet-wide: the three stored terms ARE the parent to within a rounding error, so
    /// their sum is the fetch total and storing it separately would be storing a derived number.</para>
    ///
    /// <para><b>No Lite twin, and the reasoning is stronger here than V108's.</b> The parity rule exists so
    /// state added to one store does not read as permanently empty on the other. Lite never sets
    /// <c>CollectorContext.CapturePlanXml</c> or <c>FetchQueryTextSeparately</c> — that is what makes Darling
    /// the plan-capturing SKU — so neither fetch ever RUNS under Lite. A DuckDB twin would be ten columns that
    /// are NULL on every row Lite will ever write, which is the outcome the rule prevents rather than the one
    /// it demands.</para>
    ///
    /// <para><b>NULL means no fetch ran</b>, per half and independently: a run that fetched text but no plans
    /// stores the five text columns and leaves the five plan ones NULL, matching how the log line emits its two
    /// sub-lines separately. The gate is the same <c>PerItem*FetchMs &gt; 0</c> the log line uses, deliberately,
    /// so the stored population and the logged population are the same one and the figures above transfer
    /// without re-deriving. The cost of reusing it is stated rather than hidden: it cannot separate "no fetch
    /// ran" from "a fetch ran and was sub-millisecond". That is the opposite call from V108's MEASURED flag, and
    /// on purpose — a 0 ms open is a real measurement of an event that happened, while a 0 ms fetch means the
    /// fetch found nothing to do, so NULL is the honest record and ten zeros would be noise on the ~78% of runs
    /// that fetch nothing.</para>
    ///
    /// <para>Nullable, no DEFAULT, no backfill — the V80/V108/V109 reasoning: a catalog-only change that stays
    /// instant on a large compressed hypertable, where adding a column WITH a default is the shape TimescaleDB
    /// has historically refused. A row written before this rung does not know its fetch split, and NULL says so.
    /// <c>integer</c> throughout and no <c>numeric</c> anywhere: every figure is a whole millisecond or a whole
    /// id count, matching <c>sql_duration_ms</c> and V108's columns, so there is no precision or scale to
    /// choose. The derived rates (ms per id) are computed by the reader in floating point, which is where that
    /// decision belongs — baking a scale into the schema would fix it for every future consumer.</para>
    /// </summary>
    private const string V110Sql = @"
ALTER TABLE collect.collection_log
    ADD COLUMN IF NOT EXISTS plan_fetch_probe_ms integer,
    ADD COLUMN IF NOT EXISTS plan_fetch_target_ms integer,
    ADD COLUMN IF NOT EXISTS plan_fetch_write_ms integer,
    ADD COLUMN IF NOT EXISTS plan_fetch_ids_attempted integer,
    ADD COLUMN IF NOT EXISTS plan_fetch_probe_ids integer,
    ADD COLUMN IF NOT EXISTS text_fetch_probe_ms integer,
    ADD COLUMN IF NOT EXISTS text_fetch_target_ms integer,
    ADD COLUMN IF NOT EXISTS text_fetch_write_ms integer,
    ADD COLUMN IF NOT EXISTS text_fetch_ids_attempted integer,
    ADD COLUMN IF NOT EXISTS text_fetch_probe_ids integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so without this refresh the passthrough every
   read goes through would keep serving the pre-V110 column list forever - invisible on an UPGRADED store
   while working fine on a fresh one. The V14 lesson, V80's, V108's and V109's. */
CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;";

    /// <summary>
    /// V105 — <c>collect.collector_cost</c>, the tool's own per-collector cost on the monitored servers
    /// (#2674). NOT a collector: it is INTERNAL self-telemetry, written by the worker's hourly self-metrics
    /// sweep like <c>collect.store_metrics</c> (V53), so it is deliberately absent from
    /// <c>CollectorCatalog.All</c> and therefore from the generator-parity pins. Hand-written DDL, a plain
    /// table (not a hypertable — the sweep aggregates to one row per server+collector per hour, and its own
    /// bounded retention DELETE keeps it small, the same shape store_metrics uses). Columns are an hourly
    /// aggregate: run_count, total/max sql_ms (the MAX is the load-bearing one — the tail is how a collector
    /// "sticks out" on a target), total storage_ms and total_rows. database_name is nullable for
    /// server-scoped collectors.
    /// </summary>
    private const string V105Sql = @"
CREATE TABLE IF NOT EXISTS collect.collector_cost
(
    metric_time timestamp NOT NULL,
    server_id integer NOT NULL,
    database_name text,
    collector_name text NOT NULL,
    run_count integer NOT NULL,
    total_sql_ms bigint NOT NULL,
    max_sql_ms bigint NOT NULL,
    total_storage_ms bigint NOT NULL,
    total_rows bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_collector_cost_time
    ON collect.collector_cost(metric_time);

CREATE INDEX IF NOT EXISTS idx_collector_cost_lookup
    ON collect.collector_cost(server_id, collector_name, metric_time);";

    /// <summary>
    /// V104 — the lookup index for <c>collect.pg_deadlocks</c> (#2661), and a SEPARATE rung on purpose.
    ///
    /// <para><c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c> requires a
    /// collector's rung to be column-for-column and index-for-index what <c>PgSchemaGenerator</c> emits, and
    /// the generator emits exactly one index per collector. An extra index inside V103 is not a drafting
    /// error the pin should tolerate — it is the pin working, because a fresh store builds from the
    /// generated schema and would silently not have it. Putting it in its own rung gives BOTH populations
    /// the index, which is what was actually wanted.</para>
    ///
    /// <para><b>Why the index earns its place.</b> The collector re-reads an OVERLAPPING tail of the server
    /// log every cycle, deliberately, so a report cut in half at one edge is whole in the next. Every read
    /// therefore groups or filters on <c>deadlock_hash</c> to answer once per deadlock rather than once per
    /// sighting, and the detail read looks a report up by hash directly.</para>
    ///
    /// <para><b>Not UNIQUE.</b> Two servers legitimately produce identical graph text — the same query pair
    /// deadlocking with the same process ids on two hosts is not impossible — and a unique constraint would
    /// also let a partial write during a crash block the next cycle's insert. Deduplication is a READ
    /// concern here, and the reads already do it.</para>
    /// </summary>
    private const string V104Sql = @"
CREATE INDEX IF NOT EXISTS idx_pg_deadlocks_identity
    ON collect.pg_deadlocks(server_id, deadlock_hash);";

    /// <summary>
    /// V103 — <c>collect.pg_deadlocks</c>, the deadlock reports PostgreSQL writes to its server log (#2661).
    /// We collected the COUNT (<c>pg_stat_database.deadlocks</c>) and nothing else: a number that goes up.
    /// This is which sessions, holding what, running what SQL.
    ///
    /// <para><b>The identity column is the point.</b> Both transports read a bounded TAIL of the log on a
    /// schedule and the window OVERLAPS deliberately — a report cut in half at the edge of one read is whole
    /// in the next — so without <c>deadlock_hash</c> the same deadlock is stored once per cycle for as long
    /// as it stays inside the window. The hash is over the graph text rather than over
    /// (timestamp, victim_pid): two reports in the same millisecond with the same victim pid are
    /// vanishingly unlikely, but the graph is what actually distinguishes them, and hashing the thing
    /// itself needs no argument about how unlikely a collision is.</para>
    ///
    /// <para><b><c>graph_text</c> is stored whole, beside the parsed columns.</b> The parsed fields are an
    /// interpretation of a format that varies with the lock type — <c>transaction N</c>,
    /// <c>relation N of database N</c>, <c>tuple (b,o) of relation N</c>, advisory locks — and the raw block
    /// is the evidence. A shape the parser does not recognise today still arrives readable by a person
    /// rather than as a row of NULLs.</para>
    ///
    /// <para><b>All value columns nullable</b>, matching the generated schema this must be identical to
    /// (<c>PgSchemaGeneratorTests</c>). <c>occurred_at</c> is nullable for the same reason the others are:
    /// it comes from parsing a log line, and the store's job is to record what was found rather than to
    /// assert it was always found.</para>
    /// </summary>
    private const string V103Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_deadlocks (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    occurred_at timestamp,
    victim_pid integer,
    participant_count integer,
    deadlock_hash text,
    lock_modes text,
    resources text,
    victim_statement text,
    graph_text text
);

CREATE INDEX IF NOT EXISTS idx_pg_deadlocks_time
    ON collect.pg_deadlocks(server_id, collection_time);";

    /// <summary>
    /// V106 — <c>collect.pg_cpu_utilization</c>, instance-level CPU for a managed PostgreSQL/Aurora target
    /// (#2719). Column-for-column identical to what <see cref="PgSchemaGenerator"/> generates from
    /// <see cref="PgCpuUtilizationCollector.PayloadColumns"/> — pinned by
    /// <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c>, same as every other
    /// rung above. See <see cref="PgCpuUtilizationCollector"/>'s own doc comment for why this collector has
    /// no SQL route at all: every row here arrives through the RDS/Performance Insights API, never a
    /// database connection.
    /// </summary>
    private const string V106Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_cpu_utilization (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    sample_time timestamp,
    cpu_percent double precision
);

CREATE INDEX IF NOT EXISTS idx_pg_cpu_utilization_time
    ON collect.pg_cpu_utilization(server_id, collection_time);";

    /// <summary>
    /// V102 — <c>collect.pg_server_config</c>, the server's own configuration from <c>pg_settings</c>
    /// (#2658). SQL Server answers this three ways and PostgreSQL had no answer at all: nothing stored a
    /// setting, so both "what is <c>work_mem</c> here" and "what changed last Tuesday" were unanswerable
    /// after the fact — the second permanently, because no other part of the stack can reconstruct a
    /// configuration history that was never recorded.
    ///
    /// <para><b>Every column is stored and nothing is filtered at collection.</b> <c>pg_settings</c> is a
    /// per-BACKEND view, so <c>source</c> ranges from <c>default</c> through <c>configuration file</c> to
    /// <c>client</c> — and a <c>client</c> row is the collector's own session, not the server. Dropping
    /// those here would make them unrecoverable and would still leave the read guessing; keeping
    /// <c>source</c> lets the read state which rows are server configuration and which are not. The rule
    /// lives at the read, where it can be enforced and explained.</para>
    ///
    /// <para><b>It IS a hypertable, like every collector table</b> — <c>TimescaleSupport.HypertableTables</c>
    /// is <c>CollectorCatalog.All</c>, so membership follows from being a collector and is not a per-table
    /// choice. Worth stating because the shape argues the other way: this is a snapshot of something a
    /// person changes, not a series of measurements, and the rows are wide-ish text that is nearly
    /// identical from one hour to the next. Chunking and compression still earn their place on exactly that
    /// data — a year of hourly near-duplicates is what compresses best — and the alternative would be a
    /// special case in the one place that currently has none. It does mean CI's worker sizing moves:
    /// <c>CiClusterWorkerSizingTests</c> derives the cluster's worker counts from the catalog count, so
    /// adding a collector is also a workflow edit.</para>
    ///
    /// <para><b>All value columns nullable</b>, including <c>name</c>, because the generated schema is what
    /// a fresh store builds from and it declares them that way — see
    /// <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c>, which requires this
    /// text to be column-for-column identical to what the generator walks out of
    /// <c>PgServerConfigCollector.PayloadColumns</c>. A NOT NULL added by hand here and not there is the
    /// permanent, invisible divergence that test exists to catch.</para>
    /// </summary>
    private const string V102Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_server_config (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    name text,
    setting text,
    unit text,
    category text,
    context text,
    vartype text,
    source text,
    boot_val text,
    reset_val text,
    sourcefile text,
    sourceline integer,
    pending_restart boolean,
    short_desc text
);

CREATE INDEX IF NOT EXISTS idx_pg_server_config_time
    ON collect.pg_server_config(server_id, collection_time);";

    /// <summary>
    /// V101 — the measured I/O byte totals PostgreSQL 18 gave <c>pg_stat_io</c> (#2655).
    ///
    /// <para><b>Why this is an upgrade and not a workaround.</b> 18 removed <c>op_bytes</c>, and
    /// <see cref="PgIoStatsCollector"/> substituted NULL for it so a collection would not fail outright. That
    /// kept the store readable and lost the answer: both byte figures the read serves are derived from
    /// <c>op_bytes</c>, so on 18 they came back NULL with nothing saying why. The three columns 18 replaced it
    /// with are strictly better than what was lost — <c>op_bytes</c> was the per-operation BLOCK SIZE, which
    /// the read multiplied by a count to ESTIMATE volume; these are measured byte totals.
    ///
    /// <para><b>The estimate is not merely less precise on 18, it is wrong.</b> 18 also introduced vectored
    /// reads, so one entry in <c>reads</c> can cover several blocks and <c>reads * block_size</c> undercounts.
    /// Measured on a real 18.6: 479 reads against 4,440,064 read bytes — 542 blocks, not 479. That is the
    /// reason the column was removed rather than merely renamed, and the reason these are stored separately
    /// instead of being back-filled into <c>op_bytes</c>: they are a different quantity and a reader must be
    /// able to tell which one it has.</para>
    ///
    /// <para><b><c>numeric</c>, not <c>bigint</c></b>, because that is what PostgreSQL declares them
    /// (verified on 18.6 — <c>reads</c> is <c>bigint</c> while <c>read_bytes</c> is <c>numeric</c>). Storing
    /// them as bigint would be a narrowing the catalog never promised; a byte total across a long uptime is
    /// exactly the quantity that outgrows the assumption.
    ///
    /// <para>The <c>(28,0)</c> is not decoration and is not free to differ from the collector's declared
    /// precision: <c>PgSchemaGeneratorTests</c> renders the schema from <c>PayloadColumns</c> and requires
    /// every rung to match it exactly, so a bare <c>numeric</c> here and a <c>Decimal(28, 0)</c> there is a
    /// build failure, correctly. Scale 0 because these are whole bytes; 28 rather than the 38 a DECIMAL can
    /// hold because C# <c>decimal</c> tops out near 29 significant digits, and a declared width the runtime
    /// type cannot carry is a promise broken at the reader rather than at the store.</para></para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> — as V69 and every counter rung since. These are
    /// cumulative counters differenced at read time, and PostgreSQL itself uses NULL for "this counter does
    /// not apply to this combination" (WAL rows report no <c>extend_bytes</c>, verified on 18.6). A NOT NULL
    /// 0 would turn "not reported" into a measurement, which is the one thing the I/O read works hardest not
    /// to do. Below 18 they stay NULL forever and <c>op_bytes</c> remains the answer.</para>
    ///
    /// <para><b>The columns are added in TWO places and both are required.</b> A store's tables come from
    /// one of two texts depending on when it was created: a fresh store builds them from V1's generated
    /// schema, walked from the collector catalog, while an existing store has whatever its rungs built. So
    /// the <c>pg_io_stats</c> CREATE TABLE rung gains the three columns for the fresh population — that is
    /// what <c>PgSchemaGeneratorTests</c> enforces, column for column — and THIS rung's ALTER carries the
    /// existing one. Neither is redundant: the CREATE is <c>IF NOT EXISTS</c> and never re-runs on a store
    /// that already has the table, and the ALTER is <c>ADD COLUMN IF NOT EXISTS</c> and is a no-op on the
    /// fresh store that just created them. Dropping either leaves one population permanently without the
    /// columns, which is exactly the invisible divergence that test exists to prevent.</para>
    ///
    /// <para>No view refresh: <c>collect.pg_io_stats</c> has no <c>v_</c> passthrough freezing a column
    /// list.</para>
    /// </summary>
    private const string V101Sql = @"
ALTER TABLE collect.pg_io_stats
    ADD COLUMN IF NOT EXISTS read_bytes numeric(28,0),
    ADD COLUMN IF NOT EXISTS write_bytes numeric(28,0),
    ADD COLUMN IF NOT EXISTS extend_bytes numeric(28,0);";

    /// <summary>
    /// V100 — the PostgreSQL major version on the <c>collect.servers</c> registry (#2653). V82 gave the
    /// registry <c>engine_kind</c>, which answers <i>which engine</i>; this answers <i>which version of
    /// it</i>, and without it the read layer cannot explain its own NULLs.
    ///
    /// <para><b>The gap this closes.</b> Seven PostgreSQL collectors branch on
    /// <c>postgresMajorVersion</c> and emit <c>NULL::bigint</c> for columns the target's version does not
    /// have — <c>PgWriteStatsCollector</c> alone has nine, because 17 removed <c>buffers_backend</c> and
    /// <c>buffers_backend_fsync</c> from <c>pg_stat_bgwriter</c> with no successor there. The collectors get
    /// the version from the live probe and are correct. The READS have no connection and no column to read
    /// it from, so a structurally-absent column arrives as a naked NULL and is indistinguishable from a
    /// measurement that did not happen — the failure #2511 and #2623 exist to prevent, on the version
    /// axis.</para>
    ///
    /// <para><b>Not inferable from the data.</b> Deriving "17 or later" from
    /// <c>buffers_backend IS NULL</c> would work for that one column and is the wrong fix twice over: it
    /// makes every read re-derive a fact the connector already held, and for any column where absent-on-this
    /// -version and genuinely-NULL are both possible it cannot tell them apart.</para>
    ///
    /// <para><b>Integer, not the version string.</b> The major is what every gate in the collectors compares
    /// against (<c>&gt;= 17</c>, <c>&gt;= 16</c>, <c>&gt;= 14</c>); <c>server_version_num</c> and the full
    /// text are diagnostic detail that would have to be re-parsed at every use. <c>sql_major_version</c> next
    /// door is the same choice for the same reason, which also settles why this is a second column rather
    /// than a reuse of that one: a reader joining the two engines' versions in one integer has no way to
    /// know which vocabulary a given number belongs to, and 17 is a real major in both.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> — as V82. NULL says "no connect has stamped this yet"
    /// and the readers treat it as no claim rather than as a version. The registry upsert runs on every
    /// successful connect, so the column populates itself within one collection cycle for every live server.
    /// It stays NULL forever on a SQL Server target, which is correct: it is not a fact about that server.
    /// </para>
    ///
    /// <para>No view refresh: <c>collect.servers</c> is a registry, not a hypertable, and has no
    /// <c>v_</c> passthrough freezing a <c>SELECT *</c> column list.</para>
    /// </summary>
    private const string V100Sql = @"
ALTER TABLE collect.servers
    ADD COLUMN IF NOT EXISTS postgres_major_version integer;";

    /// <summary>
    /// V99 — <c>collect.pg_plan_capture</c> (#2566, part of #2538): execution plans captured by
    /// <c>auto_explain</c> and read out of the server log.
    ///
    /// <para><b>Its own table, NOT <c>query_plan_dim</c>.</b> That table holds SQL Server plan XML and
    /// <c>plan_xml_compression = none</c> is a documented contract with direct-SQL consumers who read the
    /// column as XML. Introducing JSON rows into it would break those readers silently — no schema change to
    /// notice, no error, no version to key off. The compression codec and
    /// <c>plan_content_retention_days</c> machinery is shared; the table is not.</para>
    ///
    /// <para><b>No query text and no literals, and that is the load-bearing part.</b> <c>auto_explain</c>
    /// emits <c>Query Text</c> verbatim, and <c>auto_explain.log_parameter_max_length = 0</c> does NOT
    /// suppress it — that setting covers bind parameters only (measured, #2565). Literals also sit inside the
    /// plan tree in <c>Filter</c> and its relatives. So the text is dropped — statement identity is
    /// <c>query_id</c>, whose normalised text already lives in <c>pg_statement_stats</c> — and every
    /// remaining string is redacted before storage. Verified against 25 captured plans: zero literals
    /// survived, while a relation genuinely named <c>transactionitems1</c> kept its name.</para>
    ///
    /// <para><b><c>plan_hash</c> is of the REDACTED plan</b>, so the same plan shape recurs to the same hash
    /// whatever values it ran with. Hashing the raw text would defeat dedup exactly where it matters most.</para>
    ///
    /// <para>Self-hosted only in practice: reading the log needs <c>pg_read_server_files</c> AND an explicit
    /// <c>GRANT EXECUTE ON FUNCTION pg_read_file</c> (the role alone does not carry it — measured). Aurora and
    /// RDS have no filesystem and require the RDS API instead, which is a different integration (#2538).</para>
    /// </summary>
    private const string V99Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_plan_capture (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    query_id bigint,
    plan_hash text,
    duration_ms double precision,
    node_count integer,
    top_node_type text,
    plan_json text
);

CREATE INDEX IF NOT EXISTS idx_pg_plan_capture_time
    ON collect.pg_plan_capture(server_id, collection_time);";

    /// <summary>
    /// V98 — <c>collect.pg_predicate_stats</c> (#2603): which columns are filtered on, how selectively, and
    /// where the planner's estimate was wrong, from <c>pg_qualstats</c>.
    ///
    /// <para><c>pg_index_usage_stats</c> records indexes that were USED. This records predicates that were
    /// EVALUATED, including the ones with no index behind them — which is the index-candidate signal, and is
    /// invisible to everything else here because nothing records a scan that had no index to record.</para>
    ///
    /// <para><b><c>sample_rate</c> is stored per row and is usually not 1.</b> The extension defaults to
    /// <c>1/max_connections</c> — 0.01 on the rig, where <c>pg_qualstats()</c> returned ZERO rows on a server
    /// that had just run the queries it was meant to record. Counts read as complete are the failure this
    /// column prevents.</para>
    ///
    /// <para><b><c>worst_estimate_error_ratio</c> is the reason to look</b> — measured at 57.9x on one
    /// predicate beside 1.04x on another. Selectivity says an index might help; the error ratio says the
    /// planner does not understand the column.</para>
    ///
    /// <para>Per database, because <c>lrelid</c>/<c>lattnum</c> are OIDs meaningful only inside their own
    /// database while <c>pg_class</c> and <c>pg_attribute</c> are per-database catalogs. Unscoped, the join
    /// either silently drops other databases' rows or resolves them against whatever local object shares the
    /// OID and reports a confident wrong column name.</para>
    /// </summary>
    private const string V98Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_predicate_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    column_name text,
    operator text,
    query_id bigint,
    sample_count bigint,
    rows_evaluated bigint,
    rows_filtered bigint,
    worst_estimate_error_ratio double precision,
    sample_rate double precision
);

CREATE INDEX IF NOT EXISTS idx_pg_predicate_stats_time
    ON collect.pg_predicate_stats(server_id, collection_time);";

    /// <summary>
    /// V97 — <c>collect.pg_kernel_stats</c> (#2603): operating-system CPU and disk per query, from
    /// <c>pg_stat_kcache</c>.
    ///
    /// <para><c>pg_stat_statements</c> reports elapsed time, which cannot separate a query that was WAITING
    /// from one that was burning processor. These are the kernel's own numbers for the same
    /// <c>queryid</c>, so the two compose.</para>
    ///
    /// <para><b><c>exec_read_bytes = 0</c> does not mean nothing was read.</b> The counters come from
    /// <c>getrusage</c> and measure I/O that reached the DEVICE, so a read served by the OS page cache is
    /// genuinely zero — measured that way across every row on the rig while writes were not zero. It is not
    /// comparable to a logical-read figure and the column name says bytes for that reason.</para>
    ///
    /// <para>Only top-level statements. With <c>pg_stat_statements.track = 'all'</c> a nested statement
    /// appears both on its own row and inside its caller's, and summing them double-counts every function
    /// body on the server.</para>
    ///
    /// <para>Cumulative, differenced by the read. <c>stats_since</c> is stored so a reset is a recorded fact
    /// rather than something inferred from a counter that went backwards.</para>
    /// </summary>
    private const string V97Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_kernel_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    query_id bigint,
    exec_user_time_ms double precision,
    exec_system_time_ms double precision,
    plan_cpu_time_ms double precision,
    exec_read_bytes bigint,
    exec_write_bytes bigint,
    minor_faults bigint,
    major_faults bigint,
    stats_since timestamp
);

CREATE INDEX IF NOT EXISTS idx_pg_kernel_stats_time
    ON collect.pg_kernel_stats(server_id, collection_time);";

    /// <summary>
    /// V96 — <c>collect.pg_wait_sampling</c> (#2603): wait events attributed to the query that waited.
    ///
    /// <para>Wait analysis existed ONLY on Aurora before this. <c>PgWaitStatsCollector</c> reads
    /// <c>aurora_stat_system_waits()</c>, so every self-hosted, on-prem and plain-RDS target had no wait
    /// data at all — backwards, because self-hosted is where the extension story is richest.</para>
    ///
    /// <para><b><c>sample_count</c> is a tally of observations, not a duration</b>, and
    /// <c>profile_period_ms</c> travels beside it because the count is uninterpretable alone. Storing a
    /// derived millisecond figure would bake today's sampling period into history that outlives it.</para>
    ///
    /// <para><b>Cumulative, like <c>pg_statement_stats</c>.</b> A counter that goes backwards is a reset —
    /// <c>pg_wait_sampling_reset_profile()</c> or a restart — rather than a negative wait, and the read owns
    /// that subtraction so it can recognise the difference.</para>
    ///
    /// <para><b>No <c>database_name</c>, deliberately.</b> The profile is cluster-wide and version 1.1
    /// exposes no database column, so attributing these rows to a database would be exactly the scope error
    /// V95 removed from three other tables.</para>
    ///
    /// <para><c>event_type = 'Activity'</c> never reaches this table: those are background processes idling,
    /// and they dominate a raw profile permanently BECAUSE nothing is happening. A backend that was not
    /// waiting is stored as <c>CPU</c>/<c>Running</c> rather than as a blank row.</para>
    /// </summary>
    private const string V96Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_wait_sampling (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    event_type text,
    event text,
    query_id bigint,
    sample_count bigint,
    profile_period_ms integer,
    backend_count integer
);

CREATE INDEX IF NOT EXISTS idx_pg_wait_sampling_time
    ON collect.pg_wait_sampling(server_id, collection_time);";

    /// <summary>
    /// V95 — <c>database_name</c> on the three per-database PostgreSQL tables (#2599).
    ///
    /// <para>Three collectors ran once per database and wrote rows that could not be attributed to one.
    /// <c>pg_column_stats</c> and <c>pg_index_bloat</c> both declared <c>RunsPerDatabase</c> and no
    /// <c>database_name</c>, so on any cluster carrying the same schema in two databases — the ordinary
    /// multi-tenant shape — their rows collided on (server, schema, object) with nothing to separate them.
    /// <c>pg_extension_availability</c> had the inverse problem: it ran ONCE and reported
    /// <c>installed</c>/<c>outdated</c>, which are per-database facts read from <c>pg_extension</c>, so its
    /// answer depended on which database the target was configured for.</para>
    ///
    /// <para><b>How it surfaced.</b> On a live Aurora target two of our own collectors read
    /// <c>pg_extension</c> for <c>pgstattuple</c> in the same cycle and disagreed:
    /// <c>pg_table_bloat_stats</c> (already per-database) saw it installed, <c>pg_extension_availability</c>
    /// reported it merely available. Both were right about their own database and neither row said which
    /// database that was.</para>
    ///
    /// <para>Nullable with no DEFAULT and no backfill: it is a catalog-only change in PostgreSQL, so it
    /// stays instant on a compressed hypertable, and rows collected before this rung genuinely do not know
    /// which database they came from. NULL is the honest value for them rather than a guess at the
    /// target's default database, which would be wrong for every row the sweep collected elsewhere.</para>
    ///
    /// <para><b>The column is ALSO added to the V89, V91 and V94 CREATE statements, and that is deliberate
    /// rather than redundant.</b> Those rungs are hand-written literals held identical to the schema
    /// generator's output by <c>EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c>, and the generator now
    /// emits the column — so leaving them alone would fail that assertion. The two paths converge either
    /// way: a store new enough to create the tables here gets the column from the CREATE and this rung's
    /// <c>ADD COLUMN IF NOT EXISTS</c> no-ops, while a store that already ran V89/V91/V94 without it gets it
    /// from this rung. Neither path can reference a column before something establishes it, which is the
    /// #2119 hazard these rungs are pinned against.</para>
    /// </summary>
    private const string V95Sql = @"
ALTER TABLE collect.pg_column_stats
    ADD COLUMN IF NOT EXISTS database_name text;

ALTER TABLE collect.pg_index_bloat
    ADD COLUMN IF NOT EXISTS database_name text;

ALTER TABLE collect.pg_extension_availability
    ADD COLUMN IF NOT EXISTS database_name text;";

    /// <summary>
    /// V94 — <c>collect.pg_index_bloat</c> (#2561): b-tree index bloat, MEASURED via <c>pgstatindex</c>
    /// rather than estimated from column statistics.
    ///
    /// <para>The issue proposed porting the ioguix estimator. Measured, that route is unusable for the role
    /// this product runs as: <c>pg_stats</c> returns ZERO rows to a <c>pg_monitor</c>-only login because the
    /// view filters on <c>has_column_privilege</c> — the same trap behind #2542's 88.59% reported against a
    /// true 0.50%. <c>pgstatindex</c> DOES run for <c>pg_monitor</c>, because pgstattuple grants EXECUTE to
    /// <c>pg_stat_scan_tables</c>. Under our permissions the exact function works and the estimator is
    /// blind.</para>
    ///
    /// <para><b><c>avg_leaf_density</c> is stored RAW and is not a bloat percentage.</b> Measured across
    /// seven freshly-built indexes it sits between 89.98 and 91.48, and between 87.07 and 90.81 after
    /// <c>REINDEX</c> — so <c>100 - density</c> invents roughly ten points of bloat on a perfect index, and
    /// there is no constant to subtract because the healthy value varies per index. The server's own numbers
    /// are kept and interpretation is left to the read.</para>
    ///
    /// <para><c>skipped_reason</c> exists so a size cap can never masquerade as an absence of bloat: the
    /// function reads every page of an index, so very large ones are recorded with NULL measurements and a
    /// stated reason rather than being dropped from the result.</para>
    ///
    /// <para>Only b-tree indexes are collected. <c>pgstatindex</c> RAISES on anything else — verified on
    /// GIN, BRIN and hash — so one GIN index would otherwise take the whole collection down every cycle.
    /// </para>
    /// </summary>
    private const string V94Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_index_bloat (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    index_name text,
    index_bytes bigint,
    tree_level integer,
    internal_pages bigint,
    leaf_pages bigint,
    empty_pages bigint,
    deleted_pages bigint,
    avg_leaf_density double precision,
    leaf_fragmentation double precision,
    skipped_reason text
);

CREATE INDEX IF NOT EXISTS idx_pg_index_bloat_time
    ON collect.pg_index_bloat(server_id, collection_time);";

    /// <summary>
    /// V81 — tempdb's growth CEILING on <c>tempdb_stats</c> (#2515). <c>dm_db_file_space_usage</c>, which is
    /// where every other column in this table comes from, describes the data files AS CURRENTLY ALLOCATED. So
    /// <c>total_reserved</c> ÷ (<c>total_reserved</c> + <c>unallocated</c>) — the <c>tempdb Space</c> alert's
    /// percentage — is distance to the next AUTOGROW, and it only looks like real headroom on a pre-sized
    /// on-prem box because such a tempdb has already grown to its cap.
    ///
    /// <para><b>What made this a bug rather than a tuning question.</b> Measured on <c>GP_S_Gen5_2</c>: four
    /// tempdb data files of 16 MB each, 62.44 MB allocated, and <c>max_size</c> of 2,097,152 pages on every one
    /// of them — a 65,536 MB ceiling. One ~57 MB <c>#temp</c> table reads 95.7% full against the allocation and
    /// 0.09% against the cap, so enabling Azure collection (#2512) would have armed an alert that fires on the
    /// first busy minute of every Azure target at the shipped 80% default.</para>
    ///
    /// <para><b>Why a column and not a derivation.</b> The obvious alternative is to read the cap out of
    /// <c>database_size_stats</c>, which already carries <c>max_size_mb</c> per file — and on Azure SQL
    /// Database that collector reads only the CONNECTED database's <c>sys.database_files</c>, so tempdb never
    /// appears in it. The one platform this exists for is the one where the derivation has nothing to read.</para>
    ///
    /// <para>Nullable with no DEFAULT and no backfill, like V80 and for the same reasons: it is a catalog-only
    /// change in PostgreSQL and stays instant on a compressed hypertable, and a row collected before this rung
    /// genuinely does not know the ceiling. NULL reads as 0 in the adapter, which is the "no ceiling measured"
    /// state — history keeps reporting the percentage it always did rather than dividing by a zero cap.</para>
    /// </summary>
    private const string V81Sql = @"
ALTER TABLE collect.tempdb_stats
    ADD COLUMN IF NOT EXISTS max_size_mb numeric(18,2);

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the V4 passthrough the analysis fact
   collector reads would keep serving the twelve columns it had forever — the V14 lesson, restated by V80.
   Appending is the one alteration CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN
   produces. */
CREATE OR REPLACE VIEW collect.v_tempdb_stats AS SELECT * FROM collect.tempdb_stats;";

    /// <summary>
    /// V80 — the fan-out rollup on <c>collection_log</c> (#2472). A collector that runs once per DATABASE
    /// writes ONE row whose <c>duration_ms</c> is the sum across every database, so an 80.8-second run is
    /// indistinguishable between "eight databases at 10.1s each" and "one at 61.9s and seven at 2.7s" — two
    /// shapes that want opposite fixes, which is why #2468 could not be decided. (#2472 writes the second as
    /// 62s and rounds both to 80.9; they do not balance, and the claim under test is that the two are
    /// indistinguishable, so the figures here are its shapes made exact.)
    ///
    /// <para><b>Which collectors, and it is two mechanisms rather than one.</b> Five drive the fan-out from
    /// an ENUMERATION on any SQL Server target — <c>query_store</c>, <c>plan_correction</c>,
    /// <c>query_store_health</c>, <c>index_object_stats</c>, <c>database_scoped_config</c>. Separately,
    /// <c>RunsPerDatabase</c> puts eight on a per-database CONNECTION loop when the target is Azure SQL DB,
    /// and <c>pg_autovacuum_stats</c> on one always. Both mechanisms feed the same accumulator, which is the
    /// point: <c>query_store</c> uses the first on-prem and the second on Azure, so a rollup wired to only
    /// one of them would report a different notion of a slow database depending on where it ran.</para>
    ///
    /// <para><b>Why three columns and not a table.</b> The per-database costs already exist; the runner logs
    /// them and throws them away. A <c>collector_item_timings</c> hypertable would keep the whole
    /// distribution and cost roughly a tenth of <c>collection_log</c>'s own row volume forever. These three
    /// columns cost ZERO rows and are NULL on ~98% of them (only a productive fan-out run writes any), which
    /// on a hypertable compressed and segmented by <c>server_id</c> is very nearly free. They answer the
    /// specific question all three of #2468's live shapes need, and no more than that.</para>
    ///
    /// <para><b>The arithmetic they buy.</b> <c>slowest_item_ms * fanout_item_count / duration_ms</c> is the
    /// dominance ratio: 1.0 is a perfectly even fan-out, and the worked example above gives 1.0 against 6.1.
    /// <c>max_duration_ms</c> and <c>p95_duration_ms</c> (#2460) cannot separate those two — both runs are
    /// 80,800 ms — which is why the tail statistics, real as they are, do not close this.</para>
    ///
    /// <para>Nullable with no DEFAULT on purpose: that is a catalog-only change in PostgreSQL and stays
    /// instant on a large compressed hypertable, where adding a column WITH a default is the shape
    /// TimescaleDB has historically refused. Backfill is deliberately absent — a historical row genuinely
    /// does not know its fan-out, and NULL says so rather than inventing a zero.</para>
    /// </summary>
    private const string V80Sql = @"
ALTER TABLE collect.collection_log
    ADD COLUMN IF NOT EXISTS fanout_item_count integer,
    ADD COLUMN IF NOT EXISTS slowest_item text,
    ADD COLUMN IF NOT EXISTS slowest_item_ms integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the passthrough every read goes through
   would keep serving eleven columns forever — the V14 lesson, and the reason it exists. Appending is the
   one alteration CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN produces. */
CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;";

    /// <summary>
    /// V79 — the database file-growth alert (#2349). Between <c>tempdb Space</c> and <c>Volume Free Space</c>
    /// sits a file that has grown large but has not yet filled its disk, and neither existing alert can express
    /// it: the first fires on reserved ÷ (reserved + unallocated), whose denominator GROWS with autogrowth so
    /// the percentage FALLS as tempdb balloons; the second fires on the consequence, too late to act on and
    /// unable to attribute the space to any one file.
    ///
    /// <para>Two gates, both graded per server so ONE global setting works across a heterogeneous fleet — the
    /// constraint that shapes this, since <c>config_alert_settings</c> is a single global row and an absolute MB
    /// threshold is unusable when normal tempdb sizes differ by an order of magnitude: set it low enough for the
    /// small instances and the large ones alert constantly. The RISE gate is primary (this file grew N MB inside
    /// the window), following #2157's reasoning that a level alone pages forever about a size that has been true
    /// since Tuesday; the LEVEL gate is the file as a share of its VOLUME, which self-scales to each server's
    /// disk layout whether or not the file has a dedicated one.</para>
    ///
    /// <para>Ships OFF. A new alert that starts firing on upgrade is a bad citizen, and the right thresholds are
    /// a property of the fleet rather than of the product.</para>
    /// </summary>
    private const string V79Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS file_growth_enabled boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS file_growth_rise_mb integer NOT NULL DEFAULT 10240,
    ADD COLUMN IF NOT EXISTS file_growth_volume_percent integer NOT NULL DEFAULT 60,
    ADD COLUMN IF NOT EXISTS file_growth_lookback_minutes integer NOT NULL DEFAULT 60;";

    /// <summary>
    /// V78 — the compose statement-timeout knob (#2357). The per-session <c>statement_timeout</c> on the
    /// viewer and mcp roles is the hard backstop a composed query can never exceed, and it shipped as a bare
    /// 15-second constant. Fifteen seconds is a judgement about how big a store is and how fast its disk is,
    /// and the product knows neither for anyone else's deployment: a fleet-wide aggregate over a wide window
    /// on a large store can exceed it with nothing wrong.
    ///
    /// <para>Applied in the role PROVISIONING DDL rather than a migration, which is why the constant could
    /// not simply be raised — an existing install already has the old value baked into its roles. The
    /// provisioning SQL is re-run on every managed start ("idempotent + self-healing: re-run every managed
    /// start, converging role state"), so a store picks this up on its next restart without any new
    /// machinery.</para>
    ///
    /// <para>Default 15 preserves today's behaviour exactly for anyone who never touches it. Clamped on READ
    /// like the V59/V75 knobs, so a hand-edited absurdity cannot remove the backstop the whole design leans
    /// on — a LIMIT bounds output, a group-by scans and sorts before it, and something has to bound WORK.</para>
    /// </summary>
    private const string V78Sql = @"
ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS compose_statement_timeout_seconds integer NOT NULL DEFAULT 15;";

    /// <summary>
    /// V77 — the activity-driven plan/text fetch (#2312 Finding 2). Three small strokes for one shape
    /// change: the fetch stops walking the target's plan catalog by watermark and instead fetches exactly
    /// the plans/texts the cycle's collected rows reference that the store does not hold, making the store
    /// itself the watermark.
    ///
    /// <para><c>digest</c> goes nullable so a plan whose XML the engine cannot persist (too large, certain
    /// forced-failure paths) gets a map row with a NULL digest — the content-less MARKER. Without it the
    /// missing-set probe would re-select those plans on every cycle forever; with it, "seen, and the content
    /// will never exist" is a stored fact. Readers are unaffected: a NULL digest joins to no dimension row,
    /// which renders exactly like the absent content it records. DROP NOT NULL is metadata-only and
    /// idempotent, so this rung stays instant on the largest maps.</para>
    ///
    /// <para><c>query_store_text.query_hash</c> is the reset detector: <c>query_id</c> is only unique until
    /// a Query Store reset renumbers it, and the retired design's answer was a daily watermark expiry that
    /// re-walked the whole catalog. The stored hash lets the per-cycle probe see that an id now names a
    /// DIFFERENT statement and refetch just that text. Nullable and unbackfilled: legacy rows adopt the
    /// live hash on their first touch, which converges the fleet with zero refetches.</para>
    ///
    /// <para>The DELETEs retire the <c>planwm:</c>/<c>textwm:</c> watermark state rows wholesale — the
    /// machinery that wrote them is gone, <c>collector_state</c> has no retention (it is state, not facts),
    /// and rows nobody will ever read again should not wait for a dropped-database prune that no longer
    /// iterates their prefixes. Bare table name resolves via the migrate session's
    /// <c>search_path = collect, config, public</c>, like every rung since V8.</para>
    /// </summary>
    private const string V77Sql = @"
ALTER TABLE collect.query_store_plan_map ALTER COLUMN digest DROP NOT NULL;

ALTER TABLE collect.query_store_text ADD COLUMN IF NOT EXISTS query_hash text;

DELETE FROM collector_state WHERE collector_name = 'query_store_plan_xml' AND state_key LIKE 'planwm:%';
DELETE FROM collector_state WHERE collector_name = 'query_store_text' AND state_key LIKE 'textwm:%';";

    /// <summary>
    /// V76 — the per-database Query Store health table (#2319): what database_config's single
    /// is_query_store_on bit cannot say — actual vs desired state (the cap-hit READ_ONLY transition
    /// and its readonly_reason), current vs max storage, cleanup mode and thresholds, and the
    /// runtime-stats interval length. Body matches PgSchemaGenerator's emission for the definition
    /// (verified by generating it) plus the rung's explicit collect. prefix, so fresh stores (which
    /// generate from the catalog) and upgraded stores (which run this rung) agree byte-for-byte.
    /// Hypertable conversion is automatic from CollectorCatalog on the next service start, the same
    /// path pvs_stats took in V47. The v_ passthrough keeps the two viewers' SQL byte-identical.
    /// </summary>
    private const string V76Sql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_health (
    config_id bigint NOT NULL,
    capture_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    actual_state text,
    desired_state text,
    readonly_reason integer,
    current_storage_size_mb bigint,
    max_storage_size_mb bigint,
    size_based_cleanup_mode text,
    stale_query_threshold_days bigint,
    max_plans_per_query bigint,
    interval_length_minutes bigint
);

CREATE INDEX IF NOT EXISTS idx_query_store_health_time ON collect.query_store_health(server_id, capture_time);

CREATE OR REPLACE VIEW v_query_store_health AS SELECT * FROM query_store_health;";

    /// <summary>
    /// V75 — the plan-content retention knob (#2316). The payload dimensions' GC horizon is coupled to
    /// the WIDEST dim-feeding fact retention (90 days) so a raised override can never orphan a reader —
    /// which also means a store younger than that horizon has an UNBOUNDED plan dimension: measured on
    /// the 42-server dogfood fleet, <c>query_plan_dim</c> reached 127 GB (63% of the store) in its first
    /// 22 days, growing ~6 GB/day of parameter-sniffing recompile churn (65 distinct XMLs per plan SHAPE
    /// per day; the worst single shape produced 57k in one day), with the coupled GC unable to delete a
    /// single row until the horizon crossed the dim's birth date — a month AFTER the projected disk-full.
    /// This knob decouples plan CONTENT lifetime from fact lifetime: facts keep their full retention
    /// (metrics, hashes and text stay analyzable); stored plan XML older than this many days since last
    /// sighting becomes unfetchable, which every reader already renders as a missing plan. Clamped on
    /// READ like the V59 knobs ([7,365]; 0 = disabled, restoring the fact-coupled horizon alone).
    /// </summary>
    private const string V75Sql = @"
ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS plan_content_retention_days integer NOT NULL DEFAULT 21;";

    /// <summary>
    /// V74 — where the query-text fetch lands statement text (#2150), keyed
    /// <c>(server_id, database_name, query_id)</c>.
    ///
    /// <para>The runtime-stats payload carried <c>query_sql_text</c> (<c>nvarchar(max)</c>) inside a
    /// <c>TOP ... WITH TIES ... ORDER BY last_execution_time</c>, and a Top-N Sort carries every output
    /// column through the sort while reading ALL of its input first — so choosing the rows to ship
    /// materialized text for the entire qualifying set. With #2210's plan XML already gone and that column
    /// as the only difference, time-to-first-row measured 4.67s against 0.45s at 1,505 rows and 5.02s
    /// against 0.57s at 4,037.</para>
    ///
    /// <para>Keyed on <c>query_id</c> rather than <c>query_text_id</c> because <c>query_id</c> is ALREADY a
    /// stored column on the fact table — so this rung adds a table and touches nothing existing, and readers
    /// get the join key for free. Text is stored INLINE rather than as a digest into <c>query_plan_dim</c>'s
    /// sibling: Query Store already de-duplicates it one row per statement per database, so there is nothing
    /// to squeeze, and inline removes the dimension GC liveness interlock whose failure mode is silently
    /// missing text. Not a hypertable — it has a PRIMARY KEY and no time dimension — so it is pruned on
    /// <c>last_seen</c> rather than by <c>drop_chunks</c>, exactly like <c>query_store_plan_map</c>.</para>
    /// </summary>
    private const string V74Sql = @"CREATE TABLE IF NOT EXISTS collect.query_store_text (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    query_id bigint NOT NULL,
    query_sql_text text,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, database_name, query_id)
);
CREATE INDEX IF NOT EXISTS idx_query_store_text_last_seen
    ON collect.query_store_text(last_seen);";

    /// <summary>
    /// V72 — the Query Store plan map (#2210): <c>(server_id, database_name, plan_id) → digest</c>, so Query
    /// Store facts can reference plan XML they no longer carry once the cutover moves that content into the
    /// shared <c>query_plan_dim</c>. Plan XML was stored INLINE on <c>query_store_stats</c> at roughly 5x
    /// redundancy — the same plans re-shipped pass after pass — which is what this replaces.
    ///
    /// <para><c>plan_hash</c> is the re-verification key and is nullable on purpose: rows written before it
    /// existed re-verify once and self-heal. <c>last_seen</c> is the liveness column the map prune sweeps and
    /// the batch touch refreshes — load-bearing, because the dimension GC decides what to collect from
    /// <c>last_seen</c> rather than by counting references, and ending the re-shipping ends the signal that
    /// used to keep those dim rows alive.</para>
    ///
    /// <para>NUMBERED <c>max(dev) + 1</c>, WITHOUT a gap, and that is the load-bearing part. The runner skips
    /// any rung at or below the store's stamped version, so a gap left for another in-flight branch is skipped
    /// SILENTLY on every upgraded store the moment this one stamps a higher number. Gapping is only safe when
    /// the gap-filler lands first, which a branch cannot guarantee about another branch.</para>
    ///
    /// <para>The race this comment was written to survive HAPPENED: it was V61 while #2213 and the PostgreSQL
    /// collector rungs were in flight, they merged first and took the ladder to V71, and the collision surfaced
    /// as a conflict on the migration list — loudly, on the merge, exactly as intended — so this renumbered to
    /// sit immediately above them. A collision is loud; a gap is silent, and a map table that was never created
    /// reads as "plan not yet collected" on every lookup, so the cutover would look healthy and hold nothing.</para>
    /// </summary>
    private const string V72Sql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_plan_map (
    server_id integer NOT NULL,
    database_name text NOT NULL,
    plan_id bigint NOT NULL,
    digest bytea NOT NULL,
    plan_hash text,
    last_seen timestamp NOT NULL,
    PRIMARY KEY (server_id, database_name, plan_id)
);
CREATE INDEX IF NOT EXISTS idx_query_store_plan_map_last_seen
    ON collect.query_store_plan_map(last_seen);";

    /// <summary>
    /// V9 — the FinOps copy-parity fields that were user-input config or previously live-only:
    /// <c>server_properties</c> gains the three inventory columns the shared ServerPropertiesCollector now
    /// SELECTs (start time / host OS / AG replica role — Lite's FinOps Server Inventory previously read
    /// them from a LIVE query the headless viewer can't run), and <c>servers</c> gains
    /// <c>monthly_cost_usd</c> (the per-server FinOps budget — Lite's <c>ServerConnection.MonthlyCostUsd</c>,
    /// user config, upserted from darling.json). All are nullable appended columns: a fresh store's V1
    /// server_properties is generated from the current collector (which already includes the three), and
    /// V2's servers table gets the cost column here — so <c>ADD COLUMN IF NOT EXISTS</c> is a harmless
    /// no-op on the generated columns and the real add on an upgraded store. Appended (not inserted) so a
    /// fresh V1 store and an upgraded store keep an identical physical column order for the binary COPY.
    /// The bare names resolve through the migrate session's <c>search_path = collect, config, public</c>
    /// (V8) to <c>collect.server_properties</c> / <c>collect.servers</c>.
    /// </summary>
    private const string V9Sql = @"
ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS sqlserver_start_time timestamp;
ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS host_os_version text;
ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS ag_replica_role text;
ALTER TABLE servers ADD COLUMN IF NOT EXISTS monthly_cost_usd numeric;";

    /// <summary>
    /// V15 — the per-index DEFINITION metadata monitor-side UNUSED/DUPLICATE index analysis needs
    /// (FinOps Index Analysis, Stage 1), added additively to <c>index_object_stats</c>: the ordered
    /// <c>key_columns</c> / <c>included_columns</c> lists (sp_IndexCleanup's delimited representation,
    /// so the Stage-2 analyzer's string-comparison dedupe ports cleanly), <c>filter_definition</c>,
    /// the uniqueness/constraint/FK discriminators (<c>is_unique_constraint</c>, <c>is_foreign_key</c>,
    /// <c>is_foreign_key_reference</c>) + <c>is_disabled</c>, and the reconstruct-a-CREATE options
    /// (<c>data_compression_desc</c>, <c>optimize_for_sequential_key</c>, <c>fill_factor</c>,
    /// <c>is_padded</c>, <c>allow_page_locks</c>, <c>allow_row_locks</c>). Every column is nullable and
    /// appended, so a fresh store's V1 <c>index_object_stats</c> (generated from the current collector
    /// definition, which now includes them) already has them and <c>ADD COLUMN IF NOT EXISTS</c>
    /// no-ops, while an upgraded store gets the real add — with an identical physical column order for
    /// the binary COPY either way. The trailing <c>CREATE OR REPLACE VIEW</c> re-expands
    /// <c>v_index_object_stats</c>' pinned <c>SELECT *</c> (Postgres freezes it at CREATE, so an
    /// upgraded store's view — last refreshed by V14 before these columns existed — would otherwise
    /// omit them; append-only ADDs keep the refresh legal). Runs after V8, so the bare names resolve
    /// through <c>search_path = collect, config, public</c>.
    /// </summary>
    private const string V15Sql = @"
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS key_columns text;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS included_columns text;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS filter_definition text;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_unique_constraint boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key_reference boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_disabled boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS data_compression_desc text;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS optimize_for_sequential_key boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS fill_factor smallint;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_padded boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_page_locks boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_row_locks boolean;
ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_indexed_view boolean;

CREATE OR REPLACE VIEW v_index_object_stats AS SELECT * FROM index_object_stats;";

    /// <summary>
    /// V16 — the monitored server's UTC offset, added additively to <c>server_properties</c> so the
    /// headless viewer can render timestamps in the server's own local time (the Server-time display
    /// mode ported from Lite). The store is naive-UTC; Server-time = UTC + this offset. It is nullable
    /// and appended, so a fresh store's V1 <c>server_properties</c> (generated from the current collector
    /// definition, which now includes it) already has it and <c>ADD COLUMN IF NOT EXISTS</c> no-ops,
    /// while an upgraded store gets the real add — with an identical physical column order for the binary
    /// COPY either way. <c>server_properties</c> has no <c>v_*</c> passthrough view, so nothing to refresh.
    /// Runs after V8, so the bare name resolves through <c>search_path = collect, config, public</c>.
    /// </summary>
    private const string V16Sql = @"
ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS utc_offset_minutes integer;";

    /// <summary>
    /// V17 — the store&lt;-&gt;service control plane (Phase A, Stage 1): the six operator-writable
    /// <c>config.*</c> tables the Viewer will WRITE and the service READS + honors on the
    /// <c>config_version</c> reload beacon. Two directions over the V8 <c>config</c> schema (the
    /// admin-writable surface): the config plane (desired state — monitored servers, alert /
    /// analysis knobs, notification delivery, per-collector schedule overrides, service flags) and
    /// the command plane (<c>config_command</c>, the imperative queue Stage 2 executes).
    ///
    /// <para><b>CRITICAL — every object is schema-qualified <c>config.&lt;name&gt;</c>.</b> V17 is
    /// the first migration to CREATE directly in <c>config</c>; the migrate session runs under
    /// <c>search_path = collect, config, public</c> (<see cref="PgSchemaGenerator.SearchPath"/>), so
    /// an UNQUALIFIED <c>CREATE TABLE foo</c> would resolve to <c>collect</c> (first in the path) —
    /// wrong schema, wrong ACL (the V8 split grants config-writes to <c>admin</c> only). Qualifying
    /// every table/index/function/trigger with <c>config.</c> pins them into the admin-writable
    /// schema. The managed role provisioning (<c>DarlingManagedRoles</c>) and BYO
    /// <c>tools/provision-roles.sql</c> both <c>GRANT … ON ALL TABLES IN SCHEMA config</c> after
    /// migration + carry <c>ALTER DEFAULT PRIVILEGES</c>, so these new tables auto-inherit the
    /// admin/viewer grants with no per-table grant; <c>config_command</c> uses
    /// <c>GENERATED ALWAYS AS IDENTITY</c> (not <c>serial</c>) so INSERTs need no sequence USAGE.
    ///
    /// <para>The <c>config_version</c> reload beacon: statement-level bump triggers on the four
    /// desired-state tables increment <c>config_service.config_version</c> on any write (so the
    /// Viewer cannot forget to signal), and a BEFORE-UPDATE trigger on <c>config_service</c> itself
    /// self-increments the beacon on direct writes (pause/capture/mcp) without recursing — the
    /// service polls this one integer each sweep and reloads only when it changes. Single-row global
    /// tables are pinned to <c>id = 1</c>; <c>config_collector_schedules</c> is sparse (absent row /
    /// NULL column = the <c>CollectorScheduleDefaults</c> code default, <c>server_id</c> NULL =
    /// fleet-wide) with two partial-unique indexes because a PRIMARY KEY cannot span a nullable
    /// <c>server_id</c>. Timestamps store naive-UTC (<c>now() AT TIME ZONE 'UTC'</c>) to match the
    /// store-wide convention (Npgsql rejects Kind=Utc against <c>timestamp</c>). Server secrets are
    /// NEVER plaintext here — <c>encrypted_password</c>/<c>smtp_encrypted_password</c> are the
    /// DPAPI blobs (the <c>--encrypt-password</c> pattern); integrated auth needs none.</para>
    /// </summary>
    private const string V17Sql = @"
/* V17: store<->service control plane (Stage 1). EVERY object is schema-qualified config.* —
   the migrate session's search_path resolves bare names to collect (wrong schema/ACL). */

/* --- A. Config plane: the Viewer writes desired state, the service reads + honors it. --- */

/* 1. config_monitored_servers — the desired-state twin of the collect.servers observed registry.
      server_id is THIS ROW'S identity and this table owns it: the service reads it here rather than
      recomputing it, so a stored id keeps working when the fields below no longer produce it (#2218).
      It is minted from the storage identity host[:database][:RO] — the same value the collectors
      stamp, which is why it JOINs collected data and why no existing store needs migrating — but that
      is now the ALLOCATION rule, not a definition anything re-derives. is_enabled drives collection;
      the connection fields reconstruct a MonitoredServer for the service's connect path. */
CREATE TABLE IF NOT EXISTS config.config_monitored_servers (
    server_id integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    host text NOT NULL,
    database text,
    auth text NOT NULL DEFAULT 'integrated',
    username text,
    encrypted_password text,
    encrypt_mode text NOT NULL DEFAULT 'Mandatory',
    trust_server_certificate boolean NOT NULL DEFAULT FALSE,
    read_only_intent boolean NOT NULL DEFAULT FALSE,
    multi_subnet_failover boolean NOT NULL DEFAULT FALSE,
    excluded_databases text[] NOT NULL DEFAULT '{}'::text[],
    monthly_cost_usd numeric NOT NULL DEFAULT 0,
    capture_plans boolean,
    is_enabled boolean NOT NULL DEFAULT TRUE,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    modified_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

/* 2. config_alert_settings — single row (id=1), one column per AlertsConfig field + the analysis
      cadence knobs (analysis_enabled / interval / notifications_enabled / notify_severity). */
CREATE TABLE IF NOT EXISTS config.config_alert_settings (
    id smallint NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    enabled boolean NOT NULL DEFAULT TRUE,
    cpu_enabled boolean NOT NULL DEFAULT TRUE,
    cpu_threshold_percent integer NOT NULL DEFAULT 80,
    cpu_mode text NOT NULL DEFAULT 'total',
    blocking_enabled boolean NOT NULL DEFAULT TRUE,
    blocking_count_threshold integer NOT NULL DEFAULT 1,
    deadlock_enabled boolean NOT NULL DEFAULT TRUE,
    deadlock_count_threshold integer NOT NULL DEFAULT 1,
    poison_wait_enabled boolean NOT NULL DEFAULT TRUE,
    poison_wait_threshold_ms integer NOT NULL DEFAULT 500,
    long_running_query_enabled boolean NOT NULL DEFAULT TRUE,
    long_running_query_threshold_minutes integer NOT NULL DEFAULT 30,
    tempdb_space_enabled boolean NOT NULL DEFAULT TRUE,
    tempdb_space_threshold_percent integer NOT NULL DEFAULT 80,
    low_disk_enabled boolean NOT NULL DEFAULT TRUE,
    low_disk_threshold_percent integer NOT NULL DEFAULT 10,
    low_disk_threshold_gb integer NOT NULL DEFAULT 5,
    long_running_job_enabled boolean NOT NULL DEFAULT TRUE,
    long_running_job_multiplier integer NOT NULL DEFAULT 3,
    failed_job_enabled boolean NOT NULL DEFAULT TRUE,
    failed_job_lookback_minutes integer NOT NULL DEFAULT 60,
    cooldown_minutes integer NOT NULL DEFAULT 5,
    excluded_databases text[] NOT NULL DEFAULT '{}'::text[],
    analysis_enabled boolean NOT NULL DEFAULT TRUE,
    analysis_interval_minutes integer NOT NULL DEFAULT 30,
    analysis_notifications_enabled boolean NOT NULL DEFAULT TRUE,
    analysis_notify_severity double precision NOT NULL DEFAULT 1.5,
    modified_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

/* 3. config_notification — single row: SMTP + webhook delivery. Non-secret fields plus the SMTP
      DPAPI blob (smtp_encrypted_password); webhook URLs/proxies carry as darling.json holds them. */
CREATE TABLE IF NOT EXISTS config.config_notification (
    id smallint NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    smtp_host text NOT NULL DEFAULT '',
    smtp_port integer NOT NULL DEFAULT 587,
    smtp_use_ssl boolean NOT NULL DEFAULT TRUE,
    smtp_username text,
    smtp_encrypted_password text,
    smtp_from_address text NOT NULL DEFAULT '',
    smtp_recipients text NOT NULL DEFAULT '',
    email_cooldown_minutes integer NOT NULL DEFAULT 15,
    teams_url text NOT NULL DEFAULT '',
    teams_proxy text NOT NULL DEFAULT '',
    slack_url text NOT NULL DEFAULT '',
    slack_proxy text NOT NULL DEFAULT '',
    modified_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

/* 4. config_collector_schedules — SPARSE per-collector overrides layered on CollectorScheduleDefaults.
      Absent row / NULL column = code default; server_id NULL = fleet-wide. A PRIMARY KEY cannot span a
      nullable server_id, so two partial-unique indexes enforce one fleet-wide row + one per-server row
      per collector. */
CREATE TABLE IF NOT EXISTS config.config_collector_schedules (
    server_id integer,
    collector_name text NOT NULL,
    frequency_minutes integer CHECK (frequency_minutes >= 0),
    retention_days integer CHECK (retention_days >= 1),
    enabled boolean NOT NULL DEFAULT TRUE
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_config_collector_schedules_fleet
    ON config.config_collector_schedules (collector_name) WHERE server_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_config_collector_schedules_server
    ON config.config_collector_schedules (server_id, collector_name) WHERE server_id IS NOT NULL;

/* 5. config_service — single row: the service-wide flags + the config_version reload beacon. */
CREATE TABLE IF NOT EXISTS config.config_service (
    id smallint NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    paused boolean NOT NULL DEFAULT FALSE,
    capture_plans boolean NOT NULL DEFAULT TRUE,
    mcp_enabled boolean NOT NULL DEFAULT FALSE,
    mcp_port integer NOT NULL DEFAULT 5152,
    config_version bigint NOT NULL DEFAULT 0,
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_by text
);

/* --- B. Command plane: the Viewer enqueues, the service (Stage 2) claims/executes/reports. --- */

/* 6. config_command — the imperative queue. GENERATED ALWAYS AS IDENTITY (a natural queue key that
      needs no sequence USAGE grant for admin INSERTs, unlike serial). */
CREATE TABLE IF NOT EXISTS config.config_command (
    command_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    requested_by text,
    command_type text NOT NULL,
    target_server_id integer,
    args_json jsonb,
    status text NOT NULL DEFAULT 'pending',
    claimed_at timestamp,
    completed_at timestamp,
    result_status text,
    result_json jsonb,
    service_instance text
);
CREATE INDEX IF NOT EXISTS idx_config_command_status ON config.config_command (status);

/* --- C. The config_version reload beacon (bump triggers). --- */

/* Statement-level bump on the four desired-state tables: any write increments the beacon so the
   Viewer can never forget to signal. Targets config_service by qualified name (SECURITY INVOKER —
   both writers, the owner during seed and admin via the Viewer, hold UPDATE on config_service). */
CREATE OR REPLACE FUNCTION config.config_bump_version() RETURNS trigger
LANGUAGE plpgsql AS $bump$
BEGIN
    UPDATE config.config_service
       SET config_version = config_version + 1,
           updated_at = (now() AT TIME ZONE 'UTC')
     WHERE id = 1;
    RETURN NULL;
END;
$bump$;

/* Direct writes to config_service (pause/capture/mcp) self-bump the beacon without recursion: the
   BEFORE-UPDATE trigger increments NEW.config_version only when the writer did not already change it
   (so the config_bump_version UPDATE above, which sets config_version explicitly, is not doubled). */
CREATE OR REPLACE FUNCTION config.config_service_bump() RETURNS trigger
LANGUAGE plpgsql AS $svc$
BEGIN
    IF NEW.config_version = OLD.config_version THEN
        NEW.config_version := OLD.config_version + 1;
    END IF;
    NEW.updated_at := (now() AT TIME ZONE 'UTC');
    RETURN NEW;
END;
$svc$;

DROP TRIGGER IF EXISTS trg_bump_monitored_servers ON config.config_monitored_servers;
CREATE TRIGGER trg_bump_monitored_servers
    AFTER INSERT OR UPDATE OR DELETE ON config.config_monitored_servers
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();

DROP TRIGGER IF EXISTS trg_bump_alert_settings ON config.config_alert_settings;
CREATE TRIGGER trg_bump_alert_settings
    AFTER INSERT OR UPDATE OR DELETE ON config.config_alert_settings
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();

DROP TRIGGER IF EXISTS trg_bump_notification ON config.config_notification;
CREATE TRIGGER trg_bump_notification
    AFTER INSERT OR UPDATE OR DELETE ON config.config_notification
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();

DROP TRIGGER IF EXISTS trg_bump_collector_schedules ON config.config_collector_schedules;
CREATE TRIGGER trg_bump_collector_schedules
    AFTER INSERT OR UPDATE OR DELETE ON config.config_collector_schedules
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();

DROP TRIGGER IF EXISTS trg_service_self_bump ON config.config_service;
CREATE TRIGGER trg_service_self_bump
    BEFORE UPDATE ON config.config_service
    FOR EACH ROW EXECUTE FUNCTION config.config_service_bump();";

    /// <summary>
    /// V18 — the per-server + per-event alert delivery mode (#1236 / #1141) the Viewer writes and the
    /// service honors at delivery time. The GLOBAL default lives on <c>config_alert_settings</c>
    /// (<c>delivery_mode</c> = Summary/PerEvent, <c>per_event_max</c> = the "+N more" cap the shared
    /// <c>PerEventNotification.Split</c> applies); a PER-SERVER override lives on
    /// <c>config_monitored_servers</c> (<c>alert_delivery_mode_override</c>, nullable = "use the global"),
    /// resolved through the shared <c>AlertDeliveryModeResolver</c>. Additive ALTERs, schema-qualified
    /// <c>config.*</c> exactly like V17 (the migrate session's <c>search_path = collect, config, public</c>
    /// resolves a bare name to <c>collect</c> — the wrong schema/ACL); <c>IF NOT EXISTS</c> matches the
    /// file's ALTER idiom (V7/V9/V15/V16) so a re-run is a harmless no-op. The two settings columns are
    /// NOT NULL with the shipped defaults (Summary / 5) so the single seeded row comes up honoring Summary;
    /// the per-server column is nullable so an un-overridden server inherits the global. Neither table has a
    /// <c>v_*</c> passthrough view, so nothing to refresh.
    /// </summary>
    private const string V18Sql = @"
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS delivery_mode text NOT NULL DEFAULT 'Summary';
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS per_event_max integer NOT NULL DEFAULT 5;
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS alert_delivery_mode_override text;";

    /// <summary>
    /// V19 — the per-server ANALYSIS-STATE marker: the analysis pass's insufficient-data determination,
    /// persisted so the Viewer's Recommendations tab can tell a young deployment ("still collecting —
    /// need ~24h of history") apart from a genuine all-clear. The engine ALREADY computes this
    /// (<c>DarlingAnalysisService.InsufficientDataMessage</c>, surfaced as the AN3 pass's
    /// <c>InsufficientData</c> status when total history is under the 24h data-span gate); the Viewer
    /// makes no engine call, so without a persisted marker a zero-finding read collapses to "All clear"
    /// even when the engine only skipped for want of data. One row per server (<c>server_id</c> PRIMARY
    /// KEY), upserted by the service after each completed pass and read by the Viewer.
    ///
    /// <para><b>Schema — <c>collect</c>, qualified.</b> This is service-produced OBSERVED analysis output
    /// the Viewer READS, so it belongs in <c>collect</c> beside <c>analysis_findings</c> /
    /// <c>collection_log</c> / <c>servers</c> — NOT the <c>config</c> control plane (which is the opposite
    /// direction: the Viewer writes DESIRED state the service reads). The service connects as the owner
    /// and writes it; the managed <c>admin</c>/<c>viewer</c> roles auto-inherit SELECT via
    /// <c>ALTER DEFAULT PRIVILEGES … IN SCHEMA collect</c> (<see cref="!:DarlingManagedRoles"/>), so no
    /// per-table grant is needed. Qualifying <c>collect.</c> is belt-and-suspenders — the migrate
    /// session's <c>search_path = collect, config, public</c> already resolves a bare name to
    /// <c>collect</c> — but it makes the intent explicit (the mirror of V17/V18's <c>config.</c>
    /// qualification). <c>message</c> is nullable (null when a real pass cleared the gate);
    /// <c>analysis_time</c> stores naive-UTC like the store-wide convention. No <c>v_*</c> passthrough
    /// view: no Lite SQL ports through this Darling-specific marker.
    /// </summary>
    private const string V19Sql = @"
CREATE TABLE IF NOT EXISTS collect.analysis_state (
    server_id integer NOT NULL PRIMARY KEY,
    insufficient_data boolean NOT NULL DEFAULT FALSE,
    message text,
    analysis_time timestamp NOT NULL
);";

    /// <summary>
    /// V20 — the alert-tuning knobs the Viewer writes and the service honors that had no store column before:
    /// the long-running-query read shape (<c>long_running_query_max_results</c> + the five noise-filter opt-outs
    /// the shared <c>AlertEngine</c> forwards to <c>GetLongRunningQueriesAsync</c>) and
    /// <c>notify_connection_changes</c> (the Server-Unreachable/Restored connect-edge gate in
    /// <see cref="!:DarlingSelfAlertEvaluator"/>). Before this the service HARDCODED all of them (Lite's App
    /// defaults: max 5, every filter on, connection-notify on), so suppression happened by default but the
    /// operator could not customize it — the Settings controls were inert. All columns are additive
    /// <c>ADD COLUMN IF NOT EXISTS</c> and NOT NULL with the shipped defaults, so a pre-V20 store's single
    /// seeded row comes up honoring exactly the old hardcoded behavior and a re-run is a harmless no-op.
    /// Schema-qualified <c>config.*</c> exactly like V17/V18 (the migrate session's
    /// <c>search_path = collect, config, public</c> resolves a bare name to <c>collect</c> — the wrong
    /// schema/ACL). <c>config_alert_settings</c> has no <c>v_*</c> passthrough view, so nothing to refresh.
    /// </summary>
    private const string V20Sql = @"
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_max_results integer NOT NULL DEFAULT 5;
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_sp_server_diagnostics boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_wait_for boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_backups boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_misc_waits boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_cdc boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS notify_connection_changes boolean NOT NULL DEFAULT TRUE;";

    /// <summary>
    /// V21 — the <c>default_trace_events</c> collector table (built-in Default Trace read via
    /// <c>sys.fn_trace_gettable</c>: file auto-grow/shrink stalls, severe ErrorLog writes, schema DDL,
    /// security audits, Server Memory Change). A NEW collector table added additively for a store built
    /// before this collector existed; a FRESH store already has it (V1's
    /// <see cref="PgSchemaGenerator.GenerateFullSchema"/> now walks the collector — which includes
    /// default_trace_events — and V8 moved it to <c>collect</c>), so <c>CREATE TABLE IF NOT EXISTS</c> is a
    /// harmless no-op on fresh and the real create on upgrade, with an identical <c>collect.default_trace_events</c>
    /// shape either way. EXPLICITLY <c>collect.</c>-qualified (mirroring V19's <c>analysis_state</c>): this
    /// runs after V8, whose <c>search_path = collect, config, public</c> already resolves a bare name to
    /// <c>collect</c>, but the qualification makes the collect-schema intent explicit. Column order + types
    /// mirror the generated V1 shape exactly (the NOT NULL prefix, no PRIMARY KEY — the hypertable / binary
    /// COPY reasoning), so the positional COPY aligns on both paths.
    ///
    /// <para>It has NO <c>v_*</c> passthrough view (no Lite analysis SQL ports through it — the
    /// <c>get_default_trace_events</c> MCP tool reads the base table directly, exactly like
    /// <c>server_properties</c>), so the <see cref="PgSchemaGenerator.AllPassthroughViews"/> cross-check is
    /// unaffected. TimescaleDB hypertable conversion + compression + retention all flow from the catalog
    /// automatically (TimescaleSupport / DarlingRetention walk <see cref="CollectorCatalog.All"/>).</para>
    /// </summary>
    private const string V21Sql = @"
CREATE TABLE IF NOT EXISTS collect.default_trace_events (
    default_trace_event_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    event_time timestamp,
    event_name text,
    event_class integer,
    spid integer,
    database_name text,
    database_id integer,
    login_name text,
    host_name text,
    application_name text,
    object_name text,
    filename text,
    integer_data bigint,
    integer_data_2 bigint,
    text_data text,
    session_login_name text,
    error_number integer,
    severity integer,
    state integer,
    event_sequence bigint,
    duration_us bigint,
    end_time timestamp
);

CREATE INDEX IF NOT EXISTS idx_default_trace_events_time ON collect.default_trace_events(server_id, collection_time);";

    /// <summary>
    /// V22 — the supporting index for the hot FinOps Index Analysis read. The viewer's
    /// <c>ViewerDataService.IndexObjectStatsLatestSql</c> picks the newest row per index identity with
    /// <c>SELECT DISTINCT ON (database_id, object_id, index_id) … WHERE server_id = $1
    /// ORDER BY database_id, object_id, index_id, collection_time DESC</c> — no time bound, so it walks the
    /// whole per-server history (daily cadence, 90-day retention) to find each index's latest snapshot. The V1
    /// index <c>idx_index_object_stats_object</c> leads <c>(server_id, database_name, …)</c> — a database_NAME
    /// vs the query's database_ID mismatch, so its ordering cannot satisfy the <c>DISTINCT ON (database_id, …)</c>
    /// and Postgres sorts the entire server row set. This index matches the read's exact
    /// <c>DISTINCT ON</c>/<c>ORDER BY</c> key (<c>server_id, database_id, object_id, index_id,
    /// collection_time DESC</c>), so the <c>DISTINCT ON</c> reads in index order with no sort. It is additive:
    /// it does NOT replace the V1 index (the anomaly-detector self-joins in <c>PgAnomalyDetector</c> key
    /// <c>database_name</c>, which that index still serves) — the two readers diverged on the database key, so
    /// each gets its own index.
    ///
    /// <para><c>index_object_stats</c> is a TimescaleDB hypertable (every collector table is —
    /// <c>TimescaleSupport.HypertableTables</c>), so a plain <c>CREATE INDEX</c> is applied across every chunk
    /// by Timescale (the standard post-hypertable index path, the same shape as the V1/V21 collector indexes);
    /// on a plain-PostgreSQL store it is an ordinary btree. It is a NON-unique index that includes the partition
    /// column (<c>collection_time</c>), so it is legal on the hypertable either way. <c>CREATE INDEX IF NOT
    /// EXISTS</c> makes it idempotent: an existing V21 store gets the real create, a fresh store (whose V1 built
    /// only the differently-keyed <c>idx_index_object_stats_object</c>) gets this second index when its
    /// migrations run through V22, and a re-run is a harmless no-op. EXPLICITLY <c>collect.</c>-qualified like
    /// V21 (the migrate session's <c>search_path = collect, config, public</c> already resolves the bare name
    /// to <c>collect</c>, but the qualification makes the collect-schema intent explicit). No table shape
    /// changes, so nothing to refresh for the binary COPY.</para>
    /// </summary>
    private const string V22Sql = @"
CREATE INDEX IF NOT EXISTS idx_index_object_stats_latest ON collect.index_object_stats (server_id, database_id, object_id, index_id, collection_time DESC);";

    /// <summary>
    /// V23 — converts <c>collect.collection_log</c> (the per-collector-run observability log, the store's
    /// highest-volume plain table) from a heap to a TimescaleDB hypertable, and applies the same compression
    /// policy the collector hypertables get. As a hypertable its daily retention becomes O(1)
    /// <c>drop_chunks</c> (no DELETE churn — see <see cref="!:DarlingRetention"/>), the Overview's per-server
    /// <c>MAX(collection_time)</c> freshness read touches only the newest chunk per server (chunk exclusion on
    /// the existing <c>idx_collection_log_time (server_id, collection_time)</c> index, kept), and old chunks
    /// compress — the scale-readiness pass for 500 servers.
    ///
    /// <para><b>Engine-plain, GUARDED, and NON-FATAL — a best-effort UPGRADE fast-path, NOT the authoritative
    /// conversion.</b> The versioned migrations must run on plain PostgreSQL too (the store works with or
    /// without TimescaleDB — see <see cref="TimescaleSupport"/>), where <c>create_hypertable</c> does not
    /// exist. So the body is a <c>DO</c> block gated on <c>pg_extension</c> (plain PostgreSQL skips it — plpgsql
    /// parses the guarded statements lazily, so the missing function is never parsed) AND wrapped in an
    /// <c>EXCEPTION WHEN OTHERS</c> handler so any failure only RAISEs a WARNING and the migration COMMITS
    /// regardless. That non-fatality is deliberate and load-bearing: <c>MigrateAsync</c> runs in the service's
    /// startup-CRITICAL path (a thrown migration aborts startup), and it runs BEFORE the runtime
    /// <c>CREATE EXTENSION</c> (<see cref="TimescaleSupport.TryEnableAsync"/>) — so on a FRESH managed store the
    /// guard here is false (the extension is not created yet) and this block no-ops. The AUTHORITATIVE,
    /// proven-live, self-healing conversion is <see cref="TimescaleSupport.EnsureCollectionLogHypertableAsync"/>,
    /// applied at runtime AFTER the extension exists (the same non-transactional path validated for every
    /// collector table). This migration only wins the case where an UPGRADE's store already had the extension
    /// created by a prior startup — then it converts collection_log's existing rows a step early — and quietly
    /// yields to the runtime path otherwise. <c>collection_log</c> lives OUTSIDE
    /// <see cref="CollectorCatalog.All"/> (it is a registry-side table with no <c>ICollectorSchemaInfo</c>), so
    /// the catalog-driven convert/compress loops never reach it — it must be handled directly, here and at
    /// runtime.</para>
    ///
    /// <para><b>create_hypertable.</b> <c>migrate_data =&gt; true</c> moves existing rows into chunks (the
    /// migrate connection uses a long command timeout — <see cref="MigrationCommandTimeoutSeconds"/> — so a
    /// large / many-server backlog is not abandoned at the 30s default); <c>if_not_exists =&gt; true</c> makes
    /// a re-run or an already-converted table a harmless no-op NOTICE (idempotent, so it composes with the
    /// runtime path). <c>collection_log</c> has NO PRIMARY KEY / UNIQUE constraint (see V2), so the hypertable
    /// rule that a unique key must include the partition column cannot conflict — the usual main risk is simply
    /// absent. <c>collection_time</c> stays a naive <c>timestamp</c> (the product-wide cross-store contract), so
    /// <c>create_hypertable</c> emits the advisory use-TIMESTAMPTZ WARNING — expected, exactly like the
    /// collector hypertables.</para>
    ///
    /// <para><b>Compression.</b> Enabled + policy added mirroring <see cref="TimescaleSupport"/> for this one
    /// table: segment by <c>server_id</c> (every read filters it first) and compress chunks older than the same
    /// <see cref="TimescaleSupport.CompressAfterDays"/>, with the same <see cref="TimescaleSupport.ChunkIntervalDays"/>
    /// chunk width — the constants are interpolated so the two never drift. <c>if_not_exists</c> on the policy
    /// keeps it idempotent. Explicitly <c>collect.</c>-qualified like V21/V22.</para>
    /// </summary>
    private static string V23Sql =>
        $@"
/* V23: best-effort UPGRADE fast-path that converts collect.collection_log to a TimescaleDB hypertable + compresses
   it, mirroring the collector hypertables. GUARDED on the extension (plain PostgreSQL skips it, keeping the table a
   heap with batched-DELETE retention) and wrapped in EXCEPTION WHEN OTHERS so it can NEVER abort the startup-critical
   migration. The AUTHORITATIVE conversion is TimescaleSupport.EnsureCollectionLogHypertableAsync at runtime (after
   CREATE EXTENSION), which is the proven-live path and self-heals a fresh store this guard skipped. collection_log is
   NOT in the collector catalog, so the runtime catalog loops never touch it. */
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('collect.collection_log', by_range('collection_time', INTERVAL '{TimescaleSupport.ChunkIntervalDays} days'), if_not_exists => true, migrate_data => true);
        ALTER TABLE collect.collection_log SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id');
        PERFORM add_compression_policy('collect.collection_log', compress_after => INTERVAL '{TimescaleSupport.CompressAfterDays} days', if_not_exists => true);
    END IF;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'V23: deferred collection_log hypertable conversion to the runtime path (%): %', SQLSTATE, SQLERRM;
END
$$;";

    /// <summary>
    /// V24 — the <c>job_history</c> collector table (retained SQL Agent job-run history from
    /// <c>msdb.dbo.sysjobhistory</c>: every step row + the job-outcome row, deduped on the monotonic
    /// instance_id high-water mark, 365-day retention) for the fleet-wide Job History tab (issue #1433). A
    /// NEW collector table added additively for a store built before this collector existed; a FRESH store
    /// already has it (V1's <see cref="PgSchemaGenerator.GenerateFullSchema"/> walks the collector — which
    /// now includes job_history — and V8 moved it to <c>collect</c>), so <c>CREATE TABLE IF NOT EXISTS</c>
    /// is a harmless no-op on fresh and the real create on upgrade, with an identical
    /// <c>collect.job_history</c> shape either way. EXPLICITLY <c>collect.</c>-qualified (mirroring
    /// V21's default_trace_events): this runs after V8, whose <c>search_path</c> resolves a bare name to
    /// <c>collect</c>, but the explicit schema is defensive.
    /// <para>Column order/types are exactly <see cref="PgSchemaGenerator.CreateTable"/>'s output for the
    /// <see cref="JobHistoryCollector"/> catalog entry (prefix columns NOT NULL, payload columns nullable —
    /// the generator's convention). Like default_trace_events it has NO <c>v_*</c> passthrough view (a
    /// collector added after V14 cannot join the V14 view refresh; the viewer reads the base table directly,
    /// exactly like server_properties), so the <see cref="PgSchemaGenerator.AllPassthroughViews"/> cross-check
    /// is unaffected. TimescaleDB hypertable conversion + compression + 365-day retention all flow from the
    /// catalog at runtime (DarlingRetention / TimescaleSupport iterate CollectorCatalog.All), so none is
    /// emitted here.</para>
    /// </summary>
    private const string V24Sql = @"
CREATE TABLE IF NOT EXISTS collect.job_history (
    job_history_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    instance_id bigint,
    job_id text,
    job_name text,
    job_enabled boolean,
    category_name text,
    step_id integer,
    step_name text,
    run_status integer,
    run_status_desc text,
    run_datetime timestamp,
    run_duration_seconds bigint,
    retries_attempted integer,
    message text
);

CREATE INDEX IF NOT EXISTS idx_job_history_time ON collect.job_history(server_id, collection_time);";

    /// <summary>
    /// V25 — the <c>agent_status</c> collector table (SQL Agent service Running/Stopped from
    /// <c>sys.dm_server_services</c> + next scheduled run from <c>msdb.dbo.sysjobschedules</c>): the
    /// current-state snapshot behind the Job History tab header and the "Agent Not Running" self-alert
    /// (issue #1433 Phase 2). Added additively exactly like V21/V24 — a fresh store already has it (V1's
    /// <see cref="PgSchemaGenerator.GenerateFullSchema"/> walks the collector, V8 moved it to
    /// <c>collect</c>), so <c>CREATE TABLE IF NOT EXISTS</c> is a no-op on fresh and the real create on
    /// upgrade. Column order/types are exactly <see cref="PgSchemaGenerator.CreateTable"/>'s output for the
    /// <see cref="AgentStatusCollector"/> catalog entry (prefix NOT NULL, payload nullable). No <c>v_*</c>
    /// passthrough view (a post-V14 collector); the viewer reads the base table directly. Hypertable /
    /// compression / 7-day retention flow from the catalog at runtime.
    /// </summary>
    private const string V25Sql = @"
CREATE TABLE IF NOT EXISTS collect.agent_status (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    agent_running boolean,
    agent_status_desc text,
    agent_startup_desc text,
    next_scheduled_run timestamp
);

CREATE INDEX IF NOT EXISTS idx_agent_status_time ON collect.agent_status(server_id, collection_time);";

    /// <summary>
    /// V26 — the generic webhook channel (#1506): a third delivery channel beside Teams/Slack that POSTs an
    /// operator-authored JSON body to any endpoint, so an alert can drive automation we ship no adapter for
    /// (PagerDuty, Opsgenie, n8n, or a GitHub <c>repository_dispatch</c> that re-runs a workflow). It is the
    /// deliberate alternative to executing a script/exe on alert — same automation reach, no process-execution
    /// surface. Additive ALTERs on the V17 control-plane table, schema-qualified <c>config.*</c> and
    /// <c>IF NOT EXISTS</c> per the file's ALTER idiom (V7/V9/V15/V16/V18), so a re-run is a harmless no-op.
    /// The channel is enabled by a non-empty <c>generic_url</c> — the same derivation Teams/Slack use — so the
    /// empty-string defaults leave an upgraded store with the channel off.
    ///
    /// <para><b>Two of these columns are secrets.</b> <c>generic_url</c> is a bearer secret exactly like
    /// <c>teams_url</c>/<c>slack_url</c>, and <c>generic_headers</c> holds the <c>Authorization</c> token
    /// itself — both are carved out of the read-only <c>viewer</c> role's column grants in
    /// <c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>. That list's union-equals-the-table invariant is
    /// gated live by the build, so adding these columns without carving them there FAILS the gate rather than
    /// silently exposing a token to a viewer seat.</para>
    /// </summary>
    private const string V26Sql = @"
ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_url text NOT NULL DEFAULT '';
ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_headers text NOT NULL DEFAULT '';
ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_body_template text NOT NULL DEFAULT '';
ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_proxy text NOT NULL DEFAULT '';";

    private const string VersionTableSql = @"
CREATE TABLE IF NOT EXISTS darling_schema_version (
    version integer NOT NULL PRIMARY KEY,
    name text NOT NULL,
    applied_at timestamp NOT NULL
);";

    /// <summary>
    /// Session-scoped advisory lock key serializing concurrent migrators — two connections
    /// racing MigrateAsync (a second service instance misconfigured onto the same store, or
    /// parallel test classes) would otherwise both read the same current version and collide on
    /// the darling_schema_version primary key. Released explicitly and on connection close.
    /// </summary>
    private const long MigrationLockKey = 0x4441524C_494E47; /* "DARLING" */

    /// <summary>
    /// Command timeout (seconds) for applying one migration — well above Npgsql's 30s default so a
    /// data-moving migration is never abandoned half-done. V23 converts collection_log to a hypertable with
    /// <c>migrate_data =&gt; true</c>, which rewrites every existing row into chunks; on a long-collected /
    /// many-server store that can exceed 30s. Mirrors <see cref="TimescaleSupport"/>'s runtime-conversion
    /// budget. Harmless for the DDL-only migrations — they finish in milliseconds regardless.
    /// </summary>
    private const int MigrationCommandTimeoutSeconds = 300;

    /// <summary>
    /// Lock-wait budget (seconds) for the <c>pg_advisory_lock</c> ACQUIRE. A different quantity from
    /// <see cref="MigrationCommandTimeoutSeconds"/> above, which bounds ONE statement: the lock is taken once
    /// and held while <see cref="MigrateLockedAsync"/> applies EVERY pending rung in the same session, so what
    /// a sibling waits for here is a whole multi-rung session. Set to the statement bound (as this site first
    /// was) a several-rung upgrade can outlast the waiter while every individual statement stayed inside its
    /// own limit, so the two budgets are named separately and move independently.
    ///
    /// <para><b>Lower bound — the total the lock can legitimately be held for.</b> Of the 109 rungs in
    /// <see cref="Scripts"/> (V1-V110, V45 permanently absent), SIX touch data an earlier rung created, and
    /// FOUR of those are big enough to spend any of this budget: V22 (index built over every existing chunk
    /// of the populated <c>index_object_stats</c> hypertable), V23 (<c>create_hypertable</c> with
    /// <c>migrate_data =&gt; true</c>, rewriting <c>collection_log</c>'s rows into chunks), V39 (two partial
    /// indexes over the populated <c>query_stats</c> and <c>procedure_stats</c> hypertables) and V104 (index
    /// over the populated <c>pg_deadlocks</c>). The other two are costed at zero rather than overlooked —
    /// both are genuine DML against pre-existing rows, on targets that cannot carry a cost: V62 adds a CHECK
    /// constraint to <c>config.config_service</c>, which validates every existing row of a SINGLE-ROW
    /// control-plane table, and V77 deletes two watermark keys from <c>collect.collector_state</c>, which
    /// holds a few rows per server per collector rather than a hypertable's worth. Every other rung creates
    /// the table it then indexes, or is a metadata-only <c>ADD COLUMN</c> /
    /// <c>ALTER TABLE ... SET SCHEMA</c> / <c>DROP NOT NULL</c> / view refresh — the ladder measured
    /// 0.301 s end to end at 108 rungs on a fresh PostgreSQL 17.11 / TimescaleDB 2.29.2 store, advisory lock
    /// and version stamps included. The applier gives each rung's WHOLE SQL a single
    /// <see cref="MigrationCommandTimeoutSeconds"/>, so that bound is per-RUNG and V39's two indexes cost one
    /// multiple rather than two: four rungs is four times the bound for the data-moving part of a full
    /// V21-to-current upgrade, and the fifth multiple here is margin, sized at one rung bound because that is
    /// the granularity this ladder grows by — one new data-moving rung. Seeded reference points on that same
    /// local store, warm and on local NVMe (a cold busy store being why the rung bound is 300 s rather than
    /// 30): V22 1.29 s over 907 MB / 90 chunks, V23 9.37 s over a 608 MB heap, V39 1.44 s over 1.26 GB. The
    /// live 42-server store holds 2.72 GB, 0.69 GB and 24.5 GB in those same tables, so the real figures are
    /// larger and colder — which is the point of taking the floor from the per-rung bound rather than from
    /// these timings.</para>
    ///
    /// <para><b>Upper bound — there is none in the code, which is a finding rather than an omission.</b>
    /// <c>MigrateAsync</c> has exactly one production caller, <c>DarlingWorker.RunCollectionLoopAsync</c>, and
    /// it passes the plain stopping token: no <c>CancelAfter</c> anywhere on the path, no
    /// <c>HostOptions.StartupTimeout</c> configured (so the framework default is infinite), no health check or
    /// readiness probe in the repo, no <c>HEALTHCHECK</c> on the container image and no orchestrator manifest.
    /// The installers' 60 s / 2 min <c>WaitForStatus('Running')</c> do not bound it either — the worker is a
    /// <c>BackgroundService</c>, so the service reports Running before the first migration statement runs. The
    /// value therefore comes from the failure ASYMMETRY: too short and the waiter throws into
    /// <c>DarlingWorker</c>'s <c>LogCritical</c>-and-return, which takes that instance out of the collection
    /// loop entirely with no retry until an operator restarts it; too long and it only delays its own first
    /// cycle, because its MCP and web surfaces start independently and are already serving. Waiting is the
    /// cheap direction, so this errs long — but stays FINITE, so a genuinely wedged holder still produces a
    /// readable deadline instead of the silent hang #2874 exists to stop.</para>
    ///
    /// <para>A multiple rather than a literal so the two budgets cannot drift apart if the rung bound moves.
    /// This NARROWS the mid-wait death rather than closing it, and #2894 records the two ways it still dies:
    /// a fifth FLOOR-SETTING rung, or one rung that genuinely needs longer than its own bound. The first is
    /// now pinned rather than trusted — <c>MigrationDataMovingRungCensusPins</c> scans every rung's shipped
    /// SQL for data-moving shapes and fails when that set stops matching the census above, so a new one
    /// arrives carrying this derivation in a failure message instead of arriving silently. It resolves each
    /// statement's TARGET rather than counting keywords, because an index on a table the same rung creates is
    /// free and 130 of this ladder's 134 <c>CREATE INDEX</c> statements are that shape. The second way stays
    /// unpinned and unclosed: nothing here bounds a rung that overruns its own budget.</para>
    /// </summary>
    private const int MigrationLockWaitTimeoutSeconds = 5 * MigrationCommandTimeoutSeconds;

    /// <summary>
    /// Applies every migration newer than the store's current version, each in its own
    /// transaction, stamping darling_schema_version as it goes. Idempotent — a fully migrated
    /// store is a no-op — and safe under concurrent callers (advisory-locked). The connection
    /// must be open.
    /// </summary>
    public static Task<int> MigrateAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
        => MigrateAsync(connection, logger: null, cancellationToken);

    /// <summary>
    /// The logger-aware overload: after applying migrations it best-effort sets the database-default
    /// <c>search_path = collect, config, public</c> (V8 split) so EVERY future connection resolves the
    /// bare table names without a per-connection setting — the store-establishing side of migration.
    /// Best-effort because <c>ALTER DATABASE</c> needs the database-owner privilege a least-privilege
    /// BYO login may lack; a failure is warned (via <paramref name="logger"/>) but never fails the
    /// migration, since the moves already committed and the managed connection strings carry Search
    /// Path anyway (see <see cref="TrySetDatabaseSearchPathAsync"/>).
    /// </summary>
    public static async Task<int> MigrateAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using (var acquireLock = new NpgsqlCommand("SELECT pg_advisory_lock($1)", connection) { CommandTimeout = MigrationLockWaitTimeoutSeconds })
        {
            acquireLock.Parameters.AddWithValue(MigrationLockKey);
            await acquireLock.ExecuteNonQueryAsync(cancellationToken);
        }

        int applied;
        try
        {
            applied = await MigrateLockedAsync(connection, cancellationToken);
        }
        finally
        {
            try
            {
                using var releaseLock = new NpgsqlCommand("SELECT pg_advisory_unlock($1)", connection) { CommandTimeout = MigrationCommandTimeoutSeconds };
                releaseLock.Parameters.AddWithValue(MigrationLockKey);
                await releaseLock.ExecuteNonQueryAsync(CancellationToken.None);
            }
            catch
            {
                /* Connection close releases session advisory locks anyway. */
            }
        }

        /* Outside the advisory lock and the per-migration transactions: establish the durable
           database-default search_path for all future connections. Idempotent, best-effort. */
        await TrySetDatabaseSearchPathAsync(connection, logger, cancellationToken);
        return applied;
    }

    private static async Task<int> MigrateLockedAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        /* Resolve bare names through collect/config for this migrate session. Load-bearing from V8
           on: V8 moves darling_schema_version into collect, and the version stamp below writes it by
           its bare name — without this the post-move stamp would resolve against the default path
           ("$user", public) and fail. Setting a path whose schemas don't exist yet is legal (pre-V8
           they simply resolve to public, exactly as before), so this is safe on every store version
           and independent of any connection-string Search Path. Session-scoped (outside the
           per-migration transactions), so a migration rollback never unsets it. */
        using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection) { CommandTimeout = MigrationCommandTimeoutSeconds })
        {
            await setPath.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var createVersionTable = new NpgsqlCommand(VersionTableSql, connection) { CommandTimeout = MigrationCommandTimeoutSeconds })
        {
            await createVersionTable.ExecuteNonQueryAsync(cancellationToken);
        }

        int currentVersion;
        using (var readVersion = new NpgsqlCommand("SELECT COALESCE(MAX(version), 0) FROM darling_schema_version", connection) { CommandTimeout = MigrationCommandTimeoutSeconds })
        {
            currentVersion = Convert.ToInt32(await readVersion.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
        }

        var applied = 0;
        foreach (var migration in Scripts)
        {
            if (migration.Version <= currentVersion)
            {
                continue;
            }

            using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            using (var apply = new NpgsqlCommand(migration.Sql, connection, transaction) { CommandTimeout = MigrationCommandTimeoutSeconds })
            {
                await apply.ExecuteNonQueryAsync(cancellationToken);
            }

            using (var stamp = new NpgsqlCommand(
                "INSERT INTO darling_schema_version (version, name, applied_at) VALUES ($1, $2, $3)", connection, transaction) { CommandTimeout = MigrationCommandTimeoutSeconds })
            {
                stamp.Parameters.AddWithValue(migration.Version);
                stamp.Parameters.AddWithValue(migration.Name);
                /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` — see PgCollectorRowWriter. */
                stamp.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
                await stamp.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            applied++;
        }

        return applied;
    }

    /// <summary>
    /// Best-effort: make <c>search_path = collect, config, public</c> the database default, so
    /// EVERY future connection (the service pool's collector writes, the MCP host, the Viewer,
    /// <c>psql</c>/<c>pg_dump</c>, BYO) resolves the bare table names to the V8 schemas without any
    /// per-connection setting. Complements — does not replace — the session <c>SET</c> in
    /// <see cref="MigrateAsync"/> (which covers the migrate connection itself) and the
    /// <c>Search Path</c> keyword the managed connection strings carry.
    ///
    /// <para>Deliberately OUTSIDE the V8 transaction and swallowing failure: <c>ALTER DATABASE</c>
    /// needs the database-owner privilege, which a least-privilege bring-your-own-Postgres login may
    /// lack. A failure here must NOT fail the migration (the moves already committed) — it logs a
    /// warning telling the operator to run the statement themselves as owner. Idempotent, so it
    /// re-asserts harmlessly on every start. Targets the connection's live database name (managed =
    /// <c>darling</c>; BYO = whatever the operator connected to), identifier-quoted.</para>
    /// </summary>
    public static async Task TrySetDatabaseSearchPathAsync(
        NpgsqlConnection connection, ILogger? logger = null, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var databaseName = connection.Database;
        if (string.IsNullOrEmpty(databaseName))
        {
            return;
        }

        var quotedDatabase = "\"" + databaseName.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
        try
        {
            using var command = new NpgsqlCommand(
                $"ALTER DATABASE {quotedDatabase} SET search_path = {PgSchemaGenerator.SearchPath}", connection) { CommandTimeout = MigrationCommandTimeoutSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not set the database default search_path on {Database} ({Message}). The managed " +
                "connection strings still carry Search Path, but if you point your own tools at this store, " +
                "run this once as the database owner: ALTER DATABASE {Database} SET search_path = {SearchPath};",
                databaseName, ex.Message, databaseName, PgSchemaGenerator.SearchPath);
        }
    }
}
