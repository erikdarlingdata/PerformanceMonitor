/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the V2 observability migration (the servers registry + collection_log, column names
/// mirroring Lite's DuckDB schema) and exercises the service's writes end-to-end against a dev
/// Postgres when DARLING_TEST_PG is set: migrate (idempotent), upsert a fake server twice (the
/// second must not throw and must refresh modified_date), write one SUCCESS collection_log row,
/// read both back, clean up.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingObservabilityTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -424242;

    [Fact]
    public void MigrationScripts_AreRegisteredInAscendingOrder_V34AgCollectors_V36AgLatencyColumns()
    {
        /* Counted off the registered list rather than hard-coded: the count and the newest version are the
           same fact stated twice, and pinning the count by literal makes every stacked branch collide here. */
        Assert.Equal(PgMigrations.Scripts.Count, PgMigrations.Scripts.DistinctBy(s => s.Version).Count());
        Assert.Equal(
            PgMigrations.Scripts.Select(s => s.Version).OrderBy(v => v).ToArray(),
            PgMigrations.Scripts.Select(s => s.Version).ToArray());
        Assert.Equal(1, PgMigrations.Scripts[0].Version);
        Assert.Equal(2, PgMigrations.Scripts[1].Version);
        Assert.Equal(3, PgMigrations.Scripts[2].Version);
        Assert.Equal(4, PgMigrations.Scripts[3].Version);
        Assert.Equal(5, PgMigrations.Scripts[4].Version);
        Assert.Equal(6, PgMigrations.Scripts[5].Version);
        Assert.Equal(7, PgMigrations.Scripts[6].Version);
        Assert.Equal(8, PgMigrations.Scripts[7].Version);
        Assert.Equal(9, PgMigrations.Scripts[8].Version);
        Assert.Equal(10, PgMigrations.Scripts[9].Version);
        Assert.Equal(11, PgMigrations.Scripts[10].Version);
        Assert.Equal(12, PgMigrations.Scripts[11].Version);
        Assert.Equal(13, PgMigrations.Scripts[12].Version);
        Assert.Equal(14, PgMigrations.Scripts[13].Version);
        Assert.Equal(15, PgMigrations.Scripts[14].Version);
        Assert.Equal(16, PgMigrations.Scripts[15].Version);
        Assert.Equal(17, PgMigrations.Scripts[16].Version);
        Assert.Equal(18, PgMigrations.Scripts[17].Version);
        Assert.Equal(19, PgMigrations.Scripts[18].Version);
        Assert.Equal(20, PgMigrations.Scripts[19].Version);
        Assert.Equal(21, PgMigrations.Scripts[20].Version);
        Assert.Equal(22, PgMigrations.Scripts[21].Version);
        Assert.Equal(23, PgMigrations.Scripts[22].Version);
        Assert.Equal(24, PgMigrations.Scripts[23].Version);
        Assert.Equal(25, PgMigrations.Scripts[24].Version);
        Assert.Equal(26, PgMigrations.Scripts[25].Version);
        Assert.Equal(27, PgMigrations.Scripts[26].Version);
        Assert.Equal(28, PgMigrations.Scripts[27].Version);
        Assert.Equal(29, PgMigrations.Scripts[28].Version);
        Assert.Equal(30, PgMigrations.Scripts[29].Version);
        Assert.Equal(31, PgMigrations.Scripts[30].Version);
        Assert.Equal(32, PgMigrations.Scripts[31].Version);
        Assert.Equal(33, PgMigrations.Scripts[32].Version);
        /* The newest migration is asserted by identity rather than by ordinal: this ladder is walked by every
           stacked branch at once, and a positional pin turns each addition into a conflict for the next. */
        /* The invariant the test name states, with no literal to go stale: the build's schema version IS
           the newest registered rung. Three in-flight branches bumping versions made the literal form a
           recurring multi-test failure (#2210 round, again here at V62). */
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);

        /* V34 (#991) creates the two Availability Group collector tables. Schema-qualified collect.* and
           CREATE TABLE IF NOT EXISTS, per the file's additive-create idiom (V29): a no-op on a fresh store
           whose V1 schema was generated from the collector catalog, the real create on an upgrade.
           The full column-for-column equality against PgSchemaGenerator.CreateTable is pinned by
           PgSchemaGeneratorTests.Migrations_JobHistoryAndAgentStatus_MatchGeneratedFreshShape — this test
           only pins the migration's IDENTITY (version, name) and the two traits worth stating in prose. */
        var v34Script = PgMigrations.Scripts.Single(s => s.Version == 34);
        var v34 = v34Script.Sql;
        Assert.Equal("availability-group-collectors", v34Script.Name);

        /* The LSNs are numeric(25,0) at the source — wider than bigint — so they land as text. */
        Assert.Contains("last_hardened_lsn text", v34, StringComparison.Ordinal);
        Assert.Contains("last_commit_lsn text", v34, StringComparison.Ordinal);

        /* V36 (#991 addendum) widens the V34 database-grain table additively. Identity only here; the
           column-for-column reconstruction of V34 + V36 against the generator is pinned by
           PgSchemaGeneratorTests.Migrations_JobHistoryAndAgentStatus_MatchGeneratedFreshShape. */
        var v36Script = PgMigrations.Scripts.Single(s => s.Version == 36);
        var v36 = v36Script.Sql;
        Assert.Equal("ag-latency-columns", v36Script.Name);
        Assert.Contains("ALTER TABLE collect.ag_database_replica_states", v36, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS est_send_drain_time_min double precision", v36, StringComparison.Ordinal);

        /* V38 (#1767) adds the hash-keyed payload dimensions. Identity + the three things the migration body
           must contain; the generated shape (column list, dependency order, the resolving view) is pinned
           column-for-column by PayloadDimensionTests. */
        var v38Script = PgMigrations.Scripts.Single(s => s.Version == 38);
        var v38 = v38Script.Sql;
        Assert.Equal("query-payload-dimensions", v38Script.Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS query_text_dim (", v38, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS query_plan_dim (", v38, StringComparison.Ordinal);
        /* ADD COLUMN IF NOT EXISTS, nullable: metadata-only, so the migration never rewrites the existing
           ~234 GB of inline payload — the zero-rewrite contract the whole design rests on. */
        Assert.Contains("ALTER TABLE query_stats ADD COLUMN IF NOT EXISTS query_text_digest bytea;", v38, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS query_plan_digest bytea;", v38, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_query_stats AS", v38, StringComparison.Ordinal);

        /* V26 (#1506) adds the generic webhook channel's four columns to the V17 control-plane table.
           Schema-qualified config.* and IF NOT EXISTS, per the file's additive-ALTER idiom. */
        var v26 = PgMigrations.Scripts[25].Sql;
        Assert.Contains("ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_url", v26, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_headers", v26, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_body_template", v26, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_notification ADD COLUMN IF NOT EXISTS generic_proxy", v26, StringComparison.Ordinal);

        /* V27 (#1535) appends the Azure per-database watermark key to deadlocks and re-expands the
           pinned v_deadlocks SELECT * so the new column is visible through the view. */
        var v27 = PgMigrations.Scripts[26].Sql;
        Assert.Contains("ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS database_name text;", v27, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_deadlocks AS SELECT * FROM deadlocks;", v27, StringComparison.Ordinal);

        /* V28 (#1546) appends the SQL 2022+ replica attribution to query_store_stats and re-expands the
           pinned v_query_store_stats SELECT *. Appended (never inserted) so an upgraded store's physical
           column order still matches a fresh store's catalog-generated one — the positional binary COPY
           depends on it. */
        var v28 = PgMigrations.Scripts[27].Sql;
        Assert.Contains("ALTER TABLE query_store_stats ADD COLUMN IF NOT EXISTS replica_role text;", v28, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_query_store_stats AS SELECT * FROM query_store_stats;", v28, StringComparison.Ordinal);

        /* V29 (#1496) creates the long_query_completions collector table on an upgraded store (a fresh
           store gets it from V1's catalog walk). CREATE TABLE IF NOT EXISTS in the collect schema, plus its
           retrieval index; no v_* view (a post-V14 collector — the viewer reads the base table). */
        var v29 = PgMigrations.Scripts[28].Sql;
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.long_query_completions (", v29, StringComparison.Ordinal);
        Assert.Contains("duration_microseconds bigint", v29, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_long_query_completions_time ON collect.long_query_completions(server_id, collection_time);", v29, StringComparison.Ordinal);

        /* V30 (#1562) adds the web dashboard toggle/port to the V17 config_service control-plane table.
           Schema-qualified config.* and IF NOT EXISTS, per the file's additive-ALTER idiom. */
        var v30 = PgMigrations.Scripts[29].Sql;
        Assert.Contains("ALTER TABLE config.config_service ADD COLUMN IF NOT EXISTS web_enabled boolean NOT NULL DEFAULT FALSE;", v30, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_service ADD COLUMN IF NOT EXISTS web_port integer NOT NULL DEFAULT 5153;", v30, StringComparison.Ordinal);

        /* V31 (#1563) creates the custom-views store. Schema-qualified config.* (a bare CREATE would resolve to
           collect — wrong schema/ACL), a jsonb definition, an IDENTITY id (no sequence USAGE), and — deliberately
           — NO config_bump_version trigger (views feed the web renderer, not the collector, so no reload beacon). */
        var v31 = PgMigrations.Scripts[30].Sql;
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.custom_views (", v31, StringComparison.Ordinal);
        Assert.Contains("id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", v31, StringComparison.Ordinal);
        Assert.Contains("name text NOT NULL UNIQUE", v31, StringComparison.Ordinal);
        Assert.Contains("definition jsonb NOT NULL", v31, StringComparison.Ordinal);
        Assert.DoesNotContain("config_bump_version", v31, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TRIGGER", v31, StringComparison.Ordinal);

        /* V5 completes the v_* twin of Lite's DuckDB view layer -- the copy-parity tail tabs
           (Running Jobs, Configuration, Daily Summary, Collection Health) read these five, so
           their ported SQL stays byte-identical to Lite's. */
        var v5 = PgMigrations.Scripts[4].Sql;
        Assert.Contains("CREATE OR REPLACE VIEW v_running_jobs AS SELECT * FROM running_jobs;", v5, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_server_config AS SELECT * FROM server_config;", v5, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_database_scoped_config AS SELECT * FROM database_scoped_config;", v5, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_trace_flags AS SELECT * FROM trace_flags;", v5, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_collection_log AS SELECT * FROM collection_log;", v5, StringComparison.Ordinal);

        /* V6 adds the two memory passthrough views the Memory tab port (W1j) reads -- the Memory
           Clerks + Pressure Events sub-tabs run FROM v_memory_clerks / v_memory_pressure_events,
           byte-identical to Lite. */
        var v6 = PgMigrations.Scripts[5].Sql;
        Assert.Contains("CREATE OR REPLACE VIEW v_memory_clerks AS SELECT * FROM memory_clerks;", v6, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_memory_pressure_events AS SELECT * FROM memory_pressure_events;", v6, StringComparison.Ordinal);

        /* V7 adds the deferred-plan-capture columns (#1262) additively — one ADD COLUMN IF NOT
           EXISTS per column so a pre-plan store comes up to shape and a fresh V1 store no-ops. */
        var v7 = PgMigrations.Scripts[6].Sql;
        Assert.Equal("viewer-plan-capture-columns", PgMigrations.Scripts[6].Name);
        Assert.Contains("ALTER TABLE procedure_stats ADD COLUMN IF NOT EXISTS query_plan_xml text;", v7, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocked_query_plan_xml text;", v7, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE blocked_process_reports ADD COLUMN IF NOT EXISTS blocking_query_plan_xml text;", v7, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE deadlocks ADD COLUMN IF NOT EXISTS victim_query_plan_xml text;", v7, StringComparison.Ordinal);

        /* V8 is the collect/config security split (#1262): creates the two schemas and moves every
           existing object out of public with ALTER ... SET SCHEMA (generated from the catalog so new
           collectors move automatically). The DDL is generated, so pin the SHAPE, not exact text —
           the ALTER DATABASE search_path default is deliberately NOT here (best-effort in MigrateAsync). */
        var v8 = PgMigrations.Scripts[7].Sql;
        Assert.Equal("schema-split-collect-config", PgMigrations.Scripts[7].Name);
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS collect AUTHORIZATION darling;", v8, StringComparison.Ordinal);
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS config AUTHORIZATION darling;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.wait_stats SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.servers SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.collection_log SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.analysis_findings SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.darling_schema_version SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER VIEW IF EXISTS public.v_wait_stats SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER VIEW IF EXISTS public.v_collection_log SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.config_mute_rules SET SCHEMA config;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.config_alert_log SET SCHEMA config;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.config_edge_trigger_watermarks SET SCHEMA config;", v8, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE IF EXISTS public.analysis_muted SET SCHEMA config;", v8, StringComparison.Ordinal);
        /* The database-default search_path is set by MigrateAsync (best-effort), not baked into V8. */
        Assert.DoesNotContain("ALTER DATABASE", v8, StringComparison.Ordinal);

        /* V9 restores the FinOps copy-parity fields additively: server_properties gains the three
           inventory columns the shared collector now SELECTs (start time / host OS / AG role), and
           servers gains the per-server cost budget — one ADD COLUMN IF NOT EXISTS each, appended to keep
           the positional binary COPY aligned. Bare names resolve through V8's search_path to collect.*. */
        var v9 = PgMigrations.Scripts[8].Sql;
        Assert.Equal("server-inventory-cost-fields", PgMigrations.Scripts[8].Name);
        Assert.Contains("ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS sqlserver_start_time timestamp;", v9, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS host_os_version text;", v9, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS ag_replica_role text;", v9, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE servers ADD COLUMN IF NOT EXISTS monthly_cost_usd numeric;", v9, StringComparison.Ordinal);

        /* V10 adds the latch_stats + spinlock_stats collector tables and their v_* passthrough views
           (Dashboard->Darling parity, #1262). Generated from the collector definitions so the tables
           match the fresh V1 shape; CREATE TABLE IF NOT EXISTS no-ops on a fresh store and really
           creates them on a store built before these collectors existed. */
        var v10 = PgMigrations.Scripts[9].Sql;
        Assert.Equal("latch-spinlock-collectors", PgMigrations.Scripts[9].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS latch_stats (", v10, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS spinlock_stats (", v10, StringComparison.Ordinal);
        Assert.Contains("spins_per_collision double precision", v10, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_latch_stats_time ON latch_stats(server_id, collection_time);", v10, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_spinlock_stats_time ON spinlock_stats(server_id, collection_time);", v10, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_latch_stats AS SELECT * FROM latch_stats;", v10, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_spinlock_stats AS SELECT * FROM spinlock_stats;", v10, StringComparison.Ordinal);

        /* V11 adds the cpu_scheduler_stats + plan_cache_stats collector tables and their v_* passthrough
           views (Dashboard->Darling parity, #1262). Generated from the collector definitions so the
           tables match the fresh V1 shape; CREATE TABLE IF NOT EXISTS no-ops on a fresh store and really
           creates them on a store built before these collectors existed. The two decimal(38,2) averages
           and the boolean pressure warnings prove the type map carried through the generator. */
        var v11 = PgMigrations.Scripts[10].Sql;
        Assert.Equal("cpu-scheduler-plan-cache-collectors", PgMigrations.Scripts[10].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS cpu_scheduler_stats (", v11, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS plan_cache_stats (", v11, StringComparison.Ordinal);
        Assert.Contains("avg_runnable_tasks_count numeric(38,2)", v11, StringComparison.Ordinal);
        Assert.Contains("offline_cpu_warning boolean", v11, StringComparison.Ordinal);
        Assert.Contains("avg_use_count numeric(38,2)", v11, StringComparison.Ordinal);
        Assert.Contains("oldest_plan_create_time timestamp", v11, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_cpu_scheduler_stats_time ON cpu_scheduler_stats(server_id, collection_time);", v11, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_plan_cache_stats_time ON plan_cache_stats(server_id, collection_time);", v11, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_cpu_scheduler_stats AS SELECT * FROM cpu_scheduler_stats;", v11, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_plan_cache_stats AS SELECT * FROM plan_cache_stats;", v11, StringComparison.Ordinal);

        /* V12 adds the session_summary_stats collector table (server-wide session SUMMARY: the
           connection-leak / idle signal) and its v_* passthrough view (Dashboard->Darling parity,
           #1262). Generated from the collector definition so the table matches the fresh V1 shape;
           CREATE TABLE IF NOT EXISTS no-ops on a fresh store and really creates it on a store built
           before this collector existed. Distinct from the per-application session_stats table. */
        var v12 = PgMigrations.Scripts[11].Sql;
        Assert.Equal("session-summary-collector", PgMigrations.Scripts[11].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS session_summary_stats (", v12, StringComparison.Ordinal);
        Assert.Contains("idle_sessions_over_30min integer", v12, StringComparison.Ordinal);
        Assert.Contains("sessions_waiting_for_memory integer", v12, StringComparison.Ordinal);
        Assert.Contains("top_application_name text", v12, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_session_summary_stats_time ON session_summary_stats(server_id, collection_time);", v12, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_session_summary_stats AS SELECT * FROM session_summary_stats;", v12, StringComparison.Ordinal);

        /* V13 adds the system_health_events collector table (Stage 1 raw system_health Extended Events
           capture: one row per event, raw XML only, no shredding) and its v_* passthrough view
           (Dashboard->Darling health-parser parity, #1262). Generated from the collector definition so
           the table matches the fresh V1 shape; CREATE TABLE IF NOT EXISTS no-ops on a fresh store and
           really creates it on a store built before this collector existed. */
        var v13 = PgMigrations.Scripts[12].Sql;
        Assert.Equal("system-health-events-collector", PgMigrations.Scripts[12].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS system_health_events (", v13, StringComparison.Ordinal);
        Assert.Contains("event_time timestamp", v13, StringComparison.Ordinal);
        Assert.Contains("event_type text", v13, StringComparison.Ordinal);
        Assert.Contains("event_xml text", v13, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_system_health_events_time ON system_health_events(server_id, collection_time);", v13, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_system_health_events AS SELECT * FROM system_health_events;", v13, StringComparison.Ordinal);

        /* V14 refreshes EVERY v_* passthrough view's pinned SELECT * column list (#1262): Postgres
           freezes SELECT * at CREATE, so a store upgraded across a column-adding migration keeps a
           stale view. The plan-bearing views (v_blocked_process_reports / v_deadlocks) are the ones
           V7 left stale — the exact staleness PR #1376 worked around by reading base tables — so pin
           their refresh explicitly. (procedure_stats gained query_plan_xml in V7 but has NO v_ view,
           which is why #1376 read the base table for it; there is nothing to refresh here.) */
        var v14 = PgMigrations.Scripts[13].Sql;
        Assert.Equal("refresh-passthrough-views", PgMigrations.Scripts[13].Name);
        Assert.Contains("CREATE OR REPLACE VIEW v_blocked_process_reports AS SELECT * FROM blocked_process_reports;", v14, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_deadlocks AS SELECT * FROM deadlocks;", v14, StringComparison.Ordinal);
        Assert.DoesNotContain("v_procedure_stats", v14, StringComparison.Ordinal);

        /* V14 must refresh the COMPLETE passthrough-view set (V4-V6 + the post-V8 collector views),
           and its list must stay the single source of truth: every CREATE OR REPLACE VIEW emitted by
           ANY migration is covered by AllPassthroughViews and vice-versa, so a future collector view
           can never be added without its refresh. */
        foreach (var view in PgSchemaGenerator.AllPassthroughViews)
        {
            var table = view.Substring("v_".Length);
            Assert.Contains(
                $"CREATE OR REPLACE VIEW {view} AS SELECT * FROM {table};", v14, StringComparison.Ordinal);
        }

        var viewsCreatedByAnyMigration = new System.Collections.Generic.SortedSet<string>(StringComparer.Ordinal);
        foreach (var script in PgMigrations.Scripts)
        {
            foreach (System.Text.RegularExpressions.Match m in System.Text.RegularExpressions.Regex.Matches(
                script.Sql, @"CREATE OR REPLACE VIEW (v_\w+) AS SELECT \* FROM"))
            {
                viewsCreatedByAnyMigration.Add(m.Groups[1].Value);
            }
        }

        /* The passthrough set plus the payload-RESOLVING views (#1767): v_query_stats was created as
           a passthrough by V4/V14 and rebuilt by V38 to COALESCE the dimension tables, so it is a
           view any migration created but deliberately NOT one V14 may refresh. */
        Assert.Equal(
            new System.Collections.Generic.SortedSet<string>(
                PgSchemaGenerator.AllPassthroughViews.Concat(PgSchemaGenerator.PayloadResolvingViews),
                StringComparer.Ordinal),
            viewsCreatedByAnyMigration);

        /* The exclusion is the point: V14's refresh must NOT re-expand a resolving view. */
        Assert.DoesNotContain("v_query_stats", PgSchemaGenerator.AllPassthroughViews);
        Assert.DoesNotContain("CREATE OR REPLACE VIEW v_query_stats AS SELECT * FROM query_stats;", v14, StringComparison.Ordinal);

        /* V15 adds the per-index definition metadata for monitor-side UNUSED/DUPLICATE analysis
           (FinOps Index Analysis, Stage 1) additively — one ADD COLUMN IF NOT EXISTS per column so a
           pre-metadata store comes up to shape and a fresh V1 store no-ops — then CREATE OR REPLACE
           re-expands v_index_object_stats' SELECT * so an upgraded store's view (last refreshed by
           V14, before these columns existed) surfaces them. */
        var v15 = PgMigrations.Scripts[14].Sql;
        Assert.Equal("index-metadata-columns", PgMigrations.Scripts[14].Name);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS key_columns text;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS included_columns text;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS filter_definition text;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_unique_constraint boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_foreign_key_reference boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_disabled boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS data_compression_desc text;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS optimize_for_sequential_key boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS fill_factor smallint;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_padded boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_page_locks boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS allow_row_locks boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE index_object_stats ADD COLUMN IF NOT EXISTS is_indexed_view boolean;", v15, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_index_object_stats AS SELECT * FROM index_object_stats;", v15, StringComparison.Ordinal);

        /* V16 adds server_properties.utc_offset_minutes additively (the monitored server's UTC offset the
           shared collector now writes), so the headless viewer's Server-time display mode can render
           timestamps in the server's own local time — one nullable ADD COLUMN IF NOT EXISTS, appended to
           keep the positional binary COPY aligned. server_properties has no v_* view, so nothing to refresh. */
        var v16 = PgMigrations.Scripts[15].Sql;
        Assert.Equal("server-utc-offset", PgMigrations.Scripts[15].Name);
        Assert.Contains("ALTER TABLE server_properties ADD COLUMN IF NOT EXISTS utc_offset_minutes integer;", v16, StringComparison.Ordinal);

        /* V17 creates the six operator-writable control-plane tables (Stage 1). CRITICAL: every object
           MUST be schema-qualified config.* — the migrate session's search_path (collect, config, public)
           resolves a bare name to collect, so an unqualified CREATE would land in the wrong schema/ACL.
           Pin the config.-qualification on every table + the config_command identity PK + the
           config_version bump triggers so a future edit can never drop the qualification. */
        var v17 = PgMigrations.Scripts[16].Sql;
        Assert.Equal("config-control-plane", PgMigrations.Scripts[16].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_monitored_servers (", v17, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_alert_settings (", v17, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_notification (", v17, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_collector_schedules (", v17, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_service (", v17, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.config_command (", v17, StringComparison.Ordinal);
        /* The command queue's identity PK (no sequence USAGE grant needed) and the analysis knobs + the
           config_version reload beacon + its bump triggers. */
        Assert.Contains("command_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", v17, StringComparison.Ordinal);
        Assert.Contains("config_version bigint NOT NULL DEFAULT 0", v17, StringComparison.Ordinal);
        Assert.Contains("analysis_interval_minutes integer NOT NULL DEFAULT 30", v17, StringComparison.Ordinal);
        Assert.Contains("analysis_notify_severity double precision NOT NULL DEFAULT 1.5", v17, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE FUNCTION config.config_bump_version()", v17, StringComparison.Ordinal);
        Assert.Contains("ON config.config_monitored_servers", v17, StringComparison.Ordinal);
        /* No unqualified CREATE TABLE may sneak in — every control-plane table is config.-qualified. */
        Assert.DoesNotContain("CREATE TABLE IF NOT EXISTS config_", v17, StringComparison.Ordinal);

        /* V18 adds the per-server + per-event alert delivery mode (#1236/#1141): the two global columns on
           config_alert_settings (delivery_mode text default Summary + per_event_max int default 5) and the
           nullable per-server override on config_monitored_servers. CRITICAL: schema-qualified config.* like V17
           (a bare ALTER would resolve to collect). Pin the qualification + the shipped defaults. */
        var v18 = PgMigrations.Scripts[17].Sql;
        Assert.Equal("alert-delivery-mode", PgMigrations.Scripts[17].Name);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS delivery_mode text NOT NULL DEFAULT 'Summary';", v18, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS per_event_max integer NOT NULL DEFAULT 5;", v18, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS alert_delivery_mode_override text;", v18, StringComparison.Ordinal);
        /* Every V18 object is config.-qualified (a bare ALTER TABLE config_* would hit the wrong schema). */
        Assert.DoesNotContain("ALTER TABLE config_", v18, StringComparison.Ordinal);

        /* V19 creates the per-server analysis-state marker (the "still collecting vs all-clear" signal the
           viewer reads). It lives in collect (service-produced observed output read by the viewer, like
           analysis_findings/collection_log) and is EXPLICITLY collect.-qualified — the opposite direction
           from the config control plane. Single row per server (server_id PK) so the writer upserts; the
           columns are exactly the marker's four (insufficient flag, engine message, analysis time). It has
           NO v_* passthrough view (no Lite SQL ports through it), so the AllPassthroughViews cross-check
           above is unaffected. */
        var v19 = PgMigrations.Scripts[18].Sql;
        Assert.Equal("analysis-state-marker", PgMigrations.Scripts[18].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.analysis_state (", v19, StringComparison.Ordinal);
        Assert.Contains("server_id integer NOT NULL PRIMARY KEY", v19, StringComparison.Ordinal);
        Assert.Contains("insufficient_data boolean NOT NULL DEFAULT FALSE", v19, StringComparison.Ordinal);
        Assert.Contains("message text", v19, StringComparison.Ordinal);
        Assert.Contains("analysis_time timestamp NOT NULL", v19, StringComparison.Ordinal);
        /* Observed-output tier -> collect, never the config control plane; and no passthrough view. */
        Assert.DoesNotContain("config.analysis_state", v19, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE OR REPLACE VIEW", v19, StringComparison.Ordinal);

        /* V20 adds the previously-hardcoded alert-tuning knobs to config_alert_settings: the long-running-query
           read shape (max_results + the five noise-filter opt-outs the shared engine forwards to the LRQ read)
           and notify_connection_changes (the Server-Unreachable/Restored connect-edge gate). CRITICAL:
           schema-qualified config.* like V17/V18 (a bare ALTER would resolve to collect). All NOT NULL with the
           shipped defaults (5 / TRUE) so a pre-V20 seeded row comes up honoring the old hardcoded behavior. */
        var v20 = PgMigrations.Scripts[19].Sql;
        Assert.Equal("alert-tuning-knobs", PgMigrations.Scripts[19].Name);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_max_results integer NOT NULL DEFAULT 5;", v20, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_sp_server_diagnostics boolean NOT NULL DEFAULT TRUE;", v20, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_wait_for boolean NOT NULL DEFAULT TRUE;", v20, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_backups boolean NOT NULL DEFAULT TRUE;", v20, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_misc_waits boolean NOT NULL DEFAULT TRUE;", v20, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS long_running_query_exclude_cdc boolean NOT NULL DEFAULT TRUE;", v20, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE config.config_alert_settings ADD COLUMN IF NOT EXISTS notify_connection_changes boolean NOT NULL DEFAULT TRUE;", v20, StringComparison.Ordinal);
        /* Every V20 object is config.-qualified (a bare ALTER TABLE config_* would hit the wrong schema). */
        Assert.DoesNotContain("ALTER TABLE config_", v20, StringComparison.Ordinal);

        /* V21 creates the default_trace_events collector table (built-in Default Trace via fn_trace_gettable).
           A NEW collector table for stores built before it existed; a fresh store already has it (V1 walks the
           catalog and V8 moved it to collect), so CREATE TABLE IF NOT EXISTS no-ops on fresh / creates on
           upgrade. EXPLICITLY collect.-qualified (like V19's analysis_state, the observed-output tier). It has
           NO v_* passthrough view (the MCP tool reads the base table, like server_properties), so the
           AllPassthroughViews cross-check above is unaffected. */
        var v21 = PgMigrations.Scripts[20].Sql;
        Assert.Equal("default-trace-events-collector", PgMigrations.Scripts[20].Name);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collect.default_trace_events (", v21, StringComparison.Ordinal);
        Assert.Contains("default_trace_event_id bigint NOT NULL", v21, StringComparison.Ordinal);
        Assert.Contains("event_time timestamp", v21, StringComparison.Ordinal);
        Assert.Contains("event_name text", v21, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_default_trace_events_time ON collect.default_trace_events(server_id, collection_time);", v21, StringComparison.Ordinal);
        /* Observed-output tier -> collect, never the config control plane; and no passthrough view. */
        Assert.DoesNotContain("config.default_trace_events", v21, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE OR REPLACE VIEW", v21, StringComparison.Ordinal);

        /* V22 adds a supporting index whose key matches the FinOps Index Analysis read's
           DISTINCT ON (database_id, object_id, index_id) ... ORDER BY ..., collection_time DESC. The V1
           idx_index_object_stats_object leads database_NAME, which cannot serve the database_ID DISTINCT ON,
           forcing a whole-server sort; this second, additively-created index serves it in index order.
           collect.-qualified like V21; CREATE INDEX IF NOT EXISTS so a V21 store upgrades idempotently and a
           fresh store (which already built the differently-keyed V1 index) gets this one too. index_object_stats
           is a hypertable, so Timescale applies the CREATE INDEX across its chunks. */
        var v22 = PgMigrations.Scripts[21].Sql;
        Assert.Equal("index-object-stats-latest-index", PgMigrations.Scripts[21].Name);
        Assert.Contains(
            "CREATE INDEX IF NOT EXISTS idx_index_object_stats_latest ON collect.index_object_stats (server_id, database_id, object_id, index_id, collection_time DESC);",
            v22, StringComparison.Ordinal);
        /* Idempotent (IF NOT EXISTS) so a V21 store upgrades cleanly and a re-run no-ops. */
        Assert.Contains("CREATE INDEX IF NOT EXISTS", v22, StringComparison.Ordinal);
        /* Keys database_ID + collection_time DESC — the read's exact order, NOT the V1 index's database_NAME. */
        Assert.Contains("database_id, object_id, index_id, collection_time DESC", v22, StringComparison.Ordinal);
        Assert.DoesNotContain("database_name", v22, StringComparison.Ordinal);
        /* Collector table -> collect schema; a plain index (no config plane, no passthrough view to refresh). */
        Assert.DoesNotContain("config.index_object_stats", v22, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE OR REPLACE VIEW", v22, StringComparison.Ordinal);

        /* V23 makes collect.collection_log (the highest-volume plain table) a TimescaleDB hypertable and
           compresses it — so retention becomes O(1) drop_chunks and the freshness read hits only the newest
           chunk per server. The migration is a best-effort UPGRADE fast-path only: it is GUARDED on pg_extension
           (a plain-PostgreSQL store skips it, keeping collection_log a heap) AND wrapped in EXCEPTION WHEN OTHERS
           so it can NEVER abort the startup-critical migration. The AUTHORITATIVE conversion is
           TimescaleSupport.EnsureCollectionLogHypertableAsync at runtime (after CREATE EXTENSION) — because
           MigrateAsync runs BEFORE the extension is created, so on a fresh store this guard is false and the
           runtime path heals it (pinned in TimescaleSupportTests). collection_log is NOT in the collector
           catalog, so the runtime catalog loops never touch it either. */
        var v23 = PgMigrations.Scripts[22].Sql;
        Assert.Equal("collection-log-hypertable", PgMigrations.Scripts[22].Name);
        /* Guarded on the extension so plain PostgreSQL no-ops (create_hypertable does not exist there). */
        Assert.Contains("IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')", v23, StringComparison.Ordinal);
        /* Non-fatal: any failure (e.g. an unexpected migrate_data-in-transaction issue) warns and the migration
           commits — the runtime path is the guarantee, so a thrown conversion must not brick startup. */
        Assert.Contains("EXCEPTION WHEN OTHERS THEN", v23, StringComparison.Ordinal);
        /* create_hypertable with migrate_data (moves existing rows) + if_not_exists (idempotent / already-converted no-op). */
        Assert.Contains("create_hypertable('collect.collection_log', by_range('collection_time', INTERVAL '1 days')", v23, StringComparison.Ordinal);
        Assert.Contains("migrate_data => true", v23, StringComparison.Ordinal);
        Assert.Contains("if_not_exists => true", v23, StringComparison.Ordinal);
        /* Compression mirrors TimescaleSupport for this one table: segment by server_id, 1-day compress-after. */
        Assert.Contains("ALTER TABLE collect.collection_log SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", v23, StringComparison.Ordinal);
        Assert.Contains("add_compression_policy('collect.collection_log', compress_after => INTERVAL '1 days', if_not_exists => true)", v23, StringComparison.Ordinal);

        var v2 = PgMigrations.Scripts[1].Sql;
        Assert.Contains("CREATE TABLE IF NOT EXISTS servers (", v2, StringComparison.Ordinal);
        Assert.Contains("CREATE TABLE IF NOT EXISTS collection_log (", v2, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_collection_log_time ON collection_log(server_id, collection_time)",
            v2, StringComparison.Ordinal);
    }

    /// <summary>
    /// Win 1: the reload sync mirrors BOTH the enable flag and the FinOps cost from the desired config onto the
    /// observed registry, and fires when EITHER drifts — so a cost-only edit reaches collect.servers (which
    /// FinOps reads) without a disconnect+reconnect. Pure SQL-shape pin (no store).
    /// </summary>
    [Fact]
    public void SyncEnabledStatesSql_MirrorsEnabledAndCost_FiringOnEitherDelta()
    {
        var sql = DarlingObservability.SyncEnabledStatesSql;

        /* Both the enable flag and the FinOps cost are mirrored desired -> observed. */
        Assert.Contains("is_enabled = c.is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("monthly_cost_usd = c.monthly_cost_usd", sql, StringComparison.Ordinal);

        /* The WHERE fires when EITHER field drifts (so a cost-only edit is carried too), each via a NULL-safe
           IS DISTINCT FROM (a pre-cost observed row still takes the config value). */
        Assert.Contains("s.is_enabled IS DISTINCT FROM c.is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("s.monthly_cost_usd IS DISTINCT FROM c.monthly_cost_usd", sql, StringComparison.Ordinal);
        Assert.Contains(" OR ", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The DELETED-server half of the mirror (#2030). The enable/cost sync is an inner join on the config
    /// row, so a server REMOVED from the desired config kept its last observed is_enabled forever and stayed
    /// on the web dashboard as a ghost. This pin holds the orphan sweep to its three load-bearing choices: it
    /// DISABLES (never deletes — the row anchors un-aged collected history and a re-add under the same
    /// storage name resumes the identity), it matches on NOT EXISTS against the desired config, and it only
    /// touches rows still flagged enabled (idempotent re-runs are free). Pure SQL-shape pin (no store).
    /// </summary>
    [Fact]
    public void DisableOrphanedServersSql_DisablesNotDeletes_OnNotExistsAgainstDesiredConfig()
    {
        var sql = DarlingObservability.DisableOrphanedServersSql;

        Assert.Contains("UPDATE collect.servers", sql, StringComparison.Ordinal);
        Assert.Contains("SET is_enabled = FALSE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("DELETE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE s.is_enabled", sql, StringComparison.Ordinal);
        Assert.Contains("NOT EXISTS", sql, StringComparison.Ordinal);
        Assert.Contains("FROM config.config_monitored_servers c WHERE c.server_id = s.server_id", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The analysis-state marker write (V19) is an UPSERT on the natural server_id key over the four marker
    /// columns, so a re-run for a server overwrites its prior determination rather than accumulating rows.
    /// Bare <c>analysis_state</c> resolves through the collect/config search path to collect.analysis_state
    /// (the observed-output schema). Pure SQL-shape pin (no store).
    /// </summary>
    [Fact]
    public void WriteAnalysisStateSql_UpsertsTheFourMarkerColumnsOnServerId()
    {
        var sql = DarlingObservability.WriteAnalysisStateSql;

        Assert.Contains("INSERT INTO analysis_state (server_id, insufficient_data, message, analysis_time)", sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id) DO UPDATE SET", sql, StringComparison.Ordinal);
        Assert.Contains("insufficient_data = EXCLUDED.insufficient_data", sql, StringComparison.Ordinal);
        Assert.Contains("message = EXCLUDED.message", sql, StringComparison.Ordinal);
        Assert.Contains("analysis_time = EXCLUDED.analysis_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EndToEnd_UpsertServerAndLogCollection_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live observability test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        /* Migrations are idempotent — an older store comes up to current, a current store no-ops. */
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);

        using (var versions = new NpgsqlCommand("SELECT COUNT(*) FROM darling_schema_version", connection))
        {
            /* A fully-migrated store has one stamped row per script — assert against the live count so
               this never goes stale as migrations are appended (it was pinned at a literal that drifted). */
            Assert.Equal((long)PgMigrations.Scripts.Count, await versions.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        }

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection);

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "obs-e2e", Host = "obs-e2e-host" },
            ConnectionString = "Server=obs-e2e-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "obs-e2e-host",
            ServerId = TestServerId,
            EngineEdition = 3,
        };

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        await DarlingObservability.UpsertServerAsync(postgres, server, null, TestContext.Current.CancellationToken);

        DateTime firstModified;
        using (var read = new NpgsqlCommand(
            "SELECT server_name, display_name, is_enabled, sql_engine_edition, sql_major_version, created_date, modified_date, engine_kind FROM servers WHERE server_id = $1", connection))
        {
            read.Parameters.AddWithValue(TestServerId);
            using var reader = await read.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "servers row missing after upsert");
            Assert.Equal("obs-e2e-host", reader.GetString(0));
            Assert.Equal("obs-e2e", reader.GetString(1));
            Assert.True(reader.GetBoolean(2));
            Assert.Equal(3, reader.GetInt32(3)); /* the real probed engine edition (3 = Enterprise), not a derived 5/8/0 */
            Assert.Equal(16, reader.GetInt32(4));
            Assert.False(reader.IsDBNull(5));
            firstModified = reader.GetDateTime(6);
            /* V82 (#2530): the engine KIND the connector probed, derived from the target rather than from
               the configured engine string. A SQL Server target stamps the SQL Server token. */
            Assert.Equal(MonitoredEngineKind.SqlServer, reader.GetString(7));
        }

        /* The ON CONFLICT arm CORRECTS the engine kind, unlike is_enabled which it deliberately leaves
           alone (#2530). Same identity, re-pointed at an Aurora target — which is what a registration
           edited to a PostgreSQL host looks like on its next connect. A reconnect arm that skipped this
           column would leave the row asserting SQL Server forever, and every engine-aware surface would
           read it. */
        var repointed = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "obs-e2e", Host = "obs-e2e-host" },
            ConnectionString = "Host=obs-e2e-host",
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true, PostgresMajorVersion = 17 },
            StorageName = "obs-e2e-host",
            ServerId = TestServerId,
            EngineEdition = 0,
        };

        /* try/finally around the repoint, so a failed assertion inside it cannot leave this shared-store
           row asserting Aurora for whatever runs next. The restore is the FINALLY, not a trailing
           statement: a trailing one is skipped by exactly the failure that makes it matter. */
        try
        {
            await DarlingObservability.UpsertServerAsync(postgres, repointed, null, TestContext.Current.CancellationToken);

            using var read = new NpgsqlCommand("SELECT engine_kind, sql_engine_edition FROM servers WHERE server_id = $1", connection);
            read.Parameters.AddWithValue(TestServerId);
            using var reader = await read.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "servers row missing after re-upsert");
            Assert.Equal(MonitoredEngineKind.AuroraPostgres, reader.GetString(0));
            /* And the edition it carries is 0 — the value that used to be indistinguishable from "never
               connected", which is the whole reason the kind column exists. */
            Assert.Equal(0, reader.GetInt32(1));
        }
        finally
        {
            /* Put it back, so the rest of this test — and the next run of it — reads the SQL Server row it
               was written against. Its own connection is not needed here the way LiveStoreCleanup gives
               one: this restore goes through the pooled NpgsqlDataSource the upserts above use, not
               through the possibly-broken body connection. */
            await DarlingObservability.UpsertServerAsync(postgres, server, null, TestContext.Current.CancellationToken);
        }

        /* The second upsert must not throw and must refresh modified_date. */
        await Task.Delay(50, TestContext.Current.CancellationToken);
        await DarlingObservability.UpsertServerAsync(postgres, server, null, TestContext.Current.CancellationToken);

        using (var read = new NpgsqlCommand("SELECT modified_date FROM servers WHERE server_id = $1", connection))
        {
            read.Parameters.AddWithValue(TestServerId);
            var secondModified = Assert.IsType<DateTime>(await read.ExecuteScalarAsync(TestContext.Current.CancellationToken));
            Assert.True(secondModified > firstModified, "second upsert did not refresh modified_date");
        }

        /* A collector that does NOT fan out: the three V80 columns must come back NULL rather than zero,
           which is what tells "this run had no fan-out" apart from "its fan-out was free" (#2472). */
        await DarlingObservability.LogCollectionAsync(
            postgres, server, "wait_stats", "SUCCESS", 42, 100, 25, null, fanout: null, phases: null, drain: null, sweepPeerMaxMs: null, null, TestContext.Current.CancellationToken);

        using (var read = new NpgsqlCommand(
            "SELECT collector_name, status, rows_collected, duration_ms, sql_duration_ms, duckdb_duration_ms, error_message, fanout_item_count, slowest_item, slowest_item_ms FROM v_collection_log WHERE server_id = $1", connection))
        {
            read.Parameters.AddWithValue(TestServerId);
            using var reader = await read.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "collection_log row missing");
            Assert.Equal("wait_stats", reader.GetString(0));
            Assert.Equal("SUCCESS", reader.GetString(1));
            Assert.Equal(42, reader.GetInt32(2));
            Assert.Equal(125, reader.GetInt32(3)); /* sql + storage */
            Assert.Equal(100, reader.GetInt32(4));
            Assert.Equal(25, reader.GetInt32(5)); /* the storage (Postgres) phase, under Lite's column name */
            Assert.True(reader.IsDBNull(6));
            Assert.True(reader.IsDBNull(7));
            Assert.True(reader.IsDBNull(8));
            Assert.True(reader.IsDBNull(9));
            Assert.False(await reader.ReadAsync(TestContext.Current.CancellationToken), "expected exactly one collection_log row");
        }

        await DeleteTestRowsAsync(connection);

        /* And a collector that DID fan out, round-tripped through the same writer. Read back through
           v_collection_log rather than the table: Postgres freezes a view's SELECT * at CREATE, so a rung
           that adds columns without refreshing the view leaves the write perfectly correct and every READ
           blind to it — a failure that only a read through the view can see (#2472, and V14 before it). */
        await DarlingObservability.LogCollectionAsync(
            postgres, server, "query_store", "SUCCESS", 900, 70_000, 10_800, null,
            new FanoutCost(8, "the-busy-one", 61_900), phases: null, drain: null, sweepPeerMaxMs: null, null, TestContext.Current.CancellationToken);

        using (var read = new NpgsqlCommand(
            "SELECT duration_ms, fanout_item_count, slowest_item, slowest_item_ms FROM v_collection_log WHERE server_id = $1", connection))
        {
            read.Parameters.AddWithValue(TestServerId);
            using var reader = await read.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "collection_log row missing");

            var durationMs = reader.GetInt32(0);
            var items = reader.GetInt32(1);
            var slowestMs = reader.GetInt32(3);

            Assert.Equal(80_800, durationMs);
            Assert.Equal(8, items);
            Assert.Equal("the-busy-one", reader.GetString(2));
            Assert.Equal(61_900, slowestMs);

            /* The number the whole rung exists for, computed off the stored row rather than off the value
               that was written: 6.13 against the 1.0 an even eight-database fan-out of the same 80,800 ms
               would give. duration_ms alone cannot tell those apart, and neither can any aggregate of it. */
            Assert.Equal(6.13, (double)slowestMs * items / durationMs, 2);
        }

        await DeleteTestRowsAsync(connection);
    }

    /// <summary>
    /// The fan-out rollup, read back through the SHIPPED query against a live store (#2472).
    ///
    /// <para>This exists because the write-side test above cannot see the failure this one catches. The
    /// health read's inner subquery ENUMERATES its columns rather than <c>SELECT *</c>-ing them, so an
    /// outer aggregate naming a column the subquery does not project is perfectly valid C#, passes every
    /// text assertion, compiles on every platform, and fails only when Postgres parses it. I wrote exactly
    /// that bug building this rung, and nothing in the suite would have found it: no live test executed
    /// <c>DarlingDataReader.CollectionHealthSql</c> at all.</para>
    ///
    /// <para>So the guard is to RUN the shipped text, not to assert about it — and to run it over a fan-out
    /// whose answer is known, because a query that parses and returns nulls looks identical to one that
    /// works on a store with no fan-out rows in it.</para>
    /// </summary>
    [Fact]
    public async Task CollectionHealth_ReportsTheFanoutRollup_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fan-out rollup test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteTestRowsAsync(connection);

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "fanout-e2e", Host = "fanout-e2e-host" },
            ConnectionString = "Server=fanout-e2e-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "fanout-e2e-host",
            ServerId = TestServerId,
            EngineEdition = 3,
        };

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        await DarlingObservability.UpsertServerAsync(postgres, server, null, TestContext.Current.CancellationToken);

        /* Two runs of one collector, both 80,800 ms, one even across eight databases and one dominated by
           a single database. Every run-level aggregate the read already computes — AVG, MAX,
           PERCENTILE_DISC — sees the same number for both, which is the whole reason the rollup exists. */
        await DarlingObservability.LogCollectionAsync(
            postgres, server, "query_store", "SUCCESS", 900, 70_000, 10_800, null,
            new FanoutCost(8, "even-worst", 10_100), phases: null, drain: null, sweepPeerMaxMs: null, null, TestContext.Current.CancellationToken);

        await DarlingObservability.LogCollectionAsync(
            postgres, server, "query_store", "SUCCESS", 900, 70_000, 10_800, null,
            new FanoutCost(8, "the-busy-one", 61_900), phases: null, drain: null, sweepPeerMaxMs: null, null, TestContext.Current.CancellationToken);

        /* And a collector that does not fan out at all, so the null path is exercised by the same read. */
        await DarlingObservability.LogCollectionAsync(
            postgres, server, "wait_stats", "SUCCESS", 42, 100, 25, null,
            fanout: null, phases: null, drain: null, sweepPeerMaxMs: null, null, TestContext.Current.CancellationToken);

        var rows = await DarlingDataReader.GetCollectionHealthAsync(
            postgres, TestServerId, DateTime.UtcNow.AddDays(-1), TestContext.Current.CancellationToken);

        var queryStore = Assert.Single(rows, r => r.CollectorName == "query_store");
        var waitStats = Assert.Single(rows, r => r.CollectorName == "wait_stats");

        /* The run-level statistics agree across the two shapes, exactly as predicted. */
        Assert.Equal(80_800, queryStore.MaxDurationMs);
        Assert.Equal(80_800, queryStore.AvgDurationMs);
        Assert.Equal(80_800, queryStore.P95DurationMs);

        /* The rollup names the dominated run's database, not the even one's — the rank is on the ITEM
           cost, and both runs cost the same in total, so a rank on duration_ms would have picked
           arbitrarily between them. */
        Assert.Equal("the-busy-one", queryStore.SlowestItem);
        Assert.Equal(8, queryStore.FanoutItems);
        Assert.Equal(61_900, queryStore.SlowestItemMs);
        Assert.Equal(80_800, queryStore.SlowestRunDurationMs);
        Assert.Equal(6.13, queryStore.FanoutDominance!.Value, 2);

        /* A collector with no fan-out reports nothing rather than zero. */
        Assert.Null(waitStats.FanoutItems);
        Assert.Null(waitStats.SlowestItem);
        Assert.Null(waitStats.FanoutDominance);

        await DeleteTestRowsAsync(connection);
    }

    [Fact]
    public async Task SyncServerEnabledStates_MirrorsDesiredOntoObservedRegistry_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the enable-state sync test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteTestRowsAsync(connection);

        /* Desired: DISABLED. Observed (a stale row from a prior connect): still ENABLED. */
        await ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, 'sync-test', 'sync-host', FALSE)");
        await ExecAsync(connection,
            "INSERT INTO collect.servers (server_id, server_name, is_enabled, created_date, modified_date) VALUES ($1, 'sync-host', TRUE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')");

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        await DarlingObservability.SyncServerEnabledStatesAsync(postgres, null, TestContext.Current.CancellationToken);
        Assert.False(await ReadObservedEnabledAsync(connection));

        /* Re-enable desired -> the sync flips the observed row back to TRUE. */
        await ExecAsync(connection, "UPDATE config.config_monitored_servers SET is_enabled = TRUE WHERE server_id = $1");
        await DarlingObservability.SyncServerEnabledStatesAsync(postgres, null, TestContext.Current.CancellationToken);
        Assert.True(await ReadObservedEnabledAsync(connection));

        await DeleteTestRowsAsync(connection);
    }

    [Fact]
    public async Task SyncServerEnabledStates_MirrorsCostOnlyDelta_OntoObservedRegistry_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the cost-sync test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteTestRowsAsync(connection);

        /* Desired cost 500, SAME enable state (TRUE) as observed — a COST-ONLY delta (the case Win 1 fixes:
           before, the sync's WHERE only fired on an is_enabled delta, so a cost-only edit never reached the
           observed row unless the server reconnected). Observed starts at 0. */
        await ExecAsync(connection,
            "INSERT INTO config.config_monitored_servers (server_id, name, host, is_enabled, monthly_cost_usd) VALUES ($1, 'cost-test', 'cost-host', TRUE, 500)");
        await ExecAsync(connection,
            "INSERT INTO collect.servers (server_id, server_name, is_enabled, monthly_cost_usd, created_date, modified_date) VALUES ($1, 'cost-host', TRUE, 0, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')");

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        await DarlingObservability.SyncServerEnabledStatesAsync(postgres, null, TestContext.Current.CancellationToken);
        Assert.Equal(500m, await ReadObservedCostAsync(connection));

        /* A later cost-only edit is carried too. */
        await ExecAsync(connection, "UPDATE config.config_monitored_servers SET monthly_cost_usd = 750 WHERE server_id = $1");
        await DarlingObservability.SyncServerEnabledStatesAsync(postgres, null, TestContext.Current.CancellationToken);
        Assert.Equal(750m, await ReadObservedCostAsync(connection));

        await DeleteTestRowsAsync(connection);
    }

    [Fact]
    public async Task UpsertServer_ReconnectDoesNotReEnableADisabledObservedRow_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the upsert no-clobber test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteTestRowsAsync(connection);

        /* An observed row that has been DISABLED via the control plane. */
        await ExecAsync(connection,
            "INSERT INTO collect.servers (server_id, server_name, is_enabled, created_date, modified_date) VALUES ($1, 'noclobber-host', FALSE, now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC')");

        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = "noclobber", Host = "noclobber-host" },
            ConnectionString = "Server=noclobber-host",
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = "noclobber-host",
            ServerId = TestServerId,
            EngineEdition = 3,
        };

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        /* A re-connect upsert (ON CONFLICT) must NOT resurrect is_enabled to TRUE (Stage 2 fix). */
        await DarlingObservability.UpsertServerAsync(postgres, server, null, TestContext.Current.CancellationToken);
        Assert.False(await ReadObservedEnabledAsync(connection));

        await DeleteTestRowsAsync(connection);
    }

    [Fact]
    public async Task WriteAnalysisState_InsufficientThenSufficient_UpsertsTheMarker_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the analysis-state marker test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteTestRowsAsync(connection);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        /* Insufficient-data pass: the marker upserts insufficient_data = true with the engine's message. */
        await DarlingObservability.WriteAnalysisStateAsync(
            postgres, TestServerId, insufficientData: true, "Not enough data for reliable analysis. Need 1.0 days.",
            null, TestContext.Current.CancellationToken);

        var (insufficient1, message1) = await ReadAnalysisStateAsync(connection);
        Assert.True(insufficient1);
        Assert.Equal("Not enough data for reliable analysis. Need 1.0 days.", message1);

        /* A real pass on enough data: the SAME row (server_id PK) flips to false and clears the message —
           the upsert overwrites rather than inserting a second row. */
        await DarlingObservability.WriteAnalysisStateAsync(
            postgres, TestServerId, insufficientData: false, null, null, TestContext.Current.CancellationToken);

        var (insufficient2, message2) = await ReadAnalysisStateAsync(connection);
        Assert.False(insufficient2);
        Assert.Null(message2);

        using (var count = new NpgsqlCommand("SELECT COUNT(*) FROM analysis_state WHERE server_id = $1", connection))
        {
            count.Parameters.AddWithValue(TestServerId);
            Assert.Equal(1L, await count.ExecuteScalarAsync(TestContext.Current.CancellationToken));
        }

        await DeleteTestRowsAsync(connection);
    }

    private static async Task<(bool Insufficient, string? Message)> ReadAnalysisStateAsync(NpgsqlConnection connection)
    {
        using var read = new NpgsqlCommand("SELECT insufficient_data, message FROM analysis_state WHERE server_id = $1", connection);
        read.Parameters.AddWithValue(TestServerId);
        using var reader = await read.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), "analysis_state row missing after write");
        return (reader.GetBoolean(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    private static async Task<bool> ReadObservedEnabledAsync(NpgsqlConnection connection)
    {
        using var read = new NpgsqlCommand("SELECT is_enabled FROM collect.servers WHERE server_id = $1", connection);
        read.Parameters.AddWithValue(TestServerId);
        return (bool)(await read.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task<decimal> ReadObservedCostAsync(NpgsqlConnection connection)
    {
        using var read = new NpgsqlCommand("SELECT monthly_cost_usd FROM collect.servers WHERE server_id = $1", connection);
        read.Parameters.AddWithValue(TestServerId);
        return (decimal)(await read.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {TestServerId}; " +
            $"DELETE FROM collect.analysis_state WHERE server_id = {TestServerId}; " +
            $"DELETE FROM collect.servers WHERE server_id = {TestServerId}; " +
            $"DELETE FROM config.config_monitored_servers WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
