/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling's generated Postgres schema against the collector definitions: the catalog covers
/// every collector, the type map mirrors Lite's DuckDB types (per-column numeric(p,s) included),
/// the prefix names vary exactly where Lite's schema varies (deadlock_id / blocked_report_id /
/// system_health_event_id / config_id+capture_time / running_jobs' no-id), and the index shapes
/// mirror Lite's columns.
/// </summary>
public sealed class PgSchemaGeneratorTests
{
    [Fact]
    public void Catalog_CoversAllCollectors_WithUniqueTablesAndNames()
    {
        /* 35 through agent_status + long_query_completions (#1496 long-query trace) = 36, plus the two
           Availability Group collectors (#991) = 38, plus plan_correction (#1952 automatic plan
           correction) = 39, plus pvs_stats (#1951 ADR version store) = 40, plus database_states
           (baseline-deviation database-state alert) = 41, plus pg_wait_stats (the first PostgreSQL
           collector) = 42, plus pg_statement_stats = 43, plus pg_wraparound_stats = 44, plus pg_xmin_horizon = 45,
           plus pg_replication_slot_stats = 46, plus pg_autovacuum_stats (the first per-database PostgreSQL
           collector) = 47, plus pg_io_stats = 48, plus pg_blocking = 49, plus query_store_health
           (#2319) = 50, plus pg_database_stats (#2539, the pg_stat_database counters) = 51, plus
           pg_index_usage_stats (#2541, per-index usage) = 52, plus pg_table_bloat_stats (#2542, the
           bloat estimate) = 53, plus pg_session_states (#2540, the sessions behind a pinned xmin
           horizon) = 54, plus pg_plan_capture_readiness (#2564, whether plans can be
           captured at all) = 55, plus pg_write_stats (#2544, the write side - checkpointer, background
           writer and WAL as one row) = 56, plus pg_extension_availability (#2545, the extension
           capability axis) = 57, plus pg_lock_stats (#2544, lock state by mode and relation -
           which pg_blocking_edges cannot give) = 58, plus pg_column_stats (#2543,
           the planner statistics that explain WHY a plan was chosen) = 59, plus
           pg_replication_stats (#2544, connected standbys as distinct from slots) = 60, plus
           pg_buffer_usage (#2544, what is resident in shared buffers) = 61, plus
           pg_index_bloat (#2561) and pg_wait_sampling, pg_kernel_stats and pg_predicate_stats (#2603) = 65. The catalog is
           deliberately
           engine-mixed: the schema generator walks it to
           create tables and one store can hold both engines' data, so splitting it per engine would
           fragment DDL generation. Dispatch is gated separately, by engine, in
           CollectorCatalog.AppliesTo(definition, target). */
        Assert.Equal(65, CollectorCatalog.All.Count);

        /* Uniqueness is asserted AGAINST THE COUNT rather than against a second literal. The literals here
           had drifted to 45 while the real figure tracked the count, so the test that exists to catch a
           duplicate table or name was itself failing for an unrelated reason — and could not say so, because
           this project does not execute on the machine the collectors were written on. Two collectors sharing
           a TargetTable would still fail this, which is the point. */
        Assert.Equal(CollectorCatalog.All.Count, CollectorCatalog.All.Select(s => s.TargetTable).Distinct().Count());
        Assert.Equal(CollectorCatalog.All.Count, CollectorCatalog.All.Select(s => s.Name).Distinct().Count());
    }

    [Fact]
    public void Catalog_PrefixNames_MirrorLiteSchemaExceptions()
    {
        var byTable = CollectorCatalog.All.ToDictionary(s => s.TargetTable);

        Assert.Equal("deadlock_id", byTable["deadlocks"].PrefixIdColumnName);
        Assert.Equal("blocked_report_id", byTable["blocked_process_reports"].PrefixIdColumnName);
        Assert.Equal("system_health_event_id", byTable["system_health_events"].PrefixIdColumnName);
        foreach (var config in new[] { "server_config", "database_config", "database_scoped_config", "trace_flags" })
        {
            Assert.Equal("config_id", byTable[config].PrefixIdColumnName);
            Assert.Equal("capture_time", byTable[config].PrefixTimeColumnName);
        }
        Assert.False(byTable["running_jobs"].IncludesCollectionId);
        Assert.Equal("collection_id", byTable["wait_stats"].PrefixIdColumnName);
        Assert.Equal("collection_time", byTable["wait_stats"].PrefixTimeColumnName);
    }

    [Fact]
    public void Catalog_EveryDecimalColumn_DeclaresPrecision()
    {
        var undeclared = CollectorCatalog.All
            .SelectMany(s => s.PayloadColumns.Select(c => (s.TargetTable, Column: c)))
            .Where(x => x.Column.Type == CollectorColumnType.Decimal && x.Column.Precision <= 0)
            .Select(x => $"{x.TargetTable}.{x.Column.Name}")
            .ToArray();

        Assert.Empty(undeclared);
    }

    [Fact]
    public void TypeFor_MapsEveryColumnType()
    {
        Assert.Equal("bigint", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.BigInt)));
        Assert.Equal("integer", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Integer)));
        Assert.Equal("smallint", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.SmallInt)));
        Assert.Equal("text", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Varchar)));
        Assert.Equal("timestamp", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Timestamp)));
        Assert.Equal("double precision", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Double)));
        Assert.Equal("numeric(18,2)", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal, 18, 2)));
        Assert.Equal("numeric(5,2)", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal, 5, 2)));
        Assert.Equal("boolean", PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Boolean)));
        Assert.Throws<InvalidOperationException>(
            () => PgSchemaGenerator.TypeFor(new CollectorColumn("c", CollectorColumnType.Decimal)));
    }

    [Fact]
    public void CreateTable_Deadlocks_FullDdlPinned()
    {
        var ddl = PgSchemaGenerator.CreateTable(DeadlocksCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS deadlocks (\n" +
            "    deadlock_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    deadlock_time timestamp,\n" +
            "    victim_process_id text,\n" +
            "    victim_sql_text text,\n" +
            "    deadlock_graph_xml text,\n" +
            "    victim_query_plan_xml text,\n" +
            "    database_name text\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_RunningJobs_HasNoIdColumn()
    {
        var ddl = PgSchemaGenerator.CreateTable(RunningJobsCollector.Instance);

        Assert.StartsWith("CREATE TABLE IF NOT EXISTS running_jobs (\n    collection_time timestamp NOT NULL,", ddl, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_id", ddl, StringComparison.Ordinal);
        Assert.Contains("percent_of_average numeric(10,1)", ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateTable_ServerConfig_UsesConfigIdAndCaptureTime()
    {
        var ddl = PgSchemaGenerator.CreateTable(ServerConfigCollector.Instance);

        Assert.StartsWith(
            "CREATE TABLE IF NOT EXISTS server_config (\n" +
            "    config_id bigint NOT NULL,\n" +
            "    capture_time timestamp NOT NULL,",
            ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateTable_QuerySnapshots_MirrorsPerColumnNumericPrecision()
    {
        var ddl = PgSchemaGenerator.CreateTable(QuerySnapshotsCollector.Instance);

        Assert.Contains("granted_query_memory_gb numeric(18,2)", ddl, StringComparison.Ordinal);
        Assert.Contains("percent_complete numeric(5,2)", ddl, StringComparison.Ordinal);
        Assert.Contains("is_cdc_capture boolean", ddl, StringComparison.Ordinal);
        Assert.Contains("requested_memory_mb double precision", ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void CreateTable_LatchStats_MirrorsDeltaColumns()
    {
        var ddl = PgSchemaGenerator.CreateTable(LatchStatsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS latch_stats (\n" +
            "    collection_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    latch_class text,\n" +
            "    waiting_requests_count bigint,\n" +
            "    wait_time_ms bigint,\n" +
            "    max_wait_time_ms bigint,\n" +
            "    delta_waiting_requests_count bigint,\n" +
            "    delta_wait_time_ms bigint,\n" +
            "    delta_max_wait_time_ms bigint\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_SpinlockStats_MapsSpinsPerCollisionToDoublePrecision()
    {
        var ddl = PgSchemaGenerator.CreateTable(SpinlockStatsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS spinlock_stats (\n" +
            "    collection_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    spinlock_name text,\n" +
            "    collisions bigint,\n" +
            "    spins bigint,\n" +
            "    spins_per_collision double precision,\n" +
            "    sleep_time bigint,\n" +
            "    backoffs bigint,\n" +
            "    delta_collisions bigint,\n" +
            "    delta_spins bigint,\n" +
            "    delta_sleep_time bigint,\n" +
            "    delta_backoffs bigint\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_CpuSchedulerStats_MapsWarningsToBoolean_AndAveragesToNumeric()
    {
        var ddl = PgSchemaGenerator.CreateTable(CpuSchedulerStatsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS cpu_scheduler_stats (\n" +
            "    collection_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    max_workers_count integer,\n" +
            "    scheduler_count integer,\n" +
            "    cpu_count integer,\n" +
            "    total_runnable_tasks_count integer,\n" +
            "    total_work_queue_count bigint,\n" +
            "    total_current_workers_count integer,\n" +
            "    avg_runnable_tasks_count numeric(38,2),\n" +
            "    total_active_request_count integer,\n" +
            "    total_queued_request_count integer,\n" +
            "    total_blocked_task_count integer,\n" +
            "    total_active_parallel_thread_count bigint,\n" +
            "    runnable_request_count integer,\n" +
            "    total_request_count integer,\n" +
            "    runnable_percent numeric(38,2),\n" +
            "    worker_thread_exhaustion_warning boolean,\n" +
            "    runnable_tasks_warning boolean,\n" +
            "    blocked_tasks_warning boolean,\n" +
            "    queued_requests_warning boolean,\n" +
            "    total_physical_memory_kb bigint,\n" +
            "    available_physical_memory_kb bigint,\n" +
            "    system_memory_state_desc text,\n" +
            "    physical_memory_pressure_warning boolean,\n" +
            "    total_node_count integer,\n" +
            "    nodes_online_count integer,\n" +
            "    offline_cpu_count integer,\n" +
            "    offline_cpu_warning boolean\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_PlanCacheStats_MapsAvgUseCountToNumeric_AndOldestPlanToTimestamp()
    {
        var ddl = PgSchemaGenerator.CreateTable(PlanCacheStatsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS plan_cache_stats (\n" +
            "    collection_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    cacheobjtype text,\n" +
            "    objtype text,\n" +
            "    total_plans integer,\n" +
            "    total_size_mb integer,\n" +
            "    single_use_plans integer,\n" +
            "    single_use_size_mb integer,\n" +
            "    multi_use_plans integer,\n" +
            "    multi_use_size_mb integer,\n" +
            "    avg_use_count numeric(38,2),\n" +
            "    avg_size_kb integer,\n" +
            "    oldest_plan_create_time timestamp\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_SessionSummaryStats_MapsCountsToInteger_AndTopColumns()
    {
        var ddl = PgSchemaGenerator.CreateTable(SessionSummaryStatsCollector.Instance);

        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS session_summary_stats (\n" +
            "    collection_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    total_sessions integer,\n" +
            "    running_sessions integer,\n" +
            "    sleeping_sessions integer,\n" +
            "    background_sessions integer,\n" +
            "    dormant_sessions integer,\n" +
            "    idle_sessions_over_30min integer,\n" +
            "    sessions_waiting_for_memory integer,\n" +
            "    databases_with_connections integer,\n" +
            "    top_application_name text,\n" +
            "    top_application_connections integer,\n" +
            "    top_host_name text,\n" +
            "    top_host_connections integer\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_SystemHealthEvents_UsesSystemHealthEventIdPrefix_AndTextXml()
    {
        var ddl = PgSchemaGenerator.CreateTable(SystemHealthEventsCollector.Instance);

        /* Stage 1 raw-capture shape: the system_health_event_id bigint prefix (mirroring Lite's
           DuckDB PRIMARY KEY column), then event_time / event_type / event_xml — the XML lands as
           text (no shredding here; Stage 2 owns the parse). */
        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS system_health_events (\n" +
            "    system_health_event_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    event_time timestamp,\n" +
            "    event_type text,\n" +
            "    event_xml text\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_DefaultTraceEvents_UsesDefaultTraceEventIdPrefix_AndMirrorsPayloadOrder()
    {
        var ddl = PgSchemaGenerator.CreateTable(DefaultTraceEventsCollector.Instance);

        /* The default_trace_event_id bigint prefix (mirroring Lite's DuckDB PRIMARY KEY column), then the
           curated Default Trace payload in emission order. This generated V1 shape MUST equal the hardcoded
           V21 upgrade migration body (minus the collect. qualification) so a fresh store (V1) and an upgraded
           store (V21) land an identical table for the positional binary COPY. */
        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS default_trace_events (\n" +
            "    default_trace_event_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    event_time timestamp,\n" +
            "    event_name text,\n" +
            "    event_class integer,\n" +
            "    spid integer,\n" +
            "    database_name text,\n" +
            "    database_id integer,\n" +
            "    login_name text,\n" +
            "    host_name text,\n" +
            "    application_name text,\n" +
            "    object_name text,\n" +
            "    filename text,\n" +
            "    integer_data bigint,\n" +
            "    integer_data_2 bigint,\n" +
            "    text_data text,\n" +
            "    session_login_name text,\n" +
            "    error_number integer,\n" +
            "    severity integer,\n" +
            "    state integer,\n" +
            "    event_sequence bigint,\n" +
            "    duration_us bigint,\n" +
            "    end_time timestamp\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_JobHistory_UsesJobHistoryIdPrefix_AndMirrorsPayloadOrder()
    {
        var ddl = PgSchemaGenerator.CreateTable(JobHistoryCollector.Instance);

        /* The job_history_id bigint prefix (mirroring Lite's DuckDB PRIMARY KEY column), then the retained
           sysjobhistory payload in emission order (instance_id dedup key first). This generated V1 shape MUST
           equal the hardcoded V24 upgrade migration body (minus the collect. qualification) so a fresh store
           (V1) and an upgraded store (V24) land an identical table for the positional binary COPY (#1433). */
        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS job_history (\n" +
            "    job_history_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    instance_id bigint,\n" +
            "    job_id text,\n" +
            "    job_name text,\n" +
            "    job_enabled boolean,\n" +
            "    category_name text,\n" +
            "    step_id integer,\n" +
            "    step_name text,\n" +
            "    run_status integer,\n" +
            "    run_status_desc text,\n" +
            "    run_datetime timestamp,\n" +
            "    run_duration_seconds bigint,\n" +
            "    retries_attempted integer,\n" +
            "    message text\n" +
            ");",
            ddl);
    }

    [Fact]
    public void CreateTable_AgentStatus_MirrorsPayloadOrder()
    {
        var ddl = PgSchemaGenerator.CreateTable(AgentStatusCollector.Instance);

        /* Agent status snapshot (collection_id prefix), matching the V25 upgrade migration body (#1433 Phase 2). */
        Assert.Equal(
            "CREATE TABLE IF NOT EXISTS agent_status (\n" +
            "    collection_id bigint NOT NULL,\n" +
            "    collection_time timestamp NOT NULL,\n" +
            "    server_id integer NOT NULL,\n" +
            "    server_name text NOT NULL,\n" +
            "    agent_running boolean,\n" +
            "    agent_status_desc text,\n" +
            "    agent_startup_desc text,\n" +
            "    next_scheduled_run timestamp\n" +
            ");",
            ddl);
    }

    [Fact]
    public void Migrations_JobHistoryAndAgentStatus_MatchGeneratedFreshShape()
    {
        /* The additive upgrade bodies MUST be the collect.-qualified twin of the fresh (V1
           GenerateFullSchema) table, or a fresh store and an upgraded store drift apart. What breaks is
           NAMES and TYPES first: PgCollectorRowWriter.CopyCommandFor emits an explicit column list
           ("COPY t (a, b, ...)") built from PayloadColumns, so Postgres binds by name — a renamed or
           missing column fails the COPY outright and a retyped one mis-stores or throws. Column ORDER is
           not a COPY hazard on this side (it is on Lite's, whose DuckDB appender is genuinely positional),
           but it is pinned anyway so the two provenances stay physically comparable — a store's shape
           should not reveal whether it was installed fresh or upgraded. Pin each migration's CREATE TABLE
           against the generator's output. */
        /* Normalize line endings: CreateTable emits '\n'; the verbatim-string V##Sql consts carry the
           source file's CRLF, so compare on a common newline. */
        static string Lf(string s) => s.Replace("\r\n", "\n", StringComparison.Ordinal);

        static string CollectQualified(ICollectorSchemaInfo schema)
        {
            var fresh = Lf(PgSchemaGenerator.CreateTable(schema));
            return fresh.Replace(
                $"CREATE TABLE IF NOT EXISTS {schema.TargetTable} (",
                $"CREATE TABLE IF NOT EXISTS collect.{schema.TargetTable} (",
                StringComparison.Ordinal);
        }

        var v24 = Lf(PgMigrations.Scripts.Single(m => m.Version == 24).Sql);
        var v25 = Lf(PgMigrations.Scripts.Single(m => m.Version == 25).Sql);

        Assert.Contains(CollectQualified(JobHistoryCollector.Instance), v24, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_job_history_time ON collect.job_history(server_id, collection_time);", v24, StringComparison.Ordinal);
        Assert.Contains(CollectQualified(AgentStatusCollector.Instance), v25, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_agent_status_time ON collect.agent_status(server_id, collection_time);", v25, StringComparison.Ordinal);

        /* V76 (#2319) creates query_store_health for an already-existing store — same contract as the
           blocks below. */
        var v76 = Lf(PgMigrations.Scripts.Single(m => m.Version == 76).Sql);

        Assert.Contains(CollectQualified(QueryStoreHealthCollector.Instance), v76, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_query_store_health_time ON collect.query_store_health(server_id, capture_time);", v76, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_query_store_health AS SELECT * FROM query_store_health;", v76, StringComparison.Ordinal);

        /* V47 (#1951) creates pvs_stats for an already-existing store; a fresh store gets it from V1's
           GenerateFullSchema. Same contract as V24/V25 — and it is the ONLY thing standing between a
           hand-typed 26-column CREATE TABLE and a silent fresh-vs-upgraded shape fork. */
        var v47 = Lf(PgMigrations.Scripts.Single(m => m.Version == 47).Sql);

        Assert.Contains(CollectQualified(PvsStatsCollector.Instance), v47, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_pvs_stats_time ON collect.pvs_stats(server_id, collection_time);", v47, StringComparison.Ordinal);

        /* V34 (#991) creates BOTH Availability Group tables in one migration body — same contract. The
           weaker `Assert.Contains(column.Name)` sweep this replaces would have passed a V34 with the
           columns reordered, is_local typed text, or a spurious NOT NULL: exactly the drifts that make an
           upgraded store's physical shape differ from a fresh one. */
        var v34 = Lf(PgMigrations.Scripts.Single(m => m.Version == 34).Sql);

        /* The REPLICA-grain table is now the same two-migration story as the database grain below: V34
           created its first 10 payload columns and V37 (#1696) appended is_local, so an upgraded store's
           shape is V34 + V37 and only their sum equals the generator's current output. */
        var replicaColumns = AgReplicaStatesCollector.Instance.PayloadColumns;
        const int V34ReplicaColumnCount = 10;

        Assert.Contains(
            CollectQualified(new TruncatedSchema(AgReplicaStatesCollector.Instance, V34ReplicaColumnCount)),
            v34,
            StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_ag_replica_states_time ON collect.ag_replica_states(server_id, collection_time);", v34, StringComparison.Ordinal);

        var v37 = Lf(PgMigrations.Scripts.Single(m => m.Version == 37).Sql);

        foreach (var column in replicaColumns.Skip(V34ReplicaColumnCount))
        {
            var generatedType = Lf(PgSchemaGenerator.CreateTable(new TruncatedSchema(AgReplicaStatesCollector.Instance, replicaColumns.Count)))
                .Split('\n')
                .Single(l => l.TrimStart().StartsWith(column.Name + " ", StringComparison.Ordinal))
                .Trim()
                .TrimEnd(',');

            Assert.Contains($"ADD COLUMN IF NOT EXISTS {generatedType}", v37, StringComparison.Ordinal);
        }

        /* No "V34 was not widened in place" sweep for this grain, unlike the database one below: the
           TruncatedSchema assertion above already matches V34's ag_replica_states block EXACTLY, which is a
           strictly stronger statement than any name-absence check. A substring sweep would also be wrong
           here — is_local is a real V34 column on the DATABASE grain, so searching the whole migration body
           for it finds the other table's legitimate column and fails. */
        Assert.Contains("CREATE INDEX IF NOT EXISTS idx_ag_database_replica_states_time ON collect.ag_database_replica_states(server_id, collection_time);", v34, StringComparison.Ordinal);

        /* The database-grain table is the one case where a single migration is NOT the whole story: V34
           created its first 15 payload columns and V36 (#991 addendum) appended 6 more, so an upgraded
           store's shape is V34 + V36 and only their SUM can equal the generator's current output.
           Reconstruct that here rather than weakening the pin to name-presence — generate the historical
           15-column shape and assert V34 matches it exactly, then assert V36 appends the remaining columns
           in order with the generator's own types. Together those two prove fresh == upgraded. */
        var currentColumns = AgDatabaseReplicaStatesCollector.Instance.PayloadColumns;
        const int V34ColumnCount = 15;

        Assert.Contains(
            CollectQualified(new TruncatedSchema(AgDatabaseReplicaStatesCollector.Instance, V34ColumnCount)),
            v34,
            StringComparison.Ordinal);

        var v36 = Lf(PgMigrations.Scripts.Single(m => m.Version == 36).Sql);

        foreach (var column in currentColumns.Skip(V34ColumnCount))
        {
            var generatedType = Lf(PgSchemaGenerator.CreateTable(new TruncatedSchema(AgDatabaseReplicaStatesCollector.Instance, currentColumns.Count)))
                .Split('\n')
                .Single(l => l.TrimStart().StartsWith(column.Name + " ", StringComparison.Ordinal))
                .Trim()
                .TrimEnd(',');

            Assert.Contains($"ADD COLUMN IF NOT EXISTS {generatedType}", v36, StringComparison.Ordinal);
        }

        /* And V34 must NOT have been widened in place: its CREATE TABLE IF NOT EXISTS is a no-op on a store
           that already ran it, so editing V34 instead of adding V36 would leave every migrated store short
           the new columns while fresh installs got them. */
        foreach (var appended in currentColumns.Skip(V34ColumnCount))
        {
            Assert.DoesNotContain($"    {appended.Name} ", v34, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// A collector's schema surface with its payload truncated to the first N columns — lets a test
    /// generate the HISTORICAL shape a migration froze, so an additive migration can be pinned as
    /// "V-old created these, V-new appended those, and together they equal today's generated table".
    /// </summary>
    private sealed class TruncatedSchema : ICollectorSchemaInfo
    {
        private readonly ICollectorSchemaInfo _inner;

        public TruncatedSchema(ICollectorSchemaInfo inner, int columnCount)
        {
            _inner = inner;
            PayloadColumns = inner.PayloadColumns.Take(columnCount).ToList();
        }

        public string Name => _inner.Name;
        public string TargetTable => _inner.TargetTable;
        public bool IncludesCollectionId => _inner.IncludesCollectionId;
        public string PrefixIdColumnName => _inner.PrefixIdColumnName;
        public string PrefixTimeColumnName => _inner.PrefixTimeColumnName;
        public IReadOnlyList<CollectorColumn> PayloadColumns { get; }
        public bool AppliesTo(CollectorTargetInfo target) => _inner.AppliesTo(target);

        public bool YieldsOnLockTimeout => _inner.YieldsOnLockTimeout;

        public IReadOnlyList<string> StateKeys => _inner.StateKeys;
    }

    [Fact]
    public void CreateIndex_MirrorsLiteIndexColumns()
    {
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_wait_stats_time ON wait_stats(server_id, collection_time);",
            PgSchemaGenerator.CreateIndex(WaitStatsCollector.Instance));
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_trace_flags_time ON trace_flags(server_id, capture_time);",
            PgSchemaGenerator.CreateIndex(TraceFlagsCollector.Instance));
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_memory_pressure_events_time ON memory_pressure_events(server_id, sample_time);",
            PgSchemaGenerator.CreateIndex(MemoryPressureEventsCollector.Instance));
        Assert.Equal(
            "CREATE INDEX IF NOT EXISTS idx_index_object_stats_object ON index_object_stats(server_id, database_name, object_id, index_id, collection_time);",
            PgSchemaGenerator.CreateIndex(IndexObjectStatsCollector.Instance));
        Assert.Null(PgSchemaGenerator.CreateIndex(ServerConfigCollector.Instance));
        Assert.Null(PgSchemaGenerator.CreateIndex(DatabaseConfigCollector.Instance));
    }

    /// <summary>
    /// THE LADDER-GENERATOR DIFF. Every PostgreSQL collector rung's hand-written DDL must be
    /// column-for-column identical to what <see cref="PgSchemaGenerator"/> emits for that collector.
    ///
    /// <para><b>Why this invariant matters more than it looks.</b> There are two populations of store and
    /// they get their tables from different places: a FRESH store's tables come from V1's generated schema
    /// (walked from the collector catalog), while an ALREADY-EXISTING store's come from these rungs. Nothing
    /// forces the two texts to agree. Let one column's type drift and the divergence is permanent and
    /// invisible — every read works against one population and fails against the other, and which one you
    /// have depends on when the store was created.</para>
    ///
    /// <para>Whitespace is normalised because the rungs wrap their <c>CREATE INDEX</c> across two lines for
    /// readability and the generator emits it on one; the schema qualification is normalised because the
    /// rungs name <c>collect.</c> explicitly while V1 runs with <c>search_path</c> already set. Those two are
    /// the only differences that are allowed to exist, so they are the only two normalised away — anything
    /// else, including a reordered column, fails.</para>
    ///
    /// <para>Written after the fact for all eight rungs at once, because adding the eighth collector was the
    /// first time anyone checked: the suite asserted the generator emits every table and that each rung is
    /// well-formed, but never that they said the SAME thing.</para>
    /// </summary>
    [Fact]
    public void EveryPostgresRung_IsIdenticalToTheGeneratedSchema()
    {
        var rungs = new (int Version, ICollectorSchemaInfo Collector)[]
        {
            (63, PgWaitStatsCollector.Instance),
            (64, PgStatementStatsCollector.Instance),
            (65, PgWraparoundStatsCollector.Instance),
            (66, PgXminHorizonCollector.Instance),
            (67, PgReplicationSlotsCollector.Instance),
            (68, PgAutovacuumStatsCollector.Instance),
            (69, PgIoStatsCollector.Instance),
            (71, PgBlockingCollector.Instance),
            (83, PgDatabaseStatsCollector.Instance),
            (84, PgIndexUsageStatsCollector.Instance),
            (85, PgTableBloatStatsCollector.Instance),
            (86, PgSessionStatesCollector.Instance),
            (87, PgPlanCaptureReadinessCollector.Instance),
            (88, PgWriteStatsCollector.Instance),
            (89, PgExtensionAvailabilityCollector.Instance),
            (90, PgLockStatsCollector.Instance),
            (91, PgColumnStatsCollector.Instance),
            (92, PgReplicationStatsCollector.Instance),
            (93, PgBufferUsageCollector.Instance),
            (94, PgIndexBloatCollector.Instance),
            (96, PgWaitSamplingCollector.Instance),
            (97, PgKernelStatsCollector.Instance),
            (98, PgPredicateStatsCollector.Instance),
        };

        /* Every PostgreSQL collector must appear above. One added without a rung listed here would
           otherwise pass this test by simply not being checked, which is why the expectation is DERIVED
           from the catalog rather than written as a second literal. */
        Assert.Equal(
            CollectorCatalog.All.Count(c => c.TargetEngine == CollectorTargetEngine.PostgreSql),
            rungs.Length);

        foreach (var (version, collector) in rungs)
        {
            var rung = NormalizeDdl(PgMigrations.Scripts.Single(m => m.Version == version).Sql);

            var generated = NormalizeDdl(
                    PgSchemaGenerator.CreateTable(collector)
                    + "\n\n"
                    + PgSchemaGenerator.CreateIndex(collector))
                .Replace(
                    $"CREATE TABLE IF NOT EXISTS {collector.TargetTable} (",
                    $"CREATE TABLE IF NOT EXISTS collect.{collector.TargetTable} (",
                    StringComparison.Ordinal)
                .Replace(
                    $"ON {collector.TargetTable}(",
                    $"ON collect.{collector.TargetTable}(",
                    StringComparison.Ordinal);

            Assert.Equal(generated, rung);
        }
    }

    private static string NormalizeDdl(string sql) =>
        System.Text.RegularExpressions.Regex.Replace(sql, @"\s+", " ").Trim();

    [Fact]
    public void GenerateFullSchema_EmitsEveryTableAndIndex()
    {
        var script = PgSchemaGenerator.GenerateFullSchema();

        /* EVERY table, asserted against the catalog count rather than a literal — the test's name is the
           invariant, and a literal here silently became a subset check (it read 46 of 48) the moment the
           catalog grew. A collector whose table the generator skips still fails this. */
        var tableCount = CollectorCatalog.All.Count(s => script.Contains($"CREATE TABLE IF NOT EXISTS {s.TargetTable} (", StringComparison.Ordinal));
        Assert.Equal(CollectorCatalog.All.Count, tableCount);

        /* Every table gets a retrieval index except the two index-less config tables (server_config,
           database_config), which CreateIndex returns null for — so this tracks the catalog minus exactly
           those two, not a literal that has to be remembered per collector. */
        var indexCount = script.Split("CREATE INDEX IF NOT EXISTS").Length - 1;
        Assert.Equal(CollectorCatalog.All.Count - 2, indexCount);

        /* The precision guard can never regress silently. */
        Assert.DoesNotContain("numeric(0,0)", script, StringComparison.Ordinal);
    }

    [Fact]
    public void GenerateV8Move_CreatesSchemas_AndMovesEveryCatalogTableToCollect()
    {
        var v8 = PgSchemaGenerator.GenerateV8Move();

        Assert.Contains("CREATE SCHEMA IF NOT EXISTS collect AUTHORIZATION darling;", v8, StringComparison.Ordinal);
        Assert.Contains("CREATE SCHEMA IF NOT EXISTS config AUTHORIZATION darling;", v8, StringComparison.Ordinal);

        /* Every collector table moves to collect — catalog-driven, so a new collector moves for free. */
        foreach (var schema in CollectorCatalog.All)
        {
            Assert.Contains($"ALTER TABLE IF EXISTS public.{schema.TargetTable} SET SCHEMA collect;", v8, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void GenerateV8Move_MovesMetadataAndViewsToCollect_AndConfigTablesToConfig()
    {
        var v8 = PgSchemaGenerator.GenerateV8Move();

        foreach (var table in PgSchemaGenerator.CollectMetadataTables)
        {
            Assert.Contains($"ALTER TABLE IF EXISTS public.{table} SET SCHEMA collect;", v8, StringComparison.Ordinal);
        }

        Assert.Equal(24, PgSchemaGenerator.CollectViews.Count);
        foreach (var view in PgSchemaGenerator.CollectViews)
        {
            Assert.Contains($"ALTER VIEW IF EXISTS public.{view} SET SCHEMA collect;", v8, StringComparison.Ordinal);
        }

        /* Exactly the operator-writable coordination tables go to config — the admin write surface. */
        Assert.Equal(
            new[] { "config_alert_log", "config_edge_trigger_watermarks", "config_mute_rules", "analysis_muted" },
            PgSchemaGenerator.ConfigTables);
        foreach (var table in PgSchemaGenerator.ConfigTables)
        {
            Assert.Contains($"ALTER TABLE IF EXISTS public.{table} SET SCHEMA config;", v8, StringComparison.Ordinal);
        }

        /* analysis_findings is service-written / viewer-read-only -> collect, not config (fork #1). */
        Assert.Contains("ALTER TABLE IF EXISTS public.analysis_findings SET SCHEMA collect;", v8, StringComparison.Ordinal);
        Assert.DoesNotContain("public.analysis_findings SET SCHEMA config;", v8, StringComparison.Ordinal);

        /* The ALTER DATABASE search_path default is NOT baked into V8 — it is best-effort in MigrateAsync. */
        Assert.DoesNotContain("ALTER DATABASE", v8, StringComparison.Ordinal);
    }

    [Fact]
    public void SearchPath_ListsCollectConfigPublicInOrder()
    {
        Assert.Equal("collect, config, public", PgSchemaGenerator.SearchPath);
    }
}
