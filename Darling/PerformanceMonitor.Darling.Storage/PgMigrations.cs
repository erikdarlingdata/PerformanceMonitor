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

    /// <summary>
    /// V152 (#4503) — drops the auto-created per-column GROUP BY index TimescaleDB builds on six Query Store
    /// rollups' materialization hypertables, on every store that upgrades through this rung, the same way
    /// #3597 did for <see cref="TimescaleSupport.QueryStoreStatsIntervalHourlyView"/>'s materialization at
    /// CREATE time. A production catalog read found the same shape repeated on
    /// <see cref="TimescaleSupport.QueryStoreStatsHourlyView"/>, <see cref="TimescaleSupport.QueryStoreStatsCorrectedHourlyView"/>,
    /// <see cref="TimescaleSupport.QueryStoreStatsDailyView"/>, <see cref="TimescaleSupport.QueryStoreStatsCorrectedDailyView"/>,
    /// <see cref="TimescaleSupport.QueryStoreStatsIntervalDailyView"/> and
    /// <see cref="TimescaleSupport.QueryStoreStatsDayGrainDailyView"/>: five group indexes apiece
    /// (<c>database_name</c>, <c>module_name</c>, <c>query_hash</c>, <c>server_id</c>, <c>server_name</c>, each
    /// paired with <c>bucket DESC</c>) on the first five, eleven on the interval-daily view, and lifetime
    /// <c>idx_scan</c> of ZERO on every one of them except <c>server_id</c> (kept) and <c>server_name</c> (kept
    /// on the two hourly views only, where reads measurably use it) — the reader check this rung's PR body
    /// carries in full.
    ///
    /// <para><b>Resolved by view name and by the indexed COLUMN, never by a string-built index name
    /// (#4503).</b> A continuous aggregate's materialization hypertable id is assigned at CREATE time and
    /// differs per store — the production catalog this rung read from happened to number them
    /// 103/106/109/110/112/113, but a fresh or differently-ordered store would not; that part was already
    /// handled by resolving <c>materialization_hypertable_name</c> from
    /// <c>timescaledb_information.continuous_aggregates</c>. What was NOT safe is building the auto-created
    /// index's NAME as a string and passing it to <c>DROP INDEX IF EXISTS</c>: PostgreSQL truncates an
    /// identifier at 63 bytes with no hash suffix, and
    /// <c>_materialized_hypertable_112_runtime_stats_interval_id_bucket_idx</c> is 68 characters, so the name
    /// actually on disk is the 63-byte truncation, not the string this rung would have built — the DROP
    /// would have silently no-op'd against a name nothing wears, leaving the real index in place. The
    /// <c>DO</c> block below never builds that name: for each view it resolves the materialization's OID,
    /// then reads <c>pg_index</c>/<c>pg_attribute</c> directly for a two-key btree whose FIRST key column is
    /// one of that view's drop-list columns and whose SECOND key column is <c>bucket</c> — the exact shape
    /// <c>create_group_indexes</c>'s default builds — and drops whatever index actually carries that shape,
    /// by its real (possibly-truncated) name via <c>::regclass</c>. A single-column index (the kept
    /// <c>bucket_idx</c>) and a two-key index whose kept column ISN'T in the drop list both fail the match
    /// and are never touched. A plain-PostgreSQL store, which has never created the extension, has no
    /// <c>timescaledb_information</c> catalog to read at all, so the whole block returns before the loop —
    /// a no-op there, not a per-view skip. A TimescaleDB store missing one of the six CAGGs (one that never
    /// enabled a given rollup) skips just that view rather than erroring, and re-running the block after the
    /// indexes are already gone is a no-op — the catalog read finds nothing to drop.</para>
    ///
    /// <para><b><c>SET LOCAL lock_timeout</c>, derived from <see cref="MigrationCommandTimeoutSeconds"/>.</b>
    /// <c>DROP INDEX</c> takes <c>AccessExclusiveLock</c> on the materialization hypertable and on every one
    /// of its chunks, across six hypertables in one transaction, while a background refresh policy can hold
    /// the same lock for as long as its own run takes — measured around 264 s on the hourly rollups, well
    /// past a flat 5 s. Rather than fail the rung on the very refresh it is racing, the lock_timeout here is
    /// set to <see cref="MigrationCommandTimeoutSeconds"/> minus a 20 s margin (280 s): long enough to wait
    /// out one whole refresh cycle, but still short enough that the server's own clean, retryable
    /// <c>55P03</c> always fires before the client-side <see cref="MigrationCommandTimeoutSeconds"/> command
    /// timeout would cancel the statement out from under it — if a refresh somehow outlasts even that, the
    /// failure is still retryable (lock-not-available is already in the retryable set the service's startup
    /// triage carries), so the next start retries the same rung, still at V151, rather than blocking
    /// collection. This is a per-STATEMENT wait inside one rung, smaller by construction than
    /// <see cref="MigrationLockWaitTimeoutSeconds"/>, the whole-SESSION budget a sibling migrator polls
    /// against; V152 spending up to 280 s of its own command timeout still leaves that budget's other
    /// multiples for the rest of the ladder.</para>
    /// </summary>
    private static readonly string V152Sql = @"
SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + @"s';
DO $$
DECLARE
    v_view text;
    v_drop_cols text[];
    v_mat_schema text;
    v_mat_table text;
    v_mat_oid regclass;
    r record;
BEGIN
    /* timescaledb_information only exists once the extension has been created, and this runs on stores
       where it never was -- reaching it unconditionally raises 42P01. Probed with to_regclass (NULL rather
       than an error when absent); no extension means there are no continuous aggregates and so no rollup
       group indexes to drop, which makes this rung a no-op there. The per-view read below goes through
       EXECUTE so the timescaledb_information reference is parsed only when it runs. */
    IF to_regclass('timescaledb_information.continuous_aggregates') IS NULL THEN
        RETURN;
    END IF;

    FOR v_view, v_drop_cols IN VALUES
        ('query_store_stats_hourly',          ARRAY['database_name', 'module_name', 'query_hash']),
        ('query_store_stats_corrected_hourly', ARRAY['database_name', 'module_name', 'query_hash']),
        ('query_store_stats_daily',           ARRAY['database_name', 'module_name', 'query_hash', 'server_name']),
        ('query_store_stats_corrected_daily',  ARRAY['database_name', 'module_name', 'query_hash', 'server_name']),
        ('query_store_stats_interval_daily',   ARRAY['database_name', 'execution_type_desc', 'first_execution_time', 'module_name', 'plan_id', 'query_hash', 'query_id', 'replica_role', 'runtime_stats_interval_id', 'server_name']),
        ('query_store_stats_daygrain_daily',   ARRAY['database_name', 'module_name', 'query_hash', 'server_name'])
    LOOP
        EXECUTE 'SELECT materialization_hypertable_schema, materialization_hypertable_name
                 FROM timescaledb_information.continuous_aggregates
                 WHERE view_schema = ''collect'' AND view_name = $1'
        INTO v_mat_schema, v_mat_table
        USING v_view;

        IF v_mat_table IS NULL THEN
            CONTINUE;
        END IF;

        v_mat_oid := format('%I.%I', v_mat_schema, v_mat_table)::regclass;

        FOR r IN
            SELECT i.indexrelid::regclass AS idx
            FROM pg_index i
            JOIN pg_attribute a1 ON a1.attrelid = i.indrelid AND a1.attnum = i.indkey[0]
            JOIN pg_attribute a2 ON a2.attrelid = i.indrelid AND a2.attnum = i.indkey[1]
            WHERE i.indrelid = v_mat_oid
            AND   i.indnkeyatts = 2
            AND   NOT i.indisunique
            AND   a1.attname = ANY (v_drop_cols)
            AND   a2.attname = 'bucket'
        LOOP
            EXECUTE format('DROP INDEX IF EXISTS %s', r.idx);
        END LOOP;
    END LOOP;
END $$;";

    /// <summary>
    /// V153 (#4608, split #4615) — a plain btree on <c>first_execution_time</c> for
    /// <see cref="PerformanceMonitor.Darling.Storage.QueryStoreIntervalLatest"/>'s <c>query_store_interval_latest</c>
    /// (V143) only — the column both the daily retention sweep's
    /// <see cref="PerformanceMonitor.Darling.Service.DarlingRetention.TimeSlicedDeleteSql"/> filters on and the
    /// read gate's per-server floor (<c>PlainTableFloorSql</c>) reads. V154 is the twin rung for
    /// <c>query_store_interval_wide</c> (V145) — split into its own rung (#4615) so each index build gets its
    /// own <see cref="MigrationCommandTimeoutSeconds"/> window rather than sharing one across both tables.
    ///
    /// <para><b>The measured cost (#4608).</b> On a rig seeded with 4M <c>query_store_interval_latest</c> rows
    /// over 20 days: the purge's cold-cache "nothing to delete" run cost ~4.09 s (a Seq-equivalent full index
    /// scan of the table's unique index, since neither <c>min()</c> subquery nor the outer DELETE had a
    /// leading column to seek on) and ~211 ms warm; the same statement with this index costs ~0.56 ms cold and
    /// ~0.12 ms warm — three to four orders of magnitude down, and it stays flat regardless of table size
    /// because it seeks straight past the cutoff instead of walking the whole index. A one-day-to-delete run
    /// (200 k rows) cost ~587 ms without the index and ~0.1-52 ms with it. The read gate's per-server floor
    /// (<c>MIN(first_execution_time) WHERE server_id = $1</c>) already had an efficient plan off the existing
    /// unique index (~9 ms) — this rung's index gives it an equally fast plan (~1.4 ms) without displacing
    /// that path; either index serves it.</para>
    ///
    /// <para><b>Why a plain index, not <c>CONCURRENTLY</c>.</b> <c>MigrateAsync</c> wraps every rung in one
    /// transaction, and <c>CREATE INDEX CONCURRENTLY</c> cannot run inside a transaction block (PostgreSQL
    /// rejects it, 25001). The lock this takes is <c>ShareLock</c> (a plain <c>CREATE INDEX</c>, not a
    /// rewrite of an existing index), which blocks writers to the table for the build's duration but not
    /// readers. At the rig's 4M row size the build itself took low single-digit seconds; a field store's
    /// two tables are kept to 15 and 10 days respectively (the wide table's 9 until #5809) by the same purge this
    /// index speeds up, so neither grows unbounded between upgrades.</para>
    ///
    /// <para><b>Why not <see cref="UnorderedRowCappedDeleteSql"/> instead (the alternative measured for #4608).</b>
    /// That builder exists for the two plain (non-hypertable) tables whose V149 migration deliberately
    /// dropped their own <c>last_seen</c> btree so an unordered cap could avoid a second sort pass — it does
    /// not apply here, where V143/V145 never had a <c>first_execution_time</c> index to drop in the first
    /// place and the read gate needs one whether or not the purge does. Measured, <see cref="UnorderedRowCappedDeleteSql"/>
    /// without an index still costs ~234 ms per 50 k-row-capped batch on this rig's size (it still walks the
    /// unique index looking for rows under the cutoff before it can build the <c>ctid</c> list) — slower than
    /// this rung's indexed <see cref="PerformanceMonitor.Darling.Service.DarlingRetention.TimeSlicedDeleteSql"/>
    /// and it leaves the read gate's floor scan unindexed too. The index serves both call sites from one
    /// object, which is why it is the winner here.</para>
    ///
    /// <para><b><c>max_parallel_maintenance_workers = 2</c> (#4615).</b> Measured on a rig seeded to 20 M
    /// <c>query_store_interval_latest</c> rows with the field store's own <c>maintenance_work_mem</c>
    /// (2047 MB): a serial build (<c>max_parallel_maintenance_workers = 0</c>) took ~7.0-8.0 s; with 2
    /// workers it took ~3.0-3.2 s, a consistent ~2.3x speed-up over three runs each way. 2 is what this
    /// rig's own <c>max_parallel_workers</c> (15) and <c>max_worker_processes</c> (85) both allow with
    /// headroom to spare, and it is a plain, user-settable GUC — harmless to set on a bring-your-own store
    /// that has never heard of this service. Set inside the same <c>SET LOCAL</c> scope as
    /// <c>lock_timeout</c>: the applier gives this rung's whole SQL ONE <c>NpgsqlCommand</c> inside ONE
    /// transaction (see <see cref="MigrateLockedAsync"/>), and <c>SET LOCAL</c> is scoped to the
    /// transaction, so it is in force for the <c>CREATE INDEX</c> statement below it in the same rung.</para>
    /// </summary>
    private static readonly string V153Sql = @"
SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + @"s';
SET LOCAL max_parallel_maintenance_workers = 2;
CREATE INDEX IF NOT EXISTS idx_query_store_interval_latest_first_exec
ON collect.query_store_interval_latest (first_execution_time);";

    /// <summary>
    /// V154 (#4608, split #4615) — <c>query_store_interval_wide</c>'s (V145) twin of V153's index, in its
    /// own rung so its build gets its own <see cref="MigrationCommandTimeoutSeconds"/> window rather than
    /// sharing V153's. See V153Sql's doc comment for the measured cost, the reason for a plain (not
    /// <c>CONCURRENTLY</c>) index, and the <c>max_parallel_maintenance_workers</c> measurement — both hold
    /// identically here.
    /// </summary>
    private static readonly string V154Sql = @"
SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + @"s';
SET LOCAL max_parallel_maintenance_workers = 2;
CREATE INDEX IF NOT EXISTS idx_query_store_interval_wide_first_exec
ON collect.query_store_interval_wide (first_execution_time);";

    /// <summary>
    /// V155 (#4765) — each Query Store interval's END, on <c>collect.query_store_stats</c> and on
    /// <c>collect.query_store_interval_wide</c> (V145): <c>interval_end_time_utc</c>
    /// (<c>sys.query_store_runtime_stats_interval.end_time</c>, converted to UTC at collection), the twin of
    /// V41's <c>interval_start_time_utc</c>.
    ///
    /// <para>A rate divides an interval's totals by the interval's length. Until now the only length a read
    /// had was the time since the previous STORED interval, and Query Store stores no row for an interval with
    /// no executions, so an interval that follows a quiet one divided by the gap PLUS its own length and read
    /// too low. End minus start is the interval's own length. This rung only stores the end; the reads that
    /// turn it into a rate change separately.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, the V41 shape: a row collected before this rung never
    /// asked the engine for the end and nothing can reconstruct it, so NULL is the honest value and a reader
    /// treats it as "fall back to the previous-interval gap". A nullable, default-less <c>ADD COLUMN</c> is
    /// catalog-only in PostgreSQL and TimescaleDB accepts it on a compressed hypertable, the shape
    /// V127/V128/V132/V133/V150/V151 used.</para>
    ///
    /// <para><b>Appended LAST on both tables.</b> Both bulk writers are positional (the binary COPY into
    /// <c>query_store_stats</c>, the <c>INSERT ... SELECT</c> that composes <c>query_store_interval_wide</c>),
    /// so a fresh store and an upgraded one must share one physical column order. A fresh
    /// <c>query_store_stats</c> gets the column from V1's generated CREATE TABLE (the collector's payload
    /// carries it, appended last) and the ALTER no-ops there; V145's CREATE TABLE is fixed text, so the
    /// interval-wide column always arrives here, after <c>interval_start_time_utc</c>. The trailing
    /// <c>CREATE OR REPLACE VIEW</c> re-expands <c>v_query_store_stats</c>' pinned <c>SELECT *</c> (Postgres
    /// freezes it at CREATE; append-only ADDs keep the refresh legal), the V41 idiom. It is unqualified like
    /// V41's so it resolves through the migrate session's <c>search_path = collect, config, public</c> and
    /// stays visible to the drift guard that scans for that form.</para>
    /// </summary>
    private const string V155Sql = @"
ALTER TABLE collect.query_store_stats ADD COLUMN IF NOT EXISTS interval_end_time_utc timestamp;
ALTER TABLE collect.query_store_interval_wide ADD COLUMN IF NOT EXISTS interval_end_time_utc timestamp;
CREATE OR REPLACE VIEW v_query_store_stats AS SELECT * FROM query_store_stats;";

    /// <summary>
    /// V156 (#4834) — the hour's LONGEST single checkpoint sync on the store's own checkpointer row: two nullable
    /// columns on <c>collect.store_metrics</c>, <c>checkpoint_longest_sync_ms</c> (<c>bigint</c>) and
    /// <c>checkpoint_longest_sync_at</c> (<c>timestamp</c>, naive UTC — the sample that saw it, so the minute the
    /// checkpoint had finished by). No new table, no new hypertable (<c>TimescaleSupport.HypertableCount</c> stays
    /// 72), no DEFAULT, no backfill, no passthrough refresh, and <b>no Lite twin</b>: Lite stores no
    /// <c>store_metrics</c>.
    ///
    /// <para><b>The gap this closes.</b> Before this rung the hourly checkpointer row held only cumulative counters.
    /// The pressure rule (<c>CheckpointerReading.IsPressure</c>, read by <c>get_store_metrics</c> and by the Store
    /// Checkpointer Pressure self-alert through the same reader) differenced the two newest rows to the interval's
    /// total sync time and averaged it over the checkpoints the interval held (V140, #4037), so the alert and the tool
    /// agreed, and both were blind to the maximum. An average spreads one long sync over the interval's short ones:
    /// one 23.5 s sync among four checkpoints averages 5.9 s, under the 10 s bar, so an hour in which a single sync
    /// stalled every reader read as "no pressure". The worker now samples the checkpointer's cumulative sync time
    /// once a minute (#4823); this rung stores the largest difference that sample saw in the hour on the row the
    /// hour's other checkpointer facts already live on, where the tool, the alert and any raw read see the same
    /// value.</para>
    ///
    /// <para><b>Filled on the <c>object_kind = 'checkpointer'</c> row only</b>, NULL on every other kind by the
    /// table's per-kind convention (V137, V139, V140). NULL on that row too when the sampler took no difference in
    /// the hour (a service just started, or every read of the window failed): NULL is "no evidence", which the
    /// reader judges exactly as it judged the row before this rung existed, never as a zero. <c>store_metrics</c> is a
    /// PLAIN table and must stay one (the sweep's own retention DELETE assumes it), so a nullable, default-less
    /// <c>ADD COLUMN</c> rewrites nothing; on a compressed hypertable it is the same catalog-only shape V127/V128/V133
    /// /V137/V138/V139 used, but this rung touches none. It has no <c>v_</c> passthrough (V53; and
    /// <c>PgSchemaGenerator.AllPassthroughViews</c> agrees), so the V14 frozen-column-list problem cannot arise, and
    /// it is not a collector table, so no generator walks it and the columns live in this ALTER alone. The stamp is
    /// written from a naive-UTC parameter, never a bare cast that would render in the session's zone.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It does not backfill: rows written before it cannot
    /// know what the sampler saw. It adds no alert, knob or Viewer column. The reader's pressure rule and the tool's
    /// block change in the same commit, but both are reads of these two columns, not schema.</para>
    /// </summary>
    private const string V156Sql = @"
/* store_metrics is a plain table with no v_ passthrough (V53). Filled on the object_kind = 'checkpointer' row only,
   NULL on every other kind by the table's per-kind convention, and NULL there too when the sampler took no
   difference in the hour. Nullable, no DEFAULT, no backfill: a pre-rung row cannot know what the sampler saw, and
   NULL is what the reader judges as no evidence. The stamp is naive UTC. */
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS checkpoint_longest_sync_ms bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_longest_sync_at timestamp;";

    /// <summary>
    /// V157 — <c>config.config_mute_rules.server_id</c>: a mute rule can name a server by its store id.
    ///
    /// <para>A server's display name is not an identity. A blank display name falls back to the host, so two
    /// registrations on one logical server (two Azure SQL Database databases on one logical server) shared a
    /// whole-server silence key: silencing one silenced the other, and unsilencing one lifted both. The store id is
    /// the identity that is unique per registration, so a rule that carries it matches only that server and its
    /// <c>server_name</c> becomes a label.</para>
    ///
    /// <para><b>NULL is a legacy name-keyed rule</b>, matched by <c>server_name</c> exactly as before, so every
    /// stored rule keeps its effect and there is no backfill (a rule's name cannot be resolved to one id when the name
    /// is the ambiguity). Nullable, no DEFAULT, one catalog-only <c>ADD COLUMN</c>. Needs no GRANT (the roles' table-level
    /// grants cover a new column) and no trigger (the V117 reload beacon fires on any change to the table). The id is
    /// deliberately NOT a foreign key: a rule outlives a removed server's registration, as a name-keyed rule always did.</para>
    /// </summary>
    private const string V157Sql = @"ALTER TABLE config.config_mute_rules ADD COLUMN IF NOT EXISTS server_id integer;";

    /// <summary>
    /// V158 — <c>config.config_install_id</c>: the install id (#4961), one row.
    ///
    /// <para>The install id is the eight characters that tell this install's Extended Events sessions from another
    /// install's on a server both monitor, in the name of every session an install makes there. It has to outlive a
    /// restart and be the same for every part of the install, so it lives in the store, the one thing every part of
    /// the install shares. No store-wide table existed to hold it, so this is one: a single row pinned to
    /// <c>id = 1</c> like <c>config_alert_settings</c>, the id under a CHECK for exactly eight lowercase hex digits
    /// (lowercase because, on a case-sensitive server collation, an id read back in another case would name a
    /// different session than the one it made), and the binding stored beside it.</para>
    ///
    /// <para><b>The binding</b> is the cluster's <c>system_identifier</c> and the store database's OID, the two values
    /// that a physical copy of this store keeps and a different store does not. A row whose binding does not match
    /// the store it is read from was made for another store, so the service makes a new id and logs one Warning that
    /// names both; the old id's sessions are left alone because the install it came from may still run. Neither value
    /// is a host name: a recreated container gets a new host name and is still the same store. The OID is NOT NULL,
    /// because every login can read it. The cluster id is nullable: a login without superuser rights may call
    /// <c>pg_control_system()</c> by default (checked on PostgreSQL 18), but a managed or hardened server can revoke it,
    /// and the service has to start there. A NULL cluster id means the make could not read it, so the id is bound to the
    /// database alone. The OID always counts when the row is compared with the store; the cluster id counts only when
    /// both the stored row and the current read have one, so a grant that changes later never makes a new id on its
    /// own.</para>
    ///
    /// <para><b>DDL only.</b> The service inserts the row at start, after the migrations and before any worker
    /// (<c>INSERT ... ON CONFLICT DO NOTHING</c>, then a read), so two starts at once make one row. The CLI and the
    /// Viewer only read it. Needs no GRANT: provisioning's blanket grants on the config schema re-run on every
    /// service start and cover a table a migration introduces. Needs no trigger either: nothing reloads when the id
    /// changes, because it only changes at start. Neither value is a secret, so no read carve is needed.</para>
    /// </summary>
    private const string V158Sql = @"
/* V158: the install id (#4961), one row, made by the service at start. Schema-qualified config.* like every rung. */
CREATE TABLE IF NOT EXISTS config.config_install_id (
    id smallint NOT NULL PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    install_id text NOT NULL CONSTRAINT ck_config_install_id_format CHECK (install_id ~ '^[0-9a-f]{8}$'),
    system_identifier bigint,
    database_oid bigint NOT NULL,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);";

    /// <summary>
    /// V159 — <c>config.config_install_id.table_oid</c> and <c>server_major</c>: the OID of the install id table and the
    /// server's major version, two more bindings for the id (#4961).
    ///
    /// <para>V158 bound the id to the cluster's <c>system_identifier</c> and the database's OID, and a different cluster
    /// id made a new id. A managed major upgrade runs <c>pg_upgrade</c>, which makes a new <c>system_identifier</c>, so each
    /// upgrade made a new id and left the old id's sessions on every monitored server. <c>pg_upgrade</c> keeps the
    /// database's OID and the OID of each table (<see cref="StoreInstallId.IsSameStore"/> cites the PostgreSQL sources
    /// for both) and raises the major, and a store made again by dump and restore gets new OIDs, so the two OIDs say
    /// whether this is the same store where the cluster id cannot. OIDs restart at the same number after every
    /// <c>initdb</c>, so a copy of the row into a fresh install of the same version can match both; the major tells that
    /// copy from an upgrade, because an upgrade always raises it. The service keeps the id through a changed cluster id
    /// only when both OIDs match and the major rose, and rebinds the row to the new cluster id and major.</para>
    ///
    /// <para><b>Nullable, DDL only, one statement.</b> A row made by V158's service has neither value. They are not
    /// backfilled here, because both are facts of the cluster the service reads at start, not something a migration
    /// should guess: the service writes them on its first start after this rung, and until then a row without a table OID
    /// is compared by the old rule, which is the rule that made the row. One catalog-only <c>ADD COLUMN</c> for each, in
    /// one statement, so the two arrive together or not at all, and a second run changes nothing. Needs no GRANT (the
    /// roles' table-level grants cover a new column) and no trigger (nothing reloads when the id's binding changes).</para>
    /// </summary>
    private const string V159Sql = @"
ALTER TABLE config.config_install_id
    ADD COLUMN IF NOT EXISTS table_oid bigint,
    ADD COLUMN IF NOT EXISTS server_major integer;";

    /// <summary>
    /// V160 (#4938) — <c>config.config_collector_run_times</c>: an optional run time for a collector that runs once a day
    /// or less often, so a heavy daily collector can run in a quiet hour instead of whenever the service happened to
    /// start.
    ///
    /// <para><b>Its own table, because released viewers rewrite the schedules table.</b> The viewer's schedule Save
    /// deletes a scope's rows in <c>config_collector_schedules</c> and inserts them again with a fixed column list, and
    /// every released viewer connects to a store that is newer than it is (the connect check refuses only an older one).
    /// A run time kept as a column on that table would be set back to NULL by each Save. A table those Saves never write
    /// keeps it. No row means no run time, so a store that never sets one reads exactly as it did before.</para>
    ///
    /// <para>The value is minutes after midnight on the monitored server's own clock, from 0 to 1439. The table mirrors the
    /// schedules table: <c>server_id</c> NULL is fleet-wide, there is one fleet row and one server row per collector (two
    /// partial unique indexes, because a primary key cannot span a nullable column), and the layering is the same (the
    /// server row, else the fleet row, else no fixed time). <b>-1 is allowed on a server row only.</b> It means "no fixed
    /// time on this server" and stops a fleet-wide time, the way an empty array stops a fleet-wide database scope; a fleet
    /// row has no level above it to stop, so the CHECK refuses -1 there. Whether a run time applies at all depends on the
    /// collector's effective interval (a whole number of days), which is not a property of the row, so the service judges
    /// that when it resolves the schedule, not the CHECK.</para>
    ///
    /// <para><b>DDL only, and a second run changes nothing.</b> Every statement is <c>IF NOT EXISTS</c> or a drop and
    /// create, so running the rung again keeps the table, its indexes and its rows. Needs no GRANT: provisioning's blanket
    /// grants on the config schema re-run on every service start and cover a table a migration introduces. It does carry
    /// the one trigger the schedules table has, V17's reload beacon, so a write here bumps
    /// <c>config_service.config_version</c> and a running service reloads the run times. No foreign key to the server
    /// registry, as the schedules table has none: removing a server deletes its definition only and leaves its schedule
    /// rows, so a removed server's run time stays as well, harmlessly. No Lite twin: Lite keeps its schedules in a JSON
    /// file.</para>
    /// </summary>
    private const string V160Sql = @"
/* V160 (#4938): a collector's optional run time, in its own table. Schema-qualified config.* like every rung. */
CREATE TABLE IF NOT EXISTS config.config_collector_run_times (
    server_id integer,
    collector_name text NOT NULL,
    run_at_minute smallint NOT NULL,
    CONSTRAINT ck_config_collector_run_times_range CHECK (run_at_minute BETWEEN 0 AND 1439 OR (run_at_minute = -1 AND server_id IS NOT NULL))
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_config_collector_run_times_fleet
    ON config.config_collector_run_times (collector_name) WHERE server_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_config_collector_run_times_server
    ON config.config_collector_run_times (server_id, collector_name) WHERE server_id IS NOT NULL;

DROP TRIGGER IF EXISTS trg_bump_collector_run_times ON config.config_collector_run_times;
CREATE TRIGGER trg_bump_collector_run_times
    AFTER INSERT OR UPDATE OR DELETE ON config.config_collector_run_times
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();";

    /// <summary>
    /// V161 (#5094) — <c>collect.query_store_top_daily</c> and <c>collect.query_store_top_daily_built</c>: a per-day
    /// summary of <c>collect.query_store_interval_wide</c>, which <c>get_query_store_top</c>'s long windows read for the
    /// whole UTC days inside the window instead of scanning every interval row.
    ///
    /// <para><b>Approximate by design.</b> A day's row is built once after the day ends (and rebuilt once more a day
    /// later), so a wide-table row that changes after its day was summarized is missed, and a row whose collection time
    /// moves into the next day after the first build can be counted in both days for about a day. Everything the table
    /// holds is exactly recombinable for the rows it saw: <c>interval_rows</c>, <c>execution_count_sum</c>, and for each
    /// of the six averaged columns a sum of the non-NULL values and a count of them (<c>_sum</c>, <c>_n</c>), so a
    /// reader reproduces the unweighted mean of the interval averages as <c>SUM(_sum) / NULLIF(SUM(_n), 0)</c>. A reader
    /// that uses it says so in its answer. Rebuilding a day is the builder's job: the service owns every write here, and
    /// no reader or writer uses these tables yet.</para>
    ///
    /// <para><b>Plain tables, not hypertables</b>: one row per server, day and query identity, and the builder deletes
    /// and re-inserts a whole day, which a compressed chunk would not allow. The key's column types are the wide
    /// table's, copied, and the unique index is <c>NULLS NOT DISTINCT</c> because <c>replica_role</c>,
    /// <c>module_name</c> and the others are NULL for most rows. <c>collect.query_store_top_daily_built</c> records
    /// which days are built, at which pass, and how many wide-table rows the build read.</para>
    ///
    /// <para><b>No GRANT</b>: the <c>collect</c> schema's blanket <c>GRANT SELECT ON ALL TABLES</c> to the read-only
    /// roles (re-run on every managed start and by <c>tools/provision-roles.sql</c>) covers a table a migration
    /// introduces. Every statement is <c>IF NOT EXISTS</c>, so a second run changes nothing.</para>
    /// </summary>
    private const string V161Sql = @"
/* V161 (#5094): the daily summary for get_query_store_top's long windows. APPROXIMATE by design: a day is summarized
   after it ends, so a late change to a summarized day is missed. The builder owns every write. Schema-qualified
   collect.* like every rung; plain tables; timestamps are naive UTC. */
CREATE TABLE IF NOT EXISTS collect.query_store_top_daily
(
    server_id integer NOT NULL,
    day date NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    query_hash text,
    execution_type_desc text,
    replica_role text,
    module_name text,
    interval_rows bigint NOT NULL,
    execution_count_sum numeric,
    avg_duration_us_sum numeric,
    avg_duration_us_n bigint NOT NULL,
    avg_cpu_time_us_sum numeric,
    avg_cpu_time_us_n bigint NOT NULL,
    avg_logical_io_reads_sum numeric,
    avg_logical_io_reads_n bigint NOT NULL,
    avg_logical_io_writes_sum numeric,
    avg_logical_io_writes_n bigint NOT NULL,
    avg_physical_io_reads_sum numeric,
    avg_physical_io_reads_n bigint NOT NULL,
    avg_rowcount_sum numeric,
    avg_rowcount_n bigint NOT NULL,
    last_execution_time_max timestamp,
    query_plan_hash_max text,
    first_execution_time_min timestamp
);

/* NULLS NOT DISTINCT: replica_role, module_name and the other key columns are NULL for most rows, and two NULLs must
   be the same group. */
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_top_daily
ON collect.query_store_top_daily
(
    server_id,
    day,
    database_name,
    query_id,
    plan_id,
    query_hash,
    execution_type_desc,
    replica_role,
    module_name
)
NULLS NOT DISTINCT;

/* Which days are built, at which pass (1 = early, 2 = final), when, and how many wide-table rows the build read. */
CREATE TABLE IF NOT EXISTS collect.query_store_top_daily_built
(
    server_id integer NOT NULL,
    day date NOT NULL,
    pass smallint NOT NULL,
    built_at timestamp NOT NULL,
    source_rows bigint NOT NULL,
    CONSTRAINT pk_query_store_top_daily_built PRIMARY KEY (server_id, day)
);";

    /// <summary>
    /// V162 (#5097) — <c>collect.slow_reads</c>: one row per read that ran long or ended in a timeout, error or
    /// limit, with its normalised arguments, the source that answered, and each statement's time. Internal
    /// self-telemetry like <c>collect.read_latency</c> (V148) and <c>collect.collector_stall_probes</c> (V112): not a
    /// collector, absent from <c>CollectorCatalog.All</c>, covered by the collect schema's blanket GRANT. The writer
    /// pays for retention and the row cap itself, so no outcome CHECK and no sweep entry.
    /// </summary>
    private const string V162Sql = @"
CREATE TABLE IF NOT EXISTS collect.slow_reads
(
    slow_read_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    read_time timestamp NOT NULL,
    surface text NOT NULL,
    route text NOT NULL,
    outcome text NOT NULL,
    total_ms integer NOT NULL,
    server_id integer,
    window_start timestamp,
    window_end timestamp,
    arguments jsonb NOT NULL,
    arguments_truncated boolean NOT NULL,
    source text,
    source_reason text,
    statements jsonb NOT NULL,
    statement_count integer NOT NULL,
    statements_truncated boolean NOT NULL,
    error_class text,
    row_count bigint
);

/* Newest-first read and the retention sweep are both by time; the id breaks ties in the read's total order. */
CREATE INDEX IF NOT EXISTS idx_slow_reads_time
    ON collect.slow_reads(read_time, slow_read_id);";

    /// <summary>
    /// V163 (#5097) — the store's own statement history: an hourly, delta-per-statement record of the store's
    /// <c>pg_stat_statements</c>, so a statement's mean milliseconds per call can be read hour by hour across a store
    /// restart, a service restart, a <c>pg_stat_statements_reset()</c> and the nightly upgrade.
    ///
    /// <para><b>Not a collector.</b> The store has no <c>servers</c> row and no <c>server_id</c>, and self-telemetry is
    /// kept out of <c>CollectorCatalog.All</c> on purpose (V111's reasoning), which also keeps the hypertable
    /// conversion and the catalog purge off these tables. The three tables are plain, like V111's and V161's. The
    /// hourly store self-metrics tick writes them, and the writer pays for its own retention: 90 days, because
    /// statement ids change with a major upgrade and a year of hourly rows would read as noise.</para>
    ///
    /// <para><b>Why a baseline table.</b> The per-target statement collector keeps its previous snapshot in process
    /// memory and re-seeds it from stored rows after a restart. A top-N history cannot seed that way: a statement
    /// left out of last hour's top N has no stored previous row. So the previous cumulative counters live in the store,
    /// in <c>config.store_statement_baseline</c>, and the delta is computed in SQL. It is state and not collected data,
    /// so it sits in <c>config</c> where no purge ages it out, the same reasoning as <c>config.store_log_read_marker</c>.
    /// Provisioning re-grants every <c>config</c> table after migration, so it needs no GRANT here.</para>
    ///
    /// <para><b>No statement text.</b> The history stores the role and the query id only. A reader joins the live text
    /// at read time, so a stored row can never hold a statement the reader function would withhold. There are no WAL or
    /// temp-file columns beyond temp blocks written: the store's own statements do not move them.</para>
    ///
    /// <para><b>Reading the columns.</b> <c>max_exec_ms</c> is the longest single execution since the entry started
    /// in the extension, not within the interval: it is cumulative and cannot be differenced. <c>first_seen</c> can be
    /// an upper bound (an entry already there but missing from the baseline, such as a query id that became visible
    /// after a grant or a renamed role, is credited its whole lifetime as one interval). An entry that was evicted and
    /// re-added, and has already passed its old baseline, undercounts with no flag; <c>dealloc_delta</c> on the capture
    /// is the hint. <c>statements_seen</c> is 0 on a <c>rebaselined</c> capture.</para>
    ///
    /// <para><b>No Lite twin.</b> Lite's store is DuckDB, which has no <c>pg_stat_statements</c>; the same structural
    /// reason as V111.</para>
    /// </summary>
    private const string V163Sql = @"
CREATE TABLE IF NOT EXISTS collect.store_statement_captures
(
    capture_time timestamp NOT NULL,
    interval_seconds integer,
    stats_reset timestamp,
    dealloc bigint,
    dealloc_delta bigint,
    statements_seen integer NOT NULL,
    statements_kept integer NOT NULL,
    hidden_statements integer NOT NULL,
    outcome text NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_statement_captures_time
    ON collect.store_statement_captures(capture_time);

CREATE TABLE IF NOT EXISTS collect.store_statement_history
(
    capture_time timestamp NOT NULL,
    interval_seconds integer NOT NULL,
    role_name text NOT NULL,
    queryid bigint NOT NULL,
    delta_calls bigint NOT NULL,
    delta_total_exec_ms double precision NOT NULL,
    delta_rows bigint NOT NULL,
    delta_shared_blks_hit bigint NOT NULL,
    delta_shared_blks_read bigint NOT NULL,
    delta_temp_blks_written bigint NOT NULL,
    max_exec_ms double precision,
    first_seen boolean NOT NULL,
    entry_restarted boolean NOT NULL,
    reset_in_interval boolean NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_statement_history_time
    ON collect.store_statement_history(capture_time);

CREATE INDEX IF NOT EXISTS idx_store_statement_history_query
    ON collect.store_statement_history(queryid, capture_time);

CREATE TABLE IF NOT EXISTS config.store_statement_baseline
(
    role_name text NOT NULL,
    queryid bigint NOT NULL,
    calls bigint NOT NULL,
    total_exec_ms double precision NOT NULL,
    rows_returned bigint NOT NULL,
    shared_blks_hit bigint NOT NULL,
    shared_blks_read bigint NOT NULL,
    temp_blks_written bigint NOT NULL,
    captured_at timestamp NOT NULL,
    CONSTRAINT pk_store_statement_baseline PRIMARY KEY (role_name, queryid)
);";

    /// <summary>
    /// V164 (#4605) — the hourly row-count ledger of <c>collect.query_stats</c> and its one-row state table. The
    /// count guard of the hourly-edges read compared raw row counts with the rollup's per (server, hour), a scan of
    /// millions of rows that outran the panel deadline. The ledger is that count kept at write time, so the guard
    /// reads a few thousand rows. The text, the counted population and the reasoning are on
    /// <see cref="QueryStatsHourLedger"/>, which the rung embeds.
    ///
    /// <para><b>Both tables are empty when created</b> (the data-moving census therefore has nothing to declare), and
    /// the rung inserts only the state row: <c>counted_since</c>, the next whole hour, below which every window is
    /// uncounted and a reader stays on raw. The tables are plain and not collectors, like V111's and V162's.</para>
    ///
    /// <para><b>No GRANT and no provisioning change.</b> The <c>collect</c> schema carries blanket SELECT for
    /// admin/viewer/mcp plus the owner-scoped <c>ALTER DEFAULT PRIVILEGES</c>, both re-asserted every managed start
    /// (<c>DarlingManagedRoles</c>) and re-run in BYO mode by <c>tools/provision-roles.sql</c>, so the compose roles that
    /// read these tables need no new statement, and the collector writes as the service owner.</para>
    ///
    /// <para><b>No Lite twin.</b> Lite's store is DuckDB, which has no continuous aggregates, so no rollup exists there
    /// for a count guard to check.</para>
    /// </summary>
    private const string V164Sql = QueryStatsHourLedger.CreateSql;

    /// <summary>
    /// V165 (#5366) — the tables for the service's published password key: <c>config.password_key</c> (the public key,
    /// one <c>current</c> row at most), <c>config.password_key_service</c> (what each service host holds and its state),
    /// <c>config.legacy_secret_pin</c> and the one-row <c>config.legacy_secret_pin_marker</c>, seeded <c>pending</c>. The
    /// text and the reasoning are on <see cref="PasswordKeyTables"/>, which the rung embeds.
    ///
    /// <para><b>Only the store owner writes them,</b> by a trigger created here on each table (a row trigger and a
    /// TRUNCATE trigger, both enabled always), which refuses a write from any session that is not the owner. The
    /// provisioning scripts also revoke every privilege on the four tables from PUBLIC, admin, viewer and mcp (so none
    /// of them can add a trigger or a foreign key) and drop any trigger or foreign key already there. The two pin
    /// tables have row security on with no policy.</para>
    ///
    /// <para><b>No Lite twin.</b> Lite keeps its secrets in its own local store.</para>
    /// </summary>
    private const string V165Sql = PasswordKeyTables.CreateSql;

    /// <summary>
    /// V166 — the PagerDuty auto-resolve opt-in. Adds <c>pagerduty_auto_resolve</c> to
    /// <c>config.config_notification</c>: non-null with a FALSE default, so every existing row keeps the
    /// shipped behaviour of NOT auto-resolving incidents with PagerDuty (the closing edge of an edge-type
    /// pair stays an info-severity trigger; with the toggle on it becomes a <c>resolve</c> — see
    /// <c>WebhookAlertService.BuildPagerDutyPayload</c>, which reads the column through
    /// <c>IAlertSettings.PagerDutyAutoResolve</c>). Non-secret (a behaviour toggle, not a credential), so
    /// unlike the routing key beside it, it stays in the read-only roles' SELECT grants —
    /// <c>DarlingManagedRoles.ViewerRestrictedConfigTables</c> and <c>Darling/tools/provision-roles.sql</c>.
    ///
    /// <para><b>Numbered 166, the ladder's next free version</b> (the top was 165): the migration ladder is
    /// applied and stamped in version order, so ANY free number >= 166 would apply on every store — but the
    /// convention is dense, consecutive versions, and a gap would trip the ladder-density census.</para>
    /// </summary>
    private const string V166Sql = @"
ALTER TABLE config.config_notification
    ADD COLUMN IF NOT EXISTS pagerduty_auto_resolve boolean NOT NULL DEFAULT FALSE;";

    /// <summary>
    /// V167 (#5456) - the record of which old-format saved passwords were in the store at the upgrade:
    /// <c>config.legacy_secret_pin_candidate</c>, with the hash of each stored value and the connection columns it is
    /// saved for. The text and the reasoning are on <see cref="LegacyPinCandidateTables"/>, which the rung embeds. The
    /// table and its two owner-only triggers are made first and the values are recorded after them in the same
    /// transaction, and only while the one-time pin step is still open (a store whose step is done records nothing). V165
    /// is not changed: a store already at 165 does not run it again, which is why this is a new rung.
    ///
    /// <para><b>No Lite twin.</b> Lite keeps its secrets in its own local store.</para>
    /// </summary>
    private const string V167Sql = LegacyPinCandidateTables.CreateSql + "\n\n" + LegacyPinCandidateTables.CaptureSql;

    /// <summary>
    /// V168 (#5448) — <c>collect.plan_regression_daily</c> and <c>collect.plan_regression_daily_built</c>: the per-day
    /// per-plan totals PLAN_REGRESSION reads for CLOSED days instead of re-aggregating 14 days of
    /// <c>collect.query_store_interval_latest</c> on every run, plus the trigger that marks a day stale when a late row
    /// lands in it.
    ///
    /// <para><b>Totals, not averages.</b> A day's row holds the plan's execution count and the SUMS of
    /// <c>avg_cpu_time_us * execution_count</c> and <c>avg_duration_us * execution_count</c>, which is what the read's
    /// <c>plan_agg</c> step computes before it divides, so summing days and then dividing returns the same per-exec cost
    /// as the single 14-day aggregate. Row day is the day of <c>last_execution_time</c> (naive UTC), the column the
    /// read's window filters on. <c>query_plan_hash</c> is part of the unique key (the read's <c>any_value</c> picks one
    /// hash per plan, and a day's rows keep each one), and the index is <c>NULLS NOT DISTINCT</c> because
    /// <c>database_name</c>, <c>replica_role</c> and the others are NULL for some rows.</para>
    ///
    /// <para><b>The built table is the validity record.</b> <c>late_seq</c> counts the transactions that landed a late
    /// row on a day (the trigger bumps a (server, day) once per transaction, not once per row, because a whole apply
    /// is one transaction and tens of thousands of bumps of the same tuple inside it cost quadratic time), and
    /// <c>built_seq</c> is the value the builder read BEFORE it aggregated: a day is valid only while the two
    /// are equal, so a late row that lands after the build (even one that races it) leaves the day invalid and the
    /// builder rebuilds it. The trigger marks the day of <c>first_execution_time</c> and the day after it, because
    /// <c>last_execution_time</c> (the row's day) can cross midnight. It fires only for rows whose first execution is
    /// at least a day old and no earlier than the start of the day 17 days back (a whole-day edge, so every day the
    /// hourly cleanup still keeps can be marked): today and yesterday are always read live, and a row older than that
    /// can never be read. The <c>WHEN</c> condition reads <c>NEW</c> only, because PostgreSQL does
    /// not allow a subquery there.</para>
    ///
    /// <para><b>Plain tables, SECURITY INVOKER function, no GRANT</b>: the builder deletes and re-inserts whole days,
    /// which a compressed hypertable chunk would not allow; the trigger function runs with the writer's own
    /// privileges and pins <c>search_path</c> to <c>pg_catalog, pg_temp</c>, so it names every object schema-qualified;
    /// and the <c>collect</c> schema's blanket <c>GRANT SELECT ON ALL TABLES</c> covers a table a migration introduces.
    /// Every statement is idempotent, so a second run changes nothing. No index on
    /// <c>query_store_interval_latest</c> is added here. V153's index on <c>first_execution_time</c> alone serves the
    /// retention purge, which has no <c>server_id</c>; it does not serve the per-day builds, which bound
    /// <c>server_id</c> and <c>first_execution_time</c>. That read's index, a btree on
    /// <c>(server_id, first_execution_time)</c>, is built in the background (#5507,
    /// <c>QueryStoreBackgroundIndexes.LatestServerFirstExec</c>), not by a rung, because a rung's build would run inside
    /// the startup transaction and could pass the migration silence limit on a large store.</para>
    /// </summary>
    private const string V168Sql = @"
/* V168 (#5448): per-day per-plan totals for PLAN_REGRESSION's closed days, and the trigger that marks a day stale when
   a late row lands in it. Naive UTC throughout. The builder (the service) owns every write to the first table; the
   function writes only plan_regression_daily_built, the rung's own bookkeeping table. */
CREATE TABLE IF NOT EXISTS collect.plan_regression_daily
(
    server_id integer NOT NULL,
    day date NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    replica_role text,
    query_plan_hash text,
    execs numeric,
    cpu_us_sum numeric,
    dur_us_sum numeric,
    last_exec timestamp,
    is_forced_plan boolean,
    force_failure_count bigint
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_plan_regression_daily
ON collect.plan_regression_daily (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash)
NULLS NOT DISTINCT;

CREATE TABLE IF NOT EXISTS collect.plan_regression_daily_built
(
    server_id integer NOT NULL,
    day date NOT NULL,
    late_seq bigint NOT NULL DEFAULT 0,
    built_seq bigint,
    built_at timestamp,
    source_rows bigint,
    CONSTRAINT pk_plan_regression_daily_built PRIMARY KEY (server_id, day)
);

CREATE OR REPLACE FUNCTION collect.plan_regression_daily_mark_late() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, pg_temp
AS $f$
/* A (server, day) is bumped at most once per transaction. The apply is one statement, so tens of
   thousands of late rows would otherwise run ON CONFLICT DO UPDATE on the same one or two built rows, and each update
   leaves another version of that tuple inside the one transaction: every conflict check after it walks the whole chain
   (166 buffers at 30,000 versions against 4 on a fresh row), and the batch grows quadratically. One bump is enough:
   the builder reads late_seq before it aggregates, and the batch's rows commit together with the bump. The pairs
   already bumped are kept in a transaction-local setting (set_config(.., true)), which a rolled-back savepoint undoes
   together with the bump it recorded, and which the function's own SET search_path does not touch. Each key is
   server_id:day-number between commas, so one lookup is a substring test; the 17-day clamp keeps the list to at most
   17 pairs per server. After a commit the setting reads back as an empty string, not NULL. */
DECLARE
    d integer := NEW.first_execution_time::date - DATE '2000-01-01';
    marked text := coalesce(nullif(current_setting('darling.plan_regression_marked', true), ''), ',');
    k0 text := ',' || NEW.server_id || ':' || d || ',';
    k1 text := ',' || NEW.server_id || ':' || (d + 1) || ',';
    new0 boolean := position(k0 in marked) = 0;
    new1 boolean := position(k1 in marked) = 0;
BEGIN
    IF NOT (new0 OR new1) THEN
        RETURN NULL;
    END IF;

    INSERT INTO collect.plan_regression_daily_built AS b (server_id, day, late_seq)
    SELECT NEW.server_id, v.d, 1
    FROM (VALUES (NEW.first_execution_time::date, new0), (NEW.first_execution_time::date + 1, new1)) AS v (d, fresh)
    WHERE v.fresh
    ON CONFLICT (server_id, day) DO UPDATE SET late_seq = b.late_seq + 1;

    PERFORM set_config('darling.plan_regression_marked',
        marked || CASE WHEN new0 THEN substr(k0, 2) ELSE '' END || CASE WHEN new1 THEN substr(k1, 2) ELSE '' END, true);
    RETURN NULL;
END
$f$;

DROP TRIGGER IF EXISTS trg_plan_regression_daily_late ON collect.query_store_interval_latest;
CREATE TRIGGER trg_plan_regression_daily_late
    AFTER INSERT OR UPDATE ON collect.query_store_interval_latest
    FOR EACH ROW
    WHEN (NEW.first_execution_time < date_trunc('day', now() AT TIME ZONE 'UTC') - interval '1 day'
          AND NEW.first_execution_time >= date_trunc('day', now() AT TIME ZONE 'UTC') - interval '17 days')
    EXECUTE FUNCTION collect.plan_regression_daily_mark_late();";

    /// <summary>
    /// V169 — the per-server AWS role (#5452). Adds to <c>config.config_monitored_servers</c> the IAM role Darling
    /// assumes to reach an Amazon RDS or Aurora target (<c>aws_role_arn</c>) and the optional external ID that
    /// goes with it (<c>aws_external_id</c>), plus <c>aws_external_id_set</c>, a stored generated flag the
    /// read-only roles can SELECT in place of the ID itself. Every existing row gets NULL for all three, which is
    /// "use the process's own credentials", exactly as before.
    ///
    /// <para><b>The constraint is a coarse backstop.</b> It refuses an external ID with no role (no term of the
    /// CHECK can be NULL, so a NULL role with a non-NULL ID is false, not unknown), a role that is not an IAM role
    /// ARN, and an external ID outside AWS's character set and length. The lengths are separate <c>char_length</c>
    /// terms because PostgreSQL's regular expressions refuse a repeat count above 255. The exact rules, with their messages, live
    /// in <c>AwsRoleSettings</c>; this only stops a blind write from storing a value no surface would accept.
    /// Added behind a <c>pg_constraint</c> guard, as V62 does, so a re-run is a no-op.</para>
    ///
    /// <para><b>Idempotent and non-data-moving:</b> <c>IF NOT EXISTS</c> columns and the guarded constraint, no
    /// rows rewritten by the script itself (the stored generated column fills from the table's own rows).
    /// The columns are classified in <c>DarlingManagedRoles.ViewerRestrictedConfigTables</c> (the role and the
    /// flag readable, the external ID not) and in <c>Darling/tools/provision-roles.sql</c>.</para>
    ///
    /// <para><b>No Lite twin.</b> Lite makes no AWS call.</para>
    ///
    /// <para><b>Numbered 169, the ladder's next free version</b> (the top was 168); the convention is dense,
    /// consecutive versions.</para>
    /// </summary>
    private const string V169Sql = @"
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS aws_role_arn text;
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS aws_external_id text;
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS aws_external_id_set boolean
    GENERATED ALWAYS AS (aws_external_id IS NOT NULL) STORED;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'config_monitored_servers_aws_role_check'
                     AND conrelid = 'config.config_monitored_servers'::regclass) THEN
        ALTER TABLE config.config_monitored_servers
            ADD CONSTRAINT config_monitored_servers_aws_role_check CHECK (
                (aws_external_id IS NULL OR aws_role_arn IS NOT NULL)
                AND (aws_role_arn IS NULL OR (aws_role_arn ~ '^arn:aws(-[a-z]+)*:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+$'
                                              AND char_length(aws_role_arn) <= 2048))
                AND (aws_external_id IS NULL OR (aws_external_id ~ '^[A-Za-z0-9_+=,.@:/-]+$'
                                                 AND char_length(aws_external_id) BETWEEN 2 AND 1224)));
    END IF;
END $$;";

    /// <summary>
    /// V170 (#5495) — <c>collect.pg_io_stats_hourly</c> and <c>collect.pg_io_stats_hourly_state</c>: the hourly rollup of the
    /// differenced PostgreSQL I/O counters that <c>get_pg_io_stats</c> and the viewer's I/O tab read for long windows. The text and
    /// the reasoning are on <see cref="PgIoStatsHourly"/>, which the rung embeds. <b>Both tables are empty when created</b> (the
    /// data-moving census has nothing to declare): the service's hourly tick fills them (<see cref="PgIoStatsHourlyBuilder"/>), for at
    /// most <see cref="PgIoStatsHourlyBuilder.TickBudget"/> per tick, and a read stays on raw rows until its window is covered.
    /// Plain tables, no GRANT (the <c>collect</c> schema's blanket SELECT covers them). <b>No Lite twin:</b> Lite has no PostgreSQL targets.
    /// </summary>
    private const string V170Sql = PgIoStatsHourly.CreateSql;

    /// <summary>
    /// V171 (#5571) — <c>collect.query_store_interval_wide</c> becomes a table partitioned by day on
    /// <c>first_execution_time</c>, and the table that holds every row today is ATTACHED to it, whole, as
    /// <c>query_store_interval_wide_legacy</c>. <b>No row is copied, moved or scanned.</b> V172 does the same for
    /// <c>query_store_interval_latest</c>; the text is shared (<see cref="BuildQueryStoreIntervalPartitionRungSql"/>).
    ///
    /// <para><b>Why this is catalog-only (Erik's rules: no data copy, attach the current table, keep every startup
    /// statement under <see cref="MigrationCommandTimeoutSeconds"/>).</b> Rename the table and its indexes, create an empty
    /// partitioned parent with the same columns, and attach the renamed table with the bounds
    /// <c>(MINVALUE) TO (MAXVALUE)</c>. That partition constraint is only <c>first_execution_time IS NOT NULL</c>, which the
    /// column's <c>NOT NULL</c> already proves, so <c>ATTACH PARTITION</c> skips its validation scan; the live test reads
    /// PostgreSQL's DEBUG1 line ("is implied by existing constraints") and the table's <c>seq_scan</c> counter to prove it.
    /// The parent's indexes are made with <c>CREATE INDEX ... ON ONLY</c> and then <c>ALTER INDEX ... ATTACH PARTITION</c>,
    /// never as a plain <c>CREATE INDEX</c> on the parent: a plain one BUILDS a child when no valid equivalent exists, and a
    /// 91 GB build would blow the startup timeout, while <c>ATTACH</c> fails at once. An INVALID leftover (the #5507 shape) is
    /// therefore dropped here and the background step rebuilds it; a missing one is left for the background step.</para>
    ///
    /// <para><b>The one wait.</b> The rename takes ACCESS EXCLUSIVE on the table, so a long reader (the viewer, an MCP
    /// call) delays it; <c>lock_timeout</c> is <see cref="MigrationCommandTimeoutSeconds"/> less 20 s, V153's precedent, so
    /// the rung fails with a lock error and the next start retries, instead of the command timing out mid-transaction.
    /// No collector writes during a rung: migrations finish before collectors and the background work start.</para>
    ///
    /// <para><b>Guard, idempotence, order.</b> A table that is already partitioned (relkind <c>p</c>) is skipped, so a
    /// re-run is a no-op. The parent is built <c>LIKE</c> the live table, so the column order is the live attnum order
    /// that the positional writers use (V155's note); there is no <c>WITH</c> clause because PostgreSQL refuses storage
    /// parameters on a partitioned table, and the legacy table keeps its fillfactor 50. The rung writes no GRANT: the new
    /// parent gets the schema's default privileges (DarlingManagedRoles section 4), which cover a table the owner creates,
    /// and a live test reads the parent as the viewer role.</para>
    ///
    /// <para>The names are fixed: <c>X_legacy</c>, and each index's own name plus <c>_legacy</c>. The partitions that
    /// take over from the legacy table (daily partitions, the default partition, the NOT VALID CHECK on the legacy table)
    /// are made by the background step, not here. <b>No Lite twin:</b> Lite has no reference to either table.</para>
    /// </summary>
    private static readonly string V171Sql = BuildQueryStoreIntervalPartitionRungSql(wide: true);

    /// <summary>
    /// V172 (#5571) — the same rung for <c>collect.query_store_interval_latest</c>, in its own migration so it gets its own
    /// <see cref="MigrationCommandTimeoutSeconds"/> window. See <see cref="V171Sql"/>. This table also carries the
    /// <c>trg_plan_regression_daily_late</c> row trigger (V168): the rung drops the legacy table's copy and creates it on the
    /// parent, which clones it onto the legacy table (a row trigger on a partitioned table fires for rows written through it).
    /// </summary>
    private static readonly string V172Sql = BuildQueryStoreIntervalPartitionRungSql(wide: false);

    /// <summary>
    /// V173 (#5582) — the exact stamp-grain rollup of <c>collect.query_store_interval_wide</c>: <c>collect.query_store_compose_stamp</c>,
    /// its per-(server, hour) validity table <c>_built</c>, the fleet-level <c>_hours</c> table, and two row triggers on the partitioned
    /// PARENT <c>collect.query_store_interval_wide</c> (INSERT, UPDATE) that mark a (server, hour) stale when a row lands in, or leaves,
    /// an hour the builder may have done. The text, the column map and the reasoning are on <see cref="QueryStoreComposeStamp"/>, which
    /// the rung embeds. <b>Catalog-only:</b> the tables are created empty, and <c>CREATE TRIGGER</c> on a partitioned table is a catalog
    /// change that clones the trigger onto each existing leaf (the legacy table, DEFAULT, the day partitions) without scanning one. The
    /// lock timeout is <see cref="MigrationCommandTimeoutSeconds"/> less 20 s, as V171's, because the trigger needs a SHARE ROW EXCLUSIVE
    /// lock on the parent and each leaf; no collector writes during a rung. Plain tables, no GRANT (the <c>collect</c> schema's blanket
    /// SELECT covers them). The service's hourly tick fills the rollup (<see cref="QueryStoreComposeStamp.RunTickAsync(Npgsql.NpgsqlDataSource, DateTime, int, Microsoft.Extensions.Logging.ILogger, CancellationToken)"/>).
    /// <b>No Lite twin:</b> Lite has no PostgreSQL store.
    /// </summary>
    private static readonly string V173Sql = "SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + "s';\n" + QueryStoreComposeStamp.CreateSql;

    /// <summary>
    /// V174 (#5655) — <c>config.service_version</c>: one row per service host with the product version the service reported at
    /// start, its process start time and the time the row was written. The Manage Servers window reads it to show the SERVICE's
    /// version once, in place of the viewer's own version on every server row. A separate table keyed by the service host, as
    /// <c>config.password_key_service</c> is, and not a column of <c>config.config_service</c>, whose self-bump trigger would make
    /// every service reload on each start. The text is on <see cref="ServiceVersionTable"/>, which the rung embeds. Catalog-only:
    /// the table is created empty, and no GRANT is needed (the <c>config</c> schema's blanket SELECT and default privileges cover
    /// it). <b>No Lite twin:</b> Lite has no PostgreSQL store, and there the app is the collector.
    /// </summary>
    private static readonly string V174Sql = ServiceVersionTable.CreateSql;

    /// <summary>
    /// V179 (#5640) - everything the Client Timeouts alert needs in the store, in one rung. (1) <c>collect.attention_events</c>: the
    /// <c>AttentionCollector</c> table, a <c>count</c> row for each run whose event_counter delta is above zero (event_time NULL; a quiet run writes none) plus one <c>sample</c> row per kept ring-buffer
    /// event (attention_count 0). A collector table that stays a PLAIN table on every store (<c>ICollectorSchemaInfo.StaysPlainTable</c>): no hypertable, no compression and no
    /// Timescale retention policy, because the 73rd hypertable is a grid decision (#3035, #3044, #3107) and its rows are low volume. Its 30-day
    /// retention is the time-sliced DELETE the retention sweep runs on both store kinds. A fresh store already has it from V1's catalog walk, so
    /// the CREATE is a no-op there and the real create on upgrade.
    /// Columns are exactly <c>PgSchemaGenerator.CreateTable</c>'s output for the catalog entry. (2)
    /// <c>config.config_mute_rules.client_app_pattern</c>: the client-application match for mute rules (nullable text, no default,
    /// like V157's <c>server_id</c>). (3) Three alert settings on <c>config.config_alert_settings</c>, named like V177's:
    /// <c>client_timeouts_enabled</c> (<c>boolean NOT NULL DEFAULT FALSE</c>: the alert ships OFF until the bars are checked against a
    /// week of capture on the field stores), <c>client_timeouts_warning_per_minute</c> (default 10) and
    /// <c>client_timeouts_critical_per_minute</c> (default 60), attentions per minute averaged over three minutes. The defaults are
    /// a guess from "a healthy app rarely times out", not a measured normal rate. Written as literals because a migration's text is
    /// frozen at the version it ships in. No CHECK on the bounds, no reload beacon of its own (V17's trigger bumps the version on any
    /// write to the settings row), no GRANT (the <c>collect</c> and <c>config</c> schemas carry blanket SELECT and table-level
    /// grants). Catalog-only: every added column is nullable or has a constant default. Every statement is idempotent. <b>Lite twin:</b>
    /// the DuckDB bump to schema 68. Sequenced after V176-V178 (#5638, #5641, #5684), which are in other open changes.
    /// </summary>
    private const string V179Sql = @"
CREATE TABLE IF NOT EXISTS collect.attention_events (
    attention_event_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    event_time timestamp,
    event_kind text,
    attention_count bigint,
    database_name text,
    client_app_name text,
    client_hostname text,
    session_id integer,
    request_id integer,
    interrupt_duration_us bigint,
    statement_fingerprint text,
    statement_text text
);

CREATE INDEX IF NOT EXISTS idx_attention_events_time ON collect.attention_events(server_id, collection_time);

ALTER TABLE config.config_mute_rules ADD COLUMN IF NOT EXISTS client_app_pattern text;

ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS client_timeouts_enabled boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS client_timeouts_warning_per_minute integer NOT NULL DEFAULT 10,
    ADD COLUMN IF NOT EXISTS client_timeouts_critical_per_minute integer NOT NULL DEFAULT 60;";

    /// <summary>
    /// V175 (#5631) — <c>collect.plan_regression_daily.rowcount_sum</c>: the day's sum of <c>avg_rowcount * execution_count</c> for
    /// the plan, so PLAN_REGRESSION can tell a plan whose cost followed a much bigger output from a real regression on the daily
    /// path, as it can on the raw and interval-table paths. NULL when any interval of the day had no <c>avg_rowcount</c>, so a
    /// missing count never reads as a small one. The same step bumps <c>late_seq</c> on every built day: each day then reads as
    /// stale, so reads take it from the interval table (the same answer as the raw read) until the hourly builder rebuilds it with
    /// the column filled. Both happen only when the column is added, so a second run of the text changes nothing. Catalog-only: a
    /// nullable column without a default does not rewrite the table, and the built table is one row per server and day. The lock
    /// timeout is <see cref="MigrationCommandTimeoutSeconds"/> less 20 s, as V173's, because the new column needs an ACCESS EXCLUSIVE
    /// lock on the table. The same migration adds <c>collect.query_store_interval_latest.avg_rowcount</c> (<c>bigint</c>, as
    /// <c>query_store_stats.avg_rowcount</c>) in its own idempotent step: the interval table never had it, and the new rows-per-call
    /// reads need it. No backfill: interval rows written before the upgrade keep a NULL, so rows per call is NULL for them and the
    /// output-size rule skips them (a missing count never reads as a small one). On a day-partitioned store the column is added to the
    /// parent and the partitions inherit it. No GRANT: the <c>collect</c> schema's blanket SELECT covers a column. <b>No Lite twin:</b> Lite has no
    /// PostgreSQL store.
    /// </summary>
    private static readonly string V175Sql = "SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + @"s';
DO $$
BEGIN
    IF NOT EXISTS
    (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'collect'
        AND   table_name = 'plan_regression_daily'
        AND   column_name = 'rowcount_sum'
    )
    THEN
        ALTER TABLE collect.plan_regression_daily ADD COLUMN rowcount_sum numeric;
        UPDATE collect.plan_regression_daily_built SET late_seq = late_seq + 1;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS
    (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'collect'
        AND   table_name = 'query_store_interval_latest'
        AND   column_name = 'avg_rowcount'
    )
    THEN
        ALTER TABLE collect.query_store_interval_latest ADD COLUMN avg_rowcount bigint;
    END IF;
END
$$;";

    /// <summary>
    /// V176 (#5638) - <c>collect.procedure_stats_totals</c>: the per-collection totals of <c>collect.procedure_stats</c> that the raw
    /// procedure duration trend reads instead of every row of every collection (about 560 to 1,060 rows per collection since #5463).
    /// The collector writes it on the COPY's own transaction (<c>ProcedureStatsTotalsWriter</c>); the grain, the measures, the
    /// NULL database handling and the retention are on <see cref="ProcedureStatsTotals"/>, which the rung embeds.
    ///
    /// <para><b>Empty when created, no backfill</b> (the data-moving census therefore has nothing to declare): a collection written
    /// before the rung has no totals row and the read keeps reading its rows. A plain table, not a collector, so no catalog, hypertable or
    /// grant change; the <c>collect</c> schema's blanket SELECT and owner default privileges cover it. No Lite twin: Lite's store is
    /// DuckDB with its own procedure trend.</para>
    /// </summary>
    private const string V176Sql = ProcedureStatsTotals.CreateSql;

    /// <summary>
    /// V177 (#5641) - the Runnable Queue alert's three settings on <c>config.config_alert_settings</c>:
    /// <c>runnable_queue_enabled</c> (<c>boolean NOT NULL DEFAULT TRUE</c>, like <c>poison_wait_enabled</c>) and the two bars,
    /// <c>runnable_queue_warning_per_scheduler</c> (default 8) and <c>runnable_queue_critical_per_scheduler</c> (default 16),
    /// runnable tasks per visible online scheduler, averaged over the collections of the last 5 minutes when there are at least 3, otherwise over
    /// the newest 3 if all fall in the last 10 minutes (the rule as shipped; the migration's SQL text says nothing of the window). The defaults are
    /// <c>RunnableQueueEvaluator.DefaultWarningPerScheduler</c> / <c>DefaultCriticalPerScheduler</c>, written as literals here
    /// because a migration's text is frozen at the version it ships in; the defaults pin in the rung's test holds them equal.
    ///
    /// <para>No CHECK enforcing the bounds, matching V119/V120/V122/V124/V126: the floor (Warning at least 1, Critical never
    /// below Warning) is the <c>update_alert_settings</c> write bound and <c>DarlingAlertSettings</c>' read-side clamp, the
    /// raw-in/clamped-out split every knob on this table uses. Catalog-only: a column with a constant default does not rewrite
    /// the one-row table. No reload beacon of its own: V17's statement-level <c>trg_bump_alert_settings</c> already bumps
    /// <c>config_service.config_version</c> on any write here. No GRANT: this table carries table-level grants with no column
    /// carve. <b>No Lite twin:</b> Lite keeps these as settings-file keys. Sequenced after V176 (#5638, procedure_stats_totals).
    /// </summary>
    private const string V177Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS runnable_queue_enabled boolean NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS runnable_queue_warning_per_scheduler integer NOT NULL DEFAULT 8,
    ADD COLUMN IF NOT EXISTS runnable_queue_critical_per_scheduler integer NOT NULL DEFAULT 16;";

    /// <summary>
    /// V178 (#5684) - <c>collect.query_store_watermarks</c> and <c>collect.query_store_watermarks_coverage</c>: one row per
    /// (server, database, collection_time) with that key's <c>MAX(last_execution_time)</c>, so the Query Store per-database watermark
    /// seed read touches about thirty-six rows instead of every <c>query_store_stats</c> row of the pair's last three hours. The
    /// collector writes it on the COPY's own transaction behind a savepoint (<c>QueryStoreWatermarks.TryUpsertBatchAsync</c>); the grain,
    /// the types, the only-up upsert, the coverage start and the two-day retention are on <see cref="QueryStoreWatermarks"/>, which the
    /// rung embeds.
    ///
    /// <para><b>Empty when created, no backfill</b> (the data-moving census therefore has nothing to declare), except the one coverage row
    /// the rung inserts: the store's own clock at the moment the rung ran, kept on a re-run. A reader treats everything before it as raw's.
    /// A plain table, not a collector, so no catalog, hypertable or grant change. No Lite twin: Lite's store is DuckDB with its own
    /// watermark read.</para>
    /// </summary>
    private const string V178Sql = QueryStoreWatermarks.CreateSql;

    /// <summary>
    /// V180 (#5654) — the tables for alerts that another service raises through Darling's channels, routes and mute rules:
    /// <c>config.external_alert_source</c> (the registry that maps a login role to a source name), <c>config.external_alert_pending</c>
    /// (the queue the provisioned function appends to and the service drains) and <c>config.external_alert_state</c> (which keys are
    /// firing), plus <c>config.config_mute_rules.external_source</c>, so a mute rule can name a source. The text is on
    /// <see cref="ExternalAlertTables"/>, which the rung embeds. All three tables are created empty, so nothing is moved, and the
    /// column is nullable with no default: one catalog-only <c>ADD COLUMN</c>, and every stored rule keeps its effect (NULL names no
    /// source). No GRANT here: the <c>config</c> schema's blanket SELECT covers the tables, and owner-only writes, and the one EXECUTE
    /// each source role holds, are provisioning's, re-asserted on every start. <b>No Lite twin:</b> Lite has no PostgreSQL store.
    /// </summary>
    private static readonly string V180Sql = ExternalAlertTables.CreateSql;

    /// <summary>
    /// V181 (#5750) — a notification route can be scoped to one server or to a tag and its whole subtree.
    /// <c>config.config_notification_routes</c> gains the nullable <c>scope_server_id</c> (a registry server id) and
    /// <c>scope_tag_id</c> (a <c>config.server_tags</c> id), a CHECK that at most one is set, and a CHECK that the match <c>*</c>
    /// (the lowest metric level, ruling 4) appears only on a scoped route. A stored row keeps its effect: both columns are NULL on it,
    /// so it stays fleet-wide.
    ///
    /// <para><b>No foreign key and no ON DELETE action, deliberately.</b> A route whose server or tag was removed must stay as it was
    /// written and show as orphaned; a cascade would delete it, and <c>ON DELETE SET NULL</c> would silently widen it to fleet-wide.
    /// The service marks it orphaned at load and the router matches it to nothing.</para>
    ///
    /// <para><b>The <c>*</c> CHECK is NOT VALID</b>: it is enforced for every insert and update from now on, and it is not checked
    /// against rows already stored, so a store that holds an unscoped <c>*</c> row (a literal metric name before this rung, which
    /// matched nothing real) still migrates instead of refusing to start. The router never matches such a row.</para>
    ///
    /// <para><b>Reload.</b> The reload beacon (<c>config.config_bump_version</c>) already fires on a write to the routes table and to
    /// <c>config_monitored_servers</c>, but V32 deliberately gave the two tag tables none. A scoped route's server set depends on both, so
    /// this rung adds V117's trigger shape to <c>config.server_tags</c> and <c>config.server_tag_map</c>: a tag edit, a mapping change or
    /// a cascade from a tag delete bumps <c>config_service.config_version</c> and the running service re-reads the routes.</para>
    ///
    /// <para>Catalog-only: two nullable columns, two constraints, two triggers. Every statement is idempotent. No GRANT here: the
    /// viewer and mcp roles' column-level SELECT on the routes table is a provisioning statement (<c>DarlingManagedRoles</c>,
    /// re-asserted on every managed start after migration, and mirrored in <c>Darling/tools/provision-roles.sql</c>); both new columns are on its
    /// non-secret list. <b>Lite twin:</b> the DuckDB step at schema 69 (separate store, separate lane).</para>
    /// </summary>
    private const string V181Sql = @"
ALTER TABLE config.config_notification_routes
    ADD COLUMN IF NOT EXISTS scope_server_id integer,
    ADD COLUMN IF NOT EXISTS scope_tag_id integer;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_config_notification_routes_one_scope'
                     AND conrelid = 'config.config_notification_routes'::regclass) THEN
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_one_scope
            CHECK (scope_server_id IS NULL OR scope_tag_id IS NULL);
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_config_notification_routes_wildcard_scoped'
                     AND conrelid = 'config.config_notification_routes'::regclass) THEN
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_wildcard_scoped
            CHECK (btrim(metric_match) <> '*' OR scope_server_id IS NOT NULL OR scope_tag_id IS NOT NULL) NOT VALID;
    END IF;
END $$;

DROP TRIGGER IF EXISTS trg_bump_server_tags ON config.server_tags;
CREATE TRIGGER trg_bump_server_tags
    AFTER INSERT OR UPDATE OR DELETE ON config.server_tags
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();

DROP TRIGGER IF EXISTS trg_bump_server_tag_map ON config.server_tag_map;
CREATE TRIGGER trg_bump_server_tag_map
    AFTER INSERT OR UPDATE OR DELETE ON config.server_tag_map
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();";

    /// <summary>
    /// V182 (#5506) — what a capacity question needs from the read record. <c>collect.read_latency</c> gains
    /// <c>window_band</c> (the width of the window the read asked for: <c>1h</c>, <c>24h</c>, <c>7d</c>, <c>7d+</c>, or
    /// <c>none</c> for a read with no window), <c>max_in_flight</c> (the most reads already in progress on the service when a
    /// read of the key started, in the hour) and <c>wait_total_ms</c> (the time the key's reads waited for a store connection,
    /// apart from their run time). The band joins the primary key, so a 1-hour read and a 7-day read of one route no longer
    /// share a histogram. <c>collect.slow_reads</c> gains the nullable <c>in_flight</c> and <c>wait_ms</c> for each slow read.
    ///
    /// <para>Rows stored before this rung keep their counts: each gets <c>window_band = 'none'</c>, <c>max_in_flight = 0</c>
    /// and <c>wait_total_ms = 0</c> from the column defaults, and their old key is still unique under the new one. The primary
    /// key (V148's <c>read_latency_pkey</c>) is dropped and re-added only while it does not yet name <c>window_band</c>, so a
    /// second run does nothing. The table
    /// holds at most 90 days of hourly rows per surface, route and outcome, so the index rebuild is small. <b>No Lite
    /// twin:</b> Lite records no read latency.</para>
    /// </summary>
    private const string V182Sql = @"
ALTER TABLE collect.read_latency
    ADD COLUMN IF NOT EXISTS window_band text NOT NULL DEFAULT 'none',
    ADD COLUMN IF NOT EXISTS max_in_flight integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS wait_total_ms bigint NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS
       (
           SELECT 1
           FROM pg_constraint AS c
           JOIN pg_attribute AS a
             ON a.attrelid = c.conrelid
            AND a.attnum = ANY (c.conkey)
           WHERE c.conrelid = 'collect.read_latency'::regclass
           AND   c.contype = 'p'
           AND   a.attname = 'window_band'
       )
    THEN
        ALTER TABLE collect.read_latency DROP CONSTRAINT IF EXISTS read_latency_pkey;
        ALTER TABLE collect.read_latency
            ADD CONSTRAINT read_latency_pkey PRIMARY KEY (metric_time, surface, route, outcome, window_band);
    END IF;
END $$;

ALTER TABLE collect.slow_reads
    ADD COLUMN IF NOT EXISTS in_flight integer,
    ADD COLUMN IF NOT EXISTS wait_ms bigint;";

    /// <summary>
    /// V183 (#5793): who made each read, with no personal data. <c>collect.slow_reads</c> gains the nullable <c>caller</c> (an
    /// MCP client's own name and version, its User-Agent product token when it sent none, or <c>web:</c> and a salted hash of
    /// the web session key) and <c>collect.read_latency</c> gains <c>distinct_callers</c> (how many different callers read the
    /// key in the hour). The caller does not join the hourly key, which would multiply its rows by the number of clients.
    ///
    /// <para>Rows stored before this rung get a NULL caller and a count of 0 from the column default. Both ADD COLUMNs are
    /// IF NOT EXISTS, so a second run does nothing, and neither rewrites the table. <b>No Lite twin:</b> Lite records no read
    /// latency.</para>
    /// </summary>
    private const string V183Sql = @"
ALTER TABLE collect.slow_reads
    ADD COLUMN IF NOT EXISTS caller text;

ALTER TABLE collect.read_latency
    ADD COLUMN IF NOT EXISTS distinct_callers integer NOT NULL DEFAULT 0;";

    /// <summary>
    /// V184 (#5632 Phase A) — the tables for Level 1 investigations that an alert hands to the operator's own agent, and the
    /// agent columns on <c>config.config_notification_routes</c>. The text is on <see cref="InvestigationTables"/>, which the
    /// rung embeds. Every table is created empty, so nothing is moved; each route column is nullable or has a constant default,
    /// so the <c>ADD COLUMN</c> is catalog-only and every stored route keeps its effect (a NULL agent level names no agent).
    /// No GRANT here: the <c>config</c> schema's blanket SELECT covers the tables, and the narrow writes are provisioning's
    /// (<c>DarlingManagedRoles</c>, mirrored in <c>Darling/tools/provision-roles.sql</c>), re-asserted on every start.
    /// <b>No Lite twin</b> in Phase A: Lite Level 1 is a follow-up.
    /// </summary>
    private static readonly string V184Sql = InvestigationTables.CreateSql;

    /// <summary>
    /// V187 (#5825): alert resolutions can be sent to the channels that got the fire, opt-in per channel.
    /// <c>config.config_notification</c> gains four fleet flags, <c>slack_send_resolutions</c>, <c>teams_send_resolutions</c>,
    /// <c>generic_send_resolutions</c> (the generic webhook) and <c>email_send_resolutions</c>, non-null and FALSE by default,
    /// so a store that does not turn one on keeps sending no resolution anywhere. <c>config.config_notification_routes</c>
    /// gains the nullable <c>send_resolutions</c>: NULL follows the channel's flag, TRUE or FALSE overrides it for that
    /// route's destinations. <c>config.config_alert_log</c> gains what a sent resolution records on its own history row:
    /// <c>resolution_sent_at</c> (UTC, like <c>alert_time</c>), <c>resolution_channels</c> (the channels it went to) and
    /// <c>resolution_outcome</c> (what the send did). All three are NULL on every row a resolution was not sent for,
    /// including every row written before this rung.
    ///
    /// <para>Catalog-only and idempotent. No GRANT here: the read roles' column-level SELECT on the notification and routes
    /// tables is provisioning's (<c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>, mirrored in
    /// <c>Darling/tools/provision-roles.sql</c>), and every new column is on its non-secret list; the alert log keeps its
    /// table-wide SELECT. <b>No Lite twin</b> (#5825 decision 4: Lite shows a resolution as a tray notification).</para>
    /// </summary>
    private const string V187Sql = @"
ALTER TABLE config.config_notification
    ADD COLUMN IF NOT EXISTS slack_send_resolutions boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS teams_send_resolutions boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS generic_send_resolutions boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS email_send_resolutions boolean NOT NULL DEFAULT FALSE;

ALTER TABLE config.config_notification_routes
    ADD COLUMN IF NOT EXISTS send_resolutions boolean;

ALTER TABLE config.config_alert_log
    ADD COLUMN IF NOT EXISTS resolution_sent_at timestamp,
    ADD COLUMN IF NOT EXISTS resolution_channels text,
    ADD COLUMN IF NOT EXISTS resolution_outcome text;";

    /// <summary>
    /// V185 (#5812): the open days of <c>collect.plan_regression_daily</c> are built too, as partial days.
    /// <c>collect.plan_regression_daily_built</c> gains <c>built_through</c>, the <c>first_execution_time</c> the build covered
    /// (exclusive) by a partial build: a day's totals are its rows (by <c>last_execution_time</c>) whose first execution is
    /// below it. A partial build is valid while <c>partial_seq = late_seq</c>, the second new column, and leaves
    /// <c>built_seq</c> NULL; a whole build stamps <c>built_seq</c> and clears both. So code from before this rung, which reads
    /// a day as built only while <c>built_seq = late_seq</c>, reads a partial day live, and its builder's whole build (which
    /// leaves <c>built_through</c> as it was) still reads as whole, because <c>built_seq = late_seq</c> means the whole day
    /// whatever <c>built_through</c> holds. No built day is invalidated. The watermark is on <c>first_execution_time</c> because it is part of an interval row's identity and never
    /// moves when the row is updated, so a row is on one side of it for its whole life (its <c>last_execution_time</c> moves
    /// while the interval runs), and because the interval table is partitioned and indexed on it, so the live half of the read
    /// touches only the rows after the watermark.
    ///
    /// <para><b>The trigger now marks the open days.</b> Its upper bound moves from yesterday's midnight to two hours before the
    /// current hour (<see cref="PlanRegressionDaily.LateTriggerLagHours"/>), so a row written with a first execution below that
    /// marks its days (the #5631 validity rule: a moved <c>late_seq</c> invalidates a whole or a partial build). The builder's
    /// watermark is three hours before the current hour by the store's own clock, one hour under the trigger's, so a write whose transaction began up to an hour
    /// before a build still marks the day it could change. Steady collection does not fire it: the collector writes the
    /// intervals still running and the one that just ended, whose first executions are at or after the trigger's bound.
    /// That holds only while the two clocks agree: <c>first_execution_time</c> is the monitored server's clock and
    /// <c>now()</c> the store's. The bound sits an hour below the start of a 60-minute interval that has just ended, so a store
    /// clock up to an hour ahead stays quiet; at one hour the margin was zero, and a store a second ahead fired the trigger on
    /// every steady apply (#5812 review). A bigger skew, or Query Store intervals longer than an hour, fires it on every apply,
    /// and the open days then read live, as before #5812: slower, never a wrong number.</para>
    /// </summary>
    private static readonly string V185Sql = $@"
ALTER TABLE collect.plan_regression_daily_built
    ADD COLUMN IF NOT EXISTS built_through timestamp,
    ADD COLUMN IF NOT EXISTS partial_seq bigint;

DROP TRIGGER IF EXISTS trg_plan_regression_daily_late ON collect.query_store_interval_latest;
CREATE TRIGGER trg_plan_regression_daily_late
    AFTER INSERT OR UPDATE ON collect.query_store_interval_latest
    FOR EACH ROW
    WHEN (NEW.first_execution_time < date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '{PlanRegressionDaily.LateTriggerLagHours} hour'
          AND NEW.first_execution_time >= date_trunc('day', now() AT TIME ZONE 'UTC') - interval '17 days')
    EXECUTE FUNCTION collect.plan_regression_daily_mark_late();";

    /// <summary>
    /// V186 (#5810): <c>collect.collection_log_hourly</c>, the hourly rollup of <c>collect.collection_log</c> that
    /// <c>get_collection_health</c> reads for the closed hours of its 7-day window. One row per (server, collector, closed hour)
    /// with every decomposable aggregate the read's <c>health</c> computes (counts, <c>rows_stored</c>, <c>duration_sum</c> and
    /// <c>duration_count</c> for the average, the maxima and the newest instants) and <c>duration_histogram</c>, the count of timed
    /// runs per slot of the shared duration bounds, from which the read takes a bucketed p95. A plain table: about 34 rows per
    /// server per hour, kept about 7 days by the service's hourly pass, which also fills it (its first pass backfills the window).
    /// The <c>hour_start</c> index serves that pass's newest-hour lookup and its retention DELETE. Idempotent; it creates
    /// objects only, so a rewind is a DROP TABLE.
    /// </summary>
    private const string V186Sql = @"
CREATE TABLE IF NOT EXISTS collect.collection_log_hourly
(
    server_id integer NOT NULL,
    collector_name text NOT NULL,
    hour_start timestamp NOT NULL,
    total_runs integer NOT NULL,
    success_count integer NOT NULL,
    error_count integer NOT NULL,
    permission_denied_count integer NOT NULL,
    yield_count integer NOT NULL,
    abandoned_count integer NOT NULL,
    extension_missing_count integer NOT NULL,
    session_missing_count integer NOT NULL,
    note_count integer NOT NULL,
    runs_with_rows integer NOT NULL,
    rows_stored bigint NOT NULL,
    duration_sum bigint NOT NULL,
    duration_count integer NOT NULL,
    max_duration_ms integer,
    max_slowest_item_ms integer,
    last_success_time timestamp,
    last_run_time timestamp NOT NULL,
    last_error_time timestamp,
    last_failure_text_time timestamp,
    last_note_time timestamp,
    last_denied_time timestamp,
    last_non_skip_time timestamp,
    last_productive_time timestamp,
    last_streak_break_time timestamp,
    duration_histogram integer[] NOT NULL,
    CONSTRAINT pk_collection_log_hourly PRIMARY KEY (server_id, collector_name, hour_start)
);

CREATE INDEX IF NOT EXISTS idx_collection_log_hourly_hour ON collect.collection_log_hourly (hour_start);";

    /// <summary>
    /// V188 (#5821): <c>config.investigation_request_alerts</c> gains the five values the brief's alerts section reads from
    /// the alert's history row: <c>current_value</c>, <c>threshold_value</c>, <c>muted</c>, <c>detail_text</c> and
    /// <c>context_json</c>. An alert with an agent route now opens or joins its request before its page is sent, so the
    /// generic webhook can name the request, and its history row is written after the send: an agent that claims while the
    /// page is in flight would compose a brief with no history row to read. The link rows carry the values instead.
    ///
    /// <para>Every column is nullable with no default, so each <c>ADD COLUMN</c> is catalog-only and rewrites nothing; a link
    /// row written before this rung keeps NULLs and its brief reads the history row as before. IF NOT EXISTS, so a second run
    /// does nothing. No GRANT: the table's grants are table-level. <b>No Lite twin:</b> Lite has no investigation store.</para>
    /// </summary>
    private const string V188Sql = @"
ALTER TABLE config.investigation_request_alerts
    ADD COLUMN IF NOT EXISTS current_value double precision,
    ADD COLUMN IF NOT EXISTS threshold_value double precision,
    ADD COLUMN IF NOT EXISTS muted boolean,
    ADD COLUMN IF NOT EXISTS detail_text text,
    ADD COLUMN IF NOT EXISTS context_json text;";

    /// <summary>
    /// V190 (#5857): <c>collect.store_metrics.size_estimated</c>. The hourly store self-metrics sweep sizes hypertables
    /// and continuous aggregates from the compression catalog (a compressed chunk contributes the bytes TimescaleDB
    /// recorded when it compressed the chunk, not a file-system read), because the exact sizes make a file call per chunk and
    /// their cost grew with the chunk count. One sweep a day (the first without an exact row in the last 23 hours) writes
    /// the exact sizes. <c>true</c> marks a row sized by the estimate, <c>false</c> an exact one. NULL on every row
    /// written before this rung, which were exact; no backfill, so nothing is rewritten.
    ///
    /// <para>One nullable column with no default, so the <c>ADD COLUMN</c> is catalog-only; IF NOT EXISTS, so a second run
    /// does nothing. No GRANT: the table's grants are table-level. <b>No Lite twin:</b> Lite sizes its own DuckDB file,
    /// not hypertables.</para>
    /// </summary>
    private const string V190Sql = @"
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS size_estimated boolean;";

    /// <summary>
    /// V191 (#5909, Phase B of #5632): the store for Level 2. An agent may change alert settings, so the store needs a place
    /// to hold what an agent may do and what it has changed. This is the only migration in Phase B; every later slice uses
    /// these tables and columns.
    ///
    /// <para><b>Levels.</b> The route's <c>agent_level</c> allowed NULL, 0 and 1; it now allows 0 to 2. A request's
    /// <c>autonomy_level</c> allowed 1; it now allows 1 to 2. A level outside the range is still refused. The two CHECKs
    /// are replaced in place, under their own names, only when the stored definition does not already name level 2, so a
    /// second run replaces nothing. Widening a CHECK cannot be violated by a stored row.</para>
    ///
    /// <para><b>The switch.</b> <c>config.config_service.agent_actions_enabled</c>, default <c>true</c>, so it reads as on on
    /// a new store and on an upgraded one. It is not a second opt-in: a route set to Level 2 is the opt-in, and this switch
    /// is the operator's way to turn every agent change off at once. A constant default, so the <c>ADD COLUMN</c> is
    /// catalog-only.</para>
    ///
    /// <para><b>Threshold overrides.</b> <c>config.config_threshold_overrides</c>: one value per server, database and
    /// threshold key. It is deleted with its server (foreign key, <c>ON DELETE CASCADE</c>). The unique index covers a
    /// NULL database name through <c>coalesce(database_name, '')</c>. It joins the <c>config_version</c> reload beacon with
    /// V117's trigger shape (statement level, <c>config.config_bump_version()</c> verbatim), so a change reaches the engine
    /// the way every other config change does. <c>request_id</c> names the request that asked for the change and has no
    /// foreign key: the override outlives the request. Overrides are configuration and are never aged. <c>value</c> is <c>double
    /// precision</c>, the type of the store's own thresholds (<c>config_alert_log.threshold_value</c> and five
    /// <c>config_alert_settings</c> columns: <c>analysis_notify_severity</c>, the two retention-hold ratios and the two
    /// deadlock per-hour rates), so a fractional threshold is representable; an integer threshold is exact in it, and
    /// widening the column later would have been a table rewrite. Which keys Level 2 may touch is the engine registry's
    /// (#5910), not this table's.</para>
    ///
    /// <para><b>Feedback.</b> <c>config.investigation_feedback</c>: a person's verdict on a report (right, partly or wrong),
    /// the corrected verdict, the step the agent missed, a comment and the author; one row per request per author, compared
    /// case-insensitively (<c>lower(author)</c>), with the author held to the family's identity shape
    /// (<c>^[A-Za-z0-9._:-]{1,64}$</c>, as <c>claimed_by</c> and <c>agent_id</c>), the corrected verdict to the report
    /// verdicts and the free text to 4000 characters, as the report's summary. It is
    /// deleted with its request (<c>ON DELETE CASCADE</c>), so it ages with the request through retention's DELETE.</para>
    ///
    /// <para><b>Practice.</b> <c>config.investigation_requests.practice_of</c>, nullable with no default and <b>no foreign
    /// key</b>: a practice request survives deletion of the request it replays.</para>
    ///
    /// <para>The journal needs no schema change. Grants are provisioning's (managed section 10e and provision-roles.sql step
    /// 3j); the default privileges give admin the writes and viewer the read on a table made here. <b>No Lite twin:</b> Lite
    /// has no investigation store.</para>
    /// </summary>
    private const string V191Sql = @"
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'ck_config_notification_routes_agent'
                 AND conrelid = 'config.config_notification_routes'::regclass
                 AND pg_get_constraintdef(oid) !~ 'ARRAY\[0, 1, 2\]') THEN
        ALTER TABLE config.config_notification_routes DROP CONSTRAINT ck_config_notification_routes_agent;
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_agent CHECK (
                (agent_level IS NULL OR agent_level IN (0, 1, 2))
                AND agent_max_per_hour BETWEEN 1 AND 60
                AND agent_max_per_day BETWEEN 1 AND 500
                AND agent_lease_minutes BETWEEN 5 AND 120
                AND agent_expiry_minutes BETWEEN 15 AND 1440);
    END IF;

    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'ck_investigation_requests_autonomy'
                 AND conrelid = 'config.investigation_requests'::regclass
                 AND pg_get_constraintdef(oid) !~ 'ARRAY\[1, 2\]') THEN
        ALTER TABLE config.investigation_requests DROP CONSTRAINT ck_investigation_requests_autonomy;
        ALTER TABLE config.investigation_requests
            ADD CONSTRAINT ck_investigation_requests_autonomy CHECK (autonomy_level IN (1, 2));
    END IF;
END $do$;

ALTER TABLE config.config_service
    ADD COLUMN IF NOT EXISTS agent_actions_enabled boolean NOT NULL DEFAULT true;

ALTER TABLE config.investigation_requests
    ADD COLUMN IF NOT EXISTS practice_of uuid;

CREATE TABLE IF NOT EXISTS config.config_threshold_overrides
(
    id bigint GENERATED ALWAYS AS IDENTITY,
    server_id integer NOT NULL,
    database_name text,
    threshold_key text NOT NULL,
    value double precision NOT NULL,
    set_by text NOT NULL,
    request_id uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_config_threshold_overrides PRIMARY KEY (id),
    CONSTRAINT fk_config_threshold_overrides_server FOREIGN KEY (server_id)
        REFERENCES config.config_monitored_servers (server_id) ON DELETE CASCADE,
    CONSTRAINT ck_config_threshold_overrides_key CHECK (btrim(threshold_key) <> ''),
    CONSTRAINT ck_config_threshold_overrides_set_by CHECK (btrim(set_by) <> '')
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_config_threshold_overrides_scope
    ON config.config_threshold_overrides (server_id, (coalesce(database_name, '')), threshold_key);

DROP TRIGGER IF EXISTS trg_bump_threshold_overrides ON config.config_threshold_overrides;
CREATE TRIGGER trg_bump_threshold_overrides
    AFTER INSERT OR UPDATE OR DELETE ON config.config_threshold_overrides
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();

CREATE TABLE IF NOT EXISTS config.investigation_feedback
(
    id bigint GENERATED ALWAYS AS IDENTITY,
    request_id uuid NOT NULL,
    verdict text NOT NULL,
    corrected_verdict text,
    missed_step text,
    comment text,
    author text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_investigation_feedback PRIMARY KEY (id),
    CONSTRAINT fk_investigation_feedback_request FOREIGN KEY (request_id)
        REFERENCES config.investigation_requests (request_id) ON DELETE CASCADE,
    CONSTRAINT ck_investigation_feedback_verdict CHECK (verdict IN ('right', 'partly', 'wrong')),
    CONSTRAINT ck_investigation_feedback_corrected CHECK (corrected_verdict IS NULL OR corrected_verdict IN ('real', 'false_positive', 'needs_person')),
    CONSTRAINT ck_investigation_feedback_author CHECK (author ~ '^[A-Za-z0-9._:-]{1,64}$'),
    CONSTRAINT ck_investigation_feedback_missed_step CHECK (missed_step IS NULL OR length(missed_step) <= 4000),
    CONSTRAINT ck_investigation_feedback_comment CHECK (comment IS NULL OR length(comment) <= 4000)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_investigation_feedback_author
    ON config.investigation_feedback (request_id, lower(author));";

    /// <summary>
    /// V192 (#5977, wave 1 of #5971): the store for query tags. The tag set (<c>config.query_tags</c>, the shape of
    /// <c>config.server_tags</c> and its own table), hand assignments by <c>query_hash</c> or by object, rules, the per-pass
    /// sightings of query keys and caller fields (<c>collect.query_tag_sightings</c>), the materialised tag map the reads join
    /// (one tag per key, held by its primary key) and the one-row version state. This is the only migration of #5971.
    ///
    /// <para>Deleting a tag deletes its assignments, rules and map rows (cascade); deleting a rule leaves its map rows until the
    /// next rebuild. A write to tags, assignments or rules bumps <c>config.query_tag_state.rules_version</c> through a
    /// statement-level trigger. The trigger function is <c>SECURITY DEFINER</c> with a pinned search_path, unlike V117's
    /// invoker-rights beacon: the mcp role writes the three tables and the viewer role deletes server-scoped rows when a server is
    /// removed, and neither is granted anything on the state row, so the bump has to run as the owner.</para>
    ///
    /// <para>Grants are provisioning's (<c>DarlingManagedRoles</c>, mirrored in <c>Darling/tools/provision-roles.sql</c>). The
    /// service writes sightings, the map and the state row as the store owner. Every statement is idempotent. All new tables are
    /// created empty. <b>Lite twin:</b> the DuckDB tables come in #5989.</para>
    /// </summary>
    private static readonly string V192Sql = QueryTagSchema.CreateSql;

    /// <summary>
    /// V193 (#5997, Phase C of #5632): the store for investigation evidence, follow-ups and signed webhooks. This is the only
    /// migration in Phase C; every later slice uses these tables and columns.
    ///
    /// <para><b>Journal.</b> <c>config.investigation_journal.event_id</c>, a <c>uuid</c> per event row, unique, sent as the
    /// webhook's <c>webhook-id</c> (D6). Existing rows get one each when the column is added; the journal is a plain table.</para>
    ///
    /// <para><b>Requests.</b> <c>max_severity</c> (the highest severity any joined alert had, D6), <c>evidence_state</c> (D9),
    /// and the follow-up trio <c>follow_up_of</c>, <c>follow_up_at</c>, <c>follow_up_condition</c> (D16). <c>follow_up_of</c> has
    /// <b>no foreign key</b>, like <c>practice_of</c>: a follow-up outlives the request it follows. All nullable, no default,
    /// so each <c>ADD COLUMN</c> is catalog-only.</para>
    ///
    /// <para><b>Evidence.</b> <c>config.investigation_evidence</c> (D10): one row per evidence section of a request, unique on
    /// (request, section), <c>content</c> held to 32 KB like a report's evidence, plans by hash and never XML. A section not yet
    /// collected is <c>pending</c>, one never collected is <c>not_collected</c> (D9). Deleted with its request
    /// (<c>ON DELETE CASCADE</c>) and cleared with the brief at 90 days by retention.</para>
    ///
    /// <para><b>Deadlock signatures.</b> <c>config.investigation_signature_days</c> (D13): the daily rollup (server, signature,
    /// day, count), purged at 30 days. <c>config.investigation_signature_merges</c> (D14): the journaled, undoable merge of one
    /// signature into another, with its reason; undoing sets <c>undone_at</c> and the row stays as the record. At most one live
    /// merge per (server, merged signature). Merges are decisions a person can read back and are never aged. Neither table has a
    /// foreign key to the server, like the investigation tables.</para>
    ///
    /// <para><b>Reports.</b> <c>family_of</c> (D17), the first report's request id: a foreign key to the requests, <c>ON DELETE SET
    /// NULL</c>, so a family id always names a request that existed. <c>queries_run</c> (D17): an array of at most 20 items,
    /// no string over 2,000 characters.</para>
    ///
    /// <para><b>Signing.</b> D2 and D3: <c>signing_key</c> (sealed; #5998 seals it, this rung only stores text), the previous key
    /// and when it stops being sent, on <b>both</b> the notification settings row and the routes, because D2 gives one key per
    /// generic channel and per route with its own URL. The keys are secrets and are carved out of the read roles' column grants
    /// in <c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>. <c>fallback_route_id</c> (D8) on the route: a route to use when
    /// the agent route cannot deliver. Never the route itself (a CHECK), and <b>no foreign key</b>, like the route's scope
    /// columns (V181): the reader treats an id that names no route as no fallback.</para>
    ///
    /// <para><b>Procedure stats.</b> <c>collect.procedure_stats.module_modify_date</c>, nullable: the module's <c>modify_date</c>
    /// that D16's deploy condition compares. The collector fills it later (#6009); rows from before keep NULL.</para>
    ///
    /// <para>Grants are provisioning's (managed section 10f and provision-roles.sql step 3l). <b>No Lite twin here:</b> Lite's
    /// schema step is #6010.</para>
    /// </summary>
    private const string V193Sql = @"
ALTER TABLE config.investigation_journal
    ADD COLUMN IF NOT EXISTS event_id uuid NOT NULL DEFAULT gen_random_uuid();

CREATE UNIQUE INDEX IF NOT EXISTS uq_investigation_journal_event_id
    ON config.investigation_journal (event_id);

ALTER TABLE config.investigation_requests
    ADD COLUMN IF NOT EXISTS max_severity text,
    ADD COLUMN IF NOT EXISTS evidence_state text,
    ADD COLUMN IF NOT EXISTS follow_up_of uuid,
    ADD COLUMN IF NOT EXISTS follow_up_at timestamptz,
    ADD COLUMN IF NOT EXISTS follow_up_condition text;

ALTER TABLE config.investigation_reports
    ADD COLUMN IF NOT EXISTS family_of uuid,
    ADD COLUMN IF NOT EXISTS queries_run jsonb;

DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'fk_investigation_reports_family'
                     AND conrelid = 'config.investigation_reports'::regclass) THEN
        ALTER TABLE config.investigation_reports
            ADD CONSTRAINT fk_investigation_reports_family FOREIGN KEY (family_of)
                REFERENCES config.investigation_requests (request_id) ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_investigation_reports_queries_run'
                     AND conrelid = 'config.investigation_reports'::regclass) THEN
        ALTER TABLE config.investigation_reports
            ADD CONSTRAINT ck_investigation_reports_queries_run CHECK (
                queries_run IS NULL OR (
                    jsonb_typeof(queries_run) = 'array'
                    AND jsonb_array_length(queries_run) <= 20
                    AND NOT jsonb_path_exists(queries_run, 'strict $.** ? (@.type() == ""string"" && @ like_regex ""^(?:.{200}){10}."" flag ""s"")')));
    END IF;
END $do$;

CREATE TABLE IF NOT EXISTS config.investigation_evidence
(
    evidence_id bigint GENERATED ALWAYS AS IDENTITY,
    request_id uuid NOT NULL,
    section text NOT NULL,
    state text NOT NULL DEFAULT 'pending',
    content jsonb,
    captured_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_investigation_evidence PRIMARY KEY (evidence_id),
    CONSTRAINT uq_investigation_evidence_section UNIQUE (request_id, section),
    CONSTRAINT fk_investigation_evidence_request FOREIGN KEY (request_id)
        REFERENCES config.investigation_requests (request_id) ON DELETE CASCADE,
    CONSTRAINT ck_investigation_evidence_section CHECK (section ~ '^[a-z][a-z0-9_]{1,40}$'),
    CONSTRAINT ck_investigation_evidence_state CHECK (state IN ('pending', 'ready', 'not_collected')),
    CONSTRAINT ck_investigation_evidence_content CHECK (content IS NULL OR octet_length(content::text) <= 32768)
);

CREATE TABLE IF NOT EXISTS config.investigation_signature_days
(
    server_id integer NOT NULL,
    signature text NOT NULL,
    day date NOT NULL,
    count integer NOT NULL DEFAULT 0,
    CONSTRAINT pk_investigation_signature_days PRIMARY KEY (server_id, signature, day),
    CONSTRAINT ck_investigation_signature_days_signature CHECK (btrim(signature) <> ''),
    CONSTRAINT ck_investigation_signature_days_count CHECK (count >= 0)
);

CREATE INDEX IF NOT EXISTS ix_investigation_signature_days_day
    ON config.investigation_signature_days (day);

CREATE TABLE IF NOT EXISTS config.investigation_signature_merges
(
    merge_id bigint GENERATED ALWAYS AS IDENTITY,
    server_id integer NOT NULL,
    signature text NOT NULL,
    merged_into text NOT NULL,
    reason text NOT NULL,
    merged_by text NOT NULL,
    merged_at timestamptz NOT NULL DEFAULT now(),
    undone_at timestamptz,
    undone_by text,
    CONSTRAINT pk_investigation_signature_merges PRIMARY KEY (merge_id),
    CONSTRAINT ck_investigation_signature_merges_signature CHECK (btrim(signature) <> '' AND btrim(merged_into) <> '' AND signature <> merged_into),
    CONSTRAINT ck_investigation_signature_merges_reason CHECK (btrim(reason) <> '' AND length(reason) <= 2000),
    CONSTRAINT ck_investigation_signature_merges_merged_by CHECK (merged_by ~ '^[A-Za-z0-9._:-]{1,64}$'),
    CONSTRAINT ck_investigation_signature_merges_undone CHECK ((undone_at IS NULL) = (undone_by IS NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_investigation_signature_merges_live
    ON config.investigation_signature_merges (server_id, signature) WHERE undone_at IS NULL;

ALTER TABLE config.config_notification
    ADD COLUMN IF NOT EXISTS signing_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS previous_signing_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS previous_signing_key_until timestamptz;

ALTER TABLE config.config_notification_routes
    ADD COLUMN IF NOT EXISTS signing_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS previous_signing_key text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS previous_signing_key_until timestamptz,
    ADD COLUMN IF NOT EXISTS fallback_route_id integer;

DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_config_notification_routes_fallback'
                     AND conrelid = 'config.config_notification_routes'::regclass) THEN
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_fallback CHECK (fallback_route_id IS NULL OR fallback_route_id <> route_id);
    END IF;
END $do$;

ALTER TABLE collect.procedure_stats
    ADD COLUMN IF NOT EXISTS module_modify_date timestamp;";

    /// <summary>
    /// V194 (#5985, part of #5971): <c>database_name</c> joins the identity of a query-tag sighting. V192 made
    /// <c>collect.query_tag_sightings</c> unique on (server, kind, key, application, login, host), and the writer kept the
    /// first non-NULL <c>database_name</c>, so one hash seen in two databases by the same caller collapsed into one row and a
    /// database rule could only ever see the first database. The new unique index adds <c>COALESCE(database_name, '')</c> as
    /// the fourth caller part, after application, login and host (the same NULL-is-empty fold the other three use), so each database a hash was seen in has its
    /// own row and the map builder, which resolves a key against every sighting row it has, tests a database rule against
    /// each. The new index is created first and the old one dropped after it, both guarded, so a re-run is a no-op. Existing
    /// rows stay: they are unique on the old, coarser key, so they are unique on the finer one. Only an index changes: no
    /// grant, no column, no data.
    /// </summary>
    private const string V194Sql = @"
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_tag_sightings_key_db
    ON collect.query_tag_sightings (server_id, key_kind, key, COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, ''), COALESCE(database_name, ''));
DROP INDEX IF EXISTS collect.ux_query_tag_sightings_key;";

    /// <summary>
    /// V195 (#5632) - <c>collect.query_store_forced_plans</c>: the plans Query Store has forced, read from <c>sys.query_store_plan</c> by
    /// <c>QueryStoreForcedPlansCollector</c> every five minutes, one row per forced plan plus ONE all-NULL marker row per captured database
    /// per pass (so a force that left a captured database reads as REMOVED, not as not captured). A PLAIN table on every store
    /// (<c>StaysPlainTable</c>: a few rows per database per pass, the grid stays at 72 hypertables), its 30-day retention the time-sliced
    /// DELETE. Columns and the one index (server_id, collection_time) are exactly <c>PgSchemaGenerator</c>'s output, so a fresh store (V1's catalog walk) and an upgraded one
    /// agree; the table is small enough that it also serves the per-database newest-capture read. New table, no data moves, no grant (the <c>collect</c> schema carries blanket SELECT). Every statement is idempotent.
    /// </summary>
    private const string V195Sql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_forced_plans (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    plan_id bigint,
    query_id bigint,
    query_plan_hash text,
    plan_forcing_type text,
    force_failure_count bigint,
    last_force_failure_reason text,
    last_execution_time timestamp,
    last_compile_start_time timestamp
);

CREATE INDEX IF NOT EXISTS idx_query_store_forced_plans_time ON collect.query_store_forced_plans(server_id, collection_time);";

    /// <summary>
    /// V197 (#5632, step 8): the link a plan-force check carries. <c>config.investigation_requests.force_check_of</c> is the
    /// <c>action_called</c> journal id of the force the check is about, <c>force_check_no</c> its number (1 to 26). Both are null or both
    /// are set (a CHECK), and the pair is unique where set, so a force has at most one request per check number whatever replays the sweep.
    /// Not <c>follow_up_of</c>: a request has one follow-up child, and a check must not take the one the agent's own <c>follow_up_at</c> needs.
    /// <b>No foreign key</b> (the journal id is a plain number, like <c>follow_up_of</c>). Both columns are nullable with no default, so each
    /// <c>ADD COLUMN</c> is catalog-only and the one unique index is built over a column that is null on every stored row.
    /// </summary>
    private const string V197Sql = @"
ALTER TABLE config.investigation_requests
    ADD COLUMN IF NOT EXISTS force_check_of bigint,
    ADD COLUMN IF NOT EXISTS force_check_no smallint;

DO $do$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conname = 'ck_investigation_requests_force_check'
                     AND conrelid = 'config.investigation_requests'::regclass) THEN
        ALTER TABLE config.investigation_requests
            ADD CONSTRAINT ck_investigation_requests_force_check CHECK (
                (force_check_of IS NULL AND force_check_no IS NULL)
                OR (force_check_of IS NOT NULL AND force_check_no BETWEEN 1 AND 26));
    END IF;
END $do$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_investigation_requests_force_check
    ON config.investigation_requests (force_check_of, force_check_no)
    WHERE force_check_of IS NOT NULL;";

    /// <summary>
    /// V198 (#6066): the store's pseudonym key. <c>config.pseudonym_key</c> is a one-row table (<c>id = 1</c>, a CHECK) holding 32 bytes
    /// that turn a login or host name into the same <c>login#3fa9c2d1</c> text in every <c>metadataOnly</c> MCP answer, brief and webhook
    /// from this store (<c>PerformanceMonitor.Common.PseudonymKey</c>). The row is seeded here, once, with <c>sha256</c> of two
    /// <c>gen_random_uuid()</c> values (PostgreSQL's own strong random source, no extension), so every store has its key from the upgrade
    /// on and no two services race to make one; <c>ON CONFLICT DO NOTHING</c> keeps a second run, and a restored or copied store, on the
    /// key it has. A fresh install runs this rung too (the generated schema is V1 only), so a fresh and an upgraded store each end with
    /// exactly one row. The key is never rotated.
    ///
    /// <para><b>Only the store owner reads it.</b> The table is revoked from <c>PUBLIC</c> here and from <c>admin</c>, <c>viewer</c> and <c>mcp</c>
    /// (the roles that exist) in the same transaction, so no default grant outlives the migration; provisioning (<c>provision-roles.sql</c> step
    /// 2d and 3g, managed section 10a) revokes it again and grants SELECT to nobody. It carries the password key tables' owner-only write
    /// triggers (<c>config.password_key_owner_only()</c>, V165), <c>ENABLE ALWAYS</c>.</para>
    /// </summary>
    private const string V198Sql = @"
CREATE TABLE IF NOT EXISTS config.pseudonym_key
(
    id smallint NOT NULL,
    key bytea NOT NULL,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT pk_pseudonym_key PRIMARY KEY (id),
    CONSTRAINT ck_pseudonym_key_single_row CHECK (id = 1),
    CONSTRAINT ck_pseudonym_key_length CHECK (octet_length(key) = 32)
);

REVOKE ALL ON config.pseudonym_key FROM PUBLIC;

DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'admin') THEN
        REVOKE ALL ON config.pseudonym_key FROM admin;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'viewer') THEN
        REVOKE ALL ON config.pseudonym_key FROM viewer;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles AS r WHERE r.rolname = 'mcp') THEN
        REVOKE ALL ON config.pseudonym_key FROM mcp;
    END IF;
END
$do$;

DROP TRIGGER IF EXISTS trg_pseudonym_key_owner_only_row ON config.pseudonym_key;
CREATE TRIGGER trg_pseudonym_key_owner_only_row
    BEFORE INSERT OR UPDATE OR DELETE ON config.pseudonym_key
    FOR EACH ROW EXECUTE FUNCTION config.password_key_owner_only();
DROP TRIGGER IF EXISTS trg_pseudonym_key_owner_only_truncate ON config.pseudonym_key;
CREATE TRIGGER trg_pseudonym_key_owner_only_truncate
    BEFORE TRUNCATE ON config.pseudonym_key
    FOR EACH STATEMENT EXECUTE FUNCTION config.password_key_owner_only();
ALTER TABLE config.pseudonym_key ENABLE ALWAYS TRIGGER trg_pseudonym_key_owner_only_row;
ALTER TABLE config.pseudonym_key ENABLE ALWAYS TRIGGER trg_pseudonym_key_owner_only_truncate;

INSERT INTO config.pseudonym_key (id, key)
VALUES (1, pg_catalog.sha256(pg_catalog.decode(
    pg_catalog.replace(pg_catalog.gen_random_uuid()::text || pg_catalog.gen_random_uuid()::text, '-', ''), 'hex')))
ON CONFLICT (id) DO NOTHING;";

    /// <summary>
    /// V199 (#5632, step 11): what a person needs to judge a head blocker. Four nullable columns on the blocker side of
    /// <c>collect.dmv_blocking_snapshots</c>, from the blocker's own session: <c>blocking_login_time</c>
    /// (<c>sys.dm_exec_sessions.login_time</c>), <c>blocking_last_request_end_time</c> (<c>last_request_end_time</c>),
    /// <c>blocking_open_transaction_count</c> (<c>open_transaction_count</c>) and <c>blocking_tran_log_bytes_used</c> (the SUM of
    /// <c>database_transaction_log_bytes_used</c> over every transaction of the session, exact bytes, not MB). The two times are the
    /// monitored server's local clock, stored raw: a later kill step compares <c>login_time</c> with the server's own value, so the
    /// stored value must be the server's value, unchanged. A reader converts them to UTC with the server's offset.
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> (the V155 and V193 shape): a row collected before this rung never asked the
    /// engine, and nothing can reconstruct the values, so NULL is the honest value. A nullable, default-less <c>ADD COLUMN</c> is
    /// catalog-only in PostgreSQL and TimescaleDB accepts it on a compressed hypertable.</para>
    ///
    /// <para><b>Appended LAST</b>, after <c>blocking_client_app</c>: the writer is a positional binary COPY, so a fresh store (the
    /// generated V1 table carries the columns from the collector's payload, and these ALTERs no-op there) and an upgraded store share
    /// one physical column order. The trailing <c>CREATE OR REPLACE VIEW</c> re-expands <c>v_dmv_blocking_snapshots</c>' pinned
    /// <c>SELECT *</c>, which PostgreSQL freezes at CREATE (unqualified, like V155's, so it resolves through the migrate session's
    /// <c>search_path</c>).</para>
    /// </summary>
    private const string V199Sql = @"
ALTER TABLE collect.dmv_blocking_snapshots ADD COLUMN IF NOT EXISTS blocking_login_time timestamp;
ALTER TABLE collect.dmv_blocking_snapshots ADD COLUMN IF NOT EXISTS blocking_last_request_end_time timestamp;
ALTER TABLE collect.dmv_blocking_snapshots ADD COLUMN IF NOT EXISTS blocking_open_transaction_count integer;
ALTER TABLE collect.dmv_blocking_snapshots ADD COLUMN IF NOT EXISTS blocking_tran_log_bytes_used bigint;
CREATE OR REPLACE VIEW v_dmv_blocking_snapshots AS SELECT * FROM dmv_blocking_snapshots;";

    /// <summary>
    /// V200 (#6098): the Volume Free Space alert's fleet-wide EXCLUDED-VOLUMES list on the singleton
    /// <c>config_alert_settings</c> row: a <c>text[]</c> of mount points the alert ignores, <c>excluded_databases</c>'s
    /// shape (V17), <c>text[] NOT NULL DEFAULT '{}'</c>. The default is EMPTY, so a pre-rung row reads as "no volume
    /// excluded" and the alert behaves exactly as it did.
    ///
    /// <para><b>The match rule</b> (whole mount point, case-insensitive, a trailing <c>\</c> or <c>/</c> on either side
    /// ignored, never a prefix) is spelled once in <c>PerformanceMonitor.Alerting.VolumeExclusions</c> and applied by the
    /// shared engine to the volumes it reads, BEFORE the breach test. The store holds the normalised list (trimmed,
    /// blanks dropped, de-duplicated by the comparison form).</para>
    ///
    /// <para><b>Readers and writers.</b> <c>StoreConfigProvider</c> seeds it from <c>alerts.excludedVolumes</c> in
    /// darling.json on first run and reads it back; the Viewer's <c>ViewerDataService.AlertSettings</c> binds it from the
    /// Settings window; <c>DarlingAlertReader.AlertSettingsSelectSql</c> reads it for <c>get_alert_settings</c>;
    /// <c>update_alert_settings</c> writes <c>excluded_volumes</c>; the web admin page edits it. No reload beacon of its
    /// own: V17's statement-level <c>trg_bump_alert_settings</c> already bumps <c>config_service.config_version</c> on any
    /// write here. No ACL or provisioning change: <c>config_alert_settings</c> carries table-level grants (no column
    /// carve), the same fact every knob rung on this row has relied on. An older build ignores the column.</para>
    /// </summary>
    private const string V200Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS excluded_volumes text[] NOT NULL DEFAULT '{}'::text[];";

    /// <summary>
    /// V201 (#6160): what the forced-plan collector learned about a force beyond Query Store's own columns, on
    /// <c>collect.query_store_forced_plans</c> (V195). <c>variant_plan_ids</c> is <c>text</c>: the plan ids of the same query, newer
    /// than the forced plan, whose stored plan XML carries <c>UsePlan="1"</c> and no <c>PlanGuideName</c> (SQL Server recompiled the
    /// force under a new plan id), as a comma-separated list in ascending order with no spaces (<c>12,15</c>). The collector writer has no
    /// array type, so the list is text; NULL when there is none. <c>plan_guide_name</c>, <c>plan_guide_enabled</c> (<c>boolean</c>) and
    /// <c>plan_guide_scope</c> (OBJECT, SQL or TEMPLATE) describe a plan guide that covers the forced query, all NULL when none was found.
    ///
    /// <para><b>Appended LAST</b>, after <c>last_compile_start_time</c>: the writer is a positional binary COPY, so a fresh store (the
    /// generated V1 table carries the columns from the collector's payload, and these ALTERs no-op there) and an upgraded store share one
    /// physical column order. All four are nullable with no default, so each <c>ADD COLUMN</c> is catalog-only, and a row stored before
    /// the rung reads NULL (no variant, no guide). The table stays a plain table (<c>StaysPlainTable</c>); no grant (the <c>collect</c>
    /// schema carries blanket SELECT). An older build ignores the columns. Every statement is idempotent.</para>
    /// </summary>
    private const string V201Sql = @"
ALTER TABLE collect.query_store_forced_plans ADD COLUMN IF NOT EXISTS variant_plan_ids text;
ALTER TABLE collect.query_store_forced_plans ADD COLUMN IF NOT EXISTS plan_guide_name text;
ALTER TABLE collect.query_store_forced_plans ADD COLUMN IF NOT EXISTS plan_guide_enabled boolean;
ALTER TABLE collect.query_store_forced_plans ADD COLUMN IF NOT EXISTS plan_guide_scope text;";

    /// <summary>
    /// V202 (#6159): <c>collect.ag_replica_states.primary_replica</c>, the name of the replica that was the primary when the row was
    /// collected, read from <c>sys.dm_hadr_availability_group_states.primary_replica</c>. A secondary can read that DMV, so the Availability
    /// Groups card built from a secondary's own view can say "Primary: NODE1" instead of leaving the reader to guess why the replica
    /// list it was given has no PRIMARY row.
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> (the V151 <c>group_id</c> shape): a row collected before this rung never asked
    /// the engine, so NULL is the honest value, and a reader treats NULL as "primary not known". <b>Appended LAST</b>, after
    /// <c>group_id</c>: the writer is a positional binary COPY, so a fresh store (the generated V1 table carries the column from the
    /// collector's payload, and this ALTER no-ops there) and an upgraded store share one physical column order. The table has no
    /// passthrough view, so the rung is one ALTER. An older build ignores the column.</para>
    /// </summary>
    private const string V202Sql = @"
ALTER TABLE collect.ag_replica_states ADD COLUMN IF NOT EXISTS primary_replica text;";

    /// <summary>
    /// V205 (#6235): an optional expiry on a custom alert rule, the same column name and meaning as
    /// <c>config_mute_rules.expires_at_utc</c>. <c>expires_at_utc</c> is a nullable naive-UTC <c>timestamp</c>; NULL means the rule
    /// never expires, which is every existing row. "Expired" is computed (<c>expires_at_utc &lt;= now</c>), never stored, so no job
    /// has to flip anything. An expired rule is not evaluated and does not fire; its row stays, and moving or clearing the expiry
    /// turns it back on. A nullable column with no default is catalog-only. An older build ignores the column and keeps evaluating
    /// the rule. Idempotent.
    /// </summary>
    private const string V205Sql = @"
ALTER TABLE config.custom_alert_rules ADD COLUMN IF NOT EXISTS expires_at_utc timestamp;";

    /// <summary>
    /// V203 (#6170): the per-server login the PostgreSQL hypothetical-index check runs under, a pair in the same shape as the
    /// remediation pair V113 added. <c>hypothetical_index_username</c> is a login name. <c>hypothetical_index_encrypted_password</c>
    /// holds an <c>env:</c> or <c>file:</c> reference to its password, never the password. Both are nullable with no default and no
    /// fallback: a server with neither has the check refused, and the check never runs under the monitoring login. A new column
    /// pair needs no backfill, so each <c>ADD COLUMN</c> is catalog-only. An older build ignores the columns. Idempotent.
    /// </summary>
    private const string V203Sql = @"
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS hypothetical_index_username text;
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS hypothetical_index_encrypted_password text;";

    /// <summary>
    /// V206 (#6227): <c>config.config_collector_schedules.enabled</c> may be NULL, which reads "inherit": the fleet-wide row's value
    /// for a server's row, then the collector's shipped default. A row the schedule tools create to set only a frequency or a
    /// retention then stores no enabled state of its own, so a later fleet-wide disable still reaches that server; before, such a row
    /// stored <c>enabled</c> explicitly and the fleet-wide value never reached it. Dropping NOT NULL is catalog-only, the DEFAULT TRUE
    /// stays (a row inserted without the column is explicit, as every row was), and existing rows keep their values. The service
    /// and the viewer read NULL as "inherit" (<c>ScheduleOverride.Enabled</c> was already nullable and layers with <c>??</c>). An older
    /// build reads the column as a plain boolean and fails on a NULL, so before rolling back past this rung set every NULL to the value
    /// it inherits, or delete those rows (the rollback list in the README has the statement).
    ///
    /// <para><b>The table takes only what the schedule tools write.</b> The tools store a collector name in the catalog's own spelling
    /// (lower-case letters, digits and underscores) and values inside <c>CollectorScheduleDefaults.MaxFrequencyMinutes</c> and
    /// <c>MaxRetentionDays</c>; the unique indexes compare the name exactly, so a differently-cased name was a second row that the
    /// readers resolved in no fixed order. First the rung tidies what is already stored: where one scope holds the same name in more than
    /// one case, the lower-case row stays (else the first by name) and the others go; a remaining mixed-case name becomes lower-case; a
    /// name that is still not a lower-case identifier of at most 64 characters names no collector and goes; a value over the cap is
    /// set to the cap (a retention is already read as the cap; a frequency over a year was a collector that effectively never runs). Then three CHECK constraints, added <c>NOT VALID</c> so they bind every
    /// later INSERT and UPDATE whatever the role, and the rung holds no row they would refuse. The viewer's save deletes a scope's rows
    /// and inserts them again, so a stored row the checks refuse would fail that save: that is why the tidy comes first.</para>
    ///
    /// <para><b>No foreign key to the registry.</b> The viewer lists servers from the observed <c>servers</c> table, which keeps a server
    /// after its definition in <c>config_monitored_servers</c> is removed and is the only list on a store that was never seeded, and the
    /// schedule tools resolve against the same table, so a schedule row for an id with no definition is a normal write and a foreign key
    /// would refuse it. Instead a row-level BEFORE INSERT trigger on <c>config_monitored_servers</c> clears the schedule rows held under
    /// a server id the service has never seen: no definition in <c>config_monitored_servers</c> and no row in the observed
    /// <c>collect.servers</c>, so a row written ahead of an unknown server being registered never decides how that server is collected.
    /// It does nothing when the id already has a definition (an upsert, the seed) or an observed row, so a server that was removed and
    /// is added again keeps the schedule rows the operator set for it (a collector switched off stays off). The trigger
    /// function is <c>SECURITY DEFINER</c> with a pinned search_path, because the roles that register a server cannot delete schedule
    /// rows; it deletes by the new row's server id and nothing else.</para>
    ///
    /// <para>Idempotent: every step is guarded or is a no-op the second time.</para>
    /// </summary>
    private const string V206Sql = @"
ALTER TABLE config.config_collector_schedules ALTER COLUMN enabled DROP NOT NULL;

DELETE FROM config.config_collector_schedules
WHERE ctid IN
(
    SELECT d.row_id
    FROM
    (
        SELECT
            s.ctid AS row_id,
            row_number() OVER (
                PARTITION BY s.server_id, lower(s.collector_name)
                ORDER BY (s.collector_name = lower(s.collector_name)) DESC, s.collector_name) AS keep_rank
        FROM config.config_collector_schedules AS s
    ) AS d
    WHERE d.keep_rank > 1
);
UPDATE config.config_collector_schedules SET collector_name = lower(collector_name) WHERE collector_name <> lower(collector_name);
DELETE FROM config.config_collector_schedules WHERE collector_name !~ '^[a-z0-9_]{1,64}$';
UPDATE config.config_collector_schedules SET frequency_minutes = 525600 WHERE frequency_minutes > 525600;
UPDATE config.config_collector_schedules SET retention_days = 3650 WHERE retention_days > 3650;

DO $limits$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND conname = 'ck_config_collector_schedules_name') THEN
        ALTER TABLE config.config_collector_schedules
            ADD CONSTRAINT ck_config_collector_schedules_name CHECK (collector_name ~ '^[a-z0-9_]{1,64}$') NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND conname = 'ck_config_collector_schedules_frequency_cap') THEN
        ALTER TABLE config.config_collector_schedules
            ADD CONSTRAINT ck_config_collector_schedules_frequency_cap CHECK (frequency_minutes <= 525600) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'config.config_collector_schedules'::regclass AND conname = 'ck_config_collector_schedules_retention_cap') THEN
        ALTER TABLE config.config_collector_schedules
            ADD CONSTRAINT ck_config_collector_schedules_retention_cap CHECK (retention_days <= 3650) NOT VALID;
    END IF;
END
$limits$;

CREATE OR REPLACE FUNCTION config.config_clear_unregistered_schedules() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $clear$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM config.config_monitored_servers AS m WHERE m.server_id = NEW.server_id)
       AND NOT EXISTS (SELECT 1 FROM collect.servers AS o WHERE o.server_id = NEW.server_id) THEN
        DELETE FROM config.config_collector_schedules WHERE server_id = NEW.server_id;
    END IF;
    RETURN NEW;
END;
$clear$;
REVOKE ALL ON FUNCTION config.config_clear_unregistered_schedules() FROM PUBLIC;
DROP TRIGGER IF EXISTS trg_clear_unregistered_schedules ON config.config_monitored_servers;
CREATE TRIGGER trg_clear_unregistered_schedules
    BEFORE INSERT ON config.config_monitored_servers
    FOR EACH ROW EXECUTE FUNCTION config.config_clear_unregistered_schedules();";

    /// <summary>
    /// V204 (#6214): shared folders, personal views and an owner on the saved custom views and notebooks (both are
    /// <c>config.custom_views</c> rows). Four columns: <c>folder</c> (NULL is the top level; the writers store it trimmed, empty as NULL,
    /// at most 100 characters, no control characters, and a check holds the same rule), <c>personal</c> (default false, so every existing
    /// view stays shared) and <c>created_by</c> (NULL for every existing view, which reads as "unknown"; <c>updated_by</c> is NOT copied
    /// into it, because that is who last saved a view, not who made it) and <c>owner_key</c> (NULL for every existing view and for every view
    /// the shared token or MCP made). <c>created_by</c> is a name for display only: it is what a person's sign-in called them, it can change,
    /// and an identity provider can hand the same name to somebody else, so it never decides who may touch a personal view. <c>owner_key</c>
    /// does: the web host stamps base64url(SHA-256(issuer + newline + sub)) of the validated ID token, which is stable and is not reused. A
    /// check refuses a personal view with no owner key.
    ///
    /// <para><b>Uniqueness moves from the store to the scope.</b> V31's <c>UNIQUE (name)</c> is dropped under whatever name it carries
    /// (looked up by its column, not assumed). Two partial unique indexes replace it: shared views are unique on
    /// <c>(COALESCE(folder, ''), name)</c>, personal views on <c>(owner_key, name)</c>. Names still compare exactly (case-sensitive), as
    /// before. A view's folder does not take part in a personal view's uniqueness.</para>
    ///
    /// <para>Every step is guarded, so a second run changes nothing. An older build keeps working against the new shape: it writes top-level
    /// shared views, which the new indexes treat as before, and ignores the columns. It does not hide a person's personal views from the
    /// list. Grants need no change: the viewer and mcp roles hold table-level INSERT, UPDATE and DELETE on the table, which cover new columns.</para>
    /// </summary>
    private const string V204Sql = @"
ALTER TABLE config.custom_views ADD COLUMN IF NOT EXISTS folder text;
ALTER TABLE config.custom_views ADD COLUMN IF NOT EXISTS personal boolean NOT NULL DEFAULT false;
ALTER TABLE config.custom_views ADD COLUMN IF NOT EXISTS created_by text;
ALTER TABLE config.custom_views ADD COLUMN IF NOT EXISTS owner_key text;

DO $$
DECLARE
    unique_name text;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'config.custom_views'::regclass AND conname = 'ck_custom_views_folder')
    THEN
        ALTER TABLE config.custom_views ADD CONSTRAINT ck_custom_views_folder CHECK (
            folder IS NULL
            OR (folder = btrim(folder) AND folder <> '' AND char_length(folder) <= 100 AND folder !~ '[--]'));
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'config.custom_views'::regclass AND conname = 'ck_custom_views_personal_owner')
    THEN
        ALTER TABLE config.custom_views ADD CONSTRAINT ck_custom_views_personal_owner CHECK (NOT personal OR owner_key IS NOT NULL);
    END IF;

    /* V31's UNIQUE (name), found by the one column it covers so a renamed constraint is dropped too. */
    FOR unique_name IN
        SELECT con.conname
        FROM pg_constraint con
        WHERE con.conrelid = 'config.custom_views'::regclass
          AND con.contype = 'u'
          AND con.conkey = ARRAY[(SELECT a.attnum FROM pg_attribute a
                                  WHERE a.attrelid = 'config.custom_views'::regclass AND a.attname = 'name')]
    LOOP
        EXECUTE format('ALTER TABLE config.custom_views DROP CONSTRAINT %I', unique_name);
    END LOOP;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_views_shared_name ON config.custom_views ((COALESCE(folder, '')), name) WHERE NOT personal;
CREATE UNIQUE INDEX IF NOT EXISTS ux_custom_views_personal_name ON config.custom_views (owner_key, name) WHERE personal;";

    /// <summary>
    /// V208 (#6231): <c>config.config_mute_rules.starts_at_utc</c>, the moment a mute rule begins to apply. A quiet window for planned
    /// work is a mute rule with only a server scope (or none, for the fleet) and an end time; what it lacked was a START, so work
    /// planned for tonight could not be set up this afternoon.
    ///
    /// <para><b>NULL means the rule applies from the moment it exists</b>, exactly as every stored rule did, so there is no backfill.
    /// Nullable, no DEFAULT, one catalog-only <c>ADD COLUMN</c>. Needs no GRANT (the roles' table-level grants cover a new column)
    /// and no trigger (the V117 reload beacon fires on any change to the table). An older build ignores the column and would apply a
    /// rule that has not started yet, so a window set ahead of its start mutes early on a rolled-back build.</para>
    /// </summary>
    private const string V208Sql = @"
ALTER TABLE config.config_mute_rules ADD COLUMN IF NOT EXISTS starts_at_utc timestamp;";

    /// <summary>
    /// V209 (#6211): <c>config.config_service.query_store_fetch_top_n</c>, how many plans per database-hour and metric the Query Store collector
    /// fetches the text and plan XML of. Default 100 (the measured knee: about 86% of new plans' CPU for about a third of the fetches). 0 or less
    /// means every new plan, the behaviour before this rung. Read live each cycle, so a change needs no restart. One catalog-only <c>ADD COLUMN</c>
    /// with a constant default; no backfill, no GRANT (the roles' table-level SELECT covers a new column; the Viewer's settings save writes it as the admin role, whose
    /// table-level UPDATE on the config schema covers it, while the viewer role holds only <c>UPDATE (config_version, updated_at)</c> on this table) and no trigger (the config_version trigger fires on any row update).
    /// </summary>
    private const string V209Sql = @"
ALTER TABLE config.config_service ADD COLUMN IF NOT EXISTS query_store_fetch_top_n integer NOT NULL DEFAULT 100;";

    /// <summary>
    /// V213 (#6254): two nullable text columns. <c>config.config_monitored_servers.query_store_replica</c> is the host (or listener
    /// name) Query Store is read from instead of the server itself; NULL, the default for every existing row, keeps today's behaviour.
    /// <c>collect.collection_log.read_source</c> says which host a collection read came from; NULL on every existing row. Neither has a
    /// default and neither needs a backfill, so each <c>ADD COLUMN</c> is catalog-only. An older build ignores both columns and reads
    /// every row as it did before, so there is no rollback line. Idempotent.
    /// </summary>
    private const string V213Sql = @"
ALTER TABLE config.config_monitored_servers ADD COLUMN IF NOT EXISTS query_store_replica text;
ALTER TABLE collect.collection_log ADD COLUMN IF NOT EXISTS read_source text;

/* The log lives in the collect schema, and its passthrough view freezes its column list at CREATE (the V80 lesson), so every read
   of the log (the MCP tools, the web page, the Viewer) would never see read_source without this. Appending is the one alteration
   CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN produces. */
CREATE OR REPLACE VIEW collect.v_collection_log AS SELECT * FROM collect.collection_log;";

    /// <summary>
    /// V210 (#6281, part of #5632): the MCP tag writes may not change which servers an agent route covers. A tag-scoped route covers
    /// every registered server mapped to its tag or to any descendant of it (<c>NotificationRouteScopeExpander</c>), and
    /// <c>update_notification_route</c> refuses over MCP to change an agent route's scope, so the tag tools, which write
    /// <c>config.server_tags</c> and <c>config.server_tag_map</c>, could not be left to change the same set.
    ///
    /// <para><b>Which sessions.</b> A session whose login is <c>mcp</c>, or whose session setting <c>darling.surface</c> is
    /// <c>mcp</c>: the MCP host sets that setting inside each write's own transaction (transaction-local, so it ends with the commit or the rollback and a pooled connection never keeps it), so an MCP that connects as the store's owner (no
    /// <c>postgres.mcpConnectionString</c>), or as another login, is held to the rule too. The Viewer, the web dashboard and the
    /// service carry neither and are not held to it. The login test is on <c>session_user</c>, which a non-superuser cannot change,
    /// because the function is SECURITY DEFINER (it reads the route table, which <c>mcp</c> may not).</para>
    ///
    /// <para><b>The rule.</b> A row trigger on each table refuses with SQLSTATE <c>DL403</c>, naming the route, when any tag the
    /// write could reach is the scope tag of a route whose <c>agent_level</c> is set (0, Off, included: an Off route is how a person
    /// keeps servers away from a broader agent route, the same test the route rule uses). It does not work out which route wins. For
    /// an assignment or removal of a server, the tag and every tag above it. For a delete of a tag, or a move of it to another
    /// parent, the tag, every tag under it, and every tag above its old parent and above its new parent. A server that is not
    /// registered, or has been removed, counts like any other: its tag rows come back with it when it is added again (the id is a
    /// hash), so <c>remove_server</c> over MCP is refused for a server mapped under an agent route's tag. A rename, a colour or a
    /// sort-order change does not touch coverage and is not looked at. No new column, no backfill, no grant.</para>
    /// </summary>
    private const string V210Sql = @"
CREATE OR REPLACE FUNCTION config.agent_route_tag_guard()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $guard$
DECLARE
    v_route integer;
    v_up integer[];
    v_down integer[];
BEGIN
    IF session_user::text IS DISTINCT FROM 'mcp'
       AND COALESCE(pg_catalog.current_setting('darling.surface', true), '') <> 'mcp' THEN
        RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
    END IF;

    v_up := ARRAY[]::integer[];
    v_down := ARRAY[]::integer[];
    IF TG_TABLE_NAME = 'server_tag_map' THEN
        IF TG_OP IN ('INSERT', 'UPDATE') THEN
            v_up := v_up || NEW.tag_id;
        END IF;
        IF TG_OP IN ('DELETE', 'UPDATE') THEN
            v_up := v_up || OLD.tag_id;
        END IF;
    ELSIF TG_OP = 'DELETE' THEN
        v_down := ARRAY[OLD.id];
        IF OLD.parent_id IS NOT NULL THEN
            v_up := ARRAY[OLD.parent_id];
        END IF;
    ELSIF NEW.parent_id IS DISTINCT FROM OLD.parent_id THEN
        v_down := ARRAY[OLD.id];
        IF OLD.parent_id IS NOT NULL THEN
            v_up := v_up || OLD.parent_id;
        END IF;
        IF NEW.parent_id IS NOT NULL THEN
            v_up := v_up || NEW.parent_id;
        END IF;
    END IF;

    IF cardinality(v_up) + cardinality(v_down) > 0 THEN
        WITH RECURSIVE above(id, parent_id) AS (
            SELECT t.id, t.parent_id FROM config.server_tags AS t WHERE t.id = ANY(v_up)
            UNION
            SELECT p.id, p.parent_id FROM config.server_tags AS p JOIN above AS a ON p.id = a.parent_id),
        below(id) AS (
            SELECT t.id FROM config.server_tags AS t WHERE t.id = ANY(v_down)
            UNION
            SELECT c.id FROM config.server_tags AS c JOIN below AS b ON c.parent_id = b.id)
        SELECT r.route_id INTO v_route
        FROM config.config_notification_routes AS r
        WHERE r.agent_level IS NOT NULL
          AND (r.scope_tag_id IN (SELECT a.id FROM above AS a) OR r.scope_tag_id IN (SELECT b.id FROM below AS b))
        ORDER BY r.route_id
        LIMIT 1;
        IF v_route IS NOT NULL THEN
            RAISE EXCEPTION 'Route % has an agent setting (Off included) and its tag is this tag, or is above or below it, so changes to which servers it covers are made in the Viewer or on the web dashboard, not over MCP.', v_route
                USING ERRCODE = 'DL403';
        END IF;
    END IF;

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$guard$;
REVOKE ALL ON FUNCTION config.agent_route_tag_guard() FROM PUBLIC;

DROP TRIGGER IF EXISTS trg_agent_route_tag_guard ON config.server_tag_map;
CREATE TRIGGER trg_agent_route_tag_guard
    BEFORE INSERT OR UPDATE OR DELETE ON config.server_tag_map
    FOR EACH ROW EXECUTE FUNCTION config.agent_route_tag_guard();
ALTER TABLE config.server_tag_map ENABLE ALWAYS TRIGGER trg_agent_route_tag_guard;

DROP TRIGGER IF EXISTS trg_agent_route_tag_guard ON config.server_tags;
CREATE TRIGGER trg_agent_route_tag_guard
    BEFORE UPDATE OR DELETE ON config.server_tags
    FOR EACH ROW EXECUTE FUNCTION config.agent_route_tag_guard();
ALTER TABLE config.server_tags ENABLE ALWAYS TRIGGER trg_agent_route_tag_guard;";

    /// <summary>
    /// V211 (#5736): the two multiples of the relative Client Timeouts bars, on the singleton <c>config_alert_settings</c> row beside the two
    /// bars they scale. Warning is max(<c>client_timeouts_warning_per_minute</c>, <c>client_timeouts_warning_multiple</c> x the server's baseline) and
    /// Critical is the same with the Critical pair, the baseline being the server's own p99.9 of its 3-minute average attention rate over the last 7 days.
    /// The two existing columns keep their names and become the FLOORS (their defaults, 10 and 60, are unchanged). Two <c>integer NOT NULL</c> columns with
    /// constant defaults (3 and 10), catalog-only, no backfill, no GRANT (the roles hold table-level privileges on the config schema) and no trigger (the
    /// statement-level <c>trg_bump_alert_settings</c> already bumps <c>config_version</c> on any write here). The baseline itself needs no storage: the
    /// service computes it from <c>collection_log</c> and <c>attention_events</c> and keeps it in memory for a day.
    /// </summary>
    private const string V211Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS client_timeouts_warning_multiple integer NOT NULL DEFAULT 3,
    ADD COLUMN IF NOT EXISTS client_timeouts_critical_multiple integer NOT NULL DEFAULT 10;";
    /// V212 (#6285, #6339): <c>collect.servers.machine_identity</c>, the name the monitored machine gives itself, written on every
    /// connect. For a SQL Server it is <c>SERVERPROPERTY('ServerName')</c> folded to lower case (NULL on an Azure SQL Database, which
    /// names no machine of its own); for PostgreSQL it is the cluster's <c>system_identifier</c>, a colon and the server port (NULL
    /// when the monitoring role may not call <c>pg_control_system()</c>). <c>add_servers</c> and <c>edit_server</c> over MCP compare
    /// what a new connection reports with this column, so a machine reached under a second name is recognised. It lives on the
    /// registry, which no retention purge touches and which keeps a removed registration's row, so the identity of a removed
    /// registration stays. No backfill: a registration fills its own on its next connect, and until then it is matched by its
    /// text alone. No view refresh: <c>collect.servers</c> is a registry, not a hypertable.
    /// </summary>
    private const string V212Sql = @"
ALTER TABLE collect.servers
    ADD COLUMN IF NOT EXISTS machine_identity text;";
    /// <summary>
    /// V214 (#6334): <c>config.config_known_patterns</c>, the operator's known-pattern entries ("the backup job pushes High CPU every night
    /// at 02:00"), shown beside the alerts they match. Context only: an entry never mutes, never pages and never changes an alert.
    ///
    /// <para>Every scope column is nullable and NULL means any: <c>server_id</c> (every server), <c>metric_name</c> (every metric, the name
    /// alert history holds), <c>time_of_day</c> (any time). <c>time_of_day</c> is a wall-clock time in <c>time_zone</c> (an IANA id; NULL is
    /// UTC) and matches within <c>window_minutes</c> either side, wrapping past midnight; the shared
    /// <c>KnownPatternMatcher</c> in PerformanceMonitor.Common is the one place that rule lives. <c>expires_at_utc</c> is a naive UTC
    /// <c>timestamp</c> like the mute rules' (NULL never expires). <c>note</c> is operator-written text, held to 1-500 characters by a
    /// CHECK so a write that skips the store's function still cannot store more. Further CHECKs hold every column to what a reader can
    /// load, so a direct call to a write function (or the owner's own SQL) cannot store a row that breaks the read of every entry: a
    /// time of day below 24:00, a finite expiry before year 10000, a time zone that is a bounded IANA-shaped id (the shape only: the
    /// zone's existence is the matcher's call, because PostgreSQL's names and .NET's differ at the edges), a metric name of at most 128
    /// characters, an author of at most 256 and a note with some non-blank text. <c>updated_by</c> and <c>updated_at_utc</c> are NULL
    /// until an edit; only <c>config.update_known_pattern</c> sets them.</para>
    ///
    /// <para><b>The table joins the <c>config_version</c> reload beacon</b> with V117's statement-level trigger on V17's shared
    /// <c>config.config_bump_version()</c> function, so a write from any path (the web editor, the Viewer, hand-written SQL) moves the
    /// beacon and a service that caches the entries re-reads them. No GRANT here: the readers' schema-wide SELECT covers a new
    /// <c>config</c> table, and the writes go through owner-rights functions that provisioning grants to the operator roles. Additive;
    /// an older build never reads the table.</para>
    /// </summary>
    private const string V214Sql = @"
CREATE TABLE IF NOT EXISTS config.config_known_patterns
(
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_id integer,
    metric_name text,
    time_of_day time,
    time_zone text,
    window_minutes integer NOT NULL DEFAULT 30,
    note text NOT NULL,
    created_by text,
    created_at_utc timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    expires_at_utc timestamp,
    updated_by text,
    updated_at_utc timestamp,
    CONSTRAINT ck_known_patterns_note CHECK (char_length(note) BETWEEN 1 AND 500 AND btrim(note, E' 	
') <> ''),
    CONSTRAINT ck_known_patterns_window CHECK (window_minutes BETWEEN 0 AND 720),
    CONSTRAINT ck_known_patterns_time_of_day CHECK (time_of_day < '24:00'),
    CONSTRAINT ck_known_patterns_expires CHECK (expires_at_utc IS NULL OR (isfinite(expires_at_utc) AND expires_at_utc < '10000-01-01')),
    CONSTRAINT ck_known_patterns_time_zone CHECK (time_zone IS NULL OR (char_length(time_zone) <= 64 AND time_zone ~ '^[A-Za-z0-9_+-]+(/[A-Za-z0-9_+-]+)*$')),
    CONSTRAINT ck_known_patterns_metric CHECK (char_length(metric_name) <= 128),
    CONSTRAINT ck_known_patterns_created_by CHECK (char_length(created_by) <= 256),
    CONSTRAINT ck_known_patterns_updated_by CHECK (char_length(updated_by) <= 256)
);

DROP TRIGGER IF EXISTS trg_bump_known_patterns ON config.config_known_patterns;
CREATE TRIGGER trg_bump_known_patterns
    AFTER INSERT OR UPDATE OR DELETE ON config.config_known_patterns
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();";

    /// <summary>
    /// V215: six indexes on the investigation ("agent") tables. A restored store copy loaded with one synthetic year of requests at a
    /// production store's measured rate (73,000 requests) timed every hot read of these tables. Most stayed under 1 ms. These did not, and
    /// each index below took its read to well under 10 ms: the follow-up module read (<c>ProcedureStatsModuleDates.WatchedSql</c>, about
    /// 60,000 calls a day) 127 ms to 5.5 ms with the first two, the due follow-ups read 35 ms to 0.7 ms with the third and second,
    /// <c>PendingFollowUpsSql</c> 25 ms to 5 ms with the second, the MCP request list and the reports 30-day read 25-53 ms to under
    /// 0.3 ms with the fourth, the brief's earlier-report and prior-links lookups and the live-practice hider 12-24 ms to under 0.25 ms
    /// with the fifth, and the retention delete of requests (the <c>fk_investigation_reports_family</c> check scanned every report per
    /// deleted request) 310 ms for 8 rows to 0.5 ms with the sixth.
    ///
    /// <para>Each partial predicate is implied by the WHERE clause of the read it serves, so the planner can use it: an equality on
    /// <c>follow_up_of</c>, <c>practice_of</c> or <c>family_of</c> implies NOT NULL, and the follow-up reads state
    /// <c>state = 'reported'</c> and the NOT NULL columns themselves. Plain <c>CREATE INDEX IF NOT EXISTS</c>, not CONCURRENTLY: the tables
    /// hold hundreds of rows on a production store today, so each build is instant and takes no noticeable lock. The shipped V184 text
    /// (<c>InvestigationTables.CreateSql</c>) is not edited, and a fresh store runs every rung, so it gets these too. Additive: an older build
    /// never reads the indexes, and no GRANT is needed for an index.</para>
    /// </summary>
    private const string V215Sql = @"
CREATE INDEX IF NOT EXISTS ix_investigation_requests_reported_condition
    ON config.investigation_requests (server_id) WHERE state = 'reported' AND follow_up_condition IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_investigation_requests_follow_up_of
    ON config.investigation_requests (follow_up_of) WHERE follow_up_of IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_investigation_requests_follow_up_at
    ON config.investigation_requests (follow_up_at) WHERE state = 'reported' AND follow_up_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_investigation_requests_opened_at
    ON config.investigation_requests (opened_at);
CREATE INDEX IF NOT EXISTS ix_investigation_requests_practice_of
    ON config.investigation_requests (practice_of) WHERE practice_of IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_investigation_reports_family_of
    ON config.investigation_reports (family_of) WHERE family_of IS NOT NULL;";

    /// <summary>
    /// V216 (#5736): the Client Timeouts alert ships ON. V179 added <c>config.config_alert_settings.client_timeouts_enabled</c> with
    /// <c>DEFAULT FALSE</c> so the alert was off until its bars could be checked against captured data. #5736 made the bars relative to each
    /// server's own baseline, and a 7-day replay of the new bars over one production store of 42 SQL Servers would have paged twice, both at
    /// Warning, with 40 of the 42 servers never crossing a bar. Two statements. The column default becomes TRUE, so a store that has no row yet
    /// (or a new one) starts with the alert on. And the one row a store has is moved from FALSE to TRUE.
    ///
    /// <para><b>Why moving the row is right.</b> Nothing RECORDED "off" as a choice distinct from the default until now. Both Settings windows and <c>update_alert_settings</c> have offered the checkbox since #5640, but the column stores a boolean with no provenance. A stored FALSE is the
    /// old default, written by V179 or by a Settings save that sent the whole row back, and a store cannot tell it from an operator's choice. The
    /// rung says so rather than guessing: an operator who turned the alert off on purpose switches it off again, once. The repo has no
    /// settings-change journal to write to, so the reason is stated here, in the Darling README's V216 rollback note, in docs/alerts.md
    /// for both editions, and in this change's CHANGELOG entry, which joins the release notes at the release cut.</para>
    ///
    /// <para><b>Cost.</b> <c>config_alert_settings</c> is the one-row singleton (<c>id = 1</c>), so the UPDATE touches at most one row and the
    /// SET DEFAULT is catalog-only. Neither is given a <c>lock_timeout</c>, the shape V147 and V211 used on the same kind of table: the only thing
    /// the ALTER can wait on is a transaction holding this one-row table, which is a millisecond read, and the applier's command timeout is the
    /// backstop. The UPDATE fires V17's statement trigger, which bumps <c>config_version</c>, so a running service reloads the setting at once.
    /// Re-running changes nothing: the column default is already TRUE and no row is FALSE. Written as literals because a migration's text is frozen
    /// at the version it ships in. An older build reads the same boolean column and is unaffected.</para>
    /// </summary>
    private const string V216Sql = @"
ALTER TABLE config.config_alert_settings ALTER COLUMN client_timeouts_enabled SET DEFAULT TRUE;
UPDATE config.config_alert_settings SET client_timeouts_enabled = TRUE WHERE client_timeouts_enabled = FALSE;";

    /// <summary>
    /// V217 (#6581) - <c>collect.query_store_hints</c>: the Query Store hints in place, read from <c>sys.query_store_query_hints</c> by
    /// <c>QueryStoreHintsCollector</c> (SQL Server 2022 and later) every five minutes, one row per hint plus ONE all-NULL marker row
    /// (<c>query_hint_id</c> NULL) per captured database per pass, so a hint that left a captured database reads as CLEARED, not as not
    /// captured. The forced-plans table's model: a PLAIN table on every store (<c>StaysPlainTable</c>), 30-day time-sliced DELETE. Columns
    /// and the (server_id, collection_time) index are exactly <c>PgSchemaGenerator</c>'s output, so a fresh store (V1's catalog walk) and an
    /// upgraded one agree. The marker index is the forced-plans one (<c>idx_query_store_forced_plans_markers</c>): it finds a database's
    /// previous complete capture by one seek. New table, no data moves, no grant (the <c>collect</c> schema carries blanket SELECT).
    /// Every statement is idempotent.
    /// </summary>
    private const string V217Sql = @"
CREATE TABLE IF NOT EXISTS collect.query_store_hints (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    query_hint_id bigint,
    query_id bigint,
    query_hint_text text,
    last_query_hint_failure_reason text,
    query_hint_failure_count bigint,
    source_desc text
);

CREATE INDEX IF NOT EXISTS idx_query_store_hints_time ON collect.query_store_hints(server_id, collection_time);

CREATE INDEX IF NOT EXISTS idx_query_store_hints_markers
    ON collect.query_store_hints (server_id, database_name, collection_time) WHERE query_hint_id IS NULL;";

    /// <summary>
    /// V222 (#6874) - <c>collect.agent_alerts</c>: the SQL Server Agent alert definitions and their counters, read from
    /// <c>msdb.dbo.sysalerts</c> (with the operator count from <c>msdb.dbo.sysnotifications</c>) by <c>AgentAlertsCollector</c> every 15
    /// minutes. One row per alert per pass, so the change in <c>occurrence_count</c> between two passes says which alert fired in a window.
    /// An instance with no alerts writes ONE marker row whose <c>alert_id</c> is NULL, so "collected, none defined" differs from "never
    /// collected". A PLAIN table on every store (<c>StaysPlainTable</c>, tens of rows per pass), 30-day time-sliced DELETE. Columns and the
    /// (server_id, collection_time) index are exactly <c>PgSchemaGenerator</c>'s output, so a fresh store (V1's catalog walk) and an upgraded
    /// one agree. New table, no data moves, no grant (the <c>collect</c> schema carries blanket SELECT). Every statement is idempotent. An
    /// older build never reads the table and is unaffected.
    /// The same rung adds <c>config.config_alert_settings.agent_alert_gaps_enabled</c> (<c>boolean NOT NULL DEFAULT FALSE</c>, catalog-only): the "Agent Alert Gaps"
    /// alert ships OFF. An older build never reads the column.
    /// </summary>
    private const string V222Sql = @"
CREATE TABLE IF NOT EXISTS collect.agent_alerts (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    alert_id integer,
    alert_name text,
    enabled boolean,
    event_source text,
    message_id integer,
    severity integer,
    database_name text,
    event_description_keyword text,
    performance_condition text,
    delay_between_responses integer,
    last_occurrence timestamp,
    last_response timestamp,
    occurrence_count integer,
    count_reset timestamp,
    has_notification integer,
    operator_count integer,
    response_job_id text
);

CREATE INDEX IF NOT EXISTS idx_agent_alerts_time ON collect.agent_alerts(server_id, collection_time);

ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS agent_alert_gaps_enabled boolean NOT NULL DEFAULT FALSE;";

    /// <summary>
    /// V218 (#6569): <c>ck_config_server_actions_action</c> also allows <c>recompile_module</c> (the switch of the Level 3 action that
    /// runs <c>sys.sp_recompile</c> on a procedure, function or trigger), <c>create_plan_guide</c> and <c>drop_plan_guide</c> (the plan guide actions). The CHECK is replaced in place under its own name, only when
    /// the stored definition does not already name the newest action (V196's guard shape), so a second run replaces nothing. It inserts no row:
    /// the action starts off on every server. An older build never writes the new word and never reads it as anything else.
    /// </summary>
    private const string V218Sql = @"
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'ck_config_server_actions_action'
                 AND conrelid = 'config.config_server_actions'::regclass
                 AND pg_get_constraintdef(oid) !~ 'drop_plan_guide') THEN
        ALTER TABLE config.config_server_actions DROP CONSTRAINT ck_config_server_actions_action;
        ALTER TABLE config.config_server_actions
            ADD CONSTRAINT ck_config_server_actions_action CHECK (action IN ('force_plan', 'evict_plan', 'kill_session', 'recompile_module', 'create_plan_guide', 'drop_plan_guide'));
    END IF;
END $do$;";

    /// <summary>
    /// V219 (#6750 part 3): <c>collect.query_store_self_keys</c>, the keys of the monitor's own Query Store statements, and
    /// <c>collect.query_store_self_keys_state</c>, the one-row record that their backfill has run (<see cref="QueryStoreSelfKeys"/>),
    /// plus two row triggers on <c>collect.query_store_text</c> that keep the key table current for every writer of text, an older
    /// build's included (<see cref="QueryStoreSelfKeys.TriggerSql"/>). The keys of text stored earlier are backfilled in the background
    /// after start, because that is one scan of the TOAST-heavy text table and could pass the migration silence limit inside the startup
    /// transaction. Both tables are new and empty, and <c>CREATE TRIGGER</c> reads no row, so the rung moves no data. The lock timeout
    /// is <see cref="MigrationCommandTimeoutSeconds"/> less 20 s, as V173's, because <c>CREATE TRIGGER</c> needs a SHARE ROW EXCLUSIVE
    /// lock on the text table, which an older instance sharing the store can be writing. Plain tables, no GRANT (the <c>collect</c>
    /// schema's blanket SELECT and its default privileges cover a table a migration introduces, as V168 notes). Every statement is
    /// idempotent. <b>No Lite twin:</b> Lite keeps the text inline and the collector drops the monitor's own row there.
    /// </summary>
    private static readonly string V219Sql =
        "SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + "s';\n" + QueryStoreSelfKeys.CreateSql + "\n" + QueryStoreSelfKeys.TriggerSql;

    /// <summary>
    /// V220 (#6798): <c>idx_job_history_job_success</c> on <c>collect.job_history (server_id, job_id, run_datetime DESC) INCLUDE
    /// (run_duration_seconds, collection_time) WHERE step_id = 0 AND run_status = 1</c>, for the Job History read's per-job statistics
    /// (<see cref="DarlingJobHistoryReader.BuildJobHistorySql"/>'s <c>job_stats</c>). That part averages each listed job's successful
    /// outcomes and finds its last one. Through V150's <c>idx_job_history_server_run</c> it read the server's whole run-time range and
    /// dropped the step rows and the other jobs after fetching them from scattered heap pages: on a 210-server copy of a production
    /// store, 13.2 s of a 16.3 s cold read. This partial index holds only the successful outcomes (about 23% of the rows on one
    /// production store), and its INCLUDE columns cover the aggregate and the <c>collection_time</c> floor, so the read seeks the listed
    /// jobs and never visits the heap. A compressed chunk keeps no rows in this index (past 7 days for <c>job_history</c>), so the read
    /// takes those chunks through the compressed batches' <c>server_id</c> segment-by index, once per listed server, as it did before.
    ///
    /// <para><b>Cost, measured on PostgreSQL 17 with TimescaleDB 2.31</b> (42 servers, 9.8M rows in 101 twelve-hour chunks, 98 of them
    /// compressed, the step and status mix of one production store): the build took 0.18 s, because a compressed chunk's index is an
    /// empty shell and only the three uncompressed chunks hold rows. The index is 20 MB on those chunks against V150's 69 MB. A 93,000-row
    /// insert into the newest chunk went from 0.24 s to 0.28 s (median of three), one more btree that only the 23% of rows that are
    /// successful outcomes enter. Compressing a chunk drops its rows from the index, so the size stays bounded by the uncompressed
    /// window. Plain <c>CREATE INDEX IF NOT EXISTS</c>, V150's shape: <c>MigrateAsync</c> runs the rung in one transaction, where
    /// <c>timescaledb.transaction_per_chunk</c> cannot run. The build holds a SHARE lock on the uncompressed chunks, which blocks the
    /// collector's inserts for the build's length, so the lock timeout is <see cref="MigrationCommandTimeoutSeconds"/> less 20 s, as
    /// V173's. A fresh store runs every rung, so it gets the index too. Additive: an older build never reads it, and no GRANT is needed
    /// for an index.</para>
    /// </summary>
    private static readonly string V220Sql =
        "SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + @"s';
CREATE INDEX IF NOT EXISTS idx_job_history_job_success
    ON collect.job_history (server_id, job_id, run_datetime DESC)
    INCLUDE (run_duration_seconds, collection_time)
    WHERE step_id = 0 AND run_status = 1;";

    /// <summary>
    /// V221 (#6800): <c>collect.query_store_hash_hourly</c>, the narrow hourly source of the fleet tag roll-up, its <c>(hour, server_id)</c>
    /// index, and <c>hash_built_seq</c> on <c>collect.query_store_compose_stamp_built</c>. The text, the column map and the reasoning are on
    /// <see cref="QueryStoreComposeStamp.HashHourlyCreateSql"/>. The service's hourly tick (V173's builder) fills the table in the stamp's
    /// transaction and the planner rebuilds every hour whose <c>hash_built_seq</c> differs from <c>late_seq</c>, which is every hour built
    /// before this rung (NULL), so the backfill is the ordinary tick, newest first, six hours a tick. The same text also creates the day
    /// table <c>collect.query_store_hash_daily</c> (#6800, the 7 and 30 day windows), its <c>(day, server_id)</c> index, its ledger
    /// <c>query_store_hash_daily_built</c> and its failure record; <see cref="QueryStoreHashDaily"/> builds the days after the hours of a
    /// tick. <b>The rung scans nothing</b>: four new empty tables, two indexes on them, and a nullable column with no default on the small ledger. The column needs an ACCESS EXCLUSIVE lock
    /// on that ledger, which the wide table's late-row triggers take row locks on, so the lock timeout is
    /// <see cref="MigrationCommandTimeoutSeconds"/> less 20 s, as V173's. Additive: an older build never reads the table, and a build
    /// that rebuilds a stamp hour without writing it leaves the pair stale for the table, which this build then rebuilds.
    /// </summary>
    private static readonly string V221Sql =
        "SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + "s';\n" + QueryStoreComposeStamp.HashHourlyCreateSql;

    /// <summary>
    /// V223 (#6873) - <c>collect.database_protection</c>: per database, the last good CHECKDB and the last full and log backup, read by
    /// <c>DatabaseProtectionCollector</c> every 60 minutes (Darling only). One row per database per pass: the raw times (UTC), the
    /// recovery model, the database state and replica role, a per-row platform label (native, platform_managed, native_only) and a state
    /// word for each of the three facts (never, unknown, none_in_window, not_expected, unavailable), so a reader can tell "no backup in 60
    /// days" from "the platform owns this". The overdue judgment is the reader's. A PLAIN table on every store (<c>StaysPlainTable</c>),
    /// 90-day time-sliced DELETE. Columns and the (server_id, collection_time) index are exactly <c>PgSchemaGenerator</c>'s output, so a
    /// fresh store (V1's catalog walk) and an upgraded one agree. New table, no data moves, no grant (the <c>collect</c> schema carries
    /// blanket SELECT). Every statement is idempotent.
    /// The same rung adds three <c>config.config_alert_settings</c> columns (<c>boolean NOT NULL DEFAULT FALSE</c>, catalog-only):
    /// <c>backup_overdue_enabled</c>, <c>log_backup_overdue_enabled</c> and <c>checkdb_overdue_enabled</c>. The three overdue alerts ship OFF.
    /// An older build never reads the columns.
    /// </summary>
    private const string V223Sql = @"
CREATE TABLE IF NOT EXISTS collect.database_protection (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    state_desc text,
    recovery_model_desc text,
    create_date timestamp,
    replica_role text,
    platform text,
    last_good_checkdb_time timestamp,
    checkdb_state text,
    last_full_backup_time timestamp,
    full_backup_state text,
    last_log_backup_time timestamp,
    log_backup_state text
);

CREATE INDEX IF NOT EXISTS idx_database_protection_time ON collect.database_protection(server_id, collection_time);

ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS backup_overdue_enabled boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS log_backup_overdue_enabled boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS checkdb_overdue_enabled boolean NOT NULL DEFAULT FALSE;";

    /// <summary>
    /// V224 (#6875) - <c>collect.identity_capacity</c>: how full the identity columns are, read from <c>sys.identity_columns</c> by
    /// <c>IdentityCapacityCollector</c> every six hours, per database: the top 20 columns above 10 percent used (value, seed, increment,
    /// type, precision and the percent) plus ONE all-NULL marker row (<c>column_name</c> NULL) per scanned database, so a database with no
    /// high column reads as scanned and clean, and one that was not scanned has no marker. A PLAIN table on every store
    /// (<c>StaysPlainTable</c>), 90-day time-sliced DELETE, at most 21 rows per database per pass. Columns and the (server_id,
    /// collection_time) index are exactly <c>PgSchemaGenerator</c>'s output, so a fresh store (V1's catalog walk) and an upgraded one
    /// agree. The marker index finds a database's newest scan by one seek. New table, no data moves, no grant (the <c>collect</c> schema
    /// carries blanket SELECT). Every statement is idempotent.
    /// The same rung adds three <c>config.config_alert_settings</c> columns for the "Identity Capacity" alert (catalog-only): <c>identity_capacity_enabled</c>
    /// (<c>boolean NOT NULL DEFAULT FALSE</c>, the alert ships OFF) and the warning and critical bars <c>identity_capacity_warning_percent</c> (70) and
    /// <c>identity_capacity_critical_percent</c> (90), the global values a per-server or per-database threshold override replaces. An older build never
    /// reads the columns.
    /// </summary>
    private const string V224Sql = @"
CREATE TABLE IF NOT EXISTS collect.identity_capacity (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    schema_name text,
    table_name text,
    column_name text,
    data_type text,
    type_precision integer,
    last_value numeric(38,0),
    seed_value numeric(38,0),
    increment_value numeric(38,0),
    percent_used double precision,
    skip_reason text
);

CREATE INDEX IF NOT EXISTS idx_identity_capacity_time ON collect.identity_capacity(server_id, collection_time);

CREATE INDEX IF NOT EXISTS idx_identity_capacity_markers
    ON collect.identity_capacity (server_id, database_name, collection_time) WHERE column_name IS NULL;

ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS identity_capacity_enabled boolean NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS identity_capacity_warning_percent integer NOT NULL DEFAULT 70,
    ADD COLUMN IF NOT EXISTS identity_capacity_critical_percent integer NOT NULL DEFAULT 90;";

    /// <summary>
    /// V196 (#5632, step 5): Level 3 and the per-server action switches. <b>Nothing here turns an action on.</b>
    ///
    /// <para><b>Levels.</b> <c>ck_config_notification_routes_agent</c> now allows <c>agent_level</c> NULL and 0 to 3, and
    /// <c>ck_investigation_requests_autonomy</c> allows <c>autonomy_level</c> 1 to 3. Each is replaced in place, under its own
    /// name, only when the stored definition does not already name 3 (V191's guard), so a second run replaces nothing.
    /// Level 3 means a route's request may, once an action exists, act on the monitored server; it still needs the four
    /// conditions of <c>ServerActionGate</c>.</para>
    ///
    /// <para><b>Switches.</b> <c>config.config_server_actions</c>: one row per server and action (<c>force_plan</c>,
    /// <c>evict_plan</c>, <c>kill_session</c>), <c>enabled</c> defaulting to <c>false</c>. <b>No row means off, and this
    /// migration inserts no row.</b> It is deleted with its server (foreign key, <c>ON DELETE CASCADE</c>) and joins the
    /// <c>config_version</c> reload beacon with V117's trigger shape, as V191's overrides do. It is configuration and is never aged.
    /// Grants are provisioning's (managed section 10e, provision-roles.sql step 3j): every role reads it and <b>no role but the owner
    /// writes it</b>, so no MCP tool, web route or <c>darling.json</c> key can turn a switch on. How a person turns one on is a
    /// later decision. <b>No Lite twin:</b> Lite is advise-only.</para>
    /// </summary>
    private const string V196Sql = @"
DO $do$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'ck_config_notification_routes_agent'
                 AND conrelid = 'config.config_notification_routes'::regclass
                 AND pg_get_constraintdef(oid) !~ 'ARRAY\[0, 1, 2, 3\]') THEN
        ALTER TABLE config.config_notification_routes DROP CONSTRAINT ck_config_notification_routes_agent;
        ALTER TABLE config.config_notification_routes
            ADD CONSTRAINT ck_config_notification_routes_agent CHECK (
                (agent_level IS NULL OR agent_level IN (0, 1, 2, 3))
                AND agent_max_per_hour BETWEEN 1 AND 60
                AND agent_max_per_day BETWEEN 1 AND 500
                AND agent_lease_minutes BETWEEN 5 AND 120
                AND agent_expiry_minutes BETWEEN 15 AND 1440);
    END IF;

    IF EXISTS (SELECT 1 FROM pg_constraint
               WHERE conname = 'ck_investigation_requests_autonomy'
                 AND conrelid = 'config.investigation_requests'::regclass
                 AND pg_get_constraintdef(oid) !~ 'ARRAY\[1, 2, 3\]') THEN
        ALTER TABLE config.investigation_requests DROP CONSTRAINT ck_investigation_requests_autonomy;
        ALTER TABLE config.investigation_requests
            ADD CONSTRAINT ck_investigation_requests_autonomy CHECK (autonomy_level IN (1, 2, 3));
    END IF;
END $do$;

CREATE TABLE IF NOT EXISTS config.config_server_actions
(
    server_id integer NOT NULL,
    action text NOT NULL,
    enabled boolean NOT NULL DEFAULT false,
    set_by text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_config_server_actions PRIMARY KEY (server_id, action),
    CONSTRAINT fk_config_server_actions_server FOREIGN KEY (server_id)
        REFERENCES config.config_monitored_servers (server_id) ON DELETE CASCADE,
    CONSTRAINT ck_config_server_actions_action CHECK (action IN ('force_plan', 'evict_plan', 'kill_session')),
    CONSTRAINT ck_config_server_actions_set_by CHECK (btrim(set_by) <> '')
);

DROP TRIGGER IF EXISTS trg_bump_server_actions ON config.config_server_actions;
CREATE TRIGGER trg_bump_server_actions
    AFTER INSERT OR UPDATE OR DELETE ON config.config_server_actions
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();";

    /// <summary>
    /// The text of V171 and V172 (#5571). Plain literal DDL after token substitution, not dynamic SQL, so the data-moving
    /// census can read the shipped text. Every statement is catalog-only; see <see cref="V171Sql"/>.
    /// </summary>
    private static string BuildQueryStoreIntervalPartitionRungSql(bool wide)
    {
        var table = wide ? "query_store_interval_wide" : "query_store_interval_latest";
        var uniqueColumns = wide
            ? "server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time, execution_type_desc"
            : "server_id, database_name, runtime_stats_interval_id, plan_id, query_id, replica_role, first_execution_time";

        const string optionalIndex = @"

    IF to_regclass('collect.__NAME___legacy') IS NOT NULL THEN
        IF (SELECT i.indisvalid FROM pg_index i WHERE i.indexrelid = to_regclass('collect.__NAME___legacy')) THEN
            CREATE INDEX __NAME__ ON ONLY collect.__TABLE__ __DEFINITION__;
            ALTER INDEX collect.__NAME__ ATTACH PARTITION collect.__NAME___legacy;
        ELSE
            DROP INDEX collect.__NAME___legacy;
        END IF;
    END IF;";

        var sql = @"
SET LOCAL lock_timeout = '" + (MigrationCommandTimeoutSeconds - 20) + @"s';
DO $rung$
BEGIN
    IF (SELECT c.relkind FROM pg_class c WHERE c.oid = to_regclass('collect.__TABLE__')) = 'p' THEN
        RETURN;
    END IF;

    ALTER TABLE collect.__TABLE__ RENAME TO __TABLE___legacy;
    IF to_regclass('collect.ux___TABLE__') IS NOT NULL THEN
        ALTER INDEX collect.ux___TABLE__ RENAME TO ux___TABLE___legacy;
    END IF;
    IF to_regclass('collect.idx___TABLE___first_exec') IS NOT NULL THEN
        ALTER INDEX collect.idx___TABLE___first_exec RENAME TO idx___TABLE___first_exec_legacy;
    END IF;
    IF to_regclass('collect.ix___TABLE___server_first_exec') IS NOT NULL THEN
        ALTER INDEX collect.ix___TABLE___server_first_exec RENAME TO ix___TABLE___server_first_exec_legacy;
    END IF;" + (wide ? @"
    IF to_regclass('collect.ix___TABLE___collection_time_brin') IS NOT NULL THEN
        ALTER INDEX collect.ix___TABLE___collection_time_brin RENAME TO ix___TABLE___collection_time_brin_legacy;
    END IF;" : @"
    DROP TRIGGER IF EXISTS trg_plan_regression_daily_late ON collect.__TABLE___legacy;") + @"

    CREATE TABLE collect.__TABLE__ (LIKE collect.__TABLE___legacy INCLUDING DEFAULTS INCLUDING STORAGE)
        PARTITION BY RANGE (first_execution_time);
    ALTER TABLE collect.__TABLE__ ATTACH PARTITION collect.__TABLE___legacy FOR VALUES FROM (MINVALUE) TO (MAXVALUE);

    CREATE UNIQUE INDEX ux___TABLE__ ON ONLY collect.__TABLE__ (__UNIQUECOLUMNS__) NULLS NOT DISTINCT;
    ALTER INDEX collect.ux___TABLE__ ATTACH PARTITION collect.ux___TABLE___legacy;
    CREATE INDEX idx___TABLE___first_exec ON ONLY collect.__TABLE__ (first_execution_time);
    ALTER INDEX collect.idx___TABLE___first_exec ATTACH PARTITION collect.idx___TABLE___first_exec_legacy;"
            + optionalIndex.Replace("__NAME__", "ix___TABLE___server_first_exec", StringComparison.Ordinal)
                .Replace("__DEFINITION__", "(server_id, first_execution_time)", StringComparison.Ordinal)
            + (wide
                ? optionalIndex.Replace("__NAME__", "ix___TABLE___collection_time_brin", StringComparison.Ordinal)
                    .Replace("__DEFINITION__", "USING brin (collection_time) WITH (autosummarize = off)", StringComparison.Ordinal)
                : @"

    CREATE TRIGGER trg_plan_regression_daily_late
        AFTER INSERT OR UPDATE ON collect.__TABLE__
        FOR EACH ROW
        WHEN (NEW.first_execution_time < date_trunc('day', now() AT TIME ZONE 'UTC') - interval '1 day'
              AND NEW.first_execution_time >= date_trunc('day', now() AT TIME ZONE 'UTC') - interval '17 days')
        EXECUTE FUNCTION collect.plan_regression_daily_mark_late();") + @"
END
$rung$;";

        return sql
            .Replace("__UNIQUECOLUMNS__", uniqueColumns, StringComparison.Ordinal)
            .Replace("__TABLE__", table, StringComparison.Ordinal);
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
        /* The pre-adds ride along for the same reason V54Sql already does: the generated resolving view is
           compiled from TODAY's payload column list, so this rung's re-emission names every payload column
           the current build knows — including ones a LATER rung adds. A store climbing from an older release
           reaches this rung first, so without those columns the view references one that does not exist yet
           and the ladder stops at 42703.

           Generated from the collector definition rather than by threading a specific later rung's SQL in
           here: one column was the symptom, and the next payload column would need this edited again. Every
           statement is ADD COLUMN IF NOT EXISTS, so a store that already has them is unaffected. */
        new Migration(51, "query-stats-host-object",
            V51Sql + "\n" + V54Sql + "\n" + PgSchemaGenerator.GenerateQueryStatsPayloadColumnPreAdds()
            + "\n" + PgSchemaGenerator.GenerateQueryStatsResolvingView()),
        new Migration(52, "finding-drilldown-json", V52Sql),
        new Migration(53, "store-self-metrics", V53Sql),
        new Migration(54, "plan-dim-gzip",
            V54Sql + "\n" + PgSchemaGenerator.GenerateQueryStatsPayloadColumnPreAdds()
            + "\n" + PgSchemaGenerator.GenerateQueryStatsResolvingView()),
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
        new Migration(111, "store-log-self-monitoring", V111Sql),
        new Migration(112, "collector-stall-wait-probes", V112Sql),
        new Migration(113, "remediation-credential-and-actor", V113Sql),
        new Migration(114, "pg-index-bloat-estimate-columns", V114Sql),
        new Migration(115, "pg-cpu-capacity-headroom", V115Sql),
        new Migration(116, "custom-alert-core", V116Sql),
        new Migration(117, "mute-rules-reload-beacon", V117Sql),
        new Migration(118, "builtin-alert-persistence", V118Sql),
        new Migration(119, "retention-hold-ratio-knobs", V119Sql),
        new Migration(120, "deadlock-rate-band-knobs", V120Sql),
        new Migration(121, "oversized-plan-backlog",
            V121Sql + "\n" + OversizedPlanBacklog.CreateTableSql + "\n" + V54Sql + "\n"
            + PgSchemaGenerator.GenerateQueryStatsPayloadColumnPreAdds() + "\n"
            + PgSchemaGenerator.GenerateQueryStatsResolvingView()),
        new Migration(122, "pg-deadlock-blocking-count-knobs", V122Sql),
        new Migration(123, "fleet-sweep-state", V123Sql),
        new Migration(124, "fleet-sweep-cadence-knobs", V124Sql),
        new Migration(125, "collector-database-scope", V125Sql),
        new Migration(126, "self-disk-warn-gb-floor", V126Sql),
        new Migration(127, "delta-family-interval-columns", V127Sql),
        /* V128 re-emits the payload-resolving v_query_stats (its two new query_stats columns land mid-list,
           ahead of the digests, which CREATE OR REPLACE VIEW refuses), so it rides the V121 idiom: V54's
           gz pre-add, then every payload column's pre-add, then the regenerated view. MigrationLadderPins
           holds the ordering. */
        new Migration(128, "delta-family-interval-completion",
            V128Sql + "\n" + V54Sql + "\n"
            + PgSchemaGenerator.GenerateQueryStatsPayloadColumnPreAdds() + "\n"
            + PgSchemaGenerator.GenerateQueryStatsResolvingView()),
        new Migration(129, "pg-log-events", V129Sql),
        new Migration(130, "pg-log-event-metrics", V130Sql),
        new Migration(131, "notification-routes", V131Sql),
        new Migration(132, "perfmon-counter-type", V132Sql),
        new Migration(133, "pg-numbackends-and-sampled-ms", V133Sql),
        new Migration(134, "time-honesty", V134Sql),
        new Migration(135, "lrq-exclusion-knob", V135Sql),
        new Migration(136, "pg-database-size-and-host-memory", V136Sql),
        new Migration(137, "qs-capture-mode-route-knob-toast-utilisation", V137Sql),
        new Migration(138, "pg-server-config-database-role-overrides", V138Sql),
        new Migration(139, "postmaster-start-time", V139Sql),
        new Migration(140, "checkpointer-timed-count", V140Sql),
        new Migration(141, "collection-caveats", V141Sql),
        new Migration(142, "index-object-stats-server-time", V142Sql),
        new Migration(143, "query-store-interval-latest", V143Sql),
        new Migration(144, "raw-chunk-interval-rung-history", V144Sql),
        new Migration(145, "query-store-interval-wide", V145Sql),
        new Migration(146, "managed-conf-verdicts", V146Sql),
        new Migration(147, "compose-statement-timeout-sixty", V147Sql),
        new Migration(148, "read-latency", V148Sql),
        new Migration(149, "query-store-liveness-hot-touch", V149Sql),
        new Migration(150, "collection-log-watermark-and-job-history-indexes", V150Sql),
        new Migration(151, "ag-group-id", V151Sql),
        new Migration(152, "drop-unread-cagg-group-indexes", V152Sql),
        new Migration(153, "interval-tables-first-exec-index", V153Sql),
        new Migration(154, "interval-tables-wide-first-exec-index", V154Sql),
        new Migration(155, "query-store-interval-end", V155Sql),
        new Migration(156, "checkpoint-longest-sync", V156Sql),
        new Migration(157, "mute-rule-server-id", V157Sql),
        new Migration(158, "install-id", V158Sql),
        new Migration(159, "install-id-table-oid", V159Sql),
        new Migration(160, "collector-run-time", V160Sql),
        new Migration(161, "query-store-top-daily", V161Sql),
        new Migration(162, "slow-reads", V162Sql),
        new Migration(163, "store-statement-history", V163Sql),
        new Migration(164, "query-stats-hour-ledger", V164Sql),
        new Migration(165, "password-key", V165Sql),
        new Migration(166, "pagerduty-auto-resolve", V166Sql),
        new Migration(167, "legacy-pin-candidates", V167Sql),
        new Migration(168, "plan-regression-daily", V168Sql),
        new Migration(169, "aws-per-server-role", V169Sql),
        new Migration(170, "pg-io-stats-hourly", V170Sql),
        new Migration(171, "query-store-interval-wide-partitioned", V171Sql),
        new Migration(172, "query-store-interval-latest-partitioned", V172Sql),
        new Migration(173, "query-store-compose-stamp", V173Sql),
        new Migration(174, "service-version", V174Sql),
        new Migration(175, "plan-regression-daily-rowcount", V175Sql),
        new Migration(176, "procedure-stats-totals", V176Sql),
        new Migration(177, "runnable-queue-alert-settings", V177Sql),
        new Migration(178, "query-store-watermarks", V178Sql),
        new Migration(179, "client-timeouts-alert", V179Sql),
        new Migration(180, "external-alerts", V180Sql),
        new Migration(181, "notification-route-scope", V181Sql),
        new Migration(182, "read-latency-capacity-fields", V182Sql),
        new Migration(183, "read-caller", V183Sql),
        new Migration(184, "agent-investigations", V184Sql),
        new Migration(185, "plan-regression-partial-days", V185Sql),
        new Migration(186, "collection-log-hourly", V186Sql),
        new Migration(187, "alert-resolution-delivery", V187Sql),
        new Migration(188, "investigation-alert-facts", V188Sql),
        new Migration(189, "query-store-compose-failures", V189Sql),
        new Migration(190, "store-metrics-size-estimated", V190Sql),
        new Migration(191, "agent-level-2-store", V191Sql),
        new Migration(192, "query-tags-store", V192Sql),
        new Migration(193, "phase-c-store", V193Sql),
        new Migration(194, "query-tag-sightings-database", V194Sql),
        new Migration(195, "query-store-forced-plans", V195Sql),
        new Migration(196, "level-3-server-action-switches", V196Sql),
        new Migration(197, "plan-force-checks", V197Sql),
        new Migration(198, "pseudonym-key", V198Sql),
        new Migration(199, "blocking-head-evidence", V199Sql),
        new Migration(200, "excluded-volumes", V200Sql),
        new Migration(201, "forced-plan-variants-and-guides", V201Sql),
        new Migration(202, "ag-primary-replica", V202Sql),
        new Migration(203, "hypothetical-index-login", V203Sql),
        new Migration(204, "custom-view-folders", V204Sql),
        new Migration(205, "custom-alert-rule-expiry", V205Sql),
        new Migration(206, "collector-enabled-inherit", V206Sql),
        new Migration(207, "change-watches", ChangeWatchStore.V207Sql),
        new Migration(208, "mute-rule-starts-at", V208Sql),
        new Migration(209, "query-store-fetch-top-n", V209Sql),
        new Migration(210, "mcp-agent-route-tag-guard", V210Sql),
        new Migration(211, "client-timeouts-relative-bars", V211Sql),
        new Migration(212, "server-machine-identity", V212Sql),
        new Migration(213, "query-store-replica", V213Sql),
        new Migration(214, "known-patterns", V214Sql),
        new Migration(215, "agent-table-indexes", V215Sql),
        new Migration(216, "client-timeouts-on-by-default", V216Sql),
        new Migration(217, "query-store-hints", V217Sql),
        new Migration(218, "server-action-recompile-module", V218Sql),
        new Migration(219, "query-store-self-keys", V219Sql),
        new Migration(220, "job-history-success-index", V220Sql),
        new Migration(221, "query-store-hash-hourly", V221Sql),
        new Migration(222, "agent-alerts", V222Sql),
        new Migration(223, "database-protection", V223Sql),
        new Migration(224, "identity-capacity", V224Sql),
        new Migration(225, "server-aws-rds", V225Sql),
        new Migration(226, "server-aws-rds-web-or-express", V226Sql),
    };

    /// <summary>
    /// V225 (#6894): <c>collect.servers.is_aws_rds</c>, whether the target is an Amazon RDS for SQL Server instance (the connect query's
    /// <c>DB_ID('rdsadmin') IS NOT NULL</c>), written on every connect. The collectors that RDS gates off (running jobs, Agent status, Agent alerts)
    /// never run there, so nothing in the store could tell the read side why they have no data, and an RDS target was answered as if a setup
    /// step on the server could fix it. NULL means no connect has stamped it yet (and a PostgreSQL target, where it is not a SQL Server fact),
    /// which makes no claim. No backfill: a registration fills its own on its next connect. No view refresh: <c>collect.servers</c> is a
    /// registry, not a hypertable.
    /// </summary>
    private const string V225Sql = @"
ALTER TABLE collect.servers
    ADD COLUMN IF NOT EXISTS is_aws_rds boolean;";

    /// <summary>
    /// V226 (#6893): <c>collect.servers.is_aws_rds_web_or_express</c>, whether that Amazon RDS for SQL Server instance is on the Web or Express
    /// edition (the connect query's <c>SERVERPROPERTY('Edition')</c> together with the RDS flag), written on every connect. RDS refuses every
    /// Extended Events session create on those editions, so the deadlock, blocked-process, client-timeout and long-query captures never run
    /// there, and <c>SERVERPROPERTY('EngineEdition')</c> is 2 for both Web and Standard, so <c>sql_engine_edition</c> cannot say it. The read
    /// side needs this column to answer those reads with the edition limit instead of a setup step that cannot help. NULL means no connect has
    /// stamped it yet (and a PostgreSQL target), which makes no claim. No backfill: a registration fills its own on its next connect. No view
    /// refresh: <c>collect.servers</c> is a registry, not a hypertable.
    /// </summary>
    private const string V226Sql = @"
ALTER TABLE collect.servers
    ADD COLUMN IF NOT EXISTS is_aws_rds_web_or_express boolean;";

    /// <summary>
    /// V189 (#5867): <c>collect.query_store_compose_stamp_failures</c>, the failure record of the Query Store compose builder: one row
    /// per hour whose last build threw, with its consecutive failures, last error and next attempt, so a build that cannot finish backs
    /// off to one attempt a day instead of running every tick, and <c>get_store_metrics</c> and the self-alert can show it
    /// (<see cref="QueryStoreComposeStamp.FailuresCreateSql"/>). A plain empty table, no GRANT (the <c>collect</c> schema's blanket
    /// SELECT covers it). <b>No Lite twin:</b> Lite has no PostgreSQL store. A const, so the list above reads it whatever the
    /// order of the static initializers.
    /// </summary>
    private const string V189Sql = QueryStoreComposeStamp.FailuresCreateSql;

    /// <summary>
    /// V114 — the statistics ESTIMATE columns on <c>collect.pg_index_bloat</c>, and the retirement of the
    /// rotation cursor that the exact census needed (#3234).
    ///
    /// <para><b>ADD only, deliberately.</b> The seven <c>pgstatindex</c> measurement columns stay and the
    /// collector now writes them NULL. The store holds 90 days of exact measurements taken before this
    /// change and an on-request measurement needs somewhere to land, so dropping them would destroy
    /// history to save nothing. A NULL there means the row was ESTIMATED — it does not mean an exact
    /// measurement came back empty, and the read has to keep those two apart.</para>
    ///
    /// <para><b>Why every input is stored and not just the answer.</b> <c>index_pages</c>,
    /// <c>table_rows</c>, <c>fillfactor</c>, <c>est_tuple_bytes</c> and <c>est_leaf_pages</c> are the
    /// terms the estimate is computed from, and storing them is what lets a reader disagree with the
    /// number rather than believe it. It is also the only way a later change to the width model can be
    /// evaluated against history instead of re-measured from scratch — the model was wrong twice before
    /// it was right, in opposite directions, and both wrong versions produced plausible percentages.</para>
    ///
    /// <para><b>No stored bloat percentage is derived from a density.</b> <c>est_bloat_pct</c> is a page
    /// count comparison. It is NULL under exactly the condition that populates <c>skipped_reason</c>, so a
    /// suppressed estimate can never be read as zero bloat, and <c>est_reclaimable_bytes</c> is what reads
    /// rank on because a 64 kB index at 20 percent tops a percentage-ranked list and is worth 50 kB
    /// (#2561).</para>
    ///
    /// <para><b>The DELETE is the cursor cleanup, and it is scoped twice.</b> The estimate covers every
    /// index in one statement, so <c>pg_index_bloat</c> declares no <c>StateKeys</c> and the per-database
    /// prune no longer owns the <c>rotate:</c> prefix — which means these rows would sit in
    /// <c>collector_state</c> forever with nothing to retire them. Filtered on the collector name AND the
    /// prefix rather than either alone: the prefix is generic enough that another collector could adopt
    /// it, and <c>collector_name</c> alone would delete a future key belonging to this one.</para>
    ///
    /// <para>No index is added for the new ranking column. The read is a per-index DISTINCT ON ordered by
    /// <c>collection_time</c> and served by <c>idx_pg_index_bloat_time</c>; a ranking index would be a
    /// guess, and this collector has already cost enough unmeasured constants.</para>
    /// </summary>
    private const string V114Sql = @"
ALTER TABLE collect.pg_index_bloat
    ADD COLUMN IF NOT EXISTS index_pages bigint,
    ADD COLUMN IF NOT EXISTS table_rows bigint,
    ADD COLUMN IF NOT EXISTS fillfactor integer,
    ADD COLUMN IF NOT EXISTS est_tuple_bytes bigint,
    ADD COLUMN IF NOT EXISTS est_leaf_pages bigint,
    ADD COLUMN IF NOT EXISTS est_bloat_pct double precision,
    ADD COLUMN IF NOT EXISTS est_reclaimable_bytes bigint,
    ADD COLUMN IF NOT EXISTS pgstattuple_available boolean;

DELETE FROM collect.collector_state
WHERE collector_name = 'pg_index_bloat'
AND   state_key LIKE 'rotate:%';";

    /// <summary>
    /// V115 — the capacity-headroom columns on <c>collect.pg_cpu_utilization</c> (#3281).
    ///
    /// <para><b>Why <c>cpu_percent</c> alone was not enough.</b> It holds Performance Insights'
    /// <c>os.cpuUtilization.total.avg</c>, which is percent of the capacity CURRENTLY ALLOCATED. On Aurora
    /// Serverless v2 that allocation is re-sized continuously, so a one-vCPU instance reads exactly 100%
    /// whenever one core stays busy for a minute — the routine trigger for scaling up. Measured at one such
    /// minute on a production instance: 4 of 12 configured ACUs in use, 33% of the ceiling. Both consumers
    /// banded the 100% as an incident, and the fleet it was measured on is <b>153 of 153</b> serverless, so
    /// there was no population the old reading was correct on.</para>
    ///
    /// <para>Nullable with no DEFAULT and no backfill, matching every column-adding rung here: PI holds no
    /// history this store can reach for the minutes already recorded, and a 0 would claim measured headroom
    /// for a window nobody measured. A NULL here bands Unknown, never Healthy — which is also what an
    /// instance class with no ACU concept at all produces, and the reason the band refuses to guess.</para>
    ///
    /// <para>Column-for-column identical to what <see cref="PgSchemaGenerator"/> generates from
    /// <see cref="PgCpuUtilizationCollector.PayloadColumns"/>, so V106's own text carries the same three
    /// columns for a store created fresh — pinned by
    /// <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c>. The two texts are
    /// not forced to agree by anything but that test, and a drift between them is a permanent invisible
    /// split between stores created before and after this rung.</para>
    /// </summary>
    private const string V115Sql = @"
ALTER TABLE collect.pg_cpu_utilization
    ADD COLUMN IF NOT EXISTS acu_utilization_percent double precision,
    ADD COLUMN IF NOT EXISTS serverless_capacity_acu double precision,
    ADD COLUMN IF NOT EXISTS max_configured_acu double precision;";

    /// <summary>
    /// V116 — the custom-alerting core (#3285): two NEW config-plane tables, added additively exactly like
    /// V31's <c>custom_views</c> and V32's fleet tags.
    ///
    /// <para><c>custom_alert_rules</c> is the user-authored rule: a jsonb <c>definition</c> (a Scalar compose
    /// metric spec + predicate + hysteresis + scope, validated by the compose <c>TryParsePanel</c> authority
    /// plus a thin rule validator), an <c>enabled</c> flag the evaluator filters on, and the same
    /// optimistic-concurrency <c>version</c> + audit columns as <c>custom_views</c>. Qualified <c>config.</c>
    /// because the migrate session's <c>search_path</c> puts <c>collect</c> first (mirrors V31); no secret
    /// columns, so no <c>ViewerRestrictedConfigTables</c> carve.</para>
    ///
    /// <para><b>NO <c>config_bump_version</c> trigger</b>, deliberately and for the same reason as
    /// <c>custom_views</c>: the config-version reload beacon re-reads only the fixed <c>DarlingConfig</c> view
    /// and would NOT load this table anyway, and a bump would force a needless fleet-wide
    /// <c>ReloadFromStoreAsync</c> on every threshold tweak. The <c>CustomAlertEvaluator</c> reads enabled
    /// rules directly on its own sweep.</para>
    ///
    /// <para><c>custom_alert_state</c> is the per-<c>(rule_id, server_id)</c> evaluation state the shared
    /// <c>AlertPersistenceGate</c> needs and that no existing state shape offers: a resettable
    /// <c>consecutive_breaches</c>/<c>consecutive_clears</c> pair (the edge-trigger watermark is a single int
    /// with no resettable counter; <c>incident_occurrences</c> is a replace-the-set accumulator).
    /// <c>rule_version</c> is stamped so a threshold edit (which bumps the rule's version) resets the streak
    /// rather than firing a stale count against a new bar; <c>firing</c>/<c>fired_severity</c> track the open
    /// incident so a resolve can be emitted and a Warning→Critical transition detected; <c>next_due_at</c>
    /// carries per-rule cadence. The FK to <c>custom_alert_rules</c> with <c>ON DELETE CASCADE</c> is the
    /// teardown: deleting a rule drops its state rows (the evaluator force-resolves any open incident first).
    /// Written by the evaluator on the owner pool; no viewer/mcp grant until the editor surfaces firing
    /// status.</para>
    /// </summary>
    private const string V116Sql = @"
CREATE TABLE IF NOT EXISTS config.custom_alert_rules (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL UNIQUE,
    definition jsonb NOT NULL,
    description text,
    enabled boolean NOT NULL DEFAULT TRUE,
    version integer NOT NULL DEFAULT 1,
    created_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    updated_by text
);

CREATE TABLE IF NOT EXISTS config.custom_alert_state (
    rule_id bigint NOT NULL REFERENCES config.custom_alert_rules (id) ON DELETE CASCADE,
    server_id integer NOT NULL,
    rule_version integer NOT NULL,
    consecutive_breaches integer NOT NULL DEFAULT 0,
    consecutive_clears integer NOT NULL DEFAULT 0,
    firing boolean NOT NULL DEFAULT FALSE,
    fired_severity text,
    last_evaluated_at timestamp,
    next_due_at timestamp,
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (rule_id, server_id)
);";

    /// <summary>
    /// V117 — <c>config.config_mute_rules</c> joins the <c>config_version</c> reload beacon (#3315).
    ///
    /// <para>The mute rules the alert engine honors live in the service's in-memory
    /// <c>MuteRuleService</c> cache, and the only thing that re-<c>LoadAsync()</c>es that cache is a
    /// <c>config_version</c> change. So a mute rule that lands in the table without bumping the beacon is
    /// persisted and inert: the store agrees the rule exists, <c>get_mute_rules</c> lists it, and the
    /// evaluator keeps delivering matching alerts until something unrelated happens to bump the beacon or
    /// the service restarts. There is no upper bound on that window and nothing reports it — an operator
    /// mutes a firing alert, is told it is muted, and the pages continue.</para>
    ///
    /// <para><b>A trigger rather than a bump at each write site</b>, which is what makes this cover the
    /// writers nobody enumerated: the MCP tools (<c>create_mute_rule</c> / <c>delete_mute_rule</c>, which
    /// construct their own <c>PgMuteRuleStore</c> and never touch the live service), the Viewer's Manage
    /// Mute Rules surface, the tray Snooze, <c>PgMuteRuleStore.DeleteExpiredAsync</c>, and hand-written SQL
    /// all reach the same table. <c>AFTER INSERT OR UPDATE OR DELETE</c> because DELETE is the direction
    /// that costs most — an operator un-mutes, believes alerting is restored, and a stale cache keeps
    /// suppressing.</para>
    ///
    /// <para>Statement-level, sharing V17's <c>config.config_bump_version()</c> function verbatim, so this
    /// is the fifth instance of an established shape rather than a new mechanism: same SECURITY INVOKER
    /// beacon UPDATE, and the <c>mcp</c> role's existing column-level
    /// <c>UPDATE (config_version, updated_at) ON config.config_service</c> grant (provisioned for the
    /// <c>config_alert_settings</c> trigger) is exactly what this one needs, so no grant moves.
    /// <c>DROP TRIGGER IF EXISTS</c> first, matching V17's idiom, so a replay is a harmless no-op.</para>
    /// </summary>
    private const string V117Sql = @"
DROP TRIGGER IF EXISTS trg_bump_mute_rules ON config.config_mute_rules;
CREATE TRIGGER trg_bump_mute_rules
    AFTER INSERT OR UPDATE OR DELETE ON config.config_mute_rules
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();";

    /// <summary>
    /// V118 — the BUILT-IN alert catalog's persistence-gate state (#3282). One additive config-plane table,
    /// the twin of V116's <c>custom_alert_state</c> for alerts nobody authored.
    ///
    /// <para><b>Why a separate table rather than the custom one.</b> <c>custom_alert_state</c> is keyed
    /// <c>(rule_id, server_id)</c> with a foreign key to <c>custom_alert_rules</c> and
    /// <c>ON DELETE CASCADE</c>; a built-in alert has no rule row to reference, so it has no key there and
    /// nothing to cascade from. The built-in subject is <c>(server_id, metric_name)</c> — the key the
    /// engine's other state already uses.</para>
    ///
    /// <para><b>And why not columns on <c>config_edge_trigger_watermarks</c></b>, which carries exactly that
    /// key. Two reasons, each sufficient. That column is one monotonic integer documented as "the highest
    /// already-alerted rolling-window count", and a resettable pair of counters is not that shape. And Lite's
    /// twin of the row is written with <c>INSERT OR REPLACE</c> over a PARTIAL column list, which resets
    /// every unlisted column to its default — a streak living there would zero itself on every fired
    /// blocking or deadlock alert, i.e. while it was being counted. Same finding V61's
    /// <c>incident_occurrences</c> was split out for.</para>
    ///
    /// <para><c>last_observed_sample_at</c> is the gate's observation identity, not display data. The gate
    /// counts consecutive breaching SAMPLES and the alert sweep is twice as fast as a CPU sample arrives, so
    /// without it a re-read of one sample would count as a second observation and "three consecutive
    /// breaches" would be satisfied inside ninety seconds — shorter than every excursion #3282 measured.</para>
    ///
    /// <para>No <c>config_bump_version</c> trigger, like V116: this is evaluator state written every new
    /// sample, and a reload beacon on it would force a fleet-wide <c>ReloadFromStoreAsync</c> once a minute
    /// per server. No per-table GRANT either — provisioning re-runs
    /// <c>GRANT … ON ALL TABLES IN SCHEMA config</c> after migration — and no viewer/mcp read yet: the
    /// service's alert pass is the only consumer.</para>
    /// </summary>
    private const string V118Sql = @"
CREATE TABLE IF NOT EXISTS config.alert_persistence_state (
    server_id integer NOT NULL,
    metric_name text NOT NULL,
    consecutive_breaches integer NOT NULL DEFAULT 0,
    consecutive_clears integer NOT NULL DEFAULT 0,
    firing boolean NOT NULL DEFAULT FALSE,
    last_observed_sample_at timestamp,
    updated_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    PRIMARY KEY (server_id, metric_name)
);";

    /// <summary>
    /// V119 — the Retention Held tiers on the singleton <c>config_alert_settings</c> row (#3297), which were
    /// compile-time constants, making the one alert an operator most needs to tune the one alert that could
    /// not be. Field-reported on #3296: an hourly CRITICAL arrived, and Settings held nothing matching
    /// "Retention Held" or "Monitor Store".
    ///
    /// <para>Store-backed like its #2136 sibling <c>store_job_cadence_warn_percent</c> (V57), which is the
    /// pattern the constants' own comment named as the destination. <c>double precision</c> because the
    /// value is a ratio with a meaningful fractional part —
    /// <c>analysis_notify_severity</c> is the same type on this table, so this is the established shape
    /// rather than a new one. The column defaults ARE the constants they replace, taken from
    /// <see cref="TimescaleSupport.RetentionHoldWarnRatioDefault"/> and its critical sibling rather than
    /// restated here, so a store that upgrades and is never touched keeps firing exactly where it did.</para>
    ///
    /// <para><b>No ACL or provisioning change</b>: <c>config_alert_settings</c> carries table-level grants
    /// with no column carve (the V33/V35/V55/V57 rungs all say so), and the V17 statement-level
    /// <c>trg_bump_alert_settings</c> already bumps <c>config_service.config_version</c> on any write here,
    /// so the running service picks a change up on its next sweep with no restart and no new trigger.</para>
    ///
    /// <para><b>No CHECK ordering the two tiers</b>, deliberately. A pair where critical sits below warn is
    /// not a broken state needing a constraint or a read-time rewrite: firing is gated on warn and severity
    /// on critical, so every fire is simply Critical and the Warning tier is empty — which is exactly what
    /// an operator who put critical below warn asked for. A <c>GREATEST</c> on read would instead accept the
    /// value and then use a different one, which is the "setting did not stick" failure the MCP
    /// bound-equals-clamp parity exists to prevent.</para>
    /// </summary>
    private const string V119Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS retention_hold_warn_ratio double precision NOT NULL DEFAULT 2.0;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS retention_hold_critical_ratio double precision NOT NULL DEFAULT 4.0;";

    /// <summary>
    /// V120 — the deadlock health band's two tiers, in deadlocks per HOUR (#3368).
    ///
    /// <para>The band was <c>count &gt; 0 ? Critical : Healthy</c>, so a single resolved deadlock made a
    /// server Critical and the band's most common cause was the one condition that had already resolved
    /// itself. Banding on a rate instead needs numbers whose right value differs per workload, which is
    /// #3297's argument for the control plane rather than a compile-time constant.</para>
    ///
    /// <para>Column defaults name the same figures
    /// <c>ServerHealthThresholds.DeadlockWarnPerHourDefault</c> /
    /// <c>DeadlockCriticalPerHourDefault</c> carry, where the measured distribution they come from is
    /// documented; they are literals HERE because SQL text cannot reference a C# constant, and
    /// <c>DeadlockRateBandRungTests</c> pins the two against each other so the pair cannot drift.</para>
    /// </summary>
    private const string V120Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS deadlock_warn_per_hour double precision NOT NULL DEFAULT 5.0;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS deadlock_critical_per_hour double precision NOT NULL DEFAULT 20.0;";

    /// <summary>
    /// V121 — the measured size of each row's cached-plan XML, and the backlog of the plans the size cap
    /// declined to capture (#3392).
    ///
    /// <para><b>The columns.</b> <c>query_plan_xml_bytes</c> on <c>query_stats</c> and
    /// <c>procedure_stats</c> is the <c>DATALENGTH</c> the collectors' cap already evaluates, now also
    /// selected. It is never gated by the cap, so a row over it carries a size and a NULL plan — the only
    /// pairing that distinguishes "omitted for size" from "the handle aged out", which is what
    /// <c>collect.oversized_plan_backlog</c> is keyed on. Nullable with no DEFAULT and no backfill, matching
    /// every column-adding rung here: no measurement exists for rows already collected, and a 0 would claim
    /// a plan of zero bytes. TimescaleDB accepts a nullable ADD COLUMN on a compressed hypertable.</para>
    ///
    /// <para><b>The table's DDL comes from <see cref="OversizedPlanBacklog.CreateTableSql"/></b>, appended
    /// to this constant rather than transcribed here — the V38 idiom. The statements that address the table
    /// live beside that DDL, so the shape a store gets and the shape the code writes cannot drift.</para>
    ///
    /// <para><b><c>v_query_stats</c> is NOT a passthrough on a V38+ store</b> — it is the #1767
    /// payload-RESOLVING view (<see cref="PgSchemaGenerator.GenerateQueryStatsResolvingView"/>), so it must
    /// be rebuilt from the generator rather than re-expanded as <c>SELECT *</c>, which would silently return
    /// NULL query_text and NULL plan XML for every digest-era row. And because the generator emits payload
    /// columns BEFORE the trailing digest columns, the new column lands mid-list — an alteration
    /// <c>CREATE OR REPLACE VIEW</c> refuses (append-at-end only) — hence DROP + recreate, exactly as V51
    /// did for <c>host_object_name</c>. Plain DROP, no CASCADE: nothing persistent depends on the view;
    /// readers reference it per-query. <c>V54Sql</c> is concatenated ahead of the regenerated view because
    /// that view references the plan dim's compressed-content column, which a store upgrading from below V54
    /// does not have yet (<c>MigrationLadderPins</c> pins the ordering).</para>
    /// </summary>
    private const string V121Sql = @"
ALTER TABLE query_stats
    ADD COLUMN IF NOT EXISTS query_plan_xml_bytes bigint;
ALTER TABLE procedure_stats
    ADD COLUMN IF NOT EXISTS query_plan_xml_bytes bigint;
DROP VIEW IF EXISTS v_query_stats;";

    /// <summary>
    /// V122 — the PostgreSQL Deadlocks and Blocking alerts' count thresholds on the singleton
    /// <c>config_alert_settings</c> row (#3444), which were <c>private const int … = 1</c> in
    /// <c>DarlingWorker</c> with no settings, config or JSON path, while their SQL Server twins
    /// (<c>blocking_count_threshold</c>, <c>deadlock_count_threshold</c>) have been settable since V1.
    ///
    /// <para><b>Their OWN columns rather than the twins'.</b> The two engines' figures are calibrated
    /// against different evidence and, decisively, against different surfaces: the reason to move the SQL
    /// Server deadlock figure is agreement with <c>deadlock_warn_per_hour</c> (V120), and a PostgreSQL
    /// server has no deadlock band to agree with — <c>v_deadlocks</c> is the extended-event capture and is
    /// structurally zero for a PostgreSQL server (#3017), and the reading is nulled again by
    /// <c>ServerMetricSources.DmvSourced</c> before it reaches the band (superseded by #3638, which gave the
    /// PostgreSQL card a measured band from its own <c>pg_stat_database.deadlocks</c> counter differenced
    /// over the window, through the SAME <c>health_bands</c> tiers — so the #3444 move of raising a fire gate
    /// to meet the band's Warning bar is now available on this column too; the columns stay separate for
    /// the first reason, the instruments differ). On the blocking side the
    /// denominators differ outright: the SQL Server count is engine-recorded blocked-process reports, the
    /// PostgreSQL one is distinct root blockers in a periodic SAMPLE of <c>pg_stat_activity</c>. Reusing
    /// the columns would also make the upgrade behaviour a function of store state — a store whose
    /// operator had already raised the SQL Server threshold would have its PostgreSQL alerting silently
    /// quieted by this rung, which is the one outcome #3444 names as worse than the gap.</para>
    ///
    /// <para><b>The column defaults ARE the constants they replace</b> (1 and 1, from
    /// <c>PostgresAlertEvaluator.DeadlockCountThresholdDefault</c> and
    /// <c>BlockingCountThresholdDefault</c> — restated as literals here only because a rung is a SQL
    /// string, and pinned equal to those constants by <c>PgAlertCountKnobRungTests</c>). A store that
    /// upgrades and is never touched fires exactly where it did, on every store, whatever its SQL Server
    /// thresholds say.</para>
    ///
    /// <para><b><c>integer</c>, matching the twins</b> rather than the <c>double precision</c> the V119
    /// and V120 rungs used: these are counts of discrete events with no meaningful fractional part, and
    /// <c>blocking_count_threshold</c>/<c>deadlock_count_threshold</c> are <c>integer</c> on this same
    /// table.</para>
    ///
    /// <para><b>No ACL or provisioning change</b>: <c>config_alert_settings</c> carries table-level grants
    /// with no column carve (V33/V35/V55/V57/V119/V120 all say so), and V17's statement-level
    /// <c>trg_bump_alert_settings</c> already bumps <c>config_service.config_version</c> on any write
    /// here, so the running service picks a change up on its next sweep with no restart.</para>
    ///
    /// <para><b>No CHECK enforcing the floor</b>, deliberately, matching V119/V120. The floor is
    /// <c>PostgresAlertEvaluator.CountThresholdFloor</c>, enforced as the <c>update_alert_settings</c>
    /// write bound and as a read-side clamp on <c>DarlingAlertSettings</c> — the same raw-in/clamped-out
    /// split every knob on this table uses, so <c>get_alert_settings</c> reports back what the operator
    /// stored rather than a value a constraint rewrote.</para>
    /// </summary>
    private const string V122Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS pg_deadlock_count_threshold integer NOT NULL DEFAULT 1;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS pg_blocking_count_threshold integer NOT NULL DEFAULT 1;";

    /// <summary>
    /// V123 — the fleet sweep's state store (#3466, lane 1 of the approved four-lane sequence): the
    /// sweep-run tables and the watch-item worklist that make sweep-over-sweep diffing data instead of
    /// a report reading its own predecessor. Pinned by <c>FleetSweepStateRungTests</c>.
    ///
    /// <para><b>The DDL comes from <see cref="FleetSweepStore.CreateTablesSql"/></b>, referenced here
    /// rather than transcribed — the V38/V121 idiom. The statements that address the four tables live
    /// beside that DDL, so the shape a store gets and the shape the code writes cannot drift; every
    /// design decision (normalized verdicts, the NOT NULL instrument-liveness pair, the ledger's key,
    /// the worklist's hysteresis columns, plain-not-hypertable) is argued on that constant's docs
    /// rather than restated here.</para>
    ///
    /// <para><b>Darling-only, structurally and deliberately.</b> A sweep is a statement about a FLEET
    /// — what changed across the monitored population since the last sweep — and Lite monitors one
    /// server from one desktop with no fleet to summarize, so there is no Lite twin of this rung and
    /// none planned; the cross-SKU twinning discipline applies to surfaces both SKUs can mean, and
    /// this one only means anything centralized. The tables therefore live only in this ladder, like
    /// the oversized-plan backlog before them.</para>
    ///
    /// <para><b>No GRANT and no provisioning change.</b> The <c>collect</c> schema carries blanket
    /// SELECT for admin/viewer/mcp plus the owner-scoped <c>ALTER DEFAULT PRIVILEGES</c>, both
    /// re-asserted every managed start (<c>DarlingManagedRoles</c>) and re-run in BYO mode by
    /// <c>tools/provision-roles.sql</c>'s ON ALL TABLES form — so the web feed's viewer role and the
    /// MCP role read these tables with no new statement, and the engine writes as the service owner,
    /// so no write grant exists to add.</para>
    ///
    /// <para><b>Retention arrives with the engine (lane 2)</b>, the way the backlog's arrived beside
    /// its sweep: runs and children prune on <c>swept_at</c> at the base data horizon, watch items on
    /// <c>last_seen_at</c>. Deferring it is safe at this rung because nothing writes these tables
    /// until the engine lands in the same release line — an empty table needs no pruning.</para>
    /// </summary>
    private const string V123Sql = FleetSweepStore.CreateTablesSql;

    /// <summary>
    /// V124 — the fleet sweep's cadence knobs on the singleton <c>config_alert_settings</c> row
    /// (#3466, lane 2): whether the scheduled fleet sweep runs, and how often. The spec's own words —
    /// "at a user-configured cadence (hourly by default)" — make the cadence an operator knob from
    /// birth, so it ships on the control plane rather than graduating to it later the way #3297 and
    /// #3444's constants had to. Pinned by <c>FleetSweepCadenceKnobRungTests</c>.
    ///
    /// <para><b>This row is the precedent, not an approximation of one.</b> The scheduled-analysis
    /// cadence — the product's one existing "run a whole-fleet evaluation every N minutes" knob —
    /// lives here as <c>analysis_enabled</c>/<c>analysis_interval_minutes</c> (control-plane Stage 1),
    /// reaches the service through <c>StoreConfigProvider</c>'s wholesale config swap, and is exposed
    /// through <c>get_alert_settings</c>/<c>update_alert_settings</c> with write bounds that match the
    /// read-side clamp. The sweep's pair takes exactly that path. It is NOT an alert and delivers
    /// nothing — the master switch deliberately does not govern it, because sweeps under master-off
    /// are the muted-mode contract's whole point — but "the alert-settings row" has been the home of
    /// every operator-tunable evaluation cadence since V17, and a second config table for two columns
    /// would split the surface <c>update_alert_settings</c> documents.</para>
    ///
    /// <para><b>The column defaults ARE the shared constants</b> (<c>TRUE</c>, and
    /// <c>FleetSweepCadence.DefaultIntervalMinutes</c> — restated as literals here only because a rung
    /// is a SQL string, and pinned equal by the rung tests). Enabled-by-default is deliberate: the
    /// feature is dogfooded in a separate environment before any production install per the owner's
    /// deployment note, and a report surface that ships dark is a report surface nobody evaluates.</para>
    ///
    /// <para><b>Darling-only, structurally</b> — the V123 reasoning continues: Lite has no fleet to
    /// sweep, so there is no Lite twin of these knobs and <c>McpAlertSettingsKeyTests</c> records the
    /// omitted <c>fleet_sweep</c> group as a decision with a paying test.</para>
    ///
    /// <para><b>No CHECK enforcing the bounds</b>, matching V119/V120/V122: the floor and ceiling are
    /// <c>FleetSweepCadence</c>'s named constants, enforced as the <c>update_alert_settings</c> write
    /// bound, the Viewer's save gate, and the worker's read-side clamp — the raw-in/clamped-out split
    /// every knob on this table uses. No reload beacon of its own: V17's statement-level
    /// <c>trg_bump_alert_settings</c> already bumps <c>config_service.config_version</c> on any write
    /// here, so the running service picks a cadence change up on its next sweep with no restart. No
    /// GRANT: this table carries table-level grants with no column carve.</para>
    /// </summary>
    private const string V124Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS fleet_sweep_enabled boolean NOT NULL DEFAULT TRUE;
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS fleet_sweep_interval_minutes integer NOT NULL DEFAULT 60;";

    /// <summary>
    /// V125 — the optional per-collector database scope on <c>config_collector_schedules</c> (#3477):
    /// an ALLOW-LIST of database names beside <c>enabled</c> and the cadence columns, so an expensive
    /// per-database collector can be limited to a representative sample instead of turned off for the
    /// whole server. The reporter's measured case is the sizing argument: a 72-database instance paid
    /// a 43-minute <c>index_object_stats</c> pass whose slowest database carried 4.97% of it — pure
    /// fan-out WIDTH — and the only alternatives were paying for all 72 or losing the collector (and
    /// its growth trend) entirely. Pinned by <c>CollectorDatabaseScopeRungTests</c>.
    ///
    /// <para><b>An allow-list, deliberately, not a deny-list.</b> "Off everywhere except this one" on
    /// a 72-database instance is one name as an allow-list and 71 as a deny-list — and under a
    /// deny-list every newly created database silently REJOINS collection, which on a dev estate that
    /// creates databases daily re-grows the pass day by day. Under the allow-list a new database
    /// stays out until an operator names it, so the pass cost is independent of database-count
    /// growth.</para>
    ///
    /// <para><b>Nullable, no default, no CHECK</b> — this table's own sparse convention (absent row /
    /// NULL column = no override at this level), so every existing row and every untouched install
    /// reads NULL and collects exactly what it collects today. NULL falls through the row's standard
    /// per-column layering (per-server &gt; fleet-wide &gt; unscoped); an EMPTY array is DISTINCT from
    /// NULL and is the explicit "no scope" — it stops the fall-through, which is what lets one server
    /// opt back OUT of a fleet-wide scope without listing every database it has (the deny-list
    /// failure again, one layer up). <c>excludedDatabases</c> on the server row still WINS: the scope
    /// is an additional predicate on the same enumerations, so the effective set is scoped-in minus
    /// excluded and the coarse instrument keeps its veto.</para>
    ///
    /// <para><b>No reload beacon of its own</b>: V17's statement-level
    /// <c>trg_bump_collector_schedules</c> already bumps <c>config_service.config_version</c> on any
    /// write here, so the running service re-resolves scopes on its next sweep with no restart. No
    /// GRANT: this table carries table-level grants with no column carve, which is what every earlier
    /// rung touching <c>config</c> tables says.</para>
    /// </summary>
    private const string V125Sql = @"
ALTER TABLE config.config_collector_schedules
    ADD COLUMN IF NOT EXISTS databases text[];";

    /// <summary>
    /// V126 — the Store Disk Pressure warning's GB floor on the singleton <c>config_alert_settings</c>
    /// row (#3528): the self-alert's percent trigger additionally requires free space below this many
    /// GB before it fires, an AND qualifier so a large volume at a low percent (400 GB free on a 4 TB
    /// store) stops paging CRITICAL. 0 removes the floor and restores the percent-only condition —
    /// the <c>pvs_floor_gb</c> composition, deliberately not the target-volume pair's OR, whose GB
    /// dimension ADDS fires.
    ///
    /// <para><b>The column default IS the shipped constant</b>
    /// (<c>DarlingSelfAlertEvaluator.DiskFreeWarnFloorGb</c> — restated as a literal here only because
    /// a rung is a SQL string, and pinned equal by <c>SelfDiskWarnGbFloorRungTests</c>). Non-zero on
    /// upgrade DELIBERATELY, unlike the V122 knobs: their acceptance was "an untouched store fires
    /// exactly where it did", while #3528's is that the untouched firing IS the defect — the issue's
    /// own example is a default-configured store paging "act now" with 400 GB of runway. 50 puts the
    /// crossover at a 500 GB volume, so any store volume at or under that keeps the exact pre-#3528
    /// percent behaviour.</para>
    ///
    /// <para>No CHECK enforcing the bound, matching V119/V120/V122/V124: the floor-at-0 is enforced as
    /// the <c>update_alert_settings</c> write bound and <c>DarlingAlertSettings</c>' read-side clamp —
    /// the raw-in/clamped-out split every knob on this table uses. No reload beacon of its own: V17's
    /// statement-level <c>trg_bump_alert_settings</c> already bumps <c>config_service.config_version</c>
    /// on any write here. No GRANT: this table carries table-level grants with no column carve.</para>
    /// </summary>
    private const string V126Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS self_disk_free_warn_gb integer NOT NULL DEFAULT 50;";

    /// <summary>
    /// V127 — <c>sample_interval_seconds</c> on the four delta families that persisted their deltas NAKED
    /// (#3540): <c>wait_stats</c>, <c>file_io_stats</c>, <c>latch_stats</c>, <c>spinlock_stats</c>. The
    /// measurement-layer keystone: the shared delta calculator reports (delta 0, interval 0) when no delta is
    /// knowable — first sighting, counter reset, a gap past the measured 3600 s policy — and (0, n) when an
    /// interval was genuinely idle, and the interval is the ONLY thing that tells those apart. These four
    /// collectors discarded it at the write, so the fabricated zero survived as a measured one and every
    /// per-second reader LAG-divided it into a confident 0.00 ms/sec at exactly the moments (restarts) it was
    /// unknowable; the file-I/O latency chart rendered "0.00 ms" mid-restart; and the wait-rate window
    /// statistic counted the restart collection as a sample. <c>perfmon_stats</c> and <c>query_stats</c> have
    /// carried the column from the start and their readers <c>NULLIF(sample_interval_seconds, 0)</c> — this
    /// rung gives the other four the same column, in the same <c>integer</c> type, so the same idiom applies.
    /// Pinned by <c>DeltaFamilyIntervalColumnsRungTests</c>.
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, matching every column-adding rung on a collector
    /// table (V80, V81, V121): the interval a historical row accrued over was never recorded, so NULL is the
    /// honest value and a backfilled 0 would stamp every pre-V127 row as "unknowable" and erase 30 days of
    /// perfectly good history from every rate chart. Readers treat the three states distinctly: <c>n &gt; 0</c>
    /// is the measured interval; <c>0</c> is the calculator's unknowable marker and maps to NULL (the point is
    /// absent, never 0.00); <c>NULL</c> is a pre-V127 row and falls back to the LAG-over-collection_time
    /// derivation those readers always used, so history keeps rendering exactly as it did. A nullable
    /// no-default ADD COLUMN is a catalog-only change in PostgreSQL and TimescaleDB accepts it on a
    /// compressed hypertable, so this stays instant on a multi-hundred-GB <c>wait_stats</c>.</para>
    ///
    /// <para><b>The passthrough views are refreshed</b> because Postgres freezes a view's <c>SELECT *</c>
    /// column list at CREATE (the V14 lesson, restated by V80 and V81): without the four
    /// <c>CREATE OR REPLACE VIEW</c> lines every <c>v_*</c> reader would keep seeing the pre-V127 column list
    /// forever and the new column would be invisible to the whole read layer. Appending is the one alteration
    /// <c>CREATE OR REPLACE VIEW</c> permits, which is exactly what an ADD COLUMN produces. Fresh stores get
    /// the column from the generated CREATE TABLE at V4/V10 (the collector definitions carry it now) and
    /// this rung's ALTERs no-op there; the view refresh is idempotent either way.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do: touch <c>collect.wait_stats_baseline</c>.</b> That
    /// continuous aggregate sums <c>delta_wait_time_ms</c> per collection over the raw table, so a restart
    /// collection materializes as a <c>total_wait_ms = 0</c> sample and drags the WaitStats/WaitMsPerSec
    /// baselines' mean down (the campaign's A6). The measurement contract says the rollup should aggregate
    /// <c>sample_interval_seconds IS DISTINCT FROM 0</c> rows only — but a continuous aggregate cannot change
    /// its defining query in place; the only path is DROP + CREATE + refresh, and the raw <c>wait_stats</c>
    /// horizon is operator-editable and typically 30 days against the aggregate's 35-day baseline tier, so
    /// a rebuild forfeits materialized baseline history that raw can no longer refill. That is the exact
    /// trade #3527 declined for <c>perfmon_baseline</c> (its interval is LAG-derived from the collapsed series
    /// for the same reason), and this rung declines it the same way. The follow-up is a NEW aggregate under
    /// a new name with the filter baked in, built <c>WITH NO DATA</c> and backfilled by
    /// <c>--backfill-rollups</c>, with the old one retired through <c>RetiredBaselineRelations</c> once the
    /// new one has 35 days — the #2007 retirement shape, which loses nothing. Until then the baseline
    /// provider's magnitude heuristic (the QUALIFY restart signature) is the guard it always was.</para>
    /// </summary>
    private const string V127Sql = @"
ALTER TABLE collect.wait_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;
ALTER TABLE collect.file_io_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;
ALTER TABLE collect.latch_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;
ALTER TABLE collect.spinlock_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the four passthroughs would keep serving
   the pre-V127 column list forever — the V14 lesson, restated by V80 and V81. Appending is the one
   alteration CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN produces. */
CREATE OR REPLACE VIEW collect.v_wait_stats AS SELECT * FROM collect.wait_stats;
CREATE OR REPLACE VIEW collect.v_file_io_stats AS SELECT * FROM collect.file_io_stats;
CREATE OR REPLACE VIEW collect.v_latch_stats AS SELECT * FROM collect.latch_stats;
CREATE OR REPLACE VIEW collect.v_spinlock_stats AS SELECT * FROM collect.spinlock_stats;";

    /// <summary>
    /// V128 — the completion of V127 (#3540): <c>sample_interval_seconds</c> on the four delta families
    /// V127 left naked — <c>procedure_stats</c>, <c>memory_grant_stats</c>, <c>pg_wait_stats</c>,
    /// <c>pg_statement_stats</c> — and the two statement offsets on <c>query_stats</c> that its delta key
    /// is made of. After this rung EVERY member of <c>CollectorDeltaCalculator.DeltaFamilyCollectors</c>
    /// stores the interval its deltas accrued over, so the calculator's (delta 0, interval 0) "no delta
    /// knowable" marker reaches the store from every family and no restart zero reads as a measurement
    /// anywhere; Lite.Tests' <c>DeltaFamilyIntervalColumnTests</c> census asserts the set with nothing
    /// left on its still-naked list. Same <c>integer</c> type as the six that already carry it, so the
    /// one <c>NULLIF(sample_interval_seconds, 0)</c> idiom reads all ten. Pinned by
    /// <c>DeltaFamilyIntervalCompletionRungTests</c>.
    ///
    /// <para><b>Why five tables in one rung.</b> The repo allows one un-landed rung at a time, and these
    /// five changes are one change: every column here exists so the same reader idiom can be applied
    /// uniformly (stored interval → <c>NULLIF(…, 0)</c>; NULL → the LAG derivation; no <c>ELSE 0</c>),
    /// and the offsets exist so the restart seed can rebuild the one delta key the store could not
    /// reproduce. Five rungs would have been five fleet schema hops carrying one idea.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> on all six columns, matching V127 and every
    /// column-adding rung on a collector table (V80, V81, V121): a historical row never recorded its
    /// interval or its offsets, so NULL is the honest value. A backfilled 0 interval would stamp every
    /// pre-V128 row "unknowable" and blank 30 days of rate history; a backfilled 0/-1 offset pair would
    /// build a delta key nothing will ever present, and the seed would restore baselines under it
    /// silently. Readers treat the three interval states distinctly: <c>n &gt; 0</c> measured; <c>0</c>
    /// the unknowable marker, mapped to NULL (the point is absent, never 0.00); <c>NULL</c> a pre-V128
    /// row, falling back to the LAG-over-collection_time derivation those readers always used. The seed
    /// consumes only rows whose offsets are NOT NULL for keys, and every row for the pass window. A
    /// nullable no-default ADD COLUMN is catalog-only in PostgreSQL and TimescaleDB accepts it on a
    /// compressed hypertable with continuous aggregates attached (verified live on 2.28.1 against
    /// <c>procedure_stats</c> with its hourly/daily aggregates and <c>query_stats</c> with its hourly one),
    /// so this stays instant on a multi-hundred-GB store.</para>
    ///
    /// <para><b>The offsets' semantics, stated here because this is where the next reader will look.</b>
    /// <c>statement_start_offset</c> and <c>statement_end_offset</c> are <c>sys.dm_exec_query_stats</c>'s
    /// own columns: the statement's position inside its batch text in <b>BYTES</b> of the
    /// <c>nvarchar</c> text, not characters — so slicing the text at them divides by two (the
    /// collector's <c>SUBSTRING(st.text, (statement_start_offset / 2) + 1, …)</c>), and a reader who
    /// forgets the Unicode factor lands halfway into the wrong statement. <c>statement_end_offset = -1</c>
    /// means "to the end of the batch"; <c>(0, -1)</c> is the whole batch. They are stored
    /// <b>verbatim as the DMV reports them</b>, <c>-1</c> included and never normalized to a length,
    /// because the collector's delta key is <c>$"{sql_handle}:{start}:{end}:{plan_handle}"</c> over the raw
    /// ints and the seed has to spell the same string byte for byte.</para>
    ///
    /// <para><b>The PostgreSQL pair's columns are added in TWO places and both are required</b> — the V101
    /// rule. A store's tables come from one of two texts depending on when it was created: a fresh store
    /// builds every table from V1's generated schema, walked from the collector catalog, while a store that
    /// predates V63/V64 has whatever those rungs built. So the <c>pg_wait_stats</c> and
    /// <c>pg_statement_stats</c> CREATE TABLE rungs gain the column for the population that first meets them
    /// (<c>PgSchemaGeneratorTests</c> enforces the rung text against the generator, column for column) and
    /// THIS rung's ALTER carries the existing one. Neither is redundant: the CREATE is <c>IF NOT EXISTS</c>
    /// and never re-runs on a store that already has the table, and the ALTER is <c>ADD COLUMN IF NOT
    /// EXISTS</c> and is a no-op wherever the column already exists. The three SQL Server tables need no
    /// second site: they are generated at V1 and only ever ALTERed.</para>
    ///
    /// <para><b>Two view treatments.</b> <c>v_memory_grant_stats</c> is a <c>SELECT *</c> passthrough
    /// and is refreshed here for the V14/V80/V81/V127 reason (Postgres freezes the column list at CREATE;
    /// appending is the one alteration <c>CREATE OR REPLACE VIEW</c> permits). <c>procedure_stats</c>,
    /// <c>pg_wait_stats</c> and <c>pg_statement_stats</c> have no <c>v_*</c> view (their readers hit the
    /// base table; <c>PgSchemaGenerator.AllPassthroughViews</c> pins the set). <c>v_query_stats</c> is
    /// the #1767 payload-RESOLVING view, not a passthrough, and the generator emits payload columns BEFORE
    /// the trailing digest columns, so the two offsets land mid-list — an alteration
    /// <c>CREATE OR REPLACE VIEW</c> refuses. Hence <c>DROP VIEW</c> here and the regenerated resolving
    /// definition concatenated after this constant in the ladder entry, exactly as V51 and V121 did, with
    /// <c>V54Sql</c> and <c>GenerateQueryStatsPayloadColumnPreAdds()</c> ahead of it so a store climbing
    /// from below those rungs has every column the view names (<c>MigrationLadderPins</c>). Plain DROP,
    /// no CASCADE: nothing persistent depends on the view.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It does not touch
    /// <c>collect.wait_stats_baseline</c> (V127's stated follow-up is a new aggregate under a new name —
    /// an aggregate-plus-retirement operation, not a column, and not this rung). It does not backfill. It
    /// does not change <c>procedure_stats_hourly</c>/<c>_daily</c> or <c>query_stats_hourly</c>: those sum
    /// deltas, and a fabricated 0 adds nothing to a sum.</para>
    /// </summary>
    private const string V128Sql = @"
ALTER TABLE collect.procedure_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;
ALTER TABLE collect.memory_grant_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;
ALTER TABLE collect.pg_wait_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;
ALTER TABLE collect.pg_statement_stats
    ADD COLUMN IF NOT EXISTS sample_interval_seconds integer;

/* The delta key's two halves that were never stored. BYTE offsets into the batch's nvarchar text
   (a character position is offset / 2); statement_end_offset = -1 means ""to the end of the batch"";
   stored raw, -1 included, because the key string carries the raw values. */
ALTER TABLE collect.query_stats
    ADD COLUMN IF NOT EXISTS statement_start_offset integer;
ALTER TABLE collect.query_stats
    ADD COLUMN IF NOT EXISTS statement_end_offset integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the passthrough would keep serving the
   pre-V128 column list forever — the V14 lesson, restated by V80, V81 and V127. The other three interval
   tables have no v_ view. */
CREATE OR REPLACE VIEW collect.v_memory_grant_stats AS SELECT * FROM collect.memory_grant_stats;

/* v_query_stats is the payload-RESOLVING view (#1767), and its two new columns land ahead of the digest
   columns — mid-list, which CREATE OR REPLACE VIEW refuses. Dropped here; the ladder entry concatenates
   the regenerated resolving definition after the pre-adds, the V51/V121 idiom. */
DROP VIEW IF EXISTS collect.v_query_stats;";

    /// <summary>
    /// V129 — <c>collect.pg_log_events</c>, the classified PostgreSQL server-log events (#3601): errors,
    /// connections, lock waits, and the recognised-only spill / autovacuum / checkpoint shapes, out of the
    /// same log the deadlock (V103) and plan (V99) readers already tail. The keystone for #3602 and #3603,
    /// which each add one parser family on the pipeline this table stores.
    ///
    /// <para><b>The identity column is the point, for V103's reason.</b> The <c>pg_read_file</c> transport
    /// re-reads an OVERLAPPING tail every cycle on purpose, so without <c>raw_line_hash</c> the same event
    /// is stored once per cycle for as long as it stays inside the window; every read dedupes on it as the
    /// deadlock reads do on <c>deadlock_hash</c>. Over the RAW entry text rather than the stored columns,
    /// because two events that store alike — the same error from one statement shape run with two values, same
    /// millisecond, same pid — are two events, and the hash must keep them apart. It is NOT secret-safe (#3996's
    /// review): an unkeyed hash of text a reader can mostly rebuild is an offline guessing oracle for the rest, a
    /// failed UPDATE's four-digit PIN included, so no read surface returns it (#4004).</para>
    ///
    /// <para><b>The statement itself is never stored, and the SQL the other columns quote is normalized.</b>
    /// <c>statement_fingerprint</c> is a hash of the REDACTED <c>STATEMENT</c> companion, the plan parser's own
    /// patterns applied to SQL, so one statement shape recurs to one fingerprint and the statement's quoted
    /// literals and numbers exist nowhere in the store (a dollar-quoted body is hashed as written, #3996's
    /// review). <c>message</c>, <c>detail</c> and <c>context</c> are PostgreSQL's prose as
    /// written (#3944), except the SQL PostgreSQL writes into a DETAIL or CONTEXT (a deadlock's queries, a
    /// function's statement), which <c>PgLogTextRedactor</c> normalizes with every literal replaced by
    /// <c>?</c> (#3920). Both are enforced at the row constructor rather than by convention.</para>
    ///
    /// <para><b>Retention is thirty days, not the deadlock table's ninety</b> (<c>CollectorScheduleDefaults</c>
    /// carries the argument): a deadlock is rare by construction and a log event is as common as the
    /// target's settings let it be — <c>log_connections</c> on a reconnect-per-statement pool writes three
    /// rows per query. Hypertable conversion, one-day chunks, compression segmented by <c>server_id</c> and
    /// the retention policy all follow from the catalog entry, as for every collector table.</para>
    ///
    /// <para><b>One index, the generated one.</b> The read filters on <c>(server_id, collection_time)</c>
    /// first — chunk exclusion — and on family and severity within that window; a <c>(server_id, family,
    /// collection_time)</c> index would help a family-filtered read over a long window on a loud target,
    /// and it is a SEPARATE rung when it comes, for V104's reason: <c>PgSchemaGeneratorTests</c> requires a
    /// collector's rung to be exactly what the generator emits, and the generator emits one index, so a
    /// second one in this rung would give the upgraded store an index the fresh store never gets. All
    /// value columns nullable, matching the generated schema this must be identical to.</para>
    ///
    /// <para><b>The thirteen columns after <c>raw_line_hash</c> are V130's (#3602, #3603), and they are here
    /// for the V101 rule.</b> A store's tables come from one of two texts: a fresh store builds every table
    /// from the generated schema at V1 (this CREATE then no-ops, <c>IF NOT EXISTS</c>), while a store that
    /// climbed through V129 before V130 existed has the seventeen-column table this text built at the time.
    /// <c>PgSchemaGeneratorTests</c> requires this rung to be the generator's output column for column, and
    /// the generator emits the collector's CURRENT columns — so this text carries them for the fresh
    /// population, and V130's ALTER carries them for the existing one. Neither is redundant; dropping
    /// either leaves one population permanently without the columns.</para>
    /// </summary>
    private const string V129Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_log_events (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    occurred_at timestamp,
    family text,
    severity text,
    sqlstate text,
    database_name text,
    user_name text,
    application_name text,
    pid integer,
    message text,
    detail text,
    context text,
    statement_fingerprint text,
    raw_line_hash text,
    relation_name text,
    bytes bigint,
    duration_ms bigint,
    pages_removed bigint,
    pages_remaining bigint,
    tuples_removed bigint,
    tuples_remaining bigint,
    buffer_hits bigint,
    buffer_misses bigint,
    buffer_dirtied bigint,
    wal_records bigint,
    wal_bytes bigint,
    is_analyze boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_log_events_time
    ON collect.pg_log_events(server_id, collection_time);";

    /// <summary>
    /// V130 — the family-specific NUMBERS on <c>collect.pg_log_events</c> (#3602, #3603): a spill's bytes,
    /// and an autovacuum run's relation, duration, pages, tuples, buffers and WAL. V129 stored those two
    /// families as recognised-only events — the line's prose and nothing lifted — and this rung
    /// is where the prose becomes columns a reader can sum, rank and alert on.
    ///
    /// <para><b>Columns on the event row, not sibling tables — the shape #3601 planned and this rung
    /// declined, deliberately.</b> A <c>pg_temp_files</c> and a <c>pg_autovacuum_runs</c> table would each
    /// have needed its own identity column and its own dedupe of the same overlapping tail, its own
    /// retention, its own hypertable and compression policy, its own CI worker slot, and a join on
    /// <c>raw_line_hash</c> for every read that wanted the event beside its numbers. Thirteen nullable
    /// columns cost none of that: the row already has the identity, the dedupe, the fingerprint and the
    /// retention, and "everything at 03:07" and "what did the 03:07 spill cost" are one query. The
    /// <c>family</c> column says which columns can be non-null — <c>bytes</c> on <c>temp_file</c>, the rest
    /// on <c>autovacuum</c>, nothing on the others — so a reader is never guessing what a NULL means on a
    /// row whose family cannot carry the figure. What this trades away is per-family retention (a spill
    /// history longer than thirty days would want its own table) and per-family indexing; both are the
    /// separate rungs V129's doc already reserves.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, matching every column-adding rung on a collector
    /// table (V80, V81, V101, V114, V121, V127, V128). The rows stored under V129 were recognised-only and
    /// never had the figures lifted; NULL is the honest value for them, and re-parsing thirty days of
    /// redacted <c>message</c> text to fill them in would be re-deriving from a copy what the log itself
    /// still holds for the collector to re-read. A nullable no-default ADD COLUMN is a catalog-only change
    /// in PostgreSQL and TimescaleDB accepts it on a compressed hypertable (V127 and V128 verified the same
    /// shape live on 2.28.1 against tables with continuous aggregates attached; this table has none), so
    /// this stays instant however loud the target has been. Compression segments by <c>server_id</c> as
    /// before; the new columns compress as ordinary members.</para>
    ///
    /// <para><b>Every column is <c>bigint</c> except the two that are not, and the types are the
    /// collector's.</b> Bytes, pages, tuples, buffers and WAL figures are all counts PostgreSQL prints as
    /// 64-bit integers; <c>duration_ms</c> is whole milliseconds from a centisecond figure. <c>relation_name</c>
    /// is <c>text</c>, <c>schema.table</c>, the key <c>get_pg_autovacuum_health</c> joins on; <c>is_analyze</c>
    /// is <c>boolean</c>, false for a vacuum and true for an analyze, NULL for a row of any other family.
    /// <c>PgSchemaGeneratorTests</c> renders V129 from <c>PayloadColumns</c> and requires it to match column
    /// for column, so a type here that differs from the collector's declaration is a build failure.</para>
    ///
    /// <para><b>The columns are added in TWO places and both are required</b> — the V101 rule. V129's CREATE
    /// carries them for the fresh population (walked from the generator at V1, and never re-run on a store
    /// that has the table); this ALTER carries them for the store that built the seventeen-column table
    /// first. <c>ADD COLUMN IF NOT EXISTS</c> is a no-op on the fresh store that already has them.</para>
    ///
    /// <para>No view refresh: <c>collect.pg_log_events</c> has no <c>v_</c> passthrough. No index: the reads
    /// still enter through <c>idx_pg_log_events_time</c> and filter within the window; a
    /// <c>(server_id, relation_name, collection_time)</c> index for the per-table run history is the same
    /// separate rung as the family index, when a long window on a loud target asks for it.</para>
    /// </summary>
    private const string V130Sql = @"
ALTER TABLE collect.pg_log_events
    ADD COLUMN IF NOT EXISTS relation_name text,
    ADD COLUMN IF NOT EXISTS bytes bigint,
    ADD COLUMN IF NOT EXISTS duration_ms bigint,
    ADD COLUMN IF NOT EXISTS pages_removed bigint,
    ADD COLUMN IF NOT EXISTS pages_remaining bigint,
    ADD COLUMN IF NOT EXISTS tuples_removed bigint,
    ADD COLUMN IF NOT EXISTS tuples_remaining bigint,
    ADD COLUMN IF NOT EXISTS buffer_hits bigint,
    ADD COLUMN IF NOT EXISTS buffer_misses bigint,
    ADD COLUMN IF NOT EXISTS buffer_dirtied bigint,
    ADD COLUMN IF NOT EXISTS wal_records bigint,
    ADD COLUMN IF NOT EXISTS wal_bytes bigint,
    ADD COLUMN IF NOT EXISTS is_analyze boolean;";

    /// <summary>
    /// V131 — <c>config.config_notification_routes</c>, the sparse routes table that lets alert FAMILIES land
    /// on different channels (#3598). Before it the notification model had one routing decision, made at
    /// install time: the singleton <c>config_notification</c> row holds one destination per channel TYPE and
    /// the fan-out sends every alert to every configured one. A self-monitor alert about the store, a
    /// collector-cost digest, a failed Agent job and a deadlock page interleaved on one channel, and the
    /// reader did the routing in their head on every post. This table is the layer over the parent row that
    /// <c>config_collector_schedules</c> is over the code defaults: a row per family (or per exact metric
    /// name) with a destination per channel, EMPTY meaning "inherit the parent's", so a store with zero rows
    /// behaves byte for byte as it did and the rung is a no-op for every existing install.
    ///
    /// <para><b>The columns mirror the parent row's destinations, one per channel type</b> —
    /// <c>teams_url</c>, <c>slack_url</c>, <c>generic_url</c>, <c>pagerduty_routing_key</c>,
    /// <c>smtp_recipients</c> — spelled identically so the same ACL list, the same secret classification and
    /// the same reader idiom apply. Proxies, the generic channel's headers/template and the PagerDuty region
    /// flag are NOT mirrored: they say how a channel type is reached, not where a family lands. <c>NOT NULL
    /// DEFAULT ''</c> like the parent's, so "inherit" and "unset" are one state and the resolver has one
    /// test. <c>metric_match</c> is a family name from the closed <c>AlertFamily</c> taxonomy or an exact
    /// metric name; the CHECK refuses a blank one, which would match nothing and read as a configured route.</para>
    ///
    /// <para><b><c>configured_channels</c> is generated, and exists for the roles that may not read the
    /// URLs.</b> Four of the five destination columns are bearer secrets exactly as they are on the parent
    /// row, so they are carved from the <c>viewer</c> and <c>mcp</c> roles' SELECT
    /// (<c>DarlingManagedRoles.ViewerRestrictedConfigTables</c>). Without this column those roles could list
    /// a route and not say which channels it configures — the one fact a settings read needs. A STORED
    /// generated column over the secret columns discloses presence and nothing else (column-level SELECT
    /// is per column), so the MCP <c>get_alert_settings</c> and a read-only seat can say "route 3 sets
    /// Slack and PagerDuty" without ever holding either value. <c>array_remove</c> and <c>&lt;&gt;</c> are
    /// IMMUTABLE, which a generated expression requires.</para>
    ///
    /// <para><b>The trigger is V117's shape</b>: statement-level, <c>config.config_bump_version()</c> verbatim,
    /// <c>AFTER INSERT OR UPDATE OR DELETE</c> — DELETE because it is the direction that costs most (an
    /// operator removes a route believing alerts fall back to the parent, and a stale route list keeps
    /// sending them elsewhere). The service's <c>StoreConfigProvider</c> reads this table with the parent
    /// row on every beacon change, so a route lands on the next firing with no restart. <c>DROP TRIGGER IF
    /// EXISTS</c> first so a replay is a harmless no-op. <c>GENERATED ALWAYS AS IDENTITY</c> like
    /// <c>config_command</c>, so the admin INSERT needs no sequence USAGE grant.</para>
    ///
    /// <para><b>No GRANT here.</b> <c>admin</c> gets its writes from the schema-wide default privileges;
    /// the <c>viewer</c>/<c>mcp</c> column carve and the mcp role's narrow <c>UPDATE (enabled, modified_at)</c>
    /// + <c>DELETE</c> are provisioning statements, re-asserted every managed start after migration, and
    /// hand-mirrored in <c>Darling/tools/provision-roles.sql</c> for bring-your-own stores (the drift test
    /// holds the two together). Pinned by <c>NotificationRoutesRungTests</c>.</para>
    /// </summary>
    private const string V131Sql = @"
CREATE TABLE IF NOT EXISTS config.config_notification_routes (
    route_id integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    metric_match text NOT NULL,
    teams_url text NOT NULL DEFAULT '',
    slack_url text NOT NULL DEFAULT '',
    generic_url text NOT NULL DEFAULT '',
    pagerduty_routing_key text NOT NULL DEFAULT '',
    smtp_recipients text NOT NULL DEFAULT '',
    configured_channels text[] GENERATED ALWAYS AS (array_remove(ARRAY[
        CASE WHEN teams_url <> '' THEN 'Teams' END,
        CASE WHEN slack_url <> '' THEN 'Slack' END,
        CASE WHEN generic_url <> '' THEN 'Generic' END,
        CASE WHEN pagerduty_routing_key <> '' THEN 'PagerDuty' END,
        CASE WHEN smtp_recipients <> '' THEN 'Email' END], NULL)) STORED,
    enabled boolean NOT NULL DEFAULT TRUE,
    modified_at timestamp NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    CONSTRAINT ck_config_notification_routes_metric_match CHECK (btrim(metric_match) <> '')
);

DROP TRIGGER IF EXISTS trg_bump_notification_routes ON config.config_notification_routes;
CREATE TRIGGER trg_bump_notification_routes
    AFTER INSERT OR UPDATE OR DELETE ON config.config_notification_routes
    FOR EACH STATEMENT EXECUTE FUNCTION config.config_bump_version();";

    /// <summary>
    /// V132 — <c>cntr_type</c> on <c>collect.perfmon_stats</c> (#3653 A7's rung; the measurement contract's
    /// rule 8, "gauges are never delta'd"): the Windows performance-counter type id
    /// <c>sys.dm_os_performance_counters</c> reports for every row, stored beside the raw value so the store
    /// can finally say which of its perfmon rows are COUNTS and which are LEVELS. Until this rung the
    /// collector differenced every counter it read — <c>Total Server Memory (KB)</c> exactly like
    /// <c>Batch Requests/sec</c> — and a target that released memory, a FALLING level, presented to the
    /// shared delta calculator as a counter reset: the (0, 0) "no delta knowable" marker written at exactly
    /// the moment the drop mattered, and the perfmon chart drawing a level's meaningless per-sweep
    /// difference as "activity". The viewers had to classify by a name-suffix proxy (<c>/sec</c> = rate,
    /// #3702) because the row could not tell them. Pinned by <c>PerfmonCounterTypeRungTests</c>.
    ///
    /// <para><b>What the column means, row by row.</b> The DMV's raw <c>cntr_type</c> word, stored verbatim:
    /// <c>272696576</c> (<c>PERF_COUNTER_BULK_COUNT</c>) is a rate and the row keeps its pre-rung write — raw
    /// value, delta, measured interval; <c>65792</c> (<c>PERF_COUNTER_LARGE_RAWCOUNT</c>) is a gauge and the
    /// collector now writes the raw value with <c>delta_cntr_value</c> AND <c>sample_interval_seconds</c>
    /// NULL — NULL, not (0, 0), because 0 is the calculator's "no delta knowable" marker and a gauge has no
    /// delta to know; the average/fraction/base family (<c>1073874176</c> <c>PERF_AVERAGE_BULK</c> and
    /// siblings) keeps the pre-rung write too, its delta a real per-interval change of the raw value that the
    /// stored type tells a reader not to divide. The vocabulary and the three-way reading every consumer
    /// applies live in <c>PerformanceMonitor.Common.PerfmonCounterTypes</c>; the collector's own gauge set
    /// is pinned equal to it.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, matching V127, V128 and every column-adding rung on a
    /// collector table: a row written before this rung never recorded its type, and NULL is the honest value
    /// for it. Readers treat NULL as "classify as you did before the rung" — the viewers fall back to the
    /// #3702 name proxy, the MCP tools publish <c>counter_kind: null</c> and say so — so history renders
    /// exactly as the operator last saw it, and the type decides the moment a row carries one. Because a
    /// counter's type does not change, the viewers take ANY row's non-null type as the series' type: a
    /// gauge's whole 30-day history plots as a level the morning after the upgrade (its pre-rung rows stored
    /// <c>cntr_value</c> all along), which is the correction, not a discontinuity. No backfill could do
    /// better — the store holds no way to recover a type from a name — and a backfilled 0 would be a lie
    /// about every historical row.</para>
    ///
    /// <para><b>The compressed-chunk shape.</b> <c>perfmon_stats</c> is a compressed hypertable on the fleet
    /// (one-day chunks, compression segmented by <c>server_id</c>, 30 days of raw retention like its
    /// delta-family siblings). A nullable, default-less <c>ADD COLUMN</c> is catalog-only in PostgreSQL and TimescaleDB
    /// accepts it on a compressed hypertable with a compression policy attached — the shape V127 and V128
    /// used on <c>wait_stats</c> and <c>procedure_stats</c> and verified live on 2.28.1; verified again for
    /// this rung against a store stopped at 131 with a compressed <c>perfmon_stats</c> chunk, which climbed
    /// to 132 in one rung with the pre-rung rows reading NULL through the compressed chunk. A DEFAULT would
    /// be the trap: TimescaleDB has to rewrite compressed segments to honour one, which on a
    /// multi-hundred-GB table is neither instant nor safe inside the migration transaction.</para>
    ///
    /// <para><b>The passthrough view is refreshed</b> for the V14/V80/V81/V127/V128 reason: Postgres freezes
    /// a view's <c>SELECT *</c> column list at CREATE, so without the <c>CREATE OR REPLACE VIEW</c> every
    /// <c>v_perfmon_stats</c> reader — both trend reads, the latest-snapshot read, the analysis facts —
    /// would never see the column. Appending is the one alteration <c>CREATE OR REPLACE VIEW</c> permits,
    /// which is exactly what an <c>ADD COLUMN</c> produces. A fresh store gets the column from the generated
    /// CREATE TABLE at V4 (the collector definition carries it, appended last) and the ALTER no-ops there;
    /// the view refresh is idempotent either way.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It does not divide a <c>PERF_AVERAGE_BULK</c>
    /// numerator by its <c>PERF_LARGE_RAW_BASE</c> sibling (the default list carries ten such instance rows,
    /// the wait-statistics <c>Average wait time (ms)</c> family; their per-interval delta is plotted as
    /// exactly that and the average their name promises needs a join this store does not make — a stated
    /// finding, its own lane). It does not touch <c>perfmon_interval_baseline</c> or the analysis facts
    /// (<c>Batch Requests/sec</c> and the compilation counters only, all rates). It does not backfill.</para>
    /// </summary>
    private const string V132Sql = @"
ALTER TABLE collect.perfmon_stats
    ADD COLUMN IF NOT EXISTS cntr_type integer;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the passthrough would keep serving the
   pre-V132 column list forever — the V14 lesson, restated by V80, V81, V127 and V128. Appending is the one
   alteration CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN produces. */
CREATE OR REPLACE VIEW collect.v_perfmon_stats AS SELECT * FROM collect.perfmon_stats;";

    /// <summary>
    /// V133 — two nullable integer columns on two PostgreSQL-target tables, the ONE rung of the #3691 v2 wave:
    /// <c>numbackends</c> on <c>collect.pg_database_stats</c> and <c>sampled_ms</c> on
    /// <c>collect.pg_wait_sampling</c>. Both are written by their collectors from this rung on and READ BY
    /// NOTHING yet — the analysis-side consumers (a connection-saturation numerator, a duty-cycle-honest
    /// sampled wait rate) are follow-on lanes once the fleet has rows to calibrate against. Pinned by
    /// <c>PgNumbackendsAndSampledMsRungTests</c>.
    ///
    /// <para><b><c>numbackends</c> — the saturation NUMERATOR the design wanted.</b>
    /// <c>pg_stat_database.numbackends</c> is the number of backends currently connected to each database:
    /// a LEVEL, not a counter, client-only (background workers are not counted), universal (present since
    /// PostgreSQL 8.x, so every major this repo sweeps reports it) and sampled every minute with the rest of
    /// the row. The v1 saturation read had only <c>pg_session_states.total_sessions</c>, which is an
    /// exception capture — that collector stores the sessions holding an open transaction old enough to
    /// matter, so its count is the population that met its capture rule, not the connected population — and
    /// an exception count over <c>max_connections</c> is not a saturation fraction. Stored per database like
    /// every other column of the row
    /// (a consumer sums across databases for the cluster figure, and the per-database split is itself a
    /// finding: one database holding 900 of 1,000 slots is a different conversation from ten holding 90).
    /// NULL on a pre-rung row, which a reader must treat as "not sampled" rather than 0 connections.</para>
    ///
    /// <para><b><c>sampled_ms</c> — the duty-cycle denominator the sampler arm owed its readers.</b> The #3604
    /// service-side sampler stores <c>profile_period_ms = 1000</c> and its cumulative tally, but it OBSERVES
    /// only <c>SamplerSnapshotsPerCycle</c> one-second snapshots (30 s) of each 300 s cycle, so
    /// <c>Δsamples × period / observed_window</c> — the arithmetic a rate read over the analysis window
    /// naturally writes — understates that arm's wait rate by the duty cycle, roughly 10×. The instrument
    /// token that would let a reader correct for it lives in <c>collector_state</c>, outside the analysis
    /// FROM/JOIN whitelist, and the period alone cannot say how much of the interval was watched. This column
    /// stores, on every row a collection writes, the milliseconds of observation that collection folded into
    /// the tally: the number of snapshots actually read × the period (30 × 1,000 = 30,000 on a full window;
    /// fewer if the batch was cut short). The extension arm writes NULL — the module samples in-engine every
    /// <c>profile_period</c> for the WHOLE interval, so the interval IS the observed time and there is no
    /// duty cycle to disclose — and a reader treats NULL as "period × count over the row's whole interval",
    /// which is exactly today's behaviour. Per row rather than cumulative-in-state: the observed window of one
    /// collection is a fact the collection knows and nothing else can recover, a per-row value survives a
    /// tally reset or a cap-drop without a second counter to keep in step with the first, and a consumer that
    /// wants the window's total sums it over the collections between its two endpoints (one value per
    /// <c>collection_time</c>; every row of a collection carries the same figure, like
    /// <c>profile_period_ms</c>).</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, matching V127, V128 and V132 and every column-adding
    /// rung on a collector table: a pre-rung row never sampled either value, and NULL is the honest word for
    /// it. Both tables are compressed hypertables on the fleet (one-day chunks, segmented by <c>server_id</c>,
    /// 30 days of raw retention) and a nullable, default-less <c>ADD COLUMN</c> is catalog-only in PostgreSQL;
    /// TimescaleDB accepts it on a compressed hypertable with a compression policy attached — the shape V127
    /// and V128 used on <c>wait_stats</c>, <c>pg_wait_stats</c> and <c>pg_statement_stats</c> and V132 on
    /// <c>perfmon_stats</c>, verified live on 2.28.1 each time. A DEFAULT would be the trap: TimescaleDB has
    /// to rewrite compressed segments to honour one.</para>
    ///
    /// <para><b>No view to refresh.</b> Neither table has a <c>v_</c> passthrough — the PostgreSQL collector
    /// tables have been view-less since V63 (V83's doc says so of <c>pg_database_stats</c>; V128 restated it
    /// when it dressed <c>pg_wait_stats</c>), which is why this rung is two ALTERs and nothing else, unlike
    /// V132's ALTER-plus-view. A fresh store gets both columns from the
    /// generated CREATE TABLE (each collector definition carries its column, appended LAST so the positional
    /// COPY writer and an upgraded store's ALTER agree on where it sits) and the ALTERs no-op there. The V101
    /// rule applies: V83's and V96's CREATE texts carry the columns too, because <c>PgSchemaGeneratorTests</c>
    /// holds every collector rung column-for-column equal to the generator's CURRENT output, and a store that
    /// climbed through them before this rung existed is exactly the population these ALTERs are for.</para>
    ///
    /// <para><b>Lite is unchanged.</b> Lite stores no <c>pg_*</c> table
    /// (<c>DuckDbSchemaGenerator.StoredCollectors</c> is the SQL Server set), so there is no twin column and
    /// no DuckDB schema bump — the V96 decision, where V128's Lite bump was for the SQL Server families it
    /// also dressed, not for <c>pg_wait_stats</c>.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It reads neither column anywhere: no MCP tool
    /// projects <c>numbackends</c>, no fact divides by it, and
    /// <c>DarlingPgWaitSamplingReader.EstimatedWaitMs</c> keeps its <c>samples × period</c> arithmetic with
    /// a doc-comment naming <c>sampled_ms</c> as the denominator a rate read must use. It does not backfill,
    /// touch <c>pg_session_states</c>, or change either collector's existing columns.</para>
    /// </summary>
    private const string V133Sql = @"
ALTER TABLE collect.pg_database_stats
    ADD COLUMN IF NOT EXISTS numbackends integer;

ALTER TABLE collect.pg_wait_sampling
    ADD COLUMN IF NOT EXISTS sampled_ms integer;";

    /// <summary>
    /// V134 — the "time honesty" rung (#3653 item 13, rulings Q7 and Q8): two nullable columns that let the store
    /// say, for the first time, WHICH clock two of its values are in. <c>sample_time_utc</c> on
    /// <c>collect.cpu_utilization_stats</c> — the same instant <c>sample_time</c> records, in UTC — and
    /// <c>time_zone_id</c> on <c>collect.server_properties</c> — the monitored engine's own time-zone name,
    /// beside the <c>utc_offset_minutes</c> V16 added. Pinned by <c>TimeHonestyRungTests</c>; the Lite twin is
    /// schema v63 (<c>DuckDbInitializer</c>).
    ///
    /// <para><b>The two lies this rung retires, stated plainly.</b> First: <c>cpu_utilization_stats.sample_time</c>
    /// is the MONITORED SERVER'S LOCAL wall clock (<c>SYSDATETIME()</c> minus each ring-buffer entry's age) in a
    /// store whose every other timestamp is naive UTC. That was a documented convention rather than an
    /// accident — Lite plots it in the server's own frame and it is the collector's watermark — but every
    /// reader that windows or aligns it against UTC had to DERIVE the UTC value: Darling's #1262 de-skew recovers
    /// the offset per batch as <c>MAX(sample_time) OVER (batch) - collection_time</c> rounded to fifteen minutes,
    /// and Lite shifts its whole window by the single <c>utc_offset_minutes</c> the store holds now. Both
    /// derivations assume one offset per batch or per server, and across a DST transition that is false for
    /// exactly the rows nearest the change: the batch that straddles it rounds to one side, the Lite window
    /// applies today's offset to yesterday's samples, and the misplacement is one hour, silent, and in the
    /// plausible direction. Second: <c>server_properties.utc_offset_minutes</c> is
    /// <c>DATEDIFF(MINUTE, GETUTCDATE(), GETDATE())</c> — an OFFSET, the one in force at collection — and #3231
    /// already documented that subtracting it from a stored instant that can outlive a transition
    /// (<c>get_index_usage</c>'s <c>last_user_access</c>, the PVS cleaner stamps) is exact on one side and an
    /// hour wrong on the other, with nothing in the store able to say which side a given instant was on. An
    /// offset cannot; a ZONE can, because <c>AT TIME ZONE</c> applies the rule that was in force at that
    /// instant. The ruling: add the UTC twin as a column and store the zone id, both nullable, the offset kept
    /// alongside — "readers prefer the new column when present".</para>
    ///
    /// <para><b><c>sample_time_utc</c> — what it is and what it is not.</b> Written by <c>CpuUtilizationCollector</c>
    /// from this rung on, on every row, beside an UNCHANGED <c>sample_time</c>: on the ring-buffer arm it is
    /// the identical two-step <c>DATEADD</c> anchored on <c>SYSUTCDATETIME()</c> instead of <c>SYSDATETIME()</c>
    /// (both are runtime constants folded once per statement, so the pair differs by exactly the server's
    /// offset at the poll); on the Azure SQL DB arm it is <c>drs.end_time</c>, which is documented UTC and
    /// which <c>sample_time</c> already reads — Azure SQL Database's clock IS UTC, so the two frames coincide
    /// there and neither column lies. <c>sample_time</c> is NOT converted to UTC, deliberately and for two
    /// reasons the ruling names: existing rows would be indistinguishable from converted ones (a column that
    /// is local before some instant and UTC after it is worse than either), and it is the watermark — a
    /// watermark that changed frame would re-ingest or skip one offset's worth of samples on the first poll
    /// after the upgrade. The readers that compare the sample against a UTC window — the viewer's raw CPU read
    /// and the MCP <c>get_cpu_utilization</c> read on this side, the Lite CPU window on the other — now take
    /// <c>COALESCE(sample_time_utc, …)</c> with their pre-rung derivation as the fallback, so a post-rung row
    /// is placed by a stored UTC instant and a pre-rung row renders exactly as it did. The readers that want
    /// the server's frame (Lite's chart x-axis, the viewer's Server-time mode) keep <c>sample_time</c>; the
    /// latest-row reads that ORDER BY it as a within-batch tiebreak (<c>DarlingWorker.LatestCpuSql</c> and its
    /// three siblings) keep it too, because an ORDER BY carries no clock frame and — the load-bearing
    /// reason — that read's <c>sample_time</c> is the CPU alert gate's observation identity, compared only
    /// against the value the same read stored last sweep: switching it to the UTC column would make the first
    /// post-upgrade sample read OLDER than the last pre-upgrade one on every server east of UTC, and freeze
    /// the gate for one offset's worth of hours (#3282 named that trap when it left the value
    /// <c>Kind=Unspecified</c>).</para>
    ///
    /// <para><b><c>time_zone_id</c> — what it is and where it is.</b> <c>CURRENT_TIMEZONE_ID()</c>, the engine's
    /// own zone name (a Windows zone id such as <c>Eastern Standard Time</c>, the key <c>AT TIME ZONE</c> and
    /// <c>sys.time_zone_info</c> use), collected by <c>ServerPropertiesCollector</c> beside the offset. It lives on
    /// <c>server_properties</c> because THAT is where the offset lives (V16) and the ruling says the offset
    /// stays alongside; the brief that dispatched this lane said "server registry", and this rung says
    /// <c>server_properties</c> so the next reader does not go looking at <c>servers</c>. NULL is a REAL value
    /// here and the common one on an older fleet: the function exists on SQL Server 2022+ and Azure SQL
    /// Database / Managed Instance only, and on an older engine it is not a missing object but a missing
    /// built-in — a batch that names it fails to compile as a whole — so the collector reads it in its own
    /// <c>sp_executesql</c> batch behind a version / edition gate and inside <c>TRY … CATCH</c>, and writes NULL
    /// where the engine cannot say. Readers publish NULL as "pre-2022 engine: only the offset is known";
    /// nothing in this rung converts through the zone yet (an <c>AT TIME ZONE</c> read boundary for the #3231
    /// fields is the consumer lane this column exists for), which is why the offset is kept rather than
    /// replaced: every de-skew in the tree still runs on it.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, matching V127, V128, V132 and V133 and every
    /// column-adding rung on a collector table — and here the no-backfill is not merely the safe choice but
    /// the ONLY honest one. A pre-rung <c>sample_time</c> could be converted to UTC only by subtracting the
    /// offset its server had AT THAT SAMPLE'S INSTANT, and the store never recorded that offset: it holds the
    /// offset at each <c>server_properties</c> collection (a sparse, on-load series) and the per-batch estimate
    /// #1262 derives, both of which are exactly the DST-blind derivations this column replaces. A backfilled
    /// value would be the old lie written into the new column, where a reader could no longer tell it from a
    /// measurement. NULL says "not recorded", the readers fall back to what they always did, and the column
    /// decides the moment a row carries one. Likewise no zone can be backfilled from an offset — several zones
    /// share every offset. Both tables are compressed hypertables on the fleet (one-day chunks, segmented by
    /// <c>server_id</c>; <c>cpu_utilization_stats</c> carries the 30-day service-side retention V115's
    /// neighbour describes) and a nullable, default-less <c>ADD COLUMN</c> is catalog-only in PostgreSQL;
    /// TimescaleDB accepts it on a compressed hypertable with a compression policy attached — the shape V127,
    /// V128, V132 and V133 used, verified live on 2.28.1 each time. A DEFAULT would be the trap: TimescaleDB has
    /// to rewrite compressed segments to honour one.</para>
    ///
    /// <para><b>One view refreshed, one not.</b> <c>cpu_utilization_stats</c> HAS a <c>v_</c> passthrough (V4's
    /// <c>v_cpu_utilization_stats</c>, which the viewer's latest-CPU read, the fleet read, the analysis facts and
    /// the daily summary all read), and Postgres freezes a view's <c>SELECT *</c> column list at CREATE, so
    /// without the <c>CREATE OR REPLACE VIEW</c> here every one of those readers would never see the column
    /// — the V14 lesson, restated by V80, V81, V127, V128 and V132. Appending is the one alteration
    /// <c>CREATE OR REPLACE VIEW</c> permits, which is exactly what an <c>ADD COLUMN</c> produces.
    /// <c>server_properties</c> has no passthrough (V16 says so; <c>PgSchemaGenerator.AllPassthroughViews</c>
    /// agrees), so its ALTER stands alone. A fresh store gets both columns from the generated CREATE TABLE at
    /// V1 (each collector definition carries its column, appended LAST so the positional COPY writer and an
    /// upgraded store's ALTER agree on where it sits) and the ALTERs no-op there; the view refresh is
    /// idempotent either way.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It does not convert <c>sample_time</c>, rename it,
    /// or move the watermark off it. It does not backfill either column. It does not touch the Compose
    /// catalog's CPU measures (they bucket on <c>collection_time</c>, the collector prefix, and never read
    /// <c>sample_time</c>), the analysis windows (likewise <c>collection_time</c>-bounded), or any of the
    /// #3231 de-skews — the zone is stored so that a later lane can apply <c>AT TIME ZONE</c> at those read
    /// boundaries, and until then the offset does what it did. It does not publish the zone anywhere but
    /// <c>get_server_properties</c> (both SKUs), and it does not add a viewer surface for it.</para>
    /// </summary>
    private const string V134Sql = @"
ALTER TABLE collect.cpu_utilization_stats
    ADD COLUMN IF NOT EXISTS sample_time_utc timestamp;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the passthrough would keep serving the
   pre-V134 column list forever — the V14 lesson, restated by V80, V81, V127, V128 and V132. Appending is
   the one alteration CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN produces. */
CREATE OR REPLACE VIEW collect.v_cpu_utilization_stats AS SELECT * FROM collect.cpu_utilization_stats;

/* server_properties has no v_ passthrough (V16), so this ALTER stands alone. */
ALTER TABLE collect.server_properties
    ADD COLUMN IF NOT EXISTS time_zone_id text;";

    /// <summary>
    /// V135 — the Long-Running Query alert's OPT-OUT knob on the singleton <c>config_alert_settings</c> row
    /// (#3653 A5, ruling Q5): two <c>text[]</c> lists — <c>program_name</c> PREFIXES and exact <c>login_name</c>
    /// values — whose sessions the alert does NOT EVALUATE: not read into the decision, not counted, not
    /// fingerprinted, as opposed to a mute rule, which silences a fire already decided. The shape is
    /// <c>excluded_databases</c>'s (V17), <c>text[] NOT NULL</c>; the DEFAULT is not.
    ///
    /// <para><b>Why a knob and not a gate.</b> Measured on one production store class: 191 distinct sessions over
    /// the 30-minute bar in 7 days, the p90 of them seen in 6,192 snapshots — permanent background requests that
    /// are over any duration threshold forever, so no persistence gate can separate them from a runaway query.
    /// Coverage is the fix. The match rule (programs by case-insensitive PREFIX — job-step names embed the job
    /// id; logins EXACT, case-insensitive — a prefix would let <c>svc</c> swallow <c>svc_owner</c>; no wildcard
    /// grammar) is spelled once in <c>PerformanceMonitor.Alerting.LongRunningQueryExclusions</c> and applied
    /// INSIDE both SKUs' reads ahead of the row cap; the store holds the normalised lists (trimmed, blanks
    /// dropped, de-duplicated case-insensitively).</para>
    ///
    /// <para><b>The DEFAULT is the production read's seeds, and this is the one place a knob rung on this row
    /// has shipped a non-empty list.</b> A 7-day read of one large production store split the long-running
    /// population into four classes: (1) SQL Agent job-step programs — <c>program_name</c> starting
    /// <c>SQLAgent - TSQL JobStep</c>, ~460 sessions a week across 11 single-server jobs, medians 35–62 minutes
    /// — the seeded PREFIX; (2) the <c>NT AUTHORITY\SYSTEM</c> and <c>NT AUTHORITY\NETWORK SERVICE</c> logins —
    /// the permanent multi-DAY background, ~70 sessions across 42 servers, medians 4.8–8.6 days, 300K+
    /// snapshots, CDC-capture shaped — the seeded LOGINS; (3) the application's admin login — deliberately NOT
    /// a default, because it carries the job wave but also real ad-hoc long-runners, and the job-step prefix
    /// already removes its share; (4) named humans — 3 sessions a week, never excluded: they are what the page
    /// is for. A pre-rung row therefore reads as the seeds, which is exactly what <c>DarlingAlertSettings</c>
    /// returned before this rung and what Lite ships, so the two SKUs and the two sides of the rung all
    /// evaluate one population. The seeds are DEFAULTS an operator may clear: a cleared list is stored as an
    /// explicit empty array and excludes nothing on that arm — present-and-empty is a decision, and nothing
    /// re-seeds it. <c>LongRunningQueryExclusionKnobRungTests</c> pins the literal defaults here equal to
    /// <c>LongRunningQueryExclusions.DefaultProgramNamePrefixes</c> / <c>DefaultLogins</c>.</para>
    ///
    /// <para><b>Readers and writers.</b> <c>StoreConfigProvider</c> seeds them as the 69th/70th bindings and reads
    /// them at ordinals 67–68 (the reachability rule: a column selected but not read resets the knob on every
    /// worker start); the Viewer's <c>ViewerDataService.AlertSettings</c> binds them as $68/$69;
    /// <c>DarlingAlertReader.AlertSettingsSelectSql</c> reads them at 67–68 for <c>get_alert_settings</c>;
    /// <c>update_alert_settings</c> writes <c>long_running_query.excluded_program_name_prefixes</c> /
    /// <c>.excluded_logins</c>. No reload beacon of its own: V17's statement-level
    /// <c>trg_bump_alert_settings</c> already bumps <c>config_service.config_version</c> on any write here. No
    /// ACL or provisioning change: <c>config_alert_settings</c> carries table-level grants (no column carve),
    /// the same fact every knob rung on this row has relied on. Rides after V134 (#3653 item 13's time-honesty
    /// rung), one un-landed rung at a time.</para>
    /// </summary>
    private const string V135Sql = @"
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS long_running_query_excluded_program_name_prefixes text[] NOT NULL
        DEFAULT ARRAY['SQLAgent - TSQL JobStep']::text[];
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS long_running_query_excluded_logins text[] NOT NULL
        DEFAULT ARRAY['NT AUTHORITY\SYSTEM', 'NT AUTHORITY\NETWORK SERVICE']::text[];";

    /// <summary>
    /// V136 — two PostgreSQL-target series the analysis engine had no source for (#3691, design §4b and
    /// §6): <c>collect.pg_database_size_stats</c>, the hourly per-database size series — a NEW table — and the
    /// host's memory from AWS Performance Insights, SIX COLUMNS on <c>collect.pg_cpu_utilization</c>. One rung
    /// for both, deliberately: they unblock the same wave of consumer lanes (object growth, the memory
    /// composition checks), and the ladder rule is one un-landed rung at a time.
    ///
    /// <para><b>Why memory is columns on the CPU row and not <c>pg_host_memory</c>, the table the plan
    /// named.</b> Two reasons, and the second is the one that decided it. The memory counters ARE the CPU
    /// row: one <c>GetResourceMetrics</c> response, one minute stamp, one watermark, one COPY — a sibling table
    /// would re-store the same identity under a second name with its own resume point. And every collector
    /// table is a hypertable: <c>TimescaleSupport.CompressionPhaseBandMinutes</c> spreads them at three per
    /// minute, a 73rd hypertable widens the band 24→25 minutes, the heaviest hourly refresh's window shrinks
    /// 18→17 and its watch line drops 900→850 s — BELOW the 896 s recorded ceiling. The grid's own doc calls
    /// that a scheduling decision (#3035, #3044, #3107), not a renumbering, and it is not this rung's to make.
    /// A 72nd hypertable fits exactly (72 / 3 = 24); six nullable columns cost nothing. V130 made the same
    /// choice for the log-event numbers, for the same reasons.</para>
    ///
    /// <para><b>The new table is exactly what the generator emits</b>, column for column — the V104 rule
    /// <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c> enforces. A fresh store
    /// builds it from the generated schema at V1 and this CREATE no-ops (<c>IF NOT EXISTS</c>); a store that
    /// climbed here gets it from this text. One generated index, on <c>(server_id, collection_time)</c>.
    /// Hypertable conversion, one-day chunks, compression segmented by <c>server_id</c> and the retention policy
    /// follow from the catalog entry on the service's next start, as for every collector table. All value
    /// columns nullable, matching the generated schema this must be identical to.</para>
    ///
    /// <para><b>The six columns are added in TWO places and both are required</b> — the V101 rule. V106's
    /// CREATE carries them for the fresh population (walked from the generator at V1, and never re-run on a
    /// store that has the table); this rung's ALTER carries them for the store that built the nine-column
    /// table first. <c>ADD COLUMN IF NOT EXISTS</c> is a no-op on the fresh store that already has them.
    /// <b>Nullable, no DEFAULT, no backfill</b>, matching every column-adding rung on a collector table (V80,
    /// V81, V127, V128, V130, V133): a pre-V136 row never had a memory sample taken, NULL is the honest value,
    /// and a nullable no-default ADD COLUMN is catalog-only on a compressed hypertable (V127/V128/V133 verified
    /// the shape live on 2.28.1). No view — the PostgreSQL collector tables have no <c>v_</c> passthrough.</para>
    ///
    /// <para><b><c>pg_database_size_stats</c>: BYTES, and NULL where the role may not size a database.</b>
    /// <c>size_bytes</c> is <c>pg_database_size(oid)</c>; <c>total_bytes</c> is the instance total denormalized
    /// onto every row of a collection (the <c>pg_session_states.total_sessions</c> shape — one read, no GROUP
    /// BY) and is NULL when ANY database's size is, because a sum over the databases this role can see is not
    /// the instance total. A database the role lacks <c>CONNECT</c> on and is not <c>pg_read_all_stats</c> for
    /// — the vendor-owned database on a managed target — is a row with <c>size_bytes NULL</c>, never 0 and
    /// never a failed collection. Templates are collected and flagged (<c>is_template</c>), not skipped. The
    /// only contract a later join against the store's own <c>collect.store_metrics</c> (V53, <c>total_bytes</c>
    /// = <c>pg_total_relation_size</c>) or the SQL Server side's size columns depends on is <c>*_bytes
    /// bigint</c> naming and naive-UTC timestamps, and both are kept. Hourly, a year of retention: ~6,000 rows
    /// a day fleet-wide.</para>
    ///
    /// <para><b>The memory columns: the CPU row's source, the CPU row's minute.</b> The <c>os.memory.*</c>
    /// counters are named in the <c>GetResourceMetrics</c> request <c>RdsCpuIngestor</c> already makes — more
    /// metric names on one call rather than a second API, a second IAM grant or a second collector cycle. The
    /// five <c>memory_*_bytes</c> columns are PI's kilobyte counters × 1024; <c>configured_memory_bytes</c> is
    /// the minute's Serverless v2 capacity × 2 GiB per ACU (vendor-defined) and NULL on a provisioned instance
    /// class. <b>A stock PostgreSQL target has no <c>pg_cpu_utilization</c> row at all</b> — there is no OS
    /// source from inside the engine — and that absence is the answer a consumer must read as
    /// <c>unavailable</c>, never as "no memory". A store that monitors only SQL Server or only self-hosted
    /// PostgreSQL carries one more empty table, six more empty columns, and nothing else changes.</para>
    ///
    /// <para><b>Nothing reads the new table or the six columns yet</b>, and the rung says so rather than
    /// implying a consumer: the composition checks, the object-growth family and the disk-free read are
    /// #3691's later slices, and the point of landing the series first is that they have a day of rows when
    /// those land. No backfill — a series starts when it starts. Lite stores neither
    /// (<c>DuckDbSchemaGenerator.StoredCollectors</c> is the SQL Server set), so the rung has no DuckDB twin.</para>
    /// </summary>
    private const string V136Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_database_size_stats (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    database_name text,
    size_bytes bigint,
    total_bytes bigint,
    is_template boolean,
    allows_connections boolean
);

CREATE INDEX IF NOT EXISTS idx_pg_database_size_stats_time
    ON collect.pg_database_size_stats(server_id, collection_time);

ALTER TABLE collect.pg_cpu_utilization
    ADD COLUMN IF NOT EXISTS memory_total_bytes bigint,
    ADD COLUMN IF NOT EXISTS memory_free_bytes bigint,
    ADD COLUMN IF NOT EXISTS memory_cached_bytes bigint,
    ADD COLUMN IF NOT EXISTS memory_buffers_bytes bigint,
    ADD COLUMN IF NOT EXISTS memory_active_bytes bigint,
    ADD COLUMN IF NOT EXISTS configured_memory_bytes bigint;";

    /// <summary>
    /// V137 — four column sets on three EXISTING tables, one rung, because they unblock four follow-up
    /// lanes at once and the ladder rule is one un-landed rung at a time: (a) the two Query Store capture
    /// modes on <c>collect.query_store_health</c> (#3796); (b) the store-backed twin of the #3712
    /// uncorroborated-finding route knob on <c>config.config_alert_settings</c>; (c) the plan dimension's
    /// TOAST bytes on <c>collect.store_metrics</c> (#3783); (d) the store's own checkpointer phases, three
    /// more columns on <c>collect.store_metrics</c> (#3783, with #3802 and #3745 as the levers they inform).
    /// Eight nullable columns, no DEFAULT, no backfill, no new table, no new hypertable
    /// (<c>TimescaleSupport.HypertableCount</c> stays 72 — the V136 doc says what a 73rd would cost the
    /// compression grid), one passthrough refreshed. Pinned by <c>QsCaptureModeRouteKnobToastRungTests</c>; the
    /// Lite twin is schema v64 (<c>DuckDbInitializer</c>), for (a) only — Lite stores <c>query_store_health</c>
    /// through the shared collector, stores no <c>store_metrics</c>, and keeps its alert settings in
    /// settings.json rather than a table.
    ///
    /// <para><b>(a) <c>query_capture_mode</c> and <c>wait_stats_capture_mode</c> — the option that names a
    /// plan-churn factory (#3796).</b> <c>QueryStoreHealthCollector</c> read every column of
    /// <c>sys.database_query_store_options</c> that says whether Query Store WORKS and none that says what it
    /// CAPTURES. Measured on one production store class: ~755 k new distinct plans a day into the plan
    /// dimension from 42 servers, and one database taking 92–96 % of the <c>query_store</c> collector's
    /// per-database fan-out on the largest store — the signature of <c>QUERY_CAPTURE_MODE = ALL</c> on an
    /// ad-hoc workload, which the health row could not confirm or rule out because it never asked, while
    /// the fleet's other three knobs (200 plans / 21 days / 8 GB) are uniform. Both columns are the DMV's
    /// <c>*_desc</c> spelling verbatim (<c>ALL</c> / <c>AUTO</c> / <c>CUSTOM</c> / <c>NONE</c>; <c>ON</c> / <c>OFF</c>),
    /// <c>text</c> because the collector declares them <c>Varchar</c> and the generator renders <c>Varchar</c>
    /// as <c>text</c> (the type here is the generator's rendering of the declaration, which is what
    /// <c>PgSchemaGeneratorTests</c> holds every collector rung to). <c>query_capture_mode_desc</c> shipped
    /// with the view in 2016 and is always selected; <c>wait_stats_capture_mode_desc</c> is 2017+ (v14), and
    /// on a 2016 engine a body that names it fails to compile for the whole database, so the collector
    /// gates that ONE column (<c>QueryStoreHealthCollector.HasWaitStatsCaptureMode</c>, the
    /// <c>DatabaseConfigCollector</c> idiom) and writes NULL there — a NULL a reader must publish as
    /// "engine predates the option", never as <c>OFF</c>. NULL on every pre-rung row means "never asked". The
    /// reader is the Query Store clutter view (#3797): churn × <c>ALL</c> is "switch to AUTO", churn ×
    /// <c>AUTO</c> points at the workload; it lands in its own lane, as do the <c>get_query_store_health</c>
    /// fields and the Viewer grid columns.</para>
    ///
    /// <para><b>(b) <c>analysis_uncorroborated_route</c> — the store-backed half of a knob #3732 shipped
    /// file-level (#3712).</b> <c>FindingRouting.Classify</c> sends a notify-worthy but UNCORROBORATED finding
    /// (one fact in its chain, no matched co-fire) to the daily digest instead of a page, and the ONE knob
    /// that governs it, <c>analysis.uncorroboratedRoute</c> in darling.json, was fenced to the file because two
    /// rungs were in flight when it landed (<c>AnalysisConfig.UncorroboratedRoute</c> says so, and
    /// <c>StoreConfigProvider.LoadViewAsync</c> CARRIES the file value across the wholesale
    /// <c>config.Analysis</c> swap on every reload for exactly that reason). This column is the store's
    /// half, spelled the way the knob is already spelled everywhere the product persists it: Lite's
    /// settings.json key IS <c>analysis_uncorroborated_route</c> (<c>App.AnalysisUncorroboratedRoute</c>), and both
    /// SKUs' MCP surfaces publish <c>analysis.uncorroborated_route</c>. It is a TRI-STATE, which is why it is
    /// nullable with no DEFAULT where every numeric knob on this
    /// row ships one: NULL means "not set in the store — the file-level knob, or its shipped default
    /// <c>digest</c>, governs", and that is what every existing store reads the morning after the upgrade,
    /// so nothing changes until an operator writes a value. The precedence the code half (its own lane)
    /// implements is <b>store non-NULL wins over file</b>: <c>DarlingAlertSettings.UncorroboratedFindingRoute</c>
    /// reads this column first and falls to <c>analysis.uncorroboratedRoute</c> only on NULL, the reload
    /// carry becomes a column read, <c>get_alert_settings</c> publishes the effective route and WHICH source
    /// it came from, and <c>update_alert_settings</c> gains the field it refuses today. Until that lands the
    /// column is written by nothing and read by nothing, and the rung says so.</para>
    ///
    /// <para><b>Why (b) carries a CHECK when the knob rungs on this row deliberately do not.</b> V119, V120,
    /// V122, V124 and V126 refused a CHECK because a NUMBER has a clamp: the write tool bounds it and
    /// <c>DarlingAlertSettings</c> clamps it on read, so any stored value maps to a valid one. A route has no
    /// clamp. <c>FindingRouting.TryParseRoute</c> reads anything that is neither <c>digest</c> nor <c>page</c> as
    /// "no opinion" and falls to the default — so a misspelled <c>pgae</c> written straight into the row would
    /// not error, it would silently turn an operator's decision to PAGE back into the digest, which is the
    /// one failure this knob exists to make impossible. The CHECK makes that a write error at the store,
    /// the V62 <c>plan_xml_compression</c> precedent (an enumeration on <c>config_service</c>, CHECK mirroring the
    /// provider's normalization). <c>IS NULL OR IN ('digest', 'page')</c>, lower-case wire spellings
    /// (<c>FindingRouting.DigestText</c> / <c>PageText</c>; the parser is case-insensitive but the store holds
    /// the canonical form, which is what the write tool normalizes to). Added inside V62's <c>DO</c> guard
    /// because <c>ADD CONSTRAINT</c> has no <c>IF NOT EXISTS</c>, and the rung must be re-runnable on a store
    /// that already carries it.</para>
    ///
    /// <para><b>(c) <c>toast_bytes</c> and <c>toast_live_bytes</c> — the plan dimension's file-slack question
    /// (#3783).</b> On one production store class <c>query_plan_dim</c>'s TOAST file was 154 GB holding ~61 GB
    /// of live chunks — 40 % utilisation, ~93 GB of slack left by the V54 text→gz conversion plus ~755 k
    /// rows a day cycling through row-capped deletes — and the store self-metrics could not say so, because
    /// the dimension row stores <c>pg_total_relation_size</c> (heap + indexes + TOAST) and nothing about how
    /// full the TOAST file is. Ordinary <c>VACUUM</c> returns those pages to the table, never to the OS; only
    /// <c>--recompress-plan-dim --vacuum-full</c> (#2076) compacts the file, and an operator should learn that
    /// from a stored series rather than from a size pass. Both columns are filled only on
    /// <c>object_kind = 'dimension'</c> rows (<c>StoreSelfMetrics.DimensionInsertSql</c>) and NULL on every
    /// other kind, the table's own per-kind convention. <c>toast_bytes</c> is
    /// <c>pg_relation_size(reltoastrelid)</c> — the TOAST relation's main fork, the file whose slack #3783
    /// measured — written from this rung on; NULL where the table has no TOAST relation.</para>
    ///
    /// <para><b><c>toast_live_bytes</c> exists and is written NULL, deliberately, and the reason is a
    /// measurement.</b> The bundled store has neither <c>pgstattuple</c> nor <c>pg_freespacemap</c>
    /// installed (both are contrib modules the TimescaleDB image SHIPS and does not CREATE), and every
    /// extension-free read was tried on a rig — TimescaleDB 2.28.1 / PostgreSQL 18, 215 MB of 9.6 KB TOASTed
    /// values in the plan dimension's shape, half deleted, ordinary <c>VACUUM</c>, <c>pgstattuple</c> as the
    /// oracle (47.9 % live). The tuple-share proxy, <c>n_live_tup / (n_live_tup + n_dead_tup) × file</c>, read
    /// <b>100 %</b>: after <c>VACUUM</c> the dead tuples are gone and the file keeps the pages, which is the
    /// #3783 case exactly, so that number is a lie and is not stored. The chunk-count estimate,
    /// <c>n_live_tup(toast) × 1996</c>, read 48.9 % — honest to within one partial chunk per value on 9.6 KB
    /// values, but the error is value-size-dependent (a dimension whose values barely cross the TOAST
    /// threshold overstates by up to half), and <c>n_live_tup</c> is a statistics figure that is exact only
    /// after a vacuum. <c>pg_freespacemap</c>'s <c>file − sum(avail)</c> read 48.5 % — an honest byte
    /// measurement — but it needs <c>CREATE EXTENSION pg_freespacemap</c>, which is a product-wide dependency
    /// decision this rung does not make. So the column is created now (no second rung when the maintainer
    /// decides) and written NULL until then; a reader that finds it NULL beside a non-NULL <c>toast_bytes</c>
    /// must say "utilisation not measured", never compute one from the total.</para>
    ///
    /// <para><b>(d) <c>checkpoint_write_ms</c>, <c>checkpoint_sync_ms</c> and <c>checkpoints_requested</c> — the
    /// store's own checkpointer, as a series (#3783; the levers are #3802's and #3745's).</b> Three unattributed
    /// read kills on a production store in one day all sat inside checkpoint SYNC phases of 25.2 s and 14.0 s —
    /// the fsync storm a checkpoint's end is — and nothing in the store recorded that the checkpointer had
    /// been there. These columns are for a <c>checkpointer</c> row (<c>object_kind = 'checkpointer'</c>,
    /// <c>object_name = 'pg_stat_checkpointer'</c>) the store self-metrics inventory writes once per run,
    /// carrying DELTAS since its previous run: milliseconds the checkpointer spent in the write phase, in the
    /// sync phase, and how many REQUESTED (not timed) checkpoints ran: WAL volume reaching <c>max_wal_size</c>, a
    /// base backup and a <c>CHECKPOINT</c> statement all request one, so the count alone does not say the store
    /// outran <c>max_wal_size</c> rather than merely reaching <c>checkpoint_timeout</c>. The source is
    /// <c>pg_stat_checkpointer</c> on PostgreSQL 17+ (<c>write_time</c>, <c>sync_time</c>, <c>num_requested</c>) and
    /// <c>pg_stat_bgwriter</c> before it (<c>checkpoint_write_time</c>, <c>checkpoint_sync_time</c>,
    /// <c>checkpoints_req</c>); the bundled store is 18, and the WRITER guards the version, not this rung.
    /// The existing columns cannot carry these honestly — <c>total_bytes</c> is a size, <c>row_count</c> a count
    /// of rows, and the <c>job_history</c> kind already bent one column's name once (the sweep doc says which
    /// and why a dedicated column was the clean follow-up) — so they are NEW columns by ruling, on the table
    /// whose rows already join on one <c>metric_time</c> per run so the checkpointer's phases can be read
    /// beside the same run's sizes and job durations. <b>Filled by the inventory from #3783's code half; NULL
    /// until then.</b> The delta needs the previous run's cumulative (a read of the last checkpointer row, or
    /// an in-process baseline the first run has to seed), which is a writer with state and not a line beside
    /// the TOAST read, so this rung creates the columns and writes nothing into them; on every other kind's
    /// row they are NULL by the table's per-kind convention, exactly like (c)'s pair.</para>
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b> on all eight, matching every column-adding rung. Only
    /// <c>query_store_health</c> is a hypertable (compressed on the fleet, one-day chunks); a nullable,
    /// default-less <c>ADD COLUMN</c> is catalog-only there — the V127/V128/V132/V133/V134 shape, verified live
    /// on 2.28.1 each time and again for this rung against a store stopped at 136. <c>store_metrics</c> and
    /// <c>config_alert_settings</c> are plain tables (the first must stay one: it measures the hypertables).
    /// The passthrough view is refreshed for the V14 reason — Postgres freezes a view's <c>SELECT *</c> column
    /// list at CREATE, and <c>v_query_store_health</c> is what <c>get_query_store_health</c> and the Viewer's
    /// grid read; neither <c>store_metrics</c> nor <c>config_alert_settings</c> has a <c>v_</c> passthrough
    /// (<c>PgSchemaGenerator.AllPassthroughViews</c> agrees), so their ALTERs stand alone. A fresh store gets
    /// the two collector columns from the generated CREATE TABLE at V1 (the collector definition carries
    /// them, appended LAST so the positional COPY writer and an upgraded store's ALTER agree on where they
    /// sit) and the ALTER no-ops; the V101 rule applies, so V76's CREATE text carries them too. No GRANT on
    /// (b): <c>config_alert_settings</c> carries table-level grants with no column carve. No reload beacon of
    /// its own: V17's statement-level <c>trg_bump_alert_settings</c> already bumps <c>config_version</c> on any
    /// write to that row.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It adds no reader, tool field, Viewer column or
    /// alert for any of the eight. It does not read the 2019+ <c>capture_policy_*</c> knobs behind
    /// <c>CUSTOM</c>. It does not change the route knob's precedence today (NULL everywhere = the file
    /// governs, exactly as before). It does not install an extension. It does not compute a utilisation. It
    /// does not write the checkpointer row.</para>
    /// </summary>
    private const string V137Sql = @"
ALTER TABLE collect.query_store_health
    ADD COLUMN IF NOT EXISTS query_capture_mode text,
    ADD COLUMN IF NOT EXISTS wait_stats_capture_mode text;

/* Postgres FREEZES a view's SELECT * column list at CREATE, so the passthrough would keep serving the
   pre-V137 column list forever — the V14 lesson, restated by V80, V81, V127, V128, V132 and V134. Appending
   is the one alteration CREATE OR REPLACE VIEW permits, which is exactly what an ADD COLUMN produces. */
CREATE OR REPLACE VIEW collect.v_query_store_health AS SELECT * FROM collect.query_store_health;

/* config_alert_settings has no v_ passthrough (V17), so this ALTER stands alone. Nullable on purpose and
   nothing fills it: NULL is a value here (not set in the store; the file-level knob governs), which is what
   every existing store reads the morning after the upgrade. */
ALTER TABLE config.config_alert_settings
    ADD COLUMN IF NOT EXISTS analysis_uncorroborated_route text;

/* The CHECK the numeric knobs on this row do without: a route has no clamp, and an unrecognised
   spelling would read as the shipped route rather than as an error. V62's guard, because ADD CONSTRAINT
   has no IF NOT EXISTS. */
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'config_alert_settings_analysis_uncorroborated_route_check'
    ) THEN
        ALTER TABLE config.config_alert_settings
            ADD CONSTRAINT config_alert_settings_analysis_uncorroborated_route_check
            CHECK (analysis_uncorroborated_route IS NULL OR analysis_uncorroborated_route IN ('digest', 'page'));
    END IF;
END $$;

/* store_metrics is a plain table with no v_ passthrough (V53), so this ALTER stands alone too. The first two
   are filled only on object_kind = 'dimension' rows; toast_live_bytes is written NULL until the maintainer
   decides how live bytes are measured (the rung doc carries the rig numbers). The last three belong to the
   object_kind = 'checkpointer' row #3783's code half will write — deltas since the previous run of the
   checkpointer's write-phase ms, sync-phase ms and requested-checkpoint count — and are NULL until then. */
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS toast_bytes bigint,
    ADD COLUMN IF NOT EXISTS toast_live_bytes bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_write_ms bigint,
    ADD COLUMN IF NOT EXISTS checkpoint_sync_ms bigint,
    ADD COLUMN IF NOT EXISTS checkpoints_requested bigint;";

    /// <summary>
    /// V138 — two nullable columns on <c>collect.pg_server_config</c> so a setting row can say WHOSE setting
    /// it is (#3691, the "per-database settings" collector line). Server-wide rows leave both NULL, which is
    /// every row this table has ever held; an override row carries a database name, a role name, or both.
    /// No new table, no new hypertable (<c>TimescaleSupport.HypertableCount</c> stays 72 — the V136 doc says
    /// what a 73rd would cost the compression grid), no DEFAULT, no backfill, no passthrough refresh.
    /// <b>No Lite twin</b>: Lite has no PostgreSQL collectors at all, so there is nothing on the DuckDB side
    /// to port — <c>pg_server_config</c> is Darling-only, as is every <c>pg_*</c> collector table.
    ///
    /// <para><b>The lie this ends.</b> <c>pg_settings</c> is the RESOLVED view for the collector's own
    /// backend, and the collector connects to one database as one role — so what it stores is what that
    /// session sees, which the <c>CONFIG_PG_*</c> facts and <c>get_pg_server_config</c> then present as "the
    /// server's configuration". A cluster where one database carries
    /// <c>ALTER DATABASE … SET work_mem = '256MB'</c>, or one role carries
    /// <c>ALTER ROLE … SET statement_timeout = 0</c>, is invisible today: the override lives in
    /// <c>pg_db_role_setting</c>, which nothing read, and the fact grades a value that database's or that
    /// role's sessions never use. That is the worst shape an advisory number can take — confidently right
    /// about a value nobody runs. The collector now reads that catalog beside <c>pg_settings</c> and these
    /// two columns are what distinguish the two populations in the stored rows.</para>
    ///
    /// <para><b>Why columns on this table rather than a table of its own.</b> An override IS a setting: same
    /// name, same text value, same server, same hourly snapshot clock, and the question an operator asks is
    /// "what is <c>work_mem</c> here" — which needs the server-wide value and the overrides in one answer,
    /// ordered and stamped together. A second table would need its own index, its own hypertable decision
    /// (the 73rd, which V136 argued against on compression-grid grounds), its own retention rung, and every
    /// read that wants both would become a join across two snapshots whose <c>collection_time</c>s are only
    /// approximately equal. Two nullable columns cost the existing table nothing and make the
    /// discriminator a predicate.</para>
    ///
    /// <para><b>NULL is a value here, and it means server-wide.</b> Both columns are nullable with no
    /// DEFAULT for the reason every column-adding rung is: a nullable, default-less <c>ADD COLUMN</c> on a
    /// compressed hypertable is catalog-only on TimescaleDB 2.28.1 (the V127/V128/V132/V133/V134/V137
    /// shape), so an upgraded store rewrites nothing. It also means every pre-rung row reads as server-wide
    /// the moment the migration lands, which is exactly what those rows ARE: the collector could not have
    /// stored an override before this rung existed. <c>pg_db_role_setting</c> itself spells the same thing
    /// the same way — <c>setdatabase = 0</c> means all databases and <c>setrole = 0</c> means all roles, and
    /// the collector's <c>LEFT JOIN</c> turns both zeros into the NULLs this table stores.</para>
    ///
    /// <para><b>The reads are the correctness edge, not this rung.</b> Every shipped read of this table is a
    /// "latest snapshot, one row per setting name" shape, several of them loading a dictionary keyed by
    /// <c>name</c> — so an override row arriving with a duplicate name would either shadow the server-wide
    /// value or throw on the duplicate key. That is why this lane's commit puts
    /// <c>AND database_name IS NULL AND role_name IS NULL</c> on the OUTER row select of every one of them
    /// (the inner <c>MAX(collection_time)</c> subqueries are per SERVER, not per name, so an override row
    /// cannot move the anchor and they need no predicate), and why exactly ONE read — the override section
    /// <c>get_pg_server_config</c> gained — selects the rows where those columns are NOT NULL. A census pin
    /// asserts that set: every <c>pg_server_config</c> read in the repo either filters the overrides out or
    /// is the one that asks for them.</para>
    ///
    /// <para><b>The V101 rule applies</b>, so V102's CREATE text carries both columns too: a fresh store
    /// builds the table from the generated schema at V1 (the generator walks
    /// <c>PgServerConfigCollector.PayloadColumns</c>, where the two are appended LAST so the positional COPY
    /// writer and an upgraded store's ALTER agree on where they sit) and this ALTER no-ops, while a store
    /// that climbed through V102 before V138 existed gets them from the ALTER.
    /// <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c> requires V102's text
    /// to be the generator's output column for column, so neither place is redundant. No passthrough
    /// refresh: <c>pg_server_config</c> has no <c>v_</c> view (<c>PgSchemaGenerator.AllPassthroughViews</c>
    /// agrees — it is not in <c>PostV8ViewCollectors</c> and V102 creates none), so the V14 frozen-column-list
    /// problem cannot arise. No index either: the existing <c>(server_id, collection_time)</c> index is what
    /// every read anchors on, and the override predicate is a cheap filter inside a snapshot that holds a few
    /// hundred rows.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It does not make the <c>CONFIG_PG_*</c> facts
    /// per-database — they stay server-wide in this lane, and a per-database fact family (which needs a
    /// database dimension on the fact, a per-database grading policy, and an answer to "which database's
    /// <c>work_mem</c> is the server's work_mem") is a later brief. It adds no alert, no Viewer column and no
    /// threshold. It does not store the ROLE's effective settings resolved against the database's — PostgreSQL
    /// resolves those per session at connect time, and the catalog stores only what was SET.</para>
    /// </summary>
    private const string V138Sql = @"
/* Nullable, no DEFAULT, no backfill: catalog-only on a compressed hypertable (the V127/V128/V132/V133/V134/
   V137 shape on 2.28.1), and NULL already means what every pre-rung row is — a server-wide pg_settings row.
   pg_server_config has no v_ passthrough (V102 creates none, PgSchemaGenerator.AllPassthroughViews agrees),
   so the V14 frozen-column-list refresh does not apply here. Appended LAST, matching the collector's payload
   order, so the positional COPY writer and an upgraded store's column order agree. */
ALTER TABLE collect.pg_server_config
    ADD COLUMN IF NOT EXISTS database_name text,
    ADD COLUMN IF NOT EXISTS role_name text;";

    /// <summary>
    /// V139 — one nullable column, <c>postmaster_start_time</c>, on two existing tables (#3955): the
    /// <c>checkpointer</c> row of <c>collect.store_metrics</c> and every row of <c>collect.pg_write_stats</c>. Each
    /// sample of a server's cumulative checkpointer counters now says which postmaster produced it, so a reader
    /// can tell an interval that spans a restart from one that does not. No new table, no new hypertable
    /// (<c>TimescaleSupport.HypertableCount</c> stays 72), no DEFAULT, no backfill, no passthrough refresh, and
    /// <b>no Lite twin</b>: Lite stores neither table.
    ///
    /// <para><b>The lie this ends.</b> PostgreSQL counts a shutdown checkpoint in
    /// <c>pg_stat_checkpointer.num_requested</c> (<c>pg_stat_bgwriter.checkpoints_req</c> through 16) and keeps the
    /// count across the restart: 0|0|0, then 0|1|0, then 0|2|0 as <c>num_timed | num_requested | num_done</c> on a
    /// fresh 18.6 cluster over two fast stops. The Store Checkpointer Pressure self-alert (#3783) reads any
    /// requested checkpoint in an interval as proof the store outran <c>max_wal_size</c>, so every service restart
    /// raised it: on 2026-09-22 a production store reported "2 WAL-forced checkpoint(s)" for an interval whose
    /// server log held one timed checkpoint and two <c>shutdown immediate</c> ones, and no WAL-triggered checkpoint
    /// at all. Every host restarts when it upgrades. The monitored-target write reads difference the same counter,
    /// so a monitored server's restart read as WAL pressure in <c>PG_CHECKPOINT_PRESSURE</c> and in
    /// <c>get_pg_write_stats</c> the same way. The shutdown checkpoint's own write and sync phases land in the
    /// counters beside it, where nothing can separate them from the live checkpoints'. <see cref="PostmasterRestart"/>
    /// is the rule those readers now share, and across a restart they state none of those figures and judge
    /// neither arm; this rung is the evidence the rule reads.</para>
    ///
    /// <para><b>Why a stored column rather than a read of <c>pg_postmaster_start_time()</c> at read time.</b> A
    /// live read says when the CURRENT postmaster started, which dates the newest sample only if nothing restarted
    /// since it was taken, so <c>get_store_metrics</c> read an hour after a restart would call the last clean
    /// interval unknown. It also cannot serve the monitored-target reads, which difference rows the store holds
    /// for thirty days about servers it does not query at read time. A value on every sample makes the rule
    /// exact: two samples that report one postmaster start were taken by one postmaster, compared on the
    /// server's own clock, so skew between the collecting host and the server cannot move the answer. The per-row
    /// shape is the one <c>pg_write_stats</c> already gives its three <c>stats_reset</c> stamps, for the same
    /// reason: the reads difference per sample and need the epoch on the row they difference.</para>
    ///
    /// <para><b>Naive UTC</b>, written as <c>pg_postmaster_start_time() AT TIME ZONE 'UTC'</c> by
    /// <c>StoreSelfMetrics.CheckpointerInsertSql</c> (both majors' statements) and by the shared
    /// <c>PgWriteStatsCollector</c>, never a bare <c>::timestamp</c> cast, which renders in the session's zone.
    /// <b>Nullable, no DEFAULT, no backfill</b>, matching every column-adding rung: a row written before this rung
    /// cannot know which postmaster wrote it, and NULL is what the rule reads as "no evidence". On the newer
    /// sample of a pair that means the interval is judged as before; on the older it means the rule falls back to
    /// whether that sample predates the newer sample's postmaster start, which is what makes the first interval
    /// after the upgrade restart unknown rather than a warning. <c>pg_write_stats</c> is a compressed hypertable
    /// on the fleet (one-day chunks); a nullable, default-less <c>ADD COLUMN</c> is catalog-only there, the
    /// V127/V128/V133/V137/V138 shape, verified again for this rung on a compressed chunk under TimescaleDB 2.30.1.
    /// <c>store_metrics</c> is a plain table and must stay one.</para>
    ///
    /// <para><b>The V101 rule applies</b> to <c>pg_write_stats</c>: the collector appends the column LAST, so the
    /// positional COPY writer and an upgraded store's ALTER agree on where it sits, and V88's CREATE text carries it
    /// for the fresh population because <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c>
    /// holds that text to the generator's current output. <c>store_metrics</c> is not a collector table
    /// (V53 creates it by hand and no generator walks it), so its column lives in this ALTER alone. <b>No
    /// passthrough refresh</b>: neither table has a <c>v_</c> view (<c>PgSchemaGenerator.AllPassthroughViews</c>
    /// agrees), so the V14 frozen-column-list problem cannot arise.</para>
    ///
    /// <para><b>What this rung deliberately does NOT do.</b> It does not backfill. It does not touch the
    /// counter-reset handling (the Reset status, the <c>stats_reset</c> guards), which a crash still reaches first.
    /// It adds no alert, knob or Viewer column. The price of the rule it feeds is one unjudged interval per
    /// restart, which the readers say rather than hide.</para>
    /// </summary>
    private const string V139Sql = @"
/* store_metrics is a plain table with no v_ passthrough (V53). Filled on the object_kind = 'checkpointer' row only,
   NULL on every other kind by the table's per-kind convention. Nullable, no DEFAULT, no backfill: a pre-rung row
   cannot know which postmaster wrote it, and NULL is what the restart rule reads as no evidence. */
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS postmaster_start_time timestamp;

/* pg_write_stats is a compressed hypertable on the fleet; a nullable, default-less ADD COLUMN is catalog-only there
   (the V127/V128/V133/V137/V138 shape). No v_ passthrough, so no view refresh. Appended LAST, matching the
   collector's payload order, so the positional COPY writer and an upgraded store's column order agree. */
ALTER TABLE collect.pg_write_stats
    ADD COLUMN IF NOT EXISTS postmaster_start_time timestamp;";

    /// <summary>
    /// V140 — <c>checkpoints_timed</c> on the store's own checkpointer row (#4037): the cumulative COUNT of
    /// TIMED checkpoints, beside the write-ms/sync-ms/requested counters V137 already carries. Store
    /// Checkpointer Pressure (#3783) and <c>get_store_metrics</c>' checkpointer block judged the interval's
    /// SUMMED sync-phase milliseconds against <see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>, a
    /// PER-CHECKPOINT bar (the MCP read deadline), and the hourly interval covers about twelve timed
    /// checkpoints on the default five-minute <c>checkpoint_timeout</c> — so a healthy store whose checkpoints
    /// each sync in five to eight seconds summed past the bar every interval and never recovered, while the
    /// actual per-checkpoint figure never approached it. Judging the AVERAGE (<c>SyncMs / (timed +
    /// requested)</c>) needs the timed count on the row the sync-ms delta already comes from; nothing else in
    /// the store carries it per-store (<c>pg_write_stats.num_timed</c>, V88, is the MONITORED-target series,
    /// a different row for a different server).</para>
    ///
    /// <para><b>One column, same convention as V137/V139</b>: filled on the <c>object_kind = 'checkpointer'</c>
    /// row only, NULL on every other kind. Nullable, no DEFAULT, no backfill — a row written before this rung
    /// has no timed count, and the reader treats that NULL as unmeasured for the average arm rather than as
    /// zero (a zero would read as "every checkpoint" and misjudge the row). <c>store_metrics</c> is a plain
    /// table with no <c>v_</c> passthrough (V53), so this ALTER stands alone; appended last, matching V137's
    /// and V139's columns, so a fresh V1 store and an upgraded one agree on column order for the same reason
    /// those rungs did.</para>
    /// </summary>
    private const string V140Sql = @"
/* store_metrics is a plain table with no v_ passthrough (V53). Filled on the object_kind = 'checkpointer' row
   only, NULL on every other kind by the table's per-kind convention. Nullable, no DEFAULT, no backfill: a
   pre-rung row cannot know how many checkpoints were timed, and NULL is what the average-per-checkpoint rule
   reads as unmeasured rather than zero. */
ALTER TABLE collect.store_metrics
    ADD COLUMN IF NOT EXISTS checkpoints_timed bigint;";

    /// <summary>
    /// V141 (#3691, part a1) — <c>collect.analysis_collection_caveats</c>: the CURRENT set of fact families an
    /// analysis pass could not read, one row per (server, family), so a scheduled pass's caveats survive past
    /// the in-process ledger <see cref="PerformanceMonitor.Analysis.CollectionCaveatLedger"/> already keeps —
    /// that ledger is per-process and unread by anything outside the service that ran the pass, so the Viewer,
    /// which is a separate process reading only the store, has never been able to say "this family has not
    /// been read for N passes" at all. This table is what closes that gap: not a replacement for the ledger
    /// (which stays in-memory by design, shared with Lite, and untouched here), but the store side <c>CollectionCaveatStore</c>
    /// writes beside it, on the same pass, best-effort.
    ///
    /// <para><b>Current, not historical.</b> The table holds only the families a server is failing to read
    /// RIGHT NOW: a family a pass reads successfully has its row deleted, not stamped closed, because the
    /// pass already knows definitively which families it read and which it did not — there is no ambiguous
    /// middle state to distinguish with a status column. Bounded by construction: at most (servers × distinct
    /// families) rows, which on any fleet measured so far is a few hundred at most.</para>
    ///
    /// <para><b>Primary key <c>(server_id, family)</c>, no surrogate.</b> The upsert key IS the identity a
    /// reader wants: one open caveat per server per family. <c>reason</c> keeps only the most recent failure's
    /// classification (the payload spelling from <c>CollectionFailure.Label</c> — <c>timeout</c>, <c>cancelled</c>,
    /// <c>missing_schema</c>, <c>error</c>); a family failing for two different reasons across passes is not a
    /// fact this table tries to hold, because the ledger already holds the per-pass history for whoever wants
    /// it and the Viewer's use is "is this family currently missing", not "how did it fail last Tuesday".</para>
    ///
    /// <para><b>Not a collector table.</b> Absent from <see cref="CollectorCatalog"/>, so <see cref="TimescaleSupport"/>'s
    /// catalog-driven hypertable conversion and <c>DarlingRetention</c>'s catalog purge never reach it — the
    /// <c>collect.oversized_plan_backlog</c> (V121) precedent exactly: it needs neither chunks nor compression
    /// at this size, and its own prune (<c>CollectionCaveatStore.PruneAsync</c>, called beside the backlog's own
    /// sweep-driven prune) is a simpler policy than retention's catalog-driven one because there is no sighting
    /// cadence to reason about — a row not re-written this pass either was read (and is gone) or the server
    /// stopped being analysed (and prune reaps it after 7 days).</para>
    ///
    /// <para>Granted through the <c>collect</c> schema's blanket <c>GRANT … ON ALL TABLES IN SCHEMA collect</c>
    /// (the V54/V101 convention — no per-table GRANT here), which the service and viewer roles both already
    /// hold, so the Viewer can SELECT it without a provisioning change.</para>
    /// </summary>
    private const string V141Sql = @"
CREATE TABLE IF NOT EXISTS collect.analysis_collection_caveats
(
    server_id integer NOT NULL,
    family text NOT NULL,
    reason text NOT NULL,
    first_seen_utc timestamp NOT NULL,
    last_seen_utc timestamp NOT NULL,
    CONSTRAINT pk_analysis_collection_caveats PRIMARY KEY (server_id, family)
);";

    /// <summary>
    /// V142 — #4196: the supporting index for the anomaly detector's "latest two object-stats snapshots"
    /// read (<c>PgAnomalyDetector.ObjectGrowthSql</c> / <c>PgAnomalyDetector.ObjectContentionSql</c>'s
    /// <c>snaps</c> CTE, <c>SELECT DISTINCT collection_time FROM v_index_object_stats WHERE server_id = $1
    /// ORDER BY collection_time DESC LIMIT 2</c>). Neither existing index leads with <c>collection_time</c>
    /// second: V1's <c>idx_index_object_stats_object</c> is <c>(server_id, database_name, object_id, index_id,
    /// collection_time)</c> and V22's <c>idx_index_object_stats_latest</c> is <c>(server_id, database_id,
    /// object_id, index_id, collection_time DESC)</c> — both put two unconstrained columns between the equality
    /// filter and the sort key the read needs, so neither can drive it. TimescaleDB's automatic per-chunk
    /// <c>collection_time</c> index CAN drive the <c>ORDER BY ... LIMIT 2</c> directly (SkipScan), but it has no
    /// <c>server_id</c> column at all, so every server's rows in the chunk are read and rejected by a Filter
    /// until two matching ones turn up. <c>index_object_stats</c> is collected once daily per server, so the
    /// "prior" snapshot is usually a chunk boundary away, but the "latest" one is always in the newest chunk
    /// alongside the rest of that day's fleet — paid once per server per analysis pass, twice (once per
    /// statement). No other read needs a new index: the <c>cur</c>/<c>prv</c> CTEs also filter on
    /// <c>collection_time</c> once the two times are known, but they are already bounded to the one server by
    /// <c>idx_index_object_stats_latest</c>'s leading <c>server_id</c> column (a Filter there costs one
    /// server's history, not the fleet's), which is why only the <c>snaps</c> step gets a new index.
    ///
    /// <para><b>No SQL text changes.</b> The <c>snaps</c> CTE already has the ideal shape for this index
    /// (<c>DISTINCT</c> + <c>ORDER BY</c> + <c>LIMIT</c> on exactly the sort key, filtered on exactly the
    /// leading equality column) — it only lacked the index. Measured on a rig seeded with three chunks (one at
    /// real fleet-daily scale: 43 servers, ~12,000 index/table rows each, matching the issue's own ~11,900
    /// rows/server/day measurement, plus two smaller older chunks, one compressed): before this index, the
    /// <c>snaps</c> read for one server was <c>Custom Scan (SkipScan)</c> over the chunk's bare
    /// <c>collection_time</c> index with <c>Filter: (server_id = $1)</c>, <c>Rows Removed by Filter: 36000</c>,
    /// <c>Buffers: shared hit=92 read=972</c>, 18.5 ms. After, the same read is <c>Index Only Scan</c> using
    /// this index with <c>Index Cond: (server_id = $1) AND (collection_time &lt; ...)</c>, no Filter line,
    /// <c>Buffers: shared hit=33 read=4</c>, 0.4 ms — roughly 29x fewer buffers and 44x faster on this seed; the
    /// ratio widens on a bigger fleet since the old plan's cost scales with the WHOLE fleet's newest-chunk rows
    /// and the new plan's does not.</para>
    ///
    /// <para><b>Locking — plain <c>CREATE INDEX</c>, in the ladder, deliberately.</b> <c>CREATE INDEX
    /// CONCURRENTLY</c> is refused outright on a TimescaleDB hypertable ("hypertables do not support concurrent
    /// index creation" — see <see cref="PgTableTuning"/>'s <c>ForcePlanFailuresIndexName</c> finding for the
    /// same limitation on a much bigger table), and <c>MigrateAsync</c> wraps every rung in a transaction, which
    /// also rules out the per-chunk <c>WITH (timescaledb.transaction_per_chunk)</c> form. So this rung takes the
    /// ordinary ShareLock on the hypertable root for its build's duration, same as V22's index on this same
    /// table. Unlike <c>PgTableTuning</c>'s query_store_stats index — 8-16 million rows/DAY, big enough that the
    /// build was moved out of the ladder into the runtime Tuning stage — <c>index_object_stats</c> is the daily
    /// object-stats collector: roughly half a million rows/day fleet-wide per the issue's own measurement, and
    /// <see cref="TimescaleSupport.CompressAfterDays"/> = 1 means only about one day's chunk is ever uncompressed
    /// at migration time; every older chunk's decompressed relation is an empty shell (a compressed chunk's
    /// <c>CREATE INDEX</c> cost is one 8 KB page, not a function of the rows inside it — the same property
    /// <c>PgTableTuning</c> measured). Measured on the same three-chunk rig: 228 ms end to end for the whole
    /// build (one 516,000-row uncompressed chunk plus two smaller chunks, one compressed) — small enough,
    /// against this table's daily write rate, to stay in the ladder rather than needing the Tuning-stage
    /// treatment.</para>
    ///
    /// <para>Additive and idempotent like V22 (<c>CREATE INDEX IF NOT EXISTS</c>): a fresh store gets it at
    /// V142 like every other rung, an upgraded store gets it exactly once, a re-run is a no-op. Explicitly
    /// <c>collect.</c>-qualified like V21/V22/V23. No table shape change, so nothing to refresh for the binary
    /// COPY.</para>
    /// </summary>
    private const string V142Sql = @"
CREATE INDEX IF NOT EXISTS idx_index_object_stats_server_time ON collect.index_object_stats (server_id, collection_time DESC);";

    /// <summary>
    /// V143 — the latest Query Store snapshot per interval, kept as it is written (#3953), so PLAN_REGRESSION and its
    /// drill-down read one row per interval instead of deduplicating the whole raw <c>query_store_stats</c> slice on
    /// every pass. Three new tables, all engine-plain here (the <c>PgMigrations</c> rule); nothing on an existing
    /// table changes, and there is no backfill.
    ///
    /// <para><b><c>collect.query_store_interval_latest</c></b>: one row per Regular interval identity, the dedup's
    /// <c>GROUP BY</c> plus <c>server_id</c>, holding the snapshot the raw read's
    /// <c>ORDER BY collection_time DESC, execution_count DESC</c> would keep. Its columns are exactly what the two reads
    /// consume, and their types and nullability mirror raw's, so the table can never refuse a row raw accepted. One
    /// unique index, <c>NULLS NOT DISTINCT</c> because <c>replica_role</c> is NULL off an availability group and must
    /// still collapse (PostgreSQL 15+; the product minimum is 16). The column order is the writer's: a batch is one
    /// database's rows for one or two interval ids, so each batch's entries form one contiguous run.
    /// <c>fillfactor = 50</c> was measured (99% HOT against 56-59% at 70). The hypertable conversion, compression and
    /// retention are runtime work in <c>collection_log</c>'s shape, not this rung's.</para>
    ///
    /// <para><b><c>collect.query_store_interval_latest_coverage</c></b>: per server, <c>filled_since</c> (every raw
    /// snapshot at or after it is represented, except the pending batches below) and <c>applied_through</c> (the
    /// newest batch accounted for). The reader uses the table only where that claim covers everything the raw read
    /// would read.</para>
    ///
    /// <para><b><c>collect.query_store_interval_latest_pending</c></b>: one row per raw batch whose apply failed. The
    /// apply runs behind a savepoint in the raw COPY's transaction, so a fault rolls back only the apply, records the
    /// batch here, and raw still commits: raw ingestion never depends on this table. The next apply for the server
    /// replays the row, and a server with any pending row reads raw. Empty in steady state.</para>
    /// </summary>
    private const string V143Sql = @"
/* One row per Regular Query Store interval identity: the raw dedup's GROUP BY plus server_id. Types and nullability
   mirror query_store_stats exactly, so no row raw accepts can be refused here. fillfactor 50 keeps the open
   interval's refreshes HOT (measured, #3953). */
CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest
(
    server_id integer NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    replica_role text,
    runtime_stats_interval_id bigint,
    first_execution_time timestamp NOT NULL,
    collection_time timestamp NOT NULL,
    query_plan_hash text,
    query_hash text,
    execution_count bigint,
    avg_cpu_time_us bigint,
    avg_duration_us bigint,
    last_execution_time timestamp,
    is_forced_plan boolean,
    force_failure_count bigint,
    query_text text
)
WITH (fillfactor = 50);

/* NULLS NOT DISTINCT: replica_role is NULL off an availability group, and a NULL-role re-collection must update its
   row, not insert a second one. Column order is the writer's locality, not the reader's. */
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_interval_latest
ON collect.query_store_interval_latest
(
    server_id,
    database_name,
    runtime_stats_interval_id,
    plan_id,
    query_id,
    replica_role,
    first_execution_time
)
NULLS NOT DISTINCT;

/* Per server: every raw snapshot at or after filled_since is represented (except the pending batches), and
   applied_through is the newest batch accounted for. */
CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest_coverage
(
    server_id integer NOT NULL,
    filled_since timestamp NOT NULL,
    applied_through timestamp NOT NULL,
    CONSTRAINT pk_query_store_interval_latest_coverage PRIMARY KEY (server_id)
);

/* One row per raw batch whose apply failed; it commits with that batch's raw rows, and the next apply for the
   server replays it. Empty in steady state. */
CREATE TABLE IF NOT EXISTS collect.query_store_interval_latest_pending
(
    server_id integer NOT NULL,
    collection_time timestamp NOT NULL,
    database_name text NOT NULL,
    recorded_at timestamp NOT NULL,
    failure text,
    CONSTRAINT pk_query_store_interval_latest_pending PRIMARY KEY (server_id, collection_time, database_name)
);";

    /// <summary>
    /// V144 (#4211) — the rung history <see cref="PerformanceMonitor.Darling.Storage.RawChunkIntervalReconciler"/>
    /// both writes to and reads <c>IntervalLastChangedUtc</c> back from, plus a small per-run record of WAL
    /// volume (ruling decision 8, review finding L4: stage 1 records WAL volume so a later stage can correlate
    /// it against interval moves). Two engine-plain tables, the <c>collect.store_log_events</c> /
    /// <c>collect.store_log_captures</c> shape (V111): plain tables, no serial id, a time index, naive-UTC
    /// <c>timestamp</c> columns per the store's cross-engine contract. Neither table is purged — the run
    /// table gains one row per daily pass (about 365 a year) and the history one row per move, and the
    /// history has to keep at least
    /// <see cref="PerformanceMonitor.Darling.Storage.RawChunkIntervalPlanner.MinimumDaysBetweenMoves"/> days
    /// anyway, because the next pass reads <c>MAX(changed_at)</c> from it. Like store_log's tables, these are
    /// self-telemetry about the store's OWN tuning, not collected monitoring data, so they are deliberately
    /// outside <c>CollectorCatalog.All</c>.
    ///
    /// <para><b><c>raw_chunk_interval_rung_history</c></b>: one row per rung CHANGE (not per table per run) —
    /// table, when, the interval it moved from and to, the reason text
    /// <see cref="PerformanceMonitor.Darling.Storage.RawChunkIntervalPlanner.Decision"/> already built, and the
    /// three inputs the decision was made from (ingest rate, budget B, the store-wide open-chunk total). The
    /// reconciler's own <c>MAX(changed_at)</c> per table is what feeds
    /// <see cref="PerformanceMonitor.Darling.Storage.RawChunkIntervalPlanner.TableInput.IntervalLastChangedUtc"/>
    /// back into the next run, which is what makes <see cref="PerformanceMonitor.Darling.Storage.RawChunkIntervalPlanner.MinimumDaysBetweenMoves"/>
    /// hold across restarts and not just within one process's lifetime.</para>
    ///
    /// <para><b><c>raw_chunk_interval_reconcile_runs</c></b>: one row per reconcile RUN, whether or not it
    /// changed anything — <c>wal_bytes</c> is <c>numeric</c> because that is <c>pg_stat_wal.wal_bytes</c>'s own
    /// type (a lifetime counter PostgreSQL does not bound to bigint).</para>
    /// </summary>
    private const string V144Sql = @"
CREATE TABLE IF NOT EXISTS collect.raw_chunk_interval_rung_history
(
    changed_at timestamp NOT NULL,
    table_name text NOT NULL,
    from_interval_hours integer NOT NULL,
    to_interval_hours integer NOT NULL,
    reason text NOT NULL,
    ingest_bytes_per_hour double precision NOT NULL,
    budget_bytes double precision NOT NULL,
    store_total_bytes double precision NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_raw_chunk_interval_rung_history_table_time
    ON collect.raw_chunk_interval_rung_history(table_name, changed_at DESC);

CREATE TABLE IF NOT EXISTS collect.raw_chunk_interval_reconcile_runs
(
    run_at timestamp NOT NULL,
    wal_bytes numeric
);

CREATE INDEX IF NOT EXISTS idx_raw_chunk_interval_reconcile_runs_time
    ON collect.raw_chunk_interval_reconcile_runs(run_at);";

    /// <summary>
    /// V145 — the wide latest-snapshot-per-interval table beside V143's (#3953, review D4R + F1 measurement,
    /// ruled issuecomment-5836972848): every outcome (Regular, Aborted, Exception), every raw column the three new
    /// reads need except <c>collection_id</c>, <c>server_name</c> and <c>query_plan_text</c> (57 columns). V143's
    /// own table, coverage and pending tables are untouched — this is three MORE tables, the same shape as V143's
    /// three, engine-plain, with no backfill. Kept 9 days at V145 (F1: the 7-day preset plus a day of margin for an
    /// interval that can span a day) by the service's retention sweep, on <c>first_execution_time</c>, not this
    /// migration's concern. Since #5809 the horizon is 10 days, so get_query_store_regressions can read the table:
    /// its 7-day baseline plus a 24-hour window, with the gate's 26-hour purge-edge margin, needs 9 days 2 hours
    /// held (<c>DarlingRetention.QueryStoreIntervalWideRetentionDays</c>).
    ///
    /// <para><b><c>collect.query_store_interval_wide</c></b>: identity is V143's identity plus
    /// <c>execution_type_desc</c>, because two outcomes can share every other key. <c>first_execution_time</c>
    /// stays <c>NOT NULL</c> (M2, ruled) even though this table carries every outcome. <c>fillfactor = 50</c>,
    /// same as V143's and measured the same way. NO secondary index: F1 measured a window index on this shape
    /// taking HOT updates to 0%, and every read that would use one is slower on it than the split's plain scan.</para>
    ///
    /// <para><b><c>collect.query_store_interval_wide_coverage</c></b> and
    /// <b><c>collect.query_store_interval_wide_pending</c></b>: column-for-column copies of V143's coverage and
    /// pending tables, so a later gate can parametrize V143's own decision on the table rather than duplicate it
    /// (ruling, item 5).</para>
    ///
    /// </summary>
    private const string V145Sql = @"
/* One row per Query Store interval identity, every outcome. Types and nullability mirror query_store_stats except
   first_execution_time, which stays NOT NULL here (M2, ruled): the writer's IS NOT NULL filter and coverage-reset
   safeguard mean the table never silently drops a row raw accepted. fillfactor 50 keeps the open interval's
   refreshes HOT, as V143's. */
CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide
(
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    database_name text,
    query_id bigint,
    plan_id bigint,
    execution_type_desc text,
    first_execution_time timestamp NOT NULL,
    last_execution_time timestamp,
    module_name text,
    query_text text,
    query_hash text,
    execution_count bigint,
    avg_duration_us bigint,
    min_duration_us bigint,
    max_duration_us bigint,
    avg_cpu_time_us bigint,
    min_cpu_time_us bigint,
    max_cpu_time_us bigint,
    avg_logical_io_reads bigint,
    min_logical_io_reads bigint,
    max_logical_io_reads bigint,
    avg_logical_io_writes bigint,
    min_logical_io_writes bigint,
    max_logical_io_writes bigint,
    avg_physical_io_reads bigint,
    min_physical_io_reads bigint,
    max_physical_io_reads bigint,
    avg_clr_time_us bigint,
    min_clr_time_us bigint,
    max_clr_time_us bigint,
    min_dop bigint,
    max_dop bigint,
    avg_query_max_used_memory bigint,
    min_query_max_used_memory bigint,
    max_query_max_used_memory bigint,
    avg_rowcount bigint,
    min_rowcount bigint,
    max_rowcount bigint,
    avg_num_physical_io_reads bigint,
    min_num_physical_io_reads bigint,
    max_num_physical_io_reads bigint,
    avg_log_bytes_used bigint,
    min_log_bytes_used bigint,
    max_log_bytes_used bigint,
    avg_tempdb_space_used bigint,
    min_tempdb_space_used bigint,
    max_tempdb_space_used bigint,
    plan_type text,
    plan_forcing_type text,
    is_forced_plan boolean,
    force_failure_count bigint,
    last_force_failure_reason text,
    compatibility_level integer,
    query_plan_hash text,
    replica_role text,
    runtime_stats_interval_id bigint,
    interval_start_time_utc timestamp
)
WITH (fillfactor = 50);

/* NULLS NOT DISTINCT, same reason as V143's: replica_role is NULL off an availability group. execution_type_desc
   joins the identity because this table, unlike V143's, holds every outcome. No secondary index (F1, measured). */
CREATE UNIQUE INDEX IF NOT EXISTS ux_query_store_interval_wide
ON collect.query_store_interval_wide
(
    server_id,
    database_name,
    runtime_stats_interval_id,
    plan_id,
    query_id,
    replica_role,
    first_execution_time,
    execution_type_desc
)
NULLS NOT DISTINCT;

/* Column-for-column copy of V143's coverage table: this table's own claim, independent of V143's. */
CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide_coverage
(
    server_id integer NOT NULL,
    filled_since timestamp NOT NULL,
    applied_through timestamp NOT NULL,
    CONSTRAINT pk_query_store_interval_wide_coverage PRIMARY KEY (server_id)
);

/* Column-for-column copy of V143's pending table: one row per raw batch whose apply to THIS table failed. */
CREATE TABLE IF NOT EXISTS collect.query_store_interval_wide_pending
(
    server_id integer NOT NULL,
    collection_time timestamp NOT NULL,
    database_name text NOT NULL,
    recorded_at timestamp NOT NULL,
    failure text,
    CONSTRAINT pk_query_store_interval_wide_pending PRIMARY KEY (server_id, collection_time, database_name)
);";

    /// <summary>
    /// V146 — <c>collect.managed_conf_verdicts</c> (#4215, #4251's managed-store part): the per-key
    /// verdict a managed store's OWNER connection computes once at every service-owned start, replacing the
    /// previous start's rows. <c>DarlingStoreHostProfile.ComputeAndStoreManagedConfVerdictsAsync</c> is the
    /// only writer.
    ///
    /// <para><b>Why a table instead of <c>collect.collector_state</c>'s existing key/value shape</b> (the
    /// <c>PgSelfAlertDeliveryStampStore</c>/#3580 precedent). That table holds ONE opaque text value per
    /// (server, collector, key) — right for a stamp or a token, wrong here: a verdict row carries nine
    /// distinct fields a reader filters and displays independently (current value, derived value, source,
    /// file, line, verdict, detail), and cramming them into one text column would mean every reader,
    /// including a remote MCP caller, parses app-defined JSON instead of running SQL against typed columns.</para>
    ///
    /// <para><b>Why the owner must compute and store it, not a live per-read check</b> (#4215). The
    /// <c>mcp</c> and <c>viewer</c> roles get a NULL <c>pg_settings.sourcefile</c> and cannot read
    /// <c>pg_file_settings</c> at all (superuser/<c>pg_read_all_settings</c> only) — see
    /// <c>DarlingStoreMetricsReader.JobExecutionLoggingSql</c>'s remarks for the same wall on a different GUC.
    /// The owner (<c>darling</c>, the managed store's bootstrap superuser) computes the verdict once, right
    /// after start, and every other reader — <c>--check-settings</c> included, which is why it prints
    /// <c>computed_at</c> rather than a fresh timestamp — reads these rows back instead of re-deriving
    /// something it cannot see.</para>
    ///
    /// <para><b>Keyed on <c>setting_name</c> alone, replaced wholesale each start.</b> One managed store has
    /// exactly one owner connection computing exactly one verdict set; there is no <c>server_id</c> to key on
    /// (this is the store's own settings, not a monitored target's), and the write is DELETE-then-INSERT in
    /// one round trip rather than an UPSERT so a key a future version stops owning does not linger as a stale
    /// row forever: a future version that stops owning a key must not leave a stale row behind.</para>
    ///
    /// <para><b><c>collect</c>-qualified, plain table, not a hypertable</b> — like <c>collector_state</c> and
    /// <c>analysis_state</c>, not like the catalog-driven collector tables: <c>TimescaleSupport.HypertableTables</c>
    /// is <c>CollectorCatalog.All</c>, and this is service-written state about the store's own conf file, not a
    /// collector's payload. No <c>v_</c> passthrough view: the <c>collect</c> schema's existing blanket
    /// <c>GRANT SELECT ON ALL TABLES</c> to <c>admin</c>, <c>viewer</c> and <c>mcp</c> (<c>provision-roles.sql</c>)
    /// already covers a bare table, the same grant every other <c>collect</c> table added since V44 has relied
    /// on without restating it.</para>
    ///
    /// <para><c>computed_at</c>/<c>postmaster_start_time</c> are naive UTC, written
    /// <c>AT TIME ZONE 'UTC'</c> like every other postmaster-start column since V139 — never a bare cast, which
    /// renders in the writer's session zone.</para>
    /// </summary>
    private const string V146Sql = @"
CREATE TABLE IF NOT EXISTS collect.managed_conf_verdicts
(
    setting_name text NOT NULL,
    current_value text NOT NULL,
    derived_value text NOT NULL,
    source_description text NOT NULL,
    source_file text,
    source_line integer,
    verdict text NOT NULL,
    detail text,
    computed_at timestamp NOT NULL,
    postmaster_start_time timestamp NOT NULL,
    CONSTRAINT pk_managed_conf_verdicts PRIMARY KEY (setting_name)
);";

    /// <summary>
    /// V147 — raises the shipped default for <c>config.config_service.compose_statement_timeout_seconds</c>
    /// from 15 s to 60 s (#4442 scope 1), and moves any row still sitting at the old shipped default.
    /// An operator-set value other than 15 (including one that happens to equal the OLD default exactly at
    /// a moment nobody touched it, indistinguishable from "never touched") is left alone; a store at 45
    /// stays at 45. This mirrors the same shipped-value-move-only shape every other default-raise rung
    /// uses: the column default changes for future rows, and the one-time UPDATE only ever touches rows
    /// still at the value nobody chose.
    /// </summary>
    private const string V147Sql = @"
ALTER TABLE config.config_service ALTER COLUMN compose_statement_timeout_seconds SET DEFAULT 60;
UPDATE config.config_service SET compose_statement_timeout_seconds = 60 WHERE compose_statement_timeout_seconds = 15;";

    /// <summary>
    /// V148 — <c>collect.read_latency</c>, the hourly histogram of <c>/api/read/*</c> and composed-panel
    /// read durations (#4442 scope 2), the <c>collector_cost</c> (V105) shape applied to reads instead of
    /// collector runs. NOT a collector: it is INTERNAL self-telemetry, written by the worker's hourly
    /// self-metrics sweep on the same tick as <c>collector_cost</c>'s flush, so it is deliberately absent
    /// from <c>CollectorCatalog.All</c> and from the generator-parity pins, and needs no per-table GRANT (the
    /// <c>collect</c> schema's blanket <c>GRANT … ON ALL TABLES IN SCHEMA collect</c> covers it the moment
    /// provisioning re-runs). Hand-written DDL, a plain table (not a hypertable — the accumulator aggregates
    /// to one row per (surface, route, outcome) per hour before it ever reaches this table, and its own
    /// bounded retention DELETE keeps it small, the same shape <c>collector_cost</c> and <c>store_metrics</c>
    /// use). <c>bucket_counts</c> is the fixed log-scale histogram <c>ReadLatencyAccumulator.BucketUpperBoundsMs</c>
    /// produces — the same array's length for every row, so two rows can be summed element-wise across a
    /// window without knowing which release wrote which. The primary key is <c>(metric_time, surface, route,
    /// outcome)</c> — the accumulator drains and flushes at most once per key per hour, and the flush's own
    /// <c>ON CONFLICT</c> upsert relies on exactly this key existing twice only when two flushes land in the
    /// same hour.
    /// </summary>
    private const string V148Sql = @"
CREATE TABLE IF NOT EXISTS collect.read_latency
(
    metric_time timestamp NOT NULL,
    surface text NOT NULL,
    route text NOT NULL,
    outcome text NOT NULL,
    run_count integer NOT NULL,
    total_ms bigint NOT NULL,
    max_ms bigint NOT NULL,
    bucket_counts bigint[] NOT NULL,
    PRIMARY KEY (metric_time, surface, route, outcome)
);

CREATE INDEX IF NOT EXISTS idx_read_latency_time
    ON collect.read_latency(metric_time);";

    /// <summary>
    /// V149 — the Query Store liveness touch becomes a HOT update on <c>collect.query_store_plan_map</c> and
    /// <c>collect.query_store_text</c> (#4250): drops each table's <c>last_seen</c> btree index and sets
    /// <c>fillfactor = 90</c>. <c>TouchAndProbeSql</c> on both tables (<see cref="QueryStorePlanMap"/>,
    /// <see cref="QueryStoreTextStore"/>) only ever writes <c>last_seen</c> and, conditionally, an
    /// unindexed hash column — so once the index is gone, no indexed column the touch changes remains, and a
    /// page with fillfactor headroom lets the new tuple stay on its old page: both conditions Postgres's HOT
    /// optimization needs. Confirmed by <c>git grep</c> against the service and storage code: the only other
    /// reader of either index was each table's own <c>PruneSql</c> time-sliced <c>DELETE ... WHERE last_seen &lt;
    /// $1</c> — no reader orders, filters, or range-scans <c>last_seen</c> for anything else. This change
    /// replaces that prune on both tables with <see cref="PerformanceMonitor.Darling.Service.DarlingRetention.UnorderedRowCappedDeleteSql"/>:
    /// one capped sequential pass per batch, with no ordering or subquery over <c>last_seen</c> to lose by
    /// dropping the index. See that builder's summary for the shape and the measured cost.
    ///
    /// <para>Plain <c>DROP INDEX</c> and <c>ALTER TABLE ... SET (fillfactor = ...)</c>, not <c>CONCURRENTLY</c>:
    /// <c>MigrateAsync</c> wraps every rung in a transaction, and <c>CREATE/DROP INDEX CONCURRENTLY</c> cannot
    /// run inside one. Both operations here are metadata-only — the index drop does not touch the heap, and
    /// the fillfactor change only affects pages written from here on — so the short <c>ACCESS EXCLUSIVE</c>
    /// each takes is a catalog update, not a rewrite; nothing else in the migrate session holds a competing
    /// lock on either table at that moment.</para>
    ///
    /// <para>Fillfactor 90 is prospective only: existing pages, packed at the old default of 100, do not gain
    /// HOT headroom until they are rewritten by organic churn (the tables are continuously purged) or a
    /// deliberate rewrite. No rewrite ships in this rung.</para>
    /// </summary>
    private const string V149Sql = @"
DROP INDEX IF EXISTS collect.idx_query_store_plan_map_last_seen;
DROP INDEX IF EXISTS collect.idx_query_store_text_last_seen;
ALTER TABLE collect.query_store_plan_map SET (fillfactor = 90);
ALTER TABLE collect.query_store_text SET (fillfactor = 90);";

    /// <summary>
    /// V150 — two indexes, added additively for both #4469 and #4477:
    /// <list type="bullet">
    /// <item><c>idx_collection_log_watermark</c> on <c>collect.collection_log (server_id, collector_name,
    /// collection_time DESC)</c> so <see cref="PerformanceMonitor.Darling.Service.DarlingWorker.ReadCollectorWatermarksSql"/>
    /// can look up each collector's newest run with one index-only descent per collector instead of a
    /// bitmap heap scan of the newest chunks. Measured on a rig shaped like the field (43 servers, ~40
    /// collectors, 15M rows, 9 of 11 chunks compressed): the per-collector lookup runs ~0.65 ms against
    /// the old statement's ~24 ms, cold and warm alike, because it turns the read from a scan of every
    /// row in the newest chunks into one index-only descent per collector name. Costs: ~0.94 s to build
    /// on 2.4M uncompressed rows (well inside <c>MigrationCommandTimeoutSeconds</c>), ~115 MB, and +38%
    /// wall time on a 100,000-row bulk COPY into the newest chunk (0.353 s -&gt; 0.487 s median of 3) —
    /// one more btree every future <c>collection_log</c> write maintains.</item>
    /// <item><c>idx_job_history_server_run</c> on <c>collect.job_history (server_id, run_datetime DESC,
    /// instance_id DESC)</c> for the Viewer's Job History tab
    /// (<see cref="PerformanceMonitor.Darling.Viewer.ViewerDataService.BuildJobHistorySql"/>), matching that
    /// read's own <c>ORDER BY run_datetime_utc DESC, instance_id DESC</c> tie-break so a per-server
    /// top-N lookup needs no additional sort on the indexed columns. Measured on a rig shaped like the
    /// field (43 servers, ~28,000 rows/server over 4 days, 3 of 5 chunks compressed): the base row
    /// selection this index serves reads ~1,506 buffers cold against the unindexed scan's ~6,883 (about
    /// 4.6x fewer), and the full Job History read (including the per-job stats aggregate this index does
    /// not cover) runs ~721 ms cold against ~1,362 ms (about 1.9x) — the win is smaller than the
    /// watermark index's because the read's other half, <c>job_stats</c>, still scans every matching row
    /// in the window to compute an average/max per job and this index does not help that half.</item>
    /// </list>
    /// Both are plain <c>CREATE INDEX IF NOT EXISTS</c> (mirroring V149's shape): <c>MigrateAsync</c> wraps
    /// every rung's whole SQL in one transaction, and <c>CREATE INDEX ... WITH
    /// (timescaledb.transaction_per_chunk)</c> cannot run inside one — measured, it raises
    /// <c>CREATE INDEX ... WITH (timescaledb.transaction_per_chunk) cannot run inside a transaction
    /// block</c>. Each build takes a <c>ShareLock</c> for its duration (confirmed via <c>pg_locks</c>),
    /// blocking concurrent inserts/updates/deletes to that table until the build finishes — acceptable at
    /// the measured field-store extrapolation of well under 2 seconds each, but a store whose uncompressed
    /// chunks have grown unusually large (a long compression-policy gap, or a raised
    /// <c>CompressAfterDays</c>) would make this rung's lock window grow linearly with the uncompressed
    /// row count. <c>IF NOT EXISTS</c> makes both idempotent on a FRESH store too: <c>PgSchemaGenerator</c>
    /// does not build either index on a fresh install today, so this rung is the real create on both
    /// paths, with no fresh-vs-upgraded shape divergence to special-case.
    /// </summary>
    private const string V150Sql = @"
CREATE INDEX IF NOT EXISTS idx_collection_log_watermark
    ON collect.collection_log (server_id, collector_name, collection_time DESC);
CREATE INDEX IF NOT EXISTS idx_job_history_server_run
    ON collect.job_history (server_id, run_datetime DESC, instance_id DESC);";

    /// <summary>
    /// V151 — <c>sys.availability_groups.group_id</c> on both AG collector tables (#4475): the GUID the engine
    /// stamps identically on every replica of one Availability Group, appended LAST as text (the collector
    /// column vocabulary has no uuid type; <c>AgDatabaseReplicaStatesCollector</c>'s <c>last_hardened_lsn</c> /
    /// <c>last_commit_lsn</c> already store a wide identifier the same way). <see cref="AgTopology.CountDistinctGroups"/>
    /// uses it to close the one gap the name-plus-replica-overlap rule (#4475) could not: two monitored
    /// SECONDARIES of one AG, with its primary unmonitored, share no replica name with each other (each reports
    /// only itself under <c>sys.dm_hadr_availability_replica_states</c>'s local-only rule) and so counted as two
    /// groups. The group_id is the same on both replicas' rows, so a member carrying one groups by it exactly;
    /// a member from a row collected before this rung carries none and falls back to the pre-existing name +
    /// overlap rule among the other id-less members; and a with-id member and a without-id member of the same
    /// name whose replica sets overlap still join (the same AG, seen before and after the upgrade landed on that
    /// reporter). Stated in <see cref="AgTopology.CountDistinctGroups"/>'s own doc, not restated as a second
    /// source of truth here.
    ///
    /// <para><b>Nullable, no DEFAULT, no backfill</b>, the V127/V128/V132/V133/V150 shape for every column-adding
    /// rung on a collector table: a row collected before this rung never asked the engine for its AG's
    /// group_id, and NULL is the honest value — a reader treats it as "fall back to the pre-#4475 name + overlap
    /// rule", exactly today's behavior. Both tables are compressed hypertables on the fleet; a nullable,
    /// default-less <c>ADD COLUMN</c> is catalog-only in PostgreSQL and TimescaleDB accepts it on a compressed
    /// hypertable with a compression policy attached, the shape V127/V128/V132/V133 used and verified live each
    /// time. No view to refresh: the AG collector tables have been view-less since V34 (no <c>v_</c> passthrough
    /// was ever created for either), so this rung is two ALTERs and nothing else.</para>
    ///
    /// <para>A fresh store gets the column from the generated CREATE TABLE
    /// (<see cref="AgReplicaStatesCollector"/> / <see cref="AgDatabaseReplicaStatesCollector"/> carry it,
    /// appended last in <c>PayloadColumns</c>) and the ALTER no-ops there — the V101 rule, pinned by
    /// <c>PgSchemaGeneratorTests</c> reconstructing the current shape from V34/V36/V37/this rung and comparing it
    /// to the generator's current output.</para>
    ///
    /// <para>Lite's DuckDB twin gets the same column the same way (schema version bump, additive <c>ALTER TABLE
    /// ... ADD COLUMN IF NOT EXISTS</c>), and the shared <see cref="AgTopology.CountDistinctGroups"/> is what
    /// both Lite's AG tab and Darling's Viewer/MCP/web reads call, so the fallback rule cannot drift apart
    /// between stores.</para>
    /// </summary>
    private const string V151Sql = @"
ALTER TABLE collect.ag_replica_states
    ADD COLUMN IF NOT EXISTS group_id text;

ALTER TABLE collect.ag_database_replica_states
    ADD COLUMN IF NOT EXISTS group_id text;";

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
    /// (dozens of rows), so <c>ServerTagRules</c> (Storage) enforces both the cap and cycle-freedom for every
    /// writer. No recursive CTE, no ltree extension, no closure table.
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
    /// sidebar and never the collector loop. The alert evaluator reads them, but through its own cache refresh,
    /// so a beacon is still unnecessary and a bump would only cost a needless fleet reconcile. <c>id</c> is
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
    /// (rotated by tag id, in the viewer). Stored as <c>#RRGGBB</c> text — read by the service's tag-scoped alert
    /// rules since #3350 and validated by <c>ServerTagRules</c> — so no CHECK constraint is imposed here; writers
    /// store only palette values or a user pick.</para>
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
    /// table (total bytes, row count), and one whole-store summary row (pg_database_size_stats + the
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
    delta_wait_time_us bigint,
    sample_interval_seconds integer
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
    delta_rows bigint,
    sample_interval_seconds integer
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
    ///
    /// <para><b>The column after <c>stats_reset</c> is V133's (#3691), and it is here for the V101 rule.</b>
    /// A store's tables come from one of two texts: a fresh store builds every table from the generated schema
    /// at V1 (this CREATE then no-ops, <c>IF NOT EXISTS</c>), while a store that climbed through V83 before
    /// V133 existed has the nine-payload-column table this text built at the time.
    /// <c>PgSchemaGeneratorTests</c> requires this rung to be the generator's output column for column, and
    /// the generator emits the collector's CURRENT columns — so this text carries <c>numbackends</c> for the
    /// fresh population, and V133's ALTER carries it for the existing one. Neither is redundant.</para>
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
    stats_reset timestamp,
    numbackends integer
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
    ///
    /// <para><b>The column after <c>wal_stats_reset</c> is V139's (#3955), and it is here for the V101 rule.</b>
    /// A fresh store builds the table from the generated schema at V1 (this CREATE then no-ops,
    /// <c>IF NOT EXISTS</c>); a store that climbed through V88 before V139 existed has the twenty-six-payload-column
    /// table this text built at the time. <c>PgSchemaGeneratorTests</c> requires this rung to be the generator's
    /// output column for column, and the generator emits the collector's CURRENT columns, so this text carries
    /// <c>postmaster_start_time</c> for the fresh population and V139's ALTER carries it for the existing one.
    /// Neither is redundant.</para>
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
    wal_stats_reset timestamp,
    postmaster_start_time timestamp
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
    /// thing it audits. Enrolled in retention at the LONGEST horizon in the store
    /// (<c>DarlingRetention.PlanForceLedgerRetentionDays</c>, a year), because an audit of writes to
    /// production servers is the one series that should outlive the metrics that motivated it. The bot's own
    /// cooldowns (at most one decision per query per cooldown window) bound the arrival RATE, which is not a
    /// size bound — a bounded rate over unbounded time is unbounded — so the horizon is what keeps it
    /// finite. A batched DELETE on <c>action_time</c>, not <c>drop_chunks</c>: the identity PRIMARY KEY that
    /// <c>related_action_id</c> points back to is exactly what
    /// <c>TimescaleSupport</c> excludes PK-bearing tables for.</para>
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
    /// V111 — the store's own server log as a self-monitoring source (#3021): a per-class census
    /// (<c>collect.store_log_events</c>), the capture denominator that qualifies it
    /// (<c>collect.store_log_captures</c>), and the per-file resume marker
    /// (<c>config.store_log_read_marker</c>).
    ///
    /// <para>NOT a collector, the V53 / V105 shape: this is INTERNAL self-telemetry written by the worker's
    /// hourly self-metrics tick, so all three tables are deliberately absent from
    /// <c>CollectorCatalog.All</c> — which is what keeps the catalog-driven hypertable conversion and the
    /// catalog retention purge off the tables that observe them — and hand-written DDL is therefore correct
    /// rather than a generator-parity miss. Plain tables, bounded by
    /// <see cref="StoreLogSweep.RetentionDays"/>'s own DELETEs.</para>
    ///
    /// <para><b>Why a census and not one row per line.</b> A production day of the store's log holds ~1,100
    /// <c>ERROR:  canceling statement due to user request</c> entries — the store's rendering of a
    /// client-side <c>CommandTimeout</c> cancel, which is the ordinary consequence of having timeouts and
    /// not a fault. <c>occurrences</c> per class per capture is the shape that answers the question those
    /// lines are actually asked ("did the rate move") without producing 1,100 rows a day nobody reads.
    /// <c>message_text</c> and <c>sample_line</c> are NULL for the classes that are counted only, and that
    /// NULL is the record that the class is a counted floor rather than a missing measurement — see
    /// <see cref="StoreLogClassifier"/> for the class-by-class argument. Since #3944 a retained row's
    /// <c>message_text</c> is its message's grouping key (<see cref="StoreLogClassifier.GroupingKeyOf"/>) and its
    /// <c>sample_line</c> the entry as PostgreSQL wrote it, its SQL normalized.</para>
    ///
    /// <para><b>Why <c>store_log_captures</c> is its own table.</b> Every other sampled read in the product
    /// borrows its denominator from <c>collection_log</c> — <c>get_pg_blocking</c> reports
    /// <c>captures_total</c> beside <c>captures_with_blocking</c> precisely because an absent capture and a
    /// capture that found nothing are the same absence of rows. This source writes no
    /// <c>collection_log</c> row (it is not in the catalog), so it has no denominator to borrow and must
    /// carry its own. <c>offset_reset</c> and <c>groups_dropped</c> ride here for the same reason: a marker
    /// discarded because the weekday ring truncated, and a distinct-message budget that folded rows, are
    /// both facts about the capture's COVERAGE, and a coverage fact that is not recorded is one the reader
    /// silently assumes away.</para>
    ///
    /// <para><b>Why the marker is in <c>config</c> and keyed by file.</b> It is state the operator's store
    /// owns rather than collected data, so it is not subject to the census' retention DELETE — a marker
    /// aged out would re-read a whole file. Keyed by file because rotation is by WEEKDAY NAME
    /// (<c>postgresql-%a.log</c>), so a rotation must start a fresh marker instead of resuming a new file at
    /// an old file's offset — the same key shape the RDS log route uses, for the same reason.
    /// <c>last_size</c> is beside <c>byte_offset</c> rather than derived from it because
    /// <c>log_truncate_on_rotation</c> means the file SHRINKS, and comparing the current size against the
    /// size at the last read is what detects that (see <see cref="StoreLogSlab.ResolveResume"/>).</para>
    ///
    /// <para><b>No Lite twin, and the reasoning is structural rather than a deferral.</b> The parity rule
    /// exists so state added to one store does not read as permanently empty on the other. Lite has no
    /// embedded PostgreSQL store — its store is DuckDB, which has no server log to read — so there is no
    /// file for a twin to point at. This is <c>get_store_metrics</c>' situation, not <c>get_deadlocks</c>'.</para>
    ///
    /// <para>Timestamps are naive UTC per the store contract, and they are the SWEEP's clock rather than the
    /// log's. That is deliberate: PostgreSQL renders <c>%m</c> in <c>log_timezone</c>, which
    /// <c>DarlingManagedPostgres</c>' v9 block leaves to the host (it pins the session <c>timezone</c> only,
    /// asserted by <c>DarlingManagedPostgresTests</c>), so the store's own log stamps are host-local. The
    /// server's own rendering survives inside <c>sample_line</c>, uninterpreted, with only its SQL normalized
    /// (#3915, #3944).</para>
    /// </summary>
    private const string V111Sql = @"
CREATE TABLE IF NOT EXISTS collect.store_log_events
(
    capture_time timestamp NOT NULL,
    event_class text NOT NULL,
    severity text NOT NULL,
    occurrences integer NOT NULL,
    message_text text,
    sample_line text
);

CREATE INDEX IF NOT EXISTS idx_store_log_events_time
    ON collect.store_log_events(capture_time);

CREATE INDEX IF NOT EXISTS idx_store_log_events_class
    ON collect.store_log_events(event_class, capture_time);

CREATE TABLE IF NOT EXISTS collect.store_log_captures
(
    capture_time timestamp NOT NULL,
    log_file text NOT NULL,
    bytes_read bigint NOT NULL,
    bytes_pending bigint NOT NULL,
    lines_read integer NOT NULL,
    entries_read integer NOT NULL,
    offset_reset boolean NOT NULL,
    groups_dropped integer NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_store_log_captures_time
    ON collect.store_log_captures(capture_time);

CREATE TABLE IF NOT EXISTS config.store_log_read_marker
(
    log_file text NOT NULL PRIMARY KEY,
    byte_offset bigint NOT NULL,
    last_size bigint NOT NULL,
    updated_at timestamp NOT NULL
);";

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
    /// V112 — <c>collect.collector_stall_probes</c>: what the whole monitored INSTANCE was waiting on during a
    /// collector stall, sampled out of band because nothing inside the sweep can answer it (#2880).
    ///
    /// <para><b>Why the rung is required rather than a column family on <c>collection_log</c>.</b> V108, V109
    /// and V110 all widened that row, and this deliberately does not, for one reason: a probe folded onto the
    /// stalled run's log row would have to be AWAITED by the run before the row could be written. That puts
    /// the watchdog on the collector's critical path — it would carry up to
    /// <see cref="!:StallWaitProbePolicy.HardBudget"/> into <c>duration_ms</c>, and it would delay the next
    /// collector on a server whose sweep is already blown, on exactly the ~1% of runs the instrument exists
    /// for. Its own table is what lets the probe be dispatched and forgotten. The join back to the stalled run
    /// is <c>(server_id, collector_name, probe_time)</c> against <c>v_collection_log</c>, which is explicit
    /// rather than free, and that is the price paid for the decoupling.</para>
    ///
    /// <para><b>NOT a collector.</b> It is not in <c>CollectorCatalog.All</c>, so it is absent from the
    /// generator-parity pins, the catalog-driven hypertable conversion and the catalog retention purge —
    /// hand-written DDL is correct here rather than a parity miss, the V53 / V105 / V111 shape. A plain table,
    /// not a hypertable: the arrival rate is one row per stalled run, which the measured population puts at
    /// roughly 90 rows a day across a 43-server fleet (2 servers, 2 collectors, ~1% of ~2,400 runs each), so
    /// chunking would cost more than it saves. Bounded by <c>StallWaitProbeRunner</c>'s own retention DELETE,
    /// the way <c>store_metrics</c> and <c>collector_cost</c> are — a bounded arrival rate over unbounded time
    /// is unbounded.</para>
    ///
    /// <para><b>The trigger columns are stored, not just the sample.</b> <c>trigger_elapsed_ms</c>,
    /// <c>trigger_rows_read</c>, <c>trigger_bytes_read</c>, <c>trigger_last_read_ms</c> and <c>budget_ms</c>
    /// are what the client believed at the instant it decided to spend a probe, so "why did this fire" is
    /// answerable from the row instead of from a log line on a box. <c>trigger_last_read_ms</c> in particular
    /// is recorded and was NOT part of the decision: the measured failure mode streams slowly with 0-3 ms of
    /// terminal silence on 8 of 8 abandoned runs, so a firing condition keyed on the reader having gone quiet
    /// would never fire on the actual defect. Storing it beside the decision is how a later reader can check
    /// that for themselves.</para>
    ///
    /// <para><b><c>outcome</c> is NOT NULL and a probe that could not connect is one of its values.</b> Whether
    /// a connection can be obtained mid-stall has never been tested — the one open in evidence
    /// (<c>open:104ms</c>) is the stalled collector's OWN open, taken before the stall — so
    /// <c>CONNECT_FAILED</c> / <c>CONNECT_TIMED_OUT</c> are expected results that answer an open question, and
    /// they get a stored row with <c>connect_ms</c> and <c>error_message</c> rather than a swallowed log line.
    /// Every value comes from <c>StallWaitProbePolicy.Outcomes</c>; no CHECK constraint, for
    /// <c>collection_log.status</c>'s reason — the reads bucket by explicit list, never by complement, so a
    /// value added later joins no bucket instead of silently joining the wrong one.</para>
    ///
    /// <para><b><c>scheduler_count</c> is the sample's own denominator.</b> Zero waiting tasks is a real and
    /// interesting answer — an instance answering trivial queries in milliseconds, producing rows 50x slowly,
    /// and waiting on nothing — but it is indistinguishable from a result set that described no instance at
    /// all. Every live SQL Server reports at least one <c>VISIBLE ONLINE</c> scheduler, so a positive count
    /// beside an empty wait list is what makes the all-clear readable, and a sample without one is stored as
    /// <c>NO_SAMPLE</c> rather than as a row of zeros. The same reasoning <c>get_pg_blocking</c> carries its
    /// capture counts for.</para>
    ///
    /// <para><b>No Lite twin, structurally.</b> The parity rule exists so state added to one store does not
    /// read as permanently empty on the other. The source is Darling-only: the arm is installed by
    /// <c>DarlingCollectorRunner</c>'s server-scoped path beside the V108/V109 instrumentation it reads, and
    /// Lite's runner has neither. A DuckDB twin would be an always-empty table. V109's situation, not
    /// <c>get_deadlocks</c>'.</para>
    ///
    /// <para>Naive UTC per the store contract, and it is the PROBE's clock — the instant the sample was taken
    /// on the client, not a timestamp read off the target, so nothing here depends on the monitored server's
    /// <c>timezone</c>.</para>
    /// </summary>
    private const string V112Sql = @"
CREATE TABLE IF NOT EXISTS collect.collector_stall_probes
(
    probe_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    collector_name text NOT NULL,
    outcome text NOT NULL,
    budget_ms integer NOT NULL,
    trigger_elapsed_ms integer NOT NULL,
    trigger_rows_read bigint,
    trigger_bytes_read bigint,
    trigger_last_read_ms integer,
    connect_ms integer,
    query_ms integer,
    waiting_task_count bigint,
    distinct_wait_types integer,
    top_wait_type text,
    top_wait_total_ms bigint,
    top_wait_max_ms bigint,
    wait_summary text,
    scheduler_count integer,
    runnable_tasks bigint,
    work_queue_length bigint,
    pending_disk_io bigint,
    max_runnable_tasks integer,
    error_message text
);

/* The read is newest-first per server, and the retention DELETE is by time alone, so one index serves both.
   Not UNIQUE: two collectors on one server can stall in the same millisecond, and a unique key would drop
   the second sample - the one that proves the degradation is not confined to a single collector. */
CREATE INDEX IF NOT EXISTS idx_collector_stall_probes_time
    ON collect.collector_stall_probes(server_id, probe_time);";

    /// <summary>
    /// V113 — the per-server REMEDIATION CREDENTIAL, and the journal's <c>actor</c> (#2138 phase 1).
    ///
    /// <para><b>The credential.</b> The monitoring credential stays read-only forever; that promise is
    /// load-bearing (the MCP instructions and both READMEs state it, and operators grant against it), so a
    /// write to a monitored server cannot travel on it. <c>remediation_username</c> /
    /// <c>remediation_encrypted_password</c> are a SECOND, per-server, opt-in credential in the same shape
    /// as <c>username</c> / <c>encrypted_password</c> beside them — same DPAPI-LocalMachine blob, same
    /// <c>env:</c>/<c>file:</c> reference support, produced by the same <c>--encrypt-password</c>. Both
    /// nullable with NO default and NO fallback: a server whose remediation columns are null has no
    /// phase-1 surface at all, which is why the absence is expressed as a null credential rather than as an
    /// <c>enabled</c> boolean — a boolean invites a disabled control, and a missing credential is supposed
    /// to be unrenderable rather than explained.</para>
    ///
    /// <para>Deliberately NOT reusing the <c>auth</c> column's vocabulary: a remediation credential is
    /// always SQL auth when present (an integrated remediation identity would be the service account,
    /// which is the monitoring identity, which is the thing this exists to avoid). Presence of the username
    /// IS the auth mode, so there is no third state to get wrong.</para>
    ///
    /// <para><b>The actor.</b> V107's journal was written when the bot was the only possible writer, and
    /// <c>PgPlanForceActionStore.GetPendingReviewsAsync</c> rests on that: its own-forces-only property is
    /// documented as structural because "the read starts from rows this bot journaled". Phase 1 makes an
    /// OPERATOR a writer to the same table, and that sentence stops being true the moment it does — the
    /// bot's self-review would pick up an operator's force and take it back, breaking the standing house
    /// rule that operator-placed forces are never touched. <c>actor</c> restores the invariant as data: the
    /// review read filters <c>actor = 'bot'</c>, so own-forces-only is a predicate on the table rather than
    /// a property of who happened to be able to write to it.</para>
    ///
    /// <para><b>The DEFAULT is added and then dropped, and that is the point.</b> Every existing row was
    /// written by the bot, so <c>DEFAULT 'bot'</c> backfills them correctly and is the only honest value
    /// for rows that predate the column. Leaving the default in place afterwards would make an INSERT that
    /// forgets <c>actor</c> silently claim to be the bot — the one direction that matters, because a bot row
    /// is the kind the review is allowed to unforce. Dropping it makes that INSERT fail loudly instead. The
    /// C# side reinforces it: <c>PlanForceActionRecord.Actor</c> is a required member, so a construction
    /// site that omits it does not compile.</para>
    /// </summary>
    private const string V113Sql = @"
ALTER TABLE config.config_monitored_servers
    ADD COLUMN IF NOT EXISTS remediation_username text;

ALTER TABLE config.config_monitored_servers
    ADD COLUMN IF NOT EXISTS remediation_encrypted_password text;

ALTER TABLE collect.plan_force_actions
    ADD COLUMN IF NOT EXISTS actor text NOT NULL DEFAULT 'bot';

ALTER TABLE collect.plan_force_actions
    ALTER COLUMN actor DROP DEFAULT;

/* The review read is (server_id, actor, action) with an ordering on action_time, and it is the read the
   own-forces-only invariant rests on, so it gets its own index rather than riding
   idx_plan_force_actions_time - which leads with server_id but knows nothing about the actor and would
   make every pending-review scan read the operator's rows to discard them. */
CREATE INDEX IF NOT EXISTS idx_plan_force_actions_actor
    ON collect.plan_force_actions(server_id, actor, action, action_time);";

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
    /// <para><b>Why the index earns its place.</b> The <c>pg_read_file</c> route re-reads an OVERLAPPING
    /// tail of the server log every cycle, deliberately, so a report cut in half at one edge is whole in
    /// the next and one deadlock lands many times. Every read therefore groups or filters on
    /// <c>deadlock_hash</c> to answer once per deadlock rather than once per sighting, and the detail read
    /// looks a report up by hash directly. The consume-once RDS route repeats less often and for other
    /// reasons (#3008, #3009), which changes how many sightings a row has rather than what the reads
    /// do.</para>
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
    /// <para><b>The identity column is the point.</b> The <c>pg_read_file</c> transport reads a bounded
    /// TAIL of the log on a schedule and the window OVERLAPS deliberately, so without
    /// <c>deadlock_hash</c> the same deadlock is stored once per cycle for as long as it stays inside the
    /// window. The RDS log-API transport is consume-once and does not overlap — its repeats come from a
    /// restart discarding the in-process marker, or from #3008 leaving it in place after a write that did
    /// not land — and a report cut at one of its chunk boundaries is NOT whole in the next read (#3009).
    /// The hash is over the graph text rather than over
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
    ///
    /// <para>The three capacity columns below arrived at V115 (#3281) and are written into this text as
    /// well, so a store created fresh from this rung gets them without waiting for the ALTER — which is
    /// what keeps the rung identical to the generated schema. V115 is what an existing store applies.</para>
    /// </summary>
    private const string V106Sql = @"
CREATE TABLE IF NOT EXISTS collect.pg_cpu_utilization (
    collection_id bigint NOT NULL,
    collection_time timestamp NOT NULL,
    server_id integer NOT NULL,
    server_name text NOT NULL,
    sample_time timestamp,
    cpu_percent double precision,
    acu_utilization_percent double precision,
    serverless_capacity_acu double precision,
    max_configured_acu double precision,
    memory_total_bytes bigint,
    memory_free_bytes bigint,
    memory_cached_bytes bigint,
    memory_buffers_bytes bigint,
    memory_active_bytes bigint,
    configured_memory_bytes bigint
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
    short_desc text,
    database_name text,
    role_name text
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
    ///
    /// <para><b>The column after <c>backend_count</c> is V133's (#3691), and it is here for the V101 rule.</b>
    /// A fresh store builds the table from the generated schema at V1 (this CREATE then no-ops,
    /// <c>IF NOT EXISTS</c>); a store that climbed through V96 before V133 existed has the six-payload-column
    /// table this text built at the time. <c>PgSchemaGeneratorTests</c> requires this rung to be the
    /// generator's output column for column, and the generator emits the collector's CURRENT columns — so
    /// this text carries <c>sampled_ms</c> for the fresh population, and V133's ALTER carries it for the
    /// existing one. Neither is redundant.</para>
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
    backend_count integer,
    sampled_ms integer
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
    /// V94 — <c>collect.pg_index_bloat</c>. <b>Its column list carries the #3234 estimate columns that
    /// V114 also adds, and that duplication is the convention rather than an oversight</b>: a collector's
    /// creating rung must stay byte-identical to what <c>PgSchemaGenerator.CreateTable</c> emits from its
    /// <c>PayloadColumns</c>, which <c>PgSchemaGeneratorTests.EveryPostgresRung_IsIdenticalToTheGeneratedSchema</c>
    /// asserts. So a fresh install gets the full shape here and V114's <c>ADD COLUMN IF NOT EXISTS</c> is a
    /// no-op for it, while a store already past V94 gets the same columns from V114. Both converge, which
    /// is what the <c>IF NOT EXISTS</c> is for. V95 did the same thing with <c>database_name</c>.
    ///
    /// <para>Originally (#2561): b-tree index bloat, MEASURED via <c>pgstatindex</c>
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
    skipped_reason text,
    index_pages bigint,
    table_rows bigint,
    fillfactor integer,
    est_tuple_bytes bigint,
    est_leaf_pages bigint,
    est_bloat_pct double precision,
    est_reclaimable_bytes bigint,
    pgstattuple_available boolean
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
    /// <c>RunsPerDatabase</c> puts eleven on a per-database CONNECTION loop when the target is Azure SQL DB,
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
    /// the window — superseded by #3631, which made <c>file_growth_rise_mb</c> a RATE, megabytes per HOUR
    /// averaged over the lookback, so the same 10240 means 10 GB/hr on any lookback rather than 10 GB per
    /// window; the column and its default are unchanged, only what the number means), following #2157's
    /// reasoning that a level alone pages forever about a size that has been true since Tuesday; the LEVEL
    /// gate is the file as a share of its VOLUME, which self-scales to each server's disk layout whether or
    /// not the file has a dedicated one.</para>
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
    ///
    /// <para><b>The two columns after <c>interval_length_minutes</c> are V137's (#3796), and they are here for
    /// the V101 rule.</b> A fresh store builds the table from the generated schema at V1 (this CREATE then
    /// no-ops, <c>IF NOT EXISTS</c>); a store that climbed through V76 before V137 existed has the
    /// ten-payload-column table this text built at the time. <c>PgSchemaGeneratorTests</c> requires this rung
    /// to be the generator's output column for column, and the generator emits the collector's CURRENT
    /// columns — so this text carries <c>query_capture_mode</c> and <c>wait_stats_capture_mode</c> for the
    /// fresh population, and V137's ALTER carries them for the existing one. Neither is redundant.</para>
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
    interval_length_minutes bigint,
    query_capture_mode text,
    wait_stats_capture_mode text
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
    /// Since #5072 Index Analysis no longer runs that unbounded read: it takes each database's newest snapshot
    /// (<c>DarlingFinOpsIndexAnalysisReader.IndexObjectStatsLatestSql</c>).
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
    ///
    /// <para><b>Per RUNG, not per statement.</b> <see cref="MigrateLockedAsync"/> issues one
    /// <c>NpgsqlCommand</c> carrying a rung's entire SQL, so V39's two <c>CREATE INDEX</c>es share one
    /// budget instead of getting one each. That is the unit
    /// <see cref="MigrationLockWaitTimeoutSeconds"/>' floor is counted in.</para>
    ///
    /// <para><b>The cancel it sends IS honoured, on every rung this ladder has.</b> Client-side bounds
    /// are only requests, so this was measured rather than assumed, against a seeded PostgreSQL 17.11 /
    /// TimescaleDB 2.29.2 store carrying 13.5 million rows and 90 chunks in each of the four
    /// floor-setting rungs' targets (unbounded there: V22 6.08 s, V39 4.28 s, V104 10.57 s, V23 25.61 s).
    /// At a 1 s and a 2 s budget all four abort within 50 ms of it, from inside <c>IO/DataFileRead</c> and
    /// <c>IO/BuffileRead</c> waits; the backend leaves <c>pg_stat_activity</c> inside 0.4 s, the rung's
    /// transaction rolls back whole and the connection stays usable. The realistic overrun was run at this
    /// constant's real value through this applier — a rung parked behind a peer's table lock, which is
    /// what a second instance collecting into the same store produces: V22 sat in <c>Lock/relation</c> for
    /// the entire budget and ended at 300.06 s, nothing applied and nothing stamped. So the floor in
    /// <see cref="MigrationLockWaitTimeoutSeconds"/> is a ceiling on real holds as well, one budget per
    /// rung.</para>
    ///
    /// <para><b>What it bounds is SILENCE, not wall clock — which is why #2894's residual is a
    /// rung-SHAPE question and not a missing mechanism.</b> Npgsql implements this as a socket READ
    /// timeout, and every message the backend sends restarts it. Matched pair, same statement and the same
    /// 40 s of work both times: emitting one <c>RAISE NOTICE</c> a second it ran to completion in 40.18 s
    /// and a 5 s budget never fired at all, while silent it was cancelled at 5.02 s. A rung is therefore
    /// bounded at this many seconds of BACKEND SILENCE. Today's ladder is quiet — no rung contains a
    /// <c>LOOP</c>, and V23's is its only <c>RAISE</c>, in an exception handler that runs only when the
    /// rung is already failing — so the whole exposure is the two messages <c>create_hypertable</c> emits
    /// itself, both inside the first 3 s, which offsets V23's real ceiling to 303 s while V22, V39 and
    /// V104 are cancelled at 300.05 s. A rung that instrumented its own progress, the natural way to write
    /// a long data move, would not be cancelled at all.</para>
    /// </summary>
    private const int MigrationCommandTimeoutSeconds = 300;

    /// <summary>
    /// Lock-wait budget (seconds) for acquiring <see cref="MigrationLockKey"/>. A different quantity from
    /// <see cref="MigrationCommandTimeoutSeconds"/> above, which bounds ONE statement: the lock is taken once
    /// and held while <see cref="MigrateLockedAsync"/> applies EVERY pending rung in the same session, so what
    /// a sibling waits for here is a whole multi-rung session. Set to the statement bound (as this site first
    /// was) a several-rung upgrade can outlast the waiter while every individual statement stayed inside its
    /// own limit, so the two budgets are named separately and move independently.
    ///
    /// <para><b>Spent by polling, not by blocking.</b> <see cref="TryAcquireMigrationLockAsync"/> retries
    /// <c>pg_try_advisory_lock</c> on a <see cref="MigrationLockPollIntervalSeconds"/> cadence until this
    /// budget is gone, rather than parking one <c>pg_advisory_lock</c> behind an Npgsql
    /// <c>CommandTimeout</c>. Not because the blocking form overran — measured on PostgreSQL 17.11 it
    /// expires on time, since a backend asleep on a lock reaches an interrupt check and honours query
    /// cancel (both <c>lock_timeout</c> and <c>statement_timeout</c> abort a blocked
    /// <c>pg_advisory_lock</c> there too). It is because the blocking form cannot say what it waited for:
    /// Npgsql surfaces the expiry as <c>NpgsqlException("Exception while reading from stream")</c> wrapping
    /// <c>TimeoutException("Timeout during reading attempt")</c>, and the one production caller logs
    /// <c>ex.Message</c> alone — so a 25-minute wait on a sibling migrator reached the operator as a
    /// sentence about a stream, which is exactly the #2874 misdiagnosis shape. Polling also makes the wait
    /// visible WHILE it happens rather than only after it fails, and makes the sleep cancellable by the
    /// stopping token directly instead of through a cancel-request round trip. Each attempt is a statement
    /// that cannot block, so nothing about this budget depends on a cancel being delivered at all.</para>
    ///
    /// <para><b>Lower bound — the total the lock can legitimately be held for.</b> Of the 110 rungs in
    /// <see cref="Scripts"/> (V1-V111, V45 permanently absent), SIX touch data an earlier rung created, and
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
    /// loop entirely until an operator restarts it. #2936's retry loop around that call does not save it,
    /// and deliberately so: <c>StartupFailureTriage</c> does classify the expiry's bare
    /// <see cref="TimeoutException"/> as retryable, but gates the retries on a 120 s WALL-CLOCK budget —
    /// an order of magnitude under a single spend of this constant, precisely so 25 attempts cannot become
    /// ten hours — so the first expiry has already exhausted it and lands in the terminal arm. One expiry
    /// is terminal by design, not by omission. Too long and it only delays its own first
    /// cycle, because its MCP and web surfaces start independently and are already serving. Waiting is the
    /// cheap direction, so this errs long — but stays FINITE, so a genuinely wedged holder still produces a
    /// readable deadline instead of the silent hang #2874 exists to stop.</para>
    ///
    /// <para><b>What expiry does, and why it is not one answer.</b> The lock is needed to APPLY rungs, not
    /// to decide there are none: the applier commits each rung's DDL and its version stamp in one
    /// transaction, so a stamp of N is proof that rung N and everything below it committed. So on expiry
    /// <see cref="TryAcquireMigrationLockAsync"/> reads the stamp table once and splits. Store already at
    /// <see cref="StorageVersion.SchemaVersion"/>: nothing would be applied, the lock was only ever needed
    /// to establish that, and the call returns 0 applied with a warning naming the holder-hunting query.
    /// Store BELOW it: there are rungs to apply, they cannot be applied safely without the lock, and a
    /// <see cref="TimeoutException"/> carrying both versions and that same query is the honest answer. The
    /// split matters because the case that actually strands an instance is not a slow migrator but a dead
    /// one — a session advisory lock outlives the client whenever the server never notices the peer is
    /// gone, and then the store is perfectly current and every restart used to die at this budget for want
    /// of a lock it had nothing to do with. Never proceeding without a POSITIVE version match is what keeps
    /// this from being "give up and collect anyway".</para>
    ///
    /// <para><b>What bounds a rung that overruns its own budget: that budget does, and the residual is a
    /// rung SHAPE rather than a missing mechanism.</b> <see cref="MigrationCommandTimeoutSeconds"/> is
    /// client-side, so whether it BINDS was measured rather than reasoned about — see that constant: the
    /// cancel it sends is honoured by all four floor-setting rungs within 50 ms of the budget, from inside
    /// I/O waits and from a full-budget <c>Lock/relation</c> wait, so the floor above is a ceiling on real
    /// holds too. What it does not bound is a rung that keeps TALKING, because Npgsql restarts the timeout
    /// on every backend message. Two rung shapes opt out of it and both are one line of plpgsql from V23's
    /// existing <c>DO</c> block. A <c>RAISE</c> inside a <c>LOOP</c> is never cancelled and holds this lock
    /// for as long as it runs — loud, because the waiter says so, and caught anyway by the census above
    /// whenever such a rung also moves data. An <c>EXCEPTION WHEN query_canceled</c> is the silent one:
    /// <c>OTHERS</c> deliberately does not match a cancel, but naming the condition does, and measured, the
    /// applier then saw SUCCESS, committed the rung and stamped the version while the rung's own work never
    /// happened. That destroys the "a stamp of N proves rung N committed" property the expiry split above
    /// rests on, and permanently, so <c>MigrationDataMovingRungCensusPins</c> fails the build on the shape
    /// rather than leaving it to be rediscovered.</para>
    ///
    /// <para><b>Three mechanisms considered instead of that pin, and why none of them is built.</b> A
    /// watchdog cancelling the migrate backend out of band buys nothing measurable:
    /// <c>pg_cancel_backend</c> stopped the same four rungs at 2.01-2.03 s and
    /// <c>pg_terminate_backend</c> at 2.01-2.05 s, against this applier's own 2.04-2.07 s on the identical
    /// rungs, because all three arrive through the same <c>CHECK_FOR_INTERRUPTS</c> — nothing one can stop
    /// is beyond the others. Terminate is worse than merely redundant: it reports <c>57P01</c>, which
    /// <c>StartupFailureTriage</c> holds retryable, so it would turn a rung that can never apply into one
    /// that is retried. Splitting the expensive rungs so each statement gets its own budget helps exactly
    /// one of the four — V22, V23 and V104 are each a single statement — and pays for that by raising this
    /// floor from four multiples to five. Both server-side timeouts have the wrong unit or the wrong
    /// reach: <c>statement_timeout</c> is per STATEMENT, not per transaction — three one-second statements
    /// all survive a two-second setting — so it would bound V39's two <c>CREATE INDEX</c>es at one budget
    /// EACH, making the ladder's floor five multiples instead of four and spending this constant's entire
    /// margin to buy a bound that still is not per-rung. <c>transaction_timeout</c> has exactly the right
    /// unit, since each rung is one transaction, but it is PostgreSQL 17+ against readers here that gate
    /// as low as 13, and it ends the session with FATAL rather than failing the statement.</para>
    ///
    /// <para>A multiple rather than a literal so the two budgets cannot drift apart if the rung bound moves.
    /// #2894 recorded two ways the wait still dies, and both are now narrowed rather than closed. A fifth
    /// FLOOR-SETTING rung is pinned rather than trusted — <c>MigrationDataMovingRungCensusPins</c> scans
    /// every rung's shipped SQL for data-moving shapes and fails when that set stops matching the census
    /// above, so a new one arrives carrying this derivation in a failure message instead of arriving
    /// silently. It resolves each statement's TARGET rather than counting keywords, because an index on a
    /// table the same rung creates is free and 130 of this ladder's 134 <c>CREATE INDEX</c> statements are
    /// that shape. A rung needing longer than its own bound turns out to be bounded after all, per the two
    /// paragraphs above — one <see cref="MigrationCommandTimeoutSeconds"/> per rung, honoured, plus V23's
    /// 3 s of <c>create_hypertable</c> chatter — and what is left of that residual is the rung shape that
    /// opts out of the bound silently, which is pinned. Its victim also no longer dies undiagnosed, and no
    /// longer dies at all when it had nothing to apply. The refinement deliberately NOT built is
    /// extending this budget whenever the stamp table is seen to advance, which would make a
    /// slow-but-progressing holder unable to strand a waiter at all: it costs a catalog probe and a read
    /// per poll on the store being migrated, to buy a case that needs a single rung to exceed five minutes
    /// on a ladder measured at 0.301 s.</para>
    /// </summary>
    private const int MigrationLockWaitTimeoutSeconds = 5 * MigrationCommandTimeoutSeconds;

    /// <summary>
    /// Gap (seconds) between <c>pg_try_advisory_lock</c> attempts while a sibling migrator holds the lock.
    ///
    /// <para><b>Upper side — it is the delay added to a wait that would otherwise be over.</b> Whatever the
    /// holder is doing, this instance learns it finished up to one interval late, so the interval has to be
    /// small against the thing being waited for. The whole 109-rung ladder measures 0.301 s on a fresh
    /// store, so the ordinary contended case — a sibling bringing up a new store, or applying nothing — is
    /// already gone by the first retry, and one second is the coarsest value that keeps the notice latency
    /// the same order as the work itself.</para>
    ///
    /// <para><b>Lower side — poll traffic against a store that is concurrently rewriting a hypertable.</b>
    /// Each attempt is one round trip running a function that reads a single lock-manager hash entry.
    /// One second spends at most <see cref="MigrationLockWaitTimeoutSeconds"/> attempts over the whole
    /// budget, which is nothing beside V23 moving a multi-gigabyte heap into chunks; going sub-second would
    /// spend more on scheduling and round trips than the answer is worth, and the answer cannot change
    /// faster than the holder can commit a rung.</para>
    /// </summary>
    private const int MigrationLockPollIntervalSeconds = 1;

    /// <summary>
    /// How much elapsed wait passes between "still waiting" log lines. Separate from
    /// <see cref="MigrationLockPollIntervalSeconds"/> on purpose: how often to ASK is a question about
    /// notice latency, how often to SAY is a question about what a human can read.
    ///
    /// <para><b>Lower side — a legitimate maximum wait must not become a log flood.</b> The lower bound on
    /// the budget above is minutes of real data-moving work, and a line per poll would emit one per second
    /// for all of it, which is indistinguishable in a log from a spin. At 30 s even a wait that runs the
    /// full budget out emits tens of lines, each carrying elapsed and remaining.</para>
    ///
    /// <para><b>Upper side — it must be under the shortest wait worth mentioning.</b> Ordinary contention
    /// resolves in well under this, so the common case stays silent and adds no new startup noise; the
    /// first line only appears once a wait has outlasted every measured single-rung cost on a live-sized
    /// store by a wide margin, at which point it is genuinely news. Going much higher would leave the
    /// operator watching nothing during the interval where the decision to restart the service gets
    /// made.</para>
    /// </summary>
    private const int MigrationLockWaitLogIntervalSeconds = 30;

    /// <summary>
    /// Applies every migration newer than the store's current version, each in its own
    /// transaction, stamping darling_schema_version as it goes. Idempotent — a fully migrated
    /// store is a no-op — and safe under concurrent callers (advisory-locked). The connection
    /// must be open.
    ///
    /// <para>Returns the number applied. A rung is NEVER applied without the lock, but returning 0 does
    /// not by itself mean the lock was held: when a sibling holds it for the whole
    /// <see cref="MigrationLockWaitTimeoutSeconds"/> and the store already carries this build's version,
    /// this returns 0 rather than failing, because there was nothing the lock was needed for. A store
    /// below this build's version in that situation throws <see cref="TimeoutException"/> instead.</para>
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

        if (!await TryAcquireMigrationLockAsync(connection, logger, MigrationLockWaitTimeoutSeconds, cancellationToken))
        {
            /* The budget went and the lock is still someone else's, but the store already carries the
               version this build knows — so this call has no rung to apply and wanted the lock only to
               establish that. Nothing is applied, nothing is stamped, and the store-establishing half
               below never needed the lock in the first place. */
            await TrySetDatabaseSearchPathAsync(connection, logger, cancellationToken);
            return 0;
        }

        int applied;
        try
        {
            applied = await MigrateLockedAsync(connection, logger, cancellationToken);
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

    /// <summary>
    /// Takes <see cref="MigrationLockKey"/> by retrying <c>pg_try_advisory_lock</c> instead of blocking in
    /// <c>pg_advisory_lock</c>, spending at most <see cref="MigrationLockWaitTimeoutSeconds"/> and saying so
    /// every <see cref="MigrationLockWaitLogIntervalSeconds"/> while it does. See that constant for why the
    /// budget is polled rather than handed to a <c>CommandTimeout</c>, and for the derivation of its size.
    ///
    /// <para>TRUE means the lock is held and the caller may apply rungs. FALSE means the budget expired
    /// AND the store already reports <see cref="StorageVersion.SchemaVersion"/>, so there is nothing to
    /// apply and no reason to fail — the caller must apply nothing on that answer. A budget that expires
    /// against a store BELOW this build's version throws instead, because those rungs genuinely cannot be
    /// applied without the lock.</para>
    /// </summary>
    private static async Task<bool> TryAcquireMigrationLockAsync(
        NpgsqlConnection connection, ILogger? logger, int waitBudgetSeconds, CancellationToken cancellationToken)
    {
        var waited = System.Diagnostics.Stopwatch.StartNew();
        var attempts = 0;
        var lastReportedSecond = 0L;

        while (true)
        {
            bool acquired;
            using (var attempt = new NpgsqlCommand("SELECT pg_try_advisory_lock($1)", connection) { CommandTimeout = MigrationCommandTimeoutSeconds })
            {
                attempt.Parameters.AddWithValue(MigrationLockKey);
                acquired = await attempt.ExecuteScalarAsync(cancellationToken) is true;
            }

            attempts++;

            if (acquired)
            {
                if (attempts > 1)
                {
                    logger?.LogInformation(
                        "Migration advisory lock acquired after waiting {Seconds}s ({Attempts} attempts) — " +
                        "another migrator was holding it and has finished.",
                        (long)waited.Elapsed.TotalSeconds, attempts);
                }

                return true;
            }

            if (waited.Elapsed.TotalSeconds >= waitBudgetSeconds)
            {
                break;
            }

            var elapsedSeconds = (long)waited.Elapsed.TotalSeconds;
            if (elapsedSeconds - lastReportedSecond >= MigrationLockWaitLogIntervalSeconds)
            {
                lastReportedSecond = elapsedSeconds;
                logger?.LogWarning(
                    "Still waiting for the migration advisory lock: {Elapsed}s elapsed of {Budget}s, " +
                    "{Attempts} attempts. Another connection is migrating this store, or holds the lock " +
                    "without releasing it. Find it with: SELECT a.pid, a.state, a.query_start, a.query " +
                    "FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory';",
                    elapsedSeconds, waitBudgetSeconds, attempts);
            }

            await Task.Delay(TimeSpan.FromSeconds(MigrationLockPollIntervalSeconds), cancellationToken);
        }

        /* Budget spent. Whether that is fatal depends entirely on whether this call had anything to do,
           which the stamp table answers — see MigrationLockWaitTimeoutSeconds for why a stamp of N proves
           every rung up to N committed, and so why reading it here is not a guess. */
        var storeVersion = await ReadStampedSchemaVersionAsync(connection, cancellationToken);
        var totalWaited = (long)waited.Elapsed.TotalSeconds;

        if (storeVersion >= StorageVersion.SchemaVersion)
        {
            logger?.LogWarning(
                "Gave up on the migration advisory lock after {Elapsed}s ({Attempts} attempts), but this " +
                "store is already at schema v{StoreVersion} and this build needs v{BuildVersion}, so there " +
                "was nothing to apply — continuing without migrating. Something is holding the lock and not " +
                "releasing it, commonly an orphaned backend from an instance that died without closing its " +
                "connection; find it with: SELECT a.pid, a.state, a.query_start, a.query FROM pg_locks l " +
                "JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory';",
                totalWaited, attempts, storeVersion, StorageVersion.SchemaVersion);
            /* #5773: this path skips MigrateLockedAsync, so it makes the newer-store check itself. */
            WarnIfStoreNewerThanBuild(logger, storeVersion, Scripts[^1].Version);
            return false;
        }

        throw new TimeoutException(
            "Timed out after " + totalWaited.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + "s (" + attempts.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + " attempts) waiting for the migration advisory lock. This store is at schema v"
            + storeVersion.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + " and this build needs v"
            + StorageVersion.SchemaVersion.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + ", so there are migrations to apply and they cannot be applied while another connection holds "
            + "the lock. Either a second service instance is migrating the same store, or a connection is "
            + "holding the lock without releasing it. Find the holder with: SELECT a.pid, a.state, "
            + "a.query_start, a.query FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE "
            + "l.locktype = 'advisory';");
    }

    /// <summary>
    /// Test seam: <see cref="TryAcquireMigrationLockAsync"/> with the wait budget supplied, so real
    /// advisory-lock contention can be exercised in seconds instead of the
    /// <see cref="MigrationLockWaitTimeoutSeconds"/> the shipped path deliberately allows. The SAME code
    /// path — the budget is the only thing a test is allowed to shorten, so the poll, the expiry split and
    /// the messages under test are the ones that ship.
    /// </summary>
    internal static Task<bool> TryAcquireMigrationLockForTestsAsync(
        NpgsqlConnection connection, ILogger? logger, int waitBudgetSeconds, CancellationToken cancellationToken)
        => TryAcquireMigrationLockAsync(connection, logger, waitBudgetSeconds, cancellationToken);

    /// <summary>
    /// Test seam: the advisory lock key, so a contention test contends on the REAL one rather than a
    /// restated literal that could drift away from it.
    /// </summary>
    internal static long MigrationLockKeyForTests => MigrationLockKey;

    /// <summary>
    /// The store's highest applied schema version, or 0 when the stamp table does not exist yet. Read on
    /// the lock-wait expiry path only, so the extra round trips cost nothing that matters.
    ///
    /// <para>All three statements are static text. The <c>SET search_path</c> is the same one
    /// <see cref="MigrateLockedAsync"/> issues as its first act — legal even when those schemas do not
    /// exist yet, and it is what lets the bare name resolve either spelling, since the stamp table lives in
    /// <c>public</c> before V8 and in <c>collect</c> after it. <c>to_regclass</c> then answers "does it
    /// exist" with NULL instead of an error, so a fresh store whose holder has not committed V1 yet is an
    /// ordinary 0 rather than a failed statement used as control flow.</para>
    /// </summary>
    private static async Task<int> ReadStampedSchemaVersionAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        using (var setPath = new NpgsqlCommand("SET search_path = " + PgSchemaGenerator.SearchPath, connection) { CommandTimeout = MigrationCommandTimeoutSeconds })
        {
            await setPath.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var probe = new NpgsqlCommand("SELECT to_regclass('darling_schema_version') IS NOT NULL", connection) { CommandTimeout = MigrationCommandTimeoutSeconds })
        {
            if (await probe.ExecuteScalarAsync(cancellationToken) is not true)
            {
                return 0;
            }
        }

        using var read = new NpgsqlCommand("SELECT COALESCE(MAX(version), 0) FROM darling_schema_version", connection) { CommandTimeout = MigrationCommandTimeoutSeconds };
        return Convert.ToInt32(await read.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// #5773: the README section the newer-store warning sends the operator to. It lists, rung by rung, what a
    /// build older than that rung reads differently.
    /// </summary>
    internal const string RollbackReadmeSection = "\"Rolling back to an older build\" in Darling/README.md";

    /// <summary>
    /// #5773: logs ONE warning when the store's stamped version is above the highest rung this build knows, and
    /// returns whether it did. A newer build migrated this store and this one was started against it, which is the
    /// documented rollback (upgrade-darling.ps1 restores the install files, not the store). Rungs are additive, so
    /// this build keeps starting; refusing would break that rollback. But an older build can read some newer data
    /// differently, and without this line nothing tells the operator. The README section lists each case.
    /// The seam the startup check goes through: <see cref="MigrateAsync(NpgsqlConnection, ILogger, CancellationToken)"/>
    /// passes the version it read and <c>Scripts[^1].Version</c>, and tests plant any pair without a store.
    /// </summary>
    internal static bool WarnIfStoreNewerThanBuild(ILogger? logger, int storeVersion, int buildVersion)
    {
        if (storeVersion <= buildVersion)
        {
            return false;
        }

        var unknownRungs = storeVersion == buildVersion + 1
            ? "V" + storeVersion.ToString(System.Globalization.CultureInfo.InvariantCulture) + " is"
            : "V" + (buildVersion + 1).ToString(System.Globalization.CultureInfo.InvariantCulture)
                + "-V" + storeVersion.ToString(System.Globalization.CultureInfo.InvariantCulture) + " are";

        logger?.LogWarning(
            "The store is at V{StoreVersion}; this build knows up to V{BuildVersion}, so {UnknownRungs} unknown to it. " +
            "A newer build migrated this store. This build keeps running against it, because migrations only add, " +
            "but it can read some of the data those rungs added differently. See {ReadmeSection} for what each rung " +
            "changes for an older build, and what to do before rolling back.",
            storeVersion, buildVersion, unknownRungs, RollbackReadmeSection);
        return true;
    }

    private static async Task<int> MigrateLockedAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken)
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

        /* #5773: a store above this build's top rung was migrated by a newer build. Warn and carry on: the loop below
           applies nothing, since every rung this build has is at or below the stamp. */
        WarnIfStoreNewerThanBuild(logger, currentVersion, Scripts[^1].Version);

        var applied = 0;
        foreach (var migration in Scripts)
        {
            if (migration.Version <= currentVersion)
            {
                continue;
            }

            using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            try
            {
                using var apply = new NpgsqlCommand(migration.Sql, connection, transaction) { CommandTimeout = MigrationCommandTimeoutSeconds };
                await apply.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                /* #5582: the rung's SET LOCAL lock_timeout fired, so a session holds a lock on a table the rung needs. Roll back first
                   (the aborted transaction cannot run the diagnostic query), say who holds what, then let the failure go on: the
                   store-migrate loop retries the rung, and collection waits behind that loop. */
                await TryRollbackAsync(transaction);
                await LogLockHoldersAsync(connection, logger, migration.Version, migration.Name, ex.MessageText, cancellationToken);
                throw;
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

    private static async Task TryRollbackAsync(NpgsqlTransaction transaction)
    {
        try
        {
            await transaction.RollbackAsync(CancellationToken.None);
        }
        catch
        {
            /* The failed statement already aborted the transaction; disposing it ends it either way. */
        }
    }

    /// <summary>The least often the lock-holder list is logged, so a retry loop that fails every minute says it once per this long.</summary>
    internal static readonly TimeSpan LockHolderLogInterval = TimeSpan.FromMinutes(10);

    private static readonly object s_lockHolderLogGate = new();
    private static DateTime s_lastLockHolderLogUtc = DateTime.MinValue;

    /// <summary>True when no lock-holder list was logged in the last <see cref="LockHolderLogInterval"/> as of <paramref name="nowUtc"/>.
    /// Reads the throttle without claiming it.</summary>
    internal static bool IsLockHolderLogDue(DateTime nowUtc)
    {
        lock (s_lockHolderLogGate)
        {
            return s_lastLockHolderLogUtc == DateTime.MinValue || nowUtc - s_lastLockHolderLogUtc >= LockHolderLogInterval;
        }
    }

    /// <summary>Claims the right to log the lock holders at <paramref name="nowUtc"/>: true when none was logged in the last
    /// <see cref="LockHolderLogInterval"/>. Static on purpose, because each retry of the rung is a fresh call.</summary>
    internal static bool TryClaimLockHolderLog(DateTime nowUtc)
    {
        lock (s_lockHolderLogGate)
        {
            if (s_lastLockHolderLogUtc != DateTime.MinValue && nowUtc - s_lastLockHolderLogUtc < LockHolderLogInterval)
            {
                return false;
            }

            s_lastLockHolderLogUtc = nowUtc;
            return true;
        }
    }

    /// <summary>Test seam: forgets the last lock-holder log.</summary>
    internal static void ResetLockHolderLogThrottleForTests()
    {
        lock (s_lockHolderLogGate)
        {
            s_lastLockHolderLogUtc = DateTime.MinValue;
        }
    }

    /// <summary>The most lock-holding sessions one log line names ($1 of <see cref="LockHoldersSql"/>).</summary>
    internal const int LockHolderListCap = 20;

    /// <summary>The server-side limits of the lock-holder lookup, set for its own transaction only (#5582). The rung's
    /// <c>lock_timeout</c> is gone once the rung rolled back, so without these the lookup would wait on whatever it met.</summary>
    internal const string LockHolderLookupLimitsSql = "SET LOCAL statement_timeout = '5s'; SET LOCAL lock_timeout = '2s'";

    /// <summary>The sessions of this database holding a granted lock on a <c>collect</c> table, one row per session with the tables and
    /// modes it holds. Every mode is listed, because a plain SELECT takes <c>AccessShareLock</c> and that conflicts with the
    /// <c>AccessExclusiveLock</c> most schema changes take. The row holds the session's pid, backend type, application name, state,
    /// transaction start, wait event, and relation and lock mode; the first 200 characters of the query come back only for an
    /// autovacuum worker.</summary>
    internal const string LockHoldersSql = """
        SELECT a.pid,
               a.backend_type,
               a.application_name,
               a.state,
               a.xact_start,
               concat_ws(': ', a.wait_event_type, a.wait_event) AS wait,
               CASE WHEN a.backend_type = 'autovacuum worker' THEN left(a.query, 200) END AS query,
               string_agg(DISTINCT c.relname || ' (' || l.mode || ')', ', ') AS held
        FROM pg_locks l
        JOIN pg_class c ON c.oid = l.relation
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'collect'
        JOIN pg_stat_activity a ON a.pid = l.pid
        WHERE l.locktype = 'relation'
          AND l.granted
          AND a.datname = current_database()
          AND a.pid <> pg_backend_pid()
        GROUP BY a.pid, a.backend_type, a.application_name, a.state, a.xact_start, a.wait_event_type, a.wait_event,
                 CASE WHEN a.backend_type = 'autovacuum worker' THEN left(a.query, 200) END
        ORDER BY a.xact_start NULLS LAST, a.pid
        LIMIT $1
        """;

    /// <summary>
    /// #5582: after a rung hit <c>lock_timeout</c>, logs the sessions that hold a lock on a <c>collect</c> table: pid, backend type,
    /// application name, state, transaction start, wait event, relation and lock mode, plus the first 200 characters of the query for
    /// an autovacuum worker. At most once per <see cref="LockHolderLogInterval"/>, and the interval is claimed only after the lookup
    /// worked, so a failed lookup does not mute the next attempt. The lookup runs in its own transaction with server-side limits.
    /// A fault in the diagnostic is swallowed, because the rung's own failure is what the caller reports; a cancel is not.
    /// </summary>
    internal static async Task LogLockHoldersAsync(
        NpgsqlConnection connection, ILogger? logger, int version, string name, string reason, CancellationToken cancellationToken, Func<DateTime>? utcNow = null)
    {
        var nowUtc = (utcNow ?? (() => DateTime.UtcNow))();
        if (logger is null || !IsLockHolderLogDue(nowUtc))
        {
            return;
        }

        List<string> holders;
        try
        {
            holders = new List<string>();
            using var lookup = await connection.BeginTransactionAsync(cancellationToken);
            using (var limits = new NpgsqlCommand(LockHolderLookupLimitsSql, connection, lookup) { CommandTimeout = 30 })
            {
                await limits.ExecuteNonQueryAsync(cancellationToken);
            }

            using var command = new NpgsqlCommand(LockHoldersSql, connection, lookup) { CommandTimeout = 30 };
            command.Parameters.AddWithValue(LockHolderListCap);
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var text = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);
                    holders.Add(string.Format(
                        System.Globalization.CultureInfo.InvariantCulture,
                        "pid {0} ({1}), application {2}, state {3}, transaction started {4:u}, waiting on {5}, holding {6}{7}",
                        reader.GetInt32(0),
                        reader.IsDBNull(1) ? "unknown" : reader.GetString(1),
                        reader.IsDBNull(2) || reader.GetString(2).Length == 0 ? "none" : reader.GetString(2),
                        reader.IsDBNull(3) ? "none" : reader.GetString(3),
                        reader.IsDBNull(4) ? (DateTime?)null : reader.GetDateTime(4),
                        reader.IsDBNull(5) || reader.GetString(5).Length == 0 ? "nothing" : reader.GetString(5),
                        reader.GetString(7),
                        text.Length == 0 ? string.Empty : ", query: " + text));
                }
            }

            await lookup.CommitAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException and not OutOfMemoryException)
        {
            logger.LogDebug("The lock-holder lookup for store migration V{Version} failed: {Message}", version, ex.Message);
            return;
        }

        if (!TryClaimLockHolderLog(nowUtc))
        {
            return;
        }

        logger.LogWarning(
            "Store migration V{Version} ({Name}) could not get its table lock in time ({Reason}). Collection waits until it applies. " +
            "Sessions holding a lock on a collect table: {Holders}",
            version, name, reason, holders.Count == 0 ? "none found" : string.Join("; ", holders));
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
