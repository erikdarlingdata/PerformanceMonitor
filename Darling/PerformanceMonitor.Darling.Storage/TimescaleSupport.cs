/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Optional TimescaleDB adoption — RUNTIME setup, deliberately NOT a versioned migration. The
/// store must work with or without the extension (plain PostgreSQL remains fully supported), so
/// the versioned <see cref="PgMigrations"/> scripts stay engine-plain and every Timescale feature
/// here is gated on extension presence, detected at runtime, never assumed. The service calls
/// <see cref="TryEnableAsync"/> once at startup right after migration; when the extension is
/// present it converts the collector tables to hypertables and applies compression policies —
/// all idempotent (<c>if_not_exists</c> everywhere), so every restart re-converges, and a store
/// that grew new collector tables since the last start picks them up on the next.
///
/// Scope: the COLLECTOR tables only (<see cref="HypertableTables"/> = the shared catalog). The
/// registry/config tables (servers, config_alert_log, config_edge_trigger_watermarks,
/// config_mute_rules, analysis_muted, collector_state, darling_schema_version) are deliberately excluded —
/// registries keep their PRIMARY KEYs, which TimescaleDB would reject or force onto the partition
/// column, and none of them is time-series-shaped growth. analysis_findings COULD be a hypertable
/// later (it was designed keyless for exactly this, see the V4 remarks) — deliberately not
/// converted yet; revisit when finding volume warrants it.
///
/// <para><c>collection_log</c> IS a hypertable (the per-run observability log — the store's
/// highest-volume plain table), but it is converted + compressed DIRECTLY by the V23 migration
/// (<see cref="PgMigrations"/>), NOT here, because it lives OUTSIDE the collector catalog (it has no
/// <c>ICollectorSchemaInfo</c>), so the catalog-driven loops below never reach it. Its retention is
/// likewise handled directly by DarlingRetention (<c>drop_chunks</c>). It is counted in
/// <see cref="HypertableCount"/> so worker sizing reflects its compression policy.</para>
///
/// The collector tables were designed for this conversion: no PRIMARY KEY (see the
/// <see cref="PgSchemaGenerator"/> remarks) and a NOT NULL prefix time column per table
/// (<see cref="ICollectorSchemaInfo.PrefixTimeColumnName"/> — "collection_time" almost
/// everywhere, the config snapshots' "capture_time", memory_pressure_events included: its
/// prefix column is still collection_time; payload sample_time is not the partition column).
/// The partition columns are naive-UTC <c>timestamp</c> by the product-wide cross-store
/// contract, so create_hypertable emits an advisory use-TIMESTAMPTZ WARNING — expected and
/// accepted (validated live on TimescaleDB 2.28.1).
/// </summary>
public static class TimescaleSupport
{
    /// <summary>
    /// Compress chunks older than this many days — hardcoded (defaults over speculative config).
    /// Compressed chunks remain fully queryable, just columnar and ~10-20x smaller: this IS
    /// Darling's archival tier, the centralized-store answer to Lite's parquet archive, keeping the
    /// full retention horizon cheap instead of splitting hot/cold stores. Kept short (1 day) to
    /// match <see cref="ChunkIntervalDays"/>: at the collectors' 1-minute cadence a longer lag left
    /// the whole store uncompressed (a chunk cannot compress until it closes AND then ages past
    /// this), so even a near-idle fleet grew ~1 GB in a couple of days of hot data. Collectors only
    /// ever append current-time rows, so a day-old chunk never takes another write — safe to
    /// compress. Measured on this data: perfmon ~16.7x, plan-XML-heavy query_stats ~6.4x.
    /// </summary>
    public const int CompressAfterDays = 1;

    /// <summary>
    /// How often each compression policy WAKES UP and compresses whatever has become eligible — the TICK,
    /// which is a different lever from <see cref="CompressAfterDays"/>: the delay governs which chunks are
    /// eligible, this governs how long an eligible chunk waits before anything acts on it (#1778).
    ///
    /// <para><b>Passed explicitly because TimescaleDB's default is 12 hours and we never chose it.</b>
    /// <c>add_compression_policy</c> computes a default when <c>schedule_interval</c> is omitted — measured on
    /// 2.28.1, a hypertable with <see cref="ChunkIntervalDays"/> = 1 gets exactly <c>12:00:00</c>. The rule is
    /// half the chunk interval CAPPED at 12 hours, not floored at it: a 6-hour chunk interval gets
    /// <c>03:00:00</c>, while 2-day and 7-day intervals both get <c>12:00:00</c> rather than 24h or 84h. The cap
    /// is what makes 12 hours the default on EVERY store shape this product can produce — the 1-day chunks it
    /// creates today, and the 7-day-chunk hypertables an adopted store may still carry from before
    /// <see cref="ChunkIntervalDays"/> was passed (existing chunks keep their original width). That is the
    /// field's "twice-daily fixed tick": a chunk that had already aged
    /// past the delay still sat uncompressed for up to another half-day, and on a pre-dedup field store the
    /// newest closed chunk reached 81 GB before its scheduled compression ever reached it. The newest closed
    /// chunk is always the least-compressed data on disk, so the tick is the width of that exposure.</para>
    ///
    /// <para>One hour rather than something shorter: eligibility only changes once a day per chunk (1-day
    /// chunks, 1-day delay), so a tighter tick buys no latency and only adds wakeups. It also matches the
    /// continuous-aggregate refresh cadence already used a few hundred lines down, so the store has one
    /// background rhythm instead of two. NOT a config knob — defaults over speculative config; nothing in the
    /// field asked to tune this, they asked for it not to be half a day.</para>
    /// </summary>
    public const string CompressScheduleInterval = "1 hour";

    /// <summary>
    /// Hypertable chunk width in days. TimescaleDB's 7-day default is far too coarse for
    /// 1-minute-cadence monitoring data: a chunk stays open (and uncompressible) for its whole
    /// span, so 7-day chunks meant nothing compressed for ~2 weeks. 1-day chunks close daily and
    /// become compressible within <see cref="CompressAfterDays"/>, keeping the store compact.
    /// Applies at hypertable creation (fresh stores); existing chunks keep their original width.
    /// </summary>
    public const int ChunkIntervalDays = 1;

    /* The first conversion of a long-collected plain-PG store rewrites every row into chunks
       (migrate_data); Npgsql's default 30-second command timeout would abandon it halfway.
       Same budget reasoning as DarlingRetention's first-purge DELETE. */
    private const int SetupTimeoutSeconds = 300;

    /// <summary>
    /// Timeout for a one-time bulk aggregate materialization, as opposed to the setup statements
    /// <see cref="SetupTimeoutSeconds"/> covers. NOT a timeout bump papering over a slow query: this is a
    /// deliberate bulk backfill whose duration scales with how much history the store already had, and the
    /// 5-minute setup budget would abort it mid-way on any store large enough to need it. Bounded rather than
    /// infinite so a wedged connection still fails eventually, and safe to hit: TimescaleDB commits the
    /// refresh in per-batch transactions, so an abort keeps the progress made and the coverage gate resumes
    /// from there on the next start.
    /// </summary>
    private const int BackfillTimeoutSeconds = 6 * 60 * 60;

    /// <summary>
    /// The tables converted to hypertables — exactly the shared collector catalog, pinned by
    /// test so scope can never silently widen to the registry/config/analysis tables (see the
    /// class remarks for why those stay plain).
    /// </summary>
    public static IReadOnlyList<ICollectorSchemaInfo> HypertableTables => CollectorCatalog.All;

    /// <summary>
    /// The TRUE number of TimescaleDB hypertables in the store: the collector catalog
    /// (<see cref="HypertableTables"/>) PLUS <c>collection_log</c>, which is a hypertable (converted by the
    /// V23 migration) but lives OUTSIDE the catalog. Worker sizing derives from THIS so it is not under-sized
    /// by one background-worker slot for collection_log's compression policy. The <c>+ 1</c> must move if
    /// another non-catalog table is ever converted (pinned by test).
    /// </summary>
    public static int HypertableCount => HypertableTables.Count + 1;

    /// <summary>
    /// Is the timescaledb extension installed AND created in this database (extensions are
    /// per-database, so pg_extension is the authoritative check)? Callers cache the answer per
    /// data source — the worker detects once at startup and passes the flag around.
    /// </summary>
    public static async Task<bool> DetectAsync(NpgsqlConnection connection, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        using var command = new NpgsqlCommand(
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')", connection);
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// Attempts <c>CREATE EXTENSION IF NOT EXISTS timescaledb</c> and reports whether the
    /// extension is usable. IF NOT EXISTS short-circuits before any privilege check, so a store
    /// whose administrator pre-created the extension works for a service account that could
    /// never create it; a server without the loadable library (or without the privilege to
    /// create it) throws, which degrades gracefully to "not available" — logged once at
    /// Information (plain-PostgreSQL mode is a fully supported configuration, not a problem).
    ///
    /// <para><b>A <c>false</c> return may mean <paramref name="connection"/> IS NO LONGER USABLE, and callers
    /// must not keep using it (#1922).</b> One of the ways this fails is not an ordinary ERROR: when the
    /// library is present on disk but missing from <c>shared_preload_libraries</c>, <c>CREATE EXTENSION</c>
    /// TERMINATES THE BACKEND. The catch below turns that into <c>false</c> like any other failure, so the
    /// contract reads as "carry on in plain-PostgreSQL mode" while the connection is in fact dead, and the
    /// next statement on it throws <c>InvalidOperationException: Connection is not open</c> from wherever
    /// that happens to be — naming the cause nowhere.</para>
    ///
    /// <para><c>DarlingWorker</c> is safe from this by construction and deliberately so: it opens a DEDICATED
    /// connection for the TimescaleDB block and gates every subsequent call on the returned flag, so a
    /// <c>false</c> return means nothing touches that connection again before it is disposed. <b>Keep it that
    /// way</b> — moving a call out from under the flag, or reusing the connection afterwards, reintroduces
    /// the same masking in the service.</para>
    /// </summary>
    public static async Task<bool> TryEnableAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using var create = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS timescaledb", connection);
            await create.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogInformation("TimescaleDB not available — running in plain-PostgreSQL mode ({Message})", ex.Message);
            return false;
        }

        /* Belt-and-suspenders: CREATE EXTENSION IF NOT EXISTS succeeding means present, but
           pg_extension stays the single source of truth for "installed AND created". */
        var present = await DetectAsync(connection, cancellationToken);
        if (present)
        {
            logger?.LogInformation("TimescaleDB detected — hypertables, chunk-based retention, and compression enabled");
        }
        else
        {
            logger?.LogInformation("TimescaleDB not available — running in plain-PostgreSQL mode");
        }

        return present;
    }

    /// <summary>
    /// One collector table's hypertable conversion, partitioned on the definition's own prefix
    /// time column. The generalized <c>by_range</c> dimension form, validated live on
    /// TimescaleDB 2.28.1: <c>if_not_exists</c> makes an already-converted table a no-op NOTICE
    /// and <c>migrate_data</c> moves any rows a plain-PG store collected before the extension
    /// arrived. Table and column names come from the shared catalog constants, never from user
    /// input, so interpolation is safe here — the same reasoning as
    /// DarlingRetention.DeleteSqlFor.
    /// </summary>
    public static string CreateHypertableSql(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        return CreateHypertableSql(schema.TargetTable, schema.PrefixTimeColumnName);
    }

    /// <summary>
    /// The raw-name hypertable-conversion overload — the collection_log path (a hypertable since V23 but
    /// outside the collector catalog, so it has no <see cref="ICollectorSchemaInfo"/>). Identical shape to the
    /// schema overload; table/column come from compile-time constants, never user input, so interpolation is
    /// safe (the same reasoning as DarlingRetention.DeleteSqlFor).
    /// </summary>
    public static string CreateHypertableSql(string table, string timeColumn)
        => $"SELECT create_hypertable('{table}', by_range('{timeColumn}', INTERVAL '{ChunkIntervalDays} days'), if_not_exists => true, migrate_data => true)";

    /// <summary>
    /// One collector table's compression enablement, segmented by server_id so each server's
    /// rows compress together (every query filters server_id first — the retrieval indexes lead
    /// with it). The order-by defaults to the partition time column descending, which is exactly
    /// the read order. NOTE for the live validator: this is the long-stable pre-2.18 compression
    /// vocabulary (<c>timescaledb.compress</c> / <c>compress_segmentby</c>); TimescaleDB 2.18+
    /// rebranded it "columnstore" (<c>timescaledb.enable_columnstore</c> / <c>segmentby</c>) but
    /// keeps these as supported aliases — preferred here for compatibility across 2.x.
    /// </summary>
    public static string EnableCompressionSql(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        return EnableCompressionSql(schema.TargetTable);
    }

    /// <summary>The raw-name compression-enable overload — the collection_log path (see
    /// <see cref="CreateHypertableSql(string, string)"/>).</summary>
    public static string EnableCompressionSql(string table)
        => $"ALTER TABLE {table} SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')";

    /// <summary>
    /// One collector table's background compression policy — chunks older than
    /// <see cref="CompressAfterDays"/> compress automatically, checked every
    /// <see cref="CompressScheduleInterval"/>; <c>if_not_exists</c> makes the
    /// re-apply on every service start a no-op. Same 2.18+ naming note as
    /// <see cref="EnableCompressionSql"/> (<c>add_compression_policy</c> is the long-stable
    /// alias of the newer <c>add_columnstore_policy</c>).
    ///
    /// <para><b>This statement alone only fixes FRESH stores</b>, which is why
    /// <see cref="ConvergeCompressionScheduleAsync"/> exists. Measured on 2.28.1: called against a store that
    /// already has a compression policy with a DIFFERENT <c>schedule_interval</c>, <c>if_not_exists => true</c>
    /// returns <c>-1</c> and emits <c>NOTICE: columnstore policy already exists ... skipping</c> — it does not
    /// reconcile the parameter. Every store that ever started on an older build would therefore keep the
    /// 12-hour tick forever, including the field store #1778 was reported from. The signature verified live is
    /// <c>(hypertable REGCLASS, compress_after "any", if_not_exists BOOL, schedule_interval INTERVAL,
    /// initial_start TIMESTAMPTZ, timezone TEXT, compress_created_before INTERVAL)</c>, and the extension's own
    /// SQL notes it is "not strict because we need to set different default values for schedule_interval" —
    /// i.e. the default is computed in C, so omitting the argument is not the same as passing what we want.</para>
    /// </summary>
    public static string AddCompressionPolicySql(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        return AddCompressionPolicySql(schema.TargetTable);
    }

    /// <summary>The raw-name compression-policy overload — the collection_log path (see
    /// <see cref="CreateHypertableSql(string, string)"/>).</summary>
    public static string AddCompressionPolicySql(string table)
        => $"SELECT add_compression_policy('{table}', compress_after => INTERVAL '{CompressAfterDays} days', schedule_interval => INTERVAL '{CompressScheduleInterval}', if_not_exists => true)";

    /* ─────────────────────────── continuous aggregates (query acceleration) ─────────────────────────── */

    /// <summary>The hourly continuous-aggregate view names — query-acceleration rollups for the two tables that
    /// dominate the store (query_stats ~145 GB, procedure_stats ~49 GB, ~90% together). Every Custom Views
    /// composer panel over these tables does date_trunc('hour', collection_time) + SUM(delta_*) GROUP BY a
    /// dimension; these pre-materialize exactly that shape so anything older than the ~2-day hot window reads the
    /// rollup instead of scanning raw per-sweep rows. NOT retention (raw still exists for the hot window; dropping
    /// old raw chunks is a separate, unmade decision).</summary>
    /* -- Baseline tier (#1757) ---------------------------------------------------------------------
       The anomaly baseline asks for BaselineWindowDays (30) of history bucketed by hour-of-day x
       day-of-week; tiered retention shrank raw to 4 days. That is a CORRECTNESS regression, not a speed
       one: seven day-of-week buckets cannot be filled from four days, so on every tiered store the
       thresholds still compute, just on a fraction of the intended history and with no error at all.

       These aggregates are the baseline's own supply. THE HOURLY BUCKET IS PURELY A PARTITIONING AND
       RETENTION KEY; collection_time CARRIES THE GRAIN -- which is why every one of them groups by
       time_bucket AND collection_time. Do not "simplify" the double GROUP BY away: collapsing to the
       hourly bucket changes the unit of observation from one collection snapshot to one hour, which is a
       different statistic at a different scale, and STDDEV_SAMP cannot be reconstructed from hourly sums
       at all (the hourly tier stores no sum-of-squares). Preserving collection_time is exactly what makes
       the provider's AVG / STDDEV_SAMP / restart-exclusion LAG numerically identical to the raw path.

       Each aggregate materializes its families' per-collection collapse with their row-level filters
       INSIDE. Baking is safe because every baseline filter is a literal constant or an immutable sanity
       bound -- the provider takes no settings dependency at all, so nothing here can freeze a configurable
       behavior. The restart-exclusion prior_* predicates deliberately stay in the provider: they apply
       AFTER the collapse, over the collapsed series.

       file_io is the one family whose unit is NOT the collection: IoLatency averages a per-FILE ratio
       across file rows, so a per-collection total would be a different statistic. It stores that ratio's
       SUFFICIENT STATISTICS instead (sum, sum of squares, count), from which AVG and STDDEV_SAMP
       reconstruct exactly. */

    /// <summary>Baseline-tier retention horizon. MUST stay at or above <c>BaselineMath.BaselineWindowDays</c>
    /// (30) or #1757 silently returns; Darling.Tests pins that relation, because Storage cannot reference
    /// Analysis. 35 days gives the window five days of headroom so a drop can never eat its edge.</summary>
    public const string BaselineRetentionInterval = "35 days";

    /// <summary><see cref="TimeSpan"/> twin of <see cref="BaselineRetentionInterval"/>, pinned equal by test.</summary>
    public static readonly TimeSpan BaselineRetentionSpan = TimeSpan.FromDays(35);

    public const string PerfmonBaselineView = "perfmon_baseline";
    public const string WaitStatsBaselineView = "wait_stats_baseline";
    public const string SessionStatsBaselineView = "session_stats_baseline";
    public const string QueryStatsBaselineView = "query_stats_baseline";
    public const string BlockedProcessBaselineView = "blocked_process_baseline";
    public const string DeadlockBaselineView = "deadlock_baseline";
    public const string MemoryBaselineView = "memory_baseline";

    /// <summary>
    /// Baseline relations RETIRED by #2007: the CPU and IO anomaly arms read the RAW hypertables
    /// (cpu_utilization_stats / file_io_stats, 30-day service-side retention floored by
    /// DarlingRetention.BaselineServingRawCollectors) since the #1743/#1995 robust-statistics work
    /// — medians cannot be computed from these aggregates' sufficient statistics, so nothing reads
    /// them anymore, yet they kept materializing on schedule and holding storage on every store.
    /// Named here so <see cref="DropRetiredBaselineAggregatesAsync"/> can remove BOTH historical
    /// implementations (the continuous aggregate on TimescaleDB stores, the plain fallback view on
    /// plain-PostgreSQL stores) on the next service start, and so a future aggregate can never
    /// silently reuse these names against a store that still carries the old objects.
    /// </summary>
    public static readonly string[] RetiredBaselineRelations =
    {
        "cpu_utilization_baseline",
        "file_io_baseline",
    };

    /// <summary>BatchRequests baseline supply -- the counter_name and non-negative filters bake in. Unlike
    /// cpu, one row per collection here is a property of the DMV (Batch Requests/sec is a single instance)
    /// rather than something the collector guarantees -- it applies no object_name/instance_name predicate.
    /// sum() over a one-row group is that row, so this stays exact either way.</summary>
    public const string CreatePerfmonBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.perfmon_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    sum(delta_cntr_value) AS delta_cntr_value
FROM collect.perfmon_stats
WHERE counter_name = 'Batch Requests/sec'
AND   delta_cntr_value >= 0
GROUP BY server_id, bucket, collection_time
WITH NO DATA";

    /// <summary>WaitStats AND WaitMsPerSec baseline supply. Both families share this source and share the
    /// identical row-level filter (delta_wait_time_ms >= 0), which is what lets one aggregate serve both;
    /// Darling.Tests pins that sharing so a future family-specific filter cannot silently poison its
    /// sibling's supply. WaitMsPerSec's interval_sec comes from LAG(collection_time) over the COLLAPSED
    /// series, so the provider computes it and nothing extra is stored here.</summary>
    public const string CreateWaitStatsBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.wait_stats_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    sum(delta_wait_time_ms) AS total_wait_ms
FROM collect.wait_stats
WHERE delta_wait_time_ms >= 0
GROUP BY server_id, bucket, collection_time
WITH NO DATA";

    /// <summary>SessionCount baseline supply -- total connections per collection.</summary>
    public const string CreateSessionStatsBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.session_stats_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    sum(connection_count) AS total_connections
FROM collect.session_stats
GROUP BY server_id, bucket, collection_time
WITH NO DATA";

    /// <summary>QueryDuration baseline supply -- the family that reported #1757, and the expensive collapse:
    /// millions of per-query rows become one row per collection_time.</summary>
    public const string CreateQueryStatsBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    sum(delta_elapsed_time) AS total_elapsed
FROM collect.query_stats
WHERE delta_execution_count > 0
AND   delta_elapsed_time >= 0
GROUP BY server_id, bucket, collection_time
WITH NO DATA";


    /// <summary>Blocking AND BlockingPerMinute baseline supply -- both families share this source and both
    /// have NO row-level filter, so one aggregate serves both. Event counts per collection re-aggregate to
    /// either shape: Blocking sums them per hour/dow bucket, BlockingPerMinute re-buckets by minute.</summary>
    public const string CreateBlockedProcessBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.blocked_process_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    count(*) AS event_count
FROM collect.blocked_process_reports
GROUP BY server_id, bucket, collection_time
WITH NO DATA";

    /// <summary>Deadlock baseline supply -- event counts per collection, same shape as blocking.</summary>
    public const string CreateDeadlockBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.deadlock_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    count(*) AS event_count
FROM collect.deadlocks
GROUP BY server_id, bucket, collection_time
WITH NO DATA";

    /// <summary>Memory baseline supply -- the pressure ratio, server-level so one row per collection. The
    /// target > 0 filter bakes in, and the ratio is computed here so the provider averages the same numbers
    /// the raw path averaged.</summary>
    public const string CreateMemoryBaselineSql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.memory_baseline
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    server_id,
    time_bucket('1 hour', collection_time) AS bucket,
    collection_time,
    avg(total_server_memory_mb::DOUBLE PRECISION / NULLIF(target_server_memory_mb::DOUBLE PRECISION, 0) * 100) AS memory_pressure_pct
FROM collect.memory_stats
WHERE target_server_memory_mb > 0
GROUP BY server_id, bucket, collection_time
WITH NO DATA";

    /// <summary>
    /// One-time backfill of the baseline aggregates (#1757). WITHOUT THIS THE WHOLE CHANGE IS A REGRESSION:
    /// the aggregates are created WITH NO DATA and their refresh policy has a 3-day start_offset, so left
    /// alone they would hold roughly three days for a thirty-day question -- less than the four-day raw
    /// horizon the change exists to escape. The provider is repointed at them, so an un-backfilled deploy
    /// loses every baseline rather than improving it.
    ///
    /// <para>COVERAGE-GATED and self-healing, the shape the reshape sweep already uses: it compares the
    /// oldest bucket the aggregate holds against the oldest row raw still has and refreshes only the gap,
    /// so it converges to a no-op on every subsequent start rather than re-running a full refresh forever.
    /// A fresh store has nothing to backfill and skips immediately.</para>
    ///
    /// <para>Runs OUTSIDE a transaction on purpose -- refresh_continuous_aggregate cannot run inside one --
    /// and is failure-isolated per aggregate: a backfill that fails leaves that family reading a short
    /// window (logged loudly) rather than taking down startup. MUST run after
    /// <see cref="EnsureContinuousAggregatesAsync"/>.</para>
    /// </summary>
    public static async Task<int> BackfillBaselineAggregatesAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var backfilled = 0;

        foreach (var (_, view) in BaselineAggregates)
        {
            try
            {
                var source = SourceTableFor(view);

                var probeSql = BaselineBackfillProbeSql(view, source);

                DateTime? sourceOldest = null;
                DateTime? coverageOldest = null;
                DateTime? needFrom = null;
                using (var probe = new NpgsqlCommand(probeSql, connection) { CommandTimeout = SetupTimeoutSeconds })
                await using (var reader = await probe.ExecuteReaderAsync(cancellationToken))
                {
                    if (await reader.ReadAsync(cancellationToken))
                    {
                        sourceOldest = reader.IsDBNull(0) ? null : reader.GetDateTime(0);
                        coverageOldest = reader.IsDBNull(1) ? null : reader.GetDateTime(1);
                        needFrom = reader.IsDBNull(2) ? null : reader.GetDateTime(2);
                    }
                }

                /* Nothing in the source: a fresh store has no history to materialize. */
                if (sourceOldest is null || needFrom is null)
                {
                    continue;
                }

                /* Already reaches at least as far back as we need. This is what makes the pass converge to a
                   no-op instead of re-refreshing on every start. */
                if (coverageOldest is not null && coverageOldest <= needFrom)
                {
                    continue;
                }

                var after = await RefreshFromAsync(connection, view, needFrom.Value, force: false, cancellationToken);

                /* VERIFY, DO NOT ASSUME -- the failure this guards is SILENT. On a CAGG that has just been
                   created the plain refresh is enough: creation writes an infinite [-infinity, +infinity]
                   invalidation ("initially, everything is invalid") and WITH NO DATA does not skip it, so the
                   whole pre-existing history is materialized on the first pass. What the plain refresh cannot
                   be trusted to repair is the SECOND pass: a refresh CONSUMES invalidations as it goes, so an
                   earlier backfill cut short by a shutdown can leave a region un-materialized whose
                   invalidation entries are already gone, and a later plain refresh then no-ops over the hole
                   and reports success. The forced form ignores the invalidation log and batches every bucket
                   in range, which is what actually repairs that. Escalate only on evidence: it is strictly
                   more work, and `force` only exists from TimescaleDB 2.18 (an older bring-your-own store
                   raises 42883 here, which the per-aggregate catch reports rather than crashing the pass). */
                if (after is null || after > needFrom)
                {
                    logger?.LogInformation(
                        "TimescaleDB: {View} still starts at {After} after a plain refresh from {NeedFrom}; escalating to a forced refresh.",
                        view, after, needFrom);
                    after = await RefreshFromAsync(connection, view, needFrom.Value, force: true, cancellationToken);
                }

                backfilled++;
                logger?.LogInformation(
                    "TimescaleDB: backfilled baseline aggregate {View} from {NeedFrom} (now starts at {After}) -- it covered from {CoverageOldest} but {Source} reaches back to {SourceOldest}.",
                    view, needFrom, after, coverageOldest, source, sourceOldest);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                /* Worse than it sounds, which is why it logs loudly: with real-time aggregation the watermark
                   is a HARD PARTITION (materialized WHERE time < watermark UNION ALL raw WHERE time >=
                   watermark), so an un-materialized region older than the watermark returns nothing from
                   EITHER branch -- raw is excluded by construction. A failed backfill is not "reads raw
                   instead", it is a hole. Still isolated per aggregate: one failure must not take down the
                   pass, and the coverage gate retries it on the next start. */
                logger?.LogWarning(
                    "Baseline aggregate {View} backfill FAILED -- its baselines are computed from a short window until this succeeds, and history older than its watermark reads as absent rather than falling back to raw: {Message}",
                    view, ex.Message);
            }
        }

        return backfilled;
    }

    /// <summary>
    /// The backfill's refresh statement. The bounds are BOUND parameters and explicitly cast, not
    /// interpolated: <c>window_start</c>/<c>window_end</c> are declared <c>"any"</c>, so an untyped literal
    /// leaves PostgreSQL without a type to resolve the polymorphic argument against.
    ///
    /// <para><paramref name="force"/> maps to the 4th argument of the 2.28.1 signature
    /// <c>refresh_continuous_aggregate(cagg REGCLASS, window_start "any", window_end "any", force BOOLEAN =
    /// FALSE, options JSONB = NULL)</c>. NOTE that the tunables the published API page documents as named
    /// arguments (<c>buckets_per_batch</c>, <c>refresh_newest_first</c>) are NOT parameters of this
    /// procedure — they live inside <c>options</c>, and only the POLICY takes them by name. The defaults are
    /// what we want anyway: a manual refresh already batches internally, so this needs no hand-rolled
    /// slicing.</para>
    /// </summary>
    public static string RefreshContinuousAggregateSql(string view, bool force = false)
        => force
            ? $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, NULL::timestamp, true)"
            : $"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, NULL::timestamp)";

    /// <summary>
    /// Refreshes <paramref name="view"/> from <paramref name="from"/> forward and returns the aggregate's
    /// oldest bucket AFTERWARDS, so the caller can check the refresh actually materialized the range instead
    /// of trusting that a successful CALL means a filled aggregate.
    /// </summary>
    private static async Task<DateTime?> RefreshFromAsync(
        NpgsqlConnection connection, string view, DateTime from, bool force, CancellationToken cancellationToken)
    {
        using (var refresh = new NpgsqlCommand(RefreshContinuousAggregateSql(view, force), connection)
        {
            CommandTimeout = BackfillTimeoutSeconds,
        })
        {
            refresh.Parameters.AddWithValue(from);
            await refresh.ExecuteNonQueryAsync(cancellationToken);
        }

        using var probe = new NpgsqlCommand($"SELECT min(bucket) FROM collect.{view}", connection)
        {
            CommandTimeout = SetupTimeoutSeconds,
        };
        return await probe.ExecuteScalarAsync(cancellationToken) is DateTime oldest ? oldest : null;
    }

    /// <summary>
    /// The marker that separates a baseline aggregate's CREATE header from its SELECT body. Both the
    /// continuous aggregate and the plain-PostgreSQL fallback view are built from that ONE body, so the two
    /// cannot drift into computing different statistics — which is the whole reason the fallback is derived
    /// rather than written out a second time.
    /// </summary>
    private const string BaselineBodyMarker = "timescaledb.materialized_only = false) AS";

    /// <summary>
    /// The plain-PostgreSQL fallback for a baseline aggregate: the SAME select, as an ordinary view.
    ///
    /// <para>WITHOUT THIS, #1757's fix is a REGRESSION on any store without TimescaleDB. The provider reads
    /// the baseline relations by name; if they do not exist it throws, <c>ComputeBaselinesAsync</c> swallows
    /// it and logs "Failed to compute baselines for {metric}" — the exact line #1757 was reported on — and
    /// every family silently returns an empty baseline. Darling supports plain-PostgreSQL stores (the worker
    /// degrades to that mode whenever the TimescaleDB block fails), so this is a real deployment, not a
    /// theoretical one.</para>
    ///
    /// <para><c>time_bucket</c> is the one TimescaleDB-only construct in the body, and for a 1-hour bucket
    /// <c>date_trunc('hour', ...)</c> is the same value, so the view presents an identical column set. Nothing
    /// reads <c>bucket</c> off these relations outside the TimescaleDB-only backfill and retention paths, but
    /// it is kept so the two shapes stay column-for-column identical.</para>
    /// </summary>
    public static string CreateBaselineFallbackViewSql(string view, string createSql)
    {
        if (createSql is null)
        {
            throw new ArgumentNullException(nameof(createSql));
        }

        var bodyAt = createSql.IndexOf(BaselineBodyMarker, StringComparison.Ordinal);
        var endAt = createSql.LastIndexOf("WITH NO DATA", StringComparison.Ordinal);
        if (bodyAt < 0 || endAt < 0 || endAt <= bodyAt)
        {
            throw new ArgumentException(
                $"'{view}' does not have the expected baseline-aggregate shape, so its plain-PostgreSQL fallback cannot be derived",
                nameof(createSql));
        }

        var body = createSql[(bodyAt + BaselineBodyMarker.Length)..endAt]
            .Replace("time_bucket('1 hour', collection_time)", "date_trunc('hour', collection_time)", StringComparison.Ordinal)
            .Trim();

        return $"CREATE OR REPLACE VIEW collect.{view} AS{Environment.NewLine}{body}";
    }

    /// <summary>
    /// Guarantees every baseline relation EXISTS, filling any gap with an ordinary view over the same select.
    /// Returns how many gaps it filled.
    ///
    /// <para>PER VIEW AND DELIBERATELY UNGATED, because "no TimescaleDB" is not the only way a relation goes
    /// missing. <see cref="EnsureContinuousAggregatesAsync"/> is failure-isolated per aggregate, so a store
    /// with the extension can end up with eight aggregates and one gap — and the provider reads these
    /// relations by name, so that one gap is one family silently returning nothing. Gating this on
    /// "TimescaleDB unavailable" would cover the plain-PostgreSQL store and leave the partially-built one
    /// broken, which is the harder case to notice.</para>
    ///
    /// <para>The existence probe is what makes it safe to run everywhere: a continuous aggregate is itself a
    /// <c>relkind='v'</c> view, so an unconditional <c>CREATE OR REPLACE VIEW</c> by these names would destroy
    /// a materialization. Anything already present — aggregate or view — is left strictly alone. MUST run
    /// AFTER <see cref="EnsureContinuousAggregatesAsync"/> so a real aggregate always wins the name.</para>
    /// </summary>
    public static async Task<int> EnsureBaselineFallbackViewsAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var filled = 0;

        foreach (var (createSql, view) in BaselineAggregates)
        {
            try
            {
                using (var probe = new NpgsqlCommand(BaselineRelationExistsSql(view), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    if (await probe.ExecuteScalarAsync(cancellationToken) is true)
                    {
                        continue;
                    }
                }

                using var create = new NpgsqlCommand(CreateBaselineFallbackViewSql(view, createSql), connection)
                {
                    CommandTimeout = SetupTimeoutSeconds,
                };
                await create.ExecuteNonQueryAsync(cancellationToken);
                filled++;

                logger?.LogInformation(
                    "Baseline relation {View} had no continuous aggregate — created it as a plain view so its anomaly baseline still computes (reading raw directly).",
                    view);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Baseline relation {View} is MISSING and could not be backed by a plain view — that metric's anomaly baseline will silently return nothing: {Message}",
                    view, ex.Message);
            }
        }

        return filled;
    }

    /// <summary>Does this baseline relation exist under any implementation — continuous aggregate or plain view?</summary>
    public static string BaselineRelationExistsSql(string view)
        => $"SELECT to_regclass('collect.{view}') IS NOT NULL";

    /// <summary>
    /// The backfill's coverage gate: how far back the source reaches, how far back the aggregate covers, and
    /// how far back we NEED it to cover. The caller backfills when the source has rows and coverage is either
    /// empty or starts later than <c>need_from</c>.
    ///
    /// <para>THE CLAMP DIRECTION IS THE WHOLE THING, and it is easy to get backwards in a way that reads fine.
    /// <c>GREATEST</c> clamps the NEED — "go back as far as the source reaches, but no further than this
    /// tier's own retention" — because materializing past the tier's retention only hands its retention policy
    /// something to drop. Clamping the COVERAGE side instead (<c>LEAST</c> over the coverage and the window)
    /// inverts the gate: an empty aggregate collapses to <c>now - window</c>, the comparison becomes
    /// "now - window &lt;= source oldest", and on every store whose raw retention is SHORTER than the window
    /// it is unconditionally true — so the gate skips exactly the tiered stores it exists for, and fires only
    /// on stores that do not need it. BaselineSupplyTests pins this direction.</para>
    ///
    /// <para>This is the same predicate shape the retention arming uses (<c>MeasureRetentionCoverageAsync</c>) over
    /// the same pair of relations, deliberately: what we backfill and what unblocks arming cannot be allowed
    /// to drift apart.</para>
    /// </summary>
    public static string BaselineBackfillProbeSql(string view, string source)
        => $@"
SELECT
    (SELECT min(collection_time) FROM collect.{source}) AS source_oldest,
    (SELECT min(bucket) FROM collect.{view}) AS coverage_oldest,
    time_bucket('1 hour', GREATEST(
        (SELECT min(collection_time) FROM collect.{source}),
        now()::timestamp - INTERVAL '{BaselineRetentionInterval}')) AS need_from";

    /// <summary>
    /// Drops a baseline relation ONLY when it is a plain fallback view and NOT a continuous aggregate — the
    /// store gained TimescaleDB after running without it, and the fallback now stands in the way of the real
    /// aggregate. The `continuous_aggregates` half of the guard is what makes this safe: a CAGG is also a
    /// <c>relkind='v'</c> view, so an unguarded DROP VIEW here would silently destroy a materialized tier.
    /// </summary>
    public static string DropBaselineFallbackViewSql(string view)
        => $@"DO $do$
DECLARE
    is_continuous_aggregate boolean := false;
BEGIN
    IF NOT EXISTS (
            SELECT 1
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'collect' AND c.relname = '{view}' AND c.relkind = 'v')
    THEN
        RETURN;
    END IF;

    /* timescaledb_information only exists once the extension has been created, and this runs on stores where
       it never was -- reaching it unconditionally raises 42P01. Probed with to_regclass (NULL rather than an
       error when absent) and read through EXECUTE so the reference is parsed only when it resolves. No
       extension means nothing can be a continuous aggregate, so the guard is simply false. */
    IF to_regclass('timescaledb_information.continuous_aggregates') IS NOT NULL
    THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates WHERE view_schema = ''collect'' AND view_name = ''{view}'')'
        INTO is_continuous_aggregate;
    END IF;

    IF NOT is_continuous_aggregate
    THEN
        EXECUTE 'DROP VIEW collect.{view}';
    END IF;
END
$do$";

    /// <summary>
    /// Drops one RETIRED baseline relation (#2007) in whichever implementation this store carries:
    /// <c>DROP MATERIALIZED VIEW ... CASCADE</c> when it is a continuous aggregate (TimescaleDB
    /// removes its refresh/retention policies with it), <c>DROP VIEW</c> when it is the plain
    /// fallback a TimescaleDB-less store created under the same name, and a no-op when neither
    /// exists. The same relkind + continuous_aggregates discrimination
    /// <see cref="DropBaselineFallbackViewSql"/> uses, because a CAGG is also a
    /// <c>relkind='v'</c> view and the two need different DROP verbs.
    /// </summary>
    public static string DropRetiredBaselineRelationSql(string view)
        => $@"DO $do$
DECLARE
    is_continuous_aggregate boolean := false;
BEGIN
    IF to_regclass('collect.{view}') IS NULL
    THEN
        RETURN;
    END IF;

    /* timescaledb_information only exists once the extension has been created; to_regclass probes
       it without raising, and no extension means nothing can be a continuous aggregate. */
    IF to_regclass('timescaledb_information.continuous_aggregates') IS NOT NULL
    THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates WHERE view_schema = ''collect'' AND view_name = ''{view}'')'
        INTO is_continuous_aggregate;
    END IF;

    IF is_continuous_aggregate
    THEN
        EXECUTE 'DROP MATERIALIZED VIEW IF EXISTS collect.{view} CASCADE';
    ELSE
        EXECUTE 'DROP VIEW IF EXISTS collect.{view} CASCADE';
    END IF;
END
$do$";

    /// <summary>
    /// Removes the <see cref="RetiredBaselineRelations"/> (#2007) from this store — the CPU/IO
    /// baseline aggregates nothing has read since the anomaly arms moved to the raw hypertables,
    /// which otherwise keep materializing on schedule and holding storage forever. Runs from the
    /// worker's UNGATED fallback block (its own connection, every store shape): on TimescaleDB
    /// stores it drops the aggregates and their policies, on plain-PostgreSQL stores the fallback
    /// views, and on fresh stores it no-ops. Failure-isolated per relation, like every other
    /// startup sweep — a failed drop is retried on the next start and never kills the service.
    /// Returns how many relations were actually dropped.
    /// </summary>
    public static async Task<int> DropRetiredBaselineAggregatesAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var dropped = 0;
        foreach (var view in RetiredBaselineRelations)
        {
            try
            {
                bool existed;
                using (var probe = new NpgsqlCommand(BaselineRelationExistsSql(view), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    existed = await probe.ExecuteScalarAsync(cancellationToken) is true;
                }

                if (!existed)
                {
                    continue;
                }

                using (var drop = new NpgsqlCommand(DropRetiredBaselineRelationSql(view), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await drop.ExecuteNonQueryAsync(cancellationToken);
                }

                dropped++;
                logger?.LogInformation(
                    "Dropped retired baseline relation {View} (#2007) — the CPU/IO anomaly arms read the raw hypertables, so nothing consumed it.",
                    view);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Could not drop retired baseline relation {View} — it lingers (harmlessly, but materializing) until the next restart retries: {Message}",
                    view, ex.Message);
            }
        }

        return dropped;
    }

    /// <summary>The raw table each baseline aggregate is sourced from, for the backfill's coverage probe.</summary>
    public static string SourceTableFor(string view) => view switch
    {
        PerfmonBaselineView => "perfmon_stats",
        WaitStatsBaselineView => "wait_stats",
        SessionStatsBaselineView => "session_stats",
        QueryStatsBaselineView => "query_stats",
        BlockedProcessBaselineView => "blocked_process_reports",
        DeadlockBaselineView => "deadlocks",
        MemoryBaselineView => "memory_stats",
        _ => throw new ArgumentOutOfRangeException(nameof(view), view, "not a baseline aggregate"),
    };

    /// <summary>The seven baseline-tier aggregates in creation order (nine until #2007 retired the unread CPU/IO pair). Named ONCE so the ensure sweep, the
    /// retention list and the tests read one list rather than three hand-kept copies.</summary>
    public static readonly (string CreateSql, string View)[] BaselineAggregates =
    {
        (CreatePerfmonBaselineSql,        PerfmonBaselineView),
        (CreateWaitStatsBaselineSql,      WaitStatsBaselineView),
        (CreateSessionStatsBaselineSql,   SessionStatsBaselineView),
        (CreateQueryStatsBaselineSql,     QueryStatsBaselineView),
        (CreateBlockedProcessBaselineSql, BlockedProcessBaselineView),
        (CreateDeadlockBaselineSql,       DeadlockBaselineView),
        (CreateMemoryBaselineSql,         MemoryBaselineView),
    };

    public const string QueryStatsHourlyView = "query_stats_hourly";

    /// <summary><see cref="QueryStatsHourlyView"/>'s procedure_stats sibling.</summary>
    public const string ProcedureStatsHourlyView = "procedure_stats_hourly";

    /// <summary>The query_store_stats hourly continuous aggregate. Built now, ahead of any writable-Query-Store
    /// primary — on a read-only replica QS surfaces nothing new to harvest, so this sits empty until one is added,
    /// but the rollup path exists the moment data starts flowing. Weaker cardinality reduction than the delta
    /// tables (QS's own top-N sampling already surfaces a broad, shifting query/plan set), still worth having.</summary>
    public const string QueryStoreStatsHourlyView = "query_store_stats_hourly";

    /// <summary>The DAILY tier: hierarchical continuous aggregates sourced from the hourly CAGGs, NOT raw — 2.28.1
    /// supports a continuous aggregate built directly on another. Kept indefinitely (no retention policy) as the
    /// "coarsened but never fully lost" tier for anything past the hourly CAGG's own horizon.</summary>
    public const string QueryStatsDailyView = "query_stats_daily";

    /// <summary>The per-database query_stats rollup carrying the I/O sums FinOps needs (#1661).</summary>
    public const string QueryStatsDbHourlyView = "query_stats_db_hourly";

    /// <summary>The daily sibling of <see cref="QueryStatsDbHourlyView"/> — kept indefinitely.</summary>
    public const string QueryStatsDbDailyView = "query_stats_db_daily";

    /// <summary><see cref="QueryStatsDailyView"/>'s procedure_stats sibling (sourced from procedure_stats_hourly).</summary>
    public const string ProcedureStatsDailyView = "procedure_stats_daily";

    /// <summary>The Query Store DAILY continuous aggregate — hierarchical from <see cref="QueryStoreStatsHourlyView"/>,
    /// same composer dims (module_name / query_hash) + weighted sums. Kept indefinitely; a QS window past the
    /// hourly's horizon routes here.</summary>
    public const string QueryStoreStatsDailyView = "query_store_stats_daily";

    /// <summary>
    /// L1 of the CORRECTED Query Store rollups (#1849): the INTERVAL-grain dedup layer. Query Store rows are
    /// cumulative per-interval snapshots that the collector re-fetches every cycle, so
    /// <see cref="QueryStoreStatsHourlyView"/>'s <c>sum(execution_count)</c> counts one interval's work once per
    /// COLLECTION — measured at up to 496x on a live store, and 243x on this repo's own seeded proof.
    ///
    /// <para>This is not a read target. It exists so <see cref="QueryStoreStatsCorrectedHourlyView"/> and
    /// <see cref="QueryStoreStatsCorrectedDailyView"/> have a source in which each interval appears ONCE.</para>
    /// </summary>
    public const string QueryStoreStatsIntervalHourlyView = "query_store_stats_interval_hourly";

    /// <summary>The CORRECTED composer-grain HOURLY rollup (#1849) — <see cref="QueryStoreStatsHourlyView"/>'s
    /// replacement for windows the corrected tier covers, carrying the IDENTICAL column names so
    /// <c>ComposeCaggValueMapper</c> reads it unchanged.</summary>
    public const string QueryStoreStatsCorrectedHourlyView = "query_store_stats_corrected_hourly";

    /// <summary>The CORRECTED composer-grain DAILY rollup (#1849). A SIBLING of
    /// <see cref="QueryStoreStatsCorrectedHourlyView"/>, not its child — see that view's remarks for the
    /// identity-width leaf constraint that forces the fan-out.
    ///
    /// <para>Superseded at the daily grain by <see cref="QueryStoreStatsDayGrainDailyView"/> (#1869), and kept
    /// for the same reason the original pair is kept: it holds history the newer level starts empty of.</para>
    /// </summary>
    public const string QueryStoreStatsCorrectedDailyView = "query_store_stats_corrected_daily";

    /// <summary>
    /// L2 of the corrected Query Store rollups (#1869): the interval-grain DAILY dedup layer — one row per
    /// INTERVAL IDENTITY per DAY, holding that interval's last snapshot of the day.
    ///
    /// <para>Like <see cref="QueryStoreStatsIntervalHourlyView"/> this is not a read target. It exists so
    /// <see cref="QueryStoreStatsDayGrainDailyView"/> has a source in which each interval appears once PER DAY
    /// rather than once per collection HOUR, which is the whole of the hour-straddle residual #1849 left
    /// behind.</para>
    /// </summary>
    public const string QueryStoreStatsIntervalDailyView = "query_store_stats_interval_daily";

    /// <summary>The composer-grain DAILY rollup deduped at the DAY grain (#1869) —
    /// <see cref="QueryStoreStatsCorrectedDailyView"/>'s replacement for windows it covers, carrying the
    /// IDENTICAL column names so <c>ComposeCaggValueMapper</c> reads it unchanged.
    ///
    /// <para>Named for its dedup GRAIN and deliberately not for exactness: it removes the HOUR-straddle
    /// residual, and an interval whose snapshots straddle MIDNIGHT is still counted once per collection DAY.
    /// See <see cref="CreateQueryStoreStatsDayGrainDailySql"/> for the measured size of what remains.</para></summary>
    public const string QueryStoreStatsDayGrainDailyView = "query_store_stats_daygrain_daily";

    /// <summary>
    /// The query_stats hourly continuous aggregate. 1-hour buckets grouped by the SAME dimensions the composer's
    /// <c>MeasureCatalog</c> uses for query_stats (server_id / server_name / database_name / query_hash), so a
    /// panel can point here with no dimension remapping. SUM/MIN/MAX on each per-interval DELTA column (NOT a
    /// pre-divided average — avg composes at query time as sum/execution_count_sum, which re-aggregates
    /// correctly; a materialized average would not) plus a <c>sample_count</c>. Summing the deltas is
    /// double-count-safe: they are Darling's own per-interval deltas, not raw cumulative DMV counters. Created
    /// WITH NO DATA — a full historical refresh over 145 GB is heavy I/O, a deliberate off-hours operator op
    /// (<c>--backfill-rollups</c>), NEVER startup work. IF NOT EXISTS so a restart re-converges. A SINGLE
    /// statement: a CAGG CREATE cannot run inside a transaction, so it must never be batched with the policy call.
    ///
    /// <para><b>MATERIALIZED-ONLY, and that is deliberate (#1759).</b> This used to claim the opposite — that
    /// real-time aggregation was opted into and the view was "correct to query for any window immediately, just
    /// un-accelerated". Both halves were false. TimescaleDB 2.13+ defaults <c>materialized_only</c> to TRUE, so
    /// naming no option means real-time aggregation is OFF; and even ON it would not help, because the watermark
    /// is a hard partition — <c>build_union_query</c> emits materialized-below <c>UNION ALL</c> raw-at-or-above,
    /// with no contiguity guarantee — so history below the watermark that was never materialized is served by
    /// NEITHER branch. That premise cost this product the #1759 defect: every window older than the rollup's
    /// materialized floor read EMPTY while raw still held the rows.</para>
    ///
    /// <para>Do NOT "fix" that by adding <c>materialized_only = false</c>. It cannot surface un-materialized
    /// history (see above), and it would break the two things that now depend on materialized-only semantics:
    /// <see cref="RollupCoverageProbeSql"/> and <see cref="RetentionArmSafetySql"/> both read
    /// <c>min(bucket)</c> to mean "the oldest bucket this rollup has MATERIALIZED". Union in the raw branch and
    /// an EMPTY materialization would report raw's own oldest row as the rollup's floor — coverage would look
    /// complete when it is not, and the arming gate would arm a purge over history nothing else holds.
    /// TimescaleContinuousAggregateTests pins the absence of the option for exactly this reason.</para>
    /// </summary>
    public const string CreateQueryStatsHourlySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_hourly
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    query_hash,
    sql_handle,
    time_bucket('1 hour', collection_time) AS bucket,
    sum(delta_worker_time) AS worker_time_sum,
    min(delta_worker_time) AS worker_time_min,
    max(delta_worker_time) AS worker_time_max,
    sum(delta_elapsed_time) AS elapsed_time_sum,
    min(delta_elapsed_time) AS elapsed_time_min,
    max(delta_elapsed_time) AS elapsed_time_max,
    sum(delta_execution_count) AS execution_count_sum,
    min(delta_execution_count) AS execution_count_min,
    max(delta_execution_count) AS execution_count_max,
    count(*) AS sample_count
FROM collect.query_stats
GROUP BY server_id, server_name, database_name, query_hash, sql_handle, bucket
WITH NO DATA";

    /// <summary>The procedure_stats hourly continuous aggregate — <see cref="CreateQueryStatsHourlySql"/>'s
    /// sibling, grouped by <c>schema_name</c> + <c>object_name</c> (procedure_stats' composer dimensions; a panel
    /// grouping by schema_name alone re-aggregates over its objects). Same aggregation shape, same WITH NO DATA +
    /// IF NOT EXISTS discipline.</summary>
    public const string CreateProcedureStatsHourlySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.procedure_stats_hourly
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    schema_name,
    object_name,
    time_bucket('1 hour', collection_time) AS bucket,
    sum(delta_worker_time) AS worker_time_sum,
    min(delta_worker_time) AS worker_time_min,
    max(delta_worker_time) AS worker_time_max,
    sum(delta_elapsed_time) AS elapsed_time_sum,
    min(delta_elapsed_time) AS elapsed_time_min,
    max(delta_elapsed_time) AS elapsed_time_max,
    sum(delta_execution_count) AS execution_count_sum,
    min(delta_execution_count) AS execution_count_min,
    max(delta_execution_count) AS execution_count_max,
    count(*) AS sample_count
FROM collect.procedure_stats
GROUP BY server_id, server_name, database_name, schema_name, object_name, bucket
WITH NO DATA";

    /// <summary>
    /// The query_store_stats hourly continuous aggregate, grouped by the COMPOSER's Query Store dimensions
    /// (server / database_name / module_name / query_hash) so a composed QS panel can route here — NOT Query
    /// Store's own query_id/plan_id, which the composer never exposes. Carries the EXECUTION-WEIGHTED sums
    /// (<c>sum(avg_* * execution_count)</c>) so the composer's weighted mean composes EXACTLY as
    /// <c>duration_us_weighted_sum / execution_count_sum</c> across any window (avg*count = the interval's total,
    /// summed = the true total) — never an avg-of-avgs. This matters the moment a writable-Query-Store primary is
    /// added (the scenario this CAGG exists to be ready for); on the current read-only replica it is simply empty.
    /// WITH NO DATA + IF NOT EXISTS, one statement.
    /// </summary>
    public const string CreateQueryStoreStatsHourlySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_hourly
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    time_bucket('1 hour', collection_time) AS bucket,
    sum(execution_count) AS execution_count_sum,
    sum(avg_duration_us::double precision * execution_count) AS duration_us_weighted_sum,
    sum(avg_cpu_time_us::double precision * execution_count) AS cpu_us_weighted_sum,
    max(max_duration_us) AS max_duration_us_max,
    max(max_cpu_time_us) AS max_cpu_time_us_max,
    count(*) AS sample_count
FROM collect.query_store_stats
GROUP BY server_id, server_name, database_name, module_name, query_hash, bucket
WITH NO DATA";

    /// <summary>
    /// The per-DATABASE query_stats rollup (#1661). Added rather than folded into
    /// <see cref="CreateQueryStatsHourlySql"/> deliberately: TimescaleDB cannot ALTER columns into a continuous
    /// aggregate, so widening that one would mean DROP + recreate, and now that retention is active the rebuild
    /// would re-materialize from 4 days of raw and permanently destroy the retained hourly and indefinite daily
    /// history the tiers exist to preserve. A NEW aggregate costs nothing existing; its history simply starts
    /// accumulating from deploy.
    ///
    /// <para>Carries the I/O sums no other rollup has — FinOps' database-grain workload view sums
    /// <c>delta_logical_reads</c> / <c>delta_physical_reads</c> / <c>delta_logical_writes</c>, and the composer's
    /// measure set (which the other CAGGs were built to) never exposed I/O. Grouped by database_name only, NOT
    /// query_hash, so it is far smaller than the query-grain aggregate despite carrying more columns.</para>
    /// </summary>
    public const string CreateQueryStatsDbHourlySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_db_hourly
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    time_bucket('1 hour', collection_time) AS bucket,
    sum(delta_worker_time) AS worker_time_sum,
    sum(delta_logical_reads) AS logical_reads_sum,
    sum(delta_physical_reads) AS physical_reads_sum,
    sum(delta_logical_writes) AS logical_writes_sum,
    sum(delta_execution_count) AS execution_count_sum,
    max(last_execution_time) AS last_execution_time_max,
    count(*) AS sample_count
FROM collect.query_stats
WHERE delta_worker_time IS NOT NULL
GROUP BY server_id, server_name, database_name, bucket
WITH NO DATA";

    /// <summary>The DAILY sibling of <see cref="CreateQueryStatsDbHourlySql"/> — hierarchical (sourced from the
    /// hourly one, not raw), kept indefinitely like the other daily rollups.</summary>
    public const string CreateQueryStatsDbDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_db_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    time_bucket('1 day', bucket) AS bucket,
    sum(worker_time_sum) AS worker_time_sum,
    sum(logical_reads_sum) AS logical_reads_sum,
    sum(physical_reads_sum) AS physical_reads_sum,
    sum(logical_writes_sum) AS logical_writes_sum,
    sum(execution_count_sum) AS execution_count_sum,
    max(last_execution_time_max) AS last_execution_time_max,
    sum(sample_count) AS sample_count
FROM collect.query_stats_db_hourly
GROUP BY server_id, server_name, database_name, time_bucket('1 day', bucket)
WITH NO DATA";

    /// <summary>
    /// The query_stats DAILY continuous aggregate — a HIERARCHICAL CAGG sourced from <see cref="QueryStatsHourlyView"/>
    /// (not raw). Re-aggregates the hourly rollup to 1-day buckets: SUM of the hourly sums, MIN of the hourly mins,
    /// MAX of the hourly maxes (each composes correctly across the coarser bucket), plus SUM of the hourly
    /// sample_counts. The GROUP BY uses the explicit <c>time_bucket('1 day', bucket)</c> expression, NOT the bare
    /// <c>bucket</c> alias: an unqualified <c>bucket</c> in GROUP BY binds to the SOURCE column (the hourly bucket)
    /// under Postgres's input-column-wins ambiguity rule, which would group by hour, not day. WITH NO DATA +
    /// IF NOT EXISTS; the hourly CAGG must already exist (it is created earlier in the same sweep).
    /// </summary>
    public const string CreateQueryStatsDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_stats_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    query_hash,
    sql_handle,
    time_bucket('1 day', bucket) AS bucket,
    sum(worker_time_sum) AS worker_time_sum,
    min(worker_time_min) AS worker_time_min,
    max(worker_time_max) AS worker_time_max,
    sum(elapsed_time_sum) AS elapsed_time_sum,
    min(elapsed_time_min) AS elapsed_time_min,
    max(elapsed_time_max) AS elapsed_time_max,
    sum(execution_count_sum) AS execution_count_sum,
    min(execution_count_min) AS execution_count_min,
    max(execution_count_max) AS execution_count_max,
    sum(sample_count) AS sample_count
FROM collect.query_stats_hourly
GROUP BY server_id, server_name, database_name, query_hash, sql_handle, time_bucket('1 day', bucket)
WITH NO DATA";

    /// <summary>The procedure_stats DAILY continuous aggregate — <see cref="CreateQueryStatsDailySql"/>'s sibling,
    /// sourced from <see cref="ProcedureStatsHourlyView"/> and grouped by <c>schema_name</c> + <c>object_name</c>.
    /// Same hierarchical re-aggregation and same explicit-<c>time_bucket</c> GROUP BY discipline.</summary>
    public const string CreateProcedureStatsDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.procedure_stats_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    schema_name,
    object_name,
    time_bucket('1 day', bucket) AS bucket,
    sum(worker_time_sum) AS worker_time_sum,
    min(worker_time_min) AS worker_time_min,
    max(worker_time_max) AS worker_time_max,
    sum(elapsed_time_sum) AS elapsed_time_sum,
    min(elapsed_time_min) AS elapsed_time_min,
    max(elapsed_time_max) AS elapsed_time_max,
    sum(execution_count_sum) AS execution_count_sum,
    min(execution_count_min) AS execution_count_min,
    max(execution_count_max) AS execution_count_max,
    sum(sample_count) AS sample_count
FROM collect.procedure_stats_hourly
GROUP BY server_id, server_name, database_name, schema_name, object_name, time_bucket('1 day', bucket)
WITH NO DATA";

    /// <summary>The Query Store DAILY continuous aggregate — <see cref="CreateQueryStatsDailySql"/>'s Query Store
    /// sibling, hierarchical from <see cref="QueryStoreStatsHourlyView"/> and grouped by the composer's QS dims
    /// (module_name / query_hash). SUM re-aggregates the hourly weighted sums (so the weighted mean composes as
    /// duration_us_weighted_sum / execution_count_sum across days) and MAX the peaks. Same column NAMES as the
    /// hourly, so <c>ComposeCaggValueMapper</c> reads both with no change. Explicit-<c>time_bucket</c> GROUP BY.</summary>
    public const string CreateQueryStoreStatsDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    time_bucket('1 day', bucket) AS bucket,
    sum(execution_count_sum) AS execution_count_sum,
    sum(duration_us_weighted_sum) AS duration_us_weighted_sum,
    sum(cpu_us_weighted_sum) AS cpu_us_weighted_sum,
    max(max_duration_us_max) AS max_duration_us_max,
    max(max_cpu_time_us_max) AS max_cpu_time_us_max,
    sum(sample_count) AS sample_count
FROM collect.query_store_stats_hourly
GROUP BY server_id, server_name, database_name, module_name, query_hash, time_bucket('1 day', bucket)
WITH NO DATA";

    /* ═══════════ the CORRECTED Query Store rollups (#1849) ═══════════

       WHY THREE NEW OBJECTS INSTEAD OF FIXING THE TWO ABOVE. A continuous aggregate's columns cannot be
       ALTERed, so reshaping means DROP + recreate — and with retention active the rebuild re-materializes
       from 4 days of raw and PERMANENTLY DESTROYS the retained hourly and indefinite daily history (the same
       reason CreateQueryStatsDbHourlySql was added rather than folded in). Per #1759/#1793 materialized
       history is never destroyed, so the corrected rollups are NEW objects alongside; the old pair keeps its
       identity, data, retention and jobs, and still answers windows the corrected tier has not reached.

       THE SHAPE IS FORCED BY WHAT TIMESCALEDB ACCEPTS, all five results live-probed on PG 18.4 /
       TimescaleDB 2.28.1 (#1849 carries the tier-2 probes; the ones below were re-probed here):

         - A CAGG on query_store_stats CANNOT bucket on interval_start_time_utc:
           "time bucket function must reference the primary hypertable dimension column". So dedup cannot
           happen at the interval's own clock — it must bucket on collection_time and dedup at the interval
           GRAIN, which is what makes L1 a separate level rather than a WHERE clause.
         - Window functions inside a CAGG are rejected and the hint names an EXPERIMENTAL GUC
           (timescaledb.enable_cagg_window_functions). Shipping correctness on a server-side experimental
           toggle an operator's own PostgreSQL may not have set is not a trade worth making; last() needs no
           flag.
         - AN IDENTITY-WIDTH HIERARCHICAL CAGG IS A LEAF. This is the constraint that shapes the daily, and
           it is not in #1849 because it was found here: a child whose bucket equals its parent's width
           (CorrectedHourly is time_bucket('1 hour', bucket) over L1's 1-hour bucket) CREATES fine and
           refreshes fine, but nothing can be built ON it — a further CAGG fails with the same
           primary-dimension error. Verified it is the identity width and NOT the depth: a plain
           1h -> 1d -> 7d three-level chain is ACCEPTED. So CorrectedDaily is a SIBLING of CorrectedHourly
           sourced from L1 at a 1-day bucket, never its child. That is also the better shape: each corrected
           rollup is one hop from the deduped L1, so the daily does not compound the hourly's straddle
           residual.

       THE RESIDUAL, STATED PLAINLY. An interval whose snapshots straddle an hour boundary produces two L1
       rows, each holding a CUMULATIVE value, so the composer-grain sum counts it once per collection HOUR
       (~2) instead of once per COLLECTION (up to 496). That is a ~250x correction, not exactness. At the
       hourly grain the residual is irreducible — an interval genuinely collected in two hours has to appear
       in both, and CreateQueryStoreStatsCorrectedHourlySql still carries it.

       AT THE DAILY GRAIN IT IS REMOVED, by #1869: an interval-grain re-dedup level (L2,
       CreateQueryStoreStatsIntervalDailySql) sits between L1 and a second composer-grain collapse
       (CreateQueryStoreStatsDayGrainDailySql), so the interval is deduped across the WHOLE DAY before it is
       summed. That makes the stack three levels deep, which is legal only because L2 WIDENS 1h -> 1d: the
       leaf rule above forbids building on an identity-width child, not on a depth. The old corrected daily
       stays exactly where it is, for the same reason the original pair does — it holds history the day-grain
       level starts empty of, and reads prefer whichever actually covers the window. */

    /// <summary>
    /// L1 of the corrected Query Store rollups (#1849): one row per INTERVAL IDENTITY per collection hour,
    /// projecting each interval's LAST snapshot. This is the level that removes the double-count.
    ///
    /// <para><b>Why <c>last(x, collection_time)</c>.</b> Query Store's runtime-stats columns are cumulative
    /// WITHIN an interval, so an interval's true contribution is its final snapshot, not the sum of the
    /// snapshots. <c>last()</c> is an ordered aggregate TimescaleDB accepts inside a continuous aggregate with
    /// no flag; the <c>row_number()</c> formulation a reader might reach for first is rejected outright.</para>
    ///
    /// <para><b>Both interval keys are in the GROUP BY, deliberately not COALESCEd (#1853's argument, applied
    /// here).</b> <c>runtime_stats_interval_id</c> is the real identity but is NULL on exactly the pre-V41
    /// generation of rows, which nothing can backfill; <c>first_execution_time</c> is tier 1's proxy and is
    /// present on both. Grouping by BOTH means a post-V41 row is keyed by its real id (the proxy rides along,
    /// functionally dependent on it — Query Store fixes first_execution_time when the interval's row is
    /// created and never moves it, so it adds no groups), while a legacy row keys on the proxy alone. The two
    /// generations can never collide, so LEGACY ROWS ARE INCLUDED RATHER THAN EXCLUDED and degrade to
    /// precisely tier 1's key. Excluding them would have been the easier claim to make true, and it would
    /// silently drop every pre-upgrade hour out of the corrected rollup while the store still held the rows.
    /// A COALESCE into one key is expressible in a CAGG GROUP BY (probed: accepted) and is still the wrong
    /// choice — it fuses two identity domains into one text column and loses the ability to tell which
    /// generation a group came from.</para>
    ///
    /// <para><b>Capacity.</b> This keys on query_id/plan_id/interval, so its cardinality is near-raw — the
    /// reduction is the collection multiplicity, NOT the dimensional collapse the composer-grain rollups get.
    /// It is therefore the one rollup here whose retention is deliberately SHORT
    /// (<see cref="IntervalRetentionInterval"/>): nothing reads it, so it only has to outlive raw for the
    /// arming gate and outlive its consumers' 3-day refresh window.</para>
    /// </summary>
    public const string CreateQueryStoreStatsIntervalHourlySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_interval_hourly
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    query_id,
    plan_id,
    execution_type_desc,
    replica_role,
    runtime_stats_interval_id,
    first_execution_time,
    time_bucket('1 hour', collection_time) AS bucket,
    last(execution_count, collection_time) AS execution_count,
    last(avg_duration_us, collection_time) AS avg_duration_us,
    last(avg_cpu_time_us, collection_time) AS avg_cpu_time_us,
    last(interval_start_time_utc, collection_time) AS interval_start_time_utc,
    max(max_duration_us) AS max_duration_us,
    max(max_cpu_time_us) AS max_cpu_time_us,
    count(*) AS sample_count
FROM collect.query_store_stats
GROUP BY server_id, server_name, database_name, module_name, query_hash, query_id, plan_id,
         execution_type_desc, replica_role, runtime_stats_interval_id, first_execution_time,
         time_bucket('1 hour', collection_time)
WITH NO DATA";

    /// <summary>
    /// The corrected composer-grain HOURLY rollup (#1849): <see cref="CreateQueryStoreStatsHourlySql"/>'s
    /// column set to the byte, computed from the DEDUPED L1 instead of from raw. Same names so
    /// <c>ComposeCaggValueMapper</c> and every composed panel read it with no change — the correction is
    /// invisible to the read layer, which is the point.
    ///
    /// <para>The weighted sums are rebuilt from L1's per-interval <c>last()</c> values
    /// (<c>avg_* * execution_count</c> = that interval's total), so the composer's weighted mean still composes
    /// EXACTLY as <c>duration_us_weighted_sum / execution_count_sum</c> and is never an avg-of-avgs.
    /// <c>sample_count</c> deliberately carries L1's <c>sum(sample_count)</c> — the number of RAW SNAPSHOTS
    /// behind the bucket, matching what the old view's <c>count(*)</c> meant — so the two are comparable
    /// while both exist.</para>
    ///
    /// <para><b>This is an identity-width hierarchical CAGG and therefore a LEAF</b> (see the block comment
    /// above): its bucket equals L1's, so nothing can be built on top of it. That is why
    /// <see cref="CreateQueryStoreStatsCorrectedDailySql"/> reads L1 rather than this view.</para>
    /// </summary>
    public const string CreateQueryStoreStatsCorrectedHourlySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_corrected_hourly
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    time_bucket('1 hour', bucket) AS bucket,
    sum(execution_count) AS execution_count_sum,
    sum(avg_duration_us::double precision * execution_count) AS duration_us_weighted_sum,
    sum(avg_cpu_time_us::double precision * execution_count) AS cpu_us_weighted_sum,
    max(max_duration_us) AS max_duration_us_max,
    max(max_cpu_time_us) AS max_cpu_time_us_max,
    sum(sample_count) AS sample_count
FROM collect.query_store_stats_interval_hourly
GROUP BY server_id, server_name, database_name, module_name, query_hash, time_bucket('1 hour', bucket)
WITH NO DATA";

    /// <summary>
    /// The corrected composer-grain DAILY rollup (#1849) — the same columns as
    /// <see cref="CreateQueryStoreStatsCorrectedHourlySql"/> at a 1-day bucket, sourced from L1 DIRECTLY.
    ///
    /// <para><b>A sibling of the corrected hourly, not its child</b>, because an identity-width hierarchical
    /// CAGG is a leaf (see the block comment above) — a daily built on the corrected hourly is rejected. Every
    /// other daily in this file IS built on its hourly, so this asymmetry is deliberate and load-bearing, not
    /// an oversight to be "made consistent" later. Reading L1 also keeps the daily one hop from the dedup, so
    /// it does not inherit the hourly's straddle residual on top of its own.</para>
    ///
    /// <para>Kept indefinitely (no retention policy), like the other daily rollups.</para>
    /// </summary>
    public const string CreateQueryStoreStatsCorrectedDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_corrected_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    time_bucket('1 day', bucket) AS bucket,
    sum(execution_count) AS execution_count_sum,
    sum(avg_duration_us::double precision * execution_count) AS duration_us_weighted_sum,
    sum(avg_cpu_time_us::double precision * execution_count) AS cpu_us_weighted_sum,
    max(max_duration_us) AS max_duration_us_max,
    max(max_cpu_time_us) AS max_cpu_time_us_max,
    sum(sample_count) AS sample_count
FROM collect.query_store_stats_interval_hourly
GROUP BY server_id, server_name, database_name, module_name, query_hash, time_bucket('1 day', bucket)
WITH NO DATA";

    /// <summary>
    /// L2 of the corrected Query Store rollups (#1869): L1 re-deduped at the DAY grain — one row per interval
    /// identity per DAY, projecting that interval's LAST snapshot of the day.
    ///
    /// <para><b>What this removes.</b> L1 keys on the collection HOUR, so an interval collected across an hour
    /// boundary leaves TWO rows, each holding a cumulative value, and
    /// <see cref="CreateQueryStoreStatsCorrectedDailySql"/>'s <c>sum</c> counts it once per hour it was
    /// collected in rather than once. Bounded by 2x and measured at <b>1.97x</b> on this repo's own seeded
    /// proof (1,013 against an exact 515). Taking <c>last(execution_count, bucket)</c> over the day collapses
    /// those rows back to one before the collapse to composer dims.</para>
    ///
    /// <para><b>Legal only because it WIDENS.</b> A hierarchical CAGG whose bucket equals its parent's is a
    /// leaf (see the block comment above), so this level could not exist at 1 hour — it is 1 DAY over L1's
    /// 1 hour, and the <c>1h -> 1d -> 1d</c> chain it creates was live-probed ACCEPTED on PostgreSQL 18.4 /
    /// TimescaleDB 2.28.1 together with its refresh and retention policies.</para>
    ///
    /// <para><b>What it does NOT remove, stated because the whole point of #1869 is that a permanent
    /// mis-count is a permanent lie.</b> An interval whose snapshots straddle MIDNIGHT still produces two
    /// rows here, one per day, and is still counted twice across them — the identical argument one grain up,
    /// and equally irreducible at the daily grain. It is a far smaller residual than the hourly one and the
    /// difference is structural, not a guess: <c>QueryStoreCollector</c> fetches an interval while its
    /// <c>last_execution_time</c> keeps advancing, so a 60-minute interval is collected over roughly one hour
    /// of wall clock and crosses an hour boundary almost ALWAYS but a day boundary about once per 24
    /// intervals — a ~4% over-count against the 97% removed. Measured, pinned by a live test, and filed with
    /// the cost of a fifth near-raw-cardinality level as #1879 rather than left as a comment.</para>
    ///
    /// <para><b>Capacity.</b> Keyed on interval identity, so near-raw cardinality like L1 — but at a day
    /// bucket rather than an hour bucket, which makes it the SMALLER of the two: an interval spans ~2 hourly
    /// buckets and 1 daily one. It therefore takes a short horizon too
    /// (<see cref="IntervalDailyRetentionInterval"/>).</para>
    /// </summary>
    public const string CreateQueryStoreStatsIntervalDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_interval_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    query_id,
    plan_id,
    execution_type_desc,
    replica_role,
    runtime_stats_interval_id,
    first_execution_time,
    time_bucket('1 day', bucket) AS bucket,
    last(execution_count, bucket) AS execution_count,
    last(avg_duration_us, bucket) AS avg_duration_us,
    last(avg_cpu_time_us, bucket) AS avg_cpu_time_us,
    last(interval_start_time_utc, bucket) AS interval_start_time_utc,
    max(max_duration_us) AS max_duration_us,
    max(max_cpu_time_us) AS max_cpu_time_us,
    sum(sample_count) AS sample_count
FROM collect.query_store_stats_interval_hourly
GROUP BY server_id, server_name, database_name, module_name, query_hash, query_id, plan_id,
         execution_type_desc, replica_role, runtime_stats_interval_id, first_execution_time,
         time_bucket('1 day', bucket)
WITH NO DATA";

    /// <summary>
    /// The composer-grain DAILY rollup computed from the DAY-grain dedup (#1869) —
    /// <see cref="CreateQueryStoreStatsCorrectedDailySql"/>'s column set to the byte, sourced from L2 instead
    /// of L1 so an hour-straddling interval is counted ONCE.
    ///
    /// <para>Same column names again, so <c>ComposeCaggValueMapper</c> and every composed panel read it with
    /// no change — only which relation the router names differs. <c>sample_count</c> still carries the number
    /// of RAW SNAPSHOTS behind the bucket (L2 sums L1's, this sums L2's), so it stays comparable with both
    /// dailies it sits beside.</para>
    ///
    /// <para>This is an identity-width hierarchical CAGG (1 day over L2's 1 day) and therefore a LEAF —
    /// nothing can be built on it. Nothing needs to be: it is the end of the chain, which is exactly why the
    /// identity width is spendable HERE and was not at L2.</para>
    ///
    /// <para>Kept indefinitely (no retention policy), like every other daily. That is also why #1869 was
    /// worth its cost: the daily tier is the one whose numbers persist and get compared year over year.</para>
    /// </summary>
    public const string CreateQueryStoreStatsDayGrainDailySql = @"CREATE MATERIALIZED VIEW IF NOT EXISTS collect.query_store_stats_daygrain_daily
WITH (timescaledb.continuous) AS
SELECT
    server_id,
    server_name,
    database_name,
    module_name,
    query_hash,
    time_bucket('1 day', bucket) AS bucket,
    sum(execution_count) AS execution_count_sum,
    sum(avg_duration_us::double precision * execution_count) AS duration_us_weighted_sum,
    sum(avg_cpu_time_us::double precision * execution_count) AS cpu_us_weighted_sum,
    max(max_duration_us) AS max_duration_us_max,
    max(max_cpu_time_us) AS max_cpu_time_us_max,
    sum(sample_count) AS sample_count
FROM collect.query_store_stats_interval_daily
GROUP BY server_id, server_name, database_name, module_name, query_hash, time_bucket('1 day', bucket)
WITH NO DATA";

    /// <summary>
    /// The refresh policy for a continuous aggregate: materialize <c>[now - 3 days, now - endOffset]</c> every
    /// <c>scheduleInterval</c>. <c>start_offset 3 days</c> gives margin past the ~2-day compression/hot window
    /// (covers same-day-arriving corrections) and is the buffer the retention tiers lean on — a tier's drop must
    /// never outrun the next tier's 3-day refresh start. <c>endOffset</c> leaves the still-filling current bucket
    /// unmaterialized (no repeated rework); <c>scheduleInterval</c> matches the bucket. Defaults are the hourly
    /// shape; the daily CAGGs pass <c>"1 day"</c>/<c>"1 day"</c>. <c>if_not_exists</c> so a restart re-converges.
    /// </summary>
    public static string AddContinuousAggregatePolicySql(string view, string endOffset = "1 hour", string scheduleInterval = "1 hour")
        => $"SELECT add_continuous_aggregate_policy('collect.{view}', start_offset => INTERVAL '3 days', end_offset => INTERVAL '{endOffset}', schedule_interval => INTERVAL '{scheduleInterval}', if_not_exists => true)";

    /// <summary>
    /// The composer-dimension reshape: the QS hourly CAGG regrouped query_id/plan_id → module_name/query_hash
    /// (+ weighted sums), and the procedure_stats CAGGs gained schema_name. <c>CREATE ... IF NOT EXISTS</c> cannot
    /// ALTER an existing CAGG, so a store that already built the OLD shape must DROP it first;
    /// <see cref="EnsureContinuousAggregatesAsync"/> (run right after) recreates it in the new shape. Each affected
    /// CAGG is empty (QS on a read-only replica) or only a day or two old, so the drop loses little and the refresh
    /// backfills the recent window within the hour. Staleness is detected STRUCTURALLY — the OLD QS CAGG still has
    /// a <c>query_id</c> column; the OLD procedure_stats CAGG lacks <c>schema_name</c> — so this is a strict no-op
    /// once reshaped, and on a fresh store (no CAGG yet) nothing matches. Failure-isolated: a failed drop leaves the
    /// old shape in place (logged), never kills startup. query_stats CAGGs are unchanged and untouched. CASCADE
    /// drops the dependent daily CAGG, which the ensure sweep also recreates.
    /// </summary>
    public static async Task<int> DropStaleContinuousAggregatesAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var reshapes = new[]
        {
            /* OLD query_store_stats_hourly grouped by query_id/plan_id → stale iff it still has a query_id column. */
            (View: "query_store_stats_hourly",
             StaleCheck: "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_store_stats_hourly' AND column_name = 'query_id')"),
            /* OLD procedure_stats_hourly lacked schema_name → stale iff the view EXISTS but has no schema_name
               column. CASCADE also drops procedure_stats_daily, which the ensure sweep recreates. */
            (View: "procedure_stats_hourly",
             StaleCheck: "SELECT (EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'procedure_stats_hourly') AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'procedure_stats_hourly' AND column_name = 'schema_name'))"),
            /* query_stats_hourly / _daily gained sql_handle (object_name routing) → stale iff the view EXISTS but
               has no sql_handle column. CASCADE drops query_stats_daily, which the ensure sweep recreates. */
            (View: "query_stats_hourly",
             StaleCheck: "SELECT (EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_stats_hourly') AND NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = 'collect' AND table_name = 'query_stats_hourly' AND column_name = 'sql_handle'))"),
        };

        var dropped = 0;
        foreach (var (view, staleCheck) in reshapes)
        {
            try
            {
                bool stale;
                using (var check = new NpgsqlCommand(staleCheck, connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    stale = await check.ExecuteScalarAsync(cancellationToken) is true;
                }

                if (!stale)
                {
                    continue;
                }

                using (var drop = new NpgsqlCommand($"DROP MATERIALIZED VIEW IF EXISTS collect.{view} CASCADE", connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await drop.ExecuteNonQueryAsync(cancellationToken);
                }

                dropped++;
                logger?.LogInformation(
                    "TimescaleDB: dropped stale continuous aggregate {View} (composer-dimension reshape) — recreated in the new shape this cycle.",
                    view);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Reshape drop of {View} failed — it stays in the OLD shape until the next restart retries: {Message}",
                    view, ex.Message);
            }
        }

        return dropped;
    }

    /// <summary>
    /// Creates the continuous aggregates and attaches each one's refresh policy
    /// (<see cref="AddContinuousAggregatePolicySql"/>): three HOURLY (query_stats, procedure_stats,
    /// query_store_stats) then two DAILY (query_stats, procedure_stats). The daily tier is HIERARCHICAL — each
    /// daily CAGG is sourced from its hourly CAGG, so the ordered sweep creates the hourly ones first. Runs in the
    /// worker's TimescaleDB block (CAGGs need the extension), AFTER hypertables + compression are in place. The
    /// CREATE and the policy are SEPARATE commands
    /// per aggregate — a CAGG CREATE cannot run inside a transaction, so it is never batched with another
    /// statement. Failure-isolated per aggregate: one failure warns and the composer keeps querying raw.
    /// Idempotent (IF NOT EXISTS on both), so it re-converges every restart. Returns the number ready.
    ///
    /// <para>Does NOT backfill history, and on a store that already holds history that leaves a real gap rather
    /// than a merely un-accelerated one (#1759): the aggregates are born WITH NO DATA and each refresh policy
    /// starts 3 days back, so the materialized span begins at roughly creation-minus-3-days and never reaches
    /// further back on its own. Reads stay CORRECT because <see cref="RetentionTierRouter"/> routes windows
    /// below a rollup's measured floor to raw; the materialization itself is an operator op
    /// (<c>--backfill-rollups</c>), which is where the disk cost is preflighted rather than incurred at
    /// startup.</para>
    /// </summary>
    public static async Task<int> EnsureContinuousAggregatesAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        // Hourly CAGGs FIRST (the two delta tables + query_store_stats), THEN the daily tier — the daily CAGGs are
        // hierarchical (sourced from the hourly CAGGs), so the hourly ones must be created earlier in this ordered
        // sweep. Daily policies use the 1-day end-offset/schedule; the hourly ones take the helper's defaults.
        var aggregates = new[]
        {
            (CreateSql: CreateQueryStatsHourlySql,      View: QueryStatsHourlyView,      PolicySql: AddContinuousAggregatePolicySql(QueryStatsHourlyView)),
            (CreateSql: CreateProcedureStatsHourlySql,  View: ProcedureStatsHourlyView,  PolicySql: AddContinuousAggregatePolicySql(ProcedureStatsHourlyView)),
            (CreateSql: CreateQueryStoreStatsHourlySql, View: QueryStoreStatsHourlyView, PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsHourlyView)),
            (CreateSql: CreateQueryStatsDbHourlySql,    View: QueryStatsDbHourlyView,    PolicySql: AddContinuousAggregatePolicySql(QueryStatsDbHourlyView)),
            /* The corrected Query Store rollups (#1849). L1 is raw-sourced and MUST precede both corrected
               views, which are hierarchical from it — the same ordering requirement the daily tier has. Both
               corrected views read L1 (the daily is its SIBLING, not the hourly's child: an identity-width
               hierarchical CAGG is a leaf — see CreateQueryStoreStatsCorrectedDailySql). */
            (CreateSql: CreateQueryStoreStatsIntervalHourlySql,  View: QueryStoreStatsIntervalHourlyView,  PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsIntervalHourlyView)),
            (CreateSql: CreateQueryStoreStatsCorrectedHourlySql, View: QueryStoreStatsCorrectedHourlyView, PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsCorrectedHourlyView)),
            (CreateSql: CreateQueryStatsDailySql,       View: QueryStatsDailyView,       PolicySql: AddContinuousAggregatePolicySql(QueryStatsDailyView, "1 day", "1 day")),
            (CreateSql: CreateProcedureStatsDailySql,   View: ProcedureStatsDailyView,   PolicySql: AddContinuousAggregatePolicySql(ProcedureStatsDailyView, "1 day", "1 day")),
            (CreateSql: CreateQueryStoreStatsDailySql,  View: QueryStoreStatsDailyView,  PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsDailyView, "1 day", "1 day")),
            (CreateSql: CreateQueryStoreStatsCorrectedDailySql, View: QueryStoreStatsCorrectedDailyView, PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsCorrectedDailyView, "1 day", "1 day")),
            (CreateSql: CreateQueryStatsDbDailySql,     View: QueryStatsDbDailyView,     PolicySql: AddContinuousAggregatePolicySql(QueryStatsDbDailyView, "1 day", "1 day")),
            /* The DAY-grain corrected daily (#1869), THREE levels deep: L1 (above) -> L2 interval_daily ->
               daygrain_daily. Both must follow L1 and L2 must precede its own child, which this ordered sweep
               gives — the same requirement the daily tier has, one level longer. */
            (CreateSql: CreateQueryStoreStatsIntervalDailySql, View: QueryStoreStatsIntervalDailyView, PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsIntervalDailyView, "1 day", "1 day")),
            (CreateSql: CreateQueryStoreStatsDayGrainDailySql, View: QueryStoreStatsDayGrainDailyView, PolicySql: AddContinuousAggregatePolicySql(QueryStoreStatsDayGrainDailyView, "1 day", "1 day")),
        }
        /* The seven baseline-tier aggregates (#1757; nine until #2007) take the helper's hourly defaults: they are sourced from
           raw like the hourly tier, not hierarchically from another CAGG, so they carry no ordering
           requirement against the daily tier. Appended from the single BaselineAggregates list so this sweep
           and the retention list cannot drift apart. */
        .Concat(BaselineAggregates.Select(a => (CreateSql: a.CreateSql, View: a.View, PolicySql: AddContinuousAggregatePolicySql(a.View))))
        .ToArray();

        /* A store that ran WITHOUT TimescaleDB and has now gained it is carrying the plain fallback views
           under the exact names the baseline aggregates need (#1757). CREATE MATERIALIZED VIEW IF NOT EXISTS
           would quietly do nothing against them, leaving the store permanently on raw scans, so the stale
           fallback is dropped first. Guarded to never touch a real continuous aggregate, and isolated per
           view — this is a transition path, and failing it must not stop the sweep. */
        foreach (var (_, view) in BaselineAggregates)
        {
            try
            {
                using var drop = new NpgsqlCommand(DropBaselineFallbackViewSql(view), connection) { CommandTimeout = SetupTimeoutSeconds };
                await drop.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Could not drop the plain-PostgreSQL fallback view {View} — its continuous aggregate cannot be created while it stands: {Message}",
                    view, ex.Message);
            }
        }

        var ready = 0;
        foreach (var (createSql, view, policySql) in aggregates)
        {
            try
            {
                using (var create = new NpgsqlCommand(createSql, connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await create.ExecuteNonQueryAsync(cancellationToken);
                }

                using (var policy = new NpgsqlCommand(policySql, connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await policy.ExecuteNonQueryAsync(cancellationToken);
                }

                ready++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Continuous aggregate {View} setup failed — composer queries fall back to raw scans: {Message}",
                    view, ex.Message);
            }
        }

        /* The names are DERIVED from the array above, never restated (#1746). The previous line spelled out
           "3 hourly ... 3 daily" by hand, so when #1664 added the db-grain pair the counts said 8 and the
           text still said 6 — and an operator mid-upgrade spent real time reconciling the two before
           concluding it was not a bug. A summary that quotes its own source cannot drift from it. */
        logger?.LogInformation(
            "TimescaleDB: {Ready}/{Total} continuous aggregate(s) ready ({Views})",
            ready, aggregates.Length, string.Join(", ", aggregates.Select(a => a.View)));
        return ready;
    }

    /// <summary>Raw-tier retention horizon: keep per-sweep raw ~4 days — one day past the hourly CAGG's own 3-day
    /// refresh window, so the raw drop never outruns the aggregate that preserves it.</summary>
    public const string RawRetentionInterval = "4 days";

    /// <summary>Hourly-CAGG-tier retention horizon: keep the hourly rollups 90 days — well past the daily CAGG's
    /// 3-day refresh window, so the hourly drop never outruns the daily aggregate. The daily HISTORY CAGGs get
    /// NO retention policy: they are the coarsened, kept-indefinitely tier. (The interval-grain daily is not one
    /// of them — it is dedup plumbing and carries <see cref="IntervalDailyRetentionInterval"/>, which is why the
    /// summary line qualifies this rather than claiming it of every daily, #1958.)
    ///
    /// <para><b>90, not 21 (#1937).</b> The viewer offers month-plus windows and the reason for the number is
    /// entirely about what those windows can RENDER: at 21 days a 30-day view finds three weeks of hourly data
    /// and nothing before it, so the rest of the range either empties out or drops to daily grain partway
    /// through. That is structural rather than a lag — no amount of waiting fixes it — and 90 covers
    /// quarter-scale windows with room rather than exactly. Deliberately NOT a Lite-parity argument: Lite's
    /// query family defaults to 30 days of raw and its long tier is the parquet archive at full grain, so
    /// "match Lite" would be the wrong reason written down.</para>
    ///
    /// <para><b>Changing this number is not enough on its own</b>, in two ways that have both drawn blood.
    /// Reads route on <see cref="RetentionTierRouter.HourlyMaxAge"/>, which is derived from the twin below
    /// precisely so this cannot be raised without the router following. And stores that already have a policy
    /// keep their old horizon unless the sweep converges it — see
    /// <see cref="ConvergeRetentionHorizonSql"/>.</para></summary>
    public const string HourlyRetentionInterval = "90 days";

    /// <summary>
    /// Retention horizon for the INTERVAL-grain dedup layer (#1849): keep
    /// <see cref="QueryStoreStatsIntervalHourlyView"/> 7 days.
    ///
    /// <para>Shorter than every other CAGG tier ON PURPOSE, and the number is picked by two constraints, not
    /// by taste. It must EXCEED <see cref="RawRetentionInterval"/> (4 days) with margin, because the raw purge
    /// is gated on this view covering raw's oldest row — equal horizons would race, with raw's newest-dropped
    /// chunk and L1's oldest-kept bucket at the same age. And it only has to exceed it: nothing READS this
    /// view, and its consumers (the two corrected rollups) refresh over a 3-day window.</para>
    ///
    /// <para><b>MEASURED, because #1849 raises capacity as a real input and #1581 says settle it before
    /// shipping.</b> On a seeded 600-query store at the default 5-minute <c>query_store</c> cadence
    /// (CollectorSchedulePresets), 24 hours of collection produced: raw 40 MB / 187,200 rows; this view
    /// 11 MB / 28,800 rows (28% of raw — it keys on query_id/plan_id/interval, so the reduction is the
    /// collection multiplicity, NOT the dimensional collapse); the composer-grain rollups 4.4 MB / 15,000 rows
    /// each. Projected to the horizons: raw at 4 days is 160 MB, this view at 7 days is 79 MB — against
    /// <b>238 MB</b> had it simply inherited the 21-day hourly horizon, which would have made the store's
    /// intermediate dedup layer larger than its entire raw tier.</para>
    /// </summary>
    public const string IntervalRetentionInterval = "7 days";

    /// <summary>
    /// Retention horizon for the interval-grain DAILY dedup layer (#1869): keep
    /// <see cref="QueryStoreStatsIntervalDailyView"/> 10 days.
    ///
    /// <para>Picked by the same rule that picked <see cref="IntervalRetentionInterval"/>, one tier up, and it
    /// is the OPPOSITE direction from the one instinct suggests. This layer is downstream of L1, so it is L1's
    /// purge that is gated on THIS view covering it — meaning it must OUTLIVE ITS OWN SOURCE with margin, or
    /// L1's gate can never release and L1 grows without bound. 10 against L1's 7 is the same 3-day margin L1
    /// takes over raw's 4, and the ordering is pinned as a test invariant so a future tuning pass cannot
    /// invert it silently.</para>
    ///
    /// <para><b>Capacity (#1581), MEASURED at the same scale #1849 used</b> — a seeded 600-query store at the
    /// default 5-minute <c>query_store</c> cadence, 24 hours of collection (187,200 raw rows). The rig
    /// reproduces #1849's own L1 figures to the row (28,800 rows / 11 MB), which is what makes the rest
    /// comparable rather than merely plausible:
    ///
    /// <list type="bullet">
    /// <item>this view: <b>15,000 rows / 4.7 MB per day</b> — near-raw cardinality like L1, but keyed on
    /// interval identity x DAY where L1 keys on interval identity x HOUR, and an interval spans ~2 hourly
    /// buckets against 1 daily one. So the fourth near-raw-cardinality object is the SMALLEST of them:
    /// <b>~47 MB at 10 days</b> against L1's 79 MB at 7.</item>
    /// <item><see cref="QueryStoreStatsDayGrainDailyView"/> above it: ~600 rows/day at composer grain, and
    /// measured byte-for-byte identical to the corrected daily it sits beside (448 kB each over the same
    /// span) — which is why it is kept indefinitely like every other daily rather than needing a horizon of
    /// its own.</item>
    /// </list></para>
    /// </summary>
    public const string IntervalDailyRetentionInterval = "10 days";

    /// <summary><see cref="TimeSpan"/> twin of <see cref="RawRetentionInterval"/> for callers doing arithmetic
    /// (the #1665 partial-window notice). RetentionTierRouterTests pins the two equal, as does the all-five
    /// sweep in TimescaleContinuousAggregateTests (#1905), so they can't drift.</summary>
    public static readonly TimeSpan RawRetentionSpan = TimeSpan.FromDays(4);

    /// <summary><see cref="TimeSpan"/> twin of <see cref="IntervalRetentionInterval"/>, pinned equal by
    /// TimescaleContinuousAggregateTests. The ORDERING that keeps the raw arming gate satisfiable — this
    /// strictly greater than <see cref="RawRetentionSpan"/> — is no longer pinned here by hand: it is one pair
    /// in the walk over <see cref="RetentionPolicies"/> (#1905).</summary>
    public static readonly TimeSpan IntervalRetentionSpan = TimeSpan.FromDays(7);

    /// <summary><see cref="TimeSpan"/> twin of <see cref="IntervalDailyRetentionInterval"/>, pinned equal by
    /// TimescaleContinuousAggregateTests. Its ordering against <see cref="IntervalRetentionSpan"/> is checked
    /// by the walk over <see cref="RetentionPolicies"/> (#1905), like every other pair: a consumer that expired
    /// before its source would hold its source's purge forever, and since #1877 would also STOP one that is
    /// already running, on a healthy store, without self-releasing.</summary>
    public static readonly TimeSpan IntervalDailyRetentionSpan = TimeSpan.FromDays(10);

    /// <summary><see cref="TimeSpan"/> twin of <see cref="HourlyRetentionInterval"/>; pinned equal by
    /// RetentionTierRouterTests and by the all-five sweep in TimescaleContinuousAggregateTests (#1905).
    /// <see cref="RetentionTierRouter.HourlyMaxAge"/> is derived from this, so the read side cannot be left
    /// behind when the horizon moves (#1937).</summary>
    public static readonly TimeSpan HourlyRetentionSpan = TimeSpan.FromDays(90);

    /// <summary>
    /// A TimescaleDB retention policy: schedule a background job that DROPs chunks older than
    /// <paramref name="dropAfter"/>. <c>if_not_exists</c> so a restart re-converges. The actual drop is a
    /// chunk-level DROP TABLE (cheap, no rewrite), so unlike the CAGG backfill it needs no off-hours window.
    ///
    /// <para><b>There is deliberately no <c>scheduled</c> argument here (#1705).</b> <c>add_retention_policy</c>
    /// has NEVER accepted one on any TimescaleDB 2.x — the parameter exists only on <c>add_job</c> /
    /// <c>alter_job</c>. Passing it made this statement fail with <c>42883 function ... does not exist</c> on
    /// EVERY store, fresh or upgraded, and the per-policy catch in
    /// <see cref="EnsureRetentionPoliciesAsync"/> turned that into a warning — so retention silently stopped
    /// existing everywhere rather than only on old versions. The paused-at-creation guarantee #1680 needs is
    /// preserved by the CALLER instead: it creates and pauses inside ONE transaction (see
    /// <see cref="PauseJobSql"/>). Verified against 2.28.1: the accepted signature is
    /// <c>(regclass, "any", boolean, interval, timestamptz, text, interval)</c>.</para>
    ///
    /// <para>Returns the new policy's <c>job_id</c>, or <c>-1</c> when <c>if_not_exists</c> matched an existing
    /// policy and skipped — the caller MUST NOT feed that -1 to <c>alter_job</c>.</para>
    /// </summary>
    public static string AddRetentionPolicySql(string relation, string dropAfter)
        => $"SELECT add_retention_policy('collect.{relation}', drop_after => INTERVAL '{dropAfter}', if_not_exists => true)";

    /// <summary>
    /// Pauses a just-created job by id. Run in the SAME transaction as
    /// <see cref="AddRetentionPolicySql"/>: the TimescaleDB job scheduler is a separate backend, so it cannot see
    /// the <c>bgw_job</c> row until that transaction commits, and by then the row already reads
    /// <c>scheduled = false</c>. That closes the #1680 window without needing a parameter the API does not have.
    /// <c>job_id</c> is <c>integer</c>, not bigint (the #1586 cast trap). $1 the job id.
    /// </summary>
    public const string PauseJobSql = "SELECT alter_job($1::integer, scheduled => false)";

    /// <summary>
    /// Arms a retention policy that was created paused. Separated from creation because TimescaleDB runs a new
    /// policy's first check IMMEDIATELY at creation, not on its next interval (#1680).
    /// </summary>
    public static string ArmRetentionPolicySql(string relation)
        => SetRetentionScheduleSql(relation, scheduled: true);

    /// <summary>
    /// Moves an EXISTING retention policy onto the horizon the constants now name (#1937), and touches nothing
    /// else about it.
    ///
    /// <para><b>Why this has to exist.</b> <c>add_retention_policy(if_not_exists =&gt; true)</c> returns -1 for a
    /// policy the store already has and changes NOTHING about it — verified against 2.28.1, which additionally
    /// emits <c>WARNING: retention policy already exists</c> and leaves the old <c>drop_after</c> in place. So
    /// changing a horizon constant gives fresh installs the new number and leaves every store that already ran
    /// on the old one, forever. That is the fresh-versus-upgraded drift this project treats as a defect, and
    /// with the hourly tier it is the difference between a month-scale view rendering and not.</para>
    ///
    /// <para><b>Why it is safe to run on every start.</b> The <c>IS DISTINCT FROM</c> guard compares as
    /// INTERVAL, not text, so a policy already on the right horizon matches nothing and no job is touched —
    /// this is a no-op on the second and every later start, and on a fresh store the policy was just created
    /// with the right value. Only <c>config</c> is named, so the job's SCHEDULED state is preserved exactly:
    /// measured on 2.28.1 against both an armed and a held policy, each kept its state across the update while
    /// the horizon moved. That is what lets this run BEFORE the coverage gate without disturbing it — a policy
    /// #1877 is holding paused stays paused, and the #1680 discipline of never exposing an armed window is not
    /// weakened, because this statement cannot arm anything.</para>
    ///
    /// <para><c>next_start</c> is left alone too, and measurably does not jump to now: the armed policy's next
    /// run stayed one schedule interval out across the update, so converging a horizon never triggers an
    /// immediate purge.</para>
    /// </summary>
    public static string ConvergeRetentionHorizonSql(string relation)
        => $@"SELECT alter_job(j.job_id, config => jsonb_set(j.config, '{{drop_after}}', to_jsonb($1::text)))
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{relation}'
AND   (j.config->>'drop_after')::interval IS DISTINCT FROM $1::interval";

    /// <summary>
    /// Re-holds a retention policy that is ALREADY ARMED (#1877). The mirror of
    /// <see cref="ArmRetentionPolicySql"/>, and the statement that closes the arm-only gap: a policy created
    /// paused stays paused by itself, but <c>add_retention_policy(if_not_exists =&gt; true)</c> returns -1 for a
    /// policy this store already has, so nothing ever paused one whose COVERAGE LIST GREW under it.
    ///
    /// <para>Reached ONLY from a positive coverage measurement — never from an indeterminate one. See
    /// <c>RetentionCoverage</c> for why that distinction is the whole of #1877.</para>
    /// </summary>
    public static string HoldRetentionPolicySql(string relation)
        => SetRetentionScheduleSql(relation, scheduled: false);

    /// <summary>
    /// The shared body of <see cref="ArmRetentionPolicySql"/> and <see cref="HoldRetentionPolicySql"/>: flip one
    /// relation's retention job. Filtering by proc_name AND the hypertable is what keeps it from arming — or
    /// stopping — some other policy, or every policy, by accident. Idempotent in both directions: setting a job
    /// to the state it is already in is a no-op, which is what lets the sweep re-assert the verdict every start.
    /// </summary>
    private static string SetRetentionScheduleSql(string relation, bool scheduled)
        => $@"SELECT alter_job(j.job_id, scheduled => {(scheduled ? "true" : "false")})
FROM timescaledb_information.jobs AS j
WHERE j.proc_name = 'policy_retention'
AND   j.hypertable_schema = 'collect'
AND   j.hypertable_name = '{relation}'";

    /// <summary>
    /// Is it safe to arm <paramref name="relation"/>'s retention policy — i.e. does EVERY tier below it already
    /// cover everything this relation holds? Emits the source's oldest row followed by one
    /// <c>min(bucket)</c> column per coverage relation, in <paramref name="coverageRelations"/> order.
    ///
    /// <para>This is the check that makes arming provably non-destructive rather than a race the operator has to
    /// win. It also self-heals: a store that is not yet covered stays paused and arms on the first start AFTER a
    /// backfill, with no manual step.</para>
    ///
    /// <para><b>Plural since #1849, and the plurality is the point.</b> <c>query_store_stats</c> now feeds TWO
    /// rollup families — the original inflated pair and the corrected one — and a purge that satisfied only one
    /// of them would destroy raw history the other has never materialized. The verdict is therefore an AND over
    /// all of them, evaluated in <see cref="MeasureRetentionCoverageAsync"/> rather than folded into SQL:
    /// <c>GREATEST</c> would have expressed it in one column and is exactly wrong here, because it SKIPS NULLs.
    /// An empty new rollup would vanish from the comparison and the gate would pass on the old rollup alone —
    /// which is the whole failure this exists to prevent.</para>
    /// </summary>
    public static string RetentionArmSafetySql(string relation, string sourceTimeColumn, IReadOnlyList<string> coverageRelations)
    {
        if (coverageRelations is null)
        {
            throw new ArgumentNullException(nameof(coverageRelations));
        }

        var columns = coverageRelations.Select((c, i) => $"    (SELECT min(bucket) FROM collect.{c}) AS coverage_oldest_{i}");
        return $"SELECT{Environment.NewLine}    (SELECT min({sourceTimeColumn}) FROM collect.{relation}) AS source_oldest,{Environment.NewLine}"
            + string.Join("," + Environment.NewLine, columns);
    }

    /// <summary>
    /// The raw tier and EVERY aggregate that must already cover it — the single source of truth for which tables
    /// are coverage-gated, shared by the policy setup (<see cref="EnsureRetentionPoliciesAsync"/>) and by the
    /// catalog sweep's own drop (#1784). They MUST agree: two purge paths judging the same table by different
    /// rules is precisely the defect #1784 records.
    ///
    /// <para><b>Coverage is a LIST, because a raw table can have more than one consumer (#1849).</b>
    /// <c>query_store_stats</c> is rolled up twice: by the original
    /// <see cref="QueryStoreStatsHourlyView"/> (kept for the history it already holds) and by
    /// <see cref="QueryStoreStatsIntervalHourlyView"/>, the corrected rollups' dedup layer. Both are named here
    /// so raw cannot purge over history EITHER of them is missing. Extending this map rather than adding a
    /// second one is deliberate: both purge paths read this list, so a consumer added here is automatically
    /// honored by both, which is the #1784 invariant.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string Relation, string TimeColumn, IReadOnlyList<string> Coverage)> RawTierCoverage =
        new (string, string, IReadOnlyList<string>)[]
    {
        ("query_stats", "collection_time", new[] { QueryStatsHourlyView }),
        ("procedure_stats", "collection_time", new[] { ProcedureStatsHourlyView }),
        ("query_store_stats", "collection_time", new[] { QueryStoreStatsHourlyView, QueryStoreStatsIntervalHourlyView }),
    };

    /// <summary>
    /// EVERY retention policy this store attaches, each naming the tier(s) that must already cover it before
    /// arming is safe (#1680). The rule this list enforces is: NEVER DROP WHAT YOUR CONSUMER HAS NOT CAPTURED
    /// YET. Iterated by <see cref="EnsureRetentionPoliciesAsync"/>, which used to build it as a local.
    ///
    /// <para><b>Declared rather than built inline (#1905), because the ORDERING it encodes became testable
    /// only once something outside the sweep could enumerate it.</b> Every entry's consumers must outlive the
    /// entry itself — a consumer that expired first would hold its own source's purge forever, and since #1877
    /// would also STOP a purge already running on a healthy store, without self-releasing. That invariant used
    /// to be asserted by hand against the pairs that happened to exist; it is now walked over this list, so a
    /// policy added to a tier that has none today is covered the day it is added rather than the day someone
    /// remembers to extend a test.</para>
    ///
    /// <para>MUST stay declared AFTER <see cref="RawTierCoverage"/> and <see cref="BaselineAggregates"/>:
    /// static field initializers run in textual order, so moving it above either one reads a null and throws
    /// <c>TypeInitializationException</c> on first touch.</para>
    ///
    /// <para>For the raw and hourly tiers the consumer is the next aggregate down the ladder — raw tables are
    /// covered by their hourly CAGG, hourly CAGGs by their daily one — so "coverage" names that tier.</para>
    ///
    /// <para>THE LEAF RULE (#1757): a tier with nothing below it is not exempt, it just has a different
    /// consumer. The baseline aggregates are leaves; their consumer is the baseline COMPUTATION, whose capture
    /// requirement is <c>BaselineMath.BaselineWindowDays</c> (30). Their arming condition is therefore "the
    /// tier holds at least the baseline window of buckets" — the same rule with the consumer named honestly,
    /// still runtime-evaluable like the other seven rather than a degenerate always-open gate. It is
    /// belt-and-braces by construction: <see cref="BaselineRetentionSpan"/> (35d) already exceeds the window,
    /// so even an immediately-armed policy could not eat it. A policy with no identifiable consumer at all
    /// still does not belong in this list.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string Relation, string DropAfter, string TimeColumn, IReadOnlyList<string> Coverage)> RetentionPolicies =
        RawTierCoverage
            .Select(t => (Relation: t.Relation, DropAfter: RawRetentionInterval, TimeColumn: t.TimeColumn, Coverage: t.Coverage))
            .Concat(new (string Relation, string DropAfter, string TimeColumn, IReadOnlyList<string> Coverage)[]
        {
            (Relation: QueryStatsHourlyView,      DropAfter: HourlyRetentionInterval, TimeColumn: "bucket",          Coverage: new[] { QueryStatsDailyView }),
            (Relation: ProcedureStatsHourlyView,  DropAfter: HourlyRetentionInterval, TimeColumn: "bucket",          Coverage: new[] { ProcedureStatsDailyView }),
            (Relation: QueryStoreStatsHourlyView, DropAfter: HourlyRetentionInterval, TimeColumn: "bucket",          Coverage: new[] { QueryStoreStatsDailyView }),
            (Relation: QueryStatsDbHourlyView,    DropAfter: HourlyRetentionInterval, TimeColumn: "bucket",          Coverage: new[] { QueryStatsDbDailyView }),

            /* The corrected Query Store tier (#1849, extended by #1869).

               L1 has THREE consumers. Two because the corrected daily is its SIBLING rather than the corrected
               hourly's child (identity-width hierarchical CAGGs are leaves — see
               CreateQueryStoreStatsCorrectedDailySql), and a third because #1869 hung the interval-grain DAILY
               layer off it as well. All three are named, so L1 cannot purge over history ANY of them is still
               missing — and the third is the load-bearing one on a store taking this build, because that store
               has a fully-caught-up L1 and an empty interval_daily, which is precisely the state where a gate
               reading only the older two would drop the only copy of history the day-grain daily has never
               seen. That store's L1 policy was ALREADY ARMED under #1849, and until #1877 the gate could only
               arm — so it kept purging while the new consumer held nothing, capping how deep the day-grain
               daily could ever be backfilled. The sweep now RE-HOLDS it on the measured shortfall.

               The corrected HOURLY is a leaf, so the leaf rule applies (#1757): its consumer is the composed
               READ, which routes past HourlyRouteMaxAge to the corrected DAILY — exactly the relationship the
               original hourly has to the original daily, so it takes the same horizon and the same coverage
               tier.

               The interval-grain DAILY (#1869) mirrors L1 one level down: one consumer (the day-grain daily it
               feeds), and a short horizon that must still EXCEED L1's, since it is what L1's own gate waits on.
               Both composer-grain dailies are kept indefinitely and get no policy. */
            (Relation: QueryStoreStatsIntervalHourlyView,  DropAfter: IntervalRetentionInterval,      TimeColumn: "bucket", Coverage: new[] { QueryStoreStatsCorrectedHourlyView, QueryStoreStatsCorrectedDailyView, QueryStoreStatsIntervalDailyView }),
            (Relation: QueryStoreStatsCorrectedHourlyView, DropAfter: HourlyRetentionInterval,        TimeColumn: "bucket", Coverage: new[] { QueryStoreStatsCorrectedDailyView }),
            (Relation: QueryStoreStatsIntervalDailyView,   DropAfter: IntervalDailyRetentionInterval, TimeColumn: "bucket", Coverage: new[] { QueryStoreStatsDayGrainDailyView }),
        })
        /* The seven baseline-tier policies (#1757; nine until #2007). Coverage is the tier ITSELF: see the leaf rule in the
           summary above -- their consumer is the baseline computation, whose capture requirement is the
           30-day window, and BaselineRetentionSpan (35d) exceeds it by construction. */
        .Concat(BaselineAggregates.Select(a =>
            (Relation: a.View, DropAfter: BaselineRetentionInterval, TimeColumn: "bucket", Coverage: (IReadOnlyList<string>)new[] { a.View })))
        .ToArray();

    /// <summary>
    /// Is <paramref name="relation"/> one of the coverage-gated raw tiers? Lets a caller skip the cost of a
    /// connection for the many tables the gate does not apply to, without duplicating the membership rule --
    /// it reads the same <see cref="RawTierCoverage"/> map the gate itself does.
    /// </summary>
    public static bool IsCoverageGatedRelation(string relation)
    {
        foreach (var (tierRelation, _, _) in RawTierCoverage)
        {
            if (string.Equals(tierRelation, relation, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// May <paramref name="relation"/>'s expired chunks be dropped right now without destroying history no
    /// aggregate holds (#1784)? True for any table that is not coverage-gated — the gate is a property of the
    /// raw tier, not of retention in general.
    ///
    /// <para>This is the SAME predicate the #1680 arming gate uses, reached through the same map, deliberately:
    /// the tiered policy and the catalog sweep drop the same chunks, so they must not be able to disagree about
    /// whether that is safe. Reusing it also inherits its fail-closed behaviour — an indeterminate coverage
    /// state answers "not safe", which for a DROP means the data survives to be re-judged next cycle.</para>
    ///
    /// <para>Note this is a BINARY judgement, not a clamped cutoff, and it has to be:
    /// <c>drop_chunks</c> can only remove the OLDEST chunks, and when coverage lags it is exactly the oldest
    /// chunks that are uncovered. No cutoff drops the covered tail while sparing the uncovered head, so there is
    /// no cutoff value that expresses the safe operation — only "all of it" or "none of it". See #1784 for the
    /// worked arithmetic showing where a min(horizon, coverage-floor) clamp still deletes uncovered history.</para>
    /// </summary>
    public static async Task<bool> IsRawTierDropSafeAsync(
        NpgsqlConnection connection, string relation, CancellationToken cancellationToken = default)
    {
        foreach (var (tierRelation, timeColumn, coverage) in RawTierCoverage)
        {
            if (string.Equals(tierRelation, relation, StringComparison.Ordinal))
            {
                var (verdict, _) = await MeasureRetentionCoverageAsync(connection, tierRelation, timeColumn, coverage, cancellationToken);

                /* Only a POSITIVE all-clear permits a drop. Short and Unknown both answer "no", exactly as they
                   did when this probe was a bool — the tristate exists for the ARMING side, which alone needs
                   to tell a measured regression apart from a failed measurement (#1877). Collapsing it here
                   keeps the #1793 property intact: both purge paths still judge the same drop identically. */
                return verdict == RetentionCoverage.Covered;
            }
        }

        return true;
    }

    /// <summary>
    /// What a coverage probe was able to CONCLUDE — the distinction #1877 turns on.
    ///
    /// <para>Arming needs only "safe or not", and this was a bool for that reason. Re-holding needs more: an
    /// already-armed policy may be stopped on evidence that its coverage genuinely fell short, and must NEVER be
    /// stopped because the evidence could not be gathered. Folding a failed probe in with a measured shortfall
    /// is what made "unsafe implies disarm" unshippable — one bad probe on a busy store would have stopped
    /// purging across every tier and grown disk until someone noticed, trading #1877's bounded depth cap for an
    /// unbounded disk risk.</para>
    /// </summary>
    private enum RetentionCoverage
    {
        /// <summary>Every named consumer positively reaches at least as far back as the source — or the source
        /// is empty, so there is no history to lose. The only verdict that permits arming or dropping.</summary>
        Covered,

        /// <summary>MEASURED short: the probe ran, the source holds rows, and a named consumer either holds none
        /// or starts later than the source's oldest row. A fact about the store, not a failure to read it — and
        /// the only verdict that may stop a policy this store already armed.</summary>
        Short,

        /// <summary>Nothing could be concluded: the probe threw, timed out, or came back empty. Refuses arming
        /// exactly as before, and refuses to re-hold, because a probe error is not a coverage regression.</summary>
        Unknown,
    }

    /// <summary>
    /// How far <paramref name="relation"/>'s consumers reach relative to what it holds (#1680): <c>Covered</c>
    /// when the source is empty or EVERY relation in <paramref name="coverageRelations"/> reaches at least as far
    /// back as the source does, <c>Short</c> when one of them is measurably behind, <c>Unknown</c> when the
    /// probe could not answer at all. Also returns the first consumer found short, so the operator warning can
    /// name the tier to backfill rather than the whole list.
    ///
    /// <para>The verdict is an AND across consumers (#1849), and the loop below is short-circuiting in the SAFE
    /// direction only: one empty or shallow consumer holds the policy even if every other consumer is complete.
    /// A raw table with two rollup families is covered when the LEAST-covering of them covers it.</para>
    /// </summary>
    private static async Task<(RetentionCoverage Verdict, string? ShortConsumer)> MeasureRetentionCoverageAsync(
        NpgsqlConnection connection, string relation, string sourceTimeColumn, IReadOnlyList<string> coverageRelations, CancellationToken cancellationToken)
    {
        try
        {
            using var command = new NpgsqlCommand(RetentionArmSafetySql(relation, sourceTimeColumn, coverageRelations), connection) { CommandTimeout = SetupTimeoutSeconds };
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return (RetentionCoverage.Unknown, null);
            }

            /* Nothing in the source - a fresh store. No history to lose, so arm. */
            if (await reader.IsDBNullAsync(0, cancellationToken))
            {
                return (RetentionCoverage.Covered, null);
            }

            var sourceOldest = reader.GetDateTime(0);
            for (var i = 0; i < coverageRelations.Count; i++)
            {
                /* Source has data but THIS coverage tier is empty - arming would drop history it never
                   materialized, whatever the other tiers hold. An empty consumer is a MEASUREMENT and not an
                   unknown: the relation exists and answered with no rows, which is precisely the state a
                   newly-added consumer is born in on an upgrading store (#1877). */
                if (await reader.IsDBNullAsync(i + 1, cancellationToken))
                {
                    return (RetentionCoverage.Short, coverageRelations[i]);
                }

                if (reader.GetDateTime(i + 1) > sourceOldest)
                {
                    return (RetentionCoverage.Short, coverageRelations[i]);
                }
            }

            return (RetentionCoverage.Covered, null);
        }
        catch (Exception)
        {
            /* Fail closed: if coverage cannot be established the policy is not armed — and, since #1877, not
               re-held either. Both directions read the same way here: an unmeasurable store is left exactly as
               it was, because nothing was learned about it. */
            return (RetentionCoverage.Unknown, null);
        }
    }

    /// <summary>
    /// Attaches the tiered retention policies. The three raw tables drop at <see cref="RawRetentionInterval"/>
    /// and the hourly HISTORY CAGGs at <see cref="HourlyRetentionInterval"/>; the daily history CAGGs get no
    /// policy at all and are kept indefinitely. Two tiers are deliberately off that ladder and neither is
    /// history: the interval-identity dedup layers (<see cref="IntervalRetentionInterval"/> hourly,
    /// <see cref="IntervalDailyRetentionInterval"/> daily) are internal plumbing sized only to outlive what
    /// gates on them, and the baseline aggregates keep <see cref="BaselineRetentionInterval"/>. The summary this
    /// logs names all of them, because an operator cross-checking it against
    /// <c>timescaledb_information.jobs</c> meets every one (#1958).
    /// Ordering safety is by HORIZON, not run order — each tier's drop stays comfortably past the next
    /// tier's 3-day refresh start_offset (4d raw vs 3d hourly refresh; 90d hourly vs 3d daily refresh), so a drop
    /// never removes history the next tier has not yet materialized. Idempotent (<c>if_not_exists</c>) and
    /// failure-isolated per policy. MUST run AFTER <see cref="EnsureContinuousAggregatesAsync"/> so the hourly
    /// CAGGs the hourly policies target already exist. Returns the number of policies in place.
    ///
    /// COLD START ON AN EXISTING STORE (#1759): a store that already holds raw history older than its hourly
    /// CAGG has materialized does NOT lose it. <see cref="MeasureRetentionCoverageAsync"/> is fail-closed, so
    /// that store's raw policies are created and left PAUSED, and the per-policy WARN says which rollup is
    /// short. This used to be documented as a caveat prescribing a manual backfill "BEFORE this policy's
    /// first run" — a step no store ever received, and a defect rather than a caveat. The backfill is now a real
    /// operator verb (<c>--backfill-rollups</c>) with a disk preflight, and once it carries a rollup past the raw
    /// horizon this gate arms the held policy by itself on the next start, with no manual step.
    ///
    /// <para>A COVERAGE LIST THAT GROWS (#1877). Holding is not only for policies this sweep just created.
    /// <c>add_retention_policy(if_not_exists =&gt; true)</c> returns -1 for a policy the store already has, so
    /// nothing pauses it — which is right for a restart (it must not undo an operator's backfill) and was wrong
    /// for a build that ADDS a consumer to a gate stores have already armed, as #1869 did. Such a policy kept
    /// purging its source while the new consumer held nothing, capping how deep that consumer could ever be
    /// backfilled. It is now re-held, but ONLY on a positive measurement: see the three-valued
    /// <c>RetentionCoverage</c>, which is what keeps a probe failure from stopping retention fleet-wide.</para>
    /// </summary>
    public static async Task<int> EnsureRetentionPoliciesAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var applied = 0;
        var armed = 0;
        var held = 0;
        var indeterminate = 0;
        var converged = 0;

        foreach (var (relation, dropAfter, timeColumn, coverage) in RetentionPolicies)
        {
            try
            {
                /* Created PAUSED, always. TimescaleDB runs a new policy's first check immediately at creation
                   rather than on its next interval, so a policy created live drops before any external session
                   can pause it - there is no window to win. That cost a field store two days of history.

                   The pause happens in the SAME transaction as the create (#1705). add_retention_policy has no
                   scheduled argument on any 2.x, so the only way to never expose an armed job is to keep the
                   bgw_job row invisible until it already reads scheduled = false: the scheduler is a separate
                   backend and cannot see an uncommitted row. Verified on 2.28.1 against a hypertable holding
                   30-day-old rows under a 4-day policy - the rows survived, so no immediate drop occurred. */
                await using (var tx = await connection.BeginTransactionAsync(cancellationToken))
                {
                    int jobId;
                    using (var create = new NpgsqlCommand(AddRetentionPolicySql(relation, dropAfter), connection, tx) { CommandTimeout = SetupTimeoutSeconds })
                    {
                        jobId = Convert.ToInt32(await create.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
                    }

                    /* -1 means if_not_exists matched an existing policy and skipped. There is no new job to
                       pause, and the existing one keeps whatever armed/paused state it already had - which is
                       what makes a restart converge instead of re-pausing a policy this store already armed. */
                    if (jobId > 0)
                    {
                        using var pause = new NpgsqlCommand(PauseJobSql, connection, tx) { CommandTimeout = SetupTimeoutSeconds };
                        pause.Parameters.AddWithValue(jobId);
                        await pause.ExecuteNonQueryAsync(cancellationToken);
                    }

                    await tx.CommitAsync(cancellationToken);
                }

                applied++;

                /* #1937: converge an EXISTING policy onto the current horizon. if_not_exists returned -1 above
                   for a policy this store already had, leaving whatever drop_after it was created with — so
                   without this, a horizon change reaches fresh installs only and every upgraded store keeps the
                   old number permanently. Named config only, so the job's armed/paused state is untouched and
                   this cannot arm anything the gate below is about to judge. A no-op once converged. */
                using (var converge = new NpgsqlCommand(ConvergeRetentionHorizonSql(relation), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    converge.Parameters.AddWithValue(dropAfter);
                    using var reader = await converge.ExecuteReaderAsync(cancellationToken);
                    if (await reader.ReadAsync(cancellationToken))
                    {
                        converged++;
                        logger?.LogInformation(
                            "Retention policy for {Relation} moved to a {DropAfter} horizon - this store was created under an earlier default and kept it, because add_retention_policy does not update a policy that already exists.",
                            relation, dropAfter);
                    }
                }

                var (verdict, shortConsumer) = await MeasureRetentionCoverageAsync(connection, relation, timeColumn, coverage, cancellationToken);
                if (verdict == RetentionCoverage.Covered)
                {
                    using var arm = new NpgsqlCommand(ArmRetentionPolicySql(relation), connection) { CommandTimeout = SetupTimeoutSeconds };
                    await arm.ExecuteNonQueryAsync(cancellationToken);
                    armed++;
                }
                else if (verdict == RetentionCoverage.Short)
                {
                    /* HELD — and since #1877 that is an ACTION, not just the absence of arming. A policy this
                       store created moments ago is already paused and this re-asserts it; a policy the store
                       armed under an EARLIER build, whose coverage list has since GROWN a consumer, is stopped
                       here. That second case is the whole issue: if_not_exists returned -1 for the existing
                       policy so nothing paused it, and it kept purging its source while the new consumer held
                       nothing — capping how deep that consumer could ever be backfilled.

                       Safe to do unconditionally because the verdict is a MEASUREMENT. An indeterminate probe
                       lands in the branch below and touches nothing, so no store can have its purge stopped by
                       a timeout, a permission blip, or a relation that is mid-rebuild. And the release is the
                       existing arming path, unchanged: the next sweep measures Covered and arms it, with no
                       manual step, exactly as a first-time hold releases. */
                    using var hold = new NpgsqlCommand(HoldRetentionPolicySql(relation), connection) { CommandTimeout = SetupTimeoutSeconds };
                    await hold.ExecuteNonQueryAsync(cancellationToken);
                    held++;
                    logger?.LogWarning(
                        "Retention policy for {Relation} HELD PAUSED - {ShortConsumer} does not yet cover everything it holds, so arming could drop history that rollup has never materialized. Backfill past the {DropAfter} horizon and the policy arms itself on the next start.",
                        relation, shortConsumer, dropAfter);
                }
                else
                {
                    /* Coverage could not be MEASURED, which is not the same as measuring a shortfall. Leave the
                       policy in whatever state it is already in: a new one is paused (fail-closed, as always),
                       and one this store already armed keeps running. Disarming here instead would let a single
                       bad probe stop purging across every tier at once and grow disk without bound — the
                       failure mode that kept #1877 unfixed rather than fixed badly. */
                    indeterminate++;
                    logger?.LogWarning(
                        "Retention policy for {Relation} left as-is - its coverage ({Coverage}) could not be established this start, and an unreadable store is not evidence of anything. Re-judged on the next start.",
                        relation, string.Join(" + ", coverage));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Retention policy for {Relation} ({DropAfter}) failed - that tier keeps growing until the next restart retries: {Message}",
                    relation, dropAfter, ex.Message);
            }
        }

        /* EVERY tier that has a policy is named, and every horizon is INTERPOLATED rather than restated
           (#1942 — this exact line class has drifted before). The parenthetical used to read "raw {Raw}, hourly
           CAGGs {Hourly}; daily CAGGs kept indefinitely", which is a universal claim with three counterexamples
           sitting in timescaledb_information.jobs — the very table the docs send an operator to when they want
           to check it. The interval-dedup L1 is deliberately SHORTER than the hourly tier (it is internal
           plumbing gated on outliving raw, not history); its daily twin carries a horizon at all, despite the
           line promising dailies are kept forever; and the seven baseline aggregates have a horizon of their own
           that went unmentioned. A field operator cross-checking found the first one immediately and had to
           work out whether they had hit a bug (#1958). A summary line is only worth printing if it survives
           being checked. */
        logger?.LogInformation(
            "TimescaleDB: {Applied}/{Total} retention policies in place, {Armed} armed, {Held} held paused pending backfill, {Indeterminate} left as-is (coverage unreadable), {Converged} moved onto a new horizon (raw {Raw}, hourly history CAGGs {Hourly}, baseline CAGGs {Baseline}, internal interval-dedup tiers {Interval} hourly and {IntervalDaily} daily; the daily history CAGGs carry no policy and are kept indefinitely)",
            applied, RetentionPolicies.Count, armed, held, indeterminate, converged,
            RawRetentionInterval, HourlyRetentionInterval, BaselineRetentionInterval, IntervalRetentionInterval, IntervalDailyRetentionInterval);
        return applied;
    }

    /* ─────────────── rollup availability (the plain-PostgreSQL guard, #1664) ─────────────── */

    /// <summary>
    /// One catalog round trip answering "which retention rollups exist in THIS store?" — the availability
    /// input to <see cref="RetentionTierRouter.Resolve(DateTime, DateTime, bool, bool)"/>. <c>to_regclass</c>
    /// needs no table privilege and returns NULL for a missing relation, so this is safe under the viewer's
    /// least-privilege role and on any store shape. Column order matches
    /// <see cref="RollupAvailability"/>'s constructor.
    /// </summary>
    public static readonly string RollupProbeSql =
        "SELECT " +
        $"to_regclass('collect.{QueryStatsHourlyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStatsDailyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStatsDbHourlyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStatsDbDailyView}') IS NOT NULL, " +
        $"to_regclass('collect.{ProcedureStatsHourlyView}') IS NOT NULL, " +
        $"to_regclass('collect.{ProcedureStatsDailyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStoreStatsHourlyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStoreStatsDailyView}') IS NOT NULL, " +
        /* The corrected Query Store rollups (#1849). A store on an older service has none of them and reads
           fall back to the pair above — the same per-tier degrade #1664/#1665 built, which is why these need
           no schema migration or version gate: existence IS the probe. */
        $"to_regclass('collect.{QueryStoreStatsIntervalHourlyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStoreStatsCorrectedHourlyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStoreStatsCorrectedDailyView}') IS NOT NULL, " +
        /* The day-grain daily and its dedup layer (#1869) — the same existence-is-the-probe degrade, so a
           store on a #1849-era service keeps reading the corrected daily and needs no version gate either. */
        $"to_regclass('collect.{QueryStoreStatsIntervalDailyView}') IS NOT NULL, " +
        $"to_regclass('collect.{QueryStoreStatsDayGrainDailyView}') IS NOT NULL";

    /// <summary>
    /// Detects which continuous-aggregate rollups exist in the store (<see cref="RollupProbeSql"/>). On a
    /// plain-PostgreSQL store every flag is false — and that is a COMPLETE configuration, not a degraded one:
    /// without the extension no retention policy ever drops raw, so the raw tables hold full history and
    /// routing everything to raw loses nothing. On a TimescaleDB store the worker's ensure sweep creates the
    /// views before any reader can need them; a partially-built store (one aggregate's failure-isolated
    /// setup failed) reports exactly what exists, so the router degrades per tier instead of a reader
    /// throwing 42P01 at a user (#1664, the gated-live catch on #1661's first cut).
    /// </summary>
    public static async Task<RollupAvailability> DetectRollupsAsync(NpgsqlDataSource dataSource, CancellationToken cancellationToken = default)
    {
        if (dataSource is null)
        {
            throw new ArgumentNullException(nameof(dataSource));
        }

        await using var command = dataSource.CreateCommand(RollupProbeSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new RollupAvailability(
            reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetBoolean(3),
            reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6), reader.GetBoolean(7),
            reader.GetBoolean(8), reader.GetBoolean(9), reader.GetBoolean(10),
            reader.GetBoolean(11), reader.GetBoolean(12));
    }

    /* ─────────────── rollup COVERAGE (the un-materialized-history guard, #1759) ─────────────── */

    /// <summary>The hourly rollups' bucket width — named so <see cref="RollupViews"/> reads as data.
    /// <para>Declared BEFORE <see cref="RollupViews"/> and that is not cosmetic: C# runs static field
    /// initializers in DECLARATION order, so a list declared above these would capture
    /// <c>default(TimeSpan)</c> — zero — for every width, and every backfill bucket count would divide by
    /// zero-width buckets. Caught by RollupBackfillTests going red on exactly that.</para></summary>
    public static readonly TimeSpan HourlyBucket = TimeSpan.FromHours(1);

    /// <summary>The daily rollups' bucket width. See <see cref="HourlyBucket"/> on declaration order.</summary>
    public static readonly TimeSpan DailyBucket = TimeSpan.FromDays(1);

    /// <summary>
    /// Every rollup view in probe order, with the two DIFFERENT relations it is measured against. One list, so
    /// the coverage probe's column order, <see cref="RollupCoverage"/>'s constructor and
    /// <see cref="RollupCoverage.RawTableFor"/> cannot drift into disagreeing.
    ///
    /// <para><b><c>RawTable</c> and <c>Source</c> are not the same question, and conflating them was #1798.</b>
    /// <c>RawTable</c> is where a READ falls back to when this rollup cannot answer a window — always the raw
    /// hypertable, because that is the only relation holding per-sweep rows. <c>Source</c> is what this rollup
    /// is BUILT FROM, and therefore the most history it can ever contain: raw for the hourlies, but the HOURLY
    /// VIEW for every daily, since all four dailies are hierarchical continuous aggregates
    /// (<c>time_bucket('1 day', bucket) FROM collect.&lt;x&gt;_hourly</c>).</para>
    ///
    /// <para>The distinction decides whether a backfill can ever finish. The #1680 arming gate for an
    /// HOURLY-tier retention policy is SOURCE-relative — the daily must cover what the hourly holds — while the
    /// backfill verb converged every rollup to RAW's oldest row. On a store whose raw purges are armed, raw is
    /// a few days deep and the hourlies legitimately hold weeks, so a daily converged "to raw" stops well short
    /// of its hourly and the gate stays correctly held while the verb reports DONE. Worse, a hierarchical daily
    /// added AFTER its hourly on such a store enters a hold NOTHING can clear: the pre-raw region exists only
    /// in the hourly, and a verb aiming at raw never targets it.</para>
    ///
    /// <para><c>SourceTimeColumn</c> follows from that: raw tables are keyed on <c>collection_time</c>,
    /// rollup views on <c>bucket</c>.</para>
    ///
    /// <para><b><c>BucketWidth</c> is carried EXPLICITLY, not inferred (#1849).</b> Until the corrected Query
    /// Store rollups existed, "hierarchical" and "daily" were the same fact, so the backfill derived a rollup's
    /// bucket width from whether its source time column was <c>bucket</c>. <see cref="QueryStoreStatsCorrectedHourlyView"/>
    /// breaks that: it is hierarchical (sourced from L1) but its buckets are HOURS. Inferring would have given
    /// its backfill a 24x-too-wide bucket, so every bucket count, slice count and disk estimate for it would
    /// have been silently wrong — an under-estimate, which is the one direction the preflight exists to
    /// prevent. Ordering still keys on the source column (raw-sourced rollups must be backfilled before the
    /// rollups that read them); only the width became its own column.</para>
    /// </summary>
    public static readonly (string View, string RawTable, string Source, string SourceTimeColumn, TimeSpan BucketWidth)[] RollupViews =
    {
        (QueryStatsHourlyView, "query_stats", "query_stats", "collection_time", HourlyBucket),
        (QueryStatsDailyView, "query_stats", QueryStatsHourlyView, "bucket", DailyBucket),
        (QueryStatsDbHourlyView, "query_stats", "query_stats", "collection_time", HourlyBucket),
        (QueryStatsDbDailyView, "query_stats", QueryStatsDbHourlyView, "bucket", DailyBucket),
        (ProcedureStatsHourlyView, "procedure_stats", "procedure_stats", "collection_time", HourlyBucket),
        (ProcedureStatsDailyView, "procedure_stats", ProcedureStatsHourlyView, "bucket", DailyBucket),
        (QueryStoreStatsHourlyView, "query_store_stats", "query_store_stats", "collection_time", HourlyBucket),
        (QueryStoreStatsDailyView, "query_store_stats", QueryStoreStatsHourlyView, "bucket", DailyBucket),

        /* The corrected Query Store rollups (#1849). L1 is raw-sourced; BOTH corrected views read L1 — the
           daily is L1's second child, not the corrected hourly's, so it converges to L1 like its sibling. */
        (QueryStoreStatsIntervalHourlyView, "query_store_stats", "query_store_stats", "collection_time", HourlyBucket),
        (QueryStoreStatsCorrectedHourlyView, "query_store_stats", QueryStoreStatsIntervalHourlyView, "bucket", HourlyBucket),
        (QueryStoreStatsCorrectedDailyView, "query_store_stats", QueryStoreStatsIntervalHourlyView, "bucket", DailyBucket),

        /* The day-grain daily and its dedup layer (#1869) — L1's THIRD child, and the first rollup in this
           list whose own source is itself hierarchical. Both are DAY-bucketed, which is why the explicit
           BucketWidth above is what keeps the backfill honest here as well. */
        (QueryStoreStatsIntervalDailyView, "query_store_stats", QueryStoreStatsIntervalHourlyView, "bucket", DailyBucket),
        (QueryStoreStatsDayGrainDailyView, "query_store_stats", QueryStoreStatsIntervalDailyView, "bucket", DailyBucket),
    };

    /// <summary>The three raw tables the rollups roll up, in coverage-probe order (deduplicated
    /// <see cref="RollupViews"/>).</summary>
    public static readonly string[] RolledRawTables = { "query_stats", "procedure_stats", "query_store_stats" };

    /// <summary>
    /// How far back each rollup has actually MATERIALIZED, and how far back each raw table still reaches —
    /// the input <see cref="RetentionTierRouter"/> needs to stop routing a window at a rollup that cannot
    /// answer it (#1759).
    ///
    /// <para>The mechanism this exists for: a continuous aggregate created <c>WITH NO DATA</c> over
    /// pre-existing history serves ONLY what was materialized. Real-time aggregation cannot rescue it —
    /// the watermark is a hard partition (materialized below <c>UNION ALL</c> raw at-or-above), so raw
    /// older than the watermark is excluded by construction, not merely un-accelerated. Every rollup's
    /// refresh policy starts 3 days back, so on a store that existed before its rollups the materialized
    /// span begins at roughly creation-minus-3-days and NEVER reaches further back on its own.</para>
    ///
    /// <para><b><c>to_regclass</c>-safe by construction, not by guard.</b> A relation named in a statement
    /// is resolved at PARSE time, so no in-statement <c>to_regclass</c> test can keep <c>min(bucket)</c>
    /// off a view that does not exist. Instead the SQL is BUILT from
    /// <paramref name="availability"/> — a view the <see cref="RollupProbeSql"/> round trip just proved
    /// absent contributes a literal <c>NULL</c> and is never named. Column count is fixed either way, so
    /// the reader's indexing does not depend on the store's shape.</para>
    ///
    /// <para><c>min(bucket)</c> is deliberately the SAME expression <see cref="RetentionArmSafetySql"/>
    /// gates arming on. Routing and arming must agree about what a rollup covers, or the router would
    /// serve a window the arming gate considers uncovered (or worse, the reverse).</para>
    /// </summary>
    public static string RollupCoverageProbeSql(RollupAvailability availability)
    {
        var columns = RollupViews
            .Select(r => availability.Has(r.View)
                ? $"(SELECT min(bucket) FROM collect.{r.View})"
                : "NULL::timestamp")
            /* The raw tables are migration-created and always exist, so they need no availability gate. */
            .Concat(RolledRawTables.Select(t => $"(SELECT min(collection_time) FROM collect.{t})"));

        return "SELECT " + string.Join(", ", columns);
    }

    /// <summary>
    /// Reads every rollup's materialized floor and every rolled raw table's oldest row
    /// (<see cref="RollupCoverageProbeSql"/>). <paramref name="availability"/> comes from
    /// <see cref="DetectRollupsAsync"/> in the same probe cycle and decides which relations are named at
    /// all.
    ///
    /// <para>NOTE that a DAILY rollup's floor is the day-FLOOR of its oldest hourly bucket, so it can read
    /// up to a day earlier than the hourly it is sourced from. That over-claims coverage by at most one
    /// bucket, and it is exactly the semantics the arming gate already runs on — matching it is the point.</para>
    /// </summary>
    public static async Task<RollupCoverage> DetectRollupCoverageAsync(
        NpgsqlDataSource dataSource, RollupAvailability availability, CancellationToken cancellationToken = default)
    {
        if (dataSource is null)
        {
            throw new ArgumentNullException(nameof(dataSource));
        }

        await using var command = dataSource.CreateCommand(RollupCoverageProbeSql(availability));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return RollupCoverage.Unknown;
        }

        var floors = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        for (var i = 0; i < RollupViews.Length; i++)
        {
            if (!await reader.IsDBNullAsync(i, cancellationToken))
            {
                floors[RollupViews[i].View] = reader.GetDateTime(i);
            }
        }

        var rawOldest = new Dictionary<string, DateTime>(StringComparer.Ordinal);
        for (var i = 0; i < RolledRawTables.Length; i++)
        {
            var ordinal = RollupViews.Length + i;
            if (!await reader.IsDBNullAsync(ordinal, cancellationToken))
            {
                rawOldest[RolledRawTables[i]] = reader.GetDateTime(ordinal);
            }
        }

        return new RollupCoverage(floors, rawOldest);
    }

    /// <summary>
    /// Converts every collector table to a hypertable (<see cref="HypertableTables"/> scope;
    /// <see cref="CreateHypertableSql"/> per table). Failure-isolated per table: one failed
    /// conversion warns and the sweep continues — that table stays a plain PG table, keeps
    /// working (COPY and DELETE-based retention are hypertable-agnostic), and is retried on the
    /// next service start. Returns the number of tables that converted (or no-op'd) cleanly.
    /// </summary>
    public static async Task<int> ConvertToHypertablesAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var converted = 0;
        foreach (var schema in HypertableTables)
        {
            try
            {
                using var command = new NpgsqlCommand(CreateHypertableSql(schema), connection) { CommandTimeout = SetupTimeoutSeconds };
                await command.ExecuteNonQueryAsync(cancellationToken);
                converted++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning("Hypertable conversion failed for {Table} — it stays a plain table: {Message}",
                    schema.TargetTable, ex.Message);
            }
        }

        logger?.LogInformation("TimescaleDB: {Converted}/{Total} collector table(s) are hypertables",
            converted, HypertableTables.Count);
        return converted;
    }

    /// <summary>
    /// Enables compression and adds the <see cref="CompressAfterDays"/>-day background policy on
    /// every collector table (both statements per table, failure-isolated per table — a table
    /// that failed hypertable conversion warns here too and stays uncompressed). Compressed
    /// chunks remain fully queryable: this is Darling's archival tier (see
    /// <see cref="CompressAfterDays"/>). Returns the number of tables with a policy in place.
    /// </summary>
    public static async Task<int> ApplyCompressionPolicyAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var applied = 0;
        foreach (var schema in HypertableTables)
        {
            try
            {
                using (var enable = new NpgsqlCommand(EnableCompressionSql(schema), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await enable.ExecuteNonQueryAsync(cancellationToken);
                }

                using (var policy = new NpgsqlCommand(AddCompressionPolicySql(schema), connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await policy.ExecuteNonQueryAsync(cancellationToken);
                }

                applied++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning("Compression policy failed for {Table} — it stays uncompressed: {Message}",
                    schema.TargetTable, ex.Message);
            }
        }

        logger?.LogInformation("TimescaleDB: compression policy ({Days}d) in place on {Applied}/{Total} collector table(s)",
            CompressAfterDays, applied, HypertableTables.Count);
        return applied;
    }

    /// <summary>
    /// Every COMPRESSION-policy job whose <c>schedule_interval</c> is not
    /// <see cref="CompressScheduleInterval"/> — the stores that need converging (#1778). The comparison is
    /// done by PostgreSQL against a typed <c>INTERVAL</c> literal rather than by string in C#, so
    /// <c>01:00:00</c> and <c>1 hour</c> compare equal instead of drifting on formatting.
    ///
    /// <para>Scoped to compression jobs the SAME tolerant way <see cref="ReadStuckCompressionJobsAsync"/> is
    /// (<c>policy_compression</c> plus the 2.18+ <c>columnstore</c> rebrand). Retention, continuous-aggregate
    /// refresh, reorder and every other job type are deliberately untouched: their cadences are separate
    /// decisions, and the retention jobs in particular carry an armed/paused state (#1680) this must never
    /// disturb.</para>
    /// </summary>
    public static string StaleCompressionScheduleSql =>
        $@"
SELECT
    j.job_id,
    j.hypertable_name,
    j.schedule_interval::text
FROM timescaledb_information.jobs AS j
WHERE (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')
AND   j.schedule_interval IS DISTINCT FROM INTERVAL '{CompressScheduleInterval}'";

    /// <summary>
    /// Retunes one existing compression policy to <see cref="CompressScheduleInterval"/>. The job id is BOUND
    /// as <c>$1</c> and cast <c>::integer</c> — <c>alter_job</c> takes <c>job_id INTEGER</c> and PostgreSQL does
    /// not down-cast bigint during function resolution, the #1586 trap that shipped once already. The interval
    /// is a compile-time constant, never user input, so it interpolates like every other literal here.
    ///
    /// <para>Only <c>schedule_interval</c> is passed; every other <c>alter_job</c> parameter defaults to NULL,
    /// which TimescaleDB reads as "leave unchanged" — so this cannot arm a paused job or alter what a policy
    /// considers eligible. Measured on 2.28.1: the change also re-anchors <c>next_start</c> immediately
    /// (a job sitting at last-finish + 12h moved to last-finish + 1h), so a converged store starts honoring the
    /// new cadence on the next tick rather than after one final half-day wait.</para>
    /// </summary>
    public static string SetCompressionScheduleSql =>
        $"SELECT alter_job($1::integer, schedule_interval => INTERVAL '{CompressScheduleInterval}')";

    /// <summary>
    /// Retunes EXISTING compression policies to <see cref="CompressScheduleInterval"/> (#1778) — the half of
    /// the tick fix that reaches stores which already have policies.
    ///
    /// <para>Without this the change would only ever help fresh installs: <see cref="AddCompressionPolicySql"/>
    /// carries the interval, but <c>if_not_exists => true</c> makes it a documented no-op against an existing
    /// policy (measured: returns -1, NOTICE, parameters untouched), so every store that ever ran an older build
    /// — the field store in #1778 among them — would keep waking twice a day forever. Idempotent by
    /// construction: it selects only the jobs that DIFFER, so the first start after deploy converges the store
    /// and every start after that finds nothing and logs nothing.</para>
    ///
    /// <para>Failure-isolated PER JOB, the #1775 shape: one <c>alter_job</c> that fails (most often because a
    /// least-privilege bring-your-own store's login does not own the job) leaves that one hypertable on its old
    /// cadence and the rest still converge. Returns how many it retuned.</para>
    /// </summary>
    public static async Task<int> ConvergeCompressionScheduleAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var stale = new List<(int JobId, string? Hypertable, string? Interval)>();
        try
        {
            using var probe = new NpgsqlCommand(StaleCompressionScheduleSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                stale.Add((
                    Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A plain-PostgreSQL store (the views do not exist) or a store hiccup. The caller already gates on
               the extension; nothing to converge either way. */
            logger?.LogDebug("Compression-schedule converge: could not read policy jobs: {Message}", ex.Message);
            return 0;
        }

        var converged = 0;
        foreach (var (jobId, hypertable, interval) in stale)
        {
            try
            {
                using var alter = new NpgsqlCommand(SetCompressionScheduleSql, connection) { CommandTimeout = SetupTimeoutSeconds };
                alter.Parameters.AddWithValue(jobId);
                await alter.ExecuteNonQueryAsync(cancellationToken);
                converged++;

                logger?.LogInformation(
                    "TimescaleDB: retuned {Hypertable}'s compression policy from a {Was} tick to {Now} — that is the longest an already-eligible chunk can now sit uncompressed.",
                    hypertable, interval, CompressScheduleInterval);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Could not retune {Hypertable}'s compression policy (job {JobId}) to a {Interval} tick — it keeps its {Was} cadence, so its newest closed chunk stays uncompressed longer (often a permission issue: the store login must own the job): {Message}",
                    hypertable, jobId, CompressScheduleInterval, interval, ex.Message);
            }
        }

        if (converged > 0)
        {
            logger?.LogInformation(
                "TimescaleDB: {Converged}/{Total} compression policies retuned to a {Interval} tick (#1778 — TimescaleDB's own default for 1-day chunks is 12 hours).",
                converged, stale.Count, CompressScheduleInterval);
        }

        return converged;
    }

    /// <summary>The V23 non-catalog hypertable: the per-run observability log. Bare name — the connection's
    /// <c>collect,config,public</c> search path resolves it to <c>collect.collection_log</c>, exactly like the
    /// collector tables' bare TargetTable names.</summary>
    public const string CollectionLogTable = "collection_log";

    /// <summary>collection_log's partition (prefix time) column.</summary>
    public const string CollectionLogTimeColumn = "collection_time";

    /// <summary>
    /// The AUTHORITATIVE conversion + compression of <c>collection_log</c> — a hypertable since V23, but OUTSIDE
    /// the collector catalog, so <see cref="ConvertToHypertablesAsync"/>/<see cref="ApplyCompressionPolicyAsync"/>
    /// (which iterate the catalog) never reach it. Called by the worker in the runtime TimescaleDB block, AFTER
    /// <see cref="TryEnableAsync"/> has created the extension — which is exactly why this, not the V23 migration,
    /// is authoritative: migrations run BEFORE <c>CREATE EXTENSION</c>, so a fresh store's V23 guard skips the
    /// conversion, and this heals it. Same three statements the collector tables get, via the raw-name overloads
    /// (<see cref="CreateHypertableSql(string, string)"/>: <c>migrate_data</c> moves any existing rows into
    /// chunks — the proven non-transactional path, so no migration-transaction risk; compression segments by
    /// <c>server_id</c> at <see cref="CompressAfterDays"/>). Idempotent (<c>if_not_exists</c>), so it re-converges
    /// every restart and no-ops a store the V23 migration already converted. Failure-isolated: a failure warns and
    /// collection_log stays a plain table — its DELETE-based retention (DarlingRetention) still honors the horizon.
    /// The long <see cref="SetupTimeoutSeconds"/> command timeout covers a large first <c>migrate_data</c>.
    /// </summary>
    public static async Task<bool> EnsureCollectionLogHypertableAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using (var convert = new NpgsqlCommand(CreateHypertableSql(CollectionLogTable, CollectionLogTimeColumn), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await convert.ExecuteNonQueryAsync(cancellationToken);
            }

            using (var enable = new NpgsqlCommand(EnableCompressionSql(CollectionLogTable), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await enable.ExecuteNonQueryAsync(cancellationToken);
            }

            using (var policy = new NpgsqlCommand(AddCompressionPolicySql(CollectionLogTable), connection) { CommandTimeout = SetupTimeoutSeconds })
            {
                await policy.ExecuteNonQueryAsync(cancellationToken);
            }

            logger?.LogInformation("TimescaleDB: collection_log is a hypertable with a {Days}d compression policy", CompressAfterDays);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "collection_log hypertable setup failed — it stays a plain table (DELETE-based retention still honors its horizon): {Message}",
                ex.Message);
            return false;
        }
    }

    /* ---------------- compression-job self-heal (#1581) ---------------- */

    /// <summary>
    /// The parameterized re-arm statement (#1581): reschedule a background job to run immediately, which
    /// un-sticks a job whose <c>next_start</c> has become <c>-infinity</c> (the scheduler will never re-fire
    /// it otherwise — the field-incident root cause). The job_id is ALWAYS bound as <c>$1</c>, never
    /// interpolated (the discipline is uniform with DarlingRetention's parameterized paths); <c>now()</c> is
    /// SQL, not a value. It is cast <c>$1::integer</c> because TimescaleDB's <c>alter_job</c> takes
    /// <c>job_id integer</c>, but <see cref="StuckCompressionJob.JobId"/> is a <c>long</c> that Npgsql sends as
    /// <c>bigint</c>; Postgres does NOT down-cast bigint→integer during function resolution, so an un-cast bind
    /// fails with <c>42883: function alter_job(bigint, ...) does not exist</c> (a real defect the gated-live
    /// test caught — a TimescaleDB job_id never exceeds int4, so the cast is always safe).
    /// </summary>
    public const string RearmJobSql = "SELECT alter_job($1::integer, next_start => now())";

    /* The stuck-Running bound floor: a compression run on a single day-chunk of 1-minute-cadence data
       finishes in seconds-to-minutes, so a run still 'Running' past this floor (when it dominates
       2x the schedule interval) has hung. Kept generous so a genuinely long first-compression of a
       large adopted store is not false-flagged; next_start = -infinity (the dominant failure mode) is
       caught immediately regardless of this.

       RAISED 2h -> 6h WITH THE TICK (#1778), and the floor is now the only thing holding this bound up.
       The bound is max(2x schedule_interval, floor). While the interval was TimescaleDB's 12-hour default
       the first term dominated at 24h and the floor never bound anything; shortening the tick to 1 hour
       collapses that term to 2h, so without this the bound would silently tighten 24h -> 2h. That would be
       a regression rather than a fix: the SAME field box that reported #1778 measured a query_stats
       compression still running at 1h33m and characterized compressions as hours-long at ~16 MB/s, so a 2h
       bound would start flagging legitimately-running compressions as stuck and the #1581 self-heal would
       re-arm a job that was doing its job. 6h clears every legitimately long run observed in the field with
       real headroom while still detecting a hung run four times sooner than the accidental 24h did. */
    private static readonly TimeSpan s_stuckRunningFloor = TimeSpan.FromHours(6);

    /// <summary>
    /// The stuck-<c>Running</c> bound: <c>max(2x the schedule interval, a floor)</c>. A run legitimately in
    /// progress finishes well within twice its own cadence; crossing this bound means it hung. A missing or
    /// non-positive schedule interval falls back to the floor. Pure so the predicate pins directly.
    /// </summary>
    public static TimeSpan StuckRunningBound(TimeSpan? scheduleInterval)
    {
        if (scheduleInterval is TimeSpan interval && interval > TimeSpan.Zero)
        {
            var twice = interval + interval;
            return twice > s_stuckRunningFloor ? twice : s_stuckRunningFloor;
        }

        return s_stuckRunningFloor;
    }

    /// <summary>
    /// The pure stuck-compression-job decision (#1581). A compression policy job is STUCK when either:
    /// <list type="bullet">
    /// <item>its <c>next_start</c> is <c>-infinity</c> while the job is NOT currently running — the scheduler
    /// abandoned it and will NEVER re-fire it (the dead-job bug that let uncompressed data grow without bound
    /// until the disk filled), or</item>
    /// <item>it has been in the <c>Running</c> state since a <c>last_run_started_at</c> older than
    /// <see cref="StuckRunningBound"/> — a run that began long ago and never finished (a hung run).</item>
    /// </list>
    /// A job with neither condition is healthy and is NOT flagged. No I/O, so it pins directly with a
    /// controllable clock. Scoping to compression jobs happens in the query — this decides only "stuck".
    ///
    /// <para><b><c>-infinity</c> is ALSO the engine's mid-run marker</b>, measured live on TimescaleDB
    /// 2.x (pg17): from the moment the scheduler picks up a due job until its run completes,
    /// <c>job_stats.next_start</c> reads <c>-infinity</c> with <c>job_status = 'Running'</c>, and the real
    /// next start is only computed at completion. So <c>-infinity</c> alone cannot mean "dead" — an
    /// unconditioned first arm flagged every healthy job the check happened to catch mid-run, alerted it as
    /// stuck, and "self-healed" it with a pointless re-arm (the field's transient stuck→self-healed noise;
    /// the CI flake was the live test catching its own re-arm-triggered run). A running job is therefore
    /// left to the second arm, whose elapsed bound is what actually distinguishes a hung run from a
    /// healthy one.</para>
    ///
    /// <para>A <paramref name="lastRunStartedAtUtc"/> of <see cref="DateTime.MinValue"/> counts as NEVER RAN,
    /// not as "started in year 1" (#1760). <see cref="StuckCompressionJobsSql"/> already NULLIFs TimescaleDB's
    /// <c>-infinity</c> never-ran sentinel, so this is the second line of defence: the sentinel maps to
    /// MinValue through Npgsql, and any future caller reading the column un-guarded would otherwise compute a
    /// ~739,000-day elapsed that clears every bound and flag a healthy job on its very first run.</para>
    /// </summary>
    public static bool IsCompressionJobStuck(
        bool nextStartIsNegativeInfinity,
        string? jobStatus,
        DateTime? lastRunStartedAtUtc,
        TimeSpan? scheduleInterval,
        DateTime nowUtc,
        out string reason)
    {
        var isRunning = string.Equals(jobStatus, "Running", StringComparison.OrdinalIgnoreCase);

        if (nextStartIsNegativeInfinity && !isRunning)
        {
            reason = "next_start is -infinity — the scheduler will never run it again";
            return true;
        }

        if (isRunning
            && lastRunStartedAtUtc is DateTime startedUtc
            && startedUtc != DateTime.MinValue)
        {
            var bound = StuckRunningBound(scheduleInterval);
            var elapsed = nowUtc - startedUtc;
            if (elapsed > bound)
            {
                reason = string.Format(
                    CultureInfo.InvariantCulture,
                    "stuck in the Running state for {0:F0} minutes (over the {1:F0}-minute bound) — the run hung and never finished",
                    elapsed.TotalMinutes, bound.TotalMinutes);
                return true;
            }
        }

        reason = "";
        return false;
    }

    /// <summary>
    /// The stuck-compression-job detection query, exposed as a const (like <see cref="RearmJobSql"/>) so the
    /// gated live test can settle on and pin THIS text rather than a hand-copied paraphrase that drifts.
    ///
    /// <para>Both <c>-infinity</c> tests run IN SQL, never through Npgsql's infinity-to-DateTime mapping.
    /// For <c>next_start</c> that is a correctness guard on the comparison. For <c>last_run_started_at</c> it
    /// is load-bearing (#1760): TimescaleDB stores <b>-infinity</b>, not NULL, as the never-ran sentinel in
    /// <c>_timescaledb_internal.bgw_job_stat.last_start</c>, and Npgsql maps that to
    /// <see cref="DateTime.MinValue"/> — so an un-guarded read turned "this job has never run" into "this run
    /// started in year 1", i.e. an elapsed of ~739,000 days that clears every <see cref="StuckRunningBound"/>.
    /// <c>NULLIF</c> restores the intended meaning: no start time, so the stuck-Running arm cannot fire.</para>
    ///
    /// <para>Why that was reachable at all: <c>job_status</c> and <c>last_run_started_at</c> come from
    /// INDEPENDENT sources in TimescaleDB's own view — <c>job_status</c> is
    /// <c>CASE WHEN pg_stat_activity.state = 'active' THEN 'Running'</c>, joined on <c>application_name</c>,
    /// while <c>last_run_started_at</c> is <c>bgw_job_stat.last_start</c>. A job's FIRST run therefore reads
    /// <c>Running</c> while its start time is still the sentinel, and that window flagged a perfectly healthy
    /// job as stuck — which the self-heal then "fixed" by re-arming a job that was running fine.</para>
    /// </summary>
    public const string StuckCompressionJobsSql = @"
SELECT
    js.job_id,
    (js.next_start = '-infinity'::timestamptz)  AS next_start_neg_infinity,
    js.job_status,
    NULLIF(js.last_run_started_at, '-infinity'::timestamptz) AS last_run_started_at,
    EXTRACT(EPOCH FROM j.schedule_interval)     AS schedule_interval_seconds,
    j.hypertable_name
FROM timescaledb_information.job_stats AS js
JOIN timescaledb_information.jobs      AS j USING (job_id)
WHERE j.proc_name LIKE '%compression%'
   OR j.proc_name LIKE '%columnstore%'";

    /// <summary>
    /// Reads every COMPRESSION-policy background job (<c>proc_name</c> is <c>policy_compression</c>, or the
    /// 2.18+ columnstore rebrand's name — the same tolerant LIKE the compression test uses) and returns the
    /// ones the pure <see cref="IsCompressionJobStuck"/> predicate flags as stuck. The <c>-infinity</c> tests
    /// run IN SQL (see <see cref="StuckCompressionJobsSql"/>); the stuck-Running bound is computed in C# from
    /// the raw fields. Scoped to compression jobs ONLY — retention, continuous-aggregate refresh, reorder, and
    /// every other job type are untouched. Failure-isolated: a store hiccup, or the views being absent (a
    /// plain-PostgreSQL store — the caller also gates on the extension), yields an empty list and a Debug line,
    /// never a throw.
    /// </summary>
    public static async Task<IReadOnlyList<StuckCompressionJob>> ReadStuckCompressionJobsAsync(
        NpgsqlConnection connection, DateTime nowUtc, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var stuck = new List<StuckCompressionJob>();
        try
        {
            using var command = new NpgsqlCommand(StuckCompressionJobsSql, connection);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                long jobId = Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture);
                bool negInfinity = !reader.IsDBNull(1) && reader.GetBoolean(1);
                string? jobStatus = reader.IsDBNull(2) ? null : reader.GetString(2);
                DateTime? lastRunStartedAt = reader.IsDBNull(3)
                    ? null
                    : DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc);
                TimeSpan? scheduleInterval = reader.IsDBNull(4)
                    ? null
                    : TimeSpan.FromSeconds(Convert.ToDouble(reader.GetValue(4), CultureInfo.InvariantCulture));
                string? hypertable = reader.IsDBNull(5) ? null : reader.GetString(5);

                if (IsCompressionJobStuck(negInfinity, jobStatus, lastRunStartedAt, scheduleInterval, nowUtc, out var reason))
                {
                    stuck.Add(new StuckCompressionJob(jobId, hypertable, reason));
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The views are absent (a plain-PG store or the extension was removed) or the store hiccuped —
               no signal this check. The caller already gates on the extension; this is belt-and-suspenders. */
            logger?.LogDebug("Compression-job health check: could not read job stats: {Message}", ex.Message);
        }

        return stuck;
    }

    /// <summary>
    /// Every background job's last-run duration against its own schedule interval (#2136) — the readings
    /// the Store Job Over Cadence self-alert judges. <c>job_stats</c> for the same reason the #1778
    /// observability path uses it (maintained unconditionally; the per-execution history table is empty
    /// unless job-execution logging is on). Only a SUCCESSFUL last run judges: a failed run's duration is
    /// not a cadence signal, and job failures are their own condition (<c>total_failures</c> rides the
    /// V56 telemetry). Tolerant like <see cref="ReadStuckCompressionJobsAsync"/> — a plain-PG store or a
    /// hiccup yields no readings, never an exception.
    /// </summary>
    public static async Task<IReadOnlyList<StoreJobCadenceReading>> ReadJobCadenceReadingsAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        const string sql = @"
SELECT
    j.job_id,
    j.proc_name || coalesce(' ' || j.hypertable_name, ''),
    (EXTRACT(EPOCH FROM js.last_run_duration) * 1000)::bigint,
    (EXTRACT(EPOCH FROM j.schedule_interval) * 1000)::bigint
FROM timescaledb_information.job_stats AS js
JOIN timescaledb_information.jobs AS j USING (job_id)
WHERE js.last_run_status = 'Success'";

        var readings = new List<StoreJobCadenceReading>();
        try
        {
            using var command = new NpgsqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                readings.Add(new StoreJobCadenceReading(
                    Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
                    reader.IsDBNull(1) ? "" : reader.GetString(1),
                    reader.IsDBNull(2) ? null : Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture),
                    reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug("Store-job cadence check: could not read job stats: {Message}", ex.Message);
        }

        return readings;
    }

    /// <summary>
    /// Every retention policy's ARMED state against the consequence of it being held (#2813) — the readings
    /// the Retention Held self-alert judges, and the live block <c>get_store_metrics</c> reports.
    ///
    /// <para><b>Why this is read live rather than taken from the V56 job telemetry.</b>
    /// <see cref="StoreSelfMetrics.BackgroundJobInsertSql"/> records <c>total_runs</c> and
    /// <c>total_failures</c> but NOT <c>j.scheduled</c>, so a policy the #1680/#1877 coverage gate has held
    /// reports <c>total_failures = 0</c> and a plausible last-run duration — byte-for-byte the shape of a
    /// healthy job, because it is not failing, it is paused. On the production store five
    /// <c>query_store_stats</c> policies sat held for 16 days and every stored metric read clean; the only
    /// signal was one WARNING per service start (#2809). Persisting <c>scheduled</c> into the series is the
    /// better long-term answer and needs a migration rung; this read makes the CURRENT state answerable
    /// without one, which is the half that would have caught the incident.</para>
    ///
    /// <para><b>Held is judged by its CONSEQUENCE, not by a timer.</b> Nothing records when a policy was
    /// paused, so hold duration is not directly knowable. The data span past the policy's own horizon is
    /// the same signal measured at the other end, and it is strictly better: it is what an operator checks
    /// by hand, it is the number that makes the cost legible (4 days configured against 18 days actual),
    /// and it self-scales — a policy paused an hour ago on a young store sits at ~1x its horizon and says
    /// nothing, while one held long enough to matter climbs without bound. That is why a freshly created
    /// policy, which <see cref="EnsureRetentionPoliciesAsync"/> deliberately creates PAUSED, raises nothing.</para>
    ///
    /// <para>The span comes from <c>timescaledb_information.chunks</c>, never from the hypertable — the
    /// oldest chunk's <c>range_start</c> is catalog metadata, so this stays a catalog round trip on a
    /// multi-hundred-GB table instead of a scan. <c>range_start</c> is declared <c>timestamptz</c> even for
    /// the naive-<c>timestamp</c> partitioning column every collector table uses, so it is normalized with
    /// <c>AT TIME ZONE 'UTC'</c>: verified byte-identical under UTC, UTC+14 and UTC-7 sessions rather than
    /// assumed. Tolerant like <see cref="ReadJobCadenceReadingsAsync"/> — a plain-PG store or a hiccup
    /// yields no readings, never an exception.</para>
    /// </summary>
    public static async Task<IReadOnlyList<RetentionHoldReading>> ReadRetentionHoldReadingsAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        const string sql = @"
SELECT
    j.job_id,
    coalesce(j.hypertable_name, ''),
    j.scheduled,
    coalesce(j.config->>'drop_after', ''),
    c.chunk_count,
    EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - (c.oldest_range_start AT TIME ZONE 'UTC')))::bigint,
    CASE
        WHEN (j.config->>'drop_after') IS NULL THEN NULL
        ELSE EXTRACT(EPOCH FROM (j.config->>'drop_after')::interval)::bigint
    END
FROM timescaledb_information.jobs AS j
LEFT JOIN LATERAL (
    SELECT
        min(ch.range_start) AS oldest_range_start,
        count(*)::bigint AS chunk_count
    FROM timescaledb_information.chunks AS ch
    WHERE ch.hypertable_schema = j.hypertable_schema
      AND ch.hypertable_name   = j.hypertable_name
) AS c ON true
WHERE j.proc_name = 'policy_retention'";

        var readings = new List<RetentionHoldReading>();
        try
        {
            using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                readings.Add(new RetentionHoldReading(
                    Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
                    reader.IsDBNull(1) ? "" : reader.GetString(1),
                    !reader.IsDBNull(2) && reader.GetBoolean(2),
                    reader.IsDBNull(3) ? "" : reader.GetString(3),
                    reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture),
                    reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                    reader.IsDBNull(6) ? null : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug("Retention-hold check: could not read retention policy state: {Message}", ex.Message);
        }

        return readings;
    }

    /* ---------------- compression-run observability (#1778) ---------------- */

    /// <summary>
    /// Per-hypertable compression activity: what each policy is doing right now, how long its last completed
    /// run took, and how many eligible chunks are still waiting (#1778).
    ///
    /// <para>THE DURATION SOURCE IS <c>job_stats</c>, NOT the per-execution history table. That is deliberate:
    /// <c>_timescaledb_internal.bgw_job_stat_history</c> only records successful executions when
    /// <c>timescaledb.enable_job_execution_logging</c> is ON, and it defaults to OFF — verified live on 2.28.1,
    /// where a completed run left that table empty. <c>timescaledb_information.job_stats</c> is maintained
    /// unconditionally, so it is the surface that actually reports on an untouched store.</para>
    ///
    /// <para>The backlog count is the number this whole issue is about: chunks that are CLOSED, already past
    /// the <see cref="CompressAfterDays"/> eligibility delay, and still uncompressed. On a healthy store with a
    /// <see cref="CompressScheduleInterval"/> tick this is 0 almost always and briefly 1 after a chunk ages in;
    /// a number that stays high is the store falling behind, which is exactly what went unseen while the tick
    /// was half a day.</para>
    ///
    /// <para><c>last_run_started_at</c> is <c>NULLIF</c>'d against <c>-infinity</c> for the SAME reason
    /// <see cref="StuckCompressionJobsSql"/> does it (#1760): TimescaleDB stores <b>-infinity</b>, not NULL, as
    /// the never-ran sentinel, Npgsql maps that to <see cref="DateTime.MinValue"/>, and <c>job_status</c> comes
    /// from an INDEPENDENT source (<c>pg_stat_activity</c>) than the start time — so a policy's very FIRST run
    /// reads <c>Running</c> while its start is still the sentinel. Un-guarded, this observability path would
    /// report that healthy first run as having been going for ~739,000 days. The two queries were written in
    /// parallel branches and each was green on its own; this is the seam between them, not either side.</para>
    /// </summary>
    public static string CompressionActivitySql =>
        $@"
SELECT
    j.hypertable_name,
    js.job_status,
    NULLIF(js.last_run_started_at, '-infinity'::timestamptz) AS last_run_started_at,
    CASE
        WHEN js.last_successful_finish > NULLIF(js.last_run_started_at, '-infinity'::timestamptz)
        THEN EXTRACT(EPOCH FROM (js.last_successful_finish - NULLIF(js.last_run_started_at, '-infinity'::timestamptz)))
    END AS last_run_seconds,
    (
        SELECT count(*)
        FROM timescaledb_information.chunks AS c
        WHERE c.hypertable_schema = j.hypertable_schema
        AND   c.hypertable_name = j.hypertable_name
        AND   NOT c.is_compressed
        AND   c.range_end < now() - INTERVAL '{CompressAfterDays} days'
    ) AS eligible_uncompressed
FROM timescaledb_information.jobs      AS j
JOIN timescaledb_information.job_stats AS js USING (job_id)
WHERE j.proc_name LIKE '%compression%'
   OR j.proc_name LIKE '%columnstore%'";

    /// <summary>
    /// Reads <see cref="CompressionActivitySql"/>. Failure-isolated to an empty list the same way
    /// <see cref="ReadStuckCompressionJobsAsync"/> is — observability must never be able to break the sweep
    /// that carries it.
    /// </summary>
    public static async Task<IReadOnlyList<CompressionActivity>> ReadCompressionActivityAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var activity = new List<CompressionActivity>();
        try
        {
            using var command = new NpgsqlCommand(CompressionActivitySql, connection);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                activity.Add(new CompressionActivity(
                    reader.IsDBNull(0) ? null : reader.GetString(0),
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                    reader.IsDBNull(3)
                        ? null
                        : TimeSpan.FromSeconds(Convert.ToDouble(reader.GetValue(3), CultureInfo.InvariantCulture)),
                    Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug("Compression-run activity: could not read job stats: {Message}", ex.Message);
        }

        return activity;
    }

    /// <summary>
    /// Logs <see cref="ReadCompressionActivityAsync"/>'s result at a level proportionate to what it says.
    ///
    /// <para>A store has one compression policy per hypertable — around forty — so logging all of them hourly
    /// at Information would bury the signal it exists to provide. Information is reserved for the two things an
    /// operator actually needs to see: a compression that is RUNNING right now and how long it has been going
    /// (the field's hours-long runs were invisible while they happened), and a table whose eligible chunks are
    /// piling up. Everything else is one Debug summary line.</para>
    /// </summary>
    public static void LogCompressionActivity(
        IReadOnlyList<CompressionActivity> activity, DateTime nowUtc, ILogger? logger)
    {
        if (activity is null)
        {
            throw new ArgumentNullException(nameof(activity));
        }

        if (logger is null || activity.Count == 0)
        {
            return;
        }

        var running = 0;
        var backlog = 0L;

        foreach (var item in activity)
        {
            if (item.IsRunning)
            {
                running++;
                logger.LogInformation(
                    "TimescaleDB: compression of {Hypertable} is RUNNING — {Minutes:F0} minute(s) so far, {Waiting} eligible chunk(s) still uncompressed.",
                    item.HypertableName, item.RunningFor(nowUtc)?.TotalMinutes ?? 0d, item.EligibleUncompressedChunks);
            }
            else if (item.EligibleUncompressedChunks > 0)
            {
                logger.LogInformation(
                    "TimescaleDB: {Hypertable} has {Waiting} chunk(s) past the {Days}d compression delay and still uncompressed; its policy wakes every {Interval} (last completed run took {Seconds:F0}s).",
                    item.HypertableName, item.EligibleUncompressedChunks, CompressAfterDays, CompressScheduleInterval,
                    item.LastRunDuration?.TotalSeconds ?? 0d);
            }

            backlog += item.EligibleUncompressedChunks;
        }

        if (running == 0 && backlog == 0)
        {
            logger.LogDebug(
                "TimescaleDB: {Count} compression policies on a {Interval} tick, nothing running, no eligible chunk uncompressed.",
                activity.Count, CompressScheduleInterval);
        }
    }

    /// <summary>
    /// Re-arms one stuck background job via the parameterized <see cref="RearmJobSql"/> (job_id BOUND). Returns
    /// true when <c>alter_job</c> succeeds; false (logged once, no throw) when it fails — most often because the
    /// store login does not OWN the job (a least-privilege bring-your-own store), which the service cannot fix
    /// itself. Cancellation propagates; every other failure degrades so a single un-re-armable job can never
    /// crash the health check or the sweep.
    /// </summary>
    public static async Task<bool> TryRearmJobAsync(
        NpgsqlConnection connection, long jobId, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using var command = new NpgsqlCommand(RearmJobSql, connection);
            command.Parameters.AddWithValue(jobId);
            await command.ExecuteNonQueryAsync(cancellationToken);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not re-arm compression job {JobId} via alter_job (often a permission issue — the store login must own the job): {Message}",
                jobId, ex.Message);
            return false;
        }
    }
}

/// <summary>
/// A COMPRESSION-policy background job that <see cref="TimescaleSupport.ReadStuckCompressionJobsAsync"/> flagged
/// as stuck (#1581): its immutable <c>job_id</c>, the hypertable it compresses (for a friendlier alert label —
/// may be null on an odd catalog), and the human-readable reason the pure predicate produced.
/// </summary>
public sealed record StuckCompressionJob(long JobId, string? HypertableName, string Reason);

/// <summary>
/// One background job's cadence reading (#2136): the last SUCCESSFUL run's duration against the job's own
/// schedule interval, from <see cref="TimescaleSupport.ReadJobCadenceReadingsAsync"/>. <see cref="JobName"/>
/// is <c>proc_name</c> plus the hypertable/CAGG it serves — the V56 telemetry's naming, minus the
/// <c>[job_id]</c> suffix (the id rides separately as the alert key).
/// </summary>
public sealed record StoreJobCadenceReading(long JobId, string JobName, long? LastRunDurationMs, long ScheduleIntervalMs);

/// <summary>
/// One retention policy's armed state and the consequence of it being held (#2813), from
/// <see cref="TimescaleSupport.ReadRetentionHoldReadingsAsync"/>.
///
/// <para><see cref="Armed"/> is <c>timescaledb_information.jobs.scheduled</c> — false means the #1680/#1877
/// coverage gate is holding this policy so it cannot drop history a consumer has never materialized.
/// <see cref="SpanSeconds"/> is now minus the OLDEST chunk's start (null when the hypertable has no chunks
/// yet) and <see cref="HorizonSeconds"/> is the policy's own <c>drop_after</c>, so
/// <see cref="OverHorizonRatio"/> is how many times its intended depth the tier is actually holding. That
/// ratio is the honest measure of a hold that has begun to cost something, and it is deliberately NOT a
/// hold duration: nothing records when a policy was paused, and a policy created paused moments ago on a
/// young store must read as unremarkable rather than as a 0-second-old incident.</para>
/// </summary>
public sealed record RetentionHoldReading(
    long JobId,
    string HypertableName,
    bool Armed,
    string DropAfter,
    long ChunkCount,
    long? SpanSeconds,
    long? HorizonSeconds)
{
    /// <summary>
    /// How many times its configured horizon this tier is actually holding — 4.5 on the production store
    /// that held 18 days under a 4-day policy. Null when either side is unknown or the horizon is
    /// non-positive: a ratio over an unmeasurable denominator is not a number, and reporting one would be
    /// the false-precision this whole issue is about.
    /// </summary>
    public double? OverHorizonRatio =>
        SpanSeconds is > 0 && HorizonSeconds is > 0
            ? SpanSeconds.Value / (double)HorizonSeconds.Value
            : null;
}

/// <summary>
/// One hypertable's compression-policy activity (#1778): whether a run is in progress, when it started, how
/// long the last COMPLETED run took, and how many chunks are past the eligibility delay but still uncompressed.
/// </summary>
public sealed record CompressionActivity(
    string? HypertableName,
    string? JobStatus,
    DateTime? LastRunStartedAtUtc,
    TimeSpan? LastRunDuration,
    long EligibleUncompressedChunks)
{
    /// <summary>Is a compression run in progress right now?</summary>
    public bool IsRunning => string.Equals(JobStatus, "Running", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// How long the in-progress run has been going, or null when nothing is running (or the store never
    /// recorded a start). Clamped at zero so clock skew between the store and the service cannot report a
    /// negative elapsed time.
    ///
    /// <para>A <see cref="LastRunStartedAtUtc"/> of <see cref="DateTime.MinValue"/> counts as NEVER RAN, not as
    /// "started in year 1" (#1760's lesson, applied to the query #1778 added).
    /// <see cref="TimescaleSupport.CompressionActivitySql"/> already NULLIFs TimescaleDB's <c>-infinity</c>
    /// never-ran sentinel, so this is the second line of defence — and note the zero-clamp above does NOT cover
    /// it: the sentinel produces a huge POSITIVE elapsed, which sails straight through a guard aimed at
    /// negatives.</para>
    /// </summary>
    public TimeSpan? RunningFor(DateTime nowUtc)
    {
        if (!IsRunning || LastRunStartedAtUtc is not DateTime startedUtc || startedUtc == DateTime.MinValue)
        {
            return null;
        }

        var elapsed = nowUtc - startedUtc;
        return elapsed > TimeSpan.Zero ? elapsed : TimeSpan.Zero;
    }
}

/// <summary>
/// Which retention rollups exist in a store (<see cref="TimescaleSupport.DetectRollupsAsync"/>): the
/// query-grain pair (query_stats_hourly / _daily — the Daily Summary and top-consumer readers), the
/// database-grain pair (query_stats_db_hourly / _daily — the FinOps database-resource reader), and the
/// composer's remaining catalog pairs (procedure_stats and query_store_stats, #1665 — the built-in tabs never
/// read those rollups, but <c>ComposeSourceRouter</c> routes onto all three tables' pairs). All false on a
/// plain-PostgreSQL store, where raw is complete anyway; per-flag on a TimescaleDB store so a
/// failure-isolated partial build degrades one tier instead of erroring (#1664).
/// </summary>
public readonly record struct RollupAvailability(
    bool QueryGrainHourly, bool QueryGrainDaily, bool DbGrainHourly, bool DbGrainDaily,
    bool ProcedureGrainHourly, bool ProcedureGrainDaily, bool QueryStoreGrainHourly, bool QueryStoreGrainDaily,
    bool QueryStoreIntervalHourly = false, bool QueryStoreCorrectedHourly = false, bool QueryStoreCorrectedDaily = false,
    bool QueryStoreIntervalDaily = false, bool QueryStoreDayGrainDaily = false)
{
    /// <summary>True when every rollup exists — the steady state on a TimescaleDB store, safe to cache
    /// permanently (a created continuous aggregate is never dropped outside the reshape sweep).</summary>
    public bool AllPresent =>
        QueryGrainHourly && QueryGrainDaily && DbGrainHourly && DbGrainDaily
        && ProcedureGrainHourly && ProcedureGrainDaily && QueryStoreGrainHourly && QueryStoreGrainDaily
        && QueryStoreIntervalHourly && QueryStoreCorrectedHourly && QueryStoreCorrectedDaily
        && QueryStoreIntervalDaily && QueryStoreDayGrainDaily;

    /// <summary>No rollups at all — the plain-PostgreSQL shape, and the safe fallback when a probe fails.</summary>
    public static RollupAvailability None => default;

    /// <summary>Every flag true — the fully-built TimescaleDB shape (and the test shorthand for it).</summary>
    public static RollupAvailability All => new(true, true, true, true, true, true, true, true, true, true, true, true, true);

    /// <summary>The pre-#1849 shape: every ORIGINAL rollup present, none of the corrected Query Store ones —
    /// i.e. a store whose service has not yet created them. The routing fallback's test shorthand.</summary>
    public static RollupAvailability WithoutCorrectedQueryStore => new(true, true, true, true, true, true, true, true);

    /// <summary>The #1849-era shape: the corrected rollups present, but not the #1869 day-grain daily pair —
    /// a store whose service predates this build. Its Query Store dailies must keep routing to the corrected
    /// daily, which is the degrade that lets #1869 ship with no migration either.</summary>
    public static RollupAvailability WithoutDayGrainQueryStore => new(true, true, true, true, true, true, true, true, true, true, true);

    /// <summary>
    /// Whether <paramref name="caggView"/> (an unqualified <c>collect.*</c> rollup view name — the strings the
    /// compose catalog carries, which are the <see cref="TimescaleSupport"/> view constants) exists in this
    /// store. Unknown names answer false: a view this probe never checked must be treated as absent, so a
    /// catalog entry added without extending the probe degrades to raw instead of routing blind (#1665).
    /// </summary>
    public bool Has(string caggView) => caggView switch
    {
        TimescaleSupport.QueryStatsHourlyView => QueryGrainHourly,
        TimescaleSupport.QueryStatsDailyView => QueryGrainDaily,
        TimescaleSupport.QueryStatsDbHourlyView => DbGrainHourly,
        TimescaleSupport.QueryStatsDbDailyView => DbGrainDaily,
        TimescaleSupport.ProcedureStatsHourlyView => ProcedureGrainHourly,
        TimescaleSupport.ProcedureStatsDailyView => ProcedureGrainDaily,
        TimescaleSupport.QueryStoreStatsHourlyView => QueryStoreGrainHourly,
        TimescaleSupport.QueryStoreStatsDailyView => QueryStoreGrainDaily,
        TimescaleSupport.QueryStoreStatsIntervalHourlyView => QueryStoreIntervalHourly,
        TimescaleSupport.QueryStoreStatsCorrectedHourlyView => QueryStoreCorrectedHourly,
        TimescaleSupport.QueryStoreStatsCorrectedDailyView => QueryStoreCorrectedDaily,
        TimescaleSupport.QueryStoreStatsIntervalDailyView => QueryStoreIntervalDaily,
        TimescaleSupport.QueryStoreStatsDayGrainDailyView => QueryStoreDayGrainDaily,
        _ => false,
    };
}

/// <summary>
/// One tier ladder's measured history: how far back the hourly rollup and the daily rollup have actually
/// MATERIALIZED, and how far back the RAW table underneath them still holds rows (#1759). The unit
/// <see cref="RetentionTierRouter.Resolve(DateTime, DateTime, bool, bool, TierCoverage)"/> routes on.
///
/// <para>Every field is nullable and null means EXACTLY ONE thing to the router: "no positive evidence".
/// A floor is null when the view is empty, when it does not exist, or when the probe failed — and all
/// three must behave identically, because the router only ever moves a window DOWN a tier on a positive
/// measurement that the lower tier reaches further back. That makes an unknown coverage state inert: the
/// age + availability ladder decides, exactly as it did before this existed.</para>
/// </summary>
public readonly record struct TierCoverage(DateTime? HourlyFloorUtc, DateTime? DailyFloorUtc, DateTime? RawOldestUtc)
{
    /// <summary>Nothing measured — the router falls back to the pure age + availability decision.</summary>
    public static TierCoverage Unknown => default;

    /// <summary>
    /// Does a tier whose materialized floor is <paramref name="floorUtc"/> hold the oldest point of a window
    /// starting at <paramref name="windowStartUtc"/>? A null floor covers NOTHING (see the type remarks) —
    /// which is the whole #1759 defect in one line: a rollup created <c>WITH NO DATA</c> answers only what it
    /// materialized, so a window below its floor comes back empty rather than falling through to raw.
    /// </summary>
    public static bool Covers(DateTime? floorUtc, DateTime windowStartUtc) =>
        floorUtc is DateTime floor && floor <= windowStartUtc;

    /// <summary>
    /// Does <paramref name="candidateFloorUtc"/> reach STRICTLY further back than <paramref name="floorUtc"/> —
    /// i.e. would routing there cover more of the window? A null candidate never wins (no evidence), and a null
    /// incumbent floor is beaten by any real measurement (a tier holding nothing loses to a tier holding
    /// something).
    ///
    /// <para>This asymmetry is the guard that keeps the #1759 fix from becoming its own regression. "Window
    /// starts before the rollup's floor" ALONE is not a reason to drop to raw: on a healthy store whose purges
    /// are armed, raw keeps ~4 days while the rollup keeps weeks, so a 30-day window on a 10-day-old store
    /// predates every floor and dropping to raw would return LESS. The fallback fires only where the lower tier
    /// is measurably deeper, which is precisely the held-purge shape #1759 describes.</para>
    /// </summary>
    public static bool ReachesFurtherBack(DateTime? candidateFloorUtc, DateTime? floorUtc) =>
        candidateFloorUtc is DateTime candidate && candidate < (floorUtc ?? DateTime.MaxValue);
}

/// <summary>
/// How far back every rollup in a store has actually materialized, and how far back each rolled RAW table
/// still reaches (<see cref="TimescaleSupport.DetectRollupCoverageAsync"/>) — the #1759 companion to
/// <see cref="RollupAvailability"/>'s "does it exist at all".
///
/// <para>Kept as a separate type rather than more fields on <see cref="RollupAvailability"/> for one
/// concrete reason: availability is a value that callers compare (<c>rollups == RollupAvailability.None</c>)
/// and cache PERMANENTLY once complete, because a created aggregate is never dropped. Coverage does the
/// opposite — it MOVES, backwards on a backfill and forwards on a retention drop — so it must be re-probed
/// on a cadence, and folding a mutable dictionary into a record struct would break the equality the
/// existing caches rely on.</para>
/// </summary>
public sealed class RollupCoverage
{
    private readonly IReadOnlyDictionary<string, DateTime> _floorsByView;
    private readonly IReadOnlyDictionary<string, DateTime> _oldestByRawTable;

    public RollupCoverage(
        IReadOnlyDictionary<string, DateTime> floorsByView,
        IReadOnlyDictionary<string, DateTime> oldestByRawTable)
    {
        _floorsByView = floorsByView ?? throw new ArgumentNullException(nameof(floorsByView));
        _oldestByRawTable = oldestByRawTable ?? throw new ArgumentNullException(nameof(oldestByRawTable));
    }

    /// <summary>Nothing measured — every lookup answers null, so the router keeps its pre-#1759 behaviour.
    /// The safe answer for a store with no rollups AND for a probe that failed.</summary>
    public static RollupCoverage Unknown { get; } = new(
        new Dictionary<string, DateTime>(StringComparer.Ordinal),
        new Dictionary<string, DateTime>(StringComparer.Ordinal));

    /// <summary>The oldest bucket <paramref name="caggView"/> has materialized, or null when it holds nothing
    /// (or was never probed). Mirrors <see cref="RollupAvailability.Has"/>: an unknown name answers null.</summary>
    public DateTime? FloorOf(string caggView) =>
        _floorsByView.TryGetValue(caggView, out var floor) ? floor : null;

    /// <summary>The oldest row <paramref name="rawTable"/> still holds, or null when it is empty (or was never
    /// probed).</summary>
    public DateTime? RawOldestOf(string rawTable) =>
        _oldestByRawTable.TryGetValue(rawTable, out var oldest) ? oldest : null;

    /// <summary>
    /// The tier ladder for one rollup pair: both floors plus the raw table underneath them, ready for
    /// <see cref="RetentionTierRouter.Resolve(DateTime, DateTime, bool, bool, TierCoverage)"/>. A pair with no
    /// daily view (or an unrecognized hourly view) still answers — with nulls, which the router treats as
    /// "no evidence".
    /// </summary>
    public TierCoverage For(string hourlyView, string? dailyView)
    {
        var rawTable = RawTableFor(hourlyView);
        return new TierCoverage(
            FloorOf(hourlyView),
            dailyView is null ? null : FloorOf(dailyView),
            rawTable is null ? null : RawOldestOf(rawTable));
    }

    /// <summary>The raw table a rollup view's tier ladder falls back TO, or null for a name outside
    /// <see cref="TimescaleSupport.RollupViews"/> (which answers "no evidence" rather than guessing).
    ///
    /// <para>Deliberately still RAW for every rollup, dailies included, and NOT the source relation #1798
    /// added alongside it. This answers a READ question — where does a window go when this rollup cannot serve
    /// it — and the answer is always the relation holding per-sweep rows. Only the BACKFILL's convergence
    /// target became source-relative.</para></summary>
    public static string? RawTableFor(string caggView)
    {
        foreach (var (view, rawTable, _, _, _) in TimescaleSupport.RollupViews)
        {
            if (string.Equals(view, caggView, StringComparison.Ordinal))
            {
                return rawTable;
            }
        }

        return null;
    }
}
