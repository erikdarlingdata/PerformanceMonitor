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

    /// <summary><see cref="TimeSpan"/> twin of <see cref="CompressScheduleInterval"/>, for callers comparing a
    /// job's cadence numerically instead of by text — <c>01:00:00</c> and <c>1 hour</c> are the same interval
    /// and must never read as a difference (that would re-alter the same job on every service start).
    ///
    /// <para>Cross-checked against <see cref="CompressScheduleInterval"/> by PARSING that literal, not by
    /// pinning each side to its own constant: two independent pins force the edit on whichever side the
    /// editor is looking at and force nothing on the other, and a divergence here is not cosmetic —
    /// <see cref="ConvergeCompressionScheduleAsync(NpgsqlConnection, ILogger, CancellationToken)"/> takes its
    /// target seconds from THIS while the policy is created with the STRING, so the two disagreeing makes
    /// every job read stale forever. Raised by review.</para></summary>
    public static readonly TimeSpan CompressScheduleSpan = TimeSpan.FromHours(1);

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
            "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')", connection) { CommandTimeout = SetupTimeoutSeconds };
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
            using var create = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS timescaledb", connection) { CommandTimeout = SetupTimeoutSeconds };
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
    ///
    /// <para>That verified signature is also where <c>initial_start</c> comes from: the statement names
    /// it, which puts the job on this hypertable's slot on the <see cref="CompressionPhaseMinutes"/> grid
    /// and on a FIXED schedule (#3035). The <c>-1</c> skip above applies to that too — it keys on the policy
    /// EXISTING, not on its parameters matching — so the phase reaches a deployed store through the converge
    /// and through nothing else.</para>
    /// </summary>
    public static string AddCompressionPolicySql(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        return AddCompressionPolicySql(schema.TargetTable);
    }

    /// <summary>
    /// The raw-name compression-policy overload — the collection_log path (see
    /// <see cref="CreateHypertableSql(string, string)"/>), and the one that carries the phase.
    ///
    /// <para><b><c>initial_start</c> is named for every hypertable this product owns, and that is what puts
    /// the job on a FIXED schedule (#3035).</b> Without it TimescaleDB computes each next start from the
    /// previous FINISH, so a compression policy drifts through the hour by its own runtime every cycle and
    /// crosses each fixed refresh slot in turn, spending hours to days inside one before drifting out. A
    /// table outside <see cref="CompressionPhaseOrder"/> gets the statement without it: this code has no
    /// business deciding when a foreign hypertable compresses.</para>
    ///
    /// <para><b>This resolves a bare name where the converge additionally checks the schema, and the
    /// asymmetry is a caller invariant rather than an oversight.</b> A statement takes a table name and has
    /// no schema to check — <c>add_compression_policy</c> resolves it through the session's search path, the
    /// same way every other bare name in this file does. So the safety rests on WHO calls it, and both
    /// callers pass names this product owns: <see cref="ApplyCompressionPolicyAsync"/> walks
    /// <see cref="HypertableTables"/> and <see cref="EnsureCollectionLogHypertableAsync"/> passes
    /// <see cref="CollectionLogTable"/>. The converge has a schema available because it reads one back from
    /// the job catalog, where the rows are whatever the store contains rather than whatever this product
    /// created, so it checks. A future caller handing this overload a foreign or qualified name would break
    /// that invariant and get a phase it should not have. Raised by review.</para>
    /// </summary>
    public static string AddCompressionPolicySql(string table)
    {
        var initialStart = TryCompressionPhaseMinutesFor(table, out var phase)
            ? $", initial_start => date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 hour' + INTERVAL '{phase.ToString(CultureInfo.InvariantCulture)} minutes'"
            : string.Empty;

        return $"SELECT add_compression_policy('{table}', compress_after => INTERVAL '{CompressAfterDays} days', schedule_interval => INTERVAL '{CompressScheduleInterval}', if_not_exists => true{initialStart})";
    }

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
    /// the aggregates are created WITH NO DATA and their refresh policy only re-materializes
    /// <see cref="HourlyRefreshStartOffset"/>, so left alone they would hold roughly one day for a
    /// thirty-day question -- far less than the four-day raw horizon the change exists to escape. The provider is repointed at them, so an un-backfilled deploy
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
                {
                    /* Kind-Unspecified so Npgsql sends `timestamp`, not `timestamptz`: the horizon is
                       compared against collection_time, which is naive UTC. */
                    probe.Parameters.AddWithValue(
                        DateTime.SpecifyKind(DateTime.UtcNow - BaselineRetentionSpan, DateTimeKind.Unspecified));

                    await using var reader = await probe.ExecuteReaderAsync(cancellationToken);

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
    ///
    /// <para>THE HORIZON IS BOUND AS <c>$1</c>, not computed in the SQL. <c>now()::timestamp</c> is
    /// effectively <c>LOCALTIMESTAMP</c> — it renders the clock in the store session's TimeZone, which
    /// initdb takes from the host OS — while <c>min(collection_time)</c> is naive UTC. Both sides are
    /// <c>timestamp</c>, so PostgreSQL raises nothing and <c>GREATEST</c> simply picks between two values on
    /// different clocks: on a store whose host sits east of UTC the horizon moves LATER and the gate
    /// under-asks for coverage, leaving the baseline tier permanently short of the window #1757 needs, and
    /// west of UTC it over-asks and materializes buckets the tier's own retention policy then drops. The
    /// caller passes <see cref="BaselineRetentionSpan"/> off the service clock, the same clock that stamped
    /// every <c>collection_time</c> it is compared against.</para>
    /// </summary>
    public static string BaselineBackfillProbeSql(string view, string source)
        => $@"
SELECT
    (SELECT min(collection_time) FROM collect.{source}) AS source_oldest,
    (SELECT min(bucket) FROM collect.{view}) AS coverage_oldest,
    time_bucket('1 hour', GREATEST(
        (SELECT min(collection_time) FROM collect.{source}),
        $1)) AS need_from";

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

    /// <summary>
    /// The non-baseline HOURLY continuous aggregates, in CREATION order, paired with their CREATE SQL.
    ///
    /// <para>Order is load-bearing twice over. <see cref="EnsureContinuousAggregatesAsync"/> builds them in
    /// this sequence because <see cref="CreateQueryStoreStatsCorrectedHourlySql"/> is hierarchical from
    /// <see cref="CreateQueryStoreStatsIntervalHourlySql"/> and a hierarchical aggregate cannot precede its
    /// source. And <see cref="HourlyRefreshPhaseOrder"/> derives each view's slot on the phase grid from its
    /// position here, so a reordering moves phases — which is why the collision guard checks the RESULT rather
    /// than trusting the list.</para>
    ///
    /// <para>Hoisted out of the ensure sweep (#3012) so that sweep, the phase order and the collision guard
    /// all read ONE list. Restating it in the guard would let the guard pass while the sweep drifted.</para>
    /// </summary>
    public static readonly (string CreateSql, string View)[] HourlyAggregates =
    {
        (CreateQueryStatsHourlySql,      QueryStatsHourlyView),
        (CreateProcedureStatsHourlySql,  ProcedureStatsHourlyView),
        (CreateQueryStoreStatsHourlySql, QueryStoreStatsHourlyView),
        (CreateQueryStatsDbHourlySql,    QueryStatsDbHourlyView),
        /* The corrected Query Store rollups (#1849): L1 is raw-sourced and MUST precede the corrected view,
           which is hierarchical from it. */
        (CreateQueryStoreStatsIntervalHourlySql,  QueryStoreStatsIntervalHourlyView),
        (CreateQueryStoreStatsCorrectedHourlySql, QueryStoreStatsCorrectedHourlyView),
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
    /// arming gate and outlive its consumers' refresh windows (<see cref="HourlyRefreshStartOffset"/> for
    /// the corrected hourly, <see cref="DailyRefreshStartOffset"/> for the corrected daily).</para>
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
    /// The HOURLY refresh window: each hourly continuous aggregate re-materializes <c>[now - 1 day,
    /// now - 1 hour]</c> on every run.
    ///
    /// <para><b>1 day rather than 3 (#3012).</b> A refresh's cost is set by the WINDOW it re-scans, not by the
    /// rows that arrived in it — so a 3-day window against <see cref="RawRetentionInterval"/>'s 4 days
    /// re-materialized roughly three quarters of the whole hypertable every hour, and that cost grew with the
    /// hypertable rather than with ingest. Measured on the production store: the heaviest hourly refresh
    /// (<see cref="QueryStoreStatsIntervalHourlyView"/>) ran 3,301-6,330 s against a 1-hour cadence —
    /// <b>118-175% of its own schedule interval</b> — while rows arriving per hour FELL ~3x over the same
    /// period. Narrowed to 1 day the same refresh finishes <b>inside one phase slot</b> — by 364 s of 1,260
    /// against #3166's census and #3174's re-derived window — and the figure
    /// with its derivation is on <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> rather than
    /// restated here — a percentage of cadence written twice goes stale in one of the two places. The
    /// direction of that measurement is the whole argument: duration tracking window size while ingest moves the other way is
    /// what rules out "more rows to materialize" and rules in "too much window".</para>
    ///
    /// <para><b>Why a job that outran its cadence could not recover.</b> A refresh holds
    /// <c>AccessShareLock</c> on the hypertable it reads; the compression policy on that same hypertable
    /// queues an <c>AccessExclusiveLock</c> request behind it; and a queued exclusive request blocks every
    /// SUBSEQUENT shared request — so collector store-writes and readers piled up behind a lock that was
    /// merely QUEUED, not held, even though their own locks are mutually compatible. At >100% of cadence the
    /// next run started into the tail of the previous one, and the convoy sustained itself. Below cadence it
    /// cannot form, which is why this number and the phase grid
    /// (<see cref="LightRefreshStepMinutes"/>) are complements rather
    /// than alternatives: narrowing is what makes the heavy refresh finish, phasing is what keeps what
    /// remains of it out of everyone else's way.</para>
    ///
    /// <para><b>What still has to fit inside it, now stated as a relationship rather than left to a
    /// constant.</b> Two things need the window to reach back far enough. Live collectors only ever append
    /// current-time rows, so for them one refresh interval would do and a day is generous. The one writer of
    /// BACKDATED rows is <c>QueryStoreBackfill</c>, and its reach is now DERIVED from this span (minus one
    /// <see cref="HourlyRefreshScheduleInterval"/>, because the window slides forward between runs) instead of
    /// from the raw retention horizon. That inversion is the actual fix: before it, the refresh window had to
    /// cover a depth that retention chose, so every retention increase silently bought more refresh cost;
    /// after it, the refresh window is chosen against its own cadence and the backdating depth follows.</para>
    /// </summary>
    public const string HourlyRefreshStartOffset = "1 day";

    /// <summary><see cref="TimeSpan"/> twin of <see cref="HourlyRefreshStartOffset"/> for callers doing
    /// arithmetic against it — <c>QueryStoreBackfill.Horizon</c> is derived from this, so the backdating depth
    /// cannot be left behind when the window moves. Pinned equal to the string by
    /// TimescaleContinuousAggregateTests.</summary>
    public static readonly TimeSpan HourlyRefreshStartSpan = TimeSpan.FromDays(1);

    /// <summary>The hourly refresh cadence, and also the hourly <c>end_offset</c> — the still-filling current
    /// bucket is left unmaterialized so no run reworks it.</summary>
    public const string HourlyRefreshScheduleInterval = "1 hour";

    /// <summary><see cref="TimeSpan"/> twin of <see cref="HourlyRefreshScheduleInterval"/>. The refresh window
    /// slides forward by exactly this much between runs, which is why anything relying on "the next run will
    /// re-materialize what I just wrote" has to subtract it from
    /// <see cref="HourlyRefreshStartSpan"/> rather than using the span itself.</summary>
    public static readonly TimeSpan HourlyRefreshScheduleSpan = TimeSpan.FromHours(1);

    /// <summary>
    /// The DAILY refresh window, deliberately still 3 days.
    ///
    /// <para>The daily tier is NOT the tier #3012 was about and must not be "made consistent" with the hourly
    /// one. Its jobs run once a day, so a 3-day window is 3 days of scan against an 86,400-second cadence
    /// rather than against 3,600 — it was never near its own schedule interval, and it never appeared in the
    /// convoy. What it buys is the buffer <see cref="HourlyRetentionInterval"/> leans on: the hourly rollups
    /// are dropped at 90 days, and the daily refresh reaching 3 days back is what guarantees a drop can never
    /// outrun the aggregate meant to preserve that history. Narrowing this would trade nothing for a
    /// correctness risk.</para>
    /// </summary>
    public const string DailyRefreshStartOffset = "3 days";

    /// <summary><see cref="TimeSpan"/> twin of <see cref="DailyRefreshStartOffset"/>, pinned equal to the
    /// string and pinned STRICTLY BELOW <see cref="HourlyRetentionSpan"/> by
    /// TimescaleContinuousAggregateTests.</summary>
    public static readonly TimeSpan DailyRefreshStartSpan = TimeSpan.FromDays(3);

    /// <summary>The daily refresh cadence, and also the daily <c>end_offset</c>.</summary>
    public const string DailyRefreshScheduleInterval = "1 day";

    /// <summary>
    /// The width of the hourly refresh grid in minutes, taken from
    /// <see cref="HourlyRefreshScheduleSpan"/> rather than written as 60, so the grid and the cadence it
    /// tiles cannot disagree about how long an hour is.
    /// </summary>
    public static int MinutesInHourlyCadence => (int)HourlyRefreshScheduleSpan.TotalMinutes;

    /// <summary>
    /// THE HOURLY REFRESH GRID (#3012, re-derived at #3174) — stated as a method, because a table of
    /// minutes cannot be re-derived by the next reader and a method can.
    ///
    /// <para><b>The grid is three bands, read left to right across the hour.</b> First the LIGHT refreshes,
    /// one per minute, <see cref="LightRefreshStepMinutes"/> apart. Then <see cref="CompressionPhaseGuardMinutes"/>
    /// of clear, so the last of them has finished. Then the HEAVIEST refresh's window,
    /// <see cref="HeaviestRefreshWindowMinutes"/> wide, which no other refresh starts inside. Then the
    /// compression band, <see cref="CompressionPhaseBandMinutes"/> wide, which is the rest of the hour.
    /// Every boundary is derived from a measurement or from the catalog; nothing here is a chosen minute.</para>
    ///
    /// <para><b>Why the shape changed, and why no step change could have done it.</b> The old grid was a
    /// uniform <c>index % slots * step</c>, so a view's minute — and therefore whether it collided with
    /// another view — was a property of WHERE IT SAT IN A LIST. Thirteen policies over four slots means
    /// collisions by counting alone, and which policies collided depended on list order: the three
    /// <c>collect.query_stats</c> consumers sat at positions 0, 3 and 9, distinct only by accident, and
    /// inserting one aggregate ahead of the last of them would have put two back on the same minute. The map
    /// here is INJECTIVE over the whole list instead, so contention is structurally impossible rather than
    /// incidentally absent: no two hourly policies start on the same minute at all, whatever the order and
    /// whatever is inserted. Two independent facts also rule out simply widening the step. A wider step
    /// leaves FEWER residues, so the <c>query_stats</c> trio — all congruent mod 3 — collapses onto one
    /// minute at every step at or above 20, which is #3012's own convoy adjacency recreated by the change
    /// meant to prevent it. And <see cref="RefreshSlotPercentOfHourlyCadence"/> is 25 only while the hour
    /// divides into four, so every available wider step also moves a default that V57 has already applied to
    /// every live store.</para>
    ///
    /// <para><b>What the old configuration measured, kept because it is the only lock-wait evidence there
    /// is.</b> The production store's <c>query_store_stats</c> job family was moved to :00/:15/:30/:45 and
    /// the first full staggered cycle came back 26 s / 2 s / 864 s / 140 s with zero ungranted locks on it —
    /// a cycle that STRADDLES the narrowing boundary
    /// (<see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>), so its four figures record what the
    /// stagger did to lock waits and are not samples of the narrowed regime's cost. What that cycle
    /// establishes is that phasing removes the lock waits; it says nothing about which minutes, which is why
    /// moving them costs nothing it measured.</para>
    ///
    /// <para><b>Phasing alone is not the fix and neither is narrowing alone.</b>
    /// <see cref="HourlyRefreshStartOffset"/> is what brought the heavy refresh down from 6,330 s to
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>; the grid is what keeps what remains of it
    /// from being visible to anything else, because the refresh that is running is not the one a compression
    /// policy or a sibling refresh is about to want. Dropping either one reopens the door.</para>
    ///
    /// <para><b>One thing the hour cannot hold, said here so it is not attempted.</b> Serialising all
    /// thirteen — every policy finishing before the next starts — needs
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> plus twelve times
    /// <see cref="OtherHourlyRefreshObservedCeilingSeconds"/>, which is 3,617.6 s against a 3,600 s hour. It
    /// is 17.6 s short of possible, so the light refreshes DO overlap each other and the guarantee is
    /// distinct STARTS rather than disjoint runs. That is the guarantee #3012 needed — the convoy formed on a
    /// compression policy's queued <c>AccessExclusiveLock</c> arriving while a refresh held
    /// <c>AccessShareLock</c>, and two refreshes hold mutually compatible locks — and it is strictly stronger
    /// than the old grid delivered, where four policies including the heaviest all started on :00.</para>
    /// </summary>
    public const int LightRefreshStepMinutes = 1;

    /// <summary>
    /// The hourly refresh policies that are not <see cref="HeaviestHourlyRefreshView"/> — the ones that share
    /// the light band, counted from <see cref="HourlyRefreshPhaseOrder"/> rather than written down so
    /// registering an aggregate moves the grid instead of leaving a stale count beside it.
    /// </summary>
    public static int LightHourlyRefreshCount => HourlyRefreshPhaseOrder.Count - 1;

    /// <summary>
    /// The minute <see cref="HeaviestHourlyRefreshView"/>'s refresh starts on: past the last light refresh,
    /// by <see cref="CompressionPhaseGuardMinutes"/>.
    ///
    /// <para><b>The heaviest refresh goes AFTER the light band rather than at :00, and that placement is the
    /// decision rather than a layout preference.</b> The compression guard is one-sided
    /// (<see cref="CompressionPhaseGuardMinutes"/>), so a compression policy on the hour's last minute is
    /// still holding its <c>AccessExclusiveLock</c> when the next hour's grid opens. Whichever refresh opens
    /// the hour is the one that waits behind it. Opening with the light band puts that wait on a policy whose
    /// whole recorded ceiling is <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> and which has no
    /// window to fit inside, and leaves the heaviest refresh — the one whose runtime has to stay under
    /// <see cref="RefreshSlotWarningSeconds"/> — starting a full light band clear of the previous hour's
    /// compression.</para>
    /// </summary>
    public static int HeaviestRefreshStartMinute =>
        ((LightHourlyRefreshCount - 1) * LightRefreshStepMinutes) + CompressionPhaseGuardMinutes;

    /// <summary>
    /// The heaviest hourly refresh's window: what the hour has LEFT once the light band, its guard and the
    /// compression band are each at the width their own measurement asks for.
    ///
    /// <para><b>A remainder rather than a choice, and that is what settles where the hour's spare minutes
    /// go.</b> The light band's width follows from how many policies there are, the guard's from
    /// <see cref="OtherHourlyRefreshObservedCeilingSeconds"/>, and the compression band's from the catalog
    /// (<see cref="CompressionPhaseBandMinutes"/>). None of those has anything to gain from an extra minute —
    /// a guard that already clears the light ceiling clears it no better at seven minutes than at four, and
    /// the compression band spreads the same hypertables at the same
    /// <see cref="CompressionPhaseMaxPerMinute"/> anywhere from 24 minutes wide to 27. This window does gain:
    /// every minute here is 50 s of <see cref="RefreshSlotWarningSeconds"/> lead time on the one figure that
    /// has a measured growth series behind it. So the remainder lands where it changes an answer, without
    /// anyone choosing.</para>
    ///
    /// <para><b>It is not sized to fit the ceiling, and the difference matters.</b> Nothing above consults
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>. Whether the window the hour can spare is
    /// wide enough for the refresh that has to fit in it is an ASSERTION —
    /// <c>HeaviestHourlyRefreshObservedCeilingSeconds &lt; RefreshSlotWarningSeconds</c>, held by
    /// TimescaleSupportTests — so a ceiling that outgrows the hour goes red instead of quietly re-sizing the
    /// grid around itself. When it does go red the repair is fewer compression minutes, a cheaper refresh, or
    /// a longer cadence for this one aggregate; it is not a wider window, because there is none to take.</para>
    /// </summary>
    public static int HeaviestRefreshWindowMinutes =>
        MinutesInHourlyCadence - HeaviestRefreshStartMinute - CompressionPhaseBandMinutes;

    /// <summary>
    /// One refresh slot as a percent of the hourly cadence — the value #2136's Store Job Over Cadence warning
    /// knob ships as its default (<c>AlertsConfig.StoreJobCadenceWarnPercent</c>).
    ///
    /// <para><b>This was <c>100 / RefreshPhaseSlots</c>, and #3174 broke the derivation rather than moving
    /// the number.</b> The derivation needed the grid to be UNIFORM: "one slot" was a single width every
    /// policy shared, so a percent of cadence could name it. The grid is three bands of different widths now
    /// (see <see cref="LightRefreshStepMinutes"/>), and the tightest of them is one minute — so there is no
    /// single slot left for a percent of cadence to mean, and a knob derived from the tightest band would
    /// ship at 1% of cadence.</para>
    ///
    /// <para><b>And the number cannot move, which is why it is anchored here rather than re-derived
    /// elsewhere.</b> V57 added <c>config.config_alert_settings.store_job_cadence_warn_percent</c> with
    /// <c>DEFAULT 25</c>, and that rung has already run on every live store. The store column wins on a fresh
    /// store, so a C# seed that drifted from it would ship a default nobody chose — moving this figure means
    /// a new rung, deliberately not taken by re-deriving a grid. DarlingSelfAlertTests holds the seed and the
    /// V57 text equal for exactly this reason.</para>
    ///
    /// <para><b>What the old derivation bought is kept as an assertion, since it is the half that mattered.</b>
    /// The point of deriving it was that the alert must fire at or BEFORE the grid's precondition is false.
    /// At 25% of a 3,600 s cadence the knob lands on 900 s, inside
    /// <see cref="RefreshPhaseSlotSeconds"/> — so it still speaks first, and
    /// TimescaleContinuousAggregateTests asserts that ordering in SECONDS against the window rather than as a
    /// percent identity. What is GONE is the tightness half — the old
    /// <c>(this + 1) * RefreshPhaseSlots &gt; 100</c>, which said this was the LATEST value that still
    /// cleared one slot. That was a property of a uniform slot count and has nothing left to be tight
    /// against; the knob now fires earlier than it strictly has to, which is the safe direction.</para>
    ///
    /// <para><b>What this does NOT do.</b> It does not bound what an operator may SET. The knob stays
    /// clamped [5, 100] and a value above this one fires later — deliberately, because the knob judges
    /// families that have no window, and silently retuning a live fleet's setting would be a worse trade than
    /// the alert arriving late for one family. The heaviest refresh's window is watched independently of this
    /// knob, on the grid's own terms, by #3044's <see cref="RefreshSlotWarningSeconds"/> line — so raising
    /// the knob cannot leave the grid's precondition unattended, which is what makes leaving the clamp alone
    /// the cheaper trade.</para>
    /// </summary>
    public const int RefreshSlotPercentOfHourlyCadence = 25;

    /// <summary>
    /// The hourly refresh policies in phase order — the ONLY thing that decides which slot a policy gets, and
    /// it is keyed on the VIEW name.
    ///
    /// <para><b>Never on a job id.</b> TimescaleDB job ids are assigned per deployment: the ids that appear in
    /// #3012's evidence exist only on the store it was measured against, and would name entirely different
    /// jobs anywhere else. Keying the grid on view identity is what makes the same configuration reproducible
    /// on a store that has never seen those ids.</para>
    ///
    /// <para>Order matters and is asserted, because what the grid has to guarantee is that the policies
    /// sharing a hypertable land on DIFFERENT slots — that is the lock-queue adjacency the convoy formed on.
    /// The baseline aggregates are appended from <see cref="BaselineAggregates"/> rather than restated, so
    /// this list and that one cannot drift apart.</para>
    /// </summary>
    public static readonly IReadOnlyList<string> HourlyRefreshPhaseOrder =
        HourlyAggregates.Concat(BaselineAggregates).Select(a => a.View).ToArray();

    /// <summary>
    /// Every hourly continuous aggregate paired with the CREATE that defines it —
    /// <see cref="HourlyAggregates"/> then <see cref="BaselineAggregates"/>, in the same order
    /// <see cref="HourlyRefreshPhaseOrder"/> derives from.
    ///
    /// <para>Exists so the phase grid's real invariant can be checked against the SHIPPED DEFINITIONS rather
    /// than a second hand-written map. What the grid has to guarantee is that policies CONTENDING FOR THE SAME
    /// RELATION start at different minutes — that is the lock adjacency the convoy formed on — and a view's
    /// contended relation is whatever its own CREATE selects <c>FROM</c>. Recovering it from this text means a
    /// new aggregate is covered the moment it is registered, and a map that drifted from the definitions
    /// cannot report a false all-clear.</para>
    /// </summary>
    public static readonly IReadOnlyList<(string CreateSql, string View)> HourlyRefreshDefinitions =
        HourlyAggregates.Concat(BaselineAggregates).ToArray();

    /// <summary>
    /// Which minute of the hour <paramref name="view"/>'s hourly refresh policy starts on.
    ///
    /// <para><b>INJECTIVE, and that is the whole of the contention guarantee.</b>
    /// <see cref="HeaviestHourlyRefreshView"/> is answered by IDENTITY, not by position, and gets
    /// <see cref="HeaviestRefreshStartMinute"/> alone. Every other view gets its own consecutive minute in
    /// the light band, counted over the light views only. There is no modulus anywhere, so two policies
    /// cannot share a residue — the map has no collisions to have, at any list length the band can hold, in
    /// any order, with anything inserted anywhere. That is what makes contention structurally impossible
    /// instead of a property of where a view happens to sit in a list, which is the state #3174 replaced.</para>
    ///
    /// <para>Throws for a view that is not on <see cref="HourlyRefreshPhaseOrder"/> — including every DAILY
    /// view, which must not be dragged onto the grid. That is deliberately loud rather than defaulted: a new
    /// hourly aggregate that silently got minute 0 would be coincident with the first light refresh, which is
    /// the exact state #3012 was about. <see cref="EnsureContinuousAggregatesAsync"/> builds each policy
    /// statement inside its own per-aggregate try, so an unregistered view costs that one aggregate and names
    /// itself in the warning instead of taking the sweep down.</para>
    /// </summary>
    public static int RefreshPhaseMinutesFor(string view)
        => RefreshPhaseMinutesFor(HourlyRefreshPhaseOrder, view);

    /// <summary>
    /// The phase map itself, over an EXPLICIT order — the seam that lets the injectivity claim be tested
    /// against the shipped algorithm rather than against a copy of its rule.
    ///
    /// <para><b>Why this exists at all.</b> The claim the grid rests on is that no two hourly policies share
    /// a minute <i>whatever the order</i>, and the public overload can only ever be called at the ONE order
    /// <see cref="HourlyRefreshPhaseOrder"/> currently has. A test that permuted the list and re-implemented
    /// the counting rule inline would prove the RULE injective and leave the shipped method untested at every
    /// order but one — which is the "a test that agrees with any derivation" failure one layer down, and it
    /// is the failure this whole grid exists to remove. So the order is a parameter and the product passes
    /// its own list.</para>
    ///
    /// <para><b>Only the ORDER is a parameter, deliberately.</b> The GEOMETRY —
    /// <see cref="HeaviestRefreshStartMinute"/> and <see cref="LightRefreshStepMinutes"/> — still comes from
    /// the shipped registry, so this cannot be used to fabricate a different grid: handing it a permutation
    /// asks "does the map still collide-free at this order", which is the question, and handing it a
    /// different POPULATION would be asking something the caller has no business asking.</para>
    ///
    /// <para><c>internal</c> rather than public: the product must always reach the map through the overload
    /// that supplies its own list, or a caller could phase a policy against an order the converge does not
    /// share.</para>
    /// </summary>
    internal static int RefreshPhaseMinutesFor(IReadOnlyList<string> order, string view)
    {
        if (order is null)
        {
            throw new ArgumentNullException(nameof(order));
        }

        var lightIndex = 0;

        foreach (var candidate in order)
        {
            var heaviest = string.Equals(candidate, HeaviestHourlyRefreshView, StringComparison.Ordinal);

            if (string.Equals(candidate, view, StringComparison.Ordinal))
            {
                return heaviest
                    ? HeaviestRefreshStartMinute
                    : lightIndex * LightRefreshStepMinutes;
            }

            if (!heaviest)
            {
                lightIndex++;
            }
        }

        throw new ArgumentOutOfRangeException(
            nameof(view),
            view,
            "not an hourly continuous aggregate — register it in TimescaleSupport.HourlyRefreshPhaseOrder before giving it an hourly refresh policy");
    }

    /* ─────────────────────── the compression phase grid (#3035) ─────────────────────── */

    /// <summary>
    /// The hourly refresh the compression grid is placed AGAINST, named because its slot is the one no other
    /// background work may be scheduled inside.
    ///
    /// <para>On the narrowed <see cref="HourlyRefreshStartOffset"/> window it is still by far the largest job
    /// on the grid — see <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> for the number the grid is
    /// sized against and for the operating envelope that number defines.</para>
    ///
    /// <para><b>The asymmetry the grid's shape rests on, stated as the two relationships it needs rather than
    /// as a ratio (#3174).</b> Every other hourly refresh's recorded ceiling
    /// (<see cref="OtherHourlyRefreshObservedCeilingSeconds"/>) fits inside
    /// <see cref="CompressionPhaseGuardMinutes"/>, and this one's does not. That is why one refresh gets a
    /// window of its own that no compression minute may enter, while the rest are treated as occupied only
    /// for the guard band after their start. Both halves are asserted by TimescaleSupportTests, and both fail
    /// in the direction that matters: a light ceiling past the guard band leaves compression starting inside a
    /// running refresh, and a heaviest ceiling INSIDE the guard band would mean its window is excluded whole
    /// for no reason. It used to be stated as a ratio — "more than 4x", "under a quarter of it" — and that was
    /// the wrong pin twice over: the ratio was never what the geometry consumed, and at 896 s against 226.8 s
    /// it is 3.95x, so a guard written as <c>Other * 4 &lt; Heaviest</c> was red at every possible step
    /// while the geometry it was supposed to protect was fine.</para>
    /// </summary>
    public const string HeaviestHourlyRefreshView = QueryStoreStatsIntervalHourlyView;

    /// <summary>
    /// The runtime the compression grid is sized against for <see cref="HeaviestHourlyRefreshView"/>: the
    /// LARGEST run in the clean narrowed-window population, on the one store that carries this workload.
    ///
    /// <para><b>WHICH STATISTIC, OVER WHICH POPULATION — stated at the top because that is the sentence a
    /// later reader cites, and its absence is what let a narrower slice pass as the whole (#3182).</b> This
    /// is a MAXIMUM. Its population is the runs of this job's refresh policy that started AFTER the
    /// narrowing boundary named below, on one store, up to the instant named below. It is not a maximum over
    /// this job's whole recorded history, not a fleet figure, and not a percentile — the estimator paragraph
    /// re-takes that choice rather than assuming it. Every figure this comment draws is drawn against THAT
    /// statistic over THAT population, and a figure taken from a narrower slice of it — one day, one hour —
    /// is a DIFFERENT statistic even on the occasions when the two agree to the second.</para>
    ///
    /// <para><b>A maximum over a closed population is a LOWER BOUND on what the job does, and since #3182
    /// the product REPORTS when a live run falsifies it.</b> That is not a restatement of the slot watch and
    /// the two are not interchangeable: <see cref="ClassifyRefreshSlotHeadroom"/> asks whether a run fits
    /// the window the grid gives it and its remedy is re-deriving the grid, while
    /// <see cref="ClassifyRefreshCeilingFreshness"/> asks whether THIS NUMBER is still a maximum and its
    /// remedy is re-deriving this number. A run can be above this constant and comfortably inside the slot,
    /// which is exactly the state that carried the defect: the recorded ceiling was overtaken from inside
    /// the routine band, where the slot watch logs at Debug, so nothing anywhere said the constant had gone
    /// stale. <see cref="LogRefreshCeilingStaleness"/> is called beside that watch rather than inside its
    /// band switch, so the finding is reachable from the routine band too.</para>
    ///
    /// <para><b>THE DERIVATION, recorded as a method and not only as a value, because a value cannot be
    /// re-derived by the next reader and a method can (#3101).</b> POPULATION: runs of this job's refresh
    /// policy under the narrowed <see cref="HourlyRefreshStartOffset"/> window, each read from that store's
    /// per-run job history with an explicit <c>succeeded</c> column that said <c>t</c>. EXCLUSION RULE:
    /// every run that started at or before the narrowing boundary of <c>13:44:23</c> is out, whatever its
    /// duration — membership is decided by position against the boundary, never by whether a reading looks
    /// like it belongs. SPAN: the boundary day's remaining hours, plus the days after it. ESTIMATOR: the
    /// maximum. RefreshCeilingProvenancePinTests holds this constant equal to the maximum of the population
    /// published below and TimescaleSupportTests holds the grid clear of it, so the value and the method
    /// cannot drift apart without a red test.</para>
    ///
    /// <para><b>THE READ IS A CENSUS, and that is what changed (#3166).</b> Every run of this job since the
    /// boundary, one row per run, from <c>timescaledb_information.job_history</c> — the read this comment
    /// used to NAME as the one that would settle the sampling qualifier, now performed.
    /// <b>ONE STORE'S, and the scoping is a precondition of the READ rather than a caveat about the
    /// workload (#3175).</b> That view only records executions where
    /// <c>timescaledb.enable_job_execution_logging</c> is ON and it defaults to OFF. When this census was
    /// read the GUC was set only by the v1 <c>postgresql.conf</c> block, whose marker
    /// <c>EnsureConfAppended</c> finds already present on any pre-existing cluster — so a cluster predating
    /// the block could not be healed into it. Measured then, on two stores running the same binary: the
    /// older one had the GUC absent, effective <c>off</c>, <c>source = default</c>, and <b>1</b>
    /// <c>job_history</c> row for <b>110</b> jobs; the newer had <b>39020</b> rows. <b>A maximum over that
    /// view on such a store returns zero rows and reads as "no run exceeded the line".</b> Everything below
    /// is therefore the census of ONE store — the one that carries this workload and did have the GUC — and
    /// is not a fleet reading. <b>#3175/#3177 has since given the GUC its own marker, so existing stores
    /// heal; that does not widen this population, because the read predates the heal.</b> A later census
    /// could be broader, and would have to say so rather than inherit this one's scope. No SHIPPED read
    /// touches <c>job_history</c> (every product surface uses <c>job_stats</c>,
    /// deliberately — see <see cref="CompressionActivitySql"/>), so the gap is in what an investigation can
    /// ask, not in what the product reports. Read at <c>2026-09-08 01:37Z</c>, so the
    /// window is one that has ENDED and stays true rather than a scope read against a clock a doc comment
    /// does not have. Each side of the boundary, since a bound is only as good as what it excludes: <b>304
    /// runs</b> at or before it, median <b>1081.7 s</b>, maximum <b>13300.7 s</b>; <b>57 runs</b> after it,
    /// median <b>418.3 s</b>, maximum <b>896.1 s</b>. Not one post-boundary run failed or was left without a
    /// finish time, so the <c>succeeded</c> filter removes nothing from the span and the census is the whole
    /// of it rather than a status-selected part.</para>
    ///
    /// <para><b>THE POPULATION ITSELF, published so the estimator can be recomputed rather than taken on
    /// trust.</b> The boundary day's tail, every run that day after the boundary: <c>194, 222, 225, 335,
    /// 594, 465, 359, 293, 355</c> seconds. The days after it: <c>347, 342, 348, 286, 368, 362, 320, 252,
    /// 219, 225, 299, 376, 418, 546, 515, 473, 530, 523, 676, 778, 666, 705, 702, 599, 534, 479, 387, 373,
    /// 391, 413, 356, 336, 395, 418, 386, 460, 591, 722, 570, 812, 641, 739, 830, 871, 786, 896, 815,
    /// 681</c> seconds. Together <b>57 runs</b> spanning <b>194 s to 896 s</b>, totalling <b>27799 s</b>,
    /// median <b>418 s</b> — and <b>7</b> of them at or past <see cref="RefreshSlotWarningSeconds"/>, where
    /// the sixteen-run sample this population replaces had none.</para>
    ///
    /// <para><b>A TOTAL rather than a mean, and the reason is that a mean of this population is not
    /// exactly stateable.</b> 27799 s over 57 runs is 487.7017… s, so any one-decimal figure for it would be
    /// a rounded claim wearing an exact one's clothes — the same defect the occupancy figure below was
    /// restated to avoid. The total is exact, it is checked against the published list, and it makes every
    /// individual reading load-bearing in the same way the mean did: a single digit moved anywhere in the
    /// list changes it.</para>
    ///
    /// <para><b>MAXIMUM, NOT A PERCENTILE PLUS MARGIN — decided rather than defaulted, and the trade stated
    /// because the two answers diverge as the population grows.</b> What this constant sizes is a SCHEDULING
    /// exclusion, and the cost of undersizing it is one lock convoy (#3012's measured harm) that no later
    /// run amortises. A percentile is a statement about an ACCEPTED RATE OF EXCEEDANCE, and the rate this
    /// bound may accept over the record it is derived from is zero — so the estimator is the maximum. No
    /// margin is added on top either: the clearance is <see cref="RefreshPhaseSlotSeconds"/> minus this
    /// value, stated where it is used, so a reader sees a bound and its margin as two numbers instead of
    /// one padded one.</para>
    ///
    /// <para><b>THE SAMPLE-SIZE HALF OF THAT ARGUMENT HAS EXPIRED, which is exactly what it was written to
    /// do (#3101, #3166).</b> At sixteen readings the 95th percentile WAS the maximum by nearest rank, so a
    /// percentile bought no headroom and the two answers agreed. They no longer do: by nearest rank over
    /// <b>57 readings</b> the 95th percentile is <b>830 s</b> and the 90th is <b>786 s</b>, <b>66 s</b> and
    /// <b>110 s</b> below the maximum. So the decision is RE-TAKEN rather than inherited, and it comes out
    /// the same way on the half that never depended on the sample size: one lock convoy is not amortised by
    /// the runs that did fit, so a bound on a scheduling exclusion may accept no exceedance over its own
    /// record, and a 95th percentile is a promise to be wrong three times in every sixty runs. What is gone
    /// with the sample-size half is its EXPIRY: a reason that does not reference the population's size has
    /// nothing left to expire, so this paragraph now pins both percentiles and both gaps as readings that
    /// TRACK the population instead of as a coincidence that ends.</para>
    ///
    /// <para><b>What would change that answer, written down so it is not re-reasoned from scratch — and
    /// both of the things it named have now happened.</b> The two triggers recorded were a population large
    /// enough for a high percentile to sit meaningfully below the maximum, and this maximum ceasing to be a
    /// lower bound on the truth. The first fired at 57 readings and the estimator paragraph above re-takes
    /// the decision it forced. The second fired too, and it was a property of the READ rather than of the
    /// estimator: the sixteen-run record mixed a census of the boundary day's tail with a SAMPLE of the days
    /// after it, because the sweep that read it and the refresh that produced it tick on independent anchors
    /// (see <see cref="HeaviestRefreshRuntimeSql"/>), so that figure bounded the runs which happened to be
    /// OBSERVED. The census read named as the fix has been done and its result is this constant, so the
    /// qualifier is retired rather than restated.</para>
    ///
    /// <para><b>WHAT REPLACES IT IS A TREND, and that is a different kind of limitation from a sampling
    /// one.</b> Per closed day, the maximum of this job's runs went <b>594 s</b> over the boundary day's
    /// nine remaining runs, <b>778 s</b> over the twenty-two runs of the day after, and <b>896 s</b> over
    /// the twenty-four runs of the day after that. A census removes the "as observed" caveat and puts
    /// nothing in its place: the population is closed at a stated instant and it grows, so this maximum is
    /// the largest run the job has been RECORDED to make and not a ceiling on what it will make next. What
    /// that means for the grid is stated below and is deliberately not decided here — the value is a
    /// measurement, and the slot it has to fit inside is a scheduling choice.</para>
    ///
    /// <para><b>THE RUN THE EXCLUSION RULE REMOVES, recorded because a figure that looks like a ceiling and
    /// is not one is how the wrong number gets cited.</b> One run, <c>13:30:00</c> to <c>13:44:24</c> on the
    /// boundary day, 864 s, excluded for starting before the boundary. It is still <b>3.8x</b> faster than
    /// the fastest run the 3-day window ever produced (3,301 s, of a 3,301-6,330 s band), so it remains no
    /// sample of the pre-narrowing regime — but the other half of that argument is GONE. At <b>0.96x</b> of
    /// the largest reading in the population above it now sits INSIDE the post-boundary range instead of
    /// 1.45x past it, and an ordinary member of a distribution cannot be disqualified for belonging to
    /// neither. Which is why the rule is POSITIONAL and always was: this run is out because it STARTED
    /// before the boundary, and nothing about its duration does any of that work. The duration-based
    /// disqualification the sixteen-run record leaned on was an artefact of a population whose maximum was
    /// 302 s lower, and recording that it expired is the whole point of stating a rule rather than a
    /// verdict. Why it is that fast is NOT established — the plausible candidate is
    /// that the 6,330 s run before it had already cleared most of the backlog — and it is recorded as
    /// unexplained precisely so the low figure is not read as evidence that the narrowing had partly taken
    /// effect.</para>
    ///
    /// <para><b>ONE READING IS LEFT OPEN, and the derivation above does not rest on it: whether the boundary
    /// timestamp is independent of the excluded run's completion.</b> No independent record of the narrowing
    /// has been found — the boundary day's service log carries no <c>alter_job</c>, no <c>start_offset</c>,
    /// no <c>StartOffset</c>, no <c>refresh policy</c> and no <c>HourlyRefreshStartOffset</c> line, on a
    /// filter proved live by a positive control against the same file, so that is a real negative rather
    /// than a dead filter. The boundary therefore cannot have come from a service-log ALTER, which leaves
    /// the circular possibility live: it may have been read off the job history, plausibly off the excluded
    /// run's own completion a fraction of a second later, in which case the boundary and that run's
    /// exclusion are ONE OBSERVATION and cannot corroborate each other. That is NOT asserted as settled in
    /// either direction. What would settle it is a record of the ALTER independent of the job history, and
    /// none has been found — and nothing above needs one, because the exclusion rule is positional. It used
    /// to say "and the excluded run is disqualified by its duration alone", which was the belt to the
    /// boundary's braces; that belt is gone with the census, since 864 s is now an ordinary member of the
    /// post-boundary range. The circularity is therefore no better corroborated than it was and no worse:
    /// a positional rule needs no second reason, which is why it was chosen over one.</para>
    ///
    /// <para><b>And the rule that keeps a whole SERIES out of this constant, stated as a rule because the
    /// series keeps growing.</b> The hourly self-metrics snapshot
    /// (<see cref="StoreSelfMetrics.BackgroundJobInsertSql"/>, <c>object_kind = 'background_job'</c>)
    /// records <c>last_run_duration</c> with NO STATUS COLUMN AT ALL, while
    /// <see cref="HeaviestRefreshRuntimeSql"/> and #2136's <see cref="JobCadenceReadSql"/> both filter
    /// <c>last_run_status = 'Success'</c>. An unfiltered series can carry an aborted run's duration, so NO
    /// reading from it may set this constant — a statement about the SOURCE, deliberately not about any
    /// particular reading, because that series gains one every hour this job runs and an enumeration of it
    /// would be stale within the hour. The complete set of snapshot readings up to <c>04:20Z</c>
    /// (342 s, 348 s, 286 s) says the series stayed flat, which is corroboration and nothing more — and
    /// all three are now IN the census population above. <b>That is worth stating, because the sixteen-run
    /// record held these three to being DISJOINT from it, and the reversal is not a defect in either
    /// figure.</b> Disjointness held only while the published population was a SAMPLE of this job; against
    /// a CENSUS of the same job it cannot hold at all, because a snapshot of a job's last run reports a
    /// duration the census contains by construction. So the values were never what made these readings
    /// inadmissible and a test on them was measuring the sample's incompleteness: the rule is about which
    /// SOURCE may set this constant, and that is checked against the shipped SQL of all three reads. Note
    /// the scope has to CLOSE the population, not merely date it: "up to 04:20Z" is a window that has ended
    /// and will still be true next year, where "the readings so far" carries a scope and rots anyway,
    /// because a doc comment has no timestamp of its own to be read relative to. They are kept out of the
    /// population above for the rule's sake rather than for tidiness.</para>
    ///
    /// <para><b>Why the grid is sized against the high figure and not the low.</b> A slot chosen against the
    /// 194 s low would be correct only at the load it was chosen at, and before the narrowing this job ran
    /// 3,301-6,330 s against a 1-hour cadence, which is what invalidation looks like.</para>
    ///
    /// <para><b>THE OPERATING ENVELOPE, stated because it is a condition and not a property.</b> #3012's
    /// convoy needed a refresh and a compression policy to want the same relation at the same time. Two things
    /// make that residual small right now, and BOTH are load-dependent. The refresh window
    /// (<c>[now - 1 day, now - 1 hour]</c>) and the chunks a compression policy finds eligible
    /// (<c>range_end &lt;= now - 1 day</c> on <see cref="CompressAfterDays"/>) are disjoint at any single
    /// instant — but the two <c>now</c>s are not the same instant. A refresh that started D seconds ago is
    /// still holding its lock while a compression job evaluates eligibility against a <c>now</c> that has moved
    /// D forward, so the set of chunks that are inside the running refresh's window AND already eligible for
    /// the compression starting now is exactly <b>D wide</b>. The overlap therefore grows with D, and so does
    /// the plain window in which a compression tick can queue an <c>AccessExclusiveLock</c> behind a refresh's
    /// <c>AccessShareLock</c> on the same hypertable — the mechanism #3012 measured, which never needed chunk
    /// overlap at all.</para>
    ///
    /// <para><b>So: the small-residual reading is conditional on how long this job runs, and what
    /// invalidates it is that runtime approaching <see cref="RefreshPhaseSlotSeconds"/>.</b> At 896 s
    /// against a 1260-second slot the margin is 364 seconds — the clearance the population above carries, a
    /// property of that closed record rather than of current load; the heaviest
    /// slot is excluded WHOLE rather than guarded on the guard band being shorter than the refresh rather
    /// than on the refresh filling the slot (see <see cref="CompressionPhaseMinutes"/>). A value at or past
    /// the slot width is asserted as a failure rather than accommodated: past that point the refresh runs
    /// into its neighbour and the grid needs redesigning, not renumbering.</para>
    ///
    /// <para><b>THE LIVE ENVELOPE, which the census has now COLLAPSED onto that clearance rather than
    /// leaving beside it (#3119, #3166).</b> Over <c>2026-09-07</c> — one closed day, its 24 runs read from
    /// <c>timescaledb_information.job_history</c> at one row per run — this job's maximum was
    /// <b>896.1 s</b>. That leaves <b>363.9 s</b> of the slot, <b>28.8%</b> of it, and sits <b>153.9 s</b>
    /// BELOW <see cref="RefreshSlotWarningSeconds"/>, which <see cref="ClassifyRefreshSlotHeadroom"/> bands
    /// <see cref="RefreshSlotHeadroom.InsideSlot"/>. #3119 had to state these figures apart from the
    /// clearance because the constant was the maximum of a SAMPLE and the census exceeded it. They agree to
    /// the second — and that agreement is a COINCIDENCE ABOUT WHERE ONE RUN LANDED rather than an identity
    /// of populations (#3182). This day's runs are a SUBSET of the population above, not the whole of it:
    /// the two figures coincide because the population's largest run falls inside this day, which is a fact
    /// about that run's position and no evidence that a day is a census. <b>A day is not a population for
    /// this constant and no sentence here may treat it as one</b>, because that is precisely the substitution
    /// that lets one day's figure carry a census's authority. What one day is worth is what any single
    /// reading is worth: it is a lower bound, and the classifier can be run against it. What
    /// remains is the reading itself: short of the window width, so the grid's stated precondition holds —
    /// but the residual is D wide and D is that maximum, so nothing here can be described as completing well
    /// inside its slot. RefreshCeilingProvenancePinTests derives every figure stated against that maximum
    /// from the grid's own constants and takes the band from the shipped classifier, so a re-derived grid
    /// moves them all and a reading that started classifying as a warning goes red rather than sitting here
    /// as prose.</para>
    ///
    /// <para><b>THE CONSEQUENCE THIS CONSTANT DID NOT SETTLE, and where it was settled (#3166, #3174).</b>
    /// Against the 750 s watch line a 15-minute slot produced, a sizing figure of 896 s was ABOVE it, so
    /// <see cref="ClassifyRefreshSlotHeadroom"/> banded the grid's own sizing figure a warning and six test
    /// methods went red on exactly that — the checks doing their job rather than literals left behind, and
    /// <see cref="RefreshSlotWarningSeconds"/>'s own summary had pre-registered it. They were not widened.
    /// What the arithmetic said: restoring the five-sixths line's lead time above 896 s needs a slot of at
    /// least <b>1,077 s</b> — the smallest <c>s</c> with <c>s * 5 / 6 &gt; 896</c>, since 1,076 gives exactly
    /// 896 and <see cref="ClassifyRefreshSlotHeadroom"/> warns at <c>&gt;=</c> — which is 18 whole minutes,
    /// and 60 does not divide 18. Every step that DOES divide 60 and is wide enough collapses the
    /// <c>collect.query_stats</c> trio onto one minute and moves
    /// <see cref="RefreshSlotPercentOfHourlyCadence"/> off the default V57 has already applied, so no step
    /// change was available at all. #3174 re-derived the grid's SHAPE instead
    /// (<see cref="LightRefreshStepMinutes"/>): the window the hour can spare is
    /// <see cref="HeaviestRefreshWindowMinutes"/>, which puts the watch line at 1,050 s and this constant
    /// 154 s below it.</para>
    /// </summary>
    public const int HeaviestHourlyRefreshObservedCeilingSeconds = 896;

    /// <summary>
    /// The heaviest hourly refresh's window in seconds — the wall
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>'s envelope is stated against, named once so
    /// the build-time assertion and the runtime watch below cannot disagree about where it is.
    ///
    /// <para>Derived from <see cref="HeaviestRefreshWindowMinutes"/> rather than restated: a re-derived grid
    /// must move every bound that was expressed over it, and a literal 900 sat still while the grid changed
    /// underneath it. The name is kept because this is still the only "slot" the product has — a width one
    /// refresh has to finish inside — but it is now the heaviest refresh's own window rather than one tile of
    /// a uniform grid, which is the whole of #3174's change.</para>
    /// </summary>
    public static int RefreshPhaseSlotSeconds => HeaviestRefreshWindowMinutes * 60;

    /// <summary>
    /// The fraction of a window at which a runtime watch on that window speaks, as a numerator over
    /// <see cref="WindowWatchLeadDenominator"/> — five sixths, so a watch line sits at 83.3% of whatever
    /// space the thing being watched has.
    ///
    /// <para><b>Named because the grid now has TWO watches over it and one lead-time choice, not two.</b>
    /// <see cref="RefreshSlotWarningSeconds"/> is this fraction of
    /// <see cref="RefreshPhaseSlotSeconds"/> and <see cref="CompressionClearanceWatchSeconds"/> is the same
    /// fraction of a compression minute's clearance. Both are re-derived over this pair rather than each
    /// carrying its own <c>* 5 / 6</c>: a second literal would let the two lead times drift apart silently,
    /// and this file's own <see cref="CompressScheduleSpan"/> remark states the rule that forbids it —
    /// cross-check by DERIVING one side from the other, never by pinning each side to its own constant,
    /// which forces the edit on whichever side the editor is looking at and forces nothing on the other.
    /// The value of neither line moves by being expressed this way; the derivation is what changes.</para>
    ///
    /// <para><b>Why the same fraction is right for both, rather than a coincidence being institutionalised.</b>
    /// The argument on <see cref="RefreshSlotWarningSeconds"/> is that the remaining sixth has to be usable
    /// lead time against a runtime that grows with data volume — and the compression side is watched against
    /// the same kind of quantity, a daily chunk rewrite whose cost scales with the day's ingest. What differs
    /// between the two is the WIDTH each is a fraction of, and that is exactly what taking a fraction handles.
    /// A compression minute at the tail of the band has one light-refresh step of clearance, so a sixth of it
    /// is 10 s of lead — thin, and stated on
    /// <see cref="CompressionMinuteClearanceMinutes"/> rather than hidden, because the answer to a thin lead
    /// time there is a wider band and not a different fraction.</para>
    /// </summary>
    public const int WindowWatchLeadNumerator = 5;

    /// <summary>The denominator of <see cref="WindowWatchLeadNumerator"/>'s fraction.</summary>
    public const int WindowWatchLeadDenominator = 6;

    /// <summary>
    /// The line at which the heaviest hourly refresh's LIVE runtime is worth a warning — five sixths of
    /// <see cref="RefreshPhaseSlotSeconds"/>, so 1,050 s against today's 1,260 s window.
    ///
    /// <para><b>Why this exists at all, which is the whole of #3044.</b> The assertion on
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> bounds a CONSTANT, and the thing it bounds is
    /// a RUNTIME that moves with data volume. It fires when someone edits the constant and never when reality
    /// changes underneath it. The grid's precondition can therefore become false with every test still green.
    /// This is the same bound applied to the figure the envelope is actually about, on the hourly sweep that
    /// already reads the job catalog.</para>
    ///
    /// <para><b>Five sixths is a lead-time choice, and it is applied to the derived slot rather than written
    /// down as an answer.</b> Two things had to hold. It must clear the routine band:
    /// five consecutive live readings during #3044's own review came back 335 s, 594 s, 465 s, 359 s and
    /// 293 s — 26.6% to 47.1% of the window — so a line at 83.3% leaves the peak of THAT set more than a
    /// third of the window below it, which is what keeps it off the load those five represent. And it must
    /// leave usable lead
    /// time: the remaining sixth is 210 s here, while the walk that carries this job through the hour advances
    /// by its own runtime each cycle (see the finish-to-start note on
    /// <see cref="SetCompressionSchedulePhaseSql"/>), so the warning lands while the job still finishes inside
    /// its slot and the grid's stated precondition is still TRUE.</para>
    ///
    /// <para><b>The alternative, and the reason it is rejected — which #3174 had to RE-TAKE rather than
    /// restate, because the old reason stopped being true.</b> The alternative that tempts here is the slot
    /// less one <see cref="CompressionPhaseGuardMinutes"/> band, 1020 s, and it now sits ABOVE
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>. It used to sit below it — that was the
    /// whole of its rejection, since a line under the recorded ceiling warns on the very run the compression
    /// grid is sized against — and the re-derivation took that argument away by shrinking the guard band from
    /// half a uniform slot to the light refreshes' own ceiling. Both lines now clear the ceiling and they are
    /// 30 s apart, so the ordering no longer discriminates and lead time argues mildly FOR the lower one.
    /// What rejects it instead is COUPLING, a property the old geometry could not have exposed: the guard band
    /// is derived from <see cref="OtherHourlyRefreshObservedCeilingSeconds"/>, a measurement of the TWELVE
    /// OTHER refresh policies, so the alternative would make the heaviest refresh's watch line move whenever
    /// a light refresh got slower. Five sixths of <see cref="RefreshPhaseSlotSeconds"/> depends on the window
    /// this job has to fit inside and on nothing else. Under a uniform grid the guard was
    /// <c>step / 2</c> and both lines were functions of the same step, which is exactly why the argument had
    /// to be about ordering back then and can be about coupling now.</para>
    ///
    /// <para><b>This line sits ABOVE <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>, and that is
    /// what makes a crossing mean something.</b> The census re-derivation (#3166) put that constant at
    /// <b>896 s</b>, which INVERTED the ordering against the 750 s line a 15-minute slot produced — and
    /// restoring it is one of the two things the re-derived grid is for. Against the window the hour can
    /// spare, this line is 1,050 s and the ceiling is 154 s below it, so a reading in this band is again past
    /// the whole of the record the compression grid is sized against: a different signal calling for a
    /// different response, rather than a restatement of the grid's own sizing. <b>The relationship is what
    /// is pinned, not the two numbers</b> — a ceiling that rose past this line would put the grid's own
    /// sizing figure inside the warning band, and TimescaleSupportTests says so by going RED rather than
    /// leaving a reader to notice. That pin has now fired once and been answered by re-deriving the geometry
    /// instead of by renumbering the band, which is the only answer that changes anything.</para>
    /// </summary>
    public static int RefreshSlotWarningSeconds =>
        RefreshPhaseSlotSeconds * WindowWatchLeadNumerator / WindowWatchLeadDenominator;

    /// <summary>
    /// Where one live reading of <see cref="HeaviestHourlyRefreshView"/>'s runtime sits against the slot it
    /// has to fit inside. <see cref="ClassifyRefreshSlotHeadroom"/> produces it; nothing here reads a clock or
    /// a catalog, so it pins directly.
    /// </summary>
    public enum RefreshSlotHeadroom
    {
        /// <summary>Under <see cref="RefreshSlotWarningSeconds"/> — the routine band, and therefore inside
        /// the slot the refresh has to fit in. Not worth a line above Debug.</summary>
        InsideSlot,

        /// <summary>At or past <see cref="RefreshSlotWarningSeconds"/> but still inside
        /// <see cref="RefreshPhaseSlotSeconds"/>: the grid's precondition still holds, and there is still
        /// time to re-derive it deliberately.</summary>
        ApproachingSlot,

        /// <summary>At or past <see cref="RefreshPhaseSlotSeconds"/>. The refresh no longer fits inside its
        /// own slot, so excluding one slot is no longer enough — the compression grid's stated precondition
        /// is FALSE and #3035 has to be re-derived rather than renumbered.</summary>
        SlotExceeded,
    }

    /// <summary>
    /// Classifies one observed runtime of <see cref="HeaviestHourlyRefreshView"/> against its slot.
    ///
    /// <para><b>Both boundaries are inclusive, and that is the point of the function rather than an
    /// implementation detail.</b> The build-time assertion is
    /// <c>HeaviestHourlyRefreshObservedCeilingSeconds &lt; RefreshPhaseSlotSeconds</c>, so a value AT the
    /// slot width already fails it. <see cref="RefreshSlotHeadroom.SlotExceeded"/> therefore starts at
    /// <c>&gt;=</c> the same width: the runtime watch and the constant's assertion agree on where the wall is
    /// by construction, which is the property a second hand-written comparison could not offer.</para>
    ///
    /// <para>Negative and NaN readings classify as <see cref="RefreshSlotHeadroom.InsideSlot"/> rather than
    /// throwing: this feeds a log line on an observability sweep, and a catalog that hands back something
    /// impossible must cost the line, never the sweep.</para>
    /// </summary>
    public static RefreshSlotHeadroom ClassifyRefreshSlotHeadroom(double observedSeconds)
    {
        if (observedSeconds >= RefreshPhaseSlotSeconds)
        {
            return RefreshSlotHeadroom.SlotExceeded;
        }

        return observedSeconds >= RefreshSlotWarningSeconds
            ? RefreshSlotHeadroom.ApproachingSlot
            : RefreshSlotHeadroom.InsideSlot;
    }

    /// <summary>
    /// Whether a live reading has FALSIFIED a recorded ceiling — the #3182 finding, and a different fact
    /// from <see cref="RefreshSlotHeadroom"/> with a different remedy.
    ///
    /// <para><b>Why this is not a fourth slot band.</b> The slot bands answer "does this run fit in the
    /// window the grid gives it", and their remedy is re-deriving the grid. This answers "is the number the
    /// grid was DERIVED FROM still the maximum it claims to be", and its remedy is re-deriving that number.
    /// Both can be true of one reading and neither implies the other: a run past the slot may be under a
    /// ceiling that was measured on a worse day, and a run that falsifies the ceiling may sit comfortably
    /// inside the slot. The second case is the one that had no signal at all, which is what #3182 is
    /// about — a recorded maximum can be overtaken from inside the routine band, where the slot watch logs
    /// at Debug and says nothing is happening.</para>
    /// </summary>
    public enum RefreshCeilingFreshness
    {
        /// <summary>The reading is at or under the recorded ceiling, so the constant is still a bound on
        /// what has been seen.</summary>
        CeilingHolds,

        /// <summary>The reading is ABOVE the recorded ceiling. The constant is not the maximum of the job's
        /// behaviour any more — whatever population it was derived from has been overtaken, and the value
        /// has to be re-derived rather than the grid re-dimensioned.</summary>
        CeilingFalsified,
    }

    /// <summary>
    /// Classifies one observed runtime against a constant that claims to be a MAXIMUM.
    ///
    /// <para><b>STRICTLY greater, and that is the opposite inclusivity from
    /// <see cref="ClassifyRefreshSlotHeadroom"/> — deliberately.</b> The slot bands open at <c>&gt;=</c>
    /// because a run that took exactly its window was already colliding with its neighbour, so the wall is
    /// inclusive. A recorded maximum is a different kind of claim: a reading EQUAL to it is the reading it
    /// was derived from and confirms the constant rather than contradicting it. Only a value above it is
    /// evidence the constant is wrong, so this boundary has to exclude equality where the other one
    /// includes it. Getting that backwards would report the grid's own sizing figure as a falsification of
    /// itself on the hour it was measured.</para>
    ///
    /// <para><b>NO SPECIAL CASE for an impossible reading, and its ABSENCE is deliberate.</b> The siblings
    /// need one because they compare against a WIDTH: a negative reading is under every band boundary and a
    /// NaN is under none of them, so both have to be steered somewhere. This compares against a ceiling that
    /// is positive by construction — both constants that feed it are — so a negative reading is not greater
    /// than it and <c>NaN &gt; x</c> is false, and each of them answers
    /// <see cref="RefreshCeilingFreshness.CeilingHolds"/> from the one comparison. A guard added here would
    /// be unreachable by any value the product can produce, and an unreachable guard is not caution: it is a
    /// line no test can distinguish from its own absence, so it reads as protection and certifies nothing.
    /// Where a non-finite reading does real damage is the rate limiter's mark —
    /// <see cref="RefreshCeilingStalenessWatch.ShouldReport"/> holds that, and holds it where a mutation can
    /// reach it.</para>
    /// </summary>
    public static RefreshCeilingFreshness ClassifyRefreshCeilingFreshness(
        double observedSeconds, double recordedCeilingSeconds) =>
        observedSeconds > recordedCeilingSeconds
            ? RefreshCeilingFreshness.CeilingFalsified
            : RefreshCeilingFreshness.CeilingHolds;

    /// <summary>
    /// The name <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> is reported under by
    /// <see cref="LogRefreshCeilingStaleness"/>, taken from the member rather than typed — an operator
    /// reading the line has to be able to grep the constant it names.
    /// </summary>
    public const string HeaviestRefreshCeilingConstantName =
        nameof(HeaviestHourlyRefreshObservedCeilingSeconds);

    /// <summary>
    /// The name <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> is reported under, for the reason
    /// <see cref="HeaviestRefreshCeilingConstantName"/> exists.
    /// </summary>
    public const string OtherRefreshCeilingConstantName =
        nameof(OtherHourlyRefreshObservedCeilingSeconds);

    /// <summary>
    /// Reports that a live reading has overtaken a ceiling constant — the #3182 finding, kept SEPARATE from
    /// <see cref="LogHeaviestRefreshSlotHeadroom"/> so it is reachable from every slot band.
    ///
    /// <para><b>Why a separate call and not a fourth case in that switch.</b> The defect #3182 records is
    /// that a constant claiming to be a census maximum was overtaken while the runs that overtook it
    /// classified <see cref="RefreshSlotHeadroom.InsideSlot"/> and logged at Debug. A branch inside the
    /// band switch can only speak in the band it is written in; this is called beside it, so a falsified
    /// ceiling is reported in the routine band, the warning band and the breach band alike. The two
    /// findings are also worded so they cannot be mistaken for each other: this one names the CONSTANT and
    /// asks for it to be re-derived, the slot line names the GRID and asks for that to be.</para>
    ///
    /// <para><b>WARNING, and the level is a decision rather than a default.</b> Debug is where the defect
    /// lived, so it is not available. Error is reserved by
    /// <see cref="LogHeaviestRefreshSlotHeadroom"/> for a stated precondition of the shipped grid being
    /// FALSE, which a falsified ceiling does NOT make: the precondition is that a run fits inside its slot,
    /// and a reading can be past the ceiling and still well inside the window. Levelling this Error too
    /// would collapse the distinction the finding exists to draw. Warning is the level that says "still
    /// true, still time to act deliberately", which is exactly the state a stale sizing constant is in.</para>
    ///
    /// <para><b>RATE-LIMITED BY A HIGH-WATER MARK, because a constant that has drifted trips this often.</b>
    /// A ceiling overtaken by ordinary load is overtaken on a large share of runs, and an hourly Warning
    /// that repeats the same fact is how a signal becomes furniture — the discipline
    /// <see cref="LogCompressionActivity"/> states for Information. So
    /// <see cref="RefreshCeilingStalenessWatch"/> reports the first falsifying reading for a constant and
    /// thereafter only one that exceeds the largest already reported. That shape is chosen over a
    /// once-per-process latch and over a time window for one reason: what this finding asks for is a
    /// constant re-derived to at least the largest run on record, so a NEW record changes the answer and a
    /// repeat does not. A time window would re-report the same value on a timer, and a plain latch would
    /// hide the reading that actually sizes the re-derivation behind the first one that happened to
    /// arrive.</para>
    /// </summary>
    public static void LogRefreshCeilingStaleness(
        string constantName,
        double recordedCeilingSeconds,
        string view,
        double observedSeconds,
        RefreshCeilingStalenessWatch watch,
        ILogger? logger)
    {
        if (watch is null)
        {
            throw new ArgumentNullException(nameof(watch));
        }

        if (logger is null)
        {
            return;
        }

        if (ClassifyRefreshCeilingFreshness(observedSeconds, recordedCeilingSeconds)
            != RefreshCeilingFreshness.CeilingFalsified)
        {
            return;
        }

        if (!watch.ShouldReport(constantName, observedSeconds))
        {
            return;
        }

        logger.LogWarning(
            "TimescaleDB: {View}'s refresh policy last ran {Seconds:F1}s, which is {Over:F1}s ABOVE {Constant} = {Ceiling:F1}s — a constant recorded as the MAXIMUM of a closed population. So that population has been overtaken and the constant is stale: it has to be RE-DERIVED over a population that includes this run (#3182), which is a different repair from re-deriving the compression phase grid (#3035) and is needed whatever band the slot watch puts this reading in. Its per-run history is timescaledb_information.job_history, one row per run, but only where timescaledb.enable_job_execution_logging is on — it is off by default and a store provisioned before that GUC gained its own conf marker reports nothing there until it heals, so an empty result is that gap and not a quiet hour (#3175/#3177). Reported once per constant and then only for a larger run, because what the re-derivation needs is the LARGEST reading and a repeat of one already reported adds nothing.",
            view, observedSeconds, observedSeconds - recordedCeilingSeconds, constantName,
            recordedCeilingSeconds);
    }

    /// <summary>
    /// The longest run recorded for any hourly refresh OTHER than <see cref="HeaviestHourlyRefreshView"/> —
    /// and, since #3174, the number <see cref="CompressionPhaseGuardMinutes"/> is DERIVED from rather than
    /// merely characterised against.
    ///
    /// <para><b>WHICH STATISTIC, OVER WHICH POPULATION — stated at the top for the reason
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> states it there (#3182).</b> This is a
    /// MAXIMUM. Its population is the runs of the twelve other hourly views' refresh policies that started
    /// AFTER the narrowing boundary named below, on one store, up to the instant named below. Not a
    /// percentile, not a per-view figure, and not a fleet reading: one number covers twelve policies, so a
    /// live run of ANY of them above it falsifies it. "Recorded" in the line above means recorded in that
    /// read, and nowhere else — the word carries no claim about runs the read did not see.</para>
    ///
    /// <para><b>And since #3182 the product reports when a live run of one of those twelve falsifies it,
    /// which it previously could not because nothing read them.</b> The heaviest refresh had
    /// <see cref="HeaviestRefreshRuntimeSql"/>; these twelve had no live reading attributable to a view
    /// anywhere in the product, because #2136's <see cref="JobCadenceReadSql"/> keys on
    /// <c>proc_name || hypertable_name</c> and a refresh policy's <c>hypertable_name</c> is the
    /// MATERIALIZATION hypertable rather than the view. So a check that looks like it covers these jobs
    /// cannot attribute what it reads to the constant that bounds them.
    /// <see cref="OtherHourlyRefreshRuntimesSql"/> is the read that can, and
    /// <see cref="LogRefreshCeilingStaleness"/> is what it feeds. <b>The direction of the harm is why this
    /// one matters more than a stale assertion:</b> this constant is arithmetic input, so a light refresh
    /// past it means <see cref="CompressionPhaseGuardMinutes"/> no longer covers the refresh it exists to
    /// cover, and a compression policy can start while that refresh still holds
    /// <c>AccessShareLock</c> — #3012's convoy, by construction rather than by chance.</para>
    ///
    /// <para><b>THE DERIVATION, recorded as a method rather than only as a value (#3174).</b> The old figure
    /// came from the first full staggered cycle — 26 s / 2 s / 864 s / 140 s — a cycle that STRADDLES the
    /// narrowing boundary, so its readings' regime membership was undetermined in exactly the way the run
    /// excluded from <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>'s population is. The
    /// re-derivation is the same read that constant names, pointed at the other twelve policies.
    /// POPULATION: every run of <c>policy_refresh_continuous_aggregate</c> for the hourly views OTHER than
    /// <see cref="HeaviestHourlyRefreshView"/>, read from <c>timescaledb_information.job_history</c> at one
    /// row per run on the one store that carries this workload, at <c>2026-09-08 14:57Z</c>. EXCLUSION RULE:
    /// the same positional one — every run starting at or before the narrowing boundary of <c>13:44:23</c> is
    /// out, whatever its duration. ESTIMATOR: the maximum, for the same reason it is the maximum there — what
    /// this sizes is a scheduling exclusion, and the exceedance rate it may accept over its own record is
    /// zero.</para>
    ///
    /// <para><b>THE CENSUS.</b> Post-boundary, the maximum is <b>226.8</b> s over <b>874</b> runs of
    /// <b>12</b> views, with 95th percentile <b>42.2</b> s and median <b>0.8</b> s. Zero rows are removed by
    /// the succeeded/finish filter (<b>874</b> of <b>874</b>), so this is the whole of the span rather than a
    /// status-selected part of it. <b>ONE STORE'S, on the same precondition
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> states (#3175):</b> the read only sees
    /// executions where <c>timescaledb.enable_job_execution_logging</c> is ON, that GUC could not be healed
    /// onto a cluster predating the conf block that set it until #3175/#3177 gave it a marker of its own,
    /// and a maximum over the view on an unhealed store returns zero rows and reads as "nothing exceeded
    /// the line". This store had it at the read — 874 rows for twelve policies over three days is the
    /// positive control that says so — and no claim is made about any other.
    /// The full 874-run list is NOT republished: unlike the heaviest refresh's 57, a list that long stops
    /// being re-derivable by reading and starts being a wall of digits, and what bounds a maximum is its
    /// tail. So the tail is what the mechanism paragraph below states, and the estimator is recomputable from
    /// the named read rather than from a transcription of it. The distance between the maximum and the 95th
    /// percentile — 184.6 s over a population where the median run finishes in under a second — is the whole
    /// reason a percentile is not the estimator here: it would discard exactly the runs this bound exists
    /// for.</para>
    ///
    /// <para><b>THE MIDNIGHT REGIME IS IN, and this constant's own doc is why (#3174).</b> The two largest
    /// runs are both <see cref="QueryStoreStatsHourlyView"/> starting at <c>00:30</c>: <b>226.8</b> s on
    /// <c>2026-09-08</c> against <b>160.4</b> s on <c>2026-09-07</c>, <b>41%</b> higher night over night. So
    /// the figure is a midnight reading, and the obvious move is to exclude the midnight hour as
    /// unrepresentative. That is the wrong move, and this paragraph used to carry the argument against it:
    /// <b>NOTHING WAS SIZED FROM IT</b> — the guard was derived from the grid step, and this figure "only
    /// says how much margin that derivation happens to leave". A readout of a margin has to include the runs
    /// where the margin was consumed, or it stops being a readout of anything; removing the runs that fired a
    /// pin is the re-type-a-band-to-pass move one constant over. The mechanism behind those two values is
    /// #3112's midnight band — the daily chunk-close burst meeting the refresh grid at the shared midnight
    /// boundary, where at every other hour the compression ticks find nothing eligible and finish in seconds.
    /// The grid does not change how much work midnight carries, so this figure is the right one for the guard
    /// to be derived from and the midnight band remains its own question. The third-largest run is
    /// <b>140.1</b> s, at <c>13:45:00</c> on the boundary day — the same reading the old constant's 140 came
    /// from, reproduced as an ordinary post-boundary member, which is what says the method is the same one
    /// rather than a new one that happens to agree.</para>
    ///
    /// <para><b>What changed underneath it: it IS sized from now, so the old escape no longer applies.</b>
    /// <see cref="CompressionPhaseGuardMinutes"/> is this figure rounded up to a whole minute, which makes it
    /// a bound the grid rests on rather than a characterisation of one. The direction of risk is stated rather
    /// than left to be discovered: a light-refresh ceiling past <c>CompressionPhaseGuardMinutes * 60</c> leaves
    /// a compression policy able to start while a light refresh still holds <c>AccessShareLock</c>, which is
    /// #3012's mechanism, and TimescaleSupportTests is written to fail in that direction.</para>
    /// </summary>
    public const double OtherHourlyRefreshObservedCeilingSeconds = 226.8;

    /// <summary>
    /// How long after a light refresh starts a compression policy may be scheduled —
    /// <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> rounded UP to a whole minute, so 4 minutes
    /// against a 226.8 s ceiling.
    ///
    /// <para><b>The guard is one-sided, and that asymmetry is the mechanism rather than a simplification.</b>
    /// #3012's convoy needs compression's <c>AccessExclusiveLock</c> request to ARRIVE while a refresh already
    /// holds <c>AccessShareLock</c> on the same hypertable: a QUEUED exclusive blocks every subsequent shared
    /// request, so collector writes pile up behind a lock nobody holds. The reverse order does not compose
    /// that way — a compression run already holding its lock blocks collectors for its own duration whether or
    /// not a refresh starts, which is the ordinary cost of compressing and not something a schedule can move.
    /// So compression is kept clear of the minutes AFTER a refresh start and needs no clearance before the
    /// next one. That one-sidedness is also what decides which band opens the hour — see
    /// <see cref="HeaviestRefreshStartMinute"/>.</para>
    ///
    /// <para><b>It was <c>step / 2</c> and is now the ceiling itself, which is a smaller number and a
    /// stronger check (#3174).</b> Half a uniform slot was 7 minutes and happened to be 3x the light
    /// refreshes' then-recorded ceiling, and that 3x was asserted — a check on the CHARACTERISATION rather
    /// than on anything the grid rested on. There is no uniform slot to halve now, so the guard is derived
    /// from the measurement it has to cover: 226.8 s rounds up to 4 minutes, and the assertion becomes the
    /// requirement (<c>CompressionPhaseGuardMinutes * 60 &gt;= OtherHourlyRefreshObservedCeilingSeconds</c>)
    /// instead of a multiple of it. <b>The rounding is the whole margin, and it is 13.2 s.</b> That is thin
    /// and it is stated rather than dressed up: what it buys is that the margin can only be consumed by the
    /// light refreshes actually getting slower, which goes red here, where a multiple of a ceiling could
    /// be satisfied by a guard that had stopped covering anything. A wider guard is not free either — every
    /// minute of it comes out of <see cref="HeaviestRefreshWindowMinutes"/>, which is the band that has a
    /// measured growth series behind it.</para>
    ///
    /// <para><b>It can never exceed the gap it sits in</b>, because
    /// <see cref="HeaviestRefreshStartMinute"/> and <see cref="CompressionPhaseBandMinutes"/> are both
    /// expressed over it: widening the guard moves the heaviest refresh later and narrows its window rather
    /// than overrunning a neighbour. TimescaleContinuousAggregateTests holds the three bands to tiling the
    /// hour exactly, so a guard wide enough to leave no window at all is red rather than silent.</para>
    /// </summary>
    public static int CompressionPhaseGuardMinutes =>
        (int)Math.Ceiling(OtherHourlyRefreshObservedCeilingSeconds / 60.0);

    /// <summary>
    /// The minutes <see cref="CompressionPhaseGuardMinutes"/> and
    /// <see cref="HeaviestRefreshWindowMinutes"/> SHARE — what the hour has left once the light band and the
    /// compression band are at the widths their own measurements ask for.
    ///
    /// <para>Named because it is the budget the two of them compete for, and because it is the term that
    /// makes that competition arithmetic rather than prose: every minute the guard takes is a minute the
    /// heaviest refresh's window loses, and this is the total there is to divide. Derived from the same
    /// expressions <see cref="HeaviestRefreshStartMinute"/> and
    /// <see cref="HeaviestRefreshWindowMinutes"/> are built from, so a re-derived grid moves it rather than
    /// leaving it behind as a second opinion.</para>
    /// </summary>
    public static int GuardAndWindowSharedMinutes =>
        MinutesInHourlyCadence
        - ((LightHourlyRefreshCount - 1) * LightRefreshStepMinutes)
        - CompressionPhaseBandMinutes;

    /// <summary>
    /// The watch line a guard of <paramref name="guardMinutes"/> would leave — the same chain
    /// <see cref="RefreshSlotWarningSeconds"/> is, evaluated at a hypothetical guard instead of the shipped
    /// one.
    ///
    /// <para><b>It exists so the feasibility question can be ASKED, which the shipped chain cannot do.</b>
    /// <see cref="HeaviestRefreshWindowMinutes"/> reads <see cref="CompressionPhaseGuardMinutes"/>, so the
    /// live chain answers for exactly one guard and there is no way to find out what a WIDER guard would
    /// cost without re-spelling the arithmetic. This is that arithmetic in one place, and
    /// TimescaleSupportTests requires it to agree with the shipped chain at the live guard — so it cannot
    /// drift into being a second, kinder model of the grid.</para>
    ///
    /// <para>Integer division throughout, matching <see cref="RefreshSlotWarningSeconds"/>: a line computed
    /// with rounding would sit above the shipped one on some widths and the two would disagree about
    /// feasibility at exactly the boundary the question is about.</para>
    /// </summary>
    public static int RefreshSlotWarningSecondsForGuardMinutes(int guardMinutes) =>
        (GuardAndWindowSharedMinutes - guardMinutes) * 60
        * WindowWatchLeadNumerator / WindowWatchLeadDenominator;

    /// <summary>
    /// The WIDEST <see cref="CompressionPhaseGuardMinutes"/> the hour can carry while the grid's own stated
    /// precondition still holds at <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> — negative when
    /// no guard at all leaves a wide enough window.
    ///
    /// <para><b>Why this is stated rather than discovered (#3182).</b> The guard is
    /// <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> rounded up to a whole minute, and that ceiling
    /// is a MAXIMUM over a series with a long tail — so the derivation has no upper bound of its own, while
    /// the hour does. Nothing in the derivation notices when the two conflict: the conflict surfaces as a
    /// grid that has already been widened past what the hour contains, and then as an argument about which
    /// number to bend. This term is the bound the derivation is missing, and
    /// <see cref="WidestFeasibleOtherRefreshCeilingSeconds"/> converts it back into the units the constant is
    /// measured in, so a re-derivation that does not fit is red AT THE CONSTANT rather than after the grid
    /// has been re-dimensioned around it.</para>
    ///
    /// <para><b>Searched downward rather than solved, so it is the SHIPPED comparison that decides.</b> The
    /// precondition is <c>HeaviestHourlyRefreshObservedCeilingSeconds &lt; RefreshSlotWarningSeconds</c>, and
    /// that line is an integer-divided fraction of an integer window. A closed form would have to reproduce
    /// two truncations, and a closed form that reproduced them slightly differently would answer feasible
    /// where the build answers infeasible — the one disagreement this term must not be capable of. The walk
    /// evaluates the same expression the grid does, at most
    /// <see cref="GuardAndWindowSharedMinutes"/> times, once.</para>
    ///
    /// <para><b>Negative is a real answer, not an error code.</b> A heaviest ceiling large enough that even a
    /// zero-minute guard leaves too narrow a window is a grid the hour cannot contain at ANY guard, which is
    /// a different fact from "the guard is too wide" and calls for a different repair — a cheaper refresh or
    /// a longer cadence for that one aggregate, neither of which is a constant to re-derive. Returning a
    /// negative says so; throwing would make the caller decide what it meant.</para>
    /// </summary>
    public static int WidestFeasibleCompressionPhaseGuardMinutes
    {
        get
        {
            for (var guard = GuardAndWindowSharedMinutes; guard >= 0; guard--)
            {
                if (HeaviestHourlyRefreshObservedCeilingSeconds
                    < RefreshSlotWarningSecondsForGuardMinutes(guard))
                {
                    return guard;
                }
            }

            return -1;
        }
    }

    /// <summary>
    /// The largest value <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> may take before the grid
    /// stops fitting in the hour — <see cref="WidestFeasibleCompressionPhaseGuardMinutes"/> back in seconds,
    /// so the bound is stated in the unit the constant is measured in.
    ///
    /// <para>This is the number a re-derivation of that constant has to be read against. A light-refresh
    /// census whose maximum lands above it is not a constant to update: it is a statement that the guard the
    /// measurement asks for and the window the heaviest refresh needs cannot both be had, and that is a
    /// SCHEDULING decision rather than a renumbering — the same distinction
    /// <see cref="RefreshSlotHeadroom.SlotExceeded"/> draws for the other constant.</para>
    ///
    /// <para>Zero when no guard is feasible at all, because a negative ceiling is not a value the constant
    /// can take and a bound expressed as one would read as a wider allowance than it is.</para>
    /// </summary>
    public static int WidestFeasibleOtherRefreshCeilingSeconds =>
        Math.Max(0, WidestFeasibleCompressionPhaseGuardMinutes) * 60;

    /// <summary>
    /// The most compression policies the grid will put on one minute — the input the compression band's WIDTH
    /// is derived from, rather than a figure read off it afterwards.
    ///
    /// <para><b>Why this is the input and the width is the output (#3174).</b> The band used to be whatever
    /// minutes a uniform refresh step left over, and how thinly 70 hypertables spread across them was a
    /// consequence nobody chose — a test asserted the resulting ceiling was 3 and would have accepted 4 or 5
    /// from a moved step. The thing that has an operational meaning is the spread: every hypertable's newest
    /// 1-day chunk becomes eligible at the same UTC midnight (see <see cref="CompressionPhaseMinutes"/>), so
    /// the count sharing a minute is the count of simultaneous chunk rewrites at that boundary. So the spread
    /// is stated and the width follows from the catalog.</para>
    ///
    /// <para><b>3 preserves the shipped behaviour rather than proposing new behaviour</b>, which is the
    /// reason to prefer it to any other number here: it is the spread the grid has always produced, so a
    /// re-derivation that lands on it changes where compression runs without changing how concentrated it is.
    /// The failure direction is stated: a catalog grown past
    /// <c>CompressionPhaseMaxPerMinute * CompressionPhaseBandMinutes</c> is red, and the repair is a wider
    /// band — which the hour can only pay for out of <see cref="HeaviestRefreshWindowMinutes"/>, and only
    /// while that window still clears <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>.</para>
    /// </summary>
    public const int CompressionPhaseMaxPerMinute = 3;

    /// <summary>
    /// The compression band's width in minutes: enough for every hypertable we own to start at no more than
    /// <see cref="CompressionPhaseMaxPerMinute"/> per minute, taken from
    /// <see cref="HypertableCount"/> so registering a collector moves the band instead of quietly crowding
    /// it.
    /// </summary>
    public static int CompressionPhaseBandMinutes =>
        (HypertableCount + CompressionPhaseMaxPerMinute - 1) / CompressionPhaseMaxPerMinute;

    /// <summary>
    /// Every minute of the hour a compression policy may start on — the tail of the hour, once every hourly
    /// refresh has had the clearance its own recorded ceiling asks for.
    ///
    /// <para><b>RE-DERIVED at #3174, not renumbered.</b> These minutes were the second half of each uniform
    /// refresh slot with the heaviest refresh's slot dropped, and a grid with no uniform slot makes those
    /// minutes meaningless. The rule below is the same rule the old one was — no compression minute may start
    /// while a refresh is running — applied to the re-derived refresh grid, which is why it produces a
    /// contiguous band at the end of the hour instead of three chunks inside it: the refreshes are contiguous
    /// now too.</para>
    ///
    /// <para><b>ONE predicate, and both exclusions are cases of it.</b> A minute is clear when it is at least
    /// its own band past EVERY refresh start, measured forward round the hour. The band is
    /// <see cref="CompressionPhaseGuardMinutes"/> for a light refresh and
    /// <see cref="HeaviestRefreshWindowMinutes"/> — the whole window — for
    /// <see cref="HeaviestHourlyRefreshView"/>. Writing the exclude-whole decision as "its band is its whole
    /// window" is what keeps it a decision rather than a special case, and it walks
    /// <see cref="HourlyRefreshPhaseOrder"/> through <see cref="RefreshPhaseMinutesFor"/>, so the grid and
    /// this band cannot drift apart.</para>
    ///
    /// <para><b>Why the heaviest window is excluded whole rather than guarded.</b>
    /// <see cref="HeaviestHourlyRefreshView"/> occupies 896 of the 1260 seconds in its window and the
    /// <see cref="CompressionPhaseGuardMinutes"/> band is 4, so applying the ordinary band to this window
    /// would admit 11 minutes that sit INSIDE the refresh — the band is the wrong size for it, which is the
    /// arithmetic the exclusion rests on and the reason widening the band is not the alternative. The other
    /// 6 minutes of the window are past the refresh and are left on the table deliberately: recovering them
    /// means sizing a band for one window against a bound whose population is 57 readings and still moving
    /// (194 s to 896 s within the clean regime), which is #3035's exclude-versus-guard decision to reopen and
    /// not a renumbering. Stated in SECONDS against the window in seconds, because the occupancy is only
    /// exactly stateable to a tenth of a minute while the ceiling happens to be a multiple of six seconds, and
    /// 896 is not: a figure that has to be rounded to stay in its unit is a rounded claim wearing an exact
    /// one's clothes.</para>
    ///
    /// <para><b>Why a spread rather than one shared minute.</b> All the compression policies would happily
    /// share a minute as far as LOCKS go — they compress different hypertables, so they do not contend with
    /// each other at all — but they would then do their real work simultaneously. <see cref="CompressAfterDays"/>
    /// and <see cref="ChunkIntervalDays"/> are both 1 and TimescaleDB aligns 1-day chunks to the epoch, so
    /// every hypertable's newest closed chunk becomes eligible at the same UTC midnight. Drifting policies
    /// discover that eligibility at whatever minute they have drifted to, which spreads the daily rewrite
    /// across the hour; collapsing them onto one minute would concentrate it into one. That is a burst this
    /// change would be INTRODUCING, not removing, so the grid keeps the spread and takes only the drift away.
    /// How thin the spread has to be is <see cref="CompressionPhaseMaxPerMinute"/>, and the band's width
    /// follows from it — so the re-derivation moves WHERE compression runs without changing how concentrated
    /// it is.</para>
    ///
    /// <para><b>Concentration is NOT the only property #3112's midnight band is sensitive to, and the
    /// correction matters because the other one moved.</b> #3174 held the spread constant — <b>24</b> minutes
    /// at <b>3</b> per minute before and after — and said so. What it did not hold constant, and did not
    /// claim to, is each minute's CLEARANCE to the next refresh start
    /// (<see cref="CompressionMinuteClearanceMinutes"/>): under the previous grid the three hypertables whose
    /// chunk-close runs were measured sat on minutes with 240 s, 180 s and 120 s of clearance against runs of
    /// 360 s, 198 s and 552 s, so every one of them ran past the refresh that followed it. On this grid the
    /// same three hold minutes with <b>1,200 s</b>, <b>1,140 s</b> and <b>1,080 s</b>. A contiguous refresh
    /// band followed by a contiguous compression band puts most of the compression minutes a long way from
    /// the next refresh, where three chunks of a uniform grid put every compression minute within a guard
    /// band of one — so the re-derivation changed the axis the band actually ran through, as a consequence of
    /// its shape rather than as an aim. It remains true that NOTHING here reduces what midnight
    /// carries.</para>
    ///
    /// <para>Declared HERE, after <see cref="HourlyRefreshPhaseOrder"/>, because a static field initializer
    /// runs in declaration order and this one reads that list through
    /// <see cref="RefreshPhaseMinutesFor"/>.</para>
    /// </summary>
    public static readonly IReadOnlyList<int> CompressionPhaseMinutes = BuildCompressionPhaseMinutes();

    private static int[] BuildCompressionPhaseMinutes()
    {
        var cadence = MinutesInHourlyCadence;
        var minutes = new List<int>(cadence);

        for (var minute = 0; minute < cadence; minute++)
        {
            var clear = true;

            foreach (var view in HourlyRefreshPhaseOrder)
            {
                var band = string.Equals(view, HeaviestHourlyRefreshView, StringComparison.Ordinal)
                    ? HeaviestRefreshWindowMinutes
                    : CompressionPhaseGuardMinutes;

                if ((minute - RefreshPhaseMinutesFor(view) + cadence) % cadence < band)
                {
                    clear = false;
                    break;
                }
            }

            if (clear)
            {
                minutes.Add(minute);
            }
        }

        return minutes.ToArray();
    }

    /// <summary>
    /// The hypertables that carry a compression policy, in phase order — the collector catalog, then
    /// <c>collection_log</c>, which is a hypertable OUTSIDE that catalog (the same <c>+ 1</c>
    /// <see cref="HypertableCount"/> accounts for).
    ///
    /// <para><b>Keyed on the HYPERTABLE, never on a job id.</b> TimescaleDB assigns job ids per deployment, so
    /// an encoded id would name a different job on every other store. A compression job reports its own
    /// hypertable directly in <c>timescaledb_information.jobs</c>, which is why the converge needs no catalog
    /// join to recover identity the way the refresh converge does — and why the id only ever reaches
    /// <c>alter_job</c> as a bound parameter.</para>
    ///
    /// <para>Derived from <see cref="HypertableTables"/> rather than hand-listed, so a new collector is
    /// registered by adding it to the catalog and there is no second list to forget. That is why an
    /// unrecognised name is a FOREIGN hypertable — a bring-your-own store's own table, or a fixture table —
    /// rather than an omission, and is left unphased instead of throwing the way an unregistered hourly view
    /// does.</para>
    /// </summary>
    public static readonly IReadOnlyList<string> CompressionPhaseOrder =
        HypertableTables.Select(t => t.TargetTable).Append(CollectionLogTable).ToArray();

    /// <summary>
    /// Which minute of the hour <paramref name="table"/>'s compression policy starts on; <c>false</c> when
    /// this product does not own the hypertable, in which case its schedule is none of this code's business
    /// beyond the <see cref="CompressScheduleInterval"/> tick #1778 already converges.
    ///
    /// <para>Accepts a bare or <c>collect.</c>-qualified name: the product always passes the bare
    /// <c>TargetTable</c>, but the raw-name overload of <see cref="AddCompressionPolicySql(string)"/> is
    /// reachable with a qualified one.</para>
    /// </summary>
    public static bool TryCompressionPhaseMinutesFor(string table, out int minutes)
    {
        minutes = 0;

        if (string.IsNullOrEmpty(table))
        {
            return false;
        }

        var dot = table.LastIndexOf('.');
        var bare = dot >= 0 ? table[(dot + 1)..] : table;

        for (var index = 0; index < CompressionPhaseOrder.Count; index++)
        {
            if (string.Equals(CompressionPhaseOrder[index], bare, StringComparison.Ordinal))
            {
                minutes = CompressionPhaseMinutes[index % CompressionPhaseMinutes.Count];
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// How many minutes a compression policy starting on <paramref name="minute"/> has before the next hourly
    /// refresh starts — the space that minute's run actually has, measured forward round the hour.
    ///
    /// <para><b>Why this quantity and not the cadence (#3112).</b> The compression band's WIDTH is derived
    /// from a COUNT (<see cref="CompressionPhaseBandMinutes"/> over
    /// <see cref="CompressionPhaseMaxPerMinute"/>), and nothing in the grid compares a compression run's
    /// DURATION to anything. Both instruments that look as though they would catch it miss it, and one of
    /// them misses by a factor rather than by a margin. #2136's Store Job Over Cadence judges a job against
    /// its own <c>schedule_interval</c>, which for a compression policy is
    /// <see cref="CompressScheduleInterval"/> — so its shipped warning share
    /// (<see cref="RefreshSlotPercentOfHourlyCadence"/>) puts the line at 900 s of a 3,600 s hour, while the
    /// wall a compression run actually faces is its own minute's clearance, as little as 60 s. That is
    /// FIFTEEN times too high on the tightest minute in the band; the 552 s chunk-close run measured below
    /// reads 15.3% of cadence and never approaches the knob at all. And #1778's
    /// <see cref="LogCompressionActivity"/> reports a run that is STILL RUNNING and a chunk backlog, never a
    /// completed run's duration against the space it had. So the compression band is the one band of this
    /// grid with no runtime instrument over its own geometry, and the daily chunk close is precisely a
    /// compression-runtime event.</para>
    ///
    /// <para><b>Relation-AGNOSTIC, deliberately, and it is a LOWER BOUND on the true clearance.</b> The lock
    /// adjacency #3012 formed on is per-relation: a compression policy holds
    /// <c>AccessExclusiveLock</c> on ONE hypertable and only a refresh reading THAT hypertable queues behind
    /// it. This takes the minimum over EVERY refresh start, which is the same predicate
    /// <see cref="CompressionPhaseMinutes"/> is built from — a minimum over a superset can only be smaller
    /// than a minimum over the contending subset, so this figure is never larger than the true clearance and
    /// the watch over it therefore speaks EARLIER than a relation-aware one, never later. Taken in that
    /// direction on purpose: recovering each view's contended relation means parsing
    /// <see cref="HourlyRefreshDefinitions"/>' CREATE text, which is a test-time capability here and not an
    /// hourly-sweep one, and a watch that fails toward flagging a harmless overrun is worth more than one
    /// that can miss a harmful one.</para>
    ///
    /// <para><b>The band's own range, which is what says where the residual is.</b> Clearance falls one
    /// minute per minute across the band: 1,440 s on its first minute down to <b>60 s</b> on its last, that
    /// last figure being exactly one <see cref="LightRefreshStepMinutes"/> step because the bands partition
    /// the hour and the light band opens the next one. The tail minute is always OCCUPIED — the band is sized
    /// to hold the whole catalog at <see cref="CompressionPhaseMaxPerMinute"/> per minute, so every minute in
    /// it carries a policy — so a one-step clearance is a permanent feature of the grid rather than an
    /// arrangement that happens to be tight today. That is the one-sidedness
    /// <see cref="HeaviestRefreshStartMinute"/> accepted by DECISION, now carried as a number: which refresh
    /// band opens the hour was chosen knowing something would still be compressing, and this is how much
    /// space the something has.</para>
    ///
    /// <para><b>What the shipped grid puts where, so the residual is named rather than left to be found.</b>
    /// <c>query_store_stats</c> — the largest hypertable this store has — sits at <b>:42</b> with
    /// <b>1,080 s</b>, and <c>procedure_stats</c> sits at <b>:58</b> with <b>120 s</b>, the tightest placement
    /// any of the large hypertables has. Both figures follow from the catalog's ORDER, so a collector
    /// registered ahead of either moves them; they are pinned as derived values rather than stated as facts
    /// about those two tables, and the prose going stale is what reddens the pin.</para>
    ///
    /// <para><b>The measurement this exists because of, scoped (#3112).</b> In the <c>2026-09-08 00:00Z</c>
    /// hour on ONE store, three compression policies were read at <b>360 s</b> (<c>query_stats</c>),
    /// <b>198 s</b> (<c>query_snapshots</c>) and <b>552 s</b> (<c>query_store_stats</c>) — stated in that
    /// order throughout this paragraph, which is the order of the minutes they held. Those are
    /// <c>collect.store_metrics</c>' hourly <c>background_job</c> snapshot — a LAST-RUN reading, so no
    /// maximum question is answered by them (#3119) — and they are one store's and one night's. A far broader
    /// population exists and is recorded on the change that added this member rather than restated here: a
    /// multi-week <c>timescaledb_information.job_history</c> census of the same hypertables by hour of day,
    /// which is maximum-capable where these readings are not. It puts <c>query_store_stats</c>' typical
    /// midnight run INSIDE the clearance below and its upper tail PAST it. The readings quoted here are
    /// therefore the weaker instrument, and they are kept because they are the ones the shares stated below
    /// are computed from — a figure this file can be checked against beats a larger one it cannot. Under the
    /// grid in force that night the three started at <c>00:26</c>, <c>00:27</c> and <c>00:28</c> with
    /// <b>240 s</b>, <b>180 s</b> and <b>120 s</b> of clearance, so every one of them ran past the refresh
    /// that followed it. The one whose hypertable that refresh also READ was <c>query_store_stats</c>, and it
    /// is that refresh — not the other two — whose runtime went to 160.4 s and then 226.8 s against a
    /// 20–58 s steady state. On the grid this file ships the same three hold <c>:40</c>, <c>:41</c> and
    /// <c>:42</c>, where those readings are <b>30%</b>, <b>17%</b> and <b>51%</b> of their clearance.
    /// <b>Nothing here reduces what midnight carries</b>, and the placement that absorbs it was not chosen
    /// for that: it is a consequence of #3174's re-derivation, which claimed neutrality on CONCENTRATION and
    /// was neutral on it. Clearance is a different axis and it moved.</para>
    ///
    /// <para>Modular in <paramref name="minute"/> rather than range-checked: minute-of-hour arithmetic is
    /// modular anyway, and this feeds an observability line off a catalog timestamp, so an impossible value
    /// must cost the line and never the sweep.</para>
    /// </summary>
    public static int CompressionMinuteClearanceMinutes(int minute)
    {
        var cadence = MinutesInHourlyCadence;
        var start = ((minute % cadence) + cadence) % cadence;
        var clearance = cadence;

        foreach (var view in HourlyRefreshPhaseOrder)
        {
            var distance = (RefreshPhaseMinutesFor(view) - start + cadence) % cadence;

            if (distance < clearance)
            {
                clearance = distance;
            }
        }

        return clearance;
    }

    /// <summary><see cref="CompressionMinuteClearanceMinutes"/> in seconds — the wall a compression run
    /// starting on that minute has to finish inside, named once so the watch line and the band boundary
    /// cannot disagree about where it is (the same reason <see cref="RefreshPhaseSlotSeconds"/>
    /// exists).</summary>
    public static int CompressionMinuteClearanceSeconds(int minute) =>
        CompressionMinuteClearanceMinutes(minute) * 60;

    /// <summary>
    /// The line at which a compression run on <paramref name="minute"/> is worth saying something about:
    /// <see cref="WindowWatchLeadNumerator"/>/<see cref="WindowWatchLeadDenominator"/> of that minute's
    /// clearance, the same lead-time fraction #3044 chose for the heaviest refresh's window.
    ///
    /// <para>Integer division, so the line is never ABOVE the fraction — a watch that rounded up would speak
    /// later than its own stated lead time on some widths and not others.</para>
    /// </summary>
    public static int CompressionClearanceWatchSeconds(int minute) =>
        CompressionMinuteClearanceSeconds(minute) * WindowWatchLeadNumerator / WindowWatchLeadDenominator;

    /// <summary>
    /// Where one observed compression run sits against the clearance the minute it started on had. Produced by
    /// <see cref="ClassifyCompressionClearance"/>; nothing here reads a clock or a catalog, so it pins
    /// directly.
    /// </summary>
    public enum CompressionClearanceBand
    {
        /// <summary>Under <see cref="CompressionClearanceWatchSeconds"/> — the routine band. This is where
        /// every hour but the daily chunk close sits, by a wide margin: an ordinary tick finds nothing
        /// eligible and finishes in well under a second.</summary>
        InsideClearance,

        /// <summary>At or past <see cref="CompressionClearanceWatchSeconds"/> but still inside
        /// <see cref="CompressionMinuteClearanceSeconds"/>: the run still finished before the next refresh
        /// started, with less than the watch's lead fraction to spare.</summary>
        ApproachingRefresh,

        /// <summary>At or past <see cref="CompressionMinuteClearanceSeconds"/>. The run was still holding its
        /// <c>AccessExclusiveLock</c> when AT LEAST ONE hourly refresh started, which is the queue
        /// <see cref="HourlyRefreshPhaseOrder"/>'s stagger exists to keep empty.
        ///
        /// <para><b>At least one, and the band does not say how many.</b> A grid places a job's START; it
        /// cannot bound its END, and no arrangement of a sixty-minute hour contains a run longer than an
        /// hour. A long enough compression run passes several refresh starts in sequence — the light band
        /// alone holds one per minute — so this band must not be read as "one refresh waited". How far past
        /// the FIRST start the run went is reported (<see cref="CompressionActivity.ClearOfRefreshSeconds"/>);
        /// how many starts it passed is not derived, because that needs each refresh's own runtime and not
        /// just its minute.</para></summary>
        RefreshOverrun,
    }

    /// <summary>
    /// Classifies one observed compression runtime against the clearance of the minute it started on.
    ///
    /// <para><b>Both boundaries are inclusive</b>, for the reason
    /// <see cref="ClassifyRefreshSlotHeadroom"/>'s are: a run that took exactly its clearance was still
    /// running when the refresh started, so <see cref="CompressionClearanceBand.RefreshOverrun"/> has to
    /// begin at <c>&gt;=</c> rather than past it.</para>
    ///
    /// <para>Negative and NaN readings classify <see cref="CompressionClearanceBand.InsideClearance"/> rather
    /// than throwing — same posture as the refresh classifier, and the same reason: a catalog handing back
    /// something impossible must cost the line, never the sweep that carries it.</para>
    /// </summary>
    public static CompressionClearanceBand ClassifyCompressionClearance(double observedSeconds, int startMinute)
    {
        if (double.IsNaN(observedSeconds) || observedSeconds < 0d)
        {
            return CompressionClearanceBand.InsideClearance;
        }

        if (observedSeconds >= CompressionMinuteClearanceSeconds(startMinute))
        {
            return CompressionClearanceBand.RefreshOverrun;
        }

        return observedSeconds >= CompressionClearanceWatchSeconds(startMinute)
            ? CompressionClearanceBand.ApproachingRefresh
            : CompressionClearanceBand.InsideClearance;
    }

    /// <summary>
    /// An HOURLY continuous aggregate's refresh policy: <see cref="HourlyRefreshStartOffset"/> of window, an
    /// <see cref="HourlyRefreshScheduleInterval"/> cadence, and this view's own slot on the phase grid.
    /// </summary>
    public static string AddHourlyRefreshPolicySql(string view)
        => AddContinuousAggregatePolicySql(
            view,
            HourlyRefreshStartOffset,
            HourlyRefreshScheduleInterval,
            HourlyRefreshScheduleInterval,
            RefreshPhaseMinutesFor(view));

    /// <summary>
    /// A DAILY continuous aggregate's refresh policy: <see cref="DailyRefreshStartOffset"/> of window on a
    /// daily cadence, and NO <c>initial_start</c> — the daily tier keeps TimescaleDB's finish-to-start
    /// scheduling, exactly as it did before #3012, because it was never near its own cadence and never
    /// appeared in the convoy.
    /// </summary>
    public static string AddDailyRefreshPolicySql(string view)
        => AddContinuousAggregatePolicySql(
            view,
            DailyRefreshStartOffset,
            DailyRefreshScheduleInterval,
            DailyRefreshScheduleInterval,
            phaseMinutes: null);

    /// <summary>The TimescaleDB policy proc behind a continuous-aggregate refresh job — what
    /// <c>timescaledb_information.jobs.proc_name</c> reports, and the leading token of the job label both
    /// <see cref="JobCadenceReadSql"/> and <see cref="StoreSelfMetrics.BackgroundJobInsertSql"/>
    /// build.</summary>
    public const string RefreshPolicyProcName = "policy_refresh_continuous_aggregate";

    /// <summary>
    /// Whether a background job's <c>schedule_interval</c> is ALSO its <c>end_offset</c>, decided from the
    /// job label the store telemetry names it by.
    ///
    /// <para><b>It is the same argument twice for every refresh policy this product creates.</b>
    /// <see cref="AddHourlyRefreshPolicySql"/> passes <see cref="HourlyRefreshScheduleInterval"/> as both,
    /// and <see cref="AddDailyRefreshPolicySql"/> passes <see cref="DailyRefreshScheduleInterval"/> as both;
    /// TimescaleContinuousAggregateTests pins that equality out of the EMITTED statement, so this predicate
    /// cannot outlive the fact it reports.</para>
    ///
    /// <para><b>Which makes "widen the interval" a collection change here, not a relaxed deadline</b> — the
    /// half of #3060 an operator actually hits. On the hourly tier it does three things at once: the refresh
    /// materializes a narrower window, a wider still-filling tail is left unmaterialized, and
    /// <c>QueryStoreBackfill.RollupStoreHorizon</c> — derived as <see cref="HourlyRefreshStartSpan"/> minus
    /// that interval — silently shortens with it. Advice that is correct for a compression or retention
    /// policy alters what gets collected here.</para>
    ///
    /// <para>Keyed on <c>proc_name</c> rather than the view or the job id: the policy proc is what decides
    /// whether the interval carries a second meaning, it is uniform across every deployment, and job ids are
    /// per-deployment (the <see cref="HourlyRefreshPhaseOrder"/> reasoning). The label is
    /// <c>proc_name</c> followed by a space or by nothing, so an exact-or-prefixed-token match is the whole
    /// test — never a substring, which would also match a hypertable that happened to be named after a
    /// policy.</para>
    /// </summary>
    public static bool ScheduleIntervalDoublesAsEndOffset(string? jobLabel)
        => jobLabel is not null
            && (jobLabel.Equals(RefreshPolicyProcName, StringComparison.Ordinal)
                || jobLabel.StartsWith(RefreshPolicyProcName + " ", StringComparison.Ordinal));

    /// <summary>
    /// The refresh policy for a continuous aggregate: materialize
    /// <c>[now - startOffset, now - endOffset]</c> every <c>scheduleInterval</c>. <c>endOffset</c> leaves the
    /// still-filling current bucket unmaterialized (no repeated rework); <c>scheduleInterval</c> matches the
    /// bucket. <c>if_not_exists</c> so a restart re-converges. Prefer
    /// <see cref="AddHourlyRefreshPolicySql"/> / <see cref="AddDailyRefreshPolicySql"/>, which carry the
    /// per-tier decisions; this overload exists so those two share one statement shape.
    ///
    /// <para><b><paramref name="phaseMinutes"/> does two things, and the second one is the less obvious
    /// half.</b> It puts the job on a known minute of the hour, which is the stagger. It also switches the job
    /// to a FIXED schedule: TimescaleDB computes the next start from the previous FINISH when
    /// <c>initial_start</c> is absent, and from the previous START when it is present. Finish-to-start is what
    /// let one convoy phase-lock a whole family permanently — three jobs with unrelated schedules and very
    /// different workloads finished within 119 seconds of each other, and then re-started together every hour
    /// after that. A fixed schedule cannot inherit a phase from a bad hour.</para>
    ///
    /// <para>The anchor is the NEXT whole hour plus the phase, deliberately in the future: a fixed schedule
    /// needs a non-null <c>initial_start</c>, and anchoring forward means the statement never depends on
    /// TimescaleDB's handling of a past anchor. It is computed in UTC (<c>now() AT TIME ZONE 'UTC'</c>, then
    /// back to <c>timestamptz</c>) rather than with a bare <c>date_trunc('hour', now())</c>, which truncates
    /// in the SESSION time zone and would land off-grid on any of the half-hour and quarter-hour zones.</para>
    /// </summary>
    public static string AddContinuousAggregatePolicySql(
        string view,
        string startOffset,
        string endOffset,
        string scheduleInterval,
        int? phaseMinutes)
    {
        var initialStart = phaseMinutes is int phase
            ? $", initial_start => date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 hour' + INTERVAL '{phase.ToString(CultureInfo.InvariantCulture)} minutes'"
            : string.Empty;

        return $"SELECT add_continuous_aggregate_policy('collect.{view}', start_offset => INTERVAL '{startOffset}', end_offset => INTERVAL '{endOffset}', schedule_interval => INTERVAL '{scheduleInterval}', if_not_exists => true{initialStart})";
    }

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
    /// <para><b>MUST run AFTER <see cref="ConvergeContinuousAggregateRefreshAsync"/> (#3012).</b> The policy
    /// half is idempotent only against a policy whose window MATCHES: <c>if_not_exists =&gt; true</c> returns
    /// -1 for an identical policy, but against one whose window differs it raises <c>22023 refresh interval
    /// overlaps with an existing continuous aggregate policy</c>. That is measured, and it is unlike
    /// <c>add_compression_policy</c> and <c>add_retention_policy</c>, which both skip quietly. Run BEFORE the
    /// converge, this sweep would raise on every hourly view of every already-deployed store, swallow it in
    /// the per-aggregate catch, and report a count that reads as "the aggregates are broken" when only their
    /// refresh windows are stale.</para>
    ///
    /// <para>Does NOT backfill history, and on a store that already holds history that leaves a real gap rather
    /// than a merely un-accelerated one (#1759): the aggregates are born WITH NO DATA and each refresh policy
    /// only reaches its own start offset back (<see cref="HourlyRefreshStartOffset"/> hourly,
    /// <see cref="DailyRefreshStartOffset"/> daily), so the materialized span begins at roughly creation minus
    /// that offset and never reaches further back on its own. Reads stay CORRECT because <see cref="RetentionTierRouter"/> routes windows
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
        /* The hourly tier comes from HourlyAggregates rather than being restated here (#3012): the phase grid
           and its collision guard derive from that same list, and a second copy could drift from it. The
           corrected Query Store rollups' ordering requirement lives with the list — L1 is raw-sourced and must
           precede the corrected view, which is hierarchical from it. (The corrected DAILY is L1's SIBLING, not
           the hourly's child: an identity-width hierarchical CAGG is a leaf — see
           CreateQueryStoreStatsCorrectedDailySql.) */
        var aggregates = HourlyAggregates
            .Select(a => (CreateSql: a.CreateSql, View: a.View, Hourly: true))
            .Concat(new[]
        {
            (CreateSql: CreateQueryStatsDailySql,       View: QueryStatsDailyView,       Hourly: false),
            (CreateSql: CreateProcedureStatsDailySql,   View: ProcedureStatsDailyView,   Hourly: false),
            (CreateSql: CreateQueryStoreStatsDailySql,  View: QueryStoreStatsDailyView,  Hourly: false),
            (CreateSql: CreateQueryStoreStatsCorrectedDailySql, View: QueryStoreStatsCorrectedDailyView, Hourly: false),
            (CreateSql: CreateQueryStatsDbDailySql,     View: QueryStatsDbDailyView,     Hourly: false),
            /* The DAY-grain corrected daily (#1869), THREE levels deep: L1 (above) -> L2 interval_daily ->
               daygrain_daily. Both must follow L1 and L2 must precede its own child, which this ordered sweep
               gives — the same requirement the daily tier has, one level longer. */
            (CreateSql: CreateQueryStoreStatsIntervalDailySql, View: QueryStoreStatsIntervalDailyView, Hourly: false),
            (CreateSql: CreateQueryStoreStatsDayGrainDailySql, View: QueryStoreStatsDayGrainDailyView, Hourly: false),
        })
        /* The seven baseline-tier aggregates (#1757; nine until #2007) ride the HOURLY tier: they are sourced from
           raw like the hourly tier, not hierarchically from another CAGG, so they carry no ordering
           requirement against the daily tier. Appended from the single BaselineAggregates list so this sweep
           and the retention list cannot drift apart. HourlyRefreshPhaseOrder appends them from the same list,
           so every view here has a slot on the phase grid. */
        .Concat(BaselineAggregates.Select(a => (CreateSql: a.CreateSql, View: a.View, Hourly: true)))
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
        foreach (var (createSql, view, hourly) in aggregates)
        {
            try
            {
                using (var create = new NpgsqlCommand(createSql, connection) { CommandTimeout = SetupTimeoutSeconds })
                {
                    await create.ExecuteNonQueryAsync(cancellationToken);
                }

                /* Built HERE, not in the array above, so RefreshPhaseMinutesFor's throw for an hourly view
                   missing from HourlyRefreshPhaseOrder costs that one aggregate and names it in the warning
                   below, instead of taking the whole sweep down before the first CREATE runs. */
                var policySql = hourly ? AddHourlyRefreshPolicySql(view) : AddDailyRefreshPolicySql(view);

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

    /// <summary>
    /// Every continuous-aggregate REFRESH job in <c>collect</c>, with the three things #3012's treatment is
    /// made of: the window it re-materializes, whether it is on a fixed schedule, and which minute of the hour
    /// it starts on.
    ///
    /// <para><b>Keyed on the VIEW, and the join matches EITHER identity on purpose.</b> The caller decides
    /// what each view should look like from <see cref="HourlyRefreshPhaseOrder"/>, so this has to hand it a
    /// <c>view_name</c>; nothing here or in the caller reads a job id for anything except passing it back to
    /// <c>alter_job</c>. What the join cannot assume is WHICH name a refresh job reports. The underlying
    /// <c>bgw_job</c> row carries the aggregate's MATERIALIZATION hypertable id, so the obvious form joins on
    /// <c>materialization_hypertable_schema/name</c> — and that form is measured to find NOTHING, because
    /// <c>timescaledb_information.jobs</c> already resolves a continuous-aggregate job back to its USER VIEW
    /// and reports <c>collect</c> / <c>&lt;view&gt;</c>. Matching either identity is therefore not
    /// belt-and-braces for its own sake: it is one measured behaviour plus the one the catalog columns imply,
    /// and it cannot double-count, because a user view lives in <c>collect</c> while a materialization
    /// hypertable lives in <c>_timescaledb_internal</c> — disjoint, so at most one row can match per job.
    /// This was a real defect caught by the live test rather than a hypothetical: the materialization-only
    /// form shipped first and read back nothing at all.</para>
    ///
    /// <para>Emitted as NUMBERS, not text: <c>start_offset</c> comes back as seconds so a C# comparison cannot
    /// be fooled by <c>1 day</c> / <c>1 day 00:00:00</c> / <c>24:00:00</c> all meaning the same interval, and
    /// the phase comes back as a minute-of-hour already converted to UTC (a bare
    /// <c>EXTRACT(MINUTE FROM initial_start)</c> would read the SESSION time zone). A policy created with a
    /// NULL <c>start_offset</c> — refresh from the beginning of time — yields NULL here and is treated as
    /// stale, which is correct: it is the widest window there is.</para>
    /// </summary>
    public const string ContinuousAggregateRefreshStateSql = @"
SELECT
    j.job_id,
    ca.view_name,
    EXTRACT(EPOCH FROM (j.config->>'start_offset')::interval)::bigint AS start_offset_seconds,
    j.fixed_schedule,
    CASE
        WHEN j.initial_start IS NULL THEN NULL
        ELSE EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int
    END AS phase_minutes
FROM timescaledb_information.jobs AS j
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  (ca.view_schema = j.hypertable_schema AND ca.view_name = j.hypertable_name)
  OR  (ca.materialization_hypertable_schema = j.hypertable_schema AND ca.materialization_hypertable_name = j.hypertable_name)
WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
AND   ca.view_schema = 'collect'";

    /// <summary>
    /// Moves one EXISTING hourly refresh policy onto the shipped window and phase. <c>$1</c> the job id
    /// (<c>::integer</c> — <c>alter_job</c> takes <c>job_id INTEGER</c> and PostgreSQL does not down-cast
    /// bigint during function resolution, the #1586 trap), <c>$2</c> the <c>start_offset</c> text, <c>$3</c>
    /// the minute of the hour.
    ///
    /// <para><b>Why this has to exist, and why it is worse than the sibling cases.</b>
    /// <see cref="ConvergeRetentionHorizonSql"/> and <see cref="ConvergeCompressionScheduleAsync"/> exist
    /// because their <c>add_*</c> function returns -1 for a policy the store already has and changes nothing.
    /// <c>add_continuous_aggregate_policy</c> does that only when the window MATCHES. Against a policy whose
    /// window DIFFERS it raises <c>22023 refresh interval overlaps with an existing continuous aggregate
    /// policy</c> — measured on a live store, and it is why
    /// <see cref="EnsureContinuousAggregatesAsync"/> must run AFTER this rather than before. So without this
    /// the narrowing would not merely fail to reach an upgraded store: the create path would raise on all
    /// thirteen hourly views every start, be swallowed by that sweep's per-aggregate isolation, and leave the
    /// policies re-materializing three days an hour forever, with nothing failing until the hypertable grew
    /// into the same convoy.</para>
    ///
    /// <para><c>config</c> is updated with <c>jsonb_set</c> against the job's OWN config so the other keys
    /// (<c>end_offset</c>, <c>mat_hypertable_id</c>) are preserved untouched, which is why this is a
    /// <c>SELECT ... FROM timescaledb_information.jobs</c> rather than a bare function call. <c>scheduled</c>
    /// is deliberately not named: every un-named <c>alter_job</c> parameter means "leave unchanged", so this
    /// cannot arm a paused job.</para>
    /// </summary>
    public const string SetContinuousAggregateRefreshSql = @"
SELECT alter_job(
    j.job_id,
    config => jsonb_set(j.config, '{start_offset}', to_jsonb($2::text)),
    fixed_schedule => true,
    initial_start => date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 hour' + ($3::int * INTERVAL '1 minute'))
FROM timescaledb_information.jobs AS j
WHERE j.job_id = $1::integer";

    /// <summary>
    /// Converges EXISTING hourly continuous-aggregate refresh policies onto both halves of #3012's treatment —
    /// the narrowed <see cref="HourlyRefreshStartOffset"/> window and the
    /// <see cref="RefreshPhaseMinutesFor"/> phase grid — for stores that already have policies.
    ///
    /// <para><b>Behaviour on each of the three store states, because that is the whole contract.</b> A FRESH
    /// store has no refresh jobs yet — its aggregates are created moments LATER — so this reads an empty set
    /// and returns 0. A store still carrying the OLD values gets both halves applied on the first start after
    /// deploy, and the create that follows then finds policies which match. A store an operator already
    /// patched BY HAND to the right window is matched on the window and left alone on it — it is only
    /// re-phased if its job is not on a fixed schedule or not on this grid, which is the one case where code
    /// deliberately wins over a live hand-applied value, and production settled that argument: measured about
    /// two hours after the offsets were applied by hand, every job read <c>fixed_schedule = false</c> and only
    /// one of six hourly refreshes was still on the minute it had been set to. Without a fixed schedule the
    /// next start comes off the previous FINISH, so a hand-applied stagger decays back into coincidence within
    /// hours.</para>
    ///
    /// <para><b>DAILY refresh policies are skipped, by membership rather than by name-matching.</b> A view
    /// that is not on <see cref="HourlyRefreshPhaseOrder"/> is passed over untouched, so the daily tier keeps
    /// <see cref="DailyRefreshStartOffset"/> and its finish-to-start scheduling. This is the guard against the
    /// obvious future regression — a "make every refresh window consistent" edit — arriving through the
    /// converge path instead of through the create path.</para>
    ///
    /// <para>Failure-isolated PER JOB, the #1775 shape: one <c>alter_job</c> that fails (most often because a
    /// least-privilege bring-your-own store's login does not own the job) leaves that one policy on its old
    /// window and the rest still converge. Returns how many it moved.</para>
    /// </summary>
    public static async Task<int> ConvergeContinuousAggregateRefreshAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var hourly = new HashSet<string>(HourlyRefreshPhaseOrder, StringComparer.Ordinal);
        var desiredSeconds = (long)HourlyRefreshStartSpan.TotalSeconds;

        var stale = new List<(int JobId, string View, long? WasSeconds, bool WasFixed, int? WasPhase)>();
        try
        {
            using var probe = new NpgsqlCommand(ContinuousAggregateRefreshStateSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var view = reader.GetString(1);
                if (!hourly.Contains(view))
                {
                    continue;
                }

                var seconds = reader.IsDBNull(2) ? (long?)null : reader.GetInt64(2);
                var fixedSchedule = !reader.IsDBNull(3) && reader.GetBoolean(3);
                var phase = reader.IsDBNull(4) ? (int?)null : reader.GetInt32(4);

                if (seconds == desiredSeconds && fixedSchedule && phase == RefreshPhaseMinutesFor(view))
                {
                    continue;
                }

                stale.Add((Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture), view, seconds, fixedSchedule, phase));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* A plain-PostgreSQL store (the views do not exist) or a TimescaleDB too old to expose
               initial_start. The caller already gates on the extension; nothing to converge either way. */
            logger?.LogDebug("Continuous-aggregate refresh converge: could not read policy jobs: {Message}", ex.Message);
            return 0;
        }

        var converged = 0;
        foreach (var (jobId, view, wasSeconds, wasFixed, wasPhase) in stale)
        {
            var phase = RefreshPhaseMinutesFor(view);
            try
            {
                using var alter = new NpgsqlCommand(SetContinuousAggregateRefreshSql, connection) { CommandTimeout = SetupTimeoutSeconds };
                alter.Parameters.AddWithValue(jobId);
                alter.Parameters.AddWithValue(HourlyRefreshStartOffset);
                alter.Parameters.AddWithValue(phase);
                await alter.ExecuteNonQueryAsync(cancellationToken);
                converged++;

                logger?.LogInformation(
                    "TimescaleDB: moved {View}'s refresh policy to a {Now} window on a fixed :{Phase} schedule (was {WasSeconds}s of window, fixed_schedule={WasFixed}, phase {WasPhase}) — a refresh window wider than its own cadence is what turns a shared lock into a convoy (#3012).",
                    view, HourlyRefreshStartOffset, phase.ToString("00", CultureInfo.InvariantCulture), wasSeconds, wasFixed, wasPhase);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Could not move {View}'s refresh policy (job {JobId}) to a {Interval} window on a fixed :{Phase} schedule — it keeps re-materializing its old window every run (often a permission issue: the store login must own the job): {Message}",
                    view, jobId, HourlyRefreshStartOffset, phase.ToString("00", CultureInfo.InvariantCulture), ex.Message);
            }
        }

        if (converged > 0)
        {
            logger?.LogInformation(
                "TimescaleDB: {Converged}/{Total} hourly refresh policies moved onto the {Interval} window and their own minute on the phase grid ({Minutes} distinct minutes, #3012/#3174).",
                converged, stale.Count, HourlyRefreshStartOffset, HourlyRefreshPhaseOrder.Count);
        }

        return converged;
    }

    /// <summary>Raw-tier retention horizon: keep per-sweep raw ~4 days — comfortably past the hourly CAGG's own
    /// <see cref="HourlyRefreshStartOffset"/> refresh window, so the raw drop never outruns the aggregate that
    /// preserves it. The margin is three days since #3012 narrowed that window; it was one day before, and the
    /// dependency now runs the other way — the refresh window is chosen against its own cadence and this
    /// horizon only has to clear it.</summary>
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
    /// view, and its consumers refresh over at most <see cref="DailyRefreshStartOffset"/> (the corrected
    /// daily; the corrected hourly reaches only <see cref="HourlyRefreshStartOffset"/> back).</para>
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
    /// with the right value. That is asserted rather than only claimed:
    /// <c>EnsureRetentionPolicies_ConvergesAnOldHorizon_PreservingScheduledStateAndNextStart_AgainstDevPostgres</c>
    /// requires the settled third sweep to report moving NOTHING, and nothing else in that test can stand in
    /// for it — every state value it compares reads the same whether this statement was a no-op or a
    /// re-apply of all seventeen horizons. Only <c>config</c> is named, so the job's SCHEDULED state is
    /// preserved exactly:
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
    /// tier's refresh start_offset (4d raw vs the hourly refresh's <see cref="HourlyRefreshStartOffset"/>;
    /// 90d hourly vs the daily refresh's <see cref="DailyRefreshStartOffset"/>), so a drop never removes
    /// history the next tier has not yet materialized. Idempotent (<c>if_not_exists</c>) and
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
        command.CommandTimeout = JobCatalogReadTimeoutSeconds;
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
    /// refresh policy only reaches its own start offset back, so on a store that existed before its rollups
    /// the materialized span begins at roughly creation minus that offset and NEVER reaches further back on
    /// its own.</para>
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
        command.CommandTimeout = JobCatalogReadTimeoutSeconds;
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
    /// Every COMPRESSION-policy job's schedule, with the three things a converged policy is made of: the
    /// cadence it wakes on, whether that cadence is a FIXED schedule, and which minute of the hour it starts
    /// on.
    ///
    /// <para><b>Unfiltered, with staleness decided in C# (#3035).</b> The #1778 form carried
    /// <c>schedule_interval IS DISTINCT FROM INTERVAL '1 hour'</c> in the WHERE clause, and that cannot
    /// express the phase test: the wanted minute is PER HYPERTABLE, so no single SQL literal stands for it and
    /// a job stale only on its phase would be filtered out before the caller ever saw it. The cadence is
    /// therefore emitted as SECONDS as well as text, which is what the typed-INTERVAL comparison used to
    /// provide — <c>01:00:00</c> and <c>1 hour</c> cannot read as a difference and re-alter the same job on
    /// every start — and the text form stays because the line an operator reads names the cadence the policy
    /// LEFT.</para>
    ///
    /// <para>Scoped to compression jobs the SAME tolerant way <see cref="ReadStuckCompressionJobsAsync"/> is
    /// (<c>policy_compression</c> plus the 2.18+ <c>columnstore</c> rebrand). Retention, continuous-aggregate
    /// refresh, reorder and every other job type are deliberately untouched: their cadences are separate
    /// decisions, and the retention jobs in particular carry an armed/paused state (#1680) this must never
    /// disturb.</para>
    ///
    /// <para><b>Two of these columns are younger than the rest of the read, which is why
    /// <see cref="CompressionCadenceOnlyStateSql"/> exists.</b> <c>fixed_schedule</c> and
    /// <c>initial_start</c> entered <c>timescaledb_information.jobs</c> later than <c>schedule_interval</c>
    /// did, so on a store old enough to lack them this statement throws — and #1778's cadence converge, which
    /// only ever needed the older columns, would be lost with it. The caller retries with the narrow read
    /// instead of returning zero.</para>
    ///
    /// <para>The phase comes back as a minute-of-hour already converted to UTC. A bare
    /// <c>EXTRACT(MINUTE FROM initial_start)</c> would read the SESSION time zone, which on any of the
    /// half-hour and quarter-hour zones is a store-wide silent skew rather than a local oddity.</para>
    ///
    /// <para><b><c>hypertable_schema</c> is selected because a bare name is not an identity.</b>
    /// <see cref="CompressionPhaseOrder"/> holds bare table names, so a bring-your-own store carrying its own
    /// <c>wait_stats</c> hypertable in another schema would otherwise match one of ours and be given a minute
    /// this code has no basis for choosing. The cadence converge stays deliberately unscoped — that is
    /// #1778's reach and narrowing it would be a behaviour change — so the schema gates the PHASE only.
    /// Appended rather than inserted, so every ordinal the reader already uses keeps its position: an ordinal
    /// shift in a column list is a defect nothing but a live store can see.</para>
    /// </summary>
    public const string CompressionPolicyStateSql = @"
SELECT
    j.job_id,
    j.hypertable_name,
    j.schedule_interval::text,
    EXTRACT(EPOCH FROM j.schedule_interval)::bigint AS schedule_interval_seconds,
    j.fixed_schedule,
    CASE
        WHEN j.initial_start IS NULL THEN NULL
        ELSE EXTRACT(MINUTE FROM j.initial_start AT TIME ZONE 'UTC')::int
    END AS phase_minutes,
    j.hypertable_schema
FROM timescaledb_information.jobs AS j
WHERE (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')";

    /// <summary>
    /// The same read as <see cref="CompressionPolicyStateSql"/> minus the two columns the PHASE needs — the
    /// fallback for a store whose <c>timescaledb_information.jobs</c> does not have them.
    ///
    /// <para><b>This exists because widening a read can turn a partial capability into a total outage, and
    /// that direction is worse than the one it was widening for.</b> <c>schedule_interval</c> has been in that
    /// view far longer than <c>fixed_schedule</c> and <c>initial_start</c> have. #1778's cadence converge only
    /// ever needed the former, so a store old enough to lack the latter two used to be converged fine — and
    /// once the phase columns were added to the one read this function makes, that store's probe would throw,
    /// be caught, return 0, and lose the cadence converge it already had. A silent TOTAL regression in the
    /// name of a feature it cannot use. Falling back keeps the old behaviour exactly: cadence converged, phase
    /// skipped. Raised by review.</para>
    ///
    /// <para><b>The version range this is for is the one this file already states, not one assumed from the
    /// bundled runtime.</b> Nothing in the product declares a TimescaleDB floor and nothing checks
    /// <c>extversion</c>: <see cref="EnableCompressionSql"/> says the pre-2.18 compression vocabulary is
    /// "preferred here for compatibility across 2.x", the forced-refresh path already handles a store where
    /// <c>force</c> does not exist (2.18+), and the continuous-aggregate converge's own catch already names "a
    /// TimescaleDB too old to expose <c>initial_start</c>" as a state that reaches it. Fixed-schedule
    /// background jobs — and therefore these two columns — are a later 2.x addition than
    /// <c>schedule_interval</c>, so inside a stated 2.x compatibility target they cannot be assumed present.
    /// That is the whole argument for degrading rather than for a floor pin.</para>
    ///
    /// <para>The first four columns are byte-identical to the wide read's, in the same order, because the
    /// caller reads both with one set of ordinals — the phase columns are what it stops reading, not a
    /// different shape it starts reading.</para>
    /// </summary>
    public const string CompressionCadenceOnlyStateSql = @"
SELECT
    j.job_id,
    j.hypertable_name,
    j.schedule_interval::text,
    EXTRACT(EPOCH FROM j.schedule_interval)::bigint AS schedule_interval_seconds
FROM timescaledb_information.jobs AS j
WHERE (j.proc_name LIKE '%compression%' OR j.proc_name LIKE '%columnstore%')";

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
    ///
    /// <para>This is the FOREIGN-hypertable form. A hypertable this product owns takes
    /// <see cref="SetCompressionSchedulePhaseSql"/> instead, which also pins the schedule; leaving a foreign
    /// hypertable's <c>fixed_schedule</c> alone keeps #1778's reach exactly as wide as it was without this
    /// code choosing a minute for a table it knows nothing about.</para>
    /// </summary>
    public static string SetCompressionScheduleSql =>
        $"SELECT alter_job($1::integer, schedule_interval => INTERVAL '{CompressScheduleInterval}')";

    /// <summary>
    /// Moves one existing compression policy onto <see cref="CompressScheduleInterval"/> AND onto its slot on
    /// the compression phase grid. <c>$1</c> the job id (<c>::integer</c>, the #1586 trap), <c>$2</c> the
    /// minute of the hour.
    ///
    /// <para><b>Naming <c>initial_start</c> is the half that makes the minute durable</b>, not decoration:
    /// TimescaleDB computes the next start from the previous FINISH when <c>initial_start</c> is absent, which
    /// is why a compression policy set to a minute by hand slides off it by its own runtime every cycle. The
    /// anchor is the NEXT whole hour plus the phase — deliberately in the future, because a fixed schedule
    /// needs a non-null <c>initial_start</c> and anchoring forward never depends on how a past anchor is
    /// handled — and computed in UTC rather than with a bare <c>date_trunc('hour', now())</c>, which truncates
    /// in the SESSION time zone. Both for the same reasons
    /// <see cref="AddContinuousAggregatePolicySql"/> computes it that way.</para>
    ///
    /// <para><c>scheduled</c> is deliberately not named, so this cannot arm a paused job. That is
    /// load-bearing here rather than defensive: the fixture idiom for a deterministic compression test is a
    /// policy created and parked at <c>scheduled = false</c> in one transaction (#1888), and a converge that
    /// re-armed it would make those tests race the scheduler again.</para>
    /// </summary>
    public static string SetCompressionSchedulePhaseSql =>
        $@"SELECT alter_job(
    $1::integer,
    schedule_interval => INTERVAL '{CompressScheduleInterval}',
    fixed_schedule => true,
    initial_start => date_trunc('hour', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC' + INTERVAL '1 hour' + ($2::int * INTERVAL '1 minute'))";

    /// <summary>
    /// Converges EXISTING compression policies onto <see cref="CompressScheduleInterval"/> (#1778) and onto
    /// the <see cref="CompressionPhaseMinutes"/> grid on a fixed schedule (#3035) — one function owning both
    /// properties, because they are set by the same <c>alter_job</c> and a second sweep would fight this one
    /// for the same rows.
    ///
    /// <para><b>Why converging is the only path that reaches a deployed store, and why the reason is not the
    /// one the refresh side has.</b> <see cref="AddCompressionPolicySql"/> carries both the interval and the
    /// phase, but <c>if_not_exists =&gt; true</c> makes it a documented no-op against a policy the store
    /// already has (measured: returns -1, NOTICE, parameters untouched) — it keys on the policy EXISTING, not
    /// on its parameters matching. So the create path reaches fresh installs only. That is a quiet skip rather
    /// than the <c>22023</c> raise <c>add_continuous_aggregate_policy</c> answers a differing window with,
    /// which is why the compression create can keep running BEFORE this converge while the refresh create has
    /// to run after its own.</para>
    ///
    /// <para><b>Behaviour on each of the three store states.</b> A FRESH store has already had its policies
    /// created by <see cref="ApplyCompressionPolicyAsync"/> and
    /// <see cref="EnsureCollectionLogHypertableAsync"/> moments earlier, WITH <c>initial_start</c>, so this
    /// reads them back matching and returns 0 — the mirror image of the refresh converge, which sees an empty
    /// set on a fresh store because its aggregates do not exist yet. A store carrying OLD values gets both
    /// properties on the first start after deploy. A store an operator already patched BY HAND to a minute is
    /// re-phased anyway, and that is deliberate: a hand-set minute on a finish-to-start job is not a stagger
    /// with a slow leak, it is one with a countdown, and the production store settled the argument by reading
    /// <c>fixed_schedule = f</c> on every compression job it had.</para>
    ///
    /// <para><b>Idempotent by construction</b>: only jobs that DIFFER on cadence or on phase are altered, so
    /// the first start after deploy converges the store and every start after that finds nothing and logs
    /// nothing. FOREIGN hypertables — a bring-your-own store's own tables, fixture tables — keep #1778's
    /// cadence converge and are left off the grid, because <see cref="CompressionPhaseOrder"/> is derived from
    /// the collector catalog and an unrecognised name means "not ours" rather than "forgotten".</para>
    ///
    /// <para>Failure-isolated PER JOB, the #1775 shape: one <c>alter_job</c> that fails (most often because a
    /// least-privilege bring-your-own store's login does not own the job) leaves that one hypertable on its old
    /// schedule and the rest still converge. Returns how many it moved.</para>
    /// </summary>
    public static Task<int> ConvergeCompressionScheduleAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
        => ConvergeCompressionScheduleAsync(connection, logger, CompressionPolicyStateSql, cancellationToken);

    /// <summary>
    /// The seam that lets the FALLBACK DIRECTION be tested: <paramref name="phasedStateSql"/> is the wide read
    /// the public entry point supplies, and a test supplies a deliberately-failing one instead.
    ///
    /// <para><b>Why a seam rather than prose.</b> The property that has to hold is not "the phase applies" but
    /// "a probe failure costs the phase and NOTHING ELSE" — an old store must keep the
    /// <see cref="CompressScheduleInterval"/> cadence converge it already had from #1778. Returning 0 there
    /// would be a total regression wearing a partial's clothes, which is the failure shape this whole area
    /// keeps producing: silent and partial, never loud. Against a runtime that HAS the phase columns there is
    /// no other way to make the wide read fail, so without this the direction could only be reasoned about,
    /// and that is exactly what a reviewer had to do.</para>
    ///
    /// <para>The FALLBACK statement is deliberately NOT injectable — <see cref="CompressionCadenceOnlyStateSql"/>
    /// is always the one used, so a test cannot accidentally prove the degradation against a statement the
    /// product does not ship. And the wide statement being injectable costs nothing: the public path's own
    /// live assertions land real phases, which a wrong wide read could not do.</para>
    /// </summary>
    internal static async Task<int> ConvergeCompressionScheduleAsync(
        NpgsqlConnection connection, ILogger? logger, string phasedStateSql, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var desiredSeconds = (long)CompressScheduleSpan.TotalSeconds;

        var stale = new List<(int JobId, string? Hypertable, string? Interval, bool WasFixed, int? WasPhase, int? Phase)>();

        /* One reader for both statements: the narrow one is the wide one's first four columns in the same
           order, so withPhase decides which ordinals are READ rather than selecting a different shape. */
        async Task ReadStateAsync(string sql, bool withPhase)
        {
            stale.Clear();

            using var probe = new NpgsqlCommand(sql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var hypertable = reader.IsDBNull(1) ? null : reader.GetString(1);
                var intervalText = reader.IsDBNull(2) ? null : reader.GetString(2);
                var seconds = reader.IsDBNull(3) ? (long?)null : reader.GetInt64(3);

                var fixedSchedule = withPhase && !reader.IsDBNull(4) && reader.GetBoolean(4);
                var wasPhase = withPhase && !reader.IsDBNull(5) ? reader.GetInt32(5) : (int?)null;

                /* Schema-exact, because CompressionPhaseOrder holds BARE names: a foreign hypertable
                   named like one of ours must not inherit its minute. */
                var ours = withPhase
                    && !reader.IsDBNull(6)
                    && string.Equals(reader.GetString(6), PgSchemaGenerator.CollectSchema, StringComparison.Ordinal);

                int? phase = ours && hypertable is not null && TryCompressionPhaseMinutesFor(hypertable, out var slot)
                    ? slot
                    : null;

                /* A NULL cadence is the widest wakeup there is, so it counts as stale rather than as
                   "nothing to compare". */
                var cadenceStale = seconds != desiredSeconds;
                var phaseStale = phase is int wanted && (!fixedSchedule || wasPhase != wanted);

                if (!cadenceStale && !phaseStale)
                {
                    continue;
                }

                stale.Add((
                    Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture),
                    hypertable,
                    intervalText,
                    fixedSchedule,
                    wasPhase,
                    phase));
            }
        }

        try
        {
            await ReadStateAsync(phasedStateSql, withPhase: true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* The wide read names fixed_schedule and initial_start, which entered
               timescaledb_information.jobs later than schedule_interval did. Retry WITHOUT them rather than
               giving up: a store old enough to lack them was converged fine by #1778 before the phase was
               added to this read, and returning 0 here would silently take that away as well. See
               CompressionCadenceOnlyStateSql. */
            /* The catch is deliberately broad — the file's existing tolerant style — so this line must not
               assert a cause it has not established. It states what was observed and what it costs, and
               offers the two candidate reasons as candidates. Raised by review. */
            logger?.LogInformation(
                "TimescaleDB: could not read compression-policy schedules with their phase ({Message}) — retrying without it, so the {Interval} tick (#1778) still converges and only the phase is skipped. The reason is not established here: a job catalog predating fixed_schedule/initial_start fails this way on every start, while a transient store error fails this way once and the next start pins the phases. Either way these policies keep TimescaleDB's finish-to-start scheduling until a phased read succeeds, so they drift through the refresh slots (#3035).",
                ex.Message, CompressScheduleInterval);

            try
            {
                await ReadStateAsync(CompressionCadenceOnlyStateSql, withPhase: false);
            }
            catch (Exception narrow) when (narrow is not OperationCanceledException)
            {
                /* A plain-PostgreSQL store (the views do not exist) or a store hiccup. The caller already
                   gates on the extension; nothing to converge either way. */
                logger?.LogDebug("Compression-schedule converge: could not read policy jobs: {Message}", narrow.Message);
                return 0;
            }
        }

        var converged = 0;
        foreach (var (jobId, hypertable, interval, wasFixed, wasPhase, phase) in stale)
        {
            try
            {
                if (phase is int slot)
                {
                    using var alter = new NpgsqlCommand(SetCompressionSchedulePhaseSql, connection) { CommandTimeout = SetupTimeoutSeconds };
                    alter.Parameters.AddWithValue(jobId);
                    alter.Parameters.AddWithValue(slot);
                    await alter.ExecuteNonQueryAsync(cancellationToken);
                    converged++;

                    logger?.LogInformation(
                        "TimescaleDB: retuned {Hypertable}'s compression policy from a {Was} tick to {Now} on a fixed :{Phase} schedule (was fixed_schedule={WasFixed}, phase {WasPhase}) — an unpinned policy computes its next start from its last FINISH, so it drifts by its own runtime every cycle and laps the hour through every refresh slot in turn (#3035).",
                        hypertable, interval, CompressScheduleInterval, slot.ToString("00", CultureInfo.InvariantCulture), wasFixed, wasPhase);
                }
                else
                {
                    using var alter = new NpgsqlCommand(SetCompressionScheduleSql, connection) { CommandTimeout = SetupTimeoutSeconds };
                    alter.Parameters.AddWithValue(jobId);
                    await alter.ExecuteNonQueryAsync(cancellationToken);
                    converged++;

                    logger?.LogInformation(
                        "TimescaleDB: retuned {Hypertable}'s compression policy from a {Was} tick to {Now} — that is the longest an already-eligible chunk can now sit uncompressed.",
                        hypertable, interval, CompressScheduleInterval);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger?.LogWarning(
                    "Could not converge {Hypertable}'s compression policy (job {JobId}) onto a {Interval} tick — it keeps its {Was} cadence and its old schedule, so its newest closed chunk stays uncompressed longer and its tick keeps drifting through the refresh slots (often a permission issue: the store login must own the job): {Message}",
                    hypertable, jobId, CompressScheduleInterval, interval, ex.Message);
            }
        }

        if (converged > 0)
        {
            logger?.LogInformation(
                "TimescaleDB: {Converged}/{Total} compression policies converged onto a {Interval} tick and, where the hypertable is ours, onto the compression phase grid (#1778 — TimescaleDB's own default for 1-day chunks is 12 hours; #3035 — an unpinned policy drifts finish-to-start).",
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
            using var command = new NpgsqlCommand(StuckCompressionJobsSql, connection) { CommandTimeout = JobCatalogReadTimeoutSeconds };

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
    /// The #2136 job-cadence catalog read, public for the reason <see cref="RetentionHoldReadSql"/> is: one
    /// of its projections is now load-bearing for what an ALERT SAYS, so it belongs in CI rather than only
    /// in a throwaway harness. <see cref="StoreJobCadenceReading.JobName"/> is built <c>proc_name</c> FIRST,
    /// which is what lets <see cref="ScheduleIntervalDoublesAsEndOffset"/> decide whether widening this
    /// job's interval would move an <c>end_offset</c>; reversing the concatenation would keep collecting
    /// perfectly good readings while silently restoring the wrong remedy text.
    ///
    /// <para><c>job_stats</c> for the same reason the #1778 observability path uses it (maintained
    /// unconditionally; the per-execution history table is empty unless job-execution logging is on). Only
    /// a SUCCESSFUL last run judges: a failed run's duration is not a cadence signal, and job failures are
    /// their own condition (<c>total_failures</c> rides the V56 telemetry).</para>
    /// </summary>
    public const string JobCadenceReadSql = @"
SELECT
    j.job_id,
    j.proc_name || coalesce(' ' || j.hypertable_name, ''),
    (EXTRACT(EPOCH FROM js.last_run_duration) * 1000)::bigint,
    (EXTRACT(EPOCH FROM j.schedule_interval) * 1000)::bigint
FROM timescaledb_information.job_stats AS js
JOIN timescaledb_information.jobs AS j USING (job_id)
WHERE js.last_run_status = 'Success'";

    /// <summary>
    /// Every background job's last-run duration against its own schedule interval (#2136) — the readings the
    /// Store Job Over Cadence self-alert judges. Tolerant like
    /// <see cref="ReadStuckCompressionJobsAsync"/> — a plain-PG store or a hiccup yields no readings, never
    /// an exception. See <see cref="JobCadenceReadSql"/> for the statement and its decisions.
    /// </summary>
    public static async Task<IReadOnlyList<StoreJobCadenceReading>> ReadJobCadenceReadingsAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var readings = new List<StoreJobCadenceReading>();
        try
        {
            using var command = new NpgsqlCommand(JobCadenceReadSql, connection) { CommandTimeout = JobCatalogReadTimeoutSeconds };
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
    /// Everything <see cref="HeaviestRefreshRuntimeSql"/> and <see cref="OtherHourlyRefreshRuntimesSql"/>
    /// have in common: the projection, the MEASURED OR-join, and the two filters that make the rows refresh
    /// policies on this store's own aggregates. The two statements differ only in which views they keep.
    ///
    /// <para><b>Shared rather than copied, and the copy it replaces is the reason (#3182).</b> This join was
    /// already carried in two places — here and
    /// <see cref="ContinuousAggregateRefreshStateSql"/> — because the materialization-hypertable arm ALONE
    /// was measured to find nothing, so both arms have to be present and a re-guessed join reads back empty
    /// rather than wrong. A third copy would be a third chance to lose an arm, and it would be the copy with
    /// the fewest readers. One text, two filters.</para>
    ///
    /// <para>Not a full statement on its own: it opens with the <c>FROM</c> and ends inside the
    /// <c>WHERE</c>, so a caller appends its own <c>AND</c> clauses. That shape is what lets the composed
    /// text be byte-identical to what each statement used to spell out, which is what keeps the pins on them
    /// reading the statements rather than this fragment.</para>
    /// </summary>
    private const string RefreshPolicyRuntimeProjectionSql = @"
SELECT
    ca.view_name,
    EXTRACT(EPOCH FROM js.last_run_duration)::double precision AS last_run_seconds
FROM timescaledb_information.jobs AS j
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  (ca.view_schema = j.hypertable_schema AND ca.view_name = j.hypertable_name)
  OR  (ca.materialization_hypertable_schema = j.hypertable_schema AND ca.materialization_hypertable_name = j.hypertable_name)
JOIN timescaledb_information.job_stats AS js USING (job_id)
WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
AND   ca.view_schema = 'collect'";

    /// <summary>
    /// The last SUCCESSFUL run of <see cref="HeaviestHourlyRefreshView"/>'s refresh policy, in seconds — the
    /// live figure <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/>'s envelope is about (#3044).
    ///
    /// <para><b>Keyed on the VIEW, and the join is <see cref="ContinuousAggregateRefreshStateSql"/>'s
    /// measured one rather than a fresh guess.</b> A refresh job's identity cannot be a job id — those are
    /// assigned per deployment — and it cannot be the display string the V56 telemetry builds either, because
    /// matching on a concatenation is the drifting paraphrase this file keeps warning about. It has to be the
    /// view name, which is what the grid is keyed on. The OR-join is copied deliberately: the
    /// materialization-hypertable form alone was measured to find NOTHING, and the two identities are disjoint
    /// (a user view lives in <c>collect</c>, a materialization hypertable in
    /// <c>_timescaledb_internal</c>), so at most one row matches per job.</para>
    ///
    /// <para><c>last_run_status = 'Success'</c> for the reason #2136's read has it: a failed run's duration is
    /// not an envelope reading, and job failures are their own condition. <see cref="HeaviestHourlyRefreshView"/>
    /// is a compile-time constant, so it interpolates like every other literal here.</para>
    ///
    /// <para><b>Deliberately a SAMPLE and not a record of every run.</b> The refresh and the sweep that reads
    /// this both tick hourly but on independent anchors, and the refresh's start-minute walks (finish-to-start,
    /// so it advances by its own runtime each cycle), so some runs are seen twice and some not at all. That is
    /// adequate and it is what the condition needs: the thing being watched is a runtime trending with volume
    /// over days, and a figure that persists near the line is seen by every tick. Neither this read nor the
    /// recorded series it is often confused with is MAX-PRESERVING: <c>collect.store_metrics</c>
    /// (<c>object_kind = 'background_job'</c>, #2136/V56) is the same hourly grain taken by a different
    /// sweep, and the daily point <c>get_store_metrics</c> serves from it is that day's LAST reading rather
    /// than the day's largest — so a maximum question asked of either lands short, and a day's peak is
    /// dropped rather than smoothed. The route that carries one row per run is
    /// <c>timescaledb_information.job_history</c>, named on
    /// <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> and read there for the live envelope
    /// (#3119). This read supplies the BOUND, which is what was missing, not the history.</para>
    /// </summary>
    public static string HeaviestRefreshRuntimeSql =>
        $@"{RefreshPolicyRuntimeProjectionSql}
AND   ca.view_name = '{HeaviestHourlyRefreshView}'
AND   js.last_run_status = 'Success'";

    /// <summary>
    /// The same read pointed at every OTHER continuous aggregate refresh policy — the live feed
    /// <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> did not have (#3182).
    ///
    /// <para><b>Why this exists at all.</b> That constant is not merely asserted against; it is ARITHMETIC
    /// INPUT — <see cref="CompressionPhaseGuardMinutes"/> is it rounded up to a whole minute, so a light
    /// refresh running longer than the constant records leaves a compression policy able to start while that
    /// refresh still holds <c>AccessShareLock</c>, which is #3012's convoy. Until #3182 nothing read a light
    /// refresh's runtime back at all: the heaviest one had <see cref="HeaviestRefreshRuntimeSql"/> and the
    /// other twelve had no live reading keyed to a view anywhere in the product. #2136's
    /// <see cref="JobCadenceReadSql"/> does read their durations, but it keys on
    /// <c>proc_name || hypertable_name</c> and a refresh policy's <c>hypertable_name</c> is the
    /// MATERIALIZATION hypertable, so those readings cannot be attributed to a view without this join —
    /// which is why the gap survived a check that looks like it covers them.</para>
    ///
    /// <para><b>Filtered to "not the heaviest" in SQL and to "hourly" in C#, which is a split rather than an
    /// inconsistency.</b> Excluding one named view is a compile-time constant and belongs in the statement.
    /// Deciding which views are HOURLY is <see cref="HourlyRefreshPhaseOrder"/>'s job — it is the registry
    /// this whole grid is keyed on, a view missing from it is already a stated defect
    /// (<see cref="RefreshPhaseMinutesFor(string)"/> throws for one), and interpolating a runtime list into a
    /// statement would put a second copy of that registry in SQL text where nothing checks it against the
    /// first. So the daily tier's policies come back from the read and are dropped by
    /// <see cref="ReadOtherHourlyRefreshRuntimesAsync"/> against the registry.</para>
    ///
    /// <para><c>last_run_status = 'Success'</c> for the reason every sibling read has it: a failed run's
    /// duration is not a runtime reading, and job failures are their own condition.</para>
    /// </summary>
    public static string OtherHourlyRefreshRuntimesSql =>
        $@"{RefreshPolicyRuntimeProjectionSql}
AND   ca.view_name <> '{HeaviestHourlyRefreshView}'
AND   js.last_run_status = 'Success'";

    /// <summary>
    /// Reads <see cref="HeaviestRefreshRuntimeSql"/>. Returns null when there is no reading — a fresh store
    /// whose policy has not completed a run, a plain-PostgreSQL store, a store hiccup — never a synthesized
    /// zero, which would read as "finished instantly" and is the honest-empty rule this store's collectors are
    /// held to. Failure-isolated to null the same way <see cref="ReadCompressionActivityAsync"/> is to an empty
    /// list: observability must never be able to break the sweep that carries it.
    /// </summary>
    public static async Task<HeaviestRefreshSlotReading?> ReadHeaviestRefreshRuntimeAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using var command = new NpgsqlCommand(HeaviestRefreshRuntimeSql, connection) { CommandTimeout = JobCatalogReadTimeoutSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0) || reader.IsDBNull(1))
            {
                return null;
            }

            return new HeaviestRefreshSlotReading(
                reader.GetString(0),
                Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug("Heaviest-refresh slot headroom: could not read job stats: {Message}", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Reads <see cref="OtherHourlyRefreshRuntimesSql"/> and keeps the rows whose view is on
    /// <see cref="HourlyRefreshPhaseOrder"/> — one reading per OTHER hourly refresh policy that has
    /// completed a successful run (#3182).
    ///
    /// <para>Failure-isolated to an EMPTY LIST, the posture <see cref="ReadCompressionActivityAsync"/> and
    /// <see cref="ReadJobCadenceReadingsAsync"/> take, rather than to a null the way the single-row heaviest
    /// read is: a list read that found nothing and a list read that failed are the same absence of readings
    /// to the caller, and the honest-empty rule is that neither is allowed to become a synthesized zero.</para>
    ///
    /// <para><b>A DAILY policy's row is dropped rather than logged.</b> The statement cannot tell the tiers
    /// apart — both use <see cref="RefreshPolicyProcName"/> — and a daily aggregate's runtime is not
    /// something <see cref="OtherHourlyRefreshObservedCeilingSeconds"/> bounds, so measuring it against that
    /// constant would manufacture a finding out of a tier mismatch. Dropped silently because it is the
    /// EXPECTED shape of the result rather than an anomaly: the daily tier is supposed to be there.</para>
    /// </summary>
    public static async Task<IReadOnlyList<HourlyRefreshRuntimeReading>> ReadOtherHourlyRefreshRuntimesAsync(
        NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        var readings = new List<HourlyRefreshRuntimeReading>();
        var hourly = new HashSet<string>(HourlyRefreshPhaseOrder, StringComparer.Ordinal);

        try
        {
            using var command = new NpgsqlCommand(OtherHourlyRefreshRuntimesSql, connection) { CommandTimeout = JobCatalogReadTimeoutSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.IsDBNull(0) || reader.IsDBNull(1))
                {
                    continue;
                }

                var view = reader.GetString(0);
                if (!hourly.Contains(view))
                {
                    continue;
                }

                readings.Add(new HourlyRefreshRuntimeReading(
                    view, Convert.ToDouble(reader.GetValue(1), CultureInfo.InvariantCulture)));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(
                "Light-refresh ceiling freshness: could not read job stats: {Message}", ex.Message);
        }

        return readings;
    }

    /// <summary>
    /// Logs <see cref="ReadHeaviestRefreshRuntimeAsync"/>'s reading at a level proportionate to what it says —
    /// the #3044 watch, and the reason it is a LOG LINE rather than a health band or a self-alert.
    ///
    /// <para><b>Not a band.</b> The Collection Health bands are keyed on (server, collector). This is a
    /// store-side background job, which is neither, so there is no row for it — and #1852's inherited
    /// constraints are that the band order and semantics do not change and that a new informational signal
    /// never reaches the banding at all.</para>
    ///
    /// <para><b>Not a second self-alert, and the reason is arithmetic rather than taste.</b> #2136's Store Job
    /// Over Cadence already judges this job's <c>last_run_duration</c> against its own schedule interval, and
    /// its default warning knob of 25% of a 3,600 s cadence lands on 900 s, inside the
    /// <see cref="RefreshPhaseSlotSeconds"/> window. So an alert at the window width would fire on the same
    /// job, the same reading and the same hour as #2136 does, which is what rules the alert form out. What
    /// #2136 would not do is make the bound reliable: that 25% is a store-backed operator knob clamped
    /// [5, 100] with no relationship to the grid at all since #3174 broke the derivation
    /// (<see cref="RefreshSlotPercentOfHourlyCadence"/>), so raising it to 50 to quiet a busy store silently
    /// moves the effective line to 1,800 s — past the invalidation point — and a re-derived grid moves the
    /// window underneath a knob that cannot follow it.</para>
    ///
    /// <para><b>That the two lines coincide is a coincidence of two independent decisions, and the clearest
    /// evidence is that #2136 does not know this job's size.</b> Its clamp is justified in
    /// <c>DarlingAlertSettings</c> on the grounds that "the production worst runs ~7% of cadence" — 252 s —
    /// while <see cref="HeaviestHourlyRefreshObservedCeilingSeconds"/> here records 896 s, which is <b>24.9%
    /// of the same 3,600 s cadence</b> — so the line #2136 lands on is calibrated as though this job ran
    /// under a third of its actual length, and nothing connects the two numbers. #2136's own remedy text —
    /// "extend the job's schedule_interval" —
    /// is actively wrong for this one, because <see cref="HourlyRefreshScheduleInterval"/> is also the
    /// <c>end_offset</c> and widening it changes what the aggregate materializes without touching the slot.
    /// A line derived from the slot, keyed on the view, naming the actual remedy, is the form that stays
    /// correct when either of those numbers moves.</para>
    ///
    /// <para><b>Levels.</b> The routine band is Debug — an hourly Information line about a healthy job is
    /// how a signal gets buried (the discipline <see cref="LogCompressionActivity"/> already states).
    /// Approaching the slot is a Warning: still true, still time to act. At or past the slot it is an Error,
    /// because a documented precondition of the shipped compression grid is now FALSE — the highest level a
    /// log line has, and still not an alert, because the action it calls for is re-deriving #3035's grid
    /// rather than anything an operator does tonight.</para>
    /// </summary>
    public static void LogHeaviestRefreshSlotHeadroom(HeaviestRefreshSlotReading? reading, ILogger? logger)
    {
        if (logger is null || reading is null)
        {
            return;
        }

        switch (reading.Headroom)
        {
            case RefreshSlotHeadroom.SlotExceeded:
                logger.LogError(
                    "TimescaleDB: {View}'s hourly refresh last ran {Seconds:F0}s, at or past the {Slot}s window it has to fit inside ({Percent:F1}% of it) — so excluding that window is no longer enough and the compression phase grid's stated precondition is false. The grid has to be RE-DERIVED (#3035), not renumbered: the hour cannot spare a wider window than {Window} minutes while the compression band still spreads every hypertable, so the fix is fewer compression minutes, a longer cadence for this aggregate, or splitting it (#3044/#3174).",
                    reading.View, reading.LastRunSeconds, RefreshPhaseSlotSeconds, reading.PercentOfSlot, HeaviestRefreshWindowMinutes);
                break;

            case RefreshSlotHeadroom.ApproachingSlot:
                logger.LogWarning(
                    "TimescaleDB: {View}'s hourly refresh last ran {Seconds:F0}s against the {Slot}s refresh slot it has to fit inside ({Percent:F1}% of it, {Clear:F0}s clear) — past the {Warn}s watch line. These runtimes scale with raw data volume, and at the slot width the compression phase grid has to be re-derived rather than renumbered (#3035). Its per-run history is timescaledb_information.job_history, one row per run, but only where timescaledb.enable_job_execution_logging is on — it is off by default, and a store provisioned before that GUC gained its own conf marker reports nothing there until it heals, so an empty result is that gap and not a quiet hour (#3175/#3177). The hourly collect.store_metrics series (object_kind = 'background_job') samples one reading an hour and serves a daily point that is the day's LAST, so neither answers a maximum question on its own (#3044, #3119).",
                    reading.View, reading.LastRunSeconds, RefreshPhaseSlotSeconds, reading.PercentOfSlot,
                    reading.ClearOfSlotSeconds, RefreshSlotWarningSeconds);
                break;

            default:
                logger.LogDebug(
                    "TimescaleDB: {View}'s hourly refresh last ran {Seconds:F0}s, {Percent:F1}% of its {Slot}s slot ({Clear:F0}s clear, watch line {Warn}s).",
                    reading.View, reading.LastRunSeconds, reading.PercentOfSlot, RefreshPhaseSlotSeconds,
                    reading.ClearOfSlotSeconds, RefreshSlotWarningSeconds);
                break;
        }
    }

    /// <summary>
    /// The steady-state deadline for the hourly job-catalog reads (#2813 review). Deliberately NOT
    /// <see cref="SetupTimeoutSeconds"/>: that 300s budget is documented for one-time BULK SETUP — the
    /// migrate session's first <c>migrate_data</c>, hypertable conversion — and reusing it here would let a
    /// single stalled catalog read block the whole hourly self-alert sweep tick (disk pressure, compression
    /// health, job cadence and this check run sequentially in it) for five minutes. That is exactly the
    /// failure shape #2810 and #2871 removed from the analysis pass, and it would be perverse to
    /// reintroduce it in the same release.
    ///
    /// <para>Bounded both ways. BELOW: this is a pure catalog round trip over
    /// <c>timescaledb_information.jobs</c> and <c>chunks</c> with no hypertable scan — it returned in well
    /// under a second on all three production stores. ABOVE: it shares an hourly tick with three sibling
    /// checks, so its ceiling must be a small fraction of that hour rather than a fraction of a setup pass.
    /// 30s is the value its direct sibling <see cref="ReadJobCadenceReadingsAsync"/> gets from Npgsql's
    /// default — stated explicitly here so it is a DECISION rather than an inherited number nobody chose,
    /// which is the whole lesson of #2810.</para>
    /// </summary>
    public const int JobCatalogReadTimeoutSeconds = 30;

    /// <summary>The #2813 retention-hold catalog read, public so its two load-bearing predicates —
    /// <c>proc_name = 'policy_retention'</c> and the <c>collect</c> schema scope — are pinned in CI rather
    /// than only in a throwaway harness. Scoping matters for CORRECTNESS, not tidiness: the alert this
    /// feeds asserts the rollup-coverage gate is the cause and tells the reader not to arm the policy by
    /// hand, which would be wrong advice about a retention policy this product never created (review
    /// catch). The mutating siblings ConvergeRetentionHorizonSql / SetRetentionScheduleSql scope the same
    /// way.</summary>
    public const string RetentionHoldReadSql = @"
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
WHERE j.proc_name = 'policy_retention'
  AND j.hypertable_schema = 'collect'";

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

        var readings = new List<RetentionHoldReading>();
        try
        {
            using var command = new NpgsqlCommand(RetentionHoldReadSql, connection) { CommandTimeout = JobCatalogReadTimeoutSeconds };
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
            using var command = new NpgsqlCommand(CompressionActivitySql, connection) { CommandTimeout = JobCatalogReadTimeoutSeconds };
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
    ///
    /// <para><b>The #3112 clearance watch rides the same reading, and adds no read, no timer and no per-policy
    /// routine line.</b> Every term it needs is already in <see cref="CompressionActivity"/>: the hypertable
    /// names the minute, the minute gives
    /// <see cref="TimescaleSupport.CompressionMinuteClearanceSeconds"/>, and the last completed run's duration
    /// is already projected. So the same discipline applies as above — nothing per-policy in the routine band,
    /// because seventy Debug lines an hour is the burial this method's first paragraph is about. One Debug
    /// summary carries the TIGHTEST reading in the band, which is the one figure that says how close the grid
    /// is; a run at or past its watch line gets Information; an overrun gets Warning.</para>
    ///
    /// <para><b>Warning rather than Error for an overrun, unlike
    /// <see cref="LogHeaviestRefreshSlotHeadroom"/>'s exceeded band.</b> There, past the window means a
    /// documented precondition of the shipped grid is FALSE. Here it does not: the compression guard is
    /// one-sided BY DECISION (<see cref="HeaviestRefreshStartMinute"/>), so a policy on the tail of the band
    /// running past the hour is the accepted residual being consumed rather than an invariant breaking. It
    /// still wants saying, because that residual is what #3112's midnight band ran through, and nothing said
    /// it.</para>
    ///
    /// <para><b>Emitted unconditionally rather than folded into the existing all-clear summary.</b> That
    /// summary only fires when nothing is running and no chunk is eligible, which is the routine hour — so
    /// hanging the clearance figure off it would suppress it in exactly the hour the daily chunk close makes
    /// interesting.</para>
    ///
    /// <para><b>The off-phase report is one line for the store too, and that branch is where the discipline
    /// binds hardest rather than least.</b> A policy starting on a minute other than its assigned one is a
    /// property of the store, not of the hypertable: the phased read either succeeds or it does not, so on a
    /// job catalog too old to expose <c>fixed_schedule</c>/<c>initial_start</c> every policy is off phase at
    /// once and stays that way, because that failure does not heal. Per-hypertable it would be one
    /// Information line per hypertable per hour, indefinitely, on precisely the store where the least can be
    /// done about it. It is Information rather than Debug because it is the PRECONDITION of every other
    /// figure in this block — a clearance measured on a store whose grid was never applied describes where a
    /// policy ran, not where this product placed it.</para>
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

        LogCompressionClearance(activity, logger);
    }

    /// <summary>
    /// The #3112 half of <see cref="LogCompressionActivity"/>: every completed compression run against the
    /// clearance the minute it started on had. Split out as its own method rather than threaded through the
    /// loop above so the two concerns can be read, and tested, apart — #1778 asks "is compression keeping
    /// up", this asks "did a run reach the next refresh".
    /// </summary>
    private static void LogCompressionClearance(IReadOnlyList<CompressionActivity> activity, ILogger logger)
    {
        CompressionActivity? tightest = null;
        CompressionActivity? driftExample = null;
        var offPhase = 0;
        var onTheGrid = 0;

        foreach (var item in activity)
        {
            if (item.ClearanceBand is not CompressionClearanceBand band
                || item.ClearanceMinute is not int minute
                || item.LastRunDuration is not TimeSpan duration)
            {
                continue;
            }

            if (IsTighter(item, tightest))
            {
                tightest = item;
            }

            /* DERIVED FROM THE MINUTE, not defaulted from a nullable. Both are non-null inside this walk -
               the loop guard above has already skipped a reading with no minute or no duration - so taking
               them off the minute removes the `?? 0` rather than hiding one, and a zero clearance below is a
               REAL zero rather than a defaulted null. */
            var clearance = CompressionMinuteClearanceSeconds(minute);
            var clear = clearance - duration.TotalSeconds;

            /* THE SHARE IS PATTERN-MATCHED, NEVER DEFAULTED, and that is the whole of this block's shape.
               PercentOfClearance is null exactly when the clearance is ZERO - the sink case - and defaulting
               it to 0d rendered that case as "0.0%", byte-identical to a near-instant run with the whole band
               to spare. A reading that gets more alarming as it gets worse everywhere else wrapped around to
               look BEST in the one case with no room at all, and 0.0% could not be told from "finished
               instantly" by the operator reading it. So the two measurable bands take the share by pattern
               and the null falls through to its own finding. Raised by review. */
            switch (band)
            {
                case CompressionClearanceBand.RefreshOverrun when item.PercentOfClearance is not null:
                    logger.LogWarning(
                        "TimescaleDB: compression of {Hypertable} last ran {Seconds:F0}s from :{Minute:00}, at or past the {Clearance}s it had before the next hourly refresh started ({Over:F0}s past it) — so it was still holding AccessExclusiveLock when a refresh wanted the table. This is the daily chunk close: every hypertable's newest {Days}d chunk becomes eligible at the same UTC midnight, so one tick a day carries a full day's rewrite while the other twenty-three find nothing (#3112). The grid's guard is one-sided by decision, so the tail of the compression band has one refresh step of clearance and this is that residual being spent. Widening it costs minutes the heaviest refresh's window is holding (#3174).",
                        item.HypertableName, duration.TotalSeconds, minute, clearance, -clear, CompressAfterDays);
                    break;

                case CompressionClearanceBand.ApproachingRefresh when item.PercentOfClearance is double percent:
                    logger.LogInformation(
                        "TimescaleDB: compression of {Hypertable} last ran {Seconds:F0}s from :{Minute:00}, {Percent:F1}% of the {Clearance}s it had before the next hourly refresh ({Clear:F0}s clear, watch line {Watch}s).",
                        item.HypertableName, duration.TotalSeconds, minute, percent,
                        clearance, clear, CompressionClearanceWatchSeconds(minute));
                    break;

                /* THE SINK CASE, and it is a DIFFERENT FINDING rather than the same one with an awkward
                   number. An ordinary overrun says the day's rewrite outgrew a tight minute, and its remedy
                   is the band's width. A policy sitting on a refresh's OWN minute says the phase grid was
                   never applied to it, and its remedy is the converge - so the chunk-close reasoning above
                   would point an operator at the wrong lever. Reached from either band, so an
                   ApproachingRefresh with no share - which the arithmetic does not currently allow, since a
                   zero clearance makes both boundaries zero and every non-negative run an overrun - would
                   land here rather than on a milder line. That is the direction to be wrong in. */
                case CompressionClearanceBand.RefreshOverrun:
                case CompressionClearanceBand.ApproachingRefresh:
                    logger.LogWarning(
                        "TimescaleDB: compression of {Hypertable} last ran {Seconds:F0}s from :{Minute:00}, which is a minute an hourly refresh ALSO starts on — so it had NO CLEARANCE whatsoever, not a small amount, and was contending from the moment it began. Its share of clearance is UNDEFINED rather than low. A compression policy on a refresh's own minute means the phase grid was never applied to it, so the remedy is the phase converge and not the band's width (#3035/#3112).",
                        item.HypertableName, duration.TotalSeconds, minute);
                    break;

                default:
                    break;
            }

            if (item.OffAssignedPhase)
            {
                offPhase++;
                driftExample ??= item;
            }

            onTheGrid++;
        }

        /* ONE line for the whole store, not one per hypertable, and this is the branch where that matters
           most rather than least. The condition is a property of the STORE - a job catalog too old to expose
           fixed_schedule/initial_start fails the phased read on every start, which
           ConvergeCompressionScheduleAsync documents - so it is true of every policy at once and it does not
           heal. Per-hypertable, it would be seventy Information lines an hour forever on exactly the store
           where the least can be done about it, which is the burial this method's own discipline forbids and
           which the first version of this branch did. Raised by review. */
        if (offPhase > 0 && driftExample is not null)
        {
            logger.LogInformation(
                "TimescaleDB: {OffPhase} of {OnGrid} compression policies last started on a minute other than the one the phase grid assigns them (e.g. {Hypertable} on :{Observed:00} rather than :{Assigned:00}) — so every clearance figure in this block is measured from where those policies actually ran, not from where this product placed them. A policy that drifts by its own runtime each cycle is the finish-to-start scheduling #3035's fixed schedule replaces, which the converge cannot apply on a job catalog too old to expose initial_start.",
                offPhase, onTheGrid, driftExample.HypertableName,
                driftExample.ObservedStartMinute ?? 0, driftExample.AssignedPhaseMinute ?? 0);
        }

        /* The summary splits the same way and for the same reason: the policy SELECTED because it has no
           clearance must not be REPORTED as a low share of it. Everything here is derived from the tightest
           reading's own minute, so no rendered quantity is a defaulted null. */
        if (tightest is not null && tightest.ClearanceMinute is int tightestMinute)
        {
            var widest = CompressionMinuteClearanceSeconds(CompressionPhaseMinutes[0]);
            var narrowest = CompressionMinuteClearanceSeconds(CompressionPhaseMinutes[^1]);

            if (tightest.PercentOfClearance is double tightestShare)
            {
                logger.LogDebug(
                    "TimescaleDB: tightest compression clearance this tick is {Hypertable} at {Percent:F1}% of its {Clearance}s from :{Minute:00} ({Band}); the band runs {Widest}s down to {Narrowest}s of clearance.",
                    tightest.HypertableName, tightestShare, CompressionMinuteClearanceSeconds(tightestMinute),
                    tightestMinute, tightest.ClearanceBand, widest, narrowest);
            }
            else
            {
                logger.LogDebug(
                    "TimescaleDB: tightest compression clearance this tick is {Hypertable} with NO CLEARANCE at all — it ran from :{Minute:00}, a minute an hourly refresh also starts on, so its share of clearance is UNDEFINED rather than low ({Band}); the band runs {Widest}s down to {Narrowest}s of clearance.",
                    tightest.HypertableName, tightestMinute, tightest.ClearanceBand, widest, narrowest);
            }
        }
    }

    /// <summary>
    /// Which of two clearance readings is the one worth naming in the per-tick summary: the larger share of
    /// its own clearance.
    ///
    /// <para><b>The share IS the severity, so this needs no second key — and that is a PROPERTY rather than
    /// a coincidence.</b> <see cref="ClassifyCompressionClearance"/>'s two boundaries are the clearance and
    /// five sixths of it, and every clearance the grid can produce is a whole number of minutes, so both
    /// boundaries fall at the same SHARE on every minute in the band — 83.3% and 100%. Ranking by share is
    /// therefore ranking by band, and a reading the line calls the tightest can never carry a milder band
    /// than one it passed over. <c>CompressionClearanceWatchTests</c> pins that exactness, because integer
    /// division is what would break it: a clearance that was not a multiple of six would round the watch
    /// line down and the two orders would diverge on that minute alone.</para>
    ///
    /// <para><b>A missing share means a ZERO clearance, and its limit is infinity rather than zero.</b>
    /// <see cref="CompressionActivity.PercentOfClearance"/> is null exactly there — inside this walk the
    /// duration and the minute are both known, so a zero denominator is the only way to lose it — and zero
    /// clearance is reachable in production and is the worst geometry there is: a policy the converge could
    /// not put on a fixed schedule, drifted onto a minute an hourly refresh also starts on.
    /// <see cref="ClassifyCompressionClearance"/> already bands that
    /// <see cref="CompressionClearanceBand.RefreshOverrun"/> for any non-negative run. Defaulting the missing
    /// share to zero ranked that policy BELOW a routine reading, so the summary named the wrong one — the
    /// per-item Warning fires either way, so the cost was the aggregate line pointing away from the thing it
    /// exists to point at. Raised by review.</para>
    ///
    /// <para><b>One key rather than a band key and a share key.</b> A severity key would be redundant given
    /// the exactness above, and a redundant arm makes each arm individually unfalsifiable: with both present,
    /// restoring the zero default changes no outcome and a mutation sweep reports the defect as unreachable.
    /// So the property is pinned and the comparison is single.</para>
    /// </summary>
    private static bool IsTighter(CompressionActivity candidate, CompressionActivity? incumbent) =>
        incumbent is null
        || (candidate.PercentOfClearance ?? double.PositiveInfinity)
            > (incumbent.PercentOfClearance ?? double.PositiveInfinity);

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
            using var command = new NpgsqlCommand(RearmJobSql, connection) { CommandTimeout = SetupTimeoutSeconds };
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
/// One live reading of the heaviest hourly refresh's runtime against the slot it has to fit inside (#3044),
/// from <see cref="TimescaleSupport.ReadHeaviestRefreshRuntimeAsync"/>.
///
/// <para>Carries the derived answers rather than leaving them to each caller, so a consumer cannot log the
/// seconds and drop the classification — the same reason the readings this file's siblings return compute
/// their own ratios. <see cref="View"/> is the CAGG's user-view name, which is the identity the phase grid is
/// keyed on; a job id would name a different job on any other deployment.</para>
/// </summary>
public sealed record HeaviestRefreshSlotReading(string View, double LastRunSeconds)
{
    /// <summary>Where this reading sits against the slot — see
    /// <see cref="TimescaleSupport.ClassifyRefreshSlotHeadroom"/>.</summary>
    public TimescaleSupport.RefreshSlotHeadroom Headroom =>
        TimescaleSupport.ClassifyRefreshSlotHeadroom(LastRunSeconds);

    /// <summary>How many seconds of the slot were left unused. Goes NEGATIVE past the slot width rather than
    /// clamping at zero: how far THROUGH the wall a run went is the number that sizes the re-derivation, and
    /// clamping would report every breach as a dead heat.</summary>
    public double ClearOfSlotSeconds => TimescaleSupport.RefreshPhaseSlotSeconds - LastRunSeconds;

    /// <summary>This reading as a percentage of the slot — 71.1% for the recorded ceiling
    /// (<see cref="TimescaleSupport.HeaviestHourlyRefreshObservedCeilingSeconds"/>) against the window the
    /// hour can spare (<see cref="TimescaleSupport.HeaviestRefreshWindowMinutes"/>), which is the margin the
    /// #3044 watch is stated against.</summary>
    public double PercentOfSlot => 100.0 * LastRunSeconds / TimescaleSupport.RefreshPhaseSlotSeconds;
}

/// <summary>
/// One live reading of an hourly refresh policy's runtime, keyed on the CAGG's user-view name (#3182) —
/// what <see cref="TimescaleSupport.ReadOtherHourlyRefreshRuntimesAsync"/> returns for the twelve hourly
/// views that are not <see cref="TimescaleSupport.HeaviestHourlyRefreshView"/>.
///
/// <para>Deliberately carries NO derived verdict, which is the difference from
/// <see cref="HeaviestRefreshSlotReading"/>. The heaviest refresh has a slot of its own, so a reading of it
/// has a band; a light refresh has only the guard band after its start, which is not a per-view quantity —
/// so the only question asked of these readings is whether one has overtaken
/// <see cref="TimescaleSupport.OtherHourlyRefreshObservedCeilingSeconds"/>, and that is asked by the
/// classifier rather than answered here. Inventing a per-view band would be a verdict the grid does not
/// have.</para>
/// </summary>
public sealed record HourlyRefreshRuntimeReading(string View, double LastRunSeconds);

/// <summary>
/// The rate limiter for <see cref="TimescaleSupport.LogRefreshCeilingStaleness"/> (#3182): a HIGH-WATER
/// MARK per ceiling constant, held for the lifetime of the instance the caller keeps.
///
/// <para><b>Why an object the caller owns rather than static state.</b> The lifetime this limiter needs is
/// the PROCESS — a store whose ceiling has drifted trips the finding on a large share of hourly sweeps, and
/// a limiter reset per sweep would limit nothing. Static state would give that lifetime for free and take
/// testability with it: two test cases sharing a static latch are order-dependent, and the interesting
/// property here is a SEQUENCE of readings, which cannot be tested at all if the sequence leaks between
/// cases. So the service holds one instance and the tests hold their own.</para>
///
/// <para><b>What it reports, stated as the rule rather than left to the implementation.</b> The first
/// falsifying reading for a constant reports. After that, only a reading strictly greater than the largest
/// already reported for that constant reports. Nothing ever lowers the mark, so the finding cannot start
/// repeating because load fell — and that asymmetry is deliberate rather than an oversight: the mark is not
/// a measure of current load, it is a record of the largest value already SAID OUT LOUD, and a value
/// already said out loud stays said. A run below it changes nothing about what the constant has to be
/// re-derived to.</para>
///
/// <para><b>Keyed on the constant's NAME, and the two names are
/// <see cref="TimescaleSupport.HeaviestRefreshCeilingConstantName"/> and
/// <see cref="TimescaleSupport.OtherRefreshCeilingConstantName"/>.</b> Not on the view: the light ceiling is
/// ONE constant covering twelve views, so keying on the view would let each of them report the same
/// constant's staleness independently and the rate limit would be twelve times looser than it reads. What
/// is being reported is a constant being wrong, so the constant is the key.</para>
///
/// <para>Locked, because the sweep that calls it is one of several the worker runs and nothing in the type
/// system says it stays single-threaded. A missed report is a lost finding and a double report is noise;
/// neither costs anything worth an interlocked-compare loop at one call an hour.</para>
/// </summary>
public sealed class RefreshCeilingStalenessWatch
{
    private readonly Dictionary<string, double> _reportedHighWaterMark = new(StringComparer.Ordinal);

    /// <summary>
    /// Whether <paramref name="observedSeconds"/> should be REPORTED for
    /// <paramref name="constantName"/> — true for the first falsifying reading and thereafter only for one
    /// larger than the largest already reported. Records the mark when it answers true, so a caller that
    /// asks twice about the same reading is told once.
    ///
    /// <para><b>A NON-FINITE reading is refused and NOT recorded, and this is the one place that guard does
    /// work.</b> The comparison below is <c>observedSeconds &lt;= mark</c>, and every comparison against NaN
    /// is false — so a NaN allowed through would answer true, become the mark, and then answer true for
    /// EVERY later reading, because none of them is <c>&lt;= NaN</c> either. One impossible catalog reading
    /// would disable the rate limit for the life of the process, silently and permanently. That is why the
    /// guard is here rather than in <see cref="TimescaleSupport.ClassifyRefreshCeilingFreshness"/>, where the
    /// comparison already answers correctly for a non-finite value and a guard would be unreachable.</para>
    /// </summary>
    public bool ShouldReport(string constantName, double observedSeconds)
    {
        if (constantName is null)
        {
            throw new ArgumentNullException(nameof(constantName));
        }

        if (!double.IsFinite(observedSeconds))
        {
            return false;
        }

        lock (_reportedHighWaterMark)
        {
            if (_reportedHighWaterMark.TryGetValue(constantName, out var mark)
                && observedSeconds <= mark)
            {
                return false;
            }

            _reportedHighWaterMark[constantName] = observedSeconds;
            return true;
        }
    }

    /// <summary>
    /// The largest reading already reported for <paramref name="constantName"/>, or null when none has
    /// been. Exists so the sequence property can be asserted directly instead of inferred from log lines —
    /// a test that could only read the log would be testing the message, not the limiter.
    /// </summary>
    public double? ReportedHighWaterMark(string constantName)
    {
        if (constantName is null)
        {
            throw new ArgumentNullException(nameof(constantName));
        }

        lock (_reportedHighWaterMark)
        {
            return _reportedHighWaterMark.TryGetValue(constantName, out var mark) ? mark : null;
        }
    }
}

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

    /// <summary>
    /// The minute of the hour this hypertable's compression policy is ASSIGNED by
    /// <see cref="TimescaleSupport.CompressionPhaseMinutes"/>, or null when this product does not own the
    /// hypertable — a bring-your-own store's own table, or a fixture table. Null is what keeps every
    /// clearance figure below silent for a FOREIGN hypertable: this code chose no minute for it, so it has no
    /// standing to say whether its run overran anything.
    /// </summary>
    public int? AssignedPhaseMinute =>
        HypertableName is not null
        && TimescaleSupport.TryCompressionPhaseMinutesFor(HypertableName, out var assigned)
            ? assigned
            : null;

    /// <summary>
    /// The minute of the hour the last run actually STARTED on, or null when the store recorded no start (or
    /// recorded the never-ran sentinel, which <see cref="RunningFor"/> documents).
    /// </summary>
    public int? ObservedStartMinute =>
        LastRunStartedAtUtc is DateTime startedUtc && startedUtc != DateTime.MinValue
            ? startedUtc.Minute
            : null;

    /// <summary>
    /// The minute the clearance is measured from: the OBSERVED start when there is one, otherwise the
    /// assigned minute.
    ///
    /// <para><b>Observed first, and that ordering is the point (#3112).</b> The assigned minute is what this
    /// product INTENDS; the observed minute is what happened. Those differ on exactly the store state
    /// <see cref="TimescaleSupport.ConvergeCompressionScheduleAsync"/> documents as its degraded path — a job
    /// catalog too old to expose <c>fixed_schedule</c>/<c>initial_start</c> keeps TimescaleDB's
    /// finish-to-start scheduling and drifts through the hour by its own runtime — and on that store the
    /// intended clearance is a fiction while the observed one is the fact. Reading the intent would report the
    /// grid working on a store where it had not been applied.</para>
    /// </summary>
    public int? ClearanceMinute =>
        AssignedPhaseMinute is null ? null : ObservedStartMinute ?? AssignedPhaseMinute;

    /// <summary>True when the last run started on a minute other than the one the grid assigns — the drift
    /// condition above, worth reporting because it makes every other figure here a statement about a policy
    /// this product has not actually placed.</summary>
    public bool OffAssignedPhase =>
        AssignedPhaseMinute is int assigned && ObservedStartMinute is int observed && assigned != observed;

    /// <summary>How long the run had before the next hourly refresh started, from
    /// <see cref="TimescaleSupport.CompressionMinuteClearanceSeconds"/>. Null for a foreign hypertable.</summary>
    public int? ClearanceSeconds =>
        ClearanceMinute is int minute ? TimescaleSupport.CompressionMinuteClearanceSeconds(minute) : null;

    /// <summary>Where the last completed run sits against that clearance. Null when there is no completed run
    /// to judge or the hypertable is foreign — never a synthesized <c>InsideClearance</c>, which would read as
    /// "measured and fine" for something not measured at all.</summary>
    public TimescaleSupport.CompressionClearanceBand? ClearanceBand =>
        ClearanceMinute is int minute && LastRunDuration is TimeSpan duration
            ? TimescaleSupport.ClassifyCompressionClearance(duration.TotalSeconds, minute)
            : null;

    /// <summary>Seconds of the clearance left unused. Goes NEGATIVE past it rather than clamping, for the
    /// reason <see cref="HeaviestRefreshSlotReading.ClearOfSlotSeconds"/> does: how far THROUGH the next
    /// refresh's start a run went is the number that sizes the repair, and clamping reports every overrun as
    /// a dead heat.</summary>
    public double? ClearOfRefreshSeconds =>
        ClearanceSeconds is int clearance && LastRunDuration is TimeSpan duration
            ? clearance - duration.TotalSeconds
            : null;

    /// <summary>The last completed run as a percentage of its minute's clearance.</summary>
    public double? PercentOfClearance =>
        ClearanceSeconds is int clearance and > 0 && LastRunDuration is TimeSpan duration
            ? 100.0 * duration.TotalSeconds / clearance
            : null;
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
