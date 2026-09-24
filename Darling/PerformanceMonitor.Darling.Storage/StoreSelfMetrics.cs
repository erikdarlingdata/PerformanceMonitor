/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The hourly store self-metrics sweep (#2068) — the service measuring ITS OWN store into
/// <c>collect.store_metrics</c>, so capacity forecasting is a query instead of ad-hoc archaeology over the
/// TimescaleDB chunk catalog. The archaeology is doable (1-day chunks make per-day ingest reconstructible)
/// but ephemeral: raw chunks age out in 4 days, so the reconstructable window is always tiny — measured on
/// the production 52-replica store, compressed daily ingest jumped ~3x (620 MB/day to ~2.3 GB/day) and
/// nothing recorded it; the evidence only existed because the chunk catalog still held both eras. Each run
/// writes, under ONE <paramref name="metric_time"/> so a run's rows join:
/// <list type="bullet">
/// <item>one row per hypertable (<c>object_kind = 'hypertable'</c>): total bytes
/// (<c>hypertable_detailed_size</c>), pre/post-compression bytes (<c>chunk_compression_stats</c>, summed
/// over compressed chunks), and the chunk count — only when TimescaleDB is available (the
/// <see cref="TimescaleSupport"/> detection idiom: the caller passes the worker's cached flag, and on a
/// plain-PostgreSQL store this arm is skipped silently because the timescaledb_information views it reads
/// do not exist there);</item>
/// <item>one row per payload dimension table (<c>object_kind = 'dimension'</c>): total bytes
/// (<c>pg_total_relation_size</c> — heap + indexes + TOAST, where the plan XML actually lives), the
/// exact row count and, since V137 (#3783), the TOAST file's own size (<c>toast_bytes</c>) beside a
/// <c>toast_live_bytes</c> that is NULL unless the store happens to carry <c>pg_freespacemap</c> — see
/// <see cref="DimensionInsertSql"/> and <see cref="ToastLiveBytesUpdateSql"/>. The dims are the store's dominant payloads (measured: query_plan_dim
/// alone was 101 GB of a 147 GB store, 69%) and invisible to every hypertable-shaped surface because they
/// are deliberately PLAIN tables (see <see cref="PayloadDimensions.CreateDimTable"/>);</item>
/// <item>one row per continuous aggregate (<c>object_kind = 'continuous_aggregate'</c>, #3582): the same
/// three facts as a hypertable row, taken from the aggregate's MATERIALIZATION hypertable and reported
/// under the aggregate's user-facing view name. TimescaleDB-only, like the hypertable arm. These rows
/// exist because the hypertable arm structurally cannot see them: <c>timescaledb_information.hypertables</c>
/// ends <c>AND ca.mat_hypertable_id IS NULL</c> (verified against the 2.28.1 view definition), so a
/// materialization is never enumerated there under any name. Measured on the largest production store,
/// the twenty materializations were ~235 GiB of a 415 GiB database — 57% — and the inventory reported
/// none of it;</item>
/// <item>one row per product-owned plain table the walk would otherwise lump (<c>object_kind = 'table'</c>,
/// #3582): <c>collect.query_store_text</c> (V74 stores statement text INLINE by design — 15 GiB on that
/// store), <c>collect.query_store_plan_map</c> and <c>config.config_alert_log</c>. Every store shape;</item>
/// <item>two catch-all rows that make the inventory RECONCILABLE against <c>pg_database_size</c> (#3582):
/// <c>object_kind = 'other'</c> sums every user-schema relation none of the rows above accounts for, with
/// the relation count, and <c>object_kind = 'system'</c> does the same for the system catalogs and
/// TimescaleDB's own bookkeeping schemas. With those two, every byte the database directory holds is
/// attributed to some row or is an honest residual, and <c>get_store_metrics</c> can state its own
/// coverage instead of answering "here is the store" for 38% of it;</item>
/// <item>one row carrying the OWNER's reading of <c>timescaledb_information.job_history</c>
/// (<c>object_kind = 'job_history'</c>, #3574): how many history rows the sweep's role — which created the
/// jobs and is admitted by the view's ownership filter — saw over the fixed 24-hour evidence window, when
/// the newest one started, and how many jobs started a run in that window. TimescaleDB-only. This is the
/// managed-mode self-proof: the MCP host reads as the least-privilege <c>mcp</c> role, which that filter
/// shows NOTHING, so the tool's own count is zero by construction on every managed store and only this
/// row can make <c>recording</c> a measurement there;</item>
/// <item>one row carrying the store's OWN checkpointer counters (<c>object_kind = 'checkpointer'</c>,
/// #3783): the CUMULATIVE write-phase and sync-phase milliseconds and the cumulative count of REQUESTED
/// (WAL-forced) checkpoints, from <c>pg_stat_checkpointer</c> on PostgreSQL 17+ and <c>pg_stat_bgwriter</c>
/// before it. Every store shape. The row exists because three unattributed read kills on a production store
/// in one day all sat inside checkpoint sync phases of 25.2 s and 14.0 s and nothing in the store recorded
/// that the checkpointer had been there; the per-interval figures the MCP surface and the self-alert judge
/// are DIFFERENCES between consecutive rows, computed at read time — see <see cref="CheckpointerInsertSql"/>
/// for why the counters are stored raw;</item>
/// <item>one summary row (<c>object_kind = 'store'</c>): <c>pg_database_size</c> plus the enabled-server
/// count (the fleet reader's <c>WHERE is_enabled</c> registry predicate), so the per-server ingest rate —
/// daily growth divided by servers, the number onboarding N primaries multiplies — is derivable from the
/// stored series alone.</item>
/// </list>
///
/// <para><b>Which column means what, per kind.</b> The table has one set of nullable columns and every kind
/// leaves the ones it has no use for NULL; the two catch-all kinds and the <c>job_history</c> kind reuse
/// columns whose names were chosen for hypertables and jobs, and the mapping is stated HERE, once, because
/// a raw read of the table has nothing else to go on. <c>chunk_count</c> on an <c>other</c> or
/// <c>system</c> row is the RELATION count the sum spans (the number of physical pieces, which is what it
/// means on a hypertable row too). On a <c>job_history</c> row: <c>object_name</c> is the role that
/// counted; <c>row_count</c> is the history rows with a start inside the window that the view showed that
/// role — NULL when the role was not admitted to every job's history, because a count the filter
/// truncated is not a count; <c>total_runs</c> is the jobs whose newest start falls inside the same window
/// (the unfiltered <c>job_stats</c> population half); <c>schedule_interval_ms</c> is the window's width;
/// and <c>last_run_duration_ms</c> is the AGE of the newest history row at the sweep — milliseconds from
/// its start to <c>metric_time</c> — NOT a run's duration. That last one is the single overload that bends
/// a column's name, and it is taken rather than a migration rung because the store has no timestamp
/// column besides <c>metric_time</c>, a rung was not free when this landed, and the MCP reader decodes it
/// back into an absolute instant before anyone reads it; a dedicated column is the clean follow-up. On a
/// <c>checkpointer</c> row (V137, #3783) the three columns named for it — <c>checkpoint_write_ms</c>,
/// <c>checkpoint_sync_ms</c>, <c>checkpoints_requested</c> — hold the server's CUMULATIVE counters as the
/// sweep read them, NOT the interval's delta, and since V139 (#3955) <c>postmaster_start_time</c> holds the
/// <c>pg_postmaster_start_time()</c> of the postmaster that produced them, as naive UTC, so a reader can tell an
/// interval that spans a restart; every other kind leaves all four NULL, and the two TOAST columns
/// are filled on <c>dimension</c> rows only. That is the same convention every <c>pg_stat_*</c>-sourced
/// collector table in this store follows (the columns are raw counters; the read differences them), and
/// <see cref="CheckpointerInsertSql"/> says why it was chosen over an in-process baseline here.</para>
///
/// <para>Retention is ONE bounded DELETE inside the same sweep — deliberately no policy machinery.
/// <c>collect.store_metrics</c> is a PLAIN table and must stay one: it is not in the collector catalog, so
/// <see cref="TimescaleSupport"/>'s catalog-driven hypertable conversion and DarlingRetention's catalog
/// purge can never recurse onto the table that measures them (pinned by test). At ~30 rows/hour,
/// <see cref="RetentionDays"/> days is ~100k narrow rows — nothing.</para>
///
/// <para>Failure isolation is the caller's (the worker wraps the sweep like the compression-job check);
/// the statements here run sequentially on one connection and a failed run simply leaves a one-hour gap in
/// the series.</para>
/// </summary>
public static class StoreSelfMetrics
{
    /// <summary>
    /// The TimescaleDB GUC that decides whether <c>timescaledb_information.job_history</c> records anything
    /// (#3175). Named ONCE, because the string has two consumers that must never disagree: the managed
    /// provisioner writes it into postgresql.conf (<c>DarlingManagedPostgres.BuildJobExecutionLoggingConfAppend</c>)
    /// and the MCP read probes <c>pg_settings</c> for it (<c>DarlingStoreMetricsReader.JobExecutionLoggingSql</c>).
    /// A retyped copy that drifted would not error — <c>pg_settings</c> would simply return no row for a name
    /// nobody registered, which this read reports as "the server has no such setting". A wrong name and a
    /// plain-PostgreSQL store would be indistinguishable.
    ///
    /// <para><b>Why it lives HERE and not on the provisioner that writes it.</b>
    /// <c>DarlingManagedPostgres</c> is <c>[SupportedOSPlatform("windows")]</c>, so a constant declared
    /// there makes every call site platform-dependent — CA1416 on a read that is not Windows-specific and
    /// has no reason to be, for the sake of a string. This class is the platform-neutral owner of the
    /// store's own background-job telemetry, and it is the class whose recorded series
    /// (<see cref="BackgroundJobInsertSql"/>, one hourly sample per job) is the reason the per-run route
    /// matters at all: a maximum question cannot be answered from a sample.</para>
    /// </summary>
    public const string JobExecutionLoggingSetting = "timescaledb.enable_job_execution_logging";

    /// <summary>
    /// Per-statement command timeout for the sweep (#2317) — and, at the worker's call site, the
    /// budget for the WHOLE sweep via a linked CTS (see SweepStoreSelfMetricsAsync: this sweep is
    /// awaited on the main loop, so the sequential per-statement timeouts must not stack). The
    /// sizing queries call <c>hypertable_detailed_size</c> across every hypertable (whose inner
    /// <c>hypertable_local_size</c> is the frame the server log names when it cancels) and
    /// <c>pg_database_size</c> over the whole
    /// store, and on the dogfood fleet (141 objects, a 100+ GB dimension) they outgrew Npgsql's default
    /// 30 seconds ~5x/day under load — surfacing as "Exception while reading from stream" (Npgsql
    /// cancels the statement; the server logs 'canceling statement due to user request'; the client
    /// holds a torn stream), an ERROR that reads as a network fault and pollutes the count every health
    /// check watches. Five minutes matches DarlingRetention's destructive-statement budget: this sweep
    /// runs hourly on its own connection, so a slow sizing pass costs patience, not correctness — and a
    /// sweep that cannot finish in five minutes should skip the tick (one-hour series gap, self-healing)
    /// rather than retry into the same load.
    /// </summary>
    public const int SweepTimeoutSeconds = 300;
    /// <summary>How long the series is kept — 400 days, so a year-over-year forecast always has a full
    /// prior year plus headroom. Enforced by the sweep's own DELETE, not a retention policy.</summary>
    public const int RetentionDays = 400;

    /// <summary>The <c>object_kind</c> of the per-hypertable rows. Named for the reason
    /// <see cref="StoreObjectKind"/> gives: the MCP reader partitions and sums by kind, and a drifted
    /// spelling returns zero rows rather than an error. The value is part of the on-disk contract.</summary>
    public const string HypertableObjectKind = "hypertable";

    /// <summary>The <c>object_kind</c> of the two payload-dimension rows. See <see cref="HypertableObjectKind"/>.</summary>
    public const string DimensionObjectKind = "dimension";

    /// <summary>The <c>object_kind</c> of the per-background-job rows (#2136). See <see cref="HypertableObjectKind"/>.</summary>
    public const string BackgroundJobObjectKind = "background_job";

    /// <summary>
    /// The per-hypertable rows — TimescaleDB stores only (the caller gates on the detected flag; the
    /// timescaledb_information views referenced here do not exist on plain PostgreSQL).
    /// <c>hypertable_detailed_size</c> / <c>chunk_compression_stats</c> take a regclass, built with
    /// <c>format('%I.%I', ...)</c> from the catalog view's own rows — never user input.
    /// <c>compressed_*_bytes</c> are NULL for a hypertable with no compressed chunks yet. $1 metric_time
    /// (naive UTC, one value per run).
    ///
    /// <para><b>What this arm cannot see, and why that is not a filter of ours (#3582).</b> Nothing here
    /// restricts the walk to collector tables — it enumerates every row the view returns — but the view
    /// itself excludes two classes of hypertable: the internal compressed hypertables
    /// (<c>compression_state &lt;&gt; 2</c>, which exist only before 2.29.0: from then on compression keeps
    /// no internal hypertable) and every continuous aggregate's materialization
    /// (<c>ca.mat_hypertable_id IS NULL</c>). The first is right — <c>hypertable_detailed_size</c> on a
    /// user hypertable already includes its compressed chunk relations, byte-exact (verified on 2.28.1 and
    /// 2.30.1 against <c>pg_total_relation_size</c> over root + chunks + compressed relations). The second
    /// is the gap <see cref="ContinuousAggregateInsertSql"/> closes: a materialization is a hypertable that holds
    /// real bytes and is enumerated by this view under NO name, internal or otherwise.</para>
    /// </summary>
    public const string HypertableInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, compressed_before_bytes, compressed_after_bytes, chunk_count)
SELECT
    $1,
    h.hypertable_name,
    '{HypertableObjectKind}',
    s.total_bytes,
    c.before_bytes,
    c.after_bytes,
    h.num_chunks
FROM timescaledb_information.hypertables h
LEFT JOIN LATERAL (
    SELECT sum(total_bytes)::bigint AS total_bytes
    FROM hypertable_detailed_size(format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass)
) s ON true
LEFT JOIN LATERAL (
    SELECT
        sum(before_compression_total_bytes)::bigint AS before_bytes,
        sum(after_compression_total_bytes)::bigint AS after_bytes
    FROM chunk_compression_stats(format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass)
) c ON true";

    /// <summary>The <c>object_kind</c> of the per-continuous-aggregate rows (#3582). See
    /// <see cref="HypertableObjectKind"/> for why it is a const.</summary>
    public const string ContinuousAggregateObjectKind = "continuous_aggregate";

    /// <summary>
    /// The per-continuous-aggregate rows (#3582) — TimescaleDB stores only, like the hypertable arm. One
    /// row per aggregate in <c>timescaledb_information.continuous_aggregates</c>, under the aggregate's
    /// USER-FACING view name (<c>view_name</c>, bare, the way hypertable rows carry <c>hypertable_name</c>),
    /// sized through its materialization hypertable: <c>hypertable_detailed_size</c> on
    /// <c>materialization_hypertable_schema.materialization_hypertable_name</c> for the total,
    /// <c>chunk_compression_stats</c> on the same regclass for the pre/post-compression bytes, and the
    /// materialization's chunk count from <c>timescaledb_information.chunks</c>.
    ///
    /// <para><b>Why the chunk count is a third lateral and not a column of the second.</b>
    /// <c>chunk_compression_stats</c> returns ZERO rows for a hypertable whose compression is not enabled
    /// (measured on 2.28.1: a two-chunk materialization with compression off yields no rows at all, and
    /// two rows the moment it is enabled), so a <c>count(*)</c> over it would report every uncompressed
    /// aggregate as chunkless. The <c>chunks</c> view lists every non-OSM chunk of every hypertable,
    /// materializations included, which is also how the <c>hypertables</c> view computes
    /// <c>num_chunks</c> for the sibling rows — so the two kinds count chunks the same way.</para>
    ///
    /// <para><b>What this row deliberately does NOT carry.</b> <c>compression_enabled</c> and whether a
    /// refresh, compression or retention policy exists for the aggregate — the three facts the sibling
    /// investigation (#3581) had to assemble by hand — are STATE, not series: they change when an operator
    /// changes them and at no other time, and the table has no column that could hold a bool without
    /// bending a byte or count column's meaning. They are read LIVE by <c>get_store_metrics</c> from the
    /// same two catalog views (<c>DarlingStoreMetricsReader.ContinuousAggregateStateSql</c>), the #2813
    /// precedent for a catalog fact the series does not carry, and joined to these rows by view name.
    /// That keeps this table's schema where it is: no migration rung for three flags.</para>
    ///
    /// <para>Bare <c>view_name</c> has the same exposure the hypertable rows already accept: two aggregates
    /// of the same name in different schemas would share one series. Every aggregate this product creates
    /// lives in <c>collect</c>. $1 metric_time.</para>
    /// </summary>
    public const string ContinuousAggregateInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, compressed_before_bytes, compressed_after_bytes, chunk_count)
SELECT
    $1,
    ca.view_name,
    '{ContinuousAggregateObjectKind}',
    s.total_bytes,
    c.before_bytes,
    c.after_bytes,
    n.chunk_count
FROM timescaledb_information.continuous_aggregates ca
LEFT JOIN LATERAL (
    SELECT sum(total_bytes)::bigint AS total_bytes
    FROM hypertable_detailed_size(format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name)::regclass)
) s ON true
LEFT JOIN LATERAL (
    SELECT
        sum(before_compression_total_bytes)::bigint AS before_bytes,
        sum(after_compression_total_bytes)::bigint AS after_bytes
    FROM chunk_compression_stats(format('%I.%I', ca.materialization_hypertable_schema, ca.materialization_hypertable_name)::regclass)
) c ON true
LEFT JOIN LATERAL (
    SELECT count(*)::integer AS chunk_count
    FROM timescaledb_information.chunks ch
    WHERE ch.hypertable_schema = ca.materialization_hypertable_schema
    AND   ch.hypertable_name = ca.materialization_hypertable_name
) n ON true";

    /// <summary>
    /// The background-job rows (#2136) — TimescaleDB stores only, like the hypertable arm (the
    /// timescaledb_information views do not exist on plain PostgreSQL). The store's own background jobs
    /// (CAGG refreshes, compression, retention) are its heaviest recurring work, their runtimes scale
    /// SERIALLY with raw volume (the finalize hash-aggregate runs in one process — measured in #2136:
    /// the four most expensive jobs are all the query_store_stats family, compression at 157s and the
    /// interval_hourly refresh at 96s on a 52-server store), and a job that outgrows its own schedule
    /// interval compounds refresh lag silently. One row per job per sweep makes that a queryable series:
    /// object_name is <c>proc_name</c> plus the hypertable/CAGG it serves (the telemetry job has
    /// neither) plus a <c>[job_id]</c> suffix — the uniqueness guarantee (review catch): two user-added
    /// jobs sharing a proc_name, or two hypertable-less jobs, would otherwise collide into one
    /// object_name and the readers' DISTINCT ON would silently drop one job's telemetry. job_id is
    /// stable for a job's lifetime, so per-job series continuity holds. <c>schedule_interval_ms</c>
    /// rides along so "duration vs cadence" — the honest tripwire — is one division. $1 metric_time.
    /// </summary>
    public const string BackgroundJobInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, last_run_duration_ms, schedule_interval_ms, total_runs, total_failures)
SELECT
    $1,
    j.proc_name || coalesce(' ' || j.hypertable_name, '') || ' [' || j.job_id || ']',
    '{BackgroundJobObjectKind}',
    (EXTRACT(EPOCH FROM js.last_run_duration) * 1000)::bigint,
    (EXTRACT(EPOCH FROM j.schedule_interval) * 1000)::bigint,
    js.total_runs,
    js.total_failures
FROM timescaledb_information.job_stats AS js
JOIN timescaledb_information.jobs AS j USING (job_id)";

    /// <summary>
    /// The payload dimension rows — every store shape (the dims are plain tables everywhere). Table names
    /// are the <see cref="PayloadDimensions"/> compile-time constants, so interpolation is safe (the
    /// DarlingRetention.DeleteSqlFor reasoning). The exact <c>count(*)</c> is deliberate over
    /// <c>pg_class.reltuples</c>: it is an hourly index-only scan over the digest PK, and the dim heap is
    /// small — the bytes live in TOAST, which <c>pg_total_relation_size</c> counts and a scan never
    /// touches. $1 metric_time.
    ///
    /// <para><b><c>toast_bytes</c> and <c>toast_live_bytes</c> (V137, #3783) — the dimension rows are the
    /// only kind that fills them.</b> <c>pg_total_relation_size</c> says how big the dimension is and nothing
    /// about how FULL its TOAST file is, and on one production store class the plan dimension's TOAST file
    /// sat at 154 GB for ~61 GB of live chunks — 40 % utilisation, ~93 GB of slack left by the V54 text→gz
    /// conversion plus ~755 k rows a day cycling through row-capped deletes, which ordinary <c>VACUUM</c>
    /// returns to the table and never to the OS. <c>toast_bytes</c> is <c>pg_relation_size(reltoastrelid)</c>:
    /// the TOAST relation's main fork, the file whose slack that read measured, taken through
    /// <c>NULLIF(reltoastrelid, 0)</c> so a table with no TOAST relation stores NULL rather than erroring
    /// (both dims have one — every table with a TOAST-able column gets one at CREATE — but the read says
    /// so rather than assuming it). Its index (<c>pg_toast_NNN_index</c>) is deliberately NOT included: the
    /// utilisation question is about the heap file's pages, and the index is already inside
    /// <c>total_bytes</c>.</para>
    ///
    /// <para><b><c>toast_live_bytes</c> is written <c>NULL</c> here, on purpose, because every extension-free
    /// read of it was measured and found wanting</b> (rig: TimescaleDB 2.28.1 / PostgreSQL 18, 215 MB of
    /// 9.6 KB TOASTed values, half deleted, ordinary <c>VACUUM</c>; <c>pgstattuple</c> as the oracle, 47.9 %
    /// live). <c>n_live_tup / (n_live_tup + n_dead_tup) × file</c> reads 100 % after a vacuum — the #3783 shape
    /// exactly — and is a lie. <c>n_live_tup(toast) × 1996</c> (chunk count × <c>TOAST_MAX_CHUNK_SIZE</c>) reads
    /// 48.9 %, honest on 9.6 KB values but overstating by up to one partial chunk per value, so a dimension
    /// whose values barely cross the TOAST threshold reads up to half again too high, and <c>n_live_tup</c>
    /// is exact only just after a vacuum. <c>pg_freespacemap</c>'s <c>file − sum(avail)</c> reads 48.5 % — a
    /// real byte measurement — but <c>CREATE EXTENSION pg_freespacemap</c> is a product dependency the
    /// maintainer decides, not this sweep. The column exists so that decision needs no rung; until it is
    /// made, a reader that finds NULL beside a non-NULL <c>toast_bytes</c> says "not measured" and computes
    /// nothing from the total. The literal <c>NULL::bigint</c> is typed so the UNION's column list resolves
    /// on every PostgreSQL major the store runs on. This INSERT never consults the extension; the one
    /// statement that does is <see cref="ToastLiveBytesUpdateSql"/>, run afterwards ONLY where the sweep has
    /// just found <c>pg_freespacemap</c> installed (#3783's code half), so the maintainer's single
    /// <c>CREATE EXTENSION</c> lights the utilisation surface without another release.</para>
    /// </summary>
    public const string DimensionInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, row_count, toast_bytes, toast_live_bytes)
SELECT
    $1,
    '{PayloadDimensions.QueryTextDimTable}',
    '{DimensionObjectKind}',
    pg_total_relation_size('collect.{PayloadDimensions.QueryTextDimTable}'),
    (SELECT count(*) FROM collect.{PayloadDimensions.QueryTextDimTable}),
    pg_relation_size(NULLIF((SELECT c.reltoastrelid FROM pg_class AS c WHERE c.oid = 'collect.{PayloadDimensions.QueryTextDimTable}'::regclass), 0)),
    NULL::bigint
UNION ALL
SELECT
    $1,
    '{PayloadDimensions.QueryPlanDimTable}',
    '{DimensionObjectKind}',
    pg_total_relation_size('collect.{PayloadDimensions.QueryPlanDimTable}'),
    (SELECT count(*) FROM collect.{PayloadDimensions.QueryPlanDimTable}),
    pg_relation_size(NULLIF((SELECT c.reltoastrelid FROM pg_class AS c WHERE c.oid = 'collect.{PayloadDimensions.QueryPlanDimTable}'::regclass), 0)),
    NULL::bigint";

    /// <summary>
    /// The contrib module whose presence lets the sweep measure <c>toast_live_bytes</c> honestly (#3783).
    /// Named ONCE: the probe (<see cref="ExtensionInstalledSql"/>'s $1) and the sentence the MCP reader
    /// prints when the column is NULL both spell it, and a retyped copy that drifted would make the probe
    /// answer "not installed" forever on a store that had installed it — the honest-empty trap on the one
    /// instrument the maintainer was told would light the surface.
    /// </summary>
    public const string FreespacemapExtensionName = "pg_freespacemap";

    /// <summary>
    /// Whether an extension is INSTALLED in this database — one row when it is, zero rows when it is not
    /// (#3783). <c>pg_extension</c> rather than <c>pg_available_extensions</c> on purpose: the bundled
    /// TimescaleDB image SHIPS <c>pg_freespacemap</c> (it is in <c>pg_available_extensions</c> on every
    /// store) and does not CREATE it, and it is the CREATE that puts <c>pg_freespace()</c> in the catalog.
    /// A statement that names a function the catalog does not have fails at analysis, not at the branch
    /// that would call it — PostgreSQL resolves function names before a single row is evaluated, so a
    /// <c>CASE WHEN EXISTS (...) THEN pg_freespace(...) END</c> inside <see cref="DimensionInsertSql"/>
    /// would have failed the WHOLE dimension arm on every store without the extension. Hence a separate
    /// probe and a separate statement. $1 the extension name (<see cref="FreespacemapExtensionName"/>).
    /// </summary>
    public const string ExtensionInstalledSql = @"
SELECT 1
FROM pg_extension
WHERE extname = $1";

    /// <summary>
    /// Fills <c>toast_live_bytes</c> on the dimension rows the sweep has JUST written, from the free-space
    /// map (#3783) — run only after <see cref="ExtensionInstalledSql"/> found <see cref="FreespacemapExtensionName"/>
    /// installed, so on the shipped store this statement never executes and the column stays the NULL
    /// <see cref="DimensionInsertSql"/> wrote. The moment the maintainer runs
    /// <c>CREATE EXTENSION IF NOT EXISTS pg_freespacemap</c> on the store, the next hourly sweep fills it, and
    /// <c>get_store_metrics</c>' <c>toast_utilisation_pct</c> and the dormant slack self-alert light up
    /// without a release. That fencing is the whole design: the product does not take the dependency; it
    /// measures the moment the operator does.
    ///
    /// <para><b>The instrument, and what it measured.</b> <c>pg_freespace(reltoastrelid)</c> returns one
    /// row per page of the TOAST relation's main fork with the free-space map's record of that page's
    /// available bytes; <c>toast_bytes − sum(avail)</c> is the live figure. On the rig that decided the
    /// column (TimescaleDB 2.28.1 / PostgreSQL 18, 215 MB of 9.6 KB TOASTed values, half deleted, ordinary
    /// <c>VACUUM</c>, <c>pgstattuple</c> as the oracle at 47.9 %) it read 48.5 % — an honest byte
    /// measurement, +1.2 % — where the extension-free proxies read 100 % (tuple share) and 48.9 % (chunk
    /// count × 1996); the same shape reproduced at 48.4 % on the rig that wrote this statement, and the live
    /// test executes it against the migrated plan dimension. The +1.2 % has two known sources, both
    /// overstating LIVE: the FSM records each page's free space in 32-byte buckets and reports the bucket
    /// floor, and a page whose deletes no VACUUM has yet visited still carries its pre-delete record. Neither
    /// can understate slack, which is the direction the alert judges.</para>
    ///
    /// <para><b>Why it subtracts from the row's own <c>toast_bytes</c> and not from a fresh
    /// <c>pg_relation_size</c>.</b> The utilisation the reader publishes is <c>live / toast_bytes</c>; taking
    /// both from the same recorded size makes the quotient internally consistent even though the FSM is
    /// read a statement later than the file was sized. <c>greatest(…, 0)</c> is the honesty guard for the
    /// one ordering the arithmetic cannot rule out — a file truncated by a concurrent VACUUM between the two
    /// statements, leaving an <c>avail</c> sum taken over fewer pages than the size counted — so the column
    /// can never store a negative live figure. <c>reltoastrelid &lt;&gt; 0</c> guards the function call:
    /// <c>pg_freespace(0)</c> RAISES rather than returning no rows (the planner applies that filter on the
    /// <c>pg_class</c> scan BEFORE the lateral function scan — verified with EXPLAIN on 18), and a dimension
    /// without a TOAST relation already stored NULL <c>toast_bytes</c>, which the outer predicate leaves alone.
    /// <c>coalesce(sum(avail), 0)</c> because a zero-page TOAST file has no FSM rows and its live figure is a
    /// real zero, not a missing reading; the file's own size is zero there too and the reader publishes no
    /// percentage of nothing.</para>
    ///
    /// <para><b>Cost, stated rather than assumed.</b> The function walks the free-space map, which is
    /// ~1/8000 of the heap (one byte per page), and materialises one row per page for the aggregate: on the
    /// 215 MB rig that is ~26,000 rows and single-digit milliseconds; on the 154 GB TOAST file the issue
    /// measured it would be ~20 million rows, which this sweep has not measured and estimates at tens of
    /// seconds inside its 300 s budget. That estimate is the one number a maintainer should check on a
    /// store of that size BEFORE installing the extension there — the fencing means the check can be made
    /// on a copy, and the column stays NULL until it is. Table names are the <see cref="PayloadDimensions"/>
    /// constants. $1 metric_time — the SAME stamp the dimension INSERT carried, so exactly this run's two
    /// rows are updated and no older sweep's.</para>
    /// </summary>
    public const string ToastLiveBytesUpdateSql = $@"
UPDATE collect.store_metrics AS m
SET    toast_live_bytes = greatest(m.toast_bytes - f.free_bytes, 0)
FROM (
    SELECT
        d.dim_name,
        (SELECT coalesce(sum(fs.avail), 0)::bigint
           FROM pg_class AS c
           CROSS JOIN LATERAL pg_freespace(c.reltoastrelid) AS fs
          WHERE c.oid = ('collect.' || d.dim_name)::regclass
          AND   c.reltoastrelid <> 0) AS free_bytes
    FROM (VALUES ('{PayloadDimensions.QueryTextDimTable}'), ('{PayloadDimensions.QueryPlanDimTable}')) AS d(dim_name)
) AS f
WHERE m.metric_time = $1
AND   m.object_kind = '{DimensionObjectKind}'
AND   m.object_name = f.dim_name
AND   m.toast_bytes IS NOT NULL";

    /// <summary>The <c>object_kind</c> of the store's own checkpointer row (#3783). See
    /// <see cref="HypertableObjectKind"/> for why it is a const; the MCP reader and the self-alert both
    /// filter on it, and a drifted spelling would read as "no checkpointer row yet" forever.</summary>
    public const string CheckpointerObjectKind = "checkpointer";

    /// <summary>The <c>object_name</c> of the one <see cref="CheckpointerObjectKind"/> row — the 17+ view's
    /// name on EVERY major, including the ones that still serve the counters from <c>pg_stat_bgwriter</c>
    /// (#3783). One name, so a store that crosses a major upgrade keeps one series: the row is named for
    /// the process it measures, not for the catalog view the sweep happened to read it from.</summary>
    public const string CheckpointerObjectName = "pg_stat_checkpointer";

    /// <summary>The first PostgreSQL major on which the checkpointer's counters live in their own view,
    /// <c>pg_stat_checkpointer</c> (17). Below it the same three counters are columns of
    /// <c>pg_stat_bgwriter</c> under their old names; <see cref="SweepAsync"/> picks the statement by the
    /// connection's reported major, because a statement naming a view the catalog lacks fails at analysis
    /// (the <see cref="ExtensionInstalledSql"/> reasoning) and cannot be made conditional inside SQL.</summary>
    public const int CheckpointerViewMajorVersion = 17;

    /// <summary>
    /// The store's own checkpointer row (#3783), PostgreSQL 17+ — every store shape, one row per sweep,
    /// carrying the CUMULATIVE counters: milliseconds the checkpointer has spent in the write phase and in
    /// the sync (fsync) phase since the statistics were last reset, and how many REQUESTED checkpoints it has
    /// run — the ones forced by WAL volume reaching <c>max_wal_size</c> rather than by
    /// <c>checkpoint_timeout</c>, the count that says the store outran its WAL sizing (#3802's lever, the v12
    /// managed-conf block) rather than merely reaching the clock. <c>write_time</c> / <c>sync_time</c> are
    /// <c>double precision</c> milliseconds in the view and are rounded to the column's <c>bigint</c>.
    ///
    /// <para><b>Why the row stores the raw counters and the READ computes the interval, when the rung doc
    /// spoke of deltas.</b> The figure an operator wants IS the delta — "how many seconds of fsync did the
    /// last hour hold" — and V137's column doc described that figure. A delta needs the previous cumulative,
    /// and there are two honest places to keep it. An in-process baseline writes deltas and loses one hour
    /// on every service restart (the first sweep after start has nothing to subtract from), cannot tell the
    /// reader whether a NULL row was that restart or a counter reset, and needs the interval it spanned
    /// persisted somewhere — which on this table means bending another column's name, the shape the
    /// <c>job_history</c> kind already took once and the rung ruled against repeating. Storing the counter
    /// keeps the row STATELESS (a restart costs nothing: the previous row is on disk), makes a reset
    /// detectable from the rows alone (the newest counter reads BELOW the previous one), and yields the
    /// interval from the two rows' own <c>metric_time</c> stamps with no column borrowed. It is also the
    /// convention every <c>pg_stat_*</c>-sourced collector table in this store already follows: the stored
    /// column is the raw counter and <c>get_pg_database_stats</c>' "windowed difference clamped per
    /// interval" is computed by the MCP read — an operator who <c>SUM()</c>s a stored counter gets a
    /// nonsense total on either table, and the class summary's per-kind paragraph says so for this one.
    /// <c>DarlingStoreMetricsReader.CheckpointerReading</c> is the one place the difference is taken, for
    /// the tool and the self-alert alike.</para>
    ///
    /// <para><b>What a reader must do with the pair.</b> Delta = newest − previous on each of the three; the
    /// interval is the two stamps' difference (an hour on a healthy sweep, longer across a skipped tick,
    /// and the reader publishes the measured span rather than assuming the cadence). Any of the three going
    /// BACKWARDS means <c>pg_stat_reset_shared('checkpointer')</c> (<c>'bgwriter'</c> before 17) or a
    /// cluster restart without statistics persistence ran between the two sweeps: the interval is then
    /// unmeasurable and every delta is NULL with that reason — the #3705 discontinuity idiom, never a
    /// negative or a clamped zero. The one shape that cannot be detected from the rows is a reset followed,
    /// inside the same interval, by MORE activity than the whole prior lifetime had accumulated; that reads
    /// as a small positive delta. On a checkpointer whose counters run since cluster start it is not a
    /// realistic hour, and it is stated rather than guarded against.</para>
    ///
    /// <para><b>And which postmaster wrote the row (V139, #3955).</b> A counter that goes backwards is not the
    /// only discontinuity. A CLEAN restart keeps the counters (the statistics are written out at shutdown) and
    /// ADDS one: PostgreSQL counts the shutdown checkpoint in <c>num_requested</c>, measured +1 per fast stop on
    /// 18.6. So every restart between two sweeps put a requested checkpoint in the interval that WAL volume did not
    /// force, and the Store Checkpointer Pressure self-alert fired on each service restart. The row therefore
    /// also stores <c>pg_postmaster_start_time()</c> as naive UTC (<c>AT TIME ZONE 'UTC'</c>, never a bare cast,
    /// which renders in the session's zone), and the reader applies <see cref="PostmasterRestart"/>: across a
    /// restart the interval states no delta at all, because the shutdown checkpoint is in the requested count and
    /// its own write and sync phases are in the other two counters. That skips one hourly interval per restart; the
    /// next interval is judged normally.</para>
    ///
    /// <para><b>And how many checkpoints were timed (V140, #4037).</b> <c>num_timed</c> rides beside the other
    /// four so the reader can judge the interval's AVERAGE sync milliseconds per checkpoint
    /// (<c>SyncMs / (timed + requested)</c>) rather than the interval's summed sync milliseconds against the
    /// same bar — a healthy store running twelve five-second checkpoints an hour breached the old sum rule
    /// every interval even though no single checkpoint came near it. A row from before this rung carries a
    /// NULL timed count, which the average arm reads as unmeasured, never as zero.</para>
    ///
    /// <para>Single-row view, so no join and no filter. $1 metric_time.</para>
    /// </summary>
    public const string CheckpointerInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested, postmaster_start_time, checkpoints_timed)
SELECT
    $1,
    '{CheckpointerObjectName}',
    '{CheckpointerObjectKind}',
    round(c.write_time)::bigint,
    round(c.sync_time)::bigint,
    c.num_requested,
    pg_postmaster_start_time() AT TIME ZONE 'UTC',
    c.num_timed
FROM pg_stat_checkpointer AS c";

    /// <summary>
    /// <see cref="CheckpointerInsertSql"/> for PostgreSQL below <see cref="CheckpointerViewMajorVersion"/>
    /// (#3783): the same three cumulative counters under the names <c>pg_stat_bgwriter</c> carried them
    /// through 16 — <c>checkpoint_write_time</c>, <c>checkpoint_sync_time</c>, <c>checkpoints_req</c> — written
    /// under the SAME object name and kind so the series is one series. The bundled store is 18 and never
    /// runs this arm; a bring-your-own store on 14–16 does. Same row shape, same reader, the same postmaster
    /// start time beside the counters (<c>checkpoints_req</c> counts the shutdown checkpoint too). $1 metric_time.
    /// </summary>
    public const string CheckpointerBgwriterInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, checkpoint_write_ms, checkpoint_sync_ms, checkpoints_requested, postmaster_start_time, checkpoints_timed)
SELECT
    $1,
    '{CheckpointerObjectName}',
    '{CheckpointerObjectKind}',
    round(b.checkpoint_write_time)::bigint,
    round(b.checkpoint_sync_time)::bigint,
    b.checkpoints_req,
    pg_postmaster_start_time() AT TIME ZONE 'UTC',
    b.checkpoints_timed
FROM pg_stat_bgwriter AS b";

    /// <summary>The <c>object_kind</c> of the named plain-table rows (#3582). See
    /// <see cref="HypertableObjectKind"/> for why it is a const.</summary>
    public const string TableObjectKind = "table";

    /// <summary>
    /// <c>config.config_alert_log</c>, schema-qualified, for <see cref="TableInsertSql"/> and the
    /// un-enumerated predicate. The alert log has no owning store class with a name constant the way the
    /// two Query Store tables do (<c>QueryStoreTextStore.TableName</c>, <c>QueryStorePlanMap.TableName</c>):
    /// its writers reach it through the bare name and the session <c>search_path</c>. The V8 schema split
    /// placed it in <c>config</c> (<c>PgSchemaGenerator.ConfigTables</c>), which is where it is sized.
    /// </summary>
    public const string AlertLogTable = "config.config_alert_log";

    /// <summary>
    /// The named plain-table rows (#3582) — every store shape, like the dimension rows, and in the same
    /// shape: <c>pg_total_relation_size</c> (heap + indexes + TOAST) and the exact row count. Three
    /// product-owned tables that are neither hypertables nor payload dimensions and were therefore
    /// invisible to the inventory: <c>collect.query_store_text</c>, which V74 made an INLINE text store by
    /// design (Query Store already de-duplicates statement text one row per statement per database, so
    /// there was nothing for a digest dimension to squeeze) and which was 15 GiB on the largest production
    /// store; <c>collect.query_store_plan_map</c>, the V72 plan-id-to-digest map; and
    /// <c>config.config_alert_log</c>, the alert history and dismissals. They are stable, named, and the
    /// product knows them, so the inventory knows them by name instead of lumping them into
    /// <see cref="OtherObjectKind"/>.
    ///
    /// <para><b>Schema-qualified <c>object_name</c>, unlike every other kind.</b> The hypertable,
    /// aggregate and dimension rows are bare because their catalogs name them bare and every one lives in
    /// <c>collect</c>. This kind spans two schemas, and the un-enumerated census it shares a population
    /// with (<c>DarlingStoreMetricsReader.LargestUnenumeratedSql</c>) names relations
    /// <c>schema.relation</c>, so a table that moves from that list to this one keeps its name.</para>
    ///
    /// <para><b><c>row_count</c> here is the planner's <c>reltuples</c> ESTIMATE, not a scan, and the two
    /// kinds differ on purpose.</b> The dimension arm counts exactly because a dim's heap is small — its
    /// bytes live in TOAST, which a count never reads. <c>query_store_text</c> is the opposite shape: V74
    /// stores statement text INLINE, most statements fit a heap page, so the heap IS the 15 GiB and an
    /// exact <c>count(*)</c> would be a 15 GiB read every hour, on the same store the CAGG refresh convoy
    /// is running on, for a figure whose job is "roughly how many statements have text". <c>reltuples</c>
    /// is refreshed by every autovacuum and ANALYZE, is exact enough for that job, and costs one catalog
    /// row. It is <c>-1</c> for a table never vacuumed or analysed (PostgreSQL 14+), which maps to NULL
    /// rather than to a count of minus one. The same estimate is used for all three so the kind means one
    /// thing. $1 metric_time.</para>
    /// </summary>
    public const string TableInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, row_count)
SELECT
    $1,
    '{QueryStoreTextStore.TableName}',
    '{TableObjectKind}',
    pg_total_relation_size('{QueryStoreTextStore.TableName}'),
    (SELECT CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint END FROM pg_class c WHERE c.oid = '{QueryStoreTextStore.TableName}'::regclass)
UNION ALL
SELECT
    $1,
    '{QueryStorePlanMap.TableName}',
    '{TableObjectKind}',
    pg_total_relation_size('{QueryStorePlanMap.TableName}'),
    (SELECT CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint END FROM pg_class c WHERE c.oid = '{QueryStorePlanMap.TableName}'::regclass)
UNION ALL
SELECT
    $1,
    '{AlertLogTable}',
    '{TableObjectKind}',
    pg_total_relation_size('{AlertLogTable}'),
    (SELECT CASE WHEN c.reltuples >= 0 THEN c.reltuples::bigint END FROM pg_class c WHERE c.oid = '{AlertLogTable}'::regclass)";

    /// <summary>The <c>object_kind</c> of the user-schema catch-all row (#3582): every relation in a
    /// non-system schema that no named row accounts for. See <see cref="HypertableObjectKind"/> for why
    /// it is a const.</summary>
    public const string OtherObjectKind = "other";

    /// <summary>The <c>object_name</c> of the one <see cref="OtherObjectKind"/> row. A singleton per sweep,
    /// so the name is a fixed label rather than a relation's name.</summary>
    public const string OtherObjectName = "un-enumerated relations";

    /// <summary>The <c>object_kind</c> of the system catch-all row (#3582): <c>pg_catalog</c>,
    /// <c>information_schema</c>, and TimescaleDB's own schemas minus the chunk relations the hypertable
    /// and aggregate rows already size. See <see cref="HypertableObjectKind"/> for why it is a const.</summary>
    public const string SystemObjectKind = "system";

    /// <summary>The <c>object_name</c> of the one <see cref="SystemObjectKind"/> row.</summary>
    public const string SystemObjectName = "catalog and TimescaleDB internals";

    /// <summary>
    /// The relations the catch-all census considers at all, as a fragment shared by the two sweep variants
    /// and the MCP reader's live top-N (#3582): ordinary tables, materialized views, partitioned parents
    /// and sequences, sized with <c>pg_total_relation_size</c> so their indexes and TOAST tables ride along
    /// under the parent and are never counted twice (which is why <c>relkind</c> <c>i</c>, <c>I</c> and
    /// <c>t</c> are not listed). <c>NOT c.relisshared</c> because the shared catalogs
    /// (<c>pg_authid</c>, <c>pg_database</c>, ...) live in the cluster's <c>global/</c> directory and are
    /// NOT inside <c>pg_database_size</c> — measured on a fresh 2.28.1 rig, summing them made the census
    /// EXCEED the database by 512 KiB, and a reconciliation that starts over 100% is wrong in the direction
    /// nobody checks. Aliases <c>c</c> (<c>pg_class</c>) and <c>n</c> (<c>pg_namespace</c>) are the
    /// contract every consumer of this fragment supplies.
    /// </summary>
    public const string CensusRelationPredicateSql = @"c.relkind IN ('r', 'm', 'p', 'S')
AND   NOT c.relisshared";

    /// <summary>
    /// Which side of the user/system line a relation falls on, as a fragment over <c>n.nspname</c> (#3582).
    /// System: PostgreSQL's own two schemas, and every schema TimescaleDB creates — the <c>_timescaledb_</c>
    /// family (<c>_catalog</c>, <c>_config</c>, <c>_cache</c>, <c>_internal</c>, <c>_functions</c>) plus
    /// its two information schemas. <c>_timescaledb_internal</c> is where chunks live, but chunks and their
    /// compressed relations are removed from the census before this predicate is applied
    /// (<see cref="TimescaleInventoriedPredicateSql"/>), so what remains of it here is TimescaleDB's
    /// bookkeeping: <c>bgw_job_stat_history</c> (the <c>job_history</c> table itself), the compressed
    /// hypertables' empty roots (before 2.29.0 only), and any chunk relation whose catalog row is gone —
    /// which is exactly the class of residue a reconciliation should count rather than lose. <c>pg_toast</c>
    /// is not named because TOAST relations are <c>relkind = 't'</c> and
    /// already excluded by <see cref="CensusRelationPredicateSql"/>; their bytes arrive through their
    /// parents. <c>starts_with</c> rather than <c>LIKE</c> so the underscore is a character and not a
    /// wildcard, with no escape-string dialect to get wrong.
    /// </summary>
    public const string SystemSchemaPredicateSql =
        @"(n.nspname IN ('pg_catalog', 'information_schema', 'timescaledb_information', 'timescaledb_experimental')
       OR starts_with(n.nspname, '_timescaledb_'))";

    /// <summary>
    /// The relations some NAMED row already sizes, every store shape (#3582): the two payload dimensions
    /// and the three named plain tables. A relation matched here is never in a catch-all row, or the
    /// reconciliation would count it twice. Every name is the SAME compile-time constant the INSERT arm
    /// interpolates — the dims through <see cref="PayloadDimensions"/>, the tables through their
    /// schema-qualified owners' constants — so the census and the rows it excludes cannot drift apart. That
    /// is why the comparison is on the concatenated <c>schema.relation</c> rather than on a
    /// <c>(schema, relation)</c> tuple: the table constants are compound (<c>"collect.query_store_text"</c>)
    /// and cannot be split at compile time, and a hand-typed tuple beside them would be exactly the copy
    /// this constant exists not to have (review catch on the first cut, which had three). No product
    /// relation name contains a dot, so the concatenation is unambiguous. Aliases <c>c</c> and <c>n</c> as
    /// on <see cref="CensusRelationPredicateSql"/>.
    /// </summary>
    public const string NamedRelationPredicateSql = $@"(n.nspname || '.' || c.relname) IN (
        'collect.{PayloadDimensions.QueryTextDimTable}',
        'collect.{PayloadDimensions.QueryPlanDimTable}',
        '{QueryStoreTextStore.TableName}',
        '{QueryStorePlanMap.TableName}',
        '{AlertLogTable}')";

    /// <summary>
    /// The relations the TimescaleDB rows already size, as a fragment (#3582, reshaped by #3918): every
    /// hypertable root the <c>hypertables</c> view lists, every materialization root the
    /// <c>continuous_aggregates</c> view names, every chunk the <c>chunks</c> view lists, and every relation
    /// holding a chunk's COMPRESSED data. That is exactly the population <c>hypertable_detailed_size</c> adds
    /// up for the hypertable and aggregate rows: the root, each catalog chunk, and
    /// <c>relation_size(compress_relid)</c> through each chunk's <c>compression_settings</c> row (TimescaleDB's
    /// own <c>_timescaledb_internal.hypertable_chunk_local_size</c>, whose compressed-size join is identical on
    /// 2.28.1 and 2.30.1). So a relation matched here is inside a named row, and one that is not falls to a
    /// catch-all. It is also what
    /// keeps the census from re-summing the ~40,000 chunk relations a production store holds: the catch-all
    /// statement costs hash anti-joins over <c>pg_class</c> rather than a <c>pg_total_relation_size</c> per
    /// chunk. The root and chunk arms join on <c>(schema, name)</c> rather than casting a catalog value to
    /// <c>regclass</c>: a row whose relation is gone would make the cast RAISE and fail the sweep, where a
    /// name join simply matches nothing.
    ///
    /// <para><b>Why the public <c>chunks</c> view and not <c>_timescaledb_catalog.chunk</c> (#3918).</b>
    /// TimescaleDB 2.29.0 rebuilt the chunk catalog: its update script drops <c>chunk.schema_name</c>,
    /// <c>table_name</c> and <c>compressed_chunk_id</c> and adds <c>relid regclass</c>. The first cut of this
    /// fragment joined the catalog on the two dropped columns, and PostgreSQL resolves column names at
    /// analysis, so on 2.29+ the whole catch-all statement failed with 42703 before it read a row. The sweep
    /// threw after its earlier statements had committed their rows, so the run had no catch-all or store row
    /// to reconcile them against, and the store-log census and collector-cost flush riding the same tick
    /// stopped with it. Joining on <c>relid</c> instead fails the same way on 2.28.1, which has no such
    /// column, and one build meets both shapes: the bundled runtime, a bring-your-own store on any version,
    /// a store whose extension update did not complete. The <c>chunks</c> view is rebuilt by every
    /// version's own update script and keeps <c>chunk_schema</c> and <c>chunk_name</c> across the rewrite
    /// (taken from the catalog's names on 2.28.1, from <c>pg_class</c> through <c>relid</c> on 2.30.1), so
    /// one statement serves both shapes with no version probe and no second copy of the census. Its cost,
    /// measured on a 20,000-chunk store with 10,000 of them compressed (30,093 relations), is about 240 to
    /// 260 ms warm against the old catalog join's 200 ms on 2.28.1, and 230 to 240 ms on 2.30.1, with the
    /// same survivors and bytes as the old join on 2.28.1.</para>
    ///
    /// <para><b>Why compressed data is a fourth arm, keyed on <c>compression_settings.compress_relid</c>.</b>
    /// On 2.28.1 a chunk's compressed data lives in <c>compress_hyper_N_M_chunk</c>, itself a chunk of an
    /// internal compressed hypertable, which the old catalog join removed as a chunk. From 2.29.0 compression
    /// keeps no internal hypertable: the update deletes those chunk rows and drops the
    /// <c>_compressed_hypertable_N</c> roots, and the compressed relation (<c>_hyper_N_M_chunk_compressed</c>
    /// on a fresh 2.30.1 store, the old <c>compress_hyper_N_M_chunk</c> name kept on an upgraded one) is in no
    /// chunk catalog row at all. The one catalog that names it on every version is
    /// <c>compression_settings.compress_relid</c>, the column <c>hypertable_detailed_size</c> sizes it
    /// through, so this arm is what stops the <see cref="SystemObjectKind"/> row counting those bytes a
    /// second time. Measured on a seeded store (72 hypertables, 24 aggregates, 850 of 1,118 chunks
    /// compressed): 850 compressed relations holding 34,816,000 bytes on 2.28.1, a fresh 2.30.1 and an
    /// upgraded 2.30.1 alike, every one a <c>compress_relid</c> and none an inheritance child of anything.
    /// On 2.28.1 the <c>chunks</c> view hides compressed chunks (<c>compression_state != 2</c>) and this arm
    /// removes them instead: the two arms together exclude exactly the 1,968 relations the old catalog join
    /// did. TimescaleDB keeps the column honest in both directions (measured on both versions): decompressing
    /// a chunk clears its <c>compress_relid</c> and dropping a chunk deletes the settings row, so the arm can
    /// neither miss a live compressed relation nor exclude a stray one. The comparison is on the OID the
    /// <c>regclass</c> column stores, which resolves no name and so cannot raise.</para>
    ///
    /// <para>The internal compressed hypertables' roots on 2.28.1 (<c>_compressed_hypertable_N</c>, zero
    /// bytes) are deliberately NOT excluded: no named row sizes them, so they fall to the
    /// <see cref="SystemObjectKind"/> row, which is where an unattributed byte belongs. From 2.29.0 there are
    /// none. TimescaleDB-only; the plain-PostgreSQL variant omits it. Aliases <c>c</c> and <c>n</c> as on
    /// <see cref="CensusRelationPredicateSql"/>.</para>
    /// </summary>
    public const string TimescaleInventoriedPredicateSql = @"NOT EXISTS (
        SELECT 1 FROM timescaledb_information.hypertables h
        WHERE h.hypertable_schema = n.nspname AND h.hypertable_name = c.relname)
AND   NOT EXISTS (
        SELECT 1 FROM timescaledb_information.continuous_aggregates ca
        WHERE ca.materialization_hypertable_schema = n.nspname AND ca.materialization_hypertable_name = c.relname)
AND   NOT EXISTS (
        SELECT 1 FROM timescaledb_information.chunks ch
        WHERE ch.chunk_schema = n.nspname AND ch.chunk_name = c.relname)
AND   NOT EXISTS (
        SELECT 1 FROM _timescaledb_catalog.compression_settings cs
        WHERE cs.compress_relid = c.oid)";

    /// <summary>
    /// The two catch-all rows (#3582), TimescaleDB variant: one <c>INSERT ... SELECT</c> over a census CTE
    /// of every relation no named row accounts for, split by <see cref="SystemSchemaPredicateSql"/> into
    /// the <see cref="OtherObjectKind"/> row (user schemas — the product's own registry and config tables,
    /// <c>store_metrics</c> itself, and anything an operator added: the population the product might want
    /// to name next) and the <see cref="SystemObjectKind"/> row (the catalogs, which on a chunk-heavy store
    /// are not small: <c>pg_attribute</c>, <c>pg_statistic</c> and <c>pg_class</c> grow with the chunk
    /// count). <c>total_bytes</c> is the <c>pg_total_relation_size</c> sum, <c>chunk_count</c> the relation
    /// count. <c>coalesce(..., 0)</c> because an empty bucket is a ZERO, not a missing reading — the
    /// reconciliation needs both rows present every sweep so an absent row means the statement did not
    /// run, never that there was nothing to count.
    ///
    /// <para><b>Why two rows and not one.</b> The issue asked for <c>other</c>; <c>system</c> is what
    /// makes <c>other</c> readable. Folded together, the catalogs' bytes would inflate the count of
    /// "relations the product should know by name" with sixty <c>pg_catalog</c> tables no product would
    /// ever name; left out, they would surface as an unreconciled gap of a percent or more on every store,
    /// every hour — a false finding of exactly the shape this work exists to stop. Attributing them to a
    /// row of their own is what lets the residual gap be small enough to mean something.</para>
    ///
    /// <para>The statement touches <c>pg_class</c> once and sizes only the relations that survive the
    /// anti-joins — a few hundred at most — so its cost does not follow the chunk count. $1 metric_time.</para>
    /// </summary>
    public const string UnenumeratedInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, chunk_count)
WITH census AS (
    SELECT
        c.oid,
        {SystemSchemaPredicateSql} AS is_system
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE {CensusRelationPredicateSql}
    AND   NOT {NamedRelationPredicateSql}
    AND   {TimescaleInventoriedPredicateSql}
)
SELECT $1, '{OtherObjectName}', '{OtherObjectKind}', coalesce(sum(pg_total_relation_size(oid)), 0)::bigint, count(*)::integer
FROM census WHERE NOT is_system
UNION ALL
SELECT $1, '{SystemObjectName}', '{SystemObjectKind}', coalesce(sum(pg_total_relation_size(oid)), 0)::bigint, count(*)::integer
FROM census WHERE is_system";

    /// <summary>
    /// The two catch-all rows, plain-PostgreSQL variant (#3582): <see cref="UnenumeratedInsertSql"/>
    /// minus <see cref="TimescaleInventoriedPredicateSql"/>, because the TimescaleDB catalogs it names do
    /// not exist there. On such a store the collector tables are ordinary tables and no row enumerates
    /// them, so the <see cref="OtherObjectKind"/> row holds most of the database and the reader's coverage
    /// note says so in those words — an honest low number, not a fault. Per-collector-table rows for plain
    /// PostgreSQL would be the natural extension and are not taken here. $1 metric_time.
    /// </summary>
    public const string UnenumeratedPlainInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, chunk_count)
WITH census AS (
    SELECT
        c.oid,
        {SystemSchemaPredicateSql} AS is_system
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE {CensusRelationPredicateSql}
    AND   NOT {NamedRelationPredicateSql}
)
SELECT $1, '{OtherObjectName}', '{OtherObjectKind}', coalesce(sum(pg_total_relation_size(oid)), 0)::bigint, count(*)::integer
FROM census WHERE NOT is_system
UNION ALL
SELECT $1, '{SystemObjectName}', '{SystemObjectKind}', coalesce(sum(pg_total_relation_size(oid)), 0)::bigint, count(*)::integer
FROM census WHERE is_system";

    /// <summary>
    /// The <c>object_kind</c> of the owner's <c>job_history</c> evidence row (#3574). See
    /// <see cref="HypertableObjectKind"/> for why it is a const; the MCP reader filters on it to find the
    /// one row that can make <c>recording</c> a measurement in managed mode.
    /// </summary>
    public const string JobHistoryObjectKind = "job_history";

    /// <summary>
    /// The evidence read behind <c>recording</c> (#3574), named ONCE here because it now has two consumers
    /// that must never disagree about what they count: the MCP reader runs it as the connection asking
    /// (<c>DarlingStoreMetricsReader.JobHistoryEvidenceSql</c> is this string), and
    /// <see cref="JobHistoryInsertSql"/> embeds it verbatim to run it as the sweep's OWNER role and persist
    /// the answer. The full reasoning — the view's ownership filter, why the read evaluates the predicate
    /// for its own reader instead of counting and assuming, the population half from the unfiltered
    /// <c>job_stats</c>, the fixed 24-hour window, and the <c>timestamptz</c> bind — lives on the reader's
    /// alias, beside the code that interprets it. It lives HERE for the reason
    /// <see cref="JobExecutionLoggingSetting"/> does: the Storage project cannot reference the Service
    /// project, and a retyped copy of a nine-column predicate would drift silently.
    ///
    /// <para>$1 is the window start, <c>timestamptz</c>, bound with <c>Kind = Utc</c> — the columns it is
    /// compared to are TimescaleDB's own <c>TIMESTAMPTZ</c>, the inverse of the store's naive-UTC rule.</para>
    /// </summary>
    public const string JobHistoryEvidenceSql = @"
SELECT
    current_user::text AS reader_role,
    pg_has_role(
        current_user,
        (SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()),
        'MEMBER') IS TRUE AS reader_is_database_owner_member,
    (SELECT count(*) FROM timescaledb_information.jobs) AS job_count,
    (SELECT count(*)
       FROM timescaledb_information.jobs AS j
      WHERE pg_has_role(current_user, j.owner, 'MEMBER') IS TRUE) AS owner_member_job_count,
    (SELECT count(*)
       FROM timescaledb_information.job_history AS h
      WHERE h.start_time >= $1) AS rows_observed,
    (SELECT max(h.start_time) FROM timescaledb_information.job_history AS h) AS newest_row_at,
    (SELECT count(*)
       FROM timescaledb_information.job_stats AS js
      WHERE js.last_run_started_at >= $1) AS jobs_run_in_window,
    (SELECT max(NULLIF(js.last_run_started_at, '-infinity'::timestamptz))
       FROM timescaledb_information.job_stats AS js) AS newest_run_started_at";

    /// <summary>The evidence window <see cref="JobHistoryEvidenceSql"/> counts over, in hours — shared with
    /// the MCP reader for the same reason as the SQL. Fixed, not the tool's <c>days_back</c>: the question
    /// is whether the instrument is writing NOW, every job this product schedules runs at least daily, and
    /// the view's own retention policy trims rows after a month.</summary>
    public const int JobHistoryEvidenceWindowHours = 24;

    /// <summary>
    /// The owner's <c>job_history</c> evidence row (#3574) — TimescaleDB stores only. The sweep runs on
    /// the worker's OWNER pool: the role that ran the migrations and created every policy job, and so the
    /// role <c>timescaledb_information.job_history</c>'s ownership filter admits. It runs
    /// <see cref="JobHistoryEvidenceSql"/> as that role and persists what it saw, so that
    /// <c>get_store_metrics</c> — which in managed mode reads as the <c>mcp</c> role, a member of neither
    /// the database owner nor any job's owner, and is therefore shown NOTHING by construction — can put
    /// the owner's count beside its own verdict and say where the number came from. Without this row the
    /// managed block is honest but blind: <c>visibility: None</c>, <c>rows_observed: 0</c>, and
    /// <c>recording</c> a GUC echo forever.
    ///
    /// <para><b>The column mapping</b> is on the class summary; the load-bearing parts are these.
    /// <c>row_count</c> is the owner's <c>rows_observed</c> ONLY when the view admits this role to every
    /// job's history — the same two tests the reader's <c>Visibility</c> derives <c>All</c> from,
    /// evaluated in SQL: database-owner membership, or membership in every job's owner with at least one
    /// job to be a member of. Otherwise NULL, because a count the filter truncated is not a count, and the
    /// reader must be able to tell "the owner saw zero" from "the sweep's role could not see". Which role
    /// that was is <c>object_name</c>. <c>total_runs</c> is the unfiltered population half.
    /// <c>last_run_duration_ms</c> is the newest row's AGE at the sweep (<c>$2 - newest_row_at</c>, in
    /// milliseconds; NULL when the role saw no row ever), which the reader turns back into an instant. The
    /// window width rides in <c>schedule_interval_ms</c> so the row carries its own denominator — computed
    /// from the two binds (<c>$2 - $1</c>) rather than restated as a literal, so the row records the window
    /// it actually counted over.</para>
    ///
    /// <para><b>Two parameters, and $1 is NOT metric_time — the one arm in this sweep where that is so.</b>
    /// The embedded evidence SELECT is shared verbatim with the MCP reader and binds its window start as
    /// $1; renumbering it for this arm would mean two copies of the predicate, which is the drift this
    /// constant exists to prevent. So $1 is the window start (<c>timestamptz</c>, <c>Kind = Utc</c>) and
    /// $2 is metric_time (naive UTC). <c>$2::timestamp AT TIME ZONE 'UTC'</c> converts the naive stamp to
    /// the instant it denotes so the age subtraction is between two <c>timestamptz</c> values.</para>
    /// </summary>
    public const string JobHistoryInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, row_count, total_runs, schedule_interval_ms, last_run_duration_ms)
SELECT
    $2,
    e.reader_role,
    '{JobHistoryObjectKind}',
    CASE
        WHEN e.reader_is_database_owner_member
          OR (e.job_count > 0 AND e.owner_member_job_count >= e.job_count)
        THEN e.rows_observed
    END,
    e.jobs_run_in_window,
    (EXTRACT(EPOCH FROM (($2::timestamp AT TIME ZONE 'UTC') - $1)) * 1000)::bigint,
    (EXTRACT(EPOCH FROM (($2::timestamp AT TIME ZONE 'UTC') - e.newest_row_at)) * 1000)::bigint
FROM ({JobHistoryEvidenceSql}
) AS e";

    /// <summary>
    /// The <c>object_kind</c> the whole-store summary row carries, named ONCE because the string has
    /// several consumers that must never disagree: this sweep writes it (<see cref="StoreInsertSql"/>), the
    /// disk-pressure check filters on it (<see cref="LatestStoreSizeSql"/>), and
    /// <c>DarlingMcpStoreMetricsTools</c> partitions its response by it — the store summary is the one row
    /// those reads must separate from the per-object rows, and since #3582 the denominator every other
    /// kind's bytes are reconciled against.
    ///
    /// <para><b>Why a const and not literals.</b> A reader filtering on a kind the writer stopped
    /// writing returns ZERO ROWS, not an error, and every consumer here maps zero rows to a null or an
    /// omitted section. So a drifted spelling is indistinguishable from a store that has not swept yet —
    /// the honest-empty trap, on a value the shipped store already holds 400 days of. The value itself is
    /// therefore also part of the on-disk contract and cannot be renamed without orphaning that history.
    /// Same reasoning as <see cref="JobExecutionLoggingSetting"/>, one file over.</para>
    /// </summary>
    public const string StoreObjectKind = "store";

    /// <summary>
    /// The whole-store summary row. <c>pg_database_size</c> here is the ONLY place the product runs it on a
    /// cadence: it walks every file in the database directory, so its cost scales with the store rather
    /// than with the row it produces, and this sweep is where that cost belongs: hourly rather than every
    /// five minutes, on its own connection, and under <see cref="SweepTimeoutSeconds"/> — a budget #2317
    /// sized against a production store's own sizing queries rather than against a fixture. It is NOT off
    /// the collection loop's serial thread: the worker awaits this sweep inline, so its worst case still
    /// stalls per-server dispatch. What the sweep buys is 300 s of room and 1/12th the frequency, not
    /// isolation.
    /// <c>is_enabled</c> over the servers registry is the fleet reader's own enabled predicate, so
    /// "per-server" here means exactly the servers the fleet surfaces count. $1 metric_time.
    /// </summary>
    public const string StoreInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, enabled_server_count)
SELECT
    $1,
    current_database(),
    '{StoreObjectKind}',
    pg_database_size(current_database()),
    (SELECT count(*)::integer FROM collect.servers WHERE is_enabled)";

    /// <summary>
    /// The newest recorded whole-store size in bytes — what the store disk-pressure check reads to
    /// decorate its alert text with, instead of running <c>pg_database_size</c> itself every five minutes
    /// on the collection loop's serial thread (#3199).
    ///
    /// <para><b>Why the recorded value and not the live one.</b> <c>pg_database_size</c> stats every file
    /// in the database directory, so it is the one read on that thread whose cost scales with the store,
    /// and the 5 s bound it inherited from <c>ServiceCommandDeadlines.SerialLoopSeconds</c> was floored on
    /// 6.2 ms measured against a 4.05 GB fixture. Measured on a 225 GiB production store — 56x the fixture
    /// — thirteen samples of that same call spanned <b>2,090-3,745 ms</b> (mean ~2.5 s, one reading
    /// 3,177 ms): ~400-600x the time for 56x the size, because the cost tracks file count and a TimescaleDB
    /// store's file count follows chunks rather than bytes. That leaves 1.3-2.4x headroom under a bound
    /// whose derivation claimed ~806x. NONE of the thirteen crossed 5 s, which is the point: cancels ran
    /// mean 5.8/day against a nominal ~288 iterations (2.0%), so the visible failures were the tail and the
    /// other 98% paid ~2.5 s silently — under the deadline so no cancel, not a collector run so no
    /// <c>collection_log</c> row. This read, measured on the same store, is <b>0.101 ms</b> cold and
    /// 0.018 ms warm over three buffers — an <c>Index Scan Backward</c> on the existing V53
    /// <c>(metric_time)</c> index, adding none — and its cost does not move when the store grows.</para>
    ///
    /// <para><b>The walk is RELOCATED, not eliminated, and the honest claim is about which path pays
    /// it.</b> The row this reads is produced by <see cref="StoreInsertSql"/>, which runs
    /// <c>pg_database_size</c> itself — so the store-wide walk still happens, once an hour, inside a sweep
    /// that has room for it: <see cref="SweepTimeoutSeconds"/> is 300 s, ~120x the mean measured cost, and
    /// #2317 sized it against this very store's sizing queries rather than against a fixture. What changes
    /// is the count and the regime: ~312 executions a day, ~288 of them under a 5 s bound on the collection
    /// loop's serial thread, down to the 24 a day that were already being paid where the budget is. The
    /// per-read speedup is ~31,000x and applies to THAT read's latency, never to the change's overall
    /// effect — 92% fewer executions is that number.</para>
    ///
    /// <para><b>What it costs.</b> The value is the newest recorded sample rather than the current byte
    /// count, which is why the alert text names the sample instead of claiming currency. Priced against
    /// what the value is FOR: <c>DarlingSelfAlertEvaluator.ApplyDiskPressureAsync</c> uses it in exactly
    /// one place — one sentence of the alert detail — and never in a threshold, a comparison or a stored
    /// numeric value; the condition is judged on the store volume's free/total from <c>DriveInfo</c>,
    /// resolved on the same tick. So a stale figure cannot make the condition unjudgeable and cannot miss
    /// a fast fill. Deliberately NOT age-bounded: an interval past which the number is suppressed would be
    /// one more constant nobody measured, which is the defect #3199 is about; a stale sweep is reported by
    /// the sweep's own surfaces (its Warning line, <c>get_store_metrics</c>' series, and #3175's
    /// job-cadence self-alert) rather than by silently blanking a field here.</para>
    ///
    /// <para><b>How stale it actually gets is an observation, not a bound.</b> Over 30 days on one
    /// production store the series held 737 whole-store rows across 713 distinct hours — dense, so this is
    /// not a sparse source — with a mean gap of 58.6 minutes and the largest gap that HAPPENED to occur
    /// being 8,890.8 s (2.47 h). That maximum is a window artifact: it is the worst case in that sample on
    /// that store, it cannot decay, and nothing here guarantees it. Quote the cadence and the provenance,
    /// never "worst case is 2.47 hours".</para>
    ///
    /// <para><b>Its cheapness is incidental, not structural, and that is stated rather than glossed.</b>
    /// Every row one <see cref="SweepAsync"/> run writes carries the SAME <c>metric_time</c> — one
    /// <c>utcNow</c> stamps all of them, deliberately, so a run's rows join — and
    /// <c>idx_store_metrics_time</c> (V53) indexes <c>(metric_time)</c> alone. So a backward scan reaches
    /// this row early only because <see cref="StoreInsertSql"/> happens to run LAST in the sweep, not
    /// because the query says so. Measured on a 225 GiB store it is 3 buffers, which is that ordering
    /// holding; reordering the sweep, a <c>VACUUM</c> or a <c>REINDEX</c> could make the scan step past the
    /// rest of the tied group first.</para>
    ///
    /// <para><b>Why that is accepted here when #3199 rejected the same shape of argument.</b> The growth
    /// axis is different, and the axis is what made <c>pg_database_size</c> unbounded. The tied group is
    /// one sweep's output — <see cref="TimescaleSupport.HypertableCount"/> hypertable rows (70 today), one
    /// row per continuous aggregate, one row per Timescale background job, two dimension rows, three named
    /// plain-table rows, the two catch-all rows, the owner's <c>job_history</c> row, the checkpointer row and
    /// this one — so it
    /// tracks the COLLECTOR CATALOG, a product constant that moves only when a migration rung adds a
    /// hypertable or an aggregate, and every
    /// element is a narrow row on a plain table. <c>pg_database_size</c> tracked the store's file count,
    /// which retention span and ingest rate grow without anything choosing to. A composite
    /// <c>(object_kind, metric_time DESC)</c> index would make it exact and is the right follow-up; it
    /// needs a migration rung, and taking a rung number alongside unmerged siblings is its own
    /// documented hazard, so it is not bundled into the change that removed the unbounded read.</para>
    ///
    /// <para><c>total_bytes IS NOT NULL</c> because the column is nullable for the per-hypertable rows'
    /// sake: without it a hypothetical NULL newest row would mask a good older one, and both would arrive
    /// as the same null. On a store that has never completed a sweep this returns no row and the alert text
    /// simply carries no size — which is the first loop tick of a brand-new store, the disk check running
    /// ahead of the self-metrics sweep in the same tick. No parameters.</para>
    /// </summary>
    public const string LatestStoreSizeSql = $@"
SELECT total_bytes
FROM collect.store_metrics
WHERE object_kind = '{StoreObjectKind}'
AND   total_bytes IS NOT NULL
ORDER BY metric_time DESC
LIMIT 1";

    /// <summary>The sweep's own retention — one bounded DELETE, no policy machinery. $1 cutoff (naive UTC,
    /// metric_time minus <see cref="RetentionDays"/> days).</summary>
    public const string RetentionDeleteSql = @"
DELETE FROM collect.store_metrics
WHERE metric_time < $1";

    /// <summary>
    /// One self-metrics run: the hypertable, continuous-aggregate, background-job and owner
    /// <c>job_history</c> rows (only when <paramref name="timescaleAvailable"/> — the worker's cached
    /// <see cref="TimescaleSupport"/> detection), the dimension rows (and, only where the store carries
    /// <see cref="FreespacemapExtensionName"/>, their <c>toast_live_bytes</c>), the checkpointer row (the
    /// statement chosen by the connection's PostgreSQL major), the named plain-table rows, the two
    /// catch-all rows (the TimescaleDB or plain variant, by the same flag), the store summary row, then the
    /// retention DELETE, all stamped with one <paramref name="utcNow"/>. Returns the number of metric rows
    /// written (the caller logs it at Debug); the live-bytes UPDATE rewrites rows already counted and adds
    /// nothing to it.
    ///
    /// <para><b>Order matters for the reconciliation, and it is stated rather than relied on.</b> Every
    /// sizing statement runs before <see cref="StoreInsertSql"/>'s <c>pg_database_size</c>, so the
    /// database figure is the NEWEST reading of the run and the per-object rows are at most one sweep's
    /// duration older; ingest and retention keep moving underneath, so the residual the MCP reader
    /// computes is expected to be small and non-zero, never exactly zero. The catch-all rows run LAST
    /// among the sizing statements so the population they sum is the one the named rows were taken from.
    /// <see cref="LatestStoreSizeSql"/> documents the other property this order carries.</para>
    /// </summary>
    public static async Task<int> SweepAsync(
        NpgsqlConnection connection,
        bool timescaleAvailable,
        DateTime utcNow,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        /* Naive UTC by the product-wide cross-store contract — the same shape every collector stamps. */
        var metricTime = DateTime.SpecifyKind(utcNow, DateTimeKind.Unspecified);
        var written = 0;

        if (timescaleAvailable)
        {
            using var hypertables = new NpgsqlCommand(HypertableInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            hypertables.Parameters.AddWithValue(metricTime);
            written += await hypertables.ExecuteNonQueryAsync(cancellationToken);

            /* #3582: the materializations the hypertables view structurally omits. */
            using var aggregates = new NpgsqlCommand(ContinuousAggregateInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            aggregates.Parameters.AddWithValue(metricTime);
            written += await aggregates.ExecuteNonQueryAsync(cancellationToken);

            using var jobs = new NpgsqlCommand(BackgroundJobInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            jobs.Parameters.AddWithValue(metricTime);
            written += await jobs.ExecuteNonQueryAsync(cancellationToken);

            /* #3574: the owner's own reading of job_history, for the managed-mode reader that cannot take
               one. $1 is the evidence window start as an EXPLICIT timestamptz with Kind = Utc — the same
               bind the MCP reader makes and the inverse of every other bind in this sweep, because the
               columns it is compared to are TimescaleDB's own TIMESTAMPTZ (the reader's
               JobHistoryEvidenceSql paragraph has the full reasoning). $2 is the sweep's naive stamp. */
            using var history = new NpgsqlCommand(JobHistoryInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            history.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.TimestampTz,
                Value = DateTime.SpecifyKind(utcNow.AddHours(-JobHistoryEvidenceWindowHours), DateTimeKind.Utc),
            });
            history.Parameters.AddWithValue(metricTime);
            written += await history.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var dimensions = new NpgsqlCommand(DimensionInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            dimensions.Parameters.AddWithValue(metricTime);
            written += await dimensions.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #3783: toast_live_bytes, ONLY where the maintainer has installed pg_freespacemap. Two statements
           rather than one because a statement naming pg_freespace() fails at analysis on a store without
           the extension, whichever CASE arm would have reached it (the ExtensionInstalledSql paragraph). On
           the shipped store the probe returns no row and the column stays the NULL the INSERT wrote. */
        bool freespacemapInstalled;
        using (var probe = new NpgsqlCommand(ExtensionInstalledSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            probe.Parameters.AddWithValue(FreespacemapExtensionName);
            freespacemapInstalled = await probe.ExecuteScalarAsync(cancellationToken) is not null;
        }

        if (freespacemapInstalled)
        {
            using var liveBytes = new NpgsqlCommand(ToastLiveBytesUpdateSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            liveBytes.Parameters.AddWithValue(metricTime);
            await liveBytes.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #3783: the store's own checkpointer counters, raw (the CheckpointerInsertSql paragraph says why
           the read differences them). The view moved in 17 and a statement naming a view the catalog lacks
           fails at analysis, so the major picks the statement here; Npgsql reports it from the server's
           startup parameters, no round trip. Every store shape: both views exist on plain PostgreSQL. */
        var checkpointerSql = connection.PostgreSqlVersion.Major >= CheckpointerViewMajorVersion
            ? CheckpointerInsertSql
            : CheckpointerBgwriterInsertSql;
        using (var checkpointer = new NpgsqlCommand(checkpointerSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            checkpointer.Parameters.AddWithValue(metricTime);
            written += await checkpointer.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #3582: the product-owned plain tables the inventory knows by name. */
        using (var tables = new NpgsqlCommand(TableInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            tables.Parameters.AddWithValue(metricTime);
            written += await tables.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #3582: everything else, attributed to a row so the total reconciles. The TimescaleDB variant
           removes chunk relations, their compressed relations (#3918) and the roots the hypertable/aggregate
           rows already size; the plain variant cannot name those catalogs and has nothing to remove. Last of
           the sizing statements on purpose — see the summary. */
        using (var unenumerated = new NpgsqlCommand(
            timescaleAvailable ? UnenumeratedInsertSql : UnenumeratedPlainInsertSql, connection)
            { CommandTimeout = SweepTimeoutSeconds })
        {
            unenumerated.Parameters.AddWithValue(metricTime);
            written += await unenumerated.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var store = new NpgsqlCommand(StoreInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            store.Parameters.AddWithValue(metricTime);
            written += await store.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var retention = new NpgsqlCommand(RetentionDeleteSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            retention.Parameters.AddWithValue(metricTime.AddDays(-RetentionDays));
            await retention.ExecuteNonQueryAsync(cancellationToken);
        }

        logger?.LogDebug("Store self-metrics sweep wrote {Rows} rows at {MetricTime}", written, metricTime);
        return written;
    }
}
