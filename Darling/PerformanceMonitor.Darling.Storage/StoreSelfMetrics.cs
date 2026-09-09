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
/// (<c>pg_total_relation_size</c> — heap + indexes + TOAST, where the plan XML actually lives) and the
/// exact row count. The dims are the store's dominant payloads (measured: query_plan_dim alone was 101 GB
/// of a 147 GB store, 69%) and invisible to every hypertable-shaped surface because they are deliberately
/// PLAIN tables (see <see cref="PayloadDimensions.CreateDimTable"/>);</item>
/// <item>one summary row (<c>object_kind = 'store'</c>): <c>pg_database_size</c> plus the enabled-server
/// count (the fleet reader's <c>WHERE is_enabled</c> registry predicate), so the per-server ingest rate —
/// daily growth divided by servers, the number onboarding N primaries multiplies — is derivable from the
/// stored series alone.</item>
/// </list>
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
    /// awaited on the main loop, so five sequential per-statement timeouts must not stack). The
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

    /// <summary>
    /// The per-hypertable rows — TimescaleDB stores only (the caller gates on the detected flag; the
    /// timescaledb_information views referenced here do not exist on plain PostgreSQL).
    /// <c>hypertable_detailed_size</c> / <c>chunk_compression_stats</c> take a regclass, built with
    /// <c>format('%I.%I', ...)</c> from the catalog view's own rows — never user input.
    /// <c>compressed_*_bytes</c> are NULL for a hypertable with no compressed chunks yet. $1 metric_time
    /// (naive UTC, one value per run).
    /// </summary>
    public const string HypertableInsertSql = @"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, compressed_before_bytes, compressed_after_bytes, chunk_count)
SELECT
    $1,
    h.hypertable_name,
    'hypertable',
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
    public const string BackgroundJobInsertSql = @"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, last_run_duration_ms, schedule_interval_ms, total_runs, total_failures)
SELECT
    $1,
    j.proc_name || coalesce(' ' || j.hypertable_name, '') || ' [' || j.job_id || ']',
    'background_job',
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
    /// </summary>
    public const string DimensionInsertSql = $@"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, row_count)
SELECT
    $1,
    '{PayloadDimensions.QueryTextDimTable}',
    'dimension',
    pg_total_relation_size('collect.{PayloadDimensions.QueryTextDimTable}'),
    (SELECT count(*) FROM collect.{PayloadDimensions.QueryTextDimTable})
UNION ALL
SELECT
    $1,
    '{PayloadDimensions.QueryPlanDimTable}',
    'dimension',
    pg_total_relation_size('collect.{PayloadDimensions.QueryPlanDimTable}'),
    (SELECT count(*) FROM collect.{PayloadDimensions.QueryPlanDimTable})";

    /// <summary>
    /// The <c>object_kind</c> the whole-store summary row carries, named ONCE because the string now has
    /// six consumers that must never disagree: this sweep writes it (<see cref="StoreInsertSql"/>), the
    /// disk-pressure check filters on it (<see cref="LatestStoreSizeSql"/>), and
    /// <c>DarlingMcpStoreMetricsTools</c> partitions its response by it in four places — the store summary
    /// is the one row those reads must separate from the per-hypertable and per-dimension rows.
    ///
    /// <para><b>Why a const and not six literals.</b> A reader filtering on a kind the writer stopped
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
    /// row per Timescale background job, two dimension rows and this one — so it tracks the COLLECTOR
    /// CATALOG, a product constant that moves only when a migration rung adds a hypertable, and every
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
    /// One self-metrics run: the hypertable rows (only when <paramref name="timescaleAvailable"/> — the
    /// worker's cached <see cref="TimescaleSupport"/> detection), the dimension rows, the store summary
    /// row, then the retention DELETE, all stamped with one <paramref name="utcNow"/>. Returns the number
    /// of metric rows written (the caller logs it at Debug).
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

            using var jobs = new NpgsqlCommand(BackgroundJobInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds };
            jobs.Parameters.AddWithValue(metricTime);
            written += await jobs.ExecuteNonQueryAsync(cancellationToken);
        }

        using (var dimensions = new NpgsqlCommand(DimensionInsertSql, connection) { CommandTimeout = SweepTimeoutSeconds })
        {
            dimensions.Parameters.AddWithValue(metricTime);
            written += await dimensions.ExecuteNonQueryAsync(cancellationToken);
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
