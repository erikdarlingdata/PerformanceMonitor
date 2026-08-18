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
    /// The whole-store summary row. <c>pg_database_size</c> is the same read the disk-pressure check and
    /// the Viewer's status bar use; <c>is_enabled</c> over the servers registry is the fleet reader's own
    /// enabled predicate, so "per-server" here means exactly the servers the fleet surfaces count.
    /// $1 metric_time.
    /// </summary>
    public const string StoreInsertSql = @"
INSERT INTO collect.store_metrics
    (metric_time, object_name, object_kind, total_bytes, enabled_server_count)
SELECT
    $1,
    current_database(),
    'store',
    pg_database_size(current_database()),
    (SELECT count(*)::integer FROM collect.servers WHERE is_enabled)";

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
