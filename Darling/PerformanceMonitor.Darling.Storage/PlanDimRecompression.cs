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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The <c>--recompress-plan-dim</c> work (#2076): convert the plan dimension's PRE-V54 text rows to the
/// gzip form V54's write path produces, in bounded batches, so the whole dimension reaches the compressed
/// steady state instead of only the rows that happen to churn.
///
/// <para><b>Why attrition alone is not enough.</b> V54 (#2069) left old rows to convert by GC turnover —
/// deliberately, because a migration-time rewrite of the store's largest table is the
/// peak-disk-before-relief trap. But the dimension GC retires a row only when its digest stops being
/// re-seen, and every sighting refreshes <c>last_seen</c> — so a STABLE plan's text row never ages out and
/// never converts. This verb reaches that permanent tail; it is the operator-paced version of the rewrite
/// the migration refused to do implicitly (the <c>--collapse-legacy-slices</c> precedent: rewriting stored
/// rows runs when a person decides it should, with <c>--dry-run</c> first).</para>
///
/// <para><b>Safety properties.</b> Each row's gzip bytes are round-trip VERIFIED (decompressed and compared
/// to the original text) before the text column is nulled, in the same UPDATE — a row that fails
/// verification keeps its text untouched and is reported, never converted blind. The update's own predicate
/// (<c>query_plan_xml IS NOT NULL</c>) makes every batch idempotent and the whole run resumable: an
/// interrupted run left some rows converted and some not, and the next run's fetch simply finds the
/// remainder. The digest is untouched — it was computed over the UNCOMPRESSED text (#2069's identity rule),
/// so fact-row references, dedup, and presence flags cannot be disturbed by re-encoding the content.</para>
///
/// <para><b>What this does NOT do.</b> It does not shrink the file. PostgreSQL returns each converted row's
/// old version as reusable free space inside the relation, so the observable effect is the dimension's
/// growth flatlining and its LIVE content shrinking (~34% measured on this content, 14.0x gzip vs 8.9x
/// lz4-TOAST); handing the freed space back to the volume is a separate one-time
/// <c>VACUUM FULL</c>/repack an operator may choose to run afterwards. Disclosed, not papered over.</para>
/// </summary>
public static class PlanDimRecompression
{
    /// <summary>The dimension this verb operates on — the ONLY compressed-content dim (#2069).</summary>
    public const string Table = PayloadDimensions.QueryPlanDimTable;

    /// <summary>
    /// Rows fetched and converted per round trip. Sized by memory, not by lock ambition: a batch holds its
    /// text in managed memory while compressing (~134 KB raw average on the production store, so ~130 MB a
    /// batch), and each batch commits independently so a kill at any point loses at most one batch of work.
    /// </summary>
    public const int BatchSize = 1000;

    /// <summary>Rows the dry run samples to measure the real compression ratio on this store's content.</summary>
    public const int DryRunSampleSize = 500;

    /// <summary>
    /// The remaining work, counted by content form. <c>Pending</c> is the fetch predicate's count — text
    /// rows with no gzip bytes; <c>Converted</c> counts rows already carrying gzip (V54 writes + prior runs
    /// of this verb).
    /// </summary>
    public const string SurveySql = """
        SELECT
            COUNT(*) FILTER (WHERE query_plan_xml IS NOT NULL AND query_plan_gz IS NULL) AS pending,
            COUNT(*) FILTER (WHERE query_plan_gz IS NOT NULL) AS converted,
            COUNT(*) AS total,
            pg_total_relation_size('query_plan_dim') AS relation_bytes
        FROM query_plan_dim
        """;

    /// <summary>
    /// One batch of unconverted rows. No ORDER BY on purpose: converted rows fall out of the predicate, so
    /// a plain re-fetch advances through the table without a keyset cursor, and any subset is as good as any
    /// other — this is a sweep, not a scan with a required order. $1 batch size.
    /// </summary>
    public const string FetchBatchSql = """
        SELECT digest, query_plan_xml
        FROM query_plan_dim
        WHERE query_plan_xml IS NOT NULL
        AND   query_plan_gz IS NULL
        LIMIT $1
        """;

    /// <summary>
    /// One statement per batch (the #1767 unnest idiom — a 1,000-row batch is one round trip, not 1,000).
    /// The <c>query_plan_xml IS NOT NULL</c> guard re-checks under the row lock what the fetch saw without
    /// one, so a row that somehow converted in between is skipped rather than double-written. The row's
    /// <c>last_seen</c> is deliberately untouched: recompression is not a sighting, and stamping it would
    /// push GC-eligible rows a full retention window into the future.
    /// </summary>
    public const string UpdateBatchSql = """
        UPDATE query_plan_dim
        SET query_plan_gz = u.gz,
            query_plan_xml = NULL
        FROM unnest($1::bytea[], $2::bytea[]) AS u(digest, gz)
        WHERE query_plan_dim.digest = u.digest
        AND   query_plan_dim.query_plan_xml IS NOT NULL
        """;

    /// <summary>
    /// The compaction that makes the conversion's saving visible to the OPERATING SYSTEM (#2076, follow-up):
    /// converting rewrites rows, so the relation keeps its high-water-mark size with the freed space
    /// internal — real, reusable, but invisible to a df/volume view. VACUUM FULL rewrites the relation to
    /// its live content. ACCESS EXCLUSIVE for the duration: the collectors' dimension flushes queue behind
    /// it and resume when it releases (the same backpressure a slow monitored server produces). Unqualified
    /// table name like every statement here — the connection's search_path owns schema resolution.
    /// </summary>
    public const string VacuumFullSql = "VACUUM FULL query_plan_dim";

    /// <summary>
    /// A fast estimate of the compacted relation's size, for the disk preflight: heap + indexes copy
    /// as-is, and the TOAST rebuilds to roughly row-count × the SAMPLED average gzip size. Sampled
    /// (<see cref="DryRunSampleSize"/> rows) because summing octet_length over the whole dimension detoasts
    /// the entire content — minutes of read for a preflight that needs one significant digit.
    /// </summary>
    /// <summary>
    /// One bound for every non-VACUUM statement in this maintenance pass (#2874). Same value the
    /// estimate already chose deliberately; hoisted so the survey, fetch and update loops cannot
    /// silently fall back to Npgsql's inherited 30 s default. VACUUM FULL keeps its explicit
    /// <c>CommandTimeout = 0</c> — unlimited is the deliberate choice there, not an omission.
    /// </summary>
    private const int MaintenanceStatementTimeoutSeconds = 300;

    public const string EstimateCompactedSql = """
        SELECT
            pg_relation_size('query_plan_dim')
            + pg_indexes_size('query_plan_dim')
            + (SELECT COUNT(*) FROM query_plan_dim)
              * COALESCE((SELECT AVG(octet_length(query_plan_gz))::bigint
                          FROM (SELECT query_plan_gz FROM query_plan_dim
                                WHERE query_plan_gz IS NOT NULL LIMIT 500) AS sample), 0)
        """;

    /// <summary>Rewrites the dimension to its live content. Unbounded command timeout — a 100+ GB relation
    /// legitimately takes many minutes, and killing it mid-rewrite just wastes the work (the rewrite is
    /// transactional; an interrupted VACUUM FULL leaves the original untouched).</summary>
    public static async Task VacuumFullAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(VacuumFullSql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>The compacted-size estimate for the disk preflight (see <see cref="EstimateCompactedSql"/>).</summary>
    public static async Task<long> EstimateCompactedBytesAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(EstimateCompactedSql, connection) { CommandTimeout = MaintenanceStatementTimeoutSeconds };
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is long bytes ? bytes : Convert.ToInt64(result, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>The survey's answer — the CLI prints it for both the dry run and the real run's preamble.</summary>
    public readonly record struct Survey(long Pending, long Converted, long Total, long RelationBytes)
    {
        /// <summary>True when at least one text row still needs converting.</summary>
        public bool HasWork => Pending > 0;
    }

    /// <summary>
    /// A finished run (or dry-run sample). Byte totals are UTF-8 text bytes in and gzip bytes out for the
    /// rows this run actually processed — measured, not estimated. <c>VerifyFailures</c> counts rows whose
    /// gzip bytes did not round-trip back to the original text; those rows keep their text and are the one
    /// thing an operator must look at before re-running.
    /// </summary>
    public readonly record struct Result(long Rows, long TextBytes, long GzipBytes, long VerifyFailures);

    /// <summary>Counts the remaining work. Cheap relative to the run; the CLI prints it first.</summary>
    public static async Task<Survey> SurveyAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        await using var command = new NpgsqlCommand(SurveySql, connection) { CommandTimeout = MaintenanceStatementTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new Survey(0, 0, 0, 0);
        }

        return new Survey(
            reader.IsDBNull(0) ? 0 : reader.GetInt64(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
            reader.IsDBNull(3) ? 0 : reader.GetInt64(3));
    }

    /// <summary>
    /// Compresses a SAMPLE of pending rows in memory and reports the measured ratio — the dry run's
    /// evidence. Writes nothing; the sample rows are fetched with the same statement the real run uses
    /// (bounded by <see cref="DryRunSampleSize"/>).
    /// </summary>
    public static async Task<Result> SampleAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        var batch = await FetchBatchAsync(connection, DryRunSampleSize, cancellationToken);
        var compressed = CompressAndVerify(batch);
        return Totals(compressed);
    }

    /// <summary>
    /// The real run: fetch, compress, verify, update, repeat until the fetch comes back empty. Each batch is
    /// one transaction; <paramref name="progress"/> is called after every batch with the running totals so
    /// the CLI can narrate a multi-hour run. Safe to interrupt and re-run at any point.
    /// </summary>
    public static async Task<Result> ConvertAsync(
        NpgsqlConnection connection, Action<Result>? progress, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);

        long rows = 0, textBytes = 0, gzipBytes = 0, verifyFailures = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var batch = await FetchBatchAsync(connection, BatchSize, cancellationToken);
            if (batch.Count == 0)
            {
                break;
            }

            var compressed = CompressAndVerify(batch);
            var good = compressed.Where(row => row.Verified).ToArray();

            if (good.Length > 0)
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
                await using var update = new NpgsqlCommand(UpdateBatchSql, connection, transaction) { CommandTimeout = MaintenanceStatementTimeoutSeconds };
                update.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea,
                    Value = good.Select(row => row.Digest).ToArray(),
                });
                update.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bytea,
                    Value = good.Select(row => row.Gzip).ToArray(),
                });
                await update.ExecuteNonQueryAsync(cancellationToken);
                await transaction.CommitAsync(cancellationToken);
            }

            var totals = Totals(compressed);
            rows += totals.Rows;
            textBytes += totals.TextBytes;
            gzipBytes += totals.GzipBytes;
            verifyFailures += totals.VerifyFailures;
            progress?.Invoke(new Result(rows, textBytes, gzipBytes, verifyFailures));

            /* Every fetched row either converted or failed verification. If ALL failed, stop rather than
               spin on the same rows forever — the fetch would return them again. */
            if (good.Length == 0)
            {
                break;
            }
        }

        return new Result(rows, textBytes, gzipBytes, verifyFailures);
    }

    private static async Task<List<(byte[] Digest, string Text)>> FetchBatchAsync(
        NpgsqlConnection connection, int limit, CancellationToken cancellationToken)
    {
        var batch = new List<(byte[], string)>(limit);
        await using var fetch = new NpgsqlCommand(FetchBatchSql, connection) { CommandTimeout = MaintenanceStatementTimeoutSeconds };
        fetch.Parameters.Add(new NpgsqlParameter<int> { TypedValue = limit });
        await using var reader = await fetch.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            batch.Add((reader.GetFieldValue<byte[]>(0), reader.GetString(1)));
        }

        return batch;
    }

    /// <summary>
    /// Compress + round-trip verify one batch. CPU-parallel because gzip at Optimal is the run's bottleneck
    /// and the work is pure per row; ordering does not matter (the update keys on digest).
    /// </summary>
    private static (byte[] Digest, byte[] Gzip, int TextBytes, bool Verified)[] CompressAndVerify(
        List<(byte[] Digest, string Text)> batch)
        => batch
            .AsParallel()
            .Select(row =>
            {
                var gzip = PayloadDimensions.CompressContent(row.Text);
                var verified = string.Equals(PayloadDimensions.DecompressContent(gzip), row.Text, StringComparison.Ordinal);
                return (row.Digest, gzip, Encoding.UTF8.GetByteCount(row.Text), verified);
            })
            .ToArray();

    private static Result Totals((byte[] Digest, byte[] Gzip, int TextBytes, bool Verified)[] compressed)
        => new(
            compressed.Count(row => row.Verified),
            compressed.Where(row => row.Verified).Sum(row => (long)row.TextBytes),
            compressed.Where(row => row.Verified).Sum(row => (long)row.Gzip.Length),
            compressed.Count(row => !row.Verified));
}
