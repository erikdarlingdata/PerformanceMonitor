/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The tool's own per-collector cost ON the monitored servers (#2674) — the self-monitoring that turns
/// "which of our collectors is a performance hog on a target" from a log-scrape into a stored series.
///
/// <para>Every collector run reports its cost as a <see cref="CollectorRunResult"/> (rows, sql_ms on the
/// TARGET, storage_ms on our store). This accumulates those in memory per (server, collector) and the
/// worker flushes an HOURLY aggregate row to <c>collect.collector_cost</c> (V105) on the same tick as the
/// store self-metrics sweep, exactly like <c>StoreSelfMetrics</c>. Aggregating in memory keeps the write
/// tiny (one row per server+collector per hour) and the read cheap, while <see cref="Bucket.MaxSqlMs"/>
/// preserves the TAIL — a single 555s execution is how a collector sticks out on a target, and an hourly
/// average would hide it.</para>
///
/// <para>sql_ms is a DURATION on the target (it includes waits), not pure CPU — a slow collector may be
/// latch-waiting rather than burning CPU. It still holds a connection/slot and competes, which is the
/// point of watching it.</para>
/// </summary>
public sealed class CollectorCostAccumulator
{
    /// <summary>Rows older than this are pruned by the flush's own bounded DELETE. Denser than
    /// store_metrics (per server+collector, not whole-store), so 90 days rather than 400 — ~40k rows/day on
    /// a 42-server fleet, ~3.6M at retention, which a btree-indexed plain table serves without ceremony.</summary>
    public const int RetentionDays = 90;

    public const int FlushTimeoutSeconds = 120;

    private sealed class Bucket
    {
        public long RunCount;
        public long TotalSqlMs;
        public long MaxSqlMs;
        public long TotalStorageMs;
        public long TotalRows;
    }

    private readonly ConcurrentDictionary<(int ServerId, string Collector), Bucket> _buckets = new();

    /// <summary>Record one collector run. Thread-safe: collectors run concurrently across servers.</summary>
    public void Record(int serverId, string collectorName, long rows, long sqlMs, long storageMs)
    {
        if (string.IsNullOrEmpty(collectorName))
        {
            return;
        }

        var bucket = _buckets.GetOrAdd((serverId, collectorName), static _ => new Bucket());

        /* Per-bucket lock rather than Interlocked on four fields: MaxSqlMs needs a compare-and-set that
           would otherwise race the sum, and a bucket is contended only by the handful of concurrent runs of
           the SAME collector on the SAME server, which is at most one. */
        lock (bucket)
        {
            bucket.RunCount++;
            bucket.TotalSqlMs += sqlMs;
            if (sqlMs > bucket.MaxSqlMs)
            {
                bucket.MaxSqlMs = sqlMs;
            }
            bucket.TotalStorageMs += storageMs;
            bucket.TotalRows += rows;
        }
    }

    /* One flush: drain the buckets and write an hourly aggregate row per (server, collector), then prune.
       database_name is NULL — a CollectorRunResult is already summed across a per-database collector's
       databases (DarlingCollectorRunner sums dbSqlMs), so the server+collector grain is the one the data
       actually has. Draining (TryRemove) resets the window, so an hour with no runs writes no row rather
       than a zero. */
    private const string InsertSql = @"
INSERT INTO collect.collector_cost
(
    metric_time, server_id, database_name, collector_name,
    run_count, total_sql_ms, max_sql_ms, total_storage_ms, total_rows
)
SELECT
    $1, u.server_id, NULL, u.collector_name,
    u.run_count, u.total_sql_ms, u.max_sql_ms, u.total_storage_ms, u.total_rows
FROM unnest($2::integer[], $3::text[], $4::integer[], $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[])
    AS u(server_id, collector_name, run_count, total_sql_ms, max_sql_ms, total_storage_ms, total_rows)";

    private const string RetentionDeleteSql = @"
DELETE FROM collect.collector_cost
WHERE metric_time < $1";

    /// <summary>Drain the accumulated buckets — one aggregate per (server, collector) — and RESET the window
    /// (TryRemove), so the next hour starts clean and an hour with no runs writes nothing. Extracted so the
    /// aggregation is unit-testable without a store.</summary>
    internal IReadOnlyList<DrainedCost> Drain()
    {
        var keys = _buckets.Keys.ToArray();
        var rows = new List<DrainedCost>(keys.Length);

        foreach (var key in keys)
        {
            if (!_buckets.TryRemove(key, out var bucket))
            {
                continue;
            }

            lock (bucket)
            {
                rows.Add(new DrainedCost(
                    key.ServerId, key.Collector, bucket.RunCount, bucket.TotalSqlMs,
                    bucket.MaxSqlMs, bucket.TotalStorageMs, bucket.TotalRows));
            }
        }

        return rows;
    }

    internal sealed record DrainedCost(
        int ServerId, string CollectorName, long RunCount, long TotalSqlMs, long MaxSqlMs, long TotalStorageMs, long TotalRows);

    public async Task FlushAsync(NpgsqlConnection connection, DateTime metricTimeUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        var drained = Drain();

        if (drained.Count > 0)
        {
            await using var insert = new NpgsqlCommand(InsertSql, connection) { CommandTimeout = FlushTimeoutSeconds };
            insert.Parameters.AddWithValue(metricTimeUtc);
            insert.Parameters.AddWithValue(drained.Select(d => d.ServerId).ToArray());
            insert.Parameters.AddWithValue(drained.Select(d => d.CollectorName).ToArray());
            insert.Parameters.AddWithValue(drained.Select(d => (int)Math.Min(d.RunCount, int.MaxValue)).ToArray());
            insert.Parameters.AddWithValue(drained.Select(d => d.TotalSqlMs).ToArray());
            insert.Parameters.AddWithValue(drained.Select(d => d.MaxSqlMs).ToArray());
            insert.Parameters.AddWithValue(drained.Select(d => d.TotalStorageMs).ToArray());
            insert.Parameters.AddWithValue(drained.Select(d => d.TotalRows).ToArray());
            await insert.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var retention = new NpgsqlCommand(RetentionDeleteSql, connection) { CommandTimeout = FlushTimeoutSeconds };
        retention.Parameters.AddWithValue(metricTimeUtc.AddDays(-RetentionDays));
        await retention.ExecuteNonQueryAsync(cancellationToken);

        logger?.LogDebug("collector_cost flush wrote {Rows} server+collector rows", drained.Count);
    }
}
