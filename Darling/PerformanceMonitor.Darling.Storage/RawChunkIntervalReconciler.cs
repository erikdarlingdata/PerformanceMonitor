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
/// The live half of #4211: reads every raw hypertable's ingest rate and current interval, calls
/// <see cref="RawChunkIntervalPlanner.Plan"/>, applies every changing decision with
/// <c>set_chunk_time_interval</c>, and records the rung history <see cref="ReconcileAsync"/> itself reads
/// <see cref="RawChunkIntervalPlanner.TableInput.IntervalLastChangedUtc"/> back from
/// (<c>collect.raw_chunk_interval_rung_history</c>, V144). The planner stays pure; this class is the only
/// place that touches the store. Caller supplies <paramref name="budgetBytes"/> B ready-made — a managed
/// store's RAM figure and a bring-your-own store's <c>pg_settings</c> read both live in the Service project,
/// which this one does not reference (see <see cref="RawChunkIntervalPlanner.ManagedBudgetBytes"/>'s remarks).
/// </summary>
public static class RawChunkIntervalReconciler
{
    /// <summary>Every raw hypertable's current interval, joined to its ingest rate (tables with no compressed
    /// chunk yet drop out of the JOIN, matching the planner's "gets no decision" contract) and to when it last
    /// moved under this rule, from <see cref="RungHistoryInsertSql"/>'s own table.
    ///
    /// <para><c>chunk_compression_stats()</c> has no time-range column of its own, so the rate is the
    /// compressed chunks' total <c>before_compression_total_bytes</c> — heap AND index together, never the
    /// 2.29-only <c>hypertable_compression_stats</c> catalog columns, which are not present on every supported
    /// TimescaleDB version — divided by those SAME chunks' total wall-clock span from
    /// <c>timescaledb_information.chunks</c>, joined back on <c>(chunk_schema, chunk_name)</c>. A weighted
    /// average over every recent compressed chunk, not a single-chunk reading, so one short or long chunk near
    /// a boundary cannot alone swing the rate.</para></summary>
    internal const string TableInputsSql = @"
WITH intervals AS (
    SELECT
        d.hypertable_name,
        (EXTRACT(EPOCH FROM d.time_interval) / 3600.0)::integer AS current_interval_hours
    FROM timescaledb_information.dimensions d
    WHERE d.hypertable_schema = 'collect' AND d.dimension_type = 'Time'
),
rates AS (
    SELECT
        h.hypertable_name,
        SUM(ccs.before_compression_total_bytes)::double precision
            / (SUM(EXTRACT(EPOCH FROM (c.range_end - c.range_start))) / 3600.0) AS ingest_bytes_per_hour
    FROM timescaledb_information.hypertables h
    JOIN LATERAL chunk_compression_stats(format('%I.%I', h.hypertable_schema, h.hypertable_name)::regclass) ccs
      ON ccs.before_compression_total_bytes IS NOT NULL
    JOIN timescaledb_information.chunks c
      ON  c.hypertable_schema = h.hypertable_schema
      AND c.hypertable_name   = h.hypertable_name
      AND c.chunk_schema      = ccs.chunk_schema
      AND c.chunk_name        = ccs.chunk_name
    WHERE h.hypertable_schema = 'collect'
    GROUP BY h.hypertable_name
    HAVING SUM(EXTRACT(EPOCH FROM (c.range_end - c.range_start))) > 0
),
last_changed AS (
    SELECT table_name, MAX(changed_at) AS last_changed_at
    FROM collect.raw_chunk_interval_rung_history
    GROUP BY table_name
)
SELECT
    i.hypertable_name,
    r.ingest_bytes_per_hour,
    i.current_interval_hours,
    lc.last_changed_at
FROM intervals i
JOIN rates r ON r.hypertable_name = i.hypertable_name
LEFT JOIN last_changed lc ON lc.table_name = i.hypertable_name
ORDER BY i.hypertable_name";

    /// <summary>The store-wide chunk count the cap in <see cref="RawChunkIntervalPlanner.ChunkCountCapThreshold"/>
    /// checks — every hypertable <c>timescaledb_information.hypertables</c> enumerates, the same view (and the
    /// same "every hypertable" reach) <c>StoreSelfMetrics.HypertableInsertSql</c> already sweeps hourly.</summary>
    internal const string TotalChunkCountSql = "SELECT COALESCE(SUM(num_chunks), 0)::bigint FROM timescaledb_information.hypertables";

    /// <summary><c>set_chunk_time_interval</c> affects chunks created AFTER the call only (measured, see
    /// <see cref="TimescaleSupport.SetMaterializationChunkIntervalSql"/>) — existing chunks keep their range.
    /// $1 the bare table name (from the catalog read above, never user input), $2 the target hours.</summary>
    internal const string SetChunkTimeIntervalSql =
        "SELECT set_chunk_time_interval(format('%I.%I', 'collect', $1)::regclass, make_interval(hours => $2))";

    /// <summary>One rung-change row (#4211 ruling decision 4's history obligation): table, when, from/to hours,
    /// the reason text <see cref="RawChunkIntervalPlanner.Decision"/> already built, and the three inputs the
    /// decision was made from. <c>store_total_bytes</c> is the store-wide open-chunk total as of the START of
    /// this run, before any of the run's own changes — the number every decision this run was made against,
    /// not a moving figure re-read per row.</summary>
    internal const string RungHistoryInsertSql = @"
INSERT INTO collect.raw_chunk_interval_rung_history
    (changed_at, table_name, from_interval_hours, to_interval_hours, reason, ingest_bytes_per_hour, budget_bytes, store_total_bytes)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

    /// <summary>One row per reconcile run — whether or not it changed anything — carrying
    /// <c>pg_stat_wal.wal_bytes</c> (#4211 ruling decision 8, review finding L4: stage 1 records WAL volume so a
    /// later stage can correlate WAL growth against interval moves). <c>pg_stat_wal</c> has exactly one row.</summary>
    internal const string ReconcileRunInsertSql = @"
INSERT INTO collect.raw_chunk_interval_reconcile_runs (run_at, wal_bytes)
SELECT $1, wal_bytes FROM pg_stat_wal";

    /// <summary>
    /// Runs one reconcile pass: gathers every raw hypertable's ingest rate, current interval and last-moved
    /// stamp; calls <see cref="RawChunkIntervalPlanner.Plan"/>; applies every changing decision; records the
    /// rung history and the WAL-bytes run row; logs. Returns the number of tables changed, for the caller and
    /// for tests. Never throws for a planning or apply failure on one table — the CALLER (the daily tick)
    /// decides whether a hard failure here should stop anything else; this method's own contract is "do
    /// everything the inputs allow, report what happened."
    /// </summary>
    public static async Task<int> ReconcileAsync(
        NpgsqlConnection connection,
        double budgetBytes,
        DateTime asOfUtc,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        /* Naive UTC by the product-wide cross-store contract (StoreSelfMetrics.SweepAsync's own comment). */
        var stamp = DateTime.SpecifyKind(asOfUtc, DateTimeKind.Unspecified);

        var tables = new List<RawChunkIntervalPlanner.TableInput>();
        await using (var read = new NpgsqlCommand(TableInputsSql, connection) { CommandTimeout = 60 })
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                tables.Add(new RawChunkIntervalPlanner.TableInput(
                    TableName: reader.GetString(0),
                    IngestBytesPerHour: reader.GetDouble(1),
                    CurrentIntervalHours: reader.GetInt32(2),
                    IntervalLastChangedUtc: reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc)));
            }
        }

        long totalChunkCount;
        await using (var count = new NpgsqlCommand(TotalChunkCountSql, connection) { CommandTimeout = 60 })
        {
            totalChunkCount = (long)(await count.ExecuteScalarAsync(cancellationToken))!;
        }

        /* The pre-run store-wide open-chunk total every decision below is made against — see
           RungHistoryInsertSql's remarks for why this is read once, not per row. */
        var storeTotalBytes = 0.0;
        foreach (var table in tables)
        {
            storeTotalBytes += table.IngestBytesPerHour * table.CurrentIntervalHours;
        }

        var decisions = RawChunkIntervalPlanner.Plan(tables, budgetBytes, totalChunkCount, asOfUtc);

        var changed = 0;
        foreach (var decision in decisions)
        {
            if (!decision.Changes)
            {
                continue;
            }

            await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

            await using (var apply = new NpgsqlCommand(SetChunkTimeIntervalSql, connection, transaction) { CommandTimeout = 60 })
            {
                apply.Parameters.AddWithValue(decision.TableName);
                apply.Parameters.AddWithValue(decision.TargetIntervalHours);
                await apply.ExecuteNonQueryAsync(cancellationToken);
            }

            var ingestBytesPerHour = 0.0;
            foreach (var table in tables)
            {
                if (string.Equals(table.TableName, decision.TableName, StringComparison.Ordinal))
                {
                    ingestBytesPerHour = table.IngestBytesPerHour;
                    break;
                }
            }

            await using (var history = new NpgsqlCommand(RungHistoryInsertSql, connection, transaction) { CommandTimeout = 60 })
            {
                history.Parameters.AddWithValue(stamp);
                history.Parameters.AddWithValue(decision.TableName);
                history.Parameters.AddWithValue(decision.CurrentIntervalHours);
                history.Parameters.AddWithValue(decision.TargetIntervalHours);
                history.Parameters.AddWithValue(decision.Reason);
                history.Parameters.AddWithValue(ingestBytesPerHour);
                history.Parameters.AddWithValue(budgetBytes);
                history.Parameters.AddWithValue(storeTotalBytes);
                await history.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);

            changed++;
            logger?.LogInformation(
                "raw chunk interval: {Table} {From}h -> {To}h ({Reason}); ingest {IngestBytesPerHour:N0} B/h, budget {Budget:N0} B, store total {StoreTotal:N0} B",
                decision.TableName, decision.CurrentIntervalHours, decision.TargetIntervalHours, decision.Reason,
                ingestBytesPerHour, budgetBytes, storeTotalBytes);
        }

        await using (var run = new NpgsqlCommand(ReconcileRunInsertSql, connection) { CommandTimeout = 60 })
        {
            run.Parameters.AddWithValue(stamp);
            await run.ExecuteNonQueryAsync(cancellationToken);
        }

        if (changed == 0)
        {
            logger?.LogDebug(
                "raw chunk interval reconcile: no changes ({Tables} table(s) evaluated, store total {StoreTotal:N0} B, budget {Budget:N0} B)",
                tables.Count, storeTotalBytes, budgetBytes);
        }

        return changed;
    }
}
