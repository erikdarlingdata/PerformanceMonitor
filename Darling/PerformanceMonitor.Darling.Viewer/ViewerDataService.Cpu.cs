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
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One bucketed CPU point: a sample time plus the SQL Server and other-process CPU percentages, each
/// averaged over the bucket (#4234). This read feeds BOTH the CPU tab's chart (SQL Server vs other
/// processes) and — since W1d — the Overview's CPU lane (SQL vs SQL+other Total), mirroring Lite's CPU tab
/// (<c>ServerTab.Charts.cs</c> <c>UpdateCpuChart</c>) and Lite's Overview lanes, both over
/// <c>LocalDataService.GetCpuUtilizationAsync</c>. Both CPU columns are <c>integer</c> in the store; a bucket
/// wider than one sample averages them, so both fields are <c>double</c> here (a singleton bucket's average
/// is that one sample's own integer value, unchanged).
/// <c>SampleTime</c> is naive UTC: the store's <c>sample_time_utc</c> where the row carries one (V134, #3653
/// item 13), else the server-LOCAL <c>sample_time</c> de-skewed to naive UTC by the read
/// (see <see cref="CpuUtilizationSql"/>, #1262), so both consumers plot it through
/// <see cref="ViewerTimeHelper.ForDisplay"/> like every other Darling series.
/// </summary>
public sealed record CpuUtilizationSample(DateTime SampleTime, double SqlServerCpu, double OtherProcessCpu);

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The raw per-sample CPU read: every ring-buffer sample since <paramref name="sinceUtc"/> (not an
    /// average-per-collection roll-up), feeding both the CPU tab's scatter chart and — since W1d — the
    /// Overview's CPU lane. The window filters on <c>collection_time</c> (the naive-UTC collection
    /// prefix — the reliable clock every Darling read windows on); a NULL
    /// <c>other_process_cpu_utilization</c> (SQL on Linux, #1048) reads as 0.
    ///
    /// <para><b>sample_time de-skew (#1262).</b> Unlike every other stored column, <c>sample_time</c> is
    /// the MONITORED SERVER'S LOCAL wall clock (<c>SYSDATETIME()</c> on the server, minus each
    /// ring-buffer sample's age), NOT naive UTC — so feeding it straight to <see cref="ViewerTimeHelper.ForDisplay"/>
    /// (which assumes naive-UTC input) shifts the whole CPU series by the server's UTC offset relative
    /// to every <c>collection_time</c>-based lane — a visible misalignment in the correlated Overview
    /// (e.g. a Pacific-time server, offset -7/-8h, sits 7-8h off).
    /// The viewer has no per-server timezone config the way Lite does
    /// (<c>ServerTimeHelper.UtcOffsetMinutes</c>), so we recover the offset from the batch itself: all
    /// rows of one collection share a single <c>collection_time</c> (the batch key — <c>collection_id</c>
    /// is assigned per row, a distinct value per sample, NOT a batch id), and the NEWEST sample in a
    /// batch is at most ~1 minute older than that collection instant. So
    /// <c>MAX(sample_time) OVER (batch) - collection_time</c> is
    /// the server's UTC offset plus that sub-minute anchor age; rounding to 15 minutes recovers the exact
    /// whole/half/quarter-hour offset and absorbs the anchor age (and any few-second clock skew).
    /// Subtracting that per-batch offset turns each local <c>sample_time</c> into true naive UTC, so
    /// <see cref="ViewerTimeHelper.ForDisplay"/> then aligns the CPU series with every other lane. A UTC server yields
    /// offset 0 — byte-identical to the pre-fix read. The per-batch derivation handles a DST transition
    /// BETWEEN batches (each batch recovers its own offset) but not one INSIDE a batch: the poll that
    /// straddles the change rounds to one side and places the samples on the other side an hour wrong,
    /// silently. Reads the base <c>cpu_utilization_stats</c> table. $1 server_id, $2 window start (naive UTC).</para>
    ///
    /// <para><b>The stored UTC instant is preferred (V134, #3653 item 13, Q7).</b> Since that rung the
    /// collector writes <c>sample_time_utc</c> — the same instant in UTC, off <c>SYSUTCDATETIME()</c> — beside
    /// the unchanged local <c>sample_time</c>, so a post-rung row needs no derivation at all and is placed
    /// exactly, DST or not. <c>COALESCE(sample_time_utc, &lt;the #1262 de-skew&gt;)</c> takes it where the row
    /// carries one and falls back to the derivation for every pre-rung row, which therefore renders exactly
    /// as it did (nothing is backfilled: the offset a row's server had at its sample instant is what the store
    /// never recorded). The window predicate stays on <c>collection_time</c>, the collector prefix, which was
    /// always naive UTC and is the hypertable's partition column. The output alias stays <c>sample_time</c>
    /// so <c>ORDER BY sample_time</c> orders on the projected UTC value (Postgres resolves a bare ORDER BY
    /// name to the output column first) and the reader's ordinal is unchanged.</para>
    ///
    /// <para><b>#4234: BUCKETED.</b> The de-skewed <c>sample_time</c> above is now the <c>raw</c> CTE this read
    /// starts from; the outer query buckets it to <see cref="TrendBuckets.AutoMinutes"/>'s width (up to ~60
    /// ring-buffer samples per collection, unbounded over a wide window — the #4234 issue's own measured
    /// number, 10,080 rows over 7 days with no cap). Both CPU columns are gauges (ruling item 2): a NULL
    /// <c>other_process_cpu_utilization</c> (SQL on Linux, #1048) is COALESCEd to 0 before averaging, matching
    /// the reader's old per-row "NULL reads as 0" rule exactly rather than letting Postgres's NULL-skipping
    /// AVG silently change what a bucket with a mix of Windows/Linux-shaped rows would show. Neither consumer
    /// draws a peak line (the CPU tab's own chart and the Overview lane both plot a plain SQL/Total pair), so
    /// there is no MAX column. <c>first_sample_time</c>/<c>sample_count</c> ride along, unused in the SQL
    /// itself, so the C# reader can stamp a bucket that merged nothing at its one sample's own raw time
    /// (ruling item 3) exactly as <see cref="GetTotalWaitTrendAsync"/> does. $3 the bucket width in minutes.</para>
    /// </summary>
    public static readonly string CpuUtilizationSql = $"""
        WITH raw AS
        (
            SELECT
                /* Prefer the stored UTC instant (V134, #3653 item 13); fall back to de-skewing sample_time (the
                   monitored server's LOCAL wall clock) to naive UTC by subtracting the per-batch UTC offset =
                   round(MAX(sample_time) over the batch - collection_time) to the nearest 15 minutes.
                   collection_time (shared by every row of a collection) is the batch key; collection_id is
                   assigned per row (a distinct value per sample). A UTC server yields offset 0. See the C#
                   summary above for the full derivation (#1262) and for why the fallback stays. */
                COALESCE(
                    sample_time_utc,
                    sample_time
                        - INTERVAL '15 minutes'
                          * ROUND(EXTRACT(EPOCH FROM (
                                MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time
                            )) / 900.0)::double precision) AS sample_time,
                sqlserver_cpu_utilization,
                other_process_cpu_utilization
            FROM cpu_utilization_stats
            WHERE server_id = $1
            AND   collection_time >= $2
        )
        SELECT
            GREATEST(date_bin(CAST($3 AS integer) * INTERVAL '1 minute', sample_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(COALESCE(sqlserver_cpu_utilization, 0)) AS sqlserver_cpu_utilization,
            /* NULL other-process CPU (SQL on Linux) reads as 0 before averaging, like Lite's CPU chart reads it per row. */
            AVG(COALESCE(other_process_cpu_utilization, 0)) AS other_process_cpu_utilization,
            MIN(sample_time) AS first_sample_time,
            COUNT(*) AS sample_count
        FROM raw
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>
    /// Bucketed CPU samples for one server since <paramref name="sinceUtc"/>, time-ordered, feeding both the
    /// CPU tab's chart and the Overview's CPU lane (#4234). <paramref name="endUtc"/> sizes the bucket width
    /// only — the SQL stays start-only server-side (a "now"-anchored window must not clip the freshest sample
    /// behind a stale computed end; a custom range's caller already bounds the end client-side, unchanged) —
    /// and defaults to the wall clock when omitted. Each point is stamped at its bucket's start, UNLESS every
    /// bucket this call returned holds exactly one physical sample, in which case every point is stamped at
    /// its own raw <c>first_sample_time</c> instead (ruling item 3), matching
    /// <see cref="GetTotalWaitTrendAsync"/>'s rule.
    /// </summary>
    public async Task<List<CpuUtilizationSample>> GetCpuUtilizationAsync(
        int serverId, DateTime sinceUtc, DateTime? endUtc = null, CancellationToken cancellationToken = default)
    {
        var effectiveEnd = endUtc ?? DateTime.UtcNow;
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((effectiveEnd - sinceUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(CpuUtilizationSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstSampleTime, double SqlCpu, double OtherCpu)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(4) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(3),
                reader.GetDouble(1),
                reader.GetDouble(2)));
        }

        var samples = new List<CpuUtilizationSample>(rows.Count);
        foreach (var row in rows)
        {
            samples.Add(new CpuUtilizationSample(
                everyBucketSingleton ? row.FirstSampleTime : row.BucketStart,
                row.SqlCpu,
                row.OtherCpu));
        }

        return samples;
    }
}
