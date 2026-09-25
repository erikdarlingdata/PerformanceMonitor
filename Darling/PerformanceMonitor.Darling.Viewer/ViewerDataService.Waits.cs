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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One point on a selected wait type's trend line, with both picker metrics computed IN SQL: the
/// per-second wait rate (this interval's <c>delta_wait_time_ms</c> divided by the seconds it accrued
/// over — the row's stored <c>sample_interval_seconds</c>, or for pre-V127 rows the seconds since the
/// previous collection via a per-type <c>LAG</c> window; a row whose interval is unknowable is not a
/// point, #3540) and the average ms per wait (delta wait
/// time divided by delta waiting tasks). Signal-wait per second rides along for parity with Lite's
/// row shape though the picker's metric combo exposes only the other two. Copied from Lite's
/// <c>WaitStatsTrendPoint</c> (LocalDataService.WaitStats.cs).
/// </summary>
public sealed record WaitStatsTrendPoint(
    DateTime CollectionTime,
    double WaitTimeMsPerSecond,
    double SignalWaitTimeMsPerSecond,
    double AvgMsPerWait);

public sealed partial class ViewerDataService
{
    /// <summary>#4234: TTL memoization for <see cref="GetDistinctWaitTypesAsync"/> — see its remarks.</summary>
    private readonly ViewerNameListCache _distinctWaitTypesCache = new();

    /// <summary>
    /// The wait-type picker's population read — Lite's <c>GetDistinctWaitTypesAsync</c> ported to
    /// Postgres verbatim (runs against the <c>v_wait_stats</c> passthrough view, the Darling-analysis
    /// convention): every wait_type collected for the server over the window, ranked by total delta
    /// wait time descending so the picker sees the heaviest types first (the checked-to-top order and
    /// the "Top Waits" fill both lean on it). Lite's per-user IgnoredWaitTypes exclusion clause is
    /// deliberately DROPPED — the viewer has no per-user ignore config to share.
    /// $1 server_id, $2 window start, $3 window end (all naive UTC).
    /// </summary>
    public const string DistinctWaitTypesSql = """
        SELECT
            wait_type,
            SUM(delta_wait_time_ms) AS total_delta
        FROM v_wait_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY wait_type
        ORDER BY SUM(delta_wait_time_ms) DESC
        """;

    /// <summary>
    /// Lite's batched <c>GetWaitStatsTrendsByTypesAsync</c> body, ported to Postgres verbatim: the
    /// per-second trend for ALL selected wait types in ONE query, grouped by type. The <c>LAG</c>
    /// window is partitioned by wait_type so each type's per-second rate is computed independently,
    /// and the interval seconds come from the truncate-then-diff epoch idiom already proven
    /// value-identical between DuckDB and Postgres. The <c>wait_type IN (...)</c> list is built
    /// dynamically from <c>$4</c> onward (server_id/start/end take <c>$1</c>/<c>$2</c>/<c>$3</c>),
    /// exactly like Lite; a caller passing <paramref name="waitTypeCount"/> = 0 is a bug the
    /// <see cref="GetWaitStatsTrendsByTypesAsync"/> guard prevents.
    /// </summary>
    /// <summary>
    /// #4234: BUCKETED (the per-collection read this replaced kept every row, up to 170,020 for 20 wait types
    /// over 7 days — the issue's measured number). Same raw CTE, same three-state interval and "unrated
    /// collection contributes to neither sum" rule as before; new is the <c>rated</c> CTE and the final
    /// GROUP BY on <c>date_bin</c>, generalizing <c>DarlingDataReader.WaitTrendBucketedSql</c> (the MCP
    /// <c>get_wait_trend</c> twin, #3960 — one wait type at a time) to <paramref name="waitTypeCount"/> types
    /// in one query, the shape this read has always used. A bucket's rate is its summed wait over its summed
    /// rated seconds — time-weighted, never an average of per-collection rates — and a bucket with no rated
    /// collection is dropped (<c>HAVING</c>), same as the per-collection read always dropped that collection.
    /// <c>avg_ms_per_wait</c> has no MCP twin to mirror (that tool publishes only the two per-second rates);
    /// it follows the same summed-numerator-over-summed-denominator rule, guarded against a bucket whose
    /// rated collections all logged zero waiting tasks — Postgres raises <c>division_by_zero</c> there rather
    /// than returning NULL, so the guard is explicit, matching the per-collection read's own "0, not NULL"
    /// answer for that case. The width is appended as its OWN trailing parameter, after the dynamic
    /// <c>wait_type IN (...)</c> list, so that list's existing <c>$4..</c> numbering does not shift. Neither
    /// wait-stats chart series plots a peak, so unlike the MCP twin there is no peak column.
    /// <para>#4234 review (item 3): <c>first_collection_time</c> (<c>MIN(collection_time)</c>, every row in
    /// <c>rated</c> — rated or not, mirroring <c>DurationTrendRouting.BuildBucketedRawTrendSql</c>'s own
    /// column of the same name) and <c>collection_count</c> (<c>COUNT(*)</c> over that same population) ride
    /// along so the caller can tell a true singleton bucket — one physical collection, rated or not, landed
    /// in it — from one the bucketing actually merged. <c>GetWaitStatsTrendsByTypesAsync</c> uses
    /// <c>collection_count</c> to decide.</para>
    /// $1 server_id, $2/$3 window (naive UTC), $4.. wait types, last $ the bucket width in minutes.
    /// </summary>
    public static string WaitTrendsSql(int waitTypeCount)
    {
        var typeParams = string.Join(", ", Enumerable.Range(0, waitTypeCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (waitTypeCount + 4);
        return $$"""
            WITH raw AS
            (
                SELECT
                    wait_type,
                    collection_time,
                    delta_wait_time_ms,
                    delta_signal_wait_time_ms,
                    delta_waiting_tasks,
                    /* #3540: the STORED interval where the row has one. 0 is the calculator's "no delta
                       knowable" marker and becomes NULL through NULLIF, so the rates are NULL and the reader
                       drops the row — a missing sample, never the confident 0.00 ms/sec a restart used to
                       render. NULL (a pre-V127 row) falls back to the LAG this read always used. */
                    CASE WHEN sample_interval_seconds IS NULL
                         THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time))))
                         ELSE NULLIF(sample_interval_seconds, 0)
                    END AS interval_seconds
                FROM v_wait_stats
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3
                AND   wait_type IN ({{typeParams}})
            ),
            rated AS
            (
                SELECT
                    wait_type,
                    collection_time,
                    CASE WHEN interval_seconds > 0 THEN delta_wait_time_ms END AS rated_wait_ms,
                    CASE WHEN interval_seconds > 0 THEN delta_signal_wait_time_ms END AS rated_signal_ms,
                    CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds,
                    CASE WHEN interval_seconds > 0 THEN delta_waiting_tasks END AS rated_tasks
                FROM raw
            )
            SELECT
                wait_type,
                GREATEST(date_bin(CAST({{widthParam}} AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
                CAST(SUM(rated_wait_ms) AS DOUBLE PRECISION) / SUM(rated_seconds) AS wait_time_ms_per_second,
                CAST(SUM(rated_signal_ms) AS DOUBLE PRECISION) / SUM(rated_seconds) AS signal_wait_time_ms_per_second,
                CASE WHEN SUM(rated_tasks) > 0 THEN CAST(SUM(rated_wait_ms) AS DOUBLE PRECISION) / SUM(rated_tasks) ELSE 0 END AS avg_ms_per_wait,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count
            FROM rated
            GROUP BY wait_type, 2
            HAVING COUNT(rated_seconds) > 0
            ORDER BY wait_type, 2
            """;
    }

    /// <summary>
    /// The distinct wait types collected for one server in the window, ranked by total delta wait
    /// time descending — feeds the picker's population + default-selection.
    /// <para>#4234: memoized through <see cref="_distinctWaitTypesCache"/> — keyed on (server, window length)
    /// for <see cref="ViewerNameListCache.Ttl"/>, so the full-window DISTINCT behind this runs at most once
    /// per 15 minutes rather than on every 1-minute auto-refresh (measured 2,079 ms cold on the Perfmon twin
    /// of this read over 7 days). <paramref name="nowUtc"/> is the cache's clock seam — null uses the wall
    /// clock; a test passes an explicit time to fast-forward past the TTL without sleeping.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctWaitTypesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? nowUtc = null, CancellationToken cancellationToken = default)
    {
        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var windowLength = endUtc - startUtc;
        if (_distinctWaitTypesCache.TryGet(serverId, windowLength, endUtc, effectiveNow, out var cached))
        {
            return cached;
        }

        var items = new List<string>();

        await using var command = _dataSource.CreateCommand(DistinctWaitTypesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        _distinctWaitTypesCache.Set(serverId, windowLength, endUtc, items, effectiveNow);
        return items;
    }

    /// <summary>
    /// The per-second (and avg-per-wait) trend for every selected wait type in one query, grouped by
    /// type. Empty selection returns an empty map without touching the store.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES — the ruling's own
    /// wording, not the MCP convention of dividing one shared budget across every line a call draws — so the
    /// pin is rows ≤ budget × series count; <c>seriesCount</c> is therefore always 1 into
    /// <see cref="TrendBuckets.AutoMinutes"/>.</para>
    /// <para>#4234 review (item 3): "when the budget covers every collection in the window, the chart gets
    /// the raw points unchanged" — including the TIMESTAMP, not only the value. A bucket's SUM/AVG over its
    /// one collection already equals that collection's own value at any width, but <c>bucket_start</c> is a
    /// <c>date_bin</c> grid line, not a collection the store held, so stamping every point there (as the
    /// always-bucketed MCP twins do at every width) would still change a raw point's timestamp even when
    /// nothing was merged. This buffers every row's <c>collection_count</c> from <see cref="WaitTrendsSql"/>
    /// and, only when EVERY bucket the whole call returned holds exactly one physical collection — the
    /// literal reading of "every collection in the window" — stamps each point at its bucket's
    /// <c>first_collection_time</c> instead of <c>bucket_start</c>, which for a singleton bucket is that one
    /// collection's own raw time. A single merged bucket anywhere in the call (any wait type, any bucket)
    /// keeps <c>bucket_start</c> throughout, matching what the MCP twins always serve, rather than a chart
    /// with some points on the grid and others off it.</para>
    /// </summary>
    public async Task<Dictionary<string, List<WaitStatsTrendPoint>>> GetWaitStatsTrendsByTypesAsync(
        int serverId, List<string> waitTypes, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, List<WaitStatsTrendPoint>>();
        if (waitTypes.Count == 0)
        {
            return result;
        }

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(WaitTrendsSql(waitTypes.Count));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        foreach (var waitType in waitTypes)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = waitType });
        }
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string WaitType, DateTime BucketStart, DateTime FirstCollectionTime, double Rate, double Signal, double Avg)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is an unknowable interval (#3540): the row is dropped, not read as 0. */
            if (reader.IsDBNull(2))
            {
                continue;
            }

            if (reader.GetInt64(6) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(5),
                reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
        }

        foreach (var row in rows)
        {
            if (!result.TryGetValue(row.WaitType, out var list))
            {
                list = new List<WaitStatsTrendPoint>();
                result[row.WaitType] = list;
            }

            list.Add(new WaitStatsTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.Rate,
                row.Signal,
                row.Avg));
        }

        return result;
    }
}
