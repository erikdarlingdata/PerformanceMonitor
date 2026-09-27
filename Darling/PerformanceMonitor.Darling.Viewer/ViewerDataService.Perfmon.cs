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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One point on a selected perfmon counter's trend line: the raw counter value and the per-interval
/// delta, both summed across the counter's instances at each collection_time, plus the wall-clock
/// seconds that delta covers — MAX, not SUM, because the interval is one measured sweep gap repeated
/// per instance rather than a per-instance quantity (#2234; Transactions/sec carries a median of 12
/// instance rows). The picker plots <see cref="DeltaValue"/> THROUGH the interval (#3653 A7, via the shared
/// <see cref="PerformanceMonitor.Common.DeltaSeriesShaping"/>): a rate counter's delta divided by it, an
/// unknowable point (interval 0) as a line break — so the field is the denominator, not a passenger. It is
/// nullable under the three-state rule (#3540): <c>0</c> is the calculator's "no delta knowable" marker,
/// <c>null</c> a row that never stored one, <c>n</c> the measured sweep gap; the reader used to coerce NULL
/// to 0 and so could not tell a restart from a pre-column row. Copied from Lite's <c>PerfmonTrendPoint</c>
/// (LocalDataService.Perfmon.cs).
/// <para>Since V132 (#3653 A7) the row also carries <see cref="CntrType"/> — the counter's stored DMV type
/// when every instance row summed into the point agrees on one, else null (a pre-rung row, or the
/// wait-statistics family whose instances mix a rate, a gauge and an average, whose SUM was never one
/// quantity) — which the chart classifies by, the #3702 name proxy only for a null. <see cref="DeltaValue"/>
/// is nullable for the same rung: a gauge's instance rows store no delta, so their SUM is NULL and must not
/// read as 0; <see cref="Value"/> is the gauge's reading and what a Level series plots.</para>
/// </summary>
public sealed record PerfmonTrendPoint(
    DateTime CollectionTime,
    long Value,
    long? DeltaValue,
    long? SampleIntervalSeconds,
    int? CntrType = null,
    long ArtifactsSetAside = 0);

public sealed partial class ViewerDataService
{
    /// <summary>#4234: TTL memoization for <see cref="GetDistinctPerfmonCountersAsync"/> — see its remarks.</summary>
    private readonly ViewerNameListCache _distinctPerfmonCountersCache = new();

    /// <summary>
    /// The perfmon picker's population read — Lite's <c>GetDistinctPerfmonCountersAsync</c> ported to
    /// Postgres verbatim (runs against the <c>v_perfmon_stats</c> passthrough view, the Darling-analysis
    /// convention): every counter_name collected for the server over the window, ordered by name so the
    /// picker + pack fills see a stable list.
    /// $1 server_id, $2 window start, $3 window end (all naive UTC).
    /// </summary>
    public const string DistinctPerfmonCountersSql = """
        SELECT DISTINCT counter_name
        FROM v_perfmon_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY counter_name
        """;

    /// <summary>
    /// Lite's batched <c>GetPerfmonTrendsByCountersAsync</c> body, ported to Postgres: the trend for
    /// ALL selected counters in ONE query, grouped by counter name and collection_time. The
    /// <c>counter_name IN (...)</c> list is built dynamically from <c>$4</c> onward
    /// (server_id/start/end take <c>$1</c>/<c>$2</c>/<c>$3</c>), exactly like Lite's numbering.
    /// Deviation from Lite: the two <c>SUM</c> aggregates are wrapped in <c>CAST(... AS bigint)</c>
    /// because Postgres' <c>SUM(bigint)</c> returns <c>numeric</c> (DuckDB kept it integral), and the
    /// typed <c>GetInt64</c> reader would throw against a numeric — the same typed-reader reason the
    /// tempdb/CPU trends CAST their aggregates. A caller passing <paramref name="counterCount"/> = 0 is
    /// a bug the <see cref="GetPerfmonTrendsByCountersAsync"/> guard prevents.
    /// <para>The type column is <c>CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END</c>
    /// (V132): a type only where the instance rows summed into the point agree on one. A pre-rung row has
    /// NULL and so does a mixed-type family, and both fall to the name proxy in the chart — the MIN/MAX
    /// equality is what stops a mixed sum (the wait-statistics object's instances are a rate, a gauge and
    /// an average under one counter name) from being classified by whichever instance's id happened to
    /// sort first. Byte-identical to Lite's expression.</para>
    /// <para>
    /// #4234: the per-collection SUM above, BUCKETED — the per-collection read this replaced kept every row,
    /// up to 102,012 for 12 counters over 7 days (the issue's measured number). Wraps the unchanged
    /// per-collection statement as a subquery and re-aggregates into <c>date_bin</c> buckets, generalizing
    /// <c>DarlingTrendReader.PerfmonTrendBucketedSql</c> (the MCP <c>get_perfmon_trend</c> twin, #3960 — one
    /// counter at a time) to <paramref name="counterCount"/> counters in one query. <see cref="PerfmonTrendPoint"/>'s
    /// four fields keep their exact per-collection MEANING so <see cref="PerformanceMonitor.Common.DeltaSeriesShaping"/>
    /// downstream needs no change: <c>Value</c> is the bucket's rounded GAUGE average (the ruling's rule for a
    /// gauge); <c>DeltaValue</c>/<c>SampleIntervalSeconds</c> are summed only over rated collections (stored
    /// interval &gt; 0), so <c>DeltaValue / SampleIntervalSeconds</c> downstream is exactly the ruling's rate —
    /// summed deltas over summed intervals — and both are NULL together for a bucket with no rated collection,
    /// which <c>DeltaSeriesShaping.Shape</c> already reads as a line break, the same as an unrated per-collection
    /// row always did; <c>CntrType</c> keeps the per-collection MIN=MAX-agreement rule, now double-aggregated the
    /// same way the MCP twin re-aggregates its own per-collection subquery. Neither perfmon chart plots a peak
    /// value, so unlike the MCP twin there is no peak column. The width is its own trailing parameter, after the
    /// dynamic <c>counter_name IN (...)</c> list, so that list's existing numbering does not shift.
    /// </para>
    /// <para>#4234 review (item 3): <c>first_collection_time</c> (<c>MIN(collection_time)</c> over the
    /// per-collection subquery, mirroring <c>DurationTrendRouting.BuildBucketedRawTrendSql</c>'s column of the
    /// same name) and <c>collection_count</c> (<c>COUNT(*)</c> over that same subquery) ride along so the
    /// caller can tell a true singleton bucket from one the bucketing merged.
    /// <c>GetPerfmonTrendsByCountersAsync</c> uses <c>collection_count</c> to decide.</para>
    /// <para>#4476: a raw-row layer sits under the per-collection SUM, carrying <c>lag</c>/<c>lead</c> of
    /// <c>cntr_value</c> per instance (<c>PARTITION BY object_name, counter_name, instance_name ORDER BY
    /// collection_time</c> — this read covers several counter names in one query, unlike the MCP twin's
    /// single-counter partition, so <c>counter_name</c> joins the window) so
    /// <see cref="WaitStatisticsArtifact.ArtifactPredicateSql"/> can tell an isolated single-sample artifact
    /// (a <c>SQLServer:Wait Statistics</c> gauge instance whose one collection reads its lifetime cumulative
    /// count, #4476) apart from a real value BEFORE the instances are summed into a per-collection point — a
    /// C#-side check on the already-summed point can never see the one spiking instance. Every artifact
    /// instance-row is excluded from the per-collection <c>cntr_value</c>/<c>delta_cntr_value</c> SUMs via
    /// <c>FILTER (WHERE NOT is_artifact)</c>, while the MAX interval and the MIN=MAX type rule keep reading
    /// EVERY row (an artifact row's own type and interval are not themselves suspect). The per-collection
    /// layer counts how many of its rows were set aside (<c>COUNT(*) FILTER (WHERE is_artifact)</c>), and the
    /// outer bucket layer SUMs that count into <c>artifacts_set_aside</c>, appended LAST so ordinals 0-7 do
    /// not move. A bucket whose every row was an artifact reads <c>cntr_value IS NULL</c> with a positive
    /// <c>artifacts_set_aside</c> — <see cref="GetPerfmonTrendsByCountersAsync"/> drops that point rather than
    /// turning it into a fabricated 0.</para>
    /// <para>The raw-row layer is itself a <c>UNION ALL</c> (#4476 cost review): the lag/lead window opens only
    /// on rows <see cref="WaitStatisticsArtifact.ObjectNameSuffixMatchSql"/> lets through, with the rest of the
    /// selected counters carrying a plain <c>false AS is_artifact</c> and no window at all — windowing every
    /// row measured 12 counters over 7 days at 2.3-2.4s against ~800ms unwindowed, because the sort touched
    /// rows that could never be an artifact and pushed the planner off its index scan onto a parallel seq scan.
    /// Same shape as the MCP twin's <c>PerfmonTrendSql</c>.</para>
    /// $1 server_id, $2/$3 window (naive UTC), $4.. counter names, last $ the bucket width in minutes.
    /// </summary>
    public static string PerfmonTrendsSql(int counterCount)
    {
        var nameParams = string.Join(", ", Enumerable.Range(0, counterCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (counterCount + 4);
        var isArtifact = "COALESCE((" + WaitStatisticsArtifact.ArtifactPredicateSql(
            "cntr_type", "object_name", "prev_value", "cntr_value", "next_value") + "), false)";
        var isWaitStatistics = WaitStatisticsArtifact.ObjectNameSuffixMatchSql("object_name");
        return $$"""
            SELECT
                counter_name,
                GREATEST(date_bin(CAST({{widthParam}} AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
                CAST(ROUND(AVG(cntr_value)) AS bigint) AS cntr_value,
                SUM(delta_cntr_value) FILTER (WHERE sample_interval_seconds > 0) AS delta_cntr_value,
                SUM(sample_interval_seconds) FILTER (WHERE sample_interval_seconds > 0) AS sample_interval_seconds,
                CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count,
                SUM(artifacts) AS artifacts_set_aside
            FROM (
                SELECT
                    counter_name,
                    collection_time,
                    CAST(SUM(cntr_value) FILTER (WHERE NOT is_artifact) AS bigint) AS cntr_value,
                    CAST(SUM(delta_cntr_value) FILTER (WHERE NOT is_artifact) AS bigint) AS delta_cntr_value,
                    CAST(MAX(sample_interval_seconds) AS bigint) AS sample_interval_seconds,
                    CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type,
                    COUNT(*) FILTER (WHERE is_artifact) AS artifacts
                FROM (
                    SELECT
                        counter_name,
                        collection_time,
                        cntr_value,
                        delta_cntr_value,
                        sample_interval_seconds,
                        cntr_type,
                        false AS is_artifact
                    FROM v_perfmon_stats
                    WHERE server_id = $1
                    AND   collection_time >= $2
                    AND   collection_time <= $3
                    AND   counter_name IN ({{nameParams}})
                    AND   NOT ({{isWaitStatistics}})

                    UNION ALL

                    SELECT
                        counter_name,
                        collection_time,
                        cntr_value,
                        delta_cntr_value,
                        sample_interval_seconds,
                        cntr_type,
                        {{isArtifact}} AS is_artifact
                    FROM (
                        SELECT
                            counter_name,
                            collection_time,
                            object_name,
                            cntr_value,
                            delta_cntr_value,
                            sample_interval_seconds,
                            cntr_type,
                            lag(cntr_value) OVER w AS prev_value,
                            lead(cntr_value) OVER w AS next_value
                        FROM v_perfmon_stats
                        WHERE server_id = $1
                        AND   collection_time >= $2
                        AND   collection_time <= $3
                        AND   counter_name IN ({{nameParams}})
                        AND   {{isWaitStatistics}}
                        WINDOW w AS (PARTITION BY object_name, counter_name, instance_name ORDER BY collection_time)
                    ) AS raw
                ) AS flagged
                GROUP BY counter_name, collection_time
            ) AS collections
            GROUP BY counter_name, 2
            ORDER BY counter_name, 2
            """;
    }

    /// <summary>
    /// The distinct counter names collected for one server in the window, ordered by name — feeds the
    /// picker's population + pack fills.
    /// <para>#4234: memoized through <see cref="_distinctPerfmonCountersCache"/> — keyed on (server, window
    /// length) for <see cref="ViewerNameListCache.Ttl"/>, so the full-window DISTINCT behind this runs at most
    /// once per 15 minutes rather than on every 1-minute auto-refresh (the issue's measured 2,079 ms cold read
    /// over a 7-day window). <paramref name="nowUtc"/> is the cache's clock seam — null uses the wall clock; a
    /// test passes an explicit time to fast-forward past the TTL without sleeping.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctPerfmonCountersAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? nowUtc = null, CancellationToken cancellationToken = default)
    {
        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var windowLength = endUtc - startUtc;
        if (_distinctPerfmonCountersCache.TryGet(serverId, windowLength, endUtc, effectiveNow, out var cached))
        {
            return cached;
        }

        var items = new List<string>();

        await using var command = _dataSource.CreateCommand(DistinctPerfmonCountersSql);
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

        _distinctPerfmonCountersCache.Set(serverId, windowLength, endUtc, items, effectiveNow);
        return items;
    }

    /// <summary>
    /// The value + delta trend for every selected counter in one query, grouped by counter name.
    /// Empty selection returns an empty map without touching the store.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES — the ruling's own
    /// wording, not the MCP convention of dividing one shared budget across every line a call draws — so the
    /// pin is rows ≤ budget × series count; <c>seriesCount</c> is therefore always 1 into
    /// <see cref="TrendBuckets.AutoMinutes"/>.</para>
    /// <para>#4234 review (item 3): the same singleton-bucket rule as
    /// <see cref="GetWaitStatsTrendsByTypesAsync"/> — see its remarks. Only when EVERY bucket the whole call
    /// returned holds exactly one physical collection does the call stamp points at
    /// <c>first_collection_time</c> instead of <c>bucket_start</c>; a single merged bucket anywhere (any
    /// counter) keeps <c>bucket_start</c> throughout.</para>
    /// </summary>
    public async Task<Dictionary<string, List<PerfmonTrendPoint>>> GetPerfmonTrendsByCountersAsync(
        int serverId, List<string> counterNames, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, List<PerfmonTrendPoint>>();
        if (counterNames.Count == 0)
        {
            return result;
        }

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(PerfmonTrendsSql(counterNames.Count));
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
        foreach (var counterName in counterNames)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = counterName });
        }
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string CounterName, DateTime BucketStart, DateTime FirstCollectionTime, long? Value, long? DeltaValue, long? SampleIntervalSeconds, int? CntrType, long ArtifactsSetAside)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(7) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(6),
                /* NULL stays NULL here: a bucket whose every row was an artifact (#4476) is dropped below
                   rather than turned into a fabricated 0 — that decision needs to see the NULL. */
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                /* NULL stays NULL: a gauge's instance rows store no delta (V132), so the SUM is NULL, not 0. */
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                /* NULL stays NULL (#3540's third state); 0 is the marker and must not be manufactured from it. */
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.IsDBNull(5) ? null : reader.GetInt32(5),
                reader.GetInt64(8)));
        }

        foreach (var row in rows)
        {
            /* #4476: every instance summed into this bucket was set aside as an isolated single-sample
               artifact — there is no real value to plot, so the point is skipped rather than read as 0
               (the pre-#4476 IsDBNull(2) ? 0 : ... coercion this replaces). Every other NULL keeps that
               coercion, since a NULL cntr_value with no artifacts set aside is not this case. */
            if (row.Value is null && row.ArtifactsSetAside > 0)
            {
                continue;
            }

            if (!result.TryGetValue(row.CounterName, out var list))
            {
                list = new List<PerfmonTrendPoint>();
                result[row.CounterName] = list;
            }

            list.Add(new PerfmonTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.Value ?? 0,
                row.DeltaValue,
                row.SampleIntervalSeconds,
                row.CntrType,
                row.ArtifactsSetAside));
        }

        return result;
    }
}

/// <summary>
/// The chart-title arithmetic for <see cref="WaitStatisticsArtifact.ChartCaption"/> (#4476): SUMs
/// <see cref="PerfmonTrendPoint.ArtifactsSetAside"/> across every point of every plotted counter's trend —
/// a pure, non-WPF static so it is unit-testable without the picker's WPF chart control.
/// </summary>
public static class PerfmonChartArtifactSummary
{
    /// <summary>The total artifacts set aside across every plotted counter's points, for the counters
    /// actually plotted (<paramref name="plottedCounterNames"/>) — a counter present in
    /// <paramref name="trendsByCounter"/> but not plotted (an empty trend, #4234's guard) contributes
    /// nothing.</summary>
    public static long TotalArtifactsSetAside(
        IReadOnlyDictionary<string, List<PerfmonTrendPoint>> trendsByCounter, IEnumerable<string> plottedCounterNames)
    {
        long total = 0;
        foreach (var counterName in plottedCounterNames)
        {
            if (!trendsByCounter.TryGetValue(counterName, out var trend))
            {
                continue;
            }

            foreach (var point in trend)
            {
                total += point.ArtifactsSetAside;
            }
        }

        return total;
    }
}
