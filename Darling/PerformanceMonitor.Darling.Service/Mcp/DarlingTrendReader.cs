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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the windowed-trend MCP tools (<see cref="DarlingMcpTrendTools"/>) — the SAME
/// collected time-series the viewer's chart partials read (<c>ViewerDataService.OverviewLanes</c> /
/// <c>.Perfmon</c> / <c>.FileIo</c> / <c>.QueryStats</c> / <c>.QueryTrends</c>), adapted here so the MCP
/// host never references the WPF viewer project. Each read is a STORED read (no live monitored-server hit),
/// keyed by <c>server_id</c> and windowed BOTH-sides on the naive-UTC <c>collection_time</c> prefix — the
/// reliable clock every Darling read windows on, bound <c>Kind=Unspecified</c> because the store's columns
/// are <c>timestamp without time zone</c> (a <c>Kind=Utc</c> DateTime maps to timestamptz and throws since
/// Npgsql 6.0).
///
/// <para>
/// The result shapes mirror LITE's <c>Mcp*Tools</c> trend tools field-for-field (get_memory_trend /
/// get_perfmon_trend / get_file_io_trend / get_query_trend / get_query_duration_trend), the store-faithful
/// shape Darling's collector-mirror schema serves — the same rule the merged <see cref="DarlingDataReader"/>
/// follows (Lite's get_query_duration_trend has no Dashboard twin; the other four names + params match the
/// Dashboard, the shape follows Lite where the SKUs diverge). Every RAW-tier SQL string is byte-identical to
/// the viewer's proven Postgres read for that chart (the viewer already ported Lite's DuckDB SQL), so Darling
/// serves one consistent product; the hourly-tier twins the retention-routed reads fall to (#2353, #3541 A2)
/// are since #3666/#3653 the Storage builder's own text (<see cref="DurationTrendRouting"/>), which the
/// viewer's Performance Trends tab runs with its database filter added — aliased here, not copied. Each SQL
/// string is a public const (or, for the two aliased builder outputs, a public static readonly) so
/// Darling.Tests can pin the dialect + columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingTrendReader
{
    /* ─────────────────────────── result records ─────────────────────────── */

    /// <summary>One memory-trend point: the four MB metrics per collection (Lite's <c>MemoryTrendPoint</c>
    /// minus its <c>TotalGrantedMb</c> overlay field, which the memory_stats source never fills — the tool
    /// joins it per point from the grants series (<see cref="MemoryGrantTrendPoint"/>), null with a note
    /// naming get_memory_grants where no snapshot aligns (#3529, #3548) — see
    /// <see cref="DarlingMcpTrendTools.GetMemoryTrend"/>).</summary>
    public sealed record MemoryTrendPoint(
        DateTime CollectionTime, double TotalServerMemoryMb, double TargetServerMemoryMb,
        double BufferPoolMb, double PlanCacheMb);

    /// <summary>One memory-grant-trend point: total granted workspace memory summed across every resource
    /// pool at one grants collection (#3548) — the series <see cref="DarlingMcpTrendTools.GetMemoryTrend"/>
    /// joins onto the memory trend, and the same series the viewer's Memory Overview overlay plots. Its
    /// collection_times are the grants collector's OWN stamps, seconds apart from the memory series' even
    /// in the same cycle, which is why the join is nearest-match rather than equality.</summary>
    public sealed record MemoryGrantTrendPoint(DateTime CollectionTime, double TotalGrantedMb);

    /// <summary>One perfmon-trend point for a single counter: the counter value, the per-interval delta,
    /// and the wall-clock seconds that delta covers, all summed across the counter's instances at that
    /// collection (Lite's <c>PerfmonTrendPoint</c>, plus the interval Lite does not carry).
    /// <para><c>SampleIntervalSeconds</c> is what makes a zero readable: the collector reports 0 in
    /// exactly the cases where no delta was knowable (first sighting, counter reset, gap past the
    /// policy), so (0, 0) is "unknown" while (0, n) is "genuinely idle". Without it the two are the same
    /// number and a fabricated zero reads as quiet (#2234).</para>
    /// <para>Rows written before that fix carry a hard-coded 60 regardless of the real gap, so a rate
    /// derived over a window spanning the upgrade is only as good as its newest rows.</para>
    /// <para>Since V132 (#3653 A7) <c>CntrType</c> is the counter's stored DMV type when every instance row
    /// summed into the point agrees on one, else null (a pre-rung row, or a family whose instances mix
    /// types); <c>DeltaValue</c> and <c>SampleIntervalSeconds</c> are nullable because a GAUGE row stores
    /// neither — its <c>Value</c> is the reading — and a 0 manufactured in their place would be #3642's
    /// fabricated zero on a level that has no delta.</para></summary>
    public sealed record PerfmonTrendPoint(DateTime CollectionTime, long Value, long? DeltaValue, long? SampleIntervalSeconds, int? CntrType = null);

    /// <summary>
    /// One query-duration / execution-count trend point, shared by the three Performance-Trends siblings:
    /// the per-second rate (elapsed ms/sec) plus executions/sec (Lite's <c>QueryTrendPoint</c> shape).
    /// <para><c>ExecutionCount</c> and <c>ExecutionsPerSecond</c> are the SAME quantity - executions per
    /// second - and both are here because the first one shipped truncated to a long. On a server doing
    /// three executions a second that rounds harmlessly; on a quiet one doing 0.4 it reports ZERO, which
    /// reads as an idle server rather than a slow one. The long is kept so a consumer reading it does not
    /// break; new readers should take <c>ExecutionsPerSecond</c>.</para>
    /// </summary>
    public sealed record QueryDurationTrendPoint(
        DateTime CollectionTime, double? Value, long? ExecutionCount, double? ExecutionsPerSecond)
    {
        /// <summary>
        /// Whether this point carries a rate at all (#3541 A12). False for the window's first differenced
        /// collection — no previous collection to difference against — for a collection landing in the
        /// same second as its predecessor, and (on the plan-cache trends, which read the STORED interval:
        /// #3540 V128, #3653) for a restart collection whose interval the calculator could not measure; none
        /// has a denominator, and none is 0. On a bucketed point (#3897), false only when EVERY collection in
        /// the bucket was one of those.
        /// </summary>
        public bool HasRate => Value.HasValue;

        /// <summary>The bucket's worst single collection's elapsed ms per second (#3897) — its busiest hour on the
        /// hourly tier, where the rollup holds no finer grain. Null on a Query Store point, which is not a bucket,
        /// and on an unrated one.</summary>
        public double? PeakElapsedMsPerSecond { get; init; }

        /// <summary>The first collection inside the point (#3897): what <c>effective_start</c> reports, because a
        /// bucketed point is stamped at its bucket's start and that is not a collection the store held. Null on
        /// an unbucketed point, whose own time is its collection.</summary>
        public DateTime? FirstCollectionTime { get; init; }

        /// <summary>How many collections in the point had no knowable rate and were left out of its rates
        /// (#3897): at most one on an unbucketed point (the point itself), any number in a bucket.</summary>
        public long UnratedCollections { get; init; }
    }

    /// <summary>One point of a single query's per-collection history (Lite's <c>QueryStatsHistoryRow</c>,
    /// the columns get_query_trend surfaces): the interval deltas + DOP spread + the plan hash. Time metrics
    /// are microseconds (converted to ms by the tool, matching Lite).</summary>
    public sealed record QueryHistoryPoint(
        DateTime CollectionTime, long DeltaExecutions, long DeltaCpuUs, long DeltaElapsedUs,
        long DeltaLogicalReads, long DeltaLogicalWrites, long DeltaPhysicalReads, long DeltaRows,
        long DeltaSpills, int MinDop, int MaxDop, string QueryPlanHash);

    /// <summary>
    /// Which physical tier answered a query-history read, and over what window (#2353).
    ///
    /// <para><c>raw</c> is the per-collection <c>query_stats</c> table and carries every column.
    /// <c>hourly</c> is the <c>query_stats_hourly</c> continuous aggregate: hour buckets, and only the measures
    /// the rollup keeps — executions, CPU and elapsed. The columns it does not keep are reported as NULL rather
    /// than zero, because zero is a claim and NULL is the absence of one.</para>
    ///
    /// <para><see cref="EffectiveStartUtc"/> is what was actually read, which can be LATER than the start the
    /// caller asked for when even the aggregate does not reach that far back. It exists so the tool can say so
    /// instead of returning a short array under the requested window's label.</para>
    /// </summary>
    public sealed record QueryHistoryResult(
        List<QueryHistoryPoint> Points, string Source, DateTime EffectiveStartUtc, bool Truncated);

    /// <summary>
    /// The aggregate twin of <see cref="QueryHistorySql"/>, reading the hourly continuous aggregate (#2353).
    ///
    /// <para>Shaped to the SAME reader ordinals as the raw query so one mapper serves both. The eight columns
    /// the rollup does not carry — reads, writes, physical reads, rows, spills, the DOP pair and the plan hash —
    /// are selected as typed NULLs and surface as NULL in the payload. That is the honest answer: an hour bucket
    /// has no single plan hash and no single DOP, and inventing a zero would read as "none observed".</para>
    ///
    /// <para>What survives the rollup is exactly what a trend is usually asked for: executions, CPU and elapsed
    /// time. <c>bucket</c> is projected as <c>collection_time</c> so the series column name does not change
    /// underneath a caller that got a raw answer last time.</para>
    /// </summary>
    public static readonly string QueryHistoryHourlySql = QueryHistoryHourlySqlFor(TimescaleSupport.QueryStatsHourlyView);

    /// <summary>
    /// <see cref="QueryHistoryHourlySql"/> over <paramref name="hourlyRelation"/> — the legacy
    /// <c>query_stats_hourly</c> or its interval-honest successor <c>query_stats_interval_hourly</c> (#3653,
    /// Q12), which carries the same column names, so the text differs in the relation and nothing else. The
    /// caller resolves which through <see cref="RollupCoverage.HourlyRelationFor"/>; the constant above is this
    /// builder over the legacy name, which is what every pre-#3653 pin reads.
    /// </summary>
    public static string QueryHistoryHourlySqlFor(string hourlyRelation)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hourlyRelation);

        return $"""
        SELECT
            bucket AS collection_time,
            execution_count_sum AS delta_execution_count,
            worker_time_sum AS delta_worker_time,
            elapsed_time_sum AS delta_elapsed_time,
            CAST(NULL AS bigint) AS delta_logical_reads,
            CAST(NULL AS bigint) AS delta_logical_writes,
            CAST(NULL AS bigint) AS delta_physical_reads,
            CAST(NULL AS bigint) AS delta_rows,
            CAST(NULL AS bigint) AS delta_spills,
            CAST(NULL AS int) AS min_dop,
            CAST(NULL AS int) AS max_dop,
            CAST(NULL AS text) AS query_plan_hash
        FROM {hourlyRelation}
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        AND   bucket >= $4
        AND   bucket <= $5
        ORDER BY bucket
        """;
    }

    /* ─────────────────────────── memory trend ─────────────────────────── */

    /// <summary>
    /// The memory trend — the viewer's <c>MemoryTrendSql</c> (Lite's <c>GetMemoryTrendAsync</c>): the four MB
    /// metrics from <c>v_memory_stats</c> per collection over the window, each <c>numeric(18,2)</c> CAST to
    /// double precision for the typed reader. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string MemoryTrendSql = """
        SELECT
            collection_time,
            CAST(total_server_memory_mb AS double precision) AS total_server_memory_mb,
            CAST(target_server_memory_mb AS double precision) AS target_server_memory_mb,
            CAST(buffer_pool_mb AS double precision) AS buffer_pool_mb,
            CAST(plan_cache_mb AS double precision) AS plan_cache_mb
        FROM v_memory_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time
        """;

    public static async Task<List<MemoryTrendPoint>> GetMemoryTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryTrendPoint>();
        await using var command = postgres.CreateCommand(MemoryTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4)));
        }

        return items;
    }

    /// <summary>
    /// The memory-grant trend — the viewer's <c>MemoryGrantTrendSql</c> (Lite's
    /// <c>GetMemoryGrantTrendAsync</c>): total granted MB across all pools per grants collection over the
    /// window, for the join get_memory_trend makes onto the memory series (#3548). $1 server_id, $2/$3
    /// window (naive UTC).
    /// </summary>
    public const string MemoryGrantTrendSql = """
        SELECT
            collection_time,
            CAST(SUM(granted_memory_mb) AS double precision) AS total_granted_mb
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY collection_time
        ORDER BY collection_time
        """;

    public static async Task<List<MemoryGrantTrendPoint>> GetMemoryGrantTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryGrantTrendPoint>();
        await using var command = postgres.CreateCommand(MemoryGrantTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryGrantTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1)));
        }

        return items;
    }

    /// <summary>
    /// The memory trend BUCKETED (#3960): <see cref="MemoryTrendSql"/>'s samples, byte for byte, gathered into
    /// buckets of <c>$4</c> minutes — each level averaged over the samples in the bucket (a level is never summed),
    /// stamped at the bucket's start, the first at the window's start. $1 server_id, $2/$3 window (naive UTC),
    /// $4 the bucket width in minutes.
    /// </summary>
    public const string MemoryTrendBucketedSql = $"""
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(COALESCE(total_server_memory_mb, 0)) AS total_server_memory_mb,
            AVG(COALESCE(target_server_memory_mb, 0)) AS target_server_memory_mb,
            AVG(COALESCE(buffer_pool_mb, 0)) AS buffer_pool_mb,
            AVG(COALESCE(plan_cache_mb, 0)) AS plan_cache_mb
        FROM (
        {MemoryTrendSql}
        ) AS samples
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="MemoryTrendBucketedSql"/>.</summary>
    public static async Task<List<MemoryBucketPoint>> GetMemoryBucketsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryBucketPoint>();
        await using var command = postgres.CreateCommand(MemoryTrendBucketedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryBucketPoint(
                reader.GetDateTime(0),
                Number(reader, 1),
                Number(reader, 2),
                Number(reader, 3),
                Number(reader, 4)));
        }

        return items;
    }

    /// <summary>
    /// The memory-grant series in the same buckets (#3960): <see cref="MemoryGrantTrendSql"/>'s per-collection totals
    /// averaged and maxed per bucket, so <c>get_memory_trend</c> joins the two series on the bucket they share rather
    /// than on the nearest snapshot. $1 server_id, $2/$3 window (naive UTC), $4 the bucket width in minutes.
    /// </summary>
    public const string MemoryGrantTrendBucketedSql = $"""
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(total_granted_mb) AS avg_granted_mb,
            MAX(total_granted_mb) AS peak_granted_mb
        FROM (
        {MemoryGrantTrendSql}
        ) AS grants
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="MemoryGrantTrendBucketedSql"/>.</summary>
    public static async Task<List<GrantBucketPoint>> GetGrantBucketsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        var items = new List<GrantBucketPoint>();
        await using var command = postgres.CreateCommand(MemoryGrantTrendBucketedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new GrantBucketPoint(reader.GetDateTime(0), Number(reader, 1), Number(reader, 2)));
        }

        return items;
    }

    /// <summary>A numeric column as a double, 0 for NULL — the memory reads' long-standing reading of a missing level.</summary>
    private static double Number(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? 0 : Convert.ToDouble(reader.GetValue(ordinal), System.Globalization.CultureInfo.InvariantCulture);

    /* ─────────────────────────── perfmon trend ─────────────────────────── */

    /// <summary>
    /// A single counter's value + per-interval delta trend — Lite's <c>GetPerfmonTrendAsync</c> / the
    /// viewer's perfmon read: SUM the counter's instances per collection. Postgres <c>SUM(bigint)</c> is
    /// numeric, so both SUMs CAST back to bigint for the typed GetInt64 reader (the viewer's PerfmonTrendsSql
    /// makes the same cast). $1 server_id, $2 counter_name, $3/$4 window (naive UTC).
    /// <para>The interval is MAX, not SUM, and that distinction is load-bearing. cntr_value and
    /// delta_cntr_value are additive across a counter's instance rows — summing Transactions/sec over
    /// every database is a meaningful total — but the interval is the same measured sweep gap repeated
    /// once per instance, so summing it multiplies the denominator by the instance count. Measured on
    /// the fleet: Transactions/sec, Log Flushes/sec and Log Bytes Flushed/sec carry a median of 12 and
    /// up to 17 rows per collection_time, so a summed denominator would report rates 12-17x too LOW —
    /// the same silent corruption this read exists to expose, pointed the other way. MAX also ignores a
    /// 0 from an instance seen for the first time, while still reporting 0 when every instance is
    /// unknown.</para>
    /// <para>The type column is <c>CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END</c>
    /// (V132): a type only where the instance rows summed into the point agree on one. A pre-rung row has
    /// NULL and so does a mixed-type family (the wait-statistics object's instances are a rate, a gauge and
    /// an average under one counter name) — the equality is what stops a mixed sum being classified by
    /// whichever instance's id sorts first. Byte-identical to the viewer's and Lite's expression.</para>
    /// </summary>
    public const string PerfmonTrendSql = """
        SELECT
            collection_time,
            CAST(SUM(cntr_value) AS bigint) AS cntr_value,
            CAST(SUM(delta_cntr_value) AS bigint) AS delta_cntr_value,
            CAST(MAX(sample_interval_seconds) AS bigint) AS sample_interval_seconds,
            CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type
        FROM v_perfmon_stats
        WHERE server_id = $1
        AND   counter_name = $2
        AND   collection_time >= $3
        AND   collection_time <= $4
        GROUP BY collection_time
        ORDER BY collection_time
        """;

    public static async Task<List<PerfmonTrendPoint>> GetPerfmonTrendAsync(
        NpgsqlDataSource postgres, int serverId, string counterName, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<PerfmonTrendPoint>();
        await using var command = postgres.CreateCommand(PerfmonTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, counterName);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PerfmonTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                /* NULL stays NULL on both: a gauge's instance rows store neither (V132), and a pre-V132 row's
                   NULL interval is #3540's third state; 0 is the marker and must not be manufactured. */
                reader.IsDBNull(2) ? null : reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4)));
        }

        return items;
    }

    /// <summary>
    /// A perfmon counter BUCKETED (#3960): <see cref="PerfmonTrendSql"/>'s per-collection sums, byte for byte,
    /// gathered into buckets of <c>$5</c> minutes with every reading the envelope may need — the average, largest
    /// and last value, and the deltas summed per interval class (rated, unknowable 0, unrecorded NULL) with the
    /// seconds the rated ones accrued over — because which one a point publishes depends on the counter's kind
    /// (<see cref="TrendPayloads.PerfmonTrend"/>). The rate's peak is the busiest single rated collection, through
    /// the NULLIF the stored interval always takes. $1 server_id, $2 counter_name, $3/$4 window (naive UTC), $5 the
    /// bucket width in minutes.
    /// </summary>
    public const string PerfmonTrendBucketedSql = $"""
        SELECT
            GREATEST(date_bin(CAST($5 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $3) AS bucket_start,
            AVG(cntr_value) AS avg_value,
            MAX(cntr_value) AS max_value,
            (array_agg(cntr_value ORDER BY collection_time DESC))[1] AS last_value,
            SUM(delta_cntr_value) FILTER (WHERE sample_interval_seconds > 0) AS rated_delta,
            SUM(sample_interval_seconds) FILTER (WHERE sample_interval_seconds > 0) AS rated_seconds,
            COUNT(*) FILTER (WHERE sample_interval_seconds = 0) AS unknowable_collections,
            SUM(delta_cntr_value) FILTER (WHERE sample_interval_seconds = 0) AS unknowable_delta,
            SUM(delta_cntr_value) FILTER (WHERE sample_interval_seconds IS NULL) AS unrecorded_delta,
            MAX(CAST(delta_cntr_value AS double precision) / NULLIF(sample_interval_seconds, 0)) AS peak_per_second,
            CASE WHEN MIN(cntr_type) = MAX(cntr_type) THEN MAX(cntr_type) END AS cntr_type
        FROM (
        {PerfmonTrendSql}
        ) AS collections
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="PerfmonTrendBucketedSql"/>.</summary>
    public static async Task<List<PerfmonBucketPoint>> GetPerfmonBucketsAsync(
        NpgsqlDataSource postgres, int serverId, string counterName, DateTime startUtc, DateTime endUtc, int bucketMinutes,
        CancellationToken cancellationToken = default)
    {
        var items = new List<PerfmonBucketPoint>();
        await using var command = postgres.CreateCommand(PerfmonTrendBucketedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, counterName);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new PerfmonBucketPoint(
                reader.GetDateTime(0),
                Number(reader, 1),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? null : Convert.ToInt64(reader.GetValue(4)),
                reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0 : Convert.ToInt64(reader.GetValue(6)),
                reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7)),
                reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetValue(8)),
                reader.IsDBNull(9) ? null : Convert.ToDouble(reader.GetValue(9)),
                reader.IsDBNull(10) ? null : reader.GetInt32(10)));
        }

        return items;
    }

    /// <summary>
    /// The distinct counter names collected over the window, ordered by name — feeds the get_perfmon_trend
    /// "not_collected" hint (Lite's <c>GetDistinctPerfmonCountersAsync</c> / the viewer's
    /// <c>DistinctPerfmonCountersSql</c>). $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string DistinctPerfmonCountersSql = """
        SELECT DISTINCT counter_name
        FROM v_perfmon_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY counter_name
        """;

    public static async Task<List<string>> GetDistinctPerfmonCountersAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<string>();
        await using var command = postgres.CreateCommand(DistinctPerfmonCountersSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        return items;
    }

    /* ─────────────────────────── file I/O latency trend ─────────────────────────── */

    /// <summary>
    /// The ranking both file I/O statements share, so the series the first one counts are exactly the series the
    /// second one charts (#3897). A series is a (database, file type) pair — the per-database, data-versus-log
    /// view the tool promises — or, when <c>$4</c> names a database, each of that database's files. Ranked by the
    /// window's summed stall (read + write ms), because the read exists to find where storage is SLOW: ranking on
    /// operations, the viewer's order, put nine tempdb data files ahead of a user database whose writes averaged
    /// 190 ms on DARLING01's SQL2022. Operations break a stall tie, then the names, so the order is total.
    ///
    /// <para>Only rows that did work rank (<c>delta_reads &gt; 0 OR delta_writes &gt; 0</c>, the old
    /// <c>top_files</c> filter): an idle series has no latency to trend and must not become a line of nothing.
    /// Pre-#3897 the read took the ten busiest FILES, projected only the database name, and so returned nine
    /// indistinguishable "tempdb" rows per collection; the grain is now named in every row.</para>
    ///
    /// <para>$1 server_id, $2/$3 window (naive UTC), $4 the database to scope to (NULL = every database).</para>
    /// </summary>
    private const string FileIoRankedCte = """
        ranked AS (
            SELECT
                database_name,
                file_type,
                CASE WHEN $4::text IS NULL THEN NULL ELSE file_name END AS file_name,
                COUNT(DISTINCT file_name) AS files,
                CAST(SUM(delta_stall_read_ms + delta_stall_write_ms) AS bigint) AS stall_ms,
                CAST(SUM(delta_reads + delta_writes) AS bigint) AS ops,
                ROW_NUMBER() OVER (
                    ORDER BY SUM(delta_stall_read_ms + delta_stall_write_ms) DESC,
                             SUM(delta_reads + delta_writes) DESC,
                             database_name,
                             file_type,
                             CASE WHEN $4::text IS NULL THEN NULL ELSE file_name END
                ) AS series_rank
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text IS NULL OR database_name = $4)
            AND   (delta_reads > 0 OR delta_writes > 0)
            /* #3540: a restart row's delta is not knowable, and the trend below drops it — so it must not rank a
               series either, or a restart's garbage delta could chart a series ahead of the ones that worked. */
            AND   sample_interval_seconds IS DISTINCT FROM 0
            GROUP BY database_name, file_type, CASE WHEN $4::text IS NULL THEN NULL ELSE file_name END
        )
        """;

    /// <summary>
    /// The first of get_file_io_trend's two reads (#3897): every series the window saw activity on, in rank
    /// order, with what it was ranked by. The tool reads its length before choosing a bucket width — five charted
    /// series and one "(other)" line need a coarser width than two series do — and its rows name what the
    /// "(other)" line pools. $1 server_id, $2/$3 window (naive UTC), $4 database (NULL = all).
    /// </summary>
    public const string FileIoSeriesSql = $"""
        WITH {FileIoRankedCte}
        SELECT database_name, file_type, file_name, files, stall_ms, ops, series_rank
        FROM ranked
        ORDER BY series_rank
        """;

    /// <summary>
    /// The second read (#3897): every ranked series bucketed. The top <c>$5</c> keep their own line and every
    /// ranked series past them is folded into ONE "(other)" line (the custom-view composer's residual label),
    /// so a three-hundred-database server answers with six lines, not six hundred.
    ///
    /// <para><b>The fold happens per COLLECTION, before bucketing</b>: <c>per_collection</c> sums the folded
    /// files' operations and stall at each collection, so "(other)" is one pooled file at every collection
    /// and its peak is the pooled latency at its worst collection — not the worst single file inside it, and not
    /// a sum of intervals across files.</para>
    ///
    /// <para><b>Latency is a ratio of sums, never an average of ratios</b>: the bucket's summed stall over its
    /// summed operations, so a collection with a thousand reads weighs a thousand times one with a single read.
    /// NULL when the bucket did no reads (or no writes) — the old read's <c>ELSE 0</c> published a latency of
    /// 0.00 ms for a collection that did no I/O, a measurement nobody made. The peak is the worst single
    /// collection's ratio, so a one-minute stall survives a sixty-minute bucket.</para>
    ///
    /// <para>Rows whose stored <c>sample_interval_seconds</c> is 0 — no delta knowable, a restart — are
    /// dropped, exactly as before (#3540); <c>IS DISTINCT FROM 0</c> keeps pre-V127 rows (NULL). Each point is
    /// stamped at its bucket's start, the first at the window's start (<c>GREATEST</c>), so no point claims time
    /// the window did not ask for. $1 server_id, $2/$3 window (naive UTC), $4 database (NULL = all), $5 how many
    /// ranked series keep their own line, $6 the bucket width in minutes.</para>
    /// </summary>
    public const string FileIoTrendSql = $"""
        WITH {FileIoRankedCte},
        labelled AS (
            SELECT
                f.collection_time,
                CASE WHEN r.series_rank <= $5 THEN r.database_name ELSE '(other)' END AS database_name,
                CASE WHEN r.series_rank <= $5 THEN r.file_type ELSE '(other)' END AS file_type,
                CASE
                    WHEN r.series_rank <= $5 THEN r.file_name
                    WHEN $4::text IS NULL THEN NULL
                    ELSE '(other)'
                END AS file_name,
                f.delta_reads,
                f.delta_writes,
                f.delta_stall_read_ms,
                f.delta_stall_write_ms
            FROM v_file_io_stats AS f
            JOIN ranked AS r
              ON  r.database_name = f.database_name
              AND r.file_type = f.file_type
              AND (r.file_name IS NULL OR r.file_name = f.file_name)
            WHERE f.server_id = $1
            AND   f.collection_time >= $2
            AND   f.collection_time <= $3
            AND   ($4::text IS NULL OR f.database_name = $4)
            AND   f.sample_interval_seconds IS DISTINCT FROM 0
        ),
        per_collection AS (
            SELECT
                collection_time,
                database_name,
                file_type,
                file_name,
                SUM(delta_reads) AS reads,
                SUM(delta_writes) AS writes,
                SUM(CAST(delta_stall_read_ms AS double precision)) AS stall_read_ms,
                SUM(CAST(delta_stall_write_ms AS double precision)) AS stall_write_ms
            FROM labelled
            GROUP BY collection_time, database_name, file_type, file_name
        )
        SELECT
            GREATEST(date_bin(CAST($6 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            database_name,
            file_type,
            file_name,
            CAST(SUM(reads) AS bigint) AS reads,
            CAST(SUM(writes) AS bigint) AS writes,
            SUM(stall_read_ms) AS stall_read_ms,
            SUM(stall_write_ms) AS stall_write_ms,
            SUM(stall_read_ms) / NULLIF(CAST(SUM(reads) AS double precision), 0) AS avg_read_latency_ms,
            SUM(stall_write_ms) / NULLIF(CAST(SUM(writes) AS double precision), 0) AS avg_write_latency_ms,
            MAX(stall_read_ms / NULLIF(CAST(reads AS double precision), 0)) AS peak_read_latency_ms,
            MAX(stall_write_ms / NULLIF(CAST(writes AS double precision), 0)) AS peak_write_latency_ms
        FROM per_collection
        GROUP BY 1, 2, 3, 4
        ORDER BY 1, 2, 3, 4
        """;

    /// <summary>Runs <see cref="FileIoSeriesSql"/>: the window's active series, in rank order.</summary>
    public static async Task<List<FileIoSeries>> GetFileIoSeriesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string? databaseName,
        CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoSeries>();
        await using var command = postgres.CreateCommand(FileIoSeriesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddNullableText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new FileIoSeries(
                reader.GetString(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.GetInt64(6)));
        }

        return items;
    }

    /// <summary>Runs <see cref="FileIoTrendSql"/>: the top <paramref name="chartedSeries"/> series and the
    /// "(other)" fold, bucketed at <paramref name="bucketMinutes"/>.</summary>
    public static async Task<List<FileIoPoint>> GetFileIoTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, string? databaseName,
        int chartedSeries, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoPoint>();
        await using var command = postgres.CreateCommand(FileIoTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddNullableText(command, databaseName);
        DarlingMcpReadParameters.AddInt(command, chartedSeries);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new FileIoPoint(
                reader.GetDateTime(0),
                reader.GetString(1),
                reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetDouble(8),
                reader.IsDBNull(9) ? null : reader.GetDouble(9),
                reader.IsDBNull(10) ? null : reader.GetDouble(10),
                reader.IsDBNull(11) ? null : reader.GetDouble(11)));
        }

        return items;
    }

    /* ─────────────────────────── query duration trend ─────────────────────────── */

    /// <summary>
    /// The query-stats duration trend — <see cref="DurationTrendRouting.BuildBucketedRawTrendSql"/> over
    /// <c>query_stats</c>, by alias (#3897; #3653, measurement A11, before it): per collection, the summed
    /// <c>delta_elapsed_time</c> (→ ms) and <c>delta_execution_count</c> over the collection's STORED
    /// <c>sample_interval_seconds</c>, then every collection counted in its bucket of <c>$4</c> minutes. The
    /// per-collection CTE is the viewer's <c>QueryDurationTrendSql</c>'s own text (one builder fragment, so the
    /// tool's buckets and the desktop chart's points come from the same collections); what the tool adds is the
    /// bucketing an MCP answer needs (#3897: a day of per-collection points was 160 KB, three days 487 KB).
    /// Reads the base <c>query_stats</c> table because it projects no text — a read that wanted
    /// <c>query_text</c> or <c>query_plan_xml</c> would have to go through <c>v_query_stats</c> to resolve the
    /// #1767 payload dimensions. Summed bigints come back as numeric, so the reads Convert tolerantly. $1
    /// server_id, $2/$3 window (naive UTC), $4 bucket width in minutes.
    ///
    /// <para><b>Why this is an alias and not the LAG text that stood here.</b> Until #3653 this const was
    /// its own copy of the read, and its denominator was <c>LAG(collection_time)</c> — the seconds between
    /// this collection and the previous one, recomputed from the row clock. #3695 moved the viewer and Lite
    /// to the interval the store HAS (<c>sample_interval_seconds</c>, stamped per row by the delta
    /// calculator from the same two instants), read three-state: <c>MAX</c> over the collection's rows, 0 →
    /// NULL (a restart collection, whose deltas are the #2234 "nothing knowable" marker, is UNRATED), NULL →
    /// the LAG (a collection that never recorded an interval renders as it always did). This file was
    /// outside that lane's boundary, so between #3695 and this alias the tool's raw route and the viewer's chart
    /// disagreed about exactly one row class: a restart collection, which the LAG divided into a confident
    /// <c>0.00 ms/sec</c> while the viewer skipped it. The mechanism is documented once, on
    /// <see cref="DurationTrendRouting.BuildRawTrendSql"/>; DarlingMcpTrendToolsTests pins the declaration
    /// as an alias and the file as free of the retired LAG-only text, so there is one definition and nothing
    /// to drift. A static readonly rather than a const because the Storage side is a builder, as for the
    /// hourly twin below.</para>
    ///
    /// <para><b>An unrated collection has null rates and is KEPT (#3541 A12, #3540 A8).</b> Three rows have
    /// no denominator: the first collection of a stretch that recorded no interval (its LAG is NULL — there
    /// is no previous collection inside the window to difference against), a restart collection (stored
    /// interval 0), and the degenerate two-collections-in-one-second case. The shape this replaced in #3642
    /// (<c>CASE ... ELSE 0 END</c>, Lite's original behaviour) published that unknowable as a measured 0.0:
    /// every duration series began with a fabricated quiet instant, which an agent charting the window read
    /// as "idle, then busy" and which dragged every first-bucket average toward zero. Contract rule 5 —
    /// zero is a measurement — so the CASE has no ELSE and the rate columns are NULL for such a row. The row
    /// is KEPT rather than filtered, deliberately: the collection happened, <c>effective_start</c> is
    /// truthfully its instant, and a window holding exactly one collection is "one collection, no rate yet"
    /// rather than an empty series the empty ladder would mis-describe as a quiet window. The reader carries
    /// the nulls through (<see cref="QueryDurationTrendPoint"/>) and the tool publishes them with the
    /// reason. Since #3897 a collection like that is left out of its BUCKET's rates (numerator and denominator
    /// both), and a bucket holding nothing else is the unrated point. The hourly twin below has no such row: its
    /// denominator is the bucket width, known for every bucket.</para>
    /// </summary>
    public static readonly string QueryDurationTrendSql =
        DurationTrendRouting.BuildBucketedRawTrendSql("query_stats");

    /* ───────────── the tier ladder and the hourly-tier SQL: aliases of DurationTrendRouting (#3653) ─────────────

       Everything in this block used to be DEFINED here. #3666 moved the definitions to Storage
       (DurationTrendRouting) because the desktop viewer's Performance Trends tab needed the identical
       decision and the identical hourly text and cannot reference this assembly (the #1661 / #2530
       arrangement: the viewer reads the store beside the service, never through it). That PR pinned this
       file's copies EQUAL to the Storage members in ViewerTrendRoutingPortTests rather than editing this
       file, which belonged to other lanes that night. Equality is a pin; it fails only after the two have
       drifted. #3684 made each member an ALIAS — a const bound to the Storage const, a static
       readonly bound to the Storage builder's output, an expression-bodied delegation for the pure
       functions — so there is one definition and nothing to drift. The names stay: DarlingMcpTrendTools,
       DarlingQueryTrendTieringTests and DarlingMcpTrendToolsTests read them by these names, the payload
       vocabulary they document (source / effective_start / window_truncated) is this reader's contract, and the
       reasoning for each — the wall-clock age rule, the bucket-width denominator, the ninety-minute slack —
       lives ONCE, on the Storage member each alias names. The SQL aliases are static readonly rather
       than const because the Storage side is a builder (one text with and one without the viewer's $4
       database filter), and a const cannot be initialized from a call; no consumer needed the const-ness
       (the tests read the values, and the reads pass them as command text). The two RAW-tier consts —
       QueryDurationTrendSql above and ProcedureDurationTrendSql below — joined the block last, once #3695
       had built the raw read in Storage and pinned the builder line-equal to the hand-kept procedure text:
       the query alias changed the tool's answer on one row class (see its remarks), the procedure alias
       changed nothing but where the text lives. */

    /// <summary>
    /// The seconds in one hourly-rollup bucket, as the SQL literal the hourly-tier duration trends divide by
    /// — <see cref="DurationTrendRouting.HourlyBucketSecondsSql"/>, by alias (#3653); pinned equal to
    /// <see cref="TimescaleSupport.HourlyBucket"/> by DarlingMcpTrendToolsTests so the literal and the
    /// rollup's declared bucket cannot drift.
    /// </summary>
    public const string HourlyBucketSecondsSql = DurationTrendRouting.HourlyBucketSecondsSql;

    /// <summary>
    /// The hourly-tier twin of <see cref="QueryDurationTrendSql"/> (#3541 A2): the same two per-second rates,
    /// read from the <c>query_stats_hourly</c> continuous aggregate for windows whose oldest point the raw
    /// tier no longer holds — <see cref="DurationTrendRouting.BuildBucketedHourlyTrendSql"/> over the legacy
    /// view, by alias (#3897), which at a 60-minute width is the viewer's hourly statement's figures hour for
    /// hour and wider gathers whole hours into one point. The mechanism (why the denominator is the bucket width
    /// and not a LAG, why an hour the collector covered only partly reads LOW and never high, why the series
    /// trails the clock by up to two hours) is documented once, on
    /// <see cref="DurationTrendRouting.BuildHourlyTrendSql"/>. What is this reader's to add: the payload STATES
    /// that trade (<c>aggregate_note</c>) rather than hiding it. The routed read builds this text from the
    /// route's resolved relation; the constant is the builder over the legacy view, which every store has.
    /// $1 server_id, $2/$3 window (naive UTC), $4 bucket width in minutes (a whole number of hours).
    /// </summary>
    public static readonly string QueryDurationTrendHourlySql =
        DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView);

    /// <summary>
    /// The hourly-tier twin of <see cref="ProcedureDurationTrendSql"/> (#3541 A2), over
    /// <c>procedure_stats_hourly</c> — <see cref="DurationTrendRouting.BuildBucketedHourlyTrendSql"/>, by alias
    /// (#3897). Same shape and same bucket-width denominator as <see cref="QueryDurationTrendHourlySql"/>; see
    /// there. $1 server_id, $2/$3 window (naive UTC), $4 bucket width in minutes.
    /// </summary>
    public static readonly string ProcedureDurationTrendHourlySql =
        DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.ProcedureStatsHourlyView);

    /// <summary>
    /// Which tier one unkeyed duration trend read serves from, and the evidence the choice rests on (#3541 A2)
    /// — the get_query_trend routing (<see cref="QueryHistoryResult"/>, #2353) generalized to the reads that
    /// have no query key.
    ///
    /// <para><see cref="RawTable"/> and <see cref="HourlyView"/> are the pair this trend reads at each tier;
    /// <see cref="Relation"/> is the one <see cref="Tier"/> picked, so a payload or an empty message can say
    /// WHAT WAS READ rather than what was asked for. Since #3653 (Q12) <see cref="HourlyView"/> is the hourly
    /// relation RESOLVED for this window — the interval-honest successor where the store has it and it reaches
    /// as far back as the legacy for this window (<see cref="RollupCoverage.HourlyRelationFor"/>), the legacy
    /// otherwise — and the routed read builds its hourly SQL from it, so the payload's relation is the one
    /// that was walked. The TIER was still decided over the legacy pair's coverage, the deeper of the two. <see cref="HourlyAvailable"/> and <see cref="Coverage"/>
    /// are kept on the route so the empty branch can say why the tier it read holds nothing — a rollup that
    /// has materialized nothing and a rollup whose floor sits above the window's start are different facts
    /// with different remedies, and both differ from a quiet server.</para>
    ///
    /// <para><see cref="RawRetentionApplies"/> is whether THIS grain's raw table can have had rows dropped,
    /// and it is scoped to the grain rather than to the store on purpose. Retention is armed per raw table by
    /// the #1680 gate, which arms a table's purge only once that table's OWN rollup covers everything the
    /// table holds — so a grain whose rollup does not exist (plain PostgreSQL, a failed availability probe, or
    /// #1664's failure-isolated partial build where one grain's aggregate failed its ensure sweep while the
    /// others built) has its purge held paused and its raw rows intact, whatever the other grains' rollups
    /// are doing. A store-wide "any rollup exists" test would have told the caller of the un-rolled grain that
    /// its rows were dropped and that widening cannot help — the exact false-and-harmful narrative this route
    /// exists to remove, wearing the fix's own clothes. Where it is false, raw holds the complete answer and
    /// "quiet, widen the window" is honest (#1665); where it is true, the empty branch has to consider that
    /// the rows were DROPPED, not absent.</para>
    ///
    /// <para>It is rollup EXISTENCE, deliberately, not the gate's armed state (<c>RetentionHoldReading.Armed</c>):
    /// a rollup can exist while its raw purge is still held pending backfill, so this is an UPPER BOUND on
    /// "can have been dropped" — never a claim that rows were. That is the right bound for its one consumer:
    /// wherever raw's oldest row is MEASURED (<see cref="Coverage"/>), the measurement decides and this flag is
    /// not consulted; it backstops only the unmeasured case, and it errs toward the horizon message (which
    /// still names the measured facts it has and the remedy) rather than toward "quiet, widen" — the false
    /// direction. Reading the gate's job state per call would cost a catalog round trip on every empty answer
    /// to refine a fallback the measurement already makes rare.</para>
    ///
    /// <para><see cref="ResolvedAtUtc"/> is the wall clock the age decision was measured against. It rides on
    /// the route so the tool that consumes it never names the clock itself: an <c>as_of</c>-anchored tool's
    /// only "now" is the anchor it resolved (AsOfWindowAnchorTests pins that as an absolute), and retention's
    /// clock — which is NOT the anchor, see <see cref="ShouldUseRawTier"/> — is this reader's concern.</para>
    /// </summary>
    public sealed record DurationTrendRoute(
        RetentionTier Tier, string RawTable, string HourlyView, bool HourlyAvailable, TierCoverage Coverage,
        bool RawRetentionApplies, DateTime ResolvedAtUtc, string? HourlyFromClause = null,
        string? ProbeHourlyView = null)
    {
        /// <summary>The payload's <c>source</c> word: <c>raw</c> or <c>hourly</c>, get_query_trend's vocabulary
        /// — <see cref="DurationTrendRouting.SourceWord"/>, the one spelling the viewer's chart title also
        /// leads with (#3653), so a user reading the chart and an agent reading the payload get one word.</summary>
        public string Source => DurationTrendRouting.SourceWord(Tier);

        /// <summary>
        /// The hourly FROM-clause item to splice into the bucketed and keyed builders (#3653 A6, decision 1):
        /// <see cref="HourlyView"/> stays a bare NAME for probes/logs/messages, and this is the
        /// <see cref="RollupCoverage.StitchedRelationSql"/> output — <c>collect.&lt;HourlyView&gt; AS h</c>
        /// unchanged when there is no successor, or the stitched UNION ALL past the successor's floor.
        /// Null on a route that never resolved a from-clause (a test-only route built by the record
        /// constructor directly); callers fall back to <c>collect.{HourlyView} AS h</c>, the same text.
        /// </summary>
        public string HourlyFromClauseOrDefault => HourlyFromClause ?? $"collect.{HourlyView} AS h";

        /// <summary>
        /// The NAME <see cref="HasAnySampleOnRouteAsync"/> probes (#3653 A6, decision 4): the successor when
        /// the stitch applies (the successor exists AND its floor is at or before the window's end — the
        /// frozen legacy will empty over time, so probing it would eventually answer "never sampled" on a
        /// store that is still being written to), otherwise <see cref="HourlyView"/> as today. Null on a
        /// route built without resolving the stitch; callers fall back to <see cref="HourlyView"/>.
        /// </summary>
        public string ProbeHourlyViewOrDefault => ProbeHourlyView ?? HourlyView;

        /// <summary>The relation the read actually walks.</summary>
        public string Relation => Tier == RetentionTier.Raw ? RawTable : HourlyView;

        /// <summary>
        /// Whether the raw table is measured to hold rows at or before <paramref name="windowStartUtc"/>. Null
        /// when raw's oldest row was not measured (no rollups, probe failed, or the table is empty) — the
        /// caller falls back to the retention span, which is the proxy #1759 warns is wrong in the dangerous
        /// direction on a held-purge store, so it is used only where nothing was measured.
        /// </summary>
        public bool? RawReaches(DateTime windowStartUtc) =>
            Coverage.RawOldestUtc is DateTime oldest ? oldest <= windowStartUtc : null;
    }

    /// <summary>
    /// One routed duration-trend answer: the points, the route that produced them, and what the points
    /// actually cover — the <see cref="QueryHistoryResult"/> shape for the unkeyed trends, so all four tiered
    /// reads describe themselves with the same three words (<c>source</c>, <c>effective_start</c>,
    /// <c>window_truncated</c> — the wire spelling of <c>Truncated</c> since #3653 item 17; the member keeps its
    /// name, the key says which of the two facts spelled <c>truncated</c> this one is).
    /// </summary>
    public sealed record DurationTrendResult(
        List<QueryDurationTrendPoint> Points, DurationTrendRoute Route, DateTime EffectiveStartUtc, bool Truncated);

    /// <summary>
    /// Resolves the route for the query-stats duration trend: raw <c>query_stats</c> or
    /// <c>query_stats_hourly</c>, by <see cref="ResolveTier"/>. <paramref name="nowUtc"/> is the WALL CLOCK,
    /// never the window's end — see <see cref="ShouldUseRawTier"/> — and defaults to the real clock, the same
    /// way <see cref="GetQueryHistoryAsync"/> takes it: the tool passes only its anchored window, a test
    /// passes a fixed instant to pin the boundary.
    /// </summary>
    public static DurationTrendRoute ResolveQueryDurationTrendRoute(
        DateTime startUtc, RollupAvailability rollups, RollupCoverage coverage, DateTime? nowUtc = null,
        DateTime? windowEndUtc = null)
        => ResolveDurationTrendRoute(
            "query_stats", TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView,
            rollups.QueryGrainHourly, startUtc, nowUtc ?? DateTime.UtcNow, rollups, coverage, windowEndUtc);

    /// <summary>The procedure-stats twin of <see cref="ResolveQueryDurationTrendRoute"/>.</summary>
    public static DurationTrendRoute ResolveProcedureDurationTrendRoute(
        DateTime startUtc, RollupAvailability rollups, RollupCoverage coverage, DateTime? nowUtc = null,
        DateTime? windowEndUtc = null)
        => ResolveDurationTrendRoute(
            "procedure_stats", TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView,
            rollups.ProcedureGrainHourly, startUtc, nowUtc ?? DateTime.UtcNow, rollups, coverage, windowEndUtc);

    private static DurationTrendRoute ResolveDurationTrendRoute(
        string rawTable, string hourlyView, string dailyView, bool hourlyAvailable,
        DateTime startUtc, DateTime nowUtc, RollupAvailability rollups, RollupCoverage coverage,
        DateTime? windowEndUtc = null)
    {
        ArgumentNullException.ThrowIfNull(coverage);

        var tierCoverage = coverage.For(hourlyView, dailyView);

        /* #3653 A6, decision 4: the probe NAME is the successor when the stitch applies for THIS window — the
           successor exists (a non-null floor implies existence: an absent or never-materialized successor
           answers a null floor, see RollupCoverage.FloorOf) AND its floor is at or before the window's end.
           windowEndUtc is OPTIONAL: a caller that has not resolved a window end (every pre-#3653-A6 caller)
           leaves ProbeHourlyView null, so ProbeHourlyViewOrDefault falls back to the legacy — today's
           behaviour, unchanged. */
        var successor = TimescaleSupport.SuccessorOf(hourlyView);
        var successorFloor = successor is null ? null : coverage.FloorOf(successor);
        var probeHourlyView = windowEndUtc is DateTime end && successorFloor is DateTime floor && floor <= end
            ? successor
            : null;

        return new DurationTrendRoute(
            ResolveTier(startUtc, nowUtc, hourlyAvailable, tierCoverage),
            /* Tier over the legacy pair's coverage; relation by the supply rule (#3653, Q12) — see the record. */
            rawTable, coverage.HourlyRelationFor(hourlyView, startUtc), hourlyAvailable, tierCoverage,
            /* Grain-scoped, not rollups != None — see the record's remarks: the arming gate is per table. */
            RawRetentionApplies: hourlyAvailable,
            ResolvedAtUtc: nowUtc,
            /* #3653 A6: the FROM-clause item for the bucketed/keyed builders — decision 1 keeps HourlyView a
               bare name for probes/logs, and this is StitchedRelationSql's splice-ready answer for the same
               window the tier above was decided over. */
            HourlyFromClause: coverage.StitchedRelationSql(hourlyView, "h", startUtc, RollupCoverage.StitchTier.Hourly),
            ProbeHourlyView: probeHourlyView);
    }

    /// <summary>
    /// Runs the query-stats duration trend down <paramref name="route"/> (#3541 A2): the raw read for a
    /// window raw can serve, the hourly twin otherwise, bucketed at <paramref name="bucketMinutes"/> (#3897; a
    /// whole number of hours on the hourly tier, which the tool enforces). Coverage is described by
    /// <see cref="DescribeCoverage"/> so the payload can say what it served.
    /// </summary>
    public static Task<DurationTrendResult> GetQueryDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, DurationTrendRoute route,
        int bucketMinutes, CancellationToken cancellationToken = default)
        => ReadRoutedDurationTrendAsync(
            QueryDurationTrendSql, postgres, serverId, startUtc, endUtc, route, bucketMinutes, cancellationToken);

    /* --------------------- procedure + Query Store duration trends (#2484) --------------------- */

    /// <summary>
    /// The procedure-stats duration trend - <see cref="DurationTrendRouting.BuildBucketedRawTrendSql"/> over
    /// <c>procedure_stats</c>, by alias (#3897; #3653 before it). Its per-collection CTE is the viewer's
    /// <c>ProcedureDurationTrendSql</c>'s own text (one builder fragment); before the #3653 alias the two were
    /// hand-kept copies that #3695 pinned line-equal to the builder, so that alias moved the text and changed no
    /// answer. The bucketing is #3897's, the same as the query-stats twin's.
    /// <para>Not a duplicate of the query-stats trend, and the difference is the point: <c>query_stats</c>
    /// attributes a procedure's work to the individual statements inside it, so a procedure that got slower
    /// shows up smeared across however many statements it runs. This charges the whole call to the
    /// procedure. When both are available, the pair answers "did ad-hoc SQL regress, or did a procedure?" -
    /// which one series alone never can. $1 server_id, $2/$3 window (naive UTC), $4 bucket width in minutes.</para>
    /// <para>#3540 (V128): the interval is the collection's STORED one where the rows have it — <c>MAX</c>
    /// over the collection's rows, because a plan first seen in an otherwise steady pass carries 0 beside
    /// its siblings' real interval and contributes 0 to the sums; MAX is 0 only when EVERY row was
    /// unknowable (a restart), and that 0 becomes NULL through <c>NULLIF</c> so the rates are NULL — an
    /// UNRATED point the reader keeps rather than rendering 0.00 ms/sec (#3541 A12; see
    /// <see cref="QueryDurationTrendSql"/> for why the row stays). NULL (a pre-V128 collection) falls back to
    /// the LAG this read always used, whose first row is likewise unrated, never a fabricated 0. No
    /// <c>ELSE 0</c>. This was the idiom the query-stats twin above caught up with in #3695 / #3653; the
    /// builder's remarks carry it once for both tables.</para>
    /// </summary>
    public static readonly string ProcedureDurationTrendSql =
        DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats");

    /// <summary>
    /// The Query Store duration trend - the viewer's <c>QueryStoreDurationTrendSql</c>, verbatim apart from
    /// the database filter, INCLUDING the #1841 tier-2 interval placement and its legacy arm.
    /// <para>Copied rather than simplified. The two arms are not decoration: arm 1 dedups each runtime
    /// interval to its final cumulative snapshot and places it at <c>interval_start_time_utc</c> - the hour
    /// the work actually RAN - because Query Store has no delta columns and re-fetches an open interval's
    /// running totals every cycle, so charging every fetch to its collection time triple-counts the same
    /// work. Arm 2 keeps rows collected before that fix on their old un-deduped treatment, because no
    /// interval start exists for them and none can be reconstructed. The arms split on
    /// <c>interval_start_time_utc IS NULL</c>, so they partition the rows with no overlap and no gap.
    /// Rewriting either arm here would make the browser and the desktop viewer disagree about the same
    /// hour. The first placed interval in the window carries NULL rates, not 0 — see
    /// <see cref="QueryDurationTrendSql"/> (#3541 A12); the rollup route's builder applies the same rule to
    /// its first bucket. $1 server_id, $2/$3 window (naive UTC).</para>
    /// <para><b>#2736: this is now the FALLBACK, not the read.</b> The rank-over-raw below costs the whole
    /// slab regardless of the window, which exceeds the mcp role's statement_timeout on a large store —
    /// so on stores with a materialized <c>query_store_stats_corrected_hourly</c> the tool routes through
    /// <see cref="QueryStoreDurationTrendRollupSql"/> and this shape runs only where it is affordable
    /// (no rollup: plain PostgreSQL, or nothing materialized yet). Its ±slab stays untouched on purpose —
    /// see <see cref="QueryStoreTrendRouting"/>.</para>
    /// </summary>
    public const string QueryStoreDurationTrendSql = """
        WITH placed AS
        (
            /* Arm 1 (#1841 tier 2) - rows carrying the interval identity. Dedup to the interval's FINAL
               cumulative snapshot, then place it at interval_start_time_utc: the hour the work ran, not
               the cycle that last fetched it. */
            SELECT
                interval_start_time_utc AS point_time,
                execution_count,
                avg_duration_us
            FROM
            (
                SELECT
                    interval_start_time_utc,
                    execution_count,
                    avg_duration_us,
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                        ORDER BY collection_time DESC, execution_count DESC
                    ) AS rn
                FROM query_store_stats
                WHERE server_id = $1
                /* Windowed on the column this arm PLACES its points at (#1892). Filtering on
                   collection_time here put a point outside the range the caller asked for, and dropped
                   the range's final interval because its closing fetch had not happened yet. */
                AND   interval_start_time_utc >= $2
                AND   interval_start_time_utc <= $3
                /* Chunk-exclusion bounds only. */
                AND   collection_time >= $2 - interval '1 day'
                AND   collection_time <= $3 + interval '30 days'
                AND   interval_start_time_utc IS NOT NULL
            ) AS identified
            WHERE rn = 1

            UNION ALL

            /* Arm 2 - rows collected before tier 2. No interval start exists and none can be
               reconstructed, so these keep the pre-tier-2 treatment byte for byte. */
            SELECT
                collection_time AS point_time,
                execution_count,
                avg_duration_us
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   interval_start_time_utc IS NULL
        ),
        raw AS
        (
            SELECT
                point_time,
                SUM(execution_count * avg_duration_us / 1000.0) AS total_duration_ms,
                SUM(execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', point_time) - date_trunc('second', LAG(point_time) OVER (ORDER BY point_time)))) AS interval_seconds
            FROM placed
            GROUP BY point_time
        )
        SELECT
            point_time AS collection_time,
            CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds END AS duration_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
        FROM raw
        ORDER BY point_time
        """;

    /// <summary>
    /// The rollup-routed Query Store duration trend (#2736): the materialized window portion served from
    /// <c>query_store_stats_corrected_hourly</c> as a rollup scan, the unmaterialized tail from the raw arms
    /// with tail-tight bounds instead of the fixed ±slab. Built by
    /// <see cref="QueryStoreTrendRouting.BuildRollupTrendSql"/> — the SAME builder the viewer's twin uses,
    /// so the two apps cannot drift about the same hour; this copy just omits the viewer's $5 database
    /// filter. Chosen only when <see cref="QueryStoreTrendRouting.ResolveAsync"/> proved the rollup present
    /// and materialized; otherwise <see cref="QueryStoreDurationTrendSql"/> runs unchanged.
    /// $1 server_id, $2/$3 window, $4 the raw boundary (all naive UTC).
    /// </summary>
    public static readonly string QueryStoreDurationTrendRollupSql =
        QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: false);

    /// <summary>Runs the procedure-stats duration trend down <paramref name="route"/> (#3541 A2) — the
    /// procedure twin of <see cref="GetQueryDurationTrendAsync"/>.</summary>
    public static Task<DurationTrendResult> GetProcedureDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, DurationTrendRoute route,
        int bucketMinutes, CancellationToken cancellationToken = default)
        => ReadRoutedDurationTrendAsync(
            ProcedureDurationTrendSql, postgres, serverId, startUtc, endUtc, route, bucketMinutes, cancellationToken);

    /// <summary>
    /// The shared body of the two routed reads: pick the tier's SQL, read the three-column point shape, and
    /// describe what came back. One method so the query and procedure trends cannot drift in how they
    /// route, read, or describe coverage.
    /// </summary>
    private static async Task<DurationTrendResult> ReadRoutedDurationTrendAsync(
        string rawSql, NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DurationTrendRoute route, int bucketMinutes, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(route);

        /* The hourly text is BUILT from the route's resolved relation (#3653, Q12) rather than taken from the
           QueryDurationTrendHourlySql / ProcedureDurationTrendHourlySql constants: those are the builder over
           the LEGACY view, and the route may have resolved the interval-honest successor. Same builder, same
           shape — the constants stay as the pinned legacy text and as what a store without the successors
           still runs.
           #3653 A6: the builder's {hourlyView} slot is a FROM-clause site, not a name — decision 1 splices
           route.HourlyFromClauseOrDefault (StitchedRelationSql's answer, "collect.<HourlyView> AS h" with no
           successor, byte-identical to the pre-stitch text) rather than the bare route.HourlyView, which stays
           reserved for probes and logs. */
        await using var command = postgres.CreateCommand(
            route.Tier == RetentionTier.Raw
                ? rawSql
                : DurationTrendRouting.BuildBucketedHourlyTrendSql(route.HourlyFromClauseOrDefault));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddInt(command, bucketMinutes);
        var points = await ReadBucketedDurationPointsAsync(command, cancellationToken);

        /* effective_start is the first COLLECTION the store held (#2353), not the first bucket's start (#3897):
           a bucket is stamped at its boundary, which can sit before the window's first collection. */
        var (effectiveStart, truncated) = DescribeCoverage(points.Count > 0 ? points[0].FirstCollectionTime : null, startUtc);
        return new DurationTrendResult(points, route, effectiveStart, truncated);
    }

    /// <summary>
    /// Reads the bucketed duration shape both routed tiers project (#3897): the bucket's start, its two
    /// time-weighted rates (NULL when nothing in the bucket was rated — kept as an unrated point, #3541 A12),
    /// the worst single collection's rate (the busiest hour on the hourly tier), the bucket's first collection, and how many
    /// of its collections were left out of the rates.
    /// </summary>
    private static async Task<List<QueryDurationTrendPoint>> ReadBucketedDurationPointsAsync(
        NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var items = new List<QueryDurationTrendPoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var executionsPerSecond = reader.IsDBNull(2) ? (double?)null : Convert.ToDouble(reader.GetValue(2));
            items.Add(new QueryDurationTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                executionsPerSecond is { } eps ? (long)eps : null,
                executionsPerSecond)
            {
                PeakElapsedMsPerSecond = reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)),
                FirstCollectionTime = reader.GetDateTime(4),
                UnratedCollections = reader.IsDBNull(5) ? 0 : Convert.ToInt64(reader.GetValue(5)),
            });
        }

        return items;
    }

    /// <summary>
    /// Runs <see cref="QueryStoreDurationTrendSql"/> — the raw-only route, kept for callers that have not
    /// resolved a route (and for stores without the corrected rollup, where it IS the route).
    /// </summary>
    public static Task<List<QueryDurationTrendPoint>> GetQueryStoreDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => GetQueryStoreDurationTrendAsync(
            postgres, serverId, startUtc, endUtc, QueryStoreTrendRouting.QueryStoreTrendRoute.RawOnly, cancellationToken);

    /// <summary>
    /// Runs the Query Store duration trend down the resolved <paramref name="route"/> (#2736): the
    /// rollup-routed SQL when the corrected hourly is present and materialized, the original raw read
    /// otherwise. Callers resolve the route with <see cref="QueryStoreTrendRouting.ResolveAsync"/> so they
    /// can also disclose the routing in their payload.
    /// </summary>
    public static async Task<List<QueryDurationTrendPoint>> GetQueryStoreDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        QueryStoreTrendRouting.QueryStoreTrendRoute route, CancellationToken cancellationToken = default)
    {
        if (!route.UseRollup)
        {
            return await ReadDurationTrendAsync(QueryStoreDurationTrendSql, postgres, serverId, startUtc, endUtc, cancellationToken);
        }

        await using var command = postgres.CreateCommand(QueryStoreDurationTrendRollupSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        DarlingMcpReadParameters.AddTimestamp(command, route.RawStartUtc);
        return await ReadDurationPointsAsync(command, cancellationToken);
    }

    /// <summary>
    /// Shared reader for the three duration trends. All three project the same three columns - point time,
    /// a per-second value, a per-second execution rate - which is the viewer's own arrangement
    /// (<c>ReadDurationTrendAsync</c>), kept so the three series cannot drift apart in how they are read.
    /// Summed bigint deltas come back as Postgres numeric, so the values Convert tolerantly.
    /// </summary>
    private static async Task<List<QueryDurationTrendPoint>> ReadDurationTrendAsync(
        string sql, NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        return await ReadDurationPointsAsync(command, cancellationToken);
    }

    /// <summary>Executes a bound duration-trend command and reads the shared three-column point shape.</summary>
    private static async Task<List<QueryDurationTrendPoint>> ReadDurationPointsAsync(
        NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var items = new List<QueryDurationTrendPoint>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* NULL stays NULL (#3541 A12): no rate for the window's first collection, or for a collection whose
               stored interval was unknowable (a restart pass, #3540 V128) — either way the point is kept as
               UNRATED rather than dropped or coerced to the fabricated quiet the SQL stopped producing. */
            var executionsPerSecond = reader.IsDBNull(2) ? (double?)null : Convert.ToDouble(reader.GetValue(2));
            items.Add(new QueryDurationTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? null : Convert.ToDouble(reader.GetValue(1)),
                executionsPerSecond is { } eps ? (long)eps : null,
                executionsPerSecond)
            {
                /* An unbucketed point is its own single interval: no peak finer than itself to report, and at
                   most one unrated collection — itself. */
                UnratedCollections = reader.IsDBNull(1) ? 1 : 0,
            });
        }

        return items;
    }

    /* ─────────────────────────── single-query history trend ─────────────────────────── */

    /// <summary>
    /// A single query's per-collection history — Lite's <c>GetQueryStatsHistoryAsync</c>, focused to the
    /// columns get_query_trend surfaces: the interval deltas + DOP spread + plan hash for one
    /// (database, query_hash) over the window, oldest first. Reads the base <c>query_stats</c> table because
    /// it projects no text — a read that wanted <c>query_text</c> or <c>query_plan_xml</c> would have to go
    /// through <c>v_query_stats</c> to resolve the #1767 payload dimensions. $1 server_id, $2 database_name,
    /// $3 query_hash, $4/$5 window (naive UTC).
    /// </summary>
    public const string QueryHistorySql = """
        SELECT
            collection_time,
            delta_execution_count,
            delta_worker_time,
            delta_elapsed_time,
            delta_logical_reads,
            delta_logical_writes,
            delta_physical_reads,
            delta_rows,
            delta_spills,
            min_dop,
            max_dop,
            query_plan_hash
        FROM query_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        AND   collection_time >= $4
        AND   collection_time <= $5
        ORDER BY collection_time
        """;

    /* The tier ladder — aliases of DurationTrendRouting since #3653; see the block comment above
       HourlyBucketSecondsSql for why they are aliases and why the names stay. Each member's reasoning (the
       wall-clock age rule and why it is NOT the window's end; the availability and coverage rungs and why
       the rule only ever moves DOWN on a positive measurement; the ninety-minute slack and what it absorbs)
       is on the Storage member it names. What is MCP-specific stays here in one sentence each. */

    /// <summary>
    /// Extra room inside the raw horizon before a window is handed to the aggregate (#2353) —
    /// <see cref="DurationTrendRouting.RawTierMargin"/>, by alias (#3653). Exposed under this name so
    /// DarlingQueryTrendTieringTests pins the boundary rather than restating it.
    /// </summary>
    public static readonly TimeSpan RawTierMargin = DurationTrendRouting.RawTierMargin;

    /// <summary>
    /// Whether the raw per-collection table can serve a window, decided by the age of its OLDEST point measured
    /// from WALL CLOCK (#2353) — <see cref="DurationTrendRouting.ShouldUseRawTier"/>, by delegation (#3653).
    /// <paramref name="nowUtc"/> is the real clock, never the window's end and never an <c>as_of</c> anchor:
    /// retention drops chunks by elapsed time, so an anchored tool's "now" is still this reader's concern
    /// (<see cref="DurationTrendRoute.ResolvedAtUtc"/> carries which clock was consulted).
    /// </summary>
    public static bool ShouldUseRawTier(DateTime startUtc, DateTime nowUtc) =>
        DurationTrendRouting.ShouldUseRawTier(startUtc, nowUtc);

    /// <summary>
    /// The one tier decision every tiered trend read in this file makes (#3541 A2) —
    /// <see cref="DurationTrendRouting.ResolveTier"/>, by delegation (#3653): the age rule, degraded to what
    /// the store HAS (#1664) and to what it has MATERIALIZED (#1759), in that order. The daily tier is not on
    /// this ladder because <see cref="PerformanceMonitor.Common.McpHelpers.MaxHoursBack"/> (seven days) sits
    /// far inside <see cref="TimescaleSupport.HourlyRetentionSpan"/>; a window the hourly tier cannot reach by
    /// AGE cannot be asked for. Pure, so DarlingQueryTrendTieringTests walks its table without a store.
    /// </summary>
    public static RetentionTier ResolveTier(DateTime startUtc, DateTime nowUtc, bool hourlyAvailable, TierCoverage coverage) =>
        DurationTrendRouting.ResolveTier(startUtc, nowUtc, hourlyAvailable, coverage);

    /// <summary>
    /// How far past the requested start the first served point may sit before the answer calls itself
    /// <c>window_truncated</c> — <see cref="DurationTrendRouting.TruncationSlack"/>, by alias (#3653). Lite's twin
    /// (<c>McpQueryTools.TruncationSlack</c>) carries the same ninety minutes, pinned on each side because
    /// neither SKU references the other's assembly.
    /// </summary>
    public static readonly TimeSpan TruncationSlack = DurationTrendRouting.TruncationSlack;

    /// <summary>
    /// What a series actually covers, which is what the caller gets told (#2353) —
    /// <see cref="DurationTrendRouting.DescribeCoverage"/>, by delegation (#3653): the first served point, or
    /// the requested start when nothing came back; <c>Truncated</c> only on a NON-empty series whose head sits
    /// more than <see cref="TruncationSlack"/> after the start. An empty series reports its coverage through
    /// the tool's empty branch instead.
    /// </summary>
    public static (DateTime EffectiveStartUtc, bool Truncated) DescribeCoverage(DateTime? firstPointUtc, DateTime startUtc) =>
        DurationTrendRouting.DescribeCoverage(firstPointUtc, startUtc);

    /// <summary>
    /// Reads a query's history from the tier that can actually serve the window (#2353).
    ///
    /// <para><b>The bug this replaces.</b> This read went to the raw <c>query_stats</c> table only, and the raw
    /// tier of a ROLLED table is physically dropped at <see cref="TimescaleSupport.RawRetentionSpan"/> — four
    /// days — independently of the collector's much longer advertised retention. So a caller asking for 168
    /// hours got whatever had not aged out, under a label saying 168 hours, with nothing in the response
    /// marking the difference.</para>
    ///
    /// <para><b>Tier by the age of the window's oldest point, not by its width</b> — the same rule
    /// <c>ComposeSourceRouter</c> applies, and for the same reason: retention drops chunks by wall-clock age, so
    /// the oldest point is the only thing that decides whether raw can answer. A window that reaches past the
    /// raw horizon is served ENTIRELY from the hourly aggregate rather than stitched, because a series whose
    /// bucket width changes partway is a worse answer than a coarser consistent one.</para>
    ///
    /// <para><paramref name="nowUtc"/> defaults to the real clock and exists so a test can pin the boundary.
    /// It is deliberately NOT defaulted to <paramref name="endUtc"/>: a caller asking for a historical window
    /// would then have its start measured against its own end, which makes a two-hour window from ten days ago
    /// look recent and routes it to a tier that dropped those rows six days earlier.</para>
    ///
    /// <para><paramref name="hourlyAvailable"/> and <paramref name="coverage"/> are the store's measured shape
    /// (#3541 A2 — see <see cref="ResolveTier"/>); the defaults reproduce the age-only #2353 decision for a
    /// caller that has not probed, which is what every pre-existing test of this read exercises.
    /// <paramref name="hourlyRelation"/> (#3653, Q12) is the hourly relation that caller resolved for the window
    /// through <see cref="RollupCoverage.HourlyRelationFor"/> — the interval-honest successor where it reaches
    /// as far back as the legacy — and defaults to the legacy, which every store has.</para>
    /// </summary>
    public static async Task<QueryHistoryResult> GetQueryHistoryAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, string queryHash, DateTime startUtc, DateTime endUtc,
        DateTime? nowUtc = null, bool hourlyAvailable = true, TierCoverage coverage = default, string? hourlyRelation = null,
        CancellationToken cancellationToken = default)
    {
        var tier = ResolveTier(startUtc, nowUtc ?? DateTime.UtcNow, hourlyAvailable, coverage);

        /* #3653 (Q12): the caller that probed the store passes the hourly relation it resolved
           (RollupCoverage.HourlyRelationFor — the interval-honest successor where it reaches as far as the
           legacy for this window); a caller that has not probed reads the legacy, which every store has. */
        var items = tier == RetentionTier.Raw
            ? await ReadQueryHistoryAsync(postgres, QueryHistorySql, serverId, databaseName, queryHash, startUtc, endUtc, cancellationToken)
            : await ReadQueryHistoryAsync(postgres, QueryHistoryHourlySqlFor(hourlyRelation ?? TimescaleSupport.QueryStatsHourlyView), serverId, databaseName, queryHash, startUtc, endUtc, cancellationToken);

        var (effectiveStart, truncated) = DescribeCoverage(items.Count > 0 ? items[0].CollectionTime : null, startUtc);

        return new QueryHistoryResult(
            items,
            DurationTrendRouting.SourceWord(tier),
            effectiveStart,
            truncated);
    }

    private static async Task<List<QueryHistoryPoint>> ReadQueryHistoryAsync(
        NpgsqlDataSource postgres, string sql, int serverId, string databaseName, string queryHash,
        DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var items = new List<QueryHistoryPoint>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddText(command, databaseName);
        DarlingMcpReadParameters.AddText(command, queryHash);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryHistoryPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9)),
                reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10)),
                reader.IsDBNull(11) ? "" : reader.GetString(11)));
        }

        return items;
    }

    /* ─────────── "which nothing is this?" probes for the three windowed trends ─────────── */

    /// <summary>
    /// Whether this server has EVER recorded a memory sample, ignoring any window.
    /// <para>Exists so an empty memory trend can say WHICH kind of nothing it found. "No memory trend data
    /// available" is true both of a quiet window and of a server the collector has never touched, and those
    /// want opposite responses from the caller — widen the window, versus go find out why collection is not
    /// running. Reads <c>v_memory_stats</c>, the same source <see cref="MemoryTrendSql"/> reads, so it can
    /// never report "collected" for rows the trend cannot see. LIMIT 1, so it stops at the first row.</para>
    /// </summary>
    public const string HasAnyMemoryStatSql = """
        SELECT 1
        FROM v_memory_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether this server has EVER recorded a file I/O sample. Reads <c>v_file_io_stats</c>, the
    /// same source <see cref="FileIoLatencyTrendSql"/> reads. See <see cref="HasAnyMemoryStatSql"/>.</summary>
    public const string HasAnyFileIoStatSql = """
        SELECT 1
        FROM v_file_io_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>
    /// Whether this server has EVER recorded a query-stats sample.
    /// <para>Reads the BASE <c>query_stats</c> table, deliberately, because <see cref="QueryDurationTrendSql"/>
    /// does: on a V38+ store <c>v_query_stats</c> is the payload-RESOLVING view, not a passthrough, and the
    /// duration trend projects no text so it never needs it. Probing the view here would be probing a
    /// different relation from the one the read walks — the exact way an existence probe reports the wrong
    /// branch in the case it exists to get right.</para>
    /// </summary>
    public const string HasAnyQueryStatSql = """
        SELECT 1
        FROM query_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Whether this server has EVER recorded a stored-procedure sample. Reads the BASE
    /// <c>procedure_stats</c> table for the same reason the query-stats probe above does — it is what
    /// <see cref="ProcedureDurationTrendSql"/> reads. See <see cref="HasAnyMemoryStatSql"/>.</summary>
    public const string HasAnyProcedureStatSql = """
        SELECT 1
        FROM procedure_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>
    /// Whether this server has EVER recorded a Query Store sample. Reads the BASE
    /// <c>query_store_stats</c> table, the source <see cref="QueryStoreDurationTrendSql"/> walks.
    /// <para>Worth the most of the five, because zero rows here has a cause the others do not: Query Store
    /// can simply be OFF on every database. A server with no Query Store data is not a server with no slow
    /// queries, and the read has to say so rather than return a clean-looking empty series.</para>
    /// </summary>
    public const string HasAnyQueryStoreStatSql = """
        SELECT 1
        FROM query_store_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Runs <see cref="HasAnyMemoryStatSql"/>.</summary>
    public static Task<bool> HasAnyMemoryStatAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnySampleAsync(postgres, HasAnyMemoryStatSql, serverId, cancellationToken);

    /// <summary>Runs <see cref="HasAnyFileIoStatSql"/>.</summary>
    public static Task<bool> HasAnyFileIoStatAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnySampleAsync(postgres, HasAnyFileIoStatSql, serverId, cancellationToken);

    /// <summary>Runs <see cref="HasAnyQueryStatSql"/>.</summary>
    public static Task<bool> HasAnyQueryStatAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnySampleAsync(postgres, HasAnyQueryStatSql, serverId, cancellationToken);

    /// <summary>Runs <see cref="HasAnyProcedureStatSql"/>.</summary>
    public static Task<bool> HasAnyProcedureStatAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnySampleAsync(postgres, HasAnyProcedureStatSql, serverId, cancellationToken);

    /// <summary>Runs <see cref="HasAnyQueryStoreStatSql"/>.</summary>
    public static Task<bool> HasAnyQueryStoreStatAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
        => HasAnySampleAsync(postgres, HasAnyQueryStoreStatSql, serverId, cancellationToken);

    /// <summary>
    /// Whether this server has EVER been sampled as far as a ROUTED duration trend can tell (#3541 A2): the
    /// raw probe, OR a row in the hourly rollup the route can reach. The raw probes above are the right
    /// question for a raw-tier read, and the wrong one on a rolled table: a server disabled a week ago has no
    /// raw rows left (retention dropped them) while its hourly rollup still holds weeks of history, and the
    /// raw probe alone would answer "nothing has EVER been stored" — a false statement, not an incomplete one.
    /// Probed only when the route says the rollup exists, because the view is parse-time-resolved; the view
    /// name comes from <see cref="DurationTrendRoute.HourlyView"/>, which only ever carries a
    /// <see cref="TimescaleSupport"/> view constant.
    /// <para>#3653 A6, decision 4: the NAME probed is <see cref="DurationTrendRoute.ProbeHourlyViewOrDefault"/>
    /// — the successor when the stitch applies (it exists and its floor is at or before the window's end), else
    /// the legacy, same as today. Probing the frozen legacy forever would eventually answer "never sampled" on
    /// a store still being written to past the successor's floor.</para>
    /// </summary>
    public static async Task<bool> HasAnySampleOnRouteAsync(
        NpgsqlDataSource postgres, Task<bool> rawProbe, DurationTrendRoute route, int serverId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(rawProbe);
        ArgumentNullException.ThrowIfNull(route);

        if (await rawProbe)
        {
            return true;
        }

        return route.HourlyAvailable
            && await HasAnySampleAsync(
                postgres, $"SELECT 1 FROM {route.ProbeHourlyViewOrDefault} WHERE server_id = $1 LIMIT 1", serverId, cancellationToken);
    }

    /// <summary>
    /// The baseline discontinuities inside a trend's window (#3653 A5), oldest first, empty when none — the
    /// <c>discontinuities[]</c> block every trend tool in <see cref="DarlingMcpTrendTools"/> (and
    /// <c>get_wait_trend</c>) attaches after its points. Reads flow through here so the trend tools keep one
    /// door to the store; the read itself is the Storage-side
    /// <see cref="BaselineDiscontinuityReader.ReadAsync"/>, which the desktop viewer's Performance Trends
    /// charts also run, so the payload and the chart cannot disagree about where a series re-baselined. The
    /// window is the SAME [start, end] the tool's points were read over — a marker outside the window's points
    /// but inside the window is still a fact about the window (a restart in a quiet hour is why the hour is
    /// quiet), and one just past the window's edge is not this window's to report.
    /// </summary>
    public static Task<IReadOnlyList<PerformanceMonitor.Collectors.BaselineDiscontinuity>> GetBaselineDiscontinuitiesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
        => BaselineDiscontinuityReader.ReadAsync(postgres, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);

    /// <summary>All the probes share one shape: a scalar that is null when no row qualifies.</summary>
    private static async Task<bool> HasAnySampleAsync(
        NpgsqlDataSource postgres, string sql, int serverId, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }
}
