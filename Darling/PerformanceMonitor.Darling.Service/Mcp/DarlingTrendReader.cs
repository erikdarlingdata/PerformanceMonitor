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
/// have no viewer counterpart yet and are documented beside their raw originals. Each SQL string is a public
/// const so Darling.Tests can pin the dialect + columns without a live Postgres.
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
    /// derived over a window spanning the upgrade is only as good as its newest rows.</para></summary>
    public sealed record PerfmonTrendPoint(DateTime CollectionTime, long Value, long DeltaValue, long SampleIntervalSeconds);

    /// <summary>One file I/O-latency-trend point: average read/write latency (stall-ms / op) per collection
    /// for one (database, file) — the tool surfaces database_name + latencies, mirroring Lite's
    /// get_file_io_trend field set (file_name rides the top-10 grouping but is not projected).</summary>
    public sealed record FileIoLatencyTrendPoint(
        DateTime CollectionTime, string DatabaseName, double AvgReadLatencyMs, double AvgWriteLatencyMs);

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
        DateTime CollectionTime, double Value, long ExecutionCount, double ExecutionsPerSecond);

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
    public const string QueryHistoryHourlySql = """
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
        FROM query_stats_hourly
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        AND   bucket >= $4
        AND   bucket <= $5
        ORDER BY bucket
        """;

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
    /// </summary>
    public const string PerfmonTrendSql = """
        SELECT
            collection_time,
            CAST(SUM(cntr_value) AS bigint) AS cntr_value,
            CAST(SUM(delta_cntr_value) AS bigint) AS delta_cntr_value,
            CAST(MAX(sample_interval_seconds) AS bigint) AS sample_interval_seconds
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
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3)));
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
    /// The file I/O latency trend — the viewer's <c>FileIoLatencyTrendSql</c> (Lite's
    /// <c>GetFileIoLatencyTrendAsync</c>), focused to the columns get_file_io_trend surfaces: a
    /// <c>top_files</c> CTE picks the 10 busiest (database, file) pairs by total delta ops over the window,
    /// then per-collection average read/write latency (stall-ms / op, delta-stall sums CAST to double
    /// precision before division) is computed per (collection, database, file). The tool projects
    /// database_name + the two latencies (file_name rides the grouping but is not surfaced, mirroring Lite's
    /// get_file_io_trend field set). $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string FileIoLatencyTrendSql = """
        WITH top_files AS (
            SELECT database_name, file_name
            FROM v_file_io_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   (delta_reads > 0 OR delta_writes > 0)
            GROUP BY database_name, file_name
            ORDER BY SUM(delta_reads + delta_writes) DESC
            LIMIT 10
        )
        SELECT
            f.collection_time,
            f.database_name,
            CASE WHEN SUM(f.delta_reads) > 0
                 THEN SUM(CAST(f.delta_stall_read_ms AS double precision)) / SUM(f.delta_reads)
                 ELSE 0 END AS avg_read_latency_ms,
            CASE WHEN SUM(f.delta_writes) > 0
                 THEN SUM(CAST(f.delta_stall_write_ms AS double precision)) / SUM(f.delta_writes)
                 ELSE 0 END AS avg_write_latency_ms
        FROM v_file_io_stats f
        JOIN top_files tf ON tf.database_name = f.database_name AND tf.file_name = f.file_name
        WHERE f.server_id = $1
        AND   f.collection_time >= $2
        AND   f.collection_time <= $3
        GROUP BY f.collection_time, f.database_name, f.file_name
        ORDER BY f.collection_time, f.database_name, f.file_name
        """;

    public static async Task<List<FileIoLatencyTrendPoint>> GetFileIoLatencyTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<FileIoLatencyTrendPoint>();
        await using var command = postgres.CreateCommand(FileIoLatencyTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new FileIoLatencyTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3)));
        }

        return items;
    }

    /* ─────────────────────────── query duration trend ─────────────────────────── */

    /// <summary>
    /// The query-stats duration trend — the viewer's <c>QueryDurationTrendSql</c> (Lite's
    /// <c>GetQueryDurationTrendAsync</c>): per collection, the summed <c>delta_elapsed_time</c> (→ ms) and
    /// <c>delta_execution_count</c> divided by the seconds since the previous collection (the truncate-then-
    /// diff LAG epoch idiom proven value-identical DuckDB↔Postgres) for an elapsed-ms/sec + executions/sec
    /// rate. The first row's LAG is NULL → interval NULL → the CASE yields 0 (Lite's behaviour). Reads the
    /// base <c>query_stats</c> table because it projects no text — a read that wanted <c>query_text</c> or
    /// <c>query_plan_xml</c> would have to go through <c>v_query_stats</c> to resolve the #1767 payload
    /// dimensions. Summed bigints come back as numeric, so the reads Convert tolerantly. $1 server_id,
    /// $2/$3 window (naive UTC).
    /// </summary>
    public const string QueryDurationTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
                SUM(delta_execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>
    /// The seconds in one hourly-rollup bucket, as the SQL literal the hourly-tier duration trends divide by.
    /// A string because a constant interpolated string admits only string constants; pinned equal to
    /// <see cref="TimescaleSupport.HourlyBucket"/> by DarlingMcpTrendToolsTests so the two cannot drift.
    /// </summary>
    public const string HourlyBucketSecondsSql = "3600.0";

    /// <summary>
    /// The hourly-tier twin of <see cref="QueryDurationTrendSql"/> (#3541 A2): the same two per-second rates,
    /// read from the <c>query_stats_hourly</c> continuous aggregate for windows whose oldest point the raw
    /// tier no longer holds. The rollup carries <c>elapsed_time_sum</c> and <c>execution_count_sum</c> per
    /// (server, database, query_hash, sql_handle, hour); summing those across the identity columns per bucket
    /// is exactly what the raw read's <c>GROUP BY collection_time</c> does one grain finer, so the two tiers
    /// answer the same question at different resolution. <c>bucket</c> is projected as <c>collection_time</c>
    /// so the point shape does not change underneath a caller that got a raw answer last time.
    ///
    /// <para><b>The denominator is the bucket width, not a LAG.</b> The raw read has to recompute its interval
    /// from the gap to the previous collection because a per-sweep row carries no shared interval (the
    /// measurement lane's A11a); an hour bucket's width is KNOWN, so this read divides by
    /// <see cref="HourlyBucketSecondsSql"/> and every bucket has a real denominator — the LAG idiom's first
    /// point, whose interval is NULL and whose rate is therefore a fabricated 0, does not exist here. The
    /// trade is stated in the payload rather than hidden: an hour the collector covered only partly (a
    /// service restart mid-hour, a gap past the delta policy) reads LOW, never high, because its summed work is
    /// still spread over the full 3,600 seconds.</para>
    ///
    /// <para>Materialized-only, like every rollup here (#1759): the current hour is never materialized
    /// (<see cref="TimescaleSupport.HourlyRefreshScheduleInterval"/> is the end_offset) and the previous one
    /// lands on the next refresh, so this series trails the clock by up to two hours. $1 server_id, $2/$3
    /// window (naive UTC).</para>
    /// </summary>
    public const string QueryDurationTrendHourlySql = $"""
        SELECT
            bucket AS collection_time,
            SUM(elapsed_time_sum) / 1000.0 / {HourlyBucketSecondsSql} AS elapsed_ms_per_second,
            CAST(SUM(execution_count_sum) AS DOUBLE PRECISION) / {HourlyBucketSecondsSql} AS executions_per_second
        FROM {TimescaleSupport.QueryStatsHourlyView}
        WHERE server_id = $1
        AND   bucket >= $2
        AND   bucket <= $3
        GROUP BY bucket
        ORDER BY bucket
        """;

    /// <summary>
    /// The hourly-tier twin of <see cref="ProcedureDurationTrendSql"/> (#3541 A2), over
    /// <c>procedure_stats_hourly</c>. Same shape and same bucket-width denominator as
    /// <see cref="QueryDurationTrendHourlySql"/>; see there for why. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string ProcedureDurationTrendHourlySql = $"""
        SELECT
            bucket AS collection_time,
            SUM(elapsed_time_sum) / 1000.0 / {HourlyBucketSecondsSql} AS elapsed_ms_per_second,
            CAST(SUM(execution_count_sum) AS DOUBLE PRECISION) / {HourlyBucketSecondsSql} AS executions_per_second
        FROM {TimescaleSupport.ProcedureStatsHourlyView}
        WHERE server_id = $1
        AND   bucket >= $2
        AND   bucket <= $3
        GROUP BY bucket
        ORDER BY bucket
        """;

    /// <summary>
    /// Which tier one unkeyed duration trend read serves from, and the evidence the choice rests on (#3541 A2)
    /// — the get_query_trend routing (<see cref="QueryHistoryResult"/>, #2353) generalized to the reads that
    /// have no query key.
    ///
    /// <para><see cref="RawTable"/> and <see cref="HourlyView"/> are the pair this trend reads at each tier;
    /// <see cref="Relation"/> is the one <see cref="Tier"/> picked, so a payload or an empty message can say
    /// WHAT WAS READ rather than what was asked for. <see cref="HourlyAvailable"/> and <see cref="Coverage"/>
    /// are kept on the route so the empty branch can say why the tier it read holds nothing — a rollup that
    /// has materialized nothing and a rollup whose floor sits above the window's start are different facts
    /// with different remedies, and both differ from a quiet server.</para>
    ///
    /// <para><see cref="RawRetentionApplies"/> is false on a store with no rollups at all — plain PostgreSQL,
    /// or a failed availability probe — where no retention policy ever drops raw, so raw holds the complete
    /// answer and "quiet, widen the window" is honest there (#1665). On a TimescaleDB store it is true, and
    /// the empty branch has to consider that the rows were DROPPED, not absent.</para>
    ///
    /// <para><see cref="ResolvedAtUtc"/> is the wall clock the age decision was measured against. It rides on
    /// the route so the tool that consumes it never names the clock itself: an <c>as_of</c>-anchored tool's
    /// only "now" is the anchor it resolved (AsOfWindowAnchorTests pins that as an absolute), and retention's
    /// clock — which is NOT the anchor, see <see cref="ShouldUseRawTier"/> — is this reader's concern.</para>
    /// </summary>
    public sealed record DurationTrendRoute(
        RetentionTier Tier, string RawTable, string HourlyView, bool HourlyAvailable, TierCoverage Coverage,
        bool RawRetentionApplies, DateTime ResolvedAtUtc)
    {
        /// <summary>The payload's <c>source</c> word: <c>raw</c> or <c>hourly</c>, get_query_trend's vocabulary.</summary>
        public string Source => Tier == RetentionTier.Raw ? "raw" : "hourly";

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
    /// <c>truncated</c>).
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
        DateTime startUtc, RollupAvailability rollups, RollupCoverage coverage, DateTime? nowUtc = null)
        => ResolveDurationTrendRoute(
            "query_stats", TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView,
            rollups.QueryGrainHourly, startUtc, nowUtc ?? DateTime.UtcNow, rollups, coverage);

    /// <summary>The procedure-stats twin of <see cref="ResolveQueryDurationTrendRoute"/>.</summary>
    public static DurationTrendRoute ResolveProcedureDurationTrendRoute(
        DateTime startUtc, RollupAvailability rollups, RollupCoverage coverage, DateTime? nowUtc = null)
        => ResolveDurationTrendRoute(
            "procedure_stats", TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView,
            rollups.ProcedureGrainHourly, startUtc, nowUtc ?? DateTime.UtcNow, rollups, coverage);

    private static DurationTrendRoute ResolveDurationTrendRoute(
        string rawTable, string hourlyView, string dailyView, bool hourlyAvailable,
        DateTime startUtc, DateTime nowUtc, RollupAvailability rollups, RollupCoverage coverage)
    {
        ArgumentNullException.ThrowIfNull(coverage);

        var tierCoverage = coverage.For(hourlyView, dailyView);
        return new DurationTrendRoute(
            ResolveTier(startUtc, nowUtc, hourlyAvailable, tierCoverage),
            rawTable, hourlyView, hourlyAvailable, tierCoverage,
            RawRetentionApplies: rollups != RollupAvailability.None,
            ResolvedAtUtc: nowUtc);
    }

    /// <summary>
    /// Runs the query-stats duration trend down <paramref name="route"/> (#3541 A2): the raw read for a
    /// window raw can serve, the hourly twin otherwise. Coverage is described by <see cref="DescribeCoverage"/>
    /// so the payload can say what it served.
    /// </summary>
    public static Task<DurationTrendResult> GetQueryDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, DurationTrendRoute route,
        CancellationToken cancellationToken = default)
        => ReadRoutedDurationTrendAsync(
            QueryDurationTrendSql, QueryDurationTrendHourlySql, postgres, serverId, startUtc, endUtc, route, cancellationToken);

    /* --------------------- procedure + Query Store duration trends (#2484) --------------------- */

    /// <summary>
    /// The procedure-stats duration trend - the viewer's <c>ProcedureDurationTrendSql</c>, verbatim apart
    /// from the database filter the MCP copy of its query-stats twin already drops.
    /// <para>Not a duplicate of the query-stats trend, and the difference is the point: <c>query_stats</c>
    /// attributes a procedure's work to the individual statements inside it, so a procedure that got slower
    /// shows up smeared across however many statements it runs. This charges the whole call to the
    /// procedure. When both are available, the pair answers "did ad-hoc SQL regress, or did a procedure?" -
    /// which one series alone never can. $1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string ProcedureDurationTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
                SUM(delta_execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds ELSE 0 END AS elapsed_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

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
    /// hour. $1 server_id, $2/$3 window (naive UTC).</para>
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
            CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds ELSE 0 END AS duration_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS executions_per_second
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
        CancellationToken cancellationToken = default)
        => ReadRoutedDurationTrendAsync(
            ProcedureDurationTrendSql, ProcedureDurationTrendHourlySql, postgres, serverId, startUtc, endUtc, route, cancellationToken);

    /// <summary>
    /// The shared body of the two routed reads: pick the tier's SQL, read the three-column point shape, and
    /// describe what came back. One method so the query and procedure trends cannot drift in how they
    /// route, read, or describe coverage.
    /// </summary>
    private static async Task<DurationTrendResult> ReadRoutedDurationTrendAsync(
        string rawSql, string hourlySql, NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        DurationTrendRoute route, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(route);

        var points = await ReadDurationTrendAsync(
            route.Tier == RetentionTier.Raw ? rawSql : hourlySql, postgres, serverId, startUtc, endUtc, cancellationToken);
        var (effectiveStart, truncated) = DescribeCoverage(points.Count > 0 ? points[0].CollectionTime : null, startUtc);
        return new DurationTrendResult(points, route, effectiveStart, truncated);
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
            var executionsPerSecond = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2));
            items.Add(new QueryDurationTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                (long)executionsPerSecond,
                executionsPerSecond));
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

    /// <summary>
    /// Extra room inside the raw horizon before a window is handed to the aggregate (#2353). The purge is
    /// periodic, so a window whose oldest point sits exactly on the four-day line may or may not still find its
    /// rows depending on when the purge last ran; preferring the aggregate there trades resolution for an answer
    /// that does not change with the purge schedule. Exposed so a test pins the boundary rather than restating it.
    /// </summary>
    public static readonly TimeSpan RawTierMargin = TimeSpan.FromHours(1);

    /// <summary>
    /// Whether the raw per-collection table can serve a window, decided by the age of its OLDEST point measured
    /// from WALL CLOCK (#2353).
    ///
    /// <para><b>The second parameter is <c>now</c>, not the window's end, and the difference is not cosmetic.</b>
    /// Retention drops chunks by actual elapsed time, so what decides whether raw still holds a row is how long
    /// ago that row happened — never where it sits inside the requested window. Measuring the start against the
    /// END would call a two-hour window from ten days ago "recent", because it is recent relative to its own
    /// end, and route it to a tier that dropped those rows six days earlier. <c>ComposeSourceRouter</c> takes a
    /// caller-supplied <c>now</c> for exactly this reason.</para>
    ///
    /// <para>Pure so the boundary is unit-testable without a store — the tiering decision is the whole of this
    /// fix, and a rule that can only be exercised against a live four-day-old hypertable is a rule nobody
    /// re-checks. Width is deliberately not consulted: a narrow window sitting entirely in last week is exactly
    /// as unservable from raw as a wide one.</para>
    /// </summary>
    public static bool ShouldUseRawTier(DateTime startUtc, DateTime nowUtc) =>
        startUtc >= nowUtc - TimescaleSupport.RawRetentionSpan + RawTierMargin;

    /// <summary>
    /// The one tier decision every tiered trend read in this file makes (#3541 A2): get_query_trend's age rule
    /// (<see cref="ShouldUseRawTier"/>), degraded to what the store HAS and to what it has MATERIALIZED, in
    /// that order — the same three-rung ladder <see cref="RetentionTierRouter"/> runs for the viewer's tabs,
    /// built from the same Storage primitives, with this file's raw margin at the bottom rung instead of the
    /// router's one-day one (that margin is #2353's deliberate trade of resolution for purge-independence, and
    /// widening it to a day would push every 3-to-4-day window onto the hourly tier for nothing).
    ///
    /// <para><b>Availability (#1664).</b> A plain-PostgreSQL store has no continuous aggregates, and a
    /// relation named in a statement is resolved at PARSE time — so an age-only rule sent a 168-hour window on
    /// such a store to a view that does not exist and the tool answered 42P01. Raw is the right answer there
    /// anyway: without the extension nothing ever drops raw, so it holds the complete window.</para>
    ///
    /// <para><b>Coverage (#1759).</b> A rollup created <c>WITH NO DATA</c> serves only what a refresh or a
    /// backfill has materialized, and its refresh reaches back one <see cref="TimescaleSupport.HourlyRefreshStartOffset"/>
    /// from creation — so on a store that pre-existed its rollups and was never backfilled the hourly tier is
    /// SHALLOWER than raw, whose purge the arming gate holds paused until the rollup covers it. Routing such a
    /// window to the rollup by age alone returned EMPTY while raw held every row. The rule is comparative and
    /// only ever moves DOWN on a positive measurement: raw wins only when it is measured to reach further back
    /// than the rollup's floor (<see cref="TierCoverage.ReachesFurtherBack"/>); unmeasured coverage (nulls) is
    /// inert and the age + availability answer stands. Hourly is never abandoned merely because the window
    /// starts below its floor — on a healthy store raw keeps four days against the rollup's ninety, and
    /// dropping to raw there would return LESS. The part of the window the rollup has not reached is the
    /// payload's job to disclose (<c>effective_start</c>, <c>truncated</c>), not routing's job to hide.</para>
    ///
    /// <para>Pure, for the same reason <see cref="ShouldUseRawTier"/> is: the tiering decision is the whole of
    /// the fix, and DarlingQueryTrendTieringTests walks its table without a store. The daily tier is not on
    /// this ladder because <see cref="PerformanceMonitor.Common.McpHelpers.MaxHoursBack"/> (seven days)
    /// sits far inside <see cref="TimescaleSupport.HourlyRetentionSpan"/>; a window the hourly tier cannot
    /// reach by AGE cannot be asked for.</para>
    /// </summary>
    public static RetentionTier ResolveTier(DateTime startUtc, DateTime nowUtc, bool hourlyAvailable, TierCoverage coverage)
    {
        if (ShouldUseRawTier(startUtc, nowUtc) || !hourlyAvailable)
        {
            return RetentionTier.Raw;
        }

        if (TierCoverage.Covers(coverage.HourlyFloorUtc, startUtc))
        {
            return RetentionTier.Hourly;
        }

        return TierCoverage.ReachesFurtherBack(coverage.RawOldestUtc, coverage.HourlyFloorUtc)
            ? RetentionTier.Raw
            : RetentionTier.Hourly;
    }

    /// <summary>
    /// How far past the requested start the first served point may sit before the answer calls itself
    /// <c>truncated</c>. Ninety minutes: an hourly bucket can begin up to an hour after a window start that
    /// falls mid-hour (<c>bucket &gt;= $start</c> excludes the bucket the start falls inside), and a raw series
    /// legitimately opens a collection cadence or two late; anything past that means the tier did not hold the
    /// window's head. Extracted from #2353's inline literal so the four tiered reads share one boundary.
    /// </summary>
    public static readonly TimeSpan TruncationSlack = TimeSpan.FromMinutes(90);

    /// <summary>
    /// What a series actually covers, which is what the caller gets told (#2353): the first served point, or
    /// the requested start when nothing came back — an empty result says nothing about coverage, so the
    /// requested start stands rather than being narrowed to a window nobody can describe. <c>Truncated</c> is
    /// true only on a NON-empty series whose head sits more than <see cref="TruncationSlack"/> after the
    /// requested start; an empty series reports its coverage through the empty branch's message instead.
    /// </summary>
    public static (DateTime EffectiveStartUtc, bool Truncated) DescribeCoverage(DateTime? firstPointUtc, DateTime startUtc)
        => firstPointUtc is DateTime first
            ? (first, first > startUtc + TruncationSlack)
            : (startUtc, false);

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
    /// caller that has not probed, which is what every pre-existing test of this read exercises.</para>
    /// </summary>
    public static async Task<QueryHistoryResult> GetQueryHistoryAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, string queryHash, DateTime startUtc, DateTime endUtc,
        DateTime? nowUtc = null, bool hourlyAvailable = true, TierCoverage coverage = default, CancellationToken cancellationToken = default)
    {
        var tier = ResolveTier(startUtc, nowUtc ?? DateTime.UtcNow, hourlyAvailable, coverage);

        var items = tier == RetentionTier.Raw
            ? await ReadQueryHistoryAsync(postgres, QueryHistorySql, serverId, databaseName, queryHash, startUtc, endUtc, cancellationToken)
            : await ReadQueryHistoryAsync(postgres, QueryHistoryHourlySql, serverId, databaseName, queryHash, startUtc, endUtc, cancellationToken);

        var (effectiveStart, truncated) = DescribeCoverage(items.Count > 0 ? items[0].CollectionTime : null, startUtc);

        return new QueryHistoryResult(
            items,
            tier == RetentionTier.Raw ? "raw" : "hourly",
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
                postgres, $"SELECT 1 FROM {route.HourlyView} WHERE server_id = $1 LIMIT 1", serverId, cancellationToken);
    }

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
