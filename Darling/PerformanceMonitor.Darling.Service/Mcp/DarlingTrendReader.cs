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
/// Dashboard, the shape follows Lite where the SKUs diverge). Every SQL string is byte-identical to the
/// viewer's proven Postgres read for that chart (the viewer already ported Lite's DuckDB SQL), so Darling
/// serves one consistent product. Each SQL string is a public const so Darling.Tests can pin the dialect +
/// columns without a live Postgres.
/// </para>
/// </summary>
internal static class DarlingTrendReader
{
    /* ─────────────────────────── result records ─────────────────────────── */

    /// <summary>One memory-trend point: the four MB metrics per collection (Lite's <c>MemoryTrendPoint</c>
    /// minus its always-0 <c>TotalGrantedMb</c> overlay field, which the tool carries as a Lite parity
    /// placeholder — see <see cref="DarlingMcpTrendTools.GetMemoryTrend"/>).</summary>
    public sealed record MemoryTrendPoint(
        DateTime CollectionTime, double TotalServerMemoryMb, double TargetServerMemoryMb,
        double BufferPoolMb, double PlanCacheMb);

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

    /// <summary>One query-duration / execution-count trend point: the per-second rate (elapsed ms/sec) plus
    /// executions/sec (Lite's <c>QueryTrendPoint</c> shape).</summary>
    public sealed record QueryDurationTrendPoint(DateTime CollectionTime, double Value, long ExecutionCount);

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

    public static async Task<List<QueryDurationTrendPoint>> GetQueryDurationTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<QueryDurationTrendPoint>();
        await using var command = postgres.CreateCommand(QueryDurationTrendSql);
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new QueryDurationTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : (long)Convert.ToDouble(reader.GetValue(2))));
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
    /// <para>The margin keeps a window that lands right on the boundary off the raw tier: the purge is periodic,
    /// so "four days old" is the point where rows may or may not still be there, and preferring the aggregate
    /// there trades resolution for an answer that does not depend on when the purge last ran.</para>
    /// </summary>
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

    public static async Task<QueryHistoryResult> GetQueryHistoryAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, string queryHash, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var useRaw = ShouldUseRawTier(startUtc, endUtc);

        var items = useRaw
            ? await ReadQueryHistoryAsync(postgres, QueryHistorySql, serverId, databaseName, queryHash, startUtc, endUtc, cancellationToken)
            : await ReadQueryHistoryAsync(postgres, QueryHistoryHourlySql, serverId, databaseName, queryHash, startUtc, endUtc, cancellationToken);

        /* What was actually covered, which is what the caller gets told. An empty result says nothing about
           coverage, so the requested start stands rather than being narrowed to a window we cannot describe. */
        var effectiveStart = items.Count > 0 ? items[0].CollectionTime : startUtc;

        return new QueryHistoryResult(
            items,
            useRaw ? "raw" : "hourly",
            effectiveStart,
            Truncated: items.Count > 0 && effectiveStart > startUtc.AddMinutes(90));
    }

    private static async Task<List<QueryHistoryPoint>> ReadQueryHistoryAsync(
        NpgsqlDataSource postgres, string sql, int serverId, string databaseName, string queryHash,
        DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var items = new List<QueryHistoryPoint>();
        await using var command = postgres.CreateCommand(sql);
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
}
