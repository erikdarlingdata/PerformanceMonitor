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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the core data-read MCP tools (<see cref="DarlingMcpDataTools"/>) — the
/// SAME collected data the viewer's <c>ViewerDataService.*</c> partials read, adapted here so the
/// MCP host never references the WPF viewer project. Each read is a STORED read (no live
/// monitored-server hit), keyed by <c>server_id</c> and windowed on the naive-UTC
/// <c>collection_time</c> prefix — the reliable clock every Darling read windows on, sent
/// <c>Kind=Unspecified</c> because the store's columns are <c>timestamp without time zone</c>
/// (a <c>Kind=Utc</c> DateTime maps to timestamptz and throws since Npgsql 6.0).
///
/// <para>
/// The result shapes mirror Lite's <c>Mcp*Tools</c> field-for-field (the same tool names the
/// Dashboard and Lite expose), because Darling's store is Lite's DuckDB schema column-for-column
/// (<see cref="Storage.PgSchemaGenerator"/>) — the same reason the existing
/// <see cref="DarlingMcpTools"/> mirror Lite's analysis tools. Every SQL string is a public const so
/// Darling.Tests can pin the dialect + columns without a live Postgres. Where a view exists the read
/// uses the <c>v_*</c> passthrough (the Darling-analysis convention); <c>server_properties</c> and
/// <c>procedure_stats</c> have NO <c>v_*</c> view (they are not in
/// <see cref="Storage.PgSchemaGenerator.AllPassthroughViews"/>) so those reads hit the base table, exactly
/// like the viewer's twins and the merged <see cref="DarlingStoredPlanReader"/>. Any read that projects
/// <c>query_text</c> or <c>query_plan_xml</c> MUST go through <c>v_query_stats</c>, which resolves the
/// #1767 payload dimensions — the base table's inline columns are NULL on every row written since.
/// </para>
/// </summary>
internal static class DarlingDataReader
{
    /* ─────────────────────────── result records ─────────────────────────── */

    /// <summary>One raw CPU ring-buffer sample: sample_time (de-skewed to naive UTC) + the SQL and
    /// other-process CPU percentages. The tool buckets these to 1-minute averages.</summary>
    public sealed record CpuSample(DateTime SampleTime, int SqlServerCpu, int OtherProcessCpu);

    /// <summary>One aggregated wait type over the window — summed deltas; resource / signal-pct are
    /// computed by the tool (mirroring Lite's <c>WaitStatsRow</c>).</summary>
    public sealed record WaitStatRow(string WaitType, long TotalWaitingTasks, long TotalWaitTimeMs, long TotalSignalWaitTimeMs);

    /// <summary>One point of a single wait type's per-second trend.</summary>
    public sealed record WaitTrendPoint(DateTime CollectionTime, double WaitTimeMsPerSecond, double SignalWaitTimeMsPerSecond);

    /// <summary>The latest memory_stats snapshot (Lite's <c>MemoryStatsRow</c>); utilization is
    /// computed by the tool.</summary>
    public sealed record MemoryStatsRow(
        DateTime CollectionTime, double TotalPhysicalMemoryMb, double AvailablePhysicalMemoryMb,
        double TotalPageFileMb, double AvailablePageFileMb, string SystemMemoryState, string SqlMemoryModel,
        double TargetServerMemoryMb, double TotalServerMemoryMb, double BufferPoolMb, double PlanCacheMb);

    /// <summary>One memory clerk's footprint at the latest snapshot.</summary>
    public sealed record MemoryClerkRow(string ClerkType, double MemoryMb);

    /* #2484: the two series behind the viewer's Current Waits tab. Both read waiting_tasks, which the
       snapshot reads already expose row-by-row -- get_waiting_tasks answers "what is waiting now" and
       never "was it worse an hour ago", which is the question that decides whether anything is wrong. */
    public sealed record WaitingTaskTrendRow(DateTime CollectionTime, string WaitType, long TotalWaitMs);

    public sealed record BlockedSessionTrendRow(DateTime CollectionTime, string DatabaseName, long BlockedCount);

    /*
        One row of the RAW per-run collection log, as opposed to the CollectorHealth rollup above.
        The rollup answers "is this collector healthy over seven days"; this answers "what happened
        on each run", which is the question an operator actually has when collection looks wrong and
        the rollup says HEALTHY. Durations are split the way the collectors report them -- total, the
        part spent on the monitored server, and the part spent writing to the store -- because a
        collector that is slow because the target is slow needs a different fix from one that is slow
        because the store is.
    */
    public sealed record CollectionLogEntry(
        string CollectorName,
        DateTime CollectionTime,
        double? DurationMs,
        double? SqlDurationMs,
        double? StoreDurationMs,
        long? RowsCollected,
        string? Status,
        string? ErrorMessage);

    /// <summary>One database file's latest I/O snapshot; avg latency is computed by the tool.</summary>
    public sealed record FileIoRow(
        string DatabaseName, string FileName, string FileType, string PhysicalName, double SizeMb,
        long DeltaReads, long DeltaWrites, long DeltaReadBytes, long DeltaWriteBytes,
        long DeltaStallReadMs, long DeltaStallWriteMs);

    /// <summary>One tempdb space-usage sample over the window.</summary>
    public sealed record TempDbSample(
        DateTime CollectionTime, double UserObjectReservedMb, double InternalObjectReservedMb,
        double VersionStoreReservedMb, double TotalReservedMb, double UnallocatedMb,
        long TotalSessionsUsingTempDb, int TopSessionId, double TopSessionTempDbMb);

    /// <summary>One perfmon counter at the latest snapshot.</summary>
    public sealed record PerfmonRow(string CounterName, string InstanceName, long Value, long DeltaValue);

    /// <summary>One (database, query_hash) group's summed query-stats deltas over the window. Time
    /// metrics are in microseconds (converted to ms by the tool, matching Lite).</summary>
    public sealed record TopQueryRow(
        string DatabaseName, string QueryHash,
        /* #2012 stage 2: the statement's host object (schema.name), part of the GROUPING key — proc-hosted
           INSERT...EXEC callers sharing a hash now land in separate rows; null = ad-hoc/prepared, whose
           literal-collapse grouping is unchanged. */
        string? HostObjectName,
        string QueryPlanHash, string SqlHandle, string PlanHandle,
        long TotalExecutions, long TotalCpuUs, long TotalElapsedUs, long TotalLogicalReads, long TotalLogicalWrites,
        long TotalPhysicalReads, long TotalRows, long TotalSpills, int MinDop, int MaxDop,
        long MinCpuUs, long MaxCpuUs, long MinElapsedUs, long MaxElapsedUs, string QueryText,
        /* #2012: distinct statement texts merged into this group; with stage 2's host-object split this
           flags the remaining ad-hoc literal blends (proc-hosted groups converge to 1). */
        long DistinctTexts,
        /* #2235: how many DISTINCT query_hash values this row rolled up. Always 1 in the default
           per-hash grouping — it is only interesting under host-object rollup, where it IS the finding:
           a proc whose dynamic SQL fragments across 21 hashes reports 21 here, which is the number that
           explains why top-N-by-hash could never surface it. */
        long DistinctQueryHashes = 1);

    /// <summary>One (database, schema, object) group's summed procedure-stats deltas over the window.</summary>
    public sealed record TopProcedureRow(
        string DatabaseName, string SchemaName, string ObjectName, string ObjectType, string SqlHandle, string PlanHandle,
        long TotalExecutions, long TotalCpuUs, long TotalElapsedUs, long TotalLogicalReads, long TotalLogicalWrites,
        long TotalPhysicalReads, long TotalSpills, long MinCpuUs, long MaxCpuUs, long MinElapsedUs, long MaxElapsedUs);

    /// <summary>
    /// One Query Store (database, query_id, plan_id, query_hash, replica_role) group's interval averages.
    /// <c>ReplicaRole</c> is part of the grouping key, not a decoration: on SQL 2022+ with Query Store for
    /// secondary replicas the primary holds one shared Query Store carrying every replica's rows, so
    /// omitting it would report primary and secondary workload blended into one row. NULL when the server
    /// did not attribute the row (pre-2022, or a 2022 standalone).
    /// </summary>
    public sealed record QueryStoreRow(
        string DatabaseName, long QueryId, long PlanId, string QueryHash, string QueryPlanHash,
        long TotalExecutions, double AvgDurationMs, double AvgCpuTimeMs, double AvgLogicalReads,
        double AvgLogicalWrites, double AvgPhysicalReads, double AvgRowcount, DateTime? LastExecutionTime, string QueryText,
        string? ReplicaRole);

    /// <summary>One server-list entry — the registry row plus its newest collection instant (drives the
    /// freshness-derived status the tool assigns; the viewer has no live ping either).</summary>
    public sealed record ServerListRow(int ServerId, string ServerName, string? DisplayName, int? SqlMajorVersion, DateTime? LastCollection);

    /// <summary>The latest server_properties snapshot (Lite's <c>ServerPropertiesRow</c>).</summary>
    public sealed record ServerPropertiesReadRow(
        DateTime CollectionTime, string Edition, string ProductVersion, string ProductLevel, string? ProductUpdateLevel,
        int EngineEdition, int CpuCount, int HyperthreadRatio, long PhysicalMemoryMb, int SocketCount, int CoresPerSocket,
        bool IsHadrEnabled, bool IsClustered, string? EnterpriseFeatures, string? ServiceObjective);

    /* ─────────────────────────── CPU ─────────────────────────── */

    /// <summary>
    /// Raw per-sample CPU (the viewer's <c>CpuUtilizationSql</c>): every ring-buffer sample since the
    /// window start, with <c>sample_time</c> de-skewed from the monitored server's LOCAL wall clock to
    /// naive UTC by subtracting the per-batch UTC offset (#1262). Windows on <c>collection_time</c> (the
    /// reliable naive-UTC clock, not the server-local sample_time). Reads the base
    /// <c>cpu_utilization_stats</c> table (the de-skew window function needs collection_time alongside
    /// sample_time). $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string CpuUtilizationSql = """
        SELECT
            sample_time
                - INTERVAL '15 minutes'
                  * ROUND(EXTRACT(EPOCH FROM (
                        MAX(sample_time) OVER (PARTITION BY server_id, collection_time) - collection_time
                    )) / 900.0)::double precision AS sample_time,
            sqlserver_cpu_utilization,
            other_process_cpu_utilization
        FROM cpu_utilization_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY sample_time
        """;

    public static async Task<List<CpuSample>> GetCpuUtilizationAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<CpuSample>();
        await using var command = postgres.CreateCommand(CpuUtilizationSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new CpuSample(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2)));
        }

        return samples;
    }

    public sealed record CpuWindowAggregate(int SampleCount, DateTime? FirstSample, DateTime? LastSample, double? AvgSqlCpuPercent);

    /// <summary>
    /// The attributed-CPU denominator's pieces (#2320): sample count, coverage bounds, and average SQL
    /// CPU% over the window. Windowed on collection_time — the SAME bounds the top-queries/procedures
    /// rankings use — so numerator and denominator share collection gaps; sample_time skew is irrelevant
    /// to an average. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string CpuWindowAggregateSql = """
        SELECT
            COUNT(*),
            MIN(collection_time),
            MAX(collection_time),
            AVG(sqlserver_cpu_utilization)::double precision
        FROM cpu_utilization_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        """;

    public static async Task<CpuWindowAggregate> GetCpuWindowAggregateAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(CpuWindowAggregateSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CpuWindowAggregate(0, null, null, null);
        }

        return new CpuWindowAggregate(
            reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0), System.Globalization.CultureInfo.InvariantCulture),
            reader.IsDBNull(1) ? null : reader.GetDateTime(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            reader.IsDBNull(3) ? null : reader.GetDouble(3));
    }

    /* ─────────────────────────── wait stats ─────────────────────────── */

    /// <summary>
    /// Aggregated wait stats over the window — Lite's <c>GetWaitStatsAsync</c>: summed deltas per
    /// wait_type, heaviest first. The SUMs CAST to bigint for the typed GetInt64 reader (Postgres
    /// <c>SUM(bigint)</c> is numeric). Lite's per-user IgnoredWaitTypes exclusion is dropped (headless
    /// has no per-user ignore config — the viewer's wait reads drop it the same way). $1 server_id, $2/$3
    /// window (naive UTC).
    /// </summary>
    public const string WaitStatsSql = """
        SELECT
            wait_type,
            CAST(SUM(delta_waiting_tasks) AS bigint) AS total_waiting_tasks,
            CAST(SUM(delta_wait_time_ms) AS bigint) AS total_wait_time_ms,
            CAST(SUM(delta_signal_wait_time_ms) AS bigint) AS total_signal_wait_time_ms
        FROM v_wait_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY wait_type
        ORDER BY SUM(delta_wait_time_ms) DESC
        LIMIT 50
        """;

    public static async Task<List<WaitStatRow>> GetWaitStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<WaitStatRow>();
        await using var command = postgres.CreateCommand(WaitStatsSql);
        AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new WaitStatRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3)));
        }

        return rows;
    }

    /// <summary>
    /// The distinct wait types collected over the window, heaviest first — feeds the get_wait_trend
    /// "not_collected" hint (Lite's <c>GetDistinctWaitTypesAsync</c>). $1 server_id, $2/$3 window.
    /// </summary>
    public const string DistinctWaitTypesSql = """
        SELECT wait_type
        FROM v_wait_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY wait_type
        ORDER BY SUM(delta_wait_time_ms) DESC
        """;

    public static async Task<List<string>> GetDistinctWaitTypesAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<string>();
        await using var command = postgres.CreateCommand(DistinctWaitTypesSql);
        AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        return items;
    }

    /// <summary>
    /// Whether this server has EVER recorded a wait sample, ignoring any window.
    /// <para>Lets an empty get_wait_types say WHICH kind of nothing it found. "No wait types in the last N
    /// hours" is true both of a quiet window and of a server nothing has been stored for, and those want
    /// opposite responses — widen the window, versus go find out why collection is not running. Reads
    /// <c>v_wait_stats</c>, the same source <see cref="DistinctWaitTypesSql"/> reads, so it can never
    /// report "collected" for rows the read cannot see. LIMIT 1, so it stops at the first row.</para>
    /// </summary>
    public const string HasAnyWaitStatSql = """
        SELECT 1
        FROM v_wait_stats
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Runs <see cref="HasAnyWaitStatSql"/>.</summary>
    public static async Task<bool> HasAnyWaitStatAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HasAnyWaitStatSql);
        AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// A single wait type's per-second trend — Lite's <c>GetWaitStatsTrendAsync</c>: the interval rate is
    /// this collection's delta divided by the seconds since the previous collection (a <c>LAG</c> over the
    /// truncate-then-diff epoch idiom proven value-identical DuckDB↔Postgres). $1 server_id, $2 wait_type,
    /// $3/$4 window (naive UTC).
    /// </summary>
    public const string WaitTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                delta_wait_time_ms,
                delta_signal_wait_time_ms,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   wait_type = $2
            AND   collection_time >= $3
            AND   collection_time <= $4
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS signal_wait_time_ms_per_second
        FROM raw
        ORDER BY collection_time
        """;

    public static async Task<List<WaitTrendPoint>> GetWaitTrendAsync(
        NpgsqlDataSource postgres, int serverId, string waitType, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<WaitTrendPoint>();
        await using var command = postgres.CreateCommand(WaitTrendSql);
        AddInt(command, serverId);
        AddText(command, waitType);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WaitTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        return items;
    }

    /* ─────────────────────────── memory ─────────────────────────── */

    /// <summary>
    /// The latest memory_stats snapshot — the viewer's <c>LatestMemoryStatsSql</c>. The eight MB metrics
    /// are <c>numeric(18,2)</c> and CAST to double precision for the typed reader. $1 server_id.
    /// </summary>
    public const string LatestMemoryStatsSql = """
        SELECT
            collection_time,
            CAST(total_physical_memory_mb AS double precision),
            CAST(available_physical_memory_mb AS double precision),
            CAST(total_page_file_mb AS double precision),
            CAST(available_page_file_mb AS double precision),
            system_memory_state,
            sql_memory_model,
            CAST(target_server_memory_mb AS double precision),
            CAST(total_server_memory_mb AS double precision),
            CAST(buffer_pool_mb AS double precision),
            CAST(plan_cache_mb AS double precision)
        FROM v_memory_stats
        WHERE server_id = $1
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    public static async Task<MemoryStatsRow?> GetLatestMemoryStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(LatestMemoryStatsSql);
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new MemoryStatsRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
            reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
            reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
            reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
            reader.IsDBNull(5) ? "" : reader.GetString(5),
            reader.IsDBNull(6) ? "" : reader.GetString(6),
            reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
            reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
            reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
            reader.IsDBNull(10) ? 0 : reader.GetDouble(10));
    }

    /// <summary>
    /// The latest memory-clerk breakdown — Lite's <c>GetLatestMemoryClerksAsync</c>: every clerk at the
    /// newest collection, heaviest first. memory_mb is <c>numeric(18,2)</c> → double precision. $1 server_id.
    /// </summary>
    public const string LatestMemoryClerksSql = """
        SELECT clerk_type, CAST(memory_mb AS double precision)
        FROM v_memory_clerks
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_memory_clerks WHERE server_id = $1)
        ORDER BY memory_mb DESC
        """;

    public static async Task<List<MemoryClerkRow>> GetLatestMemoryClerksAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<MemoryClerkRow>();
        await using var command = postgres.CreateCommand(LatestMemoryClerksSql);
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MemoryClerkRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1)));
        }

        return rows;
    }

    /* ─────────────────────────── file I/O ─────────────────────────── */

    /// <summary>
    /// The latest file-I/O snapshot per database file — Lite's <c>GetLatestFileIoStatsAsync</c>, ordered
    /// by total stall descending; avg latency (stall/op) is computed by the tool. size_mb is
    /// <c>numeric</c> → double precision; the delta columns are bigint. $1 server_id.
    /// </summary>
    public const string LatestFileIoStatsSql = """
        SELECT
            database_name,
            file_name,
            file_type,
            physical_name,
            CAST(size_mb AS double precision),
            delta_reads,
            delta_writes,
            delta_read_bytes,
            delta_write_bytes,
            delta_stall_read_ms,
            delta_stall_write_ms
        FROM v_file_io_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_file_io_stats WHERE server_id = $1)
        ORDER BY (delta_stall_read_ms + delta_stall_write_ms) DESC
        """;

    public static async Task<List<FileIoRow>> GetLatestFileIoStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<FileIoRow>();
        await using var command = postgres.CreateCommand(LatestFileIoStatsSql);
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new FileIoRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10)));
        }

        return rows;
    }

    /* ─────────────────────────── tempdb ─────────────────────────── */

    /// <summary>
    /// tempdb space-usage samples over the window — the viewer's <c>TempDbTrendSql</c>. MB columns are
    /// <c>numeric(18,2)</c> → double precision; total_sessions_using_tempdb is bigint, top_session_id is
    /// integer. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string TempDbTrendSql = """
        SELECT
            collection_time,
            CAST(user_object_reserved_mb AS double precision),
            CAST(internal_object_reserved_mb AS double precision),
            CAST(version_store_reserved_mb AS double precision),
            CAST(total_reserved_mb AS double precision),
            CAST(unallocated_mb AS double precision),
            total_sessions_using_tempdb,
            top_session_id,
            CAST(top_session_tempdb_mb AS double precision)
        FROM v_tempdb_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time
        """;

    public static async Task<List<TempDbSample>> GetTempDbTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<TempDbSample>();
        await using var command = postgres.CreateCommand(TempDbTrendSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            samples.Add(new TempDbSample(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : reader.GetDouble(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8)));
        }

        return samples;
    }

    /* ─────────────────────────── perfmon ─────────────────────────── */

    /// <summary>
    /// The latest perfmon counters — Lite's <c>GetLatestPerfmonStatsAsync</c>: counter_name /
    /// instance_name / cntr_value / delta_cntr_value at the newest collection. $1 server_id.
    /// </summary>
    public const string LatestPerfmonStatsSql = """
        SELECT
            counter_name,
            instance_name,
            cntr_value,
            delta_cntr_value
        FROM v_perfmon_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_perfmon_stats WHERE server_id = $1)
        ORDER BY counter_name
        """;

    public static async Task<List<PerfmonRow>> GetLatestPerfmonStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<PerfmonRow>();
        await using var command = postgres.CreateCommand(LatestPerfmonStatsSql);
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PerfmonRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt64(3)));
        }

        return rows;
    }

    /* ─────────────────────────── top queries ─────────────────────────── */

    /// <summary>
    /// Top query-stats groups over the window — a focused projection of the viewer's <c>TopQueriesSql</c>
    /// (the columns Lite's get_top_queries_by_cpu returns): group by (database, query_hash), sum the
    /// deltas + carry min/max spreads, rank by summed <c>delta_elapsed_time</c> descending, over-fetch by
    /// 5 to drop WAITFOR shells via the latest-text LATERAL, cap at top. Summed bigints CAST back to bigint
    /// for the typed reader. The aggregate reads the base <c>query_stats</c> table (it projects no text);
    /// the text LATERAL reads <c>v_query_stats</c>, which resolves the #1767 payload dimension — the plan
    /// tools read it the same way. $1 server_id, $2/$3 window (naive UTC), $4 top.
    /// </summary>
    public const string TopQueriesSql = """
        WITH ranked AS (
            SELECT
                database_name,
                query_hash,
                host_object_name,
                CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
                CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
                CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
                CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
                CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
                CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
                CAST(SUM(delta_rows) AS bigint) AS total_rows,
                CAST(SUM(delta_spills) AS bigint) AS total_spills,
                MIN(min_dop) AS min_dop,
                MAX(max_dop) AS max_dop,
                MIN(min_worker_time) AS min_worker_time,
                MAX(max_worker_time) AS max_worker_time,
                MIN(min_elapsed_time) AS min_elapsed_time,
                MAX(max_elapsed_time) AS max_elapsed_time,
                MAX(query_plan_hash) AS query_plan_hash,
                MAX(sql_handle) AS sql_handle,
                MAX(plan_handle) AS plan_handle,
                /* #2012: how many DISTINCT statement texts this hash group merged. query_hash is a
                   SHAPE hash — INSERT...EXEC statements naming DIFFERENT callee procs share one
                   (reproduced live), and ad-hoc literal variants collapse too — so a group with
                   distinct_texts > 1 is a BLEND whose representative text below is one member, not
                   the statement. Counted over the #1767 content digest already on every row (~free);
                   COUNT(DISTINCT) skips NULLs, so 0 means only pre-dimension legacy rows, which age
                   out with raw retention. */
                COUNT(DISTINCT query_text_digest) AS distinct_texts
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            /* #2012 stage 2: host_object_name splits INSERT...EXEC callers that share a query_hash
               (each proc-hosted statement groups under its own host object), while ad-hoc rows carry
               NULL and keep collapsing into one group per hash exactly as before. */
            GROUP BY database_name, query_hash, host_object_name
            HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
            ORDER BY SUM(delta_elapsed_time) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_hash,
            r.host_object_name,
            r.query_plan_hash,
            r.sql_handle,
            r.plan_handle,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.total_reads,
            r.total_writes,
            r.total_physical_reads,
            r.total_rows,
            r.total_spills,
            r.min_dop,
            r.max_dop,
            r.min_worker_time,
            r.max_worker_time,
            r.min_elapsed_time,
            r.max_elapsed_time,
            t.query_text,
            r.distinct_texts
        FROM ranked AS r
        LEFT JOIN LATERAL (
            SELECT query_text
            FROM v_query_stats
            WHERE server_id = $1
            AND   query_hash = r.query_hash
            AND   database_name = r.database_name
            /* #2012 stage 2: the representative text must come from THIS group's own rows — before
               this, the lookup could serve one caller's text for another caller's stats, which is
               the exact mis-attribution the issue documents from live triage. */
            AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
            AND   query_text IS NOT NULL
            ORDER BY collection_time DESC
            LIMIT 1
        ) AS t ON TRUE
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_elapsed_us DESC
        LIMIT $4
        """;

    /// <summary>
    /// The same top-queries read, rolled up so proc-hosted dynamic SQL ranks as its PARENT (#2235).
    ///
    /// <para><b>The defect this answers.</b> <c>query_hash</c> is a shape hash, so dynamic SQL built with
    /// per-value literals fragments one logical statement across as many hashes as there are literal sets —
    /// measured at 21 for one <c>API.GetInventoryWithLabsV5</c> statement. Ranking by hash therefore
    /// STRUCTURALLY cannot surface it: two of its fragments together were 58-65% of the instance's
    /// worker_time in every window sampled, while the hash itself never entered the 168-hour top 20. The
    /// ranking looked healthy and explained roughly a tenth of the box.</para>
    ///
    /// <para><b>Why this is a sibling const rather than a parameter.</b> Postgres cannot parameterize
    /// <c>GROUP BY</c>, and every read here is a public const precisely so the suite can pin its dialect and
    /// columns without a live store. Building the clause by string concatenation would trade both of those
    /// for one saved copy.</para>
    ///
    /// <para><b>Ad-hoc rows keep their per-hash grouping, and that is load-bearing.</b> A bare
    /// <c>GROUP BY host_object_name</c> would pool EVERY unrelated ad-hoc statement in a database into one
    /// meaningless row, because ad-hoc rows carry <c>host_object_name = NULL</c> — turning the fix into a
    /// worse attribution bug than the one it fixes. The <c>CASE</c> in the grouping key keys ad-hoc rows on
    /// their own <c>query_hash</c> (identical to the default read) and collapses only rows that actually name
    /// a host object.</para>
    ///
    /// <para>The per-hash grouping (#2012 stage 2) stays the DEFAULT. Two procedures sharing a hash genuinely
    /// are different work, which is why that split exists; this is an additional lens, not a replacement.
    /// <c>query_hash</c> in a rolled-up row is one member of the group, exactly as <c>query_text</c> already
    /// is when <c>distinct_texts &gt; 1</c> — <c>distinct_query_hashes</c> is what says so.</para>
    /// </summary>
    public const string TopQueriesByHostObjectSql = """
        WITH ranked AS (
            SELECT
                database_name,
                MAX(query_hash) AS query_hash,
                host_object_name,
                CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
                CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
                CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
                CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
                CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
                CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
                CAST(SUM(delta_rows) AS bigint) AS total_rows,
                CAST(SUM(delta_spills) AS bigint) AS total_spills,
                MIN(min_dop) AS min_dop,
                MAX(max_dop) AS max_dop,
                MIN(min_worker_time) AS min_worker_time,
                MAX(max_worker_time) AS max_worker_time,
                MIN(min_elapsed_time) AS min_elapsed_time,
                MAX(max_elapsed_time) AS max_elapsed_time,
                MAX(query_plan_hash) AS query_plan_hash,
                MAX(sql_handle) AS sql_handle,
                MAX(plan_handle) AS plan_handle,
                COUNT(DISTINCT query_text_digest) AS distinct_texts,
                /* #2235: the fragment count IS the finding — 21 here is why a per-hash ranking missed it. */
                COUNT(DISTINCT query_hash) AS distinct_query_hashes
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            /* #2235: proc-hosted rows collapse to one row per (database, host object) — every literal
               fragment of one statement lands together. Ad-hoc rows (host_object_name NULL) fall to the
               CASE and stay keyed on their OWN query_hash, so they group exactly as the default read does;
               without that arm every unrelated ad-hoc statement in a database would pool into one row. */
            GROUP BY database_name, host_object_name,
                     CASE WHEN host_object_name IS NULL THEN query_hash END
            HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
            ORDER BY SUM(delta_elapsed_time) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_hash,
            r.host_object_name,
            r.query_plan_hash,
            r.sql_handle,
            r.plan_handle,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.total_reads,
            r.total_writes,
            r.total_physical_reads,
            r.total_rows,
            r.total_spills,
            r.min_dop,
            r.max_dop,
            r.min_worker_time,
            r.max_worker_time,
            r.min_elapsed_time,
            r.max_elapsed_time,
            t.query_text,
            r.distinct_texts,
            r.distinct_query_hashes
        FROM ranked AS r
        LEFT JOIN LATERAL (
            SELECT query_text
            FROM v_query_stats
            WHERE server_id = $1
            AND   database_name = r.database_name
            /* Mirrors the grouping: for a rolled-up proc any of its fragments' texts is a valid
               representative, but an ad-hoc row must still match its own hash or the text could come from
               an unrelated statement. */
            AND   host_object_name IS NOT DISTINCT FROM r.host_object_name
            AND   (r.host_object_name IS NOT NULL OR query_hash = r.query_hash)
            AND   query_text IS NOT NULL
            ORDER BY collection_time DESC
            LIMIT 1
        ) AS t ON TRUE
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_elapsed_us DESC
        LIMIT $4
        """;

    public static async Task<List<TopQueryRow>> GetTopQueriesByCpuAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName,
        bool rollUpByHostObject = false, CancellationToken cancellationToken = default)
    {
        var rows = new List<TopQueryRow>();
        /* #2235: same parameters, same columns, different GROUP BY — see TopQueriesByHostObjectSql. */
        await using var command = postgres.CreateCommand(rollUpByHostObject ? TopQueriesByHostObjectSql : TopQueriesSql);
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, top);
        AddNullableText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new TopQueryRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),   /* host_object_name (#2012 stage 2) */
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? "" : reader.GetString(4),
                reader.IsDBNull(5) ? "" : reader.GetString(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14)),
                reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15)),
                reader.IsDBNull(16) ? 0 : reader.GetInt64(16),
                reader.IsDBNull(17) ? 0 : reader.GetInt64(17),
                reader.IsDBNull(18) ? 0 : reader.GetInt64(18),
                reader.IsDBNull(19) ? 0 : reader.GetInt64(19),
                reader.IsDBNull(20) ? "" : reader.GetString(20),
                reader.IsDBNull(21) ? 0 : reader.GetInt64(21),
                reader.FieldCount > 22 && !reader.IsDBNull(22) ? reader.GetInt64(22) : 1));
        }

        return rows;
    }

    /* ─────────────────────────── top procedures ─────────────────────────── */

    /// <summary>
    /// Top procedure-stats groups over the window — a focused projection of the viewer's
    /// <c>TopProceduresSql</c> (the columns Lite's get_top_procedures_by_cpu returns): group by
    /// (database, schema, object, type), sum the deltas + carry min/max spreads, rank by summed
    /// <c>delta_elapsed_time</c> descending, cap at top. Reads the base <c>procedure_stats</c> table
    /// (no v_ view). $1 server_id, $2/$3 window (naive UTC), $4 top.
    /// </summary>
    public const string TopProceduresSql = """
        SELECT
            database_name,
            schema_name,
            object_name,
            object_type,
            MAX(sql_handle) AS sql_handle,
            MAX(plan_handle) AS plan_handle,
            CAST(SUM(delta_execution_count) AS bigint) AS total_executions,
            CAST(SUM(delta_worker_time) AS bigint) AS total_cpu_us,
            CAST(SUM(delta_elapsed_time) AS bigint) AS total_elapsed_us,
            CAST(SUM(delta_logical_reads) AS bigint) AS total_reads,
            CAST(SUM(delta_logical_writes) AS bigint) AS total_writes,
            CAST(SUM(delta_physical_reads) AS bigint) AS total_physical_reads,
            CAST(SUM(delta_spills) AS bigint) AS total_spills,
            MIN(min_worker_time) AS min_worker_time,
            MAX(max_worker_time) AS max_worker_time,
            MIN(min_elapsed_time) AS min_elapsed_time,
            MAX(max_elapsed_time) AS max_elapsed_time
        FROM procedure_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($5::text IS NULL OR database_name = $5)
        GROUP BY database_name, schema_name, object_name, object_type
        HAVING SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0
        ORDER BY SUM(delta_elapsed_time) DESC
        LIMIT $4
        """;

    public static async Task<List<TopProcedureRow>> GetTopProceduresByCpuAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName, CancellationToken cancellationToken = default)
    {
        var rows = new List<TopProcedureRow>();
        await using var command = postgres.CreateCommand(TopProceduresSql);
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, top);
        AddNullableText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new TopProcedureRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? "" : reader.GetString(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? "" : reader.GetString(4),
                reader.IsDBNull(5) ? "" : reader.GetString(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                reader.IsDBNull(14) ? 0 : reader.GetInt64(14),
                reader.IsDBNull(15) ? 0 : reader.GetInt64(15),
                reader.IsDBNull(16) ? 0 : reader.GetInt64(16)));
        }

        return rows;
    }

    /* ─────────────────────────── query store ─────────────────────────── */

    /// <summary>
    /// The oldest <c>collection_time</c> this server actually has inside the requested window (#2364).
    ///
    /// <para><b>Why a separate probe rather than reading the returned rows.</b> <see cref="QueryStoreTopSql"/>
    /// returns the top N by COST, not by time, so the timestamps on those rows say nothing about how far back
    /// the window reaches — the most expensive query in a month might have run this morning. The window floor is
    /// a property of the tier, not of the result set, and has to be asked for separately.</para>
    ///
    /// <para>Bounded on both sides, so it prunes chunks and answers from an ordered scan that stops at the first
    /// row rather than reading the window. $1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public const string QueryStoreWindowFloorSql = """
        SELECT MIN(collection_time)
        FROM query_store_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        """;

    /// <summary>
    /// Reads <see cref="QueryStoreWindowFloorSql"/>. Null when the window holds nothing at all, which the caller
    /// reports as "nothing was read" rather than as an absence of activity.
    /// </summary>
    public static async Task<DateTime?> GetQueryStoreWindowFloorAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(QueryStoreWindowFloorSql);
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, startUtc);
        DarlingMcpReadParameters.AddTimestamp(command, endUtc);
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime dt ? dt : null;
    }

    /// <summary>
    /// Top Query Store groups over the window — a focused projection of the viewer's
    /// <c>QueryStoreTopSql</c> (the columns Lite's get_query_store_top returns): group by
    /// (database, query_id, plan_id, query_hash, replica_role), average the per-interval metrics, rank by total duration
    /// (<c>SUM(execution_count) * AVG(avg_duration_us)</c>) descending, over-fetch by 5 for the WAITFOR
    /// trim, cap at top. The avg columns are bigint (per-interval averages) → double precision before the
    /// AVG/scale. Reads the base <c>query_store_stats</c> table (no v_ view). $1 server_id, $2/$3 window
    /// (naive UTC), $4 top.
    /// </summary>
    public const string QueryStoreTopSql = """
        WITH deduped AS (
            /* LOAD-BEARING (correctness, not just perf) — #1841. query_store_stats rows are CUMULATIVE
               per-Query-Store-interval snapshots, and the collector re-fetches the OPEN interval every
               cycle as its last_execution_time advances, so the SAME interval (same first_execution_time)
               is stored repeatedly with a growing execution_count. SUM(execution_count) over the raw rows
               reports 10 + 25 + 40 for an interval that reached 40, and AVG(avg_*) becomes an avg-of-avgs
               weighted by how many times each interval happened to be re-collected. This surface feeds
               BOTH the MCP tool and the REST route, so un-deduped numbers reach an agent's reasoning as
               readily as the web dashboard. Twins the viewer's QueryStoreTopSql.

               replica_role and execution_type_desc are in the partition because the aggregate below is
               grouped (or MAXed) on them: the dedup key must be at least as fine as the read's own row
               identity, or dedup would drop a row the read must return rather than de-duplicate one. */
            SELECT
                *,
                ROW_NUMBER() OVER
                (
                    PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                    ORDER BY collection_time DESC, execution_count DESC
                ) AS rn
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text IS NULL OR database_name = $5)
        ),
        ranked AS (
            SELECT
                database_name,
                query_id,
                plan_id,
                query_hash,
                /* A GROUP BY key, not MAX(): an AG's Query Store for secondary replicas (2022+) keeps ONE
                   shared store on the primary holding every replica's rows, so grouping without it would
                   average primary and secondary workload into a single blended row. No-op on a
                   standalone/non-AG server, where every row shares one value. */
                replica_role,
                CAST(SUM(execution_count) AS bigint) AS total_executions,
                AVG(CAST(avg_duration_us AS double precision)) / 1000.0 AS avg_duration_ms,
                AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0 AS avg_cpu_time_ms,
                AVG(CAST(avg_logical_io_reads AS double precision)) AS avg_logical_reads,
                AVG(CAST(avg_logical_io_writes AS double precision)) AS avg_logical_writes,
                AVG(CAST(avg_physical_io_reads AS double precision)) AS avg_physical_reads,
                AVG(CAST(avg_rowcount AS double precision)) AS avg_rowcount,
                MAX(last_execution_time) AS last_execution_time,
                MAX(query_plan_hash) AS query_plan_hash
            FROM deduped
            WHERE rn = 1
            GROUP BY database_name, query_id, plan_id, query_hash, replica_role
            ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS double precision)) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_id,
            r.plan_id,
            r.query_hash,
            r.query_plan_hash,
            r.total_executions,
            r.avg_duration_ms,
            r.avg_cpu_time_ms,
            r.avg_logical_reads,
            r.avg_logical_writes,
            r.avg_physical_reads,
            r.avg_rowcount,
            r.last_execution_time,
            t.query_text,
            r.replica_role
        FROM ranked AS r
        /* #2150: resolve the text inside the lateral so the projection and the WAITFOR self-exclusion below
           both keep reading one t.query_text. First arm is collect.query_store_text (one row per
           query_id, where the collector lands text once the separate fetch is on); second arm is the
           newest inline query_text, which is where text lived before the cutover and is what keeps
           existing history readable. */
        LEFT JOIN LATERAL (
            SELECT COALESCE(
                       (
                           SELECT x.query_sql_text
                           FROM query_store_text AS x
                           WHERE x.server_id = $1
                           AND   x.database_name = r.database_name
                           AND   x.query_id = r.query_id
                       ),
                       (
                           SELECT s.query_text
                           FROM query_store_stats AS s
                           WHERE s.server_id = $1
                           AND   s.query_id = r.query_id
                           AND   s.database_name = r.database_name
                           AND   s.query_text IS NOT NULL
                           ORDER BY s.collection_time DESC
                           LIMIT 1
                       )
                   ) AS query_text
        ) AS t ON TRUE
        WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
        ORDER BY r.total_executions * r.avg_duration_ms DESC
        LIMIT $4
        """;

    public static async Task<List<QueryStoreRow>> GetQueryStoreTopAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName, CancellationToken cancellationToken = default)
    {
        var rows = new List<QueryStoreRow>();
        await using var command = postgres.CreateCommand(QueryStoreTopSql);
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, top);
        AddNullableText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new QueryStoreRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                reader.IsDBNull(3) ? "" : reader.GetString(3),
                reader.IsDBNull(4) ? "" : reader.GetString(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : reader.GetDouble(6),
                reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
                reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
                reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                reader.IsDBNull(10) ? 0 : reader.GetDouble(10),
                reader.IsDBNull(11) ? 0 : reader.GetDouble(11),
                reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                reader.IsDBNull(13) ? "" : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetString(14)));
        }

        return rows;
    }

    /* ─────────────────────────── discovery / health ─────────────────────────── */

    /// <summary>
    /// Every enabled server plus its newest collection instant — the list_servers read. The
    /// correlated <c>MAX(collection_time)</c> per server drives the freshness-derived status the tool
    /// assigns (the headless viewer has no live ping either — see <c>ServerSummaryItem.ClassifyFreshness</c>).
    /// $-free (no parameters, no bare now()) so a test can pin the dialect ungated.
    /// </summary>
    public const string ServerListSql = """
        SELECT
            s.server_id,
            s.server_name,
            s.display_name,
            s.sql_major_version,
            (SELECT MAX(cl.collection_time) FROM v_collection_log cl WHERE cl.server_id = s.server_id) AS last_collection
        FROM servers s
        WHERE s.is_enabled
        ORDER BY s.server_name
        """;

    public static async Task<List<ServerListRow>> GetServerListAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var rows = new List<ServerListRow>();
        await using var command = postgres.CreateCommand(ServerListSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ServerListRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4)));
        }

        return rows;
    }

    /// <summary>
    /// Per-collector 7-day health aggregate — Lite's <c>GetCollectionHealthAsync</c>: one row per
    /// collector with run/success/error counts, the average / maximum / p95 single-run duration, last
    /// success/run/error timestamps, and the permission-denied count for the banding. SKIPPED counts as
    /// a healthy run. $1 server_id, $2 window start (naive UTC — the trailing 7 days).
    ///
    /// <para>16 columns since #2460, and no longer column-identical to the WPF viewer's own
    /// <c>CollectionHealthSql</c>: the two duration statistics feed the MCP tool's sweep-pressure
    /// arithmetic, which the viewer's health grid does not serve. Lite's DuckDB read carries them at
    /// the SAME ordinals, which is the parity that matters here — both MCP surfaces read positionally.</para>
    /// </summary>
    public const string CollectionHealthSql = """
        SELECT
            collector_name,
            COUNT(*) AS total_runs,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
            SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
            AVG(duration_ms) AS avg_duration_ms,
            -- #2460: the mean above describes a collector whose runs all cost about the same, and
            -- says nothing true about one whose runs come in two sizes. query_store on a dense shard
            -- reported a 13,834 ms average over 1,155 runs where 958 of them yielded nothing and cost
            -- ~36 ms, which puts the other 197 at ~80,900 ms EACH — each one on its own larger than
            -- the whole 60,000 ms sweep budget. duration_ms has been written per run since V2;
            -- nothing had ever read it as anything but a mean.
            --
            -- p95 rather than the max for the number a decision is made from: a max is one run, so a
            -- single pathological cycle would make a collector look permanently terrible for the rest
            -- of the window. p95 also scales itself to the sample — over 3,500 runs it discards the
            -- one bad cycle, and over the six runs a daily collector gets in a week it lands on the
            -- max, which is right, because with six samples there is no outlier anyone can afford to
            -- throw away. DISC rather than CONT so the answer is a duration some run actually took
            -- instead of an interpolation between the two modes, which would be a number describing
            -- no run at all — the exact defect this column exists to end. Both engines ignore NULL
            -- duration_ms here, as AVG already does. Byte-identical to Lite's DuckDB read.
            MAX(duration_ms) AS max_duration_ms,
            PERCENTILE_DISC(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_duration_ms,
            MAX(CASE WHEN status IN ('SUCCESS', 'SKIPPED') THEN collection_time END) AS last_success_time,
            MAX(collection_time) AS last_run_time,
            -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest
            -- one. The status re-check is load-bearing rather than belt-and-braces: when no failing run
            -- in the window carried text, error_rank = 1 falls through to the newest row of ANY class,
            -- and without it a SUCCESS row's note could surface here as a fake last error.
            MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS') THEN error_message END) AS last_error,
            -- The newest failure OUTRIGHT, text or not — "when did this last fail" means the run, not
            -- the message.
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS') THEN collection_time END) AS last_error_time,
            SUM(CASE WHEN status = 'PERMISSIONS' THEN 1 ELSE 0 END) AS permission_denied_count,
            SUM(CASE WHEN status = 'YIELDED' THEN 1 ELSE 0 END) AS yield_count,
            -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items,
            -- items whose enumeration probe failed) and how many runs carried one. Gated on SUCCESS
            -- specifically — the runners attach a note only to the SUCCESS write. Informational: it
            -- feeds no band, and a legitimately empty target stays HEALTHY.
            MAX(CASE WHEN note_rank = 1 AND status = 'SUCCESS' THEN error_message END) AS last_note,
            COUNT(CASE WHEN status = 'SUCCESS' THEN error_message END) AS note_count,
            -- #1852: the one thing that makes a persistently-empty enumeration interesting — does this
            -- target actually HAVE user databases? Zero items on a server with none is legitimate and
            -- stays quiet; zero items on a server that HAS them is a login that cannot enter any, or a
            -- filter that swallowed everything. database_size_stats rather than database_config for
            -- three reasons: it runs on the scheduled loop (60 min) where database_config is on-load
            -- and can age past this window on a long-running service; it is indexed on
            -- (server_id, collection_time) where database_config has no index at all; and it reads
            -- sys.master_files, so it still sees databases the monitoring login cannot ENTER — exactly
            -- the case being diagnosed. database_id > 4 excludes the system databases, tempdb
            -- included: the size collector takes every ONLINE database, so a bare row check
            -- would be true on every server alive.
            --
            -- The inventory window is the health read's OWN ($2) — no second parameter, and an
            -- inventory that aged out says nothing rather than something stale. Uncorrelated, so both
            -- engines evaluate it once per query (a Postgres InitPlan) instead of per row, and it
            -- needs no GROUP BY entry and no join. Feeds display text only, through the shared
            -- formatter; the banding never sees it.
            CASE
                WHEN EXISTS
                     (
                         SELECT 1
                         FROM v_database_size_stats
                         WHERE server_id = $1
                         AND   collection_time >= $2
                         AND   database_id > 4
                     )
                THEN 1
                ELSE 0
            END AS has_user_databases,
            -- #2472: the per-database fan-out, described for ONE run — the dearest one in the window.
            -- Five collectors run once per database and the run writes a single blended duration_ms, so
            -- "eight databases at 10.1s" and "one at 62s beside seven at 2.7s" are the same 80,900 ms and
            -- want opposite fixes. These four are that one run's parts, and their ratio
            -- (slowest_item_ms * fanout_items / slowest_run_duration_ms) is 1.0 for an even fan-out and
            -- 6.1 for the dominated example. All four come from the SAME row via slowest_rank rather than
            -- four independent aggregates, because a slowest item taken from one run and a width taken
            -- from another compose into a ratio describing no run that ever happened — the same defect
            -- PERCENTILE_CONT was rejected for above. NULL throughout for a collector that never fans
            -- out, which is most of them.
            MAX(CASE WHEN slowest_rank = 1 THEN fanout_item_count END) AS fanout_items,
            MAX(CASE WHEN slowest_rank = 1 THEN slowest_item END) AS slowest_item,
            MAX(CASE WHEN slowest_rank = 1 THEN slowest_item_ms END) AS slowest_item_ms,
            MAX(CASE WHEN slowest_rank = 1 THEN duration_ms END) AS slowest_run_duration_ms
        FROM
        (
            -- #1855: rank each class of message newest-first so the two exemplar columns above can take
            -- the LATEST one instead of the lexicographically greatest. Ordering on whether the class's
            -- CASE came back empty puts every row that carries such a message ahead of every row that
            -- does not, so rank 1 is the newest one that has text — and a later clean run no longer
            -- blanks a note the window still holds. Text does not sort like the number #1837's probe
            -- note carries: 12 item(s) sorts below 9 item(s). One byte-identical shape with Lite's
            -- DuckDB read and the Viewer's, down to the error_message DESC tie-break.
            SELECT
                collector_name,
                collection_time,
                duration_ms,
                status,
                error_message,
                -- #2472: projected here because this subquery enumerates its columns rather than
                -- SELECT *-ing them, so an aggregate outside that names a column the inner query does
                -- not carry fails at the STORE and nowhere earlier — no compiler, no text assertion and
                -- no local build can see it.
                fanout_item_count,
                slowest_item,
                slowest_item_ms,
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY (CASE WHEN status = 'SUCCESS' THEN error_message END) IS NULL,
                             collection_time DESC,
                             error_message DESC
                ) AS note_rank,
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY (CASE WHEN status IN ('ERROR', 'PERMISSIONS') THEN error_message END) IS NULL,
                             collection_time DESC,
                             error_message DESC
                ) AS error_rank,
                -- #2472: the window's dearest single ITEM, and with it the run that carried it. Ranked on
                -- slowest_item_ms rather than duration_ms because the question is which database is
                -- expensive, not which cycle was: a run whose total is the largest only because every
                -- database was busy is exactly the shape a per-database override should NOT be aimed at.
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY slowest_item_ms IS NULL,
                             slowest_item_ms DESC,
                             collection_time DESC
                ) AS slowest_rank
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
        ) runs
        GROUP BY collector_name
        ORDER BY collector_name
        """;

    public static async Task<List<CollectorHealth>> GetCollectionHealthAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStartUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorHealth>();
        await using var command = postgres.CreateCommand(CollectionHealthSql);
        AddInt(command, serverId);
        AddTimestamp(command, windowStartUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CollectorHealth
            {
                CollectorName = reader.GetString(0),
                TotalRuns = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                SuccessCount = reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                ErrorCount = reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                AvgDurationMs = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                MaxDurationMs = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                P95DurationMs = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                LastSuccessTime = reader.IsDBNull(7) ? null : reader.GetDateTime(7),
                LastRunTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                LastError = reader.IsDBNull(9) ? null : reader.GetString(9),
                LastErrorTime = reader.IsDBNull(10) ? null : reader.GetDateTime(10),
                PermissionDeniedCount = reader.IsDBNull(11) ? 0 : Convert.ToInt64(reader.GetValue(11)),
                YieldCount = reader.IsDBNull(12) ? 0 : Convert.ToInt64(reader.GetValue(12)),
                LastNote = reader.IsDBNull(13) ? null : reader.GetString(13),
                NoteCount = reader.IsDBNull(14) ? 0 : Convert.ToInt64(reader.GetValue(14)),
                TargetHasUserDatabases = !reader.IsDBNull(15) && Convert.ToInt64(reader.GetValue(15)) != 0,
                /* Ordinals are positional and these four were APPENDED (#2472) — never inserted. */
                FanoutItems = reader.IsDBNull(16) ? null : Convert.ToInt32(reader.GetValue(16)),
                SlowestItem = reader.IsDBNull(17) ? null : reader.GetString(17),
                SlowestItemMs = reader.IsDBNull(18) ? null : Convert.ToInt32(reader.GetValue(18)),
                SlowestRunDurationMs = reader.IsDBNull(19) ? null : Convert.ToInt32(reader.GetValue(19)),
            });
        }

        return rows;
    }

    /// <summary>
    /// The latest server_properties snapshot — Lite's <c>GetLatestServerPropertiesAsync</c>. Reads the
    /// base <c>server_properties</c> table (the store has NO <c>v_server_properties</c> view — the viewer
    /// reads the base table for its UTC-offset read too). $1 server_id.
    /// </summary>
    public const string LatestServerPropertiesSql = """
        SELECT
            collection_time,
            edition,
            product_version,
            product_level,
            product_update_level,
            engine_edition,
            cpu_count,
            hyperthread_ratio,
            physical_memory_mb,
            socket_count,
            cores_per_socket,
            is_hadr_enabled,
            is_clustered,
            enterprise_features,
            service_objective
        FROM server_properties
        WHERE server_id = $1
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    public static async Task<ServerPropertiesReadRow?> GetLatestServerPropertiesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(LatestServerPropertiesSql);
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new ServerPropertiesReadRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? "" : reader.GetString(1),
            reader.IsDBNull(2) ? "" : reader.GetString(2),
            reader.IsDBNull(3) ? "" : reader.GetString(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
            reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
            reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
            reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
            reader.IsDBNull(9) ? 0 : reader.GetInt32(9),
            reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
            !reader.IsDBNull(11) && reader.GetBoolean(11),
            !reader.IsDBNull(12) && reader.GetBoolean(12),
            reader.IsDBNull(13) ? null : reader.GetString(13),
            reader.IsDBNull(14) ? null : reader.GetString(14));
    }

    /* ─────────────────────────── parameter helpers ─────────────────────────── */

    private static void AddWindow(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
    }

    /// <summary>
    /// The raw per-run collection log for one server inside an explicit window, newest first, capped.
    /// <para>Bounded on BOTH sides rather than by a single now-relative lower bound, matching how the
    /// viewer's Collection Log tab windows this read: a caller asking about a past incident wants the
    /// rows from THEN, and an hours-back-from-now span cannot express that.</para>
    /// <para>Reads <c>v_collection_log</c>, the same view the viewer uses, so the web dashboard and the
    /// MCP surface cannot drift from what the desktop shows. $1 server_id, $2 window start, $3 window
    /// end (naive UTC), $4 row cap.</para>
    /// </summary>
    public const string CollectionLogSql = """
        SELECT
            collector_name,
            collection_time,
            duration_ms,
            sql_duration_ms,
            duckdb_duration_ms,
            rows_collected,
            status,
            error_message
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time DESC
        LIMIT $4
        """;

    /// <summary>
    /// Whether this server has EVER recorded a collector run, ignoring any window.
    /// <para>Exists so an empty log read can say which kind of nothing it found. "No runs in the last
    /// 24 hours" is true both of a quiet window and of a server that has never collected, and those
    /// need opposite responses from the caller -- widen the window, versus go find out why collection
    /// is not running. LIMIT 1 with no ordering, so it stops at the first row rather than scanning.</para>
    /// </summary>
    public const string HasAnyCollectionLogSql = """
        SELECT 1
        FROM v_collection_log
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Runs <see cref="HasAnyCollectionLogSql"/>.</summary>
    public static async Task<bool> HasAnyCollectionLogAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HasAnyCollectionLogSql);
        AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>Runs <see cref="CollectionLogSql"/>. See it for the window semantics.</summary>
    public static async Task<List<CollectionLogEntry>> GetCollectionLogAsync(
        NpgsqlDataSource postgres,
        int serverId,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int maxRows,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectionLogEntry>();
        await using var command = postgres.CreateCommand(CollectionLogSql);
        AddInt(command, serverId);
        AddTimestamp(command, windowStartUtc);
        AddTimestamp(command, windowEndUtc);
        AddInt(command, maxRows);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new CollectionLogEntry(
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3)),
                reader.IsDBNull(4) ? null : Convert.ToDouble(reader.GetValue(4)),
                reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5)),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7)));
        }

        return rows;
    }

    /// <summary>
    /// Waiting-task total wait duration per wait type per collection, for one server over an explicit
    /// window. The viewer's Current Waits reader verbatim. $1 server_id, $2 start, $3 end (naive UTC).
    /// </summary>
    public const string WaitingTaskTrendSql = """
        SELECT
            collection_time,
            wait_type,
            CAST(SUM(wait_duration_ms) AS bigint) AS total_wait_ms
        FROM waiting_tasks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   wait_type IS NOT NULL
        GROUP BY
            collection_time,
            wait_type
        ORDER BY
            collection_time,
            wait_type
        """;

    /// <summary>
    /// Whether the waiting-task collector has EVER sampled this server, ignoring any window.
    /// <para>Separates an all-clear from missing data. Of the two, the wrong answer here is the
    /// REASSURING one: "nothing was waiting" stops a caller looking, where "never collected" sends them
    /// to check the collector. LIMIT 1, so it stops at the first row.</para>
    /// </summary>
    public const string HasAnyWaitingTaskSampleSql = """
        SELECT 1
        FROM waiting_tasks
        WHERE server_id = $1
        LIMIT 1
        """;

    /// <summary>Runs <see cref="HasAnyWaitingTaskSampleSql"/>.</summary>
    public static async Task<bool> HasAnyWaitingTaskSampleAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HasAnyWaitingTaskSampleSql);
        AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>Runs <see cref="WaitingTaskTrendSql"/>.</summary>
    public static async Task<List<WaitingTaskTrendRow>> GetWaitingTaskTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<WaitingTaskTrendRow>();
        await using var command = postgres.CreateCommand(WaitingTaskTrendSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new WaitingTaskTrendRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2)));
        }

        return rows;
    }

    /// <summary>
    /// Blocked-session count per database per collection, for one server over an explicit window.
    /// <para>blocking_session_id > 0 is the blocked-ness test, so this counts sessions WAITING ON another
    /// session rather than every waiting task. The optional database filter is kept from the viewer's read
    /// rather than dropped for a simpler signature: on a busy instance one database usually owns the
    /// blocking, and a series that cannot be narrowed to it answers a different question from the one the
    /// viewer answers. $1 server_id, $2 start, $3 end (naive UTC), $4 database name or NULL.</para>
    /// </summary>
    public const string BlockedSessionTrendSql = """
        SELECT
            collection_time,
            database_name,
            COUNT(*) AS blocked_count
        FROM waiting_tasks
        WHERE server_id = $1
        AND   blocking_session_id > 0
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   database_name IS NOT NULL
        AND   ($4::text IS NULL OR database_name = $4)
        GROUP BY
            collection_time,
            database_name
        ORDER BY
            collection_time,
            database_name
        """;

    /// <summary>Runs <see cref="BlockedSessionTrendSql"/>.</summary>
    public static async Task<List<BlockedSessionTrendRow>> GetBlockedSessionTrendAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        string? databaseName = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<BlockedSessionTrendRow>();
        await using var command = postgres.CreateCommand(BlockedSessionTrendSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        command.Parameters.Add(new NpgsqlParameter
        {
            Value = string.IsNullOrWhiteSpace(databaseName) ? DBNull.Value : databaseName,
            NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text,
        });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BlockedSessionTrendRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2)));
        }

        return rows;
    }

    /// <summary>
    /// Blocking-duration aggregate per minute, the viewer's Blocking Stats read verbatim.
    /// <para>XE blocked-process reports are the primary source and the DMV snapshot is the fallback, and
    /// the fallback contributes ONLY when the XE source has no rows in the window at all. Mixing them
    /// would double-count the same incident from two captures, so it is a fallback and never a union.
    /// $1 server_id, $2 start, $3 end (naive UTC).</para>
    /// </summary>
    public const string BlockingDurationStatsSql = """
        WITH bpr AS (
            SELECT
                DATE_TRUNC('minute', event_time) AS bucket,
                COUNT(*) AS event_count,
                CAST(SUM(wait_time_ms) AS bigint) AS total_duration_ms,
                MAX(wait_time_ms) AS max_duration_ms,
                CAST(AVG(wait_time_ms) AS double precision) AS avg_duration_ms
            FROM v_blocked_process_reports
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            GROUP BY DATE_TRUNC('minute', event_time)
        ),
        dmv AS (
            SELECT
                DATE_TRUNC('minute', event_time) AS bucket,
                COUNT(*) AS event_count,
                CAST(SUM(wait_time_ms) AS bigint) AS total_duration_ms,
                MAX(wait_time_ms) AS max_duration_ms,
                CAST(AVG(wait_time_ms) AS double precision) AS avg_duration_ms
            FROM v_dmv_blocking_snapshots
            WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
            GROUP BY DATE_TRUNC('minute', event_time)
        )
        SELECT bucket, event_count, total_duration_ms, max_duration_ms, avg_duration_ms FROM bpr
        UNION ALL
        SELECT bucket, event_count, total_duration_ms, max_duration_ms, avg_duration_ms FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
        ORDER BY bucket
        """;

    public sealed record BlockingDurationStatsRow(
        DateTime Time, long EventCount, long TotalDurationMs, long MaxDurationMs, double AvgDurationMs);

    /// <summary>
    /// Whether ANY of the three capture paths behind the blocking-severity read has ever produced a row.
    /// <para>Each can be off independently: the XE blocked-process report needs its session running, the
    /// DMV snapshot needs its collector enabled, and deadlock capture is separate from both. Probing one
    /// would report "never captured" for a server capturing fine through another; probing neither would
    /// let a silent capture gap read as a clean bill of health.</para>
    /// <para>Deadlocks are in here because the verdict gates on the blocking series AND the deadlock
    /// series both being empty. Probing only the two blocking sources would call a server 'genuinely
    /// clear' on the strength of blocking capture alone, while deadlock capture had never run -- the
    /// reassuring-wrong answer this probe exists to prevent, missed for the deadlock half.</para>
    /// </summary>
    public const string HasAnyBlockingCaptureSql = """
        SELECT 1
        WHERE EXISTS (SELECT 1 FROM v_blocked_process_reports WHERE server_id = $1)
        OR    EXISTS (SELECT 1 FROM v_dmv_blocking_snapshots WHERE server_id = $1)
        OR    EXISTS (SELECT 1 FROM v_deadlocks WHERE server_id = $1)
        """;

    /// <summary>Runs <see cref="HasAnyBlockingCaptureSql"/>.</summary>
    public static async Task<bool> HasAnyBlockingCaptureAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HasAnyBlockingCaptureSql);
        AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>Runs <see cref="BlockingDurationStatsSql"/>.</summary>
    public static async Task<List<BlockingDurationStatsRow>> GetBlockingDurationStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<BlockingDurationStatsRow>();
        await using var command = postgres.CreateCommand(BlockingDurationStatsSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BlockingDurationStatsRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0 : Convert.ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4))));
        }

        return rows;
    }

    /// <summary>
    /// The raw deadlock graphs in the window, for severity aggregation.
    /// <para>Windowed on collection_time but ORDERED and bucketed by deadlock_time -- the graph carries
    /// when the deadlock happened, while collection_time is only when we picked it up, and the two differ
    /// by up to a collection interval. $1 server_id, $2 start, $3 end (naive UTC).</para>
    /// </summary>
    public const string DeadlockSeverityGraphsSql = """
        SELECT
            deadlock_time,
            deadlock_graph_xml
        FROM v_deadlocks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY deadlock_time
        """;

    /// <summary>Runs <see cref="DeadlockSeverityGraphsSql"/>, returning graphs for the shared aggregator.</summary>
    public static async Task<List<(DateTime? DeadlockTime, string? Xml)>> GetDeadlockGraphsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<(DateTime? DeadlockTime, string? Xml)>();
        await using var command = postgres.CreateCommand(DeadlockSeverityGraphsSql);
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((
                reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        return rows;
    }

    private static void AddInt(NpgsqlCommand command, int value) =>
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = value });

    private static void AddText(NpgsqlCommand command, string value) =>
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = value ?? "" });

    /// <summary>Binds a nullable text filter (a null value binds SQL NULL, activating the read's
    /// <c>$N::text IS NULL OR ...</c> "no filter" branch).</summary>
    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });

    /// <summary>Binds a naive-UTC timestamp: Kind=Unspecified maps to the store's <c>timestamp without
    /// time zone</c> columns (a Kind=Utc DateTime maps to timestamptz and throws since Npgsql 6.0).</summary>
    private static void AddTimestamp(NpgsqlCommand command, DateTime value) =>
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(value, DateTimeKind.Unspecified) });
}

/// <summary>
/// One collector's 7-day health roll-up with its health band — a faithful service-side port of the
/// viewer's <c>CollectorHealthRow</c> (itself Lite's), carrying just the fields the MCP
/// get_collection_health tool surfaces plus the computed <see cref="HealthStatus"/> / failure rate.
/// <see cref="HealthStatus"/> delegates to the shared <see cref="CollectorHealthClassifier"/> in
/// PerformanceMonitor.Common (#1573), so this service, Lite, and the viewer band identically and cannot
/// drift; it resolves the collector's cadence from the shared <see cref="CollectorScheduleDefaults"/> so a
/// healthy DAILY collector is no longer flagged stale/failing on the frequent-collector thresholds.
/// <see cref="DateTime.UtcNow"/> arithmetic is correct against the store's naive-UTC timestamps because
/// both are UTC instants (tick subtraction ignores Kind).
/// </summary>
internal sealed class CollectorHealth
{
    public string CollectorName { get; set; } = "";
    public long TotalRuns { get; set; }
    public long SuccessCount { get; set; }
    public long ErrorCount { get; set; }
    public double AvgDurationMs { get; set; }

    /// <summary>
    /// The single worst run in the window (#2460). A FACT, never a decision input: one pathological
    /// cycle would otherwise make a collector read as permanently terrible for seven days. Its job is
    /// to sit beside <see cref="P95DurationMs"/> — when the two agree the tail is routine, and when the
    /// max towers over the p95 the max was a one-off.
    /// </summary>
    public double MaxDurationMs { get; set; }

    /// <summary>
    /// The 95th-percentile run in the window (#2460) — what a HEAVY run of this collector costs, as
    /// opposed to what its runs cost on average. The number the sweep's peak-cycle arithmetic is built
    /// from (via <see cref="SweepPressureClassifier.PeakRunMs"/>), because a mean over a bimodal
    /// collector describes neither of its populations.
    /// </summary>
    public double P95DurationMs { get; set; }

    public DateTime? LastSuccessTime { get; set; }
    public DateTime? LastRunTime { get; set; }
    public string? LastError { get; set; }
    public DateTime? LastErrorTime { get; set; }
    public long PermissionDeniedCount { get; set; }
    /// <summary>1s lock-timeout yields (#1805) — deliberate, benign, counted apart from errors.</summary>
    public long YieldCount { get; set; }

    /// <summary>
    /// The note a non-failing run left behind (#1837): an enumeration that yielded 0 items, items whose
    /// enumeration probe failed. Null for the ordinary run. Informational — never an input to
    /// <see cref="HealthStatus"/>.
    /// </summary>
    public string? LastNote { get; set; }

    /// <summary>How many of <see cref="TotalRuns"/> carried a <see cref="LastNote"/>.</summary>
    public long NoteCount { get; set; }

    /// <summary>
    /// #1852: whether the store saw user databases on this target inside the health window
    /// (<c>has_user_databases</c>) — what tells a legitimately empty server apart from one that is
    /// enumerating nothing despite having databases. False also covers "no inventory to go on", which
    /// deliberately reads the same as "nothing to say". Informational, like <see cref="LastNote"/>:
    /// <see cref="HealthStatus"/> never sees it.
    /// </summary>
    public bool TargetHasUserDatabases { get; set; }

    /* ── The per-database fan-out rollup (#2472) ─────────────────────────────────────────────────────
       Four parts of ONE run — the window's dearest single item and the run that carried it. NULL on
       every collector that does not fan out, which is 36 of the 41: the columns are only written by a
       productive per-database run.

       They exist because the tail statistics above cannot answer this. MaxDurationMs and P95DurationMs
       aggregate over RUNS, and each run is one blended row, so the two shapes an operator has to tell
       apart — an even fan-out and one dominated by a single database — produce identical values in
       both. Worse on this fleet: query_store's runs are 84% empty enumerations on a busy shard and
       100% on a quiet one, so its p95/avg ratio is already saturated by empty-versus-productive and
       says nothing at all about which database is expensive. */

    /// <summary>How many items that run fanned out over, or null when it did not fan out. The
    /// denominator of <see cref="FanoutDominance"/>: without it a slowest-item duration is a number with
    /// no baseline, because "62 seconds" means something different across 2 databases and across 12.</summary>
    public int? FanoutItems { get; set; }

    /// <summary>The dearest item in the window — a database name, for every fan-out that exists today.</summary>
    public string? SlowestItem { get; set; }

    /// <summary>What that item cost, SQL plus storage.</summary>
    public int? SlowestItemMs { get; set; }

    /// <summary>The whole run that item came from, so the share is against the number the operator sees
    /// on the collection_log row rather than against a sum of item slices.</summary>
    public int? SlowestRunDurationMs { get; set; }

    /// <summary>
    /// The answer, as one number: 1.0 is a perfectly even fan-out and it rises with concentration. Eight
    /// databases at 10.1s each gives 1.0; one at 62s beside seven at 2.7s gives 6.1 — the same 80,900 ms
    /// run either way. Roughly 2.0 or above is the shape a per-database schedule override or a stagger
    /// can actually target; near 1.0 says the cost is the fan-out's WIDTH and bounded parallelism is the
    /// lever instead (#2468).
    ///
    /// <para>Null when the collector does not fan out, and also when the run's duration is zero — a
    /// ratio against nothing is not a smaller answer, it is a wrong one.</para>
    /// </summary>
    public double? FanoutDominance =>
        FanoutItems is > 0 && SlowestItemMs.HasValue && SlowestRunDurationMs is > 0
            ? (double)SlowestItemMs.Value * FanoutItems.Value / SlowestRunDurationMs.Value
            : null;

    public double FailureRatePercent => TotalRuns > 0 ? (double)ErrorCount / TotalRuns * 100 : 0;

    public double HoursSinceLastSuccess => LastSuccessTime.HasValue
        ? (DateTime.UtcNow - LastSuccessTime.Value).TotalHours
        : 999;

    /// <summary>The collector's default cadence from the shared <see cref="CollectorScheduleDefaults"/>
    /// (0 for an on-load or unknown collector — both fall to the floor thresholds). The banding uses the
    /// shipped default, not the resolved per-server override, so all three surfaces stay in parity.
    /// Internal since #2296: the tool's sweep-pressure roll-up amortizes each collector's average
    /// duration by this same cadence, so both readers of it share one resolution.</summary>
    internal int FrequencyMinutes =>
        CollectorScheduleDefaults.All.TryGetValue(CollectorName, out var schedule) ? schedule.FrequencyMinutes : 0;

    public string HealthStatus => CollectorHealthClassifier.Classify(
        TotalRuns, SuccessCount, ErrorCount, PermissionDeniedCount,
        HoursSinceLastSuccess, FrequencyMinutes, CollectorHealthClassifier.IsOnLoadCollector(CollectorName));
}
