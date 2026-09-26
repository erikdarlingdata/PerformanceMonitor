/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service;

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

    /// <summary>One raw CPU ring-buffer sample: sample_time (naive UTC — the stored UTC twin since V134, else de-skewed) + the SQL and
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
        string? ErrorMessage,
        double? SqlOpenMs = null,
        double? SqlDrainMs = null,
        double? WatermarkMs = null,
        long? DrainRowsRead = null,
        long? DrainBytesRead = null,
        double? DrainLastReadMs = null,
        int? TargetSessionId = null,
        double? SweepPeerMaxMs = null,
        /* V110 (#2860): the per-database fetch split, SUMMED across the run's fan-out. Nullable per HALF -
           a run that fetched text but no plans has the five text figures and NULL plans. NULL means no
           fetch ran (see V110's doc comment for why that is the honest reading here and why it cannot be
           told apart from a sub-millisecond fetch). Never populated on the same row as SqlOpenMs above:
           these come from the ENUMERATED path, which never sets V108's measured flag, and those come from
           the server-scoped one, which performs no deferred fetch. The two are complementary, not
           alternatives. */
        double? PlanFetchProbeMs = null,
        double? PlanFetchTargetMs = null,
        double? PlanFetchWriteMs = null,
        long? PlanFetchIdsAttempted = null,
        long? PlanFetchProbeIds = null,
        double? TextFetchProbeMs = null,
        double? TextFetchTargetMs = null,
        double? TextFetchWriteMs = null,
        long? TextFetchIdsAttempted = null,
        long? TextFetchProbeIds = null)
    {
        /* The residual, derived rather than read (V108 stores no other_ms column on purpose): open + drain
           + this SUM to SqlDurationMs by construction, so a large value here is a real finding - cost in
           our own code between the phases, in neither database - and never a stale copy. NULL when the run
           recorded no split, so "not measured" stays distinct from "measured zero". Clamped at zero: the
           phases run on separate stopwatches and tiny skew must not surface as a negative. */
        public double? SqlOtherMs =>
            SqlDurationMs is null || SqlOpenMs is null || SqlDrainMs is null
                ? null
                : Math.Max(0, SqlDurationMs.Value - SqlOpenMs.Value - SqlDrainMs.Value);

        /// <summary>
        /// The milliseconds inside <see cref="SqlDurationMs"/> that were spent against the monitoring
        /// STORE rather than the monitored target (#3192). NULL when this run performed no deferred fetch,
        /// which is every collector but the plan/text-fetching ones and most runs of even those.
        ///
        /// <para><b>Why a target-side column contains store time at all.</b> On the ENUMERATED path the
        /// driver's per-item stopwatch wraps the whole <c>readItem</c> closure
        /// (<c>EnumeratedCollectorDriver.RunAsync</c>), and for <c>query_store</c> that closure calls
        /// <c>FetchAndStorePlansAsync</c> / <c>FetchAndStoreQueryTextAsync</c> — each of which round-trips
        /// the store to learn what content is already held and then writes back what came off the target.
        /// Two of those three steps are Postgres, and all three are billed to <c>sql_duration_ms</c>. The
        /// store probe is the largest single term in both: 55.4% of <c>plan_fetch</c> and 80.6% of
        /// <c>text_fetch</c> measured over 38.2 h on 42 members (V110), and on one production run 107,334 ms
        /// of a 124,972 ms "target-side" figure — 86% — against a plan-plus-text target time of 6,494 ms.</para>
        ///
        /// <para><b>Probe and write, not target.</b> <c>*FetchTargetMs</c> is genuinely the monitored
        /// server's work and belongs where it is; only the probe round trip and the write-back are ours.</para>
        ///
        /// <para><b>Derived, never stored</b> — the <see cref="SqlOtherMs"/> and #2859 rule: a persisted copy
        /// could drift from the parent it decomposes, and deriving it means it applies RETROACTIVELY to every
        /// row written since V110 rather than only to rows written after this change. Nothing about
        /// <c>sql_duration_ms</c> moves, so the 90-day <c>collector_cost</c> series and the rows already in
        /// the store stay comparable with each other and with what follows.</para>
        ///
        /// <para><b>A FLOOR on the store share, not the whole of it, and the gap is named rather than
        /// implied.</b> The enumerated path's per-item watermark refresh is also inside the same stopwatch and
        /// is also a store read — plus a store WRITE on the catch-up/adaptive path
        /// (<c>CollectorContext.PerItemWatermarkMs</c>) — but that path never sets V108's measured flag, so
        /// <c>watermark_ms</c> is NULL on precisely the rows this property is non-null on and the component is
        /// recorded nowhere. So <c>SqlDurationMs - SqlStoreMs</c> is an UPPER bound on target-side time, not
        /// the target-side time.</para>
        /// </summary>
        public double? SqlStoreMs =>
            PlanFetchProbeMs is null && TextFetchProbeMs is null
                ? null
                : (PlanFetchProbeMs ?? 0) + (PlanFetchWriteMs ?? 0)
                    + (TextFetchProbeMs ?? 0) + (TextFetchWriteMs ?? 0);
    }

    /// <summary>One database file's latest I/O snapshot; avg latency is computed by the tool.
    /// <para><paramref name="SampleIntervalSeconds"/> (#3540): the measured seconds the deltas accrued over;
    /// 0 is the calculator's "no delta knowable" marker (first sighting, counter reset, gap past the policy)
    /// and null is a pre-V127 row that never recorded one. The tool reports latency as null on a 0 rather than
    /// the "0.00 ms" a restart used to render.</para></summary>
    public sealed record FileIoRow(
        string DatabaseName, string FileName, string FileType, string PhysicalName, double SizeMb,
        long DeltaReads, long DeltaWrites, long DeltaReadBytes, long DeltaWriteBytes,
        long DeltaStallReadMs, long DeltaStallWriteMs, int? SampleIntervalSeconds)
    {
        /// <summary>True when the row's deltas are the calculator's unknowable marker — a stored interval of
        /// exactly 0. NULL (pre-V127, interval never recorded) is NOT unknowable: those rows keep the
        /// pre-#3540 reading, because nothing about them can say otherwise.</summary>
        public bool IsUnknowable => SampleIntervalSeconds == 0;
    }

    /// <summary>One tempdb space-usage sample over the window.</summary>
    public sealed record TempDbSample(
        DateTime CollectionTime, double UserObjectReservedMb, double InternalObjectReservedMb,
        double VersionStoreReservedMb, double TotalReservedMb, double UnallocatedMb,
        long TotalSessionsUsingTempDb, int TopSessionId, double TopSessionTempDbMb);

    /// <summary>One perfmon counter at the latest snapshot. <c>DeltaValue</c> is null on a gauge row, which
    /// stores no delta (V132, #3653 A7); <c>CntrType</c> is the DMV's type id as stored, null on a row written
    /// before the rung.</summary>
    public sealed record PerfmonRow(string CounterName, string InstanceName, long Value, long? DeltaValue, int? CntrType = null);

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
        string DatabaseName, long QueryId, long PlanId, string QueryHash, string QueryPlanHash, string ExecutionTypeDesc, string? ModuleName,
        long TotalExecutions, double AvgDurationMs, double AvgCpuTimeMs, double AvgLogicalReads,
        double AvgLogicalWrites, double AvgPhysicalReads, double AvgRowcount, DateTime? LastExecutionTime, string QueryText,
        string? ReplicaRole);

    /// <summary>One server-list entry — the registry row plus its newest collection instant (drives the
    /// freshness-derived status the tool assigns; the viewer has no live ping either).
    /// <para><c>EngineKind</c> and <c>PostgresMajorVersion</c> ride along because the row's version label is
    /// engine-aware (#3145): without the discriminator this read fed <c>SqlMajorVersion</c> — <c>0</c> at
    /// every PostgreSQL target — through a SQL-Server-only table and published "SQL Server v0".</para>
    /// <para><c>RegisteredAt</c> is the registry's <c>created_date</c>, the server's first successful connect
    /// (#3967). The newest-collection read has no window, but the collection log's retention bounds what it
    /// can see, and the registration is what tells a server whose history retention dropped (Offline) from
    /// one that has never collected.</para></summary>
    public sealed record ServerListRow(
        int ServerId,
        string ServerName,
        string? DisplayName,
        int? SqlMajorVersion,
        DateTime? LastCollection,
        string? EngineKind = null,
        int? PostgresMajorVersion = null,
        DateTime? RegisteredAt = null);

    /// <summary>The latest server_properties snapshot (Lite's <c>ServerPropertiesRow</c>).
    /// <paramref name="UtcOffsetMinutes"/> is the offset IN FORCE at that collection (V16; null on a pre-V16 row)
    /// and <paramref name="TimeZoneId"/> the engine's own zone name beside it (V134, #3653 item 13, Q8) — null
    /// where the engine cannot say, which is every SQL Server before 2022 and a real, common value rather than
    /// a miss.</summary>
    public sealed record ServerPropertiesReadRow(
        DateTime CollectionTime, string Edition, string ProductVersion, string ProductLevel, string? ProductUpdateLevel,
        int EngineEdition, int CpuCount, int HyperthreadRatio, long PhysicalMemoryMb, int SocketCount, int CoresPerSocket,
        bool IsHadrEnabled, bool IsClustered, string? EnterpriseFeatures, string? ServiceObjective,
        int? UtcOffsetMinutes = null, string? TimeZoneId = null);

    /* ─────────────────────────── CPU ─────────────────────────── */

    /// <summary>
    /// Raw per-sample CPU (the viewer's <c>CpuUtilizationSql</c>): every ring-buffer sample since the
    /// window start, its instant in naive UTC — the stored <c>sample_time_utc</c> where the row carries one
    /// (V134, #3653 item 13, Q7: the collector writes the same instant in UTC beside the unchanged local
    /// stamp), else <c>sample_time</c> de-skewed from the monitored server's LOCAL wall clock by subtracting
    /// the per-batch UTC offset (#1262). The COALESCE order is the ruling ("readers prefer the new column
    /// when present"): a post-rung row is placed by a measured UTC instant, exact across a DST transition;
    /// a pre-rung row keeps the derivation, which rounds a straddling batch to one side and is the hour-wrong
    /// placement the column retires — nothing is backfilled, because the offset a server had at a past
    /// sample's instant is what the store never recorded. Windows on <c>collection_time</c> (the reliable
    /// naive-UTC clock, not the server-local sample_time). Reads the base <c>cpu_utilization_stats</c> table
    /// (the de-skew window function needs collection_time alongside sample_time). The output alias stays
    /// <c>sample_time</c>, so <c>ORDER BY sample_time</c> orders on the projected UTC value and the payload
    /// field keeps its name while its value is UTC either way. $1 server_id, $2 window start, $3 window end
    /// (naive UTC).
    /// </summary>
    public const string CpuUtilizationSql = """
        SELECT
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
        AND   collection_time <= $3
        ORDER BY sample_time
        """;

    public static async Task<List<CpuSample>> GetCpuUtilizationAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var samples = new List<CpuSample>();
        await using var command = postgres.CreateCommand(CpuUtilizationSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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

    /// <summary>
    /// CPU BUCKETED (#3960): <see cref="CpuUtilizationSql"/>'s de-skewed samples, byte for byte, averaged per bucket of
    /// <c>$4</c> minutes on the samples' own UTC instant — the tool used to bucket every sample to the minute in C#,
    /// after reading a week of them (40,000 rows on an Azure SQL DB source) — with the busiest sample's SQL and total
    /// CPU beside the averages and the sample count. A NULL reading counts as 0, as the per-sample reader always read
    /// it. Each point is stamped at its bucket's start and NOT clamped to the window's: the window is on
    /// collection_time, and a collection can carry samples from before it, which the per-minute points always showed
    /// at their own minute. $1 server_id, $2/$3 window (naive UTC), $4 the bucket width in minutes.
    /// </summary>
    public const string CpuUtilizationBucketedSql = $"""
        SELECT
            date_bin(CAST($4 AS integer) * INTERVAL '1 minute', sample_time, {TrendBucketSql.OriginSql}) AS bucket_start,
            AVG(COALESCE(sqlserver_cpu_utilization, 0)) AS sql_server_cpu,
            AVG(COALESCE(other_process_cpu_utilization, 0)) AS other_process_cpu,
            AVG(COALESCE(sqlserver_cpu_utilization, 0) + COALESCE(other_process_cpu_utilization, 0)) AS total_cpu,
            AVG(GREATEST(0, 100 - (COALESCE(sqlserver_cpu_utilization, 0) + COALESCE(other_process_cpu_utilization, 0)))) AS idle_cpu,
            MAX(COALESCE(sqlserver_cpu_utilization, 0)) AS peak_sql_server_cpu,
            MAX(COALESCE(sqlserver_cpu_utilization, 0) + COALESCE(other_process_cpu_utilization, 0)) AS peak_total_cpu,
            COUNT(*) AS samples
        FROM (
        {CpuUtilizationSql}
        ) AS samples
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="CpuUtilizationBucketedSql"/>.</summary>
    public static async Task<List<CpuBucketPoint>> GetCpuBucketsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        var items = new List<CpuBucketPoint>();
        await using var command = postgres.CreateCommand(CpuUtilizationBucketedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new CpuBucketPoint(
                reader.GetDateTime(0),
                Convert.ToDouble(reader.GetValue(1)),
                Convert.ToDouble(reader.GetValue(2)),
                Convert.ToDouble(reader.GetValue(3)),
                Convert.ToDouble(reader.GetValue(4)),
                Convert.ToInt32(reader.GetValue(5)),
                Convert.ToInt32(reader.GetValue(6)),
                reader.GetInt64(7)));
        }

        return items;
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
    /// window (naive UTC), $4 row cap.
    ///
    /// <para>The cap is a PARAMETER, not a literal (#3541 A3). It was <c>LIMIT 50</c> while the tool advertised
    /// a <c>limit</c> up to 1,000 and applied it with <c>Take(limit)</c>, so a caller asking for every wait
    /// type on a server that had observed 80 silently got 50 — the same shape <c>DarlingPgWaitReader</c> fixed
    /// for the PostgreSQL twin. The tool passes <c>limit + 1</c> and reads the extra row as truncation.</para>
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
        LIMIT $4
        """;

    /// <summary>The <paramref name="cap"/> heaviest wait types over the window. Callers detecting truncation
    /// pass <c>limit + 1</c> and read the extra row as the signal.</summary>
    public static async Task<List<WaitStatRow>> GetWaitStatsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int cap, CancellationToken cancellationToken = default)
    {
        var rows = new List<WaitStatRow>();
        await using var command = postgres.CreateCommand(WaitStatsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, cap);
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// A single wait type's per-second trend — Lite's <c>GetWaitStatsTrendAsync</c>: the interval rate is
    /// this collection's delta divided by the seconds the delta accrued over. $1 server_id, $2 wait_type,
    /// $3/$4 window (naive UTC).
    ///
    /// <para><b>The interval is the STORED one where the row has it (#3540).</b> <c>wait_stats</c> carries
    /// <c>sample_interval_seconds</c> since V127 — the calculator's measured seconds, 0 when no delta was
    /// knowable (first sighting, counter reset, a gap past the policy). A 0 maps to NULL through
    /// <c>NULLIF</c>, so the rate is NULL rather than the confident 0.00 ms/sec this read used to emit at
    /// exactly the moments (restarts) it was unknowable; the reader drops the row (a missing sample, never a
    /// fabricated idle one). A NULL interval is a pre-V127 row whose interval was never recorded, and for
    /// those the LAG over collection_time (the truncate-then-diff epoch idiom proven value-identical
    /// DuckDB↔Postgres) is what this read always did, so history keeps rendering. The first row of the
    /// window has no prior and no stored interval either way — NULL, not 0.</para>
    /// </summary>
    public const string WaitTrendSql = $"""
        WITH {WaitRawCte}
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS signal_wait_time_ms_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>The wait trend's per-row read (#3960): shared, so the per-collection statement and the bucketed one
    /// read the same rows with the same three-state interval.</summary>
    private const string WaitRawCte = """
        raw AS
        (
            SELECT
                collection_time,
                delta_wait_time_ms,
                delta_signal_wait_time_ms,
                CASE WHEN sample_interval_seconds IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                     ELSE NULLIF(sample_interval_seconds, 0)
                END AS interval_seconds
            FROM v_wait_stats
            WHERE server_id = $1
            AND   wait_type = $2
            AND   collection_time >= $3
            AND   collection_time <= $4
        )
        """;

    /// <summary>
    /// The wait trend BUCKETED (#3960): <see cref="WaitTrendSql"/>'s rows gathered into buckets of <c>$5</c> minutes.
    /// Only a collection whose rate is knowable counts — each one's wait, signal wait and seconds are NULL through a
    /// no-ELSE CASE otherwise, the same collections the per-collection reader drops — and a bucket holding none of
    /// them is left out, as the reader left such a collection out. A bucket's rate is its summed wait over the
    /// seconds its rated collections covered — time-weighted, never an average of per-collection rates — and its
    /// peak is the busiest single collection, so a one-minute pile-up survives a ten-minute bucket.
    /// $1 server_id, $2 wait_type, $3/$4 window (naive UTC), $5 the bucket width in minutes.
    /// </summary>
    public const string WaitTrendBucketedSql = $"""
        WITH {WaitRawCte},
        rated AS
        (
            SELECT
                collection_time,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) END AS rated_wait_ms,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_signal_wait_time_ms AS DOUBLE PRECISION) END AS rated_signal_ms,
                CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds,
                CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds END AS wait_time_ms_per_second
            FROM raw
        )
        SELECT
            GREATEST(date_bin(CAST($5 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $3) AS bucket_start,
            SUM(rated_wait_ms) / SUM(rated_seconds) AS wait_time_ms_per_second,
            SUM(rated_signal_ms) / SUM(rated_seconds) AS signal_wait_time_ms_per_second,
            MAX(wait_time_ms_per_second) AS peak_wait_time_ms_per_second
        FROM rated
        GROUP BY 1
        HAVING COUNT(rated_seconds) > 0
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="WaitTrendBucketedSql"/>: only buckets holding a rated collection come back.</summary>
    public static async Task<List<WaitBucketPoint>> GetWaitBucketsAsync(
        NpgsqlDataSource postgres, int serverId, string waitType, DateTime startUtc, DateTime endUtc, int bucketMinutes,
        CancellationToken cancellationToken = default)
    {
        var items = new List<WaitBucketPoint>();
        await using var command = postgres.CreateCommand(WaitTrendBucketedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        AddText(command, waitType);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new WaitBucketPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                reader.IsDBNull(3) ? null : Convert.ToDouble(reader.GetValue(3))));
        }

        return items;
    }

    public static async Task<List<WaitTrendPoint>> GetWaitTrendAsync(
        NpgsqlDataSource postgres, int serverId, string waitType, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<WaitTrendPoint>();
        await using var command = postgres.CreateCommand(WaitTrendSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        AddText(command, waitType);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is an unknowable interval (#3540) — the row is dropped rather than read as 0. Both
               rates share one interval, so they are NULL together; the first is the test. */
            if (reader.IsDBNull(1))
            {
                continue;
            }

            items.Add(new WaitTrendPoint(
                reader.GetDateTime(0),
                reader.GetDouble(1),
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
    /// <c>collection_time</c> rides along on every row (#3541 A10) so the tool can say WHEN the snapshot it
    /// serves was taken — the same statement as the rows, never a second read that could stamp the next one.
    /// </summary>
    public const string LatestMemoryClerksSql = """
        SELECT clerk_type, CAST(memory_mb AS double precision), collection_time
        FROM v_memory_clerks
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_memory_clerks WHERE server_id = $1)
        ORDER BY memory_mb DESC
        """;

    public static async Task<LatestSnapshot<MemoryClerkRow>> GetLatestMemoryClerksAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<MemoryClerkRow>();
        DateTime? capturedAt = null;
        await using var command = postgres.CreateCommand(LatestMemoryClerksSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new MemoryClerkRow(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1)));
            capturedAt ??= reader.GetDateTime(2);
        }

        return new LatestSnapshot<MemoryClerkRow>(capturedAt, rows);
    }

    /* ─────────────────────────── file I/O ─────────────────────────── */

    /// <summary>
    /// The latest file-I/O snapshot per database file — Lite's <c>GetLatestFileIoStatsAsync</c>, ordered
    /// by total stall descending; avg latency (stall/op) is computed by the tool. size_mb is
    /// <c>numeric</c> → double precision; the delta columns are bigint. $1 server_id. <c>collection_time</c>
    /// is the trailing column (#3541 A10): the snapshot's stamp, read once and published as <c>captured_at</c>.
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
            delta_stall_write_ms,
            sample_interval_seconds,
            collection_time
        FROM v_file_io_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_file_io_stats WHERE server_id = $1)
        ORDER BY (delta_stall_read_ms + delta_stall_write_ms) DESC
        """;

    public static async Task<LatestSnapshot<FileIoRow>> GetLatestFileIoStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<FileIoRow>();
        DateTime? capturedAt = null;
        await using var command = postgres.CreateCommand(LatestFileIoStatsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
                reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetInt32(11)));
            capturedAt ??= reader.GetDateTime(12);
        }

        return new LatestSnapshot<FileIoRow>(capturedAt, rows);
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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

    /// <summary>
    /// tempdb BUCKETED (#3960): <see cref="TempDbTrendSql"/>'s samples, byte for byte, gathered into buckets of
    /// <c>$4</c> minutes — each space figure averaged, the fullest collection's reserved and version-store space as
    /// the peaks, the most sessions any collection saw, and the single largest consumer (its session and its MB,
    /// the earliest on a tie). A NULL reading counts as 0, as the per-sample reader always read it. $1 server_id,
    /// $2/$3 window (naive UTC), $4 the bucket width in minutes.
    /// </summary>
    public const string TempDbTrendBucketedSql = $"""
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            AVG(COALESCE(user_object_reserved_mb, 0)) AS user_objects_mb,
            AVG(COALESCE(internal_object_reserved_mb, 0)) AS internal_objects_mb,
            AVG(COALESCE(version_store_reserved_mb, 0)) AS version_store_mb,
            AVG(COALESCE(total_reserved_mb, 0)) AS total_reserved_mb,
            AVG(COALESCE(unallocated_mb, 0)) AS unallocated_mb,
            MAX(COALESCE(total_reserved_mb, 0)) AS peak_total_reserved_mb,
            MAX(COALESCE(version_store_reserved_mb, 0)) AS peak_version_store_mb,
            MAX(COALESCE(total_sessions_using_tempdb, 0)) AS sessions_using_tempdb,
            (array_agg(COALESCE(top_session_id, 0) ORDER BY COALESCE(top_session_tempdb_mb, 0) DESC, collection_time))[1] AS top_consumer_session_id,
            MAX(COALESCE(top_session_tempdb_mb, 0)) AS top_consumer_mb
        FROM (
        {TempDbTrendSql}
        ) AS samples
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>Runs <see cref="TempDbTrendBucketedSql"/>.</summary>
    public static async Task<List<TempDbBucketPoint>> GetTempDbBucketsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int bucketMinutes, CancellationToken cancellationToken = default)
    {
        var items = new List<TempDbBucketPoint>();
        await using var command = postgres.CreateCommand(TempDbTrendBucketedSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        AddInt(command, bucketMinutes);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new TempDbBucketPoint(
                reader.GetDateTime(0),
                Convert.ToDouble(reader.GetValue(1)),
                Convert.ToDouble(reader.GetValue(2)),
                Convert.ToDouble(reader.GetValue(3)),
                Convert.ToDouble(reader.GetValue(4)),
                Convert.ToDouble(reader.GetValue(5)),
                Convert.ToDouble(reader.GetValue(6)),
                Convert.ToDouble(reader.GetValue(7)),
                Convert.ToInt64(reader.GetValue(8)),
                Convert.ToInt32(reader.GetValue(9)),
                Convert.ToDouble(reader.GetValue(10))));
        }

        return items;
    }

    /* ─────────────────────────── perfmon ─────────────────────────── */

    /// <summary>
    /// The latest perfmon counters — Lite's <c>GetLatestPerfmonStatsAsync</c>: counter_name /
    /// instance_name / cntr_value / delta_cntr_value at the newest collection, with that collection's
    /// <c>collection_time</c> trailing (#3541 A10, published once as <c>captured_at</c>) and the row's
    /// stored <c>cntr_type</c> after it (V132). $1 server_id.
    /// </summary>
    public const string LatestPerfmonStatsSql = """
        SELECT
            counter_name,
            instance_name,
            cntr_value,
            delta_cntr_value,
            collection_time,
            cntr_type
        FROM v_perfmon_stats
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM v_perfmon_stats WHERE server_id = $1)
        ORDER BY counter_name
        """;

    public static async Task<LatestSnapshot<PerfmonRow>> GetLatestPerfmonStatsAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<PerfmonRow>();
        DateTime? capturedAt = null;
        await using var command = postgres.CreateCommand(LatestPerfmonStatsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PerfmonRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                /* NULL stays NULL: a gauge row stores no delta (V132); 0 here would be a fabricated zero. */
                reader.IsDBNull(3) ? null : reader.GetInt64(3),
                reader.IsDBNull(5) ? null : reader.GetInt32(5)));
            capturedAt ??= reader.GetDateTime(4);
        }

        return new LatestSnapshot<PerfmonRow>(capturedAt, rows);
    }

    /* ─────────────────────────── top queries ─────────────────────────── */

    /// <summary>
    /// Top query-stats groups over the window — a focused projection of the viewer's <c>TopQueriesSql</c>
    /// (the columns Lite's get_top_queries_by_cpu returns): group by (database, query_hash), sum the
    /// deltas + carry min/max spreads, rank by summed <c>delta_worker_time</c> (CPU — the tool's promise;
    /// #3523, the viewer's duration grid keeps its elapsed ranking) descending, over-fetch by
    /// 5 to drop WAITFOR shells via the latest-text LATERAL, cap at top. Summed bigints CAST back to bigint
    /// for the typed reader. The aggregate reads the base <c>query_stats</c> table (it projects no text);
    /// the text LATERAL reads <c>v_query_stats</c>, which resolves the #1767 payload dimension — the plan
    /// tools read it the same way. $1 server_id, $2/$3 window (naive UTC), $4 top, $5 database filter (NULL = all),
    /// $6 lifetime max_dop floor (0 = no parallelism filter; #3541 A13).
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
            HAVING (SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0)
            /* #3541 A13: the parallelism filter is part of the QUERY, applied to the grouped population
               BEFORE the CPU ranking and the cap. It used to run in C# over the returned top-N page, so
               parallel_only=true on a box whose twenty hottest plans were serial answered an empty page while
               the window held parallel plans further down — and the engine's own CXPACKET advice sends agents
               to exactly that call. $6 is the group's lifetime max_dop floor: 0 admits every group (the
               unfiltered read, byte-identical in result to before), 2 is parallel_only, min_dop is itself. The
               COALESCE keeps a group whose max_dop was never captured (NULL) out of a filtered page, which is
               what the C# arm did too (null read as 0, and 0 > 1 is false). */
            AND COALESCE(MAX(max_dop), 0) >= $6
            ORDER BY SUM(delta_worker_time) DESC
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
        ORDER BY r.total_cpu_us DESC
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
            HAVING (SUM(delta_execution_count) > 0 OR SUM(delta_elapsed_time) > 0)
            /* #3541 A13: same in-query parallelism floor as TopQueriesSql — see its note. Under the rollup
               the group's max_dop is the max across every fragment, so a procedure whose dynamic SQL went
               parallel in ANY fragment passes parallel_only, which is the question being asked. */
            AND COALESCE(MAX(max_dop), 0) >= $6
            ORDER BY SUM(delta_worker_time) DESC
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
        ORDER BY r.total_cpu_us DESC
        LIMIT $4
        """;

    /// <summary>The FROM-clause placeholder <see cref="TopQueriesHourlySql"/> carries — replaced with
    /// <see cref="RollupCoverage.StitchedRelationSql"/>'s answer at call time. Never hardcode
    /// <c>query_stats_interval_hourly</c> or <c>query_stats_hourly</c> in its place; see
    /// <see cref="GetTopQueriesByCpuHourlyAsync"/>.</summary>
    public const string TopQueriesHourlyFromPlaceholder = "$FROM$";

    /// <summary>
    /// #4231 stage 3: the hourly-tier twin of <see cref="TopQueriesSql"/>, over <c>query_stats_hourly</c> /
    /// <c>query_stats_interval_hourly</c> — routed here ONLY through <see cref="RollupCoverage.StitchedRelationSql"/>
    /// (never by naming either relation directly). The rollup carries neither <c>host_object_name</c> nor
    /// <c>query_text</c> (see <c>s_stitchColumnsByLegacy[QueryStatsHourlyView]</c>, <c>TimescaleSupport.cs</c>),
    /// so this groups by <c>(database_name, query_hash)</c> only — a proc-hosted statement that raw would keep
    /// split by host object COLLAPSES across host objects at this tier (a real precision loss, disclosed by
    /// the MCP tool's <c>precision_note</c>, not hidden). Ranks by <c>SUM(worker_time_sum) DESC</c> — the same
    /// CPU promise <see cref="TopQueriesSql"/> makes, over the rollup's pre-summed bucket columns rather than
    /// per-collection deltas. <c>query_text</c>/<c>host_object_name</c>/<c>distinct_texts</c> are NOT projected
    /// here — <see cref="GetTopQueriesByCpuAsync"/> resolves <c>query_text</c> with a separate follow-up LATERAL
    /// over <c>v_query_stats</c> once the ranked rollup rows are known, mirroring <see cref="TopQueriesSql"/>'s
    /// own LATERAL shape minus the host-object predicate the rollup has nothing to match. <c>$FROM$</c> is a
    /// PLACEHOLDER, substituted (string.Replace, not string.Format — the SQL text otherwise contains braces)
    /// with the FROM-clause item <see cref="RollupCoverage.StitchedRelationSql"/> returns for this window at
    /// call time — never a literal relation name. $1 server_id, $2/$3 window (naive UTC), $4 top, $5 database
    /// filter (NULL = all). <c>min_dop</c> filtering (#3541 A13) is Raw-tier only for this lane — the rollup
    /// carries no per-group DOP column — so this const takes no $6.
    /// </summary>
    public const string TopQueriesHourlySql = """
        WITH ranked AS (
            SELECT
                database_name,
                query_hash,
                CAST(SUM(execution_count_sum) AS bigint) AS total_executions,
                CAST(SUM(worker_time_sum) AS bigint) AS total_cpu_us,
                CAST(SUM(elapsed_time_sum) AS bigint) AS total_elapsed_us,
                MIN(worker_time_min) AS min_worker_time,
                MAX(worker_time_max) AS max_worker_time,
                MIN(execution_count_min) AS min_execution_count,
                MAX(execution_count_max) AS max_execution_count
            FROM $FROM$
            WHERE server_id = $1
            AND   bucket >= $2
            AND   bucket <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            GROUP BY database_name, query_hash
            HAVING (SUM(execution_count_sum) > 0 OR SUM(elapsed_time_sum) > 0)
            ORDER BY SUM(worker_time_sum) DESC
            LIMIT $4
        )
        SELECT
            r.database_name,
            r.query_hash,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.min_worker_time,
            r.max_worker_time,
            r.min_execution_count,
            r.max_execution_count
        FROM ranked AS r
        ORDER BY r.total_cpu_us DESC
        """;

    /// <summary>
    /// The rollup-tier follow-up text lookup for a hourly-routed row (#4231 stage 3) — the same LATERAL shape
    /// <see cref="TopQueriesSql"/> uses, minus the host-object predicate: the rollup has no host_object_name to
    /// match against, so the representative text is the group's latest row by <c>(database_name, query_hash)</c>
    /// alone. $1 server_id, $2 database_name, $3 query_hash.
    /// </summary>
    private const string TopQueriesHourlyTextLookupSql = """
        SELECT query_text
        FROM v_query_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        AND   query_text IS NOT NULL
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The top-N groups by CPU, ranked over the population that passes every filter. <paramref name="minMaxDop"/>
    /// is the lifetime <c>max_dop</c> floor a group must reach to be ranked at all (#3541 A13): 0 for no
    /// parallelism filter, 2 for <c>parallel_only</c>, the caller's <c>min_dop</c> otherwise — see
    /// <see cref="TopQueriesSql"/>'s HAVING note. The filter is IN the statement so the page is the top-N of the
    /// filtered population, not the filtered remainder of an unfiltered top-N.
    ///
    /// <para>#4231 stage 3: when the window has aged past raw's floor, this routes to the hourly rollup via
    /// <see cref="RetentionTierRouter.Resolve(DateTime,DateTime,bool,bool,TierCoverage)"/> over
    /// <see cref="RollupCoverage.For"/>'s <c>(QueryStatsHourlyView, QueryStatsDailyView)</c> pair — Daily is out
    /// of scope for this lane, so a Daily verdict is clamped to Hourly (a top-N-by-CPU daily rollup answer is a
    /// separate ask). The parallelism filter (<paramref name="minMaxDop"/>) and <paramref name="rollUpByHostObject"/>
    /// are Raw-tier-only refinements the rollup cannot answer (no per-group DOP, no host_object_name); an
    /// hourly-routed read ignores <paramref name="rollUpByHostObject"/> (the rollup groups by hash only) and
    /// runs unfiltered by DOP — the MCP tool discloses both via <c>tier_used</c>/<c>precision_note</c>.</para>
    /// </summary>
    public static async Task<List<TopQueryRow>> GetTopQueriesByCpuAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName,
        bool rollUpByHostObject = false, int minMaxDop = 0, CancellationToken cancellationToken = default)
    {
        var result = await GetTopQueriesByCpuRoutedAsync(
            postgres, serverId, startUtc, endUtc, top, databaseName, rollUpByHostObject, minMaxDop, cancellationToken);
        return result.Rows;
    }

    /// <summary>#4231 stage 3: which tier <see cref="GetTopQueriesByCpuRoutedAsync"/> actually read —
    /// <see cref="RetentionTier.Raw"/> or <see cref="RetentionTier.Hourly"/> (Daily is clamped to Hourly, this
    /// lane's scope); the MCP tool's <c>tier_used</c> comes from here.</summary>
    public sealed record TopQueriesReadResult(List<TopQueryRow> Rows, RetentionTier Tier);

    /// <summary>
    /// #4231 stage 3: <see cref="GetTopQueriesByCpuAsync"/>'s routed form, exposing the tier it read so a
    /// caller can disclose it. Tier is decided over the LEGACY pair's coverage
    /// (<see cref="RollupCoverage.For"/>, the deeper of the legacy/successor floors); Daily is out of scope for
    /// this lane (query_stats_db_hourly's successor and a daily top-N answer are both separate asks) and is
    /// clamped to Hourly. <paramref name="rollUpByHostObject"/> and <paramref name="minMaxDop"/> are Raw-tier-
    /// only refinements the rollup cannot answer (no per-group DOP, no host_object_name) — an hourly-routed read
    /// ignores both.
    /// </summary>
    public static async Task<TopQueriesReadResult> GetTopQueriesByCpuRoutedAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName,
        bool rollUpByHostObject = false, int minMaxDop = 0, CancellationToken cancellationToken = default)
    {
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, startUtc, rollups.QueryGrainHourly, dailyAvailable: false,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));
        if (tier == RetentionTier.Daily)
        {
            tier = RetentionTier.Hourly;
        }

        if (tier == RetentionTier.Hourly)
        {
            var hourlyRows = await GetTopQueriesByCpuHourlyAsync(postgres, coverage, serverId, startUtc, endUtc, top, databaseName, cancellationToken);
            return new TopQueriesReadResult(hourlyRows, RetentionTier.Hourly);
        }

        var rows = new List<TopQueryRow>();
        /* #2235: same parameters, same columns, different GROUP BY — see TopQueriesByHostObjectSql. */
        await using var command = postgres.CreateCommand(rollUpByHostObject ? TopQueriesByHostObjectSql : TopQueriesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, top);
        AddNullableText(command, databaseName);
        AddInt(command, minMaxDop);
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

        return new TopQueriesReadResult(rows, RetentionTier.Raw);
    }

    /// <summary>
    /// #4231 stage 3: the hourly-rollup arm of <see cref="GetTopQueriesByCpuRoutedAsync"/> — builds
    /// <see cref="TopQueriesHourlySql"/>'s FROM clause ONLY through
    /// <see cref="RollupCoverage.StitchedRelationSql"/> (the standing gate: a raw-vs-rollup reader never names
    /// <c>query_stats_interval_hourly</c> or <c>query_stats_hourly</c> directly), runs the ranked read, then
    /// resolves each row's representative <c>query_text</c> with one follow-up query per row via
    /// <see cref="TopQueriesHourlyTextLookupSql"/> (the rollup has no per-row text to project). Rows carry
    /// <c>host_object_name = null</c> and <c>distinct_texts = 0</c> — the rollup has neither column, and the
    /// MCP tool's <c>precision_note</c> says so; <c>min_dop</c>/<c>max_dop</c> are 0 (no per-group DOP on the
    /// rollup).
    /// </summary>
    private static async Task<List<TopQueryRow>> GetTopQueriesByCpuHourlyAsync(
        NpgsqlDataSource postgres, RollupCoverage coverage, int serverId, DateTime startUtc, DateTime endUtc,
        int top, string? databaseName, CancellationToken cancellationToken)
    {
        var fromClause = coverage.StitchedRelationSql(
            TimescaleSupport.QueryStatsHourlyView, "f", startUtc, RollupCoverage.StitchTier.Hourly);
        var sql = TopQueriesHourlySql.Replace(TopQueriesHourlyFromPlaceholder, fromClause, StringComparison.Ordinal);

        var rankedRows = new List<(string Database, string QueryHash, long TotalExecutions, long TotalCpuUs, long TotalElapsedUs, long MinWorkerTime, long MaxWorkerTime, long MinExecCount, long MaxExecCount)>();
        await using (var command = postgres.CreateCommand(sql))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            AddWindow(command, serverId, startUtc, endUtc);
            AddInt(command, top);
            AddNullableText(command, databaseName);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rankedRows.Add((
                    reader.IsDBNull(0) ? "" : reader.GetString(0),
                    reader.IsDBNull(1) ? "" : reader.GetString(1),
                    reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                    reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                    reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    reader.IsDBNull(8) ? 0 : reader.GetInt64(8)));
            }
        }

        var rows = new List<TopQueryRow>(rankedRows.Count);
        foreach (var r in rankedRows)
        {
            string queryText = "";
            await using (var textCommand = postgres.CreateCommand(TopQueriesHourlyTextLookupSql))
            {
                textCommand.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                AddInt(textCommand, serverId);
                AddNullableText(textCommand, r.Database);
                AddNullableText(textCommand, r.QueryHash);
                var textResult = await textCommand.ExecuteScalarAsync(cancellationToken);
                if (textResult is string text)
                {
                    queryText = text;
                }
            }

            rows.Add(new TopQueryRow(
                r.Database, r.QueryHash,
                HostObjectName: null,   /* #4231 stage 3: the rollup has no host_object_name column. */
                QueryPlanHash: "", SqlHandle: "", PlanHandle: "",
                TotalExecutions: r.TotalExecutions, TotalCpuUs: r.TotalCpuUs, TotalElapsedUs: r.TotalElapsedUs,
                TotalLogicalReads: 0, TotalLogicalWrites: 0, TotalPhysicalReads: 0, TotalRows: 0, TotalSpills: 0,
                MinDop: 0, MaxDop: 0,
                MinCpuUs: r.MinWorkerTime, MaxCpuUs: r.MaxWorkerTime, MinElapsedUs: 0, MaxElapsedUs: 0,
                QueryText: queryText,
                /* #4231 stage 3: distinct_texts is unavailable at rollup grain — 0 means "no dimension read",
                   same reading TopQueryRow's own doc gives 0 (pre-dimension legacy rows). */
                DistinctTexts: 0,
                DistinctQueryHashes: 1));
        }

        return rows;
    }

    /* ─────────────────────────── top procedures ─────────────────────────── */

    /// <summary>
    /// Top procedure-stats groups over the window — a focused projection of the viewer's
    /// <c>TopProceduresSql</c> (the columns Lite's get_top_procedures_by_cpu returns): group by
    /// (database, schema, object, type), sum the deltas + carry min/max spreads, rank by summed
    /// <c>delta_worker_time</c> (CPU — the tool's promise; #3523) descending, cap at top. Reads the base <c>procedure_stats</c> table
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
        ORDER BY SUM(delta_worker_time) DESC
        LIMIT $4
        """;

    /// <summary>The FROM-clause placeholder <see cref="TopProceduresHourlySql"/> carries — replaced with
    /// <see cref="RollupCoverage.StitchedRelationSql"/>'s answer at call time. Never hardcode
    /// <c>procedure_stats_interval_hourly</c> or <c>procedure_stats_hourly</c> in its place; see
    /// <see cref="GetTopProceduresByCpuHourlyAsync"/>.</summary>
    public const string TopProceduresHourlyFromPlaceholder = "$FROM$";

    /// <summary>
    /// #4231 stage 3b: the hourly-tier twin of <see cref="TopProceduresSql"/>, over <c>procedure_stats_hourly</c> /
    /// <c>procedure_stats_interval_hourly</c> — routed here ONLY through <see cref="RollupCoverage.StitchedRelationSql"/>
    /// (never by naming either relation directly). The rollup carries neither <c>object_type</c> nor
    /// <c>sql_handle</c>/<c>plan_handle</c> (see <c>s_stitchColumnsByLegacy[ProcedureStatsHourlyView]</c>,
    /// <c>TimescaleSupport.cs</c>), so this groups by <c>(database_name, schema_name, object_name)</c> only —
    /// <c>object_type</c> is disclosed as null by the MCP tool's <c>precision_note</c>, not hidden. Ranks by
    /// <c>SUM(worker_time_sum) DESC</c> — the same CPU promise <see cref="TopProceduresSql"/> makes, over the
    /// rollup's pre-summed bucket columns rather than per-collection deltas. <c>$FROM$</c> is a PLACEHOLDER,
    /// substituted (string.Replace, not string.Format) with the FROM-clause item
    /// <see cref="RollupCoverage.StitchedRelationSql"/> returns for this window at call time — never a literal
    /// relation name. $1 server_id, $2/$3 window (naive UTC), $4 top, $5 database filter (NULL = all).
    /// </summary>
    public const string TopProceduresHourlySql = """
        WITH ranked AS (
            SELECT
                database_name,
                schema_name,
                object_name,
                CAST(SUM(execution_count_sum) AS bigint) AS total_executions,
                CAST(SUM(worker_time_sum) AS bigint) AS total_cpu_us,
                CAST(SUM(elapsed_time_sum) AS bigint) AS total_elapsed_us,
                MIN(worker_time_min) AS min_worker_time,
                MAX(worker_time_max) AS max_worker_time,
                MIN(elapsed_time_min) AS min_elapsed_time,
                MAX(elapsed_time_max) AS max_elapsed_time
            FROM $FROM$
            WHERE server_id = $1
            AND   bucket >= $2
            AND   bucket <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            GROUP BY database_name, schema_name, object_name
            HAVING (SUM(execution_count_sum) > 0 OR SUM(elapsed_time_sum) > 0)
            ORDER BY SUM(worker_time_sum) DESC
            LIMIT $4
        )
        SELECT
            r.database_name,
            r.schema_name,
            r.object_name,
            r.total_executions,
            r.total_cpu_us,
            r.total_elapsed_us,
            r.min_worker_time,
            r.max_worker_time,
            r.min_elapsed_time,
            r.max_elapsed_time
        FROM ranked AS r
        ORDER BY r.total_cpu_us DESC
        """;

    /// <summary>
    /// #4231 stage 3b: which tier <see cref="GetTopProceduresByCpuRoutedAsync"/> actually read —
    /// <see cref="RetentionTier.Raw"/> or <see cref="RetentionTier.Hourly"/> (Daily is clamped to Hourly, this
    /// lane's scope); the MCP tool's <c>tier_used</c> comes from here.</summary>
    public sealed record TopProceduresReadResult(List<TopProcedureRow> Rows, RetentionTier Tier);

    public static async Task<List<TopProcedureRow>> GetTopProceduresByCpuAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName, CancellationToken cancellationToken = default)
    {
        var result = await GetTopProceduresByCpuRoutedAsync(postgres, serverId, startUtc, endUtc, top, databaseName, cancellationToken);
        return result.Rows;
    }

    /// <summary>
    /// #4231 stage 3b: <see cref="GetTopProceduresByCpuAsync"/>'s routed form, exposing the tier it read so a
    /// caller can disclose it. Tier is decided over the LEGACY pair's coverage (<see cref="RollupCoverage.For"/>);
    /// Daily is out of scope for this lane and is clamped to Hourly.
    /// </summary>
    public static async Task<TopProceduresReadResult> GetTopProceduresByCpuRoutedAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName, CancellationToken cancellationToken = default)
    {
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, startUtc, rollups.ProcedureGrainHourly, dailyAvailable: false,
            coverage.For(TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView));
        if (tier == RetentionTier.Daily)
        {
            tier = RetentionTier.Hourly;
        }

        if (tier == RetentionTier.Hourly)
        {
            var hourlyRows = await GetTopProceduresByCpuHourlyAsync(postgres, coverage, serverId, startUtc, endUtc, top, databaseName, cancellationToken);
            return new TopProceduresReadResult(hourlyRows, RetentionTier.Hourly);
        }

        var rows = new List<TopProcedureRow>();
        await using var command = postgres.CreateCommand(TopProceduresSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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

        return new TopProceduresReadResult(rows, RetentionTier.Raw);
    }

    /// <summary>
    /// #4231 stage 3b: the hourly-rollup arm of <see cref="GetTopProceduresByCpuRoutedAsync"/> — builds
    /// <see cref="TopProceduresHourlySql"/>'s FROM clause ONLY through <see cref="RollupCoverage.StitchedRelationSql"/>
    /// (the standing gate: a raw-vs-rollup reader never names <c>procedure_stats_interval_hourly</c> or
    /// <c>procedure_stats_hourly</c> directly). Rows carry <c>ObjectType = ""</c>, <c>SqlHandle = ""</c> and
    /// <c>PlanHandle = ""</c> — the rollup has none of those columns, and the MCP tool's <c>precision_note</c>
    /// says so.
    /// </summary>
    private static async Task<List<TopProcedureRow>> GetTopProceduresByCpuHourlyAsync(
        NpgsqlDataSource postgres, RollupCoverage coverage, int serverId, DateTime startUtc, DateTime endUtc,
        int top, string? databaseName, CancellationToken cancellationToken)
    {
        var fromClause = coverage.StitchedRelationSql(
            TimescaleSupport.ProcedureStatsHourlyView, "f", startUtc, RollupCoverage.StitchTier.Hourly);
        var sql = TopProceduresHourlySql.Replace(TopProceduresHourlyFromPlaceholder, fromClause, StringComparison.Ordinal);

        var rows = new List<TopProcedureRow>();
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, top);
        AddNullableText(command, databaseName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new TopProcedureRow(
                DatabaseName: reader.IsDBNull(0) ? "" : reader.GetString(0),
                SchemaName: reader.IsDBNull(1) ? "" : reader.GetString(1),
                ObjectName: reader.IsDBNull(2) ? "" : reader.GetString(2),
                /* #4231 stage 3b: the rollup has no object_type column. */
                ObjectType: "",
                SqlHandle: "", PlanHandle: "",
                TotalExecutions: reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                TotalCpuUs: reader.IsDBNull(4) ? 0 : reader.GetInt64(4),
                TotalElapsedUs: reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                TotalLogicalReads: 0, TotalLogicalWrites: 0, TotalPhysicalReads: 0, TotalSpills: 0,
                MinCpuUs: reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                MaxCpuUs: reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                MinElapsedUs: reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                MaxElapsedUs: reader.IsDBNull(9) ? 0 : reader.GetInt64(9)));
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
    ///
    /// <para>#4231 generalized this single-table probe into <see cref="RawWindowFloor"/>, which
    /// <c>query_stats</c> and <c>procedure_stats</c> now share rather than each carrying its own copy; this
    /// constant is <see cref="RawWindowFloor.FloorSql"/> for <see cref="RawWindowFloor.Table.QueryStoreStats"/>,
    /// kept under its original name because <c>DarlingMcpQueryStoreClutterTools</c> and the #2364 tests still
    /// reach it by this one.</para>
    /// </summary>
    public static readonly string QueryStoreWindowFloorSql = RawWindowFloor.FloorSql(RawWindowFloor.Table.QueryStoreStats);

    /// <summary>
    /// Reads <see cref="QueryStoreWindowFloorSql"/> through the shared <see cref="RawWindowFloor.GetAsync"/>, at
    /// this surface's own MCP read deadline. Null when the window holds nothing at all, which the caller reports
    /// as "nothing was read" rather than as an absence of activity. Deliberately unfiltered: the floor is a
    /// property of the tier, so a database or module filter on the top read does not narrow it.
    /// </summary>
    public static Task<DateTime?> GetQueryStoreWindowFloorAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default) =>
        RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.QueryStoreStats, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);

    /// <summary>
    /// #4231's <c>query_stats</c> arm of the same probe: <c>get_top_queries_by_cpu</c> reads the raw table only,
    /// which on a store with the rollups armed is dropped at 4 days, and its top-N-by-CPU rows say nothing about
    /// how far back the window reached (the same reasoning as <see cref="GetQueryStoreWindowFloorAsync"/>).
    /// </summary>
    public static Task<DateTime?> GetQueryStatsWindowFloorAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default) =>
        RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.QueryStats, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);

    /// <summary>
    /// #4231's <c>procedure_stats</c> arm of the same probe, for <c>get_top_procedures_by_cpu</c>.
    /// </summary>
    public static Task<DateTime?> GetProcedureStatsWindowFloorAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default) =>
        RawWindowFloor.GetAsync(postgres, RawWindowFloor.Table.ProcedureStats, serverId, startUtc, endUtc, McpCommandDeadlines.ReadSeconds, cancellationToken);

    /// <summary>
    /// Top Query Store groups over the window — a focused projection of the viewer's
    /// <c>QueryStoreTopSql</c> (the columns Lite's get_query_store_top returns): group by
    /// (database, query_id, plan_id, query_hash, execution_type_desc, replica_role), average the per-interval
    /// metrics, rank by total duration (<c>SUM(execution_count) * AVG(avg_duration_us)</c>) descending,
    /// over-fetch by 5 for the WAITFOR trim, cap at top. The avg columns are bigint (per-interval averages) →
    /// double precision before the AVG/scale. Reads the base <c>query_store_stats</c> table (no v_ view).
    /// $1 server_id, $2/$3 window (naive UTC), $4 top, $5 database (NULL = all), $6 execution outcome
    /// (NULL = all; Regular, Aborted or Exception otherwise, one row per outcome either way), $7 module_name
    /// (NULL = every module; applied to the deduplicated interval rows, before the ranking and the cap).
    /// <see cref="QueryStoreTopRawPrefix"/> + <see cref="QueryStoreTopSuffix"/>, byte-identical to this
    /// constant's prior single-string form (#3953 split it off so <see cref="QueryStoreTopTableSql"/> can
    /// share <see cref="QueryStoreTopSuffix"/>).
    /// </summary>
    public const string QueryStoreTopSql = QueryStoreTopRawPrefix + QueryStoreTopSuffix;

    /// <summary>
    /// The table twin of <see cref="QueryStoreTopSql"/> (#3953): <see cref="QueryStoreTopTablePrefix"/> reads
    /// <c>query_store_interval_wide</c> instead of the raw dedupe, sharing <see cref="QueryStoreTopSuffix"/> so
    /// the two reads cannot drift below <c>ranked</c>. Chosen per call by
    /// <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>; every unfiltered/filtered combination this read
    /// supports must agree with <see cref="QueryStoreTopSql"/> over the same window.
    /// $1 server_id, $2 the gate's clamp (<c>max(window start, raw's chunk floor)</c>), $3 window end (naive
    /// UTC — the MCP surface always supplies a literal instant here, never an open/preset end), $4 top,
    /// $5 database, $6 execution outcome.
    /// </summary>
    public const string QueryStoreTopTableSql = QueryStoreTopTablePrefix + QueryStoreTopSuffix;

    /// <summary>
    /// The raw read's head (#3953 split it off <see cref="QueryStoreTopSql"/>'s prior single-string form,
    /// byte-identical — the split itself changes nothing about the text a raw call sends): the interval dedupe
    /// over the server's raw Query Store slice, plus the $6 execution-outcome filter (kept here, before the
    /// ROW_NUMBER partition, exactly where it sat before the split). <see cref="QueryStoreTopSuffix"/> is
    /// shared with <see cref="QueryStoreTopTableSql"/>, so the two reads cannot drift above <c>ranked</c>.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 top, $5 database, $6 execution outcome.
    /// </summary>
    private const string QueryStoreTopRawPrefix = """
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
            /* Filtered HERE, before the ROW_NUMBER, not after it: execution_type_desc is in the partition,
               so dropping the other outcomes first cannot change which row wins any partition, and the window
               sort then sees only the outcome asked for. */
            AND   ($6::text IS NULL OR execution_type_desc = $6)
        ),
        """;

    /// <summary>
    /// The table read's head (#3953): <c>query_store_interval_wide</c> already holds the latest snapshot per
    /// interval — the raw prefix's ROW_NUMBER dedupe above, maintained as the table is written — so this reads
    /// it directly and sets <c>rn</c> to a literal 1 rather than computing a rank. $2 is the gate's own clamp,
    /// <c>max(window start, raw's chunk floor)</c>: raw chunks drop whole, so bounding the table read there
    /// returns exactly the raw read's own answer over the snapshots raw still holds. $3 is bound the same way
    /// raw's own $3 is (a plain lower bound, never NULL): the MCP surface has no concept of an open/preset end.
    /// $6 is repeated here, before this CTE's own GROUP BY-eligible rows reach <c>ranked</c>, mirroring the raw
    /// prefix's placement — <c>execution_type_desc</c> is a <c>ranked</c> GROUP BY key, not filtered again
    /// there, so an unfiltered table CTE would silently ignore the outcome filter.
    /// </summary>
    private const string QueryStoreTopTablePrefix = """
        WITH deduped AS (
            SELECT
                *,
                1 AS rn
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($5::text IS NULL OR database_name = $5)
            AND   ($6::text IS NULL OR execution_type_desc = $6)
        ),
        """;

    /// <summary>Everything from <c>ranked</c> down, shared by <see cref="QueryStoreTopSql"/> and
    /// <see cref="QueryStoreTopTableSql"/> — both prefixes above produce the same "one row per identity, every
    /// column deduped's dedupe/the table's own upsert already kept" shape, so this aggregates either one
    /// identically. $7 (module_name) lives here, unchanged from the pre-split statement's own position
    /// (<c>QueryStoreSql_AppliesModuleFilterAfterDedupAndBeforeRankingLimit</c> pins it: after <c>WHERE rn = 1</c>,
    /// before <c>LIMIT $4 + 5</c>) — module_name is not a GROUP BY key here (<c>MAX(module_name)</c> is the
    /// aggregate), so it has to filter the deduplicated rows before the GROUP BY rather than after it, and
    /// living in the shared suffix means both the raw and the table CTE inherit that same placement.</summary>
    private const string QueryStoreTopSuffix = """
        ranked AS (
            SELECT
                database_name,
                query_id,
                plan_id,
                query_hash,
                /* A GROUP BY key, like replica_role below: Query Store keeps Regular, Aborted and Exception
                   executions of one plan in separate runtime-stats rows, and averaging them together would
                   blend a timeout's duration into the plan's normal cost. One row per outcome instead. */
                execution_type_desc,
                MAX(module_name) AS module_name,
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
            AND   ($7::text IS NULL OR module_name = $7)
            GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
            ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS double precision)) DESC
            LIMIT $4 + 5
        )
        SELECT
            r.database_name,
            r.query_id,
            r.plan_id,
            r.query_hash,
            r.query_plan_hash,
            r.execution_type_desc,
            r.module_name,
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

    /// <summary>
    /// #3953's own threshold for this read (ruling issuecomment-5836972848 item 5): below this window the
    /// table's extra round trips (the gate's own reads plus a second transaction) cost more than they save, so
    /// the gate reads raw regardless of coverage. A read's own constant — does not share
    /// <see cref="QueryStoreIntervalWide.GridWideMinWindow"/> (the grid's) — the two reads may not share a
    /// threshold. Raised from 12 to 24 hours (lane B4t, rig-d4, 15-day seed at a field store's rate,
    /// end-to-end through <see cref="GetQueryStoreTopAsync(NpgsqlDataSource,int,DateTime,DateTime,int,string,CancellationToken)"/>):
    /// median of 5 at the ruled 12-hour cell, table 737.0 ms (spread 655.1-746.5) against raw 581.1 ms (spread
    /// 544.3-629.3) — the table was slower than raw there, so the ruling moves this threshold to 24 hours.
    /// </summary>
    public static readonly TimeSpan QueryStoreTopMinWindow = TimeSpan.FromHours(24);

    /// <summary>The store schema version <see cref="TryGetQueryStoreTopFromTableAsync"/> requires (#3953 gate
    /// clause 6, ruling issuecomment-5836972848) before it will even attempt the table. Unlike the viewer —
    /// a separately-versioned desktop app that can connect to an older remote store, so it probes the live
    /// connection via <c>GetStoreSchemaVersionAsync</c> — this headless service always applies its own pending
    /// migrations up to <see cref="StorageVersion.SchemaVersion"/> before it starts serving MCP/web reads
    /// (<see cref="StorageVersion"/>: "a store at this version is fully migrated"), so the compiled constant IS
    /// the connected store's version for this surface; no extra round trip earns its keep here.</summary>
    private const int QueryStoreTopTableMinSchemaVersion = 145;

    public static Task<List<QueryStoreRow>> GetQueryStoreTopAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName,
        CancellationToken cancellationToken = default) =>
        GetQueryStoreTopAsync(postgres, serverId, startUtc, endUtc, top, databaseName, executionType: null, moduleName: null, cancellationToken);

    /// <summary>
    /// #3953: reads <c>query_store_interval_wide</c> when <see cref="QueryStoreIntervalWide.ReadsTableAsync"/>
    /// says its coverage holds the window; any fault or a "no" reads <see cref="QueryStoreTopSql"/> unchanged,
    /// exactly as before this table existed. <paramref name="endUtc"/> doubles as the gate's clause-4 literal
    /// end: every caller of this MCP surface (the tool and its <c>/api/read</c> mirror) already resolves
    /// <c>as_of</c> to a concrete instant before calling in, so there is no "open end" case to thread through
    /// the way the viewer's WPF presets have.
    /// </summary>
    public static async Task<List<QueryStoreRow>> GetQueryStoreTopAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName,
        string? executionType, string? moduleName, CancellationToken cancellationToken = default)
    {
        /* Review D4R H1: the window check first, before the gate's own round trips even open — this surface
           has no live schema probe to save (StorageVersion.SchemaVersion is a compiled constant), but every
           call under QueryStoreTopMinWindow otherwise still opens a second connection, a transaction, and
           pays ReadSourceInputsSql plus the unindexed PlainTableFloorSql scan for a read that can only ever
           land on raw (UseTable's clause 5). */
        if (StorageVersion.SchemaVersion >= QueryStoreTopTableMinSchemaVersion && endUtc - startUtc >= QueryStoreTopMinWindow)
        {
            var tableRows = await TryGetQueryStoreTopFromTableAsync(
                postgres, serverId, startUtc, endUtc, top, databaseName, executionType, moduleName, cancellationToken);
            if (tableRows is not null)
            {
                return tableRows;
            }
        }

        var rows = new List<QueryStoreRow>();
        await using var command = postgres.CreateCommand(QueryStoreTopSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddWindow(command, serverId, startUtc, endUtc);
        AddInt(command, top);
        AddNullableText(command, databaseName);
        AddNullableText(command, executionType);
        AddNullableText(command, moduleName);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadQueryStoreTopRow(reader));
        }

        return rows;
    }

    /// <summary>The transaction's own read-only statement (#3953). Named, not inline, so this store-only
    /// construction keeps the receiver shape <c>McpReadCommandTimeoutTests</c>' census recognizes: a
    /// two-argument <c>NpgsqlCommand(sqlIdentifier, connection)</c> with <c>Transaction</c> set through the
    /// object initializer rather than threaded positionally. A three-argument
    /// <c>NpgsqlCommand(sql, connection, transaction)</c> is also the shape the HypoPG experiment's
    /// monitored-TARGET command uses, so the census deliberately does not auto-accept it here.</summary>
    private const string SetTransactionReadOnlySql = "SET TRANSACTION READ ONLY";

    /// <summary>
    /// #3953's gate and table read for the MCP/web top-queries surface, on ONE connection in ONE read-only
    /// REPEATABLE READ transaction (M1, ruling issuecomment-5836972848), mirroring the viewer's
    /// <c>TryGetQueryStoreTopQueriesFromTableAsync</c> so the gate's decision and the read it authorizes see the
    /// same snapshot. Returns null (never an empty list) when the gate says raw, so the caller can tell "read
    /// raw instead" from "the table legitimately has nothing" — an empty list from the table IS a valid answer.
    /// Any fault opening the connection, starting the transaction, running the gate, or reading the table also
    /// returns null (except cancellation, which propagates): the gate already does this for its own statements,
    /// and the table read must fail the same way rather than surface to the caller as an error.
    /// </summary>
    private static async Task<List<QueryStoreRow>?> TryGetQueryStoreTopFromTableAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, int top, string? databaseName,
        string? executionType, string? moduleName, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);

            await using (var readOnly = new NpgsqlCommand(SetTransactionReadOnlySql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var (useTable, clampedStart) = await QueryStoreIntervalWide.ReadsTableAsync(
                connection, serverId, startUtc, endUtc, endUtc, QueryStoreTopMinWindow,
                McpCommandDeadlines.ReadSeconds, logger: null, cancellationToken);
            if (!useTable)
            {
                return null;
            }

            var rows = new List<QueryStoreRow>();
            await using var command = new NpgsqlCommand(QueryStoreTopTableSql, connection) { Transaction = transaction, CommandTimeout = McpCommandDeadlines.ReadSeconds };
            AddInt(command, serverId);
            AddTimestamp(command, clampedStart);
            AddTimestamp(command, endUtc);
            AddInt(command, top);
            AddNullableText(command, databaseName);
            AddNullableText(command, executionType);
            AddNullableText(command, moduleName);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(ReadQueryStoreTopRow(reader));
            }

            return rows;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            /* Review D4R M2: same silent-fallback risk as the viewer's twin. This surface has no ILogger
               reachable from a static method with no DI-injected instance (its caller, DarlingMcpDataTools,
               takes no logger either), so Trace is the only seam available here. */
            System.Diagnostics.Trace.TraceWarning($"#3953 MCP top-queries table read fell back to raw: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
    }

    /// <summary>Shared by <see cref="GetQueryStoreTopAsync(NpgsqlDataSource,int,DateTime,DateTime,int,string,string,string,CancellationToken)"/>'s
    /// raw and table paths: both <see cref="QueryStoreTopSql"/> and <see cref="QueryStoreTopTableSql"/> project
    /// the same <see cref="QueryStoreTopSuffix"/> column list, in the same order.</summary>
    private static QueryStoreRow ReadQueryStoreTopRow(System.Data.Common.DbDataReader reader) => new(
        reader.IsDBNull(0) ? "" : reader.GetString(0),
        reader.IsDBNull(1) ? 0 : reader.GetInt64(1),
        reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
        reader.IsDBNull(3) ? "" : reader.GetString(3),
        reader.IsDBNull(4) ? "" : reader.GetString(4),
        reader.IsDBNull(5) ? "" : reader.GetString(5),
        reader.IsDBNull(6) ? null : reader.GetString(6),
        reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
        reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
        reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
        reader.IsDBNull(10) ? 0 : reader.GetDouble(10),
        reader.IsDBNull(11) ? 0 : reader.GetDouble(11),
        reader.IsDBNull(12) ? 0 : reader.GetDouble(12),
        reader.IsDBNull(13) ? 0 : reader.GetDouble(13),
        reader.IsDBNull(14) ? null : reader.GetDateTime(14),
        reader.IsDBNull(15) ? "" : reader.GetString(15),
        reader.IsDBNull(16) ? null : reader.GetString(16));

    /* ─────────────────────────── discovery / health ─────────────────────────── */

    /// <summary>
    /// Every enabled server plus its newest collection instant — the list_servers read. Drives the
    /// freshness-derived status the tool assigns (the headless viewer has no live ping either — see
    /// <c>ServerSummaryItem.ClassifyFreshness</c>). <c>created_date</c> rides on the same registry row for the
    /// retention rule <see cref="ServerListRow"/> describes (#3967). $-free (no parameters, no bare now()) so a
    /// test can pin the dialect ungated.
    ///
    /// <para><b>#3976: a per-server LATERAL probe, not a correlated <c>MAX(collection_time)</c>.</b> The two
    /// read the SAME value — the newest collection per server — but a bound cannot make the old shape cheap:
    /// <c>collection_log</c> keeps <c>DarlingRetentionHorizons.CollectionLogRetentionDays</c> days, so any
    /// bound wide enough to keep the answer identical is the retention horizon itself, and every retained
    /// chunk falls inside it (measured: 172.8 ms of cold planning and 9,694 buffers over 19 daily chunks on
    /// DARLING01, several times that at the 60-day production retention). <c>ViewerDataService.ServerFreshnessSql</c>
    /// already carries this exact <c>ORDER BY collection_time DESC LIMIT 1</c> shape for the SAME table
    /// (#3895) and measures 2.5-6.4 ms there — an ordered per-chunk descent that stops at the newest chunk
    /// with a row, which a plan built to prove a MAX over the whole retained history cannot do regardless of
    /// any WHERE clause. No <c>collection_time</c> bound is added; a dark-past-retention server still reads
    /// Offline through <see cref="ServerHealthClassifier.ClassifyFreshness(DateTime?, DateTime?, DateTime, DateTime)"/>
    /// exactly as it did before this fix (<c>DarkPastRetentionReadsOfflineTests</c> pins that rule unchanged)
    /// — this is a plan-shape fix, not a semantic one, and the rows are identical.</para>
    /// </summary>
    public const string ServerListSql = """
        SELECT
            s.server_id,
            s.server_name,
            s.display_name,
            s.sql_major_version,
            latest.collection_time AS last_collection,
            s.engine_kind,
            s.postgres_major_version,
            s.created_date
        FROM servers s
        LEFT JOIN LATERAL
        (
            SELECT cl.collection_time
            FROM v_collection_log cl
            WHERE cl.server_id = s.server_id
            ORDER BY cl.collection_time DESC
            LIMIT 1
        ) AS latest ON TRUE
        WHERE s.is_enabled
        ORDER BY s.server_name
        """;

    public static async Task<List<ServerListRow>> GetServerListAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        var rows = new List<ServerListRow>();
        await using var command = postgres.CreateCommand(ServerListSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ServerListRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetDateTime(7)));
        }

        return rows;
    }

    /// <summary>
    /// Per-collector 7-day health aggregate — Lite's <c>GetCollectionHealthAsync</c>: one row per
    /// collector with run/success/error counts, the average / maximum / p95 single-run duration, last
    /// success/run/error timestamps, and the permission-denied count for the banding. SKIPPED counts as
    /// a healthy run. $1 server_id, $2 window start (naive UTC — the trailing 7 days).
    ///
    /// <para>30 columns since #3885 (16 at #2460, plus #2472's four fan-out columns, #2804's
    /// abandoned_count, #3010's last_denied_time, #3017's rows_stored/runs_with_rows, #3240's
    /// extension_missing_count, #3754's session_missing_count, #3819's current_status /
    /// last_non_skip_time / last_productive_time and #3885's trailing_zero_row_success_runs) — every
    /// addition APPENDED, never inserted, because both MCP surfaces read
    /// this result set positionally. No longer column-identical to the WPF viewer's own
    /// <c>CollectionHealthSql</c>: the two duration statistics feed the MCP tool's sweep-pressure
    /// arithmetic, which the viewer's health grid does not serve. Lite's DuckDB read carries them at
    /// the SAME ordinals, which is the parity that matters here — both MCP surfaces read positionally.</para>
    /// </summary>
    public const string CollectionHealthSql = $"""
        SELECT
            collector_name,
            COUNT(*) AS total_runs,
            -- #2926: SUCCESS excludes an abandonment that predates #2803, so the Success column beside
            -- Abandoned cannot count the same run twice. Post-#2803 rows need no exclusion - ABANDONED
            -- is not SUCCESS - and an ordinary empty run stays counted, which is what the COALESCE in
            -- the shared predicate is for: NULL under this NOT would have dropped it.
            SUM(CASE WHEN status = 'SUCCESS'
                      AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql}
                     THEN 1 ELSE 0 END) AS success_count,
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
            -- #3240: EXTENSION_MISSING is in the exemplar set because its stored sentence IS the remedy
            -- (it names the extension and the database to create it in).
            MAX(CASE WHEN error_rank = 1 AND status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) AS last_error,
            -- The newest failure OUTRIGHT, text or not — "when did this last fail" means the run, not
            -- the message.
            MAX(CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN collection_time END) AS last_error_time,
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
            -- would be true on every server alive. A NULL database_id is an Azure sibling row
            -- (#2643/#3262): sys.resource_stats carries no id, and it bills only USER databases,
            -- so those rows are inventory too — without the IS NULL arm a master-connected Azure
            -- target with fifty user databases would read as having none.
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
                         AND   (database_id > 4 OR database_id IS NULL)
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
            MAX(CASE WHEN slowest_rank = 1 THEN duration_ms END) AS slowest_run_duration_ms,
            -- #2804: runs the #2673 wall-clock budget abandoned. Appended LAST rather than placed beside
            -- yield_count, which is where it belongs by meaning: both MCP surfaces read this result set
            -- POSITIONALLY and Lite's DuckDB read mirrors these ordinals, so inserting mid-list would
            -- silently re-map every column after it in whichever surface was not edited in the same
            -- breath. An ABANDONED run was previously counted by total_runs and by nothing else, so it
            -- grew the failure-rate denominator while contributing nothing to the numerator.
            --
            -- #2926: keyed on the ROW, not on the status alone. collection_log is append-only, so a
            -- window can still hold cycles written before #2803 gave abandonment its own status:
            -- status = 'SUCCESS' beside rows_collected = 0 and the budget note. Counted by status
            -- alone this read 0 for them, and the collector banded HEALTHY while losing cycles - a
            -- filter correct against current writes and silently wrong against older ones, failing in
            -- the reassuring direction. The pattern is one LIKE because the budget is INTERPOLATED and
            -- the shipped values differ (120 s for procedure_stats/query_stats/plan_correction, 600 s
            -- for query_store), so equality against one rendered sentence matches one collector.
            SUM(CASE WHEN {EnumeratedCollectorDriver.AbandonedRunPredicateSql}
                     THEN 1 ELSE 0 END) AS abandoned_count,
            -- #3010: the newest DENIAL on its own, which is what dates the last_error slot above.
            -- last_error_time cannot stand in for it: that column is a MAX over ERROR and PERMISSIONS
            -- together, so on a collector carrying both it hands a reader an error's instant and lets
            -- them call it a denial. Compared against last_success_time this separates a collector
            -- still being refused from one whose refusals all predate a later success -- the exact
            -- distinction pg_deadlocks needed and no surface could make.
            --
            -- Appended LAST, like abandoned_count before it: both MCP surfaces read this result set
            -- POSITIONALLY and Lite's DuckDB read mirrors these ordinals, so a mid-list insert would
            -- silently re-map every column after it in whichever surface was not edited in the same
            -- breath.
            MAX(CASE WHEN status = 'PERMISSIONS' THEN collection_time END) AS last_denied_time,
            -- #3017: what the spend BOUGHT. Every other statistic on a health row describes cost --
            -- total_runs, the three durations, and the sweep-pressure roll-up built from them -- and the
            -- rows figure lived on get_collector_cost, a different tool over a different (hourly, fleet-
            -- wide) series. Correlating spend against output was a join a reader had to know to make.
            -- Measured: pg_deadlocks was the dearest collector on a managed store, 49,258,335 ms over
            -- 79,333 runs in seven days, and stored zero rows.
            --
            -- Read from THIS query's own window so cost and output cannot describe different runs. That
            -- is the same reason #3010's two instants come out of one aggregate: a rows figure taken
            -- from one window beside a duration taken from another composes into a ratio describing no
            -- collector that ever ran.
            --
            -- COALESCE so a zero is unambiguous at the STORE. Without it a collector whose every
            -- rows_collected is NULL returns NULL here, which a reader would have to guess between "this
            -- read did not measure output" and "it stored nothing" -- and the whole point of the column
            -- is that the second of those becomes a fact rather than an absence.
            COALESCE(SUM(rows_collected), 0) AS rows_stored,
            -- The DENOMINATOR's partner, and the honest half of a cost/output pair: 12 rows over 3 of
            -- 79,333 runs is a different collector from 12 rows over all of them. get_pg_blocking already
            -- reports captures_with_blocking beside captures_total for exactly this reason, off this same
            -- rows_collected > 0 test. total_runs above is the denominator; this is the numerator.
            --
            -- APPENDED, like every column since #2472: both MCP surfaces read this result set
            -- POSITIONALLY and Lite's DuckDB read mirrors these ordinals, so a mid-list insert would
            -- silently re-map every column after it in whichever surface was not edited in the same
            -- breath.
            SUM(CASE WHEN rows_collected > 0 THEN 1 ELSE 0 END) AS runs_with_rows,
            -- #3240: runs skipped because a PostgreSQL extension the collector DECLARES is not installed
            -- — the EXTENSION_MISSING status the fault mapper split out of PERMISSIONS, counted apart so
            -- the banding stops calling an uninstalled optional extension NO_PERMISSIONS. APPENDED, like
            -- every column since #2472, because this result set is read positionally.
            SUM(CASE WHEN status = 'EXTENSION_MISSING' THEN 1 ELSE 0 END) AS extension_missing_count,
            -- #3754: runs whose XE session was missing or could not be created - the SESSION_MISSING status
            -- the tolerant XE readers write for a permission-denied session read and, since #3754, the
            -- long-query reconcile writes for a session refused in every database. Until now this status
            -- reached this surface as total_runs and NOTHING else: not an error, not a success, not a
            -- denial, so a collector whose every run was SESSION_MISSING banded FAILING on the never-
            -- succeeded clock while the row said errors 0 and the output finding said it had read and
            -- found nothing. Counted apart from error_count on purpose - it is not fed to the band, whose
            -- capture-down story belongs to the self-alert - and fed with error_count to the output
            -- finding as the runs that could not read. APPENDED, like every column since #2472, because
            -- this result set is read positionally and Lite's DuckDB read mirrors the ordinals.
            SUM(CASE WHEN status = 'SESSION_MISSING' THEN 1 ELSE 0 END) AS session_missing_count,
            -- #3819: the three columns that tell a collector which STOPPED producing apart from one that
            -- never produced here. The same named skip carries opposite meanings on those two rows, and
            -- until now the surface read both as the benign resting state -- which is how an install that
            -- took pg_statement_stats from 85% productive to EXTENSION_MISSING on 23 of 50 clusters was
            -- accepted by the countersign for 24 hours. APPENDED, like every column since #2472, because
            -- this result set is read positionally and Lite's DuckDB read mirrors the ordinals.
            --
            -- current_status is what the collector is reporting NOW, for the finding's prose. Taken at
            -- recency_rank = 1 rather than as a MAX over the skip rows: MAX is lexicographic, so on a
            -- streak whose status changed it would name whichever word sorts highest instead of the one
            -- being reported.
            MAX(CASE WHEN recency_rank = 1 THEN status END) AS current_status,
            -- The instant the current skip streak began AFTER: the newest run that was NOT a named skip.
            -- The vocabulary is interpolated from CollectorRuntimePrecondition, which is where each of
            -- those statuses is declared, so this cannot ask about three of four after a fourth is split
            -- out. A NULL status counts as non-skip: it is not one of the declared skip words, and reading
            -- it as one would let an unwritten status manufacture a streak.
            MAX(CASE WHEN status IS NULL
                      OR status NOT IN ({CollectorRuntimePrecondition.NamedSkipStatusSqlList})
                     THEN collection_time END) AS last_non_skip_time,
            -- The newest run that stored anything. Off the same rows_collected > 0 test as runs_with_rows
            -- above, so productive means one thing on this row. Compared against last_non_skip_time it
            -- says the productivity sits BEFORE the streak rather than inside it, which is the ORDER that
            -- makes this a regression rather than two unrelated facts.
            MAX(CASE WHEN rows_collected > 0 THEN collection_time END) AS last_productive_time,
            -- #3885: how many runs, counting back from the NEWEST, were SUCCESS with zero rows and nothing
            -- else. The column that makes the #3819 regression arm able to see the class it could not:
            -- job_history recorded SUCCESS / 0 rows run after run on 41 of 43 servers of the largest
            -- production store for up to a fortnight -- 1,900+ consecutive such runs on one of them -- and
            -- banded HEALTHY throughout, because #3819 keys on a skip STATUS and this collector's status was
            -- the most reassuring word the vocabulary has.
            --
            -- Exact, and FREE: recency_rank already exists in the subquery below (#3819 added it for
            -- current_status), so this buys the streak's true width with no new window function and no new
            -- sort. MIN of the rank of the newest run that BREAKS the streak, minus one, is the count of
            -- runs ahead of it; NULL (no run breaks it -- every run in the window is a zero-row success)
            -- falls back to COUNT(*), which is that same count. A streak broken by an error, a denial, a
            -- skip or a productive run therefore reads 0 rather than reaching past it, because what this
            -- measures is the collector's CURRENT state and any of those is a different current state.
            --
            -- The abandonment exclusion is the one success_count above uses, for the same #2926 reason: a
            -- pre-#2803 abandoned cycle is stored as SUCCESS with zero rows and the budget note, which is
            -- data LOSS rather than a source that went quiet, and counting it here would attribute an
            -- abandonment to a regression. APPENDED, like every column since #2472, because this result set
            -- is read positionally.
            COALESCE(
                MIN(CASE WHEN NOT (status = 'SUCCESS'
                                   AND COALESCE(rows_collected, 0) = 0
                                   AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
                         THEN recency_rank END) - 1,
                COUNT(*)) AS trailing_zero_row_success_runs
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
                -- #2926: the abandonment predicate above reads it. Projected here for the same reason
                -- #2472's three columns are: this subquery ENUMERATES its columns, so an aggregate
                -- outside naming one it does not carry fails at the STORE and nowhere earlier.
                rows_collected,
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
                    ORDER BY (CASE WHEN status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING') THEN error_message END) IS NULL,
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
                ) AS slowest_rank,
                -- #3819: newest run first, so current_status above can take the status the collector is
                -- reporting NOW. status DESC only breaks an exact-timestamp tie, and breaks it identically
                -- on DuckDB and Postgres -- the same reason the ranks above tie-break on error_message.
                ROW_NUMBER() OVER
                (
                    PARTITION BY collector_name
                    ORDER BY collection_time DESC,
                             status DESC
                ) AS recency_rank
            FROM v_collection_log
            WHERE server_id = $1
            AND   collection_time >= $2
        ) runs
        GROUP BY collector_name
        ORDER BY collector_name
        """;

    /// <summary>How long a completed per-server collection-health read is served from memory before the next
    /// caller re-reads it (#3856) — 60 seconds, the SAME figure as the fleet rollup's
    /// <see cref="DarlingFleetReader.CollectionHealthMemoLifetime"/> and deliberately not an independent
    /// constant: both memos cover reads of <c>collection_log</c> over the same trailing seven days, and the
    /// floor of what either could report differently is one collector cadence (a minute). Two lifetimes would
    /// let <c>get_collection_health</c> and <c>get_fleet_overview</c>'s collector dots disagree about the same
    /// rows for up to the difference between them, which is exactly the drift #3735 memoized the rollup to
    /// avoid. Read through the fleet reader's declaration so a change to the cadence reasoning moves both.</summary>
    internal static TimeSpan CollectionHealthMemoLifetime => DarlingFleetReader.CollectionHealthMemoLifetime;

    /// <summary>One <see cref="PerServerCollectionHealthMemo"/> per <see cref="NpgsqlDataSource"/>, weakly
    /// keyed so a disposed data source takes its memo with it — the shape and the reasons are
    /// <see cref="DarlingFleetReader.CollectionHealthMemoFor"/>'s: the MCP host and the web host build their
    /// OWN data source (different roles, different pools), so the bound this buys is one scan per minute per
    /// HOST per server, and gated-live tests spin several stores in one process where a bare static would
    /// bleed one store's collection health into another's.</summary>
    private static readonly ConditionalWeakTable<NpgsqlDataSource, PerServerCollectionHealthMemo> s_collectionHealthMemos = new();

    /// <summary>The memo the per-server 7-day collection-health read for <paramref name="postgres"/> is served
    /// through (#3856). Internal so a live test can ask the memo how many statements N racing
    /// <c>get_collection_health</c> calls actually cost the store.</summary>
    internal static PerServerCollectionHealthMemo CollectionHealthMemoFor(NpgsqlDataSource postgres) =>
        s_collectionHealthMemos.GetOrCreateValue(postgres);

    /// <summary>
    /// The per-collector 7-day health rows for one server, through the single-flight memo (#3856) —
    /// <paramref name="nowUtc"/> is the freshness reference AND the instant the trailing-7-day window is cut
    /// from, and the returned age is how many whole seconds older than it the reading is (0 when this call's
    /// own statement produced it).
    ///
    /// <para><b>Why a second memo rather than a filter over the fleet rollup's.</b> #3856 preferred deriving
    /// this from the scan #3735 already memoizes, gated on the two windows being the same fixed
    /// <c>now - 7 days</c>. They ARE (this method's caller and
    /// <see cref="DarlingFleetReader.GetFleetOverviewAsync"/>'s both cut it that way), and the window is not
    /// what makes the derivation unavailable: the RESULTS are different reads of the same rows.
    /// <see cref="DarlingFleetReader.FleetCollectionHealthSql"/> is twelve plain aggregates reduced to four
    /// per-server COUNTS plus two band words — every per-collector fact this payload publishes is already
    /// summed away by the time the fleet memo holds it — while <see cref="CollectionHealthSql"/> is
    /// twenty-nine columns per collector over a subquery carrying four window functions. Widening the fleet
    /// statement to carry them is what its own comment refuses in terms: putting a fleet-wide sort of a week of
    /// <c>collection_log</c> in front of a GROUP BY that hashes today would spend the very headroom #3735
    /// bought. So the honest shape here is #3856's first: the same primitive, the same lifetime, keyed per
    /// server.</para>
    ///
    /// <para>The memo is keyed on (server, window LENGTH) — the window is part of the key rather than an
    /// argument the memo ignores, which is the guard #3856 asked for in the derivation's place, but it is the
    /// length and not the start. #3894 is why: this method's production caller cuts
    /// <c>DateTime.UtcNow.AddDays(-7)</c>, so the START moved by however many ticks had elapsed on every
    /// call, the key differed every time, and the memo never hit once in production while its dictionary grew
    /// a permanent entry per call. The doc that used to sit here asserted the opposite — "one key per server
    /// and the memo holds" — and that sentence was the bug's alibi: the LENGTH is what every caller fixes,
    /// the start is what every caller moves. The always-zero <c>collection_health_age_seconds</c> on the
    /// payload was the tell, and nobody (this author included) read it.</para>
    ///
    /// <para>Keying on the length is honest because the age stamp is the disclosure: two calls a few seconds
    /// apart asking for the same seven days are served one scan, and the payload says how many seconds old
    /// that reading is. A caller asking for a genuinely DIFFERENT window still gets its own key and its own
    /// statement rather than a reading cut from someone else's span — the fall-through the issue required.
    /// This is the shape the fleet twin already had by keeping a single slot for its one fixed window.
    /// Cited: #3735 / #3738 (the fleet twin), #3856 (this one), #3894 (the key).</para>
    /// </summary>
    internal static async Task<(List<CollectorHealth> Rows, int AgeSeconds)> GetCollectionHealthMemoizedAsync(
        NpgsqlDataSource postgres,
        int serverId,
        DateTime windowStartUtc,
        DateTime nowUtc,
        CancellationToken cancellationToken = default) =>
        await CollectionHealthMemoFor(postgres).GetAsync(
            serverId,
            windowStartUtc,
            nowUtc,
            (scanServerId, scanWindowStart, scanToken) =>
                GetCollectionHealthAsync(postgres, scanServerId, scanWindowStart, scanToken),
            cancellationToken);

    public static async Task<List<CollectorHealth>> GetCollectionHealthAsync(
        NpgsqlDataSource postgres, int serverId, DateTime windowStartUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectorHealth>();
        await using var command = postgres.CreateCommand(CollectionHealthSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
                /* Appended (#2804), for the same reason the four above were. */
                AbandonedCount = reader.IsDBNull(20) ? 0 : Convert.ToInt64(reader.GetValue(20)),
                /* Appended (#3010), for the same reason every column before it was. */
                LastDeniedTime = reader.IsDBNull(21) ? null : reader.GetDateTime(21),
                /* Appended (#3017), for the same reason every column before it was. */
                RowsStored = reader.IsDBNull(22) ? 0 : Convert.ToInt64(reader.GetValue(22)),
                RunsWithRows = reader.IsDBNull(23) ? 0 : Convert.ToInt64(reader.GetValue(23)),
                /* Appended (#3240), for the same reason every column before it was. */
                ExtensionMissingCount = reader.IsDBNull(24) ? 0 : Convert.ToInt64(reader.GetValue(24)),
                /* Appended (#3754), for the same reason every column before it was. */
                SessionMissingCount = reader.IsDBNull(25) ? 0 : Convert.ToInt64(reader.GetValue(25)),
                /* Appended (#3819), for the same reason every column before it was. */
                CurrentStatus = reader.IsDBNull(26) ? null : reader.GetString(26),
                LastNonSkipTime = reader.IsDBNull(27) ? null : reader.GetDateTime(27),
                LastProductiveTime = reader.IsDBNull(28) ? null : reader.GetDateTime(28),
                /* Appended (#3885), for the same reason every column before it was. The EXACT trailing
                   width, off the subquery's existing recency_rank -- the fleet twin estimates the same
                   quantity from two instants and the cadence because it has no ranked subquery to read
                   and will not grow one (see FleetCollectionHealthSql). */
                TrailingZeroRowSuccessRuns = reader.IsDBNull(29) ? 0 : Convert.ToInt64(reader.GetValue(29)),
            });
        }

        return rows;
    }

    /// <summary>One completed per-server collection-health read (#3856): the rows
    /// <see cref="GetCollectionHealthAsync"/> produced and the <c>nowUtc</c> the scan was started with — both
    /// the instant its window was cut from and the instant <c>collection_health_age_seconds</c> is measured
    /// from. The list is handed to every caller of the next minute and none of them may mutate it under the
    /// others, so the payload projection only ever enumerates it; the fleet twin says the same thing by giving
    /// its dictionary a read-only type, which a <c>List&lt;T&gt;</c> has no equivalent of without copying the
    /// rows this memo exists to avoid re-reading.</summary>
    internal sealed record PerServerCollectionHealthScan(List<CollectorHealth> Rows, DateTime ReadAtUtc);

    /// <summary>
    /// Single-flight plus a one-minute memo over the per-server 7-day collection-health read (#3856) —
    /// <see cref="DarlingFleetReader.CollectionHealthMemo"/>'s primitive, keyed per (server, window start).
    /// Concurrent callers asking the same key share ONE in-flight statement; a completed scan younger than
    /// <see cref="CollectionHealthMemoLifetime"/> is served from memory.
    ///
    /// <para><b>The production photo this answers.</b> 2026-09-21 21:3xZ, the largest production store:
    /// <see cref="CollectionHealthSql"/> was cancelled by the <c>mcp</c> role's 15 s server-side
    /// <c>statement_timeout</c> (SQLSTATE 57014) for the first time on record, during a store-busy minute —
    /// the hourly successor materializations' write burst and the checkpoint sync tail behind it, the same
    /// write-IO band that produced #3735's photo on this read's fleet twin — alongside an out-of-band overview
    /// timeout in the same hour. The sequential retry succeeded. Solo on a quiet store this statement runs in
    /// well under a second, and neither the plan nor the role's cap is the problem: the exposure is the band,
    /// and with one caller there is no fan-out amplifier, which is why it took a week to appear once. What is
    /// wrong is that the read was UNMEMOIZED while its twin was not — #3738 gave the rollup one scan per minute
    /// per host and left the per-server read of the same table running on every call.</para>
    ///
    /// <para><b>Keyed on (server, window start), and the window start is in the key.</b> #3856 preferred
    /// deriving this read from the fleet memo's scan if and only if the two windows were the same fixed
    /// <c>now - 7 days</c>; they are, but the fleet memo holds per-server COUNTS off a twelve-column plain
    /// aggregate and this payload needs twenty-nine columns per collector, so there is nothing to filter (the
    /// long reasoning is on <see cref="GetCollectionHealthMemoizedAsync"/>). The window still belongs in the
    /// key rather than being ignored: a reading cut from a different window is not this caller's answer, and a
    /// memo that served it anyway would answer a question nobody asked. Both production callers pass the fixed
    /// seven days, so production holds one key per server; anything else falls through to its own statement,
    /// which is the direct read staying alive.</para>
    ///
    /// <para>Everything else — the caller's <c>nowUtc</c> as the memo's clock, the shared scan running on
    /// <see cref="CancellationToken.None"/> so one caller's token releases only that caller, a failed scan
    /// never memoized and never an unobserved exception, the last good reading surviving a failed re-read —
    /// is <see cref="DarlingFleetReader.CollectionHealthMemo"/>'s behaviour for
    /// <see cref="DarlingFleetReader.CollectionHealthMemo"/>'s reasons, deliberately copied rather than
    /// re-derived. The one thing this class adds is the dictionary of per-key state, and its entries are
    /// bounded by the servers a host reads: a key whose reading has aged out is REPLACED by the next caller's
    /// scan, and the fixed window start means a server contributes one entry, not one per call.</para>
    /// </summary>
    internal sealed class PerServerCollectionHealthMemo
    {
        /// <summary>The scan's outcome as a VALUE — exactly one of the two is non-null — so the shared task
        /// completes successfully whether the statement did or not, and a scan every waiter abandoned before it
        /// failed cannot surface on the finalizer thread as an unobserved exception. The dispatch info rather
        /// than the bare exception so a waiter's rethrow keeps the statement's own stack.</summary>
        private sealed record ScanOutcome(PerServerCollectionHealthScan? Scan, ExceptionDispatchInfo? Failure);

        /// <summary>One key's state: the scan its current callers share (non-null and incomplete while a
        /// statement is running) and the newest scan that SUCCEEDED. Separate fields for the same reason the
        /// fleet memo keeps them apart — a failure must not evict a good reading that is still inside its
        /// minute, or one timed-out statement becomes a burst of retries from every caller of the next
        /// minute.</summary>
        private sealed class KeyState
        {
            public Task<ScanOutcome>? InFlight;
            public PerServerCollectionHealthScan? Latest;

            /// <summary>When this key was last ASKED for — the axis eviction runs on (#3894). Deliberately
            /// not the reading's own age: a reading that has aged past the lifetime is still the thing a
            /// failed re-read must not destroy, which is a contract
            /// <c>AFailedReRead_DoesNotEvictTheLastGoodReading</c> pins and which the first version of this
            /// sweep broke.</summary>
            public DateTime LastTouchedUtc;
        }

        private readonly object _gate = new();

        /// <summary>Per (server, window LENGTH) — see <see cref="GetCollectionHealthMemoizedAsync"/> for why
        /// the length rather than the start (#3894: a moving start made every call its own key, so the memo
        /// never hit and this dictionary never stopped growing). Guarded by <c>_gate</c>, like every field on
        /// the fleet twin — nothing here is read or written outside it, so a plain Dictionary is correct and a
        /// concurrent one would buy nothing but the illusion that the compound check-and-start below was
        /// atomic without it.</summary>
        private readonly Dictionary<(int ServerId, TimeSpan WindowLength), KeyState> _byKey = new();

        private int _scansStarted;

        /// <summary>How many statements this memo has actually started, over its whole life and across every
        /// key — the figure a live test compares against the number of <c>get_collection_health</c> calls it
        /// raced. Diagnostic; nothing reads it in production.</summary>
        internal int ScansStarted
        {
            get
            {
                lock (_gate)
                {
                    return _scansStarted;
                }
            }
        }

        /// <summary>How many keys this memo is currently holding — the figure #3894's eviction pin reads,
        /// because "the dictionary grows forever" is otherwise only observable as host memory. Diagnostic;
        /// nothing reads it in production.</summary>
        internal int TrackedKeys
        {
            get
            {
                lock (_gate)
                {
                    return _byKey.Count;
                }
            }
        }

        /// <summary>
        /// One server's per-collector health rows as of <paramref name="nowUtc"/> — from memory when this key's
        /// reading is under a minute old, from the scan already in flight for it when there is one, else from a
        /// scan this call starts through <paramref name="scan"/> — with how many whole seconds older than
        /// <paramref name="nowUtc"/> the reading is (0 when this call's own scan produced it).
        /// </summary>
        /// <param name="scan">The statement, as a function of the server, the window start and the token to run
        /// under. The memo, not the caller, decides the token (a departing caller must not tear down a
        /// statement two others are waiting on), which is why the seam takes one rather than closing over the
        /// caller's. Substitutable so a test can count executions without a store.</param>
        /// <param name="cancellationToken">Releases THIS caller's wait. It does not reach the statement.</param>
        internal async Task<(List<CollectorHealth> Rows, int AgeSeconds)> GetAsync(
            int serverId,
            DateTime windowStartUtc,
            DateTime nowUtc,
            Func<int, DateTime, CancellationToken, Task<List<CollectorHealth>>> scan,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(scan);

            /* #3894: the LENGTH, at the grain callers specify it — never a raw instant.

               A START key misses on every production call: the tool cuts its window from a fresh UtcNow, so
               the start moves by ticks each time, and that is the defect this issue was filed for. But the
               RAW length (nowUtc - windowStartUtc) fails the mirror-image way: a caller that holds its window
               start while its clock advances drifts the length by the elapsed seconds and misses too. Both are
               the same mistake — keying on an instant — applied to the two ends of the window.

               What identifies "the same question" is the span the caller ASKED for, and every caller asks in
               whole hours: the tool for a trailing seven days, the endpoints for an integer hours_back. So the
               length is rounded to the nearest hour, which absorbs any clock-read drift inside the 60 s
               lifetime (worst case well under a minute) while keeping every genuinely different window its own
               key — a one-day window and a seven-day window are 144 hours apart and cannot round together. */
            var key = (serverId, WindowLength: RoundToWholeHours(nowUtc - windowStartUtc));

            Task<ScanOutcome> shared;
            TaskCompletionSource<ScanOutcome>? lead = null;
            lock (_gate)
            {
                PruneUntouched(nowUtc);

                if (!_byKey.TryGetValue(key, out var state))
                {
                    state = new KeyState();
                    _byKey[key] = state;
                }

                state.LastTouchedUtc = nowUtc;

                if (state.Latest is { } latest && nowUtc - latest.ReadAtUtc < CollectionHealthMemoLifetime)
                {
                    return (latest.Rows, AgeSeconds(nowUtc, latest));
                }

                if (state.InFlight is not { IsCompleted: false })
                {
                    /* RunContinuationsAsynchronously: the waiters' continuations — a whole payload projection,
                       the sweep-pressure arithmetic and the JSON write each — must not run inline on whichever
                       thread completes the statement, or the leader's connection callback would carry every
                       joiner's work. */
                    lead = new TaskCompletionSource<ScanOutcome>(TaskCreationOptions.RunContinuationsAsynchronously);
                    state.InFlight = lead.Task;
                    _scansStarted++;
                }

                shared = state.InFlight;
            }

            if (lead is not null)
            {
                /* Started OUTSIDE the lock so the statement's synchronous prefix (command construction) never
                   runs under it, and deliberately not awaited here: this caller is one waiter among any
                   number, and its own cancellation below must not be the scan's. RunScanAsync completes the
                   source in both arms and never throws, so the discarded task cannot fault. */
                _ = RunScanAsync(lead, key, windowStartUtc, nowUtc, scan);
            }

            var outcome = await shared.WaitAsync(cancellationToken);
            outcome.Failure?.Throw();
            var read = outcome.Scan!;
            return (read.Rows, AgeSeconds(nowUtc, read));
        }

        /// <summary>A window length at the grain its callers ask in (#3894). Nearest rather than floor, so a
        /// length that drifted a few seconds either side of a whole hour still lands on that hour instead of
        /// splitting one question across two keys at the boundary.</summary>
        private static TimeSpan RoundToWholeHours(TimeSpan length) =>
            TimeSpan.FromHours(Math.Round(length.TotalHours, MidpointRounding.AwayFromZero));

        /// <summary>How long a key nobody has asked for is kept before it is dropped — ten times the reading
        /// lifetime, so a key in any kind of active use is never yanked out from under a caller and the
        /// dictionary still cannot accumulate history.</summary>
        private static TimeSpan KeyRetention => CollectionHealthMemoLifetime * 10;

        /// <summary>Drops keys that NOBODY HAS ASKED FOR inside <see cref="KeyRetention"/> and that have no
        /// statement in flight.
        ///
        /// <para>#3894: this dictionary had no eviction of any kind — no Remove, no cap, no sweep — which was
        /// survivable only while the key space was believed to be one entry per server. It was not: a moving
        /// window start made every call its own key, so a long-lived MCP host accumulated one KeyState, and
        /// the rows list it holds, per call forever. Length-keying alone bounds the space to (servers ×
        /// distinct windows asked); this sweep bounds it in TIME as well, so a window asked for once never
        /// occupies the host for its lifetime.</para>
        ///
        /// <para><b>On the axis, which the first version of this sweep got wrong.</b> Evicting on the
        /// READING's age looks equivalent and is not: an aged-out reading is still the last good one, and the
        /// memo's documented contract is that a failed re-read must not destroy it — a caller whose clock
        /// lands back inside the lifetime is still served it. Sweeping on staleness deleted exactly that, and
        /// <c>AFailedReRead_DoesNotEvictTheLastGoodReading</c> caught it by hanging. Eviction is therefore
        /// about whether the KEY is in use, never about whether its reading is servable.</para></summary>
        private void PruneUntouched(DateTime nowUtc)
        {
            if (_byKey.Count == 0)
            {
                return;
            }

            /* Materialized before removing: a Dictionary cannot be mutated while it is being enumerated, and
               the count here is bounded by the live key space rather than by history. */
            var untouched = new List<(int ServerId, TimeSpan WindowLength)>();
            foreach (var (key, state) in _byKey)
            {
                var scanning = state.InFlight is { IsCompleted: false };
                var idleFor = nowUtc - state.LastTouchedUtc;

                if (!scanning && idleFor >= KeyRetention)
                {
                    untouched.Add(key);
                }
            }

            foreach (var key in untouched)
            {
                _byKey.Remove(key);
            }
        }

        private async Task RunScanAsync(
            TaskCompletionSource<ScanOutcome> lead,
            (int ServerId, TimeSpan WindowLength) key,
            DateTime windowStartUtc,
            DateTime nowUtc,
            Func<int, DateTime, CancellationToken, Task<List<CollectorHealth>>> scan)
        {
            try
            {
                /* The window start comes from the caller that LED this scan, not from the key: the key
                   carries the length, and the rows are honestly the leader's span. A joiner a few seconds
                   later is served these rows with collection_health_age_seconds saying how old they are. */
                var rows = await scan(key.ServerId, windowStartUtc, CancellationToken.None);
                var read = new PerServerCollectionHealthScan(rows, nowUtc);
                lock (_gate)
                {
                    /* The key's state, not a fresh one: a scan that outlived every waiter still belongs to the
                       key it was started for, and the entry is always present because GetAsync created it
                       before starting this scan. */
                    if (_byKey.TryGetValue(key, out var state))
                    {
                        state.Latest = read;
                    }
                }

                lead.SetResult(new ScanOutcome(read, null));
            }
            catch (Exception ex)
            {
                lead.SetResult(new ScanOutcome(null, ExceptionDispatchInfo.Capture(ex)));
            }
        }

        /// <summary>Whole seconds from the scan's reference instant to <paramref name="nowUtc"/>, floored at
        /// zero — a caller whose clock reads behind the scan's is not holding a reading from the future, it is
        /// holding the freshest one there is.</summary>
        private static int AgeSeconds(DateTime nowUtc, PerServerCollectionHealthScan read) =>
            (int)Math.Max(0, Math.Floor((nowUtc - read.ReadAtUtc).TotalSeconds));
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
            service_objective,
            utc_offset_minutes,
            time_zone_id
        FROM server_properties
        WHERE server_id = $1
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    public static async Task<ServerPropertiesReadRow?> GetLatestServerPropertiesAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(LatestServerPropertiesSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
            reader.IsDBNull(14) ? null : reader.GetString(14),
            /* V16 / V134 (#3653 item 13): both nullable in the store and both read null-or-value — a 0 offset
               would claim UTC of a row that never recorded one, and an empty zone would claim a name. */
            reader.IsDBNull(15) ? null : reader.GetInt32(15),
            reader.IsDBNull(16) ? null : reader.GetString(16));
    }

    /* ─────────────────────────── parameter helpers ─────────────────────────── */

    private static void AddWindow(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
    }

    /// <summary>
    /// The SELECT and the window for the raw per-run collection log — everything both orderings share, so the
    /// twenty-six columns and the five predicates exist once. Not executable on its own: the two consts below
    /// close it with an <c>ORDER BY</c> and the cap.
    ///
    /// <para>Bounded on BOTH sides rather than by a single now-relative lower bound, matching how the
    /// viewer's Collection Log tab windows this read: a caller asking about a past incident wants the
    /// rows from THEN, and an hours-back-from-now span cannot express that.</para>
    ///
    /// <para>Reads <c>v_collection_log</c>, the same view the viewer uses, so the web dashboard and the
    /// MCP surface cannot drift from what the desktop shows. $1 server_id, $2 window start, $3 window
    /// end (naive UTC), $4 row cap, $5 collector name or NULL, $6 duration floor in ms or NULL, $7 run
    /// status or NULL.</para>
    ///
    /// <para>The status arm (#3869) compares <c>UPPER($7)</c> rather than the raw parameter, so the stored
    /// vocabulary's own casing is what matches whatever spelling the caller sent. The tool validates the
    /// value against <c>EnumeratedCollectorDriver.CollectionLogStatuses</c> before reaching here, so this
    /// predicate can never quietly match nothing on a typo — an unknown status is refused up there, not
    /// filtered to an empty page down here.</para>
    ///
    /// <para>The three filters are NULL-tolerant predicates against always-bound parameters rather than
    /// conditionally appended text, which is the shape a dozen sibling readers already use
    /// (<c>DarlingStoredPlanReader</c>, <c>DarlingObjectStatsReader</c>, <c>DarlingAgReader</c>). It keeps
    /// every parameter at a FIXED position, which is what makes a renumbering bug impossible rather than
    /// merely unlikely. The casts are load-bearing: an untyped NULL parameter has no type to compare with.</para>
    ///
    /// <para>Filtering here rather than after the cap is the whole point of #3287, and it is what keeps the
    /// truncation signal meaningful: the caller's <c>limit</c> is applied to the MATCHING rows, so
    /// <c>truncated</c> reports that more matches exist. A filter applied to an already-capped page would
    /// report truncation of the UNFILTERED window instead — a different claim wearing the same field name.</para>
    /// </summary>
    private const string CollectionLogBody = """
        SELECT
            collector_name,
            collection_time,
            duration_ms,
            sql_duration_ms,
            duckdb_duration_ms,
            rows_collected,
            status,
            error_message,
            sql_open_ms,
            sql_drain_ms,
            watermark_ms,
            drain_rows_read,
            drain_bytes_read,
            drain_last_read_ms,
            target_session_id,
            sweep_peer_max_ms,
            plan_fetch_probe_ms,
            plan_fetch_target_ms,
            plan_fetch_write_ms,
            plan_fetch_ids_attempted,
            plan_fetch_probe_ids,
            text_fetch_probe_ms,
            text_fetch_target_ms,
            text_fetch_write_ms,
            text_fetch_ids_attempted,
            text_fetch_probe_ids
        FROM v_collection_log
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($5::text IS NULL OR collector_name = $5::text)
        AND   ($6::double precision IS NULL OR duration_ms >= $6::double precision)
        AND   ($7::text IS NULL OR status = UPPER($7::text))
        """;

    /// <summary>
    /// The DEFAULT ordering: newest first, which is what a caller asking "what has this server been doing"
    /// wants and what every shipped caller gets.
    ///
    /// <para>A whole statement rather than a spliced <c>ORDER BY</c> so the leading sort key stays a bare
    /// column. TimescaleDB's ordered <c>ChunkAppend</c> can walk chunks newest-first and stop as soon as the
    /// cap is met on <c>ORDER BY collection_time DESC LIMIT $4</c>; wrap that key in the CASE expression a
    /// single-statement form would need and it cannot, so the common unfiltered read would pay for a
    /// capability only the filtered one uses.</para>
    ///
    /// <para><c>duration_ms</c> is a tiebreak, not a ranking: runs inside one sweep share a
    /// <c>collection_time</c> to the second, and breaking those ties by cost puts the interesting one first
    /// instead of leaving the order to the scan.</para>
    /// </summary>
    public const string CollectionLogSql = CollectionLogBody + """

        ORDER BY collection_time DESC, duration_ms DESC NULLS LAST
        LIMIT $4
        """;

    /// <summary>
    /// The ordering a duration floor implies: slowest first.
    ///
    /// <para>#3287's third defect. A floor under newest-first ordering still cannot surface the tail — the
    /// cap takes the most RECENT matches, and the slow runs a caller is hunting are precisely the ones that
    /// are not recent. The filter and the ordering are therefore one decision, which is why
    /// <see cref="GetCollectionLogAsync"/> derives this from whether the floor was supplied rather than
    /// taking a separate flag a caller could set the wrong way.</para>
    ///
    /// <para><c>NULLS LAST</c> is belt-and-braces: a NULL <c>duration_ms</c> cannot satisfy the floor
    /// predicate, so no unmeasured run reaches this ordering. It is written anyway because Postgres sorts
    /// NULLs FIRST under <c>DESC</c>, so a later change that decoupled the two knobs would otherwise lead
    /// the slowest-first page with the rows that have no duration at all.</para>
    /// </summary>
    public const string CollectionLogSlowestFirstSql = CollectionLogBody + """

        ORDER BY duration_ms DESC NULLS LAST, collection_time DESC
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// Runs <see cref="CollectionLogSql"/>, or <see cref="CollectionLogSlowestFirstSql"/> when
    /// <paramref name="minDurationMs"/> is supplied. See <see cref="CollectionLogBody"/> for the window and
    /// filter semantics.
    ///
    /// <para>The ordering is DERIVED from the floor here rather than passed in. Two callers on two SKUs would
    /// each have to remember the coupling, and a caller that filtered without switching the order would get a
    /// page that looks like an answer and cannot contain the tail — so the coupling is structural.</para>
    ///
    /// <para><paramref name="collectorName"/> is matched EXACTLY, not by prefix or pattern. A name the store
    /// has never seen therefore returns zero rows, which the caller cannot distinguish from a quiet window on
    /// the row list alone; the tool's empty branch is what separates those two.</para>
    ///
    /// <para><paramref name="status"/> (#3869) is matched exactly too, case-insensitively, against the closed
    /// vocabulary in <c>EnumeratedCollectorDriver.CollectionLogStatuses</c>. Unlike the collector name, an
    /// unknown value is NOT this reader's problem: the tool refuses it by name before calling, because a
    /// closed set whose members the caller cannot see makes "no rows" an unreadable answer. Appended after
    /// the floor, like every filter before it, so no existing positional caller moved.</para>
    /// </summary>
    public static async Task<List<CollectionLogEntry>> GetCollectionLogAsync(
        NpgsqlDataSource postgres,
        int serverId,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int maxRows,
        string? collectorName = null,
        double? minDurationMs = null,
        string? status = null,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<CollectionLogEntry>();
        await using var command = postgres.CreateCommand(
            minDurationMs is null ? CollectionLogSql : CollectionLogSlowestFirstSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        AddTimestamp(command, windowStartUtc);
        AddTimestamp(command, windowEndUtc);
        AddInt(command, maxRows);
        AddNullableText(command, string.IsNullOrWhiteSpace(collectorName) ? null : collectorName.Trim());
        AddNullableDouble(command, minDurationMs);
        AddNullableText(command, string.IsNullOrWhiteSpace(status) ? null : status.Trim());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(MapCollectionLogEntry(reader, 0));
        }

        return rows;
    }

    /// <summary>
    /// Maps one <see cref="CollectionLogBody"/> / <see cref="CollectionLogFleetBody"/> row to a
    /// <see cref="CollectionLogEntry"/>, starting at column <paramref name="i"/> — 0 for the per-server SELECT,
    /// whose 26 columns start at the first ordinal, and 1 for the fleet-wide SELECT (#4199), which reads
    /// <c>server_name</c> as its own leading column ahead of the same 26. Factored out of
    /// <see cref="GetCollectionLogAsync"/> so the fleet form (<see cref="GetCollectionLogFleetAsync"/>) cannot
    /// drift from the per-server column mapping by so much as one ordinal — the two SELECTs were kept
    /// column-for-column identical after the leading name for exactly this reason.
    /// </summary>
    private static CollectionLogEntry MapCollectionLogEntry(NpgsqlDataReader reader, int i) =>
        new(
            reader.GetString(i),
            reader.GetDateTime(i + 1),
            reader.IsDBNull(i + 2) ? null : Convert.ToDouble(reader.GetValue(i + 2)),
            reader.IsDBNull(i + 3) ? null : Convert.ToDouble(reader.GetValue(i + 3)),
            reader.IsDBNull(i + 4) ? null : Convert.ToDouble(reader.GetValue(i + 4)),
            reader.IsDBNull(i + 5) ? null : Convert.ToInt64(reader.GetValue(i + 5)),
            reader.IsDBNull(i + 6) ? null : reader.GetString(i + 6),
            reader.IsDBNull(i + 7) ? null : reader.GetString(i + 7),
            /* NULL on every row written before V108, and on every run whose path emits no split -
               both genuinely "not recorded", which is why these stay nullable rather than defaulting
               to zero. A zero here would claim a measured instant open. */
            reader.IsDBNull(i + 8) ? null : Convert.ToDouble(reader.GetValue(i + 8)),
            reader.IsDBNull(i + 9) ? null : Convert.ToDouble(reader.GetValue(i + 9)),
            reader.IsDBNull(i + 10) ? null : Convert.ToDouble(reader.GetValue(i + 10)),
            /* V109 (#2864). NULL means NOT RECORDED and no more: a row written before the rung, a path
               that emits no forensics (every per-database ENUMERATED collector - query_store, the
               Pg*Stats family - never sets the measured flag), or an abandon that fired before the
               counting reader was constructed. Do NOT read a NULL as 'old row'. DrainLastReadMs beside
               a 0 DrainRowsRead does mean nothing arrived; that pairing is the one safe inference. */
            reader.IsDBNull(i + 11) ? null : Convert.ToInt64(reader.GetValue(i + 11)),
            reader.IsDBNull(i + 12) ? null : Convert.ToInt64(reader.GetValue(i + 12)),
            reader.IsDBNull(i + 13) ? null : Convert.ToDouble(reader.GetValue(i + 13)),
            reader.IsDBNull(i + 14) ? null : Convert.ToInt32(reader.GetValue(i + 14)),
            reader.IsDBNull(i + 15) ? null : Convert.ToDouble(reader.GetValue(i + 15)),
            /* V110 (#2860). NULL means the run performed no deferred fetch - which is every collector
               but the plan/text-fetching ones, and ~78% of even those runs, since a fetch only runs when
               the probe finds something missing. Read raw rather than pre-divided into ms-per-id: the
               rate is the interesting number (31.65 ms/id measured on production's cold plans against
               ~1.6 on #2806's hot ones), but there are three useful rates over these five figures and
               blessing one in the record would hide the others. The consumer divides. */
            reader.IsDBNull(i + 16) ? null : Convert.ToDouble(reader.GetValue(i + 16)),
            reader.IsDBNull(i + 17) ? null : Convert.ToDouble(reader.GetValue(i + 17)),
            reader.IsDBNull(i + 18) ? null : Convert.ToDouble(reader.GetValue(i + 18)),
            reader.IsDBNull(i + 19) ? null : Convert.ToInt64(reader.GetValue(i + 19)),
            reader.IsDBNull(i + 20) ? null : Convert.ToInt64(reader.GetValue(i + 20)),
            reader.IsDBNull(i + 21) ? null : Convert.ToDouble(reader.GetValue(i + 21)),
            reader.IsDBNull(i + 22) ? null : Convert.ToDouble(reader.GetValue(i + 22)),
            reader.IsDBNull(i + 23) ? null : Convert.ToDouble(reader.GetValue(i + 23)),
            reader.IsDBNull(i + 24) ? null : Convert.ToInt64(reader.GetValue(i + 24)),
            reader.IsDBNull(i + 25) ? null : Convert.ToInt64(reader.GetValue(i + 25)));

    /// <summary>One <see cref="CollectionLogEntry"/> plus which server it came from — the fleet-wide row shape
    /// (#4199), needed only because <see cref="GetCollectionLogFleetAsync"/> merges runs across every enabled
    /// server into one ranked page and the per-server record carries no server identity of its own.</summary>
    public sealed record FleetCollectionLogEntry(string ServerName, CollectionLogEntry Entry);

    /// <summary>
    /// The FLEET-WIDE form of <see cref="CollectionLogBody"/> (#4199): the same per-run log, across EVERY
    /// ENABLED server at once, each row carrying which server it came from. $1 window start, $2 window end
    /// (naive UTC), $3 row cap, $4 collector name or NULL, $5 duration floor or NULL, $6 status or NULL.
    ///
    /// <para>Scoped to enabled servers by joining <c>servers</c> — the SAME population
    /// <see cref="DarlingServerResolver"/> resolves a name against — rather than adding a predicate of its own,
    /// so the two can never disagree about which servers are in scope. That join is also what excludes the
    /// fleet-MAINTENANCE sentinel row (<c>server_id = 0</c>), which the registry deliberately never contains
    /// (see <see cref="DarlingServerResolver.ResolveOrErrorWithFleetSentinelAsync"/>): a fleet-wide page of
    /// ordinary collector runs is not that sentinel's maintenance run-records, and no extra predicate is needed
    /// to keep the two apart.</para>
    ///
    /// <para>Column-for-column identical to <see cref="CollectionLogBody"/> after its own leading
    /// <c>server_name</c>, so <see cref="MapCollectionLogEntry"/> reads both with one mapping.</para>
    /// </summary>
    private const string CollectionLogFleetBody = """
        SELECT
            cl.server_name,
            cl.collector_name,
            cl.collection_time,
            cl.duration_ms,
            cl.sql_duration_ms,
            cl.duckdb_duration_ms,
            cl.rows_collected,
            cl.status,
            cl.error_message,
            cl.sql_open_ms,
            cl.sql_drain_ms,
            cl.watermark_ms,
            cl.drain_rows_read,
            cl.drain_bytes_read,
            cl.drain_last_read_ms,
            cl.target_session_id,
            cl.sweep_peer_max_ms,
            cl.plan_fetch_probe_ms,
            cl.plan_fetch_target_ms,
            cl.plan_fetch_write_ms,
            cl.plan_fetch_ids_attempted,
            cl.plan_fetch_probe_ids,
            cl.text_fetch_probe_ms,
            cl.text_fetch_target_ms,
            cl.text_fetch_write_ms,
            cl.text_fetch_ids_attempted,
            cl.text_fetch_probe_ids
        FROM v_collection_log cl
        JOIN servers s ON s.server_id = cl.server_id AND s.is_enabled
        WHERE cl.collection_time >= $1
        AND   cl.collection_time <= $2
        AND   ($4::text IS NULL OR cl.collector_name = $4::text)
        AND   ($5::double precision IS NULL OR cl.duration_ms >= $5::double precision)
        AND   ($6::text IS NULL OR cl.status = UPPER($6::text))
        """;

    /// <summary>Fleet-wide newest-first — see <see cref="CollectionLogSql"/>, its per-server twin.</summary>
    public const string CollectionLogFleetSql = CollectionLogFleetBody + """

        ORDER BY cl.collection_time DESC, cl.duration_ms DESC NULLS LAST
        LIMIT $3
        """;

    /// <summary>Fleet-wide slowest-first, when a duration floor is supplied — see
    /// <see cref="CollectionLogSlowestFirstSql"/>, its per-server twin.</summary>
    public const string CollectionLogFleetSlowestFirstSql = CollectionLogFleetBody + """

        ORDER BY cl.duration_ms DESC NULLS LAST, cl.collection_time DESC
        LIMIT $3
        """;

    /// <summary>Whether ANY enabled server has EVER recorded a collector run — the fleet-wide
    /// <see cref="HasAnyCollectionLogSql"/>, used the same way: to tell a genuinely quiet fleet-wide window
    /// from a fleet that has never once collected (every server newly added, or the service never started).
    /// </summary>
    public const string HasAnyCollectionLogFleetSql = """
        SELECT 1
        FROM v_collection_log cl
        JOIN servers s ON s.server_id = cl.server_id AND s.is_enabled
        LIMIT 1
        """;

    /// <summary>Runs <see cref="HasAnyCollectionLogFleetSql"/>.</summary>
    public static async Task<bool> HasAnyCollectionLogFleetAsync(
        NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(HasAnyCollectionLogFleetSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        return await command.ExecuteScalarAsync(cancellationToken) is not null;
    }

    /// <summary>
    /// Runs <see cref="CollectionLogFleetSql"/>, or <see cref="CollectionLogFleetSlowestFirstSql"/> when
    /// <paramref name="minDurationMs"/> is supplied — the fleet-wide twin of <see cref="GetCollectionLogAsync"/>,
    /// same filter and ordering rules, merged across every enabled server instead of scoped to one.
    /// </summary>
    public static async Task<List<FleetCollectionLogEntry>> GetCollectionLogFleetAsync(
        NpgsqlDataSource postgres,
        DateTime windowStartUtc,
        DateTime windowEndUtc,
        int maxRows,
        string? collectorName = null,
        double? minDurationMs = null,
        string? status = null,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<FleetCollectionLogEntry>();
        await using var command = postgres.CreateCommand(
            minDurationMs is null ? CollectionLogFleetSql : CollectionLogFleetSlowestFirstSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddTimestamp(command, windowStartUtc);
        AddTimestamp(command, windowEndUtc);
        AddInt(command, maxRows);
        AddNullableText(command, string.IsNullOrWhiteSpace(collectorName) ? null : collectorName.Trim());
        AddNullableDouble(command, minDurationMs);
        AddNullableText(command, string.IsNullOrWhiteSpace(status) ? null : status.Trim());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var serverName = reader.GetString(0);
            rows.Add(new FleetCollectionLogEntry(serverName, MapCollectionLogEntry(reader, 1)));
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
    /// $1 server_id, $2 start, $3 end (naive UTC). $4 is the <see cref="EventWindowFloor"/> for $2 — both
    /// tables are hypertables partitioned on <c>collection_time</c>, which this event-time window alone
    /// gives the planner nothing to exclude a chunk on (#4229); the floor lets it skip every chunk older
    /// than the window, without being able to drop a row (an event is collected after it happens).</para>
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
            AND   collection_time >= $4
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
            AND   collection_time >= $4
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        AddInt(command, serverId);
        AddTimestamp(command, startUtc);
        AddTimestamp(command, endUtc);
        AddTimestamp(command, EventWindowFloor.For(startUtc));
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
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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

    /// <summary>Binds a nullable numeric floor (a null value binds SQL NULL, activating the read's
    /// <c>$N::double precision IS NULL OR ...</c> "no filter" branch).</summary>
    private static void AddNullableDouble(NpgsqlCommand command, double? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Double, Value = (object?)value ?? DBNull.Value });

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

    /// <summary>
    /// The newest PERMISSIONS instant in the window (#3010) — what dates <see cref="LastError"/>.
    /// Distinct from <see cref="LastErrorTime"/>, a MAX over ERROR and PERMISSIONS together, which
    /// therefore cannot answer whether the last thing that happened here was a refusal. Null when the
    /// window holds no denial.
    /// </summary>
    public DateTime? LastDeniedTime { get; set; }
    /// <summary>1s lock-timeout yields (#1805) — deliberate, benign, counted apart from errors.</summary>
    public long YieldCount { get; set; }

    /// <summary>
    /// Runs skipped because a PostgreSQL extension the collector declares is not installed (#3240) — the
    /// <c>EXTENSION_MISSING</c> status split out of PERMISSIONS so an uninstalled optional extension bands
    /// apart from a grant problem. Deliberately NOT part of <see cref="PermissionDeniedCount"/> or
    /// <see cref="LastDeniedTime"/>: those feed <see cref="DeniedSinceLastSuccess"/>, whose sentence is
    /// about grants, and folding these in would resurrect the exact conflation the status split ends.
    /// Always 0 for SQL Server collectors.
    /// </summary>
    public long ExtensionMissingCount { get; set; }

    /// <summary>
    /// Runs recorded <c>SESSION_MISSING</c> (#3754): the XE session the collector reads was missing or could
    /// not be created, so nothing was read. Counted apart from <see cref="ErrorCount"/> because it is not
    /// an input to <see cref="HealthStatus"/> - the capture-down story belongs to the self-alert - and
    /// apart from <see cref="PermissionDeniedCount"/> because a missing session is not a grant. Fed with
    /// the error count to <see cref="OutputFinding"/> as the runs that could not read; before this the
    /// status reached this surface as <see cref="TotalRuns"/> and nothing else. Always 0 for a collector
    /// that reads no XE session.
    /// </summary>
    public long SessionMissingCount { get; set; }

    /* ── Regressed from productive (#3819) ────────────────────────────────────────────────────────
       A named skip on a collector that had been producing is a different fact from the same status on
       one that never has. These three columns are what tells them apart; the predicate and the
       sentence below are composed from the shared classifier so no surface can answer differently. */

    /// <summary>
    /// The status the collector is reporting NOW — its newest run's (<c>current_status</c>). Display
    /// text for <see cref="RegressedFinding"/> and nothing else: <see cref="HealthStatus"/> reads the
    /// window's COUNTS, never one row's word. Null on a surface that does not project it, which makes
    /// the finding null rather than a sentence with a hole in it.
    /// </summary>
    public string? CurrentStatus { get; set; }

    /// <summary>
    /// The newest run whose status was NOT one of <c>CollectorRuntimePrecondition.NamedSkipStatuses</c>
    /// (<c>last_non_skip_time</c>) — the instant the current skip streak began after. Null when every
    /// run in the window was a skip, which is the never-produced-here case the benign band already
    /// describes correctly.
    /// </summary>
    public DateTime? LastNonSkipTime { get; set; }

    /// <summary>
    /// The newest run that stored anything (<c>last_productive_time</c>) — off the same
    /// <c>rows_collected > 0</c> test as <see cref="RunsWithRows"/>, so productive means one thing on
    /// this row. Its ORDER against <see cref="LastNonSkipTime"/> is what makes a regression a
    /// regression rather than two unrelated facts.
    /// </summary>
    public DateTime? LastProductiveTime { get; set; }

    /// <summary>
    /// Whether this collector WAS producing rows and now reports a named skip every cycle (#3819) — the
    /// distinction <see cref="HealthStatus"/> could not make on its own, because the benign skip bands
    /// are gated on the window holding no success and a regressed collector's window holds its
    /// productive days. Its own member rather than an expression at the call site for the reason
    /// <see cref="DeniedSinceLastSuccess"/> is one: every surface derives it from the one shared
    /// predicate instead of each writing the comparison out.
    /// </summary>
    public bool RegressedFromProductive => CollectorHealthClassifier.RegressedFromProductive(
        LastRunTime, LastNonSkipTime, LastProductiveTime);

    /// <summary>
    /// The sentence a regressed collector carries, or null when it is not one (#3819). Composed from
    /// the shared formatter, like <see cref="OutputFinding"/> above, so no consumer re-derives it
    /// differently. <see cref="RowsStored"/> is the count it reports: on a regressed row that figure is
    /// entirely pre-regression, because a named skip stores nothing.
    /// </summary>
    public string? RegressedFinding => RegressedFromProductive
        ? CollectorHealthClassifier.FormatRegressedFromProductiveFinding(
            RowsStored, LastNonSkipTime, CurrentStatus)
        : null;

    /* ── Produced then stopped (#3885) ────────────────────────────────────────────────────────────
       The second regression class, and the one #3819 could not see: not a skip word but a SUCCESS
       storing nothing, run after run, on a collector that had been productive. job_history sat
       HEALTHY like that on 41 of 43 servers of the largest production store for up to a fortnight. */

    /// <summary>
    /// How many runs, counting back from the newest, were SUCCESS with zero rows and nothing else
    /// (<c>trailing_zero_row_success_runs</c>). EXACT on this per-server read, which resolves it off the
    /// ranked subquery #3819 already added; the fleet twin sets it from
    /// <see cref="CollectorHealthClassifier.EstimateTrailingZeroRowSuccessRuns"/> instead, because that
    /// statement has no subquery to rank and #3735's statement-timeout headroom is not spent on one.
    /// 0 on a surface that does not project it, which keeps <see cref="ProducedThenStopped"/> false
    /// rather than making a claim from an absence.
    /// </summary>
    public long TrailingZeroRowSuccessRuns { get; set; }

    /// <summary>
    /// Whether this collector WAS producing rows and is now recording SUCCESS with zero rows every cycle
    /// (#3885) — the regression class whose status word is the most reassuring one the vocabulary has,
    /// which is why nothing on any health surface could see it. Event collectors and on-load collectors
    /// are excluded inside the shared predicate, so a fortnight of zeros from a deadlock capture at rest
    /// stays HEALTHY.
    /// </summary>
    public bool ProducedThenStopped => CollectorHealthClassifier.ProducedThenStopped(
        CollectorName, TrailingZeroRowSuccessRuns, LastProductiveTime);

    /// <summary>
    /// The sentence a produced-then-stopped collector carries, or null when it is not one (#3885).
    /// Composed from the shared formatter for the same reason <see cref="RegressedFinding"/> is.
    /// </summary>
    public string? ProducedThenStoppedFinding => ProducedThenStopped
        ? CollectorHealthClassifier.FormatProducedThenStoppedFinding(
            RowsStored, LastProductiveTime, TrailingZeroRowSuccessRuns)
        : null;

    /// <summary>
    /// EITHER regression class — stopped skipping (#3819) or stopped producing (#3885). What the band
    /// floor, the <c>regressed_from_productive</c> field and the fleet's one regressed count all read, so
    /// widening the definition did not fork any of the three. The two predicates are disjoint by
    /// construction: one requires the newest run to be a named skip, the other requires the newest runs
    /// to be successes, so this is an OR over populations that cannot overlap.
    /// </summary>
    public bool AnyRegression => RegressedFromProductive || ProducedThenStopped;

    /// <summary>
    /// Whichever regression sentence applies, or null on a row that is neither (#3885). One slot rather
    /// than two on the payload, because the two classes are disjoint and a reader asking "why is this
    /// WARNING" wants the answer, not a pair of fields of which one is always null.
    /// </summary>
    public string? AnyRegressionFinding => RegressedFinding ?? ProducedThenStoppedFinding;

    /// <summary>
    /// Runs the #2673 whole-server wall-clock budget gave up on (#2804). Counted apart from errors for the
    /// same reason <see cref="YieldCount"/> is — a guard firing is not a fault — but unlike a yield it is
    /// data LOSS: the cycle stored nothing and advanced no watermark. Feeds
    /// <see cref="CollectorHealthClassifier.WarningAbandonRatePercent"/>, and reaches the surface as its own
    /// number so a WARNING can always be attributed to abandonment rather than to errors.
    /// </summary>
    public long AbandonedCount { get; set; }

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
    /// The slowest item against the MEAN item: 1.0 is a perfectly even fan-out and it rises with
    /// concentration. Eight databases at 10.1s each gives 1.0; one at 62s beside seven at 2.7s gives 6.1 —
    /// the same 80,900 ms run either way. NOT a concentration verdict (#3502): its ceiling is
    /// <see cref="FanoutItems"/>, so at width it reads well above the "one database dominates" bar with no
    /// concentration behind it — the decision belongs to <see cref="FanoutSlowestSharePercent"/>, which is
    /// this same ratio put against the whole pass instead of the mean. Kept because it still answers how
    /// EVEN the fan-out is; it just cannot say whether one database is worth chasing.
    ///
    /// <para>Null when the collector does not fan out, and also when the run's duration is zero — a
    /// ratio against nothing is not a smaller answer, it is a wrong one.</para>
    /// </summary>
    public double? FanoutDominance =>
        FanoutItems is > 0 && SlowestItemMs.HasValue && SlowestRunDurationMs is > 0
            ? (double)SlowestItemMs.Value * FanoutItems.Value / SlowestRunDurationMs.Value
            : null;

    /// <summary>
    /// The slowest item's share of its whole pass, as a percentage: slowest_ms / run_ms, the one division
    /// the width-versus-concentration decision actually turns on (#3502). Dominance is against the mean
    /// item, so what it means depends on the width: 3.58 over 72 items is a 4.97% share — width, no single
    /// database worth chasing — while the near-neighbour 4.20 over 15 items is 27.98%, one database owning
    /// over a quarter of the pass. Two scores that read as the same shape carry shares 5.6x apart wanting
    /// opposite remedies, which is why the share is published instead of left as one more division for
    /// every reader to skip.
    ///
    /// <para>Derived from <see cref="FanoutDominance"/> — share = dominance / items, the same algebra that
    /// exposed the misreading — rather than recomputed from the columns, so the two figures can never
    /// describe different runs: null exactly when dominance is null, by construction.</para>
    /// </summary>
    public double? FanoutSlowestSharePercent => FanoutDominance / FanoutItems * 100;

    public double FailureRatePercent => TotalRuns > 0 ? (double)ErrorCount / TotalRuns * 100 : 0;

    /// <summary>
    /// Whether <see cref="LastError"/> describes the collector's CURRENT state or a fault from a code
    /// path it no longer takes (#3010). Its own member rather than an expression at the call site so
    /// both SKUs' tools derive it from the one shared predicate instead of each writing the comparison
    /// out. Reported, never banded: <see cref="HealthStatus"/> does not read it.
    /// </summary>
    public bool DeniedSinceLastSuccess => CollectorHealthClassifier.DeniedSinceLastSuccess(
        PermissionDeniedCount, ErrorCount, LastSuccessTime, LastDeniedTime);

    /* ── What the spend bought (#3017) ──────────────────────────────────────────────────────────────
       Populated by the per-server health read ALONE. The fleet rollup builds its own CollectorHealth
       from FleetCollectionHealthSql to band it, and deliberately leaves these at their default 0 —
       which is why nothing but get_collection_health's own tool row is allowed to render them. A
       defaulted zero reaching a surface as "stored nothing" is the #2804 hazard exactly. */

    /// <summary>
    /// Rows the window's runs stored (<c>rows_stored</c>) — the output half of the cost/output pair, over
    /// the SAME window as <see cref="TotalRuns"/> and the durations beside it. Never
    /// <c>get_collector_cost</c>'s <c>total_rows</c>, which is Darling's own separate hourly series over
    /// that caller's window and across every server: see
    /// <see cref="CollectorHealthClassifier.OutputWindowNote"/> for what this figure is and is not.
    /// </summary>
    public long RowsStored { get; set; }

    /// <summary>
    /// How many of <see cref="TotalRuns"/> stored anything (<c>runs_with_rows</c>) — the numerator whose
    /// denominator is <see cref="TotalRuns"/>, on <c>get_pg_blocking</c>'s
    /// <c>captures_with_blocking</c>/<c>captures_total</c> pattern: a rows total with no run count behind
    /// it cannot tell a collector that is productive occasionally from one that is productive throughout.
    /// </summary>
    public long RunsWithRows { get; set; }

    /// <summary>Share of runs that stored anything. 0 with a large <see cref="TotalRuns"/> is the reading
    /// #3017 exists to surface; 0 runs gives 0 rather than a divide, matching every sibling rate here.</summary>
    public double ProductiveRunPercent => TotalRuns > 0 ? (double)RunsWithRows / TotalRuns * 100 : 0;

    /// <summary>
    /// The sentence a collector that spent and stored NOTHING gets, or null when it stored something
    /// (#3017). Its own member rather than an expression at the call site for the reason
    /// <see cref="DeniedSinceLastSuccess"/> is one: both SKUs' tools compose it from the one shared
    /// formatter instead of each writing the branch out, so the two cannot answer differently.
    ///
    /// <para>This is where <see cref="DeniedSinceLastSuccess"/> becomes the third term,
    /// <see cref="NoteCount"/> the fourth, and (#3754) the faulted-run count and the collector's category
    /// the fifth and sixth. Zero output with a current denial is a collector that could not read; zero
    /// output with faulted runs (<see cref="ErrorCount"/> plus <see cref="SessionMissingCount"/>) is one
    /// that could not read on those runs and the finding says so instead of offering the resting-state
    /// reading; zero output whose runs recorded a note is one that already said why, and the finding defers
    /// to <see cref="LastNote"/> rather than asserting the event-collector reading over it (#3160); zero
    /// output with none of those is the event collector at rest - if it IS an event collector
    /// (<see cref="CollectorHealthClassifier.IsEventCollector"/>), and a snapshot whose source came back
    /// empty if it is not. Every predicate is READ here and still not banded — <c>HealthStatus</c> does not
    /// call this, and this returns display text.</para>
    /// </summary>
    public string? OutputFinding =>
        /* #3240: an all-extension-missing window read NOTHING, so both of the formatter's zero-output
           readings would be false for it — "being refused NOW" points at a grant, and "read and found
           nothing" claims a read that never happened. The band plus the last_error sentence already
           carry the whole story, extension named. */
        string.Equals(HealthStatus, CollectorHealthClassifier.ExtensionMissing, StringComparison.Ordinal)
            ? null
            : CollectorHealthClassifier.FormatOutputFinding(
                    RowsStored, TotalRuns, DeniedSinceLastSuccess, NoteCount,
                    faultedRuns: ErrorCount + SessionMissingCount,
                    isEventCollector: CollectorHealthClassifier.IsEventCollector(CollectorName))
                is { Length: > 0 } finding
                ? finding
                : null;

    /// <summary>
    /// Share of runs the #2673 budget abandoned (#2804). Its own rate rather than part of
    /// <see cref="FailureRatePercent"/>: the two carry very different thresholds (0.5 against 20) because
    /// they mean different things, and merging them would report a 2%-abandoning collector as a
    /// 2%-erroring one — a rate no run of this collector actually produced.
    /// </summary>
    public double AbandonRatePercent => TotalRuns > 0 ? (double)AbandonedCount / TotalRuns * 100 : 0;

    public double HoursSinceLastSuccess => LastSuccessTime.HasValue
        ? (DateTime.UtcNow - LastSuccessTime.Value).TotalHours
        : 999;

    /// <summary>Hours since the newest run of ANY status — the input <see cref="CollectorHealthClassifier"/>'s
    /// STOPPED band reads. Distinct from <see cref="HoursSinceLastSuccess"/>: a collector that keeps being
    /// invoked and keeps failing has a small value here even while its success clock runs out; a collector
    /// whose gate flipped off and stopped being invoked entirely has a large value here too, which is what
    /// tells the two apart. Falls back to <see cref="HoursSinceLastSuccess"/> rather than the bare 999
    /// sentinel when the column is unset: a run can never be MORE certain than a known success, so absent
    /// better information this must not read more dormant than the success clock alone already says.</summary>
    public double HoursSinceLastRun => LastRunTime.HasValue
        ? (DateTime.UtcNow - LastRunTime.Value).TotalHours
        : HoursSinceLastSuccess;

    /// <summary>The collector's cadence, routed through <c>EffectiveRecurringIntervalMinutes</c> (#4000) so an
    /// on-load collector's catalog 0 reads as the daily recapture interval, which is what lets
    /// <see cref="CollectorHealthClassifier.Classify"/> band it on the SAME ladder as any other. A name the
    /// catalog doesn't know keeps 0 and the classifier's floor thresholds, as before #4000: resolving it to
    /// daily too would leave a collector that went dark HEALTHY for a day and a half. The banding uses the
    /// shipped default, not the resolved per-server override, so all three surfaces stay in parity. Internal
    /// since #2296: the tool's sweep-pressure roll-up amortizes each collector's average duration by this same
    /// cadence, so both readers of it share one resolution.</summary>
    internal int FrequencyMinutes =>
        CollectorScheduleDefaults.All.TryGetValue(CollectorName, out var schedule)
            ? CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(schedule.FrequencyMinutes)
            : 0;

    /// <summary>
    /// The row's band: the shared ladder's verdict, with #3819's regression FLOOR applied over it —
    /// WARNING where the ladder said HEALTHY and this collector stopped producing, the ladder's own
    /// answer everywhere else.
    ///
    /// <para>The floor is applied outside <c>Classify</c> rather than as an eleventh parameter, and
    /// deliberately: that signature takes RUN-CLASS COUNTS and nothing about output or currency, a
    /// discipline both SKUs' suites pin off the type. A regression is a fact about rows stored and the
    /// order of two instants, so feeding it in would be exactly the leak those pins refuse. The ladder
    /// stays a function of the counts; the floor is a separate, strictly-louder decision composed on
    /// top of it.</para>
    /// </summary>
    public string HealthStatus => CollectorHealthClassifier.BandWithRegression(
        CollectorHealthClassifier.Classify(
            TotalRuns, SuccessCount, ErrorCount, PermissionDeniedCount, ExtensionMissingCount, AbandonedCount,
            HoursSinceLastSuccess, HoursSinceLastRun, FrequencyMinutes),
        /* #3885: both regression classes reach the floor. A produced-then-stopped collector is the one
           that most needs it — its successes are FRESH, so the staleness ladder has nothing to say and
           would return HEALTHY forever. */
        AnyRegression);
}
