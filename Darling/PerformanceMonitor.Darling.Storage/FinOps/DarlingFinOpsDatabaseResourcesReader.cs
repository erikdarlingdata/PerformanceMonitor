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

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>One database's resource usage over the window (CPU, reads, writes, I/O and their shares).</summary>
public sealed record DatabaseResourceUsage(
    string DatabaseName, long CpuTimeMs, long LogicalReads, long PhysicalReads, long LogicalWrites, long ExecutionCount,
    decimal IoReadMb, decimal IoWriteMb, long IoStallMs, decimal PctCpuShare, decimal PctIoShare);

/// <summary>One database in a top-consumer list. By-total rows carry CPU in <see cref="CpuTimeMs"/>; by-average rows
/// carry the average CPU per execution there and the total in <see cref="TotalCpuTimeMs"/>.</summary>
public sealed record TopResourceConsumer(
    string DatabaseName, long CpuTimeMs, long ExecutionCount, decimal IoTotalMb, decimal PctCpu, decimal PctIo,
    long TotalCpuTimeMs, decimal AvgIoMb);

/// <summary>
/// The FinOps Database Resources reads (per-database usage and the top consumers), shared by the viewer and the
/// service so there is one copy of the SQL and of the retention-tier routing. The caller passes the rollup
/// availability and coverage it probed and the window start it computed; this reader resolves the tier from
/// them and reads the clock again when it does, exactly as the viewer did before the move.
/// </summary>
public static class DarlingFinOpsDatabaseResourcesReader
{
    /* ── #1661: retention-tier routing for the aggregate workload queries ───────────────────────────────
       These sum additively over database_name, which is exactly what the rollups materialize, so routing
       them is lossless. Each swap throws if it matches nothing, so editing the SQL without updating the
       matching fragment fails loudly instead of silently leaving the panel on raw. */

    /// <summary>The database-grain CTE in <see cref="DatabaseResourceUsageSql"/>, verbatim.</summary>
    private const string WorkloadCteRaw = $"""
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_logical_reads) AS logical_reads,
        SUM(delta_physical_reads) AS physical_reads,
        SUM(delta_logical_writes) AS logical_writes,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    AND   {TimescaleSupport.IntervalHonestSourceFilter}
    GROUP BY database_name
""";

    /// <summary>
    /// The same CTE over the per-database rollup. This is the ONE reader that needs
    /// <c>query_stats_db_hourly</c>: it sums the I/O columns, which the query-grain aggregate does not carry.
    /// The rollup already applies the same <c>delta_worker_time IS NOT NULL</c> filter, so it is dropped here.
    /// <paramref name="relationSql"/> is the FROM-clause item (#3653 A6): either <c>collect.&lt;relation&gt; AS f</c>
    /// unchanged, or <see cref="RollupCoverage.StitchedRelationSql"/>'s stitched form when a successor applies —
    /// either way the alias is <c>f</c>, so the rest of the CTE reads through it unqualified exactly as before.
    /// </summary>
    private static string WorkloadCteForCagg(string relationSql) => $"""
        database_name,
        SUM(worker_time_sum) / 1000.0 AS cpu_time_ms,
        SUM(logical_reads_sum) AS logical_reads,
        SUM(physical_reads_sum) AS physical_reads,
        SUM(logical_writes_sum) AS logical_writes,
        SUM(execution_count_sum) AS execution_count
    FROM {relationSql}
    WHERE server_id = $1
    AND   bucket >= $2
    GROUP BY database_name
""";

    /// <summary>The CPU/execution CTE shared by both top-consumer queries, verbatim.</summary>
    private const string ConsumerCteRaw = $"""
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    AND   {TimescaleSupport.IntervalHonestSourceFilter}
    GROUP BY database_name
""";

    /// <summary>
    /// The same CTE over the QUERY-grain rollup — these two need only CPU and executions, both of which
    /// <c>query_stats_hourly</c> already carries, so they route without the per-database aggregate.
    /// <paramref name="relationSql"/> is the FROM-clause item (#3653 A6) — see <see cref="WorkloadCteForCagg"/>.
    /// </summary>
    private static string ConsumerCteForCagg(string relationSql) => $"""
        database_name,
        SUM(worker_time_sum) / 1000.0 AS cpu_time_ms,
        SUM(execution_count_sum) AS execution_count
    FROM {relationSql}
    WHERE server_id = $1
    AND   bucket >= $2
    GROUP BY database_name
""";

    /// <summary>Database resource usage for <paramref name="tier"/>. Raw returns the constant untouched. The
    /// one-argument form reads the legacy relation for that tier, unstitched (<see cref="RollupCoverage.Unknown"/>
    /// carries <see cref="RollupAvailability.None"/>, so <see cref="RollupCoverage.StitchedRelationSql"/> names
    /// the legacy alone — the same bytes as before #3653 A6 routed this through the builder); the callers below
    /// pass the coverage/window they resolved so a successor can be stitched in instead.</summary>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier) =>
        DatabaseResourceUsageSqlFor(tier, RollupCoverage.Unknown, DateTime.MinValue);

    /// <summary>
    /// <see cref="DatabaseResourceUsageSqlFor(RetentionTier)"/> over the relation <paramref name="coverage"/>
    /// resolves for <paramref name="windowStartUtc"/> (#3653 A6): <see cref="RollupCoverage.StitchedRelationSql"/>
    /// stitches the legacy <c>query_stats_db_hourly</c>/<c>query_stats_db_daily</c> to its interval-honest
    /// successor at the successor's floor (F for hourly, F_d for daily), or names the legacy alone where there is
    /// no successor — the SAME single-relation text the one-argument overload builds, with an <c>AS f</c> alias
    /// the stitch needs (harmless: neither CTE qualifies a column, so the alias changes no result). The tier
    /// CHOICE stays on the legacy pair; only the relation within the chosen tier can stitch.
    /// </summary>
    public static string DatabaseResourceUsageSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc) =>
        tier == RetentionTier.Raw
            ? DatabaseResourceUsageSql
            : FinOpsRollupRouting.RouteOrThrow(
                DatabaseResourceUsageSql,
                WorkloadCteRaw,
                WorkloadCteForCagg(tier == RetentionTier.Hourly
                    ? coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDbHourlyView, "f", windowStartUtc, RollupCoverage.StitchTier.Hourly)
                    : coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDbDailyView, "f", windowStartUtc, RollupCoverage.StitchTier.Daily)),
                "database resource usage");

    /// <summary>Top consumers (by total AND by average — one statement, #4227) for <paramref name="tier"/>,
    /// over the legacy relation for that tier, unstitched (see <see cref="DatabaseResourceUsageSqlFor(RetentionTier)"/>
    /// for why routing this through the stitch builder with <see cref="RollupCoverage.Unknown"/> reproduces that).</summary>
    public static string TopResourceConsumersSqlFor(RetentionTier tier) =>
        TopResourceConsumersSqlFor(tier, RollupCoverage.Unknown, DateTime.MinValue);

    /// <summary>Top consumers (by total and by average) over the stitched relation (#3653 A6) — see
    /// <see cref="DatabaseResourceUsageSqlFor(RetentionTier, RollupCoverage, DateTime)"/>. The I/O side
    /// (<c>v_file_io_stats</c>) has no rollup and is never routed, on either tier — unchanged from before #4227
    /// merged the two statements.</summary>
    public static string TopResourceConsumersSqlFor(RetentionTier tier, RollupCoverage coverage, DateTime windowStartUtc) =>
        tier == RetentionTier.Raw
            ? TopResourceConsumersSql
            : FinOpsRollupRouting.RouteOrThrow(
                TopResourceConsumersSql,
                ConsumerCteRaw,
                ConsumerCteForCagg(tier == RetentionTier.Hourly
                    ? coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", windowStartUtc, RollupCoverage.StitchTier.Hourly)
                    : coverage.StitchedRelationSql(TimescaleSupport.QueryStatsDailyView, "f", windowStartUtc, RollupCoverage.StitchTier.Daily)),
                "top consumers");

    public const string DatabaseResourceUsageSql = $@"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_logical_reads) AS logical_reads,
        SUM(delta_physical_reads) AS physical_reads,
        SUM(delta_logical_writes) AS logical_writes,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    AND   {TimescaleSupport.IntervalHonestSourceFilter}
    GROUP BY database_name
),
io AS (
    SELECT
        database_name,
        SUM(delta_read_bytes) / 1048576.0 AS io_read_mb,
        SUM(delta_write_bytes) / 1048576.0 AS io_write_mb,
        SUM(delta_stall_read_ms + delta_stall_write_ms) AS io_stall_ms
    FROM v_file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_read_bytes IS NOT NULL
    GROUP BY database_name
),
combined AS (
    SELECT
        COALESCE(w.database_name, i.database_name) AS database_name,
        COALESCE(w.cpu_time_ms, 0) AS cpu_time_ms,
        COALESCE(w.logical_reads, 0) AS logical_reads,
        COALESCE(w.physical_reads, 0) AS physical_reads,
        COALESCE(w.logical_writes, 0) AS logical_writes,
        COALESCE(w.execution_count, 0) AS execution_count,
        COALESCE(i.io_read_mb, 0) AS io_read_mb,
        COALESCE(i.io_write_mb, 0) AS io_write_mb,
        COALESCE(i.io_stall_ms, 0) AS io_stall_ms
    FROM workload w
    FULL JOIN io i ON i.database_name = w.database_name
),
totals AS (
    SELECT
        NULLIF(SUM(cpu_time_ms), 0) AS total_cpu,
        NULLIF(SUM(io_read_mb + io_write_mb), 0) AS total_io
    FROM combined
)
SELECT
    c.database_name,
    c.cpu_time_ms,
    c.logical_reads,
    c.physical_reads,
    c.logical_writes,
    c.execution_count,
    CAST(c.io_read_mb AS DECIMAL(19,2)),
    CAST(c.io_write_mb AS DECIMAL(19,2)),
    c.io_stall_ms,
    CAST(c.cpu_time_ms * 100.0 / t.total_cpu AS DECIMAL(5,2)) AS pct_cpu_share,
    CAST((c.io_read_mb + c.io_write_mb) * 100.0 / t.total_io AS DECIMAL(5,2)) AS pct_io_share
FROM combined c
CROSS JOIN totals t
WHERE c.database_name IS NOT NULL
ORDER BY c.cpu_time_ms DESC";

    /// <summary>
    /// Top databases by total CPU AND by average CPU per execution for the Utilization summary — ONE pass
    /// over query_stats/file_io_stats feeding BOTH grids (#4227; the two statements this replaced each
    /// aggregated the same 24h of raw query_stats independently, ~27k buffers apiece). $1 server_id, $2 cutoff.
    /// Returns every database with a workload or an I/O row in the window, not just the topN of either
    /// ordering — <see cref="GetTopResourceConsumersAsync"/> ranks and slices both grids from this one row set,
    /// since a single statement can only LIMIT one ordering and the two grids rank by different columns.
    ///
    /// <para>Deliberately does NOT filter out a NULL database_name (an unattributed query_stats row) here.
    /// The old ByTotal statement did (<c>WHERE c.database_name IS NOT NULL</c>); the old ByAvg statement never
    /// did. Baking either rule in would apply it to both grids, so the NULL-name filter is applied client-side,
    /// per grid, in <see cref="GetTopResourceConsumersAsync"/> instead.</para>
    /// </summary>
    public const string TopResourceConsumersSql = $@"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    AND   {TimescaleSupport.IntervalHonestSourceFilter}
    GROUP BY database_name
),
io AS (
    SELECT
        database_name,
        SUM(delta_read_bytes + delta_write_bytes) / 1048576.0 AS io_total_mb
    FROM v_file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_read_bytes IS NOT NULL
    GROUP BY database_name
),
combined AS (
    SELECT
        COALESCE(w.database_name, i.database_name) AS database_name,
        COALESCE(w.cpu_time_ms, 0) AS cpu_time_ms,
        COALESCE(w.execution_count, 0) AS execution_count,
        COALESCE(i.io_total_mb, 0) AS io_total_mb
    FROM workload w
    FULL JOIN io i ON i.database_name = w.database_name
),
totals AS (
    SELECT
        NULLIF(SUM(cpu_time_ms), 0) AS total_cpu,
        NULLIF(SUM(io_total_mb), 0) AS total_io
    FROM combined
)
SELECT
    c.database_name,
    c.cpu_time_ms,
    c.execution_count,
    CAST(c.io_total_mb AS DECIMAL(19,2)),
    CAST(c.cpu_time_ms * 100.0 / t.total_cpu AS DECIMAL(5,2)),
    CAST(c.io_total_mb * 100.0 / t.total_io AS DECIMAL(5,2)),
    CASE WHEN c.execution_count > 0 THEN CAST(c.cpu_time_ms * 1.0 / c.execution_count AS DECIMAL(19,2)) END,
    CASE WHEN c.execution_count > 0 THEN CAST(c.io_total_mb * 1.0 / c.execution_count AS DECIMAL(19,4)) END
FROM combined c
CROSS JOIN totals t
ORDER BY c.database_name";

    /// <summary>Per-database resource usage for the window starting at <paramref name="cutoffUtc"/>.</summary>
    public static async Task<List<DatabaseResourceUsage>> GetDatabaseResourceUsageAsync(
        NpgsqlDataSource dataSource, int serverId, RollupAvailability rollups, RollupCoverage coverage, DateTime cutoffUtc,
        int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, cutoffUtc, rollups.DbGrainHourly, rollups.DbGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsDbHourlyView, TimescaleSupport.QueryStatsDbDailyView));

        /* #3653 (Q12): tier over the legacy pair above; the hourly relation by the supply rule. */
        await using var command = dataSource.CreateCommand(DatabaseResourceUsageSqlFor(tier, coverage, cutoffUtc));
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });

        var items = new List<DatabaseResourceUsage>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new DatabaseResourceUsage(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0L : Convert.ToInt64(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6)),
                reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),
                reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                reader.IsDBNull(9) ? 0m : Convert.ToDecimal(reader.GetValue(9)),
                reader.IsDBNull(10) ? 0m : Convert.ToDecimal(reader.GetValue(10))));
        }
        return items;
    }

    /// <summary>
    /// Both top-consumer grids from the one <see cref="TopResourceConsumersSql"/> round trip. Column 6
    /// (avg_cpu_ms) is NULL exactly when execution_count = 0, which is the old ByAvg statement's
    /// <c>HAVING SUM(delta_execution_count) > 0</c> restated as a per-row test instead of a group filter — the
    /// same rows survive either way, since HAVING on an aggregate is equivalent to filtering the already-grouped
    /// result on that same aggregate.
    /// </summary>
    public static async Task<(IReadOnlyList<TopResourceConsumer> ByTotal, IReadOnlyList<TopResourceConsumer> ByAvg)> GetTopResourceConsumersAsync(
        NpgsqlDataSource dataSource, int serverId, RollupAvailability rollups, RollupCoverage coverage, DateTime cutoffUtc,
        int commandTimeoutSeconds, int topN, CancellationToken cancellationToken)
    {
        var tier = RetentionTierRouter.Resolve(
            DateTime.UtcNow, cutoffUtc, rollups.QueryGrainHourly, rollups.QueryGrainDaily,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        /* #3653 (Q12): tier over the legacy pair above; the hourly relation by the supply rule. */
        await using var command = dataSource.CreateCommand(TopResourceConsumersSqlFor(tier, coverage, cutoffUtc));
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });

        var totalCandidates = new List<TopResourceConsumer>();
        var avgCandidates = new List<TopResourceConsumer>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var dbNameIsNull = reader.IsDBNull(0);
            var dbName = dbNameIsNull ? "" : reader.GetString(0);
            var cpuTimeMs = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
            var executionCount = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
            var ioTotalMb = reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3));

            /* The ByTotal grid has always excluded an unattributed (NULL) database_name — the old
               TopResourceConsumersByTotalSql's WHERE c.database_name IS NOT NULL. */
            if (!dbNameIsNull)
            {
                totalCandidates.Add(new TopResourceConsumer(dbName, cpuTimeMs, executionCount, ioTotalMb,
                    reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4)),
                    reader.IsDBNull(5) ? 0m : Convert.ToDecimal(reader.GetValue(5)), 0L, 0m));
            }

            /* The ByAvg grid never filtered a NULL database_name, only a zero-execution one; avg_cpu_ms is
               NULL exactly for those, so testing it here reproduces the old HAVING. */
            if (!reader.IsDBNull(6))
            {
                avgCandidates.Add(new TopResourceConsumer(dbName, Convert.ToInt64(reader.GetValue(6)), executionCount, ioTotalMb, 0m, 0m,
                    cpuTimeMs,
                    reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7))));
            }
        }

        /* Both grids were "ORDER BY <metric> DESC LIMIT $3" in SQL; ranking moves here so the one row set can
           feed both orderings. OrderByDescending is a stable sort, so a tie now breaks by the shared
           statement's ORDER BY c.database_name — deterministic, where the old per-grid statements left a
           tie's order to whichever plan Postgres picked (never a documented contract, and cpu_time_ms /
           avg_cpu_ms are sums of independent per-database deltas, so an exact tie is not expected in practice). */
        var byTotal = totalCandidates.OrderByDescending(r => r.CpuTimeMs).Take(topN).ToList();
        var byAvg = avgCandidates.OrderByDescending(r => r.CpuTimeMs).Take(topN).ToList();

        return (byTotal, byAvg);
    }
}
