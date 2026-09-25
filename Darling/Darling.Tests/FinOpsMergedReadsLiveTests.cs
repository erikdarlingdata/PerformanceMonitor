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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4227/#4240 live equality pin: the merged <see cref="ViewerDataService.TopResourceConsumersSql"/> plus
/// <see cref="ViewerDataService.GetTopResourceConsumersAsync"/>'s client-side ranking must return exactly what
/// the two statements it replaced returned. <see cref="OldTopResourceConsumersByTotalSql"/> and
/// <see cref="OldTopResourceConsumersByAvgSql"/> are copied VERBATIM from origin/dev (<c>git show</c>,
/// pre-#4227) — the constants no longer exist in source, so this is the only thing that would still catch a
/// future edit that silently changes behavior.
///
/// <para><b>Seed shape.</b> One raw-tier collection covering: AlphaDb (the clear #1 by both metrics),
/// BravoDb/CharlieDb (an EXACT tie in both cpu_time_ms and avg_cpu_ms — same worker_time, same execution
/// count), DeltaDb (zero executions: present in ByTotal, NULL avg excludes it from ByAvg), and an unattributed
/// (NULL database_name) row (excluded from ByTotal, included in ByAvg — the two grids' old, different
/// inclusion rules, preserved client-side post-merge). topN is set above the seeded row count so no tie can
/// land on the LIMIT boundary, where old and new are allowed to disagree (#4240 PR body).</para>
///
/// <para><b>Ties compare as a canonical (metric DESC, name ASC) sequence, not the raw returned order.</b> The
/// old <c>ORDER BY &lt;metric&gt; DESC LIMIT $3</c> (no secondary key) never had a documented tie contract;
/// the new tie-break (SQL <c>ORDER BY database_name</c>, then a stable client-side
/// <c>OrderByDescending</c>) is a deliberate, called-out behavior change. Canonicalizing both sides the same
/// way proves same rows / same values while making no false claim about an undocumented old ordering.</para>
///
/// <para><b>Rollup-tier live execution is not covered here.</b> <c>ConsumerCteRaw</c>/<c>ConsumerCteForCagg</c>
/// and the <c>RouteOrThrow</c> substitution are byte-identical to origin/dev (diffed by hand) and already
/// live-pinned at the SQL-text level by <c>RetentionTierRouterTests</c>/<c>ViewerFinOpsSqlTests</c>; only the
/// raw-tier merge and the new client-side ranking are new risk surface, and both are exercised below.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FinOpsTopConsumersMergeLiveTests
{
    private const string ServerName = "darling-finops-consumers-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string OldTopResourceConsumersByTotalSql = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
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
    CAST(c.io_total_mb * 100.0 / t.total_io AS DECIMAL(5,2))
FROM combined c
CROSS JOIN totals t
WHERE c.database_name IS NOT NULL
ORDER BY c.cpu_time_ms DESC
LIMIT $3";

    private const string OldTopResourceConsumersByAvgSql = @"
WITH workload AS (
    SELECT
        database_name,
        SUM(delta_worker_time) / 1000.0 AS cpu_time_ms,
        SUM(delta_execution_count) AS execution_count
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   delta_worker_time IS NOT NULL
    GROUP BY database_name
    HAVING SUM(delta_execution_count) > 0
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
)
SELECT
    w.database_name,
    CAST(w.cpu_time_ms * 1.0 / w.execution_count AS DECIMAL(19,2)) AS avg_cpu_ms,
    w.execution_count,
    CAST(COALESCE(i.io_total_mb, 0) AS DECIMAL(19,2)),
    w.cpu_time_ms,
    CAST(COALESCE(i.io_total_mb, 0) * 1.0 / w.execution_count AS DECIMAL(19,4)) AS avg_io_mb
FROM workload w
LEFT JOIN io i ON i.database_name = w.database_name
ORDER BY avg_cpu_ms DESC
LIMIT $3";

    [Fact]
    public async Task MergedTopConsumers_MatchesOldByTotalAndByAvg_NullTieAndZeroExecution_RawTier()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps top-consumers merge test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-1);

            /* AlphaDb: clear #1 by both metrics. */
            await PlantQueryAsync(connection, ct, at, "AlphaDb", cpuUs: 500_000L, execCount: 100L);
            /* BravoDb / CharlieDb: an exact tie in cpu_time_ms AND avg_cpu_ms. */
            await PlantQueryAsync(connection, ct, at, "BravoDb", cpuUs: 300_000L, execCount: 50L);
            await PlantQueryAsync(connection, ct, at, "CharlieDb", cpuUs: 300_000L, execCount: 50L);
            /* DeltaDb: zero executions — present (ByTotal), NULL avg (excluded from ByAvg). */
            await PlantQueryAsync(connection, ct, at, "DeltaDb", cpuUs: 100_000L, execCount: 0L);
            /* Unattributed: NULL database_name — excluded from ByTotal, included in ByAvg. */
            await PlantQueryAsync(connection, ct, at, null, cpuUs: 200_000L, execCount: 40L);

            var cutoff = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-24);
            const int topN = 10; /* above the 5 seeded rows: no tie can land on the LIMIT boundary. */

            var oldByTotal = await RunOldAsync(connection, OldTopResourceConsumersByTotalSql, cutoff, topN, ct);
            var oldByAvg = await RunOldAsync(connection, OldTopResourceConsumersByAvgSql, cutoff, topN, ct);

            var (newByTotal, newByAvg) = await viewer.GetTopResourceConsumersAsync(ServerId, hoursBack: 24, topN: topN, ct);

            /* ByTotal: 4 rows (NULL name excluded), AlphaDb first, Bravo/Charlie tied at 300, Delta last. */
            Assert.Equal(4, oldByTotal.Count);
            AssertSameCanonicalRows(oldByTotal, Project(newByTotal, r => r.CpuTimeMs));

            /* ByAvg: 4 rows (Delta's zero-execution excluded, unattributed included). */
            Assert.Equal(4, oldByAvg.Count);
            AssertSameCanonicalRows(oldByAvg, Project(newByAvg, r => r.CpuTimeMs));

            /* The unattributed row is NOT in ByTotal but IS in ByAvg — same NULL-handling split as before. */
            Assert.DoesNotContain(newByTotal, r => r.DatabaseName.Length == 0);
            Assert.Contains(newByAvg, r => r.DatabaseName.Length == 0);

            /* DeltaDb (zero executions) is IN ByTotal, absent from ByAvg. */
            Assert.Contains(newByTotal, r => r.DatabaseName == "DeltaDb");
            Assert.DoesNotContain(newByAvg, r => r.DatabaseName == "DeltaDb");

            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static IEnumerable<(string DatabaseName, long Metric, long ExecutionCount, decimal IoTotalMb)> Project(
        IEnumerable<TopResourceConsumerRow> rows, Func<TopResourceConsumerRow, long> metric) =>
        rows.Select(r => (r.DatabaseName, metric(r), r.ExecutionCount, r.IoTotalMb));

    private static void AssertSameCanonicalRows(
        List<(string DatabaseName, long Metric, long ExecutionCount, decimal IoTotalMb)> old,
        IEnumerable<(string DatabaseName, long Metric, long ExecutionCount, decimal IoTotalMb)> @new)
    {
        var oldCanon = old.OrderByDescending(r => r.Metric).ThenBy(r => r.DatabaseName, StringComparer.Ordinal).ToList();
        var newCanon = @new.OrderByDescending(r => r.Metric).ThenBy(r => r.DatabaseName, StringComparer.Ordinal).ToList();
        Assert.Equal(oldCanon, newCanon);
    }

    private static async Task<List<(string DatabaseName, long Metric, long ExecutionCount, decimal IoTotalMb)>> RunOldAsync(
        NpgsqlConnection connection, string sql, DateTime cutoff, int topN, CancellationToken ct)
    {
        var rows = new List<(string, long, long, decimal)>();
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(cutoff);
        command.Parameters.AddWithValue(topN);

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3))));
        }
        return rows;
    }

    private static async Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string? databaseName, long cpuUs, long execCount)
    {
        var queryHash = "0xQH" + Guid.NewGuid().ToString("N")[..12];
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, delta_execution_count, delta_worker_time)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, (object?)databaseName ?? DBNull.Value,
            queryHash, execCount, cpuUs);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}

/// <summary>
/// #4227/#4240 live equality pin: <see cref="ViewerDataService.GetObjectGrowthHeatmapDataAsync"/>'s
/// bounds-computed-once form must return exactly what the old pair returned — each of
/// <see cref="OldObjectGrowthSummarySql"/> and <see cref="OldObjectGrowthSeriesSql"/> (copied VERBATIM from
/// origin/dev, each with its own <c>bounds</c> CTE) — for both a real growth window and the empty-window case
/// (collection stopped before the window started).
/// </summary>
[Collection("live-postgres")]
public sealed class FinOpsObjectGrowthBoundsMergeLiveTests
{
    private const string ServerName = "darling-finops-growth-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string GrowthDb = "GrowthDb";
    private const string StoppedDb = "StoppedDb";

    private const string OldObjectGrowthSummarySql = @"
WITH bounds AS (
    SELECT MAX(collection_time) AS latest_time, MIN(collection_time) AS earliest_time
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time >= $3
),
latest AS (
    SELECT schema_name, table_name,
        SUM(reserved_mb) AS cur_reserved_mb,
        SUM(used_mb) AS cur_used_mb,
        MAX(total_rows) AS cur_rows,
        COUNT(*) AS index_count
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT latest_time FROM bounds)
    GROUP BY schema_name, table_name
),
earliest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS e_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT earliest_time FROM bounds)
    GROUP BY schema_name, table_name
)
SELECT
    l.schema_name,
    l.table_name,
    l.cur_reserved_mb,
    l.cur_used_mb,
    l.cur_rows,
    l.index_count,
    l.cur_reserved_mb - COALESCE(e.e_reserved_mb, l.cur_reserved_mb) AS growth_mb
FROM latest l
LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
ORDER BY growth_mb DESC, l.schema_name, l.table_name
LIMIT $4";

    private const string OldObjectGrowthSeriesSql = @"
WITH bounds AS (
    SELECT MAX(collection_time) AS latest_time, MIN(collection_time) AS earliest_time
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time >= $3
),
latest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS cur_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT latest_time FROM bounds)
    GROUP BY schema_name, table_name
),
earliest AS (
    SELECT schema_name, table_name, SUM(reserved_mb) AS e_reserved_mb
    FROM v_index_object_stats
    WHERE server_id = $1 AND database_name = $2 AND collection_time = (SELECT earliest_time FROM bounds)
    GROUP BY schema_name, table_name
),
ranked AS (
    SELECT l.schema_name, l.table_name,
        l.cur_reserved_mb - COALESCE(e.e_reserved_mb, l.cur_reserved_mb) AS growth_mb
    FROM latest l
    LEFT JOIN earliest e ON e.schema_name = l.schema_name AND e.table_name = l.table_name
    ORDER BY growth_mb DESC, l.schema_name, l.table_name
    LIMIT $4
)
SELECT
    ios.schema_name,
    ios.table_name,
    date_trunc('day', ios.collection_time) AS the_day,
    SUM(ios.reserved_mb) AS reserved_mb
FROM v_index_object_stats ios
JOIN ranked r ON r.schema_name = ios.schema_name AND r.table_name = ios.table_name
WHERE ios.server_id = $1 AND ios.database_name = $2 AND ios.collection_time >= $3
GROUP BY ios.schema_name, ios.table_name, date_trunc('day', ios.collection_time)
ORDER BY ios.schema_name, ios.table_name, the_day";

    [Fact]
    public async Task MergedBounds_MatchesOldSummaryAndSeries_GrowthAndEmptyWindow()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps object-growth bounds test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var viewer = new ViewerDataService(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            var latest = now.AddHours(-1);
            var middle = now.AddDays(-15);
            var earliest = now.AddDays(-29);       /* inside the 30-day window */
            var outsideWindow = now.AddDays(-45);  /* older than the 30-day window: the empty case */

            /* GrowingTable: 100 -> 300 -> 500 (growth +400). ShrinkingTable: 800 -> 550 -> 300 (growth -500).
               No tie: growth_mb DESC is fully deterministic, so order is asserted directly, not canonicalized. */
            await PlantObjectAsync(connection, ct, earliest, GrowthDb, "GrowingTable", reservedMb: 100m, usedMb: 50m, rows: 1000L);
            await PlantObjectAsync(connection, ct, middle, GrowthDb, "GrowingTable", reservedMb: 300m, usedMb: 150m, rows: 3000L);
            await PlantObjectAsync(connection, ct, latest, GrowthDb, "GrowingTable", reservedMb: 500m, usedMb: 250m, rows: 5000L);
            await PlantObjectAsync(connection, ct, earliest, GrowthDb, "ShrinkingTable", reservedMb: 800m, usedMb: 400m, rows: 9000L);
            await PlantObjectAsync(connection, ct, middle, GrowthDb, "ShrinkingTable", reservedMb: 550m, usedMb: 275m, rows: 6000L);
            await PlantObjectAsync(connection, ct, latest, GrowthDb, "ShrinkingTable", reservedMb: 300m, usedMb: 150m, rows: 3000L);

            /* StoppedDb: one row older than the 30-day window — collection stopped before the window started. */
            await PlantObjectAsync(connection, ct, outsideWindow, StoppedDb, "OldTable", reservedMb: 42m, usedMb: 20m, rows: 100L);

            var windowStart = now.AddDays(-30);
            const int topN = 10;

            await using (var oldSummaryCmd = new NpgsqlCommand(OldObjectGrowthSummarySql, connection))
            {
                oldSummaryCmd.Parameters.AddWithValue(ServerId);
                oldSummaryCmd.Parameters.AddWithValue(GrowthDb);
                oldSummaryCmd.Parameters.AddWithValue(windowStart);
                oldSummaryCmd.Parameters.AddWithValue(topN);
                var oldObjects = await ReadObjectsAsync(oldSummaryCmd, ct);

                await using var oldSeriesCmd = new NpgsqlCommand(OldObjectGrowthSeriesSql, connection);
                oldSeriesCmd.Parameters.AddWithValue(ServerId);
                oldSeriesCmd.Parameters.AddWithValue(GrowthDb);
                oldSeriesCmd.Parameters.AddWithValue(windowStart);
                oldSeriesCmd.Parameters.AddWithValue(topN);
                var oldSamples = await ReadSamplesAsync(oldSeriesCmd, ct);

                var (newObjects, newSamples) = await viewer.GetObjectGrowthHeatmapDataAsync(ServerId, GrowthDb, daysBack: 30, topN: topN, ct);

                Assert.Equal(2, oldObjects.Count);
                Assert.Equal(oldObjects, newObjects.Select(o => (o.SchemaName, o.TableName, o.CurrentReservedMb, o.CurrentUsedMb, o.TotalRows, o.IndexCount, o.Growth30dMb)).ToList());
                Assert.Equal("GrowingTable", newObjects[0].TableName);   /* +400 first */
                Assert.Equal("ShrinkingTable", newObjects[1].TableName); /* -500 last */

                Assert.NotEmpty(oldSamples);
                Assert.Equal(oldSamples, newSamples.Select(s => (s.ObjectKey, s.Day, s.ReservedMb)).ToList());
            }

            /* Empty window: both the old pair and the new bounds-first short-circuit return nothing. */
            await using (var oldSummaryCmd = new NpgsqlCommand(OldObjectGrowthSummarySql, connection))
            {
                oldSummaryCmd.Parameters.AddWithValue(ServerId);
                oldSummaryCmd.Parameters.AddWithValue(StoppedDb);
                oldSummaryCmd.Parameters.AddWithValue(windowStart);
                oldSummaryCmd.Parameters.AddWithValue(topN);
                var oldEmptyObjects = await ReadObjectsAsync(oldSummaryCmd, ct);
                Assert.Empty(oldEmptyObjects);
            }

            var (emptyObjects, emptySamples) = await viewer.GetObjectGrowthHeatmapDataAsync(ServerId, StoppedDb, daysBack: 30, topN: topN, ct);
            Assert.Empty(emptyObjects);
            Assert.Empty(emptySamples);

            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task<List<(string SchemaName, string TableName, decimal CurReservedMb, decimal CurUsedMb, long CurRows, int IndexCount, decimal GrowthMb)>> ReadObjectsAsync(
        NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<(string, string, decimal, decimal, long, int, decimal)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0m : Convert.ToDecimal(reader.GetValue(2)),
                reader.IsDBNull(3) ? 0m : Convert.ToDecimal(reader.GetValue(3)),
                reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                reader.IsDBNull(5) ? 0 : Convert.ToInt32(reader.GetValue(5)),
                reader.IsDBNull(6) ? 0m : Convert.ToDecimal(reader.GetValue(6))));
        }
        return rows;
    }

    private static async Task<List<(string ObjectKey, DateTime Day, double ReservedMb)>> ReadSamplesAsync(
        NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<(string, DateTime, double)>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var schema = reader.IsDBNull(0) ? "" : reader.GetString(0);
            var table = reader.IsDBNull(1) ? "" : reader.GetString(1);
            var day = reader.IsDBNull(2) ? DateTime.MinValue : reader.GetDateTime(2);
            var reservedMb = reader.IsDBNull(3) ? 0d : Convert.ToDouble(reader.GetValue(3));
            rows.Add(($"{schema}.{table}", day, reservedMb));
        }
        return rows;
    }

    private static async Task PlantObjectAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string databaseName, string tableName,
        decimal reservedMb, decimal usedMb, long rows) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name,
                                              schema_name, table_name, index_id, index_name, reserved_mb, used_mb, total_rows)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, databaseName,
            "dbo", tableName, 1, "PK_" + tableName, reservedMb, usedMb, rows);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM index_object_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
