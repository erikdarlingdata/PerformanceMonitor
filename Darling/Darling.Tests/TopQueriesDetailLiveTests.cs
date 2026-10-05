/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// The widened get_top_queries_by_cpu against live PostgreSQL: detail='full' adds the desktop Top Queries grid's remaining
/// fields from query_stats, omits the ones the store holds no value for, and the default call stays as lean as before.
/// A 20-row, 168-hour call stays under the shared response-size target in both forms.
/// </summary>
[Collection("live-postgres")]
public sealed class TopQueriesDetailLiveTests
{
    private const string ServerName = "darling-top-queries-detail-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task FullDetail_CarriesTheDesktopFields_OmitsNulls_AndStaysUnderTheBudgetAt168Hours()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live top-queries detail test.");
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            /* A non-zero offset on purpose: at 0 the conversion asserted below would pass whether or not it ran. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes) " +
                $"VALUES ({CollectionIdGenerator.Next()}, now() AT TIME ZONE 'UTC', {ServerId}, '{ServerName}', -240)");
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            var lastExec = new DateTime(2026, 3, 4, 5, 6, 7, DateTimeKind.Unspecified);
            var created = new DateTime(2026, 3, 1, 2, 3, 4, DateTimeKind.Unspecified);

            /* One fully populated group (two collections, so the extremes and the max-of columns aggregate) and, below it,
               twenty-two lean groups holding none of the detail columns: the null-omission case and the page size. */
            await PlantAsync(connection, ct, now.AddHours(-3), "0xFULLHASH", 500, full: true, lastExec, created, 2, 4);
            await PlantAsync(connection, ct, now.AddHours(-2), "0xFULLHASH", 400, full: true, lastExec, created, 1, 8);
            for (var i = 0; i < 22; i++)
            {
                await PlantAsync(connection, ct, now.AddHours(-1).AddMinutes(-i), "0xLEAN" + i.ToString("D2"), 10 + i, full: false, null, null, 1, 1);
            }

            var window = (now.AddDays(-7), now.AddMinutes(5));
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(postgres, ServerId, window.Item1, window.Item2, top: 20, databaseName: null, cancellationToken: ct);
            var detail = rows.First(r => r.QueryHash == "0xFULLHASH").Detail;
            Assert.NotNull(detail);
            /* Stored on the monitored server's clock (UTC-4); the read hands back naive UTC. */
            Assert.Equal(lastExec.AddHours(4), detail!.LastExecutionTime);
            Assert.Equal(created.AddHours(4), detail.CreationTime);
            Assert.Equal(7L, detail.MinGrantKb);
            Assert.Equal(900L, detail.MaxGrantKb);
            Assert.Equal(1L, detail.MinReservedThreads);
            Assert.Equal(8L, detail.MaxUsedThreads);
            Assert.Equal(3L, detail.PlanGenerationNum);
            Assert.Equal(1000L, detail.TotalClrTimeUs);
            Assert.NotNull(detail.WorkerTimePerSecond);
            Assert.Null(rows.First(r => r.QueryHash == "0xLEAN21").Detail!.MaxGrantKb);

            var summary = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, hours_back: 168, top: 20);
            var full = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, hours_back: 168, top: 20, detail: "full");
            Console.WriteLine($"get_top_queries_by_cpu 168h top 20 (short query text): summary={Encoding.UTF8.GetByteCount(summary)} bytes, full={Encoding.UTF8.GetByteCount(full)} bytes");
            Assert.True(Encoding.UTF8.GetByteCount(summary) < McpResponseBudget.DefaultBytes);

            using (var s = JsonDocument.Parse(summary))
            {
                var q = s.RootElement.GetProperty("queries").EnumerateArray().First(x => x.GetProperty("query_hash").GetString() == "0xFULLHASH");
                Assert.False(q.TryGetProperty("max_grant_kb", out _));
                Assert.False(q.TryGetProperty("last_execution_time", out _));
            }

            using var f = JsonDocument.Parse(full);
            var queries = f.RootElement.GetProperty("queries").EnumerateArray().ToList();
            var fq = queries.First(x => x.GetProperty("query_hash").GetString() == "0xFULLHASH");
            Assert.Equal("2026-03-04T09:06:07.0000000", fq.GetProperty("last_execution_time").GetString());
            Assert.Equal("2026-03-01T06:03:04.0000000", fq.GetProperty("creation_time").GetString());
            Assert.Equal(900, fq.GetProperty("max_grant_kb").GetInt64());
            Assert.Equal(1.0, fq.GetProperty("total_clr_ms").GetDouble(), 3);
            Assert.Equal(3, fq.GetProperty("plan_generation_num").GetInt64());
            Assert.Equal(5_000_000_000L, fq.GetProperty("max_physical_reads").GetInt64());
            var lean = queries.First(x => x.GetProperty("query_hash").GetString() == "0xLEAN21");
            Assert.False(lean.TryGetProperty("max_grant_kb", out _));
            Assert.False(lean.TryGetProperty("last_execution_time", out _));
            Assert.True(Encoding.UTF8.GetByteCount(full) < McpResponseBudget.DefaultBytes);

            /* A fully populated row (every detail field present) against the same row without them: the per-row cost of
               detail='full' on a real SQL Server target, where the grant and thread columns are populated. */
            using var sd = JsonDocument.Parse(summary);
            var sRow = sd.RootElement.GetProperty("queries").EnumerateArray().First(x => x.GetProperty("query_hash").GetString() == "0xFULLHASH");
            var perRow = Encoding.UTF8.GetByteCount(fq.GetRawText()) - Encoding.UTF8.GetByteCount(sRow.GetRawText());
            Console.WriteLine($"fully populated row: detail='full' adds {perRow} bytes per row; 20 rows add {perRow * 20} bytes");
            Assert.InRange(perRow, 250, 900);

            /* group_by=host_object runs the second statement: the same detail fields, same conversion. */
            var rolled = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, hours_back: 168, top: 20, group_by: "host_object", detail: "full");
            using (var r = JsonDocument.Parse(rolled))
            {
                var rq = r.RootElement.GetProperty("queries").EnumerateArray().First(x => x.GetProperty("query_hash").GetString() == "0xFULLHASH");
                Assert.Equal("2026-03-04T09:06:07.0000000", rq.GetProperty("last_execution_time").GetString());
                Assert.Equal(900, rq.GetProperty("max_grant_kb").GetInt64());
            }

            var refused = await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, hours_back: 168, top: 20, detail: "everything");
            Assert.Contains("detail must be", refused, StringComparison.Ordinal);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task PlantAsync(
        NpgsqlConnection connection, CancellationToken ct, DateTime at, string queryHash, long weight, bool full,
        DateTime? lastExec, DateTime? created, int minThreads, int maxThreads)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                                       query_hash, query_plan_hash, sql_handle, plan_handle, query_text,
                                       delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                                       min_dop, max_dop, sample_interval_seconds,
                                       last_execution_time, creation_time, min_grant_kb, max_grant_kb,
                                       min_reserved_threads, max_reserved_threads, min_used_threads, max_used_threads,
                                       total_clr_time, plan_generation_num, max_physical_reads)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "StackOverflow",
            queryHash, "0xPLAN", "0xSQLH" + queryHash, "0xPLANH", "SELECT 1",
            weight, weight * 1000L, weight * 2000L, weight * 10L, 1, 1, 60,
            (object?)lastExec ?? DBNull.Value, (object?)created ?? DBNull.Value,
            full ? 7L : DBNull.Value, full ? (weight == 500 ? 900L : 800L) : DBNull.Value,
            full ? (long)minThreads : DBNull.Value, full ? (long)maxThreads : DBNull.Value,
            full ? (long)minThreads : DBNull.Value, full ? (long)maxThreads : DBNull.Value,
            full ? (weight == 500 ? 1000L : 500L) : DBNull.Value, full ? 3L : DBNull.Value,
            full ? 5_000_000_000L : DBNull.Value);
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
