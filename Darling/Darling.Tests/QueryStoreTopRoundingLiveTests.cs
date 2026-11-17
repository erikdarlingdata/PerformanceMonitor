/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Http;
using PerformanceMonitor.Darling.Service;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the shared one,
   so it is not serialized against the live-postgres collection. */

/// <summary>
/// #8902: <c>get_query_store_top</c> writes its averages by the reply rounding rule (two places), not as the 15 to 17 digits of the
/// weighted double (1595.2409447799828). The read is driven against a store whose Query Store has rows, so the check covers the data
/// path and not only a precondition answer.
/// </summary>
public sealed class QueryStoreTopRoundingLiveTests
{
    private const string ServerName = "query-store-top-rounding-8902";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public async Task GetQueryStoreTop_WritesTheAveragesToTwoPlaces()
    {
        await WithSeededStoreAsync(async (source, ct) =>
        {
            var json = await DarlingMcpDataTools.GetQueryStoreTop(source, ServerName);
            Assert.Empty(McpReplyFloatNoiseLiveTests.NoisyNumbers(json));
            var row = Assert.Single(JsonNode.Parse(json)!["queries"]!.AsArray())!;
            Assert.Equal(7, (long)row["execution_count"]!);
            /* (3 * 1000 + 4 * 1) / 7 microseconds is 429.142857 us, so 0.43 ms: rounded to two places, not 0.4291428571428571. */
            Assert.Equal(0.43, (double)row["avg_duration_ms"]!, 5);
            Assert.Equal(0.39, (double)row["avg_cpu_ms"]!, 5);
            Assert.Equal(43.43, (double)row["avg_logical_reads"]!, 5);
            Assert.Equal(0.57, (double)row["avg_logical_writes"]!, 5);
            Assert.Equal(4.86, (double)row["avg_physical_reads"]!, 5);
            Assert.Equal(1.57, (double)row["avg_rowcount"]!, 5);
        });
    }

    /// <summary>
    /// The rounding is for the MCP reply only. The web compare page re-weights and divides these averages, so a plan averaging under
    /// 5 microseconds would arrive as 0 and its delta would show a dash. The same rows through the web route keep full precision.
    /// </summary>
    [Fact]
    public async Task GetQueryStoreTop_WebRoute_KeepsTheAveragesAtFullPrecision()
    {
        await WithSeededStoreAsync(async (source, ct) =>
        {
            var json = await DarlingWebEndpoints.BuildReadDispatch()["get_query_store_top"](new DefaultHttpContext(), source, null!);
            var row = Assert.Single(JsonNode.Parse(json)!["queries"]!.AsArray())!;
            Assert.Equal(7, (long)row["execution_count"]!);
            AssertUnrounded(3004 / 7.0 / 1000, (double)row["avg_duration_ms"]!);
            AssertUnrounded(2704 / 7.0 / 1000, (double)row["avg_cpu_ms"]!);
            AssertUnrounded(304 / 7.0, (double)row["avg_logical_reads"]!);
            AssertUnrounded(4 / 7.0, (double)row["avg_logical_writes"]!);
            AssertUnrounded(34 / 7.0, (double)row["avg_physical_reads"]!);
            AssertUnrounded(11 / 7.0, (double)row["avg_rowcount"]!);
        });
    }

    private static void AssertUnrounded(double expected, double actual)
    {
        Assert.NotEqual(Math.Round(expected, 2), expected);
        Assert.Equal(expected, actual, 9);
    }

    private static async Task WithSeededStoreAsync(Func<NpgsqlDataSource, System.Threading.CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the Query Store rounding tests (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(csb.ConnectionString);
        await using var connection = new NpgsqlConnection(csb.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* Two intervals of one plan, 3 and 4 executions: the execution-weighted averages divide by 7 and do not end at two places. */
        var now = DateTime.UtcNow;
        var intervals = new[]
        {
            (Id: 1L, At: now.AddMinutes(-90), Executions: 3L, DurationUs: 1000L, CpuUs: 900L, Reads: 100L, Writes: 0L, Physical: 10L, Rows: 1L),
            (Id: 2L, At: now.AddMinutes(-30), Executions: 4L, DurationUs: 1L, CpuUs: 1L, Reads: 1L, Writes: 1L, Physical: 1L, Rows: 2L),
        };
        foreach (var i in intervals)
        {
            var at = DateTime.SpecifyKind(i.At, DateTimeKind.Unspecified);
            await using var command = new NpgsqlCommand(
                """
                INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
                    query_hash, query_plan_hash, query_text, execution_count, avg_duration_us, avg_cpu_time_us, avg_logical_io_reads, avg_logical_io_writes,
                    avg_physical_io_reads, avg_rowcount, min_dop, max_dop, last_execution_time, runtime_stats_interval_id)
                VALUES ($1, $2, $3, $4, 'Sales', 42, 7, 'Regular', '0xAB', '0xCD', 'SELECT 1', $5, $6, $7, $8, $9, $10, $11, 1, 4, $2, $12)
                """, connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(i.Executions);
            command.Parameters.AddWithValue(i.DurationUs);
            command.Parameters.AddWithValue(i.CpuUs);
            command.Parameters.AddWithValue(i.Reads);
            command.Parameters.AddWithValue(i.Writes);
            command.Parameters.AddWithValue(i.Physical);
            command.Parameters.AddWithValue(i.Rows);
            command.Parameters.AddWithValue(i.Id);
            await command.ExecuteNonQueryAsync(ct);
        }

        await body(source, ct);
    }
}
