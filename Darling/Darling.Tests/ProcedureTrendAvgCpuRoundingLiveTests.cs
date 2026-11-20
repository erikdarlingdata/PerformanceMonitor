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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Tests;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. */

/// <summary>
/// #9103: <c>get_procedure_duration_trend</c> with a <c>procedure_name</c> writes each point's <c>avg_cpu_ms</c> to two places, as
/// <c>get_top_procedures_by_cpu</c> and <c>get_query_trend</c> write the same average, and not as the 15 to 17 digits of the
/// weighted double (0.4291428571428571). The form without a name and <c>get_query_duration_trend</c> carry no per-execution average;
/// their figures stay inside the reply's six-decimal rule.
/// </summary>
public sealed class ProcedureTrendAvgCpuRoundingLiveTests
{
    private const string ServerName = "procedure-trend-avg-cpu-rounding-9103";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string Database = "Sales";

    [Fact]
    public async Task OneProcedure_WritesAvgCpuMsToTwoPlaces()
    {
        await WithSeededStoreAsync(async (source, ct) =>
        {
            var json = await DarlingMcpTrendTools.GetProcedureDurationTrend(
                source, ServerName, 24, null, null, DatabaseFilter.One(Database), TrendBudget.Mcp(TrendBuckets.DurationMaxPoints), "dbo.usp_Noisy", ct);

            Assert.Empty(McpReplyFloatNoiseLiveTests.NoisyNumbers(json));
            var point = JsonNode.Parse(json)!["trend"]!.AsArray().Last()!;
            /* 3004 us of worker time over 7 executions is 0.429142857 ms: 0.43, not 0.4291428571428571. */
            Assert.Equal(0.43, (double)point["avg_cpu_ms"]!, 9);
        });
    }

    [Fact]
    public async Task TheFormWithoutAName_AndTheQueryTrend_CarryNoFloatNoise()
    {
        await WithSeededStoreAsync(async (source, ct) =>
        {
            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var procedures = await DarlingMcpTrendTools.GetProcedureDurationTrend(source, ServerName, 24, null, null, DatabaseFilter.One(Database), budget, null, ct);
            Assert.Empty(McpReplyFloatNoiseLiveTests.NoisyNumbers(procedures));
            Assert.False(JsonNode.Parse(procedures)!["trend"]!.AsArray().Last()!.AsObject().ContainsKey("avg_cpu_ms"));

            var queries = await DarlingMcpTrendTools.GetQueryDurationTrend(source, ServerName, 24, null, null, DatabaseFilter.One(Database), budget, ct);
            Assert.Empty(McpReplyFloatNoiseLiveTests.NoisyNumbers(queries));
            Assert.NotEmpty(JsonNode.Parse(queries)!["trend"]!.AsArray());
        });
    }

    private static async Task WithSeededStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the procedure trend rounding tests (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(csb.ConnectionString);
        await using var connection = new NpgsqlConnection(csb.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* One procedure, one collection: 7 executions over 3004 us of worker time, so the average does not end at two places. */
        var at = DateTime.UtcNow.AddHours(-2);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO procedure_stats
                  (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                   delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, 'dbo', 'usp_Noisy', '0xNoisy', 7, 3004, 3004, 100, 3600)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), ServerId, ServerName, Database);
        /* A query_stats row for the same hour, so the query trend has a point to scan. */
        await DurationTrendDatabaseFilterLiveTests.PlantQueryAsync(connection, ct, ServerId, ServerName, at, Database, "SELECT 1", 3004);

        await body(source, ct);
    }
}
