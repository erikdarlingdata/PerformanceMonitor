/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5226 live pins for the ranking choice the web's Top Queries and Top Procedures offer (CPU, duration, reads,
/// executions), on the raw tier of the shared PostgreSQL store, where a store with no rollups always answers raw.
/// The readers are called directly and through the web read dispatch, so a swap that lands in the SQL and a
/// <c>order_by</c> that never reaches it both fail here. The hourly path, the reads route and the retention notice
/// need rollups and live in <see cref="TopRankingHourlyLiveTests"/>, on a store of their own.
///
/// <para><b>Why the seed is shaped this way.</b> Each of the four groups wins exactly one ranking, so a swap that
/// does nothing (every choice answering CPU) and a swap that sorts on the wrong column both change a first row.
/// The delta-versus-cumulative seed gives one group a huge lifetime <c>total_logical_reads</c> and a small window
/// delta and another the reverse: a reads ranking that sums the cumulative column puts the wrong one first.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TopRankingLiveTests
{
    private const string ServerName = "darling-top-ranking-raw-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>The database name every planted row carries.</summary>
    internal const string Db = "TopRankDb";

    /// <summary>One planted group: the four metrics the rankings sort on (microseconds for CPU and elapsed time).</summary>
    internal readonly record struct Seed(string Name, long CpuUs, long ElapsedUs, long Reads, long Executions);

    /// <summary>
    /// The seed that separates the four rankings. By CPU the first row is QCPU, by duration QDUR, by reads QREADS and
    /// by executions QEXEC. QDUR is the case the issue is about: eight seconds of elapsed time on a tenth of a second
    /// of CPU (blocked, or waiting on I/O), which a CPU-only list never shows near the top.
    /// </summary>
    internal static readonly Seed[] Seeds =
    [
        new("QCPU", 9_000_000, 1_000_000, 100, 10),
        new("QDUR", 100_000, 8_000_000, 50, 5),
        new("QREADS", 500_000, 600_000, 900_000, 20),
        new("QEXEC", 300_000, 400_000, 200, 5_000),
    ];

    /// <summary>A procedure group's name for a seed name.</summary>
    internal static string ProcName(string name) => "usp_" + name;

    /// <summary>A query group's hash for a seed name.</summary>
    internal static string QueryHash(string name) => "0x" + name;

    // ---------------------------------------------------------------- the choices on the raw path

    [Theory]
    [InlineData(TopRanking.Cpu, "0xQCPU,0xQREADS,0xQEXEC,0xQDUR")]
    [InlineData(TopRanking.Duration, "0xQDUR,0xQCPU,0xQREADS,0xQEXEC")]
    [InlineData(TopRanking.Reads, "0xQREADS,0xQEXEC,0xQCPU,0xQDUR")]
    [InlineData(TopRanking.Executions, "0xQEXEC,0xQREADS,0xQCPU,0xQDUR")]
    public Task Queries_OnTheRawPath_EachChoiceRanksItsOwnMetric(TopRanking ranking, string expectedOrder) =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantSeedsAsync(connection, ct, now);
            var result = await DarlingDataReader.GetTopQueriesByCpuRoutedAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 10, databaseName: null, ranking: ranking, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, result.Tier);
            AssertOrder(result.Rows.Select(r => r.QueryHash), expectedOrder, "queries by " + TopRankings.WireName(ranking));
        });

    [Theory]
    [InlineData(TopRanking.Cpu, "usp_QCPU,usp_QREADS,usp_QEXEC,usp_QDUR")]
    [InlineData(TopRanking.Duration, "usp_QDUR,usp_QCPU,usp_QREADS,usp_QEXEC")]
    [InlineData(TopRanking.Reads, "usp_QREADS,usp_QEXEC,usp_QCPU,usp_QDUR")]
    [InlineData(TopRanking.Executions, "usp_QEXEC,usp_QREADS,usp_QCPU,usp_QDUR")]
    public Task Procedures_OnTheRawPath_EachChoiceRanksItsOwnMetric(TopRanking ranking, string expectedOrder) =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantSeedsAsync(connection, ct, now);
            var result = await DarlingDataReader.GetTopProceduresByCpuRoutedAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 10, databaseName: null, ranking: ranking, cancellationToken: ct);
            Assert.Equal(RetentionTier.Raw, result.Tier);
            AssertOrder(result.Rows.Select(r => r.ObjectName), expectedOrder, "procedures by " + TopRankings.WireName(ranking));
        });

    // ---------------------------------------------------------------- the issue's own case

    /// <summary>
    /// The case #5226 exists for: a query that ran long and used little CPU ranks first by duration and not by CPU.
    /// This is the test shown RED by making <c>TopRankings.Apply</c> return its input unchanged.
    /// </summary>
    [Fact]
    public Task Queries_ALongQueryWithLittleCpu_RanksFirstByDuration_AndNotByCpu() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantSeedsAsync(connection, ct, now);
            var byCpu = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 10, databaseName: null, ranking: TopRanking.Cpu, cancellationToken: ct);
            var byDuration = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 10, databaseName: null, ranking: TopRanking.Duration, cancellationToken: ct);

            Assert.Equal(QueryHash("QCPU"), byCpu[0].QueryHash);
            Assert.Equal(QueryHash("QDUR"), byCpu[^1].QueryHash);
            var first = byDuration[0];
            Assert.Equal(QueryHash("QDUR"), first.QueryHash);
            Assert.Equal(8_000_000L, first.TotalElapsedUs);
            Assert.Equal(100_000L, first.TotalCpuUs);
        });

    /// <summary>The same case on the procedures grid.</summary>
    [Fact]
    public Task Procedures_ALongProcedureWithLittleCpu_RanksFirstByDuration_AndNotByCpu() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantSeedsAsync(connection, ct, now);
            var byCpu = await DarlingDataReader.GetTopProceduresByCpuAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 10, databaseName: null, ranking: TopRanking.Cpu, cancellationToken: ct);
            var byDuration = await DarlingDataReader.GetTopProceduresByCpuAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 10, databaseName: null, ranking: TopRanking.Duration, cancellationToken: ct);

            Assert.Equal(ProcName("QCPU"), byCpu[0].ObjectName);
            Assert.Equal(ProcName("QDUR"), byCpu[^1].ObjectName);
            var first = byDuration[0];
            Assert.Equal(ProcName("QDUR"), first.ObjectName);
            Assert.Equal(8_000_000L, first.TotalElapsedUs);
            Assert.Equal(100_000L, first.TotalCpuUs);
        });

    // ---------------------------------------------------------------- delta versus cumulative

    /// <summary>
    /// Reads ranks on the window's <c>delta_logical_reads</c>, never the lifetime <c>total_logical_reads</c>. QCUM has the
    /// huge cumulative total and a small delta; QDELTA the reverse. The ask is for ONE row (the statement over-fetches
    /// five more than that, then re-ranks the page by the summed delta), so six fillers whose cumulative totals beat
    /// QDELTA's fill that page when the inner sort reads the cumulative column: with it QDELTA never reaches the outer
    /// re-rank at all. This is the test shown RED by pointing <c>TopRankings.Apply</c>'s reads metric at <c>total_logical_reads</c>.
    /// </summary>
    [Fact]
    public Task Queries_Reads_SumsTheWindowsDeltas_NotTheCumulativeLifetimeTotal() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantCumulativeSeedAsync(connection, ct, now);
            var rows = await DarlingDataReader.GetTopQueriesByCpuAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 1, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);

            var first = Assert.Single(rows);
            Assert.Equal(QueryHash("QDELTA"), first.QueryHash);
            Assert.Equal(5_000L, first.TotalLogicalReads);
        });

    /// <summary>The same pin on the procedures grid, whose single-level statement sorts and caps in one place.</summary>
    [Fact]
    public Task Procedures_Reads_SumsTheWindowsDeltas_NotTheCumulativeLifetimeTotal() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantCumulativeSeedAsync(connection, ct, now);
            var rows = await DarlingDataReader.GetTopProceduresByCpuAsync(
                postgres, ServerId, now.AddHours(-24), now.AddMinutes(5), top: 1, databaseName: null, ranking: TopRanking.Reads, cancellationToken: ct);

            var first = Assert.Single(rows);
            Assert.Equal(ProcName("QDELTA"), first.ObjectName);
            Assert.Equal(5_000L, first.TotalLogicalReads);
        });

    // ---------------------------------------------------------------- the web read dispatch

    /// <summary>
    /// The page's <c>order_by</c> reaches the ranking through the web read dispatch: the served list's first row follows
    /// the choice, and a request that names none is the CPU list it always was. The readers above cannot see a
    /// dispatch that stops passing the choice down.
    /// </summary>
    [Fact]
    public Task Endpoint_OrderBy_RanksTheServedList_AndAnAbsentValueIsCpu() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantSeedsAsync(connection, ct, now);
            foreach (var (orderBy, firstQuery, firstProcedure) in new[]
            {
                ("", "0xQCPU", "dbo.usp_QCPU"),
                ("cpu", "0xQCPU", "dbo.usp_QCPU"),
                ("duration", "0xQDUR", "dbo.usp_QDUR"),
                ("reads", "0xQREADS", "dbo.usp_QREADS"),
                ("executions", "0xQEXEC", "dbo.usp_QEXEC"),
            })
            {
                var query = "?server=" + ServerName + "&hours=24" + (orderBy.Length == 0 ? "" : "&order_by=" + orderBy);

                using var queries = JsonDocument.Parse(await WebReadAsync(postgres, "get_top_queries_by_cpu", query));
                Assert.Equal(firstQuery, queries.RootElement.GetProperty("queries")[0].GetProperty("query_hash").GetString());

                using var procedures = JsonDocument.Parse(await WebReadAsync(postgres, "get_top_procedures_by_cpu", query));
                Assert.Equal(firstProcedure, procedures.RootElement.GetProperty("procedures")[0].GetProperty("full_name").GetString());
            }
        });

    /// <summary>
    /// An <c>order_by</c> the whitelist does not hold is refused, never read as CPU: the read dispatch returns the
    /// <c>invalid</c> envelope, which <see cref="DarlingWebEndpoints.ClassifyToolResponse"/> calls a refusal and
    /// <see cref="DarlingWebEndpoints.ToHttpResult"/> answers with a 400. The four spellings, in any case, are served.
    /// The statement text never carries the request value, so the table the hostile spelling names is still there.
    /// </summary>
    [Fact]
    public Task Endpoint_AnUnknownOrderBy_IsRefused_As400_OnBothReads() =>
        WithSharedStoreAsync(async (connection, postgres, now, ct) =>
        {
            await PlantSeedsAsync(connection, ct, now);
            foreach (var read in new[] { "get_top_queries_by_cpu", "get_top_procedures_by_cpu" })
            {
                foreach (var bad in new[] { "bogus", "cpu; DROP TABLE query_stats", "total_cpu_us", "worker_time" })
                {
                    var body = await WebReadAsync(postgres, read, "?server=" + ServerName + "&hours=24&order_by=" + Uri.EscapeDataString(bad));
                    Assert.Equal(DarlingWebEndpoints.ToolResponseKind.Refusal, DarlingWebEndpoints.ClassifyToolResponse(body));
                    Assert.Equal(StatusCodes.Status400BadRequest, HttpStatusOf(read, body));
                    Assert.Contains("order_by must be cpu, duration, reads or executions", body, StringComparison.Ordinal);
                }

                foreach (var good in new[] { "cpu", "Duration", "READS", "executions" })
                {
                    var body = await WebReadAsync(postgres, read, "?server=" + ServerName + "&hours=24&order_by=" + good);
                    Assert.Equal(DarlingWebEndpoints.ToolResponseKind.JsonPassthrough, DarlingWebEndpoints.ClassifyToolResponse(body));
                    Assert.Equal(StatusCodes.Status200OK, HttpStatusOf(read, body));
                }
            }

            await using var count = new NpgsqlCommand("SELECT count(*) FROM query_stats WHERE server_id = $1", connection);
            count.Parameters.AddWithValue(ServerId);
            Assert.Equal((long)Seeds.Length, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });

    // ---------------------------------------------------------------- planting and plumbing

    /// <summary>Plants the four-group seed on both grids, two hours back, with the interval an honest collection carries.</summary>
    private static async Task PlantSeedsAsync(NpgsqlConnection connection, CancellationToken ct, DateTime now)
    {
        foreach (var seed in Seeds)
        {
            await PlantQueryAsync(connection, ct, "query_stats", ServerId, ServerName, now.AddHours(-2), seed.Name,
                seed.CpuUs, seed.ElapsedUs, seed.Reads, seed.Executions);
            await PlantProcedureAsync(connection, ct, "procedure_stats", ServerId, ServerName, now.AddHours(-2), seed.Name,
                seed.CpuUs, seed.ElapsedUs, seed.Reads, seed.Executions);
        }
    }

    /// <summary>
    /// QCUM: a delta of 10 against a lifetime total of fifty billion. QDELTA: a delta of 5,000 against a lifetime total of
    /// 1,000. Six fillers sit between them on the cumulative column (more than QDELTA's total, a window delta under
    /// 110), so a read that ranks on the lifetime total fills the over-fetched page without QDELTA.
    /// </summary>
    private static async Task PlantCumulativeSeedAsync(NpgsqlConnection connection, CancellationToken ct, DateTime now)
    {
        var at = now.AddHours(-2);
        await PlantQueryAsync(connection, ct, "query_stats", ServerId, ServerName, at, "QCUM", 10_000, 10_000, 10, 5, cumulativeReads: 50_000_000_000L);
        await PlantQueryAsync(connection, ct, "query_stats", ServerId, ServerName, at, "QDELTA", 10_000, 10_000, 5_000, 5, cumulativeReads: 1_000L);
        await PlantProcedureAsync(connection, ct, "procedure_stats", ServerId, ServerName, at, "QCUM", 10_000, 10_000, 10, 5, cumulativeReads: 50_000_000_000L);
        await PlantProcedureAsync(connection, ct, "procedure_stats", ServerId, ServerName, at, "QDELTA", 10_000, 10_000, 5_000, 5, cumulativeReads: 1_000L);
        for (var i = 1; i <= 6; i++)
        {
            await PlantQueryAsync(connection, ct, "query_stats", ServerId, ServerName, at, "QFILL" + i, 10_000, 10_000, 100 + i, 5, cumulativeReads: 1_000_000L + i);
            await PlantProcedureAsync(connection, ct, "procedure_stats", ServerId, ServerName, at, "QFILL" + i, 10_000, 10_000, 100 + i, 5, cumulativeReads: 1_000_000L + i);
        }
    }

    /// <summary>One <c>query_stats</c> row. <paramref name="table"/> is the store's spelling (shared: bare; scratch: <c>collect.</c>-qualified).</summary>
    internal static Task PlantQueryAsync(
        NpgsqlConnection connection, CancellationToken ct, string table, int serverId, string serverName, DateTime at,
        string name, long cpuUs, long elapsedUs, long reads, long executions, long? cumulativeReads = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            $@"INSERT INTO {table}
                   (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                    delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads,
                    sample_interval_seconds)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, Db,
            QueryHash(name), "0xSQLH" + name, executions, cpuUs, elapsedUs, reads, (object?)cumulativeReads ?? DBNull.Value, 3600);

    /// <summary>One <c>procedure_stats</c> row, in schema <c>dbo</c>; <paramref name="table"/> as for <see cref="PlantQueryAsync"/>.</summary>
    internal static Task PlantProcedureAsync(
        NpgsqlConnection connection, CancellationToken ct, string table, int serverId, string serverName, DateTime at,
        string name, long cpuUs, long elapsedUs, long reads, long executions, long? cumulativeReads = null) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            $@"INSERT INTO {table}
                   (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
                    delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads,
                    sample_interval_seconds)
               VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, $8, $9, $10, $11, $12, $13)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.TruncateToSeconds(at), serverId, serverName, Db,
            ProcName(name), "0xSQLH" + name, executions, cpuUs, elapsedUs, reads, (object?)cumulativeReads ?? DBNull.Value, 3600);

    /// <summary>Fails with the order actually read, so a RED run shows what the swap did to it.</summary>
    internal static void AssertOrder(IEnumerable<string> actual, string expectedOrder, string label)
    {
        var got = string.Join(",", actual);
        Assert.True(got == expectedOrder, $"{label}: expected the order {expectedOrder} but read {got}");
    }

    /// <summary>A read through the web dispatch, the way the page's request reaches it.</summary>
    internal static async Task<string> WebReadAsync(NpgsqlDataSource postgres, string read, string query)
    {
        var context = new DefaultHttpContext();
        context.Request.QueryString = new QueryString(query);
        return await DarlingWebEndpoints.BuildReadDispatch()[read](context, postgres, null!);
    }

    /// <summary>The HTTP status the web answers a read's returned string with.</summary>
    internal static int HttpStatusOf(string read, string toolResult)
    {
        var services = new ServiceCollection();
        services.AddOptions();
        services.AddLogging();
        using var provider = services.BuildServiceProvider();
        var context = new DefaultHttpContext { RequestServices = provider };
        context.Response.Body = new MemoryStream();
        DarlingWebEndpoints.ToHttpResult(toolResult, "/api/read/" + read, new CapturingTestLogger(), 1)
            .ExecuteAsync(context).GetAwaiter().GetResult();
        return context.Response.StatusCode;
    }

    /// <summary>Runs a body against the shared store with this class's server registered, and leaves nothing behind.</summary>
    private static async Task WithSharedStoreAsync(Func<NpgsqlConnection, NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live top-ranking tests.");
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
            await body(connection, postgres, DarlingMcpTestData.Naive(DateTime.UtcNow), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        await DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM query_stats WHERE server_id = {ServerId}; DELETE FROM procedure_stats WHERE server_id = {ServerId}; DELETE FROM servers WHERE server_id = {ServerId}");
}
