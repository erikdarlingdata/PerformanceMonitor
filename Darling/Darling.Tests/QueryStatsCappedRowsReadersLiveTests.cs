/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7419: what the plan cache readers say over rows stored the way the collector now stores them. A run keeps a statement only when it is in the top 200
/// of that run, so one statement here is stored in every run (the heavy one), one in three of six runs (the intermittent one), and one more in every run.
/// The readers sum what is stored: that is a partial sum, never a false zero and never a false rate, and the reads that present a total or a share of the
/// server carry <see cref="QueryStatsCoverage.StoredRowsNote"/> so the figure is not read as the whole server. Each test is on a scratch database of its own.
/// </summary>
[Collection("live-postgres")]
public sealed class QueryStatsCappedRowsReadersLiveTests
{
    private const string ServerName = "darling-capped-rows-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string DbA = "SalesDb";
    private const string DbB = "ReportDb";
    private const int Runs = 6;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime End => LiveClock.AnchoredEnd();

    private static string AsOf => End.ToString("o", CultureInfo.InvariantCulture);

    private static async Task RunAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live capped-rows reader tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var seed = await postgres.OpenConnectionAsync(ct))
        {
            await DarlingMcpTestData.ExecAsync(seed, ct,
                "INSERT INTO servers (server_id, server_name, display_name, is_enabled, created_date, modified_date) VALUES ($1, $2, $2, TRUE, $3, $3)",
                ServerId, ServerName, DarlingMcpTestData.Naive(DateTime.UtcNow));
        }

        await body(postgres, ct);
    }

    private static async Task RowAsync(
        NpgsqlDataSource postgres, CancellationToken ct, string database, string hash, DateTime at, long cpuUs, long executions, string text)
    {
        await using var seed = await postgres.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(seed, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                                               query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,$6,$6,$6,$6,$7,$8,$9,$10,60)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, database, hash, text, executions, cpuUs, cpuUs * 2);
    }

    /// <summary>
    /// Six one-minute runs. HEAVY (SalesDb) 500 ms and 10 executions in every run. MID (SalesDb) 100 ms and 5 executions in runs 0, 2 and 4 only: the other
    /// three runs left it out, the way the cap does. OTHER (ReportDb) 40 ms and 2 executions in every run. What is stored: SalesDb 3,300 ms and 75 executions,
    /// ReportDb 240 ms and 12. If every run had stored MID, SalesDb would hold 3,600 ms and 90 executions.
    /// </summary>
    private static async Task SeedAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var start = End.AddHours(-2);
        for (var run = 0; run < Runs; run++)
        {
            var at = start.AddMinutes(run);
            await RowAsync(postgres, ct, DbA, "0xHEAVY", at, 500_000, 10, "SELECT * FROM dbo.Orders WHERE Id = @id");
            if (run % 2 == 0)
            {
                await RowAsync(postgres, ct, DbA, "0xMID", at, 100_000, 5, "SELECT 2");
            }

            await RowAsync(postgres, ct, DbB, "0xOTHER", at, 40_000, 2, "SELECT 3");
        }
    }

    [Fact]
    public Task CpuByDatabase_SumsTheStoredRows_KeepsTheHeavyStatementExact_AndSaysItIsNotTheWholeServer() => RunAsync(async (postgres, ct) =>
    {
        await SeedAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpWhoDroveItTools.GetCpuByDatabaseCore(postgres, ServerName, 4, 20, AsOf, ct));
        var root = doc.RootElement;
        var dbs = root.GetProperty("databases").EnumerateArray().ToArray();

        Assert.Equal(new[] { DbA, DbB }, dbs.Select(d => d.GetProperty("database_name").GetString()));
        Assert.Equal(3300.0, dbs[0].GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(75, dbs[0].GetProperty("executions").GetInt64());
        Assert.Equal(2, dbs[0].GetProperty("statements").GetInt64());
        Assert.Equal(240.0, dbs[1].GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(3540.0, root.GetProperty("server_cpu_ms").GetDouble(), 3);
        Assert.Equal(Math.Round(3300.0 / 3540.0, 4), dbs[0].GetProperty("share").GetDouble(), 4);

        /* The statement kept in every run is exact: 6 x 500 ms. */
        var top = dbs[0].GetProperty("top_statement");
        Assert.Equal("0xHEAVY", top.GetProperty("query_hash").GetString());
        Assert.Equal(3000.0, top.GetProperty("cpu_ms").GetDouble(), 3);

        /* The page names what its totals cover. */
        Assert.Equal(QueryStatsCoverage.StoredRowsNote, root.GetProperty("coverage_note").GetString());
    });

    [Fact]
    public Task TopQueries_RanksTheHeavyStatementFirst_AndShowsTheIntermittentOneAsAPartialSum() => RunAsync(async (postgres, ct) =>
    {
        await SeedAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpDataTools.GetTopQueriesByCpu(postgres, ServerName, 3, 20, as_of: AsOf, cancellationToken: ct));
        var queries = doc.RootElement.GetProperty("queries").EnumerateArray().ToArray();

        Assert.Equal(new[] { "0xHEAVY", "0xMID", "0xOTHER" }, queries.Select(q => q.GetProperty("query_hash").GetString()));
        Assert.Equal(3000.0, queries[0].GetProperty("total_cpu_ms").GetDouble(), 3);
        /* MID is stored in three of six runs: 300 ms, not 0 and not a rate over the six minutes. */
        Assert.Equal(300.0, queries[1].GetProperty("total_cpu_ms").GetDouble(), 3);
        Assert.Equal(240.0, queries[2].GetProperty("total_cpu_ms").GetDouble(), 3);
    });

    [Fact]
    public Task ResourceUseByTag_PlanCacheTotalIsTheStoredRows_AndTheNoteSaysSo() => RunAsync(async (postgres, ct) =>
    {
        await SeedAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpQueryTagRollupTools.GetResourceUseByTagCore(postgres, ServerName, 4, null, 25, AsOf, null, ct));
        var root = doc.RootElement;
        var planCache = root.GetProperty("sections").EnumerateArray().Single(s => s.GetProperty("section").GetString() == QueryTagRollupSection.PlanCache.Name);

        Assert.Equal(3540.0, planCache.GetProperty("total").GetProperty("cpu_ms").GetDouble(), 3);
        Assert.Equal(87, planCache.GetProperty("total").GetProperty("executions").GetDouble(), 3);
        Assert.Contains(QueryStatsCoverage.StoredRowsNote, root.GetProperty("note").GetString());
    });

    [Fact]
    public Task Heatmap_CountsEachStoredStatementOncePerCell_AndSaysItIsNotEveryStatement() => RunAsync(async (postgres, ct) =>
    {
        await SeedAsync(postgres, ct);
        using var doc = JsonDocument.Parse(await DarlingMcpQueryHeatmapTools.GetQueryHeatmap(
            postgres, ServerName, 4, "cpu", null, 60, 100, AsOf, false, null, ct));
        var root = doc.RootElement;

        /* #7881: a cell counts distinct statements, so HEAVY, MID and OTHER count once each, however many runs stored them. */
        Assert.Equal(3, root.GetProperty("cells").EnumerateArray().Sum(c => c.GetProperty("query_count").GetInt64()));
        Assert.Equal(QueryStatsCoverage.StoredRowsNote, root.GetProperty("coverage_note").GetString());
    });
}
