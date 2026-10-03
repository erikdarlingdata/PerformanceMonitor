/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

public sealed class FinOpsDatabaseResourcesViewTests
{
    [Fact]
    public void ServedHead_NamesTheView_AndStaysUnderTheTarget()
    {
        var served = McpToolGuideTests.Served("get_finops");
        Assert.Contains("database_resources:", served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head is {served.Served.Length}");
        Assert.True(DarlingMcpFinOpsTools.DatabaseResourcesViewLine.Length <= 80);
    }

    [Fact]
    public void GuideTail_CarriesTheViewsKeyFacts()
    {
        var tail = McpToolGuideTests.Served("get_finops").Tail;
        Assert.NotNull(tail);
        Assert.Contains("rounded to 0.01", tail, StringComparison.Ordinal);
        Assert.Contains("rows lists every database up to 500, ordered by CPU, then I/O, then name", tail, StringComparison.Ordinal);
        Assert.Contains("limit caps only these two lists", tail, StringComparison.Ordinal);
        Assert.Contains("top_by_total can include databases with 0 CPU that only had file I/O", tail, StringComparison.Ordinal);
        Assert.Contains("top_by_avg is empty when no database has executions in the window", tail, StringComparison.Ordinal);
        Assert.Contains("Above 72 hours, rows reads the per-database rollup and the top lists read the query-grain rollup", tail, StringComparison.Ordinal);
        Assert.Contains("avg_cpu_ms is rounded to a whole millisecond, avg_io_mb has 4 decimals, io_total_mb has 2, and total_cpu_ms is the total behind the average", tail, StringComparison.Ordinal);
        Assert.Contains("the desktop's top-consumer grids always cover 24 hours, so send hours_back=24 to match them", tail, StringComparison.Ordinal);
        Assert.Contains("database_count", tail, StringComparison.Ordinal);
        Assert.Contains("truncated", tail, StringComparison.Ordinal);
        Assert.Contains("hourly or daily rollups", tail, StringComparison.Ordinal);
        Assert.Contains("top_by_total and top_by_avg rank databases by total CPU and by CPU per execution", tail, StringComparison.Ordinal);
        Assert.Contains("a database with no executions is left out of top_by_avg", tail, StringComparison.Ordinal);
        Assert.Contains("an unattributed one is left out of top_by_total", tail, StringComparison.Ordinal);
        Assert.Contains("No cost fields.", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewsAllowList_ContainsTheView() =>
        Assert.Contains("database_resources", DarlingMcpFinOpsTools.Views);
}

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsDatabaseResourcesViewLiveTests
{
    private const string ServerName = "darling-finops-dbres-view-a";
    private const string EmptyServerName = "darling-finops-dbres-view-b";
    private const string PostgresServerName = "darling-finops-dbres-view-c";
    private const int SeededDatabases = 8;
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);
    private static readonly int PostgresServerId = ServerIdHelper.GetDeterministicHashCode(PostgresServerName);

    private static async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, EmptyServerId, EmptyServerName, ct);
        await PgTargetFactCollectorTests.RegisterServerAsync(c, PostgresServerId, PostgresServerName, MonitoredEngineKind.Postgres, 16, ct);
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-2);
        var dbs = new[] { "tenant_a", "tenant_b", "tenant_c", "tenant_d", "tenant_e", "tenant_f" };
        for (var i = 0; i < dbs.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                    delta_worker_time, delta_execution_count, delta_logical_reads, delta_physical_reads, delta_logical_writes,
                    sample_interval_seconds)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, dbs[i], "0xQ" + CollectionIdGenerator.Next(),
                (long)(i + 1) * 1_700_000L, i == 5 ? 0L : 10L + i, 1000L * (i + 1), 100L + i, 50L + i, 60);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name,
                    file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
                    delta_stall_read_ms, delta_stall_write_ms)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, dbs[i], "f" + i + ".mdf", "ROWS", "D:\\data\\f" + i + ".mdf",
                100m, 10L, 10L, 1_054_000L * (i + 1), 3_333_333L + i, 120L + i, 30L + i);
        }

        /* Two databases with file I/O only (no query stats), different I/O totals: tenant_h has more I/O but the later name. */
        var ioOnly = new[] { ("tenant_g", 500_000L), ("tenant_h", 900_000L) };
        for (var i = 0; i < ioOnly.Length; i++)
        {
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name,
                    file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
                    delta_stall_read_ms, delta_stall_write_ms)
                  VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
                CollectionIdGenerator.Next(), at, ServerId, ServerName, ioOnly[i].Item1, "g" + i + ".mdf", "ROWS", "D:\\data\\g" + i + ".mdf",
                100m, 5L, 5L, ioOnly[i].Item2, 1_000L, 10L, 5L);
        }

        return scratch;
    }

    private static async Task<(string Total, string Avg)> ExpectedTopJsonAsync(NpgsqlDataSource ds, int limit, CancellationToken ct)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(ds, ct);
        var (byTotal, byAvg) = await DarlingFinOpsDatabaseResourcesReader.GetTopResourceConsumersAsync(
            ds, ServerId, rollups, coverage, cutoff, 60, limit, ct);
        Assert.DoesNotContain(byAvg, r => r.DatabaseName == "tenant_f");
        return (JsonSerializer.Serialize(byTotal.Select(DarlingMcpFinOpsTools.TopByTotalRow).ToList(), McpHelpers.JsonOptions),
            JsonSerializer.Serialize(byAvg.Select(DarlingMcpFinOpsTools.TopByAvgRow).ToList(), McpHelpers.JsonOptions));
    }

    private static async Task<string> ExpectedRowsJsonAsync(NpgsqlDataSource ds, int limit, CancellationToken ct)
    {
        var cutoff = DateTime.UtcNow.AddHours(-24);
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(ds, ct);
        var rows = await DarlingFinOpsDatabaseResourcesReader.GetDatabaseResourceUsageAsync(
            ds, ServerId, rollups, coverage, cutoff, 60, ct);
        return JsonSerializer.Serialize(
            DarlingMcpFinOpsTools.OrderDatabaseRows(rows).Take(limit).Select(DarlingMcpFinOpsTools.DatabaseResourcesRow).ToList(),
            McpHelpers.JsonOptions);
    }

    private static string? Cs()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live database_resources view test.");
        return cs;
    }

    [Fact]
    public async Task ToolRows_EqualTheStorageReaderRows_InCpuOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_resources", ServerName, 24, 50, cancellationToken: ct));
        Assert.Equal(SeededDatabases, tool.RootElement.GetProperty("database_count").GetInt32());
        Assert.False(tool.RootElement.GetProperty("truncated").GetBoolean());
        var names = tool.RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("database_name").GetString()).ToArray();
        Assert.Equal("tenant_f", names[0]);
        /* The I/O-only databases tie at 0 CPU: more I/O first, whatever the name. */
        Assert.Equal(new[] { "tenant_h", "tenant_g" }, names.TakeLast(2).ToArray());
        var ioOnlyTotal = tool.RootElement.GetProperty("top_by_total").EnumerateArray()
            .Where(r => r.GetProperty("database_name").GetString() is "tenant_g" or "tenant_h").ToList();
        Assert.Equal(2, ioOnlyTotal.Count);
        Assert.All(ioOnlyTotal, r => Assert.Equal(0, r.GetProperty("cpu_time_ms").GetInt64()));
        Assert.DoesNotContain(tool.RootElement.GetProperty("top_by_avg").EnumerateArray(),
            r => r.GetProperty("database_name").GetString() is "tenant_g" or "tenant_h");
        Assert.Equal(await ExpectedRowsJsonAsync(ds, 50, ct), tool.RootElement.GetProperty("rows").GetRawText());
        var top = await ExpectedTopJsonAsync(ds, 50, ct);
        Assert.Equal(top.Total, tool.RootElement.GetProperty("top_by_total").GetRawText());
        Assert.Equal(top.Avg, tool.RootElement.GetProperty("top_by_avg").GetRawText());
    }

    [Fact]
    public async Task Limit_CapsTheTopLists_AndNotTheRows()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_resources", ServerName, 24, 2, cancellationToken: ct));
        Assert.Equal(2, tool.RootElement.GetProperty("top_by_total").GetArrayLength());
        Assert.Equal(2, tool.RootElement.GetProperty("top_by_avg").GetArrayLength());
        Assert.Equal(SeededDatabases, tool.RootElement.GetProperty("rows").GetArrayLength());
        Assert.False(tool.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(SeededDatabases, tool.RootElement.GetProperty("database_count").GetInt32());
        Assert.Equal(await ExpectedRowsJsonAsync(ds, int.MaxValue, ct), tool.RootElement.GetProperty("rows").GetRawText());
        var top = await ExpectedTopJsonAsync(ds, 2, ct);
        Assert.Equal(top.Total, tool.RootElement.GetProperty("top_by_total").GetRawText());
        Assert.Equal(top.Avg, tool.RootElement.GetProperty("top_by_avg").GetRawText());
    }

    [Fact]
    public async Task ServerWithNoRows_GetsAStatus_NotAnError()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_resources", EmptyServerName, 24, 10, cancellationToken: ct));
        var status = tool.RootElement.GetProperty("status").GetString();
        Assert.Equal("empty", status);
    }

    [Fact]
    public async Task PostgresServerWithNoRows_IsNotCollected_WhenBothCollectorsAreGated()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_resources", PostgresServerName, 24, 10, cancellationToken: ct));
        Assert.Equal("not_collected", tool.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task ReadRoute_ReturnsTheToolsBody()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var tool = await DarlingMcpFinOpsTools.GetFinOps(ds, "database_resources", ServerName, 24, 50, cancellationToken: ct);

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(ds);
        await using var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, ds, new CollectorRuntimeState(), new CapturingTestLogger());
        await app.StartAsync(ct);
        using var server = app.GetTestServer();
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = "/api/read/get_finops";
            request.Request.QueryString = new QueryString($"?server={ServerName}&view=database_resources&hours=24&limit=50");
            request.Request.Headers.Host = "localhost";
        });
        using var reader = new StreamReader(context.Response.Body);
        var body = await reader.ReadToEndAsync(ct);

        Assert.Equal(StatusCodes.Status200OK, context.Response.StatusCode);
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
    }
}
