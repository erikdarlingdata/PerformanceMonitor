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

public sealed class FinOpsApplicationConnectionsViewTests
{
    [Fact]
    public void ServedHead_NamesTheView_AndStaysUnderTheTarget()
    {
        var served = McpToolGuideTests.Served("get_finops");
        Assert.Contains("application_connections:", served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head is {served.Served.Length}");
        Assert.True(DarlingMcpFinOpsTools.ApplicationConnectionsViewLine.Length <= 80);
    }

    [Fact]
    public void GuideTail_CarriesTheViewsKeyFacts()
    {
        var tail = McpToolGuideTests.Served("get_finops").Tail;
        Assert.NotNull(tail);
        Assert.Contains("ordered by peak connections, then average connections, then name", tail, StringComparison.Ordinal);
        Assert.Contains("rows lists every application up to 500", tail, StringComparison.Ordinal);
        Assert.Contains("application_count", tail, StringComparison.Ordinal);
        Assert.Contains("truncated", tail, StringComparison.Ordinal);
        Assert.Contains("read 0 until the session collector has filled them", tail, StringComparison.Ordinal);
        Assert.Contains("A program with no name is listed with an empty application_name", tail, StringComparison.Ordinal);
        Assert.Contains("Averages are whole numbers", tail, StringComparison.Ordinal);
        Assert.Contains("hours_back is honoured and defaults to 24", tail, StringComparison.Ordinal);
        Assert.Contains("a limit other than 10 is refused", tail, StringComparison.Ordinal);
        Assert.Contains("Times are UTC and end in Z", tail, StringComparison.Ordinal);
        Assert.Contains("No cost fields.", tail, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewsAllowList_ContainsTheView() =>
        Assert.Contains("application_connections", DarlingMcpFinOpsTools.Views);
}

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsApplicationConnectionsViewLiveTests
{
    private const string ServerName = "darling-finops-appconn-view-a";
    private const string EmptyServerName = "darling-finops-appconn-view-b";
    private const string PostgresServerName = "darling-finops-appconn-view-c";
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
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        /* Zulu and Alpha tie on peak (12): Zulu's average (8) is higher, so it leads although its name sorts last. */
        await Row(c, ct, now.AddHours(-2), "Zulu", 12, 3, 5, 4, 10, 20, 30, 40);
        await Row(c, ct, now.AddHours(-3), "Zulu", 4, 0, 4, 0, 12, 22, 32, 42);
        await Row(c, ct, now.AddHours(-2), "Alpha", 12, 3, 5, 4, null, null, null, null);
        await Row(c, ct, now.AddHours(-3), "Alpha", 1, 0, 1, 0, null, null, null, null);
        await Row(c, ct, now.AddHours(-4), "Alpha", 2, 0, 2, 0, null, null, null, null);
        /* Lima and Mike tie on peak and average: the name decides. */
        await Row(c, ct, now.AddHours(-2), "Mike", 9, 1, 1, 1, 5, 6, 7, 8);
        await Row(c, ct, now.AddHours(-2), "Lima", 9, 1, 1, 1, 5, 6, 7, 8);
        /* An unattributed program name, and a program seen only outside the window. */
        await Row(c, ct, now.AddHours(-6), null, 20, 0, 0, 0, 100, 200, 300, 400);
        await Row(c, ct, now.AddHours(-26), "Outside", 50, 5, 5, 5, 1, 1, 1, 1);
        return scratch;
    }

    private static Task Row(NpgsqlConnection c, CancellationToken ct, DateTime at, string? program, long connections,
        int running, int sleeping, int dormant, long? cpu, long? reads, long? writes, long? logicalReads) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name, connection_count,
                running_count, sleeping_count, dormant_count, total_cpu_time_ms, total_reads, total_writes, total_logical_reads)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, program, connections, running, sleeping, dormant,
            cpu, reads, writes, logicalReads);

    private static string? Cs()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live application_connections view test.");
        return cs;
    }

    private static async Task<string> ExpectedRowsJsonAsync(NpgsqlDataSource ds, CancellationToken ct)
    {
        var rows = await DarlingFinOpsApplicationConnectionsReader.GetApplicationConnectionsAsync(
            ds, ServerId, DateTime.UtcNow.AddHours(-24), 60, ct);
        return JsonSerializer.Serialize(
            DarlingMcpFinOpsTools.OrderApplicationRows(rows).Select(DarlingMcpFinOpsTools.ApplicationConnectionsRow).ToList(),
            McpHelpers.JsonOptions);
    }

    [Fact]
    public async Task ToolRows_EqualTheStorageReaderRows_InKeyOrder()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", ServerName, 24, 10, ct));
        Assert.Equal(5, tool.RootElement.GetProperty("application_count").GetInt32());
        Assert.False(tool.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(24, tool.RootElement.GetProperty("hours_back").GetInt32());
        Assert.Equal(await ExpectedRowsJsonAsync(ds, ct), tool.RootElement.GetProperty("rows").GetRawText());
        var names = tool.RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("application_name").GetString()).ToArray();
        Assert.DoesNotContain("Outside", names);
        Assert.Equal("", names[0]);
        /* The unattributed program carries 0 for the columns the collector left unset; Alpha's are NULL in the store. */
        var alpha = tool.RootElement.GetProperty("rows").EnumerateArray().Single(r => r.GetProperty("application_name").GetString() == "Alpha");
        Assert.Equal(0, alpha.GetProperty("max_cpu_time_ms").GetInt64());
    }

    [Fact]
    public async Task TiedPeaks_AreOrderedByAverage_ThenByName()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", ServerName, 24, 10, ct));
        var names = tool.RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetProperty("application_name").GetString()).ToArray();
        Assert.Equal(new[] { "", "Zulu", "Alpha", "Lima", "Mike" }, names.Take(5).ToArray());
    }

    [Fact]
    public async Task Times_EndInZ_AndEqualTheReadersInstants()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", ServerName, 24, 10, ct));
        var reader = (await DarlingFinOpsApplicationConnectionsReader.GetApplicationConnectionsAsync(
            ds, ServerId, DateTime.UtcNow.AddHours(-24), 60, ct)).ToDictionary(r => r.ApplicationName);
        foreach (var row in tool.RootElement.GetProperty("rows").EnumerateArray())
        {
            var first = row.GetProperty("first_seen_utc").GetString()!;
            var last = row.GetProperty("last_seen_utc").GetString()!;
            Assert.EndsWith("Z", first, StringComparison.Ordinal);
            Assert.EndsWith("Z", last, StringComparison.Ordinal);
            var expected = reader[row.GetProperty("application_name").GetString()!];
            Assert.Equal(DateTime.SpecifyKind(expected.FirstSeenUtc, DateTimeKind.Utc), DateTimeOffset.Parse(first).UtcDateTime);
            Assert.Equal(DateTime.SpecifyKind(expected.LastSeenUtc, DateTimeKind.Utc), DateTimeOffset.Parse(last).UtcDateTime);
        }
    }

    [Fact]
    public async Task HoursBack_IsHonoured_AndWidensTheWindow()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", ServerName, 48, 10, ct));
        Assert.Contains(tool.RootElement.GetProperty("rows").EnumerateArray(), r => r.GetProperty("application_name").GetString() == "Outside");
    }

    [Fact]
    public async Task ServerWithNoRows_IsExactlyEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var body = await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", EmptyServerName, 24, 10, ct);
        Assert.Equal(
            McpHelpers.Status("empty", "No session statistics were collected for this server in the last 24 hours, so there is no per-application connection data to show."),
            body);
    }

    [Fact]
    public async Task PostgresServerWithNoRows_IsNotCollected()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", PostgresServerName, 24, 10, ct));
        Assert.Equal("not_collected", tool.RootElement.GetProperty("status").GetString());
    }

    [Fact]
    public async Task NonDefaultLimit_IsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var body = await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", ServerName, 24, 5, ct);
        Assert.Contains("limit", body, StringComparison.Ordinal);
        Assert.Contains("application_connections", body, StringComparison.Ordinal);
        Assert.DoesNotContain("application_count", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadRoute_ReturnsTheToolsBody()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        var tool = await DarlingMcpFinOpsTools.GetFinOps(ds, "application_connections", ServerName, 24, 10, ct);

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
            request.Request.QueryString = new QueryString($"?server={ServerName}&view=application_connections&hours=24");
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
