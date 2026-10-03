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

/// <summary>
/// Live parity for <c>get_finops_inventory</c>: the tool's rows are the store readers' rows mapped through
/// <see cref="DarlingMcpFinOpsInventoryTools.InventoryRow"/>, in the tool's order, and the web read route answers
/// with the tool's own body. Three servers are seeded: alpha (enabled, CPU and size rows), beta (disabled Azure SQL
/// Database with a large stored host memory) and gamma (enabled, no CPU sample).
/// </summary>
/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsInventoryToolParityLiveTests
{
    private const string Alpha = "darling-finops-inv-tool-alpha";
    private const string Beta = "darling-finops-inv-tool-beta";
    private const string Gamma = "darling-finops-inv-tool-gamma";

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<ScratchPostgres> SeedAsync(string cs, bool servers, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        if (!servers) return scratch;

        var idA = ServerIdHelper.GetDeterministicHashCode(Alpha);
        var idB = ServerIdHelper.GetDeterministicHashCode(Beta);
        var idG = ServerIdHelper.GetDeterministicHashCode(Gamma);
        await DarlingMcpTestData.RegisterServerAsync(c, idA, Alpha, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, idB, Beta, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, idG, Gamma, ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET is_enabled = FALSE WHERE server_id = $1", idB);
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET monthly_cost_usd = 1234.5 WHERE server_id = $1", idA);

        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        var cpu = new[] { 10, 20, 30, 95 };
        for (var i = 0; i < cpu.Length; i++)
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, 1)",
                CollectionIdGenerator.Next(), now.AddHours(-1 - i), idA, Alpha, now.AddHours(-1 - i), cpu[i]);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb) VALUES ($1, $2, $3, $4, 'UserDbBusy', 2048)",
            CollectionIdGenerator.Next(), now.AddHours(-3), idA, Alpha);
        await InsertPropertiesAsync(c, idA, Alpha, now.AddHours(-5), "Enterprise Edition", "16.0.4100.1", 3, 8, 65536L, 2, 4, "Windows Server 2022", ct);
        await InsertPropertiesAsync(c, idB, Beta, now.AddHours(-6), "Azure SQL Database (Standard)", "12.0.2000.8", 5, 2, 913000L, null, null, null, ct);
        await InsertPropertiesAsync(c, idG, Gamma, now.AddHours(-7), "Standard Edition", "15.0.4000.1", 2, 4, 16384L, 1, 4, "Windows Server 2019", ct);
        return scratch;
    }

    private static Task InsertPropertiesAsync(NpgsqlConnection c, int id, string name, DateTime at, string edition, string version,
        int engineEdition, int cpuCount, long memoryMb, int? sockets, int? coresPerSocket, string? os, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, engine_edition, cpu_count, physical_memory_mb, socket_count, cores_per_socket, host_os_version) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
            CollectionIdGenerator.Next(), at, id, name, edition, version, engineEdition, cpuCount, memoryMb, sockets, coresPerSocket, os);

    private static async Task<(int Status, string Body)> GetAsync(NpgsqlDataSource postgres, string pathAndQuery, CancellationToken ct)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(postgres);
        await using var app = builder.Build();
        DarlingWebEndpoints.MapAll(app, postgres, new CollectorRuntimeState(), new CapturingTestLogger());
        await app.StartAsync(ct);
        using var server = app.GetTestServer();
        var path = pathAndQuery.Split('?', 2);
        var context = await server.SendAsync(request =>
        {
            request.Request.Method = "GET";
            request.Request.Path = path[0];
            request.Request.QueryString = new QueryString("?" + path[1]);
            request.Request.Headers.Host = "localhost";
        });
        using var reader = new StreamReader(context.Response.Body);
        return (context.Response.StatusCode, await reader.ReadToEndAsync(ct));
    }

    [Fact]
    public async Task Tool_ServersEqualTheReaderRows_ThroughInventoryRow_InTheToolsOrder()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, true, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, ct);
        var inventory = await DarlingFinOpsInventoryReader.GetServerInventoryAsync(postgres, 30, ct);
        var metrics = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(postgres, rollups, coverage, 30, cancellationToken: ct);
        var expected = inventory.Where(s => s.ServerName.StartsWith("darling-finops-inv-tool-", StringComparison.Ordinal))
            .OrderByDescending(s => s.IsEnabled).ThenBy(s => s.ServerName, StringComparer.Ordinal)
            .Select(s => JsonSerializer.SerializeToElement(
                DarlingMcpFinOpsInventoryTools.InventoryRow(s, metrics.TryGetValue(s.ServerId, out var m) ? m : default), McpHelpers.JsonOptions))
            .ToList();

        var body = await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 50, ct);
        using var doc = JsonDocument.Parse(body);
        var servers = doc.RootElement.GetProperty("servers");
        Assert.Equal(3, expected.Count);
        Assert.Equal(3, servers.GetArrayLength());
        for (var i = 0; i < expected.Count; i++)
            Assert.True(JsonElement.DeepEquals(expected[i], servers[i]), servers[i].GetRawText());
        Assert.Equal(new[] { Alpha, Gamma, Beta }, servers.EnumerateArray().Select(s => s.GetProperty("server").GetString()).ToArray());

        var alpha = servers[0];
        Assert.Equal("good", alpha.GetProperty("health_band").GetString());
        Assert.Equal(38.8m, alpha.GetProperty("avg_cpu_pct").GetDecimal());
        Assert.Equal(1234.5m * 12, alpha.GetProperty("annual_cost_usd").GetDecimal());

        var gamma = servers[1];
        Assert.Equal(JsonValueKind.Null, gamma.GetProperty("avg_cpu_pct").ValueKind);
        Assert.Equal(FinOpsInventoryFigures.HealthScore(null), gamma.GetProperty("health_score").GetInt32());
        Assert.NotEqual(FinOpsInventoryFigures.HealthScore(0m), gamma.GetProperty("health_score").GetInt32());

        var beta = servers[2];
        Assert.Equal("stopped", beta.GetProperty("monitoring").GetString());
        Assert.Equal(JsonValueKind.Null, beta.GetProperty("physical_memory_mb").ValueKind);
        Assert.False(string.IsNullOrEmpty(beta.GetProperty("hardware_note").GetString()));
        Assert.Equal(JsonValueKind.Null, beta.GetProperty("license_warning").ValueKind);
    }

    [Fact]
    public async Task Tool_WithLimitOne_IsTruncated()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, true, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = JsonDocument.Parse(await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 1, ct));
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
        Assert.Equal(1, doc.RootElement.GetProperty("servers_returned").GetInt32());
        Assert.Equal(3, doc.RootElement.GetProperty("total_servers").GetInt32());
        Assert.Equal(1, doc.RootElement.GetProperty("servers").GetArrayLength());
    }

    [Fact]
    public async Task WebRoute_ReturnsTheToolsBody_AndHonoursLimit()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, true, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var tool = await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 2, ct);
        var (status, body) = await GetAsync(postgres, "/api/read/get_finops_inventory?view=server_inventory&limit=2", ct);

        Assert.Equal(StatusCodes.Status200OK, status);
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
        Assert.Equal(2, actual.RootElement.GetProperty("servers_returned").GetInt32());
        Assert.True(actual.RootElement.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public async Task WebRoute_RefusesAnUnknownView_WithTheToolsRefusal()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, true, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var tool = await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "nope", 10, ct);
        var (status, body) = await GetAsync(postgres, "/api/read/get_finops_inventory?view=nope", ct);

        Assert.Equal(StatusCodes.Status400BadRequest, status);
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
        Assert.Equal("invalid", actual.RootElement.GetProperty("status").GetString());
        Assert.Contains("Invalid view value 'nope'", actual.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratedStoreWithNoServers_AnswersEmpty()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, false, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = JsonDocument.Parse(await DarlingMcpFinOpsInventoryTools.GetFinOpsInventory(postgres, "server_inventory", 10, ct));
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
    }
}
