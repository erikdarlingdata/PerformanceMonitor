/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
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
/// Live parity for <c>get_finops_recommendations</c>: for each golden server the tool's rows equal the Storage
/// composer's rows, mapped here to literal keys and read under the invariant culture; the web read route answers with
/// the tool's own body; a failed check is named and its rows are absent; every body fits the response budget.
/// </summary>
/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsRecommendationsToolParityLiveTests
{
    private const int TimeoutSeconds = 30;
    private const string NoCost = "monthly cost not set";

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await FinOpsRecommendationsGoldenLiveTests.SeedAsync(c, ct, DarlingMcpTestData.Naive(DateTime.UtcNow));
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET monthly_cost_usd = 1000 WHERE server_id = $1",
            FinOpsRecommendationsGoldenLiveTests.ServerIdA);
        return scratch;
    }

    private static readonly (string Name, int Id)[] Servers =
    {
        (FinOpsRecommendationsGoldenLiveTests.ServerNameA, FinOpsRecommendationsGoldenLiveTests.ServerIdA),
        (FinOpsRecommendationsGoldenLiveTests.ServerNameB, FinOpsRecommendationsGoldenLiveTests.ServerIdB),
        (FinOpsRecommendationsGoldenLiveTests.ServerNameC, FinOpsRecommendationsGoldenLiveTests.ServerIdC),
    };

    /// <summary>The expected rows, built from the composer's rows with literal keys; no tool helper is used.</summary>
    private static async Task<List<JsonElement>> ExpectedRowsAsync(NpgsqlDataSource postgres, int serverId, CancellationToken ct, Func<FinOpsRecommendation, bool>? keep = null)
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
            var monthly = await FinOpsUtilizationFigures.GetMonthlyCostUsdAsync(postgres, serverId, TimeoutSeconds, ct);
            var rows = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(postgres, serverId, monthly, TimeoutSeconds, cancellationToken: ct);
            return rows.Where(r => keep == null || keep(r)).Select(r => JsonSerializer.SerializeToElement(new Dictionary<string, object?>
            {
                ["category"] = r.Category,
                ["severity"] = r.Severity,
                ["confidence"] = r.Confidence,
                ["finding"] = r.Finding,
                ["detail"] = r.Detail,
                ["est_savings_usd_month"] = r.EstMonthlySavings is decimal s ? Math.Round(s, 2, MidpointRounding.AwayFromZero) : (decimal?)null,
            })).ToList();
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }

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

    private static Task<string> ToolAsync(NpgsqlDataSource postgres, string server, CancellationToken ct) =>
        DarlingMcpFinOpsRecommendationsTools.GetFinOpsRecommendations(postgres, server, ct);

    [Fact]
    public async Task Tool_EqualsTheComposer_ForEachServer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_recommendations test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var (name, id) in Servers)
        {
            var expected = await ExpectedRowsAsync(postgres, id, ct);
            using var doc = JsonDocument.Parse(await ToolAsync(postgres, name, ct));
            var root = doc.RootElement;
            var actual = root.GetProperty("recommendations");
            Assert.Equal(expected.Count, actual.GetArrayLength());
            for (var i = 0; i < expected.Count; i++)
                Assert.True(JsonElement.DeepEquals(expected[i], actual[i]), $"{name} row {i}: {actual[i].GetRawText()}");
            Assert.Equal(actual.GetArrayLength(), root.GetProperty("recommendation_count").GetInt32());
            Assert.Equal(0, root.GetProperty("skipped_checks").GetArrayLength());
            Assert.Equal(name, root.GetProperty("server").GetString());
            if (id == FinOpsRecommendationsGoldenLiveTests.ServerIdA)
            {
                Assert.NotEmpty(expected);
                Assert.Equal(1000m, root.GetProperty("monthly_cost_usd").GetDecimal());
                Assert.Equal(JsonValueKind.Null, root.GetProperty("cost_reason").ValueKind);
            }
            else
            {
                Assert.Equal(JsonValueKind.Null, root.GetProperty("monthly_cost_usd").ValueKind);
                Assert.Equal(NoCost, root.GetProperty("cost_reason").GetString());
            }
        }
    }

    [Fact]
    public async Task Tool_AnswersInvariantText_UnderAnotherCulture()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_recommendations test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var name = FinOpsRecommendationsGoldenLiveTests.ServerNameA;
        var expected = await ExpectedRowsAsync(postgres, FinOpsRecommendationsGoldenLiveTests.ServerIdA, ct);

        var saved = CultureInfo.CurrentCulture;
        string body;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            body = await ToolAsync(postgres, name, ct);
            Assert.Equal("de-DE", CultureInfo.CurrentCulture.Name);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }

        using var doc = JsonDocument.Parse(body);
        var actual = doc.RootElement.GetProperty("recommendations");
        Assert.Equal(expected.Count, actual.GetArrayLength());
        for (var i = 0; i < expected.Count; i++)
            Assert.True(JsonElement.DeepEquals(expected[i], actual[i]), actual[i].GetRawText());
        /* Under de-DE the grouped number would read "13.080 MB". */
        Assert.Contains(actual.EnumerateArray(), r => r.GetProperty("detail").GetString()!.Contains("13,080 MB", StringComparison.Ordinal));
        Assert.DoesNotContain("13.080", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WebRead_ReturnsTheToolBody()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_recommendations test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var name = FinOpsRecommendationsGoldenLiveTests.ServerNameA;

        var tool = await ToolAsync(postgres, name, ct);
        var (status, body) = await GetAsync(postgres, "/api/read/get_finops_recommendations?server=" + Uri.EscapeDataString(name), ct);
        Assert.Equal(200, status);
        using var toolDoc = JsonDocument.Parse(tool);
        using var webDoc = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(toolDoc.RootElement, webDoc.RootElement), body);
        Assert.True(webDoc.RootElement.GetProperty("recommendation_count").GetInt32() > 0);
    }

    [Fact]
    public async Task UnknownServer_ReturnsTheResolverError()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_recommendations test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var (_, resolverError) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, "NoSuchServerZ", ct);
        Assert.NotNull(resolverError);
        var tool = await ToolAsync(postgres, "NoSuchServerZ", ct);
        Assert.Equal(resolverError, tool);

        var (status, body) = await GetAsync(postgres, "/api/read/get_finops_recommendations?server=NoSuchServerZ", ct);
        Assert.Equal(tool, body);
        /* The read route answers a resolver refusal with 400. */
        Assert.Equal(400, status);
    }

    [Fact]
    public async Task AFailedCheck_IsNamed_AndItsRowsAreAbsent()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_recommendations test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var name = FinOpsRecommendationsGoldenLiveTests.ServerNameA;
        var whole = await ExpectedRowsAsync(postgres, FinOpsRecommendationsGoldenLiveTests.ServerIdA, ct);
        var without = await ExpectedRowsAsync(postgres, FinOpsRecommendationsGoldenLiveTests.ServerIdA, ct, r => r.Category != "Maintenance");
        Assert.True(whole.Count > without.Count);

        /* The maintenance read selects from the v_running_jobs view; dropping it on this scratch store breaks that one read. */
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await using var drop = new NpgsqlCommand("DROP VIEW v_running_jobs CASCADE", connection);
            await drop.ExecuteNonQueryAsync(ct);
        }

        using var doc = JsonDocument.Parse(await ToolAsync(postgres, name, ct));
        var root = doc.RootElement;
        Assert.Equal(new[] { "Maintenance window" }, root.GetProperty("skipped_checks").EnumerateArray().Select(s => s.GetString()).ToArray());
        var actual = root.GetProperty("recommendations");
        Assert.Equal(without.Count, actual.GetArrayLength());
        Assert.Equal(without.Count, root.GetProperty("recommendation_count").GetInt32());
        for (var i = 0; i < without.Count; i++)
            Assert.True(JsonElement.DeepEquals(without[i], actual[i]), actual[i].GetRawText());
        Assert.DoesNotContain(actual.EnumerateArray(), r => r.GetProperty("category").GetString() == "Maintenance");
    }

    [Fact]
    public async Task TheBody_FitsTheResponseBudget()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live get_finops_recommendations test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        foreach (var (name, _) in Servers)
        {
            var bytes = Encoding.UTF8.GetByteCount(await ToolAsync(postgres, name, ct));
            TestContext.Current.SendDiagnosticMessage($"get_finops_recommendations body for {name}: {bytes} bytes (budget {McpResponseBudget.DefaultBytes})");
            Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"{name}: {bytes} bytes over the {McpResponseBudget.DefaultBytes} budget");
        }
    }
}
