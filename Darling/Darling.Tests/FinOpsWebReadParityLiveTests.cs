/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsWebReadParityLiveTests
{
    private static async Task<(int Status, string Body)> GetAsync(NpgsqlDataSource postgres, string pathAndQuery, System.Threading.CancellationToken ct)
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
    public async Task HighImpactView_ThroughTheReadRoute_ReturnsTheToolsBody()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_finops route test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FinOpsHighImpactReaderLiveTests.SeedAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var name = FinOpsHighImpactReaderLiveTests.ServerName;
        var tool = await DarlingMcpFinOpsTools.GetFinOps(postgres, "high_impact", name, 96, 10, ct);
        var (status, body) = await GetAsync(postgres, $"/api/read/get_finops?server={name}&view=high_impact&hours_back=96&limit=10", ct);

        Assert.Equal(StatusCodes.Status200OK, status);
        /* Parsed-JSON comparison: the route serialises the tool's string as is, so the parsed trees must be equal;
           a 96-hour window also takes in the older seeded row, so a route that dropped hours_back would differ. */
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
        Assert.Contains("0xHIOLD", body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task HoursAlias_BindsTheSameWindowAsHoursBack()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_finops route test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FinOpsHighImpactReaderLiveTests.SeedAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var name = FinOpsHighImpactReaderLiveTests.ServerName;
        var (_, viaHoursBack) = await GetAsync(postgres, $"/api/read/get_finops?server={name}&view=high_impact&hours_back=96", ct);
        var (status, viaHours) = await GetAsync(postgres, $"/api/read/get_finops?server={name}&view=high_impact&hours=96", ct);

        Assert.Equal(StatusCodes.Status200OK, status);
        using var expected = JsonDocument.Parse(viaHoursBack);
        using var actual = JsonDocument.Parse(viaHours);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), viaHours);
        Assert.Equal(96, actual.RootElement.GetProperty("hours_back").GetInt32());
        Assert.Contains("0xHIOLD", viaHours, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UnknownView_ThroughTheReadRoute_IsRefusedWithTheToolsRefusal()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live get_finops route test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await FinOpsHighImpactReaderLiveTests.SeedAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var name = FinOpsHighImpactReaderLiveTests.ServerName;
        var tool = await DarlingMcpFinOpsTools.GetFinOps(postgres, "nope", name, 24, 10, ct);
        var (status, body) = await GetAsync(postgres, $"/api/read/get_finops?server={name}&view=nope", ct);

        Assert.Equal(StatusCodes.Status400BadRequest, status);
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
        Assert.Equal("invalid", actual.RootElement.GetProperty("status").GetString());
        Assert.Contains("Invalid view value 'nope'", actual.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }
}
