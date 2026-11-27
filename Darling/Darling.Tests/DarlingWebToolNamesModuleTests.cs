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
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The web page's plain-text rules read the registered MCP tool names from a module the web host writes from the tool registry
/// (<see cref="DarlingMcpToolTypes.ToolNamesModule"/>), so the page's scripts keep no list of names. This runs the REAL pipeline
/// (<c>ConfigurePipeline</c>) over the service's own <c>wwwroot</c> and asks for the module the way the page imports it.
/// </summary>
public sealed class DarlingWebToolNamesModuleTests
{
    private static async Task<TestServer> BuildServer()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();

        /* A pool the module's route never opens. */
        var postgres = NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();
        var host = new DarlingWebHostService(
            NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache());
        host.ConfigurePipeline(
            app,
            postgres,
            networkMode: false,
            networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"),
            accessToken: "unused-in-loopback-mode",
            oidcClient: null);

        await app.StartAsync();
        return app.GetTestServer();
    }

    [Fact]
    public async Task TheModule_IsServedBesidePlainTextJs_AsJavaScript_WithEveryRegisteredToolName()
    {
        using var server = await BuildServer();
        var ctx = await server.SendAsync(c =>
        {
            c.Request.Method = "GET";
            c.Request.Path = DarlingMcpToolTypes.ToolNamesModulePath;
            c.Request.Headers.Host = "localhost";
            c.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        Assert.Equal(StatusCodes.Status200OK, ctx.Response.StatusCode);
        Assert.StartsWith("text/javascript", ctx.Response.ContentType, StringComparison.Ordinal);
        Assert.Equal("no-cache", ctx.Response.Headers.CacheControl.ToString());

        using var reader = new StreamReader(ctx.Response.Body);
        var body = await reader.ReadToEndAsync();
        Assert.Equal(DarlingMcpToolTypes.ToolNamesModule, body);

        const string Export = "export const MCP_REGISTERED_TOOL_NAMES = ";
        var start = body.IndexOf(Export, StringComparison.Ordinal);
        Assert.True(start >= 0, body);
        var json = body[(start + Export.Length)..].TrimEnd().TrimEnd(';');
        var names = JsonSerializer.Deserialize<string[]>(json)!;

        Assert.Equal(McpToolsListBudgetTests.BuildServedTools().Tools.Select(t => t.ProtocolTool.Name).OrderBy(n => n, StringComparer.Ordinal), names);
        Assert.Contains("get_wait_stats", names);
        Assert.Contains("add_servers", names);
    }
}
