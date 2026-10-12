/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A newest-first capped grid's note is judged against the window the read itself used (#4966). The web route takes the
/// window's end once, before the read, hands it to the read as its anchor, and gives the same anchor to the note check, so a
/// page whose oldest row falls between the read's start and a later clock reading still gets its note. A request that sends
/// its own anchor is read and judged as it always was.
/// </summary>
public sealed class WebDataStartNoteCappedWindowTests
{
    private const int Hours = 48;

    private static async Task<TestServer> BuildServerAsync()
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(NpgsqlDataSource.Create("Host=localhost;Database=postgres;Username=darling"));
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);

        var app = builder.Build();
        var host = new DarlingWebHostService(
            Microsoft.Extensions.Logging.Abstractions.NullLogger<DarlingWebHostService>.Instance,
            new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(), baselineCache: null);
        host.ConfigurePipeline(
            app, app.Services.GetRequiredService<NpgsqlDataSource>(), networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), accessToken: "unused-in-loopback-mode", oidcClient: null);
        await app.StartAsync();
        return app.GetTestServer();
    }

    private static async Task<(JsonObject Answer, string? SeenAnchor)> AskAsync(string query)
    {
        string? seen = null;
        using var seam = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope(("get_collection_log", async (context, _, _) =>
        {
            seen = context.Request.Query["as_of"].ToString();
            var end = string.IsNullOrEmpty(seen)
                ? DateTime.UtcNow
                : DateTime.Parse(seen, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

            /* The read takes its start from the end it was given; its page then stops a few milliseconds after that
               start, and the clock moves on before the note is checked. */
            await Task.Delay(60);
            var oldest = end.AddHours(-Hours).AddMilliseconds(5);
            return "{\"server\":\"sql01\",\"hours_back\":48,\"run_count\":200,\"truncated\":true,"
                + "\"oldest_returned_collection_time\":\"" + oldest.ToString("o", CultureInfo.InvariantCulture) + "\","
                + "\"newest_returned_collection_time\":\"" + end.ToString("o", CultureInfo.InvariantCulture) + "\","
                + "\"order\":\"collection_time_desc\",\"runs\":[{\"collector\":\"wait_stats\"}]}";
        }));

        var server = await BuildServerAsync();
        var context = await server.SendAsync(ctx =>
        {
            ctx.Request.Method = "GET";
            ctx.Request.Path = "/api/read/get_collection_log";
            ctx.Request.QueryString = new QueryString(query);
            ctx.Request.Headers.Host = "localhost";
            ctx.Connection.RemoteIpAddress = IPAddress.Loopback;
        });

        Assert.Equal(200, context.Response.StatusCode);
        using var reader = new StreamReader(context.Response.Body);
        return (Assert.IsType<JsonObject>(JsonNode.Parse(await reader.ReadToEndAsync())), seen);
    }

    [Fact]
    public async Task ACappedPage_WhoseOldestRowFallsAfterTheReadsStart_GetsItsNote_WhenTheRequestSentNoAnchor()
    {
        var (answer, seen) = await AskAsync("?server=sql01&hours=48&limit=200");

        Assert.False(string.IsNullOrEmpty(seen), "the read was handed the window's end it was judged against");
        Assert.True(answer["window_truncated"]?.GetValue<bool>(), "the page stops after the read's own window start, so it is noted");
        Assert.NotNull(answer["oldest_shown_utc"]);
    }

    [Fact]
    public async Task ARequestWithItsOwnAnchor_IsReadAndJudgedAsItAlwaysWas()
    {
        var anchor = DateTime.UtcNow.AddHours(-3).ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
        var (answer, seen) = await AskAsync("?server=sql01&hours=48&limit=200&as_of=" + anchor);

        Assert.Equal(anchor, seen);
        Assert.True(answer["window_truncated"]?.GetValue<bool>());
    }
}
