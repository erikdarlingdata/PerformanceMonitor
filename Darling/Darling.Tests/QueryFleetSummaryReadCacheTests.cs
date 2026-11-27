/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: the one-statement fleet read (<c>get_query_fleet_summary</c>) names no server, so a call over 24 hours or more is the
/// fleet form, and its default window is 7 days. The read cache keeps that answer for 5 minutes, the way the by-tag read is kept,
/// so a second look at the same statement within that time does not pay the whole read again. The web tests run the real
/// <c>/api/read</c> pipeline with a counting handler standing in for the store under the route's own name, on a clock the test moves.
/// </summary>
public sealed class QueryFleetSummaryReadCacheTests
{
    private const string Route = "get_query_fleet_summary";
    private const string Path = "/api/read/get_query_fleet_summary?query_hash=0xAB12&hours=168";

    /// <summary>An admission with no learned history: the 7-day band is light, as it is on a fresh process.</summary>
    private static ReadAdmission LightAdmission(TimerTimeProvider clock) =>
        new(clock, ReadAdmission.DefaultWaitCap, null, 2);

    private static async Task<(int Calls, string[] Bodies)> RunAsync(string[] paths, TimeSpan[] advanceBefore, CancellationToken cancellationToken = default)
    {
        var clock = new TimerTimeProvider();
        var admission = LightAdmission(clock);
        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((Route, (_, _, _) =>
            Task.FromResult("{\"rows\":[" + Interlocked.Increment(ref calls).ToString(System.Globalization.CultureInfo.InvariantCulture) + "]}")));
        var (app, postgres) = await ReadCacheWebEndToEndTests.BuildServer(admission, new ReadLatencyAccumulator());
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();

        var bodies = new string[paths.Length];
        for (var i = 0; i < paths.Length; i++)
        {
            clock.Advance(advanceBefore[i]);
            bodies[i] = await client.GetStringAsync(paths[i], cancellationToken);
        }

        return (calls, bodies);
    }

    [Fact(Timeout = 60_000)]
    public async Task TwoSummaryReads_InsideTheTtl_RunTheReaderOnce()
    {
        var (calls, bodies) = await RunAsync(
            [Path, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2)], TestContext.Current.CancellationToken);

        Assert.Equal(1, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.Equal(bodies[0], bodies[2]);
    }

    [Fact(Timeout = 60_000)]
    public async Task ASummaryRead_IsAMiss_OnceFiveMinutesHavePassedSinceItWasStored()
    {
        var (calls, bodies) = await RunAsync(
            [Path, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(2)], TestContext.Current.CancellationToken);

        Assert.Equal(2, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.NotEqual(bodies[0], bodies[2]);
    }

    [Theory(Timeout = 60_000)]
    [InlineData("/api/read/get_query_fleet_summary?query_hash=0xCD34&hours=168")]
    [InlineData("/api/read/get_query_fleet_summary?query_hash=0xAB12&hours=72")]
    [InlineData("/api/read/get_query_fleet_summary?query_hash=0xAB12&hours=168&limit=20")]
    public async Task TwoSummaryReads_ThatDifferInTheirArguments_AreTwoEntries(string other)
    {
        var (calls, _) = await RunAsync(
            [Path, other],
            [TimeSpan.Zero, TimeSpan.FromSeconds(10)], TestContext.Current.CancellationToken);

        Assert.Equal(2, calls);
    }

    /// <summary>A summary that names no server is the fleet form at 24 hours or more; under that it is an ordinary read.</summary>
    [Fact]
    public void TheSummaryRead_IsAFleetRead_FromTwentyFourHours()
    {
        Assert.True(ReadAdmission.IsFleetRead(Route, null, 168));
        Assert.True(ReadAdmission.IsFleetRead(Route, "", 24));
        Assert.False(ReadAdmission.IsFleetRead(Route, null, 23));
        Assert.False(ReadAdmission.IsFleetRead(Route, "a", 168));
    }

    [Fact]
    public void TheSummaryTtl_IsFiveMinutes_InTheFleetForm()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.SevenDays, fleetRead: true));
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: true));
    }

    [Fact]
    public void IsCached_HoldsForTheFleetSummary_OnBothSurfaces()
    {
        var admission = LightAdmission(new TimerTimeProvider());
        foreach (var surface in new[] { ReadSurface.Web, ReadSurface.Mcp })
        {
            Assert.True(admission.IsCached(surface, Route, ReadWindowBand.SevenDays, fleetRead: true));
        }
    }

    private static McpToolParameterTypes RealTypes()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpQueryFleetSummaryTools>();
        using var provider = services.BuildServiceProvider();
        _ = provider.GetServices<McpServerTool>().ToList();
        return provider.GetRequiredService<McpToolParameterTypes>();
    }

    /// <summary>The MCP tool has no server_name, and a call that leaves hours_back out takes its declared 7-day default.</summary>
    [Theory]
    [InlineData("{\"query_hash\":\"0xAB12\"}", true)]
    [InlineData("{\"query_hash\":\"0xAB12\",\"hours_back\":24}", true)]
    [InlineData("{\"query_hash\":\"0xAB12\",\"hours_back\":168,\"limit\":20}", true)]
    [InlineData("{\"query_hash\":\"0xAB12\",\"hours_back\":12}", false)]
    public void AnMcpSummaryCall_IsAFleetRead_FromTwentyFourHours(string arguments, bool expected)
    {
        var parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(arguments)!;

        Assert.Equal(expected, McpToolLatencyFilter.IsFleetRead(Route, parsed, RealTypes()));
    }
}
