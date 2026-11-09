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
/// #8323: the FinOps Server Inventory read (<c>get_finops_inventory</c>) scans 24 hours of the fleet's CPU and grant rows on
/// every call, takes no window and no server, and so is a whole-fleet read the read cache keeps for 5 minutes, the way the
/// by-tag read is kept (#6800). A second visit, a second user or the grid's own re-render within that time does not pay the
/// fleet scan again. The web tests run the real <c>/api/read</c> pipeline with a counting handler standing in for the store
/// under the route's own name, on a clock the test moves.
/// </summary>
public sealed class FinOpsInventoryReadCacheTests
{
    private const string Route = "get_finops_inventory";
    private const string Path = "/api/read/get_finops_inventory?view=server_inventory&limit=200";

    /// <summary>An admission with no learned history: the window-less band is light, as it is on a fresh process.</summary>
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
    public async Task TwoInventoryReads_InsideTheTtl_RunTheReaderOnce()
    {
        var (calls, bodies) = await RunAsync(
            [Path, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2)], TestContext.Current.CancellationToken);

        Assert.Equal(1, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.Equal(bodies[0], bodies[2]);
    }

    [Fact(Timeout = 60_000)]
    public async Task AnInventoryRead_IsAMiss_OnceFiveMinutesHavePassedSinceItWasStored()
    {
        var (calls, bodies) = await RunAsync(
            [Path, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(2)], TestContext.Current.CancellationToken);

        Assert.Equal(2, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.NotEqual(bodies[0], bodies[2]);
    }

    [Fact(Timeout = 60_000)]
    public async Task TwoInventoryReads_ThatDifferInTheirArguments_AreTwoEntries()
    {
        var (calls, _) = await RunAsync(
            [Path, "/api/read/get_finops_inventory?view=server_inventory&limit=34"],
            [TimeSpan.Zero, TimeSpan.FromSeconds(10)], TestContext.Current.CancellationToken);

        Assert.Equal(2, calls);
    }

    /// <summary>The read takes no window, so it is a fleet read with no hours at all; a blank server name is no server.</summary>
    [Fact]
    public void TheInventoryRead_IsAFleetRead_WithNoWindowAndNoServer()
    {
        Assert.True(ReadAdmission.IsFleetRead(Route, null, null));
        Assert.True(ReadAdmission.IsFleetRead(Route, "", null));
        Assert.True(ReadAdmission.IsFleetRead(Route, "  ", null));
        Assert.False(ReadAdmission.IsFleetRead(Route, "a", null));

        /* The by-tag read keeps its 24-hour floor: no window is still not long enough there. */
        Assert.False(ReadAdmission.IsFleetRead("get_resource_use_by_tag", null, null));
        Assert.False(ReadAdmission.IsFleetRead("get_top_queries_by_cpu", null, null));
    }

    [Fact]
    public void TheInventoryTtl_IsFiveMinutes()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.None, fleetRead: true));
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: true));
    }

    [Fact]
    public void IsCached_HoldsForTheInventoryRead_OnBothSurfaces_AndItTakesNoSlot()
    {
        var admission = LightAdmission(new TimerTimeProvider());
        foreach (var surface in new[] { ReadSurface.Web, ReadSurface.Mcp })
        {
            Assert.True(admission.IsCached(surface, Route, ReadWindowBand.None, fleetRead: true));
            Assert.False(admission.IsCached(surface, Route, ReadWindowBand.None, fleetRead: false));
        }

        Assert.False(admission.IsHeavy(ReadSurface.Web, Route, ReadWindowBand.None, out _));
    }

    private static McpToolParameterTypes RealTypes()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpFinOpsInventoryTools>();
        using var provider = services.BuildServiceProvider();
        _ = provider.GetServices<McpServerTool>().ToList();
        return provider.GetRequiredService<McpToolParameterTypes>();
    }

    /// <summary>The MCP tool has no server_name and no hours_back argument at all, so every call is the fleet form.</summary>
    [Theory]
    [InlineData("{\"view\":\"server_inventory\"}")]
    [InlineData("{\"view\":\"server_inventory\",\"limit\":200}")]
    [InlineData("{\"view\":\"server_inventory\",\"limit\":200,\"include_removed\":true}")]
    public void AnMcpInventoryCall_IsAFleetRead(string arguments)
    {
        var parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(arguments)!;

        Assert.True(McpToolLatencyFilter.IsFleetRead(Route, parsed, RealTypes()));
    }
}
