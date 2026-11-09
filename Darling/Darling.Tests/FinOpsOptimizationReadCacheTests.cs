/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#nullable enable

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The FinOps Optimization read (<c>get_finops</c> with <c>view=optimization</c>) reads a fixed 7 days of idle-database coverage and
/// 24 hours each of tempdb, wait, query and memory-grant rows for one server on every call, so the read cache keeps one server's answer
/// for 5 minutes from the moment it is stored, the way the by-tag and Server Inventory fleet reads are kept. A second visit, a second
/// user or the tab's own re-render within that time does not pay the read again, and a 12-second read stored late in a wall-clock bucket
/// is still reachable. The other FinOps views keep the band's rule. The tests run the real <c>/api/read</c> pipeline with a counting
/// handler standing in for the store under the route's own name, on a clock the test moves.
/// </summary>
public sealed class FinOpsOptimizationReadCacheTests
{
    private const string Route = "get_finops";
    private const string Path = "/api/read/get_finops?view=optimization&server=alpha&hours=24&limit=20";

    /// <summary>An admission with no learned history: the 24-hour band is light, as it is on a fresh process.</summary>
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
    public async Task ThreeOptimizationReads_ForOneServer_InsideTheTtl_RunTheReaderOnce()
    {
        var (calls, bodies) = await RunAsync(
            [Path, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2)], TestContext.Current.CancellationToken);

        Assert.Equal(1, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.Equal(bodies[0], bodies[2]);
    }

    /// <summary>The entry is held for 5 minutes from the moment it is stored, not for what is left of a wall-clock bucket: a read at
    /// 4 minutes is a hit and a read 2 minutes after that, 6 minutes after the store, is a miss.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AnOptimizationRead_IsAMiss_OnceFiveMinutesHavePassedSinceItWasStored()
    {
        var (calls, bodies) = await RunAsync(
            [Path, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(2)], TestContext.Current.CancellationToken);

        Assert.Equal(2, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.NotEqual(bodies[0], bodies[2]);
    }

    /// <summary>The answer is one server's: another server, or another window, is its own entry and runs its own read.</summary>
    [Fact(Timeout = 60_000)]
    public async Task OptimizationReads_ForTwoServers_OrTwoWindows_AreTheirOwnEntries()
    {
        var (calls, _) = await RunAsync(
            [Path, "/api/read/get_finops?view=optimization&server=bravo&hours=24&limit=20", "/api/read/get_finops?view=optimization&server=alpha&hours=4&limit=20"],
            [TimeSpan.Zero, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)], TestContext.Current.CancellationToken);

        Assert.Equal(3, calls);
    }

    /// <summary>Only the optimization view is held: another FinOps view of the same server over the same window keeps the band's
    /// rule, and with no learned history the 24-hour band is light, so each read runs.</summary>
    [Fact(Timeout = 60_000)]
    public async Task AnotherFinOpsView_ForTheSameServer_KeepsTheBandsRule()
    {
        const string utilization = "/api/read/get_finops?view=utilization&server=alpha&hours=24&limit=20";
        var (calls, _) = await RunAsync(
            [utilization, utilization, Path, Path],
            [TimeSpan.Zero, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(10)], TestContext.Current.CancellationToken);

        Assert.Equal(3, calls);
    }

    /// <summary>The hold needs a server and the view the table names, as the tool reads the view: trimmed and in any case. A blank
    /// server is no server, another view is not held, and neither is another route with the same view word.</summary>
    [Fact]
    public void TheOptimizationRead_IsHeld_OnlyWithAServerAndItsView()
    {
        Assert.True(ReadAdmission.IsServerHeldRead(Route, "alpha", "optimization"));
        Assert.True(ReadAdmission.IsServerHeldRead(Route, "alpha", " Optimization "));
        Assert.False(ReadAdmission.IsServerHeldRead(Route, null, "optimization"));
        Assert.False(ReadAdmission.IsServerHeldRead(Route, "  ", "optimization"));
        Assert.False(ReadAdmission.IsServerHeldRead(Route, "alpha", "utilization"));
        Assert.False(ReadAdmission.IsServerHeldRead(Route, "alpha", null));
        Assert.False(ReadAdmission.IsServerHeldRead("get_finops_inventory", "alpha", "optimization"));
    }

    [Fact]
    public void TheOptimizationHold_IsFiveMinutes_WhateverTheBand()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ReadAdmission.ServerHeldTtl(Route, "optimization"));
        Assert.Null(ReadAdmission.ServerHeldTtl(Route, "utilization"));
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: false, "optimization", serverHeld: true));
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.None, fleetRead: false, "optimization", serverHeld: true));
        /* Not held: the band's TTL, as before. */
        Assert.Equal(ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: false), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: false, "utilization", serverHeld: false));
    }

    [Fact]
    public void IsCached_HoldsForTheOptimizationRead_OnBothSurfaces_AndItTakesNoSlot()
    {
        var admission = LightAdmission(new TimerTimeProvider());
        foreach (var surface in new[] { ReadSurface.Web, ReadSurface.Mcp })
        {
            Assert.True(admission.IsCached(surface, Route, ReadWindowBand.OneDay, fleetRead: false, serverHeld: true));
            Assert.False(admission.IsCached(surface, Route, ReadWindowBand.OneDay, fleetRead: false, serverHeld: false));
        }

        Assert.False(admission.IsHeavy(ReadSurface.Web, Route, ReadWindowBand.OneDay, out _));
    }

    /// <summary>The MCP call is held with a string server_name and the view; without a server, with another view, or with a server_name of
    /// another kind it is not.</summary>
    [Theory]
    [InlineData("{\"view\":\"optimization\",\"server_name\":\"alpha\"}", true)]
    [InlineData("{\"view\":\"optimization\",\"server_name\":\"alpha\",\"hours_back\":4,\"limit\":20}", true)]
    [InlineData("{\"view\":\"Optimization\",\"server_name\":\"alpha\"}", true)]
    [InlineData("{\"view\":\"optimization\"}", false)]
    [InlineData("{\"view\":\"optimization\",\"server_name\":\" \"}", false)]
    [InlineData("{\"view\":\"optimization\",\"server_name\":7}", false)]
    [InlineData("{\"view\":\"utilization\",\"server_name\":\"alpha\"}", false)]
    [InlineData("{\"server_name\":\"alpha\"}", false)]
    public void AnMcpOptimizationCall_IsHeld_WithAServerAndTheView(string arguments, bool expected)
    {
        var parsed = System.Text.Json.JsonSerializer.Deserialize<System.Collections.Generic.Dictionary<string, System.Text.Json.JsonElement>>(arguments)!;

        Assert.Equal(expected, PerformanceMonitor.Darling.Service.Mcp.McpToolLatencyFilter.IsServerHeldRead(Route, parsed));
        Assert.False(PerformanceMonitor.Darling.Service.Mcp.McpToolLatencyFilter.IsServerHeldRead("get_finops_inventory", parsed));
    }
}
