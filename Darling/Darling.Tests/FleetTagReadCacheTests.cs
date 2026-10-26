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
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.TestHost;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800: the fleet <c>get_resource_use_by_tag</c> read (no server) is kept in the read cache for 5 minutes although its 24-hour
/// band is light, so a repeat visit, a second user and the page's pickers do not run its Query Store section again. The web tests
/// run the real <c>/api/read</c> pipeline with a counting handler standing in for the store under the route's own name, on a clock
/// the test moves.
/// </summary>
public sealed class FleetTagReadCacheTests
{
    private const string Route = "get_resource_use_by_tag";

    /// <summary>An admission with no learned history: the 24-hour band is light, as it is on a fresh process.</summary>
    private static ReadAdmission LightAdmission(TimerTimeProvider clock) =>
        new(clock, ReadAdmission.DefaultWaitCap, null, 2);

    private static async Task<(int Calls, string[] Bodies)> RunAsync(string[] paths, TimeSpan[] advanceBefore)
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
            bodies[i] = await client.GetStringAsync(paths[i], TestContext.Current.CancellationToken);
        }

        return (calls, bodies);
    }

    [Fact(Timeout = 60_000)]
    public async Task TwoFleetReads_InsideTheTtl_RunTheReaderOnce()
    {
        var (calls, bodies) = await RunAsync(
            [$"/api/read/{Route}?hours=24", $"/api/read/{Route}?hours=24", $"/api/read/{Route}?hours=24"],
            [TimeSpan.Zero, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(2)]);

        Assert.Equal(1, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.Equal(bodies[0], bodies[2]);
    }

    [Fact(Timeout = 60_000)]
    public async Task AFleetRead_IsKeptPastTheBandsThirtySeconds_ButNotPastTheFiveMinuteTtl()
    {
        /* The test clock starts on a 5-minute boundary: 4 minutes later is a hit (the 24-hour band alone keeps 30 s), 6 is a miss. */
        var (calls, bodies) = await RunAsync(
            [$"/api/read/{Route}?hours=24", $"/api/read/{Route}?hours=24", $"/api/read/{Route}?hours=24"],
            [TimeSpan.Zero, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(2)]);

        Assert.Equal(2, calls);
        Assert.Equal(bodies[0], bodies[1]);
        Assert.NotEqual(bodies[0], bodies[2]);
    }

    [Fact(Timeout = 60_000)]
    public async Task AOneServerRead_IsNotCached_AndDoesNotShareAnEntryWithTheFleetRead()
    {
        var (calls, _) = await RunAsync(
            [$"/api/read/{Route}?hours=24&server=a", $"/api/read/{Route}?hours=24&server=a", $"/api/read/{Route}?hours=24&server_name=a",
             $"/api/read/{Route}?hours=24", $"/api/read/{Route}?hours=24"],
            [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        /* Three one-server reads run three times; the two fleet reads run once. */
        Assert.Equal(4, calls);
    }

    [Fact(Timeout = 60_000)]
    public async Task TwoFleetReads_ThatDifferInTheirArguments_AreTwoEntries()
    {
        var (calls, _) = await RunAsync(
            [$"/api/read/{Route}?hours=24", $"/api/read/{Route}?hours=24&tag=7", $"/api/read/{Route}?hours=24&as_of=2026-01-01T00:00:00Z"],
            [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        Assert.Equal(3, calls);
    }

    [Fact(Timeout = 60_000)]
    public async Task AFleetRead_TakesNoSlot_SoItNeverAnswersBusy_WhileEverySlotIsHeld()
    {
        var clock = new TimerTimeProvider();
        var admission = new ReadAdmission(clock, ReadAdmission.DefaultWaitCap, null, 1);
        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((Route, (_, _, _) =>
            Task.FromResult("{\"rows\":[" + Interlocked.Increment(ref calls).ToString(System.Globalization.CultureInfo.InvariantCulture) + "]}")));
        var (app, postgres) = await ReadCacheWebEndToEndTests.BuildServer(admission, new ReadLatencyAccumulator());
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();

        /* The only slot is held by a heavy read of another route (a 7-day band is heavy by its prior). */
        using var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Mcp, "get_other", ReadWindowBand.SevenDays, TestContext.Current.CancellationToken));
        Assert.Equal(1, admission.Stats().Running);

        var response = await client.GetAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, calls);
    }

    [Fact]
    public void TheFleetTtl_IsFiveMinutes_AndAnotherRouteKeepsItsBandTtl()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: true));
        Assert.Equal(TimeSpan.FromSeconds(30), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: false));
        Assert.Equal(TimeSpan.FromSeconds(30), ReadResultCache.TtlFor("get_top_queries_by_cpu", ReadWindowBand.OneDay, fleetRead: true));
    }

    [Fact]
    public void IsCached_HoldsForAFleetReadInAnyBand_ButNotForAOneServerReadOrAnotherRoute()
    {
        var admission = LightAdmission(new TimerTimeProvider());
        foreach (var surface in new[] { ReadSurface.Web, ReadSurface.Mcp })
        {
            foreach (var band in new[] { ReadWindowBand.None, ReadWindowBand.OneHour, ReadWindowBand.OneDay })
            {
                Assert.True(admission.IsCached(surface, Route, band, fleetRead: true));
                Assert.False(admission.IsCached(surface, Route, band, fleetRead: false));
                Assert.False(admission.IsCached(surface, "get_top_queries_by_cpu", band, fleetRead: true));
            }
        }

        /* And it stays light: the fleet read takes no slot. */
        Assert.False(admission.IsHeavy(ReadSurface.Mcp, Route, ReadWindowBand.OneDay, out _));
    }

    [Theory]
    [InlineData("{}", true)]
    [InlineData("{\"server_name\":null}", true)]
    [InlineData("{\"server_name\":\"\"}", true)]
    [InlineData("{\"server_name\":\"  \"}", true)]
    [InlineData("{\"hours_back\":24}", true)]
    [InlineData("{\"server_name\":\"a\"}", false)]
    [InlineData("{\"server_name\":7}", false)]
    public void AnMcpCall_IsAFleetRead_WhenItNamesNoServer(string arguments, bool expected)
    {
        var parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(arguments)!;
        Assert.Equal(expected, McpToolLatencyFilter.IsFleetRead(Route, parsed));
        Assert.False(McpToolLatencyFilter.IsFleetRead("get_top_queries_by_cpu", parsed));
    }
}
