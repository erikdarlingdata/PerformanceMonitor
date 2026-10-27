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
using System.IO.Pipelines;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.AspNetCore.TestHost;
using System.ComponentModel;
using ModelContextProtocol.Client;
using ModelContextProtocol.Protocol;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6800: the fleet <c>get_resource_use_by_tag</c> read (no server) is kept in the read cache for 5 minutes when the window asked for is 24 hours or more
/// although its band is light, so a repeat visit, a second user and the page's pickers do not run its Query Store section again.
/// A window under 24 hours (including 2 to 23, which the 24h band covers) keeps the band's TTL. The web tests
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

    /// <summary>A web fleet read that runs for <paramref name="readTakes"/> on the test clock (the handler moves it), then reads at the
    /// offsets in <paramref name="advanceBefore"/>, each after the one before has answered. Returns how many times the read ran.</summary>
    private static async Task<int> RunTimedAsync(TimeSpan startAt, TimeSpan readTakes, TimeSpan[] advanceBefore)
    {
        var clock = new TimerTimeProvider();
        var admission = LightAdmission(clock);
        var calls = 0;
        using var entry = new ReadLatencyWebRecordingTests.ExtraDispatchEntryScope((Route, (_, _, _) =>
        {
            clock.Advance(readTakes);
            return Task.FromResult("{\"rows\":[" + Interlocked.Increment(ref calls).ToString(System.Globalization.CultureInfo.InvariantCulture) + "]}");
        }));
        var (app, postgres) = await ReadCacheWebEndToEndTests.BuildServer(admission, new ReadLatencyAccumulator());
        await using var _app = app;
        await using var _postgres = postgres;
        using var client = app.GetTestServer().CreateClient();

        clock.Advance(startAt);
        await client.GetStringAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        foreach (var advance in advanceBefore)
        {
            clock.Advance(advance);
            await client.GetStringAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        }

        return calls;
    }

    [Fact(Timeout = 60_000)]
    public async Task AFleetRead_StartedLateInAFiveMinuteSpan_IsStillAHit90SecondsAfterItIsStored()
    {
        /* The test clock starts on a 5-minute boundary. The read starts at +4:00, takes 61 s (as the cold read did on a 42-server store),
           so it is stored at +5:01, past that boundary. A read 90 s later (+6:31) is inside the 5 minutes from the store. A wall-clock
           bucket key stored the answer under the +0:00 bucket, which no request after +5:00 could reach. */
        Assert.Equal(1, await RunTimedAsync(TimeSpan.FromMinutes(4), TimeSpan.FromSeconds(61), [TimeSpan.FromSeconds(90)]));
    }

    [Fact(Timeout = 60_000)]
    public async Task AFleetRead_IsAMiss_OnceFiveMinutesHavePassedSinceItWasStored()
    {
        /* Stored at +5:01. A hit at +9:59 (4:58 after the store, inside the TTL), then a miss at +10:02 (5:01 after the store). */
        Assert.Equal(1, await RunTimedAsync(TimeSpan.FromMinutes(4), TimeSpan.FromSeconds(61), [TimeSpan.FromSeconds(298)]));
        Assert.Equal(2, await RunTimedAsync(TimeSpan.FromMinutes(4), TimeSpan.FromSeconds(61), [TimeSpan.FromSeconds(298), TimeSpan.FromSeconds(3)]));
    }

    [Fact(Timeout = 60_000)]
    public async Task AFleetRead_IsDroppedByAWrite_ThoughItsKeyHasNoBucket()
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

        await client.GetStringAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        await client.GetStringAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        Assert.Equal(1, calls);
        admission.Cache.BumpGeneration();
        await client.GetStringAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        Assert.Equal(2, calls);
    }

    [Theory(Timeout = 60_000)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(12)]
    [InlineData(23)]
    public async Task AFleetRead_OfAWindowUnder24Hours_IsNotKeptForFiveMinutes(int hours)
    {
        /* hours=4 is the page's own Past 4 hours preset, in the 24h band. Four minutes apart is inside the 5 minute TTL and past the
           band's 15 and 30 seconds, so a call total of 2 says the long TTL was not applied. */
        var path = $"/api/read/{Route}?hours={hours}";
        var (calls, _) = await RunAsync([path, path], [TimeSpan.Zero, TimeSpan.FromMinutes(4)]);

        Assert.Equal(2, calls);
    }

    [Fact(Timeout = 60_000)]
    public async Task AFleetRead_OfAWindowUnder24Hours_KeepsTheBandRule_NotCachedWhenTheBandIsLight()
    {
        /* Two identical reads at the same instant: a cached read would run once. A light band is not cached, as before #6800. */
        var (calls, _) = await RunAsync(
            [$"/api/read/{Route}?hours=4", $"/api/read/{Route}?hours=4", $"/api/read/{Route}?hours=1&server_name=", $"/api/read/{Route}?hours=1&server_name="],
            [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero]);

        Assert.Equal(4, calls);
    }

    [Theory(Timeout = 60_000)]
    [InlineData("hours=24")]
    [InlineData("hours=168")]
    [InlineData("hours=720")]
    [InlineData("")]
    public async Task AFleetRead_OfAWindowOf24HoursOrMore_IsKeptForFiveMinutes(string query)
    {
        /* The last row names no window: the route's declared default is 24 hours. */
        var path = $"/api/read/{Route}" + (query.Length == 0 ? "" : "?" + query);
        var (calls, _) = await RunAsync([path, path, path], [TimeSpan.Zero, TimeSpan.FromMinutes(4), TimeSpan.FromMinutes(6)]);

        Assert.Equal(2, calls);
    }

    [Fact(Timeout = 60_000)]
    public async Task AOneServerRead_IsNeverKept_WhichArgumentNamesTheServer()
    {
        /* Two reads 4 minutes apart: a fleet entry would still be inside its 5 minutes, so a kept one-server read shows as one call. */
        foreach (var query in new[] { "server=a", "server_name=a" })
        {
            var (calls, _) = await RunAsync(
                [$"/api/read/{Route}?hours=24&{query}", $"/api/read/{Route}?hours=24&{query}"],
                [TimeSpan.Zero, TimeSpan.FromMinutes(4)]);

            Assert.Equal(2, calls);
        }
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
    public async Task ALightPair_TakesNoSlot_WhileEverySlotIsHeld()
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

        /* The only slot is held by a heavy read of another route (a 7-day band is heavy by its prior). The pair has no learned p90 here,
           so it is light and takes no slot; once its p90 is over the threshold it is heavy and takes one like any long read (the page
           retries a busy answer once, QueryTagUsageBehaviourTests). Caching it does not change which it is. */
        using var held = await Task.Run(() => admission.AdmitAsync(ReadSurface.Mcp, "get_other", ReadWindowBand.SevenDays, TestContext.Current.CancellationToken));
        Assert.Equal(1, admission.Stats().Running);

        var response = await client.GetAsync($"/api/read/{Route}?hours=24", TestContext.Current.CancellationToken);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(1, calls);
    }

    [Theory]
    [InlineData(24, true)]
    [InlineData(168, true)]
    [InlineData(23.9, false)]
    [InlineData(23, false)]
    [InlineData(4, false)]
    [InlineData(1, false)]
    [InlineData(0, false)]
    public void IsFleetRead_NeedsAWindowOf24HoursOrMore_WhateverTheBand(double hours, bool expected)
    {
        Assert.Equal(expected, ReadAdmission.IsFleetRead(Route, null, hours));
        Assert.False(ReadAdmission.IsFleetRead(Route, "a", hours));
        Assert.False(ReadAdmission.IsFleetRead("get_top_queries_by_cpu", null, hours));
        Assert.False(ReadAdmission.IsFleetRead(Route, null, null));
    }

    [Fact]
    public void TheFleetTtl_IsFiveMinutesForAFleetRead_AndTheBandTtlForAnythingElse()
    {
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: true));
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.SevenDays, fleetRead: true));
        Assert.Equal(TimeSpan.FromMinutes(5), ReadResultCache.TtlFor(Route, ReadWindowBand.Longer, fleetRead: true));
        Assert.Equal(TimeSpan.FromSeconds(30), ReadResultCache.TtlFor(Route, ReadWindowBand.OneDay, fleetRead: false));
        Assert.Equal(TimeSpan.FromSeconds(15), ReadResultCache.TtlFor(Route, ReadWindowBand.OneHour, fleetRead: false));
        Assert.Equal(TimeSpan.FromSeconds(30), ReadResultCache.TtlFor("get_top_queries_by_cpu", ReadWindowBand.OneDay, fleetRead: true));
    }

    [Fact]
    public void IsCached_HoldsForAFleetRead_ButNotForAOneServerReadOrAnotherRoute()
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

    private static McpToolParameterTypes RealTypes()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpQueryTagRollupTools>();
        using var provider = services.BuildServiceProvider();
        _ = provider.GetServices<McpServerTool>().ToList();
        return provider.GetRequiredService<McpToolParameterTypes>();
    }

    [Theory]
    [InlineData("{}", true)]
    [InlineData("{\"server_name\":null}", true)]
    [InlineData("{\"server_name\":\"\"}", true)]
    [InlineData("{\"server_name\":\"  \"}", true)]
    [InlineData("{\"hours_back\":24}", true)]
    [InlineData("{\"hours_back\":168}", true)]
    [InlineData("{\"hours_back\":4}", false)]
    [InlineData("{\"hours_back\":23}", false)]
    [InlineData("{\"hours_back\":1}", false)]
    [InlineData("{\"hours_back\":168,\"server_name\":\"a\"}", false)]
    [InlineData("{\"server_name\":\"a\"}", false)]
    [InlineData("{\"server_name\":7}", false)]
    public void AnMcpCall_IsAFleetRead_WhenItNamesNoServerAndAsksFor24HoursOrMore(string arguments, bool expected)
    {
        var parsed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(arguments)!;
        var types = RealTypes();

        /* A call that leaves hours_back out reads the tool's declared default, 24. */
        Assert.Equal(expected, McpToolLatencyFilter.IsFleetRead(Route, parsed, types));
        Assert.False(McpToolLatencyFilter.IsFleetRead("get_top_queries_by_cpu", parsed, types));
    }

    [Fact]
    public void AnMcpCall_WithAnAsOfAnchor_IsMeasuredBySpan_NotByAnchor()
    {
        var types = RealTypes();
        var shortSpan = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>("{\"hours_back\":4,\"as_of\":\"2026-01-01T00:00:00Z\"}")!;
        var longSpan = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>("{\"hours_back\":48,\"as_of\":\"2026-01-01T00:00:00Z\"}")!;

        Assert.False(McpToolLatencyFilter.IsFleetRead(Route, shortSpan, types));
        Assert.True(McpToolLatencyFilter.IsFleetRead(Route, longSpan, types));
    }

    /// <summary>A stand-in for the real tool: same name and the same two parameters, counts its runs, answers a small ok envelope.</summary>
    [McpServerToolType]
    public sealed class CountingTagTool
    {
        public static int Runs;

        /// <summary>Runs inside each call before it answers (a test moves its clock here, as a slow read would).</summary>
        public static Action? OnRun;

        [McpServerTool(Name = "get_resource_use_by_tag"), Description("counts")]
        public static string Run(string? server_name = null, int hours_back = 24)
        {
            OnRun?.Invoke();
            return "{\"rows\":[" + Interlocked.Increment(ref Runs).ToString(System.Globalization.CultureInfo.InvariantCulture) + "]}";
        }
    }

    private static async Task<int> CallThroughTheFilterAsync(int[] hoursBack, TimeSpan[] advanceBefore, TimeSpan readTakes = default)
    {
        CountingTagTool.Runs = 0;
        var clock = new TimerTimeProvider();
        CountingTagTool.OnRun = readTakes > TimeSpan.Zero ? () => clock.Advance(readTakes) : null;
        var admission = LightAdmission(clock);
        var filter = new McpToolLatencyFilter(new ReadLatencyAccumulator(), null, null, admission);
        var clientToServer = new Pipe();
        var serverToClient = new Pipe();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddMcpServer()
            .WithStreamServerTransport(clientToServer.Reader.AsStream(), serverToClient.Writer.AsStream())
            .WithRequestFilters(filters => filters.AddCallToolFilter(filter.AsFilter()))
            .WithGeminiCompatibleTools<CountingTagTool>();
        await using var provider = services.BuildServiceProvider();
        using var stop = new CancellationTokenSource();
        var run = provider.GetRequiredService<McpServer>().RunAsync(stop.Token);
        await using var client = await McpClient.CreateAsync(
            new StreamClientTransport(clientToServer.Writer.AsStream(), serverToClient.Reader.AsStream()),
            cancellationToken: TestContext.Current.CancellationToken);
        try
        {
            for (var i = 0; i < hoursBack.Length; i++)
            {
                clock.Advance(advanceBefore[i]);
                var result = await client.CallToolAsync(Route,
                    new Dictionary<string, object?> { ["hours_back"] = hoursBack[i] },
                    cancellationToken: TestContext.Current.CancellationToken);
                Assert.NotEqual(true, result.IsError);
            }
        }
        finally
        {
            CountingTagTool.OnRun = null;
            await stop.CancelAsync();
            try { await run; } catch (OperationCanceledException) { }
        }

        return CountingTagTool.Runs;
    }

    [Fact(Timeout = 60_000)]
    public async Task AnMcpFleetCall_At24Hours_RunsTheToolOnceInsideFiveMinutes()
    {
        Assert.Equal(1, await CallThroughTheFilterAsync([24, 24], [TimeSpan.Zero, TimeSpan.FromMinutes(4)]));
    }

    [Fact(Timeout = 60_000)]
    public async Task AnMcpFleetCall_StartedLateInAFiveMinuteSpan_IsStillAHit90SecondsAfterItIsStored()
    {
        /* Called at +4:00, the tool takes 61 s, so the answer is stored at +5:01. A call at +6:31 is 90 s after the store: a hit. */
        Assert.Equal(1, await CallThroughTheFilterAsync([24, 24], [TimeSpan.FromMinutes(4), TimeSpan.FromSeconds(90)], TimeSpan.FromSeconds(61)));
        /* Two calls after the first: +9:59 is a hit (4:58 after the store), +10:02 a miss (5:01 after it). */
        Assert.Equal(2, await CallThroughTheFilterAsync([24, 24, 24],
            [TimeSpan.FromMinutes(4), TimeSpan.FromSeconds(298), TimeSpan.FromSeconds(3)], TimeSpan.FromSeconds(61)));
    }

    [Fact(Timeout = 60_000)]
    public async Task AnMcpFleetCall_At4Hours_IsNotKeptForFiveMinutes()
    {
        Assert.Equal(2, await CallThroughTheFilterAsync([4, 4], [TimeSpan.Zero, TimeSpan.FromMinutes(4)]));
    }
}
