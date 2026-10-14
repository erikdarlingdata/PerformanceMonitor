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
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the window band a read is recorded under (#5506): the boundaries (each band includes its upper edge), a read with no
/// window, the argument names each surface sends, an MCP call that leaves its window to the tool's declared default, and a
/// composed panel's start and end pair.
/// </summary>
public sealed class ReadWindowBandTests
{
    [Theory]
    [InlineData(null, "none")]
    [InlineData(0.0, "none")]
    [InlineData(-3.0, "none")]
    [InlineData(double.NaN, "none")]
    [InlineData(0.25, "1h")]
    [InlineData(1.0, "1h")]
    [InlineData(1.0001, "24h")]
    [InlineData(24.0, "24h")]
    [InlineData(24.5, "7d")]
    [InlineData(168.0, "7d")]
    [InlineData(168.01, "7d+")]
    [InlineData(2160.0, "7d+")]
    public void Of_Hours_PutsEachBoundaryInTheLowerBand(double? hours, string expected) =>
        Assert.Equal(expected, ReadWindowBand.Of(hours));

    [Fact]
    public void Of_StartAndEnd_UsesTheirSpan_AndNoneWhenEitherIsMissing()
    {
        var end = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        Assert.Equal("1h", ReadWindowBand.Of(end.AddHours(-1), end));
        Assert.Equal("24h", ReadWindowBand.Of(end.AddDays(-1), end));
        Assert.Equal("7d", ReadWindowBand.Of(end.AddDays(-7), end));
        Assert.Equal("7d+", ReadWindowBand.Of(end.AddDays(-30), end));
        Assert.Equal("none", ReadWindowBand.Of(null, end));
        Assert.Equal("none", ReadWindowBand.Of(end, null));
    }

    [Fact]
    public void HoursOf_ReadsHoursThenHoursBackThenDaysBack()
    {
        Assert.Equal(6, ReadWindowBand.HoursOf(k => k == "hours" ? "6" : k == "hours_back" ? "48" : null));
        Assert.Equal(48, ReadWindowBand.HoursOf(k => k == "hours_back" ? "48" : null));
        Assert.Equal(72, ReadWindowBand.HoursOf(k => k == "days_back" ? "3" : null));
        Assert.Null(ReadWindowBand.HoursOf(k => k == "as_of" ? "2026-10-01T00:00:00Z" : null));
        Assert.Null(ReadWindowBand.HoursOf(k => k == "hours" ? "abc" : null));
    }

    [Fact]
    public void HoursOf_TakesADeclaredDefault_OnlyWhenNoWindowWasSent()
    {
        Assert.Equal(48, ReadWindowBand.HoursOf(k => k == "hours_back" ? "48" : null, k => k == "hours" ? "24" : null));
        Assert.Equal(24, ReadWindowBand.HoursOf(_ => null, k => k == "hours" ? "24" : null));
        Assert.Equal(720, ReadWindowBand.HoursOf(_ => null, k => k == "days_back" ? "30" : null));
        Assert.Null(ReadWindowBand.HoursOf(_ => null, _ => null));
    }

    /// <summary>The MCP path against a really registered tool. The schema the host serves has its <c>default</c> keywords
    /// stripped for Gemini, so a band read from it was <c>none</c> for a call that leaves <c>hours_back</c> out; the band
    /// now comes from the tool method's own parameter default, recorded as the tool is created.</summary>
    [Fact]
    public void AnMcpCallThatLeavesItsWindowOut_RecordsTheBandOfTheToolsOwnDefault_NotNone()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpDataTools>();
        using var provider = services.BuildServiceProvider();
        var waitStats = provider.GetServices<McpServerTool>().Single(t => t.ProtocolTool.Name == "get_wait_stats");
        var types = provider.GetRequiredService<McpToolParameterTypes>();

        /* The served schema carries no default to read: the band cannot come from there. */
        var hoursBack = waitStats.ProtocolTool.InputSchema.GetProperty("properties").GetProperty("hours_back");
        Assert.False(hoursBack.TryGetProperty("default", out _));

        /* get_wait_stats declares hours_back = 24: a call that sends only server_name reads 24 hours. */
        using var onlyServer = JsonDocument.Parse("""{"server_name":"SRV1"}""");
        var serverOnly = new Dictionary<string, JsonElement> { ["server_name"] = onlyServer.RootElement.GetProperty("server_name") };
        Assert.Equal("24h", McpToolLatencyFilter.WindowBandOf("get_wait_stats", serverOnly, types));
        Assert.Equal("24h", McpToolLatencyFilter.WindowBandOf("get_wait_stats", null, types));

        /* A sent window wins over the default, and a sent null is the same as leaving it out. */
        using var sent = JsonDocument.Parse("""{"hours_back":336,"nothing":null}""");
        Assert.Equal("7d+", McpToolLatencyFilter.WindowBandOf("get_wait_stats",
            new Dictionary<string, JsonElement> { ["hours_back"] = sent.RootElement.GetProperty("hours_back") }, types));
        Assert.Equal("24h", McpToolLatencyFilter.WindowBandOf("get_wait_stats",
            new Dictionary<string, JsonElement> { ["hours_back"] = sent.RootElement.GetProperty("nothing") }, types));

        /* A tool nothing recorded, or no record at all, has no default to take. */
        Assert.Equal("none", McpToolLatencyFilter.WindowBandOf("not_a_tool", null, types));
        Assert.Equal("none", McpToolLatencyFilter.WindowBandOf("get_wait_stats", null, null));
    }

    /// <summary>The web path: a request that sends no window reads the default its catalog row declares, so it records that
    /// default's band, and a read that takes no window records none.</summary>
    [Fact]
    public void AWebReadThatSendsNoWindow_RecordsTheBandOfTheCatalogDefault()
    {
        var latency = DarlingWebEndpoints.CatalogDescriptors["get_read_latency"].Params.Single(p => p.Name == "hours");
        Assert.Equal(ReadWindowBand.Of(Convert.ToDouble(latency.Default, CultureInfo.InvariantCulture)),
            DarlingWebEndpoints.WebWindowBand("get_read_latency", _ => null));
        Assert.Equal("1h", DarlingWebEndpoints.WebWindowBand("get_read_latency", k => k == "hours" ? "1" : null));
        Assert.Equal("1h", DarlingWebEndpoints.WebWindowBand("get_read_latency", k => k == "hours_back" ? "1" : null));

        var daysBack = DarlingWebEndpoints.CatalogDescriptors["get_daily_summary_range"].Params.Single(p => p.Name == "days_back");
        Assert.Equal(ReadWindowBand.Of(24 * Convert.ToDouble(daysBack.Default, CultureInfo.InvariantCulture)),
            DarlingWebEndpoints.WebWindowBand("get_daily_summary_range", _ => null));
        Assert.NotEqual("none", DarlingWebEndpoints.WebWindowBand("get_daily_summary_range", _ => null));

        var noWindow = DarlingWebEndpoints.CatalogDescriptors.First(d =>
            !d.Value.Params.Any(p => p.Name is "hours" or "hours_back" or "days_back")).Key;
        Assert.Equal("none", DarlingWebEndpoints.WebWindowBand(noWindow, _ => null));
        Assert.Equal("none", DarlingWebEndpoints.WebWindowBand("not_a_read", _ => null));
    }

    [Fact]
    public void ComposeWindowBand_UsesTheStartEndPair_ElseHours_ElseTheRunnersDefault()
    {
        Assert.Equal("7d", DarlingWebEndpoints.ComposeWindowBand(new JsonObject
        {
            ["windowStart"] = "2026-10-01T00:00:00Z",
            ["windowEnd"] = "2026-10-05T00:00:00Z",
        }));
        Assert.Equal("1h", DarlingWebEndpoints.ComposeWindowBand(new JsonObject { ["hours"] = 1 }));
        Assert.Equal(ReadWindowBand.Of(DarlingWebEndpoints.DefaultComposeHours), DarlingWebEndpoints.ComposeWindowBand(new JsonObject()));
    }
}

/// <summary>
/// Pins the in-flight count and the connection wait each read carries (#5506): a nested scope is one read (it carries its
/// outer scope's count and adds its wait to the outer scope too), a pooled wait is measured from the pending start to its
/// stop, and a connection created inside a pending wait is not counted twice.
/// </summary>
public sealed class ReadScopeCapacityTests
{
    [Fact]
    public void ANestedScope_CarriesTheOuterCount_AndItsWaitReachesTheOuterScope()
    {
        using var outer = ReadScope.Open(null);
        Assert.True(outer.Scope.InFlightAtStart >= 0);
        Assert.True(ReadScope.InFlight >= 1);
        using (var inner = ReadScope.Open(null))
        {
            Assert.Equal(outer.Scope.InFlightAtStart, inner.Scope.InFlightAtStart);
            inner.Scope.AddWait(System.Diagnostics.Stopwatch.Frequency / 10);
            Assert.InRange(inner.Scope.WaitMs, 99, 101);
        }

        Assert.InRange(outer.Scope.WaitMs, 99, 101);
    }

    [Fact]
    public void OnPending_MeasuresTheWaitInsideAScope_AndACreateInsideItIsNotAddedTwice()
    {
        using var scope = ReadScope.Open(null);
        ReadConnectionWait.OnPending(1);
        ReadConnectionWait.OnCreateSeconds(5.0);
        Thread.Sleep(60);
        ReadConnectionWait.OnPending(-1);
        Assert.InRange(scope.Scope.WaitMs, 50, 4_000);

        var afterPending = scope.Scope.WaitMs;
        ReadConnectionWait.OnCreateSeconds(0.2);
        Assert.InRange(scope.Scope.WaitMs - afterPending, 199, 201);
    }

    [Fact]
    public void OutsideAScope_NothingIsRecorded_AndNothingThrows()
    {
        ReadConnectionWait.OnPending(1);
        ReadConnectionWait.OnPending(-1);
        ReadConnectionWait.OnCreateSeconds(1.0);
        Assert.Null(ReadScope.Current);
    }
}
