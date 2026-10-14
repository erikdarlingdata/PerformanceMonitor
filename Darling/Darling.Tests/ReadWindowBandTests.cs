/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using PerformanceMonitor.Darling.Service;
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
    public void OfToolCall_UsesTheSentArgument_ElseTheToolsDeclaredDefault_ElseNone()
    {
        using var schema = JsonDocument.Parse("""{"type":"object","properties":{"hours_back":{"type":"integer","default":24},"as_of":{"type":"string"}}}""");
        using var noWindow = JsonDocument.Parse("""{"type":"object","properties":{"server_name":{"type":"string"}}}""");
        using var sent = JsonDocument.Parse("""{"hours_back":336}""");

        var sentArguments = new Dictionary<string, JsonElement> { ["hours_back"] = sent.RootElement.GetProperty("hours_back") };
        Assert.Equal("7d+", ReadWindowBand.OfToolCall(sentArguments, schema.RootElement));
        Assert.Equal("24h", ReadWindowBand.OfToolCall(new Dictionary<string, JsonElement>(), schema.RootElement));
        Assert.Equal("24h", ReadWindowBand.OfToolCall(null, schema.RootElement));
        Assert.Equal("none", ReadWindowBand.OfToolCall(null, noWindow.RootElement));
        Assert.Equal("none", ReadWindowBand.OfToolCall(null, null));
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
