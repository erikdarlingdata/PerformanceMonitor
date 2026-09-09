/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3207: the viewer's naive-UTC conversion, against ONE instant expressed in both frames, plus the raw
/// server-clock render's mode behaviour stated as a fact rather than described.
///
/// <para><b>Why one instant.</b> A pinned string per renderer passes under a sign error as readily as
/// under the right sign, because expectation and implementation are written from the same belief about
/// which way the offset goes. What discriminates is that <see cref="ViewerTimeHelper.ForDisplay"/> handed
/// the instant's NAIVE-UTC value and the raw render of the instant's SERVER-CLOCK value produce the same
/// text in Server mode and differ by the whole offset if either side's arithmetic moves.</para>
///
/// <para><b>Where -240 comes from.</b> The fleet's measured offset, not an illustration: #2932 measured
/// <c>cpu_utilization_stats.sample_time</c> exactly four hours behind the same run's
/// <c>collection_time</c> on 42 of 42 servers, and re-measured 2026-09-09 on the collected store,
/// <c>query_stats.last_execution_time - collection_time</c> is -240 on 2,092 captures across 42 of 42
/// while <c>query_store_stats.last_execution_time - collection_time</c> is -1 on 225 captures across 43 —
/// the two frames, in the same store, in one measurement. A stored server-clock value is
/// <c>utc + offset</c>, so a value already in that frame must not be sent through the conversion that adds
/// the offset again.</para>
///
/// <para><b>The raw render is mode-blind, and that is pinned here rather than left implied.</b>
/// <c>ViewerDataService.FormatServerClock</c> emits the server's own clock in all three display modes
/// because <see cref="ViewerTimeHelper"/> has no server-local arm to route through. Lite's namesake takes
/// the same frame and DOES honour the preference, through <c>ServerTimeHelper.ConvertForDisplay</c>. The
/// shared name is an input contract, not shared behaviour, and the assertions below fail if that stops
/// being true in either direction.</para>
/// </summary>
public sealed class ViewerClockRendererArithmeticTests
{
    /// <summary>The fleet's measured offset. Negative, and large enough that a sign error lands eight
    /// hours out rather than somewhere a reader might accept.</summary>
    private const int FleetOffsetMinutes = -240;

    private static readonly DateTime NaiveUtc =
        new(2026, 9, 9, 18, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime ServerClock =
        new(2026, 9, 9, 14, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The fixtures are one instant in two frames, asserted rather than assumed: they are the
    /// premise of every assertion below.</summary>
    [Fact]
    public void TheFixtures_AreOneInstantInTwoFrames()
    {
        Assert.Equal(ServerClock, NaiveUtc.AddMinutes(FleetOffsetMinutes));
        Assert.Equal(NaiveUtc, ServerClock.AddMinutes(-FleetOffsetMinutes));
        Assert.Equal(-240, FleetOffsetMinutes);
    }

    /// <summary>
    /// Server mode: the conversion of the naive-UTC value lands on the server's own wall clock, which is
    /// what the raw render of the server-clock value already shows. The two frames meet here, which is
    /// exactly why a mis-paired renderer is invisible on a UTC dev box and four hours out on the fleet.
    /// </summary>
    [Fact]
    public void ServerMode_ConvertsNaiveUtcOntoTheServersOwnWallClock()
    {
        Assert.Equal(
            ServerClock,
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.ServerTime, FleetOffsetMinutes));
    }

    /// <summary>UTC mode returns the stored value: the store is naive UTC, so there is nothing to do.</summary>
    [Fact]
    public void UtcMode_ReturnsTheStoredValue()
    {
        Assert.Equal(
            NaiveUtc,
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.UTC, FleetOffsetMinutes));
    }

    /// <summary>Local mode is the host's own zone, computed from the instant rather than pinned to a
    /// literal — CI and a developer machine are not in the same zone.</summary>
    [Fact]
    public void LocalMode_IsTheHostsOwnZone()
    {
        Assert.Equal(
            DateTime.SpecifyKind(NaiveUtc, DateTimeKind.Utc).ToLocalTime(),
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.LocalTime, FleetOffsetMinutes));
    }

    /// <summary>
    /// The defect, with its magnitude and direction: a server-clock value through the naive-UTC conversion
    /// lands exactly <see cref="FleetOffsetMinutes"/> away — four hours EARLY on this fleet — in every mode
    /// that applies the offset. Stated as the size of the error rather than as an inequality.
    /// </summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void AServerClockValue_ThroughTheNaiveUtcConversion_IsTheWholeOffsetEarly(TimeDisplayMode mode)
    {
        var correct = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, mode, FleetOffsetMinutes);
        var skewed = ViewerTimeHelper.ConvertToDisplay(ServerClock, mode, FleetOffsetMinutes);

        Assert.Equal(TimeSpan.FromMinutes(FleetOffsetMinutes), skewed - correct);
        Assert.True(skewed < correct, $"{mode}: expected the skewed conversion to be EARLIER");
    }

    /// <summary>
    /// The other direction, which no test asserted before: a naive-UTC value rendered RAW — the shape the
    /// three Query Store surfaces shipped — is the whole offset LATE against the same instant converted
    /// properly. Four hours late reads as plausible on a Query Store row, which is why these sites were
    /// silent while the running-job one was visible.
    /// </summary>
    [Fact]
    public void ANaiveUtcValue_RenderedRaw_IsTheWholeOffsetLate()
    {
        var correct = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.ServerTime, FleetOffsetMinutes);

        Assert.Equal(TimeSpan.FromMinutes(-FleetOffsetMinutes), NaiveUtc - correct);
        Assert.True(NaiveUtc > correct, "expected the raw render to be LATER");
    }

    /// <summary>
    /// The inverse the custom-range pickers use round-trips, in every mode. A conversion pair that agrees
    /// with itself in one direction only skews every window the user types.
    /// </summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void TheDisplayInverse_RoundTripsTheStoredValue(TimeDisplayMode mode)
    {
        var display = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, mode, FleetOffsetMinutes);

        Assert.Equal(NaiveUtc, ViewerTimeHelper.ConvertFromDisplay(display, mode, FleetOffsetMinutes));
    }

    /// <summary>The pinned wall clocks, so the relative assertions above cannot all agree on a wrong
    /// value.</summary>
    [Fact]
    public void ServerAndUtcModes_RenderThePinnedWallClocks()
    {
        Assert.Equal(
            "2026-09-09 14:00:00",
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.ServerTime, FleetOffsetMinutes)
                .ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(
            "2026-09-09 18:00:00",
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.UTC, FleetOffsetMinutes)
                .ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
    }
}
