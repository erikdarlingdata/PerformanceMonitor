/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3207 / #3221: the two renderers' arithmetic, against ONE instant expressed in both frames.
///
/// <para><b>Why one instant and not two pinned strings.</b> A pinned string per renderer passes under a
/// sign error as readily as under the right sign, because the expectation and the implementation are
/// written from the same belief about which way the offset goes. The load-bearing assertion here is that
/// <see cref="ServerTimeHelper.FormatServerTime"/> handed the instant's NAIVE-UTC value and
/// <see cref="ServerTimeHelper.FormatServerClock"/> handed the instant's SERVER-CLOCK value produce the
/// same text, in every display mode. That holds only if exactly one of the two adds the offset, in the
/// direction the store's data actually has it — an implementation that adds it in both, in neither, or with
/// the sign flipped disagrees by 240 or 480 minutes. The absolute renderings are pinned as well, so the
/// pair cannot agree on a jointly wrong value.</para>
///
/// <para><b>Where -240 comes from.</b> It is the fleet's measured offset, not an illustration. #2932
/// measured <c>cpu_utilization_stats.sample_time</c> exactly four hours behind the same run's
/// <c>collection_time</c> on 42 of 42 servers; re-measured 2026-09-09 on the collected store,
/// <c>query_stats.last_execution_time - collection_time</c> is -240 on 2,092 captures across 42 of 42, and
/// <c>blocked_process_reports</c> carries <c>blocked_last_tran_started</c> -240 from the XE
/// <c>event_time</c> in the SAME ROW. A stored server-clock value is therefore <c>utc + offset</c>, so
/// recovering UTC SUBTRACTS the offset — which is what <see cref="ServerTimeHelper.ConvertForDisplay"/>'s
/// UTC arm does, and what <see cref="ServerTimeHelper.FormatServerTime"/> must NOT do a second time.</para>
///
/// <para>Both display modes that are not the default are exercised, because the defect is present in all
/// three and a test written only against <c>ServerTime</c> — the default — would pass on a renderer that
/// emits the server's clock verbatim and ignores the preference entirely.</para>
/// </summary>
/* Writes ServerTimeHelper.UtcOffsetMinutes and CurrentDisplayMode, both process-wide mutable statics, so
   it joins the collection the other offset-touching classes use rather than racing them. */
[Collection("server-time-helper")]
public sealed class ServerClockRendererArithmeticTests
{
    /// <summary>The fleet's measured offset. Negative, and large enough that a sign error lands eight
    /// hours out rather than somewhere a reader might accept.</summary>
    private const int FleetOffsetMinutes = -240;

    /// <summary>One instant, twice: the naive-UTC value a collector stamps, and the same instant on the
    /// monitored server's own clock at <see cref="FleetOffsetMinutes"/>.</summary>
    private static readonly DateTime NaiveUtc =
        new(2026, 9, 9, 18, 0, 0, DateTimeKind.Unspecified);

    private static readonly DateTime ServerClock =
        new(2026, 9, 9, 14, 0, 0, DateTimeKind.Unspecified);

    /// <summary>The two frames are the same instant under <see cref="FleetOffsetMinutes"/>, asserted rather
    /// than assumed: the fixtures above are the whole premise of every assertion below, and an arithmetic
    /// slip in them would make the suite agree with itself about the wrong instant.</summary>
    [Fact]
    public void TheFixtures_AreOneInstantInTwoFrames()
    {
        Assert.Equal(ServerClock, NaiveUtc.AddMinutes(FleetOffsetMinutes));
        Assert.Equal(NaiveUtc, ServerClock.AddMinutes(-FleetOffsetMinutes));
        Assert.Equal(-240, FleetOffsetMinutes);
    }

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void TheTwoRenderers_RenderOneInstantIdentically_InEveryDisplayMode(TimeDisplayMode mode)
    {
        WithOffsetAndMode(mode, () =>
        {
            Assert.Equal(
                ServerTimeHelper.FormatServerTime(NaiveUtc),
                ServerTimeHelper.FormatServerClock(ServerClock));

            /* And the nullable overloads, which are what the row getters actually reach. */
            Assert.Equal(
                ServerTimeHelper.FormatServerTime((DateTime?)NaiveUtc),
                ServerTimeHelper.FormatServerClock((DateTime?)ServerClock));
        });
    }

    /// <summary>
    /// The defect, with its magnitude and direction: a server-clock value through the naive-UTC renderer
    /// lands exactly <see cref="FleetOffsetMinutes"/> away — four hours EARLY on this fleet — in every
    /// mode. Parsed back rather than string-compared, so the assertion states the size of the error
    /// instead of only that there is one.
    /// </summary>
    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.UTC)]
    public void AServerClockValue_ThroughTheNaiveUtcRenderer_IsTheWholeOffsetEarly(TimeDisplayMode mode)
    {
        WithOffsetAndMode(mode, () =>
        {
            var correct = Parse(ServerTimeHelper.FormatServerClock(ServerClock));
            var skewed = Parse(ServerTimeHelper.FormatServerTime(ServerClock));

            Assert.Equal(TimeSpan.FromMinutes(FleetOffsetMinutes), skewed - correct);
            Assert.True(skewed < correct, $"{mode}: expected the skewed rendering to be EARLIER");
        });
    }

    /// <summary>
    /// The two modes whose rendering is independent of the machine the test runs on, pinned absolutely, so
    /// the agreement above cannot be agreement on a wrong value.
    /// </summary>
    [Fact]
    public void ServerAndUtcModes_RenderThePinnedWallClocks()
    {
        WithOffsetAndMode(TimeDisplayMode.ServerTime, () =>
        {
            Assert.Equal("2026-09-09 14:00:00", ServerTimeHelper.FormatServerClock(ServerClock));
            Assert.Equal("2026-09-09 14:00:00", ServerTimeHelper.FormatServerTime(NaiveUtc));
        });

        WithOffsetAndMode(TimeDisplayMode.UTC, () =>
        {
            Assert.Equal("2026-09-09 18:00:00", ServerTimeHelper.FormatServerClock(ServerClock));
            Assert.Equal("2026-09-09 18:00:00", ServerTimeHelper.FormatServerTime(NaiveUtc));
        });
    }

    /// <summary>
    /// Local mode against the host's own zone, computed from the instant rather than pinned to a literal
    /// (CI and a developer machine are not in the same zone). This is what makes the mode arm load-bearing:
    /// a renderer that emitted the server's clock verbatim would pass the ServerTime pin above and fail
    /// here on any host that is not at the fleet offset.
    /// </summary>
    [Fact]
    public void LocalMode_RendersTheInstantInTheHostsOwnZone()
    {
        var expected = DateTime.SpecifyKind(NaiveUtc, DateTimeKind.Utc).ToLocalTime()
            .ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        WithOffsetAndMode(TimeDisplayMode.LocalTime, () =>
        {
            Assert.Equal(expected, ServerTimeHelper.FormatServerClock(ServerClock));
            Assert.Equal(expected, ServerTimeHelper.FormatServerTime(NaiveUtc));
        });
    }

    [Fact]
    public void ANullServerClockValue_RendersEmpty()
    {
        WithOffsetAndMode(TimeDisplayMode.ServerTime, () =>
            Assert.Equal("", ServerTimeHelper.FormatServerClock((DateTime?)null)));
    }

    private static DateTime Parse(string rendered) =>
        DateTime.ParseExact(rendered, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static void WithOffsetAndMode(TimeDisplayMode mode, Action body)
    {
        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        var savedMode = ServerTimeHelper.CurrentDisplayMode;
        try
        {
            ServerTimeHelper.UtcOffsetMinutes = FleetOffsetMinutes;
            ServerTimeHelper.CurrentDisplayMode = mode;
            body();
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
            ServerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }
}
