/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: axis ticks sit at whole WALL-clock times of the display zone while the X they are drawn at is the UTC
/// instant. <see cref="DisplayZone.WallTicks"/> is where that is decided: no tick for a wall time that never
/// happened, one tick per occurrence of one that happened twice, and whole local hours in a zone whose offset is
/// not a whole hour. Both ends of the visible range are inclusive.
/// </summary>
public sealed class DisplayZoneTickTests
{
    private static readonly TimeSpan HalfHour = TimeSpan.FromMinutes(30);

    private static string[] Labels(IReadOnlyList<(DateTime Utc, DateTime Wall)> ticks)
        => ticks.Select(t => t.Wall.ToString("HH:mm", CultureInfo.InvariantCulture)).ToArray();

    private static string[] UtcTimes(IReadOnlyList<(DateTime Utc, DateTime Wall)> ticks)
        => ticks.Select(t => t.Utc.ToString("HH:mm", CultureInfo.InvariantCulture)).ToArray();

    [Fact]
    public void SpringGap_UtcDisplay_LabelsNeverRepeat()
    {
        /* Axis 05:00Z-09:00Z on the spring change day, 30-minute step. UTC and a desktop in the server's own zone. */
        foreach (var zone in new[] { TimeZoneInfo.Utc, DisplayZoneFixtures.Eastern })
        {
            var ticks = DisplayZone.WallTicks(
                DisplayZoneFixtures.At(2026, 3, 8, 5), DisplayZoneFixtures.At(2026, 3, 8, 9), HalfHour, zone);

            Assert.Equal(9, ticks.Count);
            for (var i = 1; i < ticks.Count; i++)
            {
                Assert.True(ticks[i].Wall > ticks[i - 1].Wall, $"{zone.Id}: tick {i} reads {ticks[i].Wall:HH:mm} after {ticks[i - 1].Wall:HH:mm}");
            }
        }
    }

    [Fact]
    public void SpringGap_ServerDisplay_SkipsTheHourThatNeverHappened()
    {
        var ticks = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 3, 8, 5), DisplayZoneFixtures.At(2026, 3, 8, 7, 30), HalfHour, DisplayZoneFixtures.Eastern);

        Assert.Equal(new[] { "00:00", "00:30", "01:00", "01:30", "03:00", "03:30" }, Labels(ticks));
        /* Real spacing: every tick 30 minutes after the one before, across the change. */
        Assert.Equal(new[] { "05:00", "05:30", "06:00", "06:30", "07:00", "07:30" }, UtcTimes(ticks));
        for (var i = 1; i < ticks.Count; i++)
        {
            Assert.Equal(HalfHour, ticks[i].Utc - ticks[i - 1].Utc);
        }
    }

    [Fact]
    public void RepeatedHour_ServerDisplay_TicksEachOccurrence()
    {
        var ticks = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 11, 1, 4), DisplayZoneFixtures.At(2026, 11, 1, 7, 30), HalfHour, DisplayZoneFixtures.Eastern);

        Assert.Equal(new[] { "00:00", "00:30", "01:00", "01:30", "01:00", "01:30", "02:00", "02:30" }, Labels(ticks));
        Assert.Equal(new[] { "04:00", "04:30", "05:00", "05:30", "06:00", "06:30", "07:00", "07:30" }, UtcTimes(ticks));
        Assert.Equal(ticks.Count, ticks.Select(t => t.Utc).Distinct().Count());
    }

    [Fact]
    public void LocalDisplay_DesktopRepeatedHour_TicksEachOccurrence()
    {
        /* A desktop in US Pacific, back at 09:00Z on 2026-11-01: the axis 08:00Z-10:00Z passes through its repeated hour. */
        var ticks = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 11, 1, 8), DisplayZoneFixtures.At(2026, 11, 1, 10), HalfHour, DisplayZoneFixtures.Pacific);

        Assert.Equal(new[] { "01:00", "01:30", "01:00", "01:30", "02:00" }, Labels(ticks));
        Assert.Equal(new[] { "08:00", "08:30", "09:00", "09:30", "10:00" }, UtcTimes(ticks));
    }

    [Fact]
    public void HalfHourZones_TickOnWholeLocalHours()
    {
        /* India is UTC+5:30 all year: hourly ticks are whole local hours, so they fall on :30 in UTC. */
        var india = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 6, 1, 0), DisplayZoneFixtures.At(2026, 6, 1, 8), TimeSpan.FromHours(1), DisplayZoneFixtures.India);

        Assert.Equal(new[] { "06:00", "07:00", "08:00", "09:00", "10:00", "11:00", "12:00", "13:00" }, Labels(india));
        Assert.All(india, t => Assert.Equal(30, t.Utc.Minute));

        /* Lord Howe puts its clock back 30 minutes at 15:00Z on 2026-04-04 (02:00 to 01:30 local). Hourly ticks over the
           change neither repeat a label nor miss one; 15-minute ticks give the repeated half hour a tick per pass. */
        var hourly = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 4, 4, 12), DisplayZoneFixtures.At(2026, 4, 4, 18), TimeSpan.FromHours(1), DisplayZoneFixtures.LordHowe);
        Assert.Equal(hourly.Count, Labels(hourly).Distinct().Count());
        Assert.Equal(new[] { "23:00", "00:00", "01:00", "02:00", "03:00", "04:00" }, Labels(hourly));

        var quarters = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 4, 4, 14), DisplayZoneFixtures.At(2026, 4, 4, 16), TimeSpan.FromMinutes(15), DisplayZoneFixtures.LordHowe);
        Assert.Equal(new[] { "01:00", "01:15", "01:30", "01:45", "01:30", "01:45", "02:00", "02:15", "02:30" }, Labels(quarters));
        Assert.Equal(new[] { "14:00", "14:15", "14:30", "14:45", "15:00", "15:15", "15:30", "15:45", "16:00" }, UtcTimes(quarters));
    }

    [Fact]
    public void TheTicksAreInInstantOrder_AndBothEndsAreInclusive()
    {
        var ticks = DisplayZone.WallTicks(
            DisplayZoneFixtures.At(2026, 11, 1, 5), DisplayZoneFixtures.At(2026, 11, 1, 6, 30), TimeSpan.FromMinutes(30), DisplayZoneFixtures.Eastern);

        Assert.Equal(new[] { "05:00", "05:30", "06:00", "06:30" }, UtcTimes(ticks));
        Assert.Equal(ticks.OrderBy(t => t.Utc).Select(t => t.Utc), ticks.Select(t => t.Utc));
    }

    [Fact]
    public void AnEmptyOrBackwardsRange_HasNoTicks_AndANonPositiveStepIsRefused()
    {
        Assert.Empty(DisplayZone.WallTicks(DisplayZoneFixtures.At(2026, 3, 8, 9), DisplayZoneFixtures.At(2026, 3, 8, 5), HalfHour, TimeZoneInfo.Utc));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DisplayZone.WallTicks(DisplayZoneFixtures.At(2026, 3, 8, 5), DisplayZoneFixtures.At(2026, 3, 8, 9), TimeSpan.Zero, TimeZoneInfo.Utc));
    }
}

/// <summary>
/// #4766: the ScottPlot tick generator over <see cref="DisplayZone.WallTicks"/> and the axis extension that installs
/// it. The step ladder, the labels (24-hour time, the date on the first tick and at each date change), the
/// no-re-plot zone switch, and the integration through ScottPlot's own render pass.
/// </summary>
public sealed class DisplayZoneTickGeneratorTests
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    [Theory]
    [InlineData(10, 12, 1)]
    [InlineData(60, 12, 5)]
    [InlineData(180, 12, 15)]
    [InlineData(360, 12, 30)]
    [InlineData(720, 12, 60)]
    [InlineData(1440, 12, 180)]
    [InlineData(4320, 12, 360)]
    [InlineData(5760, 12, 720)]
    [InlineData(10080, 12, 1440)]
    public void TheStep_IsTheFinestOnTheLadderThatKeepsTheTicksToTheTarget(int spanMinutes, int target, int expectedStepMinutes)
    {
        Assert.Equal(TimeSpan.FromMinutes(expectedStepMinutes), DisplayZoneTickGenerator.ChooseStep(TimeSpan.FromMinutes(spanMinutes), target));
    }

    [Fact]
    public void ASpanBeyondTheLadder_ThinsToWholeDaysInsteadOfCrowding()
    {
        Assert.Equal(TimeSpan.FromDays(2), DisplayZoneTickGenerator.ChooseStep(TimeSpan.FromDays(14), 12));
        Assert.Equal(TimeSpan.FromDays(3), DisplayZoneTickGenerator.ChooseStep(TimeSpan.FromDays(30), 12));
        Assert.Equal(TimeSpan.FromDays(31), DisplayZoneTickGenerator.ChooseStep(TimeSpan.FromDays(365), 12));
    }

    [Fact]
    public void TheDateIsOnTheFirstTickAndAtEachDateChange_AndTheTimeIsTwentyFourHour()
    {
        var generator = new DisplayZoneTickGenerator(() => TimeZoneInfo.Utc, Invariant);
        var min = DisplayZoneFixtures.At(2026, 3, 8, 22).ToOADate();
        var max = DisplayZoneFixtures.At(2026, 3, 9, 4).ToOADate();

        var ticks = generator.Build(min, max, 1000);

        Assert.Equal("03/08\n22:00", ticks[0].Label);
        Assert.Equal("22:30", ticks[1].Label);
        var midnight = ticks.Single(t => t.Label.EndsWith("00:00", StringComparison.Ordinal));
        Assert.Equal("03/09\n00:00", midnight.Label);
        Assert.Equal(1, ticks.Count(t => t.Label.StartsWith("03/09", StringComparison.Ordinal)));
        Assert.All(ticks, t => Assert.True(t.IsMajor));
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 22).ToOADate(), ticks[0].Position);
    }

    [Fact]
    public void AZoneSwitch_RelabelsTheNextBuildAndMovesNoPosition()
    {
        var zone = TimeZoneInfo.Utc;
        var generator = new DisplayZoneTickGenerator(() => zone, Invariant);
        var min = DisplayZoneFixtures.At(2026, 11, 1, 6).ToOADate();
        var max = DisplayZoneFixtures.At(2026, 11, 1, 8).ToOADate();

        var inUtc = generator.Build(min, max, 1000);
        zone = DisplayZoneFixtures.Eastern;
        var inServer = generator.Build(min, max, 1000);

        Assert.Equal("11/01\n06:00", inUtc[0].Label);
        Assert.Equal("11/01\n01:00", inServer[0].Label);
        Assert.Equal(inUtc.Select(t => t.Position), inServer.Select(t => t.Position));
    }

    [Fact]
    public void AutumnServerAxis_LabelsTheRepeatedHourTwiceAtDistinctPositions()
    {
        var generator = new DisplayZoneTickGenerator(() => DisplayZoneFixtures.Eastern, Invariant);
        var ticks = generator.Build(
            DisplayZoneFixtures.At(2026, 11, 1, 4, 30).ToOADate(), DisplayZoneFixtures.At(2026, 11, 1, 7, 30).ToOADate(), 1000);

        var oneOClock = ticks.Where(t => t.Label.EndsWith("01:00", StringComparison.Ordinal)).ToArray();
        Assert.Equal(2, oneOClock.Length);
        Assert.NotEqual(oneOClock[0].Position, oneOClock[1].Position);
        Assert.Equal(1.0 / 24, oneOClock[1].Position - oneOClock[0].Position, 6);
    }

    [Fact]
    public void ARangeOutsideTheCalendar_OrWithNoWidth_HasNoTicks()
    {
        var generator = new DisplayZoneTickGenerator(() => TimeZoneInfo.Utc, Invariant);

        Assert.Empty(generator.Build(double.NaN, 10, 1000));
        Assert.Empty(generator.Build(-700000, 10, 1000));
        Assert.Empty(generator.Build(10, 3000000, 1000));
        Assert.Empty(generator.Build(20, 10, 1000));
        Assert.Empty(generator.Build(10, 20, 0));
    }

    [Fact]
    public void ScottPlotsRenderPass_FillsTheTicksThroughTheAxisExtension()
    {
        var plot = new ScottPlot.Plot();
        plot.Add.Scatter(new[] { 1.0 }, new[] { 1.0 });
        plot.Axes.DateTimeTicksBottomUtc(() => DisplayZoneFixtures.Eastern);
        var generator = Assert.IsType<DisplayZoneTickGenerator>(plot.Axes.Bottom.TickGenerator);
        plot.Axes.SetLimitsX(
            DisplayZoneFixtures.At(2026, 3, 8, 5).ToOADate(), DisplayZoneFixtures.At(2026, 3, 8, 9).ToOADate());

        plot.GetImage(1000, 400);

        Assert.NotEmpty(generator.Ticks);
        Assert.Contains(generator.Ticks, t => t.Label.EndsWith("03:00", StringComparison.Ordinal));
        Assert.DoesNotContain(generator.Ticks, t => t.Label.EndsWith("02:00", StringComparison.Ordinal) || t.Label.EndsWith("02:30", StringComparison.Ordinal));
    }
}
