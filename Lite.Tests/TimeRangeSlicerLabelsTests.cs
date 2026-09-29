/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the time slicer under a grid words its axis on whole wall-clock hours of the display zone. Its data and
/// its selection are UTC instants and stay put; <see cref="TimeRangeSlicerControl.SlicerLabels"/> only chooses which
/// instants get a label and what the label says. The old labels sat at the start plus even fractions of the span
/// (wherever the data happened to begin, so never on an hour) and read the selected server's clock, so a label could
/// disagree with the axes above it, and on the autumn change day two labels could read the same wall time with no
/// way to tell them apart.
///
/// <para>The slicer and the three History windows are WPF controls this suite does not instantiate, so the label
/// arithmetic is tested as the pure function it is, and the wiring is held by source pins with comments stripped
/// first. US Eastern is the display zone in the examples (clock change 2026-03-08 07:00Z forward, 2026-11-01 06:00Z
/// back).</para>
/// </summary>
public sealed class TimeRangeSlicerLabelsTests
{
    private const double SpacingPx = 90;

    /// <summary>
    /// Four hours across the autumn change: 04:00Z is 00:00 EDT, 05:00Z is 01:00 EDT, 06:00Z is 01:00 EST again, and
    /// 08:00Z is 03:00 EST. Room for four labels gives a one-hour step, so every hour of the zone is labelled and the
    /// repeated hour appears twice, at two instants an hour apart.
    /// </summary>
    [Fact]
    public void TheAutumnChangeWindow_LabelsEachWholeHour_AndTheRepeatedHourTwice()
    {
        var start = DisplayZoneFixtures.At(2026, 11, 1, 4);
        var end = DisplayZoneFixtures.At(2026, 11, 1, 8);

        var labels = TimeRangeSlicerControl.SlicerLabels(start, end, 4 * SpacingPx, SpacingPx, DisplayZoneFixtures.Eastern);

        Assert.Equal(
            new[]
            {
                DisplayZoneFixtures.At(2026, 11, 1, 4), DisplayZoneFixtures.At(2026, 11, 1, 5),
                DisplayZoneFixtures.At(2026, 11, 1, 6), DisplayZoneFixtures.At(2026, 11, 1, 7),
                DisplayZoneFixtures.At(2026, 11, 1, 8)
            },
            labels.Select(l => l.Utc));
        Assert.Equal(
            new[] { "11/01 00:00", "11/01 01:00", "11/01 01:00", "11/01 02:00", "11/01 03:00" },
            labels.Select(l => l.Text));
    }

    /// <summary>
    /// A day of room for three labels cannot hold a three-hour step, so it takes the twelve-hour one; the same day
    /// with room for twelve keeps the three-hour step. Either way, neighbouring labels sit at least the spacing apart
    /// on the strip.
    /// </summary>
    [Fact]
    public void ANarrowerStrip_PicksACoarserStep_AndNoTwoLabelsSitCloserThanTheSpacing()
    {
        var start = DisplayZoneFixtures.At(2026, 6, 10, 0);
        var end = start.AddHours(24);
        var wideWidth = 12 * SpacingPx;
        var narrowWidth = 3 * SpacingPx;

        var wide = TimeRangeSlicerControl.SlicerLabels(start, end, wideWidth, SpacingPx, DisplayZoneFixtures.Eastern);
        var narrow = TimeRangeSlicerControl.SlicerLabels(start, end, narrowWidth, SpacingPx, DisplayZoneFixtures.Eastern);

        Assert.Equal(
            new[] { "06/09 21:00", "06/10 00:00", "06/10 03:00", "06/10 06:00", "06/10 09:00", "06/10 12:00", "06/10 15:00", "06/10 18:00" },
            wide.Select(l => l.Text));
        Assert.Equal(new[] { "06/10 00:00", "06/10 12:00" }, narrow.Select(l => l.Text));
        Assert.Equal(TimeSpan.FromHours(3), wide[1].Utc - wide[0].Utc);
        Assert.Equal(TimeSpan.FromHours(12), narrow[1].Utc - narrow[0].Utc);

        foreach (var (labels, width) in new[] { (wide, wideWidth), (narrow, narrowWidth) })
        {
            for (var i = 1; i < labels.Count; i++)
            {
                var gapPx = (labels[i].Utc - labels[i - 1].Utc).TotalSeconds / (end - start).TotalSeconds * width;
                Assert.True(gapPx >= SpacingPx, $"labels {i - 1} and {i} sit {gapPx:F1}px apart on a {width}px strip; the spacing is {SpacingPx}px.");
            }
        }
    }

    /// <summary>
    /// A window that starts at 03:17Z still gets labels on the hour: the ticks are whole multiples of the step in the
    /// zone, not the start plus fractions of the span. In UTC the hour of the zone is the hour of the instant.
    /// </summary>
    [Fact]
    public void TheUtcZone_LabelsWholeUtcHours_WhereverTheWindowStarts()
    {
        var start = DisplayZoneFixtures.At(2026, 11, 1, 3, 17);
        var end = DisplayZoneFixtures.At(2026, 11, 1, 7, 41);

        var labels = TimeRangeSlicerControl.SlicerLabels(start, end, 7 * SpacingPx, SpacingPx, TimeZoneInfo.Utc);

        Assert.Equal(new[] { "11/01 04:00", "11/01 05:00", "11/01 06:00", "11/01 07:00" }, labels.Select(l => l.Text));
        Assert.All(labels, l => Assert.Equal(0, l.Utc.Minute));
    }

    /// <summary>
    /// A zone whose offset is not a whole number of hours labels its own whole hours: India's 10:00 sits at 04:30Z,
    /// so the labels are on the half hour of the instant.
    /// </summary>
    [Fact]
    public void AHalfHourZone_LabelsItsOwnWholeHours()
    {
        var start = DisplayZoneFixtures.At(2026, 6, 10, 3);
        var end = DisplayZoneFixtures.At(2026, 6, 10, 7);

        var labels = TimeRangeSlicerControl.SlicerLabels(start, end, 4 * SpacingPx, SpacingPx, DisplayZoneFixtures.India);

        Assert.Equal(new[] { "06/10 09:00", "06/10 10:00", "06/10 11:00", "06/10 12:00" }, labels.Select(l => l.Text));
        Assert.Equal(
            new[]
            {
                DisplayZoneFixtures.At(2026, 6, 10, 3, 30), DisplayZoneFixtures.At(2026, 6, 10, 4, 30),
                DisplayZoneFixtures.At(2026, 6, 10, 5, 30), DisplayZoneFixtures.At(2026, 6, 10, 6, 30)
            },
            labels.Select(l => l.Utc));
    }

    /// <summary>An empty or backwards window, or a strip with no width, has no labels rather than an error.</summary>
    [Theory]
    [InlineData(2026, 11, 1, 5, 2026, 11, 1, 5, 400.0)]
    [InlineData(2026, 11, 1, 8, 2026, 11, 1, 4, 400.0)]
    [InlineData(2026, 11, 1, 4, 2026, 11, 1, 8, 0.0)]
    public void ADegenerateWindowOrStrip_HasNoLabels(int y1, int m1, int d1, int h1, int y2, int m2, int d2, int h2, double widthPx)
    {
        var labels = TimeRangeSlicerControl.SlicerLabels(
            DisplayZoneFixtures.At(y1, m1, d1, h1), DisplayZoneFixtures.At(y2, m2, d2, h2), widthPx, SpacingPx, DisplayZoneFixtures.Eastern);

        Assert.Empty(labels);
    }

    /// <summary>
    /// The slicer and the three History windows word no time through the server's clock, the slicer draws the labels
    /// this function returns, and every hover in the windows is given the display zone.
    /// </summary>
    [Theory]
    [InlineData("Controls", "TimeRangeSlicerControl.xaml.cs")]
    [InlineData("Windows", "ProcedureHistoryWindow.xaml.cs")]
    [InlineData("Windows", "QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("Windows", "QueryStoreHistoryWindow.xaml.cs")]
    public void TheSlicerAndTheHistoryWindows_WordNoTimeThroughTheServersClock(string folder, string file)
    {
        var source = CodeOnly(ReadLite(folder, file));

        Assert.DoesNotContain("ToServerTime(", source, StringComparison.Ordinal);
        Assert.DoesNotContain("FormatServerTime(", source, StringComparison.Ordinal);

        if (file == "TimeRangeSlicerControl.xaml.cs")
        {
            Assert.Contains("SlicerLabels(DataStartUtc, DataEndUtc, w, minLabelSpacingPx, CurrentZone())", source, StringComparison.Ordinal);
            Assert.Contains("public Func<TimeZoneInfo>? DisplayZone { get; set; }", source, StringComparison.Ordinal);
            Assert.Contains("ServerTab.PickerZone(ServerTimeHelper.CurrentDisplayMode, ServerTimeHelper.ActiveServerClock)", source, StringComparison.Ordinal);
            return;
        }

        var hovers = Regex.Matches(source, @"new ChartHoverHelper\([^;]*;").Select(m => m.Value).ToList();
        Assert.NotEmpty(hovers);
        Assert.All(hovers, h => Assert.Matches(@"displayZone\s*:", h));
    }

    /// <summary>
    /// Every slicer on the server tab is given the tab's display zone, and each History window the tab opens is
    /// handed the same zone.
    /// </summary>
    [Fact]
    public void ServerTab_GivesEverySlicerAndHistoryWindowItsDisplayZone()
    {
        var tabXaml = File.ReadAllText(Path.GetFullPath(Path.Combine(ControlsFolder(), "ServerTab.xaml")));
        var slicerNames = Regex.Matches(tabXaml, @"<controls:TimeRangeSlicerControl\s+x:Name=""(\w+)""").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal(6, slicerNames.Count);

        var tab = CodeOnly(File.ReadAllText(Path.Combine(ControlsFolder(), "ServerTab.xaml.cs")));
        var wiring = Regex.Match(tab, @"foreach \(var slicer in new\[\] \{([^}]*)\}\)\s*slicer\.DisplayZone = GetPickerZone;");
        Assert.True(wiring.Success, "ServerTab.xaml.cs no longer gives its slicers the display zone in one loop.");
        var wired = wiring.Groups[1].Value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        Assert.Equal(slicerNames.OrderBy(n => n, StringComparer.Ordinal), wired.OrderBy(n => n, StringComparer.Ordinal));

        var grids = CodeOnly(File.ReadAllText(Path.Combine(ControlsFolder(), "ServerTab.Grids.cs")));
        var opens = Regex.Matches(grids, @"new Windows\.(?:Procedure|QueryStats|QueryStore)HistoryWindow\([^;]*;").Select(m => m.Value).ToList();
        Assert.Equal(3, opens.Count);
        Assert.All(opens, o => Assert.EndsWith(", GetPickerZone);", o, StringComparison.Ordinal));
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadLite(string folder, string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", folder, file)));

    private static string ControlsFolder([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
