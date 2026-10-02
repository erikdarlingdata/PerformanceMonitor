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
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Helpers;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a chart on the server tab plots the naive-UTC instant as X and draws its text in the tab's display zone,
/// so the two other places that print a time from it must agree with the axis. The CSV export writes each point's
/// time in the display zone and names the zone in the header, and its last column gives each point's UTC offset, so
/// the two points that read the same wall time in the repeated autumn hour stay apart while the time column stays
/// a plain date and time. The "Last refresh" line shows the refresh instant in the display zone rather than the
/// machine's clock under a label for some other zone.
///
/// <para>The export's time and header text come from two pure functions, called here without the WPF handler. The
/// context menus and the status line live on a WPF control this suite does not instantiate, so what wires them is
/// held by source pins over the <c>ServerTab*.cs</c> files, with comments stripped first.</para>
/// </summary>
public sealed class ChartCsvExportDisplayZoneTests
{
    private static readonly TimeZoneInfo Eastern = DisplayZoneFixtures.Eastern;

    /// <summary>The X a chart holds for a naive-UTC instant.</summary>
    private static double XOf(string utcInstant) =>
        DateTime.Parse(utcInstant, CultureInfo.InvariantCulture).ToOADate();

    /// <summary>
    /// The autumn change in US Eastern is 2026-11-01 06:00Z: 01:30 happens at 05:30Z (EDT) and again at 06:30Z (EST).
    /// The point at 06:30Z is the second 01:30, and the export writes the wall time the axis shows for it, then its
    /// offset; the same point in the UTC zone is written as 06:30 with a zero offset.
    /// </summary>
    [Fact]
    public void ThePointAt0630Z_OnTheAutumnChangeDay_IsWritten0130InEastern_And0630InUtc()
    {
        var x = XOf("2026-11-01 06:30:00");

        Assert.Equal("2026-11-01 01:30:00,CPU,12.5,-05:00", ContextMenuHelper.ChartCsvLine(x, "CPU", 12.5, ",", Eastern));
        Assert.Equal("2026-11-01 06:30:00,CPU,12.5,+00:00", ContextMenuHelper.ChartCsvLine(x, "CPU", 12.5, ",", TimeZoneInfo.Utc));
    }

    /// <summary>
    /// The two points that read 01:30 in the repeated hour write the same first three cells, and the last cell tells
    /// them apart: -04:00 for the first (05:30Z, EDT) and -05:00 for the second (06:30Z, EST). The time column is
    /// the same plain date and time on both, never a text with an offset stuck to some of its rows.
    /// </summary>
    [Fact]
    public void TheTwoPointsOfTheRepeatedHour_WriteTheSameTimeCell_AndAreToldApartByTheirOffsets()
    {
        var first = ContextMenuHelper.ChartCsvLine(XOf("2026-11-01 05:30:00"), "CPU", 12.5, ",", Eastern);
        var second = ContextMenuHelper.ChartCsvLine(XOf("2026-11-01 06:30:00"), "CPU", 12.5, ",", Eastern);

        Assert.Equal("2026-11-01 01:30:00,CPU,12.5,-04:00", first);
        Assert.Equal("2026-11-01 01:30:00,CPU,12.5,-05:00", second);
        Assert.Equal(first[..^"-04:00".Length], second[..^"-05:00".Length]);
    }

    /// <summary>
    /// Every row carries its offset, not only the repeated hour's: a summer instant is on EDT (-04:00) and a winter
    /// one on EST (-05:00), and the time cell is the plain wall time in both.
    /// </summary>
    [Theory]
    [InlineData("2026-07-15 16:00:00", "2026-07-15 12:00:00", "-04:00")]
    [InlineData("2026-01-15 17:00:00", "2026-01-15 12:00:00", "-05:00")]
    public void ASummerAndAWinterInstant_CarryTheirOwnOffsets(string utcInstant, string expectedWall, string expectedOffset)
    {
        var line = ContextMenuHelper.ChartCsvLine(XOf(utcInstant), "CPU", 3, ",", Eastern);

        Assert.Equal(expectedWall + ",CPU,3," + expectedOffset, line);
    }

    /// <summary>
    /// Each instant across both changes is written as its own wall clock: the offset switches at the change and is
    /// not one fixed number, the repeated hour reads 01:30 twice, in time order, and each row's last cell is the
    /// offset that instant carries.
    /// </summary>
    [Theory]
    [InlineData("2026-11-01 04:30:00", "2026-11-01 00:30:00", "-04:00")]
    [InlineData("2026-11-01 05:30:00", "2026-11-01 01:30:00", "-04:00")]
    [InlineData("2026-11-01 06:30:00", "2026-11-01 01:30:00", "-05:00")]
    [InlineData("2026-11-01 07:30:00", "2026-11-01 02:30:00", "-05:00")]
    [InlineData("2026-03-08 06:30:00", "2026-03-08 01:30:00", "-05:00")]
    [InlineData("2026-03-08 07:30:00", "2026-03-08 03:30:00", "-04:00")]
    public void EachInstantAcrossTheChanges_IsWrittenAsItsOwnEasternWallClockAndOffset(string utcInstant, string expectedWall, string expectedOffset)
    {
        var line = ContextMenuHelper.ChartCsvLine(XOf(utcInstant), "Series", 1.5, ",", Eastern);

        Assert.Equal(expectedWall + ",Series,1.5," + expectedOffset, line);
    }

    /// <summary>
    /// The server's own clock is a zone too: a fixed-offset server (no zone name collected) writes its offset time,
    /// the header names that offset, and the last cell repeats it.
    /// </summary>
    [Fact]
    public void AFixedOffsetServerClock_WritesItsOffsetTime_AndTheHeaderNamesTheOffset()
    {
        var zone = ServerClock.FixedOffset(330).AsTimeZone();

        Assert.Equal("2026-11-01 12:00:00;CPU;3;+05:30", ContextMenuHelper.ChartCsvLine(XOf("2026-11-01 06:30:00"), "CPU", 3, ";", zone));
        Assert.Equal("DateTime (UTC+05:30);Series;Value;UTC offset", ContextMenuHelper.ChartCsvHeader(";", zone));
    }

    /// <summary>
    /// The time column's header names the zone the times are written in; the last column, "UTC offset", is added
    /// after the other two, which are unchanged.
    /// </summary>
    [Fact]
    public void TheHeader_NamesTheZoneItsTimesAreWrittenIn_AndEndsInTheUtcOffsetColumn()
    {
        Assert.Equal("DateTime (UTC),Series,Value,UTC offset", ContextMenuHelper.ChartCsvHeader(",", TimeZoneInfo.Utc));
        Assert.Equal("DateTime (Eastern Standard Time),Series,Value,UTC offset", ContextMenuHelper.ChartCsvHeader(",", Eastern));
        Assert.Equal("DateTime (Eastern Standard Time)\tSeries\tValue\tUTC offset", ContextMenuHelper.ChartCsvHeader("\t", Eastern));
    }

    /// <summary>The series name is escaped for the separator exactly as before, in both zones.</summary>
    [Fact]
    public void ASeriesNameWithTheSeparator_IsQuotedAsBefore()
    {
        var x = XOf("2026-11-01 06:30:00");

        Assert.Equal("2026-11-01 01:30:00,\"Reads, MB\",1.5,-05:00", ContextMenuHelper.ChartCsvLine(x, "Reads, MB", 1.5, ",", Eastern));
        Assert.Equal("2026-11-01 06:30:00,\"Reads, MB\",1.5,+00:00", ContextMenuHelper.ChartCsvLine(x, "Reads, MB", 1.5, ",", TimeZoneInfo.Utc));
    }

    /// <summary>
    /// The UTC zone writes the plotted instant as it is, under a header that says so, for every separator, with a
    /// "+00:00" offset on every row, the repeated hour's two instants included.
    /// </summary>
    [Fact]
    public void InUtc_TheLineIsTheInstantItself_AndTheHeaderNamesUtc()
    {
        var x = XOf("2026-11-01 06:30:00");

        Assert.Equal("DateTime (UTC),Series,Value,UTC offset", ContextMenuHelper.ChartCsvHeader(",", TimeZoneInfo.Utc));
        Assert.Equal("DateTime (UTC);Series;Value;UTC offset", ContextMenuHelper.ChartCsvHeader(";", TimeZoneInfo.Utc));
        Assert.Equal("2026-11-01 06:30:00,Series,1.5,+00:00", ContextMenuHelper.ChartCsvLine(x, "Series", 1.5, ",", TimeZoneInfo.Utc));
        Assert.Equal("2026-11-01 06:30:00\tSeries\t1.5\t+00:00", ContextMenuHelper.ChartCsvLine(x, "Series", 1.5, "\t", TimeZoneInfo.Utc));
        Assert.Equal("2026-11-01 05:30:00,Series,1.5,+00:00", ContextMenuHelper.ChartCsvLine(XOf("2026-11-01 05:30:00"), "Series", 1.5, ",", TimeZoneInfo.Utc));
    }

    private static IEnumerable<(string Name, string Code)> ServerTabCode()
    {
        var files = Directory.GetFiles(ControlsFolder(), "ServerTab*.cs");
        Assert.True(files.Length >= 10, $"expected the ServerTab partial files, found {files.Length}.");
        return files.Select(f => (Path.GetFileName(f), CodeOnly(File.ReadAllText(f))));
    }

    /// <summary>
    /// Every context menu the server tab sets up passes the display zone, so no chart's export is left writing the raw
    /// UTC instant under a header that does not name a zone. A call added later without it fails here.
    /// </summary>
    [Fact]
    public void EverySetupChartContextMenuCall_OnTheServerTab_PassesTheDisplayZone()
    {
        var calls = 0;
        var offenders = new List<string>();
        foreach (var (name, code) in ServerTabCode())
        {
            foreach (Match open in Regex.Matches(code, @"\bSetupChartContextMenu\s*\("))
            {
                calls++;
                var call = CallText(code, open.Index + open.Length - 1);
                if (!Regex.IsMatch(call, @"\bdisplayZone\s*:\s*GetPickerZone\b"))
                {
                    offenders.Add($"{name}: {call}");
                }
            }
        }

        Assert.True(calls >= 20, $"expected the server tab's chart context menus, found {calls} calls.");
        Assert.True(offenders.Count == 0,
            "these SetupChartContextMenu calls do not pass displayZone: GetPickerZone:\n" + string.Join("\n", offenders));
    }

    /// <summary>
    /// The "Last refresh" line prints the refresh instant read in the tab's display zone, the zone its label
    /// names, and no longer the machine's clock (<c>DateTime.Now</c>) under a label for another zone. The instant is
    /// <c>DateTime.UtcNow</c> read once, straight or through a local the label shares.
    /// </summary>
    [Fact]
    public void TheLastRefreshLine_ShowsTheRefreshInstantInTheDisplayZone_NotTheMachinesClock()
    {
        var code = CodeOnly(File.ReadAllText(Path.Combine(ControlsFolder(), "ServerTab.Refresh.cs")));

        var lines = Regex.Matches(code, @"Last refresh:[^;]*;");
        Assert.Single(lines);
        var line = lines[0].Value;
        Assert.DoesNotContain("DateTime.Now", line, StringComparison.Ordinal);

        var shown = Regex.Match(line, @"\{(\w+):HH:mm:ss\}");
        Assert.True(shown.Success, $"the line should print its time as a named value: {line}");
        var name = Regex.Escape(shown.Groups[1].Value);
        var shownFrom = Regex.Match(
            code,
            @"\bvar\s+" + name + @"\s*=\s*(?:PerformanceMonitor\.Ui\.)?DisplayZone\.ToDisplay\(\s*(\w+(?:\.\w+)*)\s*,\s*GetPickerZone\(\)\s*\)\s*;");
        Assert.True(shownFrom.Success, $"the time should be DisplayZone.ToDisplay(<the refresh instant>, GetPickerZone()): {line}");

        var instant = shownFrom.Groups[1].Value;
        if (instant != "DateTime.UtcNow")
        {
            Assert.Matches(new Regex(@"\bvar\s+" + Regex.Escape(instant) + @"\s*=\s*DateTime\.UtcNow\s*;"), code);
        }
    }

    /// <summary>The text of the call whose opening parenthesis is at <paramref name="open"/>, arguments included.</summary>
    private static string CallText(string code, int open)
    {
        var depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '(') depth++;
            else if (code[i] == ')' && --depth == 0) return code.Substring(open, i - open + 1);
        }
        return code.Substring(open);
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ControlsFolder([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
