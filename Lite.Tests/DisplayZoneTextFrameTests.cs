/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the text on a server tab, the wait drill-down window and the version store chart's axis is worded in the
/// display zone the TAB names, from the instant it is given. It is never the active server's clock: a tab that
/// refreshes while another server's tab is selected would bake that other server's zone into its own text.
///
/// <para>The seams here take the zone as an argument (<c>GetTimeRangeDescription</c>, <c>BaselineBannerText</c>,
/// <c>DrillDownIndicatorText</c>), so each test hands one in and sets the active clock to something different. The
/// FinOps chart and the tab's own call sites are WPF controls this suite does not instantiate, so they are held by
/// source pins with comments stripped first.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class DisplayZoneTextFrameTests : IDisposable
{
    private const string EasternZone = "Eastern Standard Time";
    private const int WinterOffset = -300;

    /* The server tab files whose text is worded in the tab's zone. */
    private static readonly string[] TextFiles = ["ServerTab.Comparison.cs", "ServerTab.DrillDown.cs", "ServerTab.xaml.cs"];

    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    private static TimeZoneInfo Eastern() => ServerClock.Resolve(EasternZone, WinterOffset).AsTimeZone();

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /* Another server is the active one: nine hours ahead of UTC, in Server mode. Text that reads it is wrong. */
    private static void MakeAnotherServerActive()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve(null, 540);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
    }

    private static List<QuerySnapshotRow> RowsInTheRepeatedHour() =>
    [
        new QuerySnapshotRow { CollectionTime = Utc(2026, 11, 1, 6, 30) },
        new QuerySnapshotRow { CollectionTime = Utc(2026, 11, 1, 5, 30) },
    ];

    /// <summary>
    /// Rows at 05:30Z and 06:30Z on the autumn change day are one hour apart and both read 01:30 on a US Eastern
    /// clock. In UTC the line names the instants as they are; in Eastern each end is what
    /// <c>DisplayZone.Format</c> writes for it, which adds the offset that tells the two 01:30s apart.
    /// </summary>
    [Fact]
    public void GetTimeRangeDescription_NamesBothEndsInTheZoneItIsGiven_ThroughTheRepeatedHour()
    {
        var rows = RowsInTheRepeatedHour();

        Assert.Equal("2026-11-01 05:30:00 to 2026-11-01 06:30:00", WaitDrillDownWindow.GetTimeRangeDescription(rows, TimeZoneInfo.Utc));

        var eastern = WaitDrillDownWindow.GetTimeRangeDescription(rows, Eastern());
        Assert.Equal("2026-11-01 01:30:00 -04:00 to 2026-11-01 01:30:00 -05:00", eastern);
        Assert.Equal(
            DisplayZone.Format(Utc(2026, 11, 1, 5, 30), Eastern(), "yyyy-MM-dd HH:mm:ss") + " to " +
            DisplayZone.Format(Utc(2026, 11, 1, 6, 30), Eastern(), "yyyy-MM-dd HH:mm:ss"),
            eastern);
    }

    /// <summary>
    /// The zone the window is given decides the text, not the active clock: with another server nine hours ahead
    /// active in Server mode, the same rows still read as UTC in a UTC zone and as Eastern in an Eastern one. The
    /// renderer the window used before (<c>ServerTimeHelper.FormatServerTime</c>) reads the active clock and would
    /// say 14:30, so this fails on that shape.
    /// </summary>
    [Fact]
    public void GetTimeRangeDescription_IgnoresTheActiveClock()
    {
        MakeAnotherServerActive();
        Assert.Equal("2026-11-01 14:30:00", ServerTimeHelper.FormatServerTime(Utc(2026, 11, 1, 5, 30)));

        var rows = RowsInTheRepeatedHour();

        Assert.Equal("2026-11-01 05:30:00 to 2026-11-01 06:30:00", WaitDrillDownWindow.GetTimeRangeDescription(rows, TimeZoneInfo.Utc));
        Assert.Equal(
            "2026-11-01 01:30:00 -04:00 to 2026-11-01 01:30:00 -05:00",
            WaitDrillDownWindow.GetTimeRangeDescription(rows, Eastern()));
    }

    [Fact]
    public void GetTimeRangeDescription_IsEmptyForNoRows()
    {
        Assert.Equal("", WaitDrillDownWindow.GetTimeRangeDescription([], Eastern()));
    }

    /// <summary>
    /// The three baseline banners word both ends of the baseline window in the zone the tab names, with the same
    /// instants: 12:00Z to 18:00Z in July is 08:00 to 14:00 in Eastern and 12:00 to 18:00 in UTC, whichever
    /// server is active.
    /// </summary>
    [Fact]
    public void BaselineBannerText_WordsTheSameInstantsInTheZoneItIsGiven_NotTheActiveClock()
    {
        var baseline = (From: Utc(2026, 7, 1, 12, 0), To: Utc(2026, 7, 1, 18, 0));

        MakeAnotherServerActive();

        Assert.Equal(
            "Comparing against baseline: 2026-07-01 12:00:00 → 2026-07-01 18:00:00",
            ServerTab.BaselineBannerText(baseline, TimeZoneInfo.Utc));
        Assert.Equal(
            "Comparing against baseline: 2026-07-01 08:00:00 → 2026-07-01 14:00:00",
            ServerTab.BaselineBannerText(baseline, Eastern()));
    }

    /// <summary>
    /// The drill-down indicator reads the same for the heatmap drill and the generic drill, in the tab's zone, in
    /// hours and minutes, and no longer says "(server time)". Through the repeated hour the two ends read 01:30
    /// with the offset that tells them apart.
    /// </summary>
    [Fact]
    public void DrillDownIndicatorText_WordsBothEndsInTheZoneItIsGiven_AndClaimsNoServerTime()
    {
        MakeAnotherServerActive();

        var text = ServerTab.DrillDownIndicatorText(Utc(2026, 7, 1, 12, 0), Utc(2026, 7, 1, 12, 30), Eastern());
        Assert.Equal("Drill-down: 08:00 → 08:30", text);
        Assert.Equal("Drill-down: 12:00 → 12:30", ServerTab.DrillDownIndicatorText(Utc(2026, 7, 1, 12, 0), Utc(2026, 7, 1, 12, 30), TimeZoneInfo.Utc));

        var repeated = ServerTab.DrillDownIndicatorText(Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30), Eastern());
        Assert.Equal("Drill-down: 01:30 -04:00 → 01:30 -05:00", repeated);
        Assert.DoesNotContain("server time", repeated, StringComparison.OrdinalIgnoreCase);
    }

    // ── Source pins ──

    /// <summary>
    /// The version store chart plots each sample's UTC instant and draws its ticks with
    /// <c>DateTimeTicksBottomUtc</c>: it neither converts X through the server's clock nor draws the old axis. The
    /// zone comes from the selected server's own clock, read once per load, and the mode is read on every render.
    /// </summary>
    [Fact]
    public void FinOpsVersionStoreChart_PlotsTheInstantAndDrawsTheDisplayZoneAxis()
    {
        var code = CodeOnly(File.ReadAllText(Path.Combine(ControlsFolder(), "FinOpsTab.xaml.cs")));

        var render = MethodBody(code, "private void RenderPvsTrendChart(");
        Assert.DoesNotContain("ToServerTime(", render, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTimeTicksBottomDateChange(", render, StringComparison.Ordinal);
        Assert.Contains("DateTimeTicksBottomUtc(", render, StringComparison.Ordinal);
        Assert.Contains("t.CollectionTime.ToOADate()", render, StringComparison.Ordinal);

        var load = MethodBody(code, "private async System.Threading.Tasks.Task LoadPvsStatsAsync(");
        Assert.Single(Regex.Matches(load, @"\bGetServerClockAsync\s*\(\s*serverId\s*\)"));
        Assert.Contains("_openTabClock?.Invoke(serverId)", load, StringComparison.Ordinal);
        Assert.Contains("ServerTimeHelper.ClockForServer(collected, openTab)", load, StringComparison.Ordinal);
        Assert.Contains(
            "() => ServerTimeHelper.DisplayZoneFor(ServerTimeHelper.CurrentDisplayMode, clock)", load, StringComparison.Ordinal);

        Assert.Contains("Func<int, ServerClock?>? openTabClock = null", code, StringComparison.Ordinal);

        var main = CodeOnly(File.ReadAllText(Path.Combine(ControlsFolder(), "..", "MainWindow.xaml.cs")));
        Assert.Contains("FinOpsContent.Initialize(_dataService, _serverManager, OpenTabClockFor);", main, StringComparison.Ordinal);
    }

    /// <summary>
    /// The server tab files that word a time say it in the tab's own zone: every
    /// <c>ServerTimeHelper.FormatServerTime(</c> call is gone from them, and so is a <c>DateTime.Now:HH</c> printed
    /// into text (the machine's clock is neither the server's nor the display mode's). A tab words its times with
    /// <c>DisplayZone.Format</c> in <c>GetPickerZone()</c>.
    /// </summary>
    [Fact]
    public void ServerTabTextFiles_WordNoTimeOnTheActiveClockOrTheMachineClock()
    {
        var offenders = new List<string>();
        foreach (var (name, code) in ServerTabCode().Where(f => TextFiles.Contains(f.Name)))
        {
            foreach (Match m in Regex.Matches(code, @"\bServerTimeHelper\s*\.\s*FormatServerTime\s*\("))
            {
                offenders.Add($"{name} (code line {Line(code, m.Index)}): ServerTimeHelper.FormatServerTime(");
            }

            foreach (Match m in Regex.Matches(code, @"\bDateTime\s*\.\s*Now\s*:\s*[hH]|\bDateTime\s*\.\s*Now\s*\.\s*ToString\s*\(\s*""[^""]*[hH]"))
            {
                offenders.Add($"{name} (code line {Line(code, m.Index)}): DateTime.Now printed as a time");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "These word a time on the active server's clock or the machine's; use DisplayZone.Format(instant, " +
            "GetPickerZone(), format): " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The wait drill-down window takes the tab's zone at construction (required, so no caller can leave it on the
    /// active clock), and the tab hands it <c>GetPickerZone()</c>. Its range line goes through
    /// <c>DisplayZone.Format</c> and not through the active-clock renderer.
    /// </summary>
    [Fact]
    public void WaitDrillDownWindow_IsGivenTheTabsZone_AndWordsItsRangeThroughDisplayZone()
    {
        var window = CodeOnly(File.ReadAllText(Path.Combine(ControlsFolder(), "..", "Windows", "WaitDrillDownWindow.xaml.cs")));
        Assert.DoesNotContain("FormatServerTime(", window, StringComparison.Ordinal);
        Assert.Contains("TimeZoneInfo displayZone,", window, StringComparison.Ordinal);
        Assert.Contains("internal static string GetTimeRangeDescription(List<QuerySnapshotRow> data, TimeZoneInfo zone)", window, StringComparison.Ordinal);
        Assert.Contains("DisplayZone.Format(first, zone, RangeTimeFormat)", window, StringComparison.Ordinal);
        Assert.Empty(Regex.Matches(window, @"GetTimeRangeDescription\((?!List<)[^,()]*\)"));

        var drill = ServerTabCode().Single(f => f.Name == "ServerTab.DrillDown.cs").Code;
        Assert.Matches(
            new Regex(@"new\s+Windows\.WaitDrillDownWindow\(\s*_dataService,\s*_serverId,\s*waitType,\s*1,\s*GetPickerZone\(\),"),
            drill);
    }

    private static IEnumerable<(string Name, string Code)> ServerTabCode()
    {
        var files = Directory.GetFiles(ControlsFolder(), "ServerTab*.cs");
        Assert.True(files.Length >= 10, $"expected the ServerTab partial files, found {files.Length}.");
        return files.Select(f => (Path.GetFileName(f), CodeOnly(File.ReadAllText(f))));
    }

    private static string Line(string code, int index) =>
        (code.Take(index).Count(c => c == '\n') + 1).ToString();

    /* The text of the method whose signature starts with <signature>, from its opening brace to the matching close. */
    private static string MethodBody(string code, string signature)
    {
        var start = code.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} is no longer in the file; update this pin.");
        var open = code.IndexOf('{', start);
        var depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}' && --depth == 0)
            {
                return code[open..(i + 1)];
            }
        }

        throw new InvalidOperationException($"no closing brace for {signature}");
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
