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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a chart on the server tab plots the naive-UTC instant as its X value. The display mode (UTC, this
/// machine's zone, or the server's own clock) is applied only to TEXT: the tick labels, with the ticks themselves at
/// whole wall-clock times of the display zone, and the hover labels. Switching the mode therefore relabels the
/// chart and moves no point, and the window the reads take is the window the axis spans.
///
/// <para>The charts are WPF controls this suite does not instantiate, so the frame is held by source pins over the
/// <c>ServerTab*.cs</c> partial files, with comments stripped first so a sentence that names the old shape cannot
/// fail a pin. The window arithmetic is tested on its own in <c>ServerTabChartWindowTests</c> and
/// <c>ServerTabDrillWindowTests</c>.</para>
/// </summary>
public sealed class ServerTabChartUtcFrameTests
{
    /* The files where a server-local conversion may still be named in code: the definition of the tab's
       ToServerLocal (text only), and the drill-down file, which prints the heatmap drill's server-local time into
       its log line. */
    private static readonly string[] MayNameToServerLocal = ["ServerTab.xaml.cs", "ServerTab.DrillDown.cs"];

    private static IEnumerable<(string Name, string Code)> ServerTabCode()
    {
        var files = Directory.GetFiles(ControlsFolder(), "ServerTab*.cs");
        Assert.True(files.Length >= 10, $"expected the ServerTab partial files, found {files.Length}.");
        return files.Select(f => (Path.GetFileName(f), CodeOnly(File.ReadAllText(f))));
    }

    private static string Line(string code, int index) =>
        (code.Take(index).Count(c => c == '\n') + 1).ToString();

    /// <summary>
    /// A chart X is the instant itself: no conversion through the server's clock feeds <c>ToOADate()</c> in any
    /// ServerTab file. The sites that read <c>ToServerLocal(x).ToOADate()</c> are <c>x.ToOADate()</c>, and a series
    /// added later plots the instant it is given.
    /// </summary>
    [Fact]
    public void NoServerTabFile_ProjectsAChartXThroughTheServersClock()
    {
        var feed = new Regex(@"\b(?:ToServerLocal|ToServerTime)\s*\([^;{}]*\)\s*\.\s*ToOADate\s*\(");
        var offenders = new List<string>();
        foreach (var (name, code) in ServerTabCode())
        {
            foreach (Match m in feed.Matches(code))
            {
                offenders.Add($"{name} (code line {Line(code, m.Index)})");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "These plot a chart X on the server's wall clock; a chart plots the naive-UTC instant and only the " +
            "ticks and the hover apply the display zone: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The server-local conversion reaches no chart window or axis: <c>ToServerLocal(</c> is named only where the tab
    /// defines it and in the drill-down text, <c>ServerTimeHelper.ToServerTime(</c> is named nowhere, and neither is
    /// passed as a method group (<c>.Select(ToServerLocal)</c>). This is what keeps the custom-range axis bounds,
    /// which are assigned rather than fed to <c>ToOADate()</c>, in the UTC frame.
    /// </summary>
    [Fact]
    public void ServerLocalConversion_ReachesNoServerTabFileExceptItsDefinitionAndTheDrillText()
    {
        var offenders = new List<string>();
        foreach (var (name, code) in ServerTabCode())
        {
            if (!MayNameToServerLocal.Contains(name, StringComparer.OrdinalIgnoreCase))
            {
                foreach (Match m in Regex.Matches(code, @"\bToServerLocal\b"))
                {
                    offenders.Add($"{name} (code line {Line(code, m.Index)}): ToServerLocal");
                }
            }

            foreach (Match m in Regex.Matches(code, @"\bToServerTime\b"))
            {
                offenders.Add($"{name} (code line {Line(code, m.Index)}): ToServerTime");
            }

            foreach (Match m in Regex.Matches(code, @"\bToServerLocal\b(?!\s*\()"))
            {
                offenders.Add($"{name} (code line {Line(code, m.Index)}): ToServerLocal as a method group");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "These convert through the server's clock in a chart file; the window and the X values are UTC " +
            "instants: " + string.Join(", ", offenders.Distinct()));
    }

    /// <summary>
    /// Every chart draws its bottom axis with <c>DateTimeTicksBottomUtc(GetPickerZone)</c>: ticks at whole wall times
    /// of the display zone, worded in it, read again on each render so a mode switch relabels the axis. The old axis
    /// (<c>DateTimeTicksBottomDateChange</c>) put its ticks at whole wall times of the plotted X and converted only
    /// the label, so it is named in no ServerTab file.
    /// </summary>
    [Fact]
    public void NoServerTabFile_DrawsAnAxisWithTheServerLocalDateChangeTicks()
    {
        var offenders = new List<string>();
        var utcAxes = 0;
        foreach (var (name, code) in ServerTabCode())
        {
            foreach (Match m in Regex.Matches(code, @"\bDateTimeTicksBottomDateChange\s*\("))
            {
                offenders.Add($"{name} (code line {Line(code, m.Index)})");
            }

            utcAxes += Regex.Matches(code, @"\bDateTimeTicksBottomUtc\(GetPickerZone\)").Count;
        }

        Assert.True(
            offenders.Count == 0,
            "These draw the old axis, whose ticks sit on the plotted X instead of on the display zone's wall times; " +
            "use DateTimeTicksBottomUtc(GetPickerZone): " + string.Join(", ", offenders));
        Assert.True(utcAxes >= 40, $"expected the tab's charts to draw through DateTimeTicksBottomUtc, found {utcAxes}.");
    }

    /// <summary>
    /// Every <c>ChartHoverHelper</c> on the tab is given the display zone, so its label words the plotted UTC instant
    /// in the zone the ticks use. Without it the hover formats the raw X with the process-wide display mode, which
    /// for a UTC X is the wrong clock.
    /// </summary>
    [Fact]
    public void EveryChartHoverHelperInAServerTabFile_PassesTheDisplayZone()
    {
        var offenders = new List<string>();
        var built = 0;
        foreach (var (name, code) in ServerTabCode())
        {
            foreach (Match m in Regex.Matches(code, @"new\s+ChartHoverHelper\s*\(([^;]*?)\)\s*;"))
            {
                built++;
                if (!Regex.IsMatch(m.Groups[1].Value, @"\bdisplayZone\s*:\s*GetPickerZone\b"))
                {
                    offenders.Add($"{name} (code line {Line(code, m.Index)})");
                }
            }
        }

        Assert.True(built >= 30, $"expected the tab's hover helpers, found {built}.");
        Assert.True(
            offenders.Count == 0,
            "These build a ChartHoverHelper without displayZone: GetPickerZone: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The four shared chart renderers are built with the identity as their X projection and the display zone as
    /// their last argument, and plot the instant they are given. A projection through the server's clock would move
    /// every point while the ticks and the hover, which read the display zone, stayed put.
    /// </summary>
    [Fact]
    public void TheFourSharedRenderers_AreBuiltWithUnprojectedXAndTheDisplayZone()
    {
        var offenders = new List<string>();
        var built = 0;
        foreach (var (name, code) in ServerTabCode())
        {
            var pattern = @"new\s+(?:CpuSchedulerChartRenderer|GroupedTrendChartRenderer|SessionStatsChartRenderer|SystemHealthChartRenderer)\s*\(([^;]*?)\)\s*;";
            foreach (Match m in Regex.Matches(code, pattern))
            {
                built++;
                if (!Regex.IsMatch(m.Groups[1].Value, @"^\s*_chartHelper\s*,\s*t\s*=>\s*t\s*,\s*GetPickerZone\s*$"))
                {
                    offenders.Add($"{name} (code line {Line(code, m.Index)})");
                }
            }
        }

        Assert.Equal(4, built);
        Assert.True(
            offenders.Count == 0,
            "These renderers are not built as (_chartHelper, t => t, GetPickerZone): " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The server-local-to-UTC conversion the drills used is gone: a chart click is the UTC instant already, so
    /// nothing has a server-local time to convert back.
    /// </summary>
    [Fact]
    public void ToUtcFromServerLocal_IsGone()
    {
        var offenders = ServerTabCode()
            .Where(f => f.Code.Contains("ToUtcFromServerLocal", StringComparison.Ordinal))
            .Select(f => f.Name)
            .ToList();

        Assert.True(offenders.Count == 0, "still named in: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// The chart window is the UTC window, made by <c>TimeWindows.ChartAxis</c>. It does not ask the Overview timeline
    /// for its server-local window, and no other ServerTab file does either.
    /// </summary>
    [Fact]
    public void GetChartWindow_ReturnsTheUtcWindowAndNeverTheServerLocalOne()
    {
        var files = ServerTabCode().ToList();

        var callers = files
            .Where(f => f.Code.Contains("GetCurrentWindowServerLocal", StringComparison.Ordinal))
            .Select(f => f.Name)
            .ToList();
        Assert.True(callers.Count == 0, "asks for the server-local window in: " + string.Join(", ", callers));

        var timeRange = files.Single(f => f.Name == "ServerTab.TimeRange.cs").Code;
        var start = timeRange.IndexOf("internal static (DateTime Start, DateTime End) GetChartWindow(", StringComparison.Ordinal);
        Assert.True(start >= 0, "GetChartWindow is no longer in ServerTab.TimeRange.cs; update this pin.");
        var end = timeRange.IndexOf(';', start);
        var definition = timeRange[start..(end + 1)];

        Assert.Contains("TimeWindows.ChartAxis(hoursBack, fromUtc, toUtc, utcNow)", definition, StringComparison.Ordinal);
    }

    /// <summary>
    /// The block-chain window from the slicer is the slicer's selection as it is. <c>event_time</c> is the Extended
    /// Events <c>@timestamp</c>, so UTC, and the selection is a pair of UTC instants: passing them through the
    /// process-wide server clock read a window shifted by that server's offset.
    /// </summary>
    [Fact]
    public void TheBlockChainSlicerBranch_PassesTheSelectionAsUtcInstants()
    {
        var code = ServerTabCode().Single(f => f.Name == "ServerTab.BlockChain.cs").Code;

        Assert.Contains("var startUtc = BlockingSlicer.SelectionStartUtc;", code, StringComparison.Ordinal);
        Assert.Contains("start = startUtc.Value;", code, StringComparison.Ordinal);
        Assert.Contains("end = endUtc.Value;", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ToServerTime", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ToServerLocal", code, StringComparison.Ordinal);
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
