/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: a viewer chart's X value is the naive-UTC instant. The display mode applies to TEXT only: the tick labels
/// (<c>DateTimeTicksBottomUtc</c>), the hover and crosshair labels (a display zone passed to <c>ChartHoverHelper</c> and
/// set on the crosshair manager), and the CSV export. Ticks sit at whole wall times of the display zone. These source
/// pins read every file of the viewer, so a chart added later that plots a display-time X, or draws its axis or hover
/// in the old frame, fails here and not on a screen in another time zone.
/// </summary>
public sealed class ViewerChartUtcFrameTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));

    private static string ViewerDirectory()
        => Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer");

    private static string StripComments(string source)
        => Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", "", RegexOptions.Singleline);

    /// <summary>Every viewer source file with its comments removed (a pin reads code, not the prose around it).</summary>
    private static List<(string File, string Code)> ViewerSources()
    {
        var separator = Path.DirectorySeparatorChar;
        return Directory.EnumerateFiles(ViewerDirectory(), "*.cs", SearchOption.AllDirectories)
            .Where(p => !p.Contains($"{separator}obj{separator}") && !p.Contains($"{separator}bin{separator}"))
            .OrderBy(p => p, StringComparer.Ordinal)
            .Select(p => (Path.GetFileName(p), StripComments(File.ReadAllText(p))))
            .ToList();
    }

    /// <summary>The statement around <paramref name="index"/>: from the previous <c>;</c>, <c>{</c> or <c>}</c> to the
    /// next <c>;</c>.</summary>
    private static string StatementAround(string code, int index)
    {
        var start = code.LastIndexOfAny([';', '{', '}'], Math.Max(0, index - 1)) + 1;
        var end = code.IndexOf(';', index);
        return code[start..(end < 0 ? code.Length : end)];
    }

    /// <summary>The text between the parenthesis at <paramref name="open"/> and its match.</summary>
    private static string ArgumentsAt(string code, int open)
    {
        var depth = 0;
        for (var i = open; i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')' && --depth == 0)
            {
                return code[(open + 1)..i];
            }
        }

        return code[(open + 1)..];
    }

    /// <summary>A projection to display time feeds a chart when its statement turns the value into a plotted number or
    /// an axis limit, or assigns a window bound that becomes one.</summary>
    private static readonly Regex FeedsAChart = new(
        @"ToOADate\s*\(|SetLimitsX\s*\(|\b(?:rangeStart|rangeEnd|xMin|xMax|xStart|xEnd)\s*=",
        RegexOptions.CultureInvariant);

    [Fact]
    public void NoDisplayTimeProjectionFeedsAChartX()
    {
        var offenders = new List<string>();
        var forDisplayCalls = 0;

        foreach (var (file, code) in ViewerSources())
        {
            foreach (var call in Regex.Matches(code, @"\bForDisplay\s*\(").Cast<Match>())
            {
                forDisplayCalls++;
                if (FeedsAChart.IsMatch(StatementAround(code, call.Index)))
                {
                    offenders.Add($"{file}: {StatementAround(code, call.Index).Trim()}");
                }
            }

            /* A method group hands the renderer a projection to display time for every X it plots. */
            foreach (var group in Regex.Matches(code, @"ViewerTimeHelper\.ForDisplay\b\s*(?![\s(])").Cast<Match>())
            {
                offenders.Add($"{file}: ForDisplay used as a method group near '{StatementAround(code, group.Index).Trim()}'");
            }
        }

        Assert.True(forDisplayCalls >= 40, $"only {forDisplayCalls} ForDisplay calls were found; check the matcher");
        Assert.Empty(offenders);
    }

    [Fact]
    public void NoViewerChartUsesADisplayFrameAxis()
    {
        var offenders = new List<string>();
        var utcAxes = 0;

        foreach (var (file, code) in ViewerSources())
        {
            if (Regex.IsMatch(code, @"\.DateTimeTicksBottomDateChange\s*\("))
            {
                offenders.Add($"{file}: DateTimeTicksBottomDateChange");
            }

            if (Regex.IsMatch(code, @"\.DateTimeTicksBottom\s*\("))
            {
                offenders.Add($"{file}: DateTimeTicksBottom");
            }

            foreach (var axis in Regex.Matches(code, @"\.DateTimeTicksBottomUtc\s*\(").Cast<Match>())
            {
                utcAxes++;
                if (!ArgumentsAt(code, axis.Index + axis.Length - 1).Contains("ViewerTimeHelper.CurrentDisplayZone", StringComparison.Ordinal))
                {
                    offenders.Add($"{file}: DateTimeTicksBottomUtc without the current display zone");
                }
            }
        }

        Assert.True(utcAxes >= 57, $"only {utcAxes} UTC time axes were found, expected at least 57");
        Assert.Empty(offenders);
    }

    [Fact]
    public void EveryChartHoverHelperIsBuiltWithTheDisplayZone()
    {
        var offenders = new List<string>();
        var hovers = 0;

        foreach (var (file, code) in ViewerSources())
        {
            foreach (var hover in Regex.Matches(code, @"new ChartHoverHelper\s*\(").Cast<Match>())
            {
                hovers++;
                var arguments = ArgumentsAt(code, hover.Index + hover.Length - 1);
                if (!Regex.IsMatch(arguments, @"displayZone\s*:\s*ViewerTimeHelper\.CurrentDisplayZone\b"))
                {
                    offenders.Add($"{file}: new ChartHoverHelper({arguments.Trim()})");
                }
            }
        }

        Assert.True(hovers >= 45, $"only {hovers} hover helpers were found, expected at least 45");
        Assert.Empty(offenders);
    }

    [Fact]
    public void TheOverviewLanesCrosshairLabelsInTheDisplayZone()
    {
        var code = ViewerSources().Single(s => s.File == "CorrelatedTimelineLanesControl.xaml.cs").Code;

        Assert.Matches(
            @"new CorrelatedCrosshairManager\s*\{\s*DisplayZoneProvider\s*=\s*ViewerTimeHelper\.CurrentDisplayZone\s*\}",
            code);
    }

    [Fact]
    public void TheSharedRenderersPlotTheInstantAndDrawInTheDisplayZone()
    {
        var offenders = new List<string>();
        var renderers = 0;

        foreach (var (file, code) in ViewerSources())
        {
            foreach (var renderer in Regex.Matches(
                code, @"new (?:CpuSchedulerChartRenderer|GroupedTrendChartRenderer|SessionStatsChartRenderer|SystemHealthChartRenderer)\s*\(").Cast<Match>())
            {
                renderers++;
                var arguments = ArgumentsAt(code, renderer.Index + renderer.Length - 1);
                if (arguments.Contains("ForDisplay", StringComparison.Ordinal)
                    || !Regex.IsMatch(arguments, @",\s*ViewerTimeHelper\.CurrentDisplayZone\s*$"))
                {
                    offenders.Add($"{file}: new renderer({arguments.Trim()})");
                }
            }
        }

        Assert.Equal(4, renderers);
        Assert.Empty(offenders);
    }

    [Fact]
    public void TheCsvExportWritesTheTimesTheChartLabelsShow()
    {
        var code = ViewerSources().Single(s => s.File == "ViewerServerTab.ChartContextMenu.cs").Code;

        Assert.Contains("DisplayZone.ToDisplay(DateTime.FromOADate(point.X), zone)", code, StringComparison.Ordinal);
        Assert.DoesNotContain("FormatChartCsvLine(DateTime.FromOADate(", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDisplayToUtcInversesAreGone_BecauseTheChartXIsAlreadyUtc()
    {
        var names = typeof(ViewerTimeHelper)
            .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
            .Select(m => m.Name)
            .ToHashSet(StringComparer.Ordinal);

        Assert.DoesNotContain("DisplayToNaiveUtc", names);
        Assert.DoesNotContain("ConvertFromDisplay", names);

        /* Nothing else in the viewer reads a plotted X and converts it back from a display frame. */
        foreach (var (file, code) in ViewerSources())
        {
            Assert.False(code.Contains("DisplayToNaiveUtc", StringComparison.Ordinal), $"{file} still names DisplayToNaiveUtc");
            Assert.False(code.Contains("ConvertFromDisplay", StringComparison.Ordinal), $"{file} still names ConvertFromDisplay");
        }
    }
}
