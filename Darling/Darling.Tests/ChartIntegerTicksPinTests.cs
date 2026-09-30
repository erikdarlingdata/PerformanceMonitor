/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A chart of COUNTS puts its y gridlines on whole numbers.
///
/// <para><b>The defect.</b> <c>niceScale</c> picks a "nice" step for the data's range, and below a range of about
/// five that step is 0.2 or 0.5. The Overview's Blocking Events and Deadlocks charts print their tick labels
/// through a whole-number formatter, so a 0-1 axis drew six gridlines labelled "1 1 1 0 0 0" - fractional ticks
/// rounded to integers with nothing to tell the rounded ones apart.</para>
///
/// <para><b>The fix.</b> The chart library gains an <c>integerTicks</c> option (and <c>series2.integerTicks</c>
/// for the dual-axis overlay) that holds the step at 1 or more, so a small-count axis reads 0, 1, 2. Nothing else
/// changes: without the option <c>niceScale</c> is byte-for-byte what it was. Whoever declares that a series is a
/// count now gets it: a line panel with <c>format: "int"</c> (the Blocking Events and Deadlocks charts on the
/// Overview and Locking tabs, the Blocked Sessions chart, and the Blocking and deadlocks starter dashboard), and a
/// composed panel whose unit is <c>count</c>.</para>
///
/// <para>This repository carries no JavaScript test runner, so these are source pins over the shipped modules (the
/// <see cref="ChartWindowDomainTests"/> pattern). The scale itself was also run under Node on the shipped source:
/// a 0-1 axis gives ticks 0 and 1, 0-2 gives 0, 1, 2, and for every maximum from 1 to 5000 the ticks are whole,
/// distinct and cover the maximum.</para>
/// </summary>
public sealed class ChartIntegerTicksPinTests
{
    private static string Js(params string[] path) => ReadRepoFileLf(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", Path.Combine(path)));

    /// <summary>The chart library owns the option: it is read off the spec, reaches the primary scale and the
    /// overlay's own scale, and in <c>niceScale</c> it only ever raises the step to a whole number of at least 1.</summary>
    [Fact]
    public void TheChartLibrary_HasAnIntegerTickOption_ThatNeverStepsBelowOne()
    {
        var charts = Js("charts.js");

        Assert.Contains("onZoom = null, integerTicks = false, windowStart = null, windowEnd = null } = spec;", charts, StringComparison.Ordinal);
        Assert.Contains("niceScale(dataMin, dataMax, Y_TICKS, clampMax, integerTicks)", charts, StringComparison.Ordinal);
        Assert.Contains("niceScale(n2, m2, Y_TICKS, null, series2.integerTicks === true)", charts, StringComparison.Ordinal);

        var scale = Regex.Match(charts, @"function niceScale\(min, max, maxTicks, clampMax, integer = false\) \{\n(?<body>.*?)\n\}\n", RegexOptions.Singleline);
        Assert.True(scale.Success, "niceScale(min, max, maxTicks, clampMax, integer = false) is no longer declared in charts.js.");
        Assert.Contains("const step = integer ? Math.max(1, Math.ceil(niceStep)) : niceStep;", scale.Groups["body"].Value, StringComparison.Ordinal);

        /* Every other caller is untouched: the scatter axes pass no fifth argument. */
        Assert.Contains("niceScale(0, xMax > 0 ? xMax : 1, Y_TICKS, null)", charts, StringComparison.Ordinal);
        Assert.Contains("niceScale(0, yMax > 0 ? yMax : 1, Y_TICKS, null)", charts, StringComparison.Ordinal);
    }

    /// <summary>A line panel that declares <c>format: "int"</c> is a count chart: the shared <c>vizLine</c> hands
    /// the chart library the option, and every count chart the server page draws declares the format.</summary>
    [Fact]
    public void EveryCountChartOnTheServerPage_DeclaresWholeNumbers_AndVizLinePassesItOn()
    {
        Assert.Contains("integerTicks: desc.format === \"int\",", Js("panels.js"), StringComparison.Ordinal);

        var tabs = Js("pages", "server-tabs.js");

        /* The four Blocking Events / Deadlocks line panels (Overview and Locking tabs): each one that charts
           COUNT_SERIES carries the format, counted against the panels that chart it. */
        var countPanels = Regex.Matches(tabs, @"COUNT_SERIES, \{").Count;
        Assert.Equal(4, countPanels);
        Assert.Equal(countPanels, Regex.Matches(tabs, @"COUNT_SERIES, \{\n\s+subtitle: ctx\.label,\n\s+format: ""int"",").Count);

        /* The Blocked Sessions chart is a fanout spec rather than a line() call. */
        Assert.Matches(@"series: BLOCKED_SESSION_SERIES,\n\s+format: ""int"",", tabs);
    }

    /// <summary>The starter dashboard that charts the same two count trends declares them whole-number too, and a
    /// composed panel on a count measure asks the chart for whole-number ticks on both axes.</summary>
    [Fact]
    public void TheStarterDashboardAndComposedCountCharts_AskForWholeNumberTicks()
    {
        var templates = Js("view-templates.js");
        Assert.Matches(@"series: \[\{ key: ""count"", label: ""Events"" \}\],\n\s+format: ""int"",", templates);
        Assert.Matches(@"series: \[\{ key: ""count"", label: ""Deadlocks"" \}\],\n\s+format: ""int"",", templates);

        var compose = Js("compose.js");
        Assert.Contains("integerTicks: unit === \"count\",", compose, StringComparison.Ordinal);
        Assert.Contains("integerTicks: panelSpec.overlay.unit === \"count\",", compose, StringComparison.Ordinal);
    }
}
