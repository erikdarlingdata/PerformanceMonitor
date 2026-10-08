/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The server tabs of Lite and of the Darling Viewer (a copy of Lite's) say why a grid or chart is empty, and their
/// Configuration grids show whole column headers. The walk of the release found a blank Trace Flags grid, a Query Store
/// Regressions grid that said "No data for the selected time range." at Last 7 days while Last 24 hours listed rows, Configuration
/// headers cut off at the default width ("Configured Valu", "Dynami", "Compat Le"), and two Blocking charts that looked
/// broken because they hold points only where a collection found a waiting task.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class ServerTabEmptyStatesAndHeadersTests
{
    private static string LiteXaml() => RepoFile.ReadRepoFile("Lite", "Controls", "ServerTab.xaml");

    private static string ViewerXaml() => RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.xaml");

    public static IEnumerable<object[]> BothTabs() => new[] { new object[] { "Lite" }, new object[] { "Viewer" } };

    private static string Xaml(string app) => app == "Lite" ? LiteXaml() : ViewerXaml();

    /// <summary>One text column of the Configuration tab: its header words and its Width in the XAML.</summary>
    private static List<(string Header, int Width)> ConfigurationColumns(string xaml)
    {
        var start = xaml.IndexOf("x:Name=\"ServerConfigGrid\"", StringComparison.Ordinal);
        var end = xaml.IndexOf("x:Name=\"TraceFlagsNoDataMessage\"", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "the Configuration tab's grids were not found");

        return Regex.Matches(
                xaml[start..end],
                @"<DataGridTextColumn\b[^>]*?\bWidth=""(\d+)""[^>]*>\s*<DataGridTextColumn\.Header>.*?<TextBlock Text=""([^""]+)""",
                RegexOptions.Singleline)
            .Select(m => (m.Groups[2].Value, int.Parse(m.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture)))
            .ToList();
    }

    /// <summary>
    /// A bold header with a filter button needs about 6 pixels a character plus 50 for the button and the padding. The walk saw
    /// "Configured Value" cut to 15 characters at 130, "Dynamic" to 6 at 80 and "Compat Level" to 9 at 100.
    /// </summary>
    [Theory]
    [MemberData(nameof(BothTabs))]
    public void ConfigurationHeaders_FitTheirColumns(string app)
    {
        var columns = ConfigurationColumns(Xaml(app));

        Assert.True(columns.Count >= 30, $"{app}: expected the Configuration grids' columns, found {columns.Count}");

        var clipped = columns
            .Where(c => c.Width < (c.Header.Length * 6) + 50)
            .Select(c => $"{c.Header} ({c.Width})")
            .ToList();

        Assert.True(clipped.Count == 0, $"{app}: headers wider than their columns: {string.Join(", ", clipped)}");
    }

    [Theory]
    [MemberData(nameof(BothTabs))]
    public void TraceFlagsGrid_SaysNoTraceFlagsAreEnabled_WhenItHasNoRows(string app)
    {
        var xaml = Xaml(app);
        var at = xaml.IndexOf("x:Name=\"TraceFlagsNoDataMessage\"", StringComparison.Ordinal);
        Assert.True(at > 0);
        var element = xaml[at..xaml.IndexOf("/>", at, StringComparison.Ordinal)];

        Assert.Contains("Text=\"No trace flags are enabled on this server.\"", element, StringComparison.Ordinal);

        var code = app == "Lite"
            ? RepoFile.ReadRepoFile("Lite", "Controls", "ServerTab.Refresh.cs")
            : RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Filters.cs");
        var call = Regex.Match(code, @"ShowEngineGap(?:Async)?\(TraceFlagsNoDataMessage[^;]*;");

        Assert.True(call.Success, $"{app}: the Trace Flags load no longer calls the engine-gap step");
        Assert.Contains("keepsOwnEmptyText: true", call.Value, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreRegressionsGrid_SaysWhyItCanBeEmpty()
    {
        var text = ViewerServerTab.QueryStoreRegressionsEmptyText;

        Assert.Contains("7 days before", text, StringComparison.Ordinal);
        Assert.DoesNotContain("No data for the selected time range", text, StringComparison.Ordinal);

        var filters = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Filters.cs");
        Assert.Contains("EmptyState.SetText(QueryStoreRegressionsGrid, QueryStoreRegressionsEmptyText)", filters, StringComparison.Ordinal);
    }

    /// <summary>Lite has no Query Store Regressions grid (only the MCP tool reads that comparison), so only the Viewer needs the words.</summary>
    [Fact]
    public void Lite_HasNoQueryStoreRegressionsGrid()
    {
        Assert.DoesNotContain("QueryStoreRegressionsGrid", LiteXaml(), StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(BothTabs))]
    public void CurrentWaitsCharts_SayPointsAppearOnlyWhereTasksWereWaiting(string app)
    {
        var xaml = Xaml(app);
        var at = xaml.IndexOf("x:Name=\"CurrentWaitsSparseNote\"", StringComparison.Ordinal);
        Assert.True(at > 0, $"{app}: the Current Waits tab has no sparse-data note");

        var tab = xaml.IndexOf("<TabItem Header=\"Current Waits\">", StringComparison.Ordinal);
        var tabEnd = xaml.IndexOf("</TabItem>", tab, StringComparison.Ordinal);
        Assert.InRange(at, tab, tabEnd);

        Assert.Contains("Points appear only when a collection found waiting or blocked tasks.", xaml[at..], StringComparison.Ordinal);
    }
}
