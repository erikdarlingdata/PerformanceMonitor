/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for the Plan Insights card system (issue #4573): the quiet/non-quiet state helper values,
/// and a census that the insight-card markup carries no inline hex colours.
/// </summary>
public class Viewer4573Tests
{
    [Fact]
    public void AccentOpacity_MatchesPerformanceStudios0Point35QuietValue()
    {
        // PerformanceStudio's Border.insightAccent.empty style sets Opacity to 0.35
        // (erikdarlingdata/PerformanceStudio@87bad14).
        Assert.Equal(0.35, InsightCardStyle.AccentOpacity(isEmpty: true));
        Assert.Equal(1.0, InsightCardStyle.AccentOpacity(isEmpty: false));
    }

    [Fact]
    public void HeaderUsesMutedForeground_IsTrueOnlyWhenEmpty()
    {
        Assert.True(InsightCardStyle.HeaderUsesMutedForeground(isEmpty: true));
        Assert.False(InsightCardStyle.HeaderUsesMutedForeground(isEmpty: false));
    }

    [Fact]
    public void QuietAndNormalAccentOpacity_AreMutuallyExclusiveExtremes()
    {
        // Guards against a future edit collapsing the two constants to the same value, which
        // would make the quiet state visually indistinguishable from the normal one.
        Assert.NotEqual(InsightCardStyle.QuietAccentOpacity, InsightCardStyle.NormalAccentOpacity);
        Assert.True(InsightCardStyle.QuietAccentOpacity < InsightCardStyle.NormalAccentOpacity);
    }

    [Fact]
    public void InsightCardMarkup_CarriesNoInlineHexColour()
    {
        var repoRoot = FindRepoRoot();
        var xamlPath = Path.Combine(repoRoot, "PerformanceMonitor.Ui", "PlanViewerControl.xaml");
        var text = File.ReadAllText(xamlPath);

        var start = text.IndexOf("Plan Insights cards: one visual system", System.StringComparison.Ordinal);
        var end = text.IndexOf("</Grid>", start, System.StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "Could not locate the Plan Insights card block in PlanViewerControl.xaml");

        var cardBlock = text.Substring(start, end - start);
        var hexHit = Regex.Match(cardBlock, "#[0-9A-Fa-f]{6}");
        Assert.False(hexHit.Success, $"Found inline hex colour '{hexHit.Value}' in the Plan Insights card markup; use a DynamicResource token instead.");
    }

    private static string FindRepoRoot()
    {
        var dir = Directory.GetCurrentDirectory();
        while (dir != null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
            dir = Directory.GetParent(dir)?.FullName;
        Assert.NotNull(dir);
        return dir!;
    }
}
