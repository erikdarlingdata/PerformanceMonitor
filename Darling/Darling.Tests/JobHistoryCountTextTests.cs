/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4966: the Job History tab's count text keeps its cap label whenever the READ reached the cap. The viewer's column-filter
/// handler used to rewrite the text as a bare "N run(s)", so applying a column filter took "showing the newest 2,000" off a
/// count whose read was still cut at 2,000. The text is built once, in <see cref="JobHistoryCap.CountText"/>, and every
/// writer of it (the load and the filter handler) goes through it with the row count of the last read. The two WPF tabs
/// cannot be instantiated here, so the writers are pinned from their source, the way <c>LiteJobHistoryLoadingCapTests</c>
/// pins Lite's.
/// </summary>
public sealed class JobHistoryCountTextTests
{
    private const int Cap = 2000;

    [Fact]
    public void NothingShown_IsEmpty()
    {
        Assert.Equal("", JobHistoryCap.CountText(0, Cap, Cap));
        Assert.Equal("", JobHistoryCap.CountText(0, 3, Cap));
    }

    [Fact]
    public void ReadBelowTheCap_IsJustTheCount()
    {
        Assert.Equal("12 run(s)", JobHistoryCap.CountText(12, 1500, Cap));
    }

    [Fact]
    public void ReadAtTheCap_KeepsTheLabel_WhateverTheFiltersLeaveShown()
    {
        /* The load's own count, and what a column filter leaves of the same read: the label is about the read. */
        Assert.Equal("2000 run(s) (showing the newest 2,000)", JobHistoryCap.CountText(2000, 2000, Cap));
        Assert.Equal("7 run(s) (showing the newest 2,000)", JobHistoryCap.CountText(7, 2000, Cap));
        Assert.Equal("1 run(s) (showing the newest 2,000)", JobHistoryCap.CountText(1, 2000, Cap));
    }

    [Fact]
    public void ViewerTab_EveryCountTextWriteGoesThroughTheSharedHelper()
    {
        AssertEveryWriteGoesThroughTheHelper(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "JobHistoryTab.xaml.cs"), "Darling viewer");
    }

    [Fact]
    public void LiteTab_EveryCountTextWriteGoesThroughTheSharedHelper()
    {
        AssertEveryWriteGoesThroughTheHelper(RepoFile.ReadRepoFile("Lite", "Controls", "JobHistoryTab.xaml.cs"), "Lite");
    }

    /// <summary>The viewer's column-filter handler rebuilds the count from the row count of the last read, not from the grid
    /// alone: a handler that only knew the grid's count could not say whether the read hit the cap.</summary>
    [Fact]
    public void ViewerTab_ColumnFilterHandler_BuildsTheCountFromTheLastReadsRowCount()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "JobHistoryTab.xaml.cs"));

        var head = stripped.IndexOf("private void FilterPopup_FilterApplied(", StringComparison.Ordinal);
        Assert.True(head >= 0, "FilterPopup_FilterApplied was not found in the viewer's JobHistoryTab.xaml.cs");
        var handler = CSharpSourceWalker.BraceBalanced(stripped, stripped.IndexOf('{', head));
        Assert.Matches(
            @"JobCountIndicator\s*\.\s*Text\s*=\s*JobHistoryCap\s*\.\s*CountText\s*\(\s*JobHistoryDataGrid\s*\.\s*Items\s*\.\s*Count\s*,\s*_lastReadRowCount\s*,\s*RowCap\s*\)\s*;",
            handler);

        /* The load records the read's row count (all.Count, before the Status/Category/column filters) for the handler. */
        var load = CSharpSourceWalker.BraceBalanced(stripped, stripped.IndexOf('{', stripped.IndexOf("private async Task LoadJobsAsync()", StringComparison.Ordinal)));
        Assert.Matches(@"_lastReadRowCount\s*=\s*all\s*\.\s*Count\s*;", load);
    }

    private static void AssertEveryWriteGoesThroughTheHelper(string source, string tab)
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(source);
        var writes = Regex.Matches(stripped, @"JobCountIndicator\s*\.\s*Text\s*=\s*(?<rhs>[^;]*);").Cast<Match>().ToList();

        Assert.NotEmpty(writes);
        foreach (var write in writes)
        {
            Assert.True(
                Regex.IsMatch(write.Groups["rhs"].Value.TrimStart(), @"^JobHistoryCap\s*\.\s*CountText\s*\("),
                $"The {tab} Job History tab writes its count text without JobHistoryCap.CountText, so it can lose the cap label: {write.Value.Trim()}");
        }
    }
}
