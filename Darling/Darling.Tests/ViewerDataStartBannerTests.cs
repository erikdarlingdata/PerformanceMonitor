/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's server tab says where the data starts on the two surfaces that read the 7-day tables over
/// the tab's range: Active Queries (query_snapshots) and Blocking's Current Waits (waiting_tasks). A 30-day custom
/// range used to draw 7 days on both with nothing to say so. Each asks the shared probe
/// (<see cref="PerformanceMonitor.Darling.Storage.DataWindowFloor"/>) where the server's coverage starts for the
/// range (the later of its first collection and the table's retention edge, moved earlier by a row in the range,
/// so a quiet start is not a cut) and shows the Queries tab's "Showing since" banner when that is after the
/// range's start.
/// </summary>
public sealed class ViewerDataStartBannerTests
{
    private static string ViewerFile(string file) => ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file);

    private static int Count(string source, string text) => Regex.Matches(source, Regex.Escape(text)).Count;

    [Fact]
    public void ActiveQueries_ShowsWhereTheSnapshotsStart_OnEveryReadOfTheGrid()
    {
        var tab = ViewerFile("ViewerServerTab.ActiveQueries.cs");

        /* The range load, the deep-link load, and the slicer re-read each draw the grid, so each sets the banner. */
        Assert.Equal(3, Count(tab, "_dataService.GetQuerySnapshotsDataStartAsync("));
        Assert.Equal(3, Count(tab, "UpdateTruncationBanner(QuerySnapshotsTruncationBanner, "));
        Assert.Contains("x:Name=\"QuerySnapshotsTruncationBanner\"", ViewerFile("ViewerServerTab.xaml"), StringComparison.Ordinal);
    }

    [Fact]
    public void CurrentWaits_ShowsWhereTheWaitingTasksStart()
    {
        var tab = ViewerFile("ViewerServerTab.Blocking.cs");

        Assert.Contains("_dataService.GetWaitingTasksDataStartAsync(", tab, StringComparison.Ordinal);
        Assert.Contains("UpdateTruncationBanner(CurrentWaitsTruncationBanner, ", tab, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"CurrentWaitsTruncationBanner\"", ViewerFile("ViewerServerTab.xaml"), StringComparison.Ordinal);
    }

    [Fact]
    public void BothReads_GoThroughTheSharedProbe()
    {
        var snapshots = ViewerFile("ViewerDataService.QuerySnapshots.cs");
        Assert.Contains("DataWindowFloor.GetForServerAsync(", snapshots, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"query_snapshots\")", snapshots, StringComparison.Ordinal);

        var waits = ViewerFile("ViewerDataService.BlockingTrends.cs");
        Assert.Contains("DataWindowFloor.GetForServerAsync(", waits, StringComparison.Ordinal);
        Assert.Contains("DataWindowFloor.Source.ForCollectorTable(\"waiting_tasks\")", waits, StringComparison.Ordinal);
    }
}
