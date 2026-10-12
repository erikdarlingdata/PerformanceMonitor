/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Common;
using Xunit;
using Choice = PerformanceMonitor.Common.JobHistoryFilterChoices.ServerChoice;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5703 (round 1): Lite's Job History Server and Category drop-downs only grow, through the same pure merge the Darling viewer
/// uses (<see cref="JobHistoryFilterChoices"/>). The behaviour is tested here, not pinned as source text: what is already on offer
/// stays, the configured servers and the window's categories are added, the rows win on a renamed server (the tab swaps the config
/// layer's alias into the rows before the merge), the selected choice is always kept, and the "All" entry maps to no filter.
/// </summary>
public sealed class JobHistoryFilterChoicesTests
{
    [Fact]
    public void Servers_KeepWhatIsOffered_AddTheConfiguredOnes_AndTheRowsNameWins()
    {
        var offered = new[] { new Choice(1, "old-name"), new Choice(2, "kept") };
        var known = new[] { new Choice(1, "config-name"), new Choice(3, "quiet") };
        var rows = new[] { new Choice(1, "row-name") };

        var merged = JobHistoryFilterChoices.MergeServers(offered, known, rows);

        Assert.Equal(new[] { new Choice(2, "kept"), new Choice(3, "quiet"), new Choice(1, "row-name") }, merged.ToArray());
    }

    [Fact]
    public void Servers_ABusyServersRowsAloneNeverDropTheQuietServer_AndAnEmptyReadShrinksNothing()
    {
        var rows = new[] { new Choice(10, "busy") };
        var known = new[] { new Choice(10, "busy"), new Choice(20, "quiet") };

        var first = JobHistoryFilterChoices.MergeServers([], known, rows);
        Assert.Equal(new[] { 10, 20 }, first.Select(c => c.ServerId).ToArray());
        Assert.Equal(first, JobHistoryFilterChoices.MergeServers(first, [], []));
    }

    [Fact]
    public void Servers_TheSelectedChoiceIsAlwaysKept_AndANamelessServerIsIgnored()
    {
        var merged = JobHistoryFilterChoices.MergeServers([], [new Choice(1, "a"), new Choice(2, "")], [new Choice(3, "")], new Choice(9, "picked"));

        Assert.Equal(new[] { 1, 9 }, merged.Select(c => c.ServerId).ToArray());
    }

    [Fact]
    public void Categories_KeepWhatIsOffered_AddTheKnownOnes_AndMergeSpellingsIgnoringCase()
    {
        var merged = JobHistoryFilterChoices.MergeCategories(
            ["Maintenance", "", null], ["maintenance", "Replication", "Backup"], ["REPLICATION", null, "Quiet category"]);

        Assert.Equal(new[] { "Backup", "Maintenance", "Quiet category", "Replication" }, merged.ToArray());
    }

    [Fact]
    public void Categories_ABusyServersRowsAloneNeverDropTheQuietServersCategory_AndTheSelectedOneIsKept()
    {
        var merged = JobHistoryFilterChoices.MergeCategories([], ["Busy", "Quiet"], ["Busy"]);
        Assert.Equal(new[] { "Busy", "Quiet" }, merged.ToArray());
        Assert.Equal(new[] { "Busy", "Picked", "Quiet" }, JobHistoryFilterChoices.MergeCategories(merged, [], [], "Picked").ToArray());
    }

    [Fact]
    public void TheAllEntryMapsToNoFilter_AndAChoiceMapsToItsValue()
    {
        Assert.Null(JobHistoryFilterChoices.ServerFilter(0, "5"));
        Assert.Null(JobHistoryFilterChoices.ServerFilter(1, "not a number"));
        Assert.Equal(5, JobHistoryFilterChoices.ServerFilter(1, "5"));

        Assert.Null(JobHistoryFilterChoices.StatusFilter(0, "0"));
        Assert.Equal(0, JobHistoryFilterChoices.StatusFilter(1, "0"));

        Assert.Null(JobHistoryFilterChoices.CategoryFilter(0, "Backup"));
        Assert.Null(JobHistoryFilterChoices.CategoryFilter(2, ""));
        Assert.Equal("Backup", JobHistoryFilterChoices.CategoryFilter(2, "Backup"));
    }
}
