/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using Choice = PerformanceMonitor.Common.JobHistoryFilterChoices.ServerChoice;

namespace Darling.Tests;

/// <summary>
/// #5703 (round 1): the Job History tab's Server and Category drop-downs only grow. The merge behind them is a pure helper
/// (<see cref="JobHistoryFilterChoices"/>), so the behaviour is tested here as behaviour and not pinned as source text: what is
/// already on offer stays, the monitored servers and the window's categories are added, the rows win on a renamed server, the
/// selected choice is always kept, and the "All" entry maps to no filter. Lite runs the same helper (its twin test is in Lite.Tests).
/// </summary>
public sealed class JobHistoryFilterChoicesTests
{
    [Fact]
    public void Servers_KeepWhatIsOffered_AddTheKnownOnes_AndTheRowsNameWins()
    {
        var offered = new[] { new Choice(1, "old-name"), new Choice(2, "kept") };
        var known = new[] { new Choice(1, "registry-name"), new Choice(3, "quiet") };
        var rows = new[] { new Choice(1, "row-name"), new Choice(1, "row-name") };

        var merged = JobHistoryFilterChoices.MergeServers(offered, known, rows);

        Assert.Equal(new[] { new Choice(2, "kept"), new Choice(3, "quiet"), new Choice(1, "row-name") }.OrderBy(c => c.ServerName).ToArray(), merged.ToArray());
    }

    [Fact]
    public void Servers_ABusyServersRowsAloneNeverDropTheQuietServer()
    {
        /* All servers, no filter: the newest runs are the busy server's, so the rows name only it. The monitored list adds the quiet one. */
        var rows = new[] { new Choice(10, "busy") };
        var known = new[] { new Choice(10, "busy"), new Choice(20, "quiet") };

        Assert.Equal(new[] { 10, 20 }, JobHistoryFilterChoices.MergeServers([], known, rows).Select(c => c.ServerId).ToArray());

        /* And a later read that holds no rows at all (a filter that matched nothing) shrinks nothing. */
        var first = JobHistoryFilterChoices.MergeServers([], known, rows);
        Assert.Equal(first, JobHistoryFilterChoices.MergeServers(first, known, []));
    }

    [Fact]
    public void Servers_TheSelectedChoiceIsAlwaysKept_AndANamelessServerIsIgnored()
    {
        var merged = JobHistoryFilterChoices.MergeServers([], [new Choice(1, "a"), new Choice(2, "")], [new Choice(3, "")], new Choice(9, "picked"));

        Assert.Equal(new[] { 1, 9 }, merged.Select(c => c.ServerId).ToArray());
        Assert.Equal(new[] { 1 }, JobHistoryFilterChoices.MergeServers([], [new Choice(1, "a")], [], new Choice(1, "a")).Select(c => c.ServerId).ToArray());
    }

    [Fact]
    public void Servers_AreOrderedByNameIgnoringCase()
    {
        var merged = JobHistoryFilterChoices.MergeServers([], [new Choice(1, "beta"), new Choice(2, "Alpha"), new Choice(3, "gamma")], []);

        Assert.Equal(new[] { "Alpha", "beta", "gamma" }, merged.Select(c => c.ServerName).ToArray());
    }

    [Fact]
    public void Categories_KeepWhatIsOffered_AddTheKnownOnes_AndMergeSpellingsIgnoringCase()
    {
        var merged = JobHistoryFilterChoices.MergeCategories(
            ["Maintenance", "", null], ["maintenance", "Replication", "Backup"], ["REPLICATION", null, "Quiet category"]);

        /* The first spelling seen wins, so a choice already on offer keeps its text. */
        Assert.Equal(new[] { "Backup", "Maintenance", "Quiet category", "Replication" }, merged.ToArray());
    }

    [Fact]
    public void Categories_ABusyServersRowsAloneNeverDropTheQuietServersCategory_AndTheSelectedOneIsKept()
    {
        var merged = JobHistoryFilterChoices.MergeCategories([], ["Busy", "Quiet"], ["Busy"]);
        Assert.Equal(new[] { "Busy", "Quiet" }, merged.ToArray());

        /* A refresh whose known list failed (empty) and whose rows are filtered away still holds the selection and the earlier choices. */
        Assert.Equal(new[] { "Busy", "Picked", "Quiet" }, JobHistoryFilterChoices.MergeCategories(merged, [], [], "Picked").ToArray());
    }

    [Fact]
    public void TheAllEntryMapsToNoFilter_AndAChoiceMapsToItsValue()
    {
        Assert.Null(JobHistoryFilterChoices.ServerFilter(0, "5"));
        Assert.Null(JobHistoryFilterChoices.ServerFilter(-1, "5"));
        Assert.Null(JobHistoryFilterChoices.ServerFilter(1, null));
        Assert.Null(JobHistoryFilterChoices.ServerFilter(1, "not a number"));
        Assert.Equal(5, JobHistoryFilterChoices.ServerFilter(1, "5"));

        Assert.Null(JobHistoryFilterChoices.StatusFilter(0, "0"));
        Assert.Equal(0, JobHistoryFilterChoices.StatusFilter(1, "0"));
        Assert.Equal(3, JobHistoryFilterChoices.StatusFilter(4, "3"));

        Assert.Null(JobHistoryFilterChoices.CategoryFilter(0, "Backup"));
        Assert.Null(JobHistoryFilterChoices.CategoryFilter(2, ""));
        Assert.Null(JobHistoryFilterChoices.CategoryFilter(2, null));
        Assert.Equal("Backup", JobHistoryFilterChoices.CategoryFilter(2, "Backup"));
    }

    [Fact]
    public void TheViewersKnownServers_AreTheSqlServerTargets_ByDisplayName()
    {
        var choices = ViewerDataService.ServerChoicesOf(
        [
            new DarlingServer(1, "sql-a", "Alias A", true, 16),
            new DarlingServer(2, "sql-b", "", true, 16),
            new DarlingServer(3, "pg-a", "pg-a", true, null, engineKind: "postgres"),
        ]);

        Assert.Equal(new[] { new Choice(1, "Alias A"), new Choice(2, "sql-b") }, choices.ToArray());
    }
}
