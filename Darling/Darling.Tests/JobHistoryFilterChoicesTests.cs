/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Collectors;
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
    public void Servers_AreOrderedByName_WithTheDefaultComparerTheOldCodeUsed()
    {
        string[] names = ["beta", "Alpha", "gamma", "a_b", "AB", "ab", "Zed"];
        var merged = JobHistoryFilterChoices.MergeServers([], names.Select((n, i) => new Choice(i + 1, n)), []);

        /* The old tab sorted OrderBy(ServerName) with the default string comparer, not OrdinalIgnoreCase: same names, same order. */
        Assert.Equal(names.OrderBy(n => n).ToArray(), merged.Select(c => c.ServerName).ToArray());
    }

    [Fact]
    public void Categories_KeepWhatIsOffered_AddTheKnownOnes_AndTheRowsSpellingWinsIgnoringCase()
    {
        var merged = JobHistoryFilterChoices.MergeCategories(
            ["Maintenance", "", null], ["maintenance", "Replication", "Backup"], ["REPLICATION", null, "Quiet category"]);

        /* The rows' spelling wins over the known one, which wins over one already on offer (the tab rebuilt the list from the rows before). */
        Assert.Equal(new[] { "Backup", "maintenance", "Quiet category", "REPLICATION" }, merged.ToArray());
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
    public void TheViewersKnownServers_AreTheServersAJobHistoryCanBeAskedOf_ByDisplayName()
    {
        var choices = ViewerDataService.ServerChoicesOf(
        [
            new DarlingServer(1, "sql-a", "Alias A", true, 16),
            new DarlingServer(2, "sql-b", "", true, 16),
            new DarlingServer(3, "pg-a", "pg-a", true, null, engineKind: "postgres"),
            /* A disabled server is never collected, so it is not offered. */
            new DarlingServer(4, "sql-off", "Disabled", false, 16),
            new DarlingServer(9, "azure-off", "Azure Off", false, 12, engineEdition: CollectorEngineCapability.AzureSqlDatabaseEngineEdition),
            /* #6391: an Azure SQL Database has no SQL Agent and never any rows, but it is a registered server and is offered, so it can be picked. */
            new DarlingServer(5, "azure-db", "Azure DB", true, 12, engineEdition: CollectorEngineCapability.AzureSqlDatabaseEngineEdition),
            /* SQL Agent runs: Azure SQL Managed Instance (edition 8), a box SQL Server (edition 3), and a server that has not connected yet. */
            new DarlingServer(6, "mi", "Managed Instance", true, 16, engineEdition: 8),
            new DarlingServer(7, "box", "Box", true, 16, engineEdition: 3),
            new DarlingServer(8, "new", "Not connected", true, null),
        ]);

        Assert.Equal(
            new[] { new Choice(1, "Alias A"), new Choice(2, "sql-b"), new Choice(5, "Azure DB"), new Choice(6, "Managed Instance"), new Choice(7, "Box"), new Choice(8, "Not connected") },
            choices.ToArray());
    }

    [Theory]
    [InlineData(true, null, 3, true)]
    [InlineData(true, null, 0, true)]
    [InlineData(true, null, 8, true)]
    [InlineData(true, null, 5, false)]
    [InlineData(false, null, 3, false)]
    [InlineData(true, "postgres", 0, false)]
    public void TheCollectorsOwnRule_DecidesWhichServersAreOffered(bool enabled, string? engineKind, int edition, bool offered) =>
        Assert.Equal(offered, JobHistoryCollector.CollectsOn(enabled, engineKind, edition));

    [Fact]
    public void TheOfferedRule_AgreesWithTheCollectorsAppliesTo_ForEveryHostedShape()
    {
        /* The rule is the collector's own gate, swept over every shape of an edition: no second copy to drift. */
        foreach (var edition in new[] { 2, 3, 4, 5, 8 })
        {
            var runs = CollectorEngineCapability.TargetsWithEngineEdition(edition).Any(t => JobHistoryCollector.Instance.AppliesTo(t));
            Assert.Equal(runs, JobHistoryCollector.CollectsOn(true, null, edition));
        }
    }
}
