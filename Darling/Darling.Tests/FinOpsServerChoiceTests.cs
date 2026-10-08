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
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's FinOps tab follows the web FinOps page on PostgreSQL targets: it starts on a SQL Server target, names a
/// PostgreSQL target in its picker, says "Not collected for PostgreSQL" instead of running a SQL Server-only read for it,
/// and Server Inventory has a "Show removed servers" checkbox that is off by default. The choice is a pure helper
/// (<see cref="FinOpsServerChoice"/>); the wiring into the window is pinned against the source, since no window runs here.
/// </summary>
public sealed class FinOpsServerChoiceTests
{
    private static DarlingServer Sql(int id, string name = "") =>
        new(id, string.IsNullOrEmpty(name) ? $"sql-{id}" : name, string.IsNullOrEmpty(name) ? $"sql-{id}" : name, true, 16);

    private static DarlingServer Pg(int id) =>
        new(id, $"pg-{id}", $"pg-{id}", true, null, engineKind: "postgres");

    // ── Selection ──

    [Fact]
    public void Selection_SidebarPostgres_StartsOnTheFirstSqlServerTarget()
    {
        var servers = new List<DarlingServer> { Pg(1), Pg(2), Sql(3), Sql(4) };

        Assert.Equal(3, FinOpsServerChoice.Selection(servers, previousServerId: null, sidebarServerId: 2)!.ServerId);
    }

    [Fact]
    public void Selection_SidebarSqlServer_IsKept()
    {
        var servers = new List<DarlingServer> { Pg(1), Sql(2), Sql(3) };

        Assert.Equal(3, FinOpsServerChoice.Selection(servers, previousServerId: null, sidebarServerId: 3)!.ServerId);
    }

    [Fact]
    public void Selection_NoSidebarServer_IsTheFirstSqlServerTarget()
    {
        var servers = new List<DarlingServer> { Pg(1), Sql(2) };

        Assert.Equal(2, FinOpsServerChoice.Selection(servers, null, null)!.ServerId);
    }

    [Fact]
    public void Selection_ARebuildKeepsThePreviousServer_EvenAPostgresTheUserPicked()
    {
        var servers = new List<DarlingServer> { Sql(1), Pg(2) };

        Assert.Equal(2, FinOpsServerChoice.Selection(servers, previousServerId: 2, sidebarServerId: 1)!.ServerId);
    }

    [Fact]
    public void Selection_APreviousServerThatIsGone_FallsToTheSidebarThenTheFirstSqlServerTarget()
    {
        var servers = new List<DarlingServer> { Pg(1), Sql(2), Sql(3) };

        Assert.Equal(3, FinOpsServerChoice.Selection(servers, previousServerId: 99, sidebarServerId: 3)!.ServerId);
        Assert.Equal(2, FinOpsServerChoice.Selection(servers, previousServerId: 99, sidebarServerId: 1)!.ServerId);
    }

    [Fact]
    public void Selection_OnlyPostgresTargets_TakesTheSidebarServerThenTheFirst()
    {
        var servers = new List<DarlingServer> { Pg(1), Pg(2) };

        Assert.Equal(2, FinOpsServerChoice.Selection(servers, null, sidebarServerId: 2)!.ServerId);
        Assert.Equal(1, FinOpsServerChoice.Selection(servers, null, sidebarServerId: null)!.ServerId);
    }

    [Fact]
    public void Selection_EmptyList_IsNull() =>
        Assert.Null(FinOpsServerChoice.Selection(new List<DarlingServer>(), 1, 1));

    // ── Label ──

    [Fact]
    public void Label_NamesAPostgresTarget_AndLeavesASqlServerTargetAlone()
    {
        Assert.Equal("pg-5 (PostgreSQL)", FinOpsServerChoice.Label(Pg(5)));
        Assert.Equal("example-sql-01", FinOpsServerChoice.Label(Sql(1, "example-sql-01")));
    }

    // ── The gate ──

    [Fact]
    public void NotCollectedLine_PostgresTarget_SaysOneShortLineForEverySingleServerPanel()
    {
        Assert.Equal("Not collected for PostgreSQL", FinOpsServerChoice.NotCollectedLine(Pg(1), crossServer: false));
    }

    [Fact]
    public void NotCollectedLine_SqlServerTarget_RunsItsReads()
    {
        Assert.Null(FinOpsServerChoice.NotCollectedLine(Sql(1), crossServer: false));
    }

    [Fact]
    public void NotCollectedLine_ServerInventory_RunsForAPostgresSelection()
    {
        Assert.Null(FinOpsServerChoice.NotCollectedLine(Pg(1), crossServer: true));
    }

    [Fact]
    public void NotCollectedLine_NoServer_ShowsNothing()
    {
        Assert.Null(FinOpsServerChoice.NotCollectedLine(null, crossServer: false));
    }

    // ── Show removed servers ──

    [Theory]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(null, false)]
    public void IncludeRemoved_IsOnlyTheCheckedState(bool? isChecked, bool expected) =>
        Assert.Equal(expected, FinOpsServerChoice.IncludeRemoved(isChecked));

    [Theory]
    [InlineData(0, false, "")]
    [InlineData(0, true, "")]
    [InlineData(3, false, "3 server(s)")]
    [InlineData(3, true, "3 server(s) (removed servers included)")]
    public void InventoryCountText_NamesRemovedServers_LikeTheWebPage(int count, bool removed, string expected) =>
        Assert.Equal(expected, FinOpsServerChoice.InventoryCountText(count, removed));

    /// <summary>Round-2 L4: tick then untick fast. The untick's read ends first; the tick's slow read ends last and must be dropped.</summary>
    [Fact]
    public async System.Threading.Tasks.Task ASlowerEarlierLoad_ThatFinishesLast_IsDropped()
    {
        var sequence = new FinOpsLoadSequence();
        var applied = new List<string>();
        var slowRead = new System.Threading.Tasks.TaskCompletionSource();
        var fastRead = new System.Threading.Tasks.TaskCompletionSource();

        async System.Threading.Tasks.Task Load(string label, System.Threading.Tasks.TaskCompletionSource read)
        {
            var token = sequence.Begin();
            await read.Task;
            if (!sequence.IsCurrent(token)) return;
            applied.Add(label);
        }

        var ticked = Load("removed included", slowRead);
        var unticked = Load("removed left out", fastRead);
        fastRead.SetResult();
        await unticked;
        slowRead.SetResult();
        await ticked;

        Assert.Equal(["removed left out"], applied);
    }

    [Fact]
    public void OnlyTheLoadThatBeganLast_IsCurrent()
    {
        var sequence = new FinOpsLoadSequence();
        var first = sequence.Begin();
        Assert.True(sequence.IsCurrent(first));
        var second = sequence.Begin();
        Assert.False(sequence.IsCurrent(first));
        Assert.True(sequence.IsCurrent(second));
    }

    [Fact]
    public void TheInventoryLoader_UsesTheSequenceAndTheCountHelper()
    {
        var loaders = ViewerFile("FinOpsTab.Loaders.cs");
        var load = loaders[loaders.IndexOf("private async Task LoadFinOpsServerInventoryAsync()", StringComparison.Ordinal)..];
        load = load[..load.IndexOf("// ── Refresh buttons", StringComparison.Ordinal)];
        Assert.Contains("_finopsInventoryLoads.Begin()", load, StringComparison.Ordinal);
        Assert.True(load.IndexOf("IsCurrent(token)", StringComparison.Ordinal) > load.IndexOf("GetServerMetricsAsync()", StringComparison.Ordinal),
            "the staleness check runs after the last await");
        Assert.True(load.IndexOf("IsCurrent(token)", StringComparison.Ordinal) < load.IndexOf("UpdateData(servers)", StringComparison.Ordinal),
            "a superseded load must not reach the grid");
        Assert.Contains("FinOpsServerChoice.InventoryCountText(servers.Count, includeRemoved)", load, StringComparison.Ordinal);
    }

    // ── Wiring, pinned against the source ──

    private static string ViewerFile(string name, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Viewer", name))
            .ReplaceLineEndings("\n");

    [Fact]
    public void ThePicker_BuildsItsSelectionAndItsLabelsThroughTheHelper()
    {
        var code = ViewerFile("FinOpsTab.xaml.cs");
        Assert.Contains("FinOpsServerChoice.Selection(", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ViewerServerSetSync.PickerSelectionAfterReload(", code, StringComparison.Ordinal);

        var xaml = ViewerFile("FinOpsTab.xaml");
        Assert.Contains("Binding PickerLabel", xaml, StringComparison.Ordinal);
        Assert.Equal(FinOpsServerChoice.Label(Pg(7)), Pg(7).PickerLabel);
    }

    [Fact]
    public void TheLoader_RunsNoReadForAPostgresTarget_ExceptServerInventory()
    {
        var loaders = ViewerFile("FinOpsTab.Loaders.cs");
        var load = loaders[loaders.IndexOf("private async Task LoadFinOpsAsync()", StringComparison.Ordinal)..];
        var gate = load.IndexOf("FinOpsServerChoice.NotCollectedLine(", StringComparison.Ordinal);
        var firstRead = load.IndexOf("switch (FinOpsSubTabControl.SelectedIndex)", StringComparison.Ordinal);
        Assert.True(gate >= 0, "LoadFinOpsAsync must ask the helper whether the selected server's reads apply");
        Assert.True(gate < firstRead, "the gate must run before the sub-tab switch starts any read");
        Assert.Contains("crossServer: SelectedSubTabIsCrossServer", load, StringComparison.Ordinal);
    }

    [Fact]
    public void ServerInventory_HasAShowRemovedCheckbox_OffByDefault_PassedToTheReader()
    {
        var xaml = ViewerFile("FinOpsTab.xaml");
        var box = xaml.IndexOf("x:Name=\"FinOpsShowRemovedCheck\"", StringComparison.Ordinal);
        Assert.True(box >= 0, "the Server Inventory header needs the checkbox");
        var tag = xaml[box..xaml.IndexOf("/>", box, StringComparison.Ordinal)];
        Assert.Contains("Show removed servers", tag, StringComparison.Ordinal);
        Assert.DoesNotContain("IsChecked=\"True\"", tag, StringComparison.Ordinal);

        var loaders = ViewerFile("FinOpsTab.Loaders.cs");
        Assert.Contains("var includeRemoved = FinOpsServerChoice.IncludeRemoved(FinOpsShowRemovedCheck.IsChecked);", loaders, StringComparison.Ordinal);
        Assert.Contains("GetServerInventoryAsync(includeRemoved)", loaders, StringComparison.Ordinal);

        var service = ViewerFile("ViewerDataService.FinOps.Inventory.cs");
        Assert.Contains("includeRemoved: includeRemoved", service, StringComparison.Ordinal);
    }
}
