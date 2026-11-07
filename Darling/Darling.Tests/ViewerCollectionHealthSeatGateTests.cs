/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8103: on a read-only seat the Collection Health tab's three write buttons ("Purge Now...", "Run all collectors now" and each row's
/// "Run now") are off from the moment the tab is built, with the reason as the tooltip of the disabled button, as Alert History's Dismiss
/// buttons are. The seat is told before it clicks, not by the message box or status line after. The first three tests build the real tab on
/// a real data service whose seat is read-only; the last two drive the gating predicate the buttons take their state from.
/// </summary>
public sealed class ViewerCollectionHealthSeatGateTests
{
    private const string ReadOnlySentenceStart = "This viewer is connected read-only, so";

    private sealed record ButtonState(bool IsEnabled, object? ToolTip, bool ShowsTipWhenDisabled);

    private sealed record TabState(ButtonState Purge, ButtonState RunAll, ButtonState RowRunNow);

    /// <summary>
    /// Builds the real tab on a data service whose seat is <paramref name="readOnly"/>, opens its Collection Health sub-tab, gives the Health
    /// Summary grid two collectors so a "Run now" row exists, and reads the three buttons as the tab drew them. The data service points at a
    /// port nothing listens on; the tab's own reads are skipped the way the sweep-pressure tests skip them.
    /// </summary>
    private static TabState DrawTab(bool readOnly)
    {
        return StaTestThread.Run(() =>
        {
            var dataService = new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling");
            typeof(ViewerDataService).GetProperty(nameof(ViewerDataService.IsReadOnly))!.SetValue(dataService, readOnly);
            var tab = new ViewerServerTab(dataService, new DarlingServer(1, "seat-gate-test", "seat-gate-test", true, null));
            typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
            var window = new Window { Content = tab, Width = 1800, Height = 1000 };
            window.Show();
            try
            {
                ((TabItem)tab.FindName("CollectionHealthTab")).IsSelected = true;
                window.UpdateLayout();

                var grid = (DataGrid)tab.FindName("CollectionHealthGrid");
                grid.ItemsSource = new List<CollectorHealthRow>
                {
                    new() { CollectorName = "wait_stats" },
                    new() { CollectorName = "blocking" },
                };
                grid.UpdateLayout();

                var row = grid.ItemContainerGenerator.ContainerFromIndex(0) as DataGridRow;
                Assert.True(row is not null, "The Health Summary grid drew no row for its two collectors, so there is no Run now button to read.");
                var rowButton = grid.Columns[0].GetCellContent(row!) as Button;
                Assert.True(rowButton is not null, "The first Health Summary column did not draw its Run now button.");

                return new TabState(
                    Read((Button)tab.FindName("PurgeNowButton")),
                    Read((Button)tab.FindName("RunAllCollectorsButton")),
                    Read(rowButton!));
            }
            finally
            {
                window.Close();
            }
        });
    }

    private static ButtonState Read(Button button) => new(button.IsEnabled, button.ToolTip, ToolTipService.GetShowOnDisabled(button));

    /// <summary>Alert History's sentence shape: what the seat cannot do, then how to connect with a login that can.</summary>
    private static void AssertReadOnlyReason(object? toolTip, string about)
    {
        var text = Assert.IsType<string>(toolTip);
        Assert.StartsWith(ReadOnlySentenceStart, text, StringComparison.Ordinal);
        Assert.Contains(about, text, StringComparison.Ordinal);
        Assert.EndsWith(ViewerReadOnlyException.HowToConnectAsAdmin, text, StringComparison.Ordinal);
    }

    [Fact]
    public void ReadOnlySeat_PurgeNowAndRunAll_AreOffWhenTheTabIsBuilt_WithTheReasonOnTheDisabledButton()
    {
        var state = DrawTab(readOnly: true);

        Assert.False(state.Purge.IsEnabled, "Purge Now... is on for a read-only seat, which cannot enqueue the purge command.");
        AssertReadOnlyReason(state.Purge.ToolTip, "purge");
        Assert.True(state.Purge.ShowsTipWhenDisabled, "Purge Now... does not show its tooltip while disabled, so the seat is not told why.");

        Assert.False(state.RunAll.IsEnabled, "Run all collectors now is on for a read-only seat, which cannot enqueue the run command.");
        AssertReadOnlyReason(state.RunAll.ToolTip, "collectors");
        Assert.True(state.RunAll.ShowsTipWhenDisabled, "Run all collectors now does not show its tooltip while disabled, so the seat is not told why.");
    }

    [Fact]
    public void ReadOnlySeat_EachRowsRunNow_IsOffAsItIsDrawn_WithTheReason()
    {
        var state = DrawTab(readOnly: true);

        Assert.False(state.RowRunNow.IsEnabled, "A row's Run now is on for a read-only seat, which cannot enqueue the run command.");
        AssertReadOnlyReason(state.RowRunNow.ToolTip, "collectors");
        Assert.True(state.RowRunNow.ShowsTipWhenDisabled, "A row's Run now does not show its tooltip while disabled, so the seat is not told why.");
    }

    [Fact]
    public void WritableSeat_SeesTheThreeButtonsOn_WithTheirOwnTips()
    {
        var state = DrawTab(readOnly: false);

        Assert.True(state.Purge.IsEnabled);
        Assert.Null(state.Purge.ToolTip);
        Assert.True(state.RunAll.IsEnabled);
        Assert.StartsWith("Runs every enabled collector", Assert.IsType<string>(state.RunAll.ToolTip), StringComparison.Ordinal);
        Assert.True(state.RowRunNow.IsEnabled);
        Assert.StartsWith("Runs this collector once", Assert.IsType<string>(state.RowRunNow.ToolTip), StringComparison.Ordinal);
    }

    /// <summary>The predicate the buttons take their state from, by name: a missing one is the gate's absence, said plainly.</summary>
    private static (bool IsEnabled, string? ToolTip) Gate(string name, params object[] args)
    {
        var method = typeof(ViewerCollectorRun).GetMethod(name, BindingFlags.Public | BindingFlags.Static);
        Assert.True(method is not null, $"ViewerCollectorRun.{name} is missing: the Collection Health write buttons have no seat gate to take their state from when the tab is built.");
        var state = method!.Invoke(null, args)!;
        var type = state.GetType();
        return ((bool)type.GetField("Item1")!.GetValue(state)!, (string?)type.GetField("Item2")!.GetValue(state));
    }

    [Fact]
    public void RunAllState_SaysTheSeatFirst_ThenTheServersConnection_ThenTheRunInFlight()
    {
        var readOnly = Gate("RunAllState", true, false, false);
        Assert.False(readOnly.IsEnabled);
        AssertReadOnlyReason(readOnly.ToolTip, "collectors");

        /* A read-only seat that is also offline is told about the seat: no reconnect of the server makes it able to write. */
        var readOnlyAndOffline = Gate("RunAllState", true, true, true);
        Assert.False(readOnlyAndOffline.IsEnabled);
        AssertReadOnlyReason(readOnlyAndOffline.ToolTip, "collectors");

        var offline = Gate("RunAllState", false, true, false);
        Assert.False(offline.IsEnabled);
        Assert.Equal(ViewerServerTab.OfflineRunTip, offline.ToolTip);

        var inFlight = Gate("RunAllState", false, false, true);
        Assert.False(inFlight.IsEnabled);
        Assert.StartsWith("Runs every enabled collector", inFlight.ToolTip, StringComparison.Ordinal);

        var ready = Gate("RunAllState", false, false, false);
        Assert.True(ready.IsEnabled);
        Assert.StartsWith("Runs every enabled collector", ready.ToolTip, StringComparison.Ordinal);
    }

    [Fact]
    public void RunOneStateAndPurgeState_FollowTheSeatAlone()
    {
        var rowReadOnly = Gate("RunOneState", true);
        Assert.False(rowReadOnly.IsEnabled);
        AssertReadOnlyReason(rowReadOnly.ToolTip, "collectors");

        var rowWritable = Gate("RunOneState", false);
        Assert.True(rowWritable.IsEnabled);
        Assert.StartsWith("Runs this collector once", rowWritable.ToolTip, StringComparison.Ordinal);

        var purgeReadOnly = Gate("PurgeState", true);
        Assert.False(purgeReadOnly.IsEnabled);
        AssertReadOnlyReason(purgeReadOnly.ToolTip, "purge");

        var purgeWritable = Gate("PurgeState", false);
        Assert.True(purgeWritable.IsEnabled);
        Assert.Null(purgeWritable.ToolTip);
    }
}
