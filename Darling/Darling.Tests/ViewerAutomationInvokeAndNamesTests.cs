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
using System.Linq;
using System.Windows;
using System.Windows.Automation.Peers;
using System.Windows.Automation.Provider;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6138: what a screen reader or a UI test tool can do in the viewer. A sidebar row and an Overview card answer Invoke (the default
/// action) and open the server the way a double-click does, the sidebar's alert badge reads "1 unread alert" rather than a bare "1", and
/// every Settings input carries the name of its label. Lite's twin is AutomationInvokeAndNamesTests in Lite.Tests.
/// </summary>
public sealed class ViewerAutomationInvokeAndNamesTests
{
    private static void Walk(AutomationPeer peer, List<AutomationPeer> into)
    {
        into.Add(peer);
        foreach (var child in peer.GetChildren() ?? new List<AutomationPeer>())
        {
            Walk(child, into);
        }
    }

    private static void Pump(Dispatcher dispatcher)
    {
        var frame = new DispatcherFrame();
        dispatcher.BeginInvoke(DispatcherPriority.ApplicationIdle, new Action(() => frame.Continue = false));
        Dispatcher.PushFrame(frame);
    }

    [Fact]
    public void InvokingASidebarRow_SelectsItAndRunsTheOpenMethod_TheSameOneDoubleClickAndEnterRun()
    {
        var (opened, selectedWhenOpened, rowPatterns) = StaTestThread.Run(() =>
        {
            var servers = new[] { new DarlingServer(1, "example-sql-01", "Example SQL 01", true, 16), new DarlingServer(2, "example-sql-02", "Example SQL 02", true, 16) };
            var list = new InvokableListBox { ItemsSource = servers };
            var opened = new List<DarlingServer>();
            DarlingServer? selectedWhenOpened = null;

            /* The window's own wiring: the open method it runs for double-click and Enter, handed to the shared Invoke body. */
            void OpenSelected()
            {
                selectedWhenOpened = list.SelectedItem as DarlingServer;
                opened.Add((DarlingServer)list.SelectedItem);
            }

            list.ItemInvoked += (_, e) => ServerListKeyboard.HandleInvoke(list, e.Item, OpenSelected);

            var window = new Window { Content = list, Width = 300, Height = 300 };
            window.Show();
            Pump(window.Dispatcher);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(list), peers);
            var rows = peers.Where(p => p.GetPattern(PatternInterface.Invoke) is not null).ToList();
            Assert.Equal(2, rows.Count);
            ((IInvokeProvider)rows[1].GetPattern(PatternInterface.Invoke)!).Invoke();
            Pump(window.Dispatcher);
            window.Close();
            return (opened, selectedWhenOpened, rows.Select(r => r.GetPattern(PatternInterface.SelectionItem) is not null).ToList());
        });

        Assert.Single(opened);
        Assert.Equal(2, opened[0].ServerId);
        Assert.Equal(2, selectedWhenOpened!.ServerId);
        Assert.All(rowPatterns, hasSelection => Assert.True(hasSelection));
    }

    [Fact]
    public void InvokingAGroupHeaderRow_OpensNothing_WhenSelectingItMovesTheSelectionOn()
    {
        var opened = StaTestThread.Run(() =>
        {
            var list = new InvokableListBox { ItemsSource = new[] { "Group", "Server" } };
            var opened = 0;

            /* Selecting a header toggles its group, which re-selects a server row (the sidebar's rule). */
            list.SelectionChanged += (_, e) =>
            {
                if (e.AddedItems.Count == 1 && (string)e.AddedItems[0]! == "Group")
                {
                    list.SelectedItem = "Server";
                }
            };
            list.ItemInvoked += (_, e) => ServerListKeyboard.HandleInvoke(list, e.Item, () => opened++);

            var window = new Window { Content = list, Width = 300, Height = 300 };
            window.Show();
            Pump(window.Dispatcher);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(list), peers);
            var rows = peers.Where(p => p.GetPattern(PatternInterface.Invoke) is not null).ToList();
            ((IInvokeProvider)rows[0].GetPattern(PatternInterface.Invoke)!).Invoke();
            Pump(window.Dispatcher);
            window.Close();
            return opened;
        });

        Assert.Equal(0, opened);
    }

    [Fact]
    public void InvokingAnOverviewCard_ReportsThatCardsServer_AndKeepsTheServersName()
    {
        var (invoked, names) = StaTestThread.Run(() =>
        {
            var cards = new InvokableItemsControl
            {
                ItemsSource = new[]
                {
                    new ServerSummaryItem { ServerId = 7, DisplayName = "Example SQL 07" },
                    new ServerSummaryItem { ServerId = 8, DisplayName = "Example SQL 08" }
                }
            };
            var invoked = new List<int>();
            cards.ItemInvoked += (_, e) => invoked.Add(((ServerSummaryItem)e.Item).ServerId);

            var window = new Window { Content = cards, Width = 300, Height = 300 };
            window.Show();
            Pump(window.Dispatcher);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(cards), peers);
            var rows = peers.Where(p => p.GetPattern(PatternInterface.Invoke) is not null).ToList();
            ((IInvokeProvider)rows[1].GetPattern(PatternInterface.Invoke)!).Invoke();
            Pump(window.Dispatcher);
            window.Close();
            return (invoked, rows.Select(r => r.GetName()).ToList());
        });

        Assert.Equal(new[] { 8 }, invoked);
        Assert.Equal(new[] { "Example SQL 07", "Example SQL 08" }, names);
    }

    [Fact]
    public void InvokingANeedsAttentionRow_ReportsThatRowsServer()
    {
        var invoked = StaTestThread.Run(() =>
        {
            var rows = new InvokableItemsControl
            {
                ItemsSource = new[]
                {
                    new FleetRankedServer { ServerId = 4, DisplayName = "Example SQL 04", Reason = "CPU 97%" },
                    new FleetRankedServer { ServerId = 5, DisplayName = "Example SQL 05", Reason = "Offline" }
                }
            };
            var invoked = new List<int>();
            rows.ItemInvoked += (_, e) => invoked.Add(((FleetRankedServer)e.Item).ServerId);

            var window = new Window { Content = rows, Width = 300, Height = 300 };
            window.Show();
            Pump(window.Dispatcher);
            var peers = new List<AutomationPeer>();
            Walk(UIElementAutomationPeer.CreatePeerForElement(rows), peers);
            var invokable = peers.Where(p => p.GetPattern(PatternInterface.Invoke) is not null).ToList();
            ((IInvokeProvider)invokable[0].GetPattern(PatternInterface.Invoke)!).Invoke();
            Pump(window.Dispatcher);
            window.Close();
            return invoked;
        });

        Assert.Equal(new[] { 4 }, invoked);
    }

    [Theory]
    [InlineData("CPU 97%, Blocking 6", "Example SQL 04, needs attention: CPU 97%, Blocking 6")]
    [InlineData("", "Example SQL 04, needs attention")]
    public void ANeedsAttentionRow_IsNamedForTheServerAndWhyItNeedsAttention(string reason, string expected)
    {
        var row = new FleetRankedServer { DisplayName = "Example SQL 04", Reason = reason };

        Assert.Equal(expected, row.AutomationName);
    }

    [Theory]
    [InlineData(0, "")]
    [InlineData(1, "1 unread alert")]
    [InlineData(3, "3 unread alerts")]
    [InlineData(120, "120 unread alerts")]
    public void TheSidebarBadge_ReadsAsTheNumberOfUnreadAlerts(int count, string expected)
    {
        var server = new DarlingServer(1, "example-sql-01", "Example SQL 01", true, 16);
        var raised = new List<string?>();
        server.PropertyChanged += (_, e) => raised.Add(e.PropertyName);

        server.SetAttention(count, false, null);

        Assert.Equal(expected, server.AttentionAutomationName);
        if (count > 0)
        {
            Assert.Contains(nameof(DarlingServer.AttentionAutomationName), raised);
        }
    }

    [Fact]
    public void EverySettingsInput_OnEveryTab_HasANameThatIsItsLabel()
    {
        var problems = StaTestThread.Run(() =>
        {
            var store = new ViewerAppSettingsStore(Path.Combine(Path.GetTempPath(), "input-names-" + Guid.NewGuid().ToString("N") + ".json"));
            var window = new SettingsWindow(new ViewerPreferences(), store, null);
            window.Show();
            var found = InputNamesProbe.Unnamed(window, (TabControl)window.FindName("SettingsTabs"));

            /* Closing an unsaved settings window re-applies the original theme, which needs the running app's pack resources; this window changed none. */
            typeof(SettingsWindow).GetField("_themeSaved", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(window, true);
            window.Close();
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}
