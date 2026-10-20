/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6138: what a screen reader or a UI test tool can do in Lite. A sidebar row and an Overview card answer Invoke (the default action)
/// and open the server the way a double-click does, the tab badge reads "1 unread alert" rather than a bare "1", and every Settings
/// input carries the name of its label. The viewer's twin is ViewerAutomationInvokeAndNamesTests in Darling.Tests.
/// </summary>
public class AutomationInvokeAndNamesTests
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

    private static List<AutomationPeer> InvokableItems(AutomationPeer root)
    {
        var peers = new List<AutomationPeer>();
        Walk(root, peers);
        return peers.Where(p => p.GetPattern(PatternInterface.Invoke) is not null).ToList();
    }

    [Fact]
    public void InvokingASidebarRow_SelectsItAndRunsTheOpenMethod_TheSameOneDoubleClickAndEnterRun()
    {
        var (opened, selectedWhenOpened) = StaTestThread.Run(() =>
        {
            var list = new InvokableListBox { ItemsSource = new[] { "Example SQL 01", "Example SQL 02" } };
            var opened = new List<string>();
            string? selectedWhenOpened = null;

            /* The window's own wiring: the open method it runs for double-click and Enter, handed to the shared Invoke body. */
            void OpenSelected()
            {
                selectedWhenOpened = (string?)list.SelectedItem;
                opened.Add((string)list.SelectedItem);
            }

            list.ItemInvoked += (_, e) => ServerListKeyboard.HandleInvoke(list, e.Item, OpenSelected);

            var window = new Window { Content = list, Width = 300, Height = 300 };
            window.Show();
            Pump(window.Dispatcher);
            var rows = InvokableItems(UIElementAutomationPeer.CreatePeerForElement(list));
            Assert.Equal(2, rows.Count);
            ((IInvokeProvider)rows[1].GetPattern(PatternInterface.Invoke)!).Invoke();
            Pump(window.Dispatcher);
            window.Close();
            return (opened, selectedWhenOpened);
        });

        Assert.Equal(new[] { "Example SQL 02" }, opened);
        Assert.Equal("Example SQL 02", selectedWhenOpened);
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
            var rows = InvokableItems(UIElementAutomationPeer.CreatePeerForElement(list));
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
            var rows = InvokableItems(UIElementAutomationPeer.CreatePeerForElement(cards));
            ((IInvokeProvider)rows[1].GetPattern(PatternInterface.Invoke)!).Invoke();
            Pump(window.Dispatcher);
            window.Close();
            return (invoked, rows.Select(r => r.GetName()).ToList());
        });

        Assert.Equal(new[] { 8 }, invoked);
        Assert.Equal(new[] { "Example SQL 07", "Example SQL 08" }, names);
    }

    [Theory]
    [InlineData(0, "")]
    [InlineData(1, "1 unread alert")]
    [InlineData(3, "3 unread alerts")]
    public void TheTabBadge_ReadsAsTheNumberOfUnreadAlerts(int count, string expected) =>
        Assert.Equal(expected, UnreadAlertsName.For(count));

    [Fact]
    public void EverySettingsInput_OnEveryTab_HasANameThatIsItsLabel()
    {
        var dir = Path.Combine(Path.GetTempPath(), "input-names-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var problems = StaTestThread.Run(() =>
            {
                var window = new SettingsWindow(new ScheduleManager(dir), new ServerManager(dir));
                window.Show();
                var found = InputNamesProbe.Unnamed(window, (TabControl)window.FindName("SettingsTabs"));

                /* Closing an unsaved settings window re-applies the original theme, which needs the running app's pack resources; this window changed none. */
                typeof(SettingsWindow).GetField("_saved", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(window, true);
                window.Close();
                return found;
            });

            Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }
}
