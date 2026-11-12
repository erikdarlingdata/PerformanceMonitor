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
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6136: text that the viewer cuts off at its default size. The real windows and tabs are built on the shared STA thread, themed as the app
/// themes them, and every piece of text on screen is checked against the room it was given: a column header ("Run Time (serv", "Channe"),
/// a settings row ("Poison waits", a hint ending mid-sentence), a button ("Move Dov"), the toolbar's status text. Lite's twin is
/// ClippedTextTests in Lite.Tests; the probe is the shared <c>ClippedTextProbe</c>.
/// </summary>
public sealed class ViewerClippedTextTests
{
    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "Darling", "PerformanceMonitor.Darling.Viewer"));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(ViewerDir(), "Themes", "DarkTheme.xaml")));

    [Fact]
    public void TheSettingsWindow_ShowsEveryLabelAndHintInFull_OnEveryTab()
    {
        var clipped = StaTestThread.Run(() =>
        {
            var store = new ViewerAppSettingsStore(Path.Combine(Path.GetTempPath(), "clipped-" + Guid.NewGuid().ToString("N") + ".json"));
            var window = new SettingsWindow(new ViewerPreferences(), store, null);
            window.Resources.MergedDictionaries.Add(Theme());
            StaTestThread.ShowOffScreen(window);
            var tabs = (TabControl)window.FindName("SettingsTabs");
            var found = new List<string>();
            for (var i = 0; i < tabs.Items.Count; i++)
            {
                tabs.SelectedIndex = i;
                ClippedTextProbe.Settle(window);
                var tab = (TabItem)tabs.Items[i];
                var scroll = (ScrollViewer)tab.Content;
                found.AddRange(ClippedTextProbe.Clipped(scroll, scroll).Select(c => $"[{tab.Header}] {c}"));
            }

            /* Closing an unsaved settings window re-applies the original theme, which needs the running app's pack resources; this window changed none. */
            typeof(SettingsWindow).GetField("_themeSaved", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(window, true);
            window.Close();
            return found;
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }

    [Fact]
    public void JobHistoryAndAlertHistory_ShowEveryHeaderAndToolbarItemInFull_BesideTheSidebar()
    {
        var clipped = StaTestThread.Run(() =>
        {
            var found = new List<string>();
            foreach (var (name, tab) in new (string, FrameworkElement)[] { ("Job History", new JobHistoryTab()), ("Alert History", new AlertsHistoryTab()) })
            {
                /* The tab sits right of the sidebar, so it is narrower than the window: 1100 is what is left of 1550 on a busy fleet. */
                var window = new Window { Content = tab, Width = 1100, Height = 925 };
                window.Resources.MergedDictionaries.Add(Theme());
                StaTestThread.ShowOffScreen(window);
                foreach (var (indicator, text) in new[]
                {
                    ("AgentStatusIndicator", "Agents: 5/7 running, 2 stopped"),
                    ("LastRefreshedIndicator", "Refreshed 12m ago"),
                    ("JobCountIndicator", "2,000 runs (newest 2,000)"),
                    ("AlertCountIndicator", "2,000 alert(s) (newest 2,000)"),
                })
                {
                    if (tab.FindName(indicator) is TextBlock block)
                    {
                        block.Text = text;
                    }
                }

                ClippedTextProbe.Settle(window);
                found.AddRange(ClippedTextProbe.Clipped(tab, tab).Select(c => $"[{name}] {c}"));
                found.AddRange(ClippedTextProbe.ClippedHeaders(tab).Select(c => $"[{name}] {c}"));
                window.Close();
            }

            return found;
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }

    /// <summary>#8662: the PostgreSQL sentence Job History shows in an empty grid is longer than the grid is wide (it was drawn on one line, 2,162 px
    /// in a 1,600 px grid, cut at both ends). It wraps to the grid's width, so the server's name at its start and the "nothing can change
    /// it" ending are both on screen.</summary>
    [Fact]
    public void JobHistory_EmptyGridSentenceForAPostgresServer_WrapsToTheGridsWidth()
    {
        var (clipped, lines) = StaTestThread.Run(() =>
        {
            var tab = new JobHistoryTab();
            var window = new Window { Content = tab, Width = 1100, Height = 925 };
            window.Resources.MergedDictionaries.Add(Theme());
            StaTestThread.ShowOffScreen(window);
            var pg = new DarlingServer(
                7, "pg-a", "pg-a", true, null, engineKind: PerformanceMonitor.Collectors.MonitoredEngineKind.Postgres, postgresMajorVersion: 16);
            var message = (TextBlock)tab.FindName("NoJobsMessage");
            message.Text = JobHistoryTab.EmptyNote(7, ViewerDataService.ServersOf([pg]));
            message.Visibility = Visibility.Visible;
            ClippedTextProbe.Settle(window);
            var found = ClippedTextProbe.Clipped(tab, tab);
            var height = message.ActualHeight;
            var oneLine = message.FontSize * 1.2;
            window.Close();
            return (found, height / oneLine);
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
        Assert.True(lines > 1.5, $"the sentence should take more than one line in a 1,100 px window, took {lines:0.0}");
    }

    [Fact]
    public void AColumnHeader_StaysInFull_WhenItsTitleGrowsAfterTheTabOpens()
    {
        var clipped = StaTestThread.Run(() =>
        {
            var tab = new JobHistoryTab();
            var window = new Window { Content = tab, Width = 1550, Height = 925 };
            window.Resources.MergedDictionaries.Add(Theme());
            StaTestThread.ShowOffScreen(window);
            ClippedTextProbe.Settle(window);
            /* What a load does once the clock mode is known: the title gets its clock suffix. */
            ((TextBlock)tab.FindName("RunTimeHeaderText")).Text = "Run Time (server time zone, long form)";
            DataGridHeaderFit.Apply((DataGrid)tab.FindName("JobHistoryDataGrid"));
            ClippedTextProbe.Settle(window);
            var found = ClippedTextProbe.ClippedHeaders(tab);
            window.Close();
            return found;
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }
}
