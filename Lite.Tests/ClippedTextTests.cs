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
using PerformanceMonitorLite;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6136: text that Lite cuts off at its default size, checked the way the Darling Viewer's twin (ViewerClippedTextTests in Darling.Tests)
/// checks the viewer: the real Settings window and the Job History and Alert History tabs are built on the shared STA thread, themed as the
/// app themes them, and every piece of text on screen is checked against the room it was given. The probe is the shared <c>ClippedTextProbe</c>.
/// </summary>
public class ClippedTextTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Themes", "DarkTheme.xaml")));

    [Fact]
    public void TheSettingsWindow_ShowsEveryLabelAndHintInFull_OnEveryTab()
    {
        var dir = Path.Combine(Path.GetTempPath(), "clipped-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var clipped = StaTestThread.Run(() =>
            {
                /* #6175: closing the window remembers the last tab it showed in App.SettingsWindowTab (a process-wide static
                   SettingsWindowTabsLiteTests reads) and writes it to settings.json. This test visits every tab, so it ends on the
                   last one: it puts the static back and keeps the write off the real file. */
                var rememberedTab = App.SettingsWindowTab;
                var window = new SettingsWindow(new ScheduleManager(dir), new ServerManager(dir)) { PersistSelectedTab = _ => { } };
                window.Resources.MergedDictionaries.Add(Theme());
                window.Show();
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
                typeof(SettingsWindow).GetField("_saved", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(window, true);
                window.Close();
                App.SettingsWindowTab = rememberedTab;
                return found;
            });

            Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void JobHistoryAndAlertHistory_ShowEveryHeaderAndToolbarItemInFull_BesideTheTabStrip()
    {
        var clipped = StaTestThread.Run(() =>
        {
            var found = new List<string>();
            foreach (var (name, tab) in new (string, FrameworkElement)[] { ("Job History", new JobHistoryTab()), ("Alert History", new AlertsHistoryTab()) })
            {
                var window = new Window { Content = tab, Width = 1100, Height = 925 };
                window.Resources.MergedDictionaries.Add(Theme());
                window.Show();
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
}
