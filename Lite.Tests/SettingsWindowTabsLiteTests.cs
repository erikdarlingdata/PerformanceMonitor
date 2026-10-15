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
using System.Reflection;
using System.Text.Json.Nodes;
using System.Windows;
using System.Windows.Controls;
using Darling.Tests;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5846: Lite's Settings window, split into tabs like the Darling Viewer's. These build the real window (on a throwaway
/// config folder) and check what an operator meets: every control on a tab, a value on a hidden tab still saved, a Save
/// with an invalid value opening the tab that holds it, and the window reopening on the last tab.
/// </summary>
public sealed class SettingsWindowTabsLiteTests : IDisposable
{
    private readonly string _configDir = Path.Combine(Path.GetTempPath(), $"lite-settings-tabs-{Guid.NewGuid():N}");
    private readonly string _savedTab = App.SettingsWindowTab;
    private readonly int _savedCooldown = App.AlertCooldownMinutes;

    public SettingsWindowTabsLiteTests() => Directory.CreateDirectory(_configDir);

    public void Dispose()
    {
        App.SettingsWindowTab = _savedTab;
        App.AlertCooldownMinutes = _savedCooldown;
        try
        {
            Directory.Delete(_configDir, recursive: true);
        }
        catch (IOException)
        {
            /* A temp folder left behind is harmless. */
        }
    }

    /// <summary>The window, with the tab write kept off the real settings.json: each write lands in <paramref name="writes"/>.</summary>
    private SettingsWindow NewWindow(List<string>? writes = null)
    {
        var window = new SettingsWindow(new ScheduleManager(_configDir), new ServerManager(_configDir));
        window.PersistSelectedTab = name => writes?.Add(name);
        return window;
    }

    [Fact]
    public void EveryNamedControlSitsOnATab_OnlyTheTabStripAndTheFooterAreWindowWide()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            /* The fields XAML generates for x:Name: a FrameworkElement whose Name is the field's own name. */
            var named = typeof(SettingsWindow)
                .GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
                .Where(f => typeof(FrameworkElement).IsAssignableFrom(f.FieldType))
                .Select(f => (f.Name, Element: f.GetValue(window) as FrameworkElement))
                .Where(x => x.Element is not null && x.Element.Name == x.Name)
                .ToList();
            Assert.NotEmpty(named);

            var offTab = named
                .Where(x => SettingsWindow.TabOf(x.Element) is null)
                .Select(x => x.Name)
                .OrderBy(n => n, StringComparer.Ordinal);
            Assert.Equal(new[] { "FooterButtons", "SettingsTabs" }, offTab);

            var tabs = window.SettingsTabs.Items.OfType<TabItem>().ToList();
            Assert.All(named.Select(x => SettingsWindow.TabOf(x.Element)).OfType<TabItem>(),
                tab => Assert.Contains(tab, tabs));
        });
    }

    [Fact]
    public void AValueOnATabThatIsNotShowing_IsStillSaved()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            window.SettingsTabs.SelectedItem = window.CollectionTab;
            Assert.Same(window.DeliveryTab, SettingsWindow.TabOf(window.AlertCooldownBox));

            window.AlertCooldownBox.Text = "7";
            var root = new JsonObject();
            var errors = new SettingsValidationErrors();
            window.ApplyAlertSettings(root, errors);

            Assert.Equal(0, errors.Count);
            Assert.Equal(7, root["alert_cooldown_minutes"]!.GetValue<int>());
        });
    }

    [Fact]
    public void SaveValidation_OpensTheFirstTabHoldingAnInvalidValue_InTabOrder()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            window.SettingsTabs.SelectedItem = window.ApplicationTab;

            window.AnalysisIntervalBox.Text = "1";
            window.AlertPerEventMaxBox.Text = "0";
            var errors = new SettingsValidationErrors();
            window.ApplyAlertSettings(new JsonObject(), errors);

            Assert.Equal(2, errors.Count);
            Assert.Same(window.DeliveryTab, window.SelectFirstTabWithError(errors));
            Assert.Same(window.DeliveryTab, window.SettingsTabs.SelectedItem);

            /* A Save collects from several writers (the MCP port is checked before the alert fields): an Access error
               found first still loses to a Delivery error found later, because Delivery sits further left. */
            window.SettingsTabs.SelectedItem = window.ApplicationTab;
            var acrossWriters = new SettingsValidationErrors();
            acrossWriters.Add("MCP port", window.McpPortTextBox);
            acrossWriters.Add("Tray notification cooldown", window.AlertCooldownBox);
            Assert.Same(window.DeliveryTab, window.SelectFirstTabWithError(acrossWriters));
        });
    }

    [Fact]
    public void SaveValidation_AnErrorOnAHiddenTabSelectsIt_AndNoErrorLeavesTheSelectionAlone()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            window.SettingsTabs.SelectedItem = window.CollectionTab;

            var clean = new SettingsValidationErrors();
            window.ApplyAlertSettings(new JsonObject(), clean);
            Assert.Equal(0, clean.Count);
            Assert.Null(window.SelectFirstTabWithError(clean));
            Assert.Same(window.CollectionTab, window.SettingsTabs.SelectedItem);

            window.AnalysisNotifyCooldownBox.Text = "1";
            var errors = new SettingsValidationErrors();
            window.ApplyAlertSettings(new JsonObject(), errors);
            Assert.Same(window.AnalysisTab, window.SelectFirstTabWithError(errors));
            Assert.Same(window.AnalysisTab, window.SettingsTabs.SelectedItem);
        });
    }

    [Fact]
    public void TheWindowReopensOnTheLastTab_AndWritesItOnlyOnAChange()
    {
        App.SettingsWindowTab = "";
        StaTestThread.Run(() =>
        {
            var writes = new List<string>();
            var first = NewWindow(writes);
            first.SettingsTabs.SelectedItem = first.AccessTab;
            first.RememberSelectedTab();
            first.RememberSelectedTab();

            Assert.Equal(new[] { "AccessTab" }, writes);
            Assert.Equal("AccessTab", App.SettingsWindowTab);
            var second = NewWindow();
            Assert.Same(second.AccessTab, second.SettingsTabs.SelectedItem);
        });
    }

    [Fact]
    public void ATabNameNoTabHasAnyMore_OpensTheFirstTab()
    {
        App.SettingsWindowTab = "NoSuchTab";
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            /* Nothing selected yet (the tab strip takes its first tab when it shows) or the first tab: never another. */
            Assert.True(window.SettingsTabs.SelectedIndex <= 0, $"opened on tab {window.SettingsTabs.SelectedIndex}");
        });
    }
}
