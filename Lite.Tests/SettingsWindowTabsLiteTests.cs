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
/// with an invalid value opening the tab that holds it, and the window reopening on the last tab. #5881: in the
/// <c>app-alert-statics</c> collection, because the window reads and writes the process-wide <c>App</c> alert statics, and
/// <c>LiteAlertForwardingTests</c> (same collection) rewrites <c>App.AlertCooldownMinutes</c> while this class's apply runs.
/// </summary>
[Collection("app-alert-statics")]
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

    /// <summary>The window, with the tab write kept off the real settings.json: each write lands in <paramref name="writes"/>.
    /// #6175: the remembered tab (<see cref="App.SettingsWindowTab"/>) is process-wide, and a window another class closes
    /// (ClippedTextTests walks to the last tab and closes one) writes it. So it is set here, inside the body that holds the
    /// shared STA thread, never before the body is queued: set outside, it could be overwritten while this test waited its
    /// turn. <paramref name="remembered"/> is the tab the window should find (none by default); null leaves it as it is.</summary>
    private SettingsWindow NewWindow(List<string>? writes = null, string? remembered = "")
    {
        if (remembered is not null)
        {
            App.SettingsWindowTab = remembered;
        }

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

    /// <summary>#5736: the Settings save is the only writer of the marker that makes a saved off readable. Unchecking the alert and
    /// applying must put the switch AND the marker in the saved root, and that root must load with the alert still off. Without
    /// the marker, the loader ignores the switch and Lite could never turn the alert off.</summary>
    [Fact]
    public void UncheckingTheClientTimeoutsAlert_SavesTheSwitchBesideItsMarker_AndTheFileLoadsOff()
    {
        StaTestThread.Run(() =>
        {
            var before = App.AlertClientTimeoutsEnabled;
            var dir = Path.Combine(Path.GetTempPath(), $"lite-ct-save-{Guid.NewGuid():N}");
            Directory.CreateDirectory(dir);
            try
            {
                var window = NewWindow();
                window.AlertClientTimeoutsCheckBox.IsChecked = false;
                var root = new JsonObject();
                var errors = new SettingsValidationErrors();
                window.ApplyAlertSettings(root, errors);

                Assert.Equal(0, errors.Count);
                Assert.False(root["alert_client_timeouts_enabled"]!.GetValue<bool>());
                Assert.True(root["alert_client_timeouts_on_by_default_applied"]!.GetValue<bool>());

                File.WriteAllText(Path.Combine(dir, "settings.json"), root.ToJsonString());
                App.AlertClientTimeoutsEnabled = true;
                App.LoadAlertSettings(dir, key => "stored:" + key, (key, url) => Assert.Fail($"LoadAlertSettings wrote credential '{key}'."));
                Assert.False(App.AlertClientTimeoutsEnabled);
            }
            finally
            {
                App.AlertClientTimeoutsEnabled = before;
                Directory.Delete(dir, true);
            }
        });
    }

    /// <summary>#5881: the cooldown saved is the one in the box, not whatever the process-wide static holds at the
    /// root write. A second thread rewrites the static the whole time the window applies; the root must still carry the
    /// typed value on every pass.</summary>
    [Fact]
    public void TheSavedCooldown_IsTheBoxValue_WhileAnotherThreadRewritesTheStatic()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            window.AlertCooldownBox.Text = "7";
            using var stop = new System.Threading.ManualResetEventSlim();
            var hammer = new System.Threading.Thread(() =>
            {
                while (!stop.IsSet)
                    App.AlertCooldownMinutes = 9;
            });
            hammer.Start();
            try
            {
                for (var i = 0; i < 300; i++)
                {
                    var root = new JsonObject();
                    var errors = new SettingsValidationErrors();
                    window.ApplyAlertSettings(root, errors);
                    Assert.Equal(0, errors.Count);
                    Assert.Equal(7, root["alert_cooldown_minutes"]!.GetValue<int>());
                }
            }
            finally
            {
                stop.Set();
                hammer.Join();
            }
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
        StaTestThread.Run(() =>
        {
            var writes = new List<string>();
            var first = NewWindow(writes);
            first.SettingsTabs.SelectedItem = first.AccessTab;
            first.RememberSelectedTab();
            first.RememberSelectedTab();

            Assert.Equal(new[] { "AccessTab" }, writes);
            Assert.Equal("AccessTab", App.SettingsWindowTab);
            var second = NewWindow(remembered: null);
            Assert.Same(second.AccessTab, second.SettingsTabs.SelectedItem);
        });
    }

    [Fact]
    public void ATabNameNoTabHasAnyMore_OpensTheFirstTab()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow(remembered: "NoSuchTab");
            /* Nothing selected yet (the tab strip takes its first tab when it shows) or the first tab: never another. */
            Assert.True(window.SettingsTabs.SelectedIndex <= 0, $"opened on tab {window.SettingsTabs.SelectedIndex}");
        });
    }
}
