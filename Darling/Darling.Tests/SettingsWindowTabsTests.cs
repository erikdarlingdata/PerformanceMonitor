/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5846: the Darling Viewer's Settings window, split into tabs. These build the real window (disconnected, on a
/// throwaway viewer-settings file) and check what an operator meets: every control on a tab, each tab saying its
/// scope, a Save with an invalid value opening the tab that holds it, and the window reopening on the last tab.
/// </summary>
public sealed class SettingsWindowTabsTests : IDisposable
{
    private readonly string _settingsFile =
        Path.Combine(Path.GetTempPath(), $"darling-settings-tabs-{Guid.NewGuid():N}.json");

    public void Dispose()
    {
        if (File.Exists(_settingsFile))
        {
            File.Delete(_settingsFile);
        }
    }

    private SettingsWindow NewWindow() =>
        new(new ViewerPreferences(), new ViewerAppSettingsStore(_settingsFile), dataService: null);

    [Fact]
    public void EveryNamedControlSitsOnATab_OnlyTheBannerAndTheTabStripAreWindowWide()
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
            Assert.Equal(new[] { "SettingsReadOnlyBanner", "SettingsTabs" }, offTab);

            /* And every tab a control sits on is one the tab strip shows. */
            var tabs = window.SettingsTabs.Items.OfType<TabItem>().ToList();
            Assert.All(named.Select(x => SettingsWindow.TabOf(x.Element)).OfType<TabItem>(),
                tab => Assert.Contains(tab, tabs));
        });
    }

    [Fact]
    public void EveryTabOpensWithAScopeLine()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            var scopeStyle = (Style)window.FindResource("ScopeLineStyle");
            var tabs = window.SettingsTabs.Items.OfType<TabItem>().ToList();
            Assert.NotEmpty(tabs);
            Assert.All(tabs, tab =>
            {
                var panel = Assert.IsType<StackPanel>(Assert.IsType<ScrollViewer>(tab.Content).Content);
                var first = Assert.IsType<TextBlock>(panel.Children[0]);
                Assert.Same(scopeStyle, first.Style);
                Assert.False(string.IsNullOrWhiteSpace(first.Text), $"{tab.Header}: the scope line is empty");
            });
        });
    }

    [Fact]
    public void SaveValidation_OpensTheFirstTabHoldingAnInvalidValue_InTabOrder()
    {
        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            window.SettingsTabs.SelectedItem = window.ViewerTab;

            /* Save checks the analysis interval before the per-event cap, but Delivery sits left of Analysis &
               reports: the operator is taken to the leftmost tab with a problem. */
            window.AnalysisIntervalBox.Text = "1";
            window.AlertPerEventMaxBox.Text = "0";
            var errors = new SettingsValidationErrors();
            window.BuildAlertRowFromControls(errors);

            Assert.Equal(2, errors.Count);
            Assert.Same(window.DeliveryTab, window.SelectFirstTabWithError(errors));
            Assert.Same(window.DeliveryTab, window.SettingsTabs.SelectedItem);
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
            window.BuildAlertRowFromControls(clean);
            Assert.Equal(0, clean.Count);
            Assert.Null(window.SelectFirstTabWithError(clean));
            Assert.Same(window.CollectionTab, window.SettingsTabs.SelectedItem);

            window.FleetSweepIntervalBox.Text = "0";
            var errors = new SettingsValidationErrors();
            window.BuildAlertRowFromControls(errors);
            Assert.Same(window.AnalysisReportsTab, window.SelectFirstTabWithError(errors));
            Assert.Same(window.AnalysisReportsTab, window.SettingsTabs.SelectedItem);
        });
    }

    [Fact]
    public void TheWindowReopensOnTheLastTab_KeptInTheViewersOwnSettings()
    {
        StaTestThread.Run(() =>
        {
            var first = NewWindow();
            first.SettingsTabs.SelectedItem = first.AccessTab;
            first.RememberSelectedTab();

            Assert.Equal("AccessTab", new ViewerAppSettingsStore(_settingsFile).Load().SettingsWindowTab);
            var second = NewWindow();
            Assert.Same(second.AccessTab, second.SettingsTabs.SelectedItem);
        });
    }

    [Fact]
    public void ATabNameNoTabHasAnyMore_OpensTheFirstTab()
    {
        var store = new ViewerAppSettingsStore(_settingsFile);
        var settings = store.Load();
        settings.SettingsWindowTab = "NoSuchTab";
        Assert.True(store.Save(settings));

        StaTestThread.Run(() =>
        {
            var window = NewWindow();
            /* Nothing selected yet (the tab strip takes its first tab when it shows) or the first tab: never another. */
            Assert.True(window.SettingsTabs.SelectedIndex <= 0, $"opened on tab {window.SettingsTabs.SelectedIndex}");
        });
    }
}
