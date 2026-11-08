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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's dialogs, rendered with its Dark theme: the column filter popup names a column by its header; the Edit Server
/// dialog heads itself Edit; Credential Profiles with no profiles says so inside its grid; Database State Overrides calls its state
/// column the last known state; the Analysis card's heading is sized and placed like the Delivery card's and its note points at
/// the Delivery tab; Manage Servers shows every column at its default size; and, from the theme, a disabled PasswordBox keeps
/// a disabled TextBox's fill while a chosen radio button marks itself with the accent.
/// </summary>
public sealed class ViewerDialogsWalkTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";
    private const string NoStore = "Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x";

    [Fact]
    public void TheAlertHistoryServerFilter_HeadsItsPopupWithTheColumnHeader()
    {
        var heading = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new AlertsHistoryTab();
            var window = new Window { Content = tab, Width = 1000, Height = 600, Left = 0, Top = 0 };
            Show(window);
            var button = ClippedTextProbe.Descendants(tab).OfType<Button>().Single(b => b.Tag is string tag && tag == "ServerName");
            button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

            var popup = (ColumnFilterPopup)typeof(AlertsHistoryTab).GetField("_filterPopupContent", Instance)!.GetValue(tab)!;
            var text = ((TextBlock)popup.FindName("HeaderText")).Text;
            ((Popup)typeof(AlertsHistoryTab).GetField("_filterPopup", Instance)!.GetValue(tab)!).IsOpen = false;
            window.Close();
            return text;
        }));

        Assert.Equal("Filter: Server", heading);
    }

    [Fact]
    public void TheEditServerDialog_HeadsItselfEditSqlServerConnection()
    {
        using var dir = new TempDir();
        var heading = StaTestThread.Run(() => WithTheme(() =>
        {
            var (servers, profiles) = dir.Stores();
            var row = new MonitoredServerRow { ServerId = 7, Name = "Edited", Host = "edited-host", Engine = MonitoredServerRow.EngineSqlServer, Auth = ServerStoreCredential.Sql, Username = "monitor" };
            var dialog = new AddServerDialog(new ViewerDataService(NoStore), servers, profiles, row, isFavorite: false);
            return ((TextBlock)dialog.FindName("HeaderText")).Text;
        }));

        Assert.Equal("Edit SQL Server Connection", heading);
    }

    [Fact]
    public void CredentialProfiles_WithNoProfiles_SaysSoInsideTheGrid()
    {
        using var dir = new TempDir();
        var found = StaTestThread.Run(() => WithTheme(() =>
        {
            var (_, profiles) = dir.Stores();
            var window = new ManageCredentialProfilesDialog(profiles);
            Show(window);
            var texts = TextBlocks(window).Where(t => t.IsVisible && t.Text.StartsWith("No credential profiles", StringComparison.Ordinal)).Select(t => t.Text).ToList();
            window.Close();
            return texts;
        }));

        Assert.Single(found);
    }

    [Fact]
    public void DatabaseStateOverrides_CallsTheStateColumnTheLastKnownState()
    {
        var headers = StaTestThread.Run(() => WithTheme(() =>
        {
            var window = new DatabaseStateOverridesWindow(new ViewerDataService(NoStore), Array.Empty<DarlingServer>());
            return ((DataGrid)window.FindName("StatesGrid")).Columns.Select(c => c.Header as string).ToList();
        }));

        Assert.Contains("Last Known State", headers);
        Assert.DoesNotContain("Current State", headers);
    }

    [Fact]
    public void TheAnalysisNote_PointsAtTheDeliveryTab()
    {
        var note = WithSettings(window =>
            TextBlocks(SelectTab(window, "AnalysisReportsTab")).Single(t => t.Text.StartsWith("Requires SMTP", StringComparison.Ordinal)).Text);

        Assert.Contains("Delivery tab", note, StringComparison.Ordinal);
        Assert.DoesNotContain("below", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAutomatedAnalysisHeading_IsSizedAndPlacedLikeTheDeliveryCardHeading()
    {
        var (analysis, delivery) = WithSettings(window =>
        {
            var deliveryHeading = TextBlocks(SelectTab(window, "DeliveryTab")).First(t => t.Text == "Delivery");
            var deliveryAt = (deliveryHeading.FontSize, Math.Round(LeftIn(deliveryHeading, CardOf(deliveryHeading)), 1));

            var analysisHeading = TextBlocks(SelectTab(window, "AnalysisReportsTab")).Single(t => t.Text == "Automated Analysis");
            var analysisAt = (analysisHeading.FontSize, Math.Round(LeftIn(analysisHeading, CardOf(analysisHeading)), 1));
            return (analysisAt, deliveryAt);
        });

        Assert.Equal(delivery.FontSize, analysis.FontSize);
        Assert.Equal(delivery.Item2, analysis.Item2);
    }

    [Fact]
    public void ManageServers_ShowsEveryColumnAtItsDefaultSize_WithNoHorizontalScrollBar()
    {
        using var dir = new TempDir();
        var (scrollBar, lastColumnRight, gridWidth) = StaTestThread.Run(() => WithTheme(() =>
        {
            var (servers, profiles) = dir.Stores();
            var window = new ManageServersWindow(servers, profiles, null);
            Show(window);
            var grid = (DataGrid)window.FindName("ServersGrid");
            var scroll = ClippedTextProbe.Descendants(grid).OfType<ScrollViewer>().First();
            var last = ClippedTextProbe.Descendants(grid).OfType<DataGridColumnHeader>().Single(h => h.Content as string == "Last Collected");
            var result = (scroll.ComputedHorizontalScrollBarVisibility, Math.Round(last.TransformToAncestor(grid).Transform(new Point(last.ActualWidth, 0)).X, 1), Math.Round(grid.ActualWidth, 1));
            window.Close();
            return result;
        }));

        Assert.True(scrollBar == Visibility.Collapsed, $"the grid needs a horizontal scroll bar at its default size (Last Collected ends at {lastColumnRight} in a {gridWidth} grid)");
        Assert.True(lastColumnRight <= gridWidth + 0.5, $"Last Collected ends at {lastColumnRight}, past the grid's {gridWidth}");
    }

    [Fact]
    public void ADisabledPasswordBox_DimsAsADisabledTextBoxDoes()
    {
        var (enabledText, disabledText, disabledPassword) = StaTestThread.Run(() => WithTheme(() =>
        {
            var enabledBox = new TextBox { Width = 160, Text = "smtp.example" };
            var disabledBox = new TextBox { IsEnabled = false, Width = 160, Text = "smtp.example" };
            var passwordBox = new PasswordBox { IsEnabled = false, Width = 160 };
            var window = new Window { Content = new StackPanel { Children = { enabledBox, disabledBox, passwordBox } }, Width = 300, Height = 200, Left = 0, Top = 0 };
            Show(window);
            var result = (enabledBox.Opacity, disabledBox.Opacity, passwordBox.Opacity);
            window.Close();
            return result;
        }));

        Assert.True(disabledText < enabledText, $"a disabled TextBox ({disabledText}) looks like an enabled one ({enabledText})");
        Assert.Equal(disabledText, disabledPassword);
    }

    [Fact]
    public void AChosenRadioButton_MarksItselfWithTheAccent()
    {
        var accentMarks = StaTestThread.Run(() => WithTheme(() =>
        {
            var radio = new RadioButton { Content = "SQL Server", IsChecked = true };
            var window = new Window { Content = new StackPanel { Children = { radio } }, Width = 300, Height = 120, Left = 0, Top = 0 };
            Show(window);
            var accent = ((SolidColorBrush)Application.Current.Resources["AccentBrush"]).Color;
            var marks = ClippedTextProbe.Descendants(radio).OfType<Shape>().Count(s => s.IsVisible && s.Fill is SolidColorBrush fill && fill.Color == accent);
            window.Close();
            return marks;
        }));

        Assert.Equal(1, accentMarks);
    }

    private static T WithSettings<T>(Func<SettingsWindow, T> body)
    {
        using var dir = new TempDir();
        return StaTestThread.Run(() =>
        {
            /* The theme goes on the window, not the application: the Settings window re-applies the application's theme by a
               relative URI the test process cannot resolve. */
            var window = new SettingsWindow(new ViewerPreferences(), new ViewerAppSettingsStore(System.IO.Path.Combine(dir.Path, "settings.json")), null);
            window.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri(ThemeUri) });
            Show(window);
            try
            {
                return body(window);
            }
            finally
            {
                /* An unsaved window re-applies the application's theme on close by a resource name only the running app serves. */
                typeof(SettingsWindow).GetField("_themeSaved", Instance)!.SetValue(window, true);
                window.Close();
            }
        });
    }

    /// <summary>Selects the tab and returns its content, which the TabControl presents outside the TabItem's own visual tree.</summary>
    private static DependencyObject SelectTab(SettingsWindow window, string tabName)
    {
        var tabs = (TabControl)window.FindName("SettingsTabs");
        var tab = (TabItem)window.FindName(tabName);
        tabs.SelectedItem = tab;
        ClippedTextProbe.Settle(window);
        return (DependencyObject)tab.Content;
    }

    private static void Show(Window window)
    {
        StaTestThread.ShowOffScreen(window);
        ClippedTextProbe.Settle(window);
    }

    private static IEnumerable<TextBlock> TextBlocks(DependencyObject root) => ClippedTextProbe.Descendants(root).OfType<TextBlock>();

    private static double LeftIn(FrameworkElement element, Visual ancestor) => element.TransformToAncestor(ancestor).Transform(new Point(0, 0)).X;

    private static Border CardOf(DependencyObject element)
    {
        var at = VisualTreeHelper.GetParent(element);
        while (at is not null && at is not Border)
        {
            at = VisualTreeHelper.GetParent(at);
        }

        return (Border)at!;
    }

    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("BackgroundLightBrush"))
        {
            added = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }

    private sealed class TempDir : IDisposable
    {
        public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ViewerDialogsWalk_" + Guid.NewGuid().ToString("N")[..8]);

        public TempDir() => Directory.CreateDirectory(Path);

        public (ViewerServerStore Servers, ViewerProfileStore Profiles) Stores()
        {
            var secrets = new NoSecrets();
            var servers = new ViewerServerStore(System.IO.Path.Combine(Path, "servers.json"), secrets);
            return (servers, new ViewerProfileStore(servers, System.IO.Path.Combine(Path, "profiles.json"), secrets));
        }

        public void Dispose()
        {
            try { Directory.Delete(Path, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private sealed class NoSecrets : IViewerServerSecretStore
    {
        public void Save(string id, string username, string password) { }
        public (string Username, string Password)? Find(string id) => null;
        public void Delete(string id) { }
    }
}
