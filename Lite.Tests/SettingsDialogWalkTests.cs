/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Shapes;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Settings, rendered with the Dark theme at the window's own size: the Client Timeouts row keeps its two baseline multiples
/// together on one line; the Analysis card's heading is sized and placed like the Delivery card's and its note points at the
/// Delivery tab; and, from the theme itself, a disabled PasswordBox keeps the fill a disabled TextBox has, while a chosen radio
/// button marks itself with the accent the CheckBox mark uses.
/// </summary>
[Collection("credential-manager")]
public sealed class SettingsDialogWalkTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void TheClientTimeoutsRow_KeepsBothMultiplesAndTheirBaseline_OnOneLine()
    {
        var tops = WithSettings(window =>
        {
            var tab = SelectTab(window, "AlertRulesTab");
            var warning = (FrameworkElement)window.FindName("AlertClientTimeoutsWarningMultipleBox");
            var critical = (FrameworkElement)window.FindName("AlertClientTimeoutsCriticalMultipleBox");
            var between = DialogWalkTestHost.TextBlocks(tab).Single(t => t.Text == "x /");
            var baseline = DialogWalkTestHost.TextBlocks(tab).Single(t => t.Text == "x the server's own baseline");
            /* A TextBox is taller than a TextBlock, so what one line shares is the vertical centre, not the top. */
            return new[] { warning, between, critical, baseline }
                .Select(e => (e is TextBlock t ? t.Text : ((FrameworkElement)e).Name, Math.Round(DialogWalkTestHost.TopIn(e, window) + e.ActualHeight / 2)))
                .ToList();
        });

        Assert.True(tops.Select(t => t.Item2).Distinct().Count() == 1,
            "the multiples split across lines: " + string.Join(", ", tops.Select(t => $"{t.Item1} centred at y={t.Item2}")));
    }

    [Fact]
    public void TheAnalysisNote_PointsAtTheDeliveryTab()
    {
        var note = WithSettings(window =>
            DialogWalkTestHost.TextBlocks(SelectTab(window, "AnalysisTab")).Single(t => t.Text.StartsWith("Requires SMTP", StringComparison.Ordinal)).Text);

        Assert.Contains("Delivery tab", note, StringComparison.Ordinal);
        Assert.DoesNotContain("below", note, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAutomatedAnalysisHeading_IsSizedAndPlacedLikeTheDeliveryCardHeading()
    {
        var (analysis, delivery) = WithSettings(window =>
        {
            var deliveryHeading = DialogWalkTestHost.TextBlocks(SelectTab(window, "DeliveryTab")).First(t => t.Text == "Alert Delivery");
            var deliveryAt = (deliveryHeading.FontSize, Math.Round(DialogWalkTestHost.LeftIn(deliveryHeading, DialogWalkTestHost.CardOf(deliveryHeading)), 1));

            var analysisHeading = DialogWalkTestHost.TextBlocks(SelectTab(window, "AnalysisTab")).Single(t => t.Text == "Automated Analysis");
            var analysisAt = (analysisHeading.FontSize, Math.Round(DialogWalkTestHost.LeftIn(analysisHeading, DialogWalkTestHost.CardOf(analysisHeading)), 1));
            return (analysisAt, deliveryAt);
        });

        Assert.Equal(delivery.FontSize, analysis.FontSize);
        Assert.Equal(delivery.Item2, analysis.Item2);
    }

    [Fact]
    public void ADisabledPasswordBox_KeepsTheFillADisabledTextBoxHas()
    {
        var (password, text) = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
        {
            var passwordBox = new PasswordBox { IsEnabled = false, Width = 160 };
            var textBox = new TextBox { IsEnabled = false, Width = 160, Text = "smtp.example" };
            var window = new Window { Content = new StackPanel { Children = { passwordBox, textBox } }, Width = 300, Height = 200, Left = 0, Top = 0 };
            DialogWalkTestHost.Show(window);
            var result = (passwordBox.Opacity, textBox.Opacity);
            window.Close();
            return result;
        }));

        Assert.Equal(text, password);
    }

    [Fact]
    public void AChosenRadioButton_MarksItselfWithTheAccent()
    {
        var accentMarks = StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
        {
            var radio = new RadioButton { Content = "SQL Server Authentication", IsChecked = true };
            var window = new Window { Content = new StackPanel { Children = { radio } }, Width = 300, Height = 120, Left = 0, Top = 0 };
            DialogWalkTestHost.Show(window);
            var accent = ((SolidColorBrush)Application.Current.Resources["AccentBrush"]).Color;
            var marks = ClippedTextProbe.Descendants(radio).OfType<Shape>()
                .Count(s => s.IsVisible && s.Fill is SolidColorBrush fill && fill.Color == accent);
            window.Close();
            return marks;
        }));

        Assert.Equal(1, accentMarks);
    }

    private static T WithSettings<T>(Func<SettingsWindow, T> body)
    {
        var dir = DialogWalkTestHost.ScratchDir("SettingsWalk");
        try
        {
            return StaTestThread.Run(() => DialogWalkTestHost.WithTheme(() =>
            {
                /* Closing the window remembers its tab in a process-wide static and writes settings.json; both are kept off this test. */
                var rememberedTab = App.SettingsWindowTab;
                var window = new SettingsWindow(new ScheduleManager(dir), new ServerManager(dir)) { PersistSelectedTab = _ => { } };
                DialogWalkTestHost.Show(window);
                try
                {
                    return body(window);
                }
                finally
                {
                    /* An unsaved window re-applies the original theme on close, which needs the running app's pack resources. */
                    typeof(SettingsWindow).GetField("_saved", Instance)!.SetValue(window, true);
                    window.Close();
                    App.SettingsWindowTab = rememberedTab;
                }
            }));
        }
        finally
        {
            DialogWalkTestHost.Delete(dir);
        }
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
}
