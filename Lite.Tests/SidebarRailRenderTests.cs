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
using System.Windows.Documents;
using Ellipse = System.Windows.Shapes.Ellipse;
using Darling.Tests;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Models;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The sidebar as a rail (#8306): the window at 900 px wide collapses the sidebar to 40 px. Each server row keeps its whole
/// status dot inside the list, the footer's buttons show their icons whole, and the open sidebar's subtitle sits on one line.
/// The window is the real one, laid out in a host window of the stated size under each theme.
/// </summary>
[Collection("credential-manager")]
public sealed class SidebarRailRenderTests
{
    public static TheoryData<string> Themes => new() { "Dark", "Light", "CoolBreeze" };

    private static Uri ThemeUri(string name) => new($"pack://application:,,,/PerformanceMonitorLite;component/Themes/{name}Theme.xaml");

    private static T WithWindow<T>(string theme, double width, Func<MainWindow, Window, T> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "SidebarRail_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            return StaTestThread.Run(() =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                var before = app.Resources.MergedDictionaries.ToList();
                var previous = ThemeManager.CurrentTheme;
                ThemeManager.ThemeUriProvider = ThemeUri;
                try
                {
                    ThemeManager.Apply(theme);
                    foreach (var (name, value) in new[] { ("DatabasePath", Path.Combine(dir, "monitor.duckdb")), ("ConfigDirectory", dir), ("SharedConfigDirectory", dir) })
                    {
                        typeof(App).GetProperty(name, BindingFlags.Static | BindingFlags.Public)!.SetValue(null, value);
                    }

                    var w = new MainWindow();
                    var root = (UIElement)w.Content;
                    w.Content = null;
                    var host = new Window { Content = root, Width = width, Height = 600, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
                    StaTestThread.ShowOffScreen(host);
                    try
                    {
                        ClippedTextProbe.Settle(host);
                        return body(w, host);
                    }
                    finally
                    {
                        host.Close();
                    }
                }
                finally
                {
                    ThemeManager.Apply(previous);
                    app.Resources.MergedDictionaries.Clear();
                    foreach (var dictionary in before)
                    {
                        app.Resources.MergedDictionaries.Add(dictionary);
                    }

                    ThemeManager.ThemeUriProvider = ThemeManager.ThemeUri;
                }
            });
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static void Collapse(MainWindow w, Window host)
    {
        var column = (ColumnDefinition)w.FindName("SidebarColumn");
        if (column.Width.Value > 40)
        {
            ((Button)w.FindName("SidebarToggleButton")).RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
        }

        ClippedTextProbe.Settle(host);
        Assert.Equal(40, column.ActualWidth);
    }

    private static List<FleetRow> Rows(int count) =>
        Enumerable.Range(1, count)
            .Select(i => (FleetRow)new FleetServerRow(new FleetServer(i, $"server-{i:00}", false, new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = $"server-{i:00}", DisplayName = $"server-{i:00}", IsEnabled = true }), 0))
            .ToList();

    private static Rect BoundsIn(FrameworkElement element, Visual root) =>
        element.TransformToAncestor(root).TransformBounds(new Rect(0, 0, element.ActualWidth, element.ActualHeight));

    // ── Item 1: the status dot ──────────────────────────────────────────────────────────────────

    /// <summary>With enough servers to scroll, every status dot the rail shows is a whole circle inside the list's viewport, not cut by the list's edge or its scroll bar.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheRail_ShowsEachServersStatusDotWhole(string theme)
    {
        var (dots, viewport, clipped) = WithWindow(theme, 900, (w, host) =>
        {
            var list = (ListBox)w.FindName("ServerListView");
            list.ItemsSource = Rows(40);
            Collapse(w, host);
            var presenter = ClippedTextProbe.Descendants(list).OfType<ScrollContentPresenter>().First();
            var shown = ClippedTextProbe.Descendants(presenter).OfType<Ellipse>()
                .Where(e => e.IsVisible && e.ActualWidth > 0)
                .Select(e => BoundsIn(e, presenter))
                .Where(b => b.Top >= 0 && b.Bottom <= presenter.ActualHeight)
                .ToList();
            var cut = shown.Where(b => b.Left < 0 || b.Right > presenter.ActualWidth + 0.5).Select(b => $"{b.Left:F1}..{b.Right:F1}").ToList();
            return (shown.Count, presenter.ActualWidth, cut);
        });

        Assert.True(dots >= 5, $"{theme}: only {dots} status dots are in view in the rail");
        Assert.True(clipped.Count == 0, $"{theme}: {clipped.Count} of {dots} status dots run past the {viewport:F1} px list: {string.Join(", ", clipped)}");
    }

    // ── Item 8: the footer ──────────────────────────────────────────────────────────────────────

    /// <summary>Each footer button in the rail shows its icon whole inside the button, in a colour that reads against the button's face.</summary>
    [Theory]
    [MemberData(nameof(Themes))]
    public void TheRailFooter_ShowsEveryButtonsIconWhole(string theme)
    {
        var findings = WithWindow(theme, 900, (w, host) =>
        {
            Collapse(w, host);
            var footer = (SidebarFooter)w.FindName("Sidebar");
            var results = new List<string>();
            foreach (var button in ClippedTextProbe.Descendants(footer).OfType<Button>())
            {
                var icon = ClippedTextProbe.Descendants(button).OfType<TextBlock>().FirstOrDefault(t => t.FontFamily.Source.Contains("MDL2", StringComparison.Ordinal));
                if (icon is null)
                {
                    results.Add($"{button.Name}: no icon");
                    continue;
                }

                if (!button.IsVisible || !icon.IsVisible)
                {
                    results.Add($"{button.Name}: not shown");
                    continue;
                }

                var bounds = BoundsIn(icon, button);
                var natural = ClippedTextProbe.NaturalWidth(icon);
                if (bounds.Left < 0 || bounds.Left + natural > button.ActualWidth + 0.5)
                {
                    results.Add($"{button.Name}: the icon spans {bounds.Left:F1}..{bounds.Left + natural:F1} in a {button.ActualWidth:F1} px button");
                    continue;
                }

                var face = button.Background as SolidColorBrush;
                var ink = icon.Foreground as SolidColorBrush ?? TextElement.GetForeground(button) as SolidColorBrush;
                if (face is null || ink is null || Contrast(face.Color, ink.Color) < 3.0)
                {
                    results.Add($"{button.Name}: the icon {Hex(ink)} on the face {Hex(face)} reads at {(face is null || ink is null ? 0 : Contrast(face.Color, ink.Color)):F2}");
                }
            }

            return results;
        });

        Assert.True(findings.Count == 0, $"{theme}: {string.Join("; ", findings)}");
    }

    // ── Item 5: the subtitle ────────────────────────────────────────────────────────────────────

    /// <summary>The open sidebar's subtitle fits on one line at the sidebar's width.</summary>
    [Fact]
    public void TheOpenSidebarSubtitle_StaysOnOneLine()
    {
        var (natural, width, height, fontSize) = WithWindow("Dark", 1200, (w, host) =>
        {
            var column = (ColumnDefinition)w.FindName("SidebarColumn");
            Assert.Equal(280, column.ActualWidth);
            var subtitle = (TextBlock)w.FindName("SidebarSubtitle");
            return (ClippedTextProbe.NaturalWidth(subtitle), subtitle.ActualWidth, subtitle.ActualHeight, subtitle.FontSize);
        });

        Assert.True(natural <= width + 0.5, $"the subtitle needs {natural:F1} px on one line and has {width:F1}");
        Assert.True(height < fontSize * 2, $"the subtitle is {height:F1} px tall at a {fontSize} px font: it wrapped");
    }

    private static double Luminance(Color c)
    {
        static double Channel(byte v)
        {
            var d = v / 255.0;
            return d <= 0.03928 ? d / 12.92 : Math.Pow((d + 0.055) / 1.055, 2.4);
        }

        return 0.2126 * Channel(c.R) + 0.7152 * Channel(c.G) + 0.0722 * Channel(c.B);
    }

    private static double Contrast(Color a, Color b)
    {
        var (la, lb) = (Luminance(a), Luminance(b));
        return (Math.Max(la, lb) + 0.05) / (Math.Min(la, lb) + 0.05);
    }

    private static string Hex(SolidColorBrush? brush) => brush is null ? "(none)" : $"#{brush.Color.R:X2}{brush.Color.G:X2}{brush.Color.B:X2}";
}
