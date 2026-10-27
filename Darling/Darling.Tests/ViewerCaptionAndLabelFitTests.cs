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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6832: at 1366x768 the Viewer cut three captions and a card label, and the time range menu's Calendar list ran each label into its
/// duration. Each element is laid out at the width it gets on a 1366 screen and checked: wrapping text keeps all of its text, a label
/// shows in full, and the label and duration keep a gap.
/// </summary>
public sealed class ViewerCaptionAndLabelFitTests
{
    private const double ScreenWidth = 1366;
    private const double ScreenHeight = 768;

    private static string ViewerDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "Darling", "PerformanceMonitor.Darling.Viewer"));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(ViewerDir(), "Themes", "DarkTheme.xaml")));

    /// <summary>Runs the body on the shared STA thread with the app's Dark theme merged at application level, as App.xaml supplies it (the tabs' static resources need it), and takes it back out.</summary>
    private static T RunThemed<T>(Func<T> body) => StaTestThread.Run(() =>
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var theme = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
        app.Resources.MergedDictionaries.Add(theme);
        try
        {
            return body();
        }
        finally
        {
            app.Resources.MergedDictionaries.Remove(theme);
        }
    });

    private static IEnumerable<T> Logical<T>(DependencyObject root) where T : DependencyObject
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            if (child is T hit)
            {
                yield return hit;
            }

            foreach (var inner in Logical<T>(child))
            {
                yield return inner;
            }
        }
    }

    /// <summary>Selects every tab between the element and the root, so the element is laid out and visible.</summary>
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem tab && ItemsControl.ItemsControlFromItemContainer(tab) is TabControl owner)
            {
                owner.SelectedItem = tab;
            }
        }
    }

    // ---- 1. The Performance Trends source caption wraps, and carries no em dash ----

    private static QueryTrendSeries TruncatedSeries()
        => new(new List<QueryTrendPoint>(), RetentionTier.Raw, new DateTime(2026, 10, 26, 10, 3, 0, DateTimeKind.Unspecified), true);

    [Fact]
    public void TheSourceCaption_WrapsInsideTheChart_AtTheWidthOf1366()
    {
        StaTestThread.Run(() =>
        {
            var caption = ViewerServerTab.DescribeTrendCoverage(TruncatedSeries());
            Assert.DoesNotContain('—', caption);

            /* The four trend charts sit two across: half of 1366 less the tab margins. */
            var grid = new Grid { Width = (ScreenWidth - 66) / 2, Height = 300 };
            var chart = new WpfPlot();
            grid.Children.Add(chart);
            grid.Measure(new Size(grid.Width, grid.Height));
            grid.Arrange(new Rect(0, 0, grid.Width, grid.Height));
            grid.UpdateLayout();
            ChartStyle.ApplyThemeToChart(chart);
            chart.Plot.Add.Scatter(new double[] { 1, 2, 3 }, new double[] { 1, 2, 3 });

            ViewerServerTab.ShowTrendCoverageTitle(chart, caption);
            using var image = chart.Plot.GetImage((int)chart.ActualWidth, (int)chart.ActualHeight);

            /* The shown title is the whole caption, broken into lines, and no line is wider than the chart it is drawn in. */
            var shown = chart.Plot.Axes.Title.Label.Text;
            Assert.Equal(caption, shown.Replace('\n', ' '));
            using var paint = ScottPlot.Paint.NewDisposablePaint();
            var title = chart.Plot.Axes.Title.Label.Measure(paint).Size;
            Assert.True(title.Width <= chart.ActualWidth - ChartCaption.AxisMargin, $"the caption is {title.Width:0} px wide in a {chart.ActualWidth:0} px chart (the plot starts {ChartCaption.AxisMargin:0} px in)");
        });
    }

    [Fact]
    public void ACaptionThatFits_IsLeftOnOneLine()
    {
        Assert.Equal("Source: raw (one point per collection)", ChartCaption.Wrap("Source: raw (one point per collection)", 650, 11));
    }

    // ---- 2. The Query Store Regressions captions wrap ----

    [Fact]
    public void TheRegressionsCaptions_WrapAndKeepAllTheirText_At1366()
    {
        var clipped = RunThemed(() =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "caption-fit-test", "caption-fit-test", true, null));
            var window = new Window { Content = tab, Width = ScreenWidth, Height = ScreenHeight };
            window.Resources.MergedDictionaries.Add(Theme());
            window.Show();
            try
            {
                typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.SetValue(tab, true);
                var banner = (TextBlock)tab.FindName("QueryStoreRegressionsTruncationBanner");
                banner.Text = "Showing since 2026-10-26 10:03: this server's Query Store history starts after the baseline window does, so a regression that can come only from a run before the window that is not held here cannot be listed.";
                banner.Visibility = Visibility.Visible;
                var caption = Logical<TextBlock>(tab).Single(b => b.Text.StartsWith("Queries whose Query Store performance regressed", StringComparison.Ordinal));
                Reveal(caption);
                ClippedTextProbe.Settle(window);

                var found = ClippedTextProbe.Clipped((DependencyObject)caption.Parent, tab);
                Assert.Equal(TextWrapping.Wrap, caption.TextWrapping);
                Assert.Equal(TextWrapping.Wrap, banner.TextWrapping);
                Assert.True(caption.ActualHeight > caption.FontSize * 2.4, $"the caption is one line tall ({caption.ActualHeight:0} px)");
                return found;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }

    // ---- 3. The FinOps Utilization CPU labels show in full ----

    [Fact]
    public void TheCpuCardLabels_ShowInFull_At1366()
    {
        var clipped = RunThemed(() =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = ScreenWidth, Height = ScreenHeight };
            window.Resources.MergedDictionaries.Add(Theme());
            window.Show();
            try
            {
                ((FrameworkElement)tab.FindName("FinOpsUtilizationContent")).Visibility = Visibility.Visible;
                var labels = Logical<TextBlock>(tab)
                    .Where(b => b.Text is ViewerLabels.AvgSqlServerCpu or ViewerLabels.P95SqlServerCpu or ViewerLabels.MaxSqlServerCpu)
                    .ToList();
                Assert.Equal(3, labels.Count);
                Reveal(labels[0]);
                ClippedTextProbe.Settle(window);

                var found = ClippedTextProbe.Clipped((DependencyObject)labels[0].Parent, tab);
                foreach (var label in labels)
                {
                    var room = ((Grid)label.Parent).ColumnDefinitions[0].ActualWidth;
                    if (ClippedTextProbe.NaturalWidth(label) > room + 1)
                    {
                        found.Add($"label '{label.Text}' needs {ClippedTextProbe.NaturalWidth(label):0} px but its column is {room:0} px");
                    }
                }

                var widths = labels.Select(l => Math.Round(l.ActualWidth)).Distinct().ToList();
                Assert.True(found.Count > 0 || widths.Count == 1, "the three labels do not share one column, so their bars do not line up");
                return found;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }

    // ---- 4. The Calendar list keeps the label and its duration apart ----

    [Fact]
    public void TheCalendarList_KeepsEachLabelClearOfItsDuration_AndTheDurationAtTheRightEdge()
    {
        var problems = StaTestThread.Run(() =>
        {
            var picker = new TimeRangePicker
            {
                ZoneProvider = () => TimeZoneInfo.Utc,
                NowProvider = () => new DateTime(2026, 10, 8, 7, 52, 0, DateTimeKind.Unspecified),
            };
            var window = new Window { Content = picker, Width = ScreenWidth, Height = ScreenHeight };
            window.Resources.MergedDictionaries.Add(Theme());
            window.Show();
            var found = new List<string>();
            try
            {
                picker.PickerPopup.IsOpen = true;
                ClippedTextProbe.Settle(window);

                var rows = picker.PeriodPanel.Children.OfType<Button>().ToList();
                Assert.Equal(8, rows.Count);
                foreach (var button in rows)
                {
                    var row = (Grid)button.Content;
                    var label = (TextBlock)row.Children[0];
                    var length = (TextBlock)row.Children[1];
                    if (string.IsNullOrEmpty(length.Text))
                    {
                        continue;
                    }

                    var labelRight = label.TransformToAncestor(button).TransformBounds(new Rect(0, 0, label.ActualWidth, label.ActualHeight)).Right;
                    var lengthBox = length.TransformToAncestor(button).TransformBounds(new Rect(0, 0, length.ActualWidth, length.ActualHeight));
                    if (lengthBox.Left - labelRight < 8)
                    {
                        found.Add($"'{label.Text}' ends at {labelRight:0} and '{length.Text}' starts at {lengthBox.Left:0}");
                    }

                    var inner = button.ActualWidth - button.Padding.Right - button.BorderThickness.Right;
                    if (lengthBox.Right < inner - 1.5)
                    {
                        found.Add($"'{length.Text}' ends at {lengthBox.Right:0}, short of the row's right edge {inner:0}");
                    }
                }
            }
            finally
            {
                picker.PickerPopup.IsOpen = false;
                window.Close();
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}
