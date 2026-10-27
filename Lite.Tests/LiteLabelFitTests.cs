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
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6832: the Darling Viewer's twin is ViewerCaptionAndLabelFitTests in Darling.Tests. Lite shares the time range menu and has the same FinOps
/// Utilization CPU card, so both are laid out at the 1366x768 width with Lite's own theme: the CPU labels show in full and the Calendar list
/// keeps each label clear of its duration.
/// </summary>
public class LiteLabelFitTests
{
    private const double ScreenWidth = 1366;
    private const double ScreenHeight = 768;

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Themes", "DarkTheme.xaml")));

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

    [Fact]
    public void TheCpuCardLabels_ShowInFull_At1366()
    {
        var found = StaTestThread.Run(() =>
        {
            var tab = new FinOpsTab();
            var window = new Window { Content = tab, Width = ScreenWidth, Height = ScreenHeight };
            window.Resources.MergedDictionaries.Add(Theme());
            window.Show();
            try
            {
                ((FrameworkElement)tab.FindName("UtilizationContent")).Visibility = Visibility.Visible;
                var labels = Logical<TextBlock>(tab)
                    .Where(b => b.Text is "Avg SQL Server CPU" or "P95 SQL Server CPU" or "Max SQL Server CPU")
                    .ToList();
                Assert.Equal(3, labels.Count);
                Reveal(labels[0]);
                ClippedTextProbe.Settle(window);

                var problems = new List<string>();
                foreach (var label in labels)
                {
                    var room = ((Grid)label.Parent).ColumnDefinitions[0].ActualWidth;
                    if (ClippedTextProbe.NaturalWidth(label) > room + 1)
                    {
                        problems.Add($"label '{label.Text}' needs {ClippedTextProbe.NaturalWidth(label):0} px but its column is {room:0} px");
                    }
                }

                if (problems.Count == 0)
                {
                    Assert.Single(labels.Select(l => Math.Round(l.ActualWidth)).Distinct());
                }

                return problems;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(found.Count == 0, string.Join(Environment.NewLine, found));
    }

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
