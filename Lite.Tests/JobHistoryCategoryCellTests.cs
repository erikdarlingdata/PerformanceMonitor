/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Markup;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8332, Lite's side: the Job History grid's Category cell shows the default category, "[Uncategorized (Local)]", whole. The real tab
/// is built on the shared STA thread with the app's theme, given one row, and the cell's text is measured against the cell. The Viewer's
/// twin is in ViewerSmallLayoutDefectsTests.
/// </summary>
public sealed class JobHistoryCategoryCellTests
{
    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    private static ResourceDictionary Theme() =>
        (ResourceDictionary)XamlReader.Parse(File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Themes", "DarkTheme.xaml")));

    [Fact]
    public void TheCategoryCell_ShowsTheDefaultCategoryWhole()
    {
        var clipped = StaTestThread.Run(() =>
        {
            var tab = new JobHistoryTab();
            /* The tab is laid out at 1600 wide inside a window that fits a 1024x768 screen. */
            var wide = new Border { Child = tab, Width = 1600, Height = 700, HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top };
            var window = new Window { Content = wide, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            window.Resources.MergedDictionaries.Add(Theme());
            StaTestThread.ShowOffScreen(window);
            try
            {
                tab.JobHistoryDataGrid.ItemsSource = new List<JobHistoryRow>
                {
                    new() { ServerId = 1, JobName = "nightly maintenance", CategoryName = "[Uncategorized (Local)]", RunStatusDesc = "Succeeded", Message = "The job succeeded." },
                };
                ClippedTextProbe.Settle(window);

                var found = new List<string>();
                var cells = ClippedTextProbe.Descendants(tab.JobHistoryDataGrid).OfType<DataGridCell>().ToList();
                if (cells.Count == 0)
                {
                    found.Add("the grid realized no cell");
                }

                foreach (var cell in cells)
                {
                    foreach (var block in ClippedTextProbe.Descendants(cell).OfType<TextBlock>().Where(b => b.IsVisible && !string.IsNullOrWhiteSpace(b.Text)))
                    {
                        var right = block.TransformToAncestor(cell).TransformBounds(new Rect(block.RenderSize)).Right;
                        if (right > cell.ActualWidth + 1)
                        {
                            found.Add($"'{block.Text}' ends at {right:F0} in a {cell.ActualWidth:F0} px cell");
                        }
                    }
                }

                return found;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(clipped.Count == 0, ClippedTextProbe.Describe(clipped));
    }
}
