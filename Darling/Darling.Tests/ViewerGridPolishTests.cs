/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8197: the real Viewer server tab, built on the shared STA thread and themed as the app themes it. Every grid column is as wide as its
/// header (also with no rows), an empty window shows one muted sentence in the grid area with the slicer band gone, and the Memory
/// Broker panel does not repeat its tab's name as a title.
/// </summary>
public sealed class ViewerGridPolishTests
{
    [Theory]
    [InlineData("DarkTheme", 1280, 800)]
    [InlineData("LightTheme", 1280, 800)]
    [InlineData("CoolBreezeTheme", 1280, 800)]
    [InlineData("DarkTheme", 900, 600)]
    [InlineData("CoolBreezeTheme", 900, 600)]
    public void EveryColumnOfTheQueryGrids_IsAsWideAsItsHeader_WithNoRows(string theme, int width, int height)
    {
        var clipped = Run(theme, width, height, (tab, window) =>
        {
            var found = new List<string>();
            foreach (var name in new[] { "QueryStatsGrid", "ProcedureStatsGrid", "QueryStoreGrid" })
            {
                var grid = (DataGrid)tab.FindName(name);
                SelectTabOf(grid);
                ClippedTextProbe.Settle(window);
                Assert.Empty(grid.Items);
                found.AddRange(ClippedTextProbe.ClippedHeaders(grid).Select(c => $"[{name}] {c}"));
            }

            return found;
        });

        Assert.True(clipped.Count == 0, $"{theme} {width}x{height}:{Environment.NewLine}" + ClippedTextProbe.Describe(clipped));
    }

    [Theory]
    [InlineData("ActiveQueriesSlicer")]
    [InlineData("QueryStatsSlicer")]
    [InlineData("ProcStatsSlicer")]
    [InlineData("QueryStoreSlicer")]
    [InlineData("BlockingSlicer")]
    [InlineData("DeadlockSlicer")]
    public void AnEmptyWindow_ShowsOneMutedSentenceInTheGridArea_AndTheSlicerBandCollapses(string slicerName)
    {
        const string sentence = "Nothing was collected in the selected time window.";
        var problems = Run("DarkTheme", 1280, 800, (tab, window) =>
        {
            var slicer = (TimeRangeSlicerControl)tab.FindName(slicerName);
            slicer.ShowEmpty(sentence);
            SelectTabOf(slicer);
            ClippedTextProbe.Settle(window);

            var found = new List<string>();
            var shown = ClippedTextProbe.Descendants(tab).OfType<TextBlock>().Where(b => b.IsVisible && b.Text == sentence).ToList();
            if (shown.Count != 1)
            {
                found.Add($"{shown.Count} visible copies of the sentence, expected 1");
            }

            if (slicer.IsVisible || slicer.ActualHeight > 0)
            {
                found.Add($"the slicer band still takes {slicer.ActualHeight:0} px");
            }

            if (shown.Count == 1)
            {
                var block = shown[0];
                var muted = (SolidColorBrush)Application.Current.FindResource("ForegroundMutedBrush");
                if (block.FontStyle == FontStyles.Italic || block.TextAlignment != TextAlignment.Center || block.Foreground is not SolidColorBrush b || b.Color != muted.Color)
                {
                    found.Add("the sentence is not the muted, centered, upright EmptyHint look");
                }

                var grid = ((Panel)LogicalTreeHelper.GetParent(slicer)).Children.OfType<DataGrid>().FirstOrDefault();
                if (grid is null || !new Rect(grid.TransformToAncestor(tab).Transform(new Point(0, 0)), grid.RenderSize)
                        .Contains(block.TransformToAncestor(tab).Transform(new Point(block.ActualWidth / 2, block.ActualHeight / 2))))
                {
                    found.Add("the sentence is not in the grid area");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public void TheMemoryBrokerPanel_HasNoTitleOfItsOwn_OnlyTheTargetShrinksSubtitle()
    {
        var problems = Run("DarkTheme", 1280, 800, (tab, window) =>
        {
            var grid = (DataGrid)tab.FindName("MemoryBrokerGrid");
            SelectTabOf(grid);
            ClippedTextProbe.Settle(window);
            var panel = (FrameworkElement)LogicalTreeHelper.GetParent(LogicalTreeHelper.GetParent(grid));
            var inGrid = ClippedTextProbe.Descendants(grid).ToHashSet();
            var texts = ClippedTextProbe.Descendants(panel).OfType<TextBlock>()
                .Where(b => b.IsVisible && b.Text.Length > 0 && !inGrid.Contains(b)).ToList();
            var found = texts.Where(b => b.FontSize >= 14).Select(b => $"title-level text '{b.Text}' ({b.FontSize} px)").ToList();
            if (!texts.Any(b => b.Text.Contains("target shrinks", StringComparison.OrdinalIgnoreCase) && b.FontSize < 14))
            {
                found.Add("no 'target shrinks' subtitle");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>Shows every tab on the way to <paramref name="element"/>, as a click on each would.</summary>
    private static void SelectTabOf(DependencyObject element)
    {
        for (var node = element; node is not null; node = LogicalTreeHelper.GetParent(node))
        {
            if (node is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl tabs)
            {
                tabs.SelectedItem = item;
            }
        }
    }

    private static List<string> Run(string theme, int width, int height, Func<ViewerServerTab, Window, List<string>> body) =>
        StaTestThread.Run(() => WithTheme(theme, () =>
        {
            var tab = new ViewerServerTab(
                new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                new DarlingServer(1, "grid-polish", "grid-polish", true, null));
            var window = new Window { Content = tab, Width = width, Height = height, Left = 0, Top = 0 };
            window.Show();
            try
            {
                return body(tab, window);
            }
            finally
            {
                window.Close();
            }
        }));

    private static T WithTheme<T>(string theme, Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        var added = new List<ResourceDictionary>();
        if (!app.Resources.Contains("NumericCell"))
        {
            added.Add(new ResourceDictionary { Source = new Uri($"pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/{theme}.xaml") });
            added.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }
        }

        try
        {
            return body();
        }
        finally
        {
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Remove(dictionary);
            }
        }
    }
}
