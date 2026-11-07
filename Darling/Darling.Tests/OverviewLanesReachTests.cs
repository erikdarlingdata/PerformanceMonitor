/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Viewer;
using ScottPlot.WPF;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8104: every Overview lane can be reached at every height the Viewer gives the control. The five lanes each keep a row minimum,
/// so when the control is shorter than the rows' total the scroll viewer's extent must hold every lane and offer the rest by
/// scrolling, and when it is taller the lanes share the room with nothing to scroll. The control is laid out bare, as the server
/// tab's Overview hosts it, at the heights that tab leaves it.
/// </summary>
public sealed class OverviewLanesReachTests
{
    /// <summary>
    /// The heights the server tab leaves the control: what a maximised 1920x1080 window gives it (about 880 px of viewport inside
    /// the control's 8 px margin), what the Viewer's smallest window (MinHeight 600) gives it, and a tall window where all five
    /// lanes fit.
    /// </summary>
    public static TheoryData<double> ControlHeights => new() { 896, 560, 1200 };

    [Theory]
    [MemberData(nameof(ControlHeights))]
    public void EveryLane_LiesInsideTheScrollViewersExtent_AndItScrollsWhenTheLanesDoNotFit(double controlHeight) =>
        StaTestThread.Run(() =>
        {
            var control = new CorrelatedTimelineLanesControl { Height = controlHeight };
            var window = new Window { Content = new Grid { Children = { control } }, Width = 1900, Height = 1300, Left = 0, Top = 0 };
            window.Show();
            try
            {
                ChartLayoutProbe.Layout(window);
                var scroller = ChartLayoutProbe.Logical<ScrollViewer>(control).Single();
                var grid = Assert.IsType<Grid>(scroller.Content);
                var lanes = ChartLayoutProbe.Logical<WpfPlot>(control).ToList();
                Assert.Equal(grid.RowDefinitions.Count, lanes.Count);

                foreach (var lane in lanes)
                {
                    var bottom = lane.TransformToAncestor(grid).Transform(new Point(0, lane.ActualHeight)).Y;
                    Assert.True(
                        bottom <= scroller.ExtentHeight + 0.5,
                        $"{ChartLayoutProbe.NameOf(lane)} ends {bottom:0.#} px into the lanes but the scroll viewer's extent is {scroller.ExtentHeight:0.#} px in a {controlHeight} px control: the lane is clipped and cannot be scrolled to");
                }

                var rowsNeed = grid.RowDefinitions.Sum(r => r.MinHeight);
                if (scroller.ViewportHeight < rowsNeed)
                {
                    Assert.True(
                        scroller.ScrollableHeight > 0,
                        $"the rows need {rowsNeed} px in a {scroller.ViewportHeight:0.#} px viewport, and the scroll viewer offers no scrolling (extent {scroller.ExtentHeight:0.#} px)");
                    Assert.Equal(Visibility.Visible, scroller.ComputedVerticalScrollBarVisibility);
                }
                else
                {
                    Assert.Equal(0, scroller.ScrollableHeight, 1);
                }
            }
            finally
            {
                window.Close();
            }
        });
}
