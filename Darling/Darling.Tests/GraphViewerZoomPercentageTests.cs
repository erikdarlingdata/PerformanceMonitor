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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8491: the deadlock graph and the blocking chain show their zoom percentage as the nearest whole number, as the plan
/// viewer does (#8449). A scale of 0.2975 reads "30%", an exact midpoint such as 0.125 reads "13%", and three Zoom Out
/// steps from 100% land on 0.5499..., which reads "55%" and not the truncated "54%". Both controls are shared by Lite and
/// the Viewer, and each is rendered on the STA thread with the Viewer's theme in its own resources.
/// </summary>
public sealed class GraphViewerZoomPercentageTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    public static IEnumerable<object[]> Scales()
    {
        yield return new object[] { 0.29, "29%" };
        yield return new object[] { 0.2975, "30%" };
        yield return new object[] { 0.125, "13%" };
        yield return new object[] { 0.625, "63%" };
        yield return new object[] { 1.0, "100%" };
        yield return new object[] { 2.625, "263%" };
    }

    [Theory]
    [MemberData(nameof(Scales))]
    public void DeadlockGraph_ZoomPercentage_IsTheNearestWholeNumber(double zoom, string text)
    {
        StaTestThread.Run(() =>
        {
            var control = WithTheme(new DeadlockGraphControl());
            try
            {
                SetZoom(control, zoom);
                Assert.Equal(text, ZoomText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Theory]
    [MemberData(nameof(Scales))]
    public void BlockingChain_ZoomPercentage_IsTheNearestWholeNumber(double zoom, string text)
    {
        StaTestThread.Run(() =>
        {
            var control = WithTheme(new BlockingChainControl());
            try
            {
                SetZoom(control, zoom);
                Assert.Equal(text, ZoomText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void DeadlockGraph_ThreeZoomOutSteps_ReadFiftyFivePercent()
    {
        StaTestThread.Run(() =>
        {
            var control = WithTheme(new DeadlockGraphControl());
            try
            {
                LayOut(control);
                var zoomOut = ButtonWithToolTip(control, "Zoom Out");
                Click(zoomOut);
                Click(zoomOut);
                Click(zoomOut);

                Assert.Equal("55%", ZoomText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void BlockingChain_ThreeZoomOutSteps_ReadFiftyFivePercent()
    {
        StaTestThread.Run(() =>
        {
            var control = WithTheme(new BlockingChainControl());
            try
            {
                LayOut(control);
                var zoomOut = ButtonWithToolTip(control, "Zoom Out");
                Click(zoomOut);
                Click(zoomOut);
                Click(zoomOut);

                Assert.Equal("55%", ZoomText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    /// <summary>
    /// Each control's SetZoom is the one place its zoom percentage is written, and it is private; the reflection anchor
    /// fails with a message rather than a null reference when the method moves.
    /// </summary>
    private static void SetZoom(UserControl control, double zoom)
    {
        var setZoom = control.GetType().GetMethod("SetZoom", Instance);
        Assert.True(setZoom is not null, control.GetType().Name + ".SetZoom no longer exists under that name - this test's reflection anchor moved.");
        setZoom!.Invoke(control, new object[] { zoom });
    }

    private static string ZoomText(UserControl control) => ((TextBlock)control.FindName("ZoomLevelText")).Text;

    /// <summary>The Viewer's theme in the control's own resources, never at application scope, so the brushes resolve.</summary>
    private static T WithTheme<T>(T control) where T : UserControl
    {
        control.Resources.MergedDictionaries.Add(new ResourceDictionary
        {
            Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
        });
        return control;
    }

    /// <summary>A fixed size well inside the CI runner's screen; the toolbar's buttons exist in the visual tree after this.</summary>
    private static void LayOut(UIElement control)
    {
        var size = new Size(900, 600);
        control.Measure(size);
        control.Arrange(new Rect(size));
        control.UpdateLayout();
    }

    private static Button ButtonWithToolTip(DependencyObject root, string toolTip) =>
        Descendants(root).OfType<Button>().Single(b => b.ToolTip as string == toolTip);

    private static void Click(Button button) => button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var grandchild in Descendants(child))
                yield return grandchild;
        }
    }
}
