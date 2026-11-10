/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8534: where the deadlock graph sits in its viewer. A graph smaller than the viewer is drawn in the middle of it, with the same room left
/// and right and the same room above and below, at 100% and after a zoom out. On an axis where the graph is larger than the viewer it starts
/// at the viewer's edge and scrolls, so no part of it lies where scrolling cannot reach. The control is the one Lite and the Darling Viewer
/// both show in their graph window; it is laid out here at a stated size on the shared STA thread and the canvas's place read back.
/// </summary>
public sealed class DeadlockGraphPlacementTests
{
    private static DeadlockGraphModel TwoSessionCycle() => new()
    {
        Processes = new[]
        {
            new DeadlockProcessNode { Id = "process1", Spid = 61, IsVictim = true, LockMode = "X" },
            new DeadlockProcessNode { Id = "process2", Spid = 62, LockMode = "X" },
        },
        Edges = new[]
        {
            new DeadlockWaitEdge { WaiterProcessId = "process1", OwnerProcessId = "process2", ResourceKind = "KEY", ResourceLabel = "KEY walk.dbo.walk_rows", RequestMode = "X" },
            new DeadlockWaitEdge { WaiterProcessId = "process2", OwnerProcessId = "process1", ResourceKind = "KEY", ResourceLabel = "KEY walk.dbo.walk_rows", RequestMode = "X" },
        },
    };

    /// <summary>The graph's drawn bounds inside its scroll viewer, the viewport they sit in, and how far the viewer scrolls.</summary>
    private sealed record Placed(Rect Graph, Size Viewport, double ScrollableWidth, double ScrollableHeight)
    {
        internal double Left => Graph.Left;
        internal double Right => Viewport.Width - Graph.Right;
        internal double Above => Graph.Top;
        internal double Below => Viewport.Height - Graph.Bottom;

        public override string ToString() =>
            $"a {Graph.Width:0}x{Graph.Height:0} graph in a {Viewport.Width:0}x{Viewport.Height:0} viewport: {Left:0.#} px left of it, {Right:0.#} right, {Above:0.#} above, {Below:0.#} below";
    }

    /// <summary>
    /// The two-session deadlock in a viewer <paramref name="width"/> by <paramref name="height"/>, its properties panel closed so the whole
    /// width is the graph's, after <paramref name="zoomOuts"/> clicks on Zoom Out.
    /// </summary>
    private static Placed Place(double width, double height, int zoomOuts = 0) => StaTestThread.Run(() =>
    {
        var control = new DeadlockGraphControl { Width = width, Height = height };
        void Lay()
        {
            control.Measure(new Size(width, height));
            control.Arrange(new Rect(0, 0, width, height));
            control.UpdateLayout();
        }

        Lay();
        control.LoadModel(TwoSessionCycle());
        Lay();
        Click(control, "Close Properties");
        for (var i = 0; i < zoomOuts; i++)
        {
            Click(control, "Zoom Out");
        }

        Lay();
        var scroll = control.GraphScrollViewer;
        var canvas = control.GraphCanvas;
        var graph = canvas.TransformToAncestor(scroll).TransformBounds(new Rect(0, 0, canvas.ActualWidth, canvas.ActualHeight));
        return new Placed(graph, new Size(scroll.ViewportWidth, scroll.ViewportHeight), scroll.ScrollableWidth, scroll.ScrollableHeight);
    });

    private static void Click(DeadlockGraphControl control, string toolTip)
    {
        var button = ClippedTextProbe.Descendants(control).OfType<Button>().Single(b => Equals(b.ToolTip, toolTip));
        button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
    }

    private static void AssertInTheMiddle(Placed placed)
    {
        Assert.True(placed.Viewport.Width - placed.Graph.Width > 40 && placed.Viewport.Height - placed.Graph.Height > 40,
            $"the viewer should have room around the graph for this test to mean anything: {placed}");
        Assert.True(Math.Abs(placed.Left - placed.Right) <= 1, $"the graph is not in the middle, left to right: {placed}");
        Assert.True(Math.Abs(placed.Above - placed.Below) <= 1, $"the graph is not in the middle, top to bottom: {placed}");
    }

    [Theory]
    [InlineData(1000, 700)]
    [InlineData(1200, 800)]
    public void AGraphSmallerThanTheViewer_IsDrawnInTheMiddleOfIt(double width, double height) =>
        AssertInTheMiddle(Place(width, height));

    [Fact]
    public void AGraphTallerThanTheViewer_IsInTheMiddleLeftToRight_AndStartsAtTheTop()
    {
        var placed = Place(900, 600);

        Assert.True(placed.Graph.Height > placed.Viewport.Height && placed.Viewport.Width - placed.Graph.Width > 40,
            $"the graph should be taller and narrower than the viewer for this test to mean anything: {placed}");
        Assert.True(Math.Abs(placed.Left - placed.Right) <= 1, $"the graph is not in the middle, left to right: {placed}");
        Assert.True(Math.Abs(placed.Above) <= 0.5, $"a graph taller than the viewer starts at its top edge: {placed}");
        Assert.True(placed.ScrollableHeight > 0, $"the viewer scrolls down to the rest of the graph: {placed.ScrollableHeight:0.#} px");
    }

    [Fact]
    public void AGraphSmallerThanTheViewer_StaysInTheMiddle_AfterAZoomOut()
    {
        var full = Place(900, 600);
        var smaller = Place(900, 600, zoomOuts: 2);

        Assert.True(smaller.Graph.Width < full.Graph.Width - 1, $"two Zoom Out clicks should draw the graph smaller: {full.Graph.Width:0} px, then {smaller.Graph.Width:0} px");
        AssertInTheMiddle(smaller);
    }

    [Fact]
    public void AGraphLargerThanTheViewer_StartsAtItsTopLeftCorner_AndScrolls()
    {
        var placed = Place(260, 220);

        Assert.True(placed.Graph.Width > placed.Viewport.Width && placed.Graph.Height > placed.Viewport.Height, $"the graph should be larger than the viewer for this test to mean anything: {placed}");
        Assert.True(Math.Abs(placed.Left) <= 0.5 && Math.Abs(placed.Above) <= 0.5, $"a graph larger than the viewer starts at its top-left corner: {placed}");
        Assert.True(placed.ScrollableWidth > 0 && placed.ScrollableHeight > 0, $"the viewer scrolls to the rest of the graph: {placed.ScrollableWidth:0.#} px across, {placed.ScrollableHeight:0.#} px down");
    }
}
