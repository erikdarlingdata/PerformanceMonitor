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
using System.Windows.Controls.Primitives;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8534: where the blocking chain sits in its viewer, as <see cref="DeadlockGraphPlacementTests"/> reads it for the deadlock graph. On an
/// axis where the chain is smaller than the viewer it has the same room on both sides, at 100% and after a zoom out; on an axis where it is
/// larger it starts at the viewer's edge and scrolls. The control is the one Lite and the Darling Viewer both show in their graph window; it
/// is laid out here at a stated size on the shared STA thread, with Lite's theme in its own resources, and the canvas's place read back.
/// </summary>
public sealed class BlockingChainPlacementTests
{
    private static readonly DateTime Started = new(2026, 11, 10, 9, 0, 0);

    /// <summary>One lead blocker (61) and the one session it blocks (62).</summary>
    private static BlockingChainModel TwoSessionChain() => BlockingChainTreeBuilder.Build(
        new[]
        {
            new BlockingChainInput
            {
                ApexSpid = 61,
                ApexTranStarted = Started,
                ApexSleeping = false,
                Magnitude = 1.0,
                Edges = new List<BlockingEdgeInput>
                {
                    new()
                    {
                        Level = 1,
                        BlockingSpid = 61,
                        BlockingEcid = 0,
                        BlockingTranStarted = Started,
                        BlockedSpid = 62,
                        BlockedEcid = 0,
                        BlockedTranStarted = Started.AddSeconds(5),
                        WaitTimeMs = 8000,
                        LockMode = "X",
                        DatabaseName = "walk",
                        BlockingSqlText = "UPDATE dbo.walk_rows SET n = n + 1 WHERE id = 1",
                        BlockedSqlText = "UPDATE dbo.walk_rows SET n = n + 1 WHERE id = 1",
                        BlockedLoginName = "login62",
                        BlockedHostName = "host62",
                        BlockedClientApp = "app62",
                        BlockingLoginName = "login61",
                        BlockingHostName = "host61",
                        BlockingClientApp = "app61",
                    },
                },
            },
        },
        false, false, false);

    /// <summary>The chain's drawn bounds inside its scroll viewer, the viewport they sit in, and how far the viewer scrolls.</summary>
    private sealed record Placed(Rect Chain, Size Viewport, double ScrollableWidth, double ScrollableHeight)
    {
        internal double Left => Chain.Left;
        internal double Right => Viewport.Width - Chain.Right;
        internal double Above => Chain.Top;
        internal double Below => Viewport.Height - Chain.Bottom;

        public override string ToString() =>
            $"a {Chain.Width:0}x{Chain.Height:0} chain in a {Viewport.Width:0}x{Viewport.Height:0} viewport: {Left:0.#} px left of it, {Right:0.#} right, {Above:0.#} above, {Below:0.#} below";
    }

    /// <summary>
    /// The two-session chain in a viewer <paramref name="width"/> by <paramref name="height"/>, its properties panel closed so the whole
    /// width is the chain's, after <paramref name="zoomOuts"/> clicks on Zoom Out.
    /// </summary>
    private static Placed Place(double width, double height, int zoomOuts = 0) => StaTestThread.Run(() =>
    {
        var control = new BlockingChainControl { Width = width, Height = height };
        control.Resources.MergedDictionaries.Add(new ResourceDictionary
        {
            Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml"),
        });
        void Lay()
        {
            control.Measure(new Size(width, height));
            control.Arrange(new Rect(0, 0, width, height));
            control.UpdateLayout();
        }

        try
        {
            Lay();
            control.LoadModel(TwoSessionChain(), 62, 0);
            Lay();
            Click(control, "Close Properties");
            for (var i = 0; i < zoomOuts; i++)
            {
                Click(control, "Zoom Out");
            }

            Lay();
            var scroll = control.ChainScrollViewer;
            var canvas = control.ChainCanvas;
            var chain = canvas.TransformToAncestor(scroll).TransformBounds(new Rect(0, 0, canvas.ActualWidth, canvas.ActualHeight));
            return new Placed(chain, new Size(scroll.ViewportWidth, scroll.ViewportHeight), scroll.ScrollableWidth, scroll.ScrollableHeight);
        }
        finally
        {
            control.Cleanup();
        }
    });

    private static void Click(BlockingChainControl control, string toolTip)
    {
        var button = ClippedTextProbe.Descendants(control).OfType<Button>().Single(b => Equals(b.ToolTip, toolTip));
        button.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
    }

    /// <summary>On each axis on its own: a chain smaller than the viewer has the same room on both sides, and a larger one starts at the viewer's edge.</summary>
    private static void AssertPlacedOnEachAxis(Placed placed)
    {
        if (placed.Chain.Width <= placed.Viewport.Width)
        {
            Assert.True(Math.Abs(placed.Left - placed.Right) <= 1, $"the chain is not in the middle, left to right: {placed}");
        }
        else
        {
            Assert.True(Math.Abs(placed.Left) <= 0.5, $"a chain wider than the viewer starts at its left edge: {placed}");
        }

        if (placed.Chain.Height <= placed.Viewport.Height)
        {
            Assert.True(Math.Abs(placed.Above - placed.Below) <= 1, $"the chain is not in the middle, top to bottom: {placed}");
        }
        else
        {
            Assert.True(Math.Abs(placed.Above) <= 0.5, $"a chain taller than the viewer starts at its top edge: {placed}");
        }
    }

    [Theory]
    [InlineData(1000, 700)]
    [InlineData(1200, 800)]
    public void AChainNarrowerThanTheViewer_IsDrawnInTheMiddleOfIt(double width, double height)
    {
        var placed = Place(width, height);

        Assert.True(placed.Viewport.Width - placed.Chain.Width > 40, $"the viewer should have room beside the chain for this test to mean anything: {placed}");
        AssertPlacedOnEachAxis(placed);
    }

    [Fact]
    public void AChainNarrowerThanTheViewer_StaysInTheMiddle_AfterAZoomOut()
    {
        var full = Place(1000, 700);
        var smaller = Place(1000, 700, zoomOuts: 2);

        Assert.True(smaller.Chain.Width < full.Chain.Width - 1, $"two Zoom Out clicks should draw the chain smaller: {full.Chain.Width:0} px, then {smaller.Chain.Width:0} px");
        Assert.True(smaller.Viewport.Width - smaller.Chain.Width > 40, $"the viewer should have room beside the chain for this test to mean anything: {smaller}");
        AssertPlacedOnEachAxis(smaller);
    }

    [Fact]
    public void AChainLargerThanTheViewer_StartsAtItsTopLeftCorner_AndScrolls()
    {
        var placed = Place(300, 220);

        Assert.True(placed.Chain.Width > placed.Viewport.Width && placed.Chain.Height > placed.Viewport.Height, $"the chain should be larger than the viewer for this test to mean anything: {placed}");
        AssertPlacedOnEachAxis(placed);
        Assert.True(placed.ScrollableWidth > 0 && placed.ScrollableHeight > 0, $"the viewer scrolls to the rest of the chain: {placed.ScrollableWidth:0.#} px across, {placed.ScrollableHeight:0.#} px down");
    }
}
