/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="MinimapLayout"/>, the pure geometry behind the plan viewer's minimap: the
/// scale from canvas/plan sizes, node and subtree rectangles in minimap space, the viewport
/// rectangle from scroll/zoom/viewport size, the click-to-scroll-offset conversion, and the
/// 8-color branch cycle. The class is new on this branch, so the RED here is a compile failure
/// on the base branch (no <c>MinimapLayout</c> type exists there) — the runtime mutation pin
/// below is the behavioral RED.
/// </summary>
public class Viewer4580Tests
{
    private static PlanNode MakeNode(double x, double y, bool hasActualStats = false, long actualRows = 0, double estimateRows = 0)
    {
        return new PlanNode
        {
            X = x,
            Y = y,
            HasActualStats = hasActualStats,
            ActualRows = actualRows,
            EstimateRows = estimateRows
        };
    }

    [Fact]
    public void GetScale_UsesTheBindingDimension()
    {
        // Canvas 220x220, plan 1000x400: width-bound scale is 0.22, height-bound is 0.55.
        // The smaller wins so the whole plan fits without stretching either axis.
        var scale = MinimapLayout.GetScale(220, 220, 1000, 400);
        Assert.Equal(0.22, scale, 3);
    }

    [Fact]
    public void GetScale_ReturnsOne_WhenPlanHasNoSize()
    {
        Assert.Equal(1, MinimapLayout.GetScale(220, 220, 0, 0));
    }

    [Fact]
    public void GetScale_ReturnsOne_WhenCanvasHasNoSize()
    {
        Assert.Equal(1, MinimapLayout.GetScale(0, 0, 1000, 400));
    }

    [Fact]
    public void GetNodeRect_ScalesPositionAndSize_WithFourPixelFloor()
    {
        var node = MakeNode(100, 50);
        // At scale 0.01 the raw size (150*0.01=1.5, NodeHeightMin*0.01=0.9) is floored to 4.
        var (x, y, w, h) = MinimapLayout.GetNodeRect(node, 0.01);
        Assert.Equal(1.0, x, 3);
        Assert.Equal(0.5, y, 3);
        Assert.Equal(4, w);
        Assert.Equal(4, h);
    }

    [Fact]
    public void GetNodeRect_AtFullScale_MatchesPlanLayoutEngineDimensions()
    {
        var node = MakeNode(100, 50);
        var (_, _, w, h) = MinimapLayout.GetNodeRect(node, 1.0);
        Assert.Equal(PlanLayoutEngine.NodeWidth, w);
        Assert.Equal(PlanLayoutEngine.GetNodeHeight(node), h);
    }

    [Fact]
    public void CollectSubtreeBounds_CoversNodeAndAllDescendants()
    {
        var leaf1 = MakeNode(200, 0);
        var leaf2 = MakeNode(200, 300);
        var mid = MakeNode(100, 0);
        mid.Children.Add(leaf1);
        mid.Children.Add(leaf2);

        double minX = double.MaxValue, minY = double.MaxValue, maxX = double.MinValue, maxY = double.MinValue;
        MinimapLayout.CollectSubtreeBounds(mid, ref minX, ref minY, ref maxX, ref maxY);

        Assert.Equal(100, minX);
        Assert.Equal(0, minY);
        Assert.Equal(200, maxX);
        // maxY is leaf2's bottom edge (Y + its rendered height), not just its Y.
        Assert.Equal(300 + PlanLayoutEngine.GetNodeHeight(leaf2), maxY);
    }

    [Fact]
    public void GetSubtreeRect_AddsFourPixelMargin_AtFullScale()
    {
        var leaf = MakeNode(200, 100);
        var child = MakeNode(100, 100);
        child.Children.Add(leaf);

        var (x, y, w, h) = MinimapLayout.GetSubtreeRect(child, 1.0);

        // minX=100,minY=100 (child itself is the min since leaf shares Y); width spans
        // (maxX-minX+NodeWidth) + 4px margin.
        Assert.Equal(100 - 2, x);
        Assert.Equal(100 - 2, y);
        Assert.Equal((200 - 100 + PlanLayoutEngine.NodeWidth) + 4, w, 3);
    }

    [Fact]
    public void BranchColorFor_CyclesThroughEightColors()
    {
        var first = MinimapLayout.BranchColorFor(0);
        var eighth = MinimapLayout.BranchColorFor(7);
        var wrapped = MinimapLayout.BranchColorFor(8);

        Assert.Equal(first, wrapped);
        Assert.NotEqual(first, eighth);
        Assert.Equal(8, MinimapLayout.BranchColors.Length);
        // Fixed alpha of 0x30 across the whole cycle (translucent, not opaque).
        Assert.Equal((byte)0x30, first.A);
        Assert.Equal((byte)0x30, eighth.A);
    }

    [Fact]
    public void GetEdgeThickness_IsLogarithmicAndScaled()
    {
        // Full-size thickness for 1000 rows is floor(log(1000))=6 (natural log ~6.9 -> floor 6),
        // clamped to [2,12]; at scale 0.5 that becomes 3.
        var thickness = MinimapLayout.GetEdgeThickness(1000, 0.5);
        Assert.Equal(3.0, thickness, 3);
    }

    [Fact]
    public void GetEdgeThickness_NeverGoesBelowHalfAPixel()
    {
        var thickness = MinimapLayout.GetEdgeThickness(1, 0.01);
        Assert.Equal(0.5, thickness);
    }

    [Fact]
    public void GetViewportRect_ScalesWithZoomAndClampsToWholePlan()
    {
        // Viewport (800x600) is bigger than the zoomed plan (1000*0.5=500 wide), so the box
        // clamps to the whole plan width (Min(viewW/contentW,1.0) == 1.0).
        var (x, y, w, h) = MinimapLayout.GetViewportRect(
            scale: 0.2, zoomLevel: 0.5, planWidth: 1000, planHeight: 400,
            viewportWidth: 800, viewportHeight: 600, scrollOffsetX: 0, scrollOffsetY: 0);

        Assert.Equal(0, x);
        Assert.Equal(0, y);
        Assert.Equal(1000 * 0.2, w, 3);
        Assert.Equal(400 * 0.2, h, 3);
    }

    [Fact]
    public void GetViewportRect_PositionTracksScrollOffsetDividedByZoom()
    {
        var (x, y, _, _) = MinimapLayout.GetViewportRect(
            scale: 0.2, zoomLevel: 2.0, planWidth: 1000, planHeight: 400,
            viewportWidth: 200, viewportHeight: 100, scrollOffsetX: 400, scrollOffsetY: 200);

        // (scrollOffset / zoom) * scale
        Assert.Equal((400 / 2.0) * 0.2, x, 3);
        Assert.Equal((200 / 2.0) * 0.2, y, 3);
    }

    [Fact]
    public void GetViewportRect_ReturnsZero_WhenViewportHasNoSize()
    {
        var (x, y, w, h) = MinimapLayout.GetViewportRect(0.2, 1.0, 1000, 400, 0, 0, 0, 0);
        Assert.Equal(0, x);
        Assert.Equal(0, y);
        Assert.Equal(0, w);
        Assert.Equal(0, h);
    }

    [Fact]
    public void ClickToScrollOffset_ConvertsMinimapPointToCenteredPlanOffset()
    {
        // Click at minimap (44, 22) with scale 0.2 -> plan content point (220, 110).
        // At zoom 1.0 with an 800x600 viewport, centered offset is (220 - 400, 110 - 300),
        // both clamped to 0.
        var (offsetX, offsetY) = MinimapLayout.ClickToScrollOffset(44, 22, 0.2, 1.0, 800, 600);
        Assert.Equal(0, offsetX);
        Assert.Equal(0, offsetY);
    }

    [Fact]
    public void ClickToScrollOffset_CentersWithoutClamping_WhenFarFromTheEdge()
    {
        var (offsetX, offsetY) = MinimapLayout.ClickToScrollOffset(200, 100, 0.2, 1.0, 200, 100);
        // contentX=1000, contentY=500; offset = content*zoom - viewport/2
        Assert.Equal(1000 - 100, offsetX, 3);
        Assert.Equal(500 - 50, offsetY, 3);
    }

    [Fact]
    public void ClickToScrollOffset_ReturnsZero_WhenScaleIsZeroOrLess()
    {
        var (offsetX, offsetY) = MinimapLayout.ClickToScrollOffset(50, 50, 0, 1.0, 800, 600);
        Assert.Equal(0, offsetX);
        Assert.Equal(0, offsetY);
    }

    // -- Part 2: resize clamp, double-click zoom-and-center, minimap node hit-testing --
    // These members are new on this branch; the RED for this section is a compile failure on the
    // pre-fix branch (b3317c250, part 1), same as the class overall.

    [Fact]
    public void ClampPanelSize_ClampsToPerformanceStudiosBounds()
    {
        Assert.Equal(MinimapLayout.MinPanelSize, MinimapLayout.ClampPanelSize(50));
        Assert.Equal(MinimapLayout.MaxPanelSize, MinimapLayout.ClampPanelSize(9999));
        Assert.Equal(300, MinimapLayout.ClampPanelSize(300));
    }

    [Fact]
    public void ResizeFromDrag_SubtractsDeltaFromStartSize_ClampedToBounds()
    {
        // Dragging toward the top-left (negative delta) grows the panel pinned bottom-right.
        var (width, height) = MinimapLayout.ResizeFromDrag(220, 220, -30, -10);
        Assert.Equal(250, width);
        Assert.Equal(230, height);
    }

    [Fact]
    public void ResizeFromDrag_ClampsBelowMinAndAboveMax()
    {
        var (width, height) = MinimapLayout.ResizeFromDrag(170, 170, 500, 500);
        Assert.Equal(MinimapLayout.MinPanelSize, width);
        Assert.Equal(MinimapLayout.MinPanelSize, height);

        var (width2, height2) = MinimapLayout.ResizeFromDrag(480, 480, -500, -500);
        Assert.Equal(MinimapLayout.MaxPanelSize, width2);
        Assert.Equal(MinimapLayout.MaxPanelSize, height2);
    }

    [Fact]
    public void GetZoomToNodeLevel_FitsNodeToAboutAThirdOfTheViewport()
    {
        // Node 200x60, viewport 900x600: width-bound fitZoom = 900/600 = 1.5, height-bound = 600/180 = 3.33.
        // The smaller wins, clamped to [0.1, 3.0].
        var zoom = MinimapLayout.GetZoomToNodeLevel(200, 60, 900, 600, 0.1, 3.0);
        Assert.Equal(1.5, zoom, 3);
    }

    [Fact]
    public void GetZoomToNodeLevel_ClampsToMaxZoom()
    {
        var zoom = MinimapLayout.GetZoomToNodeLevel(10, 10, 900, 600, 0.1, 3.0);
        Assert.Equal(3.0, zoom, 3);
    }

    [Fact]
    public void GetZoomToNodeLevel_ReturnsMinZoom_WhenViewportOrNodeHasNoSize()
    {
        Assert.Equal(0.1, MinimapLayout.GetZoomToNodeLevel(0, 60, 900, 600, 0.1, 3.0));
        Assert.Equal(0.1, MinimapLayout.GetZoomToNodeLevel(200, 60, 0, 600, 0.1, 3.0));
    }

    [Fact]
    public void GetNodeCenterOffset_CentersNodeInViewport_ClampedToZero()
    {
        // Node at (100, 50), 20x10, zoom 1.0, viewport 400x300: center=(110,55), offset=(110-200,55-150) -> clamped to 0.
        var (offsetX, offsetY) = MinimapLayout.GetNodeCenterOffset(100, 50, 20, 10, 1.0, 400, 300);
        Assert.Equal(0, offsetX);
        Assert.Equal(0, offsetY);
    }

    [Fact]
    public void GetNodeCenterOffset_CentersWithoutClamping_WhenFarFromTheEdge()
    {
        var (offsetX, offsetY) = MinimapLayout.GetNodeCenterOffset(1000, 500, 20, 10, 1.0, 200, 100);
        Assert.Equal(1000 + 10 - 100, offsetX, 3);
        Assert.Equal(500 + 5 - 50, offsetY, 3);
    }

    [Fact]
    public void FindNodeAt_ReturnsTheNodeWhoseRectangleContainsThePoint()
    {
        // Rows far enough apart (500) that nodes' rectangles never overlap, so the match is unambiguous.
        var root = MakeNode(0, 0);
        var child = MakeNode(0, 500);
        var grandchild = MakeNode(0, 1000);
        child.Children.Add(grandchild);
        root.Children.Add(child);

        // At scale 1, grandchild's rect is Y=1000..1000+height, X=0..NodeWidth. Pick a point inside it.
        var found = MinimapLayout.FindNodeAt(root, 10, 1002, 1.0);
        Assert.Same(grandchild, found);
    }

    [Fact]
    public void FindNodeAt_ReturnsNull_WhenNoNodeContainsThePoint()
    {
        var root = MakeNode(0, 0);
        var found = MinimapLayout.FindNodeAt(root, 9999, 9999, 1.0);
        Assert.Null(found);
    }
}
