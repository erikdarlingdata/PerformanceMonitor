/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the two #4622 fixes in the shared plan viewer's minimap (<c>PerformanceMonitor.Ui</c>, used by
/// both Lite and the Darling viewer): the minimap staying empty on a plan tab's first open, and a
/// double-click zoom leaving the node off-center once the properties panel narrows the viewport. Both
/// root causes are WPF layout-timing races -- reading <c>ActualWidth</c>/<c>ViewportWidth</c> before a
/// pending layout pass has actually run -- which this test project cannot reproduce without a real,
/// sized WPF window. So these pins split in two: a source scan of
/// <c>PlanViewerControl.Minimap.cs</c> for the ordering/deferral the fix depends on (fails on the old
/// code, the same technique <see cref="PlanViewerCapabilityPinTests"/> uses for paste-path ordering),
/// plus a pure-math reproduction of the issue's own measured pixel numbers against
/// <see cref="MinimapLayout.GetNodeCenterOffset"/>, which needs no WPF at all.
/// </summary>
public class Viewer4622Tests
{
    private static string MinimapCs([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "..", "PerformanceMonitor.Ui", "PlanViewerControl.Minimap.cs"));

    private static string Source()
    {
        var path = MinimapCs();
        Assert.True(File.Exists(path), $"PlanViewerControl.Minimap.cs not found at {path} -- the scan is broken, fix the path.");
        return File.ReadAllText(path);
    }

    // ---- Item 1: minimap empty on first open ----

    [Fact]
    public void RenderMinimap_DefersOnZeroSizeCanvas_InsteadOfGivingUp()
    {
        var source = Source();
        const string open = "if (canvasW <= 0 || canvasH <= 0)";
        const string close = "var scale = MinimapLayout.GetScale(canvasW, canvasH, PlanCanvas.Width, PlanCanvas.Height);";

        var start = source.IndexOf(open, StringComparison.Ordinal);
        Assert.True(start >= 0, "RenderMinimap's zero-size canvas check is gone -- the scan is broken or the method was rewritten.");
        var end = source.IndexOf(close, start, StringComparison.Ordinal);
        Assert.True(end > start, "couldn't find the end of RenderMinimap's zero-size branch -- the scan is broken.");

        var block = source.Substring(start, end - start);
        Assert.True(block.Contains("DispatcherPriority.Loaded", StringComparison.Ordinal),
            "RenderMinimap no longer re-posts itself at DispatcherPriority.Loaded when MinimapCanvas has no size yet " +
            "(#4622) -- MinimapPanel starts Collapsed, so the call from OpenMinimapPanel lands before layout has " +
            "measured the canvas, and without a deferred retry nothing else re-renders it: the minimap stays empty " +
            "until the user drags the resize grip or closes/reopens the panel.");
        Assert.True(block.Contains("RenderMinimap()", StringComparison.Ordinal),
            "the deferred callback should call RenderMinimap() again once layout has run, not just swallow the miss.");
    }

    [Fact]
    public void RenderMinimap_CapsTheZeroSizeRetryAtOnce()
    {
        // Without a one-shot guard, a canvas that is STILL unsized when the Loaded-priority retry runs
        // (e.g. the minimap was opened in a hidden tab) would re-post itself forever, starving input.
        var source = Source();
        Assert.True(source.Contains("_minimapRenderDeferred", StringComparison.Ordinal),
            "the one-shot retry guard '_minimapRenderDeferred' is gone (#4622) -- a still-unsized canvas after the " +
            "first retry would re-queue itself forever instead of giving up.");
    }

    // ---- Item 2: double-click zoom off-center ----

    [Fact]
    public void ZoomToMinimapNode_SelectsBeforeDeferringTheCenterOffset()
    {
        var source = Source();
        const string methodStart = "private void ZoomToMinimapNode(PlanNode node, double viewportWidth, double viewportHeight)";
        var methodIdx = source.IndexOf(methodStart, StringComparison.Ordinal);
        Assert.True(methodIdx >= 0, "ZoomToMinimapNode's signature changed or is gone -- the scan is broken.");

        var nextMethodIdx = source.IndexOf("private void PlanScrollViewer_ScrollChanged", methodIdx, StringComparison.Ordinal);
        Assert.True(nextMethodIdx > methodIdx, "couldn't find the end of ZoomToMinimapNode -- the scan is broken.");
        var body = source.Substring(methodIdx, nextMethodIdx - methodIdx);

        // Anchored on "Dispatcher.BeginInvoke(" rather than "DispatcherPriority.Loaded": this method's
        // own explanatory comment says "Deferring to DispatcherPriority.Loaded" in prose, ahead of the
        // real call, so that token alone would match the comment instead of the code.
        var selectIdx = body.IndexOf("SelectNode(b, n)", StringComparison.Ordinal);
        var deferIdx = body.IndexOf("Dispatcher.BeginInvoke(", StringComparison.Ordinal);
        var offsetIdx = body.IndexOf("GetNodeCenterOffset(", StringComparison.Ordinal);

        Assert.True(selectIdx >= 0 && deferIdx >= 0 && offsetIdx >= 0 && body.Contains("DispatcherPriority.Loaded", StringComparison.Ordinal),
            "ZoomToMinimapNode should call SelectNode, defer via Dispatcher.BeginInvoke at DispatcherPriority.Loaded, " +
            "and compute the center offset via MinimapLayout.GetNodeCenterOffset -- one of those is missing.");
        // Textual order: SelectNode, then the BeginInvoke( call that opens the deferred lambda, then the
        // GetNodeCenterOffset( call inside that lambda's body.
        Assert.True(selectIdx < deferIdx && deferIdx < offsetIdx,
            "SelectNode must run BEFORE the center offset is computed, and that computation must sit inside the " +
            "Dispatcher.BeginInvoke(..., DispatcherPriority.Loaded) deferral: SelectNode opens the properties panel " +
            "(when it wasn't already open) and narrows PlanScrollViewer by ~400px, so centering against the " +
            "pre-select viewport put the node 201px right of center once the panel opened (#4622).");
    }

    [Fact]
    public void ZoomToMinimapNode_ReadsViewportWidthFreshInsideTheDeferral()
    {
        var source = Source();
        var methodIdx = source.IndexOf("private void ZoomToMinimapNode(PlanNode node, double viewportWidth, double viewportHeight)", StringComparison.Ordinal);
        Assert.True(methodIdx >= 0, "ZoomToMinimapNode's signature changed or is gone -- the scan is broken.");

        // Anchored on "Dispatcher.BeginInvoke(", not "DispatcherPriority.Loaded" -- see the comment in
        // ZoomToMinimapNode_SelectsBeforeDeferringTheCenterOffset above for why.
        var deferIdx = source.IndexOf("Dispatcher.BeginInvoke(", methodIdx, StringComparison.Ordinal);
        Assert.True(deferIdx >= 0, "the Dispatcher.BeginInvoke deferral in ZoomToMinimapNode is gone -- the scan is broken.");

        var offsetIdx = source.IndexOf("GetNodeCenterOffset(", deferIdx, StringComparison.Ordinal);
        Assert.True(offsetIdx >= 0, "couldn't find the center-offset call inside the deferral -- the scan is broken.");

        var offsetCallEnd = source.IndexOf(");", offsetIdx, StringComparison.Ordinal);
        var offsetCall = source.Substring(offsetIdx, offsetCallEnd - offsetIdx);
        Assert.True(offsetCall.Contains("PlanScrollViewer.ViewportWidth", StringComparison.Ordinal) &&
                     offsetCall.Contains("PlanScrollViewer.ViewportHeight", StringComparison.Ordinal),
            "the deferred center-offset call should re-read PlanScrollViewer.ViewportWidth/ViewportHeight, not the " +
            "stale viewportWidth/viewportHeight the mouse handler captured before SelectNode opened the properties " +
            "panel -- a scrollbar toggling on the zoom change alone shifts ViewportWidth enough to leave a 9px " +
            "residual even when the panel was already open (#4622).");
    }

    // ---- Pure-math reproduction of the issue's measured numbers ----

    [Theory]
    [InlineData(1538, 1135, 201.5)] // properties panel closed before the double-click
    [InlineData(1153, 1135, 9.0)]   // properties panel already open before the double-click
    public void GetNodeCenterOffset_StaleViewportWidth_MisplacesTheNodeByHalfTheShrink(
        double capturedBeforeSelectWidth, double actualViewportWidthAfterSelect, double expectedResidualPixels)
    {
        // Reproduces the issue's own measurement: an offset computed against the viewport width from
        // BEFORE SelectNode opens/narrows the properties panel lands the node off-center, by half of
        // however much the viewport actually shrinks by the time the offset is applied -- 201px when
        // the panel was closed (1,538 -> 1,135), 9px when it was already open (1,153 -> 1,135, an 18px
        // shift from a scrollbar toggling on the zoom change alone). Reading the correct, already-
        // narrowed width -- the fix -- lands the node within a pixel of true center instead.
        const double zoom = 1.0;
        const double nodeWidth = 240;
        const double nodeX = 5000; // far from the left edge, so neither offset clamps to 0

        var nodeContentCenterX = nodeX + nodeWidth / 2;

        var (correctOffsetX, _) = MinimapLayout.GetNodeCenterOffset(nodeX, 0, nodeWidth, 0, zoom, actualViewportWidthAfterSelect, 0);
        var (staleOffsetX, _) = MinimapLayout.GetNodeCenterOffset(nodeX, 0, nodeWidth, 0, zoom, capturedBeforeSelectWidth, 0);

        var correctResidual = nodeContentCenterX - (correctOffsetX + actualViewportWidthAfterSelect / 2);
        var staleResidual = nodeContentCenterX - (staleOffsetX + actualViewportWidthAfterSelect / 2);

        Assert.True(Math.Abs(correctResidual) < 1, $"the correct-width offset should center within 1px, was off by {correctResidual}px");
        Assert.Equal(expectedResidualPixels, staleResidual, 1);
    }
}
