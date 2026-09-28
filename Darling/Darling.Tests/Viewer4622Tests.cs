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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the #4622 fix for the minimap staying empty on a plan tab's first open, in the shared plan
/// viewer's minimap (<c>PerformanceMonitor.Ui</c>, used by both Lite and the Darling viewer). The root
/// cause is a WPF layout-timing race -- <c>RenderMinimap</c> reads <c>MinimapCanvas.ActualWidth</c>
/// before the panel's first Collapsed-to-Visible layout pass has run -- which this test project cannot
/// reproduce without a real, sized WPF window. So this is a source scan of
/// <c>PlanViewerControl.Minimap.cs</c> for the deferral the fix depends on, the same technique
/// <see cref="PlanViewerCapabilityPinTests"/> uses for paste-path ordering: it fails on the pre-fix
/// code, which has neither <c>DispatcherPriority.Loaded</c> nor <c>Dispatcher.BeginInvoke(</c>
/// anywhere in the file.
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
}
