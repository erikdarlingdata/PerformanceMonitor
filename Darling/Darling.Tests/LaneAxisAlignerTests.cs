/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Ui;
using ScottPlot;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2533: the Overview's five lanes are separate plots, and matching their X-axis LIMITS never made them
/// match in PIXELS - each sized its own left gutter from its own Y tick labels, so a lane reading
/// <c>120,000</c> began its data area ~27 px right of one reading <c>0.4</c>. Correct values, skewed
/// chart; the reporter's tooltips read true throughout, which is what identified it as layout.
///
/// <para>These are real renders, not a model of one. ScottPlot draws to a CPU raster surface, so
/// <c>Plot.RenderInMemory</c> produces a genuine <c>LastRender.DataRect</c> with no window, no dispatcher
/// and no desktop - the whole defect is reachable headlessly, which is the point. This is the second
/// alignment bug in these lanes; the first shipped with nothing that could have caught it.</para>
///
/// <para><b>Shown red before green.</b> Against the pre-fix path (the same lanes rendered with no
/// alignment step) <see cref="TwoLanesWithMismatchedMagnitudes_ShareAnXOriginAndWidth"/> fails at
/// 20.02 px and <see cref="AllFiveOverviewLanes_ShareAnXOrigin"/> at a 26.70 px spread.
/// <see cref="TheFixture_ReallyDoesProvokeTheDefect_WhenNothingAlignsIt"/> keeps that honest: it asserts
/// the un-aligned divergence still happens, so if a future ScottPlot equalised gutters on its own the
/// suite would say the fixture had stopped exercising the bug rather than quietly passing forever.</para>
///
/// <para>Scope is the Darling viewer, which is where this was reported. Lite's Overview control is the
/// same five separate plots with the same axis configuration and carries the same latent defect, but
/// nobody has hit it there, so it is deliberately not changed here - the helper lives in the shared
/// <c>PerformanceMonitor.Ui</c> so picking it up is one call site and a copy of the wiring pin below.</para>
/// </summary>
public sealed class LaneAxisAlignerTests
{
    /* The lane geometry the Overview actually uses: five equal rows, full tab width. */
    private const int LaneWidth = 1100;
    private const int LaneHeight = 200;

    /* Real Overview magnitudes. Wait ms/sec into six digits is what the reporter's server showed;
       I/O latency at 0.4 ms and blocking counts in single digits are the lanes it pushed out of line. */
    private const double CpuMax = 105;
    private const double WaitMsPerSecMax = 120000;
    private const double BlockingMax = 2;
    private const double BufferPoolMbMax = 48000;
    private const double IoLatencyMsMax = 0.4;

    /// <summary>A lane shaped like the control's: a time series, date ticks, explicit X and Y limits.</summary>
    private static Plot Lane(double yMax)
    {
        var plot = new Plot();
        double t0 = new DateTime(2026, 8, 20, 0, 0, 0).ToOADate();
        double step = 1.0 / 24.0 / 60.0;
        double[] times = Enumerable.Range(0, 240).Select(i => t0 + (i * step)).ToArray();
        double[] values = Enumerable.Range(0, 240).Select(i => yMax * (0.5 + (0.5 * Math.Sin(i / 7.0)))).ToArray();

        plot.Add.Scatter(times, values);
        plot.Axes.DateTimeTicksBottom();
        plot.Axes.SetLimitsX(times[0], times[^1]);
        plot.Axes.SetLimitsY(0, yMax * 1.05);
        plot.Legend.IsVisible = false;
        return plot;
    }

    private static Plot[] OverviewStack() =>
        [Lane(CpuMax), Lane(WaitMsPerSecMax), Lane(BlockingMax), Lane(BufferPoolMbMax), Lane(IoLatencyMsMax)];

    /// <summary>Renders every lane at the control's size and returns the data rectangle each one landed on.</summary>
    private static PixelRect[] Render(Plot[] lanes)
    {
        foreach (var lane in lanes)
        {
            lane.RenderInMemory(LaneWidth, LaneHeight);
        }

        return lanes.Select(l => l.RenderManager.LastRender.DataRect).ToArray();
    }

    /// <summary>The gutter a lane of this magnitude asks for on its own, with nothing constraining it.</summary>
    private static float NaturalGutter(double yMax)
    {
        var plot = Lane(yMax);
        plot.RenderInMemory(LaneWidth, LaneHeight);
        return plot.RenderManager.LastRender.Padding.Left;
    }

    /* ---------------- the defect ---------------- */

    /// <summary>
    /// The reported case reduced to two lanes: I/O latency topping out at 0.4 ms against wait time at
    /// 120,000 ms/sec. Same X limits, same control size - the data rectangles must start at the same pixel
    /// and be the same width, or the same instant sits at a different X in the two lanes.
    /// </summary>
    [Fact]
    public void TwoLanesWithMismatchedMagnitudes_ShareAnXOriginAndWidth()
    {
        Plot[] lanes = [Lane(IoLatencyMsMax), Lane(WaitMsPerSecMax)];

        LaneAxisAligner.AlignLeftGutters(lanes, LaneHeight);
        var rects = Render(lanes);

        Assert.Equal(rects[0].Left, rects[1].Left, 2);
        Assert.Equal(rects[0].Width, rects[1].Width, 2);
    }

    /// <summary>All five Overview lanes at once - the surface as the reporter sees it.</summary>
    [Fact]
    public void AllFiveOverviewLanes_ShareAnXOrigin()
    {
        var lanes = OverviewStack();

        LaneAxisAligner.AlignLeftGutters(lanes, LaneHeight);
        var rects = Render(lanes);

        double spread = rects.Max(r => r.Left) - rects.Min(r => r.Left);
        Assert.True(spread < 0.01,
            $"lanes start at [{string.Join(", ", rects.Select(r => r.Left.ToString("F2")))}] - {spread:F2} px apart");

        double widthSpread = rects.Max(r => r.Width) - rects.Min(r => r.Width);
        Assert.True(widthSpread < 0.01, $"lane widths differ by {widthSpread:F2} px");
    }

    /// <summary>
    /// The fixture guard. Without the aligner these magnitudes must still pull the lanes apart - if they
    /// stop doing so (a ScottPlot layout change, a tick-format change), the two tests above would begin
    /// passing for a reason that has nothing to do with the fix, and this is what says so.
    /// </summary>
    [Fact]
    public void TheFixture_ReallyDoesProvokeTheDefect_WhenNothingAlignsIt()
    {
        var rects = Render([Lane(IoLatencyMsMax), Lane(WaitMsPerSecMax)]);

        double delta = Math.Abs(rects[0].Left - rects[1].Left);
        Assert.True(delta > 5,
            $"un-aligned lanes were only {delta:F2} px apart - the fixture no longer reproduces #2533");
    }

    /* ---------------- the width decision ---------------- */

    /// <summary>
    /// Too narrow clips the lane that needed the room; too wide steals it from every server that did not.
    /// The shared gutter must be exactly the widest lane's own natural gutter - not a pixel less (the wide
    /// lane clips) and not a pixel more (every other lane pays for a label nobody is reading).
    /// </summary>
    [Fact]
    public void TheSharedGutter_IsExactlyTheWidestLanesNaturalGutter()
    {
        float widestNatural = NaturalGutter(WaitMsPerSecMax);

        var lanes = OverviewStack();
        float shared = LaneAxisAligner.AlignLeftGutters(lanes, LaneHeight);
        Assert.Equal(widestNatural, shared, 2);

        foreach (var lane in lanes)
        {
            lane.RenderInMemory(LaneWidth, LaneHeight);
            Assert.Equal(widestNatural, lane.RenderManager.LastRender.Padding.Left, 2);
        }
    }

    /// <summary>
    /// A quiet stack must not inherit a busy stack's gutter. <c>MinimumSize</c> is a floor, so leaving the
    /// previous refresh's value in place would ratchet: once a server spiked into six digits, every later
    /// refresh would keep reserving room for a number no longer on screen.
    /// </summary>
    [Fact]
    public void TheGutter_ComesBackDown_WhenTheWideValuesGoAway()
    {
        Plot[] lanes = [Lane(IoLatencyMsMax), Lane(WaitMsPerSecMax)];
        float busy = LaneAxisAligner.AlignLeftGutters(lanes, LaneHeight);

        foreach (var lane in lanes)
        {
            lane.Axes.SetLimitsY(0, BlockingMax);
        }

        float quiet = LaneAxisAligner.AlignLeftGutters(lanes, LaneHeight);

        Assert.True(quiet < busy,
            $"gutter stayed at {quiet:F2} px after the six-digit lane went away (was {busy:F2})");
    }

    /* ---------------- the two premises the measuring render rests on ---------------- */

    /// <summary>
    /// The aligner measures at a fixed, cheap <see cref="LaneAxisAligner.MeasureWidth"/> because the left
    /// gutter is a function of the Y tick labels only. Pin that: if a ScottPlot change ever made the left
    /// gutter depend on plot width, the measuring render would stop describing the real one.
    /// </summary>
    [Theory]
    [InlineData(IoLatencyMsMax)]
    [InlineData(BlockingMax)]
    [InlineData(CpuMax)]
    [InlineData(BufferPoolMbMax)]
    [InlineData(WaitMsPerSecMax)]
    public void TheLeftGutter_DoesNotDependOnPlotWidth(double yMax)
    {
        float? reference = null;
        foreach (int width in new[] { LaneAxisAligner.MeasureWidth, 300, 900, 1920, 3840 })
        {
            var plot = Lane(yMax);
            plot.RenderInMemory(width, LaneHeight);
            float gutter = plot.RenderManager.LastRender.Padding.Left;

            reference ??= gutter;
            Assert.Equal(reference.Value, gutter, 2);
        }
    }

    /// <summary>
    /// Height is the dimension that DOES move the gutter - a taller plot gets denser Y ticks, which can add
    /// a decimal place and a digit of width. The WPF entry point therefore measures at the lanes' real
    /// height and falls back to a deliberately generous one, which is only safe because the gutter is
    /// non-decreasing in height: measuring taller can over-reserve, never under-reserve. Pin that direction
    /// across the whole range the aligner will ever measure at - its own clamp, 60 px to 4000 px - with the
    /// fallback height sitting in the sequence at its true ordinal position.
    /// </summary>
    [Theory]
    [InlineData(IoLatencyMsMax)]
    [InlineData(BlockingMax)]
    [InlineData(CpuMax)]
    [InlineData(BufferPoolMbMax)]
    [InlineData(WaitMsPerSecMax)]
    public void TheLeftGutter_NeverShrinksAsThePlotGetsTaller(double yMax)
    {
        /* Sorted, not written in order by hand: the fallback height is one of the samples, and a claim about
           "as the plot gets taller" is meaningless if the sequence stops ascending because that constant
           moved. Sorting keeps the pin honest whatever value it takes. */
        int[] heights = [60, 100, 150, 200, 320, 500, LaneAxisAligner.FallbackMeasureHeight, 800, 1200, 2000, 3000, 4000];
        Array.Sort(heights);

        float previous = 0;
        foreach (int height in heights)
        {
            var plot = Lane(yMax);
            plot.RenderInMemory(LaneWidth, height);
            float gutter = plot.RenderManager.LastRender.Padding.Left;

            Assert.True(gutter >= previous - 0.01f,
                $"gutter fell from {previous:F2} to {gutter:F2} px at {height} px tall - measuring tall no longer bounds measuring short");
            previous = gutter;
        }
    }

    /* ---------------- the wiring (Darling side) ---------------- */

    /// <summary>
    /// The helper working proves nothing if the control never calls it - the defect was an absent step, not
    /// a wrong calculation, so a behavioral test alone would have passed on the broken build. Parse the real
    /// control and assert the aligner runs inside <c>SyncXAxes</c> and BEFORE the loop that refreshes the
    /// lanes, since a floor applied after the render would not take effect until something else redrew.
    /// </summary>
    [Fact]
    public void TheDarlingOverviewControl_AlignsItsLanes_BeforeItRefreshesThem()
    {
        var source = ReadRepoFile(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Viewer", "CorrelatedTimelineLanesControl.xaml.cs"));
        const string What = "the Darling viewer's Overview control";

        string body = SyncXAxesBody(source, What);

        int align = body.IndexOf("LaneAxisAligner.AlignLeftGutters(", StringComparison.Ordinal);
        Assert.True(align >= 0,
            $"{What} sets identical X LIMITS but never gives the lanes one left gutter (#2533) - they will skew again the moment one lane's Y labels get wide");

        int refresh = body.IndexOf(".Refresh();", StringComparison.Ordinal);
        Assert.True(refresh > align,
            $"{What} aligns the lanes after refreshing them - the shared gutter would not appear until something else redrew");
    }

    /// <summary>
    /// The body of <c>SyncXAxes</c>, bounded by brace matching rather than by the next member's name, so a
    /// call in a LATER method can never satisfy the assertions above by accident - several later members
    /// of this control call <c>Refresh()</c> too.
    /// </summary>
    private static string SyncXAxesBody(string source, string what)
    {
        int start = source.IndexOf("private void SyncXAxes(", StringComparison.Ordinal);
        Assert.True(start >= 0, $"{what} no longer has a SyncXAxes - re-anchor this pin rather than deleting it");

        int open = source.IndexOf('{', start);
        Assert.True(open > start, $"{what}: SyncXAxes has no body");

        int depth = 0;
        int end = -1;
        for (int i = open; i < source.Length && end < 0; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                end = i;
            }
        }

        Assert.True(end > open, $"{what}: SyncXAxes body is unbalanced");
        return source[open..(end + 1)];
    }

    /// <summary>
    /// Walks up from this source file until the requested path resolves - the idiom the #1949 grid pins use.
    /// Deliberately NOT a <c>.git</c> probe: in a git WORKTREE <c>.git</c> is a FILE, not a directory, so a
    /// <c>Directory.Exists</c> check walks past the root and the pin fails everywhere feature work happens.
    /// </summary>
    private static string ReadRepoFile(string relativePath, [CallerFilePath] string callerPath = "")
    {
        var dir = Path.GetDirectoryName(callerPath);
        while (dir is not null && !File.Exists(Path.Combine(dir, relativePath)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, $"{relativePath} not found walking up from the test source");
        return File.ReadAllText(Path.Combine(dir!, relativePath));
    }
}
