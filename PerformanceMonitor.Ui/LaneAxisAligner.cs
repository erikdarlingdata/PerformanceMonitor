/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Gives a stack of correlated timeline lanes ONE left-hand gutter, so their data areas start at the
/// same X pixel and are the same width.
/// </summary>
/// <remarks>
/// <para>
/// #2533: the Overview lanes are five SEPARATE <c>WpfPlot</c> controls. Matching their X-axis LIMITS
/// (<c>SyncXAxes</c>) makes them agree on the time range but not on where that range lands in pixels:
/// every plot sizes its own left gutter from its own tick labels, so a lane whose Y ticks read
/// <c>120,000</c> starts its data area ~27 px further right than one reading <c>0.4</c>. Every value
/// stays correct - which is why tooltips still read true - but the lanes stop lining up vertically,
/// and the whole point of the surface is reading five signals against one time ruler.
/// </para>
/// <para>
/// Mechanism: <c>Axes.Left.MinimumSize</c>, applied to all lanes at the widest lane's natural gutter.
/// It constrains exactly the one dimension that is wrong and leaves the rest of the layout automatic.
/// The alternatives were rejected deliberately:
/// <c>Layout.Fixed(PixelPadding)</c> pins all four sides, and the BOTTOM lane legitimately needs more
/// bottom room than the other four because it alone renders the time tick labels; and
/// <c>LayoutEngines.MatchedDataRect</c> copies one reference plot's whole data rect, which both
/// re-introduces the vertical problem and squeezes every lane to the REFERENCE lane's gutter - if the
/// reference is a narrow-label lane the wide-label lane's numbers get clipped, which is the failure
/// mode this exists to avoid.
/// </para>
/// <para>
/// The width is DERIVED, not frozen. A constant would have to cover wait ms/sec at five or six digits
/// (~63-73 px) and would then steal that much from every server whose lanes read in single digits.
/// Measuring means each stack pays for the labels it actually has.
/// </para>
/// <para>
/// This half is deliberately framework-free - plain <see cref="ScottPlot.Plot"/>, no WPF - because
/// that is what makes the defect testable. ScottPlot renders headlessly, so CI (and a throwaway
/// console harness) can build two lanes with mismatched Y magnitudes, run this, and compare data
/// rectangles with no window, no dispatcher and no desktop. The WPF entry point lives in
/// <c>LaneAxisAligner.Wpf.cs</c> and does nothing but pick the measuring height.
/// </para>
/// </remarks>
internal static partial class LaneAxisAligner
{
    /// <summary>
    /// Width of the throwaway measuring render. The left gutter is a function of the Y tick LABELS, and
    /// those do not depend on how wide the plot is - measured identical from 200 px to 3840 px across
    /// every magnitude from 0.4 to 5,000,000 (pinned by <c>LaneAxisAlignerTests</c>). So this is chosen
    /// to be cheap, not representative.
    /// </summary>
    internal const int MeasureWidth = 400;

    /// <summary>
    /// Height used when the caller cannot supply the lanes' real one (control not laid out yet). Height
    /// DOES matter - a taller plot gets denser Y ticks, which can add a decimal place and one digit of
    /// width - so this errs on the generous side: over-measuring costs at most one digit of gutter,
    /// under-measuring lets the tallest lane out-grow the floor and misalign again.
    /// </summary>
    internal const int FallbackMeasureHeight = 600;

    /// <summary>Clamp for a caller-supplied height, guarding a zero/absurd value from WPF layout.</summary>
    private const int MinMeasureHeight = 60;
    private const int MaxMeasureHeight = 4000;

    /// <summary>
    /// Core, framework-free form: measures every lane's natural left gutter and floors them all at the
    /// widest one. Takes plain <see cref="ScottPlot.Plot"/> objects and renders in memory, so it is
    /// exercisable headlessly with no window, no dispatcher and no WPF.
    /// </summary>
    /// <param name="lanes">The stacked lanes, already carrying their final data and axis limits.</param>
    /// <param name="measureHeight">
    /// The height the lanes will really render at. Any value at or above the real height yields exact
    /// alignment (the gutter is non-decreasing in height, so measuring taller can only over-reserve);
    /// measuring SHORTER is the direction that can misalign.
    /// </param>
    /// <returns>The shared gutter width in pixels, or 0 if nothing was aligned.</returns>
    internal static float AlignLeftGutters(IReadOnlyList<ScottPlot.Plot> lanes, float measureHeight)
    {
        if (lanes is null || lanes.Count == 0)
            return 0f;

        int height = (int)Math.Clamp(measureHeight, MinMeasureHeight, MaxMeasureHeight);

        try
        {
            /* Clear first, every time. MinimumSize is a FLOOR: left in place from a previous refresh it
               would ratchet - the stack would keep the gutter a six-digit spike once needed and never
               give it back when the values came down. */
            foreach (var plot in lanes)
                plot.Axes.Left.MinimumSize = 0f;

            float widest = 0f;
            foreach (var plot in lanes)
            {
                plot.RenderInMemory(MeasureWidth, height);
                widest = Math.Max(widest, plot.RenderManager.LastRender.Padding.Left);
            }

            foreach (var plot in lanes)
                plot.Axes.Left.MinimumSize = widest;

            return widest;
        }
        catch (Exception ex)
        {
            /* A measuring render is not worth losing the Overview over. Leave every lane un-floored -
               that is exactly the pre-#2533 behavior, correct data with a ragged left edge - rather than
               half-floored, which would misalign in a new and more confusing way. */
            Debug.WriteLine($"Lane gutter alignment skipped: {ex.Message}");
            foreach (var plot in lanes)
                plot.Axes.Left.MinimumSize = 0f;
            return 0f;
        }
    }
}
