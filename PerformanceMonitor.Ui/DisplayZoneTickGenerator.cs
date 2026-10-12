/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Ui;

/// <summary>
/// A ScottPlot bottom-axis tick generator for a chart whose X is the UTC instant (#4766): ticks sit at whole
/// wall-clock times of the display zone, labelled in that zone, with the date on the first tick and at every date
/// change. It replaces <c>DateTimeAutomatic</c> on such an axis because that generator places ticks at whole
/// multiples of the X value, which are whole UTC times, not whole local ones (a chart in India would put "10:00"
/// at 04:30Z), and it cannot tell a repeated hour from a skipped one.
///
/// <para>The zone is a <see cref="Func{TResult}"/> read at every render pass, so a display-mode switch changes the
/// next render's labels with no re-plot: X does not change, only the text drawn under it.</para>
///
/// <para><b>Spacing.</b> The step is the finest of 1, 5, 15 and 30 minutes, 1, 3, 6 and 12 hours, and 1 day that
/// keeps the ticks no closer than <see cref="MinPixelsPerTick"/> pixels. A span beyond that many days at one day
/// steps thins to whole multiples of days rather than crowding the axis. A wall time that never happened has no
/// tick, and one that happened twice has a tick at each occurrence (<see cref="DisplayZone.WallTicks"/>), both
/// labelled with the same clock text: the crosshair and hover add the offset that tells them apart.</para>
///
/// <para><b>Labels.</b> "HH:mm" on the 24-hour clock. The first tick, and any tick on a different wall date from the
/// one before it, prints the month and day above the time in the culture's own order (the same pattern
/// <see cref="AxesExtensions.DateTimeTicksBottomDateChange"/> prints).</para>
/// </summary>
public sealed class DisplayZoneTickGenerator : ScottPlot.TickGenerators.IDateTimeTickGenerator
{
    /// <summary>The closest two ticks may sit, in pixels: room for a two-line date-and-time label.</summary>
    internal const double MinPixelsPerTick = 80;

    /// <summary>The steps a chart picks from, finest first. Every one divides a day.</summary>
    internal static readonly TimeSpan[] StepLadder =
    {
        TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(15), TimeSpan.FromMinutes(30),
        TimeSpan.FromHours(1), TimeSpan.FromHours(3), TimeSpan.FromHours(6), TimeSpan.FromHours(12),
        TimeSpan.FromDays(1)
    };

    private readonly Func<TimeZoneInfo> _zone;
    private readonly CultureInfo _culture;
    private readonly string _monthDayPattern;

    /// <inheritdoc />
    public ScottPlot.Tick[] Ticks { get; private set; } = Array.Empty<ScottPlot.Tick>();

    /// <inheritdoc />
    public int MaxTickCount { get; set; } = 10_000;

    /// <param name="zone">The display zone, read each time ticks are made.</param>
    /// <param name="culture">Orders the month and day of the date line; the current culture when omitted.</param>
    public DisplayZoneTickGenerator(Func<TimeZoneInfo> zone, CultureInfo? culture = null)
    {
        _zone = zone ?? throw new ArgumentNullException(nameof(zone));
        _culture = culture ?? CultureInfo.CurrentCulture;
        _monthDayPattern = AxesExtensions.MonthDayPatternFor(_culture);
    }

    /// <summary>
    /// ScottPlot's date axis only accepts a date tick generator, and asks it to place dates on the axis: a date is
    /// its OLE-Automation number, the same X every chart in this project plots.
    /// </summary>
    public IEnumerable<double> ConvertToCoordinateSpace(IEnumerable<DateTime> dates) => dates.Select(d => d.ToOADate());

    /// <inheritdoc />
    public void Regenerate(
        ScottPlot.CoordinateRange range, ScottPlot.Edge edge, ScottPlot.PixelLength size,
        ScottPlot.Paint paint, ScottPlot.LabelStyle labelStyle)
    {
        Ticks = Build(range.Min, range.Max, size.Length);
    }

    /// <summary>The ticks for an axis showing the OLE-Automation dates <paramref name="minX"/> to <paramref name="maxX"/> over <paramref name="pixels"/> pixels.</summary>
    internal ScottPlot.Tick[] Build(double minX, double maxX, double pixels)
    {
        /* Outside the legal OLE-Automation range (an empty or auto-scaled chart) there are no times to label,
           and FromOADate would throw inside a render. */
        if (double.IsNaN(minX) || double.IsNaN(maxX) || double.IsNaN(pixels)
            || minX <= -657435.0 || maxX >= 2958466.0 || maxX <= minX || pixels <= 0)
        {
            return Array.Empty<ScottPlot.Tick>();
        }

        var minUtc = DateTime.FromOADate(minX);
        var maxUtc = DateTime.FromOADate(maxX);
        var target = (int)Math.Clamp(Math.Floor(pixels / MinPixelsPerTick), 2, Math.Max(2, MaxTickCount));
        var step = ChooseStep(maxUtc - minUtc, target);

        IReadOnlyList<(DateTime Utc, DateTime Wall)> wallTicks;
        try
        {
            wallTicks = DisplayZone.WallTicks(minUtc, maxUtc, step, _zone());
        }
        catch (ArgumentOutOfRangeException)
        {
            return Array.Empty<ScottPlot.Tick>();
        }

        var ticks = new ScottPlot.Tick[wallTicks.Count];
        DateTime? previousDate = null;
        for (var i = 0; i < ticks.Length; i++)
        {
            var wall = wallTicks[i].Wall;
            var time = wall.ToString("HH:mm", CultureInfo.InvariantCulture);
            var label = previousDate is null || wall.Date != previousDate.Value
                ? wall.ToString(_monthDayPattern, _culture) + "\n" + time
                : time;
            previousDate = wall.Date;
            ticks[i] = ScottPlot.Tick.Major(wallTicks[i].Utc.ToOADate(), label);
        }

        return ticks;
    }

    /// <summary>
    /// The finest <see cref="StepLadder"/> step that leaves at most <paramref name="targetTickCount"/> ticks across
    /// <paramref name="span"/>. Past the top of the ladder, whole multiples of a day.
    /// </summary>
    internal static TimeSpan ChooseStep(TimeSpan span, int targetTickCount)
    {
        foreach (var step in StepLadder)
        {
            if (span.Ticks <= step.Ticks * (long)targetTickCount)
            {
                return step;
            }
        }

        var day = TimeSpan.FromDays(1);
        var days = (long)Math.Ceiling(span.TotalDays / targetTickCount);
        return TimeSpan.FromTicks(day.Ticks * Math.Max(1, days));
    }
}
