/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// One day's worth of banded data supplied to <see cref="PerformanceCalendar"/>. Apps build one of
    /// these per day that had any collection (from their own <c>DailySummaryRow</c> range read), setting the
    /// <see cref="Band"/> they already computed via <see cref="DailyHealthBandCalculator"/>, a hover
    /// <see cref="Tooltip"/>, and the <see cref="Signals"/> + key metrics the shared day-detail panel renders
    /// on click. Days absent from the supplied set render as <see cref="DailyHealthBand.NoData"/> (no panel).
    /// </summary>
    public sealed class PerformanceCalendarDay
    {
        /// <summary>The calendar date (date component only; time ignored).</summary>
        public DateTime Date { get; init; }

        /// <summary>The composite health band that colors the cell.</summary>
        public DailyHealthBand Band { get; init; }

        /// <summary>Multi-line hover text summarizing the day's signals (see <see cref="DailyHealthBandCalculator.Describe"/>).</summary>
        public string Tooltip { get; init; } = string.Empty;

        /// <summary>The day's rolled-up signal counts — drives the day-detail panel's "why this day is
        /// &lt;band&gt;" reasons and which drill buttons it offers (<see cref="DailyHealthBandCalculator.BuildReasons"/> /
        /// <see cref="DailyHealthBandCalculator.AvailableDrills"/>).</summary>
        public DailyHealthSignals Signals { get; init; }

        /// <summary>The day's top wait type (for the panel's key-metrics line).</summary>
        public string TopWaitType { get; init; } = string.Empty;

        /// <summary>The day's total wait, in seconds (for the panel's key-metrics line).</summary>
        public decimal TotalWaitSeconds { get; init; }

        /// <summary>The day's distinct-query count (for the panel's key-metrics line), or <c>null</c> when the
        /// store's rollup tier never carried the day (#3653 A6, Darling only): the line then says "not
        /// materialized" rather than 0. Lite's host always assigns a measured count — it has no rollup tier —
        /// so the nullability is a no-op there.</summary>
        public long? UniqueQueries { get; init; }

        /// <summary>The day's peak/max block wait in ms (0 when unknown), shown on the panel's blocking reason.</summary>
        public long PeakBlockMs { get; init; }
    }

    /// <summary>Event args carrying a day-detail drill request: the clicked day plus which grid to jump to.
    /// The shared panel raises this; each host scopes its toolbar to the day and switches to the target tab.</summary>
    public sealed class PerformanceCalendarDrillEventArgs : EventArgs
    {
        public PerformanceCalendarDrillEventArgs(DateTime date, DayDrillTarget target)
        {
            Date = date;
            Target = target;
        }

        /// <summary>The day to scope the drill to (date component only).</summary>
        public DateTime Date { get; }

        /// <summary>Which grid the user asked to jump to for that day.</summary>
        public DayDrillTarget Target { get; }
    }

    /// <summary>Event args carrying the month the calendar wants data for (its first day, at midnight).</summary>
    public sealed class PerformanceCalendarMonthEventArgs : EventArgs
    {
        public PerformanceCalendarMonthEventArgs(DateTime month)
        {
            Month = month;
        }

        /// <summary>The first day of the month now being displayed.</summary>
        public DateTime Month { get; }
    }
}
