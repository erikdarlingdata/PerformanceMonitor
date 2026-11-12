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

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// #8109: the hour bins and bar geometry of the Memory Pressure Events chart, shared by Lite's Memory tab and the Viewer's twin.
    /// Samples are binned on the display zone's hour, where the axis draws its ticks, and each hour draws two stacks inside its own
    /// hour: SQL Server (process pressure) left of the hour's midpoint and Operating System (system pressure) right of it, each
    /// stacked medium under severe. A bar's position is its centre, so each stack is placed off the hour's midpoint and never off
    /// its starting tick: the first hour is not cut by the axis start, and the newest, partial hour is drawn between its tick and now.
    /// </summary>
    public static class MemoryPressureBars
    {
        /// <summary>One hour in OADate days: the slot an hour's stacks sit inside.</summary>
        public const double HourWidth = 1.0 / 24.0;

        /// <summary>Each stack's width in OADate days (24 minutes).</summary>
        public const double BarSize = HourWidth * 0.4;

        /// <summary>How far each stack's centre sits from the hour's midpoint (13.2 minutes), the SQL Server stack to the left and the
        /// Operating System stack to the right, so the two straddle the midpoint with a gap between them and both lie inside the hour.</summary>
        public const double BarOffset = HourWidth * 0.22;

        /// <summary>
        /// The hours with pressure, oldest first, one per display-zone hour that has a sample. Medium is an indicator of 2 and severe
        /// 3 or more, on each side; a sample with no pressure on a side counts nothing there.
        /// </summary>
        public static List<MemoryPressureHour> Place(IEnumerable<(DateTime SampleTimeUtc, int ProcessIndicator, int SystemIndicator)> samples, TimeZoneInfo zone) =>
            samples
                .GroupBy(s => HourStartUtc(s.SampleTimeUtc, zone))
                .OrderBy(g => g.Key)
                .Select(g => new MemoryPressureHour(
                    g.Key,
                    SqlMedium: g.Count(s => s.ProcessIndicator == 2),
                    SqlSevere: g.Count(s => s.ProcessIndicator >= 3),
                    OsMedium: g.Count(s => s.SystemIndicator == 2),
                    OsSevere: g.Count(s => s.SystemIndicator >= 3)))
                .ToList();

        /// <summary>
        /// The UTC instant at which the display zone's hour holding <paramref name="sampleTimeUtc"/> begins: where that hour's tick
        /// falls on an axis drawn in the zone. The same as the UTC hour in a whole-hour zone, thirty minutes off it in a half-hour
        /// zone. The zone's offset at the sample's own instant is used, so a sample near a daylight-saving change bins with the
        /// offset in force when it was taken.
        /// </summary>
        public static DateTime HourStartUtc(DateTime sampleTimeUtc, TimeZoneInfo zone)
        {
            var utc = DateTime.SpecifyKind(sampleTimeUtc, DateTimeKind.Utc);
            var offset = zone.GetUtcOffset(utc);
            var local = utc + offset;
            var localHour = new DateTime(local.Year, local.Month, local.Day, local.Hour, 0, 0, DateTimeKind.Utc);
            return localHour - offset;
        }
    }

    /// <summary>
    /// One hour of the Memory Pressure Events chart: its counts by side and severity, and where its two stacks are drawn.
    /// <see cref="HourStartUtc"/> is the hour's left edge, the UTC instant of the display zone's hour boundary; a stack's position is
    /// its centre in OADate days, <see cref="MemoryPressureBars.BarSize"/> wide.
    /// </summary>
    public readonly record struct MemoryPressureHour(DateTime HourStartUtc, int SqlMedium, int SqlSevere, int OsMedium, int OsSevere)
    {
        /// <summary>The centre of the SQL Server stack, left of the hour's midpoint.</summary>
        public double SqlStackPosition => HourStartUtc.ToOADate() + MemoryPressureBars.HourWidth / 2 - MemoryPressureBars.BarOffset;

        /// <summary>The centre of the Operating System stack, right of the hour's midpoint.</summary>
        public double OsStackPosition => HourStartUtc.ToOADate() + MemoryPressureBars.HourWidth / 2 + MemoryPressureBars.BarOffset;
    }
}
