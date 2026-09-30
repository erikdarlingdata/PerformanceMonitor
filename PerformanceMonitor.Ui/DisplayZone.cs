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

namespace PerformanceMonitor.Ui;

/// <summary>Which end of a range a typed bound is (<see cref="DisplayZone.ToUtcBound"/>).</summary>
public enum BoundSide
{
    /// <summary>The start of the range: it reaches back to the earliest instant the typed value can mean.</summary>
    From,

    /// <summary>The end of the range: it reaches forward to the latest instant the typed value can mean.</summary>
    To
}

/// <summary>
/// The display-zone side of a chart or a picker (#4766): a time is held as a naive-UTC instant, and the zone the
/// user reads it in (UTC, this desktop's zone, or the monitored server's zone) applies only when text is drawn
/// or typed text is read back. Everything here is a pure function of the instant and a <see cref="TimeZoneInfo"/>
/// the caller passes, so a chart axis, a hover, a picker and a test all agree, and none reads the machine's zone.
///
/// <para>A zone's wall clock is not a line. Where a daylight saving change puts the clock back, one wall time
/// happens twice; where it puts the clock forward, a stretch of wall times never happens. Four rules follow, and
/// each function below is one of them:</para>
/// <list type="bullet">
///   <item><see cref="ToDisplay"/> renders any instant, in either odd hour, as its own wall clock.</item>
///   <item><see cref="AmbiguousOffsetSuffix"/> tells the two occurrences of a repeated wall time apart.</item>
///   <item><see cref="ToUtcBound"/> turns a typed wall time into the widest instant range it can mean.</item>
///   <item><see cref="WallTicks"/> places axis ticks at whole wall times: none where the time never happened, one
///   per occurrence where it happened twice.</item>
/// </list>
///
/// <para>Every DateTime in and out is naive, <see cref="DateTimeKind.Unspecified"/>, the frame the stores keep.</para>
/// </summary>
public static class DisplayZone
{
    /// <summary>
    /// How far, in wall time, a tick search reaches past the visible instants. The wall clock of an instant inside
    /// the window can sit outside the wall clocks of the window's two ends when a change falls between them (the
    /// clock goes forward past the end's wall time and comes back), and no real zone shifts its clock by more than
    /// this in one change.
    /// </summary>
    private static readonly TimeSpan WallSearchSlack = TimeSpan.FromHours(4);

    /// <summary>
    /// The most wall multiples one <see cref="WallTicks"/> call examines. A caller picks a step that keeps the
    /// visible ticks to a few dozen; this only stops a mistaken step from allocating without bound.
    /// </summary>
    private const long MaxWallMultiples = 200_000;

    /// <summary>The wall-clock time of the instant <paramref name="naiveUtc"/> in <paramref name="zone"/>, Kind Unspecified.</summary>
    /// <remarks>
    /// Exact in the repeated hour: 05:30Z and 06:30Z on a US Eastern autumn change day both read 01:30, and each
    /// call answers for the instant it was given. A shift that runs past <see cref="DateTime.MinValue"/> or
    /// <see cref="DateTime.MaxValue"/> is held at that end of the calendar rather than thrown.
    /// </remarks>
    public static DateTime ToDisplay(DateTime naiveUtc, TimeZoneInfo zone)
        => Shift(naiveUtc, OffsetAt(naiveUtc, zone));

    /// <summary>
    /// The UTC offset of the instant <paramref name="naiveUtc"/> as "+hh:mm" or "-hh:mm" when its wall-clock time
    /// happens twice in <paramref name="zone"/>, else <c>null</c>. Two rows that both read 01:30 differ only by this
    /// suffix (-04:00 for the first, -05:00 for the second), so a hover or crosshair adds it exactly when the bare
    /// time would be ambiguous.
    /// </summary>
    public static string? AmbiguousOffsetSuffix(DateTime naiveUtc, TimeZoneInfo zone)
    {
        var wall = ToDisplay(naiveUtc, zone);
        return zone.IsAmbiguousTime(wall) ? FormatOffset(OffsetAt(naiveUtc, zone)) : null;
    }

    /// <summary>
    /// The UTC offset of the instant <paramref name="naiveUtc"/> in <paramref name="zone"/> as "+hh:mm" or "-hh:mm",
    /// for every instant, ambiguous or not ("+00:00" in UTC). A chart CSV export writes it in its own column, so each
    /// row names its instant exactly while the time column stays a plain date and time.
    /// </summary>
    public static string UtcOffsetText(DateTime naiveUtc, TimeZoneInfo zone)
        => FormatOffset(OffsetAt(naiveUtc, zone));

    /// <summary>
    /// The instant <paramref name="naiveUtc"/> as text in <paramref name="zone"/>: <paramref name="format"/> applied
    /// to <see cref="ToDisplay"/> with the invariant culture, then a space and <see cref="AmbiguousOffsetSuffix"/>
    /// when the wall time is ambiguous. The one string a hover or crosshair prints.
    /// </summary>
    public static string Format(DateTime naiveUtc, TimeZoneInfo zone, string format)
    {
        var text = ToDisplay(naiveUtc, zone).ToString(format, CultureInfo.InvariantCulture);
        var suffix = AmbiguousOffsetSuffix(naiveUtc, zone);
        return suffix is null ? text : text + " " + suffix;
    }

    /// <summary>
    /// The instant a typed wall-clock bound means, as the widest range the value can name:
    /// <list type="bullet">
    ///   <item><see cref="BoundSide.From"/> is the earliest instant whose wall clock reads at or after the value.</item>
    ///   <item><see cref="BoundSide.To"/> is the latest instant whose wall clock reads at or before it.</item>
    /// </list>
    /// A wall time that happens once is that one instant. In the repeated hour, From takes the first occurrence and
    /// To the second, so a range typed as 01:00 to 01:45 on the autumn change day holds every instant labelled in
    /// it. A wall time that never happened (02:30 on the spring change day) gives the change instant for both sides.
    /// UTC is never ambiguous. Never throws for a value inside the calendar; a shift that runs past either end of
    /// the calendar is held at that end rather than thrown.
    /// </summary>
    /// <param name="wall">The typed wall-clock value.</param>
    /// <param name="zone">The zone the value was typed in.</param>
    /// <param name="side">Which end of the range the value is.</param>
    public static DateTime ToUtcBound(DateTime wall, TimeZoneInfo zone, BoundSide side)
    {
        var local = DateTime.SpecifyKind(wall, DateTimeKind.Unspecified);

        if (zone.IsInvalidTime(local))
        {
            return SkippedChangeInstant(local, zone);
        }

        if (zone.IsAmbiguousTime(local))
        {
            /* An instant is the wall time minus its offset, so the larger offset is the earlier instant: the
               first occurrence. From takes it, To takes the smaller offset, the second occurrence. */
            var offsets = zone.GetAmbiguousTimeOffsets(local);
            var pick = offsets[0];
            foreach (var offset in offsets)
            {
                if (side == BoundSide.From ? offset > pick : offset < pick)
                {
                    pick = offset;
                }
            }

            return Shift(local, -pick);
        }

        return Shift(local, -zone.GetUtcOffset(local));
    }

    /// <summary>
    /// The instant of a wall-clock time by the rule a stored server-local stamp uses (the same one
    /// <c>ServerClock.ToUtc</c> applies): a repeated time takes the FIRST occurrence, and a time that never
    /// happened moves forward by the gap (02:30 on the spring change day reads as 03:30). For a value that is
    /// already a place on another day's clock, such as a comparison line drawn a day later, where there is no typed
    /// range to widen.
    /// </summary>
    public static DateTime ToUtcFirstOccurrence(DateTime wall, TimeZoneInfo zone)
    {
        var local = DateTime.SpecifyKind(wall, DateTimeKind.Unspecified);
        if (zone.IsAmbiguousTime(local))
        {
            var offsets = zone.GetAmbiguousTimeOffsets(local);
            var first = offsets[0];
            foreach (var offset in offsets)
            {
                if (offset > first)
                {
                    first = offset;
                }
            }

            return Shift(local, -first);
        }

        /* An invalid (skipped) time answers with the offset from before the change, which lands it on the far
           side of the gap. */
        return Shift(local, -zone.GetUtcOffset(local));
    }

    /// <summary>
    /// The ticks of a time axis over the instants <paramref name="minUtc"/> to <paramref name="maxUtc"/> (both ends
    /// inclusive), at every whole wall-clock multiple of <paramref name="step"/> in <paramref name="zone"/>, as
    /// (instant, wall clock) pairs in instant order. "Whole multiple" counts from the start of the calendar, so a
    /// step that divides a day (every step a chart picks) lands on the hour, the half hour, the quarter hour and
    /// midnight, and a zone with a half-hour offset gets its whole LOCAL hours (India's 10:00 sits at 04:30Z).
    ///
    /// <para>A wall time that never happened gets no tick, so the spring gap has none between 01:30 and 03:00. A
    /// wall time that happened twice gets one tick per occurrence, at two different instants, so the autumn axis
    /// reads 01:00 01:30 01:00 01:30 and the four sit an hour apart in real time.</para>
    /// </summary>
    /// <param name="minUtc">The first visible instant.</param>
    /// <param name="maxUtc">The last visible instant.</param>
    /// <param name="step">The wall-clock spacing; positive.</param>
    /// <param name="zone">The zone the ticks are whole in.</param>
    public static IReadOnlyList<(DateTime Utc, DateTime Wall)> WallTicks(
        DateTime minUtc, DateTime maxUtc, TimeSpan step, TimeZoneInfo zone)
    {
        if (step <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(step), step, "The tick step must be positive.");
        }

        var ticks = new List<(DateTime Utc, DateTime Wall)>();
        if (maxUtc < minUtc)
        {
            return ticks;
        }

        var stepTicks = step.Ticks;
        var lowestMultiple = FloorDivide(Shift(ToDisplay(minUtc, zone), -WallSearchSlack).Ticks, stepTicks);
        var highestMultiple = FloorDivide(Shift(ToDisplay(maxUtc, zone), WallSearchSlack).Ticks, stepTicks) + 1;
        var lastMultiple = DateTime.MaxValue.Ticks / stepTicks;
        lowestMultiple = Math.Max(lowestMultiple, 0);
        highestMultiple = Math.Min(highestMultiple, lastMultiple);

        if (highestMultiple - lowestMultiple >= MaxWallMultiples)
        {
            throw new ArgumentOutOfRangeException(
                nameof(step), step, "The step is too small for the span: it would place more ticks than an axis can show.");
        }

        for (var k = lowestMultiple; k <= highestMultiple; k++)
        {
            var wall = new DateTime(k * stepTicks, DateTimeKind.Unspecified);
            if (zone.IsInvalidTime(wall))
            {
                continue;
            }

            if (zone.IsAmbiguousTime(wall))
            {
                foreach (var offset in zone.GetAmbiguousTimeOffsets(wall))
                {
                    AddIfVisible(ticks, Shift(wall, -offset), wall, minUtc, maxUtc);
                }
            }
            else
            {
                AddIfVisible(ticks, Shift(wall, -zone.GetUtcOffset(wall)), wall, minUtc, maxUtc);
            }
        }

        /* The two occurrences of a repeated wall time come out beside each other, an hour apart in real time, with
           the ticks between them still to come: put the list in instant order. */
        ticks.Sort(static (a, b) => a.Utc.CompareTo(b.Utc));
        return ticks;
    }

    private static void AddIfVisible(
        List<(DateTime Utc, DateTime Wall)> ticks, DateTime utc, DateTime wall, DateTime minUtc, DateTime maxUtc)
    {
        if (utc >= minUtc && utc <= maxUtc)
        {
            ticks.Add((utc, wall));
        }
    }

    /// <summary>
    /// The instant a clock change skips a wall time over: the first instant on the far side of the gap. Found by
    /// halving the day either side of the wall time until the offset flips, so it needs no reading of the zone's
    /// adjustment rules and is exact to the tick.
    /// </summary>
    private static DateTime SkippedChangeInstant(DateTime skippedWall, TimeZoneInfo zone)
    {
        var lowTicks = Math.Max(skippedWall.Ticks - TimeSpan.TicksPerDay, DateTime.MinValue.Ticks);
        var highTicks = Math.Min(skippedWall.Ticks + TimeSpan.TicksPerDay, DateTime.MaxValue.Ticks);
        var before = OffsetAtTicks(lowTicks, zone);

        if (OffsetAtTicks(highTicks, zone) == before)
        {
            /* No change within a day of a time the zone calls invalid: not a shape a real zone has. Answer as a
               stored server-local stamp would rather than guess. */
            return ToUtcFirstOccurrence(skippedWall, zone);
        }

        while (highTicks - lowTicks > 1)
        {
            var middle = lowTicks + (highTicks - lowTicks) / 2;
            if (OffsetAtTicks(middle, zone) == before)
            {
                lowTicks = middle;
            }
            else
            {
                highTicks = middle;
            }
        }

        return new DateTime(highTicks, DateTimeKind.Unspecified);
    }

    private static TimeSpan OffsetAt(DateTime naiveUtc, TimeZoneInfo zone)
        => zone.GetUtcOffset(DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc));

    private static TimeSpan OffsetAtTicks(long utcTicks, TimeZoneInfo zone)
        => zone.GetUtcOffset(new DateTime(utcTicks, DateTimeKind.Utc));

    private static string FormatOffset(TimeSpan offset)
    {
        var magnitude = offset.Duration();
        return string.Create(
            CultureInfo.InvariantCulture,
            $"{(offset < TimeSpan.Zero ? '-' : '+')}{magnitude.Hours:00}:{magnitude.Minutes:00}");
    }

    private static long FloorDivide(long dividend, long divisor)
    {
        var quotient = dividend / divisor;
        return dividend % divisor != 0 && (dividend < 0) != (divisor < 0) ? quotient - 1 : quotient;
    }

    /// <summary>
    /// <paramref name="value"/> moved by <paramref name="by"/>, held at the ends of the calendar rather than
    /// thrown, so a sentinel bound survives the conversion. Kind Unspecified.
    /// </summary>
    internal static DateTime Shift(DateTime value, TimeSpan by)
    {
        var ticks = value.Ticks + by.Ticks;
        if (ticks < DateTime.MinValue.Ticks)
        {
            return DateTime.MinValue;
        }

        if (ticks > DateTime.MaxValue.Ticks)
        {
            return DateTime.MaxValue;
        }

        return new DateTime(ticks, DateTimeKind.Unspecified);
    }
}
