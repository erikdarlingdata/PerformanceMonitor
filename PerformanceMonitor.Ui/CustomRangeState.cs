/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The custom time range a tab holds (#4766): a pair of naive-UTC instants, or nothing when a preset ("last 4
/// hours") is selected. The pickers on screen are a rendering of this pair in the display zone, not the state:
/// a display-mode switch re-renders the same instants in the new zone (<see cref="Render"/>), so 06:30Z reads
/// 06:30 in UTC, 01:30 in a US Eastern server's zone and 23:30 the day before in a US Pacific desktop's, and
/// switching back returns to exactly 06:30Z. Only a typed edit parses text back into an instant
/// (<see cref="ApplyEdit"/>), and it does that through <see cref="DisplayZone.ToUtcBound"/>.
///
/// <para>Not thread-safe: a tab's state, read and written on its UI thread.</para>
/// </summary>
public sealed class CustomRangeState
{
    private DateTime? _fromUtc;
    private DateTime? _toUtc;

    /// <summary>True when a custom range is held; false for a preset.</summary>
    public bool IsCustom => _fromUtc.HasValue && _toUtc.HasValue;

    /// <summary>The held range's start, or <c>null</c> for a preset.</summary>
    public DateTime? FromUtc => _fromUtc;

    /// <summary>The held range's end, or <c>null</c> for a preset.</summary>
    public DateTime? ToUtc => _toUtc;

    /// <summary>Holds the range <paramref name="fromUtc"/> to <paramref name="toUtc"/> (naive UTC), as given.</summary>
    public void Set(DateTime fromUtc, DateTime toUtc)
    {
        _fromUtc = DateTime.SpecifyKind(fromUtc, DateTimeKind.Unspecified);
        _toUtc = DateTime.SpecifyKind(toUtc, DateTimeKind.Unspecified);
    }

    /// <summary>Holds nothing: a preset window is in force.</summary>
    public void Clear()
    {
        _fromUtc = null;
        _toUtc = null;
    }

    /// <summary>
    /// The held range as the wall clock of <paramref name="zone"/>, for the pickers to show, or <c>null</c> for a
    /// preset. Each bound is rendered as its own instant, so a bound in the repeated hour shows that occurrence's
    /// wall time and a switch of zone changes only the text.
    /// </summary>
    public (DateTime From, DateTime To)? Render(TimeZoneInfo zone)
    {
        if (!IsCustom)
        {
            return null;
        }

        return (DisplayZone.ToDisplay(_fromUtc!.Value, zone), DisplayZone.ToDisplay(_toUtc!.Value, zone));
    }

    /// <summary>
    /// A typed edit: reads <paramref name="wall"/> as a wall-clock bound in <paramref name="zone"/>
    /// (<see cref="DisplayZone.ToUtcBound"/>), holds it as that side of the range when a range is held, and returns
    /// the instant. When a preset is in force nothing is held to change, so the bound is only converted: the caller
    /// pairs it with the other picker's value and calls <see cref="Set"/>. It does not check the pair's order.
    /// </summary>
    public DateTime ApplyEdit(DateTime wall, BoundSide side, TimeZoneInfo zone)
    {
        var bound = DisplayZone.ToUtcBound(wall, zone, side);
        if (IsCustom)
        {
            if (side == BoundSide.From)
            {
                _fromUtc = bound;
            }
            else
            {
                _toUtc = bound;
            }
        }

        return bound;
    }
}
