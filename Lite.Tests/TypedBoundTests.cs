/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a range typed into a picker names wall-clock times, and a wall-clock time is not always one instant.
/// <see cref="DisplayZone.ToUtcBound"/> reads a typed bound as the widest range of instants it can mean: FROM is
/// the earliest instant whose wall clock reads at or after the value, TO the latest whose wall clock reads at or
/// before it, so a range typed over the repeated hour holds every instant labelled in it. The rule takes no
/// account of what the picker held before: a typed value means the same thing whatever it replaced.
/// </summary>
public sealed class TypedBoundTests
{
    [Fact]
    public void RepeatedHour_FromTakesTheFirst_ToTakesTheSecond()
    {
        /* Server display: US Eastern, 01:00 to 01:45 on the autumn change day. */
        var from = DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 11, 1, 1, 0), DisplayZoneFixtures.Eastern, BoundSide.From);
        var to = DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 11, 1, 1, 45), DisplayZoneFixtures.Eastern, BoundSide.To);

        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 5, 0), from);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 6, 45), to);
        Assert.Equal(DateTimeKind.Unspecified, from.Kind);

        /* Local display on a desktop in another zone (US Pacific, back at 09:00Z): the same rule in its own repeated hour. */
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 8, 0),
            DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 11, 1, 1, 0), DisplayZoneFixtures.Pacific, BoundSide.From));
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 9, 45),
            DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 11, 1, 1, 45), DisplayZoneFixtures.Pacific, BoundSide.To));
    }

    [Fact]
    public void SkippedHour_BothBoundsTakeTheChangeInstant()
    {
        /* 02:30 on the spring change day never happened on the server's clock; the change instant is 07:00Z. */
        var wall = DisplayZoneFixtures.At(2026, 3, 8, 2, 30);

        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 7, 0), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.Eastern, BoundSide.From));
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 7, 0), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.Eastern, BoundSide.To));

        /* The desktop's own change is at 10:00Z. */
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 10, 0), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.Pacific, BoundSide.From));
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 10, 0), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.Pacific, BoundSide.To));

        /* The edges of the gap: 01:59 is the last minute before it and 03:00 the first after, each one instant. */
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 6, 59), DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 3, 8, 1, 59), DisplayZoneFixtures.Eastern, BoundSide.To));
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 7, 0), DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 3, 8, 3, 0), DisplayZoneFixtures.Eastern, BoundSide.From));
        Assert.Equal(DisplayZoneFixtures.At(2026, 3, 8, 7, 0), DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 3, 8, 3, 0), DisplayZoneFixtures.Eastern, BoundSide.To));
    }

    [Fact]
    public void AnEditInsideTheSecondOccurrence_TakesTheFirstForFrom()
    {
        /* Server display, US Eastern, autumn change day. The pickers held FROM 01:00 (second occurrence, 06:00Z) and
           TO 02:00 (07:00Z). FROM is edited to 01:15. The typed value is read on its own: the earliest instant that
           reads 01:15 is the first occurrence, 05:15Z, however the old value was placed. */
        var state = new CustomRangeState();
        state.Set(DisplayZoneFixtures.At(2026, 11, 1, 6, 0), DisplayZoneFixtures.At(2026, 11, 1, 7, 0));

        var from = state.ApplyEdit(DisplayZoneFixtures.At(2026, 11, 1, 1, 15), BoundSide.From, DisplayZoneFixtures.Eastern);
        var to = state.ApplyEdit(DisplayZoneFixtures.At(2026, 11, 1, 2, 0), BoundSide.To, DisplayZoneFixtures.Eastern);

        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 5, 15), from);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 7, 0), to);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 5, 15), state.FromUtc);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 7, 0), state.ToUtc);
    }

    [Fact]
    public void ADayThatIsNotAChangeDay_IsOneInstantForBothSides()
    {
        var wall = DisplayZoneFixtures.At(2026, 7, 1, 9, 30);

        Assert.Equal(DisplayZoneFixtures.At(2026, 7, 1, 13, 30), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.Eastern, BoundSide.From));
        Assert.Equal(DisplayZoneFixtures.At(2026, 7, 1, 13, 30), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.Eastern, BoundSide.To));
        Assert.Equal(wall, DisplayZone.ToUtcBound(wall, TimeZoneInfo.Utc, BoundSide.From));
        Assert.Equal(DisplayZoneFixtures.At(2026, 7, 1, 4, 0), DisplayZone.ToUtcBound(wall, DisplayZoneFixtures.India, BoundSide.To));
    }

    [Fact]
    public void ARepeatedHalfHour_InALordHoweRange_IsHeldWhole()
    {
        /* Lord Howe's repeated 01:30-02:00 (14:30Z first, 15:00Z second): a range typed 01:30 to 01:45 holds both passes. */
        var from = DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 4, 5, 1, 30), DisplayZoneFixtures.LordHowe, BoundSide.From);
        var to = DisplayZone.ToUtcBound(DisplayZoneFixtures.At(2026, 4, 5, 1, 45), DisplayZoneFixtures.LordHowe, BoundSide.To);

        Assert.Equal(DisplayZoneFixtures.At(2026, 4, 4, 14, 30), from);
        Assert.Equal(DisplayZoneFixtures.At(2026, 4, 4, 15, 15), to);
    }

    [Fact]
    public void ARangeTypedOverTheRepeatedHour_HoldsEveryInstantLabelledInIt()
    {
        /* The property behind the rule, checked on every minute of the change day instead of a few chosen ones: an
           instant whose label falls inside the typed range is inside the range ToUtcBound returns. */
        var zone = DisplayZoneFixtures.Eastern;
        var typedFrom = DisplayZoneFixtures.At(2026, 11, 1, 0, 40);
        var typedTo = DisplayZoneFixtures.At(2026, 11, 1, 2, 10);
        var from = DisplayZone.ToUtcBound(typedFrom, zone, BoundSide.From);
        var to = DisplayZone.ToUtcBound(typedTo, zone, BoundSide.To);

        for (var instant = DisplayZoneFixtures.At(2026, 11, 1, 3, 0); instant <= DisplayZoneFixtures.At(2026, 11, 1, 10, 0); instant = instant.AddMinutes(1))
        {
            var label = DisplayZone.ToDisplay(instant, zone);
            if (label >= typedFrom && label <= typedTo)
            {
                Assert.True(instant >= from && instant <= to, $"{instant:HH:mm}Z reads {label:HH:mm} and is inside the typed range, but the window is {from:HH:mm}Z-{to:HH:mm}Z");
            }
        }

        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 4, 40), from);
        Assert.Equal(DisplayZoneFixtures.At(2026, 11, 1, 7, 10), to);
    }
}
