/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #8539: when a due scheduled analysis pass may start. The pass runs inside its server's fleet collection slot, after the
/// collectors, and holds the slot for its whole length, so every pass that is running is a slot collection cannot use. After
/// a service restart every server's first pass comes due within a few minutes; on a store whose pass is long they took most
/// of the slots at once, and collection bodies waited minutes for one and stepped over their one-minute slots. Two rules keep
/// collection ahead of the passes:
/// <list type="bullet">
/// <item>passes hold at most <see cref="SlotShare"/> of the fleet's slots at once, so collection always has the rest;</item>
/// <item>a due pass does not start while a collection body is waiting for a slot, for up to <see cref="YieldLimit"/> past
/// its due time. After that it starts within the share, so a fleet whose bodies always queue is still analysed.</item>
/// </list>
/// A pass that does not start keeps its due stamp, and the server's next body asks again. Both rules count from the pass's
/// due time, so a stamp that has none is given one by the first body that finds it due (<see cref="DueSince"/>). A pass
/// that is still refused more than one analysis interval past its due time is reported once per interval
/// (<see cref="WarnsLate"/>), so reaching the share is never silent.
/// </summary>
internal static class ScheduledAnalysisAdmission
{
    /// <summary>What a due pass is told when it asks to start.</summary>
    internal enum Answer
    {
        /// <summary>It took a place in the share and runs.</summary>
        Starts,

        /// <summary>A collection body is waiting for a fleet slot and the pass is under <see cref="YieldLimit"/> past due.</summary>
        GivesWayToCollection,

        /// <summary>Passes already hold the whole <see cref="SlotShare"/>.</summary>
        ShareIsFull,
    }

    /// <summary>
    /// The due time the two rules count from, for a stamp that is due at <paramref name="nowUtc"/>. A stamp at or behind the
    /// clock is its own due time. A stamp more than one <paramref name="interval"/> ahead is due at once (#4732: an interval
    /// that was shortened, or a clock that stepped back) and has no due time of its own, so it is due from now. A stamp
    /// never set is past the give-way limit by definition, so it is due since <see cref="YieldLimit"/> ago. The caller
    /// writes the answer back to the stamp, so the next body counts from the same time.
    /// </summary>
    internal static DateTime DueSince(DateTime stampUtc, DateTime nowUtc, TimeSpan interval)
    {
        if (stampUtc == DateTime.MinValue)
        {
            return nowUtc - YieldLimit;
        }

        return stampUtc - nowUtc > interval ? nowUtc : stampUtc;
    }

    /// <summary>The answer for a pass due since <paramref name="dueUtc"/> that asks at <paramref name="nowUtc"/>. Giving way
    /// is asked first and takes no place; a place is taken only by <see cref="Answer.Starts"/>.</summary>
    internal static Answer Ask(int bodiesWaiting, DateTime dueUtc, DateTime nowUtc, ref int passesInSlot, int fleetSlots)
    {
        if (GivesWay(bodiesWaiting, dueUtc, nowUtc))
        {
            return Answer.GivesWayToCollection;
        }

        return TryTakePlace(ref passesInSlot, fleetSlots) ? Answer.Starts : Answer.ShareIsFull;
    }

    /// <summary>Whether a refused pass is reported now: it is more than one <paramref name="interval"/> past its due time,
    /// and the last report is at least one interval old. A last report ahead of the clock (a clock that stepped back)
    /// counts as old.</summary>
    internal static bool WarnsLate(DateTime dueUtc, DateTime nowUtc, TimeSpan interval, DateTime lastWarnedUtc) =>
        nowUtc - dueUtc > interval && (lastWarnedUtc > nowUtc || nowUtc - lastWarnedUtc >= interval);

    /// <summary>The longest a due pass gives way to waiting collection bodies: the shortest analysis interval.</summary>
    internal static readonly TimeSpan YieldLimit = TimeSpan.FromMinutes(5);

    /// <summary>How many of <paramref name="fleetSlots"/> scheduled passes may hold at once: half of them, and never fewer
    /// than one.</summary>
    internal static int SlotShare(int fleetSlots) => Math.Max(1, fleetSlots / 2);

    /// <summary>Whether a pass due since <paramref name="dueUtc"/> gives way: a collection body is waiting for a fleet slot
    /// and the pass is not yet <see cref="YieldLimit"/> past due.</summary>
    internal static bool GivesWay(int bodiesWaiting, DateTime dueUtc, DateTime nowUtc) =>
        bodiesWaiting > 0 && nowUtc - dueUtc < YieldLimit;

    /// <summary>
    /// Takes one of the share's places for a pass that is starting, or returns false when passes already hold the whole
    /// share. The count moves only when the place is taken, so two bodies asking at once cannot both take the last one. The
    /// caller gives the place back by decrementing <paramref name="passesInSlot"/> when the pass ends.
    /// </summary>
    internal static bool TryTakePlace(ref int passesInSlot, int fleetSlots)
    {
        var share = SlotShare(fleetSlots);
        while (true)
        {
            var held = Volatile.Read(ref passesInSlot);
            if (held >= share)
            {
                return false;
            }

            if (Interlocked.CompareExchange(ref passesInSlot, held + 1, held) == held)
            {
                return true;
            }
        }
    }
}
