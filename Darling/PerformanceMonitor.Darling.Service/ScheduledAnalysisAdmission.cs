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
/// a service restart every server's first pass comes due within a few minutes. Two rules keep collection ahead of the
/// passes:
/// <list type="bullet">
/// <item>passes hold at most <see cref="SlotShare"/> of the fleet's slots at once: half of them, and the only slot of a
/// gate one wide, so collection has the rest on any gate wider than one;</item>
/// <item>a due pass does not start while a collection body has waited <see cref="WaitFloor"/> or longer for a slot, for
/// up to <see cref="YieldLimit"/> past its due time. After that it starts within the share, so a fleet whose bodies
/// always wait that long is still analysed.</item>
/// </list>
/// A shorter wait holds nothing back. Every sweep tick launches a body for every server, so on a fleet larger than its
/// gate all but the first few park for a moment on every tick, and that queue drains inside the tick and costs no
/// collection slot. A pass that does not start keeps its due stamp, and the server's next body asks again. Both rules count
/// from the pass's due time, so a stamp that has none is given one by the first body that finds it due
/// (<see cref="DueSince"/>). A pass that has been refused for more than one analysis interval is reported once per interval
/// (<see cref="WarnsLate"/>), so reaching the share is never silent.
/// </summary>
internal static class ScheduledAnalysisAdmission
{
    /// <summary>What a due pass is told when it asks to start.</summary>
    internal enum Answer
    {
        /// <summary>It took a place in the share and runs.</summary>
        Starts,

        /// <summary>A collection body has waited <see cref="WaitFloor"/> or longer for a fleet slot and the pass is under
        /// <see cref="YieldLimit"/> past due.</summary>
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
    internal static Answer Ask(TimeSpan? longestWait, DateTime dueUtc, DateTime nowUtc, ref int passesInSlot, int fleetSlots)
    {
        if (GivesWay(longestWait, dueUtc, nowUtc))
        {
            return Answer.GivesWayToCollection;
        }

        return TryTakePlace(ref passesInSlot, fleetSlots) ? Answer.Starts : Answer.ShareIsFull;
    }

    /// <summary>Whether a refused pass is reported now: it has been refused for more than one <paramref name="interval"/>,
    /// counted from its first refusal (<paramref name="refusedSinceUtc"/>), and the last report is at least one interval
    /// old. The count starts at the first refusal and not at the due time, because a due time keeps ageing while no body
    /// asks (collection paused, analysis turned off), and that time is not time the pass was refused. A last report ahead
    /// of the clock (a clock that stepped back) counts as old.</summary>
    internal static bool WarnsLate(DateTime refusedSinceUtc, DateTime nowUtc, TimeSpan interval, DateTime lastWarnedUtc) =>
        nowUtc - refusedSinceUtc > interval && (lastWarnedUtc > nowUtc || nowUtc - lastWarnedUtc >= interval);

    /// <summary>The longest a due pass gives way to waiting collection bodies: the shortest analysis interval.</summary>
    internal static readonly TimeSpan YieldLimit = TimeSpan.FromMinutes(5);

    /// <summary>The wait for a fleet slot a due pass gives way to: the wait a <c>collection_cycle</c> row names as the
    /// cause of a lost slot (<see cref="DarlingCycleSkip.CauseFloorMs"/>, 30 s), two sweep ticks. The longest wait is read
    /// and not the age of the queue, because a gate whose every tick drains just after the next one launches is never
    /// empty while no body in it waits longer than a tick.</summary>
    internal static readonly TimeSpan WaitFloor = TimeSpan.FromMilliseconds(DarlingCycleSkip.CauseFloorMs);

    /// <summary>How many of <paramref name="fleetSlots"/> scheduled passes may hold at once: half of them, and never fewer
    /// than one, which on a gate one wide is the whole gate.</summary>
    internal static int SlotShare(int fleetSlots) => Math.Max(1, fleetSlots / 2);

    /// <summary>
    /// The longest a due pass is deferred in the ordinary run of things: <see cref="YieldLimit"/> of giving way, and then
    /// one <paramref name="passBudget"/>, within which a pass that holds a place in a full share ends and frees it. The
    /// PLAN_REGRESSION candidate reuse window is one cadence plus this, so the candidates a pass read are still served
    /// while the pass that replaces them is deferred. A pass kept out of the share for longer leaves them to expire.
    /// </summary>
    internal static TimeSpan OrdinaryDeferral(TimeSpan passBudget) => YieldLimit + passBudget;

    /// <summary>Whether a pass due since <paramref name="dueUtc"/> gives way: a collection body has waited
    /// <see cref="WaitFloor"/> or longer for a fleet slot (<paramref name="longestWait"/> is how long the one that has
    /// waited longest has waited, null when none is waiting) and the pass is not yet <see cref="YieldLimit"/> past due.</summary>
    internal static bool GivesWay(TimeSpan? longestWait, DateTime dueUtc, DateTime nowUtc) =>
        longestWait >= WaitFloor && nowUtc - dueUtc < YieldLimit;

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
