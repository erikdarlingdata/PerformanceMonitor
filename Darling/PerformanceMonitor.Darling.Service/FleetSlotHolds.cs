/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9192: the slowest collector run of one collection body that held a fleet collection slot. <see cref="TotalMs"/> is the run's
/// length as the collection log measures it (monitored-server read plus store write) when the run reported its split, and the
/// wall clock of the run when it did not. <see cref="ReadMs"/> and <see cref="StoreMs"/> are null when the run did not report a
/// split (a run that faulted): the row then names the collector and says nothing about the phases.
/// </summary>
internal readonly record struct SlotHold(string Collector, long TotalMs, long? ReadMs, long? StoreMs);

/// <summary>
/// #9192: what held the fleet collection slots while one body waited for a slot. The collector that ran longest in the bodies
/// that held slots meanwhile, how many of those bodies it was the slowest collector in, and how many bodies there were. The
/// split is the longest run's own.
/// </summary>
internal sealed record SlotHolder(string Collector, long TotalMs, long? ReadMs, long? StoreMs, int BodiesSlowestIn, int Bodies);

/// <summary>
/// #9192: the last bodies that held a fleet collection slot, each with its slowest collector run. A body records its hold when
/// its collectors end, inside the slot. A body that waits for a slot takes a mark before it waits and asks for the holders since
/// that mark when it gets the slot, so the answer is the bodies that finished a collector sweep during its wait. The ledger keeps
/// the last <see cref="Capacity"/> holds, so it stays small however long a wait lasts.
/// </summary>
internal sealed class FleetSlotHolds
{
    internal const int Capacity = 128;

    private readonly object _lock = new();
    private readonly Queue<(long Seq, SlotHold Hold)> _holds = new();
    private long _seq;

    /// <summary>The position a body that is about to wait for a slot passes to <see cref="HolderSince"/>.</summary>
    internal long Mark()
    {
        lock (_lock)
        {
            return _seq;
        }
    }

    /// <summary>A body that held a slot ends its collector sweep with <paramref name="hold"/> as its slowest run.</summary>
    internal void Record(SlotHold hold)
    {
        lock (_lock)
        {
            _holds.Enqueue((++_seq, hold));
            while (_holds.Count > Capacity)
            {
                _holds.Dequeue();
            }
        }
    }

    /// <summary>
    /// What held the slots since <paramref name="mark"/>, or null when the wait was under
    /// <see cref="DarlingCycleSkip.CauseFloorMs"/> (the row names no cause for a short wait) or no body recorded a hold in it.
    /// The longest run wins; a tie goes to the later body.
    /// </summary>
    internal SlotHolder? HolderSince(long mark, long waitedMs)
    {
        if (waitedMs < DarlingCycleSkip.CauseFloorMs)
        {
            return null;
        }

        lock (_lock)
        {
            SlotHold? longest = null;
            var bodies = 0;
            foreach (var (seq, hold) in _holds)
            {
                if (seq <= mark)
                {
                    continue;
                }

                bodies++;
                if (longest is null || hold.TotalMs >= longest.Value.TotalMs)
                {
                    longest = hold;
                }
            }

            if (longest is not { } top)
            {
                return null;
            }

            var slowestIn = 0;
            foreach (var (seq, hold) in _holds)
            {
                if (seq > mark && string.Equals(hold.Collector, top.Collector, StringComparison.OrdinalIgnoreCase))
                {
                    slowestIn++;
                }
            }

            return new SlotHolder(top.Collector, top.TotalMs, top.ReadMs, top.StoreMs, slowestIn, bodies);
        }
    }
}
