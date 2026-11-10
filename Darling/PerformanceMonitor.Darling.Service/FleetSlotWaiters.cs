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
/// #8539: the collection bodies waiting for a fleet collection slot, each with the time it started waiting. A body enters
/// before it waits on the fleet gate and leaves when the wait ends, with a slot or on a cancel. A due scheduled analysis
/// pass reads how long the body that has waited longest has waited (<see cref="ScheduledAnalysisAdmission"/>).
/// </summary>
internal sealed class FleetSlotWaiters
{
    private readonly object _lock = new();
    private readonly LinkedList<DateTime> _waitingSince = new();

    /// <summary>How many bodies are waiting.</summary>
    internal int Count
    {
        get
        {
            lock (_lock)
            {
                return _waitingSince.Count;
            }
        }
    }

    /// <summary>A body starts waiting at <paramref name="nowUtc"/>. The answer is what it hands to <see cref="Leave"/>.</summary>
    internal LinkedListNode<DateTime> Enter(DateTime nowUtc)
    {
        lock (_lock)
        {
            return _waitingSince.AddLast(nowUtc);
        }
    }

    /// <summary>The body's wait is over.</summary>
    internal void Leave(LinkedListNode<DateTime> waiter)
    {
        lock (_lock)
        {
            _waitingSince.Remove(waiter);
        }
    }

    /// <summary>
    /// How long the body that has waited longest has waited at <paramref name="nowUtc"/>, or null when no body is waiting.
    /// A body that started waiting after <paramref name="nowUtc"/> (a clock that stepped back) has waited no time.
    /// </summary>
    internal TimeSpan? LongestWait(DateTime nowUtc)
    {
        lock (_lock)
        {
            if (_waitingSince.Count == 0)
            {
                return null;
            }

            var oldest = DateTime.MaxValue;
            foreach (var since in _waitingSince)
            {
                if (since < oldest)
                {
                    oldest = since;
                }
            }

            return oldest < nowUtc ? nowUtc - oldest : TimeSpan.Zero;
        }
    }
}
