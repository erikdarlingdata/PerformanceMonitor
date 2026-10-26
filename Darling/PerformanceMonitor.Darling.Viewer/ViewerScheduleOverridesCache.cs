/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One server tab's copy of the collector schedule rows, read at most once every five minutes. The time-range picker's
/// "collected every N minutes" note and the panel notes that say how often a collector runs all resolve a cadence from
/// these rows (<see cref="ViewerTimeRangeWindow.SampleIntervalFor"/>), and the tab refreshes every 30 seconds to 5 minutes,
/// so one read is shared instead of one per note. Callers that ask while a read is under way share it. A read that fails
/// is not kept: the callers get null (the shipped cadence) and the next call tries again. Used from the UI thread.
/// </summary>
internal sealed class ViewerScheduleOverridesCache
{
    internal static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(5);

    private readonly Func<Task<IReadOnlyList<CollectorScheduleRow>>> _read;
    private readonly Func<DateTime> _utcNow;
    private readonly Action<Exception> _onFailure;
    private Task<IReadOnlyList<CollectorScheduleRow>>? _pending;
    private DateTime _readUtc;

    internal ViewerScheduleOverridesCache(Func<Task<IReadOnlyList<CollectorScheduleRow>>> read, Func<DateTime> utcNow, Action<Exception> onFailure)
    {
        _read = read;
        _utcNow = utcNow;
        _onFailure = onFailure;
    }

    /// <summary>The schedule rows, or null when they could not be read (the caller then keeps the shipped cadence).</summary>
    internal async Task<IReadOnlyList<CollectorScheduleRow>?> GetAsync()
    {
        if (_pending is null || _utcNow() - _readUtc > MaxAge)
        {
            _pending = _read();
            _readUtc = _utcNow();
        }

        var pending = _pending;
        try
        {
            return await pending;
        }
        catch (Exception ex)
        {
            /* Every caller that shared this read lands here. The first one to resume clears it and reports the failure; the
               rest find it already cleared (or replaced by a newer read) and only return null, so one failed read is one
               report, not one per waiting panel (#6766). */
            if (ReferenceEquals(_pending, pending))
            {
                _pending = null;
                _onFailure(ex);
            }

            return null;
        }
    }
}
