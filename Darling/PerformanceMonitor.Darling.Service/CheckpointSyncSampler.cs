/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The longest single checkpoint sync a <see cref="CheckpointSyncSampler"/> window saw.</summary>
/// <param name="SampledUtc">When the sample that saw it was taken, UTC. The checkpoint had finished by then, within
/// the sampling interval before it.</param>
/// <param name="SyncMs">Milliseconds of sync time that finished since the sample before it.</param>
internal readonly record struct CheckpointSyncMax(DateTime SampledUtc, long SyncMs);

/// <summary>
/// Finds the longest single checkpoint sync inside a window from once-a-minute reads of the checkpointer's
/// CUMULATIVE sync time (#4823). The Store Checkpointer Pressure self-alert judged the hour's average sync per
/// checkpoint, so one checkpoint that synced for 23.5 s among four in the hour (an average of 5.9 s) stayed under
/// the 10 s bar while collection stalled on every server. The hourly rows cannot show it, because they hold
/// cumulative counters; a difference taken every minute can.
///
/// <para><b>Why the sync delta alone, with no checkpoint count beside it.</b> PostgreSQL adds a checkpoint's whole
/// sync time to <c>sync_time</c> when the checkpoint ENDS, while <c>num_timed</c> counts it when it STARTS (and
/// counts skipped ones on 17), so pairing a minute's count with that minute's sync time misattributes. A minute's
/// sync delta is one checkpoint's sync, or the sum of two that ended inside the same minute, which can only
/// over-report.</para>
///
/// <para><b>What a sample does.</b> <see cref="Observe"/> keeps the previous cumulative value. When the new value is
/// at or above it, the difference is the sync time finished since the last sample, and the largest difference (and
/// the time of the sample that saw it) is the window maximum. A value BELOW the previous one is a restart or a
/// statistics reset: it becomes the new baseline and states no difference, never a negative or a clamped zero. The
/// first sample has nothing to subtract from and only sets the baseline. <see cref="TakeWindowMax"/> returns the
/// window maximum and clears it; the baseline stays, so the next window's first difference is against the last
/// sample of this one. A minute in which no sync time finished is a measured difference of zero, so a window of
/// quiet minutes reads as a zero maximum and null means only that no difference was taken.</para>
///
/// <para>One instance is shared by the worker's minute loop (<see cref="Observe"/>) and its hourly evaluation
/// (<see cref="TakeWindowMax"/>), so both are guarded by one lock. In memory only: a service restart starts a new
/// baseline, and the hourly average arm covers the interval the sampler did not see.</para>
/// </summary>
internal sealed class CheckpointSyncSampler
{
    private readonly object _gate = new();
    private long? _previousSyncMs;
    private CheckpointSyncMax? _windowMax;

    /// <summary>Records one read of the cumulative sync time, in milliseconds, taken at <paramref name="sampledUtc"/>.</summary>
    public void Observe(DateTime sampledUtc, long syncTimeMs)
    {
        lock (_gate)
        {
            var previous = _previousSyncMs;
            _previousSyncMs = syncTimeMs;

            /* The first sample, or a counter that fell (a restart or a statistics reset): a new baseline, no difference. */
            if (previous is not long before || syncTimeMs < before)
            {
                return;
            }

            var finished = syncTimeMs - before;
            if (_windowMax is not CheckpointSyncMax held || finished > held.SyncMs)
            {
                _windowMax = new CheckpointSyncMax(sampledUtc, finished);
            }
        }
    }

    /// <summary>Returns the window maximum, or null when no difference was taken in the window, and clears it.</summary>
    public CheckpointSyncMax? TakeWindowMax()
    {
        lock (_gate)
        {
            var taken = _windowMax;
            _windowMax = null;
            return taken;
        }
    }
}
