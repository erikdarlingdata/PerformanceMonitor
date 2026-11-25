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

/// <summary>The longest single checkpoint sync a <see cref="CheckpointSyncSampler"/> window saw.</summary>
/// <param name="SampledUtc">When the sample that saw it was taken, UTC. The checkpoint had finished by then, within
/// the sampling interval before it.</param>
/// <param name="SyncMs">Milliseconds of sync time that finished since the sample before it.</param>
internal readonly record struct CheckpointSyncMax(DateTime SampledUtc, long SyncMs);

/// <summary>
/// One checkpoint sync a <see cref="CheckpointSyncSampler"/> saw finish (#9549): the sync time that finished between two
/// consecutive samples of the cumulative counter. The Store Checkpointer Pressure self-alert asks whether a store read or a
/// collector store write suffered while it ran.
/// </summary>
/// <param name="PreviousSampleUtc">When the sample before it was taken, UTC. The checkpoint ended after this.</param>
/// <param name="SampledUtc">When the sample that saw it was taken, UTC. The checkpoint had ended by this.</param>
/// <param name="SyncMs">Milliseconds of sync time that finished between the two samples.</param>
internal readonly record struct CheckpointSync(DateTime PreviousSampleUtc, DateTime SampledUtc, long SyncMs)
{
    /// <summary>
    /// The earliest the sync can have started: the checkpoint ended somewhere after <see cref="PreviousSampleUtc"/>, so it
    /// started no earlier than that minus its length. The samples are a minute apart and say no more than that, so the
    /// span from here to <see cref="SampledUtc"/> holds the whole sync and up to a sampling interval that was not sync.
    /// </summary>
    public DateTime StartedUtc => PreviousSampleUtc.AddMilliseconds(-SyncMs);

    /// <summary>True when an operation that ran from <paramref name="startedUtc"/> to <paramref name="endedUtc"/> overlaps
    /// the span the sync ran in (both ends inclusive).</summary>
    public bool Overlaps(DateTime startedUtc, DateTime endedUtc) =>
        startedUtc <= SampledUtc && endedUtc >= StartedUtc;
}

/// <summary>
/// Finds each checkpoint sync inside a window, and the longest of them, from once-a-minute reads of the
/// checkpointer's CUMULATIVE sync time (#4823). The hourly rows hold cumulative counters, so they give the hour's
/// total sync time and an average per checkpoint: one checkpoint that synced for 23.5 s among four in the hour reads
/// as an average of 5.9 s, and neither figure says which minute it ran in. A difference taken every minute does, and
/// the Store Checkpointer Pressure self-alert fires when a store read or write suffered in such a minute,
/// inside a sync that ran for at least the read deadline (#9549).
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
/// <para><b>Every sync of the window is kept too (#9549).</b> Each difference above zero is one
/// <see cref="CheckpointSync"/>: the two sample times it finished between and its length. <see cref="TakeWindowMax"/>
/// moves the window's syncs to <see cref="TakenSyncs"/>, where the hourly evaluation reads which minutes a sync ran
/// in, and starts a new list. A window that is not taken keeps its syncs, the way it keeps its maximum. The list holds
/// at most <see cref="MaxWindowSyncs"/>, the newest: a day of minutes, far past the hour a window covers.</para>
///
/// <para>One instance is shared by the worker's minute loop (<see cref="Observe"/>) and its hourly evaluation
/// (<see cref="TakeWindowMax"/>), so both are guarded by one lock. In memory only: a service restart starts a new
/// baseline, and the syncs before it are not seen.</para>
/// </summary>
internal sealed class CheckpointSyncSampler
{
    private readonly object _gate = new();
    /// <summary>The most syncs one window keeps: one per minute for a day.</summary>
    internal const int MaxWindowSyncs = 1440;

    private long? _previousSyncMs;
    private DateTime _previousSampledUtc;
    private CheckpointSyncMax? _windowMax;
    private List<CheckpointSync> _windowSyncs = new();
    private IReadOnlyList<CheckpointSync> _takenSyncs = Array.Empty<CheckpointSync>();

    /// <summary>Records one read of the cumulative sync time, in milliseconds, taken at <paramref name="sampledUtc"/>.</summary>
    public void Observe(DateTime sampledUtc, long syncTimeMs)
    {
        lock (_gate)
        {
            var previous = _previousSyncMs;
            var previousSampledUtc = _previousSampledUtc;
            _previousSyncMs = syncTimeMs;
            _previousSampledUtc = sampledUtc;

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

            /* A minute in which no sync time finished is not a sync. */
            if (finished > 0)
            {
                if (_windowSyncs.Count == MaxWindowSyncs)
                {
                    _windowSyncs.RemoveAt(0);
                }

                _windowSyncs.Add(new CheckpointSync(previousSampledUtc, sampledUtc, finished));
            }
        }
    }

    /// <summary>Returns the window maximum, or null when no difference was taken in the window, and clears it. The
    /// window's syncs move to <see cref="TakenSyncs"/> in the same step, so the two describe one window.</summary>
    public CheckpointSyncMax? TakeWindowMax()
    {
        lock (_gate)
        {
            var taken = _windowMax;
            _windowMax = null;
            _takenSyncs = _windowSyncs;
            _windowSyncs = new List<CheckpointSync>();
            return taken;
        }
    }

    /// <summary>The syncs of the window <see cref="TakeWindowMax"/> last took, oldest first. Empty before the first
    /// take and for a window in which no sync finished.</summary>
    public IReadOnlyList<CheckpointSync> TakenSyncs()
    {
        lock (_gate)
        {
            return _takenSyncs;
        }
    }
}
