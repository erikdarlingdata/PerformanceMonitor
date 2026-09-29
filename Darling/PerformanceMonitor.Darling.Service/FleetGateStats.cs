/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What the fleet collection gate did over a window (#4732): collector slots that ran, slots that came due and
/// were skipped because the run was late, and how long bodies queued for a gate slot.
/// </summary>
/// <param name="Run">Collector slots that ran.</param>
/// <param name="Skipped">Slots that came due but never ran, because a run landed after the next slot was already
/// due (<see cref="CollectorCadence.SkippedSlots"/>).</param>
/// <param name="QueueWaits">Collection bodies that waited for, and got, a fleet gate slot.</param>
/// <param name="QueueWaitTotal">The sum of those waits.</param>
/// <param name="QueueWaitMax">The longest single wait.</param>
internal readonly record struct FleetGateSnapshot(
    long Run,
    long Skipped,
    long QueueWaits,
    TimeSpan QueueWaitTotal,
    TimeSpan QueueWaitMax)
{
    /// <summary>Slots that came due: the ones that ran plus the ones skipped.</summary>
    public long Due => Run + Skipped;

    /// <summary>The share of due slots that were skipped, as a percentage; 0 when nothing came due.</summary>
    public double SkippedPercent => Due == 0 ? 0 : 100.0 * Skipped / Due;

    /// <summary>The mean queue wait; zero when no body waited.</summary>
    public TimeSpan QueueWaitAverage => QueueWaits == 0 ? TimeSpan.Zero : TimeSpan.FromTicks(QueueWaitTotal.Ticks / QueueWaits);
}

/// <summary>
/// One hour of fleet-gate health, in per-minute buckets (#4732). The worker's gate could fall behind with nobody
/// seeing it: a collector slot that came due while the run was late was skipped with one Info line, and how long a
/// body queued for a gate slot was never measured, so capacity at hundreds of servers was worked out by
/// arithmetic. This keeps the counts the "Collection Falling Behind" self-alert and the hourly log line are
/// judged on.
///
/// <para>Thread-safe: collection bodies record from the thread pool while the worker's tick reads. One instance
/// per worker. The clock is injected so the bucket math is tested without waiting. A bucket belongs to one
/// wall-clock minute, is reset when its slot is reused for a later minute, and a read only counts the current
/// minute and the 59 before it, so a bucket older than the window drops out on its own.</para>
/// </summary>
internal sealed class FleetGateStats
{
    /// <summary>The window every read covers: the current minute and the 59 before it.</summary>
    internal const int WindowMinutes = 60;

    private readonly Func<DateTime> _utcNow;
    private readonly object _lock = new();
    private readonly Bucket[] _buckets = new Bucket[WindowMinutes];

    public FleetGateStats(Func<DateTime> utcNow)
    {
        ArgumentNullException.ThrowIfNull(utcNow);
        _utcNow = utcNow;
    }

    /// <summary>
    /// Records one collector slot that ran (#4732), and how many slots it stepped over on the way. Called where a
    /// collector's due time advances on the grid, with <see cref="CollectorCadence.SkippedSlots"/> for that step;
    /// 0 for a run on time.
    /// </summary>
    public void RecordSlot(long skipped)
    {
        lock (_lock)
        {
            ref var bucket = ref BucketFor(_utcNow());
            bucket.Run++;
            bucket.Skipped += Math.Max(0, skipped);
        }
    }

    /// <summary>Records how long one collection body waited for a fleet gate slot.</summary>
    public void RecordQueueWait(TimeSpan wait)
    {
        var ticks = Math.Max(0, wait.Ticks);
        lock (_lock)
        {
            ref var bucket = ref BucketFor(_utcNow());
            bucket.QueueWaits++;
            bucket.QueueWaitTicks += ticks;
            bucket.QueueWaitMaxTicks = Math.Max(bucket.QueueWaitMaxTicks, ticks);
        }
    }

    /// <summary>The counts over the current minute and the 59 before it.</summary>
    public FleetGateSnapshot Snapshot()
    {
        lock (_lock)
        {
            var minute = MinuteOf(_utcNow());
            long run = 0, skipped = 0, waits = 0, waitTicks = 0, waitMaxTicks = 0;
            foreach (var bucket in _buckets)
            {
                if (bucket.Minute > minute - WindowMinutes && bucket.Minute <= minute)
                {
                    run += bucket.Run;
                    skipped += bucket.Skipped;
                    waits += bucket.QueueWaits;
                    waitTicks += bucket.QueueWaitTicks;
                    waitMaxTicks = Math.Max(waitMaxTicks, bucket.QueueWaitMaxTicks);
                }
            }

            return new FleetGateSnapshot(run, skipped, waits, TimeSpan.FromTicks(waitTicks), TimeSpan.FromTicks(waitMaxTicks));
        }
    }

    private ref Bucket BucketFor(DateTime now)
    {
        var minute = MinuteOf(now);
        ref var bucket = ref _buckets[(int)(minute % WindowMinutes)];
        if (bucket.Minute != minute)
        {
            bucket = new Bucket { Minute = minute };
        }

        return ref bucket;
    }

    private static long MinuteOf(DateTime utc) => utc.Ticks / TimeSpan.TicksPerMinute;

    private struct Bucket
    {
        public long Minute;
        public long Run;
        public long Skipped;
        public long QueueWaits;
        public long QueueWaitTicks;
        public long QueueWaitMaxTicks;
    }
}

/// <summary>The one-line summary the worker logs (#4732): the last hour's counts and the gate's width.</summary>
internal static class FleetGateLine
{
    internal static string Describe(FleetGateSnapshot snapshot, int gateWidth) => string.Create(
        CultureInfo.InvariantCulture,
        $"fleet collection gate, last {FleetGateStats.WindowMinutes} minutes: {snapshot.Run:N0} collector slots ran, {snapshot.Skipped:N0} skipped ({snapshot.SkippedPercent:0.#}%); {snapshot.QueueWaits:N0} bodies queued for a gate slot (average {snapshot.QueueWaitAverage.TotalMilliseconds:N0} ms, longest {snapshot.QueueWaitMax.TotalMilliseconds:N0} ms); gate width {gateWidth}");
}

/// <summary>
/// When the worker writes its fleet-gate log line (#4732): once an hour, and at once when the behind/not-behind
/// state changes. The first call only arms the hourly clock, so a service that has just started does not log a
/// line about a minute of data.
/// </summary>
internal sealed class FleetGateLogCadence
{
    internal static readonly TimeSpan Interval = TimeSpan.FromHours(1);

    private bool _lastBehind;
    private DateTime _next = DateTime.MinValue;

    /// <summary>True when the caller should log now. A due time more than one interval ahead (a wall clock that
    /// stepped back) counts as due, the same clamp the collector schedule uses.</summary>
    public bool ShouldLog(bool behind, DateTime nowUtc)
    {
        if (_next == DateTime.MinValue)
        {
            _next = nowUtc + Interval;
        }

        if (behind == _lastBehind && nowUtc < CollectorCadence.ClampDue(_next, nowUtc, Interval))
        {
            return false;
        }

        _lastBehind = behind;
        _next = nowUtc + Interval;
        return true;
    }
}
