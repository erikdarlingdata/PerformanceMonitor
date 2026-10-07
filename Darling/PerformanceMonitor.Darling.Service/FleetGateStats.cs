/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// What the fleet collection gate did over a window (#4732): collector slots that ran, slots that came due and
/// were skipped because the run was late, and how long bodies queued for a gate slot.
/// </summary>
/// <param name="Run">Collector slots that ran.</param>
/// <param name="Skipped">Slots that came due but never ran, because a run landed after the next slot was already
/// due (<see cref="CollectorCadence.SkippedSlots"/>), counted only from the moment the sweep loop was running
/// (<see cref="SkipCreditFloor"/>).</param>
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
/// minute and the 59 before it, so a bucket older than the window drops out on its own. A bucket stamped ahead of
/// the clock (the wall clock stepped backwards) is folded into the current minute.</para>
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
    /// collector's due time advances on the grid, with <see cref="SkipCreditFloor.Skipped"/> for that step (the slots
    /// stepped over that came due while the sweep loop was running); 0 for a run on time.
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

    /// <summary>
    /// #4938: records slots that were skipped and never ran, with no slot that ran beside them. A collector with a run time
    /// that the pass could not hand off inside its hour loses that day's run, and <see cref="RecordSlot"/> would count a run
    /// that did not happen. Pass <see cref="DarlingWorker.StepRunTimeCollector"/>'s count: 1 for a day lost while the
    /// sweep loop was running, 0 after a sleep, a pause or a stopped service, which records nothing.
    /// </summary>
    public void RecordSkippedSlots(long skipped)
    {
        if (skipped <= 0)
        {
            return;
        }

        lock (_lock)
        {
            ref var bucket = ref BucketFor(_utcNow());
            bucket.Skipped += skipped;
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
            FoldBucketsAheadOf(minute);
            long run = 0, skipped = 0, waits = 0, waitTicks = 0, waitMaxTicks = 0;
            foreach (var bucket in _buckets)
            {
                /* No bucket is ahead of the clock here: FoldBucketsAheadOf moved each into the current minute. */
                if (bucket.Minute > minute - WindowMinutes)
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
        FoldBucketsAheadOf(minute);
        ref var bucket = ref _buckets[(int)(minute % WindowMinutes)];
        if (bucket.Minute != minute)
        {
            bucket = new Bucket { Minute = minute };
        }

        return ref bucket;
    }

    /// <summary>
    /// A bucket stamped ahead of the clock can only come from a wall clock that stepped backwards after the bucket was
    /// written (#4732), and what it holds happened in the last hour like everything else. It is folded into the
    /// current minute: leaving it out until the clock caught up would read a thinner window for as long as the step,
    /// and a step of an hour or more would hide the whole window. A bucket dropped instead would lose real counts,
    /// so the alert would judge on less than it saw. Only the buckets still inside the window when the clock stepped
    /// (the newest stamp and the 59 before it) are folded; an older one had already aged out and is dropped. A clock
    /// that has not stepped finds nothing ahead. Runs under the lock, before every write and read.
    /// </summary>
    private void FoldBucketsAheadOf(long minute)
    {
        var newest = minute;
        foreach (var bucket in _buckets)
        {
            newest = Math.Max(newest, bucket.Minute);
        }

        if (newest == minute)
        {
            return;
        }

        var folded = default(Bucket);
        for (var i = 0; i < _buckets.Length; i++)
        {
            ref var ahead = ref _buckets[i];
            if (ahead.Minute <= minute)
            {
                continue;
            }

            if (ahead.Minute > newest - WindowMinutes)
            {
                folded.Add(in ahead);
            }

            ahead = default;
        }

        ref var current = ref _buckets[(int)(minute % WindowMinutes)];
        if (current.Minute != minute)
        {
            current = new Bucket { Minute = minute };
        }

        current.Add(in folded);
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

        /// <summary>Adds another bucket's counts to this one; the minute is left alone.</summary>
        public void Add(in Bucket other)
        {
            Run += other.Run;
            Skipped += other.Skipped;
            QueueWaits += other.QueueWaits;
            QueueWaitTicks += other.QueueWaitTicks;
            QueueWaitMaxTicks = Math.Max(QueueWaitMaxTicks, other.QueueWaitMaxTicks);
        }
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

/// <summary>
/// The instant before which a collector slot that came due is not counted as skipped (#4732). The count behind
/// "Collection Falling Behind" is "slots stepped over by a late run", and it cannot tell a run that was late because
/// the fleet gate was full from a run that was late because nothing was running: a host that slept for an hour, a wall
/// clock that stepped forward an hour, or a service that was paused for an hour leaves every due stamp an hour old, and
/// the first run after it stepped over about sixty slots for every one-minute collector on every server. On 44 servers
/// that is over 50,000 skipped slots against about 1,500 that ran, so the alert fired on the next minute, stood for
/// about two hours, and named <c>max_concurrent_sweeps</c> when nothing had been behind.
///
/// <para>So the sweep loop raises this floor to "now" when it comes back from a stretch it was not running, and the
/// slot record counts skipped slots only from the later of the slot's own due time and the floor
/// (<see cref="Skipped"/>). A slot that came due while the loop kept ticking still counts, so a body that starts
/// late because the gate was full is still seen. The stretches are a gap between two ticks that is over
/// <see cref="MaxTickGap"/>, or negative, measured on the wall clock the due stamps are written on (which is how a
/// sleep, a stall and a clock step of either sign all show up), and a pause, whose ticks keep coming but start no
/// collection (<see cref="Resume"/>).</para>
///
/// <para>Thread-safe: the loop thread calls <see cref="Tick"/> and <see cref="Resume"/>, and the per-server
/// collection bodies read the floor from the thread pool. The clock is a parameter, so the rule is tested with a fixed
/// one.</para>
/// </summary>
internal sealed class SkipCreditFloor
{
    /// <summary>The longest gap between two loop ticks that still counts as the loop running. The loop ticks every 15
    /// seconds, so this is eight ticks in a row missing.</summary>
    internal static readonly TimeSpan MaxTickGap = TimeSpan.FromMinutes(2);

    private long _floorTicks;
    private long _lastTickTicks;
    private bool _ticked;

    /// <summary>The instant before which no slot counts as skipped: <see cref="DateTime.MinValue"/> until the loop
    /// first comes back from a stretch it was not running.</summary>
    public DateTime Floor => new(Interlocked.Read(ref _floorTicks), DateTimeKind.Utc);

    /// <summary>
    /// Called on every pass of the sweep loop, paused or not, before the pass does anything else. Raises the floor to
    /// <paramref name="nowUtc"/> when the time since the previous tick is over <see cref="MaxTickGap"/> or negative.
    /// The floor follows the clock down after a backward step instead of staying ahead of it, because a floor
    /// ahead of the clock would hide every real skip until the clock caught up to it.
    /// </summary>
    public void Tick(DateTime nowUtc)
    {
        var previous = _lastTickTicks;
        var hadPrevious = _ticked;
        _lastTickTicks = nowUtc.Ticks;
        _ticked = true;

        if (!hadPrevious)
        {
            return;
        }

        var gap = nowUtc.Ticks - previous;
        if (gap > MaxTickGap.Ticks || gap < 0)
        {
            Interlocked.Exchange(ref _floorTicks, nowUtc.Ticks);
        }
    }

    /// <summary>Called on the first pass that runs collection after a pause, however short the pause was: the ticks
    /// kept coming through it, so <see cref="Tick"/> saw no gap, but nothing was collected.</summary>
    public void Resume(DateTime nowUtc) => Interlocked.Exchange(ref _floorTicks, nowUtc.Ticks);

    /// <summary>
    /// <see cref="CollectorCadence.SkippedSlots"/> for the slot at <paramref name="due"/> running at
    /// <paramref name="nowUtc"/>, counting only the slots that came due at or after the floor. 0 when the floor is at or
    /// after <paramref name="nowUtc"/>.
    /// </summary>
    public long Skipped(DateTime due, DateTime nowUtc, TimeSpan interval)
    {
        var floor = Floor;
        return CollectorCadence.SkippedSlots(due < floor ? floor : due, nowUtc, interval);
    }
}
