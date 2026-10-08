/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
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
///
/// <para>#5597: every count is kept twice, in full and as the "judged" count that leaves out what was recorded in the first
/// <see cref="DarlingSelfAlertEvaluator.FleetGateStartupMinutes"/> minutes after the service started. Which count a record
/// belongs to is decided when it is recorded, on the monotonic uptime, so a wall-clock step in either direction cannot move
/// a start-up count into the judged window or a judged one out of it.</para>
/// </summary>
internal sealed class FleetGateStats
{
    /// <summary>The window every read covers: the current minute and the 59 before it.</summary>
    internal const int WindowMinutes = 60;

    private readonly Func<DateTime> _utcNow;
    private readonly Func<TimeSpan?>? _uptime;
    private readonly object _lock = new();
    private readonly Bucket[] _buckets = new Bucket[WindowMinutes];

    /// <param name="utcNow">The wall clock the minute buckets are stamped on.</param>
    /// <param name="uptime">#5597: how long the service has run, on a monotonic clock (<see cref="SkipCreditFloor.Uptime"/>); null
    /// before the first tick. A slot recorded while it is under <see cref="DarlingSelfAlertEvaluator.FleetGateStartupMinutes"/>
    /// (or null, or no clock given) is left out of the judged counts (<see cref="SnapshotJudged"/>) and stays in the shared ones.</param>
    public FleetGateStats(Func<DateTime> utcNow, Func<TimeSpan?>? uptime = null)
    {
        ArgumentNullException.ThrowIfNull(utcNow);
        _utcNow = utcNow;
        _uptime = uptime;
    }

    /// <summary>
    /// #5597: whether a slot recorded now belongs in the judged counts: the monotonic uptime is at least
    /// <see cref="DarlingSelfAlertEvaluator.FleetGateStartupMinutes"/>. Decided when the slot is recorded, so a wall-clock step
    /// afterwards cannot bring a start-up count into the judged window, or take a judged one out of it.
    /// </summary>
    private bool IsJudgedNow()
    {
        var uptime = _uptime?.Invoke();
        return uptime is not null && uptime.Value >= TimeSpan.FromMinutes(DarlingSelfAlertEvaluator.FleetGateStartupMinutes);
    }

    /// <summary>
    /// Records one collector slot that ran (#4732), and how many slots it stepped over on the way. Called where a
    /// collector's due time advances on the grid, with <see cref="SkipCreditFloor.Skipped"/> for that step (the slots
    /// stepped over that came due while the sweep loop was running); 0 for a run on time.
    /// </summary>
    public void RecordSlot(long skipped)
    {
        var judged = IsJudgedNow();
        lock (_lock)
        {
            ref var bucket = ref BucketFor(_utcNow());
            var counted = Math.Max(0, skipped);
            bucket.Run++;
            bucket.Skipped += counted;
            if (judged)
            {
                bucket.JudgedRun++;
                bucket.JudgedSkipped += counted;
            }
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

        var judged = IsJudgedNow();
        lock (_lock)
        {
            ref var bucket = ref BucketFor(_utcNow());
            bucket.Skipped += skipped;
            if (judged)
            {
                bucket.JudgedSkipped += skipped;
            }
        }
    }

    /// <summary>Records how long one collection body waited for a fleet gate slot.</summary>
    public void RecordQueueWait(TimeSpan wait)
    {
        var ticks = Math.Max(0, wait.Ticks);
        var judged = IsJudgedNow();
        lock (_lock)
        {
            ref var bucket = ref BucketFor(_utcNow());
            bucket.QueueWaits++;
            bucket.QueueWaitTicks += ticks;
            bucket.QueueWaitMaxTicks = Math.Max(bucket.QueueWaitMaxTicks, ticks);
            if (judged)
            {
                bucket.JudgedQueueWaits++;
                bucket.JudgedQueueWaitTicks += ticks;
                bucket.JudgedQueueWaitMaxTicks = Math.Max(bucket.JudgedQueueWaitMaxTicks, ticks);
            }
        }
    }

    /// <summary>The counts over the current minute and the 59 before it.</summary>
    public FleetGateSnapshot Snapshot() => Read(judged: false);

    /// <summary>
    /// The counts over the current minute and the 59 before it, limited to what was recorded once the service had run
    /// <see cref="DarlingSelfAlertEvaluator.FleetGateStartupMinutes"/> minutes (#5597): the slots that ran, the slots skipped and
    /// the queue waits. The "Collection Falling Behind" alert judges this; the hourly log line keeps <see cref="Snapshot"/>. The
    /// window is the same 60 buckets as <see cref="Snapshot"/>, so a wall-clock step moves the same counts either way, and the
    /// start-up counts cannot come back into it.
    /// </summary>
    public FleetGateSnapshot SnapshotJudged() => Read(judged: true);

    private FleetGateSnapshot Read(bool judged)
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
                    run += judged ? bucket.JudgedRun : bucket.Run;
                    skipped += judged ? bucket.JudgedSkipped : bucket.Skipped;
                    waits += judged ? bucket.JudgedQueueWaits : bucket.QueueWaits;
                    waitTicks += judged ? bucket.JudgedQueueWaitTicks : bucket.QueueWaitTicks;
                    waitMaxTicks = Math.Max(waitMaxTicks, judged ? bucket.JudgedQueueWaitMaxTicks : bucket.QueueWaitMaxTicks);
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
        public long JudgedRun;
        public long JudgedSkipped;
        public long QueueWaits;
        public long QueueWaitTicks;
        public long QueueWaitMaxTicks;
        public long JudgedQueueWaits;
        public long JudgedQueueWaitTicks;
        public long JudgedQueueWaitMaxTicks;

        /// <summary>Adds another bucket's counts to this one; the minute is left alone.</summary>
        public void Add(in Bucket other)
        {
            Run += other.Run;
            Skipped += other.Skipped;
            JudgedRun += other.JudgedRun;
            JudgedSkipped += other.JudgedSkipped;
            QueueWaits += other.QueueWaits;
            QueueWaitTicks += other.QueueWaitTicks;
            QueueWaitMaxTicks = Math.Max(QueueWaitMaxTicks, other.QueueWaitMaxTicks);
            JudgedQueueWaits += other.JudgedQueueWaits;
            JudgedQueueWaitTicks += other.JudgedQueueWaitTicks;
            JudgedQueueWaitMaxTicks = Math.Max(JudgedQueueWaitMaxTicks, other.JudgedQueueWaitMaxTicks);
        }
    }
}

/// <summary>The one-line summary the worker logs (#4732): the last hour's counts and the gate's width.</summary>
internal static class FleetGateLine
{
    /// <summary>
    /// #5597: how many minutes the counts of the line cover: the hour, or the minutes the service has run
    /// (<see cref="SkipCreditFloor.Uptime"/>, a monotonic clock, so a wall-clock step cannot make it "1 minute") while that is
    /// under an hour (rounded up to a whole minute, at least 1). The first line after a start came 4.5 to 6.9 minutes in and
    /// still said "last 60 minutes". 60 when the loop has not ticked yet.
    /// </summary>
    internal static int SpanMinutes(TimeSpan? uptime)
    {
        if (uptime is not { } ran)
        {
            return FleetGateStats.WindowMinutes;
        }

        var whole = (ran.Ticks + TimeSpan.TicksPerMinute - 1) / TimeSpan.TicksPerMinute;
        return (int)Math.Clamp(whole, 1, FleetGateStats.WindowMinutes);
    }

    internal static string Describe(FleetGateSnapshot snapshot, int gateWidth, int spanMinutes = FleetGateStats.WindowMinutes) => string.Create(
        CultureInfo.InvariantCulture,
        $"fleet collection gate, last {spanMinutes} minutes{(spanMinutes < FleetGateStats.WindowMinutes ? ", since the service started" : string.Empty)}: {snapshot.Run:N0} collector slots ran, {snapshot.Skipped:N0} skipped ({snapshot.SkippedPercent:0.#}%); {snapshot.QueueWaits:N0} bodies queued for a gate slot (average {snapshot.QueueWaitAverage.TotalMilliseconds:N0} ms, longest {snapshot.QueueWaitMax.TotalMilliseconds:N0} ms); gate width {gateWidth}");
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

    private static readonly long s_processOrigin = Stopwatch.GetTimestamp();

    private readonly Func<TimeSpan> _monotonic;
    private long _floorTicks;
    private long _lastTickTicks;
    private long _firstMonotonicTicks = -1;
    private bool _ticked;

    /// <summary>The floor reads time on the process's monotonic clock (a stopwatch), which a wall-clock step cannot move.</summary>
    public SkipCreditFloor() : this(static () => Stopwatch.GetElapsedTime(s_processOrigin))
    {
    }

    /// <summary>A floor with a monotonic clock of the caller's own, so a test moves the two clocks apart.</summary>
    internal SkipCreditFloor(Func<TimeSpan> monotonic) => _monotonic = monotonic;

    /// <summary>The instant before which no slot counts as skipped: <see cref="DateTime.MinValue"/> until the loop
    /// first comes back from a stretch it was not running.</summary>
    public DateTime Floor => new(Interlocked.Read(ref _floorTicks), DateTimeKind.Utc);

    /// <summary>
    /// Called on every pass of the sweep loop, paused or not, before the pass does anything else. Raises the floor to
    /// <paramref name="nowUtc"/> when the time since the previous tick is over <see cref="MaxTickGap"/> or negative.
    /// The floor follows the clock down after a backward step instead of staying ahead of it, because a floor
    /// ahead of the clock would hide every real skip until the clock caught up to it. The first call also stamps the
    /// service start on the monotonic clock (<see cref="Uptime"/>).
    /// </summary>
    public void Tick(DateTime nowUtc)
    {
        var previous = _lastTickTicks;
        var hadPrevious = _ticked;
        _lastTickTicks = nowUtc.Ticks;
        _ticked = true;

        if (!hadPrevious)
        {
            Interlocked.Exchange(ref _firstMonotonicTicks, _monotonic().Ticks);
            return;
        }

        var gap = nowUtc.Ticks - previous;
        if (gap > MaxTickGap.Ticks || gap < 0)
        {
            Interlocked.Exchange(ref _floorTicks, nowUtc.Ticks);
        }
    }

    /// <summary>
    /// #5597: how long the sweep loop has run since its first tick (the service start, as far as collection goes), on a
    /// monotonic clock; null before the first tick. A wall clock that steps back cannot lengthen it, so the start-up
    /// minutes the alert leaves out are never more than the real ones. Only a service START gets those minutes: a floor
    /// raise (a stall over <see cref="MaxTickGap"/>, a clock step), the end of a pause and the memory launch guard's release do
    /// not start them again, because a store that stalls or is held off every 20 minutes is behind, and the alert has to see it.
    /// </summary>
    public TimeSpan? Uptime
    {
        get
        {
            var first = Interlocked.Read(ref _firstMonotonicTicks);
            return first < 0 ? null : TimeSpan.FromTicks(Math.Max(0, _monotonic().Ticks - first));
        }
    }

    /// <summary>
    /// #5597: a per-server seed stamp (the wall-clock instant a connect body finished seeding) that is ahead of
    /// <paramref name="nowUtc"/> because the clock stepped back after it was written, brought down to now. Counting from an
    /// instant that has not happened leaves no slot due, so every slot of every server that connected inside the step went
    /// uncounted until the clock caught up to the stamp.
    /// </summary>
    internal static long ClampSeedStamp(long stampTicks, DateTime nowUtc) => Math.Min(stampTicks, nowUtc.Ticks);

    /// <summary>The same clamp on the stored stamp: the stamp itself moves down, so the later passes count from it.</summary>
    internal static long ClampSeedStamp(ref long stampTicks, DateTime nowUtc)
    {
        var stamp = Interlocked.Read(ref stampTicks);
        var clamped = ClampSeedStamp(stamp, nowUtc);
        if (clamped != stamp)
        {
            Interlocked.CompareExchange(ref stampTicks, clamped, stamp);
        }

        return clamped;
    }

    /// <summary>Called on the first pass that runs collection after a pause, however short the pause was: the ticks
    /// kept coming through it, so <see cref="Tick"/> saw no gap, but nothing was collected.</summary>
    public void Resume(DateTime nowUtc) => Interlocked.Exchange(ref _floorTicks, nowUtc.Ticks);

    /// <summary>
    /// <see cref="CollectorCadence.SkippedSlots"/> for the slot at <paramref name="due"/> running at
    /// <paramref name="nowUtc"/>, counting only the slots that came due at or after the floor. 0 when the floor is at or
    /// after <paramref name="nowUtc"/>.
    /// </summary>
    public long Skipped(DateTime due, DateTime nowUtc, TimeSpan interval) => Skipped(due, nowUtc, interval, default);

    /// <summary>
    /// #5597: the same count with a floor of the caller's own. A server's connect body seeds its collectors' first due
    /// stamps from a clock read BEFORE it runs its on-load snapshots, and that body is the server's only body: the slots
    /// that came due while it ran could not have run, so the pass counts from the instant the seeding finished
    /// (<paramref name="notBefore"/>) when that is later than <see cref="Floor"/>.
    /// </summary>
    public long Skipped(DateTime due, DateTime nowUtc, TimeSpan interval, DateTime notBefore)
    {
        var floor = Floor;

        if (notBefore > floor)
        {
            floor = notBefore;
        }

        return CollectorCadence.SkippedSlots(due < floor ? floor : due, nowUtc, interval);
    }
}

/// <summary>
/// What a collector slot's watermark remembers while the memory launch guard holds collection off (#5479): the due stamp
/// the count was made against, the interval it was counted at, how many lost slots were already counted for it, and the
/// earliest slot the count may reach back to. An interval change mid-hold restarts the mark at the change (the due stamp
/// keeps its place, so without that the new, shorter interval would count every slot of the hold so far at once). Written
/// only by the sweep loop thread.
/// </summary>
internal readonly record struct HeldSlotMark(DateTime Due, long Counted, TimeSpan Interval = default, DateTime Floor = default);

/// <summary>
/// #5479: the skipped-slot count while the launch guard holds collection off. A held pass launches no body, so no body
/// records a slot, and the hour's count read "0 ran, 0 skipped" and cleared "Collection Falling Behind" an hour into an
/// outage; when the guard released, the first bodies stepped over every slot at once (101,774 of 101,881).
///
/// <para>A held slot is a skipped slot, so each slot that comes due is counted once, within a pass of its due time. The
/// due stamp does not move during a hold, so the total lost since the stamp is a pure function of the clock
/// (<see cref="Lost"/>) and a <see cref="HeldSlotMark"/> keeps what was already counted, so no slot is counted twice
/// (<see cref="Newly"/>). For a collector on the interval grid a slot is lost once the NEXT slot has come due, which is
/// exactly the count <see cref="CollectorCadence.SkippedSlots"/> gives when a run finally steps over it: the held count
/// is that number spread over the hold. The slot at the stamp itself is the one the first run after the hold serves, so
/// it is counted as a run then and never as skipped here. A collector with a run time has no next slot to wait for: its
/// day is lost once the grace after its stamp has passed.</para>
///
/// <para>When the guard releases, the loop calls <see cref="SkipCreditFloor.Resume"/>, so the first bodies count none of
/// what this counted.</para>
/// </summary>
internal static class HeldSlots
{
    /// <summary>
    /// How many slots of one collector are lost by <paramref name="nowUtc"/>, for a due stamp that has not moved since
    /// <paramref name="due"/>: the slots at <c>due + k * interval</c> that are not before <paramref name="floor"/> (a slot
    /// that came due while the loop was not running is not the guard's doing) and whose <paramref name="serveWindow"/> has
    /// passed. Pass the interval as the window for a collector on the grid, the run-time grace for one with a run time.
    /// </summary>
    public static long Lost(DateTime due, DateTime nowUtc, DateTime floor, TimeSpan interval, TimeSpan serveWindow)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);

        var first = due;
        if (due < floor)
        {
            first = due + TimeSpan.FromTicks(interval.Ticks * (long)Math.Ceiling((floor - due).Ticks / (double)interval.Ticks));
        }

        var lostAt = first + serveWindow;
        if (nowUtc < lostAt)
        {
            return 0;
        }

        return (long)Math.Floor((nowUtc - lostAt).Ticks / (double)interval.Ticks) + 1;
    }

    /// <summary>
    /// The slots to add to the skipped count on this pass: the lost total less what the mark already holds. The mark is
    /// moved to the new total, also when the total fell (the floor was raised by a sleep or a pause), so a slot is never
    /// counted twice and the count starts again from the raised floor. A mark made for another due stamp is dropped: the
    /// stamp moved because a body ran, and that body counted its own slots. A mark made for another interval is restarted at
    /// <paramref name="nowUtc"/>: the slots before the change were counted (or not) at the old interval, and counting them
    /// again at the new one would invent them (60 minutes to 1 minute during a 2 hour hold made 119 slots at once).
    /// </summary>
    public static long Newly(
        DateTime due, DateTime nowUtc, DateTime floor, TimeSpan interval, TimeSpan serveWindow, ref HeldSlotMark mark)
    {
        long counted;
        DateTime markFloor;
        if (mark.Due != due)
        {
            counted = 0;
            markFloor = DateTime.MinValue;
        }
        else if (mark.Interval != interval)
        {
            counted = 0;
            markFloor = nowUtc;
        }
        else
        {
            counted = mark.Counted;
            markFloor = mark.Floor;
        }

        var total = Lost(due, nowUtc, floor > markFloor ? floor : markFloor, interval, serveWindow);
        mark = new HeldSlotMark(due, total, interval, markFloor);
        return Math.Max(0, total - counted);
    }
}
