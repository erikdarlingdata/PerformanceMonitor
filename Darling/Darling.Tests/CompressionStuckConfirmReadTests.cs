/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3575: the compression-stuck check's <c>-infinity</c> arm is a NON-ATOMIC read of TimescaleDB's own view,
/// and the fix is to read it twice.
///
/// <para><b>The defect these pin against.</b> <see cref="TimescaleSupport.IsPolicyJobStuck"/> already
/// guarded its dead-job arm with <c>!isRunning</c>, and a production store paged through the guard anyway —
/// the alert's stamp 53 ms inside a 63 ms scheduled run that succeeded. The 2.28.1 view definition explains
/// it: <c>job_status</c> is <c>CASE WHEN pgs.state = 'active' THEN 'Running' … END</c> over a
/// <c>LEFT JOIN pg_stat_activity</c> on <c>application_name</c>, and <c>next_start</c> is the
/// <c>bgw_job_stat</c> row. The scheduler commits <c>-infinity</c> before the worker exists; the worker is
/// gone before its <c>mark_end</c> is visible to a snapshot taken a moment earlier. A tight poll of
/// <see cref="TimescaleSupport.StuckPolicyJobsSql"/> across a 10-second-cadence policy on a PG18 +
/// TimescaleDB 2.28.1 rig caught <c>-infinity + Scheduled</c> at BOTH edges of every one of seven runs.
/// The predicate stays pure and single-shot; <see cref="ReadStuckAsync(NpgsqlConnection, DateTime, ILogger, CancellationToken)"/>
/// re-reads after <see cref="TimescaleSupport.StuckPolicyJobConfirmDelay"/> and reports only what
/// persists. These tests script the two reads through the internal seam, so an edge and a dead row are
/// each a pair of result sets and nothing here sleeps.</para>
///
/// <para><b>What is NOT pinned here, deliberately.</b> The stuck-Running arm's six-hour bound and the
/// evaluator's re-arm-once/escalate machine are unchanged by #3575 and keep their own pins
/// (<c>TimescaleSupportTests</c>, <c>DarlingSelfAlertTests</c>). The only claims this file makes are about the
/// second read: when it is taken, what it ratifies, what it clears, and what a failed one does.</para>
/// </summary>
public sealed class CompressionStuckConfirmReadTests
{
    private static readonly DateTime s_now = new(2026, 9, 18, 8, 47, 0, DateTimeKind.Utc);

    /* The three row shapes the view can hand the predicate for one job, named for what they are. */

    /// <summary>The dead-job shape and the run-instant edge: identical on one read — that is the defect.</summary>
    private static PolicyJobStatRow NegInfinityScheduled(long jobId, string hypertable = "file_io_stats") =>
        new(jobId, NextStartIsNegativeInfinity: true, JobStatus: "Scheduled",
            LastRunStartedAtUtc: s_now.AddHours(-1), ScheduleInterval: TimeSpan.FromHours(1), HypertableName: hypertable);

    /// <summary>A healthy job between runs: finite next_start, not running.</summary>
    private static PolicyJobStatRow Healthy(long jobId, string hypertable = "file_io_stats") =>
        new(jobId, NextStartIsNegativeInfinity: false, JobStatus: "Scheduled",
            LastRunStartedAtUtc: s_now.AddMinutes(-1), ScheduleInterval: TimeSpan.FromHours(1), HypertableName: hypertable);

    /// <summary>The mid-run marker: -infinity WITH Running. Belongs to the elapsed arm, never the dead-job arm.</summary>
    private static PolicyJobStatRow MidRun(long jobId, string hypertable = "file_io_stats") =>
        new(jobId, NextStartIsNegativeInfinity: true, JobStatus: "Running",
            LastRunStartedAtUtc: s_now.AddMilliseconds(-40), ScheduleInterval: TimeSpan.FromHours(1), HypertableName: hypertable);

    /// <summary>A hung run: Running since eight hours ago against the six-hour floor.</summary>
    private static PolicyJobStatRow HungRun(long jobId, string hypertable = "query_stats") =>
        new(jobId, NextStartIsNegativeInfinity: true, JobStatus: "Running",
            LastRunStartedAtUtc: s_now.AddHours(-8), ScheduleInterval: TimeSpan.FromHours(1), HypertableName: hypertable);

    /// <summary>
    /// A scripted read: hands back each result set in turn, records how many times it was asked, and throws
    /// the scripted exception in place of a result set when one is planted.
    /// </summary>
    private sealed class ScriptedReads
    {
        private readonly Queue<object> _script = new();
        public int Calls { get; private set; }

        public ScriptedReads Then(params PolicyJobStatRow[] rows)
        {
            _script.Enqueue((IReadOnlyList<PolicyJobStatRow>)rows);
            return this;
        }

        public ScriptedReads ThenThrow(Exception ex)
        {
            _script.Enqueue(ex);
            return this;
        }

        public Task<IReadOnlyList<PolicyJobStatRow>> Read(CancellationToken ct)
        {
            Calls++;
            Assert.True(_script.Count > 0, $"the read was asked a {Calls}th time with nothing scripted for it");
            var next = _script.Dequeue();
            return next is Exception ex
                ? Task.FromException<IReadOnlyList<PolicyJobStatRow>>(ex)
                : Task.FromResult((IReadOnlyList<PolicyJobStatRow>)next);
        }
    }

    /// <summary>A recording delay: never sleeps, remembers every span it was asked to wait.</summary>
    private sealed class RecordedDelay
    {
        public List<TimeSpan> Waits { get; } = new();

        public Task Wait(TimeSpan span, CancellationToken ct)
        {
            Waits.Add(span);
            return Task.CompletedTask;
        }
    }

    /// <summary>
    /// #3816: the read returns the whole pass now — the stuck list AND the census of every job it looked at
    /// — because the evaluator's summary line and its <c>total_failures</c> arm are both about the jobs that
    /// are fine. Every pin in THIS file is a claim about the confirm read's effect on the STUCK list, so they
    /// all go through this projection rather than restating <c>.Stuck</c> twenty times.
    /// </summary>
    private static async Task<IReadOnlyList<StuckPolicyJob>> ReadStuckAsync(
        Func<CancellationToken, Task<IReadOnlyList<PolicyJobStatRow>>> readRows,
        Func<TimeSpan, CancellationToken, Task> delay,
        DateTime nowUtc,
        ILogger? logger,
        CancellationToken cancellationToken,
        Func<CancellationToken, Task<Version?>>? readVersion = null) =>
        (await TimescaleSupport.ReadStuckPolicyJobsAsync(
            readRows, delay, nowUtc, logger, cancellationToken, readVersion)).Stuck;

    /* ---------------- the arm projection ---------------- */

    [Fact]
    public void ClassifyCompressionJob_NamesTheArm_AndIsCompressionJobStuck_IsItsProjection()
    {
        /* The boolean the existing pins hold is the classifier's projection, so the two cannot disagree — the
           reason the classifier is the implementation and not a sibling copy of the same branches. */
        foreach (var (row, expected) in new (PolicyJobStatRow Row, StuckPolicyJobArm Arm)[]
        {
            (NegInfinityScheduled(1), StuckPolicyJobArm.NextStartNegativeInfinity),
            (Healthy(2), StuckPolicyJobArm.None),
            (MidRun(3), StuckPolicyJobArm.None),
            (HungRun(4), StuckPolicyJobArm.RunningPastBound),
        })
        {
            var arm = TimescaleSupport.ClassifyPolicyJob(
                row.NextStartIsNegativeInfinity, row.JobStatus, row.LastRunStartedAtUtc, row.ScheduleInterval, s_now, out var reason);
            var stuck = TimescaleSupport.IsPolicyJobStuck(
                row.NextStartIsNegativeInfinity, row.JobStatus, row.LastRunStartedAtUtc, row.ScheduleInterval, s_now, out var boolReason);

            Assert.Equal(expected, arm);
            Assert.Equal(arm != StuckPolicyJobArm.None, stuck);
            Assert.Equal(reason, boolReason);
        }
    }

    /* ---------------- the confirm-read: when the second read is taken ---------------- */

    [Fact]
    public async Task HealthyPass_ReadsOnce_AndNeverWaits()
    {
        /* The common hourly pass: nothing on the racing arm, so the pass costs exactly what it did before
           #3575 — one read, no delay. A confirm taken unconditionally would hold the serial sweep loop five
           seconds every hour for nothing. */
        var reads = new ScriptedReads().Then(Healthy(1), Healthy(2), MidRun(3));
        var delay = new RecordedDelay();

        var result = await ReadStuckAsync(
            reads.Read, delay.Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        Assert.Empty(result);
        Assert.Equal(1, reads.Calls);
        Assert.Empty(delay.Waits);
    }

    [Fact]
    public async Task HungRunOnly_ReportsFromTheFirstRead_AndNeverWaits()
    {
        /* The stuck-Running arm is judged on hours of elapsed time; a second look five seconds later could not
           change it, so it neither triggers the confirm nor waits on one. */
        var reads = new ScriptedReads().Then(HungRun(4), Healthy(1));
        var delay = new RecordedDelay();

        var result = await ReadStuckAsync(
            reads.Read, delay.Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        var job = Assert.Single(result);
        Assert.Equal(4L, job.JobId);
        Assert.Contains("Running", job.Reason, StringComparison.Ordinal);
        Assert.Equal(1, reads.Calls);
        Assert.Empty(delay.Waits);
    }

    [Fact]
    public async Task NegInfinityTrip_WaitsTheConfirmDelay_ThenReadsAgain_Once()
    {
        /* One confirm per pass, not per job: two jobs on the racing arm still cost one delay and one second
           read. The span waited is the published constant, so the budgeting argument on it is the one the
           code actually honours. */
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(1), NegInfinityScheduled(2))
            .Then(NegInfinityScheduled(1), NegInfinityScheduled(2));
        var delay = new RecordedDelay();

        var result = await ReadStuckAsync(
            reads.Read, delay.Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal(2, reads.Calls);
        Assert.Equal(new[] { TimescaleSupport.StuckPolicyJobConfirmDelay }, delay.Waits);
    }

    /* ---------------- the confirm-read: what the second read decides ---------------- */

    [Fact]
    public async Task TransientEdge_ClearsOnConfirm_ReportsNothing_AndSaysSoAtInformation()
    {
        /* THE production shape: the first read lands on the run instant and sees -infinity + Scheduled; five
           seconds later the run is long over and the job reads healthy. Nothing is reported, so nothing is
           re-armed and nothing is paged — and the near miss is written down where a person reading the log
           after this alert family fires would look for it. */
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(1011, "file_io_stats"))
            .Then(Healthy(1011, "file_io_stats"));
        var delay = new RecordedDelay();
        var log = new CapturingTestLogger();

        var result = await ReadStuckAsync(
            reads.Read, delay.Wait, s_now, log, TestContext.Current.CancellationToken);

        Assert.Empty(result);
        Assert.Equal(2, reads.Calls);
        Assert.Contains("Information:", log.Joined, StringComparison.Ordinal);
        Assert.Contains("1011", log.Joined, StringComparison.Ordinal);
        Assert.Contains("file_io_stats", log.Joined, StringComparison.Ordinal);
        Assert.Contains("run-instant edge", log.Joined, StringComparison.Ordinal);
        Assert.Contains("#3575", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Warning:", log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PersistentNegInfinity_IsConfirmed_AndReported()
    {
        /* A row the scheduler has abandoned — or a crashed run sitting out its five-minute backoff, reproduced
           on the rig by SIGKILLing a worker — reads the same on both passes. Detection is unchanged in kind;
           it is five seconds later in time. */
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(7, "wait_stats"))
            .Then(NegInfinityScheduled(7, "wait_stats"));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        var job = Assert.Single(result);
        Assert.Equal(7L, job.JobId);
        Assert.Equal("wait_stats", job.HypertableName);
        Assert.Contains("-infinity", job.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task EdgeThatBecameMidRunOnConfirm_Clears()
    {
        /* The start edge seen twice in one run, at different stages: first -infinity + Scheduled (the worker
           not yet Running), then -infinity + Running (mid-run). The second read's arm is None, so the trip
           clears — mid-run belongs to the elapsed arm and always did. */
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(1))
            .Then(MidRun(1));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public async Task MixedPass_KeepsTheHungRun_ConfirmsOneTrip_ClearsTheOther()
    {
        /* Every row class in one pass, so the merge is pinned as a table rather than one row at a time: the
           hung run from the first read, the confirmed trip from the second, the transient trip dropped, and
           the always-healthy job never mentioned. */
        var reads = new ScriptedReads()
            .Then(HungRun(4), NegInfinityScheduled(1), NegInfinityScheduled(2), Healthy(3))
            .Then(HungRun(4), NegInfinityScheduled(1), Healthy(2), Healthy(3));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 4L, 1L }, result.Select(r => r.JobId).ToArray());
    }

    [Fact]
    public async Task JobThatOnlyTripsOnTheConfirm_IsNotReported()
    {
        /* The confirm ratifies the first pass; it does not widen it. A job that went -infinity between the two
           reads has been seen once, which is the count this issue proved insufficient, and it gets its own two
           reads next hour. */
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(1), Healthy(2))
            .Then(NegInfinityScheduled(1), NegInfinityScheduled(2));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { 1L }, result.Select(r => r.JobId).ToArray());
    }

    /* ---------------- the confirm-read: failure isolation ---------------- */

    [Fact]
    public async Task ConfirmReadFails_DropsTheTrips_KeepsTheHungRun_WarnsOnce_DoesNotThrow()
    {
        /* A confirm that fails confirms nothing. The -infinity trips are not reported on the strength of the
           single read this issue proved insufficient; the hung run, judged from the first read, still is. The
           failure is a Warning naming the count and the consequence — the views were readable seconds ago, so
           this is a hiccup on the one read that decides whether to page — and it never reaches the sweep. */
        var reads = new ScriptedReads()
            .Then(HungRun(4), NegInfinityScheduled(1), NegInfinityScheduled(2))
            .ThenThrow(new InvalidOperationException("connection reset by peer"));
        var log = new CapturingTestLogger();

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, log, TestContext.Current.CancellationToken);

        var job = Assert.Single(result);
        Assert.Equal(4L, job.JobId);
        Assert.Contains("Warning:", log.Joined, StringComparison.Ordinal);
        Assert.Contains("2 job(s)", log.Joined, StringComparison.Ordinal);
        Assert.Contains("confirm read", log.Joined, StringComparison.Ordinal);
        Assert.Contains("next hour", log.Joined, StringComparison.Ordinal);
        Assert.Contains("connection reset by peer", log.Joined, StringComparison.Ordinal);
        Assert.Contains("#3575", log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FirstReadFails_ReturnsEmpty_LogsDebug_NeverWaits_DoesNotThrow()
    {
        /* The pre-#3575 posture, unchanged: a failed first read is "no signal this check" at Debug — the views
           may simply be absent on a plain-PG store — and there is nothing to confirm, so no delay is taken. */
        var reads = new ScriptedReads().ThenThrow(new InvalidOperationException("relation does not exist"));
        var delay = new RecordedDelay();
        var log = new CapturingTestLogger();

        var result = await ReadStuckAsync(
            reads.Read, delay.Wait, s_now, log, TestContext.Current.CancellationToken);

        Assert.Empty(result);
        Assert.Empty(delay.Waits);
        Assert.StartsWith("Debug:", log.Joined, StringComparison.Ordinal);
        Assert.DoesNotContain("Warning:", log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Cancellation_DuringTheConfirmDelay_Propagates()
    {
        /* Shutdown during the five-second wait is cancellation, not a read failure: it propagates to the worker's
           own quiet catch rather than being swallowed into "confirm failed" and logged as a store fault. */
        var reads = new ScriptedReads().Then(NegInfinityScheduled(1));
        using var cts = new CancellationTokenSource();

        Task CancelInsteadOfWaiting(TimeSpan span, CancellationToken ct)
        {
            cts.Cancel();
            return Task.FromCanceled(cts.Token);
        }

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            ReadStuckAsync(reads.Read, CancelInsteadOfWaiting, s_now, null, cts.Token));
    }

    /* ---------------- the pure merge, pinned directly ---------------- */

    [Fact]
    public void ConfirmStuckCompressionJobs_NullConfirm_DropsEveryNegInfinityTrip_KeepsRunningPastBound()
    {
        var first = TimescaleSupport.ClassifyStuckPolicyJobs(
            new[] { HungRun(4), NegInfinityScheduled(1), NegInfinityScheduled(2) }, s_now);
        Assert.Equal(3, first.Count);

        var merged = TimescaleSupport.ConfirmStuckPolicyJobs(first, confirm: null, s_now, logger: null);

        Assert.Equal(new[] { 4L }, merged.Select(m => m.JobId).ToArray());
    }

    [Fact]
    public void ConfirmStuckCompressionJobs_CarriesTheConfirmPassRow()
    {
        /* The reported job is built from the CONFIRM pass's row — the later read is the one that stood. Today the
           two reasons are the same string; the pin is on which row is carried, using the hypertable name the two
           passes would only ever disagree on in a test. */
        var first = TimescaleSupport.ClassifyStuckPolicyJobs(new[] { NegInfinityScheduled(1, "first") }, s_now);
        var merged = TimescaleSupport.ConfirmStuckPolicyJobs(
            first, new[] { NegInfinityScheduled(1, "confirm") }, s_now, logger: null);

        var job = Assert.Single(merged);
        Assert.Equal("confirm", job.HypertableName);
    }

    /* ---------------- the version read: when it is taken and what it decides (#3591) ---------------- */

    /// <summary>A scripted version read: hands back the planted version (or throws), counting calls.</summary>
    private sealed class ScriptedVersion
    {
        private readonly Version? _version;
        private readonly Exception? _throw;
        public int Calls { get; private set; }

        public ScriptedVersion(Version? version) => _version = version;
        public ScriptedVersion(Exception ex) => _throw = ex;

        public Task<Version?> Read(CancellationToken ct)
        {
            Calls++;
            return _throw is null ? Task.FromResult(_version) : Task.FromException<Version?>(_throw);
        }
    }

    [Fact]
    public async Task HealthyPass_NeverReadsTheVersion()
    {
        /* The version decides the -infinity arm's sentence and nothing else, so a pass with nothing on that arm
           does not pay for it — the common hourly pass is still one read. */
        var reads = new ScriptedReads().Then(Healthy(1), HungRun(4));
        var version = new ScriptedVersion(new Version(2, 28, 1));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken, version.Read);

        Assert.Single(result);
        Assert.Equal(0, version.Calls);
        Assert.False(result[0].SchedulerRetries);
    }

    [Fact]
    public async Task PersistentNegInfinity_On_2_28_1_IsConfirmed_WithTheCrashBackoffSentence_AndSchedulerRetries()
    {
        /* The fleet's shape: a SIGKILLed worker's row on 2.28.1, reproduced on the rig. Still reported — the
           arm is the arm — but the sentence is the true one for this version, and SchedulerRetries tells the
           evaluator not to re-arm it (which, measured, resets the backoff rather than shortening it). */
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(7, "wait_stats"))
            .Then(NegInfinityScheduled(7, "wait_stats"));
        var version = new ScriptedVersion(new Version(2, 28, 1));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken, version.Read);

        var job = Assert.Single(result);
        Assert.Equal(7L, job.JobId);
        Assert.Equal(1, version.Calls);  /* once per pass, like the confirm */
        Assert.Equal(TimescaleSupport.NextStartNegativeInfinityCrashBackoffReason, job.Reason);
        Assert.True(job.SchedulerRetries);
    }

    [Fact]
    public async Task PersistentNegInfinity_BelowTheFix_KeepsTheOldSentence_AndTheReArm()
    {
        var reads = new ScriptedReads()
            .Then(NegInfinityScheduled(7))
            .Then(NegInfinityScheduled(7));
        var version = new ScriptedVersion(new Version(2, 26, 3));

        var result = await ReadStuckAsync(
            reads.Read, new RecordedDelay().Wait, s_now, logger: null, TestContext.Current.CancellationToken, version.Read);

        var job = Assert.Single(result);
        Assert.Equal(TimescaleSupport.NextStartNegativeInfinityPermanentReason, job.Reason);
        Assert.False(job.SchedulerRetries);
    }

    [Fact]
    public async Task VersionReadFails_OrIsAbsent_IsTheOldSentence_AndNeverFailsThePass()
    {
        /* Words, not verdicts: a version read that throws is swallowed at Debug and the pass proceeds exactly as
           if the version were unknown — the conservative sentence, the #1581 re-arm. The pre-#3591 seam (no
           reader at all) is the same case. */
        foreach (var reader in new Func<CancellationToken, Task<Version?>>?[]
        {
            new ScriptedVersion(new InvalidOperationException("permission denied for table pg_extension")).Read,
            new ScriptedVersion((Version?)null).Read,
            null,
        })
        {
            var reads = new ScriptedReads()
                .Then(NegInfinityScheduled(7))
                .Then(NegInfinityScheduled(7));
            var log = new CapturingTestLogger();

            var result = await ReadStuckAsync(
                reads.Read, new RecordedDelay().Wait, s_now, log, TestContext.Current.CancellationToken, reader);

            var job = Assert.Single(result);
            Assert.Equal(TimescaleSupport.NextStartNegativeInfinityPermanentReason, job.Reason);
            Assert.False(job.SchedulerRetries);
            Assert.DoesNotContain("Warning:", log.Joined, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ConfirmStuckCompressionJobs_TheVersionPhrasesTheConfirmRow_HungRunUntouched()
    {
        /* The merge applies the version to the CONFIRM pass — the row that is carried — and the hung run, judged
           from the first pass, never sees it. */
        var first = TimescaleSupport.ClassifyStuckPolicyJobs(new[] { HungRun(4), NegInfinityScheduled(1) }, s_now);
        var merged = TimescaleSupport.ConfirmStuckPolicyJobs(
            first, new[] { HungRun(4), NegInfinityScheduled(1) }, s_now, logger: null, new Version(2, 28, 1));

        Assert.Equal(2, merged.Count);
        Assert.Contains("Running", merged[0].Reason, StringComparison.Ordinal);
        Assert.False(merged[0].SchedulerRetries);
        Assert.Equal(TimescaleSupport.NextStartNegativeInfinityCrashBackoffReason, merged[1].Reason);
        Assert.True(merged[1].SchedulerRetries);
    }

    /* ---------------- the delay constant, against what it has to clear and what it costs ---------------- */

    [Fact]
    public void ConfirmDelay_ClearsTheMeasuredEdges_AndIsSmallAgainstEveryCadenceItSitsInside()
    {
        var delay = TimescaleSupport.StuckPolicyJobConfirmDelay;

        /* Quoted measurements, not derived: the rig's whole run was ~7.5 ms end to end and its start edge
           ~3 ms; the production store's hourly no-op runs were 40–100 ms; a Windows backend start is realistically
           tens of milliseconds. The delay must outlast all of them by an order of magnitude at least. */
        Assert.True(delay >= TimeSpan.FromMilliseconds(100 * 10),
            $"the confirm delay ({delay}) must clear a 100 ms run-instant edge ten times over");

        /* ...and it must stay far below the shortest PERSISTENT -infinity state there is, TimescaleDB's
           MIN_WAIT_AFTER_CRASH_MS of five minutes, or a real crash could slip between the two reads. */
        Assert.True(delay < TimeSpan.FromMinutes(1),
            $"the confirm delay ({delay}) must be far below the five-minute crash backoff a crashed row sits at -infinity for");

        /* ...and it must be negligible against the cadences it sits inside — the hourly check and the six-hour
           stuck-Running floor — so a genuinely dead job is detected on the same pass it always was. */
        Assert.True(delay.TotalSeconds * 100 < TimeSpan.FromHours(1).TotalSeconds,
            $"the confirm delay ({delay}) must be under 1 % of the hourly check cadence");
        Assert.True(delay * 100 < TimescaleSupport.StuckRunningBound(TimeSpan.FromHours(1)),
            $"the confirm delay ({delay}) must be under 1 % of the stuck-Running bound");
    }

    /* ---------------- de-alignment: the check's wall-clock phase ---------------- */

    [Fact]
    public void NextCompressionCheckUtc_SnapsToThePhase_NeverToAMinuteBoundary()
    {
        var interval = TimeSpan.FromHours(1);

        /* The production fire: 08:47:00.053, 53 ms into file_io_stats' :47:00 run. Scheduled from this fire the
           old way, the next sample would have been 09:47:00.053 + the loop's latency — on the boundary again.
           Snapped, it is 09:47:30 exactly. */
        var fired = new DateTime(2026, 9, 18, 8, 47, 0, 53, DateTimeKind.Utc);
        var next = TimescaleSupport.NextCompressionCheckUtc(fired, interval);
        Assert.Equal(new DateTime(2026, 9, 18, 9, 47, 30, DateTimeKind.Utc), next);
        Assert.Equal(DateTimeKind.Utc, next.Kind);

        /* From any second of the minute, the result sits on the phase, and the phase is not zero — the whole
           point is to be OFF the :00 instant the compression policies fire on. */
        Assert.NotEqual(0, TimescaleSupport.CompressionCheckPhaseSeconds);
        for (var second = 0; second < 60; second++)
        {
            var now = new DateTime(2026, 9, 18, 8, 47, second, 500, DateTimeKind.Utc);
            var due = TimescaleSupport.NextCompressionCheckUtc(now, interval);
            Assert.Equal(TimescaleSupport.CompressionCheckPhaseSeconds, due.Second);
            Assert.Equal(0, due.Millisecond);
            /* And the cadence stays hourly to within half a minute either way — never two hours, never zero. */
            var spacing = due - now;
            Assert.InRange(spacing, interval - TimeSpan.FromSeconds(30), interval + TimeSpan.FromSeconds(30));
        }
    }

    [Fact]
    public void NextCompressionCheckUtc_LocksThePhase_AcrossSimulatedFires_UnderLoopLatency()
    {
        /* The steady state the field will see: each fire happens at the first 15-second sweep pass at or after
           the due time, so the fire is 0–15 s late; the next due must still snap back to :30, so the phase does
           not creep the way "UtcNow + 1 h" did. Simulated over a day with every latency the loop can produce. */
        var interval = TimeSpan.FromHours(1);
        var due = TimescaleSupport.NextCompressionCheckUtc(new DateTime(2026, 9, 18, 1, 46, 13, DateTimeKind.Utc), interval);

        for (var hour = 0; hour < 24; hour++)
        {
            var latency = TimeSpan.FromSeconds(hour % 16); /* 0..15 s, the loop's whole range */
            var fired = due + latency;
            due = TimescaleSupport.NextCompressionCheckUtc(fired, interval);

            Assert.Equal(TimescaleSupport.CompressionCheckPhaseSeconds, due.Second);
            Assert.Equal(46, due.Minute); /* the minute never moves while the loop keeps under the half-minute */
        }
    }

    [Fact]
    public void NextCompressionCheckUtc_ALatePass_SnapsBackToThePhase_OrMovesAWholeMinute_NeverTowardTheBoundary()
    {
        /* A sweep pass delayed past the half-minute (the #2327 store-metrics worst case can hold the loop for
           minutes) fires late. Re-anchored from the fire the old way, the next due would carry that lateness
           forward and creep toward a boundary. Snapped, there are only two outcomes, and neither is nearer :00.

           Late inside its own minute (:46:42): the next due snaps BACK to :46:30 — twelve seconds short of a
           full hour, still on the phase. */
        var lateInMinute = new DateTime(2026, 9, 18, 9, 46, 42, DateTimeKind.Utc);
        Assert.Equal(
            new DateTime(2026, 9, 18, 10, 46, 30, DateTimeKind.Utc),
            TimescaleSupport.NextCompressionCheckUtc(lateInMinute, TimeSpan.FromHours(1)));

        /* Late into the NEXT minute (:47:05, a pass more than 35 s behind its :46:30 due): the check moves one
           whole minute later, to :47:30, and stays on the phase there. */
        var lateIntoNextMinute = new DateTime(2026, 9, 18, 9, 47, 5, DateTimeKind.Utc);
        Assert.Equal(
            new DateTime(2026, 9, 18, 10, 47, 30, DateTimeKind.Utc),
            TimescaleSupport.NextCompressionCheckUtc(lateIntoNextMinute, TimeSpan.FromHours(1)));
    }

    [Fact]
    public void ThePhase_IsHalfTheCompressionGridStep_AndClearOfEveryPolicyStart()
    {
        /* The compression policies start at :MM:00 for every MM in CompressionPhaseMinutes (the fixed-schedule
           initial_start carries whole minutes and nothing smaller). Half the one-minute grid step is the point
           furthest from every start instant in both directions. Derived from the grid's step, not restated. */
        Assert.Equal(30, TimescaleSupport.CompressionCheckPhaseSeconds);
        Assert.Equal(TimeSpan.FromMinutes(1) / 2, TimeSpan.FromSeconds(TimescaleSupport.CompressionCheckPhaseSeconds));

        /* And the AddCompressionPolicySql initial_start really is whole minutes: any policy this product owns
           puts its job on :MM:00, so the :30 phase is 30 s from every one of them. */
        foreach (var table in TimescaleSupport.CompressionPhaseOrder)
        {
            Assert.True(TimescaleSupport.TryCompressionPhaseMinutesFor(table, out var minute));
            Assert.Contains(
                $"INTERVAL '{minute} minutes'",
                TimescaleSupport.AddCompressionPolicySql(table), StringComparison.Ordinal);
            Assert.DoesNotContain("seconds", TimescaleSupport.AddCompressionPolicySql(table), StringComparison.Ordinal);
        }
    }
}
