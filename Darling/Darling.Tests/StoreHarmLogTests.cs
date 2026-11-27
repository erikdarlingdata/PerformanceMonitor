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
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins #9549's record of harm: the Store Checkpointer Pressure self-alert fires when a store read or a collector
/// store write suffered while a checkpoint sync ran, so the service keeps each such operation with its time and judges
/// them against the syncs the once-a-minute sampler saw. This class holds the rule
/// (<see cref="StoreHarmLog.Judge"/> and <see cref="CheckpointSync.Overlaps"/>), the log and its bounds, and the four
/// places an operation is recorded from: the alert pass's failed reads and retried reads
/// (<see cref="AlertReadFailureCounter.RecentReads"/>), a served read that ended at its deadline
/// (<see cref="SlowReadLog.Offer"/>), and a collector store write that ended in a transport fault
/// (<see cref="StoreHarmLog.NoteCollectorWriteFault"/>). The evaluator half, what fires and what the text says, is
/// <see cref="StoreToastAndCheckpointerTests"/>.
/// </summary>
public sealed class StoreHarmLogTests
{
    private static readonly DateTime Minute = new(2026, 9, 20, 12, 41, 0, DateTimeKind.Utc);

    /// <summary>A 23,542 ms sync the sample at 12:41 UTC saw, after the sample at 12:40.</summary>
    private static readonly CheckpointSync Sync = new(Minute.AddMinutes(-1), Minute, 23_542);

    private static StoreHarmEvent Harm(
        DateTime endedUtc, long elapsedMs = 10_000, StoreHarmKind kind = StoreHarmKind.AlertReadFailed, string name = "collection signals") =>
        new(endedUtc.AddMilliseconds(-elapsedMs), endedUtc, kind, name);

    /* ---- the rule ---------------------------------------------------------------------------------------------- */

    [Fact]
    public void ASync_RunsFromTheSampleBeforeItMinusItsLength_ToTheSampleThatSawIt()
    {
        /* The counter grows when the checkpoint ends, somewhere after the sample before it, so the sync began no
           earlier than that sample minus its own length and was over by the sample that saw it. */
        Assert.Equal(Minute.AddMinutes(-1).AddMilliseconds(-23_542), Sync.StartedUtc);

        /* Both ends count: an operation that ended as the sync began, or began as the sample saw it, shared an instant with it. */
        Assert.True(Sync.Overlaps(Sync.StartedUtc.AddSeconds(-10), Sync.StartedUtc));
        Assert.False(Sync.Overlaps(Sync.StartedUtc.AddSeconds(-10), Sync.StartedUtc.AddTicks(-1)));
        Assert.True(Sync.Overlaps(Minute, Minute.AddSeconds(10)));
        Assert.False(Sync.Overlaps(Minute.AddTicks(1), Minute.AddSeconds(10)));

        /* An operation that spans the whole sync, and one that is a single instant inside it. */
        Assert.True(Sync.Overlaps(Sync.StartedUtc.AddMinutes(-5), Minute.AddMinutes(5)));
        Assert.True(Sync.Overlaps(Minute.AddSeconds(-30), Minute.AddSeconds(-30)));
    }

    [Fact]
    public void Judge_FindsOnlyTheHarmThatOverlapsASync_OldestFirst()
    {
        var inside = Harm(Minute.AddSeconds(-30));
        var before = Harm(Minute.AddMinutes(-21));
        var after = Harm(Minute.AddMinutes(5));
        var earlierInside = Harm(Minute.AddSeconds(-45), kind: StoreHarmKind.HostReadDeadline, name: "get_wait_stats");

        var verdict = StoreHarmLog.Judge(Minute.AddMinutes(19), new[] { Sync }, new[] { after, inside, before, earlierInside });

        Assert.Equal(new[] { earlierInside, inside }, verdict.Findings.Select(finding => finding.Harm));
        Assert.All(verdict.Findings, finding => Assert.Equal(Sync, finding.Sync));
        Assert.Equal(Minute.AddMinutes(19), verdict.JudgedUtc);
        Assert.Equal(new[] { Sync }, verdict.Syncs);
    }

    [Fact]
    public void Judge_WithNoSync_FindsNothing_WhateverSuffered()
    {
        var verdict = StoreHarmLog.Judge(
            Minute.AddMinutes(19), Array.Empty<CheckpointSync>(), new[] { Harm(Minute.AddSeconds(-30)), Harm(Minute) });

        Assert.Empty(verdict.Findings);
        Assert.Empty(verdict.Syncs);
    }

    [Fact]
    public void Judge_StatesAnOperationThatOverlapsTwoSyncs_AgainstTheLongerOne()
    {
        /* A 12 s sync finished in the next minute. A read that ran from 12:40:50 to 12:41:20 overlaps both, and is
           one finding, stated against the 23.5 s sync, in whichever order the syncs arrive. */
        var shortSync = new CheckpointSync(Minute, Minute.AddMinutes(1), 12_000);
        var harm = Harm(Minute.AddSeconds(20), elapsedMs: 30_000);

        foreach (var syncs in new[] { new[] { shortSync, Sync }, new[] { Sync, shortSync } })
        {
            var finding = Assert.Single(StoreHarmLog.Judge(Minute.AddMinutes(19), syncs, new[] { harm }).Findings);
            Assert.Equal(harm, finding.Harm);
            Assert.Equal(Sync, finding.Sync);
        }
    }

    /// <summary>
    /// A sync shorter than the store read deadline (<see cref="DarlingSelfAlertEvaluator.CheckpointSyncBarMs"/>) cannot cut
    /// a read off by itself, so harm that overlaps only such a sync is nobody's finding, and the sync stays in the verdict
    /// as a sync the minute samples saw. A sync at the deadline can carry harm.
    /// </summary>
    [Theory]
    [InlineData(400L, false)]
    [InlineData(9_999L, false)]
    [InlineData(10_000L, true)]
    [InlineData(23_542L, true)]
    public void Judge_BlamesOnlyASyncOfAtLeastTheReadDeadline(long syncMs, bool carriesHarm)
    {
        var sync = new CheckpointSync(Minute.AddMinutes(-1), Minute, syncMs);
        var harm = Harm(Minute.AddSeconds(-30));
        Assert.True(sync.Overlaps(harm.StartedUtc, harm.EndedUtc), "the harm overlaps the sync either way");

        var verdict = StoreHarmLog.Judge(Minute.AddMinutes(19), new[] { sync }, new[] { harm });

        Assert.Equal(carriesHarm, StoreHarmLog.CanCarryHarm(sync));
        Assert.Equal(carriesHarm ? new[] { harm } : Array.Empty<StoreHarmEvent>(), verdict.Findings.Select(finding => finding.Harm));
        Assert.Equal(new[] { sync }, verdict.Syncs);
    }

    [Fact]
    public void Judge_StatesHarmAgainstTheLongSync_WhenAShortOneOverlapsItToo()
    {
        /* The 400 ms sync overlaps the read as well, but only the 23.5 s sync can carry it, in whichever order they arrive. */
        var shortSync = new CheckpointSync(Minute, Minute.AddMinutes(1), 400);
        var harm = Harm(Minute.AddSeconds(20), elapsedMs: 30_000);

        var finding = Assert.Single(StoreHarmLog.Judge(Minute.AddMinutes(19), new[] { shortSync, Sync }, new[] { harm }).Findings);
        Assert.Equal(Sync, finding.Sync);

        Assert.Empty(StoreHarmLog.Judge(Minute.AddMinutes(19), new[] { shortSync }, new[] { harm }).Findings);
    }

    [Fact]
    public void AVerdict_DescribesTheRowWrittenBesideIt_AndNotANeighbouringHours()
    {
        var row = new DateTime(2026, 9, 20, 13, 0, 0, DateTimeKind.Utc);
        var verdict = StoreHarmLog.Judge(row.AddSeconds(5), Array.Empty<CheckpointSync>(), Array.Empty<StoreHarmEvent>());

        Assert.True(verdict.Describes(row));
        Assert.False(verdict.Describes(row.AddHours(1)));
        Assert.False(verdict.Describes(row.AddHours(-1)));

        /* Fifteen minutes either way is the row's own; a tick more is not. */
        Assert.Equal(TimeSpan.FromMinutes(15), CheckpointHarmVerdict.RowMatchWindow);
        Assert.True(verdict.Describes(verdict.JudgedUtc.AddMinutes(15)));
        Assert.True(verdict.Describes(verdict.JudgedUtc.AddMinutes(-15)));
        Assert.False(verdict.Describes(verdict.JudgedUtc.AddMinutes(15).AddTicks(1)));
        Assert.False(verdict.Describes(verdict.JudgedUtc.AddMinutes(-15).AddTicks(-1)));
    }

    [Theory]
    [InlineData(nameof(StoreHarmKind.AlertReadFailed), "the alert pass's blocking read failed at 12:41 UTC")]
    [InlineData(nameof(StoreHarmKind.AlertReadRetried), "the alert pass's blocking read crossed its deadline and was retried at 12:41 UTC")]
    [InlineData(nameof(StoreHarmKind.HostReadDeadline), "the blocking read was cut off at its deadline at 12:41 UTC")]
    [InlineData(nameof(StoreHarmKind.CollectorWriteStalled), "the blocking collector's store write stalled at 12:41 UTC")]
    public void AnEvent_SaysWhatSufferedAndTheMinuteItEnded(string kind, string expected) =>
        Assert.Equal(expected, Harm(Minute.AddSeconds(12), kind: Enum.Parse<StoreHarmKind>(kind), name: "blocking").Describe());

    [Theory]
    [InlineData(1, "1 store operation")]
    [InlineData(2, "2 store operations")]
    [InlineData(1024, "1024 store operations")]
    public void TheCount_ReadsInTheSingularForOne(int count, string expected) =>
        Assert.Equal(expected, StoreHarmLog.CountText(count));

    /* ---- the log ----------------------------------------------------------------------------------------------- */

    [Fact]
    public void Record_KeepsTheOperationWithItsStartAndItsEnd()
    {
        var log = new StoreHarmLog();
        Assert.Empty(log.Snapshot());
        Assert.Null(log.LatestVerdict);

        log.Record(StoreHarmKind.HostReadDeadline, "get_wait_stats", Minute, 10_000);

        var only = Assert.Single(log.Snapshot());
        Assert.Equal(new StoreHarmEvent(Minute.AddSeconds(-10), Minute, StoreHarmKind.HostReadDeadline, "get_wait_stats"), only);

        /* A negative length is zero, and a blank name still reads as something. */
        log.Record(StoreHarmKind.CollectorWriteStalled, "  ", Minute.AddSeconds(1), -5);
        Assert.Equal(
            new StoreHarmEvent(Minute.AddSeconds(1), Minute.AddSeconds(1), StoreHarmKind.CollectorWriteStalled, "unnamed"), log.Snapshot()[1]);
    }

    [Fact]
    public void Record_KeepsTheNewestCapacity_AndDropsTheOldest()
    {
        var log = new StoreHarmLog();
        for (var i = 0; i < StoreHarmLog.Capacity + 3; i++)
        {
            log.Record(StoreHarmKind.HostReadDeadline, "read_" + i, Minute.AddSeconds(i), 0);
        }

        var kept = log.Snapshot();
        Assert.Equal(StoreHarmLog.Capacity, kept.Count);
        Assert.Equal("read_3", kept[0].Name);
        Assert.Equal("read_" + (StoreHarmLog.Capacity + 2), kept[^1].Name);
    }

    [Fact]
    public void ACollectorWriteFault_IsRecordedWhenTheStoreDidNotAnswer_AndNotWhenItDid()
    {
        var log = new StoreHarmLog();

        /* The store did not answer the write: Npgsql's command timeout, the shape a write caught inside a long sync takes. */
        var timeout = new NpgsqlException("Exception while reading from stream", new TimeoutException("Timeout during reading attempt"));
        Assert.False(log.NoteCollectorWriteFault(timeout, "wait_stats", Minute, 30_000));

        var stalled = Assert.Single(log.Snapshot());
        Assert.Equal(new StoreHarmEvent(Minute.AddSeconds(-30), Minute, StoreHarmKind.CollectorWriteStalled, "wait_stats"), stalled);

        /* A fault that is not the store failing to answer, and a cancel (the service stopping), are not stalls. The
           filter answers false for all of them, so the fault always goes on to the arm that handles it. */
        Assert.False(log.NoteCollectorWriteFault(new InvalidOperationException("a row the writer refused"), "wait_stats", Minute, 5));
        Assert.False(log.NoteCollectorWriteFault(new OperationCanceledException(), "wait_stats", Minute, 5));
        Assert.False(log.NoteCollectorWriteFault(new TaskCanceledException("cancelled", timeout), "wait_stats", Minute, 5));
        Assert.Single(log.Snapshot());
    }

    [Fact]
    public void ACollectorWriteTheStoreAnswered_IsAStallOnlyPastTheReadDeadline()
    {
        var log = new StoreHarmLog();
        Assert.Equal(DarlingSelfAlertEvaluator.CheckpointSyncBarMs, StoreHarmLog.StalledWriteMs);

        /* A write that landed in 400 ms, and one that took exactly the deadline: not stalls. */
        log.NoteCollectorWriteTime("wait_stats", Minute, 400);
        log.NoteCollectorWriteTime("wait_stats", Minute, StoreHarmLog.StalledWriteMs);
        Assert.Empty(log.Snapshot());

        /* One millisecond past it: the rows landed, and collection stood still for longer than a read is allowed to. */
        log.NoteCollectorWriteTime("wait_stats", Minute, StoreHarmLog.StalledWriteMs + 1);
        Assert.Equal(
            new StoreHarmEvent(Minute.AddMilliseconds(-10_001), Minute, StoreHarmKind.CollectorWriteStalled, "wait_stats"),
            Assert.Single(log.Snapshot()));

        /* The field shape: a write that waited out a 23.5 s sync and then landed overlaps that sync. */
        var waited = new StoreHarmLog();
        waited.NoteCollectorWriteTime("query_stats", Minute.AddSeconds(-5), 21_000);
        var finding = Assert.Single(waited.JudgeAndKeep(Minute.AddMinutes(19), new[] { Sync }, alertReads: null).Findings);
        Assert.Equal(Sync, finding.Sync);
        Assert.Equal("the query_stats collector's store write stalled at 12:40 UTC", finding.Harm.Describe());
    }

    /* ---- where harm is recorded from ---------------------------------------------------------------------------- */

    [Fact]
    public void TheAlertPassCounter_RemembersEachFailedAndRetriedRead_WithItsTimeAndLength()
    {
        var clock = Minute.AddSeconds(-30);
        var counter = new AlertReadFailureCounter(() => clock);
        Assert.Empty(counter.RecentReads());

        counter.RecordReadFailure("101", "collection signals", 10_004);
        clock = clock.AddSeconds(5);
        counter.RecordRetriedRead(null, "blocking", 10_001);
        clock = clock.AddSeconds(5);
        counter.RecordRetriedRead("101", " ", -3);

        Assert.Equal(
            new[]
            {
                new AlertReadFailureCounter.RecentRead(Minute.AddSeconds(-30), "collection signals", 10_004, Retried: false),
                new AlertReadFailureCounter.RecentRead(Minute.AddSeconds(-25), "blocking", 10_001, Retried: true),
                new AlertReadFailureCounter.RecentRead(Minute.AddSeconds(-20), "unnamed read", 0, Retried: true),
            },
            counter.RecentReads());

        /* The counts the health surface reads are what they were: one failure, two retries. */
        Assert.Equal(1, counter.ReadFor("101").ServerReadFailures);
        Assert.Equal(2, counter.ReadFor("101").InstanceRetriedReads);

        /* As harm: each read ran from its time minus its length to its time. */
        Assert.Equal(
            new[]
            {
                new StoreHarmEvent(Minute.AddSeconds(-30).AddMilliseconds(-10_004), Minute.AddSeconds(-30), StoreHarmKind.AlertReadFailed, "collection signals"),
                new StoreHarmEvent(Minute.AddSeconds(-25).AddMilliseconds(-10_001), Minute.AddSeconds(-25), StoreHarmKind.AlertReadRetried, "blocking"),
                new StoreHarmEvent(Minute.AddSeconds(-20), Minute.AddSeconds(-20), StoreHarmKind.AlertReadRetried, "unnamed read"),
            },
            StoreHarmLog.FromAlertReads(counter));
        Assert.Empty(StoreHarmLog.FromAlertReads(null));
    }

    [Fact]
    public void TheAlertPassCounter_KeepsTheNewestRecentReads_AndDropsTheOldest()
    {
        var clock = Minute;
        var counter = new AlertReadFailureCounter(() => clock);
        for (var i = 0; i < AlertReadFailureCounter.RecentReadCapacity + 2; i++)
        {
            counter.RecordRetriedRead(null, "read_" + i, i);
        }

        var kept = counter.RecentReads();
        Assert.Equal(AlertReadFailureCounter.RecentReadCapacity, kept.Count);
        Assert.Equal("read_2", kept[0].Read);
        Assert.Equal("read_" + (AlertReadFailureCounter.RecentReadCapacity + 1), kept[^1].Read);
    }

    [Fact]
    public async Task ARetriedAlertRead_IsRememberedAtTheMomentItsFirstAttemptCrossedTheDeadline()
    {
        var clock = Minute.AddSeconds(-30);
        var counter = new AlertReadFailureCounter(() => clock);
        var attempts = 0;

        var answer = await DarlingAlertReadAdapter.ExecuteWithOneRetryAsync(
            _ =>
            {
                attempts++;
                return attempts == 1
                    ? throw new NpgsqlException(
                        "Exception while reading from stream",
                        new TimeoutException("Timeout during reading attempt"))
                    : Task.FromResult(7);
            },
            "41",
            "collection signals",
            counter,
            (_, _) => Task.CompletedTask,
            CancellationToken.None);

        Assert.Equal(7, answer);
        var retried = Assert.Single(counter.RecentReads());
        Assert.Equal(Minute.AddSeconds(-30), retried.AtUtc);
        Assert.Equal("collection signals", retried.Read);
        Assert.True(retried.Retried);
        Assert.True(retried.ElapsedMs >= 0);
    }

    [Fact]
    public void AServedReadThatEndsAtItsDeadline_IsRecorded_AndOneThatAnswersOrFailsIsNot()
    {
        var harm = new StoreHarmLog();
        var log = new SlowReadLog { Harm = harm };
        using var opened = ReadScope.Open(null);

        log.Offer(opened.Scope, ReadSurface.Mcp, "get_fast", ReadOutcome.Ok, 40, null, null, null);
        log.Offer(opened.Scope, ReadSurface.Mcp, "get_slow", ReadOutcome.Ok, 7_000, null, null, null);
        log.Offer(opened.Scope, ReadSurface.Mcp, "get_broken", ReadOutcome.Error, 12_000, null, "X", null);
        Assert.Empty(harm.Snapshot());

        var before = DateTime.UtcNow;
        log.Offer(opened.Scope, ReadSurface.Web, "get_wait_stats", ReadOutcome.Timeout, 10_000, null, "57014", null);
        var after = DateTime.UtcNow;

        var recorded = Assert.Single(harm.Snapshot());
        Assert.Equal(StoreHarmKind.HostReadDeadline, recorded.Kind);
        Assert.Equal("get_wait_stats", recorded.Name);
        Assert.InRange(recorded.EndedUtc, before, after);
        Assert.Equal(TimeSpan.FromSeconds(10), recorded.EndedUtc - recorded.StartedUtc);
    }

    [Fact]
    public void JudgeAndKeep_JudgesTheLogAndTheAlertPassCounterTogether_AndKeepsTheVerdict()
    {
        var clock = Minute.AddSeconds(-30);
        var counter = new AlertReadFailureCounter(() => clock);
        counter.RecordReadFailure("101", "collection signals", 10_000);
        clock = Minute.AddMinutes(10);
        counter.RecordRetriedRead("101", "blocking", 10_000);

        var log = new StoreHarmLog();
        log.Record(StoreHarmKind.HostReadDeadline, "get_wait_stats", Minute.AddSeconds(-40), 10_000);
        log.Record(StoreHarmKind.CollectorWriteStalled, "wait_stats", Minute.AddMinutes(-30), 30_000);

        var verdict = log.JudgeAndKeep(Minute.AddMinutes(19), new[] { Sync }, counter);

        /* The served read and the failed alert read sat inside the sync, oldest first. The retried read ten minutes
           later and the write half an hour before did not. */
        Assert.Equal(
            new[] { (StoreHarmKind.HostReadDeadline, "get_wait_stats"), (StoreHarmKind.AlertReadFailed, "collection signals") },
            verdict.Findings.Select(finding => (finding.Harm.Kind, finding.Harm.Name)));
        Assert.Same(verdict, log.LatestVerdict);

        /* Without the alert engine there is no counter, and the log is judged alone. */
        var alone = log.JudgeAndKeep(Minute.AddMinutes(19), new[] { Sync }, alertReads: null);
        Assert.Equal("get_wait_stats", Assert.Single(alone.Findings).Harm.Name);
        Assert.Same(alone, log.LatestVerdict);
    }
}
