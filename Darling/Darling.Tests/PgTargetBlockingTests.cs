/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The blocking / active-query family of the PostgreSQL-target analysis engine (#3691 lane 17, design §2a):
/// <c>PG_BLOCKING_CHAIN</c>, <c>PG_LOCK_WAIT_EVENTS</c>, <c>PG_LONG_RUNNING_QUERY</c>, <c>ANOMALY_PG_BLOCKING</c>.
///
/// <para><b>Ungated:</b> the chain reconstructor (head detection, depth, the sessions behind a head, a waiter held by
/// two blockers, a cycle in flight with and without a head beside it); the scorer's three ramps with every bar's
/// <c>threshold_lineage = 0</c> stamp and the self-gate under each warning bar; the event fact's four shapes
/// (unavailable off / unknown / collector silent, the measured zero, graded); the amplifiers through the real
/// <c>ScoreAll</c>; every edge of the family — declared set, predicate gating on the head's state and the pid seam,
/// the Lock-wait and idle-fact edges declared from the saturation file — and the traversal both ways round; the
/// advice by exact string for the exit shape and by clause for each branch; the collector's SQL by text (the
/// parametrised floor, the exclusions, the message-parsed duration, the two log denominators, the alias to the
/// blocking reader's capture count); the baseline arm's and the detector's shared zero rule.</para>
///
/// <para><b>Gated e2e</b> (<c>DARLING_TEST_PG</c>): 31 days of one-minute <c>pg_blocking</c> captures in the collection
/// log with light routine blocking (one blocked session every fifth minute, two every fiftieth — the "0–2" of the
/// STEP brief), then a 4-hour window in which every capture holds a 3-deep chain under an idle-in-transaction head,
/// 12 <c>lock_wait</c> lines, and one 40-minute statement; the REAL <c>analyze_server</c> anchored at the window's end
/// returns <c>PG_BLOCKING_CHAIN</c> CRITICAL by the blocked side's own duration walking to <c>PG_IDLE_IN_TRANSACTION</c>,
/// <c>PG_LOCK_WAIT_EVENTS</c> corroborating, <c>PG_LONG_RUNNING_QUERY</c> in the WARNING band, and
/// <c>ANOMALY_PG_BLOCKING</c> in the chain's incident.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetBlockingTests
{
    private const string ServerName = "darling-pg-target-blocking-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static readonly DateTime T0 = new(2026, 9, 19, 10, 0, 0, DateTimeKind.Unspecified);

    /* ───────────────────────── the reconstructor ───────────────────────── */

    [Fact]
    public void Reconstruction_FindsTheHead_CountsEveryReachableWaiter_AndMeasuresDepth()
    {
        /* H → A → B → C (a 3-deep chain) plus H → D directly: one head, four behind it, depth 3. */
        var capture = PgTargetFactCollector.ReconstructBlockingCapture(T0,
        [
            Edge(blocked: 9001, blocking: 9000, blockedQueryMs: 400_000, blockingIdle: true),
            Edge(blocked: 9002, blocking: 9001, blockedQueryMs: 200_000),
            Edge(blocked: 9003, blocking: 9002, blockedQueryMs: 100_000),
            Edge(blocked: 9004, blocking: 9000, blockedQueryMs: 5_000, blockingIdle: true),
        ]);

        var head = Assert.Single(capture.Heads);
        Assert.Equal(9000, head.Pid);
        Assert.True(head.IsIdleInTransaction);
        Assert.Equal(3, head.Depth);
        Assert.Equal(4, head.BlockedSessions);
        Assert.Equal(400_000, head.MaxBlockedQueryMs);
        Assert.Equal("billing-worker", head.ApplicationName);
        Assert.Equal("billing", head.Username);
        Assert.Equal("appdb", head.DatabaseName);
        Assert.Equal(4, capture.BlockedSessions);
        Assert.False(capture.HasCycle);
    }

    [Fact]
    public void Reconstruction_AWaiterHeldByTwoBlockers_IsCountedBehindBoth_AndTwoIndependentChainsAreTwoHeads()
    {
        var capture = PgTargetFactCollector.ReconstructBlockingCapture(T0,
        [
            Edge(blocked: 9001, blocking: 9000, blockedQueryMs: 1_000),
            Edge(blocked: 9001, blocking: 9500, blockedQueryMs: 1_000),
            Edge(blocked: 9601, blocking: 9600, blockedQueryMs: 60_000),
        ]);

        Assert.Equal(new[] { 9000, 9500, 9600 }, capture.Heads.Select(h => h.Pid).Order().ToArray());
        Assert.All(capture.Heads, h => Assert.Equal(1, h.Depth));
        Assert.Equal(1, capture.Heads.Single(h => h.Pid == 9000).BlockedSessions);
        Assert.Equal(1, capture.Heads.Single(h => h.Pid == 9500).BlockedSessions);
        /* Distinct blocked pids in the capture — the anomaly's count — is two, not three edges. */
        Assert.Equal(2, capture.BlockedSessions);
    }

    [Fact]
    public void Reconstruction_ToleratesCycles_TerminatesOnOneReachableFromAHead_AndReportsOneNoHeadReaches()
    {
        /* A pure cycle (A ↔ B) has no head and is reported as one; a head H whose chain runs into a cycle (H → C,
           C → D, D → C) walks it once and stops. */
        var capture = PgTargetFactCollector.ReconstructBlockingCapture(T0,
        [
            Edge(blocked: 9001, blocking: 9002, blockedQueryMs: 2_000),
            Edge(blocked: 9002, blocking: 9001, blockedQueryMs: 2_000),
            Edge(blocked: 9003, blocking: 9000, blockedQueryMs: 30_000),
            Edge(blocked: 9004, blocking: 9003, blockedQueryMs: 20_000),
            Edge(blocked: 9003, blocking: 9004, blockedQueryMs: 30_000),
        ]);

        var head = Assert.Single(capture.Heads);
        Assert.Equal(9000, head.Pid);
        Assert.Equal(2, head.BlockedSessions);
        Assert.Equal(2, head.Depth);
        Assert.True(capture.HasCycle);
        Assert.Equal(4, capture.BlockedSessions);

        var pureCycle = PgTargetFactCollector.ReconstructBlockingCapture(T0,
        [
            Edge(blocked: 9001, blocking: 9002, blockedQueryMs: 2_000),
            Edge(blocked: 9002, blocking: 9001, blockedQueryMs: 2_000),
        ]);
        Assert.Empty(pureCycle.Heads);
        Assert.True(pureCycle.HasCycle);
        Assert.Equal(2, pureCycle.BlockedSessions);

        Assert.Empty(PgTargetFactCollector.ReconstructBlockingCapture(T0, []).Heads);
    }

    [Fact]
    public void ParseDurationMs_AppliesThePgSettingsUnit_AndRejectsNonsense()
    {
        Assert.Equal(1_000, PgTargetFactCollector.ParseDurationMs("1000", "ms"));
        Assert.Equal(2_000, PgTargetFactCollector.ParseDurationMs("2", "s"));
        Assert.Equal(60_000, PgTargetFactCollector.ParseDurationMs("1", "min"));
        Assert.Equal(1_000, PgTargetFactCollector.ParseDurationMs("1000", null));
        Assert.Null(PgTargetFactCollector.ParseDurationMs("on", null));
        Assert.Null(PgTargetFactCollector.ParseDurationMs(null, "ms"));
        Assert.Null(PgTargetFactCollector.ParseDurationMs("-1", "ms"));
    }

    /* ───────────────────────── the scorer ───────────────────────── */

    [Fact]
    public void TheBars_AreUnmeasured_OneVocabularyForChainAndEvents_AndTheLongRunnerCriticalIsSixWarnings()
    {
        Assert.Equal(30_000, PgTargetScorer.BlockingWaitWarningMs);
        Assert.Equal(300_000, PgTargetScorer.BlockingWaitCriticalMs);
        Assert.Equal(600_000, PgTargetScorer.LongRunningQueryWarningMs);
        Assert.Equal(3_600_000, PgTargetScorer.LongRunningQueryCriticalMs);
        Assert.Equal(3, PgTargetScorer.BlockingPersistenceCaptures);
        Assert.Equal(0.2, PgTargetScorer.BlockingCoFireBoost);
        Assert.Equal(1_000, PgTargetScorer.DeadlockTimeoutDefaultMs);
        /* No SQL Server constant reused by value (#3538 A5): the fleet blocking bars are seconds of a different quantity. */
        Assert.NotEqual(ServerHealthThresholds.BlockingCriticalWaitSeconds * 1_000.0, PgTargetScorer.BlockingWaitCriticalMs);

        /* Every bar says "unmeasured" in the six lines above it (the census enforces the marker; this pins the WORD
           for this file, whose every bar the 2026-09-19 calibration could not read). */
        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Blocking.cs");
        Assert.DoesNotContain("[\"threshold_lineage\"] = 1;", source, StringComparison.Ordinal);
        Assert.Contains("[\"threshold_lineage\"] = 0;", source, StringComparison.Ordinal);
        Assert.DoesNotContain("3_600_000", CSharpSourceWalker.StripCommentsAndStrings(source), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(29_999, 0.0)]
    [InlineData(30_000, 0.5)]
    [InlineData(165_000, 0.75)]
    [InlineData(300_000, 1.0)]
    [InlineData(900_000, 1.0)]
    public void TheChain_GradesTheBlockedSidesOwnDuration_ZeroUnderTheBar_AndStampsLineageZero(double blockedMs, double expected)
    {
        var chain = Chain(blockedMs);
        new FactScorer().ScoreAll([chain]);
        Assert.Equal(expected, chain.BaseSeverity, precision: 9);
        Assert.Equal(0, chain.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void TheChain_WithoutTheGradedKey_ScoresZero()
    {
        var bare = new Fact { Source = PgTargetSources.BlockingSource, Key = PgTargetFactKeys.BlockingChain, Value = 900, ServerId = 1 };
        new FactScorer().ScoreAll([bare]);
        Assert.Equal(0.0, bare.BaseSeverity);
    }

    [Theory]
    [InlineData(29_999, 0.0)]
    [InlineData(30_000, 0.5)]
    [InlineData(65_000, 0.5 + 0.5 * 35_000 / 270_000.0)]
    [InlineData(300_000, 1.0)]
    public void TheEvents_GradeTheLongestWrittenWait_OnTheChainsBars(double eventMs, double expected)
    {
        var events = Events(eventMs, waits: 6);
        new FactScorer().ScoreAll([events]);
        Assert.Equal(expected, events.BaseSeverity, precision: 9);
        Assert.Equal(0, events.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void TheEvents_UnavailableOrAMeasuredZero_ScoreNothing_WhateverTheValueSays()
    {
        var off = Events(900_000, waits: 0);
        off.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
        off.Metadata[PgTargetScorer.LockWaitReasonLogLockWaitsOffKey] = 1;
        var none = Events(900_000, waits: 0);
        none.Metadata[PgTargetScorer.LockWaitNoEventsKey] = 1;
        new FactScorer().ScoreAll([off, none]);
        Assert.Equal(0.0, off.BaseSeverity);
        Assert.Equal(0.0, none.BaseSeverity);
        Assert.Equal(0, off.Metadata["threshold_lineage"]);
    }

    [Theory]
    [InlineData(599_999, 0.0)]
    [InlineData(600_000, 0.5)]
    [InlineData(2_400_000, 0.8)]
    [InlineData(3_600_000, 1.0)]
    public void TheLongRunner_GradesTheStatementsOwnDuration(double queryMs, double expected)
    {
        var runner = Runner(queryMs, pid: 9500);
        new FactScorer().ScoreAll([runner]);
        Assert.Equal(expected, runner.BaseSeverity, precision: 9);
        Assert.Equal(0, runner.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void TheChainsAmplifiers_ArePersistence_TheWrittenEvents_AndTheAnomaly_EachOnTheSiblingsOwnVerdict()
    {
        /* Alone, one capture: no amplifier matches. */
        var alone = Chain(400_000, recurringHeadCaptures: 1);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(1.0, alone.Severity, precision: 9);
        Assert.Equal(3, alone.AmplifierResults.Count);
        Assert.All(alone.AmplifierResults, r => Assert.False(r.Matched));

        /* Persistence at the gate. */
        var persistent = Chain(400_000, recurringHeadCaptures: 3);
        new FactScorer().ScoreAll([persistent]);
        Assert.Equal(1.2, persistent.Severity, precision: 9);

        /* All three: 1.0 × (1 + 0.2 + 0.2 + 0.2) = 1.6. The events fact is lifted by the chain in return (0.5 → 0.6). */
        var chain = Chain(400_000, recurringHeadCaptures: 240);
        var events = Events(30_000, waits: 6);
        var anomaly = Anomaly(sigma: 6);
        new FactScorer().ScoreAll([chain, events, anomaly]);
        Assert.Equal(1.6, chain.Severity, precision: 9);
        Assert.Equal(0.6, events.Severity, precision: 9);
        Assert.True(anomaly.BaseSeverity > 0);

        /* An unfired sibling (under its own bar) amplifies nothing — the predicate reads BaseSeverity, not presence. */
        var quiet = Chain(400_000, recurringHeadCaptures: 1);
        var underBar = Events(10_000, waits: 6);
        new FactScorer().ScoreAll([quiet, underBar]);
        Assert.Equal(1.0, quiet.Severity, precision: 9);
        Assert.Equal(0.0, underBar.Severity);
    }

    [Fact]
    public void TheLongRunnersOnlyAmplifier_IsPersistence()
    {
        var once = Runner(2_400_000, pid: 9500, recurringCaptures: 2);
        var chronic = Runner(2_400_000, pid: 9500, recurringCaptures: 3);
        new FactScorer().ScoreAll([once]);
        new FactScorer().ScoreAll([chronic]);
        Assert.Equal(0.8, once.Severity, precision: 9);
        Assert.Single(once.AmplifierResults);
        Assert.Equal(0.96, chronic.Severity, precision: 9);
    }

    /* ───────────────────────── the graph ───────────────────────── */

    [Fact]
    public void TheChainsEdges_AreDeclared_AndTheLockWaitsAndTheIdleFactWalkIntoTheChainFromTheSaturationFile()
    {
        var graph = new PgTargetRelationshipGraph();
        Assert.Equal(
            new[] { PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.LongRunningQuery }.Order(StringComparer.Ordinal),
            graph.GetAllEdges(PgTargetFactKeys.BlockingChain).Select(e => e.Destination).Order(StringComparer.Ordinal));
        Assert.Equal(PgTargetFactKeys.BlockingChain, Assert.Single(graph.GetAllEdges(PgTargetFactKeys.LongRunningQuery)).Destination);
        Assert.Equal(PgTargetFactKeys.BlockingChain, Assert.Single(graph.GetAllEdges(PgTargetFactKeys.LockWaitEvents)).Destination);
        Assert.Equal(PgTargetFactKeys.BlockingChain, Assert.Single(graph.GetAllEdges(PgTargetFactKeys.AnomalyBlocking)).Destination);
        Assert.All(graph.GetAllEdges(PgTargetFactKeys.BlockingChain), e => Assert.Equal("blocking", e.Category));

        /* Declared from the file that owns the source node (the Lock waits' and the idle fact's edges are the saturation file's). */
        Assert.Contains(graph.GetAllEdges(PgTargetFactKeys.WaitKey("Lock", "relation")), e => e.Destination == PgTargetFactKeys.BlockingChain);
        Assert.Contains(graph.GetAllEdges(PgTargetFactKeys.WaitKey("Lock", null)), e => e.Destination == PgTargetFactKeys.BlockingChain);
        Assert.Contains(graph.GetAllEdges(PgTargetFactKeys.IdleInTransaction), e => e.Destination == PgTargetFactKeys.BlockingChain);
        var blockingFile = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetRelationshipGraph.Blocking.cs"));
        Assert.Equal(5, Regex.Matches(blockingFile, @"AddEdge\(").Count);
        Assert.DoesNotContain("AddEdge(PgTargetFactKeys.WaitKey", blockingFile, StringComparison.Ordinal);
        Assert.DoesNotContain("AddEdge(PgTargetFactKeys.IdleInTransaction", blockingFile, StringComparison.Ordinal);
        /* Every predicate reads a verdict (BaseSeverity) or the chain's own head metadata — never Severity. */
        Assert.DoesNotContain(".Severity > 0", blockingFile, StringComparison.Ordinal);
    }

    [Fact]
    public void TheChainToIdleEdge_OpensOnlyForAnIdleHead_WithTheIdleFactFired_AndTheReverseEdgeAgrees()
    {
        var graph = new PgTargetRelationshipGraph();

        var idleHead = Chain(400_000, headIdle: true);
        var parked = Parked(900_000);
        new FactScorer().ScoreAll([idleHead, parked]);
        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.BlockingChain, Lookup(idleHead, parked)), e => e.Destination == PgTargetFactKeys.IdleInTransaction);
        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.IdleInTransaction, Lookup(idleHead, parked)), e => e.Destination == PgTargetFactKeys.BlockingChain);

        /* An ACTIVE head beside a fired parked holder elsewhere: two findings, no edge either way. */
        var activeHead = Chain(400_000, headIdle: false);
        new FactScorer().ScoreAll([activeHead, parked]);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.BlockingChain, Lookup(activeHead, parked)));
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.IdleInTransaction, Lookup(activeHead, parked)), e => e.Destination == PgTargetFactKeys.BlockingChain);

        /* An idle head with the idle fact under its floor (base 0): nothing opens — the verdict, not presence. */
        var underFloor = Parked(59_999);
        new FactScorer().ScoreAll([idleHead, underFloor]);
        Assert.Equal(0.0, underFloor.BaseSeverity);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.BlockingChain, Lookup(idleHead, underFloor)));

        /* A chain under the bar (base 0) opens nothing into it from the idle side either. */
        var shortChain = Chain(10_000, headIdle: true);
        new FactScorer().ScoreAll([shortChain, parked]);
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.IdleInTransaction, Lookup(shortChain, parked)), e => e.Destination == PgTargetFactKeys.BlockingChain);
    }

    [Fact]
    public void TheChainAndTheLongRunner_JoinOnThePid_BothWays_AndNotOtherwise()
    {
        var graph = new PgTargetRelationshipGraph();
        var chain = Chain(400_000, headIdle: false, headPid: 9500);
        var sameRunner = Runner(2_400_000, pid: 9500);
        var otherRunner = Runner(2_400_000, pid: 9600);
        new FactScorer().ScoreAll([chain, sameRunner, otherRunner]);

        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.BlockingChain, Lookup(chain, sameRunner)), e => e.Destination == PgTargetFactKeys.LongRunningQuery);
        Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.LongRunningQuery, Lookup(chain, sameRunner)));
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.BlockingChain, Lookup(chain, otherRunner)));
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.LongRunningQuery, Lookup(chain, otherRunner)));
    }

    [Fact]
    public void TheLockWait_TheEvents_AndTheAnomaly_WalkIntoAFiredChain_AndNotIntoAnUnfiredOne()
    {
        var graph = new PgTargetRelationshipGraph();
        var chain = Chain(400_000);
        var lockWait = LockWait(PgTargetFactKeys.WaitKey("Lock", "relation"), 0.6);
        var events = Events(65_000, waits: 6);
        var anomaly = Anomaly(sigma: 6);
        new FactScorer().ScoreAll([chain, lockWait, events, anomaly]);
        var lookup = Lookup(chain, lockWait, events, anomaly);
        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.WaitKey("Lock", "relation"), lookup), e => e.Destination == PgTargetFactKeys.BlockingChain);
        Assert.Equal(PgTargetFactKeys.BlockingChain, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.LockWaitEvents, lookup)).Destination);
        Assert.Equal(PgTargetFactKeys.BlockingChain, Assert.Single(graph.GetActiveEdges(PgTargetFactKeys.AnomalyBlocking, lookup)).Destination);

        var shortChain = Chain(10_000);
        new FactScorer().ScoreAll([shortChain, lockWait, events, anomaly]);
        var quiet = Lookup(shortChain, lockWait, events, anomaly);
        Assert.DoesNotContain(graph.GetActiveEdges(PgTargetFactKeys.WaitKey("Lock", "relation"), quiet), e => e.Destination == PgTargetFactKeys.BlockingChain);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.LockWaitEvents, quiet));
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.AnomalyBlocking, quiet));
    }

    [Fact]
    public void TheTraversal_RootsTheChainAndWalksToTheIdleHolder_OrTheReverseWhenTheHolderOutranks()
    {
        /* Chain 1.0 × 1.2 (persistence) = 1.2 vs idle 1.0: [chain, idle], the story's category the family's source. */
        var chain = Chain(400_000, headIdle: true, recurringHeadCaptures: 3);
        var parked = Parked(600_000);
        var facts = new List<Fact> { chain, parked };
        new FactScorer().ScoreAll(facts);
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.BlockingChain, PgTargetFactKeys.IdleInTransaction }, story.Path);
        Assert.Equal(PgTargetSources.BlockingSource, story.Category);

        /* Idle 1.0 × 1.2 (recurrence) = 1.2 vs chain 0.5: [idle, chain] — one story either way round (lane 5's lesson). */
        var shortChain = Chain(30_000, headIdle: true);
        var chronic = Parked(600_000, recurring: 6);
        var reversed = new List<Fact> { shortChain, chronic };
        new FactScorer().ScoreAll(reversed);
        var holderFirst = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(reversed));
        Assert.Equal(new[] { PgTargetFactKeys.IdleInTransaction, PgTargetFactKeys.BlockingChain }, holderFirst.Path);

        /* An active head beside the same holder: two stories. */
        var activeHead = Chain(400_000, headIdle: false, recurringHeadCaptures: 3);
        var split = new List<Fact> { activeHead, Parked(600_000) };
        new FactScorer().ScoreAll(split);
        Assert.Equal(2, new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(split).Count);
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void TheChainAdvice_NamesTheHeadByStateAndCount_StatesTheSampleCaveat_AndSwitchesTheRemedyOnTheHeadsState()
    {
        var idleHead = Chain(400_000, headIdle: true, headPid: 9000, headBlocked: 720, headCaptures: 240, recurringHeadCaptures: 240,
            capturesWithBlocking: 240, capturesTotal: 240, peakBlocked: 3, longestDepth: 3, blockedXactMs: 410_000);
        var parked = Parked(900_000);
        var events = Events(65_000, waits: 6);
        new FactScorer().ScoreAll([idleHead, parked, events]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.BlockingChain, Lookup(idleHead, parked, events))!;

        Assert.Equal("billing-worker as billing headed a blocking chain while idle in transaction — 720 sessions queued behind it, the longest blocked for 6 min 40 s", block.Headline);
        Assert.Contains("pid 9000, idle in transaction — had 720 blocked sessions behind it summed over the 240 captures it headed", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The longest any blocked statement had been waiting when sampled was 6 min 40 s (its transaction 6 min 50 s old)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("read from the blocked session's own clock, never from captures × cadence", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Blocking was caught in 240 of the 240 pg_blocking captures in the window", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("3 sessions were blocked at once, the deepest chain was 3 deep", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The head was IDLE IN TRANSACTION", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_IDLE_IN_TRANSACTION fired in the same window (billing-worker as billing, 15 min)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("One head was the root in 240 captures", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_LOCK_WAIT_EVENTS fired with 6 waits logged past deadlock_timeout, the longest 1 min 5 s", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("deadlock_timeout on this server is 1 s (from the config snapshot)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("threshold_lineage = 0", block.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("The lever is the application behind billing-worker as billing", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("ROLLS ITS WORK BACK", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_terminate_backend(9000)", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("lock_timeout makes a statement fail instead of queueing", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_blocking", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_lock_stats", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_log_events", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.Ordinal);

        /* An active head that IS the long runner: the query is the lever, and the runner is named by pid. */
        var activeHead = Chain(400_000, headIdle: false, headPid: 9500, headQueryMs: 2_400_000, headXactMs: 2_500_000, capturesTotal: 0, cycles: 2);
        var runner = Runner(2_400_000, pid: 9500);
        new FactScorer().ScoreAll([activeHead, runner]);
        var active = PgTargetAdvice.Compose(PgTargetFactKeys.BlockingChain, Lookup(activeHead, runner))!;
        Assert.Contains("headed a blocking chain while running a statement", active.Headline, StringComparison.Ordinal);
        Assert.Contains("The head was ACTIVE — running a statement for 40 min in a transaction 41 min 40 s old", active.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_LONG_RUNNING_QUERY fired on the same pid (40 min at its longest sighting): the head IS the long runner.", active.Investigation, StringComparison.Ordinal);
        Assert.Contains("the window's captures (the collection log recorded no run count)", active.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 captures contained a cycle with no head", active.Investigation, StringComparison.Ordinal);
        Assert.Contains("(the engine default — no snapshot named it)", active.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("The lever is the statement billing-worker as billing was running", active.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("PG_IDLE_IN_TRANSACTION fired", active.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("PG_LOCK_WAIT_EVENTS fired", active.Investigation, StringComparison.Ordinal);

        /* The static block claims no figure and names the state switch. */
        var stat = PgTargetAdvice.Static(PgTargetFactKeys.BlockingChain)!;
        Assert.Equal("A blocking chain was sampled — sessions queued behind one head blocker's locks", stat.Headline);
        Assert.Contains("The remedy depends on the head's STATE", stat.Remediation, StringComparison.Ordinal);
        Assert.Contains("SAMPLES", stat.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheEventsAdvice_HasFourShapes_EachSayingWhatTheSilenceMeans()
    {
        var graded = Events(65_000, waits: 6, relation: "orders");
        graded.Metadata["acquired_events"] = 6;
        graded.Metadata[PgTargetScorer.LockWaitEventsAcquiredMsKey] = 390_000;
        graded.Metadata[PgTargetScorer.LockWaitEventsPerHourKey] = 1.5;
        graded.Metadata["top_relation_waits"] = 6;
        var chain = Chain(400_000, headIdle: true);
        new FactScorer().ScoreAll([graded, chain]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.LockWaitEvents, Lookup(graded, chain))!;
        Assert.Equal("The engine logged 6 lock waits past deadlock_timeout (most on orders) — the longest 1 min 5 s", block.Headline);
        Assert.Contains("The engine logged 6 lock waits that outlived deadlock_timeout (1 s) in the window — 1.5 per observed hour — and 6 of them were later acquired, for 6 min 30 s of waiting the engine wrote down", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("The relation most waited on was orders (6 waits, from the lines' CONTEXT)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_BLOCKING_CHAIN fired in the same window and names the head: billing-worker as billing, idle in transaction", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("one vocabulary for the two readings of one contention", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_log_events (family lock_wait)", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Lowering deadlock_timeout logs shorter waits and detects deadlocks sooner, at the cost of", block.Remediation, StringComparison.Ordinal);

        var off = Events(0, waits: 0);
        off.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
        off.Metadata[PgTargetScorer.LockWaitReasonLogLockWaitsOffKey] = 1;
        var offBlock = PgTargetAdvice.Compose(PgTargetFactKeys.LockWaitEvents, Lookup(off))!;
        Assert.Equal("Lock-wait events are unavailable on this server — log_lock_waits is off", offBlock.Headline);
        Assert.Contains("that is not a measured zero: log_lock_waits is OFF", offBlock.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Set log_lock_waits = on (reloadable, no restart)", offBlock.Remediation, StringComparison.Ordinal);
        Assert.Contains("Counter-objective: log volume", offBlock.Remediation, StringComparison.Ordinal);

        var silent = Events(0, waits: 0);
        silent.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
        silent.Metadata[PgTargetScorer.LockWaitReasonLogCollectorSilentKey] = 1;
        var silentBlock = PgTargetAdvice.Compose(PgTargetFactKeys.LockWaitEvents, Lookup(silent))!;
        Assert.Equal("Lock-wait events are unavailable on this server — the log collector did not run", silentBlock.Headline);
        Assert.Contains("nobody was reading the log", silentBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_log_events collector is enabled", silentBlock.Remediation, StringComparison.Ordinal);

        var unknown = Events(0, waits: 0);
        unknown.Metadata[PgTargetScorer.LockWaitUnavailableKey] = 1;
        unknown.Metadata[PgTargetScorer.LockWaitReasonSettingUnknownKey] = 1;
        Assert.Equal("Lock-wait events are unavailable on this server — the setting is unknown", PgTargetAdvice.Compose(PgTargetFactKeys.LockWaitEvents, Lookup(unknown))!.Headline);

        var none = Events(0, waits: 0);
        none.Metadata[PgTargetScorer.LockWaitNoEventsKey] = 1;
        none.Metadata["log_captures"] = 48;
        var noneBlock = PgTargetAdvice.Compose(PgTargetFactKeys.LockWaitEvents, Lookup(none))!;
        Assert.Equal("No lock wait outlived deadlock_timeout in the window — the engine logged none, with log_lock_waits on", noneBlock.Headline);
        Assert.Contains("the log collector ran 48 times in the window", noneBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("A measured zero", noneBlock.Investigation, StringComparison.Ordinal);

        Assert.Contains("silence is never read as zero waits", PgTargetAdvice.Static(PgTargetFactKeys.LockWaitEvents)!.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLongRunnerAdvice_NamesTheRunner_ExcludesLockWaiters_AndNamesTheChainOnlyOnItsOwnPid()
    {
        var runner = Runner(2_400_000, pid: 9500, recurringCaptures: 48, queryId: 12345, waiting: false);
        runner.Metadata["runners"] = 1;
        runner.Metadata["captures_with_runners"] = 48;
        runner.Metadata["runner_captures_seen"] = 48;
        var chain = Chain(400_000, headIdle: false, headPid: 9500, headBlocked: 3);
        new FactScorer().ScoreAll([runner, chain]);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.LongRunningQuery, Lookup(runner, chain))!;
        Assert.Equal("reporting-batch as reports ran one statement for 40 min — and headed a blocking chain while it ran", block.Headline);
        Assert.Contains("pid 9500, query_id 12345 — had been running for 40 min at its longest sighting", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("over the 10 min floor in 48 captures", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("it was on CPU each time it was sampled", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("Sessions waiting on a Lock are not counted here", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("still running the same statement in 48 captures", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_BLOCKING_CHAIN fired with THIS pid as its head: 3 sessions queued", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("10 min and 1 h of statement duration — unmeasured", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("statement_timeout ends any statement past the interval and ROLLS IT BACK", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_blocking shows the chain behind it", block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE INDEX", block.Remediation, StringComparison.Ordinal);

        var otherChain = Chain(400_000, headIdle: false, headPid: 9000);
        new FactScorer().ScoreAll([runner, otherChain]);
        var alone = PgTargetAdvice.Compose(PgTargetFactKeys.LongRunningQuery, Lookup(runner, otherChain))!;
        Assert.Equal("reporting-batch as reports ran one statement for 40 min", alone.Headline);
        Assert.DoesNotContain("THIS pid", alone.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAnomalyAdvice_StatesThePeakAndSigma_OrFirstOccurrence_AndTheStaticBlockNamesTheZeroRule()
    {
        var trusted = Anomaly(sigma: 6.2, peak: 3, lowQuality: false);
        var block = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBlocking, Lookup(trusted))!;
        Assert.Equal("Blocked sessions per capture spiked to 3 sessions — 6.2σ above its baseline for this time of week", block.Headline);
        Assert.Contains("baseline mean for this hour-of-week", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Anomalous spike", block.Headline, StringComparison.Ordinal);

        var young = Anomaly(sigma: 0, peak: 12, lowQuality: true);
        var first = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyBlocking, Lookup(young))!;
        Assert.Equal("Blocked sessions per capture reached 12 sessions — first occurrence, no baseline yet for this time of week", first.Headline);

        var stat = PgTargetAdvice.Static(PgTargetFactKeys.AnomalyBlocking)!;
        Assert.Equal("More sessions were blocked at once than this server's normal for this time of week", stat.Headline);
        Assert.Contains("ZERO for every capture the collection log says ran and found no edge", stat.Investigation, StringComparison.Ordinal);
        Assert.Contains("Both the peak and the window's mean had to clear the bar", stat.Investigation, StringComparison.Ordinal);
        Assert.Equal(stat, FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyBlocking));
    }

    /* ───────────────────────── the SQL and the arm, by text ───────────────────────── */

    [Fact]
    public void TheCollectorsReads_CarryTheFloorAsAParameter_TheExclusions_TheMessageParsedDuration_AndTheLogDenominators()
    {
        var runner = PgTargetFactCollector.PgTargetLongRunningQuerySql;
        Assert.Contains("query_duration_ms >= $4", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("600000", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("600_000", runner, StringComparison.Ordinal);
        Assert.Contains("state = 'active'", runner, StringComparison.Ordinal);
        Assert.Contains("coalesce(backend_type, '') = 'client backend'", runner, StringComparison.Ordinal);
        Assert.Contains("coalesce(wait_event_type, '') <> 'Lock'", runner, StringComparison.Ordinal);
        Assert.Contains("NOT coalesce(state_is_redacted, false)", runner, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT collection_time)", runner, StringComparison.Ordinal);
        Assert.Contains("GROUP BY pid, query_id, application_name, username, database_name", runner, StringComparison.Ordinal);
        Assert.Contains("LIMIT 25", runner, StringComparison.Ordinal);

        var events = PgTargetFactCollector.PgTargetLockWaitEventsSql;
        Assert.Contains("family = 'lock_wait'", events, StringComparison.Ordinal);
        /* Verified at source: the lock_wait parser lifts no metrics, so the duration is the message's own figure. */
        Assert.Contains("substring(message from 'after ([0-9]+(?:\\.[0-9]+)?) ms')", events, StringComparison.Ordinal);
        Assert.Contains("coalesce(duration_ms::DOUBLE PRECISION,", events, StringComparison.Ordinal);
        Assert.Contains("message LIKE 'process % still waiting for %'", events, StringComparison.Ordinal);
        Assert.Contains("message LIKE 'process % acquired %'", events, StringComparison.Ordinal);
        Assert.Contains("message LIKE 'process % detected deadlock %'", events, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'pg_log_events'", events, StringComparison.Ordinal);
        Assert.Contains("status = 'SUCCESS'", events, StringComparison.Ordinal);

        var settings = PgTargetFactCollector.PgTargetBlockingSettingsSql;
        Assert.Contains("c.name IN ('deadlock_timeout', 'log_lock_waits')", settings, StringComparison.Ordinal);
        Assert.Contains("NOT IN ('client', 'session', 'override')", settings, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(settings, @"collection_time >= \$3").Count);

        var edges = PgTargetFactCollector.PgTargetBlockingEdgesSql;
        Assert.Contains("FROM pg_blocking_edges", edges, StringComparison.Ordinal);
        Assert.DoesNotContain("blocked_query,", edges, StringComparison.Ordinal);
        Assert.DoesNotContain("blocking_query,", edges, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time, blocking_pid, blocked_pid", edges, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", edges, StringComparison.Ordinal);

        /* The denominator is the reader's, by alias — the fact and get_pg_blocking cannot disagree on captures_total. */
        Assert.Equal(DarlingPgBlockingReader.PgBlockingCaptureCountsSql, PgTargetFactCollector.PgTargetBlockingCaptureCountsSql);
        foreach (var sql in new[] { runner, events, settings, edges, PgTargetFactCollector.PgTargetBlockingCaptureCountsSql })
            Assert.Contains(sql, PgTargetFactCollector.AllSql);

        /* The read helpers are not collect-surface members (the census enumerates Collect*Async). */
        var code = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Blocking.cs"));
        Assert.Single(Regex.Matches(code, @"\bCollect\w+Async\("));
        Assert.Equal(5, Regex.Matches(code, @"new NpgsqlCommand\(").Count);
        /* Order inside the family: settings, chain, events, long runner. */
        var order = new[] { "ReadBlockingSettingsAsync(connection, context)", "ReadBlockingChainAsync(connection, context, facts, settings, windowEnd)", "ReadLockWaitEventsAsync(connection, context, facts, settings, windowEnd, chainEmitted)", "ReadLongRunningQueryAsync(connection, context, facts, windowEnd)" }
            .Select(call => code.IndexOf(call, StringComparison.Ordinal)).ToArray();
        Assert.All(order, i => Assert.True(i > 0));
        Assert.Equal(order.Order().ToArray(), order);
        /* The three silent event shapes are emitted only beside a chain fact — a quiet server keeps "no facts". */
        Assert.Contains("if (lines == 0 && !chainEmitted)", code, StringComparison.Ordinal);
    }

    [Fact]
    public void TheBaselineArmAndTheWindowRead_ShareTheZeroRule_AndTheArmEndsInTheOneScaffold()
    {
        var arm = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgBlockedSessions)!;
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, arm, StringComparison.Ordinal);
        Assert.Contains("clean AS (", arm, StringComparison.Ordinal);
        Assert.Contains("FROM collection_log", arm, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'pg_blocking'", arm, StringComparison.Ordinal);
        Assert.Contains("FULL OUTER JOIN blocked AS b ON b.minute = l.minute", arm, StringComparison.Ordinal);
        Assert.Contains("coalesce(b.blocked_sessions, 0)  AS v", arm, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT blocked_pid)::DOUBLE PRECISION", arm, StringComparison.Ordinal);
        Assert.Equal(2, Regex.Matches(arm, @"server_id = \$1 AND collection_time >= \$2 AND collection_time < \$3").Count);
        /* Q6 discipline: no EXTRACT of its own, no positional parameter past $3, the bucket never selected here. */
        Assert.DoesNotContain("EXTRACT(", arm[..arm.IndexOf("keyed AS (", StringComparison.Ordinal)], StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"\$[4-9]"), arm[..arm.IndexOf("keyed AS (", StringComparison.Ordinal)]);
        Assert.Equal(3, Regex.Matches(arm, @"date_bin\('1 minute', collection_time, TIMESTAMP '2000-01-01'\)").Count);

        var window = PgTargetAnomalyDetector.BlockedSessionsWindowSql;
        Assert.Contains("FULL OUTER JOIN blocked AS b ON b.minute = l.minute", window, StringComparison.Ordinal);
        Assert.Contains("coalesce(b.blocked_sessions, 0)  AS blocked_sessions", window, StringComparison.Ordinal);
        Assert.Equal(3, Regex.Matches(window, @"date_bin\('1 minute', collection_time, TIMESTAMP '2000-01-01'\)").Count);
        Assert.Contains("AVG(blocked_sessions) AS avg_blocked_sessions", window, StringComparison.Ordinal);
    }

    /* ───────────────────────── gated: the exit criterion ───────────────────────── */

    [Fact]
    public async Task ThirtyOneDaysOfLightBlocking_ThenAFourHourChainUnderAnIdleHead_YieldsTheChainStory_TheEvents_TheRunner_AndTheAnomalyInOneIncident()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the blocking-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);

            /* #4274: anchored on the hour, not on the raw minute — see AnchorEndUtc's doc comment. The
               chain's edges start at windowStart's minute + 1 (below), so windowStart's own minute is the
               window's only zero-blocked reading; a raw TruncateToMinutes(UtcNow) anchor let that reading
               land in a tile large enough to drag the worst tile's mean under the pinned 2.8 floor (observed
               2.727 in CI) on some wall clocks. See the proof matrix in PR #4274's description. */
            var windowEnd = AnchorEndUtc();
            var windowStart = windowEnd.AddHours(-4);
            var historyStart = windowStart.AddDays(-31);

            /* The coverage witness and the span gate read pg_database_stats: 25 h of span, one row a minute. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* deadlock_timeout 1 s, log_lock_waits on — the two settings this family states; no max_connections, so the
               sessions family emits no saturation fact (context only) and the idle fact roots on its own. */
            await PlantSettingsSnapshotAsync(connection, windowEnd.AddMinutes(-30), ct);

            /* 31 days of one-minute pg_blocking SUCCESS runs in the collection log (the zero witness), and the light
               routine blocking: one blocked session every fifth minute, two every fiftieth, each blocked for 2 s
               (under every bar). Set-based, so 45,000 log rows and ~9,000 edge rows land in three statements. */
            await PlantHistoryAsync(connection, historyStart, windowEnd, ct);

            /* The window: every one of the 240 captures holds H(9000, idle in transaction, billing-worker as billing) →
               A(9001, blocked 400 s) → B(9002, 200 s) → C(9003, 100 s). The blocked side's own clock says 400 s; the
               sample saw it 240 times, and the fact must say 400 s, not 240 minutes. */
            for (var minute = 1; minute <= 240; minute++)
            {
                var at = windowStart.AddMinutes(minute);
                var collectionId = CollectionIdGenerator.Next();
                await PlantEdgeAsync(connection, collectionId, at, blockedPid: 9001, blockingPid: 9000, blockedQueryMs: 400_000, blockedXactMs: 410_000, blockingIdle: true, blockingQueryMs: -1, blockingXactMs: 900_000, ct);
                await PlantEdgeAsync(connection, collectionId, at, blockedPid: 9002, blockingPid: 9001, blockedQueryMs: 200_000, blockedXactMs: 210_000, blockingIdle: false, blockingQueryMs: 400_000, blockingXactMs: 410_000, ct);
                await PlantEdgeAsync(connection, collectionId, at, blockedPid: 9003, blockingPid: 9002, blockedQueryMs: 100_000, blockedXactMs: 110_000, blockingIdle: false, blockingQueryMs: 200_000, blockingXactMs: 210_000, ct);
            }

            /* The session side, every five minutes (48 captures): the parked head (15 min idle in transaction, pins
               nothing) and the 40-minute report on pid 9500 — a client backend, active, on CPU, not a Lock waiter. */
            for (var capture = 1; capture <= 48; capture++)
            {
                var at = windowStart.AddMinutes(capture * 5);
                var collectionId = CollectionIdGenerator.Next();
                await PlantSessionRowAsync(connection, collectionId, at, pid: 9000, state: "idle in transaction", isIdle: true, xactMs: 900_000, queryMs: 900_000, waitEventType: "Client", queryId: null, applicationName: "billing-worker", username: "billing", ct);
                await PlantSessionRowAsync(connection, collectionId, at, pid: 9500, state: "active", isIdle: false, xactMs: 2_400_000, queryMs: 2_400_000, waitEventType: null, queryId: 12345L, applicationName: "reporting-batch", username: "reports", ct);
                /* A Lock waiter over the runner floor: the chain's, and excluded from the runner fact by the read. */
                await PlantSessionRowAsync(connection, collectionId, at, pid: 9001, state: "active", isIdle: false, xactMs: 410_000, queryMs: 400_000 + 600_000, waitEventType: "Lock", queryId: 777L, applicationName: "web", username: "app", ct);
                await PlantLogRunAsync(connection, "pg_log_events", at, rows: capture <= 12 ? 1 : 0, ct);
            }

            /* Twelve lock_wait lines: six 'still waiting' (deadlock_timeout crossed, on orders) and six 'acquired' at 65 s. */
            for (var i = 0; i < 6; i++)
            {
                var at = windowStart.AddMinutes(20 + i * 10);
                await PlantLockWaitLineAsync(connection, at, $"process {4100 + i} still waiting for ShareLock on transaction {800 + i} after 1000.123 ms", "while updating tuple (0,7) in relation \"orders\"", ct);
                await PlantLockWaitLineAsync(connection, at.AddMinutes(1), $"process {4100 + i} acquired ShareLock on transaction {800 + i} after 65000.500 ms", null, ct);
            }

            /* ── the collector alone, on the exact planted window. */
            var collector = new PgTargetFactCollector(postgres);
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = await collector.CollectFactsAsync(context);
            Assert.Equal(14_400_000, context.ObservedDurationMs, precision: 3);

            var family = facts.Where(f => f.Source == PgTargetSources.BlockingSource).ToList();
            Assert.Equal(new[] { PgTargetFactKeys.BlockingChain, PgTargetFactKeys.LockWaitEvents, PgTargetFactKeys.LongRunningQuery }, family.Select(f => f.Key).ToArray());

            var chain = family[0];
            Assert.Equal(400, chain.Value, precision: 9);
            Assert.Equal("billing-worker as billing", chain.ObjectName);
            Assert.Equal("appdb", chain.DatabaseName);
            /* Duration from the row, never from the cadence: 240 sightings, 400 s. */
            Assert.Equal(400_000, chain.Metadata[PgTargetScorer.BlockingMaxBlockedQueryMsKey]);
            Assert.Equal(410_000, chain.Metadata[PgTargetScorer.BlockingMaxBlockedXactMsKey]);
            Assert.Equal(9000, chain.Metadata[PgTargetScorer.BlockingHeadPidKey]);
            Assert.Equal(1, chain.Metadata[PgTargetScorer.BlockingHeadIsIdleInTransactionKey]);
            Assert.Equal(720, chain.Metadata[PgTargetScorer.BlockingHeadBlockedSessionsKey]);
            Assert.Equal(240, chain.Metadata[PgTargetScorer.BlockingHeadCapturesKey]);
            Assert.Equal(240, chain.Metadata[PgTargetScorer.BlockingRecurringHeadCapturesKey]);
            Assert.Equal(3, chain.Metadata[PgTargetScorer.BlockingLongestChainDepthKey]);
            Assert.Equal(3, chain.Metadata[PgTargetScorer.BlockingPeakBlockedSessionsKey]);
            Assert.Equal(240, chain.Metadata[PgTargetScorer.BlockingCapturesWithBlockingKey]);
            /* The log says the collector looked 240 times in the window: the runner stamps the log row when the run
               ENDS (20 s after the capture here), so the capture at windowStart logs inside the window and the one at
               windowEnd logs after it — 240 of 240 caught, and the count came from the log, not the edges. */
            Assert.Equal(240, chain.Metadata[PgTargetScorer.BlockingCapturesTotalKey]);
            Assert.Equal(0, chain.Metadata[PgTargetScorer.BlockingCycleCapturesKey]);
            Assert.Equal(1, chain.Metadata["distinct_heads"]);
            Assert.Equal(1_000, chain.Metadata[PgTargetScorer.BlockingDeadlockTimeoutMsKey]);
            Assert.Equal(1, chain.Metadata[PgTargetScorer.BlockingDeadlockTimeoutFromSnapshotKey]);
            Assert.Equal(0, chain.Metadata["head_last_seen_age_s"]);

            var events = family[1];
            Assert.Equal("orders", events.ObjectName);
            Assert.Equal(6, events.Metadata[PgTargetScorer.LockWaitEventsCountKey]);
            Assert.Equal(6, events.Metadata["acquired_events"]);
            Assert.Equal(12, events.Metadata["lines"]);
            Assert.Equal(65_000.5, events.Metadata[PgTargetScorer.LockWaitEventsMaxMsKey], precision: 6);
            Assert.Equal(6 * 65_000.5, events.Metadata[PgTargetScorer.LockWaitEventsAcquiredMsKey], precision: 6);
            Assert.Equal(1.5, events.Metadata[PgTargetScorer.LockWaitEventsPerHourKey], precision: 9);
            Assert.Equal(6, events.Metadata["top_relation_waits"]);
            /* 47, not 48: the log row lands when the run ends (20 s after the capture), so the run at windowEnd logs past it. */
            Assert.Equal(47, events.Metadata["log_captures"]);
            Assert.Equal(1, events.Metadata[PgTargetScorer.LockWaitLogLockWaitsOnKey]);
            Assert.False(events.Metadata.ContainsKey(PgTargetScorer.LockWaitUnavailableKey));
            Assert.False(events.Metadata.ContainsKey(PgTargetScorer.LockWaitNoEventsKey));

            var runner = family[2];
            Assert.Equal("reporting-batch as reports", runner.ObjectName);
            Assert.Equal(2_400, runner.Value, precision: 9);
            Assert.Equal(9500, runner.Metadata[PgTargetScorer.LongRunningQueryPidKey]);
            Assert.Equal(12345, runner.Metadata[PgTargetScorer.LongRunningQueryIdKey]);
            Assert.Equal(48, runner.Metadata[PgTargetScorer.LongRunningQueryCapturesKey]);
            Assert.Equal(0, runner.Metadata[PgTargetScorer.LongRunningQueryWaitingKey]);
            /* The Lock waiter on pid 9001 (1,000 s active) is excluded: one runner, not two. */
            Assert.Equal(1, runner.Metadata["runners"]);
            Assert.Equal(600_000, runner.Metadata["floor_ms"]);

            /* Lane 14's idle fact is in the same list, ahead of the family (emission order), for the graph to join. */
            var parked = Assert.Single(facts, f => f.Key == PgTargetFactKeys.IdleInTransaction);
            Assert.True(facts.IndexOf(parked) < facts.IndexOf(chain));

            /* ── the baseline and the detector alone. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgBlockedSessions, windowStart, ct);
            Assert.True(bucket.SampleCount > 0);
            Assert.True(bucket.IsTrustworthy, $"the 30-day blocked-sessions bucket is not trustworthy (samples {bucket.SampleCount}, days {bucket.DistinctDays})");
            Assert.InRange(bucket.Mean, 0.1, 0.4);
            Assert.Equal(0, bucket.Median);

            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyBlocking);
            Assert.Equal(3, anomaly.Value);
            Assert.Equal(3, anomaly.Metadata["peak_blocked_sessions"]);
            /* #3653 A8 option B: avg_blocked_sessions now comes from the WORST-SCORING TILE's own mean (one target-local
               hour of the 4h chain), not the whole window's mean across all four hours — so it can differ slightly from
               the pre-tile whole-window figure. #4274: with AnchorEndUtc's hour-pinned windowStart, the one tile that
               would hold the window's sole zero-blocked minute (windowStart's own) is always sized 1 — below
               MinTileSamples (3) — so it never scores, and every tile that does score reads a clean 3.0. The range
               stays (not tightened to 3.0 exactly) because it is the pre-existing, deliberately loose assertion. */
            Assert.InRange(anomaly.Metadata["avg_blocked_sessions"], 2.8, 3.0);
            Assert.Equal(0, anomaly.Metadata["baseline_low_quality"]);
            Assert.Equal(0, anomaly.Metadata["threshold_lineage"]);
            Assert.True(anomaly.Metadata["deviation_sigma"] >= 2.0);
            Assert.True(anomaly.Metadata["mean_sigma"] >= 2.0);

            /* ── THE EXIT CRITERION: the real analyze_server tool, anchored at the planted window's end. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                /* The chain roots CRITICAL by duration, lifted by persistence + the written events + the anomaly (1.6),
                   and walks to the idle holder — one story, category the family's source. */
                var card = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.BlockingChain);
                Assert.Equal(1.6, card.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal(PgTargetSources.BlockingSource, card.GetProperty("category").GetString());
                Assert.Equal(400, card.GetProperty("root_fact").GetProperty("value").GetDouble(), precision: 9);
                Assert.Equal(PgTargetFactKeys.IdleInTransaction, card.GetProperty("leaf_fact").GetProperty("key").GetString());
                Assert.Equal(2, card.GetProperty("fact_count").GetInt32());
                var advice = card.GetProperty("advice");
                Assert.Equal("billing-worker as billing headed a blocking chain while idle in transaction — 720 sessions queued behind it, the longest blocked for 6 min 40 s", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("Blocking was caught in 240 of the 240 pg_blocking captures in the window", investigation, StringComparison.Ordinal);
                Assert.Contains("PG_IDLE_IN_TRANSACTION fired in the same window (billing-worker as billing, 15 min)", investigation, StringComparison.Ordinal);
                Assert.Contains("PG_LOCK_WAIT_EVENTS fired with 6 waits logged past deadlock_timeout, the longest 1 min 5 s", investigation, StringComparison.Ordinal);
                Assert.Contains("ANOMALY_PG_BLOCKING co-fired: 3 blocked at the peak capture", investigation, StringComparison.Ordinal);
                Assert.Contains("never from captures × cadence", investigation, StringComparison.Ordinal);
                Assert.Contains("pg_terminate_backend(9000)", advice.GetProperty("remediation").GetString(), StringComparison.Ordinal);
                var tools = card.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                /* The story's reads: the chain's three, then the idle leaf's (the path merges its nodes' rows). */
                Assert.Equal(new[] { "get_pg_blocking", "get_pg_lock_stats", "get_pg_log_events", "get_pg_session_states", "get_pg_xmin_horizon" }, tools);
                /* The idle holder is consumed into the chain's story: no card of its own. */
                Assert.DoesNotContain(findings, f => RootKey(f) == PgTargetFactKeys.IdleInTransaction);

                /* The written events corroborate: WARNING by the longest wait, lifted by the chain, their own card. */
                var eventsCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.LockWaitEvents);
                /* analyze_server rounds severity to two places: (0.5 + 0.5 × 35,000.5 / 270,000) × 1.2 = 0.6778 → 0.68. */
                Assert.Equal(Math.Round((0.5 + 0.5 * 35_000.5 / 270_000.0) * 1.2, 2), eventsCard.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal("The engine logged 6 lock waits past deadlock_timeout (most on orders) — the longest 1 min 5 s", eventsCard.GetProperty("advice").GetProperty("headline").GetString());
                Assert.Contains("PG_BLOCKING_CHAIN fired in the same window and names the head: billing-worker as billing, idle in transaction", eventsCard.GetProperty("advice").GetProperty("investigation").GetString(), StringComparison.Ordinal);

                /* The 40-minute report: WARNING band (0.8 × 1.2 persistence), its own card, not the chain's head. */
                var runnerCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.LongRunningQuery);
                Assert.Equal(0.96, runnerCard.GetProperty("severity").GetDouble(), precision: 9);
                Assert.Equal("reporting-batch as reports ran one statement for 40 min", runnerCard.GetProperty("advice").GetProperty("headline").GetString());
                Assert.Contains("get_pg_session_states", runnerCard.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!));

                /* The anomaly shares the chain's incident (the fold), and its prose is the family's, never the SQL Server composer's. */
                var anomalyCard = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyBlocking);
                Assert.Equal(card.GetProperty("incident_id").GetString(), anomalyCard.GetProperty("incident_id").GetString());
                var anomalyHeadline = anomalyCard.GetProperty("advice").GetProperty("headline").GetString()!;
                Assert.StartsWith("Blocked sessions per capture spiked to 3 sessions — ", anomalyHeadline, StringComparison.Ordinal);
                Assert.DoesNotContain("Anomalous spike", anomalyHeadline, StringComparison.Ordinal);

                Assert.All(findings.SelectMany(f => f.GetProperty("next_tools").EnumerateArray()).Select(t => t.GetProperty("tool").GetString()!), t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));
            }

            /* get_analysis_facts under the family's source: three facts, every one stamped unmeasured. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.BlockingSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(3, shown.Count);
                Assert.All(shown, f => Assert.Equal(0, f.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble()));
                var chainShown = Assert.Single(shown, f => f.GetProperty("key").GetString() == PgTargetFactKeys.BlockingChain);
                Assert.Equal(240, chainShown.GetProperty("metadata").GetProperty(PgTargetScorer.BlockingCapturesTotalKey).GetDouble());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────── gated: #3691 lane 41, the zero-history extremity ───────────────────── */

    /// <summary>
    /// A month in which this server never blocked, then 92 blocked sessions: the face of #3691 lane 41.
    ///
    /// <para><b>What used to happen.</b> The 30-day <c>pg_blocked_sessions</c> bucket for a server that never blocks
    /// is all zeros — every logged <c>pg_blocking</c> capture a measured zero, no edge ever — so
    /// <c>EffectiveStdDev</c> is 0, <c>IsTrustworthy</c> is false however many samples it holds, and the gate routed
    /// the window to the absolute-fallback bar meant for a young store. <c>ANOMALY_PG_BLOCKING</c> came out
    /// <c>baseline_low_quality = 1</c>, graded off the fallback ramp's 0.5 floor, worded "first occurrence, no
    /// baseline yet" — against the most confident statement a baseline can make.</para>
    ///
    /// <para><b>What happens now.</b> The bucket is <c>IsZeroHistory</c>, the gate takes the extremity arm, and the
    /// fact carries <c>baseline_low_quality = 0</c> / <c>baseline_zero_history = 1</c> with the sample and distinct-day
    /// counts the claim rests on. Severity clears the face line's 0.5 and the advice takes its third shape. This is
    /// the LIVE proof: the real baseline provider's SQL produces the zero-history bucket (not a hand-built one), the
    /// real detector reads the real window, and the real <c>analyze_server</c> composes the card.</para>
    /// </summary>
    [Fact]
    public async Task AMonthOfZeroBlocking_ThenNinetyTwoBlockedSessions_FiresAsAnExtremity_NotAsFirstOccurrence()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the zero-history blocking e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, "postgres", 18, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);
            var historyStart = windowStart.AddDays(-31);

            /* The coverage witness and the span gate read pg_database_stats, as the family e2e above does. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* 31 days of one-minute pg_blocking SUCCESS runs and NOT ONE EDGE: every capture looked and saw
               nothing, which the baseline arm reads as a measured zero. This is the whole fixture — the history
               is the absence, deliberately planted as logged captures rather than as missing rows, because a
               capture that did not run is not a sample (the family's zero rule). */
            await PlantZeroBlockingHistoryAsync(connection, historyStart, windowStart, ct);

            /* The window: 92 distinct blocked sessions in every capture, one shallow chain each (a pool-wide
               lock storm), so peak and mean both sit at 92. Ninety-two is the face line's number. */
            for (var minute = 1; minute <= 240; minute++)
            {
                var at = windowStart.AddMinutes(minute);
                var collectionId = CollectionIdGenerator.Next();
                await PlantBlockedFanAsync(connection, collectionId, at, blockedSessions: 92, ct);
                await PlantLogRunAsync(connection, "pg_blocking", at, rows: 92, ct);
            }

            /* ── the baseline the provider's own SQL builds. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgBlockedSessions, windowStart, ct);
            Assert.True(bucket.SampleCount >= 10, $"samples {bucket.SampleCount}");
            Assert.True(bucket.DistinctDays >= 3, $"distinct days {bucket.DistinctDays}");
            Assert.Equal(0, bucket.Mean);
            Assert.Equal(0, bucket.StdDev);
            Assert.Equal(0, bucket.Median);
            Assert.Equal(0, bucket.Mad);
            /* THE FLIP: the same bucket that is not trustworthy IS zero-history, and its confidence is real. */
            Assert.False(bucket.IsTrustworthy);
            Assert.True(bucket.IsZeroHistory);
            Assert.True(bucket.Confidence > 0, "a month of measured zeros is quality, not the absence of it");

            /* ── the detector alone, on the real window. */
            var context = new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            var anomaly = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyBlocking);
            Assert.Equal(92, anomaly.Value);
            Assert.Equal(92, anomaly.Metadata["peak_blocked_sessions"]);
            Assert.Equal(1, anomaly.Metadata["baseline_zero_history"]);
            Assert.Equal(0, anomaly.Metadata["baseline_low_quality"]);   /* NOT the young-store path */
            Assert.Equal(0, anomaly.Metadata["fallback_exceedance"]);    /* no absolute bar was used */
            Assert.Equal(AnomalyThresholds.SigmaDisplayCap, anomaly.Metadata["deviation_sigma"]);
            Assert.Equal(bucket.SampleCount, anomaly.Metadata["baseline_samples"]);
            Assert.Equal(bucket.DistinctDays, anomaly.Metadata["baseline_distinct_days"]);
            Assert.True(anomaly.Metadata["confidence"] > 0);

            /* Severity past the face line, off the shared deviation ramp — no new scale was invented. */
            new FactScorer().ScoreAll([anomaly]);
            Assert.True(anomaly.BaseSeverity > 0.5, $"92 blocked sessions against a clean month scored {anomaly.BaseSeverity}");

            /* ── THE EXIT CRITERION: the real analyze_server tool, and the advice's third shape. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var findings = doc.RootElement.GetProperty("findings").EnumerateArray().ToList();
                var card = Assert.Single(findings, f => f.GetProperty("root_fact").GetProperty("key").GetString() == PgTargetFactKeys.AnomalyBlocking);
                Assert.True(card.GetProperty("severity").GetDouble() > 0.5);
                var advice = card.GetProperty("advice");
                Assert.Equal("Blocked sessions per capture reached 92 sessions — against a month in which this hour saw none", advice.GetProperty("headline").GetString());
                var investigation = advice.GetProperty("investigation").GetString()!;
                Assert.Contains("it is a measured ZERO", investigation, StringComparison.Ordinal);
                Assert.Contains("not one of them above zero", investigation, StringComparison.Ordinal);
                Assert.Contains("distinct day", investigation, StringComparison.Ordinal);
                Assert.Contains("Beyond any σ", investigation, StringComparison.Ordinal);
                /* The two older shapes must not leak into this one. */
                Assert.DoesNotContain("first occurrence", investigation, StringComparison.Ordinal);
                Assert.DoesNotContain("too thin", investigation, StringComparison.Ordinal);
                Assert.DoesNotContain("σ above", investigation, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>31 days of one-minute <c>pg_blocking</c> SUCCESS runs with zero rows collected and no edge rows at
    /// all — the zero-history fixture. Set-based: ~45,000 log rows in one statement.</summary>
    private static async Task PlantZeroBlockingHistoryAsync(NpgsqlConnection connection, DateTime historyStart, DateTime windowStart, CancellationToken ct)
    {
        using var log = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
SELECT $4 + row_number() OVER (ORDER BY m), $1, $2, 'pg_blocking', m + INTERVAL '20 seconds', 120, 'SUCCESS', NULL, 0, 90, 30
FROM generate_series($3::timestamp, $5::timestamp - INTERVAL '1 minute', INTERVAL '1 minute') AS s(m)", connection);
        log.Parameters.AddWithValue(ServerId);
        log.Parameters.AddWithValue(ServerName);
        log.Parameters.AddWithValue(historyStart);
        log.Parameters.AddWithValue(CollectionIdGenerator.Next());
        log.Parameters.AddWithValue(windowStart);
        await log.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One capture in which <paramref name="blockedSessions"/> distinct pids all wait behind one head — the
    /// pool-wide lock storm shape, so <c>COUNT(DISTINCT blocked_pid)</c> for the minute is exactly that count.</summary>
    private static async Task PlantBlockedFanAsync(NpgsqlConnection connection, long collectionId, DateTime at, int blockedSessions, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_blocking_edges
    (collection_id, collection_time, server_id, server_name, blocked_pid, blocking_pid, database_name, blocked_state,
     blocked_query_duration_ms, blocked_xact_duration_ms, blocking_state, blocking_application_name, blocking_username,
     blocking_query_duration_ms, blocking_xact_duration_ms, blocked_pid_count, blocking_is_idle_in_transaction, query_text_may_be_truncated)
SELECT $1, $2, $3, $4, 8000 + g, 7999, 'appdb', 'active', 30000, 31000, 'idle in transaction', 'billing-worker', 'billing',
       -1, 900000, 1, true, false
FROM generate_series(1, $5) AS s(g)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(blockedSessions);
        await command.ExecuteNonQueryAsync(ct);
    }

    /* ───────────────────────── builders ───────────────────────── */

    private static PgTargetFactCollector.PgBlockingEdgeSample Edge(int blocked, int blocking, long blockedQueryMs, bool blockingIdle = false) =>
        new(T0, blocked, blocking, blockedQueryMs, blockedQueryMs + 10_000, blockingIdle, "appdb", "billing-worker", "billing",
            blockingIdle ? -1 : 50_000, 900_000, false);

    private static Fact Chain(
        double blockedMs, bool headIdle = true, double headPid = 9000, double headBlocked = 3, double headCaptures = 1, double recurringHeadCaptures = 1,
        double capturesWithBlocking = 1, double capturesTotal = 240, double peakBlocked = 3, double longestDepth = 3, double blockedXactMs = 0,
        double headQueryMs = 0, double headXactMs = 900_000, double cycles = 0) => new()
    {
        Source = PgTargetSources.BlockingSource,
        Key = PgTargetFactKeys.BlockingChain,
        Value = blockedMs / 1_000.0,
        ServerId = 1,
        ObjectName = "billing-worker as billing",
        DatabaseName = "appdb",
        Metadata =
        {
            [PgTargetScorer.BlockingMaxBlockedQueryMsKey] = blockedMs,
            [PgTargetScorer.BlockingMaxBlockedXactMsKey] = blockedXactMs == 0 ? blockedMs + 10_000 : blockedXactMs,
            [PgTargetScorer.BlockingHeadPidKey] = headPid,
            [PgTargetScorer.BlockingHeadIsIdleInTransactionKey] = headIdle ? 1 : 0,
            [PgTargetScorer.BlockingHeadBlockedSessionsKey] = headBlocked,
            [PgTargetScorer.BlockingHeadCapturesKey] = headCaptures,
            [PgTargetScorer.BlockingRecurringHeadCapturesKey] = recurringHeadCaptures,
            ["head_depth"] = longestDepth,
            ["head_query_ms"] = headQueryMs,
            ["head_xact_ms"] = headXactMs,
            ["distinct_heads"] = 1,
            [PgTargetScorer.BlockingLongestChainDepthKey] = longestDepth,
            [PgTargetScorer.BlockingPeakBlockedSessionsKey] = peakBlocked,
            [PgTargetScorer.BlockingCapturesWithBlockingKey] = capturesWithBlocking,
            [PgTargetScorer.BlockingCapturesTotalKey] = capturesTotal,
            [PgTargetScorer.BlockingCycleCapturesKey] = cycles,
            [PgTargetScorer.BlockingDeadlockTimeoutMsKey] = 1_000,
            [PgTargetScorer.BlockingDeadlockTimeoutFromSnapshotKey] = capturesTotal > 0 ? 1 : 0,
        },
    };

    private static Fact Events(double maxMs, double waits, string? relation = null) => new()
    {
        Source = PgTargetSources.BlockingSource,
        Key = PgTargetFactKeys.LockWaitEvents,
        Value = maxMs / 1_000.0,
        ServerId = 1,
        ObjectName = relation,
        Metadata =
        {
            [PgTargetScorer.LockWaitEventsCountKey] = waits,
            [PgTargetScorer.LockWaitEventsMaxMsKey] = maxMs,
            ["lines"] = waits * 2,
            [PgTargetScorer.BlockingDeadlockTimeoutMsKey] = 1_000,
            [PgTargetScorer.BlockingDeadlockTimeoutFromSnapshotKey] = 1,
        },
    };

    private static Fact Runner(double queryMs, double pid, double recurringCaptures = 1, double queryId = 0, bool waiting = false) => new()
    {
        Source = PgTargetSources.BlockingSource,
        Key = PgTargetFactKeys.LongRunningQuery,
        Value = queryMs / 1_000.0,
        ServerId = 1,
        ObjectName = "reporting-batch as reports",
        DatabaseName = "appdb",
        Metadata =
        {
            [PgTargetScorer.LongRunningQueryMaxMsKey] = queryMs,
            [PgTargetScorer.LongRunningQueryPidKey] = pid,
            [PgTargetScorer.LongRunningQueryIdKey] = queryId,
            [PgTargetScorer.LongRunningQueryCapturesKey] = recurringCaptures,
            [PgTargetScorer.LongRunningQueryWaitingKey] = waiting ? 1 : 0,
        },
    };

    private static Fact Anomaly(double sigma, double peak = 3, bool lowQuality = false) => new()
    {
        Source = "anomaly",
        Key = PgTargetFactKeys.AnomalyBlocking,
        Value = peak,
        ServerId = 1,
        Metadata =
        {
            ["baseline_mean"] = 0.2,
            ["baseline_stddev"] = 0.45,
            ["baseline_median"] = 0,
            ["baseline_mad"] = 0,
            ["deviation_sigma"] = sigma,
            ["fire_threshold"] = 2.0,
            ["baseline_low_quality"] = lowQuality ? 1 : 0,
            ["fallback_exceedance"] = lowQuality ? peak / AnomalyThresholds.PgBlockedSessionsFallback : 0,
            ["baseline_samples"] = 257,
            ["window_samples"] = 240,
            ["peak_blocked_sessions"] = peak,
            ["avg_blocked_sessions"] = peak,
            ["confidence"] = 1.0,
            ["threshold_lineage"] = 0,
        },
    };

    /// <summary>Lane 14's idle fact, the shape its own tests build.</summary>
    private static Fact Parked(double heldMs, double horizonAge = -1, double recurring = 1) => new()
    {
        Source = PgTargetSources.SessionsSource,
        Key = PgTargetFactKeys.IdleInTransaction,
        Value = heldMs / 1_000.0,
        ServerId = 1,
        ObjectName = "billing-worker as billing",
        DatabaseName = "appdb",
        Metadata =
        {
            [PgTargetScorer.IdleInTransactionDurationMsKey] = heldMs,
            [PgTargetScorer.IdleInTransactionHolderHorizonAgeKey] = horizonAge,
            [PgTargetScorer.IdleInTransactionRecurringCapturesKey] = recurring,
            ["holder_captures_seen"] = recurring,
            ["holder_identities"] = 1,
            ["captures_with_holders"] = recurring,
            ["captures_with_rows"] = 48,
            ["peak_concurrent_holders"] = 1,
        },
    };

    /// <summary>Lane 5's Aurora Lock wait, over its bar (the wait tests' builder, by shape).</summary>
    private static Fact LockWait(string key, double fraction) => new()
    {
        Source = PgTargetSources.WaitsSource,
        Key = key,
        Value = fraction,
        ServerId = 1,
        Metadata =
        {
            ["wait_ms_per_sec"] = fraction * 1_000,
            ["share_of_waits"] = 0.9,
            ["is_measured"] = 1,
            ["is_standout"] = 1,
        },
    };

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static DateTime TruncateToHour(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour)), DateTimeKind.Unspecified);

    /// <summary>#4274's test-only clock seam: <c>DARLING_TEST_NOW_UTC</c> (ISO-8601, e.g.
    /// <c>2026-09-25T00:15:00Z</c>) stands in for <c>DateTime.UtcNow</c> when set, so the wall-clock proof
    /// matrix in PR #4274 can drive <see cref="AnchorEndUtc"/> at chosen instants without waiting for real
    /// clock minutes to land there. Unset in every normal run (CI included) — falls through to the real clock.</summary>
    private static DateTime SimulatedUtcNow()
    {
        var raw = Environment.GetEnvironmentVariable("DARLING_TEST_NOW_UTC");
        return string.IsNullOrEmpty(raw)
            ? DateTime.UtcNow
            : DateTime.Parse(raw, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal);
    }

    /// <summary>#4274: the window's END, pinned to the hour instead of the raw minute. Truncating to the
    /// CURRENT hour and stepping back one minute always lands on :59 of the PRIOR hour — deterministic
    /// regardless of what minute <see cref="SimulatedUtcNow"/> (or the real clock) happens to read, unlike
    /// the old <c>TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1)</c>, whose minute-of-hour varied with
    /// wall-clock time and could land <c>windowStart</c>'s own tile (the one holding the window's sole
    /// zero-blocked minute) at a size large enough to drag the worst tile's mean under the pinned floor, on
    /// roughly a 1-in-60 draw.</summary>
    private static DateTime AnchorEndUtc() => TruncateToHour(SimulatedUtcNow()).AddMinutes(-1);

    /* ───────────────────────── planting ───────────────────────── */

    private static async Task PlantSettingsSnapshotAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        var collectionId = CollectionIdGenerator.Next();
        foreach (var (name, setting, unit, vartype) in new (string, string, string?, string)[] { ("deadlock_timeout", "1000", "ms", "integer"), ("log_lock_waits", "on", null, "bool") })
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Test', 'superuser', $8, 'configuration file', $6, $6, NULL, NULL, false, NULL)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(name);
            command.Parameters.AddWithValue(setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddWithValue(vartype);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// 31 days of the collector's minute: a SUCCESS log row per minute (rows_collected 0 / 1 / 2 as the edges say), an
    /// edge on every fifth minute and a second on every fiftieth, blocked for 2 s — the light routine blocking the
    /// STEP brief describes, zero-heavy so the buckets' median is 0 and the classical frame decides. The log rows run
    /// through the window too (the sample's denominator: 240 land inside the 4-hour window), stamped 20 s after the
    /// capture as the runner would stamp them.
    /// </summary>
    private static async Task PlantHistoryAsync(NpgsqlConnection connection, DateTime historyStart, DateTime windowEnd, CancellationToken ct)
    {
        var windowStart = windowEnd.AddHours(-4);
        using (var log = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
SELECT $4 + row_number() OVER (ORDER BY m), $1, $2, 'pg_blocking', m + INTERVAL '20 seconds', 120, 'SUCCESS', NULL,
       CASE WHEN m >= $5 THEN 3 WHEN (EXTRACT(EPOCH FROM (m - $3)) / 60)::bigint % 50 = 0 THEN 2 WHEN (EXTRACT(EPOCH FROM (m - $3)) / 60)::bigint % 5 = 0 THEN 1 ELSE 0 END, 90, 30
FROM generate_series($3::timestamp, $6::timestamp, INTERVAL '1 minute') AS s(m)", connection))
        {
            log.Parameters.AddWithValue(ServerId);
            log.Parameters.AddWithValue(ServerName);
            log.Parameters.AddWithValue(historyStart);
            log.Parameters.AddWithValue(CollectionIdGenerator.Next());
            log.Parameters.AddWithValue(windowStart);
            log.Parameters.AddWithValue(windowEnd);
            await log.ExecuteNonQueryAsync(ct);
        }

        foreach (var (every, blockedPid) in new[] { (5, 7001), (50, 7002) })
        {
            using var edges = new NpgsqlCommand(@"
INSERT INTO pg_blocking_edges
    (collection_id, collection_time, server_id, server_name, blocked_pid, blocking_pid, database_name, blocked_state,
     blocked_query_duration_ms, blocked_xact_duration_ms, blocking_state, blocking_application_name, blocking_username,
     blocking_query_duration_ms, blocking_xact_duration_ms, blocked_pid_count, blocking_is_idle_in_transaction, query_text_may_be_truncated)
SELECT $4 + row_number() OVER (ORDER BY m), m, $1, $2, $6, 7000, 'appdb', 'active', 2000, 2500, 'active', 'web', 'app', 1500, 2500, 1, false, false
FROM generate_series($3::timestamp, $5::timestamp - INTERVAL '1 minute', INTERVAL '1 minute') AS s(m)
WHERE (EXTRACT(EPOCH FROM (m - $3)) / 60)::bigint % $7 = 0", connection);
            edges.Parameters.AddWithValue(ServerId);
            edges.Parameters.AddWithValue(ServerName);
            edges.Parameters.AddWithValue(historyStart);
            edges.Parameters.AddWithValue(CollectionIdGenerator.Next());
            edges.Parameters.AddWithValue(windowStart);
            edges.Parameters.AddWithValue(blockedPid);
            edges.Parameters.AddWithValue((long)every);
            await edges.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task PlantEdgeAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, int blockedPid, int blockingPid, long blockedQueryMs, long blockedXactMs,
        bool blockingIdle, long blockingQueryMs, long blockingXactMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_blocking_edges
    (collection_id, collection_time, server_id, server_name, blocked_pid, blocking_pid, database_name,
     blocked_username, blocked_application_name, blocked_state, blocked_wait_event_type, blocked_wait_event, blocked_query,
     blocked_xact_duration_ms, blocked_query_duration_ms,
     blocking_username, blocking_application_name, blocking_state, blocking_query, blocking_xact_duration_ms, blocking_query_duration_ms,
     blocked_pid_count, blocking_is_idle_in_transaction, query_text_may_be_truncated)
VALUES ($1, $2, $3, $4, $5, $6, 'appdb',
        'app', 'web', 'active', 'Lock', 'transactionid', 'UPDATE orders SET status = $1 WHERE id = $2',
        $7, $8,
        'billing', 'billing-worker', $9, 'UPDATE orders SET total = $1 WHERE id = $2', $10, $11,
        1, $12, false)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(blockedPid);
        command.Parameters.AddWithValue(blockingPid);
        command.Parameters.AddWithValue(blockedXactMs);
        command.Parameters.AddWithValue(blockedQueryMs);
        command.Parameters.AddWithValue(blockingIdle ? "idle in transaction" : "active");
        command.Parameters.AddWithValue(blockingXactMs);
        command.Parameters.AddWithValue(blockingQueryMs);
        command.Parameters.AddWithValue(blockingIdle);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantSessionRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime at, int pid, string state, bool isIdle, long xactMs, long queryMs,
        string? waitEventType, long? queryId, string applicationName, string username, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_session_states
    (collection_id, collection_time, server_id, server_name, backend_id, pid, database_name, username, application_name, client_addr,
     backend_type, state, wait_event_type, wait_event, command_tag, query_id,
     state_duration_ms, xact_duration_ms, query_duration_ms, backend_duration_ms, xmin_age, xid_age, horizon_age,
     is_idle_in_transaction, is_horizon_holder, state_is_redacted,
     total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions)
VALUES ($1, $2, $3, $4, $5, $5, 'appdb', $6, $7, NULL,
        'client backend', $8, $9, $9, 'UPDATE', $10,
        $11, $11, $12, 3600000, -1, -1, -1,
        $13, false, false,
        30, 10, 1, 3)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue((long)pid);
        command.Parameters.AddWithValue(username);
        command.Parameters.AddWithValue(applicationName);
        command.Parameters.AddWithValue(state);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)waitEventType ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = queryId.HasValue ? queryId.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.AddWithValue(xactMs);
        command.Parameters.AddWithValue(queryMs);
        command.Parameters.AddWithValue(isIdle);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantLogRunAsync(NpgsqlConnection connection, string collector, DateTime at, int rows, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 120, 'SUCCESS', NULL, $6, 90, 30)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(at.AddSeconds(20));
        command.Parameters.AddWithValue(rows);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>One <c>lock_wait</c> line as the parser stores it: family, LOG severity, the message as written, the
    /// CONTEXT, and NO metrics — <c>duration_ms</c> and <c>relation_name</c> NULL, exactly as <c>PgLockWaitEventParser</c>
    /// writes today, so the read's message parse is what the e2e exercises.</summary>
    private static async Task PlantLockWaitLineAsync(NpgsqlConnection connection, DateTime at, string message, string? context, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_log_events
    (collection_id, collection_time, server_id, server_name, occurred_at, family, severity, sqlstate, database_name, user_name, application_name,
     pid, message, detail, context, statement_fingerprint, raw_line_hash, relation_name, duration_ms)
VALUES ($1, $2, $3, $4, $2, 'lock_wait', 'LOG', '00000', 'appdb', 'app', 'web',
        4102, $5, 'Process holding the lock: 9000. Wait queue: 4102.', $6, 'fp-orders-update', $7, NULL, NULL)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(message);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)context ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(Guid.NewGuid().ToString("N"));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_blocking_edges WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_log_events WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_session_states WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_server_config WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
