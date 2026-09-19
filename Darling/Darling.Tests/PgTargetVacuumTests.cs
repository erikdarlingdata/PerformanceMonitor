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
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The vacuum family of the PostgreSQL-target analysis pass (#3542 lane 4): <c>PG_AUTOVACUUM_BACKLOG</c>,
/// <c>PG_WRAPAROUND_TREND</c>, <c>PG_XMIN_HOLD</c> — scoring, amplifiers, chain and advice, ungated. The live
/// half (the collector's reads against a planted store, driven through the real <c>analyze_server</c>) is
/// <c>PgTargetVacuumLiveTests</c>.
///
/// <para><b>The D9 pin.</b> The wraparound and xmin bars are ONE definition (<see cref="PostgresOutagePredictorThresholds"/>)
/// that both the alert evaluator and the analysis scorer reference. Two pins hold it: a value pin that every
/// evaluator alias equals its shared symbol, and a SOURCE pin that neither <c>PostgresAlertEvaluator.cs</c>
/// nor <c>PgTargetScorer.Vacuum.cs</c> carries any of the literals in code — the aliasing is only worth
/// having if a future edit cannot quietly reintroduce a second copy of 0.745. A third pin walks a grid of
/// (age, setting, keeping-up) through BOTH graders and asserts the verdicts agree, so the two walks cannot
/// drift even if the constants stay shared.</para>
/// </summary>
public sealed class PgTargetVacuumTests
{
    /* ── D9: shared constants ── */

    [Fact]
    public void TheEvaluatorsOutagePredictorConstants_AreAliasesOfTheSharedDefinitions()
    {
        Assert.Equal(PostgresOutagePredictorThresholds.WraparoundWarningFractionOfFreezeMaxAge, PostgresAlertEvaluator.WraparoundWarningFractionOfFreezeMaxAge);
        Assert.Equal(PostgresOutagePredictorThresholds.WraparoundCriticalMultipleOfFreezeMaxAge, PostgresAlertEvaluator.WraparoundCriticalMultipleOfFreezeMaxAge);
        Assert.Equal(PostgresOutagePredictorThresholds.WraparoundCeiling, PostgresAlertEvaluator.WraparoundCeiling);
        Assert.Equal(PostgresOutagePredictorThresholds.WraparoundCriticalFractionOfCeiling, PostgresAlertEvaluator.WraparoundCriticalFractionOfCeiling);
        Assert.Equal(PostgresOutagePredictorThresholds.WraparoundWarningFractionOfCeiling, PostgresAlertEvaluator.WraparoundWarningFractionOfCeiling);
        Assert.Equal(PostgresOutagePredictorThresholds.XminAgeWarningThreshold, PostgresAlertEvaluator.XminAgeWarningThreshold);
        Assert.Equal(PostgresOutagePredictorThresholds.XminPersistenceFraction, PostgresAlertEvaluator.XminPersistenceFraction);
        Assert.Equal(PostgresOutagePredictorThresholds.XminMinimumObservations, PostgresAlertEvaluator.XminMinimumObservations);
        Assert.Equal(PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes, PostgresAlertEvaluator.SlotRetainedWalWarningBytes);
    }

    /// <summary>
    /// The literals live in exactly one file. Both consumers are scanned with comments and strings stripped,
    /// so the evaluator's prose ("74.5% of the space") and the scorer's doc comments do not count — only a
    /// retyped bar in CODE fails here. The evaluator's declarations are also pinned by shape: each of the
    /// eight shared names is <c>= PostgresOutagePredictorThresholds.&lt;same name&gt;</c>.
    /// </summary>
    [Fact]
    public void NeitherConsumer_RetypesASharedBar_AndTheEvaluatorDeclaresEachAsAnAlias()
    {
        var evaluator = RepoFile.ReadRepoFile("PerformanceMonitor.Alerting", "PostgresAlertEvaluator.cs");
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Vacuum.cs");
        var shared = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PostgresOutagePredictorThresholds.cs");

        /* The definitions ARE here (so the pin below is not satisfied by nobody having the number). */
        var sharedCode = CSharpSourceWalker.StripCommentsAndStrings(shared);
        foreach (var literal in new[] { "0.745", "50_000_000", "2_147_483_648L", "10L * 1024 * 1024 * 1024" })
            Assert.Contains(literal, sharedCode, StringComparison.Ordinal);

        foreach (var literal in new[] { "0.745", "50_000_000", "50000000", "2_147_483_648", "2147483648", "10L * 1024" })
        {
            Assert.DoesNotContain(literal, CSharpSourceWalker.StripCommentsAndStrings(evaluator), StringComparison.Ordinal);
            Assert.DoesNotContain(literal, CSharpSourceWalker.StripCommentsAndStrings(scorer), StringComparison.Ordinal);
        }

        var evaluatorCode = CSharpSourceWalker.StripCommentsAndStrings(evaluator);
        foreach (var name in new[]
        {
            "WraparoundWarningFractionOfFreezeMaxAge", "WraparoundCriticalMultipleOfFreezeMaxAge", "WraparoundCeiling",
            "WraparoundCriticalFractionOfCeiling", "WraparoundWarningFractionOfCeiling",
            "XminAgeWarningThreshold", "XminPersistenceFraction", "XminMinimumObservations", "SlotRetainedWalWarningBytes",
        })
        {
            Assert.Matches(
                new Regex(@"public\s+const\s+\w+\s+" + name + @"\s*=\s*PostgresOutagePredictorThresholds\." + name + @"\s*;"),
                evaluatorCode);
        }

        /* And the scorer grades on the shared symbols, not on its aliases' owner (its doc may NAME the
           evaluator — the comparison there is the point — so this reads code only). */
        var scorerCode = CSharpSourceWalker.StripCommentsAndStrings(scorer);
        Assert.Contains("PostgresOutagePredictorThresholds.", scorerCode, StringComparison.Ordinal);
        Assert.DoesNotContain("PostgresAlertEvaluator", scorerCode, StringComparison.Ordinal);
    }

    /// <summary>
    /// The two graders walk the same four arms in the same order. Every point on the grid — below the setting,
    /// at it with and without recovery, between the relative arms, past 2×, past half the ceiling on a cluster
    /// tuned so the relative arms sit beyond it, past <c>vacuum_failsafe_age</c> — yields the same verdict:
    /// the evaluator's null ↔ 0, Warning ↔ [0.5, 1), Critical ↔ 1.0. XID only (MultiXact setting 0 = not
    /// judgeable), because <see cref="PgTargetScorer.GradeWraparoundCounter"/> grades one counter and the
    /// evaluator's per-counter Grade is private — the public entry is how it is reached.
    /// </summary>
    [Theory]
    [InlineData(150_000_000L, 200_000_000L, true)]
    [InlineData(150_000_000L, 200_000_000L, false)]
    [InlineData(200_000_000L, 200_000_000L, true)]
    [InlineData(200_000_000L, 200_000_000L, false)]
    [InlineData(300_000_000L, 200_000_000L, true)]
    [InlineData(300_000_000L, 200_000_000L, false)]
    [InlineData(399_999_999L, 200_000_000L, false)]
    [InlineData(400_000_000L, 200_000_000L, true)]
    [InlineData(1_000_000_000L, 200_000_000L, true)]
    [InlineData(1_100_000_000L, 1_900_000_000L, true)]
    [InlineData(1_500_000_000L, 1_900_000_000L, true)]
    [InlineData(1_600_000_000L, 1_900_000_000L, true)]
    [InlineData(1_700_000_000L, 2_000_000_000L, true)]
    [InlineData(1L, 0L, false)]
    public void TheScorersWraparoundGrade_AgreesWithTheAlertEvaluatorsVerdict(long age, long setting, bool keepingUp)
    {
        /* keeping up = latest below the window peak (PostgresAlertInfo.XidFreezingIsKeepingUp). */
        var peak = keepingUp ? age + 1 : age;
        var alert = PostgresAlertEvaluator.EvaluateWraparound(
            new PostgresWraparoundAlertInfo("appdb", age, MultiXactAge: 0, setting, AutovacuumMultixactFreezeMaxAge: 0, WindowPeakXidAge: peak));

        var (severity, arm) = PgTargetScorer.GradeWraparoundCounter(age, setting, keepingUp);

        if (alert is null)
        {
            Assert.Equal(0.0, severity);
            Assert.Equal(0, arm);
        }
        else if (alert.Severity == AlertSeverityLevel.Critical)
        {
            Assert.Equal(1.0, severity);
            Assert.True(arm is 3 or 4, $"arm {arm}");
        }
        else
        {
            Assert.Equal(AlertSeverityLevel.Warning, alert.Severity);
            Assert.True(severity >= 0.5 && severity < 1.0, $"severity {severity}");
            Assert.True(arm is 1 or 2, $"arm {arm}");
        }
    }

    [Fact]
    public void TheWarningRamp_OrdersTwoWarningDatabasesTheWayTheAlertMessageReadsThem()
    {
        var (nearer, _) = PgTargetScorer.GradeWraparoundCounter(390_000_000, 200_000_000, freezingIsKeepingUp: false);
        var (justOver, _) = PgTargetScorer.GradeWraparoundCounter(201_000_000, 200_000_000, freezingIsKeepingUp: false);
        Assert.True(nearer > justOver);
        Assert.Equal(0.5, PgTargetScorer.GradeWraparoundCounter(200_000_000, 200_000_000, false).Severity, precision: 9);
    }

    /* ── PG_AUTOVACUUM_BACKLOG: the persistence gate and the engine-defined ratio ── */

    [Theory]
    [InlineData(2, 5.0, 0.0)]
    [InlineData(3, 1.0, 0.5)]
    [InlineData(3, 5.5, 0.75)]
    [InlineData(4, 10.0, 1.0)]
    [InlineData(5, 40.0, 1.0)]
    public void TheBacklogBase_IsZeroUnderThePersistenceGate_AndRampsFromTheEnginesOwnLine(int trailing, double ratio, double expected)
    {
        var fact = Backlog(ratio, trailing);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(3, PgTargetScorer.BacklogPersistenceSamples);
        /* The persistence gate and the critical multiple are fleet-measured (#3691, 2026-09-19); the concerning
           line is the engine's own — so a graded fact says every bar that decided is measured or engine-defined. */
        if (expected > 0)
            Assert.Equal(1, fact.Metadata["threshold_lineage"]);
    }

    [Fact]
    public void TheBacklogAmplifiers_ReadTheSlopeTheRunCountTheReloptionAndTheHold()
    {
        var losing = Backlog(5.0, 4, (PgTargetScorer.BacklogSlopePerHourKey, 1083), (PgTargetScorer.BacklogAutovacuumRunsKey, 2));
        var idle = Backlog(5.0, 4, (PgTargetScorer.BacklogSlopePerHourKey, 1083), (PgTargetScorer.BacklogAutovacuumRunsKey, 0));
        var disabled = Backlog(20.0, 5, (PgTargetScorer.BacklogTableAutovacuumDisabledKey, 1));

        new FactScorer().ScoreAll([losing]);
        new FactScorer().ScoreAll([idle]);
        new FactScorer().ScoreAll([disabled]);

        Assert.Contains(losing.AmplifierResults, a => a.Matched && a.Description.Contains("running and losing", StringComparison.Ordinal));
        Assert.DoesNotContain(idle.AmplifierResults, a => a.Matched && a.Description.Contains("running and losing", StringComparison.Ordinal));
        Assert.Contains(disabled.AmplifierResults, a => a.Matched && a.Description.Contains("autovacuum_enabled is OFF", StringComparison.Ordinal));
        Assert.True(disabled.Severity > disabled.BaseSeverity);

        var withHold = Backlog(5.0, 4);
        var hold = Xmin(60_000_000, held: 31, total: 41);
        new FactScorer().ScoreAll([withHold, hold]);
        Assert.Contains(withHold.AmplifierResults, a => a.Matched && a.Description.Contains("PG_XMIN_HOLD co-fired", StringComparison.Ordinal));
        Assert.Contains(hold.AmplifierResults, a => a.Matched && a.Description.Contains("PG_AUTOVACUUM_BACKLOG co-fired", StringComparison.Ordinal));
    }

    /* ── PG_XMIN_HOLD: the alert's identity arm, on the shared bars ── */

    [Theory]
    [InlineData(60_000_000L, 31, 41, 0.5)]   /* majority over ≥ 5 observations, at the bar: the alert's Warning */
    [InlineData(60_000_000L, 20, 41, 0.0)]   /* under the majority: transient */
    [InlineData(60_000_000L, 3, 4, 0.0)]     /* 75% of four observations: under the floor (#3537's false fire) */
    [InlineData(49_999_999L, 31, 41, 0.0)]   /* under the age bar */
    [InlineData(60_000_000L, 0, 0, 0.0)]     /* no observations */
    public void TheXminBase_IsTheAlertsIdentityArm(long age, int held, int total, double expected)
    {
        var fact = Xmin(age, held, total);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(expected > 0 ? 1 : 0, fact.Metadata[PgTargetScorer.XminIdentityArmKey]);
    }

    [Fact]
    public void TheXminRamp_TopsOutAtTheServersOwnFreezeMaxAge_AndStaysFlatWithoutOne()
    {
        var flat = Xmin(150_000_000, 31, 41);
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(flat), precision: 9);

        var halfway = Xmin(125_000_000, 31, 41, (PgTargetScorer.XminFreezeMaxAgeKey, 200_000_000));
        Assert.Equal(0.75, PgTargetScorer.ScoreBase(halfway), precision: 9);

        var atFreeze = Xmin(200_000_000, 31, 41, (PgTargetScorer.XminFreezeMaxAgeKey, 200_000_000));
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(atFreeze), precision: 9);

        /* A setting at or under the warning bar cannot be a top — flat, not a division by a tiny span. */
        var degenerate = Xmin(60_000_000, 31, 41, (PgTargetScorer.XminFreezeMaxAgeKey, 40_000_000));
        Assert.Equal(0.5, PgTargetScorer.ScoreBase(degenerate), precision: 9);
    }

    [Theory]
    [InlineData("session", 1)]
    [InlineData("replication_slot", 2)]
    [InlineData("replication_slot_catalog", 3)]
    [InlineData("standby_feedback", 4)]
    [InlineData("prepared_transaction", 5)]
    [InlineData("Session", 1)]
    [InlineData("something_new", 0)]
    [InlineData(null, 0)]
    public void TheHolderSourceEncoding_RoundTrips(string? source, int code)
    {
        Assert.Equal(code, PgTargetAdvice.HolderSourceCode(source));
        Assert.Equal(code == 0 ? "unknown" : source!.ToLowerInvariant(), PgTargetAdvice.HolderSourceName(code));
        Assert.Equal("unknown", PgTargetAdvice.HolderSourceName(99));
    }

    /* ── the chain: one incident when all three fire ── */

    /// <summary>
    /// The mesh, walked by the engine: the highest amplified severity roots, and the walk reaches both others.
    /// Arranged so the wraparound leads (0.55 × (1 + 0.5 hold + 0.3 backlog) = 0.99) over the backlog (0.72 ×
    /// 1.3 = 0.94) over the hold (0.5 × 1.8 = 0.90) — the canonical symptom-first order. The live e2e arranges
    /// the backlog to lead instead and pins THAT order, so both facts about the mesh are held: one story, and
    /// a path order that follows severity.
    /// </summary>
    [Fact]
    public void AllThreeFacts_WalkOneStory_AndTheWalkFollowsSeverity()
    {
        var wraparound = Wraparound(220_000_000, 200_000_000, keepingUp: false);
        var backlog = Backlog(5.0, 4);
        var hold = Xmin(60_000_000, 31, 41);
        var facts = new List<Fact> { wraparound, backlog, hold };
        new FactScorer().ScoreAll(facts);
        Assert.All(facts, f => Assert.True(f.Severity >= 0.5, $"{f.Key} {f.Severity}"));
        Assert.Equal(0.99, wraparound.Severity, precision: 6);
        Assert.True(wraparound.Severity > backlog.Severity && backlog.Severity > hold.Severity);

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);

        var story = Assert.Single(stories);
        Assert.Equal(PgTargetFactKeys.WraparoundTrend, story.RootFactKey);
        Assert.Equal(
            new[] { PgTargetFactKeys.WraparoundTrend, PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.XminHold },
            story.Path);
    }

    /// <summary>
    /// The failure the mesh exists to prevent, kept as a pin: when the HOLD leads (a backlog with a rising
    /// slope lifts it, and nothing lifts the wraparound), a one-directional chain rooted at the wraparound
    /// would leave the hold a story of its own. Here the hold roots and still walks the other two.
    /// </summary>
    [Fact]
    public void WhenTheHoldLeads_ItRootsTheOneStory_AndStillReachesTheOtherTwo()
    {
        var wraparound = Wraparound(200_000_000, 200_000_000, keepingUp: false);   /* exactly at the bar: 0.5 */
        var backlog = Backlog(1.0, 3);                                             /* exactly at its line: 0.5 */
        var hold = Xmin(190_000_000, 31, 41, (PgTargetScorer.XminFreezeMaxAgeKey, 200_000_000));
        var facts = new List<Fact> { wraparound, backlog, hold };
        new FactScorer().ScoreAll(facts);
        Assert.True(hold.Severity > wraparound.Severity && hold.Severity > backlog.Severity, $"{hold.Severity} {wraparound.Severity} {backlog.Severity}");

        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(PgTargetFactKeys.XminHold, story.RootFactKey);
        Assert.Equal(3, story.Path.Count);
        Assert.Contains(PgTargetFactKeys.WraparoundTrend, story.Path);
        Assert.Contains(PgTargetFactKeys.AutovacuumBacklog, story.Path);
    }

    [Fact]
    public void WithoutABacklog_TheWraparoundStillReachesTheHold_AndABacklogAloneRootsItsOwnStory()
    {
        var wraparound = Wraparound(220_000_000, 200_000_000, keepingUp: false);
        var hold = Xmin(60_000_000, 31, 41);
        var two = new List<Fact> { wraparound, hold };
        new FactScorer().ScoreAll(two);
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(two));
        Assert.Equal(new[] { PgTargetFactKeys.WraparoundTrend, PgTargetFactKeys.XminHold }, story.Path);

        var alone = new List<Fact> { Backlog(5.0, 4) };
        new FactScorer().ScoreAll(alone);
        var solo = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(alone));
        Assert.Equal(new[] { PgTargetFactKeys.AutovacuumBacklog }, solo.Path);
    }

    [Fact]
    public void AHealthySawtoothAndATransientHolder_ScoreZero_AndBuildNoStory()
    {
        var facts = new List<Fact>
        {
            Wraparound(150_000_000, 200_000_000, keepingUp: true),
            Xmin(60_000_000, held: 2, total: 41),
        };
        new FactScorer().ScoreAll(facts);
        Assert.All(facts, f => Assert.Equal(0.0, f.Severity));
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);
        Assert.Equal("server_health", Assert.Single(stories).RootFactKey);
    }

    /* ── advice: value-stated, counter-objective, never "disable autovacuum", holder named per source ── */

    [Fact]
    public void TheBacklogAdvice_StatesTheTablesOwnNumbers_TheSlope_AndTheRunCount_AndNeverSuggestsSwitchingAutovacuumOff()
    {
        var fact = Backlog(5.0, 4,
            (PgTargetScorer.BacklogDeadTuplesKey, 5250), (PgTargetScorer.BacklogVacuumThresholdKey, 1050),
            (PgTargetScorer.BacklogLiveTuplesKey, 10_000), (PgTargetScorer.BacklogHoursKey, 3),
            (PgTargetScorer.BacklogSlopePerHourKey, 1083.3), (PgTargetScorer.BacklogSlopeComputableKey, 1),
            (PgTargetScorer.BacklogAutovacuumRunsKey, 2), (PgTargetScorer.BacklogRunsComputableKey, 1),
            (PgTargetScorer.BacklogTablesKey, 3), (PgTargetScorer.BacklogHoursSinceLastAutovacuumKey, 0.33));
        fact.ObjectName = "public.hot";
        fact.DatabaseName = "appdb";
        new FactScorer().ScoreAll([fact]);

        var advice = FactAdvice.Compose(PgTargetFactKeys.AutovacuumBacklog, Lookup(fact))!;

        Assert.Contains("public.hot in appdb", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("5,250 dead tuples, 5× its own autovacuum trigger line, for 4 consecutive hourly samples", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("trigger line of 1,050", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("rose at 1,083 per hour", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("autovacuum ran on the table 2 times", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("running and LOSING", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 other tables are persistently past their line", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE public.hot SET (autovacuum_vacuum_cost_delay = 0)", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE public.hot SET (autovacuum_vacuum_scale_factor = 0.053)", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("Counter-objective", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("Never switch autovacuum off", advice.Remediation, StringComparison.Ordinal);
        AssertNeverDisablesAutovacuum(advice);
        Assert.Null(advice.RemediationTsql);
    }

    [Fact]
    public void TheBacklogAdvice_SaysWhenTheSlopeAndTheRunCountAreNotComputable_AndNamesTheInsertArm()
    {
        var fact = Backlog(2.38, 3,
            (PgTargetScorer.BacklogArmIsInsertKey, 1), (PgTargetScorer.BacklogInsertsSinceVacuumKey, 50_000),
            (PgTargetScorer.BacklogInsertThresholdKey, 21_000), (PgTargetScorer.BacklogLiveTuplesKey, 100_000),
            (PgTargetScorer.BacklogSlopeComputableKey, 0), (PgTargetScorer.BacklogRunsComputableKey, 0));
        fact.ObjectName = "public.appendonly";
        var advice = FactAdvice.Compose(PgTargetFactKeys.AutovacuumBacklog, Lookup(fact))!;

        Assert.Contains("50,000 inserts since its last vacuum, 2.4× its insert-vacuum threshold", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("slope across the run is not computable", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("statistics reset", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("never vacuumed is never frozen", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("autovacuum_vacuum_insert_scale_factor", advice.Remediation, StringComparison.Ordinal);
        AssertNeverDisablesAutovacuum(advice);
    }

    [Fact]
    public void TheWraparoundAdvice_DistinguishesRoutineFromEmergency_AndStatesTimeToWallOrThatItIsNotComputable()
    {
        var routine = Wraparound(220_000_000, 200_000_000, keepingUp: false,
            (PgTargetScorer.WraparoundSlopePerHourKey, 5_000_000), (PgTargetScorer.WraparoundHoursToWallKey, 385.5),
            (PgTargetScorer.WraparoundTimeToWallComputableKey, 1), (PgTargetScorer.WraparoundXidsRemainingKey, 1_927_483_648));
        routine.DatabaseName = "appdb";
        new FactScorer().ScoreAll([routine]);
        var r = FactAdvice.Compose(PgTargetFactKeys.WraparoundTrend, Lookup(routine))!;
        Assert.Contains("XID age 220,000,000 in appdb has reached autovacuum_freeze_max_age (200,000,000) and has not come back down", r.Headline, StringComparison.Ordinal);
        Assert.Contains("ROUTINE crossing", r.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("EMERGENCY", r.Headline, StringComparison.Ordinal);
        Assert.Contains("5,000,000 per hour the wall is roughly 16.1 days away", r.Investigation, StringComparison.Ordinal);
        Assert.Contains("Do NOT raise autovacuum_freeze_max_age", r.Remediation, StringComparison.Ordinal);
        Assert.Contains("Never disable autovacuum", r.Remediation, StringComparison.Ordinal);
        AssertNeverDisablesAutovacuum(r);

        var emergency = Wraparound(1_700_000_000, 200_000_000, keepingUp: false,
            (PgTargetScorer.WraparoundSlopePerHourKey, -1000), (PgTargetScorer.WraparoundTimeToWallComputableKey, 0));
        emergency.DatabaseName = "appdb";
        new FactScorer().ScoreAll([emergency]);
        var e = FactAdvice.Compose(PgTargetFactKeys.WraparoundTrend, Lookup(emergency))!;
        Assert.Contains("past vacuum_failsafe_age; EMERGENCY", e.Headline, StringComparison.Ordinal);
        Assert.Contains("EMERGENCY, not routine", e.Remediation, StringComparison.Ordinal);
        Assert.Contains("VACUUM (FREEZE", e.Remediation, StringComparison.Ordinal);
        Assert.Contains("Time-to-wall is not computable: the age fell or held", e.Investigation, StringComparison.Ordinal);
        AssertNeverDisablesAutovacuum(e);

        var multi = Wraparound(220_000_000, 200_000_000, keepingUp: false);
        multi.Metadata[PgTargetScorer.WraparoundCounterIsMultiXactKey] = 1;
        multi.Metadata[PgTargetScorer.WraparoundMultiXactAgeKey] = 900_000_000;
        multi.Metadata[PgTargetScorer.WraparoundMultiXactFreezeMaxAgeKey] = 400_000_000;
        multi.Metadata[PgTargetScorer.WraparoundMultiXactPeakKey] = 900_000_000;
        multi.Metadata[PgTargetScorer.WraparoundArmKey] = 3;
        var m = FactAdvice.Compose(PgTargetFactKeys.WraparoundTrend, Lookup(multi))!;
        Assert.Contains("MultiXact age 900,000,000", m.Headline, StringComparison.Ordinal);
        Assert.Contains("autovacuum_multixact_freeze_max_age", m.Headline, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("session", "pg_terminate_backend")]
    [InlineData("replication_slot", "pg_drop_replication_slot")]
    [InlineData("replication_slot_catalog", "catalog_xmin")]
    [InlineData("standby_feedback", "hot_standby_feedback = off")]
    [InlineData("prepared_transaction", "COMMIT PREPARED or ROLLBACK PREPARED")]
    [InlineData("unknown", "pg_prepared_xacts directly")]
    public void TheXminAdvice_NamesTheFixForTheHolderKind(string source, string expectedRemedy)
    {
        var fact = Xmin(60_000_000, 31, 41);
        fact.Metadata[PgTargetScorer.XminHolderSourceKey] = PgTargetAdvice.HolderSourceCode(source);
        fact.ObjectName = source == "unknown" ? "mystery:42" : $"{source}:4242";
        new FactScorer().ScoreAll([fact]);

        var advice = FactAdvice.Compose(PgTargetFactKeys.XminHold, Lookup(fact))!;

        Assert.Contains("holding the xmin horizon 60,000,000 transactions back, in 31 of 41 holder-bearing collections", advice.Headline, StringComparison.Ordinal);
        Assert.Contains(expectedRemedy, advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("Counter-objective", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("the alert's majority standard is 50% over at least 5 observations", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("No action on this evidence alone", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ATransientHolder_IsNamedAsContext_NotAsAFinding()
    {
        var fact = Xmin(60_000_000, held: 2, total: 41);
        fact.ObjectName = "session:4242";
        new FactScorer().ScoreAll([fact]);
        var advice = FactAdvice.Compose(PgTargetFactKeys.XminHold, Lookup(fact))!;
        Assert.Contains("a transient holder, not a chronic one", advice.Headline, StringComparison.Ordinal);
        Assert.StartsWith("No action on this evidence alone", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheStaticBlocks_ExistForEveryVacuumKey_AndCarryNoDdlAndNoDisableAdvice()
    {
        foreach (var key in new[]
        {
            PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.WraparoundTrend, PgTargetFactKeys.XminHold,
            PgTargetFactKeys.ConfigAutovacuumOff, PgTargetFactKeys.ConfigMaintWorkMem,
            PgTargetFactKeys.ConfigAutovacuumDisabled,   /* #3691 step 22 */
        })
        {
            var block = PgTargetAdvice.Static(key);
            Assert.NotNull(block);
            Assert.False(string.IsNullOrWhiteSpace(block!.Headline));
            Assert.DoesNotContain("CREATE INDEX", block.Investigation + block.Remediation, StringComparison.OrdinalIgnoreCase);
            AssertNeverDisablesAutovacuum(block);
            Assert.Null(block.RemediationTsql);
        }
    }

    [Fact]
    public void TheConfigCoFireBlocks_NameTheBacklogTable_WhenItFired()
    {
        var backlog = Backlog(5.0, 4, (PgTargetScorer.BacklogDeadTuplesKey, 5250));
        backlog.ObjectName = "public.hot";
        var off = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigAutovacuumOff, Value = 1, ServerId = 1 };
        var maint = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigMaintWorkMem, Value = 64, ServerId = 1 };
        new FactScorer().ScoreAll([backlog, off, maint]);

        var offAdvice = FactAdvice.Compose(PgTargetFactKeys.ConfigAutovacuumOff, Lookup(backlog, off, maint))!;
        Assert.Contains("public.hot is 5× past its own trigger line because of it", offAdvice.Headline, StringComparison.Ordinal);
        var maintAdvice = FactAdvice.Compose(PgTargetFactKeys.ConfigMaintWorkMem, Lookup(backlog, off, maint))!;
        Assert.Contains("public.hot's 5,250 dead tuples", maintAdvice.Headline, StringComparison.Ordinal);
        Assert.Contains("30.8 kB", maintAdvice.Investigation, StringComparison.Ordinal);
    }

    /* ── CONFIG_PG_AUTOVACUUM_DISABLED (#3691 step 22, design §3.1) ── */

    /// <summary>
    /// The card's two gates are the backlog's own: under the persistence gate it is 0 (a hand-built fact, or a
    /// window too short to hold three samples), under the table's own line it is 0, and past both it is the
    /// flat 0.9 band with <c>threshold_lineage = 1</c> — the engine's line and the measured gate, by name.
    /// "Disabled but quiet" and "enabled but backlogged" never reach the scorer at all: the read's WHERE is
    /// what excludes them, pinned by text below.
    /// </summary>
    [Theory]
    [InlineData(2, 4.2, 0.0)]
    [InlineData(3, 0.8, 0.0)]
    [InlineData(3, 1.0, 0.9)]
    [InlineData(6, 4.2, 0.9)]
    [InlineData(6, 350.0, 0.9)]
    public void TheDisabledCard_IsTheFlatBand_PastThePersistenceGateAndTheTablesOwnLine_AndZeroUnderEither(int trailing, double ratio, double expected)
    {
        var fact = Disabled(ratio, trailing, hours: trailing - 1);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(fact), precision: 9);
        Assert.Equal(0.9, PgTargetScorer.AutovacuumDisabledBaseSeverity);
        if (expected > 0)
            Assert.Equal(1, fact.Metadata["threshold_lineage"]);
        else
            Assert.False(fact.Metadata.ContainsKey("threshold_lineage"));

        /* No amplifier arm: the shared dispatcher's CONFIG_PG_ prefix carries the key to ConfigAmplifiers' empty
           list, and the backlog fact already carries the reloption boost for the same table. */
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(fact.BaseSeverity, fact.Severity);
        Assert.Empty(fact.AmplifierResults);
    }

    /// <summary>
    /// The disabled read is the backlog read's CTE chain and projection with a different tail: the SAME
    /// persistence parameter (<c>$4</c>), a <c>WHERE</c> that adds only the reloption, no disabled-first ORDER
    /// (every row is disabled). The backlog read keeps its shape byte-for-byte (its live pins depend on it)
    /// and stays reloption-agnostic — an ENABLED backlogged table is that fact's business, not this one's.
    /// </summary>
    [Fact]
    public void TheDisabledRead_SharesTheBacklogReadsRuns_AndAddsOnlyTheReloptionFilter()
    {
        /* The source file is CRLF, so the verbatim literals carry \r\n; compare on one line-ending shape. */
        var backlog = PgTargetFactCollector.PgTargetAutovacuumBacklogSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var disabled = PgTargetFactCollector.PgTargetAutovacuumDisabledSql.Replace("\r\n", "\n", StringComparison.Ordinal);

        var backlogTail = backlog.LastIndexOf("\nWHERE ", StringComparison.Ordinal);
        var disabledTail = disabled.LastIndexOf("\nWHERE ", StringComparison.Ordinal);
        Assert.True(backlogTail > 0 && disabledTail > 0);
        Assert.Equal(backlog[..backlogTail], disabled[..disabledTail]);

        Assert.Contains("WHERE l.autovacuum_disabled\nAND   l.trailing_samples_past_line >= $4", disabled, StringComparison.Ordinal);
        Assert.DoesNotContain("WHERE l.autovacuum_disabled", backlog, StringComparison.Ordinal);
        Assert.Contains("WHERE l.trailing_samples_past_line >= $4", backlog, StringComparison.Ordinal);
        Assert.Contains("l.autovacuum_disabled DESC", backlog, StringComparison.Ordinal);
        Assert.DoesNotContain("l.autovacuum_disabled DESC", disabled, StringComparison.Ordinal);
        Assert.EndsWith("LIMIT $5", disabled, StringComparison.Ordinal);
        Assert.Contains(PgTargetFactCollector.PgTargetAutovacuumDisabledSql, PgTargetFactCollector.AllSql);
    }

    /// <summary>
    /// The edge is gated on the two facts naming the SAME table. Same table: one story, backlog-led (its
    /// reloption amplifier lifts it to the cap) with the disabled card as the named cause. A different table:
    /// two findings — a disabled table and an unrelated backlog share no hop, and the 0.9 card roots on its own.
    /// </summary>
    [Fact]
    public void TheDisabledCard_JoinsTheBacklogsStory_OnlyWhenBothNameTheSameTable()
    {
        var backlog = Backlog(4.2, 6, (PgTargetScorer.BacklogTableAutovacuumDisabledKey, 1));
        backlog.DatabaseName = "appdb";
        backlog.ObjectName = "public.hot";
        var same = Disabled(4.2, 6, hours: 6);
        var facts = new List<Fact> { backlog, same };
        new FactScorer().ScoreAll(facts);
        /* 4.2× grades 0.68, the reloption boost (+0.5) lifts it past the card's flat 0.9: the backlog leads. */
        Assert.True(backlog.Severity > same.Severity, $"{backlog.Severity} {same.Severity}");
        Assert.Equal(0.9, same.Severity);

        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(new[] { PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.ConfigAutovacuumDisabled }, story.Path);
        Assert.Equal("vacuum_starvation", Assert.Single(new PgTargetRelationshipGraph().GetActiveEdges(PgTargetFactKeys.AutovacuumBacklog, Lookup(backlog, same))).Category);

        var elsewhere = Disabled(4.2, 6, hours: 6);
        elsewhere.ObjectName = "public.other";
        var split = new List<Fact> { Backlog(4.2, 6, (PgTargetScorer.BacklogTableAutovacuumDisabledKey, 1)), elsewhere };
        split[0].DatabaseName = "appdb";
        split[0].ObjectName = "public.hot";
        new FactScorer().ScoreAll(split);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(split);
        Assert.Equal(2, stories.Count);
        Assert.Contains(stories, s => s.Path.SequenceEqual([PgTargetFactKeys.AutovacuumBacklog]));
        Assert.Contains(stories, s => s.Path.SequenceEqual([PgTargetFactKeys.ConfigAutovacuumDisabled]));

        /* Same schema.table in a different database is a different table. */
        var otherDb = Disabled(4.2, 6, hours: 6);
        otherDb.DatabaseName = "otherdb";
        Assert.Empty(new PgTargetRelationshipGraph().GetActiveEdges(PgTargetFactKeys.ConfigAutovacuumDisabled, Lookup(backlog, otherDb)));
    }

    /// <summary>The server-wide switch and the reloption ride in ONE story with the backlog. Severities after
    /// amplification: the backlog 0.68 × (1 + 0.5 reloption + 0.5 server-off co-fire) = 1.356 leads by a hair
    /// over <c>autovacuum = off</c> at its 0.9 posture band × 1.5 backlog co-fire = 1.35, then the flat card at
    /// 0.9 — so the walk is backlog → off → disabled, the last hop being the off → disabled edge this step
    /// added. Pinned as an order because the mesh's promise is that the PATH follows severity; a different
    /// leader (the executed pins found the off setting a whisker behind) would still reach all three.</summary>
    [Fact]
    public void WhenAutovacuumIsOffServerWideToo_TheWalkCarriesBothSettingsIntoOneStory()
    {
        var backlog = Backlog(4.2, 6, (PgTargetScorer.BacklogTableAutovacuumDisabledKey, 1));
        backlog.DatabaseName = "appdb";
        backlog.ObjectName = "public.hot";
        var disabled = Disabled(4.2, 6, hours: 6, (PgTargetScorer.AutovacuumDisabledServerOffKey, 1));
        var off = new Fact { Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ConfigAutovacuumOff, Value = 1, ServerId = 1 };
        var facts = new List<Fact> { backlog, disabled, off };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(1.35, off.Severity, precision: 6);
        Assert.True(backlog.Severity > off.Severity && off.Severity > disabled.Severity, $"{backlog.Severity} {off.Severity} {disabled.Severity}");

        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(
            new[] { PgTargetFactKeys.AutovacuumBacklog, PgTargetFactKeys.ConfigAutovacuumOff, PgTargetFactKeys.ConfigAutovacuumDisabled },
            story.Path);

        var advice = FactAdvice.Compose(PgTargetFactKeys.ConfigAutovacuumDisabled, Lookup(backlog, disabled, off))!;
        Assert.Contains("CONFIG_PG_AUTOVACUUM_OFF also fired", advice.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("Turn autovacuum back on server-wide first", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDisabledAdvice_StatesTheTableTheRatioTheHoursAndTheShapeOfTheOthers_AndOffersBothLeversWithTheirCosts()
    {
        var disabled = Disabled(4.2, 7, hours: 6,
            (PgTargetScorer.BacklogDeadTuplesKey, 4_410), (PgTargetScorer.BacklogVacuumThresholdKey, 1_050), (PgTargetScorer.BacklogLiveTuplesKey, 10_000),
            (PgTargetScorer.BacklogTotalBytesKey, 8_192_000), (PgTargetScorer.BacklogHoursSinceLastAutovacuumKey, 30),
            (PgTargetScorer.AutovacuumDisabledTablesKey, 3),
            (PgTargetScorer.AutovacuumDisabledRankRatioKey(2), 3.1), (PgTargetScorer.AutovacuumDisabledRankHoursKey(2), 5),
            (PgTargetScorer.AutovacuumDisabledRankRatioKey(3), 1.4), (PgTargetScorer.AutovacuumDisabledRankHoursKey(3), 2));
        var backlog = Backlog(4.2, 7, (PgTargetScorer.BacklogTableAutovacuumDisabledKey, 1));
        backlog.DatabaseName = "appdb";
        backlog.ObjectName = "public.hot";
        new FactScorer().ScoreAll([backlog, disabled]);

        var block = FactAdvice.Compose(PgTargetFactKeys.ConfigAutovacuumDisabled, Lookup(backlog, disabled))!;
        Assert.Equal("public.hot in appdb has autovacuum_enabled = off and sits at 4.2× its own autovacuum trigger line for 6 hours", block.Headline);
        Assert.Contains("4,410 dead tuples against its own trigger line of 1,050", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("for 7 consecutive hourly samples spanning 6 hours", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("autovacuum last ran on this table 30 hours before the window end", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("7.8 MB on disk", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("2 more disabled tables have met the same gate (3.1× for 5 hours, 1.4× for 2 hours)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("PG_AUTOVACUUM_BACKLOG carries the grade", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("CONFIG_PG_AUTOVACUUM_OFF also fired", block.Investigation, StringComparison.Ordinal);

        Assert.Contains("ALTER TABLE public.hot SET (autovacuum_enabled = true);", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("VACUUM (ANALYZE) public.hot;", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("Never VACUUM FULL as the first lever", block.Remediation, StringComparison.Ordinal);
        Assert.Contains("autovacuum_vacuum_scale_factor = 0.053", block.Remediation, StringComparison.Ordinal);
        /* Every lever names its cost: re-enable, manual schedule, and the lower-line variant. */
        Assert.Equal(3, Regex.Matches(block.Remediation, "Counter-objective").Count);
        Assert.DoesNotContain("CREATE INDEX", block.Investigation + block.Remediation, StringComparison.OrdinalIgnoreCase);
        AssertNeverDisablesAutovacuum(block);
        Assert.Null(block.RemediationTsql);

        /* The insert arm names its own line and no scale-factor statement for the dead-tuple line. */
        var appendOnly = Disabled(2.5, 3, hours: 2, (PgTargetScorer.BacklogArmIsInsertKey, 1),
            (PgTargetScorer.BacklogInsertsSinceVacuumKey, 52_500), (PgTargetScorer.BacklogInsertThresholdKey, 21_000), (PgTargetScorer.BacklogLiveTuplesKey, 100_000));
        var insert = FactAdvice.Compose(PgTargetFactKeys.ConfigAutovacuumDisabled, Lookup(appendOnly))!;
        Assert.Contains("2.5× its own insert-vacuum line for 2 hours", insert.Headline, StringComparison.Ordinal);
        Assert.Contains("52,500 inserts since its last vacuum against its own insert line of 21,000", insert.Investigation, StringComparison.Ordinal);
        Assert.Contains("never run on this table", insert.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("autovacuum_vacuum_scale_factor", insert.Remediation, StringComparison.Ordinal);

        /* Next reads: the per-table list and what it cost, get_pg_ only. */
        var tools = PerformanceMonitor.Darling.Service.Mcp.PgTargetToolRecommendations.GetForKey(PgTargetFactKeys.ConfigAutovacuumDisabled)!;
        Assert.Equal(new[] { "get_pg_autovacuum_health", "get_pg_table_bloat" }, tools.Select(t => t.Tool));
    }

    /* ── helpers ── */

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts) => facts.ToFactLookup();

    private static Fact Disabled(double ratio, int trailing, double hours, params (string Key, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.VacuumSource,
            Key = PgTargetFactKeys.ConfigAutovacuumDisabled,
            Value = ratio,
            ServerId = 1,
            DatabaseName = "appdb",
            ObjectName = "public.hot",
            Metadata =
            {
                [PgTargetScorer.BacklogRatioKey] = ratio,
                [PgTargetScorer.BacklogTrailingSamplesKey] = trailing,
                [PgTargetScorer.BacklogSamplesInWindowKey] = trailing + 1,
                [PgTargetScorer.BacklogHoursKey] = hours,
                [PgTargetScorer.BacklogTableAutovacuumDisabledKey] = 1,
                [PgTargetScorer.AutovacuumDisabledTablesKey] = 1,
                [PgTargetScorer.AutovacuumDisabledServerOffKey] = 0,
            },
        };
        foreach (var (k, v) in extra) fact.Metadata[k] = v;
        return fact;
    }

    private static void AssertNeverDisablesAutovacuum(AdviceBlock block)
    {
        /* The prohibition, as a pin: no block ever RECOMMENDS disabling autovacuum — per table (the reloption
           set false) or server-wide (set / turn / switch it off) — while "never switch autovacuum off" and the
           descriptive "autovacuum = off in pg_settings" are allowed to appear, because saying so is the point. */
        var text = block.Investigation + " " + block.Remediation;
        Assert.DoesNotContain("autovacuum_enabled = false", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotMatch(new Regex(@"(?<!\b(?:never|not)\s)\b(?:set|turn|switch)\s+autovacuum\s*(?:=\s*)?off\b", RegexOptions.IgnoreCase), text);
    }

    private static Fact Backlog(double ratio, int trailing, params (string Key, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.VacuumSource,
            Key = PgTargetFactKeys.AutovacuumBacklog,
            Value = ratio,
            ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.BacklogRatioKey] = ratio,
                [PgTargetScorer.BacklogTrailingSamplesKey] = trailing,
                [PgTargetScorer.BacklogSamplesInWindowKey] = 5,
                [PgTargetScorer.BacklogTablesKey] = 1,
            },
        };
        foreach (var (k, v) in extra) fact.Metadata[k] = v;
        return fact;
    }

    private static Fact Wraparound(long xidAge, long setting, bool keepingUp, params (string Key, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.VacuumSource,
            Key = PgTargetFactKeys.WraparoundTrend,
            Value = xidAge,
            ServerId = 1,
            Metadata =
            {
                [PgTargetScorer.WraparoundXidAgeKey] = xidAge,
                [PgTargetScorer.WraparoundFreezeMaxAgeKey] = setting,
                [PgTargetScorer.WraparoundXidPeakKey] = keepingUp ? xidAge + 1 : xidAge,
                [PgTargetScorer.WraparoundXidKeepingUpKey] = keepingUp ? 1 : 0,
                [PgTargetScorer.WraparoundMultiXactAgeKey] = 1000,
                [PgTargetScorer.WraparoundMultiXactFreezeMaxAgeKey] = 400_000_000,
                [PgTargetScorer.WraparoundMultiXactPeakKey] = 1000,
                [PgTargetScorer.WraparoundMultiXactKeepingUpKey] = 0,
                [PgTargetScorer.WraparoundArmKey] = PgTargetScorer.GradeWraparoundCounter(xidAge, setting, keepingUp).Arm,
            },
        };
        foreach (var (k, v) in extra) fact.Metadata[k] = v;
        return fact;
    }

    private static Fact Xmin(long age, int held, int total, params (string Key, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.VacuumSource,
            Key = PgTargetFactKeys.XminHold,
            Value = age,
            ServerId = 1,
            ObjectName = "session:4242",
            Metadata =
            {
                [PgTargetScorer.XminAgeKey] = age,
                [PgTargetScorer.XminHolderSourceKey] = 1,
                [PgTargetScorer.XminObservationsTotalKey] = total,
                [PgTargetScorer.XminObservationsHeldKey] = held,
                [PgTargetScorer.XminObservationsAboveThresholdKey] = total,
                [PgTargetScorer.XminMinutesSinceLastHolderKey] = 0,
            },
        };
        foreach (var (k, v) in extra) fact.Metadata[k] = v;
        return fact;
    }
}
