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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The replication family of the PostgreSQL-target analysis pass (#3691 lane 12): <c>PG_REPLICATION_LAG</c>,
/// <c>PG_SLOT_RETENTION</c>, <c>PG_SLOT_XMIN</c> and the <c>ANOMALY_PG_REPLICATION_LAG</c> detector — scoring,
/// amplifiers, chain, advice, the detector's and baseline's SQL shape, ungated; and, gated on
/// <c>DARLING_TEST_PG</c>, the exit criterion against a real store through the REAL <c>analyze_server</c>.
///
/// <para><b>The D9 pin, replication edition.</b> The slot bar is ONE symbol
/// (<see cref="PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes"/>) that the alert evaluator, the slot
/// fact, the lag fact's absolute arm and the anomaly detector's fallback all REFERENCE. Three pins hold it: a value
/// pin that every alias equals the shared symbol; a SOURCE pin that <c>PgTargetScorer.Replication.cs</c> carries none
/// of the literals in code and declares its aliases as <c>= PostgresOutagePredictorThresholds.…</c>; and a grid that
/// walks (retained, wal_status, active, growth) through BOTH graders and asserts the verdicts agree.</para>
///
/// <para><b>What the fleet measured, and so what is pinned as unmeasured.</b> One standby (steady, under a
/// megabyte, <c>replay_lag_ms</c> NULL on every row) and one inactive logical slot (reserved, ~1.5 MB) — n = 1, so
/// the drift multiple is unmeasured and the fact carries <c>threshold_lineage = 0</c> whenever that arm was
/// consulted; the NULL-milliseconds shape is the norm the scorer and advice are pinned to tolerate.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetReplicationTests
{
    /* ── D9: shared constants ── */

    [Fact]
    public void TheSlotBar_IsSharedByIdentity_TheLagArmsAndTheDetectorFallbackAlias_AndTheFloorIsOneWalSegment()
    {
        Assert.Equal(PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes, PostgresAlertEvaluator.SlotRetainedWalWarningBytes);
        Assert.Equal(PostgresOutagePredictorThresholds.SlotRetainedWalWarningBytes, PgTargetScorer.PgReplayLagBytesFallback);
        Assert.Equal(PgTargetScorer.ReplayLagNoiseFloorBytes, PgTargetScorer.PgReplayLagBytesFloor);
        Assert.True(PgTargetScorer.PgReplayLagBytesFallback > PgTargetScorer.PgReplayLagBytesFloor);
        Assert.True(PgTargetScorer.ReplayLagDriftCriticalMultiple > PgTargetScorer.ReplayLagDriftMultiple);

        /* Not a SQL Server constant by value: the SQL Server anomaly floors are counts and rates, not bytes. */
        Assert.NotEqual(AnomalyThresholds.SessionCountFloor, PgTargetScorer.PgReplayLagBytesFloor);
        Assert.NotEqual(AnomalyThresholds.BatchRequestFloor, PgTargetScorer.PgReplayLagBytesFloor);
    }

    /// <summary>The literals live in <c>PostgresOutagePredictorThresholds.cs</c> only; the scorer partial declares
    /// its alias as a reference, and the detector reads the scorer's symbols rather than a number.</summary>
    [Fact]
    public void TheScorerPartial_RetypesNoSharedBar_AndDeclaresItsFallbackAsAnAlias_AndTheDetectorReadsTheSymbols()
    {
        var scorer = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "PgTargetScorer.Replication.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(scorer);
        foreach (var literal in new[] { "10L * 1024", "1024 * 1024 * 1024", "10737418240", "50_000_000", "50000000" })
            Assert.DoesNotContain(literal, code, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"public\s+const\s+double\s+PgReplayLagBytesFallback\s*=\s*PostgresOutagePredictorThresholds\.SlotRetainedWalWarningBytes\s*;"), code);
        Assert.Matches(new Regex(@"public\s+const\s+double\s+PgReplayLagBytesFloor\s*=\s*ReplayLagNoiseFloorBytes\s*;"), code);
        Assert.Contains("PostgresOutagePredictorThresholds.XminAgeWarningThreshold", code, StringComparison.Ordinal);
        Assert.Contains("PostgresOutagePredictorThresholds.XminPersistenceFraction", code, StringComparison.Ordinal);
        Assert.Contains("PostgresOutagePredictorThresholds.XminMinimumObservations", code, StringComparison.Ordinal);

        var detector = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.Replication.cs"));
        Assert.Contains("PgTargetScorer.PgReplayLagBytesFloor", detector, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.PgReplayLagBytesFallback", detector, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", detector, StringComparison.Ordinal);
        Assert.Contains("_baselineProvider.GetBaselineAsync(", detector, StringComparison.Ordinal);
        Assert.Contains("ZScoreMetadata(baseline, decision, windowSamples)", detector, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgReplayLagBytes", detector, StringComparison.Ordinal);
        Assert.DoesNotContain("DateTime.UtcNow", detector, StringComparison.Ordinal);
    }

    /// <summary>
    /// The slot grade agrees with the alert's verdict across the grid: the evaluator's null ↔ 0, Warning ↔ 0.5,
    /// Critical ↔ 1.0 — and the ARM says which of the alert's three conditions produced it.
    /// </summary>
    [Theory]
    [InlineData(1_000_000L, "reserved", true, 0L)]
    [InlineData(1_000_000L, "reserved", false, 500_000L)]
    [InlineData(9_999_999_999L, "extended", false, 1_000_000_000L)]
    [InlineData(10L * 1024 * 1024 * 1024, "reserved", true, 0L)]
    [InlineData(10L * 1024 * 1024 * 1024, "reserved", true, 1L)]
    [InlineData(10L * 1024 * 1024 * 1024, "reserved", false, 0L)]
    [InlineData(10L * 1024 * 1024 * 1024, "reserved", false, 1L)]
    [InlineData(45L * 1024 * 1024 * 1024, "extended", false, 40L * 1024 * 1024 * 1024)]
    [InlineData(200_000_000L, "lost", true, 0L)]
    [InlineData(200_000_000L, "unreserved", false, -1L)]
    [InlineData(0L, "lost", false, 0L)]
    [InlineData(20L * 1024 * 1024 * 1024, null, false, 1L)]
    public void TheScorersSlotGrade_AgreesWithTheAlertEvaluatorsVerdict(long retained, string? walStatus, bool active, long growth)
    {
        var alert = PostgresAlertEvaluator.EvaluateSlot(new PostgresSlotAlertInfo("s", walStatus, active, retained, growth, null));
        var (severity, arm) = PgTargetScorer.GradeSlotRetention(retained, PgTargetScorer.WalStatusCode(walStatus), active, growth);

        if (alert is null)
        {
            Assert.Equal(0.0, severity);
            Assert.Equal(0, arm);
        }
        else if (alert.Severity == AlertSeverityLevel.Critical)
        {
            Assert.Equal(1.0, severity);
            Assert.True(arm is 2 or 3, $"arm {arm}");
            Assert.Equal(walStatus is "lost" or "unreserved", arm == 3);
        }
        else
        {
            Assert.Equal(AlertSeverityLevel.Warning, alert.Severity);
            Assert.Equal(0.5, severity);
            Assert.Equal(1, arm);
        }
    }

    /* ── PG_REPLICATION_LAG ── */

    /// <summary>
    /// Steady at a distance is 0 (context); drifting past the multiple is 0.5 ramping to 1.0 at the critical
    /// multiple; a caught-up first half divides by one WAL segment so a 20 MB wobble stays under the bar and a
    /// 200 MB slide crosses it; a peak at the slot alert's bar fires the absolute arm at 0.5 however steady. The
    /// lineage flag is stamped exactly when the drift arm was consulted.
    /// </summary>
    [Theory]
    [InlineData(880_000.0, 890_000.0, 892_000.0, true, 0.0, 0, true)]                    /* the measured standby: steady, sub-megabyte */
    [InlineData(51_049_472.0, 157_573_120.0, 209_747_968.0, true, 0.5679, 1, true)]     /* the live plant's shape: 1 → 200 MB over 4 h, ratio 3.09 */
    [InlineData(10_000_000.0, 100_000_000.0, 120_000_000.0, true, 1.0, 1, true)]        /* ten times: critical */
    [InlineData(0.0, 20_000_000.0, 25_000_000.0, true, 0.0, 0, true)]                   /* caught up → 20 MB: 1.2 segments, under the multiple */
    [InlineData(0.0, 200_000_000.0, 250_000_000.0, true, 1.0, 1, true)]                 /* caught up → 200 MB: 11.9 segments, past critical */
    [InlineData(100_000_000.0, 150_000_000.0, 160_000_000.0, true, 0.0, 0, true)]       /* growing but under 2×: steady */
    [InlineData(0.0, 0.0, 12_000_000_000.0, false, 0.5, 0, false)]                      /* one collection, peak past the slot bar: absolute arm, drift not consulted */
    [InlineData(500_000_000.0, 600_000_000.0, 11_000_000_000.0, true, 0.5, 0, true)]    /* steady but past the slot bar: absolute arm */
    public void TheLagBase_IsZeroWhenSteady_RampsWhenDrifting_AndFiresTheAbsoluteArmAtTheSlotBar(
        double firstHalf, double secondHalf, double peak, bool computable, double expected, int drifting, bool lineageStamped)
    {
        var fact = Lag(peak, firstHalf, secondHalf, computable);
        new FactScorer().ScoreAll([fact]);

        Assert.Equal(expected, fact.BaseSeverity, precision: 3);
        Assert.Equal(drifting, fact.Metadata[PgTargetScorer.LagDriftingKey]);
        Assert.Equal(lineageStamped, fact.Metadata.ContainsKey("threshold_lineage"));
        if (lineageStamped) Assert.Equal(0, fact.Metadata["threshold_lineage"]);
    }

    /// <summary>A bytes-only row grades: no <c>replay_lag_ms</c> anywhere on the fact, and the advice says the engine
    /// did not report it rather than inventing a duration.</summary>
    [Fact]
    public void ANullLagMsRow_GradesOnBytes_AndTheAdviceSaysMillisecondsWereNotReported()
    {
        var fact = Lag(209_747_968, 51_049_472, 157_573_120, computable: true);
        Assert.DoesNotContain(PgTargetScorer.LagReplayMsLatestKey, fact.Metadata.Keys);
        new FactScorer().ScoreAll([fact]);
        Assert.True(fact.BaseSeverity > 0.5);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.ReplicationLag, Lookup(fact))!;
        Assert.Contains("standby [replica-a] peaked 200 MB behind the primary at replay, DRIFTING", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("replay_lag_ms was not reported by this engine", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("48.7 MB → second-half mean 150.3 MB", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("The gap opens at REPLAY", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_replication_stats", advice.Remediation, StringComparison.Ordinal);

        var withMs = Lag(209_747_968, 51_049_472, 157_573_120, computable: true, (PgTargetScorer.LagMsReportedKey, 1), (PgTargetScorer.LagReplayMsLatestKey, 4_500), (PgTargetScorer.LagReplayMsPeakKey, 9_000));
        new FactScorer().ScoreAll([withMs]);
        var msAdvice = PgTargetAdvice.Compose(PgTargetFactKeys.ReplicationLag, Lookup(withMs))!;
        Assert.Contains("latest 4,500 ms, peak 9,000 ms", msAdvice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLagAdvice_NamesTheStage_TheSyncTrade_AndSlotStateNotObserved_AndCallsASteadyStandbyContext()
    {
        var steady = Lag(892_000, 880_000, 890_000, computable: true,
            (PgTargetScorer.LagStageKey, PgTargetScorer.LagStageSent), (PgTargetScorer.LagStageBytesKey, 700_000),
            (PgTargetScorer.LagSyncStateKey, PgTargetScorer.SyncStateSync), (PgTargetScorer.LagSlotsObservedKey, 0));
        new FactScorer().ScoreAll([steady]);
        Assert.Equal(0.0, steady.Severity);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.ReplicationLag, Lookup(steady))!;
        Assert.Contains("steady — pacing behind at a distance; context, not a finding", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("the stage furthest behind at the latest sample is sent", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("This standby is SYNCHRONOUS", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("No slot state was observed in this window", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("The gap opens at SENT", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("moving it to async (synchronous_standby_names)", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("possible data loss on failover", advice.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLagAmplifiers_ReadTheSlotTheAnomalyTheSyncStateAndTheWalShift_OffBaseSeverity()
    {
        var lag = Lag(209_747_968, 51_049_472, 157_573_120, computable: true, (PgTargetScorer.LagSyncStateKey, PgTargetScorer.SyncStateQuorum));
        var slot = Slot(12L * 1024 * 1024 * 1024, "reserved", active: false, growth: 500_000_000);
        var anomaly = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyReplicationLag, Value = 209_747_968, ServerId = 1,
            Metadata = { ["deviation_sigma"] = 12, ["fire_threshold"] = 3.5, ["baseline_low_quality"] = 0, ["fallback_exceedance"] = 0, ["confidence"] = 1.0, ["baseline_tier"] = 2, ["threshold_lineage"] = 0 } };
        var facts = new List<Fact> { lag, slot, anomaly };
        new FactScorer().ScoreAll(facts);

        Assert.Contains(lag.AmplifierResults, a => a.Matched && a.Description.Contains("PG_SLOT_RETENTION co-fired", StringComparison.Ordinal));
        Assert.Contains(lag.AmplifierResults, a => a.Matched && a.Description.Contains("SYNCHRONOUS", StringComparison.Ordinal));
        Assert.Contains(lag.AmplifierResults, a => a.Matched == (anomaly.BaseSeverity > 0) && a.Description.Contains("ANOMALY_PG_REPLICATION_LAG co-fired", StringComparison.Ordinal));
        Assert.Contains(lag.AmplifierResults, a => !a.Matched && a.Description.Contains("PG_WAL_VOLUME_SHIFT", StringComparison.Ordinal));
        Assert.True(lag.Severity > lag.BaseSeverity);

        /* async, no siblings: nothing lifts it. */
        var alone = Lag(209_747_968, 51_049_472, 157_573_120, computable: true);
        new FactScorer().ScoreAll([alone]);
        Assert.Equal(alone.BaseSeverity, alone.Severity);
    }

    /* ── PG_SLOT_RETENTION ── */

    [Fact]
    public void ALostSlot_IsCriticalAtAnySize_AndTheAdviceSaysSo()
    {
        var lost = Slot(200_000_000, "lost", active: false, growth: 0);
        new FactScorer().ScoreAll([lost]);
        Assert.Equal(1.0, lost.BaseSeverity);
        Assert.Equal(3, lost.Metadata[PgTargetScorer.SlotArmKey]);
        Assert.DoesNotContain("threshold_lineage", lost.Metadata.Keys);   /* engine-defined: the engine's own verdict */

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.SlotRetention, Lookup(lost))!;
        Assert.Contains("slot [cdc_slot] (logical) is LOST", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("The slot is LOST", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_drop_replication_slot", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("that resync is the cost", advice.Remediation, StringComparison.Ordinal);

        var unreserved = Slot(200_000_000, "unreserved", active: true, growth: 0);
        new FactScorer().ScoreAll([unreserved]);
        Assert.Equal(1.0, unreserved.BaseSeverity);
    }

    [Fact]
    public void TheSlotAdvice_StatesInactiveFor_WhenTheEngineSaid_AndThatTheMajorDoesNotReportIt_Otherwise_AndNeverComputesTimeToFull()
    {
        var known = Slot(12L * 1024 * 1024 * 1024, "reserved", active: false, growth: 512L * 1024 * 1024,
            (PgTargetScorer.SlotInactiveSinceKnownKey, 1), (PgTargetScorer.SlotInactiveHoursKey, 30.0), (PgTargetScorer.SlotSpanHoursKey, 4.0), (PgTargetScorer.SlotGrowthBytesPerHourKey, 128L * 1024 * 1024));
        new FactScorer().ScoreAll([known]);
        Assert.Equal(1.0, known.BaseSeverity);
        Assert.Equal(2, known.Metadata[PgTargetScorer.SlotArmKey]);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.SlotRetention, Lookup(known))!;
        Assert.Contains("slot [cdc_slot] (logical) is inactive and still accumulating: 12 GB retained, up 512 MB this window", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("inactive for 30 hours (inactive_since)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("128 MB/h", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("max_slot_wal_keep_size is at its default (-1)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("Time to disk full is not computable here", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("logical subscriber (CDC pipeline, subscription)", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("loses its position", advice.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("hours until full", advice.Investigation + advice.Remediation, StringComparison.OrdinalIgnoreCase);

        var unknown = Slot(12L * 1024 * 1024 * 1024, "reserved", active: false, growth: 0, (PgTargetScorer.SlotInactiveSinceKnownKey, 0), (PgTargetScorer.SlotKeepSizeSetKey, 1), (PgTargetScorer.SlotSafeWalBytesKey, 3L * 1024 * 1024 * 1024));
        new FactScorer().ScoreAll([unknown]);
        Assert.Equal(0.5, unknown.BaseSeverity);
        var unknownAdvice = PgTargetAdvice.Compose(PgTargetFactKeys.SlotRetention, Lookup(unknown))!;
        Assert.Contains("inactive_since is PostgreSQL 17+", unknownAdvice.Investigation, StringComparison.Ordinal);
        Assert.Contains("3 GB of headroom remains", unknownAdvice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Set max_slot_wal_keep_size", unknownAdvice.Remediation, StringComparison.Ordinal);
    }

    /* ── PG_SLOT_XMIN ── */

    [Theory]
    [InlineData(60_000_000L, 49, 49, 0.5)]     /* the live plant: every sample above, no freeze age → flat 0.5 */
    [InlineData(60_000_000L, 20, 49, 0.0)]     /* under the majority: a consumer that fell behind and caught up */
    [InlineData(60_000_000L, 4, 4, 0.0)]       /* under the minimum observations */
    [InlineData(49_999_999L, 49, 49, 0.0)]     /* under the age bar */
    public void TheSlotXminBase_IsTheAlertsIdentityArm_OnTheSlotsOwnSamples(long age, int above, int samples, double expected)
    {
        var fact = SlotXmin(age, above, samples);
        new FactScorer().ScoreAll([fact]);
        Assert.Equal(expected, fact.BaseSeverity);
        Assert.Equal(expected > 0 ? 1 : 0, fact.Metadata[PgTargetScorer.SlotXminIdentityArmKey]);
        Assert.DoesNotContain("threshold_lineage", fact.Metadata.Keys);
    }

    [Fact]
    public void TheSlotXminRamp_TopsOutAtTheServersOwnFreezeMaxAge_AndTheAdviceNamesTheCatalogArm()
    {
        var mid = SlotXmin(125_000_000, 49, 49, (PgTargetScorer.XminFreezeMaxAgeKey, 200_000_000));
        new FactScorer().ScoreAll([mid]);
        Assert.Equal(0.75, mid.BaseSeverity, precision: 6);

        var advice = PgTargetAdvice.Compose(PgTargetFactKeys.SlotXmin, Lookup(mid))!;
        Assert.Contains("slot [cdc_slot] (logical) is holding the vacuum horizon 125,000,000 transactions back through its catalog_xmin, in 49 of 49 samples", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("autovacuum_freeze_max_age (200,000,000)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("A logical slot's catalog_xmin advances only when its subscriber confirms", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("get_pg_xmin_horizon", advice.Remediation, StringComparison.Ordinal);

        var transient = SlotXmin(60_000_000, 10, 49);
        new FactScorer().ScoreAll([transient]);
        var context = PgTargetAdvice.Compose(PgTargetFactKeys.SlotXmin, Lookup(transient))!;
        Assert.Contains("briefly, not persistently; context", context.Headline, StringComparison.Ordinal);
        Assert.StartsWith("No action on this evidence alone", context.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSlotXminAmplifier_ReadsTheHoldOnlyWhenItsHolderIsASlot()
    {
        var slotXmin = SlotXmin(60_000_000, 49, 49);
        var slotHold = XminHold("replication_slot_catalog");
        new FactScorer().ScoreAll([slotXmin, slotHold]);
        Assert.Contains(slotXmin.AmplifierResults, a => a.Matched && a.Description.Contains("PG_XMIN_HOLD co-fired with a replication-slot holder", StringComparison.Ordinal));
        Assert.Equal(0.65, slotXmin.Severity, precision: 6);

        var again = SlotXmin(60_000_000, 49, 49);
        var sessionHold = XminHold("session");
        new FactScorer().ScoreAll([again, sessionHold]);
        Assert.Contains(again.AmplifierResults, a => !a.Matched && a.Description.Contains("PG_XMIN_HOLD co-fired with a replication-slot holder", StringComparison.Ordinal));
        Assert.Equal(0.5, again.Severity, precision: 6);
    }

    /* ── encodings ── */

    [Theory]
    [InlineData("reserved", PgTargetScorer.WalStatusReserved)]
    [InlineData("Extended", PgTargetScorer.WalStatusExtended)]
    [InlineData("unreserved", PgTargetScorer.WalStatusUnreserved)]
    [InlineData("lost", PgTargetScorer.WalStatusLost)]
    [InlineData(null, PgTargetScorer.WalStatusUnknown)]
    [InlineData("something-new", PgTargetScorer.WalStatusUnknown)]
    public void TheWalStatusEncoding_RoundTrips(string? status, int code)
    {
        Assert.Equal(code, PgTargetScorer.WalStatusCode(status));
        Assert.Equal(code == PgTargetScorer.WalStatusUnknown ? "unknown" : status!.ToLowerInvariant(), PgTargetScorer.WalStatusName(code));
    }

    [Theory]
    [InlineData("async", PgTargetScorer.SyncStateAsync)]
    [InlineData("sync", PgTargetScorer.SyncStateSync)]
    [InlineData("quorum", PgTargetScorer.SyncStateQuorum)]
    [InlineData("potential", PgTargetScorer.SyncStatePotential)]
    [InlineData(null, PgTargetScorer.SyncStateUnknown)]
    public void TheSyncStateEncoding_RoundTrips(string? state, int code)
    {
        Assert.Equal(code, PgTargetScorer.SyncStateCode(state));
        Assert.Equal(code == PgTargetScorer.SyncStateUnknown ? "unknown" : state!, PgTargetScorer.SyncStateName(code));
    }

    /* ── the chain ── */

    /// <summary>
    /// The live plant's shape, ungated: the inactive growing slot (1.0 × 1.5) roots ONE story through the slot's
    /// horizon to the cluster's winning holder — <c>PG_SLOT_RETENTION → PG_SLOT_XMIN → PG_XMIN_HOLD</c> — and the
    /// drifting standby, whose only edge points at the consumed slot, roots its own. Two stories, both findings.
    /// </summary>
    [Fact]
    public void TheSlotRootsOneStoryThroughItsHorizonToTheHold_AndTheDriftingStandbyRootsItsOwn()
    {
        var lag = Lag(209_747_968, 51_049_472, 157_573_120, computable: true);
        var slot = Slot(12L * 1024 * 1024 * 1024, "reserved", active: false, growth: 512L * 1024 * 1024);
        var slotXmin = SlotXmin(60_000_000, 49, 49);
        var hold = XminHold("replication_slot_catalog");
        var facts = new List<Fact> { lag, slot, slotXmin, hold };
        new FactScorer().ScoreAll(facts);

        Assert.Equal(1.5, slot.Severity, precision: 6);
        Assert.InRange(lag.Severity, 0.73, 0.75);
        Assert.Equal(0.65, slotXmin.Severity, precision: 6);
        Assert.Equal(0.5, hold.Severity, precision: 6);

        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts).OrderByDescending(s => s.Severity).ToList();
        Assert.Equal(2, stories.Count);
        Assert.Equal(PgTargetFactKeys.SlotRetention, stories[0].RootFactKey);
        Assert.Equal(new[] { PgTargetFactKeys.SlotRetention, PgTargetFactKeys.SlotXmin, PgTargetFactKeys.XminHold }, stories[0].Path);
        Assert.Equal(PgTargetFactKeys.ReplicationLag, stories[1].RootFactKey);
        Assert.Single(stories[1].Path);
    }

    /// <summary>The mesh's other direction, and the inert edges: the hold leading reaches the slot leaf; the
    /// WAL-shift and anomaly edges are declared against the constants and open only on the destination's BASE.</summary>
    [Fact]
    public void WhenTheHoldLeads_ItReachesTheSlotLeaf_AndTheDeclaredEdgesOpenOnBaseSeverityOnly()
    {
        var hold = XminHold("replication_slot_catalog", age: 190_000_000, (PgTargetScorer.XminFreezeMaxAgeKey, 200_000_000));
        var slotXmin = SlotXmin(60_000_000, 49, 49);
        var facts = new List<Fact> { hold, slotXmin };
        new FactScorer().ScoreAll(facts);
        Assert.True(hold.Severity > slotXmin.Severity, $"{hold.Severity} {slotXmin.Severity}");
        var story = Assert.Single(new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts));
        Assert.Equal(PgTargetFactKeys.XminHold, story.RootFactKey);
        Assert.Contains(PgTargetFactKeys.SlotXmin, story.Path);

        var graph = new PgTargetRelationshipGraph();
        var lag = Lag(209_747_968, 51_049_472, 157_573_120, computable: true);
        var steady = Lag(892_000, 880_000, 890_000, computable: true);
        new FactScorer().ScoreAll([lag]);
        new FactScorer().ScoreAll([steady]);
        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.AnomalyReplicationLag, Lookup(lag)), e => e.Destination == PgTargetFactKeys.ReplicationLag);
        var slot = Slot(12L * 1024 * 1024 * 1024, "reserved", active: false, growth: 1);
        new FactScorer().ScoreAll([slot]);
        Assert.Contains(graph.GetActiveEdges(PgTargetFactKeys.WalVolumeShift, Lookup(slot)), e => e.Destination == PgTargetFactKeys.SlotRetention);
        Assert.DoesNotContain(graph.GetAllEdges(PgTargetFactKeys.WalVolumeShift), e => e.Destination == PgTargetFactKeys.ReplicationLag);
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.AnomalyReplicationLag, Lookup(steady)));
        /* An amplifier cannot open an edge: BaseSeverity 0 with an inflated Severity stays closed. */
        steady.Severity = 0.9;
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.AnomalyReplicationLag, Lookup(steady)));
        Assert.Equal("replication_risk", graph.GetActiveEdges(PgTargetFactKeys.AnomalyReplicationLag, Lookup(lag)).Single().Category);
    }

    /* ── advice statics and the delegation ── */

    [Fact]
    public void TheStaticBlocks_ExistForEveryReplicationKey_AreTheDelegation_AndCarryNoDdl()
    {
        foreach (var key in new[] { PgTargetFactKeys.ReplicationLag, PgTargetFactKeys.SlotRetention, PgTargetFactKeys.SlotXmin })
        {
            var block = PgTargetAdvice.Static(key);
            Assert.NotNull(block);
            Assert.Equal(block, FactAdvice.GetForFactKey(key));
            Assert.Equal(block, PgTargetAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal)));
            var text = block!.Headline + block.Investigation + block.Remediation;
            Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);
            Assert.Null(block.RemediationTsql);
            Assert.Contains("counter-objective", text, StringComparison.OrdinalIgnoreCase);
        }
        Assert.Contains("Time to disk full is NOT computed", PgTargetAdvice.Static(PgTargetFactKeys.SlotRetention)!.Investigation, StringComparison.Ordinal);

        /* The anomaly composes in this family file through ComposeAnomaly's one delegating arm (lane 11's shape). */
        var anomalyStatic = PgTargetAdvice.Static(PgTargetFactKeys.AnomalyReplicationLag);
        Assert.NotNull(anomalyStatic);
        Assert.Contains("further behind the primary than this server's normal", anomalyStatic!.Headline, StringComparison.Ordinal);
        var anomaly = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyReplicationLag, Value = 209_715_228, ServerId = 1,
            Metadata = { ["peak_replay_bytes"] = 209_715_228, ["deviation_sigma"] = 12, ["fire_threshold"] = 3.5, ["baseline_low_quality"] = 0, ["baseline_median"] = 900_000, ["baseline_mean"] = 900_000, ["baseline_stddev"] = 10_000, ["baseline_samples"] = 400, ["confidence"] = 1.0, ["baseline_tier"] = 2, ["threshold_lineage"] = 0 } };
        var composed = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyReplicationLag, Lookup(anomaly))!;
        Assert.NotEqual(anomalyStatic, composed);
        Assert.Contains("200 MB behind", composed.Headline + composed.Investigation, StringComparison.Ordinal);
        Assert.Contains("delivered versus offered", PgTargetAdvice.Static(PgTargetFactKeys.ReplicationLag)!.Investigation, StringComparison.Ordinal);
    }

    /* ── the reads: baseline arm, detector window, collector ── */

    [Fact]
    public void TheReplayLagBaselineArm_IsThePointSeriesShape_OverTheCollectorTable_EndingInTheOneScaffold()
    {
        var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgReplayLagBytes);
        Assert.NotNull(sql);
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgReplayLagBytes));
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, sql, StringComparison.Ordinal);
        Assert.Contains("clean AS (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_replication_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(replay_bytes_behind)::DOUBLE PRECISION AS v", sql, StringComparison.Ordinal);
        Assert.Contains("replay_bytes_behind IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CURRENT_TIMESTAMP", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("LAG(", sql, StringComparison.Ordinal);   /* a point series, never differenced */

        /* The detector's window read takes the SAME per-collection pick. */
        var window = PgTargetAnomalyDetector.ReplayLagWindowSql;
        Assert.Contains("FROM pg_replication_stats", window, StringComparison.Ordinal);
        Assert.Contains("MAX(replay_bytes_behind)::DOUBLE PRECISION", window, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time <= $3", window, StringComparison.Ordinal);

        /* Membership: deviation-scored, folds onto the lag fact, and the table is floored for the baseline window. */
        Assert.True(PgTargetScorer.IsDeviationScoredAnomalyKey(PgTargetFactKeys.AnomalyReplicationLag));
        Assert.Equal(new[] { PgTargetFactKeys.ReplicationLag }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyReplicationLag]);
        Assert.Contains("pg_replication_stats", DarlingRetentionHorizons.BaselineServingRawCollectors);
    }

    [Fact]
    public void TheCollectorReads_AreInTheInventory_NameTheStandbyByApplicationName_AndKeepTheConventions()
    {
        Assert.Contains(PgTargetFactCollector.PgTargetReplicationLagSql, PgTargetFactCollector.AllSql);
        Assert.Contains(PgTargetFactCollector.PgTargetReplicationSlotsSql, PgTargetFactCollector.AllSql);

        var lagSql = PgTargetFactCollector.PgTargetReplicationLagSql;
        Assert.Contains("FROM pg_replication_stats", lagSql, StringComparison.Ordinal);
        Assert.Contains("IS NOT DISTINCT FROM", lagSql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE b.collection_time <  s.midpoint)", lagSql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE b.collection_time >= s.midpoint)", lagSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY p.peak_replay_bytes DESC NULLS LAST", lagSql, StringComparison.Ordinal);   /* by BYTES, never replay_lag_ms */
        Assert.EndsWith("LIMIT 1", lagSql, StringComparison.Ordinal);
        Assert.Contains("COUNT(DISTINCT collection_time) AS collections_in_window", lagSql, StringComparison.Ordinal);

        var slotSql = PgTargetFactCollector.PgTargetReplicationSlotsSql;
        Assert.Contains("FROM pg_replication_slot_stats", slotSql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(COALESCE(xmin_age, 0), COALESCE(catalog_xmin_age, 0)) >= $4", slotSql, StringComparison.Ordinal);
        Assert.Contains("first_retained_wal_bytes", slotSql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", slotSql, StringComparison.Ordinal);   /* every slot: two facts pick two worsts */

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetFactCollector.Replication.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Contains("CommandTimeout = FactCommandTimeoutSeconds", code, StringComparison.Ordinal);
        Assert.Contains("ReportCollectionFailure(ex, context)", code, StringComparison.Ordinal);
        Assert.Contains("PostgresOutagePredictorThresholds.XminAgeWarningThreshold", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.GradeSlotRetention(", code, StringComparison.Ordinal);
        Assert.Contains("ObjectName = string.IsNullOrWhiteSpace(applicationName)", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.WraparoundFreezeMaxAgeKey", code, StringComparison.Ordinal);
        Assert.Contains("PgTargetScorer.LagSlotsObservedKey", code, StringComparison.Ordinal);
        Assert.DoesNotContain("ObservedDurationMs", code, StringComparison.Ordinal);   /* levels and halves, no rate to divide */
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);
        Assert.Contains("/* filled by lane 12", source, StringComparison.Ordinal);
    }

    /* ── the exit criterion, live ── */

    private const string ServerName = "darling-pg-target-replication-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /// <summary>
    /// The family's exit criterion against a real store: a standby drifting 1 → 200 MB over four hours (bytes only,
    /// <c>replay_lag_ms</c> NULL — the measured shape), an inactive logical slot at 12 GB and growing whose
    /// <c>catalog_xmin</c> sits 60 M transactions back on every sample, a second healthy physical slot, and the
    /// cluster's winning xmin holder attributed to that slot → through the REAL <c>analyze_server</c>, TWO stories:
    /// <c>PG_SLOT_RETENTION → PG_SLOT_XMIN → PG_XMIN_HOLD</c> (the slot-family edge into lane 4's key) and the
    /// drifting standby's own. Gated on <c>DARLING_TEST_PG</c>; planting shape from the plumbing e2e; cleanup
    /// through <see cref="LiveStoreCleanup"/> (the #1902 ratchet).
    /// </summary>
    [Fact]
    public async Task ADriftingStandbyAndAnInactiveGrowingSlot_ProduceBothStories_AndTheSlotXminEdgeIntoTheHold()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the replication-family e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 17, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            /* The gate and the coverage witness: 25 h of span, one row a minute across the window. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowStart.AddMinutes(minute - 1), ct);

            /* ── 48 five-minute replication samples from T-4h+2min to T-3min — wholly inside BOTH the collector
               context below and the tool's own four-hour window (which starts a minute or two after windowStart,
               the plumbing e2e's reasoning), so the two reads see the same rows.
               replica-a: replay gap 1 MiB → 200 MiB linearly (4,439,716 bytes a step); sent/write/flush gaps a
               constant 64 KiB so the stage behind is REPLAY; async, streaming, replay_lag_ms NULL on every row.
               cdc_slot (logical, pgoutput): inactive since T-30h, reserved, retained 11.5 → 12 GiB (+512 MiB),
               catalog_xmin 60 M back on every sample, xmin NULL, safe_wal_size −1 (unbounded).
               standby_slot (physical): active, reserved, 200 MB flat, no xmin — the healthy second slot. */
            for (var n = 0; n <= 47; n++)
            {
                var at = windowStart.AddMinutes(2 + 5 * n);
                await PlantReplicationAsync(connection, at, "replica-a", replayBehind: 1_048_576L + n * 4_439_716L, ct);
                await PlantSlotAsync(connection, at, "cdc_slot", "logical", "pgoutput", active: false, "reserved",
                    retained: 12_348_030_976L + n * 11_422_786L, xminAge: null, catalogXminAge: 60_000_000L, inactiveSince: windowEnd.AddHours(-30), ct);
                await PlantSlotAsync(connection, at, "standby_slot", "physical", null, active: true, "reserved",
                    retained: 200_000_000L, xminAge: null, catalogXminAge: null, inactiveSince: null, ct);
            }

            /* ── pg_xmin_horizon, every minute: the slot's catalog horizon wins the cluster's horizon throughout. */
            for (var minute = 0; minute <= 4 * 60; minute++)
                await PlantXminAsync(connection, windowStart.AddMinutes(minute), "replication_slot_catalog", "cdc_slot", 60_000_000, ct);

            /* ── The collector alone. */
            var context = new AnalysisContext
            {
                ServerId = ServerId, ServerName = ServerName, TimeRangeStart = windowStart, TimeRangeEnd = windowEnd, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
            };
            var facts = await new PgTargetFactCollector(postgres).CollectFactsAsync(context);

            var lag = Assert.Single(facts, f => f.Key == PgTargetFactKeys.ReplicationLag);
            Assert.Equal("replica-a", lag.ObjectName);
            Assert.Equal(209_715_228.0, lag.Value);
            Assert.Equal(209_715_228.0, lag.Metadata[PgTargetScorer.LagLatestBytesKey]);
            Assert.Equal(PgTargetScorer.LagStageReplay, lag.Metadata[PgTargetScorer.LagStageKey]);
            Assert.Equal(PgTargetScorer.SyncStateAsync, lag.Metadata[PgTargetScorer.LagSyncStateKey]);
            Assert.Equal(1, lag.Metadata[PgTargetScorer.LagStandbyStreamingKey]);
            Assert.Equal(0, lag.Metadata[PgTargetScorer.LagMsReportedKey]);
            Assert.DoesNotContain(PgTargetScorer.LagReplayMsLatestKey, lag.Metadata.Keys);
            Assert.Equal(1, lag.Metadata[PgTargetScorer.LagDriftComputableKey]);
            Assert.Equal(52_105_310.0, lag.Metadata[PgTargetScorer.LagFirstHalfMeanBytesKey], precision: 3);
            Assert.Equal(158_658_494.0, lag.Metadata[PgTargetScorer.LagSecondHalfMeanBytesKey], precision: 3);
            Assert.Equal(48, lag.Metadata[PgTargetScorer.LagSamplesKey]);
            Assert.Equal(48, lag.Metadata[PgTargetScorer.LagCollectionsKey]);
            Assert.Equal(1, lag.Metadata[PgTargetScorer.LagStandbysKey]);
            Assert.Equal(235.0 / 60, lag.Metadata[PgTargetScorer.LagSpanHoursKey], precision: 6);
            Assert.Equal(2, lag.Metadata[PgTargetScorer.LagSlotsObservedKey]);

            var slot = Assert.Single(facts, f => f.Key == PgTargetFactKeys.SlotRetention);
            Assert.Equal("cdc_slot", slot.ObjectName);
            Assert.Equal(12_884_901_918.0, slot.Value);
            Assert.Equal(536_870_942.0, slot.Metadata[PgTargetScorer.SlotGrowthBytesKey]);
            Assert.Equal(0, slot.Metadata[PgTargetScorer.SlotActiveKey]);
            Assert.Equal(PgTargetScorer.WalStatusReserved, slot.Metadata[PgTargetScorer.SlotWalStatusKey]);
            Assert.Equal(0, slot.Metadata[PgTargetScorer.SlotKeepSizeSetKey]);
            Assert.Equal(1, slot.Metadata[PgTargetScorer.SlotLogicalKey]);
            Assert.Equal(1, slot.Metadata[PgTargetScorer.SlotInactiveSinceKnownKey]);
            Assert.Equal(30.0, slot.Metadata[PgTargetScorer.SlotInactiveHoursKey], precision: 3);
            Assert.Equal(2, slot.Metadata[PgTargetScorer.SlotsInWindowKey]);
            Assert.Equal(1, slot.Metadata[PgTargetScorer.SlotsGradedKey]);
            Assert.Equal(48, slot.Metadata[PgTargetScorer.SlotSamplesKey]);

            var slotXmin = Assert.Single(facts, f => f.Key == PgTargetFactKeys.SlotXmin);
            Assert.Equal("cdc_slot", slotXmin.ObjectName);
            Assert.Equal(60_000_000.0, slotXmin.Value);
            Assert.Equal(1, slotXmin.Metadata[PgTargetScorer.SlotXminArmIsCatalogKey]);
            Assert.Equal(48, slotXmin.Metadata[PgTargetScorer.SlotXminSamplesKey]);
            Assert.Equal(48, slotXmin.Metadata[PgTargetScorer.SlotXminObservationsAboveKey]);
            Assert.DoesNotContain(PgTargetScorer.XminFreezeMaxAgeKey, slotXmin.Metadata.Keys);   /* no wraparound read: flat ramp */

            Assert.Contains(facts, f => f.Key == PgTargetFactKeys.XminHold && f.ObjectName == "replication_slot_catalog:cdc_slot");

            /* ── The detector sits out: the replay-lag bucket has nothing before the window (the baseline window
               ends at the analysis start), which is "no baseline", not a fabricated first occurrence. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(ServerId, MetricNames.PgReplayLagBytes, windowStart, ct);
            Assert.Equal(0, bucket.SampleCount);
            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            Assert.DoesNotContain(anomalies, a => a.Key == PgTargetFactKeys.AnomalyReplicationLag);

            /* ── THE EXIT CRITERION: the real analyze_server, two stories. */
            var service = new DarlingAnalysisService(postgres);
            var analysis = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName, 4);
            using (var doc = JsonDocument.Parse(analysis))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                var slotStory = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.SlotRetention);
                Assert.Equal(
                    $"{PgTargetFactKeys.SlotRetention} → {PgTargetFactKeys.SlotXmin} → {PgTargetFactKeys.XminHold}",
                    slotStory.GetProperty("story_path").GetString());
                Assert.Equal(3, slotStory.GetProperty("fact_count").GetInt32());
                Assert.Equal(PgTargetSources.ReplicationSource, slotStory.GetProperty("category").GetString());
                Assert.Equal(1.5, slotStory.GetProperty("severity").GetDouble(), precision: 6);
                var slotAdvice = slotStory.GetProperty("advice");
                Assert.Contains("slot [cdc_slot] (logical) is inactive and still accumulating: 12 GB retained, up 512 MB this window", slotAdvice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("inactive for 30 hours (inactive_since)", slotAdvice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("PG_SLOT_XMIN co-fired", slotAdvice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("Time to disk full is not computable here", slotAdvice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("SELECT pg_drop_replication_slot", slotAdvice.GetProperty("remediation").GetString(), StringComparison.Ordinal);
                var tools = slotStory.GetProperty("next_tools").EnumerateArray().Select(t => t.GetProperty("tool").GetString()!).ToList();
                Assert.Contains("get_pg_replication_slots", tools);
                Assert.All(tools, t => Assert.StartsWith("get_pg_", t, StringComparison.Ordinal));

                /* Consumed by the chain: neither the slot xmin nor the hold roots a second story. */
                Assert.DoesNotContain(findings, f => RootKey(f) is PgTargetFactKeys.SlotXmin or PgTargetFactKeys.XminHold);

                var lagStory = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.ReplicationLag);
                Assert.Equal(1, lagStory.GetProperty("fact_count").GetInt32());
                Assert.InRange(lagStory.GetProperty("severity").GetDouble(), 0.73, 0.75);
                var lagAdvice = lagStory.GetProperty("advice");
                Assert.Contains("standby [replica-a] peaked 200 MB behind the primary at replay, DRIFTING", lagAdvice.GetProperty("headline").GetString(), StringComparison.Ordinal);
                Assert.Contains("replay_lag_ms was not reported by this engine", lagAdvice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("PG_SLOT_RETENTION co-fired", lagAdvice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
                Assert.Contains("48 samples of this standby across 48 five-minute collections", lagAdvice.GetProperty("investigation").GetString(), StringComparison.Ordinal);
            }

            /* The facts read shows the family under its source, with the lineage flag on the drift-graded fact only. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, ServerName, 4, PgTargetSources.ReplicationSource);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var shown = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(3, shown.Count);
                var lagFact = Assert.Single(shown, f => f.GetProperty("key").GetString() == PgTargetFactKeys.ReplicationLag);
                Assert.Equal(0, lagFact.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                Assert.Equal(1, lagFact.GetProperty("metadata").GetProperty(PgTargetScorer.LagDriftingKey).GetDouble());
                var slotFact = Assert.Single(shown, f => f.GetProperty("key").GetString() == PgTargetFactKeys.SlotRetention);
                Assert.False(slotFact.GetProperty("metadata").TryGetProperty("threshold_lineage", out _));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ── helpers ── */

    private static Fact Lag(double peak, double firstHalf, double secondHalf, bool computable, params (string Name, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.ReplicationSource,
            Key = PgTargetFactKeys.ReplicationLag,
            Value = peak,
            ServerId = 1,
            ObjectName = "replica-a",
            Metadata =
            {
                [PgTargetScorer.LagPeakBytesKey] = peak,
                [PgTargetScorer.LagLatestBytesKey] = peak,
                [PgTargetScorer.LagStageKey] = PgTargetScorer.LagStageReplay,
                [PgTargetScorer.LagStageBytesKey] = peak,
                [PgTargetScorer.LagSyncStateKey] = PgTargetScorer.SyncStateAsync,
                [PgTargetScorer.LagStandbyStreamingKey] = 1,
                [PgTargetScorer.LagMsReportedKey] = 0,
                [PgTargetScorer.LagSamplesKey] = 49,
                [PgTargetScorer.LagCollectionsKey] = 49,
                [PgTargetScorer.LagStandbysKey] = 1,
                [PgTargetScorer.LagSpanHoursKey] = 4,
                [PgTargetScorer.LagSlotsObservedKey] = 2,
                [PgTargetScorer.LagDriftComputableKey] = computable ? 1 : 0,
            },
        };
        if (computable)
        {
            fact.Metadata[PgTargetScorer.LagFirstHalfMeanBytesKey] = firstHalf;
            fact.Metadata[PgTargetScorer.LagSecondHalfMeanBytesKey] = secondHalf;
        }
        foreach (var (name, value) in extra) fact.Metadata[name] = value;
        return fact;
    }

    private static Fact Slot(long retained, string walStatus, bool active, long growth, params (string Name, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.ReplicationSource,
            Key = PgTargetFactKeys.SlotRetention,
            Value = retained,
            ServerId = 1,
            ObjectName = "cdc_slot",
            Metadata =
            {
                [PgTargetScorer.SlotRetainedBytesKey] = retained,
                [PgTargetScorer.SlotFirstRetainedBytesKey] = retained - growth,
                [PgTargetScorer.SlotGrowthBytesKey] = growth,
                [PgTargetScorer.SlotActiveKey] = active ? 1 : 0,
                [PgTargetScorer.SlotWalStatusKey] = PgTargetScorer.WalStatusCode(walStatus),
                [PgTargetScorer.SlotKeepSizeSetKey] = 0,
                [PgTargetScorer.SlotLogicalKey] = 1,
                [PgTargetScorer.SlotInactiveSinceKnownKey] = 0,
                [PgTargetScorer.SlotsInWindowKey] = 2,
                [PgTargetScorer.SlotsGradedKey] = 1,
                [PgTargetScorer.SlotSamplesKey] = 49,
                [PgTargetScorer.SlotSpanHoursKey] = 4,
            },
        };
        foreach (var (name, value) in extra) fact.Metadata[name] = value;
        return fact;
    }

    private static Fact SlotXmin(long age, int above, int samples, params (string Name, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.ReplicationSource,
            Key = PgTargetFactKeys.SlotXmin,
            Value = age,
            ServerId = 1,
            ObjectName = "cdc_slot",
            Metadata =
            {
                [PgTargetScorer.SlotXminAgeKey] = age,
                [PgTargetScorer.SlotXminPhysicalAgeKey] = 0,
                [PgTargetScorer.SlotXminCatalogAgeKey] = age,
                [PgTargetScorer.SlotXminArmIsCatalogKey] = 1,
                [PgTargetScorer.SlotXminSamplesKey] = samples,
                [PgTargetScorer.SlotXminObservationsAboveKey] = above,
                [PgTargetScorer.SlotActiveKey] = 0,
                [PgTargetScorer.SlotLogicalKey] = 1,
                [PgTargetScorer.SlotInactiveSinceKnownKey] = 0,
            },
        };
        foreach (var (name, value) in extra) fact.Metadata[name] = value;
        return fact;
    }

    private static Fact XminHold(string source, long age = 60_000_000, params (string Name, double Value)[] extra)
    {
        var fact = new Fact
        {
            Source = PgTargetSources.VacuumSource,
            Key = PgTargetFactKeys.XminHold,
            Value = age,
            ServerId = 1,
            ObjectName = $"{source}:cdc_slot",
            Metadata =
            {
                [PgTargetScorer.XminAgeKey] = age,
                [PgTargetScorer.XminHolderSourceKey] = PgTargetAdvice.HolderSourceCode(source),
                [PgTargetScorer.XminObservationsTotalKey] = 241,
                [PgTargetScorer.XminObservationsHeldKey] = 241,
                [PgTargetScorer.XminObservationsAboveThresholdKey] = 241,
                [PgTargetScorer.XminHeldFractionKey] = 1.0,
                [PgTargetScorer.XminPeakWinningAgeKey] = age,
                [PgTargetScorer.XminMinutesSinceLastHolderKey] = 0,
            },
        };
        foreach (var (name, value) in extra) fact.Metadata[name] = value;
        return fact;
    }

    private static Dictionary<string, Fact> Lookup(params Fact[] facts)
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var fact in facts)
            lookup[fact.Key] = fact;
        return lookup;
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One <c>pg_replication_stats</c> row as the collector writes it: a streaming async standby on a Unix
    /// socket (NULL <c>client_addr</c>), the three upstream gaps a constant 64 KiB, the millisecond lags NULL.</summary>
    private static async Task PlantReplicationAsync(NpgsqlConnection connection, DateTime at, string applicationName, long replayBehind, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_replication_stats
    (collection_id, collection_time, server_id, server_name, application_name, client_addr, state, sync_state, sync_priority,
     sent_bytes_behind, write_bytes_behind, flush_bytes_behind, replay_bytes_behind, write_lag_ms, flush_lag_ms, replay_lag_ms, backend_start)
VALUES ($1, $2, $3, $4, $5, NULL, 'streaming', 'async', 0, 65536, 65536, 65536, $6, NULL, NULL, NULL, $2)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(applicationName);
        command.Parameters.AddWithValue(replayBehind);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantSlotAsync(
        NpgsqlConnection connection, DateTime at, string slotName, string slotType, string? plugin, bool active, string walStatus,
        long retained, long? xminAge, long? catalogXminAge, DateTime? inactiveSince, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_replication_slot_stats
    (collection_id, collection_time, server_id, server_name, slot_name, slot_type, plugin, database_name, is_active, active_pid,
     is_temporary, two_phase, wal_status, safe_wal_size_bytes, retained_wal_bytes, xmin_age, catalog_xmin_age, inactive_since, invalidation_reason, conflicting)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'appdb', $8, NULL, FALSE, FALSE, $9, -1, $10, $11, $12, $13, NULL, FALSE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(slotName);
        command.Parameters.AddWithValue(slotType);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)plugin ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(active);
        command.Parameters.AddWithValue(walStatus);
        command.Parameters.AddWithValue(retained);
        command.Parameters.Add(new NpgsqlParameter { Value = xminAge.HasValue ? xminAge.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = catalogXminAge.HasValue ? catalogXminAge.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = inactiveSince.HasValue ? inactiveSince.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task PlantXminAsync(NpgsqlConnection connection, DateTime at, string source, string holder, long xminAge, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO pg_xmin_horizon (collection_id, collection_time, server_id, server_name, source, xmin_age, holder, detail, is_winner)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, TRUE)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(source);
        command.Parameters.AddWithValue(xminAge);
        command.Parameters.AddWithValue(holder);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_replication_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_replication_slot_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM pg_xmin_horizon WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
