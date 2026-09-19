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
/// Lane 15 of #3691 — the write family tells Aurora the truth (design §3.11, calibration §A4 / §A7), in two halves.
///
/// <para><b>Half one: <c>not_applicable</c> on Aurora.</b> Fifty Aurora clusters over fourteen days reported sixty
/// timed "checkpoints" an hour and a requested share of 0 on every one — Aurora storage owns checkpointing and the
/// counters are synthetic — so <c>PG_CHECKPOINT_PRESSURE</c> and <c>CONFIG_PG_MAX_WAL_SIZE</c> are emitted with
/// <c>not_applicable = 1</c> (the numeric-metadata idiom the buffer composite's <c>hit_ratio_suppressed</c> set),
/// score 0 off the flag, take no lineage stamp, arm no amplifier, open no edge, root no card — and the advice says
/// what Aurora does instead. Stock PostgreSQL is byte-identical: lane 2's pins run unchanged in
/// <c>PgTargetKnobsTests</c>, and the contrast is re-run here beside the Aurora shape.</para>
///
/// <para><b>Half two: <c>PG_WAL_VOLUME_SHIFT</c> and <c>ANOMALY_PG_WAL_VOLUME</c>.</b> The shift is a CONTEXT fact —
/// mean and peak WAL bytes per second per collection, <c>threshold_lineage = 1</c> because no bar was chosen — or
/// <c>unavailable</c> with one reason flag where WAL is not reported. The anomaly is the graded instrument: the
/// window's peak against the server's own hour-of-week bucket through the shared gate, floor 1 MiB/s and fallback
/// 16 MiB/s (both unmeasured, <c>threshold_lineage = 0</c>). It lifts the pressure fact through the trigger
/// amplifier and folds onto its incident through <c>AnomalyToFamilies</c>; there is no graph edge from either
/// key (<c>PgTargetRelationshipGraph.Write.cs</c> says why).</para>
///
/// <para><b>Every number asserted here was executed on this machine</b> through a net10.0 harness over the built
/// assemblies before the first CI run — the brief's rule for pure-logic pins. The two gated e2es plant an
/// Aurora-stamped server with requested-DOMINANT checkpoints (impossible on Aurora; planted to prove the gate) and a
/// stock-stamped server with 31 days of WAL at 1 MiB/s then 12 MiB/s for the last hours, and drive the REAL
/// <c>analyze_server</c>.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetWriteTests
{
    private const string AuroraServerName = "darling-pg-target-write-aurora";
    private static readonly int AuroraServerId = ServerIdHelper.GetDeterministicHashCode(AuroraServerName);
    private const string StockServerName = "darling-pg-target-write-stock";
    private static readonly int StockServerId = ServerIdHelper.GetDeterministicHashCode(StockServerName);

    private const double MiB = 1024.0 * 1024.0;

    /* ───────────────────────── half one: the scorer reads the flag ───────────────────────── */

    [Fact]
    public void CheckpointPressure_OnAurora_ScoresZero_TakesNoLineageStamp_AndStockIsUnchanged()
    {
        var aurora = Pressure(0.95, 240, notApplicable: true);
        Assert.Equal(0.0, PgTargetScorer.ScoreBase(aurora));
        Assert.False(aurora.Metadata.ContainsKey("threshold_lineage"));

        var stock = Pressure(0.95, 240);
        Assert.Equal(1.0, PgTargetScorer.ScoreBase(stock), precision: 9);
        Assert.Equal(0, stock.Metadata["threshold_lineage"]);

        /* The flag alone decides — the same share, the same count, one metadata key apart. */
        Assert.Equal(aurora.Value, stock.Value);
        Assert.Equal(aurora.Metadata["checkpoints_total"], stock.Metadata["checkpoints_total"]);
    }

    [Theory]
    [InlineData(1024.0, false, 0.4)]   // the shipped default on stock PostgreSQL: the advisory base
    [InlineData(1024.0, true, 0.0)]    // the same value on Aurora: not a finding — the engine does not consult it
    [InlineData(80.0, true, 0.0)]
    [InlineData(16384.0, false, 0.0)]
    public void MaxWalSize_ScoresTheAdvisoryBase_OnlyWhereTheEngineConsultsIt(double mb, bool notApplicable, double expected)
    {
        var knob = Config(PgTargetFactKeys.ConfigMaxWalSize, mb, notApplicable);
        Assert.Equal(expected, PgTargetScorer.ScoreBase(knob), precision: 9);
    }

    [Fact]
    public void AnAuroraPass_WithRequestedDominantCheckpoints_FormsNoCheckpointStory_AndArmsNoKnob()
    {
        var facts = new List<Fact>
        {
            Registry(isAurora: true),
            Config(PgTargetFactKeys.ConfigSharedBuffers, 128),
            Config(PgTargetFactKeys.ConfigMaxWalSize, 1024, notApplicable: true),
            Pressure(0.95, 240, notApplicable: true),
        };
        new FactScorer().ScoreAll(facts);

        var pressure = facts.Single(f => f.Key == PgTargetFactKeys.CheckpointPressure);
        var knob = facts.Single(f => f.Key == PgTargetFactKeys.ConfigMaxWalSize);
        Assert.Equal(0.0, pressure.Severity);
        Assert.Equal(0.0, knob.Severity);
        /* Base 0 means the amplifier pass never ran on either: no results, matched or otherwise. */
        Assert.Empty(pressure.AmplifierResults);
        Assert.Empty(knob.AmplifierResults);

        var graph = new PgTargetRelationshipGraph();
        Assert.Empty(graph.GetActiveEdges(PgTargetFactKeys.CheckpointPressure, Lookup(facts)));

        var stories = new InferenceEngine(graph).BuildStories(facts);
        Assert.DoesNotContain(stories, s => s.RootFactKey == PgTargetFactKeys.CheckpointPressure);
        Assert.DoesNotContain(stories, s => s.RootFactKey == PgTargetFactKeys.ConfigMaxWalSize);
        Assert.DoesNotContain(stories, s => s.Path.Contains(PgTargetFactKeys.ConfigMaxWalSize));
        /* shared_buffers at its default still roots its own advisory — the suppression is the write chain's only. */
        Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.ConfigSharedBuffers);
    }

    [Fact]
    public void TheSameFacts_OnStockPostgres_TellThePressureStory_LaneTwosContrast()
    {
        var facts = new List<Fact>
        {
            Registry(isAurora: false),
            Config(PgTargetFactKeys.ConfigSharedBuffers, 128),
            Config(PgTargetFactKeys.ConfigMaxWalSize, 1024),
            Pressure(0.95, 240),
        };
        new FactScorer().ScoreAll(facts);
        var stories = new InferenceEngine(new PgTargetRelationshipGraph()).BuildStories(facts);

        var churn = Assert.Single(stories, s => s.RootFactKey == PgTargetFactKeys.CheckpointPressure);
        Assert.Equal($"{PgTargetFactKeys.CheckpointPressure} → {PgTargetFactKeys.ConfigMaxWalSize}", churn.StoryPath);
        Assert.True(churn.Severity >= 0.5);
    }

    /* ───────────────────────── half two: the shift is context, the anomaly is the trigger ───────────────────────── */

    [Fact]
    public void WalVolumeShift_ScoresZero_InEveryShape_AndCarriesLineageOne_FromTheCollector()
    {
        foreach (var shift in new[] { Shift(mean: 5 * MiB, peak: 12 * MiB), UnavailableShift("reason_wal_stats_not_reported"), UnavailableShift("reason_pg_stat_wal_absent") })
        {
            Assert.Equal(0.0, PgTargetScorer.ScoreBase(shift));
            Assert.Equal(1, shift.Metadata["threshold_lineage"]);
            /* Through the shared pass beside a fired anomaly: still 0, and no amplifier ever ran on it. */
            new FactScorer().ScoreAll([shift, FiredWalAnomaly(sigma: 7.0)]);
            Assert.Equal(0.0, shift.Severity);
            Assert.Empty(shift.AmplifierResults);
        }
    }

    [Fact]
    public void TheCheckpointTriggerAmplifier_ReadsTheAnomaly_NotTheContextFact()
    {
        /* The context fact alone, however large its numbers: no lift. */
        var withShift = new List<Fact> { Config(PgTargetFactKeys.ConfigMaxWalSize, 16384), Pressure(0.6, 48), Shift(mean: 50 * MiB, peak: 90 * MiB) };
        new FactScorer().ScoreAll(withShift);
        var pressureAlone = withShift.Single(f => f.Key == PgTargetFactKeys.CheckpointPressure);
        var trigger = Assert.Single(pressureAlone.AmplifierResults, a => a.Description.StartsWith("ANOMALY_PG_WAL_VOLUME", StringComparison.Ordinal));
        Assert.False(trigger.Matched);
        Assert.Equal(0.0, trigger.Boost);   /* an unmatched amplifier records no boost */
        Assert.DoesNotContain(pressureAlone.AmplifierResults, a => a.Description.StartsWith("PG_WAL_VOLUME_SHIFT", StringComparison.Ordinal));

        /* The anomaly fired (a trusted 7σ deviation past its 3.5 anchor: base 1.0): the trigger lifts the pressure. */
        var withAnomaly = new List<Fact> { Config(PgTargetFactKeys.ConfigMaxWalSize, 16384), Pressure(0.6, 48), Shift(mean: 5 * MiB, peak: 12 * MiB), FiredWalAnomaly(sigma: 7.0) };
        new FactScorer().ScoreAll(withAnomaly);
        var pressure = withAnomaly.Single(f => f.Key == PgTargetFactKeys.CheckpointPressure);
        var anomaly = withAnomaly.Single(f => f.Key == PgTargetFactKeys.AnomalyWalVolume);
        Assert.Equal(1.0, anomaly.BaseSeverity, precision: 9);
        var lifted = Assert.Single(pressure.AmplifierResults, a => a.Description.StartsWith("ANOMALY_PG_WAL_VOLUME", StringComparison.Ordinal));
        Assert.True(lifted.Matched);
        Assert.Equal(PgTargetScorer.CheckpointTriggerBoost, lifted.Boost);
        /* 0.6 share → ApplyThresholdFormula(0.6, 0.5, 0.9) = 0.625; × (1 + 0.2 trigger; the sized knob does not match). */
        Assert.Equal(0.625, pressure.BaseSeverity, precision: 9);
        Assert.Equal(0.625 * 1.2, pressure.Severity, precision: 9);
    }

    [Fact]
    public void TheWalAnomaly_FoldsOntoCheckpointPressure_AndHasNoGraphEdge_NorDoesTheShift()
    {
        Assert.Equal(new[] { PgTargetFactKeys.CheckpointPressure }, PgTargetFactKeys.AnomalyToFamilies[PgTargetFactKeys.AnomalyWalVolume]);
        var graph = new PgTargetRelationshipGraph();
        Assert.Empty(graph.GetAllEdges(PgTargetFactKeys.AnomalyWalVolume));
        Assert.DoesNotContain(graph.GetAllEdges(PgTargetFactKeys.WalVolumeShift), e => e.Destination == PgTargetFactKeys.CheckpointPressure);
        /* No edge anywhere points INTO the shift or the anomaly either — neither can be walked to. */
        foreach (var source in new[] { PgTargetFactKeys.CheckpointPressure, PgTargetFactKeys.ConfigMaxWalSize, PgTargetFactKeys.WaitKey("IO", "WALSync"), PgTargetFactKeys.WaitKey("LWLock", "WALWrite") })
            Assert.DoesNotContain(graph.GetAllEdges(source), e => e.Destination is PgTargetFactKeys.WalVolumeShift or PgTargetFactKeys.AnomalyWalVolume);
    }

    /* ───────────────────────── the gate: floor and fallback, in bytes per second ───────────────────────── */

    [Theory]
    [InlineData(9.2, true)]     // 9.2 MiB/s against a 1.1 MiB/s routine: far past 3.5 robust sigmas and past the 1 MiB/s floor
    [InlineData(2.0, true)]     // 2 MiB/s: still ~8 robust sigmas up, and above the floor
    [InlineData(0.9, false)]    // under the 1 MiB/s floor: however many sigmas, a quiet server's ripple is not an event
    public void TheGate_FiresOnATrustedBucket_OnlyAboveTheFloor(double peakMiB, bool expectedFire)
    {
        /* A trusted Full-tier bucket: median 1.1 MiB/s, MAD 0.1 MiB/s → robust sigma ≈ 0.148 MiB/s; 0.9 MiB/s is
           BELOW the median and cannot deviate upward at all, which is the point of the theory row — the floor is
           what a reader must know keeps small servers quiet, and it is asserted directly below as well. */
        var bucket = TrustedBucket(median: 1.1 * MiB, mad: 0.1 * MiB);
        var decision = AnomalyGate.EvaluateZScore(
            bucket, peakMiB * MiB,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec),
            AnomalyThresholds.PgWalBytesFloorPerSec, AnomalyThresholds.PgWalBytesFallbackPerSec, AnomalyThresholds.SigmaDisplayCap);
        Assert.Equal(expectedFire, decision.Fire);
        Assert.False(decision.LowQualityBaseline);
        Assert.Equal(AnomalyThresholds.ModifiedZThreshold, decision.ThresholdUsed);
    }

    [Fact]
    public void TheGate_HoldsTheFloor_AgainstAQuietServersManySigmas_AndFallsBackToSixteenMiB_OnAThinBucket()
    {
        /* A 30 KB/s server tripling to 90 KB/s reads dozens of robust sigmas — and stays quiet under the floor. */
        var quiet = TrustedBucket(median: 30 * 1024, mad: 1024);
        var tripled = AnomalyGate.EvaluateZScore(
            quiet, 90 * 1024,
            AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThresholdFor(MetricNames.PgWalBytesPerSec),
            AnomalyThresholds.PgWalBytesFloorPerSec, AnomalyThresholds.PgWalBytesFallbackPerSec, AnomalyThresholds.SigmaDisplayCap);
        Assert.True(tripled.Sigma > 20);
        Assert.False(tripled.Fire);

        /* A thin bucket (three samples, one day): the absolute fallback bar, 16 MiB/s, and only that. */
        var thin = new BaselineBucket { HourOfDay = 3, DayOfWeek = 2, Mean = 1.1 * MiB, StdDev = 0.1 * MiB, Median = 1.1 * MiB, Mad = 0.1 * MiB, SampleCount = 3, DistinctDays = 1, Tier = BaselineTier.Full };
        Assert.False(thin.IsTrustworthy);
        var below = AnomalyGate.EvaluateZScore(thin, 12 * MiB, AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold, AnomalyThresholds.PgWalBytesFloorPerSec, AnomalyThresholds.PgWalBytesFallbackPerSec, AnomalyThresholds.SigmaDisplayCap);
        var above = AnomalyGate.EvaluateZScore(thin, 16 * MiB, AnomalyThresholds.DefaultDeviationThreshold, AnomalyThresholds.ModifiedZThreshold, AnomalyThresholds.PgWalBytesFloorPerSec, AnomalyThresholds.PgWalBytesFallbackPerSec, AnomalyThresholds.SigmaDisplayCap);
        Assert.False(below.Fire);
        Assert.True(above.Fire);
        Assert.True(above.LowQualityBaseline);
        Assert.Equal(1.0, above.FallbackExceedance, precision: 9);
    }

    /* ───────────────────────── the SQL: one differencing, three readers ───────────────────────── */

    [Fact]
    public void TheWalVolumeRead_IsTheCheckpointReadsDifferencing_RatedPerCollection_AndIsTheDetectorsReadByAlias()
    {
        var sql = PgTargetFactCollector.PgTargetWalVolumeSql;
        Assert.Contains(sql, PgTargetFactCollector.AllSql);
        Assert.Equal(PgTargetAnomalyDetector.WalVolumeWindowSql, sql);

        Assert.Contains("FROM pg_write_stats", sql, StringComparison.Ordinal);
        Assert.Contains("wal_bytes   - LAG(wal_bytes)   OVER series AS raw_wal_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("wal_records - LAG(wal_records) OVER series AS raw_wal_records", sql, StringComparison.Ordinal);
        Assert.Contains("ROW_NUMBER() OVER series > 1", sql, StringComparison.Ordinal);
        Assert.Contains("wal_stats_reset IS DISTINCT FROM LAG(wal_stats_reset) OVER series", sql, StringComparison.Ordinal);
        Assert.Contains("(wal_bytes IS NOT NULL) AS wal_tracked", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_wal_bytes, 0)::DOUBLE PRECISION / interval_sec AS bytes_per_sec", sql, StringComparison.Ordinal);
        Assert.Contains("LAG(collection_time) OVER series", sql, StringComparison.Ordinal);
        Assert.Contains("interval_sec > 0", sql, StringComparison.Ordinal);
        /* Server-scoped, window-bound, positional — the collector census's rules, stated here for the one read. */
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TheWalBaselineArm_TakesTheSameDifferencing_HalfOpen_UntrackedRowsExcluded_UnderTheOneScaffold()
    {
        var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgWalBytesPerSec);
        Assert.NotNull(sql);
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_write_stats", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);
        Assert.Contains("wal_bytes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("wal_bytes - LAG(wal_bytes) OVER series AS raw_wal_bytes", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(raw_wal_bytes, 0)::DOUBLE PRECISION / interval_sec AS v", sql, StringComparison.Ordinal);
        Assert.Contains("interval_sec > 0", sql, StringComparison.Ordinal);
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgWalBytesPerSec));
    }

    /* ───────────────────────── the advice ───────────────────────── */

    [Fact]
    public void AuroraAdvice_SaysWhatAuroraDoesInstead_StatesTheSyntheticShape_AndOffersNoSizing()
    {
        var pressure = Pressure(0.0, 240, notApplicable: true, timedPerHour: 60);
        var knob = Config(PgTargetFactKeys.ConfigMaxWalSize, 1024, notApplicable: true);

        var pressureBlock = PgTargetAdvice.Compose(PgTargetFactKeys.CheckpointPressure, Lookup(pressure, knob))!;
        Assert.Equal("Checkpoint pressure is not applicable on Aurora — the storage layer owns checkpointing", pressureBlock.Headline);
        Assert.Contains("60 timed checkpoints an hour", pressureBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("requested share of 0 %", pressureBlock.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_write_stats", pressureBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("Raise it", pressureBlock.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("checkpoint_completion_target", pressureBlock.Remediation, StringComparison.Ordinal);

        var knobBlock = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMaxWalSize, Lookup(pressure, knob))!;
        Assert.Equal("max_wal_size is 1 GB — not a finding on Aurora, where the engine does not consult it", knobBlock.Headline);
        Assert.Contains("does not govern", knobBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("shipped default", knobBlock.Investigation, StringComparison.Ordinal);

        /* Stock PostgreSQL's blocks are lane 2's, untouched: the same facts without the flag compose the sizing arithmetic. */
        var stockBlock = PgTargetAdvice.Compose(PgTargetFactKeys.ConfigMaxWalSize, Lookup(Pressure(0.0, 240), Config(PgTargetFactKeys.ConfigMaxWalSize, 1024)))!;
        Assert.Equal("max_wal_size is 1 GB — the shipped default", stockBlock.Headline);
    }

    [Fact]
    public void WalVolumeShiftAdvice_StatesTheMeasuredRates_OrTheReasonItCannot()
    {
        var tracked = PgTargetAdvice.Compose(PgTargetFactKeys.WalVolumeShift, Lookup(Shift(mean: 1.1 * MiB, peak: 9.2 * MiB, totalBytes: 15.8 * 1024 * MiB, ratedSamples: 240)))!;
        Assert.Equal("WAL volume: 1.1 MB/s mean, 9.2 MB/s peak this window", tracked.Headline);
        Assert.Contains("15.8 GB of WAL", tracked.Investigation, StringComparison.Ordinal);
        Assert.Contains("240 rated collections", tracked.Investigation, StringComparison.Ordinal);
        Assert.Contains("no absolute bar", tracked.Investigation, StringComparison.Ordinal);
        Assert.Contains("wal_compression", tracked.Remediation, StringComparison.Ordinal);
        Assert.Contains("pg_wal", tracked.Remediation, StringComparison.Ordinal);

        var aurora = PgTargetAdvice.Compose(PgTargetFactKeys.WalVolumeShift, Lookup(UnavailableShift("reason_wal_stats_not_reported")))!;
        Assert.Equal("WAL volume is not reported by this engine", aurora.Headline);
        Assert.Contains("not implemented on Aurora", aurora.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_pg_top_queries", aurora.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("0 B/s", aurora.Headline + aurora.Investigation, StringComparison.Ordinal);

        var old = PgTargetAdvice.Compose(PgTargetFactKeys.WalVolumeShift, Lookup(UnavailableShift("reason_pg_stat_wal_absent")))!;
        Assert.Equal("WAL volume is not reported by this engine", old.Headline);
        Assert.Contains("below 14", old.Investigation, StringComparison.Ordinal);

        /* The static path (no fact in hand) is non-null and honest about being context. */
        var stat = PgTargetAdvice.Static(PgTargetFactKeys.WalVolumeShift)!;
        Assert.Contains("Context, not a finding", stat.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void WalAnomalyAdvice_StatesPeakSigmaAndTheRatioToRoutine_FramesDeployments_AndNeverAdvisesFullPageWritesOff()
    {
        var anomaly = FiredWalAnomaly(sigma: 7.0, peak: 9.2 * MiB, baselineMean: 1.1 * MiB, baselineMedian: 1.05 * MiB);
        anomaly.Metadata["baseline_ratio"] = 9.2 / 1.05;   /* peak over the robust centre (the median), as the detector stamps it */

        var leading = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWalVolume, Lookup(anomaly, Pressure(0.1, 48)))!;
        Assert.Equal("WAL volume spiked to 9.2 MB/s — 7σ above its baseline for this time of week", leading.Headline);
        Assert.Contains("7σ above its 1.1 MB/s baseline median", leading.Investigation, StringComparison.Ordinal);
        Assert.Contains("8.76× the 1.1 MB/s this server routinely writes", leading.Investigation, StringComparison.Ordinal);
        Assert.Contains("leading edge", leading.Investigation, StringComparison.Ordinal);
        Assert.Contains("pg_write_stats", leading.Investigation, StringComparison.Ordinal);
        Assert.Contains("deploy", leading.Investigation, StringComparison.Ordinal);
        Assert.Contains("wal_compression trades CPU", leading.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("full_page_writes", leading.Headline + leading.Investigation + leading.Remediation, StringComparison.Ordinal);

        /* With the pressure fired beside it, the block points at the sizing card rather than repeating it. */
        var pressure = Pressure(0.95, 240);
        pressure.BaseSeverity = pressure.Severity = PgTargetScorer.ScoreBase(pressure);
        var coFired = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWalVolume, Lookup(anomaly, pressure))!;
        Assert.Contains("Checkpoint pressure co-fired this window", coFired.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("Checkpoints were not yet forced", coFired.Investigation, StringComparison.Ordinal);

        /* First occurrence: the fallback wording, no sigma, no multiple. */
        var first = FiredWalAnomaly(sigma: 0, peak: 17 * MiB, lowQuality: true);
        var firstBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWalVolume, Lookup(first))!;
        Assert.Contains("first occurrence, no baseline yet", firstBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", firstBlock.Headline + firstBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("×", firstBlock.Headline + firstBlock.Investigation, StringComparison.Ordinal);

        /* The static block exists, names its table, and is what the shared entry points answer. */
        var stat = PgTargetAdvice.Static(PgTargetFactKeys.AnomalyWalVolume);
        Assert.NotNull(stat);
        Assert.Contains("pg_write_stats", stat!.Investigation, StringComparison.Ordinal);
        Assert.Equal(stat, FactAdvice.GetForFactKey(PgTargetFactKeys.AnomalyWalVolume));
        Assert.Equal(PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWalVolume, Lookup(anomaly)), FactAdvice.Compose(PgTargetFactKeys.AnomalyWalVolume, Lookup(anomaly)));
    }

    /* ───────────────────────── gated: the two exit criteria through the real tool ───────────────────────── */

    /// <summary>
    /// Aurora-stamped, requested-DOMINANT checkpoints planted (two requested and one timed a minute — a shape Aurora
    /// cannot produce, planted to prove the gate), the knob at its shipped default, WAL columns NULL as the collector
    /// types them there: <c>analyze_server</c> forms NO checkpoint story and NO <c>max_wal_size</c> advisory; the
    /// facts read shows the pressure fact <c>not_applicable</c> with the synthetic shape (60 timed an hour) and the
    /// shift fact <c>unavailable</c> with its reason.
    /// </summary>
    [Fact]
    public async Task AnAuroraTarget_WithPlantedRequestedDominantCheckpoints_YieldsNoCheckpointStory_AndTheFactsSayNotApplicable()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the write-family Aurora e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, AuroraServerId, AuroraServerName, MonitoredEngineKind.AuroraPostgres, 17, ct);

            var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            var windowStart = windowEnd.AddHours(-4);

            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, AuroraServerId, AuroraServerName, windowEnd.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, AuroraServerId, AuroraServerName, windowStart.AddMinutes(minute - 1), ct);
            await PlantConfigSnapshotAsync(connection, AuroraServerId, AuroraServerName, windowEnd.AddMinutes(-30), ct);

            /* Two requested and one timed checkpoint every minute; WAL typed NULL, as the collector writes it on Aurora. */
            await PlantWriteSeriesAsync(connection, AuroraServerId, AuroraServerName, windowStart.AddMinutes(-1), minutes: 4 * 60 + 1,
                requestedPerMinute: 2, timedPerMinute: 1, walBytesPerMinuteBase: null, walBytesPerMinuteSpike: null, spikeFromMinute: int.MaxValue, ct);

            var service = new DarlingAnalysisService(postgres);
            var asOf = windowEnd.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);

            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, AuroraServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;
                /* shared_buffers at its default is the ONE advisory; the write chain roots nothing and is in no path. */
                var only = Assert.Single(findings);
                Assert.Equal(PgTargetFactKeys.ConfigSharedBuffers, RootKey(only));
                Assert.DoesNotContain(PgTargetFactKeys.CheckpointPressure, only.GetProperty("story_path").GetString()!, StringComparison.Ordinal);
                Assert.DoesNotContain(PgTargetFactKeys.ConfigMaxWalSize, only.GetProperty("story_path").GetString()!, StringComparison.Ordinal);
            }

            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, AuroraServerName, 4, PgTargetSources.WriteSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var facts = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                Assert.Equal(2, facts.Count);

                var pressure = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.CheckpointPressure);
                Assert.Equal(0, pressure.GetProperty("base_severity").GetDouble());
                var meta = pressure.GetProperty("metadata");
                Assert.Equal(1, meta.GetProperty("not_applicable").GetDouble());
                Assert.Equal(1, meta.GetProperty("not_applicable_on_aurora").GetDouble());
                Assert.Equal(1, meta.GetProperty("reason_aurora_storage_checkpointing").GetDouble());
                Assert.Equal(60, meta.GetProperty("timed_per_hour").GetDouble(), precision: 2);
                Assert.Equal(2.0 / 3.0, meta.GetProperty("requested_share").GetDouble(), precision: 2);
                Assert.Equal(0, meta.GetProperty("wal_tracked").GetDouble());
                Assert.False(meta.TryGetProperty("threshold_lineage", out _));

                var shift = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.WalVolumeShift);
                Assert.Equal(0, shift.GetProperty("base_severity").GetDouble());
                Assert.Equal(0, shift.GetProperty("value").GetDouble());
                Assert.Equal(1, shift.GetProperty("metadata").GetProperty("unavailable").GetDouble());
                Assert.Equal(1, shift.GetProperty("metadata").GetProperty("reason_wal_stats_not_reported").GetDouble());
                Assert.Equal(1, shift.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
            }

            var configJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, AuroraServerName, 4, PgTargetSources.ConfigSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(configJson))
            {
                var knob = doc.RootElement.GetProperty("facts").EnumerateArray().Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.ConfigMaxWalSize);
                Assert.Equal(1024, knob.GetProperty("value").GetDouble());
                Assert.Equal(0, knob.GetProperty("base_severity").GetDouble());
                Assert.Equal(1, knob.GetProperty("metadata").GetProperty("not_applicable_on_aurora").GetDouble());
                var sharedBuffers = doc.RootElement.GetProperty("facts").EnumerateArray().Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.ConfigSharedBuffers);
                Assert.Equal(0.4, sharedBuffers.GetProperty("base_severity").GetDouble(), precision: 6);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// Stock-stamped, 31 days of one-minute <c>pg_write_stats</c> with WAL at 1 MiB/s (a deterministic ±5 % ripple)
    /// and, for the four-hour window, 12 MiB/s; requested-dominant checkpoints throughout so the pressure fact fires.
    /// The bucket alone: trusted, median near 1 MiB/s. The detector alone: <c>ANOMALY_PG_WAL_VOLUME</c> at 12 MiB/s,
    /// lineage 0, ratio ≈ 12 against the median. Through <c>analyze_server</c>: the pressure story with the trigger amplifier MATCHED, the
    /// anomaly card folded onto the pressure story's incident, and the shift fact stating the window's rates.
    /// </summary>
    [Fact]
    public async Task AStockTarget_WithThirtyOneDaysOfWalAndAThreeHourSurge_FiresTheWalAnomaly_LiftsAndFoldsOntoThePressureStory()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the write-family WAL e2e.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, StockServerId, StockServerName, MonitoredEngineKind.Postgres, 18, ct);

            var end = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
            const int minutes = 31 * 24 * 60;
            var start = end.AddMinutes(-minutes);
            /* The surge starts exactly at the window's first row, so the half-open baseline (< window start) holds
               31 days of the routine rate and nothing of the surge, and every one of the window's 240 deltas is surge. */
            const int spikeFrom = minutes - 240;
            var windowStart = end.AddHours(-4);

            /* The coverage series over the window plus the 25-hour span row, then the write series for the whole 31 days. */
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, end.AddHours(-25), ct);
            for (var minute = 0; minute <= 4 * 60 + 1; minute++)
                await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, StockServerId, StockServerName, windowStart.AddMinutes(minute - 1), ct);
            await PlantConfigSnapshotAsync(connection, StockServerId, StockServerName, end.AddMinutes(-30), ct);
            await PlantWriteSeriesAsync(connection, StockServerId, StockServerName, start, minutes,
                requestedPerMinute: 1, timedPerMinute: 0, walBytesPerMinuteBase: 60L * 1024 * 1024, walBytesPerMinuteSpike: 12L * 60 * 1024 * 1024, spikeFromMinute: spikeFrom, ct);

            /* ── The bucket alone: trusted, centred near 1 MiB/s. */
            var baselines = new PgTargetBaselineProvider(postgres);
            var bucket = await baselines.GetBaselineAsync(StockServerId, MetricNames.PgWalBytesPerSec, windowStart, ct);
            Assert.True(bucket.IsTrustworthy, "the 30-day WAL bucket is not trustworthy");
            Assert.InRange(bucket.Median, 0.94 * MiB, 1.06 * MiB);
            Assert.True(bucket.EffectiveRobustSigma > 0);

            /* ── The detector alone, on the collector-shaped context. */
            var context = new AnalysisContext
            {
                ServerId = StockServerId, ServerName = StockServerName, TimeRangeStart = windowStart, TimeRangeEnd = end, ServerUtcOffset = TimeSpan.Zero,
                Coverage = new WindowCoverage { NominalMs = 4 * 3_600_000, ObservedMs = 4 * 3_600_000, SampleCount = 240 },
            };
            var anomalies = await new PgTargetAnomalyDetector(postgres, baselines).DetectAnomaliesAsync(context);
            var wal = Assert.Single(anomalies, a => a.Key == PgTargetFactKeys.AnomalyWalVolume);
            Assert.Equal(12 * MiB, wal.Value, precision: 0);
            Assert.Equal(12 * MiB, wal.Metadata["peak_wal_bytes_per_sec"], precision: 0);
            Assert.Equal(0, wal.Metadata["threshold_lineage"]);
            Assert.Equal(0, wal.Metadata["baseline_low_quality"]);
            Assert.Equal(AnomalyThresholds.ModifiedZThreshold, wal.Metadata["fire_threshold"]);
            Assert.InRange(wal.Metadata["baseline_ratio"], 11.0, 13.0);
            Assert.Equal(240, wal.Metadata["window_samples"]);

            /* ── THE EXIT CRITERION, through the real analyze_server, anchored at the planted window's end. */
            var service = new DarlingAnalysisService(postgres);
            var asOf = end.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", System.Globalization.CultureInfo.InvariantCulture);
            var json = await DarlingMcpTools.AnalyzeServer(service, postgres, StockServerName, 4, as_of: asOf);
            using (var doc = JsonDocument.Parse(json))
            {
                var root = doc.RootElement;
                Assert.Equal("findings", root.GetProperty("status").GetString());
                var findings = root.GetProperty("findings").EnumerateArray().ToList();
                string RootKey(JsonElement f) => f.GetProperty("root_fact").GetProperty("key").GetString()!;

                var pressure = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.CheckpointPressure);
                Assert.Equal($"{PgTargetFactKeys.CheckpointPressure} → {PgTargetFactKeys.ConfigMaxWalSize}", pressure.GetProperty("story_path").GetString());
                /* Base 1.0 (share 1.0) × (1 + 0.3 knob + 0.2 trigger) = 1.5: the trigger is what carries it to the notify floor. */
                Assert.Equal(1.5, pressure.GetProperty("severity").GetDouble(), precision: 6);

                var anomaly = Assert.Single(findings, f => RootKey(f) == PgTargetFactKeys.AnomalyWalVolume);
                Assert.Equal(pressure.GetProperty("incident_id").GetString(), anomaly.GetProperty("incident_id").GetString());
                var advice = anomaly.GetProperty("advice");
                var text = advice.GetProperty("headline").GetString() + advice.GetProperty("investigation").GetString();
                Assert.Contains("WAL volume spiked to 12 MB/s", text, StringComparison.Ordinal);
                Assert.Contains("σ above its", text, StringComparison.Ordinal);
                Assert.Contains("Checkpoint pressure co-fired this window", text, StringComparison.Ordinal);
                Assert.Contains("this server routinely writes", text, StringComparison.Ordinal);
                Assert.All(anomaly.GetProperty("next_tools").EnumerateArray(), t => Assert.StartsWith("get_pg_", t.GetProperty("tool").GetString()!, StringComparison.Ordinal));

                /* No card roots on the shift: it is context. */
                Assert.DoesNotContain(findings, f => RootKey(f) == PgTargetFactKeys.WalVolumeShift);
            }

            /* ── The facts read: the trigger amplifier MATCHED on the pressure fact; the shift states the window's rates. */
            var factsJson = await DarlingMcpTools.GetAnalysisFacts(service, postgres, StockServerName, 4, PgTargetSources.WriteSource, as_of: asOf);
            using (var doc = JsonDocument.Parse(factsJson))
            {
                var facts = doc.RootElement.GetProperty("facts").EnumerateArray().ToList();
                var shift = facts.Single(f => f.GetProperty("key").GetString() == PgTargetFactKeys.WalVolumeShift);
                Assert.Equal(0, shift.GetProperty("base_severity").GetDouble());
                Assert.Equal(12 * MiB, shift.GetProperty("metadata").GetProperty("peak_wal_bytes_per_sec").GetDouble(), precision: 0);
                Assert.Equal(12 * MiB, shift.GetProperty("value").GetDouble(), precision: 0);   /* the whole window is inside the surge */
                Assert.Equal(1, shift.GetProperty("metadata").GetProperty("wal_tracked").GetDouble());
                Assert.Equal(1, shift.GetProperty("metadata").GetProperty("threshold_lineage").GetDouble());
                /* get_analysis_facts does not run the detector (lane 9's e2e states the same), so the amplifier's match is
                   asserted on the scored pass above through the story's 1.5, not here. */
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    /* ───────────────────────── fixtures ───────────────────────── */

    private static Fact Registry(bool isAurora) => new()
    {
        Source = PgTargetSources.ConfigSource, Key = PgTargetFactKeys.ServerMajorVersion, Value = 17, ServerId = 1,
        Metadata = { ["is_aurora"] = isAurora ? 1 : 0 },
    };

    private static Fact Config(string key, double value, bool notApplicable = false)
    {
        var fact = new Fact { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };
        if (notApplicable)
        {
            fact.Metadata["not_applicable"] = 1;
            fact.Metadata["not_applicable_on_aurora"] = 1;
        }
        return fact;
    }

    private static Fact Pressure(double share, double total, bool notApplicable = false, double? timedPerHour = null)
    {
        var requested = Math.Round(share * total);
        var fact = new Fact
        {
            Source = PgTargetSources.WriteSource,
            Key = PgTargetFactKeys.CheckpointPressure,
            Value = share,
            ServerId = 1,
            Metadata =
            {
                ["checkpoints_requested"] = requested,
                ["checkpoints_timed"] = total - requested,
                ["checkpoints_total"] = total,
                ["requested_share"] = share,
                ["wal_tracked"] = 0,
            },
        };
        if (notApplicable)
        {
            fact.Metadata["not_applicable"] = 1;
            fact.Metadata["not_applicable_on_aurora"] = 1;
            fact.Metadata["reason_aurora_storage_checkpointing"] = 1;
            fact.Metadata["timed_per_hour"] = timedPerHour ?? (total - requested) / 4.0;
        }
        return fact;
    }

    private static Fact Shift(double mean, double peak, double? totalBytes = null, double ratedSamples = 240) => new()
    {
        Source = PgTargetSources.WriteSource,
        Key = PgTargetFactKeys.WalVolumeShift,
        Value = mean,
        ServerId = 1,
        Metadata =
        {
            ["wal_tracked"] = 1,
            ["avg_wal_bytes_per_sec"] = mean,
            ["peak_wal_bytes_per_sec"] = peak,
            ["wal_bytes"] = totalBytes ?? mean * 14_400,
            ["wal_records"] = 1_000_000,
            ["wal_reset_count"] = 0,
            ["rated_samples"] = ratedSamples,
            ["sample_count"] = ratedSamples + 1,
            ["threshold_lineage"] = 1,
        },
    };

    private static Fact UnavailableShift(string reason) => new()
    {
        Source = PgTargetSources.WriteSource,
        Key = PgTargetFactKeys.WalVolumeShift,
        Value = 0,
        ServerId = 1,
        Metadata = { ["wal_tracked"] = 0, ["unavailable"] = 1, [reason] = 1, ["sample_count"] = 241, ["threshold_lineage"] = 1 },
    };

    /// <summary>A WAL anomaly as the detector writes it: the gate's metadata, a Full-tier trusted bucket's context.</summary>
    private static Fact FiredWalAnomaly(double sigma, double peak = 12 * MiB, double baselineMean = 1.1 * MiB, double baselineMedian = 1.05 * MiB, bool lowQuality = false)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = PgTargetFactKeys.AnomalyWalVolume, Value = peak, ServerId = 1 };
        fact.Metadata["baseline_mean"] = baselineMean;
        fact.Metadata["baseline_stddev"] = 0.1 * MiB;
        fact.Metadata["deviation_sigma"] = sigma;
        fact.Metadata["fire_threshold"] = AnomalyThresholds.ModifiedZThreshold;
        fact.Metadata["baseline_low_quality"] = lowQuality ? 1 : 0;
        fact.Metadata["fallback_exceedance"] = lowQuality ? peak / AnomalyThresholds.PgWalBytesFallbackPerSec : 0;
        fact.Metadata["baseline_samples"] = lowQuality ? 3 : 120;
        fact.Metadata["window_samples"] = 240;
        fact.Metadata["threshold_lineage"] = 0;
        fact.Metadata["baseline_hour"] = 3;
        fact.Metadata["baseline_dow"] = 2;
        fact.Metadata["baseline_tier"] = (double)BaselineTier.Full;
        fact.Metadata["baseline_median"] = lowQuality ? 0 : baselineMedian;
        fact.Metadata["baseline_mad"] = 0.1 * MiB;
        fact.Metadata["confidence"] = lowQuality ? 0 : 1.0;
        fact.Metadata["peak_wal_bytes_per_sec"] = peak;
        fact.Metadata["avg_wal_bytes_per_sec"] = peak * 0.8;
        return fact;
    }

    private static BaselineBucket TrustedBucket(double median, double mad) => new()
    {
        HourOfDay = 3, DayOfWeek = 2, Mean = median, StdDev = mad * 1.5, Median = median, Mad = mad,
        SampleCount = 120, DistinctDays = 28, Tier = BaselineTier.Full,
    };

    private static Dictionary<string, Fact> Lookup(params Fact[] facts) => Lookup((IEnumerable<Fact>)facts);

    private static Dictionary<string, Fact> Lookup(IEnumerable<Fact> facts)
    {
        var lookup = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var fact in facts)
            lookup[fact.Key] = fact;
        return lookup;
    }

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>The knobs at their shipped defaults and the convention checks tuned so only <c>shared_buffers</c> (and,
    /// on stock, <c>max_wal_size</c>) can root — <c>PgTargetKnobsTests</c>' snapshot, without the session-row probe.</summary>
    private static async Task PlantConfigSnapshotAsync(NpgsqlConnection connection, int serverId, string serverName, DateTime at, CancellationToken ct)
    {
        var rows = new (string Name, string Setting, string? Unit, string BootVal)[]
        {
            ("shared_buffers", "16384", "8kB", "1024"),
            ("max_wal_size", "1024", "MB", "1024"),
            ("min_wal_size", "80", "MB", "80"),
            ("effective_cache_size", "1048576", "8kB", "524288"),
            ("random_page_cost", "1.1", null, "4"),
            ("track_io_timing", "on", null, "off"),
            ("checkpoint_timeout", "300", "s", "300"),
            ("checkpoint_completion_target", "0.9", null, "0.9"),
            ("wal_compression", "off", null, "off"),
            ("bgwriter_delay", "200", "ms", "200"),
            ("bgwriter_lru_maxpages", "100", null, "100"),
            ("max_connections", "100", null, "100"),
            ("superuser_reserved_connections", "3", null, "3"),
            ("work_mem", "4096", "kB", "4096"),
            ("maintenance_work_mem", "65536", "kB", "65536"),
            ("autovacuum", "on", null, "on"),
        };

        var collectionId = CollectionIdGenerator.Next();
        foreach (var row in rows)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO pg_server_config
    (collection_id, collection_time, server_id, server_name, name, setting, unit, category, context, vartype, source, boot_val, reset_val, sourcefile, sourceline, pending_restart, short_desc)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'Test', 'postmaster', 'string', 'configuration file', $8, $8, NULL, NULL, false, NULL)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(at);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(serverName);
            command.Parameters.AddWithValue(row.Name);
            command.Parameters.AddWithValue(row.Setting);
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)row.Unit ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.AddWithValue(row.BootVal);
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    /// <summary>
    /// One-minute <c>pg_write_stats</c> rows from <paramref name="start"/> for <paramref name="minutes"/> (both ends
    /// inclusive), cumulative counters: <paramref name="requestedPerMinute"/> / <paramref name="timedPerMinute"/>
    /// checkpoint increments; WAL cumulative from a base that grows <paramref name="walBytesPerMinuteBase"/> a minute
    /// with a deterministic ±5 % ripple, then <paramref name="walBytesPerMinuteSpike"/> from <paramref name="spikeFromMinute"/>;
    /// <c>null</c> WAL plants typed NULLs, the collector's Aurora shape. <c>wal_records</c> counts one per 8 kB. Reset
    /// stamps NULL throughout (never reset — the common state).
    /// </summary>
    private static async Task PlantWriteSeriesAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime start, int minutes,
        int requestedPerMinute, int timedPerMinute, long? walBytesPerMinuteBase, long? walBytesPerMinuteSpike, int spikeFromMinute, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
WITH s AS (
    SELECT n,
           CASE WHEN $8::bigint IS NULL THEN NULL
                WHEN n >= $10 THEN $9::bigint
                ELSE $8::bigint + round($8::bigint * 0.05 * sin(n)) END AS wal_inc
    FROM generate_series(0, $5) AS n
)
INSERT INTO pg_write_stats
    (collection_id, collection_time, server_id, server_name,
     num_timed, num_requested, num_done, checkpoint_write_time_ms, checkpoint_sync_time_ms, buffers_written_checkpoint, checkpointer_stats_reset,
     buffers_clean, maxwritten_clean, buffers_alloc, buffers_backend, buffers_backend_fsync, bgwriter_stats_reset,
     wal_records, wal_fpi, wal_bytes, wal_buffers_full, wal_write, wal_sync, wal_write_time_ms, wal_sync_time_ms, wal_stats_reset)
SELECT $1 + n, $2 + (n * interval '1 minute'), $3, $4,
       500 + n * $7, 1000 + n * $6, 1500 + n * ($6 + $7), 0, 0, 0, NULL,
       0, 0, 0, NULL, NULL, NULL,
       CASE WHEN wal_inc IS NULL THEN NULL ELSE (SUM(wal_inc) OVER (ORDER BY n)) / 8192 END,
       CASE WHEN wal_inc IS NULL THEN NULL ELSE 0 END,
       CASE WHEN wal_inc IS NULL THEN NULL ELSE 1000000000000 + SUM(wal_inc) OVER (ORDER BY n) END,
       CASE WHEN wal_inc IS NULL THEN NULL ELSE 0 END, NULL, NULL, NULL, NULL, NULL
FROM s", connection) { CommandTimeout = 120 };
        command.Parameters.AddWithValue(CollectionIdGenerator.Next() + 2_000_000L);
        command.Parameters.AddWithValue(start);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(minutes);
        command.Parameters.AddWithValue(requestedPerMinute);
        command.Parameters.AddWithValue(timedPerMinute);
        command.Parameters.Add(new NpgsqlParameter { Value = walBytesPerMinuteBase.HasValue ? walBytesPerMinuteBase.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = walBytesPerMinuteSpike.HasValue ? walBytesPerMinuteSpike.Value : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
        command.Parameters.AddWithValue(spikeFromMinute);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM pg_write_stats WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM pg_server_config WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM analysis_findings WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM analysis_muted WHERE server_id IN ({AuroraServerId}, {StockServerId}); " +
            $"DELETE FROM servers WHERE server_id IN ({AuroraServerId}, {StockServerId});", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
