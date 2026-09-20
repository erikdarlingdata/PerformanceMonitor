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
using System.Reflection;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Stock PostgreSQL's SAMPLED wait profile (#3691 lane 24): the <c>pg_sampled_wait_ms_per_sec</c> baseline arm and
/// the <c>ANOMALY_PG_SAMPLED_WAIT_PROFILE</c> detector — their SQL's shape (the V133 <c>sampled_ms</c> denominator with
/// NULL read as the whole interval, the reader's reset rule, CPU/Running excluded, the other source's cross-count),
/// the key's routing through every shared switch (ratio family, the modified-z ramp, the extremity escape, the
/// wait-profile amplifiers, the reconciler's per-story fold onto the dominant wait, the advice arm, the tool
/// recommendation), the bar's lineage (unmeasured, stated, not an alias of the Aurora bar), and the retention floor.
/// The collector half of the lane (the honest fraction on the wait facts) is pinned in <c>PgTargetWaitTests</c>; the
/// live half (31 days of sampler rows, the real <c>analyze_server</c>) in <c>PgTargetSampledWaitLiveTests</c>.
/// </summary>
public sealed class PgTargetSampledWaitTests
{
    /* ── the baseline arm ── */

    [Fact]
    public void TheSampledArm_IsItsOwnMetric_OverPgWaitSampling_DividingBySampledMsWithNullAsTheWholeInterval_EndingInTheScaffold()
    {
        Assert.Equal("pg_sampled_wait_ms_per_sec", MetricNames.PgSampledWaitMsPerSec);
        Assert.NotEqual(MetricNames.PgWaitMsPerSec, MetricNames.PgSampledWaitMsPerSec);
        Assert.Null(PgBaselineProvider.GetBaselineQuery(MetricNames.PgSampledWaitMsPerSec));

        var sql = PgTargetBaselineProvider.GetPgTargetBaselineQuery(MetricNames.PgSampledWaitMsPerSec);
        Assert.NotNull(sql);
        Assert.Contains("FROM pg_wait_sampling", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM pg_wait_stats", sql, StringComparison.Ordinal);   /* never pooled with the measured series */
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time < $3", sql, StringComparison.Ordinal);
        Assert.EndsWith(PgBaselineProvider.RobustTierScaffold, sql, StringComparison.Ordinal);
        Assert.Contains("clean AS (", sql, StringComparison.Ordinal);
        /* The reader's reset rule (newest whole, never GREATEST), the series identity, the CPU exclusion, the V133 denominator. */
        Assert.Contains("LAG(sample_count) OVER (PARTITION BY event_type, event, query_id ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN sample_count < prev_count THEN sample_count", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("GREATEST(", sql, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE lower(event_type) IS DISTINCT FROM 'cpu')", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(sampled_ms) AS sampled_ms", sql, StringComparison.Ordinal);
        Assert.Contains("coalesce(sampled_ms / 1000.0, interval_sec)", sql, StringComparison.Ordinal);
        Assert.Contains("AS DOUBLE PRECISION", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("30000", sql, StringComparison.Ordinal);   /* never a guessed 30 s */
        Assert.DoesNotContain("ELSE 0", sql, StringComparison.Ordinal);

        /* Routed through the root's switch to the partial, like every v2 arm. */
        var provider = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetBaselineProvider.cs");
        Assert.Contains("MetricNames.PgSampledWaitMsPerSec => SampledWaitBaselineQuery(),", provider, StringComparison.Ordinal);
        Assert.Contains("private static partial string? SampledWaitBaselineQuery();", provider, StringComparison.Ordinal);
    }

    /// <summary>D10: the arm reads <c>pg_wait_sampling</c> directly, so its purge horizon is floored at the window.</summary>
    [Fact]
    public void PgWaitSampling_JoinsTheBaselineServingFlooredSet()
    {
        Assert.Contains("pg_wait_sampling", DarlingRetention.BaselineServingRawCollectors);
        Assert.Equal(BaselineMath.BaselineWindowDays, DarlingRetention.EffectivePurgeRetentionDays("pg_wait_sampling", 7));
    }

    /* ── the detector ── */

    [Fact]
    public void TheDetectorReads_ShareTheArmsDenominatorAndDifferencing_CountTheExactSource_AndRateBothPeakAndMean()
    {
        var rate = PgTargetAnomalyDetector.SampledWaitRateWindowSql;
        Assert.Contains("FROM pg_wait_sampling", rate, StringComparison.Ordinal);
        Assert.Contains("server_id = $1 AND collection_time >= $2 AND collection_time <= $3", rate, StringComparison.Ordinal);
        Assert.Contains("MAX(sampled_ms) AS sampled_ms", rate, StringComparison.Ordinal);
        Assert.Contains("coalesce(sampled_ms / 1000.0, interval_sec)", rate, StringComparison.Ordinal);
        Assert.Contains("WHEN sample_count < prev_count THEN sample_count", rate, StringComparison.Ordinal);
        Assert.Contains("FILTER (WHERE lower(event_type) IS DISTINCT FROM 'cpu')", rate, StringComparison.Ordinal);
        /* #3653 / #3724: the peak AND the mean of the same per-collection series. */
        Assert.Contains("MAX(total_wait_ms / observed_sec)", rate, StringComparison.Ordinal);
        Assert.Contains("AVG(total_wait_ms / observed_sec)", rate, StringComparison.Ordinal);
        Assert.Contains("AS unknown_sampled_collections", rate, StringComparison.Ordinal);
        /* Both sources: the exact table's collections over the same window ride on the row the detector reads. */
        Assert.Contains("FROM pg_wait_stats AS w", rate, StringComparison.Ordinal);
        Assert.Contains("AS exact_collections", rate, StringComparison.Ordinal);

        var contrib = PgTargetAnomalyDetector.SampledWaitContribWindowSql;
        Assert.Contains("FROM pg_wait_sampling", contrib, StringComparison.Ordinal);
        Assert.Contains("lower(event_type) IS DISTINCT FROM 'cpu'", contrib, StringComparison.Ordinal);
        Assert.Contains("LIMIT 6", contrib, StringComparison.Ordinal);
        Assert.Contains("ORDER BY total_ms DESC", contrib, StringComparison.Ordinal);

        /* The detector body: the fence, the sit-outs, the gate asked of both statistics, the lineage stamp, the key. */
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.WaitsSampled.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Contains("catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))", code, StringComparison.Ordinal);
        Assert.Contains("MetricNames.PgSampledWaitMsPerSec", code, StringComparison.Ordinal);
        Assert.Contains("if (collectionCount == 0 || exactCollections > 0 || sampleCount == 0) return;", code, StringComparison.Ordinal);
        Assert.Contains("modifiedZ < HeavyTailModifiedZThreshold || meanModifiedZ < HeavyTailModifiedZThreshold || peakRate < PgSampledWaitProfileFallbackMsPerSec", code, StringComparison.Ordinal);
        Assert.Contains("ratio < PgRatioAnomalyThreshold || meanRatio < PgRatioAnomalyThreshold || peakRate < PgSampledWaitProfileFallbackMsPerSec", code, StringComparison.Ordinal);
        /* The is_new arm is the one arm NOT pair-gated — lane 35's ruling; the parity pin below owns its shape. */
        Assert.Contains("if (fallbackExceedance < 1.0) return;", code, StringComparison.Ordinal);
        Assert.DoesNotContain("meanRate < PgSampledWaitProfileFallbackMsPerSec", code, StringComparison.Ordinal);
        Assert.Contains("Key = PgTargetFactKeys.AnomalySampledWaitProfile", code, StringComparison.Ordinal);
        Assert.DoesNotContain("PgWaitProfileFallbackMsPerSec", code, StringComparison.Ordinal);   /* its own bar, never the Aurora one */
        Assert.DoesNotContain("DateTime.UtcNow", code, StringComparison.Ordinal);
        Assert.DoesNotContain("engine_kind", code, StringComparison.Ordinal);

        /* Reachable from the root, after the wave-3 detector. */
        var root = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs"));
        var blocking = root.IndexOf("await DetectBlockingAnomalies(context, anomalies);", StringComparison.Ordinal);
        var sampled = root.IndexOf("await DetectSampledWaitProfileAnomalies(context, anomalies);", StringComparison.Ordinal);
        Assert.True(blocking > 0 && sampled > blocking);
    }

    /// <summary>
    /// The #3691 line "RULED 2026-09-20 → lane 35: the sampled twin matches the unsampled twin's <c>is_new</c>
    /// gating": the sampled detector's first-occurrence arm gates on the PEAK's bar alone, textually the Aurora
    /// twin's clause (<c>if (fallbackExceedance &lt; 1.0) return;</c>, no <c>meanRate &lt;</c> bar), while the robust
    /// and ratio arms keep the #3724 pair gate. Lane 24 had pair-gated <c>is_new</c>; the Aurora twin (#3780) and the
    /// SQL Server profile (#3773) never did, by #3741's ruling.
    ///
    /// <para>The arithmetic is EXECUTED here with the detector's constants and <see cref="BaselineMath.ModifiedZScore"/>
    /// — the sampled arm's gates are inline predicates with no shared call to invoke, so the pin states each predicate
    /// in the detector's own terms and the source pin above holds the text to it. Fixture: a young stock server
    /// (untrustworthy bucket) whose window has ONE hot sampler cycle at 1,700 ms/sec watched over a flat 333 ms/sec:
    /// the peak clears the 500 bar 3.4×, the window mean (362) does not — FIRES now (was: held by the mean clause),
    /// and the same peak through the Aurora arm's clause fires identically. With a trustworthy bucket (lane 24's live
    /// planting: median 333, MAD 33) the same one-cycle spike does NOT fire — the mean's modified z is under the
    /// heavy-tail cutoff — and a sustained shift to 1,700 fires on both statistics; the classical arm holds the
    /// spike on <c>meanRatio</c> and fires the shift. The pair gate is intact where a baseline exists.</para>
    /// </summary>
    [Fact]
    public void TheFirstOccurrenceArm_GatesOnThePeakAloneLikeItsTwins_WhileTheBaselineArmsKeepThePairGate()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.WaitsSampled.cs");
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var start = code.IndexOf("private async partial Task DetectSampledWaitProfileAnomalies(", StringComparison.Ordinal);
        Assert.True(start > 0, "the sampled wait-profile detector moved");
        var body = code[start..];
        /* Three arms, in order: robust (pair), classical (pair), first-occurrence (peak alone). */
        var robust = body.IndexOf("modifiedZ < HeavyTailModifiedZThreshold || meanModifiedZ < HeavyTailModifiedZThreshold || peakRate < PgSampledWaitProfileFallbackMsPerSec", StringComparison.Ordinal);
        var classical = body.IndexOf("ratio < PgRatioAnomalyThreshold || meanRatio < PgRatioAnomalyThreshold || peakRate < PgSampledWaitProfileFallbackMsPerSec", StringComparison.Ordinal);
        var first = body.IndexOf("if (fallbackExceedance < 1.0) return;", StringComparison.Ordinal);
        Assert.True(robust > 0 && classical > robust && first > classical, "the three arms are not in lane 24's order");
        Assert.DoesNotContain("meanRate < PgSampledWaitProfileFallbackMsPerSec", body, StringComparison.Ordinal);
        Assert.Contains("fallbackExceedance = peakRate / PgSampledWaitProfileFallbackMsPerSec;", body, StringComparison.Ordinal);
        /* Twin parity, textually: the Aurora arm's first-occurrence clause is the same text. */
        var aurora = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "PgTargetAnomalyDetector.cs"));
        var auroraStart = aurora.IndexOf("private async Task DetectWaitProfileAnomalies(", StringComparison.Ordinal);
        var auroraEnd = aurora.IndexOf("\n    internal const string AnomalySource", auroraStart, StringComparison.Ordinal);
        Assert.True(auroraStart > 0 && auroraEnd > auroraStart);
        var auroraBody = aurora[auroraStart..auroraEnd];
        Assert.Contains("fallbackExceedance = peakRate / PgWaitProfileFallbackMsPerSec;", auroraBody, StringComparison.Ordinal);
        Assert.Contains("if (fallbackExceedance < 1.0) return;", auroraBody, StringComparison.Ordinal);
        Assert.DoesNotContain("meanRate < PgWaitProfileFallbackMsPerSec", auroraBody, StringComparison.Ordinal);
        /* The ruling's lineage note travels with the arm. */
        Assert.Contains("population 0 on the dogfood fleet", source, StringComparison.Ordinal);
        Assert.Contains("revisit when a", source, StringComparison.Ordinal);

        /* ── The arithmetic, executed. Window: 48 countable cycles, 47 quiet at 333.3 ms/sec watched, one at 1,700. ── */
        const double bar = AnomalyThresholds.PgSampledWaitProfileFallbackMsPerSec;
        const double quiet = 10_000.0 / 30.0;   /* (4 + 6) s of sampled waiting over 30 s watched */
        const double spike = 1_700.0;           /* (45 + 6) s over 30 s watched — lane 24's heavy cycle */
        var spikeMean = (47 * quiet + spike) / 48;
        Assert.InRange(spikeMean, 360.0, 365.0);

        /* First occurrence (no trustworthy baseline): the peak's bar alone. */
        var fallbackExceedance = spike / bar;
        Assert.Equal(3.4, fallbackExceedance, precision: 9);
        Assert.False(fallbackExceedance < 1.0, "the first-occurrence spike fires on the peak");
        Assert.True(spikeMean < bar, "…and the retired mean clause would have held it — the red this pin turns green");
        /* Twin parity, numerically: the same fixture through the Aurora arm's clause and bar. */
        Assert.False(spike / AnomalyThresholds.PgWaitProfileFallbackMsPerSec < 1.0);
        Assert.Equal(spike / AnomalyThresholds.PgWaitProfileFallbackMsPerSec, fallbackExceedance, precision: 9);
        /* The fact the arm would stamp: is_new 1, ratio 0, fire_threshold 0 — the grade the scorer reads. */
        var isNew = Anomaly(PgTargetFactKeys.AnomalySampledWaitProfile, ("is_new", 1), ("ratio", 0), ("fallback_exceedance", fallbackExceedance), ("current_ms_per_sec", spike), ("mean_ms_per_sec", spikeMean));
        Assert.True(PgTargetScorer.ScoreRatioAnomaly(isNew) > 0);

        /* A trustworthy robust bucket (lane 24's live planting): the same spike does NOT fire — the pair gate. */
        var bucket = new BaselineBucket
        {
            Tier = BaselineTier.Full, HourOfDay = 12, DayOfWeek = 3,
            Mean = quiet, StdDev = 27.2, Median = quiet, Mad = 1_000.0 / 30.0,
            SampleCount = 250, DistinctDays = 5, AbsStdDevFloor = 0,
        };
        Assert.True(bucket.IsTrustworthy && bucket.EffectiveRobustSigma > 0);
        var peakZ = BaselineMath.ModifiedZScore(bucket, spike);
        var spikeMeanZ = BaselineMath.ModifiedZScore(bucket, spikeMean);
        var shiftMeanZ = BaselineMath.ModifiedZScore(bucket, spike);   /* the whole window at 1,700: mean = peak */
        Assert.True(peakZ >= AnomalyThresholds.HeavyTailModifiedZThreshold && spike >= bar, "the peak clears on both windows");
        Assert.True(spikeMeanZ < AnomalyThresholds.HeavyTailModifiedZThreshold, "one hot cycle over a flat window: the mean's z holds it");
        Assert.True(peakZ < AnomalyThresholds.HeavyTailModifiedZThreshold || spikeMeanZ < AnomalyThresholds.HeavyTailModifiedZThreshold || spike < bar, "the robust arm's exact predicate returns (holds) on the spike");
        Assert.False(peakZ < AnomalyThresholds.HeavyTailModifiedZThreshold || shiftMeanZ < AnomalyThresholds.HeavyTailModifiedZThreshold || spike < bar, "…and fires on the sustained shift");

        /* The classical arm (trustworthy, no MAD): the ratio on both; the spike's mean ratio is under 3×. */
        var ratio = spike / bucket.Mean;
        var spikeMeanRatio = spikeMean / bucket.Mean;
        var shiftMeanRatio = spike / bucket.Mean;
        Assert.True(ratio >= AnomalyThresholds.PgRatioAnomalyThreshold);
        Assert.True(spikeMeanRatio < AnomalyThresholds.PgRatioAnomalyThreshold);
        Assert.True(ratio < AnomalyThresholds.PgRatioAnomalyThreshold || spikeMeanRatio < AnomalyThresholds.PgRatioAnomalyThreshold || spike < bar, "the classical arm holds the spike on meanRatio");
        Assert.False(ratio < AnomalyThresholds.PgRatioAnomalyThreshold || shiftMeanRatio < AnomalyThresholds.PgRatioAnomalyThreshold || spike < bar, "…and fires the shift");
    }

    /* ── the bar ── */

    [Fact]
    public void TheSampledBar_IsDeclaredOnItsOwnUnmeasuredLineage_EqualInValueButNotAnAlias_OfTheAuroraBar()
    {
        Assert.Equal(500.0, AnomalyThresholds.PgSampledWaitProfileFallbackMsPerSec);
        Assert.Equal(AnomalyThresholds.PgWaitProfileFallbackMsPerSec, AnomalyThresholds.PgSampledWaitProfileFallbackMsPerSec);

        var lines = RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "Baselines", "AnomalyThresholds.cs").Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var at = Array.FindIndex(lines, l => l.Contains("public const double PgSampledWaitProfileFallbackMsPerSec ", StringComparison.Ordinal));
        Assert.True(at > 0);
        /* The declaration is a literal, not `= PgWaitProfileFallbackMsPerSec` — the two instruments calibrate apart. */
        Assert.Contains("= 500.0;", lines[at], StringComparison.Ordinal);
        var block = string.Join('\n', lines[Math.Max(0, at - 16)..(at + 1)]);
        Assert.Contains("unmeasured:", block, StringComparison.Ordinal);
        Assert.Contains("NOT an alias", block, StringComparison.Ordinal);
        Assert.Contains("sampled_ms", block, StringComparison.Ordinal);
        Assert.Contains("// lane 24", block, StringComparison.Ordinal);
    }

    /* ── the shared switches ── */

    [Fact]
    public void TheKey_IsARatioFamily_GradesOnTheProfileRamps_EscapesWhenExtreme_AndTakesTheProfileAmplifiers()
    {
        var key = PgTargetFactKeys.AnomalySampledWaitProfile;
        Assert.Equal("ANOMALY_PG_SAMPLED_WAIT_PROFILE", key);
        Assert.True(PgTargetFactKeys.IsPgAnomalyKey(key));
        Assert.True(PgTargetFactKeys.IsWaitProfileAnomaly(key));
        Assert.True(PgTargetFactKeys.IsWaitProfileAnomaly(PgTargetFactKeys.AnomalyWaitProfile));
        Assert.False(PgTargetFactKeys.IsWaitProfileAnomaly(PgTargetFactKeys.AnomalyDeadlockRate));
        Assert.False(PgTargetFactKeys.IsWaitProfileAnomaly("ANOMALY_WAIT_PROFILE"));
        Assert.False(PgTargetFactKeys.IsWaitProfileAnomaly(null));

        Assert.True(PgTargetScorer.IsPgRatioAnomalyKey(key));
        Assert.False(PgTargetScorer.IsDeviationScoredAnomalyKey(key));

        /* The three readings, the Aurora profile's exactly: first occurrence off the exceedance; robust off modified z
           (0.5 at 5σ, 1.0 at 15σ); classical off the ratio (0.5 at 3×, 1.0 at 9×). Every graded fact says lineage 0. */
        var isNew = Anomaly(key, ("is_new", 1), ("fallback_exceedance", 1.5));
        Assert.Equal(0.75, PgTargetScorer.ScoreRatioAnomaly(isNew), precision: 9);
        Assert.Equal(0, isNew.Metadata["threshold_lineage"]);
        Assert.Equal(0.0, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("modified_z", 4.9))));
        Assert.Equal(0.5, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("modified_z", 5.0))), precision: 9);
        Assert.Equal(0.75, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("modified_z", 10.0))), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("modified_z", 15.0))), precision: 9);
        Assert.Equal(0.0, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("ratio", 2.9))));
        Assert.Equal(0.5, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("ratio", 3.0))), precision: 9);
        Assert.Equal(1.0, PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("ratio", 9.0))), precision: 9);
        Assert.Equal(
            PgTargetScorer.ScoreRatioAnomaly(Anomaly(PgTargetFactKeys.AnomalyWaitProfile, ("modified_z", 8.0))),
            PgTargetScorer.ScoreRatioAnomaly(Anomaly(key, ("modified_z", 8.0))), precision: 9);

        /* The extremity escape, on the same statistic the ramp grades from. */
        Assert.True(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(key, ("modified_z", 15.0)), 3.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(key, ("modified_z", 14.9)), 3.0));
        Assert.True(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(key, ("ratio", 9.0)), 3.0));
        Assert.False(PgTargetScorer.IsExtremeWaitProfileAnomaly(Anomaly(key, ("modified_z", 40.0), ("is_new", 1)), 3.0));

        /* Reachable through the shared scorer: extreme + a fired sampled standout + a load sibling → past 1.49. */
        var profile = Anomaly(key, ("modified_z", 40.0), ("ratio", 60.0));
        var sampledStandout = new Fact
        {
            Source = PgTargetSources.WaitsSource, Key = PgTargetFactKeys.WaitKey("Lock", "relation"), Value = 0.5,
            Metadata = { ["wait_fraction"] = 0.5, ["is_standout"] = 1, [PgTargetScorer.WaitIsSampledKey] = 1, [PgTargetScorer.WaitDeltaSamplesKey] = 5_000, [PgTargetScorer.WaitSourceObservedMsKey] = 1_440_000 },
        };
        var set = new List<Fact>
        {
            profile, sampledStandout,
            Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)),
        };
        new FactScorer().ScoreAll(set);
        Assert.Equal(1.0, profile.BaseSeverity, precision: 9);
        Assert.Equal(2, profile.AmplifierResults.Count(r => r.Matched));
        Assert.True(profile.Severity > 1.49, $"an extreme, corroborated sampled profile must leave the tuning-class cap; scored {profile.Severity}");

        /* Un-extreme: capped, however corroborated. */
        var modest = Anomaly(key, ("modified_z", 14.0));
        var modestSet = new List<Fact> { modest, sampledStandout, Anomaly(PgTargetFactKeys.AnomalySessionSpike, ("deviation_sigma", 4.0), ("fire_threshold", 3.5)) };
        new FactScorer().ScoreAll(modestSet);
        Assert.Equal(1.49, modest.Severity, precision: 9);

        /* The same five amplifiers as the Aurora profile (the CPU confirmer is inert on stock — no PG_CPU_PERCENT fact). */
        var amplifiers = typeof(PgTargetScorer).GetMethod("Amplifiers", BindingFlags.Static | BindingFlags.NonPublic)!;
        Assert.Equal(5, ((System.Collections.IEnumerable)amplifiers.Invoke(null, [key])!).Cast<object>().Count());
    }

    [Fact]
    public void TheStory_FoldsOntoTheDominantSampledWait_NotAStaticFamily_AndHasAToolRow()
    {
        var key = PgTargetFactKeys.AnomalySampledWaitProfile;
        Assert.False(PgTargetFactKeys.AnomalyToFamilies.ContainsKey(key));
        Assert.Equal(
            new[] { PgTargetFactKeys.WaitKey("IO", "DataFileRead"), PgTargetFactKeys.WaitKey("IO", null) },
            PgTargetFactKeys.WaitProfileFamilies(new Dictionary<string, double> { ["contrib_IO:DataFileRead"] = 900_000, ["contrib_Lock:relation"] = 100 }));

        var standout = Story(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), "inc-standout", 0.8);
        var profile = Story(key, "inc-profile", 0.6);
        profile.RootFactMetadata = new Dictionary<string, double>(StringComparer.Ordinal) { ["contrib_IO:DataFileRead"] = 900_000, ["contrib_Lock:relation"] = 100 };
        AnomalyIncidentReconciler.Reconcile([standout, profile]);
        Assert.Equal("inc-standout", profile.IncidentId);

        var bare = Story(key, "inc-bare", 0.6);
        AnomalyIncidentReconciler.Reconcile([Story(PgTargetFactKeys.WaitKey("IO", "DataFileRead"), "inc-x", 0.8), bare]);
        Assert.Equal("inc-bare", bare.IncidentId);

        /* The reconciler routes on the predicate, not on a literal, so the two instruments cannot drift apart. */
        var reconciler = CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFile("PerformanceMonitor.Analysis", "AnomalyIncidentReconciler.cs"));
        Assert.Contains("PgTargetFactKeys.IsWaitProfileAnomaly(anomaly.RootFactKey)", reconciler, StringComparison.Ordinal);

        var tools = PgTargetToolRecommendations.GetForKey(key);
        Assert.NotNull(tools);
        Assert.Contains(tools!, t => t.Tool == "get_pg_wait_sampling");
    }

    /* ── the advice ── */

    [Fact]
    public void TheAdvice_SaysEstimatedFromSampling_StatesTheDutyCycleAndTheMean_AndNeverClaimsTheEngineMeasured()
    {
        var key = PgTargetFactKeys.AnomalySampledWaitProfile;
        var staticBlock = PgTargetAdvice.Static(key);
        Assert.NotNull(staticBlock);
        Assert.Equal(staticBlock, FactAdvice.GetForFactKey(key));
        Assert.Contains("estimated from sampling", staticBlock!.Headline, StringComparison.Ordinal);
        Assert.Contains("get_pg_wait_sampling", staticBlock.Remediation, StringComparison.Ordinal);

        var fact = Anomaly(key,
            ("current_ms_per_sec", 1_200.0), ("mean_ms_per_sec", 1_100.0), ("baseline_mean", 60.0), ("modified_z", 22.4), ("ratio", 20.0),
            (PgTargetScorer.WaitSourceObservedMsKey, 1_440_000), (PgTargetScorer.WaitSourceIntervalMsKey, 14_400_000), (PgTargetScorer.WaitSampledMsKnownKey, 1),
            ("contrib_Lock:relation", 1_500_000), ("contrib_IO:DataFileRead", 200_000), ("contrib_LWLock:WALWrite", 50));
        var composed = PgTargetAdvice.Compose(key, new[] { fact }.ToFactLookup())!;
        var text = composed.Headline + "\n" + composed.Investigation + "\n" + composed.Remediation;
        Assert.Contains("1200 ms/sec", composed.Headline, StringComparison.Ordinal);
        Assert.Contains("22.4σ above its baseline", composed.Headline, StringComparison.Ordinal);
        Assert.Contains("(estimated from sampling)", composed.Headline, StringComparison.Ordinal);
        Assert.Contains("mean 1100 ms/sec, also above the bar", composed.Investigation, StringComparison.Ordinal);
        Assert.Contains("led by Lock:relation, IO:DataFileRead, LWLock:WALWrite", composed.Investigation, StringComparison.Ordinal);
        Assert.Contains("The sampler watched 24 min of the 4 h between collections", composed.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("did not record how long the sampler watched", composed.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("the engine measured", text, StringComparison.Ordinal);
        Assert.DoesNotContain("spent", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("aurora", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("synchronous_commit", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("CREATE INDEX", text, StringComparison.OrdinalIgnoreCase);

        /* The pre-V133 caveat when the denominator was partly blind; the first-occurrence rendering with no sigma. */
        var blind = Anomaly(key, ("current_ms_per_sec", 600.0), ("mean_ms_per_sec", 550.0), ("is_new", 1), ("fallback_exceedance", 1.2),
            (PgTargetScorer.WaitSourceObservedMsKey, 14_400_000), (PgTargetScorer.WaitSourceIntervalMsKey, 14_400_000), (PgTargetScorer.WaitSampledMsKnownKey, 0));
        var first = PgTargetAdvice.Compose(key, new[] { blind }.ToFactLookup())!;
        Assert.Contains("no baseline yet", first.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("σ", first.Headline, StringComparison.Ordinal);
        Assert.Contains("did not record how long the sampler watched", first.Investigation, StringComparison.Ordinal);
        Assert.Contains("the baseline was built the same way", first.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("The sampler watched", first.Investigation, StringComparison.Ordinal);
        Assert.Contains("first look", first.Investigation, StringComparison.Ordinal);

        /* The shared composer routes the key here and not to the SQL Server "Anomalous spike" block. */
        Assert.Equal(composed, FactAdvice.Compose(key, new[] { fact }.ToFactLookup()));
    }

    /* ── helpers ── */

    private static Fact Anomaly(string key, params (string Name, double Value)[] metadata)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = key, Value = 1, ServerId = 1 };
        foreach (var (name, value) in metadata)
            fact.Metadata[name] = value;
        return fact;
    }

    private static AnalysisStory Story(string rootKey, string incidentId, double severity) => new()
    {
        RootFactKey = rootKey,
        Path = [rootKey],
        StoryPath = rootKey,
        Severity = severity,
        IncidentId = incidentId,
    };
}
