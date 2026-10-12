/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731, the PostgreSQL-target half: an anomaly judged against a baseline that MEASURED zero. A bucket that
/// cleared its tier's sample and distinct-day floors and held nothing but zeros (<see cref="BaselineBucket.IsZeroHistory"/>)
/// is never <see cref="BaselineBucket.IsTrustworthy"/>, so the deadlock-rate and wait-profile detectors take their
/// <c>is_new</c> arm on it — right for the firing rule and the scorer, wrong for the words: "first occurrence, no
/// baseline yet" and "too thin to trust" describe a baseline the engine has not built, said about one it measured
/// as empty. The detectors now stamp <c>baseline_zero_history</c> beside <c>is_new</c> and the composers read the
/// stamp BEFORE <c>is_new</c>. Everything here is pure: the deadlock-rate rule runs through
/// <see cref="PgTargetAnomalyDetector.BuildDeadlockRateMetadata"/> (the detector's own body, factored out), the
/// wording through <see cref="PgTargetAdvice.Compose"/>, and the two wait-profile detectors' stamps are pinned in
/// their source because their reads need a database.
/// </summary>
public sealed class PgTargetZeroHistoryTests
{
    /// <summary>A FULL-tier bucket: 10 samples over 3 distinct days is where the tier's floors clear.</summary>
    private static BaselineBucket Bucket(double mean, double stddev, long samples, long days, double median = 0, double mad = 0) => new()
    {
        HourOfDay = 14,
        DayOfWeek = 2,
        Mean = mean,
        StdDev = stddev,
        Median = median,
        Mad = mad,
        SampleCount = samples,
        DistinctDays = days,
        Tier = BaselineTier.Full,
    };

    /// <summary>The floors cleared and every statistic zero: five same-weekday dates of this hour, never once non-zero.</summary>
    private static BaselineBucket ZeroHistory() => Bucket(mean: 0, stddev: 0, samples: 300, days: 5);

    /// <summary>The floors NOT cleared and every statistic zero: a young baseline that has not looked long enough to claim anything.</summary>
    private static BaselineBucket Thin() => Bucket(mean: 0, stddev: 0, samples: 2, days: 2);

    /// <summary>Floors cleared, real dispersion and a real rate: the trusted ratio arm.</summary>
    private static BaselineBucket Trusted() => Bucket(mean: 2.0, stddev: 1.0, samples: 300, days: 5, median: 2.0, mad: 0.5);

    private const double ObservedHours = 4.0;

    /// <summary>A window rate that clears every bar the deadlock rule sets: twice the absolute Warning tier.</summary>
    private static (long Deadlocks, double PerHour) LoudWindow()
    {
        var deadlocks = (long)Math.Ceiling(2 * AnomalyThresholds.PgDeadlockRateFallbackPerHour * ObservedHours);
        return (deadlocks, deadlocks / ObservedHours);
    }

    private static Dictionary<string, double>? Build(BaselineBucket bucket, long deadlocks, double perHour) =>
        PgTargetAnomalyDetector.BuildDeadlockRateMetadata(bucket, deadlocks, perHour, ObservedHours, topDatabaseCount: deadlocks);

    /// <summary>What the detector hands the composer: the rule's metadata on a fact for the deadlock family.</summary>
    private static Fact DeadlockFact(Dictionary<string, double> metadata) => new()
    {
        Source = PgTargetAnomalyDetector.AnomalySource,
        Key = PgTargetFactKeys.AnomalyDeadlockRate,
        Value = metadata["current_rate_per_hour"],
        ServerId = 1,
        DatabaseName = "appdb",
        Metadata = metadata,
    };

    private static AdviceBlock Compose(Fact fact) =>
        PgTargetAdvice.Compose(fact.Key, new Dictionary<string, Fact> { [fact.Key] = fact })!;

    private static Fact Anomaly(string key, params (string Name, double Value)[] metadata)
    {
        var fact = new Fact { Source = PgTargetAnomalyDetector.AnomalySource, Key = key, Value = 1, ServerId = 1 };
        foreach (var (name, value) in metadata)
            fact.Metadata[name] = value;
        return fact;
    }

    // ---------------------------------------------------------------- the deadlock-rate detector

    [Fact]
    public void DeadlockRate_ZeroHistoryBucket_AtTheFallbackRate_StampsTheMeasuredZero_AndKeepsTheFirstOccurrenceKeys()
    {
        var bucket = ZeroHistory();
        Assert.True(bucket.IsZeroHistory);
        Assert.False(bucket.IsTrustworthy);
        var (deadlocks, perHour) = LoudWindow();

        var metadata = Build(bucket, deadlocks, perHour);

        Assert.NotNull(metadata);
        Assert.Equal(1.0, metadata["baseline_zero_history"]);
        // The firing rule and the scorer's inputs are exactly what they were: the absolute Warning tier decides.
        Assert.Equal(1.0, metadata["is_new"]);
        Assert.Equal(0.0, metadata["ratio"]);
        Assert.Equal(perHour / AnomalyThresholds.PgDeadlockRateFallbackPerHour, metadata["fallback_exceedance"], precision: 9);
        Assert.Equal(0.0, metadata["baseline_rate"]);
        Assert.Equal(300.0, metadata["baseline_samples"]);
        Assert.Equal(5.0, metadata["baseline_distinct_days"]);
    }

    [Fact]
    public void DeadlockRate_ZeroHistoryBucket_BelowTheFallbackRate_StillDoesNotFire()
    {
        // 1 deadlock over 4 hours is 0.25/h, under the Warning tier: silence is the firing rule, unchanged by the stamp.
        Assert.True(0.25 < AnomalyThresholds.PgDeadlockRateFallbackPerHour);
        Assert.Null(Build(ZeroHistory(), deadlocks: 1, perHour: 0.25));
    }

    [Fact]
    public void DeadlockRate_ThinBucket_StampsNoZeroHistory_AndStaysAFirstOccurrence()
    {
        var bucket = Thin();
        Assert.False(bucket.IsZeroHistory);
        var (deadlocks, perHour) = LoudWindow();

        var metadata = Build(bucket, deadlocks, perHour);

        Assert.NotNull(metadata);
        Assert.Equal(0.0, metadata["baseline_zero_history"]);
        Assert.Equal(1.0, metadata["is_new"]);
        Assert.Equal(0.0, metadata["ratio"]);
        Assert.Equal(2.0, metadata["baseline_samples"]);
    }

    [Fact]
    public void DeadlockRate_TrustedBucket_CarriesTheRealMultiple_AndNoStamp()
    {
        var bucket = Trusted();
        Assert.True(bucket.IsTrustworthy);
        var perHour = bucket.Mean * (AnomalyThresholds.PgRatioAnomalyThreshold + 1);
        Assert.True(perHour >= AnomalyThresholds.PgDeadlockRateFloorPerHour);

        var metadata = Build(bucket, deadlocks: (long)Math.Ceiling(perHour * ObservedHours), perHour);

        Assert.NotNull(metadata);
        Assert.Equal(0.0, metadata["baseline_zero_history"]);
        Assert.Equal(0.0, metadata["is_new"]);
        Assert.Equal(AnomalyThresholds.PgRatioAnomalyThreshold + 1, metadata["ratio"], precision: 9);
        Assert.Equal(2.0, metadata["baseline_rate"]);
    }

    // ---------------------------------------------------------------- the deadlock-rate advice

    [Fact]
    public void ComposeDeadlockRatio_ZeroHistoryFact_ReadsAsAMeasuredZero_NotAFirstOccurrence()
    {
        var (deadlocks, perHour) = LoudWindow();
        var fact = DeadlockFact(Build(ZeroHistory(), deadlocks, perHour)!);

        // The detector stamps a zero-history bucket is_new = 1 as well; the stamp must win over it.
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        var block = Compose(fact);

        var span = $"{BaselineMath.BaselineWindowDays}-day";
        Assert.Contains($"deadlocks this window ({perHour:0.#}/hour) — against a {span} baseline in which this hour saw none", block.Headline, StringComparison.Ordinal);
        Assert.Contains("measured ZERO: 300 baseline samples across 5 distinct days, not one of them above zero", block.Investigation, StringComparison.Ordinal);
        Assert.Contains($"This server's {span} baseline for this hour-of-week is not thin", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("a rate that is zero has no multiple to print", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("deadlock-rate warning tier", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("appdb had the most", block.Investigation, StringComparison.Ordinal);

        var text = block.Headline + block.Investigation;
        Assert.DoesNotContain("first occurrence", text, StringComparison.Ordinal);
        Assert.DoesNotContain("no baseline yet", text, StringComparison.Ordinal);
        Assert.DoesNotContain("too thin", text, StringComparison.Ordinal);
        Assert.DoesNotContain("×", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeDeadlockRatio_ZeroHistoryFact_OfOneDeadlock_IsSingular_AndMissingCountsFallBackToThePlainClause()
    {
        var one = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate,
            ("current_count", 1), ("current_rate_per_hour", 5.5), ("observed_hours", 0.2), ("is_new", 1), ("ratio", 0),
            ("baseline_zero_history", 1), ("baseline_samples", 1), ("baseline_distinct_days", 1));
        var block = Compose(one);
        Assert.Contains("1 deadlock this window (5.5/hour) — against a", block.Headline, StringComparison.Ordinal);
        Assert.Contains("measured ZERO: 1 baseline sample across 1 distinct day, not one of them above zero", block.Investigation, StringComparison.Ordinal);

        // A fact that carries the stamp but no sample count (nothing to rest the claim on) still reads as a zero, plainly.
        var bare = Anomaly(PgTargetFactKeys.AnomalyDeadlockRate,
            ("current_count", 6), ("current_rate_per_hour", 6), ("observed_hours", 1), ("is_new", 1), ("ratio", 0), ("baseline_zero_history", 1));
        var bareBlock = Compose(bare);
        Assert.Contains("measured ZERO: this server's hour-of-week baseline", bareBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first occurrence", bareBlock.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeDeadlockRatio_IsNewWithoutTheStamp_KeepsTheFirstOccurrenceWording()
    {
        var (deadlocks, perHour) = LoudWindow();
        var thin = DeadlockFact(Build(Thin(), deadlocks, perHour)!);
        Assert.Equal(0.0, thin.Metadata["baseline_zero_history"]);
        var thinBlock = Compose(thin);
        Assert.Contains("first occurrence, no baseline yet", thinBlock.Headline, StringComparison.Ordinal);
        Assert.Contains("baseline is too thin to trust a ratio against yet", thinBlock.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("measured ZERO", thinBlock.Investigation, StringComparison.Ordinal);

        // A fact persisted before the stamp existed carries no key at all and reads exactly as it always did.
        var unstamped = DeadlockFact(Build(ZeroHistory(), deadlocks, perHour)!);
        unstamped.Metadata.Remove("baseline_zero_history");
        var unstampedBlock = Compose(unstamped);
        Assert.Contains("first occurrence, no baseline yet", unstampedBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("measured ZERO", unstampedBlock.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeDeadlockRatio_TrustedBucket_KeepsTheRatioWording()
    {
        var bucket = Trusted();
        var perHour = bucket.Mean * (AnomalyThresholds.PgRatioAnomalyThreshold + 1);
        var fact = DeadlockFact(Build(bucket, (long)Math.Ceiling(perHour * ObservedHours), perHour)!);

        var block = Compose(fact);

        Assert.Contains("about 4× its baseline for this time of week", block.Headline, StringComparison.Ordinal);
        Assert.Contains("about 4× the 2/hour this server normally sees at this hour-of-week", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("measured ZERO", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first occurrence", block.Headline, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------- the wait-profile advice

    [Fact]
    public void ComposeWaitProfile_ZeroHistoryFact_ReadsAsAMeasuredZero_NotATooThinBaseline()
    {
        var fact = Anomaly(PgTargetFactKeys.AnomalyWaitProfile,
            ("current_ms_per_sec", 900), ("mean_ms_per_sec", 640), ("is_new", 1), ("ratio", 0), ("baseline_mean", 0),
            ("baseline_zero_history", 1), ("baseline_samples", 300), ("baseline_distinct_days", 5),
            ("contrib_Lock:relation", 900_000), ("contrib_IO:DataFileRead", 500_000));

        var block = Compose(fact);

        var span = $"{BaselineMath.BaselineWindowDays}-day";
        Assert.Equal($"The cluster's wait profile reached 900 ms/sec — against a {span} baseline in which this hour saw no waiting", block.Headline);
        Assert.Contains("measured ZERO: 300 baseline samples across 5 distinct days, not one of them above zero", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("(CPU excluded; the window's mean was about 640 ms/sec), led by Lock:relation, IO:DataFileRead", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("the engine measured", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("estimated from sampling", block.Headline + block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("no baseline yet", block.Headline + block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("too thin", block.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeWaitProfile_IsNewWithoutTheStamp_KeepsTheFirstOccurrenceWording()
    {
        var fact = Anomaly(PgTargetFactKeys.AnomalyWaitProfile,
            ("current_ms_per_sec", 900), ("mean_ms_per_sec", 640), ("is_new", 1), ("ratio", 0), ("baseline_zero_history", 0), ("baseline_samples", 2));

        var block = Compose(fact);

        Assert.Equal("The cluster's wait profile is heavy, with no baseline yet for this time of week", block.Headline);
        Assert.Contains("(CPU excluded; the window's mean was about 640 ms/sec)", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("hour-of-week wait baseline is too thin to trust a deviation against yet", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("measured ZERO", block.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeSampledWaitProfile_ZeroHistoryFact_ReadsAsAMeasuredZero_InTheSampledGradesWords()
    {
        var fact = Anomaly(PgTargetFactKeys.AnomalySampledWaitProfile,
            ("current_ms_per_sec", 900), ("mean_ms_per_sec", 640), ("is_new", 1), ("ratio", 0), ("baseline_mean", 0),
            ("baseline_zero_history", 1), ("baseline_samples", 300), ("baseline_distinct_days", 5),
            (PgTargetScorer.WaitSampledMsKnownKey, 1), ("contrib_Lock:relation", 900_000));

        var block = Compose(fact);

        var span = $"{BaselineMath.BaselineWindowDays}-day";
        Assert.Contains($"against a {span} baseline in which the sampler saw no waiting in this hour (estimated from sampling)", block.Headline, StringComparison.Ordinal);
        Assert.Contains("measured ZERO: 300 baseline samples across 5 distinct days, not one of them above zero", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("estimated from sampling", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("the engine measured", block.Headline + block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("no baseline yet", block.Headline + block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("too thin", block.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ComposeSampledWaitProfile_IsNewWithoutTheStamp_KeepsTheFirstOccurrenceWording()
    {
        var fact = Anomaly(PgTargetFactKeys.AnomalySampledWaitProfile,
            ("current_ms_per_sec", 900), ("mean_ms_per_sec", 640), ("is_new", 1), ("ratio", 0), ("baseline_zero_history", 0),
            (PgTargetScorer.WaitSampledMsKnownKey, 1));

        var block = Compose(fact);

        Assert.Contains("with no baseline yet for this time of week (estimated from sampling)", block.Headline, StringComparison.Ordinal);
        Assert.Contains("sampled-wait baseline is too thin to trust a deviation against yet", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("measured ZERO", block.Investigation, StringComparison.Ordinal);
    }

    // ---------------------------------------------------------------- the wait-profile detectors' stamps

    /// <summary>
    /// The two wait-profile detectors read a database, so their stamp is pinned in the source: each stamps
    /// <c>baseline_zero_history</c> from the bucket the peak was judged against (<c>scoredBucket</c> — the tile's own
    /// bucket on the tiled trusted arm, the start bucket everywhere else), inside the metadata it hands the composer.
    /// </summary>
    [Theory]
    [InlineData("PgTargetAnomalyDetector.cs", @"Task\s+DetectWaitProfileAnomalies\s*\(")]
    [InlineData("PgTargetAnomalyDetector.WaitsSampled.cs", @"Task\s+DetectSampledWaitProfileAnomalies\s*\(")]
    public void WaitProfileDetectors_StampTheMeasuredZero_FromTheBucketTheyJudged(string file, string anchor)
    {
        var raw = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", file);
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(raw);
        var declaration = Regex.Match(stripped, anchor);
        Assert.True(declaration.Success, $"{file}: the wait-profile detector was not found; this pin's anchor is stale.");

        var open = stripped.IndexOf('{', declaration.Index);
        var rawBody = raw.Substring(open, CSharpSourceWalker.BraceBalanced(stripped, open).Length);

        Assert.Matches(
            @"\[\s*""is_new""\s*\]\s*=\s*isNew\s*\?\s*1\s*:\s*0\s*,(?:\s*/\*(?:(?!\*/).)*\*/)?\s*\[\s*""baseline_zero_history""\s*\]\s*=\s*scoredBucket\.IsZeroHistory\s*\?\s*1\s*:\s*0\s*,",
            rawBody.Replace("\r\n", "\n").Replace('\n', ' '));
    }
}
