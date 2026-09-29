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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4731: the two SQL Server count families, <c>ANOMALY_BLOCKING_SPIKE</c> and <c>ANOMALY_DEADLOCK_SPIKE</c>,
/// against a zero-history baseline. Both products assemble these facts' metadata through
/// <see cref="CountFamilyMetadata.Build"/>, so one set of pins holds both: the function stamps
/// <c>baseline_zero_history</c> without touching <c>ratio</c> or <c>is_new</c>, and
/// <c>FactAdvice.ComposeAnomalyRatio</c> reads that stamp BEFORE <c>is_new</c> — a bucket that is a measured
/// zero is never trustworthy, so the detector stamps it <c>is_new = 1</c> as well, and read in the old order
/// it was worded "first occurrence, no baseline yet". The wiring of each product's detector to the shared
/// function is pinned in <c>DarlingAnomalyBaselineTests</c>.
/// </summary>
public sealed class CountFamilyZeroHistoryTests
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

    /// <summary>The floors cleared and every statistic zero: a month in which this hour saw none.</summary>
    private static BaselineBucket ZeroHistory() => Bucket(mean: 0, stddev: 0, samples: 30, days: 30);

    /// <summary>The floors NOT cleared and every statistic zero: a young baseline that has not looked long
    /// enough to claim anything.</summary>
    private static BaselineBucket Thin() => Bucket(mean: 0, stddev: 0, samples: 2, days: 2);

    /// <summary>Floors cleared, real rate: the trusted ratio arm.</summary>
    private static BaselineBucket Trusted() => Bucket(mean: 2.0, stddev: 0, samples: 30, days: 30);

    /// <summary>What a detector hands the composer: the shared function's keys, then the baseline context the
    /// detector's own <c>AddBaselineContext</c> adds (only the distinct-day count matters to the wording).</summary>
    private static Fact CountFact(string key, long count, double perHour, BaselineBucket bucket)
    {
        var rate = bucket.SampleCount > 0 ? bucket.Mean : 0;
        var metadata = CountFamilyMetadata.Build(count, perHour, rate, bucket);
        metadata["baseline_distinct_days"] = bucket.DistinctDays;
        return new Fact { Source = "anomaly", Key = key, Value = count, Metadata = metadata };
    }

    private static AdviceBlock Compose(Fact fact) =>
        FactAdvice.Compose(fact.Key, new Dictionary<string, Fact> { [fact.Key] = fact })!;

    // ---------------------------------------------------------------- the shared function

    [Fact]
    public void ZeroHistoryBucket_IsStamped_AndRatioAndIsNewAreUnchanged()
    {
        var bucket = ZeroHistory();
        Assert.True(bucket.IsZeroHistory);
        Assert.False(bucket.IsTrustworthy);

        var metadata = CountFamilyMetadata.Build(currentCount: 12, currentPerHour: 3.0, baselineRate: 0, bucket);

        Assert.Equal(1.0, metadata["baseline_zero_history"]);
        Assert.Equal(1.0, metadata["is_new"]);
        Assert.Equal(AnomalyThresholds.NoBaselineRatio, metadata["ratio"]);
        Assert.Equal(12.0, metadata["current_count"]);
        Assert.Equal(0.0, metadata["baseline_rate"]);
        Assert.Equal(30.0, metadata["baseline_samples"]);
    }

    [Fact]
    public void ThinBucket_AndTheEmptyBucket_KeepZeroHistoryAtZero_AndStayFirstOccurrences()
    {
        foreach (var bucket in new[] { Thin(), BaselineBucket.Empty })
        {
            Assert.False(bucket.IsZeroHistory);

            var metadata = CountFamilyMetadata.Build(currentCount: 12, currentPerHour: 3.0, baselineRate: 0, bucket);

            Assert.Equal(0.0, metadata["baseline_zero_history"]);
            Assert.Equal(1.0, metadata["is_new"]);
            Assert.Equal(AnomalyThresholds.NoBaselineRatio, metadata["ratio"]);
        }
    }

    [Fact]
    public void TrustedBucket_CarriesTheRealMultiple_AndNoStamp()
    {
        var bucket = Trusted();
        Assert.True(bucket.IsTrustworthy);

        var metadata = CountFamilyMetadata.Build(currentCount: 24, currentPerHour: 6.0, baselineRate: bucket.Mean, bucket);

        Assert.Equal(0.0, metadata["baseline_zero_history"]);
        Assert.Equal(0.0, metadata["is_new"]);
        Assert.Equal(3.0, metadata["ratio"]);
        Assert.Equal(2.0, metadata["baseline_rate"]);
    }

    [Fact]
    public void TheKeySet_IsTheFourOriginalKeysPlusTheStampAndItsSampleCount()
    {
        var keys = CountFamilyMetadata.Build(12, 3.0, 0, ZeroHistory()).Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.Equal(
            new[] { "baseline_rate", "baseline_samples", "baseline_zero_history", "current_count", "is_new", "ratio" },
            keys);
    }

    // ---------------------------------------------------------------- the advice arm

    [Theory]
    [InlineData("ANOMALY_BLOCKING_SPIKE", 12, "12 blocking events this window — against a month in which this hour saw none", "standard blocking event threshold")]
    [InlineData("ANOMALY_DEADLOCK_SPIKE", 4, "4 deadlocks this window — against a month in which this hour saw none", "standard deadlock threshold")]
    public void ZeroHistoryFact_ReadsAsAMeasuredZero_NotAFirstOccurrence(string key, long count, string headline, string thresholdClause)
    {
        var fact = CountFact(key, count, perHour: count / 4.0, ZeroHistory());

        // The detector stamps a zero-history bucket is_new = 1 as well; the arm must win over it.
        Assert.Equal(1.0, fact.Metadata["is_new"]);

        var block = Compose(fact);

        Assert.Equal(headline, block.Headline);
        Assert.Contains("is not thin — it is a measured ZERO: 30 baseline samples across 30 distinct days, not one of them above zero", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("extremity", block.Investigation, StringComparison.Ordinal);
        Assert.Contains("workload change, a deploy, or a one-off job", block.Investigation, StringComparison.Ordinal);
        Assert.Contains(thresholdClause, block.Remediation, StringComparison.Ordinal);
        Assert.Contains("on a later window", block.Remediation, StringComparison.Ordinal);

        // No multiple, no "first occurrence": neither is true of a baseline that is a measured zero.
        Assert.DoesNotContain("first occurrence", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("no baseline yet", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);
        Assert.DoesNotContain("×", block.Headline + block.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ThinBaselineFact_KeepsTheFirstOccurrenceWording_Verbatim()
    {
        var block = Compose(CountFact("ANOMALY_BLOCKING_SPIKE", 12, perHour: 3.0, Thin()));

        Assert.Equal("12 blocking events this window — first occurrence, no baseline yet", block.Headline);
        Assert.Equal(
            "12 blocking events this window — a first occurrence, with no established baseline for this hour-of-week yet to compare against. " +
            "Treat it as a new event, not a proven regression: check whether it coincides with a workload change, a deploy, or a one-off before treating it as chronic.",
            block.Investigation);
        Assert.Equal(
            "If it recurs or sustains, it will cross the standard blocking event threshold and surface as a first-class finding " +
            "with its chain or graph detail on a later window; treat it then. A one-time event that matches a known cause needs only awareness.",
            block.Remediation);
    }

    [Fact]
    public void TrustedBaselineFact_KeepsTheRatioWording_Verbatim()
    {
        var block = Compose(CountFact("ANOMALY_DEADLOCK_SPIKE", 24, perHour: 6.0, Trusted()));

        Assert.Equal("deadlock rate spiked to 3× its baseline for this time of week", block.Headline);
        Assert.Equal(
            "24 deadlocks this window — about 3× the normal rate for this hour-of-week (baseline 2). " +
            "This is a spike against the time-of-week baseline, not necessarily a sustained problem — check whether it coincides with a workload change or a one-off event.",
            block.Investigation);
        Assert.Equal(
            "If it recurs or sustains, it will cross the standard deadlock threshold and surface as a first-class finding " +
            "with its chain or graph detail on a later window; treat it then. A one-time spike that matches a known event needs only awareness.",
            block.Remediation);
    }

    [Fact]
    public void AFactWithoutTheStamp_ComposesAsBefore()
    {
        // A fact persisted before the stamp existed carries no baseline_zero_history key at all.
        var thin = CountFact("ANOMALY_BLOCKING_SPIKE", 12, perHour: 3.0, Thin());
        var unstampedThin = CountFact("ANOMALY_BLOCKING_SPIKE", 12, perHour: 3.0, Thin());
        unstampedThin.Metadata.Remove("baseline_zero_history");
        unstampedThin.Metadata.Remove("baseline_samples");

        var stamped = Compose(thin);
        var unstamped = Compose(unstampedThin);
        Assert.Equal(stamped.Headline, unstamped.Headline);
        Assert.Equal(stamped.Investigation, unstamped.Investigation);
        Assert.Equal(stamped.Remediation, unstamped.Remediation);

        var trusted = CountFact("ANOMALY_BLOCKING_SPIKE", 24, perHour: 6.0, Trusted());
        var unstampedTrusted = CountFact("ANOMALY_BLOCKING_SPIKE", 24, perHour: 6.0, Trusted());
        unstampedTrusted.Metadata.Remove("baseline_zero_history");
        Assert.Equal(Compose(trusted).Investigation, Compose(unstampedTrusted).Investigation);
    }
}
