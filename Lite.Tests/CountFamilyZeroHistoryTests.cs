using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4731: Lite's blocking and deadlock spikes against a zero-history baseline. Lite's <c>AnomalyDetector</c>
/// builds both facts' metadata through <see cref="CountFamilyMetadata.Build"/>, the same function Darling's
/// detector calls, and <c>FactAdvice.ComposeAnomalyRatio</c> reads the <c>baseline_zero_history</c> stamp before
/// <c>is_new</c>. The arm is pinned on hand-built buckets here. The real detector paths are pinned elsewhere: a
/// measured-zero bucket, which the event baselines return for an hour their collector covered without seeing
/// an event (#4731), in <c>EventBaselineCoveredDaysTests</c>, and a history with no covered hour at all, an
/// empty bucket, in <c>ScenarioTests</c>. The detector-to-function wiring is pinned in Darling's
/// <c>DarlingAnomalyBaselineTests</c>.
/// </summary>
public class CountFamilyZeroHistoryTests
{
    private static BaselineBucket Bucket(double mean, long samples, long days) => new()
    {
        HourOfDay = 14,
        DayOfWeek = 2,
        Mean = mean,
        StdDev = 0,
        Median = 0,
        Mad = 0,
        SampleCount = samples,
        DistinctDays = days,
        Tier = BaselineTier.Full,
    };

    private static Fact CountFact(string key, long count, double perHour, BaselineBucket bucket)
    {
        var rate = bucket.SampleCount > 0 ? bucket.Mean : 0;
        var metadata = CountFamilyMetadata.Build(count, perHour, rate, bucket);
        metadata["baseline_distinct_days"] = bucket.DistinctDays;
        return new Fact { Source = "anomaly", Key = key, Value = count, Metadata = metadata };
    }

    private static AdviceBlock Compose(Fact fact) =>
        FactAdvice.Compose(fact.Key, new Dictionary<string, Fact> { [fact.Key] = fact })!;

    [Theory]
    [InlineData("ANOMALY_BLOCKING_SPIKE", 12, "12 blocking events this window — against a month in which this hour saw none")]
    [InlineData("ANOMALY_DEADLOCK_SPIKE", 4, "4 deadlocks this window — against a month in which this hour saw none")]
    public void ZeroHistoryBucket_IsStamped_KeepsIsNewAndRatio_AndReadsAsAMeasuredZero(string key, long count, string headline)
    {
        var bucket = Bucket(mean: 0, samples: 30, days: 30);
        Assert.True(bucket.IsZeroHistory);

        var fact = CountFact(key, count, perHour: count / 4.0, bucket);

        Assert.Equal(1.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal(AnomalyThresholds.NoBaselineRatio, fact.Metadata["ratio"]);

        var block = Compose(fact);
        Assert.Equal(headline, block.Headline);
        Assert.Contains("a measured ZERO: 30 baseline samples across 30 distinct days, not one of them above zero", block.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first occurrence", block.Headline + block.Investigation + block.Remediation, StringComparison.Ordinal);
    }

    [Fact]
    public void ThinBucket_KeepsZeroHistoryAtZero_AndTheFirstOccurrenceWording()
    {
        var bucket = Bucket(mean: 0, samples: 2, days: 2);
        Assert.False(bucket.IsZeroHistory);

        var fact = CountFact("ANOMALY_BLOCKING_SPIKE", 12, perHour: 3.0, bucket);

        Assert.Equal(0.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(1.0, fact.Metadata["is_new"]);
        Assert.Equal("12 blocking events this window — first occurrence, no baseline yet", Compose(fact).Headline);
    }

    [Fact]
    public void TrustedBucket_KeepsTheRatioWording()
    {
        var bucket = Bucket(mean: 2.0, samples: 30, days: 30);
        Assert.True(bucket.IsTrustworthy);

        var fact = CountFact("ANOMALY_DEADLOCK_SPIKE", 24, perHour: 6.0, bucket);

        Assert.Equal(0.0, fact.Metadata["baseline_zero_history"]);
        Assert.Equal(0.0, fact.Metadata["is_new"]);
        Assert.Equal(3.0, fact.Metadata["ratio"]);
        Assert.Equal("deadlock rate spiked to 3× its baseline for this time of week", Compose(fact).Headline);
    }
}
