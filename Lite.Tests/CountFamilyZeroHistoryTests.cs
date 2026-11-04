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
/// an event, on a server whose source holds at least one event in the window (#4731), in
/// <c>EventBaselineCoveredDaysTests</c>, and a history with no covered hour at all, or a source that has captured
/// no event, an empty bucket, in that class and in <c>ScenarioTests</c>. The detector-to-function wiring is pinned in Darling's
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
    [InlineData("ANOMALY_BLOCKING_SPIKE", 12, "12 blocked process reports this window — against a month in which this hour saw none")]
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
        Assert.Equal("12 blocked process reports this window — first occurrence, no baseline yet", Compose(fact).Headline);
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

    // #7537: ANOMALY_BLOCKING_SPIKE counts blocked process report rows, BLOCKING_EVENTS counts waits. The spike
    // text names its unit and says it is not the event count, so two figures for one window are not read as one.
    [Fact]
    public void BlockingSpike_NamesItsUnitAsReportRows_AndTheEventCountStaysWaits()
    {
        var spike = Compose(CountFact("ANOMALY_BLOCKING_SPIKE", 291, perHour: 72.75, Bucket(mean: 10, samples: 30, days: 30)));

        Assert.StartsWith("291 blocked process reports this window", spike.Investigation, StringComparison.Ordinal);
        Assert.StartsWith("blocked process report rate spiked", spike.Headline, StringComparison.Ordinal);
        Assert.Contains("counts blocked process report rows", spike.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("291 blocking events", spike.Headline + spike.Investigation, StringComparison.Ordinal);

        var events = new Fact
        {
            Source = "blocking", Key = "BLOCKING_EVENTS", Value = 181,
            Metadata = new Dictionary<string, double> { ["event_count"] = 181 },
        };
        var block = FactAdvice.Compose("BLOCKING_EVENTS", new Dictionary<string, Fact> { ["BLOCKING_EVENTS"] = events })!;

        Assert.StartsWith("181 blocking events this window", block.Headline, StringComparison.Ordinal);
        Assert.Contains("Each is one wait", block.Investigation, StringComparison.Ordinal);
    }

    // #7537: when the detector took its DMV arm the rows are blocking snapshots, not blocked process reports, and the
    // baseline is still in report rows. The text names the right noun and says the units differ; the report arm is unchanged.
    [Theory]
    [InlineData("trusted")]
    [InlineData("zero")]
    [InlineData("thin")]
    public void BlockingSpike_OnTheDmvArm_NamesSnapshotRows_AndSaysTheBaselineIsInReportRows(string shape)
    {
        var bucket = shape switch { "trusted" => Bucket(mean: 10, samples: 30, days: 30), "zero" => Bucket(mean: 0, samples: 30, days: 30), _ => Bucket(mean: 0, samples: 2, days: 2) };
        var fact = CountFact("ANOMALY_BLOCKING_SPIKE", 291, perHour: 72.75, bucket);
        var report = Compose(fact);
        fact.Metadata[CountFamilyMetadata.FromDmvSnapshotsKey] = 1;
        var dmv = Compose(fact);

        Assert.StartsWith("291 blocking snapshot rows this window", dmv.Investigation, StringComparison.Ordinal);
        Assert.Contains("counts blocking snapshot rows", dmv.Investigation, StringComparison.Ordinal);
        Assert.Contains("built from blocked process report rows, not snapshot rows", dmv.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("blocked process reports this window", dmv.Headline + dmv.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("is in report rows too", dmv.Investigation, StringComparison.Ordinal);
        if (shape == "trusted")
            Assert.StartsWith("blocking snapshot row rate spiked", dmv.Headline, StringComparison.Ordinal);
        else
            Assert.StartsWith("291 blocking snapshot rows this window", dmv.Headline, StringComparison.Ordinal);

        // The report arm keeps its wording, and the DMV sentence is gone from it.
        Assert.StartsWith("291 blocked process reports this window", report.Investigation, StringComparison.Ordinal);
        Assert.Contains("The baseline it is compared with is in report rows too.", report.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("snapshot", report.Headline + report.Investigation, StringComparison.Ordinal);
    }
}
