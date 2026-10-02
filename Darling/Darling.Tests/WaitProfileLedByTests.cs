using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The wait-profile finding names the waits that led it. On an Azure SQL Database whose baseline is still young the
/// firing bar leaves REMOTE_BLOCK_IO out, so the "led by" list ranks the waits that counted first and labels the
/// excluded one. A fact without the exclusion marker reads exactly as before.
/// </summary>
public sealed class WaitProfileLedByTests
{
    private static string Story(bool excluded, double markValue = 1)
    {
        var metadata = new Dictionary<string, double>
        {
            ["current_ms_per_sec"] = 1300,
            ["avg_ms_per_sec"] = 1250,
            ["baseline_mean"] = 0,
            ["ratio"] = 100,
            ["is_new"] = 1,
            ["contrib_REMOTE_BLOCK_IO"] = 900_000,
            ["contrib_LCK_M_S_XACT_MODIFY"] = 270_000,
            ["contrib_PAGEIOLATCH_SH"] = 90_000
        };
        if (excluded) metadata["bar_excluded_REMOTE_BLOCK_IO"] = markValue;
        var fact = new Fact { Source = "anomaly", Key = "ANOMALY_WAIT_PROFILE", Value = 1_260_000, Metadata = metadata };
        return FactAdvice.Compose("ANOMALY_WAIT_PROFILE", new Dictionary<string, Fact> { ["ANOMALY_WAIT_PROFILE"] = fact })!.Investigation;
    }

    [Fact]
    public void ExcludedWait_IsRankedAfterTheCountedWaits_AndLabelled()
    {
        var story = Story(excluded: true);
        Assert.Contains("led by LCK_M_S_XACT_MODIFY, PAGEIOLATCH_SH, REMOTE_BLOCK_IO (not counted toward the threshold on Azure SQL Database)", story, StringComparison.Ordinal);
        Assert.Contains("peaked at about 1300 ms/sec", story, StringComparison.Ordinal);
    }

    [Fact]
    public void MarkerWithValueZero_IsNotLabelled()
    {
        var story = Story(excluded: true, markValue: 0);
        Assert.Contains("led by REMOTE_BLOCK_IO, LCK_M_S_XACT_MODIFY, PAGEIOLATCH_SH.", story, StringComparison.Ordinal);
        Assert.DoesNotContain("not counted", story, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData(0.0, false)]
    [InlineData(1.0, true)]
    public void IsBarExcluded_TrueOnlyForTheValueOne(double? value, bool expected)
    {
        var m = new Dictionary<string, double>();
        if (value is double v) m["bar_excluded_REMOTE_BLOCK_IO"] = v;
        Assert.Equal(expected, PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.IsBarExcluded(m, "REMOTE_BLOCK_IO"));
        Assert.False(PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.IsBarExcluded(null, "REMOTE_BLOCK_IO"));
    }

    [Fact]
    public void WithoutTheMarker_TheListIsRankedByValueAsBefore()
    {
        var story = Story(excluded: false);
        Assert.Contains("led by REMOTE_BLOCK_IO, LCK_M_S_XACT_MODIFY, PAGEIOLATCH_SH.", story, StringComparison.Ordinal);
        Assert.DoesNotContain("not counted", story, StringComparison.Ordinal);
    }
}
