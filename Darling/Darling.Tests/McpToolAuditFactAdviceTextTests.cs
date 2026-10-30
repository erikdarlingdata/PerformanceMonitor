/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 items 2, 5 and 8: the wording of three FactAdvice composers, pinned through the public
/// <see cref="FactAdvice.Compose"/> entry point. Each assertion fails on the old text.
/// </summary>
public sealed class McpToolAuditFactAdviceTextTests
{
    private static AdviceBlock Compose(string key, Dictionary<string, double> metadata)
    {
        var facts = new Dictionary<string, Fact>
        {
            [key] = new Fact { Key = key, Severity = 1, Metadata = metadata },
        };
        var advice = FactAdvice.Compose(key, facts);
        Assert.NotNull(advice);
        return advice!;
    }

    // ---- item 2: total_spills counts 8 KB pages, not spill events ----

    [Fact]
    public void QuerySpills_NamesPagesAndMegabytes_NotSpills()
    {
        // 12,800 pages x 8 KB = 100 MB.
        var advice = Compose("QUERY_SPILLS", new() { ["total_spills"] = 12_800, ["spilling_query_count"] = 3 });

        Assert.Contains("12,800 pages (100 MB) spilled to tempdb", advice.Headline);
        Assert.Contains("12,800 pages (100 MB) spilled to tempdb this window", advice.Investigation);
        Assert.DoesNotContain("12,800 spills", advice.Headline + advice.Investigation);
        Assert.DoesNotContain("query spills", advice.Headline);
    }

    [Fact]
    public void QuerySpills_SmallValue_KeepsFractionalMegabytes_AndSingularPage()
    {
        Assert.Equal("1 page (0.01 MB)", FactAdvice.SpilledPagesText(1));
        Assert.Equal("128 pages (1.0 MB)", FactAdvice.SpilledPagesText(128));
    }

    [Fact]
    public void QuerySpills_StaticBlock_RanksByPages_NotSpillCount()
    {
        var block = FactAdvice.Compose("QUERY_SPILLS", new Dictionary<string, Fact>());

        Assert.NotNull(block);
        Assert.DoesNotContain("ranked by spill count", block!.Investigation);
        Assert.Contains("ranked by pages spilled", block.Investigation);
    }

    // ---- item 5 ----

    [Fact]
    public void PlanWarning_SaysCrossJoins_NotJoops()
    {
        var advice = Compose("PLAN_WARNING", new() { ["warning_count"] = 4, ["critical_count"] = 1 });

        Assert.Contains("accidental cross joins", advice.Investigation);
        Assert.DoesNotContain("joops", advice.Investigation);
    }

    // ---- item 8: the sigma is measured from the median in the robust frame, and 25 is a cap ----

    private static Dictionary<string, double> CpuMetadata(double sigma, double meanSigma, double median, double mad) => new()
    {
        ["peak_cpu"] = 90,
        ["avg_cpu_in_window"] = 70,
        ["baseline_mean"] = 40,
        ["baseline_median"] = median,
        ["baseline_mad"] = mad,
        ["deviation_sigma"] = sigma,
        ["mean_deviation_sigma"] = meanSigma,
    };

    [Fact]
    public void Anomaly_RobustFrame_NamesBaselineMedian_NotMean()
    {
        var advice = Compose("ANOMALY_CPU_SPIKE", CpuMetadata(sigma: 6.1, meanSigma: 3.2, median: 20, mad: 4));

        Assert.Contains("6.1 times its usual variation above its 20% normal level for this hour-of-week", advice.Investigation);
        Assert.DoesNotContain("40%", advice.Investigation);
        Assert.Contains("3.2 times its usual variation above it", advice.Investigation);
        Assert.Contains("6.1 times its usual variation above its normal level for this time of week", advice.Headline);
    }

    [Fact]
    public void Anomaly_ClassicalFrame_StillNamesMean()
    {
        var advice = Compose("ANOMALY_CPU_SPIKE", CpuMetadata(sigma: 6.1, meanSigma: 3.2, median: 0, mad: 0));

        Assert.Contains("6.1 times its usual variation above its 40% normal level for this hour-of-week", advice.Investigation);
        Assert.DoesNotContain("median", advice.Investigation);
    }

    [Fact]
    public void Anomaly_SigmaAtDisplayCap_SaysAtLeast_UsingTheConstant()
    {
        var cap = AnomalyThresholds.SigmaDisplayCap;
        var advice = Compose("ANOMALY_CPU_SPIKE", CpuMetadata(sigma: cap, meanSigma: cap, median: 20, mad: 4));

        Assert.Contains("at least 25 times its usual variation above its 20% normal level", advice.Investigation);
        Assert.Contains("sat at least 25 times its usual variation above it", advice.Investigation);
        Assert.Contains("at least 25 times its usual variation above its normal level for this time of week", advice.Headline);
        Assert.DoesNotContain("σ", advice.Headline);
    }

    // #6925: file I/O latency advice had to read in plain words, with no Greek letter and no "robust".
    [Fact]
    public void ReadLatencyAnomaly_AtDisplayCap_ReadsInPlainWords_WithNoSigma()
    {
        var advice = Compose("ANOMALY_READ_LATENCY", new()
        {
            ["current_latency_ms"] = 140,
            ["avg_latency_ms"] = 90,
            ["baseline_mean"] = 12,
            ["baseline_median"] = 8,
            ["baseline_mad"] = 2,
            ["deviation_sigma"] = AnomalyThresholds.SigmaDisplayCap,
            ["mean_deviation_sigma"] = 6.1,
        });

        Assert.Contains("at least 25 times its usual variation above its 8 ms normal level for this hour-of-week", advice.Investigation);
        Assert.Contains("sat 6.1 times its usual variation above it", advice.Investigation);
        Assert.Contains("at least 25 times its usual variation above its normal level for this time of week", advice.Headline);
        foreach (var text in new[] { advice.Headline, advice.Investigation, advice.Remediation })
        {
            Assert.DoesNotContain("σ", text);
            Assert.DoesNotContain("robust", text, System.StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("sigma", text, System.StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void WaitProfile_MeanAtDisplayCap_SaysAtLeast()
    {
        var advice = Compose("ANOMALY_WAIT_PROFILE", new()
        {
            ["current_ms_per_sec"] = 900,
            ["avg_ms_per_sec"] = 800,
            ["baseline_mean"] = 100,
            ["ratio"] = 9,
            ["mean_modified_z"] = AnomalyThresholds.SigmaDisplayCap,
        });

        Assert.Contains("sat at least 25 times its usual variation above its normal level", advice.Investigation);
    }
}
