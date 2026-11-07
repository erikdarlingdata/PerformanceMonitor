/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7536: the shared analysis text Lite shows on its Recommendations tab and serves from its MCP tools holds no em dash, rounds a
/// baseline multiple of 100 or more to a whole number with a thousands separator, and leaves the "not one hot sample" clause out when
/// the window's mean is its peak.
/// </summary>
public class AnalysisTextPunctuationTests
{
    private static AdviceBlock Compose(string key, Dictionary<string, double> metadata)
    {
        var fact = new Fact { Key = key, Metadata = metadata, ServerId = 1 };
        return FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact })!;
    }

    private static Dictionary<string, double> ReadLatency(double peak, double windowMean) => new()
    {
        ["current_latency_ms"] = peak,
        ["avg_latency_ms"] = windowMean,
        ["baseline_mean"] = 12,
        ["deviation_sigma"] = 5.0,
        ["mean_deviation_sigma"] = 5.0,
    };

    [Fact]
    public void TheAnomalyTextAndTheParallelismWaitBlock_ReadWithoutAnEmDash()
    {
        foreach (var block in new[] { Compose("ANOMALY_READ_LATENCY", ReadLatency(peak: 140, windowMean: 90)), FactAdvice.GetForFactKey("CXPACKET")! })
            foreach (var text in new[] { block.Headline, block.Investigation, block.Remediation })
                Assert.DoesNotContain("—", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ABaselineMultipleOf100OrMore_IsAWholeNumberWithAThousandsSeparator()
    {
        var block = Compose("ANOMALY_WAIT_PROFILE", new()
        {
            ["current_ms_per_sec"] = 900,
            ["avg_ms_per_sec"] = 800,
            ["baseline_mean"] = 0.4,
            ["ratio"] = 2464.1,
            ["mean_modified_z"] = 3,
        });
        Assert.Contains("about 2,464× its baseline for this time of week", block.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("2464.1", block.Headline + block.Investigation, StringComparison.Ordinal);

        var small = Compose("ANOMALY_WAIT_PROFILE", new() { ["current_ms_per_sec"] = 9, ["baseline_mean"] = 2, ["ratio"] = 4.5 });
        Assert.Contains("about 4.5× its baseline", small.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void AWindowWhoseMeanIsItsPeak_DoesNotClaimTheWindowRanHigh()
    {
        var oneReading = Compose("ANOMALY_READ_LATENCY", ReadLatency(peak: 81.4, windowMean: 81.4));
        Assert.Contains("the window's mean of 81.4 ms", oneReading.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("not one hot sample", oneReading.Investigation, StringComparison.Ordinal);

        var many = Compose("ANOMALY_READ_LATENCY", ReadLatency(peak: 140, windowMean: 90));
        Assert.Contains("not one hot sample", many.Investigation, StringComparison.Ordinal);
    }
}
