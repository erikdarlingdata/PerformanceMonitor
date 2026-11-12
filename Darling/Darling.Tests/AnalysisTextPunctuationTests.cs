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
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7536: the Analysis Findings and Recommendations text a person reads on the Overview and Recommendations tabs, and that the MCP
/// tools serve, holds no em dash; a baseline multiple of 100 or more is a whole number with a thousands separator; the "not one hot
/// sample" clause is left out when the window's mean is its peak; and the Configuration Audit's shared_buffers row states the value once.
/// </summary>
public sealed class AnalysisTextPunctuationTests
{
    private static AdviceBlock Compose(string key, Dictionary<string, double> metadata, string source = "")
    {
        var fact = new Fact { Source = source, Key = key, Metadata = metadata, ServerId = 1 };
        return FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact })!;
    }

    private static Fact PgConfig(string key, double value) =>
        new() { Source = PgTargetSources.ConfigSource, Key = key, Value = value, ServerId = 1 };

    private static IReadOnlyDictionary<string, Fact> Lookup(params Fact[] facts)
    {
        var d = new Dictionary<string, Fact>(StringComparer.Ordinal);
        foreach (var f in facts) d[f.Key] = f;
        return d;
    }

    private static Fact PgDeadlocks(double ratio)
    {
        var fact = new Fact { Key = PgTargetFactKeys.AnomalyDeadlockRate, Value = 6, ServerId = 1 };
        fact.Metadata["current_count"] = 24;
        fact.Metadata["current_rate_per_hour"] = 6;
        fact.Metadata["observed_hours"] = 4;
        fact.Metadata["ratio"] = ratio;
        fact.Metadata["baseline_rate"] = 0.005;
        return fact;
    }

    private static Fact PgWaitProfile(double current, double windowMean)
    {
        var fact = new Fact { Key = PgTargetFactKeys.AnomalyWaitProfile, Value = current, ServerId = 1 };
        fact.Metadata["current_ms_per_sec"] = current;
        fact.Metadata["baseline_mean"] = 200;
        fact.Metadata["modified_z"] = 0;
        fact.Metadata["ratio"] = current / 200;
        fact.Metadata["mean_ms_per_sec"] = windowMean;
        fact.Metadata["mean_ratio"] = windowMean / 200;
        return fact;
    }

    private static Dictionary<string, double> ReadLatency(double peak, double windowMean) => new()
    {
        ["current_latency_ms"] = peak,
        ["avg_latency_ms"] = windowMean,
        ["baseline_mean"] = 12,
        ["deviation_sigma"] = 5.0,
        ["mean_deviation_sigma"] = 5.0,
    };

    private static void AssertNoEmDash(AdviceBlock? block)
    {
        Assert.NotNull(block);
        foreach (var text in new[] { block.Headline, block.Investigation, block.Remediation })
            Assert.DoesNotContain("—", text, StringComparison.Ordinal);
    }

    [Fact]
    public void TheIssuesExamples_ReadWithoutAnEmDash()
    {
        /* The SQL Server anomaly text (its hedge and its "one-time event" aside), the parallelism wait's static block, the PostgreSQL
           shared_buffers knob at its default, and the PostgreSQL deadlock-rate anomaly with a baseline multiple. */
        AssertNoEmDash(Compose("ANOMALY_READ_LATENCY", ReadLatency(peak: 140, windowMean: 90)));
        AssertNoEmDash(FactAdvice.GetForFactKey("CXPACKET"));
        AssertNoEmDash(PgTargetAdvice.Compose(PgTargetFactKeys.ConfigSharedBuffers, Lookup(PgConfig(PgTargetFactKeys.ConfigSharedBuffers, 128))));
        AssertNoEmDash(PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyDeadlockRate, Lookup(PgDeadlocks(4.0))));
    }

    [Fact]
    public void ABaselineMultipleOf100OrMore_IsAWholeNumberWithAThousandsSeparator()
    {
        var sql = Compose("ANOMALY_WAIT_PROFILE", new()
        {
            ["current_ms_per_sec"] = 900,
            ["avg_ms_per_sec"] = 800,
            ["baseline_mean"] = 0.4,
            ["ratio"] = 2464.1,
            ["mean_modified_z"] = 3,
        });
        Assert.Contains("about 2,464× its baseline for this time of week", sql.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("2464.1", sql.Headline + sql.Investigation, StringComparison.Ordinal);

        var pgBlock = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyDeadlockRate, Lookup(PgDeadlocks(1203.7)))!;
        Assert.Contains("about 1,204× its baseline", pgBlock.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("1203.7", pgBlock.Headline + pgBlock.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void ABaselineMultipleUnder100_KeepsItsTenth()
    {
        var sql = Compose("ANOMALY_WAIT_PROFILE", new() { ["current_ms_per_sec"] = 9, ["baseline_mean"] = 2, ["ratio"] = 4.5 });
        Assert.Contains("about 4.5× its baseline", sql.Headline, StringComparison.Ordinal);
    }

    [Fact]
    public void AWindowWhoseMeanIsItsPeak_DoesNotClaimTheWindowRanHigh()
    {
        /* One reading in the window: the mean is the peak, so there is no window-versus-sample contrast to state. */
        var oneReading = Compose("ANOMALY_READ_LATENCY", ReadLatency(peak: 81.4, windowMean: 81.4));
        Assert.Contains("the window's mean of 81.4 ms", oneReading.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("not one hot sample", oneReading.Investigation, StringComparison.Ordinal);

        var many = Compose("ANOMALY_READ_LATENCY", ReadLatency(peak: 140, windowMean: 90));
        Assert.Contains("the window's mean of 90 ms", many.Investigation, StringComparison.Ordinal);
        Assert.Contains("not one hot sample", many.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void APostgresWindowWhoseMeanIsItsPeak_DoesNotClaimTheWholeWindowSatHigh()
    {
        var oneDelta = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(PgWaitProfile(current: 1500, windowMean: 1500)))!;
        Assert.Contains("The window's mean rate was 1500 ms/sec", oneDelta.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("not one delta", oneDelta.Investigation, StringComparison.Ordinal);

        var many = PgTargetAdvice.Compose(PgTargetFactKeys.AnomalyWaitProfile, Lookup(PgWaitProfile(current: 3200, windowMean: 1500)))!;
        Assert.Contains("not one delta", many.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSharedBuffersAuditRow_StatesTheValueOnce()
    {
        var fact = PgConfig(PgTargetFactKeys.ConfigSharedBuffers, 128);
        fact.BaseSeverity = PgTargetScorer.ScoreBase(fact);
        fact.Severity = fact.BaseSeverity;
        Assert.True(fact.Severity > 0, "the default shared_buffers is a convention finding, so the row joins the headline to the remedy");

        var advice = FactAdvice.Compose(fact.Key, Lookup(fact));
        var row = PgConfigAuditText.Compose(fact, "shared_buffers", "128 MB", advice, notApplicable: false, isAuroraCheckpointTimeout: false);

        Assert.StartsWith("shared_buffers is 128 MB", row, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(row, "shared_buffers is 128 MB"));
        Assert.Contains("quarter of the host's RAM", row, StringComparison.Ordinal);
        Assert.DoesNotContain("—", row, StringComparison.Ordinal);
    }
}
