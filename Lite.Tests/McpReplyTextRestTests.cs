/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>#8572, Lite's side of the three classes the field re-check still found: float noise, ungrouped counts and tracker numbers in reader text.</summary>
public sealed class McpReplyTextRestTests
{
    [Fact]
    public void RoundFloatNoise_RoundsLongFloats_AndLeavesTheRestAlone()
    {
        var reply = McpHelpers.RoundFloatNoise("{\"regression_factor\":9.531238439784213,\"ranked\":[{\"value\":489.9820089195216,\"n\":5,\"ok\":0.123456}]}");
        using var doc = JsonDocument.Parse(reply);
        Assert.Equal("9.53", doc.RootElement.GetProperty("regression_factor").GetRawText());
        Assert.Equal("489.98", doc.RootElement.GetProperty("ranked")[0].GetProperty("value").GetRawText());
        Assert.Equal("5", doc.RootElement.GetProperty("ranked")[0].GetProperty("n").GetRawText());
        Assert.Equal("0.123456", doc.RootElement.GetProperty("ranked")[0].GetProperty("ok").GetRawText());
    }

    [Fact]
    public void AnAgeOfThousandsOfDays_ReadsWithItsSeparator()
    {
        Assert.Equal("2,196 days 6 hours", QueryStoreSnapshotStaleness.Ago(TimeSpan.FromDays(2196) + TimeSpan.FromHours(6)));
        Assert.Equal("587,455", CountText.Grouped(587455));
    }

    [Fact]
    public void TheConfidenceBasis_CarriesNoTrackerNumber()
    {
        foreach (var text in new[] { StoryConfidence.DescribeBasis("CPU_SPIKE", 0.2, 1), StoryConfidence.DescribeBasis("CPU_SPIKE", 0.6, 3), StoryConfidence.DescribeBasis("CPU_SPIKE", 1.0, 1) })
        {
            Assert.Empty(ReplyTextScan.Hits(JsonSerializer.Serialize(new { confidence_basis = text })));
        }
    }

    [Fact]
    public void EveryServedToolDescriptionAndGuide_CarriesNoTrackerNumber()
    {
        var hits = Lite.Tests.McpToolsListBudgetTests.Measure().Tools
            .SelectMany(t => new[] { t.Served, t.Tail ?? string.Empty }.SelectMany(text => ReplyTextScan.Hits(text).Where(h => h.Contains("tracker reference", StringComparison.Ordinal)).Select(h => $"{t.Name}: {h}")))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        Assert.True(hits.Count == 0, "Tool descriptions or guides name a tracker number:\n" + string.Join("\n", hits.Take(60)));
    }
}
