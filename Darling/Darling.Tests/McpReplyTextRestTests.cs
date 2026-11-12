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
using System.Text.Json;
using System.Text.Json.Nodes;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8572, the three classes the field re-check still found: unrounded floats in a reply, a count of four digits or more written without its
/// separators inside a sentence, and a tracker number in reader text. Each test calls the code that builds the text and asserts on what a reader gets.
/// </summary>
public sealed class McpReplyTextRestTests
{
    [Fact]
    public void RoundFloatNoise_RoundsLongFloatsInAnyDepth_AndLeavesTheRestAlone()
    {
        var reply = McpHelpers.RoundFloatNoise(
            "{\"regression_factor\":9.531238439784213,\"drill_down\":[{\"grant_ratio\":1.0269424840141261,\"n\":5,\"ok\":0.123456}],"
            + "\"ranked\":[{\"value\":489.9820089195216}],\"candidate_avg_rows\":0.07894736842105263,\"name\":\"9.531238439784213\",\"none\":null}");
        using var doc = JsonDocument.Parse(reply);
        var root = doc.RootElement;
        Assert.Equal("9.53", root.GetProperty("regression_factor").GetRawText());
        Assert.Equal("1.03", root.GetProperty("drill_down")[0].GetProperty("grant_ratio").GetRawText());
        Assert.Equal("489.98", root.GetProperty("ranked")[0].GetProperty("value").GetRawText());
        Assert.Equal("0.079", root.GetProperty("candidate_avg_rows").GetRawText());
        /* An integer, a figure already at six decimals, a string that looks like a number and a null are not touched. */
        Assert.Equal("5", root.GetProperty("drill_down")[0].GetProperty("n").GetRawText());
        Assert.Equal("0.123456", root.GetProperty("drill_down")[0].GetProperty("ok").GetRawText());
        Assert.Equal("9.531238439784213", root.GetProperty("name").GetString());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("none").ValueKind);
        Assert.Equal("not json", McpHelpers.RoundFloatNoise("not json"));
    }

    [Fact]
    public void RoundFloatNoise_RoundsADoubleBackedNode()
    {
        var node = JsonSerializer.SerializeToNode(new { regression_factor = 9.531238439784213, latest_cpu_per_exec_us = 6086.295424577798 });
        McpHelpers.RoundFloatNoise(node);
        Assert.Equal("{\"regression_factor\":9.53,\"latest_cpu_per_exec_us\":6086.3}", node!.ToJsonString());
    }

    [Fact]
    public void AStoredSweepSignalsDocument_ReadsRounded_WhenItIsEmbeddedInAReply()
    {
        var embedded = FleetSweepPresentation.EmbedJson("{\"window_minutes\":13.287291981666666,\"deadlocks\":2}");
        Assert.Equal("{\"window_minutes\":13.29,\"deadlocks\":2}", embedded!.ToJsonString());
    }

    [Fact]
    public void AnAgeOfThousandsOfDays_ReadsWithItsSeparator()
    {
        Assert.Equal("2,196 days 6 hours", DatabaseProtectionBands.Describe(TimeSpan.FromDays(2196) + TimeSpan.FromHours(6)));
        Assert.Equal("9 days 4 hours", DatabaseProtectionBands.Describe(TimeSpan.FromDays(9) + TimeSpan.FromHours(4)));
        var overdue = new DatabaseProtectionOverdue(DatabaseProtectionBands.KindCheckDb, "Sales", "overdue", TimeSpan.FromDays(2196) + TimeSpan.FromHours(6), TimeSpan.FromDays(14));
        Assert.Equal("Sales (2,196 days 6 hours old)", overdue.Text);
        Assert.Equal("2,196 days 6 hours", QueryStoreSnapshotStaleness.Ago(TimeSpan.FromDays(2196) + TimeSpan.FromHours(6)));
        Assert.Equal("1,436 captures", CountText.Plural(1436, "capture"));
        Assert.Equal("587,455", CountText.Grouped(587455));
    }

    [Fact]
    public void TheConfidenceBasis_CarriesNoTrackerNumber_InAnyOfItsThreeWordings()
    {
        var texts = new[]
        {
            StoryConfidence.DescribeBasis("CPU_SPIKE", 0.2, 1),
            StoryConfidence.DescribeBasis("CPU_SPIKE", 0.6, 3),
            StoryConfidence.DescribeBasis("CPU_SPIKE", 1.0, 1),
        };
        foreach (var text in texts)
        {
            Assert.Empty(ReplyTextScan.Hits(JsonSerializer.Serialize(new { confidence_basis = text })));
        }

        Assert.StartsWith("corroboration:", texts[1], StringComparison.Ordinal);
    }

    [Fact]
    public void EveryServedToolDescriptionAndGuide_CarriesNoTrackerNumber()
    {
        var hits = McpToolsListBudgetTests.Measure().Tools
            .SelectMany(t => new[] { t.Served, t.Tail ?? string.Empty }.SelectMany(text => ReplyTextScan.Hits(text).Where(h => h.Contains("tracker reference", StringComparison.Ordinal)).Select(h => $"{t.Name}: {h}")))
            .Distinct(StringComparer.Ordinal)
            .ToList();
        Assert.True(hits.Count == 0, "Tool descriptions or guides name a tracker number:\n" + string.Join("\n", hits.Take(60)));
    }
}
