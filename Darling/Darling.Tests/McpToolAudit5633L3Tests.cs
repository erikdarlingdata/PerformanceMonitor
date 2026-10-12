/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the Darling performance monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 items 10, 12 and 23, each pinned on the shape that was wrong:
/// audit_config called a Developer Edition server "Enterprise" (it mapped engine edition 3); compare_analysis
/// accepted overlapping windows (it only checked baseline_hours_back &gt; hours_back, and defaulted to 28 whatever
/// hours_back was); get_read_latency reported percentiles (bucket upper bounds) above max_ms.
/// </summary>
public sealed class McpToolAudit5633L3Tests
{
    [Theory]
    [InlineData("Developer Edition (64-bit)", 3)]
    [InlineData("Enterprise Evaluation Edition (64-bit)", 3)]
    [InlineData("Standard Edition (64-bit)", 2)]
    public void AuditEditionLabel_ReportsTheStoredEditionString_NotTheEngineEditionWord(string stored, int engineEdition)
    {
        var (name, note) = DarlingMcpTools.AuditEditionLabel(stored, engineEdition);

        Assert.Equal(stored, name);
        Assert.Null(note);
    }

    [Fact]
    public void AuditEditionLabel_FallsBackToTheEngineLabel_AndSaysSo_WhenTheStoreHoldsNoEditionString()
    {
        var (name, note) = DarlingMcpTools.AuditEditionLabel("  ", 3);

        Assert.Equal("Enterprise", name);
        Assert.NotNull(note);
        Assert.Contains("engine edition", note, StringComparison.Ordinal);
        Assert.Contains("Developer", note, StringComparison.Ordinal);

        var (unknownName, unknownNote) = DarlingMcpTools.AuditEditionLabel(null, 0);
        Assert.Equal("Unknown", unknownName);
        Assert.NotNull(unknownNote);
    }

    [Fact]
    public void AuditConfig_ReadsTheStoredEditionProperty()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        var start = source.IndexOf("Name = \"audit_config\"", StringComparison.Ordinal);
        var body = source[start..source.IndexOf("FormatError(\"audit_config\"", StringComparison.Ordinal)];

        Assert.Contains("GetLatestServerPropertiesAsync(postgres, resolved.ServerId", body, StringComparison.Ordinal);
        Assert.Contains("AuditEditionLabel(propertiesRow?.Edition, edition)", body, StringComparison.Ordinal);
        Assert.Contains("edition_note = editionNote", body, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(4, null, 28)]
    [InlineData(1, null, 25)]
    [InlineData(24, null, 48)]
    [InlineData(48, null, 96)]
    [InlineData(4, 8, 8)]
    [InlineData(4, 100, 100)]
    public void ValidateBaselineWindow_AcceptsDisjointWindows_AndDefaultsFromHoursBack(int hoursBack, int? baseline, int expected)
    {
        Assert.Null(McpHelpers.ValidateBaselineWindow(hoursBack, baseline, out var resolved));
        Assert.Equal(expected, resolved);
    }

    [Theory]
    [InlineData(4, 5)]
    [InlineData(4, 7)]
    [InlineData(24, 30)]
    public void ValidateBaselineWindow_RefusesOverlappingWindows_NamingBoth(int hoursBack, int baseline)
    {
        var refusal = McpHelpers.ValidateBaselineWindow(hoursBack, baseline, out _);

        Assert.NotNull(refusal);
        var message = McpHelpers.ErrorMessageOf(refusal);
        Assert.Contains("overlap", message, StringComparison.Ordinal);
        Assert.Contains($"last {hoursBack} hours", message, StringComparison.Ordinal);
        Assert.Contains($"baseline_hours_back {baseline}", message, StringComparison.Ordinal);
        Assert.Contains($"{2 * hoursBack}", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateBaselineWindow_OmittedBaseline_PastHalfTheCeiling_IsRefusedWithTheRealReason()
    {
        var refusal = McpHelpers.ValidateBaselineWindow(100, null, out _);

        Assert.NotNull(refusal);
        Assert.Contains("no room", McpHelpers.ErrorMessageOf(refusal), StringComparison.Ordinal);
    }

    [Fact]
    public void CompareAnalysis_NoLongerDefaultsBaselineHoursBackTo28()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpTools.cs");
        var start = source.IndexOf("Name = \"compare_analysis\"", StringComparison.Ordinal);
        var body = source[start..source.IndexOf("FormatError(\"compare_analysis\"", StringComparison.Ordinal)];

        Assert.Contains("int? baseline_hours_back = null", body, StringComparison.Ordinal);
        Assert.Contains("McpHelpers.ValidateBaselineWindow(hours_back, baseline_hours_back", body, StringComparison.Ordinal);
        Assert.DoesNotContain("baseline_hours_back = 28", body, StringComparison.Ordinal);
        Assert.DoesNotContain("baseline_hours_back <= hours_back", body, StringComparison.Ordinal);
    }

    [Fact]
    public void FromBuckets_CapsEachPercentileAtMax_SoNoneExceedsTheSlowestReadSeen()
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        var counts = new long[bounds.Length + 1];
        /* The issue's field shape: every sample landed in the bucket past 3,568 ms, the slowest took 3,568. */
        counts[ReadLatencyAccumulator.BucketIndexFor(3_568)] = 100;

        foreach (var p in new[] { 0.50, 0.95, 0.99 })
        {
            var uncapped = ReadLatencyPercentiles.FromBuckets(counts, bounds, p);
            var capped = ReadLatencyPercentiles.FromBuckets(counts, bounds, p, maxMs: 3_568);

            Assert.True(uncapped!.Value.UpperBoundMs > 3_568, "the old shape: the bucket bound is above max");
            Assert.Equal(3_568, capped!.Value.UpperBoundMs);
            Assert.False(capped.Value.IsAtLeast);
        }
    }

    [Fact]
    public void FromBuckets_LeavesABoundAlone_WhenItIsAtOrBelowMax_AndKeepsTheOverflowFloor()
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        var counts = new long[bounds.Length + 1];
        counts[ReadLatencyAccumulator.BucketIndexFor(50)] = 10;

        var low = ReadLatencyPercentiles.FromBuckets(counts, bounds, 0.5, maxMs: 90_000);
        Assert.Equal(bounds[ReadLatencyAccumulator.BucketIndexFor(50)], low!.Value.UpperBoundMs);

        var overflow = new long[bounds.Length + 1];
        overflow[^1] = 5;
        var floor = ReadLatencyPercentiles.FromBuckets(overflow, bounds, 0.5, maxMs: 300_000);
        Assert.Equal(bounds[^1], floor!.Value.UpperBoundMs);
        Assert.True(floor.Value.IsAtLeast);
    }
}
