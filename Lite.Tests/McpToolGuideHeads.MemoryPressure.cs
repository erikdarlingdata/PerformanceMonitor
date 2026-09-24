/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D3 head pins for get_memory_pressure_events's Lite twin. Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsMemoryPressureTests</c>, which also holds the cross-SKU lockstep pin.
/// </summary>
public sealed class McpToolGuideHeadsMemoryPressureTests
{
    private const string Tool = "get_memory_pressure_events";

    private static readonly string[] HeadFacts =
    [
        "over a sample_time window ending at as_of (default 24h), oldest first.",
        "0-1 normal, 2 medium (Resource Monitor trims caches, cuts grants), 3+ severe (aggressive eviction)",
        "process is this instance, system is the whole box",
        "Empty: none in the window.",
        "not_collected: Azure SQL DB has no ring buffer.",
    ];

    [Fact]
    public void ConvertedHead_CarriesEveryGuardrailFact_AndThePointer()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"{Tool}: served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{Tool}.{p.Parameter}: {p.Length} > 200"));

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D9: this tool's zero-rows path only ever answers <c>not_collected</c> (Azure SQL DB) or
    /// <c>empty</c> — never <c>unavailable</c>, the word its memory-grant sibling <c>get_resource_semaphore</c>
    /// uses on Darling's zero-rows path. A head that borrowed that word here would promise a status this tool
    /// never returns.</summary>
    [Fact]
    public void Head_NeverClaimsUnavailable()
    {
        var served = McpToolGuideTests.Served(Tool).Served;
        Assert.DoesNotContain("unavailable", served, StringComparison.Ordinal);
    }

    /// <summary>Nothing dropped: the shared paragraphs plus Lite's own two extra "check this instead" sentences
    /// still ride the tail, verbatim.</summary>
    [Fact]
    public void Tail_CarriesTheOriginalIndicatorScaleAndLitesOwnCrossReferenceSentences()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        Assert.Contains("Indicator scale (applies to both memory_indicators_process and memory_indicators_system):", tail, StringComparison.Ordinal);
        Assert.Contains("0-1 = normal, no pressure", tail, StringComparison.Ordinal);
        Assert.Contains("2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)", tail, StringComparison.Ordinal);
        Assert.Contains("3+  = severe pressure (aggressive buffer pool / plan cache eviction)", tail, StringComparison.Ordinal);
        Assert.Contains("Not available on Azure SQL DB (ring buffer not exposed).", tail, StringComparison.Ordinal);
        Assert.Contains("Process pressure: check get_memory_grants and get_memory_clerks.", tail, StringComparison.Ordinal);
        Assert.Contains("System pressure with process normal: check get_server_properties (likely another process on the box, not SQL Server).", tail, StringComparison.Ordinal);
    }
}
