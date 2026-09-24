/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for get_memory_pressure_events (a Darling/Lite twin; #4103 converted its
/// get_resource_semaphore/get_memory_grants siblings in the same source file separately and is left alone here).
/// Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsMemoryPressureTests
{
    private const string Tool = "get_memory_pressure_events";

    /// <summary>The per-tool guardrail phrases the head must state: the indicator scale (shared by both
    /// indicator columns), which column reads which thing, the window/order, and the two miss words with their
    /// own scope (an all-clear vs. an engine that never collects this).</summary>
    private static readonly string[] HeadFacts =
    [
        "over a sample_time window ending at as_of (default 24h), oldest first.",
        "0-1 normal, 2 medium (Resource Monitor trims caches, cuts grants), 3+ severe (aggressive eviction)",
        "process is this instance, system is the whole box",
        "Empty: none in the window.",
        "not_collected: Azure SQL DB (no ring buffer) or a non-SQL Server target.",
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

    /// <summary>D9: this tool's zero-rows path only ever answers <c>not_collected</c> (Azure SQL DB, or a non-SQL Server target on Darling) or
    /// <c>empty</c> — never <c>unavailable</c>, the word its memory-grant siblings in the same source file
    /// (<c>get_resource_semaphore</c>, <c>get_memory_grants</c>) use on their own zero-rows path. A head that
    /// borrowed that word here would promise a status this tool never returns.</summary>
    [Fact]
    public void Head_NeverClaimsUnavailable_UnlikeItsMemoryGrantSiblings()
    {
        var served = McpToolGuideTests.Served(Tool).Served;
        Assert.DoesNotContain("unavailable", served, StringComparison.Ordinal);
    }

    /// <summary>Nothing dropped: every sentence of the original description (both products' shared paragraphs,
    /// plus Lite's own two extra sentences) still rides the tail, verbatim.</summary>
    [Fact]
    public void Tail_CarriesTheOriginalIndicatorScaleAndAzureSqlDbSentence()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        Assert.Contains("Indicator scale (applies to both memory_indicators_process and memory_indicators_system):", tail, StringComparison.Ordinal);
        Assert.Contains("0-1 = normal, no pressure", tail, StringComparison.Ordinal);
        Assert.Contains("2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)", tail, StringComparison.Ordinal);
        Assert.Contains("3+  = severe pressure (aggressive buffer pool / plan cache eviction)", tail, StringComparison.Ordinal);
        Assert.Contains("Not available on Azure SQL DB (ring buffer not exposed).", tail, StringComparison.Ordinal);
    }
}
