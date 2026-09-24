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
/// #3898 D3 head pin for <c>get_long_query_completions</c> (a twin, served byte-identical on both products).
/// Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>. Lite's twin is
/// <c>Lite.Tests/McpToolGuideHeadsLongQueryTests</c>; the cross-SKU lockstep pin itself is the generic one in
/// <see cref="McpToolGuideTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsLongQueryTests
{
    private const string Tool = "get_long_query_completions";

    /// <summary>D9: the page is duration-RANKED, so it is the window's SLOWEST rows, never its newest, and the
    /// two returned-event-time stamps bound how old the slowest runs are, not how far back the read reached.</summary>
    private const string PageIsSlowestFact =
        "THE PAGE IS THE window's limit SLOWEST, NOT ITS NEWEST: completions_returned/truncated say how many " +
        "you got and whether the window held more; oldest/newest_returned_event_time bound the slowest runs' " +
        "ages, NOT how far the read reached.";

    /// <summary>D9: the collector is opt-in and OFF by default, so an empty answer is ambiguous between a
    /// genuinely quiet window and a collector nobody has switched on yet.</summary>
    private const string CollectorGateFact =
        "Collector is opt-in, OFF by default: empty can mean none in the window, or never enabled";

    [Fact]
    public void Head_CarriesThePageRankingTrap_TheCollectorGate_AndThePointer()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.Contains(PageIsSlowestFact, served.Served, StringComparison.Ordinal);
        Assert.Contains(CollectorGateFact, served.Served, StringComparison.Ordinal);
        /* Attentions carry no duration and still sort last under the DESC ranking; a null duration_ms on an
           otherwise-populated row is that, not a read gone wrong. */
        Assert.Contains("attentions (no duration) last", served.Served, StringComparison.Ordinal);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"{Tool}: served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{Tool}.{p.Parameter}: {p.Length} > 200"));
    }

    /// <summary>Nothing dropped: every original sentence rides in the tail, including the two the head compresses
    /// away entirely (the field list, and the cross-SKU population note).</summary>
    [Fact]
    public void Tail_KeepsEveryOriginalSentence_TheHeadCompressedAway()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        Assert.Contains("Shows duration, CPU, reads/writes, row count, result (OK/Error/Abort", tail, StringComparison.Ordinal);
        Assert.Contains("Both SKUs keep the same population.", tail, StringComparison.Ordinal);
        Assert.Contains(
            "The collector is OFF by default; if it returns empty, enable the 'long_query_completions' collector in the schedule.",
            tail, StringComparison.Ordinal);
    }
}
