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
/// #3898 D3 head pin for <c>get_spinlock_stats</c>'s Lite twin (wave E lane e5). Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsSpinlockTests</c>, which also carries the lane's 597-character cap
/// rationale and the cross-SKU byte-identical-head check (the generic
/// <c>McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads</c>).
/// </summary>
public sealed class McpToolGuideHeadsSpinlockTests
{
    private const string Tool = "get_spinlock_stats";

    private const int SpinlockHeadCap = 597;

    [Fact]
    public void SpinlockHead_StaysAtOrUnderTheLaneCap_AndCarriesEveryGuardrailFact()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= SpinlockHeadCap, $"{Tool}: served head {served.Served.Length} is over the {SpinlockHeadCap} lane cap");

        foreach (var fact in new[]
        {
            "Darling sums every collection in hours_back, top N by total collisions",
            "Lite returns only the latest snapshot within hours_back",
            "bounded by limit (truncated flags more)",
            "null - never 0 - when unknowable (restart/first sample); totals/counters still stand.",
            "No rows: unavailable (or not_collected first).",
        })
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }

        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{Tool}.{p.Parameter}: {p.Length} > 200"));
    }

    /// <summary>Nothing dropped: Lite's own original sentences ride the tail verbatim (D3 rule 3), including the
    /// LATEST IS A TIME / page-bound catalog phrasing this family established.</summary>
    [Fact]
    public void SpinlockTail_CarriesEveryOriginalLiteSentence()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        foreach (var sentence in new[]
        {
            "Gets the latest spinlock-contention snapshot:",
            "LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of",
            "THE PAGE IS BOUNDED BY limit:",
            "sys.dm_os_spinlock_stats carries well over a hundred",
            "Raise limit when truncated is true.",
            "the same keys Darling's twin derives from its latest interval.",
            "on a row collected before the interval was stored.",
        })
        {
            Assert.Contains(sentence, tail, StringComparison.Ordinal);
        }
    }
}
