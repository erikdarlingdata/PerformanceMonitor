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
/// #3898 D3 head pin for <c>get_spinlock_stats</c> (wave E lane e5): a twin with a lane-specific 597-character
/// served-head cap (head plus the 31-character <see cref="McpToolGuide.GuidePointer"/>), tighter than the
/// generic 620 target, so Darling's already-terse original (597 chars, unconverted) does not grow. Darling's
/// tool is a window AGGREGATE (sums every collection over hours_back, top N by total collisions); Lite's is a
/// latest SNAPSHOT (bounded by limit, truncated flags more). D6: since the fact itself differs by product, the
/// head states both in one clause instead of picking either product's shape. Lite's twin is
/// <c>Lite.Tests/McpToolGuideHeadsSpinlockTests</c>; the cross-SKU byte-identical-head check is the generic
/// <see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>.
/// </summary>
public sealed class McpToolGuideHeadsSpinlockTests
{
    private const string Tool = "get_spinlock_stats";

    /// <summary>The lane's own cap: the brief requires the served head not exceed Darling's original
    /// unconverted length (597), well under the generic 620-character target.</summary>
    private const int SpinlockHeadCap = 597;

    [Fact]
    public void SpinlockHead_StaysAtOrUnderTheLaneCap_AndCarriesEveryGuardrailFact()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= SpinlockHeadCap, $"{Tool}: served head {served.Served.Length} is over the {SpinlockHeadCap} lane cap");

        /* D9 guardrail facts: which product does which (aggregate vs snapshot), the shared null-never-0 rule for
           the latest-interval derived numbers, and the status routes when nothing was collected. */
        foreach (var fact in new[]
        {
            "Darling sums every collection in hours_back, top N by total collisions",
            "Lite: LATEST IS A TIME, only the newest snapshot within hours_back",
            "bounded by limit (truncated flags more)",
            "null - never 0 - when unknowable (restart/first sample); totals/counters still stand.",
            "No rows: unavailable (or not_collected first).",
        })
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }

        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{Tool}.{p.Parameter}: {p.Length} > 200"));
    }

    /// <summary>Nothing dropped: Darling's own original sentences ride the tail verbatim (D3 rule 3). The head's
    /// terse facts are new text, not a replacement for the original prose.</summary>
    [Fact]
    public void SpinlockTail_CarriesEveryOriginalDarlingSentence()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        foreach (var sentence in new[]
        {
            "Gets top spinlock contention.",
            "Shows collisions, spins, backoffs, and per-second rates.",
            "total_delta_* SUM every collection in the window",
            "collisions_per_second and spins_per_second are derived from the LATEST interval only",
            "the window totals beside them still stand.",
        })
        {
            Assert.Contains(sentence, tail, StringComparison.Ordinal);
        }
    }
}
