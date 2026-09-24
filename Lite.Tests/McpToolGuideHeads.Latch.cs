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
/// #3898 D3 head pins for <c>get_latch_stats</c>'s Lite twin (wave E, lane e4). Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsLatchTests</c>, which also holds the cross-SKU lockstep pin.
/// </summary>
public sealed class McpToolGuideHeadsLatchTests
{
    private static readonly string[] HeadFacts =
    [
        "Darling sums waits over the whole hours_back window and ranks by that total",
        "Lite returns only the newest snapshot in hours_back, paged to limit, heaviest last-interval wait first",
        "High LATCH_EX on ACCESS_METHODS_DATASET_PARENT or FGCB_ADD_REMOVE means TempDB allocation contention",
        "interval_seconds, both per-second rates and (on Darling) severity come from the LATEST interval only",
        "they are null, never 0 or LOW",
        "Zero rows: not_collected off SQL Server engines, else unavailable, never empty",
    ];

    [Fact]
    public void GetLatchStatsHead_CarriesEveryGuardrailFact_AndThePointer()
    {
        var served = McpToolGuideTests.Served("get_latch_stats");
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"get_latch_stats: served head {served.Served.Length} is over the 620 target");
        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"get_latch_stats.{p.Parameter}: {p.Length} > 200"));

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D6: the shared head names neither product's exclusive field. Lite's own paging fields
    /// (truncated, captured_at) stay off the shared head and live only in Lite's tail; Darling's severity
    /// block never appears here at all.</summary>
    [Fact]
    public void SharedHead_KeepsPerProductFieldsOffIt_AndLiteTailKeepsItsOwn()
    {
        var served = McpToolGuideTests.Served("get_latch_stats");
        Assert.DoesNotContain("truncated", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("captured_at", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("severity_banded_from", served.Served, StringComparison.Ordinal);
        Assert.Contains("truncated", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("captured_at", served.Tail!, StringComparison.Ordinal);
        Assert.DoesNotContain("severity_banded_from", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>Nothing dropped from Lite's original: the LATEST-IS-A-TIME sentence, the page-bounded-by-limit
    /// rule and the restart null list all ride verbatim in the tail get_tool_guide serves.
    /// <c>dropcheck.py</c> is the formal check; this pins the sentences most likely to be paraphrased by a
    /// future edit.</summary>
    [Fact]
    public void LiteTail_KeepsEveryOriginalSentence()
    {
        var tail = McpToolGuideTests.Served("get_latch_stats").Tail!;
        Assert.Contains("Gets the latest latch-contention snapshot by latch class", tail, StringComparison.Ordinal);
        Assert.Contains("LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours", tail, StringComparison.Ordinal);
        Assert.Contains("THE PAGE IS BOUNDED BY limit: latches_returned is how many latch classes you got, heaviest last-interval wait first", tail, StringComparison.Ordinal);
        Assert.Contains("null, never 0, so a restart cannot read as a quiet latch; interval_seconds is also null, with the deltas standing, on a row collected before the interval was stored.", tail, StringComparison.Ordinal);
    }
}
