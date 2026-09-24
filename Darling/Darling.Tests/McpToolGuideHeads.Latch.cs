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
/// #3898 D3 head pins for <c>get_latch_stats</c> (wave E, lane e4). Darling ranks the TOP latch classes by a
/// window total; Lite pages the newest SNAPSHOT. That is a real behavior difference (D6), so the shared head
/// states both in one clause rather than picking one product's framing. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>. Lite's twin is <c>Lite.Tests/McpToolGuideHeadsLatchTests</c>.
/// </summary>
public sealed class McpToolGuideHeadsLatchTests
{
    private static readonly string[] ConvertedTools = ["get_latch_stats"];

    /// <summary>The per-tool guardrail phrases the shared head must state.</summary>
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

    /// <summary>D6: the shared head names neither product's exclusive field (Darling's severity_banded_from
    /// block, Lite's truncated/captured_at paging) — those stay in each product's own tail, unchanged, the way
    /// <c>list_servers</c> keeps peer_fleets and engine_kind off its shared head.</summary>
    [Fact]
    public void SharedHead_KeepsPerProductFieldsOffIt_AndEachTailKeepsItsOwn()
    {
        var served = McpToolGuideTests.Served("get_latch_stats");
        Assert.DoesNotContain("severity_banded_from", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("truncated", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("captured_at", served.Served, StringComparison.Ordinal);
        Assert.Contains("severity_banded_from", served.Tail!, StringComparison.Ordinal);
    }

    /// <summary>Nothing dropped from Darling's original: every sentence the pre-#3898 description carried
    /// (the two-clocks rule, the LOW-severity-beside-a-large-total note, the restart null list) rides verbatim
    /// in the tail get_tool_guide serves. <c>dropcheck.py</c> is the formal check; this pins the sentences most
    /// likely to be paraphrased by a future edit.</summary>
    [Fact]
    public void DarlingTail_KeepsEveryOriginalSentence()
    {
        var tail = McpToolGuideTests.Served("get_latch_stats").Tail!;
        Assert.Contains("Gets top latch contention by class.", tail, StringComparison.Ordinal);
        Assert.Contains("Shows latch waits, wait time, and per-second rates.", tail, StringComparison.Ordinal);
        Assert.Contains("TWO CLOCKS PER ROW, NAMED: total_delta_* SUM every collection in the window", tail, StringComparison.Ordinal);
        Assert.Contains("A LOW severity beside a large window total is a class that was hot earlier in the window and is quiet now, not a contradiction.", tail, StringComparison.Ordinal);
        Assert.Contains("never a quiet 0.00 or a LOW banded from a zero nobody measured; the window totals beside them still stand.", tail, StringComparison.Ordinal);
    }
}
