/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICEDarling.TestsE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5640: <c>get_client_timeouts</c> keeps its guardrail facts in the served head and its reading rules in the tail
/// (<c>get_tool_guide</c>), because the head is held to the tools/list byte budget. The tail says the count is exact and the
/// samples are a ring-buffer subset, that the rate covers only the minutes the collector covered, what each status word means
/// here, and that a high rate is a client-side symptom whose top statements and waits are the next look. Twin:
/// <c>Lite.Tests/McpToolGuideHeads.ClientTimeouts.cs</c>.
/// </summary>
public sealed class McpToolGuideHeadsClientTimeoutsTests
{
    [Fact]
    public void ClientTimeoutsHead_StaysUnder620Characters_PointsToTheGuide_AndKeepsTheGuardrailFacts()
    {
        var served = McpToolGuideTests.Served("get_client_timeouts");
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head {served.Served.Length} is over the 620 target");

        foreach (var fact in new[]
        {
            "Samples are a subset, never the total", "empty = collector ran, none counted",
            "unavailable = no recent run", "not_collected = engine without the collector", "precondition = capture session missing",
        })
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ClientTimeoutsGuideTail_ExplainsTheExactCount_TheSamples_TheRate_TheStatusWords_AndWhatToLookAtNext()
    {
        var served = McpToolGuideTests.Served("get_client_timeouts");
        foreach (var phrase in new[]
        {
            "count is exact", "event_counter", "ring buffer", "1,000 events", "a subset",
            "minutes_covered", "do not dilute the rate",
            "empty means", "unavailable means", "not_collected means", "precondition means",
            "an absent row is not a zero",
            "client cancel or timeout", "client-side symptom", "top_statements and their waits",
        })
        {
            Assert.Contains(phrase, served.Tail!, StringComparison.Ordinal);
        }

        /* The long-form reading stays off the served head. */
        foreach (var phrase in new[] { "event_counter", "minutes_covered", "client-side symptom" })
        {
            Assert.DoesNotContain(phrase, served.Served, StringComparison.Ordinal);
        }
    }
}
