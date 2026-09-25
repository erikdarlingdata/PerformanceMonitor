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
/// #3898 D3 head pins for the <c>get_health_parser_*</c> family (the D1 pilot). This is the pattern every
/// family converted under #3898 copies into its own <c>McpToolGuideHeads.&lt;Family&gt;.cs</c>: the family's
/// tool roster and guardrail facts, plus the two tests that pin them, using <see cref="McpToolGuideTests"/>'s
/// shared <see cref="McpToolGuideTests.Served"/> helper. <c>McpToolGuideTests</c> itself is never edited to add
/// a family, so two families converting in parallel never conflict there.
/// </summary>
public sealed class McpToolGuideHeadsHealthParserTests
{
    private static readonly string[] HealthParserTools =
    [
        "get_health_parser_cpu_tasks",
        "get_health_parser_io_issues",
        "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions",
        "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues",
        "get_health_parser_severe_errors",
        "get_health_parser_significant_waits",
        "get_health_parser_system_health",
    ];

    /// <summary>The per-tool guardrail fact each head must state: the significance gate (a floor under what can
    /// be listed at all) or its absence.</summary>
    private static readonly (string Tool, string Fact)[] GateFacts =
    [
        ("get_health_parser_system_health", "Ungated: every parsed event is returned."),
        ("get_health_parser_memory_node_oom", "Ungated: every recorded OOM is returned."),
        ("get_health_parser_severe_errors", "Gated: severity 19 or higher only, benign connection-reset error numbers excluded"),
        ("get_health_parser_io_issues", "Gated: WARNING-state results only."),
        ("get_health_parser_scheduler_issues", "Gated: WARNING-state results only."),
        ("get_health_parser_memory_conditions", "Gated: only snapshots whose last notification is RESOURCE_MEMPHYSICAL_LOW."),
        ("get_health_parser_cpu_tasks", "Gated: WARNING-state results with at least 10 pending tasks only."),
        ("get_health_parser_memory_broker", "Gated: RESOURCE_MEMPHYSICAL_LOW notifications only."),
        ("get_health_parser_significant_waits", "Floors: a real session, a non-BACKUP statement, at least 500 ms, and a wait type off the idle/background list; shorter waits are never listed."),
    ];

    /// <summary>D9: what an empty answer means, tied to the gate (or the floors, or the absence of one) beside
    /// it rather than one identical sentence on all nine — a fresh reader given only the head misread the
    /// ungated tools' empty answer as ambiguous when it was a real, healthy result (#4048 D9 round).</summary>
    private const string GatedEmptyFact =
        "An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way.";

    private const string FloorsEmptyFact =
        "An empty answer with status empty is a real result: nothing in this window passed the floors, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way.";

    private const string UngatedEmptyFact =
        "An empty answer with status empty is a real result: nothing of this kind was recorded in this window. status unavailable with source_observed false is no evidence either way.";

    private static readonly (string Tool, string Fact)[] EmptyAnswerFacts =
    [
        ("get_health_parser_system_health", UngatedEmptyFact),
        ("get_health_parser_memory_node_oom", UngatedEmptyFact),
        ("get_health_parser_significant_waits", FloorsEmptyFact),
        ("get_health_parser_severe_errors", GatedEmptyFact),
        ("get_health_parser_io_issues", GatedEmptyFact),
        ("get_health_parser_scheduler_issues", GatedEmptyFact),
        ("get_health_parser_memory_conditions", GatedEmptyFact),
        ("get_health_parser_cpu_tasks", GatedEmptyFact),
        ("get_health_parser_memory_broker", GatedEmptyFact),
    ];

    [Fact]
    public void EveryPilotHead_ServesTheWindow_TheEmptyGuardrail_AndItsGate()
    {
        foreach (var tool in HealthParserTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            /* The window is fixed on the event's own time, ends at as_of, newest first. */
            Assert.Contains("over an event_time window ending at as_of, newest first.", served.Served, StringComparison.Ordinal);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            /* D9: the floors sentence is the longest of the three variants; 620 leaves it headroom while
               staying far under the D2 absolute cap of 1,000 (McpToolsListBudgetTests.ConvertedHeadCap). */
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
        }

        foreach (var (tool, fact) in GateFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }

        /* Zero is not a measurement here unless the witness says so (#3541 A12); D9: each tool's empty-answer
           sentence ties to ITS gate (or floors, or absence of one) rather than one sentence on all nine, so a
           reader given only the head does not have to guess which of the four rungs an empty answer landed on. */
        foreach (var (tool, fact) in EmptyAnswerFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }

        /* No required parameter, so the head need not name one: all four are optional in the served schema. */
        Assert.All(HealthParserTools, tool => Assert.All(McpToolGuideTests.Served(tool).ParameterDescriptionLengths, p => Assert.True(p.Length <= 200)));
    }

    /// <summary>Nothing the old descriptions said was lost: the four-rung empty-window sentence every one of
    /// them carried is the topic, and each guide carries the topic verbatim, so one call answers it.</summary>
    [Fact]
    public void EveryPilotGuide_CarriesTheEmptyWindowTopic_AndTheTopicCarriesEveryRung()
    {
        foreach (var tool in HealthParserTools)
        {
            Assert.EndsWith(McpToolGuideTopics.SystemHealthEmptyWindows, McpToolGuideTests.Served(tool).Tail!, StringComparison.Ordinal);
        }

        var topic = McpToolGuideTopics.SystemHealthEmptyWindows;
        foreach (var fact in new[] { "source_observed", "EVER", "last_captured_at", "gated out", "Captured before", "Never recorded",
                     "status unavailable", "not a clean bill", "events_in_window", "last_captured_of_type_at" })
        {
            Assert.Contains(fact, topic, StringComparison.Ordinal);
        }
    }
}
