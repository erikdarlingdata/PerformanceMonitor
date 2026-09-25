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
/// #3898 D3 head pins for the "Platform C" lane: three Darling-only tools with no Lite twin
/// (<c>get_collector_stall_probes</c>, <c>describe_custom_view_catalog</c>, <c>get_sweep_reports</c>), each in
/// its own file under <c>Darling/PerformanceMonitor.Darling.Service/Mcp/</c>. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>. <c>get_sweep_reports</c>' own family test class
/// (<see cref="DarlingMcpFleetSweepToolsTests"/>) carries its two pre-existing description pins, re-pointed to
/// head/tail; this file adds the general D3 head-length/pointer/param checks plus the guardrail facts specific
/// to the other two tools.
/// </summary>
public sealed class McpToolGuideHeadsPlatformCTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_collector_stall_probes",
        "describe_custom_view_catalog",
        "get_sweep_reports",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_collector_stall_probes", "not_collected empty answer is EXPECTED and healthy"),
        ("get_collector_stall_probes", "scheduler_count is the sample's denominator"),
        ("get_collector_stall_probes", "outcome_census is never filtered to successes"),
        ("get_collector_stall_probes", "Unbanded, untrended"),
        ("describe_custom_view_catalog", "the compiler emits ONLY these identifiers"),
        ("describe_custom_view_catalog", "CALL THIS FIRST"),
        ("describe_custom_view_catalog", "a neq filter on procedure name still INCLUDES those rows"),
        ("describe_custom_view_catalog", "Static reference data: no server, time window, or collected-data read"),
        ("get_sweep_reports", "MUTE SEMANTICS"),
        ("get_sweep_reports", "QUIET IS NOT CLEAN"),
        ("get_sweep_reports", "default: open+carried only"),
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>D9: a probe row is server-wide evidence about one moment, not a series with a healthy range — a
    /// reader given only the head must not average several rows into a band the tool never claims to have.</summary>
    [Fact]
    public void StallProbes_HeadNamesTheGate_AndDisclaimsAnyBand()
    {
        var served = McpToolGuideTests.Served("get_collector_stall_probes").Served;
        Assert.Contains("a quarter elapsed and throughput is under 1 MB/s", served, StringComparison.Ordinal);
        Assert.Contains("Unbanded, untrended", served, StringComparison.Ordinal);
        Assert.DoesNotContain("healthy range", served, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>D9: the catalog tool itself reads no monitored server and no collected data — a fresh reader must
    /// not infer it needs a server_name or a time window from the family's other (windowed) tools.</summary>
    [Fact]
    public void CustomViewCatalog_HeadStatesItIsStaticWithNoServerOrWindow()
    {
        var served = McpToolGuideTests.Served("describe_custom_view_catalog").Served;
        Assert.Contains("Static reference data", served, StringComparison.Ordinal);
        Assert.DoesNotContain("server_name", served, StringComparison.Ordinal);
    }

    /// <summary>D9: alerts_enabled false means delivery was off, not that the fleet was quiet — the two are
    /// easy to conflate from an empty would_have_paged-less sweep.</summary>
    [Fact]
    public void SweepReports_HeadDistinguishesMutedFromQuiet()
    {
        var served = McpToolGuideTests.Served("get_sweep_reports").Served;
        Assert.Contains("DELIVERY WAS OFF", served, StringComparison.Ordinal);
        Assert.Contains("instruments_alive false means the sweep could not prove its data sources", served, StringComparison.Ordinal);
    }
}
