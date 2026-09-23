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
/// #3898 D3 head pins for the "Data" family's Lite twins. Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsDataTests</c>, which also holds the cross-SKU lockstep pin.
/// </summary>
public sealed class McpToolGuideHeadsDataTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_file_io_stats",
        "get_perfmon_stats",
        "get_server_properties",
        "get_top_procedures_by_cpu",
        "get_top_queries_by_cpu",
        "get_wait_stats",
        "list_servers",
    ];

    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_file_io_stats", "LATEST IS A TIME: the newest snapshot, not a window"),
        ("get_perfmon_stats", "LATEST IS A TIME: the newest snapshot, not a window"),
        ("get_server_properties", "LATEST IS A TIME: the newest snapshot, not a window"),
        ("get_top_procedures_by_cpu", "LIFETIME extremes, not windowed"),
        ("get_top_queries_by_cpu", "LIFETIME extremes, not windowed"),
        ("get_wait_stats", "Bounded by limit"),
        ("list_servers", "Lite's status IS a live connection check"),
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

    [Fact]
    public void CpuTimeExtremesTopic_RidesOnBothTopByCpuTools()
    {
        Assert.EndsWith(McpToolGuideTopics.CpuTimeExtremesAndAttribution, McpToolGuideTests.Served("get_top_procedures_by_cpu").Tail!, StringComparison.Ordinal);
        Assert.EndsWith(McpToolGuideTopics.CpuTimeExtremesAndAttribution, McpToolGuideTests.Served("get_top_queries_by_cpu").Tail!, StringComparison.Ordinal);
    }

    /// <summary>Lite's list_servers has no fleet/engine concept; its tail is just the original "use this first"
    /// sentence, never the Darling-only peer_fleets/engine_kind prose (that lives only in Darling's own tail).</summary>
    [Fact]
    public void ListServers_TailHasNoDarlingOnlyFleetDetail()
    {
        var served = McpToolGuideTests.Served("list_servers");
        Assert.DoesNotContain("peer_fleets", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("peer_fleets", served.Tail!, StringComparison.Ordinal);
    }
}
