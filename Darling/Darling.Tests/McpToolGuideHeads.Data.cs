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
/// #3898 D3 head pins for the "Data" family: the core resource/query/discovery tools converted out of
/// <c>DarlingMcpDataTools</c> (Darling) and their Lite twins. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
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

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_file_io_stats", "Reads the newest snapshot, not a window"),
        ("get_perfmon_stats", "Reads the newest snapshot, not a window"),
        ("get_server_properties", "Reads the newest snapshot, not a window"),
        ("get_top_procedures_by_cpu", "LIFETIME extremes, not windowed"),
        ("get_top_queries_by_cpu", "LIFETIME extremes, not windowed"),
        ("get_top_queries_by_cpu", "an empty page under it is the window's real answer, not a miss"),
        ("get_wait_stats", "Bounded by limit"),
        ("list_servers", "Darling has no live connection to monitored servers"),
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

    /// <summary>Nothing dropped: the shared cpu-extremes/attribution topic rides on both top_queries and
    /// top_procedures tails, and the zero-semantics sentence each carries in its own tail.</summary>
    [Fact]
    public void CpuTimeExtremesTopic_RidesOnBothTopByCpuTools()
    {
        Assert.EndsWith(McpToolGuideTopics.CpuTimeExtremesAndAttribution, McpToolGuideTests.Served("get_top_procedures_by_cpu").Tail!, StringComparison.Ordinal);
        Assert.EndsWith(McpToolGuideTopics.CpuTimeExtremesAndAttribution, McpToolGuideTests.Served("get_top_queries_by_cpu").Tail!, StringComparison.Ordinal);
        Assert.Contains("distinct_texts", McpToolGuideTests.Served("get_top_queries_by_cpu").Tail!, StringComparison.Ordinal);
    }

    /// <summary>list_servers is D10's always-loaded entry tool; its Darling-only peer_fleets/engine_kind detail
    /// lives in its own tail, never the shared head (D6: the head must be true on both SKUs).</summary>
    [Fact]
    public void ListServers_KeepsEngineAndPeerDetailOffTheSharedHead()
    {
        var served = McpToolGuideTests.Served("list_servers");
        Assert.DoesNotContain("peer_fleets", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("engine_kind", served.Served, StringComparison.Ordinal);
        Assert.Contains("peer_fleets", served.Tail!, StringComparison.Ordinal);
    }
}
