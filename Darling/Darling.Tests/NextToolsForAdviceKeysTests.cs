/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7449, Darling's half of Lite's <c>ToolRecommendationsTests</c> additions: <c>analyze_server</c>'s <c>next_tools</c> come
/// from <c>ToolRecommendations.GetForStoryPath</c>, and every root key FactAdvice writes a card for must reach at least one
/// tool, all of them tools this SKU serves. The memory-pressure card had <c>"next_tools":[]</c> and nowhere to send the reader
/// for the figures its advice promised.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class NextToolsForAdviceKeysTests
{
    public static TheoryData<string> AdviceKeysThatWereMissingNextTools => new()
    {
        "ANOMALY_BLOCKING_SPIKE", "ANOMALY_CPU_SPIKE", "ANOMALY_DEADLOCK_SPIKE", "ANOMALY_OBJECT_CONTENTION", "ANOMALY_OBJECT_GROWTH",
        "ANOMALY_READ_LATENCY", "ANOMALY_WAIT_PROFILE", "ANOMALY_WRITE_LATENCY", "BLOCKING_CHAIN", "CONFIG_CTFP",
        "CONFIG_IFI_DISABLED", "CONFIG_LIGHTWEIGHT_POOLING", "CONFIG_LPIM_DISABLED", "CONFIG_MAXDOP", "CONFIG_MAX_MEMORY_MB",
        "CONFIG_MIN_MAX_MEMORY_NARROW", "CONFIG_PRIORITY_BOOST", "LCK_RANGE", "MEMORY_CLERKS", "MEMORY_PRESSURE_EVENTS",
        "MISSING_INDEX", "PAGELATCH_EX", "PAGELATCH_UP", "PLAN_CACHE_BLOAT", "PLAN_WARNING", "RESOURCE_SEMAPHORE_QUERY_COMPILE",
        "RUNNABLE_TASKS", "SERVER_MEMORY_DUMPS", "THREADPOOL_BLOCKING", "THREADPOOL_MIXED", "THREADPOOL_PARALLEL",
    };

    private static string[] ToolsFor(string factKey)
    {
        var json = JsonSerializer.Serialize(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath(factKey)));
        return JsonDocument.Parse(json).RootElement.EnumerateArray().Select(e => e.GetProperty("tool").GetString()!).ToArray();
    }

    private static HashSet<string> RegisteredToolNames() =>
        typeof(ToolRecommendations).Assembly.GetTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name)
            .Where(n => n is not null)
            .Select(n => n!)
            .ToHashSet(StringComparer.Ordinal);

    [Theory]
    [MemberData(nameof(AdviceKeysThatWereMissingNextTools))]
    public void EveryKeyTheAdviceCovers_ReachesAtLeastOneNextTool_ThatThisSkuServes(string factKey)
    {
        var tools = ToolsFor(factKey);

        Assert.NotEmpty(tools);
        var registered = RegisteredToolNames();
        Assert.All(tools, tool => Assert.Contains(tool, registered));
    }

    [Fact]
    public void MemoryPressureEvents_SendsTheReaderToItsOwnRead_ThenMemoryStatsAndClerks()
    {
        Assert.Equal(["get_memory_pressure_events", "get_memory_stats", "get_memory_clerks"], ToolsFor("MEMORY_PRESSURE_EVENTS"));
    }

    [Fact]
    public void ABlockingKey_NamesDarlingsBlockingRead_NotLitesBlockedProcessReports()
    {
        Assert.Contains("get_blocking", ToolsFor("BLOCKING_CHAIN"));
        Assert.DoesNotContain("get_blocked_process_reports", ToolsFor("BLOCKING_CHAIN"));
    }
}
