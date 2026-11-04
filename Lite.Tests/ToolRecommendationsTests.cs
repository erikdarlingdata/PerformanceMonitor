/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 (from #3538 A5): <c>analyze_server</c>'s <c>next_tools</c> come from <c>ToolRecommendations</c>, a
/// table keyed on fact key that was never checked against the tools this SKU actually serves — and that had
/// no entry for <c>HADR_SYNC_COMMIT</c> after #3616 taught the scorer to grade it, so the fleet's
/// second-largest wait arrived on the card with nothing to do next.
///
/// <para>Two properties. The specific one: the new entry exists and points at the wait's own trend. The
/// structural one: every tool the table names is a Lite MCP tool, and every parameter it suggests is one that
/// tool's signature takes — a recommendation an agent follows into an "unknown tool" or "unknown parameter"
/// error is worse than no recommendation. Reflected off the Lite assembly's <c>[McpServerTool]</c> attributes,
/// the way the host discovers them, so a tool renamed on the surface fails here rather than on the wire.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ToolRecommendationsTests
{
    private static readonly Dictionary<string, MethodInfo> LiteTools = typeof(McpAnalysisTools).Assembly.GetTypes()
        .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
        .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name is not null)
        .ToDictionary(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name!, m => m, StringComparer.Ordinal);

    private static JsonElement[] RecommendationsFor(string factKey)
    {
        /* #3859: the table takes the engine's TYPED keys now, never a rendered path — OfPath names one chain
           key here, which is exactly what this helper always meant by its single-key argument. */
        var json = JsonSerializer.Serialize(ToolRecommendations.GetForStoryPath(StoryKeys.OfPath(factKey)), PerformanceMonitor.Common.McpHelpers.JsonOptions);
        return JsonDocument.Parse(json).RootElement.EnumerateArray().Select(e => e.Clone()).ToArray();
    }

    [Fact]
    public void HadrSyncCommit_HasAnEntry_ThatLeadsWithItsOwnWaitTrend()
    {
        Assert.Contains("HADR_SYNC_COMMIT", ToolRecommendations.FactKeys);

        var recommendations = RecommendationsFor("HADR_SYNC_COMMIT");
        Assert.True(recommendations.Length >= 2, "an entry with one tool is a pointer, not an investigation path");

        var trend = recommendations[0];
        Assert.Equal("get_wait_trend", trend.GetProperty("tool").GetString());
        Assert.Equal("HADR_SYNC_COMMIT", trend.GetProperty("suggested_params").GetProperty("wait_type").GetString());

        /* Lite has no AG-health read, so the secondary-side evidence is the stored alert rows; the entry must
           say which ones, in AgAlertPolicy's own spelling, or an agent cannot find them in get_alert_history. */
        var alerts = Assert.Single(recommendations, r => r.GetProperty("tool").GetString() == "get_alert_history");
        Assert.Contains(PerformanceMonitor.Common.AgAlertPolicy.SyncFellBehindMetric, alerts.GetProperty("reason").GetString(), StringComparison.Ordinal);
        Assert.Contains(PerformanceMonitor.Common.AgAlertPolicy.ReplicaDisconnectedMetric, alerts.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void EveryRecommendedTool_IsALiteTool_AndEverySuggestedParameter_IsOnItsSignature()
    {
        /* The population floors: a table that reflected to nothing, or an assembly scan that found no tools,
           would make the loop below pass over nothing. 30 tools were referenced at the time of writing. */
        Assert.True(LiteTools.Count >= 60, $"only {LiteTools.Count} Lite MCP tools were discovered by reflection");
        Assert.True(ToolRecommendations.FactKeys.Count >= 30, $"only {ToolRecommendations.FactKeys.Count} fact keys carry recommendations");

        var problems = new List<string>();
        var toolsSeen = new HashSet<string>(StringComparer.Ordinal);

        foreach (var key in ToolRecommendations.FactKeys)
        {
            foreach (var recommendation in RecommendationsFor(key))
            {
                var tool = recommendation.GetProperty("tool").GetString()!;
                toolsSeen.Add(tool);

                if (!LiteTools.TryGetValue(tool, out var method))
                {
                    problems.Add($"{key} → {tool}: no Lite MCP tool of that name");
                    continue;
                }

                if (!recommendation.TryGetProperty("suggested_params", out var suggested))
                {
                    continue;
                }

                var parameters = method.GetParameters().Select(p => p.Name).ToHashSet(StringComparer.Ordinal);
                problems.AddRange(suggested.EnumerateObject()
                    .Where(p => !parameters.Contains(p.Name))
                    .Select(p => $"{key} → {tool}: suggests parameter '{p.Name}', which the tool does not take"));
            }
        }

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
        Assert.True(toolsSeen.Count >= 25, $"only {toolsSeen.Count} distinct tools are recommended; the table has emptied");
    }

    /// <summary>
    /// #7449: every fact key FactAdvice writes a card for, other than the ones the table already carried. Each arrived
    /// on the reply with <c>"next_tools":[]</c>, the memory-pressure card among them. The prefix-routed anomaly keys
    /// are here too, so a routing change that drops one fails.
    /// </summary>
    public static TheoryData<string> AdviceKeysThatWereMissingNextTools => new()
    {
        "ANOMALY_BLOCKING_SPIKE", "ANOMALY_CPU_SPIKE", "ANOMALY_DEADLOCK_SPIKE", "ANOMALY_OBJECT_CONTENTION", "ANOMALY_OBJECT_GROWTH",
        "ANOMALY_READ_LATENCY", "ANOMALY_WAIT_PROFILE", "ANOMALY_WRITE_LATENCY", "BLOCKING_CHAIN", "CONFIG_CTFP",
        "CONFIG_IFI_DISABLED", "CONFIG_LIGHTWEIGHT_POOLING", "CONFIG_LPIM_DISABLED", "CONFIG_MAXDOP", "CONFIG_MAX_MEMORY_MB",
        "CONFIG_MIN_MAX_MEMORY_NARROW", "CONFIG_PRIORITY_BOOST", "LCK_M_RIn_NL", "LCK_M_RIn_S", "LCK_M_RIn_U", "LCK_M_RIn_X",
        "LCK_M_RS_S", "LCK_M_RS_U", "LCK_M_RX_S", "LCK_M_RX_U", "LCK_M_RX_X", "MEMORY_CLERKS", "MEMORY_PRESSURE_EVENTS",
        "MISSING_INDEX", "PAGELATCH_EX", "PAGELATCH_UP", "PLAN_CACHE_BLOAT", "PLAN_WARNING", "RESOURCE_SEMAPHORE_QUERY_COMPILE",
        "RUNNABLE_TASKS", "SERVER_MEMORY_DUMPS", "THREADPOOL_BLOCKING", "THREADPOOL_MIXED", "THREADPOOL_PARALLEL",
    };

    [Theory]
    [MemberData(nameof(AdviceKeysThatWereMissingNextTools))]
    public void EveryKeyTheAdviceCovers_ReachesAtLeastOneNextTool(string factKey)
    {
        Assert.NotEmpty(RecommendationsFor(factKey));
    }

    [Fact]
    public void MemoryPressureEvents_SendsTheReaderToItsOwnRead_ThenMemoryStatsAndClerks()
    {
        var tools = RecommendationsFor("MEMORY_PRESSURE_EVENTS").Select(r => r.GetProperty("tool").GetString()).ToArray();

        Assert.Equal(["get_memory_pressure_events", "get_memory_stats", "get_memory_clerks"], tools);
    }

    [Fact]
    public void MemoryPressureAdvice_StatesTheCountAndBothPeaks_FromTheFactsMetadata()
    {
        var fact = new Fact
        {
            Source = "memory",
            Key = "MEMORY_PRESSURE_EVENTS",
            Value = 3,
            Metadata = new() { ["event_count"] = 12, ["max_process_indicator"] = 3, ["max_system_indicator"] = 1 }
        };

        var advice = FactAdvice.Compose("MEMORY_PRESSURE_EVENTS", new Dictionary<string, Fact> { [fact.Key] = fact });

        Assert.NotNull(advice);
        Assert.Contains("12 pressure notifications", advice!.Investigation, StringComparison.Ordinal);
        Assert.Contains("process indicator peaked at 3 and the system indicator at 1", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("get_memory_pressure_events", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryPressureAdvice_WithoutMetadata_PointsAtTheReadInsteadOfPromisingFiguresTheReplyLacks()
    {
        var fact = new Fact { Source = "memory", Key = "MEMORY_PRESSURE_EVENTS", Value = 3 };

        var advice = FactAdvice.Compose("MEMORY_PRESSURE_EVENTS", new Dictionary<string, Fact> { [fact.Key] = fact });

        Assert.NotNull(advice);
        Assert.Contains("get_memory_pressure_events", advice!.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("The finding carries", advice.Investigation, StringComparison.Ordinal);
    }
}
