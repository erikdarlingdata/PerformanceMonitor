/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5820 slice L5c: the reading-guide heads of the four investigation tools in Lite. Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeads.Investigations.cs</c>. These check what a client can observe: the served head is short
/// and points to the guide, and <c>get_tool_guide</c> hands back the tail the head leaves out. They do not pin the heads'
/// wording.
/// </summary>
public sealed class McpToolGuideHeadsInvestigationsTests
{
    private static readonly string[] InvestigationTools =
    [
        "list_investigation_requests",
        "claim_investigation_request",
        "submit_investigation_report",
        "get_investigation_reports",
    ];

    [Fact]
    public void EveryInvestigationHead_StaysAtOrUnder300Characters_AndPointsToTheGuide()
    {
        foreach (var tool in InvestigationTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 300, $"{tool}: served head {served.Served.Length} is over the 300 target");
        }
    }

    [Fact]
    public void GetToolGuide_ServesEachInvestigationToolsTail()
    {
        var services = new ServiceCollection();
        services.AddMcpServer()
            .WithGeminiCompatibleTools<McpInvestigationTools>()
            .WithGeminiCompatibleTools<McpToolGuideTools>();
        var catalog = (McpToolGuideCatalog)services.Single(d => d.ServiceType == typeof(McpToolGuideCatalog)).ImplementationInstance!;

        using var doc = JsonDocument.Parse(McpToolGuideTools.GetToolGuide(catalog, InvestigationTools, null));
        var tools = doc.RootElement.GetProperty("tools").EnumerateArray().ToList();

        Assert.Equal("ok", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(InvestigationTools.OrderBy(n => n, StringComparer.Ordinal),
            tools.Select(t => t.GetProperty("name").GetString()!).OrderBy(n => n, StringComparer.Ordinal));
        foreach (var tool in tools)
        {
            Assert.Equal(McpToolGuideTests.Served(tool.GetProperty("name").GetString()!).Tail, tool.GetProperty("guide").GetString());
        }
    }
}
