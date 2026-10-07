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
/// Head pins for the FinOps Server Inventory: <c>get_finops_inventory</c>. Follows the pattern in
/// <see cref="McpToolGuideHeadsPvsTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsFinOpsInventoryTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_finops_inventory",
    ];

    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_finops_inventory", "Fixed windows"),
        ("get_finops_inventory", "Views: server_inventory."),
        ("get_finops_inventory", "An unknown view is refused with the valid list."),
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
    public void ReadingGuidance_NotRepeatedInHead_StaysInTail()
    {
        var served = McpToolGuideTests.Served("get_finops_inventory");
        Assert.DoesNotContain("health_band", served.Served, StringComparison.Ordinal);
        Assert.Contains("health_band is good at 80 and above, fair at 60 and above, else poor", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("24-hour average CPU", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Servers without a collected properties snapshot are not listed", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("annual_cost_usd is monthly", served.Tail!, StringComparison.Ordinal);
    }
}
