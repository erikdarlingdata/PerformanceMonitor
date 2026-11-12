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
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8320: the statement filter (#4348) sweeps every tool answer, and a value it names is replaced whole by the
/// withheld marker. A tool guide or a note is prose the product wrote, so a guide the judge names is a wording
/// defect in the product, caught here, not a leak caught on a store. The filter itself is never taught to skip a
/// key: a note can quote collected text, and the sweep is the one choke point the design relies on, so the last
/// test holds the other direction too.
/// </summary>
public sealed class McpProseSurvivesStatementFilterTests
{
    /// <summary>Every served tool's guide tail, discovered from the served list and not a hand-kept roster, comes
    /// back from the judge as the same text. A tail that comes back as the marker names the tool, so the wording
    /// can be fixed where it is written.</summary>
    [Fact]
    public void EveryServedGuideTail_ComesBackReadableFromTheStatementFilter()
    {
        var withheld = McpToolsListBudgetTests.Measure().Tools
            .Where(t => t.Tail is not null && !ReferenceEquals(SensitiveStatements.Text(t.Tail), t.Tail))
            .Select(t => t.Name)
            .ToList();
        Assert.True(withheld.Count == 0, "guide tail(s) the statement filter withholds: " + string.Join(", ", withheld));
    }

    /// <summary>The evict_cached_plan guide through the real path: the get_tool_guide answer, wrapped as the tool
    /// result the host sees, swept by the host filter. Nothing in it is named, so the very same result instance
    /// comes back and the guide field is the served tail, not the marker.</summary>
    [Fact]
    public void TheEvictCachedPlanGuide_ReadsThroughTheHostFilter()
    {
        var answer = DarlingMcpToolGuideTools.GetToolGuide(Catalog(), new[] { "evict_cached_plan" }, null);
        var result = new CallToolResult { Content = new List<ContentBlock> { new TextContentBlock { Text = answer } } };

        var swept = SensitiveStatementOutputFilter.Sweep(result);

        var guide = JsonDocument.Parse(((TextContentBlock)swept.Content[0]).Text!).RootElement
            .GetProperty("tools")[0].GetProperty("guide").GetString();
        Assert.NotEqual(SensitiveStatements.PlaceholderText, guide);
        Assert.Equal(McpToolGuideTests.Served("evict_cached_plan").Tail, guide);
        Assert.Same(result, swept);
    }

    /// <summary>The guard is not weakened for the sake of the prose: a credential statement in a collected field, and
    /// one planted where a guide or a note would sit, are each still withheld, while a clean note next to them reads.</summary>
    [Fact]
    public void ACredentialStatementNextToProse_IsStillWithheld()
    {
        const string note = "database_overrides are values one DATABASE or one ROLE was given with a scoped SET.";
        var json = JsonSerializer.Serialize(new
        {
            database_overrides_note = note,
            statement = "ALTER ROLE app_user WITH PASSWORD 'hunter2'",
            tools = new[] { new { name = "x", guide = "CREATE LOGIN etl WITH PASSWORD = N'hunter2'" } },
        });
        var result = new CallToolResult { Content = new List<ContentBlock> { new TextContentBlock { Text = json } } };

        var swept = JsonDocument.Parse(((TextContentBlock)SensitiveStatementOutputFilter.Sweep(result).Content[0]).Text!).RootElement;

        Assert.Equal(note, swept.GetProperty("database_overrides_note").GetString());
        Assert.Equal(SensitiveStatements.PlaceholderText, swept.GetProperty("statement").GetString());
        Assert.Equal(SensitiveStatements.PlaceholderText, swept.GetProperty("tools")[0].GetProperty("guide").GetString());
    }

    private static McpToolGuideCatalog Catalog()
    {
        var services = new ServiceCollection();
        services.AddMcpServer()
            .WithGeminiCompatibleTools<DarlingMcpPlanForceTools>()
            .WithGeminiCompatibleTools<DarlingMcpToolGuideTools>();
        return services.Single(d => d.ServiceType == typeof(McpToolGuideCatalog)).ImplementationInstance as McpToolGuideCatalog
            ?? throw new InvalidOperationException("no catalog registered");
    }
}
