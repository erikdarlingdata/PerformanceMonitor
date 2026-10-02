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
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D10: exactly the four entry tools — <c>list_servers</c>, <c>get_fleet_overview</c>,
/// <c>analyze_server</c> and <c>get_tool_guide</c> — carry <c>_meta["anthropic/alwaysLoad"] = true</c>
/// in <c>tools/list</c>, so Claude Code keeps them loaded when it defers the rest (Claude Code MCP
/// docs, "Exempt a server from deferral": "An MCP server can also mark individual tools as
/// always-loaded by including <c>"anthropic/alwaysLoad": true</c> in the tool's <c>_meta</c> object").
/// Built through the same <c>WithGeminiCompatibleTools</c> path the host registers every tool with —
/// no running server required. Lite twin: <see cref="Lite.Tests.McpAlwaysLoadedToolsTests"/> (Lite has
/// no <c>get_fleet_overview</c>; central-store-only, Darling only).
/// </summary>
public class McpAlwaysLoadedToolsTests
{
    private static readonly string[] ExpectedAlwaysLoaded =
    {
        "analyze_server",
        "get_fleet_overview",
        "get_tool_guide",
        "list_servers"
    };

    private static List<ModelContextProtocol.Protocol.Tool> BuildProtocolTools()
    {
        var toolTypes = typeof(DarlingMcpHostService).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        var services = new ServiceCollection();
        var serviceParamTypes = toolTypes
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(t => McpServedSchema.IsServiceParameter(t) && t != typeof(McpToolGuideCatalog))
            .Distinct();

        foreach (var serviceType in serviceParamTypes)
        {
            services.AddSingleton(serviceType, _ => null!);
        }

        var builder = services.AddMcpServer();
        var register = typeof(McpSchemaCompat).GetMethod(nameof(McpSchemaCompat.WithGeminiCompatibleTools), BindingFlags.Public | BindingFlags.Static)!;
        foreach (var toolType in toolTypes)
        {
            register.MakeGenericMethod(toolType).Invoke(null, new object?[] { builder });
        }

        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    /// <summary>True when a served tool's <c>_meta</c> carries <c>anthropic/alwaysLoad: true</c>.</summary>
    private static bool CarriesAlwaysLoadMeta(ModelContextProtocol.Protocol.Tool tool) =>
        tool.Meta is not null
        && tool.Meta.TryGetPropertyValue("anthropic/alwaysLoad", out var value)
        && value is not null
        && value.GetValue<bool>();

    [Fact]
    public void ExactlyTheFourEntryTools_CarryAlwaysLoadMeta()
    {
        var tools = BuildProtocolTools();
        Assert.NotEmpty(tools);

        var actualAlwaysLoaded = tools
            .Where(CarriesAlwaysLoadMeta)
            .Select(t => t.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        Assert.Equal(
            ExpectedAlwaysLoaded.OrderBy(n => n, StringComparer.Ordinal),
            actualAlwaysLoaded);
    }

    [Fact]
    public void NoOtherTool_CarriesAlwaysLoadMeta()
    {
        var tools = BuildProtocolTools();

        var unexpected = tools
            .Where(t => CarriesAlwaysLoadMeta(t) && !ExpectedAlwaysLoaded.Contains(t.Name, StringComparer.Ordinal))
            .Select(t => t.Name)
            .ToList();

        Assert.True(
            unexpected.Count == 0,
            "These tools unexpectedly carry _meta[\"anthropic/alwaysLoad\"] = true: " + string.Join(", ", unexpected));
    }
}
