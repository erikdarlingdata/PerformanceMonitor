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
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Golden-sample tests for the Gemini-compatible MCP tool schema transform (issue #1074).
///
/// Google Antigravity (Gemini) connects to the MCP server but lists zero tools when any tool's
/// parameter schema contains a union "type" array (e.g. ["string","null"], emitted for a
/// nullable-with-default parameter) or a "default" keyword. These tests assert against the exact
/// JSON schema the server advertises in tools/list (McpServerTool.ProtocolTool.InputSchema) — no
/// running server required.
/// </summary>
public class McpSchemaCompatTests
{
    /// <summary>All Lite MCP tool classes, discovered by their [McpServerToolType] attribute.</summary>
    private static List<Type> LiteToolTypes() =>
        typeof(PerformanceMonitorLite.Mcp.McpWaitTools).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// A tool parameter is DI-injected (and excluded from the schema) when its type is not a simple
    /// model-facing value. Registering these types as no-op singletons makes the SDK's
    /// IServiceProviderIsService report them as services, exactly as production DI does.
    /// </summary>
    private static bool IsServiceParameter(Type t) =>
        !t.IsPrimitive && t != typeof(string) && !t.IsEnum && t != typeof(decimal) &&
        t != typeof(DateTime) && t != typeof(DateTimeOffset) && t != typeof(Guid) && t != typeof(TimeSpan);

    private static readonly MethodInfo GeminiCompatibleToolsMethod =
        typeof(McpSchemaCompat).GetMethod(
            nameof(McpSchemaCompat.WithGeminiCompatibleTools),
            BindingFlags.Public | BindingFlags.Static)!;

    private static readonly MethodInfo StockWithToolsMethod =
        typeof(McpServerBuilderExtensions).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(McpServerBuilderExtensions.WithTools)
                && m.IsGenericMethodDefinition
                && m.GetParameters() is { Length: 2 } p
                && p[1].ParameterType == typeof(JsonSerializerOptions));

    /// <summary>
    /// Builds the protocol tools as the host would, registering each tool type via the supplied
    /// open-generic registration method (the Gemini-compatible path or the stock SDK path).
    /// </summary>
    private static List<ModelContextProtocol.Protocol.Tool> BuildProtocolTools(MethodInfo openGenericRegister, bool stock)
    {
        var toolTypes = LiteToolTypes();
        var services = new ServiceCollection();

        /* Register every service-typed tool parameter as a no-op singleton so it is excluded from the
           schema. The test only reads schemas and never invokes a tool, so the null factory is never run. */
        var serviceParamTypes = toolTypes
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(IsServiceParameter)
            .Distinct();

        foreach (var serviceType in serviceParamTypes)
        {
            services.AddSingleton(serviceType, _ => null!);
        }

        var builder = services.AddMcpServer();

        foreach (var toolType in toolTypes)
        {
            var closed = openGenericRegister.MakeGenericMethod(toolType);
            var args = stock ? new object?[] { builder, null } : new object?[] { builder };
            closed.Invoke(null, args);
        }

        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().Select(t => t.ProtocolTool).ToList();
    }

    /// <summary>Recursively collects every schema-violation path (union type arrays or default keywords).</summary>
    private static List<string> FindViolations(string toolName, JsonElement schema)
    {
        var violations = new List<string>();
        Walk(schema, toolName);
        return violations;

        void Walk(JsonElement element, string path)
        {
            switch (element.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var property in element.EnumerateObject())
                    {
                        if (property.NameEquals("type") && property.Value.ValueKind == JsonValueKind.Array)
                        {
                            violations.Add($"{path}: union type {property.Value.GetRawText()}");
                        }

                        if (property.NameEquals("default"))
                        {
                            violations.Add($"{path}: default = {property.Value.GetRawText()}");
                        }

                        Walk(property.Value, $"{path}.{property.Name}");
                    }
                    break;

                case JsonValueKind.Array:
                    var index = 0;
                    foreach (var item in element.EnumerateArray())
                    {
                        Walk(item, $"{path}[{index++}]");
                    }
                    break;
            }
        }
    }

    [Fact]
    public void GeminiCompatibleTools_EmitNoUnionTypesOrDefaultKeywords()
    {
        var tools = BuildProtocolTools(GeminiCompatibleToolsMethod, stock: false);

        Assert.NotEmpty(tools);

        var allViolations = tools
            .SelectMany(t => FindViolations(t.Name, t.InputSchema))
            .ToList();

        Assert.True(
            allViolations.Count == 0,
            "Gemini-incompatible schema keywords leaked into tools/list:\n" + string.Join("\n", allViolations));
    }

    [Fact]
    public void StockWithTools_WouldEmitUnionTypesOrDefaults_ProvingTransformIsNecessary()
    {
        /* Guards the premise: if a future SDK stops emitting union types / defaults, the transform
           becomes unnecessary and this test will flag it for removal. */
        var tools = BuildProtocolTools(StockWithToolsMethod, stock: true);

        var violationCount = tools.Sum(t => FindViolations(t.Name, t.InputSchema).Count);

        Assert.True(
            violationCount > 0,
            "Expected the stock SDK registration to emit union types / default keywords; it did not. " +
            "If the SDK changed, McpSchemaCompat may no longer be needed.");
    }

    [Fact]
    public void GeminiCompatibleTools_ExposeSameToolCountAsStock()
    {
        var gemini = BuildProtocolTools(GeminiCompatibleToolsMethod, stock: false);
        var stock = BuildProtocolTools(StockWithToolsMethod, stock: true);

        /* The transform must not drop or add tools — only rewrite their schemas. */
        Assert.Equal(
            stock.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal),
            gemini.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal));
    }
}
