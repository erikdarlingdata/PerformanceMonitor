/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Lite's half of the unknown-argument census (#3870) — the same property, asserted over Lite's own
/// registered tool surface, plus the parity claim that makes "uniform across both SKUs" a fact rather than
/// an intention.
///
/// <para><b>The defect.</b> The MCP SDK binds a call's <c>arguments</c> to tool parameters by name and
/// silently ignores the rest, so a misremembered parameter produced an answer to a different question with
/// nothing in the payload saying so. On Darling that was <c>get_collection_log</c> returning two hundred
/// unfiltered rows for a hallucinated <c>status_filter</c>; the binder is the SDK's, shared, so Lite had the
/// identical hole on its own ~90 reads.</para>
///
/// <para><b>Why the guard is not copied.</b> <see cref="McpUnknownArgumentGuard"/> lives in
/// <c>PerformanceMonitor.Common</c> and BOTH hosts register the same filter object, so there is one
/// implementation, one error shape and one case policy — the two SKUs cannot drift into refusing
/// differently, which is the whole point of putting it there rather than writing two thin copies and a
/// parity pin over them. What this file adds is that Lite's registration EXISTS (the routing half) and that
/// the shared decision behaves correctly against Lite's real advertised schemas (the decision half), which
/// is not implied by Darling's census: the schemas differ per SKU.</para>
/// </summary>
public sealed class McpUnknownArgumentGuardTests
{
    /// <summary>All Lite MCP tool classes, discovered by their [McpServerToolType] attribute — the same
    /// derivation <see cref="McpSchemaCompatTests"/> uses.</summary>
    private static List<Type> LiteToolTypes() =>
        typeof(PerformanceMonitorLite.Mcp.McpWaitTools).Assembly
            .GetTypes()
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

    private static bool IsServiceParameter(Type t) =>
        !t.IsPrimitive && t != typeof(string) && !t.IsEnum && t != typeof(decimal) &&
        t != typeof(DateTime) && t != typeof(DateTimeOffset) && t != typeof(Guid) && t != typeof(TimeSpan);

    /// <summary>
    /// Lite's tools with the schemas the host advertises, built through the same
    /// <c>WithGeminiCompatibleTools</c> path, with service-typed parameters registered as null singletons.
    /// Nothing is invoked — only schemas and the guard's decision are read.
    /// </summary>
    private static List<McpServerTool> RegisteredTools()
    {
        var toolTypes = LiteToolTypes();
        var services = new ServiceCollection();

        var serviceParamTypes = toolTypes
            .SelectMany(t => t.GetMethods(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance))
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

        var register = typeof(McpSchemaCompat).GetMethod(
            nameof(McpSchemaCompat.WithGeminiCompatibleTools),
            BindingFlags.Public | BindingFlags.Static)!;

        foreach (var toolType in toolTypes)
        {
            register.MakeGenericMethod(toolType).Invoke(null, new object?[] { builder });
        }

        var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToList();
    }

    private static CallToolRequestParams Call(string toolName, Dictionary<string, JsonElement> arguments) =>
        new() { Name = toolName, Arguments = arguments };

    private static Dictionary<string, JsonElement> Args(params (string Key, string Value)[] pairs)
    {
        var arguments = new Dictionary<string, JsonElement>(StringComparer.Ordinal);

        foreach (var (key, value) in pairs)
        {
            arguments[key] = JsonSerializer.SerializeToElement(value);
        }

        return arguments;
    }

    private static string TextOf(CallToolResult result)
    {
        Assert.NotNull(result.Content);
        var block = Assert.Single(result.Content!);
        return Assert.IsType<TextContentBlock>(block).Text;
    }

    /// <summary>
    /// THE census: every tool Lite registers refuses a deliberately unknown key, in the house refusal
    /// envelope, naming the key. Derived from the attribute scan, so a tool added tomorrow is covered
    /// without editing this test.
    /// </summary>
    [Fact]
    public void EveryRegisteredTool_RefusesAnUnknownArgument_AndNamesIt()
    {
        var tools = RegisteredTools();

        Assert.NotEmpty(tools);

        var unguarded = new List<string>();

        foreach (var tool in tools)
        {
            var name = tool.ProtocolTool.Name;
            var result = McpUnknownArgumentGuard.Refuse(
                Call(name, Args(("definitely_not_a_parameter", "x"))),
                tool);

            if (result is null)
            {
                unguarded.Add($"{name}: accepted an unknown argument");
                continue;
            }

            var wire = TextOf(result);

            if (!McpHelpers.IsRefusalEnvelope(wire))
            {
                unguarded.Add($"{name}: refused outside the shared refusal envelope -> {wire}");
                continue;
            }

            if (!wire.Contains("definitely_not_a_parameter", StringComparison.Ordinal))
            {
                unguarded.Add($"{name}: refusal does not name the offending key -> {wire}");
            }
        }

        Assert.True(
            unguarded.Count == 0,
            "These registered Lite tools do not refuse an argument they never declared, so a misspelled "
            + "parameter from an agent caller is silently dropped and the tool answers a different "
            + "question:\n" + string.Join("\n", unguarded));
    }

    /// <summary>
    /// The inverse: the guard may only reject keys the binder would have dropped, so no tool refuses its own
    /// declared parameters, and an omitted-everything call is never refused. Optionality and null handling
    /// are untouched by design and pinned here.
    /// </summary>
    [Fact]
    public void EveryRegisteredTool_AcceptsItsOwnDeclaredParameters_AndAnEmptyCall()
    {
        var wrongful = new List<string>();

        foreach (var tool in RegisteredTools())
        {
            var name = tool.ProtocolTool.Name;

            if (McpUnknownArgumentGuard.Refuse(Call(name, new Dictionary<string, JsonElement>()), tool) is not null)
            {
                wrongful.Add($"{name}: refused a call with no arguments at all");
            }

            var schema = tool.ProtocolTool.InputSchema;
            if (schema.ValueKind != JsonValueKind.Object
                || !schema.TryGetProperty("properties", out var properties)
                || properties.ValueKind != JsonValueKind.Object)
            {
                continue;
            }

            var declared = properties.EnumerateObject().Select(p => p.Name).ToArray();
            if (declared.Length == 0)
            {
                continue;
            }

            if (McpUnknownArgumentGuard.Refuse(Call(name, Args(declared.Select(p => (p, "x")).ToArray())), tool)
                is { } refused)
            {
                wrongful.Add($"{name}: refused its OWN declared parameters -> {TextOf(refused)}");
            }
        }

        Assert.True(
            wrongful.Count == 0,
            "The unknown-argument guard refused Lite calls it must serve:\n" + string.Join("\n", wrongful));
    }

    /// <summary>
    /// The ROUTING half: Lite's host must install the filter. The shared decision is worth nothing if the
    /// single registration line is lost in a future edit, and no per-tool test would notice — every tool
    /// would still bind, exactly as before #3870.
    /// </summary>
    [Fact]
    public void LitesHost_RegistersTheUnknownArgumentGuard_AsACallToolFilter()
    {
        var path = Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs");
        var source = File.ReadAllText(path);

        Assert.True(
            Regex.IsMatch(source, @"AddCallToolFilter\(\s*McpUnknownArgumentGuard\.Instance\s*\)"),
            "Lite/Mcp/McpHostService.cs does not register McpUnknownArgumentGuard as a call-tool filter. "
            + "Without that ONE line every Lite tool goes back to binding arguments by name and silently "
            + "dropping the rest, which is #3870.");
    }

    /// <summary>
    /// The PARITY claim, which is why this is one guard and not two copies: both hosts register the SAME
    /// shared filter, so the error shape, the case policy and the near-miss wording cannot differ between
    /// SKUs. Asserted by reading both host sources — if a future edit gives either SKU a private guard, this
    /// reds and says so.
    /// </summary>
    [Fact]
    public void BothHosts_RegisterTheSameSharedGuard_SoTheRefusalCannotDriftBetweenSkus()
    {
        var lite = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Mcp", "McpHostService.cs"));
        var darling = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs"));

        const string Registration = @"AddCallToolFilter\(\s*McpUnknownArgumentGuard\.Instance\s*\)";

        Assert.True(Regex.IsMatch(lite, Registration), "Lite's host does not register the shared guard.");
        Assert.True(Regex.IsMatch(darling, Registration), "Darling's host does not register the shared guard.");

        /* And the guard itself is the shared one, in Common — not a per-SKU copy that merely shares a name. */
        Assert.Equal("PerformanceMonitor.Common", typeof(McpUnknownArgumentGuard).Namespace);
        Assert.Equal(
            "PerformanceMonitor.Common",
            typeof(McpUnknownArgumentGuard).Assembly.GetName().Name);
    }

    /// <summary>
    /// The refusal SHAPE on a Lite tool: status invalid, hints.parameter naming the offending key, the
    /// accepted list quoted. Pinned per SKU because a client reads Lite's wire, not Darling's.
    /// </summary>
    [Fact]
    public void TheRefusal_WearsTheHouseEnvelope_AndListsAcceptedParameters()
    {
        var tool = RegisteredTools().First(t =>
            t.ProtocolTool.InputSchema.ValueKind == JsonValueKind.Object
            && t.ProtocolTool.InputSchema.TryGetProperty("properties", out var p)
            && p.ValueKind == JsonValueKind.Object
            && p.EnumerateObject().Any(x => x.Name == "hours_back"));

        var result = McpUnknownArgumentGuard.Refuse(
            Call(tool.ProtocolTool.Name, Args(("hours", "1"))),
            tool);

        Assert.NotNull(result);
        Assert.True(result!.IsError);

        var wire = TextOf(result);
        Assert.True(McpHelpers.IsRefusalEnvelope(wire), wire);

        using var document = JsonDocument.Parse(wire);
        var root = document.RootElement;

        Assert.Equal("invalid", root.GetProperty("status").GetString());
        Assert.Equal("hours", root.GetProperty("hints").GetProperty("parameter").GetString());

        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("'hours'", message, StringComparison.Ordinal);
        Assert.Contains("hours_back", message, StringComparison.Ordinal);
        Assert.Contains("Accepted parameters:", message, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
