// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An argument the tool does not declare is REFUSED, never dropped (#3870) — the request-side twin of the
/// payload-contract census, pinned as a property of the whole surface rather than of one tool.
///
/// <para><b>What shipped.</b> <c>get_collection_log {"server_name":"…","hours":1,"status_filter":"failure"}</c>
/// returned two hundred rows, every one SUCCESS, at the twenty-four-hour default. <c>status_filter</c> does
/// not exist and the real window knob is <c>hours_back</c>, so BOTH arguments the caller set were dropped by
/// the SDK's bind-by-name and the tool answered a question nobody asked — with no tell in the payload beyond
/// an <c>hours_back</c> echo the caller had no reason to re-read. The tool already refused a wrong VALUE on
/// <c>limit</c> ("told no instead of quietly given 1000") while swallowing a wrong NAME.</para>
///
/// <para><b>Census design, and why THIS shape.</b> The guard is ONE call-tool filter, so the binding
/// property cannot vary per tool — every tool reaches its method through
/// <see cref="McpUnknownArgumentGuard"/> or through none. Enumerating 147 tools and invoking each with a
/// junk key would need a live Postgres store per call and would be 147 assertions of one decision. So the
/// census is split the way the architecture is: the DECISION is pinned directly against
/// <see cref="McpUnknownArgumentGuard.Refuse"/> over every REGISTERED tool's real advertised schema (so a
/// tool whose schema the guard cannot read, or whose parameters it would wrongly reject, reds here), and the
/// ROUTING is pinned by reading the host source for the single registration line (so a future host that
/// forgets the filter reds even though every tool still binds). Together those are the same claim an
/// invoke-everything census would make, without a database.</para>
///
/// <para><b>What is deliberately NOT refused.</b> An omitted optional parameter (the guard reads arrived
/// keys only), a declared parameter a code path ignores, and the protocol's own metadata — <c>_meta</c> and
/// the progress token ride SIBLINGS of <c>arguments</c> in <c>CallToolRequestParams</c>, never inside it,
/// which is asserted here rather than assumed because the whole guard would be a client-compatibility
/// hazard if it were false. Case-insensitive matching is asserted too: it mirrors the binder, so the guard
/// can only ever refuse a call the binder would already have mangled.</para>
/// </summary>
public sealed class McpUnknownArgumentGuardTests
{
    /// <summary>
    /// Every tool the Darling host registers, with the schema it advertises — built through the same
    /// <c>WithGeminiCompatibleTools</c> path the host uses, with each DI-injected service parameter
    /// registered as a null singleton (nothing is invoked here; only schemas and the guard's decision are
    /// read). The registration list is derived from the host source so this census covers what SHIPS.
    /// </summary>
    private static List<McpServerTool> RegisteredTools()
    {
        var registered = RegisteredToolTypeNames();
        var toolTypes = typeof(DarlingMcpHostService).Assembly
            .GetTypes()
            .Where(t => registered.Contains(t.Name))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();

        Assert.NotEmpty(toolTypes);

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

    /// <summary>
    /// A tool parameter is DI-injected (and excluded from the advertised schema) when its type is not a
    /// simple model-facing value — the same predicate the schema-compat census uses, for the same reason.
    /// </summary>
    private static bool IsServiceParameter(Type t) =>
        !t.IsPrimitive && t != typeof(string) && !t.IsEnum && t != typeof(decimal) &&
        t != typeof(DateTime) && t != typeof(DateTimeOffset) && t != typeof(Guid) && t != typeof(TimeSpan);

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
    /// THE census: for every registered tool, a deliberately unknown key is refused, the refusal wears the
    /// house envelope, and the message names the offending key. One decision, asserted across the whole
    /// registered surface — so a tool added tomorrow is covered without editing this test, and a tool whose
    /// schema the guard cannot read shows up here rather than as a silent hole.
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
            "These registered tools do not refuse an argument they never declared, so a misspelled "
            + "parameter from an agent caller is silently dropped and the tool answers a different "
            + "question:\n" + string.Join("\n", unguarded));
    }

    /// <summary>
    /// The ROUTING half of the census: the host must actually install the filter. The decision above is
    /// worth nothing if the single registration line is dropped in a future edit of the host, and no
    /// per-tool test would notice — the tools would all still bind, exactly as they did before #3870.
    /// </summary>
    [Fact]
    public void TheHost_RegistersTheUnknownArgumentGuard_AsACallToolFilter()
    {
        var path = HostSourcePath();
        var source = File.ReadAllText(path);

        Assert.True(
            Regex.IsMatch(source, @"AddCallToolFilter\(\s*McpUnknownArgumentGuard\.Instance\s*\)"),
            $"{Path.GetFileName(path)} does not register McpUnknownArgumentGuard as a call-tool filter. "
            + "Without that ONE line every tool goes back to binding arguments by name and silently "
            + "dropping the rest, which is #3870: a misspelled parameter produces a confidently wrong "
            + "answer. If the registration style changed, teach this test the new one rather than delete "
            + "it.");
    }

    /// <summary>
    /// The refusal's SHAPE, pinned on the call that motivated the issue: the key is named, the near-miss is
    /// offered, the tool's accepted parameters are listed, and <c>hints.parameter</c> carries the offending
    /// key so a client can branch on what to fix without parsing prose.
    /// </summary>
    [Fact]
    public void TheRefusal_NamesTheKey_SuggestsTheNearMiss_AndListsAcceptedParameters()
    {
        var tool = RegisteredTools().First(t => t.ProtocolTool.Name == "get_collection_log");

        var result = McpUnknownArgumentGuard.Refuse(
            Call("get_collection_log", Args(("status_filter", "failure"), ("hours", "1"))),
            tool);

        Assert.NotNull(result);
        Assert.True(result!.IsError);

        var wire = TextOf(result);

        Assert.True(McpHelpers.IsRefusalEnvelope(wire), wire);

        using var document = JsonDocument.Parse(wire);
        var root = document.RootElement;

        Assert.Equal("invalid", root.GetProperty("status").GetString());
        Assert.Equal(
            "hours",
            root.GetProperty("hints").GetProperty("parameter").GetString());

        var message = root.GetProperty("message").GetString()!;

        /* Both bogus keys are named, not just the first. */
        Assert.Contains("'hours'", message, StringComparison.Ordinal);
        Assert.Contains("'status_filter'", message, StringComparison.Ordinal);
        Assert.Contains("get_collection_log", message, StringComparison.Ordinal);

        /* The near-miss: hours -> hours_back is the whole remedy for the call in the issue. */
        Assert.Contains("hours_back", message, StringComparison.Ordinal);

        /* And the accepted list, so a model can correct itself in one turn. */
        Assert.Contains("Accepted parameters:", message, StringComparison.Ordinal);
        Assert.Contains("server_name", message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The guard must never refuse a call that WORKS. Every parameter a tool advertises is accepted, and an
    /// omitted optional parameter is not an unknown key — the two ways a strict guard would break the
    /// surface it is supposed to protect.
    /// </summary>
    [Fact]
    public void EveryRegisteredTool_AcceptsItsOwnDeclaredParameters_AndAnEmptyCall()
    {
        var tools = RegisteredTools();
        var wrongful = new List<string>();

        foreach (var tool in tools)
        {
            var name = tool.ProtocolTool.Name;

            /* An omitted-everything call: optionality must be untouched. */
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

            var everything = Args(declared.Select(p => (p, "x")).ToArray());

            if (McpUnknownArgumentGuard.Refuse(Call(name, everything), tool) is { } refused)
            {
                wrongful.Add($"{name}: refused its OWN declared parameters -> {TextOf(refused)}");
            }
        }

        Assert.True(
            wrongful.Count == 0,
            "The unknown-argument guard refused calls it must serve — the guard may only reject keys the "
            + "binder would have dropped:\n" + string.Join("\n", wrongful));
    }

    /// <summary>
    /// Case is the binder's, stated. A key differing from a real parameter only by case is BOUND by the SDK,
    /// so the guard accepts it: refusing there would break working calls, and the guard's whole license is
    /// that it can only reject what would have been dropped anyway.
    /// </summary>
    [Fact]
    public void AKeyDifferingOnlyByCase_IsAcceptedBecauseTheBinderBindsIt()
    {
        var tool = RegisteredTools().First(t => t.ProtocolTool.Name == "get_collection_log");

        Assert.Null(McpUnknownArgumentGuard.Refuse(
            Call("get_collection_log", Args(("HOURS_BACK", "1"))),
            tool));
    }

    /// <summary>
    /// The protocol's own metadata cannot be refused, because it never reaches the guard: <c>_meta</c> and
    /// the progress token are SIBLINGS of <c>Arguments</c> on <see cref="CallToolRequestParams"/>. Asserted
    /// against the SDK's type rather than assumed — if a future SDK moved them INTO the arguments object,
    /// this guard would start refusing well-formed client traffic, and this is the test that would say so.
    /// </summary>
    [Fact]
    public void ProtocolMetadata_TravelsOutsideTheArgumentsObject()
    {
        var argumentsProperty = typeof(CallToolRequestParams).GetProperty("Arguments");
        Assert.NotNull(argumentsProperty);

        /* Both live on the params (via RequestParams), NOT inside the arguments dictionary. */
        Assert.NotNull(typeof(CallToolRequestParams).GetProperty("Meta"));
        Assert.NotNull(typeof(CallToolRequestParams).GetProperty("ProgressToken"));

        var tool = RegisteredTools().First(t => t.ProtocolTool.Name == "get_collection_log");

        /* And a params object carrying protocol furniture but no stray argument is clean. */
        var parameters = new CallToolRequestParams
        {
            Name = "get_collection_log",
            Arguments = Args(("server_name", "SQL2022")),
        };

        Assert.Null(McpUnknownArgumentGuard.Refuse(parameters, tool));
    }

    /// <summary>
    /// The registrations named in the host source — the same derivation
    /// <see cref="McpToolTypeRegistrationTests"/> uses, so this census covers the tools that actually ship
    /// rather than every class in the assembly.
    /// </summary>
    private static HashSet<string> RegisteredToolTypeNames()
    {
        var source = File.ReadAllText(HostSourcePath());

        var names = Regex
            .Matches(source, @"WithGeminiCompatibleTools<(\w+)>")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);

        Assert.True(names.Count > 0, "Found no .WithGeminiCompatibleTools<T>() registrations in the host.");

        return names;
    }

    private static string HostSourcePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);

        while (dir is not null)
        {
            var candidate = Path.Combine(
                dir.FullName,
                "Darling",
                "PerformanceMonitor.Darling.Service",
                "Mcp",
                "DarlingMcpHostService.cs");

            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = dir.Parent;
        }

        throw new FileNotFoundException(
            "Could not locate DarlingMcpHostService.cs by walking up from the test output directory.");
    }
}
