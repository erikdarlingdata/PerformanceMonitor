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
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A value an integer parameter cannot take, such as <c>hours_back: 0.5</c>, is refused with a message that names
/// the parameter, instead of failing inside the SDK's argument binding as "An error occurred invoking ...".
///
/// <para>The binder reads an integer parameter only from an integer literal or a string holding one. Anything else
/// (<c>0.5</c>, <c>1.0</c>, <c>1e1</c>, <c>"0.5"</c>, <c>true</c>) threw a <c>JsonException</c> before the tool
/// body ran, and the SDK answered every one of them with the same generic error. The shared call-tool guard now
/// checks each integer parameter's value before dispatch, as it already checks the argument names.</para>
/// </summary>
public sealed class McpWholeNumberArgumentTests
{
    internal static List<Type> RegisteredToolTypes()
    {
        var registered = McpUnknownArgumentGuardTests.RegisteredToolTypeNames();

        return typeof(DarlingMcpHostService).Assembly
            .GetTypes()
            .Where(t => registered.Contains(t.Name))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();
    }

    /// <summary>Inert stand-ins for the two service types that cannot be left uninitialized. The data source points
    /// at a closed local port and is never opened by these tests: every call they make is refused before the body.</summary>
    private static object? InertInstanceFor(Type serviceType) =>
        serviceType == typeof(NpgsqlDataSource) ? NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=none;Database=none;Timeout=1")
        : serviceType == typeof(ILogger) ? NullLogger.Instance
        : null;

    /// <param name="installGuard">False to leave the call-tool guard out, so a call meets the SDK's binder alone.</param>
    /// <param name="extraToolTypes">Test-only tool classes to register beside the shipped ones, through the same
    /// <c>McpSchemaCompat</c> path.</param>
    internal static Task<McpInProcessHost> StartHostAsync(bool installGuard = true, params Type[] extraToolTypes) =>
        McpInProcessHost.StartAsync(
            RegisteredToolTypes().Concat(extraToolTypes).ToList(), McpUnknownArgumentGuardTests.IsServiceParameter,
            InertInstanceFor, installGuard, TestContext.Current.CancellationToken);

    private static JsonElement Json(string raw) => JsonDocument.Parse(raw).RootElement.Clone();

    /// <summary>The tool names whose method declares a parameter named <paramref name="parameter"/>.</summary>
    private static SortedSet<string> ToolsDeclaring(string parameter) =>
        new(RegisteredToolTypes()
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Select(m => (Method: m, Attribute: m.GetCustomAttribute<McpServerToolAttribute>()))
            .Where(x => x.Attribute is not null && x.Method.GetParameters().Any(p => p.Name == parameter))
            .Select(x => x.Attribute!.Name ?? x.Method.Name),
            StringComparer.Ordinal);

    /// <summary>
    /// Every tool that takes <c>hours_back</c>, called through a real in-process server with <c>hours_back: 0.5</c>,
    /// answers with the refusal that names <c>hours_back</c>. The list comes from the tools the host registers, so a
    /// tool added later is called too, and a tool that does not answer this way fails here by name.
    /// </summary>
    [Fact]
    public async Task EveryToolThatTakesHoursBack_RefusesHalfAnHour_WithAMessageNamingIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = await StartHostAsync();

        var tools = await host.Client.ListToolsAsync(cancellationToken: ct);
        var advertised = new SortedSet<string>(StringComparer.Ordinal);
        var failures = new List<string>();

        foreach (var tool in tools)
        {
            if (!tool.JsonSchema.TryGetProperty("properties", out var properties)
                || !properties.TryGetProperty("hours_back", out var hoursBack))
            {
                continue;
            }

            advertised.Add(tool.Name);

            if (!hoursBack.TryGetProperty("type", out var type) || type.ValueKind != JsonValueKind.String || type.GetString() != "integer")
            {
                failures.Add($"{tool.Name}: hours_back is advertised as {hoursBack.GetRawText()}, not as an integer");
                continue;
            }

            var result = await host.Client.CallToolAsync(
                tool.Name, new Dictionary<string, object?> { ["hours_back"] = Json("0.5") }, cancellationToken: ct);

            var failure = McpInProcessHost.WholeNumberRefusalProblem(tool.Name, "hours_back", "whole number of hours", result);
            if (failure is not null)
            {
                failures.Add(failure);
            }
        }

        Assert.NotEmpty(advertised);

        /* The tools that declare hours_back in code are exactly the tools that advertise it, so none slips past
           the loop above by leaving it out of its schema. */
        var declared = ToolsDeclaring("hours_back");
        foreach (var missing in declared.Except(advertised))
        {
            failures.Add($"{missing}: declares hours_back but does not advertise it");
        }

        Assert.True(
            failures.Count == 0,
            $"{failures.Count} of {advertised.Count} tools that take hours_back do not refuse 0.5 by name:\n"
            + string.Join("\n", failures));
    }

    /// <summary>
    /// The same treatment for an integer parameter other than <c>hours_back</c>, through the same real call path:
    /// <c>get_wait_stats</c> with <c>limit: 2.5</c>.
    /// </summary>
    [Fact]
    public async Task AnotherIntegerParameter_RefusesAFraction_WithAMessageNamingIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var host = await StartHostAsync();

        var result = await host.Client.CallToolAsync(
            "get_wait_stats", new Dictionary<string, object?> { ["limit"] = Json("2.5") }, cancellationToken: ct);

        Assert.Null(McpInProcessHost.WholeNumberRefusalProblem("get_wait_stats", "limit", "whole number", result));
    }

    /// <summary>
    /// Every integer parameter of every registered tool refuses 2.5, 1.0, "0.5" and true by name, and passes 1, "1"
    /// and null. Checked against the guard's decision, so no tool body runs. The binder reads 1 and "1" for every
    /// integer parameter, but null only for a nullable one: the guard passes null because the schema does not say
    /// which parameters are nullable, and a non-nullable one still gets the SDK's own error.
    /// </summary>
    [Fact]
    public async Task EveryIntegerParameter_RefusesWhatTheBinderCannotRead_AndAcceptsWhatItCan()
    {
        await using var host = await StartHostAsync();
        var tools = host.RegisteredTools;
        var failures = new List<string>();
        var checkedCount = 0;

        foreach (var tool in tools)
        {
            var name = tool.ProtocolTool.Name;
            if (!tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties))
            {
                continue;
            }

            foreach (var property in properties.EnumerateObject())
            {
                if (!property.Value.TryGetProperty("type", out var type) || type.ValueKind != JsonValueKind.String || type.GetString() != "integer")
                {
                    continue;
                }

                checkedCount++;

                foreach (var bad in new[] { "2.5", "1.0", "\"0.5\"", "true" })
                {
                    var refusal = McpUnknownArgumentGuard.Refuse(Call(name, property.Name, bad), tool);
                    var problem = refusal is null
                        ? $"{name}.{property.Name}: accepted {bad}"
                        : McpInProcessHost.WholeNumberRefusalProblem(name, property.Name, "whole number", refusal);

                    if (problem is not null)
                    {
                        failures.Add(problem);
                    }
                }

                foreach (var good in new[] { "1", "\"1\"", "null" })
                {
                    if (McpUnknownArgumentGuard.Refuse(Call(name, property.Name, good), tool) is { } refused)
                    {
                        failures.Add($"{name}.{property.Name}: refused {good}, which the guard must pass -> {McpInProcessHost.TextOf(refused)}");
                    }
                }
            }
        }

        Assert.True(checkedCount > 0, "Found no integer parameters to check.");
        Assert.True(
            failures.Count == 0,
            $"{failures.Count} problems across {checkedCount} integer parameters:\n" + string.Join("\n", failures.Take(40)));
    }

    /// <summary>
    /// A whole number past <see cref="long"/> is refused, since no integer parameter can read it, and the refusal says
    /// why: too large, or too small. It must not ask for "no decimal point", because the caller sent none.
    /// </summary>
    [Theory]
    [InlineData("99999999999999999999", "too large")]
    [InlineData("\"99999999999999999999\"", "too large")]
    [InlineData("-99999999999999999999", "too small")]
    [InlineData("\"-99999999999999999999\"", "too small")]
    public async Task AWholeNumberPastLong_IsRefusedAsTooLargeOrTooSmall(string rawValue, string reason)
    {
        await using var host = await StartHostAsync();
        var tool = host.RegisteredTools.Single(registered => registered.ProtocolTool.Name == "get_wait_stats");

        var refusal = McpUnknownArgumentGuard.Refuse(Call("get_wait_stats", "hours_back", rawValue), tool);

        Assert.NotNull(refusal);
        var problem = McpInProcessHost.WholeNumberRefusalProblem("get_wait_stats", "hours_back", reason, refusal);
        Assert.True(problem is null, problem);
        var message = McpInProcessHost.RefusalMessage(refusal)!;
        Assert.False(message.Contains("no decimal point", StringComparison.Ordinal), message);
    }

    /// <summary>
    /// The refusal quotes the value back, cut short at 40 UTF-16 units. Twenty-one emoji in a string are 44 units with
    /// the quotes, and a cut at 40 splits the twentieth emoji, which System.Text.Json writes as U+FFFD. The cut keeps
    /// whole characters, so the message shows nineteen emoji, then the truncation suffix, and no U+FFFD.
    /// </summary>
    [Fact]
    public async Task AValueCutShortInTheRefusal_KeepsWholeCharacters()
    {
        const string Emoji = "\U0001F600";
        await using var host = await StartHostAsync();
        var tool = host.RegisteredTools.Single(registered => registered.ProtocolTool.Name == "get_wait_stats");
        var sent = "\"" + string.Concat(Enumerable.Repeat(Emoji, 21)) + "\"";

        var refusal = McpUnknownArgumentGuard.Refuse(Call("get_wait_stats", "hours_back", sent), tool);

        Assert.NotNull(refusal);
        var message = McpInProcessHost.RefusalMessage(refusal)!;
        Assert.False(message.Contains('�'), message);
        Assert.Contains(
            "the call sent \"" + string.Concat(Enumerable.Repeat(Emoji, 19)) + "... (truncated).", message, StringComparison.Ordinal);
    }

    private static CallToolRequestParams Call(string toolName, string parameter, string rawValue) =>
        new() { Name = toolName, Arguments = new Dictionary<string, JsonElement> { [parameter] = Json(rawValue) } };
}
