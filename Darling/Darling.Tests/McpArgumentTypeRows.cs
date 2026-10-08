/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A test-only tool with a parameter of each CLR type that a product's shipped tools do not declare: <see cref="short"/>,
/// <see cref="byte"/>, <see cref="ulong"/>, an integer array and a nullable <see cref="bool"/> (found by reflection
/// over the tool types of both products), and <see cref="long"/> and <c>long?</c> for Lite. Darling's tools declare
/// both. Lite declares no <c>long?</c>, and its one <see cref="long"/> (<c>plan_id</c> on
/// <c>analyze_query_store_plan</c>) comes with a second required argument (<c>database_name</c>), so the runner
/// passes that tool over. The in-process host registers it through the same
/// <c>McpSchemaCompat.WithGeminiCompatibleTools</c> path as the shipped tools, so the call-tool guard meets it the same
/// way. It carries no <c>McpServerToolType</c> attribute, so no census over the product's tool types sees it. It also
/// declares a <see cref="CancellationToken"/>, which the SDK supplies and the schema leaves out, so the check that the
/// record holds nothing the schema does not advertise always has such a parameter to see, whatever the shipped tools
/// declare.
/// </summary>
internal static class McpArgumentTypeProbeTools
{
    [McpServerTool(Name = "probe_argument_types")]
    [Description("Test-only tool: one parameter of each CLR type no shipped tool declares.")]
    public static string Probe(
        [Description("A short.")] short small = 0,
        [Description("A byte.")] byte tiny = 0,
        [Description("A ulong.")] ulong huge = 0,
        [Description("An integer array.")] int[]? ids = null,
        [Description("A nullable bool.")] bool? maybe = null,
        [Description("A long.")] long big = 0,
        [Description("A nullable long.")] long? maybeBig = null,
        CancellationToken cancellationToken = default) => "ok";
}

/// <summary>
/// The table behind the per-type argument pins, shared by both products. Each row is one value sent for one CLR type,
/// and says whether the SDK's binder can read it. The runner checks BOTH halves of every row: with the guard left out,
/// the binder throws on exactly the rows marked refused (so the table states what the binder does, not what the guard
/// hopes), and with the guard in, those rows are refused by a message that names the argument and says what it takes,
/// while every other row is left alone.
///
/// <para>The tool and parameter for a row are found by reflection over the product's tool types: the first tool that
/// declares a parameter of that CLR type and needs no other argument, taking the tool types in the order the host
/// registers them and each type's methods by method name, and passing over a tool whose name says it changes data
/// (<c>delete_</c>, <c>create_</c> or <c>update_</c>), since a row that sends a value the binder reads runs the tool.
/// A type no shipped tool declares falls through to
/// <see cref="McpArgumentTypeProbeTools"/>.</para>
/// </summary>
internal static class McpArgumentTypeRows
{
    private static readonly Dictionary<string, Type> s_types = new(StringComparer.Ordinal)
    {
        ["int"] = typeof(int),
        ["int?"] = typeof(int?),
        ["long"] = typeof(long),
        ["long?"] = typeof(long?),
        ["short"] = typeof(short),
        ["byte"] = typeof(byte),
        ["ulong"] = typeof(ulong),
        ["bool"] = typeof(bool),
        ["bool?"] = typeof(bool?),
        ["string"] = typeof(string),
        ["int[]"] = typeof(int[]),
        ["double"] = typeof(double),
        ["double?"] = typeof(double?),
        ["string[]"] = typeof(string[]),
    };

    /// <summary>One row per (type, value): the CLR type, the JSON value as sent, whether the binder cannot read it, and
    /// the phrases the refusal must say, separated by '|' (empty for a value the binder reads).</summary>
    public static TheoryData<string, string, bool, string> All()
    {
        var data = new TheoryData<string, string, bool, string>();
        foreach (var row in Table())
        {
            data.Add(row.Type, row.Raw, row.Refused, row.Says);
        }

        return data;
    }

    /// <summary>The CLR types the table has at least one row for.</summary>
    public static HashSet<string> CoveredTypeKeys() => Table().Select(row => row.Type).ToHashSet(StringComparer.Ordinal);

    /// <summary>The table's key for each CLR type the host advertises a parameter of, across the shipped tool types.</summary>
    public static SortedSet<string> ShippedParameterTypeKeys(McpInProcessHost host, IEnumerable<Type> toolTypes)
    {
        var served = host.RegisteredTools.ToDictionary(tool => tool.ProtocolTool.Name, StringComparer.Ordinal);
        var keys = new SortedSet<string>(StringComparer.Ordinal);

        foreach (var method in toolTypes.SelectMany(
                     type => type.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)))
        {
            if (method.GetCustomAttribute<McpServerToolAttribute>() is not { } attribute
                || !served.TryGetValue(attribute.Name ?? method.Name, out var tool)
                || !tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties))
            {
                continue;
            }

            foreach (var parameter in method.GetParameters().Where(p => properties.TryGetProperty(p.Name!, out _)))
            {
                keys.Add(s_types.FirstOrDefault(entry => entry.Value == parameter.ParameterType).Key
                    ?? parameter.ParameterType.ToString());
            }
        }

        return keys;
    }

    /// <summary>
    /// What is wrong with the types the host recorded for its tools, or an empty list. Every parameter a tool
    /// advertises has a recorded type, a type that agrees with the schema's word for it (an "integer" is an integer
    /// type, a "boolean" is a bool), and an integer parameter refuses null exactly when its type cannot take one.
    /// A tool the record misses would quietly fall back to the schema-only check.
    /// </summary>
    public static List<string> RecordedTypeProblems(McpInProcessHost host)
    {
        var types = host.ParameterTypes;
        var problems = new List<string>();

        foreach (var tool in host.RegisteredTools)
        {
            var name = tool.ProtocolTool.Name;
            if (!tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties) || !properties.EnumerateObject().Any())
            {
                continue;
            }

            if (!types.TryGet(name, out var declared))
            {
                problems.Add($"{name}: no parameter types were recorded");
                continue;
            }

            foreach (var property in properties.EnumerateObject())
            {
                if (!declared.TryGetValue(property.Name, out var parameter))
                {
                    problems.Add($"{name}.{property.Name}: advertised, but no type was recorded");
                    continue;
                }

                var clr = Nullable.GetUnderlyingType(parameter.Type) ?? parameter.Type;
                /* An enum is not a whole number here, as in McpArgumentValueCheck.Bounds: Type.GetTypeCode answers an
                   enum's underlying integer type. */
                var integral = !clr.IsEnum && Type.GetTypeCode(clr) is TypeCode.SByte or TypeCode.Byte or TypeCode.Int16 or TypeCode.UInt16
                    or TypeCode.Int32 or TypeCode.UInt32 or TypeCode.Int64 or TypeCode.UInt64;
                var schemaType = property.Value.TryGetProperty("type", out var type)
                    ? type.ValueKind == JsonValueKind.String
                        ? type.GetString()
                        : type.EnumerateArray().Select(entry => entry.GetString()).FirstOrDefault(word => word != "null")
                    : null;

                if ((schemaType == "integer") != integral || (schemaType == "boolean" && clr != typeof(bool)))
                {
                    problems.Add($"{name}.{property.Name}: the schema says {schemaType}, the recorded type is {parameter.Type}");
                }

                if (schemaType == "integer")
                {
                    var call = new CallToolRequestParams
                    {
                        Name = name,
                        Arguments = new Dictionary<string, JsonElement> { [property.Name] = JsonDocument.Parse("null").RootElement.Clone() },
                    };

                    var refused = McpUnknownArgumentGuard.Refuse(call, tool, types) is not null;
                    if (refused == parameter.AllowsNull)
                    {
                        problems.Add(
                            $"{name}.{property.Name}: null is {(refused ? "refused" : "passed")}, but the recorded type {parameter.Type} "
                            + (parameter.AllowsNull ? "takes null" : "does not take null"));
                    }
                }
            }
        }

        return problems;
    }

    /// <summary>
    /// What the host recorded for a tool that the tool's schema does not advertise, or an empty list: the reverse of
    /// <see cref="RecordedTypeProblems"/>. The record holds only what a caller may send, so a parameter the schema
    /// leaves out (a cancellation token the SDK supplies, a service resolved from DI) has no place in it, whatever the
    /// tool method declares.
    /// </summary>
    public static List<string> RecordedButNotAdvertisedProblems(McpInProcessHost host)
    {
        var types = host.ParameterTypes;
        var problems = new List<string>();

        foreach (var tool in host.RegisteredTools)
        {
            var name = tool.ProtocolTool.Name;
            if (!types.TryGet(name, out var recorded))
            {
                continue;
            }

            var advertised = tool.ProtocolTool.InputSchema.TryGetProperty("properties", out var properties)
                ? properties.EnumerateObject().Select(property => property.Name).ToHashSet(StringComparer.Ordinal)
                : new HashSet<string>(StringComparer.Ordinal);

            problems.AddRange(recorded.Keys
                .Where(parameter => !advertised.Contains(parameter))
                .Select(parameter => $"{name}.{parameter}: recorded, but the schema does not advertise it"));
        }

        return problems;
    }

    private sealed class RowList : List<(string Type, string Raw, bool Refused, string Says)>
    {
        public void Add(string type, string raw, bool refused, string says) => Add((type, raw, refused, says));
    }

    private static List<(string Type, string Raw, bool Refused, string Says)> Table()
    {
        var rows = new RowList();

        /* An int past int range, which the guard used to read as a long and pass. */
        rows.Add("int", "3000000000", true, "whole number|too large");
        rows.Add("int", "-3000000000", true, "whole number|too small");
        rows.Add("int", "\"3000000000\"", true, "whole number|too large");
        rows.Add("int", "2147483647", false, "");
        rows.Add("int", "\"5\"", false, "");
        rows.Add("int", "2.5", true, "whole number|no decimal point");
        rows.Add("int?", "3000000000", true, "whole number|too large");
        rows.Add("int?", "null", false, "");
        rows.Add("long", "9223372036854775807", false, "");
        rows.Add("long", "9223372036854775808", true, "whole number|too large");
        rows.Add("long?", "null", false, "");

        /* null for a parameter that is not nullable. */
        rows.Add("int", "null", true, "whole number|cannot be null");
        rows.Add("long", "null", true, "whole number|cannot be null");
        rows.Add("bool", "null", true, "true or false|cannot be null");

        /* short and byte past their range. */
        rows.Add("short", "32767", false, "");
        rows.Add("short", "32768", true, "whole number|too large");
        rows.Add("short", "-32769", true, "whole number|too small");
        rows.Add("short", "\"7\"", false, "");
        rows.Add("byte", "255", false, "");
        rows.Add("byte", "256", true, "whole number|too large");
        rows.Add("byte", "-1", true, "whole number|too small");

        /* A ulong: past long range but within its own is read, past its own is not. */
        rows.Add("ulong", "18446744073709551615", false, "");
        rows.Add("ulong", "9223372036854775808", false, "");
        rows.Add("ulong", "18446744073709551616", true, "whole number|too large");
        rows.Add("ulong", "-1", true, "whole number|too small");
        /* Zero with a minus sign is no smaller than zero, but an unsigned type does not read the sign. */
        rows.Add("ulong", "-0", true, "whole number|too small");

        /* Integer arrays. */
        rows.Add("int[]", "[1,2,3]", false, "");
        rows.Add("int[]", "[]", false, "");
        rows.Add("int[]", "null", false, "");
        rows.Add("int[]", "[\"1\",\"2\"]", false, "");
        rows.Add("int[]", "[1,2.5]", true, "list|whole numbers");
        rows.Add("int[]", "[3000000000]", true, "list|whole numbers");
        rows.Add("int[]", "[null]", true, "list|whole numbers");
        rows.Add("int[]", "\"1,2,3\"", true, "list|whole numbers");
        rows.Add("int[]", "5", true, "list|whole numbers");

        /* A number, a boolean or a container sent to a string. */
        rows.Add("string", "5", true, "text");
        rows.Add("string", "true", true, "text");
        rows.Add("string", "[\"a\"]", true, "text");
        rows.Add("string", "{}", true, "text");
        rows.Add("string", "\"x\"", false, "");
        rows.Add("string", "null", false, "");

        /* A string, or a number, sent to a boolean. */
        rows.Add("bool", "\"yes\"", true, "true or false");
        rows.Add("bool", "\"true\"", true, "true or false");
        rows.Add("bool", "1", true, "true or false");
        rows.Add("bool", "true", false, "");
        rows.Add("bool", "false", false, "");
        rows.Add("bool?", "null", false, "");
        rows.Add("bool?", "\"yes\"", true, "true or false");

        /* A floating-point parameter, nullable and not. */
        rows.Add("double?", "2.5", false, "");
        rows.Add("double?", "\"2.5\"", false, "");
        rows.Add("double?", "\"abc\"", true, "number");
        rows.Add("double?", "true", true, "number");
        rows.Add("double", "2.5", false, "");
        rows.Add("double", "\"abc\"", true, "number");
        rows.Add("double", "null", true, "number|cannot be null");

        /* A list of text. */
        rows.Add("string[]", "[\"a\",\"b\"]", false, "");
        rows.Add("string[]", "null", false, "");
        rows.Add("string[]", "[\"a\",5]", true, "list|text");
        rows.Add("string[]", "\"a\"", true, "list|text");

        return rows;
    }

    /// <summary>
    /// Runs one row against a host with the guard in and a host with it left out. <paramref name="toolTypes"/> lists
    /// the shipped tool types first and the probe tool last, so a shipped tool is preferred.
    /// </summary>
    public static async Task CheckAsync(
        McpInProcessHost guarded, McpInProcessHost unguarded, IReadOnlyCollection<Type> toolTypes,
        string typeKey, string raw, bool refused, string says, CancellationToken cancellationToken)
    {
        var (tool, parameter) = Resolve(guarded, toolTypes, s_types[typeKey]);
        var arguments = new Dictionary<string, object?> { [parameter] = JsonDocument.Parse(raw).RootElement.Clone() };
        var label = $"{tool}.{parameter} ({typeKey}) <- {raw}";

        /* Half one: the binder on its own. The row says what it does; this is what pins that. */
        var before = unguarded.ToolExceptions.Entries.Count;
        await unguarded.Client.CallToolAsync(tool, arguments, cancellationToken: cancellationToken);
        var bound = !unguarded.ToolExceptions.BindingFailureSince(before);
        Assert.True(
            bound != refused,
            refused
                ? $"{label}: the row says the binder cannot read this, but the call bound without a JSON error"
                : $"{label}: the row says the binder reads this, but it threw: " + unguarded.ToolExceptions.Describe());

        /* Half two: the guard in front of it. */
        var result = await guarded.Client.CallToolAsync(tool, arguments, cancellationToken: cancellationToken);
        var text = McpInProcessHost.TextOf(result);
        var refusal = McpInProcessHost.RefusalMessage(result);
        var guardRefused = refusal is not null && refusal.Contains("refused before it ran", StringComparison.Ordinal);

        if (!refused)
        {
            Assert.False(guardRefused, $"{label}: the guard refused a value the binder reads -> {text}");
            return;
        }

        Assert.True(guardRefused, $"{label}: the guard passed a value the binder cannot read, so the SDK answered -> {text}");

        foreach (var phrase in says.Split('|', StringSplitOptions.RemoveEmptyEntries))
        {
            var problem = McpInProcessHost.WholeNumberRefusalProblem(tool, parameter, phrase, result);
            Assert.True(problem is null, $"{label}: {problem}");
        }

        if (raw.Length <= 40)
        {
            Assert.True(refusal!.Contains(raw, StringComparison.Ordinal), $"{label}: the refusal does not quote the value -> {refusal}");
        }
    }

    /// <summary>The first tool with a parameter of the CLR type that the host advertises and that needs no other
    /// argument, so a call sending only that one argument reaches the binder for it alone. The walk takes the tool types
    /// in the order the caller gives them (both products' test lists sort them by full type name) and each type's
    /// methods by method name, not by tool name. A tool that changes data is passed over, by its name (<c>delete_</c>,
    /// <c>create_</c> or <c>update_</c>): a row that sends a value the binder reads runs the tool, and a row should not
    /// run a write. The name check covers those three prefixes only, so a write tool named otherwise
    /// (<c>add_servers</c>, <c>remove_server</c>, <c>set_mute_rule_enabled</c>, <c>mute_analysis_finding</c>) is not
    /// passed over. What keeps any tool body from reaching a real store is the test host's stand-in services, not the
    /// name check: Darling's data source points at a closed port, and Lite's services are uninitialized instances with
    /// every field null.</summary>
    private static (string Tool, string Parameter) Resolve(McpInProcessHost host, IEnumerable<Type> toolTypes, Type clr)
    {
        var served = host.RegisteredTools.ToDictionary(tool => tool.ProtocolTool.Name, StringComparer.Ordinal);

        foreach (var toolType in toolTypes)
        {
            foreach (var method in toolType
                         .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static)
                         .OrderBy(method => method.Name, StringComparer.Ordinal))
            {
                if (method.GetCustomAttribute<McpServerToolAttribute>() is not { } attribute
                    || !served.TryGetValue(attribute.Name ?? method.Name, out var tool)
                    || ChangesData(tool.ProtocolTool.Name))
                {
                    continue;
                }

                var schema = tool.ProtocolTool.InputSchema;
                var advertised = schema.TryGetProperty("properties", out var properties)
                    ? properties.EnumerateObject().Select(property => property.Name).ToHashSet(StringComparer.Ordinal)
                    : new HashSet<string>(StringComparer.Ordinal);
                var required = schema.TryGetProperty("required", out var requiredNames)
                    ? requiredNames.EnumerateArray().Select(name => name.GetString()!).ToHashSet(StringComparer.Ordinal)
                    : new HashSet<string>(StringComparer.Ordinal);

                foreach (var parameter in method.GetParameters().Where(p => p.ParameterType == clr && advertised.Contains(p.Name!)))
                {
                    if (required.All(name => name == parameter.Name))
                    {
                        return (tool.ProtocolTool.Name, parameter.Name!);
                    }
                }
            }
        }

        throw new InvalidOperationException($"No tool declares a parameter of type {clr}.");
    }

    private static bool ChangesData(string toolName) =>
        toolName.StartsWith("delete_", StringComparison.Ordinal)
        || toolName.StartsWith("create_", StringComparison.Ordinal)
        || toolName.StartsWith("update_", StringComparison.Ordinal);
}
