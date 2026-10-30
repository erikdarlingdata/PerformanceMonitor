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
using System.Globalization;
using System.Linq;
using System.Reflection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;

namespace Darling.Tests;

/// <summary>
/// The one contract Lite's and Darling's investigation MCP tools both keep (#5820): per tool name, the parameters a caller
/// fills in (name, type, default, description) and the status words the description names. Each product's tool class is read
/// by REFLECTION and compared with this table in its own test project, so a drift in either class fails that product's
/// suite and the two classes cannot differ without this one table changing. The file is compiled into both test projects
/// (Lite.Tests links it), because neither can reference the other's assembly.
/// </summary>
internal static class InvestigationToolContract
{
    internal sealed record ToolParameter(string Name, string Type, string Default, string Description);

    internal sealed record ToolShape(string[] StatusWords, ToolParameter[] Parameters);

    private static ToolParameter P(string name, string type, string defaultValue, string description) =>
        new(name, type, defaultValue, description);

    private const string Required = "(required)";

    /* #6462: the submit entry takes its required parameter names from the list the brief's report_contract also reads, so the
       reflection tests that check this table against both products' real tool methods cover the brief too. Only the names alias;
       the types and descriptions below stay here. Declared before Tools: static initializers run in textual order. */
    private static readonly string[] SubmitRequired = InvestigationToolCore.SubmitReportRequired;

    /// <summary>The four tools both products serve; <c>wait_for_investigation_requests</c> is Darling's alone.</summary>
    internal static readonly IReadOnlyDictionary<string, ToolShape> Tools = new Dictionary<string, ToolShape>(StringComparer.Ordinal)
    {
        ["list_investigation_requests"] = new(
            new[] { "ok", "empty" },
            new[]
            {
                P("state", "string?", "null", "open, claimed, reported, closed or expired. Omit for all."),
                P("server_name", "string?", "null", "Server name. Omit for all."),
                P("family", "string?", "null", "Alert family (see the guide). Omit for all."),
                P("limit", "int", "20", "1-100. Default 20."),
            }),
        ["claim_investigation_request"] = new(
            new[] { "claimed", "renewed", "already_claimed", "not_open", "not_found", "unsupported_format" },
            new[]
            {
                P("request_id", "string", Required, "From list_investigation_requests."),
                P("agent_id", "string", Required, "Your agent name: 1-64 of A-Za-z0-9._:-"),
                P("claim_id", "string?", "null", "Your claim_id, to renew. Omit to claim."),
                P("brief_format", "int?", "null", "Omit, or 1."),
            }),
        ["submit_investigation_report"] = new(
            new[] { "reported", "invalid", "unsupported_format", "lease_lost", "already_reported", "not_found" },
            new[]
            {
                P(SubmitRequired[0], "string", Required, "The request you claimed."),
                P(SubmitRequired[1], "string", Required, "From claim_investigation_request."),
                P(SubmitRequired[2], "int", Required, "1."),
                P(SubmitRequired[3], "string", Required, "real, false_positive or needs_person."),
                P(SubmitRequired[4], "string", Required, "What caused the alert, or that you could not tell."),
                P(SubmitRequired[5], "string", Required, "JSON array of {tool, arguments, key_numbers, basis}; basis (" + InvestigationReportFields.BasesAlternation + ") is required on each item; see the guide."),
                P(SubmitRequired[6], "string", Required, "At most 4,000 characters."),
                P("proposed_note", "string?", "null", "Optional note for a person to approve, at most 2,000 characters."),
                P("action_proposals", "string?", "null", "Refused at Level 1. Omit."),
                P("links", "string?", "null", "Optional JSON array of at most 10 {kind, url, key}; https only. See the guide."),
                P("family_of", "string?", "null", "Optional. The request_id of an earlier request on this store; it need not have a report yet."),
                P("queries_run", "string?", "null", "Optional JSON array of statements you ran, " + InvestigationReportFields.QueriesRunRule + "."),
                P("follow_up_at", "string?", "null", "Optional. When to open a follow-up request: " + InvestigationReportFields.FollowUpRule + "."),
            }),
        ["get_investigation_reports"] = new(
            new[] { "ok", "empty" },
            new[]
            {
                P("server_name", "string?", "null", "Server name. Omit for all."),
                P("request_id", "string?", "null", "One request, with its brief. Omit for the window."),
                P("hours_back", "int", "24", "1-168. Default 24. Ignored with request_id."),
            }),
    };

    /// <summary>
    /// The parameters only Darling's tools take, after the shared ones: Darling's submit takes <c>follow_up_condition</c>
    /// (#6009); Lite's does not (Lite opens a time follow-up only, #6012). <c>links</c>, <c>family_of</c> and <c>queries_run</c>
    /// (#5914, #6004) and the basis on <c>evidence</c> are shared since #6011, and <c>follow_up_at</c> (#6008) since #6012. Darling's test passes this table to
    /// <see cref="Differences"/>; Lite's passes none.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, ToolParameter[]> DarlingOnlyParameters = new Dictionary<string, ToolParameter[]>(StringComparer.Ordinal)
    {
        ["submit_investigation_report"] = new[]
        {
            P("follow_up_condition", "string?", "null", "Optional. Open the follow-up when a module is deployed: " + InvestigationReportFields.FollowUpConditionRule + "."),
        },
    };

    /// <summary>Every <c>[McpServerTool]</c> method on <paramref name="toolClass"/>, by tool name.</summary>
    internal static IReadOnlyDictionary<string, MethodInfo> ToolMethods(Type toolClass) =>
        toolClass.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Select(m => (Method: m, Attribute: m.GetCustomAttribute<McpServerToolAttribute>()))
            .Where(x => x.Attribute?.Name is not null)
            .ToDictionary(x => x.Attribute!.Name!, x => x.Method, StringComparer.Ordinal);

    /// <summary>The parameters a caller fills in: the service-typed ones (the store, the composer, the server list) and the
    /// cancellation token are not part of the tool's wire schema.</summary>
    internal static IReadOnlyList<ToolParameter> Describe(MethodInfo method)
    {
        var nullability = new NullabilityInfoContext();
        var parameters = new List<ToolParameter>();
        foreach (var parameter in method.GetParameters())
        {
            var underlying = Nullable.GetUnderlyingType(parameter.ParameterType) ?? parameter.ParameterType;
            if (!(underlying.IsPrimitive || underlying == typeof(string)))
                continue;

            var nullable = Nullable.GetUnderlyingType(parameter.ParameterType) is not null
                           || (!parameter.ParameterType.IsValueType && nullability.Create(parameter).WriteState == NullabilityState.Nullable);
            var type = (underlying == typeof(string) ? "string" : underlying == typeof(int) ? "int" : underlying.Name) + (nullable ? "?" : "");
            var defaultValue = !parameter.HasDefaultValue
                ? Required
                : parameter.DefaultValue is null ? "null" : Convert.ToString(parameter.DefaultValue, CultureInfo.InvariantCulture)!;
            parameters.Add(new ToolParameter(parameter.Name!, type, defaultValue,
                parameter.GetCustomAttribute<DescriptionAttribute>()?.Description ?? ""));
        }

        return parameters;
    }

    /// <summary>The status words a description names: the sentence that starts at its first "status " and ends at the next
    /// full stop ("status claimed, renewed or not_found.").</summary>
    internal static IReadOnlyList<string> StatusWords(string description)
    {
        var start = description.IndexOf("status ", StringComparison.Ordinal);
        if (start < 0)
            return Array.Empty<string>();

        start += "status ".Length;
        var end = description.IndexOf('.', start);
        var sentence = end < 0 ? description[start..] : description[start..end];
        return sentence.Replace(" or ", ", ", StringComparison.Ordinal)
            .Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }

    /// <summary>Compares <paramref name="toolClass"/> with <see cref="Tools"/>, each tool's parameters followed by its
    /// <paramref name="productParameters"/>; returns every difference, empty when equal.</summary>
    internal static IReadOnlyList<string> Differences(
        Type toolClass, IReadOnlyDictionary<string, ToolParameter[]>? productParameters = null,
        IReadOnlyDictionary<string, string>? productDescriptions = null)
    {
        var differences = new List<string>();
        var methods = ToolMethods(toolClass);
        foreach (var (name, shape) in Tools)
        {
            if (!methods.TryGetValue(name, out var method))
            {
                differences.Add($"{name}: the tool is not declared");
                continue;
            }

            var actual = Describe(method);
            var shared = productDescriptions is null ? shape.Parameters
                : shape.Parameters.Select(p => productDescriptions.TryGetValue(name + "." + p.Name, out var text) ? p with { Description = text } : p).ToArray();
            var expected = productParameters is not null && productParameters.TryGetValue(name, out var extra)
                ? shared.Concat(extra).ToArray() : shared;
            if (!actual.SequenceEqual(expected))
            {
                differences.Add($"{name}: parameters are [{string.Join("; ", actual)}], the contract is [{string.Join("; ", (IEnumerable<ToolParameter>)expected)}]");
            }

            var description = method.GetCustomAttribute<DescriptionAttribute>()?.Description ?? "";
            var words = StatusWords(description);
            if (!words.SequenceEqual(shape.StatusWords))
            {
                differences.Add($"{name}: the description names status [{string.Join(", ", words)}], the contract is [{string.Join(", ", shape.StatusWords)}]");
            }
        }

        return differences;
    }
}
