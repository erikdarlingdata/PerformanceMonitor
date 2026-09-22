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
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// The pre-dispatch guard that REFUSES a tool call carrying an argument the tool does not declare
/// (#3870), on both SKUs, from one implementation.
///
/// <para><b>What was wrong.</b> The MCP SDK binds a call's <c>arguments</c> object to the tool method's
/// parameters by NAME, and a key that matches no parameter is simply not bound — <c>JsonSerializer</c>
/// semantics, no diagnostic, no trace. So <c>get_collection_log {"hours":1,"status_filter":"failure"}</c>
/// returned two hundred rows, every one SUCCESS, at the twenty-four-hour default: <c>status_filter</c> does
/// not exist and <c>hours</c> is spelled <c>hours_back</c>, so BOTH knobs the caller set were dropped and
/// the tool answered a question nobody asked. <b>For a surface whose callers are language models that is the
/// worst failure shape there is</b> — the caller believes it asked something narrower than it did and reads
/// the answer under that belief, with no tell anywhere in the payload. The codebase already holds the
/// principle one level down, on <c>limit</c>: "rejects out of range rather than silently clamping, so a
/// caller asking for 5000 is told no instead of quietly given 1000." A wrong VALUE was refused and a wrong
/// NAME was swallowed, and the misspelled name is by far the likelier mistake from an LLM.
///
/// <para><b>Why here and not in the tools.</b> The SDK owns deserialization, so there is nothing to fix
/// inside a tool body — by the time a method runs, the unknown key is already gone. The choke point is a
/// validation pass over the RAW argument keys against the tool's own declared input schema, BEFORE dispatch:
/// a call-tool filter, registered once per host, covering every registered tool with no per-tool change —
/// the same seam and the same "registered once; covers every tool" argument as
/// <c>GcfCallToolFilter</c>. Two hosts register it; one implementation decides it, so the two SKUs cannot
/// drift into refusing differently, and a tool added tomorrow is covered the day it is registered.</para>
///
/// <para><b>What it refuses, and what it deliberately does not.</b> Only a key that is NOT a property of the
/// tool's advertised schema. An OMITTED optional parameter is not an unknown key and never was — the guard
/// reads the keys that arrived, never the ones that did not, so optionality and null handling are untouched.
/// A declared parameter that some code path ignores is likewise not unknown: the schema is the contract, not
/// the reachability of a branch. Protocol metadata (<c>_meta</c>, the progress token) travels in
/// <c>CallToolRequestParams</c> SIBLINGS of <c>Arguments</c>, not inside it, so this reads only the
/// tool-arguments object and cannot refuse a client's protocol furniture; the <c>_</c>-prefix carve-out the
/// issue floated is unnecessary for that reason, and absent a measured need plain strict is better.</para>
///
/// <para><b>Case.</b> The binder matches parameter names case-INSENSITIVELY, so the guard accepts a key the
/// binder would have bound and refuses only what the binder would have dropped — the guard and the binder
/// agree by construction, which is the only way this can never refuse a working call. A key that differs
/// from a real parameter by case alone therefore still WORKS. What the message adds is the near-miss: when a
/// rejected key is within one small edit of a real parameter (the misspelling case this exists for) the
/// sentence names the candidate, because "did you mean hours_back" is the whole remedy for the call that
/// motivated the issue.</para>
///
/// <para><b>The shape.</b> <see cref="McpHelpers.Refusal"/>, the house's one refusal envelope
/// (<c>status</c> = <c>invalid</c>, <c>hints.parameter</c> = the offending key): the request as given cannot
/// be served, which is exactly what that word means (#3739). The message names the unknown key AND lists the
/// tool's accepted parameters, so a model can correct itself in one turn instead of asking what it may
/// send.</para>
/// </summary>
public static class McpUnknownArgumentGuard
{
    /// <summary>
    /// The filter: validate the raw argument keys, and only then run the tool. A filter is
    /// <c>next =&gt; handler</c>, so refusing means never calling <paramref name="next"/> — the read does not
    /// happen, which is the point (a call with a dropped knob must not reach the database and come back
    /// looking authoritative).
    ///
    /// <para>Registered per host beside the GCF filter. Order does not matter between the two: this one
    /// short-circuits before dispatch, that one re-encodes a result after it.</para>
    /// </summary>
    public static McpRequestFilter<CallToolRequestParams, CallToolResult> Instance =>
        next => async (request, cancellationToken) =>
            Refuse(request.Params, FindTool(request)) ?? await next(request, cancellationToken);

    /// <summary>
    /// The refusal, or null when the call is clean. Split from <see cref="Instance"/> so the decision is
    /// testable without a running server: a test hands it the params and the tool and reads the envelope.
    /// </summary>
    /// <param name="parameters">The call's parameters; only <see cref="CallToolRequestParams.Arguments"/> is read.</param>
    /// <param name="tool">The tool being called, for its advertised input schema. Null (a name the server does not know) is left to the SDK, which owns that error.</param>
    public static CallToolResult? Refuse(CallToolRequestParams? parameters, McpServerTool? tool)
    {
        if (parameters?.Arguments is not { Count: > 0 } arguments || tool is null)
        {
            return null;
        }

        var accepted = AcceptedParameters(tool);

        /* A tool that advertises no properties at all is not evidence that everything is unknown: the SDK's
           default schema for a no-parameter tool is the bare {"type":"object"}, and a schema we could not
           read is a guard bug, not a caller bug. Refusing on an empty accepted set would turn either into a
           wall of false refusals across the whole surface, so an unreadable schema declines to judge. */
        if (accepted.Count == 0)
        {
            return null;
        }

        var unknown = arguments.Keys
            .Where(key => !accepted.Contains(key))
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToList();

        if (unknown.Count == 0)
        {
            return null;
        }

        return Envelope(tool.ProtocolTool.Name, unknown, accepted);
    }

    /// <summary>
    /// The parameter names a tool accepts, read from the schema it ADVERTISES (<c>ProtocolTool.InputSchema</c>)
    /// rather than from reflection over the method. The advertised schema is what the caller was told, it is
    /// what the binder was built from, and it already excludes the DI-injected service parameters that no
    /// caller may send — so quoting it back is both the honest list and the correct one.
    ///
    /// <para>Ordinal-ignore-case because that is the binder's own matching, per the type doc.</para>
    /// </summary>
    private static HashSet<string> AcceptedParameters(McpServerTool tool)
    {
        var accepted = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        var schema = tool.ProtocolTool.InputSchema;
        if (schema.ValueKind == JsonValueKind.Object
            && schema.TryGetProperty("properties", out var properties)
            && properties.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in properties.EnumerateObject())
            {
                accepted.Add(property.Name);
            }
        }

        return accepted;
    }

    /// <summary>
    /// Builds the refusal. The unknown key goes in <c>hints.parameter</c> — the house convention is that a
    /// refusal names the knob the caller must change, and here the knob IS the mistake.
    /// </summary>
    private static CallToolResult Envelope(string toolName, List<string> unknown, HashSet<string> accepted)
    {
        var named = string.Join(", ", unknown.Select(key => $"'{key}'"));
        var acceptedList = string.Join(", ", accepted.OrderBy(name => name, StringComparer.Ordinal));

        var sentence = unknown.Count == 1
            ? $"Unknown argument {named} for tool '{toolName}'."
            : $"Unknown arguments {named} for tool '{toolName}'.";

        /* The near-miss, when there is one. This is the case the issue was filed over — hours for
           hours_back — and naming the candidate turns a refusal into a correction. */
        var suggestions = unknown
            .Select(key => new { Key = key, Match = NearestMatch(key, accepted) })
            .Where(pair => pair.Match is not null)
            .Select(pair => $"'{pair.Key}' -> '{pair.Match}'")
            .ToList();

        if (suggestions.Count > 0)
        {
            sentence += $" Did you mean {string.Join(", ", suggestions)}?";
        }

        sentence += $" Accepted parameters: {acceptedList}."
            + " The call was refused rather than run without it, because an argument this tool does not"
            + " declare would have been dropped and the answer would have been to a different question.";

        return new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock { Text = McpHelpers.Refusal(unknown[0], sentence) }
            },
            IsError = true,
        };
    }

    /// <summary>
    /// The accepted parameter within one edit of a rejected key, or null. Deliberately narrow: an edit
    /// distance of one catches the real agent typo (a dropped or doubled character, a transposition, a
    /// single wrong letter) and a shared prefix catches the truncation that motivated the issue
    /// (<c>hours</c> for <c>hours_back</c>), while a wider net would confidently propose a parameter the
    /// caller never meant. When nothing is close the message simply lists what is accepted.
    /// </summary>
    private static string? NearestMatch(string key, HashSet<string> accepted)
    {
        foreach (var candidate in accepted.OrderBy(name => name, StringComparer.Ordinal))
        {
            /* The truncation case: the caller sent a real parameter's prefix (hours for hours_back). Bounded
               at three characters so a one-letter key does not "match" half the schema. */
            if (key.Length >= 3 && candidate.StartsWith(key, StringComparison.OrdinalIgnoreCase))
            {
                return candidate;
            }

            if (WithinOneEdit(key, candidate))
            {
                return candidate;
            }
        }

        return null;
    }

    /// <summary>
    /// Whether two names differ by at most one insertion, deletion, substitution, or adjacent transposition.
    /// A hand-rolled bounded check rather than a full edit-distance matrix: the only question asked is
    /// "within one", the inputs are parameter names, and this runs once per rejected key on a refusal path.
    /// </summary>
    private static bool WithinOneEdit(string left, string right)
    {
        if (Math.Abs(left.Length - right.Length) > 1)
        {
            return false;
        }

        if (left.Length == right.Length)
        {
            var differences = 0;
            var firstDifference = -1;

            for (var index = 0; index < left.Length; index++)
            {
                if (char.ToLowerInvariant(left[index]) != char.ToLowerInvariant(right[index]))
                {
                    if (++differences > 2)
                    {
                        return false;
                    }

                    if (firstDifference < 0)
                    {
                        firstDifference = index;
                    }
                }
            }

            if (differences <= 1)
            {
                return true;
            }

            /* Two differences are within one edit only as an adjacent transposition (hours_bcak). */
            return differences == 2
                && firstDifference + 1 < left.Length
                && char.ToLowerInvariant(left[firstDifference]) == char.ToLowerInvariant(right[firstDifference + 1])
                && char.ToLowerInvariant(left[firstDifference + 1]) == char.ToLowerInvariant(right[firstDifference]);
        }

        /* One is longer: walk both, allowing a single skip in the longer one. */
        var shorter = left.Length < right.Length ? left : right;
        var longer = left.Length < right.Length ? right : left;

        var shortIndex = 0;
        var longIndex = 0;
        var skipped = false;

        while (shortIndex < shorter.Length && longIndex < longer.Length)
        {
            if (char.ToLowerInvariant(shorter[shortIndex]) == char.ToLowerInvariant(longer[longIndex]))
            {
                shortIndex++;
                longIndex++;
                continue;
            }

            if (skipped)
            {
                return false;
            }

            skipped = true;
            longIndex++;
        }

        return true;
    }

    /// <summary>
    /// The tool the request names, from the server's own registered collection — the same list
    /// <c>tools/list</c> is built from, so the schema the guard validates against is byte-for-byte the schema
    /// the caller was shown. Null when the server does not know the name; the SDK owns that error and this
    /// guard stays out of it.
    /// </summary>
    private static McpServerTool? FindTool(RequestContext<CallToolRequestParams> request)
    {
        var name = request.Params?.Name;
        if (string.IsNullOrEmpty(name))
        {
            return null;
        }

        var tools = request.Server?.ServerOptions?.ToolCollection;
        if (tools is null)
        {
            return null;
        }

        return tools.TryGetPrimitive(name!, out var tool) ? tool : null;
    }
}
