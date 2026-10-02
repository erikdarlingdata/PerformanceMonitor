/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
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
/// <para><b>Case.</b> The binder matches parameter names EXACTLY, letter case included: a key that differs from
/// a real parameter by case alone is not bound, and the tool runs at that parameter's default. Measured on
/// ModelContextProtocol 2.2.0, on both registration paths: <c>{"HOURS_BACK": 2}</c> ran at the default 24 hours,
/// and the SDK JSON options' <c>PropertyNameCaseInsensitive</c> does not govern argument names. #3873 assumed the
/// opposite and matched names ignoring case, so from then until this change a miscased key was dropped with no
/// word, which is the very failure this guard exists to stop. The guard now matches exactly too, so it refuses
/// what the binder would drop and accepts what the binder binds. What the message adds is the near-miss: when a
/// rejected key differs from a real parameter only by case, or is within one small edit of one (the misspelling
/// case this exists for), the sentence names the candidate, because "did you mean hours_back" is the whole
/// remedy for the call that motivated the issue.</para>
///
/// <para><b>Integer values.</b> The same pass also reads the VALUE of each argument whose parameter is advertised
/// as an integer. The binder reads an integer parameter only from an integer literal or a string holding one, so
/// <c>hours_back: 0.5</c> (or <c>1.0</c>, <c>"0.5"</c>, <c>true</c>) threw inside the SDK before the tool ran,
/// and the caller got a bare "An error occurred invoking ..." with no word on which argument was wrong or why.
/// Such a call is now refused with a message that names the argument, says it takes a whole number (of hours,
/// for <c>hours_back</c>), quotes the value sent and lists the accepted parameters. For the integer types the
/// tools declare (<c>int</c>, <c>int?</c> and <c>long</c>), the check refuses only values that none of the three
/// can read, so it cannot refuse a call that would have worked.</para>
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

        /* A schema we cannot READ is a guard bug, not a caller bug, and refusing on one would turn a guard
           into a wall of false refusals across the whole surface — so an unreadable schema declines to
           judge. An EMPTY one is a different thing entirely: a tool that takes no parameters advertises a
           perfectly readable object schema with no properties, and every key sent to it is unknown. The
           census caught seven of those (list_servers, get_alert_settings, describe_custom_view_catalog and
           four more) sailing through an earlier version of this guard that read "no properties" as "cannot
           tell" — and a no-argument read is exactly where a stray filter key does the most damage, because
           the caller believes it narrowed a fleet-wide answer. */
        if (!TryReadParameters(tool, out var parameterSchemas))
        {
            return null;
        }

        var accepted = new HashSet<string>(parameterSchemas.Keys, StringComparer.Ordinal);

        var unknown = arguments.Keys
            .Where(key => !accepted.Contains(key))
            .OrderBy(key => key, StringComparer.Ordinal)
            .ToList();

        if (unknown.Count > 0)
        {
            return Envelope(tool.ProtocolTool.Name, unknown, accepted);
        }

        /* Every key is known; now the values of the integer parameters. The binder reads an integer
           parameter only from an integer literal or a string holding one, and anything else (0.5, 1.0, 1e1,
           "0.5", true) throws inside the SDK before the tool runs, which the SDK answers with a bare
           "An error occurred invoking ..." that names neither the argument nor the reason. */
        var notWhole = arguments
            .Where(argument => parameterSchemas.TryGetValue(argument.Key, out var schema)
                && IsIntegerParameter(schema)
                && !BinderReadsAsInteger(argument.Value))
            .OrderBy(argument => argument.Key, StringComparer.Ordinal)
            .ToList();

        return notWhole.Count == 0 ? null : WholeNumberEnvelope(tool.ProtocolTool.Name, notWhole, accepted);
    }

    /// <summary>
    /// The parameters a tool accepts, with each one's schema, read from the schema it ADVERTISES
    /// (<c>ProtocolTool.InputSchema</c>) rather than from reflection over the method. The advertised schema is
    /// what the caller was told, it is what the binder was built from, and it already excludes the DI-injected
    /// service parameters that no caller may send — so quoting it back is both the honest list and the correct
    /// one.
    ///
    /// <para>Ordinal, because the binder matches names exactly, per the type doc. Returns false only when the
    /// schema is not a readable object — an object with no <c>properties</c> is a readable schema for a tool
    /// that accepts nothing, and returns an empty set rather than a failure.</para>
    /// </summary>
    private static bool TryReadParameters(McpServerTool tool, out Dictionary<string, JsonElement> parameterSchemas)
    {
        parameterSchemas = new Dictionary<string, JsonElement>(StringComparer.Ordinal);

        var schema = tool.ProtocolTool.InputSchema;
        if (schema.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        if (schema.TryGetProperty("properties", out var properties)
            && properties.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in properties.EnumerateObject())
            {
                parameterSchemas[property.Name] = property.Value;
            }
        }

        return true;
    }

    /// <summary>
    /// Whether a parameter is advertised as an integer. The schema says <c>"integer"</c> for every <c>int</c>,
    /// <c>int?</c> and <c>long</c> parameter alike, so this cannot tell them apart, and the value check below is
    /// built to be right for all three.
    /// </summary>
    private static bool IsIntegerParameter(JsonElement parameterSchema)
    {
        if (parameterSchema.ValueKind != JsonValueKind.Object
            || !parameterSchema.TryGetProperty("type", out var type))
        {
            return false;
        }

        if (type.ValueKind == JsonValueKind.String)
        {
            return type.GetString() == "integer";
        }

        /* A type list counts only when "integer" is its one non-null type: with "number" or "string" beside it,
           a fraction or a word could be a value the parameter takes. */
        if (type.ValueKind != JsonValueKind.Array)
        {
            return false;
        }

        var types = type.EnumerateArray()
            .Where(entry => entry.ValueKind == JsonValueKind.String)
            .Select(entry => entry.GetString())
            .Where(name => name != "null")
            .ToList();

        return types.Count == 1 && types[0] == "integer";
    }

    /// <summary>
    /// Whether the binder could read <paramref name="value"/> into an integer parameter: an integer literal
    /// within <see cref="long"/>, or a string that holds one (the SDK reads numbers from strings). Null passes,
    /// because a nullable parameter takes it and the schema does not say which parameters are nullable.
    ///
    /// <para>This errs only toward passing. A value it passes that the binder still cannot read (above
    /// <see cref="int.MaxValue"/> for an <c>int</c>, or null for a parameter that is not nullable) gets the SDK's
    /// own error, as before. A value it refuses is one that <c>int</c>, <c>int?</c> and <c>long</c> parameters all
    /// fail to read; no tool declares an unsigned or smaller integer type.</para>
    /// </summary>
    private static bool BinderReadsAsInteger(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.Number => value.TryGetInt64(out _),
        JsonValueKind.String => long.TryParse(value.GetString(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out _),
        JsonValueKind.Null => true,
        _ => false,
    };

    /// <summary>
    /// "too large" or "too small" when <paramref name="value"/> is an integer literal that
    /// <see cref="BinderReadsAsInteger"/> refused, so it is past <see cref="long"/>; otherwise null. Decided by the
    /// value's own text: a JSON number with no '.', 'e' or 'E', or a string of digits with an optional sign. Failing
    /// to read as a long is not enough on its own, because 0.5 fails that too.
    /// </summary>
    private static string? OutOfRangeWord(JsonElement value)
    {
        var text = value.ValueKind switch
        {
            JsonValueKind.Number when value.GetRawText().AsSpan().IndexOfAny('.', 'e', 'E') < 0 => value.GetRawText(),
            JsonValueKind.String => value.GetString() ?? "",
            _ => null,
        };

        if (text is null)
        {
            return null;
        }

        var digits = text.AsSpan(text.StartsWith('-') || text.StartsWith('+') ? 1 : 0);
        if (digits.IsEmpty || digits.ContainsAnyExceptInRange('0', '9'))
        {
            return null;
        }

        return text.StartsWith('-') ? "too small" : "too large";
    }

    /// <summary>
    /// Builds the refusal for integer parameters given a value they cannot take. One sentence per argument, naming
    /// the argument, the unit its name gives (hours for <c>hours_back</c>) and the value that was sent, then the
    /// accepted parameters, as the unknown-argument refusal lists them. A whole number past <see cref="long"/> is
    /// called too large or too small (<see cref="OutOfRangeWord"/>), since asking for "no decimal point" would
    /// misstate what is wrong with it. <c>hints.parameter</c> is the first argument, the knob the caller must change.
    /// </summary>
    private static CallToolResult WholeNumberEnvelope(
        string toolName, List<KeyValuePair<string, JsonElement>> notWhole, HashSet<string> accepted)
    {
        var sentences = notWhole.Select(argument => OutOfRangeWord(argument.Value) is string reason
            ? $"Argument '{argument.Key}' for tool '{toolName}' takes {WholeNumberOf(argument.Key)},"
              + $" and the call sent {Shorten(argument.Value.GetRawText())}, which is {reason}."
            : $"Argument '{argument.Key}' for tool '{toolName}' takes {WholeNumberOf(argument.Key)} with no decimal point,"
              + $" such as 1, and the call sent {Shorten(argument.Value.GetRawText())}.");

        var message = string.Join(" ", sentences)
            + $" Accepted parameters: {string.Join(", ", accepted.OrderBy(name => name, StringComparer.Ordinal))}."
            + " The call was refused before it ran, because the tool cannot read "
            + (notWhole.Count == 1 ? "this value." : "these values.");

        return new CallToolResult
        {
            Content = new List<ContentBlock>
            {
                new TextContentBlock { Text = McpHelpers.Refusal(notWhole[0].Key, message) }
            },
            IsError = true,
        };
    }

    /// <summary>"a whole number of hours" for <c>hours_back</c>, from the unit word in the parameter's name, or
    /// "a whole number" when its name has none (<c>limit</c>, <c>top</c>).</summary>
    private static string WholeNumberOf(string parameter)
    {
        var unit = parameter
            .Split('_')
            .FirstOrDefault(segment => s_unitWords.Contains(segment));

        return unit is null ? "a whole number" : $"a whole number of {unit.ToLowerInvariant()}";
    }

    private static readonly HashSet<string> s_unitWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "hours", "days", "minutes", "seconds"
    };

    /// <summary>The value as it was sent, cut short so a long string cannot flood the message. The shared
    /// <see cref="McpHelpers.Truncate"/>, so the cut never splits a character.</summary>
    private static string Shorten(string rawValue) => McpHelpers.Truncate(rawValue, 40)!;

    /// <summary>
    /// Builds the refusal. The unknown key goes in <c>hints.parameter</c> — the house convention is that a
    /// refusal names the knob the caller must change, and here the knob IS the mistake.
    /// </summary>
    private static CallToolResult Envelope(string toolName, List<string> unknown, HashSet<string> accepted)
    {
        var named = string.Join(", ", unknown.Select(key => $"'{key}'"));

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

        /* A key that differs from a parameter only by letter case looks right to the caller, so say why it is
           unknown: the SDK binds a name only when it matches exactly. */
        if (unknown.Any(key => accepted.Any(name => string.Equals(name, key, StringComparison.OrdinalIgnoreCase))))
        {
            sentence += " Argument names must match exactly, including letter case.";
        }

        /* A no-parameter tool says so, rather than printing "Accepted parameters: ." — it is also the
           clearest possible correction, because the caller's whole argument object was the mistake. */
        sentence += accepted.Count == 0
            ? " This tool accepts no parameters."
            : $" Accepted parameters: {string.Join(", ", accepted.OrderBy(name => name, StringComparer.Ordinal))}.";

        sentence += " The call was refused rather than run without it, because an argument this tool does"
            + " not declare would have been dropped and the answer would have been to a different"
            + " question.";

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
        /* A key that differs only by letter case (HOURS_BACK) names its parameter outright, before any looser
           match below can name a different one first. */
        var sameLetters = accepted.FirstOrDefault(name => string.Equals(name, key, StringComparison.OrdinalIgnoreCase));
        if (sameLetters is not null)
        {
            return sameLetters;
        }

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
