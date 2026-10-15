/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// #4348: the JSON walk of the statement filter. It takes a whole tool result and hands every string in it to
/// the text judge or the XML judge, so the caller does not have to know which fields carry statements.
/// <see cref="JsonCore"/> holds the mechanics only. The two judges and the refusal sentence come in as
/// arguments, so the walk is tested with fake judges and the filter wires the real ones in.
/// </summary>
public static partial class SensitiveStatements
{
    /// <summary>A string value shorter than this cannot name a statement, so it is not handed to the text judge.</summary>
    private const int MinTextChars = 5;

    /// <summary>
    /// Walks <paramref name="output"/> and returns it with every statement-naming string replaced by what the
    /// judges return.
    /// <list type="bullet">
    /// <item>A string value whose first non-space character is <c>&lt;</c> goes to <paramref name="xml"/>; any
    /// other value of 5 or more characters goes to <paramref name="text"/>.</item>
    /// <item>A key is judged with <paramref name="text"/>. A key the judge replaces becomes the replacement plus
    /// <c>#n</c> (n = the key's 0-based position in its object), so the keys of one object stay unique.</item>
    /// <item>When nothing changed, the ORIGINAL string instance comes back. When something changed, the tree is
    /// re-serialized with <see cref="McpHelpers.JsonOptions"/>.</item>
    /// <item>Output that is not JSON (raw plan XML, a plain-text message) is judged whole: first non-space
    /// <c>&lt;</c> to <paramref name="xml"/>, else <paramref name="text"/>.</item>
    /// <item>Any exception, from a judge or from the walk, returns <paramref name="refusal"/>, never the
    /// input.</item>
    /// </list>
    /// </summary>
    internal static string JsonCore(string output, Func<string?, string?> text, Func<string?, string?> xml, string refusal) =>
        JsonCore(
            output,
            // A value shorter than MinTextChars cannot name a statement, so the statement filter does not judge it.
            (key, value) => key is not null && value is { Length: < MinTextChars } ? value : text(value),
            (_, value) => xml(value),
            text,
            refusal);

    /// <summary>
    /// The keyed form of the walk above (#5632): each value judge also receives the value's key, so a judge can
    /// decide by field as well as by shape.
    /// <list type="bullet">
    /// <item>The key is the property the value sits under; an array element takes its array's key; a bare string
    /// root takes the empty key, and a whole non-JSON
    /// output takes null.</item>
    /// <item>A <c>value</c> beside a string <c>label</c> (an alert context field, <c>{"label": "Query", "value":
    /// ...}</c>) takes the label as its key, because the label is what says what the value holds.</item>
    /// <item>Every string value reaches a judge, however short (a login can be two characters); the unkeyed form
    /// keeps its own 5-character floor for values inside a document.</item>
    /// <item>A key of 5 or more characters is judged by <paramref name="keyText"/> and renamed as the unkeyed form
    /// renames it.</item>
    /// </list>
    /// Everything else (the unchanged instance, the refusal on any exception) is the unkeyed form's.
    /// </summary>
    internal static string JsonCore(
        string output,
        Func<string?, string?, string?> text,
        Func<string?, string?, string?> xml,
        Func<string?, string?> keyText,
        string refusal)
    {
        try
        {
            if (string.IsNullOrEmpty(output)) return output;

            if (StartsWithAngle(output)) return Whole(output, text, xml, refusal);

            JsonNode? root;
            try
            {
                root = JsonNode.Parse(output);
            }
            catch (JsonException)
            {
                // Output that opens like JSON and does not parse (cut, or nested past the parser's depth) would be
                // judged as its escaped text, which the judge's patterns do not match: refuse it instead.
                if (StartsWithJsonOpener(output)) return refusal;
                return Whole(output, text, xml, refusal);
            }

            if (root is null) return output;

            bool changed = false;
            JsonNode? walked = Walk(root, null, text, xml, keyText, ref changed);
            if (!changed) return output;
            return walked is null ? "null" : walked.ToJsonString(McpHelpers.JsonOptions);
        }
        catch (Exception)
        {
            return refusal;
        }
    }

    /// <summary>Judges a non-JSON output as one piece. A judge that returns null for a non-empty piece fails closed.</summary>
    private static string Whole(
        string output, Func<string?, string?, string?> text, Func<string?, string?, string?> xml, string refusal)
    {
        string? judged = StartsWithAngle(output) ? xml(null, output) : text(null, output);
        return judged ?? refusal;
    }

    /// <summary>True when the first non-space character of <paramref name="s"/> is <c>{</c> or <c>[</c>.</summary>
    private static bool StartsWithJsonOpener(string s)
    {
        foreach (char c in s)
        {
            if (char.IsWhiteSpace(c)) continue;
            return c == '{' || c == '[';
        }
        return false;
    }

    /// <summary>True when the first non-space character of <paramref name="s"/> is <c>&lt;</c>.</summary>
    private static bool StartsWithAngle(string s)
    {
        foreach (char c in s)
        {
            if (char.IsWhiteSpace(c)) continue;
            return c == '<';
        }
        return false;
    }

    /// <summary>
    /// Returns the node to keep in <paramref name="node"/>'s place: the node itself (its children may have been
    /// replaced in place) or a fresh string value. <paramref name="changed"/> turns true when anything differs.
    /// </summary>
    private static JsonNode? Walk(
        JsonNode? node, string? key, Func<string?, string?, string?> text, Func<string?, string?, string?> xml,
        Func<string?, string?> keyText, ref bool changed)
    {
        switch (node)
        {
            case JsonObject obj:
                WalkObject(obj, text, xml, keyText, ref changed);
                return obj;
            case JsonArray arr:
                for (int i = 0; i < arr.Count; i++)
                {
                    JsonNode? child = arr[i];
                    bool childChanged = false;
                    JsonNode? replaced = Walk(child, key, text, xml, keyText, ref childChanged);
                    if (!childChanged) continue;
                    changed = true;
                    if (ReferenceEquals(replaced, child)) continue;
                    arr[i] = replaced;
                }
                return arr;
            case JsonValue val:
                if (val.GetValueKind() != JsonValueKind.String) return val;
                string s = val.GetValue<string>();
                string? judged = JudgeValue(key, s, text, xml);
                if (string.Equals(judged, s, StringComparison.Ordinal)) return val;
                changed = true;
                return judged is null ? null : JsonValue.Create(judged);
            default:
                return node;
        }
    }

    private static void WalkObject(
        JsonObject obj, Func<string?, string?, string?> text, Func<string?, string?, string?> xml,
        Func<string?, string?> keyText, ref bool changed)
    {
        // An alert context field ({"label": ..., "value": ...}): the label says what the value holds.
        string? label = obj.TryGetPropertyValue("label", out JsonNode? labelNode)
            && labelNode is JsonValue labelValue && labelValue.GetValueKind() == JsonValueKind.String
            ? labelValue.GetValue<string>()
            : null;

        var pairs = new List<KeyValuePair<string, JsonNode?>>(obj.Count);
        foreach (KeyValuePair<string, JsonNode?> pair in obj) pairs.Add(pair);

        bool objectChanged = false;
        var used = new HashSet<string>(StringComparer.Ordinal);
        var rebuilt = new List<KeyValuePair<string, JsonNode?>>(pairs.Count);
        for (int n = 0; n < pairs.Count; n++)
        {
            string key = pairs[n].Key;
            JsonNode? value = pairs[n].Value;

            string newKey = key;
            if (key.Length >= MinTextChars)
            {
                string? judgedKey = keyText(key);
                if (judgedKey is null || !string.Equals(judgedKey, key, StringComparison.Ordinal))
                {
                    newKey = (judgedKey ?? string.Empty) + "#" + n.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    objectChanged = true;
                }
            }
            // A kept key could in theory equal an earlier renamed one; the position suffix makes that
            // vanishingly rare, and this keeps the object's keys unique if it ever happens.
            while (!used.Add(newKey))
            {
                newKey += "_";
                objectChanged = true;
            }

            bool childChanged = false;
            string valueKey = label is not null && string.Equals(key, "value", StringComparison.Ordinal) ? label : key;
            JsonNode? newValue = Walk(value, valueKey, text, xml, keyText, ref childChanged);
            if (childChanged) objectChanged = true;
            rebuilt.Add(new KeyValuePair<string, JsonNode?>(newKey, newValue));
        }

        if (!objectChanged) return;
        changed = true;

        // Detach the children, then add them back under their (possibly renamed) keys in the same order.
        obj.Clear();
        foreach (KeyValuePair<string, JsonNode?> pair in rebuilt)
        {
            JsonNode? value = pair.Value;
            if (value?.Parent != null) value = value.DeepClone();
            obj.Add(pair.Key, value);
        }
    }

    private static string? JudgeValue(
        string? key, string s, Func<string?, string?, string?> text, Func<string?, string?, string?> xml)
    {
        if (StartsWithAngle(s)) return xml(key, s);
        return text(key ?? string.Empty, s);
    }
}
