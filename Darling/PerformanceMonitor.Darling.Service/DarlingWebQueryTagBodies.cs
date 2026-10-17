/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// The JSON body readers behind the query-tag web routes (#5986). Each takes an allow-list of keys and the JSON type of
/// each, so a stray or mistyped field is a 400 naming it rather than a silently dropped one. The values are handed to the
/// same MCP cores the tools use, which own every rule (names, colours, depth, keys, patterns); nothing is validated twice.
/// </summary>
internal sealed class QueryTagBodyReader
{
    private readonly JsonObject _body;

    internal QueryTagBodyReader(JsonObject body, string what, params string[] allowed)
    {
        _body = body;
        var set = new HashSet<string>(allowed, StringComparer.Ordinal);
        foreach (var property in body)
        {
            if (!set.Contains(property.Key))
            {
                Error = $"Unknown field '{property.Key}'. {what} takes {string.Join(", ", allowed)}.";
                return;
            }
        }
    }

    /// <summary>The first problem found, or null when every field read so far was fine.</summary>
    internal string? Error { get; private set; }

    /// <summary>A string field (null when absent or JSON null).</summary>
    internal string? String(string key)
    {
        if (Error is not null || _body[key] is not JsonNode node)
        {
            return null;
        }

        if (node is JsonValue value && value.TryGetValue<string>(out var text))
        {
            return text;
        }

        Error = $"{key} must be a string.";
        return null;
    }

    /// <summary>A whole-number field (null when absent or JSON null).</summary>
    internal int? Int(string key)
    {
        if (Error is not null || _body[key] is not JsonNode node)
        {
            return null;
        }

        if (node is JsonValue value && value.TryGetValue<int>(out var number))
        {
            return number;
        }

        Error = $"{key} must be a whole number.";
        return null;
    }

    /// <summary>A 64-bit whole-number field (null when absent or JSON null).</summary>
    internal long? Long(string key)
    {
        if (Error is not null || _body[key] is not JsonNode node)
        {
            return null;
        }

        if (node is JsonValue value && value.TryGetValue<long>(out var number))
        {
            return number;
        }

        Error = $"{key} must be a whole number.";
        return null;
    }

    /// <summary>A true/false field (null when absent or JSON null).</summary>
    internal bool? Bool(string key)
    {
        if (Error is not null || _body[key] is not JsonNode node)
        {
            return null;
        }

        if (node is JsonValue value && value.TryGetValue<bool>(out var flag))
        {
            return flag;
        }

        Error = $"{key} must be true or false.";
        return null;
    }
}
