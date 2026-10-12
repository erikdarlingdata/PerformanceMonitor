/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5307: finds the keys under <c>web.network</c> and <c>mcp.network</c> (including their <c>tls</c> blocks and
/// <c>web.network.oidc</c>) that no config class declares, so the worker can name each one at start. A mistyped
/// key such as <c>ssl</c> for <c>tls</c> is otherwise dropped at load and the block behaves as if the setting had
/// never been written.
///
/// <para>The known keys are read from the config classes' own <c>[JsonPropertyName]</c> attributes, so the set
/// cannot drift from the code. The result is key PATHS only: the walk never copies a value anywhere, and the
/// list lives on an internal <c>[JsonIgnore]</c> property, so neither the log line nor a diagnostics bundle (which
/// serializes the whole config) can carry what a mistyped key held.</para>
/// </summary>
internal static class DarlingUnknownKeys
{
    private static readonly Dictionary<Type, IReadOnlyDictionary<string, PropertyInfo>> s_known = new();

    /// <summary>The unknown key paths in the raw darling.json text, in file order, e.g. <c>mcp.network.ssl</c>.</summary>
    public static List<string> Find(string json)
    {
        var found = new List<string>();
        var options = new JsonDocumentOptions { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true };
        using var doc = JsonDocument.Parse(json, options);

        foreach (var root in doc.RootElement.ValueKind == JsonValueKind.Object ? doc.RootElement.EnumerateObject() : default)
        {
            var sectionType = root.Name.ToLowerInvariant() switch { "web" => typeof(WebConfig), "mcp" => typeof(McpConfig), _ => null };
            if (sectionType is not null && TryGetIgnoreCase(root.Value, "network", out var network))
            {
                var networkType = sectionType.GetProperty("Network")!.PropertyType;
                Walk(network, Nullable.GetUnderlyingType(networkType) ?? networkType, sectionType == typeof(WebConfig) ? "web.network" : "mcp.network", found);
            }
        }

        return found;
    }

    private static void Walk(JsonElement element, Type type, string path, List<string> found)
    {
        if (element.ValueKind != JsonValueKind.Object)
        {
            return;
        }

        var known = KnownKeys(type);
        foreach (var property in element.EnumerateObject())
        {
            if (!known.TryGetValue(property.Name, out var member))
            {
                found.Add(path + "." + property.Name);
                continue;
            }

            var memberType = Nullable.GetUnderlyingType(member.PropertyType) ?? member.PropertyType;
            if (IsConfigObject(memberType))
            {
                Walk(property.Value, memberType, path + "." + member.GetCustomAttribute<JsonPropertyNameAttribute>()!.Name, found);
            }
        }
    }

    /// <summary>The JSON names (case-insensitive, like the loader) a config class binds.</summary>
    internal static IReadOnlyDictionary<string, PropertyInfo> KnownKeys(Type type)
    {
        lock (s_known)
        {
            if (!s_known.TryGetValue(type, out var map))
            {
                map = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.GetCustomAttribute<JsonIgnoreAttribute>() is null)
                    .Where(p => p.GetCustomAttribute<JsonPropertyNameAttribute>() is not null)
                    .ToDictionary(p => p.GetCustomAttribute<JsonPropertyNameAttribute>()!.Name, p => p, StringComparer.OrdinalIgnoreCase);
                s_known[type] = map;
            }

            return map;
        }
    }

    private static bool IsConfigObject(Type type) =>
        type.IsClass && type != typeof(string) && !type.IsArray && type.Assembly == typeof(DarlingConfig).Assembly;

    private static bool TryGetIgnoreCase(JsonElement element, string name, out JsonElement value)
    {
        value = default;
        if (element.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        foreach (var property in element.EnumerateObject())
        {
            if (string.Equals(property.Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = property.Value;
                return true;
            }
        }

        return false;
    }
}
