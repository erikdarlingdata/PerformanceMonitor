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
/// #5307: finds the keys directly under <c>web</c> and under <c>web.network</c> and <c>mcp.network</c> (including their
/// <c>tls</c> blocks and <c>web.network.oidc</c>), in each <c>mcp.tokens</c> entry (#7023) and in each <c>servers</c> entry (#8239), that no config class declares, so the worker can name each one at start. A mistyped
/// key such as <c>ssl</c> for <c>tls</c> is otherwise dropped at load and the block behaves as if the setting had
/// never been written.
///
/// <para>The known keys are read from the config classes' own <c>[JsonPropertyName]</c> attributes, so the set
/// cannot drift from the code. A key that starts with <c>// </c> is a comment the sample files carry and is never
/// reported. The result is key PATHS only: the walk never copies a value anywhere, and the
/// list lives on an internal <c>[JsonIgnore]</c> property, so neither the log line nor a diagnostics bundle (which
/// serializes the whole config) can carry what a mistyped key held.</para>
/// </summary>
internal static class DarlingUnknownKeys
{
    /// <summary>The prefix of a comment key in darling.json, e.g. <c>"// tls": "..."</c>, which the loader ignores by design.</summary>
    internal const string CommentKeyPrefix = "// ";

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
            if (string.Equals(root.Name, "servers", StringComparison.OrdinalIgnoreCase) && root.Value.ValueKind == JsonValueKind.Array)
            {
                // #8239: each servers entry is walked against MonitoredServer. The registry is store-authoritative once seeded, so a
                // key it does not declare (displayName for name, say) is dropped for good unless the start names it.
                var index = 0;
                foreach (var entry in root.Value.EnumerateArray())
                {
                    Walk(entry, typeof(MonitoredServer), "servers[" + index++ + "]", found);
                }
            }
            else if (sectionType == typeof(WebConfig))
            {
                // The web section is walked whole (its direct keys and, through them, web.network), so a misspelled
                // "allowAdminWrite" is named at start rather than silently leaving the default (#6600).
                Walk(root.Value, typeof(WebConfig), "web", found);

                // #7735: each web.network.tokens entry is walked too, as each mcp.tokens entry is: a mistyped "rol" binds to nothing and
                // the entry is refused for having no role, so the start names the key the operator meant.
                if (TryGetIgnoreCase(root.Value, "network", out var webNetwork)
                    && TryGetIgnoreCase(webNetwork, "tokens", out var webTokens) && webTokens.ValueKind == JsonValueKind.Array)
                {
                    var index = 0;
                    foreach (var entry in webTokens.EnumerateArray())
                    {
                        Walk(entry, typeof(WebTokenEntry), "web.network.tokens[" + index++ + "]", found);
                    }
                }
            }
            else if (sectionType is not null)
            {
                if (TryGetIgnoreCase(root.Value, "network", out var network))
                {
                    var networkType = sectionType.GetProperty("Network")!.PropertyType;
                    Walk(network, Nullable.GetUnderlyingType(networkType) ?? networkType, "mcp.network", found);
                }

                // #7023: each mcp.tokens entry is walked too. A mistyped data_detail for dataDetail binds to nothing, and the entry would be
                // served the service's level, wider than the operator meant; every other entry mistake leaves the entry unserved.
                if (TryGetIgnoreCase(root.Value, "tokens", out var tokens) && tokens.ValueKind == JsonValueKind.Array)
                {
                    var index = 0;
                    foreach (var entry in tokens.EnumerateArray())
                    {
                        Walk(entry, typeof(McpTokenEntry), "mcp.tokens[" + index++ + "]", found);
                    }
                }
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
            if (property.Name.StartsWith(CommentKeyPrefix, StringComparison.Ordinal))
            {
                // A "// "-prefixed key is a note the sample files keep beside the setting it explains, so it is not a misspelling.
                continue;
            }

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
