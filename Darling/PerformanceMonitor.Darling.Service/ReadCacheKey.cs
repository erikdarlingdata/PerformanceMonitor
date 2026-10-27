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
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5506 part 3: the keys of <see cref="ReadResultCache"/>. A key is the surface, the route, the canonical arguments,
/// the write generation, and then the <c>as_of</c> the caller sent or, with none, the TTL bucket
/// <c>floor(now / TTL)</c>. A key built with <c>heldFromStore</c> (#6800, the whole-fleet read of a route in
/// <see cref="ReadAdmission.FleetCachedRoutes"/>) carries no bucket, as an <c>as_of</c> key carries none: its entry is reachable
/// until its own TTL runs out from the moment it is stored (<see cref="ReadResultCache.TryStore"/>), whatever wall-clock bucket the
/// read started in. Two calls that would run the same read get the same key; anything that can change the answer
/// is in it. The surfaces never share a key (D4): the web passes other defaults, and a composed panel is keyed by the
/// surface that ran it.
/// </summary>
internal static class ReadCacheKey
{
    private const char Separator = '\u001f';

    /// <summary>
    /// The key of an MCP tool call. The arguments are sorted by name, an argument the caller left out takes the default
    /// the tool method declares (so leaving out <c>hours_back</c> and sending its default are one key), and a number is
    /// written in its shortest form (<c>24</c> and <c>24.0</c> are one key). A null the caller sent for a parameter with
    /// no default is the same as leaving it out. <paramref name="gcf"/> is the GCF output switch, which changes the text.
    /// </summary>
    internal static string ForMcp(string tool, IDictionary<string, JsonElement>? arguments, McpToolParameterTypes? types,
        bool gcf, long generation, ReadResultCache cache, TimeSpan ttl, bool heldFromStore = false)
    {
        var canonical = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (arguments is not null)
        {
            foreach (var (name, value) in arguments)
            {
                canonical[name] = Canonical(value);
            }
        }

        if (types is not null && types.TryGet(tool, out var parameters))
        {
            foreach (var name in parameters.Keys)
            {
                if (!canonical.ContainsKey(name) && types.TryGetDefault(tool, name, out var declared) && declared is not null)
                {
                    canonical[name] = Canonical(JsonSerializer.SerializeToElement(declared, declared.GetType()));
                }
            }
        }

        /* A null sent for a parameter with no default binds the same null as leaving it out. */
        foreach (var name in canonical.Where(p => p.Value == "null").Select(p => p.Key).ToList())
        {
            if (types is null || !types.TryGetDefault(tool, name, out _))
            {
                canonical.Remove(name);
            }
        }

        string? asOf = arguments is not null && arguments.TryGetValue("as_of", out var sent) && sent.ValueKind == JsonValueKind.String
            ? sent.GetString()
            : null;
        return Build("mcp", tool, canonical, generation, cache, ttl, asOf, gcf ? "gcf" : "json", heldFromStore);
    }

    /// <summary>The key of a web read: the query string as the browser sent it (before the read loop adds its own
    /// <c>as_of</c>), sorted by name. Values are kept as sent: the web refuses <c>?hours=24.0</c>, so it is not
    /// <c>?hours=24</c> there. Round 2, L4: each value of a repeated name comes as its own pair, and every name and value
    /// is escaped (<see cref="Uri.EscapeDataString(string)"/>), so the comma that joins a repeated name's values, the
    /// <c>=</c> and the separator never occur inside one: <c>?database=x&amp;database=y</c> (the handler reads <c>x</c>)
    /// and <c>?database=x%2Cy</c> are two keys.</summary>
    internal static string ForWeb(string route, IEnumerable<KeyValuePair<string, string?>> query, long generation, ReadResultCache cache, TimeSpan ttl,
        bool heldFromStore = false)
    {
        var canonical = new SortedDictionary<string, string>(StringComparer.Ordinal);
        string? asOf = null;
        foreach (var (rawName, rawValue) in query)
        {
            var name = Uri.EscapeDataString(rawName);
            var value = Uri.EscapeDataString(rawValue ?? string.Empty);
            canonical[name] = canonical.TryGetValue(name, out var earlier) ? earlier + "," + value : value;
            if (string.Equals(rawName, "as_of", StringComparison.Ordinal) && !string.IsNullOrEmpty(rawValue))
            {
                asOf = value;
            }
        }

        return Build("web", route, canonical, generation, cache, ttl, asOf, extra: null, heldFromStore);
    }

    /// <summary>The key of a composed-panel run: the surface that ran it (D4: <c>web</c> and <c>mcp</c> entries are never
    /// shared), the route, and the request body as sent.</summary>
    internal static string ForCompose(ReadSurface caller, string route, string body, long generation, ReadResultCache cache, TimeSpan ttl) =>
        Build("compose", route, new SortedDictionary<string, string>(StringComparer.Ordinal) { ["body"] = body },
            generation, cache, ttl, asOf: null, extra: caller == ReadSurface.Mcp ? "mcp" : "web");

    private static string Build(string surface, string route, SortedDictionary<string, string> arguments, long generation,
        ReadResultCache cache, TimeSpan ttl, string? asOf, string? extra, bool heldFromStore = false)
    {
        var key = new StringBuilder(128);
        key.Append(surface).Append(Separator).Append(route).Append(Separator);
        foreach (var (name, value) in arguments)
        {
            key.Append(name).Append('=').Append(value).Append(Separator);
        }

        key.Append("gen=").Append(generation.ToString(CultureInfo.InvariantCulture)).Append(Separator);
        /* #6800: a key held from its store time has no bucket. A bucket key is reachable only by requests that arrive inside the
           bucket the read started in, so a read that takes a large part of its TTL (the fleet tag read, 61 s or more cold against a
           5 minute TTL) would store an entry that lives for what is left of the bucket, or for nothing at all. */
        key.Append(!string.IsNullOrEmpty(asOf) ? "as_of=" + asOf
            : heldFromStore ? "held"
            : "bucket=" + cache.BucketOf(ttl).ToString(CultureInfo.InvariantCulture));
        if (extra is not null)
        {
            key.Append(Separator).Append(extra);
        }

        return key.ToString();
    }

    /// <summary>A JSON value in one form: a number in its shortest decimal form, anything else as its raw JSON.</summary>
    internal static string Canonical(JsonElement value)
    {
        if (value.ValueKind == JsonValueKind.Number)
        {
            return value.TryGetDecimal(out var number)
                ? number.ToString("0.############################", CultureInfo.InvariantCulture)
                : value.GetRawText();
        }

        return value.ValueKind is JsonValueKind.Undefined ? "null" : value.GetRawText();
    }
}
