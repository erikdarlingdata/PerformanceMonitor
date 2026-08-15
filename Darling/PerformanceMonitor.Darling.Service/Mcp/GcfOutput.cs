using System;
using System.Collections.Generic;
using System.Text.Json;
using BlackwellSystems.Gcf;

namespace PerformanceMonitor.Darling.Service.Mcp;

// Optional GCF (Graph Compact Format, https://gcformat.com) output for the MCP tool
// results. When DARLING_OUTPUT_FORMAT=gcf, a call-tool filter (GcfCallToolFilter)
// re-encodes each tool's JSON result as a GCF generic wire: the repeated field names of
// the record arrays these tools return (blocking events, alerts, wait stats, config, ...)
// are factored into a single header and the indentation is dropped, cutting the token
// cost of a result roughly in half versus the pretty-printed JSON. Opt-in, lossless, and
// never larger than the JSON.
public static class GcfOutput
{
    // True when GCF output is requested. Read from the environment on each call so it can
    // be toggled per process (or per test) without a restart.
    public static bool Enabled =>
        string.Equals(
            Environment.GetEnvironmentVariable("DARLING_OUTPUT_FORMAT")?.Trim(),
            "gcf",
            StringComparison.OrdinalIgnoreCase
        );

    // Returns a GCF wire for the given JSON, or null to keep the JSON. Null is returned
    // whenever the JSON does not parse, GCF is not smaller than the JSON (never-grow
    // guard), or the wire does not round-trip (fail-safe), so enabling GCF never grows or
    // garbles a tool result.
    public static string? TryEncode(string json)
    {
        if (string.IsNullOrEmpty(json))
            return null;

        object? native;
        try
        {
            using var doc = JsonDocument.Parse(json);
            native = FromJson(doc.RootElement);
        }
        catch
        {
            return null;
        }

        string wire;
        try
        {
            wire = Gcf.EncodeGeneric(native);
        }
        catch
        {
            return null;
        }

        // Never-grow guard: only offer GCF when it is actually smaller than the JSON the
        // tool would otherwise return.
        if (wire.Length >= json.Length)
            return null;

        // Fail-safe: require a stable round-trip (decode then re-encode reproduces the
        // wire). Combined with the int64-preserving decode below, this rejects any value
        // GCF cannot represent losslessly.
        try
        {
            if (Gcf.EncodeGeneric(Gcf.DecodeGeneric(wire)) != wire)
                return null;
        }
        catch
        {
            return null;
        }

        return wire;
    }

    // Converts a parsed JSON value into the gcf-dotnet native model (OrderedMap / List /
    // scalars), preserving object key order. Integers are kept as long rather than double
    // so large ids, counts, and durations are never float-rounded.
    private static object? FromJson(JsonElement e)
    {
        switch (e.ValueKind)
        {
            case JsonValueKind.Object:
                var map = new OrderedMap();
                foreach (var p in e.EnumerateObject())
                    map.Add(p.Name, FromJson(p.Value));
                return map;

            case JsonValueKind.Array:
                var list = new List<object?>();
                foreach (var item in e.EnumerateArray())
                    list.Add(FromJson(item));
                return list;

            case JsonValueKind.String:
                return e.GetString();

            case JsonValueKind.Number:
                if (e.TryGetInt64(out var l))
                    return l;
                return e.GetDouble();

            case JsonValueKind.True:
                return true;

            case JsonValueKind.False:
                return false;

            default:
                return null; // Null / Undefined
        }
    }
}
