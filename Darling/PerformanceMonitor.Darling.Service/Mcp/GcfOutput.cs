using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;
using BlackwellSystems.Gcf;

namespace PerformanceMonitor.Darling.Service.Mcp;

// Optional GCF (Graph Compact Format, https://gcformat.com) output for the MCP tool
// results. When DARLING_OUTPUT_FORMAT=gcf, a call-tool filter (GcfCallToolFilter)
// re-encodes each tool's JSON result as a GCF generic wire: the repeated field names of
// the record arrays these tools return (blocking events, alerts, wait stats, config, ...)
// are factored into a single header, cutting the token cost of a record-heavy result by
// roughly a quarter to a half of the server's compact JSON depending on shape (uniform
// numeric records win most; mostly-text results win least). Opt-in, lossless, and never
// larger than the JSON.
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
    // whenever the JSON does not parse, contains a number GCF cannot carry exactly (a
    // non-integer beyond double precision, e.g. a high-precision decimal), GCF is not
    // smaller than the JSON (never-grow guard), or the decoded wire does not equal the input
    // (fail-safe), so enabling GCF never grows, drops, or garbles a tool result.
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

        // Fail-safe: verify the wire against the INPUT, not against itself. Decode the wire
        // back to a value and require it to equal the model the tool's JSON parsed to
        // (`native`). FromJson has already declined any number the wire could not carry
        // exactly, so a match here means the JSON survives the full JSON -> GCF -> value
        // round-trip. Object key order may normalize to header order (semantically equal for
        // JSON objects), so the key comparison is order-insensitive.
        try
        {
            if (!ValuesEqual(Gcf.DecodeGeneric(wire), native))
                return null;
        }
        catch
        {
            return null;
        }

        return wire;
    }

    // Order-insensitive structural equality over the gcf-dotnet model (OrderedMap / List /
    // long / double / string / bool / null), used to confirm a decoded wire equals the
    // input model.
    private static bool ValuesEqual(object? a, object? b)
    {
        if (a is null || b is null)
            return a is null && b is null;

        if (a is OrderedMap ma && b is OrderedMap mb)
        {
            if (ma.Count != mb.Count)
                return false;
            foreach (var key in ma.Keys)
            {
                if (!mb.TryGetValue(key, out var vb) || !ValuesEqual(ma[key], vb))
                    return false;
            }
            return true;
        }

        if (a is List<object?> la && b is List<object?> lb)
        {
            if (la.Count != lb.Count)
                return false;
            for (var i = 0; i < la.Count; i++)
                if (!ValuesEqual(la[i], lb[i]))
                    return false;
            return true;
        }

        if (a is string sa && b is string sb)
            return sa == sb;
        if (a is bool ba && b is bool bb)
            return ba == bb;
        if (IsNumber(a) && IsNumber(b))
            return NumbersEqual(a!, b!);
        return false;
    }

    private static bool IsNumber(object? v) => v is long || v is double;

    // long/long compare exactly; a long and an integer-valued double (an integer can decode
    // as either) compare by value. Precision-lossy numbers never reach here: FromJson
    // declined them before the wire was produced; the mixed branch still avoids widening the
    // long to double so the guard cannot itself launder a value above 2^53.
    private static bool NumbersEqual(object a, object b)
    {
        if (a is long al && b is long bl)
            return al == bl;
        if (a is double ad && b is double bd)
            return ad.Equals(bd);

        // Mixed long/double. Compare without casting the long to double (that cast rounds
        // above 2^53 and would let a lost value compare equal). The two are equal only when
        // the double is integral, sits inside the long range, and equals the long exactly.
        long lng;
        double dbl;
        if (a is long la)
        {
            lng = la;
            dbl = (double)b;
        }
        else
        {
            lng = (long)b;
            dbl = (double)a;
        }
        return dbl == Math.Floor(dbl)
            && dbl >= long.MinValue
            && dbl <= long.MaxValue
            && (long)dbl == lng;
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
                // A non-integer is carried on the wire as an IEEE-754 double (SPEC 2.3.2).
                // Keep it only when the double holds the JSON token exactly; otherwise
                // decline the whole payload (this throw is caught in TryEncode and the tool
                // result stays JSON) rather than emit a wire that has silently dropped
                // precision. A token outside the decimal range is inherently double-domain.
                var d = e.GetDouble();
                if (!NumberSurvivesAsDouble(e, d))
                    throw new NotSupportedException("number not exactly representable as a double");
                return d;

            case JsonValueKind.True:
                return true;

            case JsonValueKind.False:
                return false;

            default:
                return null; // Null / Undefined
        }
    }

    // Reports whether a double holds the JSON number token exactly, so a non-integer can
    // be carried on the wire without silently dropping precision. A non-finite double (a
    // token that overflowed to +/-Infinity, e.g. 1e400) never represents a finite token and
    // is declined. Otherwise the double is exact when its shortest round-trip form
    // reproduces the token, or, for a token in the decimal range, when the decimal it parsed
    // to is unchanged. The shortest round-trip comparison is what makes a 16-significant-digit
    // value such as 0.5029000043869019 (exactly representable, but 16 digits) survive: a
    // (decimal)d cast keeps only 15 digits and would wrongly reject it.
    private static bool NumberSurvivesAsDouble(JsonElement e, double d)
    {
        if (!double.IsFinite(d))
            return false;
        if (d.ToString("R", CultureInfo.InvariantCulture)
            .Equals(e.GetRawText(), StringComparison.Ordinal))
            return true;
        return e.TryGetDecimal(out var exact) && (decimal)d == exact;
    }
}
