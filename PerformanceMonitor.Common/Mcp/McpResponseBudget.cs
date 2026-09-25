/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// The shared response-size budget every MCP read tool is subject to (#4198), applied ONCE at the call-tool
/// filter layer (<c>McpResponseBudgetCallToolFilter</c>) rather than tuned per tool. #4198 measured 12 tools
/// over 50 KB and 3 over 100 KB at DEFAULT arguments on one server — the worst, <c>get_query_store_regressions</c>,
/// at 211 KB — and Claude Code refused <c>get_fleet_overview</c> (67-81 KB) inline outright. Every tool already
/// has its own row cap (<c>limit</c>/<c>top</c>), sized to what its author guessed a row costs; this is the
/// backstop for when that guess was wrong, or the row is wider on one store than another (plan XML, deadlock
/// graphs, query text all vary a lot row to row).
///
/// <para><b>Never silent.</b> Whenever a payload is cut, three fields are ADDED to the JSON root —
/// <see cref="TruncatedField"/>, <see cref="BudgetBytesField"/> and <see cref="NoteField"/> — so a caller that
/// hits the budget can always tell it happened and how to ask for less at a time. This is a SEPARATE signal
/// from any tool's own row-cap <c>truncated</c> field: a call can hit the row cap AND the byte budget at once,
/// and both markers stay independently true. It does not replace a tool's own field, because "the cap capped
/// the matching rows" and "the page still would not fit" are different facts.</para>
///
/// <para><b>Strategy: shape, not schema.</b> This never parses what a payload MEANS, only its JSON SHAPE. The
/// dominant shape across every tool #4198 measured is one big top-level array of similarly-sized rows (runs,
/// regressions, deadlocks, cards, ...), so the cut is: find the largest top-level array, estimate bytes per
/// element from the whole payload, and trim from the END until the WHOLE object fits (or the array is empty).
/// A payload with no array, or one too small to matter, is left alone — most tool responses never get near the
/// budget, which is why this can run on every call rather than only on the tools #4198 measured as offenders.
/// </para>
/// </summary>
public static class McpResponseBudget
{
    /// <summary>
    /// The default budget: 32 KB, roughly 8k tokens at a common 4-bytes-per-token estimate. Sized so a
    /// default call on a large production store fits inline in an agent client's per-result cap — #4198
    /// measured Claude Code refusing three 65,860-80,816 CHARACTER <c>get_fleet_overview</c> results outright
    /// ("result exceeds maximum allowed tokens") and writing them to files instead.
    /// </summary>
    public const int DefaultMaxBytes = 32 * 1024;

    /// <summary>Set true on the JSON root whenever this budget cut the payload. A tool-specific
    /// <c>truncated</c> field (the row cap) is untouched and may be true, false, or absent alongside this.</summary>
    public const string TruncatedField = "response_budget_truncated";

    /// <summary>The budget (bytes) this payload was measured against, echoed back so a client never has to
    /// hard-code what the server enforces.</summary>
    public const string BudgetBytesField = "response_budget_bytes";

    /// <summary>The "never silent" sentence: what happened and the lever to pull for the rest.</summary>
    public const string NoteField = "response_budget_note";

    private static readonly JsonSerializerOptions ReserializeOptions = new() { WriteIndented = false };

    /// <summary>
    /// Applies the budget to a tool's already-built JSON result. Returns the ORIGINAL string unchanged
    /// (verbatim, not a reparsed-and-reserialized copy) whenever it already fits, is not a JSON object, or
    /// cannot be parsed — this filter must never be the reason a tool's result stops round-tripping.
    /// </summary>
    public static (string Json, bool Truncated) Apply(string json, int maxBytes = DefaultMaxBytes)
    {
        if (string.IsNullOrEmpty(json) || Encoding.UTF8.GetByteCount(json) <= maxBytes)
        {
            return (json, false);
        }

        JsonNode? root;
        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException)
        {
            return (json, false);
        }

        if (root is not JsonObject obj)
        {
            return (json, false);
        }

        var currentBytes = Encoding.UTF8.GetByteCount(json);
        var trimmedField = TrimLargestArray(obj, currentBytes, maxBytes);

        if (trimmedField is null)
        {
            /* No array to cut — a single oversized field (or several small ones) is the whole payload.
               Nothing here reduces that safely without guessing which field matters, so the budget is
               reported as exceeded (never silent) without an attempted cut. */
            obj[TruncatedField] = true;
            obj[BudgetBytesField] = maxBytes;
            obj[NoteField] =
                $"This response exceeded the default {maxBytes:N0}-byte response budget and no array field "
                + "was found to shorten. Narrow the request (a smaller limit/hours_back, or a filter) to fit.";
            return (obj.ToJsonString(ReserializeOptions), true);
        }

        obj[TruncatedField] = true;
        obj[BudgetBytesField] = maxBytes;
        obj[NoteField] =
            $"This response exceeded the default {maxBytes:N0}-byte response budget; the '{trimmedField}' "
            + "array was cut from the end to fit. This is separate from any row-limit truncation the tool "
            + "itself reports. Narrow the request (a smaller limit/hours_back, or a filter) to see the rows "
            + "this cut, or page through with as_of/a narrower window for the remainder.";

        return (obj.ToJsonString(ReserializeOptions), true);
    }

    /// <summary>
    /// Finds the top-level array with the most elements and trims it from the end until the WHOLE object
    /// (re-serialized) fits <paramref name="maxBytes"/>, or the array is empty. Returns the trimmed
    /// property's name, or null when no non-empty top-level array exists.
    ///
    /// <para>Two passes rather than one: a cheap ESTIMATE (bytes-per-element from the payload's current total,
    /// which over-counts slightly since it divides the object's fixed overhead across every element too) cuts
    /// most of the way in a single re-serialize, then a bounded correction loop re-serializes the WHOLE object
    /// — not just the array — because every other field on the payload counts against the budget too, and
    /// rows are rarely perfectly uniform in size.</para>
    /// </summary>
    private static string? TrimLargestArray(JsonObject obj, int currentBytes, int maxBytes)
    {
        string? bestName = null;
        JsonArray? best = null;
        var bestCount = 0;

        foreach (var pair in obj)
        {
            if (pair.Value is JsonArray array && array.Count > bestCount)
            {
                bestName = pair.Key;
                best = array;
                bestCount = array.Count;
            }
        }

        if (best is null || bestCount == 0)
        {
            return null;
        }

        var perElement = Math.Max(1.0, (double)currentBytes / bestCount);
        /* 90% of the budget as the first-cut target, not 100%: the estimate above over-counts (it spreads
           the object's fixed overhead across every element), so aiming short of the line means the
           correction loop below usually has little or nothing left to do. */
        var target = (int)Math.Floor(maxBytes * 0.9 / perElement);
        target = Math.Clamp(target, 0, bestCount);

        while (best.Count > target)
        {
            best.RemoveAt(best.Count - 1);
        }

        /* Bounded correction: re-serializing the WHOLE object on every iteration is the only accurate
           measure (the array alone omits every sibling field), so this is capped rather than left to run
           until it fits — a pathological payload (every remaining "row" still huge) must still terminate. */
        var guard = 0;
        while (best.Count > 0 && guard++ < 200
            && Encoding.UTF8.GetByteCount(obj.ToJsonString(ReserializeOptions)) > maxBytes)
        {
            best.RemoveAt(best.Count - 1);
        }

        return bestName;
    }
}
