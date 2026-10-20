/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The shared response-size target every MCP read tool's DEFAULT arguments are sized against (#4198). #4198
/// measured 12 tools over 50 KB and 3 over 100 KB at default arguments on one server — the worst,
/// <c>get_query_store_regressions</c>, at 211 KB — and Claude Code refused <c>get_fleet_overview</c> (67-81 KB)
/// inline outright, spilling the result to a file instead.
///
/// <para>An earlier pass of this work trimmed any oversized reply after the fact, in one call-tool filter shared
/// by every tool. #4198 ruled that out: a trim after the fact makes the tool's own <c>truncated</c> /
/// <c>*_returned</c> fields wrong, cuts calls that explicitly asked for more rows, and — for anything sorted
/// oldest-first — drops the newest rows instead of the oldest. So this is a SIZE TARGET for each tool's own
/// default <c>limit</c>/<c>top</c> and field widths, not a filter: every tool sizes its own defaults so that a
/// default call stays near this many bytes, the same way <see cref="TrendBuckets.McpPointBudget"/> is the point
/// count trend reads size their own default bucketing against.</para>
/// </summary>
public static class McpResponseBudget
{
    /// <summary>
    /// The size target: 32 KB, roughly 8k tokens at a common 4-bytes-per-token estimate. Each read tool's
    /// default arguments (row limits, field widths) are sized so a default call on a large production store
    /// stays under this — #4198 measured Claude Code refusing three 65,860-80,816 CHARACTER
    /// <c>get_fleet_overview</c> results outright ("result exceeds maximum allowed tokens") and writing them to
    /// files instead.
    /// </summary>
    public const int DefaultBytes = 32 * 1024;

    /// <summary>
    /// <c>get_collection_log</c>'s fleet-wide form (#4199, both products) default row limit, shared so a caller
    /// who omits <c>server_name</c> (or passes <c>"*"</c>) gets the same page size from Darling and Lite. The
    /// per-server form keeps its own default — sized separately by <see cref="CollectionLogPerServerDefaultLimit"/>.
    /// #4198 measured a per-server default call at 85-88 KB for 200 rows (about 435-440 bytes/row); the fleet
    /// form's row is that same shape plus one added <c>server_name</c> field, so a page this size stays under
    /// <see cref="DefaultBytes"/> with room for the envelope fields around the array. See the fleet-form
    /// byte-budget tests in Darling.Tests / Lite.Tests for the measured number this was set from.
    /// </summary>
    public const int CollectionLogFleetDefaultLimit = 60;

    /// <summary>
    /// <c>get_collection_log</c>'s ONE-SERVER form (#4198) default row limit. The old default (200) measured
    /// 85,206-90,514 bytes on a seeded store — about 2.6-2.8x <see cref="DefaultBytes"/> — with the SQL Server
    /// target shape the wider of the two, because its <c>query_store</c> rows carry the <c>plan_fetch</c>/
    /// <c>text_fetch</c> deferred-fetch split that PostgreSQL targets never populate. Sized down so a default
    /// call stays under budget on the wider shape, with room for the envelope fields around the array; a
    /// caller after more rows still passes <c>limit</c> explicitly. See
    /// <c>McpCollectionLogPerServerResponseBudgetLivePostgresTests</c> / the Lite.Tests twin for the measured
    /// numbers this was set from.
    /// </summary>
    public const int CollectionLogPerServerDefaultLimit = 58;

    /// <summary>
    /// #6180: how many LEADING items of a list a reply can carry and still fit <paramref name="budgetBytes"/>, for the
    /// reads that answer a caller who asked for more than the budget holds (<c>full_detail</c>, a large <c>limit</c>) or
    /// whose defaults were over it (<c>get_perfmon_stats</c>' whole counter set). The reply is serialized through
    /// <paramref name="serializeFirst"/> with the first <c>n</c> items, so the envelope's own digits, flags and notes
    /// count against the budget, and the largest <c>n</c> that fits wins (binary search: the size only grows with
    /// <c>n</c>). At least <paramref name="minimum"/> items are always kept, so one item bigger than the budget is still
    /// answered whole rather than cut mid-way or replaced by nothing. Items are never trimmed, only left off the end.
    /// </summary>
    /// <param name="total">How many items there are.</param>
    /// <param name="serializeFirst">The full reply for the first <c>n</c> items.</param>
    /// <param name="budgetBytes">The UTF-8 byte target, normally <see cref="DefaultBytes"/>.</param>
    /// <param name="minimum">The fewest items to keep, even when they are over the budget. Default 1.</param>
    /// <returns>The count that fits and that count's reply.</returns>
    public static (int Count, string Json) FitPrefix(int total, Func<int, string> serializeFirst, int budgetBytes = DefaultBytes, int minimum = 1)
    {
        ArgumentNullException.ThrowIfNull(serializeFirst);
        var floor = Math.Clamp(minimum, 0, Math.Max(total, 0));

        var whole = serializeFirst(total);
        if (total <= floor || System.Text.Encoding.UTF8.GetByteCount(whole) <= budgetBytes)
        {
            return (total, whole);
        }

        /* Largest n in [floor, total) whose reply fits; floor when none does. */
        var lo = floor;
        var hi = total - 1;
        var best = floor;
        string? bestJson = null;
        while (lo <= hi)
        {
            var mid = lo + ((hi - lo) / 2);
            var json = serializeFirst(mid);
            if (mid == floor || System.Text.Encoding.UTF8.GetByteCount(json) <= budgetBytes)
            {
                best = mid;
                bestJson = json;
                lo = mid + 1;
            }
            else
            {
                hi = mid - 1;
            }
        }

        return (best, bestJson ?? serializeFirst(best));
    }

    /// <summary>
    /// How many leading rows of a page fit <paramref name="budgetBytes"/> (#6131). <paramref name="serializedBytes"/>
    /// returns the byte size of the WHOLE answer carrying its first n rows, built the way the tool really writes it
    /// (envelope, cut note and all), so the count is exact rather than an estimate from one row's size. The answer
    /// grows with n, so the largest n that fits is found by bisection (about eight serializations for the 200-row
    /// ceiling) instead of one per row. Never below one row when there is one: a single row wider than the budget is
    /// still the answer, as <c>get_collection_log</c> and the Availability Group read treat it.
    /// </summary>
    public static int RowsThatFit(int rowCount, Func<int, int> serializedBytes, int budgetBytes = DefaultBytes)
    {
        if (rowCount <= 1 || serializedBytes(rowCount) <= budgetBytes)
        {
            return rowCount;
        }

        var low = 1;
        var high = rowCount - 1;
        while (low < high)
        {
            var mid = (low + high + 1) / 2;
            if (serializedBytes(mid) <= budgetBytes)
            {
                low = mid;
            }
            else
            {
                high = mid - 1;
            }
        }

        return low;
    }

    /// <summary>
    /// The one sentence a tool prints when <see cref="RowsThatFit"/> cut its page (#6131), in the wording the Availability
    /// Group read uses ("fit the N-byte response budget").
    /// </summary>
    public static string PageCutNote(int rowsInScope, int rowsReturned, string order, string noun = "rows") =>
        string.Create(System.Globalization.CultureInfo.InvariantCulture,
            $"TRUNCATED: {rowsInScope:#,0} {noun} were read; only {rowsReturned:#,0} fit the {DefaultBytes:#,0}-byte response budget ({order}). Lower the row count asked for, or narrow the window or database, to see the rest.");

    /// <summary>
    /// Cuts <paramref name="payload"/>'s <paramref name="arrayKey"/> array to the rows that fit <paramref name="budgetBytes"/> and says
    /// so (#6131): <c>truncated</c> (true when rows were left out, false when none were) and <c>budget_note</c> (the
    /// <see cref="PageCutNote"/> sentence, null when nothing was cut) are written onto the payload, and the answer is returned
    /// serialized with <see cref="McpHelpers.JsonOptions"/>. The rows keep their order, so a ranked page loses its lightest rows.
    /// <c>truncated</c> here is the budget cut only: a tool whose own payload also carries a window-floor flag keeps that
    /// flag under its own name.
    /// </summary>
    public static string CapPage(System.Text.Json.Nodes.JsonObject payload, string arrayKey, string order, string noun, int budgetBytes = DefaultBytes)
    {
        if (payload[arrayKey] is not System.Text.Json.Nodes.JsonArray page)
        {
            return payload.ToJsonString(McpHelpers.JsonOptions);
        }

        var rowNodes = page.ToList();
        var inScope = rowNodes.Count;

        string Measure(int n)
        {
            var cut = n < inScope;
            payload[arrayKey] = new System.Text.Json.Nodes.JsonArray(rowNodes.Take(n).Select(x => x?.DeepClone()).ToArray());
            payload["truncated"] = cut;
            payload["budget_note"] = cut ? PageCutNote(inScope, n, order, noun) : null;
            return payload.ToJsonString(McpHelpers.JsonOptions);
        }

        var fit = RowsThatFit(inScope, n => System.Text.Encoding.UTF8.GetByteCount(Measure(n)), budgetBytes);
        return Measure(fit);
    }
}
