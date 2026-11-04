/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #7545: what the web page does with the reports get_blocked_process_xml left out to stay within its reply size target.
/// Each report is a whole XML document, so the page of 5 can hold one, and the tool lists the others by their identifying
/// fields in <c>left_out_reports</c>. The Blocked Process Reports grid reads <c>reports</c> only, so those rows were
/// invisible and the note above the grid blamed the row limit. This puts them in <c>reports</c> as rows without their XML
/// (<c>xml_left_out: true</c>, for the grid's Load XML button), says in <c>left_out_note</c> why they have none, and moves
/// the page's oldest-row time to the oldest row the grid now shows. The tool's own reply and its size target are untouched.
/// </summary>
internal static class WebBlockedReportFold
{
    internal static async Task<string> FoldAsync(Task<string> read) => Fold(await read);

    internal static string Fold(string result)
    {
        JsonObject? payload;
        try
        {
            payload = JsonNode.Parse(result) as JsonObject;
        }
        catch (JsonException)
        {
            return result;
        }

        if (payload is null
            || payload["left_out_count"] is not JsonValue countValue || !countValue.TryGetValue<int>(out var leftOutCount) || leftOutCount <= 0
            || payload["left_out_reports"] is not JsonArray listed
            || payload["reports"] is not JsonArray reports)
        {
            return result;
        }

        var server = payload["server"]?.GetValue<string>();
        var served = reports.Count;
        var added = 0;
        foreach (var item in listed)
        {
            if (item is not JsonObject source)
            {
                continue;
            }

            reports.Add(new JsonObject
            {
                ["event_time"] = source["event_time"]?.DeepClone(),
                ["server"] = server,
                ["database_name"] = source["database_name"]?.DeepClone(),
                ["blocked_spid"] = source["blocked_spid"]?.DeepClone(),
                ["blocking_spid"] = source["blocking_spid"]?.DeepClone(),
                ["wait_time_ms"] = source["wait_time_ms"]?.DeepClone(),
                ["blocked_process_report_xml"] = null,
                ["xml_left_out"] = true,
            });
            added++;
        }

        if (added == 0)
        {
            return result;
        }

        var notListed = Math.Max(0, leftOutCount - added);
        payload.Remove("left_out_reports");
        payload.Remove("size_note");
        payload["left_out_count"] = notListed;
        payload["reports_returned"] = reports.Count;
        payload["reports_with_xml"] = served;

        DateTime? oldestAt = null;
        foreach (var row in reports)
        {
            if (row?["event_time"] is JsonValue v && v.TryGetValue<string>(out var text)
                && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var at)
                && (oldestAt is null || at < oldestAt))
            {
                oldestAt = at;
            }
        }

        /* #5015: the tool writes this key in the UTC round-trip form, so the fold does too: the row's own text can be naive. */
        if (oldestAt is DateTime oldestUtc)
        {
            payload["oldest_returned_event_time"] = McpHelpers.FormatEffectiveStart(oldestUtc);
        }

        /* The tool's truncated is "more reports than the page, or some left out for size". With the left-out ones listed, only the
           first can still be true, and the tool sends it on its own as more_reports_than_limit (a window holding exactly limit reports
           is not more). Rows past the tool's list of 20 are not shown, so they keep truncated true. */
        var moreThanLimit = payload["more_reports_than_limit"] is JsonValue more && more.TryGetValue<bool>(out var flag) && flag;
        payload["truncated"] = notListed > 0 || moreThanLimit;

        payload["left_out_note"] =
            (added == 1 ? "1 report has no XML here" : $"{added} reports have no XML here")
            + $" (the newest {reports.Count} are listed), because one reply holds about 32 KB and each report is large. "
            + "Click Load XML on a row to read that report alone."
            + (notListed > 0 ? $" {notListed} more are not listed: narrow the time range to reach them." : "");

        return payload.ToJsonString();
    }
}
