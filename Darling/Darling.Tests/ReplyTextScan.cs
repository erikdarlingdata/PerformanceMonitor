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
using System.Text.Json;
using System.Text.RegularExpressions;

namespace Darling.Tests;

/// <summary>
/// #8572: the reply-text rules every MCP reply follows, as a scan over the text a tool actually returns. A reply is read by a model or
/// pasted into a ticket as it stands, so its prose carries no em dash (U+2014), no tracker reference (a "#" followed by digits), and no count
/// of four or more digits written without its thousands separators. The scan walks every string value in a JSON reply and answers one line per
/// hit. Strings that carry captured data rather than product prose (the text of a captured statement, a plan, a report) are not the product's
/// wording, so <see cref="DataProperties"/> names the few properties skipped, each with its reason.
/// </summary>
internal static partial class ReplyTextScan
{
    /// <summary>Properties whose value is captured data the product did not word: the text of a monitored statement, a plan or a report.
    /// Their content is whatever the monitored server ran, so an em dash or a number in it is not a defect in a reply.</summary>
    internal static readonly HashSet<string> DataProperties = new(StringComparer.OrdinalIgnoreCase)
    {
        "query_text", "sql_text", "statement_text", "blocked_sql_text", "blocking_sql_text", "query_plan", "plan_xml",
        "blocked_process_report_xml", "deadlock_graph", "deadlock_xml", "plan_text", "text", "input_buf", "event_xml",
    };

    [GeneratedRegex(@"#\d+")]
    private static partial Regex TrackerReference();

    /* A run of four or more digits that stands alone as a count: not inside a longer token (a hash, a date, a time, a version, a decimal, a
       range or an identifier), so the character before it is not a letter, digit, dot, comma, colon, slash, hyphen, plus or hash, and the
       character after it is not a letter, digit, hyphen, colon or slash (a date or a time), and not a dot or comma that is
       followed by a digit. Two phrases name an identifier, not a count: a product release year ("SQL Server 2019") and a database known
       only by its object id ("database 16385", the read's own fallback when the name is unknown). */
    [GeneratedRegex(@"(?<![\w.,:/\-+#$])(?<!SQL Server )(?<!database )\d{4,}(?![\w\-:/])(?![.,]\d)")]
    private static partial Regex UngroupedCount();

    /// <summary>One line per rule hit in <paramref name="reply"/>: the path of the string and the offending text. A reply that is not JSON is
    /// scanned as one string.</summary>
    internal static List<string> Hits(string reply)
    {
        var found = new List<string>();
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(reply);
        }
        catch (JsonException)
        {
            ScanString(reply, "$", found);
            return found;
        }

        using (document)
        {
            Walk(document.RootElement, "$", found);
        }

        return found;
    }

    private static void Walk(JsonElement element, string path, List<string> found)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.String:
                ScanString(element.GetString() ?? string.Empty, path, found);
                break;
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    if (DataProperties.Contains(property.Name))
                    {
                        continue;
                    }

                    Walk(property.Value, path + "." + property.Name, found);
                }

                break;
            case JsonValueKind.Array:
                var index = 0;
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item, path + "[" + index.ToString(CultureInfo.InvariantCulture) + "]", found);
                    index++;
                }

                break;
        }
    }

    private static void ScanString(string text, string path, List<string> found)
    {
        if (text.Contains('\u2014', StringComparison.Ordinal))
        {
            found.Add(path + " em dash: " + Excerpt(text, text.IndexOf('\u2014', StringComparison.Ordinal)));
        }

        foreach (Match m in TrackerReference().Matches(text))
        {
            found.Add(path + " tracker reference: " + Excerpt(text, m.Index));
        }

        foreach (Match m in UngroupedCount().Matches(text))
        {
            found.Add(path + " ungrouped count: " + Excerpt(text, m.Index));
        }
    }

    private static string Excerpt(string text, int at)
    {
        var start = Math.Max(0, at - 40);
        var end = Math.Min(text.Length, at + 40);
        return "..." + text[start..end].Replace('\n', ' ') + "...";
    }
}
