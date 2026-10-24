/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>What a saved view's panel or cell becomes when the viewer runs it.</summary>
public enum ViewerRunItemKind
{
    /// <summary>A notebook markdown cell: <see cref="ViewerRunItem.Text"/> is drawn as text.</summary>
    Markdown,

    /// <summary>A composed panel: <see cref="ViewerRunItem.Body"/> is the run body the web sends to <c>/api/compose/run</c>.</summary>
    Composed,

    /// <summary>A v1 read panel (or a notebook read cell). The viewer does not run it: <see cref="ViewerRunItem.Text"/> says where it runs.</summary>
    ReadPanel
}

public sealed record ViewerRunItem(ViewerRunItemKind Kind, string Title, string? Text, string? Viz, JsonObject? Body);

/// <summary>
/// #6390 S6: turns a stored Custom View definition into the items the viewer runs, and a run payload into what it draws. The run
/// body is built the way the web's <c>compose.js</c> builds it (<c>buildRunBody</c> / <c>toRunPanel</c>: the compose fields only, the
/// view's variables and default values, the scoped server, the view or panel window), so the same panel gets the same
/// <c>/api/compose/run</c> payload from <see cref="ViewerDataService.RunComposedPanelAsync"/> and from the web.
/// </summary>
public static class ViewerCustomViewRun
{
    /// <summary>The line a v1 read panel shows in place of a run (the 82-read dispatch is in the service only).</summary>
    public static string ReadPanelText(string? readName) =>
        $"This panel reads {(string.IsNullOrWhiteSpace(readName) ? "a read" : readName)}. Open it in the web dashboard.";

    /// <summary>The items of a stored definition, in page order, or the reason it cannot be read at all.</summary>
    public static (IReadOnlyList<ViewerRunItem> Items, string? Error) Plan(string definitionJson, string? server)
    {
        JsonObject root;
        try
        {
            root = JsonNode.Parse(definitionJson) as JsonObject ?? throw new JsonException("not an object");
        }
        catch (JsonException)
        {
            return (Array.Empty<ViewerRunItem>(), "This view's definition is not valid JSON, so it cannot be run.");
        }

        var isNotebook = string.Equals((root["kind"] as JsonValue)?.GetValue<string>(), "notebook", StringComparison.Ordinal);
        var members = (isNotebook ? root["cells"] : root["panels"]) as JsonArray;
        if (members is null)
        {
            return (Array.Empty<ViewerRunItem>(), isNotebook ? "This notebook has no cells." : "This view has no panels.");
        }

        var variables = root["variables"] as JsonArray;
        var values = DefaultValues(variables);
        var viewRange = root["range"] as JsonObject;
        var items = new List<ViewerRunItem>();
        foreach (var member in members.OfType<JsonObject>())
        {
            var title = StringOf(member["title"]) ?? "";
            if (isNotebook && string.Equals(StringOf(member["type"]), "markdown", StringComparison.Ordinal))
            {
                items.Add(new ViewerRunItem(ViewerRunItemKind.Markdown, title, StringOf(member["text"]) ?? "", null, null));
            }
            else if (member["source"] is not null)
            {
                items.Add(new ViewerRunItem(
                    ViewerRunItemKind.Composed, title, null, StringOf(member["viz"]),
                    BuildRunBody(member, variables, values, server, viewRange)));
            }
            else
            {
                items.Add(new ViewerRunItem(ViewerRunItemKind.ReadPanel, title, ReadPanelText(StringOf(member["read"])), StringOf(member["viz"]), null));
            }
        }

        return (items, null);
    }

    /// <summary>The <c>/api/compose/run</c> body for one composed panel: the web's <c>buildRunBody</c>.</summary>
    public static JsonObject BuildRunBody(JsonObject panelSpec, JsonArray? variables, JsonObject values, string? server, JsonObject? viewRange)
    {
        var body = new JsonObject { ["panel"] = ToRunPanel(panelSpec) };
        if (variables is { Count: > 0 })
        {
            body["variables"] = variables.DeepClone();
        }

        if (values.Count > 0)
        {
            body["values"] = values.DeepClone();
        }

        if (!string.IsNullOrWhiteSpace(server))
        {
            body["server"] = server;
        }

        if (ReadHours(viewRange) is { } viewHours)
        {
            body["hours"] = viewHours;
        }

        /* The panel's own window pin wins over the view's: the #2735 cell `range` (relative hours or an absolute pair), else the
           composer's legacy per-panel `hours` of at least 1. */
        if (panelSpec["range"] is JsonObject range)
        {
            if (range["windowStart"] is not null && range["windowEnd"] is not null)
            {
                body["windowStart"] = range["windowStart"]!.DeepClone();
                body["windowEnd"] = range["windowEnd"]!.DeepClone();
            }
            else if (ReadHours(range) is { } pinned)
            {
                body["hours"] = pinned;
            }
        }
        else if (ReadHours(new JsonObject { ["hours"] = panelSpec["hours"]?.DeepClone() }) is { } legacy)
        {
            body["hours"] = legacy;
        }

        return body;
    }

    /// <summary>The compose fields the run reads, nothing of the page's own (kind, title, span, hours, range): <c>toRunPanel</c> in compose.js.</summary>
    private static JsonObject ToRunPanel(JsonObject p)
    {
        var o = new JsonObject { ["source"] = p["source"]?.DeepClone(), ["viz"] = p["viz"]?.DeepClone() };
        foreach (var key in new[] { "measure", "ratio", "aggregate", "unit", "topN" })
        {
            if (p[key] is { } v && !(key is "aggregate" or "unit" && StringOf(v) is "" or null))
            {
                o[key] = v.DeepClone();
            }
        }

        var timeBucket = StringOf(p["timeBucket"]);
        if (!string.IsNullOrEmpty(timeBucket) && timeBucket != "none")
        {
            o["timeBucket"] = timeBucket;
        }

        if (p["includeOther"] is JsonValue io && io.TryGetValue<bool>(out var other) && other)
        {
            o["includeOther"] = true;
        }

        foreach (var key in new[] { "filters", "groupBy" })
        {
            if (p[key] is JsonArray { Count: > 0 } arr)
            {
                o[key] = arr.DeepClone();
            }
        }

        if (p["overlay"] is JsonObject overlay && overlay["measure"] is not null)
        {
            o["overlay"] = overlay.DeepClone();
        }

        if (o["timeBucket"] is not null && p["annotations"] is JsonArray { Count: > 0 } annotations)
        {
            o["annotations"] = annotations.DeepClone();
        }

        return o;
    }

    private static JsonObject DefaultValues(JsonArray? variables)
    {
        var values = new JsonObject();
        foreach (var variable in variables?.OfType<JsonObject>() ?? Enumerable.Empty<JsonObject>())
        {
            if (StringOf(variable["name"]) is { Length: > 0 } name && variable["default"] is { } d)
            {
                values[name] = d.DeepClone();
            }
        }

        return values;
    }

    private static int? ReadHours(JsonObject? range) =>
        range?["hours"] is JsonValue v && v.TryGetValue<double>(out var h) && h >= 1 ? (int)Math.Floor(h) : null;

    private static string? StringOf(JsonNode? node) =>
        node is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;

    /// <summary>The columns a payload's rows carry, in first-seen order. Rows are objects keyed by column name.</summary>
    public static IReadOnlyList<string> ColumnsOf(JsonArray rows)
    {
        var columns = new List<string>();
        foreach (var row in rows.OfType<JsonObject>())
        {
            foreach (var key in row.Select(kv => kv.Key).Where(k => !columns.Contains(k)))
            {
                columns.Add(key);
            }
        }

        return columns;
    }

    /// <summary>One cell as the grid holds it: a number stays a number (so a column sorts as numbers), null is DBNull, else text.</summary>
    public static object CellValue(JsonNode? node)
    {
        if (node is not JsonValue v)
        {
            return node is null ? DBNull.Value : node.ToJsonString();
        }

        if (v.TryGetValue<long>(out var l))
        {
            return l;
        }

        if (v.TryGetValue<double>(out var d))
        {
            return d;
        }

        if (v.TryGetValue<bool>(out var b))
        {
            return b;
        }

        return v.TryGetValue<string>(out var s) ? s : v.ToJsonString();
    }

    /// <summary>The payload's rows as a grid table. A column mixing numbers and text holds text.</summary>
    public static DataTable ToTable(JsonArray rows)
    {
        var table = new DataTable { Locale = CultureInfo.InvariantCulture };
        var columns = ColumnsOf(rows);
        var objects = rows.OfType<JsonObject>().ToList();
        foreach (var column in columns)
        {
            var kinds = objects.Select(r => CellValue(r[column])).Where(c => c is not DBNull).Select(c => c.GetType()).Distinct().ToList();
            table.Columns.Add(column, kinds.Count == 1 ? kinds[0] : typeof(string));
        }

        foreach (var row in objects)
        {
            var cells = columns.Select(c => CellValue(row[c])).ToArray();
            for (var i = 0; i < cells.Length; i++)
            {
                if (cells[i] is not DBNull && table.Columns[i].DataType == typeof(string) && cells[i] is not string)
                {
                    cells[i] = Convert.ToString(cells[i], CultureInfo.InvariantCulture)!;
                }
            }

            table.Rows.Add(cells);
        }

        return table;
    }

    /// <summary>The viz families the viewer draws as a chart; the others (table, bar, pie, bandlist) are a table or list.</summary>
    public static bool IsLineFamily(string? viz) => viz is "line" or "area" or "stacked" or "stacked-bar";

    /// <summary>The most lines one chart draws; a legend of more is unreadable.</summary>
    public const int MaxSeries = 12;

    /// <summary>The line shown beside a chart that drew fewer lines than the payload held, or null when every line is drawn.</summary>
    public static string? SeriesCapNotice(int total) =>
        total > MaxSeries ? $"Showing the first {MaxSeries} of {total} series." : null;

    /// <summary>A time-series payload's lines: one per distinct value of the non-time, non-value columns, each point (UTC bucket, value).
    /// Null when the rows carry no <c>bucket</c> (a ranked or scalar result), which is shown as a table.</summary>
    public static IReadOnlyList<(string Name, IReadOnlyList<(DateTime Bucket, double Value)> Points)>? Lines(JsonArray rows)
    {
        var objects = rows.OfType<JsonObject>().ToList();
        if (objects.Count == 0 || objects.Any(r => r["bucket"] is null))
        {
            return null;
        }

        var series = new List<string>();
        var points = new Dictionary<string, List<(DateTime, double)>>(StringComparer.Ordinal);
        foreach (var row in objects)
        {
            if (!DateTime.TryParse(StringOf(row["bucket"]), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal, out var bucket)
                || row["value"] is not JsonValue value || !value.TryGetValue<double>(out var y))
            {
                continue;
            }

            var name = string.Join(" / ", row.Where(kv => kv.Key is not ("bucket" or "value" or "value2")).Select(kv => CellValue(kv.Value) is DBNull ? "(none)" : Convert.ToString(CellValue(kv.Value), CultureInfo.InvariantCulture)));
            if (name.Length == 0)
            {
                name = "value";
            }

            if (!points.TryGetValue(name, out var list))
            {
                points[name] = list = new List<(DateTime, double)>();
                series.Add(name);
            }

            list.Add((bucket, y));
        }

        return series.Select(s => (s, (IReadOnlyList<(DateTime, double)>)points[s].OrderBy(p => p.Item1).ToList())).ToList();
    }

    /// <summary>The stat tile's text: the first row's <c>value</c>, or null when there is none.</summary>
    public static string? StatText(JsonArray rows)
    {
        if (rows.OfType<JsonObject>().FirstOrDefault() is not { } first || first["value"] is null)
        {
            return null;
        }

        return CellValue(first["value"]) switch
        {
            DBNull => "-",
            double d => d.ToString("#,##0.##", CultureInfo.CurrentCulture),
            long l => l.ToString("N0", CultureInfo.CurrentCulture),
            var other => Convert.ToString(other, CultureInfo.CurrentCulture),
        };
    }
}
