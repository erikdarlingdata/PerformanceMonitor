/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Helpers;

/// <summary>One line of a section's table. <c>Kind</c> is <c>tag</c>, <c>untagged</c> or <c>total</c>.</summary>
public sealed record QueryTagUsageRow(string Kind, string Label, IReadOnlyList<string> Cells);

/// <summary>One section's table for one scope (one server, or the fleet line). Sections are never added together.</summary>
public sealed record QueryTagUsageTable(
    string Section, string Title, string Status, string? Message, string? Note,
    IReadOnlyList<string> Columns, IReadOnlyList<QueryTagUsageRow> Rows);

/// <summary>The view's answer: a top-level message (or null) and the tables, in section order.</summary>
public sealed record QueryTagUsageResult(string? Message, IReadOnlyList<QueryTagUsageTable> Tables);

/// <summary>
/// #5993 part B: the "Resource use by tag" view's model. It shapes what <see cref="QueryTagRollup"/> returns (the same roll-up
/// <c>get_resource_use_by_tag</c> answers from) into one table per section, with the tag rows' own figures, the <c>untagged</c> row and
/// the section's total. It never adds one section to another.
/// </summary>
public static class QueryTagUsageView
{
    private static readonly QueryTagRollupSection[] Additive =
    [
        QueryTagRollupSection.PlanCache, QueryTagRollupSection.QueryStore, QueryTagRollupSection.Procedures,
        QueryTagRollupSection.BlockingReports, QueryTagRollupSection.BlockingSnapshots,
    ];

    public static QueryTagUsageResult Build(
        QueryTagRollupData data, IReadOnlyList<(int Id, string Name)> scope, bool allServers, int? tagFilter, int limit = 200)
    {
        var tables = new List<QueryTagUsageTable>();
        var notServed = new List<string>();
        var anyUnfiltered = data.Deadlocks.Count > 0;
        var anyData = tagFilter is null
            ? data.Deadlocks.Count > 0
            : data.Deadlocks.Any(d => d.ProcessesByTag.Keys.Any(k => QueryTagRollup.IsUnderTag(k, data.Tags, tagFilter)));

        foreach (var section in Additive)
        {
            var facts = data.Sections[section.Name];
            anyUnfiltered |= facts.Count > 0;
            anyData |= facts.Any(f => QueryTagRollup.IsUnderTag(f.TagId, data.Tags, tagFilter));
            data.Unavailable.TryGetValue(section.Name, out var why);
            if (why is not null) notServed.Add($"{PerformanceMonitor.Common.QueryTagUsageLabels.SectionTitle(section.Name)} is unavailable: {why}");
            Add(tables, QueryTagRollup.Section(section, facts, scope, allServers, data.Tags, tagFilter, limit, why), section);
        }

        data.Unavailable.TryGetValue(QueryTagRollupSection.Deadlocks.Name, out var deadlockWhy);
        if (deadlockWhy is not null) notServed.Add($"{PerformanceMonitor.Common.QueryTagUsageLabels.SectionTitle(QueryTagRollupSection.Deadlocks.Name)} is unavailable: {deadlockWhy}");
        Add(tables, QueryTagRollup.DeadlockSection(data.Deadlocks, scope, allServers, data.Tags, tagFilter, limit, data.ScanTruncated, deadlockWhy), QueryTagRollupSection.Deadlocks);

        string? message = null;
        if (tagFilter is int id && !anyData && anyUnfiltered)
        {
            message = $"The window held resource use, but none of it is under query tag '{data.Tags.First(t => t.Id == id).Name}', so every section is empty for that tag. Choose All tags to see what the window holds.";
        }
        else if (!anyData)
        {
            message = "No resource use was collected in the window" + (allServers ? " on any server." : " on this server.");
        }

        if (notServed.Count > 0)
        {
            message = (message is null ? "" : message + " ") + "Sections not served: " + string.Join(" ", notServed);
        }

        return new QueryTagUsageResult(message, tables);
    }

    private static void Add(List<QueryTagUsageTable> tables, Dictionary<string, object?> block, QueryTagRollupSection section)
    {
        if (block.TryGetValue("servers", out var servers) && servers is IEnumerable<object> list)
        {
            foreach (var server in list.OfType<Dictionary<string, object?>>())
            {
                tables.Add(One(server, section, $"{PerformanceMonitor.Common.QueryTagUsageLabels.SectionTitle(section.Name)} - {server["server"]}"));
            }

            if (block.TryGetValue("fleet_total", out var fleet) && fleet is Dictionary<string, object> figures)
            {
                var columns = Columns(section);
                tables.Add(new QueryTagUsageTable(section.Name, $"{PerformanceMonitor.Common.QueryTagUsageLabels.SectionTitle(section.Name)} - all servers",
                    block["status"]?.ToString() ?? "ok", block.GetValueOrDefault("message") as string, Note(block), columns,
                    [new QueryTagUsageRow("total", "Total (all servers)", Cells(figures, section))]));
            }

            return;
        }

        tables.Add(One(block, section, PerformanceMonitor.Common.QueryTagUsageLabels.SectionTitle(section.Name)));
    }

    private static QueryTagUsageTable One(Dictionary<string, object?> block, QueryTagRollupSection section, string title)
    {
        var rows = new List<QueryTagUsageRow>();
        if (block.GetValueOrDefault("tag_rows") is IEnumerable<object> tagRows)
        {
            foreach (var row in tagRows.OfType<Dictionary<string, object?>>())
            {
                rows.Add(new QueryTagUsageRow("tag", row["path"]?.ToString() ?? "", Cells((Dictionary<string, object>)row["own"]!, section)));
            }
        }

        if (block.GetValueOrDefault("untagged") is Dictionary<string, object> untagged)
        {
            rows.Add(new QueryTagUsageRow("untagged", "untagged", Cells(untagged, section)));
        }

        if (block.GetValueOrDefault("total") is Dictionary<string, object> total)
        {
            rows.Add(new QueryTagUsageRow("total", "Total", Cells(total, section)));
        }

        return new QueryTagUsageTable(section.Name, title, block["status"]?.ToString() ?? "ok", block.GetValueOrDefault("message") as string, Note(block), Columns(section), rows);
    }

    private static string? Note(Dictionary<string, object?> block) => block.GetValueOrDefault("note") as string;

    private static List<string> Columns(QueryTagRollupSection section) => ["Tag", .. section.Measures.Select(m => PerformanceMonitor.Common.QueryTagUsageLabels.MeasureLabel(m.Name))];

    private static List<string> Cells(Dictionary<string, object> figures, QueryTagRollupSection section) =>
        section.Measures.Select(m => Convert.ToDouble(figures[m.Name], CultureInfo.InvariantCulture).ToString("#,0.###", CultureInfo.InvariantCulture)).ToList();
}
