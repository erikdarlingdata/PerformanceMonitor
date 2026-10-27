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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One line of a section's table. <c>Kind</c> is <c>tag</c>, <c>untagged</c> or <c>total</c>.</summary>
public sealed record QueryTagUsageRow(string Kind, string Label, IReadOnlyList<string> Cells);

/// <summary>One section's table for one scope (one server, or the fleet line). Sections are never added together.</summary>
public sealed record QueryTagUsageTable(
    string Section, string Title, string Status, string? Message, string? Note,
    IReadOnlyList<string> Columns, IReadOnlyList<QueryTagUsageRow> Rows);

/// <summary>The view's answer: a top-level message (or null) and the tables, in section order.</summary>
public sealed record QueryTagUsageResult(string? Message, IReadOnlyList<QueryTagUsageTable> Tables);

/// <summary>
/// #5988 part B: the "Resource use by tag" view's model, the twin of Lite's <c>QueryTagUsageView</c>. It shapes what
/// <see cref="DarlingQueryTagRollupReader"/> assembles (the same figures <c>get_resource_use_by_tag</c> answers) into one table per section, with
/// the tag rows' own figures, the <c>untagged</c> row and the section's total. It never adds one section to another. Pure: no WPF, no store.
/// </summary>
public static class QueryTagUsageView
{
    private static readonly QueryTagRollupSection[] Additive =
    [
        QueryTagRollupSection.PlanCache, QueryTagRollupSection.QueryStore, QueryTagRollupSection.Procedures,
        QueryTagRollupSection.BlockingReports, QueryTagRollupSection.BlockingSnapshots,
    ];

    /// <summary>The choice in the tag picker that means no filter.</summary>
    public const string AllChoice = "All tags";

    /// <summary>The picker's choices: all, then every tag by its path, in path order.</summary>
    public static List<string> Choices(IReadOnlyList<QueryTagNode> tags) => [AllChoice, .. Paths(tags).Values.Order(StringComparer.OrdinalIgnoreCase)];

    /// <summary>The tag a picker choice names, or null for all (or a path no tag has).</summary>
    public static int? FilterFor(IReadOnlyList<QueryTagNode> tags, string? choice) =>
        string.IsNullOrWhiteSpace(choice) || choice == AllChoice
            ? null
            : Paths(tags).Where(p => p.Value == choice).Select(p => (int?)p.Key).FirstOrDefault();

    /// <summary>Each tag's path, parents first, joined with the one separator the read joins use.</summary>
    public static Dictionary<int, string> Paths(IReadOnlyList<QueryTagNode> tags)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var paths = new Dictionary<int, string>();
        foreach (var node in tags)
        {
            var parts = new List<string> { node.Name };
            var parent = node.ParentId;
            var steps = 0;
            while (parent is int id && byId.TryGetValue(id, out var up) && steps++ < 64)
            {
                parts.Add(up.Name);
                parent = up.ParentId;
            }

            parts.Reverse();
            paths[node.Id] = string.Join(QueryTagReadJoin.PathSeparator, parts);
        }

        return paths;
    }

    public static QueryTagUsageResult Build(
        QueryTagRollupData data, IReadOnlyList<(int Id, string Name)> scope, bool allServers, int? tagFilter, int limit = 200)
    {
        var tables = new List<QueryTagUsageTable>();
        var notServed = new List<string>();
        var anyUnfiltered = data.Deadlocks.Count > 0;
        var anyData = tagFilter is null
            ? data.Deadlocks.Count > 0
            : data.Deadlocks.Any(d => d.ProcessesByTag.Keys.Any(k => IsUnder(k, tagFilter.Value, data.Tags)));

        foreach (var section in Additive)
        {
            var facts = data.Sections.GetValueOrDefault(section.Name) ?? [];
            anyUnfiltered |= facts.Count > 0;
            anyData |= tagFilter is null ? facts.Count > 0 : facts.Any(f => f.TagId is int id && IsUnder(id, tagFilter.Value, data.Tags));
            data.Unavailable.TryGetValue(section.Name, out var why);
            if (why is not null)
            {
                notServed.Add($"{section.Name} is unavailable: {why}");
            }

            var block = Wrap(section, facts, scope, allServers, data.Tags, tagFilter, limit, why, why is null && section == QueryTagRollupSection.QueryStore ? data.Refused : null);
            if (data.Notes.TryGetValue(section.Name, out var note))
            {
                block["note"] = note;
                /* #6794: in the all-servers view each server's own table carries the note too, not only the fleet total. */
                if (block.GetValueOrDefault("servers") is IEnumerable<object> perServer)
                {
                    foreach (var server in perServer.OfType<Dictionary<string, object?>>())
                    {
                        server["note"] = note;
                    }
                }
            }

            Add(tables, block, section);
        }

        data.Unavailable.TryGetValue(QueryTagRollupSection.Deadlocks.Name, out var deadlockWhy);
        if (deadlockWhy is not null)
        {
            notServed.Add($"{QueryTagRollupSection.Deadlocks.Name} is unavailable: {deadlockWhy}");
        }

        Add(tables, WrapDeadlocks(data.Deadlocks, scope, allServers, data.Tags, tagFilter, limit, data.ScanTruncated, deadlockWhy), QueryTagRollupSection.Deadlocks);

        string? message = null;
        if (tagFilter is int tagId && !anyData && anyUnfiltered)
        {
            message = $"The window held resource use, but none of it is under query tag '{data.Tags.First(t => t.Id == tagId).Name}', so every section is empty for that tag. Choose All tags to see what the window holds.";
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

    private static bool IsUnder(int tagId, int ancestor, IReadOnlyList<QueryTagNode> tags)
    {
        var byId = tags.ToDictionary(t => t.Id);
        return byId.ContainsKey(tagId) && DarlingQueryTagRollupReader.IsUnder(tagId, ancestor, byId);
    }

    private static Dictionary<string, object?> Wrap(
        QueryTagRollupSection section, List<QueryTagRollupFact> facts, IReadOnlyList<(int Id, string Name)> scope, bool allServers,
        IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit, string? unavailableWhy, IReadOnlyDictionary<int, string>? refused)
    {
        var status = unavailableWhy is not null ? "unavailable" : facts.Count == 0 ? "empty" : "ok";
        var message = unavailableWhy ?? (facts.Count == 0 ? "No rows in the window." : null);
        if (!allServers)
        {
            var one = facts.Count == 0
                ? DarlingQueryTagRollupReader.Empty(section, status, message ?? string.Empty, tagFilter)
                : DarlingQueryTagRollupReader.Assemble(section, facts, tags, tagFilter, limit);
            if (refused is not null && refused.TryGetValue(scope[0].Id, out var why))
            {
                one["status"] = "unavailable";
                one["message"] = "Not readable for this server: " + why + ".";
            }

            return one;
        }

        var servers = scope.Select(s =>
        {
            var mine = facts.Where(f => f.ServerId == s.Id).ToList();
            var block = refused is not null && refused.TryGetValue(s.Id, out var why)
                ? DarlingQueryTagRollupReader.Empty(section, "unavailable", "Not readable for this server: " + why + ".", tagFilter)
                : mine.Count == 0
                    ? DarlingQueryTagRollupReader.Empty(section, status == "ok" ? "empty" : status, message ?? "No rows in the window for this server.", tagFilter)
                    : DarlingQueryTagRollupReader.Assemble(section, mine, tags, tagFilter, limit);
            block["server"] = s.Name;
            return (object)block;
        }).ToList();
        return new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["status"] = status,
            ["message"] = message,
            ["servers"] = servers,
            ["fleet_total"] = DarlingQueryTagRollupReader.FleetTotal(section, facts, tags, tagFilter),
        };
    }

    private static Dictionary<string, object?> WrapDeadlocks(
        List<QueryTagDeadlockFact> facts, IReadOnlyList<(int Id, string Name)> scope, bool allServers,
        IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit, bool scanTruncated, string? unavailableWhy)
    {
        var section = QueryTagRollupSection.Deadlocks;
        var status = unavailableWhy is not null ? "unavailable" : facts.Count == 0 ? "empty" : "ok";
        var message = unavailableWhy ?? (facts.Count == 0 ? "No deadlock graphs in the window." : null);

        Dictionary<string, object?> One(IReadOnlyList<QueryTagDeadlockFact> mine)
        {
            if (mine.Count > 0)
            {
                return DarlingQueryTagRollupReader.AssembleDeadlocks(mine, tags, tagFilter, limit);
            }

            return DarlingQueryTagRollupReader.Empty(section, status == "ok" ? "empty" : status, message ?? "No deadlock graphs in the window.", tagFilter);
        }

        if (!allServers)
        {
            var block = One(facts);
            block["note"] = scanTruncated ? "The newest deadlock graphs only: the window held more than the read's ceiling." : null;
            return block;
        }

        var servers = scope.Select(s =>
        {
            var block = One(facts.Where(f => f.ServerId == s.Id).ToList());
            block["server"] = s.Name;
            return (object)block;
        }).ToList();
        return new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["status"] = status,
            ["message"] = message,
            ["note"] = "A deadlock is counted once for each tag among its processes, so the deadlock rows do not add up to the total."
                + (scanTruncated ? " The newest deadlock graphs only: the window held more than the read's ceiling." : ""),
            ["servers"] = servers,
            ["fleet_total"] = DarlingQueryTagRollupReader.FleetDeadlockTotal(facts, tags, tagFilter),
        };
    }

    private static void Add(List<QueryTagUsageTable> tables, Dictionary<string, object?> block, QueryTagRollupSection section)
    {
        if (block.TryGetValue("servers", out var servers) && servers is IEnumerable<object> list)
        {
            foreach (var server in list.OfType<Dictionary<string, object?>>())
            {
                tables.Add(One(server, section, $"{section.Name} - {server["server"]}"));
            }

            if (block.TryGetValue("fleet_total", out var fleet) && fleet is Dictionary<string, object> figures)
            {
                tables.Add(new QueryTagUsageTable(section.Name, $"{section.Name} - all servers",
                    block["status"]?.ToString() ?? "ok", block.GetValueOrDefault("message") as string, Note(block), Columns(section),
                    [new QueryTagUsageRow("total", "Total (all servers)", Cells(figures, section))]));
            }

            return;
        }

        tables.Add(One(block, section, section.Name));
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

    private static List<string> Columns(QueryTagRollupSection section) => ["Tag", .. section.Measures.Select(m => m.Name)];

    private static List<string> Cells(Dictionary<string, object> figures, QueryTagRollupSection section) =>
        section.Measures.Select(m => Convert.ToDouble(figures[m.Name], CultureInfo.InvariantCulture).ToString("#,0.###", CultureInfo.InvariantCulture)).ToList();
}
