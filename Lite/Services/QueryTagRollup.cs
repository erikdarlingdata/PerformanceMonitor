/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The shaping half of the Lite resource-use-by-tag roll-up (#5992): grouped facts in, tag rows (own and subtree figure), an
/// <c>untagged</c> row and a section total out. Pure, so the tree maths is the same for every section, for one server and for the
/// all-servers view, and the Lite UI (#5993) can call it. The output keys match Darling's reader (#5983). Sections are never
/// added together: each is assembled from its own source's facts only.
/// </summary>
public static class QueryTagRollup
{
    /// <summary>The caller key of one process or blocking side, as the sighting hook builds it: application, login, host, database joined by U+001F.</summary>
    public static string CallerKey(string? application, string? login, string? host, string? database) =>
        (application ?? string.Empty) + '\u001F' + (login ?? string.Empty) + '\u001F' + (host ?? string.Empty) + '\u001F' + (database ?? string.Empty);

    public static double[] Combine(double[] a, double[] b, QueryTagRollupSection section)
    {
        var result = new double[a.Length];
        for (var i = 0; i < a.Length; i++)
        {
            result[i] = section.Measures[i].Peak ? Math.Max(a[i], b[i]) : a[i] + b[i];
        }

        return result;
    }

    private static IEnumerable<int> SelfAndAncestors(int tagId, Dictionary<int, QueryTagRow> byId)
    {
        var current = (int?)tagId;
        var guard = 0;
        while (current is int id && byId.TryGetValue(id, out var node) && guard++ < 64)
        {
            yield return id;
            current = node.ParentId;
        }
    }

    private static string PathOf(int tagId, Dictionary<int, QueryTagRow> byId) =>
        string.Join(QueryTagReadScope.PathSeparator, SelfAndAncestors(tagId, byId).Reverse().Select(id => byId[id].Name));

    private static Dictionary<string, object> Figures(double[] values, QueryTagRollupSection section)
    {
        var map = new Dictionary<string, object>();
        for (var i = 0; i < section.Measures.Count; i++)
        {
            map[section.Measures[i].Name] = Math.Round(values[i] / section.Measures[i].Scale, 3);
        }

        return map;
    }

    /// <summary>
    /// Shapes one additive section for one scope. Within the section the tag rows' own figures plus <c>untagged</c> add up to
    /// <c>total</c> (the map holds one tag per key). With <paramref name="tagFilter"/> the rows hold only that tag's subtree, there is
    /// no <c>untagged</c> row, and <c>total</c> is the filtered tag's subtree figure.
    /// </summary>
    public static Dictionary<string, object?> Assemble(
        QueryTagRollupSection section, IReadOnlyList<QueryTagRollupFact> facts, IReadOnlyList<QueryTagRow> tags, int? tagFilter, int limit)
    {
        var zero = new double[section.Measures.Count];
        var byId = tags.ToDictionary(t => t.Id);
        var own = new Dictionary<int, double[]>();
        var subtree = new Dictionary<int, double[]>();
        var untagged = zero;
        var total = zero;
        foreach (var fact in facts)
        {
            total = Combine(total, fact.Values, section);
            if (fact.TagId is not int tagId || !byId.ContainsKey(tagId))
            {
                untagged = Combine(untagged, fact.Values, section);
                continue;
            }

            own[tagId] = Combine(own.GetValueOrDefault(tagId) ?? zero, fact.Values, section);
            foreach (var ancestor in SelfAndAncestors(tagId, byId))
            {
                subtree[ancestor] = Combine(subtree.GetValueOrDefault(ancestor) ?? zero, fact.Values, section);
            }
        }

        return Shape(section, own, subtree, untagged, total, tags, tagFilter, limit);
    }

    /// <summary>
    /// The deadlock section. A deadlock counts once for each tag among its processes (and once as untagged when a process has none),
    /// so the rows do not add up to <c>total</c>; the output says so. A subtree figure counts a graph once however many processes sit under the tag.
    /// </summary>
    public static Dictionary<string, object?> AssembleDeadlocks(
        IReadOnlyList<QueryTagDeadlockFact> facts, IReadOnlyList<QueryTagRow> tags, int? tagFilter, int limit)
    {
        var section = QueryTagRollupSection.Deadlocks;
        var byId = tags.ToDictionary(t => t.Id);
        var own = new Dictionary<int, double[]>();
        var subtree = new Dictionary<int, double[]>();
        var untagged = new double[2];
        var total = new double[2];
        var unparsedGraphs = 0;
        foreach (var fact in facts)
        {
            if (fact.Unparsed)
            {
                unparsedGraphs++;
            }

            total[0] += 1;
            total[1] += fact.ProcessesByTag.Values.Sum() + fact.UntaggedProcesses;
            if (fact.UntaggedProcesses > 0)
            {
                untagged[0] += 1;
                untagged[1] += fact.UntaggedProcesses;
            }

            var reached = new HashSet<int>();
            foreach (var (tagId, count) in fact.ProcessesByTag)
            {
                if (!byId.ContainsKey(tagId))
                {
                    continue;
                }

                own.TryAdd(tagId, new double[2]);
                own[tagId][0] += 1;
                own[tagId][1] += count;
                foreach (var ancestor in SelfAndAncestors(tagId, byId))
                {
                    subtree.TryAdd(ancestor, new double[2]);
                    subtree[ancestor][1] += count;
                    if (reached.Add(ancestor))
                    {
                        subtree[ancestor][0] += 1;
                    }
                }
            }
        }

        var shaped = Shape(section, own, subtree, untagged, total, tags, tagFilter, limit);
        /* Graphs the section counts that gave no process rows: in total, in no tag row and in no untagged figure. */
        shaped["unparsed_graphs"] = unparsedGraphs;
        shaped["rows_add_up_to_total"] = false;
        shaped["note"] = "A deadlock is counted once for each tag among its processes, so the deadlock rows do not add up to the total.";
        return shaped;
    }

    private static Dictionary<string, object?> Shape(
        QueryTagRollupSection section, Dictionary<int, double[]> own, Dictionary<int, double[]> subtree,
        double[] untagged, double[] total, IReadOnlyList<QueryTagRow> tags, int? tagFilter, int limit)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var orderIndex = section.Measures.ToList().FindIndex(m => m.Name == section.OrderMeasure);
        var candidates = subtree.Keys
            .Where(id => tagFilter is not int filter || SelfAndAncestors(id, byId).Contains(filter))
            .OrderByDescending(id => subtree[id][orderIndex])
            .ThenBy(id => byId[id].Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(id => id)
            .ToList();
        var page = candidates.Take(limit).Select(id => new Dictionary<string, object?>
        {
            ["tag_id"] = id,
            ["tag"] = byId[id].Name,
            ["path"] = PathOf(id, byId),
            ["parent_id"] = byId[id].ParentId,
            ["own"] = Figures(own.GetValueOrDefault(id) ?? new double[section.Measures.Count], section),
            ["subtree"] = Figures(subtree[id], section),
        }).ToList();

        var result = new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["status"] = "ok",
            ["order"] = section.OrderMeasure + "_desc",
            ["tag_rows"] = page,
            ["tag_rows_total"] = candidates.Count,
            ["truncated"] = candidates.Count > limit,
        };
        if (tagFilter is int tagId)
        {
            result["total_scope"] = "tag_subtree";
            result["total"] = Figures(subtree.GetValueOrDefault(tagId) ?? new double[section.Measures.Count], section);
        }
        else
        {
            result["untagged"] = Figures(untagged, section);
            result["total"] = Figures(total, section);
        }

        return result;
    }

    /// <summary>A section with no rows in the window: all figures zero, status <c>empty</c>.</summary>
    public static Dictionary<string, object?> Empty(QueryTagRollupSection section, string message, string status = "empty", int? tagFilter = null)
    {
        var zeros = Figures(new double[section.Measures.Count], section);
        var result = new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["status"] = status,
            ["message"] = message,
            ["tag_rows"] = new List<object>(),
            ["tag_rows_total"] = 0,
            ["truncated"] = false,
        };

        /* Under a tag filter there is no untagged row and the total is the tag's subtree, as in Shape. */
        if (tagFilter is not null)
        {
            result["total_scope"] = "tag_subtree";
        }
        else
        {
            result["untagged"] = zeros;
        }

        result["total"] = zeros;
        return result;
    }

    /// <summary>True when <paramref name="tagId"/> is the filtered tag or sits under it. With no filter every fact counts.</summary>
    public static bool IsUnderTag(int? tagId, IReadOnlyList<QueryTagRow> tags, int? tagFilter)
    {
        if (tagFilter is not int filter)
        {
            return true;
        }

        var byId = tags.ToDictionary(t => t.Id);
        return tagId is int id && byId.ContainsKey(id) && SelfAndAncestors(id, byId).Contains(filter);
    }

    /// <summary>
    /// The totals across a section's facts, for the fleet line. With a tag filter it is the sum of the per-server tagged totals (the
    /// facts under the tag), the same predicate each server's own <c>total</c> uses; untagged and other tags' facts are left out.
    /// </summary>
    public static Dictionary<string, object> FleetTotal(
        QueryTagRollupSection section, IReadOnlyList<QueryTagRollupFact> facts, IReadOnlyList<QueryTagRow> tags, int? tagFilter)
    {
        var total = new double[section.Measures.Count];
        foreach (var fact in facts.Where(f => IsUnderTag(f.TagId, tags, tagFilter)))
        {
            total = Combine(total, fact.Values, section);
        }

        return Figures(total, section);
    }

    /// <summary>The fleet line of the deadlock section; with a tag filter, the graphs with a process under the tag and those processes.</summary>
    public static Dictionary<string, object> FleetDeadlockTotal(IReadOnlyList<QueryTagDeadlockFact> facts, IReadOnlyList<QueryTagRow> tags, int? tagFilter)
    {
        if (tagFilter is null)
        {
            return Figures([facts.Count, facts.Sum(f => (double)(f.ProcessesByTag.Values.Sum() + f.UntaggedProcesses))], QueryTagRollupSection.Deadlocks);
        }

        double graphs = 0, processes = 0;
        foreach (var fact in facts)
        {
            var under = fact.ProcessesByTag.Where(p => IsUnderTag(p.Key, tags, tagFilter)).Sum(p => (double)p.Value);
            if (under > 0)
            {
                graphs++;
                processes += under;
            }
        }

        return Figures([graphs, processes], QueryTagRollupSection.Deadlocks);
    }

    /// <summary>One section for one server's facts, or for the all-servers view (a block per server and a <c>fleet_total</c>).</summary>
    public static Dictionary<string, object?> Section(
        QueryTagRollupSection section, IReadOnlyList<QueryTagRollupFact> facts, IReadOnlyList<(int Id, string Name)> scope,
        bool allServers, IReadOnlyList<QueryTagRow> tags, int? tagFilter, int limit, string? unavailableReason = null)
    {
        Dictionary<string, object?> One(IReadOnlyList<QueryTagRollupFact> mine) => unavailableReason is not null
            ? Empty(section, unavailableReason, "unavailable", tagFilter)
            : mine.Count == 0
                ? Empty(section, "No rows in the window.", "empty", tagFilter)
                : Assemble(section, mine, tags, tagFilter, limit);

        if (!allServers)
        {
            var block = One(facts.ToList());
            block["source"] = SourceOf(section);
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
            ["source"] = SourceOf(section),
            ["status"] = unavailableReason is not null ? "unavailable" : facts.Count == 0 ? "empty" : "ok",
            ["message"] = unavailableReason,
            ["servers"] = servers,
            ["fleet_total"] = FleetTotal(section, facts, tags, tagFilter),
        };
    }

    /// <summary>The store table or view a section reads, named in a section's <c>source</c> and in its unavailable reason.</summary>
    public static string SourceOf(QueryTagRollupSection section) => section.Name switch
    {
        "plan_cache" => "v_query_stats",
        "query_store" => "v_query_store_stats",
        "procedures" => "v_procedure_stats",
        "blocking_reports" => "blocked_process_reports",
        "blocking_snapshots" => "v_dmv_blocking_snapshots",
        _ => "deadlocks",
    };

    /// <summary>What a section's data is called in "This store has no ... data yet." (#6831): words, never the store's view or table name.
    /// The Darling Viewer says the same words for the same missing sources.</summary>
    public static string SourceWords(QueryTagRollupSection section) => section.Name switch
    {
        "plan_cache" => "query stats",
        "query_store" => "Query Store",
        "procedures" => "procedure stats",
        "blocking_reports" => "blocked process report",
        "blocking_snapshots" => "blocking snapshot",
        _ => "deadlock",
    };

    /// <summary>The deadlock section for one server or the all-servers view.</summary>
    public static Dictionary<string, object?> DeadlockSection(
        IReadOnlyList<QueryTagDeadlockFact> facts, IReadOnlyList<(int Id, string Name)> scope, bool allServers,
        IReadOnlyList<QueryTagRow> tags, int? tagFilter, int limit, bool scanTruncated, string? unavailableReason = null)
    {
        Dictionary<string, object?> One(IReadOnlyList<QueryTagDeadlockFact> mine)
        {
            if (mine.Count > 0)
            {
                return AssembleDeadlocks(mine, tags, tagFilter, limit);
            }

            var empty = unavailableReason is not null
                ? Empty(QueryTagRollupSection.Deadlocks, unavailableReason, "unavailable", tagFilter)
                : Empty(QueryTagRollupSection.Deadlocks, "No deadlock graphs in the window.", "empty", tagFilter);
            empty["unparsed_graphs"] = 0;
            return empty;
        }

        if (!allServers)
        {
            var block = One(facts.ToList());
            block["source"] = SourceOf(QueryTagRollupSection.Deadlocks);
            block["scan_truncated"] = scanTruncated;
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
            ["section"] = QueryTagRollupSection.Deadlocks.Name,
            ["source"] = SourceOf(QueryTagRollupSection.Deadlocks),
            ["status"] = unavailableReason is not null ? "unavailable" : facts.Count == 0 ? "empty" : "ok",
            ["message"] = unavailableReason ?? (facts.Count == 0 ? "No deadlock graphs in the window." : null),
            ["scan_truncated"] = scanTruncated,
            ["unparsed_graphs"] = facts.Count(f => f.Unparsed),
            ["rows_add_up_to_total"] = false,
            ["note"] = "A deadlock is counted once for each tag among its processes, so the deadlock rows do not add up to the total.",
            ["servers"] = servers,
            ["fleet_total"] = FleetDeadlockTotal(facts, tags, tagFilter),
        };
    }
}
