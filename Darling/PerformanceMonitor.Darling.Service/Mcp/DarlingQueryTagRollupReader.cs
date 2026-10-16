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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>One tag of the tree.</summary>
internal sealed record QueryTagNode(int Id, string Name, int? ParentId);

/// <summary>One source's grouped facts: a server, a tag (null = no map row) and the section's measures in statement order.</summary>
internal sealed record QueryTagRollupFact(int ServerId, int? TagId, double[] Values);

/// <summary>One deadlock graph, reduced to the tags of its processes: tag (null = untagged) to the number of processes holding it.</summary>
internal sealed record QueryTagDeadlockFact(int ServerId, IReadOnlyDictionary<int, int> ProcessesByTag, int UntaggedProcesses);

/// <summary>
/// The executing half of <c>get_resource_use_by_tag</c> (#5983): runs <see cref="QueryTagRollupSql"/> and shapes the grouped facts
/// into tag rows (own figure and subtree figure), an <c>untagged</c> row and a section total. The shaping is pure
/// (<see cref="Assemble"/>), so the tree maths is the same for every section and for the fleet. Sections are never added
/// together: each is assembled from its own source's facts only.
/// </summary>
internal static class DarlingQueryTagRollupReader
{
    /// <summary>Deadlock graphs read per call; a window with more sets <c>scan_truncated</c>, and the newest are kept.</summary>
    internal const int DeadlockScanCeiling = 2000;

    private static NpgsqlCommand Command(NpgsqlDataSource postgres, string sql, int[] serverIds, DateTime startUtc, DateTime endUtc)
    {
        var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        return command;
    }

    internal static async Task<List<QueryTagNode>> ReadTagsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var tags = new List<QueryTagNode>();
        await using var command = postgres.CreateCommand(QueryTagRollupSql.TagTreeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            tags.Add(new QueryTagNode(reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetInt32(2)));
        }

        return tags;
    }

    /// <summary>
    /// Runs one grouped statement. <paramref name="ceiling"/> is the hourly rollup's materialisation ceiling ($4) or null for none;
    /// a statement without <see cref="QueryTagRollupSql.CeilingPlaceholder"/> ignores it.
    /// </summary>
    internal static async Task<List<QueryTagRollupFact>> ReadFactsAsync(
        NpgsqlDataSource postgres, string sql, int measureCount, int[] serverIds, DateTime startUtc, DateTime endUtc,
        DateTime? ceiling, CancellationToken cancellationToken)
    {
        var text = sql.Replace(QueryTagRollupSql.CeilingPlaceholder, ceiling is null ? string.Empty : QueryTagRollupSql.CeilingClause, StringComparison.Ordinal);
        var facts = new List<QueryTagRollupFact>();
        await using var command = Command(postgres, text, serverIds, startUtc, endUtc);
        if (ceiling is not null && sql.Contains(QueryTagRollupSql.CeilingPlaceholder, StringComparison.Ordinal))
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(ceiling.Value, DateTimeKind.Unspecified) });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var values = new double[measureCount];
            for (var i = 0; i < measureCount; i++)
            {
                values[i] = reader.IsDBNull(2 + i) ? 0 : Convert.ToDouble(reader.GetValue(2 + i), CultureInfo.InvariantCulture);
            }

            facts.Add(new QueryTagRollupFact(reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetInt32(1), values));
        }

        return facts;
    }

    /// <summary>The deadlock graphs of the window with each process looked up in the caller-key map (null tag = no map row).</summary>
    internal static async Task<(List<QueryTagDeadlockFact> Facts, bool ScanTruncated)> ReadDeadlocksAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken)
    {
        var map = new Dictionary<(int Server, string Key), int>();
        await using (var mapCommand = postgres.CreateCommand(QueryTagRollupSql.CallerMapSql))
        {
            mapCommand.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            mapCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
            await using var mapReader = await mapCommand.ExecuteReaderAsync(cancellationToken);
            while (await mapReader.ReadAsync(cancellationToken))
            {
                map[(mapReader.GetInt32(0), mapReader.GetString(1))] = mapReader.GetInt32(2);
            }
        }

        var facts = new List<QueryTagDeadlockFact>();
        await using var command = Command(postgres, QueryTagRollupSql.DeadlockGraphsSql, serverIds, startUtc, endUtc);
        /* The ceiling + 1 as the fetch: the extra row is the observed truncation signal. */
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DeadlockScanCeiling + 1 });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var read = 0;
        var truncated = false;
        while (await reader.ReadAsync(cancellationToken))
        {
            if (++read > DeadlockScanCeiling)
            {
                truncated = true;
                break;
            }

            var serverId = reader.GetInt32(0);
            var xml = reader.IsDBNull(2) ? null : reader.GetString(2);
            DateTime? when = reader.IsDBNull(3) ? null : reader.GetDateTime(3);
            var byTag = new Dictionary<int, int>();
            var untagged = 0;
            foreach (var p in DarlingDeadlockProcessRows.Parse(xml, when))
            {
                var key = QueryTagRollupSql.CallerKey(p.ClientApp, p.LoginName, p.HostName, p.DatabaseName);
                if (map.TryGetValue((serverId, key), out var tagId))
                {
                    byTag[tagId] = byTag.GetValueOrDefault(tagId) + 1;
                }
                else
                {
                    untagged++;
                }
            }

            if (byTag.Count + untagged > 0)
            {
                facts.Add(new QueryTagDeadlockFact(serverId, byTag, untagged));
            }
        }

        return (facts, truncated);
    }

    private static double[] Combine(double[] a, double[] b, QueryTagRollupSection section)
    {
        var result = new double[a.Length];
        for (var i = 0; i < a.Length; i++)
        {
            result[i] = section.Measures[i].Peak ? Math.Max(a[i], b[i]) : a[i] + b[i];
        }

        return result;
    }

    private static IEnumerable<int> SelfAndAncestors(int tagId, Dictionary<int, QueryTagNode> byId)
    {
        var current = (int?)tagId;
        var guard = 0;
        while (current is int id && byId.TryGetValue(id, out var node) && guard++ < 64)
        {
            yield return id;
            current = node.ParentId;
        }
    }

    private static string PathOf(int tagId, Dictionary<int, QueryTagNode> byId) =>
        string.Join(" > ", SelfAndAncestors(tagId, byId).Reverse().Select(id => byId[id].Name));

    /// <summary>Whether <paramref name="tagId"/> is <paramref name="ancestor"/> or sits under it.</summary>
    internal static bool IsUnder(int tagId, int ancestor, Dictionary<int, QueryTagNode> byId) =>
        SelfAndAncestors(tagId, byId).Contains(ancestor);

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
    /// Shapes one section for one scope (a server, or the fleet's totals). Within the section the tag rows' own figures plus
    /// <c>untagged</c> add up to <c>total</c> (the map holds one tag per key). With <paramref name="tagFilter"/> the rows hold only
    /// that tag's subtree, there is no <c>untagged</c> row, and <c>total</c> is the filtered tag's subtree figure.
    /// <paramref name="limit"/> bounds the tag rows; <c>truncated</c> says the section held more.
    /// </summary>
    internal static Dictionary<string, object?> Assemble(
        QueryTagRollupSection section, IReadOnlyList<QueryTagRollupFact> facts, IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit)
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
    /// The deadlock section. A deadlock is counted once for each tag among its processes (and once as untagged when a process has
    /// none), so the rows do not add up to <c>total</c>; the output says so. A subtree figure counts a graph once however many
    /// processes sit under the tag.
    /// </summary>
    internal static Dictionary<string, object?> AssembleDeadlocks(
        IReadOnlyList<QueryTagDeadlockFact> facts, IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit)
    {
        var section = QueryTagRollupSection.Deadlocks;
        var byId = tags.ToDictionary(t => t.Id);
        var own = new Dictionary<int, double[]>();
        var subtree = new Dictionary<int, double[]>();
        var untagged = new double[2];
        var total = new double[2];
        foreach (var fact in facts)
        {
            var processes = fact.ProcessesByTag.Values.Sum() + fact.UntaggedProcesses;
            total[0] += 1;
            total[1] += processes;
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
        shaped["rows_add_up_to_total"] = false;
        shaped["note"] = "A deadlock is counted once for each tag among its processes, so the deadlock rows do not add up to the total.";
        return shaped;
    }

    private static Dictionary<string, object?> Shape(
        QueryTagRollupSection section, Dictionary<int, double[]> own, Dictionary<int, double[]> subtree,
        double[] untagged, double[] total, IReadOnlyList<QueryTagNode> tags, int? tagFilter, int limit)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var orderIndex = section.Measures.ToList().FindIndex(m => m.Name == section.OrderMeasure);
        var candidates = subtree.Keys
            .Where(id => tagFilter is not int filter || IsUnder(id, filter, byId))
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
    internal static Dictionary<string, object?> Empty(QueryTagRollupSection section, string status, string message)
    {
        var zeros = Figures(new double[section.Measures.Count], section);
        return new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["status"] = status,
            ["message"] = message,
            ["tag_rows"] = new List<object>(),
            ["tag_rows_total"] = 0,
            ["truncated"] = false,
            ["untagged"] = zeros,
            ["total"] = zeros,
        };
    }

    /// <summary>The totals across a fleet's facts, for the fleet line of a section.</summary>
    internal static Dictionary<string, object> FleetTotal(QueryTagRollupSection section, IReadOnlyList<QueryTagRollupFact> facts)
    {
        var total = new double[section.Measures.Count];
        foreach (var fact in facts)
        {
            total = Combine(total, fact.Values, section);
        }

        return Figures(total, section);
    }

    /// <summary>The fleet line of the deadlock section: graphs and processes across the servers.</summary>
    internal static Dictionary<string, object> FleetDeadlockTotal(IReadOnlyList<QueryTagDeadlockFact> facts) =>
        Figures([facts.Count, facts.Sum(f => (double)(f.ProcessesByTag.Values.Sum() + f.UntaggedProcesses))], QueryTagRollupSection.Deadlocks);
}
