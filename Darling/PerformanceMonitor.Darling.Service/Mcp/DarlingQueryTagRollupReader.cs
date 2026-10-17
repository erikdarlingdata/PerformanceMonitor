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
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>One tag of the tree.</summary>
internal sealed record QueryTagNode(int Id, string Name, int? ParentId);

/// <summary>One source's grouped facts: a server, a tag (null = no map row) and the section's measures in statement order.</summary>
internal sealed record QueryTagRollupFact(int ServerId, int? TagId, double[] Values);

/// <summary>One deadlock graph, reduced to the tags of its processes: tag to the number of processes holding it, plus the untagged ones. <paramref name="Unparsed"/>: the graph was NULL or yielded no process rows, so it counts as a deadlock and as nothing else.</summary>
internal sealed record QueryTagDeadlockFact(int ServerId, IReadOnlyDictionary<int, int> ProcessesByTag, int UntaggedProcesses, bool Unparsed = false);

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

    /// <summary>
    /// How one stats section (plan cache or procedures) is served and what it says about how far it reached. <paramref name="Hourly"/>: the
    /// io rollup serves it, cut at <paramref name="Ceiling"/>. <paramref name="Disclosure"/> is merged into the section: on the hourly
    /// branch <c>window_end</c> (the served end) and <c>window_edges_note</c>; on the raw branch <c>effective_start</c>, <c>window_truncated</c>
    /// and <c>truncation_note</c>, the notice <c>get_top_queries_by_cpu</c> carries.
    /// </summary>
    internal sealed record StatsRoute(bool Hourly, DateTime? Ceiling, Dictionary<string, object?> Disclosure);

    /// <summary>
    /// Routes the plan-cache and procedures sections independently (their rollups, floors and tiers are independent) and builds each
    /// one's reach disclosure (#5983). The hourly rollup serves a section only when the retention tier left raw and the io route reaches
    /// the window's start; <c>tier Hourly</c> with <c>route Raw</c> falls to raw over a window raw may not hold, and says so. The tier is
    /// judged against the wall clock (retention is wall-clock age), the window's own end is the anchor.
    /// </summary>
    internal static async Task<(StatsRoute Plan, StatsRoute Procedures)> ChooseStatsRoutesAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime windowStart, DateTime windowEnd, CancellationToken cancellationToken)
    {
        var (rollups, coverage) = await ComposeStoreAvailability.GetRollupsAsync(postgres, cancellationToken);
        var now = DateTime.UtcNow;

        async Task<StatsRoute> RouteAsync(
            HourlyRoute route, RetentionTier tier, string ioView, string rawSource, RawWindowFloor.Table floorTable)
        {
            var hourly = tier != RetentionTier.Raw && route == HourlyRoute.Io;
            if (hourly)
            {
                var ceiling = coverage.HourlyEndCeiling(ioView, windowStart);
                var (_, servedEnd) = HourlyWindowEdges.ServedSpan(windowStart, null, windowEnd, ceiling);
                var edges = HourlyWindowEdges.Note(windowStart, null, windowEnd, ceiling);
                return new StatsRoute(true, ceiling, new Dictionary<string, object?>
                {
                    ["tier"] = "hourly",
                    ["window_end"] = servedEnd is DateTime end ? McpHelpers.FormatEffectiveStart(end) : null,
                    ["window_edges_note"] = string.IsNullOrEmpty(edges) ? null : edges,
                });
            }

            /* The raw tier: the reach is what raw holds per server (the probe get_top_queries_by_cpu runs), and the section carries the
               latest floor among the servers in scope, because from there every server has coverage. */
            DateTime? latest = null;
            var truncated = false;
            foreach (var serverId in serverIds)
            {
                var floor = await RawWindowFloor.GetAsync(
                    postgres, floorTable, serverId, windowStart, windowEnd, McpCommandDeadlines.ReadSeconds, cancellationToken);
                var effective = RawWindowFloor.EffectiveStart(floor, windowStart);
                if (latest is null || effective > latest)
                {
                    latest = effective;
                }

                truncated |= RawWindowFloor.IsTruncated(floor, windowStart);
            }

            var notice = ComposeStoreAvailability.BuildRetentionNotice(rawSource, ComposeRoute.Raw, windowStart, now, rollups, coverage);
            return new StatsRoute(false, null, new Dictionary<string, object?>
            {
                ["tier"] = "raw",
                ["effective_start"] = McpHelpers.FormatEffectiveStart(latest ?? windowStart),
                ["window_truncated"] = truncated,
                ["truncation_note"] = truncated ? notice : null,
            });
        }

        var plan = await RouteAsync(
            DarlingDataReader.ChooseQueriesHourlyRoute(rollups, coverage, TopRanking.Reads, windowStart),
            RetentionTierRouter.Resolve(now, windowStart, rollups.QueryGrainHourly, dailyAvailable: false,
                coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView)),
            TimescaleSupport.QueryStatsIoHourlyView, "query_stats", RawWindowFloor.Table.QueryStats);
        var procedures = await RouteAsync(
            DarlingDataReader.ChooseProceduresHourlyRoute(rollups, coverage, TopRanking.Reads, windowStart),
            RetentionTierRouter.Resolve(now, windowStart, rollups.ProcedureGrainHourly, dailyAvailable: false,
                coverage.For(TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView)),
            TimescaleSupport.ProcedureStatsIoHourlyView, "procedure_stats", RawWindowFloor.Table.ProcedureStats);
        return (plan, procedures);
    }

    /// <summary>
    /// The Query Store gate, per server: the interval table is read only where the same gate every other reader of it applies lets it be
    /// (<see cref="DarlingDataReader.QueryStoreTopMayReadTable"/>, then <see cref="QueryStoreIntervalWide.ResolveReadAsync"/>: coverage exists,
    /// no pending batch, the window is long enough). There is no raw fallback and that is deliberate: raw <c>query_store_stats</c> re-collects
    /// an open interval every pass, so summing raw counts one interval many times; <c>get_query_store_top</c> dedupes raw, a roll-up that
    /// sums cannot. A server the gate refuses is reported <c>unavailable</c> with the reason, never <c>empty</c> (#3553). Servers the gate
    /// allows come back with the start the table provably holds for them.
    /// </summary>
    internal static async Task<(Dictionary<int, DateTime> ReadStart, Dictionary<int, string> Refused)> ResolveQueryStoreGateAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime windowStart, DateTime windowEnd, ILogger? logger, CancellationToken cancellationToken)
    {
        var allowed = new Dictionary<int, DateTime>();
        var refused = new Dictionary<int, string>();
        if (!DarlingDataReader.QueryStoreTopMayReadTable(windowStart, windowEnd))
        {
            var why = $"the window is shorter than {DarlingDataReader.QueryStoreTopMinWindow.TotalMinutes:0} minutes, which the Query Store interval table is not read for";
            foreach (var serverId in serverIds)
            {
                refused[serverId] = why;
            }

            return (allowed, refused);
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        foreach (var serverId in serverIds)
        {
            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, windowStart, windowEnd, windowEnd, DarlingDataReader.QueryStoreTopMinWindow,
                McpCommandDeadlines.ReadSeconds, logger, cancellationToken);
            if (plan.UseTable)
            {
                allowed[serverId] = plan.ReadStart;
            }
            else
            {
                refused[serverId] = plan.DecisionFailed
                    ? "the interval table's coverage could not be decided just now"
                    : "the interval table does not provably hold this window for the server yet (no coverage row, a pending batch, or the window ends before the composed range)";
            }
        }

        return (allowed, refused);
    }

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
        NpgsqlDataSource postgres, string sql, QueryTagRollupSection section, int[] serverIds, DateTime startUtc, DateTime endUtc,
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
        /* The measure columns are aliased with the section's measure names in every statement, and read by name: the statement's column
           order and QueryTagRollupSection.Measures are no longer two copies of one ordering. */
        var ordinals = section.Measures.Select(m => reader.GetOrdinal(m.Name)).ToArray();
        while (await reader.ReadAsync(cancellationToken))
        {
            var values = new double[ordinals.Length];
            for (var i = 0; i < ordinals.Length; i++)
            {
                values[i] = reader.IsDBNull(ordinals[i]) ? 0 : Convert.ToDouble(reader.GetValue(ordinals[i]), CultureInfo.InvariantCulture);
            }

            facts.Add(new QueryTagRollupFact(reader.GetInt32(reader.GetOrdinal("server_id")), reader.IsDBNull(reader.GetOrdinal("tag_id")) ? null : reader.GetInt32(reader.GetOrdinal("tag_id")), values));
        }

        return facts;
    }

    /// <summary>
    /// The deadlock graphs of the window with each process looked up in the caller-key map (null tag = no map row). A graph that is NULL or
    /// yields no process rows still becomes a fact (<see cref="QueryTagDeadlockFact.Unparsed"/>), so it counts in the section's total
    /// (#3553: a deadlock the store holds is not an absent one), and the failure is logged. The map read is bound to the servers in the
    /// call and the caller keys the window's processes actually carry, not to every caller row of the fleet.
    /// </summary>
    internal static async Task<(List<QueryTagDeadlockFact> Facts, bool ScanTruncated)> ReadDeadlocksAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, ILogger? logger, CancellationToken cancellationToken)
    {
        var graphs = new List<(int ServerId, List<(string Key, DeadlockProcessInfo Process)> Processes, bool Unparsed)>();
        var truncated = false;
        await using (var command = Command(postgres, QueryTagRollupSql.DeadlockGraphsSql, serverIds, startUtc, endUtc))
        {
            /* The ceiling + 1 as the fetch: the extra row is the observed truncation signal. */
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DeadlockScanCeiling + 1 });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var read = 0;
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
                var processes = DarlingDeadlockProcessRows.Parse(xml, when)
                    .Select(p => (QueryTagRollupSql.CallerKey(p.ClientApp, p.LoginName, p.HostName, p.DatabaseName), p))
                    .ToList();
                var unparsed = processes.Count == 0;
                if (unparsed)
                {
                    logger?.LogWarning(
                        "get_resource_use_by_tag: deadlock {DeadlockId} on server {ServerId} has {Graph} deadlock graph, so it counts in the total with no process rows.",
                        reader.GetValue(1), serverId, xml is null ? "a NULL" : "an unreadable or process-less");
                }

                graphs.Add((serverId, processes, unparsed));
            }
        }

        var keys = graphs.SelectMany(g => g.Processes.Select(p => p.Key)).Distinct(StringComparer.Ordinal).ToArray();
        var map = new Dictionary<(int Server, string Key), int>();
        if (keys.Length > 0)
        {
            await using var mapCommand = postgres.CreateCommand(QueryTagRollupSql.CallerMapSql);
            mapCommand.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            mapCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
            mapCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = keys });
            await using var mapReader = await mapCommand.ExecuteReaderAsync(cancellationToken);
            while (await mapReader.ReadAsync(cancellationToken))
            {
                map[(mapReader.GetInt32(0), mapReader.GetString(1))] = mapReader.GetInt32(2);
            }
        }

        var facts = new List<QueryTagDeadlockFact>();
        foreach (var (serverId, processes, unparsed) in graphs)
        {
            var byTag = new Dictionary<int, int>();
            var untagged = 0;
            foreach (var (key, _) in processes)
            {
                if (map.TryGetValue((serverId, key), out var tagId))
                {
                    byTag[tagId] = byTag.GetValueOrDefault(tagId) + 1;
                }
                else
                {
                    untagged++;
                }
            }

            facts.Add(new QueryTagDeadlockFact(serverId, byTag, untagged, unparsed));
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
        var unparsedGraphs = 0;
        foreach (var fact in facts)
        {
            if (fact.Unparsed)
            {
                unparsedGraphs++;
            }

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
        /* Beside total: graphs the section counts that gave no process rows (NULL, or a graph that will not parse). They sit in no tag row
           and in no untagged figure, so a total that exceeds rows plus untagged is explained here, not hidden. */
        shaped["unparsed_graphs"] = unparsedGraphs;
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

    /// <summary>
    /// A section with no rows in the window: all figures zero. Carries the keys <see cref="Shape"/> carries, so a quiet window and a busy one
    /// read the same way: under a <paramref name="tagFilter"/> there is no <c>untagged</c> row and <c>total_scope</c> says the total is the subtree's.
    /// </summary>
    internal static Dictionary<string, object?> Empty(QueryTagRollupSection section, string status, string message, int? tagFilter = null)
    {
        var zeros = Figures(new double[section.Measures.Count], section);
        var result = new Dictionary<string, object?>
        {
            ["section"] = section.Name,
            ["status"] = status,
            ["message"] = message,
            ["order"] = section.OrderMeasure + "_desc",
            ["tag_rows"] = new List<object>(),
            ["tag_rows_total"] = 0,
            ["truncated"] = false,
        };
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
