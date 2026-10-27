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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One tag of the tree.</summary>
public sealed record QueryTagNode(int Id, string Name, int? ParentId);

/// <summary>One source's grouped facts: a server, a tag (null = no map row) and the section's measures in statement order.</summary>
public sealed record QueryTagRollupFact(int ServerId, int? TagId, double[] Values);

/// <summary>One deadlock graph, reduced to the tags of its processes: tag to the number of processes holding it, plus the untagged ones. <paramref name="Unparsed"/>: the graph was NULL or yielded no process rows, so it counts as a deadlock and as nothing else.</summary>
public sealed record QueryTagDeadlockFact(int ServerId, IReadOnlyDictionary<int, int> ProcessesByTag, int UntaggedProcesses, bool Unparsed = false);

/// <summary>
/// The executing half of <c>get_resource_use_by_tag</c> (#5983): runs <see cref="QueryTagRollupSql"/> and shapes the grouped facts
/// into tag rows (own figure and subtree figure), an <c>untagged</c> row and a section total. The shaping is pure
/// (<see cref="Assemble"/>), so the tree maths is the same for every section and for the fleet. Sections are never added
/// together: each is assembled from its own source's facts only.
/// </summary>
public static class DarlingQueryTagRollupReader
{
    /// <summary>Deadlock graphs read per call; a window with more sets <c>scan_truncated</c>, and the newest are kept.</summary>
    public const int DeadlockScanCeiling = 2000;

    /// <summary>The read deadline (seconds) a caller that names none gets: the MCP surface's, kept so the tool's behaviour is unchanged by the move (#5988). The viewer passes its own.</summary>
    public const int DefaultReadSeconds = 75;

    /// <summary>The Query Store interval table is read only for a window at least this long (the MCP top read's rule, #3953). The same field as that read's: <see cref="QueryStoreIntervalWide.TopMinWindow"/>.</summary>
    public static readonly TimeSpan QueryStoreMinWindow = QueryStoreIntervalWide.TopMinWindow;

    /// <summary>The store schema version that has the interval table; pinned equal to the other readers' copies of it.</summary>
    private const int QueryStoreTableMinSchemaVersion = 145;

    private const string SetTransactionReadOnlySql = "SET TRANSACTION READ ONLY";

    /// <summary>Whether the Query Store interval table may be read for this window at all (the first half of the gate; the second is <see cref="QueryStoreIntervalWide.ResolveReadAsync"/>).</summary>
    public static bool QueryStoreMayReadTable(DateTime startUtc, DateTime endUtc) =>
        StorageVersion.SchemaVersion >= QueryStoreTableMinSchemaVersion && endUtc - startUtc >= QueryStoreMinWindow;

    /// <summary>What <see cref="ReadQueryStoreAsync"/> decided and read: the servers the gate allowed with the start the table provably holds
    /// for them, the servers it refused with why, the facts read for the allowed ones, and whether the interval table itself was missing.
    /// <paramref name="UsedHashHourly"/>: the fleet route read built hours from <c>collect.query_store_hash_hourly</c> (#6800), so the section's source is both tables.</summary>
    public readonly record struct QueryStoreRead(
        Dictionary<int, DateTime> Allowed, Dictionary<int, string> Refused, List<QueryTagRollupFact> Facts, bool TableMissing, bool UsedHashHourly = false);

    /// <summary>
    /// The Query Store gate and the read it authorizes, per server: the interval table is read only where the same gate every other reader of
    /// it applies lets it be (<see cref="QueryStoreMayReadTable"/>, then <see cref="QueryStoreIntervalWide.ResolveReadAsync"/>:
    /// coverage exists, no pending batch, the window is long enough). The gate's decision and the read share ONE connection in ONE read-only
    /// REPEATABLE READ transaction, as <c>get_query_store_top</c> does (<see cref="QueryStoreIntervalWide.ResolveReadAsync"/>'s contract): a compose
    /// pass that applies a batch between the decision and the read cannot be half-seen. There is no raw fallback and that is deliberate: raw
    /// <c>query_store_stats</c> re-collects an open interval every pass, so summing raw counts one interval many times; <c>get_query_store_top</c>
    /// dedupes raw, a roll-up that sums cannot. A server the gate refuses is reported <c>unavailable</c> with the reason, never <c>empty</c> (#3553).
    /// </summary>
    public static async Task<QueryStoreRead> ReadQueryStoreAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime windowStart, DateTime windowEnd, ILogger? logger, CancellationToken cancellationToken, int readSeconds = DefaultReadSeconds)
    {
        var allowed = new Dictionary<int, DateTime>();
        var refused = new Dictionary<int, string>();
        var facts = new List<QueryTagRollupFact>();
        if (!QueryStoreMayReadTable(windowStart, windowEnd))
        {
            var why = $"the window is under the {QueryStoreMinWindow.TotalHours:0}-hour minimum the Query Store interval table is read for (ask for hours_back of {QueryStoreMinWindow.TotalHours:0} or more)";
            foreach (var serverId in serverIds)
            {
                refused[serverId] = why;
            }

            return new QueryStoreRead(allowed, refused, facts, false);
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);
        await using (var readOnly = new NpgsqlCommand(SetTransactionReadOnlySql, connection) { Transaction = transaction, CommandTimeout = readSeconds })
        {
            await readOnly.ExecuteNonQueryAsync(cancellationToken);
        }

        /* #6807: the gate's per-server table floor in one statement for the servers that reach the floor step, and the store-wide raw
           floor once, inside this same snapshot, instead of one floor statement per server. Each server's answer is the one it got
           before. The statement runs under a savepoint, so if it fails or passes its short deadline only it is undone, and each server
           reads its own floor on the snapshot as before. */
        var storeWide = new QueryStoreIntervalWide.StoreWideInputsCache();
        storeWide.ReadTableFloorsTogether(serverIds, inTransaction: true);

        /* The gate decides per server, on the shared snapshot; the read is then ONE statement over every allowed server (#6794), each
           bound to its own ReadStart, so a server's rows are the rows a per-server statement would read. */
        foreach (var serverId in serverIds)
        {
            var plan = await QueryStoreIntervalWide.ResolveReadAsync(
                connection, serverId, windowStart, windowEnd, windowEnd, QueryStoreMinWindow,
                readSeconds, logger, legacyRows: QueryStoreIntervalWide.LegacyRows.Allow, cancellationToken, storeWide: storeWide);
            if (!plan.UseTable)
            {
                refused[serverId] = plan.DecisionFailed
                    ? "the interval table's coverage could not be decided just now"
                    : "the interval table does not provably hold this window for the server yet (no coverage row, a pending batch, or the window ends before the composed range)";
                continue;
            }

            allowed[serverId] = plan.ReadStart;
        }

        if (allowed.Count == 0)
        {
            return new QueryStoreRead(allowed, refused, facts, false);
        }

        return await ReadAllowedAsync(connection, allowed, refused, windowEnd, readSeconds, cancellationToken);
    }

    /// <summary>
    /// The read after the gate: the facts for the servers it allowed, each from its own read start, on the gate's connection and snapshot.
    /// Split out so a test can hand it servers with different read starts (the gate gives them only where a server's below-floor start is
    /// allowed, which takes a raw chunk, a collection log and a table floor per server) and see whether the batched commands pair each
    /// start, and each stale pair, with its own server (#6800 part 2, step 2b).
    /// </summary>
    internal static async Task<QueryStoreRead> ReadAllowedAsync(
        NpgsqlConnection connection, Dictionary<int, DateTime> allowed, Dictionary<int, string> refused, DateTime windowEnd, int readSeconds, CancellationToken cancellationToken)
    {
        var facts = new List<QueryTagRollupFact>();
        /* #6800 part 2: a fleet read takes the hours the hourly builder has done from collect.query_store_hash_hourly and the rest from the
           wide table (QueryTagRollupHashRoute). One server, a store without the table, or no hour the table can answer yet: today's
           statement. The route's two small reads and the statement share this snapshot. */
        var route = await QueryTagRollupHashRoute.TryPlanAsync(connection, allowed, windowEnd, readSeconds, cancellationToken);
        try
        {
            await using var command = route is null
                ? QueryStoreCommand(connection, allowed, windowEnd, readSeconds)
                : QueryTagRollupHashRoute.Command(connection, route, windowEnd, readSeconds);
            facts.AddRange(await ReadFactsFromAsync(command, QueryTagRollupSection.QueryStore, cancellationToken));
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
        {
            /* The failed statement aborted the transaction: nothing more can be read on it. */
            return new QueryStoreRead(allowed, refused, [], true);
        }

        return new QueryStoreRead(allowed, refused, facts, false, route is not null);
    }

    /// <summary>The batched Query Store statement's command (<see cref="QueryTagRollupSql.QueryStoreSql"/>): the allowed servers and each one's
    /// read start as two parallel arrays, the window end, and the earliest read start.</summary>
    private static NpgsqlCommand QueryStoreCommand(NpgsqlConnection connection, Dictionary<int, DateTime> allowed, DateTime windowEnd, int readSeconds)
    {
        var ordered = allowed.OrderBy(a => a.Key).ToArray();
        var command = new NpgsqlCommand(QueryTagRollupSql.QueryStoreSql, connection);
        command.CommandTimeout = readSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ordered.Select(a => a.Key).ToArray() });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp,
            Value = ordered.Select(a => DateTime.SpecifyKind(a.Value, DateTimeKind.Unspecified)).ToArray(),
        });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(windowEnd, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(ordered.Min(a => a.Value), DateTimeKind.Unspecified),
        });
        return command;
    }

    /* The deadline is set HERE, on the name the construction is bound to, so the read-surface deadline scan certifies each site itself. */
    private static NpgsqlCommand Command(NpgsqlDataSource postgres, string sql, int[] serverIds, DateTime startUtc, DateTime endUtc, int readSeconds)
    {
        var command = postgres.CreateCommand(sql);
        command.CommandTimeout = readSeconds;
        return WithWindow(command, serverIds, startUtc, endUtc, readSeconds);
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, int[] serverIds, DateTime startUtc, DateTime endUtc, int readSeconds)
    {
        var command = new NpgsqlCommand(sql, connection);
        command.CommandTimeout = readSeconds;
        return WithWindow(command, serverIds, startUtc, endUtc, readSeconds);
    }

    private static NpgsqlCommand WithWindow(NpgsqlCommand command, int[] serverIds, DateTime startUtc, DateTime endUtc, int readSeconds)
    {
        command.CommandTimeout = readSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = serverIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        return command;
    }

    public static async Task<List<QueryTagNode>> ReadTagsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken, int readSeconds = DefaultReadSeconds)
    {
        var tags = new List<QueryTagNode>();
        await using var command = postgres.CreateCommand(QueryTagRollupSql.TagTreeSql);
        command.CommandTimeout = readSeconds;
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
    public static async Task<List<QueryTagRollupFact>> ReadFactsAsync(
        NpgsqlDataSource postgres, string sql, QueryTagRollupSection section, int[] serverIds, DateTime startUtc, DateTime endUtc,
        DateTime? ceiling, CancellationToken cancellationToken, int readSeconds = DefaultReadSeconds)
    {
        var text = sql.Replace(QueryTagRollupSql.CeilingPlaceholder, ceiling is null ? string.Empty : QueryTagRollupSql.CeilingClause, StringComparison.Ordinal);
        await using var command = Command(postgres, text, serverIds, startUtc, endUtc, readSeconds);
        if (ceiling is not null && sql.Contains(QueryTagRollupSql.CeilingPlaceholder, StringComparison.Ordinal))
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(ceiling.Value, DateTimeKind.Unspecified) });
        }

        return await ReadFactsFromAsync(command, section, cancellationToken);
    }

    /// <summary>Reads the grouped statement's rows from a prepared command, on whatever connection (and transaction) it carries.</summary>
    private static async Task<List<QueryTagRollupFact>> ReadFactsFromAsync(NpgsqlCommand command, QueryTagRollupSection section, CancellationToken cancellationToken)
    {
        var facts = new List<QueryTagRollupFact>();
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
    public static async Task<(List<QueryTagDeadlockFact> Facts, bool ScanTruncated)> ReadDeadlocksAsync(
        NpgsqlDataSource postgres, int[] serverIds, DateTime startUtc, DateTime endUtc, ILogger? logger, CancellationToken cancellationToken, int readSeconds = DefaultReadSeconds)
    {
        var graphs = new List<(int ServerId, List<(string Key, DeadlockProcessInfo Process)> Processes, bool Unparsed)>();
        var truncated = false;
        await using (var command = Command(postgres, QueryTagRollupSql.DeadlockGraphsSql, serverIds, startUtc, endUtc, readSeconds))
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
                var processes = ParseProcesses(xml, when)
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
            mapCommand.CommandTimeout = readSeconds;
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

    /// <summary>The process rows of one graph, or none when it does not parse (the same rule <c>DarlingDeadlockProcessRows.Parse</c> applies for the MCP tool).</summary>
    private static List<DeadlockProcessInfo> ParseProcesses(string? graphXml, DateTime? deadlockTime)
        => DeadlockGraphProcessParser.TryParseGraph<DeadlockProcessInfo>(
            new DeadlockGraphProcessParser.DeadlockGraphInput(graphXml, deadlockTime, null, null), out var rows)
            ? rows
            : new List<DeadlockProcessInfo>();

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
        string.Join(QueryTagReadJoin.PathSeparator, SelfAndAncestors(tagId, byId).Reverse().Select(id => byId[id].Name));

    /// <summary>Whether <paramref name="tagId"/> is <paramref name="ancestor"/> or sits under it.</summary>
    public static bool IsUnder(int tagId, int ancestor, Dictionary<int, QueryTagNode> byId) =>
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
    public static Dictionary<string, object?> Assemble(
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
    public static Dictionary<string, object?> AssembleDeadlocks(
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
    public static Dictionary<string, object?> Empty(QueryTagRollupSection section, string status, string message, int? tagFilter = null)
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

    /// <summary>
    /// The totals across a fleet's facts, for the fleet line of a section. It is the sum of the per-server totals (<see cref="Assemble"/>), so
    /// under a <paramref name="tagFilter"/> it is the tag's subtree on every server (untagged work and other tags left out), exactly as each
    /// server's own <c>total</c> is (#5983).
    /// </summary>
    public static Dictionary<string, object> FleetTotal(
        QueryTagRollupSection section, IReadOnlyList<QueryTagRollupFact> facts, IReadOnlyList<QueryTagNode> tags, int? tagFilter)
    {
        var byId = tags.ToDictionary(t => t.Id);
        var total = new double[section.Measures.Count];
        foreach (var fact in facts)
        {
            if (tagFilter is int filter && !(fact.TagId is int tagId && byId.ContainsKey(tagId) && IsUnder(tagId, filter, byId)))
            {
                continue;
            }

            total = Combine(total, fact.Values, section);
        }

        return Figures(total, section);
    }

    /// <summary>
    /// The fleet line of the deadlock section: graphs and processes across the servers. Under a <paramref name="tagFilter"/> a graph counts
    /// where one of its processes carries the tag or a tag under it, and its processes are those under the tag, as each server's total does.
    /// </summary>
    public static Dictionary<string, object> FleetDeadlockTotal(
        IReadOnlyList<QueryTagDeadlockFact> facts, IReadOnlyList<QueryTagNode> tags, int? tagFilter)
    {
        if (tagFilter is not int filter)
        {
            return Figures([facts.Count, facts.Sum(f => (double)(f.ProcessesByTag.Values.Sum() + f.UntaggedProcesses))], QueryTagRollupSection.Deadlocks);
        }

        var byId = tags.ToDictionary(t => t.Id);
        double graphs = 0;
        double processes = 0;
        foreach (var fact in facts)
        {
            var under = fact.ProcessesByTag.Where(p => byId.ContainsKey(p.Key) && IsUnder(p.Key, filter, byId)).Sum(p => (double)p.Value);
            if (under > 0)
            {
                graphs += 1;
                processes += under;
            }
        }

        return Figures([graphs, processes], QueryTagRollupSection.Deadlocks);
    }
}
