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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>One measure of a roll-up section. <c>Scale</c> divides the stored value (microseconds to milliseconds); <c>Peak</c> takes the maximum instead of the sum.</summary>
public sealed record QueryTagRollupMeasure(string Name, double Scale = 1, bool Peak = false);

/// <summary>One source's section: its name, its measures in the reader's column order, and the measure the tag rows are ordered by, descending.
/// The same sections, names and order as Darling's <c>QueryTagRollupSection</c> (#5983).</summary>
public sealed record QueryTagRollupSection(string Name, IReadOnlyList<QueryTagRollupMeasure> Measures, string OrderMeasure)
{
    public static readonly QueryTagRollupSection PlanCache = new("plan_cache",
        [new("cpu_ms", Scale: 1000), new("duration_ms", Scale: 1000), new("logical_reads"), new("logical_writes"), new("executions")], "cpu_ms");

    public static readonly QueryTagRollupSection QueryStore = new("query_store",
        [new("cpu_ms", Scale: 1000), new("duration_ms", Scale: 1000), new("logical_reads"), new("logical_writes"), new("executions"),
         new("peak_memory_pages", Peak: true)], "cpu_ms");

    public static readonly QueryTagRollupSection Procedures = new("procedures",
        [new("cpu_ms", Scale: 1000), new("duration_ms", Scale: 1000), new("logical_reads"), new("logical_writes"), new("executions")], "cpu_ms");

    public static readonly QueryTagRollupSection BlockingReports = new("blocking_reports",
        [new("blocked_ms"), new("caused_ms"), new("blocked_reports"), new("caused_reports")], "blocked_ms");

    public static readonly QueryTagRollupSection BlockingSnapshots = new("blocking_snapshots",
        [new("blocked_ms"), new("caused_ms"), new("blocked_snapshots"), new("caused_snapshots")], "blocked_ms");

    public static readonly QueryTagRollupSection Deadlocks = new("deadlocks",
        [new("deadlocks"), new("processes")], "deadlocks");
}

/// <summary>Grouped facts for one server and one tag (null tag = no map row, the untagged bucket); values follow the section's measure order.</summary>
public sealed record QueryTagRollupFact(int ServerId, int? TagId, double[] Values);

/// <summary>One deadlock graph reduced to the tags of its processes.</summary>
public sealed record QueryTagDeadlockFact(int ServerId, IReadOnlyDictionary<int, int> ProcessesByTag, int UntaggedProcesses, bool Unparsed = false);

/// <summary>What the reader found in a window: the tag tree, each additive section's facts and the deadlock facts.</summary>
public sealed record QueryTagRollupData(
    IReadOnlyList<QueryTagRow> Tags,
    IReadOnlyDictionary<string, List<QueryTagRollupFact>> Sections,
    List<QueryTagDeadlockFact> Deadlocks,
    bool ScanTruncated,
    IReadOnlyDictionary<string, string> Unavailable);

public partial class LocalDataService
{
    /// <summary>Deadlock graphs read per call; a window with more sets <c>ScanTruncated</c> and the newest are kept.</summary>
    internal const int QueryTagDeadlockScanCeiling = 2000;

    /// <summary>Caller keys per statement when the caller map is read for only the keys the window carries.</summary>
    private const int CallerMapChunk = 500;

    /// <summary>
    /// The facts behind <c>get_resource_use_by_tag</c> (#5992), one list per source over <paramref name="serverIds"/> in the window.
    /// Every source is grouped by its key in SQL and joined to the materialised map (<c>query_tag_map</c>) in C#: nothing evaluates a
    /// rule here. A hash key is <see cref="QueryHashIdentity.CanonicalKey"/>, an object key is <c>database.schema.name</c> and a caller key
    /// is <c>application, login, host, database</c> joined by U+001F, the keys the sighting hook writes.
    /// </summary>
    public async Task<QueryTagRollupData> GetQueryTagRollupDataAsync(IReadOnlyList<int> serverIds, DateTime startUtc, DateTime endUtc)
    {
        var ids = string.Join(",", serverIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        var tags = await GetQueryTagsAsync();
        using var connection = await OpenConnectionAsync();

        var map = new Dictionary<(int Server, string Kind, string Key), int>();
        await ReadRowsAsync(connection, $"SELECT server_id, key_kind, key, tag_id FROM query_tag_map WHERE server_id IN ({ids}) AND key_kind IN ('hash', 'object')", [],
            r => map[(r.GetInt32(0), r.GetString(1), r.GetString(2))] = r.GetInt32(3));

        int? Lookup(int server, string kind, string? key) =>
            key is not null && map.TryGetValue((server, kind, key), out var tag) ? tag : null;

        var window = new object[] { startUtc, endUtc };
        var sections = new Dictionary<string, List<QueryTagRollupFact>>();
        /* A section whose table is missing is unavailable with the reason, never a tool error (the twin's rule, #5983). */
        var unavailable = new Dictionary<string, string>();

        async Task<List<T>> Served<T>(QueryTagRollupSection section, Func<Task<List<T>>> read)
        {
            try
            {
                return await read();
            }
            catch (DuckDBException ex) when (ex.Message.Contains("does not exist", StringComparison.OrdinalIgnoreCase))
            {
                /* #6831: the windows show this text, so it says what is missing in words, as the Darling Viewer does, not the view's name. */
                unavailable[section.Name] = QueryTagSourceWords.Missing(section.Name);
                return [];
            }
        }

        Task<List<QueryTagRollupFact>> Additive(QueryTagRollupSection section, string sql, int keyColumns, Func<System.Data.Common.DbDataReader, int, int?> tagOf) =>
            Served(section, () => AdditiveRead(section, sql, keyColumns, tagOf));

        async Task<List<QueryTagRollupFact>> AdditiveRead(QueryTagRollupSection section, string sql, int keyColumns, Func<System.Data.Common.DbDataReader, int, int?> tagOf)
        {
            var grouped = new Dictionary<(int, int?), double[]>();
            await ReadRowsAsync(connection, sql, window, r =>
            {
                var server = r.GetInt32(0);
                var tag = tagOf(r, server);
                var values = new double[section.Measures.Count];
                for (var i = 0; i < values.Length; i++)
                {
                    values[i] = r.IsDBNull(1 + keyColumns + i) ? 0 : Convert.ToDouble(r.GetValue(1 + keyColumns + i), CultureInfo.InvariantCulture);
                }

                grouped[(server, tag)] = grouped.TryGetValue((server, tag), out var have) ? QueryTagRollup.Combine(have, values, section) : values;
            });
            return grouped.Select(g => new QueryTagRollupFact(g.Key.Item1, g.Key.Item2, g.Value)).ToList();
        }

        int? HashTag(System.Data.Common.DbDataReader r, int server) =>
            Lookup(server, "hash", QueryHashIdentity.CanonicalKey(r.IsDBNull(1) ? null : r.GetString(1)));

        int? ObjectTag(System.Data.Common.DbDataReader r, int server)
        {
            var name = r.IsDBNull(3) ? null : r.GetString(3);
            return string.IsNullOrEmpty(name)
                ? null
                : Lookup(server, "object", new QueryTagObject(r.IsDBNull(1) ? null : r.GetString(1), r.IsDBNull(2) ? null : r.GetString(2), name).Qualified);
        }

        sections[QueryTagRollupSection.PlanCache.Name] = await Additive(QueryTagRollupSection.PlanCache, $@"
SELECT server_id, query_hash,
       CAST(COALESCE(SUM(delta_worker_time), 0) AS DOUBLE), CAST(COALESCE(SUM(delta_elapsed_time), 0) AS DOUBLE),
       CAST(COALESCE(SUM(delta_logical_reads), 0) AS DOUBLE), CAST(COALESCE(SUM(delta_logical_writes), 0) AS DOUBLE),
       CAST(COALESCE(SUM(delta_execution_count), 0) AS DOUBLE)
FROM v_query_stats
WHERE server_id IN ({ids}) AND collection_time >= $1 AND collection_time <= $2
GROUP BY server_id, query_hash", 1, HashTag);

        /* The latest snapshot of each interval, as the existing Query Store reads dedupe (#1907). */
        sections[QueryTagRollupSection.QueryStore.Name] = await Additive(QueryTagRollupSection.QueryStore, $@"
WITH deduped AS (
    SELECT server_id, query_hash, execution_count, avg_cpu_time_us, avg_duration_us, avg_logical_io_reads, avg_logical_io_writes,
           max_query_max_used_memory,
           ROW_NUMBER() OVER (
               PARTITION BY server_id, database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
               ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM v_query_store_stats
    WHERE server_id IN ({ids}) AND collection_time >= $1 AND collection_time <= $2
)
SELECT server_id, query_hash,
       CAST(COALESCE(SUM(CAST(avg_cpu_time_us AS DOUBLE) * execution_count), 0) AS DOUBLE),
       CAST(COALESCE(SUM(CAST(avg_duration_us AS DOUBLE) * execution_count), 0) AS DOUBLE),
       CAST(COALESCE(SUM(CAST(avg_logical_io_reads AS DOUBLE) * execution_count), 0) AS DOUBLE),
       CAST(COALESCE(SUM(CAST(avg_logical_io_writes AS DOUBLE) * execution_count), 0) AS DOUBLE),
       CAST(COALESCE(SUM(execution_count), 0) AS DOUBLE),
       CAST(COALESCE(MAX(max_query_max_used_memory), 0) AS DOUBLE)
FROM deduped
WHERE rn = 1
GROUP BY server_id, query_hash", 1, HashTag);

        sections[QueryTagRollupSection.Procedures.Name] = await Additive(QueryTagRollupSection.Procedures, $@"
SELECT server_id, database_name, schema_name, object_name,
       CAST(COALESCE(SUM(delta_worker_time), 0) AS DOUBLE), CAST(COALESCE(SUM(delta_elapsed_time), 0) AS DOUBLE),
       CAST(COALESCE(SUM(delta_logical_reads), 0) AS DOUBLE), CAST(COALESCE(SUM(delta_logical_writes), 0) AS DOUBLE),
       CAST(COALESCE(SUM(delta_execution_count), 0) AS DOUBLE)
FROM v_procedure_stats
WHERE server_id IN ({ids}) AND collection_time >= $1 AND collection_time <= $2
GROUP BY server_id, database_name, schema_name, object_name", 3, ObjectTag);

        static string CallerOf(System.Data.Common.DbDataReader r, int at) =>
            QueryTagRollup.CallerKey(Str(r, at), Str(r, at + 1), Str(r, at + 2), Str(r, at + 3));

        /* A blocking row lands on the blocked side's tag (blocked) and on the blocking side's tag (caused). The caller keys are read
           first and the caller map is read for only those keys (and only the servers in the call), never every caller row of the file. */
        var callerKeys = new HashSet<string>(StringComparer.Ordinal);
        async Task<List<(int Server, string Key, bool Blocked, double Ms, double Count)>> BlockingRows(QueryTagRollupSection section, Func<string, string> source, string timeColumn)
        {
            var rows = new List<(int, string, bool, double, double)>();
            foreach (var side in new[] { "blocked", "blocking" })
            {
                var sql = $@"
SELECT server_id, {side}_client_app, {side}_login_name, {side}_host_name, database_name,
       CAST(COALESCE(SUM(COALESCE(wait_time_ms, 0)), 0) AS DOUBLE), COUNT(*)
FROM {source($"server_id IN ({ids}) AND {timeColumn} >= $1 AND {timeColumn} <= $2")}
GROUP BY server_id, {side}_client_app, {side}_login_name, {side}_host_name, database_name";
                await ReadRowsAsync(connection, sql, window, r =>
                {
                    var key = CallerOf(r, 1);
                    callerKeys.Add(key);
                    rows.Add((r.GetInt32(0), key, side == "blocked",
                        Convert.ToDouble(r.GetValue(5), CultureInfo.InvariantCulture), Convert.ToDouble(r.GetValue(6), CultureInfo.InvariantCulture)));
                });
            }

            return rows;
        }

        var reportRows = await Served(QueryTagRollupSection.BlockingReports,
            () => BlockingRows(QueryTagRollupSection.BlockingReports, w => StoredEventCopies.BlockedProcessReports(w) + " AS ev", "event_time"));
        var snapshotRows = await Served(QueryTagRollupSection.BlockingSnapshots,
            () => BlockingRows(QueryTagRollupSection.BlockingSnapshots, w => $"(SELECT * FROM v_dmv_blocking_snapshots WHERE {w}) AS ev", "collection_time"));

        var graphs = new List<(int Server, List<string> Keys, bool Unparsed)>();
        var read = 0;
        var truncated = false;
        var deadlockRows = await Served(QueryTagRollupSection.Deadlocks, async () =>
        {
            var found = new List<(int Server, string? Xml, DateTime? When)>();
            await ReadRowsAsync(connection, $@"
SELECT server_id, deadlock_graph_xml, deadlock_time
FROM {StoredEventCopies.Deadlocks($"server_id IN ({ids}) AND deadlock_time >= $1 AND deadlock_time <= $2")} AS dl
ORDER BY deadlock_time DESC, deadlock_id DESC
LIMIT {QueryTagDeadlockScanCeiling + 1}", window,
                r =>
                {
                    if (++read > QueryTagDeadlockScanCeiling)
                    {
                        truncated = true;
                        return;
                    }

                    found.Add((r.GetInt32(0), r.IsDBNull(1) ? null : r.GetString(1), r.IsDBNull(2) ? null : r.GetDateTime(2)));
                });
            return found;
        });

        foreach (var (server, xml, when) in deadlockRows)
        {
            /* A graph that is NULL, will not parse or has no process still counts in the section's total, as unparsed. */
            if (!DeadlockGraphProcessParser.TryParseGraph<DeadlockProcessInfo>(new DeadlockGraphProcessParser.DeadlockGraphInput(xml, when, null, null), out var processes) || processes.Count == 0)
            {
                graphs.Add((server, [], true));
                continue;
            }

            var keys = processes.Select(p => QueryTagRollup.CallerKey(p.ClientApp, p.LoginName, p.HostName, p.DatabaseName)).ToList();
            callerKeys.UnionWith(keys);
            graphs.Add((server, keys, false));
        }

        /* The bounded caller map read: the servers in the call and the keys the window's rows carry, in chunks. */
        var callerKeyList = callerKeys.ToList();
        for (var offset = 0; offset < callerKeyList.Count; offset += CallerMapChunk)
        {
            var chunk = callerKeyList.Skip(offset).Take(CallerMapChunk).ToList();
            var marks = string.Join(", ", chunk.Select((_, i) => "$" + (i + 1)));
            await ReadRowsAsync(connection, $"SELECT server_id, key_kind, key, tag_id FROM query_tag_map WHERE server_id IN ({ids}) AND key_kind = 'caller' AND key IN ({marks})",
                chunk.Cast<object>().ToList(), r => map[(r.GetInt32(0), r.GetString(1), r.GetString(2))] = r.GetInt32(3));
        }

        List<QueryTagRollupFact> Fold(QueryTagRollupSection section, List<(int Server, string Key, bool Blocked, double Ms, double Count)> rows)
        {
            var grouped = new Dictionary<(int, int?), double[]>();
            foreach (var (server, key, blocked, ms, n) in rows)
            {
                var tag = Lookup(server, "caller", key);
                var values = blocked ? new[] { ms, 0, n, 0 } : new[] { 0, ms, 0, n };
                grouped[(server, tag)] = grouped.TryGetValue((server, tag), out var have) ? QueryTagRollup.Combine(have, values, section) : values;
            }

            return grouped.Select(g => new QueryTagRollupFact(g.Key.Item1, g.Key.Item2, g.Value)).ToList();
        }

        sections[QueryTagRollupSection.BlockingReports.Name] = Fold(QueryTagRollupSection.BlockingReports, reportRows);
        sections[QueryTagRollupSection.BlockingSnapshots.Name] = Fold(QueryTagRollupSection.BlockingSnapshots, snapshotRows);

        var deadlocks = new List<QueryTagDeadlockFact>();
        foreach (var (server, keys, unparsed) in graphs)
        {
            var byTag = new Dictionary<int, int>();
            var untagged = 0;
            foreach (var key in keys)
            {
                if (Lookup(server, "caller", key) is int tag)
                {
                    byTag[tag] = byTag.GetValueOrDefault(tag) + 1;
                }
                else
                {
                    untagged++;
                }
            }

            deadlocks.Add(new QueryTagDeadlockFact(server, byTag, untagged, unparsed));
        }

        return new QueryTagRollupData(tags, sections, deadlocks, truncated, unavailable);
    }

    private static string? Str(System.Data.Common.DbDataReader r, int at) => r.IsDBNull(at) ? null : r.GetString(at);

    private static async Task ReadRowsAsync(LockedConnection connection, string sql, IReadOnlyList<object> parameters, Action<System.Data.Common.DbDataReader> each)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        foreach (var value in parameters)
        {
            command.Parameters.Add(new DuckDBParameter { Value = value });
        }

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            each(reader);
        }
    }
}
