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
public sealed record QueryTagDeadlockFact(int ServerId, IReadOnlyDictionary<int, int> ProcessesByTag, int UntaggedProcesses);

/// <summary>What the reader found in a window: the tag tree, each additive section's facts and the deadlock facts.</summary>
public sealed record QueryTagRollupData(
    IReadOnlyList<QueryTagRow> Tags,
    IReadOnlyDictionary<string, List<QueryTagRollupFact>> Sections,
    List<QueryTagDeadlockFact> Deadlocks,
    bool ScanTruncated);

public partial class LocalDataService
{
    /// <summary>Deadlock graphs read per call; a window with more sets <c>ScanTruncated</c> and the newest are kept.</summary>
    internal const int QueryTagDeadlockScanCeiling = 2000;

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
        await ReadRowsAsync(connection, $"SELECT server_id, key_kind, key, tag_id FROM query_tag_map WHERE server_id IN ({ids})", [],
            r => map[(r.GetInt32(0), r.GetString(1), r.GetString(2))] = r.GetInt32(3));

        int? Lookup(int server, string kind, string? key) =>
            key is not null && map.TryGetValue((server, kind, key), out var tag) ? tag : null;

        var window = new object[] { startUtc, endUtc };
        var sections = new Dictionary<string, List<QueryTagRollupFact>>();

        async Task<List<QueryTagRollupFact>> Additive(QueryTagRollupSection section, string sql, int keyColumns, Func<System.Data.Common.DbDataReader, int, int?> tagOf)
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

        string CallerOf(System.Data.Common.DbDataReader r, int at) =>
            QueryTagRollup.CallerKey(Str(r, at), Str(r, at + 1), Str(r, at + 2), Str(r, at + 3));

        /* A blocking row lands on the blocked side's tag (blocked) and on the blocking side's tag (caused). */
        async Task<List<QueryTagRollupFact>> Blocking(QueryTagRollupSection section, Func<string, string> source, string timeColumn)
        {
            var grouped = new Dictionary<(int, int?), double[]>();
            foreach (var side in new[] { "blocked", "blocking" })
            {
                var sql = $@"
SELECT server_id, {side}_client_app, {side}_login_name, {side}_host_name, database_name,
       CAST(COALESCE(SUM(COALESCE(wait_time_ms, 0)), 0) AS DOUBLE), COUNT(*)
FROM {source($"server_id IN ({ids}) AND {timeColumn} >= $1 AND {timeColumn} <= $2")}
GROUP BY server_id, {side}_client_app, {side}_login_name, {side}_host_name, database_name";
                await ReadRowsAsync(connection, sql, window, r =>
                {
                    var server = r.GetInt32(0);
                    var tag = Lookup(server, "caller", CallerOf(r, 1));
                    var ms = Convert.ToDouble(r.GetValue(5), CultureInfo.InvariantCulture);
                    var n = Convert.ToDouble(r.GetValue(6), CultureInfo.InvariantCulture);
                    var values = side == "blocked" ? new[] { ms, 0, n, 0 } : new[] { 0, ms, 0, n };
                    grouped[(server, tag)] = grouped.TryGetValue((server, tag), out var have) ? QueryTagRollup.Combine(have, values, section) : values;
                });
            }

            return grouped.Select(g => new QueryTagRollupFact(g.Key.Item1, g.Key.Item2, g.Value)).ToList();
        }

        sections[QueryTagRollupSection.BlockingReports.Name] = await Blocking(
            QueryTagRollupSection.BlockingReports, w => StoredEventCopies.BlockedProcessReports(w) + " AS ev", "event_time");
        sections[QueryTagRollupSection.BlockingSnapshots.Name] = await Blocking(
            QueryTagRollupSection.BlockingSnapshots, w => $"(SELECT * FROM v_dmv_blocking_snapshots WHERE {w}) AS ev", "collection_time");

        var deadlocks = new List<QueryTagDeadlockFact>();
        var read = 0;
        var truncated = false;
        var graphs = new List<(int Server, string? Xml, DateTime? When)>();
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

                graphs.Add((r.GetInt32(0), r.IsDBNull(1) ? null : r.GetString(1), r.IsDBNull(2) ? null : r.GetDateTime(2)));
            });

        foreach (var (server, xml, when) in graphs)
        {
            if (!DeadlockGraphProcessParser.TryParseGraph<DeadlockProcessInfo>(new DeadlockGraphProcessParser.DeadlockGraphInput(xml, when, null, null), out var processes))
            {
                continue;
            }

            var byTag = new Dictionary<int, int>();
            var untagged = 0;
            foreach (var p in processes)
            {
                if (Lookup(server, "caller", QueryTagRollup.CallerKey(p.ClientApp, p.LoginName, p.HostName, p.DatabaseName)) is int tag)
                {
                    byTag[tag] = byTag.GetValueOrDefault(tag) + 1;
                }
                else
                {
                    untagged++;
                }
            }

            if (byTag.Count + untagged > 0)
            {
                deadlocks.Add(new QueryTagDeadlockFact(server, byTag, untagged));
            }
        }

        return new QueryTagRollupData(tags, sections, deadlocks, truncated);
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
