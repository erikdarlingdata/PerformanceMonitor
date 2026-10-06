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
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/*
 * #5381: the Query Store reads rank and aggregate on narrow columns, then read query_text here, for the final rows
 * only and by key.
 *
 * Why a separate read instead of a join: the archive's parquet files hold one row group per day, and DuckDB decodes a
 * row group's whole query_text column chunk when anything in the group is read, so a join that probes the wide
 * column over the window pays for every day the window touches, at the same time. A statement that names the rows it
 * wants lets the scan skip every row group whose min/max cannot hold them, and one statement per day keeps a single
 * day's text in memory at a time. The text that comes back is then per row asked for, not per row in the window.
 */
public partial class LocalDataService
{
    /// <summary>Most row keys one by-key text statement carries; the statement's IN list is built from numbers only.</summary>
    internal const int QueryStoreTextKeyChunk = 400;

    /// <summary>
    /// Reads <c>query_text</c> for the named <c>v_query_store_stats</c> rows (keyed by collection_id and
    /// collection_time), one statement per UTC day of collection_time and per <see cref="QueryStoreTextKeyChunk"/> ids.
    /// A row that is not found is absent from the result; a row found with no text maps to null.
    /// </summary>
    internal static async Task<Dictionary<(long CollectionId, DateTime CollectionTime), string?>> ReadQueryStoreTextByRowAsync(
        LockedConnection connection, int serverId, IEnumerable<(long CollectionId, DateTime CollectionTime)> rows)
    {
        var result = new Dictionary<(long, DateTime), string?>();
        foreach (var day in rows.Distinct().GroupBy(r => r.CollectionTime.Date).OrderBy(g => g.Key))
        {
            var ordered = day.OrderBy(r => r.CollectionId).ToList();
            for (var i = 0; i < ordered.Count; i += QueryStoreTextKeyChunk)
            {
                var slice = ordered.Skip(i).Take(QueryStoreTextKeyChunk).ToList();
                using var command = connection.CreateCommand();
                command.CommandText = TextByRowSql(slice.Select(r => r.CollectionId));
                command.Parameters.Add(new DuckDBParameter { Value = serverId });
                command.Parameters.Add(new DuckDBParameter { Value = slice.Min(r => r.CollectionTime) });
                command.Parameters.Add(new DuckDBParameter { Value = slice.Max(r => r.CollectionTime) });
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    result[(reader.GetInt64(0), reader.GetDateTime(1))] = reader.IsDBNull(2) ? null : reader.GetString(2);
                }
            }
        }

        return result;
    }

    /// <summary>
    /// The by-key text statement for the given ids: $1 server, $2 and $3 the time bounds. The bounds sit beside the ids so
    /// a parquet row group is skipped on either. Ids are BIGINTs read back from the store and formatted by hand: no
    /// user text reaches this IN list.
    /// </summary>
    internal static string TextByRowSql(IEnumerable<long> collectionIds) => @"
SELECT collection_id, collection_time, query_text
FROM v_query_store_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   collection_id IN (" + string.Join(",", collectionIds.Select(id => id.ToString(CultureInfo.InvariantCulture))) + ")";

    /// <summary>The window text statement: $1 server, $2 database, $3 query_id, $4 and $5 the window.</summary>
    internal const string WindowTextSql = @"
SELECT MAX(query_text)
FROM v_query_store_stats
WHERE server_id = $1
AND   database_name = $2
AND   query_id = $3
AND   collection_time >= $4
AND   collection_time <= $5";

    /// <summary>Most (database, query_id) keys one latest-row statement carries; each key is two parameters.</summary>
    internal const int QueryStoreLatestRowKeyChunk = 100;

    /// <summary>
    /// For each (database_name, query_id), the key (collection_id, collection_time) of its LATEST <c>v_query_store_stats</c>
    /// row over ALL history, whatever that row's text is: the row the old top-queries lateral picked, found on narrow
    /// columns only and for the named keys only. A key with no row is absent from the result.
    /// </summary>
    internal static async Task<Dictionary<(string DatabaseName, long QueryId), (long CollectionId, DateTime CollectionTime)>> ReadQueryStoreLatestRowsAsync(
        LockedConnection connection, int serverId, IEnumerable<(string DatabaseName, long QueryId)> keys)
    {
        var result = new Dictionary<(string, long), (long, DateTime)>();
        var distinct = keys.Distinct().ToList();
        for (var i = 0; i < distinct.Count; i += QueryStoreLatestRowKeyChunk)
        {
            var slice = distinct.Skip(i).Take(QueryStoreLatestRowKeyChunk).ToList();
            using var command = connection.CreateCommand();
            /* The key list is parameters, never text: a database name is user data. $1 is the server; key n is ($2n, $2n+1). */
            command.CommandText = @"
SELECT x.database_name, x.query_id, x.collection_id, x.collection_time
FROM
(
    SELECT
        v.database_name,
        v.query_id,
        v.collection_id,
        v.collection_time,
        ROW_NUMBER() OVER (PARTITION BY v.database_name, v.query_id ORDER BY v.collection_time DESC, v.collection_id DESC) AS lr
    FROM v_query_store_stats v
    INNER JOIN (VALUES " + string.Join(", ", slice.Select((_, n) => $"(CAST(${2 + 2 * n} AS VARCHAR), CAST(${3 + 2 * n} AS BIGINT))")) + @") k(database_name, query_id)
      ON  k.database_name = v.database_name
      AND k.query_id = v.query_id
    WHERE v.server_id = $1
) x
WHERE x.lr = 1";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            foreach (var (databaseName, queryId) in slice)
            {
                command.Parameters.Add(new DuckDBParameter { Value = databaseName });
                command.Parameters.Add(new DuckDBParameter { Value = queryId });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result[(reader.GetString(0), reader.GetInt64(1))] = (reader.GetInt64(2), reader.GetDateTime(3));
            }
        }

        return result;
    }

    /// <summary>
    /// The latest NON-NULL <c>query_text</c> of one (database, query_id) over all history: the old top-queries lateral,
    /// for ONE key. Only the rare candidate whose latest row carries no text gets here.
    /// </summary>
    internal static async Task<string?> ReadQueryStoreLatestNonNullTextAsync(
        LockedConnection connection, int serverId, string databaseName, long queryId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT query_text
FROM v_query_store_stats
WHERE server_id = $1
AND   query_id = $2
AND   database_name = $3
AND   query_text IS NOT NULL
/* #5299 round 2 (N3): a tie on the time breaks on the row collected last - see GetTopQueriesByCpuAsync. */
ORDER BY collection_time DESC, collection_id DESC
LIMIT 1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = queryId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        var value = await command.ExecuteScalarAsync();
        return value is null or DBNull ? null : (string)value;
    }

    /// <summary>
    /// The greatest non-null <c>query_text</c> one (database, query_id) has inside a collection_time window, or null.
    /// The by-key reads' fallback for a key whose latest kept row carried no text. One statement per UTC day of the
    /// window, newest day first, stopping at the first day that has a text: a single day's text is in memory at a time,
    /// and a query that has text anywhere recent never touches the older days (the review of #5396).
    /// </summary>
    internal static async Task<string?> ReadQueryStoreWindowTextAsync(
        LockedConnection connection, int serverId, string databaseName, long queryId, DateTime windowStart, DateTime windowEnd)
    {
        for (var day = windowEnd.Date; day >= windowStart.Date; day = day.AddDays(-1))
        {
            /* DuckDB's TIMESTAMP has microsecond precision: a tick-sized step back from midnight would round up to it. */
            var from = day > windowStart ? day : windowStart;
            var dayEnd = day.AddDays(1).AddTicks(-10);
            var to = dayEnd < windowEnd ? dayEnd : windowEnd;
            using var command = connection.CreateCommand();
            command.CommandText = WindowTextSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = databaseName });
            command.Parameters.Add(new DuckDBParameter { Value = queryId });
            command.Parameters.Add(new DuckDBParameter { Value = from });
            command.Parameters.Add(new DuckDBParameter { Value = to });
            var value = await command.ExecuteScalarAsync();
            if (value is not null and not DBNull)
            {
                return (string)value;
            }
        }

        return null;
    }

    /// <summary>
    /// The KEPT rows (the latest snapshot of each interval, with executions) one period has for the named
    /// (database_name, query_id) keys, on narrow columns only, as <c>(database_name, query_hash, query_id,
    /// collection_id, collection_time)</c>. The dedupe is the comparison's own, so a row an older snapshot of the same
    /// interval would offer is not among them. Keys travel as parameters: a database name is user data.
    /// </summary>
    internal static async Task<List<(string? DatabaseName, string? QueryHash, long QueryId, long CollectionId, DateTime CollectionTime)>> ReadQueryStoreKeptRowsAsync(
        LockedConnection connection, int serverId, IEnumerable<(string? DatabaseName, long QueryId)> keys, DateTime windowStart, DateTime windowEnd)
    {
        var result = new List<(string?, string?, long, long, DateTime)>();
        var distinct = keys.Distinct().ToList();
        for (var i = 0; i < distinct.Count; i += QueryStoreLatestRowKeyChunk)
        {
            var slice = distinct.Skip(i).Take(QueryStoreLatestRowKeyChunk).ToList();
            using var command = connection.CreateCommand();
            /* $1 server, $2 and $3 the window; key n is ($4 + 2n, $5 + 2n). */
            command.CommandText = @"
SELECT x.database_name, x.query_hash, x.query_id, x.collection_id, x.collection_time
FROM
(
    SELECT
        v.database_name,
        v.query_hash,
        v.query_id,
        v.collection_id,
        v.collection_time,
        v.execution_count,
        ROW_NUMBER() OVER
        (
            PARTITION BY v.database_name, v.query_id, v.plan_id, v.runtime_stats_interval_id, v.first_execution_time, v.execution_type_desc, v.replica_role
            ORDER BY v.collection_time DESC, v.execution_count DESC
        ) AS rn
    FROM v_query_store_stats v
    INNER JOIN (VALUES " + string.Join(", ", slice.Select((_, n) => $"(CAST(${4 + 2 * n} AS VARCHAR), CAST(${5 + 2 * n} AS BIGINT))")) + @") k(database_name, query_id)
      ON  k.database_name IS NOT DISTINCT FROM v.database_name
      AND k.query_id = v.query_id
    WHERE v.server_id = $1
    AND   v.collection_time >= $2
    AND   v.collection_time <= $3
) x
WHERE x.rn = 1
AND   x.execution_count > 0";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = windowStart });
            command.Parameters.Add(new DuckDBParameter { Value = windowEnd });
            foreach (var (databaseName, queryId) in slice)
            {
                command.Parameters.Add(new DuckDBParameter { Value = (object?)databaseName ?? DBNull.Value });
                command.Parameters.Add(new DuckDBParameter { Value = queryId });
            }

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.GetInt64(2), reader.GetInt64(3), reader.GetDateTime(4)));
            }
        }

        return result;
    }

    /// <summary>
    /// The comparison's text fallback. A period's winner row is the latest kept row of one query_id; when its text came
    /// back NULL, the old per-period MAX still found a text on an older kept row of that query in the same period. This
    /// reads only the queries whose winner has no text: their other kept rows are listed on narrow columns, then their
    /// text is read one UTC day at a time, newest day first, dropping a query from the work as soon as it has a text.
    /// What is found is added to <paramref name="rowsByKey"/> and <paramref name="texts"/> so the MAX sees it.
    /// </summary>
    internal static async Task ReadQueryStoreFallbackTextsAsync(
        LockedConnection connection, int serverId,
        List<(string? DatabaseName, string? QueryHash, long QueryId, long CollectionId, DateTime CollectionTime)> winners,
        DateTime windowStart, DateTime windowEnd,
        Dictionary<(string?, string?), List<(long, DateTime)>> rowsByKey,
        Dictionary<(long CollectionId, DateTime CollectionTime), string?> texts)
    {
        var pending = winners
            .Where(w => !texts.TryGetValue((w.CollectionId, w.CollectionTime), out var text) || text is null)
            .Select(w => (w.DatabaseName, w.QueryHash, w.QueryId))
            .ToHashSet();
        if (pending.Count == 0)
        {
            return;
        }

        var kept = await ReadQueryStoreKeptRowsAsync(connection, serverId, pending.Select(p => (p.DatabaseName, p.QueryId)), windowStart, windowEnd);
        foreach (var day in kept
            .Where(r => !texts.ContainsKey((r.CollectionId, r.CollectionTime)))
            .GroupBy(r => r.CollectionTime.Date)
            .OrderByDescending(g => g.Key))
        {
            if (pending.Count == 0)
            {
                break;
            }

            var rows = day.Where(r => pending.Contains((r.DatabaseName, r.QueryHash, r.QueryId))).ToList();
            if (rows.Count == 0)
            {
                continue;
            }

            var dayTexts = await ReadQueryStoreTextByRowAsync(connection, serverId, rows.Select(r => (r.CollectionId, r.CollectionTime)));
            foreach (var (rowKey, text) in dayTexts)
            {
                texts[rowKey] = text;
            }

            foreach (var row in rows)
            {
                var key = (row.DatabaseName, row.QueryHash);
                if (!rowsByKey.TryGetValue(key, out var list))
                {
                    rowsByKey[key] = list = [];
                }

                list.Add((row.CollectionId, row.CollectionTime));
                if (dayTexts.TryGetValue((row.CollectionId, row.CollectionTime), out var found) && found is not null)
                {
                    pending.Remove((row.DatabaseName, row.QueryHash, row.QueryId));
                }
            }
        }
    }

    /// <summary>
    /// The greatest non-null text among the rows read for <paramref name="key"/>, compared the way DuckDB compares
    /// VARCHAR (UTF-8 byte order), or null when the key has no row or every row's text is null.
    /// </summary>
    internal static string? MaxUtf8Text(
        Dictionary<(string?, string?), List<(long, DateTime)>> rowsByKey,
        (string?, string?) key,
        Dictionary<(long CollectionId, DateTime CollectionTime), string?> texts)
    {
        if (!rowsByKey.TryGetValue(key, out var rows))
        {
            return null;
        }

        string? best = null;
        foreach (var row in rows)
        {
            if (texts.TryGetValue((row.Item1, row.Item2), out var text) && text is not null
                && (best is null || CompareUtf8Order(text, best) > 0))
            {
                best = text;
            }
        }

        return best;
    }

    /// <summary>
    /// Orders two strings by their UTF-8 bytes, which is DuckDB's VARCHAR order. UTF-16 ordinal order agrees with it
    /// except where a surrogate (a code point above U+FFFF) meets a character in U+E000..U+FFFF: the surrogate sorts
    /// lower in UTF-16 and higher in UTF-8.
    /// </summary>
    internal static int CompareUtf8Order(string a, string b)
    {
        var n = a.AsSpan().CommonPrefixLength(b.AsSpan());
        if (n == a.Length || n == b.Length)
        {
            return a.Length.CompareTo(b.Length);
        }

        var x = a[n];
        var y = b[n];
        var xIsSurrogate = char.IsSurrogate(x);
        var yIsSurrogate = char.IsSurrogate(y);
        if (xIsSurrogate != yIsSurrogate)
        {
            return xIsSurrogate ? 1 : -1;
        }

        return x.CompareTo(y);
    }
}
