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
                /* Ids are BIGINTs read back from the store and formatted by hand: no user text reaches this IN list.
                   The time bounds sit beside the ids so a row group is skipped on either. */
                command.CommandText = @"
SELECT collection_id, collection_time, query_text
FROM v_query_store_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   collection_id IN (" + string.Join(",", slice.Select(r => r.CollectionId.ToString(CultureInfo.InvariantCulture))) + ")";
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
    /// The by-key reads' fallback for a key whose latest kept row carried no text.
    /// </summary>
    internal static async Task<string?> ReadQueryStoreWindowTextAsync(
        LockedConnection connection, int serverId, string databaseName, long queryId, DateTime windowStart, DateTime windowEnd)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT MAX(query_text)
FROM v_query_store_stats
WHERE server_id = $1
AND   database_name = $2
AND   query_id = $3
AND   collection_time >= $4
AND   collection_time <= $5";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDBParameter { Value = queryId });
        command.Parameters.Add(new DuckDBParameter { Value = windowStart });
        command.Parameters.Add(new DuckDBParameter { Value = windowEnd });
        var value = await command.ExecuteScalarAsync();
        return value is null or DBNull ? null : (string)value;
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
