/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One Query Store hint in the newest capture of its database (#6581). <see cref="FirstSeen"/> is the first capture inside the window
/// that shows this hint id on this query: Query Store keeps no set time, so the real one can be up to one collector pass earlier, and a
/// hint set before the window reads as first seen at the window's first capture.
/// </summary>
public sealed record QueryStoreHintFact(
    string DatabaseName,
    long QueryId,
    long HintId,
    string? HintText,
    string? FailureReason,
    long FailureCount,
    string? Source,
    DateTime FirstSeen,
    DateTime LastSeen,
    string? QueryText = null,
    bool QueryTextTruncated = false)
{
    /// <summary>The engine records a failure: a reason, or a count above zero. The view carries no "applied" column, so no failure is not proof the hint took effect.</summary>
    public bool Failing => !string.IsNullOrEmpty(FailureReason) || FailureCount > 0;
}

/// <summary>The newest hint capture of one database inside the window.</summary>
/// <param name="DatabaseName">The database.</param>
/// <param name="NewestCapture">Its newest marker row in the window.</param>
/// <param name="HintCount">Every hint in that capture (counted in SQL over the whole capture, so a hint the read cap cut still counts).</param>
public sealed record QueryStoreHintCapture(string DatabaseName, DateTime NewestCapture, int HintCount);

/// <summary>What <see cref="DarlingQueryStoreHintReader.ReadAsync"/> found.</summary>
/// <param name="Hints">One entry per hint in its database's newest capture, failing ones first, at most <see cref="DarlingQueryStoreHintReader.MaxHints"/>.</param>
/// <param name="Captures">Each database that has a marker row in the window, with its own newest capture and hint count.</param>
/// <param name="TotalHints">Every hint in the newest captures, counted in SQL: more than <see cref="Hints"/> holds when the read was cut.</param>
/// <param name="FailingHints">Every failing hint in the newest captures, counted in SQL.</param>
public sealed record QueryStoreHintRead(
    IReadOnlyList<QueryStoreHintFact> Hints,
    IReadOnlyList<QueryStoreHintCapture> Captures,
    int TotalHints,
    int FailingHints)
{
    /// <summary>The read hit <see cref="DarlingQueryStoreHintReader.MaxHints"/> and left hints out (all of them healthy ones, because failing hints sort first).</summary>
    public bool ReadCapped => TotalHints > Hints.Count;
}

/// <summary>
/// #6581: the Query Store hints in place, from <c>collect.query_store_hints</c> (V217): the collector's per-database marker row plus one
/// row per hint, every pass. A database is judged by its own newest marker row inside the window, so a database whose last capture is
/// older than another's is not judged against the other's. A hint that is not in a captured database's newest capture is cleared and is
/// not listed. Both reads are bounded by <c>server_id</c> and the window on <c>collection_time</c>
/// (<c>idx_query_store_hints_time</c>).
/// </summary>
public static class DarlingQueryStoreHintReader
{
    /// <summary>
    /// The most hints one read holds, across every captured database together (the collector's own cap is 2000 per database per pass, so one
    /// database at its cap fills the whole read). The cut falls on healthy hints only, because <see cref="HintsSql"/> sorts failing ones first,
    /// and the answer counts the whole set in SQL (<see cref="CountsSql"/>) so a cut read says so.
    /// </summary>
    public const int MaxHints = 2000;

    /// <summary>The most characters of a query's text one entry carries.</summary>
    public const int QueryTextChars = 1000;

    /// <summary>Each database's newest marker row in the window. $1 server, $2 start, $3 end, $4 database filter.</summary>
    public static string CapturesSql(DatabaseFilter databases) => $@"
SELECT database_name, MAX(collection_time)
FROM collect.query_store_hints
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3 AND query_hint_id IS NULL{databases.Clause("database_name", 4)}
GROUP BY database_name
ORDER BY database_name";

    /// <summary>
    /// The hints in each database's newest capture. $1 server, $2 start, $3 end, $4 database filter, $5 query_id or NULL. The hints share
    /// the marker row's collection_time (one pass stamps one time), so the join is on equality. First seen (#6876) comes from one grouped pass
    /// over the hint rows (<c>firsts</c>): the earliest row after the hint's last absence. A correlated per-hint minimum scanned every captured
    /// row once per hint (345 hints over 24 hours of captures took 2 seconds).
    /// </summary>
    public static string HintsSql(DatabaseFilter databases) => $@"
WITH {Ctes(databases)},
firsts AS (
    SELECT x.database_name, x.query_id, x.query_hint_id, MIN(x.collection_time) AS first_seen
    FROM hints AS x
    LEFT JOIN absent AS a ON a.database_name = x.database_name AND a.query_id = x.query_id AND a.query_hint_id = x.query_hint_id
    WHERE a.last_absent IS NULL OR x.collection_time > a.last_absent
    GROUP BY x.database_name, x.query_id, x.query_hint_id)
SELECT h.database_name, h.query_id, h.query_hint_id, h.query_hint_text, h.last_query_hint_failure_reason, h.query_hint_failure_count,
       h.source_desc, h.collection_time,
       f.first_seen
FROM hints AS h
JOIN newest AS n ON n.database_name = h.database_name AND n.newest_capture = h.collection_time
LEFT JOIN firsts AS f ON f.database_name = h.database_name AND f.query_id = h.query_id AND f.query_hint_id = h.query_hint_id
ORDER BY CASE WHEN {FailingPredicate} THEN 0 ELSE 1 END, h.database_name, h.query_id, h.query_hint_id
LIMIT {MaxHints}";

    /// <summary>The engine records a failure: a reason, or a count above zero (the SQL twin of <see cref="QueryStoreHintFact.Failing"/>).</summary>
    private const string FailingPredicate = "(COALESCE(h.last_query_hint_failure_reason, '') <> '' OR COALESCE(h.query_hint_failure_count, 0) > 0)";

    /// <summary>
    /// How many hints each database's newest capture holds and how many of them fail, over the whole capture (no <see cref="MaxHints"/> cut).
    /// Same parameters as <see cref="HintsSql"/>.
    /// </summary>
    public static string CountsSql(DatabaseFilter databases) => $@"
WITH {Ctes(databases)}
SELECT h.database_name, COUNT(*), COUNT(*) FILTER (WHERE {FailingPredicate})
FROM hints AS h
JOIN newest AS n ON n.database_name = h.database_name AND n.newest_capture = h.collection_time
GROUP BY h.database_name";

    internal static string Ctes(DatabaseFilter databases) => $@"win AS (
    SELECT database_name, collection_time, query_hint_id, query_id, query_hint_text, last_query_hint_failure_reason,
           query_hint_failure_count, source_desc
    FROM collect.query_store_hints
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3{databases.Clause("database_name", 4)}),
hints AS (
    SELECT * FROM win WHERE query_hint_id IS NOT NULL AND ($5::bigint IS NULL OR query_id = $5)),
absent AS (
    SELECT p.database_name, p.query_id, p.query_hint_id, MAX(m.collection_time) AS last_absent
    FROM (SELECT DISTINCT database_name, query_id, query_hint_id FROM hints) AS p
    JOIN win AS m ON m.database_name = p.database_name AND m.query_hint_id IS NULL
    WHERE NOT EXISTS (SELECT 1 FROM hints AS x
                      WHERE x.database_name = p.database_name AND x.query_id = p.query_id AND x.query_hint_id = p.query_hint_id
                      AND x.collection_time = m.collection_time)
    GROUP BY p.database_name, p.query_id, p.query_hint_id),
newest AS (
    SELECT database_name, MAX(collection_time) AS newest_capture FROM win WHERE query_hint_id IS NULL GROUP BY database_name)";

    /// <summary>
    /// The text of each listed query. $1 server, $2 start, $3 end, then two parallel arrays (database, query_id). The text dimension first
    /// and the newest inline text second, as the grid resolves it (the same fallback the plan-force pre-check reads); cut to
    /// <see cref="QueryTextChars"/> + 1 characters so the caller can tell a cut.
    /// </summary>
    public const string QueryTextSql = @"
SELECT k.database_name, k.query_id, LEFT(COALESCE(
    (SELECT x.query_sql_text FROM query_store_text AS x WHERE x.server_id = $1 AND x.database_name = k.database_name AND x.query_id = k.query_id),
    (SELECT s.query_text FROM query_store_stats AS s
      WHERE s.server_id = $1 AND s.database_name = k.database_name AND s.query_id = k.query_id
      AND s.collection_time >= $2 AND s.collection_time <= $3 AND s.query_text IS NOT NULL
      ORDER BY s.collection_time DESC, s.collection_id DESC LIMIT 1)), 1001)
FROM unnest($4::text[], $5::bigint[]) AS k(database_name, query_id)";

    /// <summary>Reads the hints of one server in the window <paramref name="start"/> to <paramref name="end"/> (UTC), optionally narrowed to databases and one query.</summary>
    public static async Task<QueryStoreHintRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DatabaseFilter databases, long? queryId, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var from = DateTime.SpecifyKind(start, DateTimeKind.Unspecified);
        var to = DateTime.SpecifyKind(end, DateTimeKind.Unspecified);

        var captures = new List<(string Database, DateTime Newest)>();
        await using (var command = postgres.CreateCommand(CapturesSql(databases)))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
            command.Parameters.Add(databases.Parameter());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                captures.Add((reader.GetString(0), reader.GetDateTime(1)));
        }

        var hints = new List<QueryStoreHintFact>();
        var counts = new Dictionary<string, (int Total, int Failing)>(StringComparer.Ordinal);
        if (captures.Count > 0)
        {
            await using (var countCommand = postgres.CreateCommand(CountsSql(databases)))
            {
                countCommand.CommandTimeout = commandTimeoutSeconds;
                countCommand.Parameters.AddWithValue(serverId);
                countCommand.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
                countCommand.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
                countCommand.Parameters.Add(databases.Parameter());
                countCommand.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId.HasValue ? queryId.Value : DBNull.Value });
                await using var countReader = await countCommand.ExecuteReaderAsync(cancellationToken);
                while (await countReader.ReadAsync(cancellationToken))
                    counts[countReader.GetString(0)] = (checked((int)countReader.GetInt64(1)), checked((int)countReader.GetInt64(2)));
            }

            await using var command = postgres.CreateCommand(HintsSql(databases));
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, from);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, to);
            command.Parameters.Add(databases.Parameter());
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId.HasValue ? queryId.Value : DBNull.Value });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var lastSeen = reader.GetDateTime(7);
                hints.Add(new QueryStoreHintFact(
                    reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.IsDBNull(5) ? 0 : reader.GetInt64(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6),
                    reader.IsDBNull(8) ? lastSeen : reader.GetDateTime(8),
                    lastSeen));
            }
        }

        var captureNodes = captures
            .Select(c => new QueryStoreHintCapture(c.Database, c.Newest, counts.TryGetValue(c.Database, out var n) ? n.Total : 0))
            .ToList();
        var ordered = hints
            .OrderBy(h => h.Failing ? 0 : 1).ThenBy(h => h.DatabaseName, StringComparer.Ordinal).ThenBy(h => h.QueryId).ThenBy(h => h.HintId)
            .ToList();
        return new QueryStoreHintRead(ordered, captureNodes, counts.Values.Sum(c => c.Total), counts.Values.Sum(c => c.Failing));
    }

    /// <summary>
    /// Adds the query text to the listed hints (one lookup for the page, not per hint). A query with no stored text keeps null.
    /// </summary>
    public static async Task<IReadOnlyList<QueryStoreHintFact>> WithQueryTextAsync(
        NpgsqlDataSource postgres, int serverId, IReadOnlyList<QueryStoreHintFact> listed, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        ArgumentNullException.ThrowIfNull(listed);
        if (listed.Count == 0)
            return listed;

        var keys = listed.Select(h => (h.DatabaseName, h.QueryId)).Distinct().ToList();
        var text = new Dictionary<(string, long), string?>();
        await using (var command = postgres.CreateCommand(QueryTextSql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, keys.Select(k => k.DatabaseName).ToArray());
            command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bigint, keys.Select(k => k.QueryId).ToArray());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                text[(reader.GetString(0), reader.GetInt64(1))] = reader.IsDBNull(2) ? null : reader.GetString(2);
        }

        return listed
            .Select(h =>
            {
                if (!text.TryGetValue((h.DatabaseName, h.QueryId), out var t) || t is null)
                    return h;
                var cut = t.Length > QueryTextChars;
                return h with { QueryText = cut ? t[..QueryTextChars] : t, QueryTextTruncated = cut };
            })
            .ToList();
    }
}
