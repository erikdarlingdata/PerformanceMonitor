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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One-time removal of exact duplicate rows already stored in <c>collect.deadlocks</c>: an Azure SQL Database
/// registered at master could store one deadlock twice, once from the database's own session and once from the
/// server's telemetry. The write path now drops the second copy as it arrives; this removes the copies an older
/// build already stored. It is the mirror <see cref="PgStatementTextScrub"/> is for statement text: a background
/// job, not a migration, with its own marker row.
///
/// <para><b>This is a DELETE path, so the identity is deliberately narrow.</b> A row goes only when an EARLIER row
/// on the same server has the same <c>deadlock_time</c> and the byte-for-byte same <c>deadlock_graph_xml</c>
/// (no hash, so a collision cannot delete anything). The earliest <c>collection_time</c> stays, the lowest
/// <c>deadlock_id</c> breaking a tie. A row with a NULL <c>deadlock_time</c>, or a NULL or empty graph, is never a
/// candidate and is never a keeper. A graph that differs by one character is a different event and is kept.
/// This applies to every server: an exact duplicate is wrong anywhere, and off Azure the delete finds nothing.</para>
///
/// <para><b>Batches are one (server, day), and a day with no duplicate is only read.</b> The groups come from
/// <c>timescaledb_information.chunks</c> range metadata crossed with the servers that have rows (never a scan of
/// the hypertable), ordered by server then day, with a bounded per-server range as the fallback on a store
/// without TimescaleDB. Each (server, day) first runs a READ-ONLY candidate select (the same partition as the
/// delete would use: <c>rn &gt; 1 AND key_count = 1</c>). A day that finds nothing issues no delete and lifts no
/// limit, so a compressed chunk with no duplicate stays compressed and the first start after an upgrade does
/// not decompress the history. A day that finds candidates deletes exactly the listed rows, in small keyed
/// batches, each its own transaction that first lifts the decompress-tuple limit for that transaction only. The
/// read reaches back one day, so a keeper stored before midnight is seen, but the delete is confined to the
/// target day. One day is enough because the old re-read stored its copies within about ten minutes of each other.</para>
///
/// <para><b>Progress survives a restart.</b> After each (server, day) the last completed one is stored in this
/// cleanup's own <c>collector_state</c> rows, and the next start resumes after it, so a storm day that hits the
/// batch timeout does not make every later start redo everything. Progress stops advancing at the first failure
/// (a failed day is never skipped over), and it is cleared when the marker is written. The days that lost rows
/// are stored the same way, so the <c>deadlock_baseline</c> continuous aggregate is refreshed once over exactly
/// those days after the deletes (outside every delete transaction, since a refresh cannot run in one), and a
/// store without TimescaleDB or without the aggregate skips that.</para>
///
/// <para>A retention delete running at the same time is safe: it only ever removes whole old rows, a row that is
/// already gone matches nothing in the keyed delete, and a keeper it removes first leaves its copy as the
/// earliest remaining row for a later start to judge.</para>
///
/// <para><b>No primary key here.</b> <c>deadlock_id</c> is stamped per row by <c>CollectionIdGenerator</c>, a
/// process-wide counter, so a row is addressed by <c>(collection_time, deadlock_id)</c>. If two rows ever shared
/// that pair, a delete keyed on it could remove a keeper with its copy, so the join also repeats the identity
/// (time and graph) and a pair that is not unique in the window is left alone. That can only narrow the delete.</para>
///
/// <para><b>Failure is isolated and retried.</b> A per-batch catch logs the server id and SQLSTATE only, never
/// the exception text. The marker is written only when every batch succeeded, so a partial run resumes from its
/// stored progress on the next start; a second run deletes nothing anyway.</para>
/// </summary>
public static class DeadlockDuplicateCleanup
{
    /// <summary>The owner name in <c>collect.collector_state</c>, distinct from every scrub's.</summary>
    public const string StateCollectorName = "deadlock_duplicate_cleanup";

    /// <summary>The marker's one key: <see cref="CleanupVersion"/> as text.</summary>
    public const string CleanupVersionStateKey = "cleanup_version";

    /// <summary>The last completed (server, day), as <c>serverId|yyyy-MM-dd</c>; cleared with the marker.</summary>
    public const string ProgressStateKey = "progress";

    /// <summary>The first and last day that lost rows, as <c>yyyy-MM-dd|yyyy-MM-dd</c>, until the baseline is refreshed.</summary>
    public const string DeletedRangeStateKey = "deleted_range";

    /// <summary>Bumped if the identity ever changes in a way a prior run did not cover.</summary>
    public const int CleanupVersion = 1;

    /// <summary>Each read's deadline.</summary>
    internal const int ReadTimeoutSeconds = 300;

    /// <summary>One (server, day) delete's deadline.</summary>
    internal const int DeleteBatchTimeoutSeconds = 120;

    /// <summary>The continuous aggregate refresh's deadline.</summary>
    internal const int RefreshTimeoutSeconds = 300;

    /// <summary>Candidate rows listed, then deleted, per keyed batch.</summary>
    internal const int MaxCandidatesPerBatch = 200;

    /// <summary>The marker read/write's deadline.</summary>
    internal const int MarkerTimeoutSeconds = 30;

    /// <summary>What one run found and did, for the caller's summary log line.</summary>
    public sealed class Summary
    {
        public static readonly Summary NoOp = new(alreadyDone: true, 0, 0, 0, 0, false);

        internal Summary(bool alreadyDone, int rowsRemoved, int daysVisited, int daysWithCandidates, int deleteStatements, bool baselineRefreshed)
        {
            AlreadyDone = alreadyDone;
            RowsRemoved = rowsRemoved;
            DaysVisited = daysVisited;
            DaysWithCandidates = daysWithCandidates;
            DeleteStatements = deleteStatements;
            BaselineRefreshed = baselineRefreshed;
        }

        /// <summary>True when the marker already named the current <see cref="CleanupVersion"/>.</summary>
        public bool AlreadyDone { get; }

        /// <summary>Rows deleted from <c>collect.deadlocks</c>.</summary>
        public int RowsRemoved { get; }

        /// <summary>(Server, day) batches visited this run (days before the stored progress are not).</summary>
        public int DaysVisited { get; }

        /// <summary>Visited days whose read-only candidate select found at least one duplicate.</summary>
        public int DaysWithCandidates { get; }

        /// <summary>DELETE statements issued; zero on a store with no duplicate, whatever its size.</summary>
        public int DeleteStatements { get; }

        /// <summary>True when the <c>deadlock_baseline</c> aggregate was refreshed this run.</summary>
        public bool BaselineRefreshed { get; }
    }

    private const string MarkerGetSql = @"
SELECT state_value
FROM collect.collector_state
WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";

    private const string StateDeleteSql = @"
DELETE FROM collect.collector_state
WHERE server_id = $1 AND collector_name = $2 AND state_key = $3";

    private const string MarkerUpsertSql = @"
INSERT INTO collect.collector_state (server_id, collector_name, state_key, state_value, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (server_id, collector_name, state_key)
DO UPDATE SET state_value = EXCLUDED.state_value, updated_at = EXCLUDED.updated_at";

    private const string ChunkDaysSql = @"
SELECT DISTINCT d::date AS day
FROM timescaledb_information.chunks ch
CROSS JOIN LATERAL generate_series(
    ch.range_start AT TIME ZONE 'UTC',
    (ch.range_end AT TIME ZONE 'UTC') - INTERVAL '1 microsecond',
    INTERVAL '1 day') AS d
WHERE ch.hypertable_schema = 'collect'
AND   ch.hypertable_name = 'deadlocks'
ORDER BY 1";

    private const string PlainTableServerRangeSql = @"
SELECT min(collection_time), max(collection_time)
FROM collect.deadlocks
WHERE server_id = $1";

    /* READ-ONLY. $1 server, $2 day start, $3 day end, $4 the cap. The window reads from $2 - 1 day so a keeper
       stored just before midnight is seen; the candidates are confined to [$2, $3). key_count = 1 leaves alone any
       row whose (collection_time, deadlock_id) is shared, so a keyed delete can never match a keeper. The graph
       is compared with COLLATE "C" so the identity is ordinal whatever the database collation. #4348: a graph the
       statement filter withheld WHOLE is the marker text, the same for every such graph, so the text cannot tell two
       deadlocks at one time apart. A marker row's identity adds its victim process id and database (the two
       PARTITION BY CASEs, NULL for a real graph, so a real graph's identity is unchanged), and a marker row with no
       victim process id is never a candidate. */
    private const string CandidateSql = @"
SELECT collection_time, deadlock_id, deadlock_time, deadlock_graph_xml
FROM (
    SELECT collection_time, deadlock_id, deadlock_time, deadlock_graph_xml,
           row_number() OVER (PARTITION BY server_id, deadlock_time, deadlock_graph_xml COLLATE ""C"",
                              CASE WHEN deadlock_graph_xml COLLATE ""C"" = '" + SensitiveStatements.PlaceholderText + @"' THEN victim_process_id COLLATE ""C"" END,
                              CASE WHEN deadlock_graph_xml COLLATE ""C"" = '" + SensitiveStatements.PlaceholderText + @"' THEN COALESCE(database_name, '') COLLATE ""C"" END
                              ORDER BY collection_time, deadlock_id) AS rn,
           count(*) OVER (PARTITION BY collection_time, deadlock_id) AS key_count
    FROM collect.deadlocks
    WHERE server_id = $1
    AND   collection_time >= $2 - INTERVAL '1 day' AND collection_time < $3
    AND   deadlock_time IS NOT NULL
    AND   deadlock_graph_xml IS NOT NULL AND deadlock_graph_xml <> ''
    AND   (deadlock_graph_xml COLLATE ""C"" <> '" + SensitiveStatements.PlaceholderText + @"'
           OR (victim_process_id IS NOT NULL AND victim_process_id <> ''))
) x
WHERE x.rn > 1
AND   x.key_count = 1
AND   x.collection_time >= $2 AND x.collection_time < $3
ORDER BY x.collection_time, x.deadlock_id
LIMIT $4";

    /* $1 server, $2..$5 the listed rows' collection_time, deadlock_id, deadlock_time and graph, $6 day start, $7
       day end. Only the listed rows can match, and each must still carry the same time and the same graph. */
    private const string KeyedDeleteSql = @"
DELETE FROM collect.deadlocks d
USING unnest($2::timestamp[], $3::bigint[], $4::timestamp[], $5::text[])
      AS k(collection_time, deadlock_id, deadlock_time, graph)
WHERE d.server_id = $1
AND   d.collection_time = k.collection_time AND d.deadlock_id = k.deadlock_id
AND   d.deadlock_time = k.deadlock_time AND d.deadlock_graph_xml COLLATE ""C"" = k.graph COLLATE ""C""
AND   d.collection_time >= $6 AND d.collection_time < $7";

    private const string BaselineAggregateViewExistsSql =
        "SELECT to_regclass('timescaledb_information.continuous_aggregates') IS NOT NULL";

    private const string BaselineAggregateExistsSql = @"
SELECT EXISTS (SELECT 1 FROM timescaledb_information.continuous_aggregates
               WHERE view_schema = 'collect' AND view_name = '" + TimescaleSupport.DeadlockBaselineView + "')";

    private const string RefreshBaselineSql =
        "CALL refresh_continuous_aggregate('collect." + TimescaleSupport.DeadlockBaselineView + "', $1, $2)";

    /// <summary>
    /// Runs the cleanup once against <paramref name="postgres"/>, or returns <see cref="Summary.NoOp"/> at once if
    /// the marker already names the current <see cref="CleanupVersion"/>. A failure inside a batch is logged and
    /// leaves the marker unwritten; a failure reading the marker or the server list propagates to the caller,
    /// which isolates it.
    /// </summary>
    public static async Task<Summary> RunAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        var currentVersion = CleanupVersion.ToString(CultureInfo.InvariantCulture);
        if (await ReadMarkerAsync(connection, cancellationToken) == currentVersion)
        {
            return Summary.NoOp;
        }

        var serverIds = await ReadServerIdsAsync(connection, cancellationToken);
        var failed = false;
        var removed = 0;
        var visited = 0;
        var daysWithCandidates = 0;
        var deleteStatements = 0;

        List<(int ServerId, DateTime Day)> serverDays;
        try
        {
            serverDays = await ReadServerDaysAsync(connection, serverIds, cancellationToken);
        }
        catch (NpgsqlException ex)
        {
            logger?.LogWarning(
                "deadlock_duplicate_cleanup: reading collect.deadlocks' chunk ranges failed with SQLSTATE {SqlState}; skipping this run, will retry on the next start",
                ex.SqlState);
            return new Summary(alreadyDone: false, 0, 0, 0, 0, false);
        }

        var progress = ParseProgress(await ReadStateAsync(connection, ProgressStateKey, cancellationToken));
        var range = ParseRange(await ReadStateAsync(connection, DeletedRangeStateKey, cancellationToken));

        var failedServerIds = new SortedSet<int>();
        var progressFrozen = false;
        foreach (var (serverId, day) in serverDays)
        {
            if (failedServerIds.Contains(serverId))
            {
                continue;
            }

            if (progress is { } done && (serverId, day).CompareTo(done) <= 0)
            {
                continue;
            }

            try
            {
                var (dayRemoved, dayStatements) = await CleanDayAsync(connection, serverId, day, cancellationToken);
                removed += dayRemoved;
                deleteStatements += dayStatements;
                visited++;
                if (dayStatements > 0)
                {
                    daysWithCandidates++;
                }

                if (dayRemoved > 0)
                {
                    range = range is { } r ? (r.From < day ? r.From : day, r.To > day ? r.To : day) : (day, day);
                    await WriteStateAsync(connection, DeletedRangeStateKey, FormatRange(range.Value), cancellationToken);
                }

                if (!progressFrozen)
                {
                    await WriteStateAsync(connection, ProgressStateKey, FormatProgress(serverId, day), cancellationToken);
                }
            }
            catch (NpgsqlException ex)
            {
                logger?.LogWarning(
                    "deadlock_duplicate_cleanup: server {ServerId} failed on {Day:yyyy-MM-dd} with SQLSTATE {SqlState}; skipping this server for the rest of the run, will retry on the next start",
                    serverId, day, ex.SqlState);
                failedServerIds.Add(serverId);
                failed = true;

                // Never advance past a failed day: the next start resumes at the first day that did not complete.
                progressFrozen = true;
            }
        }

        // Refresh once, after every delete transaction has committed (a refresh cannot run inside one), and only
        // when some run deleted something that has not yet been reflected in the aggregate.
        var refreshed = false;
        if (range is { } deletedRange)
        {
            try
            {
                refreshed = await RefreshBaselineAsync(connection, deletedRange, cancellationToken);
                if (refreshed || !await BaselineAggregateExistsAsync(connection, cancellationToken))
                {
                    await DeleteStateAsync(connection, DeletedRangeStateKey, cancellationToken);
                }
            }
            catch (NpgsqlException ex)
            {
                logger?.LogWarning(
                    "deadlock_duplicate_cleanup: refreshing {View} failed with SQLSTATE {SqlState}; leaving the marker unwritten so the next start retries it",
                    TimescaleSupport.DeadlockBaselineView, ex.SqlState);
                failed = true;
            }
        }

        if (!failed)
        {
            await WriteMarkerAsync(connection, currentVersion, cancellationToken);
            await DeleteStateAsync(connection, ProgressStateKey, cancellationToken);
            await DeleteStateAsync(connection, DeletedRangeStateKey, cancellationToken);
        }
        else if (failedServerIds.Count > 0)
        {
            logger?.LogWarning(
                "deadlock_duplicate_cleanup: {FailedCount} server(s) failed this run ({FailedServerIds}); leaving the marker unwritten so the next start retries them",
                failedServerIds.Count, string.Join(",", failedServerIds));
        }

        return new Summary(alreadyDone: false, removed, visited, daysWithCandidates, deleteStatements, refreshed);
    }

    private static string FormatProgress(int serverId, DateTime day) =>
        serverId.ToString(CultureInfo.InvariantCulture) + "|" + day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static (int ServerId, DateTime Day)? ParseProgress(string? value)
    {
        var parts = value?.Split('|');
        if (parts is { Length: 2 }
            && int.TryParse(parts[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out var serverId)
            && DateTime.TryParseExact(parts[1], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day))
        {
            return (serverId, day);
        }

        // Unreadable progress is treated as none: starting over is always safe, it only costs time.
        return null;
    }

    private static string FormatRange((DateTime From, DateTime To) range) =>
        range.From.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + "|" + range.To.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

    private static (DateTime From, DateTime To)? ParseRange(string? value)
    {
        var parts = value?.Split('|');
        if (parts is { Length: 2 }
            && DateTime.TryParseExact(parts[0], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var from)
            && DateTime.TryParseExact(parts[1], "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var to))
        {
            return (from, to);
        }

        return null;
    }

    /// <summary>
    /// One (server, day): reads the candidates (no write, no limit lift), and only if there are some deletes exactly
    /// those rows in keyed batches until the read finds none. Returns the rows removed and the DELETEs issued.
    /// </summary>
    private static async Task<(int Removed, int DeleteStatements)> CleanDayAsync(
        NpgsqlConnection connection, int serverId, DateTime day, CancellationToken cancellationToken)
    {
        var removed = 0;
        var statements = 0;

        while (true)
        {
            var times = new List<DateTime>();
            var ids = new List<long>();
            var deadlockTimes = new List<DateTime>();
            var graphs = new List<string>();

            await using (var read = new NpgsqlCommand(CandidateSql, connection) { CommandTimeout = ReadTimeoutSeconds })
            {
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day });
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day.AddDays(1) });
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = MaxCandidatesPerBatch });

                await using var reader = await read.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    times.Add(reader.GetDateTime(0));
                    ids.Add(reader.GetInt64(1));
                    deadlockTimes.Add(reader.GetDateTime(2));
                    graphs.Add(reader.GetString(3));
                }
            }

            if (ids.Count == 0)
            {
                return (removed, statements);
            }

            var deleted = await DeleteListedAsync(connection, serverId, day, times, ids, deadlockTimes, graphs, cancellationToken);
            statements++;
            removed += deleted;

            // A listed row that no longer matches (a concurrent retention delete took it) would be read again
            // forever; stop at a batch that removed nothing. The next start judges what is left.
            if (deleted == 0)
            {
                return (removed, statements);
            }
        }
    }

    private static async Task<int> DeleteListedAsync(
        NpgsqlConnection connection, int serverId, DateTime day,
        List<DateTime> times, List<long> ids, List<DateTime> deadlockTimes, List<string> graphs,
        CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* The same guarded per-transaction lift PgStatementTextScrub uses, now only for a day that has listed
           duplicates: the literal server_id and day bounds confine decompression to one segment of one chunk. */
        await using (var setLocal = new NpgsqlCommand(
            "SELECT set_config('timescaledb.max_tuples_decompressed_per_dml_transaction', '0', true) " +
            "WHERE current_setting('timescaledb.max_tuples_decompressed_per_dml_transaction', true) IS NOT NULL",
            connection, transaction)
            { CommandTimeout = DeleteBatchTimeoutSeconds })
        {
            await setLocal.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var delete = new NpgsqlCommand(KeyedDeleteSql, connection, transaction) { CommandTimeout = DeleteBatchTimeoutSeconds };
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = times.ToArray() });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = ids.ToArray() });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Timestamp, Value = deadlockTimes.ToArray() });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = graphs.ToArray() });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day.AddDays(1) });

        var deleted = await delete.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return deleted;
    }

    private static async Task<bool> BaselineAggregateExistsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        // A store without TimescaleDB has no timescaledb_information schema, and a statement that names a missing
        // relation fails at parse time, so the view's existence is asked on its own first (to_regclass never throws).
        await using (var viewExists = new NpgsqlCommand(BaselineAggregateViewExistsSql, connection) { CommandTimeout = MarkerTimeoutSeconds })
        {
            if (!(bool)(await viewExists.ExecuteScalarAsync(cancellationToken))!)
            {
                return false;
            }
        }

        await using var command = new NpgsqlCommand(BaselineAggregateExistsSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>
    /// Refreshes the baseline aggregate over [first deleted day, last deleted day + 1 day) once. Returns false
    /// without error when this store has no such aggregate (no TimescaleDB, or the plain fallback view).
    /// </summary>
    private static async Task<bool> RefreshBaselineAsync(
        NpgsqlConnection connection, (DateTime From, DateTime To) range, CancellationToken cancellationToken)
    {
        if (!await BaselineAggregateExistsAsync(connection, cancellationToken))
        {
            return false;
        }

        await using var command = new NpgsqlCommand(RefreshBaselineSql, connection) { CommandTimeout = RefreshTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = range.From });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = range.To.AddDays(1) });
        await command.ExecuteNonQueryAsync(cancellationToken);
        return true;
    }

    private static async Task<List<(int ServerId, DateTime Day)>> ReadServerDaysAsync(
        NpgsqlConnection connection, List<int> serverIds, CancellationToken cancellationToken)
    {
        var chunkDays = await ReadChunkDaysAsync(connection, cancellationToken);
        var pairs = new List<(int ServerId, DateTime Day)>();

        if (chunkDays.Count > 0)
        {
            foreach (var serverId in serverIds)
            {
                foreach (var day in chunkDays)
                {
                    pairs.Add((serverId, day));
                }
            }

            return pairs;
        }

        /* No chunks: not a hypertable on this store shape. Each server's own bounded, index-served range. */
        foreach (var serverId in serverIds)
        {
            await using var read = new NpgsqlCommand(PlainTableServerRangeSql, connection) { CommandTimeout = ReadTimeoutSeconds };
            read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0) || reader.IsDBNull(1))
            {
                continue;
            }

            var minDay = reader.GetDateTime(0).Date;
            var maxDay = reader.GetDateTime(1).Date;
            for (var day = minDay; day <= maxDay; day = day.AddDays(1))
            {
                pairs.Add((serverId, day));
            }
        }

        return pairs;
    }

    private static async Task<List<DateTime>> ReadChunkDaysAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var days = new List<DateTime>();

        /* A store without the TimescaleDB extension has no timescaledb_information.chunks at all; to_regclass
           answers without throwing for a missing relation. */
        await using (var viewExists = new NpgsqlCommand(
            "SELECT to_regclass('timescaledb_information.chunks') IS NOT NULL", connection) { CommandTimeout = ReadTimeoutSeconds })
        {
            if (!(bool)(await viewExists.ExecuteScalarAsync(cancellationToken))!)
            {
                return days;
            }
        }

        await using var read = new NpgsqlCommand(ChunkDaysSql, connection) { CommandTimeout = ReadTimeoutSeconds };
        await using var reader = await read.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            days.Add(reader.GetDateTime(0));
        }

        return days;
    }

    private static async Task<List<int>> ReadServerIdsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var ids = new List<int>();
        await using var read = new NpgsqlCommand("SELECT DISTINCT server_id FROM collect.deadlocks ORDER BY server_id", connection) { CommandTimeout = ReadTimeoutSeconds };
        await using var reader = await read.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            ids.Add(reader.GetInt32(0));
        }

        return ids;
    }

    private static Task<string?> ReadMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken) =>
        ReadStateAsync(connection, CleanupVersionStateKey, cancellationToken);

    private static Task WriteMarkerAsync(NpgsqlConnection connection, string version, CancellationToken cancellationToken) =>
        WriteStateAsync(connection, CleanupVersionStateKey, version, cancellationToken);

    private static async Task<string?> ReadStateAsync(NpgsqlConnection connection, string key, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerGetSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = key });

        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private static async Task DeleteStateAsync(NpgsqlConnection connection, string key, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(StateDeleteSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = key });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task WriteStateAsync(NpgsqlConnection connection, string key, string value, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerUpsertSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = key });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = value });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
