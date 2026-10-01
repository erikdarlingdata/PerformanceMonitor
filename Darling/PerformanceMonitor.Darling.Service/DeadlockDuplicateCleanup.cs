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
using Npgsql;
using NpgsqlTypes;

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
/// <para><b>Batches are one (server, day).</b> The groups come from <c>timescaledb_information.chunks</c> range
/// metadata crossed with the servers that have rows (never a scan of the hypertable), with a bounded per-server
/// range as the fallback on a store without TimescaleDB. Each batch is one transaction that first lifts the
/// decompress-tuple limit for that transaction only (a compressed chunk is decompressed by the DELETE, and the
/// literal server and day bounds confine that to one segment of one chunk), then deletes. The read reaches back
/// one day, so a keeper stored before midnight is seen, but the delete is confined to the target day.</para>
///
/// <para><b>No primary key here.</b> <c>deadlock_id</c> is stamped per row by <c>CollectionIdGenerator</c>, a
/// process-wide counter, so a row is addressed by <c>(collection_time, deadlock_id)</c>. If two rows ever shared
/// that pair, a delete keyed on it could remove a keeper with its copy, so the join also repeats the identity
/// (time and graph) and a pair that is not unique in the window is left alone. That can only narrow the delete.</para>
///
/// <para><b>Failure is isolated and retried.</b> A per-batch catch logs the server id and SQLSTATE only, never
/// the exception text. The marker is written only when every batch succeeded, so a partial run starts over on the
/// next start; a second run deletes nothing anyway.</para>
/// </summary>
public static class DeadlockDuplicateCleanup
{
    /// <summary>The owner name in <c>collect.collector_state</c>, distinct from every scrub's.</summary>
    public const string StateCollectorName = "deadlock_duplicate_cleanup";

    /// <summary>The marker's one key: <see cref="CleanupVersion"/> as text.</summary>
    public const string CleanupVersionStateKey = "cleanup_version";

    /// <summary>Bumped if the identity ever changes in a way a prior run did not cover.</summary>
    public const int CleanupVersion = 1;

    /// <summary>Each read's deadline.</summary>
    internal const int ReadTimeoutSeconds = 300;

    /// <summary>One (server, day) delete's deadline.</summary>
    internal const int DeleteBatchTimeoutSeconds = 120;

    /// <summary>The marker read/write's deadline.</summary>
    internal const int MarkerTimeoutSeconds = 30;

    /// <summary>What one run found and did, for the caller's summary log line.</summary>
    public sealed class Summary
    {
        public static readonly Summary NoOp = new(alreadyDone: true, 0, 0);

        internal Summary(bool alreadyDone, int rowsRemoved, int daysVisited)
        {
            AlreadyDone = alreadyDone;
            RowsRemoved = rowsRemoved;
            DaysVisited = daysVisited;
        }

        /// <summary>True when the marker already named the current <see cref="CleanupVersion"/>.</summary>
        public bool AlreadyDone { get; }

        /// <summary>Rows deleted from <c>collect.deadlocks</c>.</summary>
        public int RowsRemoved { get; }

        /// <summary>(Server, day) batches visited.</summary>
        public int DaysVisited { get; }
    }

    private const string MarkerGetSql = @"
SELECT state_value
FROM collect.collector_state
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
AND   ch.hypertable_name = 'deadlocks'";

    private const string PlainTableServerRangeSql = @"
SELECT min(collection_time), max(collection_time)
FROM collect.deadlocks
WHERE server_id = $1";

    /* $1 server, $2 day start, $3 day end. The window reads from $2 - 1 day so a keeper stored just before
       midnight is seen; the delete itself is confined to [$2, $3). key_count = 1 leaves alone any row whose
       (collection_time, deadlock_id) is shared, so the join below can never match a keeper. */
    private const string DeleteSql = @"
DELETE FROM collect.deadlocks d
USING (
    SELECT collection_time, deadlock_id, deadlock_time, deadlock_graph_xml,
           row_number() OVER (PARTITION BY server_id, deadlock_time, deadlock_graph_xml
                              ORDER BY collection_time, deadlock_id) AS rn,
           count(*) OVER (PARTITION BY collection_time, deadlock_id) AS key_count
    FROM collect.deadlocks
    WHERE server_id = $1
    AND   collection_time >= $2 - INTERVAL '1 day' AND collection_time < $3
    AND   deadlock_time IS NOT NULL
    AND   deadlock_graph_xml IS NOT NULL AND deadlock_graph_xml <> ''
) x
WHERE x.rn > 1
AND   x.key_count = 1
AND   d.server_id = $1
AND   d.collection_time = x.collection_time AND d.deadlock_id = x.deadlock_id
AND   d.deadlock_time = x.deadlock_time AND d.deadlock_graph_xml = x.deadlock_graph_xml
AND   d.collection_time >= $2 AND d.collection_time < $3";

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
            return new Summary(alreadyDone: false, 0, 0);
        }

        var failedServerIds = new SortedSet<int>();
        foreach (var (serverId, day) in serverDays)
        {
            if (failedServerIds.Contains(serverId))
            {
                continue;
            }

            try
            {
                removed += await DeleteDayAsync(connection, serverId, day, cancellationToken);
                visited++;
            }
            catch (NpgsqlException ex)
            {
                logger?.LogWarning(
                    "deadlock_duplicate_cleanup: server {ServerId} failed on {Day:yyyy-MM-dd} with SQLSTATE {SqlState}; skipping this server for the rest of the run, will retry on the next start",
                    serverId, day, ex.SqlState);
                failedServerIds.Add(serverId);
                failed = true;
            }
        }

        if (!failed)
        {
            await WriteMarkerAsync(connection, currentVersion, cancellationToken);
        }
        else
        {
            logger?.LogWarning(
                "deadlock_duplicate_cleanup: {FailedCount} server(s) failed this run ({FailedServerIds}); leaving the marker unwritten so the next start retries them",
                failedServerIds.Count, string.Join(",", failedServerIds));
        }

        return new Summary(alreadyDone: false, removed, visited);
    }

    private static async Task<int> DeleteDayAsync(
        NpgsqlConnection connection, int serverId, DateTime day, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* The same guarded per-transaction lift PgStatementTextScrub uses: the literal server_id and day bounds
           confine decompression to one segment of one chunk. */
        await using (var setLocal = new NpgsqlCommand(
            "SELECT set_config('timescaledb.max_tuples_decompressed_per_dml_transaction', '0', true) " +
            "WHERE current_setting('timescaledb.max_tuples_decompressed_per_dml_transaction', true) IS NOT NULL",
            connection, transaction)
            { CommandTimeout = DeleteBatchTimeoutSeconds })
        {
            await setLocal.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var delete = new NpgsqlCommand(DeleteSql, connection, transaction) { CommandTimeout = DeleteBatchTimeoutSeconds };
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day });
        delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day.AddDays(1) });

        var deleted = await delete.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return deleted;
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

    private static async Task<string?> ReadMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerGetSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = CleanupVersionStateKey });

        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private static async Task WriteMarkerAsync(NpgsqlConnection connection, string version, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerUpsertSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = CleanupVersionStateKey });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = version });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
