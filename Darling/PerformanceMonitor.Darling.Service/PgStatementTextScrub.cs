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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// One-time scrub of statement text collected under an older build that predates the shared sensitive-
/// statement filter (#4348): <c>collect.pg_statement_text</c> (the store's own text lookup) and
/// <c>collect.pg_blocking_edges</c> (a monitored target's blocked/blocking backend text, captured verbatim
/// by <c>PgBlockingCollector</c>). Both are now filtered going forward at the fetch/collection query
/// (<see cref="PerformanceMonitor.Darling.Storage.PgStatementText"/>, <see cref="PgBlockingCollector"/>); this
/// is the mirror <see cref="PgSettingScrub"/> is for <c>collect.pg_server_config</c>, applied to rows a
/// pre-#4348 build already stored.
///
/// <para><b>A background job, not a migration</b> — same ruling <see cref="PgSettingScrub"/> follows: both
/// target tables already exist, so this reads candidate rows with the shared
/// <see cref="PgSensitiveStatementFilter.SensitiveStatementPattern"/>, replaces the matching text with
/// <see cref="PgSensitiveStatementFilter.PlaceholderText"/>, and only updates rows the pattern actually
/// names. Runs after startup, concurrently with the rest of the startup sequence, and never holds up
/// collection.</para>
///
/// <para><b><c>collect.pg_statement_text</c> is a plain table</b> keyed <c>(server_id, queryid)</c> — not a
/// hypertable, so its scrub needs no chunk-exclusion predicate. A per-server, LIMIT-chunked loop over
/// <c>queryid</c> keeps each UPDATE's batch small without a day dimension to group on.</para>
///
/// <para><b><c>collect.pg_blocking_edges</c> IS a hypertable.</b> Every entry in <c>CollectorCatalog.All</c>
/// (which includes <see cref="PgBlockingCollector"/>) is converted and compressed with
/// <c>compress_segmentby = server_id</c> by <c>TimescaleSupport.ConvertToHypertablesAsync</c>/
/// <c>ApplyCompressionPolicyAsync</c> — the same shape <c>collect.pg_server_config</c> gets. So its scrub
/// reuses <see cref="PgSettingScrub"/>'s (server, day) literal-bounds batching and the guarded
/// decompress-limit <c>set_config</c> line, for the identical H1/M3 reasons documented there.</para>
///
/// <para><b>The marker is its own row</b>, in <c>collect.collector_state</c> under the fleet sentinel
/// <see cref="DarlingObservability.FleetServerId"/>, with an owner name (<see cref="StateCollectorName"/>)
/// distinct from <see cref="PgSettingScrub.StateCollectorName"/> — this is a different scrub over different
/// tables, not a shared row. The stored value is <see cref="ScrubVersion"/> as text, so a later pattern
/// change is a version bump the marker compares against and the scrub runs again.</para>
///
/// <para><b>Failure is isolated and retried, never fatal</b> — a per-server catch logs the server id and
/// SQLSTATE only, then continues with the next server; the marker is written only when every server's rows
/// (across both tables) are confirmed scrubbed, so a partial run always retries from the top on the next
/// start.</para>
/// </summary>
public static class PgStatementTextScrub
{
    /// <summary>The owner name in <c>collect.collector_state</c> — distinct from <c>PgSettingScrub</c>'s,
    /// the same precedent (<c>QueryStoreBackfill</c> / <c>self_alert</c>) of a store-wide one-shot marker
    /// that is not a collector's declared key.</summary>
    public const string StateCollectorName = "pg_statement_text_scrub";

    /// <summary>The marker's one key: <see cref="ScrubVersion"/> as text.</summary>
    public const string ScrubVersionStateKey = "scrub_version";

    /// <summary>Bumped when the shared pattern changes in a way that would redact more (or differently)
    /// than a prior run already covered, so a store that already ran this scrub runs it again rather than
    /// trusting a stale "done".</summary>
    public const int ScrubVersion = 1;

    /// <summary>The candidate read's deadline for one server's <c>pg_statement_text</c> batch.</summary>
    internal const int CandidateReadTimeoutSeconds = 300;

    /// <summary>Each batch UPDATE's deadline.</summary>
    internal const int UpdateBatchTimeoutSeconds = 60;

    /// <summary>The marker read/write's deadline.</summary>
    internal const int MarkerTimeoutSeconds = 30;

    /// <summary>Rows fetched/updated per LIMIT batch against <c>collect.pg_statement_text</c>, keyed by
    /// <c>queryid</c> range within one server.</summary>
    internal const int StatementTextBatchSize = 500;

    /// <summary>Sub-batch cap within a single (server, day) group for <c>pg_blocking_edges</c> — the same
    /// cap <see cref="PgSettingScrub.MaxKeysPerUpdate"/> uses, for the identical reason.</summary>
    internal const int MaxKeysPerUpdate = 500;

    /// <summary>What one run of the scrub found and did, for the caller's summary log line.</summary>
    public sealed class Summary
    {
        public static readonly Summary NoOp = new(alreadyDone: true, 0, 0);

        internal Summary(bool alreadyDone, int statementTextRowsUpdated, int blockingEdgesRowsUpdated)
        {
            AlreadyDone = alreadyDone;
            StatementTextRowsUpdated = statementTextRowsUpdated;
            BlockingEdgesRowsUpdated = blockingEdgesRowsUpdated;
        }

        /// <summary>True when the marker already named the current <see cref="ScrubVersion"/> and the run
        /// did nothing else.</summary>
        public bool AlreadyDone { get; }

        /// <summary>Rows updated in <c>collect.pg_statement_text</c>.</summary>
        public int StatementTextRowsUpdated { get; }

        /// <summary>Rows updated in <c>collect.pg_blocking_edges</c>.</summary>
        public int BlockingEdgesRowsUpdated { get; }
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

    private static readonly string StatementTextCandidateSql = @"
SELECT server_id, queryid, query_text
FROM collect.pg_statement_text
WHERE server_id = $1
AND   queryid > $2
AND   query_text ~* '" + PgSensitiveStatementFilter.SensitiveStatementPattern.Replace("'", "''", StringComparison.Ordinal) + @"'
AND   query_text <> '" + PgSensitiveStatementFilter.PlaceholderText.Replace("'", "''", StringComparison.Ordinal) + @"'
ORDER BY queryid
LIMIT " + StatementTextBatchSize.ToString(CultureInfo.InvariantCulture);

    private const string StatementTextUpdateSql = @"
UPDATE collect.pg_statement_text
SET query_text = $1
WHERE server_id = $2
AND   queryid = ANY($3::bigint[])";

    /* new_blocked_query/new_blocking_query are computed IN SQL, the same CASE WHEN ... ~* ... THEN
       placeholder ELSE column END shape PgBlockingCollector's own SensitiveTextCase applies going
       forward, so the .NET side never re-implements PostgreSQL's ~* match against the POSIX-ARE pattern
       (which .NET Regex cannot parse) to decide which of the two columns on a row actually matched. */
    private static readonly string BlockingEdgesCandidateSql = @"
SELECT
    server_id, collection_time, collection_id,
    CASE WHEN blocked_query ~* '" + PgSensitiveStatementFilter.SensitiveStatementPattern.Replace("'", "''", StringComparison.Ordinal) + @"' THEN '" + PgSensitiveStatementFilter.PlaceholderText.Replace("'", "''", StringComparison.Ordinal) + @"' ELSE blocked_query END AS new_blocked_query,
    CASE WHEN blocking_query ~* '" + PgSensitiveStatementFilter.SensitiveStatementPattern.Replace("'", "''", StringComparison.Ordinal) + @"' THEN '" + PgSensitiveStatementFilter.PlaceholderText.Replace("'", "''", StringComparison.Ordinal) + @"' ELSE blocking_query END AS new_blocking_query
FROM collect.pg_blocking_edges
WHERE
    (blocked_query ~* '" + PgSensitiveStatementFilter.SensitiveStatementPattern.Replace("'", "''", StringComparison.Ordinal) + @"' AND blocked_query <> '" + PgSensitiveStatementFilter.PlaceholderText.Replace("'", "''", StringComparison.Ordinal) + @"')
 OR (blocking_query ~* '" + PgSensitiveStatementFilter.SensitiveStatementPattern.Replace("'", "''", StringComparison.Ordinal) + @"' AND blocking_query <> '" + PgSensitiveStatementFilter.PlaceholderText.Replace("'", "''", StringComparison.Ordinal) + @"')";

    private const string BlockingEdgesUpdateSql = @"
WITH batch AS (
    SELECT * FROM unnest($1::bigint[], $2::text[], $3::text[]) AS k(collection_id, new_blocked_query, new_blocking_query)
)
UPDATE collect.pg_blocking_edges t
SET blocked_query = b.new_blocked_query, blocking_query = b.new_blocking_query
FROM batch b
WHERE t.collection_id = b.collection_id
AND   t.collection_time >= $4 AND t.collection_time < $5
AND   t.server_id = $6";

    private sealed record BlockingEdgeRow(
        int ServerId, DateTime CollectionTime, long CollectionId, string? NewBlockedQuery, string? NewBlockingQuery);

    /// <summary>
    /// Runs the scrub once against <paramref name="postgres"/>, or returns <see cref="Summary.NoOp"/> at
    /// once if the marker already names the current <see cref="ScrubVersion"/>. Propagates a failure to the
    /// caller, which isolates it exactly like <see cref="PgSettingScrub.RunAsync"/> — see the type remarks.
    /// </summary>
    public static async Task<Summary> RunAsync(NpgsqlDataSource postgres, ILogger? logger, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        var currentVersion = ScrubVersion.ToString(CultureInfo.InvariantCulture);
        var marker = await ReadMarkerAsync(connection, cancellationToken);
        if (marker == currentVersion)
        {
            return Summary.NoOp;
        }

        var serverIds = await ReadServerIdsAsync(connection, cancellationToken);
        var failedServerIds = new SortedSet<int>();

        var statementTextRowsUpdated = 0;
        foreach (var serverId in serverIds)
        {
            try
            {
                statementTextRowsUpdated += await ScrubStatementTextForServerAsync(connection, serverId, cancellationToken);
            }
            catch (NpgsqlException ex)
            {
                /* Never the exception TEXT — just the server id and SQLSTATE, the same discipline
                   PgSettingScrub applies. */
                logger?.LogWarning(
                    "pg_statement_text_scrub: server {ServerId} failed scrubbing pg_statement_text with SQLSTATE {SqlState}; skipping this server for the rest of the run, will retry on the next start",
                    serverId, ex.SqlState);
                failedServerIds.Add(serverId);
            }
        }

        var blockingEdgesRowsUpdated = 0;
        if (failedServerIds.Count == 0)
        {
            var blockingCandidates = await ReadBlockingEdgesCandidatesAsync(connection, cancellationToken);
            var byServerDay = new SortedDictionary<(int ServerId, DateTime Day), List<BlockingEdgeRow>>();
            foreach (var c in blockingCandidates)
            {
                var key = (c.ServerId, c.CollectionTime.Date);
                if (!byServerDay.TryGetValue(key, out var list))
                {
                    list = new List<BlockingEdgeRow>();
                    byServerDay[key] = list;
                }
                list.Add(c);
            }

            foreach (var ((serverId, day), dayCandidates) in byServerDay)
            {
                if (failedServerIds.Contains(serverId))
                {
                    continue;
                }

                try
                {
                    for (var i = 0; i < dayCandidates.Count; i += MaxKeysPerUpdate)
                    {
                        var take = Math.Min(MaxKeysPerUpdate, dayCandidates.Count - i);
                        blockingEdgesRowsUpdated += await RunBlockingEdgesBatchAsync(
                            connection, dayCandidates, i, take, day, serverId, cancellationToken);
                    }
                }
                catch (NpgsqlException ex)
                {
                    logger?.LogWarning(
                        "pg_statement_text_scrub: server {ServerId} failed on {Day:yyyy-MM-dd} scrubbing pg_blocking_edges with SQLSTATE {SqlState}; skipping this server for the rest of the run, will retry on the next start",
                        serverId, day, ex.SqlState);
                    failedServerIds.Add(serverId);
                }
            }
        }

        if (failedServerIds.Count == 0)
        {
            await WriteMarkerAsync(connection, currentVersion, cancellationToken);
        }
        else
        {
            logger?.LogWarning(
                "pg_statement_text_scrub: {FailedCount} server(s) failed this run ({FailedServerIds}); leaving the marker unwritten so the next start retries them",
                failedServerIds.Count, string.Join(",", failedServerIds));
        }

        return new Summary(alreadyDone: false, statementTextRowsUpdated, blockingEdgesRowsUpdated);
    }

    private static async Task<int> ScrubStatementTextForServerAsync(
        NpgsqlConnection connection, int serverId, CancellationToken cancellationToken)
    {
        var totalUpdated = 0;
        var lastQueryId = long.MinValue;

        while (true)
        {
            var queryIds = new List<long>();
            await using (var read = new NpgsqlCommand(StatementTextCandidateSql, connection) { CommandTimeout = CandidateReadTimeoutSeconds })
            {
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                read.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = lastQueryId });

                await using var reader = await read.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    queryIds.Add(reader.GetInt64(1));
                }
            }

            if (queryIds.Count == 0)
            {
                break;
            }

            lastQueryId = queryIds[^1];

            await using (var update = new NpgsqlCommand(StatementTextUpdateSql, connection) { CommandTimeout = UpdateBatchTimeoutSeconds })
            {
                update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = PgSensitiveStatementFilter.PlaceholderText });
                update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = queryIds.ToArray() });

                totalUpdated += await update.ExecuteNonQueryAsync(cancellationToken);
            }

            if (queryIds.Count < StatementTextBatchSize)
            {
                break;
            }
        }

        return totalUpdated;
    }

    private static async Task<List<BlockingEdgeRow>> ReadBlockingEdgesCandidatesAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var rows = new List<BlockingEdgeRow>();
        await using var read = new NpgsqlCommand(BlockingEdgesCandidateSql, connection) { CommandTimeout = CandidateReadTimeoutSeconds };
        await using var reader = await read.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new BlockingEdgeRow(
                reader.GetInt32(0),
                reader.GetDateTime(1),
                reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }

        return rows;
    }

    private static async Task<int> RunBlockingEdgesBatchAsync(
        NpgsqlConnection connection, List<BlockingEdgeRow> candidates, int offset, int count, DateTime day, int serverId,
        CancellationToken cancellationToken)
    {
        var collectionIds = new long[count];
        var blockedQueries = new string?[count];
        var blockingQueries = new string?[count];

        for (var j = 0; j < count; j++)
        {
            var c = candidates[offset + j];
            collectionIds[j] = c.CollectionId;
            blockedQueries[j] = c.NewBlockedQuery;
            blockingQueries[j] = c.NewBlockingQuery;
        }

        /* Same guarded decompress-limit SET LOCAL PgSettingScrub uses, for the identical reason: the
           literal server_id + day-range predicates confine decompression to one target's segment in one
           day's chunk, so disabling the limit for this one transaction is safe. */
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        await using (var setLocal = new NpgsqlCommand(
            "SELECT set_config('timescaledb.max_tuples_decompressed_per_dml_transaction', '0', true) " +
            "WHERE current_setting('timescaledb.max_tuples_decompressed_per_dml_transaction', true) IS NOT NULL",
            connection, transaction)
            { CommandTimeout = UpdateBatchTimeoutSeconds })
        {
            await setLocal.ExecuteNonQueryAsync(cancellationToken);
        }

        await using var update = new NpgsqlCommand(BlockingEdgesUpdateSql, connection, transaction) { CommandTimeout = UpdateBatchTimeoutSeconds };
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = collectionIds });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = blockedQueries });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = blockingQueries });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = day.AddDays(1) });
        update.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });

        var updated = await update.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return updated;
    }

    private static async Task<List<int>> ReadServerIdsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var ids = new SortedSet<int>();
        await using (var read = new NpgsqlCommand("SELECT DISTINCT server_id FROM collect.pg_statement_text", connection) { CommandTimeout = CandidateReadTimeoutSeconds })
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                ids.Add(reader.GetInt32(0));
            }
        }

        await using (var read = new NpgsqlCommand("SELECT DISTINCT server_id FROM collect.pg_blocking_edges", connection) { CommandTimeout = CandidateReadTimeoutSeconds })
        await using (var reader = await read.ExecuteReaderAsync(cancellationToken))
        {
            while (await reader.ReadAsync(cancellationToken))
            {
                ids.Add(reader.GetInt32(0));
            }
        }

        return new List<int>(ids);
    }

    private static async Task<string?> ReadMarkerAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerGetSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = ScrubVersionStateKey });

        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result as string;
    }

    private static async Task WriteMarkerAsync(NpgsqlConnection connection, string scrubVersion, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(MarkerUpsertSql, connection) { CommandTimeout = MarkerTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingObservability.FleetServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = StateCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = ScrubVersionStateKey });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = scrubVersion });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });

        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
