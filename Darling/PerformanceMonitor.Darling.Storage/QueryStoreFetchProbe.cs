/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One referenced id's probe verdict: does the store resolve it, and is the stored hash stale.</summary>
/// <param name="Id">plan_id for the plan probe, query_id for the text probe.</param>
/// <param name="Resolved">A store row exists — including the plan side's NULL-digest content-less markers,
/// which must read as known or they ride every cycle's fetch list forever.</param>
/// <param name="HashStale">The stored hash and the batch's live hash both exist and DIFFER — an in-place
/// rewrite (plans) or a post-reset renumbering (texts). Stale ids refetch even though they resolve.</param>
public readonly record struct FetchProbeVerdict(long Id, bool Resolved, bool HashStale);

/// <summary>
/// Executes the two touch-and-probes for one database's cycle (#2312): the liveness refresh the
/// dimension GC depends on and the missing-set answer the activity-driven fetch runs on, one round trip
/// each. The runner hands in the cycle's distinct referenced ids with their live hashes; what comes back is
/// the fetch list — <c>!Resolved || HashStale</c> — and nothing else needs to be consulted, because the
/// store IS the watermark.
///
/// <para>The plan side is one statement. The text side is three, sent as one batch
/// (<see cref="TouchAndProbeTextsAsync"/>).</para>
/// </summary>
public static class QueryStoreFetchProbe
{
    public static Task<List<FetchProbeVerdict>> TouchAndProbePlansAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<(long PlanId, string? PlanHash)> references,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(connection, QueryStorePlanMap.TouchAndProbeSql, serverId, databaseName, references, collectionTimeUtc, commandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// The text side's touch and probe (#9043): <see cref="QueryStoreTextStore.RefreshTextRowSql"/>, then
    /// <see cref="QueryStoreTextStore.TouchSeenSql"/>, then <see cref="QueryStoreTextStore.ProbeSql"/>, whose rows are the
    /// verdicts.
    ///
    /// <para>One <see cref="NpgsqlBatch"/>: one round trip, and PostgreSQL runs a batch as one transaction (the statements
    /// share a single Sync), so the three run in this order and commit together. That order is the lock order every writer
    /// of these two tables keeps, text rows first and seen rows second. A caller that is already inside a transaction keeps
    /// it; the batch then runs inside that one.</para>
    /// </summary>
    public static Task<List<FetchProbeVerdict>> TouchAndProbeTextsAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<(long QueryId, string? QueryHash)> references,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(connection, s_textStatements, serverId, databaseName, references, collectionTimeUtc, commandTimeoutSeconds, cancellationToken);

    private static readonly string[] s_textStatements =
    [
        QueryStoreTextStore.RefreshTextRowSql,
        QueryStoreTextStore.TouchSeenSql,
        QueryStoreTextStore.ProbeSql,
    ];

    private static Task<List<FetchProbeVerdict>> ExecuteAsync(
        NpgsqlConnection connection,
        string sql,
        int serverId,
        string databaseName,
        IReadOnlyList<(long Id, string? Hash)> references,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
        => ExecuteAsync(connection, [sql], serverId, databaseName, references, collectionTimeUtc, commandTimeoutSeconds, cancellationToken);

    /// <summary>Runs <paramref name="statements"/> in order as one batch, each bound with the same five values. The last
    /// one is the verdict read; the ones before it return no rows.</summary>
    private static async Task<List<FetchProbeVerdict>> ExecuteAsync(
        NpgsqlConnection connection,
        IReadOnlyList<string> statements,
        int serverId,
        string databaseName,
        IReadOnlyList<(long Id, string? Hash)> references,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (references is null)
        {
            throw new ArgumentNullException(nameof(references));
        }

        var verdicts = new List<FetchProbeVerdict>(references.Count);
        if (references.Count == 0)
        {
            return verdicts;
        }

        var serverIds = new int[references.Count];
        var databases = new string[references.Count];
        var ids = new long[references.Count];
        var hashes = new string?[references.Count];
        for (var i = 0; i < references.Count; i++)
        {
            serverIds[i] = serverId;
            databases[i] = databaseName;
            ids[i] = references[i].Id;
            hashes[i] = references[i].Hash;
        }

        /* #2776: an EXPLICIT store-side timeout, because the implicit one was the bug. Every command on
           this path previously fell through to Npgsql's 30s default, and a cancelled probe or write is
           indistinguishable to the caller from "these ids are still missing" — so the ids carry over and
           the target re-decompresses the identical plans next cycle, forever. The caller passes the
           collector's own budget so both halves of a fetch share one number. */
        /* Naive(), the #1969 trap: a Kind=Utc value infers timestamptz and Postgres converts it into the
           session zone on the way into the naive last_seen columns — hours of silent skew on the exact
           stamp the GC sweeps. */
        var stamp = QueryStorePlanMap.Naive(collectionTimeUtc);

        await using var batch = new NpgsqlBatch(connection) { Timeout = commandTimeoutSeconds };
        foreach (var statement in statements)
        {
            /* A parameter belongs to one command, so each statement gets its own five over the same arrays. */
            var command = new NpgsqlBatchCommand(statement);
            command.Parameters.AddWithValue(serverIds);
            command.Parameters.AddWithValue(databases);
            command.Parameters.AddWithValue(ids);
            command.Parameters.AddWithValue(hashes);
            command.Parameters.AddWithValue(stamp);
            batch.BatchCommands.Add(command);
        }

        /* The statements before the last return no result set, so the reader opens on the verdict rows. */
        await using var reader = await batch.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            verdicts.Add(new FetchProbeVerdict(
                reader.GetInt64(2),
                !reader.IsDBNull(3) && reader.GetBoolean(3),
                !reader.IsDBNull(4) && reader.GetBoolean(4)));
        }

        return verdicts;
    }
}
