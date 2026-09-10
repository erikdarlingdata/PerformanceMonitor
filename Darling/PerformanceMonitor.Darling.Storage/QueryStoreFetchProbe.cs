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
/// Executes the two touch-and-probe statements for one database's cycle (#2312): the liveness refresh the
/// dimension GC depends on and the missing-set answer the activity-driven fetch runs on, one round trip
/// each. The runner hands in the cycle's distinct referenced ids with their live hashes; what comes back is
/// the fetch list — <c>!Resolved || HashStale</c> — and nothing else needs to be consulted, because the
/// store IS the watermark.
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

    public static Task<List<FetchProbeVerdict>> TouchAndProbeTextsAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<(long QueryId, string? QueryHash)> references,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
        => ExecuteAsync(connection, QueryStoreTextStore.TouchAndProbeSql, serverId, databaseName, references, collectionTimeUtc, commandTimeoutSeconds, cancellationToken);

    private static async Task<List<FetchProbeVerdict>> ExecuteAsync(
        NpgsqlConnection connection,
        string sql,
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
        using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverIds);
        command.Parameters.AddWithValue(databases);
        command.Parameters.AddWithValue(ids);
        command.Parameters.AddWithValue(hashes);
        /* Naive(), the #1969 trap: a Kind=Utc value infers timestamptz and Postgres converts it into the
           session zone on the way into the naive last_seen columns — hours of silent skew on the exact
           stamp the GC sweeps. */
        command.Parameters.AddWithValue(QueryStorePlanMap.Naive(collectionTimeUtc));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
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
