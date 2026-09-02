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

/// <summary>One statement's text as the fetch returned it.</summary>
/// <param name="QueryHash">SQL Server's <c>query_hash</c>, the renumbering detector (#2312): query_id is
/// only unique until a Query Store reset, so the stored hash is what lets the probe see that an id now
/// names a DIFFERENT statement and refetch its text.</param>
public readonly record struct FetchedQueryText(long QueryId, string? QueryText, string? QueryHash);

/// <summary>
/// Lands what the query-text fetch returned into <see cref="QueryStoreTextStore"/> (#2150).
///
/// <para>Much simpler than <see cref="QueryStorePlanWriter"/>, and the difference is the whole point of
/// keying this store the way it is keyed: there is no content dimension to write first, so there is no
/// torn-write ordering to reason about, no digest, and no transaction — a single upsert either lands or it
/// does not.</para>
/// </summary>
public static class QueryStoreTextWriter
{
    /// <summary>
    /// Lands a fetch's statement text for one database, returning the <c>query_id</c>s that stored, in the
    /// order supplied — the caller uses them to clear its budget-carry-over set, since anything landed is
    /// no longer missing (#2312).
    ///
    /// <para>Rows with null text are stored as null rather than skipped. Query Store does not produce them
    /// in practice, so this is about not having a special case to get wrong: a null in this store means "we
    /// fetched and there was nothing", the readers already <c>COALESCE</c> onto the fact row's own column,
    /// and the stored row is what stops the probe from re-selecting the id as missing forever.</para>
    /// </summary>
    public static async Task<IReadOnlyList<long>> WriteAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<FetchedQueryText> texts,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (texts is null)
        {
            throw new ArgumentNullException(nameof(texts));
        }

        var landed = new List<long>(texts.Count);
        if (texts.Count == 0)
        {
            return landed;
        }

        var serverIds = new int[texts.Count];
        var databases = new string[texts.Count];
        var queryIds = new long[texts.Count];
        var bodies = new string?[texts.Count];
        var hashes = new string?[texts.Count];
        var stamps = new DateTime[texts.Count];

        /* Naive() on the stamp, not the raw UTC value: last_seen is a ::timestamp parameter, and Npgsql
           infers timestamptz from a Kind=Utc value and lets Postgres convert it into the session zone on the
           way in (#1969). A last_seen written at the wrong hour ages rows out ahead of the facts that
           reference them, and does it silently. */
        var stamp = QueryStorePlanMap.Naive(collectionTimeUtc);

        for (var i = 0; i < texts.Count; i++)
        {
            var text = texts[i];
            landed.Add(text.QueryId);

            serverIds[i] = serverId;
            databases[i] = databaseName;
            queryIds[i] = text.QueryId;
            bodies[i] = text.QueryText;
            hashes[i] = text.QueryHash;
            stamps[i] = stamp;
        }

        /* #2776: explicit store-side budget, same reason as the plan writer. Text bodies are not compressed
           and carry no decompression cost on the way in, so this upsert is the cheaper of the two writes —
           but it was inheriting the same unchosen 30s default, and a cancel here has the identical effect:
           the ids read as still-missing and the target re-ships the same statements next cycle. */
        using var upsert = new NpgsqlCommand(QueryStoreTextStore.UpsertSql, connection) { CommandTimeout = commandTimeoutSeconds };
        upsert.Parameters.AddWithValue(serverIds);
        upsert.Parameters.AddWithValue(databases);
        upsert.Parameters.AddWithValue(queryIds);
        upsert.Parameters.AddWithValue(bodies);
        upsert.Parameters.AddWithValue(hashes);
        upsert.Parameters.AddWithValue(stamps);
        await upsert.ExecuteNonQueryAsync(cancellationToken);

        return landed;
    }
}
