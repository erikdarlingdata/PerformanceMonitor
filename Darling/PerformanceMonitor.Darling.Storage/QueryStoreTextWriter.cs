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
using PerformanceMonitor.Common;

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
    ///
    /// <para>#4348: each text goes through the statement filter before it is stored, and the stored value is
    /// the filtered one (a named statement is the marker). A text the filter could NOT judge because its
    /// per-batch budget ran out (<see cref="SensitiveStatements.Session.TryText"/> false) is DROPPED from the
    /// batch instead of stored: this store is write-once per (query_id, query_hash), so a stored marker would
    /// stand for that statement for good, while an absent row is exactly what the missing-set probe reads as
    /// "fetch me again", and the next cycle judges it under a fresh budget. A dropped id is not in the returned
    /// list, so the caller keeps it owed. The caller's byte accounting measures the raw fetch and is not
    /// affected. <paramref name="scrub"/> is the cycle's session; null starts a standalone one for this batch.</para>
    /// </summary>
    public static async Task<IReadOnlyList<long>> WriteAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<FetchedQueryText> texts,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken = default,
        SensitiveStatements.Session? scrub = null)
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

        scrub ??= new SensitiveStatements.Session();
        var serverIds = new List<int>(texts.Count);
        var databases = new List<string>(texts.Count);
        var queryIds = new List<long>(texts.Count);
        var bodies = new List<string?>(texts.Count);
        var hashes = new List<string?>(texts.Count);
        var stamps = new List<DateTime>(texts.Count);

        /* Naive() on the stamp, not the raw UTC value: last_seen is a ::timestamp parameter, and Npgsql
           infers timestamptz from a Kind=Utc value and lets Postgres convert it into the session zone on the
           way in (#1969). A last_seen written at the wrong hour ages rows out ahead of the facts that
           reference them, and does it silently. */
        var stamp = QueryStorePlanMap.Naive(collectionTimeUtc);

        foreach (var text in texts)
        {
            /* #4348: judged HERE, before the row is built, so what is stored is the filtered value. A text the
               budget left unjudged is dropped from the batch (see the method comment), not stored as the marker. */
            if (!scrub.TryText(text.QueryText, out var body))
            {
                continue;
            }

            landed.Add(text.QueryId);

            serverIds.Add(serverId);
            databases.Add(databaseName);
            queryIds.Add(text.QueryId);
            bodies.Add(body);
            hashes.Add(text.QueryHash);
            stamps.Add(stamp);
        }

        if (landed.Count == 0)
        {
            return landed;
        }

        /* #2776: explicit store-side budget, same reason as the plan writer. Text bodies are not compressed
           and carry no decompression cost on the way in, so this upsert is the cheaper of the two writes —
           but it was inheriting the same unchosen 30s default, and a cancel here has the identical effect:
           the ids read as still-missing and the target re-ships the same statements next cycle. */
        using var upsert = new NpgsqlCommand(QueryStoreTextStore.UpsertSql, connection) { CommandTimeout = commandTimeoutSeconds };
        upsert.Parameters.AddWithValue(serverIds.ToArray());
        upsert.Parameters.AddWithValue(databases.ToArray());
        upsert.Parameters.AddWithValue(queryIds.ToArray());
        upsert.Parameters.AddWithValue(bodies.ToArray());
        upsert.Parameters.AddWithValue(hashes.ToArray());
        upsert.Parameters.AddWithValue(stamps.ToArray());
        await upsert.ExecuteNonQueryAsync(cancellationToken);

        return landed;
    }
}
