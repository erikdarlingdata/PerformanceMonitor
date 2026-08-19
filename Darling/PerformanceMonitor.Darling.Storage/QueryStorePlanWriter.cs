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

/// <summary>One plan the plan-XML fetch landed, before it has been given a content digest.</summary>
/// <param name="PlanId">The Query Store plan id, unique within its database.</param>
/// <param name="PlanXml">The plan XML, or null — a plan too large to persist reads NULL and still ships, so the
/// watermark can advance past a plan whose content will never exist.</param>
/// <param name="PlanHash">SQL Server's <c>query_plan_hash</c>, readable without decompressing the plan, which is
/// what lets the re-verify cursor detect in-place rewrites on cheap columns alone.</param>
public readonly record struct FetchedPlan(long PlanId, string? PlanXml, string? PlanHash);

/// <summary>
/// Writes what the plan fetch landed: the XML into the shared <c>query_plan_dim</c> (gzip, content-keyed,
/// fleet-wide deduplicated) and one map row per plan pointing at it (#2210).
///
/// <para>Deliberately reuses <see cref="PayloadDimensionBatch"/> and <see cref="PayloadDimensionWriter"/> rather
/// than writing the dimension directly. That is the whole argument for putting Query Store plans in the shared
/// dimension instead of a table of their own: compression, content dedup across collectors, the GC, the
/// recompression pass and every reader already exist and are exercised by two other collectors. A Query Store
/// plan byte-identical to the same plan collected through <c>query_stats</c> costs nothing new.</para>
///
/// <para>Both writes go in ONE transaction, and the order inside it matters: the dimension row lands before the
/// map row that points at it. A map row referencing a digest that is not yet in the dimension is the
/// resolves-to-missing-content state this design exists to prevent — the transaction makes it unobservable, and
/// doing the dimension first means even a torn write leaves the recoverable side (content with nothing pointing
/// at it, which the GC reclaims on its own horizon) rather than the unrecoverable one.</para>
/// </summary>
public static class QueryStorePlanWriter
{
    /// <summary>
    /// Lands a fetch's plans for one database. Returns the plan_ids whose content actually stored, in the order
    /// they were supplied, which is what the caller feeds to
    /// <c>QueryStorePlanXmlState.AdvanceWatermark</c> — the watermark must reflect what LANDED, not what was
    /// selected, or a torn pass advances past content that never arrived.
    ///
    /// <para>A plan with NULL XML counts as landed and gets NO dimension row and NO map row: there is no content
    /// to key, and inventing a digest for absent content would make the map point at nothing. The watermark
    /// still advances past it, which is correct — that plan's XML will never exist, and stalling on it forever is
    /// the failure the budget predicate already had to be fixed for twice.</para>
    /// </summary>
    public static async Task<IReadOnlyList<long>> WriteAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<FetchedPlan> plans,
        DateTime collectionTimeUtc,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        if (plans is null)
        {
            throw new ArgumentNullException(nameof(plans));
        }

        var landed = new List<long>(plans.Count);
        if (plans.Count == 0)
        {
            return landed;
        }

        var batch = new PayloadDimensionBatch();
        var mapServerIds = new List<int>(plans.Count);
        var mapDatabases = new List<string>(plans.Count);
        var mapPlanIds = new List<long>(plans.Count);
        var mapDigests = new List<byte[]>(plans.Count);
        var mapHashes = new List<string?>(plans.Count);

        /* Naive() on the stamp, not the raw UTC value: these are ::timestamp parameters, and Npgsql would infer
           timestamptz from a Utc Kind and let Postgres convert into the session zone on the way in. See
           QueryStorePlanMap.Naive — a last_seen written at the wrong hour ages rows out ahead of the facts that
           reference them, which is silent. */
        var stamp = QueryStorePlanMap.Naive(collectionTimeUtc);

        foreach (var plan in plans)
        {
            landed.Add(plan.PlanId);

            if (string.IsNullOrEmpty(plan.PlanXml))
            {
                continue;
            }

            var digest = PayloadDimensions.Digest(plan.PlanXml!);
            batch.Add(PayloadDimensions.QueryPlanDimTable, digest, plan.PlanXml!);

            mapServerIds.Add(serverId);
            mapDatabases.Add(databaseName);
            mapPlanIds.Add(plan.PlanId);
            mapDigests.Add(digest);
            mapHashes.Add(plan.PlanHash);
        }

        if (mapPlanIds.Count == 0)
        {
            return landed;
        }

        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* Dimension FIRST — see the class comment on why the torn-write side matters. */
        await PayloadDimensionWriter.FlushAsync(connection, transaction, batch, stamp, cancellationToken);

        using (var upsert = new NpgsqlCommand(QueryStorePlanMap.UpsertSql, connection, transaction))
        {
            upsert.Parameters.AddWithValue(mapServerIds.ToArray());
            upsert.Parameters.AddWithValue(mapDatabases.ToArray());
            upsert.Parameters.AddWithValue(mapPlanIds.ToArray());
            upsert.Parameters.AddWithValue(mapDigests.ToArray());
            upsert.Parameters.AddWithValue(mapHashes.ToArray());
            upsert.Parameters.AddWithValue(CreateStamps(stamp, mapPlanIds.Count));
            await upsert.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        return landed;
    }

    private static DateTime[] CreateStamps(DateTime stamp, int count)
    {
        var stamps = new DateTime[count];
        for (var i = 0; i < count; i++)
        {
            stamps[i] = stamp;
        }

        return stamps;
    }
}
