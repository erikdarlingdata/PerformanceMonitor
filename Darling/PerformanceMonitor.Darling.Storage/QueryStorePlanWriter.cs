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
/// store can record the content-less marker instead of re-selecting the plan as missing forever (#2312).</param>
/// <param name="PlanHash">SQL Server's <c>query_plan_hash</c>, readable without decompressing the plan — the
/// stored baseline <c>QueryStorePlanMap.TouchAndProbeSql</c> compares live hashes against to catch in-place
/// rewrites on cheap columns alone.</param>
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
    /// Lands a fetch's plans for one database. Returns the plan_ids that landed, in the order they were
    /// supplied — the caller uses them to clear its budget-carry-over set, since anything landed is no
    /// longer missing.
    ///
    /// <para>A plan with NULL XML gets NO dimension row (there is no content to key, and inventing a digest
    /// for absent content would make the map point at nothing) but it DOES get a map row with a NULL digest
    /// — the #2312 content-less marker. Under store-as-watermark the map row IS the fact that the plan was
    /// fetched and the engine had nothing to give: without it the probe reads the plan as missing and the
    /// fetch re-selects it every cycle forever, which is the old oversized-plan stall reborn through the
    /// probe. Readers are unaffected — a NULL digest joins to no dimension row, which renders exactly like
    /// the absent content it records.</para>
    /// </summary>
    public static async Task<IReadOnlyList<long>> WriteAsync(
        NpgsqlConnection connection,
        int serverId,
        string databaseName,
        IReadOnlyList<FetchedPlan> plans,
        DateTime collectionTimeUtc,
        int commandTimeoutSeconds,
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
        var mapDigests = new List<byte[]?>(plans.Count);
        var mapHashes = new List<string?>(plans.Count);

        /* Naive() on the stamp, not the raw UTC value: these are ::timestamp parameters, and Npgsql would infer
           timestamptz from a Utc Kind and let Postgres convert into the session zone on the way in. See
           QueryStorePlanMap.Naive — a last_seen written at the wrong hour ages rows out ahead of the facts that
           reference them, which is silent. */
        var stamp = QueryStorePlanMap.Naive(collectionTimeUtc);

        foreach (var plan in plans)
        {
            landed.Add(plan.PlanId);

            byte[]? digest = null;
            if (!string.IsNullOrEmpty(plan.PlanXml))
            {
                digest = PayloadDimensions.Digest(plan.PlanXml!);
                batch.Add(PayloadDimensions.QueryPlanDimTable, digest, plan.PlanXml!);
            }

            mapServerIds.Add(serverId);
            mapDatabases.Add(databaseName);
            mapPlanIds.Add(plan.PlanId);
            mapDigests.Add(digest);
            mapHashes.Add(plan.PlanHash);
        }

        using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        /* #2776: THIS transaction is the one the 30s Npgsql default was cutting. The dimension flush below
           writes up to the whole per-pass plan-XML budget (12 MB) and the map upsert follows it, both inside
           this single transaction, on a store concurrently serving a 4-wide sweep — the longest store-side
           operation the collector performs. Both commands now carry the caller's explicit budget instead of
           inheriting a default nobody chose. */

        /* Dimension FIRST — see the class comment on why the torn-write side matters. */
        await PayloadDimensionWriter.FlushAsync(connection, transaction, batch, stamp, cancellationToken, commandTimeoutSeconds: commandTimeoutSeconds);

        using (var upsert = new NpgsqlCommand(QueryStorePlanMap.UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
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
