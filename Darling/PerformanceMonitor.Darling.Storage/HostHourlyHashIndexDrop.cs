/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// V322 (#9863): drops <c>collect.idx_query_stats_host_hourly_hash</c>, the <c>(server_id, query_hash, bucket)</c> index of the hourly host
/// rollup. V287 creates it and V322 drops it, so a fresh store makes the empty index and removes it in the same ladder, and V287's text stays
/// as it shipped.
///
/// <para><b>No reader takes it.</b> The wide row joins the materialized units, and the query-tag filter, the one read by <c>query_hash</c>,
/// also carries <c>server_id</c> and the bucket range, which <c>ux_query_stats_host_hourly</c> leads with. The index's cost is the build's own
/// writes: every row the hourly build inserts lands on a random leaf of it, and those leaf reads are most of the build's block reads.</para>
///
/// <para><b>A short exclusive lock.</b> A plain <c>DROP INDEX</c> takes <c>ACCESS EXCLUSIVE</c> on the rollup for an instant, inside the
/// migration's transaction, so a build running at that moment makes the migration wait and the next build waits behind it. It cannot be
/// <c>CONCURRENTLY</c> inside a transaction. Idempotent.</para>
///
/// <para><b>An older build ignores the drop.</b> It never names the index. A build older than V287 never had it.</para>
/// </summary>
public static class HostHourlyHashIndexDrop
{
    /// <summary>
    /// The store schema version that drops the index. The one place the number is written: the migration, the schema version constant and the
    /// viewer's version map read it, so renumbering the rung is a change to this line.
    /// </summary>
    public const int Version = 322;

    /// <summary>The name of the dropped index.</summary>
    public const string IndexName = "idx_query_stats_host_hourly_hash";

    /// <summary>The DDL of the rung.</summary>
    public const string DropSql = "DROP INDEX IF EXISTS collect." + IndexName + ";";
}
