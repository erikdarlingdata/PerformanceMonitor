/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// V322 (#9863): drops <c>collect.idx_query_stats_host_hourly_hash</c>, the <c>(server_id, query_hash, bucket)</c> index of the hourly host
/// rollup. V287 creates it and V322 drops it, so a fresh store makes the empty index and removes it in the same ladder, and V287's text stays
/// as it shipped.
///
/// <para><b>The one read it served.</b> The wide row joins the materialized units, so the only read by <c>query_hash</c> is the query-tag
/// filter. That filter also carries <c>server_id</c> and the bucket range, so <c>ux_query_stats_host_hourly</c> serves it, and without this
/// index it reads a wider range of that index, which is slower on a long window. The index's cost is the build's own writes: every row the
/// hourly build inserts lands on a random leaf of it, and those leaf reads are most of the build's block reads.</para>
///
/// <para><b>A short exclusive lock.</b> A plain <c>DROP INDEX</c> takes <c>ACCESS EXCLUSIVE</c> on the rollup for an instant, inside the
/// migration's transaction, so a build running at that moment makes the migration wait and the next build waits behind it. It cannot be
/// <c>CONCURRENTLY</c> inside a transaction. Idempotent.</para>
///
/// <para><b>The service re-asserts the drop on each start.</b> The rung runs once, so a store stamped V322 that holds the index again (an
/// operator who restored it for an older build, then upgraded again, or created it by hand) would keep it, and the viewer reads the index's
/// absence as the V322 mark, so it would read that store as V321. <see cref="PgMigrations.MigrateAsync(NpgsqlConnection, ILogger, CancellationToken)"/>
/// calls <see cref="ReassertAsync"/> after the ladder on every start, like the database-default <c>search_path</c> beside it. It reads the
/// catalog first and takes no lock while the index is absent, which is every start but the one that finds it.</para>
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

    /// <summary>
    /// Whether the index is there. <c>to_regclass</c> resolves the name without locking the table or the index and answers NULL for a
    /// name that does not resolve, a missing <c>collect</c> schema included, so the check costs a catalog read on a store that lacks it.
    /// </summary>
    public const string IndexPresentSql = "SELECT to_regclass('collect." + IndexName + "') IS NOT NULL";

    /// <summary>
    /// The <c>lock_timeout</c> of the re-assert's drop. The drop takes <c>ACCESS EXCLUSIVE</c> on the rollup, so one queued behind the
    /// hourly build would hold up every later reader of the rollup. It gives up at this bound instead, the failure is logged, and the next
    /// start retries it.
    /// </summary>
    public const int ReassertLockTimeoutSeconds = 5;

    /// <summary>The <c>CommandTimeout</c> of the catalog read and of the drop. The drop's own wait is bounded by <see cref="ReassertLockTimeoutSeconds"/>.</summary>
    private const int ReassertCommandTimeoutSeconds = 30;

    /// <summary>
    /// Drops the index when it is there, with <see cref="DropSql"/>, in its own transaction under <see cref="ReassertLockTimeoutSeconds"/>.
    /// Returns true when it dropped the index and false when the index was absent or the drop failed. A failure (a lock timeout, an
    /// owner-only privilege, a read-only session) is logged as a warning that carries the statement to run by hand and never fails the start,
    /// and the transaction rolls back, so the connection is clean for the caller.
    /// </summary>
    public static async Task<bool> ReassertAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        try
        {
            using (var probe = new NpgsqlCommand(IndexPresentSql, connection) { CommandTimeout = ReassertCommandTimeoutSeconds })
            {
                if (!(bool)(await probe.ExecuteScalarAsync(cancellationToken))!)
                {
                    return false;
                }
            }

            using var transaction = await connection.BeginTransactionAsync(cancellationToken);
            using (var limit = new NpgsqlCommand("SET LOCAL lock_timeout = '" + ReassertLockTimeoutSeconds + "s'", connection, transaction) { CommandTimeout = ReassertCommandTimeoutSeconds })
            {
                await limit.ExecuteNonQueryAsync(cancellationToken);
            }

            using (var drop = new NpgsqlCommand(DropSql, connection, transaction) { CommandTimeout = ReassertCommandTimeoutSeconds })
            {
                await drop.ExecuteNonQueryAsync(cancellationToken);
            }

            await transaction.CommitAsync(cancellationToken);
            logger?.LogInformation(
                "Dropped collect.{Index}, which this store (V{Version} or later) had again. The service drops it on each start.",
                IndexName, Version);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogWarning(
                "Could not drop collect.{Index} ({Message}). The viewer reads a store that holds it as one version behind until it is gone, " +
                "and the next start tries again. To drop it now, run as the owner of the table: {Drop}",
                IndexName, ex.Message, DropSql);
            return false;
        }
    }
}
