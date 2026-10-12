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
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The write half of the per-collection procedure totals (#5638): the one statement that brings
/// <see cref="ProcedureStatsTotals.Table"/> up to date after a <c>collect.procedure_stats</c> batch. The caller is
/// <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, the only code that writes <c>collect.procedure_stats</c>.
///
/// <para><b>Atomic with the COPY, and fatal to it.</b> <see cref="UpsertBatchAsync"/> runs on the COPY's own transaction, after the COPY
/// and the dimension flush and before the commit, with no savepoint and no catch (the hour ledger's shape, #4605): a fault throws out of
/// the batch, the transaction rolls back and the batch's raw rows are not stored. Rows and totals therefore commit together or not at all,
/// and the read can trust a totals row to equal its collection's rows. The cost of a fault is the batch's sample, which
/// <c>StoreWriteReattempt</c> declines to re-run because the delta baselines have moved.</para>
///
/// <para><b>Exact when a collection spans batches.</b> The statement recomputes from the rows the store holds for the collection
/// (those of earlier committed batches and this batch's, visible on this transaction), so it never adds, and a second batch at the
/// same <c>collection_time</c> or a re-attempt writes the same totals. A batch that wrote no rows writes no totals: the read keeps
/// its <c>collection_log</c> axis for a run that stored nothing.</para>
/// </summary>
public static class ProcedureStatsTotalsWriter
{
    /// <summary>
    /// Upserts the totals of the collection stamped <paramref name="collectionTime"/> for <paramref name="serverId"/> on the COPY's
    /// transaction (<see cref="ProcedureStatsTotals.UpsertSql"/>). A <paramref name="rowsWritten"/> of 0 or less writes nothing and costs
    /// no round trip. Returns the totals rows the statement wrote (one per database in the collection).
    /// </summary>
    public static async Task<int> UpsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        int rowsWritten,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        if (rowsWritten <= 0)
        {
            return 0;
        }

        await using var command = new NpgsqlCommand(ProcedureStatsTotals.UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });

        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` - see PgCollectorRowWriter. */
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
