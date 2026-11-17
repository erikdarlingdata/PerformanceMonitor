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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// A short read-only transaction with nested loops off, for the readers whose statement reads a window of a plain table that grows by appends
/// and joins it in memory (#8570 the forced plan list, #8909 the Query Store hints list). A plain table's statistics lag: autoanalyze waits for
/// 10 percent of the table to change, so on a busy store the newest hours of the table are often past the end of the <c>collection_time</c>
/// histogram. The planner then estimates every window scan at one row and picks nested loops that re-run an index scan of the window once per
/// row on the other side (33.5 s for 274 forces, 60 s and a timeout for 310 hints, against 45 ms and 0.5 s with nested loops off). These
/// statements read the window rows a few times and join them in memory, so a hash or merge join is right at every size, and setting this makes
/// the plan independent of the window estimate. The statement text is not changed, so its answer cannot change.
/// </summary>
internal static class NoNestLoopRead
{
    internal const string ReadOnlySql = "SET TRANSACTION READ ONLY";

    internal const string NoNestLoopSql = "SET LOCAL enable_nestloop = off";

    /// <summary>
    /// Opens the transaction a guarded statement runs in: read-only, with <see cref="NoNestLoopSql"/> set for its length. <c>SET LOCAL</c> ends with
    /// the transaction, so a caller that disposes it leaves no setting on the pooled connection.
    /// </summary>
    internal static async Task<NpgsqlTransaction> BeginAsync(NpgsqlConnection connection, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var transaction = await connection.BeginTransactionAsync(cancellationToken);
        try
        {
            foreach (var setting in new[] { ReadOnlySql, NoNestLoopSql })
            {
                await using var set = new NpgsqlCommand(setting, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
                await set.ExecuteNonQueryAsync(cancellationToken);
            }

            return transaction;
        }
        catch
        {
            await transaction.DisposeAsync();
            throw;
        }
    }
}
