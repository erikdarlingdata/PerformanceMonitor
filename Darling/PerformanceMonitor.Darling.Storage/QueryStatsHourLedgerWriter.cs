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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The write half of the hourly row-count ledger (#4605, plan lane 2): what a <c>collect.query_stats</c> batch adds to
/// <see cref="QueryStatsHourLedger.LedgerTable"/>, and the one statement that adds it. The ledger's table, upsert and
/// recount are <see cref="QueryStatsHourLedger"/>; the caller is <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, the
/// only code that writes <c>collect.query_stats</c>.
///
/// <para><b>What a batch adds, and where it is counted.</b> The number of rows the batch wrote whose
/// <c>sample_interval_seconds</c> is not 0 (a NULL counts, as the rollup counts it). The count is taken by
/// <see cref="PgCollectorRowWriter.CountNonZeroAt"/> at the exact write of that column, from the value the COPY sends,
/// at the payload position <see cref="IntervalPayloadIndex"/> resolves from the same <c>PayloadColumns</c> list the COPY's
/// column list is built from. Nothing here re-derives the interval from the row: <c>QueryStatsCollector.WritePayload</c>
/// computes it from the delta calculator, which moves a baseline as it does, and a second derivation could disagree with
/// the first.</para>
///
/// <para><b>One batch is one hour.</b> <c>CopyBatchOnceAsync</c> stamps every row of a batch with the same
/// <c>storedCollectionTime</c> local (it writes that one value into the <c>collection_time</c> column for each row and
/// takes no per-row time), so a batch cannot span two hours and a single upsert is exact.</para>
///
/// <para><b>Atomic with the COPY, and fatal to it.</b> <see cref="AddBatchAsync"/> runs on the COPY's own transaction,
/// after the COPY and the dimension flush and before the commit, with no savepoint: a fault in it throws out of the batch,
/// the transaction rolls back and the batch's raw rows are not stored. Raw and the ledger therefore commit together or not
/// at all, which is the invariant the count guard rests on. The cost of a fault is the batch's sample: the fault lands
/// in the unstamped region of <c>CopyBatchOnceAsync</c> (after the COPY block), which <c>StoreWriteReattempt</c> declines
/// because the row loop has run and the delta baselines have moved, so the collector's next cycle is the retry. Losing
/// the count silently instead would leave the ledger below the rollup and quietly fail the guard for the hour.</para>
/// </summary>
public static class QueryStatsHourLedgerWriter
{
    /// <summary>The payload column whose written value the ledger counts (the rollup's <c>WHERE sample_interval_seconds IS DISTINCT FROM 0</c>).</summary>
    public const string IntervalColumn = "sample_interval_seconds";

    /// <summary>
    /// The payload position of <see cref="IntervalColumn"/> in <paramref name="schema"/>'s <c>PayloadColumns</c>, the order
    /// the COPY and <see cref="PgCollectorRowWriter"/> both use. Throws when the column is missing, because a batch that
    /// cannot be counted must not be written: an uncounted batch is the one way the ledger falls below the rollup without
    /// a fault.
    /// </summary>
    public static int IntervalPayloadIndex(ICollectorSchemaInfo schema)
    {
        ArgumentNullException.ThrowIfNull(schema);

        for (var i = 0; i < schema.PayloadColumns.Count; i++)
        {
            if (string.Equals(schema.PayloadColumns[i].Name, IntervalColumn, StringComparison.Ordinal))
            {
                return i;
            }
        }

        throw new InvalidOperationException(
            $"Collector '{schema.Name}' declares no payload column '{IntervalColumn}', so its rows cannot be counted into {QueryStatsHourLedger.LedgerTable} (#4605).");
    }

    /// <summary>
    /// Fails the batch before its COPY completes when any row wrote the interval through an overload the tally does not
    /// see (#4605): an undercounted ledger is the one error the tally must not make silently. The runner calls this with
    /// the rows it wrote and <see cref="PgCollectorRowWriter.CountedWrites"/>; it is a method of its own so a test can
    /// fail the check, which a substring pin in the runner's source cannot do.
    /// </summary>
    public static void EnsureEveryRowTallied(string collectorName, int rowsWritten, long countedWrites)
    {
        if (countedWrites != rowsWritten)
        {
            throw new InvalidOperationException(
                $"The {collectorName} batch wrote {rowsWritten} rows but {countedWrites} integer values at " +
                $"'{IntervalColumn}', so its ledger count would be wrong (#4605).");
        }
    }

    /// <summary>
    /// Adds one batch's count to its hour on the COPY's transaction (<see cref="QueryStatsHourLedger.UpsertSql"/>).
    /// <paramref name="serverName"/> must be the storage name the COPY wrote into <c>server_name</c>, byte for byte, and
    /// <paramref name="collectionTime"/> the batch's stored <c>collection_time</c>. A <paramref name="count"/> of 0 or less
    /// writes nothing and costs no round trip: the rollup has no row for an hour with no counted rows. Returns the rows
    /// the statement affected (1, or 0 when it wrote nothing).
    /// </summary>
    public static async Task<int> AddBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        string serverName,
        DateTime collectionTime,
        long count,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);
        ArgumentNullException.ThrowIfNull(serverName);

        if (count <= 0)
        {
            return 0;
        }

        await using var command = new NpgsqlCommand(QueryStatsHourLedger.UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = serverName });

        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` - see PgCollectorRowWriter. */
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = count });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
