/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The write half of the per-collection procedure totals (#5638): the one statement that brings
/// <see cref="ProcedureStatsTotals.Table"/> up to date after a <c>collect.procedure_stats</c> batch. The caller is
/// <c>DarlingCollectorRunner.CopyBatchOnceAsync</c>, the only code that writes <c>collect.procedure_stats</c>.
///
/// <para><b>Atomic with the COPY, and fatal to it.</b> <see cref="UpsertBatchAsync"/> runs the batch's own upsert on the COPY's own
/// transaction, after the COPY and the dimension flush and before the commit, with no savepoint and no catch (the hour ledger's shape,
/// #4605): a fault throws out of the batch, the transaction rolls back and the batch's raw rows are not stored. Rows and totals
/// therefore commit together or not at all, and the read can trust a totals row to equal its collection's rows. The cost of a fault is
/// the batch's sample, which <c>StoreWriteReattempt</c> declines to re-run because the delta baselines have moved.</para>
///
/// <para><b>The gap fill is the exception: it can never fail the batch (seat review of #5706).</b> It totals every collection a
/// pre-V176 binary wrote after the server's newest totals row, which after a rollback that ran for days can be millions of rows. If it
/// failed the batch, the next batch would see the same anchor and the same gap and fail again, and procedure_stats collection would
/// stop for that server for good. So it runs behind a savepoint (the #3953 shape), under a server-side <c>statement_timeout</c> (half
/// the command timeout, so the server cancels first and Npgsql's client-side cancel, which can break the connection, never fires). A
/// fault rolls back to the savepoint, logs a warning naming the server and the gap range, and lets the batch's own upsert run. The
/// batch commits and its own totals row becomes the new anchor, and the anchor takes rows AT the batch's time too
/// (<see cref="ProcedureStatsTotals.GapProbeSql"/>), so no later batch of the same cycle (Azure writes one per database at one
/// <c>collection_time</c>) and no later cycle tries that gap again: a gap is attempted once per committed batch, never once per
/// batch of a cycle. A batch that rolls back leaves no anchor and so retries. Those collections stay out of the trend, as before
/// the fix.</para>
///
/// <para><b>The normal batch pays one extra statement (round 2, L1).</b> <see cref="ProcedureStatsTotals.GapProbeSql"/> reads the
/// anchor and probes for a gap in one round trip; the savepoint, the timeout change and the gap statement run only when a gap
/// exists. The probe is bounded by the index, so it can fail the batch only as the batch's own upsert can.</para>
///
/// <para><b>Exact when a collection spans batches.</b> The statement recomputes from the rows the store holds for the collection
/// (those of earlier committed batches and this batch's, visible on this transaction), so it never adds, and a second batch at the
/// same <c>collection_time</c> or a re-attempt writes the same totals. A batch that wrote no rows writes no totals: the read keeps
/// its <c>collection_log</c> axis for a run that stored nothing.</para>
/// </summary>
public static class ProcedureStatsTotalsWriter
{
    /// <summary>The savepoint the gap fill runs behind.</summary>
    internal const string GapSavepointName = "pst_gap";

    /// <summary>
    /// The databases a procedure_stats batch holds, for the read-back limit (<see cref="UpsertBatchAsync"/>'s
    /// <c>databaseNames</c>): one entry per distinct database, null for the NULL database. Matches any
    /// <see cref="IEnumerable{T}"/> of <see cref="ProcedureStatsCollector.Row"/>, so the runner's <c>List&lt;TRow&gt;</c> with
    /// <c>TRow</c> = Row qualifies on the server, Azure per-database and import paths alike. Anything else returns null, and a null
    /// collection reads the whole collection (correct, but unbounded).
    /// </summary>
    public static IReadOnlyCollection<string?>? DatabaseNamesOf(object? rows) =>
        rows is IEnumerable<ProcedureStatsCollector.Row> typed
            ? typed.Select(static row => (string?)row.DatabaseName).Distinct().ToList()
            : null;

    /// <summary>
    /// Upserts the totals of the collection stamped <paramref name="collectionTime"/> for <paramref name="serverId"/> on the COPY's
    /// transaction (<see cref="ProcedureStatsTotals.UpsertSql"/>), after trying to total any collection a pre-V176 binary left above the
    /// server's floor (<see cref="ProcedureStatsTotals.GapFillSql"/>, behind a savepoint: its failure is logged to
    /// <paramref name="logger"/> and never fails the batch). A <paramref name="rowsWritten"/> of 0 or less writes nothing and
    /// costs no round trip. <paramref name="databaseNames"/> are the databases the batch wrote (null entries are the NULL database); the
    /// read-back is limited to them, and a null collection reads the whole collection.
    /// <paramref name="gapStatementTimeoutMilliseconds"/> is the gap fill's server-side bound (default: half of
    /// <paramref name="commandTimeoutSeconds"/>, at least a second); tests shrink it. Returns the totals rows the upsert wrote (one per
    /// database in the batch), not counting gap rows.
    /// </summary>
    public static async Task<int> UpsertBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime collectionTime,
        int rowsWritten,
        int commandTimeoutSeconds,
        IReadOnlyCollection<string?>? databaseNames,
        CancellationToken cancellationToken,
        ILogger? logger = null,
        string? serverName = null,
        int? gapStatementTimeoutMilliseconds = null)
    {
        ArgumentNullException.ThrowIfNull(connection);
        ArgumentNullException.ThrowIfNull(transaction);

        if (rowsWritten <= 0)
        {
            return 0;
        }

        /* Naive-UTC storage: Npgsql 6+ rejects Kind=Utc against `timestamp` - see PgCollectorRowWriter. */
        var stamp = DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified);

        await TryGapFillAsync(
            connection, transaction, serverId, stamp, commandTimeoutSeconds,
            gapStatementTimeoutMilliseconds ?? Math.Max(1000, commandTimeoutSeconds * 500),
            logger, serverName, cancellationToken);

        await using var command = new NpgsqlCommand(ProcedureStatsTotals.UpsertSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });

        /* $3: the non-NULL names (NULL array = no limit); $4: whether the batch holds the NULL database. */
        var names = databaseNames?.Where(static name => name is not null).Distinct(StringComparer.Ordinal).ToArray();
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = names is null ? DBNull.Value : names });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Boolean, Value = databaseNames?.Any(static name => name is null) ?? false });
        return await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>
    /// The gap first: a collection an older binary wrote between the server's newest totals row (at or before this batch's time)
    /// and this batch. One probe statement reads the anchor and whether a gap exists (<see cref="ProcedureStatsTotals.GapProbeSql"/>);
    /// normally there is none and nothing else runs. Not limited to the batch's databases: those collections belong to nobody's batch.
    /// A gap runs behind <see cref="GapSavepointName"/> and a server-side <c>statement_timeout</c> that is restored on success (a
    /// <c>SET LOCAL</c> would otherwise outlive the savepoint and bound the batch's own upsert). A fault, a timeout included, rolls
    /// back to the savepoint (which also undoes the <c>SET LOCAL</c>) and is logged; only the caller's cancellation escapes.
    /// </summary>
    private static async Task TryGapFillAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int serverId,
        DateTime stamp,
        int commandTimeoutSeconds,
        int statementTimeoutMilliseconds,
        ILogger? logger,
        string? serverName,
        CancellationToken cancellationToken)
    {
        string previousTimeout;
        DateTime anchor;
        await using (var probe = new NpgsqlCommand(ProcedureStatsTotals.GapProbeSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
        {
            probe.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            probe.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
            await using var reader = await probe.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            previousTimeout = reader.GetString(0);

            /* No totals row yet (a server's first batch) or no gap after the anchor: the normal path, nothing more to run. */
            if (reader.IsDBNull(1) || !reader.GetBoolean(2))
            {
                return;
            }

            anchor = reader.GetDateTime(1);
        }

        await transaction.SaveAsync(GapSavepointName, cancellationToken);
        try
        {
            await SetStatementTimeoutAsync(
                connection, transaction, statementTimeoutMilliseconds.ToString(CultureInfo.InvariantCulture) + "ms", commandTimeoutSeconds, cancellationToken);

            /* The client's own deadline stays at the full command timeout, above the server's bound, so the server cancels first. */
            await using (var gap = new NpgsqlCommand(ProcedureStatsTotals.GapFillSql, connection, transaction) { CommandTimeout = commandTimeoutSeconds })
            {
                gap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
                gap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = stamp });
                gap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = anchor });
                await gap.ExecuteNonQueryAsync(cancellationToken);
            }

            await SetStatementTimeoutAsync(connection, transaction, previousTimeout, commandTimeoutSeconds, cancellationToken);
            await transaction.ReleaseAsync(GapSavepointName, cancellationToken);
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            /* The transaction is aborted until this runs; it also undoes the SET LOCAL above. */
            await transaction.RollbackAsync(GapSavepointName, cancellationToken);
            await transaction.ReleaseAsync(GapSavepointName, cancellationToken);

            logger?.LogWarning(
                ex,
                "Procedure totals gap fill (#5638) failed for server {ServerName} ({ServerId}): the collections after {GapAfter:o} and before {GapBefore:o} " +
                "were not totalled, so they stay out of the duration trend (as before the fix). The batch continues and its own totals row becomes the new anchor; " +
                "once this batch commits the gap is not retried, by a later batch of this cycle or of any other.",
                serverName, serverId, anchor, stamp);
        }
    }

    private static async Task SetStatementTimeoutAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string value,
        int commandTimeoutSeconds,
        CancellationToken cancellationToken)
    {
        await using var set = new NpgsqlCommand("SELECT set_config('statement_timeout', $1, true)", connection, transaction) { CommandTimeout = commandTimeoutSeconds };
        set.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = value });
        await set.ExecuteScalarAsync(cancellationToken);
    }
}
