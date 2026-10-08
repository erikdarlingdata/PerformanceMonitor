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
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The BRIN indexes on the Query Store interval tables and autovacuum (#5594): they are summarized by the service, never
/// by an autovacuum work item, so a running VACUUM of a big table is not cancelled for them.
///
/// <para><b>The defect.</b> <c>autosummarize = on</c> (#4862) queues a "BRIN summarize" autovacuum work item for every new
/// block range. The work item needs <c>SHARE UPDATE EXCLUSIVE</c> on the table, the same lock a VACUUM holds, so it waits;
/// after <c>deadlock_timeout</c> (1 s) PostgreSQL cancels the blocking autovacuum for any waiter. On a store that had just
/// drained 17.8 M rows from a 12.3 M-block table, the table's autovacuum was cancelled 8 times in 14 minutes and had not
/// finished since: each attempt got 170 K to 313 K blocks in. On a throwaway PostgreSQL 18 rig (a 1 GB table, a throttled
/// VACUUM, 3,000 rows inserted every 0.7 s) it was cancelled 69 times in 240 s and never finished.</para>
///
/// <para><b>Off, everywhere.</b> New builds say <c>autosummarize = off</c> (<see cref="QueryStoreIntervalWideBrinIndex"/>).
/// <see cref="TurnOffAutosummarizeAsync"/> turns it off on every existing BRIN leaf of a table (the legacy table's, each
/// day leaf's, DEFAULT's): it reads the catalog and alters only an index that is still on, so a converged store pays one
/// catalog read. It runs on the hourly convergence pass, which also runs on the start path before the collectors.
/// Day partitions made by <c>PARTITION OF</c> or <c>ATTACH</c> clone the parent index's options, and the partitioned
/// parent index cannot be altered ("This operation is not supported for partitioned indexes", PostgreSQL 18; a parent
/// index is a catalog template with no storage, so its setting changes nothing by itself), so a store whose parent was
/// made with <c>on</c> by V171 keeps cloning <c>on</c>. Promotion and create-ahead therefore turn it off inside the
/// transaction that makes the leaf, while the leaf is new and nothing else holds it.</para>
///
/// <para><b>The lock the ALTER takes.</b> <c>ALTER INDEX ... SET (autosummarize = off)</c> takes ACCESS EXCLUSIVE on the INDEX,
/// not the table (measured with pg_locks on PostgreSQL 18.6). A running VACUUM holds ROW EXCLUSIVE on each of its table's
/// indexes for its whole run, so the ALTER waits for it; after <c>deadlock_timeout</c> PostgreSQL cancels that autovacuum
/// and the ALTER goes through (measured: 1,008 ms, one cancel). Writers to that leaf queue behind the pending request for
/// that second. That is deliberately accepted: it happens once per index, ends the cancel loop for good, and the VACUUM it
/// cancels was being cancelled every couple of minutes anyway. The transaction runs under the same 5 s
/// <c>lock_timeout</c> as every other step here; a timeout (a manual VACUUM, which is never cancelled) is
/// <see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/> and the next pass tries again.</para>
///
/// <para><b>Summarizing, explicitly.</b> Without autosummarize a range is summarized only when VACUUM reaches the end of
/// its table, and an unsummarized range is always read, so every <c>collection_time</c> read through the BRIN scans the
/// unsummarized tail of every leaf it touches. Measured on a throwaway rig (a 111 K-block table, a 6 h window of 24 h):
/// 27,904 blocks with no tail, 38,895 with a 10% tail (+39%), 50,006 with a 20% tail (+79%), 27,904 again once
/// summarized; and a leaf that stops growing keeps its tail until another VACUUM comes. Insert-driven autovacuum fires
/// after 20% growth, so the tail is real. <see cref="SummarizeNewRangesAsync"/> closes it hourly with
/// <c>brin_summarize_new_values</c>, which takes the same <c>SHARE UPDATE EXCLUSIVE</c> a VACUUM holds, so it must
/// never be the waiter that cancels one: it reads <c>deadlock_timeout</c> at run time, runs each index in its own
/// transaction under a <c>lock_timeout</c> of half of it (capped at <see cref="MaximumSummarizeLockTimeoutMs"/>), skips an
/// index whose table <c>pg_stat_progress_vacuum</c> shows being vacuumed without asking for a lock, and does nothing when
/// <c>deadlock_timeout</c> is under <see cref="MinimumDeadlockTimeoutMs"/>. It also stops each index at
/// <see cref="SummarizeStatementTimeoutSeconds"/>: the ranges it has finished stay summarized (index tuples are not
/// rolled back), so a very large tail is worked off over several passes. A call with nothing to summarize costs 0.2 to
/// 0.5 ms.</para>
///
/// <para><b>Only leaves.</b> A partitioned index (relkind <c>I</c>) has no storage: it can be neither altered nor
/// summarized. Every statement here names a leaf index found in the catalog (<c>format('%I.%I')</c>, never a built-up
/// string), through the table's partition tree, so a future leaf is covered the day it exists.</para>
/// </summary>
public static class QueryStoreIntervalBrin
{
    /// <summary>The smallest <c>deadlock_timeout</c> (milliseconds) that leaves room for a lock wait strictly below it.</summary>
    public const int MinimumDeadlockTimeoutMs = 200;

    /// <summary>The longest lock wait of a summarize, in milliseconds, whatever <c>deadlock_timeout</c> is.</summary>
    public const int MaximumSummarizeLockTimeoutMs = 2000;

    /// <summary>The server-side deadline of one index's summarize, in seconds.</summary>
    public const int SummarizeStatementTimeoutSeconds = 60;

    /// <summary>Deadline for the catalog reads, in seconds.</summary>
    public const int CatalogReadTimeoutSeconds = 30;

    /* The BRIN leaf indexes of $1 and its partition tree. pg_partition_tree is strict, so a missing table reads nothing; a plain
       table is found by the first predicate. Relkind i only: the partitioned parent index (I) has no storage and no ALTER. */
    private const string LeafBrinIndexesFrom = @"
FROM pg_class AS i
JOIN pg_index AS x ON x.indexrelid = i.oid
JOIN pg_namespace AS n ON n.oid = i.relnamespace
JOIN pg_am AS am ON am.oid = i.relam
WHERE am.amname = 'brin'
AND   i.relkind = 'i'
AND   (x.indrelid = to_regclass($1) OR x.indrelid IN (SELECT pt.relid FROM pg_partition_tree(to_regclass($1)) AS pt))";

    /* Every BRIN leaf index of $1 whose autosummarize is on. The option text is whatever the DDL said (on, true, yes, 1). */
    internal const string AutosummarizeOnSql = @"
SELECT format('%I.%I', n.nspname, i.relname)" + LeafBrinIndexesFrom + @"
AND   EXISTS
(
    SELECT 1
    FROM pg_options_to_table(i.reloptions) AS o
    WHERE o.option_name = 'autosummarize'
    AND   lower(o.option_value) IN ('on', 'true', 'yes', 't', 'y', '1')
)
ORDER BY 1;";

    /* Every valid BRIN leaf index of $1, and whether its table is being vacuumed right now. pg_stat_progress_vacuum is
       per database, and a role that may not see another role's worker reads NULL there, which only means the lock timeout
       decides instead of this check. */
    internal const string SummarizeTargetsSql = @"
SELECT format('%I.%I', n.nspname, i.relname),
       EXISTS
       (
           SELECT 1
           FROM pg_stat_progress_vacuum AS v
           WHERE v.datid = (SELECT d.oid FROM pg_database AS d WHERE d.datname = current_database())
           AND   v.relid = x.indrelid
       )" + LeafBrinIndexesFrom + @"
AND   x.indisvalid
ORDER BY 1;";

    internal const string DeadlockTimeoutSql = "SELECT setting::int FROM pg_settings WHERE name = 'deadlock_timeout';";

    /// <summary>
    /// The lock wait of a summarize in milliseconds for a server whose <c>deadlock_timeout</c> is
    /// <paramref name="deadlockTimeoutMs"/>: half of it, at most <see cref="MaximumSummarizeLockTimeoutMs"/>, so always
    /// strictly below it; 0 when <paramref name="deadlockTimeoutMs"/> is under <see cref="MinimumDeadlockTimeoutMs"/>, which
    /// means the step does not run.
    /// </summary>
    public static int SummarizeLockTimeoutMs(int deadlockTimeoutMs) =>
        deadlockTimeoutMs < MinimumDeadlockTimeoutMs ? 0 : Math.Min(deadlockTimeoutMs / 2, MaximumSummarizeLockTimeoutMs);

    private static string AlterOffSql(string index) => $"ALTER INDEX {index} SET (autosummarize = off);";

    private static async Task<List<string>> ReadAutosummarizeOnAsync(
        NpgsqlConnection connection, string relation, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var found = new List<string>();
        await using var command = new NpgsqlCommand(AutosummarizeOnSql, connection, transaction) { CommandTimeout = CatalogReadTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = relation });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            found.Add(reader.GetString(0));
        }

        return found;
    }

    /// <summary>
    /// Turns <c>autosummarize</c> off on every BRIN leaf index of <paramref name="relation"/> and its partitions that still has
    /// it on, inside <paramref name="transaction"/> (which carries its own <c>lock_timeout</c>). For a partition the same
    /// transaction has just created or locked, so the ALTER waits for nothing. Returns how many indexes it altered.
    /// </summary>
    internal static async Task<int> TurnOffInTransactionAsync(
        NpgsqlConnection connection, NpgsqlTransaction transaction, string relation, CancellationToken cancellationToken)
    {
        var on = await ReadAutosummarizeOnAsync(connection, relation, transaction, cancellationToken).ConfigureAwait(false);
        foreach (var index in on)
        {
            await using var alter = new NpgsqlCommand(AlterOffSql(index), connection, transaction) { CommandTimeout = QueryStoreIntervalPartitions.ShortTimeoutSeconds };
            await alter.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return on.Count;
    }

    /// <summary>
    /// The convergence step: every BRIN leaf index of <paramref name="table"/> that still has <c>autosummarize</c> on (an
    /// index built by #4862, V171's legacy and parent copies, a day leaf that cloned the parent's option) is altered off,
    /// each in its own transaction under the 5 s step <c>lock_timeout</c>. A lock timeout is
    /// <see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/>; the indexes already done stay done. Count is the
    /// number of indexes altered.
    /// </summary>
    public static async Task<QueryStoreIntervalPartitions.StepResult> TurnOffAutosummarizeAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, ILogger logger, CancellationToken cancellationToken)
    {
        var on = await ReadAutosummarizeOnAsync(connection, table.Parent, null, cancellationToken).ConfigureAwait(false);
        if (on.Count == 0)
        {
            return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "no BRIN index has autosummarize on");
        }

        var altered = 0;
        foreach (var index in on)
        {
            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await using (var timeout = new NpgsqlCommand(
                    $"SET LOCAL lock_timeout = '{QueryStoreIntervalPartitions.LockTimeoutSeconds}s';", connection, transaction)
                { CommandTimeout = QueryStoreIntervalPartitions.ShortTimeoutSeconds })
                {
                    await timeout.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await using (var alter = new NpgsqlCommand(AlterOffSql(index), connection, transaction) { CommandTimeout = QueryStoreIntervalPartitions.ShortTimeoutSeconds })
                {
                    await alter.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                altered++;
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                logger.LogWarning(
                    "Query Store interval table {Table}: turning autosummarize off on {Index} hit a lock timeout and rolled back; "
                    + "the next pass retries.",
                    table.Parent, index);
                return new QueryStoreIntervalPartitions.StepResult(
                    QueryStoreIntervalPartitions.StepOutcome.RetryLater, $"lock timeout on {index}", altered);
            }
        }

        logger.LogInformation(
            "Query Store interval table {Table}: turned autosummarize off on {Count} BRIN index(es), so a BRIN summarize work item "
            + "no longer waits for the table's lock and cancels its autovacuum (#5594).",
            table.Parent, altered);
        return new QueryStoreIntervalPartitions.StepResult(
            QueryStoreIntervalPartitions.StepOutcome.Done, $"autosummarize off on {altered} BRIN index(es)", altered);
    }

    /// <summary>
    /// The hourly summarize (see the class remarks): <c>brin_summarize_new_values</c> on each valid BRIN leaf index of
    /// <paramref name="table"/>, each in its own transaction, under a <c>lock_timeout</c> strictly below
    /// <c>deadlock_timeout</c>. An index whose table is being vacuumed is skipped without a lock request. Count is the
    /// number of ranges summarized. A lock timeout is <see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/>; a
    /// <c>deadlock_timeout</c> too short to leave room is <see cref="QueryStoreIntervalPartitions.StepOutcome.NotReady"/>.
    /// Never throws for a lock or a statement deadline; shutdown propagates.
    /// </summary>
    public static async Task<QueryStoreIntervalPartitions.StepResult> SummarizeNewRangesAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, ILogger logger, CancellationToken cancellationToken)
    {
        int deadlockMs;
        await using (var read = new NpgsqlCommand(DeadlockTimeoutSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds })
        {
            deadlockMs = Convert.ToInt32(await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
        }

        var lockMs = SummarizeLockTimeoutMs(deadlockMs);
        if (lockMs == 0)
        {
            return new QueryStoreIntervalPartitions.StepResult(
                QueryStoreIntervalPartitions.StepOutcome.NotReady,
                $"deadlock_timeout is {deadlockMs} ms, too short to wait for a lock below it, so no BRIN summarize runs");
        }

        var targets = new List<(string Index, bool BeingVacuumed)>();
        await using (var list = new NpgsqlCommand(SummarizeTargetsSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds })
        {
            list.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = table.Parent });
            await using var reader = await list.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                targets.Add((reader.GetString(0), reader.GetBoolean(1)));
            }
        }

        if (targets.Count == 0)
        {
            return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.NothingToDo, "no valid BRIN index");
        }

        var summarized = 0;
        var vacuumed = 0;
        var lockSkipped = 0;
        var stopped = 0;
        foreach (var (index, beingVacuumed) in targets)
        {
            if (beingVacuumed)
            {
                vacuumed++;
                continue;
            }

            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await using (var timeouts = new NpgsqlCommand(
                    $"SET LOCAL lock_timeout = '{lockMs}ms'; SET LOCAL statement_timeout = '{SummarizeStatementTimeoutSeconds}s';",
                    connection,
                    transaction)
                { CommandTimeout = CatalogReadTimeoutSeconds })
                {
                    await timeouts.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                await using (var summarize = new NpgsqlCommand(
                    "SELECT brin_summarize_new_values(to_regclass($1));", connection, transaction)
                { CommandTimeout = SummarizeStatementTimeoutSeconds + CatalogReadTimeoutSeconds })
                {
                    summarize.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = index });
                    summarized += Convert.ToInt32(
                        await summarize.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                lockSkipped++;
                logger.LogDebug(
                    "Query Store interval table {Table}: BRIN summarize of {Index} gave up after {LockMs} ms without the lock "
                    + "(deadlock_timeout is {DeadlockMs} ms); the next pass retries.",
                    table.Parent, index, lockMs, deadlockMs);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled && !cancellationToken.IsCancellationRequested)
            {
                stopped++;
                logger.LogInformation(
                    "Query Store interval table {Table}: BRIN summarize of {Index} stopped at its {Seconds} s deadline; the ranges "
                    + "it finished stay summarized and the next pass continues.",
                    table.Parent, index, SummarizeStatementTimeoutSeconds);
            }
        }

        var detail = $"summarized {summarized} range(s) on {targets.Count - vacuumed - lockSkipped - stopped} index(es); "
            + $"skipped {vacuumed} being vacuumed, {lockSkipped} without the lock, {stopped} stopped at the deadline";
        if (summarized > 0)
        {
            logger.LogInformation("Query Store interval table {Table}: BRIN summarize: {Detail}.", table.Parent, detail);
            return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.Done, detail, summarized);
        }

        return new QueryStoreIntervalPartitions.StepResult(
            lockSkipped > 0 ? QueryStoreIntervalPartitions.StepOutcome.RetryLater : QueryStoreIntervalPartitions.StepOutcome.NothingToDo,
            detail);
    }
}
