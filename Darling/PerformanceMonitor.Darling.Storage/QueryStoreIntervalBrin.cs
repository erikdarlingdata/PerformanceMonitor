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
/// <para><b>Off, everywhere.</b> New builds say <c>autosummarize = off</c> (<see cref="QueryStoreIntervalWideBrinIndex"/>, and
/// V171's parent index, so a day partition made by <c>PARTITION OF</c> or <c>ATTACH</c> clones <c>off</c>).
/// <see cref="TurnOffAutosummarizeAsync"/> turns it off on every existing BRIN leaf of a table (the legacy table's, each
/// day leaf's, DEFAULT's): it reads the catalog and alters only an index that is still on, so a converged store pays one
/// catalog read. It runs on the hourly convergence pass, which also runs on the start path before the collectors. A store
/// that ran V171 before the parent said <c>off</c> keeps an <c>on</c> parent, and the partitioned parent index cannot be
/// altered ("This operation is not supported for partitioned indexes", PostgreSQL 18; a parent index is a catalog template
/// with no storage, so its setting changes nothing by itself), so such a store keeps cloning <c>on</c>. Promotion and
/// create-ahead therefore turn it off inside the transaction that makes the leaf, while the leaf is new and nothing else
/// holds it.</para>
///
/// <para><b>The lock the ALTER takes.</b> <c>ALTER INDEX ... SET (autosummarize = off)</c> takes ACCESS EXCLUSIVE on the INDEX,
/// not the table (measured with pg_locks on PostgreSQL 18.6). A running VACUUM holds ROW EXCLUSIVE on each of its table's
/// indexes for its whole run, so the ALTER waits for it, and a queued ACCESS EXCLUSIVE request blocks everything that asks
/// for the index after it: the collectors' inserts (ROW EXCLUSIVE) and also every read that does not prune the leaf (the
/// planner takes ACCESS SHARE on each index of a leaf it plans against). So the wait is felt by readers as well as writers.
/// What the holder is decides whether waiting does any good:</para>
/// <list type="bullet">
/// <item>A regular autovacuum is cancelled by the deadlock check after <c>deadlock_timeout</c>, and the ALTER goes through
/// (measured: 1,008 ms, one cancel). That is deliberately accepted: it happens once per index, ends the cancel loop for
/// good, and the VACUUM it cancels was being cancelled every couple of minutes anyway.</item>
/// <item>An anti-wraparound autovacuum and a manual VACUUM (any role) are never cancelled: waiting for them only queues
/// the ALTER, and the leaf's writers and readers with it. A store that never finished vacuuming its big legacy table is the
/// likely one to be in anti-wraparound. So an index whose table <c>pg_stat_progress_vacuum</c> shows being vacuumed by one
/// of those is not asked for at all: the step leaves it on, says so, and the next pass tries again
/// (<see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/>). A vacuum is anti-wraparound when its table's
/// <c>age(relfrozenxid)</c> or <c>mxid_age(relminmxid)</c> is past <c>autovacuum_freeze_max_age</c> or
/// <c>autovacuum_multixact_freeze_max_age</c> (the table's own setting when it has one); a vacuum whose backend is not an
/// autovacuum worker is a manual one. A role that may not see another role's worker reads NULL from
/// <c>pg_stat_progress_vacuum</c> and <c>pg_stat_activity</c> (measured), and then only the next point applies.</item>
/// <item>What no check can see waits at most <c>min(5 s, deadlock_timeout + 1 s)</c> (<see cref="TurnOffLockTimeoutMs"/>,
/// read at run time in the same session): a regular autovacuum still gets its <c>deadlock_timeout</c> to be cancelled plus a
/// second to let go, and a futile wait is about 2 s on a default server, not 5.</item>
/// </list>
/// <para>The turn-off and the summarize run on different tasks (the convergence pass and the background task, 20 minutes
/// apart after a start and each hourly), each on its own connection, so nothing keeps them from overlapping. They do not
/// get in each other's way: <c>brin_summarize_range</c> takes its <c>SHARE UPDATE EXCLUSIVE</c> locks inside the call and
/// releases them when the call ends (measured with pg_locks: none held after it), so the summarize holds the index for one
/// range at a time, not for its run. Measured on PostgreSQL 18.6, an ALTER sent 1.5 s into a 10.7 s summarize of a
/// 156 K-block table took 150 ms and the summarize finished every range. A lock timeout or a skipped index moves on to the
/// next index of the table; the step reports <c>RetryLater</c> after the loop.</para>
///
/// <para><b>Summarizing, explicitly.</b> Without autosummarize a range is summarized only when VACUUM reaches the end of
/// its table, and an unsummarized range is always read, so every <c>collection_time</c> read through the BRIN scans the
/// unsummarized tail of every leaf it touches. Measured on a throwaway rig (a 111 K-block table, a 6 h window of 24 h):
/// 27,904 blocks with no tail, 38,895 with a 10% tail (+39%), 50,006 with a 20% tail (+79%), 27,904 again once
/// summarized; and a leaf that stops growing keeps its tail until another VACUUM comes. Insert-driven autovacuum fires
/// after 20% growth, so the tail is real. <see cref="SummarizeNewRangesAsync"/> closes it hourly. Summarizing takes the same
/// <c>SHARE UPDATE EXCLUSIVE</c> a VACUUM holds, so it must never be the waiter that cancels one: it reads
/// <c>deadlock_timeout</c> at run time, runs each index in its own transaction under a <c>lock_timeout</c> of half of it
/// (capped at <see cref="MaximumSummarizeLockTimeoutMs"/>), skips an index whose table <c>pg_stat_progress_vacuum</c> shows
/// being vacuumed without asking for a lock, and does nothing when <c>deadlock_timeout</c> is under
/// <see cref="MinimumDeadlockTimeoutMs"/>.</para>
///
/// <para><b>Stopping between ranges.</b> <c>brin_summarize_new_values</c> cannot be stopped cleanly. A range is summarized by
/// first inserting a placeholder tuple for it and then reading its 128 blocks, so a <c>statement_timeout</c> cancel lands
/// inside a range nearly every time, and a range that has any tuple (a placeholder included) is skipped by every later
/// summarize. Measured on PostgreSQL 18.6: a 150 ms <c>statement_timeout</c> on a 47 K-block table left a placeholder; a second
/// <c>brin_summarize_new_values</c> and a <c>brin_summarize_range</c> on that very block both returned without touching it;
/// only <c>brin_desummarize_range</c> and then a summarize healed it. A placeholder is always returned by a scan, so that
/// range (128 blocks) would be read by every query for good, one per deadline hit. So the summarize is one statement per
/// index that calls <c>brin_summarize_range</c> for each range of the heap in block order, each call guarded by a clock check
/// against <see cref="SummarizeBudgetSeconds"/>: when the budget is spent the remaining ranges are not started, and the
/// next pass continues at the first range still missing. Measured: a 200 ms budget summarized 24 ranges, left no
/// placeholder, and the next pass finished the other 342. A <c>statement_timeout</c> of <see cref="SummarizeBackstopSeconds"/>
/// past the budget stays as a backstop for a read that stalls; only that one can leave a placeholder. The cost of testing
/// every range each pass is about 3 microseconds a range (measured on summarized ranges), about 0.3 s for a 12.3 M-block table. The
/// budget is reached only by a tail nothing has ever summarized: at the 14,600 to 36,000 blocks a second measured on the rig
/// (a 156 K-block table took 10.7 s, a 47 K-block one 1.2 s), the 12.3 M-block legacy table of the store in #5594 is 6 to 14
/// minutes of reading, more from a slower disk, so several hourly passes; one hour's growth of a day leaf (one 24th of a
/// leaf, for example 17 K blocks of 400 K) is a second or two. A call with nothing to summarize costs a few
/// milliseconds.</para>
///
/// <para><b>Only leaves.</b> A partitioned index (relkind <c>I</c>) has no storage: it can be neither altered nor
/// summarized. Every statement here names a leaf index found in the catalog (<c>format('%I.%I')</c>, never a built-up
/// string), through the table's partition tree, so a future leaf is covered the day it exists. A leaf dropped between the
/// catalog read and its statement (the sweep drops expired days) is skipped.</para>
/// </summary>
public static class QueryStoreIntervalBrin
{
    /// <summary>The smallest <c>deadlock_timeout</c> (milliseconds) that leaves room for a lock wait strictly below it.</summary>
    public const int MinimumDeadlockTimeoutMs = 200;

    /// <summary>The longest lock wait of a summarize, in milliseconds, whatever <c>deadlock_timeout</c> is.</summary>
    public const int MaximumSummarizeLockTimeoutMs = 2000;

    /// <summary>The time one index's summarize may spend starting new ranges, in seconds; it stops between ranges.</summary>
    public const int SummarizeBudgetSeconds = 60;

    /// <summary>How long after the budget the server cancels a summarize statement that is stuck inside a range, in seconds.</summary>
    public const int SummarizeBackstopSeconds = 30;

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

    /* Every BRIN leaf index of $1 whose autosummarize is on, and whether its table is being vacuumed by a vacuum the deadlock
       check never cancels (#5594 M1): a manual VACUUM (a backend that is not an autovacuum worker), or an autovacuum that is
       anti-wraparound (the table's age past the freeze limit, the table's own reloption first). pg_stat_progress_vacuum and
       pg_stat_activity read NULL for another role's worker the caller may not see, which makes this false for it, and the
       lock_timeout decides instead. The option text is whatever the DDL said (on, true, yes, 1). */
    internal const string AutosummarizeOnSql = @"
SELECT format('%I.%I', n.nspname, i.relname),
       EXISTS
       (
           SELECT 1
           FROM pg_stat_progress_vacuum AS v
           JOIN pg_class AS t ON t.oid = v.relid
           WHERE v.datid = (SELECT d.oid FROM pg_database AS d WHERE d.datname = current_database())
           AND   v.relid = x.indrelid
           AND
           (
               age(t.relfrozenxid) >
                   COALESCE
                   (
                       (SELECT o.option_value::int FROM pg_options_to_table(t.reloptions) AS o WHERE o.option_name = 'autovacuum_freeze_max_age'),
                       current_setting('autovacuum_freeze_max_age')::int
                   )
               OR mxid_age(t.relminmxid) >
                   COALESCE
                   (
                       (SELECT o.option_value::int FROM pg_options_to_table(t.reloptions) AS o WHERE o.option_name = 'autovacuum_multixact_freeze_max_age'),
                       current_setting('autovacuum_multixact_freeze_max_age')::int
                   )
               OR EXISTS (SELECT 1 FROM pg_stat_activity AS a WHERE a.pid = v.pid AND a.backend_type <> 'autovacuum worker')
           )
       )" + LeafBrinIndexesFrom + @"
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

    /* One index's summarize ($1 the index, $2 the budget in seconds): brin_summarize_range for each range of the heap in block
       order, each call behind a clock check, so the statement stops BETWEEN ranges (brin_summarize_new_values can only be
       cancelled, which strands a placeholder; see the class remarks). Column 0 is the ranges summarized, column 1 the ranges
       not started because the budget was spent. The index is resolved once in the inner query: a dropped index reads no rows
       (0, 0) instead of a NULL scalar. The last, partial range is left alone by both functions. pages_per_range is the
       index's own setting, 128 when unset. */
    internal const string SummarizeIndexSql = @"
SELECT COALESCE(sum(s.r), 0)::bigint,
       count(*) FILTER (WHERE s.r IS NULL)::bigint
FROM
(
    SELECT CASE WHEN clock_timestamp() < statement_timestamp() + make_interval(secs => $2)
                THEN brin_summarize_range(ix.indexrelid, g.b)
           END AS r
    FROM
    (
        SELECT x.indexrelid,
               x.indrelid,
               COALESCE((SELECT o.option_value::int FROM pg_options_to_table(i.reloptions) AS o WHERE o.option_name = 'pages_per_range'), 128) AS pages_per_range
        FROM pg_index AS x
        JOIN pg_class AS i ON i.oid = x.indexrelid
        WHERE x.indexrelid = to_regclass($1)
    ) AS ix
    CROSS JOIN LATERAL generate_series(0::bigint, pg_relation_size(ix.indrelid) / current_setting('block_size')::int - 1, ix.pages_per_range) AS g(b)
) AS s;";

    internal const string DeadlockTimeoutSql = "SELECT setting::int FROM pg_settings WHERE name = 'deadlock_timeout';";

    /// <summary>
    /// The lock wait of a summarize in milliseconds for a server whose <c>deadlock_timeout</c> is
    /// <paramref name="deadlockTimeoutMs"/>: half of it, at most <see cref="MaximumSummarizeLockTimeoutMs"/>, so always
    /// strictly below it; 0 when <paramref name="deadlockTimeoutMs"/> is under <see cref="MinimumDeadlockTimeoutMs"/>, which
    /// means the step does not run.
    /// </summary>
    public static int SummarizeLockTimeoutMs(int deadlockTimeoutMs) =>
        deadlockTimeoutMs < MinimumDeadlockTimeoutMs ? 0 : Math.Min(deadlockTimeoutMs / 2, MaximumSummarizeLockTimeoutMs);

    /// <summary>
    /// The lock wait of the turn-off ALTER in milliseconds for a server whose <c>deadlock_timeout</c> is
    /// <paramref name="deadlockTimeoutMs"/>: <c>deadlock_timeout</c> plus one second, at most the step's usual
    /// <see cref="QueryStoreIntervalPartitions.LockTimeoutSeconds"/>. A regular autovacuum is cancelled by the deadlock check
    /// at <c>deadlock_timeout</c>, so this gives it that and a second to let go; a wait that long on a holder that is not
    /// cancelled is the most a request that queues readers and writers should cost.
    /// </summary>
    public static int TurnOffLockTimeoutMs(int deadlockTimeoutMs) =>
        (int)Math.Min(QueryStoreIntervalPartitions.LockTimeoutSeconds * 1000L, Math.Max(deadlockTimeoutMs, 1) + 1000L);

    private static string AlterOffSql(string index) => $"ALTER INDEX {index} SET (autosummarize = off);";

    private static async Task<int> ReadDeadlockTimeoutMsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var read = new NpgsqlCommand(DeadlockTimeoutSql, connection) { CommandTimeout = CatalogReadTimeoutSeconds };
        return Convert.ToInt32(await read.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<List<(string Index, bool Held)>> ReadAutosummarizeOnAsync(
        NpgsqlConnection connection, string relation, NpgsqlTransaction? transaction, CancellationToken cancellationToken)
    {
        var found = new List<(string Index, bool Held)>();
        await using var command = new NpgsqlCommand(AutosummarizeOnSql, connection, transaction) { CommandTimeout = CatalogReadTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = relation });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            found.Add((reader.GetString(0), reader.GetBoolean(1)));
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
        foreach (var (index, _) in on)
        {
            await using var alter = new NpgsqlCommand(AlterOffSql(index), connection, transaction) { CommandTimeout = QueryStoreIntervalPartitions.ShortTimeoutSeconds };
            await alter.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        return on.Count;
    }

    /// <summary>
    /// The convergence step: every BRIN leaf index of <paramref name="table"/> that still has <c>autosummarize</c> on (an
    /// index built by #4862, V171's legacy and parent copies, a day leaf that cloned the parent's option) is altered off,
    /// each in its own transaction under <see cref="TurnOffLockTimeoutMs"/>. An index a holder that cannot be cancelled has
    /// (see the class remarks) is skipped without a lock request; a lock timeout rolls that index back. Neither stops the
    /// other indexes of the table; either one makes the outcome
    /// <see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/>, and the indexes already done stay done. Count is the
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
        var held = 0;
        var timedOut = 0;
        var lockTimeoutMs = 0;
        foreach (var (index, uncancellableHolder) in on)
        {
            if (uncancellableHolder)
            {
                held++;
                logger.LogDebug(
                    "Query Store interval table {Table}: autosummarize stays on for {Index} this pass: its table is being vacuumed by a vacuum "
                    + "that is never cancelled (a manual VACUUM or an anti-wraparound autovacuum), so asking for the lock would only queue readers and writers.",
                    table.Parent, index);
                continue;
            }

            if (lockTimeoutMs == 0)
            {
                lockTimeoutMs = TurnOffLockTimeoutMs(await ReadDeadlockTimeoutMsAsync(connection, cancellationToken).ConfigureAwait(false));
            }

            try
            {
                await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
                await using (var timeout = new NpgsqlCommand(
                    $"SET LOCAL lock_timeout = '{lockTimeoutMs}ms';", connection, transaction)
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
                timedOut++;
                logger.LogDebug(
                    "Query Store interval table {Table}: turning autosummarize off on {Index} hit its {LockMs} ms lock timeout and rolled back.",
                    table.Parent, index, lockTimeoutMs);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                /* The leaf was dropped (the sweep drops expired days) between the catalog read and the ALTER: nothing to turn off. */
                logger.LogDebug("Query Store interval table {Table}: {Index} was dropped before autosummarize could be turned off on it.", table.Parent, index);
            }
        }

        if (altered > 0)
        {
            logger.LogInformation(
                "Query Store interval table {Table}: turned autosummarize off on {Count} BRIN index(es), so a BRIN summarize work item "
                + "no longer waits for the table's lock and cancels its autovacuum (#5594).",
                table.Parent, altered);
        }

        if (held + timedOut > 0)
        {
            var detail = $"autosummarize off on {altered} BRIN index(es); {held} left on while a vacuum that cannot be cancelled runs on their table, {timedOut} lock timeout(s)";
            if (timedOut > 0)
            {
                logger.LogWarning(
                    "Query Store interval table {Table}: turning autosummarize off hit a lock timeout on {TimedOut} BRIN index(es) and rolled them back, "
                    + "and left {Held} on while a vacuum that cannot be cancelled runs on their table; the next pass retries.",
                    table.Parent, timedOut, held);
            }
            else
            {
                logger.LogInformation(
                    "Query Store interval table {Table}: autosummarize stays on for {Held} BRIN index(es) while a vacuum that cannot be "
                    + "cancelled runs on their table; the next pass retries.",
                    table.Parent, held);
            }

            return new QueryStoreIntervalPartitions.StepResult(QueryStoreIntervalPartitions.StepOutcome.RetryLater, detail, altered);
        }

        return new QueryStoreIntervalPartitions.StepResult(
            QueryStoreIntervalPartitions.StepOutcome.Done, $"autosummarize off on {altered} BRIN index(es)", altered);
    }

    /// <summary>
    /// The hourly summarize (see the class remarks): every range not yet summarized on each valid BRIN leaf index of
    /// <paramref name="table"/>, each index in its own transaction, under a <c>lock_timeout</c> strictly below
    /// <c>deadlock_timeout</c> and a <see cref="SummarizeBudgetSeconds"/> budget that stops it between ranges. An index whose
    /// table is being vacuumed is skipped without a lock request. Count is the number of ranges summarized. A lock timeout
    /// is <see cref="QueryStoreIntervalPartitions.StepOutcome.RetryLater"/>; a <c>deadlock_timeout</c> too short to leave
    /// room is <see cref="QueryStoreIntervalPartitions.StepOutcome.NotReady"/>. Never throws for a lock, a budget or a
    /// dropped leaf; shutdown propagates.
    /// </summary>
    public static async Task<QueryStoreIntervalPartitions.StepResult> SummarizeNewRangesAsync(
        NpgsqlConnection connection, QueryStoreIntervalPartitions.IntervalTable table, ILogger logger, CancellationToken cancellationToken)
    {
        var deadlockMs = await ReadDeadlockTimeoutMsAsync(connection, cancellationToken).ConfigureAwait(false);
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

        long summarized = 0;
        var vacuumed = 0;
        var lockSkipped = 0;
        var stopped = 0;
        var dropped = 0;
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
                    $"SET LOCAL lock_timeout = '{lockMs}ms'; SET LOCAL statement_timeout = '{SummarizeBudgetSeconds + SummarizeBackstopSeconds}s';",
                    connection,
                    transaction)
                { CommandTimeout = CatalogReadTimeoutSeconds })
                {
                    await timeouts.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }

                long done;
                long notStarted;
                await using (var summarize = new NpgsqlCommand(SummarizeIndexSql, connection, transaction)
                { CommandTimeout = SummarizeBudgetSeconds + SummarizeBackstopSeconds + CatalogReadTimeoutSeconds })
                {
                    summarize.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = index });
                    summarize.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double, Value = (double)SummarizeBudgetSeconds });
                    await using var result = await summarize.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                    await result.ReadAsync(cancellationToken).ConfigureAwait(false);
                    done = result.GetInt64(0);
                    notStarted = result.GetInt64(1);
                }

                await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
                summarized += done;
                if (notStarted > 0)
                {
                    stopped++;
                    logger.LogInformation(
                        "Query Store interval table {Table}: BRIN summarize of {Index} spent its {Seconds} s budget after {Done} range(s) and stopped between "
                        + "ranges; those stay summarized and the next pass continues at the first range still missing.",
                        table.Parent, index, SummarizeBudgetSeconds, done);
                }
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
            {
                lockSkipped++;
                logger.LogDebug(
                    "Query Store interval table {Table}: BRIN summarize of {Index} gave up after {LockMs} ms without the lock "
                    + "(deadlock_timeout is {DeadlockMs} ms); the next pass retries.",
                    table.Parent, index, lockMs, deadlockMs);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedTable)
            {
                /* The leaf was dropped (the sweep drops expired days) after the target list was read: nothing to summarize, and
                   the table's other indexes still run. */
                dropped++;
                logger.LogDebug("Query Store interval table {Table}: BRIN summarize of {Index} found it dropped.", table.Parent, index);
            }
            catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.QueryCanceled && !cancellationToken.IsCancellationRequested)
            {
                stopped++;
                logger.LogInformation(
                    "Query Store interval table {Table}: BRIN summarize of {Index} was cancelled by its {Seconds} s backstop, stuck inside one range; "
                    + "what it finished stays summarized, that range stays a placeholder every scan reads until the index is rebuilt, and the next pass continues.",
                    table.Parent, index, SummarizeBudgetSeconds + SummarizeBackstopSeconds);
            }
        }

        var detail = $"summarized {summarized} range(s) on {targets.Count - vacuumed - lockSkipped - dropped} index(es); "
            + $"skipped {vacuumed} being vacuumed, {lockSkipped} without the lock, {dropped} dropped, {stopped} stopped at the deadline";
        if (summarized > 0)
        {
            return new QueryStoreIntervalPartitions.StepResult(
                QueryStoreIntervalPartitions.StepOutcome.Done, detail, (int)Math.Min(summarized, int.MaxValue));
        }

        return new QueryStoreIntervalPartitions.StepResult(
            lockSkipped > 0 ? QueryStoreIntervalPartitions.StepOutcome.RetryLater : QueryStoreIntervalPartitions.StepOutcome.NothingToDo,
            detail);
    }
}
