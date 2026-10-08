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
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store's own statement history (#5097): an hourly snapshot of the per-statement DELTAS of the store's
/// <c>pg_stat_statements</c>, so a statement's mean milliseconds per call can be read hour by hour across a store
/// restart, a service restart, a <c>pg_stat_statements_reset()</c> and the nightly upgrade.
///
/// <para><b>The baseline lives in the store.</b> The per-target statement collector keeps its previous snapshot in
/// process memory and re-seeds it from stored rows. A top-N history cannot be re-seeded that way: a statement left
/// out of last hour's top N has no stored previous row. So the previous cumulative counters are kept in
/// <c>config.store_statement_baseline</c> and the subtraction happens in SQL. A service restart, including the
/// nightly install, needs nothing from process memory.</para>
///
/// <para><b>Epochs.</b> <c>pg_stat_statements_info.stats_reset</c> is the epoch. <see cref="DecideEpoch"/> reads it
/// against the previous capture: unchanged means the baseline is still valid; changed to a moment after the previous
/// capture means the reset happened inside the interval, so the current counters are credited whole (they were all
/// earned since the reset); changed in any other way, or no previous capture, means the baseline cannot be trusted and
/// the pass records a baseline and no history. Inside one epoch a counter that fell means the entry was evicted and
/// came back after the previous capture, so its current counters are credited whole and the row is flagged as a lower
/// bound. That second rule also covers a restart that leaves <c>stats_reset</c> unchanged.</para>
///
/// <para><b>Bounded.</b> At most <see cref="TopStatements"/> statements are kept per hour, ranked by time spent in the
/// interval. The capture row carries how many were active (<c>statements_seen</c>) and how many entries the extension
/// evicted (<c>dealloc_delta</c>), so a reader can tell when the cap or the eviction hid something. Rows older than
/// <see cref="RetentionDays"/> are deleted by the pass itself. Statements without a query id (the extension hides
/// other roles' ids from a caller that may not read them) are counted in <c>hidden_statements</c> and never keyed.</para>
///
/// <para><b>Reads through the reader function.</b> The snapshot calls the SECURITY DEFINER
/// <c>config.store_statement_stats()</c> and its info companion, which already restrict the rows to this database and
/// map users to role names, and never touches <c>pg_stat_statements</c> directly. If the functions are missing or the
/// caller may not execute them, the extension is not loaded, or the role may not create temporary tables, the pass
/// writes one capture with outcome <c>precondition</c> and no history.</para>
///
/// <para><b>Limits a reader should know.</b> An entry that was evicted and came back after the previous capture, and
/// has already passed its old baseline, shows <c>current - baseline</c>: a plausible undercount with no flag (only the
/// capture's <c>dealloc_delta</c> hints at eviction). <c>first_seen</c> can be an upper bound: an entry that was already
/// there but missing from the baseline (a query id that became visible after a <c>pg_read_all_stats</c> grant, or a
/// role that was renamed) is credited its whole lifetime counters as one interval. <c>statements_seen</c> is 0 on a
/// rebaseline, which reads no deltas.</para>
/// </summary>
public static class StoreStatementHistory
{
    /// <summary>The snapshot's own slice of the hourly tick's budget, so a slow pass cannot cost the flushes after it.</summary>
    public static readonly TimeSpan SliceBudget = TimeSpan.FromSeconds(120);

    /// <summary>Per-command timeout, in seconds. The pass shares the hourly tick's budget with other self-telemetry.</summary>
    public const int SnapshotTimeoutSeconds = 60;

    /// <summary>The most statements kept per snapshot, by time spent in the interval.</summary>
    public const int TopStatements = 100;

    /// <summary>How long history and capture rows are kept. The baseline is state and is never aged out.</summary>
    public const int RetentionDays = 90;

    /// <summary>Capture outcome: deltas were written.</summary>
    public const string OutcomeOk = "ok";

    /// <summary>Capture outcome: a baseline was recorded and no history written, because no delta could be trusted.</summary>
    public const string OutcomeRebaselined = "rebaselined";

    /// <summary>Capture outcome: the reader functions were not usable, so nothing was read.</summary>
    public const string OutcomePrecondition = "precondition";

    private const string Config = PgSchemaGenerator.ConfigSchema;

    private const string Function = Config + "." + StoreStatementStats.FunctionName;

    private const string InfoFunction = Config + "." + StoreStatementStats.InfoFunctionName;

    /// <summary>
    /// Whether the snapshot can run: both reader functions exist, the calling role may run them, the extension is
    /// loaded (<c>pg_stat_statements.max</c> is a setting only when it is in <c>shared_preload_libraries</c>; an installed
    /// but unloaded extension makes the reader raise 55000), and the role may create the transaction-local table.
    /// </summary>
    public const string ReaderReadySql = @"
SELECT CASE
           WHEN to_regprocedure('" + Function + @"()') IS NULL
             OR to_regprocedure('" + InfoFunction + @"()') IS NULL THEN false
           ELSE has_function_privilege('" + Function + @"()', 'EXECUTE')
            AND has_function_privilege('" + InfoFunction + @"()', 'EXECUTE')
            AND EXISTS (SELECT 1 FROM pg_settings WHERE name = 'pg_stat_statements.max')
            AND has_database_privilege(current_database(), 'TEMP')
       END";

    /// <summary>The epoch and the eviction counter. Nulls on extension 1.8, which has no info view.</summary>
    public const string InfoSql = @"
SELECT stats_reset, dealloc
FROM " + InfoFunction + @"()";

    /// <summary>The newest capture that actually read the extension.</summary>
    public const string PreviousCaptureSql = @"
SELECT capture_time, stats_reset, dealloc
FROM collect.store_statement_captures
WHERE outcome <> '" + OutcomePrecondition + @"'
ORDER BY capture_time DESC
LIMIT 1";

    /// <summary>
    /// Reads the reader function ONCE into a transaction-local table, so the delta and the next baseline come from
    /// the same instant. Folds <c>toplevel</c> true and false together: the function already does, and only
    /// <c>track = all</c> would split them. Rows without a query id stay in as one group per role so they can be
    /// counted; every later statement filters them out.
    /// </summary>
    public const string CaptureCurrentSql = @"
CREATE TEMP TABLE store_statement_cur ON COMMIT DROP AS
SELECT role_name,
       queryid,
       sum(calls)::bigint AS calls,
       sum(total_exec_ms)::double precision AS total_exec_ms,
       max(max_exec_ms)::double precision AS max_exec_ms,
       sum(rows_returned)::bigint AS rows_returned,
       sum(shared_blks_hit)::bigint AS shared_blks_hit,
       sum(shared_blks_read)::bigint AS shared_blks_read,
       sum(temp_blks_written)::bigint AS temp_blks_written,
       count(*)::integer AS entries
FROM " + Function + @"()
GROUP BY role_name, queryid";

    /// <summary>Hidden (query-id-less) entries in the capture, for the capture row.</summary>
    public const string HiddenCountSql = @"
SELECT COALESCE(sum(entries) FILTER (WHERE queryid IS NULL), 0)::integer,
       count(*) FILTER (WHERE queryid IS NOT NULL)::integer
FROM store_statement_cur";

    /// <summary>
    /// Writes the interval's deltas and answers (statements active, statements kept). $1 capture time, $2 top,
    /// $3 previous capture time, $4 whether the baseline is valid (same epoch), $5 whether a reset happened inside
    /// the interval, $6 the interval in seconds (computed once in C#, so the capture row agrees). With $4 false every row is credited whole. A counter below its baseline means the entry was
    /// evicted and re-entered, so it is credited whole too and flagged. Ranked by time spent, then query id, then role name, so a tie cuts the same way every time.
    /// </summary>
    public const string SnapshotSql = @"
WITH joined AS
(
    SELECT c.role_name,
           c.queryid,
           c.calls,
           c.total_exec_ms,
           c.max_exec_ms,
           c.rows_returned,
           c.shared_blks_hit,
           c.shared_blks_read,
           c.temp_blks_written,
           b.queryid IS NOT NULL AS has_base,
           COALESCE(c.calls < b.calls
                    OR c.total_exec_ms < b.total_exec_ms
                    OR c.rows_returned < b.rows_returned
                    OR c.shared_blks_hit < b.shared_blks_hit
                    OR c.shared_blks_read < b.shared_blks_read
                    OR c.temp_blks_written < b.temp_blks_written, false) AS fell,
           b.calls AS b_calls,
           b.total_exec_ms AS b_total_exec_ms,
           b.rows_returned AS b_rows_returned,
           b.shared_blks_hit AS b_shared_blks_hit,
           b.shared_blks_read AS b_shared_blks_read,
           b.temp_blks_written AS b_temp_blks_written
    FROM store_statement_cur AS c
    LEFT JOIN config.store_statement_baseline AS b
      ON b.role_name = c.role_name
     AND b.queryid = c.queryid
    WHERE c.queryid IS NOT NULL
),
deltas AS
(
    SELECT j.role_name,
           j.queryid,
           j.max_exec_ms,
           (j.has_base AND NOT j.fell AND $4::boolean) AS use_base,
           ($4::boolean AND NOT j.has_base) AS first_seen,
           ($4::boolean AND j.has_base AND j.fell) AS entry_restarted,
           j.calls - CASE WHEN j.has_base AND NOT j.fell AND $4::boolean THEN j.b_calls ELSE 0 END AS delta_calls,
           j.total_exec_ms - CASE WHEN j.has_base AND NOT j.fell AND $4::boolean THEN j.b_total_exec_ms ELSE 0 END AS delta_total_exec_ms,
           j.rows_returned - CASE WHEN j.has_base AND NOT j.fell AND $4::boolean THEN j.b_rows_returned ELSE 0 END AS delta_rows,
           j.shared_blks_hit - CASE WHEN j.has_base AND NOT j.fell AND $4::boolean THEN j.b_shared_blks_hit ELSE 0 END AS delta_shared_blks_hit,
           j.shared_blks_read - CASE WHEN j.has_base AND NOT j.fell AND $4::boolean THEN j.b_shared_blks_read ELSE 0 END AS delta_shared_blks_read,
           j.temp_blks_written - CASE WHEN j.has_base AND NOT j.fell AND $4::boolean THEN j.b_temp_blks_written ELSE 0 END AS delta_temp_blks_written
    FROM joined AS j
),
active AS
(
    SELECT *
    FROM deltas
    WHERE delta_calls > 0
),
kept AS
(
    INSERT INTO collect.store_statement_history
        (capture_time, interval_seconds, role_name, queryid, delta_calls, delta_total_exec_ms, delta_rows,
         delta_shared_blks_hit, delta_shared_blks_read, delta_temp_blks_written, max_exec_ms,
         first_seen, entry_restarted, reset_in_interval)
    SELECT $1::timestamp,
           $6::integer,
           a.role_name,
           a.queryid,
           a.delta_calls,
           a.delta_total_exec_ms,
           a.delta_rows,
           a.delta_shared_blks_hit,
           a.delta_shared_blks_read,
           a.delta_temp_blks_written,
           a.max_exec_ms,
           a.first_seen,
           a.entry_restarted,
           $5::boolean
    FROM active AS a
    ORDER BY a.delta_total_exec_ms DESC, a.queryid, a.role_name
    LIMIT $2::integer
    RETURNING 1
)
SELECT (SELECT count(*) FROM active)::integer,
       (SELECT count(*) FROM kept)::integer";

    /// <summary>Drops the old baseline. Run before <see cref="BaselineInsertSql"/>, in the same transaction.</summary>
    public const string BaselineDeleteSql = @"
DELETE FROM config.store_statement_baseline";

    /// <summary>Records every keyed statement's cumulative counters as the next baseline. $1 capture time.</summary>
    public const string BaselineInsertSql = @"
INSERT INTO config.store_statement_baseline
    (role_name, queryid, calls, total_exec_ms, rows_returned, shared_blks_hit, shared_blks_read, temp_blks_written, captured_at)
SELECT role_name, queryid, calls, total_exec_ms, rows_returned, shared_blks_hit, shared_blks_read, temp_blks_written, $1::timestamp
FROM store_statement_cur
WHERE queryid IS NOT NULL";

    /// <summary>
    /// The capture row. $1 capture time, $2 interval seconds, $3 stats_reset, $4 dealloc, $5 dealloc delta,
    /// $6 statements seen, $7 statements kept, $8 hidden statements, $9 outcome.
    /// </summary>
    public const string CaptureInsertSql = @"
INSERT INTO collect.store_statement_captures
    (capture_time, interval_seconds, stats_reset, dealloc, dealloc_delta, statements_seen, statements_kept, hidden_statements, outcome)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";

    /// <summary>History retention. $1 cutoff (naive UTC).</summary>
    public const string HistoryRetentionDeleteSql = @"
DELETE FROM collect.store_statement_history
WHERE capture_time < $1";

    /// <summary>Capture retention, on the same cutoff so no history row outlives the capture that qualifies it. $1 cutoff.</summary>
    public const string CaptureRetentionDeleteSql = @"
DELETE FROM collect.store_statement_captures
WHERE capture_time < $1";

    /// <summary>What the counters since the last capture are measured against.</summary>
    internal enum Epoch
    {
        /// <summary>The extension's counters have not been reset since the previous capture: subtract the baseline.</summary>
        SameEpoch,

        /// <summary>The counters were reset after the previous capture: everything now was earned inside the interval.</summary>
        ResetInsideInterval,

        /// <summary>The baseline cannot be trusted: record a new one and write no history.</summary>
        Rebaseline,
    }

    /// <summary>What one pass did.</summary>
    /// <param name="Outcome">The capture row's outcome.</param>
    /// <param name="StatementsSeen">Statements with calls in the interval, before the cap.</param>
    /// <param name="StatementsKept">History rows written.</param>
    /// <param name="HiddenStatements">Entries the extension showed without a query id.</param>
    public readonly record struct SnapshotResult(string Outcome, int StatementsSeen, int StatementsKept, int HiddenStatements);

    /// <summary>
    /// Pure: which epoch the counters are in. Both stamps are naive UTC. No previous capture means there is nothing
    /// to subtract. Two absent stamps (extension 1.8) read as the same epoch: the fallen-counter rule is then the only
    /// guard. A changed stamp that falls after the previous capture is a reset inside the interval; any other change
    /// (earlier than the previous capture, or unreadable) cannot be placed.
    /// </summary>
    internal static Epoch DecideEpoch(DateTime? previousCaptureTime, DateTime? previousStatsReset, DateTime? currentStatsReset)
    {
        if (previousCaptureTime is null)
        {
            return Epoch.Rebaseline;
        }

        if (previousStatsReset == currentStatsReset)
        {
            return Epoch.SameEpoch;
        }

        return currentStatsReset is { } now && now > previousCaptureTime.Value
            ? Epoch.ResetInsideInterval
            : Epoch.Rebaseline;
    }

    /// <summary>
    /// One snapshot, in one transaction: it either lands whole (history, baseline, capture and retention) or not at all.
    /// </summary>
    /// <param name="connection">An open connection to the store, as its owner.</param>
    /// <param name="captureTimeUtc">The capture's moment; stored naive UTC.</param>
    /// <param name="logger">Optional; debug only.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    public static async Task<SnapshotResult> SnapshotAsync(
        NpgsqlConnection connection,
        DateTime captureTimeUtc,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var captureTime = DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified);
        var cutoff = captureTime.AddDays(-RetentionDays);

        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        SnapshotResult result;
        var ready = await ScalarAsync(connection, transaction, ReaderReadySql, cancellationToken) is true;
        if (!ready)
        {
            await InsertCaptureAsync(connection, transaction, captureTime, null, null, null, null, 0, 0, 0, OutcomePrecondition, cancellationToken);
            result = new SnapshotResult(OutcomePrecondition, 0, 0, 0);
        }
        else
        {
            DateTime? currentReset = null;
            long? currentDealloc = null;
            await using (var info = Command(connection, transaction, InfoSql))
            await using (var reader = await info.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    currentReset = reader.IsDBNull(0) ? null : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Unspecified);
                    currentDealloc = reader.IsDBNull(1) ? null : reader.GetInt64(1);
                }
            }

            DateTime? previousTime = null;
            DateTime? previousReset = null;
            long? previousDealloc = null;
            await using (var previous = Command(connection, transaction, PreviousCaptureSql))
            await using (var reader = await previous.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    previousTime = DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Unspecified);
                    previousReset = reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Unspecified);
                    previousDealloc = reader.IsDBNull(2) ? null : reader.GetInt64(2);
                }
            }

            var epoch = DecideEpoch(previousTime, previousReset, currentReset);
            int? interval = previousTime is { } t ? Math.Max(1, (int)(captureTime - t).TotalSeconds) : null;

            await using (var current = Command(connection, transaction, CaptureCurrentSql))
            {
                await current.ExecuteNonQueryAsync(cancellationToken);
            }

            var hidden = 0;
            await using (var count = Command(connection, transaction, HiddenCountSql))
            await using (var reader = await count.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    hidden = reader.GetInt32(0);
                }
            }

            var seen = 0;
            var kept = 0;
            if (epoch != Epoch.Rebaseline)
            {
                await using var snapshot = Command(connection, transaction, SnapshotSql);
                snapshot.Parameters.Add(new NpgsqlParameter { Value = captureTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
                snapshot.Parameters.Add(new NpgsqlParameter { Value = TopStatements, NpgsqlDbType = NpgsqlDbType.Integer });
                snapshot.Parameters.Add(new NpgsqlParameter { Value = (object?)previousTime ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
                snapshot.Parameters.Add(new NpgsqlParameter { Value = epoch == Epoch.SameEpoch, NpgsqlDbType = NpgsqlDbType.Boolean });
                snapshot.Parameters.Add(new NpgsqlParameter { Value = epoch == Epoch.ResetInsideInterval, NpgsqlDbType = NpgsqlDbType.Boolean });
                snapshot.Parameters.Add(new NpgsqlParameter { Value = interval ?? 1, NpgsqlDbType = NpgsqlDbType.Integer });
                await using var reader = await snapshot.ExecuteReaderAsync(cancellationToken);
                if (await reader.ReadAsync(cancellationToken))
                {
                    seen = reader.GetInt32(0);
                    kept = reader.GetInt32(1);
                }
            }

            /* A rebaseline reads no deltas, so it reports 0 active statements (not every keyed entry): the column
               always means "statements with calls in the interval". */

            await using (var delete = Command(connection, transaction, BaselineDeleteSql))
            {
                await delete.ExecuteNonQueryAsync(cancellationToken);
            }

            await using (var insert = Command(connection, transaction, BaselineInsertSql))
            {
                insert.Parameters.Add(new NpgsqlParameter { Value = captureTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
                await insert.ExecuteNonQueryAsync(cancellationToken);
            }

            long? dealloc = epoch == Epoch.SameEpoch && currentDealloc is { } d && previousDealloc is { } p && d >= p ? d - p : null;
            var outcome = epoch == Epoch.Rebaseline ? OutcomeRebaselined : OutcomeOk;
            await InsertCaptureAsync(connection, transaction, captureTime, interval, currentReset, currentDealloc, dealloc, seen, kept, hidden, outcome, cancellationToken);
            result = new SnapshotResult(outcome, seen, kept, hidden);
        }

        foreach (var sql in new[] { HistoryRetentionDeleteSql, CaptureRetentionDeleteSql })
        {
            await using var purge = Command(connection, transaction, sql);
            purge.Parameters.Add(new NpgsqlParameter { Value = cutoff, NpgsqlDbType = NpgsqlDbType.Timestamp });
            await purge.ExecuteNonQueryAsync(cancellationToken);
        }

        await transaction.CommitAsync(cancellationToken);
        logger?.LogDebug(
            "Store statement history: {Outcome}, {Seen} active statement(s), {Kept} kept, {Hidden} hidden.",
            result.Outcome, result.StatementsSeen, result.StatementsKept, result.HiddenStatements);
        return result;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql) =>
        new(sql, connection, transaction) { CommandTimeout = SnapshotTimeoutSeconds };

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, sql);
        return await command.ExecuteScalarAsync(cancellationToken);
    }

    private static async Task InsertCaptureAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        DateTime captureTime,
        int? intervalSeconds,
        DateTime? statsReset,
        long? dealloc,
        long? deallocDelta,
        int seen,
        int kept,
        int hidden,
        string outcome,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, transaction, CaptureInsertSql);
        command.Parameters.Add(new NpgsqlParameter { Value = captureTime, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)intervalSeconds ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)statsReset ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Timestamp });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)dealloc ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)deallocDelta ?? DBNull.Value, NpgsqlDbType = NpgsqlDbType.Bigint });
        command.Parameters.Add(new NpgsqlParameter { Value = seen, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter { Value = kept, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter { Value = hidden, NpgsqlDbType = NpgsqlDbType.Integer });
        command.Parameters.Add(new NpgsqlParameter { Value = outcome, NpgsqlDbType = NpgsqlDbType.Text });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }
}
