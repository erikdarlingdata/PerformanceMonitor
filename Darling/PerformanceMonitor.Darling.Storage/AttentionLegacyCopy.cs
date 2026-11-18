/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Spaces a batch's WAL out (the retention purge's rule, #4823) without the Storage project knowing the Service's pacer.
/// <see cref="AttentionLegacyCopy"/> reads the WAL position on the batch's own connection before the batch and reports it
/// after the commit. Neither call throws, except for a cancellation.
/// </summary>
public interface IBatchWalPacer
{
    /// <summary>The WAL position before a batch, or null when the pacer measures nothing.</summary>
    Task<long?> ReadWalPositionAsync(NpgsqlConnection connection, CancellationToken cancellationToken);

    /// <summary>Charges the WAL written since <paramref name="before"/>, and waits when that overdraws the budget.</summary>
    Task AfterBatchAsync(NpgsqlConnection connection, long? before, CancellationToken cancellationToken);
}

/// <summary>
/// #8967: the background copy that moves the older <c>collect.attention_events_legacy</c> rows into <c>collect.attention_events</c>.
/// The rung that made the attention table a hypertable (V277) swapped a populated plain table to <c>_legacy</c> with a rename, so it
/// held no lock longer than a catalog change. The new table starts empty and the collector writes to it at once. This copy then
/// fills in the older history, in short transactions that never lock the new table against a writer, and drops the legacy table at
/// the end.
///
/// <para><b>Progress.</b> One row in <c>collect.attention_events_legacy_copy</c>: <c>next_page</c> (the first heap page not yet
/// copied), <c>last_page</c> (the legacy table's page count when the copy began, and the legacy table never changes after the
/// swap) and <c>cutoff</c> (now less the attention retention, naive UTC like the column). The row is the only state. Each batch
/// copies one TID range of legacy pages and advances <c>next_page</c> in the SAME transaction, so a restart, a failure or a
/// cancellation resumes at the last committed batch and no row is copied twice. Rows older than the cutoff are skipped: the
/// retention sweep would drop them anyway.</para>
///
/// <para><b>The end.</b> When <c>next_page</c> reaches <c>last_page</c> one transaction deletes the progress row and drops the
/// legacy table, under a 5 s <c>lock_timeout</c>. A reader that holds the legacy table makes the drop time out: both stay in
/// place, and the next pass retries the drop alone (every batch is already done). The progress table itself is never dropped,
/// because the schema probe reads it.</para>
///
/// <para><b>Pace.</b> A batch is <see cref="DefaultBatchPages"/> heap pages. After each commit the WAL it wrote goes to
/// <see cref="IBatchWalPacer"/>, which is the retention purge's pacer, so the copy holds the same share of the checkpoint budget
/// the purge does. It runs whatever the TimescaleDB mode: the new table may be a hypertable or a plain table.</para>
/// </summary>
public static class AttentionLegacyCopy
{
    /// <summary>The swapped-out table the copy reads and then drops.</summary>
    public const string LegacyTable = "collect.attention_events_legacy";

    /// <summary>The table the rows go to.</summary>
    public const string TargetTable = "collect.attention_events";

    /// <summary>The one-row progress table, created by the rung and never dropped.</summary>
    public const string ProgressTable = "collect.attention_events_legacy_copy";

    /// <summary>Heap pages one batch copies (8 kB each).</summary>
    public const int DefaultBatchPages = 2000;

    /// <summary>The lock wait of the final drop, in seconds.</summary>
    public const int DropLockTimeoutSeconds = 5;

    /// <summary>Deadline of one batch, in seconds.</summary>
    public const int BatchTimeoutSeconds = 600;

    /// <summary>How long the background loop waits after a pass that left work, before the next one.</summary>
    public static readonly TimeSpan RetryInterval = TimeSpan.FromMinutes(10);

    private const int ShortTimeoutSeconds = 60;
    private static readonly TimeSpan ProgressLogEvery = TimeSpan.FromMinutes(1);

    /// <summary>How one pass ended.</summary>
    public enum PassOutcome
    {
        /// <summary>There is no legacy table to copy (a fresh store, a plain-PostgreSQL store, or the copy is finished).</summary>
        NothingToDo,

        /// <summary>The copy reached the end and the legacy table is dropped.</summary>
        Finished,

        /// <summary>Work is left: a failure, or a drop that hit its lock timeout. A later pass resumes.</summary>
        RetryLater,

        /// <summary>Shutdown stopped the pass between batches.</summary>
        Cancelled,
    }

    /// <summary>
    /// The background loop: waits <paramref name="delay"/> (off the post-restart IO burst, as the Query Store background steps do), then runs
    /// <see cref="RunPassAsync"/> until a pass finds nothing left or finishes. A pass that leaves work waits <see cref="RetryInterval"/>.
    /// Never throws: every failure is logged inside the pass, and cancellation ends it quietly.
    /// </summary>
    /// <param name="retentionDays">The retention the retention sweep applies to this table, read when the copy first starts.</param>
    /// <param name="pacerFactory">Builds the WAL pacer on the first batch of a pass. Null runs unpaced.</param>
    public static async Task RunDelayedAsync(
        NpgsqlDataSource postgres, ILogger logger, TimeSpan delay, Func<int> retentionDays,
        Func<CancellationToken, Task<IBatchWalPacer?>>? pacerFactory, CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            while (!cancellationToken.IsCancellationRequested)
            {
                var outcome = await RunPassAsync(
                    postgres, logger, retentionDays, pacerFactory, DefaultBatchPages, cancellationToken).ConfigureAwait(false);
                if (outcome != PassOutcome.RetryLater)
                {
                    return;
                }

                await Task.Delay(RetryInterval, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            logger.LogDebug("The attention legacy copy stopped at shutdown; the next start resumes it.");
        }
    }

    /// <summary>
    /// One pass: start the copy if it has not started, copy every remaining batch, then drop the legacy table. A failure is logged and
    /// returns <see cref="PassOutcome.RetryLater"/>. A cancellation returns <see cref="PassOutcome.Cancelled"/> with the progress row at the
    /// last committed batch.
    /// </summary>
    public static async Task<PassOutcome> RunPassAsync(
        NpgsqlDataSource postgres, ILogger logger, Func<int> retentionDays,
        Func<CancellationToken, Task<IBatchWalPacer?>>? pacerFactory, int batchPages, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchPages, 1);
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            if (!await LegacyExistsAsync(connection, cancellationToken).ConfigureAwait(false))
            {
                return PassOutcome.NothingToDo;
            }

            var cutoff = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-Math.Max(1, retentionDays())), DateTimeKind.Unspecified);
            await StartIfNeededAsync(connection, cutoff, cancellationToken).ConfigureAwait(false);

            var progress = await ReadProgressAsync(connection, cancellationToken).ConfigureAwait(false);
            if (progress is null)
            {
                return PassOutcome.RetryLater;
            }

            var (nextPage, lastPage, copyCutoff) = progress.Value;
            if (nextPage < lastPage)
            {
                logger.LogInformation(
                    "The attention legacy copy is running: pages {Next} to {Last} of the swapped-out table, rows from {Cutoff:u} on.",
                    nextPage, lastPage, copyCutoff);
                var columns = await ColumnListAsync(connection, cancellationToken).ConfigureAwait(false);
                var pacer = pacerFactory is null ? null : await pacerFactory(cancellationToken).ConfigureAwait(false);
                var copied = 0L;
                var lastLog = DateTime.UtcNow;
                while (nextPage < lastPage)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var batchEnd = nextPage + batchPages;
                    copied += await CopyBatchAsync(connection, columns, nextPage, batchEnd, copyCutoff, pacer, cancellationToken).ConfigureAwait(false);
                    nextPage = batchEnd;
                    if (DateTime.UtcNow - lastLog >= ProgressLogEvery)
                    {
                        lastLog = DateTime.UtcNow;
                        logger.LogInformation(
                            "The attention legacy copy is at page {Next} of {Last}; {Rows} rows copied this pass.",
                            Math.Min(nextPage, lastPage), lastPage, copied);
                    }
                }

                logger.LogInformation("The attention legacy copy read every page; {Rows} rows copied this pass. Dropping the legacy table.", copied);
            }

            if (!await DropAsync(connection, logger, cancellationToken).ConfigureAwait(false))
            {
                return PassOutcome.RetryLater;
            }

            logger.LogInformation("The attention legacy copy is finished and the legacy table is dropped.");
            return PassOutcome.Finished;
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return PassOutcome.Cancelled;
        }
        catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
        {
            logger.LogWarning(ex, "The attention legacy copy failed: {Message}. It resumes at the last committed batch on a later pass.", ex.Message);
            return PassOutcome.RetryLater;
        }
    }

    private static async Task<bool> LegacyExistsAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT to_regclass('" + LegacyTable + "') IS NOT NULL AND to_regclass('" + ProgressTable + "') IS NOT NULL AND to_regclass('" + TargetTable + "') IS NOT NULL",
            connection) { CommandTimeout = ShortTimeoutSeconds };
        return (bool)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    /// <summary>
    /// The first run: one progress row, with the legacy table's page count at this moment. The table never changes after the swap, so the
    /// count is final. Does nothing when a row exists.
    /// </summary>
    private static async Task StartIfNeededAsync(NpgsqlConnection connection, DateTime cutoff, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO " + ProgressTable + " (next_page, last_page, cutoff) "
            + "SELECT 0, pg_relation_size('" + LegacyTable + "') / current_setting('block_size')::bigint, @cutoff "
            + "WHERE NOT EXISTS (SELECT 1 FROM " + ProgressTable + ")",
            connection) { CommandTimeout = ShortTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlTypes.NpgsqlDbType.Timestamp) { Value = cutoff });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static async Task<(long NextPage, long LastPage, DateTime Cutoff)?> ReadProgressAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT next_page, last_page, cutoff FROM " + ProgressTable + " ORDER BY next_page DESC LIMIT 1",
            connection) { CommandTimeout = ShortTimeoutSeconds };
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        return await reader.ReadAsync(cancellationToken).ConfigureAwait(false)
            ? (reader.GetInt64(0), reader.GetInt64(1), reader.GetDateTime(2))
            : null;
    }

    /// <summary>
    /// The columns both tables hold, in the new table's order, quoted. The new table was made LIKE the legacy one, so they match today, and a
    /// column a later rung adds to the new table alone is left to its default.
    /// </summary>
    private static async Task<string> ColumnListAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            "SELECT string_agg(quote_ident(n.attname), ', ' ORDER BY n.attnum) FROM pg_attribute AS n "
            + "JOIN pg_attribute AS l ON l.attname = n.attname AND l.attrelid = '" + LegacyTable + "'::regclass AND l.attnum > 0 AND NOT l.attisdropped "
            + "WHERE n.attrelid = '" + TargetTable + "'::regclass AND n.attnum > 0 AND NOT n.attisdropped AND n.attgenerated = ''",
            connection) { CommandTimeout = ShortTimeoutSeconds };
        return (await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)) as string
            ?? throw new InvalidOperationException("the attention tables share no column");
    }

    /// <summary>One batch: the TID range of pages [<paramref name="from"/>, <paramref name="to"/>) and the progress update, in one transaction.</summary>
    private static async Task<long> CopyBatchAsync(
        NpgsqlConnection connection, string columns, long from, long to, DateTime cutoff, IBatchWalPacer? pacer,
        CancellationToken cancellationToken)
    {
        var walBefore = pacer is null ? null : await pacer.ReadWalPositionAsync(connection, cancellationToken).ConfigureAwait(false);

        long rows;
        await using (var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false))
        {
            /* The page numbers are longs formatted here, never user text, and a literal tid lets the planner pick a TID range scan. */
            await using (var insert = new NpgsqlCommand(
                "INSERT INTO " + TargetTable + " (" + columns + ") SELECT " + columns + " FROM " + LegacyTable
                + " WHERE ctid >= '(" + from.ToString(CultureInfo.InvariantCulture) + ",0)'::tid AND ctid < '(" + to.ToString(CultureInfo.InvariantCulture)
                + ",0)'::tid AND collection_time >= @cutoff",
                connection, transaction) { CommandTimeout = BatchTimeoutSeconds })
            {
                insert.Parameters.Add(new NpgsqlParameter("cutoff", NpgsqlTypes.NpgsqlDbType.Timestamp) { Value = cutoff });
                rows = await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await using (var advance = new NpgsqlCommand(
                "UPDATE " + ProgressTable + " SET next_page = " + to.ToString(CultureInfo.InvariantCulture),
                connection, transaction) { CommandTimeout = ShortTimeoutSeconds })
            {
                await advance.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        }

        if (pacer is not null)
        {
            await pacer.AfterBatchAsync(connection, walBefore, cancellationToken).ConfigureAwait(false);
        }

        return rows;
    }

    /// <summary>
    /// Deletes the progress row and drops the legacy table in one transaction under <see cref="DropLockTimeoutSeconds"/>. A lock timeout
    /// (55P03) rolls both back and returns false, because a reader still holds the table: not an error, the next pass retries the drop alone.
    /// </summary>
    private static async Task<bool> DropAsync(NpgsqlConnection connection, ILogger logger, CancellationToken cancellationToken)
    {
        try
        {
            await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
            await using var command = new NpgsqlCommand(
                "SET LOCAL lock_timeout = '" + DropLockTimeoutSeconds.ToString(CultureInfo.InvariantCulture) + "s'; "
                + "DROP TABLE " + LegacyTable + "; DELETE FROM " + ProgressTable,
                connection, transaction) { CommandTimeout = ShortTimeoutSeconds };
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
            return true;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            logger.LogInformation("The attention legacy copy could not drop the legacy table within {Seconds} s because another session holds it. It tries again on the next pass.", DropLockTimeoutSeconds);
            return false;
        }
    }
}
