/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9411: the record of what the service's background passes did, one row per step. Before it, the hourly store-maintenance tick, the
/// 15-minute Query Store compose rollup and the hourly store self-metrics tick wrote one log line per step and nothing a store read could
/// show, so lining a pass up against a checkpoint spike or a slow read took a search of the service host's log. A row holds the pass, the
/// step, when it started and finished, whether it worked, and the counts the step's own log line already carries.
///
/// <para><b>The columns are generic on purpose.</b> <c>built</c> is what the step made (rows, hours, days or snapshots, whatever its log line
/// counts), <c>removed</c> is what it deleted, and <c>detail</c> is a short text for the one thing a count cannot say (a step that failed
/// says why, a step that was cut short says how many it left).</para>
///
/// <para><b>Retention.</b> One bounded DELETE of rows older than <see cref="RetentionDays"/>, run by the recorder at most once an hour, the
/// shape <c>collect.store_metrics</c> uses. The table is plain, so the catalog purge never reaches it.</para>
/// </summary>
public static class StorePassSteps
{
    /// <summary>The migration that creates the table.</summary>
    public const int Version = 306;

    /// <summary>The hourly store-maintenance tick.</summary>
    public const string StoreMaintenancePass = "store_maintenance";

    /// <summary>The 15-minute Query Store compose rollup.</summary>
    public const string QueryStoreComposePass = "query_store_compose";

    /// <summary>The hourly store self-metrics tick.</summary>
    public const string StoreSelfMetricsPass = "store_self_metrics";

    /// <summary>Days a step row is kept. A pass writes about twenty rows an hour, so ninety days is a few tens of thousands of narrow rows.</summary>
    public const int RetentionDays = 90;

    /// <summary>The longest <c>detail</c> text a row keeps.</summary>
    public const int MaxDetailLength = 300;

    /// <summary>The step's outcome: it ran and finished.</summary>
    public const string OutcomeOk = "ok";

    /// <summary>The step's outcome: it threw, or it finished with builds it could not complete.</summary>
    public const string OutcomeFailed = "failed";

    /// <summary>The step's outcome: it did not run, with the reason in <c>detail</c>.</summary>
    public const string OutcomeSkipped = "skipped";

    /// <summary>The step table and the newest-first index the window read walks. Created empty, and idempotent: no data moves.</summary>
    public const string CreateSql = @"
CREATE TABLE IF NOT EXISTS collect.store_pass_steps
(
    step_id bigint GENERATED ALWAYS AS IDENTITY,
    pass text NOT NULL,
    step text NOT NULL,
    started_at timestamp NOT NULL,
    finished_at timestamp NOT NULL,
    outcome text NOT NULL,
    built bigint NULL,
    removed bigint NULL,
    detail text NULL,
    CONSTRAINT pk_store_pass_steps PRIMARY KEY (step_id)
);

CREATE INDEX IF NOT EXISTS idx_store_pass_steps_started
ON collect.store_pass_steps (started_at DESC);";

    /// <summary>One step row. $1 pass, $2 step, $3 started_at, $4 finished_at, $5 outcome, $6 built, $7 removed, $8 detail.</summary>
    public const string InsertSql = @"
INSERT INTO collect.store_pass_steps (pass, step, started_at, finished_at, outcome, built, removed, detail)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";

    /// <summary>The retention DELETE. $1 cutoff (naive UTC).</summary>
    public const string RetentionDeleteSql = @"
DELETE FROM collect.store_pass_steps
WHERE started_at < $1";

    /// <summary>
    /// The window read: rows that started in <c>[$1, $2)</c>, newest first, with the pass and step filters ($3, $4) skipped when null, and the
    /// matching row total beside each row so the caller learns how many the cap left out. $5 row cap.
    /// </summary>
    public const string ReadSql = @"
SELECT s.pass, s.step, s.started_at, s.finished_at, s.outcome, s.built, s.removed, s.detail, COUNT(*) OVER () AS total
FROM collect.store_pass_steps AS s
WHERE s.started_at >= $1
  AND s.started_at < $2
  AND ($3::text IS NULL OR s.pass = $3)
  AND ($4::text IS NULL OR s.step = $4)
ORDER BY s.started_at DESC, s.step_id DESC
LIMIT $5";

    /// <summary>What a step reports about its work.</summary>
    /// <param name="Built">What the step made, in whatever unit its log line counts; null when the step counts nothing.</param>
    /// <param name="Removed">What the step deleted; null when it deletes nothing.</param>
    /// <param name="Failed">How many builds the step could not complete. Above zero the row's outcome is failed.</param>
    /// <param name="Detail">A short text for what a count cannot say; null for none.</param>
    /// <param name="Skipped">True when the step did not run, with the reason in <paramref name="Detail"/>.</param>
    public readonly record struct StepCounts(long? Built = null, long? Removed = null, int Failed = 0, string? Detail = null, bool Skipped = false);

    /// <summary>One row the window read returns.</summary>
    public sealed record Row(
        string Pass, string Step, DateTime StartedAt, DateTime FinishedAt, string Outcome, long? Built, long? Removed, string? Detail);

    /// <summary>The window read's answer: the rows newest first, and how many rows matched in all.</summary>
    public sealed record Page(IReadOnlyList<Row> Rows, long Total);

    /// <summary>Reads the rows that started in <c>[from, to)</c>, newest first, at most <paramref name="limit"/>.
    /// <paramref name="commandTimeoutSeconds"/> is the read's own deadline, so a stalled store cannot hold the pooled connection on Npgsql's default.</summary>
    public static async Task<Page> ReadAsync(
        NpgsqlDataSource dataSource, DateTime from, DateTime to, string? pass, string? step, int limit, CancellationToken cancellationToken,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds)
    {
        await using var command = dataSource.CreateCommand(ReadSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.AddWithValue(Unspecified(from));
        command.Parameters.AddWithValue(Unspecified(to));
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)pass ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)step ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
        command.Parameters.AddWithValue(limit);

        var rows = new List<Row>();
        long total = 0;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                reader.GetString(0),
                reader.GetString(1),
                DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetInt64(5),
                reader.IsDBNull(6) ? null : reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetString(7)));
            total = reader.GetInt64(8);
        }

        return new Page(rows, total);
    }

    private static DateTime Unspecified(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    internal static string? Shorten(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return null;
        }

        var oneLine = text.ReplaceLineEndings(" ");
        return oneLine.Length <= MaxDetailLength ? oneLine : oneLine[..MaxDetailLength];
    }

    /// <summary>
    /// The writer the passes call next to each step's log line. It never fails the step and never slows it past its own short budget: a step's
    /// outcome is recorded after the step has finished, and a write that fails (the table missing on a store that is not yet migrated, the store
    /// busy, the budget spent) is logged ONCE at Warning, then at Debug, so an hourly repeat does not fill the service log.
    /// </summary>
    public sealed class Recorder
    {
        /// <summary>How long one write, and the retention DELETE that follows it, may take before the recorder gives up on it.</summary>
        public static readonly TimeSpan WriteBudget = TimeSpan.FromSeconds(10);

        /// <summary>The write commands' own deadline, the same 10 s as <see cref="WriteBudget"/>, so the budget token and the command agree.</summary>
        private static readonly int WriteCommandSeconds = (int)WriteBudget.TotalSeconds;

        /// <summary>The detail of a step that ran out its time budget, in the words the retention and convergence rows use.</summary>
        public const string CutShortDetail = "cut short by its time budget";

        private static readonly TimeSpan s_pruneEvery = TimeSpan.FromHours(1);

        private readonly NpgsqlDataSource _dataSource;
        private readonly ILogger _logger;
        private readonly Func<DateTime> _utcNow;
        private readonly CancellationToken _shutdown;
        private DateTime _lastPrune = DateTime.MinValue;
        private int _warned;

        /// <summary>
        /// Creates a recorder that writes to <paramref name="dataSource"/>. <paramref name="utcNow"/> is the clock, for tests.
        /// <paramref name="shutdown"/> is the service's stopping token: a step cancelled while it is live ran out its own time slice.
        /// </summary>
        public Recorder(NpgsqlDataSource dataSource, ILogger logger, Func<DateTime>? utcNow = null, CancellationToken shutdown = default)
        {
            _dataSource = dataSource;
            _logger = logger;
            _utcNow = utcNow ?? (() => DateTime.UtcNow);
            _shutdown = shutdown;
        }

        /// <summary>
        /// Runs <paramref name="body"/> as one step of <paramref name="pass"/> and records the row. A body that throws records outcome failed with
        /// the message and the exception goes on to the caller, whose own catch logs it as before. A body cancelled while the service is still
        /// running ran out its own time budget, so it records outcome failed ("cut short by its time budget") and rethrows. Cancellation of the
        /// service itself is not a step failure, so a body cancelled by the shutdown records nothing and rethrows.
        /// </summary>
        public async Task StepAsync(string pass, string step, Func<Task<StepCounts>> body)
        {
            var started = _utcNow();
            var clock = Stopwatch.StartNew();
            StepCounts counts;
            try
            {
                counts = await body();
            }
            catch (OperationCanceledException)
            {
                /* #9411: the write uses its own WriteBudget token, so the cancelled step token does not cancel it. */
                if (!_shutdown.IsCancellationRequested)
                {
                    await WriteAsync(pass, step, started, clock.Elapsed, OutcomeFailed, null, null, CutShortDetail);
                }

                throw;
            }
            catch (Exception ex)
            {
                await WriteAsync(pass, step, started, clock.Elapsed, OutcomeFailed, null, null, Shorten(ex.Message));
                throw;
            }

            var outcome = counts.Skipped ? OutcomeSkipped : counts.Failed > 0 ? OutcomeFailed : OutcomeOk;
            var detail = counts.Failed > 0 && string.IsNullOrEmpty(counts.Detail) ? $"{counts.Failed} failed" : counts.Detail;
            await WriteAsync(pass, step, started, clock.Elapsed, outcome, counts.Built, counts.Removed, Shorten(detail));
        }

        /// <summary>Records a step that ran outside <see cref="StepAsync"/>: the caller measured it and knows how it ended.</summary>
        public Task RecordAsync(string pass, string step, DateTime startedUtc, TimeSpan elapsed, StepCounts counts, string? failure = null)
        {
            var outcome = failure is not null || counts.Failed > 0 ? OutcomeFailed : counts.Skipped ? OutcomeSkipped : OutcomeOk;
            var detail = failure ?? (counts.Failed > 0 && string.IsNullOrEmpty(counts.Detail) ? $"{counts.Failed} failed" : counts.Detail);
            return WriteAsync(pass, step, startedUtc, elapsed, outcome, counts.Built, counts.Removed, Shorten(detail));
        }

        private async Task WriteAsync(
            string pass, string step, DateTime startedUtc, TimeSpan elapsed, string outcome, long? built, long? removed, string? detail)
        {
            try
            {
                using var budget = new CancellationTokenSource(WriteBudget);
                await using var command = _dataSource.CreateCommand(InsertSql);
                command.CommandTimeout = WriteCommandSeconds;
                command.Parameters.AddWithValue(pass);
                command.Parameters.AddWithValue(step);
                command.Parameters.AddWithValue(Unspecified(startedUtc));
                command.Parameters.AddWithValue(Unspecified(startedUtc + elapsed));
                command.Parameters.AddWithValue(outcome);
                command.Parameters.Add(new NpgsqlParameter { Value = (object?)built ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
                command.Parameters.Add(new NpgsqlParameter { Value = (object?)removed ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
                command.Parameters.Add(new NpgsqlParameter { Value = (object?)detail ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
                await command.ExecuteNonQueryAsync(budget.Token);

                var now = _utcNow();
                if (now - _lastPrune >= s_pruneEvery)
                {
                    _lastPrune = now;
                    await using var prune = _dataSource.CreateCommand(RetentionDeleteSql);
                    prune.CommandTimeout = WriteCommandSeconds;
                    prune.Parameters.AddWithValue(Unspecified(now.AddDays(-RetentionDays)));
                    await prune.ExecuteNonQueryAsync(budget.Token);
                }
            }
            catch (Exception ex)
            {
                /* Nothing here may fail or slow the step: not a missing table, not a busy store, not the service stopping. */
                if (Interlocked.Exchange(ref _warned, 1) == 0)
                {
                    _logger.LogWarning(
                        "The step history could not be written, so {Pass} '{Step}' has no row (the pass is not affected, and later failures log at Debug): {Message}",
                        pass, step, ex.Message);
                }
                else
                {
                    _logger.LogDebug("The step history could not be written for {Pass} '{Step}': {Message}", pass, step, ex.Message);
                }
            }
        }
    }
}
