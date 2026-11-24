/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store's compression backlog per hypertable, read live (#9428). The hourly store tick already counts the chunks past their
/// compression delay that are still uncompressed and logs the count when it is not zero. This is the same count, from the same
/// statement (<see cref="TimescaleSupport.CompressionActivitySql"/>), on the read surface, with what a reader needs to judge it:
/// the oldest waiting chunk's range end, the policy's <c>compress_after</c>, the range end of the newest compressed chunk (the
/// horizon a reader that must not touch compressed chunks stays after), and the job's own last run, last success, next start and
/// last error.
///
/// <para><b>It adds no table and no collector.</b> Nothing is stored: the statement reads the TimescaleDB catalog and job stats
/// the moment it is asked.</para>
///
/// <para><b>The last error is the one fact the role may not see.</b> <c>timescaledb_information.job_errors</c> shows a row only to
/// a member of the database owner's role or of the job's owner, and the store's <c>mcp</c> and <c>viewer</c> roles are neither
/// (see the note on <c>DarlingStoreMetricsReader.JobHistoryEvidenceSql</c>). So the read asks the same question the view asks,
/// per job (<see cref="CompressionActivity.ErrorsVisible"/>), and reports a job's error as unreadable for such a role rather than as
/// absent. <c>last_run_status</c> comes from <c>job_stats</c>, which every role can read, so a failing job still shows as
/// <c>Failed</c> there.</para>
/// </summary>
public static class DarlingStoreCompressionHealthReader
{
    /// <summary>The most characters of a job's error message the read returns.</summary>
    public const int MaxErrorMessageChars = 500;

    /// <summary>
    /// The newest recorded error of each job in <c>$1</c>, one row per job. <c>start_time</c> is a <c>timestamptz</c>, so the row
    /// carries the instant and <see cref="DateTime.Kind"/> is UTC.
    /// </summary>
    public const string LastErrorSql = @"
SELECT DISTINCT ON (e.job_id) e.job_id, e.start_time, e.sqlerrcode, e.err_message
FROM timescaledb_information.job_errors AS e
WHERE e.job_id = ANY($1)
ORDER BY e.job_id, e.start_time DESC";

    /// <summary>Whether the connected database has the TimescaleDB extension.</summary>
    public const string ExtensionProbeSql =
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'timescaledb')";

    /// <summary>One job's newest recorded error.</summary>
    /// <param name="StartedAtUtc">When the failed run started (UTC), null when the view recorded none.</param>
    /// <param name="SqlState">The error's SQLSTATE, null when the view carried none.</param>
    /// <param name="Message">The error message, cut to <see cref="MaxErrorMessageChars"/> characters.</param>
    public sealed record JobError(DateTime? StartedAtUtc, string? SqlState, string? Message);

    /// <summary>The result of <see cref="GetAsync"/>.</summary>
    /// <param name="TimescaleInstalled">False when the database has no TimescaleDB extension, which has no compression policies to read.</param>
    /// <param name="Hypertables">One row per compression policy, the most waiting chunks first. Empty when there is no policy.</param>
    /// <param name="Errors">The newest error of each job that has one the role can read, by job id.</param>
    /// <param name="TimescaleVersion">The extension's version, or null when it could not be read, which reads as a version below 2.26.4.</param>
    /// <param name="ErrorsReadFailure">One sentence when the error read itself failed, so the missing errors are not read as none. Null otherwise.</param>
    public sealed record CompressionHealth(
        bool TimescaleInstalled,
        IReadOnlyList<CompressionActivity> Hypertables,
        IReadOnlyDictionary<int, JobError> Errors,
        string? ErrorsReadFailure,
        Version? TimescaleVersion = null)
    {
        /// <summary>The hypertables with at least one chunk past the delay and still uncompressed.</summary>
        public int HypertablesWithBacklog => Hypertables.Count(h => h.EligibleUncompressedChunks > 0);

        /// <summary>The chunks past the delay and still uncompressed, over every hypertable.</summary>
        public long TotalBacklogChunks => Hypertables.Sum(h => h.EligibleUncompressedChunks);
    }

    /// <summary>The job is waiting for its next start.</summary>
    public const string NextStartScheduled = "scheduled";

    /// <summary>The job is parked: paused, or its next start is <c>infinity</c> (<c>scheduled =&gt; false</c>).</summary>
    public const string NextStartPaused = "paused";

    /// <summary>The scheduler holds the job at <c>-infinity</c> after a run that crashed.</summary>
    public const string NextStartCrashBackoff = "crash_backoff";

    /// <summary>
    /// What a job's null <c>next_start</c> means (#9428). <c>infinity</c> is a parked job, <c>-infinity</c> a crash backoff, and a job
    /// that is running right now also holds <c>-infinity</c> while it runs, so that is not a backoff. A finite start is
    /// <c>scheduled</c>, unless the job's status says <c>Paused</c>.
    /// </summary>
    public static string NextStartStateFor(string? jobStatus, string? nextStartSentinel)
    {
        if (string.Equals(nextStartSentinel, "infinity", StringComparison.Ordinal))
        {
            return NextStartPaused;
        }

        if (string.Equals(nextStartSentinel, "-infinity", StringComparison.Ordinal)
            && !string.Equals(jobStatus, "Running", StringComparison.OrdinalIgnoreCase))
        {
            return NextStartCrashBackoff;
        }

        return string.Equals(jobStatus, "Paused", StringComparison.OrdinalIgnoreCase) ? NextStartPaused : NextStartScheduled;
    }

    /// <summary>
    /// The sentence that goes with a <c>crash_backoff</c> state, by the store's TimescaleDB version: from 2.26.4 the scheduler
    /// clears the backoff itself, below it (and for an unreadable version) the job stays held until someone re-arms it.
    /// </summary>
    public static string CrashBackoffNote(Version? timescaleVersion) =>
        TimescaleSupport.SchedulerRecoversNegativeInfinity(timescaleVersion)
            ? "The job crashed and the scheduler holds it back. This TimescaleDB clears that on its own and runs the job again."
            : "The job crashed and the scheduler holds it back. This TimescaleDB is below 2.26.4 (or its version could not be read), "
              + "which does not clear that on its own: the job stays held until it is re-armed with alter_job.";

    /// <summary>Most waiting chunks first, then by name, so the worst table leads and the order is stable.</summary>
    public static IReadOnlyList<CompressionActivity> Order(IEnumerable<CompressionActivity> activity) =>
        activity
            .OrderByDescending(a => a.EligibleUncompressedChunks)
            .ThenBy(a => a.HypertableName, StringComparer.Ordinal)
            .ToList();

    /// <summary>
    /// The read. A failure of the main statement throws: an empty answer must mean no policy. A failure of the error statement alone is
    /// reported in <see cref="CompressionHealth.ErrorsReadFailure"/> and the rest still returns.
    /// </summary>
    public static async Task<CompressionHealth> GetAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken = default)
    {
        if (postgres is null)
        {
            throw new ArgumentNullException(nameof(postgres));
        }

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        bool installed;
        await using (var probe = new NpgsqlCommand(ExtensionProbeSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds })
        {
            installed = (bool)(await probe.ExecuteScalarAsync(cancellationToken) ?? false);
        }

        if (!installed)
        {
            return new CompressionHealth(false, Array.Empty<CompressionActivity>(), new Dictionary<int, JobError>(), null, null);
        }

        var activity = Order(await TimescaleSupport.QueryCompressionActivityAsync(connection, cancellationToken));

        /* Only a job whose last run failed has an error worth the second read, and only one the role can see. */
        var failing = activity
            .Where(a => a.JobId is not null && a.ErrorsVisible && string.Equals(a.LastRunStatus, "Failed", StringComparison.OrdinalIgnoreCase))
            .Select(a => a.JobId!.Value)
            .Distinct()
            .ToArray();

        var errors = new Dictionary<int, JobError>();
        string? failure = null;
        if (failing.Length > 0)
        {
            try
            {
                await using var command = new NpgsqlCommand(LastErrorSql, connection) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
                command.Parameters.AddWithValue(failing);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var message = reader.IsDBNull(3) ? null : reader.GetString(3);
                    if (message is { Length: > MaxErrorMessageChars })
                    {
                        message = message[..MaxErrorMessageChars];
                    }

                    errors[reader.GetInt32(0)] = new JobError(
                        reader.IsDBNull(1) ? null : DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
                        reader.IsDBNull(2) ? null : reader.GetString(2),
                        message);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                failure = ex is PostgresException pg
                    ? $"The job error read was refused or failed (SQLSTATE {pg.SqlState}), so last_error is not shown."
                    : "The job error read failed, so last_error is not shown.";
            }
        }

        var version = await TimescaleSupport.ReadTimescaleVersionAsync(connection, null, cancellationToken);
        return new CompressionHealth(true, activity, errors, failure, version);
    }
}
