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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Each TimescaleDB background job run, read live from TimescaleDB's own history (#9440). TimescaleDB 2.15 and later keeps one row per run in
/// <c>timescaledb_information.job_history</c>. This read adds no table and no collector: it asks that view the moment it is called.
///
/// <para><b>What the role can see is the point.</b> The view shows a row only to a member of the database owner's role or of the job's owner
/// role, and the store's <c>mcp</c> and <c>viewer</c> roles are neither (see the note on <see cref="DarlingStoreMetricsReader.JobHistoryEvidenceSql"/>).
/// A hidden run is not a short row, it is no row, so a role that cannot see a job's history gets an empty page for that job. The read therefore
/// counts, with the view's own two tests, how many of the jobs it was asked about the role can read (<see cref="Access"/>), and the tool says
/// "this role cannot read those jobs" rather than "no runs". That is the path for a store where <c>collect.store_job_runs</c> is not there
/// (an older schema, or a role without EXECUTE on it). With the function, the read goes through it instead: it runs as the store owner, so the
/// runs of every job come back, and it withholds only the error text from a caller TimescaleDB would not show it to (<see cref="Run.ErrorReadable"/>).</para>
///
/// <para><b>Successes are recorded only while <c>timescaledb.enable_job_execution_logging</c> is on.</b> A failed run is always recorded. The read
/// reports the setting so an empty window or a window of only failures is not read as "nothing ran".</para>
/// </summary>
internal static class DarlingStoreJobRunsReader
{
    /// <summary>The most characters of a run's error message the read returns.</summary>
    internal const int MaxErrorMessageChars = 500;

    /// <summary>The first TimescaleDB release that keeps per-run history.</summary>
    internal static readonly Version FirstVersionWithHistory = new(2, 15);

    internal const string ProbeSql = @"
SELECT (SELECT e.extversion FROM pg_catalog.pg_extension AS e WHERE e.extname = 'timescaledb'),
       pg_catalog.to_regclass('timescaledb_information.job_history') IS NOT NULL,
       EXISTS (SELECT 1 FROM pg_catalog.pg_proc AS f JOIN pg_catalog.pg_namespace AS n ON n.oid = f.pronamespace
               WHERE n.nspname = 'collect' AND f.proname = 'store_job_runs' AND f.pronargs = 5
                 AND pg_catalog.has_schema_privilege(n.oid, 'USAGE') AND pg_catalog.has_function_privilege(f.oid, 'EXECUTE'))";

    /// <summary>
    /// How many of the jobs matching $1 (kind) and $2 (hypertable) this connection's role can read the history of. The two tests are the view's own:
    /// membership of the database owner's role, or of the job's owner role. <c>IS TRUE</c> reads a NULL owner as "not a member".
    /// </summary>
    internal const string AccessSql = @"
SELECT count(*)::int AS jobs,
       count(*) FILTER (WHERE pg_catalog.pg_has_role(current_user, (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database()), 'MEMBER') IS TRUE
                           OR pg_catalog.pg_has_role(current_user, j.owner, 'MEMBER') IS TRUE)::int AS readable
FROM timescaledb_information.jobs AS j
WHERE ($1::text IS NULL OR j.proc_name = $1 OR j.proc_name = 'policy_' || $1)
  AND ($2::text IS NULL OR j.hypertable_name = $2)";

    /// <summary>
    /// How far back TimescaleDB's own history reaches: the oldest row this role can see, and the config of the job that trims the history
    /// (<c>drop_after</c>, <c>max_successes_per_job</c>, <c>max_failures_per_job</c>).
    /// </summary>
    internal const string HorizonSql = @"
SELECT (SELECT min(h.start_time) FROM timescaledb_information.job_history AS h),
       (SELECT j.config::text FROM timescaledb_information.jobs AS j WHERE j.proc_name = 'policy_job_stat_history_retention' ORDER BY j.job_id LIMIT 1)";

    internal const string LoggingSql = "SELECT pg_catalog.current_setting('timescaledb.enable_job_execution_logging', true)";

    /// <summary>
    /// The runs, newest first. $1 and $2 bound the start time (timestamptz), $3 is the job kind (the procedure name, with or without its
    /// <c>policy_</c> prefix), $4 the hypertable name and $5 the cap. Both filters are in the WHERE, so the cap never takes a run the filter
    /// would have dropped. The hypertable comes from the job, so a run of a job TimescaleDB has since deleted has none and never matches a
    /// hypertable filter.
    /// </summary>
    internal const string RunsSql = @"
SELECT h.id, h.job_id, h.proc_schema, h.proc_name, j.hypertable_name,
       h.start_time, h.finish_time, h.succeeded, h.sqlerrcode, true AS error_readable, h.err_message, NULL::timestamptz AS oldest_row_at
FROM timescaledb_information.job_history AS h
LEFT JOIN timescaledb_information.jobs AS j ON j.job_id = h.job_id
WHERE h.start_time >= $1 AND h.start_time < $2
  AND ($3::text IS NULL OR h.proc_name = $3 OR h.proc_name = 'policy_' || $3)
  AND ($4::text IS NULL OR j.hypertable_name = $4)
ORDER BY h.start_time DESC, h.id DESC
LIMIT $5";

    /// <summary>
    /// The same runs through <c>collect.store_job_runs</c>, with the same five parameters in the same order. The function applies both filters and the
    /// cap itself, before the limit, and runs as the store owner, so a role that cannot read the view still gets the rows.
    /// </summary>
    internal const string FunctionRunsSql =
        "SELECT " + StoreJobRunsFunction.SelectList + " FROM collect.store_job_runs($1, $2, $3, $4, $5)";

    /// <summary>Only the trim job's config: on the function path the oldest row comes from the function, and the view would show this role none.</summary>
    internal const string RetentionSql = @"
SELECT (SELECT j.config::text FROM timescaledb_information.jobs AS j WHERE j.proc_name = 'policy_job_stat_history_retention' ORDER BY j.job_id LIMIT 1)";

    /// <param name="ErrorReadable">False when the error text was withheld because the caller is neither the job's owner nor a member of the database owner's role. True on the view path, where the view hides a whole row instead.</param>
    internal sealed record Run(
        long Id, int JobId, string? ProcSchema, string? ProcName, string? HypertableName,
        DateTime StartedAt, DateTime? FinishedAt, bool? Succeeded, string? SqlState, bool ErrorReadable, string? ErrorMessage);

    internal sealed record Access(int JobsMatching, int JobsReadable)
    {
        internal int JobsHidden => JobsMatching - JobsReadable;
    }

    internal sealed record Retention(string? DropAfter, long? MaxSuccessesPerJob, long? MaxFailuresPerJob);

    internal sealed record Horizon(DateTime? OldestRowAt, Retention? Retention);

    /// <param name="Unavailable">Why there is nothing to read (no extension, or a TimescaleDB with no per-run history), null when the history can be read.</param>
    /// <param name="Access">How many matching jobs this role can read, or null when the runs came through the function and every job is readable.</param>
    internal sealed record JobRuns(
        string? Unavailable, string? Version, Access? Access, Horizon Horizon, string? LoggingSetting, IReadOnlyList<Run> Fetched);

    private static readonly Access NoAccess = new(0, 0);
    private static readonly Horizon NoHorizon = new(null, null);

    /// <summary>The newest runs that started in [<paramref name="startUtc"/>, <paramref name="endUtc"/>), at most <paramref name="fetch"/> of them.</summary>
    internal static async Task<JobRuns> ReadAsync(
        NpgsqlDataSource postgres, DateTime startUtc, DateTime endUtc, string? jobKind, string? hypertable, int fetch, CancellationToken cancellationToken)
    {
        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        string? version;
        bool hasView;
        bool hasFunction;
        await using (var probe = new NpgsqlCommand(ProbeSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
        await using (var reader = await probe.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            version = reader.IsDBNull(0) ? null : reader.GetString(0);
            hasView = reader.GetBoolean(1);
            hasFunction = reader.GetBoolean(2);
        }

        if (version is null)
        {
            return new JobRuns(
                "This store has no TimescaleDB extension, so it has no background jobs and no job history. That is how the store is built, not a read that failed.",
                null, NoAccess, NoHorizon, null, Array.Empty<Run>());
        }

        if (!hasView)
        {
            return new JobRuns(
                BelowFirstHistoryVersion(version)
                    ? $"This store runs TimescaleDB {version}, which keeps no per-run job history (it arrived in {FirstVersionWithHistory}). get_store_metrics still gives each job's totals and last run."
                    : $"This store runs TimescaleDB {version}, but timescaledb_information.job_history is not there for this connection, so there is no per-run history to read. get_store_metrics still gives each job's totals and last run.",
                version, NoAccess, NoHorizon, null, Array.Empty<Run>());
        }

        Access? access = null;
        if (!hasFunction)
        {
            await using var command = new NpgsqlCommand(AccessSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)jobKind ?? DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)hypertable ?? DBNull.Value });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            access = new Access(reader.GetInt32(0), reader.GetInt32(1));
        }

        var page = await ReadRunsAsync(
            connection, hasFunction ? FunctionRunsSql : RunsSql, startUtc, endUtc, jobKind, hypertable, fetch, cancellationToken);
        var runs = page.Runs;

        Horizon horizon;
        if (hasFunction)
        {
            /* The oldest row is the whole history's, not the window's or the filters': the function carries it on every row. A page with rows
               already has it. Only an empty page asks the function for the newest row of all time. */
            var oldestRowAt = page.OldestRowAt;
            if (runs.Count == 0)
            {
                oldestRowAt = (await ReadRunsAsync(connection, FunctionRunsSql, HistoryFloor, HistoryCeiling, null, null, 1, cancellationToken)).OldestRowAt;
            }

            await using var command = new NpgsqlCommand(RetentionSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            horizon = new Horizon(oldestRowAt, ParseRetention(reader.IsDBNull(0) ? null : reader.GetString(0)));
        }
        else
        {
            await using var command = new NpgsqlCommand(HorizonSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            horizon = new Horizon(
                reader.IsDBNull(0) ? null : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                ParseRetention(reader.IsDBNull(1) ? null : reader.GetString(1)));
        }

        /* Read after the history, so the TimescaleDB library is loaded in this session and the setting is registered. */
        string? logging;
        await using (var command = new NpgsqlCommand(LoggingSql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds })
        {
            logging = await command.ExecuteScalarAsync(cancellationToken) as string;
        }

        return new JobRuns(null, version, access, horizon, logging, runs);
    }

    /// <summary>The ends of a window that holds every history row TimescaleDB could keep.</summary>
    private static readonly DateTime HistoryFloor = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime HistoryCeiling = new(2200, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>The error text, cut to <see cref="MaxErrorMessageChars"/> UTF-16 units but never inside a surrogate pair or a grapheme: a cut at a bare index ends the payload in U+FFFD when a non-BMP character sits across it (#9484).</summary>
    internal static string? CutError(string? message) =>
        message is { Length: > MaxErrorMessageChars } ? message[..McpHelpers.TextElementCutLength(message, MaxErrorMessageChars)] : message;

    private static async Task<(List<Run> Runs, DateTime? OldestRowAt)> ReadRunsAsync(
        NpgsqlConnection connection, string sql, DateTime startUtc, DateTime endUtc, string? jobKind, string? hypertable, int fetch, CancellationToken cancellationToken)
    {
        var runs = new List<Run>();
        DateTime? oldest = null;
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = McpCommandDeadlines.ReadSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = DateTime.SpecifyKind(startUtc, DateTimeKind.Utc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = DateTime.SpecifyKind(endUtc, DateTimeKind.Utc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)jobKind ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)hypertable ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = fetch });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var message = CutError(reader.IsDBNull(10) ? null : reader.GetString(10));

            if (!reader.IsDBNull(11))
            {
                oldest = DateTime.SpecifyKind(reader.GetDateTime(11), DateTimeKind.Utc);
            }

            runs.Add(new Run(
                reader.GetInt64(0),
                reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                DateTime.SpecifyKind(reader.GetDateTime(5), DateTimeKind.Utc),
                reader.IsDBNull(6) ? null : DateTime.SpecifyKind(reader.GetDateTime(6), DateTimeKind.Utc),
                reader.IsDBNull(7) ? null : reader.GetBoolean(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.GetBoolean(9),
                message));
        }

        return (runs, oldest);
    }

    internal static bool BelowFirstHistoryVersion(string extversion)
    {
        var parts = extversion.Split('.', '-');
        return parts.Length >= 2
            && int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var major)
            && int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var minor)
            && new Version(major, minor) < FirstVersionWithHistory;
    }

    /// <summary>The history-trimming job's config, or null when there is none or it is not the JSON object TimescaleDB writes.</summary>
    internal static Retention? ParseRetention(string? config)
    {
        if (string.IsNullOrWhiteSpace(config))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(config);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            string? dropAfter = root.TryGetProperty("drop_after", out var drop) ? drop.ToString() : null;
            long? Number(string name) => root.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt64(out var n) ? n : null;
            return new Retention(dropAfter, Number("max_successes_per_job"), Number("max_failures_per_job"));
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
