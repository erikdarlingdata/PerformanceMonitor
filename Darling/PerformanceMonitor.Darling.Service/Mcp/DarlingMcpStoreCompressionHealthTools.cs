/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.FrontDoor;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_store_compression_health</c> (#9428): the monitoring store's compression backlog per hypertable. The statement and the
/// ordering are <see cref="DarlingStoreCompressionHealthReader"/> and <see cref="TimescaleSupport.CompressionActivitySql"/>, the
/// one the hourly store tick already counts the backlog with. This wraps them as text for MCP and the web.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreCompressionHealthTools
{
    /// <summary>This tool's entries on the web read surface and in Custom Views validation, kept here and not in the shared lists (#9477).</summary>
    internal sealed class Registration : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            "get_store_compression_health",
            /* #9527: full_detail=true keeps the web read's payload what it was: every hypertable with its whole row. The MCP default lists a
                   healthy hypertable by name only, to stay under the reply budget. */
                (c, pg, an) => GetStoreCompressionHealth(pg, full_detail: true, cancellationToken: c.RequestAborted),
            new(
                DarlingWebEndpoints.CatOverview,
                "Whether the monitoring store compression is keeping up: per hypertable, the chunks past the compression delay and still uncompressed, the oldest of them, the policy delay, the newest compressed chunk, and the policy job last run, last success, next start and last error.",
                Array.Empty<DarlingWebEndpoints.CatalogParam>()));
    }

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_compression_health"), Description(
        "Says whether the monitoring STORE's own compression is keeping up (not a monitored server): per hypertable, how many chunks are "
        + "past the compression delay and still uncompressed, and the policy job's last run. <<GUIDE>> No server_name. Read live from the "
        + "TimescaleDB catalog and job stats, the same statement the hourly store tick counts the backlog with, so it adds no table and "
        + "the service log is no longer the only place the figure lives. A hypertable that is keeping up comes back by NAME ONLY in "
        + "healthy_hypertables, so the answer stays small as the store adds hypertables. Every hypertable that needs a look comes back in "
        + "full in hypertables: one with a backlog (eligible_uncompressed_chunks above 0), a last run that is not Success, a job that is not "
        + "Scheduled or Running, a crash_backoff next_start_state, or a recorded error. A job that has never run and has no backlog counts as "
        + "keeping up. Pass full_detail=true to get every hypertable in full in hypertables, with no healthy_hypertables list. "
        + "summary: hypertables (policies read, both lists together), hypertables_with_backlog, backlog_chunks, healthy_hypertables (how many "
        + "are listed by name). hypertables, the table with the most waiting chunks first, each with: hypertable, eligible_uncompressed_chunks "
        + "(closed chunks older than compress_after and not compressed), oldest_eligible_range_end (the range end of the oldest of them, "
        + "null when none), compress_after_days (the policy's own delay, null when its config has none), newest_compressed_range_end "
        + "(the range end of the newest compressed chunk: the horizon a reader that must not touch compressed chunks, such as an exporter, "
        + "stays after; null when nothing is compressed yet) and job: job_id, status (Running, Scheduled or Paused), last_run_status (Success or "
        + "Failed, from job stats, which every role can read), last_run_started, last_run_seconds (the last completed run), "
        + "last_successful_finish, next_start (null when the job has no start time: see next_start_state), next_start_state (scheduled, "
        + "paused for a parked job, or crash_backoff when the scheduler is holding the job back after a crash; a crash_backoff state "
        + "comes with next_start_note, which says whether this store's TimescaleDB clears it on its own, as 2.26.4 and later do, or keeps "
        + "the job held until it is re-armed) and last_error (sqlstate, message cut to 500 characters, started_at) only when the last run failed and the role can read it. last_error_readable is false when the role "
        + "cannot read job errors: TimescaleDB shows them only to a member of the database owner's role or of the job's owner, so a "
        + "Failed run with last_error null and last_error_readable false means the error exists and this role cannot see it, not that "
        + "there is none. When the error read itself failed, errors_read_failure says so. Times are UTC. When the policy last changed "
        + "is not in TimescaleDB's catalog and is not here. A store without TimescaleDB answers not_collected, and one with no "
        + "compression policy answers empty. Read-only and live.")]
    public static async Task<string> GetStoreCompressionHealth(
        NpgsqlDataSource postgres,
        [Description("Return every hypertable in full. Default lists a hypertable that is keeping up by name only, in healthy_hypertables, and keeps the full row for each one that needs a look.")] bool full_detail = false,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var read = await DarlingStoreCompressionHealthReader.GetAsync(postgres, cancellationToken);
            if (!read.TimescaleInstalled)
            {
                return McpHelpers.Status(
                    "not_collected",
                    "This store has no TimescaleDB extension, so it has no compression policies and no chunks to compress. "
                    + "That is how the store is built, not a read that failed.");
            }

            if (read.Hypertables.Count == 0)
            {
                return McpHelpers.Status(
                    "empty",
                    "TimescaleDB is installed but the store has no compression policy job, so there is no backlog to report.");
            }

            /* #9527: the full row of every hypertable was ~400 B, so the answer grew with each hypertable the store added and
               passed the default reply budget. A hypertable that needs nothing is a name in one list instead. */
            var shown = full_detail ? read.Hypertables : read.Hypertables.Where(h => NeedsALook(h, read)).ToList();
            var healthy = full_detail
                ? new List<string?>()
                : read.Hypertables.Where(h => !NeedsALook(h, read)).Select(h => h.HypertableName).ToList();

            return JsonSerializer.Serialize(new
            {
                summary = new
                {
                    hypertables = read.Hypertables.Count,
                    hypertables_with_backlog = read.HypertablesWithBacklog,
                    backlog_chunks = read.TotalBacklogChunks,
                    healthy_hypertables = healthy.Count,
                },
                errors_read_failure = read.ErrorsReadFailure,
                healthy_hypertables = full_detail ? null : healthy,
                hypertables = shown.Select(h => new
                {
                    hypertable = h.HypertableName,
                    eligible_uncompressed_chunks = h.EligibleUncompressedChunks,
                    oldest_eligible_range_end = Iso(h.OldestEligibleRangeEndUtc),
                    compress_after_days = h.CompressAfter?.TotalDays,
                    newest_compressed_range_end = Iso(h.NewestCompressedRangeEndUtc),
                    job = new
                    {
                        job_id = h.JobId,
                        status = h.JobStatus,
                        last_run_status = h.LastRunStatus,
                        last_run_started = Iso(h.LastRunStartedAtUtc),
                        last_run_seconds = h.LastRunDuration?.TotalSeconds,
                        last_successful_finish = Iso(h.LastSuccessfulFinishUtc),
                        next_start = Iso(h.NextStartUtc),
                        next_start_state = DarlingStoreCompressionHealthReader.NextStartStateFor(h.JobStatus, h.NextStartSentinel),
                        next_start_note = DarlingStoreCompressionHealthReader.NextStartStateFor(h.JobStatus, h.NextStartSentinel) == DarlingStoreCompressionHealthReader.NextStartCrashBackoff
                            ? DarlingStoreCompressionHealthReader.CrashBackoffNote(read.TimescaleVersion)
                            : null,
                        last_error_readable = h.ErrorsVisible,
                        last_error = h.JobId is { } id && read.Errors.TryGetValue(id, out var error)
                            ? new { sqlstate = error.SqlState, message = error.Message, started_at = Iso(error.StartedAtUtc) }
                            : null,
                    },
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_compression_health", ex);
        }
    }

    /// <summary>
    /// #9527: does this hypertable need a look, so the default answer gives its full row? Yes for a backlog, a last run that is
    /// not Success, a job that is not Scheduled or Running, a crash_backoff state, or a recorded error. A job that has not run yet
    /// (no last run status) with no backlog is keeping up. Nothing a reader needs to act on is dropped: a Failed run whose error the
    /// role cannot read is still a last run that is not Success.
    /// </summary>
    internal static bool NeedsALook(CompressionActivity h, DarlingStoreCompressionHealthReader.CompressionHealth read) =>
        h.EligibleUncompressedChunks > 0
        || (h.LastRunStatus is not null && !string.Equals(h.LastRunStatus, "Success", StringComparison.OrdinalIgnoreCase))
        || !(string.Equals(h.JobStatus, "Scheduled", StringComparison.OrdinalIgnoreCase)
            || string.Equals(h.JobStatus, "Running", StringComparison.OrdinalIgnoreCase))
        || DarlingStoreCompressionHealthReader.NextStartStateFor(h.JobStatus, h.NextStartSentinel) == DarlingStoreCompressionHealthReader.NextStartCrashBackoff
        || (h.JobId is { } id && read.Errors.ContainsKey(id));

    /// <summary>Naive UTC with no trailing Z, like every other timestamp on an MCP payload here.</summary>
    private static string? Iso(DateTime? value) =>
        value is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Unspecified).ToString("O", CultureInfo.InvariantCulture) : null;
}
