/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>get_store_job_runs</c> (#9440): each run of a TimescaleDB background job on the monitoring store, read live from TimescaleDB's own
/// history (<see cref="DarlingStoreJobRunsReader"/>). <c>get_store_metrics</c> keeps each job's totals and last run, which cannot line a slow
/// store read up with a compression, retention or refresh run that overlapped it. This can. Darling-only, like the other <c>get_store_*</c>
/// reads: it reads the store, and Lite has no store.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreJobRunsTools
{
    /// <summary>This tool's entries on the web read surface and in Custom Views validation, kept here and not in the shared lists (#9477).</summary>
    internal sealed class Registration : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            "get_store_job_runs",
            (c, pg, an) => GetStoreJobRuns(
                pg,
                DarlingWebEndpoints.Hours(c, 24),
                DarlingWebEndpoints.Str(c, "job_kind"),
                DarlingWebEndpoints.Str(c, "hypertable"),
                DarlingWebEndpoints.Rows(c, "limit", DefaultLimit),
                DarlingWebEndpoints.AsOf(c),
                responseByteBudget: null,
                c.RequestAborted),
            new(
                DarlingWebEndpoints.CatOverview,
                "Each run of the monitoring store's own TimescaleDB background jobs (compression, retention, refresh), newest first, with start, end, outcome and error, read live from TimescaleDB's own job history.",
                new[]
                {
                    DarlingWebEndpoints.PHours(24),
                    DarlingWebEndpoints.PText("job_kind"),
                    DarlingWebEndpoints.PText("hypertable"),
                    DarlingWebEndpoints.PLimit(DefaultLimit),
                    DarlingWebEndpoints.PAsOf(),
                }));
    }

    /// <summary>Default runs returned. A busy store runs its jobs many times an hour, so this is a recent slice and the filters reach the rest.</summary>
    public const int DefaultLimit = 50;

    /// <summary>The longest window the read accepts. TimescaleDB's own trim job keeps a month by default, so a longer window can only
    /// be asking for rows that are gone.</summary>
    public const int MaxHours = 744;

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_job_runs"), Description(
        "Each run of the monitoring STORE's own TimescaleDB background jobs (compression, retention, refresh), newest first, with start, end, "
        + "outcome and error. Not a monitored server; no server_name. Filter with job_kind and hypertable. <<GUIDE>> "
        + "Read live from timescaledb_information.job_history (TimescaleDB 2.15 and later), so it adds no table. Each run: job_id, procedure, "
        + "hypertable (from the job, null for a job with none or one since deleted), started_at, finished_at, duration_seconds, outcome "
        + "(succeeded, failed, or unfinished when the run has no end yet, either still running or its process died), sqlstate and error "
        + "(cut to 500 characters). job_kind is the procedure with or without its policy_ prefix, for example compression or "
        + "policy_retention. A failed run is always recorded, but a successful one only while timescaledb.enable_job_execution_logging is on: "
        + "history.job_execution_logging gives the setting, and with it off a window of only failures is not a window where nothing else ran. "
        + "history says how far back TimescaleDB's own history reaches: oldest_row_at is the oldest row the read can see, "
        + "window_starts_before_oldest_row is true when part of the window is older than that, and retention is the trim job's own drop_after "
        + "and per-job caps. An empty older window is therefore not proof of no runs. TimescaleDB shows a job's runs only to a member of the "
        + "database owner's role or of the job's owner, and the store's mcp and viewer roles are neither, so the read goes through the store's "
        + "collect.store_job_runs function, which every read role may call: every run comes back, and error_readable is false, with the error "
        + "withheld, on a run whose error text TimescaleDB would not show this login (not the job's owner and not in the database owner's role). access is null on this path, because every job is readable and error_readable says which runs carry their error text. "
        + "On a store that has no such function yet (an older schema, or a login without EXECUTE on it) the read falls back to the view itself: "
        + "access then gives jobs_matching, jobs_readable and jobs_hidden for the filters, and when a job's runs are hidden the answer is "
        + "precondition, never empty. A hidden run is not a short row, it is no row. limit cuts the page "
        + "(truncated) and the 32,768-byte response budget can cut it further (budget_truncated), newest rows kept. Times are UTC. "
        + "A store without TimescaleDB, or on one older than 2.15, answers not_collected with the reason. Read-only and live.")]
    public static Task<string> GetStoreJobRuns(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 744.")] int hours_back = 24,
        [Description("Only this job kind: the procedure name, with or without policy_, for example compression, retention, refresh_continuous_aggregate. Omit for every job.")] string? job_kind = null,
        [Description("Only jobs of this hypertable or continuous aggregate, by its exact name. Omit for every job.")] string? hypertable = null,
        [Description("Maximum runs to return, newest first. Default 50. This is what bounds the page: read truncated to know whether the window held more.")] int limit = DefaultLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetStoreJobRuns(postgres, hours_back, job_kind, hypertable, limit, as_of, McpResponseBudget.DefaultBytes, cancellationToken);

    /// <summary>get_store_job_runs under an explicit <paramref name="responseByteBudget"/>: the MCP tool passes
    /// <see cref="McpResponseBudget.DefaultBytes"/>, and the web viewer's <c>/api/read</c> mirror passes null and keeps every row.</summary>
    internal static async Task<string> GetStoreJobRuns(
        NpgsqlDataSource postgres, int hours_back, string? job_kind, string? hypertable, int limit, string? as_of, int? responseByteBudget,
        CancellationToken cancellationToken)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, MaxHours, out var windowEnd);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        var kindFilter = string.IsNullOrWhiteSpace(job_kind) ? null : job_kind.Trim();
        var hypertableFilter = string.IsNullOrWhiteSpace(hypertable) ? null : hypertable.Trim();

        try
        {
            var windowStart = windowEnd.AddHours(-hours_back);
            var read = await DarlingStoreJobRunsReader.ReadAsync(
                postgres, windowStart, windowEnd, kindFilter, hypertableFilter, limit + 1, cancellationToken);
            if (read.Unavailable is not null)
            {
                return McpHelpers.Status("not_collected", read.Unavailable);
            }

            var (page, truncated) = McpHelpers.BoundPage(read.Fetched, limit);
            var access = read.Access;
            var horizon = read.Horizon;
            var beforeOldest = horizon.OldestRowAt is { } oldest && windowStart < oldest;

            if (page.Count == 0)
            {
                var filters = (kindFilter, hypertableFilter) switch
                {
                    (not null, not null) => $" for job kind '{kindFilter}' and hypertable '{hypertableFilter}'",
                    (not null, null) => $" for job kind '{kindFilter}'",
                    (null, not null) => $" for hypertable '{hypertableFilter}'",
                    _ => string.Empty,
                };
                var span = $"{windowStart.ToString("u", CultureInfo.InvariantCulture)} to {windowEnd.ToString("u", CultureInfo.InvariantCulture)}";

                if (access is { JobsHidden: > 0 })
                {
                    return McpHelpers.Status(
                        "precondition",
                        $"No run is shown for {span}{filters}, and that is not proof that none ran. TimescaleDB shows a job's runs only to a member of the database owner's role or of the job's owner, "
                        + $"and this connection's role is neither for {access.JobsHidden} of the {access.JobsMatching} matching jobs. get_store_metrics gives each job's totals and last run to every role.");
                }

                return McpHelpers.Status(
                    "empty",
                    $"TimescaleDB recorded no job run that started {span}{filters}. " + HorizonSentence(horizon, beforeOldest) + LoggingSentence(read.LoggingSetting));
            }

            string Reply(int kept) => JsonSerializer.Serialize(new
            {
                window_hours = hours_back,
                as_of = Iso(windowEnd),
                job_kind = kindFilter,
                hypertable = hypertableFilter,
                runs_returned = kept,
                truncated,
                budget_truncated = kept < page.Count,
                budget_note = kept < page.Count
                    ? McpResponseBudget.PageCutNote(page.Count, kept, "newest first", "runs",
                        "Filter with job_kind or hypertable, or narrow the window with hours_back or as_of, to read the rest.")
                    : null,
                access = access is null
                    ? null
                    : new
                    {
                        jobs_matching = access.JobsMatching,
                        jobs_readable = access.JobsReadable,
                        jobs_hidden = access.JobsHidden,
                        note = access.JobsHidden > 0
                            ? $"This role cannot read the runs of {access.JobsHidden} of the {access.JobsMatching} matching jobs, so those runs are missing from this page, not absent from the store."
                            : null,
                    },
                history = new
                {
                    oldest_row_at = Iso(horizon.OldestRowAt),
                    window_starts_before_oldest_row = beforeOldest,
                    retention = horizon.Retention is { } r
                        ? new { drop_after = r.DropAfter, max_successes_per_job = r.MaxSuccessesPerJob, max_failures_per_job = r.MaxFailuresPerJob }
                        : null,
                    job_execution_logging = read.LoggingSetting,
                    note = LoggingSentence(read.LoggingSetting).Trim() is { Length: > 0 } logging ? logging : null,
                },
                runs = page.Take(kept).Select(run => new
                {
                    job_id = run.JobId,
                    procedure = run.ProcName,
                    hypertable = run.HypertableName,
                    started_at = Iso(run.StartedAt),
                    finished_at = Iso(run.FinishedAt),
                    duration_seconds = run.FinishedAt is { } end
                        ? Math.Round(Math.Max(0, (end - run.StartedAt).TotalSeconds), 3)
                        : (double?)null,
                    outcome = Outcome(run),
                    sqlstate = run.SqlState,
                    error_readable = run.ErrorReadable,
                    error = run.ErrorMessage,
                }).ToList(),
            }, McpHelpers.JsonOptions);

            return responseByteBudget is int budget
                ? McpResponseBudget.FitPrefix(page.Count, Reply, budget, minimum: 1).Json
                : Reply(page.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_job_runs", ex);
        }
    }

    internal static string Outcome(DarlingStoreJobRunsReader.Run run) =>
        run.FinishedAt is null ? "unfinished" : run.Succeeded == true ? "succeeded" : "failed";

    private static string HorizonSentence(DarlingStoreJobRunsReader.Horizon horizon, bool windowStartsBeforeOldest)
    {
        var sentence = horizon.OldestRowAt is { } oldest
            ? $"The oldest history row this read can see started {oldest.ToString("u", CultureInfo.InvariantCulture)}, "
              + (windowStartsBeforeOldest
                  ? "so part of this window is older than anything TimescaleDB still holds."
                  : "so the whole window is inside what TimescaleDB still holds.")
            : "TimescaleDB holds no history rows this read can see at all.";
        if (horizon.Retention is { } r)
        {
            sentence += $" Its trim job keeps rows for {r.DropAfter ?? "an unstated time"}"
                + (r.MaxSuccessesPerJob is { } s ? $", at most {s.ToString(CultureInfo.InvariantCulture)} successes" : string.Empty)
                + (r.MaxFailuresPerJob is { } f ? $" and {f.ToString(CultureInfo.InvariantCulture)} failures per job" : string.Empty)
                + ".";
        }

        return sentence;
    }

    private static string LoggingSentence(string? setting) =>
        string.Equals(setting, "off", StringComparison.OrdinalIgnoreCase)
            ? " timescaledb.enable_job_execution_logging is off, so only failed runs are recorded and a successful run leaves no row."
            : string.Empty;

    /// <summary>Naive UTC with no trailing Z, like every other timestamp on an MCP payload here.</summary>
    private static string? Iso(DateTime? value) =>
        value is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Unspecified).ToString("O", CultureInfo.InvariantCulture) : null;
}
