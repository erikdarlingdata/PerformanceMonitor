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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Service.FrontDoor;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The read over the record of the service's background pass steps (#9411): what <c>collect.store_pass_steps</c> holds, one row per step
/// of the hourly store-maintenance tick, the 15-minute Query Store compose rollup and the hourly store self-metrics tick. It answers
/// "what was the service doing at 02:10", which until now took a search of the service host's log.
///
/// <para>A tool of its own beside <c>get_store_log</c>: that read is what the store's PostgreSQL complained about, this one is what
/// the service's own passes did, and the two are lined up by time.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStorePassStepsTools
{
    /// <summary>Default rows returned. A pass writes about twenty rows an hour, so this is a few hours of the newest steps, and the
    /// filters or a longer limit reach the rest.</summary>
    public const int DefaultLimit = 50;

    private const string ExistsSql = "SELECT to_regclass('collect.store_pass_steps') IS NOT NULL";

    [FrontDoorClass(FrontDoorToolClass.FanOut)]
    [DarlingToolAccess(ToolAccess.Read)]
    [McpServerTool(Name = "get_store_pass_steps"), Description(
        "What the monitoring STORE's own background passes did, step by step, newest first: when each step of the hourly "
        + "store-maintenance, 15-minute Query Store compose and hourly store self-metrics passes started, how long it took, "
        + "whether it worked, and the counts it reported. Not a monitored server; no server_name. Filter with pass and step. "
        + "empty means no step started in the window. <<GUIDE>> "
        + "Reads collect.store_pass_steps, one row per step, written after the step finishes. pass is store_maintenance (hourly), "
        + "query_store_compose (every 15 minutes) or store_self_metrics (hourly). outcome is ok (finished), failed (threw, or finished "
        + "with builds it could not complete, with the reason in detail) or skipped (did not run, with the reason in detail). "
        + "A step's duration is finished_at minus started_at, given as duration_seconds. built is what the step made and removed is what "
        + "it deleted, in whatever unit that step's service log line counts (rows, hours, days or snapshots); null means the step counts "
        + "nothing of that kind. A missing row is not proof a step did not run: the service never holds or fails a step for its own "
        + "record, so a record it could not write leaves no row. Rows are kept 90 days. rows_total is how many rows matched, and "
        + "rows_returned is how many this answer holds: limit cuts the page and the 32,768-byte response budget can cut it further "
        + "(budget_truncated), newest rows kept. Times are UTC. A store that has not yet been migrated to the table answers precondition.")]
    public static Task<string> GetStorePassSteps(
        NpgsqlDataSource postgres,
        [Description("Hours of history. Default 24; max 168.")] int hours_back = 24,
        [Description("Only this pass: store_maintenance, query_store_compose or store_self_metrics. Omit for every pass.")] string? pass = null,
        [Description("Only this step, by the name the rows carry (for example module_map_refresh). Omit for every step.")] string? step = null,
        [Description("Maximum rows to return. Default 50.")] int limit = DefaultLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default) =>
        GetStorePassSteps(postgres, hours_back, pass, step, limit, as_of, McpResponseBudget.DefaultBytes, cancellationToken);

    /// <summary>get_store_pass_steps under an explicit <paramref name="responseByteBudget"/>: the MCP tool passes
    /// <see cref="McpResponseBudget.DefaultBytes"/>, and the web viewer's <c>/api/read</c> mirror passes null and keeps every row.</summary>
    internal static async Task<string> GetStorePassSteps(
        NpgsqlDataSource postgres, int hours_back, string? pass, string? step, int limit, string? as_of, int? responseByteBudget,
        CancellationToken cancellationToken)
    {
        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd);
        if (validation != null)
        {
            return validation;
        }

        validation = McpHelpers.ValidateTop(limit);
        if (validation != null)
        {
            return validation;
        }

        var passFilter = string.IsNullOrWhiteSpace(pass) ? null : pass.Trim();
        var stepFilter = string.IsNullOrWhiteSpace(step) ? null : step.Trim();

        try
        {
            await using (var exists = postgres.CreateCommand(ExistsSql))
            {
                exists.CommandTimeout = McpCommandDeadlines.ReadSeconds;
                if (!(bool)(await exists.ExecuteScalarAsync(cancellationToken))!)
                {
                    return McpHelpers.Status(
                        "precondition",
                        "This store has no pass step record yet: it predates the table. The service migrates the store at its next start, and the first rows land when a pass next runs after that.");
                }
            }

            var windowStart = windowEnd.AddHours(-hours_back);
            var page = await StorePassSteps.ReadAsync(postgres, windowStart, windowEnd, passFilter, stepFilter, limit, cancellationToken, McpCommandDeadlines.ReadSeconds);
            if (page.Rows.Count == 0)
            {
                var filters = (passFilter, stepFilter) switch
                {
                    (not null, not null) => $" for pass '{passFilter}' and step '{stepFilter}'",
                    (not null, null) => $" for pass '{passFilter}'",
                    (null, not null) => $" for step '{stepFilter}'",
                    _ => string.Empty,
                };
                return McpHelpers.Status(
                    "empty",
                    $"No pass step started {McpHelpers.InTheLast(CountText.Plural(hours_back, "hour"))}{filters}. Passes are store_maintenance, query_store_compose and store_self_metrics. "
                    + "A step the service could not record leaves no row, and a store migrated within the window has rows only from the migration on.");
            }

            var rows = page.Rows;
            string Reply(int kept) => JsonSerializer.Serialize(new
            {
                window_hours = hours_back,
                as_of = windowEnd.ToString("o", CultureInfo.InvariantCulture),
                pass = passFilter,
                step = stepFilter,
                rows_total = page.Total,
                rows_returned = kept,
                budget_truncated = kept < rows.Count,
                budget_note = kept < rows.Count
                    ? McpResponseBudget.PageCutNote(rows.Count, kept, "newest first", "steps",
                        "Filter with pass or step, or narrow the window with hours_back or as_of, to read the rest.")
                    : null,
                steps = rows.Take(kept).Select(r => new
                {
                    pass = r.Pass,
                    step = r.Step,
                    started_at = r.StartedAt.ToString("o", CultureInfo.InvariantCulture),
                    finished_at = r.FinishedAt.ToString("o", CultureInfo.InvariantCulture),
                    duration_seconds = Math.Round(Math.Max(0, (r.FinishedAt - r.StartedAt).TotalSeconds), 3),
                    outcome = r.Outcome,
                    built = r.Built,
                    removed = r.Removed,
                    detail = r.Detail,
                }).ToList(),
            }, McpHelpers.JsonOptions);

            return responseByteBudget is int budget
                ? McpResponseBudget.FitPrefix(rows.Count, Reply, budget, minimum: 1).Json
                : Reply(rows.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_store_pass_steps", ex);
        }
    }
}
