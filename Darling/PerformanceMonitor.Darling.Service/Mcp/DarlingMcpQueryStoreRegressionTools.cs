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
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// get_query_store_regressions (#2484) - the viewer's Queries &gt; Query Store Regressions tab, which was the
/// only tab in the per-server page entirely unreachable from a browser or an agent rather than merely
/// reduced.
///
/// <para>Every other Query Store read answers "what is expensive". This answers "what got WORSE", which is
/// a different question and not derivable from the first: the most expensive query on a server is usually
/// the one that has always been the most expensive, and the one that changed last Tuesday can sit well
/// down the list. A STORED read over <see cref="DarlingQueryStoreRegressionReader"/>, no live
/// monitored-server hit.</para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpQueryStoreRegressionTools
{
    [McpServerTool(Name = "get_query_store_regressions"), Description("Finds queries whose Query Store performance got WORSE, by comparing each (database, query_id) group's averages inside a recent window against its baseline - every capture BEFORE that window. Returns baseline vs recent duration, CPU and logical reads with the regression percent for each, the execution-count-weighted extra duration (the ranking key: a 5 ms regression executed a million times outranks a 5-second one executed twice), the plan counts on both sides, and a duration-driven severity band. get_query_store_top answers what is EXPENSIVE; the most expensive query is usually the one that always was. This answers what CHANGED. Rows are kept only where average CPU regressed by more than 25%. A regression percent whose BASELINE side is 0 has no denominator and is returned as null, with the reason under undefined_percents - never as 0, which would read as no change when the truth is the largest possible one; compare the two absolute figures instead. The ranking key is the absolute, execution-weighted duration delta, which exists whether or not a ratio does, so a null percent never sorts as 0. severity is banded from the duration percent and is null when that percent is.")]
    public static async Task<string> GetQueryStoreRegressions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Size of the RECENT window, in hours back from now. Everything collected before it is the baseline. Default 24.")] int hours_back = 24,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum rows to return, worst first. Default 50 (the number the desktop viewer shows).")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);

            /*
                Over-fetch by one. Comparing the row count to the cap reports truncation for a server that
                happens to have exactly `limit` regressions and nothing more, which is a false positive in
                the one field whose whole reason for existing is that the cap should not have to be inferred.
            */
            var rows = await DarlingQueryStoreRegressionReader.GetQueryStoreRegressionsAsync(
                postgres, resolved.ServerId, start, end, database_name, limit + 1);

            if (rows.Count == 0)
                return await EmptyAsync(postgres, resolved.ServerName, resolved.ServerId, start, end, hours_back);

            var truncated = rows.Count > limit;
            var shown = rows.Take(limit);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                database_name,
                /*
                    Named so the caller cannot mistake which side is which. "baseline" is NOT a fixed
                    lookback: it is everything collected before the window, so a longer hours_back makes
                    the recent window bigger AND the baseline shorter.
                */
                recent_window_start = start.ToString("o"),
                recent_window_end = end.ToString("o"),
                baseline_is = "every Query Store capture collected BEFORE recent_window_start",
                gate = "average CPU regressed by more than 25%",
                regression_count = Math.Min(rows.Count, limit),
                truncated,
                regressions = shown.Select(r => new
                {
                    database_name = r.DatabaseName,
                    query_id = r.QueryId,
                    /* Banded from the duration percent by the TVF's CASE, whose ELSE is 'LOW' — which for a
                       row with NO duration ratio is a verdict about a number that does not exist. Null there
                       (#3541 A12); the SQL's band is kept verbatim for the viewer it is shared with. */
                    severity = r.DurationRegressionPercent is null ? null : r.Severity,
                    baseline_duration_ms = r.BaselineDurationMs,
                    recent_duration_ms = r.RecentDurationMs,
                    duration_regression_percent = r.DurationRegressionPercent,
                    baseline_cpu_ms = r.BaselineCpuMs,
                    recent_cpu_ms = r.RecentCpuMs,
                    cpu_regression_percent = r.CpuRegressionPercent,
                    baseline_reads = r.BaselineReads,
                    recent_reads = r.RecentReads,
                    io_regression_percent = r.IoRegressionPercent,
                    /* Null percents, and why (#3541 A12): a 0 baseline has no ratio, and the reader used to
                       publish that as 0 — "no change" — for the row that changed the most. */
                    undefined_percents = UndefinedPercentNotes(r),
                    /* The ranking key, and the one number that says whether this regression MATTERS: a
                       5 ms regression executed a million times outranks a 5-second one executed twice. It is
                       an absolute delta, so it exists for every row and a null ratio never sorts as 0. */
                    additional_duration_ms = r.AdditionalDurationMs,
                    baseline_exec_count = r.BaselineExecCount,
                    recent_exec_count = r.RecentExecCount,
                    /* A plan count that moved between the two sides is the first thing to check: a query
                       that regressed while gaining a plan is usually a plan-choice problem, not a data one. */
                    baseline_plan_count = r.BaselinePlanCount,
                    recent_plan_count = r.RecentPlanCount,
                    last_execution_time = r.LastExecutionTime?.ToString("o"),
                    query_text = r.QueryTextSample,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_regressions", ex);
        }
    }


    /// <summary>
    /// Which of a row's three regression percents are undefined, and why (#3541 A12, contract rule 5). Each
    /// percent divides through <c>NULLIF(baseline, 0)</c>, so a NULL means the baseline side was 0 — there is
    /// no denominator, not no change — and the caller is pointed at the absolute pair it can still compare.
    /// Null when every percent is defined, so the common row carries no noise. Lite's twin builds the same
    /// sentences.
    /// </summary>
    private static List<string>? UndefinedPercentNotes(DarlingQueryStoreRegressionReader.RegressionRow r)
    {
        List<string>? notes = null;
        void Note(string field, string baseline, string recent)
            => (notes ??= new List<string>()).Add(
                $"{field} is null: no_baseline — {baseline} is 0, so the ratio has no denominator; this is NOT 0% change. Compare {baseline} to {recent} directly.");

        if (r.DurationRegressionPercent is null) Note("duration_regression_percent", "baseline_duration_ms", "recent_duration_ms");
        if (r.CpuRegressionPercent is null) Note("cpu_regression_percent", "baseline_cpu_ms", "recent_cpu_ms");
        if (r.IoRegressionPercent is null) Note("io_regression_percent", "baseline_reads", "recent_reads");
        return notes;
    }

    /// <summary>
    /// What zero regressions actually means, which is four different things.
    /// <para>Only ONE of them is good news, and the other three all look identical to it in a bare empty
    /// array. The dangerous one is a server whose entire collected history sits INSIDE the requested
    /// window: it has no BEFORE, so it can never show a regression however badly it regressed, and
    /// answering "no regressions" there is a confident wrong answer rather than a missing one. One probe,
    /// two booleans, run only on this path.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, string serverName, int serverId, DateTime start, DateTime end, int hours_back)
    {
        var (hasBaseline, hasRecent) = await DarlingQueryStoreRegressionReader.GetCoverageAsync(
            postgres, serverId, start, end);

        if (!hasBaseline && !hasRecent)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, serverId, serverName, "query_store")
                ?? McpHelpers.Status(
                    "unavailable",
                    $"No Query Store data has EVER been collected for {serverName}, so this is NOT a report of zero regressions — there is nothing to compare. Query Store may be OFF on this server's databases, which get_query_store_health will say; otherwise check that collection is running for this server.");
        }

        if (!hasBaseline)
        {
            return McpHelpers.Status(
                "unavailable",
                $"Every Query Store capture for {serverName} falls INSIDE the last {hours_back} hour(s), so there is no baseline to compare against and no regression can be detected however badly one regressed. This is NOT a clean bill of health. Shorten hours_back so more of the collected history falls before the window, or wait until this server has history older than it.");
        }

        if (!hasRecent)
        {
            return McpHelpers.Status(
                "empty",
                $"{serverName} has Query Store history from before this window but nothing collected IN the last {hours_back} hour(s), so there is a recent side missing rather than nothing to report. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.");
        }

        return McpHelpers.Status(
            "empty",
            $"No query on {serverName} regressed in the last {hours_back} hour(s). Both a baseline and this window were collected and no query's average CPU is more than 25% worse than its baseline — this IS the all-clear for this read.");
    }
}
