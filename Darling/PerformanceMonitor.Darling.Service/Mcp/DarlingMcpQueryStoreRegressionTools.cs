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
using System.Threading;
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
    /// <summary>
    /// #4198: query_text is this tool's own wide field - up to 50 rows of numeric columns plus a full,
    /// unbounded Query Store text each measured 211 KB at default arguments on a busy production store, the
    /// worst of every #4198 offender (<see cref="McpResponseBudget"/>'s own doc comment). Previewed to this
    /// length per row at default (<c>full_text: true</c> opts back in), the same preview-plus-opt-in shape
    /// <c>get_store_query_stats</c> uses for its own <c>full_text</c>.
    /// </summary>
    private const int QueryTextPreviewLength = 240;

    [McpServerTool(Name = "get_query_store_regressions"), Description("Finds queries whose Query Store performance got WORSE: recent window (hours_back, ending at as_of) vs a fixed 7-day baseline before it (see baseline_start/baseline_end). get_query_store_top ranks EXPENSIVE, this ranks CHANGED. Gated: average CPU regressed over 25%. duration_regression_percent, io_regression_percent and severity are null, not 0%, when their baseline is 0. additional_duration_ms is the ranking key. empty: no regression, or nothing yet in the baseline window. unavailable: no baseline exists yet. not_collected: this server's engine cannot run Query Store. <<GUIDE>> Finds queries whose Query Store performance got WORSE, by comparing each (database, query_id) group's averages inside a recent window against its baseline - a FIXED 7-day lookback ending at that window's start (#4195; before this it was every capture EVER collected before the window, so its cost tracked how much history the store still retained rather than the window asked for, and the comparison period silently grew on a server with more retention). baseline_start and baseline_end report exactly which period was compared - a regression against something older than the baseline lookback is not caught; a store retaining less than that is unaffected. Returns baseline vs recent duration, CPU and logical reads with the regression percent for each, the execution-count-weighted extra duration (the ranking key: a 5 ms regression executed a million times outranks a 5-second one executed twice), the plan counts on both sides, and a duration-driven severity band. get_query_store_top answers what is EXPENSIVE; the most expensive query is usually the one that always was. This answers what CHANGED. Rows are kept only where average CPU regressed by more than 25%. A regression percent whose BASELINE side is 0 has no denominator and is returned as null, with the reason under undefined_percents - never as 0, which would read as no change when the truth is the largest possible one; compare the two absolute figures instead. The ranking key is the absolute, execution-weighted duration delta, which exists whether or not a ratio does, so a null percent never sorts as 0. severity is banded from the duration percent and is null when that percent is.")]
    public static async Task<string> GetQueryStoreRegressions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Size of the RECENT window, in hours back from now. Everything collected before it is the baseline. Default 24.")] int hours_back = 24,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum rows to return, worst first. Default 30, sized to keep a default call under the shared response budget. Read truncated to know whether the window held more.")] int limit = 30,
        [Description("Return each row's full query text instead of a 240-character preview. Default false.")] bool full_text = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            /* #4195: the baseline's own fixed lookback, ending at the recent window's start - no longer
               "every retained row before it", whose cost tracked retention rather than the window asked for. */
            var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);

            /*
                Over-fetch by one. Comparing the row count to the cap reports truncation for a server that
                happens to have exactly `limit` regressions and nothing more, which is a false positive in
                the one field whose whole reason for existing is that the cap should not have to be inferred.
            */
            var rows = await DarlingQueryStoreRegressionReader.GetQueryStoreRegressionsAsync(
                postgres, resolved.ServerId, start, end, database_name, limit + 1, baselineStart, cancellationToken);

            if (rows.Count == 0)
                return await EmptyAsync(postgres, resolved.ServerName, resolved.ServerId, start, end, baselineStart, hours_back, cancellationToken);

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
                /* #4195: a fixed lookback ending at recent_window_start, not "every capture ever collected
                   before it" - the baseline no longer grows with retention. */
                baseline_start = baselineStart.ToString("o"),
                baseline_end = start.ToString("o"),
                baseline_is = $"Query Store captures from baseline_start to recent_window_start ({DarlingQueryStoreRegressionReader.BaselineLookbackDays} days)",
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
                    baseline_duration_ms = Round(r.BaselineDurationMs),
                    recent_duration_ms = Round(r.RecentDurationMs),
                    duration_regression_percent = Round(r.DurationRegressionPercent),
                    baseline_cpu_ms = Round(r.BaselineCpuMs),
                    recent_cpu_ms = Round(r.RecentCpuMs),
                    cpu_regression_percent = Round(r.CpuRegressionPercent),
                    baseline_reads = Round(r.BaselineReads),
                    recent_reads = Round(r.RecentReads),
                    io_regression_percent = Round(r.IoRegressionPercent),
                    /* Null percents, and why (#3541 A12): a 0 baseline has no ratio, and the reader used to
                       publish that as 0 — "no change" — for the row that changed the most. */
                    undefined_percents = UndefinedPercentNotes(r),
                    /* The ranking key, and the one number that says whether this regression MATTERS: a
                       5 ms regression executed a million times outranks a 5-second one executed twice. It is
                       an absolute delta, so it exists for every row and a null ratio never sorts as 0. */
                    additional_duration_ms = Round(r.AdditionalDurationMs),
                    baseline_exec_count = r.BaselineExecCount,
                    recent_exec_count = r.RecentExecCount,
                    /* A plan count that moved between the two sides is the first thing to check: a query
                       that regressed while gaining a plan is usually a plan-choice problem, not a data one. */
                    baseline_plan_count = r.BaselinePlanCount,
                    recent_plan_count = r.RecentPlanCount,
                    last_execution_time = r.LastExecutionTime?.ToString("o"),
                    query_text = full_text ? r.QueryTextSample : McpHelpers.Truncate(r.QueryTextSample, QueryTextPreviewLength),
                    query_text_truncated = !full_text && r.QueryTextSample.Length > QueryTextPreviewLength,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_query_store_regressions", ex);
        }
    }


    /// <summary>
    /// #4198: the reader's ms/percent/read figures come straight off <c>AVG()</c> over microsecond
    /// integers, so most rows serialize with a long, meaningless decimal tail (a double's round-trip
    /// representation, not a display one) - real weight across ten numeric fields on up to 30 rows. Rounded
    /// to 2 decimal places here, at the JSON edge only; the reader's SQL and the viewer's own read are
    /// untouched, and every existing pin uses whole-number fixtures that round trip unchanged.
    /// </summary>
    private static double Round(double value) => Math.Round(value, 2);

    private static double? Round(double? value) => value is null ? null : Math.Round(value.Value, 2);

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
        NpgsqlDataSource postgres, string serverName, int serverId, DateTime start, DateTime end, DateTime baselineStart, int hours_back,
        CancellationToken cancellationToken)
    {
        var (hasBaseline, hasRecent) = await DarlingQueryStoreRegressionReader.GetCoverageAsync(
            postgres, serverId, start, end, baselineStart, cancellationToken);

        if (!hasBaseline && !hasRecent)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, serverId, serverName, "query_store", cancellationToken)
                ?? McpHelpers.Status(
                    "unavailable",
                    $"No Query Store data has EVER been collected for {serverName}, so this is NOT a report of zero regressions — there is nothing to compare. Query Store may be OFF on this server's databases, which get_query_store_health will say; otherwise check that collection is running for this server.");
        }

        if (!hasBaseline)
        {
            return McpHelpers.Status(
                "unavailable",
                $"{serverName} has no Query Store capture in the {DarlingQueryStoreRegressionReader.BaselineLookbackDays}-day baseline window before this window, so there is no baseline to compare against and no regression can be detected however badly one regressed. This is NOT a clean bill of health. Either this server's whole collected history falls inside the last {hours_back} hour(s), or it has none older than the baseline lookback yet.");
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
