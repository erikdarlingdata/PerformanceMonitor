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
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

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

    [McpServerTool(Name = "get_query_store_regressions"), Description("Finds queries whose Query Store performance got WORSE: recent window (hours_back) vs a fixed 7-day baseline before it. get_query_store_top ranks EXPENSIVE, this ranks CHANGED. Gated: average CPU regressed over 25%. Timeouts or errors that rose also qualify (regression_reason). duration_regression_percent and severity are null, not 0%, when their baseline is 0. additional_duration_ms is the ranking key. empty: no regression, or nothing yet in the baseline window. unavailable: no baseline exists yet. not_collected: this server's engine cannot run Query Store. <<GUIDE>> Finds queries whose Query Store performance got WORSE, by comparing each (database, query_id) group's per-execution cost inside a recent window against its baseline - a FIXED 7-day lookback ending at that window's start (#4195; before this it was every capture EVER collected before the window, so its cost tracked how much history the store still retained rather than the window asked for, and the comparison period silently grew on a server with more retention). baseline_start and baseline_end name the period the baseline asks for. cost_baseline_start and failure_baseline_start say where the cost baseline and the timeouts and errors baseline really start, effective_start is the later of the two, and truncation_note says which table served each, so read them to know what was compared. A store that starts inside the baseline holds only part of it, and one that starts at or after baseline_end has a baseline with no hours, so timeouts and errors are flagged on recent runs alone, and cost only against an interval that ran before baseline_end and was collected after it. A regression against something older than the baseline lookback is not caught. Returns baseline vs recent duration and CPU with the regression percent for each (logical reads are not reported, #5506), and beside them the Aborted (timeouts, cancels) and Exception (errors) runs of the same query: for each group the run count, the CPU and duration per execution, and the count per hour, for the baseline and the recent window (the two differ in length, so compare the per-hour figures), the execution-count-weighted extra duration (the ranking key: a 5 ms regression executed a million times outranks a 5-second one executed twice), the plan counts on both sides, and a duration-driven severity band. get_query_store_top answers what is EXPENSIVE; the most expensive query is usually the one that always was. This answers what CHANGED. Rows are kept where Regular average CPU regressed by more than 25% and the query added at least 1 s of CPU over the recent window (its recent runs times the rise in its CPU per run), so a few runs of a cheap query are not listed while one run of a job that got much more expensive is (regression_reason cost), or where the Aborted rate per hour (timeouts) or the Exception rate per hour (errors) in the recent window is at least twice the baseline's, or nonzero where the baseline had none, with at least 3 such runs in the recent window - so a query whose timeouts rose is listed even if its Regular CPU fell, and one 30 s timeout among a thousand fast runs, which barely moves an average, is not hidden. regression_reason is cost, timeouts, errors or a comma-separated combination. Failure-only rows are listed after the cost rows and take at most a third of the limit (more only if cost rows leave room), so a flood of errors cannot push the cost rows out; their severity is HIGH when the rate is at least five times the baseline's (or the baseline had none) and MEDIUM otherwise, never CRITICAL. Cost compares Regular runs only: the daily totals hold no other kind, and blending Aborted runs in would hide them. The Aborted and Exception figures are read live from the per-interval table (or the raw tier), not from the daily totals. A regression percent whose BASELINE side is 0 has no denominator and is returned as null, with the reason under undefined_percents - never as 0, which would read as no change when the truth is the largest possible one; compare the two absolute figures instead. The ranking key is the absolute, execution-weighted duration delta, which exists whether or not a ratio does, so a null percent never sorts as 0. severity is banded from the duration percent and is null when that percent is. Both sides are WEIGHTED per execution (the summed cost divided by the summed executions, so a busy interval counts for what it ran; before #5506 the baseline was an average of the interval averages, which is not the true cost per execution), over Regular executions only, and the baseline's closed days come from the per-day totals the analysis engine keeps (collect.plan_regression_daily), with the days at its edges read live, so a call no longer re-reads a week of Query Store rows. Reads the per-interval tables when they hold the window, and the raw tier otherwise; history_source says which served. Past the raw tier's retention the cost baseline is read from query_store_interval_latest (kept 15 days, so a full baseline at hours_back 168) and the timeouts and errors baseline from query_store_interval_wide (kept 10 days, so a shorter one), and each per-hour rate divides by the hours from its own start.")]
    public static Task<string> GetQueryStoreRegressions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Size of the RECENT window, in hours back from now. Everything collected before it is the baseline. Default 24.")] int hours_back = 24,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum rows to return, worst first. Default 30, sized to keep a default call under the shared response budget. Read truncated to know whether the window held more.")] int limit = 30,
        [Description("Return each row's full query text instead of a 240-character preview. Default false.")] bool full_text = false,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
        => GetQueryStoreRegressions(
            postgres, server_name, hours_back, DatabaseFilter.One(database_name), limit, full_text, as_of, logger, cancellationToken);

    /// <summary>
    /// #5245: the get_query_store_regressions read over a LIST of databases. The MCP tool passes
    /// <see cref="DatabaseFilter.One"/> of its one <c>database_name</c>; a later lane wires the parameter to a list. Every
    /// one-name consumer is list-aware: the echoed <c>database_name</c> field (<see cref="DatabaseFilter.Describe"/>: the
    /// name for one database, "the chosen databases" for two or more, null for all) and the three filtered empty answers
    /// (<c>EmptyAsync</c>). For one name all of them read exactly as they did.
    /// </summary>
    internal static async Task<string> GetQueryStoreRegressions(
        NpgsqlDataSource postgres,
        string? server_name,
        int hours_back,
        DatabaseFilter databaseFilter,
        int limit,
        bool full_text,
        string? as_of,
        ILogger? logger,
        CancellationToken cancellationToken)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = DarlingReadWindow.Validate(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var end = windowEnd;
            var start = end.AddHours(-hours_back);
            /* #4195: the baseline's own fixed lookback, ending at the recent window's start - no longer
               "every retained row before it", whose cost tracked retention rather than the window asked for. */
            var baselineStart = start.AddDays(-DarlingQueryStoreRegressionReader.BaselineLookbackDays);

            /* #6654: with no database chosen the read leaves out the databases the server DROPPED (named by an earlier database_states
               snapshot, absent from the newest), as get_top_procedures_by_cpu does (#5633 item 4): their queries would otherwise fill the
               page. A named database is read as asked; an unknown set (no snapshot) leaves out nothing. The names ride the payload. */
            databaseFilter = await CurrentDatabaseScope.NarrowAsync(postgres, resolved.ServerId, databaseFilter,
                asOfUtc: string.IsNullOrWhiteSpace(as_of) ? null : windowEnd, windowStartUtc: baselineStart, logger: logger, cancellationToken: cancellationToken);
            var leftOut = DatabasesLeftOut.Distinct(databaseFilter.ExcludedNames);

            /*
                Over-fetch by one. Comparing the row count to the cap reports truncation for a server that
                happens to have exactly `limit` regressions and nothing more, which is a false positive in
                the one field whose whole reason for existing is that the cap should not have to be inferred.
            */
            /* #6750: where THIS server's query_store_stats start, probed once and ahead of the read. The notice below reports it as
               effective_start, and the read divides each per-hour rate by the hours from the later of it and the plan's start, on the
               raw path and the table path alike, so the two cannot name different instants. The store-wide chunk floor the raw row
               clamp reads is not a per-server fact. A probe that throws is not rethrown here: the notice reads the same faulted task
               and answers Unavailable, and the read keeps the plan's start. */
            var floorTask = ProbeDataStartAsync();
            async Task<DateTime?> ProbeDataStartAsync() =>
                await DarlingMcpWindowNotice.Probe(postgres, "query_store_stats", resolved.ServerName, baselineStart, end, cancellationToken);
            DateTime? dataStart = null;
            try
            {
                dataStart = await floorTask;
            }
            catch (Exception ex) when (!(ex is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                logger?.LogDebug(ex, "#6750 the data-start probe of query_store_stats failed; the rates divide by the plan's start");
            }

            var read = await DarlingQueryStoreRegressionReader.ReadWithSpansAsync(
                postgres, resolved.ServerId, start, end, databaseFilter, limit + 1, baselineStart, dataStart, cancellationToken);
            var rows = read.Rows;
            var historySource = read.Source;
            /* #6750: where the table read reached below raw's floor, each arm started where its own table proved complete history, and
               the notice names those starts instead of raw's. */
            var tableSpans = read.Spans is { BelowRawFloor: true } s ? s : null;

            /* #4966: where this server's query_store_stats start, checked against the EARLIER of the two windows this tool
               compares: the baseline's start, not the recent window's. A store that began inside the baseline compares
               against a shorter baseline than baseline_start names, and nothing else on the payload says so. Read ahead of the
               empty answers, which carry it too: "no query regressed" over a baseline the store only partly holds is not a true
               negative. Unfiltered by database_name (the floor is a property of the table). */
            var notice = tableSpans is not null
                ? TableNotice(tableSpans, baselineStart, start)
                : await DarlingMcpWindowNotice.ReadAsync(
                () => floorTask,
                baselineStart, end, "query_store_stats",
                tail: null,
                emptyAnswer: rows.Count == 0, logger: logger, cancellationToken: cancellationToken,
                /* #6750: the closing sentence follows where the data starts. Before baseline_end the baseline holds part of its span,
                   at or after it the baseline is empty, and after baseline_end (the recent window's start) the recent window is cut too. Both paths divide
                   each per-hour rate by the hours from this server's data start (the windows CTE's hours-held start), the instant
                   effective_start reports, so the note says so on either path. The row filters keep the requested start. */
                tailFor: effectiveStart => QueryStoreRegressionRules.TailFor(effectiveStart, start));

            if (rows.Count == 0)
                return McpHelpers.WithDatabasesLeftOut(
                    await EmptyAsync(postgres, resolved.DisplayName, resolved.ServerId, start, end, baselineStart, hours_back, databaseFilter, notice, tableSpans, cancellationToken), leftOut)!;

            /* #6750: always present. On the table path below raw's floor each arm's own start; otherwise both are the notice's effective_start. */
            var costBaselineStart = tableSpans is not null ? McpHelpers.FormatEffectiveStart(tableSpans.CostStart) : notice.EffectiveStart;
            var failureBaselineStart = tableSpans is not null ? McpHelpers.FormatEffectiveStart(tableSpans.FailureStart) : notice.EffectiveStart;

            var truncated = rows.Count > limit;
            var shown = rows.Take(limit);

            var json = JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* The window floor, always present (false and null when the store covered the baseline). No effective_hours_back:
                   this payload carries a page `truncated`, and the census holds that key apart for the window floor, so the reach
                   is the instant. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                /* #5809: which tier answered, in get_query_store_top's words (interval_table or raw). */
                history_source = historySource,
                database_name = databaseFilter.Describe(),
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
                cost_baseline_start = costBaselineStart,
                failure_baseline_start = failureBaselineStart,
                baseline_is = tableSpans is not null
                    ? "Cost: Query Store Regular runs from cost_baseline_start to baseline_end. Timeouts and errors: Aborted and Exception runs from failure_baseline_start to baseline_end"
                    : $"Query Store captures from baseline_start to recent_window_start ({DarlingQueryStoreRegressionReader.BaselineLookbackDays} days)",
                gate = "Regular average CPU regressed by more than 25% and the rise in CPU per run times the recent runs added at least 1 s of CPU over the window (cost), or the recent Aborted (timeouts) or Exception (errors) rate per hour is at least twice the baseline's (or nonzero where the baseline had none) with at least 3 such runs",
                /* #6128: every regression that passed the gate, whatever limit asked for; returned_count is this page. */
                regression_count = Math.Max(rows[0].TotalCount, rows.Count),
                returned_count = shown.Count(),
                truncated,
                regressions = shown.Select(r => RegressionRowJson(r, full_text)),
                /* #6654: the dropped databases this read left out, in the words get_top_procedures_by_cpu uses. */
                databases_left_out = DatabasesLeftOut.Named(leftOut),
                databases_left_out_note = DatabasesLeftOut.Note(leftOut),
            }, McpHelpers.JsonOptions);

            /* A failed probe costs the notice, never the rows (see DarlingMcpWindowNotice.ReadAsync). */
            return notice.IsUnavailable ? DarlingMcpWindowNotice.WithoutKeys(json) : json;
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

    /// <summary>
    /// The window-floor notice of a table read that reached below raw's floor (#6750). No raw probe: the read compared each arm from
    /// where its own table proved complete history (<see cref="DarlingQueryStoreRegressionReader.RegressionSpans"/>), so
    /// <c>effective_start</c> is the later of the two starts, the instant from which every arm is complete (as
    /// get_blocking_stats does with <see cref="DarlingMcpWindowNotice.Later"/>). <c>window_truncated</c> uses the shared ninety-minute
    /// slack. The note names each start in <c>effective_start</c>'s own text (#4966) and the table that served it.
    /// </summary>
    internal static McpWindowNotice TableNotice(DarlingQueryStoreRegressionReader.RegressionSpans spans, DateTime baselineStart, DateTime baselineEnd)
    {
        var later = DarlingMcpWindowNotice.Later(spans.CostStart, spans.FailureStart)!.Value;
        var truncated = RawWindowFloor.IsTruncated(later, baselineStart);
        var effective = RawWindowFloor.EffectiveStart(later, baselineStart);
        return new McpWindowNotice(McpHelpers.FormatEffectiveStart(effective), truncated, truncated ? TableNote(spans, baselineEnd) : null);
    }

    /// <summary>The note of <see cref="TableNotice"/>: which table served which arm, and from when.</summary>
    internal static string TableNote(DarlingQueryStoreRegressionReader.RegressionSpans spans, DateTime baselineEnd)
    {
        var cost = McpHelpers.FormatEffectiveStart(spans.CostStart);
        var failure = McpHelpers.FormatEffectiveStart(spans.FailureStart);
        var note = "The window reaches further back than this server's raw query_store_stats retains, so the baseline was read from the per-interval tables, each from where it holds complete history. ";
        note += spans.CostFromLatest
            ? $"Cost (Regular runs) was compared from {cost} to the end of the baseline, from query_store_interval_latest, which keeps {DarlingRetention.QueryStoreIntervalLatestRetentionDays} days. Timeouts and errors were compared from {failure} to the end of the baseline, from query_store_interval_wide, which keeps {QueryStoreIntervalPartitions.WideHorizonDays} days."
            : $"Cost, timeouts and errors were compared from {failure} to the end of the baseline, from query_store_interval_wide, which keeps {QueryStoreIntervalPartitions.WideHorizonDays} days.";
        if (DateTime.SpecifyKind(spans.FailureStart, DateTimeKind.Unspecified) >= DateTime.SpecifyKind(baselineEnd, DateTimeKind.Unspecified))
        {
            note += " That timeouts and errors baseline holds no hours, so timeouts and errors are flagged on recent runs alone.";
        }

        return note + " Each per-hour rate divides by the hours from its own start.";
    }

    private static double? Round(double? value) => value is null ? null : Math.Round(value.Value, 2);

    /// <summary>One regression row. The Regular figures are always there. Each of the Aborted and Exception groups always carries its
    /// counts, so a 0 reads as none; its CPU and duration per execution are left out of a side with no runs (there is no figure to
    /// show), and its runs per hour only on a row whose reason names it, where they are the figure the reason was judged on (#5506).</summary>
    private static Dictionary<string, object?> RegressionRowJson(DarlingQueryStoreRegressionReader.RegressionRow r, bool full_text)
    {
        var row = new Dictionary<string, object?>
        {
                ["database_name"] = r.DatabaseName,
                ["query_id"] = r.QueryId,
                /* Banded from the duration percent by the TVF's CASE, whose ELSE is 'LOW' — which for a
                   row with NO duration ratio is a verdict about a number that does not exist. Null there
                   (#3541 A12); the SQL's band is kept verbatim for the viewer it is shared with. */
                ["severity"] = r.DurationRegressionPercent is null && r.RegressionReason == "cost" ? null : r.Severity,
                /* #5506: why the row is listed: cost, timeouts, errors or a comma-separated combination. */
                ["regression_reason"] = r.RegressionReason,
                ["baseline_duration_ms"] = Round(r.BaselineDurationMs),
                ["recent_duration_ms"] = Round(r.RecentDurationMs),
                ["duration_regression_percent"] = Round(r.DurationRegressionPercent),
                ["baseline_cpu_ms"] = Round(r.BaselineCpuMs),
                ["recent_cpu_ms"] = Round(r.RecentCpuMs),
                ["cpu_regression_percent"] = Round(r.CpuRegressionPercent),
                /* Null percents, and why (#3541 A12): a 0 baseline has no ratio, and the reader used to
                   publish that as 0 — "no change" — for the row that changed the most. */
                ["undefined_percents"] = UndefinedPercentNotes(r),
                /* The ranking key, and the one number that says whether this regression MATTERS: a
                   5 ms regression executed a million times outranks a 5-second one executed twice. It is
                   an absolute delta, so it exists for every row and a null ratio never sorts as 0. */
                ["additional_duration_ms"] = Round(r.AdditionalDurationMs),
                ["baseline_exec_count"] = r.BaselineExecCount,
                ["recent_exec_count"] = r.RecentExecCount,
                /* A plan count that moved between the two sides is the first thing to check: a query
                   that regressed while gaining a plan is usually a plan-choice problem, not a data one. */
                ["baseline_plan_count"] = r.BaselinePlanCount,
                ["recent_plan_count"] = r.RecentPlanCount,
            ["baseline_regular_per_hour"] = Round(r.BaselineRegularPerHour),
            ["recent_regular_per_hour"] = Round(r.RecentRegularPerHour),
        };
        AddGroup(row, "aborted", r.Aborted, r.RegressionReason.Contains("timeouts", StringComparison.Ordinal));
        AddGroup(row, "exception", r.Exception, r.RegressionReason.Contains("errors", StringComparison.Ordinal));
        row["last_execution_time"] = r.LastExecutionTime?.ToString("o");
        row["query_text"] = full_text ? r.QueryTextSample : McpHelpers.TruncateStatement(r.QueryTextSample, QueryTextPreviewLength);
        row["query_text_truncated"] = !full_text && r.QueryTextSample.Length > QueryTextPreviewLength;
        return row;
    }

    private static void AddGroup(Dictionary<string, object?> row, string name, DarlingQueryStoreRegressionReader.RunGroup? g, bool reasonNamesIt)
    {
        row["baseline_" + name + "_count"] = g?.BaselineCount ?? 0;
        row["recent_" + name + "_count"] = g?.RecentCount ?? 0;
        if ((g?.BaselineCount ?? 0) > 0)
        {
            row["baseline_" + name + "_cpu_ms"] = Round(g!.BaselineCpuMs);
            row["baseline_" + name + "_duration_ms"] = Round(g.BaselineDurationMs);
        }
        if ((g?.RecentCount ?? 0) > 0)
        {
            row["recent_" + name + "_cpu_ms"] = Round(g!.RecentCpuMs);
            row["recent_" + name + "_duration_ms"] = Round(g.RecentDurationMs);
        }
        if (reasonNamesIt)
        {
            row["baseline_" + name + "_per_hour"] = Round(g?.BaselinePerHour);
            row["recent_" + name + "_per_hour"] = Round(g?.RecentPerHour);
        }
    }

    /// <summary>
    /// Which of a row's two regression percents are undefined, and why (#3541 A12, contract rule 5). Each
    /// percent divides through <c>NULLIF(baseline, 0)</c>, so a NULL means the baseline side was 0 — there is
    /// no denominator, not no change — and the caller is pointed at the absolute pair it can still compare.
    /// Null when every percent is defined, so the common row carries no noise. Lite's twin builds the same
    /// sentences.
    /// </summary>
    private static List<string>? UndefinedPercentNotes(DarlingQueryStoreRegressionReader.RegressionRow r)
    {
        List<string>? notes = null;
        /* #5506: a row listed only for timeouts or errors can have no Regular runs on one side, so there is no cost comparison:
           say so once instead of calling a missing side a zero baseline. */
        if (r.BaselineExecCount == 0 || r.RecentExecCount == 0)
        {
            return r.DurationRegressionPercent is null || r.CpuRegressionPercent is null
                ? new List<string> { "duration_regression_percent and cpu_regression_percent are null: there are no Regular runs in the " + (r.BaselineExecCount == 0 ? "baseline" : "recent") + " window, so this row has no cost comparison; it is listed for regression_reason." }
                : null;
        }

        void Note(string field, string baseline, string recent)
            => (notes ??= new List<string>()).Add(
                $"{field} is null: no_baseline — {baseline} is 0, so the ratio has no denominator; this is NOT 0% change. Compare {baseline} to {recent} directly.");

        if (r.DurationRegressionPercent is null) Note("duration_regression_percent", "baseline_duration_ms", "recent_duration_ms");
        if (r.CpuRegressionPercent is null) Note("cpu_regression_percent", "baseline_cpu_ms", "recent_cpu_ms");
        return notes;
    }

    /// <summary>
    /// What zero regressions actually means, which is four different things.
    /// <para>Only ONE of them is good news, and the other three all look identical to it in a bare empty
    /// array. The dangerous one is a server whose entire collected history sits INSIDE the requested
    /// window: it has no BEFORE, so it can never show a regression however badly it regressed, and
    /// answering "no regressions" there is a confident wrong answer rather than a missing one. One probe,
    /// two booleans, run only on this path.</para>
    /// <para>#5015: with a <c>database_name</c> filter, the server passing those questions is not enough. The same two
    /// questions are then asked of the filtered database alone, so a database with only one side collected gets the
    /// matching no-baseline or missing-window answer, naming it, and only a database with both sides is the all-clear.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        NpgsqlDataSource postgres, string serverName, int serverId, DateTime start, DateTime end, DateTime baselineStart, int hours_back,
        DatabaseFilter databases, McpWindowNotice notice, DarlingQueryStoreRegressionReader.RegressionSpans? tableSpans, CancellationToken cancellationToken)
    {
        /* #6750: when the table read reached below raw's floor, the two questions are asked of the table that served the cost arms, over
           the span it compared. Raw's probe would answer "no baseline" for a 168 hour read on a store whose raw tier keeps 4 days, while
           the per-interval table held the whole baseline. */
        Task<(bool HasBaseline, bool HasRecent)> CoverageAsync(DatabaseFilter filter) => tableSpans is not null
            ? DarlingQueryStoreRegressionReader.GetTableCoverageAsync(postgres, serverId, start, end, tableSpans, filter, cancellationToken)
            : DarlingQueryStoreRegressionReader.GetCoverageAsync(postgres, serverId, start, end, baselineStart, filter, cancellationToken);

        var (hasBaseline, hasRecent) = await CoverageAsync(DatabaseFilter.All);

        if (!hasBaseline && !hasRecent)
        {
            /* #6181: Query Store on, collector healthy, first interval not closed yet: say so instead of "may be OFF". */
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, serverId, serverName, "query_store", cancellationToken)
                ?? (await DarlingRuntimePrecondition.QueryStoreFirstIntervalMessageAsync(postgres, serverId, serverName, databases, cancellationToken)
                    is { } firstInterval ? McpHelpers.Status("unavailable", firstInterval + " There is nothing to compare yet, so this is not a report of zero regressions.") : null)
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
                $"{serverName} has Query Store history from before this window but nothing collected IN the last {hours_back} hour(s), so there is a recent side missing rather than nothing to report. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.",
                notice.AsHints());
        }

        /* #5015: the probe above is the SERVER's, so it answers a database_name filter the same whether the database was
           compared or not, and only a database with BOTH sides collected can be the all-clear. The filter reads the same
           database the read was asked for, and the answer is decided from that pair with the server-wide answers' own
           statuses: neither side means the filter matched nothing, no baseline is the server-wide no-baseline answer and
           no capture in the window the server-wide missing-window answer, each naming the database. Lite's twin words
           it the same. */
        if (!databases.IsAll)
        {
            var (filteredBaseline, filteredRecent) = await CoverageAsync(databases);

            /* One name reads exactly as it always did. Two or more say "the chosen databases" where the one name stood
               (DatabaseFilter.Describe), and the singular database/its/that words go plural. */
            var one = databases.Names.Count == 1;
            var scope = one ? $"database_name '{databases.Names[0]}'" : DatabaseFilter.ManyDatabasesDescription;
            var that = one ? "that database" : "those databases";
            var thatPossessive = one ? "that database's" : "those databases'";

            if (!filteredBaseline && !filteredRecent)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No Query Store capture on {serverName} in the last {hours_back} hour(s) or in the {DarlingQueryStoreRegressionReader.BaselineLookbackDays}-day baseline before it matched {scope}, so the filter matched nothing and this is NOT the all-clear: no query in {that} was compared. Check the database {(one ? "name" : "names")}, or drop the filter to read every database.",
                    notice.AsHints());
            }

            if (!filteredBaseline)
            {
                return McpHelpers.Status(
                    "unavailable",
                    $"{serverName} has no Query Store capture of {scope} in the {DarlingQueryStoreRegressionReader.BaselineLookbackDays}-day baseline window before this window, so there is no baseline for {that} to compare against and no regression of {(one ? "its" : "their")} queries can be detected however badly one regressed. This is NOT a clean bill of health: no query in {that} was compared. Either {thatPossessive} whole collected history falls inside the last {hours_back} hour(s), or {(one ? "it has" : "they have")} none older than the baseline lookback yet.");
            }

            if (!filteredRecent)
            {
                return McpHelpers.Status(
                    "empty",
                    $"{serverName} has Query Store history of {scope} from before this window but nothing of {(one ? "it" : "them")} collected IN the last {hours_back} hour(s), so {thatPossessive} recent side is missing rather than nothing to report, and this is NOT the all-clear: no query in {that} was compared. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.",
                    notice.AsHints());
            }
        }

        return McpHelpers.Status(
            "empty",
            McpHelpers.QuietUnlessCut(
                notice.WindowTruncated, notice.EffectiveStart,
                factual: $"No query on {serverName} regressed in the last {hours_back} hour(s). Both a baseline and this window were collected and no query's Regular average CPU is more than 25% worse than its baseline with at least 1 s of added CPU, and none of their timeouts or errors rose",
                coveredClaim: " — this IS the all-clear for this read."),
            notice.AsHints());
    }
}
