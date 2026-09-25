using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpQueryTools
{
    [McpServerTool(Name = "get_top_queries_by_cpu"), Description("Gets expensive cached queries from sys.dm_exec_query_stats, ranked by CPU over a window ending at as_of. Filters (database_name, parallel_only, min_dop) apply before the top-N cap: filter_applied names the floor in force, and an empty page under it is the window's real answer, not a miss. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes, not windowed; cpu_attribution's ratio is omitted, not invented, when its inputs are missing. <<GUIDE>> Gets expensive queries from sys.dm_exec_query_stats (plan cache). Best for: currently cached queries with detailed per-execution stats, DOP, spills, and query_hash for trending. Returns query_hash, query_plan_hash, sql_handle, plan_handle, and host_object (the hosting procedure/function for proc-hosted statements, null for ad-hoc) — groups key on (database, query_hash, host_object), so INSERT...EXEC callers in different procedures report separately with their own text. distinct_texts counts statement texts merged into a group (>1 = ad-hoc literal variants or pre-upgrade history; query_text is one representative, 0 means no stored text for the group). Supports database and parallelism filtering; every filter is applied IN the query before the ranking and the cap, so the page is the top-N of the FILTERED population (filter_applied names the parallelism floor in force, null when none), and an empty page under parallel_only/min_dop is the window's answer rather than a page artefact. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. max_dop comes from sys.dm_exec_query_stats and is a lifetime-max for the plan's time in cache, so a plan compiled before MAXDOP was lowered keeps reporting the old higher value until it is evicted or recompiled; confirm current parallelism with analyze_query_plan, which reads the actual plan. " + McpToolGuideTopics.CpuTimeExtremesAndAttribution)]
    public static async Task<string> GetTopQueriesByCpu(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries whose cached plan has EVER run at DOP > 1 (a LIFETIME max_dop; can read stale after a MAXDOP change). See the tool's reading guide.")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering. Filters the same lifetime-max value as parallel_only, not current parallelism.")] int min_dop = 0,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            /* Captured BEFORE the ranking read, whose window is its own internal UtcNow — hoisting
               shrinks the numerator/denominator window skew from the ranking query's full duration to
               call-entry overhead (review catch; threading one instant INTO the shared ranking read's
               signature is the only way to zero it, and sub-microsecond against an hours window does
               not buy that churn). */
            var nowUtc = windowEnd;

            /* #3541 A13: the parallelism filter goes INTO the read as a lifetime max_dop floor on the grouped
               population, before the CPU ranking and the cap (Darling's twin: TopQueriesSql's HAVING note). It
               used to be a .Where over the returned top-N page, so parallel_only=true on a box whose hottest
               plans were serial came back EMPTY while the window held parallel plans. 2 for parallel_only,
               min_dop when set above that (min_dop implies parallel filtering, as its description says), 0
               (admit all) otherwise. */
            var minMaxDop = min_dop > 1 ? min_dop : parallel_only ? 2 : 0;
            var filterApplied = minMaxDop > 0
                ? $"lifetime max_dop >= {minMaxDop} (applied in SQL before the top-{top} ranking; the page is the top-{top} of the parallel population)"
                : null;

            var rows = await dataService.GetTopQueriesByCpuAsync(resolved.ServerId, hours_back, top, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name }, asOfUtc: windowEnd, minMaxDop: minMaxDop);
            if (rows.Count == 0)
            {
                /* A filtered miss is not a collection miss — same words as Darling's twin. */
                if (minMaxDop > 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No query-stats group on {resolved.ServerName} in the last {hours_back} hour(s) has a cached plan with lifetime max_dop >= {minMaxDop}. The filter was applied in SQL over the whole window, so this is the window's answer rather than a page artefact — drop parallel_only / min_dop to see the unfiltered ranking, or confirm current parallelism with analyze_query_plan.",
                        new { filter_applied = filterApplied });
                }

                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status("unavailable", "No query stats available for the specified time range.");
            }

            /* #2320: what fraction of the box's measured CPU the RETURNED rows explain — numerator is
               the caller-visible ranking (post top-N, post filters), denominator is measured, and the
               ratio is omitted rather than invented when a denominator piece is missing. One nowUtc
               backs the aggregate read AND the ratio math, and the two independent reads run
               concurrently (review catches; Darling has both by construction). */
            var cpuAggregateTask = dataService.GetCpuWindowAggregateAsync(resolved.ServerId, nowUtc.AddHours(-hours_back), nowUtc);
            var propertiesTask = dataService.GetLatestServerPropertiesAsync(resolved.ServerId);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuMs) / 1000.0,
                nowUtc.AddHours(-hours_back), nowUtc,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.CpuCount ?? 0);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.TotalExecutions,
                total_cpu_ms = r.TotalCpuMs,
                total_elapsed_ms = r.TotalElapsedMs,
                avg_cpu_ms = r.AvgCpuMs,
                avg_elapsed_ms = r.AvgElapsedMs,
                min_cpu_ms = r.MinCpuMs,
                max_cpu_ms = r.MaxCpuMs,
                min_elapsed_ms = r.MinElapsedMs,
                max_elapsed_ms = r.MaxElapsedMs,
                /* #2235: min/max are lifetime extremes (see QueryStatExtremes) — flagged only on
                   the provable case, an extreme exceeding the whole window's total. */
                extremes_note = QueryStatExtremes.LifetimeExtremeNote(
                    r.TotalCpuMs, r.MaxCpuMs, r.TotalElapsedMs, r.MaxElapsedMs),
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                is_parallel = r.MaxDop > 1,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_rows = r.TotalRows,
                total_spills = r.TotalSpills,
                avg_reads = r.AvgReads,
                // #2012 stage 2: same annotations as Darling's twin — the host object joins the
                // grouping key, so proc-hosted INSERT...EXEC callers sharing a hash land in
                // separate, correctly-labeled rows; null = ad-hoc/prepared text (or pre-upgrade
                // history, which ages out with retention).
                host_object = r.HostObjectName,
                query_text = McpHelpers.Truncate(r.QueryText, 2000),
                distinct_texts = r.DistinctTexts,
                text_note = r.DistinctTexts > 1
                    ? $"this group blends {r.DistinctTexts} distinct statement texts (ad-hoc literal variants; or history predating the host-object split for INSERT...EXEC callers); query_text is one representative"
                    : null
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #3541 A13: the filter that shaped the population, stated on the payload; null when none. */
                filter_applied = filterApplied,
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_queries_by_cpu", ex);
        }
    }

    [McpServerTool(Name = "get_top_procedures_by_cpu"), Description("Gets the most expensive stored procedures ranked by total CPU time over a window ending at as_of. Delta-based: requires ~30 minutes after adding a new server before data appears. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes, not windowed (extremes_note flags a provably stale one); cpu_attribution's ratio is omitted, not invented, when its inputs are missing. <<GUIDE>> Shows execution counts, CPU/elapsed times, and I/O metrics. Delta-based: requires ~30 minutes after adding a new server before data appears. " + McpToolGuideTopics.CpuTimeExtremesAndAttribution)]
    public static async Task<string> GetTopProceduresByCpu(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top procedures. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            /* Same pre-read capture as the queries tool — the skew shrinks to call-entry overhead. */
            var nowUtc = windowEnd;
            var rows = await dataService.GetTopProceduresByCpuAsync(resolved.ServerId, hours_back, top, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name }, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "procedure_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No procedure stats available. Delta-based collection requires at least two collection cycles (~30 minutes) to produce non-zero values.");
            }

            /* #2320: same attributed-CPU disclosure as the queries tool — one shared computation, one
               nowUtc backing aggregate and ratio, same concurrent independent reads. */
            var cpuAggregateTask = dataService.GetCpuWindowAggregateAsync(resolved.ServerId, nowUtc.AddHours(-hours_back), nowUtc);
            var propertiesTask = dataService.GetLatestServerPropertiesAsync(resolved.ServerId);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuMs) / 1000.0,
                nowUtc.AddHours(-hours_back), nowUtc,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.CpuCount ?? 0);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                full_name = r.FullName,
                object_type = r.ObjectType,
                sql_handle = r.SqlHandle,
                plan_handle = r.PlanHandle,
                execution_count = r.TotalExecutions,
                total_cpu_ms = r.TotalCpuMs,
                total_elapsed_ms = r.TotalElapsedMs,
                avg_cpu_ms = r.AvgCpuMs,
                avg_elapsed_ms = r.AvgElapsedMs,
                min_cpu_ms = r.MinCpuMs,
                max_cpu_ms = r.MaxCpuMs,
                min_elapsed_ms = r.MinElapsedMs,
                max_elapsed_ms = r.MaxElapsedMs,
                /* #2235: same lifetime-extremes flag as the queries tool. Mirrors Darling. */
                extremes_note = QueryStatExtremes.LifetimeExtremeNote(
                    r.TotalCpuMs, r.MaxCpuMs, r.TotalElapsedMs, r.MaxElapsedMs),
                avg_reads = r.AvgReads,
                total_logical_reads = r.TotalLogicalReads,
                total_logical_writes = r.TotalLogicalWrites,
                total_physical_reads = r.TotalPhysicalReads,
                total_spills = r.TotalSpills
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
                procedures = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_procedures_by_cpu", ex);
        }
    }

    /// <summary>
    /// #4198: the default page's <c>query_text</c> preview length. Mirrors
    /// <c>DarlingMcpDataTools.QueryTextPreviewLength</c> -- see that constant's remarks for why 400.
    /// </summary>
    private const int QueryTextPreviewLength = 400;

    [McpServerTool(Name = "get_query_store_top"), Description("Cost-ranked top Query Store queries (heaviest first), not time-ordered. Requires Query Store enabled on target databases. Darling: window_truncated marks a window floor, not a page cut — no limit changes it — because raw retention can be shorter than asked; effective_start / effective_hours_back give the reach actually served. Lite: no such floor; the full requested window is always read. <<GUIDE>> Gets expensive queries from Query Store (persistent, survives restarts). Best for: historical analysis, queries no longer in plan cache. Requires Query Store enabled on target databases. Supports database and module filtering. Rows are per Query Store execution outcome (execution_type: Regular, Aborted, Exception): a plan with aborted executions returns one row per outcome, each with its own counts and averages. The execution_type filter keeps one outcome, and module_name keeps one module: the exact, case-sensitive schema-qualified name the collector records (get_top_procedures_by_cpu's full_name; Adhoc for ad-hoc statements, Unknown for an object it could not resolve), applied after interval deduplication and before ranking. When a filter matches nothing but the same read without the filters has rows, the answer is empty (a measured zero), not a Query Store precondition. query_text is a 400-character preview by default (query_text_truncated marks a cut row); full_text=true returns each row's whole statement.")]
    public static async Task<string> GetQueryStoreTop(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top queries. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Filter by Query Store execution outcome: Regular, Aborted, or Exception.")] string? execution_type = null,
        [Description("Exact schema-qualified module name, as get_top_procedures_by_cpu returns it in full_name (e.g. dbo.usp_ProcessOrder). Case-sensitive; applied before ranking. Ad-hoc statements are Adhoc.")] string? module_name = null,
        [Description("Return each row's full query_text instead of a 400-character preview. Default false.")] bool full_text = false)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;
            /* A closed set, refused by name rather than applied (#3541 A13): an unknown outcome can never match,
               and the empty answer under it would read as "no such executions". Downstream filters on the
               canonical spelling, which is how the collector stores it. */
            var executionTypeError = McpHelpers.ValidateChoice(execution_type, McpHelpers.QueryStoreExecutionTypes, "execution_type");
            if (executionTypeError != null) return executionTypeError;
            execution_type = string.IsNullOrWhiteSpace(execution_type)
                ? null
                : McpHelpers.QueryStoreExecutionTypes.First(t => string.Equals(t, execution_type.Trim(), StringComparison.OrdinalIgnoreCase));

            module_name = string.IsNullOrWhiteSpace(module_name) ? null : module_name;

            var rows = await dataService.GetQueryStoreTopQueriesAsync(
                resolved.ServerId, hours_back, top,
                databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name },
                asOfUtc: windowEnd,
                executionType: execution_type,
                moduleName: module_name);
            if (rows.Count == 0)
            {
                /* A filter that matched nothing is an answer, not a missing collection. Most queries never abort,
                   so an Aborted or Exception filter is empty far more often than not, and falling through to the
                   chain below ended at "Query Store may not be enabled" -- false, whenever the same read without
                   the filter has rows. One unfiltered top-1 read tells the two apart; it runs only on this path.
                   module_name (#4057) is the same case and takes the same test: a module that did not run in the
                   window is a measured zero whenever the read without the filters has rows. */
                if ((execution_type != null || module_name != null)
                    && (await dataService.GetQueryStoreTopQueriesAsync(resolved.ServerId, hours_back, 1, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name }, asOfUtc: windowEnd)).Count > 0)
                    return module_name is null
                        ? McpHelpers.QueryStoreExecutionTypeEmpty(execution_type!, hours_back, database_name)
                        : McpHelpers.QueryStoreModuleEmpty(module_name, execution_type, hours_back, database_name);

                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_store")
                    /* #2546: the sentence below GUESSES ("may not be enabled"), and it had to, because the
                       read had no way to find out. The store has known all along — query_store_health
                       records actual_state per database every hour for exactly this purpose. Asking it turns
                       a hedge into a fact plus the ALTER DATABASE that fixes it, and it answers for the
                       database this read was scoped to rather than for the server's most flattering one. */
                    ?? await McpRuntimePrecondition.QueryStoreStatusAsync(dataService, resolved.ServerId, resolved.ServerName, database_name)
                    /* And the collector's own last run, for the case Query Store is on and the collector is
                       the thing that cannot read it. */
                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_store")
                    ?? McpHelpers.Status("unavailable", "No Query Store data available. Query Store may not be enabled on target databases.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_id = r.QueryId,
                plan_id = r.PlanId,
                query_hash = r.QueryHash,
                query_plan_hash = r.QueryPlanHash,
                execution_type = r.ExecutionTypeDesc,
                module_name = r.ModuleName,
                execution_count = r.TotalExecutions,
                avg_duration_ms = r.AvgDurationMs,
                avg_cpu_ms = r.AvgCpuTimeMs,
                avg_logical_reads = r.AvgLogicalReads,
                avg_logical_writes = r.AvgLogicalWrites,
                avg_physical_reads = r.AvgPhysicalReads,
                avg_rowcount = r.AvgRowcount,
                last_execution_time = r.LastExecutionTime?.ToString("o"),
                query_text = full_text ? r.QueryText : McpHelpers.Truncate(r.QueryText, QueryTextPreviewLength),
                query_text_truncated = !full_text && r.QueryText != null && r.QueryText.Length > QueryTextPreviewLength
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_regressions"), Description("Finds queries whose Query Store performance got WORSE: recent window (hours_back, ending at as_of) vs a fixed 7-day baseline before it (see baseline_start/baseline_end). get_query_store_top ranks EXPENSIVE, this ranks CHANGED. Gated: average CPU regressed over 25%. duration_regression_percent, io_regression_percent and severity are null, not 0%, when their baseline is 0. additional_duration_ms is the ranking key. empty: no regression, or nothing yet in the baseline window. unavailable: no baseline exists yet. not_collected: this server's engine cannot run Query Store. <<GUIDE>> Finds queries whose Query Store performance got WORSE, by comparing each (database, query_id) group's averages inside a recent window against its baseline - a FIXED 7-day lookback ending at that window's start (before this it was every capture EVER collected before the window, so its cost tracked how much history the store still retained rather than the window asked for, and the comparison period silently grew on a server with more retention). baseline_start and baseline_end report exactly which period was compared - a regression against something older than the baseline lookback is not caught; a store retaining less than that is unaffected. Returns baseline vs recent duration, CPU and logical reads with the regression percent for each, the execution-count-weighted extra duration (the ranking key: a 5 ms regression executed a million times outranks a 5-second one executed twice), the plan counts on both sides, and a duration-driven severity band. get_query_store_top answers what is EXPENSIVE; the most expensive query is usually the one that always was. This answers what CHANGED. Rows are kept only where average CPU regressed by more than 25%. A regression percent whose BASELINE side is 0 has no denominator and is returned as null, with the reason under undefined_percents - never as 0, which would read as no change when the truth is the largest possible one; compare the two absolute figures instead. The ranking key is the absolute, execution-weighted duration delta, which exists whether or not a ratio does, so a null percent never sorts as 0. severity is banded from the duration percent and is null when that percent is.")]
    public static async Task<string> GetQueryStoreRegressions(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Size of the RECENT window, in hours back from now. Everything collected before it is the baseline. Default 24.")] int hours_back = 24,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum rows to return, worst first. Default 50 (the number the desktop viewer shows).")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            /*
                Over-fetch by one. Comparing the row count to the cap reports truncation for a server that
                happens to have exactly `limit` regressions and nothing more, which is a false positive in
                the one field whose whole reason for existing is that the cap should not have to be inferred.
            */
            var databases = string.IsNullOrWhiteSpace(database_name) ? null : new[] { database_name };
            var rows = await dataService.GetQueryStoreRegressionsAsync(
                resolved.ServerId, hours_back, limit + 1, databases, asOfUtc: windowEnd);

            var windowStart = windowEnd.AddHours(-hours_back);
            var baselineStart = windowStart.AddDays(-LocalDataService.BaselineLookbackDays);

            if (rows.Count == 0)
                return await EmptyRegressionsAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd);

            var truncated = rows.Count > limit;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                database_name,
                /* A fixed lookback ending at the recent window's start, not "every capture ever collected
                   before it" - the baseline no longer grows with retention (Lite's twin of Darling's #4195). */
                baseline_start = baselineStart.ToString("o"),
                baseline_end = windowStart.ToString("o"),
                baseline_is = $"Query Store captures from baseline_start to the recent window's start ({LocalDataService.BaselineLookbackDays} days)",
                gate = "average CPU regressed by more than 25%",
                regression_count = Math.Min(rows.Count, limit),
                truncated,
                regressions = rows.Take(limit).Select(r => new
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
                    /* The ranking key, and the one number that says whether this regression MATTERS. It is
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
    /// Null when every percent is defined, so the common row carries no noise. Darling's twin builds the same
    /// sentences.
    /// </summary>
    private static List<string>? UndefinedPercentNotes(QueryStoreRegressionRow r)
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
    /// What zero regressions actually means, which is four different things — word for word with Darling.
    /// <para>Only ONE of them is good news, and the other three look identical to it in a bare empty array.
    /// The dangerous one is a server whose entire collected history sits INSIDE the requested window: it has
    /// no BEFORE, so it can never show a regression however badly it regressed, and answering "no
    /// regressions" there is a confident wrong answer rather than a missing one.</para>
    /// </summary>
    private static async Task<string> EmptyRegressionsAsync(
        LocalDataService dataService, int serverId, string serverName, int hours_back, DateTime windowEnd)
    {
        /* The anchor is threaded in rather than resolved again: the coverage probe answers "does a BEFORE
           exist for this window", and a window it computed for itself would be a different one. */
        var (hasBaseline, hasRecent) = await dataService.GetQueryStoreRegressionCoverageAsync(serverId, hours_back, asOfUtc: windowEnd);

        if (!hasBaseline && !hasRecent)
        {
            return await McpEngineCapability.NotCollectedStatusAsync(dataService, serverId, serverName, "query_store")
                ?? McpHelpers.Status(
                    "unavailable",
                    $"No Query Store data has EVER been collected for {serverName}, so this is NOT a report of zero regressions — there is nothing to compare. Query Store may be OFF on this server's databases, which get_query_store_health will say; otherwise check that collection is running for this server.");
        }

        if (!hasBaseline)
        {
            return McpHelpers.Status(
                "unavailable",
                $"{serverName} has no Query Store capture in the {LocalDataService.BaselineLookbackDays}-day baseline window before this window, so there is no baseline to compare against and no regression can be detected however badly one regressed. This is NOT a clean bill of health. Either this server's whole collected history falls inside the last {hours_back} hour(s), or it has none older than the baseline lookback yet.");
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

    [McpServerTool(Name = "get_query_heatmap"), Description("Draws the desktop viewer's Query Heatmap as a table: how many distinct queries fell into each (time bin x log-magnitude bucket) cell over a window, plus the most-executed query in each cell. It answers when a server was slow and how slow at the same time - get_top_queries_by_cpu ranks queries over a whole window and cannot show that the window had two very different halves. Bins are 5 minutes wide by default because that is exactly what the desktop viewer uses, so a browser, an agent and a desktop pointed at the same server draw the same picture; raise bucket_minutes for a longer window, which is also the lever that fits more of the window inside the cell cap. Magnitude buckets are the viewer's seven, in the metric's own unit: under 1, 1-10, 10-100, 100-1K, 1K-10K, 10K-100K and over 100K.")]
    public static async Task<string> GetQueryHeatmap(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("How far back to look, in hours. Default 24.")] int hours_back = 24,
        [Description("Which per-execution metric to bucket by: duration, cpu, logical_reads, logical_writes or execution_count. Default duration.")] string? metric = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Width of each time bin, in minutes. Default 5 - the desktop viewer's own bin width, so the two surfaces agree. Raise it to cover a longer window in fewer cells.")] int bucket_minutes = LocalDataService.ViewerHeatmapBucketMinutes,
        [Description("Maximum CELLS to return, most recent bins first. Default 500. A full day of 5-minute bins can reach 2,016 cells on a busy server; raise bucket_minutes rather than the cap to see the whole window.")] int limit = DefaultHeatmapCellLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            /*
                #2495: the anchor, and it earns its place here more than on most reads. A heatmap IS a time
                axis, so "the 4 hours ending Tuesday 03:00" is the shape of every question anyone brings to
                it — and widening hours_back until an old incident falls inside is not the same question,
                because the extra hours land as extra COLUMNS that push the incident's own columns past the
                cell cap.
            */
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd)
                ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            /* The bin-width bound, refused rather than clamped, and built where the sentence is — Darling's
               twin's rule (#3897: it was a bare sentence behind a `??` chain). */
            if (bucket_minutes < 1 || bucket_minutes > LocalDataService.MaxHeatmapBucketMinutes)
                return McpHelpers.Refusal("bucket_minutes", $"Invalid bucket_minutes value '{bucket_minutes}'. Must be between 1 and 1440 (one day). The desktop viewer's Query Heatmap uses 5, which is this read's default.");

            /* A metric we do not know is REFUSED, not quietly turned into duration: a caller who asked for
               CPU and silently got elapsed time would read the wrong grid with nothing to tell them so. */
            if (!LocalDataService.TryParseHeatmapMetric(metric, out var parsedMetric))
                return McpHelpers.Refusal("metric", $"Invalid metric '{metric}'. Valid values: duration, cpu, logical_reads, logical_writes, execution_count.");

            /*
                Over-fetch by one. Comparing the row count to the cap reports truncation for a server that
                happens to have exactly `limit` cells and nothing more, which is a false positive in the one
                field whose whole reason for existing is that the cap should not have to be inferred.
            */
            /* The resolved anchor is threaded INTO the read rather than left to it. Before #2495 this
               call let the service compute its own window from UtcNow and the reported window was taken
               after the call returned, so the two disagreed by however long the read took — on the one
               read whose entire output is a time axis (review catch). One instant now decides both. */
            var databases = string.IsNullOrWhiteSpace(database_name) ? null : new[] { database_name };
            var rows = await dataService.GetQueryHeatmapCellsAsync(
                resolved.ServerId, parsedMetric, hours_back, bucket_minutes, limit + 1, databases, asOfUtc: windowEnd);

            if (rows.Count == 0)
                return await EmptyHeatmapAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd);

            var truncated = rows.Count > limit;
            var cells = rows.Take(limit).ToList();

            if (truncated)
            {
                /*
                    The rows arrive newest-bin-first so the cap keeps the RECENT end of the window, which is
                    what anyone looking at an incident wants. The cost is that the cap can land in the middle
                    of the oldest bin it reached, handing back a column missing its low buckets — and a column
                    with holes reads as "nothing fast ran then" rather than "we stopped looking", which is the
                    kind of quiet wrong answer a grid makes very easy to believe. So the partial column goes.
                    Kept when it is the ONLY column (a cap below one bin's seven cells has nothing to fall
                    back to); `truncated` and last_time_bin still say what happened.
                */
                var oldestReached = cells[^1].TimeBucket;
                var wholeColumns = cells.Where(c => c.TimeBucket > oldestReached).ToList();
                if (wholeColumns.Count > 0) cells = wholeColumns;
            }

            /*
                Back into reading order: time ascending, and buckets ascending WITHIN each bin. A plain
                Reverse() would hand back the bins in the right order with each bin's buckets upside down,
                because the SQL sorts time DESC and bucket ASC. The DESC exists only so the cap cuts the
                right end of the window; it should not leak into the shape of the grid.
            */
            cells = cells.OrderBy(c => c.TimeBucket).ThenBy(c => c.BucketIndex).ToList();

            var labels = LocalDataService.HeatmapBucketLabelsFor(parsedMetric);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                metric = LocalDataService.HeatmapMetricName(parsedMetric),
                metric_unit = LocalDataService.HeatmapMetricUnit(parsedMetric),
                database_name,
                window_start = windowEnd.AddHours(-hours_back).ToString("o"),
                window_end = windowEnd.ToString("o"),
                bucket_minutes,
                /* The same bin width the desktop viewer hardcodes, so the two surfaces cannot disagree
                   about the same server over the same window. */
                bucket_minutes_matches_desktop_viewer = bucket_minutes == LocalDataService.ViewerHeatmapBucketMinutes,
                /* A bare bucket_index is unreadable, and the labels differ by metric family: duration and
                   CPU are milliseconds, the other three are counts. */
                magnitude_buckets = labels.Select((label, index) => new { bucket_index = index, label }),
                time_bin_count = cells.Select(c => c.TimeBucket).Distinct().Count(),
                cell_count = cells.Count,
                /* Which slice of [window_start, window_end] actually came back. When truncated is true the
                   read dropped the OLDEST bins, not the least interesting cells, so these two are the only
                   way to see how much of the window is missing. */
                first_time_bin = cells[0].TimeBucket.ToString("o"),
                last_time_bin = cells[^1].TimeBucket.ToString("o"),
                truncated,
                cells = cells.Select(c => new
                {
                    time_bucket = c.TimeBucket.ToString("o"),
                    bucket_index = c.BucketIndex,
                    bucket_label = labels[Math.Clamp(c.BucketIndex, 0, HeatmapBucketCount - 1)],
                    /* Distinct queries in the cell, NOT executions — a cell of 40 is forty different queries
                       that ran at that speed, which is a different finding from one query running 40 times. */
                    query_count = c.QueryCount,
                    top_query_hash = c.TopQueryHash,
                    top_query_text = c.TopQueryText,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_heatmap", ex);
        }
    }

    /// <summary>The web panel's cap and this tool's default: 500 cells, which is a full day of 5-minute
    /// bins on a server whose queries land in two or three magnitude buckets per bin.</summary>
    private const int DefaultHeatmapCellLimit = 500;

    /// <summary>The seven log-magnitude rows of the grid — the viewer's, not a new banding.</summary>
    private const int HeatmapBucketCount = 7;

    /// <summary>
    /// What an empty grid means, which is three different things — word for word with Darling.
    /// <para>The probe reads the DATA rather than counting SUCCESS runs in the collection log, and that is a
    /// judgement about which kind of table this is. <c>query_stats</c> is PERIODIC: the collector writes rows
    /// every cycle for whatever is in the plan cache, so an empty history really does mean nobody looked. An
    /// edge table — blocking, deadlocks — would need the opposite treatment, because there zero rows is the
    /// healthy answer and a data probe sends someone to fix collection that works.</para>
    /// <para>The third branch is the one only this read has: collection ran, the window has captures, and
    /// every one of them recorded zero executions. That is an IDLE server, not a broken one, and telling a
    /// caller to widen the window there would be advice pointed at the wrong problem.</para>
    /// </summary>
    private static async Task<string> EmptyHeatmapAsync(
        LocalDataService dataService, int serverId, string serverName, int hours_back, DateTime windowEnd)
    {
        /* The anchor is threaded in rather than resolved again: the probe answers "was anything collected
           in THIS window", and a window it computed for itself would be a different one. */
        var (hasAny, hasInWindow) = await dataService.GetQueryHeatmapCoverageAsync(serverId, hours_back, asOfUtc: windowEnd);

        if (!hasAny)
        {
            return await McpEngineCapability.NotCollectedStatusAsync(dataService, serverId, serverName, "query_stats")
                ?? McpHelpers.Status(
                    "unavailable",
                    $"No query stats have EVER been collected for {serverName}, so this is NOT a report of a quiet server — there is nothing to draw. query_stats is a PERIODIC table rather than an edge table: the collector writes rows every cycle for whatever is in the plan cache, so an empty history means nobody looked. Check get_collection_health for this server.");
        }

        if (!hasInWindow)
        {
            return McpHelpers.Status(
                "empty",
                $"{serverName} has query stats from outside this window but nothing collected IN the last {hours_back} hour(s), so the grid has no columns rather than no hot cells. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.");
        }

        return McpHelpers.Status(
            "empty",
            $"Query stats WERE collected for {serverName} in the last {hours_back} hour(s), but no capture recorded an execution: every row carried a zero execution delta, so nothing lands on the grid. A server that is up and idle looks exactly like this, and so does a database_name filter matching nothing collected. Delta-based collection also needs a SECOND cycle before the first non-zero row exists.");
    }

    [McpServerTool(Name = "get_query_duration_trend"), Description("Gets a time-series of query elapsed_ms_per_second and executions_per_second across all queries, points ending at as_of. A collection whose interval was unknowable is excluded, not counted as 0: unrated_collections counts it, and a point left with nothing else carries null rates (unrated_points), never zero. No points: unavailable means query_stats was never collected here; empty means it was (the message says quiet window or rollup coverage gap). window_truncated is the store's retention floor, not a page cut: effective_start / effective_hours_back say where the answer begins. <<GUIDE>> Gets a time-series of average query duration over time. Useful for spotting overall performance degradation or improvement trends across all queries. Points are time buckets (bucket, aggregate_note); rates are over each collection's STORED sample interval, and a collection whose interval was unknowable - a restart or counter reset, or the window's first collection when none was stored - is left out rather than counted as 0 (unrated_collections counts them; a point with nothing else carries null rates, unrated_points). Lite has one tier - nothing is rolled up." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryDurationTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var grain = Bucketed(bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
            var startUtc = windowEnd.AddHours(-hours_back);
            var points = await dataService.GetBucketedQueryDurationTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, bucketMinutes);

            if (points.Count == 0)
            {
                /* Same two states again, same words as Darling's twin. The probe reads v_query_stats
                   because THIS trend does; Darling's probes the base table because ITS trend does. Each
                   probe follows its own read — what the caller sees is one sentence, not two. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await dataService.HasAnyQueryStatAsync(resolved.ServerId)
                    ? EmptyStatus(
                        "empty",
                        $"No query samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected query stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.",
                        startUtc, windowEnd, grain)
                    : EmptyStatus(
                        "unavailable",
                        $"No query stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the query_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_top_queries_by_cpu will be equally empty until it does.",
                        startUtc, windowEnd, grain);
            }

            /* The two siblings below serialize through the SAME helper, so the three Performance-Trends
               reads cannot advertise three different field sets for one shape. */
            return SerializeTrend(resolved.ServerName, hours_back, startUtc, windowEnd, points, grain,
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_procedure_duration_trend"), Description("Gets a time-series of stored-procedure elapsed_ms_per_second and executions_per_second, summed across every procedure and charged to the whole call rather than smeared across its statements the way query_stats attributes work. Points end at as_of. unrated_points, unrated_collections, the empty/unavailable split and window_truncated (a retention floor, not a page cut) all follow get_query_duration_trend exactly. <<GUIDE>> Gets a time-series of stored-procedure elapsed time per second and executions per second over time, summed across every procedure. The sibling of get_query_duration_trend, and NOT a duplicate of it: query_stats attributes a procedure's work to the individual statements inside it, so a procedure that got slower is smeared across however many statements it runs. This charges the whole call to the procedure. Read the two together to tell an ad-hoc SQL regression from a procedure regression. Points, rates and unrated collections (unrated_points, unrated_collections) follow get_query_duration_trend exactly." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetProcedureDurationTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var budget = TrendBudget.Mcp(TrendBuckets.DurationMaxPoints);
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var grain = Bucketed(bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
            var startUtc = windowEnd.AddHours(-hours_back);
            var points = await dataService.GetBucketedProcedureDurationTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, bucketMinutes);
            if (points.Count == 0)
            {
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "procedure_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyTrendAsync(
                    dataService.HasAnyProcedureStatAsync(resolved.ServerId), resolved.ServerName, hours_back,
                    startUtc, windowEnd, grain,
                    "stored-procedure",
                    "Check that collection is running and that the server is enabled. A server that genuinely runs no stored procedures also lands here, and that is a real answer rather than a fault.");
            }

            return SerializeTrend(resolved.ServerName, hours_back, startUtc, windowEnd, points, grain,
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_procedure_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_duration_trend"), Description("Gets a time-series of Query Store duration and executions per second, summed across every query, each interval counted once, at the hour it ran. not_collected: engine cannot run Query Store. unavailable: never sampled here. empty: quiet on Lite always; on Darling, empty can also be a rollup coverage gap (window predates the corrected rollup, run --backfill-rollups). A point with no earlier point to rate against has null rates, never 0 (unrated_points, unrated_note says why). window_truncated marks the retention floor, not a page cut; effective_start says where the answer begins. <<GUIDE>> Gets a time-series of Query Store duration per second and executions per second over time, summed across every query. Where get_query_duration_trend reads the plan cache and loses everything an eviction or a restart takes with it, this reads Query Store, which persists per interval - so it is the series that survives a failover and the one to reach for when a regression is older than the cache. Each interval is counted once, at the hour the work ran. Every point is a rate over the gap since the PREVIOUS point, so the window's first collection - which has no previous one to difference against - carries null rates: unknowable, never reported as 0 (unrated_points counts them, unrated_note says why)." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryStoreDurationTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var startUtc = windowEnd.AddHours(-hours_back);
            var points = await dataService.GetQueryStoreDurationTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (points.Count == 0)
            {
                /*
                    The one empty answer here that is NOT about the collector: Query Store can be off on
                    every database on the instance. A server with no Query Store data is not a server with
                    no slow queries, so the message names that cause first.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_store");
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyTrendAsync(
                    dataService.HasAnyQueryStoreStatAsync(resolved.ServerId), resolved.ServerName, hours_back,
                    startUtc, windowEnd, PerInterval,
                    "Query Store",
                    "Query Store may be OFF on this server's databases — that, not an absence of slow queries, is the usual cause. Check QUERY_STORE = ON per database, then that collection is running for this server.");
            }

            return SerializeTrend(resolved.ServerName, hours_back, startUtc, windowEnd, points, PerInterval,
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_duration_trend", ex);
        }
    }

    /// <summary>
    /// What one point of a Performance-Trends answer is (#3897): the <c>bucket</c> word, the width in minutes, and
    /// the prose a rolled-up point owes its reader — null on a series that is not time-bucketed. Darling's
    /// <c>TrendDisclosure</c> carries the same three facts under the same keys.
    /// </summary>
    private sealed record TrendGrain(string Bucket, int? Minutes, string? AggregateNote);

    /// <summary>The grain of a bucketed plan-cache trend (#3897).</summary>
    private static TrendGrain Bucketed(int bucketMinutes, bool requested, int autoPoints) =>
        new(TrendBuckets.Word(bucketMinutes), bucketMinutes, TrendBuckets.AggregateNote(bucketMinutes, requested, autoPoints));

    /// <summary>The grain of the Query Store series, whose points sit at runtime-interval starts: not bucketed.</summary>
    private static readonly TrendGrain PerInterval = new("per-interval", null, null);

    /// <summary>The grain of get_query_trend's per-collection history of one query: not bucketed.</summary>
    private static readonly TrendGrain PerCollection = new("per-collection", null, null);

    /// <summary>
    /// How far past the requested start the first served point may sit before the answer calls itself
    /// <c>window_truncated</c>. Twin of Darling's <c>DarlingTrendReader.TruncationSlack</c> (#2353, #3541 A2) and
    /// must stay equal to it: the two SKUs' payloads are one contract, and a window that one SKU calls
    /// truncated and the other does not is a divergence about the same data. Ninety minutes: a raw series
    /// legitimately opens a collection cadence or two late; anything past that means the store did not hold
    /// the window's head (Lite's per-collector <c>retention_days</c> purge, or a server added mid-window).
    /// </summary>
    internal static readonly TimeSpan TruncationSlack = TimeSpan.FromMinutes(90);

    /// <summary>
    /// The disclosure block every Performance-Trends payload carries (#3541 A2), written in the same key order
    /// on both SKUs and on both branches. Lite has ONE tier — DuckDB keeps raw rows for the collector's whole
    /// <c>retention_days</c>, nothing rolls them up — so <c>source</c> is always <c>raw</c> and
    /// <c>aggregate_note</c> is always null here; what varies is <c>effective_start</c>, the first point the
    /// store actually held for the window, and <c>window_truncated</c>, true when that head sits more than
    /// <see cref="TruncationSlack"/> after what was asked for — the WINDOW floor, spelled apart from the page
    /// dialect's <c>truncated</c> since #3653 item 17 (this file's paged tools mean "limit bit" by that word;
    /// this block means "the store did not hold the head", and no limit changes it). Darling's twin publishes the same keys with
    /// Darling's truth (<c>raw</c>, <c>hourly</c> or <c>rollup+raw</c>), so a client reads one shape across
    /// SKUs even though the depth behind it differs. Emitted as an ordered dictionary rather than an
    /// anonymous type so the data envelope and the empty envelope are built by the same code.
    /// </summary>
    private static void WriteDisclosure(
        Dictionary<string, object?> envelope, DateTime? firstPointUtc, DateTime startUtc, DateTime windowEndUtc, TrendGrain grain)
    {
        var effectiveStart = firstPointUtc ?? startUtc;
        envelope["source"] = "raw";
        /* The store's own frame (naive UTC, Kind=Unspecified), so it prints exactly like the points' `time`
           beside it — the requested start arrives Kind=Utc and would otherwise carry a trailing Z. */
        envelope["effective_start"] = DateTime.SpecifyKind(effectiveStart, DateTimeKind.Unspecified).ToString("o");
        envelope["effective_hours_back"] = Math.Round((windowEndUtc - effectiveStart).TotalHours, 1);
        /* #3653 item 17: the window floor under its own key, at the same position Darling's TrendDisclosure.WriteTo
           writes it. Darling.Tests' McpPayloadContractCensusTests sweeps this file too and fails a bare
           `truncated` beside `effective_hours_back`; the literal stays a literal so that sweep can see it. */
        envelope["window_truncated"] = firstPointUtc is DateTime first && first > startUtc + TruncationSlack;
        envelope["bucket"] = grain.Bucket;
        /* #3897: the width beside its word, at the position Darling's TrendDisclosure.WriteTo writes it; null on the
           Query Store series, which is not time-bucketed. */
        envelope["bucket_minutes"] = grain.Minutes;
        envelope["aggregate_note"] = grain.AggregateNote;
    }

    /// <summary>
    /// The one payload shape the three Performance-Trends siblings share, so a caller can chart them on one
    /// axis without learning three field names. Darling serializes the same fields.
    /// <para><c>execution_count</c> and <c>executions_per_second</c> are the SAME quantity. The first
    /// shipped truncated to an integer, which on a quiet server turns 0.4 executions a second into a
    /// reported ZERO - an idle server, when the truth was a slow one. It is kept so a consumer reading it
    /// does not break; read <c>executions_per_second</c>.</para>
    /// <para><c>value</c> and <c>elapsed_ms_per_second</c> are the same quantity too (#3541): a bare
    /// <c>value</c> named no unit, and a reasoning agent charted it as whatever it guessed. The named field is
    /// the one to read; <c>value</c> stays for the consumer already reading it, on the precedent above.</para>
    /// </summary>
    private static string SerializeTrend(
        string serverName, int hours_back, DateTime startUtc, DateTime windowEndUtc, List<QueryTrendPoint> points, TrendGrain grain,
        IReadOnlyList<BaselineDiscontinuity> discontinuities)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hours_back,
        };
        /* effective_start is the first COLLECTION the store held — on a bucketed point its first collection, not the
           bucket's start (#3897). */
        WriteDisclosure(envelope, points.Count > 0 ? points[0].FirstCollectionTime ?? points[0].CollectionTime : null, startUtc, windowEndUtc, grain);
        /* #3541 A12: a point with no rate is published as null, never as 0, and the envelope says how many
           and why. Two whys since #3695 / v61 (#3653 A11): the plan-cache trends read the STORED interval,
           so a restart collection (stored 0) is unrated beside the first-in-window LAG case; the Query Store
           trend stores no interval and only hits the second arm, and the sentence stays true there. Same keys
           and the same sentence as Darling's twin, byte-identical — pinned by McpMissMessageParityPinTests. */
        var unrated = points.Count(p => !p.HasRate);
        var unratedCollections = points.Sum(p => p.UnratedInBucket ?? (p.HasRate ? 0 : 1));
        envelope["unrated_points"] = unrated;
        /* #3897: a bucket leaves an unknowable collection out of its rates, so the collections are counted apart
           from the points — a restart inside a rated bucket is still reported. Darling's key, at Darling's position. */
        envelope["unrated_collections"] = unratedCollections;
        envelope["unrated_note"] = unratedCollections == 0
            ? null
            : $"{unratedCollections} collection(s) had no knowable rate: a rate is a collection's work divided by the seconds it accrued over, and that denominator is unknowable two ways — the collection's STORED sample interval is 0 (a restart or counter reset: the collector could not difference its two snapshots, so the zeros beside it were never measured), or the collection is rated against the PREVIOUS one and has none inside the window (the window's first collection where no interval was stored, or one landing in the same second as its predecessor). Unknowable is not 0 — such a collection is left out of its point's rates rather than counted as zero, and a point that held nothing else carries null rates ({unrated} here).";
        envelope["trend"] = points.Select(p => new
        {
            time = p.CollectionTime.ToString("o"),
            value = p.Value,
            elapsed_ms_per_second = p.Value,
            execution_count = p.ExecutionCount,
            executions_per_second = p.ExecutionsPerSecond,
            /* #3897: the bucket's worst single collection; null where a point is not a bucket of collections. */
            peak_elapsed_ms_per_second = p.PeakElapsedMsPerSecond,
        });
        /* #3653 A5: trailing, after the points, on the data envelope only — the window's baseline
           discontinuities, the same key and shape Darling's SerializeTrend writes (BaselineDiscontinuities). */
        envelope[BaselineDiscontinuities.PayloadKey] = BaselineDiscontinuities.ToPayload(discontinuities);

        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// <see cref="McpHelpers.Status"/> with the disclosure block beside <c>status</c> and <c>message</c>, so
    /// an empty Performance-Trends answer carries the same six keys the data envelope does (#3541 A2) — a
    /// caller reads <c>source</c> without first checking whether it got data.
    /// </summary>
    private static string EmptyStatus(string status, string message, DateTime startUtc, DateTime windowEndUtc, TrendGrain grain)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["status"] = status,
            ["message"] = message,
        };
        WriteDisclosure(envelope, null, startUtc, windowEndUtc, grain);
        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// The two-branch empty answer the two new Performance-Trends siblings share (#2484), word for word with
    /// Darling's <c>DarlingMcpTrendTools.QuietWindowMessage</c> / <c>NeverSampledMessage</c> and phrased like
    /// the get_query_duration_trend answer that already ships -- a user moving between the SKUs, or between
    /// the three trends, must not be told a different story about the same state. Lite has no third state:
    /// nothing here drops a window's head behind a tier boundary, so "quiet, widen hours_back" is always
    /// true of a sampled server with nothing in the window (Darling's routed twins add the dropped-or-
    /// unmaterialized state, which only a tiered store can be in).
    /// </summary>
    private static async Task<string> EmptyTrendAsync(
        Task<bool> probe, string serverName, int hours_back, DateTime startUtc, DateTime windowEndUtc, TrendGrain grain,
        string what, string checkThis)
    {
        var everSampled = await probe;
        return everSampled
            ? EmptyStatus(
                "empty",
                $"No {what} samples were recorded for {serverName} in the last {hours_back} hour(s). This server HAS been sampled before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.",
                startUtc, windowEndUtc, grain)
            : EmptyStatus(
                "unavailable",
                $"No {what} samples have EVER been recorded for {serverName}. This is not an empty window — nothing at all has been stored for this server, so it is NOT a quiet server. {checkThis}",
                startUtc, windowEndUtc, grain);
    }

    [McpServerTool(Name = "get_query_trend"), Description("Gets a time-series of performance metrics for a specific query identified by its query_hash. Use this after identifying a problematic query from get_top_queries_by_cpu or get_query_store_top to see how it has changed over time." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The query_hash value from get_top_queries_by_cpu or get_query_store_top.")] string query_hash,
        [Description("The database name the query belongs to.")] string database_name,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetQueryStatsHistoryAsync(resolved.ServerId, database_name, query_hash, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "query_stats")
                    ?? McpHelpers.Status("empty", $"No history found for query_hash '{query_hash}' in database '{database_name}' within the last {hours_back} hours.");
            }

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                execution_count = r.DeltaExecutions,
                cpu_ms = Math.Round(r.DeltaCpuMs, 2),
                elapsed_ms = Math.Round(r.DeltaElapsedMs, 2),
                avg_cpu_ms = Math.Round(r.AvgCpuMs, 2),
                avg_elapsed_ms = Math.Round(r.AvgElapsedMs, 2),
                logical_reads = r.DeltaLogicalReads,
                logical_writes = r.DeltaLogicalWrites,
                physical_reads = r.DeltaPhysicalReads,
                rows = r.DeltaRows,
                spills = r.DeltaSpills,
                min_dop = r.MinDop,
                max_dop = r.MaxDop,
                query_plan_hash = r.QueryPlanHash
            });

            /* The same disclosure block Darling's get_query_trend has carried since #2353 and the
               duration-trend trio carries on both SKUs since #3541 A2 — with Lite's truth: one raw tier,
               per-collection, the first point the store held as effective_start. Left as the one tool where
               the SKUs' envelopes disagreed until a review of the trio's change pointed at it. */
            var envelope = new Dictionary<string, object?>
            {
                ["server"] = resolved.ServerName,
                ["database_name"] = database_name,
                ["query_hash"] = query_hash,
                ["hours_back"] = hours_back,
            };
            WriteDisclosure(envelope, rows[0].CollectionTime, windowEnd.AddHours(-hours_back), windowEnd, PerCollection);
            envelope["data_points"] = rows.Count;
            envelope["trend"] = result;
            /* #3653 A5: the window's baseline discontinuities as the trailing key — see BaselineDiscontinuities. */
            envelope[BaselineDiscontinuities.PayloadKey] = BaselineDiscontinuities.ToPayload(
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd));

            return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_trend", ex);
        }
    }
}
