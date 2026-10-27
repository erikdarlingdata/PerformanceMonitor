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
    [McpServerTool(Name = "get_top_queries_by_cpu"), Description("Gets expensive cached queries from sys.dm_exec_query_stats, ranked by CPU over a window ending at as_of. Filters (database_name, parallel_only, min_dop) apply before the top-N cap: filter_applied names the floor in force, and an empty page under it is the window's real answer, not a miss. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes, not windowed; cpu_attribution's ratio is omitted, not invented, when its inputs are missing. window_truncated marks a window floor, not a page cut; effective_start says where the answer begins. Row cap is top (not limit). <<GUIDE>> Gets expensive queries from sys.dm_exec_query_stats (plan cache). Best for: currently cached queries with detailed per-execution stats, DOP, spilled pages (total_spills counts 8 KB pages written to tempdb, not spill events), and query_hash for trending. Returns query_hash, query_plan_hash, sql_handle, plan_handle, and host_object (the hosting procedure/function for proc-hosted statements, null for ad-hoc) — groups key on (database, query_hash, host_object), so INSERT...EXEC callers in different procedures report separately with their own text. distinct_texts counts statement texts merged into a group (>1 = ad-hoc literal variants or pre-upgrade history; query_text is one representative, 0 means no stored text for the group). Supports database and parallelism filtering; every filter is applied IN the query before the ranking and the cap, so the page is the top-N of the FILTERED population (filter_applied names the parallelism floor in force, null when none), and an empty page under parallel_only/min_dop is the window's answer rather than a page artefact. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes for the plan's time in cache (same semantics as max_dop), not windowed — totals and avgs are windowed deltas; rows where an extreme provably predates the window carry extremes_note. max_dop comes from sys.dm_exec_query_stats and is a lifetime-max for the plan's time in cache, so a plan compiled before MAXDOP was lowered keeps reporting the old higher value until it is evicted or recompiled; confirm current parallelism with analyze_query_plan, which reads the actual plan. Reads the raw tier only, which on a store with a lowered retention_days or less history than the window can be shorter than hours_back: window_truncated is the store's retention floor, not a page cut, and effective_start / effective_hours_back say where the answer begins." + McpHelpers.WindowTruncatedDescription + " " + McpToolGuideTopics.CpuTimeExtremesAndAttribution)]
    public static async Task<string> GetTopQueriesByCpu(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of rows to return. The parameter is top, not limit. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("If true, only return queries whose cached plan has EVER run at DOP > 1 (a LIFETIME max_dop; can read stale after a MAXDOP change). See the tool's reading guide.")] bool parallel_only = false,
        [Description("Minimum DOP to filter on. Implies parallel filtering. Filters the same lifetime-max value as parallel_only, not current parallelism.")] int min_dop = 0,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(QueryTagReadScope.ParameterDescription)] string? tag = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            /* #5992: the query tag filter. A name no tag carries is an empty answer that says so. */
            var tagScope = await dataService.GetQueryTagReadScopeAsync(resolved.ServerId, tag);
            if (tagScope.Unknown)
            {
                return McpHelpers.Status("empty", QueryTagReadScope.UnknownTagMessage(tagScope.Requested!));
            }

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

            /* #5984 round 1 (Darling's twin): a tag that cut the population is named in filter_applied beside the parallel floor, so a
               tag-only call never publishes null over a population the tag shaped. A call without a tag is unchanged. */
            if (tagScope.Named is { } queryTagName)
            {
                var tagText = $"query tag '{queryTagName}' (applied in SQL before the top-{top} ranking)";
                filterApplied = filterApplied is null ? tagText : filterApplied + "; " + tagText;
            }

            /* #4793: the ranking read floors last_execution_time, a stamp on THIS server's wall clock, at the
               window's start, so the floor needs THIS server's clock. It used to get none and floored at the
               UTC number: west of UTC a query last run early in the window was dropped, east of UTC one from
               before the window was kept. See McpServerLocalWindow. */
            var serverClock = await McpServerLocalWindow.ClockForAsync(dataService, resolved.ServerId);

            var requestedStart = nowUtc.AddHours(-hours_back);
            var rows = await dataService.GetTopQueriesByCpuAsync(resolved.ServerId, hours_back, top, serverClock: serverClock, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name }, asOfUtc: windowEnd, minMaxDop: minMaxDop, tags: tagScope);
            if (rows.Count == 0)
            {
                if (tagScope.Active)
                {
                    return McpHelpers.Status("empty", $"No query-stats group on {resolved.DisplayName} in the last {hours_back} hour(s) carries query tag '{tagScope.Requested}'. The tag filter was applied in SQL before the top-{top} ranking.");
                }

                /* A filtered miss is not a collection miss — same words as Darling's twin. */
                if (minMaxDop > 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No query-stats group on {resolved.DisplayName} in the last {hours_back} hour(s) has a cached plan with lifetime max_dop >= {minMaxDop}. The filter was applied in SQL over the whole window, so this is the window's answer rather than a page artefact — drop parallel_only / min_dop to see the unfiltered ranking, or confirm current parallelism with analyze_query_plan.",
                        new { filter_applied = filterApplied });
                }

                /* #6127 (Darling's twin): a window that stored snapshots but none with a measured delta is not a collection miss. */
                var (snapshots, newestMeasured) = await dataService.GetQueryStatsSnapshotProbeAsync(
                    resolved.ServerId, hours_back, asOfUtc: windowEnd, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name });
                if (snapshots > 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        McpHelpers.SnapshotsWithoutDeltaMessage(resolved.DisplayName, hours_back, snapshots, newestMeasured, windowEnd),
                        new
                        {
                            snapshots_in_window = snapshots,
                            snapshots_with_measured_delta = 0,
                            newest_measured_delta_at = McpHelpers.FormatEffectiveStart(newestMeasured),
                            suggested_hours_back = newestMeasured is { } at ? McpHelpers.SuggestedHoursBack(at, windowEnd) : (int?)null
                        });
                }

                /* #6226 (Darling's twin): the collector looked and nothing ran. query_stats stores only what executed since its
                   watermark, so a window in which it ran successfully and stored no row is a measurement (empty), not a gap
                   (unavailable). After not_collected, which names an engine that never runs the collector. */
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_stats")
                    ?? EmptyCollectorRan(
                        await dataService.GetCollectorRunFactsAsync(resolved.ServerId, "query_stats", hours_back, asOfUtc: windowEnd),
                        "query_stats", hours_back, "executed statements", database_name)
                    ?? McpHelpers.Status("unavailable", "No query stats available for the specified time range.");
            }

            /* #4231: what the window ACTUALLY reached, from the SAME relation (v_query_stats) the ranking read
               above just used — a raw-only table with no rollup fallback, so a lowered retention_days or a
               young install can cut a "Last 7 days" ask well short of 7 days with nothing on the page saying
               so. One probe, shared with the Queries-tab grid (LocalDataService.GetQueryWindowFloorAsync). */
            var floor = await dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, resolved.ServerId, requestedStart, nowUtc);
            var effectiveStart = EffectiveWindowStart(floor, requestedStart);
            var truncated = IsWindowTruncated(floor, requestedStart);

            /* #2320: what fraction of the box's measured CPU the RETURNED rows explain — numerator is
               the caller-visible ranking (post top-N, post filters), denominator is measured, and the
               ratio is omitted rather than invented when a denominator piece is missing. One nowUtc
               backs the aggregate read AND the ratio math, and the two independent reads run
               concurrently (review catches; Darling has both by construction). */
            var cpuAggregateTask = dataService.GetCpuWindowAggregateAsync(resolved.ServerId, requestedStart, nowUtc);
            var propertiesTask = dataService.GetLatestServerPropertiesAsync(resolved.ServerId);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            /* The core count is what the server is given: on an Azure SQL Database the stored cpu_count is the schedulers it can see,
               which can be more than its vCores, so this divides by its vcore_count, or omits the ratio for a DTU-model objective or
               an elastic pool (see CpuAttribution). */
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuMs) / 1000.0,
                requestedStart, nowUtc,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.EngineEdition, properties?.CpuCount ?? 0, properties?.VcoreCount);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_hash = r.QueryHash,
                tag = tagScope.TagOfHash(r.QueryHash),
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
                query_text = McpHelpers.TruncateStatement(r.QueryText, 2000),
                distinct_texts = r.DistinctTexts,
                text_note = r.DistinctTexts > 1
                    ? $"this group blends {r.DistinctTexts} distinct statement texts (ad-hoc literal variants; or history predating the host-object split for INSERT...EXEC callers); query_text is one representative"
                    : null
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* #4231: what was served, beside what was asked for — query_stats is raw-only (no rollup
                   fallback), so a lowered retention_days or a young install can serve less than hours_back
                   with nothing else on the payload saying so. */
                effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                effective_hours_back = Math.Round((nowUtc - effectiveStart).TotalHours, 1),
                window_truncated = truncated,
                truncation_note = truncated
                    ? "The window reaches further back than this server's raw query_stats retains (or this server has been monitored for less time than that), so the older part of it was not read."
                    : null,
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

    /// <summary>#6226: the <c>empty</c> envelope for a ranking over a window the collector ran in and stored nothing, with
    /// <c>successful_runs_in_window</c> beside the sentence; null when it has no successful run in the window. Twin of
    /// <c>DarlingMcpDataTools.EmptyCollectorRan</c>; the words are shared in <see cref="CollectorRunFacts.RanFoundNothing"/>.</summary>
    internal static string? EmptyCollectorRan(
        CollectorRunFacts facts, string collectorName, int hoursBack, string found, string? databaseName)
    {
        var message = facts.RanFoundNothing(collectorName, hoursBack, found,
            string.IsNullOrEmpty(databaseName) ? null : McpDatabaseSelection.Scope(new[] { databaseName }));
        return message is null ? null : McpHelpers.Status("empty", message, new { successful_runs_in_window = facts.SuccessfulRunsInWindow });
    }

    /// <summary>#5633 item 3: the empty-page envelope of get_top_procedures_by_cpu, from the collector's own run history. Two cycles only while
    /// the collector is new; <c>empty</c> once it has run and found no procedure in the window. Words in <see cref="CollectorRunFacts"/>,
    /// shared with Darling's twin.</summary>
    internal static string EmptyProcedureStatsStatus(CollectorRunFacts facts, int hoursBack, int databasesLeftOut = 0)
    {
        var (status, message) = facts.ProcedureStatsEmpty(hoursBack, databasesLeftOut);
        return McpHelpers.Status(status, message);
    }

    [McpServerTool(Name = "get_top_procedures_by_cpu"), Description("Gets the most expensive stored procedures ranked by total CPU time over a window ending at as_of. Delta-based: requires ~30 minutes after adding a new server before data appears. min/max_cpu_ms and min/max_elapsed_ms are LIFETIME extremes, not windowed (extremes_note flags a provably stale one); cpu_attribution's ratio is omitted, not invented, when its inputs are missing. window_truncated marks a window floor, not a page cut; effective_start / effective_hours_back give the reach actually served. <<GUIDE>> Shows execution counts, CPU/elapsed times, and I/O metrics. Delta-based: requires ~30 minutes after adding a new server before data appears. Reads the raw tier only, which on a store with a lowered retention_days or less history than the window can be shorter than hours_back." + McpHelpers.WindowTruncatedDescription + " " + McpToolGuideTopics.CpuTimeExtremesAndAttribution)]
    public static async Task<string> GetTopProceduresByCpu(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Number of top procedures. Default 20.")] int top = 20,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(QueryTagReadScope.ParameterDescription)] string? tag = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            /* #5992: the query tag filter. A name no tag carries is an empty answer that says so. */
            var tagScope = await dataService.GetQueryTagReadScopeAsync(resolved.ServerId, tag);
            if (tagScope.Unknown)
            {
                return McpHelpers.Status("empty", QueryTagReadScope.UnknownTagMessage(tagScope.Requested!));
            }

            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var topError = McpHelpers.ValidateTop(top, "top");
            if (topError != null) return topError;

            /* Same pre-read capture as the queries tool — the skew shrinks to call-entry overhead. */
            var nowUtc = windowEnd;
            /* #4793: the same last_execution_time floor as get_top_queries_by_cpu, on this server's clock. */
            var serverClock = await McpServerLocalWindow.ClockForAsync(dataService, resolved.ServerId);

            var requestedStart = nowUtc.AddHours(-hours_back);
            /* #5633 item 4: with no database named, rank without the databases the server DROPPED (named by an earlier database_states
               snapshot and absent from the newest), not those dropped since: their rows would fill the page. A database no snapshot ever
               listed (created after the newest one) is still read. Unknown (no snapshot) leaves out nothing. A named database is read as asked. */
            var droppedDatabases = string.IsNullOrEmpty(database_name)
                ? await dataService.GetDroppedDatabasesAsync(resolved.ServerId, asOfUtc: string.IsNullOrWhiteSpace(as_of) ? null : nowUtc, windowStartUtc: requestedStart)
                : null;
            var rows = await dataService.GetTopProceduresByCpuAsync(resolved.ServerId, hours_back, top, serverClock: serverClock, databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name }, asOfUtc: windowEnd, excludeDatabaseNames: droppedDatabases, tags: tagScope);
            /* #5633 review (finding D): the narrowing is disclosed, never silent (Darling's twin does the same, in the same words). The names
               it left out ride the envelope of an empty page and of a page with rows, and an empty page's sentence claims only the databases
               the server still has, so "the collector recorded nothing" is never said over a window with rows in a dropped database. */
            var leftOut = DatabasesLeftOut.Distinct(droppedDatabases);
            if (rows.Count == 0 && tagScope.Active)
            {
                return McpHelpers.WithDatabasesLeftOut(McpHelpers.Status("empty", $"No procedure on {resolved.DisplayName} in the last {hours_back} hour(s) carries query tag '{tagScope.Requested}'. The tag filter was applied in SQL before the top-{top} ranking."), leftOut)!;
            }

            if (rows.Count == 0)
            {
                return McpHelpers.WithDatabasesLeftOut(
                    await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "procedure_stats")
                    ?? EmptyProcedureStatsStatus(
                        await dataService.GetCollectorRunFactsAsync(resolved.ServerId, "procedure_stats", hours_back, asOfUtc: windowEnd),
                        hours_back, leftOut?.Count ?? 0), leftOut)!;
            }

            /* #4231: what the window ACTUALLY reached, from the SAME relation (v_procedure_stats) the ranking
               read above just used. One probe, shared with the Queries-tab grid. */
            var floor = await dataService.GetQueryWindowFloorAsync(QueryWindowRelation.ProcedureStats, resolved.ServerId, requestedStart, nowUtc);
            var effectiveStart = EffectiveWindowStart(floor, requestedStart);
            var truncated = IsWindowTruncated(floor, requestedStart);

            /* #2320: same attributed-CPU disclosure as the queries tool — one shared computation, one
               nowUtc backing aggregate and ratio, same concurrent independent reads. */
            var cpuAggregateTask = dataService.GetCpuWindowAggregateAsync(resolved.ServerId, requestedStart, nowUtc);
            var propertiesTask = dataService.GetLatestServerPropertiesAsync(resolved.ServerId);
            await Task.WhenAll(cpuAggregateTask, propertiesTask);
            var cpuAggregate = await cpuAggregateTask;
            var properties = await propertiesTask;
            /* The core count is what the server is given: on an Azure SQL Database the stored cpu_count is the schedulers it can see,
               which can be more than its vCores, so this divides by its vcore_count, or omits the ratio for a DTU-model objective or
               an elastic pool (see CpuAttribution). */
            var attribution = CpuAttribution.Compute(
                rows.Sum(r => r.TotalCpuMs) / 1000.0,
                requestedStart, nowUtc,
                cpuAggregate.SampleCount, cpuAggregate.FirstSample, cpuAggregate.LastSample, cpuAggregate.AvgSqlCpuPercent,
                properties?.EngineEdition, properties?.CpuCount ?? 0, properties?.VcoreCount);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                full_name = r.FullName,
                tag = tagScope.TagOfObject(r.DatabaseName, r.SchemaName, r.ObjectName),
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

            return WithTagFilter(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* #4231: what was served, beside what was asked for — procedure_stats is raw-only (no rollup
                   fallback), so a lowered retention_days or a young install can serve less than hours_back
                   with nothing else on the payload saying so. */
                effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                effective_hours_back = Math.Round((nowUtc - effectiveStart).TotalHours, 1),
                window_truncated = truncated,
                truncation_note = truncated
                    ? "The window reaches further back than this server's raw procedure_stats retains (or this server has been monitored for less time than that), so the older part of it was not read."
                    : null,
                cpu_attribution = new
                {
                    ranked_cpu_seconds = attribution.RankedCpuSeconds,
                    sql_cpu_seconds_in_window = attribution.SqlCpuSecondsInWindow,
                    attributed_cpu_ratio = attribution.AttributedCpuRatio,
                    note = attribution.Note
                },
                /* #5633 review (finding D): null when the read left no database out. */
                databases_left_out = DatabasesLeftOut.Named(leftOut),
                databases_left_out_note = DatabasesLeftOut.Note(leftOut),
                procedures = result
            }, tagScope.Named);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_top_procedures_by_cpu", ex);
        }
    }

    /// <summary>
    /// Serializes a payload and, only when a tag shaped the read, adds a <c>tag_filter</c> member naming it: the tag as the caller gave it,
    /// or <c>untagged</c> (the twin of Darling's #5984 payload member). A call without a tag is unchanged.
    /// </summary>
    internal static string WithTagFilter(object payload, string? tagName)
    {
        if (tagName is null)
        {
            return JsonSerializer.Serialize(payload, McpHelpers.JsonOptions);
        }

        var node = JsonSerializer.SerializeToNode(payload, McpHelpers.JsonOptions)!.AsObject();
        node["tag_filter"] = tagName;
        return node.ToJsonString(McpHelpers.JsonOptions);
    }

    /// <summary>
    /// #4198: the default page's <c>query_text</c> preview length. Mirrors
    /// <c>DarlingMcpDataTools.QueryTextPreviewLength</c> -- see that constant's remarks for why 400.
    /// </summary>
    private const int QueryTextPreviewLength = 400;

    /// <summary>
    /// #6181: Lite's twin of Darling's first-interval answer for a Query Store top read that found nothing; null when a Query Store
    /// sample was ever stored or the health state is unknown or off, so the caller keeps its own text.
    /// </summary>
    private static async Task<string?> FirstIntervalStatusAsync(
        LocalDataService dataService, int serverId, string serverName, string? databaseName)
    {
        try
        {
            if (await dataService.HasAnyQueryStoreStatAsync(serverId))
            {
                return null;
            }
        }
        catch (Exception)
        {
            return null;
        }

        var message = await McpRuntimePrecondition.QueryStoreFirstIntervalMessageAsync(dataService, serverId, serverName, databaseName);
        return message is null ? null : McpHelpers.Status("unavailable", message);
    }

    [McpServerTool(Name = "get_query_store_top"), Description("Cost-ranked top Query Store queries (heaviest first), not time-ordered. Requires Query Store enabled on target databases. window_truncated marks a window floor, not a page cut — no limit changes it — because stored history can be shorter than asked; effective_start / effective_hours_back give the reach actually served. <<GUIDE>> Gets expensive queries from Query Store (persistent, survives restarts). Best for: historical analysis, queries no longer in plan cache. Requires Query Store enabled on target databases. Supports database and module filtering. Rows are per Query Store execution outcome (execution_type: Regular, Aborted, Exception): a plan with aborted executions returns one row per outcome, each with its own counts and averages. The execution_type filter keeps one outcome, and module_name keeps one module: the exact, case-sensitive schema-qualified name the collector records (get_top_procedures_by_cpu's full_name; Adhoc for ad-hoc statements, Unknown for an object it could not resolve), applied after interval deduplication and before ranking. When a filter matches nothing but the same read without the filters has rows, the answer is empty (a measured zero), not a Query Store precondition; a module_name miss also carries the window read (effective_start, effective_hours_back, window_truncated) as hints. query_text is a 400-character preview by default (query_text_truncated marks a cut row); full_text=true returns each row's whole statement. The page is also capped at the shared ~32 KB response budget, heaviest rows kept: truncated says rows were cut for that, and budget_note says how many fit (window_truncated stays the window floor)." + McpHelpers.WindowTruncatedDescription)]
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
        [Description("Return each row's full query_text instead of a 400-character preview. Default false.")] bool full_text = false,
        [Description(QueryTagReadScope.ParameterDescription)] string? tag = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            /* #5992: the query tag filter. A name no tag carries is an empty answer that says so. */
            var tagScope = await dataService.GetQueryTagReadScopeAsync(resolved.ServerId, tag);
            if (tagScope.Unknown)
            {
                return McpHelpers.Status("empty", QueryTagReadScope.UnknownTagMessage(tagScope.Requested!));
            }

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

            var nowUtc = windowEnd;
            var requestedStart = nowUtc.AddHours(-hours_back);
            var rows = await dataService.GetQueryStoreTopQueriesAsync(
                resolved.ServerId, hours_back, top,
                databaseNames: string.IsNullOrEmpty(database_name) ? null : new[] { database_name },
                asOfUtc: windowEnd,
                executionType: execution_type,
                moduleName: module_name,
                tags: tagScope);

            /* #4231 (Lite's twin of Darling's #2364): what the window ACTUALLY reached. rows above are the top
               N by COST, so their timestamps say nothing about how far back the read went. query_store_stats
               has no rollup fallback (the only one that keeps query_id/plan_id would approach raw's own size),
               so the honest move is to report what was served rather than echo what was asked for. Deliberately
               unfiltered: the floor is a property of the tier, not narrowed by database/execution_type/module. */
            var floor = await dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, resolved.ServerId, requestedStart, nowUtc);
            var effectiveStart = EffectiveWindowStart(floor, requestedStart);
            var truncated = IsWindowTruncated(floor, requestedStart);

            /* #6074: every empty answer below is also read against the database's newest query_store_health row: when Query
               Store is not READ_WRITE there, the answer says so and names get_query_store_health. The ladder is unchanged. */
            async Task<string> EmptyAnswerAsync()
            {
                if (tagScope.Active)
                {
                    return McpHelpers.Status("empty", $"No Query Store query on {resolved.DisplayName} in the last {hours_back} hour(s) carries query tag '{tagScope.Requested}'. The tag filter was applied in SQL before the top-{top} ranking.");
                }

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
                        /* The module miss hands back the window it read: "did not run" is a claim about that
                           window, and the raw tier may not reach the whole of the one asked for. */
                        : McpHelpers.QueryStoreModuleEmpty(module_name, execution_type, hours_back, database_name, truncated, new
                        {
                            effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                            effective_hours_back = Math.Round((nowUtc - effectiveStart).TotalHours, 1),
                            window_truncated = truncated
                        });

                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_store")
                    /* #2546: the sentence below GUESSES ("may not be enabled"), and it had to, because the
                       read had no way to find out. The store has known all along — query_store_health
                       records actual_state per database every hour for exactly this purpose. Asking it turns
                       a hedge into a fact plus the ALTER DATABASE that fixes it, and it answers for the
                       database this read was scoped to rather than for the server's most flattering one. */
                    ?? await McpRuntimePrecondition.QueryStoreStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, database_name)
                    /* And the collector's own last run, for the case Query Store is on and the collector is
                       the thing that cannot read it. */

                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_store")
                    /* #6181: a server just added stores no Query Store row until its first interval closes. When none was
                       ever stored but Query Store is READ_WRITE and the collector's last run succeeded, say that instead of
                       "may not be enabled". */
                    ?? await FirstIntervalStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, database_name)
                    /* #6226 (Darling's twin): the collector ran in the window and stored nothing, so nothing executed: empty, not a gap. */
                    ?? EmptyCollectorRan(
                        await dataService.GetCollectorRunFactsAsync(resolved.ServerId, "query_store", hours_back, asOfUtc: windowEnd),
                        "query_store", hours_back, "Query Store intervals", database_name)
                    ?? McpHelpers.Status("unavailable", "No Query Store data available. Query Store may not be enabled on target databases.");
            }

            if (rows.Count == 0)
                return await McpRuntimePrecondition.WithQueryStoreNotWritingHintAsync(
                    await EmptyAnswerAsync(), dataService, resolved.ServerId, resolved.DisplayName, database_name);

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                query_id = r.QueryId,
                plan_id = r.PlanId,
                query_hash = r.QueryHash,
                tag = tagScope.TagOfHash(r.QueryHash),
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
                query_text = full_text ? r.QueryText : McpHelpers.TruncateStatement(r.QueryText, QueryTextPreviewLength),
                query_text_truncated = !full_text && r.QueryText != null && r.QueryText.Length > QueryTextPreviewLength
            });

            var topJson = WithTagFilter(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* #4231: what was served, beside what was asked for — hours_back alone is a request echoed
                   back as though it described the data. Same keys, same meaning as Darling's get_query_store_top. */
                effective_start = McpHelpers.FormatEffectiveStart(effectiveStart),
                effective_hours_back = Math.Round((nowUtc - effectiveStart).TotalHours, 1),
                window_truncated = truncated,
                truncation_note = truncated
                    ? "The window reaches further back than this server's raw query_store_stats retains (or this server has been monitored for less time than that), so the older part of it was not read. This tool reads the raw tier only: no rollup carries query_id or plan_id, and plan identity is what it exists to return."
                    : null,
                queries = result
            }, tagScope.Named);

            /* #6131: the page is capped under the shared response budget (top 200 measured 177 KB on the Darling twin, which
               has the same shape), and `truncated` says when it was. It is the budget cut only; the window floor keeps
               window_truncated and its own note. */
            return McpResponseBudget.CapPage(
                System.Text.Json.Nodes.JsonNode.Parse(topJson)!.AsObject(), "queries", "heaviest first", "queries");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_top", ex);
        }
    }

    /// <summary>
    /// #4198: query_text is this tool's own wide field - up to 50 rows of numeric columns plus a full,
    /// unbounded Query Store text each measured 211 KB at default arguments on a busy production store (the
    /// Darling twin's own measurement; the read and its payload shape are identical). Previewed to this
    /// length per row at default (<c>full_text: true</c> opts back in), the same preview-plus-opt-in shape
    /// <c>get_store_query_stats</c> uses for its own <c>full_text</c>. Named apart from get_query_store_top's
    /// preview constant in this class; the Darling twin is
    /// <c>DarlingMcpQueryStoreRegressionTools.QueryTextPreviewLength</c>.
    /// </summary>
    private const int RegressionsQueryTextPreviewLength = 240;

    /* Lite's baseline rows are filtered on collection_time, so a baseline with no hours holds no row and nothing can flag for cost.
       Darling's description differs because its baseline filters on last_execution_time (#6750). */
    [McpServerTool(Name = "get_query_store_regressions"), Description("Finds queries whose Query Store performance got WORSE: recent window (hours_back) vs a fixed 7-day baseline before it. get_query_store_top ranks EXPENSIVE, this ranks CHANGED. Gated: average CPU regressed over 25%. Timeouts or errors that rose also qualify (regression_reason). duration_regression_percent and severity are null, not 0%, when their baseline is 0. additional_duration_ms is the ranking key. empty: no regression, or nothing yet in the baseline window. unavailable: no baseline exists yet. not_collected: this server's engine cannot run Query Store. <<GUIDE>> Finds queries whose Query Store performance got WORSE, by comparing each (database, query_id) group's per-execution cost inside a recent window against its baseline - a FIXED 7-day lookback ending at that window's start (before this it was every capture EVER collected before the window, so its cost tracked how much history the store still retained rather than the window asked for, and the comparison period silently grew on a server with more retention). baseline_start and baseline_end name the period the baseline asks for, and effective_start with truncation_note say where the store's data really starts, so read them to know what was compared. A store that starts inside the baseline holds only part of it, and one that starts at or after baseline_end has an empty baseline, so nothing can be flagged for cost. A regression against something older than the baseline lookback is not caught. Returns baseline vs recent duration and CPU with the regression percent for each (logical reads are not reported, #5506), and beside them the Aborted (timeouts, cancels) and Exception (errors) runs of the same query: for each group the run count, the CPU and duration per execution, and the count per hour, for the baseline and the recent window (the two differ in length, so compare the per-hour figures), the execution-count-weighted extra duration (the ranking key: a 5 ms regression executed a million times outranks a 5-second one executed twice), the plan counts on both sides, and a duration-driven severity band. get_query_store_top answers what is EXPENSIVE; the most expensive query is usually the one that always was. This answers what CHANGED. Rows are kept where Regular average CPU regressed by more than 25% and the query added at least 1 s of CPU over the recent window (its recent runs times the rise in its CPU per run), so a few runs of a cheap query are not listed while one run of a job that got much more expensive is (regression_reason cost), or where the Aborted rate per hour (timeouts) or the Exception rate per hour (errors) in the recent window is at least twice the baseline's, or nonzero where the baseline had none, with at least 3 such runs in the recent window - so a query whose timeouts rose is listed even if its Regular CPU fell, and one 30 s timeout among a thousand fast runs, which barely moves an average, is not hidden. regression_reason is cost, timeouts, errors or a comma-separated combination. Failure-only rows are listed after the cost rows and take at most a third of the limit (more only if cost rows leave room), so a flood of errors cannot push the cost rows out; their severity is HIGH when the rate is at least five times the baseline's (or the baseline had none) and MEDIUM otherwise, never CRITICAL. Cost compares Regular runs only: blending Aborted runs in would hide them. A regression percent whose BASELINE side is 0 has no denominator and is returned as null, with the reason under undefined_percents - never as 0, which would read as no change when the truth is the largest possible one; compare the two absolute figures instead. The ranking key is the absolute, execution-weighted duration delta, which exists whether or not a ratio does, so a null percent never sorts as 0. severity is banded from the duration percent and is null when that percent is. Both sides are WEIGHTED per execution (the summed cost divided by the summed executions, so a busy interval counts for what it ran, not an average of the interval averages), over Regular executions only.")]
    public static async Task<string> GetQueryStoreRegressions(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Size of the RECENT window, in hours back from now. Everything collected before it is the baseline. Default 24.")] int hours_back = 24,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Maximum rows to return, worst first. Default 30, sized to keep a default call under the shared response budget. Read truncated to know whether the window held more.")] int limit = 30,
        [Description("Return each row's full query text instead of a 240-character preview. Default false.")] bool full_text = false,
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
            var windowStart = windowEnd.AddHours(-hours_back);
            var baselineStart = windowStart.AddDays(-LocalDataService.BaselineLookbackDays);
            /* #6654: with no database named, the read leaves out the databases the server DROPPED (named by an earlier database_states
               snapshot, absent from the newest), as get_top_procedures_by_cpu does (#5633 item 4); the names ride the payload. */
            var droppedDatabases = databases is null
                ? await dataService.GetDroppedDatabasesAsync(resolved.ServerId, asOfUtc: string.IsNullOrWhiteSpace(as_of) ? null : windowEnd, windowStartUtc: baselineStart)
                : null;
            var leftOut = DatabasesLeftOut.Distinct(droppedDatabases);
            /* #6750: the store's data start, probed once. The window notice below reports it as effective_start, and the read divides
               each per-hour rate by the hours from it, so the two cannot disagree. */
            var floor = await dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStoreStats, resolved.ServerId, baselineStart, windowEnd);
            var rows = await dataService.GetQueryStoreRegressionsAsync(
                resolved.ServerId, hours_back, limit + 1, databases, asOfUtc: windowEnd, excludedDatabases: droppedDatabases?.ToList(),
                dataStartUtc: EffectiveWindowStart(floor, baselineStart));

            /* #4966: where this server's query_store_stats start, checked against the EARLIER of the two windows this
               tool compares — the baseline's start, not the recent window's: a store that began inside the baseline
               compares against a shorter baseline than baseline_is names, and nothing else on the payload says so.
               The probe reads v_query_store_stats, the view both sides of the comparison read, and is unfiltered by
               database_name (the floor is a property of the table). Read ahead of the empty answers, which carry it
               too: "no query regressed" over a baseline the store only partly holds is not a true negative. */
            var notice = await WindowNoticeAsync(
                () => Task.FromResult(floor),
                baselineStart, windowEnd, "query_store_stats",
                tailFor: effectiveStart => QueryStoreRegressionRules.TailFor(effectiveStart, windowStart),
                emptyAnswer: rows.Count == 0);

            if (rows.Count == 0)
                return McpHelpers.WithDatabasesLeftOut(
                    await EmptyRegressionsAsync(dataService, resolved.ServerId, resolved.DisplayName, hours_back, windowEnd, notice, databases), leftOut)!;

            var truncated = rows.Count > limit;

            return JsonSerializer.Serialize(new
            {
                server = resolved.DisplayName,
                hours_back,
                /* The window floor, always present (false and null when the store covered the baseline). No
                   effective_hours_back: this payload carries a page `truncated`, and the census holds that key apart
                   for the window floor (McpPayloadContractCensusTests), so the reach is the instant. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                database_name,
                /* A fixed lookback ending at the recent window's start, not "every capture ever collected
                   before it" - the baseline no longer grows with retention (Lite's twin of Darling's #4195). */
                baseline_start = baselineStart.ToString("o"),
                baseline_end = windowStart.ToString("o"),
                baseline_is = $"Query Store captures from baseline_start to the recent window's start ({LocalDataService.BaselineLookbackDays} days)",
                gate = "Regular average CPU regressed by more than 25% and the rise in CPU per run times the recent runs added at least 1 s of CPU over the window (cost), or the recent Aborted (timeouts) or Exception (errors) rate per hour is at least twice the baseline's (or nonzero where the baseline had none) with at least 3 such runs",
                /* #6128: every regression that passed the gate, whatever limit asked for; returned_count is this page. */
                regression_count = Math.Max(rows[0].TotalCount, rows.Count),
                returned_count = Math.Min(rows.Count, limit),
                truncated,
                regressions = rows.Take(limit).Select(r => RegressionRowJson(r, full_text)),
                /* #6654: the dropped databases this read left out, in the words get_top_procedures_by_cpu uses. */
                databases_left_out = DatabasesLeftOut.Named(leftOut),
                databases_left_out_note = DatabasesLeftOut.Note(leftOut),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_store_regressions", ex);
        }
    }

    /// <summary>
    /// #4198: the reader's ms/percent/read figures come straight off an <c>AVG()</c> over microsecond
    /// integers, so most rows serialize with a long, meaningless decimal tail (a double's round-trip
    /// representation, not a display one) - real weight across ten numeric fields on up to 30 rows. Rounded
    /// to 2 decimal places here, at the JSON edge only; every existing pin uses whole-number fixtures that
    /// round trip unchanged.
    /// </summary>
    private static double Round(double value) => Math.Round(value, 2);

    private static double? Round(double? value) => value is null ? null : Math.Round(value.Value, 2);

    /// <summary>One regression row. The Regular figures are always there. Each of the Aborted and Exception groups always carries its
    /// counts, so a 0 reads as none; its CPU and duration per execution are left out of a side with no runs (there is no figure to
    /// show), and its runs per hour only on a row whose reason names it, where they are the figure the reason was judged on (#6268,
    /// Darling's #5506). Logical reads are not reported. Darling's twin builds the same row.</summary>
    private static Dictionary<string, object?> RegressionRowJson(QueryStoreRegressionRow r, bool full_text)
    {
        var row = new Dictionary<string, object?>
        {
            ["database_name"] = r.DatabaseName,
            ["query_id"] = r.QueryId,
            /* Banded from the duration percent by the SQL's CASE, whose ELSE is 'LOW' - which for a cost row with NO duration
               ratio is a verdict about a number that does not exist. Null there (#3541 A12). */
            ["severity"] = r.DurationRegressionPercent is null && r.RegressionReason == "cost" ? null : r.Severity,
            /* #6268: why the row is listed: cost, timeouts, errors or a comma-separated combination. */
            ["regression_reason"] = r.RegressionReason,
            ["baseline_duration_ms"] = Round(r.BaselineDurationMs),
            ["recent_duration_ms"] = Round(r.RecentDurationMs),
            ["duration_regression_percent"] = Round(r.DurationRegressionPercent),
            ["baseline_cpu_ms"] = Round(r.BaselineCpuMs),
            ["recent_cpu_ms"] = Round(r.RecentCpuMs),
            ["cpu_regression_percent"] = Round(r.CpuRegressionPercent),
            /* Null percents, and why (#3541 A12): a 0 baseline has no ratio, and the reader used to
               publish that as 0 - "no change" - for the row that changed the most. */
            ["undefined_percents"] = UndefinedPercentNotes(r),
            /* The ranking key, and the one number that says whether this regression MATTERS. It is
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
        row["query_text"] = full_text ? r.QueryTextSample : McpHelpers.TruncateStatement(r.QueryTextSample, RegressionsQueryTextPreviewLength);
        row["query_text_truncated"] = !full_text && r.QueryTextSample.Length > RegressionsQueryTextPreviewLength;
        return row;
    }

    private static void AddGroup(Dictionary<string, object?> row, string name, QueryStoreRunGroup g, bool reasonNamesIt)
    {
        row["baseline_" + name + "_count"] = g.BaselineCount;
        row["recent_" + name + "_count"] = g.RecentCount;
        if (g.BaselineCount > 0)
        {
            row["baseline_" + name + "_cpu_ms"] = Round(g.BaselineCpuMs);
            row["baseline_" + name + "_duration_ms"] = Round(g.BaselineDurationMs);
        }
        if (g.RecentCount > 0)
        {
            row["recent_" + name + "_cpu_ms"] = Round(g.RecentCpuMs);
            row["recent_" + name + "_duration_ms"] = Round(g.RecentDurationMs);
        }
        if (reasonNamesIt)
        {
            row["baseline_" + name + "_per_hour"] = Round(g.BaselinePerHour);
            row["recent_" + name + "_per_hour"] = Round(g.RecentPerHour);
        }
    }

    /// <summary>
    /// Which of a row's two regression percents are undefined, and why (#3541 A12, contract rule 5). Each
    /// percent divides through <c>NULLIF(baseline, 0)</c>, so a NULL means the baseline side was 0 - there is
    /// no denominator, not no change - and the caller is pointed at the absolute pair it can still compare.
    /// Null when every percent is defined, so the common row carries no noise. Darling's twin builds the same
    /// sentences.
    /// </summary>
    private static List<string>? UndefinedPercentNotes(QueryStoreRegressionRow r)
    {
        List<string>? notes = null;
        /* #6268: a row listed only for timeouts or errors can have no Regular runs on one side, so there is no cost comparison:
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
    /// What zero regressions actually means, which is four different things — word for word with Darling.
    /// <para>Only ONE of them is good news, and the other three look identical to it in a bare empty array.
    /// The dangerous one is a server whose entire collected history sits INSIDE the requested window: it has
    /// no BEFORE, so it can never show a regression however badly it regressed, and answering "no
    /// regressions" there is a confident wrong answer rather than a missing one.</para>
    /// <para>#5015: with a <c>database_name</c> filter, the server passing those questions is not enough. The same two
    /// questions are then asked of the filtered database alone, so a database with only one side collected gets the
    /// matching no-baseline or missing-window answer, naming it, and only a database with both sides is the all-clear.</para>
    /// </summary>
    private static async Task<string> EmptyRegressionsAsync(
        LocalDataService dataService, int serverId, string serverName, int hours_back, DateTime windowEnd, McpWindowNotice notice,
        string[]? databases)
    {
        /* The anchor is threaded in rather than resolved again: the coverage probe answers "does a BEFORE
           exist for this window", and a window it computed for itself would be a different one. */
        var (hasBaseline, hasRecent) = await dataService.GetQueryStoreRegressionCoverageAsync(serverId, hours_back, asOfUtc: windowEnd);

        if (!hasBaseline && !hasRecent)
        {
            /* #6181: Query Store on, collector healthy, first interval not closed yet: say so instead of "may be OFF". */
            return await McpEngineCapability.NotCollectedStatusAsync(dataService, serverId, serverName, "query_store")
                ?? (await McpRuntimePrecondition.QueryStoreFirstIntervalMessageAsync(
                        dataService, serverId, serverName, databases is { Length: 1 } ? databases[0] : null)
                    is { } firstInterval ? McpHelpers.Status("unavailable", firstInterval + " There is nothing to compare yet, so this is not a report of zero regressions.") : null)
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
                $"{serverName} has Query Store history from before this window but nothing collected IN the last {hours_back} hour(s), so there is a recent side missing rather than nothing to report. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.",
                notice.AsHints());
        }

        /* #5015: the probe above is the SERVER's, so it answers a database_name filter the same whether the database was
           compared or not, and only a database with BOTH sides collected can be the all-clear. The filter reads the same
           databases the read was asked for, and the answer is decided from that pair with the server-wide answers' own
           statuses: neither side means the filter matched nothing, no baseline is the server-wide no-baseline answer and
           no capture in the window the server-wide missing-window answer, each naming the database. Darling's twin words
           it the same. */
        if (databases is not null)
        {
            var (filteredBaseline, filteredRecent) = await dataService.GetQueryStoreRegressionCoverageAsync(
                serverId, hours_back, asOfUtc: windowEnd, databaseNames: databases);

            if (!filteredBaseline && !filteredRecent)
            {
                return McpHelpers.Status(
                    "empty",
                    $"No Query Store capture on {serverName} in the last {hours_back} hour(s) or in the {LocalDataService.BaselineLookbackDays}-day baseline before it matched database_name '{databases[0]}', so the filter matched nothing and this is NOT the all-clear: no query in that database was compared. Check the database name, or drop the filter to read every database.",
                    notice.AsHints());
            }

            if (!filteredBaseline)
            {
                return McpHelpers.Status(
                    "unavailable",
                    $"{serverName} has no Query Store capture of database_name '{databases[0]}' in the {LocalDataService.BaselineLookbackDays}-day baseline window before this window, so there is no baseline for that database to compare against and no regression of its queries can be detected however badly one regressed. This is NOT a clean bill of health: no query in that database was compared. Either that database's whole collected history falls inside the last {hours_back} hour(s), or it has none older than the baseline lookback yet.");
            }

            if (!filteredRecent)
            {
                return McpHelpers.Status(
                    "empty",
                    $"{serverName} has Query Store history of database_name '{databases[0]}' from before this window but nothing of it collected IN the last {hours_back} hour(s), so that database's recent side is missing rather than nothing to report, and this is NOT the all-clear: no query in that database was compared. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.",
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

    [McpServerTool(Name = "get_query_heatmap"), Description("Draws the desktop viewer's Query Heatmap as a table: how many distinct queries fell into each (time bin x log-magnitude bucket) cell over a window, plus the most-executed query in each cell. It answers when a server was slow and how slow at the same time - get_top_queries_by_cpu ranks queries over a whole window and cannot show that the window had two very different halves. Bins are 5 minutes wide by default because that is exactly what the desktop viewer uses, so a browser, an agent and a desktop pointed at the same server draw the same picture; raise bucket_minutes for a longer window, which is also the lever that fits more of the window inside the cell cap. Magnitude buckets are the viewer's seven, in the metric's own unit: under 1, 1-10, 10-100, 100-1K, 1K-10K, 10K-100K and over 100K.")]
    public static async Task<string> GetQueryHeatmap(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("How far back to look, in hours. Default 24.")] int hours_back = 24,
        [Description("Which per-execution metric to bucket by: duration, cpu, logical_reads, logical_writes or execution_count. Default duration.")] string? metric = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null,
        [Description("Width of each time bin, in minutes. Default 5 - the desktop viewer's own bin width, so the two surfaces agree. Raise it to cover a longer window in fewer cells.")] int bucket_minutes = LocalDataService.ViewerHeatmapBucketMinutes,
        [Description("Maximum CELLS to return, most recent bins first. Default 100. A full day of 5-minute bins can reach 2,016 cells on a busy server; raise bucket_minutes rather than the cap to see the whole window.")] int limit = DefaultHeatmapCellLimit,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Return each cell's top query at full length instead of an 80-character preview. Default false.")] bool full_text = false)
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
            var previewLength = full_text ? FullTextHeatmapPreviewLength : DefaultHeatmapPreviewLength;
            var rows = await dataService.GetQueryHeatmapCellsAsync(
                resolved.ServerId, parsedMetric, hours_back, bucket_minutes, limit + 1, databases, asOfUtc: windowEnd, previewLength);

            /* #4966: where this server's query_stats start for the window, beside the cell cap's `truncated` below —
               that one says the grid held more cells than limit, this one says the store did not hold the window's
               head, so the grid's empty left edge is a retention cut or a young install rather than a quiet server.
               The probe reads v_query_stats, the view the cells came from, and is unfiltered by database_name (the
               floor is a property of the table). Read ahead of the empty answers, which carry it too: an empty grid
               over a window the store does not reach back to is not a true negative. */
            var requestedStart = windowEnd.AddHours(-hours_back);
            var notice = await WindowNoticeAsync(
                () => dataService.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, resolved.ServerId, requestedStart, windowEnd),
                requestedStart, windowEnd, "query_stats", emptyAnswer: rows.Count == 0);

            if (rows.Count == 0)
                return await EmptyHeatmapAsync(dataService, resolved.ServerId, resolved.DisplayName, hours_back, windowEnd, notice, filtered: databases is not null);

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
                server = resolved.DisplayName,
                hours_back,
                /* The window floor, always present (false and null when the store covered the window). No
                   effective_hours_back: this payload carries a page `truncated`, and the census holds that key apart
                   for the window floor (McpPayloadContractCensusTests), so the reach is the instant. */
                effective_start = notice.EffectiveStart,
                window_truncated = notice.WindowTruncated,
                truncation_note = notice.TruncationNote,
                metric = LocalDataService.HeatmapMetricName(parsedMetric),
                metric_unit = LocalDataService.HeatmapMetricUnit(parsedMetric),
                database_name,
                window_start = windowEnd.AddHours(-hours_back).ToString("o"),
                window_end = windowEnd.ToString("o"),
                bucket_minutes,
                /* The same bin width the desktop viewer hardcodes, so the two surfaces cannot disagree
                   about the same server over the same window. */
                bucket_minutes_matches_desktop_viewer = bucket_minutes == LocalDataService.ViewerHeatmapBucketMinutes,
                /* Echoed rather than left implicit in the cell-level flags alone (#4198): a caller who never
                   looks at an individual cell still learns, from this one field, that a second call with
                   full_text=true gets more than what came back. */
                full_text,
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
                    /* #4198: honest about the preview it just spent bytes on. True whenever the stored
                       statement is longer than this call's preview width, at either preview length. */
                    top_query_text_truncated = c.TopQueryTextTruncated,
                }),
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_query_heatmap", ex);
        }
    }

    /// <summary>This tool's default cell cap (#4198, down from the web panel's own 500): at 500 cells the
    /// fixed per-cell fields alone (time bin, bucket, count, hash) ran to roughly 75 KB before one byte of
    /// query text, already more than double the shared 32 KB response budget
    /// (<see cref="McpResponseBudget.DefaultBytes"/>) — so the cap had to fall regardless of the text width
    /// chosen. Darling twin: <c>DarlingMcpQueryHeatmapTools.DefaultCellLimit</c>.</summary>
    private const int DefaultHeatmapCellLimit = 100;

    /// <summary>The default <c>top_query_text</c> preview width, in characters (#4198). Darling twin:
    /// <c>DarlingMcpQueryHeatmapTools.DefaultPreviewLength</c>.</summary>
    private const int DefaultHeatmapPreviewLength = 80;

    /// <summary>What <c>full_text = true</c> asks for: a generous bound, not a literally unbounded fetch.
    /// Darling twin: <c>DarlingMcpQueryHeatmapTools.FullTextPreviewLength</c>.</summary>
    private const int FullTextHeatmapPreviewLength = 32_000;

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
        LocalDataService dataService, int serverId, string serverName, int hours_back, DateTime windowEnd, McpWindowNotice notice,
        bool filtered)
    {
        /* The anchor is threaded in rather than resolved again: the probe answers "was anything collected
           in THIS window", and a window it computed for itself would be a different one. */
        var (hasAny, hasInWindow) = await dataService.GetQueryHeatmapCoverageAsync(serverId, hours_back, asOfUtc: windowEnd);

        if (!hasAny)
        {
            var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, serverId, serverName, "query_stats");
            if (gated != null)
            {
                return gated;
            }

            /* #6453: nothing ever stored, but the query_stats collector's newest run completed clean: the plan cache had
               nothing to read, which collection health calls HEALTHY, so this is a true negative and not "unavailable".
               Darling's twin does the same. */
            var healthyEmpty = await McpRuntimePrecondition.HealthyEmptyMessageAsync(
                dataService, serverId, serverName, "query_stats", "query stats");
            if (healthyEmpty != null)
            {
                return McpHelpers.Status("empty", healthyEmpty);
            }

            return McpHelpers.Status(
                    "unavailable",
                    $"No query stats have EVER been collected for {serverName}, so this is NOT a report of a quiet server — there is nothing to draw. query_stats is a PERIODIC table rather than an edge table: the collector writes rows every cycle for whatever is in the plan cache, so an empty history means nobody looked. Check get_collection_health for this server.");
        }

        if (!hasInWindow)
        {
            return McpHelpers.Status(
                "empty",
                $"{serverName} has query stats from outside this window but nothing collected IN the last {hours_back} hour(s), so the grid has no columns rather than no hot cells. Widen hours_back, or check get_collection_health — a collector that stopped looks exactly like this.",
                notice.AsHints());
        }

        return McpHelpers.Status(
            "empty",
            McpHelpers.QuietUnlessCut(
                /* A database_name filter is exempt: the coverage probe is unfiltered, so a filtered empty grid may be the filter and
                   not the window, and the covered sentence is the one that says so. */
                notice.WindowTruncated && !filtered, notice.EffectiveStart,
                factual: $"Query stats WERE collected for {serverName} in the last {hours_back} hour(s), but no capture recorded an execution: every row carried a zero execution delta, so nothing lands on the grid",
                coveredClaim: ". A server that is up and idle looks exactly like this, and so does a database_name filter matching nothing collected.",
                tail: " Delta-based collection also needs a SECOND cycle before the first non-zero row exists."),
            notice.AsHints());
    }

    [McpServerTool(Name = "get_query_duration_trend"), Description("Gets elapsed_ms_per_second and executions_per_second across ALL queries, points ending at as_of. Not CPU. One query_hash: get_query_trend. A collection whose interval was unknowable is excluded, not counted as 0: unrated_collections counts it, and a point left with nothing else carries null rates (unrated_points), never zero. No points: unavailable means query_stats was never collected here; empty means it was (the message says quiet window or rollup coverage gap). window_truncated is the store's retention floor, not a page cut: effective_start says where the answer begins. <<GUIDE>> Gets a time-series of average query duration over time. Useful for spotting overall performance degradation or improvement trends across all queries. Points are time buckets (bucket, aggregate_note); rates are over each collection's STORED sample interval, and a collection whose interval was unknowable - a restart or counter reset, or the window's first collection when none was stored - is left out rather than counted as 0 (unrated_collections counts them; a point with nothing else carries null rates, unrated_points). Lite has one tier - nothing is rolled up." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryDurationTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null)
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

            /* #5244: database_name appended LAST (H1). Blank or whitespace is every database. The bucketed read takes a
               database list, the way the rest of the trend family does. */
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name;
            var grain = Bucketed(bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
            var startUtc = windowEnd.AddHours(-hours_back);
            var points = await dataService.GetBucketedQueryDurationTrendAsync(
                resolved.ServerId, hours_back, asOfUtc: windowEnd, bucketMinutes, database == null ? null : new[] { database });

            if (points.Count == 0)
            {
                /* Same two states again, same words as Darling's twin. The probe reads v_query_stats
                   because THIS trend does; Darling's probes the base table because ITS trend does. Each
                   probe follows its own read — what the caller sees is one sentence, not two. */
                var gated = McpHelpers.WithDatabase(await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_stats"), database);
                if (gated != null)
                {
                    return gated;
                }

                var everSampled = await dataService.HasAnyQueryStatAsync(resolved.ServerId);

                /* #6453: no row EVER stored is a true negative, not "unavailable", when the collector's newest run completed
                   clean. Darling's twin. */
                var healthyEmpty = everSampled
                    ? null
                    : await McpRuntimePrecondition.HealthyEmptyMessageAsync(
                        dataService, resolved.ServerId, resolved.DisplayName, "query_stats", "query samples");
                if (healthyEmpty != null)
                {
                    return EmptyStatus("empty", healthyEmpty, startUtc, windowEnd, grain, database);
                }

                return everSampled
                    ? EmptyStatus(
                        "empty",
                        $"No query samples recorded for {ScopedServer(resolved.DisplayName, database)} in the last {hours_back} hour(s). This server HAS collected query stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.",
                        startUtc, windowEnd, grain, database)
                    : EmptyStatus(
                        "unavailable",
                        $"No query stats have EVER been recorded for {resolved.DisplayName}. This is not an empty window — the query_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_top_queries_by_cpu will be equally empty until it does.",
                        startUtc, windowEnd, grain, database);
            }

            /* The two siblings below serialize through the SAME helper, so the three Performance-Trends
               reads cannot advertise three different field sets for one shape. */
            return SerializeTrend(resolved.DisplayName, hours_back, startUtc, windowEnd, points, grain,
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd), database);
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
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null)
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

            /* #5244: database_name appended LAST (H1); blank is every database. See get_query_duration_trend. */
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name;
            var grain = Bucketed(bucketMinutes, bucket_minutes is not null, budget.AutoPoints);
            var startUtc = windowEnd.AddHours(-hours_back);
            var points = await dataService.GetBucketedProcedureDurationTrendAsync(
                resolved.ServerId, hours_back, asOfUtc: windowEnd, bucketMinutes, database == null ? null : new[] { database });
            if (points.Count == 0)
            {
                var gated = McpHelpers.WithDatabase(await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "procedure_stats"), database);
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyTrendAsync(
                    dataService.HasAnyProcedureStatAsync(resolved.ServerId), resolved.DisplayName, database, hours_back,
                    startUtc, windowEnd, grain,
                    "stored-procedure",
                    "Check that collection is running and that the server is enabled. A server that genuinely runs no stored procedures also lands here, and that is a real answer rather than a fault.",
                    healthyEmptyProbe: () => McpRuntimePrecondition.HealthyEmptyMessageAsync(
                        dataService, resolved.ServerId, resolved.DisplayName, "procedure_stats", "stored-procedure samples"));
            }

            return SerializeTrend(resolved.DisplayName, hours_back, startUtc, windowEnd, points, grain,
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd), database);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_procedure_duration_trend", ex);
        }
    }

    [McpServerTool(Name = "get_query_store_duration_trend"), Description("Gets a time-series of Query Store duration and executions per second, summed across every query, each interval counted once, at the hour it ran. not_collected: engine cannot run Query Store. unavailable: never sampled here. empty: quiet on Lite always; on Darling, empty can also be a rollup coverage gap (window predates the corrected rollup, run --backfill-rollups). Only a point that stored no interval end and has no earlier point to rate against has null rates, never 0 (unrated_points, unrated_note says why). window_truncated marks the retention floor, not a page cut. <<GUIDE>> Gets a time-series of Query Store duration per second and executions per second over time, summed across every query. Where get_query_duration_trend reads the plan cache and loses everything an eviction or a restart takes with it, this reads Query Store, which persists per interval - so it is the series that survives a failover and the one to reach for when a regression is older than the cache. Each interval is counted once, at the hour the work ran. Each point is a rate over its own stored interval, its end minus its start. A point with no stored end (a row collected before the end was recorded) falls back to the gap since the PREVIOUS point, so only such a point, when it is the window's first collection and has no previous one to difference against, carries null rates: unknowable, never reported as 0 (unrated_points counts them, unrated_note says why)." + McpHelpers.WindowTruncatedDescription + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetQueryStoreDurationTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Limit to one database. Omit for all databases.")] string? database_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            /* #5244: database_name appended LAST (H1); blank is every database. The reader already took a database list
               (the Query Store tab's filter), on both of its arms. */
            var database = string.IsNullOrWhiteSpace(database_name) ? null : database_name;
            var startUtc = windowEnd.AddHours(-hours_back);
            var points = await dataService.GetQueryStoreDurationTrendAsync(
                resolved.ServerId, hours_back, databaseNames: database == null ? null : new[] { database }, asOfUtc: windowEnd);
            if (points.Count == 0)
            {
                /*
                    The one empty answer here that is NOT about the collector: Query Store can be off on
                    every database on the instance. A server with no Query Store data is not a server with
                    no slow queries, so the message names that cause first.
                */
                var gated = McpHelpers.WithDatabase(await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_store"), database);
                if (gated != null)
                {
                    return gated;
                }

                return await EmptyTrendAsync(
                    dataService.HasAnyQueryStoreStatAsync(resolved.ServerId), resolved.DisplayName, database, hours_back,
                    startUtc, windowEnd, PerInterval,
                    "Query Store",
                    "Query Store may be OFF on this server's databases — that, not an absence of slow queries, is the usual cause. Check QUERY_STORE = ON per database, then that collection is running for this server.",
                    () => McpRuntimePrecondition.QueryStoreFirstIntervalMessageAsync(dataService, resolved.ServerId, resolved.DisplayName, database));
            }

            return SerializeTrend(resolved.DisplayName, hours_back, startUtc, windowEnd, points, PerInterval,
                await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd), database);
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
    /// #4231: whether a <see cref="LocalDataService.GetQueryWindowFloorAsync"/> probe found the store's raw
    /// data starting LATER than the window asked for, by more than <see cref="TruncationSlack"/> — the window
    /// floor, not a page cut; no <c>top</c>/<c>limit</c> changes it. Shared so <c>get_top_queries_by_cpu</c>,
    /// <c>get_top_procedures_by_cpu</c>, <c>get_query_store_top</c> and the Queries-tab grids that read the
    /// same three tables all compute the same verdict from the same probe instead of drifting apart. A null floor
    /// is never truncated: the probe answers null only when the window holds nothing to report (no row, and for
    /// the Active Queries and Current Waits relations, which the probe reads by coverage, no logged run of the
    /// collector either), and an empty panel already says so. The slack belongs to this probe verdict alone (#4966): a
    /// grid that filled its row cap dropped rows for certain, so its banner is not judged here (it shows whenever the
    /// oldest row it shows is later than the window's start).
    /// </summary>
    internal static bool IsWindowTruncated(DateTime? floor, DateTime requestedStart) =>
        floor is DateTime f && f > requestedStart + TruncationSlack;

    /// <summary>
    /// #4966: whether a window can ever get a coverage note from the probe at all. The probe's answer is null, the
    /// window's start, or a row or run INSIDE the window (<see cref="LocalDataService.GetQueryWindowFloorAsync"/> bounds
    /// its first read on both sides), so a window no longer than <see cref="TruncationSlack"/> never has a floor later
    /// than the start by more than the slack: <see cref="IsWindowTruncated"/> answers false for every floor it can
    /// give. The banner step and the Query Heatmap's column start skip the probe for such a window, which is every
    /// range of 90 minutes or less and a narrow slicer drag.
    /// </summary>
    internal static bool CanWindowBeTruncated(DateTime startUtc, DateTime endUtc) =>
        endUtc - startUtc > TruncationSlack;

    /// <summary>
    /// The instant the served window really starts: the probe's floor, but never earlier than the start that was
    /// asked for. The probe (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) answers the requested start
    /// itself when the server also holds a row before the window (the window's start is covered), and the
    /// server's first row inside the window when its data starts late, so only a floor AFTER the requested start
    /// moves the served window. The clamp stays so that a floor at or before the start can never read as the window
    /// having begun earlier. A null floor leaves the requested start in place, as it always did. Null means the
    /// window holds nothing: no row, and for the Active Queries and Current Waits relations no logged run of the
    /// collector either (a window the collector ran in, with nothing to record, answers the start, not null).
    /// </summary>
    internal static DateTime EffectiveWindowStart(DateTime? floor, DateTime requestedStart) =>
        floor is DateTime f && f > requestedStart ? f : requestedStart;

    /// <summary>
    /// #4966: the window floor of a tool whose payload also carries a PAGE cut (<c>truncated</c>), in the three keys
    /// that tool writes after <c>hours_back</c>: <c>effective_start</c>, <c>window_truncated</c> and
    /// <c>truncation_note</c>, with the Queries tools' verdict (<see cref="IsWindowTruncated"/>), clamp
    /// (<see cref="EffectiveWindowStart"/>) and wording, and the table the tool read named in the note. Always
    /// present on the payload: a covered window says <c>window_truncated: false</c> and a null note, as the
    /// Queries tools do. <c>effective_hours_back</c> is NOT written here: Darling's
    /// <c>McpPayloadContractCensusTests</c> holds that key apart for the window floor and fails it beside a bare
    /// <c>truncated</c>, and every tool this serves carries one (a limit, a cell cap), so a caller reads the reach
    /// off <c>effective_start</c> instead. <paramref name="requestedStart"/> is the start of the EARLIEST window the
    /// tool compares (a tool that sets two windows against each other passes the older one's start), and
    /// <paramref name="floor"/> the probe's answer for that start (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>).
    /// <paramref name="tail"/> is one more sentence for a tool whose reading of the window needs it.
    /// <para><paramref name="emptyAnswer"/> (#5015): the answer this notice rides on holds no rows. A null floor then
    /// means the store holds no collection for this server in the window at all, and an empty answer over a window
    /// nothing was read from is not a true negative, so the notice says NOT covered: <c>window_truncated</c> true,
    /// no <c>effective_start</c> (there is no start to name) and a note that the store holds no collection in the
    /// window. On an answer WITH rows a null floor keeps its old meaning (covered, at the start that was asked for).</para>
    /// </summary>
    internal static McpWindowNotice WindowNotice(
        DateTime? floor, DateTime requestedStart, string table, string? tail = null, bool emptyAnswer = false, bool listOnly = false,
        Func<DateTime, string?>? tailFor = null)
    {
        if (floor is null && listOnly)
        {
            /* The windowed list is empty but the answer carries other data (a latest-snapshot block), so it is not an empty answer. */
            return new McpWindowNotice(
                null,
                true,
                $"The store holds no collection of {table} for this server in this window, so no windowed rows were read, and a list with no rows is not a report that nothing happened. "
                    + "The window may reach further back than the store retains, this server may have been monitored for less time than that, or collection may have stopped; get_collection_health shows which.");
        }

        if (floor is null && emptyAnswer)
        {
            return new McpWindowNotice(
                null,
                true,
                $"The store holds no collection of {table} for this server in this window, so nothing was read, and this empty answer is not a report that nothing happened. "
                    + "The window may reach further back than the store retains, this server may have been monitored for less time than that, or collection may have stopped; get_collection_health shows which.");
        }

        var truncated = IsWindowTruncated(floor, requestedStart);
        var effectiveStart = EffectiveWindowStart(floor, requestedStart);
        tail = tailFor?.Invoke(effectiveStart) ?? tail;
        return new McpWindowNotice(
            McpHelpers.FormatEffectiveStart(effectiveStart),
            truncated,
            truncated
                ? $"The window reaches further back than this server's raw {table} retains (or this server has been monitored for less time than that), so the older part of it was not read."
                    + (tail is null ? "" : " " + tail)
                : null);
    }

    /// <summary>
    /// #4966: <see cref="WindowNotice"/> with its coverage probe, which a window no longer than <see cref="TruncationSlack"/>
    /// never needs (<see cref="CanWindowBeTruncated"/>: the probe cannot find a floor later than the start by more than
    /// the slack, so the answer is covered whatever it would read). The four tools that carry the three window-floor
    /// keys all go through here, so a read of an hour or less makes no probe call, as the Lite tabs' banner step does.
    /// The probe is passed as a delegate and is not started for a short window. A short window's
    /// <c>effective_start</c> is the start that was asked for, because no floor was read.
    /// <para>#5015: an EMPTY answer (<paramref name="emptyAnswer"/>) is always probed, whatever the window's length. The skip
    /// answers "covered" without reading anything, which is right beside rows (the page itself shows the data) but is a
    /// claim an empty answer cannot make: nothing was read, so nothing says the store held the window at all. A probe
    /// that finds no row and no run in the window then reads as NOT covered (<see cref="WindowNotice"/>).</para>
    /// </summary>
    internal static async Task<McpWindowNotice> WindowNoticeAsync(
        Func<Task<DateTime?>> probe, DateTime requestedStart, DateTime windowEnd, string table, string? tail = null, bool emptyAnswer = false, bool listOnly = false,
        Func<DateTime, string?>? tailFor = null) =>
        WindowNotice(
            emptyAnswer || listOnly || CanWindowBeTruncated(requestedStart, windowEnd) ? await probe() : null,
            requestedStart, table, tail, emptyAnswer, listOnly, tailFor);

    /// <summary>
    /// #4966: <see cref="WindowNoticeAsync"/> for an EVENT list (deadlocks, blocked process reports, long query
    /// completions), where a first run of the collector can store events from before itself: the coverage probe
    /// reads the collector's runs and the rows' own time column, but where that column is collection_time
    /// (long query completions) a page can still show an event older than the probe's floor (the deadlock and
    /// blocked-process probes already read the event column), and a notice that names a start later than a row it shows is wrong on its face. On a data
    /// answer the floor is the EARLIER of the probe's and <paramref name="oldestEventShown"/> (the oldest event
    /// time on the page; null when the page holds none, as on an empty answer). The comparison runs inside the
    /// probe delegate, so a window the probe is skipped for (90 minutes or less, with rows) stays covered at the
    /// start that was asked for, as every other window-floor tool does.
    /// </summary>
    internal static Task<McpWindowNotice> EventWindowNoticeAsync(
        Func<Task<DateTime?>> probe, DateTime? oldestEventShown, DateTime requestedStart, DateTime windowEnd, string table, bool emptyAnswer = false) =>
        WindowNoticeAsync(
            async () => LocalDataService.EarlierCoverageFloor(await probe(), oldestEventShown),
            requestedStart, windowEnd, table, emptyAnswer: emptyAnswer);

    /// <summary>
    /// The three window-floor keys <see cref="WindowNotice"/> answers, as the values a tool writes into its payload.
    /// <see cref="EffectiveStart"/> is null only for an empty answer over a window the store holds nothing in
    /// (#5015): there is no start to name.
    /// </summary>
    internal readonly record struct McpWindowNotice(string? EffectiveStart, bool WindowTruncated, string? TruncationNote)
    {
        /// <summary>
        /// #4966: the same three keys, with the same values and wording, for an <c>empty</c> status, which carries them under
        /// <c>hints</c> (the shape <c>get_query_store_top</c>'s module miss uses): an empty answer over a window the store does
        /// not reach back to is not a true negative, and without them it read as one. A status that says the collector never ran
        /// (<c>not_collected</c>) does not carry them.
        /// </summary>
        internal object AsHints() => new { effective_start = EffectiveStart, window_truncated = WindowTruncated, truncation_note = TruncationNote };
    }

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
        IReadOnlyList<BaselineDiscontinuity> discontinuities, string? database)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["server"] = serverName,
            ["hours_back"] = hours_back,
            /* #5244: which database the series is limited to: the name for one, null for all (the twin of Darling's database_name echo). */
            ["database_name"] = database,
        };
        /* effective_start is the first COLLECTION the store held — on a bucketed point its first collection, not the
           bucket's start (#3897). */
        WriteDisclosure(envelope, points.Count > 0 ? points[0].FirstCollectionTime ?? points[0].CollectionTime : null, startUtc, windowEndUtc, grain);
        /* #3541 A12: a point with no rate is published as null, never as 0, and the envelope says how many
           and why. Two whys since #3695 / v61 (#3653 A11): the plan-cache trends read the STORED interval,
           so a restart collection (stored 0) is unrated beside the first-in-window LAG case; the Query Store trend stores no sample interval and only hits the second arm, and only for a point that
           stored no end (#4765, a rate over its own stored interval otherwise); the sentence stays true there. Same keys
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
    private static string EmptyStatus(string status, string message, DateTime startUtc, DateTime windowEndUtc, TrendGrain grain, string? database)
    {
        var envelope = new Dictionary<string, object?>
        {
            ["status"] = status,
            ["message"] = message,
            /* #5244 review L2: the echo the data envelope writes (the name for one database, null for all), so an empty
               answer says which database it was limited to; Darling's twin writes Describe() here. */
            ["database_name"] = McpDatabaseSelection.Describe(database == null ? null : new[] { database }),
        };
        WriteDisclosure(envelope, null, startUtc, windowEndUtc, grain);
        return JsonSerializer.Serialize(envelope, McpHelpers.JsonOptions);
    }

    /// <summary>
    /// The server's name with the database an answer covered (#5244), Darling's <c>DarlingMcpTrendTools.ScopedServer</c>
    /// for one database: with a filter the read looked at that database only, so "nothing recorded for the server" would
    /// be a verdict about a database it never read. No filter leaves the name as it was. The never-sampled answer stays
    /// on the bare name, because its probe is server-wide and its verdict is too.
    /// </summary>
    private static string ScopedServer(string serverName, string? database) =>
        serverName + McpBlockingTools.ForChosenDatabase(database);

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
        Task<bool> probe, string serverName, string? database, int hours_back, DateTime startUtc, DateTime windowEndUtc, TrendGrain grain,
        string what, string checkThis, Func<Task<string?>>? neverSampledOverride = null, Func<Task<string?>>? healthyEmptyProbe = null)
    {
        var everSampled = await probe;

        /* #6453: no row EVER stored is a true negative, not "unavailable", when the collector's newest run completed clean:
           it looked and the server had nothing to report, which collection health calls HEALTHY. Darling's twin. */
        if (!everSampled && healthyEmptyProbe != null && await healthyEmptyProbe() is { } healthyEmpty)
        {
            return EmptyStatus("empty", healthyEmpty, startUtc, windowEndUtc, grain, database);
        }

        /* #6181: a caller that can tell "no sample yet, but the source is healthy" supplies the whole answer for the
           never-sampled state; null keeps the standard text. */
        if (!everSampled && neverSampledOverride != null && await neverSampledOverride() is { } overrideText)
        {
            return EmptyStatus("unavailable", overrideText, startUtc, windowEndUtc, grain, database);
        }

        return everSampled
            ? EmptyStatus(
                "empty",
                $"No {what} samples were recorded for {ScopedServer(serverName, database)} in the last {hours_back} hour(s). This server HAS been sampled before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.",
                startUtc, windowEndUtc, grain, database)
            : EmptyStatus(
                "unavailable",
                $"No {what} samples have EVER been recorded for {serverName}. This is not an empty window — nothing at all has been stored for this server, so it is NOT a quiet server. {checkThis}",
                startUtc, windowEndUtc, grain, database);
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
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "query_stats")
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
                ["server"] = resolved.DisplayName,
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
