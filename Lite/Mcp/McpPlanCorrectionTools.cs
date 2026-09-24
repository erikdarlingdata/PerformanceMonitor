/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The automatic plan correction MCP surface (#2028) — Lite's twin of Darling's
/// <c>DarlingMcpPlanCorrectionTools</c>, over the existing <see cref="LocalDataService"/> plan-correction
/// reads (the same <c>v_plan_correction</c> archive-union view the Plan Corrections and Automatic Tuning
/// grids read). One tool, both layers: the windowed recommendation/action rows and the newest per-database
/// FORCE_LAST_GOOD_PLAN enablement snapshot.
/// </summary>
[McpServerToolType]
public sealed class McpPlanCorrectionTools
{
    [McpServerTool(Name = "get_plan_corrections"), Description(
        "Gets SQL Server automatic plan correction (APC) activity: FORCE_LAST_GOOD_PLAN recommendations/actions over the window ending at as_of, newest capture first, plus each database's automatic-tuning enablement. Rows recur per capture, not per distinct recommendation. THE PAGE IS BOUNDED BY limit, NOT hours_back: truncated means more rows existed; oldest/newest_returned_collection_time bound the page. automatic_tuning ignores the window: a latest snapshot, as_of says when. All timestamps are UTC. No rows and no automatic_tuning: empty (not_collected checked first). <<GUIDE>> Gets SQL Server automatic plan correction (APC) activity: the engine's FORCE_LAST_GOOD_PLAN recommendations and actions over the window, NEWEST CAPTURE FIRST, plus each database's current automatic-tuning enablement state. Use when a query's plan changed suddenly - APC forcing or unforcing a plan is a first-class explanation - or to check whether automatic tuning is on and actually working (desired vs actual state). Rows come from sys.dm_db_tuning_recommendations captured on a schedule, and the collector RE-CAPTURES every open recommendation on every cycle, so the same recommendation appears once per capture and a few open recommendations fill a page fast. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: recommendations_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_collection_time / newest_returned_collection_time bound the page - under newest-first ordering the oldest stamp IS how far back this read reached, and on a server with open recommendations a week-long request at the default limit reaches back hours, not days. Raise limit or narrow hours_back when truncated is true. automatic_tuning is a latest-snapshot read that ignores the window entirely; its as_of stamps say when. A recommendation's state moves through Active/Verifying/Success/Reverted as the engine acts. Every timestamp here is UTC, including valid_since / last_refresh / execute_action_initiated_time / revert_action_initiated_time - sys.dm_db_tuning_recommendations reports those four in UTC and they are stored and returned unconverted - so they order correctly against collection_time and against get_query_store_regressions.")]
    public static async Task<string> GetPlanCorrections(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum recommendation rows to return, newest capture first. Default 50. This is what bounds the page - read truncated to know whether the window held more.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            var tuning = await dataService.GetLatestAutomaticTuningAsync(resolved.ServerId);
            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal. The reader's LIMIT 200 over per-cycle re-captures gave every window the same ~16-hour
               reach, and `total_recommendations` published that page as the window's count. */
            var rows = await dataService.GetPlanCorrectionsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            if (tuning.Count == 0 && rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "plan_correction")
                    ?? McpHelpers.Status("empty",
                        "No plan correction data collected for this server. The collector runs against SQL Server 2017+ " +
                        "(sys.dm_db_tuning_recommendations); a server that has never produced a row here either predates " +
                        "that or has no databases with Query Store on.");
            }

            var recommendations = page.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                database_name = r.DatabaseName,
                query_id = r.QueryId,
                regressed_plan_id = r.RegressedPlanId,
                last_good_plan_id = r.LastGoodPlanId,
                recommendation_state = r.RecommendationState,
                recommendation_state_reason = r.RecommendationStateReason,
                recommendation_reason = r.RecommendationReason,
                score = r.Score,
                estimated_gain_seconds = r.EstimatedGainSeconds,
                last_good_plan_forcing_type = r.LastGoodPlanForcingType,
                last_good_plan_is_forced = r.LastGoodPlanIsForced,
                last_good_plan_force_failure_reason = r.LastGoodPlanForceFailureReason,
                regressed_plan_execution_count = r.RegressedPlanExecutionCount,
                regressed_plan_cpu_time_average_ms = r.RegressedPlanCpuTimeAverageMs,
                last_good_plan_execution_count = r.LastGoodPlanExecutionCount,
                last_good_plan_cpu_time_average_ms = r.LastGoodPlanCpuTimeAverageMs,
                /* Emitted as stored: sys.dm_db_tuning_recommendations reports these four in UTC, which is
                   the frame every other field on this payload is in, so there is no offset to apply. The
                   frame is measured against the collector-written UTC collection_time rather than inferred
                   from the collector shipping the DMV verbatim; DarlingPlanCorrectionReader's remarks carry
                   the measurement, and the twelve stamps that DO need a per-server offset are listed
                   beside it. */
                valid_since = r.ValidSince?.ToString("o"),
                last_refresh = r.LastRefresh?.ToString("o"),
                execute_action_initiated_by = r.ExecuteActionInitiatedBy,
                execute_action_initiated_time = r.ExecuteActionInitiatedTime?.ToString("o"),
                revert_action_initiated_by = r.RevertActionInitiatedBy,
                revert_action_initiated_time = r.RevertActionInitiatedTime?.ToString("o"),
                query_text = McpHelpers.Truncate(r.QueryText, 2000),
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                automatic_tuning = tuning.Select(t => new
                {
                    database_name = t.DatabaseName,
                    force_last_good_plan_desired_state = t.DesiredState,
                    force_last_good_plan_actual_state = t.ActualState,
                    force_last_good_plan_reason = t.Reason,
                    as_of = t.CollectionTime.ToString("o"),
                }),
                /* #3541 A3: the page described as a page, on Darling's names. Newest-capture-first makes it a
                   contiguous slice of the window's tail, so the oldest stamp IS the reach; null when the window
                   held no recommendation rows and only the tuning snapshot answered. */
                recommendations_returned = page.Count,
                truncated,
                oldest_returned_collection_time = page.Count == 0 ? null : page.Min(r => r.CollectionTime).ToString("o"),
                newest_returned_collection_time = page.Count == 0 ? null : page.Max(r => r.CollectionTime).ToString("o"),
                order = "collection_time_desc",
                recommendations,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_corrections", ex);
        }
    }
}
