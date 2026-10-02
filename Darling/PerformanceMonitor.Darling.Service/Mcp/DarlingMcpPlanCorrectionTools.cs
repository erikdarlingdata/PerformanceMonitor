/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The automatic plan correction MCP surface (#2028) — until this, <c>plan_correction</c> was the one
/// collected table with NO agent-readable path at all: no reader tool, and absent from the custom-view
/// MeasureCatalog. An agent asking "did automatic plan correction flip or unforce a plan on this server last
/// night?" — a first-class explanation for sudden plan-shape changes — had no way to answer even though both
/// desktop apps render the data. One tool returns both layers the collector captures: the windowed
/// recommendation/action rows, and the newest per-database FORCE_LAST_GOOD_PLAN enablement snapshot.
/// Registered in both hosts by the same twin convention as the blocking tools.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPlanCorrectionTools
{
    /// <summary>
    /// #4198: the default page's <c>query_text</c> preview length. Cutting the old 2,000-character truncation
    /// alone is not enough here: this row carries 20+ named fields (four lifecycle timestamps, several
    /// free-text reason columns), and <see cref="PlanCorrectionBudgetLiveTests"/> measured that shape alone
    /// at roughly 900 bytes/row before query_text — so the row LIMIT below is cut too, the way #4198 cut
    /// get_query_store_regressions' default from 20 rows to about 5 for the same reason (a wide row, not
    /// just a wide field). 150 matches the order of magnitude <c>get_store_query_stats</c> uses for the same
    /// preview-then-opt-in shape.
    /// </summary>
    private const int QueryTextPreviewLength = 150;

    [McpServerTool(Name = "get_plan_corrections"), Description(
        "Gets SQL Server automatic plan correction (APC) activity: FORCE_LAST_GOOD_PLAN recommendations/actions over the window ending at as_of, newest capture first, plus each database's automatic-tuning enablement. Rows recur per capture, not per distinct recommendation. THE PAGE IS BOUNDED BY limit, NOT hours_back: truncated means more rows existed; oldest/newest_returned_collection_time bound the page. automatic_tuning ignores the window: the latest snapshot; each row's as_of says when. All timestamps are UTC. No rows and no automatic_tuning: empty (not_collected checked first). <<GUIDE>> Gets SQL Server automatic plan correction (APC) activity: the engine's FORCE_LAST_GOOD_PLAN recommendations and actions over the window, NEWEST CAPTURE FIRST, plus each database's current automatic-tuning enablement state. Use when a query's plan changed suddenly - APC forcing or unforcing a plan is a first-class explanation - or to check whether automatic tuning is on and actually working (desired vs actual state). Rows come from sys.dm_db_tuning_recommendations captured on a schedule, and the collector RE-CAPTURES every open recommendation on every cycle, so the same recommendation appears once per capture and a few open recommendations fill a page fast. THE PAGE IS BOUNDED BY limit, NOT BY hours_back: recommendations_returned is how many rows you got, truncated says the window held more than limit, and oldest_returned_collection_time / newest_returned_collection_time bound the page - under newest-first ordering the oldest stamp IS how far back this read reached, and on a server with open recommendations a week-long request at the default limit reaches back hours, not days. Raise limit or narrow hours_back when truncated is true. automatic_tuning is a latest-snapshot read that ignores the window entirely; its as_of stamps say when. A recommendation's state moves through Active/Verifying/Success/Reverted as the engine acts. query_text is a preview by default, capped at 150 characters with query_text_truncated marking whichever rows were actually cut - pass full_text=true for the whole regressed statement on every row instead, the same opt-in get_store_query_stats uses. Every timestamp here is UTC, including valid_since / last_refresh / execute_action_initiated_time / revert_action_initiated_time - sys.dm_db_tuning_recommendations reports those four in UTC and they are stored and returned unconverted - so they order correctly against collection_time and against get_query_store_regressions.")]
    public static async Task<string> GetPlanCorrections(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum recommendation rows to return, newest capture first. Default 25. This is what bounds the page - read truncated to know whether the window held more.")] int limit = 25,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description("Return each row's full query_text instead of a 150-character preview. Default false.")] bool full_text = false,
        CancellationToken cancellationToken = default)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name, cancellationToken);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var tuning = await DarlingPlanCorrectionReader.GetLatestAutomaticTuningAsync(postgres, resolved.ServerId, cancellationToken);
            /* #3541 A3: the caller's limit + 1 as the fetch, the extra row as the observed truncation
               signal. The reader's LIMIT 200 over per-cycle re-captures gave every window the same ~16-hour
               reach, and `total_recommendations` published that page as the window's count. */
            var rows = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1, cancellationToken);
            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            if (tuning.Count == 0 && rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "plan_correction", cancellationToken)
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
                valid_since = r.ValidSinceUtc?.ToString("o"),
                last_refresh = r.LastRefreshUtc?.ToString("o"),
                execute_action_initiated_by = r.ExecuteActionInitiatedBy,
                execute_action_initiated_time = r.ExecuteActionInitiatedTimeUtc?.ToString("o"),
                revert_action_initiated_by = r.RevertActionInitiatedBy,
                revert_action_initiated_time = r.RevertActionInitiatedTimeUtc?.ToString("o"),
                query_text = full_text ? r.QueryText : McpHelpers.Truncate(r.QueryText, QueryTextPreviewLength),
                query_text_truncated = !full_text && r.QueryText != null && r.QueryText.Length > QueryTextPreviewLength,
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
                /* #3541 A3: the page described as a page. Newest-capture-first makes it a contiguous slice of
                   the window's tail, so the oldest stamp IS the reach; null when the window held no
                   recommendation rows and only the tuning snapshot answered. */
                recommendations_returned = page.Count,
                truncated,
                oldest_returned_collection_time = page.Count == 0 ? null : page.Min(r => r.CollectionTime).ToString("o"),
                newest_returned_collection_time = page.Count == 0 ? null : page.Max(r => r.CollectionTime).ToString("o"),
                order = "collection_time_desc",
                recommendations,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("get_plan_corrections", ex);
        }
    }
}
