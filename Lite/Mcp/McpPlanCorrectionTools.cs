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
        "Gets SQL Server automatic plan correction (APC) activity: the engine's FORCE_LAST_GOOD_PLAN recommendations and actions over the window, plus each database's current automatic-tuning enablement state. Use when a query's plan changed suddenly - APC forcing or unforcing a plan is a first-class explanation - or to check whether automatic tuning is on and actually working (desired vs actual state). Rows come from sys.dm_db_tuning_recommendations captured on a schedule; a recommendation's state moves through Active/Verifying/Success/Reverted as the engine acts.")]
    public static async Task<string> GetPlanCorrections(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum recommendation rows. Default 50.")] int limit = 50,
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
            var rows = await dataService.GetPlanCorrectionsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (tuning.Count == 0 && rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "plan_correction")
                    ?? McpHelpers.Status("empty",
                        "No plan correction data collected for this server. The collector runs against SQL Server 2017+ " +
                        "(sys.dm_db_tuning_recommendations); a server that has never produced a row here either predates " +
                        "that or has no databases with Query Store on.");
            }

            var recommendations = rows.Take(limit).Select(r => new
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
                total_recommendations = rows.Count,
                recommendations,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_plan_corrections", ex);
        }
    }
}
