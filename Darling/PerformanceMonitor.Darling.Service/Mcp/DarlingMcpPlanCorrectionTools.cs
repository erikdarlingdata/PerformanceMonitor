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
    [McpServerTool(Name = "get_plan_corrections"), Description(
        "Gets SQL Server automatic plan correction (APC) activity: the engine's FORCE_LAST_GOOD_PLAN recommendations and actions over the window, plus each database's current automatic-tuning enablement state. Use when a query's plan changed suddenly - APC forcing or unforcing a plan is a first-class explanation - or to check whether automatic tuning is on and actually working (desired vs actual state). Rows come from sys.dm_db_tuning_recommendations captured on a schedule; a recommendation's state moves through Active/Verifying/Success/Reverted as the engine acts.")]
    public static async Task<string> GetPlanCorrections(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum recommendation rows. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            var tuning = await DarlingPlanCorrectionReader.GetLatestAutomaticTuningAsync(postgres, resolved.ServerId);
            var rows = await DarlingPlanCorrectionReader.GetPlanCorrectionsAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now);

            if (tuning.Count == 0 && rows.Count == 0)
            {
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "plan_correction")
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
