/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The automatic plan correction reads for the MCP/web surface (#2028) — the Viewer's
/// <c>ViewerDataService.PlanCorrection.cs</c> reads, mirrored service-side the way
/// <see cref="DarlingBlockingReader"/> mirrors the blocking reads (the service cannot reference the WPF
/// viewer assembly, so the SQL twins by convention and the tests pin both shapes). One collector row carries
/// two layers: the per-database FORCE_LAST_GOOD_PLAN enablement state repeats on every one of that
/// database's recommendation rows, and a database with nothing to recommend lands a single enablement-only
/// row whose <c>recommendation_name</c> is NULL. The recommendations read drops those rows; the tuning-state
/// read keeps exactly one per database.
/// </summary>
internal static class DarlingPlanCorrectionReader
{
    /// <summary>
    /// The engine's automatic plan correction recommendations for one server over the window, newest first.
    /// <c>recommendation_name IS NOT NULL</c> drops the enablement-only rows. LIMIT 200 mirrors the Viewer's
    /// grid read; the tool applies its own smaller take on top. $1 server_id, $2 window start, $3 window end
    /// (naive UTC).
    /// </summary>
    public const string PlanCorrectionsSql = @"
SELECT
    collection_time,
    database_name,
    query_text,
    recommendation_state,
    recommendation_state_reason,
    recommendation_reason,
    score,
    estimated_gain_seconds,
    query_id,
    regressed_plan_id,
    last_good_plan_id,
    last_good_plan_forcing_type,
    last_good_plan_is_forced,
    last_good_plan_force_failure_reason,
    regressed_plan_execution_count,
    regressed_plan_cpu_time_average_ms,
    last_good_plan_execution_count,
    last_good_plan_cpu_time_average_ms,
    valid_since,
    last_refresh,
    execute_action_initiated_by,
    execute_action_initiated_time,
    revert_action_initiated_by,
    revert_action_initiated_time
FROM plan_correction
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   recommendation_name IS NOT NULL
ORDER BY collection_time DESC, score DESC
LIMIT 200";

    /// <summary>
    /// The latest FORCE_LAST_GOOD_PLAN enablement snapshot per database. The enablement columns repeat on
    /// every one of a database's recommendation rows, so this takes the newest capture for the server and
    /// DISTINCTs it back to one row per database. $1 server_id.
    /// </summary>
    public const string AutomaticTuningSql = @"
SELECT DISTINCT
    database_name,
    force_last_good_plan_desired_state,
    force_last_good_plan_actual_state,
    force_last_good_plan_reason,
    collection_time
FROM plan_correction
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM plan_correction WHERE server_id = $1)
ORDER BY database_name";

    /// <summary>One recommendation row the engine produced (or acted on) in the window.</summary>
    public sealed record PlanCorrectionRow(
        DateTime CollectionTime,
        string DatabaseName,
        string? QueryText,
        string? RecommendationState,
        string? RecommendationStateReason,
        string? RecommendationReason,
        int? Score,
        double? EstimatedGainSeconds,
        long? QueryId,
        long? RegressedPlanId,
        long? LastGoodPlanId,
        string? LastGoodPlanForcingType,
        bool? LastGoodPlanIsForced,
        string? LastGoodPlanForceFailureReason,
        long? RegressedPlanExecutionCount,
        double? RegressedPlanCpuTimeAverageMs,
        long? LastGoodPlanExecutionCount,
        double? LastGoodPlanCpuTimeAverageMs,
        DateTime? ValidSince,
        DateTime? LastRefresh,
        string? ExecuteActionInitiatedBy,
        DateTime? ExecuteActionInitiatedTime,
        string? RevertActionInitiatedBy,
        DateTime? RevertActionInitiatedTime);

    /// <summary>One database's FORCE_LAST_GOOD_PLAN enablement state at the newest capture.</summary>
    public sealed record AutomaticTuningRow(
        string DatabaseName,
        string? DesiredState,
        string? ActualState,
        string? Reason,
        DateTime CollectionTime);

    public static async Task<List<PlanCorrectionRow>> GetPlanCorrectionsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<PlanCorrectionRow>();
        await using var command = postgres.CreateCommand(PlanCorrectionsSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanCorrectionRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.IsDBNull(7) ? null : reader.GetDouble(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetBoolean(12),
                reader.IsDBNull(13) ? null : reader.GetString(13),
                reader.IsDBNull(14) ? null : reader.GetInt64(14),
                reader.IsDBNull(15) ? null : reader.GetDouble(15),
                reader.IsDBNull(16) ? null : reader.GetInt64(16),
                reader.IsDBNull(17) ? null : reader.GetDouble(17),
                reader.IsDBNull(18) ? null : reader.GetDateTime(18),
                reader.IsDBNull(19) ? null : reader.GetDateTime(19),
                reader.IsDBNull(20) ? null : reader.GetString(20),
                reader.IsDBNull(21) ? null : reader.GetDateTime(21),
                reader.IsDBNull(22) ? null : reader.GetString(22),
                reader.IsDBNull(23) ? null : reader.GetDateTime(23)));
        }

        return rows;
    }

    public static async Task<List<AutomaticTuningRow>> GetLatestAutomaticTuningAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        var rows = new List<AutomaticTuningRow>();
        await using var command = postgres.CreateCommand(AutomaticTuningSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new AutomaticTuningRow(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.IsDBNull(1) ? null : reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetDateTime(4)));
        }

        return rows;
    }
}
