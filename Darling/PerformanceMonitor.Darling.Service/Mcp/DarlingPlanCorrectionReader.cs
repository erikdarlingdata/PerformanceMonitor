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
///
/// <para><b>The four recommendation lifecycle times are de-skewed to naive UTC at this boundary.</b>
/// <c>PlanCorrectionCollector</c> ships <c>valid_since</c>, <c>last_refresh</c>,
/// <c>execute_action_initiated_time</c> and <c>revert_action_initiated_time</c> verbatim off
/// <c>sys.dm_db_tuning_recommendations</c>, so the stored values are the monitored server's LOCAL wall
/// clock, while the <c>collection_time</c> on the same row and the tuning read's <c>as_of</c> are naive UTC.
/// Left raw they are early by the server's offset — 4 hours on the production fleet — which is enough to
/// place a recommendation's <c>valid_since</c> before the collection that first observed it, and to make a
/// forced plan look as though it was forced hours before the regression that prompted it. The window and
/// the ordering stay on <c>collection_time</c>, so no row SELECTION changes. The tuning-state read needs
/// nothing: its only timestamp is <c>collection_time</c>.</para>
/// </summary>
internal static class DarlingPlanCorrectionReader
{
    /// <summary>
    /// The engine's automatic plan correction recommendations for one server over the window, newest first.
    /// <c>recommendation_name IS NOT NULL</c> drops the enablement-only rows. LIMIT 200 mirrors the Viewer's
    /// grid read; the tool applies its own smaller take on top. The four lifecycle times are de-skewed from
    /// the server's local clock to naive UTC (see the class remarks). $1 server_id, $2 window start, $3 window
    /// end (naive UTC).
    /// </summary>
    public const string PlanCorrectionsSql = @"
WITH svr AS (
    SELECT COALESCE((
        SELECT sp.utc_offset_minutes
        FROM server_properties AS sp
        WHERE sp.server_id = $1
        AND   sp.utc_offset_minutes IS NOT NULL
        ORDER BY sp.collection_time DESC
        LIMIT 1), 0) AS offset_minutes
)
SELECT
    pc.collection_time,
    pc.database_name,
    pc.query_text,
    pc.recommendation_state,
    pc.recommendation_state_reason,
    pc.recommendation_reason,
    pc.score,
    pc.estimated_gain_seconds,
    pc.query_id,
    pc.regressed_plan_id,
    pc.last_good_plan_id,
    pc.last_good_plan_forcing_type,
    pc.last_good_plan_is_forced,
    pc.last_good_plan_force_failure_reason,
    pc.regressed_plan_execution_count,
    pc.regressed_plan_cpu_time_average_ms,
    pc.last_good_plan_execution_count,
    pc.last_good_plan_cpu_time_average_ms,
    pc.valid_since - make_interval(mins => svr.offset_minutes) AS valid_since_utc,
    pc.last_refresh - make_interval(mins => svr.offset_minutes) AS last_refresh_utc,
    pc.execute_action_initiated_by,
    pc.execute_action_initiated_time - make_interval(mins => svr.offset_minutes) AS execute_action_initiated_time_utc,
    pc.revert_action_initiated_by,
    pc.revert_action_initiated_time - make_interval(mins => svr.offset_minutes) AS revert_action_initiated_time_utc
FROM plan_correction AS pc, svr
WHERE pc.server_id = $1
AND   pc.collection_time >= $2
AND   pc.collection_time <= $3
AND   pc.recommendation_name IS NOT NULL
ORDER BY pc.collection_time DESC, pc.score DESC
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
        DateTime? ValidSinceUtc,
        DateTime? LastRefreshUtc,
        string? ExecuteActionInitiatedBy,
        DateTime? ExecuteActionInitiatedTimeUtc,
        string? RevertActionInitiatedBy,
        DateTime? RevertActionInitiatedTimeUtc);

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
