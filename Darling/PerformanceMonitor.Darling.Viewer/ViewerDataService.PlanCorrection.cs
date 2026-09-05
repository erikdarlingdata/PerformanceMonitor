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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The two automatic plan correction reads (#1952) — Lite's <c>LocalDataService.PlanCorrection.cs</c> ported
/// to Postgres. Both hit the BASE <c>plan_correction</c> table (a post-V14 collector has no <c>v_*</c>
/// passthrough view); Lite reads its <c>v_plan_correction</c> archive union view, which is the only
/// difference between the two files. One collector writes two grids because it writes two layers in one row:
/// the per-database FORCE_LAST_GOOD_PLAN enablement state repeats on every one of that database's
/// recommendation rows, and a database with nothing to recommend lands a single enablement-only row whose
/// <c>recommendation_name</c> is NULL. The Plan Corrections read drops that row; the Automatic Tuning read
/// keeps exactly one per database.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The engine's automatic plan correction recommendations for one server over the window.
    /// <c>recommendation_name IS NOT NULL</c> drops the enablement-only rows. Same windowed shape as the
    /// long-query read (LIMIT 200); the grid applies a view-only DESCENDING-by-score sort, so the SQL keeps
    /// the chronological ORDER BY. $1 server_id, $2 window start, $3 window end (naive UTC), $4 db filter.
    /// </summary>
    public const string PlanCorrectionsSql = """
        SELECT
            collection_time,
            query_text,
            database_name,
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
            is_executable_action,
            is_revertable_action,
            execute_action_initiated_by,
            execute_action_initiated_time,
            revert_action_initiated_by,
            revert_action_initiated_time,
            implementation_script
        FROM plan_correction
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   recommendation_name IS NOT NULL
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ORDER BY collection_time DESC, score DESC, recommendation_name
        LIMIT 200
        """;

    /// <summary>
    /// The latest FORCE_LAST_GOOD_PLAN enablement snapshot per database. The enablement columns repeat on
    /// every one of a database's recommendation rows, so this takes the newest capture for the server and
    /// DISTINCTs it back to one row per database. $1 server_id, $2 db filter.
    /// </summary>
    public const string AutomaticTuningSql = """
        SELECT DISTINCT
            database_name,
            force_last_good_plan_desired_state,
            force_last_good_plan_actual_state,
            force_last_good_plan_reason,
            create_index_actual_state,
            drop_index_actual_state,
            collection_time
        FROM plan_correction
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM plan_correction WHERE server_id = $1)
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        ORDER BY database_name
        """;

    /// <summary>Automatic plan correction recommendations over the window (Plan Corrections grid).</summary>
    public async Task<List<PlanCorrectionRow>> GetPlanCorrectionsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<PlanCorrectionRow>();

        await using var command = _dataSource.CreateCommand(PlanCorrectionsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new PlanCorrectionRow
            {
                CollectionTime = reader.GetDateTime(0),
                QueryText = reader.IsDBNull(1) ? "" : reader.GetString(1),
                DatabaseName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                RecommendationState = reader.IsDBNull(3) ? "" : reader.GetString(3),
                RecommendationStateReason = reader.IsDBNull(4) ? "" : reader.GetString(4),
                RecommendationReason = reader.IsDBNull(5) ? "" : reader.GetString(5),
                Score = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                EstimatedGainSeconds = reader.IsDBNull(7) ? null : reader.GetDouble(7),
                QueryId = reader.IsDBNull(8) ? null : reader.GetInt64(8),
                RegressedPlanId = reader.IsDBNull(9) ? null : reader.GetInt64(9),
                LastGoodPlanId = reader.IsDBNull(10) ? null : reader.GetInt64(10),
                LastGoodPlanForcingType = reader.IsDBNull(11) ? "" : reader.GetString(11),
                LastGoodPlanIsForced = reader.IsDBNull(12) ? null : reader.GetBoolean(12),
                LastGoodPlanForceFailureReason = reader.IsDBNull(13) ? "" : reader.GetString(13),
                RegressedPlanExecutionCount = reader.IsDBNull(14) ? null : reader.GetInt64(14),
                RegressedPlanCpuTimeAverageMs = reader.IsDBNull(15) ? null : reader.GetDouble(15),
                LastGoodPlanExecutionCount = reader.IsDBNull(16) ? null : reader.GetInt64(16),
                LastGoodPlanCpuTimeAverageMs = reader.IsDBNull(17) ? null : reader.GetDouble(17),
                ValidSince = reader.IsDBNull(18) ? null : reader.GetDateTime(18),
                LastRefresh = reader.IsDBNull(19) ? null : reader.GetDateTime(19),
                IsExecutableAction = reader.IsDBNull(20) ? null : reader.GetBoolean(20),
                IsRevertableAction = reader.IsDBNull(21) ? null : reader.GetBoolean(21),
                ExecuteActionInitiatedBy = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ExecuteActionInitiatedTime = reader.IsDBNull(23) ? null : reader.GetDateTime(23),
                RevertActionInitiatedBy = reader.IsDBNull(24) ? "" : reader.GetString(24),
                RevertActionInitiatedTime = reader.IsDBNull(25) ? null : reader.GetDateTime(25),
                ImplementationScript = reader.IsDBNull(26) ? "" : reader.GetString(26),
            });
        }

        return rows;
    }

    /// <summary>Latest per-database FORCE_LAST_GOOD_PLAN enablement snapshot (Automatic Tuning grid).</summary>
    public async Task<List<AutomaticTuningRow>> GetLatestAutomaticTuningAsync(
        int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<AutomaticTuningRow>();

        await using var command = _dataSource.CreateCommand(AutomaticTuningSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new AutomaticTuningRow
            {
                DatabaseName = reader.IsDBNull(0) ? "" : reader.GetString(0),
                DesiredState = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ActualState = reader.IsDBNull(2) ? "" : reader.GetString(2),
                Reason = reader.IsDBNull(3) ? "" : reader.GetString(3),
                CreateIndexState = reader.IsDBNull(4) ? "" : reader.GetString(4),
                DropIndexState = reader.IsDBNull(5) ? "" : reader.GetString(5),
                CollectionTime = reader.GetDateTime(6),
            });
        }

        return rows;
    }
}

/// <summary>
/// One automatic plan correction recommendation grid row (#1952): the engine's own regression finding for a
/// Query Store query — its state, the regressed and last-good plans with their per-execution CPU, and what
/// the engine did or could do about it. Mirrors Lite's <c>PlanCorrectionRow</c>; the timestamps are stored
/// naive-UTC, so the display properties convert via <see cref="ViewerTimeHelper.ForDisplay"/>.
/// </summary>
public class PlanCorrectionRow
{
    public DateTime CollectionTime { get; set; }
    public string QueryText { get; set; } = "";
    public string DatabaseName { get; set; } = "";
    public string RecommendationState { get; set; } = "";
    public string RecommendationStateReason { get; set; } = "";
    public string RecommendationReason { get; set; } = "";
    public int? Score { get; set; }
    public double? EstimatedGainSeconds { get; set; }
    public long? QueryId { get; set; }
    public long? RegressedPlanId { get; set; }
    public long? LastGoodPlanId { get; set; }
    public string LastGoodPlanForcingType { get; set; } = "";
    public bool? LastGoodPlanIsForced { get; set; }
    public string LastGoodPlanForceFailureReason { get; set; } = "";
    public long? RegressedPlanExecutionCount { get; set; }
    public double? RegressedPlanCpuTimeAverageMs { get; set; }
    public long? LastGoodPlanExecutionCount { get; set; }
    public double? LastGoodPlanCpuTimeAverageMs { get; set; }
    public DateTime? ValidSince { get; set; }
    public DateTime? LastRefresh { get; set; }
    public bool? IsExecutableAction { get; set; }
    public bool? IsRevertableAction { get; set; }
    public string ExecuteActionInitiatedBy { get; set; } = "";
    public DateTime? ExecuteActionInitiatedTime { get; set; }
    public string RevertActionInitiatedBy { get; set; } = "";
    public DateTime? RevertActionInitiatedTime { get; set; }
    public string ImplementationScript { get; set; } = "";

    public string CollectionTimeLocal => Local(CollectionTime);
    public string ValidSinceLocal => Local(ValidSince);
    public string LastRefreshLocal => Local(LastRefresh);
    public string ExecuteActionInitiatedTimeLocal => Local(ExecuteActionInitiatedTime);
    public string RevertActionInitiatedTimeLocal => Local(RevertActionInitiatedTime);

    /* Tri-state: the flags are NULL when Query Store aged the plan out, which is not the same as "No". */
    public string ForcedDisplay => YesNo(LastGoodPlanIsForced);
    public string ExecutableDisplay => YesNo(IsExecutableAction);
    public string RevertableDisplay => YesNo(IsRevertableAction);

    internal static string Local(DateTime? naiveUtc)
        => naiveUtc is { } utc ? ViewerTimeHelper.ForDisplay(utc).ToString("yyyy-MM-dd HH:mm:ss") : "";

    private static string YesNo(bool? value) => value is bool flag ? (flag ? "Yes" : "No") : "";
}

/// <summary>
/// One FORCE_LAST_GOOD_PLAN enablement row (#1952) — the latest per-database automatic tuning state. Reason
/// is populated only when the engine could not honour the desired state.
/// </summary>
public class AutomaticTuningRow
{
    public string DatabaseName { get; set; } = "";
    public string DesiredState { get; set; } = "";
    public string ActualState { get; set; } = "";
    public string Reason { get; set; } = "";
    public string CreateIndexState { get; set; } = "";
    public string DropIndexState { get; set; } = "";
    public DateTime CollectionTime { get; set; }

    public string CollectionTimeLocal => PlanCorrectionRow.Local(CollectionTime);
}
