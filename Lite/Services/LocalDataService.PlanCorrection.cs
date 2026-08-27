/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The engine's automatic plan correction recommendations (#1952) from <c>v_plan_correction</c> (the
    /// archive union view), for the Plan Corrections grid. <c>recommendation_name IS NOT NULL</c> drops the
    /// enablement-only row the collector emits for a database the engine has nothing to recommend for —
    /// that half of the payload feeds <see cref="GetLatestAutomaticTuningAsync"/> instead. The grid applies
    /// a view-only DESCENDING-by-score sort, so the SQL keeps the chronological ORDER BY (mirrors the
    /// long-query reader).
    /// </summary>
    public async Task<List<PlanCorrectionRow>> GetPlanCorrectionsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
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
FROM v_plan_correction
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   recommendation_name IS NOT NULL" + dbClause + @"
ORDER BY collection_time DESC, score DESC, recommendation_name
LIMIT 200";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<PlanCorrectionRow>();
        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                items.Add(new PlanCorrectionRow
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
        }

        return items;
    }

    /// <summary>
    /// The latest FORCE_LAST_GOOD_PLAN enablement snapshot per database (#1952), for the Automatic Tuning
    /// grid. The enablement columns repeat on every one of a database's recommendation rows, so this takes
    /// the newest capture for the server and DISTINCTs it back to one row per database. reason is NULL when
    /// the desired state was honoured and carries the engine's own explanation (QUERY_STORE_OFF /
    /// NOT_SUPPORTED / AUTO_CONFIGURED) when it was not — that is the whole diagnostic value of the grid, so
    /// null-reason rows are kept.
    /// </summary>
    public async Task<List<AutomaticTuningRow>> GetLatestAutomaticTuningAsync(int serverId, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 2, out var dbValues);
        command.CommandText = @"
SELECT DISTINCT
    database_name,
    force_last_good_plan_desired_state,
    force_last_good_plan_actual_state,
    force_last_good_plan_reason,
    create_index_actual_state,
    drop_index_actual_state,
    collection_time
FROM v_plan_correction
WHERE server_id = $1
AND   collection_time = (SELECT MAX(collection_time) FROM v_plan_correction WHERE server_id = $1)" + dbClause + @"
ORDER BY database_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<AutomaticTuningRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new AutomaticTuningRow
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

        return items;
    }
}

/// <summary>
/// One automatic plan correction recommendation grid row (#1952): the engine's own regression finding for a
/// Query Store query — its state, the regressed and last-good plans with their per-execution CPU, and what
/// the engine did or could do about it. All of it is erased by a restart, so a captured row may be the only
/// surviving record that the regression happened.
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

    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
    public string ValidSinceLocal => ServerTimeHelper.FormatServerTime(ValidSince);
    public string LastRefreshLocal => ServerTimeHelper.FormatServerTime(LastRefresh);
    public string ExecuteActionInitiatedTimeLocal => ServerTimeHelper.FormatServerTime(ExecuteActionInitiatedTime);
    public string RevertActionInitiatedTimeLocal => ServerTimeHelper.FormatServerTime(RevertActionInitiatedTime);

    /* Tri-state: the flags are NULL when Query Store aged the plan out, which is not the same as "No". */
    public string ForcedDisplay => YesNo(LastGoodPlanIsForced);
    public string ExecutableDisplay => YesNo(IsExecutableAction);
    public string RevertableDisplay => YesNo(IsRevertableAction);

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

    public string CollectionTimeLocal => ServerTimeHelper.FormatServerTime(CollectionTime);
}
