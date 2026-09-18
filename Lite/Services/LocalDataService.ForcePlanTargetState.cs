/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>The read-time forcing/APC state for a finding's force-plan targets (#3652) — the instance
    /// entry point beside the other plan-correction readers; <see cref="ForcePlanTargetStateReader"/> holds
    /// the statement so the analysis service (which owns no <see cref="LocalDataService"/>) reads the same
    /// SQL through its own connection.</summary>
    public async Task<IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>> GetForcePlanTargetStatesAsync(
        int serverId, IReadOnlyList<ForcePlanTarget> targets, CancellationToken cancellationToken = default)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        return await ForcePlanTargetStateReader.ReadAsync(command, serverId, targets, DateTime.UtcNow, cancellationToken);
    }
}

/// <summary>
/// The DuckDB read behind <c>FactRemediation.BuildStructuredRemediation</c>'s state-taking overload
/// (#3652): for a list of (database, query_id, plan_id) force targets, what the store's two forcing-aware
/// collectors last saw. Darling's <c>DarlingForcePlanTargetStateReader</c> is the Postgres twin, kept
/// shape-for-shape (the Lite.Tests parity pin holds the two texts equal but for the view names and the
/// parameter casts) so the two SKUs cannot reach different verdicts from the same facts.
///
/// <para><b>One statement, one row per requested target, LEFT JOINs throughout.</b> The targets travel as
/// a <c>VALUES</c> list (three positional parameters per target after the two fixed ones) and are the
/// driving table, so every requested target comes back — with all-null halves when nothing was collected
/// for it — and the pure function can tell "no snapshot" (a note) from "snapshot says unforced" (a fact).
/// Four halves per row:</para>
/// <list type="bullet">
/// <item><b>The target plan's newest <c>query_store_stats</c> row.</b> The table holds one row per plan
/// PER INTERVAL per collection and the forcing columns are plan-level attributes repeated across them, so
/// each (plan, collection_time) collapses with MAX first — the same discipline as the forced-plan-failure
/// alert read (<see cref="LocalDataService.ForcePlanFailuresSql"/>), whose <c>force_failure_count</c> this half reads.</item>
/// <item><b>The newest forced plan of the same query that is NOT the target</b>, because SQL Server keeps
/// one forced plan per query and a manual force of the target would replace it silently.</item>
/// <item><b>The newest <c>plan_correction</c> RECOMMENDATION row for (database, query_id)</b> —
/// <c>recommendation_name IS NOT NULL</c> drops the enablement-only rows, as the grid read does.</item>
/// <item><b>The database's FORCE_LAST_GOOD_PLAN enablement at the server's newest capture</b> — the
/// Automatic Tuning grid's own read, joined by database.</item>
/// </list>
///
/// <para>Both scans are bounded by <c>collection_time &gt; $2</c>, the caller's clock minus
/// <see cref="ForcePlanTargetState.Lookback"/>, BOUND rather than written <c>now() - INTERVAL</c>:
/// <c>collection_time</c> is naive UTC and <c>now()</c> is TIMESTAMP WITH TIME ZONE, so the mixed
/// comparison would resolve in the host's session zone (the <see cref="LocalDataService.ForcePlanFailuresSql"/> remarks
/// carry the measurement). The enablement half deliberately ignores the lookback — it is a latest-snapshot
/// read, exactly as the grid's is.</para>
/// </summary>
public static class ForcePlanTargetStateReader
{
    /// <summary>
    /// The statement's fixed head; <see cref="BuildSql"/> splices the <c>VALUES</c> rows in. $1 server_id,
    /// $2 lookback start (naive UTC); $3.. the targets, three per row.
    /// </summary>
    public const string SqlTemplate = @"
WITH targets(database_name, query_id, plan_id) AS (
    VALUES {0}
),
target_queries AS (
    SELECT DISTINCT database_name, query_id FROM targets
),
per_collection AS (
    SELECT
        qs.database_name,
        qs.query_id,
        qs.plan_id,
        qs.collection_time,
        MAX(CASE WHEN qs.is_forced_plan THEN 1 ELSE 0 END) AS forced,
        MAX(COALESCE(qs.plan_forcing_type, '')) AS forcing_type,
        MAX(COALESCE(qs.force_failure_count, 0)) AS failures,
        MAX(COALESCE(qs.last_force_failure_reason, '')) AS reason
    FROM target_queries AS tq
    JOIN v_query_store_stats AS qs
      ON  qs.server_id = $1
      AND qs.database_name = tq.database_name
      AND qs.query_id = tq.query_id
      AND qs.collection_time > $2
    GROUP BY qs.database_name, qs.query_id, qs.plan_id, qs.collection_time
),
plan_latest AS (
    SELECT
        pc.*,
        ROW_NUMBER() OVER (PARTITION BY pc.database_name, pc.query_id, pc.plan_id ORDER BY pc.collection_time DESC) AS rn
    FROM per_collection AS pc
),
other_forced AS (
    SELECT
        pl.database_name,
        pl.query_id,
        pl.plan_id,
        pl.forcing_type,
        pl.collection_time,
        ROW_NUMBER() OVER (PARTITION BY pl.database_name, pl.query_id ORDER BY pl.collection_time DESC) AS rn
    FROM plan_latest AS pl
    WHERE pl.rn = 1
    AND   pl.forced = 1
),
apc_latest AS (
    SELECT
        p.database_name,
        p.query_id,
        p.recommendation_state,
        p.recommendation_state_reason,
        p.regressed_plan_id,
        p.last_good_plan_id,
        p.last_good_plan_forcing_type,
        p.last_good_plan_is_forced,
        p.last_good_plan_force_failure_reason,
        p.execute_action_initiated_by,
        p.collection_time,
        ROW_NUMBER() OVER (PARTITION BY p.database_name, p.query_id ORDER BY p.collection_time DESC, p.score DESC) AS rn
    FROM target_queries AS tq
    JOIN v_plan_correction AS p
      ON  p.server_id = $1
      AND p.database_name = tq.database_name
      AND p.query_id = tq.query_id
      AND p.collection_time > $2
    WHERE p.recommendation_name IS NOT NULL
),
enablement AS (
    SELECT DISTINCT
        e.database_name,
        e.force_last_good_plan_actual_state,
        e.collection_time
    FROM v_plan_correction AS e
    WHERE e.server_id = $1
    AND   e.collection_time = (SELECT MAX(collection_time) FROM v_plan_correction WHERE server_id = $1)
)
SELECT
    t.database_name,
    t.query_id,
    t.plan_id,
    tp.forced,
    tp.forcing_type,
    tp.failures,
    tp.reason,
    tp.collection_time AS plan_observed_at,
    op.plan_id AS other_forced_plan_id,
    op.forcing_type AS other_forced_plan_forcing_type,
    op.collection_time AS other_forced_plan_observed_at,
    a.recommendation_state,
    a.recommendation_state_reason,
    a.regressed_plan_id,
    a.last_good_plan_id,
    a.last_good_plan_forcing_type,
    a.last_good_plan_is_forced,
    a.last_good_plan_force_failure_reason,
    a.execute_action_initiated_by,
    a.collection_time AS apc_observed_at,
    en.force_last_good_plan_actual_state,
    en.collection_time AS enablement_observed_at
FROM targets AS t
LEFT JOIN plan_latest AS tp
  ON  tp.database_name = t.database_name
  AND tp.query_id = t.query_id
  AND tp.plan_id = t.plan_id
  AND tp.rn = 1
LEFT JOIN other_forced AS op
  ON  op.database_name = t.database_name
  AND op.query_id = t.query_id
  AND op.plan_id <> t.plan_id
  AND op.rn = 1
LEFT JOIN apc_latest AS a
  ON  a.database_name = t.database_name
  AND a.query_id = t.query_id
  AND a.rn = 1
LEFT JOIN enablement AS en
  ON  en.database_name = t.database_name
ORDER BY t.database_name, t.query_id, t.plan_id";

    /// <summary>The <c>VALUES</c> row for target <paramref name="index"/> (0-based): parameters $3, $4, $5
    /// for the first, $6, $7, $8 for the second, and so on. DuckDB infers the column types from the bound
    /// values; Darling's twin adds <c>::text</c> / <c>::bigint</c> casts because Postgres will not.</summary>
    public static string ValuesRow(int index) =>
        $"(${3 + index * 3}, ${4 + index * 3}, ${5 + index * 3})";

    /// <summary>The complete statement for <paramref name="targetCount"/> targets.</summary>
    public static string BuildSql(int targetCount)
    {
        if (targetCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(targetCount), "at least one target is required");
        }

        var rows = new StringBuilder();
        for (var i = 0; i < targetCount; i++)
        {
            if (i > 0) rows.Append(", ");
            rows.Append(ValuesRow(i));
        }

        return SqlTemplate.Replace("{0}", rows.ToString(), StringComparison.Ordinal);
    }

    /// <summary>
    /// Runs the statement through a command the caller created on its own open connection (the
    /// <c>LockedConnection</c> and the analysis service's raw <c>DuckDBConnection</c> both hand one out, and
    /// share nothing else). Distinct targets only — a finding can carry the same
    /// (database, query_id, plan_id) twice across replicas (#1882), and the VALUES list would then hand
    /// back two identical rows for one key. <paramref name="nowUtc"/> is the caller's clock so a test can
    /// pin the window edge; production passes <see cref="DateTime.UtcNow"/>.
    /// </summary>
    public static async Task<IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>> ReadAsync(
        DuckDBCommand command, int serverId, IReadOnlyList<ForcePlanTarget> targets, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer);
        var keys = targets.Select(ForcePlanTargetKey.Of).Distinct(ForcePlanTargetKey.Comparer).ToList();
        if (keys.Count == 0)
        {
            return result;
        }

        command.CommandText = BuildSql(keys.Count);
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = nowUtc - ForcePlanTargetState.Lookback });
        foreach (var key in keys)
        {
            command.Parameters.Add(new DuckDBParameter { Value = key.Database });
            command.Parameters.Add(new DuckDBParameter { Value = key.QueryId });
            command.Parameters.Add(new DuckDBParameter { Value = key.PlanId });
        }

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var key = new ForcePlanTargetKey(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                Convert.ToInt64(reader.GetValue(1)),
                Convert.ToInt64(reader.GetValue(2)));

            result[key] = new ForcePlanTargetState(
                PlanIsForced: reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3)) == 1,
                PlanForcingType: NullIfEmpty(reader, 4),
                ForceFailureCount: reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5)),
                LastForceFailureReason: NullIfEmpty(reader, 6),
                PlanObservedAtUtc: reader.IsDBNull(7) ? null : Utc(reader.GetDateTime(7)),
                OtherForcedPlanId: reader.IsDBNull(8) ? null : Convert.ToInt64(reader.GetValue(8)),
                OtherForcedPlanForcingType: NullIfEmpty(reader, 9),
                OtherForcedPlanObservedAtUtc: reader.IsDBNull(10) ? null : Utc(reader.GetDateTime(10)),
                ApcState: NullIfEmpty(reader, 11),
                ApcStateReason: NullIfEmpty(reader, 12),
                ApcRegressedPlanId: reader.IsDBNull(13) ? null : Convert.ToInt64(reader.GetValue(13)),
                ApcLastGoodPlanId: reader.IsDBNull(14) ? null : Convert.ToInt64(reader.GetValue(14)),
                ApcLastGoodPlanForcingType: NullIfEmpty(reader, 15),
                ApcLastGoodPlanIsForced: reader.IsDBNull(16) ? null : Convert.ToBoolean(reader.GetValue(16)),
                ApcLastGoodPlanForceFailureReason: NullIfEmpty(reader, 17),
                ApcExecuteActionInitiatedBy: NullIfEmpty(reader, 18),
                ApcObservedAtUtc: reader.IsDBNull(19) ? null : Utc(reader.GetDateTime(19)),
                ForceLastGoodPlanActualState: NullIfEmpty(reader, 20),
                EnablementObservedAtUtc: reader.IsDBNull(21) ? null : Utc(reader.GetDateTime(21)));
        }

        return result;

        static string? NullIfEmpty(System.Data.Common.DbDataReader r, int ordinal)
        {
            if (r.IsDBNull(ordinal)) return null;
            var s = r.GetString(ordinal);
            return string.IsNullOrEmpty(s) ? null : s;
        }

        /* A naive-UTC TIMESTAMP read back Kind-Unspecified, stamped Utc because that is what it is. */
        static DateTime Utc(DateTime t) => DateTime.SpecifyKind(t, DateTimeKind.Utc);
    }
}
