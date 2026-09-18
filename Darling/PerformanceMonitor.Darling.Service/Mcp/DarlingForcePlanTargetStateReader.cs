/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The Postgres read behind <c>FactRemediation.BuildStructuredRemediation</c>'s state-taking overload
/// (#3652) — Lite's <c>ForcePlanTargetStateReader</c> ported, kept shape-for-shape with it the way
/// <see cref="DarlingPlanCorrectionReader"/> twins the Viewer's plan-correction reads: same CTEs, same
/// column list in the same order, so the Lite.Tests parity pin can hold the two texts equal but for the
/// view names and the parameter casts, and the two SKUs cannot reach different verdicts from the same
/// facts. The mechanism remarks live on the Lite twin; what is Darling-specific is here.
///
/// <para><b>Access path.</b> Both halves are bounded by <c>server_id = $1 AND collection_time &gt; $2</c>
/// with $2 the service clock minus <see cref="ForcePlanTargetState.Lookback"/> (BOUND, never
/// <c>now() - interval</c> — <c>DarlingAlertReadAdapter.ForcePlanFailuresSql</c>'s remarks measure why).
/// <c>query_store_stats</c> is a hypertable; the distinct target queries are an inner JOIN
/// (<c>target_queries</c>) rather than an EXISTS filter so the planner MAY drive from them and seek
/// <c>idx_query_store_stats_server_db_query_plan_time</c> (<c>server_id, database_name, query_id, plan_id,
/// collection_time DESC</c>) on its leading columns per target. Stated as "may" on purpose: on the seeded
/// rig (seven rows, no statistics worth the name) both forms planned as an index scan on
/// <c>(server_id, collection_time &gt; $2)</c> with the targets as a join filter — one server's day, then the
/// filter — which is a row-count decision the rig cannot settle. The production EXPLAIN is the instrument
/// that can (the #3573 lesson: reference a column the chosen index lacks and nothing fails, the plan just
/// changes shape silently); until it has run, the cost ceiling is one server's day of
/// <c>query_store_stats</c> hashed on (database, query, plan, collection_time), once per tool call, only
/// when the result carries force-plan targets.
/// <c>plan_correction</c> is small (one row per open recommendation per capture) and
/// <c>idx_plan_correction_time</c> covers the server/time bound.</para>
///
/// <para><b>The VALUES rows carry casts.</b> Postgres types a <c>VALUES</c> column from its first row and
/// an untyped parameter there is <c>unknown</c>, so every row spells <c>$n::text, $n::bigint, $n::bigint</c>;
/// DuckDB infers from the bound value and the Lite twin omits them. That, and the view names, are the
/// whole of the difference.</para>
/// </summary>
internal static class DarlingForcePlanTargetStateReader
{
    /// <summary>$1 server_id, $2 lookback start (naive UTC), $3.. the targets, three per row.</summary>
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
    JOIN query_store_stats AS qs
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
    JOIN plan_correction AS p
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
    FROM plan_correction AS e
    WHERE e.server_id = $1
    AND   e.collection_time = (SELECT MAX(collection_time) FROM plan_correction WHERE server_id = $1)
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

    /// <summary>The typed <c>VALUES</c> row for target <paramref name="index"/> (0-based).</summary>
    public static string ValuesRow(int index) =>
        $"(${3 + index * 3}::text, ${4 + index * 3}::bigint, ${5 + index * 3}::bigint)";

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
    /// The state for every distinct target across <paramref name="findings"/>' remediations, or null with
    /// the reason when the read failed — the MCP tools put the reason on every target's <c>state_note</c>
    /// and never fail the finding read over its enrichment. <paramref name="nowUtc"/> is the service clock
    /// (the same clock that stamped <c>collection_time</c>), exposed so a rig test can pin the window edge.
    /// </summary>
    public static async Task<(IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>? States, string? UnavailableReason)> TryReadAsync(
        NpgsqlDataSource postgres, int serverId, IEnumerable<AnalysisFinding> findings, DateTime? nowUtc = null,
        CancellationToken cancellationToken = default)
    {
        var targets = new List<ForcePlanTarget>();
        foreach (var finding in findings)
        {
            if (finding.Remediation?.Targets is { Count: > 0 } ts)
                targets.AddRange(ts);
        }

        if (targets.Count == 0)
            return (new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer), null);

        try
        {
            return (await ReadAsync(postgres, serverId, targets, nowUtc ?? DateTime.UtcNow, cancellationToken), null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (null, $"the forcing and automatic-plan-correction state read failed ({ex.GetType().Name}: {ex.Message}); eligible reflects only the finding's own evidence. Check get_plan_corrections and sys.query_store_plan before forcing.");
        }
    }

    /// <summary>Runs the statement. Distinct targets only (a finding can carry one key twice across
    /// replicas, #1882).</summary>
    public static async Task<IReadOnlyDictionary<ForcePlanTargetKey, ForcePlanTargetState>> ReadAsync(
        NpgsqlDataSource postgres, int serverId, IReadOnlyList<ForcePlanTarget> targets, DateTime nowUtc,
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<ForcePlanTargetKey, ForcePlanTargetState>(ForcePlanTargetKey.Comparer);
        var keys = targets.Select(ForcePlanTargetKey.Of).Distinct(ForcePlanTargetKey.Comparer).ToList();
        if (keys.Count == 0)
        {
            return result;
        }

        await using var command = postgres.CreateCommand(BuildSql(keys.Count));
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(nowUtc - ForcePlanTargetState.Lookback, DateTimeKind.Unspecified));
        foreach (var key in keys)
        {
            command.Parameters.AddWithValue(key.Database);
            command.Parameters.AddWithValue(key.QueryId);
            command.Parameters.AddWithValue(key.PlanId);
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var key = new ForcePlanTargetKey(
                reader.IsDBNull(0) ? "" : reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt64(2));

            result[key] = new ForcePlanTargetState(
                PlanIsForced: reader.IsDBNull(3) ? null : Convert.ToInt64(reader.GetValue(3), CultureInfo.InvariantCulture) == 1,
                PlanForcingType: NullIfEmpty(reader, 4),
                ForceFailureCount: reader.IsDBNull(5) ? null : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                LastForceFailureReason: NullIfEmpty(reader, 6),
                PlanObservedAtUtc: reader.IsDBNull(7) ? null : Utc(reader.GetDateTime(7)),
                OtherForcedPlanId: reader.IsDBNull(8) ? null : reader.GetInt64(8),
                OtherForcedPlanForcingType: NullIfEmpty(reader, 9),
                OtherForcedPlanObservedAtUtc: reader.IsDBNull(10) ? null : Utc(reader.GetDateTime(10)),
                ApcState: NullIfEmpty(reader, 11),
                ApcStateReason: NullIfEmpty(reader, 12),
                ApcRegressedPlanId: reader.IsDBNull(13) ? null : reader.GetInt64(13),
                ApcLastGoodPlanId: reader.IsDBNull(14) ? null : reader.GetInt64(14),
                ApcLastGoodPlanForcingType: NullIfEmpty(reader, 15),
                ApcLastGoodPlanIsForced: reader.IsDBNull(16) ? null : reader.GetBoolean(16),
                ApcLastGoodPlanForceFailureReason: NullIfEmpty(reader, 17),
                ApcExecuteActionInitiatedBy: NullIfEmpty(reader, 18),
                ApcObservedAtUtc: reader.IsDBNull(19) ? null : Utc(reader.GetDateTime(19)),
                ForceLastGoodPlanActualState: NullIfEmpty(reader, 20),
                EnablementObservedAtUtc: reader.IsDBNull(21) ? null : Utc(reader.GetDateTime(21)));
        }

        return result;

        static string? NullIfEmpty(NpgsqlDataReader r, int ordinal)
        {
            if (r.IsDBNull(ordinal)) return null;
            var s = r.GetString(ordinal);
            return string.IsNullOrEmpty(s) ? null : s;
        }

        /* A naive-UTC timestamp read back Kind-Unspecified, stamped Utc because that is what it is. */
        static DateTime Utc(DateTime t) => DateTime.SpecifyKind(t, DateTimeKind.Utc);
    }
}
