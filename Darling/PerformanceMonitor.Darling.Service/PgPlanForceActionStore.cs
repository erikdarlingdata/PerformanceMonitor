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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One journal row (V107 <c>collect.plan_force_actions</c>). Timestamps naive UTC.</summary>
public sealed record PlanForceActionRecord(
    long ActionId,
    DateTime ActionTimeUtc,
    int ServerId,
    string ServerName,
    string DatabaseName,
    long QueryId,
    long PlanId,
    string Action,
    string Mode,
    string Decision,
    string Reasons,
    double RegressionFactor,
    double LatestCpuPerExecUs,
    double BestCpuPerExecUs,
    string? ReplicaRole,
    bool ParameterSensitivityCoFired,
    string Outcome,
    string? Detail,
    long? RelatedActionId);

/// <summary>
/// The slice of the journal the BOT consumes — a seam so the orchestrator is testable with an
/// in-memory fake (the write-gate re-checks in <c>PlanForceBot</c> are exactly the logic that must
/// be provable without a live store). The audit read (<c>GetRecentActionsAsync</c>) stays on the
/// concrete class: it serves future read surfaces, not the bot.
/// </summary>
public interface IPlanForceActionStore
{
    Task<long> JournalAsync(PlanForceActionRecord record, CancellationToken ct);

    Task<ForcePlanBotHistory> GetQueryHistoryAsync(
        int serverId, string database, long queryId, ForcePlanBotSettings settings, DateTime nowUtc, CancellationToken ct);

    Task<IReadOnlyList<PlanForceActionRecord>> GetPendingReviewsAsync(int serverId, CancellationToken ct);
}

/// <summary>
/// The force-plan bot's journal over V107 <c>collect.plan_force_actions</c> (#2138). Append-only on
/// purpose: an audit trail the bot could UPDATE would be an audit of nothing, so outcomes and
/// reviews are their own rows pointing back through <c>related_action_id</c>. The history read is
/// WINDOWED — it counts failures inside the cooldown horizon rather than ever — because that is
/// what makes the policy's give-up state self-healing (see <see cref="ForcePlanBotHistory"/>).
/// </summary>
public sealed class PgPlanForceActionStore : IPlanForceActionStore
{
    /* The journal's action vocabulary. Strings rather than an enum at the wire so the table stays
       readable in psql and a future Lite twin shares the exact values. */
    public const string ActionWouldForce = "would_force";
    public const string ActionBlocked = "blocked";
    public const string ActionForce = "force";
    public const string ActionUnforce = "unforce";
    public const string ActionReview = "review";

    public const string ModeDryRun = "dry_run";
    public const string ModeLive = "live";

    public const string OutcomeLogged = "logged";
    public const string OutcomeAttempting = "attempting";
    public const string OutcomeSucceeded = "succeeded";
    public const string OutcomeFailed = "failed";

    private readonly NpgsqlDataSource _postgres;

    public PgPlanForceActionStore(NpgsqlDataSource postgres)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
    }

    /// <summary>Appends one row and returns its <c>action_id</c> so a follow-up row can reference it.</summary>
    public async Task<long> JournalAsync(PlanForceActionRecord record, CancellationToken ct)
    {
        if (record is null)
        {
            throw new ArgumentNullException(nameof(record));
        }

        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.plan_force_actions (
    action_time, server_id, server_name, database_name, query_id, plan_id,
    action, mode, decision, reasons,
    regression_factor, latest_cpu_per_exec_us, best_cpu_per_exec_us,
    replica_role, parameter_sensitivity_cofired, outcome, detail, related_action_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
RETURNING action_id", connection);

        /* Naive UTC per the store convention — Kind=Utc would make Npgsql infer timestamptz and
           silently zone-shift (the two-store parity trap). */
        command.Parameters.AddWithValue(DateTime.SpecifyKind(record.ActionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(record.ServerId);
        command.Parameters.AddWithValue(record.ServerName);
        command.Parameters.AddWithValue(record.DatabaseName);
        command.Parameters.AddWithValue(record.QueryId);
        command.Parameters.AddWithValue(record.PlanId);
        command.Parameters.AddWithValue(record.Action);
        command.Parameters.AddWithValue(record.Mode);
        command.Parameters.AddWithValue(record.Decision);
        command.Parameters.AddWithValue(record.Reasons);
        command.Parameters.AddWithValue(record.RegressionFactor);
        command.Parameters.AddWithValue(record.LatestCpuPerExecUs);
        command.Parameters.AddWithValue(record.BestCpuPerExecUs);
        command.Parameters.AddWithValue((object?)record.ReplicaRole ?? DBNull.Value);
        command.Parameters.AddWithValue(record.ParameterSensitivityCoFired);
        command.Parameters.AddWithValue(record.Outcome);
        command.Parameters.AddWithValue((object?)record.Detail ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)record.RelatedActionId ?? DBNull.Value);

        var result = await command.ExecuteScalarAsync(ct);
        return Convert.ToInt64(result, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The policy's history inputs for one (server, database, query), in one round trip. Each
    /// aggregate carries its own window so the cooldowns are properties of the READ:
    /// <list type="bullet">
    /// <item>last journaled decision for the query (would_force/blocked/force — the kinds the
    /// per-query cooldown dedupes);</item>
    /// <item>actionable decisions for the whole server in the trailing 24h (would_force + force,
    /// so the dry run spends the same budget the live bot would);</item>
    /// <item>failed forces for the query inside the failure-memory window: force rows whose outcome
    /// is failed, plus unforce rows the self-review issued (not_net_benefit / force_failing).</item>
    /// </list>
    /// </summary>
    public async Task<ForcePlanBotHistory> GetQueryHistoryAsync(
        int serverId, string database, long queryId, ForcePlanBotSettings settings, DateTime nowUtc, CancellationToken ct)
    {
        if (settings is null)
        {
            throw new ArgumentNullException(nameof(settings));
        }

        var now = DateTime.SpecifyKind(nowUtc, DateTimeKind.Unspecified);

        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT
    (SELECT MAX(pfa.action_time)
     FROM collect.plan_force_actions AS pfa
     WHERE pfa.server_id = $1
     AND   pfa.database_name = $2
     AND   pfa.query_id = $3
     AND   pfa.action IN ('would_force', 'blocked', 'force')) AS last_journaled,
    (SELECT COUNT(*)
     FROM collect.plan_force_actions AS pfa
     WHERE pfa.server_id = $1
     AND   pfa.action IN ('would_force', 'force')
     /* Decision rows only: a live force writes an intent row AND a completion row (both
        action='force', the completion pointing back via related_action_id), and counting both
        would spend the daily budget at double rate for exactly the actions it exists to bound. */
     AND   pfa.related_action_id IS NULL
     AND   pfa.action_time > $4) AS server_actions_24h,
    (SELECT COUNT(*)
     FROM collect.plan_force_actions AS pfa
     WHERE pfa.server_id = $1
     AND   pfa.database_name = $2
     AND   pfa.query_id = $3
     AND   pfa.action_time > $5
     AND   ((pfa.action = 'force' AND pfa.outcome = 'failed')
            OR (pfa.action = 'unforce' AND pfa.decision IN ('not_net_benefit', 'force_failing')))) AS recent_failed", connection);

        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(queryId);
        command.Parameters.AddWithValue(now.AddHours(-24));
        command.Parameters.AddWithValue(now.AddHours(-settings.FailedForceCooldownHours));

        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return ForcePlanBotHistory.Empty;
        }

        DateTime? lastJournaled = reader.IsDBNull(0)
            ? null
            : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);

        return new ForcePlanBotHistory(
            lastJournaled,
            Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture),
            Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// Live forces still owed a review: succeeded force rows with no terminal follow-up row (an
    /// unforce, or a review row whose decision closed it). OWN-FORCES-ONLY is structural here — the
    /// read starts from rows this bot journaled, so an operator's hand-placed force can never
    /// surface as something to unforce.
    /// </summary>
    public async Task<IReadOnlyList<PlanForceActionRecord>> GetPendingReviewsAsync(
        int serverId, CancellationToken ct)
    {
        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT pfa.action_id, pfa.action_time, pfa.server_id, pfa.server_name, pfa.database_name,
       pfa.query_id, pfa.plan_id, pfa.action, pfa.mode, pfa.decision, pfa.reasons,
       pfa.regression_factor, pfa.latest_cpu_per_exec_us, pfa.best_cpu_per_exec_us,
       pfa.replica_role, pfa.parameter_sensitivity_cofired, pfa.outcome, pfa.detail, pfa.related_action_id
FROM collect.plan_force_actions AS pfa
WHERE pfa.server_id = $1
AND   pfa.action = 'force'
AND   pfa.outcome = 'succeeded'
AND   NOT EXISTS (
        SELECT 1
        FROM collect.plan_force_actions AS closer
        WHERE closer.related_action_id = pfa.action_id
        AND   closer.action IN ('unforce', 'review'))
ORDER BY pfa.action_time
LIMIT 16", connection);

        command.Parameters.AddWithValue(serverId);

        var rows = new List<PlanForceActionRecord>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRecord(reader));
        }

        return rows;
    }

    /// <summary>The audit read behind <c>get_plan_force_actions</c> — newest first, optional server scope.</summary>
    public async Task<IReadOnlyList<PlanForceActionRecord>> GetRecentActionsAsync(
        int? serverId, DateTime sinceUtc, int limit, CancellationToken ct)
    {
        await using var connection = await _postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(@"
SELECT pfa.action_id, pfa.action_time, pfa.server_id, pfa.server_name, pfa.database_name,
       pfa.query_id, pfa.plan_id, pfa.action, pfa.mode, pfa.decision, pfa.reasons,
       pfa.regression_factor, pfa.latest_cpu_per_exec_us, pfa.best_cpu_per_exec_us,
       pfa.replica_role, pfa.parameter_sensitivity_cofired, pfa.outcome, pfa.detail, pfa.related_action_id
FROM collect.plan_force_actions AS pfa
WHERE pfa.action_time > $1
AND   ($2::integer IS NULL OR pfa.server_id = $2)
ORDER BY pfa.action_time DESC
LIMIT $3", connection);

        command.Parameters.AddWithValue(DateTime.SpecifyKind(sinceUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue((object?)serverId ?? DBNull.Value);
        command.Parameters.AddWithValue(limit);

        var rows = new List<PlanForceActionRecord>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(ReadRecord(reader));
        }

        return rows;
    }

    private static PlanForceActionRecord ReadRecord(NpgsqlDataReader reader) => new(
        ActionId: reader.GetInt64(0),
        ActionTimeUtc: DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
        ServerId: reader.GetInt32(2),
        ServerName: reader.GetString(3),
        DatabaseName: reader.GetString(4),
        QueryId: reader.GetInt64(5),
        PlanId: reader.GetInt64(6),
        Action: reader.GetString(7),
        Mode: reader.GetString(8),
        Decision: reader.GetString(9),
        Reasons: reader.GetString(10),
        RegressionFactor: Convert.ToDouble(reader.GetValue(11), CultureInfo.InvariantCulture),
        LatestCpuPerExecUs: Convert.ToDouble(reader.GetValue(12), CultureInfo.InvariantCulture),
        BestCpuPerExecUs: Convert.ToDouble(reader.GetValue(13), CultureInfo.InvariantCulture),
        ReplicaRole: reader.IsDBNull(14) ? null : reader.GetString(14),
        ParameterSensitivityCoFired: reader.GetBoolean(15),
        Outcome: reader.GetString(16),
        Detail: reader.IsDBNull(17) ? null : reader.GetString(17),
        RelatedActionId: reader.IsDBNull(18) ? null : reader.GetInt64(18));
}
