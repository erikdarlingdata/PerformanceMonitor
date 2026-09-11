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
using NpgsqlTypes;
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Persists the <see cref="AlertPersistenceGate"/>'s per-<c>(rule_id, server_id)</c> hysteresis state to
/// <c>config.custom_alert_state</c> (#3285). Written by the <see cref="CustomAlertEvaluator"/> on the worker's
/// OWNER pool (the evaluator already reads collect + writes <c>config_alert_log</c> there), so this store does
/// NOT go through the least-privilege viewer/mcp pools and needs no extra grant.
///
/// <para>The resettable consecutive-breach/clear counter is the shape no existing state table offered (the
/// edge-trigger watermark is a monotonic single int; <c>incident_occurrences</c> is a replace-the-set
/// accumulator). <c>rule_version</c> is stamped so the evaluator can detect a threshold edit and reset the
/// streak — <see cref="LoadAsync"/> zeroes the counters when the stored version differs from the rule's
/// current version, so a stale count never fires against a new bar — while preserving <c>firing</c> so an
/// already-open incident is not silently dropped (it resolves normally). <c>next_due_at</c> carries per-rule
/// cadence so a single sweep timer can still honor a rule's own interval.</para>
/// </summary>
public sealed class CustomAlertStateStore
{
    private readonly NpgsqlDataSource _dataSource;

    public CustomAlertStateStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <summary>Reads one subject's state. $1 rule_id, $2 server_id.</summary>
    public const string LoadSql = @"
SELECT rule_version, consecutive_breaches, consecutive_clears, firing, fired_severity, last_evaluated_at, next_due_at
FROM custom_alert_state
WHERE rule_id = $1 AND server_id = $2";

    /// <summary>Upserts one subject's state. $1 rule_id, $2 server_id, $3 rule_version, $4 consecutive_breaches,
    /// $5 consecutive_clears, $6 firing, $7 fired_severity, $8 last_evaluated_at, $9 next_due_at.</summary>
    public const string SaveSql = @"
INSERT INTO custom_alert_state
    (rule_id, server_id, rule_version, consecutive_breaches, consecutive_clears, firing, fired_severity,
     last_evaluated_at, next_due_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, (now() AT TIME ZONE 'UTC'))
ON CONFLICT (rule_id, server_id) DO UPDATE SET
    rule_version = EXCLUDED.rule_version,
    consecutive_breaches = EXCLUDED.consecutive_breaches,
    consecutive_clears = EXCLUDED.consecutive_clears,
    firing = EXCLUDED.firing,
    fired_severity = EXCLUDED.fired_severity,
    last_evaluated_at = EXCLUDED.last_evaluated_at,
    next_due_at = EXCLUDED.next_due_at,
    updated_at = (now() AT TIME ZONE 'UTC')";

    /// <summary>Every subject-state row for one rule (both firing and not). $1 rule_id. Used to resolve+prune
    /// servers that have left the rule's scope and to force-resolve open incidents before a rule is deleted.</summary>
    public const string ListForRuleSql = @"
SELECT server_id, firing FROM custom_alert_state WHERE rule_id = $1";

    /// <summary>Deletes one subject's state row (server left scope, after any open incident was resolved).
    /// $1 rule_id, $2 server_id.</summary>
    public const string DeleteSql = "DELETE FROM custom_alert_state WHERE rule_id = $1 AND server_id = $2";

    /// <summary>
    /// Loads one subject's state for the rule at <paramref name="currentRuleVersion"/>. Returns
    /// <see cref="CustomAlertRuleState.Fresh"/> when no row exists. When the stored <c>rule_version</c> differs
    /// (the rule was edited), the consecutive counters are RESET to zero so a stale streak cannot fire against
    /// the new threshold — but <c>firing</c>/<c>fired_severity</c> are preserved so an open incident resolves
    /// normally rather than being orphaned.
    /// </summary>
    public async Task<CustomAlertRuleState> LoadAsync(
        long ruleId, int serverId, int currentRuleVersion, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(LoadSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = ruleId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return CustomAlertRuleState.Fresh(currentRuleVersion);
        }

        var storedVersion = reader.GetInt32(0);
        var persistence = new PersistenceState(
            reader.GetInt32(1),
            reader.GetInt32(2),
            reader.GetBoolean(3));
        var firedSeverity = reader.IsDBNull(4) ? null : reader.GetString(4);
        var lastEvaluatedAt = reader.IsDBNull(5) ? (DateTime?)null : reader.GetDateTime(5);
        var nextDueAt = reader.IsDBNull(6) ? (DateTime?)null : reader.GetDateTime(6);

        if (storedVersion != currentRuleVersion)
        {
            // Threshold (or any definition) edit: reset the streak, keep the incident open so it can resolve.
            persistence = persistence with { ConsecutiveBreaches = 0, ConsecutiveClears = 0 };
        }

        return new CustomAlertRuleState(persistence, currentRuleVersion, firedSeverity, lastEvaluatedAt, nextDueAt);
    }

    /// <summary>Upserts one subject's state.</summary>
    public async Task SaveAsync(
        long ruleId, int serverId, CustomAlertRuleState state, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(SaveSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = ruleId });                          // $1
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });                         // $2
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = state.RuleVersion });                // $3
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = state.Persistence.ConsecutiveBreaches }); // $4
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = state.Persistence.ConsecutiveClears }); // $5
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = state.Persistence.Firing });        // $6
        command.Parameters.Add(new NpgsqlParameter // $7
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = (object?)state.FiredSeverity ?? DBNull.Value,
        });
        AddNullableTimestamp(command, state.LastEvaluatedAt);                                               // $8
        AddNullableTimestamp(command, state.NextDueAt);                                                     // $9
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    /// <summary>Lists every subject-state row for one rule as <c>(serverId, firing)</c>.</summary>
    public async Task<IReadOnlyList<(int ServerId, bool Firing)>> ListForRuleAsync(
        long ruleId, CancellationToken cancellationToken = default)
    {
        var rows = new List<(int, bool)>();
        await using var command = _dataSource.CreateCommand(ListForRuleSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = ruleId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add((reader.GetInt32(0), reader.GetBoolean(1)));
        }

        return rows;
    }

    /// <summary>Deletes one subject's state row.</summary>
    public async Task DeleteAsync(long ruleId, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DeleteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = ruleId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddNullableTimestamp(NpgsqlCommand command, DateTime? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = (object?)value ?? DBNull.Value,
        });
}

/// <summary>
/// One subject's persisted evaluation state: the gate's <see cref="PersistenceState"/> plus the bookkeeping
/// the evaluator needs — the <c>rule_version</c> the state was written under (for streak reset on edit), the
/// severity a currently-firing incident fired at (to detect a Warning→Critical transition and to render the
/// resolve), and the last-evaluated / next-due timestamps that make per-rule cadence work over a single sweep.
/// </summary>
public sealed record CustomAlertRuleState(
    PersistenceState Persistence,
    int RuleVersion,
    string? FiredSeverity,
    DateTime? LastEvaluatedAt,
    DateTime? NextDueAt)
{
    /// <summary>The state of a (rule, server) never evaluated before, at the rule's current version.</summary>
    public static CustomAlertRuleState Fresh(int ruleVersion) =>
        new(PersistenceState.Initial, ruleVersion, null, null, null);
}
