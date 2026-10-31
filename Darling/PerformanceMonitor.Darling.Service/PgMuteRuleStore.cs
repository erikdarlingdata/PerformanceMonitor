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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Postgres-backed <see cref="IMuteRuleStore"/> over the V3 <c>config_mute_rules</c> table —
/// Lite's <c>DuckDbMuteRuleStore</c> ported statement-for-statement. The shared
/// <see cref="MuteRuleService"/> keeps the in-memory cache + matching; per its contract EVERY method
/// here throws on failure — the mutating ones because the service catches, logs and skips the cache
/// mutation (persist-then-cache ordering), and <see cref="LoadAllAsync"/> because an empty list is the
/// answer "this store holds no rules" and must not also be the answer "I could not read them".
/// Timestamps are stored naive-UTC and tagged UTC on read.
/// </summary>
public sealed class PgMuteRuleStore : IMuteRuleStore
{
    private readonly NpgsqlDataSource? _postgres;
    private readonly NpgsqlConnection? _connection;
    private readonly NpgsqlTransaction? _transaction;

    public PgMuteRuleStore(NpgsqlDataSource postgres)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
    }

    /// <summary>
    /// A store that runs every statement on the given connection and transaction and commits nothing (#5915): a Level 2
    /// agent change writes inside the gate's transaction, so the re-check, the change and the <c>action_done</c> journal
    /// row are one commit.
    /// </summary>
    internal PgMuteRuleStore(NpgsqlConnection connection, NpgsqlTransaction transaction)
    {
        _connection = connection ?? throw new ArgumentNullException(nameof(connection));
        _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));
    }

    private Task<PgStoreScope> OpenAsync(CancellationToken cancellationToken = default) =>
        PgStoreScope.OpenAsync(_postgres, _connection, _transaction, transactional: false, cancellationToken);

    /// <summary>
    /// Every persisted rule, newest first. A store fault propagates: the one caller that caches these rows
    /// leaves its cache alone when this throws, and the MCP reads report the fault rather than answering
    /// "no mute rules are configured for this store". This store carries no logger of its own for the same
    /// reason Lite's does not — the caller that swallows the fault is the one that knows what it cost.
    /// </summary>
    public async Task<IReadOnlyList<MuteRule>> LoadAllAsync(CancellationToken cancellationToken = default)
    {
        var rules = new List<MuteRule>();

        await using var scope = await OpenAsync(cancellationToken);
        using var command = new NpgsqlCommand(@"
SELECT id, enabled, created_at_utc, expires_at_utc, reason,
       server_name, metric_name, database_pattern,
       query_text_pattern, wait_type_pattern, job_name_pattern, server_id, client_app_pattern, external_source, starts_at_utc,
       recur_days, recur_start_time, recur_end_time, recur_time_zone
FROM config_mute_rules
ORDER BY created_at_utc DESC", scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rules.Add(new MuteRule
            {
                Id = reader.GetString(0),
                Enabled = reader.GetBoolean(1),
                CreatedAtUtc = DateTime.SpecifyKind(reader.GetDateTime(2), DateTimeKind.Utc),
                ExpiresAtUtc = reader.IsDBNull(3) ? null : DateTime.SpecifyKind(reader.GetDateTime(3), DateTimeKind.Utc),
                Reason = reader.IsDBNull(4) ? null : reader.GetString(4),
                ServerName = reader.IsDBNull(5) ? null : reader.GetString(5),
                MetricName = reader.IsDBNull(6) ? null : reader.GetString(6),
                DatabasePattern = reader.IsDBNull(7) ? null : reader.GetString(7),
                QueryTextPattern = reader.IsDBNull(8) ? null : reader.GetString(8),
                WaitTypePattern = reader.IsDBNull(9) ? null : reader.GetString(9),
                JobNamePattern = reader.IsDBNull(10) ? null : reader.GetString(10),
                ServerId = reader.IsDBNull(11) ? null : reader.GetInt32(11),
                ClientAppPattern = reader.IsDBNull(12) ? null : reader.GetString(12),
                ExternalSource = reader.IsDBNull(13) ? null : reader.GetString(13),
                StartsAtUtc = reader.IsDBNull(14) ? null : DateTime.SpecifyKind(reader.GetDateTime(14), DateTimeKind.Utc),
                RecurDays = reader.IsDBNull(15) ? null : reader.GetInt16(15),
                RecurStartTime = reader.IsDBNull(16) ? null : reader.GetFieldValue<TimeOnly>(16),
                RecurEndTime = reader.IsDBNull(17) ? null : reader.GetFieldValue<TimeOnly>(17),
                RecurTimeZone = reader.IsDBNull(18) ? null : reader.GetString(18)
            });
        }

        return rules;
    }

    /// <summary>
    /// A repeating rule's zone must be one this store's PostgreSQL resolves (#7040): the fleet statement reads any other as outside
    /// the window, so a rule saved with one would mute nothing and give no sign why. The service host's own check
    /// (<see cref="MuteRule.TryNormalizeRecurrence"/>) cannot see the store's tzdata, which can be older than the host's.
    /// </summary>
    public async Task<string?> RecurrenceRefusalAsync(MuteRule rule, CancellationToken cancellationToken = default)
    {
        if (rule is null || !rule.IsRecurring) return null;
        await using var scope = await OpenAsync(cancellationToken);
        using var command = new NpgsqlCommand { Connection = scope.Connection, Transaction = scope.Transaction, CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        return await MuteRuleRecurrenceSql.RefusalAsync(command, rule.RecurTimeZone, cancellationToken);
    }

    private async Task ThrowIfZoneUnknownAsync(MuteRule rule)
    {
        if (await RecurrenceRefusalAsync(rule) is { } refusal)
        {
            throw new ArgumentException(refusal);
        }
    }

    public async Task InsertAsync(MuteRule rule)
    {
        if (rule is null)
        {
            throw new ArgumentNullException(nameof(rule));
        }

        await ThrowIfZoneUnknownAsync(rule);
        await using var scope = await OpenAsync();
        using var command = new NpgsqlCommand(@"
INSERT INTO config_mute_rules
    (id, enabled, created_at_utc, expires_at_utc, reason,
     server_name, metric_name, database_pattern,
     query_text_pattern, wait_type_pattern, job_name_pattern, server_id, client_app_pattern, external_source, starts_at_utc,
     recur_days, recur_start_time, recur_end_time, recur_time_zone)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)", scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(rule.Id);
        command.Parameters.AddWithValue(rule.Enabled);
        command.Parameters.AddWithValue(Naive(rule.CreatedAtUtc));
        AddNullable(command, rule.ExpiresAtUtc);
        AddNullableText(command, rule.Reason);
        AddNullableText(command, rule.ServerName);
        AddNullableText(command, rule.MetricName);
        AddNullableText(command, rule.DatabasePattern);
        AddNullableText(command, rule.QueryTextPattern);
        AddNullableText(command, rule.WaitTypePattern);
        AddNullableText(command, rule.JobNamePattern);
        AddNullableInt(command, rule.ServerId);
        AddNullableText(command, rule.ClientAppPattern);
        AddNullableText(command, rule.ExternalSource);
        AddNullable(command, rule.StartsAtUtc);
        AddRecurrence(command, rule);
        await command.ExecuteNonQueryAsync();
    }

    public async Task UpdateAsync(MuteRule rule)
    {
        if (rule is null)
        {
            throw new ArgumentNullException(nameof(rule));
        }

        await ThrowIfZoneUnknownAsync(rule);
        await using var scope = await OpenAsync();
        using var command = new NpgsqlCommand(@"
UPDATE config_mute_rules SET
    enabled = $2, expires_at_utc = $3, reason = $4,
    server_name = $5, metric_name = $6, database_pattern = $7,
    query_text_pattern = $8, wait_type_pattern = $9, job_name_pattern = $10, server_id = $11, client_app_pattern = $12,
    external_source = $13, starts_at_utc = $14,
    recur_days = $15, recur_start_time = $16, recur_end_time = $17, recur_time_zone = $18
WHERE id = $1", scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(rule.Id);
        command.Parameters.AddWithValue(rule.Enabled);
        AddNullable(command, rule.ExpiresAtUtc);
        AddNullableText(command, rule.Reason);
        AddNullableText(command, rule.ServerName);
        AddNullableText(command, rule.MetricName);
        AddNullableText(command, rule.DatabasePattern);
        AddNullableText(command, rule.QueryTextPattern);
        AddNullableText(command, rule.WaitTypePattern);
        AddNullableText(command, rule.JobNamePattern);
        AddNullableInt(command, rule.ServerId);
        AddNullableText(command, rule.ClientAppPattern);
        AddNullableText(command, rule.ExternalSource);
        AddNullable(command, rule.StartsAtUtc);
        AddRecurrence(command, rule);
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// The Level 2 recheck (#5915): locks the stored rule <c>FOR UPDATE</c> on this store's transaction and returns the server
    /// and metric it is scoped to now, or <c>null</c> when the id is gone. Only the gate-bound store (connection and
    /// transaction given) holds the lock to the write; on a pooled store it would release at once.
    /// </summary>
    internal async Task<(int? ServerId, string? MetricName)?> LockServerAndMetricAsync(string ruleId, CancellationToken cancellationToken = default)
    {
        await using var scope = await OpenAsync(cancellationToken);
        using var command = new NpgsqlCommand("SELECT server_id, metric_name FROM config_mute_rules WHERE id = $1 FOR UPDATE", scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text, Value = ruleId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
            return null;
        return (reader.IsDBNull(0) ? null : reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1));
    }

    public async Task SetEnabledAsync(string ruleId, bool enabled)
    {
        await using var scope = await OpenAsync();
        using var command = new NpgsqlCommand("UPDATE config_mute_rules SET enabled = $2 WHERE id = $1", scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(ruleId);
        command.Parameters.AddWithValue(enabled);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteAsync(string ruleId)
    {
        await using var scope = await OpenAsync();
        using var command = new NpgsqlCommand(MuteRulePurge.DeleteByIdSql, scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(ruleId);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds)
    {
        if (expiredIds is null || expiredIds.Count == 0)
        {
            return;
        }

        await using var scope = await OpenAsync();
        foreach (var id in expiredIds)
        {
            using var command = new NpgsqlCommand(MuteRulePurge.DeleteByIdSql, scope.Connection, scope.Transaction) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
            command.Parameters.AddWithValue(id);
            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>Npgsql rejects Kind=Utc against `timestamp`; the values are UTC by contract.</summary>
    private static DateTime Naive(DateTime value) =>
        DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static void AddNullable(NpgsqlCommand command, DateTime? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = value.HasValue ? Naive(value.Value) : DBNull.Value
        });

    /// <summary>The four recurrence columns (#7040), in the order both the INSERT and the UPDATE number them.</summary>
    private static void AddRecurrence(NpgsqlCommand command, MuteRule rule)
    {
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Smallint,
            Value = rule.RecurDays.HasValue ? (short)rule.RecurDays.Value : DBNull.Value
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Time,
            Value = rule.RecurStartTime.HasValue ? rule.RecurStartTime.Value : DBNull.Value
        });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Time,
            Value = rule.RecurEndTime.HasValue ? rule.RecurEndTime.Value : DBNull.Value
        });
        AddNullableText(command, rule.RecurTimeZone);
    }

    private static void AddNullableInt(NpgsqlCommand command, int? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Integer,
            Value = value.HasValue ? value.Value : DBNull.Value
        });

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = (object?)value ?? DBNull.Value
        });
}
