/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Postgres-backed <see cref="IMuteRuleStore"/> over the V3 <c>config_mute_rules</c> table —
/// Lite's <c>DuckDbMuteRuleStore</c> ported statement-for-statement. The shared
/// <see cref="MuteRuleService"/> keeps the in-memory cache + matching; per its contract the
/// mutating methods here THROW on failure (the service catches, logs, and skips the cache
/// mutation — persist-then-cache ordering) while <see cref="LoadAllAsync"/> swallows errors and
/// returns an empty set (a headless service with no rules configured — or a store not yet
/// migrated — simply mutes nothing). Timestamps are stored naive-UTC and tagged UTC on read.
/// </summary>
public sealed class PgMuteRuleStore : IMuteRuleStore
{
    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgMuteRuleStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    public async Task<IReadOnlyList<MuteRule>> LoadAllAsync()
    {
        var rules = new List<MuteRule>();
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync();
            using var command = new NpgsqlCommand(@"
SELECT id, enabled, created_at_utc, expires_at_utc, reason,
       server_name, metric_name, database_pattern,
       query_text_pattern, wait_type_pattern, job_name_pattern
FROM config_mute_rules
ORDER BY created_at_utc DESC", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
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
                    JobNamePattern = reader.IsDBNull(10) ? null : reader.GetString(10)
                });
            }
        }
        catch (Exception ex)
        {
            /* Non-fatal — start with empty rules if the store is not ready (Lite's behavior). */
            _logger?.LogWarning("Could not load mute rules — starting with none: {Message}", ex.Message);
            rules.Clear();
        }

        return rules;
    }

    public async Task InsertAsync(MuteRule rule)
    {
        if (rule is null)
        {
            throw new ArgumentNullException(nameof(rule));
        }

        await using var connection = await _postgres.OpenConnectionAsync();
        using var command = new NpgsqlCommand(@"
INSERT INTO config_mute_rules
    (id, enabled, created_at_utc, expires_at_utc, reason,
     server_name, metric_name, database_pattern,
     query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
        await command.ExecuteNonQueryAsync();
    }

    public async Task UpdateAsync(MuteRule rule)
    {
        if (rule is null)
        {
            throw new ArgumentNullException(nameof(rule));
        }

        await using var connection = await _postgres.OpenConnectionAsync();
        using var command = new NpgsqlCommand(@"
UPDATE config_mute_rules SET
    enabled = $2, expires_at_utc = $3, reason = $4,
    server_name = $5, metric_name = $6, database_pattern = $7,
    query_text_pattern = $8, wait_type_pattern = $9, job_name_pattern = $10
WHERE id = $1", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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
        await command.ExecuteNonQueryAsync();
    }

    public async Task SetEnabledAsync(string ruleId, bool enabled)
    {
        await using var connection = await _postgres.OpenConnectionAsync();
        using var command = new NpgsqlCommand("UPDATE config_mute_rules SET enabled = $2 WHERE id = $1", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(ruleId);
        command.Parameters.AddWithValue(enabled);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteAsync(string ruleId)
    {
        await using var connection = await _postgres.OpenConnectionAsync();
        using var command = new NpgsqlCommand("DELETE FROM config_mute_rules WHERE id = $1", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
        command.Parameters.AddWithValue(ruleId);
        await command.ExecuteNonQueryAsync();
    }

    public async Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds)
    {
        if (expiredIds is null || expiredIds.Count == 0)
        {
            return;
        }

        await using var connection = await _postgres.OpenConnectionAsync();
        foreach (var id in expiredIds)
        {
            using var command = new NpgsqlCommand("DELETE FROM config_mute_rules WHERE id = $1", connection) { CommandTimeout = DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds };
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

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Text,
            Value = (object?)value ?? DBNull.Value
        });
}
