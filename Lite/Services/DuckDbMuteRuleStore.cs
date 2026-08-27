/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// DuckDB-backed <see cref="IMuteRuleStore"/> over <c>config_mute_rules</c>.
/// Wraps the persistence that previously lived directly inside
/// <see cref="MuteRuleService"/> (LoadAsync / PersistRuleAsync / RemoveRuleAsync /
/// UpdateRuleAsync / SetRuleEnabledAsync / PurgeExpiredRulesAsync) — verbatim SQL.
/// The mutating methods throw on failure; <see cref="MuteRuleService"/> keeps the
/// try/catch + logging + in-memory cache (persist-then-cache ordering preserved).
/// <see cref="LoadAllAsync"/> swallows errors and returns an empty set, matching
/// the old LoadAsync ("start with empty rules if DB not ready").
///
/// <para><b>Four of the five write-lock sites are earned; <see cref="InsertAsync"/> is not (#2463).</b>
/// Worth saying because <c>FindingStore.MuteStoryAsync</c> writes a mute row under the READ lock, and the
/// pair reads as one convention contradicting another. It is not the same species of write.
/// <c>config_mute_rules</c> is UPDATEd by an operator (<see cref="UpdateAsync"/>,
/// <see cref="SetEnabledAsync"/>) and DELETEd by both the operator and a timer-driven expiry purge
/// (<see cref="DeleteAsync"/>, <see cref="DeleteExpiredAsync"/>) that can be running at the same instant;
/// DuckDB fails the loser of such a collision rather than queueing it, and the write lock is what stops
/// that happening. Nothing but the analysis pass writes <c>analysis_muted</c>, so its append needs only
/// the read lock. <see cref="InsertAsync"/> is an append too and by the rule needs only the read lock;
/// it is left on the write lock so this store speaks with one voice. The rule and the measurements are
/// on <c>DuckDbInitializer.s_dbLock</c>.</para>
/// </summary>
public sealed class DuckDbMuteRuleStore : IMuteRuleStore
{
    private readonly DuckDbInitializer _dbInitializer;

    public DuckDbMuteRuleStore(DuckDbInitializer dbInitializer)
    {
        _dbInitializer = dbInitializer;
    }

    public async Task<IReadOnlyList<MuteRule>> LoadAllAsync()
    {
        var rules = new List<MuteRule>();
        try
        {
            using var readLock = _dbInitializer.AcquireReadLock();
            using var connection = _dbInitializer.CreateConnection();
            await connection.OpenAsync();
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
                    SELECT id, enabled, created_at_utc, expires_at_utc, reason,
                           server_name, metric_name, database_pattern,
                           query_text_pattern, wait_type_pattern, job_name_pattern
                    FROM config_mute_rules
                    ORDER BY created_at_utc DESC";

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rules.Add(new MuteRule
                {
                    Id = reader.GetString(0),
                    Enabled = reader.GetBoolean(1),
                    CreatedAtUtc = reader.GetDateTime(2),
                    ExpiresAtUtc = reader.IsDBNull(3) ? null : reader.GetDateTime(3),
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
        catch
        {
            /* Non-fatal — start with empty rules if DB not ready */
        }

        return rules;
    }

    public async Task InsertAsync(MuteRule rule)
    {
        using var writeLock = _dbInitializer.AcquireWriteLock();
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
                INSERT INTO config_mute_rules
                    (id, enabled, created_at_utc, expires_at_utc, reason,
                     server_name, metric_name, database_pattern,
                     query_text_pattern, wait_type_pattern, job_name_pattern)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = rule.Id });
        cmd.Parameters.Add(new DuckDBParameter { Value = rule.Enabled });
        cmd.Parameters.Add(new DuckDBParameter { Value = rule.CreatedAtUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.ExpiresAtUtc ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.Reason ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.ServerName ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.MetricName ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.DatabasePattern ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.QueryTextPattern ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.WaitTypePattern ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.JobNamePattern ?? System.DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task UpdateAsync(MuteRule rule)
    {
        using var writeLock = _dbInitializer.AcquireWriteLock();
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
                    UPDATE config_mute_rules SET
                        enabled = $2, expires_at_utc = $3, reason = $4,
                        server_name = $5, metric_name = $6, database_pattern = $7,
                        query_text_pattern = $8, wait_type_pattern = $9, job_name_pattern = $10
                    WHERE id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = rule.Id });
        cmd.Parameters.Add(new DuckDBParameter { Value = rule.Enabled });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.ExpiresAtUtc ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.Reason ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.ServerName ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.MetricName ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.DatabasePattern ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.QueryTextPattern ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.WaitTypePattern ?? System.DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.JobNamePattern ?? System.DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task SetEnabledAsync(string ruleId, bool enabled)
    {
        using var writeLock = _dbInitializer.AcquireWriteLock();
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "UPDATE config_mute_rules SET enabled = $2 WHERE id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = ruleId });
        cmd.Parameters.Add(new DuckDBParameter { Value = enabled });
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DeleteAsync(string ruleId)
    {
        using var writeLock = _dbInitializer.AcquireWriteLock();
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "DELETE FROM config_mute_rules WHERE id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = ruleId });
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds)
    {
        using var writeLock = _dbInitializer.AcquireWriteLock();
        using var connection = _dbInitializer.CreateConnection();
        await connection.OpenAsync();
        foreach (var id in expiredIds)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "DELETE FROM config_mute_rules WHERE id = $1";
            cmd.Parameters.Add(new DuckDBParameter { Value = id });
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
