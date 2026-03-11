using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services
{
    /// <summary>
    /// Manages alert mute rules with DuckDB persistence.
    /// Rules are cached in memory for fast matching and synced to DuckDB on changes.
    /// Thread-safe: all operations are protected by _lock.
    /// Database operations use the LockedConnection pattern to coordinate with CHECKPOINT.
    /// </summary>
    public class MuteRuleService
    {
        private readonly DuckDbInitializer _dbInitializer;
        private readonly object _lock = new object();
        private List<MuteRule> _rules = new();

        public MuteRuleService(DuckDbInitializer dbInitializer)
        {
            _dbInitializer = dbInitializer;
        }

        public bool IsAlertMuted(AlertMuteContext context)
        {
            lock (_lock)
            {
                return _rules.Any(r => r.Matches(context));
            }
        }

        public List<MuteRule> GetRules()
        {
            lock (_lock)
            {
                return _rules.ToList();
            }
        }

        public List<MuteRule> GetActiveRules()
        {
            lock (_lock)
            {
                return _rules.Where(r => r.Enabled && !r.IsExpired).ToList();
            }
        }

        public async Task LoadAsync()
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

            lock (_lock)
            {
                _rules = rules;
            }

            /* Purge expired rules on startup */
            await PurgeExpiredRulesAsync();
        }

        public async Task AddRuleAsync(MuteRule rule)
        {
            try
            {
                await PersistRuleAsync(rule);
            }
            catch (Exception ex)
            {
                AppLogger.Warn("MuteRuleService", $"Failed to persist new mute rule to DuckDB — rule will not be saved: {ex.Message}");
                return;
            }

            lock (_lock)
            {
                _rules.Add(rule);
            }
        }

        public async Task RemoveRuleAsync(string ruleId)
        {
            try
            {
                using var writeLock = _dbInitializer.AcquireWriteLock();
                using var connection = _dbInitializer.CreateConnection();
                await connection.OpenAsync();
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "DELETE FROM config_mute_rules WHERE id = $1";
                cmd.Parameters.Add(new DuckDBParameter { Value = ruleId });
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                AppLogger.Warn("MuteRuleService", $"Failed to delete mute rule from DuckDB: {ex.Message}");
            }

            lock (_lock)
            {
                _rules.RemoveAll(r => r.Id == ruleId);
            }
        }

        public async Task UpdateRuleAsync(MuteRule updated)
        {
            try
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
                cmd.Parameters.Add(new DuckDBParameter { Value = updated.Id });
                cmd.Parameters.Add(new DuckDBParameter { Value = updated.Enabled });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.ExpiresAtUtc ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.Reason ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.ServerName ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.MetricName ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.DatabasePattern ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.QueryTextPattern ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.WaitTypePattern ?? DBNull.Value });
                cmd.Parameters.Add(new DuckDBParameter { Value = (object?)updated.JobNamePattern ?? DBNull.Value });
                await cmd.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                AppLogger.Warn("MuteRuleService", $"Failed to update mute rule in DuckDB: {ex.Message}");
            }

            lock (_lock)
            {
                var index = _rules.FindIndex(r => r.Id == updated.Id);
                if (index >= 0)
                    _rules[index] = updated;
            }
        }

        public async Task SetRuleEnabledAsync(string ruleId, bool enabled)
        {
            try
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
            catch (Exception ex)
            {
                AppLogger.Warn("MuteRuleService", $"Failed to update mute rule enabled state in DuckDB: {ex.Message}");
            }

            lock (_lock)
            {
                var rule = _rules.FirstOrDefault(r => r.Id == ruleId);
                if (rule != null) rule.Enabled = enabled;
            }
        }

        public async Task<int> PurgeExpiredRulesAsync()
        {
            List<string> expiredIds;
            lock (_lock)
            {
                expiredIds = _rules.Where(r => r.IsExpired).Select(r => r.Id).ToList();
                if (expiredIds.Count == 0) return 0;
            }

            try
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
            catch (Exception ex)
            {
                AppLogger.Warn("MuteRuleService", $"Failed to purge expired mute rules from DuckDB: {ex.Message}");
                return 0;
            }

            lock (_lock)
            {
                _rules.RemoveAll(r => expiredIds.Contains(r.Id));
            }

            return expiredIds.Count;
        }

        private async Task PersistRuleAsync(MuteRule rule)
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
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.ExpiresAtUtc ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.Reason ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.ServerName ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.MetricName ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.DatabasePattern ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.QueryTextPattern ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.WaitTypePattern ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rule.JobNamePattern ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
