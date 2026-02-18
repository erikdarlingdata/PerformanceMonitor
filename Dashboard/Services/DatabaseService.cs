/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public partial class DatabaseService
    {
        private readonly string _connectionString;

        // Limit concurrent database queries to prevent thundering herd on initial load
        // 42 queries fire on server tab open; throttle to 7 concurrent (6 batches)
        // Per-instance so each server's queries don't starve other servers
        private readonly SemaphoreSlim _querySemaphore = new(7);

        public DatabaseService(string connectionString)
        {
            _connectionString = connectionString;
        }

        /// <summary>
        /// Opens a throttled database connection. The semaphore is released when the connection is disposed.
        /// </summary>
        private async Task<ThrottledConnection> OpenThrottledConnectionAsync()
        {
            await _querySemaphore.WaitAsync();
            try
            {
                var connection = new SqlConnection(_connectionString);
                await connection.OpenAsync();
                return new ThrottledConnection(connection, _querySemaphore);
            }
            catch
            {
                _querySemaphore.Release();
                throw;
            }
        }

        /// <summary>
        /// Wrapper that releases the semaphore when the connection is disposed.
        /// </summary>
        private readonly struct ThrottledConnection : IAsyncDisposable
        {
            private readonly SqlConnection _connection;
            private readonly SemaphoreSlim _semaphore;

            public ThrottledConnection(SqlConnection connection, SemaphoreSlim semaphore)
            {
                _connection = connection;
                _semaphore = semaphore;
            }

            public SqlConnection Connection => _connection;

            public async ValueTask DisposeAsync()
            {
                await _connection.DisposeAsync();
                _semaphore.Release();
            }
        }

        public static SqlConnectionStringBuilder BuildConnectionString(
            string serverName,
            bool useWindowsAuth,
            string? username = null,
            string? password = null,
            string encryptMode = "Mandatory",
            bool trustServerCertificate = false)
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = serverName,
                InitialCatalog = "PerformanceMonitor",
                TrustServerCertificate = trustServerCertificate,
                IntegratedSecurity = useWindowsAuth,
                MultipleActiveResultSets = true
            };

            // Set encryption mode
            builder.Encrypt = encryptMode switch
            {
                "Optional" => SqlConnectionEncryptOption.Optional,
                "Strict" => SqlConnectionEncryptOption.Strict,
                _ => SqlConnectionEncryptOption.Mandatory
            };

            if (!useWindowsAuth)
            {
                builder.UserID = username;
                builder.Password = password;
            }

            return builder;
        }

        public async Task<bool> TestConnectionAsync()
        {
            try
            {
                await using var tc = await OpenThrottledConnectionAsync();
                var connection = tc.Connection;
                return true;
            }
            catch (Exception ex)
            {
                Logger.Warning($"Connection test failed: {ex.Message}");
                return false;
            }
        }

        public async Task<List<CollectionHealthItem>> GetCollectionHealthAsync()
        {
            var items = new List<CollectionHealthItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
                SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    collector_name,
                    last_success_time,
                    hours_since_success,
                    health_status,
                    failure_rate_percent,
                    total_runs_7d,
                    failed_runs_7d,
                    avg_duration_ms,
                    total_rows_collected_7d
                FROM report.collection_health
                ORDER BY
                    CASE health_status
                        WHEN 'FAILING' THEN 1
                        WHEN 'STALE' THEN 2
                        WHEN 'WARNING' THEN 3
                        WHEN 'HEALTHY' THEN 4
                        WHEN 'NEVER_RUN' THEN 5
                    END,
                    collector_name;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using (StartQueryTiming("Collection Health", query, connection))
            {
                using var reader = await command.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    items.Add(new CollectionHealthItem
                    {
                        CollectorName = reader.GetString(0),
                        LastSuccessTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                        HoursSinceSuccess = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture),
                        HealthStatus = reader.GetString(3),
                        FailureRatePercent = reader.IsDBNull(4) ? 0m : Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture),
                        TotalRuns7d = reader.IsDBNull(5) ? 0L : Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture),
                        FailedRuns7d = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture),
                        AvgDurationMs = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture),
                        TotalRowsCollected7d = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8), CultureInfo.InvariantCulture)
                    });
                }
            }

            return items;
        }

        // ============================================
        // Helper Methods for Safe Type Conversion
        // ============================================

        private static short? SafeToInt16(object value, string columnName)
        {
            if (value == null || value == DBNull.Value)
                return null;
            var str = value.ToString();
            if (string.IsNullOrWhiteSpace(str))
                return null;
            if (short.TryParse(str.Replace(",", "", StringComparison.Ordinal), out var result))
                return result;
            throw new FormatException($"Column '{columnName}': Cannot convert '{str}' to Int16");
        }

        private static decimal? SafeToDecimal(object value, string columnName)
        {
            if (value == null || value == DBNull.Value)
                return null;
            var str = value.ToString();
            if (string.IsNullOrWhiteSpace(str))
                return null;
            if (decimal.TryParse(str.Replace(",", "", StringComparison.Ordinal), out var result))
                return result;
            throw new FormatException($"Column '{columnName}': Cannot convert '{str}' to Decimal");
        }

        private static long? SafeToInt64(object value, string columnName)
        {
            if (value == null || value == DBNull.Value)
                return null;
            var str = value.ToString();
            if (string.IsNullOrWhiteSpace(str))
                return null;
            str = str.Replace(",", "", StringComparison.Ordinal).Replace(" ", "", StringComparison.Ordinal);
            if (long.TryParse(str, out var result))
                return result;
            if (decimal.TryParse(str, out var decResult))
                return (long)decResult;
            throw new FormatException($"Column '{columnName}': Cannot convert '{str}' to Int64");
        }

        // ============================================
        // Query Timing Helper for Slow Query Logging
        // ============================================

        /// <summary>
        /// Creates a timing context for query execution. Logs slow queries when disposed.
        /// Usage: using var timing = StartQueryTiming("MethodName", query, connection);
        /// </summary>
        private QueryExecutionContext StartQueryTiming(string context, string query, SqlConnection connection)
        {
            var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
            return QueryLogger.StartQuery(
                context,
                query,
                builder.DataSource,
                builder.InitialCatalog);
        }

        public async Task<List<CollectorScheduleItem>> GetCollectorSchedulesAsync()
        {
            var items = new List<CollectorScheduleItem>();

            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
                SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

                SELECT
                    schedule_id,
                    collector_name,
                    enabled,
                    frequency_minutes,
                    last_run_time,
                    next_run_time,
                    retention_days,
                    description
                FROM config.collection_schedule
                ORDER BY
                    collector_name;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                items.Add(new CollectorScheduleItem
                {
                    ScheduleId = reader.GetInt32(0),
                    CollectorName = reader.GetString(1),
                    Enabled = reader.GetBoolean(2),
                    FrequencyMinutes = reader.GetInt32(3),
                    LastRunTime = reader.IsDBNull(4) ? null : reader.GetDateTime(4),
                    NextRunTime = reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    RetentionDays = reader.GetInt32(6),
                    Description = reader.IsDBNull(7) ? null : reader.GetString(7)
                });
            }

            return items;
        }

        public async Task UpdateCollectorScheduleAsync(int scheduleId, bool enabled, int frequencyMinutes, int retentionDays)
        {
            await using var tc = await OpenThrottledConnectionAsync();
            var connection = tc.Connection;

            string query = @"
                UPDATE
                    config.collection_schedule
                SET
                    enabled = @enabled,
                    frequency_minutes = @frequency_minutes,
                    retention_days = @retention_days,
                    modified_date = SYSDATETIME()
                WHERE schedule_id = @schedule_id;";

            using var command = new SqlCommand(query, connection);
            command.CommandTimeout = 120;
            command.Parameters.Add(new SqlParameter("@schedule_id", SqlDbType.Int) { Value = scheduleId });
            command.Parameters.Add(new SqlParameter("@enabled", SqlDbType.Bit) { Value = enabled });
            command.Parameters.Add(new SqlParameter("@frequency_minutes", SqlDbType.Int) { Value = frequencyMinutes });
            command.Parameters.Add(new SqlParameter("@retention_days", SqlDbType.Int) { Value = retentionDays });

            await command.ExecuteNonQueryAsync();
        }
    }
}
