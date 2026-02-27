/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;


namespace PerformanceMonitorLite.Services;

/// <summary>
/// Base service for collecting performance data from remote SQL Servers.
/// Partial class - individual collectors are in separate files.
/// </summary>
/// <summary>
/// Tracks the health state of an individual collector.
/// </summary>
public class CollectorHealthEntry
{
    public int ServerId { get; set; }
    public string CollectorName { get; set; } = "";
    public DateTime? LastSuccessTime { get; set; }
    public DateTime? LastErrorTime { get; set; }
    public string? LastErrorMessage { get; set; }
    public int ConsecutiveErrors { get; set; }
    public int TotalErrors { get; set; }
    public int TotalSuccesses { get; set; }
}

/// <summary>
/// Summary of collector health across all collectors.
/// </summary>
public class CollectorHealthSummary
{
    public int TotalCollectors { get; set; }
    public int ErroringCollectors { get; set; }
    public int LoggingFailures { get; set; }
    public List<CollectorHealthEntry> Errors { get; set; } = new();
}

public partial class RemoteCollectorService
{
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly ScheduleManager _scheduleManager;
    private readonly ILogger<RemoteCollectorService>? _logger;
    private readonly DeltaCalculator _deltaCalculator;
    private static long s_idCounter = DateTime.UtcNow.Ticks;

    /// <summary>
    /// Limits concurrent SQL connections to avoid overwhelming target servers.
    /// </summary>
    private static readonly SemaphoreSlim s_connectionThrottle = new(7, 7);

    /// <summary>
    /// Serializes MFA authentication attempts to prevent multiple popups.
    /// Only one MFA authentication can happen at a time.
    /// </summary>
    private static readonly SemaphoreSlim s_mfaAuthLock = new(1, 1);

    /// <summary>
    /// Command timeout for DMV queries in seconds.
    /// </summary>
    private const int CommandTimeoutSeconds = 30;

    /// <summary>
    /// Connection timeout for SQL Server connections in seconds. Read from App settings each call.
    /// </summary>
    private static int ConnectionTimeoutSeconds => App.ConnectionTimeoutSeconds;

    /// <summary>
    /// Per-call timing fields set by each collector method.
    /// Read by RunCollectorAsync after the collector completes.
    /// </summary>
    private long _lastSqlMs;
    private long _lastDuckDbMs;

    /// <summary>
    /// Tracks health state per collector per server.
    /// </summary>
    private readonly Dictionary<(int ServerId, string CollectorName), CollectorHealthEntry> _collectorHealth = new();
    private readonly object _healthLock = new();

    /// <summary>
    /// Tracks consecutive failures of the collection_log INSERT itself.
    /// </summary>
    private int _logInsertFailures;
    private string? _lastLogInsertError;

    public RemoteCollectorService(
        DuckDbInitializer duckDb,
        ServerManager serverManager,
        ScheduleManager scheduleManager,
        ILogger<RemoteCollectorService>? logger = null)
    {
        _duckDb = duckDb;
        _serverManager = serverManager;
        _scheduleManager = scheduleManager;
        _logger = logger;
        _deltaCalculator = new DeltaCalculator(logger);
        _ignoredWaitTypes = new Lazy<HashSet<string>>(LoadIgnoredWaitTypes);
    }

    /// <summary>
    /// Seeds the delta calculator cache from DuckDB to survive application restarts.
    /// Should be called once during application startup.
    /// </summary>
    public Task SeedDeltaCacheAsync() => _deltaCalculator.SeedFromDatabaseAsync(_duckDb);

    /// <summary>
    /// Runs a manual DuckDB WAL checkpoint during idle time between collection cycles.
    /// </summary>
    public Task CheckpointAsync() => _duckDb.CheckpointAsync();

    /// <summary>
    /// Gets a summary of collector health for a specific server connection.
    /// </summary>
    public CollectorHealthSummary GetHealthSummary(ServerConnection server)
        => GetHealthSummary(GetServerId(server));

    /// <summary>
    /// Gets a summary of collector health. When serverId is provided, filters to that server only.
    /// </summary>
    public CollectorHealthSummary GetHealthSummary(int? serverId = null)
    {
        lock (_healthLock)
        {
            var summary = new CollectorHealthSummary
            {
                LoggingFailures = _logInsertFailures
            };

            foreach (var entry in _collectorHealth.Values)
            {
                if (serverId.HasValue && entry.ServerId != serverId.Value)
                    continue;

                summary.TotalCollectors++;

                if (entry.ConsecutiveErrors > 0)
                {
                    summary.ErroringCollectors++;
                    summary.Errors.Add(entry);
                }
            }

            return summary;
        }
    }

    /// <summary>
    /// Records a collector execution result for health tracking.
    /// </summary>
    private void RecordCollectorResult(int serverId, string collectorName, string status, string? errorMessage = null)
    {
        lock (_healthLock)
        {
            var key = (serverId, collectorName);
            if (!_collectorHealth.TryGetValue(key, out var entry))
            {
                entry = new CollectorHealthEntry { ServerId = serverId, CollectorName = collectorName };
                _collectorHealth[key] = entry;
            }

            if (status == "SUCCESS")
            {
                entry.LastSuccessTime = DateTime.UtcNow;
                entry.ConsecutiveErrors = 0;
                entry.TotalSuccesses++;
            }
            else if (status == "PERMISSIONS")
            {
                /* Permission errors are not transient — don't count as failures
                   (which would show FAILING) but don't count as success either.
                   Record the error message so the user can see what's wrong. */
                entry.LastErrorTime = DateTime.UtcNow;
                entry.LastErrorMessage = errorMessage;
            }
            else
            {
                entry.LastErrorTime = DateTime.UtcNow;
                entry.LastErrorMessage = errorMessage;
                entry.ConsecutiveErrors++;
                entry.TotalErrors++;
            }
        }
    }

    /// <summary>
    /// Runs all due collectors for all enabled servers.
    /// </summary>
    public async Task RunDueCollectorsAsync(CancellationToken cancellationToken = default)
    {
        var dueCollectors = _scheduleManager.GetDueCollectors();
        var enabledServers = _serverManager.GetEnabledServers();

        if (dueCollectors.Count == 0 || enabledServers.Count == 0)
        {
            return;
        }

        int skippedOffline = 0;
        var onlineServers = new List<ServerConnection>();

        foreach (var server in enabledServers)
        {
            var serverStatus = _serverManager.GetConnectionStatus(server.Id);
            if (serverStatus.IsOnline == false)
            {
                skippedOffline++;
                _logger?.LogDebug("Skipping offline server '{Server}'", server.DisplayName);
                continue;
            }
            onlineServers.Add(server);
        }

        _logger?.LogInformation("Running {CollectorCount} collectors for {OnlineCount}/{TotalCount} servers ({SkippedCount} offline, skipped)",
            dueCollectors.Count, onlineServers.Count, enabledServers.Count, skippedOffline);

        /* Run servers in parallel, but collectors within each server sequentially.
           DuckDB is single-writer; running all collectors in parallel causes spin-wait
           contention (50%+ CPU, multi-second stalls). Sequential per-server eliminates
           this while still allowing multi-server parallelism. */
        var serverTasks = onlineServers.Select(server => Task.Run(async () =>
        {
            foreach (var collector in dueCollectors)
            {
                await RunCollectorAsync(server, collector.Name, cancellationToken);
            }
        }, cancellationToken));

        await Task.WhenAll(serverTasks);

        /* Run CHECKPOINT here after all collector connections are closed.
           Write lock ensures no UI readers have stale file offsets when
           CHECKPOINT reorganizes/truncates the database file. */
        try
        {
            using var writeLock = _duckDb.AcquireWriteLock();
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "CHECKPOINT";
            await cmd.ExecuteNonQueryAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Post-collection checkpoint failed (non-critical)");
        }
    }

    /// <summary>
    /// Runs all enabled collectors for a single server immediately (ignoring schedule).
    /// Used for initial data population when a server tab is first opened.
    /// </summary>
    public async Task RunAllCollectorsForServerAsync(ServerConnection server, CancellationToken cancellationToken = default)
    {
        var enabledSchedules = _scheduleManager.GetEnabledSchedules()
            .Concat(_scheduleManager.GetOnLoadCollectors())
            .ToList();

        /* Ensure XE sessions are set up before collecting */
        var serverStatus = _serverManager.GetConnectionStatus(server.Id);
        var engineEdition = serverStatus.SqlEngineEdition;
        await EnsureBlockedProcessXeSessionAsync(server, engineEdition, cancellationToken);
        await EnsureDeadlockXeSessionAsync(server, engineEdition, cancellationToken);

        AppLogger.Info("Collector", $"Running {enabledSchedules.Count} collectors for '{server.DisplayName}' (serverId={GetServerId(server)})");
        _logger?.LogInformation("Running {Count} collectors for server '{Server}' (initial load)",
            enabledSchedules.Count, server.DisplayName);

        foreach (var schedule in enabledSchedules)
        {
            try
            {
                await RunCollectorAsync(server, schedule.Name, cancellationToken);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Initial collector '{Collector}' failed for server '{Server}'",
                    schedule.Name, server.DisplayName);
            }
        }
    }

    /// <summary>
    /// Runs a specific collector for a specific server.
    /// </summary>
    public async Task RunCollectorAsync(ServerConnection server, string collectorName, CancellationToken cancellationToken = default)
    {
        var startTime = DateTime.UtcNow;
        var status = "SUCCESS";
        string? errorMessage = null;
        int rowsCollected = 0;

        try
        {
            // Version-gate and edition-gate collectors
            var serverStatus = _serverManager.GetConnectionStatus(server.Id);
            var majorVersion = serverStatus.SqlMajorVersion;
            var engineEdition = serverStatus.SqlEngineEdition;
            var isAwsRds = serverStatus.IsAwsRds;

            if (!IsCollectorSupported(collectorName, majorVersion, engineEdition, isAwsRds))
            {
                AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} SKIPPED (version {majorVersion}, edition {engineEdition})");
                return;
            }

            // Skip MFA servers if user has cancelled authentication
            // This prevents repeated popup dialogs during background data collection
            if (server.AuthenticationType == AuthenticationTypes.EntraMFA && serverStatus.UserCancelledMfa)
            {
                AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} SKIPPED - MFA authentication cancelled by user");
                _logger?.LogDebug("Skipping collector '{Collector}' for server '{Server}' - user cancelled MFA",
                    collectorName, server.DisplayName);
                return;
            }

            _logger?.LogDebug("Running collector '{Collector}' for server '{Server}'",
                collectorName, server.DisplayName);

            rowsCollected = collectorName switch
            {
                "wait_stats" => await CollectWaitStatsAsync(server, cancellationToken),
                "cpu_utilization" => await CollectCpuUtilizationAsync(server, cancellationToken),
                "memory_stats" => await CollectMemoryStatsAsync(server, cancellationToken),
                "memory_clerks" => await CollectMemoryClerksAsync(server, cancellationToken),
                "file_io_stats" => await CollectFileIoStatsAsync(server, cancellationToken),
                "query_stats" => await CollectQueryStatsAsync(server, cancellationToken),
                "procedure_stats" => await CollectProcedureStatsAsync(server, cancellationToken),
                "query_snapshots" => await CollectQuerySnapshotsAsync(server, cancellationToken),
                "tempdb_stats" => await CollectTempDbStatsAsync(server, cancellationToken),
                "perfmon_stats" => await CollectPerfmonStatsAsync(server, cancellationToken),
                "deadlocks" => await CollectDeadlocksAsync(server, cancellationToken),
                "server_config" => await CollectServerConfigAsync(server, cancellationToken),
                "database_config" => await CollectDatabaseConfigAsync(server, cancellationToken),
                "query_store" => await CollectQueryStoreAsync(server, cancellationToken),
                "memory_grant_stats" => await CollectMemoryGrantStatsAsync(server, cancellationToken),
                "waiting_tasks" => await CollectWaitingTasksAsync(server, cancellationToken),
                "blocked_process_report" => await CollectBlockedProcessReportsAsync(server, cancellationToken),
                "database_scoped_config" => await CollectDatabaseScopedConfigAsync(server, cancellationToken),
                "trace_flags" => await CollectTraceFlagsAsync(server, cancellationToken),
                "running_jobs" => await CollectRunningJobsAsync(server, cancellationToken),
                _ => throw new ArgumentException($"Unknown collector: {collectorName}")
            };

            _scheduleManager.MarkCollectorRun(collectorName, startTime);

            var elapsed = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;
            AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} => {rowsCollected} rows in {elapsed}ms (sql:{_lastSqlMs}ms, duck:{_lastDuckDbMs}ms)");
        }
        catch (SqlException ex)
        {
            status = "ERROR";
            errorMessage = $"SQL Error #{ex.Number}: {ex.Message}";
            AppLogger.Error("Collector", $"  [{server.DisplayName}] {collectorName} SQL Error #{ex.Number}: {ex.Message}");

            if (RetryHelper.IsTransient(ex))
            {
                _logger?.LogWarning("Collector '{Collector}' transient SQL error #{ErrorNumber} for server '{Server}': {Message}",
                    collectorName, ex.Number, server.DisplayName, ex.Message);
            }
            else if (ex.Number == 207) /* Invalid column name - likely version incompatibility */
            {
                _logger?.LogWarning("Collector '{Collector}' column not found for server '{Server}' (possible version incompatibility): {Message}",
                    collectorName, server.DisplayName, ex.Message);
            }
            else if (ex.Number == 229 || ex.Number == 297 || ex.Number == 300)
            {
                status = "PERMISSIONS";
                _logger?.LogWarning("Collector '{Collector}' permission denied for server '{Server}': {Message}",
                    collectorName, server.DisplayName, ex.Message);
            }
            else
            {
                _logger?.LogError(ex, "Collector '{Collector}' SQL error #{ErrorNumber} for server '{Server}'",
                    collectorName, ex.Number, server.DisplayName);
            }
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("MFA authentication cancelled"))
        {
            // User cancelled MFA - don't log as error, this is expected
            status = "SKIPPED";
            errorMessage = "MFA authentication cancelled by user";
            AppLogger.Info("Collector", $"  [{server.DisplayName}] {collectorName} SKIPPED - {errorMessage}");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            status = "CANCELLED";
            errorMessage = "Collection cancelled";
            _logger?.LogDebug("Collector '{Collector}' cancelled for server '{Server}'", collectorName, server.DisplayName);
        }
        catch (Exception ex)
        {
            status = "ERROR";
            errorMessage = ex.Message;
            AppLogger.Error("Collector", $"  [{server.DisplayName}] {collectorName} {ex.GetType().Name}: {ex.Message}");
            _logger?.LogError(ex, "Collector '{Collector}' failed for server '{Server}'",
                collectorName, server.DisplayName);
        }

        // Track collector health
        RecordCollectorResult(GetServerId(server), collectorName, status, errorMessage);

        // Log the collection attempt
        await LogCollectionAsync(GetServerId(server), server.DisplayName, collectorName, startTime, status, errorMessage, rowsCollected, _lastSqlMs, _lastDuckDbMs);
    }

    /// <summary>
    /// Logs a collection attempt to the collection_log table.
    /// </summary>
    private async Task LogCollectionAsync(int serverId, string serverName, string collectorName, DateTime startTime, string status, string? errorMessage, int rowsCollected, long sqlMs = 0, long duckDbMs = 0)
    {
        try
        {
            var durationMs = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;

            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

            command.Parameters.Add(new DuckDBParameter { Value = GenerateCollectionId() });
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = serverName });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = durationMs });
            command.Parameters.Add(new DuckDBParameter { Value = status });
            command.Parameters.Add(new DuckDBParameter { Value = errorMessage ?? (object)DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = rowsCollected });
            command.Parameters.Add(new DuckDBParameter { Value = (int)sqlMs });
            command.Parameters.Add(new DuckDBParameter { Value = (int)duckDbMs });

            await command.ExecuteNonQueryAsync();

            /* Reset failure counter on success */
            if (_logInsertFailures > 0)
            {
                AppLogger.Info("Collector", $"Collection logging recovered after {_logInsertFailures} failure(s)");
                _logInsertFailures = 0;
                _lastLogInsertError = null;
            }
        }
        catch (Exception ex)
        {
            _logInsertFailures++;
            _lastLogInsertError = ex.Message;

            if (_logInsertFailures <= 3)
            {
                /* First few failures: log at Error level with full detail */
                AppLogger.Error("Collector", $"COLLECTION LOGGING FAILED ({_logInsertFailures}x): {ex.GetType().Name}: {ex.Message}");
                _logger?.LogError(ex, "Failed to log collection for {Collector} (failure #{Count})", collectorName, _logInsertFailures);
            }
            else if (_logInsertFailures % 100 == 0)
            {
                /* Periodic reminder for ongoing failures */
                AppLogger.Error("Collector", $"COLLECTION LOGGING STILL BROKEN: {_logInsertFailures} consecutive failures. Last error: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// Creates a SQL connection to a remote server.
    /// Throws InvalidOperationException if MFA authentication was cancelled by user.
    /// </summary>
    protected async Task<SqlConnection> CreateConnectionAsync(ServerConnection server, CancellationToken cancellationToken)
    {
        // For MFA servers, serialize authentication attempts to prevent multiple popups
        bool isMfaServer = server.AuthenticationType == AuthenticationTypes.EntraMFA;
        bool mfaLockAcquired = false;

        try
        {
            // Acquire MFA lock first (if applicable) to serialize authentication
            if (isMfaServer)
            {
                await s_mfaAuthLock.WaitAsync(cancellationToken);
                mfaLockAcquired = true;

                // Check if user already cancelled MFA for this server
                var serverStatus = _serverManager.GetConnectionStatus(server.Id);
                if (serverStatus.UserCancelledMfa)
                {
                    AppLogger.Info("Collector", $"  [{server.DisplayName}] MFA authentication already cancelled - aborting");
                    throw new InvalidOperationException("MFA authentication cancelled by user. Please connect to the server explicitly to retry.");
                }
            }

            // Now acquire connection throttle
            await s_connectionThrottle.WaitAsync(cancellationToken);
            try
            {
                var connectionString = server.GetConnectionString(_serverManager.CredentialService);

            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = ConnectionTimeoutSeconds
            };

            var connStr = builder.ConnectionString;

                return await RetryHelper.ExecuteWithRetryAsync(async () =>
                {
                    var connection = new SqlConnection(connStr);
                    
                    try
                    {
                        await connection.OpenAsync(cancellationToken);
                        return connection;
                    }
                    catch (Exception ex) when (isMfaServer)
                    {
                        // Detect MFA cancellation and mark immediately so other waiting connections abort
                        if (MfaAuthenticationHelper.IsMfaCancelledException(ex))
                        {
                            var serverStatus = _serverManager.GetConnectionStatus(server.Id);
                            serverStatus.UserCancelledMfa = true;
                            AppLogger.Info("Collector", $"  [{server.DisplayName}] MFA authentication cancelled by user");
                            _logger?.LogInformation("MFA authentication cancelled by user for server '{DisplayName}' - flagging to abort other pending connections", server.DisplayName);
                        }
                        throw;
                    }
                }, _logger, $"Connect to {server.DisplayName}", cancellationToken: cancellationToken);
            }
            finally
            {
                s_connectionThrottle.Release();
            }
        }
        finally
        {
            // Release MFA lock if we acquired it
            if (mfaLockAcquired)
            {
                s_mfaAuthLock.Release();
            }
        }
    }

    /// <summary>
    /// Generates a unique collection ID based on timestamp.
    /// </summary>
    protected static long GenerateCollectionId()
    {
        return Interlocked.Increment(ref s_idCounter);
    }

    /// <summary>
    /// Gets the numeric server ID from the server connection.
    /// </summary>
    protected static int GetServerId(ServerConnection server)
    {
        return GetDeterministicHashCode(server.ServerName);
    }

    /// <summary>
    /// Gets the most recent value of a timestamp column from DuckDB for incremental collection.
    /// Returns null on first run or if the query fails (caller uses a fallback window).
    /// </summary>
    protected async Task<DateTime?> GetLastCollectedTimeAsync(
        int serverId, string tableName, string columnName, CancellationToken cancellationToken)
    {
        try
        {
            using var conn = _duckDb.CreateConnection();
            await conn.OpenAsync(cancellationToken);
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT MAX({columnName}) FROM {tableName} WHERE server_id = $1";
            cmd.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            var result = await cmd.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime dt)
                return dt;
        }
        catch
        {
            /* If DuckDB query fails, caller uses fallback window */
        }
        return null;
    }

    /// <summary>
    /// Safely converts a SQL Server float/real value to decimal.
    /// Returns 0 for Infinity, NaN, or values outside decimal range.
    /// </summary>
    protected static decimal SafeToDecimal(object value)
    {
        try
        {
            if (value is double d)
            {
                if (double.IsInfinity(d) || double.IsNaN(d))
                    return 0m;
            }
            else if (value is float f)
            {
                if (float.IsInfinity(f) || float.IsNaN(f))
                    return 0m;
            }
            return Convert.ToDecimal(value);
        }
        catch (OverflowException)
        {
            return 0m;
        }
    }

    /// <summary>
    /// Deterministic hash code for a string. .NET Core randomizes string.GetHashCode()
    /// per process, so we use a simple FNV-1a hash to get a stable value across restarts.
    /// </summary>
    internal static int GetDeterministicHashCode(string value)
    {
        unchecked
        {
            var hash = (int)2166136261;
            foreach (var c in value)
            {
                hash = (hash ^ c) * 16777619;
            }
            return hash;
        }
    }

    /// <summary>
    /// Checks if a collector is supported on the given SQL Server version and engine edition.
    /// Version 13 = SQL Server 2016, 14 = 2017, 15 = 2019, 16 = 2022, 17 = 2025.
    /// Engine edition 5 = Azure SQL DB, 8 = Azure MI.
    /// </summary>
    private static bool IsCollectorSupported(string collectorName, int majorVersion, int engineEdition, bool isAwsRds = false)
    {
        bool isAzureSqlDb = engineEdition == 5;
        bool isAzureMi = engineEdition == 8;

        /* Version gates — only for on-prem/RDS.
           Azure SQL DB reports ProductMajorVersion=12 and Azure MI may report similar values,
           but both fully support dm_exec_query_stats, Query Store, etc. */
        if (majorVersion > 0 && !isAzureSqlDb && !isAzureMi)
        {
            switch (collectorName)
            {
                case "query_store":
                case "query_stats":
                    if (majorVersion < 13) return false;
                    break;
            }
        }

        /* Azure SQL DB edition gates — skip collectors that use unsupported DMVs */
        if (isAzureSqlDb)
        {
            switch (collectorName)
            {
                case "server_config":     /* sys.configurations not available */
                case "trace_flags":       /* DBCC TRACESTATUS not available */
                case "running_jobs":      /* msdb.dbo.sysjobs not available */
                    return false;
            }
        }

        /* AWS RDS gates — limited msdb permissions (syssessions not accessible) */
        if (isAwsRds)
        {
            switch (collectorName)
            {
                case "running_jobs":      /* msdb.dbo.syssessions not accessible */
                    return false;
            }
        }

        return true;
    }
}
