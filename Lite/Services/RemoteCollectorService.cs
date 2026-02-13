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
    /// Connection timeout for SQL Server connections in seconds.
    /// </summary>
    private const int ConnectionTimeoutSeconds = 15;

    /// <summary>
    /// Per-call timing fields set by each collector method.
    /// Read by RunCollectorAsync after the collector completes.
    /// </summary>
    private long _lastSqlMs;
    private long _lastDuckDbMs;

    /// <summary>
    /// Tracks health state per collector (keyed by collector name).
    /// </summary>
    private readonly Dictionary<string, CollectorHealthEntry> _collectorHealth = new(StringComparer.OrdinalIgnoreCase);
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
    }

    /// <summary>
    /// Seeds the delta calculator cache from DuckDB to survive application restarts.
    /// Should be called once during application startup.
    /// </summary>
    public Task SeedDeltaCacheAsync() => _deltaCalculator.SeedFromDatabaseAsync(_duckDb);

    /// <summary>
    /// Gets a summary of collector health across all tracked collectors.
    /// </summary>
    public CollectorHealthSummary GetHealthSummary()
    {
        lock (_healthLock)
        {
            var summary = new CollectorHealthSummary
            {
                TotalCollectors = _collectorHealth.Count,
                LoggingFailures = _logInsertFailures
            };

            foreach (var entry in _collectorHealth.Values)
            {
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
    private void RecordCollectorResult(string collectorName, bool success, string? errorMessage = null)
    {
        lock (_healthLock)
        {
            if (!_collectorHealth.TryGetValue(collectorName, out var entry))
            {
                entry = new CollectorHealthEntry { CollectorName = collectorName };
                _collectorHealth[collectorName] = entry;
            }

            if (success)
            {
                entry.LastSuccessTime = DateTime.UtcNow;
                entry.ConsecutiveErrors = 0;
                entry.TotalSuccesses++;
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

        _logger?.LogInformation("Running {CollectorCount} collectors for {ServerCount} servers",
            dueCollectors.Count, enabledServers.Count);

        var tasks = new List<Task>();

        foreach (var server in enabledServers)
        {
            foreach (var collector in dueCollectors)
            {
                tasks.Add(RunCollectorAsync(server, collector.Name, cancellationToken));
            }
        }

        await Task.WhenAll(tasks);
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
            if (server.AuthenticationType == "EntraMFA" && serverStatus.UserCancelledMfa)
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
        RecordCollectorResult(collectorName, status == "SUCCESS", errorMessage);

        // Log the collection attempt
        await LogCollectionAsync(GetServerId(server), collectorName, startTime, status, errorMessage, rowsCollected, _lastSqlMs, _lastDuckDbMs);
    }

    /// <summary>
    /// Logs a collection attempt to the collection_log table.
    /// </summary>
    private async Task LogCollectionAsync(int serverId, string collectorName, DateTime startTime, string status, string? errorMessage, int rowsCollected, long sqlMs = 0, long duckDbMs = 0)
    {
        try
        {
            var durationMs = (int)(DateTime.UtcNow - startTime).TotalMilliseconds;

            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync();

            using var command = connection.CreateCommand();
            command.CommandText = @"
                INSERT INTO collection_log (log_id, server_id, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";

            command.Parameters.Add(new DuckDBParameter { Value = GenerateCollectionId() });
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
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
        bool isMfaServer = server.AuthenticationType == "EntraMFA";
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
                        if (IsMfaCancelledException(ex))
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
    /// Checks if an exception indicates that the user cancelled MFA authentication.
    /// </summary>
    private static bool IsMfaCancelledException(Exception ex)
    {
        var message = ex.Message?.ToLowerInvariant() ?? string.Empty;
        
        // Common patterns when user cancels Azure AD authentication
        return message.Contains("user canceled") ||
               message.Contains("user cancelled") ||
               message.Contains("authentication was cancelled") ||
               message.Contains("authentication was canceled") ||
               message.Contains("user intervention is required") ||
               message.Contains("aadsts50058") || // Need to select account
               message.Contains("aadsts50126"); // Invalid credentials or cancelled
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
