/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Data;
using System.Diagnostics;
using System.Text;
using Installer.Core.Models;
using Microsoft.Data.SqlClient;

namespace Installer.Core;

/// <summary>
/// Core installation service for the Performance Monitor database.
/// All methods are static — no instance state needed.
/// </summary>
public static class InstallationService
{
    private static readonly char[] NewLineChars = ['\r', '\n'];

    /// <summary>
    /// Logs a diagnostic message through the progress reporter.
    /// Uses "Debug" status so consumers can filter verbose output.
    /// </summary>
    private static void LogDebug(IProgress<InstallationProgress>? progress, string message)
    {
        progress?.Report(new InstallationProgress { Message = $"[DEBUG] {message}", Status = "Debug" });
    }

    /// <summary>
    /// Timeout for standard SQL file execution (5 minutes).
    /// </summary>
    public const int StandardTimeoutSeconds = 300;

    /// <summary>
    /// Timeout for upgrade migrations on large tables (1 hour).
    /// </summary>
    public const int UpgradeTimeoutSeconds = 3600;

    /// <summary>
    /// Timeout for short operations like cleanup (1 minute).
    /// </summary>
    public const int ShortTimeoutSeconds = 60;

    /// <summary>
    /// Timeout for dependency installation (2 minutes).
    /// </summary>
    public const int DependencyTimeoutSeconds = 120;

    /// <summary>
    /// Build a connection string from the provided parameters.
    /// </summary>
    public static string BuildConnectionString(
        string server,
        bool useWindowsAuth,
        string? username = null,
        string? password = null,
        string encryption = "Mandatory",
        bool trustCertificate = false,
        bool useEntraAuth = false)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = server,
            InitialCatalog = "master",
            TrustServerCertificate = trustCertificate
        };

        builder.Encrypt = encryption switch
        {
            "Optional" => SqlConnectionEncryptOption.Optional,
            "Mandatory" => SqlConnectionEncryptOption.Mandatory,
            "Strict" => SqlConnectionEncryptOption.Strict,
            _ => SqlConnectionEncryptOption.Mandatory
        };

        if (useEntraAuth)
        {
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive;
            builder.UserID = username;
        }
        else if (useWindowsAuth)
        {
            builder.IntegratedSecurity = true;
        }
        else
        {
            builder.UserID = username;
            builder.Password = password;
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Test connection to SQL Server and get server information.
    /// </summary>
    public static async Task<ServerInfo> TestConnectionAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        var info = new ServerInfo();
        LogDebug(progress, $"TestConnectionAsync: opening connection");
        var sw = Stopwatch.StartNew();

        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);
            LogDebug(progress, $"TestConnectionAsync: connected in {sw.ElapsedMilliseconds}ms");

            info.IsConnected = true;

            using var command = new SqlCommand(@"
                SELECT
                    @@VERSION,
                    SERVERPROPERTY('Edition'),
                    @@SERVERNAME,
                    CONVERT(int, SERVERPROPERTY('EngineEdition')),
                    SERVERPROPERTY('ProductMajorVersion');", connection);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                info.SqlServerVersion = reader.GetString(0);
                info.SqlServerEdition = reader.GetString(1);
                info.ServerName = reader.GetString(2);
                info.EngineEdition = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
                info.ProductMajorVersion = reader.IsDBNull(4) ? 0 : int.TryParse(reader.GetValue(4).ToString(), out var v) ? v : 0;
            }

            LogDebug(progress, $"TestConnectionAsync: server={info.ServerName}, edition={info.SqlServerEdition}, " +
                $"engineEdition={info.EngineEdition}, majorVersion={info.ProductMajorVersion}, " +
                $"supported={info.IsSupportedVersion}, elapsed={sw.ElapsedMilliseconds}ms");
        }
        catch (Exception ex)
        {
            info.IsConnected = false;
            info.ErrorMessage = ex.Message;
            if (ex.InnerException != null)
            {
                info.ErrorMessage += $"\n{ex.InnerException.Message}";
            }
            LogDebug(progress, $"TestConnectionAsync: FAILED after {sw.ElapsedMilliseconds}ms — " +
                $"{ex.GetType().Name}: {ex.Message}" +
                (ex.InnerException != null ? $" → {ex.InnerException.GetType().Name}: {ex.InnerException.Message}" : ""));
        }

        return info;
    }

    /// <summary>
    /// Perform clean install (drop existing database and jobs).
    /// </summary>
    public static async Task CleanInstallAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        LogDebug(progress, "CleanInstallAsync: starting — will drop database, jobs, XE sessions");
        var sw = Stopwatch.StartNew();
        progress?.Report(new InstallationProgress
        {
            Message = "Performing clean install...",
            Status = "Info"
        });

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        /*Stop any existing traces before dropping database*/
        try
        {
            using var traceCmd = new SqlCommand(
                "EXECUTE PerformanceMonitor.collect.trace_management_collector @action = 'STOP';",
                connection);
            traceCmd.CommandTimeout = ShortTimeoutSeconds;
            await traceCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            progress?.Report(new InstallationProgress
            {
                Message = "Stopped existing traces",
                Status = "Success"
            });
        }
        catch (SqlException)
        {
            /*Database or procedure doesn't exist - no traces to clean*/
        }

        /*Remove Agent jobs, XE sessions, and database*/
        string cleanupSql = @"
USE msdb;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Collection')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Collection', @delete_unused_schedule = 1;
END;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Data Retention')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Data Retention', @delete_unused_schedule = 1;
END;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Hung Job Monitor')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Hung Job Monitor', @delete_unused_schedule = 1;
END;

USE master;

IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'PerformanceMonitor_BlockedProcess')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE name = N'PerformanceMonitor_BlockedProcess')
        ALTER EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER STATE = STOP;
    DROP EVENT SESSION [PerformanceMonitor_BlockedProcess] ON SERVER;
END;

IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'PerformanceMonitor_Deadlock')
BEGIN
    IF EXISTS (SELECT 1 FROM sys.dm_xe_sessions WHERE name = N'PerformanceMonitor_Deadlock')
        ALTER EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER STATE = STOP;
    DROP EVENT SESSION [PerformanceMonitor_Deadlock] ON SERVER;
END;

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'PerformanceMonitor')
BEGIN
    ALTER DATABASE PerformanceMonitor SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE PerformanceMonitor;
END;";

        using var command = new SqlCommand(cleanupSql, connection);
        command.CommandTimeout = ShortTimeoutSeconds;
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        progress?.Report(new InstallationProgress
        {
            Message = "Clean install completed (jobs, XE sessions, and database removed)",
            Status = "Success"
        });
    }

    /// <summary>
    /// Perform complete uninstall (remove database, jobs, XE sessions, and traces).
    /// </summary>
    public static async Task<bool> ExecuteUninstallAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        progress?.Report(new InstallationProgress
        {
            Message = "Uninstalling Performance Monitor...",
            Status = "Info"
        });

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        /*Stop existing traces before dropping database*/
        try
        {
            using var traceCmd = new SqlCommand(
                "EXECUTE PerformanceMonitor.collect.trace_management_collector @action = 'STOP';",
                connection);
            traceCmd.CommandTimeout = ShortTimeoutSeconds;
            await traceCmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            progress?.Report(new InstallationProgress
            {
                Message = "Stopped server-side traces",
                Status = "Success"
            });
        }
        catch (SqlException)
        {
            progress?.Report(new InstallationProgress
            {
                Message = "No traces to stop (database or procedure not found)",
                Status = "Info"
            });
        }

        /*Remove Agent jobs, XE sessions, and database*/
        await CleanInstallAsync(connectionString, progress, cancellationToken)
            .ConfigureAwait(false);

        progress?.Report(new InstallationProgress
        {
            Message = "Uninstall completed successfully",
            Status = "Success",
            ProgressPercent = 100
        });

        return true;
    }

    /// <summary>
    /// Execute SQL installation files from the given ScriptProvider.
    /// </summary>
    public static async Task<InstallationResult> ExecuteInstallationAsync(
        string connectionString,
        ScriptProvider provider,
        bool cleanInstall,
        bool resetSchedule = false,
        IProgress<InstallationProgress>? progress = null,
        Func<Task>? preValidationAction = null,
        CancellationToken cancellationToken = default)
    {
        var scriptFiles = provider.GetInstallFiles();
        ArgumentNullException.ThrowIfNull(scriptFiles);

        LogDebug(progress, $"ExecuteInstallationAsync: cleanInstall={cleanInstall}, resetSchedule={resetSchedule}, " +
            $"scriptCount={scriptFiles.Count}, providerType={provider.GetType().Name}");
        LogDebug(progress, $"ExecuteInstallationAsync: scripts=[{string.Join(", ", scriptFiles.Select(f => f.Name))}]");

        var result = new InstallationResult
        {
            StartTime = DateTime.Now
        };

        /*Perform clean install if requested*/
        if (cleanInstall)
        {
            try
            {
                await CleanInstallAsync(connectionString, progress, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                progress?.Report(new InstallationProgress
                {
                    Message = $"CLEAN INSTALL FAILED: {ex.Message}",
                    Status = "Error"
                });
                progress?.Report(new InstallationProgress
                {
                    Message = "Installation aborted - clean install was requested but failed.",
                    Status = "Error"
                });
                result.EndTime = DateTime.Now;
                result.Success = false;
                result.FilesFailed = 1;
                result.Errors.Add(("Clean Install", ex.Message));
                return result;
            }
        }

        /*
        Execute SQL files.
        Files execute without transaction wrapping because many contain DDL.
        If installation fails mid-way, use clean install to reset and retry.
        */
        progress?.Report(new InstallationProgress
        {
            Message = "Starting installation...",
            Status = "Info",
            CurrentStep = 0,
            TotalSteps = scriptFiles.Count,
            ProgressPercent = 0
        });

        bool preValidationActionRan = false;

        for (int i = 0; i < scriptFiles.Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var scriptFile = scriptFiles[i];
            string fileName = scriptFile.Name;

            /*Install community dependencies before validation runs.
              Collectors in 98_validate need sp_WhoIsActive, sp_HealthParser, etc.*/
            if (!preValidationActionRan &&
                preValidationAction != null &&
                fileName.StartsWith("98_", StringComparison.Ordinal))
            {
                preValidationActionRan = true;
                await preValidationAction().ConfigureAwait(false);
            }

            progress?.Report(new InstallationProgress
            {
                Message = $"Executing {fileName}...",
                Status = "Info",
                CurrentStep = i + 1,
                TotalSteps = scriptFiles.Count,
                ProgressPercent = (int)(((i + 1) / (double)scriptFiles.Count) * 100)
            });

            try
            {
                var fileSw = Stopwatch.StartNew();
                string sqlContent = await provider.ReadScriptAsync(scriptFile, cancellationToken).ConfigureAwait(false);
                LogDebug(progress, $"  {fileName}: read {sqlContent.Length} chars");

                /*Reset schedule to defaults if requested*/
                if (resetSchedule && fileName.StartsWith("04_", StringComparison.Ordinal))
                {
                    sqlContent = "TRUNCATE TABLE [PerformanceMonitor].[config].[collection_schedule];\nGO\n" + sqlContent;
                    progress?.Report(new InstallationProgress
                    {
                        Message = "Resetting schedule to recommended defaults...",
                        Status = "Info"
                    });
                }

                /*Remove SQLCMD directives*/
                sqlContent = Patterns.SqlCmdDirectivePattern.Replace(sqlContent, "");

                /*Execute the SQL batch*/
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                /*Split by GO statements*/
                string[] batches = Patterns.GoBatchSplitter.Split(sqlContent);
                int nonEmptyBatches = batches.Count(b => !string.IsNullOrWhiteSpace(b));
                LogDebug(progress, $"  {fileName}: {nonEmptyBatches} batches to execute");

                int batchNumber = 0;
                foreach (string batch in batches)
                {
                    string trimmedBatch = batch.Trim();
                    if (string.IsNullOrWhiteSpace(trimmedBatch))
                        continue;

                    batchNumber++;

                    using var command = new SqlCommand(trimmedBatch, connection);
                    command.CommandTimeout = StandardTimeoutSeconds;

                    try
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (SqlException ex)
                    {
                        string batchPreview = trimmedBatch.Length > 500
                            ? trimmedBatch[..500] + $"... [truncated, total length: {trimmedBatch.Length}]"
                            : trimmedBatch;
                        throw new InvalidOperationException(
                            $"Batch {batchNumber} failed:\n{batchPreview}\n\nOriginal error: {ex.Message}", ex);
                    }
                }

                LogDebug(progress, $"  {fileName}: completed in {fileSw.ElapsedMilliseconds}ms ({batchNumber} batches)");
                progress?.Report(new InstallationProgress
                {
                    Message = $"{fileName} - Success",
                    Status = "Success",
                    CurrentStep = i + 1,
                    TotalSteps = scriptFiles.Count,
                    ProgressPercent = (int)(((i + 1) / (double)scriptFiles.Count) * 100)
                });

                result.FilesSucceeded++;
            }
            catch (Exception ex)
            {
                LogDebug(progress, $"  {fileName}: FAILED — {ex.GetType().Name}: {ex.Message}");
                if (ex.InnerException != null)
                    LogDebug(progress, $"  {fileName}: InnerException — {ex.InnerException.GetType().Name}: {ex.InnerException.Message}");

                progress?.Report(new InstallationProgress
                {
                    Message = $"{fileName} - FAILED: {ex.Message}",
                    Status = "Error",
                    CurrentStep = i + 1,
                    TotalSteps = scriptFiles.Count
                });

                result.FilesFailed++;
                result.Errors.Add((fileName, ex.Message));

                /*Critical files abort installation*/
                if (Patterns.IsCriticalFile(fileName))
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = "Critical installation file failed. Aborting installation.",
                        Status = "Error"
                    });
                    break;
                }
            }
        }

        result.EndTime = DateTime.Now;
        result.Success = result.FilesFailed == 0;

        var totalDuration = result.EndTime - result.StartTime;
        LogDebug(progress, $"ExecuteInstallationAsync: finished — success={result.Success}, " +
            $"succeeded={result.FilesSucceeded}, failed={result.FilesFailed}, " +
            $"duration={totalDuration.TotalSeconds:F1}s");

        return result;
    }

    /// <summary>
    /// Run validation (master collector) after installation.
    /// </summary>
    public static async Task<(int CollectorsSucceeded, int CollectorsFailed)> RunValidationAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        progress?.Report(new InstallationProgress
        {
            Message = "Running initial collection to validate installation...",
            Status = "Info"
        });

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        /*Capture timestamp before running so we only check errors from this run.
          Use SYSDATETIME() (local) because collection_time is stored in server local time.*/
        DateTime validationStart;
        using (var command = new SqlCommand("SELECT SYSDATETIME();", connection))
        {
            validationStart = (DateTime)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
        }

        /*Run master collector with @force_run_all*/
        progress?.Report(new InstallationProgress
        {
            Message = "Executing master collector...",
            Status = "Info"
        });

        using (var command = new SqlCommand(
            "EXECUTE PerformanceMonitor.collect.scheduled_master_collector @force_run_all = 1, @debug = 0;",
            connection))
        {
            command.CommandTimeout = StandardTimeoutSeconds;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        progress?.Report(new InstallationProgress
        {
            Message = "Master collector completed",
            Status = "Success"
        });

        /*Check results - only from this validation run, not historical errors*/
        int successCount = 0;
        int errorCount = 0;

        using (var command = new SqlCommand(@"
            SELECT
                success_count = COUNT_BIG(DISTINCT CASE WHEN collection_status = 'SUCCESS' THEN collector_name END),
                error_count = SUM(CASE WHEN collection_status = 'ERROR' THEN 1 ELSE 0 END)
            FROM PerformanceMonitor.config.collection_log
            WHERE collection_time >= @validation_start;", connection))
        {
            command.Parameters.AddWithValue("@validation_start", validationStart);
            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                successCount = reader.IsDBNull(0) ? 0 : (int)reader.GetInt64(0);
                errorCount = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            }
        }

        progress?.Report(new InstallationProgress
        {
            Message = $"Validation complete: {successCount} collectors succeeded, {errorCount} failed",
            Status = errorCount == 0 ? "Success" : "Warning"
        });

        /*Show failed collectors if any*/
        if (errorCount > 0)
        {
            using var command = new SqlCommand(@"
                SELECT collector_name, error_message
                FROM PerformanceMonitor.config.collection_log
                WHERE collection_status = 'ERROR'
                AND   collection_time >= @validation_start
                ORDER BY collection_time DESC;", connection);
            command.Parameters.AddWithValue("@validation_start", validationStart);

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string name = reader["collector_name"]?.ToString() ?? "";
                string error = reader["error_message"] == DBNull.Value
                    ? "(no error message)"
                    : reader["error_message"]?.ToString() ?? "";

                progress?.Report(new InstallationProgress
                {
                    Message = $"  {name}: {error}",
                    Status = "Error"
                });
            }
        }

        return (successCount, errorCount);
    }

    /// <summary>
    /// Run installation verification diagnostics using 99_installer_troubleshooting.sql.
    /// </summary>
    public static async Task<bool> RunTroubleshootingAsync(
        string connectionString,
        ScriptProvider provider,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        bool hasErrors = false;

        try
        {
            string? scriptContent = provider.ReadTroubleshootingScript();

            if (scriptContent == null)
            {
                progress?.Report(new InstallationProgress
                {
                    Message = "Troubleshooting script not found: 99_installer_troubleshooting.sql",
                    Status = "Error"
                });
                return false;
            }

            progress?.Report(new InstallationProgress
            {
                Message = "Running installation diagnostics...",
                Status = "Info"
            });

            /*Remove SQLCMD directives*/
            scriptContent = Patterns.SqlCmdDirectivePattern.Replace(scriptContent, string.Empty);

            /*Split into batches*/
            var batches = Patterns.GoBatchSplitter.Split(scriptContent)
                .Where(b => !string.IsNullOrWhiteSpace(b))
                .ToList();

            /*Connect to master first (script will USE PerformanceMonitor)*/
            using var connection = new SqlConnection(connectionString);

            /*Capture PRINT messages and determine status*/
            connection.InfoMessage += (sender, e) =>
            {
                string message = e.Message;

                string status = "Info";
                if (message.Contains("[OK]", StringComparison.OrdinalIgnoreCase))
                    status = "Success";
                else if (message.Contains("[WARN]", StringComparison.OrdinalIgnoreCase))
                {
                    status = "Warning";
                }
                else if (message.Contains("[ERROR]", StringComparison.OrdinalIgnoreCase))
                {
                    status = "Error";
                    hasErrors = true;
                }

                progress?.Report(new InstallationProgress
                {
                    Message = message,
                    Status = status
                });
            };

            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            foreach (var batch in batches)
            {
                if (string.IsNullOrWhiteSpace(batch))
                    continue;

                cancellationToken.ThrowIfCancellationRequested();

                using var cmd = new SqlCommand(batch, connection)
                {
                    CommandTimeout = DependencyTimeoutSeconds
                };

                try
                {
                    await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (SqlException ex)
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"SQL Error: {ex.Message}",
                        Status = "Error"
                    });
                    hasErrors = true;
                }

                /*Small delay to allow UI to process messages*/
                await Task.Delay(25, cancellationToken).ConfigureAwait(false);
            }

            return !hasErrors;
        }
        catch (Exception ex)
        {
            progress?.Report(new InstallationProgress
            {
                Message = $"Diagnostics failed: {ex.Message}",
                Status = "Error"
            });
            return false;
        }
    }

    /// <summary>
    /// Generate installation summary report file.
    /// </summary>
    /// <param name="outputDirectory">Directory to write the report. Null defaults to user profile.</param>
    public static string GenerateSummaryReport(
        string serverName,
        string sqlServerVersion,
        string sqlServerEdition,
        string installerVersion,
        InstallationResult result,
        string? outputDirectory = null)
    {
        ArgumentNullException.ThrowIfNull(serverName);
        ArgumentNullException.ThrowIfNull(result);

        var duration = result.EndTime - result.StartTime;

        string timestamp = result.StartTime.ToString("yyyyMMdd_HHmmss");
        string fileName = $"PerformanceMonitor_Install_{serverName.Replace("\\", "_", StringComparison.Ordinal)}_{timestamp}.txt";
        string reportDir = outputDirectory ?? Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        string reportPath = Path.Combine(reportDir, fileName);

        var sb = new StringBuilder();

        sb.AppendLine("================================================================================");
        sb.AppendLine("Performance Monitor Installation Report");
        sb.AppendLine("================================================================================");
        sb.AppendLine();

        sb.AppendLine("INSTALLATION SUMMARY");
        sb.AppendLine("--------------------------------------------------------------------------------");
        sb.AppendLine($"Status:              {(result.Success ? "SUCCESS" : "FAILED")}");
        sb.AppendLine($"Start Time:          {result.StartTime:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"End Time:            {result.EndTime:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine($"Duration:            {duration.TotalSeconds:F1} seconds");
        sb.AppendLine($"Files Executed:      {result.FilesSucceeded}");
        sb.AppendLine($"Files Failed:        {result.FilesFailed}");
        sb.AppendLine();

        sb.AppendLine("SERVER INFORMATION");
        sb.AppendLine("--------------------------------------------------------------------------------");
        sb.AppendLine($"Server Name:         {serverName}");
        sb.AppendLine($"SQL Server Edition:  {sqlServerEdition}");
        sb.AppendLine();

        if (!string.IsNullOrEmpty(sqlServerVersion))
        {
            string[] versionLines = sqlServerVersion.Split(NewLineChars, StringSplitOptions.RemoveEmptyEntries);
            if (versionLines.Length > 0)
            {
                sb.AppendLine("SQL Server Version:");
                foreach (var line in versionLines)
                {
                    sb.AppendLine($"  {line.Trim()}");
                }
            }
        }
        sb.AppendLine();

        sb.AppendLine("INSTALLER INFORMATION");
        sb.AppendLine("--------------------------------------------------------------------------------");
        sb.AppendLine($"Installer Version:   {installerVersion}");
        sb.AppendLine($"Working Directory:   {Directory.GetCurrentDirectory()}");
        sb.AppendLine($"Machine Name:        {Environment.MachineName}");
        sb.AppendLine($"User Name:           {Environment.UserName}");
        sb.AppendLine();

        if (result.Errors.Count > 0)
        {
            sb.AppendLine("ERRORS");
            sb.AppendLine("--------------------------------------------------------------------------------");
            foreach (var (file, error) in result.Errors)
            {
                sb.AppendLine($"File: {file}");
                string errorMsg = error.Length > 500 ? error[..500] + "..." : error;
                sb.AppendLine($"Error: {errorMsg}");
                sb.AppendLine();
            }
        }

        if (result.LogMessages.Count > 0)
        {
            sb.AppendLine("DETAILED INSTALLATION LOG");
            sb.AppendLine("--------------------------------------------------------------------------------");
            foreach (var (message, status) in result.LogMessages)
            {
                string prefix = status switch
                {
                    "Success" => "[OK] ",
                    "Error" => "[ERROR] ",
                    "Warning" => "[WARN] ",
                    _ => ""
                };
                sb.AppendLine($"{prefix}{message}");
            }
            sb.AppendLine();
        }

        sb.AppendLine("================================================================================");
        sb.AppendLine("Generated by Performance Monitor Installer");
        sb.AppendLine($"Copyright (c) {DateTime.Now.Year} Darling Data, LLC");
        sb.AppendLine("================================================================================");

        File.WriteAllText(reportPath, sb.ToString());

        return reportPath;
    }

    /// <summary>
    /// Get the currently installed version from the database.
    /// Returns null if database doesn't exist or no successful installation found.
    /// </summary>
    public static async Task<string?> GetInstalledVersionAsync(
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        LogDebug(progress, "GetInstalledVersionAsync: checking for existing installation");
        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            /*Check if PerformanceMonitor database exists*/
            using var dbCheckCmd = new SqlCommand(@"
                SELECT database_id
                FROM sys.databases
                WHERE name = N'PerformanceMonitor';", connection);

            var dbExists = await dbCheckCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (dbExists == null || dbExists == DBNull.Value)
            {
                LogDebug(progress, "GetInstalledVersionAsync: database does not exist → clean install");
                return null;
            }
            LogDebug(progress, "GetInstalledVersionAsync: database exists, checking installation_history table");

            /*Check if installation_history table exists*/
            using var tableCheckCmd = new SqlCommand(@"
                USE PerformanceMonitor;
                SELECT OBJECT_ID(N'config.installation_history', N'U');", connection);

            var tableExists = await tableCheckCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (tableExists == null || tableExists == DBNull.Value)
            {
                LogDebug(progress, "GetInstalledVersionAsync: installation_history table does not exist → old or corrupted install");
                return null;
            }

            /*Get most recent successful installation version*/
            using var versionCmd = new SqlCommand(@"
                SELECT TOP 1 installer_version
                FROM PerformanceMonitor.config.installation_history
                WHERE installation_status = 'SUCCESS'
                ORDER BY installation_date DESC;", connection);

            var version = await versionCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            if (version != null && version != DBNull.Value)
            {
                LogDebug(progress, $"GetInstalledVersionAsync: found installed version {version}");
                return version.ToString();
            }

            /*
            Fallback: database and history table exist but no SUCCESS rows.
            This can happen if a prior install didn't write history (#538/#539).
            Return "1.0.0" so all idempotent upgrade scripts are attempted
            rather than treating this as a fresh install (which would drop the database).
            */
            LogDebug(progress, "GetInstalledVersionAsync: no SUCCESS rows — fallback to 1.0.0 (#538 guard)");
            return "1.0.0";
        }
        catch (SqlException ex)
        {
            LogDebug(progress, $"GetInstalledVersionAsync: SqlException — {ex.Number}: {ex.Message}");
            return null;
        }
        catch (Exception ex)
        {
            LogDebug(progress, $"GetInstalledVersionAsync: {ex.GetType().Name} — {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Execute an upgrade's SQL scripts using the ScriptProvider.
    /// Returns (successCount, failureCount).
    /// </summary>
    public static async Task<(int successCount, int failureCount)> ExecuteUpgradeAsync(
        ScriptProvider provider,
        UpgradeInfo upgrade,
        string connectionString,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        int successCount = 0;
        int failureCount = 0;

        LogDebug(progress, $"ExecuteUpgradeAsync: {upgrade.FolderName} ({upgrade.FromVersion} → {upgrade.ToVersion})");
        var upgradeSw = Stopwatch.StartNew();

        progress?.Report(new InstallationProgress
        {
            Message = $"Applying upgrade: {upgrade.FolderName}",
            Status = "Info"
        });

        var sqlFileNames = provider.GetUpgradeManifest(upgrade);
        LogDebug(progress, $"ExecuteUpgradeAsync: manifest has {sqlFileNames.Count} scripts: [{string.Join(", ", sqlFileNames)}]");

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

        foreach (var fileName in sqlFileNames)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!provider.UpgradeScriptExists(upgrade, fileName))
            {
                progress?.Report(new InstallationProgress
                {
                    Message = $"  {fileName} - WARNING: File not found",
                    Status = "Warning"
                });
                failureCount++;
                continue;
            }

            try
            {
                string sql = await provider.ReadUpgradeScriptAsync(upgrade, fileName, cancellationToken).ConfigureAwait(false);

                /*Remove SQLCMD directives*/
                sql = Patterns.SqlCmdDirectivePattern.Replace(sql, "");

                /*Split by GO statements*/
                string[] batches = Patterns.GoBatchSplitter.Split(sql);

                int batchNumber = 0;
                foreach (var batch in batches)
                {
                    batchNumber++;
                    string trimmedBatch = batch.Trim();

                    if (string.IsNullOrWhiteSpace(trimmedBatch))
                        continue;

                    using var cmd = new SqlCommand(trimmedBatch, connection);
                    cmd.CommandTimeout = UpgradeTimeoutSeconds;

                    try
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (SqlException ex)
                    {
                        string batchPreview = trimmedBatch.Length > 500
                            ? trimmedBatch[..500] + $"... [truncated, total length: {trimmedBatch.Length}]"
                            : trimmedBatch;
                        throw new InvalidOperationException(
                            $"Batch {batchNumber} failed:\n{batchPreview}\n\nOriginal error: {ex.Message}", ex);
                    }
                }

                progress?.Report(new InstallationProgress
                {
                    Message = $"  {fileName} - Success",
                    Status = "Success"
                });
                successCount++;
            }
            catch (Exception ex)
            {
                progress?.Report(new InstallationProgress
                {
                    Message = $"  {fileName} - FAILED: {ex.Message}",
                    Status = "Error"
                });
                failureCount++;
            }
        }

        progress?.Report(new InstallationProgress
        {
            Message = $"Upgrade {upgrade.FolderName}: {successCount} succeeded, {failureCount} failed",
            Status = failureCount == 0 ? "Success" : "Warning"
        });

        return (successCount, failureCount);
    }

    /// <summary>
    /// Execute all applicable upgrades in order using the ScriptProvider.
    /// </summary>
    public static async Task<(int totalSuccessCount, int totalFailureCount, int upgradeCount)> ExecuteAllUpgradesAsync(
        ScriptProvider provider,
        string connectionString,
        string? currentVersion,
        string targetVersion,
        IProgress<InstallationProgress>? progress = null,
        CancellationToken cancellationToken = default)
    {
        int totalSuccessCount = 0;
        int totalFailureCount = 0;

        var upgrades = provider.GetApplicableUpgrades(currentVersion, targetVersion,
            warning => progress?.Report(new InstallationProgress { Message = warning, Status = "Warning" }));

        if (upgrades.Count == 0)
        {
            return (0, 0, 0);
        }

        progress?.Report(new InstallationProgress
        {
            Message = $"Found {upgrades.Count} upgrade(s) to apply",
            Status = "Info"
        });

        foreach (var upgrade in upgrades)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var (success, failure) = await ExecuteUpgradeAsync(
                provider,
                upgrade,
                connectionString,
                progress,
                cancellationToken).ConfigureAwait(false);

            totalSuccessCount += success;
            totalFailureCount += failure;
        }

        return (totalSuccessCount, totalFailureCount, upgrades.Count);
    }

    /// <summary>
    /// Log installation history to config.installation_history.
    /// </summary>
    public static async Task LogInstallationHistoryAsync(
        string connectionString,
        string assemblyVersion,
        string infoVersion,
        DateTime startTime,
        int filesExecuted,
        int filesFailed,
        bool isSuccess,
        IProgress<InstallationProgress>? progress = null)
    {
        LogDebug(progress, $"LogInstallationHistoryAsync: version={assemblyVersion}, filesExecuted={filesExecuted}, " +
            $"filesFailed={filesFailed}, isSuccess={isSuccess}");
        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync().ConfigureAwait(false);

        /*Check if this is an upgrade by checking for existing installation*/
        string? previousVersion = null;
        string installationType = "INSTALL";

        try
        {
            using var checkCmd = new SqlCommand(@"
                SELECT TOP 1 installer_version
                FROM PerformanceMonitor.config.installation_history
                WHERE installation_status = 'SUCCESS'
                ORDER BY installation_date DESC;", connection);

            var result = await checkCmd.ExecuteScalarAsync().ConfigureAwait(false);
            if (result != null && result != DBNull.Value)
            {
                previousVersion = result.ToString();
                bool isSameVersion = Version.TryParse(previousVersion, out var prevVer)
                    && Version.TryParse(assemblyVersion, out var currVer)
                    && prevVer == currVer;
                installationType = isSameVersion ? "REINSTALL" : "UPGRADE";
            }
        }
        catch (SqlException)
        {
            /*Table might not exist yet on first install*/
        }

        /*Get SQL Server version info*/
        string sqlVersion = "";
        string sqlEdition = "";

        using (var versionCmd = new SqlCommand("SELECT @@VERSION, SERVERPROPERTY('Edition');", connection))
        using (var reader = await versionCmd.ExecuteReaderAsync().ConfigureAwait(false))
        {
            if (await reader.ReadAsync().ConfigureAwait(false))
            {
                sqlVersion = reader.GetString(0);
                sqlEdition = reader.GetString(1);
            }
        }

        long durationMs = (long)(DateTime.Now - startTime).TotalMilliseconds;
        string status = isSuccess ? "SUCCESS" : (filesFailed > 0 ? "PARTIAL" : "FAILED");

        var insertSql = @"
            INSERT INTO PerformanceMonitor.config.installation_history
            (
                installer_version,
                installer_info_version,
                sql_server_version,
                sql_server_edition,
                installation_type,
                previous_version,
                installation_status,
                files_executed,
                files_failed,
                installation_duration_ms
            )
            VALUES
            (
                @installer_version,
                @installer_info_version,
                @sql_server_version,
                @sql_server_edition,
                @installation_type,
                @previous_version,
                @installation_status,
                @files_executed,
                @files_failed,
                @installation_duration_ms
            );";

        using var insertCmd = new SqlCommand(insertSql, connection);
        insertCmd.Parameters.Add(new SqlParameter("@installer_version", SqlDbType.NVarChar, 50) { Value = assemblyVersion });
        insertCmd.Parameters.Add(new SqlParameter("@installer_info_version", SqlDbType.NVarChar, 100) { Value = (object?)infoVersion ?? DBNull.Value });
        insertCmd.Parameters.Add(new SqlParameter("@sql_server_version", SqlDbType.NVarChar, 500) { Value = sqlVersion });
        insertCmd.Parameters.Add(new SqlParameter("@sql_server_edition", SqlDbType.NVarChar, 128) { Value = sqlEdition });
        insertCmd.Parameters.Add(new SqlParameter("@installation_type", SqlDbType.VarChar, 20) { Value = installationType });
        insertCmd.Parameters.Add(new SqlParameter("@previous_version", SqlDbType.NVarChar, 50) { Value = (object?)previousVersion ?? DBNull.Value });
        insertCmd.Parameters.Add(new SqlParameter("@installation_status", SqlDbType.VarChar, 20) { Value = status });
        insertCmd.Parameters.Add(new SqlParameter("@files_executed", SqlDbType.Int) { Value = filesExecuted });
        insertCmd.Parameters.Add(new SqlParameter("@files_failed", SqlDbType.Int) { Value = filesFailed });
        insertCmd.Parameters.Add(new SqlParameter("@installation_duration_ms", SqlDbType.BigInt) { Value = durationMs });

        await insertCmd.ExecuteNonQueryAsync().ConfigureAwait(false);
        LogDebug(progress, $"LogInstallationHistoryAsync: wrote {installationType} record (status={status}, previousVersion={previousVersion ?? "null"})");
    }
}
