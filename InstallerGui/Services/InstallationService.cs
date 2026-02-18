/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorInstallerGui.Services
{
    /// <summary>
    /// Progress information for installation steps
    /// </summary>
    public class InstallationProgress
    {
        public string Message { get; set; } = string.Empty;
        public string Status { get; set; } = "Info"; // Info, Success, Error, Warning
        public int? CurrentStep { get; set; }
        public int? TotalSteps { get; set; }
        public int? ProgressPercent { get; set; }
    }

    /// <summary>
    /// Server information returned from connection test
    /// </summary>
    public class ServerInfo
    {
        public string ServerName { get; set; } = string.Empty;
        public string SqlServerVersion { get; set; } = string.Empty;
        public string SqlServerEdition { get; set; } = string.Empty;
        public bool IsConnected { get; set; }
        public string? ErrorMessage { get; set; }
    }

    /// <summary>
    /// Installation result summary
    /// </summary>
    public class InstallationResult
    {
        public bool Success { get; set; }
        public int FilesSucceeded { get; set; }
        public int FilesFailed { get; set; }
        public List<(string FileName, string ErrorMessage)> Errors { get; } = new();
        public List<(string Message, string Status)> LogMessages { get; } = new();
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public string? ReportPath { get; set; }
    }

    /// <summary>
    /// Service for installing the Performance Monitor database
    /// </summary>
    public class InstallationService : IDisposable
    {
        private readonly HttpClient _httpClient;
        private bool _disposed;

        /*
        Compiled regex patterns for better performance
        */
        private static readonly Regex SqlFilePattern = new(
            @"^\d{2}[a-z]?_.*\.sql$",
            RegexOptions.Compiled);

        private static readonly Regex SqlCmdDirectivePattern = new(
            @"^:r\s+.*$",
            RegexOptions.Compiled | RegexOptions.Multiline);

        private static readonly Regex GoBatchSplitter = new(
            @"^\s*GO\s*(?:--[^\r\n]*)?\s*$",
            RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

        private static readonly char[] NewLineChars = { '\r', '\n' };

        public InstallationService()
        {
            _httpClient = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(30)
            };
        }

        /// <summary>
        /// Build a connection string from the provided parameters
        /// </summary>
        public static string BuildConnectionString(
            string server,
            bool useWindowsAuth,
            string? username = null,
            string? password = null,
            string encryption = "Mandatory",
            bool trustCertificate = false)
        {
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = server,
                InitialCatalog = "master",
                TrustServerCertificate = trustCertificate,
                IntegratedSecurity = useWindowsAuth
            };

            /*Set encryption mode: Optional, Mandatory, or Strict*/
            builder.Encrypt = encryption switch
            {
                "Mandatory" => SqlConnectionEncryptOption.Mandatory,
                "Strict" => SqlConnectionEncryptOption.Strict,
                _ => SqlConnectionEncryptOption.Mandatory
            };

            if (!useWindowsAuth)
            {
                builder.UserID = username;
                builder.Password = password;
            }

            return builder.ConnectionString;
        }

        /// <summary>
        /// Test connection to SQL Server and get server information
        /// </summary>
        public static async Task<ServerInfo> TestConnectionAsync(string connectionString, CancellationToken cancellationToken = default)
        {
            var info = new ServerInfo();

            try
            {
                using var connection = new SqlConnection(connectionString);
                await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                info.IsConnected = true;

                using var command = new SqlCommand("SELECT @@VERSION, SERVERPROPERTY('Edition'), @@SERVERNAME;", connection);
                using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

                if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    info.SqlServerVersion = reader.GetString(0);
                    info.SqlServerEdition = reader.GetString(1);
                    info.ServerName = reader.GetString(2);
                }
            }
            catch (Exception ex)
            {
                info.IsConnected = false;
                info.ErrorMessage = ex.Message;
                if (ex.InnerException != null)
                {
                    info.ErrorMessage += $"\n{ex.InnerException.Message}";
                }
            }

            return info;
        }

        /// <summary>
        /// Find SQL installation files
        /// </summary>
        public static (string? SqlDirectory, string? MonitorRootDirectory, List<string> SqlFiles) FindInstallationFiles()
        {
            string? sqlDirectory = null;
            string? monitorRootDirectory = null;
            var sqlFiles = new List<string>();

            /*Try multiple starting locations: current directory and executable location*/
            var startingDirectories = new List<string>
            {
                Directory.GetCurrentDirectory(),
                AppDomain.CurrentDomain.BaseDirectory
            };

            foreach (string startDir in startingDirectories.Distinct())
            {
                if (sqlDirectory != null)
                    break;

                DirectoryInfo? searchDir = new DirectoryInfo(startDir);

                for (int i = 0; i < 6 && searchDir != null; i++)
                {
                    /*Check for install/ subfolder first (new structure)*/
                    string installFolder = Path.Combine(searchDir.FullName, "install");
                    if (Directory.Exists(installFolder))
                    {
                        var installFiles = Directory.GetFiles(installFolder, "*.sql")
                            .Where(f => SqlFilePattern.IsMatch(Path.GetFileName(f)))
                            .ToList();

                        if (installFiles.Count > 0)
                        {
                            sqlDirectory = installFolder;
                            monitorRootDirectory = searchDir.FullName;
                            break;
                        }
                    }

                    /*Fall back to old structure (SQL files in root)*/
                    var files = Directory.GetFiles(searchDir.FullName, "*.sql")
                        .Where(f => SqlFilePattern.IsMatch(Path.GetFileName(f)))
                        .ToList();

                    if (files.Count > 0)
                    {
                        sqlDirectory = searchDir.FullName;
                        monitorRootDirectory = searchDir.FullName;
                        break;
                    }

                    searchDir = searchDir.Parent;
                }
            }

            if (sqlDirectory != null)
            {
                sqlFiles = Directory.GetFiles(sqlDirectory, "*.sql")
                    .Where(f =>
                    {
                        string fileName = Path.GetFileName(f);
                        /*Match numbered SQL files but exclude 97 (tests) and 99 (troubleshooting)*/
                        if (!SqlFilePattern.IsMatch(fileName))
                            return false;
                        /*Exclude test and troubleshooting scripts from main install*/
                        if (fileName.StartsWith("97_", StringComparison.Ordinal) ||
                            fileName.StartsWith("99_", StringComparison.Ordinal))
                            return false;
                        return true;
                    })
                    .OrderBy(f => Path.GetFileName(f))
                    .ToList();
            }

            return (sqlDirectory, monitorRootDirectory, sqlFiles);
        }

        /// <summary>
        /// Perform clean install (drop existing database and jobs)
        /// </summary>
        public static async Task CleanInstallAsync(
            string connectionString,
            IProgress<InstallationProgress>? progress = null,
            CancellationToken cancellationToken = default)
        {
            progress?.Report(new InstallationProgress
            {
                Message = "Performing clean install...",
                Status = "Info"
            });

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            /*
            Stop any existing traces before dropping database
            */
            try
            {
                using var traceCmd = new SqlCommand(
                    "EXECUTE PerformanceMonitor.collect.trace_management_collector @action = 'STOP';",
                    connection);
                traceCmd.CommandTimeout = 60;
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

            /*
            Remove Agent jobs and database
            */
            string cleanupSql = @"
USE msdb;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Collection')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Collection';
END;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Data Retention')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Data Retention';
END;

USE master;

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'PerformanceMonitor')
BEGIN
    ALTER DATABASE PerformanceMonitor SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE PerformanceMonitor;
END;";

            using var command = new SqlCommand(cleanupSql, connection);
            command.CommandTimeout = 60;
            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

            progress?.Report(new InstallationProgress
            {
                Message = "Clean install completed (jobs and database removed)",
                Status = "Success"
            });
        }

        /// <summary>
        /// Execute SQL installation files
        /// </summary>
        public static async Task<InstallationResult> ExecuteInstallationAsync(
            string connectionString,
            List<string> sqlFiles,
            bool cleanInstall,
            bool resetSchedule = false,
            IProgress<InstallationProgress>? progress = null,
            Func<Task>? preValidationAction = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(sqlFiles);

            var result = new InstallationResult
            {
                StartTime = DateTime.Now
            };

            /*
            Perform clean install if requested
            */
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
            Execute SQL files
            Note: Files execute without transaction wrapping because many contain DDL.
            If installation fails mid-way, use clean install to reset and retry.
            */
            progress?.Report(new InstallationProgress
            {
                Message = "Starting installation...",
                Status = "Info",
                CurrentStep = 0,
                TotalSteps = sqlFiles.Count,
                ProgressPercent = 0
            });

            bool preValidationActionRan = false;

            for (int i = 0; i < sqlFiles.Count; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string sqlFile = sqlFiles[i];
                string fileName = Path.GetFileName(sqlFile);

                /*Install community dependencies before validation runs
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
                    TotalSteps = sqlFiles.Count,
                    ProgressPercent = (int)(((i + 1) / (double)sqlFiles.Count) * 100)
                });

                try
                {
                    string sqlContent = await File.ReadAllTextAsync(sqlFile, cancellationToken).ConfigureAwait(false);

                    /*Reset schedule to defaults if requested*/
                    if (resetSchedule && fileName.StartsWith("04_", StringComparison.Ordinal))
                    {
                        sqlContent = "TRUNCATE TABLE config.collection_schedule;\nGO\n" + sqlContent;
                        progress?.Report(new InstallationProgress
                        {
                            Message = "Resetting schedule to recommended defaults...",
                            Status = "Info"
                        });
                    }

                    /*Remove SQLCMD directives*/
                    sqlContent = SqlCmdDirectivePattern.Replace(sqlContent, "");

                    /*Execute the SQL batch*/
                    using var connection = new SqlConnection(connectionString);
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                    /*Split by GO statements*/
                    string[] batches = GoBatchSplitter.Split(sqlContent);

                    int batchNumber = 0;
                    foreach (string batch in batches)
                    {
                        string trimmedBatch = batch.Trim();
                        if (string.IsNullOrWhiteSpace(trimmedBatch))
                            continue;

                        batchNumber++;

                        using var command = new SqlCommand(trimmedBatch, connection);
                        command.CommandTimeout = 300;

                        try
                        {
                            await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (SqlException ex)
                        {
                            string batchPreview = trimmedBatch.Length > 500
                                ? trimmedBatch.Substring(0, 500) + $"... [truncated, total length: {trimmedBatch.Length}]"
                                : trimmedBatch;
                            throw new InvalidOperationException(
                                $"Batch {batchNumber} failed:\n{batchPreview}\n\nOriginal error: {ex.Message}", ex);
                        }
                    }

                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{fileName} - Success",
                        Status = "Success",
                        CurrentStep = i + 1,
                        TotalSteps = sqlFiles.Count,
                        ProgressPercent = (int)(((i + 1) / (double)sqlFiles.Count) * 100)
                    });

                    result.FilesSucceeded++;
                }
                catch (Exception ex)
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{fileName} - FAILED: {ex.Message}",
                        Status = "Error",
                        CurrentStep = i + 1,
                        TotalSteps = sqlFiles.Count
                    });

                    result.FilesFailed++;
                    result.Errors.Add((fileName, ex.Message));

                    /*Critical files abort installation*/
                    if (fileName.StartsWith("01_", StringComparison.Ordinal) ||
                        fileName.StartsWith("02_", StringComparison.Ordinal) ||
                        fileName.StartsWith("03_", StringComparison.Ordinal))
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

            /*Allow query_snapshots to fail - those views get created eventually*/
            bool onlyQuerySnapshotsFailed = result.FilesFailed == 1 &&
                result.Errors.Any(e => e.FileName.Contains("query_snapshots", StringComparison.OrdinalIgnoreCase));
            result.Success = result.FilesFailed == 0 || onlyQuerySnapshotsFailed;

            return result;
        }

        /// <summary>
        /// Install community dependencies (sp_WhoIsActive, DarlingData, First Responder Kit)
        /// </summary>
        public async Task<int> InstallDependenciesAsync(
            string connectionString,
            IProgress<InstallationProgress>? progress = null,
            CancellationToken cancellationToken = default)
        {
            var dependencies = new List<(string Name, string Url, string Description)>
            {
                (
                    "sp_WhoIsActive",
                    "https://raw.githubusercontent.com/amachanic/sp_whoisactive/refs/heads/master/sp_WhoIsActive.sql",
                    "Query activity monitoring by Adam Machanic (GPLv3)"
                ),
                (
                    "DarlingData",
                    "https://raw.githubusercontent.com/erikdarlingdata/DarlingData/main/Install-All/DarlingData.sql",
                    "sp_HealthParser, sp_HumanEventsBlockViewer by Erik Darling (MIT)"
                ),
                (
                    "First Responder Kit",
                    "https://raw.githubusercontent.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/refs/heads/main/Install-All-Scripts.sql",
                    "sp_BlitzLock and diagnostic tools by Brent Ozar Unlimited (MIT)"
                )
            };

            progress?.Report(new InstallationProgress
            {
                Message = "Installing community dependencies...",
                Status = "Info"
            });

            int successCount = 0;

            foreach (var (name, url, description) in dependencies)
            {
                cancellationToken.ThrowIfCancellationRequested();

                progress?.Report(new InstallationProgress
                {
                    Message = $"Installing {name}...",
                    Status = "Info"
                });

                try
                {
                    string sql = await DownloadWithRetryAsync(_httpClient, url, progress, cancellationToken: cancellationToken).ConfigureAwait(false);

                    if (string.IsNullOrWhiteSpace(sql))
                    {
                        progress?.Report(new InstallationProgress
                        {
                            Message = $"{name} - FAILED (empty response)",
                            Status = "Error"
                        });
                        continue;
                    }

                    using var connection = new SqlConnection(connectionString);
                    await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

                    using (var useDbCommand = new SqlCommand("USE PerformanceMonitor;", connection))
                    {
                        await useDbCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    string[] batches = GoBatchSplitter.Split(sql);

                    foreach (string batch in batches)
                    {
                        string trimmedBatch = batch.Trim();
                        if (string.IsNullOrWhiteSpace(trimmedBatch))
                            continue;

                        using var command = new SqlCommand(trimmedBatch, connection);
                        command.CommandTimeout = 120;
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }

                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{name} - Success ({description})",
                        Status = "Success"
                    });

                    successCount++;
                }
                catch (HttpRequestException ex)
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{name} - Download failed: {ex.Message}",
                        Status = "Error"
                    });
                }
                catch (SqlException ex)
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{name} - SQL execution failed: {ex.Message}",
                        Status = "Error"
                    });
                }
                catch (Exception ex)
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"{name} - Failed: {ex.Message}",
                        Status = "Error"
                    });
                }
            }

            progress?.Report(new InstallationProgress
            {
                Message = $"Dependencies installed: {successCount}/{dependencies.Count}",
                Status = successCount == dependencies.Count ? "Success" : "Warning"
            });

            return successCount;
        }

        /// <summary>
        /// Run validation (master collector) after installation
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
                command.CommandTimeout = 300;
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            progress?.Report(new InstallationProgress
            {
                Message = "Master collector completed",
                Status = "Success"
            });

            /*Check results*/
            int successCount = 0;
            int errorCount = 0;

            using (var command = new SqlCommand(@"
                SELECT
                    success_count = COUNT_BIG(DISTINCT CASE WHEN collection_status = 'SUCCESS' THEN collector_name END),
                    error_count = SUM(CASE WHEN collection_status = 'ERROR' THEN 1 ELSE 0 END)
                FROM PerformanceMonitor.config.collection_log;", connection))
            {
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
                    ORDER BY collection_time DESC;", connection);

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
        /// Run installation verification diagnostics using 99_installer_troubleshooting.sql
        /// </summary>
        public static async Task<bool> RunTroubleshootingAsync(
            string connectionString,
            string sqlDirectory,
            IProgress<InstallationProgress>? progress = null,
            CancellationToken cancellationToken = default)
        {
            bool hasErrors = false;

            try
            {
                /*Find the troubleshooting script*/
                string scriptPath = Path.Combine(sqlDirectory, "99_installer_troubleshooting.sql");
                if (!File.Exists(scriptPath))
                {
                    /*Try parent directory (install folder might be one level up)*/
                    string? parentDir = Directory.GetParent(sqlDirectory)?.FullName;
                    if (parentDir != null)
                    {
                        string altPath = Path.Combine(parentDir, "install", "99_installer_troubleshooting.sql");
                        if (File.Exists(altPath))
                            scriptPath = altPath;
                    }
                }

                if (!File.Exists(scriptPath))
                {
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"Troubleshooting script not found: 99_installer_troubleshooting.sql",
                        Status = "Error"
                    });
                    return false;
                }

                progress?.Report(new InstallationProgress
                {
                    Message = "Running installation diagnostics...",
                    Status = "Info"
                });

                /*Read and prepare the script*/
                string scriptContent = await File.ReadAllTextAsync(scriptPath, cancellationToken).ConfigureAwait(false);

                /*Remove SQLCMD directives*/
                scriptContent = SqlCmdDirectivePattern.Replace(scriptContent, string.Empty);

                /*Split into batches*/
                var batches = GoBatchSplitter.Split(scriptContent)
                    .Where(b => !string.IsNullOrWhiteSpace(b))
                    .ToList();

                /*Connect to master first (script will USE PerformanceMonitor)*/
                using var connection = new SqlConnection(connectionString);

                /*Capture PRINT messages and determine status*/
                connection.InfoMessage += (sender, e) =>
                {
                    string message = e.Message;

                    /*Determine status based on message content*/
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

                /*Execute each batch*/
                foreach (var batch in batches)
                {
                    if (string.IsNullOrWhiteSpace(batch))
                        continue;

                    cancellationToken.ThrowIfCancellationRequested();

                    using var cmd = new SqlCommand(batch, connection)
                    {
                        CommandTimeout = 120
                    };

                    try
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (SqlException ex)
                    {
                        /*Report SQL errors but continue with remaining batches*/
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
        /// Generate installation summary report file
        /// </summary>
        public static string GenerateSummaryReport(
            string serverName,
            string sqlServerVersion,
            string sqlServerEdition,
            string installerVersion,
            InstallationResult result)
        {
            ArgumentNullException.ThrowIfNull(serverName);
            ArgumentNullException.ThrowIfNull(result);

            var duration = result.EndTime - result.StartTime;

            string timestamp = result.StartTime.ToString("yyyyMMdd_HHmmss");
            string fileName = $"PerformanceMonitor_Install_{serverName.Replace("\\", "_", StringComparison.Ordinal)}_{timestamp}.txt";
            string reportPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), fileName);

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
                    sb.AppendLine($"SQL Server Version:");
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
                    string errorMsg = error.Length > 500 ? error.Substring(0, 500) + "..." : error;
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
            sb.AppendLine("Generated by Performance Monitor Installer GUI");
            sb.AppendLine($"Copyright (c) {DateTime.Now.Year} Darling Data, LLC");
            sb.AppendLine("================================================================================");

            File.WriteAllText(reportPath, sb.ToString());

            return reportPath;
        }

        /// <summary>
        /// Information about an applicable upgrade
        /// </summary>
        public class UpgradeInfo
        {
            public string Path { get; set; } = string.Empty;
            public string FolderName { get; set; } = string.Empty;
            public Version? FromVersion { get; set; }
            public Version? ToVersion { get; set; }
        }

        /// <summary>
        /// Get the currently installed version from the database
        /// Returns null if database doesn't exist or no successful installation found
        /// </summary>
        public static async Task<string?> GetInstalledVersionAsync(
            string connectionString,
            CancellationToken cancellationToken = default)
        {
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
                    return null; /*Database doesn't exist - clean install needed*/
                }

                /*Check if installation_history table exists*/
                using var tableCheckCmd = new SqlCommand(@"
                    USE PerformanceMonitor;
                    SELECT OBJECT_ID(N'config.installation_history', N'U');", connection);

                var tableExists = await tableCheckCmd.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (tableExists == null || tableExists == DBNull.Value)
                {
                    return null; /*Table doesn't exist - old version or corrupted install*/
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
                    return version.ToString();
                }

                return null;
            }
            catch (SqlException)
            {
                /*Connection or query failed - treat as no version installed*/
                return null;
            }
            catch (Exception)
            {
                /*Any other error - treat as no version installed*/
                return null;
            }
        }

        /// <summary>
        /// Find upgrade folders that need to be applied
        /// Returns list of upgrade info in order of application
        /// Filters by version: only applies upgrades where FromVersion >= currentVersion and ToVersion <= targetVersion
        /// </summary>
        public static List<UpgradeInfo> GetApplicableUpgrades(
            string monitorRootDirectory,
            string? currentVersion,
            string targetVersion)
        {
            var upgrades = new List<UpgradeInfo>();
            string upgradesDirectory = Path.Combine(monitorRootDirectory, "upgrades");

            if (!Directory.Exists(upgradesDirectory))
            {
                return upgrades; /*No upgrades folder - return empty list*/
            }

            /*If there's no current version, it's a clean install - no upgrades needed*/
            if (currentVersion == null)
            {
                return upgrades;
            }

            /*Parse current version - if invalid, skip upgrades*/
            if (!Version.TryParse(currentVersion, out var current))
            {
                return upgrades;
            }

            /*Parse target version - if invalid, skip upgrades*/
            if (!Version.TryParse(targetVersion, out var target))
            {
                return upgrades;
            }

            /*
            Find all upgrade folders matching pattern: {from}-to-{to}
            Parse versions and filter to only applicable upgrades
            */
            var applicableUpgrades = Directory.GetDirectories(upgradesDirectory)
                .Select(d => new UpgradeInfo
                {
                    Path = d,
                    FolderName = Path.GetFileName(d)
                })
                .Where(x => x.FolderName.Contains("-to-", StringComparison.Ordinal))
                .Select(x =>
                {
                    var parts = x.FolderName.Split("-to-");
                    x.FromVersion = Version.TryParse(parts[0], out var from) ? from : null;
                    x.ToVersion = parts.Length > 1 && Version.TryParse(parts[1], out var to) ? to : null;
                    return x;
                })
                .Where(x => x.FromVersion != null && x.ToVersion != null)
                .Where(x => x.FromVersion >= current)   /*Don't re-apply old upgrades*/
                .Where(x => x.ToVersion <= target)      /*Don't apply future upgrades*/
                .OrderBy(x => x.FromVersion)
                .ToList();

            foreach (var upgrade in applicableUpgrades)
            {
                string upgradeFile = Path.Combine(upgrade.Path, "upgrade.txt");
                if (File.Exists(upgradeFile))
                {
                    upgrades.Add(upgrade);
                }
            }

            return upgrades;
        }

        /// <summary>
        /// Execute an upgrade folder's SQL scripts
        /// Returns (successCount, failureCount)
        /// </summary>
        public static async Task<(int successCount, int failureCount)> ExecuteUpgradeAsync(
            string upgradeFolder,
            string connectionString,
            IProgress<InstallationProgress>? progress = null,
            CancellationToken cancellationToken = default)
        {
            int successCount = 0;
            int failureCount = 0;

            string upgradeName = Path.GetFileName(upgradeFolder);
            string upgradeFile = Path.Combine(upgradeFolder, "upgrade.txt");

            progress?.Report(new InstallationProgress
            {
                Message = $"Applying upgrade: {upgradeName}",
                Status = "Info"
            });

            /*Read the upgrade.txt file to get ordered list of SQL files*/
            var sqlFileNames = (await File.ReadAllLinesAsync(upgradeFile, cancellationToken).ConfigureAwait(false))
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith('#'))
                .Select(line => line.Trim())
                .ToList();

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            foreach (var fileName in sqlFileNames)
            {
                cancellationToken.ThrowIfCancellationRequested();

                string filePath = Path.Combine(upgradeFolder, fileName);

                if (!File.Exists(filePath))
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
                    string sql = await File.ReadAllTextAsync(filePath, cancellationToken).ConfigureAwait(false);

                    /*Remove SQLCMD directives*/
                    sql = SqlCmdDirectivePattern.Replace(sql, "");

                    /*Split by GO statements*/
                    string[] batches = GoBatchSplitter.Split(sql);

                    int batchNumber = 0;
                    foreach (var batch in batches)
                    {
                        batchNumber++;
                        string trimmedBatch = batch.Trim();

                        if (string.IsNullOrWhiteSpace(trimmedBatch))
                            continue;

                        using var cmd = new SqlCommand(trimmedBatch, connection);
                        cmd.CommandTimeout = 300;

                        try
                        {
                            await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                        }
                        catch (SqlException ex)
                        {
                            /*Add batch info to error message*/
                            string batchPreview = trimmedBatch.Length > 500
                                ? trimmedBatch.Substring(0, 500) + $"... [truncated, total length: {trimmedBatch.Length}]"
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
                Message = $"Upgrade {upgradeName}: {successCount} succeeded, {failureCount} failed",
                Status = failureCount == 0 ? "Success" : "Warning"
            });

            return (successCount, failureCount);
        }

        /// <summary>
        /// Execute all applicable upgrades in order
        /// </summary>
        public static async Task<(int totalSuccessCount, int totalFailureCount, int upgradeCount)> ExecuteAllUpgradesAsync(
            string monitorRootDirectory,
            string connectionString,
            string? currentVersion,
            string targetVersion,
            IProgress<InstallationProgress>? progress = null,
            CancellationToken cancellationToken = default)
        {
            int totalSuccessCount = 0;
            int totalFailureCount = 0;

            var upgrades = GetApplicableUpgrades(monitorRootDirectory, currentVersion, targetVersion);

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
                    upgrade.Path,
                    connectionString,
                    progress,
                    cancellationToken).ConfigureAwait(false);

                totalSuccessCount += success;
                totalFailureCount += failure;
            }

            return (totalSuccessCount, totalFailureCount, upgrades.Count);
        }

        /*
        Download content from URL with retry logic for transient failures
        Uses exponential backoff: 2s, 4s, 8s between retries
        */
        private static async Task<string> DownloadWithRetryAsync(
            HttpClient client,
            string url,
            IProgress<InstallationProgress>? progress = null,
            int maxRetries = 3,
            CancellationToken cancellationToken = default)
        {
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    return await client.GetStringAsync(url, cancellationToken).ConfigureAwait(false);
                }
                catch (HttpRequestException) when (attempt < maxRetries)
                {
                    int delaySeconds = (int)Math.Pow(2, attempt); /*2s, 4s, 8s*/
                    progress?.Report(new InstallationProgress
                    {
                        Message = $"Network error, retrying in {delaySeconds}s ({attempt}/{maxRetries})...",
                        Status = "Warning"
                    });
                    await Task.Delay(delaySeconds * 1000, cancellationToken).ConfigureAwait(false);
                }
            }
            /*Final attempt - let exception propagate if it fails*/
            return await client.GetStringAsync(url, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Dispose of managed resources
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _httpClient?.Dispose();
                _disposed = true;
            }
            GC.SuppressFinalize(this);
        }
    }
}
