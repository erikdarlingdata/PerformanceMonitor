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
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Reflection;
using Microsoft.Data.SqlClient;

namespace PerformanceMonitorInstaller
{
    class Program
    {
        /*
        Pre-compiled regex patterns for performance
        */
        private static readonly Regex GoBatchPattern = new Regex(
            @"^\s*GO\s*(?:--[^\r\n]*)?\s*$",
            RegexOptions.Compiled | RegexOptions.Multiline | RegexOptions.IgnoreCase);

        private static readonly Regex SqlFileNamePattern = new Regex(
            @"^\d{2}[a-z]?_.*\.sql$",
            RegexOptions.Compiled);

        private static readonly Regex SqlCmdDirectivePattern = new Regex(
            @"^:r\s+.*$",
            RegexOptions.Compiled | RegexOptions.Multiline);

        /*
        SQL command timeout constants (in seconds)
        */
        private const int ShortTimeoutSeconds = 60;       // Quick operations (cleanup, queries)
        private const int MediumTimeoutSeconds = 120;     // Dependency installation
        private const int LongTimeoutSeconds = 300;       // SQL file execution (5 minutes)

        /*
        Exit codes for granular error reporting
        */
        private static class ExitCodes
        {
            public const int Success = 0;
            public const int InvalidArguments = 1;
            public const int ConnectionFailed = 2;
            public const int CriticalFileFailed = 3;
            public const int PartialInstallation = 4;
            public const int VersionCheckFailed = 5;
            public const int SqlFilesNotFound = 6;
        }

        static async Task<int> Main(string[] args)
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version?.ToString() ?? "Unknown";
            var infoVersion = Assembly.GetExecutingAssembly()
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ?? version;

            Console.WriteLine("================================================================================");
            Console.WriteLine($"Performance Monitor Installation Utility v{infoVersion}");
            Console.WriteLine("Copyright © 2026 Darling Data, LLC");
            Console.WriteLine("Licensed under the MIT License");
            Console.WriteLine("https://github.com/erikdarlingdata/PerformanceMonitor");
            Console.WriteLine("================================================================================");
            Console.WriteLine();

            /*
            Determine if running in automated mode (command-line arguments provided)
            Usage: PerformanceMonitorInstaller.exe [server] [username] [password] [options]
            If server is provided alone, uses Windows Authentication
            If server, username, and password are provided, uses SQL Authentication

            Options:
              --reinstall       Drop existing database and perform clean install
              --encrypt=X       Connection encryption: mandatory (default), optional, strict
              --trust-cert      Trust server certificate without validation (default: require valid cert)
            */
            bool automatedMode = args.Length > 0;
            bool reinstallMode = args.Any(a => a.Equals("--reinstall", StringComparison.OrdinalIgnoreCase));
            bool resetSchedule = args.Any(a => a.Equals("--reset-schedule", StringComparison.OrdinalIgnoreCase));
            bool trustCert = args.Any(a => a.Equals("--trust-cert", StringComparison.OrdinalIgnoreCase));

            /*Parse encryption option (default: Mandatory)*/
            var encryptArg = args.FirstOrDefault(a => a.StartsWith("--encrypt=", StringComparison.OrdinalIgnoreCase));
            SqlConnectionEncryptOption encryptOption = SqlConnectionEncryptOption.Mandatory;
            if (encryptArg != null)
            {
                string encryptValue = encryptArg.Substring("--encrypt=".Length).ToLowerInvariant();
                encryptOption = encryptValue switch
                {
                    "optional" => SqlConnectionEncryptOption.Optional,
                    "strict" => SqlConnectionEncryptOption.Strict,
                    _ => SqlConnectionEncryptOption.Mandatory
                };
            }

            /*Filter out option flags to get positional arguments*/
            var filteredArgs = args
                .Where(a => !a.Equals("--reinstall", StringComparison.OrdinalIgnoreCase))
                .Where(a => !a.Equals("--reset-schedule", StringComparison.OrdinalIgnoreCase))
                .Where(a => !a.Equals("--trust-cert", StringComparison.OrdinalIgnoreCase))
                .Where(a => !a.StartsWith("--encrypt=", StringComparison.OrdinalIgnoreCase))
                .ToArray();
            string? serverName;
            string? username = null;
            string? password = null;
            bool useWindowsAuth;

            if (automatedMode)
            {
                /*
                Automated mode with command-line arguments
                */
                serverName = filteredArgs.Length > 0 ? filteredArgs[0] : null;

                if (filteredArgs.Length >= 2)
                {
                    /*SQL Authentication - password from env var or command-line*/
                    useWindowsAuth = false;
                    username = filteredArgs[1];

                    string? envPassword = Environment.GetEnvironmentVariable("PM_SQL_PASSWORD");
                    if (filteredArgs.Length >= 3)
                    {
                        password = filteredArgs[2];
                        if (envPassword == null)
                        {
                            Console.WriteLine("Note: Password provided via command-line is visible in process listings.");
                            Console.WriteLine("      Consider using PM_SQL_PASSWORD environment variable instead.");
                            Console.WriteLine();
                        }
                    }
                    else if (envPassword != null)
                    {
                        password = envPassword;
                    }
                    else
                    {
                        Console.WriteLine("Error: Password is required for SQL Server Authentication.");
                        Console.WriteLine("Provide password as third argument or set PM_SQL_PASSWORD environment variable.");
                        return ExitCodes.InvalidArguments;
                    }

                    Console.WriteLine($"Server: {serverName}");
                    Console.WriteLine($"Authentication: SQL Server ({username})");
                }
                else if (filteredArgs.Length == 1)
                {
                    /*Windows Authentication*/
                    useWindowsAuth = true;
                    Console.WriteLine($"Server: {serverName}");
                    Console.WriteLine($"Authentication: Windows");
                }
                else
                {
                    Console.WriteLine("Error: Invalid arguments.");
                    Console.WriteLine("Usage:");
                    Console.WriteLine("  Windows Auth:   PerformanceMonitorInstaller.exe <server> [options]");
                    Console.WriteLine("  SQL Auth:       PerformanceMonitorInstaller.exe <server> <username> <password> [options]");
                    Console.WriteLine("  SQL Auth:       PerformanceMonitorInstaller.exe <server> <username> [options]");
                    Console.WriteLine("                  (with PM_SQL_PASSWORD environment variable set)");
                    Console.WriteLine();
                    Console.WriteLine("Options:");
                    Console.WriteLine("  --reinstall          Drop existing database and perform clean install");
                    Console.WriteLine("  --reset-schedule     Reset collection schedule to recommended defaults");
                    Console.WriteLine("  --encrypt=<level>    Connection encryption: optional (default), mandatory, strict");
                    Console.WriteLine("  --trust-cert         Trust server certificate without validation (default: require valid cert)");
                    return ExitCodes.InvalidArguments;
                }
            }
            else
            {
                /*
                Interactive mode - prompt for connection information
                */
                Console.Write("SQL Server instance (e.g., localhost, SQL2022): ");
                serverName = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(serverName))
                {
                    Console.WriteLine("Error: Server name is required.");
                    WaitForExit();
                    return ExitCodes.InvalidArguments;
                }

                Console.Write("Use Windows Authentication? (Y/N, default Y): ");
                string? authResponse = Console.ReadLine();
                useWindowsAuth = string.IsNullOrWhiteSpace(authResponse) ||
                                       authResponse.Trim().Equals("Y", StringComparison.OrdinalIgnoreCase);

                if (!useWindowsAuth)
                {
                    Console.Write("SQL Server login: ");
                    username = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(username))
                    {
                        Console.WriteLine("Error: Login is required for SQL Server Authentication.");
                        WaitForExit();
                        return ExitCodes.InvalidArguments;
                    }

                    Console.Write("Password: ");
                    password = ReadPassword();
                    Console.WriteLine();

                    if (string.IsNullOrWhiteSpace(password))
                    {
                        Console.WriteLine("Error: Password is required for SQL Server Authentication.");
                        WaitForExit();
                        return ExitCodes.InvalidArguments;
                    }
                }
            }

            /*
            Build connection string
            */
            var builder = new SqlConnectionStringBuilder
            {
                DataSource = serverName,
                InitialCatalog = "master",
                Encrypt = encryptOption,
                TrustServerCertificate = trustCert,
                IntegratedSecurity = useWindowsAuth
            };

            if (!useWindowsAuth)
            {
                builder.UserID = username;
                builder.Password = password;
            }

            /*
            Test connection and get SQL Server version
            */
            string sqlServerVersion = "";
            string sqlServerEdition = "";

            Console.WriteLine();
            Console.WriteLine("Testing connection...");
            try
            {
                using (var connection = new SqlConnection(builder.ConnectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);
                    Console.WriteLine("Connection successful!");

                    /*Capture SQL Server version for summary report*/
                    using (var versionCmd = new SqlCommand("SELECT @@VERSION, SERVERPROPERTY('Edition');", connection))
                    {
                        using (var reader = await versionCmd.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            if (await reader.ReadAsync().ConfigureAwait(false))
                            {
                                sqlServerVersion = reader.GetString(0);
                                sqlServerEdition = reader.GetString(1);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Connection failed: {ex.Message}");
                Console.WriteLine($"Exception type: {ex.GetType().Name}");
                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
                if (!automatedMode)
                {
                    WaitForExit();
                }
                return ExitCodes.ConnectionFailed;
            }

            /*
            Find SQL files to execute (do this once before the installation loop)
            Search current directory and up to 5 parent directories
            Prefer install/ subfolder if it exists (new structure)
            */
            string? sqlDirectory = null;
            string? monitorRootDirectory = null;
            string currentDirectory = Directory.GetCurrentDirectory();
            DirectoryInfo? searchDir = new DirectoryInfo(currentDirectory);

            for (int i = 0; i < 6 && searchDir != null; i++)
            {
                /*Check for install/ subfolder first (new structure)*/
                string installFolder = Path.Combine(searchDir.FullName, "install");
                if (Directory.Exists(installFolder))
                {
                    var installFiles = Directory.GetFiles(installFolder, "*.sql")
                        .Where(f => SqlFileNamePattern.IsMatch(Path.GetFileName(f)))
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
                    .Where(f => SqlFileNamePattern.IsMatch(Path.GetFileName(f)))
                    .ToList();

                if (files.Count > 0)
                {
                    sqlDirectory = searchDir.FullName;
                    monitorRootDirectory = searchDir.FullName;
                    break;
                }

                searchDir = searchDir.Parent;
            }

            if (sqlDirectory == null)
            {
                Console.WriteLine($"Error: No SQL installation files found.");
                Console.WriteLine($"Searched in: {currentDirectory}");
                Console.WriteLine("Expected files in install/ folder or root directory:");
                Console.WriteLine("  install/01_install_database.sql, install/02_create_tables.sql, etc.");
                Console.WriteLine();
                Console.WriteLine("Make sure the installer is in the Monitor directory or a subdirectory.");
                if (!automatedMode)
                {
                    WaitForExit();
                }
                return ExitCodes.SqlFilesNotFound;
            }

            var sqlFiles = Directory.GetFiles(sqlDirectory, "*.sql")
                .Where(f =>
                {
                    string fileName = Path.GetFileName(f);
                    if (!SqlFileNamePattern.IsMatch(fileName))
                        return false;
                    /*Exclude test and troubleshooting scripts from main install*/
                    if (fileName.StartsWith("97_", StringComparison.Ordinal) ||
                        fileName.StartsWith("99_", StringComparison.Ordinal))
                        return false;
                    return true;
                })
                .OrderBy(f => Path.GetFileName(f))
                .ToList();

            Console.WriteLine();
            Console.WriteLine($"Found {sqlFiles.Count} SQL files in: {sqlDirectory}");
            if (monitorRootDirectory != sqlDirectory)
            {
                Console.WriteLine($"Using new folder structure (install/ subfolder)");
            }

            /*
            Main installation loop - allows retry on failure
            */
            int upgradeSuccessCount = 0;
            int upgradeFailureCount = 0;
            int installSuccessCount = 0;
            int installFailureCount = 0;
            int totalSuccessCount = 0;
            int totalFailureCount = 0;
            var installationErrors = new List<(string FileName, string ErrorMessage)>();
            bool installationSuccessful = false;
            bool retry;
            DateTime installationStartTime = DateTime.Now;
            do
            {
                retry = false;
                upgradeSuccessCount = 0;
                upgradeFailureCount = 0;
                installSuccessCount = 0;
                installFailureCount = 0;
                installationErrors.Clear();
                installationSuccessful = false;
                installationStartTime = DateTime.Now;

                /*
                Ask about clean install (automated mode preserves database unless --reinstall flag is used)
                */
                bool dropExisting;
                if (automatedMode)
                {
                    dropExisting = reinstallMode;
                    Console.WriteLine();
                    if (reinstallMode)
                    {
                        Console.WriteLine("Automated mode: Performing clean reinstall (dropping existing database)...");
                    }
                    else
                    {
                        Console.WriteLine("Automated mode: Performing upgrade (preserving existing database)...");
                    }
                }
                else
                {
                    Console.WriteLine();
                    Console.Write("Drop existing PerformanceMonitor database if it exists? (Y/N, default N): ");
                    string? cleanInstall = Console.ReadLine();
                    dropExisting = cleanInstall?.Trim().Equals("Y", StringComparison.OrdinalIgnoreCase) ?? false;
                }

                if (dropExisting)
            {
                Console.WriteLine();
                Console.WriteLine("Performing clean install...");
                try
                {
                    using (var connection = new SqlConnection(builder.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        /*
                        Stop any existing traces before dropping database
                        Traces are server-level and persist after database drops
                        Use existing procedure if database exists
                        */
                        try
                        {
                            using (var command = new SqlCommand("EXECUTE PerformanceMonitor.collect.trace_management_collector @action = 'STOP';", connection))
                            {
                                command.CommandTimeout = ShortTimeoutSeconds;
                                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                                Console.WriteLine("✓ Stopped existing traces");
                            }
                        }
                        catch (SqlException)
                        {
                            /*Database or procedure doesn't exist - no traces to clean*/
                        }

                        string cleanupSql = @"
/*
Remove any existing Agent jobs
*/
USE msdb;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Collection')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Collection';
    PRINT 'Dropped PerformanceMonitor - Collection job';
END;

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Data Retention')
BEGIN
    EXECUTE msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Data Retention';
    PRINT 'Dropped PerformanceMonitor - Data Retention job';
END;

/*
Drop the database
*/
USE master;

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = N'PerformanceMonitor')
BEGIN
    ALTER DATABASE PerformanceMonitor SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE PerformanceMonitor;
    PRINT 'PerformanceMonitor database dropped successfully';
END
ELSE
BEGIN
    PRINT 'PerformanceMonitor database does not exist';
END";

                        using (var command = new SqlCommand(cleanupSql, connection))
                        {
                            command.CommandTimeout = ShortTimeoutSeconds;
                            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                    }

                    Console.WriteLine("✓ Clean install completed (jobs and database removed)");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Could not complete cleanup: {ex.Message}");
                    Console.WriteLine("Continuing with installation...");
                }
            }
            else
            {
                /*
                Upgrade mode - check for existing installation and apply upgrades
                */
                string? currentVersion = null;
                try
                {
                    currentVersion = await GetInstalledVersion(builder.ConnectionString);
                }
                catch (InvalidOperationException ex)
                {
                    Console.WriteLine();
                    Console.WriteLine("================================================================================");
                    Console.WriteLine("ERROR: Failed to check for existing installation");
                    Console.WriteLine("================================================================================");
                    Console.WriteLine(ex.Message);
                    if (ex.InnerException != null)
                    {
                        Console.WriteLine($"Details: {ex.InnerException.Message}");
                    }
                    Console.WriteLine();
                    Console.WriteLine("This may indicate a permissions issue or database corruption.");
                    Console.WriteLine("Please review the error log and report this issue if it persists.");
                    Console.WriteLine();

                    /*Write error log for bug reporting*/
                    string errorLogPath = WriteErrorLog(ex, serverName!, infoVersion);
                    Console.WriteLine($"Error log written to: {errorLogPath}");

                    if (!automatedMode)
                    {
                        WaitForExit();
                    }
                    return ExitCodes.VersionCheckFailed;
                }

                if (currentVersion != null && monitorRootDirectory != null)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Existing installation detected: v{currentVersion}");
                    Console.WriteLine("Checking for applicable upgrades...");

                    var upgrades = GetApplicableUpgrades(monitorRootDirectory, currentVersion, version);

                    if (upgrades.Count > 0)
                    {
                        Console.WriteLine($"Found {upgrades.Count} upgrade(s) to apply.");
                        Console.WriteLine();
                        Console.WriteLine("================================================================================" );
                        Console.WriteLine("Applying upgrades...");
                        Console.WriteLine("================================================================================");

                        using (var connection = new SqlConnection(builder.ConnectionString))
                        {
                            await connection.OpenAsync().ConfigureAwait(false);

                            foreach (var upgradeFolder in upgrades)
                            {
                                var (upgradeSuccess, upgradeFail) = await ExecuteUpgrade(upgradeFolder, connection);
                                upgradeSuccessCount += upgradeSuccess;
                                upgradeFailureCount += upgradeFail;
                            }
                        }

                        Console.WriteLine();
                        Console.WriteLine($"Upgrades complete: {upgradeSuccessCount} succeeded, {upgradeFailureCount} failed");
                    }
                    else
                    {
                        Console.WriteLine("No pending upgrades found.");
                    }
                }
                else
                {
                    Console.WriteLine();
                    Console.WriteLine("No existing installation detected, proceeding with fresh install...");
                }
            }

            /*
            Execute SQL files in order
            */
            Console.WriteLine();
            Console.WriteLine("================================================================================");
            Console.WriteLine("Starting installation...");
            Console.WriteLine("================================================================================");
            Console.WriteLine();

            /*
            Open a single connection for all SQL file execution
            Connection pooling handles the underlying socket reuse
            */
            bool communityDepsInstalled = false;

            using (var connection = new SqlConnection(builder.ConnectionString))
            {
                await connection.OpenAsync().ConfigureAwait(false);

                foreach (var sqlFile in sqlFiles)
                {
                    string fileName = Path.GetFileName(sqlFile);

                    /*Install community dependencies before validation runs
                      Collectors in 98_validate need sp_WhoIsActive, sp_HealthParser, etc.*/
                    if (!communityDepsInstalled &&
                        fileName.StartsWith("98_", StringComparison.Ordinal))
                    {
                        communityDepsInstalled = true;
                        try
                        {
                            await InstallDependenciesAsync(builder.ConnectionString);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Warning: Dependency installation encountered errors: {ex.Message}");
                            Console.WriteLine("Continuing with installation...");
                        }
                    }

                    Console.Write($"Executing {fileName}... ");

                    try
                    {
                        string sqlContent = await File.ReadAllTextAsync(sqlFile);

                        /*
                        Reset schedule to defaults if requested — truncate before the
                        INSERT...WHERE NOT EXISTS re-populates with current recommended values
                        */
                        if (resetSchedule && fileName.StartsWith("04_", StringComparison.Ordinal))
                        {
                            sqlContent = "TRUNCATE TABLE config.collection_schedule;\nGO\n" + sqlContent;
                            Console.Write("(resetting schedule) ");
                        }

                        /*
                        Remove SQLCMD directives (:r includes) as we're executing files directly
                        */
                        sqlContent = SqlCmdDirectivePattern.Replace(sqlContent, "");

                        /*
                        Split by GO statements using pre-compiled regex
                        Match GO only when it's a whole word on its own line
                        */
                        string[] batches = GoBatchPattern.Split(sqlContent);

                        int batchNumber = 0;
                        foreach (string batch in batches)
                        {
                            string trimmedBatch = batch.Trim();

                            /*Skip empty batches*/
                            if (string.IsNullOrWhiteSpace(trimmedBatch))
                                continue;

                            batchNumber++;

                            using (var command = new SqlCommand(trimmedBatch, connection))
                            {
                                command.CommandTimeout = LongTimeoutSeconds;
                                try
                                {
                                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                                }
                                catch (SqlException ex)
                                {
                                    /*Add batch info to error message*/
                                    string batchPreview = trimmedBatch.Length > 500 ?
                                        trimmedBatch.Substring(0, 500) + $"... [truncated, total length: {trimmedBatch.Length}]" :
                                        trimmedBatch;
                                    throw new InvalidOperationException($"Batch {batchNumber} failed:\n{batchPreview}\n\nOriginal error: {ex.Message}", ex);
                                }
                            }
                        }

                        Console.WriteLine("✓ Success");
                        installSuccessCount++;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"✗ FAILED");
                        Console.WriteLine($"  Error: {ex.Message}");
                        installFailureCount++;
                        installationErrors.Add((fileName, ex.Message));

                        if (fileName.StartsWith("01_", StringComparison.Ordinal) || fileName.StartsWith("02_", StringComparison.Ordinal) || fileName.StartsWith("03_", StringComparison.Ordinal))
                        {
                            Console.WriteLine();
                            Console.WriteLine("Critical installation file failed. Aborting installation.");
                            if (!automatedMode)
                            {
                                WaitForExit();
                            }
                            return ExitCodes.CriticalFileFailed;
                        }
                    }
                }
            }

            Console.WriteLine();
            Console.WriteLine("================================================================================");
            Console.WriteLine("File Execution Summary");
            Console.WriteLine("================================================================================");
            if (upgradeSuccessCount > 0 || upgradeFailureCount > 0)
            {
                Console.WriteLine($"Upgrades:     {upgradeSuccessCount} succeeded, {upgradeFailureCount} failed");
            }
            Console.WriteLine($"Installation: {installSuccessCount} succeeded, {installFailureCount} failed");
            Console.WriteLine();

            /*
            Install community dependencies if not already done (no 98_ files in batch)
            */
            if (!communityDepsInstalled && installFailureCount <= 1)
            {
                try
                {
                    await InstallDependenciesAsync(builder.ConnectionString);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Warning: Dependency installation encountered errors: {ex.Message}");
                    Console.WriteLine("Continuing with validation...");
                }
            }

            /*
            Run initial collection and retry failed views
            This validates the installation and creates dynamically-generated tables
            */
            if (installFailureCount <= 1 && automatedMode) /* Allow 1 failure for query_snapshots view */
            {
                Console.WriteLine();
                Console.WriteLine("================================================================================");
                Console.WriteLine("Running initial collection to validate installation...");
                Console.WriteLine("================================================================================");
                Console.WriteLine();

                try
                {
                    using (var connection = new SqlConnection(builder.ConnectionString))
                    {
                        await connection.OpenAsync().ConfigureAwait(false);

                        /*Run master collector once with @force_run_all to collect everything immediately*/
                        Console.Write("Executing master collector... ");
                        using (var command = new SqlCommand("EXECUTE PerformanceMonitor.collect.scheduled_master_collector @force_run_all = 1, @debug = 0;", connection))
                        {
                            command.CommandTimeout = LongTimeoutSeconds;
                            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                        Console.WriteLine("✓ Success");

                        /*
                        Verify data was collected
                        */
                        Console.WriteLine();
                        Console.Write("Verifying data collection... ");

                        /* First check total count in collection_log */
                        int totalLogEntries = 0;
                        using (var command = new SqlCommand(@"
                            SELECT COUNT(*) FROM PerformanceMonitor.config.collection_log;", connection))
                        {
                            totalLogEntries = (int)(await command.ExecuteScalarAsync().ConfigureAwait(false) ?? 0);
                        }

                        /* Check successful collections (all time - we just installed) */
                        int collectedCount = 0;
                        using (var command = new SqlCommand(@"
                            SELECT
                                COUNT(DISTINCT collector_name)
                            FROM PerformanceMonitor.config.collection_log
                            WHERE collection_status = 'SUCCESS';", connection))
                        {
                            collectedCount = (int)(await command.ExecuteScalarAsync().ConfigureAwait(false) ?? 0);
                        }

                        Console.WriteLine($"✓ {collectedCount} collectors ran successfully (total log entries: {totalLogEntries})");

                        /* Show failed collectors if any */
                        int errorCount = 0;
                        using (var command = new SqlCommand(@"
                            SELECT COUNT(*) FROM PerformanceMonitor.config.collection_log WHERE collection_status = 'ERROR';", connection))
                        {
                            errorCount = (int)(await command.ExecuteScalarAsync().ConfigureAwait(false) ?? 0);
                        }

                        if (errorCount > 0)
                        {
                            Console.WriteLine();
                            Console.WriteLine($"⚠ {errorCount} collector(s) encountered errors:");
                            using (var command = new SqlCommand(@"
                                SELECT
                                    collector_name,
                                    error_message
                                FROM PerformanceMonitor.config.collection_log
                                WHERE collection_status = 'ERROR'
                                ORDER BY collection_time DESC;", connection))
                            {
                                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                                {
                                    while (await reader.ReadAsync().ConfigureAwait(false))
                                    {
                                        string name = reader["collector_name"]?.ToString() ?? "";
                                        string error = reader["error_message"] == DBNull.Value ? "(no error message)" : reader["error_message"]?.ToString() ?? "";
                                        Console.WriteLine($"  ✗ {name}");
                                        Console.WriteLine($"    {error}");
                                    }
                                }
                            }
                        }

                        /* Show recent log entries for debugging */
                        if (totalLogEntries > 0 && errorCount == 0)
                        {
                            Console.WriteLine();
                            Console.WriteLine("Sample collection log entries:");
                            using (var command = new SqlCommand(@"
                                SELECT TOP 5
                                    collector_name,
                                    collection_status,
                                    rows_collected,
                                    error_message
                                FROM PerformanceMonitor.config.collection_log
                                WHERE collection_status = 'SUCCESS'
                                ORDER BY collection_time DESC;", connection))
                            {
                                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                                {
                                    while (await reader.ReadAsync().ConfigureAwait(false))
                                    {
                                        string status = reader["collection_status"]?.ToString() ?? "";
                                        string name = reader["collector_name"]?.ToString() ?? "";
                                        int rows = (int)reader["rows_collected"];
                                        string error = reader["error_message"] == DBNull.Value ? "" : $" - {reader["error_message"]}";
                                        Console.WriteLine($"  {status,10}: {name,-35} ({rows,4} rows){error}");
                                    }
                                }
                            }
                        }

                        /*
                        Check if sp_WhoIsActive created query_snapshots table
                        The collector creates daily tables like query_snapshots_20260102
                        */
                        if (installFailureCount > 0)
                        {
                            Console.WriteLine();
                            Console.Write("Checking for query_snapshots table... ");

                            bool tableExists = false;
                            using (var command = new SqlCommand(@"
                                SELECT TOP (1) 1
                                FROM sys.tables AS t
                                WHERE t.name LIKE 'query_snapshots_%'
                                AND t.schema_id = SCHEMA_ID('collect');", connection))
                            {
                                var result = await command.ExecuteScalarAsync().ConfigureAwait(false);
                                tableExists = result != null && result != DBNull.Value;
                            }

                            if (tableExists)
                            {
                                Console.WriteLine("✓ Found");
                                Console.Write("Retrying query plan views... ");

                                try
                                {
                                    string viewFile = Path.Combine(sqlDirectory, "46_create_query_plan_views.sql");
                                    if (File.Exists(viewFile))
                                    {
                                        string sqlContent = await File.ReadAllTextAsync(viewFile);
                                        sqlContent = SqlCmdDirectivePattern.Replace(sqlContent, "");

                                        string[] batches = GoBatchPattern.Split(sqlContent);

                                        foreach (string batch in batches)
                                        {
                                            string trimmedBatch = batch.Trim();
                                            if (string.IsNullOrWhiteSpace(trimmedBatch)) continue;

                                            using (var command = new SqlCommand(trimmedBatch, connection))
                                            {
                                                command.CommandTimeout = ShortTimeoutSeconds;
                                                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                                            }
                                        }

                                        Console.WriteLine("✓ Success");
                                        installFailureCount = 0; /* Reset failure count */
                                    }
                                }
                                catch (SqlException)
                                {
                                    Console.WriteLine("✗ Skipped (sp_WhoIsActive not installed or incompatible schema)");
                                    /*This is expected if sp_WhoIsActive isn't installed - keep installFailureCount = 1 but don't error*/
                                }
                                catch (IOException)
                                {
                                    Console.WriteLine("✗ Skipped (could not read view file)");
                                }
                            }
                            else
                            {
                                Console.WriteLine("✗ Not created (sp_WhoIsActive installation may have failed)");
                                Console.WriteLine();
                                Console.WriteLine("NOTE: The query_snapshots table creation depends on sp_WhoIsActive");
                                Console.WriteLine("      The view will be created automatically on next collection if available");
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ Failed");
                    Console.WriteLine($"Error: {ex.Message}");
                    Console.WriteLine();
                    Console.WriteLine("Installation completed but initial collection failed.");
                    Console.WriteLine("Check PerformanceMonitor.config.collection_log for details.");
                }
            }

            /*
            Installation summary
            Calculate totals and determine success
            Treat query_snapshots view failure as a warning, not an error
            */
            totalSuccessCount = upgradeSuccessCount + installSuccessCount;
            totalFailureCount = upgradeFailureCount + installFailureCount;
            installationSuccessful = (totalFailureCount == 0) || (totalFailureCount == 1 && automatedMode);

            /*
            Log installation history to database
            */
            try
            {
                await LogInstallationHistory(
                    builder.ConnectionString,
                    version,
                    infoVersion,
                    installationStartTime,
                    totalSuccessCount,
                    totalFailureCount,
                    installationSuccessful
                );
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Warning: Could not log installation history: {ex.Message}");
            }

            Console.WriteLine();
            Console.WriteLine("================================================================================");
            Console.WriteLine("Installation Summary");
            Console.WriteLine("================================================================================");

            if (installationSuccessful)
            {
                Console.WriteLine("Installation completed successfully!");
                Console.WriteLine();
                Console.WriteLine("WHAT WAS INSTALLED:");
                Console.WriteLine("✓ PerformanceMonitor database and all collection tables");
                Console.WriteLine("✓ All collector stored procedures");
                Console.WriteLine("✓ Community dependencies (sp_WhoIsActive, DarlingData, First Responder Kit)");
                Console.WriteLine("✓ SQL Agent Job: PerformanceMonitor - Collection (runs every 1 minute)");
                Console.WriteLine("✓ SQL Agent Job: PerformanceMonitor - Data Retention (runs daily at 2:00 AM)");
                Console.WriteLine("✓ Initial collection completed successfully");

                Console.WriteLine();
                Console.WriteLine("NEXT STEPS:");
                Console.WriteLine("1. Ensure SQL Server Agent service is running");
                Console.WriteLine("2. Verify installation: SELECT * FROM PerformanceMonitor.report.collection_health;");
                Console.WriteLine("3. Monitor job history in SQL Server Agent");
                Console.WriteLine();
                Console.WriteLine("See README.md for detailed information.");
            }
            else
            {
                Console.WriteLine($"Installation completed with {totalFailureCount} error(s).");
                Console.WriteLine("Review errors above and check PerformanceMonitor.config.collection_log for details.");
            }

            /*
            Ask if user wants to retry or exit (skip in automated mode)
            */
            if (totalFailureCount > 0 && !automatedMode)
            {
                retry = PromptRetryOrExit();
            }

            } while (retry);

            /*
            Generate installation summary report file
            */
            try
            {
                string reportPath = GenerateSummaryReport(
                    serverName!,
                    sqlServerVersion,
                    sqlServerEdition,
                    infoVersion,
                    installationStartTime,
                    totalSuccessCount,
                    totalFailureCount,
                    installationSuccessful,
                    installationErrors
                );
                Console.WriteLine();
                Console.WriteLine($"Installation report saved to: {reportPath}");
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine($"Warning: Could not generate summary report: {ex.Message}");
            }

            /*
            Exit message for successful completion or user chose not to retry
            */
            if (!automatedMode)
            {
                Console.WriteLine();
                Console.Write("Press any key to exit...");
                Console.ReadKey(true);
                Console.WriteLine();
            }

            return installationSuccessful ? ExitCodes.Success : ExitCodes.PartialInstallation;
        }

        /*
        Ask user if they want to retry or exit
        Returns true to retry, false to exit
        */
        private static bool PromptRetryOrExit()
        {
            Console.WriteLine();
            Console.Write("Y to retry installation, N to exit: ");
            string? response = Console.ReadLine();
            return response?.Trim().Equals("Y", StringComparison.OrdinalIgnoreCase) ?? false;
        }

        /*
        Log installation history to database
        Tracks version, duration, success/failure, and upgrade detection
        */

        /*
        Get currently installed version from database
        Returns null if not installed (database or table doesn't exist)
        Throws exception for unexpected errors (permissions, network, etc.)
        */
        private static async Task<string?> GetInstalledVersion(string connectionString)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    /*Check if PerformanceMonitor database exists*/
                    using var dbCheckCmd = new SqlCommand(@"
                        SELECT database_id
                        FROM sys.databases
                        WHERE name = N'PerformanceMonitor';", connection);

                    var dbExists = await dbCheckCmd.ExecuteScalarAsync().ConfigureAwait(false);
                    if (dbExists == null || dbExists == DBNull.Value)
                    {
                        return null; /*Database doesn't exist - clean install needed*/
                    }

                    /*Check if installation_history table exists*/
                    using var tableCheckCmd = new SqlCommand(@"
                        USE PerformanceMonitor;
                        SELECT OBJECT_ID(N'config.installation_history', N'U');", connection);

                    var tableExists = await tableCheckCmd.ExecuteScalarAsync().ConfigureAwait(false);
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

                    var version = await versionCmd.ExecuteScalarAsync().ConfigureAwait(false);
                    if (version != null && version != DBNull.Value)
                    {
                        return version.ToString();
                    }

                    return null;
                }
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException(
                    $"Failed to check installed version. SQL Error {ex.Number}: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to check installed version: {ex.Message}", ex);
            }
        }

        /*
        Find upgrade folders that need to be applied
        Returns list of upgrade folder paths in order
        Filters by version: only applies upgrades where FromVersion >= currentVersion and ToVersion <= targetVersion
        */
        private static List<string> GetApplicableUpgrades(
            string monitorRootDirectory,
            string? currentVersion,
            string targetVersion)
        {
            var upgradeFolders = new List<string>();
            string upgradesDirectory = Path.Combine(monitorRootDirectory, "upgrades");

            if (!Directory.Exists(upgradesDirectory))
            {
                return upgradeFolders; /*No upgrades folder - return empty list*/
            }

            /*If there's no current version, it's a clean install - no upgrades needed*/
            if (currentVersion == null)
            {
                return upgradeFolders;
            }

            /*Parse current version - if invalid, skip upgrades*/
            if (!Version.TryParse(currentVersion, out var current))
            {
                return upgradeFolders;
            }

            /*Parse target version - if invalid, skip upgrades*/
            if (!Version.TryParse(targetVersion, out var target))
            {
                return upgradeFolders;
            }

            /*
            Find all upgrade folders matching pattern: {from}-to-{to}
            Parse versions and filter to only applicable upgrades
            */
            var applicableUpgrades = Directory.GetDirectories(upgradesDirectory)
                .Select(d => new
                {
                    Path = d,
                    FolderName = Path.GetFileName(d)
                })
                .Where(x => x.FolderName.Contains("-to-", StringComparison.Ordinal))
                .Select(x =>
                {
                    var parts = x.FolderName.Split("-to-");
                    return new
                    {
                        x.Path,
                        FromVersion = Version.TryParse(parts[0], out var from) ? from : null,
                        ToVersion = parts.Length > 1 && Version.TryParse(parts[1], out var to) ? to : null
                    };
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
                    upgradeFolders.Add(upgrade.Path);
                }
            }

            return upgradeFolders;
        }

        /*
        Execute an upgrade folder
        Returns (successCount, failureCount)
        */
        private static async Task<(int successCount, int failureCount)> ExecuteUpgrade(
            string upgradeFolder,
            SqlConnection connection)
        {
            int successCount = 0;
            int failureCount = 0;

            string upgradeName = Path.GetFileName(upgradeFolder);
            string upgradeFile = Path.Combine(upgradeFolder, "upgrade.txt");

            Console.WriteLine();
            Console.WriteLine($"Applying upgrade: {upgradeName}");
            Console.WriteLine("--------------------------------------------------------------------------------");

            /*Read the upgrade.txt file to get ordered list of SQL files*/
            var sqlFileNames = (await File.ReadAllLinesAsync(upgradeFile).ConfigureAwait(false))
                .Where(line => !string.IsNullOrWhiteSpace(line) && !line.TrimStart().StartsWith('#'))
                .Select(line => line.Trim())
                .ToList();

            foreach (var fileName in sqlFileNames)
            {
                string filePath = Path.Combine(upgradeFolder, fileName);

                if (!File.Exists(filePath))
                {
                    Console.WriteLine($"  {fileName}... ? WARNING: File not found");
                    failureCount++;
                    continue;
                }

                Console.Write($"  {fileName}... ");

                try
                {
                    string sql = await File.ReadAllTextAsync(filePath).ConfigureAwait(false);
                    string[] batches = GoBatchPattern.Split(sql);

                    int batchNumber = 0;
                    foreach (var batch in batches)
                    {
                        batchNumber++;
                        string trimmedBatch = batch.Trim();

                        if (string.IsNullOrWhiteSpace(trimmedBatch))
                            continue;

                        using (var cmd = new SqlCommand(trimmedBatch, connection))
                        {
                            cmd.CommandTimeout = LongTimeoutSeconds;
                            try
                            {
                                await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                            }
                            catch (SqlException ex)
                            {
                                /*Add batch info to error message*/
                                string batchPreview = trimmedBatch.Length > 500 ?
                                    trimmedBatch.Substring(0, 500) + $"... [truncated, total length: {trimmedBatch.Length}]" :
                                    trimmedBatch;
                                throw new InvalidOperationException($"Batch {batchNumber} failed:\n{batchPreview}\n\nOriginal error: {ex.Message}", ex);
                            }
                        }
                    }

                    Console.WriteLine("✓ Success");
                    successCount++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ FAILED");
                    Console.WriteLine($"    Error: {ex.Message}");
                    failureCount++;
                }
            }

            return (successCount, failureCount);
        }

        private static async Task LogInstallationHistory(
            string connectionString,
            string assemblyVersion,
            string infoVersion,
            DateTime startTime,
            int filesExecuted,
            int filesFailed,
            bool isSuccess)
        {
            try
            {
                using (var connection = new SqlConnection(connectionString))
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    /*Check if this is an upgrade by checking for existing installation*/
                    string? previousVersion = null;
                    string installationType = "INSTALL";

                    try
                    {
                        using (var checkCmd = new SqlCommand(@"
                            SELECT TOP 1 installer_version
                            FROM PerformanceMonitor.config.installation_history
                            WHERE installation_status = 'SUCCESS'
                            ORDER BY installation_date DESC;", connection))
                        {
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
                    }
                    catch (SqlException)
                    {
                        /*Table might not exist yet on first install - that's ok*/
                    }

                    /*Get SQL Server version info*/
                    string sqlVersion = "";
                    string sqlEdition = "";

                    using (var versionCmd = new SqlCommand("SELECT @@VERSION, SERVERPROPERTY('Edition');", connection))
                    {
                        using (var reader = await versionCmd.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            if (await reader.ReadAsync().ConfigureAwait(false))
                            {
                                sqlVersion = reader.GetString(0);
                                sqlEdition = reader.GetString(1);
                            }
                        }
                    }

                    /*Calculate duration*/
                    long durationMs = (long)(DateTime.Now - startTime).TotalMilliseconds;

                    /*Determine installation status*/
                    string status = isSuccess ? "SUCCESS" : (filesFailed > 0 ? "PARTIAL" : "FAILED");

                    /*Insert installation record*/
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

                    using (var insertCmd = new SqlCommand(insertSql, connection))
                    {
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
                    }
                }
            }
            catch (Exception ex)
            {
                /*Don't fail installation if logging fails*/
                Console.WriteLine($"Warning: Failed to log installation history: {ex.Message}");
            }
        }

        /*
        Write error log file for bug reporting
        Returns the path to the log file
        */
        private static string WriteErrorLog(Exception ex, string serverName, string installerVersion)
        {
            string timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string sanitizedServer = SanitizeFilename(serverName);
            string fileName = $"PerformanceMonitor_Error_{sanitizedServer}_{timestamp}.log";
            string logPath = Path.Combine(Directory.GetCurrentDirectory(), fileName);

            var sb = new System.Text.StringBuilder();

            sb.AppendLine("================================================================================");
            sb.AppendLine("Performance Monitor Installer - Error Log");
            sb.AppendLine("================================================================================");
            sb.AppendLine();
            sb.AppendLine($"Timestamp:         {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"Installer Version: {installerVersion}");
            sb.AppendLine($"Server:            {serverName}");
            sb.AppendLine($"Machine:           {Environment.MachineName}");
            sb.AppendLine($"User:              {Environment.UserName}");
            sb.AppendLine($"OS:                {Environment.OSVersion}");
            sb.AppendLine($".NET Version:      {Environment.Version}");
            sb.AppendLine();
            sb.AppendLine("--------------------------------------------------------------------------------");
            sb.AppendLine("ERROR DETAILS");
            sb.AppendLine("--------------------------------------------------------------------------------");
            sb.AppendLine($"Type:    {ex.GetType().FullName}");
            sb.AppendLine($"Message: {ex.Message}");
            sb.AppendLine();

            if (ex.InnerException != null)
            {
                sb.AppendLine("Inner Exception:");
                sb.AppendLine($"  Type:    {ex.InnerException.GetType().FullName}");
                sb.AppendLine($"  Message: {ex.InnerException.Message}");
                sb.AppendLine();
            }

            sb.AppendLine("Stack Trace:");
            sb.AppendLine(ex.StackTrace ?? "(not available)");
            sb.AppendLine();

            if (ex.InnerException?.StackTrace != null)
            {
                sb.AppendLine("Inner Exception Stack Trace:");
                sb.AppendLine(ex.InnerException.StackTrace);
                sb.AppendLine();
            }

            sb.AppendLine("================================================================================");
            sb.AppendLine("Please include this file when reporting issues at:");
            sb.AppendLine("https://github.com/erikdarlingdata/PerformanceMonitor/issues");
            sb.AppendLine("================================================================================");

            File.WriteAllText(logPath, sb.ToString());

            return logPath;
        }

        /*
        Sanitize a string for use in a filename
        Replaces invalid characters with underscores
        */
        private static string SanitizeFilename(string input)
        {
            var invalid = Path.GetInvalidFileNameChars();
            return string.Concat(input.Select(c => invalid.Contains(c) ? '_' : c));
        }

        /*
        Download content from URL with retry logic for transient failures
        Uses exponential backoff: 2s, 4s, 8s between retries
        */
        private static async Task<string> DownloadWithRetryAsync(
            HttpClient client,
            string url,
            int maxRetries = 3)
        {
            for (int attempt = 1; attempt <= maxRetries; attempt++)
            {
                try
                {
                    return await client.GetStringAsync(url).ConfigureAwait(false);
                }
                catch (HttpRequestException) when (attempt < maxRetries)
                {
                    int delaySeconds = (int)Math.Pow(2, attempt); /*2s, 4s, 8s*/
                    Console.WriteLine($"network error, retrying in {delaySeconds}s ({attempt}/{maxRetries})...");
                    Console.Write($"Installing ... ");
                    await Task.Delay(delaySeconds * 1000).ConfigureAwait(false);
                }
            }
            /*Final attempt - let exception propagate if it fails*/
            return await client.GetStringAsync(url).ConfigureAwait(false);
        }

        /*
        Wait for user input before exiting (prevents window from closing)
        Used for fatal errors where retry doesn't make sense
        */
        private static void WaitForExit()
        {
            Console.WriteLine();
            Console.Write("Press any key to exit...");
            Console.ReadKey(true);
            Console.WriteLine();
        }

        /*
        Read password from console, displaying asterisks
        */
        private static string ReadPassword()
        {
            string password = string.Empty;
            ConsoleKeyInfo key;

            do
            {
                key = Console.ReadKey(true);

                if (key.Key == ConsoleKey.Backspace && password.Length > 0)
                {
                    password = password.Substring(0, password.Length - 1);
                    Console.Write("\b \b");
                }
                else if (key.Key != ConsoleKey.Enter && !char.IsControl(key.KeyChar))
                {
                    password += key.KeyChar;
                    Console.Write("*");
                }
            } while (key.Key != ConsoleKey.Enter);

            return password;
        }

        /*
        Install community dependencies (sp_WhoIsActive, DarlingData, First Responder Kit)
        Downloads and installs latest versions in PerformanceMonitor database
        */
        private static async Task InstallDependenciesAsync(string connectionString)
        {
            Console.WriteLine();
            Console.WriteLine("================================================================================");
            Console.WriteLine("Installing community dependencies...");
            Console.WriteLine("================================================================================");
            Console.WriteLine();

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
                    "sp_HealthParser, sp_HumanEventsBlockViewer, and others by Erik Darling (MIT)"
                ),
                (
                    "First Responder Kit",
                    "https://raw.githubusercontent.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/refs/heads/main/Install-All-Scripts.sql",
                    "sp_BlitzLock and other diagnostic tools by Brent Ozar Unlimited (MIT)"
                )
            };

            using var httpClient = new HttpClient();
            httpClient.Timeout = TimeSpan.FromSeconds(30);

            int successCount = 0;
            int failureCount = 0;

            foreach (var (name, url, description) in dependencies)
            {
                Console.Write($"Installing {name}... ");

                try
                {
                    /*Download the script with retry for transient failures*/
                    string sql = await DownloadWithRetryAsync(httpClient, url).ConfigureAwait(false);

                    if (string.IsNullOrWhiteSpace(sql))
                    {
                        Console.WriteLine("✗ FAILED (empty response)");
                        failureCount++;
                        continue;
                    }

                    /*Execute in PerformanceMonitor database*/
                    using var connection = new SqlConnection(connectionString);
                    await connection.OpenAsync().ConfigureAwait(false);

                    /*Switch to PerformanceMonitor database*/
                    using (var useDbCommand = new SqlCommand("USE PerformanceMonitor;", connection))
                    {
                        await useDbCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    /*
                    Split by GO statements using pre-compiled regex
                    */
                    string[] batches = GoBatchPattern.Split(sql);

                    foreach (string batch in batches)
                    {
                        string trimmedBatch = batch.Trim();

                        if (string.IsNullOrWhiteSpace(trimmedBatch))
                            continue;

                        using var command = new SqlCommand(trimmedBatch, connection);
                        command.CommandTimeout = MediumTimeoutSeconds;
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    Console.WriteLine("✓ Success");
                    Console.WriteLine($"  {description}");
                    successCount++;
                }
                catch (HttpRequestException ex)
                {
                    Console.WriteLine($"✗ Download failed: {ex.Message}");
                    failureCount++;
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"✗ SQL execution failed: {ex.Message}");
                    failureCount++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"✗ Failed: {ex.Message}");
                    failureCount++;
                }
            }

            Console.WriteLine();
            Console.WriteLine($"Dependencies installed: {successCount}/{dependencies.Count}");

            if (failureCount > 0)
            {
                Console.WriteLine($"Note: {failureCount} dependencies failed to install. The system will work but some");
                Console.WriteLine("      collectors may not function optimally. Check network connectivity and try again.");
            }
        }

        /*
        Generate installation summary report file
        Creates a text file with installation details for documentation and troubleshooting
        */
        private static string GenerateSummaryReport(
            string serverName,
            string sqlServerVersion,
            string sqlServerEdition,
            string installerVersion,
            DateTime startTime,
            int filesSucceeded,
            int filesFailed,
            bool overallSuccess,
            List<(string FileName, string ErrorMessage)> errors)
        {
            var endTime = DateTime.Now;
            var duration = endTime - startTime;

            /*
            Generate unique filename with timestamp
            */
            string timestamp = startTime.ToString("yyyyMMdd_HHmmss");
            string fileName = $"PerformanceMonitor_Install_{SanitizeFilename(serverName)}_{timestamp}.txt";
            string reportPath = Path.Combine(Directory.GetCurrentDirectory(), fileName);

            var sb = new System.Text.StringBuilder();

            /*
            Header
            */
            sb.AppendLine("================================================================================");
            sb.AppendLine("Performance Monitor Installation Report");
            sb.AppendLine("================================================================================");
            sb.AppendLine();

            /*
            Installation summary
            */
            sb.AppendLine("INSTALLATION SUMMARY");
            sb.AppendLine("--------------------------------------------------------------------------------");
            sb.AppendLine($"Status:              {(overallSuccess ? "SUCCESS" : "FAILED")}");
            sb.AppendLine($"Start Time:          {startTime:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"End Time:            {endTime:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"Duration:            {duration.TotalSeconds:F1} seconds");
            sb.AppendLine($"Files Executed:      {filesSucceeded}");
            sb.AppendLine($"Files Failed:        {filesFailed}");
            sb.AppendLine();

            /*
            Server information
            */
            sb.AppendLine("SERVER INFORMATION");
            sb.AppendLine("--------------------------------------------------------------------------------");
            sb.AppendLine($"Server Name:         {serverName}");
            sb.AppendLine($"SQL Server Edition:  {sqlServerEdition}");
            sb.AppendLine();

            /*
            Extract version info from @@VERSION (first line only)
            */
            if (!string.IsNullOrEmpty(sqlServerVersion))
            {
                string[] versionLines = sqlServerVersion.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
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

            /*
            Installer information
            */
            sb.AppendLine("INSTALLER INFORMATION");
            sb.AppendLine("--------------------------------------------------------------------------------");
            sb.AppendLine($"Installer Version:   {installerVersion}");
            sb.AppendLine($"Working Directory:   {Directory.GetCurrentDirectory()}");
            sb.AppendLine($"Machine Name:        {Environment.MachineName}");
            sb.AppendLine($"User Name:           {Environment.UserName}");
            sb.AppendLine();

            /*
            Errors section (if any)
            */
            if (errors.Count > 0)
            {
                sb.AppendLine("ERRORS");
                sb.AppendLine("--------------------------------------------------------------------------------");
                foreach (var (file, error) in errors)
                {
                    sb.AppendLine($"File: {file}");
                    /*
                    Truncate very long error messages
                    */
                    string errorMsg = error.Length > 500 ? error.Substring(0, 500) + "..." : error;
                    sb.AppendLine($"Error: {errorMsg}");
                    sb.AppendLine();
                }
            }

            /*
            Footer
            */
            sb.AppendLine("================================================================================");
            sb.AppendLine("Generated by Performance Monitor Installer");
            sb.AppendLine($"Copyright (c) {DateTime.Now.Year} Darling Data, LLC");
            sb.AppendLine("================================================================================");

            /*
            Write file
            */
            File.WriteAllText(reportPath, sb.ToString());

            return reportPath;
        }
    }
}
