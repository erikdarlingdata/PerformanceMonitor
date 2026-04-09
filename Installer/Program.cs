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
using System.Threading.Tasks;
using System.Reflection;
using Installer.Core;
using Installer.Core.Models;

namespace PerformanceMonitorInstaller
{
    class Program
    {
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

            await CheckForInstallerUpdateAsync(version);


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
            if (args.Any(a => a.Equals("--help", StringComparison.OrdinalIgnoreCase)
                              || a.Equals("-h", StringComparison.OrdinalIgnoreCase)))
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  PerformanceMonitorInstaller.exe                                   Interactive mode");
                Console.WriteLine("  PerformanceMonitorInstaller.exe <server> [options]                 Windows Auth");
                Console.WriteLine("  PerformanceMonitorInstaller.exe <server> <username> <password>     SQL Auth");
                Console.WriteLine("  PerformanceMonitorInstaller.exe <server> <username>                SQL Auth (password via env var)");
                Console.WriteLine("  PerformanceMonitorInstaller.exe <server> --entra <email>           Entra ID (MFA)");
                Console.WriteLine();
                Console.WriteLine("Options:");
                Console.WriteLine("  -h, --help           Show this help message");
                Console.WriteLine("  --reinstall          Drop existing database and perform clean install");
                Console.WriteLine("  --uninstall          Remove database, Agent jobs, and XE sessions");
                Console.WriteLine("  --reset-schedule     Reset collection schedule to recommended defaults");
                Console.WriteLine("  --encrypt=<level>    Connection encryption: mandatory (default), optional, strict");
                Console.WriteLine("  --trust-cert         Trust server certificate without validation");
                Console.WriteLine("  --entra <email>      Use Microsoft Entra ID interactive authentication (MFA)");
                Console.WriteLine();
                Console.WriteLine("Environment Variables:");
                Console.WriteLine("  PM_SQL_PASSWORD      SQL Auth password (avoids passing on command line)");
                Console.WriteLine();
                Console.WriteLine("Exit Codes:");
                Console.WriteLine("  0  Success");
                Console.WriteLine("  1  Invalid arguments");
                Console.WriteLine("  2  Connection failed");
                Console.WriteLine("  3  Critical file failed");
                Console.WriteLine("  4  Partial installation (non-critical failures)");
                Console.WriteLine("  5  Version check failed");
                Console.WriteLine("  6  SQL files not found");
                Console.WriteLine("  7  Uninstall failed");
                Console.WriteLine("  8  Upgrade failed");
                return 0;
            }

            bool automatedMode = args.Length > 0;
            bool reinstallMode = args.Any(a => a.Equals("--reinstall", StringComparison.OrdinalIgnoreCase));
            bool uninstallMode = args.Any(a => a.Equals("--uninstall", StringComparison.OrdinalIgnoreCase));
            bool resetSchedule = args.Any(a => a.Equals("--reset-schedule", StringComparison.OrdinalIgnoreCase));
            bool trustCert = args.Any(a => a.Equals("--trust-cert", StringComparison.OrdinalIgnoreCase));
            bool entraMode = args.Any(a => a.Equals("--entra", StringComparison.OrdinalIgnoreCase));

            /*Parse --entra email (the argument following --entra)*/
            string? entraEmail = null;
            if (entraMode)
            {
                int entraIndex = Array.FindIndex(args, a => a.Equals("--entra", StringComparison.OrdinalIgnoreCase));
                if (entraIndex >= 0 && entraIndex + 1 < args.Length && !args[entraIndex + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    entraEmail = args[entraIndex + 1];
                }
            }

            /*Parse encryption option (default: Mandatory)
              Supports both --encrypt=optional and --encrypt optional */
            string encryptionLevel = "Mandatory";
            var encryptEqualsArg = args.FirstOrDefault(a => a.StartsWith("--encrypt=", StringComparison.OrdinalIgnoreCase));
            if (encryptEqualsArg != null)
            {
                string encryptValue = encryptEqualsArg.Substring("--encrypt=".Length).ToLowerInvariant();
                encryptionLevel = encryptValue switch
                {
                    "optional" => "Optional",
                    "strict" => "Strict",
                    _ => "Mandatory"
                };
            }
            else
            {
                int encryptIndex = Array.FindIndex(args, a => a.Equals("--encrypt", StringComparison.OrdinalIgnoreCase));
                if (encryptIndex >= 0 && encryptIndex + 1 < args.Length && !args[encryptIndex + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    encryptionLevel = args[encryptIndex + 1].ToLowerInvariant() switch
                    {
                        "optional" => "Optional",
                        "strict" => "Strict",
                        _ => "Mandatory"
                    };
                }
            }

            /*Filter out all --flags and their trailing values to get positional arguments
              (server, username, password). Flags like --entra <email> and --encrypt <level>
              have a following value that must also be removed.*/
            var filteredArgsList = new List<string>();
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].StartsWith("--", StringComparison.Ordinal))
                {
                    /*Skip flags that take a trailing value (--entra <email>, --encrypt <level>)*/
                    if ((args[i].Equals("--entra", StringComparison.OrdinalIgnoreCase)
                        || args[i].Equals("--encrypt", StringComparison.OrdinalIgnoreCase))
                        && i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                    {
                        i++; /*skip the value too*/
                    }
                    continue;
                }
                filteredArgsList.Add(args[i]);
            }

            var filteredArgs = filteredArgsList.ToArray();
            string? serverName;
            string? username = null;
            string? password = null;
            bool useWindowsAuth;
            bool useEntraAuth = false;

            if (automatedMode)
            {
                /*
                Automated mode with command-line arguments
                */
                serverName = filteredArgs.Length > 0 ? filteredArgs[0] : null;

                if (entraMode)
                {
                    /*Microsoft Entra ID interactive authentication*/
                    useWindowsAuth = false;
                    useEntraAuth = true;
                    username = entraEmail;

                    if (string.IsNullOrWhiteSpace(username))
                    {
                        Console.WriteLine("Error: Email address is required for Entra ID authentication.");
                        Console.WriteLine("Usage: PerformanceMonitorInstaller.exe <server> --entra <email>");
                        return (int)InstallationResultCode.InvalidArguments;
                    }

                    Console.WriteLine($"Server: {serverName}");
                    Console.WriteLine($"Authentication: Microsoft Entra ID ({username})");
                    Console.WriteLine("A browser window will open for interactive authentication...");
                }
                else if (filteredArgs.Length >= 2)
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
                        return (int)InstallationResultCode.InvalidArguments;
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
                    Console.WriteLine("  --encrypt=<level>    Connection encryption: mandatory (default), optional, strict");
                    Console.WriteLine("  --trust-cert         Trust server certificate without validation (default: require valid cert)");
                    return (int)InstallationResultCode.InvalidArguments;
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
                    return (int)InstallationResultCode.InvalidArguments;
                }

                Console.Write("Trust server certificate? (Y/N, default Y): ");
                string? trustResponse = Console.ReadLine()?.Trim();
                trustCert = string.IsNullOrWhiteSpace(trustResponse)
                    || trustResponse.Equals("Y", StringComparison.OrdinalIgnoreCase);

                Console.WriteLine("Encryption level:");
                Console.WriteLine("  [O] Optional (default)");
                Console.WriteLine("  [M] Mandatory");
                Console.WriteLine("  [S] Strict");
                Console.Write("Choice (O/M/S, default O): ");
                string? encryptResponse = Console.ReadLine()?.Trim();
                encryptionLevel = encryptResponse?.ToUpperInvariant() switch
                {
                    "M" => "Mandatory",
                    "S" => "Strict",
                    _ => "Optional"
                };

                Console.WriteLine("Authentication type:");
                Console.WriteLine("  [W] Windows Authentication (default)");
                Console.WriteLine("  [S] SQL Server Authentication");
                Console.WriteLine("  [E] Microsoft Entra ID (interactive MFA)");
                Console.Write("Choice (W/S/E, default W): ");
                string? authResponse = Console.ReadLine()?.Trim();

                if (string.IsNullOrWhiteSpace(authResponse) || authResponse.Equals("W", StringComparison.OrdinalIgnoreCase))
                {
                    useWindowsAuth = true;
                }
                else if (authResponse.Equals("E", StringComparison.OrdinalIgnoreCase))
                {
                    useWindowsAuth = false;
                    useEntraAuth = true;

                    Console.Write("Email address (UPN): ");
                    username = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(username))
                    {
                        Console.WriteLine("Error: Email address is required for Entra ID authentication.");
                        WaitForExit();
                        return (int)InstallationResultCode.InvalidArguments;
                    }

                    Console.WriteLine("A browser window will open for interactive authentication...");
                }
                else
                {
                    useWindowsAuth = false;

                    Console.Write("SQL Server login: ");
                    username = Console.ReadLine();
                    if (string.IsNullOrWhiteSpace(username))
                    {
                        Console.WriteLine("Error: Login is required for SQL Server Authentication.");
                        WaitForExit();
                        return (int)InstallationResultCode.InvalidArguments;
                    }

                    Console.Write("Password: ");
                    password = ReadPassword();
                    Console.WriteLine();

                    if (string.IsNullOrWhiteSpace(password))
                    {
                        Console.WriteLine("Error: Password is required for SQL Server Authentication.");
                        WaitForExit();
                        return (int)InstallationResultCode.InvalidArguments;
                    }
                }
            }

            /*
            Build connection string using Installer.Core
            */
            string connectionString = InstallationService.BuildConnectionString(
                serverName!,
                useWindowsAuth,
                username,
                password,
                encryptionLevel,
                trustCert,
                useEntraAuth);

            /*
            Test connection and get SQL Server version
            */
            string sqlServerVersion = "";
            string sqlServerEdition = "";

            Console.WriteLine();
            Console.WriteLine("Testing connection...");

            var serverInfo = await InstallationService.TestConnectionAsync(connectionString).ConfigureAwait(false);

            if (!serverInfo.IsConnected)
            {
                WriteError($"Connection failed: {serverInfo.ErrorMessage}");
                if (!automatedMode)
                {
                    WaitForExit();
                }
                return (int)InstallationResultCode.ConnectionFailed;
            }

            WriteSuccess("Connection successful!");
            sqlServerVersion = serverInfo.SqlServerVersion;
            sqlServerEdition = serverInfo.SqlServerEdition;

            /*Check minimum SQL Server version -- 2016+ required for on-prem (Standard/Enterprise).
              Azure MI (EngineEdition 8) is always current, skip the check.*/
            if (serverInfo.ProductMajorVersion > 0 && !serverInfo.IsSupportedVersion)
            {
                Console.WriteLine();
                Console.WriteLine($"ERROR: {serverInfo.ProductMajorVersionName} is not supported.");
                Console.WriteLine("Performance Monitor requires SQL Server 2016 (13.x) or later.");
                if (!automatedMode)
                {
                    WaitForExit();
                }
                return (int)InstallationResultCode.VersionCheckFailed;
            }

            /*
            Handle --uninstall mode (no SQL files needed)
            */
            if (uninstallMode)
            {
                return await PerformUninstallAsync(connectionString, automatedMode);
            }

            /*
            Find SQL files using ScriptProvider.FromDirectory()
            Search current directory and up to 5 parent directories
            Prefer install/ subfolder if it exists (new structure)
            */
            ScriptProvider? scriptProvider = null;
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
                        .Where(f => Patterns.SqlFilePattern().IsMatch(Path.GetFileName(f)))
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
                    .Where(f => Patterns.SqlFilePattern().IsMatch(Path.GetFileName(f)))
                    .ToList();

                if (files.Count > 0)
                {
                    sqlDirectory = searchDir.FullName;
                    monitorRootDirectory = searchDir.FullName;
                    break;
                }

                searchDir = searchDir.Parent;
            }

            if (sqlDirectory == null || monitorRootDirectory == null)
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
                return (int)InstallationResultCode.SqlFilesNotFound;
            }

            scriptProvider = ScriptProvider.FromDirectory(monitorRootDirectory);
            var sqlFiles = scriptProvider.GetInstallFiles();

            Console.WriteLine();
            Console.WriteLine($"Found {sqlFiles.Count} SQL files in: {sqlDirectory}");
            if (monitorRootDirectory != sqlDirectory)
            {
                Console.WriteLine($"Using new folder structure (install/ subfolder)");
            }

            /*
            Create progress reporter that routes to console helpers
            */
            var progress = new Progress<InstallationProgress>(p =>
            {
                switch (p.Status)
                {
                    case "Success":
                        WriteSuccess(p.Message);
                        break;
                    case "Error":
                        WriteError(p.Message);
                        break;
                    case "Warning":
                        WriteWarning(p.Message);
                        break;
                    case "Debug":
                        /*Suppress debug messages in CLI output*/
                        break;
                    default:
                        Console.WriteLine(p.Message);
                        break;
                }
            });

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
                    await InstallationService.CleanInstallAsync(connectionString).ConfigureAwait(false);
                    WriteSuccess("Clean install completed (jobs and database removed)");
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
                    currentVersion = await InstallationService.GetInstalledVersionAsync(connectionString).ConfigureAwait(false);
                }
                catch (Exception ex)
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
                    return (int)InstallationResultCode.VersionCheckFailed;
                }

                if (currentVersion != null)
                {
                    Console.WriteLine();
                    Console.WriteLine($"Existing installation detected: v{currentVersion}");
                    Console.WriteLine("Checking for applicable upgrades...");

                    var (upgSuccessCount, upgFailureCount, upgradeCount) =
                        await InstallationService.ExecuteAllUpgradesAsync(
                            scriptProvider,
                            connectionString,
                            currentVersion,
                            version,
                            progress).ConfigureAwait(false);

                    upgradeSuccessCount = upgSuccessCount;
                    upgradeFailureCount = upgFailureCount;

                    if (upgradeCount > 0)
                    {
                        Console.WriteLine();
                        Console.WriteLine($"Upgrades complete: {upgradeSuccessCount} succeeded, {upgradeFailureCount} failed");

                        /*Abort if any upgrade scripts failed -- proceeding would reinstall over a partially-upgraded database*/
                        if (upgradeFailureCount > 0)
                        {
                            Console.WriteLine();
                            Console.WriteLine("================================================================================");
                            WriteError("Installation aborted: upgrade scripts must succeed before installation can proceed.");
                            Console.WriteLine("Fix the errors above and re-run the installer.");
                            Console.WriteLine("================================================================================");
                            if (!automatedMode)
                            {
                                WaitForExit();
                            }
                            return (int)InstallationResultCode.UpgradesFailed;
                        }
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
            Execute installation using Installer.Core
            Use DependencyInstaller for community dependencies before validation
            */
            string communityDir = Path.Combine(monitorRootDirectory, "community");
            using var dependencyInstaller = new DependencyInstaller(communityDir);

            var installResult = await InstallationService.ExecuteInstallationAsync(
                connectionString,
                scriptProvider,
                cleanInstall: false, /* Clean install was already handled above if requested */
                resetSchedule: resetSchedule,
                progress: new Progress<InstallationProgress>(p =>
                {
                    switch (p.Status)
                    {
                        case "Success":
                            if (p.Message.EndsWith(" - Success", StringComparison.Ordinal))
                            {
                                /*File success: replicate the original "Executing <file>... Success" format*/
                                string fileName = p.Message.Replace(" - Success", "", StringComparison.Ordinal);
                                /*The "Executing..." was already printed by the Info message*/
                                WriteSuccess("Success");
                            }
                            else
                            {
                                WriteSuccess(p.Message);
                            }
                            break;
                        case "Error":
                            if (p.Message.Contains(" - FAILED:", StringComparison.Ordinal))
                            {
                                WriteError("FAILED");
                                string errorMsg = p.Message.Substring(p.Message.IndexOf(" - FAILED: ", StringComparison.Ordinal) + 11);
                                Console.WriteLine($"  Error: {errorMsg}");
                            }
                            else if (p.Message == "Critical installation file failed. Aborting installation.")
                            {
                                Console.WriteLine();
                                Console.WriteLine(p.Message);
                            }
                            else
                            {
                                WriteError(p.Message);
                            }
                            break;
                        case "Warning":
                            WriteWarning(p.Message);
                            break;
                        case "Info":
                            if (p.Message.StartsWith("Executing ", StringComparison.Ordinal) && p.Message.EndsWith("...", StringComparison.Ordinal))
                            {
                                /*Replicate "Executing <file>... " format (no newline yet)*/
                                Console.Write(p.Message.Replace("Executing ", "Executing ", StringComparison.Ordinal) + " ");
                            }
                            else if (p.Message == "Resetting schedule to recommended defaults...")
                            {
                                Console.Write("(resetting schedule) ");
                            }
                            else if (p.Message != "Starting installation...")
                            {
                                Console.WriteLine(p.Message);
                            }
                            break;
                        case "Debug":
                            /*Suppress debug messages in CLI output*/
                            break;
                        default:
                            Console.WriteLine(p.Message);
                            break;
                    }
                }),
                preValidationAction: async () =>
                {
                    Console.WriteLine();
                    Console.WriteLine("================================================================================");
                    Console.WriteLine("Installing community dependencies...");
                    Console.WriteLine("================================================================================");
                    Console.WriteLine();

                    try
                    {
                        await dependencyInstaller.InstallDependenciesAsync(
                            connectionString,
                            new Progress<InstallationProgress>(dp =>
                            {
                                switch (dp.Status)
                                {
                                    case "Success":
                                        WriteSuccess(dp.Message);
                                        break;
                                    case "Error":
                                        WriteError(dp.Message);
                                        break;
                                    case "Warning":
                                        WriteWarning(dp.Message);
                                        break;
                                    case "Debug":
                                        break;
                                    default:
                                        Console.WriteLine(dp.Message);
                                        break;
                                }
                            })).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Warning: Dependency installation encountered errors: {ex.Message}");
                        Console.WriteLine("Continuing with installation...");
                    }
                }).ConfigureAwait(false);

            installSuccessCount = installResult.FilesSucceeded;
            installFailureCount = installResult.FilesFailed;
            installationErrors.AddRange(installResult.Errors);

            /*Check for critical file failure*/
            if (installResult.FilesFailed > 0 && installResult.Errors.Any(e => Patterns.IsCriticalFile(e.FileName)))
            {
                if (!automatedMode)
                {
                    WaitForExit();
                }
                return (int)InstallationResultCode.CriticalScriptFailed;
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
                    Console.Write("Executing master collector... ");
                    var (collectorsSucceeded, collectorsFailed) = await InstallationService.RunValidationAsync(
                        connectionString,
                        new Progress<InstallationProgress>(vp =>
                        {
                            /*Suppress most messages; the method writes detailed results*/
                            if (vp.Status == "Error" && !vp.Message.StartsWith("  ", StringComparison.Ordinal))
                            {
                                WriteError(vp.Message);
                            }
                        })).ConfigureAwait(false);

                    WriteSuccess("Success");
                    Console.WriteLine();
                    Console.Write("Verifying data collection... ");
                    Console.WriteLine($"✓ {collectorsSucceeded} collectors ran successfully");

                    if (collectorsFailed > 0)
                    {
                        Console.WriteLine();
                        Console.WriteLine($"⚠ {collectorsFailed} collector(s) encountered errors");
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
            installationSuccessful = totalFailureCount == 0;

            /*
            Log installation history to database
            */
            try
            {
                await InstallationService.LogInstallationHistoryAsync(
                    connectionString,
                    version,
                    infoVersion,
                    installationStartTime,
                    totalSuccessCount,
                    totalFailureCount,
                    installationSuccessful
                ).ConfigureAwait(false);
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
                WriteSuccess("Installation completed successfully!");
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
                WriteWarning($"Installation completed with {totalFailureCount} error(s).");
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
                var summaryResult = new InstallationResult
                {
                    Success = installationSuccessful,
                    FilesSucceeded = totalSuccessCount,
                    FilesFailed = totalFailureCount,
                    StartTime = installationStartTime,
                    EndTime = DateTime.Now
                };
                foreach (var (fileName, errorMessage) in installationErrors)
                {
                    summaryResult.Errors.Add((fileName, errorMessage));
                }

                string reportPath = InstallationService.GenerateSummaryReport(
                    serverName!,
                    sqlServerVersion,
                    sqlServerEdition,
                    infoVersion,
                    summaryResult,
                    outputDirectory: Directory.GetCurrentDirectory());

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

            return installationSuccessful
                ? (int)InstallationResultCode.Success
                : (int)InstallationResultCode.PartialInstallation;
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

        /// <summary>
        /// Performs a complete uninstall: stops traces, removes jobs, XE sessions, and database.
        /// </summary>
        private static async Task<int> PerformUninstallAsync(string connectionString, bool automatedMode)
        {
            Console.WriteLine();
            Console.WriteLine("================================================================================");
            Console.WriteLine("UNINSTALL MODE");
            Console.WriteLine("================================================================================");
            Console.WriteLine();

            if (!automatedMode)
            {
                Console.WriteLine("This will remove:");
                Console.WriteLine("  - SQL Agent jobs (Collection, Data Retention, Hung Job Monitor)");
                Console.WriteLine("  - Extended Events sessions (BlockedProcess, Deadlock)");
                Console.WriteLine("  - Server-side traces");
                Console.WriteLine("  - PerformanceMonitor database and ALL collected data");
                Console.WriteLine();
                Console.Write("Are you sure you want to continue? (Y/N, default N): ");
                string? confirm = Console.ReadLine();
                if (!confirm?.Trim().Equals("Y", StringComparison.OrdinalIgnoreCase) ?? true)
                {
                    Console.WriteLine("Uninstall cancelled.");
                    WaitForExit();
                    return (int)InstallationResultCode.Success;
                }
            }

            Console.WriteLine();
            Console.WriteLine("Uninstalling Performance Monitor...");

            try
            {
                await InstallationService.ExecuteUninstallAsync(
                    connectionString,
                    new Progress<InstallationProgress>(p =>
                    {
                        switch (p.Status)
                        {
                            case "Success":
                                WriteSuccess(p.Message);
                                break;
                            case "Error":
                                WriteError(p.Message);
                                break;
                            case "Warning":
                                WriteWarning(p.Message);
                                break;
                            case "Info":
                                Console.WriteLine(p.Message);
                                break;
                            case "Debug":
                                break;
                            default:
                                Console.WriteLine(p.Message);
                                break;
                        }
                    })).ConfigureAwait(false);

                Console.WriteLine();
                WriteSuccess("Uninstall completed successfully");
                Console.WriteLine();
                Console.WriteLine("Note: blocked process threshold (s) was NOT reset.");
            }
            catch (Exception ex)
            {
                Console.WriteLine();
                Console.WriteLine($"Uninstall failed: {ex.Message}");
                if (!automatedMode)
                {
                    WaitForExit();
                }
                return (int)InstallationResultCode.UninstallFailed;
            }

            if (!automatedMode)
            {
                WaitForExit();
            }
            return (int)InstallationResultCode.Success;
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

        private static void WriteSuccess(string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.Write("√ ");
            Console.ResetColor();
            Console.WriteLine(message);
        }

        private static void WriteError(string message)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.Write("✗ ");
            Console.ResetColor();
            Console.WriteLine(message);
        }

        private static void WriteWarning(string message)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.Write("! ");
            Console.ResetColor();
            Console.WriteLine(message);
        }

        private static async Task CheckForInstallerUpdateAsync(string currentVersion)
        {
            try
            {
                using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(5) };
                client.DefaultRequestHeaders.Add("User-Agent", "PerformanceMonitor");
                client.DefaultRequestHeaders.Add("Accept", "application/vnd.github.v3+json");

                var response = await client.GetAsync(
                    "https://api.github.com/repos/erikdarlingdata/PerformanceMonitor/releases/latest")
                    .ConfigureAwait(false);

                if (!response.IsSuccessStatusCode) return;

                var json = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                using var doc = System.Text.Json.JsonDocument.Parse(json);
                var tagName = doc.RootElement.GetProperty("tag_name").GetString() ?? "";
                var versionString = tagName.TrimStart('v', 'V');

                if (!Version.TryParse(versionString, out var latest)) return;
                if (!Version.TryParse(currentVersion, out var current)) return;

                if (latest > current)
                {
                    Console.WriteLine();
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("╔══════════════════════════════════════════════════════════════════════╗");
                    Console.WriteLine($"║  A newer version ({tagName}) is available!                          ");
                    Console.WriteLine("║  https://github.com/erikdarlingdata/PerformanceMonitor/releases     ");
                    Console.WriteLine("╚══════════════════════════════════════════════════════════════════════╝");
                    Console.ResetColor();
                    Console.WriteLine();
                }
            }
            catch
            {
                /* Best effort — don't block installation if GitHub is unreachable */
            }
        }
    }
}
