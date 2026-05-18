/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using Microsoft.Data.SqlClient;
using Installer.Core;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;

namespace PerformanceMonitorDashboard.Services
{
    public class ServerManager : IServerManager
    {
        private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };
        private readonly object _serversLock = new();
        private readonly string _configFilePath;
        private readonly CredentialService _credentialService;
        private List<ServerConnection> _servers;
        private readonly ConcurrentDictionary<string, ServerConnectionStatus> _connectionStatuses;

        /// <summary>
        /// Timeout in seconds for connectivity checks. Kept short to avoid blocking UI.
        /// </summary>
        private const int ConnectionCheckTimeoutSeconds = 5;

        public ServerManager()
        {
            _configFilePath = ResolveSharedServersJsonPath();
            _credentialService = new CredentialService();
            _servers = new List<ServerConnection>();
            _connectionStatuses = new ConcurrentDictionary<string, ServerConnectionStatus>();

            LoadServers();
        }

        /// <summary>
        /// Resolves the path to the machine-wide servers.json under %ProgramData% so multiple
        /// Windows users on the same machine see the same server list. On first directory
        /// creation, grants Authenticated Users Modify so any user can edit the file.
        /// One-time migrates an existing per-user %APPDATA% servers.json if no shared file
        /// is present yet (the old file is left in place as a backup).
        /// Credentials remain per-user in Windows Credential Manager.
        /// </summary>
        private static string ResolveSharedServersJsonPath()
        {
            string sharedDir = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "PerformanceMonitorDashboard");
            string sharedPath = Path.Combine(sharedDir, "servers.json");

            bool directoryCreated = !Directory.Exists(sharedDir);
            Directory.CreateDirectory(sharedDir);

            if (directoryCreated)
            {
                TryGrantAuthenticatedUsersModify(sharedDir);
            }

            if (!File.Exists(sharedPath))
            {
                string legacyPath = Path.Combine(
                    Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                    "PerformanceMonitorDashboard",
                    "servers.json");

                if (File.Exists(legacyPath))
                {
                    try
                    {
                        File.Copy(legacyPath, sharedPath);
                        Logger.Info($"Migrated servers.json from '{legacyPath}' to '{sharedPath}'. " +
                                    "The old file was left in place as a backup. " +
                                    "Passwords in Windows Credential Manager remain per-user — other users on this machine will need to re-enter SQL passwords for each server.");
                    }
                    catch (Exception ex)
                    {
                        Logger.Warning($"Failed to migrate servers.json from '{legacyPath}': {ex.Message}");
                    }
                }
            }

            return sharedPath;
        }

        private static void TryGrantAuthenticatedUsersModify(string directoryPath)
        {
            try
            {
                var dirInfo = new DirectoryInfo(directoryPath);
                var security = dirInfo.GetAccessControl();
                var authenticatedUsers = new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null);

                security.AddAccessRule(new FileSystemAccessRule(
                    authenticatedUsers,
                    FileSystemRights.Modify | FileSystemRights.Synchronize,
                    InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                    PropagationFlags.None,
                    AccessControlType.Allow));

                dirInfo.SetAccessControl(security);
                Logger.Info($"Granted Authenticated Users Modify on '{directoryPath}' so other Windows users on this machine can edit the shared server list.");
            }
            catch (Exception ex)
            {
                Logger.Warning($"Could not set shared ACL on '{directoryPath}': {ex.Message}. Other Windows users may be unable to edit the server list until permissions are fixed manually.");
            }
        }

        public List<ServerConnection> GetAllServers()
        {
            lock (_serversLock)
            {
                return _servers.OrderByDescending(s => s.IsFavorite)
                              .ThenByDescending(s => s.LastConnected)
                              .ToList();
            }
        }

        public ServerConnection? GetServerById(string id)
        {
            lock (_serversLock)
            {
                return _servers.FirstOrDefault(s => s.Id == id);
            }
        }

        public void AddServer(ServerConnection server, string? username = null, string? password = null)
        {
            lock (_serversLock)
            {
                if (_servers.Any(s => s.Id == server.Id))
                {
                    throw new InvalidOperationException($"Server with ID {server.Id} already exists");
                }

                _servers.Add(server);
                SaveServersInternal();
            }

            if (server.AuthenticationType == AuthenticationTypes.SqlServer && !string.IsNullOrEmpty(username) && password != null)
            {
                // For SQL Server auth, save both username and password
                if (!_credentialService.SaveCredential(server.Id, username, password))
                {
                    throw new InvalidOperationException("Failed to save credentials to Windows Credential Manager");
                }
            }
            else if (server.AuthenticationType == AuthenticationTypes.EntraMFA && !string.IsNullOrEmpty(username))
            {
                // For MFA auth, save username hint only (no password needed)
                if (!_credentialService.SaveCredential(server.Id, username, string.Empty))
                {
                    throw new InvalidOperationException("Failed to save username to Windows Credential Manager");
                }
            }

            // Initialize status as unknown for new server
            _connectionStatuses[server.Id] = new ServerConnectionStatus { ServerId = server.Id };
        }

        public void UpdateServer(ServerConnection server, string? username = null, string? password = null)
        {
            lock (_serversLock)
            {
                var existing = _servers.FirstOrDefault(s => s.Id == server.Id);
                if (existing == null)
                {
                    throw new InvalidOperationException($"Server with ID {server.Id} not found");
                }

                var index = _servers.IndexOf(existing);
                _servers[index] = server;
                SaveServersInternal();
            }

            if (server.AuthenticationType == AuthenticationTypes.SqlServer && !string.IsNullOrEmpty(username) && password != null)
            {
                // For SQL Server auth, update both username and password
                if (!_credentialService.UpdateCredential(server.Id, username, password))
                {
                    throw new InvalidOperationException("Failed to update credentials in Windows Credential Manager");
                }
            }
            else if (server.AuthenticationType == AuthenticationTypes.EntraMFA && !string.IsNullOrEmpty(username))
            {
                // For MFA auth, update username hint only (no password needed)
                if (!_credentialService.UpdateCredential(server.Id, username, string.Empty))
                {
                    throw new InvalidOperationException("Failed to update username in Windows Credential Manager");
                }
            }
            else if (server.AuthenticationType == AuthenticationTypes.Windows)
            {
                // For Windows auth, remove any stored credentials
                _credentialService.DeleteCredential(server.Id);
            }
        }

        public void DeleteServer(string id)
        {
            lock (_serversLock)
            {
                var server = _servers.FirstOrDefault(s => s.Id == id);
                if (server != null)
                {
                    _servers.Remove(server);
                    SaveServersInternal();

                    _credentialService.DeleteCredential(id);

                    // Remove status tracking for deleted server
                    _connectionStatuses.TryRemove(id, out _);
                }
            }
        }

        public async Task DropMonitorDatabaseAsync(ServerConnection server)
        {
            var connectionString = server.GetConnectionString(_credentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "master",
                ConnectTimeout = 10
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            // Remove SQL Agent jobs before dropping the database
            using var jobCmd = new SqlCommand(@"
                IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Collection')
                    EXEC msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Collection', @delete_unused_schedule = 1;
                IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Data Retention')
                    EXEC msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Data Retention', @delete_unused_schedule = 1;
                IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'PerformanceMonitor - Hung Job Monitor')
                    EXEC msdb.dbo.sp_delete_job @job_name = N'PerformanceMonitor - Hung Job Monitor', @delete_unused_schedule = 1;", connection);
            jobCmd.CommandTimeout = 30;
            await jobCmd.ExecuteNonQueryAsync();

            // Close active connections before dropping
            using var killCmd = new SqlCommand(@"
                IF DB_ID('PerformanceMonitor') IS NOT NULL
                BEGIN
                    ALTER DATABASE [PerformanceMonitor] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [PerformanceMonitor];
                END", connection);
            killCmd.CommandTimeout = 30;
            await killCmd.ExecuteNonQueryAsync();

            Logger.Info($"Dropped PerformanceMonitor database and Agent jobs on '{server.DisplayName}'");
        }

        public class PurgeResult
        {
            public int RowsDeleted { get; set; }
            public int TableCount { get; set; }
            public int DurationMs { get; set; }
            public string Status { get; set; } = "";
            public string? Message { get; set; }
        }

        /// <summary>
        /// Runs config.data_retention against the PerformanceMonitor database on the given server.
        /// </summary>
        /// <param name="retentionDaysOverride">
        /// null = use per-collector retention from config.collection_schedule.
        /// 0 = TRUNCATE every collect.* table.
        /// N > 0 = override every table's cutoff to N days.
        /// </param>
        public async Task<PurgeResult> RunDataRetentionAsync(
            ServerConnection server,
            int? retentionDaysOverride)
        {
            var connectionString = server.GetConnectionString(_credentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "PerformanceMonitor",
                ConnectTimeout = 10
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            using (var cmd = new SqlCommand("config.data_retention", connection))
            {
                cmd.CommandType = System.Data.CommandType.StoredProcedure;
                cmd.CommandTimeout = 600;

                if (retentionDaysOverride.HasValue)
                {
                    cmd.Parameters.Add(new SqlParameter("@retention_days", System.Data.SqlDbType.Int) { Value = retentionDaysOverride.Value });
                }

                await cmd.ExecuteNonQueryAsync();
            }

            using var readCmd = new SqlCommand(@"
SELECT TOP (1)
    cl.collection_status,
    cl.rows_collected,
    cl.duration_ms,
    cl.error_message
FROM config.collection_log AS cl
WHERE cl.collector_name = N'data_retention'
ORDER BY cl.collection_time DESC;", connection);
            readCmd.CommandTimeout = 30;

            using var reader = await readCmd.ExecuteReaderAsync();
            var result = new PurgeResult();

            if (await reader.ReadAsync())
            {
                result.Status = reader.IsDBNull(0) ? "" : reader.GetString(0);
                result.RowsDeleted = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
                result.DurationMs = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
                result.Message = reader.IsDBNull(3) ? null : reader.GetString(3);

                if (result.Message is not null && result.Message.StartsWith("Cleaned ", StringComparison.Ordinal))
                {
                    int spaceIdx = result.Message.IndexOf(' ', 8);
                    if (spaceIdx > 8 && int.TryParse(result.Message.AsSpan(8, spaceIdx - 8), out int tableCount))
                    {
                        result.TableCount = tableCount;
                    }
                }
                else if (result.Message is not null && result.Message.StartsWith("TRUNCATE all: ", StringComparison.Ordinal))
                {
                    int spaceIdx = result.Message.IndexOf(' ', 14);
                    if (spaceIdx > 14 && int.TryParse(result.Message.AsSpan(14, spaceIdx - 14), out int tableCount))
                    {
                        result.TableCount = tableCount;
                    }
                }
            }

            Logger.Info($"Ran data_retention on '{server.DisplayName}': status={result.Status}, rowsDeleted={result.RowsDeleted}, tables={result.TableCount}, durationMs={result.DurationMs}");

            return result;
        }

        /// <summary>
        /// Returns user database names (excluding system DBs and PerformanceMonitor) on the target server,
        /// for use in the Excluded Databases dialog.
        /// </summary>
        public async Task<List<string>> GetUserDatabasesAsync(ServerConnection server)
        {
            var connectionString = server.GetConnectionString(_credentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "master",
                ConnectTimeout = 10
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            using var cmd = new SqlCommand(@"
SELECT d.name
FROM sys.databases AS d
WHERE d.database_id > 4
AND   d.state_desc = N'ONLINE'
AND   d.name <> N'PerformanceMonitor'
AND   d.database_id < 32761 /*exclude contained AG system databases*/
ORDER BY d.name;", connection);
            cmd.CommandTimeout = 30;

            var names = new List<string>();
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                names.Add(reader.GetString(0));
            }
            return names;
        }

        /// <summary>
        /// Returns the current per-database exclusion list from config.collector_database_exclusions on the target.
        /// </summary>
        public async Task<List<string>> GetCollectorDatabaseExclusionsAsync(ServerConnection server)
        {
            var connectionString = server.GetConnectionString(_credentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "PerformanceMonitor",
                ConnectTimeout = 10
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            using var cmd = new SqlCommand(@"
SELECT e.database_name
FROM config.collector_database_exclusions AS e
ORDER BY e.database_name;", connection);
            cmd.CommandTimeout = 30;

            var names = new List<string>();
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                names.Add(reader.GetString(0));
            }
            return names;
        }

        /// <summary>
        /// Replaces the contents of config.collector_database_exclusions with the supplied list, transactionally.
        /// </summary>
        public async Task SaveCollectorDatabaseExclusionsAsync(ServerConnection server, IEnumerable<string> databaseNames)
        {
            var connectionString = server.GetConnectionString(_credentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "PerformanceMonitor",
                ConnectTimeout = 10
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            using var transaction = connection.BeginTransaction();
            try
            {
                using (var deleteCmd = new SqlCommand("DELETE FROM config.collector_database_exclusions;", connection, transaction))
                {
                    deleteCmd.CommandTimeout = 30;
                    await deleteCmd.ExecuteNonQueryAsync();
                }

                foreach (var name in databaseNames.Distinct(StringComparer.OrdinalIgnoreCase))
                {
                    using var insertCmd = new SqlCommand(
                        "INSERT INTO config.collector_database_exclusions (database_name) VALUES (@name);",
                        connection, transaction);
                    insertCmd.CommandTimeout = 30;
                    insertCmd.Parameters.Add(new SqlParameter("@name", System.Data.SqlDbType.NVarChar, 128) { Value = name });
                    await insertCmd.ExecuteNonQueryAsync();
                }

                transaction.Commit();
                Logger.Info($"Saved collector database exclusions on '{server.DisplayName}'");
            }
            catch
            {
                transaction.Rollback();
                throw;
            }
        }

        public void UpdateLastConnected(string id)
        {
            lock (_serversLock)
            {
                var server = _servers.FirstOrDefault(s => s.Id == id);
                if (server != null)
                {
                    server.LastConnected = DateTime.Now;
                    SaveServersInternal();
                }
            }
        }

        public ServerConnectionStatus GetConnectionStatus(string serverId)
        {
            if (_connectionStatuses.TryGetValue(serverId, out var status))
            {
                return status;
            }

            // Return a new status indicating not yet checked
            var newStatus = new ServerConnectionStatus { ServerId = serverId };
            _connectionStatuses[serverId] = newStatus;
            return newStatus;
        }

        public async Task<ServerConnectionStatus> CheckConnectionAsync(string serverId, bool allowInteractiveAuth = false)
        {
            var server = GetServerById(serverId);
            if (server == null)
            {
                return new ServerConnectionStatus
                {
                    ServerId = serverId,
                    IsOnline = false,
                    LastChecked = DateTime.Now,
                    StatusChangedAt = DateTime.Now,
                    ErrorMessage = "Server not found"
                };
            }

            // Get previous status to detect status changes
            var previousStatus = GetConnectionStatus(serverId);

            // Skip interactive authentication methods during background checks
            if (!allowInteractiveAuth && server.AuthenticationType == AuthenticationTypes.EntraMFA)
            {
                var errorMsg = previousStatus.UserCancelledMfa
                    ? "Authentication cancelled by user"
                    : "Skipped — requires interactive authentication";

                return new ServerConnectionStatus
                {
                    ServerId = serverId,
                    IsOnline = previousStatus.UserCancelledMfa ? false : previousStatus.IsOnline,
                    LastChecked = DateTime.Now,
                    StatusChangedAt = previousStatus.StatusChangedAt,
                    ErrorMessage = errorMsg,
                    PreviousIsOnline = previousStatus.IsOnline,
                    UserCancelledMfa = previousStatus.UserCancelledMfa
                };
            }

            var status = new ServerConnectionStatus
            {
                ServerId = serverId,
                LastChecked = DateTime.Now,
                PreviousIsOnline = previousStatus.IsOnline
            };

            try
            {
                var connectionString = server.GetConnectionString(_credentialService);

                // Modify connection string to use short timeout for connectivity check
                var builder = new SqlConnectionStringBuilder(connectionString)
                {
                    ConnectTimeout = ConnectionCheckTimeoutSeconds
                };

                using var connection = new SqlConnection(builder.ConnectionString);
                await connection.OpenAsync();

                // Query server start time and UTC offset to verify connectivity
                using var command = new SqlCommand(@"
                    SELECT
                        sqlserver_start_time,
                        DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) AS utc_offset_minutes,
                        CONVERT(integer, SERVERPROPERTY('EngineEdition')) AS engine_edition,
                        CASE WHEN DB_ID('rdsadmin') IS NOT NULL THEN 1 ELSE 0 END AS is_aws_rds
                    FROM sys.dm_os_sys_info", connection);
                command.CommandTimeout = ConnectionCheckTimeoutSeconds;

                using var reader = await command.ExecuteReaderAsync();
                status.IsOnline = true;
                status.ErrorMessage = null;
                if (await reader.ReadAsync())
                {
                    if (!reader.IsDBNull(0))
                    {
                        status.ServerStartTime = reader.GetDateTime(0);
                    }
                    if (!reader.IsDBNull(1))
                    {
                        status.UtcOffsetMinutes = Convert.ToInt32(reader.GetValue(1));
                    }
                    if (!reader.IsDBNull(2))
                    {
                        status.SqlEngineEdition = Convert.ToInt32(reader.GetValue(2));
                    }
                    if (!reader.IsDBNull(3))
                    {
                        status.IsAwsRds = Convert.ToInt32(reader.GetValue(3)) == 1;
                    }
                }

                /* Azure SQL DB not supported for Full Dashboard (no full system catalog access)
                   Engine Edition 5 = Azure SQL Database, 8 = Azure SQL Managed Instance (supported) */
                if (status.SqlEngineEdition == 5)
                {
                    status.IsOnline = false;
                    status.ErrorMessage = "Azure SQL Database is not supported by the Full Dashboard edition. Use the Lite edition for Azure SQL DB monitoring.";
                    Logger.Warning($"Server '{server.DisplayName}' is Azure SQL Database (EngineEdition=5) — not supported");
                }
                else
                {
                    Logger.Info($"Connectivity check passed for server '{server.DisplayName}'");
                    status.UserCancelledMfa = false; // Clear any previous cancellation flag

                    /* Query installed PerformanceMonitor version */
                    try
                    {
                        using var versionCmd = new SqlCommand(@"
                            IF DB_ID(N'PerformanceMonitor') IS NOT NULL
                            AND EXISTS (
                                SELECT 1
                                FROM PerformanceMonitor.sys.tables AS t
                                JOIN PerformanceMonitor.sys.schemas AS s
                                    ON t.schema_id = s.schema_id
                                WHERE s.name = N'config'
                                AND   t.name = N'installation_history'
                            )
                            BEGIN
                                SELECT TOP (1)
                                    installer_version
                                FROM PerformanceMonitor.config.installation_history
                                WHERE installation_status = N'SUCCESS'
                                ORDER BY installation_date DESC;
                            END;", connection);
                        versionCmd.CommandTimeout = ConnectionCheckTimeoutSeconds;
                        var versionResult = await versionCmd.ExecuteScalarAsync();
                        status.InstalledMonitorVersion = versionResult is string v ? v : null;
                    }
                    catch (SqlException)
                    {
                        /* Non-critical — don't fail the connectivity check */
                        status.InstalledMonitorVersion = null;
                    }
                }
            }
            catch (SqlException ex)
            {
                status.IsOnline = false;
                status.ErrorMessage = ex.Message;

                if (server.AuthenticationType == AuthenticationTypes.EntraMFA && MfaAuthenticationHelper.IsMfaCancelledException(ex))
                {
                    status.UserCancelledMfa = true;
                    status.ErrorMessage = "Authentication cancelled by user";
                    Logger.Info($"MFA authentication cancelled by user for server '{server.DisplayName}'");
                }
                else
                {
                    Logger.Warning($"Connectivity check failed for server '{server.DisplayName}': {ex.Message}");
                }
            }
            catch (Exception ex)
            {
                status.IsOnline = false;
                status.ErrorMessage = ex.Message;

                if (server.AuthenticationType == AuthenticationTypes.EntraMFA && MfaAuthenticationHelper.IsMfaCancelledException(ex))
                {
                    status.UserCancelledMfa = true;
                    status.ErrorMessage = "Authentication cancelled by user";
                    Logger.Info($"MFA authentication cancelled by user for server '{server.DisplayName}'");
                }
                else
                {
                    Logger.Warning($"Connectivity check error for server '{server.DisplayName}': {ex.Message}");
                }
            }

            // Track when status changed (online to offline or vice versa)
            if (previousStatus.IsOnline != status.IsOnline)
            {
                // Status changed - record the change time
                status.StatusChangedAt = DateTime.Now;
            }
            else
            {
                // Status unchanged - preserve the previous change time
                status.StatusChangedAt = previousStatus.StatusChangedAt;
            }

            // Update the cached status
            _connectionStatuses[serverId] = status;

            return status;
        }

        public async Task CheckAllConnectionsAsync()
        {
            var servers = GetAllServers();
            var tasks = servers.Select(s => CheckConnectionAsync(s.Id));
            await Task.WhenAll(tasks);
        }

        public async Task<string?> GetInstalledVersionAsync(ServerConnection server)
        {
            var connectionString = server.GetConnectionString(_credentialService);
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                InitialCatalog = "master",
                ConnectTimeout = ConnectionCheckTimeoutSeconds
            };
            return await InstallationService.GetInstalledVersionAsync(builder.ConnectionString);
        }

        private void LoadServers()
        {
            if (!File.Exists(_configFilePath))
            {
                lock (_serversLock)
                {
                    CreateDefaultServers();
                    SaveServersInternal();
                }
                return;
            }

            try
            {
                string json = File.ReadAllText(_configFilePath);
                lock (_serversLock)
                {
                    _servers = JsonSerializer.Deserialize<List<ServerConnection>>(json)
                              ?? new List<ServerConnection>();

                    // Initialize status tracking for all loaded servers
                    foreach (var server in _servers)
                    {
                        _connectionStatuses[server.Id] = new ServerConnectionStatus { ServerId = server.Id };
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to load servers.json: {ex.Message}. Resetting to defaults.");

                MessageBox.Show(
                    $"Failed to load saved servers: {ex.Message}\n\nThe server list has been reset to defaults.",
                    "Configuration Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);

                lock (_serversLock)
                {
                    _servers = new List<ServerConnection>();
                    CreateDefaultServers();
                    SaveServersInternal();
                }
            }
        }

        // Internal method called when lock is already held
        private void SaveServersInternal()
        {
            string json = JsonSerializer.Serialize(_servers, s_jsonOptions);
            File.WriteAllText(_configFilePath, json);
        }

        private void CreateDefaultServers()
        {
            // No default servers - users must add their own
        }
    }
}
