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
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows;
using Microsoft.Data.SqlClient;
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
            string appDataPath = Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
                "PerformanceMonitorDashboard"
            );

            Directory.CreateDirectory(appDataPath);
            _configFilePath = Path.Combine(appDataPath, "servers.json");
            _credentialService = new CredentialService();
            _servers = new List<ServerConnection>();
            _connectionStatuses = new ConcurrentDictionary<string, ServerConnectionStatus>();

            LoadServers();
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

            if (!server.UseWindowsAuth && !string.IsNullOrEmpty(username) && password != null)
            {
                if (!_credentialService.SaveCredential(server.Id, username, password))
                {
                    throw new InvalidOperationException("Failed to save credentials to Windows Credential Manager");
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

            if (!server.UseWindowsAuth && !string.IsNullOrEmpty(username) && password != null)
            {
                if (!_credentialService.UpdateCredential(server.Id, username, password))
                {
                    throw new InvalidOperationException("Failed to update credentials in Windows Credential Manager");
                }
            }
            else if (server.UseWindowsAuth)
            {
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

            // Close active connections before dropping
            using var killCmd = new SqlCommand(@"
                IF DB_ID('PerformanceMonitor') IS NOT NULL
                BEGIN
                    ALTER DATABASE [PerformanceMonitor] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                    DROP DATABASE [PerformanceMonitor];
                END", connection);
            killCmd.CommandTimeout = 30;
            await killCmd.ExecuteNonQueryAsync();

            Logger.Info($"Dropped PerformanceMonitor database on '{server.DisplayName}'");
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

        public async Task<ServerConnectionStatus> CheckConnectionAsync(string serverId)
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
                }
            }
            catch (SqlException ex)
            {
                status.IsOnline = false;
                status.ErrorMessage = ex.Message;
                Logger.Warning($"Connectivity check failed for server '{server.DisplayName}': {ex.Message}");
            }
            catch (Exception ex)
            {
                status.IsOnline = false;
                status.ErrorMessage = ex.Message;
                Logger.Warning($"Connectivity check error for server '{server.DisplayName}': {ex.Message}");
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
