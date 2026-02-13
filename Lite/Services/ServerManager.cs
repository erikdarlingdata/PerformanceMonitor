/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
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
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Services;

public class ServerManager
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };
    private readonly string _configFilePath;
    private readonly CredentialService _credentialService;
    private readonly ILogger<ServerManager>? _logger;
    private List<ServerConnection> _servers;
    private readonly object _serversLock = new();
    private readonly ConcurrentDictionary<string, ServerConnectionStatus> _connectionStatuses;

    /// <summary>
    /// Timeout in seconds for connectivity checks. Kept short to avoid blocking UI.
    /// </summary>
    private const int ConnectionCheckTimeoutSeconds = 5;

    public ServerManager(string configDirectory, ILogger<ServerManager>? logger = null)
    {
        _configFilePath = Path.Combine(configDirectory, "servers.json");
        _credentialService = new CredentialService();
        _logger = logger;
        _servers = new List<ServerConnection>();
        _connectionStatuses = new ConcurrentDictionary<string, ServerConnectionStatus>();

        LoadServers();
    }

    /// <summary>
    /// Gets the credential service instance.
    /// </summary>
    public CredentialService CredentialService => _credentialService;

    /// <summary>
    /// Gets all servers sorted by favorite status and last connected time.
    /// </summary>
    public List<ServerConnection> GetAllServers()
    {
        lock (_serversLock)
        {
            return _servers.OrderByDescending(s => s.IsFavorite)
                          .ThenByDescending(s => s.LastConnected)
                          .ToList();
        }
    }

    /// <summary>
    /// Gets only enabled servers for data collection.
    /// </summary>
    public List<ServerConnection> GetEnabledServers()
    {
        lock (_serversLock)
        {
            return _servers.Where(s => s.IsEnabled)
                          .OrderByDescending(s => s.IsFavorite)
                          .ThenByDescending(s => s.LastConnected)
                          .ToList();
        }
    }

    /// <summary>
    /// Gets a server by its ID.
    /// </summary>
    public ServerConnection? GetServerById(string id)
    {
        lock (_serversLock)
        {
            return _servers.FirstOrDefault(s => s.Id == id);
        }
    }

    /// <summary>
    /// Adds a new server to the list.
    /// </summary>
    public void AddServer(ServerConnection server, string? username = null, string? password = null)
    {
        lock (_serversLock)
        {
            if (_servers.Any(s => s.Id == server.Id))
            {
                throw new InvalidOperationException($"Server with ID {server.Id} already exists");
            }

            _servers.Add(server);
            SaveServers();
        }

        // Save credentials based on authentication type
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
            // For MFA auth, save username (password can be empty)
            if (!_credentialService.SaveCredential(server.Id, username, string.Empty))
            {
                throw new InvalidOperationException("Failed to save username to Windows Credential Manager");
            }
        }

        // Initialize status as unknown for new server
        _connectionStatuses[server.Id] = new ServerConnectionStatus { ServerId = server.Id };

        _logger?.LogInformation("Added server '{DisplayName}' ({ServerName})", server.DisplayName, server.ServerName);
    }

    /// <summary>
    /// Updates an existing server.
    /// </summary>
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
            SaveServers();
        }

        // Update credentials based on authentication type
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
            // For MFA auth, update username (password can be empty)
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

        _logger?.LogInformation("Updated server '{DisplayName}' ({ServerName})", server.DisplayName, server.ServerName);
    }

    /// <summary>
    /// Deletes a server by its ID.
    /// </summary>
    public void DeleteServer(string id)
    {
        ServerConnection? server;
        lock (_serversLock)
        {
            server = _servers.FirstOrDefault(s => s.Id == id);
            if (server != null)
            {
                _servers.Remove(server);
                SaveServers();
            }
        }

        if (server != null)
        {
            _credentialService.DeleteCredential(id);
            _connectionStatuses.TryRemove(id, out _);
            _logger?.LogInformation("Deleted server '{DisplayName}' ({ServerName})", server.DisplayName, server.ServerName);
        }
    }

    /// <summary>
    /// Updates the last connected timestamp for a server.
    /// </summary>
    public void UpdateLastConnected(string id)
    {
        lock (_serversLock)
        {
            var server = _servers.FirstOrDefault(s => s.Id == id);
            if (server != null)
            {
                server.LastConnected = DateTime.Now;
                SaveServers();
            }
        }
    }

    /// <summary>
    /// Toggles the favorite status for a server.
    /// </summary>
    public void ToggleFavorite(string id)
    {
        lock (_serversLock)
        {
            var server = _servers.FirstOrDefault(s => s.Id == id);
            if (server != null)
            {
                server.IsFavorite = !server.IsFavorite;
                SaveServers();
            }
        }
    }

    /// <summary>
    /// Gets the current connection status for a server.
    /// </summary>
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

    /// <summary>
    /// Checks the connection status of a server.
    /// </summary>
    /// <param name="serverId">The server ID to check.</param>
    /// <param name="allowInteractiveAuth">Whether to allow interactive authentication (e.g., MFA). Set to false for background checks.</param>
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
            // Determine appropriate message based on whether user cancelled
            var errorMsg = previousStatus.UserCancelledMfa 
                ? "Authentication cancelled by user" 
                : "Skipped - requires interactive authentication";
            
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

        // Clear cancellation flag when user explicitly tries to connect (allowInteractiveAuth = true)
        // This gives them a fresh attempt at authentication
        if (allowInteractiveAuth && previousStatus.UserCancelledMfa)
        {
            _logger?.LogDebug("Clearing MFA cancellation flag for server '{DisplayName}' - user is retrying", server.DisplayName);
        }

        // CRITICAL: Prevent connection checks while Add/Edit dialog is open
        // This prevents MFA popups when user is just configuring the server
        if (Windows.AddServerDialog.IsDialogOpen && server.AuthenticationType == AuthenticationTypes.EntraMFA)
        {
            return new ServerConnectionStatus
            {
                ServerId = serverId,
                IsOnline = previousStatus.IsOnline,
                LastChecked = DateTime.Now,
                StatusChangedAt = previousStatus.StatusChangedAt,
                ErrorMessage = "Skipped - dialog open",
                PreviousIsOnline = previousStatus.IsOnline
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

            // Query server start time, version, and UTC offset to verify connectivity
            using var command = new SqlCommand(@"
                SELECT
                    sqlserver_start_time,
                    @@VERSION AS sql_version,
                    CONVERT(integer, SERVERPROPERTY('ProductMajorVersion')) AS major_version,
                    DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) AS utc_offset_minutes,
                    CONVERT(integer, SERVERPROPERTY('EngineEdition')) AS engine_edition,
                    CASE WHEN DB_ID('rdsadmin') IS NOT NULL THEN 1 ELSE 0 END AS is_aws_rds
                FROM sys.dm_os_sys_info", connection);
            command.CommandTimeout = ConnectionCheckTimeoutSeconds;

            using var reader = await command.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                status.IsOnline = true;
                status.ErrorMessage = null;
                status.UserCancelledMfa = false; // Clear cancellation flag on successful connection

                if (!reader.IsDBNull(0))
                {
                    status.ServerStartTime = reader.GetDateTime(0);
                }
                if (!reader.IsDBNull(1))
                {
                    status.SqlServerVersion = reader.GetString(1);
                }
                if (!reader.IsDBNull(2))
                {
                    status.SqlMajorVersion = Convert.ToInt32(reader.GetValue(2));
                }
                if (!reader.IsDBNull(3))
                {
                    status.UtcOffsetMinutes = Convert.ToInt32(reader.GetValue(3));
                }
                if (!reader.IsDBNull(4))
                {
                    status.SqlEngineEdition = Convert.ToInt32(reader.GetValue(4));
                }
                if (!reader.IsDBNull(5))
                {
                    status.IsAwsRds = Convert.ToInt32(reader.GetValue(5)) == 1;
                }
            }

            _logger?.LogDebug("Connectivity check passed for server '{DisplayName}'", server.DisplayName);
        }
        catch (SqlException ex)
        {
            status.IsOnline = false;
            status.ErrorMessage = ex.Message;
            
            // Detect MFA cancellation (error code 0 with specific message patterns)
            if (server.AuthenticationType == AuthenticationTypes.EntraMFA && MfaAuthenticationHelper.IsMfaCancelledException(ex))
            {
                status.UserCancelledMfa = true;
                status.ErrorMessage = "Authentication cancelled by user";
                _logger?.LogInformation("MFA authentication cancelled by user for server '{DisplayName}'", server.DisplayName);
            }
            else
            {
                _logger?.LogWarning("Connectivity check failed for server '{DisplayName}': {Message}", server.DisplayName, ex.Message);
            }
        }
        catch (Exception ex)
        {
            status.IsOnline = false;
            status.ErrorMessage = ex.Message;
            
            // Detect MFA cancellation from generic exceptions
            if (server.AuthenticationType == AuthenticationTypes.EntraMFA && MfaAuthenticationHelper.IsMfaCancelledException(ex))
            {
                status.UserCancelledMfa = true;
                status.ErrorMessage = "Authentication cancelled by user";
                _logger?.LogInformation("MFA authentication cancelled by user for server '{DisplayName}'", server.DisplayName);
            }
            else
            {
                _logger?.LogWarning(ex, "Connectivity check error for server '{DisplayName}'", server.DisplayName);
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

    /// <summary>
    /// Checks the connection status of all servers.
    /// Background operation - will skip servers requiring interactive authentication (e.g., MFA).
    /// </summary>
    public async Task CheckAllConnectionsAsync()
    {
        var servers = GetAllServers();
        // Explicitly pass allowInteractiveAuth: false to prevent MFA popups during background checks
        var tasks = servers.Select(s => CheckConnectionAsync(s.Id, allowInteractiveAuth: false));
        await Task.WhenAll(tasks);
    }

    /// <summary>
    /// Loads servers from the JSON config file.
    /// </summary>
    private void LoadServers()
    {
        if (!File.Exists(_configFilePath))
        {
            _servers = new List<ServerConnection>();
            SaveServers();
            return;
        }

        try
        {
            string json = File.ReadAllText(_configFilePath);
            var config = JsonSerializer.Deserialize<ServersConfig>(json);
            _servers = config?.Servers ?? new List<ServerConnection>();

            /* Create backup of valid config */
            try { File.Copy(_configFilePath, _configFilePath + ".bak", overwrite: true); }
            catch { /* best effort */ }

            // MIGRATION: Backward compatibility for existing servers.json files
            // Old configs only had UseWindowsAuth, new code uses AuthenticationType
            MigrateServerAuthentication(_servers);

            // Initialize status tracking for all loaded servers
            foreach (var server in _servers)
            {
                _connectionStatuses[server.Id] = new ServerConnectionStatus { ServerId = server.Id };
            }

            _logger?.LogInformation("Loaded {Count} servers from configuration", _servers.Count);
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Failed to load servers.json, attempting backup restore");

            /* Try to restore from backup */
            var bakPath = _configFilePath + ".bak";
            if (File.Exists(bakPath))
            {
                try
                {
                    string bakJson = File.ReadAllText(bakPath);
                    var bakConfig = JsonSerializer.Deserialize<ServersConfig>(bakJson);
                    _servers = bakConfig?.Servers ?? new List<ServerConnection>();
                    
                    // MIGRATION: Backward compatibility
                    MigrateServerAuthentication(_servers);
                    
                    foreach (var server in _servers)
                    {
                        _connectionStatuses[server.Id] = new ServerConnectionStatus { ServerId = server.Id };
                    }
                    _logger?.LogInformation("Restored {Count} servers from backup file", _servers.Count);
                    return;
                }
                catch { /* backup also corrupt, fall through to empty list */ }
            }

            _servers = new List<ServerConnection>();
            SaveServers();
        }
    }

    /// <summary>
    /// Saves servers to the JSON config file.
    /// </summary>
    private void SaveServers()
    {
        lock (_serversLock)
        {
            try
            {
                var config = new ServersConfig { Servers = _servers };
                string json = JsonSerializer.Serialize(config, s_jsonOptions);
                File.WriteAllText(_configFilePath, json);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to save servers.json");
                throw;
            }
        }
    }

    /// <summary>
    /// Migrates server authentication configuration for backward compatibility.
    /// Old configs only had UseWindowsAuth, new code uses AuthenticationType.
    /// If AuthenticationType is default Windows but UseWindowsAuth is false, set to SqlServer.
    /// </summary>
    private void MigrateServerAuthentication(List<ServerConnection> servers)
    {
        foreach (var server in servers)
        {
            // If AuthenticationType is Windows (default) but UseWindowsAuth is false,
            // this is a SQL Server auth server from an old config
            if (server.AuthenticationType == AuthenticationTypes.Windows && !server.UseWindowsAuth)
            {
                server.AuthenticationType = AuthenticationTypes.SqlServer;
                _logger?.LogInformation("Migrated server '{DisplayName}' authentication type from legacy UseWindowsAuth=false to AuthenticationType=SqlServer", 
                    server.DisplayName);
            }
            // Ensure UseWindowsAuth stays in sync with AuthenticationType for consistency
            else if (server.AuthenticationType == AuthenticationTypes.SqlServer || server.AuthenticationType == AuthenticationTypes.EntraMFA)
            {
                server.UseWindowsAuth = false;
            }
            else if (server.AuthenticationType == AuthenticationTypes.Windows)
            {
                server.UseWindowsAuth = true;
            }
        }
    }

    /// <summary>
    /// JSON wrapper for servers list.
    /// </summary>
    private class ServersConfig
    {
        public List<ServerConnection> Servers { get; set; } = new();
    }
}
