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
using PerformanceMonitor.Common;

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
    /// Timeout in seconds for connectivity checks. Read from App settings each call.
    /// </summary>
    private static int ConnectionCheckTimeoutSeconds => App.ConnectionTimeoutSeconds;

    /// <summary>
    /// Detection query for the connectivity check - engine edition, version, UTC offset, RDS and
    /// msdb access, all from scalar functions that need NO special permission. Deliberately carries
    /// NO <c>FROM sys.dm_os_sys_info</c>: that DMV requires VIEW DATABASE STATE, which an Azure SQL
    /// DB monitoring login often lacks, and coupling edition detection to it made the whole probe
    /// throw and silently mis-detect Azure SQL DB (EngineEdition 5) as on-prem (#1535). Must
    /// succeed for any connected login. Columns: 0 sql_version, 1 major_version, 2 utc_offset,
    /// 3 engine_edition, 4 is_aws_rds, 5 has_msdb_access.
    /// </summary>
    internal const string DetectionQueryText = @"
        SELECT
            @@VERSION AS sql_version,
            CONVERT(integer, SERVERPROPERTY('ProductMajorVersion')) AS major_version,
            DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) AS utc_offset_minutes,
            CONVERT(integer, SERVERPROPERTY('EngineEdition')) AS engine_edition,
            /* #2575: DB_ID alone answers NULL both for a database that does not exist and for one the caller
       cannot SEE, so a least-privilege monitoring login without VIEW ANY DATABASE reported every RDS
       instance as not-RDS - measured across 84 of them. SUSER_SNAME is executable by public and is not
       filtered by database visibility: RDS renames the sa-equivalent principal (SID 0x01) to 'rdsa',
       where a stock instance answers 'sa'. OR'd rather than replaced, so a privileged login keeps the
       direct signal and a restricted one gains a second chance at the truth. */
    CASE WHEN DB_ID('rdsadmin') IS NOT NULL OR SUSER_SNAME(0x01) = 'rdsa' THEN 1 ELSE 0 END AS is_aws_rds,
            HAS_DBACCESS(N'msdb') AS has_msdb_access";

    /// <summary>
    /// Best-effort start-time read - <c>sqlserver_start_time</c> is the ONLY probe fact that
    /// genuinely needs <c>sys.dm_os_sys_info</c> (VIEW SERVER/DATABASE STATE). Run in its OWN
    /// try/catch so a permission failure leaves ServerStartTime unset without flipping IsOnline or
    /// failing platform detection.
    /// </summary>
    internal const string ServerStartTimeQueryText =
        "SELECT sqlserver_start_time FROM sys.dm_os_sys_info";

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
    /// Full path to this manager's servers.json. Used by <see cref="ProfileManager"/> to colocate
    /// profiles.json in the same (shared) config directory.
    /// </summary>
    public string ConfigFilePath => _configFilePath;

    /// <summary>
    /// Late-injected profile lookup (§3.1, B1-R2). Default null = today's no-profile behavior: every
    /// server resolves to its own inline auth. When wired (after <see cref="ProfileManager"/> exists),
    /// <see cref="CheckConnectionAsync"/> resolves profile-backed servers through the SAME fail-closed
    /// atomic-tuple logic as the external consumers, rather than flipping them "Offline" under the
    /// wrong identity. <see cref="ServerManager"/> holds only this abstraction — never ProfileManager —
    /// so coupling stays acyclic.
    /// </summary>
    public IProfileLookup? ProfileLookup { get; set; }

    /// <summary>
    /// A resolver bound to this manager's credential service + current profile lookup. Hand this to
    /// connection-string consumers (ServerTab, FinOpsTab, the collector, the MCP path, etc.) so they
    /// all resolve through the same profile-or-self/fail-closed path.
    /// </summary>
    public CredentialResolver CredentialResolver => new(_credentialService, ProfileLookup);

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
        else if (server.AuthenticationType == AuthenticationTypes.ServicePrincipal && !string.IsNullOrEmpty(username) && password != null)
        {
            // For service principal, save client id (username) + client secret (password).
            // The secret lives ONLY in Windows Credential Manager (DPAPI), never in servers.json.
            if (!_credentialService.SaveCredential(server.Id, username, password))
            {
                throw new InvalidOperationException("Failed to save service principal secret to Windows Credential Manager");
            }
        }
        // ManagedIdentity stores nothing (no secret).

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
        else if (server.AuthenticationType == AuthenticationTypes.ServicePrincipal && !string.IsNullOrEmpty(username) && password != null)
        {
            // For service principal, update client id (username) + client secret (password).
            // The secret lives ONLY in Windows Credential Manager (DPAPI), never in servers.json.
            if (!_credentialService.UpdateCredential(server.Id, username, password))
            {
                throw new InvalidOperationException("Failed to update service principal secret in Windows Credential Manager");
            }
        }
        else if (server.AuthenticationType == AuthenticationTypes.Windows ||
                 server.AuthenticationType == AuthenticationTypes.ManagedIdentity ||
                 server.AuthenticationType == AuthenticationTypes.EntraMFA)
        {
            // Zero-touch auth (Windows / Managed Identity): remove any stored credential.
            // This also deletes an orphaned secret left behind when switching away from
            // SqlServer or ServicePrincipal (e.g. SP -> MI, SP -> Windows).
            //
            // EntraMFA reaches this arm ONLY when the MFA username is blank, because the
            // earlier EntraMFA arm (which requires a non-blank username) runs first and stores
            // the username. The blank-username case is exactly the orphan case: switching e.g.
            // SP -> EntraMFA with no username must delete the stale SP client secret rather than
            // leaving it in Credential Manager indefinitely.
            _credentialService.DeleteCredential(server.Id);
        }

        _logger?.LogInformation("Updated server '{DisplayName}' ({ServerName})", server.DisplayName, server.ServerName);
    }

    /// <summary>
    /// Persists an in-place change to an existing server's NON-credential settings (e.g. the #1319
    /// per-server view database filter) by re-serializing servers.json. Unlike <see cref="UpdateServer"/>
    /// this NEVER touches Windows Credential Manager, so it is safe to call for settings unrelated to
    /// auth (calling UpdateServer with no username/password would delete an EntraMFA server's stored
    /// username). The passed instance is normally the same reference already in the list, mutated in place.
    /// </summary>
    public void UpdateServerSettings(ServerConnection server)
    {
        lock (_serversLock)
        {
            var existing = _servers.FirstOrDefault(s => s.Id == server.Id);
            if (existing == null)
            {
                return;
            }

            var index = _servers.IndexOf(existing);
            _servers[index] = server;
            SaveServers();
        }
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
            // Resolve through the same fail-closed atomic-tuple logic as the external consumers
            // (§3.1). A profile-backed server with a dangling/missing profile throws here rather than
            // silently connecting under the server's own stale inline auth.
            var connectionString = ServerConnection.ResolveConnectionString(server, _credentialService, ProfileLookup);

            // Modify connection string to use short timeout for connectivity check
            var builder = new SqlConnectionStringBuilder(connectionString)
            {
                ConnectTimeout = ConnectionCheckTimeoutSeconds
            };

            using var connection = new SqlConnection(builder.ConnectionString);
            await connection.OpenAsync();

            // Connection succeeded — server is reachable regardless of DMV permissions below.
            status.IsOnline = true;
            status.ErrorMessage = null;
            status.UserCancelledMfa = false; // Clear cancellation flag on successful connection

            // Detection query: platform/version/UTC-offset/RDS/msdb facts from scalar functions
            // that need NO permission (no sys.dm_os_sys_info), so an Azure SQL DB login lacking
            // VIEW DATABASE STATE still classifies correctly instead of mis-detecting as on-prem
            // (#1535). In its own try/catch: a failure must NOT flip IsOnline back to false.
            try
            {
                using var command = new SqlCommand(DetectionQueryText, connection);
                command.CommandTimeout = ConnectionCheckTimeoutSeconds;

                using var reader = await command.ExecuteReaderAsync();
                if (await reader.ReadAsync())
                {
                    if (!reader.IsDBNull(0))
                        status.SqlServerVersion = reader.GetString(0);
                    if (!reader.IsDBNull(1))
                        status.SqlMajorVersion = Convert.ToInt32(reader.GetValue(1));
                    if (!reader.IsDBNull(2))
                        status.UtcOffsetMinutes = Convert.ToInt32(reader.GetValue(2));
                    if (!reader.IsDBNull(3))
                        status.SqlEngineEdition = Convert.ToInt32(reader.GetValue(3));
                    if (!reader.IsDBNull(4))
                        status.IsAwsRds = Convert.ToInt32(reader.GetValue(4)) == 1;
                    if (!reader.IsDBNull(5))
                        status.HasMsdbAccess = Convert.ToInt32(reader.GetValue(5)) == 1;
                }
            }
            catch (SqlException metaEx)
            {
                // Detection query failed (unexpected - it needs no special permission) but the
                // server IS reachable - keep IsOnline = true, just record the warning.
                status.ErrorMessage = $"Connected, but metadata query failed: {metaEx.Message}";
                _logger?.LogWarning("Metadata query failed for server '{DisplayName}' (server is still online): {Message}",
                    server.DisplayName, metaEx.Message);
            }

            // Best-effort start-time read in its OWN try/catch: sqlserver_start_time is the only
            // probe fact that genuinely needs sys.dm_os_sys_info (VIEW SERVER/DATABASE STATE), so a
            // permission gap here leaves ServerStartTime unset without flipping IsOnline or failing
            // the detection above (#1535).
            try
            {
                using var startCommand = new SqlCommand(ServerStartTimeQueryText, connection);
                startCommand.CommandTimeout = ConnectionCheckTimeoutSeconds;

                using var startReader = await startCommand.ExecuteReaderAsync();
                if (await startReader.ReadAsync() && !startReader.IsDBNull(0))
                    status.ServerStartTime = startReader.GetDateTime(0);
            }
            catch (SqlException startEx)
            {
                _logger?.LogDebug("Start-time query unavailable for server '{DisplayName}' (server is still online): {Message}",
                    server.DisplayName, startEx.Message);
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
    /// Imports server connections from an external servers.json file.
    /// Upserts by ServerName — existing servers are skipped, new ones are added
    /// with their original GUIDs so Credential Manager entries still resolve.
    /// Returns (imported count, skipped count).
    /// </summary>
    public (int Imported, int Skipped) ImportServersFromFile(string serversJsonPath)
    {
        if (!File.Exists(serversJsonPath))
            throw new FileNotFoundException("servers.json not found", serversJsonPath);

        var json = File.ReadAllText(serversJsonPath);
        var config = JsonSerializer.Deserialize<ServersConfig>(json);
        var importedServers = config?.Servers ?? [];

        int imported = 0;
        int skipped = 0;

        lock (_serversLock)
        {
            foreach (var server in importedServers)
            {
                // Skip if we already have a server with the same name
                var existing = _servers.FirstOrDefault(s =>
                    string.Equals(s.ServerName, server.ServerName, StringComparison.OrdinalIgnoreCase) &&
                    s.ReadOnlyIntent == server.ReadOnlyIntent);

                if (existing != null)
                {
                    skipped++;
                    continue;
                }

                // Also skip if the same GUID already exists (shouldn't happen, but defensive)
                if (_servers.Any(s => s.Id == server.Id))
                {
                    skipped++;
                    continue;
                }

                // Add with original GUID so Credential Manager entries still work
                _servers.Add(server);
                _connectionStatuses[server.Id] = new ServerConnectionStatus { ServerId = server.Id };
                imported++;
            }

            if (imported > 0)
                SaveServers();
        }

        _logger?.LogInformation("Imported {Imported} servers, skipped {Skipped} duplicates", imported, skipped);
        return (imported, skipped);
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
    /// JSON wrapper for servers list.
    /// </summary>
    private class ServersConfig
    {
        public List<ServerConnection> Servers { get; set; } = new();
    }
}
