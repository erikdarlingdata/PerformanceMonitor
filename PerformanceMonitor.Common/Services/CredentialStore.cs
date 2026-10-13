/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using CredentialManagement;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Common;

/// <summary>
/// Secure credential storage backed by Windows Credential Manager. Credentials are
/// encrypted by Windows using DPAPI and stored per-user. NEVER stores passwords in plain text.
///
/// <para>
/// Shared by Dashboard and Lite. The <c>credentialPrefix</c> namespaces each app's stored
/// secrets so they never collide in Windows Credential Manager: Dashboard uses
/// <c>"PerformanceMonitor_"</c>, Lite uses <c>"PerformanceMonitorLite_"</c>. Those prefixes
/// are LOAD-BEARING and must stay distinct — unifying them would let one app read the other's
/// credentials. Each app wraps this type in a thin adapter that supplies its own prefix.
/// </para>
/// </summary>
public class CredentialStore
{
    private const string DefaultDescription = "SQL Server credentials for Performance Monitor";

    private readonly string _credentialPrefix;
    private readonly string _description;
    private readonly ILogger? _logger;

    /// <summary>
    /// Creates a credential store for the given prefix using the default credential description.
    /// </summary>
    /// <param name="credentialPrefix">
    /// Per-app prefix prepended to every credential target (e.g. <c>"PerformanceMonitor_"</c>).
    /// Must be non-empty and distinct per app.
    /// </param>
    /// <param name="logger">Optional logger; failures are logged as warnings.</param>
    public CredentialStore(string credentialPrefix, ILogger? logger = null)
        : this(credentialPrefix, DefaultDescription, logger)
    {
    }

    /// <summary>
    /// Creates a credential store for the given prefix and credential description.
    /// </summary>
    /// <param name="credentialPrefix">
    /// Per-app prefix prepended to every credential target (e.g. <c>"PerformanceMonitor_"</c>).
    /// Must be non-empty and distinct per app.
    /// </param>
    /// <param name="description">Description stored on each credential entry in Credential Manager.</param>
    /// <param name="logger">Optional logger; failures are logged as warnings.</param>
    public CredentialStore(string credentialPrefix, string description, ILogger? logger = null)
    {
        if (string.IsNullOrWhiteSpace(credentialPrefix))
        {
            throw new ArgumentException("Credential prefix cannot be null or empty", nameof(credentialPrefix));
        }

        _credentialPrefix = credentialPrefix;
        _description = description;
        _logger = logger;
    }

    /// <summary>
    /// Saves SQL Server credentials to Windows Credential Manager.
    /// Credentials are encrypted automatically by Windows.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <param name="username">SQL Server username</param>
    /// <param name="password">SQL Server password (will be encrypted)</param>
    /// <returns>True if successful, false otherwise</returns>
    public bool SaveCredential(string serverId, string username, string password)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        if (string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException("Username cannot be null or empty", nameof(username));
        }

        if (password == null)
        {
            throw new ArgumentNullException(nameof(password));
        }

        try
        {
            using var credential = new Credential
            {
                Target = GetCredentialTarget(serverId),
                Username = username,
                Password = password,
                Type = CredentialType.Generic,
                PersistanceType = PersistanceType.LocalComputer,
                Description = _description
            };

            return credential.Save();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to save credential for server {ServerId}: {Error}", serverId, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Retrieves SQL Server credentials from Windows Credential Manager.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <returns>Tuple of (username, password) or null if not found</returns>
    public (string Username, string Password)? GetCredential(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        try
        {
            using var credential = new Credential
            {
                Target = GetCredentialTarget(serverId),
                Type = CredentialType.Generic
            };

            if (credential.Load())
            {
                return (credential.Username, credential.Password);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to load credential for server {ServerId}: {Error}", serverId, ex.Message);
        }

        return null;
    }

    /// <summary>
    /// Deletes SQL Server credentials from Windows Credential Manager.
    /// Should be called when a server is removed.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <returns>True if deleted or didn't exist, false on error</returns>
    public bool DeleteCredential(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        try
        {
            using var credential = new Credential
            {
                Target = GetCredentialTarget(serverId),
                Type = CredentialType.Generic
            };

            return credential.Delete();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to delete credential for server {ServerId}: {Error}", serverId, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Checks if credentials exist for a server.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <returns>True if credentials exist in Credential Manager</returns>
    public bool CredentialExists(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            return false;
        }

        try
        {
            using var credential = new Credential
            {
                Target = GetCredentialTarget(serverId),
                Type = CredentialType.Generic
            };

            return credential.Exists();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to check credential existence for server {ServerId}: {Error}", serverId, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Updates existing credentials. If credentials don't exist, creates them.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <param name="username">SQL Server username</param>
    /// <param name="password">SQL Server password (will be encrypted)</param>
    /// <returns>True if successful, false otherwise</returns>
    public bool UpdateCredential(string serverId, string username, string password)
    {
        DeleteCredential(serverId);
        return SaveCredential(serverId, username, password);
    }

    /// <summary>
    /// Generates the credential target name for Windows Credential Manager.
    /// Format: {credentialPrefix}{serverId}
    /// </summary>
    private string GetCredentialTarget(string serverId)
    {
        return $"{_credentialPrefix}{serverId}";
    }

    /// <summary>
    /// Lists the keys of this store's Generic credentials whose key starts with <paramref name="keyPrefix"/> (#5775). A key is
    /// what <see cref="SaveCredential"/> took as <c>serverId</c>: the Credential Manager target without this store's prefix, so
    /// the result feeds <see cref="DeleteCredential"/> and <see cref="GetCredential"/> as it is. Only this store's own prefix is
    /// searched, so another app's credentials (Dashboard's <c>PerformanceMonitor_</c> against Lite's
    /// <c>PerformanceMonitorLite_</c>) never appear. A failure to enumerate is logged and returns an empty list: the caller
    /// then does nothing, which is the safe outcome for a cleanup.
    /// </summary>
    /// <param name="keyPrefix">The start of the key to look for; may not be empty.</param>
    public System.Collections.Generic.IReadOnlyList<string> ListKeys(string keyPrefix)
    {
        if (string.IsNullOrWhiteSpace(keyPrefix))
        {
            throw new ArgumentException("Key prefix cannot be null or empty", nameof(keyPrefix));
        }

        var keys = new System.Collections.Generic.List<string>();
        try
        {
            using var set = new CredentialSet(GetCredentialTarget(keyPrefix) + "*");
            set.Load();
            foreach (var credential in set)
            {
                var target = credential.Target ?? "";
                if (credential.Type == CredentialType.Generic
                    && target.StartsWith(_credentialPrefix + keyPrefix, StringComparison.Ordinal))
                {
                    keys.Add(target[_credentialPrefix.Length..]);
                }

                credential.Dispose();
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to list credentials starting with {KeyPrefix}: {Error}", keyPrefix, ex.Message);
            return Array.Empty<string>();
        }

        return keys;
    }
}
