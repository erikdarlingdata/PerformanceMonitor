/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security;
using CredentialManagement;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;

namespace PerformanceMonitorDashboard.Services
{
    /// <summary>
    /// Secure credential storage service using Windows Credential Manager.
    /// Credentials are encrypted by Windows using DPAPI and stored per-user.
    /// NEVER stores passwords in plain text.
    /// </summary>
    public class CredentialService : ICredentialService
    {
        private const string CredentialPrefix = "PerformanceMonitor_";

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
                using (var credential = new Credential
                {
                    Target = GetCredentialTarget(serverId),
                    Username = username,
                    Password = password,
                    Type = CredentialType.Generic,
                    PersistanceType = PersistanceType.LocalComputer,
                    Description = "SQL Server credentials for Performance Monitor Dashboard"
                })
                {
                    return credential.Save();
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to save credential for server {serverId}: {ex.Message}");
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
                using (var credential = new Credential
                {
                    Target = GetCredentialTarget(serverId),
                    Type = CredentialType.Generic
                })
                {
                    if (credential.Load())
                    {
                        return (credential.Username, credential.Password);
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to load credential for server {serverId}: {ex.Message}");
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
                using (var credential = new Credential
                {
                    Target = GetCredentialTarget(serverId),
                    Type = CredentialType.Generic
                })
                {
                    return credential.Delete();
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to delete credential for server {serverId}: {ex.Message}");
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
                using (var credential = new Credential
                {
                    Target = GetCredentialTarget(serverId),
                    Type = CredentialType.Generic
                })
                {
                    return credential.Exists();
                }
            }
            catch (Exception ex)
            {
                Logger.Warning($"Failed to check credential existence for server {serverId}: {ex.Message}");
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
        /// Format: PerformanceMonitor_{serverId}
        /// </summary>
        private string GetCredentialTarget(string serverId)
        {
            return $"{CredentialPrefix}{serverId}";
        }

    }
}
