/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Serialization;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Models
{
    public class ServerConnection
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string ServerName { get; set; } = string.Empty;
        public string DisplayName { get; set; } = string.Empty;
        public bool UseWindowsAuth { get; set; } = true;
        public string? Description { get; set; }
        public DateTime CreatedDate { get; set; } = DateTime.Now;
        public DateTime LastConnected { get; set; } = DateTime.Now;
        public bool IsFavorite { get; set; }

        /// <summary>
        /// Encryption mode for the connection. Valid values: Optional, Mandatory, Strict.
        /// Default is Mandatory for security. Users can opt down to Optional if needed.
        /// </summary>
        public string EncryptMode { get; set; } = "Mandatory";

        /// <summary>
        /// Whether to trust the server certificate without validation.
        /// Default is false for security. Enable for servers with self-signed certificates.
        /// </summary>
        public bool TrustServerCertificate { get; set; } = false;

        /// <summary>
        /// Display-only property for showing authentication type in UI.
        /// </summary>
        [JsonIgnore]
        public string AuthenticationDisplay => UseWindowsAuth ? "Windows" : "SQL Server";

        /// <summary>
        /// SECURITY: Credentials are NEVER serialized to JSON.
        /// They are stored securely in Windows Credential Manager.
        /// This method retrieves the connection string with credentials loaded from secure storage.
        /// </summary>
        /// <param name="credentialService">The credential service to use for retrieving stored credentials</param>
        /// <returns>Connection string for SQL Server</returns>
        public string GetConnectionString(ICredentialService credentialService)
        {
            string? username = null;
            string? password = null;

            if (!UseWindowsAuth)
            {
                var cred = credentialService.GetCredential(Id);
                if (cred.HasValue)
                {
                    username = cred.Value.Username;
                    password = cred.Value.Password;
                }
            }

            return DatabaseService.BuildConnectionString(
                ServerName,
                UseWindowsAuth,
                username,
                password,
                EncryptMode,
                TrustServerCertificate
            ).ConnectionString;
        }

        /// <summary>
        /// Indicates whether credentials are stored in Windows Credential Manager for this server.
        /// Used to validate that SQL auth servers have credentials available.
        /// </summary>
        /// <param name="credentialService">The credential service to use for checking credentials</param>
        /// <returns>True if Windows auth is used or if credentials exist in credential manager</returns>
        public bool HasStoredCredentials(ICredentialService credentialService)
        {
            if (UseWindowsAuth)
            {
                return true;
            }

            return credentialService.CredentialExists(Id);
        }
    }
}
