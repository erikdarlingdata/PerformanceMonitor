/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Models
{
    public class ServerConnection
    {
        public string Id { get; set; } = Guid.NewGuid().ToString();
        public string ServerName { get; set; } = string.Empty;
        public string DisplayName { get; set; } = string.Empty;

        /// <summary>
        /// Backward compatibility property for old servers.json files.
        /// Returns true if authentication type is Windows.
        /// Setter updates AuthenticationType for migration from old configs.
        /// </summary>
        public bool UseWindowsAuth
        {
            get => AuthenticationType == AuthenticationTypes.Windows;
            set
            {
                // During JSON deserialization of old configs, update AuthenticationType based on UseWindowsAuth
                // Only apply this if AuthenticationType is still at default (indicating old JSON without that field)
                if (AuthenticationType == AuthenticationTypes.Windows && !value)
                {
                    // Old config with UseWindowsAuth=false -> SQL Server auth
                    AuthenticationType = AuthenticationTypes.SqlServer;
                }
                // If value is true, keep Windows (already the default)
            }
        }

        /// <summary>
        /// Authentication type: Windows, SqlServer, or EntraMFA.
        /// </summary>
        public string AuthenticationType { get; set; } = AuthenticationTypes.Windows;
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
        public string AuthenticationDisplay => AuthenticationType switch
        {
            AuthenticationTypes.EntraMFA => "Microsoft Entra MFA",
            AuthenticationTypes.SqlServer => "SQL Server",
            _ => "Windows"
        };

        /// <summary>
        /// SECURITY: Credentials are NEVER serialized to JSON.
        /// They are stored securely in Windows Credential Manager.
        /// This method retrieves the connection string with credentials loaded from secure storage.
        /// </summary>
        /// <param name="credentialService">The credential service to use for retrieving stored credentials</param>
        /// <returns>Connection string for SQL Server</returns>
        public string GetConnectionString(ICredentialService credentialService)
        {
            if (AuthenticationType == AuthenticationTypes.EntraMFA)
            {
                // Build MFA connection string with ActiveDirectoryInteractive
                var mfaBuilder = new SqlConnectionStringBuilder
                {
                    DataSource = ServerName,
                    InitialCatalog = "PerformanceMonitor",
                    ApplicationName = "PerformanceMonitorDashboard",
                    ConnectTimeout = 15,
                    MultipleActiveResultSets = true,
                    TrustServerCertificate = TrustServerCertificate,
                    Encrypt = EncryptMode switch
                    {
                        "Optional" => SqlConnectionEncryptOption.Optional,
                        "Strict" => SqlConnectionEncryptOption.Strict,
                        _ => SqlConnectionEncryptOption.Mandatory
                    },
                    Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive
                };

                // Optionally pre-populate username from credential store
                var mfaCred = credentialService.GetCredential(Id);
                if (mfaCred.HasValue && !string.IsNullOrEmpty(mfaCred.Value.Username))
                    mfaBuilder.UserID = mfaCred.Value.Username;

                return mfaBuilder.ConnectionString;
            }

            string? username = null;
            string? password = null;

            if (AuthenticationType == AuthenticationTypes.SqlServer)
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
        /// <returns>True if Windows auth or MFA is used, or if credentials exist in credential manager</returns>
        public bool HasStoredCredentials(ICredentialService credentialService)
        {
            if (AuthenticationType == AuthenticationTypes.Windows || AuthenticationType == AuthenticationTypes.EntraMFA)
            {
                return true;
            }

            return credentialService.CredentialExists(Id);
        }
    }
}
