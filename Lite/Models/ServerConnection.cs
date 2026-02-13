/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Models;

public class ServerConnection
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string ServerName { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;
    public bool UseWindowsAuth { get; set; } = true;
    
    /// <summary>
    /// Authentication type: "Windows", "SqlServer", or "EntraMFA"
    /// </summary>
    public string AuthenticationType { get; set; } = "Windows";
    
    public string? Description { get; set; }
    public DateTime CreatedDate { get; set; } = DateTime.Now;
    public DateTime LastConnected { get; set; } = DateTime.Now;
    public bool IsFavorite { get; set; }
    public bool IsEnabled { get; set; } = true;

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
    /// Optional database name for the initial connection.
    /// Required for Azure SQL Database (which doesn't allow connecting to master).
    /// Leave empty for on-premises SQL Server (defaults to master).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Display-only property for showing authentication type in UI.
    /// </summary>
    [JsonIgnore]
    public string AuthenticationDisplay => AuthenticationType switch
    {
        "EntraMFA" => "Microsoft Entra MFA",
        "SqlServer" => "SQL Server",
        _ => "Windows"
    };

    /// <summary>
    /// Display-only property for showing status in UI.
    /// </summary>
    [JsonIgnore]
    public string StatusDisplay => IsEnabled ? "Enabled" : "Disabled";

    /// <summary>
    /// Builds and returns a connection string for this server.
    /// Credentials are retrieved from Windows Credential Manager if SQL auth is used.
    /// </summary>
    public string GetConnectionString(CredentialService credentialService)
    {
        string? username = null;
        string? password = null;

        if (AuthenticationType == "SqlServer")
        {
            var cred = credentialService.GetCredential(Id);
            if (cred.HasValue)
            {
                username = cred.Value.Username;
                password = cred.Value.Password;
            }
        }

        return BuildConnectionString(username, password);
    }

    /// <summary>
    /// Builds the connection string with the given credentials.
    /// </summary>
    private string BuildConnectionString(string? username, string? password)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = ServerName,
            InitialCatalog = string.IsNullOrWhiteSpace(DatabaseName) ? "master" : DatabaseName,
            ApplicationName = "PerformanceMonitorLite",
            ConnectTimeout = 15,
            CommandTimeout = 60,
            TrustServerCertificate = TrustServerCertificate,
            MultipleActiveResultSets = true
        };

        // Set encryption mode
        builder.Encrypt = EncryptMode switch
        {
            "Mandatory" => SqlConnectionEncryptOption.Mandatory,
            "Strict" => SqlConnectionEncryptOption.Strict,
            _ => SqlConnectionEncryptOption.Optional
        };

        if (AuthenticationType == "Windows")
        {
            builder.IntegratedSecurity = true;
        }
        else if (AuthenticationType == "SqlServer")
        {
            builder.IntegratedSecurity = false;
            builder.UserID = username ?? string.Empty;
            builder.Password = password ?? string.Empty;
        }
        else if (AuthenticationType == "EntraMFA")
        {
            // Microsoft Entra MFA (Azure AD Interactive)
            builder.IntegratedSecurity = false;
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryInteractive;
            // Optionally set UserID (email/UPN)
            if (!string.IsNullOrWhiteSpace(username))
            {
                builder.UserID = username;
            }
        }

        return builder.ConnectionString;
    }

    /// <summary>
    /// Checks if credentials are stored in Windows Credential Manager for this server.
    /// </summary>
    public bool HasStoredCredentials(CredentialService credentialService)
    {
        if (AuthenticationType == "Windows" || AuthenticationType == "EntraMFA")
        {
            return true;
        }

        return credentialService.CredentialExists(Id);
    }
}
