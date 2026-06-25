/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json.Serialization;
using Microsoft.Data.SqlClient;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Models;

public class ServerConnection : INotifyPropertyChanged
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
    /// Authentication type: Windows, SqlServer, EntraMFA, ServicePrincipal, or ManagedIdentity
    /// </summary>
    public string AuthenticationType { get; set; } = AuthenticationTypes.Windows;

    /// <summary>
    /// Service-principal application/client id (used as UserID for ServicePrincipal auth).
    /// Non-secret; safe to persist in servers.json. The SP secret is NEVER stored here —
    /// it lives only in Windows Credential Manager (DPAPI) via CredentialService.
    /// </summary>
    public string? AzureClientId { get; set; }

    /// <summary>
    /// Optional Microsoft Entra tenant id. Stored for display/future use only.
    /// MDS resolves the tenant from the target server's AAD authority during the handshake,
    /// so this is NOT injected into the connection string (no first-class SP tenant keyword).
    /// </summary>
    public string? AzureTenantId { get; set; }

    /// <summary>
    /// Optional user-assigned managed identity client id. Blank = system-assigned identity.
    /// Non-secret; safe to persist in servers.json.
    /// </summary>
    public string? ManagedIdentityClientId { get; set; }

    /// <summary>
    /// Optional id of a <see cref="CredentialProfile"/> this server uses instead of inline per-server
    /// credentials. When set, the profile supplies the auth type + credentials (the server keeps its own
    /// connection facts: ServerName, DatabaseName, Encrypt, intent, etc.). null = today's per-server
    /// behavior, unchanged and backward-compatible (old servers.json simply omits this field → null).
    /// </summary>
    public string? CredentialProfileId { get; set; }

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
    /// Monthly cost of this server in USD, used for FinOps cost attribution.
    /// Set to 0 to hide cost columns. All FinOps costs are proportional to this budget.
    /// </summary>
    public decimal MonthlyCostUsd { get; set; } = 0m;

    /// <summary>
    /// Optional database name for the initial connection.
    /// Required for Azure SQL Database (which doesn't allow connecting to master).
    /// Leave empty for on-premises SQL Server (defaults to master).
    /// </summary>
    public string? DatabaseName { get; set; }

    /// <summary>
    /// Optional database where community stored procedures (sp_IndexCleanup) are installed.
    /// When null or empty, falls back to the connection database.
    /// </summary>
    public string? UtilityDatabase { get; set; }

    /// <summary>
    /// When true, sets ApplicationIntent=ReadOnly on the connection string.
    /// Required for connecting to AG listener read-only replicas and
    /// Azure SQL Business Critical / Managed Instance built-in read replicas.
    /// </summary>
    public bool ReadOnlyIntent { get; set; } = false;

    /// <summary>
    /// When true, sets MultiSubnetFailover=true on the connection string.
    /// Recommended for AG listeners and FCIs spanning multiple subnets.
    /// </summary>
    public bool MultiSubnetFailover { get; set; } = false;

    /// <summary>
    /// User databases to skip in per-database collectors (query_store, file_io_stats, etc.).
    /// System databases (master/tempdb/model/msdb) and the connection database itself are always
    /// excluded by the collectors and aren't represented here.
    /// </summary>
    public System.Collections.Generic.List<string> ExcludedDatabases { get; set; } = new();

    /// <summary>
    /// Server name with "(Read-Only)" suffix when ReadOnlyIntent is enabled.
    /// Used for sidebar subtitle and status text.
    /// </summary>
    [JsonIgnore]
    public string ServerNameDisplay => ReadOnlyIntent ? $"{ServerName} (Read-Only)" : ServerName;

    /// <summary>
    /// Display name with "(Read-Only)" suffix when ReadOnlyIntent is enabled.
    /// Used for alerts, tray notifications, status bar, and overview cards.
    /// </summary>
    [JsonIgnore]
    public string DisplayNameWithIntent => ReadOnlyIntent ? $"{DisplayName} (Read-Only)" : DisplayName;

    /// <summary>
    /// Display-only property for showing authentication type in UI.
    /// </summary>
    [JsonIgnore]
    public string AuthenticationDisplay => AuthenticationType switch
    {
        AuthenticationTypes.EntraMFA => "Microsoft Entra MFA",
        AuthenticationTypes.SqlServer => "SQL Server",
        AuthenticationTypes.ServicePrincipal => "Azure — Service Principal",
        AuthenticationTypes.ManagedIdentity => "Azure — Managed Identity",
        _ => "Windows"
    };

    /// <summary>
    /// Actual connection status from the most recent connection check.
    /// null = not checked yet, true = online, false = offline.
    /// </summary>
    [JsonIgnore]
    public bool? IsOnline { get; set; }

    /// <summary>
    /// Whether one or more collectors are currently failing for this server.
    /// null = not yet determined; true = some collectors have consecutive errors; false = all healthy.
    /// </summary>
    [JsonIgnore]
    public bool? HasCollectorErrors { get; set; }

    /// <summary>
    /// Computed dot status for the sidebar indicator. One of: "Unknown", "Online", "Warning", "Offline".
    /// Drives the Ellipse fill via DataTrigger in MainWindow.xaml.
    /// </summary>
    [JsonIgnore]
    public string DotStatus
    {
        get
        {
            if (IsOnline == true)
                return HasCollectorErrors == true ? "Warning" : "Online";
            if (IsOnline == false)
                return "Offline";
            return "Unknown"; // null — not yet checked
        }
    }

    /// <summary>
    /// Display-only property for showing status in UI.
    /// </summary>
    [JsonIgnore]
    public string StatusDisplay => IsEnabled ? "Enabled" : "Disabled";

    private string? _installedVersion;

    /// <summary>
    /// PerformanceMonitor Lite app version monitoring this server (e.g., "2.10.0").
    /// Populated by the Manage Servers window. Not persisted — runtime-only display field.
    /// Same value for every row since one Lite process monitors all servers.
    /// </summary>
    [JsonIgnore]
    public string? InstalledVersion
    {
        get => _installedVersion;
        set
        {
            if (_installedVersion == value) return;
            _installedVersion = value;
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(InstalledVersion)));
        }
    }

    public event PropertyChangedEventHandler? PropertyChanged;

    /// <summary>
    /// The immutable, atomic resolution tuple fed to <see cref="ApplyAuthentication"/>. It is sourced
    /// ENTIRELY from one place — either entirely from a credential profile or entirely from this
    /// server's own inline fields — never mixed field-by-field. This atomic shape is what structurally
    /// prevents a wrong-identity build (e.g. a profile secret combined with the server's stale
    /// AzureClientId). See <see cref="ResolveAuth"/>.
    /// </summary>
    private readonly record struct ResolvedAuth(
        string AuthType,
        string? Username,
        string? Password,
        string? AzureClientId,
        string? ManagedIdentityClientId);

    /// <summary>
    /// Resolves the single auth tuple for this server, fail-closed (§2, B-2/B-3).
    /// <list type="bullet">
    /// <item>No profile (<see cref="CredentialProfileId"/> == null) → tuple sourced entirely from this
    /// server's own inline fields (today's behavior, regression-identical).</item>
    /// <item>Profile in effect, profile FOUND → tuple sourced entirely from the profile (its AuthType,
    /// its non-secret fields, secret from <c>profile_{id}</c>). NEVER reads any <c>this.*</c> auth field.</item>
    /// <item>Profile in effect, profile MISSING → THROWS "credential profile '{id}' not found". A closed
    /// branch with NO fall-through to the server-self path — a dangling profile must never silently
    /// connect under the server's own stale inline auth.</item>
    /// </list>
    /// </summary>
    private ResolvedAuth ResolveAuth(CredentialService credentialService, IProfileLookup? profileLookup)
    {
        // No profile referenced → entirely server-self (regression-identical).
        if (string.IsNullOrEmpty(CredentialProfileId))
        {
            string? username = null;
            string? password = null;

            if (AuthenticationType == AuthenticationTypes.SqlServer ||
                AuthenticationType == AuthenticationTypes.ServicePrincipal)
            {
                var cred = credentialService.GetCredential(Id);
                if (cred.HasValue)
                {
                    username = cred.Value.Username;
                    password = cred.Value.Password;
                }
            }
            // ManagedIdentity fetches no credential (no secret).

            return new ResolvedAuth(AuthenticationType, username, password, AzureClientId, ManagedIdentityClientId);
        }

        // Profile referenced — CLOSED, fail-closed branch (B-2). No fall-through to server-self.
        var profile = profileLookup?.GetProfile(CredentialProfileId);
        if (profile == null)
        {
            throw new InvalidOperationException(
                $"credential profile '{CredentialProfileId}' not found");
        }

        // Tuple sourced ENTIRELY from the profile (B-3). We never read this.AzureClientId /
        // this.ManagedIdentityClientId here — only the profile's own fields.
        string? profileUser = null;
        string? profileSecret = null;
        if (profile.AuthType == AuthenticationTypes.SqlServer ||
            profile.AuthType == AuthenticationTypes.ServicePrincipal)
        {
            var cred = credentialService.GetCredential(ProfileManager.ProfileCredentialId(profile.Id));
            if (cred.HasValue)
            {
                profileUser = cred.Value.Username;
                profileSecret = cred.Value.Password;
            }
        }
        // ManagedIdentity profile carries no secret.

        return new ResolvedAuth(
            profile.AuthType,
            profileUser,
            profileSecret,
            profile.AzureClientId,
            profile.ManagedIdentityClientId);
    }

    /// <summary>
    /// Builds the connection string for this server, resolving credentials from a profile (when
    /// <see cref="CredentialProfileId"/> is set) or from this server's own inline fields otherwise.
    /// This is the single resolution entry point; the removed instance overloads used to expose a
    /// <see cref="CredentialService"/>-only path that could silently bypass a profile.
    /// </summary>
    public static string ResolveConnectionString(
        ServerConnection server,
        CredentialService credentialService,
        IProfileLookup? profileLookup)
    {
        var auth = server.ResolveAuth(credentialService, profileLookup);
        return server.BuildConnectionString(auth);
    }

    /// <summary>
    /// Like <see cref="ResolveConnectionString"/> but targets <see cref="UtilityDatabase"/> when set.
    /// Used for locating community stored procedures (sp_IndexCleanup) installed in a non-default database.
    /// </summary>
    public static string ResolveUtilityConnectionString(
        ServerConnection server,
        CredentialService credentialService,
        IProfileLookup? profileLookup)
    {
        var baseConnStr = ResolveConnectionString(server, credentialService, profileLookup);

        if (string.IsNullOrWhiteSpace(server.UtilityDatabase))
            return baseConnStr;

        var builder = new SqlConnectionStringBuilder(baseConnStr)
        {
            InitialCatalog = server.UtilityDatabase
        };
        return builder.ConnectionString;
    }

    /// <summary>
    /// Builds the connection string with the given credentials.
    /// Used by tests for the server-self shape; production paths go through
    /// <see cref="ResolveConnectionString"/>, which supplies the resolved tuple.
    /// </summary>
    internal string BuildConnectionString(string? username, string? password)
        => BuildConnectionString(new ResolvedAuth(AuthenticationType, username, password, AzureClientId, ManagedIdentityClientId));

    private string BuildConnectionString(ResolvedAuth auth)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = ServerName,
            InitialCatalog = string.IsNullOrWhiteSpace(DatabaseName) ? "master" : DatabaseName,
            ApplicationName = "PerformanceMonitorLite",
            ConnectTimeout = 15,
            CommandTimeout = 60,
            TrustServerCertificate = TrustServerCertificate,
            MultipleActiveResultSets = true,
            ApplicationIntent = ReadOnlyIntent ? ApplicationIntent.ReadOnly : ApplicationIntent.ReadWrite,
            MultiSubnetFailover = MultiSubnetFailover
        };

        // Set encryption mode
        builder.Encrypt = EncryptMode switch
        {
            "Mandatory" => SqlConnectionEncryptOption.Mandatory,
            "Strict" => SqlConnectionEncryptOption.Strict,
            "Optional" => SqlConnectionEncryptOption.Optional,
            // Fail closed: an unknown/blank EncryptMode (legacy or hand-edited servers.json) must
            // not silently drop to an unencrypted, cert-unvalidated connection. The UI default is
            // Mandatory and the Dashboard builders also default to Mandatory — match that.
            _ => SqlConnectionEncryptOption.Mandatory
        };

        // The tuple is atomic: every auth field comes from the SAME source (profile or server).
        ApplyAuthentication(builder, auth.AuthType, auth.Username, auth.Password,
            auth.AzureClientId, auth.ManagedIdentityClientId);

        return builder.ConnectionString;
    }

    /// <summary>
    /// Applies the authentication-mode keywords (IntegratedSecurity / Authentication / UserID /
    /// Password) to a connection-string builder. Shared by <see cref="BuildConnectionString"/> and
    /// the Add/Edit dialog's Test-Connection builder so the two build sites never diverge.
    /// </summary>
    /// <param name="builder">The builder to mutate.</param>
    /// <param name="authenticationType">One of the <see cref="AuthenticationTypes"/> constants.</param>
    /// <param name="username">SQL username, Entra UPN, or SP client id (depending on mode).</param>
    /// <param name="password">SQL password or SP client secret (depending on mode). Never logged.</param>
    /// <param name="azureClientId">SP client id fallback when <paramref name="username"/> is null.</param>
    /// <param name="managedIdentityClientId">User-assigned MI client id; blank = system-assigned.</param>
    internal static void ApplyAuthentication(
        SqlConnectionStringBuilder builder,
        string authenticationType,
        string? username,
        string? password,
        string? azureClientId,
        string? managedIdentityClientId)
    {
        if (authenticationType == AuthenticationTypes.Windows)
        {
            builder.IntegratedSecurity = true;
        }
        else if (authenticationType == AuthenticationTypes.SqlServer)
        {
            builder.IntegratedSecurity = false;
            builder.UserID = username ?? string.Empty;
            builder.Password = password ?? string.Empty;
        }
        else if (authenticationType == AuthenticationTypes.EntraMFA)
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
        else if (authenticationType == AuthenticationTypes.ServicePrincipal)
        {
            // Microsoft Entra service principal (non-interactive): client id + secret.
            builder.IntegratedSecurity = false;
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryServicePrincipal;
            builder.UserID = username ?? azureClientId ?? string.Empty;   // client/app id
            builder.Password = password ?? string.Empty;                  // client secret (from Credential Manager)
        }
        else if (authenticationType == AuthenticationTypes.ManagedIdentity)
        {
            // Azure managed identity (non-interactive): no secret.
            builder.IntegratedSecurity = false;
            builder.Authentication = SqlAuthenticationMethod.ActiveDirectoryManagedIdentity;
            if (!string.IsNullOrWhiteSpace(managedIdentityClientId))
            {
                builder.UserID = managedIdentityClientId;   // user-assigned MI; omit for system-assigned
            }
        }
    }

    /// <summary>
    /// Checks if credentials are available for this server (profile-aware, N-1).
    /// When a profile is referenced, reflects the profile's stored-secret state (SP/SQL → the
    /// <c>profile_{id}</c> key; MI → true); a dangling profile reference reports false. Otherwise
    /// reflects the per-server <c>this.Id</c> key, matching the legacy behavior.
    /// </summary>
    public static bool HasStoredCredentials(
        ServerConnection server,
        CredentialService credentialService,
        IProfileLookup? profileLookup)
    {
        if (!string.IsNullOrEmpty(server.CredentialProfileId))
        {
            var profile = profileLookup?.GetProfile(server.CredentialProfileId);
            if (profile == null)
            {
                // Dangling profile — no resolvable credential.
                return false;
            }
            // MI needs no secret; SQL/SP need the profile secret present.
            if (profile.AuthType == AuthenticationTypes.ManagedIdentity)
                return true;
            return credentialService.CredentialExists(ProfileManager.ProfileCredentialId(profile.Id));
        }

        // Zero-touch auth modes need no stored secret.
        if (server.AuthenticationType == AuthenticationTypes.Windows ||
            server.AuthenticationType == AuthenticationTypes.EntraMFA ||
            server.AuthenticationType == AuthenticationTypes.ManagedIdentity)
        {
            return true;
        }

        // SqlServer (password) and ServicePrincipal (client secret) require a stored credential.
        return credentialService.CredentialExists(server.Id);
    }
}
