/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One monitored-server DEFINITION in the viewer's own registry — the Darling analog of Lite's
/// <see cref="T:PerformanceMonitorLite.Models.ServerConnection"/>, ported for the viewer's Add / Manage /
/// Edit surfaces (persisted by <see cref="ViewerServerStore"/> to viewer-servers.json).
///
/// <para><b>Persistence adaptation (matching the #1402 Settings port).</b> In Lite a
/// <c>ServerConnection</c> both configures the app's in-process collector AND builds the live SqlClient
/// connection string. The Darling viewer never connects to monitored SQL Servers (it reads only the
/// Postgres store — see the README), so this is a pure DECLARATIVE record: the same operator-facing
/// fields, but with no connection-string builder and no SqlClient dependency. The viewer persists these
/// faithfully so the Add/Manage/Edit UI is complete and survives a restart; making the running SERVICE
/// honor an added/edited server (it reads darling.json and registers servers into the store) is a
/// separate wiring concern flagged in the PR. Favorites are the one field the viewer acts on today —
/// the sidebar stars + pins them.</para>
///
/// <para>Secrets (the SQL password and the service-principal client secret) are NOT stored here — like
/// Lite, they go to Windows Credential Manager, keyed by <see cref="Id"/>, via
/// <see cref="ViewerServerStore"/>. The optional Entra UPN is a non-secret hint and IS persisted here
/// (a small viewer adaptation of Lite, which parked it in the credential store).</para>
/// </summary>
public sealed class ViewerServerEntry
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public string ServerName { get; set; } = string.Empty;
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>Windows, SqlServer, EntraMFA, ServicePrincipal, or ManagedIdentity (see <see cref="AuthenticationTypes"/>).</summary>
    public string AuthenticationType { get; set; } = AuthenticationTypes.Windows;

    /// <summary>
    /// Id of a shared <see cref="ViewerCredentialProfile"/> (from <see cref="ViewerProfileStore"/>) this
    /// server references, or null for inline per-server auth. When set, the profile supplies the identity
    /// (the Darling service resolves it at connect time) and no per-server secret is stored — mirroring
    /// Lite's <c>ServerConnection.CredentialProfileId</c>.
    /// </summary>
    public string? CredentialProfileId { get; set; }

    /// <summary>Service-principal application/client id (non-secret; the SP secret lives in Credential Manager).</summary>
    public string? AzureClientId { get; set; }

    /// <summary>Optional Entra tenant id (stored for reference only).</summary>
    public string? AzureTenantId { get; set; }

    /// <summary>Optional user-assigned managed-identity client id (blank = system-assigned).</summary>
    public string? ManagedIdentityClientId { get; set; }

    /// <summary>Optional Entra MFA UPN hint (non-secret). Viewer adaptation — Lite kept this in the credential store.</summary>
    public string? EntraUsername { get; set; }

    public string? Description { get; set; }
    public DateTime CreatedDate { get; set; } = DateTime.Now;
    public DateTime LastConnected { get; set; } = DateTime.Now;
    public bool IsFavorite { get; set; }
    public bool IsEnabled { get; set; } = true;

    /// <summary>Encryption mode: Optional, Mandatory (default), or Strict.</summary>
    public string EncryptMode { get; set; } = "Mandatory";

    public bool TrustServerCertificate { get; set; }

    /// <summary>Monthly cost in USD for FinOps cost attribution; 0 hides cost columns.</summary>
    public decimal MonthlyCostUsd { get; set; }

    /// <summary>Per-server override of the deadlock/blocking alert delivery mode; null = inherit the global setting.</summary>
    public AlertNotificationMode? AlertDeliveryModeOverride { get; set; }

    /// <summary>Optional initial-connection database (required for Azure SQL Database).</summary>
    public string? DatabaseName { get; set; }

    /// <summary>Optional database where community stored procedures (sp_IndexCleanup) are installed.</summary>
    public string? UtilityDatabase { get; set; }

    /// <summary>Sets ApplicationIntent=ReadOnly on the eventual connection (AG readable replicas).</summary>
    public bool ReadOnlyIntent { get; set; }

    /// <summary>Sets MultiSubnetFailover=true (AG listeners and FCIs spanning subnets).</summary>
    public bool MultiSubnetFailover { get; set; }

    /// <summary>User databases to skip in per-database collectors.</summary>
    public List<string> ExcludedDatabases { get; set; } = new();

    /// <summary>
    /// #1319: the per-server global database FILTER (display-only). When non-empty, database-scoped views
    /// (query grids, blocking, per-DB config, trends/slicers/heatmap) show only these databases; empty =
    /// "All". Distinct from <see cref="ExcludedDatabases"/> (collector-side "don't COLLECT"); this changes
    /// only what the viewer displays. Persisted per-server in viewer-servers.json so a subset stays sticky.
    /// </summary>
    public List<string> ViewFilterDatabases { get; set; } = new();

    /// <summary>Server name with "(Read-Only)" suffix when <see cref="ReadOnlyIntent"/> is set (grid + status text).</summary>
    [JsonIgnore]
    public string ServerNameDisplay => ReadOnlyIntent ? $"{ServerName} (Read-Only)" : ServerName;

    /// <summary>Display name with "(Read-Only)" suffix when <see cref="ReadOnlyIntent"/> is set.</summary>
    [JsonIgnore]
    public string DisplayNameWithIntent => ReadOnlyIntent ? $"{DisplayName} (Read-Only)" : DisplayName;

    /// <summary>Human-readable authentication label for the Manage Servers grid.</summary>
    [JsonIgnore]
    public string AuthenticationDisplay => AuthenticationType switch
    {
        AuthenticationTypes.EntraMFA => "Microsoft Entra MFA",
        AuthenticationTypes.SqlServer => "SQL Server",
        AuthenticationTypes.ServicePrincipal => "Azure — Service Principal",
        AuthenticationTypes.ManagedIdentity => "Azure — Managed Identity",
        _ => "Windows"
    };

    /// <summary>"Enabled"/"Disabled" for the Manage Servers grid.</summary>
    [JsonIgnore]
    public string StatusDisplay => IsEnabled ? "Enabled" : "Disabled";
}
