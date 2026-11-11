/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Serialization;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// A named, reusable credential that many <see cref="ViewerServerEntry"/> definitions can reference via
/// <see cref="ViewerServerEntry.CredentialProfileId"/> — the Darling viewer's port of Lite's
/// <c>CredentialProfile</c>. One identity (a shared SQL login, an Azure service principal, or a managed
/// identity) covers a whole fleet of servers without per-server secret entry, and the Darling service
/// consumes the reference when it connects.
///
/// <para>SECURITY: this type holds NO secret. The SQL password / SP client secret lives ONLY in Windows
/// Credential Manager (DPAPI), keyed by <c>profile_{Id}</c> under the viewer's <c>PerformanceMonitorDarling_</c>
/// prefix, via <see cref="ViewerProfileStore"/> — mirroring the secret-free discipline of
/// <see cref="ViewerServerEntry"/>. viewer-profiles.json never stores a secret. As with the rest of the
/// Lite→Darling port the viewer persists these faithfully; the running SERVICE honoring a profile-backed
/// server is the separate wiring flagged in the PR.</para>
/// </summary>
public sealed class ViewerCredentialProfile
{
    /// <summary>Stable unique identifier (Guid string) — the REAL key; display-name uniqueness is a per-process UX nicety.</summary>
    public string Id { get; set; } = Guid.NewGuid().ToString();

    /// <summary>Human-friendly display name shown in the profile picker.</summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Authentication type. Only the three profile-eligible modes are valid:
    /// <see cref="AuthenticationTypes.SqlServer"/>, <see cref="AuthenticationTypes.ServicePrincipal"/>,
    /// or <see cref="AuthenticationTypes.ManagedIdentity"/>. Windows (no credential) and EntraMFA
    /// (interactive, per-user) are NOT profile-eligible — a shared profile is meaningless for them.
    /// </summary>
    public string AuthType { get; set; } = AuthenticationTypes.SqlServer;

    /// <summary>SQL login username for <see cref="AuthenticationTypes.SqlServer"/> profiles. Non-secret.</summary>
    public string? Username { get; set; }

    /// <summary>Service-principal application/client id for <see cref="AuthenticationTypes.ServicePrincipal"/> profiles. Non-secret.</summary>
    public string? AzureClientId { get; set; }

    /// <summary>Optional Microsoft Entra tenant id. Stored for display/reference only.</summary>
    public string? AzureTenantId { get; set; }

    /// <summary>Optional user-assigned managed-identity client id for <see cref="AuthenticationTypes.ManagedIdentity"/> profiles. Blank = system-assigned. Non-secret.</summary>
    public string? ManagedIdentityClientId { get; set; }

    /// <summary>Display-only label for the profile's auth type.</summary>
    [JsonIgnore]
    public string AuthTypeDisplay => AuthType switch
    {
        AuthenticationTypes.SqlServer => "SQL Server",
        AuthenticationTypes.ServicePrincipal => "Azure: Service Principal",
        AuthenticationTypes.ManagedIdentity => "Azure: Managed Identity",
        _ => AuthType
    };

    /// <summary>
    /// Display text for the profile picker: name plus a short id suffix to disambiguate duplicate display
    /// names (the shared file can hold same-named profiles; the Id is the real key).
    /// </summary>
    [JsonIgnore]
    public string PickerDisplay =>
        string.IsNullOrEmpty(Id) || Id.Length < 8
            ? $"{Name} ({AuthTypeDisplay})"
            : $"{Name} ({AuthTypeDisplay}), {Id[..8]}";
}
