/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Serialization;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Models;

/// <summary>
/// A named, reusable credential that many <see cref="ServerConnection"/> entries can reference
/// via <see cref="ServerConnection.CredentialProfileId"/>. One identity (a shared SQL login or a
/// single Azure service principal / managed identity) can then cover a whole fleet of servers
/// without per-server secret entry.
///
/// SECURITY: this type holds NO secret. The SQL password / SP client secret lives ONLY in Windows
/// Credential Manager (DPAPI) under the key <c>PerformanceMonitorLite_profile_{Id}</c>, mirroring
/// the secret-free discipline of <see cref="ServerConnection"/>. profiles.json never stores a secret.
/// </summary>
public class CredentialProfile
{
    /// <summary>
    /// Stable unique identifier (Guid string). The REAL key — display name uniqueness is a
    /// per-process UX nicety only (the shared profiles.json is multi-user-writable).
    /// </summary>
    public string Id { get; set; } = Guid.NewGuid().ToString();

    /// <summary>
    /// Human-friendly display name shown in the profile picker.
    /// </summary>
    public string Name { get; set; } = string.Empty;

    /// <summary>
    /// Authentication type. Only the three profile-eligible modes are valid:
    /// <see cref="AuthenticationTypes.SqlServer"/>, <see cref="AuthenticationTypes.ServicePrincipal"/>,
    /// or <see cref="AuthenticationTypes.ManagedIdentity"/>. Windows (no credential) and EntraMFA
    /// (interactive, per-user) are NOT profile-eligible — a shared profile is meaningless for them.
    /// </summary>
    public string AuthType { get; set; } = AuthenticationTypes.SqlServer;

    /// <summary>
    /// SQL login username for <see cref="AuthenticationTypes.SqlServer"/> profiles. Non-secret.
    /// (The matching password is stored only in Credential Manager.)
    /// </summary>
    public string? Username { get; set; }

    /// <summary>
    /// Service-principal application/client id for <see cref="AuthenticationTypes.ServicePrincipal"/>
    /// profiles (used as UserID). Non-secret. (The client secret is stored only in Credential Manager.)
    /// </summary>
    public string? AzureClientId { get; set; }

    /// <summary>
    /// Optional Microsoft Entra tenant id. Stored for display/future use only; not injected into the
    /// connection string (MDS resolves the tenant from the target server's AAD authority).
    /// </summary>
    public string? AzureTenantId { get; set; }

    /// <summary>
    /// Optional user-assigned managed identity client id for
    /// <see cref="AuthenticationTypes.ManagedIdentity"/> profiles. Blank = system-assigned. Non-secret.
    /// </summary>
    public string? ManagedIdentityClientId { get; set; }

    /// <summary>
    /// Display-only label for the profile's auth type.
    /// </summary>
    [JsonIgnore]
    public string AuthTypeDisplay => AuthType switch
    {
        AuthenticationTypes.SqlServer => "SQL Server",
        AuthenticationTypes.ServicePrincipal => "Azure — Service Principal",
        AuthenticationTypes.ManagedIdentity => "Azure — Managed Identity",
        _ => AuthType
    };

    /// <summary>
    /// Display text for the profile picker: name plus a short id suffix to disambiguate duplicate
    /// display names (N-2 — the shared file can hold same-named profiles; the Id is the real key).
    /// </summary>
    [JsonIgnore]
    public string PickerDisplay =>
        string.IsNullOrEmpty(Id) || Id.Length < 8
            ? $"{Name} ({AuthTypeDisplay})"
            : $"{Name} ({AuthTypeDisplay}) — {Id[..8]}";
}
