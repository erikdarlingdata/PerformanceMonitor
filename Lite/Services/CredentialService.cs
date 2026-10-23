/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Secure credential storage service using Windows Credential Manager.
/// Credentials are encrypted by Windows using DPAPI and stored per-user.
/// NEVER stores passwords in plain text.
///
/// Thin adapter over the shared <see cref="CredentialStore"/>. The
/// <c>"PerformanceMonitorLite_"</c> prefix keeps Lite credentials isolated from
/// Dashboard's (<c>"PerformanceMonitor_"</c>) in Windows Credential Manager — it MUST
/// stay distinct.
/// </summary>
public class CredentialService
{
    internal const string CredentialPrefix = "PerformanceMonitorLite_";

    private readonly CredentialStore _store;

    public CredentialService(ILogger<CredentialService>? logger = null)
    {
        _store = new CredentialStore(
            CredentialPrefix,
            "SQL Server credentials for Performance Monitor Lite",
            logger);
    }

    /// <summary>A service over a store with another prefix, so a test can use the real Credential Manager under keys of its
    /// own without touching the user's Lite credentials (#5775).</summary>
    internal CredentialService(CredentialStore store)
    {
        _store = store;
    }

    /// <summary>
    /// Saves SQL Server credentials to Windows Credential Manager.
    /// Credentials are encrypted automatically by Windows.
    /// </summary>
    public bool SaveCredential(string serverId, string username, string password)
        => _store.SaveCredential(serverId, username, password);

    /// <summary>
    /// Saves a credential and returns why it failed when it does (#5772). Accepts a secret of up to
    /// <see cref="CredentialStore.MaxSecretLength"/> characters; the failure text never contains the secret.
    /// </summary>
    public CredentialSaveResult TrySaveCredential(string serverId, string username, string password)
        => _store.TrySaveCredential(serverId, username, password);

    /// <summary>
    /// Deletes a credential and returns why it failed when it does (#5772). An entry that is not there counts
    /// as deleted. <see cref="DeleteCredential"/> keeps returning a bare bool for its existing callers.
    /// </summary>
    public CredentialSaveResult TryDeleteCredential(string serverId)
        => _store.TryDeleteCredential(serverId);

    /// <summary>
    /// Retrieves SQL Server credentials from Windows Credential Manager.
    /// </summary>
    public (string Username, string Password)? GetCredential(string serverId)
        => _store.GetCredential(serverId);

    /// <summary>One entry's read with its reason when it fails (#6066): not found apart from unreadable.</summary>
    public CredentialReadOutcome TryReadCredential(string serverId, out (string Username, string Password)? credential, out string? failure)
        => _store.TryReadCredential(serverId, out credential, out failure);

    /// <summary>
    /// Deletes SQL Server credentials from Windows Credential Manager.
    /// Should be called when a server is removed.
    /// </summary>
    public bool DeleteCredential(string serverId)
        => _store.DeleteCredential(serverId);

    /// <summary>
    /// Checks if credentials exist for a server.
    /// </summary>
    public bool CredentialExists(string serverId)
        => _store.CredentialExists(serverId);

    /// <summary>
    /// Updates existing credentials. If credentials don't exist, creates them.
    /// </summary>
    public bool UpdateCredential(string serverId, string username, string password)
        => _store.UpdateCredential(serverId, username, password);

    /// <summary>
    /// Lists the keys of the stored credentials that start with <paramref name="keyPrefix"/>, without the
    /// <c>PerformanceMonitorLite_</c> prefix (#5775). Empty when nothing matches or the list cannot be read.
    /// </summary>
    public IReadOnlyList<string> ListKeys(string keyPrefix)
        => _store.ListKeys(keyPrefix);
}
