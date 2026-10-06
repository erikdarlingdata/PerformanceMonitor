/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Produces (and reads back) the DPAPI-LocalMachine password blob the control-plane writes into
/// <c>config.config_monitored_servers.encrypted_password</c> — the SAME format the service's
/// <c>PerformanceMonitor.Darling.Service.DarlingSecrets</c> reads with <c>--encrypt-password</c>.
///
/// <para>This is DELIBERATELY distinct from <see cref="ViewerSecretStore"/> /
/// <see cref="WindowsCredentialServerSecretStore"/>, which keep viewer-local secrets in the per-USER
/// Windows Credential Manager. A monitored-server secret has to be decryptable by the SERVICE account,
/// so it must be a DPAPI-<b>LocalMachine</b> blob with the service's entropy, not a per-user vault entry.
/// LocalMachine scope is exactly why the service uses it (its comment: an administrator can encrypt a
/// password interactively that the service account later decrypts on the same machine). This works for
/// the managed single-box deploy (viewer + service on one host); a remote viewer producing a blob a
/// different service host can't decrypt is the documented service-side-provision path.</para>
///
/// <para>The entropy string is duplicated from the service's <c>DarlingSecrets.s_entropy</c> byte-for-byte
/// under the SAME "sliver rule" <see cref="ViewerSettings"/> already follows for the managed-Postgres
/// credential (the viewer does not reference the Service project); a Darling.Tests round-trip pins them in
/// lockstep so a drift is caught.</para>
/// </summary>
[SupportedOSPlatform("windows")]
public static class ViewerServerSecret
{
    /* Must equal PerformanceMonitor.Darling.Service.DarlingSecrets.s_entropy byte-for-byte, or a
       blob this viewer writes cannot be decrypted by the service (pinned by ViewerServerSecretTests). */
    private static readonly byte[] s_entropy = Encoding.UTF8.GetBytes("PerformanceMonitor.Darling.v1");

    /// <summary>DPAPI-LocalMachine-protects a plaintext password and returns the base64 blob for the store.</summary>
    public static string Protect(string plaintext)
    {
        ArgumentNullException.ThrowIfNull(plaintext);

        var protectedBytes = ProtectedData.Protect(
            Encoding.UTF8.GetBytes(plaintext), s_entropy, DataProtectionScope.LocalMachine);
        return Convert.ToBase64String(protectedBytes);
    }

    /// <summary>
    /// Reverses <see cref="Protect"/> for the Edit dialog's password pre-fill, returning null (never throwing)
    /// when the blob is empty, malformed, or was produced on a different machine (a remote-viewer blob this
    /// host's DPAPI key can't open) — the dialog then shows a blank password with a "re-enter to change" hint
    /// rather than dead-clicking into a cryptographic error.
    /// </summary>
    public static string? TryUnprotect(string? base64Blob)
    {
        /* A sealed value (#5366) opens only in the service, for its own connection settings: there is nothing to pre-fill. */
        if (string.IsNullOrWhiteSpace(base64Blob) || PasswordSeal.IsSealed(base64Blob))
        {
            return null;
        }

        try
        {
            var plainBytes = ProtectedData.Unprotect(
                Convert.FromBase64String(base64Blob), s_entropy, DataProtectionScope.LocalMachine);
            return Encoding.UTF8.GetString(plainBytes);
        }
        catch (Exception ex) when (ex is FormatException or CryptographicException)
        {
            return null;
        }
    }
}
