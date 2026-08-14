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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// DPAPI protection for SQL-auth passwords in darling.json. LocalMachine scope deliberately —
/// the service runs under a service account, and machine scope lets an administrator encrypt a
/// password interactively (--encrypt-password) that the service account can later decrypt on
/// the same machine. (Lite uses the Windows Credential Manager instead, which is user-profile
/// scoped — right for an interactive app, wrong for a service.) The blob is machine-bound:
/// moving darling.json to another machine requires re-encrypting.
/// </summary>
[SupportedOSPlatform("windows")]
public static class DarlingSecrets
{
    private static readonly byte[] s_entropy = Encoding.UTF8.GetBytes("PerformanceMonitor.Darling.v1");

    public static string Protect(string plaintext)
    {
        if (plaintext is null)
        {
            throw new ArgumentNullException(nameof(plaintext));
        }

        var protectedBytes = ProtectedData.Protect(
            Encoding.UTF8.GetBytes(plaintext), s_entropy, DataProtectionScope.LocalMachine);
        return Convert.ToBase64String(protectedBytes);
    }

    /// <summary>
    /// What a DPAPI decrypt failure actually means, in the operator's terms (#2255).
    ///
    /// <para><b>Why this exists.</b> <c>ProtectedData.Unprotect</c> throws
    /// <c>CryptographicException: Key not valid for use in specified state</c>, and that message went into the
    /// log verbatim, once every 60 seconds, forever. The field report shows exactly how it lands: the operator
    /// read it as SQL Server rejecting the login and went looking at the server's credentials, because nothing
    /// in it says DPAPI, names what failed to decrypt, or mentions that a MACHINE boundary is involved.</para>
    ///
    /// <para><b>The cause it points at.</b> These blobs are <see cref="DataProtectionScope.LocalMachine"/>, so
    /// any user on the machine that wrote one can decrypt it and NO other machine ever can. That makes the
    /// overwhelmingly likely cause a credential saved by a Viewer running on a DIFFERENT PC — which is the
    /// documented single-box limitation of the Viewer's write path, not a permissions problem on the service
    /// account. The remedies are therefore all "encrypt it on this host", which is what the message says.</para>
    ///
    /// <para>Kept as a function rather than a literal at each throw site so the three surfaces that can hit
    /// this — a monitored server's password, the store credential, a network token — cannot drift into
    /// explaining the same failure three different ways.</para>
    /// </summary>
    internal static string DescribeDecryptFailure(string what) =>
        $"Could not DPAPI-decrypt {what}. This is a Windows Data Protection failure on THIS host, not SQL Server " +
        "rejecting a login — no credential was ever sent to the server. These blobs are encrypted with " +
        "LocalMachine scope, so they can only be decrypted on the machine that wrote them (any user on it, but " +
        "no other machine). The usual cause is a credential saved by a Viewer running on a DIFFERENT PC: the " +
        "Viewer encrypts on the machine it runs on, so a remotely-added server's password is unreadable here. " +
        "Fix it on this host, any one of: re-add the server from a Viewer running on this machine; run " +
        "'--add-server' here; run '--encrypt-password' here and paste the blob; or store the password as an " +
        "'env:' / 'file:' reference, which is not machine-bound.";

    public static string Unprotect(string base64Blob)
    {
        if (string.IsNullOrWhiteSpace(base64Blob))
        {
            throw new ArgumentException("Encrypted password blob is empty.", nameof(base64Blob));
        }

        var plainBytes = ProtectedData.Unprotect(
            Convert.FromBase64String(base64Blob), s_entropy, DataProtectionScope.LocalMachine);
        return Encoding.UTF8.GetString(plainBytes);
    }

    /// <summary>
    /// Resolves a monitored server's SQL-auth password: DPAPI blob preferred, then the <c>password</c>
    /// slot — which since #1804 may be an <c>env:</c>/<c>file:</c> REFERENCE
    /// (<see cref="DarlingSecretSource"/>) rather than a literal. <paramref name="usedPlaintext"/> is true
    /// only for a LITERAL (a reference is not plaintext-in-config, so callers do not warn on it).
    /// </summary>
    public static string ResolvePassword(MonitoredServer server, out bool usedPlaintext)
    {
        if (server is null)
        {
            throw new ArgumentNullException(nameof(server));
        }

        if (!string.IsNullOrWhiteSpace(server.EncryptedPassword))
        {
            usedPlaintext = false;

            /* #2087: add_servers stores env:/file: REFERENCES verbatim in this slot on Linux (a pointer is
               not a secret; DPAPI cannot exist there). A reference can never be confused with a DPAPI blob:
               blobs are base64 and contain no ':' prefix match. */
            if (DarlingSecretSource.IsReference(server.EncryptedPassword))
            {
                return DarlingSecretSource.Resolve(server.EncryptedPassword, $"servers['{server.DisplayName}'].encryptedPassword");
            }

            /* #2255: the raw CryptographicException ("Key not valid for use in specified state") reached the
               worker's connect-retry warning verbatim and repeated every 60s with no way to act on it. Server
               identity is only known HERE, so this is where it gets attached. */
            try
            {
                return Unprotect(server.EncryptedPassword);
            }
            catch (CryptographicException ex)
            {
                throw new InvalidOperationException(
                    DescribeDecryptFailure($"the stored password for server '{server.DisplayName}' " +
                                           "(servers[].encryptedPassword)"),
                    ex);
            }
        }

        if (!string.IsNullOrWhiteSpace(server.Password))
        {
            usedPlaintext = !DarlingSecretSource.IsReference(server.Password);
            return DarlingSecretSource.Resolve(server.Password, $"servers['{server.DisplayName}'].password");
        }

        throw new InvalidOperationException(
            $"Server '{server.DisplayName}' uses sql auth but has neither encryptedPassword nor password.");
    }
}
