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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// DPAPI protection for SQL-auth passwords in darling.json. LocalMachine scope deliberately —
/// the service runs under a service account, and machine scope lets an administrator encrypt a
/// password interactively (--encrypt-password) that the service account can later decrypt on
/// the same machine. (Lite uses the Windows Credential Manager instead, which is user-profile
/// scoped — right for an interactive app, wrong for a service.) The blob is machine-bound:
/// moving darling.json to another machine requires re-encrypting.
/// </summary>
public static class DarlingSecrets
{
    private static readonly byte[] s_entropy = Encoding.UTF8.GetBytes("PerformanceMonitor.Darling.v1");

    [SupportedOSPlatform("windows")]
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
    /// <para>Kept as a function rather than a literal at each throw site so the surfaces that can hit
    /// this — a monitored server's password, a network token — cannot drift into explaining the same failure
    /// different ways. It is written for a monitored server's saved password, so its remedies do not fit the
    /// store's own credential file: that one has <see cref="DescribeStoreCredentialDecryptFailure"/> (#4744).</para>
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

    /// <summary>
    /// What a DPAPI decrypt failure means for the MANAGED STORE's own credential file, in the operator's terms
    /// (#4744). <see cref="DescribeDecryptFailure"/> is written for a monitored server's saved password, and its
    /// remedies (re-add the server, <c>--add-server</c>, <c>--encrypt-password</c>, an <c>env:</c>/<c>file:</c>
    /// reference) do nothing for the store: the file (<c>pg-credential.dpapi</c>, beside the data directory) holds
    /// the store owner's generated password, which cannot be regenerated. The cause an operator can act on is that
    /// the store was copied here from the machine that created it, so the remedy is to run the command back on that
    /// machine.
    /// </summary>
    /// <param name="credentialFileName">The credential file's name, so the message names the file the operator is
    /// looking at; <see cref="DarlingManagedPostgres.CredentialFileName"/> for the store owner's credential.</param>
    internal static string DescribeStoreCredentialDecryptFailure(string credentialFileName) =>
        "Could not DPAPI-decrypt the store credential. This is a Windows Data Protection failure on THIS machine, " +
        "not PostgreSQL rejecting a login. The store's credential file (" + credentialFileName + ", beside the " +
        "data directory) is encrypted with LocalMachine scope, so only the machine that wrote it can read it. If " +
        "the store was copied here from another machine, run this command on the machine that created it.";

    [SupportedOSPlatform("windows")]
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
    /// Dereferences an <c>env:</c>/<c>file:</c> reference held in a server's stored secret slot (#5240), after asking
    /// whether it names one of this service's own configuration files or secrets
    /// (<see cref="DarlingOwnedSecrets.ReferenceRefusal(string?)"/>), the question the app asks before it saves a
    /// reference. Asking it again here, where the reference is read, makes the rule hold for a slot however it was
    /// written, not only for a reference that came through the app.
    ///
    /// <para>A refused reference fails the way an unresolvable one does: an <see cref="InvalidOperationException"/>
    /// that names the setting, so the worker's connect path logs it, backs off and retries that one server, and
    /// nothing else stops. The text is the refusal's own sentence, which names no path and no variable, and the
    /// referenced value is never read.</para>
    /// </summary>
    private static string ResolveStoredReference(string reference, string settingName, bool declaredByFile)
    {
        /* One exception to the refusal: darling.json itself declares this same reference, in this same slot, for this same
           server id (MonitoredServer.EncryptedPasswordDeclaredByFile). The mark is set where the server is built, from the
           file, and never from a stored value, so a reference that is only in the store is still asked about. */
        if (!declaredByFile && DarlingOwnedSecrets.ReferenceRefusal(reference) is { } refusal)
        {
            throw new InvalidOperationException($"{settingName}: {refusal}");
        }

        return DarlingSecretSource.Resolve(reference, settingName);
    }

    /// <summary>
    /// Resolves a monitored server's SQL-auth password from the <c>encryptedPassword</c> slot: an <c>env:</c>/<c>file:</c>
    /// reference first, then a sealed value (any <c>sealed:</c> version), then a legacy DPAPI value, then the
    /// <c>password</c> slot, which since #1804 may be a reference rather than a literal.
    /// <paramref name="usedPlaintext"/> is true only for a LITERAL (a reference is not plaintext-in-config, so callers
    /// do not warn on it). A reference held in the stored slot that names one of this service's own secrets is refused
    /// before it resolves (<see cref="ResolveStoredReference"/>). A sealed value opens only through the key ring and only
    /// for the connection it was saved for; a DPAPI value opens only on Windows, and only when darling.json declares it
    /// or a pin taken at upgrade still matches the row (#5366). Every refusal throws before any credential is sent.
    /// </summary>
    public static string ResolvePassword(MonitoredServer server, out bool usedPlaintext, IPasswordKeyRing? ring = null) =>
        ResolvePassword(server, out usedPlaintext, ring ?? DarlingPasswordKey.Current, LegacyDpapi.Current);

    internal static string ResolvePassword(
        MonitoredServer server, out bool usedPlaintext, IPasswordKeyRing ring, LegacyDpapi dpapi)
    {
        if (server is null)
        {
            throw new ArgumentNullException(nameof(server));
        }

        if (!string.IsNullOrWhiteSpace(server.EncryptedPassword))
        {
            usedPlaintext = false;

            /* #2087: add_servers stores env:/file: REFERENCES verbatim in this slot on Linux (a pointer is
               not a secret). A reference can never be confused with a DPAPI blob or a sealed value: blobs are
               base64 and a sealed value starts with "sealed:". */
            if (DarlingSecretSource.IsReference(server.EncryptedPassword))
            {
                return ResolveStoredReference(
                    server.EncryptedPassword, $"servers['{server.DisplayName}'].encryptedPassword", server.EncryptedPasswordDeclaredByFile);
            }

            /* #2255: the raw CryptographicException ("Key not valid for use in specified state") reached the
               worker's connect-retry warning verbatim and repeated every 60s with no way to act on it. Server
               identity is only known HERE, so this is where it gets attached. */
            return OpenSaved(
                server.EncryptedPassword, $"The saved password for server '{server.DisplayName}'", () => server.SecretBinding,
                server.EncryptedPasswordDeclaredByFile, server.SecretPin, ring, dpapi,
                $"the stored password for server '{server.DisplayName}' (servers[].encryptedPassword)");
        }

        if (!string.IsNullOrWhiteSpace(server.Password))
        {
            usedPlaintext = !DarlingSecretSource.IsReference(server.Password);
            return DarlingSecretSource.Resolve(server.Password, $"servers['{server.DisplayName}'].password");
        }

        throw new InvalidOperationException(
            $"Server '{server.DisplayName}' requires a secret (a SQL password or a service-principal client secret) but has neither encryptedPassword nor password.");
    }

    /// <summary>
    /// Resolves a server's REMEDIATION credential password (#2138 phase 1) — the second, opt-in identity a
    /// write to a monitored server travels on. Same shapes <see cref="ResolvePassword(MonitoredServer, out bool, IPasswordKeyRing?)"/>
    /// accepts for its stored slot, minus the plaintext one.
    ///
    /// <para>Returns <b>null</b> for an unarmed server rather than throwing, and that asymmetry with
    /// <see cref="ResolvePassword(MonitoredServer, out bool, IPasswordKeyRing?)"/> is the point. A missing monitoring password is a
    /// misconfiguration — the operator declared sql auth and left the secret out — so it throws. A missing remediation password is
    /// the SHIPPED STATE of every server: nothing has gone wrong, this server simply has no phase-1
    /// surface. Making it throw would turn the normal case into an exception, and an exception in the normal
    /// case is a thing callers learn to swallow.</para>
    ///
    /// <para>There is no plaintext arm. <see cref="MonitoredServer.Password"/>'s dev-convenience slot has no
    /// remediation counterpart: a wrong monitoring password fails a read, and a wrong remediation password
    /// fails a write against a production server, so the convenience is not worth the same money. An
    /// <c>env:</c>/<c>file:</c> reference is still accepted — a pointer is not a secret. One that names this
    /// service's own configuration files or secrets is refused before it resolves.</para>
    ///
    /// <para>A sealed or DPAPI refusal DOES throw: an armed server whose value will not open is a real fault.</para>
    /// </summary>
    public static string? ResolveRemediationPassword(MonitoredServer server, IPasswordKeyRing? ring = null) =>
        ResolveRemediationPassword(server, ring ?? DarlingPasswordKey.Current, LegacyDpapi.Current);

    internal static string? ResolveRemediationPassword(MonitoredServer server, IPasswordKeyRing ring, LegacyDpapi dpapi)
    {
        if (server is null)
        {
            throw new ArgumentNullException(nameof(server));
        }

        /* Both halves or nothing — HasRemediationCredential, not just the blob. A blob with no username
           cannot build a connection string, and resolving its secret first would decrypt a credential to
           then discover it is unusable. */
        if (!server.HasRemediationCredential)
        {
            return null;
        }

        var blob = server.RemediationEncryptedPassword!;

        if (DarlingSecretSource.IsReference(blob))
        {
            return ResolveStoredReference(
                blob, $"servers['{server.DisplayName}'].remediationEncryptedPassword", server.RemediationEncryptedPasswordDeclaredByFile);
        }

        return OpenSaved(
            blob, $"The saved remediation password for server '{server.DisplayName}'", () => server.RemediationBinding,
            server.RemediationEncryptedPasswordDeclaredByFile, server.RemediationPin, ring, dpapi,
            $"the stored REMEDIATION password for server '{server.DisplayName}' (servers[].remediationEncryptedPassword)");
    }

    /// <summary>
    /// Resolves the SMTP password held in <c>smtp.encryptedPassword</c> by the same dispatch as a server's: a reference,
    /// then a sealed value, then DPAPI on Windows when declared or pinned (#5366).
    /// </summary>
    public static string ResolveSmtpPassword(SmtpConfig smtp, IPasswordKeyRing? ring = null) =>
        ResolveSmtpPassword(smtp, ring ?? DarlingPasswordKey.Current, LegacyDpapi.Current);

    internal static string ResolveSmtpPassword(SmtpConfig smtp, IPasswordKeyRing ring, LegacyDpapi dpapi)
    {
        if (smtp is null)
        {
            throw new ArgumentNullException(nameof(smtp));
        }

        var stored = smtp.EncryptedPassword!;
        if (DarlingSecretSource.IsReference(stored))
        {
            return ResolveStoredReference(stored, "smtp.encryptedPassword", smtp.EncryptedPasswordDeclaredByFile);
        }

        return OpenSaved(
            stored, "The saved SMTP password", () => smtp.SecretBinding, smtp.EncryptedPasswordDeclaredByFile, smtp.SecretPin,
            ring, dpapi, "the stored SMTP password (smtp.encryptedPassword)");
    }

    /// <summary>
    /// Whether a secret slot read from darling.json holds something the file declares: a reference or an old-format value, not
    /// blank and not a sealed value (a sealed value opens through the key ring, whoever wrote it).
    /// </summary>
    internal static bool DeclaresSecretText(string? stored) =>
        !string.IsNullOrWhiteSpace(stored) && !PasswordSeal.IsSealed(stored);

    /// <summary>True for stored text that is neither blank, a reference, nor a sealed value: the old DPAPI format.</summary>
    internal static bool IsLegacyDpapi(string? stored) =>
        !string.IsNullOrWhiteSpace(stored) && !DarlingSecretSource.IsReference(stored) && !PasswordSeal.IsSealed(stored);

    /// <summary>
    /// Whether a legacy DPAPI value can be opened here: Windows, and either darling.json declares it or the pin taken at
    /// upgrade still matches the row. The config load counts the values this returns false for.
    /// </summary>
    internal static bool LegacyValueUsable(
        string stored, bool declaredByFile, LegacyPin? pin, Func<PasswordBinding> bindingFor, LegacyDpapi dpapi)
    {
        if (!dpapi.IsWindows)
        {
            return false;
        }

        if (declaredByFile)
        {
            return true;
        }

        try
        {
            return pin is not null && PinMatches(pin, stored, bindingFor());
        }
        catch (ArgumentException)
        {
            return false;
        }
    }

    private static string OpenSaved(
        string stored, string subject, Func<PasswordBinding> bindingFor, bool declaredByFile, LegacyPin? pin,
        IPasswordKeyRing ring, LegacyDpapi dpapi, string decryptWhat)
    {
        if (PasswordSeal.IsSealed(stored))
        {
            /* A ring that cannot open anything says why (not loaded yet, or refused). Asking it to open would
               report an unknown key, which is not what is wrong. */
            if (!ring.Status.CanSeal)
            {
                throw new InvalidOperationException(ring.Status.Reason ?? DarlingPasswordKey.NotReadyReason);
            }

            try
            {
                return ring.Open(stored, bindingFor());
            }
            catch (PasswordSealException ex)
            {
                throw new InvalidOperationException(SealFailureText(subject, ex));
            }
            catch (ArgumentException)
            {
                /* A connection field that is not valid text: the value cannot be opened for this connection. */
                throw new InvalidOperationException(BindingText(subject));
            }
        }

        if (!dpapi.IsWindows)
        {
            throw new InvalidOperationException(
                $"{subject} was saved with Windows DPAPI on another machine and cannot be read here. Enter it again.");
        }

        if (!LegacyValueUsable(stored, declaredByFile, pin, bindingFor, dpapi))
        {
            throw new InvalidOperationException(
                $"{subject} is in the old format and was saved after this service was upgraded, or its connection changed. " +
                "Update the Darling Viewer, then enter the password again.");
        }

        try
        {
            return dpapi.Unprotect(stored);
        }
        catch (CryptographicException ex)
        {
            throw new InvalidOperationException(DescribeDecryptFailure(decryptWhat), ex);
        }
    }

    private static string BindingText(string subject) =>
        $"{subject} was saved for a different connection, or was changed. Enter the password again.";

    private static string SealFailureText(string subject, PasswordSealException ex) => ex.Kind switch
    {
        PasswordSealFailure.UnknownKey =>
            $"{subject} was sealed to password key {PasswordSeal.DisplayKeyId(ex.KeyId ?? "")}, which this service does not have. " +
            "Restore the credentials volume, or enter the password again.",
        PasswordSealFailure.NewerVersion => $"{subject} was saved by a newer version of Darling.",
        _ => BindingText(subject),
    };

    private static bool PinMatches(LegacyPin pin, string stored, PasswordBinding binding) =>
        CryptographicOperations.FixedTimeEquals(pin.ValueSha256, SHA256.HashData(Encoding.UTF8.GetBytes(stored)))
        && CryptographicOperations.FixedTimeEquals(pin.BindingSha256, binding.LegacyPinHash());
}

/// <summary>A legacy DPAPI value's pin as read from <c>config.legacy_secret_pin</c>: the hash of the stored text and the hash of the
/// connection it was pinned to.</summary>
internal sealed record LegacyPin(byte[] ValueSha256, byte[] BindingSha256);

/// <summary>What the resolver may do with a legacy DPAPI value on this machine: whether it is Windows and how to open one. Tests pass their own.</summary>
internal sealed record LegacyDpapi(bool IsWindows, Func<string, string> Unprotect)
{
    internal static LegacyDpapi Current =>
        OperatingSystem.IsWindows()
            ? OnWindows()
            : new LegacyDpapi(false, _ => throw new PlatformNotSupportedException("DPAPI requires Windows."));

    [SupportedOSPlatform("windows")]
    private static LegacyDpapi OnWindows() => new(true, blob => DarlingSecrets.Unprotect(blob));
}
