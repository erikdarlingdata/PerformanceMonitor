/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Versioning;
using System.Security.AccessControl;
using System.Security.Principal;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Restrictive Windows ACLs for the DPAPI credential files (V8 security hardening, #1262). DPAPI
/// LocalMachine scope is deliberate — the service (a service account) writes the credential and a
/// DIFFERENT interactive user's Viewer reads it — so the machine-bound blob is decryptable by any
/// local code that can READ the file, and the in-source entropy is no secret. The file ACL is
/// therefore the real access boundary: without it, any local user could read
/// <c>pg-credential.dpapi</c>, unprotect it, and connect as the <c>darling</c> SUPERUSER
/// (DROP / exfil / <c>COPY … TO PROGRAM</c> = RCE as the service account). This class strips the
/// inherited world-readable access and re-grants it narrowly.
///
/// <para><b>Principal model (the assumption to review):</b>
/// <list type="bullet">
/// <item>The <b>superuser</b> credential (<c>pg-credential.dpapi</c>) and the transient init password
/// file are readable ONLY by <c>SYSTEM</c>, <c>Administrators</c>, and the service account — never an
/// interactive user. Post-split the Viewer no longer needs the superuser credential (it connects as
/// admin/viewer), so the RCE vector is locked hardest.</item>
/// <item>The <b>admin/viewer</b> credentials additionally grant read to <c>NT AUTHORITY\INTERACTIVE</c>.
/// The Viewer runs as the interactive operator, whose identity is unknown at provisioning time;
/// INTERACTIVE is the concrete principal that lets that Viewer read the credential with zero config
/// while excluding exactly the non-interactive local attack primitives the design's threat model
/// calls out (services, SSRF/sandboxed socket code, scheduled tasks — none hold an interactive token).</item>
/// </list>
/// On the design's default single-operator VM, INTERACTIVE == the operator, so this is tight. On a
/// shared machine where untrusted users can log on interactively they could read the admin/viewer
/// credentials (never the superuser one); the tightening path there is a configured operator account
/// ACL'd specifically — a future knob, not built for the Phase 1 single-operator default.</para>
/// </summary>
[SupportedOSPlatform("windows")]
public static class DarlingFileSecurity
{
    private static SecurityIdentifier LocalSystem => new(WellKnownSidType.LocalSystemSid, null);
    private static SecurityIdentifier Administrators => new(WellKnownSidType.BuiltinAdministratorsSid, null);
    private static SecurityIdentifier Interactive => new(WellKnownSidType.InteractiveSid, null);

    /// <summary>The three "anyone on this box" groups a secret-bearing file must never grant read to
    /// (<see cref="IsReadableByOrdinaryUsers"/>). <c>BUILTIN\Users</c> is the one that actually bites: a folder
    /// created directly under <c>C:\</c> — the documented install location — inherits Read &amp; Execute for it
    /// from the root DACL.</summary>
    private static SecurityIdentifier[] OrdinaryUserGroups =>
    [
        new(WellKnownSidType.BuiltinUsersSid, null),
        new(WellKnownSidType.AuthenticatedUserSid, null),
        new(WellKnownSidType.WorldSid, null),
    ];

    /* #2371: set by --harden-files to the account the SERVICE is registered under, because that verb is the
       one caller that is deliberately NOT the service. Null everywhere else, so the in-service callers keep
       resolving themselves exactly as before. */
    private static SecurityIdentifier? _serviceAccountOverride;

    /// <summary>
    /// Harden FOR <paramref name="account"/> rather than for whoever is running this process (#2371).
    ///
    /// <para>Every original caller of this class runs INSIDE the service, so "the current identity" and "the
    /// account the service runs as" were the same value and the distinction did not exist. <c>--harden-files</c>
    /// breaks that: it exists precisely because a virtual service account cannot re-ACL a file it does not own,
    /// so it is always run by somebody else — and resolving from the caller there grants the OPERATOR and drops
    /// the service, which is a working install turned into one that fails on its next start.</para>
    /// </summary>
    public static void HardenForAccount(SecurityIdentifier account) => _serviceAccountOverride = account;

    /// <summary>
    /// The account the service is REGISTERED under, read from its SCM entry rather than from this process —
    /// `ObjectName` is what the SCM logs the service on with. Returns null when the service is not registered
    /// (a console run, or hardening a tree before install), leaving the caller to fall back.
    ///
    /// <para>Handles the well-known aliases the SCM stores unqualified: <c>LocalSystem</c> has no
    /// <see cref="NTAccount"/> spelling to translate, while a virtual account (<c>NT SERVICE\…</c>), a domain
    /// account and a gMSA all translate directly.</para>
    /// </summary>
    public static SecurityIdentifier? RegisteredServiceAccount(string serviceName)
    {
        try
        {
            using var key = Microsoft.Win32.Registry.LocalMachine.OpenSubKey(
                $@"SYSTEM\CurrentControlSet\Services\{serviceName}");

            if (key?.GetValue("ObjectName") is not string account || string.IsNullOrWhiteSpace(account))
            {
                return null;
            }

            account = account.Trim();

            return account.Equals("LocalSystem", StringComparison.OrdinalIgnoreCase)
                ? new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null)
                : (SecurityIdentifier)new NTAccount(account).Translate(typeof(SecurityIdentifier));
        }
        catch (Exception)
        {
            /* Same reasoning as the display name below: a resolution failure must degrade to the old
               behaviour, never take the harden down. */
            return null;
        }
    }

    /// <summary>
    /// Whether <paramref name="path"/> still grants the account being hardened for. The harden's own check is
    /// <see cref="IsReadableByOrdinaryUsers"/>, which asks whether anyone TOO MANY can read — this is the
    /// other half, and the one #2371 needed: an ACL can be perfectly private and still lock the service out
    /// of its own credentials.
    /// </summary>
    public static bool GrantsHardenedAccount(string path)
    {
        try
        {
            var target = ServiceAccount;
            var rules = (Directory.Exists(path)
                ? new DirectoryInfo(path).GetAccessControl().GetAccessRules(true, true, typeof(SecurityIdentifier))
                : new FileInfo(path).GetAccessControl().GetAccessRules(true, true, typeof(SecurityIdentifier)));

            foreach (FileSystemAccessRule rule in rules)
            {
                if (rule.AccessControlType == AccessControlType.Allow
                    && rule.IdentityReference is SecurityIdentifier sid
                    && sid.Equals(target)
                    && (rule.FileSystemRights & FileSystemRights.Read) != 0)
                {
                    return true;
                }
            }

            return false;
        }
        catch (Exception)
        {
            /* Unreadable ACL is not proof of absence, so do not report a lockout we cannot see. */
            return true;
        }
    }

    /// <summary>The account to harden FOR: the registered service account when
    /// <see cref="HardenForAccount"/> named one, otherwise the account this process runs as.</summary>
    private static SecurityIdentifier ServiceAccount =>
        _serviceAccountOverride
            ?? WindowsIdentity.GetCurrent().User
            ?? throw new InvalidOperationException("Cannot resolve the current Windows identity for ACL hardening.");

    /// <summary>
    /// The current identity as a display name for remediation log lines: <c>NT SERVICE\PerformanceMonitor
    /// Darling</c> on a default install, <c>DOMAIN\svc-account</c> when an operator re-homed the service to a
    /// domain account or gMSA for integrated auth (#1802, #1823). The <c>icacls /grant</c> a harden failure
    /// prints must name the account the service RUNS AS — granting the virtual account on a re-homed install
    /// is a fix that cannot work, handed to the one operator who needs it. Falls back to the default virtual
    /// account name rather than throwing: a remediation string must never itself take the log line down.
    /// </summary>
    public static string ServiceAccountDisplayName
    {
        get
        {
            try
            {
                /* #2371: when hardening for the REGISTERED account, name that one — printing the caller here
                   would describe an ACL the harden is not writing. */
                return _serviceAccountOverride is not null
                    ? ((NTAccount)_serviceAccountOverride.Translate(typeof(NTAccount))).Value
                    : WindowsIdentity.GetCurrent().Name;
            }
            catch (Exception)
            {
                return @"NT SERVICE\PerformanceMonitor Darling";
            }
        }
    }

    /// <summary>
    /// Locks a directory to SYSTEM + Administrators + the service account (full control, inherited by
    /// children), removing ALL inherited access so no Users / Authenticated Users read survives. When
    /// <paramref name="allowInteractiveTraverse"/> is set, INTERACTIVE gets traverse (not list) on THIS
    /// folder only — so the operator's Viewer can reach the admin/viewer credential files beside the
    /// data directory without that access flowing into the data-directory subtree.
    /// </summary>
    public static void HardenDirectory(string path, bool allowInteractiveTraverse)
    {
        var info = new DirectoryInfo(path);
        var security = new DirectorySecurity();

        /* Protect the DACL (isProtected: true) and drop inherited ACEs (preserveInheritance: false):
           the directory's access is now EXACTLY the rules added below, nothing world-readable. */
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);

        const InheritanceFlags subtree = InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit;
        AddFull(security, LocalSystem, subtree);
        AddFull(security, Administrators, subtree);
        AddFull(security, ServiceAccount, subtree);

        if (allowInteractiveTraverse)
        {
            /* Traverse + synchronize, THIS FOLDER ONLY (no inheritance) — the operator can open a
               known credential path here but cannot enumerate the folder or descend into the PG data
               directory. */
            security.AddAccessRule(new FileSystemAccessRule(
                Interactive,
                FileSystemRights.Traverse | FileSystemRights.Synchronize,
                InheritanceFlags.None,
                PropagationFlags.None,
                AccessControlType.Allow));
        }

        info.SetAccessControl(security);
    }

    /// <summary>
    /// Locks a file to SYSTEM + Administrators + the service account, dropping inherited access. When
    /// <paramref name="allowInteractiveRead"/> is set (the admin/viewer credentials the Viewer reads),
    /// INTERACTIVE gets read. The superuser credential and the transient init pwfile pass false.
    /// </summary>
    public static void HardenFile(string path, bool allowInteractiveRead)
    {
        var info = new FileInfo(path);
        var security = new FileSecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);

        AddFile(security, LocalSystem, FileSystemRights.FullControl);
        AddFile(security, Administrators, FileSystemRights.FullControl);
        AddFile(security, ServiceAccount, FileSystemRights.FullControl);

        if (allowInteractiveRead)
        {
            AddFile(security, Interactive, FileSystemRights.Read | FileSystemRights.Synchronize);
        }

        info.SetAccessControl(security);
    }

    /// <summary>
    /// Is an existing credential file owned by a trusted principal (SYSTEM, Administrators, or the
    /// service account)? A file owned by anyone else may have been PRE-PLANTED — for a role credential,
    /// by an attacker who wrote a password they know so the service's <c>ALTER ROLE … PASSWORD</c>
    /// re-assert would hand them that login; for the owner credential, a tamper signal. Callers must not
    /// trust an untrusted-owned file. Returns false (untrusted) on any error reading the owner.
    /// </summary>
    public static bool IsTrustedOwner(string path)
    {
        try
        {
            var owner = new FileInfo(path).GetAccessControl().GetOwner(typeof(SecurityIdentifier)) as SecurityIdentifier;
            return owner is not null
                && (owner.Equals(LocalSystem) || owner.Equals(Administrators) || owner.Equals(ServiceAccount));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>
    /// Can an ordinary local user READ this file's bytes? True when the effective DACL carries an Allow ACE
    /// granting <see cref="FileSystemRights.ReadData"/> to <c>Users</c>, <c>Authenticated Users</c>, or
    /// <c>Everyone</c> — explicit or inherited. The verification half of <see cref="HardenFile"/>: for
    /// LocalMachine-DPAPI content (the credential files, and <c>darling.json</c> since #1647) read access IS
    /// the secret, so the caller reports a still-readable file as Critical rather than assuming the harden took.
    ///
    /// <para>Checks <see cref="FileSystemRights.ReadData"/> specifically, NOT the composite
    /// <see cref="FileSystemRights.Read"/>: the latter ORs in ReadPermissions/ReadAttributes/
    /// ReadExtendedAttributes, so a mask test against it would call a metadata-only grant "readable" and
    /// train operators to ignore the alarm.</para>
    ///
    /// <para>Returns false when the DACL itself cannot be read — the harden attempt the caller just made
    /// already logs loudly on failure, and a Critical raised on an unreadable DACL would be noise, not signal.</para>
    /// </summary>
    public static bool IsReadableByOrdinaryUsers(string path)
    {
        try
        {
            var rules = new FileInfo(path)
                .GetAccessControl()
                .GetAccessRules(includeExplicit: true, includeInherited: true, typeof(SecurityIdentifier));

            var ordinary = OrdinaryUserGroups;
            foreach (FileSystemAccessRule rule in rules)
            {
                if (rule.AccessControlType != AccessControlType.Allow
                    || (rule.FileSystemRights & FileSystemRights.ReadData) != FileSystemRights.ReadData
                    || rule.IdentityReference is not SecurityIdentifier sid)
                {
                    continue;
                }

                foreach (var group in ordinary)
                {
                    if (sid.Equals(group))
                    {
                        return true;
                    }
                }
            }

            return false;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException)
        {
            return false;
        }
    }

    /// <summary>
    /// Who OWNS this path and which ordinary-user group can read it — the two facts a harden failure needs and
    /// that "fix the file permissions by hand" leaves the operator to discover.
    ///
    /// <para>The owner is the load-bearing half, because it usually IS the root cause: <c>SetAccessControl</c>
    /// needs WRITE_DAC, which comes with ownership or FullControl, so a service account holding only inherited
    /// Modify on a file owned by someone else can NEVER re-ACL it. That failure is permanent, not transient, and
    /// no amount of restarting fixes it — which is exactly what an operator reading "fix the permissions by hand"
    /// cannot tell. Observed on a field box: owner <c>BUILTIN\Administrators</c>, service account with Modify,
    /// the same error every start for a day.</para>
    ///
    /// <para>Returns a short parenthetical for log interpolation, or an empty string when neither fact can be
    /// read — a diagnostic must never itself throw inside a catch block.</para>
    /// </summary>
    public static string DescribeOwnerAndExposure(string path)
    {
        string? owner = null;
        string? readable = null;

        try
        {
            var security = new FileInfo(path).GetAccessControl();
            owner = security.GetOwner(typeof(NTAccount))?.Value;

            foreach (FileSystemAccessRule rule in security.GetAccessRules(includeExplicit: true, includeInherited: true, typeof(SecurityIdentifier)))
            {
                if (rule.AccessControlType != AccessControlType.Allow
                    || (rule.FileSystemRights & FileSystemRights.ReadData) != FileSystemRights.ReadData
                    || rule.IdentityReference is not SecurityIdentifier sid)
                {
                    continue;
                }

                foreach (var group in OrdinaryUserGroups)
                {
                    if (sid.Equals(group))
                    {
                        readable = TranslateOrRaw(sid);
                        break;
                    }
                }

                if (readable is not null)
                {
                    break;
                }
            }
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException or IdentityNotMappedException)
        {
            _ = ex;
        }

        if (owner is null && readable is null)
        {
            return string.Empty;
        }

        var parts = new List<string>(2);
        if (owner is not null)
        {
            parts.Add($"owner is {owner}");
        }

        if (readable is not null)
        {
            parts.Add($"readable by {readable}");
        }

        return $" ({string.Join("; ", parts)})";
    }

    /// <summary>A SID's friendly name, falling back to the raw SID when it does not map (a deleted or
    /// cross-domain principal must still appear in the message rather than vanishing).</summary>
    private static string TranslateOrRaw(SecurityIdentifier sid)
    {
        try
        {
            return ((NTAccount)sid.Translate(typeof(NTAccount))).Value;
        }
        catch (IdentityNotMappedException)
        {
            return sid.Value;
        }
    }

    private static void AddFull(DirectorySecurity security, SecurityIdentifier sid, InheritanceFlags inheritance) =>
        security.AddAccessRule(new FileSystemAccessRule(
            sid, FileSystemRights.FullControl, inheritance, PropagationFlags.None, AccessControlType.Allow));

    private static void AddFile(FileSecurity security, SecurityIdentifier sid, FileSystemRights rights) =>
        security.AddAccessRule(new FileSystemAccessRule(sid, rights, AccessControlType.Allow));
}
