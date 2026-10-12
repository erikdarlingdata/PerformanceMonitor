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
/// <item>The <b>admin/viewer</b> credentials, and <c>darling.json</c>, additionally grant read to the local
/// group <see cref="OperatorGroupName"/>. The Viewer and the CLI run as the interactive operator, whose identity
/// is unknown at provisioning time; the group is the concrete principal an administrator fills with exactly the
/// people who run them. It replaced <c>NT AUTHORITY\INTERACTIVE</c>, which every account that logs on to the
/// machine holds: on a jump host or a terminal server that grant handed every user the admin store credential,
/// and with it the whole store.</item>
/// </list>
/// The installer creates the group (empty) and the service grants it on every start. When the group does not exist
/// (a hand install) nothing replaces it: the files stay readable by SYSTEM, Administrators and the service account
/// only, and the Viewer and the CLI fail to read them until the group is created and the service restarted.</para>
/// </summary>
[SupportedOSPlatform("windows")]
public static class DarlingFileSecurity
{
    private static SecurityIdentifier LocalSystem => new(WellKnownSidType.LocalSystemSid, null);
    private static SecurityIdentifier Administrators => new(WellKnownSidType.BuiltinAdministratorsSid, null);

    /// <summary>
    /// The local group whose members may read <c>darling.json</c> and the admin and viewer store credentials:
    /// the people who run the Viewer or the CLI without elevation. Named after the service's own account
    /// (<c>NT SERVICE\PerformanceMonitor Darling</c>) plus a role, the way Windows names <c>Performance Log
    /// Users</c>, so it sorts beside the service in Local Users and Groups. The Viewer carries a copy of this
    /// string, because it does not reference this assembly.
    /// </summary>
    public const string OperatorGroupName = DarlingOperatorGroup.Name;

    /// <summary>The four "anyone on this box" groups a secret-bearing file must never grant read to
    /// (<see cref="IsReadableByOrdinaryUsers"/>). <c>BUILTIN\Users</c> is the one that actually bites: a folder
    /// created directly under <c>C:\</c> — the documented install location — inherits Read &amp; Execute for it
    /// from the root DACL. <c>INTERACTIVE</c> is here because every account that logs on holds it: an install
    /// hardened before the operator group existed granted it, and a file that still does is as readable as one
    /// that grants Users.</summary>
    private static SecurityIdentifier[] OrdinaryUserGroups =>
    [
        new(WellKnownSidType.BuiltinUsersSid, null),
        new(WellKnownSidType.AuthenticatedUserSid, null),
        new(WellKnownSidType.WorldSid, null),
        new(WellKnownSidType.InteractiveSid, null),
    ];

    /// <summary>
    /// The <c>icacls</c> argument that removes every explicit grant <see cref="IsReadableByOrdinaryUsers"/> flags
    /// (Users, Authenticated Users, Everyone and INTERACTIVE), by SID so it works on any display language. Built from
    /// the same list as the check, so a fix text that uses it names everything the check can flag.
    /// </summary>
    public static string RemoveOrdinaryUserGrantsArgument =>
        "/remove:g " + string.Join(" ", Array.ConvertAll(OrdinaryUserGroups, group => "*" + group.Value));

    /* A test cannot create a local group on the build machine, so it substitutes a SID that exists (or null for
       "no group"). Null everywhere else, so the service resolves the real group. */
    private static Func<SecurityIdentifier?>? _operatorGroupOverride;

    /// <summary>Substitutes <paramref name="resolver"/> for the real group lookup; null restores it.</summary>
    internal static void UseOperatorGroupForTests(Func<SecurityIdentifier?>? resolver) => _operatorGroupOverride = resolver;

    /// <summary>
    /// The SID of <see cref="OperatorGroupName"/>, or null when no such group exists on this machine. Looked up on
    /// every call, never cached: the service runs as a virtual account and cannot create the group, so an
    /// administrator creates it after the fact and the next harden (the next service start) picks it up.
    /// </summary>
    public static SecurityIdentifier? OperatorGroup
    {
        get
        {
            if (_operatorGroupOverride is not null)
            {
                return _operatorGroupOverride();
            }

            try
            {
                /* Machine-qualified: an unqualified name is also looked up in the domain and in trusted domains, and
                   a group of that name made there must never be the one that is granted access. */
                var sid = (SecurityIdentifier)new NTAccount(Environment.MachineName, OperatorGroupName)
                    .Translate(typeof(SecurityIdentifier));
                return IsLocalGroup(sid, DomainOf) ? sid : null;
            }
            catch (Exception ex) when (ex is IdentityNotMappedException or SystemException)
            {
                return null;
            }
        }
    }

    private static string? DomainOf(SecurityIdentifier sid)
    {
        var name = ((NTAccount)sid.Translate(typeof(NTAccount))).Value;
        var slash = name.IndexOf('\\', StringComparison.Ordinal);
        return slash < 0 ? null : name[..slash];
    }

    /// <summary>
    /// Is <paramref name="sid"/> a group of this machine's own account database: its domain part is the machine
    /// name or BUILTIN? The qualified lookup already asks only that database, and this checks the answer as well, so
    /// a name that resolves to a domain group is refused rather than granted.
    /// </summary>
    internal static bool IsLocalGroup(SecurityIdentifier sid, Func<SecurityIdentifier, string?> domainOf)
    {
        string? domain;
        try
        {
            domain = domainOf(sid);
        }
        catch (Exception ex) when (ex is IdentityNotMappedException or SystemException)
        {
            return false;
        }

        return string.Equals(domain, Environment.MachineName, StringComparison.OrdinalIgnoreCase)
            || string.Equals(domain, "BUILTIN", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The warning a harden caller logs when <see cref="OperatorGroup"/> is null: what stays locked out, which
    /// install step creates the group, and what to do on a hand install. One line, so the log reads the same from
    /// every caller.
    /// </summary>
    public static string OperatorGroupMissingWarning =>
        $"The local group '{OperatorGroupName}' does not exist on this machine (a group of that name in a domain is never used), so darling.json and the store's admin " +
        "and viewer credentials are readable by SYSTEM, Administrators and the service account only, and the Viewer " +
        "and the CLI cannot read them unless run elevated. install-darling.ps1 step 4b creates the group; on a hand " +
        $"install create it from an elevated prompt (net localgroup \"{OperatorGroupName}\" /add), add the people who " +
        "run the Viewer or the CLI, and restart this service so it grants the group.";

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
    /// children), removing ALL inherited access so no Users / Authenticated Users / INTERACTIVE read survives.
    /// When <paramref name="allowInteractiveTraverse"/> is set and the operator group exists, the group gets
    /// traverse (not list) on THIS folder only — so the operator's Viewer can reach the admin/viewer credential
    /// files beside the data directory without that access flowing into the data-directory subtree. (The
    /// parameter keeps its old name: the grant is still "the interactive operator's", only the principal changed.)
    /// </summary>
    public static void HardenDirectory(string path, bool allowInteractiveTraverse) =>
        new DirectoryInfo(path).SetAccessControl(HardenedDirectorySecurity(allowInteractiveTraverse));

    /// <summary>The one ACL <see cref="HardenDirectory"/> and <see cref="HardenWithoutFollowingLinks"/> apply to a
    /// directory.</summary>
    private static DirectorySecurity HardenedDirectorySecurity(bool allowInteractiveTraverse)
    {
        var security = new DirectorySecurity();

        /* Protect the DACL (isProtected: true) and drop inherited ACEs (preserveInheritance: false):
           the directory's access is now EXACTLY the rules added below, nothing world-readable. */
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);

        const InheritanceFlags subtree = InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit;
        AddFull(security, LocalSystem, subtree);
        AddFull(security, Administrators, subtree);
        AddFull(security, ServiceAccount, subtree);

        if (allowInteractiveTraverse && OperatorGroup is { } operators)
        {
            /* Traverse + synchronize, THIS FOLDER ONLY (no inheritance) — the operator can open a
               known credential path here but cannot enumerate the folder or descend into the PG data
               directory. */
            security.AddAccessRule(new FileSystemAccessRule(
                operators,
                FileSystemRights.Traverse | FileSystemRights.Synchronize,
                InheritanceFlags.None,
                PropagationFlags.None,
                AccessControlType.Allow));
        }

        return security;
    }

    /// <summary>
    /// <see cref="HardenFile"/> or <see cref="HardenDirectory"/>, applied through a handle to exactly the object at
    /// <paramref name="path"/>, never through a junction or symbolic link (#4004 review, round 2). An ACL set by path
    /// follows every link on it, so a link planted where the path runs would have an elevated caller rewrite whatever
    /// it points at. The object is opened without following a link at its own name
    /// (<c>FILE_FLAG_OPEN_REPARSE_POINT</c>) and kept open while it is checked and changed, so what is checked is what
    /// is changed: it must not be a link itself, and its final path must be <paramref name="anchorDirectory"/>'s own
    /// final path followed by the same names, which a link anywhere between the two would change.
    ///
    /// <para>A hard link passes both of those (#4004 review, round 3): it is not a reparse point, and the final path is
    /// the name it was opened by. But the DACL belongs to the file, not the name, so hardening a planted
    /// <c>darling.json.bak-x</c> hard-linked to someone else's file would rewrite that file's access. A file with more
    /// than one name is refused.</para>
    /// </summary>
    /// <returns>Null once the ACL is applied; otherwise why it was not, and nothing was changed.</returns>
    public static string? HardenWithoutFollowingLinks(string path, bool isDirectory, bool allowInteractive, string anchorDirectory)
    {
        var anchor = Path.TrimEndingDirectorySeparator(Path.GetFullPath(anchorDirectory));
        var full = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        if (!DarlingPathContainment.IsStrictlyBelow(anchor, full))
        {
            throw new ArgumentException($"{full} is not below {anchor}.", nameof(path));
        }

        string anchorFinal;
        using (var anchorHandle = OpenForSecurity(anchor, followLink: true, access: FileReadAttributes))
        {
            anchorFinal = FinalPath(anchorHandle);
        }

        using var handle = OpenForSecurity(full, followLink: false, access: ReadControl | WriteDac | FileReadAttributes);
        if ((File.GetAttributes(handle) & FileAttributes.ReparsePoint) != 0)
        {
            return $"{full} is a junction or symbolic link";
        }

        if (!GetFileInformationByHandle(handle, out var information))
        {
            throw new System.ComponentModel.Win32Exception(System.Runtime.InteropServices.Marshal.GetLastPInvokeError(), $"{full} could not be checked for hard links");
        }

        if (information.NumberOfLinks > 1)
        {
            return $"{full} is a hard link: the file has {information.NumberOfLinks.ToString(System.Globalization.CultureInfo.InvariantCulture)} names, and its ACL is the same under every one of them";
        }

        /* Joined rather than concatenated: a volume root keeps its separator (C:\), and its final path can come back
           without one (a SUBST drive's root is a folder on another volume). */
        var relative = full[(Path.EndsInDirectorySeparator(anchor) ? anchor.Length : anchor.Length + 1)..];
        var expected = Path.Join(anchorFinal, relative);
        var actual = FinalPath(handle);
        if (!string.Equals(expected, actual, StringComparison.OrdinalIgnoreCase))
        {
            return $"{full} resolves to {actual}, so a junction or symbolic link is on its path";
        }

        NativeObjectSecurity security = isDirectory ? HardenedDirectorySecurity(allowInteractive) : HardenedFileSecurity(allowInteractive);
        var descriptor = new RawSecurityDescriptor(security.GetSecurityDescriptorBinaryForm(), 0);
        var dacl = new byte[descriptor.DiscretionaryAcl!.BinaryLength];
        descriptor.DiscretionaryAcl.GetBinaryForm(dacl, 0);
        var pinned = System.Runtime.InteropServices.GCHandle.Alloc(dacl, System.Runtime.InteropServices.GCHandleType.Pinned);
        try
        {
            var result = SetSecurityInfo(
                handle, FileObject, DaclSecurityInformation | ProtectedDaclSecurityInformation,
                IntPtr.Zero, IntPtr.Zero, pinned.AddrOfPinnedObject(), IntPtr.Zero);
            if (result != 0)
            {
                throw new System.ComponentModel.Win32Exception((int)result);
            }
        }
        finally
        {
            pinned.Free();
        }

        return null;
    }

    private const uint GenericRead = 0x80000000;
    private const uint ReadControl = 0x00020000;
    private const uint WriteDac = 0x00040000;
    private const uint FileReadAttributes = 0x00000080;
    private const uint FileFlagBackupSemantics = 0x02000000;
    private const uint FileFlagOpenReparsePoint = 0x00200000;
    private const int FileObject = 1;
    private const uint DaclSecurityInformation = 0x00000004;
    private const uint ProtectedDaclSecurityInformation = 0x80000000;

    /// <summary>Opens a file or directory (backup semantics) for its security. Holding the handle does not keep the
    /// name where it is: what protects the write is that the checks and the ACL change all go through this one
    /// handle, so they apply to the object that was opened, whatever the name points at afterwards.</summary>
    private static Microsoft.Win32.SafeHandles.SafeFileHandle OpenForSecurity(string path, bool followLink, uint access)
    {
        var handle = CreateFileW(
            path, access, FileShare.ReadWrite, IntPtr.Zero, FileMode.Open,
            FileFlagBackupSemantics | (followLink ? 0 : FileFlagOpenReparsePoint), IntPtr.Zero);
        if (handle.IsInvalid)
        {
            var error = System.Runtime.InteropServices.Marshal.GetLastPInvokeError();
            handle.Dispose();
            throw new System.ComponentModel.Win32Exception(error, $"{path} could not be opened");
        }

        return handle;
    }

    /// <summary>The path the system resolves an open handle to, without the <c>\\?\</c> prefix.</summary>
    private static string FinalPath(Microsoft.Win32.SafeHandles.SafeFileHandle handle)
    {
        var buffer = new char[1024];
        var length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Length, 0);
        if (length > buffer.Length)
        {
            buffer = new char[length];
            length = GetFinalPathNameByHandleW(handle, buffer, (uint)buffer.Length, 0);
        }

        if (length == 0 || length > buffer.Length)
        {
            throw new System.ComponentModel.Win32Exception(System.Runtime.InteropServices.Marshal.GetLastPInvokeError());
        }

        var path = new string(buffer, 0, (int)length);
        return path.StartsWith(@"\\?\UNC\", StringComparison.Ordinal) ? @"\\" + path[8..]
            : path.StartsWith(@"\\?\", StringComparison.Ordinal) ? path[4..]
            : path;
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern Microsoft.Win32.SafeHandles.SafeFileHandle CreateFileW(
        string lpFileName, uint dwDesiredAccess, FileShare dwShareMode, IntPtr lpSecurityAttributes,
        FileMode dwCreationDisposition, uint dwFlagsAndAttributes, IntPtr hTemplateFile);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern uint GetFinalPathNameByHandleW(
        Microsoft.Win32.SafeHandles.SafeFileHandle hFile, [System.Runtime.InteropServices.Out] char[] lpszFilePath, uint cchFilePath, uint dwFlags);

    [System.Runtime.InteropServices.DllImport("advapi32.dll", SetLastError = true)]
    private static extern uint SetSecurityInfo(
        Microsoft.Win32.SafeHandles.SafeFileHandle handle, int objectType, uint securityInfo,
        IntPtr psidOwner, IntPtr psidGroup, IntPtr pDacl, IntPtr pSacl);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool GetFileInformationByHandle(
        Microsoft.Win32.SafeHandles.SafeFileHandle hFile, out ByHandleFileInformation lpFileInformation);

    /// <summary><c>BY_HANDLE_FILE_INFORMATION</c>: each <c>FILETIME</c> as its two <c>DWORD</c> halves, so nothing
    /// realigns the struct to 8 bytes.</summary>
    [System.Runtime.InteropServices.StructLayout(System.Runtime.InteropServices.LayoutKind.Sequential)]
    private struct ByHandleFileInformation
    {
        public uint FileAttributes;
        public uint CreationTimeLow;
        public uint CreationTimeHigh;
        public uint LastAccessTimeLow;
        public uint LastAccessTimeHigh;
        public uint LastWriteTimeLow;
        public uint LastWriteTimeHigh;
        public uint VolumeSerialNumber;
        public uint FileSizeHigh;
        public uint FileSizeLow;
        public uint NumberOfLinks;
        public uint FileIndexHigh;
        public uint FileIndexLow;
    }


    /// <summary>
    /// Locks a file to SYSTEM + Administrators + the service account, dropping inherited access, and with it any
    /// INTERACTIVE entry an earlier version granted. When <paramref name="allowInteractiveRead"/> is set (the
    /// admin/viewer credentials and the config the Viewer and the CLI read) and the operator group exists, the
    /// group gets read; with no group, nothing else does. The superuser credential and the transient init pwfile
    /// pass false. (The parameter keeps its old name: the grant is still "the interactive operator's", only the
    /// principal changed.)
    /// </summary>
    public static void HardenFile(string path, bool allowInteractiveRead) =>
        new FileInfo(path).SetAccessControl(HardenedFileSecurity(allowInteractiveRead));

    /// <summary>
    /// Creates <paramref name="path"/>, which must not exist yet, with <see cref="HardenFile"/>'s ACL applied AT
    /// creation (#3914): the file never carries the access its folder would otherwise let it inherit, not even for
    /// the moment between a create and a harden, so a secret written through the returned stream is never readable
    /// by anyone the harden excludes. <see cref="FileMode.CreateNew"/>, so anything that appears at the path first
    /// makes the create fail rather than be written through.
    /// </summary>
    public static FileStream CreateHardenedFile(string path, bool allowInteractiveRead) =>
        new FileInfo(path).Create(
            FileMode.CreateNew, FileSystemRights.Write, FileShare.None, bufferSize: 4096, FileOptions.None,
            HardenedFileSecurity(allowInteractiveRead));

    /// <summary>The one ACL <see cref="HardenFile"/> and <see cref="CreateHardenedFile"/> both apply.</summary>
    private static FileSecurity HardenedFileSecurity(bool allowInteractiveRead)
    {
        var security = new FileSecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);

        AddFile(security, LocalSystem, FileSystemRights.FullControl);
        AddFile(security, Administrators, FileSystemRights.FullControl);
        AddFile(security, ServiceAccount, FileSystemRights.FullControl);

        if (allowInteractiveRead && OperatorGroup is { } operators)
        {
            AddFile(security, operators, FileSystemRights.Read | FileSystemRights.Synchronize);
        }

        return security;
    }

    /// <summary>
    /// Is an existing credential file owned by a trusted principal (SYSTEM, Administrators, or the
    /// service account)? A file owned by anyone else may have been PRE-PLANTED — for a role credential,
    /// by an attacker who wrote a password they know so the service's <c>ALTER ROLE … PASSWORD</c>
    /// re-assert would hand them that login; for the owner credential, a tamper signal. Callers must not
    /// trust an untrusted-owned file. Returns false (untrusted) on any error reading the owner. Works on a
    /// directory too: the store folder is judged the same way before the first initdb, because an ordinary user
    /// can create and own a folder under ProgramData before the service does.
    /// </summary>
    public static bool IsTrustedOwner(string path)
    {
        try
        {
            var owner = SecurityOf(path).GetOwner(typeof(SecurityIdentifier)) as SecurityIdentifier;
            return owner is not null
                && (owner.Equals(LocalSystem) || owner.Equals(Administrators) || owner.Equals(ServiceAccount));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary>
    /// The accounts beyond SYSTEM, Administrators and the service account that hold ANY Allow entry on
    /// <paramref name="path"/>, as one line for a refusal, or null when there are none (#4028). An ALLOWLIST, where
    /// <see cref="IsReadableByOrdinaryUsers"/> is a denylist of the four broad groups, which Domain Users, the
    /// operator group or one named user all pass. For the log-hash key, which nothing but the service reads; the credential
    /// files keep the denylist their existing DACLs were built against, so this cannot newly refuse an install's
    /// credentials. Unlike the denylist, a DACL that cannot be read is reported, not passed: this is the check a key
    /// is trusted on.
    ///
    /// <para>Any right counts, not only <see cref="FileSystemRights.ReadData"/> (#4044 review). An account holding
    /// WriteData can put a key of its own in place, one any local user can DPAPI-protect, because the protection is
    /// LocalMachine and the entropy is in the source; one holding ChangePermissions or TakeOwnership can grant itself
    /// the read. The service writes only the three trusted entries, so any other is someone else's doing.</para>
    /// </summary>
    public static string? AccessBeyondTrusted(string path)
    {
        try
        {
            return AccessBeyondTrusted(new FileInfo(path).GetAccessControl());
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException)
        {
            return $"its permissions could not be read ({ex.Message})";
        }
    }

    /// <summary>
    /// The accounts beyond SYSTEM, Administrators and the service account that are allowed to write to
    /// <paramref name="directory"/>, delete from it or change who may (#5366), comma-separated; null when there are
    /// none. Read access is not counted: a directory others may list is not one they can change. An entry that only
    /// children inherit (inherit-only) does not apply to the directory itself and is not counted. An OWNER outside the
    /// three is listed too, as "(the owner)": an owner can always change who may do what. A DACL that cannot be read is
    /// reported, not passed.
    /// </summary>
    public static string? WriteAccessBeyondTrusted(string directory)
    {
        try
        {
            return WriteAccessBeyondTrusted(new DirectoryInfo(directory).GetAccessControl());
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException)
        {
            return $"its permissions could not be read ({ex.Message})";
        }
    }

    /// <summary><see cref="WriteAccessBeyondTrusted(string)"/> over a security descriptor already in hand.</summary>
    internal static string? WriteAccessBeyondTrusted(DirectorySecurity security) =>
        AccessBeyondTrusted(security, DirectoryChangeRights, includeOwner: true);

    private const FileSystemRights DirectoryChangeRights =
        FileSystemRights.WriteData | FileSystemRights.AppendData | FileSystemRights.WriteExtendedAttributes
        | FileSystemRights.WriteAttributes | FileSystemRights.Delete | FileSystemRights.DeleteSubdirectoriesAndFiles
        | FileSystemRights.ChangePermissions | FileSystemRights.TakeOwnership;

    private static string? AccessBeyondTrusted(
        FileSystemSecurity security, FileSystemRights? onlyRights = null, bool includeOwner = false, Func<SecurityIdentifier, bool>? isTrusted = null)
    {
        isTrusted ??= IsTrusted;
        var accounts = new List<string>();
        if (includeOwner && security.GetOwner(typeof(SecurityIdentifier)) is SecurityIdentifier owner && !isTrusted(owner))
        {
            accounts.Add(AccountName(owner) + " (the owner)");
        }

        foreach (FileSystemAccessRule rule in security.GetAccessRules(true, true, typeof(SecurityIdentifier)))
        {
            if (rule.AccessControlType != AccessControlType.Allow
                || rule.IdentityReference is not SecurityIdentifier sid
                || isTrusted(sid)
                || (onlyRights is { } mask && (rule.FileSystemRights & mask) == 0)
                || (onlyRights is not null && rule.PropagationFlags.HasFlag(PropagationFlags.InheritOnly)))
            {
                continue;
            }

            var name = AccountName(sid);
            if (!accounts.Contains(name))
            {
                accounts.Add(name);
            }
        }

        return accounts.Count == 0 ? null : string.Join(", ", accounts);
    }

    private static string AccountName(SecurityIdentifier sid)
    {
        try
        {
            return sid.Translate(typeof(NTAccount)).Value;
        }
        catch (IdentityNotMappedException)
        {
            return sid.Value;
        }
    }

    private static bool IsTrusted(SecurityIdentifier sid) =>
        sid.Equals(LocalSystem) || sid.Equals(Administrators) || sid.Equals(ServiceAccount);

    /* The store folder and what the service writes into it are judged against a wider owner list than the
       credential files: an install whose Log On account was changed after the service first ran still has them
       owned by the account Windows derives from the service's name. */
    private static SecurityIdentifier? _ownServiceSidOverride;

    /// <summary>Substitutes the running account and the service-name account in the store checks; null for both
    /// restores the real ones. A test cannot make a file owned by the real service-name account.</summary>
    internal static void UseStoreTrustForTests(
        SecurityIdentifier? runningAccount, SecurityIdentifier? ownServiceSid, Func<string, SecurityIdentifier?>? linkOwner = null)
    {
        _serviceAccountOverride = runningAccount;
        _ownServiceSidOverride = ownServiceSid;
        _linkOwnerOverride = linkOwner;
    }

    private static Func<string, SecurityIdentifier?>? _linkOwnerOverride;

    /// <summary>
    /// <see cref="IsTrustedStoreOwner"/> for a junction or symbolic link, judged by the link's own owner: it is opened
    /// without following it, so the folder it points to is never asked. A link that cannot be opened or read is not
    /// trusted.
    /// </summary>
    public static bool IsTrustedStoreLinkOwner(string path)
    {
        if (_linkOwnerOverride?.Invoke(path) is { } stoodIn)
        {
            return IsTrustedStoreSid(stoodIn);
        }

        try
        {
            using var handle = OpenForSecurity(path, followLink: false, access: ReadControl);
            var result = GetSecurityInfo(handle, FileObject, OwnerSecurityInformation, out var owner, IntPtr.Zero, IntPtr.Zero, IntPtr.Zero, out var descriptor);
            if (result != 0)
            {
                return false;
            }

            try
            {
                return owner != IntPtr.Zero && IsTrustedStoreSid(new SecurityIdentifier(owner));
            }
            finally
            {
                LocalFree(descriptor);
            }
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or ArgumentException)
        {
            return false;
        }
    }

    private const uint OwnerSecurityInformation = 0x00000001;

    [System.Runtime.InteropServices.DllImport("advapi32.dll", SetLastError = true)]
    private static extern uint GetSecurityInfo(
        Microsoft.Win32.SafeHandles.SafeFileHandle handle, int objectType, uint securityInfo,
        out IntPtr ownerSid, IntPtr group, IntPtr dacl, IntPtr sacl, out IntPtr securityDescriptor);

    [System.Runtime.InteropServices.DllImport("kernel32.dll")]
    private static extern IntPtr LocalFree(IntPtr memory);

    /// <summary>
    /// The SID Windows derives from a service name (what <c>sc.exe showsid</c> prints): the SHA-1 of the upper-cased
    /// name in UTF-16LE, its first 20 bytes read as five little-endian integers under <c>S-1-5-80</c>. Computed rather
    /// than looked up, because Windows answers for <c>NT SERVICE\&lt;name&gt;</c> only while a service of that name is
    /// registered, and the SID that owns files outlives the registration. install-darling.ps1 derives it the same way.
    /// </summary>
    internal static SecurityIdentifier ServiceSidFor(string serviceName)
    {
        /* SHA-1 because Windows derives the account from the name with it: this finds the account, it protects nothing. */
#pragma warning disable CA5350
        var hash = System.Security.Cryptography.SHA1.HashData(System.Text.Encoding.Unicode.GetBytes(serviceName.ToUpperInvariant()));
#pragma warning restore CA5350
        var parts = new string[5];
        for (var i = 0; i < parts.Length; i++)
        {
            parts[i] = BitConverter.ToUInt32(hash, i * 4).ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return new SecurityIdentifier("S-1-5-80-" + string.Join('-', parts));
    }

    /// <summary>This service's own account, <c>NT SERVICE\&lt;service name&gt;</c>, from the name it is registered under.</summary>
    private static SecurityIdentifier OwnServiceSid => _ownServiceSidOverride ?? ServiceSidFor(DarlingCliCommands.ServiceName);

    /// <summary>
    /// Who may own, or hold access on, the store folder and what the service writes into it: SYSTEM,
    /// Administrators, TrustedInstaller, the account the service runs as, and this service's own service SID
    /// (<c>NT SERVICE\&lt;service name&gt;</c>). The last is what the files belong to on an install whose Log On account
    /// was changed after the service first ran. Only that service's own process (or a holder of the restore
    /// privilege) can make an object owned by it, so an ordinary user cannot become it. install-darling.ps1 trusts
    /// the same list.
    /// </summary>
    private static bool IsTrustedStoreSid(SecurityIdentifier sid) =>
        IsTrusted(sid) || sid.Equals(OwnServiceSid) || sid.Equals(ServiceSidFor("TrustedInstaller"));

    /// <summary><see cref="IsTrustedOwner"/> against the wider list the store checks use (<see cref="IsTrustedStoreSid"/>).</summary>
    public static bool IsTrustedStoreOwner(string path)
    {
        try
        {
            return SecurityOf(path).GetOwner(typeof(SecurityIdentifier)) is SecurityIdentifier owner && IsTrustedStoreSid(owner);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
            return false;
        }
    }

    /// <summary><see cref="WriteAccessBeyondTrusted(string)"/> against the store checks' owner list.</summary>
    public static string? StoreWriteAccessBeyondTrusted(string directory)
    {
        try
        {
            return AccessBeyondTrusted(new DirectoryInfo(directory).GetAccessControl(), DirectoryChangeRights, includeOwner: true, IsTrustedStoreSid);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException)
        {
            return $"its permissions could not be read ({ex.Message})";
        }
    }

    private const FileSystemRights EntrySwapRights =
        FileSystemRights.Delete | FileSystemRights.DeleteSubdirectoriesAndFiles
        | FileSystemRights.ChangePermissions | FileSystemRights.TakeOwnership;

    /// <summary>
    /// Which of Users, Everyone and Authenticated Users may remove or replace an entry of <paramref name="directory"/>,
    /// or change who may: a Delete, delete-child, change-permissions or take-ownership grant that applies to the folder
    /// itself, comma-separated; null when none do. Rights that only add new entries (create files, create folders) are
    /// not counted: %ProgramData% hands them to Users, and they cannot rename or remove an entry that is already there.
    /// A DACL that cannot be read is reported, not passed.
    /// </summary>
    public static string? EntryChangeByOrdinaryUsers(string directory)
    {
        try
        {
            var groups = new[]
            {
                new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null),
                new SecurityIdentifier(WellKnownSidType.AuthenticatedUserSid, null),
                new SecurityIdentifier(WellKnownSidType.WorldSid, null),
            };
            var names = new List<string>();
            foreach (FileSystemAccessRule rule in new DirectoryInfo(directory).GetAccessControl().GetAccessRules(true, true, typeof(SecurityIdentifier)))
            {
                if (rule.AccessControlType == AccessControlType.Allow
                    && (rule.FileSystemRights & EntrySwapRights) != 0
                    && !rule.PropagationFlags.HasFlag(PropagationFlags.InheritOnly)
                    && rule.IdentityReference is SecurityIdentifier sid
                    && Array.Exists(groups, group => group.Equals(sid)))
                {
                    var name = AccountName(sid);
                    if (!names.Contains(name))
                    {
                        names.Add(name);
                    }
                }
            }

            return names.Count == 0 ? null : string.Join(", ", names);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException or PlatformNotSupportedException)
        {
            return $"its permissions could not be read ({ex.Message})";
        }
    }

    /// <summary>
    /// Why the service must not write its log files into <paramref name="folder"/>, or null when it may: the folder
    /// or the folder above it is a junction or symbolic link, or the folder is owned by anyone outside the store
    /// owner list. A folder the service locks by path is locked wherever a link on that path leads, and its owner
    /// can always give itself access again.
    /// </summary>
    public static string? LogFolderProblem(string folder)
    {
        var full = Path.TrimEndingDirectorySeparator(Path.GetFullPath(folder));
        var parent = Path.GetDirectoryName(full);
        foreach (var candidate in new[] { full, parent })
        {
            if (candidate is null)
            {
                continue;
            }

            try
            {
                if ((File.GetAttributes(candidate) & FileAttributes.ReparsePoint) != 0)
                {
                    return $"{candidate} is a junction or symbolic link";
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or ArgumentException or NotSupportedException)
            {
                return $"{candidate} could not be read ({ex.Message})";
            }
        }

        return IsTrustedStoreOwner(full)
            ? null
            : $"{full} is not owned by SYSTEM, Administrators, TrustedInstaller, the service account or the service's own account{DescribeOwnerAndExposure(full)}";
    }

    /// <summary>
    /// Opens <paramref name="path"/>, a file only the service reads, and judges it through that one handle (#4044
    /// review): not a junction, symbolic link or directory, one name only, owned by SYSTEM, Administrators or the
    /// service account, and no Allow entry for anyone else (<see cref="AccessBeyondTrusted(string)"/>). Checked by
    /// path and then read by path, the check and the read could each find a different file: whoever can write the
    /// directory can swap one in between. The handle shares read and nothing else, so while it is held the file
    /// cannot be renamed, deleted, replaced or written, and what was judged is what the returned stream reads.
    /// </summary>
    /// <returns>The stream, unbuffered, at the start of the file; or null, with <paramref name="refusal"/> saying why
    /// nothing may be read from the path.</returns>
    public static FileStream? OpenForServiceOnlyRead(string path, out string? refusal)
    {
        var handle = CreateFileW(path, GenericRead, FileShare.Read, IntPtr.Zero, FileMode.Open, FileFlagOpenReparsePoint, IntPtr.Zero);
        if (handle.IsInvalid)
        {
            var error = System.Runtime.InteropServices.Marshal.GetLastPInvokeError();
            handle.Dispose();
            refusal = $"it could not be opened for the service alone to read ({new System.ComponentModel.Win32Exception(error).Message})";
            return null;
        }

        FileStream? stream = null;
        var handedOver = false;
        try
        {
            if ((File.GetAttributes(handle) & (FileAttributes.ReparsePoint | FileAttributes.Directory)) != 0)
            {
                refusal = "it is a junction, a symbolic link or a directory";
                return null;
            }

            if (!GetFileInformationByHandle(handle, out var information))
            {
                var error = System.Runtime.InteropServices.Marshal.GetLastPInvokeError();
                refusal = $"it could not be checked for hard links ({new System.ComponentModel.Win32Exception(error).Message})";
                return null;
            }

            if (information.NumberOfLinks > 1)
            {
                refusal = $"it is a hard link: the file has {information.NumberOfLinks.ToString(System.Globalization.CultureInfo.InvariantCulture)} names, and whoever made another one reaches the same bytes";
                return null;
            }

            stream = new FileStream(handle, FileAccess.Read, bufferSize: 0);
            var security = stream.GetAccessControl();
            if (security.GetOwner(typeof(SecurityIdentifier)) is not SecurityIdentifier owner || !IsTrusted(owner))
            {
                refusal = "it is not owned by SYSTEM, Administrators or the service account";
                return null;
            }

            if (AccessBeyondTrusted(security) is { } accounts)
            {
                refusal = $"accounts beyond SYSTEM, Administrators and the service account have access to it: {accounts}";
                return null;
            }

            refusal = null;
            handedOver = true;
            return stream;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or InvalidOperationException)
        {
            refusal = $"it could not be checked through the open file ({ex.Message})";
            return null;
        }
        finally
        {
            if (!handedOver)
            {
                if (stream is not null)
                {
                    stream.Dispose();
                }
                else
                {
                    handle.Dispose();
                }
            }
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
            /* On a directory the same bit is ListDirectory, and everything created in it inherits the grant. */
            var rules = SecurityOf(path)
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
            var security = SecurityOf(path);
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

    /// <summary>The DACL and owner of a file or a directory, whichever <paramref name="path"/> is.</summary>
    private static FileSystemSecurity SecurityOf(string path) =>
        Directory.Exists(path)
            ? new DirectoryInfo(path).GetAccessControl()
            : new FileInfo(path).GetAccessControl();

    private static void AddFull(DirectorySecurity security, SecurityIdentifier sid, InheritanceFlags inheritance) =>
        security.AddAccessRule(new FileSystemAccessRule(
            sid, FileSystemRights.FullControl, inheritance, PropagationFlags.None, AccessControlType.Allow));

    private static void AddFile(FileSecurity security, SecurityIdentifier sid, FileSystemRights rights) =>
        security.AddAccessRule(new FileSystemAccessRule(sid, rights, AccessControlType.Allow));
}

/// <summary>The operator group's name, reachable from code that is not Windows-only (the config loader's error text,
/// the CLI help): the class above is annotated Windows-only, so a reference to it from unguarded code is a build
/// warning.</summary>
public static class DarlingOperatorGroup
{
    /// <summary>See <see cref="DarlingFileSecurity.OperatorGroupName"/>.</summary>
    public const string Name = "PerformanceMonitor Darling Operators";

    /// <summary>What a reader that was refused the config or a credential file tells the person running it.</summary>
    public static string AccessDeniedAdvice(string path) =>
        $"{path} is restricted to SYSTEM, Administrators, the service account and members of the local group " +
        $"'{Name}'. Ask an administrator to add your account to that group (install-darling.ps1 creates it), then " +
        "sign out and back in so the membership takes effect, or run this from an elevated prompt.";
}

/// <summary>Path containment for the file-security checks, on every platform (the class above is Windows-only).</summary>
internal static class DarlingPathContainment
{
    /// <summary>
    /// Whether <paramref name="path"/> is strictly below <paramref name="directory"/>, by full path, case-insensitively
    /// (#4004 review, round 3). A volume root keeps its separator through
    /// <see cref="Path.TrimEndingDirectorySeparator(string)"/> (<c>C:\</c>), so the prefix gains one only when it lacks
    /// one: appending it regardless made <c>C:\\</c>, which nothing is below, and sent the targets of a config directory
    /// at a volume root to the path-based harden, which follows a junction anywhere on the path.
    /// </summary>
    internal static bool IsStrictlyBelow(string directory, string path)
    {
        var root = Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));
        var full = Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));
        var prefix = Path.EndsInDirectorySeparator(root) ? root : root + Path.DirectorySeparatorChar;
        return full.Length > prefix.Length && full.StartsWith(prefix, StringComparison.OrdinalIgnoreCase);
    }
}
