/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Principal;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The DPAPI-credential ACL hardening (V8 security hardening, #1262). Windows-gated (ACLs are a
/// Windows concept). Proves the created files/dirs carry a PROTECTED DACL (inheritance stripped) with
/// no world-readable ACE (Everyone / Authenticated Users / Users), SYSTEM + Administrators + the
/// service account granted, INTERACTIVE granted only where the operator's Viewer must read
/// (admin/viewer credentials), and the trusted-owner guard accepting a file this process created.
/// </summary>
public sealed class DarlingFileSecurityTests
{
    private static readonly SecurityIdentifier s_system = new(WellKnownSidType.LocalSystemSid, null);
    private static readonly SecurityIdentifier s_administrators = new(WellKnownSidType.BuiltinAdministratorsSid, null);
    private static readonly SecurityIdentifier s_interactive = new(WellKnownSidType.InteractiveSid, null);
    private static readonly SecurityIdentifier s_everyone = new(WellKnownSidType.WorldSid, null);
    private static readonly SecurityIdentifier s_authenticatedUsers = new(WellKnownSidType.AuthenticatedUserSid, null);
    private static readonly SecurityIdentifier s_builtinUsers = new(WellKnownSidType.BuiltinUsersSid, null);

    [Fact]
    public void HardenFile_SuperuserCredential_LocksOutWorldAndInteractive()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-acl-owner-" + Guid.NewGuid().ToString("N") + ".dpapi");
        File.WriteAllText(path, "blob");
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: false);

            var rules = ReadRules(new FileInfo(path).GetAccessControl());
            AssertProtectedAndNoWorldRead(new FileInfo(path).GetAccessControl(), rules);

            /* SYSTEM + Administrators present; INTERACTIVE absent (the Viewer never reads the superuser cred). */
            Assert.Contains(rules, r => r.sid.Equals(s_system));
            Assert.Contains(rules, r => r.sid.Equals(s_administrators));
            Assert.DoesNotContain(rules, r => r.sid.Equals(s_interactive));
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>
    /// #1769: the non-interactive posture emits EXACTLY three identities — SYSTEM, Administrators and the
    /// service identity — and nothing else. Presence/absence assertions alone cannot say that: they pass
    /// happily while a fourth principal rides along, which is precisely the shape of the defect that prompted
    /// this (an unintended <c>NT AUTHORITY\INTERACTIVE</c> re-added on every service start). This is the
    /// posture the superuser credential, the transient init pwfile, and the config BACKUPS all take.
    /// </summary>
    [Fact]
    public void HardenFile_NonInteractive_EmitsExactlyTheThreeIntendedIdentities()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-acl-exact-" + Guid.NewGuid().ToString("N") + ".bak");
        File.WriteAllText(path, "blob");
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: false);

            var security = new FileInfo(path).GetAccessControl();
            var rules = ReadRules(security);
            AssertProtectedAndNoWorldRead(security, rules);

            /* The service identity is whoever this process runs as — the same resolution HardenFile uses. */
            var serviceIdentity = WindowsIdentity.GetCurrent().User;
            Assert.NotNull(serviceIdentity);

            var expected = new[] { s_system, s_administrators, serviceIdentity! };
            var actual = rules.Select(r => r.sid).Distinct().ToList();

            /* No more, no less. The failure names the unexpected principals rather than just a count, because
               "expected 3, got 4" tells whoever hits this nothing about which grant crept in. */
            var unexpected = actual.Where(sid => !expected.Any(sid.Equals)).Select(TranslateOrRaw).ToList();
            Assert.True(unexpected.Count == 0,
                "The non-interactive DACL granted principals beyond SYSTEM, Administrators and the service "
                + "identity: " + string.Join(", ", unexpected));

            var missing = expected.Where(sid => !actual.Any(sid.Equals)).Select(TranslateOrRaw).ToList();
            Assert.True(missing.Count == 0,
                "The non-interactive DACL is missing intended principals: " + string.Join(", ", missing));
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>A SID's friendly name for a failure message, falling back to the raw SID when it does not map.</summary>
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

    [Fact]
    public void HardenFile_RoleCredential_GrantsInteractiveRead()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-acl-admin-" + Guid.NewGuid().ToString("N") + ".dpapi");
        File.WriteAllText(path, "blob");
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: true);

            var security = new FileInfo(path).GetAccessControl();
            var rules = ReadRules(security);
            AssertProtectedAndNoWorldRead(security, rules);

            /* The operator's Viewer (interactive) can READ, but INTERACTIVE gets no more than read. */
            var interactive = rules.Where(r => r.sid.Equals(s_interactive)).ToList();
            Assert.NotEmpty(interactive);
            Assert.All(interactive, r => Assert.True(
                (r.rights & FileSystemRights.Read) == FileSystemRights.Read
                && (r.rights & FileSystemRights.Write) == 0,
                $"INTERACTIVE should be read-only, was {r.rights}"));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void HardenDirectory_StripsInheritance_AndGrantsInteractiveTraverseOnly()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-acl-dir-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        try
        {
            DarlingFileSecurity.HardenDirectory(path, allowInteractiveTraverse: true);

            var security = new DirectoryInfo(path).GetAccessControl();
            var rules = ReadRules(security);
            AssertProtectedAndNoWorldRead(security, rules);

            Assert.Contains(rules, r => r.sid.Equals(s_system));
            Assert.Contains(rules, r => r.sid.Equals(s_administrators));

            /* INTERACTIVE gets traverse (execute) but NOT list/read-data, and only on this folder. */
            var interactive = rules.Where(r => r.sid.Equals(s_interactive)).ToList();
            Assert.NotEmpty(interactive);
            Assert.All(interactive, r =>
            {
                Assert.True((r.rights & FileSystemRights.ExecuteFile) == FileSystemRights.ExecuteFile, "traverse missing");
                Assert.True((r.rights & FileSystemRights.ListDirectory) == 0, "should not grant list");
                Assert.Equal(InheritanceFlags.None, r.inheritance);
            });
        }
        finally
        {
            Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public void IsTrustedOwner_TrueForAFileThisProcessCreated()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-acl-owner-check-" + Guid.NewGuid().ToString("N") + ".dpapi");
        File.WriteAllText(path, "blob");
        try
        {
            /* Created by this process => owned by the service account (this identity) => trusted. */
            Assert.True(DarlingFileSecurity.IsTrustedOwner(path));
        }
        finally
        {
            File.Delete(path);
        }

        /* A path that doesn't exist is not trusted (fails closed). */
        Assert.False(DarlingFileSecurity.IsTrustedOwner(path));
    }

    /* ---- #1647: the verification half — is a secret-bearing file still readable by ordinary users? ----
       darling.json carries every monitored server's encryptedPassword plus the MCP and web tokens, all under
       DPAPI LocalMachine scope with an entropy constant published in this repo, so READ access to the file IS
       the secret. It never got an ACL: it sits beside the binary, and the documented install (extract to
       C:\PerformanceMonitorDarling) inherits BUILTIN\Users: Read & Execute from the root DACL. The service now
       hardens it at startup and raises a Critical when it is still exposed — this is the check behind that. */

    [Fact]
    public void IsReadableByOrdinaryUsers_TrueWhenUsersHoldsAReadAce_FalseAfterHardening()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-config-acl-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, "{}");
        try
        {
            /* Reproduce what an install under C:\ inherits: BUILTIN\Users allowed Read. Added explicitly
               because %TEMP% is per-user and grants Users nothing to inherit. */
            var exposed = new FileInfo(path).GetAccessControl();
            exposed.AddAccessRule(new FileSystemAccessRule(
                s_builtinUsers, FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(path).SetAccessControl(exposed);

            Assert.True(
                DarlingFileSecurity.IsReadableByOrdinaryUsers(path),
                "a file granting BUILTIN\\Users read must be reported as exposed");

            /* The fix the service applies. HardenFile protects the DACL and drops every inherited ACE, so the
               Users grant is gone even though INTERACTIVE read is added for the Viewer. */
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: true);

            Assert.False(
                DarlingFileSecurity.IsReadableByOrdinaryUsers(path),
                "after hardening, no Users/Authenticated Users/Everyone read ACE may survive");
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Theory]
    [InlineData(WellKnownSidType.AuthenticatedUserSid)]
    [InlineData(WellKnownSidType.WorldSid)]
    public void IsReadableByOrdinaryUsers_AlsoCatchesAuthenticatedUsersAndEveryone(WellKnownSidType sidType)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-config-acl-grp-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, "{}");
        try
        {
            var security = new FileInfo(path).GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(sidType, null), FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(path).SetAccessControl(security);

            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void IsReadableByOrdinaryUsers_IgnoresAMetadataOnlyGrant_AndADenyAce()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var path = Path.Combine(Path.GetTempPath(), "darling-config-acl-meta-" + Guid.NewGuid().ToString("N") + ".json");
        File.WriteAllText(path, "{}");
        try
        {
            DarlingFileSecurity.HardenFile(path, allowInteractiveRead: true);

            var security = new FileInfo(path).GetAccessControl();
            /* ReadAttributes/ReadPermissions grant no BYTES. Testing against the composite
               FileSystemRights.Read mask would call this "readable" and cry wolf on a common,
               harmless ACE — hence the check keys on ReadData specifically. */
            security.AddAccessRule(new FileSystemAccessRule(
                s_builtinUsers,
                FileSystemRights.ReadAttributes | FileSystemRights.ReadPermissions,
                AccessControlType.Allow));
            /* A DENY ACE is not a grant either. */
            security.AddAccessRule(new FileSystemAccessRule(
                s_authenticatedUsers, FileSystemRights.Read, AccessControlType.Deny));
            new FileInfo(path).SetAccessControl(security);

            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void IsReadableByOrdinaryUsers_FalseForAnUnreadableDacl_RatherThanThrowing()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        /* A missing file has no DACL to read. It returns false rather than throwing or crying wolf: the
           caller has ALREADY logged loudly if its harden attempt failed, and a Critical raised on an
           unreadable DACL would be noise that trains operators to ignore the real one. */
        Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(
            Path.Combine(Path.GetTempPath(), "darling-config-absent-" + Guid.NewGuid().ToString("N") + ".json")));
    }

    [Fact]
    public void HardenDirectory_OnTheCredentialParent_LeavesTheSiblingLogDirectoryWritable()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        /* Mirrors the REAL on-disk layout. DarlingManagedPostgres hardens ParentOf(dataDirectory) —
           %ProgramData%\PerformanceMonitorDarling — which ALSO contains the service's own `logs`
           directory (a sibling of the `pg` data dir). #1581 asked whether that credential lockdown is
           what silenced file logging in the field. It is NOT: HardenDirectory grants
           WindowsIdentity.GetCurrent().User — the identity the service is actually running as — Full
           Control with ContainerInherit|ObjectInherit, so `logs` INHERITS write access and the running
           service can never lock itself out. Proven with real I/O, not just ACE inspection, because
           that is the claim that matters. A field ACL failure therefore means something EXTERNAL
           re-ACL'd the path (on the field box, a SYSTEM-context SSM operation on one log file), not
           that the hardening scope is wrong — so DO NOT "fix" this by narrowing the scope off the
           parent, which would drop the inherited protection from the data dir subtree (server.key). */
        var parent = Path.Combine(Path.GetTempPath(), "darling-acl-parent-" + Guid.NewGuid().ToString("N"));
        var logs = Path.Combine(parent, "logs");
        Directory.CreateDirectory(logs);
        var existingLog = Path.Combine(logs, "darling-service_existing.log");
        File.WriteAllText(existingLog, "before\n");
        try
        {
            /* Exactly what DarlingManagedPostgres does before initdb. */
            DarlingFileSecurity.HardenDirectory(parent, allowInteractiveTraverse: true);

            /* 1. An already-open-style log file stays APPENDABLE (the flush path). */
            File.AppendAllText(existingLog, "after\n");
            Assert.Equal("before\nafter\n", File.ReadAllText(existingLog));

            /* 2. A NEW log file — the daily-rotation case — can still be created and written. */
            var rotatedLog = Path.Combine(logs, "darling-service_rotated.log");
            File.WriteAllText(rotatedLog, "rotated\n");
            Assert.Equal("rotated\n", File.ReadAllText(rotatedLog));

            /* 3. A subdirectory created AFTER hardening — the initdb data-dir case — is writable, which
                  is the whole reason the parent is hardened first (the subtree inherits). */
            var dataDir = Path.Combine(parent, "pg");
            Directory.CreateDirectory(dataDir);
            var serverKey = Path.Combine(dataDir, "server.key");
            File.WriteAllText(serverKey, "key\n");
            Assert.Equal("key\n", File.ReadAllText(serverKey));

            /* 4. The lockdown DID reach both (no world-readable principal survives) — the log directory
                  really is swept into the credential ACL; it is simply still writable by the service. */
            foreach (var swept in new[] { logs, dataDir })
            {
                var rules = ReadRules(new DirectoryInfo(swept).GetAccessControl());
                Assert.DoesNotContain(rules, r => r.sid.Equals(s_everyone));
                Assert.DoesNotContain(rules, r => r.sid.Equals(s_authenticatedUsers));
                Assert.DoesNotContain(rules, r => r.sid.Equals(s_builtinUsers));
            }
        }
        finally
        {
            Directory.Delete(parent, recursive: true);
        }
    }

    private static (SecurityIdentifier sid, FileSystemRights rights, InheritanceFlags inheritance)[] ReadRules(
        FileSystemSecurity security)
    {
        return security.GetAccessRules(includeExplicit: true, includeInherited: true, typeof(SecurityIdentifier))
            .Cast<FileSystemAccessRule>()
            .Where(r => r.AccessControlType == AccessControlType.Allow)
            .Select(r => ((SecurityIdentifier)r.IdentityReference, r.FileSystemRights, r.InheritanceFlags))
            .ToArray();
    }

    [Fact]
    public async Task WriteWithBackup_HardensTheBackup_BecauseFileCopyDoesNotCarryTheDacl()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        /* The install location this reproduces is a folder created directly under C:\, whose root DACL
           grants BUILTIN\Users an INHERITABLE read. %TEMP% is per-user and grants Users nothing, so the
           ACE is planted here deliberately — without it the backup would be clean for the wrong reason and
           this test would pass with the hardening removed. */
        var directory = Path.Combine(Path.GetTempPath(), "darling-cfg-bak-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(directory);
        try
        {
            var directorySecurity = new DirectoryInfo(directory).GetAccessControl();
            directorySecurity.AddAccessRule(new FileSystemAccessRule(
                s_builtinUsers,
                FileSystemRights.Read,
                InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None,
                AccessControlType.Allow));
            new DirectoryInfo(directory).SetAccessControl(directorySecurity);

            var configPath = Path.Combine(directory, "darling.json");
            await File.WriteAllTextAsync(configPath, "{\"servers\":[]}");

            /* Proves the planted ACE actually flows to a new file in this directory — i.e. that the
               scenario under test is real on this machine, not just asserted. */
            var canary = Path.Combine(directory, "canary.json");
            await File.WriteAllTextAsync(canary, "{}");
            Assert.True(
                DarlingFileSecurity.IsReadableByOrdinaryUsers(canary),
                "a new file in this directory must inherit the Users read ACE, or the test proves nothing");

            var written = await DarlingCliCommands.WriteWithBackupAsync(
                configPath, "{\"servers\":[],\"edited\":true}", TextWriter.Null, TextWriter.Null, default);
            Assert.True(written);

            var backup = Directory.GetFiles(directory, "darling.json.bak-*").Single();
            Assert.False(
                DarlingFileSecurity.IsReadableByOrdinaryUsers(backup),
                "the backup is a full copy of the encrypted passwords and access tokens — File.Copy does not " +
                "carry the source DACL, so an unhardened backup hands every local user the credentials that " +
                "darling.json's own ACL exists to protect");

            /* #1769: and NOT to INTERACTIVE either. The live darling.json grants it because things genuinely
               read the config as the interactive operator — the Viewer and the CLI verbs. Nothing reads a
               backup: the only code that knows the .bak- name is the code that creates it, and restoring one
               already needs elevation because writing darling.json does. So the grant bought nothing and left
               a second readable copy of every secret. Asserted end to end through the real WriteWithBackupAsync
               path rather than against HardenFile directly, because the defect this closes was a CALL SITE
               passing the wrong argument, which a test of the shape alone cannot see. */
            Assert.DoesNotContain(
                ReadRules(new FileInfo(backup).GetAccessControl()),
                r => r.Item1.Equals(s_interactive));
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static void AssertProtectedAndNoWorldRead(
        FileSystemSecurity security, (SecurityIdentifier sid, FileSystemRights rights, InheritanceFlags inheritance)[] rules)
    {
        /* Inheritance stripped: the ACL is exactly what we set, nothing inherited from %ProgramData%. */
        Assert.True(security.AreAccessRulesProtected, "DACL must be protected (inheritance disabled)");

        /* No world-readable principal survives. */
        Assert.DoesNotContain(rules, r => r.sid.Equals(s_everyone));
        Assert.DoesNotContain(rules, r => r.sid.Equals(s_authenticatedUsers));
        Assert.DoesNotContain(rules, r => r.sid.Equals(s_builtinUsers));
    }

    /// <summary>
    /// #1816: the worker's backup sweep hardens EXISTING darling.json.bak-* siblings — the backups
    /// made before #1786 fixed the creation path kept whatever the folder handed them (on the field
    /// box, inherited BUILTIN\Users read against machine-scoped DPAPI blobs). The sweep must strip
    /// that from every backup, leave the live file to its own hardening, and no-op cleanly when there
    /// are no backups.
    /// </summary>
    [Fact]
    public void ConfigBackupSweep_HardensEveryExistingBackup_AndSkipsTheLiveFile()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-bak-sweep-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var configPath = Path.Combine(directory, "darling.json");
        var backup1 = configPath + ".bak-20260721-162218";
        var backup2 = configPath + ".bak-20260722-123603";
        var decoy = Path.Combine(directory, "unrelated.json.bak-20260721-000000");
        try
        {
            foreach (var file in new[] { configPath, backup1, backup2, decoy })
            {
                File.WriteAllText(file, "{}");
                var exposed = new FileInfo(file).GetAccessControl();
                exposed.AddAccessRule(new FileSystemAccessRule(
                    s_builtinUsers, FileSystemRights.Read, AccessControlType.Allow));
                new FileInfo(file).SetAccessControl(exposed);
                Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(file));
            }

            DarlingWorker.TryHardenConfigBackups(configPath, NullLogger.Instance);

            /* Both backups locked down... */
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(backup1),
                "backup 1 must lose its inherited Users read");
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(backup2),
                "backup 2 must lose its inherited Users read");

            /* ...the live file untouched by THIS sweep (its own hardening owns it)... */
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(configPath),
                "the backup sweep must not touch the live config — TryHardenConfigFile owns it");

            /* ...and a file that merely LOOKS like a backup of something else is not swept. */
            Assert.True(DarlingFileSecurity.IsReadableByOrdinaryUsers(decoy),
                "only darling.json.bak-* siblings are the sweep's business");
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>
    /// #2093 (ghauan): a backup whose exposure is ALREADY closed must be skipped entirely — not re-hardened,
    /// not errored about. The field shape: an operator ran the error message's icacls commands (which fix the
    /// ACL but do not transfer OWNERSHIP), so the sweep's rewrite kept throwing "unauthorized" on every start
    /// about a file that was already secure. The canary ACE proves the skip: HardenFile's rewrite would strip
    /// it, so its survival means the sweep never touched the descriptor.
    /// </summary>
    [Fact]
    public void ConfigBackupSweep_AlreadyHardenedBackup_IsSkippedUntouched()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-bak-idem-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var configPath = Path.Combine(directory, "darling.json");
        var backup = configPath + ".bak-20260807-094518";
        var canary = new SecurityIdentifier(WellKnownSidType.BuiltinGuestsSid, null);
        try
        {
            File.WriteAllText(configPath, "{}");
            File.WriteAllText(backup, "{}");

            /* Hand-hardened, the way the error message instructs: inheritance off, no ordinary-user
               read — plus the canary ACE a HardenFile rewrite would remove. */
            var acl = new FileInfo(backup).GetAccessControl();
            acl.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
            acl.AddAccessRule(new FileSystemAccessRule(
                WindowsIdentity.GetCurrent().User!, FileSystemRights.FullControl, AccessControlType.Allow));
            acl.AddAccessRule(new FileSystemAccessRule(
                canary, FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(backup).SetAccessControl(acl);
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(backup));

            DarlingWorker.TryHardenConfigBackups(configPath, NullLogger.Instance);

            /* The canary survives — the sweep skipped the already-closed exposure instead of rewriting. */
            var after = new FileInfo(backup).GetAccessControl();
            var survived = after.GetAccessRules(true, false, typeof(SecurityIdentifier))
                .Cast<FileSystemAccessRule>()
                .Any(rule => rule.IdentityReference == canary);
            Assert.True(survived, "an already-hardened backup must be skipped, not re-hardened");
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    [Fact]
    public void ConfigBackupSweep_NoBackups_IsANoOp()
    {
        var directory = Path.Combine(Path.GetTempPath(), "darling-bak-none-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var configPath = Path.Combine(directory, "darling.json");
        try
        {
            File.WriteAllText(configPath, "{}");
            DarlingWorker.TryHardenConfigBackups(configPath, NullLogger.Instance);
        }
        finally
        {
            try { Directory.Delete(directory, recursive: true); } catch { /* best-effort */ }
        }
    }

    /// <summary>
    /// The installer's owner step must set the owner on the file's CURRENT descriptor, never on a freshly
    /// constructed <c>FileSecurity</c> (#1957).
    ///
    /// <para><b>The bug this pins is a PowerShell semantic no compiler can see.</b> <c>Set-Acl</c> applies the
    /// whole descriptor it is handed, so a bare <c>FileSecurity</c> carrying nothing but <c>SetOwner</c> also
    /// wrote an empty, unprotected DACL — re-enabling inheritance and handing <c>BUILTIN\Users</c> read back on
    /// an install under <c>C:\</c>, one statement after the hardened DACL was applied. Measured on a scratch
    /// layout under <c>C:\</c>: immediately after that step the file read protected=False with
    /// <c>BUILTIN\Users</c> present and every hardened ACE gone. The per-file verification that follows was
    /// therefore correct all along and the SECURITY WARNING it printed on three consecutive field installs was
    /// honest — about a hole the script itself had just opened.</para>
    ///
    /// <para>Pinned STRUCTURALLY rather than behaviourally on purpose. Reproducing the defect needs PowerShell's
    /// <c>Set-Acl</c> and an elevated <c>SeRestorePrivilege</c> to set a service SID as owner; the equivalent
    /// .NET call persists only the sections it sees modified, so a C# test would go green against the exact
    /// code that broke the field boxes. What is checkable here is the shape: one descriptor built for the DACL,
    /// the owner applied onto a re-read of what was written, and the verification LAST so the state it judges
    /// is the state the installer leaves behind.</para>
    /// </summary>
    [Fact]
    public void InstallScript_SetsTheOwnerOnTheCurrentDescriptor_SoTheHardenedDaclSurvives()
    {
        var script = ReadRepoFile(Path.Combine("Darling", "tools", "install-darling.ps1"));

        var setOwner = script.IndexOf("$owner.SetOwner($serviceSid)", StringComparison.Ordinal);
        Assert.True(setOwner >= 0, "install-darling.ps1 no longer sets the credential files' owner");

        /* The owner descriptor is READ from the file, and read close enough to the SetOwner to be the same
           object rather than an unrelated Get-Acl elsewhere in the script. */
        var reRead = script.IndexOf("$owner = Get-Acl -Path $secretFile", StringComparison.Ordinal);
        Assert.True(reRead >= 0 && reRead < setOwner,
            "the owner must be set on the file's CURRENT ACL ($owner = Get-Acl ...), not on a fresh " +
            "FileSecurity — Set-Acl writes the whole descriptor, so a bare one wipes the hardened DACL (#1957).");

        /* Exactly ONE FileSecurity is constructed in the whole script: the hardened DACL. A second one is how
           this defect looked, and the assertion above alone would stay green with a bare-descriptor Set-Acl
           left behind after the re-read. */
        var constructions = script.Split("New-Object System.Security.AccessControl.FileSecurity", StringSplitOptions.None).Length - 1;
        Assert.True(constructions == 1,
            $"expected exactly 1 FileSecurity construction (the hardened DACL), found {constructions}. A second " +
            "bare descriptor applied over the first is #1957: it resets DACL protection and lets the folder's " +
            "inherited BUILTIN\\Users read come straight back.");

        /* And the verification must judge the FINAL state — after both the DACL and the owner. */
        var verify = script.IndexOf("$after = Get-Acl -Path $secretFile", StringComparison.Ordinal);
        Assert.True(verify > setOwner,
            "the per-file verification must run AFTER the owner step, or it certifies a state the installer " +
            "then changes.");
    }

}
