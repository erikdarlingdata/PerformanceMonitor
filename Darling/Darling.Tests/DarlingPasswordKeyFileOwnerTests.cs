/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8006 review: on Windows a <c>passwordKey</c> <c>file:</c> key owned by an account other than SYSTEM, Administrators
/// or the service account loads with a Warning naming the path, because a file's owner can always change its ACL and so
/// give itself write. A real file, judged by <see cref="DarlingFileSecurity.IsTrustedOwner"/>, the check the default key
/// file's owner rule uses. A key whose folder is owned by such an account is refused, as the default key file's folder
/// is, since the folder's owner can put another file in its place.
///
/// <para>In <c>darling-config-env</c> (it runs alone) because the tests name another account as the service's through
/// <see cref="DarlingFileSecurity.HardenForAccount"/>, a process-wide setting, and put the current user back after.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingPasswordKeyFileOwnerTests
{
    private static readonly SecurityIdentifier s_localSystem = new(WellKnownSidType.LocalSystemSid, null);
    private static readonly SecurityIdentifier s_administrators = new(WellKnownSidType.BuiltinAdministratorsSid, null);

    /* An account the service could run as that is not the test's own; no such account exists. */
    private static readonly SecurityIdentifier s_otherServiceAccount = new("S-1-5-80-8006-1-2-3-4");

    private static SecurityIdentifier CurrentUser => WindowsIdentity.GetCurrent().User!;

    [Fact]
    public void OnWindows_AKeyOwnedByTheServiceAccount_LoadsWithoutTheOwnerWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "File owners as SIDs are Windows-only; Linux runs the owner check in the self-check.");

        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-owner-").FullName;
        try
        {
            /* Owned by the current user, which is the service account unless another one is named. */
            var path = WriteKeyOwnedByTheCurrentUser(dir, pkcs8);

            var trusted = new CapturingTestLogger();
            AssertLoads(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, trusted), keyId);
            Assert.DoesNotContain(trusted.Lines, l => l.Contains("is owned by", StringComparison.Ordinal));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// The owner warning, with another account named as the service's. The folder has to pass its own check under that
    /// account, so it is given to Administrators and kept to SYSTEM and Administrators, which only an elevated token may
    /// do; elsewhere the test is skipped, and the owner sentence is pinned through the facts in
    /// <see cref="DarlingPasswordKeyFileChecksTests"/>.
    /// </summary>
    [Fact]
    public void OnWindows_AKeyOwnedByAnotherAccount_InAFolderOnlyTrustedAccountsCanChange_LoadsWithTheOwnerWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "File owners as SIDs are Windows-only; Linux runs the owner check in the self-check.");

        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-owner-").FullName;
        try
        {
            var path = WriteKeyOwnedByTheCurrentUser(dir, pkcs8);
            var folder = new DirectoryInfo(dir);
            var owner = folder.GetAccessControl();
            owner.SetOwner(s_administrators);
            try
            {
                folder.SetAccessControl(owner);
            }
            catch (Exception ex) when (ex is InvalidOperationException or UnauthorizedAccessException or IOException)
            {
                Assert.Skip("this account may not hand a folder to Administrators (it needs an elevated token): " + ex.Message);
            }

            folder.SetAccessControl(OnlySystemAndAdministrators());

            try
            {
                DarlingFileSecurity.HardenForAccount(s_otherServiceAccount);

                var untrusted = new CapturingTestLogger();
                AssertLoads(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, untrusted), keyId);
                var warning = Assert.Single(untrusted.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
                Assert.Contains("passwordKey: file '" + path + "' is owned by an account other than SYSTEM, Administrators or the service account", warning, StringComparison.Ordinal);
                Assert.Contains("loaded anyway", warning, StringComparison.Ordinal);
                Assert.DoesNotContain(Convert.ToBase64String(pkcs8)[..16], warning, StringComparison.Ordinal);
            }
            finally
            {
                DarlingFileSecurity.HardenForAccount(CurrentUser);
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    /// <summary>
    /// #8006 review: a key in a folder owned by an account other than SYSTEM, Administrators or the service account is
    /// refused, naming the owner, though no entry in the folder's ACL lets anyone else write: the owner can always grant
    /// itself that. The folder gives the test's account read only, which no check counts. With the test's account named
    /// as the service's again, the same folder passes and the key loads.
    /// </summary>
    [Fact]
    public void OnWindows_AKeyInAFolderOwnedByAnAccountBeyondTheTrustedOnes_IsRefusedNamingTheOwner()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Folder owners as SIDs are Windows-only.");

        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-owner-").FullName;
        var folder = new DirectoryInfo(dir);
        try
        {
            var path = WriteKeyOwnedByTheCurrentUser(dir, pkcs8);
            var owner = folder.GetAccessControl();
            owner.SetOwner(CurrentUser);
            folder.SetAccessControl(owner);
            var readOnly = OnlySystemAndAdministrators();
            readOnly.AddAccessRule(new FileSystemAccessRule(
                CurrentUser, FileSystemRights.ReadAndExecute, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None, AccessControlType.Allow));
            folder.SetAccessControl(readOnly);

            PasswordKeyFileLoad refused;
            try
            {
                DarlingFileSecurity.HardenForAccount(s_otherServiceAccount);
                refused = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, new CapturingTestLogger());
            }
            finally
            {
                DarlingFileSecurity.HardenForAccount(CurrentUser);
            }

            Assert.True(refused.Untrusted);
            Assert.Null(refused.Pkcs8);
            Assert.StartsWith("passwordKey: file '" + path + "' is not read, because its folder " + dir + " is not trusted", refused.Refusal, StringComparison.Ordinal);
            Assert.Contains(": " + CurrentUser.Translate(typeof(NTAccount)).Value + " (the owner).", refused.Refusal, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("/setowner *S-1-5-32-544", refused.Refusal, StringComparison.Ordinal);
            Assert.DoesNotContain(Convert.ToBase64String(pkcs8)[..16], refused.Refusal, StringComparison.Ordinal);

            var logger = new CapturingTestLogger();
            AssertLoads(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, logger), keyId);
            Assert.DoesNotContain(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            /* The test's account owns the folder, so it may give itself full control back before the delete. */
            var security = folder.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                CurrentUser, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None, AccessControlType.Allow));
            folder.SetAccessControl(security);
            Directory.Delete(dir, recursive: true);
        }
    }

    private static (byte[] Pkcs8, string KeyId) NewKey()
    {
        using var key = PasswordPrivateKey.Generate();
        return (key.ExportPkcs8(), key.PublicKey.KeyId);
    }

    /// <summary>Writes the key into <paramref name="dir"/> and makes the current user its owner, which any account may
    /// do for itself.</summary>
    private static string WriteKeyOwnedByTheCurrentUser(string dir, byte[] pkcs8)
    {
        var path = Path.Combine(dir, "password-key");
        File.WriteAllText(path, Convert.ToBase64String(pkcs8));
        var security = new FileInfo(path).GetAccessControl();
        security.SetOwner(CurrentUser);
        new FileInfo(path).SetAccessControl(security);
        return path;
    }

    /// <summary>A folder ACL with nothing inherited and full control for SYSTEM and Administrators only, handed down to
    /// what is in the folder.</summary>
    private static DirectorySecurity OnlySystemAndAdministrators()
    {
        var security = new DirectorySecurity();
        security.SetAccessRuleProtection(isProtected: true, preserveInheritance: false);
        foreach (var sid in new[] { s_localSystem, s_administrators })
        {
            security.AddAccessRule(new FileSystemAccessRule(
                sid, FileSystemRights.FullControl, InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                PropagationFlags.None, AccessControlType.Allow));
        }

        return security;
    }

    private static void AssertLoads(PasswordKeyFileLoad load, string keyId)
    {
        Assert.Null(load.Refusal);
        Assert.NotNull(load.Pkcs8);
        try
        {
            using var key = PasswordPrivateKey.FromPkcs8(load.Pkcs8);
            Assert.Equal(keyId, key.PublicKey.KeyId);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(load.Pkcs8);
        }
    }
}
