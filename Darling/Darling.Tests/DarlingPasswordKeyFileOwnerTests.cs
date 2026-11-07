/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
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
/// file's owner rule uses.
///
/// <para>In <c>darling-config-env</c> (it runs alone) because the test names another account as the service's through
/// <see cref="DarlingFileSecurity.HardenForAccount"/>, a process-wide setting, and puts the current user back after.</para>
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingPasswordKeyFileOwnerTests
{
    private static SecurityIdentifier CurrentUser => WindowsIdentity.GetCurrent().User!;

    [Fact]
    public void OnWindows_AKeyOwnedByAnotherAccount_LoadsWithTheOwnerWarning_AndOneOwnedByTheServiceAccountDoesNot()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "File owners as SIDs are Windows-only; Linux runs the owner check in the self-check.");

        byte[] pkcs8;
        string keyId;
        using (var key = PasswordPrivateKey.Generate())
        {
            pkcs8 = key.ExportPkcs8();
            keyId = key.PublicKey.KeyId;
        }

        var dir = Directory.CreateTempSubdirectory("password-key-owner-").FullName;
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, Convert.ToBase64String(pkcs8));
            /* Owned by the current user, which is the service account unless another one is named. */
            var security = new FileInfo(path).GetAccessControl();
            security.SetOwner(CurrentUser);
            new FileInfo(path).SetAccessControl(security);

            var trusted = new CapturingTestLogger();
            AssertLoads(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, trusted), keyId);
            Assert.DoesNotContain(trusted.Lines, l => l.Contains("is owned by", StringComparison.Ordinal));

            try
            {
                DarlingFileSecurity.HardenForAccount(new SecurityIdentifier("S-1-5-80-8006-1-2-3-4"));

                var untrusted = new CapturingTestLogger();
                AssertLoads(DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, untrusted), keyId);
                var warning = Assert.Single(untrusted.Lines, l => l.Contains("is owned by", StringComparison.Ordinal));
                Assert.StartsWith("Warning:", warning, StringComparison.Ordinal);
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
