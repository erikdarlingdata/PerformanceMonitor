/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8141: the key owner refusal names both users and gives the chown that fixes it. The Unix owner check is reached
/// through <see cref="StandInUnixModes"/>.
/// </summary>
public sealed class CredentialOwnershipMessageTests
{
    private const string ComposeKey = "/var/lib/darling/credentials/password-key";

    [Fact]
    public void RootService_OnA1654OwnedKey_NamesBothUsersAndTheReverseChown()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 0 };
        modes.ReportOwner(ComposeKey, 1654);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var reason = DarlingServiceKeyFile.UnexpectedOwnerReason(ComposeKey);

            Assert.NotNull(reason);
            Assert.Contains("owned by user 1654", reason);
            Assert.Contains("not by user 0", reason);
            Assert.Contains("chown darling -R 0:0 /var/lib/darling", reason);
        }
    }

    [Fact]
    public void NonRootService_OnARootOwnedKey_GivesTheForwardChown()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(ComposeKey, 0);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var reason = DarlingServiceKeyFile.UnexpectedOwnerReason(ComposeKey);

            Assert.NotNull(reason);
            Assert.Contains("-R 1654:1654 /var/lib/darling", reason);
        }
    }

    [Fact]
    public void AnotherUser_OnAnImageOwnedKey_SaysMountAVolumeOrRunAs1654()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1000 };
        modes.ReportOwner(ComposeKey, 1654);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var reason = DarlingServiceKeyFile.UnexpectedOwnerReason(ComposeKey);

            Assert.NotNull(reason);
            Assert.Contains("owned by user 1654, not by user 1000", reason);
            Assert.Contains("mount a volume at /var/lib/darling (or /var/lib/darling/credentials)", reason);
            Assert.Contains("run the service as user 1654", reason);
            Assert.Contains("already mounted there, run: docker compose run", reason);
        }
    }

    [Fact]
    public void OutsideTheComposeVolume_GivesAPlainChownOfThePath()
    {
        const string path = "/opt/darling/darling-keys/password-key";
        var modes = new StandInUnixModes { EffectiveUserId = 1000 };
        modes.ReportOwner(path, 0);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var reason = DarlingServiceKeyFile.UnexpectedOwnerReason(path);

            Assert.NotNull(reason);
            Assert.Contains("chown -R 1000:1000 '/opt/darling/darling-keys/password-key'", reason);
            Assert.DoesNotContain("docker compose", reason);
        }
    }

    [Fact]
    public void SameOwner_OneName_IsNull()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(ComposeKey, 1654, links: 1);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Null(DarlingServiceKeyFile.UnexpectedOwnerReason(ComposeKey));
        }
    }

    private static string NewDirectory()
    {
        var dir = Path.Combine(Path.GetTempPath(), "darling-8141-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    [Fact]
    public void UnlistableDirectory_PasswordKey_IsRefusedAsUnreadable_NotMissing()
    {
        var dir = NewDirectory();
        try
        {
            var modes = new StandInUnixModes { EffectiveUserId = 1654 };
            modes.Report(dir, "700");
            modes.Unlistable.Add(dir);

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingPasswordKeyFile.Load(dir, NullLogger.Instance);

                Assert.True(load.Present);
                Assert.True(load.Untrusted);
                Assert.Contains("cannot be read (permission denied)", load.Refusal);
                Assert.DoesNotContain("missing", load.Refusal);

                var spki = PasswordPrivateKey.Generate().PublicKey.Spki;
                var published = new PublishedKey(PasswordSeal.KeyIdFor(spki), spki);
                var decision = DarlingPasswordKeyState.Decide(new PasswordKeyFacts(
                    load.Present, load.Untrusted, load.Refusal, null, load.FoundAfterOpenDirectory, published, false));

                Assert.Equal(PasswordKeyAction.Refused, decision.Action);
                Assert.DoesNotContain("--reset-password-key", decision.Reason);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void UnlistableDirectory_LogHashKey_IsRefused_NotGenerated()
    {
        var dir = NewDirectory();
        try
        {
            var modes = new StandInUnixModes { EffectiveUserId = 1654 };
            modes.Report(dir, "700");
            modes.Unlistable.Add(dir);

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingLogHashKeyFile.Load(dir, NullLogger.Instance);

                Assert.Null(load.Key);
                Assert.False(load.Generated);
                Assert.Contains("cannot be read (permission denied)", load.Refusal);
                Assert.Empty(Directory.GetFileSystemEntries(dir));

                /* The key is likely behind the directory: the chown keeps it, so no "delete it" advice. */
                Assert.EndsWith("Run the command above, then restart; the key is kept.", load.Refusal, StringComparison.Ordinal);
                Assert.DoesNotContain("delete it", load.Refusal, StringComparison.Ordinal);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void ListableDirectory_NoFile_StillAbsent()
    {
        var dir = NewDirectory();
        try
        {
            var modes = new StandInUnixModes { EffectiveUserId = 1654 };
            modes.Report(dir, "700");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingPasswordKeyFile.Load(dir, NullLogger.Instance);

                Assert.False(load.Present);
                Assert.False(load.Untrusted);
            }
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void CredentialOwnershipFix_TheVolumeRootItself_IsTheComposeCommand()
    {
        Assert.Equal(
            "docker compose run --rm --no-deps --user 0 --entrypoint chown darling -R 1654:1654 /var/lib/darling "
            + "(on Kubernetes, run the one-off root Job under Container user in docs/darling/security.md instead)",
            DarlingManagedRoles.CredentialOwnershipFix(1654, "/var/lib/darling"));
    }

    private const string ComposeSecretKey = "/run/secrets/password_key";

    [Fact]
    public void PasswordKeyFile_DeniedAndOwnedByAnotherUser_NamesBothUsersAndThePlainChown()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(ComposeSecretKey, 1000);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingPasswordKeyFileReader.CouldNotRead(ComposeSecretKey, new UnauthorizedAccessException("denied"));

            Assert.StartsWith("passwordKey: secret file '/run/secrets/password_key' could not be read: denied", refusal);
            Assert.Contains("owned by user 1000, not by user 1654", refusal);
            var command = refusal[refusal.IndexOf("run on the host", StringComparison.Ordinal)..];
            Assert.Contains("sudo chown 1654:1654 secrets/password_key", command);
            Assert.Contains("the password_key secret", command);
            Assert.Contains("Darling/compose/secrets/README.md", command);
            Assert.DoesNotContain("/run/secrets/", command);
            Assert.DoesNotContain("-R", command);
            Assert.DoesNotContain("docker compose run", refusal);
            Assert.DoesNotContain("missing", refusal);
        }
    }

    [Fact]
    public void PasswordKeyFile_DeniedButOwnedByTheService_GivesNoChown()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(ComposeSecretKey, 1654);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingPasswordKeyFileReader.CouldNotRead(ComposeSecretKey, new UnauthorizedAccessException("denied"));

            Assert.Equal("passwordKey: secret file '/run/secrets/password_key' could not be read: denied", refusal);
        }
    }

    [Fact]
    public void PasswordKeyFile_NotFound_GivesNoChown()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(ComposeSecretKey, 1000);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingPasswordKeyFileReader.CouldNotRead(ComposeSecretKey, new FileNotFoundException("gone"));

            Assert.Equal("passwordKey: secret file '/run/secrets/password_key' could not be read: gone", refusal);
        }
    }

    [Fact]
    public void PasswordKeyFile_DeniedOutsideTheSecretsMount_GivesThePlainChownWithoutTheHostNote()
    {
        const string path = "/opt/keys/password_key";
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(path, 0);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var refusal = DarlingPasswordKeyFileReader.CouldNotRead(path, new UnauthorizedAccessException("denied"));

            Assert.Contains("owned by user 0, not by user 1654", refusal);
            Assert.Contains("chown -R 1654:1654 '/opt/keys/password_key'", refusal);
            Assert.DoesNotContain("on the host", refusal);
        }
    }
}
