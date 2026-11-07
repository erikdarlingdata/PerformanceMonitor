/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
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
    public void OutsideTheComposeVolume_GivesAPlainChownOfThePath()
    {
        const string path = "/opt/darling/darling-keys/password-key";
        var modes = new StandInUnixModes { EffectiveUserId = 1000 };
        modes.ReportOwner(path, 0);

        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var reason = DarlingServiceKeyFile.UnexpectedOwnerReason(path);

            Assert.NotNull(reason);
            Assert.Contains("chown 1000:1000 '/opt/darling/darling-keys/password-key'", reason);
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

    [Fact]
    public void CredentialOwnershipFix_TheVolumeRootItself_IsTheComposeCommand()
    {
        Assert.Equal(
            "docker compose run --rm --no-deps --user 0 --entrypoint chown darling -R 1654:1654 /var/lib/darling "
            + "(on Kubernetes, chown the volume mounted at /var/lib/darling to 1654:1654)",
            DarlingManagedRoles.CredentialOwnershipFix(1654, "/var/lib/darling"));
    }
}
