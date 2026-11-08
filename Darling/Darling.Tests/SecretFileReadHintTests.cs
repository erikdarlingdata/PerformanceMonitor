/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8141 review: a <c>file:</c> secret the service's user is denied stops the start before the credentials-directory
/// refusal can run, so its message carries the upgrade steps itself. The service's user comes from
/// <see cref="StandInUnixModes"/>.
/// </summary>
public sealed class SecretFileReadHintTests
{
    private const string StoreSecret = "/run/secrets/store_connection";
    private const string Setting = "postgres.connectionString";

    [Fact]
    public void DeniedUnderTheSecretsMount_AsNonRoot_GivesBothUpgradeLinesAndTheRestart()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead(Setting, StoreSecret, new UnauthorizedAccessException("denied"));

            Assert.StartsWith("postgres.connectionString: secret file '/run/secrets/store_connection' could not be read: denied", message);
            Assert.Contains("user 1654", message, StringComparison.Ordinal);
            var chmod = message.IndexOf("chmod 700 secrets && chmod 644 darling.json secrets/*", StringComparison.Ordinal);
            var chown = message.IndexOf("docker compose run --rm --no-deps --user 0 --entrypoint chown darling -R 1654:1654 /var/lib/darling", StringComparison.Ordinal);
            var restart = message.IndexOf("docker compose restart darling", StringComparison.Ordinal);
            Assert.True(chmod > 0, message);
            Assert.True(chown > chmod, message);
            Assert.True(restart > chown, message);
            Assert.Contains("in the compose folder", message, StringComparison.Ordinal);
            Assert.DoesNotContain("Kubernetes", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Denied_AsRoot_GivesNoHint()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 0 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Equal(
                "postgres.connectionString: secret file '/run/secrets/store_connection' could not be read: denied",
                DarlingSecretSource.CouldNotRead(Setting, StoreSecret, new UnauthorizedAccessException("denied")));
        }
    }

    [Fact]
    public void DeniedOutsideTheSecretsMount_GivesTheGenericHint()
    {
        const string path = "/opt/darling/store_connection";
        var modes = new StandInUnixModes { EffectiveUserId = 1000 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead(Setting, path, new UnauthorizedAccessException("denied"));

            Assert.StartsWith("postgres.connectionString: secret file '/opt/darling/store_connection' could not be read: denied", message);
            Assert.Contains("user 1000", message, StringComparison.Ordinal);
            Assert.Contains("'/opt/darling/store_connection'", message[message.IndexOf("denied", StringComparison.Ordinal)..], StringComparison.Ordinal);
            Assert.DoesNotContain("docker compose", message, StringComparison.Ordinal);
            Assert.DoesNotContain("chmod 644 darling.json", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void NotFound_IsUnchanged()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            Assert.Equal(
                "postgres.connectionString: secret file '/run/secrets/store_connection' could not be read: gone",
                DarlingSecretSource.CouldNotRead(Setting, StoreSecret, new FileNotFoundException("gone")));
        }
    }

    [Fact]
    public void NoUnixSeam_IsUnchanged()
    {
        Assert.Equal(
            "postgres.connectionString: secret file '/run/secrets/store_connection' could not be read: denied",
            DarlingSecretSource.CouldNotRead(Setting, StoreSecret, new UnauthorizedAccessException("denied")));
    }

    [Fact]
    public void Resolve_CarriesTheHint_WhenTheReadIsDenied()
    {
        /* Reading a directory as a file is denied on Windows, and fails as denied-or-IO elsewhere: either way the
           message Resolve throws is the one CouldNotRead builds for that exception. */
        var dir = Path.Combine(Path.GetTempPath(), "darling-8141-secret-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        try
        {
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var thrown = Assert.Throws<InvalidOperationException>(() => DarlingSecretSource.Resolve("file:" + dir, Setting));

                Assert.NotNull(thrown.InnerException);
                Assert.Equal(DarlingSecretSource.CouldNotRead(Setting, dir, thrown.InnerException), thrown.Message);
            }
        }
        finally
        {
            Directory.Delete(dir);
        }
    }
}
