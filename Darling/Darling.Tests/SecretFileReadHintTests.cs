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
/// <see cref="StandInUnixModes"/>. #9793: a missing file and a denied one each say what is true of that file, and the token
/// failure line carries DPAPI advice only when the setting read was encryptedToken.
/// </summary>
public sealed class SecretFileReadHintTests
{
    private const string StoreSecret = "/run/secrets/store_connection";
    private const string Setting = "postgres.connectionString";
    private const string McpSecret = "/run/secrets/mcp_token";

    [Fact]
    public void DeniedUnderTheSecretsMount_AsNonRoot_GivesBothUpgradeLinesAndTheRestart()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.ReportOwner(StoreSecret, 1000);
        modes.Report(StoreSecret, "600");
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead(Setting, StoreSecret, new UnauthorizedAccessException("denied"));

            Assert.StartsWith("postgres.connectionString: secret file '/run/secrets/store_connection' could not be read: denied", message);
            Assert.Contains("user 1654", message, StringComparison.Ordinal);
            Assert.Contains("owned by user 1000", message, StringComparison.Ordinal);
            Assert.Contains("mode is 0600", message, StringComparison.Ordinal);
            var chmod = message.IndexOf(DarlingSecretSource.ComposeSecretsModeFix, StringComparison.Ordinal);
            var chown = message.IndexOf("docker compose run --rm --no-deps --user 0 --entrypoint chown darling -R 1654:1654 /var/lib/darling", StringComparison.Ordinal);
            var restart = message.IndexOf("docker compose restart darling", StringComparison.Ordinal);
            Assert.True(chmod > 0, message);
            Assert.True(chown > chmod, message);
            Assert.Contains("chmod 700 secrets && chmod 644 darling.json && find secrets -type f ! -name password_key -exec chmod 644 {} +", message, StringComparison.Ordinal);
            Assert.DoesNotContain("chmod 600", message, StringComparison.Ordinal);
            Assert.True(restart > chown, message);
            Assert.Contains("in the compose folder", message, StringComparison.Ordinal);
            Assert.DoesNotContain("Kubernetes", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Denied_AsRoot_NamesTheAccountAndGivesNoUpgradeSteps()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 0 };
        modes.Report(StoreSecret, "600");
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead(Setting, StoreSecret, new UnauthorizedAccessException("denied"));

            Assert.StartsWith("postgres.connectionString: secret file '/run/secrets/store_connection' could not be read: denied", message);
            Assert.Contains("user 0", message, StringComparison.Ordinal);
            Assert.DoesNotContain("docker compose", message, StringComparison.Ordinal);
            Assert.DoesNotContain("chmod", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DeniedOutsideTheSecretsMount_GivesTheGenericHint()
    {
        const string path = "/opt/darling/store_connection";
        var modes = new StandInUnixModes { EffectiveUserId = 1000 };
        modes.Report(path, "640");
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Missing_UnderTheSecretsMount_SaysTheFileIsNotThere_AndHowComposeMountsIt(bool directoryMissing)
    {
        Exception missing = directoryMissing ? new DirectoryNotFoundException("no dir") : new FileNotFoundException("no file");
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead("mcp.network.token", McpSecret, missing);

            Assert.StartsWith("mcp.network.token: secret file '/run/secrets/mcp_token' could not be read: ", message);
            Assert.Contains("not there", message, StringComparison.Ordinal);
            Assert.Contains("'mcp_token'", message, StringComparison.Ordinal);
            Assert.Contains("docker compose up -d", message, StringComparison.Ordinal);
            Assert.Contains("docker compose restart", message, StringComparison.Ordinal);
            Assert.DoesNotContain("DPAPI", message, StringComparison.Ordinal);
            Assert.DoesNotContain("--encrypt-password", message, StringComparison.Ordinal);
            Assert.DoesNotContain("which cannot read it", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Missing_OutsideTheSecretsMount_NamesThePath_WithoutNamingAComposeSecret()
    {
        const string path = "/opt/darling/mcp_token";
        var modes = new StandInUnixModes { EffectiveUserId = 1000 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead("mcp.network.token", path, new FileNotFoundException("no file"));

            Assert.Contains("not there: '/opt/darling/mcp_token'", message, StringComparison.Ordinal);
            Assert.Contains("mcp.network.token", message[message.IndexOf("not there", StringComparison.Ordinal)..], StringComparison.Ordinal);
            Assert.DoesNotContain("is where compose mounts", message, StringComparison.Ordinal);
            Assert.DoesNotContain("DPAPI", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Denied_NamesTheAccountByIdAndName_AndTheFilesOwnerAndMode()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654, EffectiveUserName = "app" };
        modes.ReportOwner(McpSecret, 1000);
        modes.Report(McpSecret, "600");
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead("mcp.network.token", McpSecret, new UnauthorizedAccessException("denied"));

            Assert.Contains("runs as user 1654 (app)", message, StringComparison.Ordinal);
            Assert.Contains("owned by user 1000", message, StringComparison.Ordinal);
            Assert.Contains("mode is 0600", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Denied_WhenTheOwnerCannotBeRead_StillGivesTheMode()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        modes.OwnerThrows[McpSecret] = new IOException("stat failed");
        modes.Report(McpSecret, "644");
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var message = DarlingSecretSource.CouldNotRead("mcp.network.token", McpSecret, new UnauthorizedAccessException("denied"));

            Assert.Contains("mode is 0644", message, StringComparison.Ordinal);
            Assert.DoesNotContain("owned by", message, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Denied_WithNoAccountNameAndNoFileFacts_SaysOnlyWhatItKnows()
    {
        using (ComposeCredentialDirectoryGuard.BeginForTest(new UnstatableModes()))
        {
            var message = DarlingSecretSource.CouldNotRead("mcp.network.token", McpSecret, new UnauthorizedAccessException("denied"));

            Assert.Contains("runs as user 1654, which cannot read it", message, StringComparison.Ordinal);
            Assert.DoesNotContain("(", message[message.IndexOf("runs as", StringComparison.Ordinal)..message.IndexOf("which cannot", StringComparison.Ordinal)], StringComparison.Ordinal);
            Assert.DoesNotContain("owned by", message, StringComparison.Ordinal);
            Assert.DoesNotContain("mode is", message, StringComparison.Ordinal);
        }
    }

    /// <summary>A Unix seam whose stat calls all fail, as when the secret's folder cannot be searched.</summary>
    private sealed class UnstatableModes : IUnixDirectoryModes
    {
        public uint EffectiveUserId => 1654;

        public void CreateOwnerOnly(string directory) => throw new NotSupportedException();

        public UnixFileMode Get(string directory) => throw new UnauthorizedAccessException("stat denied");

        public void Set(string directory, UnixFileMode mode) => throw new NotSupportedException();

        public UnixFileOwner? OwnerOf(string path) => throw new UnauthorizedAccessException("stat denied");
    }

    [Fact]
    public void Denied_NeverCarriesTheFilesContentOrLength()
    {
        const string content = "tok-9f3a-never-printed-0123456789";
        var path = Path.Combine(Path.GetTempPath(), "darling-9793-secret-" + Guid.NewGuid().ToString("N"));
        File.WriteAllText(path, content);
        try
        {
            var modes = new StandInUnixModes { EffectiveUserId = 1654 };
            modes.Report(path, "600");
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var message = DarlingSecretSource.CouldNotRead("mcp.network.token", path, new UnauthorizedAccessException("denied"));

                Assert.DoesNotContain(content, message, StringComparison.Ordinal);
                Assert.DoesNotContain(content.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) + " bytes", message, StringComparison.Ordinal);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void NoUnixSeam_MissingFile_IsTheWindowsWording()
    {
        Assert.Equal(
            "mcp.network.token: secret file '/run/secrets/mcp_token' could not be read: gone",
            DarlingSecretSource.CouldNotRead("mcp.network.token", McpSecret, new FileNotFoundException("gone")));
    }

    [Fact]
    public void Resolve_OfAMissingComposeSecret_ThenTheTokenLine_NamesTheMountAndNoDpapi()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654, EffectiveUserName = "app" };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var thrown = Assert.Throws<InvalidOperationException>(() => DarlingSecretSource.Resolve("file:" + McpSecret, "mcp.network.token"));
            var line = DarlingSecretSource.DescribeTokenReadFailure(
                "MCP network token", "mcp.network", "MCP server not started.", thrown.Message, encrypted: false);

            Assert.Contains("'/run/secrets/mcp_token'", line, StringComparison.Ordinal);
            Assert.Contains("docker compose up -d", line, StringComparison.Ordinal);
            Assert.Contains("MCP server not started.", line, StringComparison.Ordinal);
            Assert.DoesNotContain("DPAPI", line, StringComparison.Ordinal);
            Assert.DoesNotContain("--encrypt-password", line, StringComparison.Ordinal);
            Assert.DoesNotContain("another machine", line, StringComparison.Ordinal);
            Assert.DoesNotContain(".exe", line, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("MCP network token", "mcp.network", "MCP server not started.")]
    [InlineData("Web dashboard token", "web.network", "The web dashboard is not started.")]
    public void TokenReadFailure_EncryptedTokenSet_KeepsTheDpapiAdvice(string tokenName, string section, string consequence)
    {
        var line = DarlingSecretSource.DescribeTokenReadFailure(tokenName, section, consequence, "the cause", encrypted: true);

        Assert.Equal(
            $"{tokenName} ({section}.encryptedToken) could not be decrypted (the cause). {consequence} "
            + "A token made on another machine or account cannot be read here. "
            + $"Run PerformanceMonitor.Darling.Service.exe --encrypt-password on this machine and put the new value in {section}.encryptedToken. "
            + $"On another OS, remove it and set {section}.token to an env: or file: reference.",
            line);
    }

    [Theory]
    [InlineData("MCP network token", "mcp.network", "MCP server not started.")]
    [InlineData("Web dashboard token", "web.network", "The web dashboard is not started.")]
    public void TokenReadFailure_TokenReference_NamesTheCauseAndNoDpapiAdvice(string tokenName, string section, string consequence)
    {
        var line = DarlingSecretSource.DescribeTokenReadFailure(tokenName, section, consequence, "the cause", encrypted: false);

        Assert.StartsWith(tokenName, line, StringComparison.Ordinal);
        Assert.Contains("the cause", line, StringComparison.Ordinal);
        Assert.Contains(consequence, line, StringComparison.Ordinal);
        Assert.DoesNotContain("decrypted", line, StringComparison.Ordinal);
        Assert.DoesNotContain("--encrypt-password", line, StringComparison.Ordinal);
        Assert.DoesNotContain("encryptedToken", line, StringComparison.Ordinal);
        Assert.DoesNotContain("another machine", line, StringComparison.Ordinal);
    }

    /// <summary>The setting <c>ResolveToken</c> read, as the host's catch sites decide it, and the line that follows.</summary>
    private static string LineFor(McpNetworkConfig network)
    {
        var thrown = Record.Exception(() => network.ResolveToken(out _));
        Assert.NotNull(thrown);
        return DarlingSecretSource.DescribeTokenReadFailure(
            "MCP network token", "mcp.network", "MCP server not started.", thrown.Message, !string.IsNullOrWhiteSpace(network.EncryptedToken));
    }

    [Fact]
    public void TokenLine_ForATokenFileReference_NamesTheFileAndNoDpapiAdvice_OnAnyOs()
    {
        var modes = new StandInUnixModes { EffectiveUserId = 1654 };
        using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
        {
            var line = LineFor(new McpNetworkConfig { Token = "file:" + McpSecret });

            Assert.Contains("'/run/secrets/mcp_token'", line, StringComparison.Ordinal);
            Assert.Contains("MCP server not started.", line, StringComparison.Ordinal);
            Assert.DoesNotContain("decrypted", line, StringComparison.Ordinal);
            Assert.DoesNotContain("--encrypt-password", line, StringComparison.Ordinal);
            Assert.DoesNotContain("another machine", line, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TokenLine_ForAnEncryptedToken_KeepsTheDpapiAdvice_OnAnyOs()
    {
        var line = LineFor(new McpNetworkConfig { EncryptedToken = "AAAA" });

        Assert.Contains("(mcp.network.encryptedToken) could not be decrypted", line, StringComparison.Ordinal);
        Assert.Contains("--encrypt-password", line, StringComparison.Ordinal);
        Assert.Contains("remove it and set mcp.network.token to an env: or file: reference", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TokenLine_WhenBothAreSet_FollowsTheEncryptedTokenResolveTokenReads()
    {
        var line = LineFor(new McpNetworkConfig { EncryptedToken = "AAAA", Token = "file:" + McpSecret });

        Assert.Contains("could not be decrypted", line, StringComparison.Ordinal);
        Assert.DoesNotContain(McpSecret, line, StringComparison.Ordinal);
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
            modes.Report(dir, "700");
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var thrown = Assert.Throws<InvalidOperationException>(() => DarlingSecretSource.Resolve("file:" + dir, Setting));

                Assert.NotNull(thrown.InnerException);
                Assert.Contains("user 1000", thrown.Message, StringComparison.Ordinal);
                Assert.Equal(DarlingSecretSource.CouldNotRead(Setting, dir, thrown.InnerException), thrown.Message);
            }
        }
        finally
        {
            Directory.Delete(dir);
        }
    }
}
