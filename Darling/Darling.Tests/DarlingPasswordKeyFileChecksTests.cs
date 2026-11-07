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
using System.Linq;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The checks a <c>passwordKey</c> <c>file:</c> key gets (#8006 review, request 1): a target that is not a regular
/// file, is larger than the key-file limit, or that others could write to is refused; one owned by another user, with
/// more than one name, readable by others, or in a directory others can write to without the sticky bit loads with a
/// Warning naming the path and the check, so a Kubernetes secret mount (root-owned, 0644 or 0440) is never refused.
/// The Unix decisions are pinned here through <see cref="StandInUnixModes"/>; the real calls run in the Linux
/// self-check (<c>--self-check-password-key</c>).
/// </summary>
[Collection("darling-owned-secrets")]
public sealed class DarlingPasswordKeyFileChecksTests
{
    private const string KeyPath = "/run/secrets/password-key";
    private static readonly SecurityIdentifier s_builtinUsers = new(WellKnownSidType.BuiltinUsersSid, null);

    private static PasswordKeyFileFacts OwnFile(UnixFileMode mode) => new(
        RegularFile: true, Length: 2400, Mode: mode, Owner: new UnixFileOwner(1000, 1), EffectiveUserId: 1000,
        DirectoryModes: [Octal("700")], WritableByOrdinaryUsers: false, ReadableByOrdinaryUsers: false);

    private static UnixFileMode Octal(string octal) => (UnixFileMode)Convert.ToInt32(octal, 8);

    public static TheoryData<string, string> Refusals => new()
    {
        { "not a regular file", "is not a regular file" },
        { "larger than the limit", "larger than 8192 bytes" },
        { "group-writable", "can be written by its group or by other users (mode 664)" },
        { "writable by others", "can be written by its group or by other users (mode 602)" },
        { "writable by ordinary users on Windows", "can be written by Users, Authenticated Users, Everyone or INTERACTIVE" },
    };

    [Theory]
    [MemberData(nameof(Refusals))]
    public void Judge_RefusesAFileThatIsNoKeyFile_OrThatOthersCouldReplace(string shape, string expected)
    {
        var facts = shape switch
        {
            "not a regular file" => OwnFile(Octal("600")) with { RegularFile = false, Length = 0 },
            "larger than the limit" => OwnFile(Octal("600")) with { Length = 8193 },
            "group-writable" => OwnFile(Octal("664")),
            "writable by others" => OwnFile(Octal("602")),
            _ => new PasswordKeyFileFacts(true, 2400, null, null, 0, [], WritableByOrdinaryUsers: true, ReadableByOrdinaryUsers: true),
        };

        var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);

        Assert.NotNull(verdict.Refusal);
        Assert.StartsWith("passwordKey: file '" + KeyPath + "' ", verdict.Refusal, StringComparison.Ordinal);
        Assert.Contains(expected, verdict.Refusal, StringComparison.Ordinal);
        Assert.Empty(verdict.Warnings);
    }

    [Fact]
    public void Judge_ALimitSizedOwnerOnlyFile_IsNeitherRefusedNorWarned()
    {
        foreach (var facts in new[]
                 {
                     OwnFile(Octal("600")) with { Length = 8192 },
                     OwnFile(Octal("400")),
                     /* The Kubernetes default the docs advise: root-owned, 0440, read by a service that runs as root. */
                     OwnFile(Octal("440")) with { Owner = new UnixFileOwner(0, 1), EffectiveUserId = 0, DirectoryModes = [Octal("1777"), Octal("755")] },
                     new PasswordKeyFileFacts(true, 2400, null, null, 0, [], false, false),
                 })
        {
            var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);
            Assert.Null(verdict.Refusal);
            Assert.Empty(verdict.Warnings);
        }
    }

    public static TheoryData<string, string> Warnings => new()
    {
        { "owned by another user", "is owned by another user than the one the service runs as" },
        { "two names", "has more than one name" },
        { "readable by others", "is readable by other users" },
        { "open directory", "is in a directory that other users can write to and that has no sticky bit" },
        { "readable by ordinary users on Windows", "is readable by Users, Authenticated Users, Everyone or INTERACTIVE" },
    };

    [Theory]
    [MemberData(nameof(Warnings))]
    public void Judge_WarnsNamingThePathAndTheCheck_AndStillLoads(string shape, string expected)
    {
        var facts = shape switch
        {
            "owned by another user" => OwnFile(Octal("440")) with { Owner = new UnixFileOwner(0, 1) },
            "two names" => OwnFile(Octal("600")) with { Owner = new UnixFileOwner(1000, 2) },
            "readable by others" => OwnFile(Octal("604")),
            "open directory" => OwnFile(Octal("600")) with { DirectoryModes = [Octal("700"), Octal("777")] },
            _ => new PasswordKeyFileFacts(true, 2400, null, null, 0, [], WritableByOrdinaryUsers: false, ReadableByOrdinaryUsers: true),
        };

        var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);

        Assert.Null(verdict.Refusal);
        var warning = Assert.Single(verdict.Warnings);
        Assert.Equal(KeyPath, warning.Path);
        Assert.Contains("{Path}", warning.Template, StringComparison.Ordinal);
        Assert.StartsWith("passwordKey: ", warning.Sentence, StringComparison.Ordinal);
        Assert.Contains("'" + KeyPath + "'", warning.Sentence, StringComparison.Ordinal);
        Assert.Contains(expected, warning.Sentence, StringComparison.Ordinal);
        Assert.Contains("loaded anyway", warning.Sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void Judge_AKubernetesSecretAtItsDefault0644_ReadByAnotherUser_LoadsWithTwoWarnings()
    {
        var facts = OwnFile(Octal("644")) with { Owner = new UnixFileOwner(0, 1), DirectoryModes = [Octal("1777"), Octal("755")] };

        var verdict = DarlingPasswordKeyFileReader.Judge(KeyPath, facts);

        Assert.Null(verdict.Refusal);
        Assert.Equal(2, verdict.Warnings.Count);
        Assert.Contains("owned by another user", verdict.Warnings[0].Sentence, StringComparison.Ordinal);
        Assert.Contains("readable by other users", verdict.Warnings[1].Sentence, StringComparison.Ordinal);
    }

    [Fact]
    public void LoadFromSetting_OnUnix_WarnsOnceACheckPerCall_AndRefusesAWritableOrSpecialFile()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, secret);
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            modes.Report(dir, "755");
            modes.ReportOwner(path, userId: 0);
            modes.Report(path, "644");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                for (var call = 1; call <= 2; call++)
                {
                    var logger = new CapturingTestLogger();
                    var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, logger);
                    Assert.Null(load.Refusal);
                    Assert.NotNull(load.Pkcs8);
                    Assert.Equal(keyId, KeyIdOf(load.Pkcs8));
                    CryptographicOperations.ZeroMemory(load.Pkcs8);

                    Assert.Equal(2, logger.CountAtLevel(LogLevel.Warning));
                    var warnings = logger.Lines.Where(l => l.StartsWith("Warning:", StringComparison.Ordinal)).ToList();
                    Assert.All(warnings, w => Assert.Contains(path, w, StringComparison.Ordinal));
                    Assert.All(warnings, w => AssertNoKeyText(w, secret));
                    Assert.Contains(warnings, w => w.Contains("owned by another user", StringComparison.Ordinal));
                    Assert.Contains(warnings, w => w.Contains("readable by other users", StringComparison.Ordinal));
                }

                modes.Report(path, "664");
                var writable = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);
                AssertRefused(writable, "file:" + path, secret, "(mode 664)");

                modes.Report(path, "600");
                modes.ReportRegularFile(path, false);
                var special = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);
                AssertRefused(special, "file:" + path, secret, "is not a regular file");
            }
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void AFileOverTheKeyFileLimit_IsRefusedBeforeItIsRead()
    {
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, new string('A', 8193));

            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);

            AssertRefused(load, "file:" + path, new string('A', 8193), "larger than 8192 bytes");
        }
        finally
        {
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void OnWindows_ADevice_IsRefusedAsNotARegularFile()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "NUL is a Windows device name; the Linux self-check reads /dev/zero.");

        var load = DarlingPasswordKeySource.LoadFromSetting("file:NUL", null, NullLogger.Instance);

        AssertRefused(load, "file:NUL", "unused-key-text-0000", "is not a regular file");
        Assert.Contains("'NUL'", load.Refusal, StringComparison.Ordinal);
    }

    [Fact]
    public void OnWindows_AFileUsersCanWrite_IsRefused_AndOneTheyCanRead_LoadsWithAWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ACLs are Windows-only.");

        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var readable = Path.Combine(dir, "readable-key");
            var writable = Path.Combine(dir, "writable-key");
            File.WriteAllText(readable, secret);
            File.WriteAllText(writable, secret);
            Grant(readable, FileSystemRights.Read);
            Grant(writable, FileSystemRights.Write);

            var logger = new CapturingTestLogger();
            var load = DarlingPasswordKeySource.LoadFromSetting("file:" + readable, null, logger);
            Assert.Null(load.Refusal);
            Assert.NotNull(load.Pkcs8);
            Assert.Equal(keyId, KeyIdOf(load.Pkcs8));
            CryptographicOperations.ZeroMemory(load.Pkcs8);
            var warning = Assert.Single(logger.Lines, l => l.StartsWith("Warning:", StringComparison.Ordinal));
            Assert.Contains("passwordKey: ", warning, StringComparison.Ordinal);
            Assert.Contains(readable, warning, StringComparison.Ordinal);
            Assert.Contains("readable by Users", warning, StringComparison.Ordinal);
            AssertNoKeyText(warning, secret);

            var refused = DarlingPasswordKeySource.LoadFromSetting("file:" + writable, null, NullLogger.Instance);
            AssertRefused(refused, "file:" + writable, secret, "can be written by Users");
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void DescribeForValidation_PrintsEachWarningAsItsOwnLine_BeforeTheFingerprint()
    {
        var (pkcs8, keyId) = NewKey();
        var dir = Directory.CreateTempSubdirectory("password-key-checks-").FullName;
        var secret = Convert.ToBase64String(pkcs8);
        try
        {
            var path = Path.Combine(dir, "password-key");
            File.WriteAllText(path, secret);
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            modes.Report(dir, "777");
            modes.ReportOwner(path, userId: 0);
            modes.Report(path, "644");
            var config = DarlingConfig.Parse(
                "{\"postgres\":{\"connectionString\":\"Host=h\"},\"passwordKey\":" + JsonSerializer.Serialize("file:" + path) + "}");

            IReadOnlyList<string> lines;
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                lines = DarlingPasswordKeySource.DescribeForValidation(config, null);
            }

            Assert.Equal(4, lines.Count);
            for (var i = 0; i < 3; i++)
            {
                Assert.StartsWith("WARNING: passwordKey: file '" + path + "' ", lines[i], StringComparison.Ordinal);
                Assert.DoesNotContain("{Path}", lines[i], StringComparison.Ordinal);
            }

            Assert.Contains("owned by another user", lines[0], StringComparison.Ordinal);
            Assert.Contains("readable by other users", lines[1], StringComparison.Ordinal);
            Assert.Contains("no sticky bit", lines[2], StringComparison.Ordinal);
            Assert.Equal("passwordKey: password key " + PasswordSeal.DisplayKeyId(keyId), lines[3]);
            Assert.All(lines, l => AssertNoKeyText(l, secret));
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pkcs8);
            Directory.Delete(dir, recursive: true);
        }
    }

    private static void Grant(string path, FileSystemRights rights)
    {
        var info = new FileInfo(path);
        var security = info.GetAccessControl();
        security.AddAccessRule(new FileSystemAccessRule(s_builtinUsers, rights, AccessControlType.Allow));
        info.SetAccessControl(security);
    }

    private static (byte[] Pkcs8, string KeyId) NewKey()
    {
        using var key = PasswordPrivateKey.Generate();
        return (key.ExportPkcs8(), key.PublicKey.KeyId);
    }

    private static string KeyIdOf(byte[] pkcs8)
    {
        using var key = PasswordPrivateKey.FromPkcs8(pkcs8);
        return key.PublicKey.KeyId;
    }

    private static void AssertRefused(PasswordKeyFileLoad load, string reference, string secret, string expected)
    {
        Assert.True(load.Present);
        Assert.True(load.Untrusted);
        Assert.Null(load.Pkcs8);
        Assert.Equal(reference, load.Path);
        Assert.NotNull(load.Refusal);
        Assert.StartsWith("passwordKey: ", load.Refusal, StringComparison.Ordinal);
        Assert.Contains(reference["file:".Length..], load.Refusal, StringComparison.Ordinal);
        Assert.Contains(expected, load.Refusal, StringComparison.Ordinal);
        AssertNoKeyText(load.Refusal, secret);
    }

    /// <summary>The sentence holds neither the key text nor any 16-character slice of it.</summary>
    private static void AssertNoKeyText(string sentence, string secret)
    {
        Assert.DoesNotContain(secret, sentence, StringComparison.Ordinal);
        for (var i = 0; i + 16 <= secret.Length; i++)
        {
            Assert.False(sentence.AsSpan().Contains(secret.AsSpan(i, 16), StringComparison.Ordinal), $"the sentence holds key text at offset {i}");
        }
    }
}
