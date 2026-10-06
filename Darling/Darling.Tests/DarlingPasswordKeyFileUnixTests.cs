/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Unix half of the password key file (#5366), run against the operating system itself: the real owner and link
/// count read, and a key generated, loaded and refused through <see cref="PlatformUnixDirectoryModes"/> with no stand-in.
/// Every test skips on Windows, which judges an ACL instead. The test project targets Windows, so it cannot run on a
/// Linux runner; a developer on macOS or Linux, or the container's own start-up check, is where these run.
/// </summary>
[UnsupportedOSPlatform("windows")]
public sealed class DarlingPasswordKeyFileUnixTests
{
    private const string UnixOnly = "the owner and link count are read from the Unix file system; Windows judges an ACL";

    [Fact]
    public void ANewFile_ReportsTheRunningUserAsOwner_AndOneName()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), UnixOnly);
        var directory = NewDirectory();
        try
        {
            var path = WriteFile(directory, "example-content");

            var owner = FileIdentity.UnixOwnerOf(path);

            Assert.NotNull(owner);
            Assert.Equal(ReadUserIdFromTheSystem(), owner.Value.UserId);
            Assert.Equal(FileIdentity.EffectiveUserId(), owner.Value.UserId);
            Assert.Equal(1UL, owner.Value.Links);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AFileWithASecondName_ReportsTwoNames()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), UnixOnly);
        var directory = NewDirectory();
        try
        {
            var path = WriteFile(directory, "example-content");
            LinkTo(path, Path.Combine(directory, "example-second-name"));

            var owner = FileIdentity.UnixOwnerOf(path);

            Assert.NotNull(owner);
            Assert.Equal(2UL, owner.Value.Links);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ASymbolicLink_IsFollowed_ToTheFileItNames()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), UnixOnly);
        var directory = NewDirectory();
        try
        {
            var path = WriteFile(directory, "example-content");
            var link = Path.Combine(directory, "example-symbolic-name");
            File.CreateSymbolicLink(link, path);

            var owner = FileIdentity.UnixOwnerOf(link);

            Assert.NotNull(owner);
            Assert.Equal(1UL, owner.Value.Links);
            Assert.Equal(FileIdentity.EffectiveUserId(), owner.Value.UserId);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void APathWithNothingThere_ReadsAsNothing_NotAsAnOwner()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), UnixOnly);
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);

            Assert.Null(FileIdentity.UnixOwnerOf(Path.Combine(directory, "example-absent")));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AKey_GeneratedAndLoadedThroughThePlatformsOwnCalls_RoundTrips_AndIsOwnerOnly()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), UnixOnly);
        Assert.IsType<PlatformUnixDirectoryModes>(ComposeCredentialDirectoryGuard.Current.UnixModes);
        var directory = NewDirectory();
        try
        {
            using var rsa = RSA.Create(DarlingPasswordKeyFile.KeyBits);
            var pkcs8 = rsa.ExportPkcs8PrivateKey();

            var generated = DarlingPasswordKeyFile.Generate(directory, () => (byte[])pkcs8.Clone(), NullLogger.Instance);
            var loaded = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(generated.Refusal);
            Assert.Null(loaded.Refusal);
            Assert.Equal(pkcs8, loaded.Pkcs8);
            Assert.Equal(
                UnixFileMode.UserRead | UnixFileMode.UserWrite,
                File.GetUnixFileMode(Path.Combine(directory, DarlingPasswordKeyFile.FileName)));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AKeyFile_WithASecondName_IsRefused_ByTheRealLinkCount()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), UnixOnly);
        var directory = NewDirectory();
        try
        {
            using var rsa = RSA.Create(DarlingPasswordKeyFile.KeyBits);
            var pkcs8 = rsa.ExportPkcs8PrivateKey();
            Assert.Null(DarlingPasswordKeyFile.Generate(directory, () => (byte[])pkcs8.Clone(), NullLogger.Instance).Refusal);
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            LinkTo(path, Path.Combine(directory, "example-second-name"));

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.True(load.Present);
            Assert.True(load.Untrusted);
            Assert.Null(load.Pkcs8);
            Assert.Contains("2 names, not one", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(DarlingPasswordKeyFile.FileName, load.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Remove(directory);
        }
    }

    private static string NewDirectory() =>
        Path.Combine(Path.GetTempPath(), "darling-5366-unix-" + Guid.NewGuid().ToString("N"), "keys");

    private static string WriteFile(string directory, string content)
    {
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "example-file");
        File.WriteAllText(path, content);
        return path;
    }

    private static void Remove(string directory)
    {
        var root = Path.GetDirectoryName(directory)!;
        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }

    /// <summary>The user id the system reports for this process, asked of <c>id</c> rather than of the call under test.</summary>
    private static uint ReadUserIdFromTheSystem() =>
        uint.Parse(RunTool("id", ["-u"]).Trim(), CultureInfo.InvariantCulture);

    /// <summary>Gives <paramref name="existing"/> a second name with the system's own <c>ln</c>.</summary>
    private static void LinkTo(string existing, string second) => RunTool("ln", [existing, second]);

    private static string RunTool(string tool, string[] arguments)
    {
        var start = new ProcessStartInfo(tool) { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }

        using var process = Process.Start(start) ?? throw new InvalidOperationException($"{tool} could not be started.");
        var output = process.StandardOutput.ReadToEnd();
        var error = process.StandardError.ReadToEnd();
        process.WaitForExit();
        Assert.True(process.ExitCode == 0, $"{tool} failed: {error}");
        return output;
    }
}
