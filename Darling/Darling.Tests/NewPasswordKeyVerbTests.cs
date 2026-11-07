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
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8006: <c>--new-password-key &lt;path&gt;</c> writes a new password key for a <c>passwordKey</c> secret and prints only
/// its fingerprint; it never writes over anything. With the setting, <c>--reset-password-key</c> says to put a new key in
/// the <c>passwordKey</c> source instead of saying the service will make one.
/// </summary>
public sealed class NewPasswordKeyVerbTests : IDisposable
{
    private readonly string _scratch = Directory.CreateTempSubdirectory("new-password-key-test-").FullName;

    public void Dispose()
    {
        try
        {
            Directory.Delete(_scratch, recursive: true);
        }
        catch (IOException)
        {
            /* A leftover temporary directory changes nothing the tests found. */
        }
    }

    [Fact]
    public void TheVerbWritesALoadableKey_PrintsOnlyItsFingerprint()
    {
        var path = Path.Combine(_scratch, "password-key");
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exit = DarlingCliCommands.NewPasswordKey(new[] { path }, output, error);

        Assert.Equal(0, exit);
        Assert.Equal("", error.ToString());

        var load = DarlingPasswordKeySource.LoadFromSetting("file:" + path, null, NullLogger.Instance);
        Assert.Null(load.Refusal);
        Assert.NotNull(load.Pkcs8);
        string keyId;
        using (var key = PasswordPrivateKey.FromPkcs8(load.Pkcs8))
        {
            keyId = key.PublicKey.KeyId;
        }

        CryptographicOperations.ZeroMemory(load.Pkcs8);

        var printed = output.ToString();
        Assert.Equal(
            $"Wrote a new password key to {path} (fingerprint {PasswordSeal.DisplayKeyId(keyId)}). Put it in your secret store, point passwordKey at it, then delete this file." + Environment.NewLine,
            printed);

        var text = File.ReadAllText(path).Trim();
        Assert.True(text.Length > 100, "the file holds a base64 key");
        Assert.EndsWith("\n", File.ReadAllText(path), StringComparison.Ordinal);
        for (var i = 0; i + 16 <= text.Length; i++)
        {
            Assert.False(printed.AsSpan().Contains(text.AsSpan(i, 16), StringComparison.Ordinal), $"the output holds key text at offset {i}");
        }
    }

    [Fact]
    public void TheVerbRefusesAPathThatExists_AndLeavesItUnchanged()
    {
        var path = Path.Combine(_scratch, "existing");
        var before = new byte[] { 1, 2, 3, 4, 5 };
        File.WriteAllBytes(path, before);
        using var output = new StringWriter();
        using var error = new StringWriter();

        var exit = DarlingCliCommands.NewPasswordKey(new[] { path }, output, error);

        Assert.Equal(1, exit);
        Assert.Equal(before, File.ReadAllBytes(path));
        Assert.Contains(path, error.ToString(), StringComparison.Ordinal);
        Assert.Equal("", output.ToString());

        /* A directory is something at the path too. */
        using var dirError = new StringWriter();
        Assert.Equal(1, DarlingCliCommands.NewPasswordKey(new[] { _scratch }, TextWriter.Null, dirError));
        Assert.Contains(_scratch, dirError.ToString(), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void TheVerbWithNoPath_IsAUsageError(int count)
    {
        var args = new string[count];
        for (var i = 0; i < count; i++)
        {
            args[i] = Path.Combine(_scratch, "key-" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        using var output = new StringWriter();
        using var error = new StringWriter();

        Assert.Equal(1, DarlingCliCommands.NewPasswordKey(args, output, error));
        Assert.Contains("--new-password-key <path>", error.ToString(), StringComparison.Ordinal);
        Assert.Equal("", output.ToString());
        Assert.Empty(Directory.GetFileSystemEntries(_scratch));
    }

    [Theory]
    [InlineData("--new-password-key", true)]
    [InlineData("--NEW-PASSWORD-KEY", true)]
    [InlineData("--new-password", false)]
    [InlineData("--reset-password-key", false)]
    public void TheVerbIsRecognizedCaseInsensitively_AndIsAKnownVerb(string arg, bool expected)
    {
        Assert.Equal(expected, DarlingCliCommands.IsNewPasswordKeyVerb(arg));
        Assert.Equal(expected || DarlingCliCommands.IsResetPasswordKeyVerb(arg), DarlingCliCommands.IsKnownVerb(arg));
    }

    [Fact]
    public void TheUsageTextNamesTheVerb() =>
        Assert.Contains("--new-password-key", DarlingCliCommands.UsageText(), StringComparison.Ordinal);

    [Fact]
    public void ProgramDispatchesTheVerb()
    {
        var program = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Program.cs");
        Assert.Contains("IsNewPasswordKeyVerb(args[0])", program, StringComparison.Ordinal);
        Assert.Contains("DarlingCliCommands.NewPasswordKey(args[1..], Console.Out, Console.Error)", program, StringComparison.Ordinal);
    }

    [Fact]
    public void TheResetSentence_SaysPutANewKey_WithTheSetting_AndIsUnchangedWithout()
    {
        Assert.Equal(
            "Restart the service. It will make a new password key, and these 3 saved passwords must be entered again.",
            DarlingCliCommands.ResetPasswordKeyRestartSentence(3));
        Assert.Equal(
            "Restart the service. It will make a new password key, and this 1 saved password must be entered again.",
            DarlingCliCommands.ResetPasswordKeyRestartSentence(1));
        Assert.Equal(
            "Put a new password key in the passwordKey source (make one with --new-password-key), then restart the service. These 3 saved passwords must be entered again.",
            DarlingCliCommands.ResetPasswordKeyRestartSentence(3, true));
        Assert.Equal(
            "Put a new password key in the passwordKey source (make one with --new-password-key), then restart the service. This 1 saved password must be entered again.",
            DarlingCliCommands.ResetPasswordKeyRestartSentence(1, true));
    }
}
