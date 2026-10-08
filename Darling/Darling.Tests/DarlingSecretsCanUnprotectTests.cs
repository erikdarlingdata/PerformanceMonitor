/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="DarlingSecrets.CanUnprotect"/> answers whether this machine opens a saved old-format value without
/// returning it (#5456), and throws what <see cref="DarlingSecrets.Unprotect"/> throws for anything else.
/// </summary>
public sealed class DarlingSecretsCanUnprotectTests
{
    [Fact]
    [SupportedOSPlatform("windows")]
    public void ARealBlobOpens_OtherTextThrows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Windows data protection is needed to make a real blob.");

        var blob = DarlingSecrets.Protect("p@ss-not-real");
        Assert.True(DarlingSecrets.CanUnprotect(blob));

        Assert.Throws<CryptographicException>(() => DarlingSecrets.CanUnprotect(Convert.ToBase64String(new byte[] { 1, 2, 3, 4 })));
        Assert.ThrowsAny<FormatException>(() => DarlingSecrets.CanUnprotect("not base64 at all!"));
        Assert.Throws<ArgumentException>(() => DarlingSecrets.CanUnprotect("  "));
    }

    [Fact]
    public void TheCheck_BuildsNoString_FromTheOpenedBytes()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingSecrets.cs");
        var start = source.IndexOf("internal static bool CanUnprotect(", StringComparison.Ordinal);
        Assert.True(start > 0);
        var end = source.IndexOf("/// <summary>", start, StringComparison.Ordinal);
        var body = source[start..end];

        Assert.Contains("CryptographicOperations.ZeroMemory(plainBytes);", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GetString", body, StringComparison.Ordinal);
    }
}
