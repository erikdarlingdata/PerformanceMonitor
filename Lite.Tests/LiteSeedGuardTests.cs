/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using UiSmokeWpf;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The UI smoke walk's seed writes into Lite's one data folder, so it refuses to run where that folder already exists unless
/// the CI variable is set. Each case points the check at its own temp folder; the real folder is never named.
/// </summary>
public sealed class LiteSeedGuardTests : IDisposable
{
    private readonly string _root = Path.Combine(Path.GetTempPath(), "LiteSeedGuard_" + Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch (IOException) { } catch (UnauthorizedAccessException) { }
    }

    [Fact]
    public void FolderThatDoesNotExist_MaySeed()
    {
        Assert.Null(LiteSeedGuard.Refusal(Path.Combine(_root, "PerformanceMonitorLite-Data"), ciValue: null));
    }

    [Fact]
    public void FolderThatExists_IsRefused_WithTheFolderNamed()
    {
        var folder = Path.Combine(_root, "PerformanceMonitorLite-Data");
        Directory.CreateDirectory(folder);

        var message = LiteSeedGuard.Refusal(folder, ciValue: null);

        Assert.NotNull(message);
        Assert.Contains(folder, message, StringComparison.Ordinal);
        Assert.NotNull(LiteSeedGuard.Refusal(folder, ciValue: ""));
    }

    [Fact]
    public void FolderThatExists_MaySeed_WhenTheCiVariableIsSet()
    {
        var folder = Path.Combine(_root, "PerformanceMonitorLite-Data");
        Directory.CreateDirectory(folder);

        Assert.Null(LiteSeedGuard.Refusal(folder, ciValue: "true"));
    }
}
