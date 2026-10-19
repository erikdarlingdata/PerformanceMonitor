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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store disk-pressure alert judges the volume that holds the store's data directory. A data directory
/// on a volume mounted at a folder is on a different volume from its drive root, and the alert exists to
/// warn before the store's own disk fills, so a read from the drive letter would raise or hold the alert on
/// the wrong disk.
/// </summary>
public sealed class StoreDiskPressureVolumeTests
{
    private const long OneGb = 1024L * 1024 * 1024;

    /// <summary>The read is asked for the data directory itself, never for the drive root above it, and both
    /// figures come back from that one read.</summary>
    [Fact]
    public void ReadStoreVolumeSpace_AsksTheDataDirectoryItself_NotItsDriveRoot()
    {
        var dataDirectory = Directory.CreateTempSubdirectory("pm-alert-volume-").FullName;
        try
        {
            var asked = new List<string>();

            var (free, total) = DarlingWorker.ReadStoreVolumeSpace(dataDirectory, directory =>
            {
                asked.Add(directory);
                return (64 * OneGb, 120 * OneGb);
            });

            Assert.Equal(64 * OneGb, free);
            Assert.Equal(120 * OneGb, total);
            Assert.Equal(new[] { dataDirectory }, asked);
        }
        finally
        {
            Directory.Delete(dataDirectory);
        }
    }

    /// <summary>A read that fails is not turned into a number. The alert's own catch decides what a failed read
    /// means (no signal this tick), and a made-up figure would raise or clear the alert on nothing.</summary>
    [Fact]
    public void ReadStoreVolumeSpace_ReadFails_Throws()
    {
        var dataDirectory = Directory.CreateTempSubdirectory("pm-alert-volume-").FullName;
        try
        {
            Assert.Throws<IOException>(() => DarlingWorker.ReadStoreVolumeSpace(
                dataDirectory, _ => throw new IOException("the volume is not ready")));
        }
        finally
        {
            Directory.Delete(dataDirectory);
        }
    }

    /// <summary>Left to itself the read answers for a real directory: a positive size, and free space that
    /// fits inside it.</summary>
    [Fact]
    public void ReadStoreVolumeSpace_RealDirectory_ReportsItsVolume()
    {
        var (free, total) = DarlingWorker.ReadStoreVolumeSpace(Path.GetTempPath());

        Assert.True(total > 0);
        Assert.InRange(free, 0L, total);
    }

    /// <summary>The alert's gather reads through <c>ReadStoreVolumeSpace</c> and no longer builds a
    /// <c>DriveInfo</c> for the data directory's drive letter. The gather is a private member of a worker
    /// that needs a running store, so the wiring is pinned in the source.</summary>
    [Fact]
    public void EvaluateStoreDiskPressureAsync_ReadsTheStoreVolume_NotItsDriveLetter()
    {
        var stripped = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        var declaration = Regex.Match(
            stripped,
            @"^[ \t]*private[^\r\n=]*?\bEvaluateStoreDiskPressureAsync\s*\(",
            RegexOptions.Multiline);
        Assert.True(declaration.Success, "EvaluateStoreDiskPressureAsync has no declaration: a rename has moved it out from under this pin");

        var body = CSharpSourceWalker.BraceBalanced(stripped, stripped.IndexOf('{', declaration.Index));

        Assert.Contains("ReadStoreVolumeSpace(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("DriveInfo", body, StringComparison.Ordinal);
        Assert.DoesNotContain("GetPathRoot", body, StringComparison.Ordinal);
    }
}
