/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The retrying move the managed-PostgreSQL tests use right after a stop
/// (<see cref="DarlingManagedPostgresTests.MoveDirectoryOnceReleased"/>). On Windows a file that the stopped
/// process still maps holds its folder for a moment, and the rename fails with a sharing violation or access
/// denied. Here a file opened without sharing plays that process, so no PostgreSQL is needed. Windows-only:
/// elsewhere an open file does not block a rename.
/// </summary>
public sealed class MoveDirectoryOnceReleasedTests
{
    /// <summary>
    /// The planted lock reproduces the failure: a plain move of a folder with an open file inside is
    /// refused with the exception the CI run threw.
    /// </summary>
    [Fact]
    public void APlainMove_WhileAFileInsideIsHeldOpen_IsRefusedByWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "An open file blocking its folder's rename is Windows behavior.");
        var root = Directory.CreateTempSubdirectory("darling-move-plain-");
        try
        {
            var (source, destination, file) = Plant(root.FullName);
            using (HoldOpen(file))
            {
                var failure = Record.Exception(() => Directory.Move(source, destination));

                Assert.True(failure is IOException or UnauthorizedAccessException,
                    $"a folder holding an open file should refuse the rename, got: {failure?.ToString() ?? "no exception"}");
                Assert.True(Directory.Exists(source));
                Assert.False(Directory.Exists(destination));
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// Started while the file is still held, the retrying move waits and then completes: the folder is at
    /// the destination with its file intact, and the release came first.
    /// </summary>
    [Fact]
    public async Task TheRetryingMove_StartedWhileAFileIsHeldOpen_CompletesOnceTheHandleIsReleased()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "An open file blocking its folder's rename is Windows behavior.");
        using var released = new ManualResetEventSlim(false);
        var root = Directory.CreateTempSubdirectory("darling-move-retry-");
        FileStream? held = null;
        Task? release = null;
        try
        {
            var (source, destination, file) = Plant(root.FullName);
            held = HoldOpen(file);
            var handle = held;
            release = Task.Run(async () =>
            {
                await Task.Delay(500);

                /* Signalled before the handle goes, so a move that has returned can only have followed the release. */
                released.Set();
                handle.Dispose();
            });

            DarlingManagedPostgresTests.MoveDirectoryOnceReleased(source, destination);

            Assert.True(released.IsSet, "the move returned while the file was still held open.");
            Assert.False(Directory.Exists(source));
            Assert.Equal("17", File.ReadAllText(Path.Combine(destination, "PG_VERSION")));
        }
        finally
        {
            held?.Dispose();
            if (release is not null)
            {
                await release;
            }

            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>
    /// A lock that never lifts is not waited out forever: the retrying move keeps trying until its patience
    /// is spent, and then the last refusal escapes with the folder still where it was.
    /// </summary>
    [Fact]
    public void TheRetryingMove_WithALockThatNeverLifts_ThrowsWhenItsPatienceRunsOut()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "An open file blocking its folder's rename is Windows behavior.");
        var root = Directory.CreateTempSubdirectory("darling-move-giveup-");
        try
        {
            var (source, destination, file) = Plant(root.FullName);
            var patience = TimeSpan.FromMilliseconds(300);
            using (HoldOpen(file))
            {
                var clock = Stopwatch.StartNew();
                var failure = Record.Exception(() => DarlingManagedPostgresTests.MoveDirectoryOnceReleased(source, destination, patience));
                clock.Stop();

                Assert.True(failure is IOException or UnauthorizedAccessException,
                    $"the last refusal should escape, got: {failure?.ToString() ?? "no exception"}");
                Assert.True(clock.Elapsed >= patience,
                    $"it gave up after {clock.Elapsed.TotalMilliseconds:N0} ms, before its {patience.TotalMilliseconds:N0} ms of patience ran out.");
                Assert.True(Directory.Exists(source));
                Assert.False(Directory.Exists(destination));
            }
        }
        finally
        {
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    /// <summary>A data folder holding one file, and the two paths a move would run between.</summary>
    private static (string Source, string Destination, string File) Plant(string root)
    {
        var source = Path.Combine(root, "pg");
        Directory.CreateDirectory(source);
        var file = Path.Combine(source, "PG_VERSION");
        File.WriteAllText(file, "17");
        return (source, Path.Combine(root, "pg.moved"), file);
    }

    /// <summary>Opens the file the way a process that maps it does: nobody else may delete, rename or write it.</summary>
    private static FileStream HoldOpen(string file) =>
        new(file, FileMode.Open, FileAccess.Read, FileShare.None);
}
