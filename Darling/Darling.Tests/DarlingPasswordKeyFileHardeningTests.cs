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
using System.Security.Cryptography;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5366: the password key file's write, discard and load paths. A new file is checked for its owner before it is moved
/// into place; a check that cannot be made refuses a write; leftover temporary files are removed; a key that could not be
/// moved aside leaves a marker; the chmod of a kept key is for the service's own file only.
/// (Zeroing the live key on the two-names refusal is not observable from outside, so it has no test.)
/// </summary>
public sealed class DarlingPasswordKeyFileHardeningTests
{
    private static readonly Lazy<byte[]> Key3072 = new(() =>
    {
        using var rsa = RSA.Create(DarlingPasswordKeyFile.KeyBits);
        return rsa.ExportPkcs8PrivateKey();
    });

    /// <summary>The stand-in modes with an owner report of its own and a record of every mode set.</summary>
    private sealed class ReportingModes(StandInUnixModes inner) : IUnixDirectoryModes
    {
        public List<string> SetPaths { get; } = [];

        public Func<string, UnixFileOwner?> Owner { get; set; } = _ => null;

        public uint EffectiveUserId => 1000;

        public void CreateOwnerOnly(string directory) => inner.CreateOwnerOnly(directory);

        public UnixFileMode Get(string directory) => inner.Get(directory);

        public void Set(string directory, UnixFileMode mode)
        {
            SetPaths.Add(Path.GetFullPath(directory));
            inner.Set(directory, mode);
        }

        public UnixFileOwner? OwnerOf(string path) => Owner(path);
    }

    [Fact]
    public void ANewFile_OwnedByAnotherUser_IsNotKept_AndNothingIsLeftBehind()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var inner = new StandInUnixModes();
            inner.Report(directory, "700");
            var modes = new ReportingModes(inner)
            {
                Owner = path => path.Contains(DarlingServiceKeyFile.UniqueTemporaryInfix, StringComparison.Ordinal)
                    ? new UnixFileOwner(2000, 1)
                    : null,
            };

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var generated = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance);

                Assert.NotNull(generated.Refusal);
                Assert.Contains("another user", generated.Refusal, StringComparison.Ordinal);
                Assert.Null(generated.Pkcs8);
                Assert.Empty(Directory.GetFileSystemEntries(directory));
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AKeptFileCheckThatCannotBeMade_RefusesAWrite_BeforeAnyKeyIsSupplied()
    {
        var directory = NewDirectory() + "\0example";
        var supplied = false;

        var generated = DarlingPasswordKeyFile.Generate(directory, () =>
        {
            supplied = true;
            return (byte[])Key3072.Value.Clone();
        }, NullLogger.Instance);

        Assert.NotNull(generated.Refusal);
        Assert.Contains("could not be checked", generated.Refusal, StringComparison.Ordinal);
        Assert.Null(generated.Pkcs8);
        Assert.False(supplied, "a key was asked for although a kept file might be waiting");
    }

    [Fact]
    public void AWrite_RemovesLeftoverTemporaryFiles_AndLeavesADirectoryWithSuchAName()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var staleFile = Path.Combine(directory, DarlingPasswordKeyFile.FileName + ".tmp-abc");
            var staleDirectory = Path.Combine(directory, DarlingPasswordKeyFile.FileName + ".tmp-dir");
            File.WriteAllText(staleFile, "example-left-over");
            Directory.CreateDirectory(staleDirectory);
            File.WriteAllText(Path.Combine(staleDirectory, "inside.txt"), "example");

            var generated = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance);

            Assert.True(generated.Refusal is null, generated.Refusal);
            Assert.False(File.Exists(staleFile), "a leftover temporary file outlived the write");
            Assert.True(Directory.Exists(staleDirectory), "a directory with a temporary file's name was removed");
            Assert.True(File.Exists(Path.Combine(staleDirectory, "inside.txt")));
            Assert.True(File.Exists(Path.Combine(directory, DarlingPasswordKeyFile.FileName)));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>A key that cannot be moved aside (an open handle without delete sharing stops the rename on Windows; Unix
    /// has no such lock, so this arm is Windows's) leaves an empty file at the quarantine name, so every later load
    /// refuses with both names.</summary>
    [Fact]
    public void AKeyThatCouldNotBeMovedAside_LeavesAnEmptyMarker_AndEveryLaterLoadRefuses()
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            StandInUnixModes.WriteOwnerOnly(keyPath, DarlingSecrets.Protect(Convert.ToBase64String(Key3072.Value)));
            var quarantinePath = keyPath + DarlingPasswordKeyFile.QuarantineSuffix;
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (var held = new FileStream(keyPath, FileMode.Open, FileAccess.Read, FileShare.Read))
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var first = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                Assert.NotNull(first.Refusal);
                Assert.Null(first.Pkcs8);
                Assert.True(File.Exists(quarantinePath), "no marker was left at the quarantine name: " + first.Refusal + " / " + string.Join(",", Directory.GetFileSystemEntries(directory).Select(Path.GetFileName)));
                Assert.Equal(0, new FileInfo(quarantinePath).Length);
                Assert.True(held.CanRead);
            }

            /* The next start finds the directory owner-only and the handle gone: the key is at its live name, and the
               marker still says it was found while the directory was open. */
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var next = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(next.Pkcs8);
                Assert.True(next.Untrusted);
                Assert.Contains("Two password key files", next.Refusal, StringComparison.Ordinal);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Theory]
    [InlineData(1000u, 1ul, true)]
    [InlineData(1000u, 2ul, false)]
    [InlineData(2000u, 1ul, false)]
    public void AKeptKey_IsSetToOwnerOnly_OnlyWhenItIsTheServicesOwnWithOneName(uint owner, ulong links, bool expectSet)
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            StandInUnixModes.WriteOwnerOnly(keyPath, OperatingSystem.IsWindows()
                ? DarlingSecrets.Protect(Convert.ToBase64String(Key3072.Value))
                : Convert.ToBase64String(Key3072.Value));
            var kept = Path.GetFullPath(keyPath + DarlingPasswordKeyFile.QuarantineSuffix);
            var inner = new StandInUnixModes();
            inner.Report(directory, "777");
            var modes = new ReportingModes(inner)
            {
                Owner = path => string.Equals(Path.GetFullPath(path), kept, StringComparison.OrdinalIgnoreCase)
                    ? new UnixFileOwner(owner, links)
                    : null,
            };

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                _ = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);
            }

            Assert.Equal(expectSet, modes.SetPaths.Contains(kept, StringComparer.OrdinalIgnoreCase));
        }
        finally
        {
            Remove(directory);
        }
    }

    private static string NewDirectory() =>
        Path.Combine(Path.GetTempPath(), "darling-5366-h-" + Guid.NewGuid().ToString("N"), "keys");

    private static void Remove(string directory)
    {
        var root = Path.GetDirectoryName(directory)!;
        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
