/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5366: the service's password key file. Load never generates; Generate never overwrites; Retire renames and never
/// deletes; a directory that was open to other users' writes keeps the key file (and says so) while every other
/// credential file in it is removed. Windows runs the DPAPI and ACL arm; the Unix arm of the directory check is pinned
/// through <see cref="StandInUnixModes"/>, and the 0644 file check is the Unix run's.
/// </summary>
public sealed class DarlingPasswordKeyFileTests
{
    private static readonly Lazy<byte[]> Key3072 = new(() =>
    {
        using var rsa = RSA.Create(DarlingPasswordKeyFile.KeyBits);
        return rsa.ExportPkcs8PrivateKey();
    });

    private static readonly Lazy<byte[]> Key2048 = new(() =>
    {
        using var rsa = RSA.Create(2048);
        return rsa.ExportPkcs8PrivateKey();
    });

    [Fact]
    public void Load_OnAnEmptyDirectory_FindsNothing_AndWritesNothing()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.False(load.Present);
            Assert.False(load.Untrusted);
            Assert.False(load.FoundAfterOpenDirectory);
            Assert.Null(load.Refusal);
            Assert.Null(load.Pkcs8);
            Assert.Equal(Path.Combine(directory, DarlingPasswordKeyFile.FileName), load.Path);
            Assert.Empty(Directory.GetFileSystemEntries(directory));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Load_OnADirectoryThatDoesNotExist_FindsNothing_AndCreatesNothing()
    {
        var directory = NewDirectory();
        try
        {
            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.False(load.Present);
            Assert.Null(load.Refusal);
            Assert.False(Directory.Exists(directory), "a load made the directory");
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Generate_WritesAnOwnerOnlyKey_ThatLoadsBack_AndZeroesTheBytesItWasGiven()
    {
        var directory = NewDirectory();
        try
        {
            var supplied = (byte[])Key3072.Value.Clone();

            var generated = DarlingPasswordKeyFile.Generate(directory, () => supplied, NullLogger.Instance);

            Assert.Null(generated.Refusal);
            Assert.True(generated.Present);
            Assert.Equal(Key3072.Value, generated.Pkcs8);
            Assert.All(supplied, b => Assert.Equal(0, b));

            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            Assert.Equal(path, generated.Path);
            AssertOwnerOnly(path);
            Assert.False(File.Exists(path + ".tmp"), "the temporary file the key is written through was left behind");

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Refusal);
            Assert.True(load.Present);
            Assert.False(load.Untrusted);
            Assert.False(load.FoundAfterOpenDirectory);
            Assert.Equal(Key3072.Value, load.Pkcs8);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Generate_OverAnExistingFile_Fails_AndLeavesItUnchanged()
    {
        var directory = NewDirectory();
        try
        {
            Assert.Null(DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance).Refusal);
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var before = File.ReadAllBytes(path);

            var second = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key2048.Value.Clone(), NullLogger.Instance);
            var third = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance);

            foreach (var refused in new[] { second, third })
            {
                Assert.NotNull(refused.Refusal);
                Assert.Null(refused.Pkcs8);
                Assert.Contains(DarlingPasswordKeyFile.FileName, refused.Refusal, StringComparison.Ordinal);
                Assert.Contains(directory, refused.Refusal, StringComparison.Ordinal);
            }

            Assert.Equal(before, File.ReadAllBytes(path));
            Assert.Equal(Key3072.Value, DarlingPasswordKeyFile.Load(directory, NullLogger.Instance).Pkcs8);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Generate_OverADirectoryAtItsName_Fails_AndLeavesItAlone()
    {
        var directory = NewDirectory();
        try
        {
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            Directory.CreateDirectory(path);

            var result = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance);

            Assert.NotNull(result.Refusal);
            Assert.True(result.Present);
            Assert.True(Directory.Exists(path));
            Assert.Empty(Directory.GetFileSystemEntries(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Generate_RefusesAKeyThatIsNotExactly3072Bits_AndWritesNothing()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);

            var tooSmall = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key2048.Value.Clone(), NullLogger.Instance);
            var notAKey = DarlingPasswordKeyFile.Generate(directory, () => new byte[] { 1, 2, 3 }, NullLogger.Instance);

            Assert.Contains("3072", tooSmall.Refusal, StringComparison.Ordinal);
            Assert.NotNull(notAKey.Refusal);
            Assert.Empty(Directory.GetFileSystemEntries(directory));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ALoadOfAFileOrdinaryUsersCanRead_IsUntrusted_AndLeavesItUnchanged_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The ACL arm is Windows'; the Unix mode arm is the next test's.");

        var directory = NewDirectory();
        try
        {
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            Assert.Null(DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance).Refusal);
            var exposed = new FileInfo(path).GetAccessControl();
            exposed.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null), FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(path).SetAccessControl(exposed);
            var before = File.ReadAllBytes(path);

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            AssertRefusedUntrusted(load, directory, "ordinary local users can read it");
            Assert.Equal(before, File.ReadAllBytes(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ALoadOfA0644File_IsUntrusted_AndLeavesItUnchanged_OnUnix()
    {
        Assert.SkipUnless(!OperatingSystem.IsWindows(), "File modes are Unix's; Windows judges the ACL (the previous test).");

        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            File.WriteAllText(path, Convert.ToBase64String(Key3072.Value));
            if (!OperatingSystem.IsWindows())
            {
                File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.GroupRead | UnixFileMode.OtherRead);
            }

            var before = File.ReadAllBytes(path);

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            AssertRefusedUntrusted(load, directory, "gives other users access");
            Assert.Equal(before, File.ReadAllBytes(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ALoadOfAFileThatIsNotA3072BitKey_IsRefusedNamingTheFile_NotTheKey_AndLeavesItUnchanged()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var small = Convert.ToBase64String(Key2048.Value);
            WriteKeyFile(path, small);
            var before = File.ReadAllBytes(path);

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.True(load.Present);
            Assert.False(load.Untrusted);
            Assert.Null(load.Pkcs8);
            Assert.Contains("3072", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(DarlingPasswordKeyFile.FileName, load.Refusal, StringComparison.Ordinal);
            Assert.Contains(directory, load.Refusal, StringComparison.Ordinal);
            Assert.DoesNotContain(small[..40], load.Refusal, StringComparison.Ordinal);
            Assert.Equal(before, File.ReadAllBytes(path));

            WriteKeyFile(path + ".other", "this is not base64 !!");
            File.Delete(path);
            File.Move(path + ".other", path);
            var garbage = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);
            Assert.True(garbage.Present);
            Assert.NotNull(garbage.Refusal);
            Assert.Null(garbage.Pkcs8);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Retire_RenamesTheFile_KeepingItsBytes_AndNeverOverwritesAnEarlierOne()
    {
        var directory = NewDirectory();
        try
        {
            var path = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            Assert.Null(DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance).Refusal);
            var bytes = File.ReadAllBytes(path);

            var retired = DarlingPasswordKeyFile.Retire(directory, "retired", NullLogger.Instance);

            Assert.Equal(path + ".retired", retired);
            Assert.False(File.Exists(path));
            Assert.Equal(bytes, File.ReadAllBytes(retired));
            Assert.False(DarlingPasswordKeyFile.Load(directory, NullLogger.Instance).Present);

            /* A second key retired the same way takes its own name, and the first stays. */
            Assert.Null(DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance).Refusal);
            var secondBytes = File.ReadAllBytes(path);
            var second = DarlingPasswordKeyFile.Retire(directory, "retired", NullLogger.Instance);

            Assert.NotEqual(retired, second);
            Assert.Equal(bytes, File.ReadAllBytes(retired));
            Assert.Equal(secondBytes, File.ReadAllBytes(second));

            Assert.Equal(string.Empty, DarlingPasswordKeyFile.Retire(directory, "retired", NullLogger.Instance));
            Assert.Throws<ArgumentException>(() => DarlingPasswordKeyFile.Retire(directory, "../x", NullLogger.Instance));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>An open directory keeps the key file and says so; the role-password files, the log-hash key and the
    /// temporary files are still removed; the next load in the same start reports it too.</summary>
    [Fact]
    public void AnOpenDirectory_KeepsTheKeyFile_UnderItsQuarantineName_AndStillRemovesTheOtherCredentialFiles()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var adminPath = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName));
            var hashKeyPath = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            WriteKeyFile(keyPath, Convert.ToBase64String(Key3072.Value));
            StandInUnixModes.WriteOwnerOnly(adminPath, "Example5366Admin");
            StandInUnixModes.WriteOwnerOnly(hashKeyPath, OperatingSystem.IsWindows() ? DarlingSecrets.Protect("AAAA") : "AAAA");
            StandInUnixModes.WriteOwnerOnly(keyPath + ".tmp", "example-temporary");
            StandInUnixModes.WriteOwnerOnly(keyPath + ".tmp-0123abcd", "example-unique-temporary");
            var keyBefore = File.ReadAllBytes(keyPath);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                Assert.True(load.Refusal is null, load.Refusal);
                Assert.True(load.Present);
                Assert.True(load.FoundAfterOpenDirectory);
                Assert.Equal(Key3072.Value, load.Pkcs8);
                Assert.Equal(keyPath + DarlingPasswordKeyFile.QuarantineSuffix, load.Path);
                Assert.Equal("700", modes.Octal(directory));
                Assert.Equal("600", modes.Octal(load.Path));
                Assert.False(File.Exists(keyPath), "the live name was not left empty");
                Assert.Equal(keyBefore, File.ReadAllBytes(load.Path));
                Assert.False(File.Exists(adminPath), "the role password outlived the start that closed its directory");
                Assert.False(File.Exists(hashKeyPath), "the log-hash key outlived the start that closed its directory");
                Assert.False(File.Exists(keyPath + ".tmp"));
                Assert.False(File.Exists(keyPath + ".tmp-0123abcd"), "a temporary file with a name of its own outlived the start");

                Assert.True(DarlingPasswordKeyFile.Load(directory, NullLogger.Instance).FoundAfterOpenDirectory);
            }

            /* The next start finds the directory owner-only, and the key is still the one that was found open: the
               record is on disk, not in the process. */
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var next = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(next.Refusal);
                Assert.True(next.FoundAfterOpenDirectory);
                Assert.Equal(Key3072.Value, next.Pkcs8);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ADirectoryAtTheKeyName_IsQuarantinedToo_AndTheLiveNameIsEmpty()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            Directory.CreateDirectory(keyPath);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                Assert.True(load.Present);
                Assert.True(load.Untrusted);
                Assert.True(load.FoundAfterOpenDirectory);
                Assert.Null(load.Pkcs8);
                Assert.False(Directory.Exists(keyPath), "the live name still holds the directory");
                Assert.True(Directory.Exists(keyPath + DarlingPasswordKeyFile.QuarantineSuffix));
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AnEarlierQuarantine_IsNeverReplaced_AndTheNewOneIsKeptUnderTheNextFreeName()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var quarantine = keyPath + DarlingPasswordKeyFile.QuarantineSuffix;
            WriteKeyFile(quarantine, "example-earlier-quarantine");
            WriteKeyFile(keyPath, Convert.ToBase64String(Key3072.Value));
            var keyBefore = File.ReadAllBytes(keyPath);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                /* The earlier file stays where it was, unread and untouched; the file that was at the live name is kept
                   beside it, and the quarantine name still answers for the earlier one. */
                Assert.False(File.Exists(keyPath));
                Assert.Equal(keyBefore, File.ReadAllBytes(keyPath + ".discarded-1"));
                Assert.NotEqual(keyBefore, File.ReadAllBytes(quarantine));
                Assert.True(load.FoundAfterOpenDirectory);
                Assert.Null(load.Pkcs8);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void FilesAtBothNames_AreRefusedAsUntrusted_AndNeitherIsTouched()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var quarantine = keyPath + DarlingPasswordKeyFile.QuarantineSuffix;
            WriteKeyFile(keyPath, Convert.ToBase64String(Key3072.Value));
            WriteKeyFile(quarantine, Convert.ToBase64String(Key3072.Value));

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.True(load.Present);
            Assert.True(load.Untrusted);
            Assert.Null(load.Pkcs8);
            Assert.StartsWith("Two password key files are in ", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(directory, load.Refusal, StringComparison.Ordinal);
            Assert.Contains("remove the other, then restart", load.Refusal, StringComparison.Ordinal);
            Assert.True(File.Exists(keyPath));
            Assert.True(File.Exists(quarantine));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Accept_PutsTheKeptKeyBackAtTheLiveName_AndTheNextLoadIsNotFlagged()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            WriteKeyFile(keyPath, Convert.ToBase64String(Key3072.Value));
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                Assert.True(DarlingPasswordKeyFile.Load(directory, NullLogger.Instance).FoundAfterOpenDirectory);
            }

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                Assert.Equal(keyPath, DarlingPasswordKeyFile.Accept(directory, NullLogger.Instance));

                Assert.True(File.Exists(keyPath));
                Assert.False(File.Exists(keyPath + DarlingPasswordKeyFile.QuarantineSuffix));
                var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);
                Assert.Null(load.Refusal);
                Assert.False(load.FoundAfterOpenDirectory);
                Assert.Equal(Key3072.Value, load.Pkcs8);
                Assert.Equal(keyPath, load.Path);

                /* Nothing is kept now, so there is nothing to accept. */
                var none = Assert.Throws<InvalidOperationException>(() => DarlingPasswordKeyFile.Accept(directory, NullLogger.Instance));
                Assert.Contains(directory, none.Message, StringComparison.Ordinal);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Accept_NeverOverwrites_WhenTheLiveNameIsTaken()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var quarantine = keyPath + DarlingPasswordKeyFile.QuarantineSuffix;
            WriteKeyFile(keyPath, "example-live");
            WriteKeyFile(quarantine, "example-kept");

            var thrown = Assert.Throws<InvalidOperationException>(() => DarlingPasswordKeyFile.Accept(directory, NullLogger.Instance));

            Assert.Contains(DarlingPasswordKeyFile.FileName, thrown.Message, StringComparison.Ordinal);
            Assert.Contains("remove the other", thrown.Message, StringComparison.Ordinal);
            Assert.Equal("example-live", ReadKeyText(keyPath));
            Assert.Equal("example-kept", ReadKeyText(quarantine));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Retire_RetiresTheKeptKey_WhenThereIsOne_AndOtherwiseTheLiveKey()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            var quarantine = keyPath + DarlingPasswordKeyFile.QuarantineSuffix;
            WriteKeyFile(quarantine, Convert.ToBase64String(Key3072.Value));
            var keptBytes = File.ReadAllBytes(quarantine);

            var retired = DarlingPasswordKeyFile.Retire(directory, "retired", NullLogger.Instance);

            Assert.Equal(keyPath + ".retired", retired);
            Assert.False(File.Exists(quarantine));
            Assert.Equal(keptBytes, File.ReadAllBytes(retired));
            Assert.False(DarlingPasswordKeyFile.Load(directory, NullLogger.Instance).Present);

            /* With a live key and a kept one, the kept one goes and the live one stays. */
            WriteKeyFile(keyPath, "example-live");
            WriteKeyFile(quarantine, "example-kept");

            var second = DarlingPasswordKeyFile.Retire(directory, "retired", NullLogger.Instance);

            Assert.Equal(keyPath + ".retired-1", second);
            Assert.True(File.Exists(keyPath));
            Assert.Equal("example-kept", ReadKeyText(second));
            Assert.Equal(keptBytes, File.ReadAllBytes(retired));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void Generate_RefusesWhileAKeptKeyIsWaiting_AndWritesNothing()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var quarantine = Path.Combine(directory, DarlingPasswordKeyFile.QuarantineFileName);
            WriteKeyFile(quarantine, "example-kept");
            var supplied = (byte[])Key3072.Value.Clone();

            var generated = DarlingPasswordKeyFile.Generate(directory, () => supplied, NullLogger.Instance);

            Assert.NotNull(generated.Refusal);
            Assert.Null(generated.Pkcs8);
            Assert.False(File.Exists(Path.Combine(directory, DarlingPasswordKeyFile.FileName)));
            Assert.Equal("example-kept", ReadKeyText(quarantine));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AFileBiggerThanTheKeyLimit_IsRefusedBeforeItIsRead()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            StandInUnixModes.WriteOwnerOnly(keyPath, new string('A', 9000));

            var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

            Assert.True(load.Present);
            Assert.False(load.Untrusted);
            Assert.Null(load.Pkcs8);
            Assert.Contains("larger than 8192 bytes", load.Refusal, StringComparison.Ordinal);
            Assert.Contains(DarlingPasswordKeyFile.FileName, load.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AKeyFileAnotherUserOwns_OrWithMoreThanOneName_IsRefusedAsUntrusted_OnUnix()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            WriteKeyFile(keyPath, Convert.ToBase64String(Key3072.Value));
            var modes = new StandInUnixModes { EffectiveUserId = 1000 };
            modes.Report(directory, "700");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                modes.ReportOwner(keyPath, userId: 1000);
                var own = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);
                Assert.Null(own.Refusal);
                Assert.Equal(Key3072.Value, own.Pkcs8);

                modes.ReportOwner(keyPath, userId: 1001);
                var foreign = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);
                AssertRefusedUntrusted(foreign, directory, "owned by another user");

                modes.ReportOwner(keyPath, userId: 1000, links: 2);
                var linked = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);
                AssertRefusedUntrusted(linked, directory, "2 names, not one");
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void TheStatBuffer_IsReadAtEachLayoutsOffsets()
    {
        var linuxX64 = new byte[512];
        BitConverter.GetBytes(3UL).CopyTo(linuxX64, 16);
        BitConverter.GetBytes(1234u).CopyTo(linuxX64, 28);
        Assert.Equal(new UnixFileOwner(1234, 3), FileIdentity.DecodeUnixOwner(linuxX64, FileIdentity.UnixStatLayout.LinuxX64));

        var linuxArm64 = new byte[512];
        BitConverter.GetBytes(2u).CopyTo(linuxArm64, 20);
        BitConverter.GetBytes(4321u).CopyTo(linuxArm64, 24);
        Assert.Equal(new UnixFileOwner(4321, 2), FileIdentity.DecodeUnixOwner(linuxArm64, FileIdentity.UnixStatLayout.LinuxArm64));

        var mac = new byte[512];
        BitConverter.GetBytes((ushort)1).CopyTo(mac, 6);
        BitConverter.GetBytes(501u).CopyTo(mac, 16);
        Assert.Equal(new UnixFileOwner(501, 1), FileIdentity.DecodeUnixOwner(mac, FileIdentity.UnixStatLayout.MacOs));
    }

    [Fact]
    public void ALoad_WithADirectoryNameThatCannotBeUsed_ReturnsARefusal_AndNeverThrows()
    {
        var load = DarlingPasswordKeyFile.Load("example-keys-\0-directory", NullLogger.Instance);

        Assert.Null(load.Pkcs8);
        Assert.False(load.FoundAfterOpenDirectory);
        Assert.NotNull(load.Path);
    }

    [Fact]
    public void Generate_IntoAnExistingDirectoryOthersCanWrite_IsRefused_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the directory ACL is Windows's");
        var directory = NewDirectory();
        try
        {
            var info = Directory.CreateDirectory(directory);
            var security = info.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null), FileSystemRights.Modify,
                InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
            info.SetAccessControl(security);

            var generated = DarlingPasswordKeyFile.Generate(directory, () => (byte[])Key3072.Value.Clone(), NullLogger.Instance);

            Assert.True(generated.Untrusted);
            Assert.Null(generated.Pkcs8);
            Assert.Contains("can write to it", generated.Refusal, StringComparison.Ordinal);
            Assert.Contains(directory, generated.Refusal, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(directory, DarlingPasswordKeyFile.FileName)));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ADirectoryThatWasNeverOpen_DoesNotFlagTheKey()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingPasswordKeyFile.FileName);
            WriteKeyFile(keyPath, Convert.ToBase64String(Key3072.Value));
            var modes = new StandInUnixModes();
            modes.Report(directory, "700");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingPasswordKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Refusal);
                Assert.False(load.FoundAfterOpenDirectory);
                Assert.Equal(Key3072.Value, load.Pkcs8);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void ThePasswordKeyLoad_DoesNotTakeTheLogHashKeysDiscardRecord()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var hashKeyPath = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            StandInUnixModes.WriteOwnerOnly(hashKeyPath, OperatingSystem.IsWindows() ? DarlingSecrets.Protect("AAAA") : "AAAA");
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                /* The password key's load is the one that finds the directory open and discards the log-hash key... */
                Assert.False(DarlingPasswordKeyFile.Load(directory, NullLogger.Instance).Present);

                /* ...and the log-hash key's load, which generates the replacement, still hears about it. */
                var hash = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
                Assert.True(hash.Generated);
                Assert.NotNull(hash.Replaced);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void TheDiscardList_RemovesThePasswordKeysTemporaryFiles_ButNotTheKeyFiles()
    {
        var names = DarlingManagedRoles.CredentialDirectoryFileNames;

        Assert.Contains(DarlingPasswordKeyFile.UnixFileName + ".tmp", names);
        Assert.Contains(DarlingPasswordKeyFile.WindowsFileName + ".tmp", names);
        Assert.DoesNotContain(DarlingPasswordKeyFile.UnixFileName, names);
        Assert.DoesNotContain(DarlingPasswordKeyFile.WindowsFileName, names);

        /* The log-hash key and the role passwords are still discarded. */
        Assert.Contains(DarlingLogHashKeyFile.UnixFileName, names);
        Assert.Contains(DarlingLogHashKeyFile.WindowsFileName, names);
        Assert.Contains(DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName), names);
    }

    [Fact]
    public void ThePasswordKeyAndTheLogHashKey_ShareADirectory()
    {
        var configPath = Path.Combine(Path.GetTempPath(), "darling-5366-config", "darling.json");
        var config = new DarlingConfig();
        config.Postgres.Managed = false;

        Assert.Equal(
            Path.Combine(Path.GetDirectoryName(configPath)!, DarlingLogHashKeyFile.BringYourOwnDirectoryName),
            DarlingLogHashKeyFile.DirectoryFor(config, configPath));
        Assert.Equal("password-key", DarlingPasswordKeyFile.UnixFileName);
        Assert.Equal("password-key.dpapi", DarlingPasswordKeyFile.WindowsFileName);
    }

    private static void AssertRefusedUntrusted(PasswordKeyFileLoad load, string directory, string reason)
    {
        Assert.True(load.Present);
        Assert.True(load.Untrusted);
        Assert.Null(load.Pkcs8);
        Assert.Contains(reason, load.Refusal, StringComparison.Ordinal);
        Assert.Contains(DarlingPasswordKeyFile.FileName, load.Refusal, StringComparison.Ordinal);
        Assert.Contains(directory, load.Refusal, StringComparison.Ordinal);
    }

    /// <summary>A key file written the way a person or another process would leave one (not through the service).</summary>
    private static void WriteKeyFile(string path, string base64) =>
        StandInUnixModes.WriteOwnerOnly(path, OperatingSystem.IsWindows() ? DarlingSecrets.Protect(base64) : base64);

    /// <summary>What a key file written by <see cref="WriteKeyFile"/> holds, DPAPI layer removed on Windows.</summary>
    private static string ReadKeyText(string path) =>
        OperatingSystem.IsWindows() ? DarlingSecrets.Unprotect(File.ReadAllText(path).Trim()) : File.ReadAllText(path).Trim();

    private static void AssertOwnerOnly(string path)
    {
        if (OperatingSystem.IsWindows())
        {
            Assert.True(DarlingFileSecurity.IsTrustedOwner(path));
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
            Assert.True(DarlingFileSecurity.GrantsHardenedAccount(path));
        }
        else
        {
            Assert.Equal(UnixFileMode.UserRead | UnixFileMode.UserWrite, File.GetUnixFileMode(path));
        }
    }

    private static string NewDirectory() =>
        Path.Combine(Path.GetTempPath(), "darling-5366-" + Guid.NewGuid().ToString("N"), "keys");

    private static void Remove(string directory)
    {
        var root = Path.GetDirectoryName(directory)!;
        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
