/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>A fixed log-hash key for tests that classify log text (#4004). Any 32 bytes do; these are 1..32.</summary>
internal static class TestLogHashKeys
{
    internal static readonly byte[] FixedMaterial = Enumerable.Range(1, PgLogHashKey.KeyLength).Select(i => (byte)i).ToArray();

    public static PgLogHashKey Fixed { get; } = new(FixedMaterial);
}

/// <summary>
/// Unix directory modes on a box that has none (#4004's review): what a test reports a credentials directory's mode as,
/// and what the service's chmod leaves it at, as the kernel would. Stood in through
/// <see cref="ComposeCredentialDirectoryGuard.BeginForTest"/>; one instance outlives a start, the way a directory does.
/// </summary>
internal sealed class StandInUnixModes : IUnixDirectoryModes
{
    private readonly Dictionary<string, UnixFileMode> _modes = new(StringComparer.OrdinalIgnoreCase);

    public void Report(string directory, string octal) => _modes[Key(directory)] = (UnixFileMode)Convert.ToInt32(octal, 8);

    public string Octal(string directory) => Convert.ToString((int)_modes[Key(directory)], 8);

    public void CreateOwnerOnly(string directory)
    {
        Directory.CreateDirectory(directory);
        _modes[Key(directory)] = DarlingManagedRoles.OwnerOnlyDirectory;
    }

    public UnixFileMode Get(string directory) => _modes[Key(directory)];

    public void Set(string directory, UnixFileMode mode) => _modes[Key(directory)] = mode;

    /// <summary>A file someone else wrote that the file check trusts, which is the point: owner-only on Unix (a 0600
    /// file whose owner managed code cannot see), and the temp directory's inherited ACL on Windows.</summary>
    public static void WriteOwnerOnly(string path, string text)
    {
        if (OperatingSystem.IsWindows())
        {
            File.WriteAllText(path, text);
            return;
        }

        using var stream = new FileStream(path, new FileStreamOptions
        {
            Mode = FileMode.CreateNew,
            Access = FileAccess.Write,
            UnixCreateMode = DarlingManagedRoles.OwnerOnlyFile,
        });
        using var writer = new StreamWriter(stream);
        writer.Write(text);
    }

    private static string Key(string directory) => Path.TrimEndingDirectorySeparator(Path.GetFullPath(directory));
}

/// <summary>
/// #4004: <c>raw_line_hash</c> and <c>statement_fingerprint</c> are HMAC-SHA-256 under a per-store key the service holds
/// outside the store, instead of unkeyed SHA-256 a store reader could test guesses against. Pins the hash itself (keyed,
/// stable for one key, different across stores), the pipeline's use of it (stamped by the classifier, refused without a
/// key), and the key file (generated once, owner-only, never rotated: missing generates, unreadable or untrusted refuses).
/// </summary>
public sealed class PgLogHashKeyTests
{
    private const string FailedUpdate =
        "2026-09-18 03:07:11.123 UTC [4242] ERROR:  duplicate key value violates unique constraint \"cards_pin_key\"\n"
        + "2026-09-18 03:07:11.123 UTC [4242] STATEMENT:  UPDATE cards SET pin = '4821' WHERE id = 7\n";

    private static string UnkeyedSha256(string text) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)), 0, 16);

    [Fact]
    public void TheKeyedHashes_DifferFromTheUnkeyedOnes_AndAreStableForOneKey()
    {
        const string raw = "ERROR:  value too long for type character varying(4)";
        const string statement = "UPDATE cards SET pin = ? WHERE id = ?";

        var key = TestLogHashKeys.Fixed;

        /* The old values were SHA-256 over exactly these inputs; the new ones must not be. */
        Assert.NotEqual(UnkeyedSha256(raw), key.RawLineHash(raw));
        Assert.NotEqual(UnkeyedSha256(statement), key.Fingerprint(statement));

        /* Same key material, a second instance: what a restart that reads the same key file builds. */
        var afterRestart = new PgLogHashKey(TestLogHashKeys.FixedMaterial);
        Assert.Equal(key.RawLineHash(raw), afterRestart.RawLineHash(raw));
        Assert.Equal(key.Fingerprint(statement), afterRestart.Fingerprint(statement));

        /* The column width is unchanged: 16 bytes, 32 hex characters. */
        Assert.Equal(32, key.RawLineHash(raw).Length);
        Assert.Equal(32, key.Fingerprint(statement)!.Length);

        /* The two identities use separate subkeys, so one column cannot be matched against the other. */
        Assert.NotEqual(key.RawLineHash(statement), key.Fingerprint(statement));

        /* The fingerprint still groups a statement shape re-indented by a client, and null stays null. */
        Assert.Equal(key.Fingerprint("SELECT * FROM t WHERE id = ?"), key.Fingerprint("  SELECT  *  FROM t\n\tWHERE id = ?  "));
        Assert.Null(key.Fingerprint(null));
        Assert.Null(key.Fingerprint("   \n "));
    }

    [Fact]
    public void TwoStores_HashTheSameTextDifferently()
    {
        var one = new PgLogHashKey(RandomNumberGenerator.GetBytes(PgLogHashKey.KeyLength));
        var two = new PgLogHashKey(RandomNumberGenerator.GetBytes(PgLogHashKey.KeyLength));

        Assert.NotEqual(one.RawLineHash(FailedUpdate), two.RawLineHash(FailedUpdate));
        Assert.NotEqual(one.Fingerprint("UPDATE cards SET pin = ? WHERE id = ?"), two.Fingerprint("UPDATE cards SET pin = ? WHERE id = ?"));

        var fromOne = new PgLogEventClassifier(one).Classify(FailedUpdate).Single();
        var fromTwo = new PgLogEventClassifier(two).Classify(FailedUpdate).Single();
        Assert.NotEqual(fromOne.RawLineHash, fromTwo.RawLineHash);
        Assert.NotEqual(fromOne.StatementFingerprint, fromTwo.StatementFingerprint);
    }

    /// <summary>#3996's review recovered a four-digit PIN from a stored raw_line_hash in 6 ms by hashing each
    /// candidate line. The same enumeration against the keyed value finds nothing without the key.</summary>
    [Fact]
    public void ThePinGuessingAttack_FindsNothingWithoutTheKey()
    {
        var stored = new PgLogEventClassifier(TestLogHashKeys.Fixed).Classify(FailedUpdate).Single().RawLineHash;
        var attackersKey = new PgLogHashKey(RandomNumberGenerator.GetBytes(PgLogHashKey.KeyLength));

        string? unkeyedHit = null;
        string? wrongKeyHit = null;
        string? rightKeyHit = null;
        for (var pin = 0; pin < 10_000; pin++)
        {
            var guess = FailedUpdate.Replace("'4821'", "'" + pin.ToString("D4", CultureInfo.InvariantCulture) + "'", StringComparison.Ordinal);
            var entry = PgLogEntryAssembler.Assemble(guess).Single();
            if (UnkeyedSha256(entry.RawText) == stored) unkeyedHit = guess;
            if (attackersKey.RawLineHash(entry.RawText) == stored) wrongKeyHit = guess;
            if (TestLogHashKeys.Fixed.RawLineHash(entry.RawText) == stored) rightKeyHit = guess;
        }

        Assert.Null(unkeyedHit);
        Assert.Null(wrongKeyHit);
        /* Control: the enumeration is sound, so the two misses above are the key's doing. */
        Assert.Contains("'4821'", rightKeyHit, StringComparison.Ordinal);
    }

    [Fact]
    public void TheClassifierStampsEveryEvent_AndTheWriterRefusesAnUnstampedOne()
    {
        var logEvent = new PgLogEventClassifier(TestLogHashKeys.Fixed).Classify(FailedUpdate).Single();
        var entry = PgLogEntryAssembler.Assemble(FailedUpdate).Single();

        Assert.Equal(TestLogHashKeys.Fixed.RawLineHash(entry.RawText), logEvent.RawLineHash);
        Assert.Equal(
            TestLogHashKeys.Fixed.Fingerprint(PgLogTextRedactor.RedactStatement(entry.Statement)),
            logEvent.StatementFingerprint);

        /* A parser's own output carries no identity, and the one writer both transports use will not store it. */
        var unstamped = PgLogEvent.From(entry, PgLogFamilies.Error);
        Assert.Equal(string.Empty, unstamped.RawLineHash);
        Assert.Null(unstamped.StatementFingerprint);
        var writer = new PgCollectorRowWriter();
        var refused = Assert.Throws<InvalidOperationException>(() =>
            PgLogEventsCollector.Instance.WritePayload(unstamped, writer, Context(key: null)));
        Assert.Contains("#4004", refused.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void WithoutAKey_TheCollectorRefusesBeforeItQueriesTheTarget()
    {
        var refused = Assert.Throws<InvalidOperationException>(() => PgLogEventsCollector.Instance.BuildQuery(Context(key: null)));
        Assert.Equal(PgLogHashKey.UnavailableMessage, refused.Message);

        Assert.NotNull(PgLogEventsCollector.Instance.BuildQuery(Context(TestLogHashKeys.Fixed)).Text);
    }

    [Fact]
    public void TheKeyIsExactlyThirtyTwoBytes_AndNothingPrintsIt()
    {
        Assert.Throws<ArgumentException>(() => new PgLogHashKey(new byte[31]));
        Assert.Throws<ArgumentException>(() => new PgLogHashKey(new byte[33]));

        Assert.Equal(nameof(PgLogHashKey), TestLogHashKeys.Fixed.ToString());
        var load = new DarlingLogHashKeyLoad(TestLogHashKeys.Fixed, "p", Generated: false, Refusal: null);
        Assert.DoesNotContain(Convert.ToBase64String(TestLogHashKeys.FixedMaterial), load.ToString(), StringComparison.Ordinal);
        Assert.DoesNotContain(Convert.ToHexString(TestLogHashKeys.FixedMaterial), load.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AMissingKey_IsGeneratedOnce_OwnerOnly_AndTheSameKeyComesBackAfterARestart()
    {
        var directory = NewDirectory();
        try
        {
            var first = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            Assert.True(first.Generated);
            Assert.NotNull(first.Key);
            Assert.Null(first.Refusal);
            Assert.Null(first.Replaced);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            Assert.Equal(path, first.Path);
            var bytesWritten = File.ReadAllBytes(path);

            AssertOwnerOnly(path);

            var restarted = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            Assert.False(restarted.Generated);
            Assert.Equal(first.Key!.RawLineHash(FailedUpdate), restarted.Key!.RawLineHash(FailedUpdate));
            Assert.Equal(first.Key.Fingerprint("SELECT ?"), restarted.Key.Fingerprint("SELECT ?"));
            Assert.Equal(bytesWritten, File.ReadAllBytes(path));

            /* The file never holds the key in the clear on Windows: it is a DPAPI blob. */
            if (OperatingSystem.IsWindows())
            {
                var blob = Convert.FromBase64String(File.ReadAllText(path).Trim());
                Assert.NotEqual(PgLogHashKey.KeyLength, blob.Length);
                Assert.Equal(
                    PgLogHashKey.KeyLength,
                    Convert.FromBase64String(DarlingSecrets.Unprotect(File.ReadAllText(path).Trim())).Length);
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AnUnreadableKey_IsRefused_AndNeverReplaced()
    {
        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;

            /* A key file whose content is not a key this machine can read: a DPAPI blob from another machine, a
               truncated write, an operator's edit. Written into the existing file, so its ACL stays owner-only. */
            File.WriteAllText(path, "not-a-key");
            var before = File.ReadAllBytes(path);

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.False(load.Generated);
            Assert.Contains(path, load.Refusal, StringComparison.Ordinal);
            /* #4004's review: "does not replace THIS key", since the service now does replace a key whose directory was
               open to other users until the start (below); a refused key is still never replaced. */
            Assert.Contains("does not replace this key on its own", load.Refusal, StringComparison.Ordinal);
            Assert.Contains("delete it", load.Refusal, StringComparison.Ordinal);
            Assert.Equal(before, File.ReadAllBytes(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4028: on Windows the key is held to an allowlist. The credential check it shares refuses only a key Users,
    /// Authenticated Users or Everyone can read, so INTERACTIVE, Domain Users or one named user holding ReadData
    /// passed it, and whoever can read the key can test guesses at every literal the keyed hashes hide.
    /// </summary>
    [Fact]
    public void AKeyAnyoneBeyondTheServiceCanRead_IsRefused_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the allowlist is the Windows DACL check");
        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;
            Assert.Null(DarlingFileSecurity.AccessBeyondTrusted(path));

            var file = new FileInfo(path);
            var security = file.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.InteractiveSid, null), FileSystemRights.Read, AccessControlType.Allow));
            file.SetAccessControl(security);
            var before = File.ReadAllBytes(path);

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.False(load.Generated);
            Assert.Contains("INTERACTIVE", load.Refusal, StringComparison.Ordinal);
            Assert.Equal(before, File.ReadAllBytes(path));
            /* The shared check alone would have loaded it: INTERACTIVE is not one of its three groups. */
            Assert.False(DarlingFileSecurity.IsReadableByOrdinaryUsers(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4044 review: any right refuses the key, not only a read. WriteData lets an account put a key of its own in
    /// place, which any local user can DPAPI-protect (LocalMachine, entropy in the source); ChangePermissions or
    /// TakeOwnership lets it grant itself the read; Delete lets it swap the file. The service writes none of these.
    /// </summary>
    [Theory]
    [InlineData(FileSystemRights.WriteData)]
    [InlineData(FileSystemRights.AppendData)]
    [InlineData(FileSystemRights.ChangePermissions)]
    [InlineData(FileSystemRights.TakeOwnership)]
    [InlineData(FileSystemRights.Delete)]
    public void AKeyAnyoneBeyondTheServiceHoldsAnyRightTo_IsRefused_OnWindows(FileSystemRights right)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the allowlist is the Windows DACL check");
        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;
            var file = new FileInfo(path);
            var security = file.GetAccessControl();
            security.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.InteractiveSid, null), right, AccessControlType.Allow));
            file.SetAccessControl(security);
            var before = File.ReadAllBytes(path);

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.False(load.Generated);
            Assert.Contains("INTERACTIVE", load.Refusal, StringComparison.Ordinal);
            Assert.Equal(before, File.ReadAllBytes(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4044 review: the key is judged and read through one handle that shares read alone, so nobody who can write the
    /// directory can rename, delete, replace or rewrite the file between the check and the read. Checked by path and
    /// read by path, the two could each find a different file.
    /// </summary>
    [Fact]
    public void AHeldKey_CannotBeRenamedDeletedOrWritten_UntilItIsReleased_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the held handle is the Windows read");
        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;
            var moved = path + ".moved";

            using (var held = DarlingFileSecurity.OpenForServiceOnlyRead(path, out var refusal))
            {
                Assert.Null(refusal);
                Assert.NotNull(held);
                Assert.ThrowsAny<IOException>(() => File.Move(path, moved));
                Assert.ThrowsAny<IOException>(() => File.Delete(path));
                Assert.ThrowsAny<IOException>(() => File.OpenWrite(path).Dispose());
            }

            /* Released, the same calls go through: the hold refused them, not the file's ACL. */
            File.Move(path, moved);
            Assert.True(File.Exists(moved));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>#4044 review: a key with a second name is refused, since whoever made that name reaches the same
    /// bytes through a directory the key's own check never sees.</summary>
    [Fact]
    public void AHardLinkedKey_IsRefused_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "hard links through CreateHardLinkW are Windows'");
        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;
            Assert.True(CreateHardLinkW(path + ".second-name", path, IntPtr.Zero), "the hard link was not created");

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.False(load.Generated);
            Assert.Contains("hard link", load.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>#4044 review: at most 1 KB is read from the held key; a key this service writes is about 350 bytes.</summary>
    [Fact]
    public void AnOversizedKey_IsRefused_OnWindows()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "the bounded read is the Windows read");
        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;
            Assert.InRange(new FileInfo(path).Length, 1, 1024);
            File.WriteAllText(path, new string('A', 1025));

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.Contains("larger than 1024 bytes", load.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void AKeyOthersCanRead_IsRefused_AndNeverReplaced()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The ACL arm is Windows'; the Unix mode arm is #3983's shared check.");

        var directory = NewDirectory();
        try
        {
            var path = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance).Path;
            var exposed = new FileInfo(path).GetAccessControl();
            exposed.AddAccessRule(new FileSystemAccessRule(
                new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null), FileSystemRights.Read, AccessControlType.Allow));
            new FileInfo(path).SetAccessControl(exposed);
            var before = File.ReadAllBytes(path);

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.Contains("ordinary local users can read it", load.Refusal, StringComparison.Ordinal);
            Assert.Equal(before, File.ReadAllBytes(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void SomethingElseAtThePath_IsRefused_NotGeneratedOver()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            Directory.CreateDirectory(path);

            var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(load.Key);
            Assert.Contains("it is a directory", load.Refusal, StringComparison.Ordinal);
            Assert.True(Directory.Exists(path));
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4004's review (F1): on compose, role provisioning looks at the credentials directory first and sets it 0700, so
    /// a key load that judged the directory by what IT found saw owner-only and loaded a key planted while the directory
    /// was 0777. Round 2: whichever look finds the directory open removes the key there and then, with every role
    /// password, so no later look can find it. Driven through the Unix-mode seam, since the suite runs on Windows:
    /// provisioning's own call on a directory reported as 0777 that holds a planted key, then the load. The planted key
    /// is gone before the load runs and a new one is generated, and the NEXT start, which finds the directory 0700
    /// because this one set it so, keeps the new key rather than rotating again.
    /// </summary>
    [Fact]
    public void AKeyPlantedWhileItsDirectoryWasOpen_IsDiscarded_ThoughProvisioningClosedTheDirectoryFirst()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            var planted = Convert.ToBase64String(TestLogHashKeys.FixedMaterial);
            StandInUnixModes.WriteOwnerOnly(path, OperatingSystem.IsWindows() ? DarlingSecrets.Protect(planted) : planted);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            DarlingLogHashKeyLoad load;
            var log = new CapturingTestLogger();
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                /* Provisioning's call, first, as the worker makes it, spelled with a trailing separator. The directory
                   is trusted once the call returns, because nothing anyone else put in it is left. */
                var provisioning = DarlingManagedRoles.PrepareComposeCredentialDirectory(
                    directory + Path.DirectorySeparatorChar, create: true, log);
                Assert.Null(provisioning.Distrust);
                Assert.True(provisioning.MayWrite);
                Assert.Equal("700", modes.Octal(directory));
                Assert.False(File.Exists(path), "the planted key outlived the look that found its directory open");
                Assert.Contains("was open to other users until now (its mode was 0777", log.Joined, StringComparison.Ordinal);
                Assert.Contains($"{DarlingLogHashKeyFile.FileName} discarded (#4004)", log.Joined, StringComparison.Ordinal);

                load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            }

            Assert.Null(load.Refusal);
            Assert.True(load.Generated, "the planted key was loaded: the key load judged its directory by the mode provisioning had just set");
            Assert.NotEqual(TestLogHashKeys.Fixed.RawLineHash(FailedUpdate), load.Key!.RawLineHash(FailedUpdate));
            /* Provisioning's look removed the key, and the load, a separate call, still knows it replaced one. */
            Assert.StartsWith("its mode was 0777", load.Replaced, StringComparison.Ordinal);
            AssertOwnerOnly(path);

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var next = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.False(next.Generated);
                Assert.Null(next.Replaced);
                Assert.Equal(load.Key.RawLineHash(FailedUpdate), next.Key!.RawLineHash(FailedUpdate));
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4004's review: in a directory that was open until now, a directory with something in it at the key's path
    /// cannot be discarded (discarding never deletes a tree), so that start reads and writes nothing there, and names
    /// the path to remove. Round 3: the directory stays owner-only rather than going back to the mode it was found with,
    /// which let anyone plant files again until an operator closed it. The next start finds it owner-only, so the
    /// directory is trusted, and the key file's own check refuses the directory at its path, which no reader takes for
    /// a key. A directory still open to others after the chmod (a filesystem that ignores modes) is refused without
    /// anything being written or removed.
    /// </summary>
    [Fact]
    public void InADirectoryThatWasOpen_ADirectoryAtThePathOrAModeThatWillNotClose_IsRefused()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            Directory.CreateDirectory(Path.Combine(path, "someones"));
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Key);
                Assert.Contains(
                    $"its mode was 0777, which let other users put files in it until the service set it to owner-only this start, and this entry at a credential's name could not be removed, so nothing in it is used this start: {path} (",
                    load.Refusal, StringComparison.Ordinal);
                Assert.Contains("so remove it and restart", load.Refusal, StringComparison.Ordinal);
                Assert.DoesNotContain("restrict it to the service account", load.Refusal, StringComparison.Ordinal);
                Assert.True(Directory.Exists(path));
                Assert.Equal("700", modes.Octal(directory));
            }

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Key);
                Assert.Contains($"the store's log-hash key {path} cannot be used because it is a directory", load.Refusal, StringComparison.Ordinal);
                Assert.Equal("700", modes.Octal(directory));
            }

            Directory.Delete(path, recursive: true);
            StandInUnixModes.WriteOwnerOnly(path, "left-alone");
            var ignoresModes = new IgnoresModes(modes);
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryGuard.BeginForTest(ignoresModes))
            {
                var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Key);
                Assert.Contains("its mode is still 0777 after the service set it to owner-only", load.Refusal, StringComparison.Ordinal);
                Assert.Equal("left-alone", File.ReadAllText(path));
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>A filesystem that ignores Unix modes: the chmod succeeds and changes nothing.</summary>
    private sealed class IgnoresModes(StandInUnixModes inner) : IUnixDirectoryModes
    {
        public void CreateOwnerOnly(string directory) => inner.CreateOwnerOnly(directory);

        public UnixFileMode Get(string directory) => inner.Get(directory);

        public void Set(string directory, UnixFileMode mode)
        {
        }
    }

    /// <summary>
    /// #4004's review, round 2 (M1): the "was open" verdict lived only in the process's memory while the 0700 chmod was
    /// permanent. A start whose store stood down never provisioned and never loaded the key, but the web or MCP host's
    /// earlier-credential read still set the directory 0700, and the NEXT start found it owner-only and trusted the
    /// planted key and the planted admin password, which provisioning re-asserts on the admin role. Now the look that
    /// finds the directory open removes both at once. The next start's provisioning half is
    /// <c>ComposeStoreRolesLiveTests</c>' (it needs a store); this pins the files and the key.
    /// </summary>
    [Fact]
    public void AHostsReadOfAnOpenDirectory_DiscardsWhatWasPlanted_SoTheNextStartUsesNeither()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var keyPath = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            var adminPath = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName));
            var planted = Convert.ToBase64String(TestLogHashKeys.FixedMaterial);
            StandInUnixModes.WriteOwnerOnly(keyPath, OperatingSystem.IsWindows() ? DarlingSecrets.Protect(planted) : planted);
            StandInUnixModes.WriteOwnerOnly(adminPath, "Planted4004Admin");
            StandInUnixModes.WriteOwnerOnly(keyPath + ".tmp", "planted-temporary");
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            /* This start: the store stood down, so neither provisioning nor the key load ran; a host read its role's
               earlier credential, and that read closed the directory. */
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, DarlingManagedPostgres.AdminRoleName, NullLogger.Instance);

                Assert.Null(earlier.Password);
                Assert.Equal($"{adminPath} does not exist", earlier.MissingReason);
            }

            Assert.Equal("700", modes.Octal(directory));
            Assert.False(File.Exists(adminPath), "the planted admin password outlived the start that closed its directory");
            Assert.False(File.Exists(keyPath), "the planted key outlived the start that closed its directory");
            Assert.False(File.Exists(keyPath + ".tmp"));

            /* The next start finds the directory owner-only, as this one left it. */
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Refusal);
                Assert.True(load.Generated, "the next start loaded the key planted while the directory was open");
                Assert.NotEqual(TestLogHashKeys.Fixed.RawLineHash(FailedUpdate), load.Key!.RawLineHash(FailedUpdate));
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4004's review, round 3 (M): the discard stopped at the first entry it could not remove and set the directory
    /// back to its open mode, so a directory planted at the FIRST name (<c>pg-admin-credential</c>) shielded the 0600
    /// files planted at the later ones. Every start refused and reopened it, and an operator who did what the refusal
    /// said (set it 0700, remove the entry it named) got a next start that trusted the planted passwords and key. Now
    /// every name is tried: every planted file goes, an empty directory at a name goes, the directory stays 0700, and
    /// the start refuses naming what is left. With that removed, the next start loads no planted key or password.
    /// </summary>
    [Fact]
    public void ADirectoryAtTheFirstName_ShieldsNoPlantedFileBehindIt_AndTheDirectoryStaysOwnerOnly()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var adminPath = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.AdminRoleName));
            var viewerPath = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.ViewerRoleName));
            var mcpPath = Path.Combine(directory, DarlingManagedRoles.ComposeStoreCredentialFileName(DarlingManagedPostgres.McpRoleName));
            var keyPath = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            Assert.Equal(Path.GetFileName(adminPath), DarlingManagedRoles.CredentialDirectoryFileNames[0]);

            Directory.CreateDirectory(Path.Combine(adminPath, "someones"));
            Directory.CreateDirectory(keyPath + ".tmp");
            StandInUnixModes.WriteOwnerOnly(viewerPath, "Planted4004Viewer");
            StandInUnixModes.WriteOwnerOnly(mcpPath, "Planted4004Mcp");
            var planted = Convert.ToBase64String(TestLogHashKeys.FixedMaterial);
            StandInUnixModes.WriteOwnerOnly(keyPath, OperatingSystem.IsWindows() ? DarlingSecrets.Protect(planted) : planted);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            var log = new CapturingTestLogger();
            ComposeCredentialDirectoryTrust trust;
            DarlingLogHashKeyLoad refused;
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                trust = DarlingManagedRoles.PrepareComposeCredentialDirectory(directory, create: true, log);
                refused = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            }

            Assert.False(File.Exists(viewerPath), "a planted viewer password outlived the start: the discard stopped at the directory before it");
            Assert.False(File.Exists(mcpPath), "a planted mcp password outlived the start");
            Assert.False(File.Exists(keyPath), "the planted key outlived the start");
            Assert.False(Directory.Exists(keyPath + ".tmp"), "an empty directory at a name is not removed");
            Assert.True(Directory.Exists(Path.Combine(adminPath, "someones")), "someone else's tree was removed");
            Assert.Equal("700", modes.Octal(directory));
            Assert.Contains($"{Path.GetFileName(viewerPath)}, {Path.GetFileName(mcpPath)}, {DarlingLogHashKeyFile.FileName}", log.Joined, StringComparison.Ordinal);

            Assert.False(trust.MayWrite);
            Assert.Contains($"could not be removed, so nothing in it is used this start: {adminPath} (", trust.Distrust, StringComparison.Ordinal);
            Assert.Contains("so remove it and restart", trust.Distrust, StringComparison.Ordinal);
            Assert.Null(refused.Key);
            Assert.Contains(adminPath, refused.Refusal, StringComparison.Ordinal);
            Assert.DoesNotContain("restrict it to the service account", refused.Refusal, StringComparison.Ordinal);

            /* The operator does what the refusal says (round 2's said to close the directory too, a no-op now), and
               restarts. */
            Directory.Delete(adminPath, recursive: true);
            modes.Report(directory, "700");
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Refusal);
                Assert.True(load.Generated, "the next start loaded the key planted while the directory was open");
                Assert.NotEqual(TestLogHashKeys.Fixed.RawLineHash(FailedUpdate), load.Key!.RawLineHash(FailedUpdate));

                foreach (var role in new[] { DarlingManagedPostgres.AdminRoleName, DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName })
                {
                    var earlier = DarlingManagedRoles.ReadEarlierComposeCredential(directory, role, NullLogger.Instance);
                    Assert.True(earlier.Password is null, $"the next start read a planted '{role}' password");
                }
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4004's review, round 3 (Erik's default for the parked item): a key the start discarded and regenerated is
    /// noted on the collection-log row of the FIRST pg_log_events run after it, and only that run. The load says the key
    /// replaced one (a first-ever key replaces nothing); the worker arms the note from it; and every run it records
    /// passes through <see cref="LogHashKeyRotationNote.ApplyTo"/>: another collector's run neither carries nor takes
    /// it, the first pg_log_events run carries it beside its own note and measurements, and the second does not.
    /// </summary>
    [Fact]
    public void TheFirstPgLogEventsRunAfterARotation_CarriesTheNote_AndTheSecondDoesNot()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            var planted = Convert.ToBase64String(TestLogHashKeys.FixedMaterial);
            StandInUnixModes.WriteOwnerOnly(path, OperatingSystem.IsWindows() ? DarlingSecrets.Protect(planted) : planted);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            DarlingLogHashKeyLoad rotated;
            DarlingLogHashKeyLoad next;
            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                rotated = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            }

            using (ComposeCredentialDirectoryGuard.BeginForTest(modes))
            {
                next = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            }

            Assert.True(rotated.Generated);
            var note = DarlingLogHashKeyFile.RotationNote(rotated);
            Assert.NotNull(note);
            Assert.StartsWith($"the store's log-hash key was replaced at service start ({path} was discarded because its mode was 0777", note, StringComparison.Ordinal);
            Assert.False(next.Generated);
            Assert.Null(next.Replaced);
            Assert.Null(DarlingLogHashKeyFile.RotationNote(next));

            var holder = new LogHashKeyRotationNote();
            holder.Arm(note);
            holder.Arm(DarlingLogHashKeyFile.RotationNote(next));

            var run = new CollectorRunResult(3, 5, 7, new[] { new CollectorMeasurement("lines_read", 12) }, HostNote: "no new classifiable log events");
            Assert.Same(run, holder.ApplyTo(PgWaitSamplingCollector.Instance.Name, run));

            var first = holder.ApplyTo(PgLogEventsCollector.Instance.Name, run);
            Assert.Equal($"no new classifiable log events; {note}", first.HostNote);
            Assert.Contains(note, first.Note, StringComparison.Ordinal);
            Assert.Contains("lines_read", first.Note, StringComparison.Ordinal);

            var second = holder.ApplyTo(PgLogEventsCollector.Instance.Name, run);
            Assert.Same(run, second);
            Assert.DoesNotContain("log-hash key", second.Note ?? string.Empty, StringComparison.Ordinal);

            /* The worker arms it from its one load, and applies it to every run it records, after the dispatch and before
               the row is written. */
            var worker = RepoFile.ReadRepoFileLf("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
            Assert.Contains("_logHashKeyRotation.Arm(DarlingLogHashKeyFile.RotationNote(logHashKeyLoad));", worker, StringComparison.Ordinal);
            var runOne = worker.IndexOf("private async Task<int> RunOneAsync(", StringComparison.Ordinal);
            var dispatch = worker.IndexOf("var result = await run(runner, runtime, cancellationToken);", runOne, StringComparison.Ordinal);
            var apply = worker.IndexOf("result = _logHashKeyRotation.ApplyTo(collectorName, result);", runOne, StringComparison.Ordinal);
            var written = worker.IndexOf("await DarlingObservability.LogCollectionAsync(\n                _postgres!, runtime, collectorName, status, result.Rows, result.SqlMs, result.StorageMs, result.Note,", runOne, StringComparison.Ordinal);
            Assert.True(runOne > 0 && dispatch > runOne && apply > dispatch && written > apply,
                "RunOneAsync applies the rotation note after the dispatch and before the row that carries result.Note is written");
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4004's review, round 2 (L3): a directory where the new key's temporary file goes made every start refuse with
    /// "it did not exist", naming only the key. An empty one is cleared and the key generated; one with something in it
    /// is left alone and named as the reason.
    /// </summary>
    [Fact]
    public void ADirectoryAtTheTemporaryPath_IsClearedWhenEmpty_AndNamedWhenNot()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            var temporary = path + ".tmp";
            Directory.CreateDirectory(Path.Combine(temporary, "someones"));

            var refused = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(refused.Key);
            Assert.Contains($"{temporary}, the file a new key is written through before it is moved into place, is a directory the service could not remove", refused.Refusal, StringComparison.Ordinal);
            Assert.DoesNotContain("did not exist", refused.Refusal, StringComparison.Ordinal);
            Assert.True(Directory.Exists(Path.Combine(temporary, "someones")));

            Directory.Delete(Path.Combine(temporary, "someones"));
            var generated = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

            Assert.Null(generated.Refusal);
            Assert.True(generated.Generated);
            Assert.False(Directory.Exists(temporary));
        }
        finally
        {
            Remove(directory);
        }
    }

    [Fact]
    public void TheKeyLivesBesideTheDistributionsOtherSecrets()
    {
        var configPath = Path.Combine(Path.GetTempPath(), "darling-4004-config", "darling.json");

        var byo = new DarlingConfig();
        byo.Postgres.Managed = false;
        Assert.Equal(
            Path.Combine(Path.GetDirectoryName(configPath)!, DarlingLogHashKeyFile.BringYourOwnDirectoryName),
            DarlingLogHashKeyFile.DirectoryFor(byo, configPath));

        if (OperatingSystem.IsWindows())
        {
            var managed = new DarlingConfig();
            managed.Postgres.Managed = true;
            var credential = DarlingManagedPostgres.CredentialPathFor(DarlingManagedPostgres.ResolveDataDirectory(managed.Postgres));
            Assert.Equal(Path.GetDirectoryName(credential), DarlingLogHashKeyFile.DirectoryFor(managed, configPath));
            Assert.Equal(DarlingLogHashKeyFile.WindowsFileName, DarlingLogHashKeyFile.FileName);
        }
    }

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

    private static CollectorContext Context(PgLogHashKey? key) => new()
    {
        ServerId = 1,
        ServerName = "target-a",
        CollectionTime = new DateTime(2026, 9, 23, 3, 10, 0, DateTimeKind.Unspecified),
        Deltas = new CollectorDeltaCalculator(),
        LogHashKey = key,
    };

    private static string NewDirectory() =>
        Path.Combine(Path.GetTempPath(), "darling-4004-" + Guid.NewGuid().ToString("N"), "keys");

    private static void Remove(string directory)
    {
        var root = Path.GetDirectoryName(directory)!;
        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }

    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    [return: System.Runtime.InteropServices.MarshalAs(System.Runtime.InteropServices.UnmanagedType.Bool)]
    private static extern bool CreateHardLinkW(string lpFileName, string lpExistingFileName, IntPtr lpSecurityAttributes);
}
