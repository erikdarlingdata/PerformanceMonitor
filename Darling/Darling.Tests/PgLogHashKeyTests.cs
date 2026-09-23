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
/// <see cref="ComposeCredentialDirectoryStart.BeginForTest"/>; one instance outlives a start, the way a directory does.
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
    /// was 0777. The verdict is the start's now: the first look records the mode, and every later caller in that start
    /// is judged by it. Driven through the Unix-mode seam, since the suite runs on Windows: provisioning's own call on a
    /// directory reported as 0777 that holds a planted key, then the load. The planted key is discarded and a new one
    /// generated, as #3983 replaces every role password there, and the NEXT start, which finds the directory 0700
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
            using (ComposeCredentialDirectoryStart.BeginForTest(modes))
            {
                /* Provisioning's call, first, as the worker makes it; spelled with a trailing separator, because the
                   start's record is per full path, not per spelling. */
                var provisioning = DarlingManagedRoles.PrepareComposeCredentialDirectory(
                    directory + Path.DirectorySeparatorChar, create: true, NullLogger.Instance);
                Assert.Equal(
                    "its mode was 0777, which let other users put files in it until the service set it to owner-only this start",
                    provisioning.Distrust);
                Assert.True(provisioning.MayWrite);
                Assert.Equal("700", modes.Octal(directory));

                load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);
            }

            Assert.Null(load.Refusal);
            Assert.True(load.Generated, "the planted key was loaded: the key load judged its directory by the mode provisioning had just set");
            Assert.NotEqual(TestLogHashKeys.Fixed.RawLineHash(FailedUpdate), load.Key!.RawLineHash(FailedUpdate));
            AssertOwnerOnly(path);

            using (ComposeCredentialDirectoryStart.BeginForTest(modes))
            {
                var next = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.False(next.Generated);
                Assert.Equal(load.Key.RawLineHash(FailedUpdate), next.Key!.RawLineHash(FailedUpdate));
            }
        }
        finally
        {
            Remove(directory);
        }
    }

    /// <summary>
    /// #4004's review: in a directory that was open until this start, a directory at the key's path is refused like
    /// anywhere else (discarding never deletes a tree), and a directory still open to others after the chmod (a
    /// filesystem that ignores modes) is refused without anything being written or removed.
    /// </summary>
    [Fact]
    public void InADirectoryThatWasOpen_ADirectoryAtThePathOrAModeThatWillNotClose_IsRefused()
    {
        var directory = NewDirectory();
        try
        {
            Directory.CreateDirectory(directory);
            var path = Path.Combine(directory, DarlingLogHashKeyFile.FileName);
            Directory.CreateDirectory(path);
            var modes = new StandInUnixModes();
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryStart.BeginForTest(modes))
            {
                var load = DarlingLogHashKeyFile.Load(directory, NullLogger.Instance);

                Assert.Null(load.Key);
                Assert.Contains("it is a directory", load.Refusal, StringComparison.Ordinal);
                Assert.True(Directory.Exists(path));
            }

            Directory.Delete(path);
            StandInUnixModes.WriteOwnerOnly(path, "left-alone");
            var ignoresModes = new IgnoresModes(modes);
            modes.Report(directory, "777");

            using (ComposeCredentialDirectoryStart.BeginForTest(ignoresModes))
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
}
