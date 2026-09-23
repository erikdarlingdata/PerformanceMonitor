/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
            Assert.Contains("never replaces an existing key", load.Refusal, StringComparison.Ordinal);
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
