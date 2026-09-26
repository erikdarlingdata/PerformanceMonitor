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
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The service-orchestrated store runtime upgrade (#1706). The ungated tests pin the pure decisions the
/// orchestration is built on — version parsing, the transfer-mode arithmetic, and the initdb/pg_upgrade
/// argument shapes, which is where a wrong default silently produces an incompatible cluster.
///
/// <para>The gated test is the one that matters: it builds a REAL store on the OLD runtime (initdb,
/// TimescaleDB, a hypertable with TOAST-sized plan XML, a continuous aggregate, compression), then runs
/// the real <see cref="DarlingManagedPostgres.EnsureRunningAsync"/> against a deployment whose shipped zip
/// is the NEW runtime, and proves the upgraded store still holds the same rows and still answers through
/// its continuous aggregate. That is the UPGRADED-IN-PLACE fixture #1705 proved CI could not see: a store
/// that has been through a version change behaves differently from the fresh ones darling-pg creates.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")], and NOT for the reason a sweep might assume.
   This class never reads DARLING_TEST_PG at all — it reads DARLING_TEST_PGRUNTIME_OLD / DARLING_TEST_PGRUNTIME_NEWZIP, which merely shares that
   prefix, and it stands up its OWN throwaway cluster from the bundled runtime. A substring search for
   "DARLING_TEST_PG" matches it anyway (that is how #1776's original sweep came to list it), so this note is here to
   stop the next one serializing a class that touches no shared store. */
public sealed class DarlingStoreUpgradeTests
{
    [Theory]
    [InlineData("pg_ctl (PostgreSQL) 18.4", 18)]
    [InlineData("pg_ctl (PostgreSQL) 17.10", 17)]
    [InlineData("initdb (PostgreSQL) 16.2", 16)]
    [InlineData("postgres (PostgreSQL) 19beta1", 19)]
    [InlineData("17", 17)]
    [InlineData("", null)]
    [InlineData("no version here", null)]
    public void ParsePostgresMajor_ReadsTheMajorOrRefuses(string input, int? expected)
        => Assert.Equal(expected, DarlingStoreUpgrade.ParsePostgresMajor(input));

    [Theory]
    [InlineData("pg_ctl (PostgreSQL) 18.6", "18.6")]
    [InlineData("pg_ctl (PostgreSQL) 17.10", "17.10")]
    [InlineData("18.4", "18.4")]
    [InlineData("18.6.0", "18.6")]
    [InlineData("postgres (PostgreSQL) 19beta1", null)]
    [InlineData("18", null)]
    [InlineData("", null)]
    [InlineData("no version here", null)]
    public void ParsePostgresVersion_ReadsMajorAndMinorOrRefuses(string input, string? expected)
        => Assert.Equal(expected is null ? null : Version.Parse(expected), DarlingStoreUpgrade.ParsePostgresVersion(input));

    /// <summary>
    /// #3906: an unstamped host is adopted as current only when it IS the shipped runtime. Before this, a
    /// matching major and TimescaleDB were enough, so 18.4 on disk was stamped as the 18.6 package and kept
    /// every CVE 18.6 fixes. An unreadable minor falls back to the TimescaleDB comparison alone, which is
    /// exactly the check as it stood before.
    /// </summary>
    [Theory]
    [InlineData("18.6", "18.6", "2.30.1", "2.30.1", true)]
    [InlineData("18.4", "18.6", "2.30.1", "2.30.1", false)]
    [InlineData("18.6", "18.4", "2.30.1", "2.30.1", false)]
    [InlineData("18.6", "18.6", "2.28.1", "2.30.1", false)]
    [InlineData("18.4", "18.6", "2.28.1", "2.30.1", false)]
    [InlineData(null, "18.6", "2.30.1", "2.30.1", true)]
    [InlineData("18.6", null, "2.30.1", "2.30.1", true)]
    [InlineData(null, null, "2.28.1", "2.30.1", false)]
    [InlineData("18.6", "18.6", null, null, true)]
    public void ExtractedRuntimeMatchesPackage_NeedsTheMinorAndTheExtensionToMatch(
        string? installedPostgres, string? packagePostgres, string? installedTimescale, string? packageTimescale, bool expected)
        => Assert.Equal(
            expected,
            DarlingStoreUpgrade.ExtractedRuntimeMatchesPackage(
                installedPostgres is null ? null : Version.Parse(installedPostgres),
                packagePostgres is null ? null : Version.Parse(packagePostgres),
                installedTimescale,
                packageTimescale));

    [Fact]
    public void ParseTimescaleDefaultVersion_ReadsTheControlFile()
    {
        const string control = """
            # timescaledb extension
            comment = 'Enables scalable inserts and complex queries for time-series data'
            default_version = '2.28.1'
            module_pathname = '$libdir/timescaledb'
            """;

        Assert.Equal("2.28.1", DarlingStoreUpgrade.ParseTimescaleDefaultVersion(control));
        Assert.Null(DarlingStoreUpgrade.ParseTimescaleDefaultVersion("comment = 'no default here'"));
        Assert.Null(DarlingStoreUpgrade.ParseTimescaleDefaultVersion(null));
    }

    /* ---------------- ParseAutoConf (#4253) ---------------- */

    [Fact]
    public void ParseAutoConf_DecodesQuotedValues_DoublesQuoteAndBackslashLikeAlterSystemWrites()
    {
        /* Exactly the escaping a live ALTER SYSTEM write produced for #4253: an embedded quote doubled
           ('' -> '), an embedded backslash doubled (\\ -> \). */
        const string content = "log_line_prefix = 'it''s a test \\\\backslash % value'\n";

        var setting = Assert.Single(DarlingStoreUpgrade.ParseAutoConf(content, out _));

        Assert.Equal("log_line_prefix", setting.Name);
        Assert.Equal("it's a test \\backslash % value", setting.DisplayValue);
    }

    [Fact]
    public void ParseAutoConf_SkipsCommentsAndTheHeaderLinesPostgresWrites()
    {
        const string content = """
            # Do not edit this file manually!
            # It will be overwritten by the ALTER SYSTEM command.
            work_mem = '128MB'
            """;

        var setting = Assert.Single(DarlingStoreUpgrade.ParseAutoConf(content, out _));

        Assert.Equal("work_mem", setting.Name);
        Assert.Equal("128MB", setting.DisplayValue);
    }

    [Fact]
    public void ParseAutoConf_IgnoresBlankLines()
    {
        var setting = Assert.Single(DarlingStoreUpgrade.ParseAutoConf("\n   \nwork_mem = '128MB'\n\n", out _));

        Assert.Equal("work_mem", setting.Name);
    }

    [Fact]
    public void ParseAutoConf_ANameSetTwice_TheLastOneWins()
    {
        const string content = """
            work_mem = '64MB'
            shared_buffers = '1GB'
            work_mem = '256MB'
            """;

        var settings = DarlingStoreUpgrade.ParseAutoConf(content, out _);

        Assert.Equal(2, settings.Count);
        var workMem = Assert.Single(settings, s => s.Name == "work_mem");
        Assert.Equal("256MB", workMem.DisplayValue);
        Assert.Equal("work_mem = '256MB'", workMem.RawLine);
    }

    [Fact]
    public void ParseAutoConf_AcceptsAnUnquotedBareToken()
    {
        var setting = Assert.Single(DarlingStoreUpgrade.ParseAutoConf("max_connections = 250\n", out _));

        Assert.Equal("max_connections", setting.Name);
        Assert.Equal("250", setting.DisplayValue);
    }

    [Fact]
    public void ParseAutoConf_SkipsALineItCannotParse_RatherThanGuessing()
    {
        /* An unterminated quote on its own line is malformed and dropped; the next line still parses —
           one bad line must not cost every other setting in the file. */
        var setting = Assert.Single(DarlingStoreUpgrade.ParseAutoConf("broken = 'unterminated\nwork_mem = '128MB'\n", out _));

        Assert.Equal("work_mem", setting.Name);
    }

    [Fact]
    public void ParseAutoConf_SkipsALineWhoseNameIsNotAValidGucName()
    {
        /* A quote or a space in the name portion is not something ALTER SYSTEM ever writes — only a
           hand-edited file could put one there — and that name goes into one double-quoted "-C {name}"
           argument in CarryAutoConfAsync, so a bad one could split that argument. An extension-qualified
           name like timescaledb.max_background_workers is still valid and must still be kept. */
        const string content = """
            bad"name = '1'
            a b = '1'
            timescaledb.max_background_workers = 8
            """;

        var settings = DarlingStoreUpgrade.ParseAutoConf(content, out var skippedLines);

        var setting = Assert.Single(settings);
        Assert.Equal("timescaledb.max_background_workers", setting.Name);
        Assert.Equal("8", setting.DisplayValue);
        /* Line numbers, not names/text (round-1 security review, #4280 Medium 1) — line 1 is "bad"name = '1'",
           line 2 is "a b = '1'", both invalid GUC names. */
        Assert.Equal(new[] { 1, 2 }, skippedLines);
    }

    [Theory]
    [InlineData("work_mem '128MB'\n", "work_mem", "128MB")]
    [InlineData("max_connections 250\n", "max_connections", "250")]
    public void ParseAutoConf_AcceptsANameValueLineWithNoEqualsSign(string content, string expectedName, string expectedValue)
    {
        /* PostgreSQL's own grammar makes the '=' optional between name and value (round-1 security review,
           #4280 parse note) — ALTER SYSTEM always writes one, but a hand-edited file need not. */
        var setting = Assert.Single(DarlingStoreUpgrade.ParseAutoConf(content, out var skippedLines));

        Assert.Equal(expectedName, setting.Name);
        Assert.Equal(expectedValue, setting.DisplayValue);
        Assert.Empty(skippedLines);
    }

    [Fact]
    public void ParseAutoConf_ANameValueLineSeparatedByNbspOnly_IsSkippedNotCarried()
    {
        /* U+00A0 (NBSP) satisfies char.IsWhiteSpace but is neither ' ' nor '\t' — guc-file.l's own
           tokenizer would not split a line on it, so this parser must not either (#4280 round-2 Low 2).
           Before the fix this split into name "work_mem" and value "128MB" and was carried; now nothing
           separates name from value, so the whole line is unparseable and only its line number is
           reported — never silently carried as work_mem. */
        var settings = DarlingStoreUpgrade.ParseAutoConf("work_mem 128MB\n", out var skippedLines);

        Assert.Empty(settings);
        Assert.Equal(new[] { 1 }, skippedLines);
    }

    [Fact]
    public void ParseAutoConf_ANameValueLineWhoseValueStartsWithNbsp_IsNotCarriedAsAStrippedValue()
    {
        /* The no-'=' form's value side used string.Trim() (full-Unicode trim), stripping a leading NBSP
           that guc-file.l would not treat as separating whitespace — silently carrying "64MB" when the
           file actually held NBSP+"64MB" (#4280 round-2 part 3 leftover, one token). Trim(' ', '\t') now
           matches the '=' form two lines below it: the NBSP stays, so DecodeAutoConfValue treats it as
           the start of an empty unquoted token and the line is not carried at all — never work_mem = 64MB. */
        var settings = DarlingStoreUpgrade.ParseAutoConf("work_mem  64MB\n", out var skippedLines);

        Assert.Empty(settings);
        Assert.Empty(skippedLines);
    }

    [Fact]
    public void ParseAutoConf_ANameValueLineSplitByABareCarriageReturn_SkipsBothResultingLines()
    {
        /* StringReader.ReadLine treats a bare '\r' as its own line terminator, so "work_mem\r128MB" reads
           back as two separate lines — neither "work_mem" nor "128MB" has a space/tab left to split on, so
           BOTH land in skippedLines under their own line number, not just the first (#4280 round-2 Low 2). */
        var settings = DarlingStoreUpgrade.ParseAutoConf("work_mem\r128MB\n", out var skippedLines);

        Assert.Empty(settings);
        Assert.Equal(new[] { 1, 2 }, skippedLines);
    }

    [Fact]
    public void DecideTransferMode_CopyWhenTheVolumeHasRoomForTwoCopies()
    {
        const long tenGb = 10L * 1024 * 1024 * 1024;
        var decision = DarlingStoreUpgrade.DecideTransferMode(tenGb, 40L * 1024 * 1024 * 1024, hardLinksSupported: true);

        Assert.Equal(DarlingStoreUpgrade.FileTransferMode.Copy, decision.Mode);
    }

    [Fact]
    public void DecideTransferMode_LinkOnlyWhenCopyCannotFitAndLinksWork()
    {
        const long tenGb = 10L * 1024 * 1024 * 1024;

        /* 12 GB free cannot hold a second 10 GB copy plus slack, but easily covers link mode. */
        var link = DarlingStoreUpgrade.DecideTransferMode(tenGb, 12L * 1024 * 1024 * 1024, hardLinksSupported: true);
        Assert.Equal(DarlingStoreUpgrade.FileTransferMode.Link, link.Mode);

        /* Same space, but the volume cannot make hard links: there is no safe mode left, so do not upgrade.
           An abort keeps the store running on its existing major, which beats a half-finished upgrade. */
        var abort = DarlingStoreUpgrade.DecideTransferMode(tenGb, 12L * 1024 * 1024 * 1024, hardLinksSupported: false);
        Assert.Equal(DarlingStoreUpgrade.FileTransferMode.Abort, abort.Mode);
    }

    [Fact]
    public void DecideTransferMode_AbortWhenEvenLinkModeCannotFit()
    {
        const long tenGb = 10L * 1024 * 1024 * 1024;
        var decision = DarlingStoreUpgrade.DecideTransferMode(tenGb, 200L * 1024 * 1024, hardLinksSupported: true);

        Assert.Equal(DarlingStoreUpgrade.FileTransferMode.Abort, decision.Mode);
    }

    [Fact]
    public void BuildInitDbArguments_ReproducesTheOldClusterRatherThanTheNewMajorsDefaults()
    {
        /* The managed store's own identity: UTF8 + C locale via libc + checksums ON. */
        var identity = new DarlingStoreUpgrade.ClusterIdentity("UTF8", "C", "C", "c", null, DataChecksums: true);
        var arguments = DarlingStoreUpgrade.BuildInitDbArguments(@"C:\pg\new", "darling", @"C:\pg\pw.tmp", identity, 18);

        Assert.Contains("-U darling", arguments, StringComparison.Ordinal);
        Assert.Contains("-A scram-sha-256", arguments, StringComparison.Ordinal);
        Assert.Contains("-E UTF8", arguments, StringComparison.Ordinal);
        Assert.Contains("--locale-provider=libc", arguments, StringComparison.Ordinal);
        Assert.Contains("--lc-collate=C", arguments, StringComparison.Ordinal);
        Assert.Contains("--lc-ctype=C", arguments, StringComparison.Ordinal);
        Assert.Contains("--data-checksums", arguments, StringComparison.Ordinal);
        Assert.DoesNotContain("--no-data-checksums", arguments, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildInitDbArguments_DisablesChecksumsExplicitlyOn18BecauseItsDefaultFlipped()
    {
        /* PostgreSQL 18 changed initdb to enable data checksums by DEFAULT, and pg_upgrade hard-refuses a
           checksum mismatch. A checksum-less 17 store therefore needs an explicit --no-data-checksums, a
           flag that does not exist before 18 — which is exactly why this is derived, never assumed. */
        var identity = new DarlingStoreUpgrade.ClusterIdentity("UTF8", "C", "C", "c", null, DataChecksums: false);

        var on18 = DarlingStoreUpgrade.BuildInitDbArguments(@"C:\pg\new", "darling", @"C:\pg\pw.tmp", identity, 18);
        Assert.Contains("--no-data-checksums", on18, StringComparison.Ordinal);

        var on17 = DarlingStoreUpgrade.BuildInitDbArguments(@"C:\pg\new", "darling", @"C:\pg\pw.tmp", identity, 17);
        Assert.DoesNotContain("--no-data-checksums", on17, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildInitDbArguments_CarriesTheIcuAndBuiltinProvidersThrough()
    {
        var icu = new DarlingStoreUpgrade.ClusterIdentity("UTF8", "en_US.UTF-8", "en_US.UTF-8", "i", "en-US", true);
        var icuArguments = DarlingStoreUpgrade.BuildInitDbArguments(@"C:\pg\new", "darling", @"C:\pg\pw.tmp", icu, 18);
        Assert.Contains("--locale-provider=icu", icuArguments, StringComparison.Ordinal);
        Assert.Contains("--icu-locale=en-US", icuArguments, StringComparison.Ordinal);

        var builtin = new DarlingStoreUpgrade.ClusterIdentity("UTF8", "C", "C", "b", "C.UTF-8", true);
        var builtinArguments = DarlingStoreUpgrade.BuildInitDbArguments(@"C:\pg\new", "darling", @"C:\pg\pw.tmp", builtin, 18);
        Assert.Contains("--locale-provider=builtin", builtinArguments, StringComparison.Ordinal);
        Assert.Contains("--builtin-locale=C.UTF-8", builtinArguments, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPgUpgradeArguments_CheckModeIsADryRunAndLinkModeIsOptIn()
    {
        var check = DarlingStoreUpgrade.BuildPgUpgradeArguments(
            @"C:\pg\old\bin", @"C:\pg\new\bin", @"C:\data\pg", @"C:\data\pg-upgrade-18", "darling",
            DarlingStoreUpgrade.FileTransferMode.Copy, checkOnly: true, jobs: 4, serverOptions: null);

        Assert.Contains("--check", check, StringComparison.Ordinal);
        Assert.Contains(@"--old-bindir ""C:\pg\old\bin""", check, StringComparison.Ordinal);
        Assert.Contains(@"--new-datadir ""C:\data\pg-upgrade-18""", check, StringComparison.Ordinal);
        Assert.Contains("--username darling", check, StringComparison.Ordinal);
        Assert.DoesNotContain("--link", check, StringComparison.Ordinal);
        /* --check does no file work, so jobs would only be noise. */
        Assert.DoesNotContain("--jobs", check, StringComparison.Ordinal);

        var real = DarlingStoreUpgrade.BuildPgUpgradeArguments(
            @"C:\pg\old\bin", @"C:\pg\new\bin", @"C:\data\pg", @"C:\data\pg-upgrade-18", "darling",
            DarlingStoreUpgrade.FileTransferMode.Link, checkOnly: false, jobs: 4, serverOptions: null);

        Assert.Contains("--link", real, StringComparison.Ordinal);
        Assert.Contains("--jobs 4", real, StringComparison.Ordinal);
        Assert.DoesNotContain("--check", real, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPgUpgradeArguments_QuiescesTimescaleOnBothClusters()
    {
        /* -o reaches the OLD cluster, -O the NEW one, and BOTH need it: pg_upgrade starts each in turn and
           connects database by database, which is exactly the workload TimescaleDB's scheduler background
           worker deadlocks against (timescale/timescaledb#1593). One side quiesced is not enough. */
        var arguments = DarlingStoreUpgrade.BuildPgUpgradeArguments(
            @"C:\pg\old\bin", @"C:\pg\new\bin", @"C:\data\pg", @"C:\data\pg-upgrade-18", "darling",
            DarlingStoreUpgrade.FileTransferMode.Copy, checkOnly: false, jobs: 1,
            DarlingStoreUpgrade.QuiesceTimescaleServerOptions);

        Assert.Contains(@"-o ""-c timescaledb.max_background_workers=0", arguments, StringComparison.Ordinal);
        Assert.Contains(@"-O ""-c timescaledb.max_background_workers=0", arguments, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildPgUpgradeArguments_RestoresDualStackLoopbackForTheUpgradeWindow()
    {
        /* pg_upgrade has no Unix sockets on Windows, so it dials its own clusters BY NAME, and Windows
           resolves "localhost" to ::1 first. The managed v1 conf block pins listen_addresses to '127.0.0.1'
           — IPv4 only — leaving nothing on ::1 for it to reach. Caught verbatim in pg_upgrade's own log:
           connection to server at "localhost" (::1), port 50432 failed. The override is scoped to the two
           throwaway postmasters; the store's conf is never edited. */
        var arguments = DarlingStoreUpgrade.BuildPgUpgradeArguments(
            @"C:\pg\old\bin", @"C:\pg\new\bin", @"C:\data\pg", @"C:\data\pg-upgrade-18", "darling",
            DarlingStoreUpgrade.FileTransferMode.Copy, checkOnly: false, jobs: 1,
            DarlingStoreUpgrade.QuiesceTimescaleServerOptions);

        Assert.Contains("-c listen_addresses=localhost", arguments, StringComparison.Ordinal);
        /* Both clusters, because pg_upgrade connects to each in turn. */
        Assert.Equal(2, CountOccurrences(arguments, "-c listen_addresses=localhost"));
    }

    [Fact]
    public void BuildPgUpgradeArguments_NeverUsesTheWellKnownDefaultPort()
    {
        /* pg_upgrade defaults BOTH throwaway postmasters to 50432, so two upgrades on one host — or one
           leftover postmaster from an interrupted run — collide and pg_upgrade talks to a stranger's
           cluster. Observed here as FATAL: role "darling" does not exist, answered by a postmaster this
           service never started. */
        var arguments = DarlingStoreUpgrade.BuildPgUpgradeArguments(
            @"C:\pg\old\bin", @"C:\pg\new\bin", @"C:\data\pg", @"C:\data\pg-upgrade-18", "darling",
            DarlingStoreUpgrade.FileTransferMode.Copy, checkOnly: true, jobs: 1, serverOptions: null);

        Assert.Contains("--old-port ", arguments, StringComparison.Ordinal);
        Assert.Contains("--new-port ", arguments, StringComparison.Ordinal);
        Assert.DoesNotContain("50432", arguments, StringComparison.Ordinal);
        /* The two must differ: pg_upgrade has both clusters up at once during the dump/restore. */
        Assert.NotEqual(DarlingStoreUpgrade.UpgradeOldClusterPort, DarlingStoreUpgrade.UpgradeNewClusterPort);
    }

    [Fact]
    public void FindOccupiedPorts_NamesOnlyThePortsActuallyListening()
    {
        /* MED-HIGH from review: moving off pg_upgrade's shared 50432 default removed the collision with
           other software, not with OURSELVES — our own timed-out upgrade can leave a postmaster on 55432,
           and the next attempt would then inspect a stranger's cluster with no actionable error. The
           preflight must name which port so an operator can kill the right thing. */
        var listeners = new[]
        {
            new IPEndPoint(IPAddress.Loopback, 5432),
            new IPEndPoint(IPAddress.Loopback, DarlingStoreUpgrade.UpgradeNewClusterPort),
            new IPEndPoint(IPAddress.IPv6Loopback, 8080),
        };

        var occupied = DarlingStoreUpgrade.FindOccupiedPorts(
            listeners, DarlingStoreUpgrade.UpgradeOldClusterPort, DarlingStoreUpgrade.UpgradeNewClusterPort);

        Assert.Equal(new[] { DarlingStoreUpgrade.UpgradeNewClusterPort }, occupied);
    }

    [Fact]
    public void FindOccupiedPorts_EmptyWhenBothArePrivate()
        => Assert.Empty(DarlingStoreUpgrade.FindOccupiedPorts(
            new[] { new IPEndPoint(IPAddress.Loopback, 5432) },
            DarlingStoreUpgrade.UpgradeOldClusterPort,
            DarlingStoreUpgrade.UpgradeNewClusterPort));

    [Theory]
    [InlineData("timescaledb-2.28.1.dll", "2.28.1")]
    [InlineData(@"C:\pg\lib\timescaledb-2.24.0.dll", "2.24.0")]
    /* The TSL sibling and the unversioned loader must NOT answer, or the comparison could read a version
       off the wrong file and call two different runtimes identical. */
    [InlineData("timescaledb-tsl-2.28.1.dll", null)]
    [InlineData("timescaledb.dll", null)]
    [InlineData("libpq.dll", null)]
    public void ParseTimescaleLibraryVersion_ReadsOnlyTheVersionedLoaderName(string fileName, string? expected)
        => Assert.Equal(expected, DarlingStoreUpgrade.ParseTimescaleLibraryVersion(fileName));

    /// <summary>
    /// #3908: a runtime carries the libraries of older TimescaleDB releases beside its own, so "the" versioned
    /// library is ambiguous and name order would answer 2.28.1 for a 2.30.1 runtime. Its version is the control
    /// file's default_version, read the same way from an extracted tree and from a zip; the library list is
    /// every version it carries BOTH libraries for.
    /// </summary>
    [Fact]
    public void TimescaleVersionReaders_UseTheControlFile_AndListEveryCarriedLibrary()
    {
        var root = Directory.CreateTempSubdirectory("darling-tsreaders-");
        try
        {
            var pgsql = Path.Combine(root.FullName, "pgsql");
            Directory.CreateDirectory(Path.Combine(pgsql, "bin"));
            Directory.CreateDirectory(Path.Combine(pgsql, "lib"));
            Directory.CreateDirectory(Path.Combine(pgsql, "share", "extension"));
            /* 2.27.0 has only its first library: without the TSL one a store cannot run compression or its
               continuous aggregates, so it is not a version the runtime can carry a store on. */
            foreach (var library in new[] { "timescaledb.dll", "timescaledb-2.27.0.dll", "timescaledb-2.28.1.dll", "timescaledb-tsl-2.28.1.dll", "timescaledb-2.30.1.dll", "timescaledb-tsl-2.30.1.dll" })
            {
                File.WriteAllText(Path.Combine(pgsql, "lib", library), "x");
            }

            File.WriteAllText(
                Path.Combine(pgsql, "share", "extension", "timescaledb.control"),
                "comment = 'Enables scalable inserts'\ndefault_version = '2.30.1'\nmodule_pathname = '$libdir/timescaledb'\n");

            var zip = Path.Combine(root.FullName, "pg-runtime.zip");
            ZipFile.CreateFromDirectory(pgsql, zip, CompressionLevel.NoCompression, includeBaseDirectory: true);

            Assert.Equal("2.30.1", DarlingStoreUpgrade.TryReadInstalledTimescaleVersion(Path.Combine(pgsql, "bin")));
            Assert.Equal("2.30.1", DarlingStoreUpgrade.TryReadZipTimescaleVersion(zip));
            Assert.Equal(new[] { "2.28.1", "2.30.1" }, DarlingStoreUpgrade.TryReadTimescaleLibraryVersions(pgsql));
            Assert.Equal(new[] { "2.28.1", "2.30.1" }, DarlingStoreUpgrade.TryReadZipTimescaleLibraryVersions(zip));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3908's seam, on real servers. A store created by 3.8.0's runtime (TimescaleDB 2.28.1) is started on the
    /// current bundle. An ordinary Npgsql session cannot move its extension: type loading is the session's
    /// first statement, which loads the old library, and TimescaleDB then refuses the ALTER (or, with no
    /// carried library, the load itself fails). The primitive, whose ALTER is the session's first statement,
    /// moves it. Also checked: the probe that loads nothing reads a database's version, or its absence, where an
    /// ordinary session cannot, the primitive refuses a database without the extension rather than calling that
    /// an answer, and repeating the ALTER is harmless. Started with the scheduler off, the way the quiesced step
    /// starts it.
    /// Gated on DARLING_TEST_PGRUNTIME_PREVIOUS and DARLING_TEST_PGRUNTIME, both set by the nightly.
    /// </summary>
    [Fact]
    public async Task UpdateTimescaleAsFirstStatement_MovesAStoreAnOrdinarySessionCannot_Gated()
    {
        var previousRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_PREVIOUS");
        var currentRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(previousRuntime) || string.IsNullOrWhiteSpace(currentRuntime),
            "Set DARLING_TEST_PGRUNTIME_PREVIOUS (the previous release's runtime) and DARLING_TEST_PGRUNTIME (the current bundle, extracted).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");

        var previousBin = Path.Combine(previousRuntime!, "pgsql", "bin");
        var currentBin = Path.Combine(currentRuntime!, "pgsql", "bin");
        var bundled = DarlingStoreUpgrade.TryReadInstalledTimescaleVersion(currentBin);
        var original = DarlingStoreUpgrade.TryReadInstalledTimescaleVersion(previousBin);
        Assert.NotNull(bundled);
        Assert.SkipWhen(string.Equals(bundled, original, StringComparison.Ordinal),
            $"Both runtimes ship TimescaleDB {bundled}, so there is no extension version to move.");

        var root = Directory.CreateTempSubdirectory("darling-tsfirst-");
        var dataDirectory = Path.Combine(root.FullName, "pg");
        var port = FindFreeTcpPort();
        var owner = $"Host=127.0.0.1;Port={port};Username=darling;Database=postgres;Pooling=false";
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));
        string? runningBin = null;
        try
        {
            var (initExit, initOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(previousBin, "initdb.exe"),
                $"-D \"{dataDirectory}\" -U darling -A trust -E UTF8 --locale=C --data-checksums",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(initExit == 0, $"initdb failed: {initOutput}");
            await File.AppendAllTextAsync(Path.Combine(dataDirectory, "postgresql.conf"), "\nshared_preload_libraries = 'timescaledb'\n", timeout.Token);

            /* The store as the previous release left it: TimescaleDB at that release's version, with data. */
            runningBin = await StartDirectAsync(previousBin, dataDirectory, port, quiesced: false, timeout.Token);
            await ExecuteOnAsync(owner, "CREATE DATABASE darling", timeout.Token);
            var store = owner.Replace("Database=postgres", "Database=darling", StringComparison.Ordinal);
            await ExecuteOnAsync(store, "CREATE EXTENSION timescaledb", timeout.Token);
            await ExecuteOnAsync(store, "CREATE TABLE m (t timestamptz NOT NULL, v int)", timeout.Token);
            await ExecuteOnAsync(store, "SELECT create_hypertable('m', by_range('t'))", timeout.Token);
            await ExecuteOnAsync(store, "INSERT INTO m SELECT now() - (g || ' min')::interval, g FROM generate_series(1, 5000) AS g", timeout.Token);
            Assert.Equal(original, await ScalarOnAsync(store, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", timeout.Token));
            await StopDirectAsync(previousBin, dataDirectory, timeout.Token);
            runningBin = null;

            /* The current bundle, quiesced. */
            runningBin = await StartDirectAsync(currentBin, dataDirectory, port, quiesced: true, timeout.Token);

            /* An ordinary session: its type-loading query is its first statement. Npgsql loads types once per
               connection string and this process already connected with the store's, so the control uses a
               string it has never seen, as the service's first store connection after a start would be. */
            var fresh = new NpgsqlConnectionStringBuilder(store) { ApplicationName = "ordinary-" + Guid.NewGuid().ToString("N") }.ConnectionString;
            var ordinary = await Assert.ThrowsAsync<PostgresException>(() => ExecuteOnAsync(fresh, "ALTER EXTENSION timescaledb UPDATE", timeout.Token));
            Assert.True(ordinary.SqlState is "58P01" or "0A000" or "55000",
                $"expected the loader to block an ordinary session's ALTER, got {ordinary.SqlState}: {ordinary.MessageText}");

            /* The probe the quiesced update decides with loads nothing, so it reads the old version where an
               ordinary session could not even open. */
            Assert.Equal(original, await DarlingStoreUpgrade.ReadTimescaleVersionUnloadedAsync(owner, "darling", timeout.Token));

            await DarlingStoreUpgrade.UpdateTimescaleAsFirstStatementAsync(owner, "darling", timeout.Token);
            Assert.Equal(bundled, await ScalarOnAsync(store, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", timeout.Token));
            Assert.Equal(bundled, await DarlingStoreUpgrade.ReadTimescaleVersionLoadedAsync(owner, "darling", timeout.Token));
            Assert.Equal("5000", await ScalarOnAsync(store, "SELECT count(*)::text FROM m", timeout.Token));

            /* A database without the extension is an error to the primitive, not an answer: the update script
               raises the same 42704 for a catalog object it did not find, so only the probe may say "absent". */
            Assert.Null(await DarlingStoreUpgrade.ReadTimescaleVersionUnloadedAsync(owner, "postgres", timeout.Token));
            var absent = await Assert.ThrowsAsync<PostgresException>(
                () => DarlingStoreUpgrade.UpdateTimescaleAsFirstStatementAsync(owner, "postgres", timeout.Token));
            Assert.Equal(PostgresErrorCodes.UndefinedObject, absent.SqlState);

            /* Repeated on a current store it is a NOTICE, not an error. */
            await DarlingStoreUpgrade.UpdateTimescaleAsFirstStatementAsync(owner, "darling", timeout.Token);
        }
        finally
        {
            if (runningBin is not null)
            {
                await StopDirectAsync(runningBin, dataDirectory, CancellationToken.None);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The carry step alone (#4253), on this machine, using the current runtime for both the "old" and
    /// "new" roles — the pairing the second live test in the issue's Ruled comment calls for, distinct from
    /// the gated real-pg_upgrade test below which needs a genuinely older runtime. Builds an old data
    /// directory, sets one real value with ALTER SYSTEM, and hand-appends one setting no binaries would
    /// accept (an unqualified unknown name — ALTER SYSTEM itself refuses that outright, so the only way to
    /// reproduce a stale cross-major setting is to write the line directly, the way pg_upgrade's untouched
    /// old file would carry one). Runs <see cref="DarlingStoreUpgrade.CarryAutoConfAsync"/> against a
    /// separate new data directory, then starts THAT cluster for real and reads the good value back with
    /// SHOW: the rejected setting cost nothing but itself, and the store starts.
    /// </summary>
    [Fact]
    public async Task CarryAutoConfAsync_CarriesAGoodSetting_LeavesOutOneTheNewBinariesReject_AndTheStoreStarts()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        var bin = Path.Combine(runtimeRoot!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-autoconf-carry-");
        var oldDataDirectory = Path.Combine(root.FullName, "old");
        var newDataDirectory = Path.Combine(root.FullName, "new");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        string? runningDataDirectory = null;

        try
        {
            var (oldInitExit, oldInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{oldDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(oldInitExit == 0, $"initdb (old) failed: {oldInitOutput}");

            var oldPort = FindFreeTcpPort();
            runningDataDirectory = oldDataDirectory;
            await StartDirectAsync(bin, oldDataDirectory, oldPort, quiesced: false, timeout.Token);
            var oldOwner = $"Host=127.0.0.1;Port={oldPort};Username=darling;Database=postgres;Pooling=false";
            await ExecuteOnAsync(oldOwner, "ALTER SYSTEM SET work_mem = '199MB'", timeout.Token);
            await StopDirectAsync(bin, oldDataDirectory, timeout.Token);
            runningDataDirectory = null;

            await File.AppendAllTextAsync(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
                "darling_4253_unknown_setting = 'on'\n", timeout.Token);

            var (newInitExit, newInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{newDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(newInitExit == 0, $"initdb (new) failed: {newInitOutput}");

            var log = new CapturingLogger();
            var result = await new DarlingStoreUpgrade(log).CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, bin, timeout.Token);

            Assert.Contains("work_mem", result.CarriedNames);
            Assert.Contains("darling_4253_unknown_setting", result.RejectedNames);

            var logText = log.ToString();
            Assert.Contains("Carried work_mem", logText);
            Assert.Contains("NOT carried: darling_4253_unknown_setting", logText);
            /* Round-1 security review, #4280 Medium 1: a carried setting's value never reaches any log line. */
            Assert.DoesNotContain("199MB", logText, StringComparison.Ordinal);

            var preUpgradeCopy = Path.Combine(root.FullName, DarlingStoreUpgrade.PreUpgradeAutoConfFileName);
            Assert.True(File.Exists(preUpgradeCopy), $"expected the pre-upgrade file at {preUpgradeCopy}");
            Assert.Contains("darling_4253_unknown_setting", await File.ReadAllTextAsync(preUpgradeCopy, timeout.Token));

            var newAutoConf = await File.ReadAllTextAsync(Path.Combine(newDataDirectory, "postgresql.auto.conf"), timeout.Token);
            Assert.DoesNotContain("darling_4253_unknown_setting", newAutoConf);

            var newPort = FindFreeTcpPort();
            runningDataDirectory = newDataDirectory;
            await StartDirectAsync(bin, newDataDirectory, newPort, quiesced: false, timeout.Token);
            var newOwner = $"Host=127.0.0.1;Port={newPort};Username=darling;Database=postgres;Pooling=false";
            Assert.Equal("199MB", await ScalarOnAsync(newOwner, "SHOW work_mem", timeout.Token));
        }
        finally
        {
            if (runningDataDirectory is not null)
            {
                await StopDirectAsync(bin, runningDataDirectory, CancellationToken.None);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Item 3 (#4280), live — the round-1 security review's Medium 2 case: a carried <c>ssl_ca_file</c>
    /// naming a missing file passes its own per-setting <c>-C</c> probe (that check only reads the config,
    /// never touches SSL/certificate setup), so an SSL-off trial starts fine and wrongly keeps it — a
    /// landmine for the operator's next SSL-on start. <see cref="DarlingStoreUpgrade.CarryAutoConfAsync(string, string, string, CancellationToken, Func{string})"/>'s
    /// <c>sslServerOptions</c> parameter rides the SAME trial start, so calling it with a real SSL delegate
    /// here makes the combined trial actually turn SSL on and catch it. The cert/key come from the product's
    /// own <see cref="DarlingManagedPostgres.EnsureServerCertificate"/>, not a hand-made file, through
    /// <see cref="DarlingManagedPostgres.BuildSslServerOptions"/> — the same pairing a real store's network
    /// exposure uses.
    /// </summary>
    [Fact]
    public async Task CarryAutoConfAsync_CarriedSslCaFileNamesAMissingFile_SslOnTrialDropsIt_NeverLogsItsValue()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime and cert key hardening are Windows-only.");
        var bin = Path.Combine(runtimeRoot!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-autoconf-sslca-");
        var oldDataDirectory = Path.Combine(root.FullName, "old");
        var newDataDirectory = Path.Combine(root.FullName, "new");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        string? runningDataDirectory = null;
        var missingCaPath = Path.Combine(root.FullName, "does-not-exist-ca.pem").Replace('\\', '/');

        try
        {
            var (oldInitExit, oldInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{oldDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(oldInitExit == 0, $"initdb (old) failed: {oldInitOutput}");

            var oldPort = FindFreeTcpPort();
            runningDataDirectory = oldDataDirectory;
            await StartDirectAsync(bin, oldDataDirectory, oldPort, quiesced: false, timeout.Token);
            var oldOwner = $"Host=127.0.0.1;Port={oldPort};Username=darling;Database=postgres;Pooling=false";
            await ExecuteOnAsync(oldOwner, $"ALTER SYSTEM SET ssl_ca_file = '{missingCaPath}'", timeout.Token);
            await StopDirectAsync(bin, oldDataDirectory, timeout.Token);
            runningDataDirectory = null;

            var (newInitExit, newInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{newDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(newInitExit == 0, $"initdb (new) failed: {newInitOutput}");

            /* The product's own certificate code (#4280 item 3), not a hand-made cert file — the same call
               BuildNetworkPlan makes for a real store's exposure. */
            var certPath = Path.Combine(root.FullName, DarlingManagedPostgres.ServerCertFileName);
            var keyPath = Path.Combine(root.FullName, DarlingManagedPostgres.ServerKeyFileName);
            var certConfig = new PostgresConfig { Managed = true, Port = oldPort, DataDirectory = oldDataDirectory };
            new DarlingManagedPostgres(certConfig, NullLogger.Instance).EnsureServerCertificate(IPAddress.Parse("127.0.0.1"), certPath, keyPath);

            var log = new CapturingLogger();
            var result = await new DarlingStoreUpgrade(log).CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, bin, timeout.Token,
                sslServerOptions: () => DarlingManagedPostgres.BuildSslServerOptions(certPath, keyPath));

            /* The per-setting -C probe alone cannot see this: ssl_ca_file only fails once the combined trial
               actually turns SSL on, which drops everything that trial carried (there was only this one
               setting), never a partial "carried but flagged" state. */
            Assert.Empty(result.CarriedNames);
            Assert.Contains("ssl_ca_file", result.RejectedNames);

            var logText = log.ToString();
            Assert.Contains("Dropped: ssl_ca_file", logText, StringComparison.Ordinal);
            /* Round-1 security review, #4280 Medium 1: the dropped-name log line names the setting, never
               the missing path it pointed at. */
            Assert.DoesNotContain(missingCaPath, logText, StringComparison.Ordinal);

            var newAutoConf = await File.ReadAllTextAsync(Path.Combine(newDataDirectory, "postgresql.auto.conf"), timeout.Token);
            Assert.DoesNotContain("ssl_ca_file", newAutoConf, StringComparison.Ordinal);

            /* The store starts (SSL off — proving the drop, not just a returned result object, actually
               left a bootable cluster), and ssl_ca_file is back at its unset default. */
            var newPort = FindFreeTcpPort();
            runningDataDirectory = newDataDirectory;
            await StartDirectAsync(bin, newDataDirectory, newPort, quiesced: false, timeout.Token);
            var newOwner = $"Host=127.0.0.1;Port={newPort};Username=darling;Database=postgres;Pooling=false";
            Assert.Equal(string.Empty, await ScalarOnAsync(newOwner, "SHOW ssl_ca_file", timeout.Token));
        }
        finally
        {
            if (runningDataDirectory is not null)
            {
                await StopDirectAsync(bin, runningDataDirectory, CancellationToken.None);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Medium 1, live: the trial's real start (round-2 review) used to run on the store's own configured
    /// port, on the assumption that port is free because the old cluster on it was already stopped. Proves
    /// that assumption no longer matters — a listener held on that exact port for the whole call, simulating
    /// anything (a slow TIME_WAIT teardown, an unrelated process) still sitting on it, must not cost the
    /// trial: it starts on its own private port from <see cref="DarlingStoreUpgrade.FindFreeLoopbackPort"/>,
    /// which the OS cannot also hand out for the held listener's port.
    /// </summary>
    [Fact]
    public async Task CarryAutoConfAsync_TheStoreConfiguredPortIsStillHeld_TheTrialStartsOnItsOwnPrivatePort()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        var bin = Path.Combine(runtimeRoot!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-autoconf-portheld-");
        var oldDataDirectory = Path.Combine(root.FullName, "old");
        var newDataDirectory = Path.Combine(root.FullName, "new");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        string? runningDataDirectory = null;
        TcpListener? heldListener = null;

        try
        {
            var (oldInitExit, oldInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{oldDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(oldInitExit == 0, $"initdb (old) failed: {oldInitOutput}");

            var configuredPort = FindFreeTcpPort();
            runningDataDirectory = oldDataDirectory;
            await StartDirectAsync(bin, oldDataDirectory, configuredPort, quiesced: false, timeout.Token);
            var oldOwner = $"Host=127.0.0.1;Port={configuredPort};Username=darling;Database=postgres;Pooling=false";
            await ExecuteOnAsync(oldOwner, "ALTER SYSTEM SET work_mem = '199MB'", timeout.Token);
            await StopDirectAsync(bin, oldDataDirectory, timeout.Token);
            runningDataDirectory = null;

            /* Stands in for "the configured port is not actually free at this step" — held on the exact
               port the old cluster just vacated, which is the port a pre-fix trial would have reused. */
            heldListener = new TcpListener(IPAddress.Loopback, configuredPort);
            heldListener.Start();

            var (newInitExit, newInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{newDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(newInitExit == 0, $"initdb (new) failed: {newInitOutput}");

            var log = new CapturingLogger();
            var result = await new DarlingStoreUpgrade(log).CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, bin, timeout.Token);

            Assert.Contains("work_mem", result.CarriedNames);

            heldListener.Stop();
            heldListener = null;

            var newPort = FindFreeTcpPort();
            runningDataDirectory = newDataDirectory;
            await StartDirectAsync(bin, newDataDirectory, newPort, quiesced: false, timeout.Token);
            var newOwner = $"Host=127.0.0.1;Port={newPort};Username=darling;Database=postgres;Pooling=false";
            Assert.Equal("199MB", await ScalarOnAsync(newOwner, "SHOW work_mem", timeout.Token));
        }
        finally
        {
            heldListener?.Stop();

            if (runningDataDirectory is not null)
            {
                await StopDirectAsync(bin, runningDataDirectory, CancellationToken.None);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The failure #4253's own fix exists for: a per-setting probe THROWS (here, <see cref="TimeoutException"/>
    /// on the 2nd of 3 settings) instead of returning a bad exit code. Before the fix the loop below exited
    /// with postgresql.auto.conf holding whatever its LAST write left there — one candidate line nothing had
    /// verified — and step 8's post-commit handler finishes the upgrade regardless, so that unverified line is
    /// what the next start would read. Uses the probe seam so no real postgres.exe or 30 s wait is needed;
    /// <see cref="CarryAutoConfAsync_CarriesAGoodSetting_LeavesOutOneTheNewBinariesReject_AndTheStoreStarts"/>
    /// above already covers the real-binaries path.
    /// </summary>
    [Fact]
    public async Task CarryAutoConfAsync_ProbeThrowsTimeout_ResetsToHeaderAndRethrows()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-throw-");
        try
        {
            var (oldDataDirectory, newDataDirectory, newAutoConfPath) = SetUpAutoConfCarryDirectories(root.FullName);

            var probeCalls = 0;
            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
            {
                probeCalls++;
                if (probeCalls == 2)
                {
                    throw new TimeoutException("forced timeout on the 2nd probe, for the test");
                }

                return Task.FromResult((0, string.Empty));
            }

            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());
            await Assert.ThrowsAsync<TimeoutException>(() => upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, CancellationToken.None));

            Assert.Equal(AutoConfHeaderOnly, await File.ReadAllTextAsync(newAutoConfPath));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Same defect as <see cref="CarryAutoConfAsync_ProbeThrowsTimeout_ResetsToHeaderAndRethrows"/>, but the
    /// throw is the other shape <see cref="DarlingManagedPostgres.RunToolAsync"/> produces: the caller's own
    /// token is cancelled (a service stop mid-upgrade) and the probe rethrows
    /// <see cref="OperationCanceledException"/>. CancellationToken.None matters here specifically: the
    /// safety write that resets the file must not itself be blocked by the very token whose cancellation
    /// caused the reset to be needed.
    /// </summary>
    [Fact]
    public async Task CarryAutoConfAsync_ProbeThrowsAfterCancellation_ResetsToHeaderAndRethrows()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-cancel-");
        try
        {
            var (oldDataDirectory, newDataDirectory, newAutoConfPath) = SetUpAutoConfCarryDirectories(root.FullName);

            using var cts = new CancellationTokenSource();
            var probeCalls = 0;
            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
            {
                probeCalls++;
                if (probeCalls == 2)
                {
                    cts.Cancel();
                    token.ThrowIfCancellationRequested();
                }

                return Task.FromResult((0, string.Empty));
            }

            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());
            await Assert.ThrowsAsync<OperationCanceledException>(() => upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, cts.Token));

            Assert.Equal(AutoConfHeaderOnly, await File.ReadAllTextAsync(newAutoConfPath, CancellationToken.None));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Item 0b, live: a cancellation raised while the trial's REAL cluster start is in flight must propagate
    /// as <see cref="OperationCanceledException"/>. Before the fix, the trial's own catch swallowed it into
    /// <c>started = false</c> like any other failed start, driving the header-only retry — which fails the
    /// same way under the same already-cancelled token — and then threw the fixed "would not start even with
    /// an empty postgresql.auto.conf" message, wrong for a service stop unrelated to the carried settings. The
    /// probe is faked (always exits 0) so the trial is reached without a real "postgres -C" call; the trial's
    /// own start is real and live, so cancellation lands while <c>pg_ctl -w</c> is actually waiting, the same
    /// as a real service stop would (#4280 round-2 part 2, item 0b).
    /// </summary>
    [Fact]
    public async Task CarryAutoConfAsync_CancelledDuringTheTrialStart_PropagatesCancellation_NotTheFixedMessage()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        var bin = Path.Combine(runtimeRoot!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-autoconf-trialcancel-");
        var oldDataDirectory = Path.Combine(root.FullName, "old");
        var newDataDirectory = Path.Combine(root.FullName, "new");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);

        try
        {
            Directory.CreateDirectory(oldDataDirectory);
            File.WriteAllText(Path.Combine(oldDataDirectory, "postgresql.auto.conf"), "work_mem = '64MB'\n");

            var (newInitExit, newInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{newDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(newInitExit == 0, $"initdb (new) failed: {newInitOutput}");

            /* Never touches postgres.exe — only the trial's own real cluster start does. */
            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan probeTimeout, CancellationToken token)
                => Task.FromResult((0, string.Empty));

            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());
            var carryTask = upgrade.CarryAutoConfAsync(oldDataDirectory, newDataDirectory, bin, Probe, cts.Token);

            /* The trial writes its marker synchronously, before it starts the real cluster (#3908) — as soon
               as it appears, the awaited StartClusterAsync call is in flight, so cancelling now lands there. */
            var marker = Path.Combine(newDataDirectory, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName);
            var deadline = DateTime.UtcNow.AddSeconds(30);
            while (!File.Exists(marker) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(10, timeout.Token);
            }

            Assert.True(File.Exists(marker), "The trial never wrote its marker within 30s.");
            cts.Cancel();

            await Assert.ThrowsAsync<OperationCanceledException>(() => carryTask);
            Assert.Equal(
                AutoConfHeaderOnly,
                await File.ReadAllTextAsync(Path.Combine(newDataDirectory, "postgresql.auto.conf"), CancellationToken.None));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Items 2 and 4 (#4280 round-2 part 2): the marker's states parse correctly and hold names never values,
    /// and <see cref="DarlingStoreUpgrade.ResetAutoConfCarryAsync"/> — the ONE routine both item 2's real-start
    /// fallback and item 4's leftover-"carrying" recovery call, rather than reset-and-drop a new way each —
    /// resets to header-only, logs names only, and deletes the marker. Uses the static/instance helpers
    /// directly (the "static helper" the brief's test plan allows) rather than a full EnsureRunningAsync
    /// bootstrap, which needs a second pg-runtime fixture (DARLING_TEST_PGRUNTIME_OLD/NEWZIP) this lane's rig
    /// does not have.
    /// </summary>
    [Fact]
    public async Task AutoConfCarryMarker_StatesAndReset_HoldNamesNeverValues_AndDeleteOnAGoodStart()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-marker-");
        try
        {
            var dataDirectory = root.FullName;
            var autoConfPath = Path.Combine(dataDirectory, "postgresql.auto.conf");
            File.WriteAllText(autoConfPath, "work_mem = '256MB'\nshared_buffers = '999MB'\n");

            var markerPath = Path.Combine(dataDirectory, DarlingStoreUpgrade.AutoConfCarryStateMarkerFileName);
            File.WriteAllText(markerPath, "carrying\nwork_mem\nshared_buffers\n");

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);

            var carrying = upgrade.TryReadAutoConfCarryMarker(dataDirectory);
            Assert.NotNull(carrying);
            Assert.Equal(DarlingStoreUpgrade.AutoConfCarryStateCarrying, carrying!.Value.State);
            Assert.Equal(new[] { "work_mem", "shared_buffers" }, carrying.Value.Names);

            Assert.True(
                await upgrade.ResetAutoConfCarryAsync(dataDirectory, carrying.Value, "test reason, never a setting value"),
                "a good header-only write must return true (#4280 item 2).");

            Assert.Equal(AutoConfHeaderOnly, await File.ReadAllTextAsync(autoConfPath));
            Assert.False(File.Exists(markerPath), "ResetAutoConfCarryAsync must delete the marker.");

            var logged = log.ToString();
            Assert.Contains("work_mem", logged);
            Assert.Contains("shared_buffers", logged);
            Assert.DoesNotContain("256MB", logged);
            Assert.DoesNotContain("999MB", logged);

            /* trial-passed parses too, and TryDeleteAutoConfCarryMarker — what EnsureRunningAsync calls after
               a good real start, the already-running path included — removes it without touching auto.conf. */
            File.WriteAllText(markerPath, "trial-passed\nwork_mem\n");
            var passed = upgrade.TryReadAutoConfCarryMarker(dataDirectory);
            Assert.Equal(DarlingStoreUpgrade.AutoConfCarryStateTrialPassed, passed!.Value.State);
            DarlingStoreUpgrade.TryDeleteAutoConfCarryMarker(dataDirectory);
            Assert.False(File.Exists(markerPath));

            /* A missing marker is a no-op read (#4280 items 2/4) — never blocks a start. */
            Assert.Null(upgrade.TryReadAutoConfCarryMarker(dataDirectory));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Item 2 (#4280): when the header-only write itself fails (here, a read-only auto.conf), the reset must
    /// return false and must not claim success — no "NOT carried ... reset to header-only" warning, no marker
    /// deletion, no file change. Before this fix, the write's own catch only logged a warning and execution
    /// fell through to log the drop and delete the marker anyway, so the next start ran the still-carried,
    /// unverified settings with no marker left to retry the reset.
    /// </summary>
    [Fact]
    public async Task ResetAutoConfCarryAsync_HeaderOnlyWriteFails_ReturnsFalse_KeepsMarkerAndFile()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-resetfail-");
        var autoConfPath = Path.Combine(root.FullName, "postgresql.auto.conf");
        var madeReadOnly = false;
        try
        {
            const string carriedText = "work_mem = '256MB'\n";
            File.WriteAllText(autoConfPath, carriedText);

            var markerPath = Path.Combine(root.FullName, DarlingStoreUpgrade.AutoConfCarryStateMarkerFileName);
            File.WriteAllText(markerPath, "trial-passed\nwork_mem\n");

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            var marker = upgrade.TryReadAutoConfCarryMarker(root.FullName)!.Value;

            File.SetAttributes(autoConfPath, FileAttributes.ReadOnly);
            madeReadOnly = true;

            var reset = await upgrade.ResetAutoConfCarryAsync(root.FullName, marker, "test reason, never a setting value");

            Assert.False(reset, "a failed header-only write must return false.");
            Assert.True(File.Exists(markerPath), "a failed reset must keep the marker so the next start retries it.");
            Assert.Equal(carriedText, await File.ReadAllTextAsync(autoConfPath));

            var logged = log.ToString();
            Assert.DoesNotContain("reset to header-only", logged, StringComparison.Ordinal);
        }
        finally
        {
            if (madeReadOnly)
            {
                File.SetAttributes(autoConfPath, FileAttributes.Normal);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Matches the private <c>header</c> constant in <see cref="DarlingStoreUpgrade.CarryAutoConfAsync"/>
    /// exactly, for the two tests above to assert the reset-on-exception file state against.</summary>
    private const string AutoConfHeaderOnly =
        "# Do not edit this file manually!\n# It will be overwritten by the ALTER SYSTEM command.\n";

    /// <summary>
    /// An old data directory with three valid, ALTER-SYSTEM-shaped settings, and an empty new data directory
    /// — enough for <see cref="DarlingStoreUpgrade.CarryAutoConfAsync"/> to run its probe loop against without
    /// a real postgres.exe. Three settings so a throw on the 2nd leaves one already carried, one never
    /// reached, and (before the fix) one half-written candidate line on disk.
    /// </summary>
    private static (string OldDataDirectory, string NewDataDirectory, string NewAutoConfPath) SetUpAutoConfCarryDirectories(string root)
    {
        var oldDataDirectory = Path.Combine(root, "old");
        var newDataDirectory = Path.Combine(root, "new");
        Directory.CreateDirectory(oldDataDirectory);
        Directory.CreateDirectory(newDataDirectory);

        File.WriteAllText(
            Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
            "work_mem = '64MB'\nshared_buffers = '256MB'\nmax_connections = '250'\n");

        return (oldDataDirectory, newDataDirectory, Path.Combine(newDataDirectory, "postgresql.auto.conf"));
    }

    /* ---------------- round-1 security review, #4280 ---------------- */

    /// <summary>Medium 1: a rejected setting whose name looks like it may hold a credential gets no reason
    /// logged, and never its value — PostgreSQL's own reject reason for an out-of-range value often repeats
    /// the offending value verbatim.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_RejectedSecretNamedSetting_NeverLogsItsValueOrReason()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-secretreject-");
        try
        {
            var oldDataDirectory = Path.Combine(root.FullName, "old");
            var newDataDirectory = Path.Combine(root.FullName, "new");
            Directory.CreateDirectory(oldDataDirectory);
            Directory.CreateDirectory(newDataDirectory);

            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
                "primary_conninfo = 'host=x password=hunter2'\n");

            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
                => Task.FromResult((1, "invalid value for parameter \"primary_conninfo\": \"host=x password=hunter2\""));

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            var result = await upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, CancellationToken.None);

            Assert.Empty(result.CarriedNames);
            Assert.Contains("primary_conninfo", result.RejectedNames);

            var logText = log.ToString();
            Assert.Contains("primary_conninfo", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("host=x", logText, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Round-2 Q3 hardening: an extension-qualified name withholds the reason even when it also
    /// matches one of the new secret fragments ("auth", "token") — PostgreSQL's own reject reason for an
    /// out-of-range value often repeats the offending value verbatim, and an unrecognized extension's own
    /// wording is not something this class can vouch for either way.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_RejectedDotQualifiedSecretNamedSetting_NeverLogsItsValueOrReason()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-dotsecretreject-");
        try
        {
            var oldDataDirectory = Path.Combine(root.FullName, "old");
            var newDataDirectory = Path.Combine(root.FullName, "new");
            Directory.CreateDirectory(oldDataDirectory);
            Directory.CreateDirectory(newDataDirectory);

            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
                "myext.auth_token = 'topsecrettoken'\n");

            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
                => Task.FromResult((1, "invalid value for parameter \"myext.auth_token\": \"topsecrettoken\""));

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            var result = await upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, CancellationToken.None);

            Assert.Empty(result.CarriedNames);
            Assert.Contains("myext.auth_token", result.RejectedNames);

            var logText = log.ToString();
            Assert.Contains("myext.auth_token", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("topsecrettoken", logText, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Medium 1: a CARRIED setting's log line never repeats its value either, secret-looking name or
    /// not — only the name and a pointer to the pre-upgrade copy. "unused-bin-dir" has no real pg_ctl.exe, so
    /// item 3's retry (also on "unused-bin-dir") fails too — unrelated to the settings, which is exactly what
    /// the new throw says. This test cares that the throw, the log, and the file all stay clean of the value
    /// either way.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_CarriedSecretNamedSetting_NeverLogsItsValue()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-secretcarry-");
        try
        {
            var oldDataDirectory = Path.Combine(root.FullName, "old");
            var newDataDirectory = Path.Combine(root.FullName, "new");
            Directory.CreateDirectory(oldDataDirectory);
            Directory.CreateDirectory(newDataDirectory);

            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
                "primary_conninfo = 'host=x password=hunter2'\n");

            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
                => Task.FromResult((0, string.Empty));

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, CancellationToken.None));

            Assert.Contains("pg.log", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2", ex.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("host=x", ex.Message, StringComparison.Ordinal);

            var logText = log.ToString();
            Assert.Contains("Carried primary_conninfo", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("host=x", logText, StringComparison.Ordinal);

            var newAutoConf = await File.ReadAllTextAsync(Path.Combine(newDataDirectory, "postgresql.auto.conf"));
            Assert.Equal(AutoConfHeaderOnly, newAutoConf);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Medium 1 (the ":2831" finding): a skipped invalid-name line logs a line number, never its
    /// text — with the '=' optional, the text before this parser's name/value boundary can actually be part
    /// of the value.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_SkippedInvalidNameLine_LogsALineNumberNotTheText()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-skipname-");
        try
        {
            var oldDataDirectory = Path.Combine(root.FullName, "old");
            var newDataDirectory = Path.Combine(root.FullName, "new");
            Directory.CreateDirectory(oldDataDirectory);
            Directory.CreateDirectory(newDataDirectory);

            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
                "bad\"name = 'hunter2fragment'\n");

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            var result = await upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir",
                (exePath, arguments, timeout, token) => Task.FromResult((0, string.Empty)),
                CancellationToken.None);

            Assert.Empty(result.CarriedNames);
            Assert.Empty(result.RejectedNames);

            var logText = log.ToString();
            Assert.Contains("(line(s) 1)", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("hunter2fragment", logText, StringComparison.Ordinal);
            Assert.DoesNotContain("bad\"name", logText, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Low 1: when the catch's reset write ALSO fails, the fallback File.Delete is tried, and when
    /// THAT also fails the thrown exception names the file, keeps the original as InnerException, AND folds the
    /// original's own message into its text — so a caller or a log that only shows the top-level message still
    /// sees why the carry itself did not finish, not just why the two cleanup attempts after it also failed.
    /// Here a read-only postgresql.auto.conf blocks both the reset write and the delete deterministically.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_ResetAndDeleteBothFail_ThrowsNamingTheFile_WithTheOriginalAsInnerException()
    {
        var root = Directory.CreateTempSubdirectory("darling-autoconf-doublefail-");
        try
        {
            var (oldDataDirectory, newDataDirectory, newAutoConfPath) = SetUpAutoConfCarryDirectories(root.FullName);

            var probeCalls = 0;
            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
            {
                probeCalls++;
                if (probeCalls == 2)
                {
                    /* Read-only blocks both the reset write and File.Delete deterministically, the same as a
                       locked or access-denied file would. */
                    File.SetAttributes(newAutoConfPath, FileAttributes.ReadOnly);
                    throw new TimeoutException("forced timeout on the 2nd probe, for the test");
                }

                return Task.FromResult((0, string.Empty));
            }

            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());
            var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, CancellationToken.None));

            Assert.Contains(newAutoConfPath, ex.Message, StringComparison.Ordinal);
            Assert.IsType<TimeoutException>(ex.InnerException);
            Assert.Contains(ex.InnerException!.Message, ex.Message, StringComparison.Ordinal);
        }
        finally
        {
            try
            {
                File.SetAttributes(Path.Combine(root.FullName, "new", "postgresql.auto.conf"), FileAttributes.Normal);
            }
            catch (Exception)
            {
                /* best-effort cleanup */
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Medium 2, live: "ssl = on" with no certificate files passes "postgres -C ssl" (which exits
    /// right after reading the config files, before SSL setup) but stops the very next real start. The
    /// belt-and-braces check must catch that with a real start/stop, not another -C probe: postgresql.auto.conf
    /// ends at the header only, and the cluster still starts.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_ALineThatPassesDashCButFailsARealStart_EndsHeaderOnly_AndTheStoreStarts()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        var bin = Path.Combine(runtimeRoot!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-autoconf-sslstart-");
        var oldDataDirectory = Path.Combine(root.FullName, "old");
        var newDataDirectory = Path.Combine(root.FullName, "new");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        string? runningDataDirectory = null;

        try
        {
            var (oldInitExit, oldInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{oldDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(oldInitExit == 0, $"initdb (old) failed: {oldInitOutput}");

            /* Hand-appended, not ALTER SYSTEM: "ssl = on" with no server.crt/server.key configured is exactly
               the review's example, and it is syntactically valid so ALTER SYSTEM would accept it too. */
            await File.AppendAllTextAsync(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"), "ssl = 'on'\n", timeout.Token);

            var (newInitExit, newInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{newDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(newInitExit == 0, $"initdb (new) failed: {newInitOutput}");

            var log = new CapturingLogger();
            var result = await new DarlingStoreUpgrade(log).CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, bin, timeout.Token);

            Assert.Empty(result.CarriedNames);
            Assert.Contains("ssl", result.RejectedNames);

            var newAutoConf = await File.ReadAllTextAsync(Path.Combine(newDataDirectory, "postgresql.auto.conf"), timeout.Token);
            Assert.Equal(AutoConfHeaderOnly, newAutoConf);

            var newPort = FindFreeTcpPort();
            runningDataDirectory = newDataDirectory;
            await StartDirectAsync(bin, newDataDirectory, newPort, quiesced: false, timeout.Token);
        }
        finally
        {
            if (runningDataDirectory is not null)
            {
                await StopDirectAsync(bin, runningDataDirectory, CancellationToken.None);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>Low 1, live: PostgreSQL treats postgresql.auto.conf as optional, so the File.Delete fallback
    /// the reset's own catch falls back to is safe to reach for — proven here by removing the file the normal
    /// reset (a probe throw mid-loop) leaves behind and confirming a real cluster still starts with none at
    /// all. <see cref="CarryAutoConfAsync_ResetAndDeleteBothFail_ThrowsNamingTheFile_WithTheOriginalAsInnerException"/>
    /// pins the code path that reaches File.Delete itself.</summary>
    [Fact]
    public async Task CarryAutoConfAsync_ProbeThrows_TheNewClusterStarts_WithNoAutoConfFile()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        var bin = Path.Combine(runtimeRoot!, "pgsql", "bin");
        Assert.SkipUnless(File.Exists(Path.Combine(bin, "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-autoconf-nofilestart-");
        var oldDataDirectory = Path.Combine(root.FullName, "old");
        var newDataDirectory = Path.Combine(root.FullName, "new");
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
        string? runningDataDirectory = null;

        try
        {
            var (newInitExit, newInitOutput) = await DarlingManagedPostgres.RunToolAsync(
                Path.Combine(bin, "initdb.exe"),
                $"-D \"{newDataDirectory}\" -U darling -A trust -E UTF8 --locale=C",
                TimeSpan.FromMinutes(3), timeout.Token);
            Assert.True(newInitExit == 0, $"initdb (new) failed: {newInitOutput}");

            Directory.CreateDirectory(oldDataDirectory);
            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.auto.conf"),
                "work_mem = '64MB'\nshared_buffers = '256MB'\n");

            var probeCalls = 0;
            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan probeTimeout, CancellationToken token)
            {
                probeCalls++;
                if (probeCalls == 2)
                {
                    throw new TimeoutException("forced timeout on the 2nd probe, for the test");
                }

                return Task.FromResult((0, string.Empty));
            }

            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());
            await Assert.ThrowsAsync<TimeoutException>(() => upgrade.CarryAutoConfAsync(
                oldDataDirectory, newDataDirectory, bin, Probe, timeout.Token));

            File.Delete(Path.Combine(newDataDirectory, "postgresql.auto.conf"));

            var newPort = FindFreeTcpPort();
            runningDataDirectory = newDataDirectory;
            await StartDirectAsync(bin, newDataDirectory, newPort, quiesced: false, timeout.Token);
        }
        finally
        {
            if (runningDataDirectory is not null)
            {
                await StopDirectAsync(bin, runningDataDirectory, CancellationToken.None);
            }

            TryDeleteTree(root.FullName);
        }
    }

    /* ---------------- #4358: CarryOperatorConfLinesAsync ---------------- */

    /// <summary>
    /// Ordering pin (#4358): the carried operator lines land AFTER the legacy blocks
    /// <c>context.AppendManagedConf</c> already wrote into the new cluster's <c>postgresql.conf</c> (the
    /// <c>baseline</c> this method reads), so where the same key is set by both, the operator's below-include
    /// value wins — matching <see cref="ManagedConfMigration.Rewrite"/>'s rule 3 for the same-major case.
    /// Parsed with <see cref="DarlingManagedPostgres.ParseConfText"/>, never a raw string/position check, so
    /// this pin cannot be satisfied by an accidental substring match.
    /// </summary>
    [Fact]
    public async Task CarryOperatorConfLinesAsync_CarriesAfterTheLegacyBlocks_OperatorValueWins()
    {
        var root = Directory.CreateTempSubdirectory("darling-opconf-order-");
        try
        {
            var oldDataDirectory = Path.Combine(root.FullName, "old");
            var newDataDirectory = Path.Combine(root.FullName, "new");
            Directory.CreateDirectory(oldDataDirectory);
            Directory.CreateDirectory(newDataDirectory);

            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.conf"),
                "max_connections = 200\n" +
                "include 'darling-managed.conf'\n" +
                "# operator settings kept from the previous postgresql.conf (#4215)\n" +
                "log_min_duration_statement = 999\n");

            var newConfPath = Path.Combine(newDataDirectory, "postgresql.conf");
            File.WriteAllText(
                newConfPath,
                "include 'darling-managed.conf'\n" +
                "# legacy settings kept from the previous postgresql.conf (#4215)\n" +
                "log_min_duration_statement = 111\n");

            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());
            var result = await upgrade.CarryOperatorConfLinesAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir",
                (exePath, arguments, timeout, token) => Task.FromResult((0, string.Empty)),
                CancellationToken.None);

            /* 2 carried: the comment line (unconditionally carried) plus the one probeable setting. */
            Assert.Equal(2, result.CarriedCount);
            Assert.Equal(0, result.RejectedCount);

            var finalConf = await File.ReadAllTextAsync(newConfPath);
            var legacyIndex = finalConf.IndexOf("legacy settings", StringComparison.Ordinal);
            var movedIndex = finalConf.IndexOf(ManagedConfMigration.MovedOperatorLinesComment, StringComparison.Ordinal);
            Assert.True(legacyIndex >= 0 && movedIndex > legacyIndex,
                "expected the operator's moved-lines block to appear AFTER the legacy block");

            var values = DarlingManagedPostgres.ParseConfText(finalConf)
                .Where(a => string.Equals(a.Name, "log_min_duration_statement", StringComparison.OrdinalIgnoreCase))
                .ToList();
            Assert.Equal("999", values.Last().Value);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// Never-fatal pin (#4358): one operator line the new binaries reject (via the probe delegate — no real
    /// postgres.exe needed) is skipped and logged as not carried, while the OTHER candidate lines are still
    /// carried, and the method returns normally rather than throwing — the same "one bad line costs only
    /// itself" posture <see cref="DarlingStoreUpgrade.CarryAutoConfAsync"/> gives auto.conf settings.
    /// </summary>
    [Fact]
    public async Task CarryOperatorConfLinesAsync_RejectedLine_IsSkippedAndLogged_OthersStillCarried_NeverFatal()
    {
        var root = Directory.CreateTempSubdirectory("darling-opconf-reject-");
        try
        {
            var oldDataDirectory = Path.Combine(root.FullName, "old");
            var newDataDirectory = Path.Combine(root.FullName, "new");
            Directory.CreateDirectory(oldDataDirectory);
            Directory.CreateDirectory(newDataDirectory);

            File.WriteAllText(
                Path.Combine(oldDataDirectory, "postgresql.conf"),
                "max_connections = 200\n" +
                "include 'darling-managed.conf'\n" +
                "log_min_duration_statement = 250\n" +
                "darling_4358_unknown_setting = 'on'\n" +
                "work_mem = '64MB'\n");

            var newConfPath = Path.Combine(newDataDirectory, "postgresql.conf");
            File.WriteAllText(newConfPath, "include 'darling-managed.conf'\n");

            Task<(int ExitCode, string Output)> Probe(string exePath, string arguments, TimeSpan timeout, CancellationToken token)
                => Task.FromResult(arguments.Contains("darling_4358_unknown_setting", StringComparison.Ordinal)
                    ? (1, "unrecognized configuration parameter \"darling_4358_unknown_setting\"")
                    : (0, string.Empty));

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            var result = await upgrade.CarryOperatorConfLinesAsync(
                oldDataDirectory, newDataDirectory, "unused-bin-dir", Probe, CancellationToken.None);

            Assert.Equal(2, result.CarriedCount);
            Assert.Equal(1, result.RejectedCount);

            var logText = log.ToString();
            Assert.Contains("NOT carried: darling_4358_unknown_setting", logText, StringComparison.Ordinal);

            var finalConf = await File.ReadAllTextAsync(newConfPath);
            Assert.DoesNotContain("darling_4358_unknown_setting", finalConf, StringComparison.Ordinal);
            Assert.Contains("log_min_duration_statement = 250", finalConf, StringComparison.Ordinal);
            Assert.Contains("work_mem = '64MB'", finalConf, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    private static async Task<string> StartDirectAsync(string binDirectory, string dataDirectory, int port, bool quiesced, CancellationToken cancellationToken)
    {
        var options = $"-p {port} -c listen_addresses=127.0.0.1" + (quiesced ? " -c timescaledb.max_background_workers=0" : string.Empty);
        var exitCode = await DarlingManagedPostgres.RunDetachingToolAsync(
            Path.Combine(binDirectory, "pg_ctl.exe"),
            $"-D \"{dataDirectory}\" -o \"{options}\" -w -t 120 start",
            TimeSpan.FromMinutes(3),
            cancellationToken);
        Assert.Equal(0, exitCode);
        return binDirectory;
    }

    private static async Task StopDirectAsync(string binDirectory, string dataDirectory, CancellationToken cancellationToken)
        => await DarlingManagedPostgres.RunToolAsync(
            Path.Combine(binDirectory, "pg_ctl.exe"),
            $"stop -D \"{dataDirectory}\" -m fast -w -t 120",
            TimeSpan.FromMinutes(3),
            cancellationToken);

    private static async Task ExecuteOnAsync(string connectionString, string sql, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<string?> ScalarOnAsync(string connectionString, string sql, CancellationToken cancellationToken)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    /// <summary>
    /// Reads the LIVE server's own start time, on a <c>Pooling=false</c> connection so this probe itself
    /// cannot be handed a stale pooled socket. Used to catch a restart between the upgrade bootstrap
    /// returning and a later pooled read: two calls with the same server identity return the same instant,
    /// and a moved instant means the server the caller thinks it still has is gone.
    /// </summary>
    private static async Task<DateTime> ReadPostmasterStartTimeAsync(string connectionString, CancellationToken cancellationToken)
    {
        var unpooled = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;
        await using var connection = new NpgsqlConnection(unpooled);
        await connection.OpenAsync(cancellationToken);
        await using var command = new NpgsqlCommand("SELECT pg_postmaster_start_time()", connection) { CommandTimeout = 60 };
        return DateTime.SpecifyKind((DateTime)(await command.ExecuteScalarAsync(cancellationToken))!, DateTimeKind.Utc);
    }

    /// <summary>
    /// Builds the failure message for ANY exception raised after <c>EnsureRunningAsync</c> returned: whether
    /// the upgraded server restarted under us since the bootstrap finished (compared against the
    /// <c>Pooling=false</c> reading taken right after it returned), plus the upgraded cluster's OWN server
    /// log tail — not the shared rig's log, which is all a bare stack trace leaves behind. FATAL/PANIC and
    /// termination lines are surfaced first, ahead of the raw tail, because those are the lines that turn
    /// "a pooled read failed" into "the server crashed and restarted".
    /// </summary>
    private static async Task<string> DescribePostBootstrapFailureAsync(
        Exception failure,
        string dataDirectory,
        string connectionString,
        DateTime startTimeAfterBootstrap,
        CancellationToken cancellationToken)
    {
        var restartNote = "(could not re-read pg_postmaster_start_time() to check for a restart)";
        try
        {
            var startTimeNow = await ReadPostmasterStartTimeAsync(connectionString, cancellationToken);
            restartNote = startTimeNow == startTimeAfterBootstrap
                ? $"the server did not restart (pg_postmaster_start_time() stayed {startTimeAfterBootstrap:O})"
                : $"the upgraded server restarted between the bootstrap and this read " +
                  $"(start time {startTimeAfterBootstrap:O} → {startTimeNow:O})";
        }
        catch (Exception probeEx)
        {
            restartNote = $"(could not re-read pg_postmaster_start_time() to check for a restart: {probeEx.Message})";
        }

        var serverLogTail = ReadUpgradedServerLogTail(dataDirectory);
        var highlighted = string.Join('\n', serverLogTail
            .Split('\n')
            .Where(line =>
                line.Contains("FATAL", StringComparison.Ordinal) ||
                line.Contains("PANIC", StringComparison.Ordinal) ||
                line.Contains("terminated by exception", StringComparison.Ordinal) ||
                line.Contains("was terminated", StringComparison.Ordinal) ||
                line.Contains("database system is shut down", StringComparison.Ordinal)));

        var highlightBlock = string.IsNullOrEmpty(highlighted)
            ? "(no FATAL/PANIC/terminated lines in the server log)"
            : highlighted;

        return $"{failure.Message}\n\n--- restart check ---\n{restartNote}" +
               $"\n\n--- upgraded server log, matching lines first ---\n{highlightBlock}" +
               $"\n\n--- upgraded server log tail ---\n{serverLogTail}";
    }

    /// <summary>
    /// The upgraded cluster's OWN server log — the newest of its <c>pg.log</c> (beside its data directory)
    /// and its logging-collector ring files (its data directory's <c>log\</c>), the same choice
    /// <see cref="DarlingManagedPostgres.PickNewestServerLog"/> makes for the live orchestration. Read by
    /// line offset, last 200 lines only — this file can be large after a whole bootstrap plus pg_upgrade run.
    /// </summary>
    private static string ReadUpgradedServerLogTail(string dataDirectory)
    {
        var serverLogPath = Path.Combine(
            Path.GetDirectoryName(Path.TrimEndingDirectorySeparator(Path.GetFullPath(dataDirectory)))!,
            DarlingManagedPostgres.ServerLogFileName);

        var newest = DarlingManagedPostgres.PickNewestServerLog(serverLogPath, dataDirectory);
        if (newest is null)
        {
            return "(no server log written for the upgraded cluster)";
        }

        try
        {
            var lines = File.ReadAllLines(newest);
            var take = Math.Min(200, lines.Length);
            return $"({newest})\n" + string.Join('\n', lines[^take..]);
        }
        catch (IOException ex)
        {
            return $"(could not read the upgraded cluster's server log {newest}: {ex.Message})";
        }
    }

    [Fact]
    public void BuildStoreUpgradeReport_CarriesAPostCommitWarningThroughOnSuccess()
    {
        /* THE SEAM THE DEFECT LIVED IN. A post-commit bookkeeping failure returns Succeeded with the reason
           in Message, and this mapping is what hands it to the alert engine. It previously hardcoded null
           here, so the reason was dropped between the orchestration and the operator: the log alarmed and the
           alert reassured. Pinned at the MAPPING rather than at the evaluator, because an evaluator test
           passes its own report in and cannot see this hop at all — verified by re-introducing the bug and
           watching the evaluator test stay green. */
        var outcome = new DarlingStoreUpgrade.StoreUpgradeOutcome(
            DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded, 17, 18, "2.28.1", "2.28.1",
            FailedStep: null, Message: "the retention marker could not be written (disk full)", UsedLinkMode: false);

        var report = DarlingWorker.BuildStoreUpgradeReport(outcome);

        Assert.NotNull(report);
        Assert.True(report!.Succeeded);
        Assert.Equal("the retention marker could not be written (disk full)", report.FailureMessage);
    }

    [Fact]
    public void BuildStoreUpgradeReport_CleanSuccessCarriesNoWarning_AndNothingIsNoReport()
    {
        var clean = DarlingWorker.BuildStoreUpgradeReport(new DarlingStoreUpgrade.StoreUpgradeOutcome(
            DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded, 17, 18, "2.28.1", "2.28.1", null, null, false));
        Assert.NotNull(clean);
        Assert.Null(clean!.FailureMessage);

        /* An extension-only update is not a major upgrade at all since #3908: it has its own outcome and its own
           report (BuildStoreTimescaleReport), and a start where nothing moved maps to nothing. */
        Assert.Null(DarlingWorker.BuildStoreUpgradeReport(DarlingStoreUpgrade.StoreUpgradeOutcome.None));
    }

    /* ==================================================================================
       WIRING pins, parsed from source.

       Both defects below are invisible to every other kind of test here. The logic tests pass because the
       logic is correct; the gated E2E passes because it is a HAPPY path that never throws after the swap and
       never meets an occupied port. Deleting `swapped = true`, reordering the catch clauses, or deleting the
       preflight call leaves the whole suite green — which is exactly the hole that let the original HIGH
       reach an arming gate. Same reasoning, and the same idiom, as HostHeaderGuardTests' #1648 middleware
       ordering pins: a WIRING omission needs a wiring test.
       ================================================================================== */

    private static string ReadUpgradeSource()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "DarlingStoreUpgrade.cs");
        Assert.True(File.Exists(path),
            "DarlingStoreUpgrade.cs was not copied beside the test binary — check the csproj None/Link item.");

        var source = File.ReadAllText(path);
        /* Guard the guard: if the file were restructured past recognition these assertions could pass
           vacuously on a mismatched parse, so pin the anchors they key off. */
        Assert.Contains("swapped = true;", source, StringComparison.Ordinal);
        Assert.Contains("catch (Exception ex)", source, StringComparison.Ordinal);
        return source;
    }

    [Fact]
    public void PostCommitCatch_StaysAheadOfTheGeneralCatch_AndNeverReverts()
    {
        var source = ReadUpgradeSource();

        var filtered = source.IndexOf("catch (Exception ex) when (swapped)", StringComparison.Ordinal);
        Assert.True(filtered >= 0, "the post-commit catch clause is gone — a failure after the swap would revert the runtime and leave old binaries in front of a new data directory");

        var general = source.IndexOf("catch (Exception ex)\r\n", filtered, StringComparison.Ordinal);
        if (general < 0)
        {
            general = source.IndexOf("catch (Exception ex)\n", filtered, StringComparison.Ordinal);
        }

        /* Belt-and-braces, and worth being honest about why: reordering these two clauses does NOT compile
           (CS0160, "a previous catch clause already catches all exceptions of this or of a super type"), so
           the compiler — not this assertion — is what actually prevents that specific mutation. This test
           earns its place on the two assertions around this one: that the filtered clause exists at all, and
           that nothing inside it reverts. Both of those mutations compile silently. */
        Assert.True(general > filtered,
            "the unfiltered catch now precedes the post-commit one, so the filtered clause would be " +
            "unreachable — a runtime revert over an already-swapped data directory, which is an unbootable " +
            "store. If this ever fails rather than failing to compile, the structure has changed in a way " +
            "that needs a human to look at it.");

        /* And within the post-commit handler, nothing may revert. */
        var body = source[filtered..general];
        Assert.DoesNotContain("RevertRuntime", body, StringComparison.Ordinal);
    }

    [Fact]
    public void CancellationPath_DoesNotRevertOnceTheSwapCommitted()
    {
        var source = ReadUpgradeSource();
        var cancel = source.IndexOf("catch (OperationCanceledException)", StringComparison.Ordinal);
        Assert.True(cancel >= 0);

        /* The revert in the cancellation handler must sit behind the !swapped guard. A shutdown timed after
           the swap is no more entitled to undo a completed upgrade than an exception is. */
        var guard = source.IndexOf("if (!swapped)", cancel, StringComparison.Ordinal);
        var revert = source.IndexOf("RevertRuntimeForCancel", cancel, StringComparison.Ordinal);
        Assert.True(guard >= 0 && revert > guard,
            "the cancellation path reverts the runtime without checking whether the swap already committed");

        /* ORDER IS NOT CONTAINMENT, and that difference is the whole bug. Moving the revert call OUT of
           the guarded block leaves it textually after `if (!swapped)`, so an order-only assertion still
           passes while the call now runs unconditionally:

               if (!swapped) { TryDeleteDirectory(newDataDirectory); }
               RevertRuntimeForCancel(context);          // unguarded again

           That is the store-bricking path — a shutdown after the swap reverts the runtime and leaves
           PostgreSQL 17 binaries in front of an 18 data directory. It compiles, and it passed this test
           green until this line existed. Correct code has NO closing brace between the guard and the call;
           relocating the call introduces one. Cheap containment without needing a parser — and the same
           vacuous-pass shape this very test was written to close, one level in. */
        Assert.DoesNotContain("}", source[guard..revert], StringComparison.Ordinal);
    }

    [Fact]
    public void PortPreflight_IsActuallyInvokedBeforePgUpgradeRuns()
    {
        var source = ReadUpgradeSource();

        var call = source.IndexOf("AssertUpgradePortsFree();", StringComparison.Ordinal);
        Assert.True(call >= 0,
            "nothing calls AssertUpgradePortsFree — FindOccupiedPorts being correct is worth nothing if the " +
            "check never runs, and a clean CI runner has free ports so no behavioral test would notice");

        var firstUpgrade = source.IndexOf("pg_upgrade.exe", StringComparison.Ordinal);
        Assert.True(firstUpgrade > call, "the port preflight must run BEFORE pg_upgrade is invoked");
    }

    [Fact]
    public void DowngradeGuard_IsInvokedBeforeTheRuntimeIsRescued_AndOutsideTheNoStampBranch()
    {
        /* #1738 was a WIRING failure in the same family as the two pins above: the majors were compared, the
           comparison was correct, and nothing checked which DIRECTION the difference went before swapping.
           Two things have to hold and neither is visible to a logic test.

           First the call must exist and precede the rescue — a guard evaluated after Directory.Move has
           already replaced the runtime is decoration.

           Second, and this is the subtle one, it must sit OUTSIDE the `if (stamp is null)` block. The stamp
           records only that the zip CHANGED, never which way, so a stamped host receiving an older package
           downgrades exactly as DARLING01 did. Putting the guard inside that branch would pass every
           behavioral test — the fixture is unstamped — while leaving every already-stamped host exposed,
           which after this release is all of them. */
        var source = ReadUpgradeSource();

        var call = source.IndexOf("IsDowngradeAgainstStore(dataDirectory, runtimeZipPath)", StringComparison.Ordinal);
        Assert.True(call >= 0, "nothing calls IsDowngradeAgainstStore — a correct downgrade check that is never invoked is what #1738 already was");

        var rescue = source.IndexOf("Directory.Move(pgsqlDirectory, previousPgsql)", StringComparison.Ordinal);
        Assert.True(rescue > call, "the downgrade guard must run BEFORE the runtime is rescued and replaced");

        var noStampBranch = source.IndexOf("if (stamp is null)", StringComparison.Ordinal);
        Assert.True(noStampBranch >= 0, "the no-stamp branch anchor is gone — this pin can no longer locate what it guards");
        Assert.True(call > noStampBranch,
            "the downgrade guard appears before the no-stamp branch, so it cannot be positioned relative to it");

        /* The no-stamp block closes before the guard: in correct code the guard is at method-body indent
           (8 spaces), which is only reachable once that block has closed — AND the call is the whole
           condition. Both halves are load-bearing, and each rules out a mutation the other misses:

             if (IsDowngradeAgainstStore(dataDirectory, runtimeZipPath) && stamp is null)   <- A
             if (stamp is null && IsDowngradeAgainstStore(dataDirectory, runtimeZipPath))   <- B

           Both re-bury the guard behind the stamp semantically while leaving it at the right indent and in
           the right position. A `Contains` on the opening text admits A — it stops at the open paren and
           never reads the rest of the condition — while an ordering check catches B and misses A. The line
           anchor rejects both, and still permits the guard's BODY to be reformatted freely. */
        Assert.Matches(@"(?m)^        if \(IsDowngradeAgainstStore\(dataDirectory, runtimeZipPath\)\)\s*$", source);
    }

    [Theory]
    /* The case #1738 is: a PostgreSQL 17 package landed beside an 18 store, the majors merely DIFFERED, the
       runtime was swapped, and the store was down for ~7 minutes. */
    [InlineData(18, 17, true)]
    [InlineData(18, 16, true)]
    /* Forward and same-major must NOT be blocked — inverting this comparison refuses every legitimate
       upgrade while permitting every downgrade, which is the filed defect doubled. That inversion left the
       whole suite green until this Theory existed. */
    [InlineData(17, 18, false)]
    [InlineData(18, 18, false)]
    /* Unknown either side abstains: this gate blocks only what it can PROVE is backwards. */
    [InlineData(null, 17, false)]
    [InlineData(18, null, false)]
    [InlineData(null, null, false)]
    public void IsDowngrade_BlocksOnlyAProvenBackwardsMove(int? storeMajor, int? packageMajor, bool expected)
        => Assert.Equal(expected, DarlingStoreUpgrade.IsDowngrade(storeMajor, packageMajor));

    [Theory]
    /* The DARLING01 pairing: the store's need KNOWN, the runtime unable to say what it is because its
       binaries would not launch. Logged as "skipping the runtime version check. The store starts normally"
       one second before the bootstrap died on STATUS_DLL_NOT_FOUND. */
    [InlineData(18, null, true)]
    [InlineData(17, null, true)]
    /* An unreadable DATA DIRECTORY is not the same thing: there is no known requirement to violate, and
       refusing would take a store down over an unreadable file. */
    [InlineData(null, 18, false)]
    [InlineData(null, null, false)]
    /* Both known is the ordinary path — the caller's own comparison decides from here. */
    [InlineData(18, 18, false)]
    [InlineData(17, 18, false)]
    public void MustRefuseUnidentifiableRuntime_StopsOnlyWhenTheStoreNeedsSomethingSpecific(
        int? dataMajor, int? runtimeMajor, bool expected)
        => Assert.Equal(expected, DarlingStoreUpgrade.MustRefuseUnidentifiableRuntime(dataMajor, runtimeMajor));

    [Fact]
    public void TryReadDataDirectoryMajor_ReadsPgVersionWithoutExecutingAnything()
    {
        /* The whole point of reading PG_VERSION rather than asking the binaries: on DARLING01 the extracted
           17 binaries could not launch (STATUS_DLL_NOT_FOUND), so every check that interrogated them got
           "unreadable" and degraded to proceeding — while this file answered 18 the entire time. */
        var root = Directory.CreateTempSubdirectory("darling-pgver-");
        try
        {
            Assert.Null(DarlingStoreUpgrade.TryReadDataDirectoryMajor(root.FullName));

            File.WriteAllText(Path.Combine(root.FullName, "PG_VERSION"), "18\n");
            Assert.Equal(18, DarlingStoreUpgrade.TryReadDataDirectoryMajor(root.FullName));

            File.WriteAllText(Path.Combine(root.FullName, "PG_VERSION"), "17");
            Assert.Equal(17, DarlingStoreUpgrade.TryReadDataDirectoryMajor(root.FullName));

            /* Garbage answers "unknown", never a guess — a wrong major here would drive a wrong decision. */
            File.WriteAllText(Path.Combine(root.FullName, "PG_VERSION"), "not a version");
            Assert.Null(DarlingStoreUpgrade.TryReadDataDirectoryMajor(root.FullName));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    [Fact]
    public void RevertRuntime_RefusesOnceTheDataDirectoryHasMovedForward()
    {
        /* #1737 item 3, the deferred defence-in-depth guard. Reverting to older binaries once the data
           directory is on a newer major produces a store that cannot start — which is precisely the outcome
           #1738 demonstrated empirically by a different route. Today's two callers cannot reach this, so this
           test is the only thing that will tell a future third caller it got it wrong. */
        var root = Directory.CreateTempSubdirectory("darling-revert-");
        try
        {
            var runtimeRoot = Path.Combine(root.FullName, "pg-runtime");
            var dataDirectory = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(Path.Combine(runtimeRoot, "pgsql", "bin"));
            Directory.CreateDirectory(Path.Combine(
                DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot), "pgsql", "bin"));
            Directory.CreateDirectory(dataDirectory);

            /* The store has already moved to 18; a revert that assumes 17 must refuse. */
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18\n");

            var log = new CapturingLogger();
            new DarlingStoreUpgrade(log).RevertRuntime(runtimeRoot, "deadbeef", dataDirectory, expectedDataMajor: 17);

            /* Refused: the rescued copy is still where it was, and the live runtime was not replaced. */
            Assert.True(Directory.Exists(Path.Combine(
                DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot), "pgsql")));
            Assert.True(Directory.Exists(Path.Combine(runtimeRoot, "pgsql")));
            Assert.Contains("REFUSING to revert", log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    [Fact]
    public void RevertRuntime_ProceedsWhenTheDataDirectoryIsStillOnTheExpectedMajor()
    {
        /* The guard must not block the case it exists to protect: a genuine pre-commit revert, where the data
           directory never moved. Getting this backwards would disable the whole fail-safe. */
        var root = Directory.CreateTempSubdirectory("darling-revert-ok-");
        try
        {
            var runtimeRoot = Path.Combine(root.FullName, "pg-runtime");
            var dataDirectory = Path.Combine(root.FullName, "pg");
            var previousPgsql = Path.Combine(DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot), "pgsql");
            Directory.CreateDirectory(Path.Combine(runtimeRoot, "pgsql", "bin"));
            Directory.CreateDirectory(Path.Combine(previousPgsql, "bin"));
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(previousPgsql, "bin", "marker.txt"), "rescued");
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "17\n");

            var log = new CapturingLogger();
            new DarlingStoreUpgrade(log).RevertRuntime(runtimeRoot, "deadbeef", dataDirectory, expectedDataMajor: 17);

            /* The rescued tree moved back over the live path, marker and all. */
            Assert.True(File.Exists(Path.Combine(runtimeRoot, "pgsql", "bin", "marker.txt")));
            Assert.DoesNotContain("REFUSING to revert", log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    [Fact]
    public void RetainedDataDirectory_IsNamedForTheMajorItCameFrom()
        => Assert.Equal(
            @"C:\ProgramData\PerformanceMonitorDarling\pg-old-17",
            DarlingStoreUpgrade.RetainedDataDirectoryFor(@"C:\ProgramData\PerformanceMonitorDarling\pg", 17));

    [Fact]
    public void PreviousRuntimeRoot_SitsBesideTheRuntimeItRescued()
        => Assert.Equal(
            @"C:\Program Files\Darling\pg-runtime-prev",
            DarlingStoreUpgrade.PreviousRuntimeRootFor(@"C:\Program Files\Darling\pg-runtime"));

    /// <summary>
    /// #3919. The swap clears the last update's rescued runtime (<c>pg-runtime-prev</c>) before rescuing the
    /// current one into it, and a file held open under it (an antivirus scan, an operator shell sitting in
    /// the folder) makes that delete throw. The rescue right after it already treats "cannot rescue" as a
    /// reason to SKIP the update, never to refuse to start, but the delete sat outside that guard: the
    /// exception escaped <c>EnsureRuntimeAsync</c> and the store did not start that time. A release that
    /// changes the bundled runtime sends every host down this path on its first start.
    ///
    /// <para>No PostgreSQL is needed and nothing is executed; see <see cref="PlantHostAwaitingARuntimeSwap"/>
    /// for how the fixture reaches the rescue without one.</para>
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_APreviousRuntimeItCannotDelete_DefersTheSwapInsteadOfFailingTheStart()
    {
        var root = Directory.CreateTempSubdirectory("darling-prev-locked-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);

            /* The last update's rescued runtime, with one of its files held open and no sharing allowed. */
            var previousRoot = DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot);
            var heldFile = Path.Combine(previousRoot, "pgsql", "bin", "postgres.exe");
            Directory.CreateDirectory(Path.GetDirectoryName(heldFile)!);
            File.WriteAllText(heldFile, "previous runtime");
            using var hold = new FileStream(heldFile, FileMode.Open, FileAccess.Read, FileShare.None);

            var log = new CapturingLogger();
            var advance = await new DarlingStoreUpgrade(log).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory,
                /* nothing is running in this fixture */ (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            var warning = AssertSwapDeferred(advance, host, log);
            Assert.Contains(previousRoot, warning, StringComparison.Ordinal);

            /* ...and the error with it, which names the file being held. */
            Assert.Contains(Path.GetFileName(heldFile), warning, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The other half of #3919's guard. Re-creating <c>pg-runtime-prev</c> right after clearing it can fail
    /// as well: a scanner still holding the folder that was just deleted leaves it delete-pending, and a stray
    /// FILE by that name blocks it outright. Either way there is nowhere to rescue the current runtime to, the
    /// same condition with the same answer. The stray file is the deterministic way to get there: the folder
    /// check sees no folder, nothing is deleted, and the create throws.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_APreviousRuntimeFolderItCannotCreate_DefersTheSwapToo()
    {
        var root = Directory.CreateTempSubdirectory("darling-prev-file-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);
            var previousRoot = DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot);
            File.WriteAllText(previousRoot, "a file where the folder should be");

            var log = new CapturingLogger();
            var advance = await new DarlingStoreUpgrade(log).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory,
                (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            var warning = AssertSwapDeferred(advance, host, log);
            Assert.Contains(previousRoot, warning, StringComparison.Ordinal);

            /* Nothing is deleted to make room: the file is not the service's to remove. */
            Assert.Equal("a file where the folder should be", File.ReadAllText(previousRoot));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The control for the two deferrals above: the same host, with a previous runtime nothing is holding,
    /// clears it and swaps. Without this, a guard that deferred EVERY swap would pass both of them. It needs
    /// no PostgreSQL either, because nothing after the extract executes a binary.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_APreviousRuntimeItCanClear_IsReplacedAndTheSwapProceeds()
    {
        var root = Directory.CreateTempSubdirectory("darling-prev-clear-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);

            var previousRoot = DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot);
            var staleFile = Path.Combine(previousRoot, "pgsql", "bin", "postgres.exe");
            Directory.CreateDirectory(Path.GetDirectoryName(staleFile)!);
            File.WriteAllText(staleFile, "previous runtime");

            var log = new CapturingLogger();
            var advance = await new DarlingStoreUpgrade(log).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory,
                (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            Assert.True(advance.Swapped);
            Assert.Equal(Path.Combine(previousRoot, "pgsql", "bin"), advance.PreviousBinDirectory);

            /* The last update's leftovers are gone, the live runtime was rescued in their place, the package's
               runtime is now live, and the stamp names the package. */
            Assert.False(File.Exists(staleFile));
            Assert.Equal(
                HostAwaitingARuntimeSwap.LiveRuntime,
                File.ReadAllText(Path.Combine(previousRoot, "pgsql", "bin", "pg_ctl.exe")));
            Assert.Equal(HostAwaitingARuntimeSwap.PackageRuntime, File.ReadAllText(host.PgCtl));
            Assert.Equal(DarlingStoreUpgrade.ComputeFileHash(host.Package), File.ReadAllText(host.StampPath).Trim());
            Assert.DoesNotContain("Could not clear the previous runtime", log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #4052: the narrowed install-root grant leaves the service Modify on <c>pg-runtime-prev</c> itself but
    /// only Read &amp; Execute on the root above it, so a delete-then-recreate of the folder can delete and then
    /// fail to recreate. The rescue must EMPTY the folder in place rather than delete and recreate it — this
    /// proves the folder surviving the rescue (same folder, not a new one) by comparing its creation time
    /// before and after.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_LeavesPgRuntimePrevInPlace_RatherThanDeletingAndRecreatingIt()
    {
        var root = Directory.CreateTempSubdirectory("darling-prev-inplace-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);

            var previousRoot = DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot);
            var staleFile = Path.Combine(previousRoot, "pgsql", "bin", "postgres.exe");
            Directory.CreateDirectory(Path.GetDirectoryName(staleFile)!);
            File.WriteAllText(staleFile, "previous runtime");
            var creationTimeBefore = Directory.GetCreationTimeUtc(previousRoot);

            var advance = await new DarlingStoreUpgrade(new CapturingLogger()).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory,
                (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            Assert.True(advance.Swapped);
            Assert.True(Directory.Exists(previousRoot));
            Assert.Equal(creationTimeBefore, Directory.GetCreationTimeUtc(previousRoot));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>#3908's rollback guard, on the swap path. After the swap, this release's own stamp names the package it
    /// installed, and the legacy file names the package every 3.3 to 3.8 release shipped. Each of those releases
    /// returns early when that file equals its own package, so a rollback to one keeps this runtime instead of
    /// swapping in one that cannot load the store. The legacy value is never this release's own hash.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_AfterASwap_PinsTheLegacyStampToThe33To38Package()
    {
        var root = Directory.CreateTempSubdirectory("darling-legacy-pin-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);
            var advance = await new DarlingStoreUpgrade(new CapturingLogger()).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory,
                (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            Assert.True(advance.Swapped);
            Assert.Equal(DarlingStoreUpgrade.ComputeFileHash(host.Package), File.ReadAllText(host.StampPath).Trim());
            Assert.Equal(
                DarlingStoreUpgrade.LegacyRuntimePackageHash,
                File.ReadAllText(Path.Combine(host.RuntimeRoot, DarlingStoreUpgrade.LegacyRuntimeStampFileName)).Trim());
            Assert.NotEqual(DarlingStoreUpgrade.RuntimeStampFileName, DarlingStoreUpgrade.LegacyRuntimeStampFileName);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3908's migration read. On a host's first start of this release, only the legacy stamp exists, and it is
    /// that host's real package hash. A legacy stamp equal to the shipped package means nothing changed: no swap,
    /// no rescue, the runtime untouched, exactly as the new stamp would have said.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_WithOnlyALegacyStamp_ReadsItAsTheRuntimesPackage()
    {
        var root = Directory.CreateTempSubdirectory("darling-legacy-read-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);
            File.Delete(host.StampPath);
            File.WriteAllText(
                Path.Combine(host.RuntimeRoot, DarlingStoreUpgrade.LegacyRuntimeStampFileName),
                DarlingStoreUpgrade.ComputeFileHash(host.Package));

            var advance = await new DarlingStoreUpgrade(new CapturingLogger()).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory,
                (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            Assert.False(advance.Swapped);
            Assert.Equal(HostAwaitingARuntimeSwap.LiveRuntime, File.ReadAllText(host.PgCtl));
            Assert.False(Directory.Exists(DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot)));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    [Fact]
    public void PinLegacyRuntimeStamp_WritesTheConstant_OverAnyOtherValue_AndLeavesItAlone()
    {
        var root = Directory.CreateTempSubdirectory("darling-legacy-const-");
        try
        {
            var legacy = Path.Combine(root.FullName, DarlingStoreUpgrade.LegacyRuntimeStampFileName);

            DarlingStoreUpgrade.PinLegacyRuntimeStamp(root.FullName, NullLogger.Instance);
            Assert.Equal(DarlingStoreUpgrade.LegacyRuntimePackageHash, File.ReadAllText(legacy));

            File.WriteAllText(legacy, "0000000000000000000000000000000000000000000000000000000000000000");
            DarlingStoreUpgrade.PinLegacyRuntimeStamp(root.FullName, NullLogger.Instance);
            Assert.Equal(DarlingStoreUpgrade.LegacyRuntimePackageHash, File.ReadAllText(legacy));

            var written = File.GetLastWriteTimeUtc(legacy);
            DarlingStoreUpgrade.PinLegacyRuntimeStamp(root.FullName, NullLogger.Instance);
            Assert.Equal(written, File.GetLastWriteTimeUtc(legacy));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The constant is the 3.3 to 3.8 package's real hash, not a placeholder: 64 lowercase hex characters, the
    /// format <see cref="DarlingStoreUpgrade.ComputeFileHash"/> writes, so a byte comparison by those releases
    /// matches.
    /// </summary>
    [Fact]
    public void LegacyRuntimePackageHash_IsAStampInTheFormatThoseReleasesWrote()
        => Assert.Matches("^[0-9a-f]{64}$", DarlingStoreUpgrade.LegacyRuntimePackageHash);

    /* ==================================================================================
       #3908: the store's TimescaleDB extension, moved before the store opens.
       ================================================================================== */

    [Theory]
    [InlineData("2.30.1", true)]
    [InlineData("2.28.1", true)]
    [InlineData("10.0.12", true)]
    [InlineData("2.30.1-dev", false)]
    [InlineData("2.30", false)]
    [InlineData("2.30.1.0", false)]
    [InlineData("2.30.1'; DROP TABLE t; --", false)]
    [InlineData("2.3a.1", false)]
    [InlineData("2.30.+1", false)]
    [InlineData("2.30.1 ", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void ParseTimescaleVersion_AcceptsOnlyAReleaseVersion(string? text, bool valid)
        => Assert.Equal(valid, DarlingStoreUpgrade.ParseTimescaleVersion(text) is not null);

    [Fact]
    public void TimescaleRecord_RoundTripsEveryState_AndRejectsAnythingElse()
    {
        foreach (var record in new[]
                 {
                     new DarlingStoreUpgrade.TimescaleRecord(DarlingStoreUpgrade.TimescaleRecordState.Absent, "2.30.1"),
                     new DarlingStoreUpgrade.TimescaleRecord(DarlingStoreUpgrade.TimescaleRecordState.Current, "2.30.1"),
                     new DarlingStoreUpgrade.TimescaleRecord(DarlingStoreUpgrade.TimescaleRecordState.Pending, "2.28.1", "2.30.1"),
                 })
        {
            Assert.Equal(record, DarlingStoreUpgrade.ParseTimescaleRecord(record.Format()));
            Assert.Equal(record, DarlingStoreUpgrade.ParseTimescaleRecord(record.Format() + "\r\n"));
        }

        /* A bare "absent" included: absence only means something under the runtime it was read under. */
        foreach (var malformed in new[] { null, "", "absent", "absent 2.30", "pending 2.28.1", "pending 2.28.1 2.30.1 2.31.0", "current 2.30.1", "2.30.1-dev" })
        {
            Assert.Null(DarlingStoreUpgrade.ParseTimescaleRecord(malformed));
        }

        /* A pending record names both versions: a start that stopped after the ALTER committed leaves the store
           at either, and a runtime has to be able to open it at both. */
        Assert.Equal(
            new[] { "2.28.1", "2.30.1" },
            new DarlingStoreUpgrade.TimescaleRecord(DarlingStoreUpgrade.TimescaleRecordState.Pending, "2.28.1", "2.30.1").StoreVersions);
        Assert.Equal(
            new[] { "2.30.1" },
            new DarlingStoreUpgrade.TimescaleRecord(DarlingStoreUpgrade.TimescaleRecordState.Current, "2.30.1").StoreVersions);
        Assert.Empty(new DarlingStoreUpgrade.TimescaleRecord(DarlingStoreUpgrade.TimescaleRecordState.Absent, "2.30.1").StoreVersions);
    }

    [Theory]
    [InlineData(null, "2.30.1", true)]
    [InlineData("2.28.1", "2.30.1", true)]
    [InlineData("2.30.1", "2.30.1", false)]
    [InlineData("absent 2.30.1", "2.30.1", false)]
    [InlineData("absent 2.28.1", "2.30.1", true)]
    [InlineData("pending 2.28.1 2.30.1", "2.30.1", true)]
    [InlineData("not a record", "2.30.1", true)]
    [InlineData(null, null, false)]
    [InlineData("2.28.1", "", false)]
    public void NeedsQuiescedTimescaleUpdate_RunsUnlessTheRecordSaysThereIsNothingToMove(string? record, string? bundled, bool expected)
        => Assert.Equal(expected, DarlingStoreUpgrade.NeedsQuiescedTimescaleUpdate(DarlingStoreUpgrade.ParseTimescaleRecord(record), bundled));

    [Fact]
    public void PlanTimescaleBridge_SkipsWhatTheNewRuntimeCarries_AndOtherwiseTargetsTheOldRuntimesOwnDefault()
    {
        string[] current = { "2.28.1", "2.30.1" };

        /* Every store this service has shipped: the new runtime carries 2.28.1, so pg_upgrade restores it as it
           is, and nothing runs on the old cluster. */
        Assert.Equal(
            new DarlingStoreUpgrade.TimescaleBridgePlan(false, null),
            DarlingStoreUpgrade.PlanTimescaleBridge("2.28.1", current, "2.28.1"));

        /* Not carried: the old runtime's own default, the one version it has update scripts for, when the new
           runtime can restore it. */
        Assert.Equal(
            new DarlingStoreUpgrade.TimescaleBridgePlan(true, "2.28.1"),
            DarlingStoreUpgrade.PlanTimescaleBridge("2.24.0", current, "2.28.1"));
        Assert.Equal(
            new DarlingStoreUpgrade.TimescaleBridgePlan(true, "2.30.1"),
            DarlingStoreUpgrade.PlanTimescaleBridge("2.24.0", current, "2.30.1"));

        /* No reachable target: the upgrade stops before pg_upgrade instead of failing inside it. The old default
           not above the store's version, a default the new runtime cannot restore, and no default at all. */
        Assert.Equal(
            new DarlingStoreUpgrade.TimescaleBridgePlan(true, null),
            DarlingStoreUpgrade.PlanTimescaleBridge("2.24.0", current, "2.24.0"));
        Assert.Equal(
            new DarlingStoreUpgrade.TimescaleBridgePlan(true, null),
            DarlingStoreUpgrade.PlanTimescaleBridge("2.24.0", current, "2.29.0"));
        Assert.Equal(
            new DarlingStoreUpgrade.TimescaleBridgePlan(true, null),
            DarlingStoreUpgrade.PlanTimescaleBridge("2.24.0", current, null));
    }

    [Fact]
    public void TryReadPostmasterPort_ReadsTheFourthLine()
    {
        var root = Directory.CreateTempSubdirectory("darling-pmport-");
        try
        {
            var pid = Path.Combine(root.FullName, "postmaster.pid");
            Assert.Null(DarlingStoreUpgrade.TryReadPostmasterPort(root.FullName));

            File.WriteAllText(pid, "4242\nC:/data\n1758600000\n56123\n\n127.0.0.1\n  1234567  0\nready   \n");
            Assert.Equal("56123", DarlingStoreUpgrade.TryReadPostmasterPort(root.FullName));

            File.WriteAllText(pid, "4242\nC:/data\n");
            Assert.Null(DarlingStoreUpgrade.TryReadPostmasterPort(root.FullName));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3908's orphan rule, without a server. A marker whose port no <c>postmaster.pid</c> claims is stale (a crash
    /// after the stop, or a server someone else started since) and is removed. A marker whose port the pid file
    /// does claim is the quiesced update's own server, and one that cannot be confirmed stopped, here because the
    /// runtime has no pg_ctl at all, refuses the start and keeps the marker, so the next start tries again rather
    /// than adopting it as the store.
    /// </summary>
    [Fact]
    public async Task StopQuiescedUpdateOrphan_ClearsAStaleMarker_AndRefusesAnOrphanItCannotStop()
    {
        var root = Directory.CreateTempSubdirectory("darling-orphan-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(data);
            var bin = Path.Combine(root.FullName, "pg-runtime", "pgsql", "bin");
            Directory.CreateDirectory(bin);
            var marker = Path.Combine(data, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName);
            var pid = Path.Combine(data, "postmaster.pid");
            var upgrade = new DarlingStoreUpgrade(new CapturingLogger());

            Assert.True(await upgrade.StopQuiescedUpdateOrphanAsync(bin, data));

            File.WriteAllText(marker, "56123");
            File.WriteAllText(pid, $"4242\n{data}\n1758600000\n5432\n");
            Assert.True(await upgrade.StopQuiescedUpdateOrphanAsync(bin, data));
            Assert.False(File.Exists(marker));

            File.WriteAllText(marker, "56123");
            File.WriteAllText(pid, $"4242\n{data}\n1758600000\n56123\n");
            Assert.False(await upgrade.StopQuiescedUpdateOrphanAsync(bin, data));
            Assert.True(File.Exists(marker));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3908's rollback guard. A store whose record names a TimescaleDB version the shipped package has no
    /// libraries for keeps the runtime on disk, because that package could not open it: this is a newer
    /// release's store under an older release's package. The stamp stays as it was, so the refusal repeats until
    /// a package that can load the store ships. The same host, once the package does carry the version, swaps.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_KeepsTheRuntimeWhenThePackageCannotOpenTheStoresTimescale()
    {
        var root = Directory.CreateTempSubdirectory("darling-tsguard-");
        try
        {
            var host = PlantHostAwaitingARuntimeSwap(root.FullName);
            File.WriteAllText(Path.Combine(host.DataDirectory, DarlingStoreUpgrade.TimescaleRecordFileName), "2.31.0");

            var log = new CapturingLogger();
            var refused = await new DarlingStoreUpgrade(log).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory, (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);

            Assert.False(refused.Swapped);
            Assert.Equal(HostAwaitingARuntimeSwap.LiveRuntime, File.ReadAllText(host.PgCtl));
            Assert.Equal(HostAwaitingARuntimeSwap.PriorStamp, File.ReadAllText(host.StampPath).Trim());
            Assert.False(Directory.Exists(DarlingStoreUpgrade.PreviousRuntimeRootFor(host.RuntimeRoot)));
            Assert.Contains("has no libraries for 2.31.0", log.ToString(), StringComparison.Ordinal);

            /* A package carrying both of the version's libraries can open the store, so it swaps. */
            var lib = Path.Combine(root.FullName, "package", "pgsql", "lib");
            Directory.CreateDirectory(lib);
            File.WriteAllText(Path.Combine(lib, "timescaledb-2.31.0.dll"), "x");
            File.WriteAllText(Path.Combine(lib, "timescaledb-tsl-2.31.0.dll"), "x");
            File.Delete(host.Package);
            ZipFile.CreateFromDirectory(
                Path.Combine(root.FullName, "package", "pgsql"), host.Package, CompressionLevel.NoCompression, includeBaseDirectory: true);

            var swapped = await new DarlingStoreUpgrade(new CapturingLogger()).TryAdvanceRuntimeAsync(
                host.RuntimeRoot, host.Package, host.DataDirectory, (_, _) => Task.FromResult(false),
                TestContext.Current.CancellationToken);
            Assert.True(swapped.Swapped);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3908's shared "can this package open the store" answer: the recorded versions the package has no complete
    /// libraries for. Both a pending record's versions count, an absent extension needs nothing, and no record
    /// abstains.
    /// </summary>
    [Fact]
    public void MissingTimescaleLibraries_NamesOnlyWhatThePackageCannotLoad()
    {
        var root = Directory.CreateTempSubdirectory("darling-tsmissing-");
        try
        {
            var data = Path.Combine(root.FullName, "pg");
            Directory.CreateDirectory(data);
            var record = Path.Combine(data, DarlingStoreUpgrade.TimescaleRecordFileName);
            var zip = PlantRuntimeZip(root.FullName, "carries-2.30.1", "2.30.1");

            Assert.Empty(DarlingStoreUpgrade.MissingTimescaleLibraries(data, zip));

            File.WriteAllText(record, "2.30.1");
            Assert.Empty(DarlingStoreUpgrade.MissingTimescaleLibraries(data, zip));

            File.WriteAllText(record, "2.31.0");
            Assert.Equal(new[] { "2.31.0" }, DarlingStoreUpgrade.MissingTimescaleLibraries(data, zip));

            File.WriteAllText(record, "pending 2.30.1 2.31.0");
            Assert.Equal(new[] { "2.31.0" }, DarlingStoreUpgrade.MissingTimescaleLibraries(data, zip));

            File.WriteAllText(record, "absent 2.31.0");
            Assert.Empty(DarlingStoreUpgrade.MissingTimescaleLibraries(data, zip));
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// #3908, the clean-reinstall case. The runtime folder is gone and only the zip is beside the service, so the
    /// start would extract it with nothing to swap. When the store's recorded TimescaleDB is one that zip cannot
    /// load, the start refuses with the reason instead of extracting a runtime that cannot open the store, and
    /// nothing is extracted. No PostgreSQL is needed: the refusal comes before anything runs.
    /// </summary>
    [Fact]
    public async Task FirstExtraction_RefusesAPackageThatCannotOpenTheStore()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");

        var root = Directory.CreateTempSubdirectory("darling-tsextract-");
        try
        {
            var deployment = Path.Combine(root.FullName, "deploy");
            var runtimeRoot = Path.Combine(deployment, "pg-runtime");
            Directory.CreateDirectory(runtimeRoot);
            var zip = PlantRuntimeZip(root.FullName, "carries-2.30.1", "2.30.1");
            File.Copy(zip, Path.Combine(deployment, "pg-runtime.zip"));

            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, DarlingStoreUpgrade.TimescaleRecordFileName), "2.31.0");

            var managed = new DarlingManagedPostgres(
                new PostgresConfig { Managed = true, Port = FindFreeTcpPort(), DataDirectory = dataDirectory },
                new CapturingLogger(),
                runtimeRoot);

            var refused = await Assert.ThrowsAsync<InvalidOperationException>(
                () => managed.EnsureRunningAsync(TestContext.Current.CancellationToken));
            Assert.Contains("carries no libraries for it", refused.Message, StringComparison.Ordinal);
            Assert.Contains("2.31.0", refused.Message, StringComparison.Ordinal);
            Assert.False(Directory.Exists(Path.Combine(runtimeRoot, "pgsql")), "nothing may be extracted");
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>A runtime zip carrying a fake pg_ctl and both libraries of each given TimescaleDB version.</summary>
    private static string PlantRuntimeZip(string root, string name, params string[] timescaleVersions)
    {
        var pgsql = Path.Combine(root, name, "pgsql");
        Directory.CreateDirectory(Path.Combine(pgsql, "bin"));
        Directory.CreateDirectory(Path.Combine(pgsql, "lib"));
        File.WriteAllText(Path.Combine(pgsql, "bin", "pg_ctl.exe"), "fake");
        foreach (var version in timescaleVersions)
        {
            File.WriteAllText(Path.Combine(pgsql, "lib", $"timescaledb-{version}.dll"), "x");
            File.WriteAllText(Path.Combine(pgsql, "lib", $"timescaledb-tsl-{version}.dll"), "x");
        }

        var zip = Path.Combine(root, name + ".zip");
        ZipFile.CreateFromDirectory(pgsql, zip, CompressionLevel.NoCompression, includeBaseDirectory: true);
        return zip;
    }

    [Fact]
    public void BuildStoreTimescaleReport_ReportsOnlyAnExtensionThatDidNotReachTheRuntime()
    {
        Assert.Equal(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(true, "2.28.1", "2.30.1", "no update path"),
            DarlingWorker.BuildStoreTimescaleReport(new DarlingStoreUpgrade.TimescaleUpdateOutcome(
                DarlingStoreUpgrade.TimescaleUpdateStatus.Failed, "2.28.1", "2.30.1", "no update path")));
        Assert.Equal(
            new DarlingSelfAlertEvaluator.StoreTimescaleReport(false, "2.28.1", "2.30.1", null),
            DarlingWorker.BuildStoreTimescaleReport(new DarlingStoreUpgrade.TimescaleUpdateOutcome(
                DarlingStoreUpgrade.TimescaleUpdateStatus.Behind, "2.28.1", "2.30.1", null)));

        /* An update that landed is routine maintenance the log records; nothing is nothing. */
        Assert.Null(DarlingWorker.BuildStoreTimescaleReport(new DarlingStoreUpgrade.TimescaleUpdateOutcome(
            DarlingStoreUpgrade.TimescaleUpdateStatus.Updated, "2.28.1", "2.30.1", null)));
        Assert.Null(DarlingWorker.BuildStoreTimescaleReport(DarlingStoreUpgrade.TimescaleUpdateOutcome.None));
    }

    /// <summary>
    /// #3908's wiring, pinned the #1648 way, because a wiring omission needs a wiring test. The quiesced update
    /// runs after the conf append and before the network plan and the start, which is what makes it quiesced.
    /// The orphan check runs before the runtime advance, which would otherwise defer behind an orphan. And
    /// nothing after the start moves the extension any more.
    /// </summary>
    [Fact]
    public void QuiescedUpdate_IsWiredBeforeTheStoreOpens_AndNothingMovesTheExtensionAfterIt()
    {
        var managed = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingManagedPostgres.cs");

        int At(string needle, int from)
        {
            var index = managed.IndexOf(needle, from, StringComparison.Ordinal);
            Assert.True(index >= 0, $"'{needle}' is gone from DarlingManagedPostgres.cs");
            return index;
        }

        var ensureRunning = At("public async Task<string> EnsureRunningAsync(", 0);
        var conf = At("EnsureConfAppended(_dataDirectory);", ensureRunning);
        var update = At("UpdateTimescaleQuiescedAsync(", ensureRunning);
        /* networkPlan is declared as a Lazy<NetworkPlan> before this method's upgrade branch (#4280 round-2
           part 2, item 2 — so the auto.conf carry trial's SSL delegate can force it), but RESOLVED (.Value)
           here, in the same relative spot the eager BuildNetworkPlan() call used to sit — this is still the
           ordering that matters: nothing forces cert generation before the quiesced update has run. */
        var plan = At("networkPlan.Value.DegradeReason", ensureRunning);
        var start = At("StartServerAsync(binDirectory, networkPlan", ensureRunning);
        Assert.True(conf < update && update < plan && plan < start,
            "the quiesced TimescaleDB update must run after the conf append and before the network plan and the start");

        /* A bootstrap retry keeps the outcome of the attempt that ran the update, and the update never starts new
           binaries on a data directory of another major (a revert that could not run, #3927). */
        var gate = managed[ensureRunning..update];
        Assert.Contains("if (!(alreadyRunning && _startedByThisProcess))", gate, StringComparison.Ordinal);
        Assert.Contains("DarlingStoreUpgrade.TryReadDataDirectoryMajor(_dataDirectory) == _bundledMajor", gate, StringComparison.Ordinal);

        var ensureRuntime = At("private async Task<string> EnsureRuntimeAsync(", 0);
        Assert.True(At("StopQuiescedUpdateOrphanAsync(", ensureRuntime) < At("TryAdvanceRuntimeAsync(", ensureRuntime),
            "a server the quiesced update left behind must be stopped before the runtime advance checks for a running server");

        var upgrade = ReadUpgradeSource();
        var complete = upgrade.IndexOf("internal async Task<StoreUpgradeOutcome> CompleteAfterStartAsync(", StringComparison.Ordinal);
        var completeEnd = upgrade.IndexOf("private async Task VerifySentinelReadAsync(", complete, StringComparison.Ordinal);
        Assert.True(complete >= 0 && completeEnd > complete);
        Assert.DoesNotContain("ALTER EXTENSION", upgrade[complete..completeEnd], StringComparison.Ordinal);
    }

    /// <summary>A host one start away from a runtime swap, and what the fixture wrote there.</summary>
    private sealed record HostAwaitingARuntimeSwap(string RuntimeRoot, string PgCtl, string StampPath, string Package, string DataDirectory)
    {
        public const string LiveRuntime = "live runtime";

        public const string PackageRuntime = "new runtime";

        public const string PriorStamp = "0000000000000000000000000000000000000000000000000000000000000000";
    }

    /// <summary>
    /// A host that <see cref="DarlingStoreUpgrade.TryAdvanceRuntimeAsync"/> walks all the way to the rescue
    /// with no PostgreSQL present: a live runtime STAMPED with the package it came from, a different package
    /// beside it, and a data directory with no <c>PG_VERSION</c>. A stamp that exists and differs skips the
    /// no-stamp branch, the only one that runs <c>pg_ctl</c>, so the runtime's files can be plain text. The
    /// missing <c>PG_VERSION</c> makes the downgrade guard abstain before it opens the zip. Any real zip does
    /// as the package, since a hash that differs from the stamp is the whole trigger. With nothing running,
    /// the next step is clearing <c>pg-runtime-prev</c> for the rescue.
    /// </summary>
    private static HostAwaitingARuntimeSwap PlantHostAwaitingARuntimeSwap(string root)
    {
        var runtimeRoot = Path.Combine(root, "pg-runtime");
        var pgCtl = Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe");
        Directory.CreateDirectory(Path.GetDirectoryName(pgCtl)!);
        File.WriteAllText(pgCtl, HostAwaitingARuntimeSwap.LiveRuntime);

        var stampPath = Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName);
        File.WriteAllText(stampPath, HostAwaitingARuntimeSwap.PriorStamp);

        var packageSource = Path.Combine(root, "package", "pgsql");
        Directory.CreateDirectory(Path.Combine(packageSource, "bin"));
        File.WriteAllText(Path.Combine(packageSource, "bin", "pg_ctl.exe"), HostAwaitingARuntimeSwap.PackageRuntime);
        var package = Path.Combine(root, "pg-runtime.zip");
        ZipFile.CreateFromDirectory(packageSource, package, CompressionLevel.NoCompression, includeBaseDirectory: true);

        var dataDirectory = Path.Combine(root, "pg");
        Directory.CreateDirectory(dataDirectory);

        return new HostAwaitingARuntimeSwap(runtimeRoot, pgCtl, stampPath, package, dataDirectory);
    }

    /// <summary>
    /// What a deferred swap must leave behind: no swap, the live runtime exactly as it was, and the OLD stamp,
    /// so the next start sees the difference and tries again. Returns the one warning that says why, for the
    /// caller's own checks. It is matched on its own wording because the "rescuing the current runtime to"
    /// warning logged just before it names a path under the same folder, which would satisfy a bare path
    /// check by itself.
    /// </summary>
    private static string AssertSwapDeferred(
        DarlingStoreUpgrade.RuntimeAdvance advance, HostAwaitingARuntimeSwap host, CapturingLogger log)
    {
        Assert.False(advance.Swapped);
        Assert.Null(advance.PreviousBinDirectory);
        Assert.True(File.Exists(host.PgCtl), "the live runtime must still be in place; a deferred update never costs the store its binaries");
        Assert.Equal(HostAwaitingARuntimeSwap.LiveRuntime, File.ReadAllText(host.PgCtl));
        Assert.Equal(HostAwaitingARuntimeSwap.PriorStamp, File.ReadAllText(host.StampPath).Trim());

        var warning = Assert.Single(
            log.ToString().Split(Environment.NewLine),
            line => line.Contains("Could not clear the previous runtime", StringComparison.Ordinal));
        Assert.StartsWith("[Warning]", warning, StringComparison.Ordinal);
        return warning;
    }

    /* ==================================================================================
       The gated upgraded-in-place fixture.
       ================================================================================== */

    /// <summary>
    /// Builds a REAL store on the old runtime, ships the new runtime as the package's zip, and runs the
    /// production bootstrap over it — then proves the upgraded store kept its data and its TimescaleDB
    /// objects. Everything is measured before and after: this test fails if the upgrade "succeeds" while
    /// losing rows, breaking the continuous aggregate, or leaving the extension behind its binaries.
    /// </summary>
    [Fact]
    public async Task UpgradeInPlace_OldMajorStoreWithRealData_UpgradesAndKeepsEverything_Gated()
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_OLD");
        var newZip = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_NEWZIP");

        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime) || string.IsNullOrWhiteSpace(newZip),
            "Set DARLING_TEST_PGRUNTIME_OLD to an assembled pg-runtime directory built from the PREVIOUS " +
            "PostgreSQL major (the folder containing pgsql\\bin\\pg_ctl.exe) and DARLING_TEST_PGRUNTIME_NEWZIP " +
            "to a pg-runtime.zip built from the CURRENT one. Darling\\tools\\new-upgraded-store-fixture.ps1 " +
            "produces both.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(oldRuntime!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_OLD={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");
        Assert.SkipUnless(File.Exists(newZip!), $"DARLING_TEST_PGRUNTIME_NEWZIP={newZip} does not exist.");

        var root = Directory.CreateTempSubdirectory("darling-pgupgrade-");
        try
        {
            /* The deployment starts as the OLD install: pg-runtime\pgsql only. The NEW package's zip is
               dropped in later, because that IS the event under test — the store has to be created by the
               old runtime, undisturbed, before the upgrade can be a real upgrade. */
            var deployment = Path.Combine(root.FullName, "deploy");
            var runtimeRoot = Path.Combine(deployment, "pg-runtime");
            Directory.CreateDirectory(deployment);
            CopyDirectory(Path.Combine(oldRuntime!, "pgsql"), Path.Combine(runtimeRoot, "pgsql"));

            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            var config = new PostgresConfig
            {
                Managed = true,
                Port = FindFreeTcpPort(),
                DataDirectory = dataDirectory,
            };

            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(20));

            /* ---- 1. Create the store on the OLD runtime, exactly as a field install did. The stamp is
                    written for the OLD runtime so the bootstrap sees a legitimately-installed one. ---- */
            var oldMajor = await BuildOldStoreAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);

            /* Stamp the installed runtime with the identity of the zip it came from — what production does
               at extraction time. Stored uncompressed: this is a hash source, not an artifact. */
            var stampSource = Path.Combine(root.FullName, "old-runtime-stamp-source.zip");
            ZipFile.CreateFromDirectory(
                Path.Combine(runtimeRoot, "pgsql"), stampSource, CompressionLevel.NoCompression, includeBaseDirectory: true);
            File.WriteAllText(
                Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName),
                DarlingStoreUpgrade.ComputeFileHash(stampSource));

            /* NOW the new package arrives beside the old runtime — the deploy that must trigger the upgrade. */
            File.Copy(newZip!, Path.Combine(deployment, "pg-runtime.zip"));

            var password = DarlingSecrets.Unprotect(
                File.ReadAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory)).Trim());
            /* Pooling=false: this connection string is used against a server this test itself stops and
               restarts (StopWithRuntimeAsync/StartWithRuntimeAsync below). A pooled Npgsql connection handed
               back on the same host/port/user after a restart is a physical socket opened against the OLD
               server's lifetime, which the stop already killed — its first use fails with a forcibly-closed
               connection. */
            var oldConnection = new NpgsqlConnectionStringBuilder(
                DarlingManagedPostgres.BuildConnectionString(config.Port, password)) { Pooling = false }.ConnectionString;

            /* ---- 2. Measure the store BEFORE, through the old binaries. An ALTER SYSTEM value here (#4253)
                    is the operator tuning the upgrade must not silently discard — written straight to
                    postgresql.auto.conf, so no reload is needed for it to be on disk for the carry step to
                    find once this cluster stops. ---- */
            await StartWithRuntimeAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);
            await ExecuteOnAsync(oldConnection, "ALTER SYSTEM SET work_mem = '199MB'", timeout.Token);
            var before = await MeasureStoreAsync(oldConnection, timeout.Token);
            await StopWithRuntimeAsync(runtimeRoot, dataDirectory, timeout.Token);

            Assert.True(before.LogRows > 0, "the fixture must contain rows for the comparison to mean anything");
            Assert.True(before.CaggRows > 0, "the fixture must contain a materialized continuous aggregate");

            /* ---- 3. Run the REAL bootstrap. This is the whole feature: detect, rescue the old runtime,
                    extract the new one, bridge TimescaleDB, initdb, pg_upgrade, swap, start, verify.

                    A CAPTURING logger, not NullLogger: the orchestration reports which of its steps failed
                    and why through the log, and swallowing that turns any failure here into a bare stack
                    trace with no cause. Replayed into the assertion message below so a red run in CI is
                    diagnosable from the output alone. ---- */
            var log = new CapturingLogger();
            var managed = new DarlingManagedPostgres(config, log, runtimeRoot);

            string connectionString;
            try
            {
                connectionString = await managed.EnsureRunningAsync(timeout.Token);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"The store upgrade bootstrap threw: {ex.Message}\n\n--- orchestration log ---\n{log}", ex);
            }

            /* DIAGNOSTIC for a nightly failure where a pooled read taken right after EnsureRunningAsync
               returned failed with "forcibly closed": that pooled connector was opened by product code
               INSIDE the bootstrap, so if the upgraded server restarted (a crash-restart, or a stop/start
               this test hasn't found) between the bootstrap returning and the first pooled read below, the
               pool handed back a socket to a server whose lifetime already ended. Recorded on a
               Pooling=false connection — the same shape ReadClusterIdentityAsync already uses — so this
               probe itself can't be the thing that dies. */
            var startTimeAfterBootstrap = await ReadPostmasterStartTimeAsync(connectionString, timeout.Token);

            try
            {
                /* ---- 4. The store is on the NEW major and its data survived intact. ---- */
                var after = await MeasureStoreAsync(connectionString, timeout.Token);

                Assert.True(after.ServerMajor > oldMajor,
                    $"expected an upgrade past PostgreSQL {oldMajor}, but the server reports {after.ServerMajor}." +
                    $"\n\n--- orchestration log ---\n{log}");
                Assert.Equal(before.LogRows, after.LogRows);
                Assert.Equal(before.PlanRows, after.PlanRows);
                Assert.Equal(before.PlanXmlLength, after.PlanXmlLength);
                Assert.Equal(before.LogChecksum, after.LogChecksum);

                /* #4253: the operator's ALTER SYSTEM value from step 2 survived the real pg_upgrade, read
                   back through the upgraded store itself rather than just the isolated carry-step test. */
                Assert.Equal("199MB", await ScalarOnAsync(connectionString, "SHOW work_mem", timeout.Token));

                /* The continuous aggregate is not merely present — it still ANSWERS, which means the
                   TimescaleDB catalog, the materialization hypertable and its chunks all came across. */
                Assert.Equal(before.CaggRows, after.CaggRows);

                /* The extension matches the binaries it now runs on. A store whose extension lagged its
                   runtime is the #1705 drift this whole issue exists to end. */
                Assert.Equal(BundledTimescaleVersion(runtimeRoot), after.TimescaleVersion);

                /* #3908: pg_upgrade restored the store's own TimescaleDB, which the new runtime carries, and the
                   quiesced update moved it after the swap committed. Both reports agree on where it ended up. */
                Assert.Equal(DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded, managed.LastUpgradeOutcome.Status);
                Assert.Null(DarlingWorker.BuildStoreTimescaleReport(managed.LastTimescaleOutcome));
                if (!string.Equals(managed.LastUpgradeOutcome.FromTimescale, after.TimescaleVersion, StringComparison.Ordinal))
                {
                    Assert.Equal(after.TimescaleVersion, managed.LastUpgradeOutcome.ToTimescale);
                    Assert.Equal(DarlingStoreUpgrade.TimescaleUpdateStatus.Updated, managed.LastTimescaleOutcome.Status);
                }

                Assert.Equal(
                    after.TimescaleVersion,
                    File.ReadAllText(Path.Combine(dataDirectory, DarlingStoreUpgrade.TimescaleRecordFileName)).Trim());

                /* Compressed chunks survived as compressed chunks. */
                Assert.Equal(before.CompressedChunks, after.CompressedChunks);

                /* The rollback copy is on disk, named for the major it came from. */
                Assert.True(
                    Directory.Exists(DarlingStoreUpgrade.RetainedDataDirectoryFor(dataDirectory, oldMajor)),
                    "the pre-upgrade data directory should be retained for rollback after a copy-mode upgrade");

                /* The rescued old runtime is still there — the only thing that could ever upgrade this
                   store again from the old major, and pg_upgrade's --old-bindir. */
                Assert.True(Directory.Exists(
                    Path.Combine(DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot), "pgsql")));

                /* The managed conf blocks are on the NEW data directory: the upgrade wrote them before
                   pg_upgrade (shared_preload_libraries has to be live for the extension to restore) and
                   the normal heal path did not duplicate them. */
                var conf = await File.ReadAllTextAsync(Path.Combine(dataDirectory, "postgresql.conf"), timeout.Token);
                Assert.Contains("shared_preload_libraries = 'timescaledb'", conf, StringComparison.Ordinal);
                Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarker));
                Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV6));
                Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV7));
            }
            catch (Exception ex)
            {
                /* ANY failure after the bootstrap returned — a dead pooled connector's exception, a data
                   mismatch assertion, anything — gets the same diagnosis: did the upgraded server restart
                   under us, and if so (or regardless) what does its own log say right now. */
                throw new InvalidOperationException(
                    await DescribePostBootstrapFailureAsync(
                        ex, dataDirectory, connectionString, startTimeAfterBootstrap, timeout.Token),
                    ex);
            }
            finally
            {
                await managed.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            /* DARLING_TEST_KEEP leaves the whole fixture — both data directories, both runtimes, the
               server logs and pg_upgrade's own output tree — on disk. A failure here is almost always
               explained by a file this test would otherwise delete on its way out, and re-running to
               reproduce costs minutes each time. Off by default so ordinary runs leave nothing behind. */
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_KEEP")))
            {
                TryDeleteTree(root.FullName);
            }
        }
    }

    /// <summary>
    /// #3906: the runtime swap every existing field host takes when a release moves the bundle WITHIN its
    /// major, as 3.8.0's PostgreSQL 18.4 moving to 18.6 does. The twin above covers pg_upgrade. This path has
    /// none: rescue the runtime, extract the new one, and start the SAME data directory on the new binaries.
    /// Everything the twin measures is measured here too, because a same-major swap that loses a continuous
    /// aggregate or breaks the extension is the same #1705 failure without the major change to blame.
    ///
    /// <para>Both host shapes run. A STAMPED host (every install since stamping shipped) swaps because the
    /// zip's hash changed. An UNSTAMPED one reaches the no-stamp branch, which before #3906 compared majors
    /// and TimescaleDB only, so it adopted 18.4 as the 18.6 package and swapped nothing. The unstamped run's
    /// version assertion is what catches that.</para>
    ///
    /// <para>When the pair also moves TimescaleDB, the extension moves too (#3908): before the store opens, on
    /// a private port, and recorded in the data directory. Gated on <c>DARLING_TEST_PGRUNTIME_PREVIOUS</c> (the previous release's runtime on the bundle's major,
    /// built by new-upgraded-store-fixture.ps1 and set by the nightly) and <c>DARLING_TEST_PGRUNTIME_NEWZIP</c>.</para>
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task RuntimeAdvance_SameMajorStoreWithRealData_SwapsWithoutPgUpgradeAndKeepsEverything_Gated(bool stamped)
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_PREVIOUS");
        var newZip = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_NEWZIP");

        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime) || string.IsNullOrWhiteSpace(newZip),
            "Set DARLING_TEST_PGRUNTIME_PREVIOUS to the previous release's assembled pg-runtime (the bundle's PostgreSQL " +
            "major, an older minor) and DARLING_TEST_PGRUNTIME_NEWZIP to a pg-runtime.zip built from the current pins. " +
            "Darling\\tools\\new-upgraded-store-fixture.ps1 produces both.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(oldRuntime!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_PREVIOUS={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");
        Assert.SkipUnless(File.Exists(newZip!), $"DARLING_TEST_PGRUNTIME_NEWZIP={newZip} does not exist.");

        /* A fixture that is not the same major is a stale fixture, not a reason to skip: a skip here would
           quietly drop the only run of the upgrade nearly every field host takes. When the bundle moves to a
           new major, new-upgraded-store-fixture.ps1's previous-release pins move with it. */
        var previousMajor = OldRuntimeMajor(oldRuntime!);
        var packageMajor = DarlingStoreUpgrade.TryReadZipPostgresMajor(newZip!);
        Assert.NotNull(previousMajor);
        Assert.NotNull(packageMajor);
        Assert.True(previousMajor == packageMajor,
            $"DARLING_TEST_PGRUNTIME_PREVIOUS is PostgreSQL {previousMajor} but the package is {packageMajor}: move the " +
            "previous-release pins in new-upgraded-store-fixture.ps1 to what the last release shipped.");

        var root = Directory.CreateTempSubdirectory("darling-samemajor-");
        try
        {
            var deployment = Path.Combine(root.FullName, "deploy");
            var runtimeRoot = Path.Combine(deployment, "pg-runtime");
            Directory.CreateDirectory(deployment);
            CopyDirectory(Path.Combine(oldRuntime!, "pgsql"), Path.Combine(runtimeRoot, "pgsql"));

            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            var config = new PostgresConfig
            {
                Managed = true,
                Port = FindFreeTcpPort(),
                DataDirectory = dataDirectory,
            };

            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(20));

            /* ---- 1. The store as the previous release left it. A stamped host carries the identity of the
                    zip its runtime came from, which production writes at extraction; an unstamped one predates
                    stamping, and its first start on the new package goes through the no-stamp branch. ---- */
            var oldMajor = await BuildOldStoreAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);
            var oldTimescale = BundledTimescaleVersion(runtimeRoot);

            var newStamp = Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName);
            var legacyStamp = Path.Combine(runtimeRoot, DarlingStoreUpgrade.LegacyRuntimeStampFileName);
            if (stamped)
            {
                /* The state every 3.3-3.8 host is in: only the legacy stamp, holding that package's hash. Building
                   the store through this release's bootstrap leaves exactly that (the pin), and no stamp of this
                   release's own. */
                Assert.False(File.Exists(newStamp), "a 3.3-3.8 host has no stamp of this release's own");
                Assert.Equal(DarlingStoreUpgrade.LegacyRuntimePackageHash, File.ReadAllText(legacyStamp).Trim());
            }
            else
            {
                /* A host whose runtime predates stamping, or was staged by hand: no stamp at all, so the start
                   takes the no-stamp branch, which compares the runtimes themselves. */
                File.Delete(newStamp);
                File.Delete(legacyStamp);
            }

            var shippedZip = Path.Combine(deployment, "pg-runtime.zip");
            File.Copy(newZip!, shippedZip);

            var password = DarlingSecrets.Unprotect(
                File.ReadAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory)).Trim());
            /* Pooling=false: this connection string is used against a server this test itself stops and
               restarts (StopWithRuntimeAsync/StartWithRuntimeAsync below). A pooled Npgsql connection handed
               back on the same host/port/user after a restart is a physical socket opened against the OLD
               server's lifetime, which the stop already killed — its first use fails with a forcibly-closed
               connection. */
            var oldConnection = new NpgsqlConnectionStringBuilder(
                DarlingManagedPostgres.BuildConnectionString(config.Port, password)) { Pooling = false }.ConnectionString;

            /* ---- 2. Measure BEFORE, through the old binaries. ---- */
            await StartWithRuntimeAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);
            var before = await MeasureStoreAsync(oldConnection, timeout.Token);
            var beforeVersion = await ReadServerVersionAsync(oldConnection, timeout.Token);
            await StopWithRuntimeAsync(runtimeRoot, dataDirectory, timeout.Token);

            Assert.True(before.LogRows > 0, "the fixture must contain rows for the comparison to mean anything");
            Assert.True(before.CaggRows > 0, "the fixture must contain a materialized continuous aggregate");

            var packageVersion = DarlingStoreUpgrade.TryReadZipPostgresVersion(shippedZip);
            Assert.True(packageVersion != beforeVersion || BundledTimescaleVersionOfZip(shippedZip) != oldTimescale,
                $"the fixture must be a real runtime change; both sides are PostgreSQL {beforeVersion} + TimescaleDB {oldTimescale}");

            /* ---- 3. The REAL bootstrap, with a capturing logger replayed into every failure message. ---- */
            var log = new CapturingLogger();
            var managed = new DarlingManagedPostgres(config, log, runtimeRoot);

            string connectionString;
            try
            {
                connectionString = await managed.EnsureRunningAsync(timeout.Token);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"The same-major runtime swap threw: {ex.Message}\n\n--- orchestration log ---\n{log}", ex);
            }

            try
            {
                /* ---- 4. Same major, NEW binaries, data intact, extension moved with its library. ---- */
                var after = await MeasureStoreAsync(connectionString, timeout.Token);
                var afterVersion = await ReadServerVersionAsync(connectionString, timeout.Token);

                Assert.Equal(oldMajor, after.ServerMajor);
                Assert.True(afterVersion == packageVersion,
                    $"expected the store to run the package's PostgreSQL {packageVersion}, but it reports {afterVersion}." +
                    $"\n\n--- orchestration log ---\n{log}");
                Assert.Equal(before.LogRows, after.LogRows);
                Assert.Equal(before.PlanRows, after.PlanRows);
                Assert.Equal(before.PlanXmlLength, after.PlanXmlLength);
                Assert.Equal(before.LogChecksum, after.LogChecksum);
                Assert.Equal(before.CaggRows, after.CaggRows);
                Assert.Equal(before.CompressedChunks, after.CompressedChunks);
                Assert.Equal(BundledTimescaleVersion(runtimeRoot), after.TimescaleVersion);

                /* #3908: the extension moved before the store opened, and the data directory records it. */
                var bundledTimescale = BundledTimescaleVersion(runtimeRoot);
                if (!string.Equals(oldTimescale, bundledTimescale, StringComparison.Ordinal))
                {
                    Assert.Equal(DarlingStoreUpgrade.TimescaleUpdateStatus.Updated, managed.LastTimescaleOutcome.Status);
                    Assert.Equal(oldTimescale, managed.LastTimescaleOutcome.From);
                    Assert.Equal(bundledTimescale, managed.LastTimescaleOutcome.To);
                }

                Assert.Null(DarlingWorker.BuildStoreTimescaleReport(managed.LastTimescaleOutcome));
                Assert.Equal(
                    bundledTimescale,
                    File.ReadAllText(Path.Combine(dataDirectory, DarlingStoreUpgrade.TimescaleRecordFileName)).Trim());
                Assert.False(File.Exists(Path.Combine(dataDirectory, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName)));

                /* No pg_upgrade ran, so there is no retained pre-upgrade data directory. */
                Assert.False(
                    Directory.Exists(DarlingStoreUpgrade.RetainedDataDirectoryFor(dataDirectory, oldMajor)),
                    $"a same-major swap must not copy the data directory aside; that is pg_upgrade's rollback.\n\n--- orchestration log ---\n{log}");

                /* Each arm took the path it claims: the unstamped host compared the runtimes themselves. */
                if (!stamped)
                {
                    Assert.Contains("are both PostgreSQL", log.ToString(), StringComparison.Ordinal);
                    Assert.Contains("not the same runtime", log.ToString(), StringComparison.Ordinal);
                }

                Assert.Equal(DarlingStoreUpgrade.LegacyRuntimePackageHash, File.ReadAllText(legacyStamp).Trim());

                /* The previous runtime is rescued like any other swap, and the stamp now names the shipped zip,
                   so the next start adopts it instead of swapping again. */
                Assert.True(Directory.Exists(
                    Path.Combine(DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot), "pgsql")));
                Assert.Equal(
                    DarlingStoreUpgrade.ComputeFileHash(shippedZip),
                    File.ReadAllText(Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName)).Trim());

                var conf = await File.ReadAllTextAsync(Path.Combine(dataDirectory, "postgresql.conf"), timeout.Token);
                Assert.Contains("shared_preload_libraries = 'timescaledb'", conf, StringComparison.Ordinal);
                Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarker));
                Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV6));
                Assert.Equal(1, CountOccurrences(conf, DarlingManagedPostgres.ConfMarkerV7));
            }
            finally
            {
                await managed.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_KEEP")))
            {
                TryDeleteTree(root.FullName);
            }
        }
    }

    /// <summary>
    /// #3908's failure path, on real servers. The package is the current one minus its update script from the
    /// store's TimescaleDB, the way a broken build would ship. The runtime swaps (same major), the quiesced
    /// update fails, and NOTHING is reverted: the store opens on its own version through the library the runtime
    /// carries, every row intact, and the outcome says Failed for the alert. Then the next start finds a server
    /// on the update's private port with its marker, the way a service killed mid-update leaves it, and stops it
    /// rather than adopting it as the store. Gated like the same-major test.
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_SameMajor_AnUpdateThatCannotRun_OpensOnTheCarriedLibrary_Gated()
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_PREVIOUS");
        var newZip = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_NEWZIP");

        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime) || string.IsNullOrWhiteSpace(newZip),
            "Set DARLING_TEST_PGRUNTIME_PREVIOUS and DARLING_TEST_PGRUNTIME_NEWZIP; Darling\\tools\\new-upgraded-store-fixture.ps1 produces both.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(oldRuntime!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_PREVIOUS={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");
        Assert.SkipUnless(File.Exists(newZip!), $"DARLING_TEST_PGRUNTIME_NEWZIP={newZip} does not exist.");

        var root = Directory.CreateTempSubdirectory("darling-tsfail-");
        try
        {
            var deployment = Path.Combine(root.FullName, "deploy");
            var runtimeRoot = Path.Combine(deployment, "pg-runtime");
            Directory.CreateDirectory(deployment);
            CopyDirectory(Path.Combine(oldRuntime!, "pgsql"), Path.Combine(runtimeRoot, "pgsql"));

            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            var config = new PostgresConfig
            {
                Managed = true,
                Port = FindFreeTcpPort(),
                DataDirectory = dataDirectory,
            };

            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(20));

            var oldMajor = await BuildOldStoreAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);
            var oldTimescale = BundledTimescaleVersion(runtimeRoot);
            var bundled = BundledTimescaleVersionOfZip(newZip!);
            Assert.NotNull(oldTimescale);
            Assert.True(bundled is not null && bundled != oldTimescale,
                $"the pair must move TimescaleDB for an update to be able to fail; both are {oldTimescale}");
            Assert.Contains(oldTimescale!, DarlingStoreUpgrade.TryReadZipTimescaleLibraryVersions(newZip!));

            /* The package with the one script the update needs taken out. */
            var shippedZip = Path.Combine(deployment, "pg-runtime.zip");
            File.Copy(newZip!, shippedZip);
            using (var archive = ZipFile.Open(shippedZip, ZipArchiveMode.Update))
            {
                var script = archive.GetEntry($"pgsql/share/extension/timescaledb--{oldTimescale}--{bundled}.sql");
                Assert.NotNull(script);
                script!.Delete();
            }

            var password = DarlingSecrets.Unprotect(
                File.ReadAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory)).Trim());
            /* Pooling=false: this connection string is used against a server this test itself stops and
               restarts (StopWithRuntimeAsync/StartWithRuntimeAsync below). A pooled Npgsql connection handed
               back on the same host/port/user after a restart is a physical socket opened against the OLD
               server's lifetime, which the stop already killed — its first use fails with a forcibly-closed
               connection. */
            var oldConnection = new NpgsqlConnectionStringBuilder(
                DarlingManagedPostgres.BuildConnectionString(config.Port, password)) { Pooling = false }.ConnectionString;

            await StartWithRuntimeAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);
            var before = await MeasureStoreAsync(oldConnection, timeout.Token);
            await StopWithRuntimeAsync(runtimeRoot, dataDirectory, timeout.Token);

            var packageVersion = DarlingStoreUpgrade.TryReadZipPostgresVersion(shippedZip);
            var log = new CapturingLogger();
            var managed = new DarlingManagedPostgres(config, log, runtimeRoot);

            string connectionString;
            try
            {
                connectionString = await managed.EnsureRunningAsync(timeout.Token);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"A failed TimescaleDB update must not fail the start: {ex.Message}\n\n--- log ---\n{log}", ex);
            }

            try
            {
                var after = await MeasureStoreAsync(connectionString, timeout.Token);
                Assert.Equal(oldMajor, after.ServerMajor);
                Assert.Equal(before.LogRows, after.LogRows);
                Assert.Equal(before.LogChecksum, after.LogChecksum);
                Assert.Equal(before.CaggRows, after.CaggRows);
                Assert.Equal(before.CompressedChunks, after.CompressedChunks);

                /* On its own extension, through the carried library, on the NEW binaries: nothing reverted. */
                Assert.Equal(oldTimescale, after.TimescaleVersion);
                Assert.Equal(packageVersion, await ReadServerVersionAsync(connectionString, timeout.Token));
                Assert.False(File.Exists(Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeBlockedFileName)),
                    $"a failed extension update is not a bad package; nothing may block it.\n\n--- log ---\n{log}");

                Assert.Equal(DarlingStoreUpgrade.TimescaleUpdateStatus.Failed, managed.LastTimescaleOutcome.Status);
                Assert.Equal(oldTimescale, managed.LastTimescaleOutcome.From);
                Assert.Equal(bundled, managed.LastTimescaleOutcome.To);
                Assert.NotNull(DarlingWorker.BuildStoreTimescaleReport(managed.LastTimescaleOutcome));
                Assert.Equal(oldTimescale, File.ReadAllText(Path.Combine(dataDirectory, DarlingStoreUpgrade.TimescaleRecordFileName)).Trim());
                Assert.False(File.Exists(Path.Combine(dataDirectory, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName)));
            }
            finally
            {
                await managed.StopIfStartedByThisProcessAsync();
            }

            /* ---- The next start, with the update's own server left on its private port. ---- */
            var privatePort = FindFreeTcpPort();
            await StartWithRuntimeAsync(runtimeRoot, dataDirectory, privatePort, timeout.Token);
            File.WriteAllText(
                Path.Combine(dataDirectory, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName),
                privatePort.ToString(CultureInfo.InvariantCulture));

            var nextLog = new CapturingLogger();
            var next = new DarlingManagedPostgres(config, nextLog, runtimeRoot);
            try
            {
                var nextConnection = await next.EnsureRunningAsync(timeout.Token);
                Assert.Equal(before.LogRows, (await MeasureStoreAsync(nextConnection, timeout.Token)).LogRows);
                Assert.Contains("Found the store on private port", nextLog.ToString(), StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dataDirectory, DarlingStoreUpgrade.QuiescedUpdateMarkerFileName)));
                Assert.Equal(config.Port.ToString(CultureInfo.InvariantCulture), DarlingStoreUpgrade.TryReadPostmasterPort(dataDirectory));
            }
            finally
            {
                await next.StopIfStartedByThisProcessAsync();
            }
        }
        finally
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_KEEP")))
            {
                TryDeleteTree(root.FullName);
            }
        }
    }

    /// <summary>
    /// The DARLING01 incident (#1738), reproduced with REAL packages: an 18 store, an 18 runtime extracted,
    /// and a PostgreSQL 17 zip dropped beside the service. Before the guard the runtime was swapped because
    /// the majors merely DIFFERED, and the store was down about seven minutes until the previous runtime was
    /// restored by hand.
    ///
    /// <para>Gated on the SAME two variables as the upgrade fixture, so it costs nothing extra in CI: the
    /// "older package" is built by zipping the old-major runtime tree, which produces a genuine archive whose
    /// <c>pgsql/bin/pg_ctl.exe</c> version resource reads 17 — the exact thing
    /// <see cref="DarlingStoreUpgrade.TryReadZipPostgresMajor"/> inspects.</para>
    /// </summary>
    [Fact]
    public async Task RuntimeAdvance_RefusesAPackageOlderThanTheStore_AndLeavesTheRuntimeAlone_Gated()
    {
        var oldRuntime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_OLD");
        var newZip = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME_NEWZIP");

        Assert.SkipWhen(string.IsNullOrWhiteSpace(oldRuntime) || string.IsNullOrWhiteSpace(newZip),
            "Set DARLING_TEST_PGRUNTIME_OLD and DARLING_TEST_PGRUNTIME_NEWZIP (see new-upgraded-store-fixture.ps1).");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(oldRuntime!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME_OLD={oldRuntime} does not contain pgsql\\bin\\pg_ctl.exe.");
        Assert.SkipUnless(File.Exists(newZip!), $"DARLING_TEST_PGRUNTIME_NEWZIP={newZip} does not exist.");

        var root = Directory.CreateTempSubdirectory("darling-downgrade-");
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(10));

            /* The host as DARLING01 was: the NEW major extracted and working. */
            var deployment = Path.Combine(root.FullName, "deploy");
            var runtimeRoot = Path.Combine(deployment, "pg-runtime");
            Directory.CreateDirectory(deployment);
            using (var archive = ZipFile.OpenRead(newZip!))
            {
                archive.ExtractToDirectory(runtimeRoot);
            }

            var newMajor = DarlingStoreUpgrade.TryReadZipPostgresMajor(newZip!);
            Assert.NotNull(newMajor);

            /* A data directory on that same new major - the store's own authority on what it needs. */
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(dataDirectory);
            await File.WriteAllTextAsync(
                Path.Combine(dataDirectory, "PG_VERSION"),
                newMajor!.Value.ToString(CultureInfo.InvariantCulture) + "\n", timeout.Token);

            /* The bad package: a REAL older-major zip, rooted at pgsql/ exactly as the shipped one is. */
            var olderPackage = Path.Combine(deployment, "pg-runtime.zip");
            ZipFile.CreateFromDirectory(
                Path.Combine(oldRuntime!, "pgsql"), olderPackage, CompressionLevel.NoCompression, includeBaseDirectory: true);
            var packageMajor = DarlingStoreUpgrade.TryReadZipPostgresMajor(olderPackage);
            Assert.True(packageMajor < newMajor, $"the fixture must be a downgrade; got package {packageMajor} vs store {newMajor}");

            var log = new CapturingLogger();
            var advance = await new DarlingStoreUpgrade(log).TryAdvanceRuntimeAsync(
                runtimeRoot, olderPackage, dataDirectory,
                /* nothing is running in this fixture */ (_, _) => Task.FromResult(false),
                timeout.Token);

            /* REFUSED: no swap, no rescue directory, and the live runtime untouched. */
            Assert.False(advance.Swapped);
            Assert.Null(advance.PreviousBinDirectory);
            Assert.False(Directory.Exists(DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot)),
                "a refused package must not rescue anything — nothing is being replaced");
            Assert.True(File.Exists(Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe")),
                "the working runtime must still be in place; this is the store-down failure #1738 filed");

            /* And it must SAY so, at Critical, naming both majors. */
            var text = log.ToString();
            Assert.Contains("REFUSING the shipped Postgres runtime", text, StringComparison.Ordinal);
            Assert.Contains("[Critical]", text, StringComparison.Ordinal);

            /* The stamp is deliberately NOT written: a refusal that goes quiet on the next start is one
               nobody acts on, and the operator still has the wrong zip beside the service. */
            var stampPath = Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName);
            Assert.False(File.Exists(stampPath),
                "recording the stamp would silence this refusal on every subsequent start");

            /* Now the same package against a STAMPED host — which, after this release, is every host. The
               stamp says only that the zip CHANGED, never which way, so a stamped host handed an older
               package walks past the no-stamp branch and into the swap unless the direction check sits
               OUTSIDE that branch. Source placement is pinned above; this proves the behaviour, and it
               costs nothing because the refusal returns before any extraction. */
            const string PriorPackageHash = "0000000000000000000000000000000000000000000000000000000000000000";
            await File.WriteAllTextAsync(stampPath, PriorPackageHash, timeout.Token);

            var stampedLog = new CapturingLogger();
            var stamped = await new DarlingStoreUpgrade(stampedLog).TryAdvanceRuntimeAsync(
                runtimeRoot, olderPackage, dataDirectory,
                (_, _) => Task.FromResult(false),
                timeout.Token);

            Assert.False(stamped.Swapped);
            Assert.Null(stamped.PreviousBinDirectory);
            Assert.False(Directory.Exists(DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot)),
                "a stamped host must refuse the package an unstamped one refused — the stamp is not a direction");
            Assert.True(File.Exists(Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe")),
                "the working runtime must still be in place on a stamped host too");
            Assert.Contains("REFUSING the shipped Postgres runtime", stampedLog.ToString(), StringComparison.Ordinal);

            /* The existing stamp is left as it was. Overwriting it with the refused package's hash would
               read as "already seen" on the next start and go quiet with the wrong zip still sitting
               beside the service. */
            Assert.Equal(PriorPackageHash, (await File.ReadAllTextAsync(stampPath, timeout.Token)).Trim());
        }
        finally
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable("DARLING_TEST_KEEP")))
            {
                TryDeleteTree(root.FullName);
            }
        }
    }

    /* ---------------- fixture construction ---------------- */

    /// <summary>
    /// Creates the pre-upgrade store with the OLD runtime: a real initdb + managed conf, TimescaleDB, a
    /// hypertable of collector rows, a second hypertable carrying TOAST-sized plan XML (the blob shape the
    /// PostgreSQL 18 move is FOR), a continuous aggregate, and a compressed chunk. Returns its major.
    /// </summary>
    private static async Task<int> BuildOldStoreAsync(
        string runtimeRoot, string dataDirectory, int port, CancellationToken cancellationToken)
    {
        var config = new PostgresConfig { Managed = true, Port = port, DataDirectory = dataDirectory };

        /* The product's own first-run path builds the cluster, so the fixture is a store this service
           really would have created — not a hand-rolled approximation of one. */
        var bootstrap = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        var connectionString = await bootstrap.EnsureRunningAsync(cancellationToken);

        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var major = await ReadServerMajorAsync(connection, cancellationToken);

            /* #3908: the store the product builds, not an approximation. The migrations, then TimescaleDB where
               the product puts it (the collect schema), then the worker's own store-object convergence list:
               72 hypertables, the continuous aggregates, and every refresh, compression and retention policy,
               created under this runtime's TimescaleDB. An upgrade that breaks a policy the product owns is only
               visible on a store that has them. */
            await BuildProductStoreObjectsAsync(connectionString, cancellationToken);

            /* The data-bearing fixtures sit in their own schema beside the product's tables, so they neither
               collide with collect's real ones nor depend on their columns. */
            await ExecuteAsync(connection, "CREATE SCHEMA IF NOT EXISTS fixture", cancellationToken);

            await ExecuteAsync(
                connection,
                """
                CREATE TABLE fixture.collection_log (
                    collection_time timestamptz NOT NULL,
                    server_id       integer      NOT NULL,
                    collector_name  text         NOT NULL,
                    status          text         NOT NULL,
                    rows_collected  bigint       NOT NULL
                )
                """,
                cancellationToken);
            await ExecuteAsync(
                connection,
                "SELECT create_hypertable('fixture.collection_log', by_range('collection_time'))",
                cancellationToken);

            await ExecuteAsync(
                connection,
                """
                CREATE TABLE fixture.query_plans (
                    collection_time timestamptz NOT NULL,
                    server_id       integer      NOT NULL,
                    query_hash      text         NOT NULL,
                    plan_xml        text         NOT NULL
                )
                """,
                cancellationToken);
            await ExecuteAsync(
                connection,
                "SELECT create_hypertable('fixture.query_plans', by_range('collection_time'))",
                cancellationToken);

            /* Collector rows across several days so the hypertable really has multiple chunks. */
            await ExecuteAsync(
                connection,
                """
                INSERT INTO fixture.collection_log (collection_time, server_id, collector_name, status, rows_collected)
                SELECT now() - (n || ' minutes')::interval,
                       1 + (n % 4),
                       'collector_' || (n % 7),
                       CASE WHEN n % 23 = 0 THEN 'ERROR' ELSE 'SUCCESS' END,
                       n * 3
                FROM generate_series(1, 20000) AS g(n)
                """,
                cancellationToken);

            /* TOAST-sized plan XML: the large-blob shape whose compression is the reason for the move. */
            await ExecuteAsync(
                connection,
                """
                INSERT INTO fixture.query_plans (collection_time, server_id, query_hash, plan_xml)
                SELECT now() - (n || ' hours')::interval,
                       1 + (n % 4),
                       md5(n::text),
                       '<ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan">'
                       || repeat('<RelOp NodeId="' || n || '" PhysicalOp="Clustered Index Scan" LogicalOp="Index Scan" EstimateRows="1234.5"><OutputList><ColumnReference Database="[X]" Schema="[dbo]" Table="[T]" Column="C' || n || '" /></OutputList></RelOp>', 400)
                       || '</ShowPlanXML>'
                FROM generate_series(1, 500) AS g(n)
                """,
                cancellationToken);

            /* A continuous aggregate: its own materialization hypertable, catalog entries and refresh
               policy all have to survive pg_upgrade for the store to still work. */
            await ExecuteAsync(
                connection,
                """
                CREATE MATERIALIZED VIEW fixture.collection_hourly
                WITH (timescaledb.continuous) AS
                SELECT time_bucket('1 hour', collection_time) AS bucket,
                       server_id,
                       count(*)             AS runs,
                       sum(rows_collected)  AS rows_collected
                FROM fixture.collection_log
                GROUP BY 1, 2
                """,
                cancellationToken);

            /* Compression on the blob hypertable, then actually compress its chunks — a compressed chunk
               is a different on-disk shape, and "the upgrade kept the rows" has to hold for those too. */
            await ExecuteAsync(
                connection,
                "ALTER TABLE fixture.query_plans SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')",
                cancellationToken);
            await ExecuteAsync(
                connection,
                "SELECT compress_chunk(c) FROM show_chunks('fixture.query_plans') AS c",
                cancellationToken);

            return major;
        }
        finally
        {
            await bootstrap.StopIfStartedByThisProcessAsync();
        }
    }

    /// <summary>
    /// The product's own store objects, built the way the worker's start path builds them (#3908): the migrations,
    /// TimescaleDB enabled where the product enables it, then <see cref="DarlingWorker.StoreObjectConvergence"/>
    /// in its order. Every step has to succeed: a fixture missing a policy would let an upgrade that breaks it
    /// pass.
    /// </summary>
    private static async Task BuildProductStoreObjectsAsync(string ownerConnectionString, CancellationToken cancellationToken)
    {
        var connectionString = new NpgsqlConnectionStringBuilder(ownerConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
            Pooling = false,
            CommandTimeout = 600,
        }.ConnectionString;

        await using (var migrations = new NpgsqlConnection(connectionString))
        {
            await migrations.OpenAsync(cancellationToken);
            await PgMigrations.MigrateAsync(migrations, NullLogger.Instance, cancellationToken);
        }

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, NullLogger.Instance, cancellationToken),
            "TimescaleDB could not be enabled on the fixture store.");

        foreach (var step in DarlingWorker.StoreObjectConvergence)
        {
            await step.EnsureAsync(connection, NullLogger.Instance, cancellationToken);
        }
    }

    /* ---------------- measurement ---------------- */

    private sealed record StoreSnapshot(
        int ServerMajor,
        string? TimescaleVersion,
        long LogRows,
        long PlanRows,
        long PlanXmlLength,
        string LogChecksum,
        long CaggRows,
        long CompressedChunks,
        long Hypertables,
        long ContinuousAggregates,
        long Policies);

    /// <summary>
    /// The store's observable content, read identically before and after the upgrade. The checksum is an
    /// aggregate over every row's values, so a silently truncated or reordered restore fails the comparison
    /// even when the row COUNT happens to match.
    /// </summary>
    private static async Task<StoreSnapshot> MeasureStoreAsync(string connectionString, CancellationToken cancellationToken)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);

        var major = await ReadServerMajorAsync(connection, cancellationToken);
        var timescale = await ScalarAsync<string>(
            connection, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'", cancellationToken);
        var logRows = await ScalarLongAsync(connection, "SELECT count(*) FROM fixture.collection_log", cancellationToken);
        var planRows = await ScalarLongAsync(connection, "SELECT count(*) FROM fixture.query_plans", cancellationToken);
        var planLength = await ScalarLongAsync(
            connection, "SELECT COALESCE(sum(length(plan_xml)), 0) FROM fixture.query_plans", cancellationToken);
        var checksum = await ScalarAsync<string>(
            connection,
            """
            SELECT md5(string_agg(
                       collection_time::text || '|' || server_id || '|' || collector_name || '|' || status || '|' || rows_collected,
                       ',' ORDER BY collection_time, server_id, collector_name))
            FROM fixture.collection_log
            """,
            cancellationToken);
        var caggRows = await ScalarLongAsync(connection, "SELECT count(*) FROM fixture.collection_hourly", cancellationToken);
        var compressed = await ScalarLongAsync(
            connection,
            "SELECT count(*) FROM timescaledb_information.chunks WHERE is_compressed",
            cancellationToken);

        var hypertables = await ScalarLongAsync(connection, "SELECT count(*) FROM timescaledb_information.hypertables", cancellationToken);
        var aggregates = await ScalarLongAsync(connection, "SELECT count(*) FROM timescaledb_information.continuous_aggregates", cancellationToken);
        /* User jobs only: job_id 1 is TimescaleDB's own telemetry job. */
        var policies = await ScalarLongAsync(connection, "SELECT count(*) FROM timescaledb_information.jobs WHERE job_id >= 1000", cancellationToken);

        return new StoreSnapshot(
            major, timescale, logRows, planRows, planLength, checksum ?? string.Empty, caggRows, compressed, hypertables, aggregates, policies);
    }

    private static async Task<int> ReadServerMajorAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        var raw = await ScalarAsync<string>(connection, "SHOW server_version_num", cancellationToken);
        return int.TryParse(raw, NumberStyles.None, CultureInfo.InvariantCulture, out var num) ? num / 10000 : 0;
    }

    private static string? BundledTimescaleVersion(string runtimeRoot)
    {
        var control = Path.Combine(runtimeRoot, "pgsql", "share", "extension", "timescaledb.control");
        return File.Exists(control)
            ? DarlingStoreUpgrade.ParseTimescaleDefaultVersion(File.ReadAllText(control))
            : null;
    }

    private static string? BundledTimescaleVersionOfZip(string zipPath)
    {
        using var archive = ZipFile.OpenRead(zipPath);
        var control = archive.GetEntry("pgsql/share/extension/timescaledb.control");
        if (control is null)
        {
            return null;
        }

        using var reader = new StreamReader(control.Open());
        return DarlingStoreUpgrade.ParseTimescaleDefaultVersion(reader.ReadToEnd());
    }

    /// <summary>The PostgreSQL major of an extracted runtime, from its pg_ctl.exe version resource.</summary>
    private static int? OldRuntimeMajor(string runtimeRoot)
    {
        var info = System.Diagnostics.FileVersionInfo.GetVersionInfo(Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe"));
        return info.FileMajorPart > 0 ? info.FileMajorPart : DarlingStoreUpgrade.ParsePostgresMajor(info.ProductVersion ?? info.FileVersion);
    }

    /// <summary>The server's full version (18.6), read the way the runtime check parses a version line.</summary>
    private static async Task<Version?> ReadServerVersionAsync(string connectionString, CancellationToken cancellationToken)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString) { Pooling = false };
        await using var connection = new NpgsqlConnection(builder.ConnectionString);
        await connection.OpenAsync(cancellationToken);
        return DarlingStoreUpgrade.ParsePostgresVersion(await ScalarAsync<string>(connection, "SHOW server_version", cancellationToken));
    }

    /* ---------------- plumbing ---------------- */

    private static async Task StartWithRuntimeAsync(
        string runtimeRoot, string dataDirectory, int port, CancellationToken cancellationToken)
    {
        var exitCode = await DarlingManagedPostgres.RunDetachingToolAsync(
            Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe"),
            $"-D \"{dataDirectory}\" -o \"-p {port} -c listen_addresses=127.0.0.1\" -w -t 120 start",
            TimeSpan.FromMinutes(3),
            cancellationToken);
        Assert.Equal(0, exitCode);
    }

    private static async Task StopWithRuntimeAsync(string runtimeRoot, string dataDirectory, CancellationToken cancellationToken)
    {
        var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(
            Path.Combine(runtimeRoot, "pgsql", "bin", "pg_ctl.exe"),
            $"stop -D \"{dataDirectory}\" -m fast -w -t 120",
            TimeSpan.FromMinutes(3),
            cancellationToken);
        Assert.True(exitCode == 0, $"pg_ctl stop failed: {output}");
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static async Task<T?> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
        where T : class
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        return await command.ExecuteScalarAsync(cancellationToken) as T;
    }

    private static async Task<long> ScalarLongAsync(NpgsqlConnection connection, string sql, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken) ?? 0L, CultureInfo.InvariantCulture);
    }

    private static int CountOccurrences(string haystack, string needle)
    {
        var count = 0;
        var index = 0;
        while ((index = haystack.IndexOf(needle, index, StringComparison.Ordinal)) >= 0)
        {
            count++;
            index += needle.Length;
        }

        return count;
    }

    /* ==================== #1770: the retained-copy sweep reaches every sibling ==================== */

    /// <summary>
    /// The sweep ages out EVERY retained copy, not only the one this run's upgrade produced. Two copies are
    /// planted before the service ever starts — the shape a host reaches after upgrading more than once — and
    /// two starts later both are gone.
    /// </summary>
    [Fact]
    public void Sweep_AgesOutEveryRetainedSibling_NotOnlyOne()
    {
        var root = Directory.CreateTempSubdirectory("darling-sweep-all-");
        try
        {
            var dataDirectory = PlantLiveDataDirectory(root.FullName);
            var from17 = PlantRetainedCopy(dataDirectory, 17);
            var from16 = PlantRetainedCopy(dataDirectory, 16);

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);

            /* First start: both are kept, and each says so — the countdown is per copy. */
            upgrade.SweepRetainedDataDirectories(dataDirectory);
            Assert.True(Directory.Exists(from17));
            Assert.True(Directory.Exists(from16));

            /* Second start: both have now survived RollbackRetentionStarts and go. */
            upgrade.SweepRetainedDataDirectories(dataDirectory);
            Assert.False(Directory.Exists(from17));
            Assert.False(Directory.Exists(from16));

            /* ...each named in its own line, with what it gave back. */
            Assert.Equal(2, CountOccurrences(log.ToString(), "Deleted the pre-upgrade store data directory"));
            Assert.Contains(from17, log.ToString(), StringComparison.Ordinal);
            Assert.Contains(from16, log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// THE #1770 pin. One retained copy that cannot be deleted — here a file held open with no sharing, in
    /// the field an antivirus scan or a postmaster that has not finished exiting — must cost only itself.
    ///
    /// <para>With the failure handling outside the loop, the throw abandoned the sweep, so the OTHER copy
    /// survived too, its counter never advanced, and it survived every subsequent start for as long as the
    /// lock lasted. That is the mechanism by which multi-GB directories pile up unnoticed. Restore the
    /// handler to the outer scope and this goes red on the healthy copy, which is the one assertion that
    /// distinguishes the fix from the bug.</para>
    /// </summary>
    [Fact]
    public void Sweep_ARetainedCopyItCannotDelete_DoesNotStopTheOthers()
    {
        var root = Directory.CreateTempSubdirectory("darling-sweep-locked-");
        try
        {
            var dataDirectory = PlantLiveDataDirectory(root.FullName);

            /* Ordinal order matters: the locked copy must be swept BEFORE the healthy one, or an
               abort-on-first-failure bug would still leave the healthy one deleted and pass. */
            var locked = PlantRetainedCopy(dataDirectory, 16);
            var healthy = PlantRetainedCopy(dataDirectory, 17);

            using var hold = new FileStream(
                Path.Combine(locked, "PG_VERSION"), FileMode.Open, FileAccess.Read, FileShare.None);

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            upgrade.SweepRetainedDataDirectories(dataDirectory);
            upgrade.SweepRetainedDataDirectories(dataDirectory);

            /* The healthy copy aged out on schedule despite its sibling failing... */
            Assert.False(Directory.Exists(healthy));

            /* ...the locked one is still there, reported by name rather than in silence... */
            Assert.True(Directory.Exists(locked));
            Assert.Contains("Could not age out the retained pre-upgrade store data directory", log.ToString(), StringComparison.Ordinal);
            Assert.Contains(locked, log.ToString(), StringComparison.Ordinal);

            /* ...and the failure did not swallow the other copy's own outcome. */
            Assert.Contains("Deleted the pre-upgrade store data directory", log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The scope pin, and the #1770 decision it encodes. A store copy this service did not create — the
    /// field instance's seven were named <c>_rollback_manual_*</c> and hand-made across a week of upgrade
    /// rehearsals, by which time nothing in the product had ever produced such a name — is REPORTED with its
    /// size and left exactly where it is.
    ///
    /// <para>Deleting it would be the product silently reversing a decision a person made, on a directory it
    /// cannot know the purpose of; the sweep's delete stays scoped to the names
    /// <see cref="DarlingStoreUpgrade.RetainedDataDirectoryFor"/> produces, and nothing else in the parent is
    /// eligible however store-shaped it looks.</para>
    /// </summary>
    [Fact]
    public void Sweep_ReportsAStoreCopyItDidNotCreate_AndNeverDeletesIt()
    {
        var root = Directory.CreateTempSubdirectory("darling-sweep-manual-");
        try
        {
            var dataDirectory = PlantLiveDataDirectory(root.FullName);
            var ours = PlantRetainedCopy(dataDirectory, 17);

            /* A hand-made copy, in the shape the field reported. */
            var manual = dataDirectory + "_rollback_manual_20260720";
            Directory.CreateDirectory(manual);
            File.WriteAllText(Path.Combine(manual, "PG_VERSION"), "17\n");
            File.WriteAllText(Path.Combine(manual, "postgresql.conf"), new string('x', 4096));

            /* ...and a neighbour that is not a store at all, which must never be mentioned. */
            var notAStore = Path.Combine(root.FullName, "logs");
            Directory.CreateDirectory(notAStore);
            File.WriteAllText(Path.Combine(notAStore, "darling-service.log"), "hello");

            var log = new CapturingLogger();
            var upgrade = new DarlingStoreUpgrade(log);
            upgrade.SweepRetainedDataDirectories(dataDirectory);
            upgrade.SweepRetainedDataDirectories(dataDirectory);

            /* Ours aged out; theirs did not, and neither did the live data directory. */
            Assert.False(Directory.Exists(ours));
            Assert.True(Directory.Exists(manual));
            Assert.True(Directory.Exists(dataDirectory));

            /* It was reported rather than left to be discovered when the volume filled. */
            Assert.Contains("are not part of the running store", log.ToString(), StringComparison.Ordinal);
            Assert.Contains("Store data directory this service did not create: " + manual, log.ToString(), StringComparison.Ordinal);

            /* A directory with no PG_VERSION is not a store copy and is never named. */
            Assert.DoesNotContain(notAStore, log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// A half-built cluster left by an interrupted upgrade is OURS, and the report has to say so — telling
    /// an operator that the product did not create a directory the product plainly created is how a
    /// diagnostic stops being believed. It is still not deleted, and the line says why: the commit point is
    /// two directory moves, so a process that died between them leaves the UPGRADED cluster under this name.
    /// </summary>
    [Fact]
    public void Sweep_NamesAnInterruptedUpgradeLeftoverAsOurs_NotAsAStrangers()
    {
        var root = Directory.CreateTempSubdirectory("darling-sweep-leftover-");
        try
        {
            var dataDirectory = PlantLiveDataDirectory(root.FullName);

            /* What UpgradeDataDirectoryAsync builds and only best-effort deletes. */
            var leftover = dataDirectory + "-upgrade-18";
            Directory.CreateDirectory(leftover);
            File.WriteAllText(Path.Combine(leftover, "PG_VERSION"), "18\n");

            var log = new CapturingLogger();
            new DarlingStoreUpgrade(log).SweepRetainedDataDirectories(dataDirectory);

            Assert.True(Directory.Exists(leftover));
            Assert.Contains("Leftover store data directory from an interrupted upgrade: " + leftover,
                log.ToString(), StringComparison.Ordinal);

            /* ...and it must NOT be blamed on someone else. */
            Assert.DoesNotContain("this service did not create: " + leftover, log.ToString(), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// NO directory naming the upgrade orchestration can create beside the data directory is ever reported as
    /// something the product did not create. The report decides foreign-ness by ELIMINATION — store-shaped and
    /// not one of ours — so a sibling naming that nobody remembered to register does not fail loudly, it
    /// quietly tells an operator the product did not create a directory the product created. That is the one
    /// way this diagnostic can lie, so every naming is planted here at once.
    ///
    /// <para>The runtime namings are planted too, and for the opposite reason: they must be reported as
    /// NOTHING, because they hold binaries rather than a cluster and the structural <c>PG_VERSION</c> test
    /// should never reach them. Planting them proves that claim instead of assuming it.</para>
    /// </summary>
    [Fact]
    public void Sweep_NeverCallsAProductOwnedSiblingForeign()
    {
        var root = Directory.CreateTempSubdirectory("darling-sweep-owned-");
        try
        {
            var dataDirectory = PlantLiveDataDirectory(root.FullName);

            /* Every naming this class can put beside the data directory. */
            var retained = PlantRetainedCopy(dataDirectory, 17);
            var staging = dataDirectory + DarlingStoreUpgrade.UpgradeStagingDirectorySuffix + "18";
            Directory.CreateDirectory(staging);
            File.WriteAllText(Path.Combine(staging, "PG_VERSION"), "18\n");

            /* ...and the runtime namings, which carry binaries and must stay invisible to a PG_VERSION test. */
            var runtimeRoot = Path.Combine(root.FullName, "pg-runtime");
            var previousRuntime = DarlingStoreUpgrade.PreviousRuntimeRootFor(runtimeRoot);
            var failedRuntime = Path.Combine(runtimeRoot, "pgsql") + ".failed";
            foreach (var directory in new[] { Path.Combine(runtimeRoot, "pgsql", "bin"), Path.Combine(previousRuntime, "pgsql", "bin"), failedRuntime })
            {
                Directory.CreateDirectory(directory);
                File.WriteAllText(Path.Combine(directory, "pg_ctl.exe"), "binary");
            }

            var log = new CapturingLogger();
            new DarlingStoreUpgrade(log).SweepRetainedDataDirectories(dataDirectory);
            var lines = log.ToString();

            /* The retained copy is the sweep's own business and never reaches the report; the staging cluster
               is reported, but AS OURS. */
            Assert.DoesNotContain("did not create: " + retained, lines, StringComparison.Ordinal);
            Assert.DoesNotContain("did not create: " + staging, lines, StringComparison.Ordinal);
            Assert.Contains("Leftover store data directory from an interrupted upgrade: " + staging, lines, StringComparison.Ordinal);

            /* No runtime directory is mentioned at all — none of them is a cluster. */
            Assert.DoesNotContain(previousRuntime, lines, StringComparison.Ordinal);
            Assert.DoesNotContain(failedRuntime, lines, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteTree(root.FullName);
        }
    }

    /// <summary>
    /// The two data-directory sibling suffixes are DEFINED once and used everywhere, so the enumeration the
    /// report relies on cannot drift from the names the upgrade actually creates. Source-parsed, because the
    /// staging name is computed inside the upgrade's orchestration where a unit test cannot reach it — the
    /// point is that no second copy of either literal exists to fall out of step.
    /// </summary>
    [Fact]
    public void SiblingDirectorySuffixes_HaveExactlyOneDefinitionEach()
    {
        var source = ReadUpgradeSource();

        Assert.Equal("-old-", DarlingStoreUpgrade.RetainedDataDirectorySuffix);
        Assert.Equal("-upgrade-", DarlingStoreUpgrade.UpgradeStagingDirectorySuffix);

        /* One occurrence each: the const declaration. Every other site names the constant. */
        Assert.Equal(1, CountOccurrences(source, "\"-old-\""));
        Assert.Equal(1, CountOccurrences(source, "\"-upgrade-\""));

        /* ...and the staging directory the upgrade builds into is one of those sites. */
        Assert.Contains("+ UpgradeStagingDirectorySuffix + context.NewMajor", source, StringComparison.Ordinal);
        Assert.Contains("+ RetainedDataDirectorySuffix + oldMajor", source, StringComparison.Ordinal);
    }

    /// <summary>A live data directory with the one file that makes a directory a cluster.</summary>
    private static string PlantLiveDataDirectory(string root)
    {
        var dataDirectory = Path.Combine(root, "pg");
        Directory.CreateDirectory(dataDirectory);
        File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "18\n");
        return dataDirectory;
    }

    /// <summary>A retained pre-upgrade copy, named exactly as the upgrade names one.</summary>
    private static string PlantRetainedCopy(string dataDirectory, int oldMajor)
    {
        var retained = DarlingStoreUpgrade.RetainedDataDirectoryFor(dataDirectory, oldMajor);
        Directory.CreateDirectory(retained);
        File.WriteAllText(
            Path.Combine(retained, "PG_VERSION"), oldMajor.ToString(CultureInfo.InvariantCulture) + "\n");
        return retained;
    }

    private static void CopyDirectory(string source, string destination)
    {
        Directory.CreateDirectory(destination);
        foreach (var directory in Directory.GetDirectories(source, "*", SearchOption.AllDirectories))
        {
            Directory.CreateDirectory(directory.Replace(source, destination, StringComparison.Ordinal));
        }

        foreach (var file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
        {
            File.Copy(file, file.Replace(source, destination, StringComparison.Ordinal), overwrite: true);
        }
    }

    private static void TryDeleteTree(string path)
    {
        try
        {
            Directory.Delete(path, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* A leftover temp tree is not a test failure. */
        }
    }

    /// <summary>
    /// Keeps every line the orchestration logs so a failed gated run explains itself. The store upgrade
    /// reports its failed STEP and the underlying error through <see cref="ILogger"/> and nowhere else, so
    /// without this a red run is an unexplained stack trace.
    /// </summary>
    private sealed class CapturingLogger : Microsoft.Extensions.Logging.ILogger
    {
        private readonly System.Text.StringBuilder _lines = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(Microsoft.Extensions.Logging.LogLevel logLevel) => true;

        public void Log<TState>(
            Microsoft.Extensions.Logging.LogLevel logLevel,
            Microsoft.Extensions.Logging.EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_lines)
            {
                _lines.Append('[').Append(logLevel).Append("] ").AppendLine(formatter(state, exception));
                if (exception is not null)
                {
                    _lines.Append("    ").AppendLine(exception.Message);
                }
            }
        }

        public override string ToString()
        {
            lock (_lines)
            {
                return _lines.ToString();
            }
        }
    }

    private static int FindFreeTcpPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }
}
