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
    public void BuildStoreUpgradeReport_CleanSuccessCarriesNoWarning_AndExtensionOnlyIsLogOnly()
    {
        var clean = DarlingWorker.BuildStoreUpgradeReport(new DarlingStoreUpgrade.StoreUpgradeOutcome(
            DarlingStoreUpgrade.StoreUpgradeStatus.Succeeded, 17, 18, "2.28.1", "2.28.1", null, null, false));
        Assert.NotNull(clean);
        Assert.Null(clean!.FailureMessage);

        /* An extension-only update is routine maintenance the log already records — not something to page
           about, so it maps to no report at all. */
        Assert.Null(DarlingWorker.BuildStoreUpgradeReport(new DarlingStoreUpgrade.StoreUpgradeOutcome(
            DarlingStoreUpgrade.StoreUpgradeStatus.ExtensionUpdated, 18, 18, "2.24.0", "2.28.1", null, null, false)));
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
            var oldConnection = DarlingManagedPostgres.BuildConnectionString(config.Port, password);

            /* ---- 2. Measure the store BEFORE, through the old binaries. ---- */
            await StartWithRuntimeAsync(runtimeRoot, dataDirectory, config.Port, timeout.Token);
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

                /* The continuous aggregate is not merely present — it still ANSWERS, which means the
                   TimescaleDB catalog, the materialization hypertable and its chunks all came across. */
                Assert.Equal(before.CaggRows, after.CaggRows);

                /* The extension matches the binaries it now runs on. A store whose extension lagged its
                   runtime is the #1705 drift this whole issue exists to end. */
                Assert.Equal(BundledTimescaleVersion(runtimeRoot), after.TimescaleVersion);

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
    /// <para>The fixture moves PostgreSQL's minor only. A TimescaleDB change in the pair is #3908: the store
    /// upgrade cannot yet move an existing store's extension, and this test would fail on exactly that.
    /// Gated on <c>DARLING_TEST_PGRUNTIME_PREVIOUS</c> (the previous release's runtime on the bundle's major,
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

            if (stamped)
            {
                var stampSource = Path.Combine(root.FullName, "old-runtime-stamp-source.zip");
                ZipFile.CreateFromDirectory(
                    Path.Combine(runtimeRoot, "pgsql"), stampSource, CompressionLevel.NoCompression, includeBaseDirectory: true);
                File.WriteAllText(
                    Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName),
                    DarlingStoreUpgrade.ComputeFileHash(stampSource));
            }
            else
            {
                Assert.False(File.Exists(Path.Combine(runtimeRoot, DarlingStoreUpgrade.RuntimeStampFileName)),
                    "the unstamped run needs a host with no stamp; building the store must not have written one");
            }

            var shippedZip = Path.Combine(deployment, "pg-runtime.zip");
            File.Copy(newZip!, shippedZip);

            var password = DarlingSecrets.Unprotect(
                File.ReadAllText(DarlingManagedPostgres.CredentialPathFor(dataDirectory)).Trim());
            var oldConnection = DarlingManagedPostgres.BuildConnectionString(config.Port, password);

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

                /* No pg_upgrade ran, so there is no retained pre-upgrade data directory. */
                Assert.False(
                    Directory.Exists(DarlingStoreUpgrade.RetainedDataDirectoryFor(dataDirectory, oldMajor)),
                    $"a same-major swap must not copy the data directory aside; that is pg_upgrade's rollback.\n\n--- orchestration log ---\n{log}");

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

            await ExecuteAsync(connection, "CREATE EXTENSION IF NOT EXISTS timescaledb", cancellationToken);
            await ExecuteAsync(connection, "CREATE SCHEMA IF NOT EXISTS collect", cancellationToken);

            await ExecuteAsync(
                connection,
                """
                CREATE TABLE collect.collection_log (
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
                "SELECT create_hypertable('collect.collection_log', by_range('collection_time'))",
                cancellationToken);

            await ExecuteAsync(
                connection,
                """
                CREATE TABLE collect.query_plans (
                    collection_time timestamptz NOT NULL,
                    server_id       integer      NOT NULL,
                    query_hash      text         NOT NULL,
                    plan_xml        text         NOT NULL
                )
                """,
                cancellationToken);
            await ExecuteAsync(
                connection,
                "SELECT create_hypertable('collect.query_plans', by_range('collection_time'))",
                cancellationToken);

            /* Collector rows across several days so the hypertable really has multiple chunks. */
            await ExecuteAsync(
                connection,
                """
                INSERT INTO collect.collection_log (collection_time, server_id, collector_name, status, rows_collected)
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
                INSERT INTO collect.query_plans (collection_time, server_id, query_hash, plan_xml)
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
                CREATE MATERIALIZED VIEW collect.collection_hourly
                WITH (timescaledb.continuous) AS
                SELECT time_bucket('1 hour', collection_time) AS bucket,
                       server_id,
                       count(*)             AS runs,
                       sum(rows_collected)  AS rows_collected
                FROM collect.collection_log
                GROUP BY 1, 2
                """,
                cancellationToken);

            /* Compression on the blob hypertable, then actually compress its chunks — a compressed chunk
               is a different on-disk shape, and "the upgrade kept the rows" has to hold for those too. */
            await ExecuteAsync(
                connection,
                "ALTER TABLE collect.query_plans SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')",
                cancellationToken);
            await ExecuteAsync(
                connection,
                "SELECT compress_chunk(c) FROM show_chunks('collect.query_plans') AS c",
                cancellationToken);

            return major;
        }
        finally
        {
            await bootstrap.StopIfStartedByThisProcessAsync();
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
        long CompressedChunks);

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
        var logRows = await ScalarLongAsync(connection, "SELECT count(*) FROM collect.collection_log", cancellationToken);
        var planRows = await ScalarLongAsync(connection, "SELECT count(*) FROM collect.query_plans", cancellationToken);
        var planLength = await ScalarLongAsync(
            connection, "SELECT COALESCE(sum(length(plan_xml)), 0) FROM collect.query_plans", cancellationToken);
        var checksum = await ScalarAsync<string>(
            connection,
            """
            SELECT md5(string_agg(
                       collection_time::text || '|' || server_id || '|' || collector_name || '|' || status || '|' || rows_collected,
                       ',' ORDER BY collection_time, server_id, collector_name))
            FROM collect.collection_log
            """,
            cancellationToken);
        var caggRows = await ScalarLongAsync(connection, "SELECT count(*) FROM collect.collection_hourly", cancellationToken);
        var compressed = await ScalarLongAsync(
            connection,
            "SELECT count(*) FROM timescaledb_information.chunks WHERE is_compressed",
            cancellationToken);

        return new StoreSnapshot(major, timescale, logRows, planRows, planLength, checksum ?? string.Empty, caggRows, compressed);
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
