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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Every store command on the once-per-process STARTUP / BOOTSTRAP path must carry an explicit command
/// deadline (#2874) — the managed-Postgres bootstrap and network reconcile, the in-place major-upgrade
/// path, least-privilege role provisioning, the delta-baseline re-seed, and the config store's first-run
/// seed — and so must the commands the collection loop's own SERIAL thread runs ahead of every launch,
/// which share seven of their call sites with that path and cannot share its number.
///
/// <para><b>Three constants, because the failure modes differ rather than the magnitudes.</b>
/// <see cref="ServiceCommandDeadlines.BootstrapSeconds"/> covers the twenty-six un-retried bootstrap
/// sites; <see cref="ServiceCommandDeadlines.BootstrapConnectProbeSeconds"/> the two inside the
/// bootstrap's six-attempt first-connection retry, where the deadline MULTIPLIES; and
/// <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/> the nine on the collection loop's serial
/// thread, where nine sequential commands have to fit inside one watchdog window.</para>
///
/// <para><b>This is the regime whose value goes UP, and the pin has to defend that as much as the
/// number.</b> Two of the four failure handlers on this path are <c>LogCritical</c> followed by
/// <c>return</c> — a managed bootstrap throw exits the service, a migrate-path throw ends collection for
/// the life of the process — so a deadline that fires during a slow-but-healthy start converts a
/// recoverable delay into a dead service. Every other regime in this sweep was bounded from above by
/// something scarce it was holding; this one is bounded from above by nothing, and errs long on purpose.
/// The derivation, both bounds and every measurement, is on
/// <see cref="ServiceCommandDeadlines.BootstrapSeconds"/> and
/// <see cref="ServiceCommandDeadlines.BootstrapConnectProbeSeconds"/>.</para>
///
/// <para><b>Scoped by MEMBER, and by member only — no directory glob anywhere in this file.</b>
/// <c>.Service</c> is roughly fifteen budget regimes across five groups, and three of the files below
/// hold sites belonging to other regimes: <c>StoreConfigProvider</c> carries the 15 s reload beacon and
/// <c>LoadViewAsync</c>'s five control-plane reads next to the seven seeding sites here;
/// <c>DarlingManagedPostgres</c> carries the retried connect probe next to the un-retried network
/// reconcile; <c>DarlingManagedRoles</c> carries the reload-path re-assert next to startup provisioning.
/// A directory-scoped guard would either fail on sites this group deliberately left alone or be relaxed
/// until it guarded nothing. The reliable test — and it is the one #2928 arrived at from two directions —
/// is <i>which cancellation token does this call site actually receive, and what re-runs it?</i></para>
///
/// <para><b>Why this pin checks the construction's OWN text rather than a two-statement window.</b> Every
/// landed pin in this sweep asks <c>StatementSpanFrom(..., statements: 2)</c> whether a deadline appears
/// near a construction, because the <c>CreateCommand</c> shape cannot take an object initializer and its
/// deadline is therefore the statement AFTER it. That window can be satisfied by a NEIGHBOUR: when a
/// command is built in a <c>using (...) { ...; }</c> block, the two statements are consumed by the block
/// body plus the statement following the block, so a timed construction there marks an untimed site
/// clean. That is not hypothetical here — it is the exact shape of
/// <c>DarlingStoreUpgrade.BridgeTimescaleAsync</c>, where the bridge's <c>ALTER EXTENSION UPDATE</c> sits
/// in such a block immediately ahead of the extension-verify read. <see cref="ConstructionSpan"/> instead
/// walks the argument list and its optional trailing object initializer and stops, so what it reads is
/// exactly one command's own deadline and a neighbour cannot be borrowed in either direction. Every one
/// of this group's twenty-eight edits uses the object-initializer form, which is what makes that
/// possible — and it also makes the deadline unrelocatable, closing the count-invariant MOVE that a
/// separate statement would allow.
/// </para>
/// </summary>
public sealed class StartupCommandTimeoutTests
{
    /// <summary>
    /// The bootstrap path's store-touching members, as (file, member) pairs with the exact number of
    /// command sites each holds and which deadline each site is expected to carry.
    ///
    /// <para><c>Other</c> counts sites carrying a deadline this group deliberately did NOT change:
    /// the two <c>ALTER EXTENSION UPDATE</c> statements already bounded by
    /// <c>DarlingStoreUpgrade.s_bridgeTimeout</c> (1 h), which is right for a data-rewriting extension
    /// update and wrong for the catalog reads beside it. Counting them rather than ignoring them is what
    /// keeps this pin honest about the two files it only partly owns.</para>
    ///
    /// <para><b>What is deliberately absent, and why each is a different budget — the token test, member
    /// by member.</b></para>
    ///
    /// <para><c>StoreConfigProvider.ReadConfigVersionAsync</c> (1 site) and <c>LoadViewAsync</c>'s five
    /// reads (<c>ReadServiceRowAsync</c>, <c>ReadAlertSettingsAsync</c>, <c>ReadNotificationAsync</c>,
    /// <c>ReadMonitoredServersAsync</c>, <c>ReadScheduleOverridesAsync</c>),
    /// <c>DarlingObservability.SyncServerEnabledStatesAsync</c>'s two, and
    /// <c>DarlingManagedRoles.ReassertComposeStatementTimeoutAsync</c>'s one are the CONTROL-PLANE
    /// regime: ten sites awaited inline on the serial collection-loop thread, ahead of every per-server
    /// launch, on the 15 s <c>s_sweepInterval</c> tick. Their blast radius is the whole fleet while their
    /// floor is a single-row lookup, they fail OPEN (the live config stands), and their ceiling is the
    /// tick — a strictly TIGHTER bound on both sides than this group's. Seven of them also run once on
    /// the bootstrap path, at <c>DarlingWorker.cs:1179</c> and <c>:1192</c>, and that is exactly why they
    /// are excluded rather than claimed: a deadline is a property of the command, so the tighter of a
    /// dual-caller site's two bounds has to win, and it is not this one.</para>
    ///
    /// <para><c>DarlingWorker.ReadStoreSizeBytesAsync</c> runs on the same serial loop thread on the
    /// 5-minute <c>s_diskCheckInterval</c> and fails to null at Debug. <c>ReadLatestCpuAsync</c> belongs
    /// to #2882's alert pass (10 s), a site that pin's file-scoped list missed.
    /// <c>DarlingObservability.LogRetentionRunAsync</c> is the daily purge's own row.
    /// <c>RunTestHypotheticalIndexAsync</c> / <c>RunExecuteActualPlanAsync</c> and
    /// <c>DarlingCommandExecutor</c> are the command plane, with a 5-minute claim lease and no
    /// heartbeat.</para>
    /// </summary>
    private static readonly (string File, string Member, int Bootstrap, int ConnectProbe, int SerialLoop, int Other)[] s_startupMembers =
    {
        ("DarlingManagedPostgres.cs", "EnsureDatabaseOnceAsync", 0, 2, 0, 0),
        ("DarlingManagedPostgres.cs", "VerifyPgHbaAsync", 3, 0, 0, 0),
        ("DarlingManagedPostgres.cs", "GuardAdoptedListenAsync", 1, 0, 0, 0),
        ("DarlingStoreUpgrade.cs", "ReadClusterIdentityAsync", 1, 0, 0, 0),
        ("DarlingStoreUpgrade.cs", "BridgeTimescaleAsync", 3, 0, 0, 1),
        ("DarlingStoreUpgrade.cs", "CompleteAfterStartAsync", 3, 0, 0, 1),
        ("DarlingStoreUpgrade.cs", "VerifySentinelReadAsync", 2, 0, 0, 0),
        ("DarlingManagedRoles.cs", "EnsureProvisionedAsync", 1, 0, 0, 0),
        ("DarlingManagedRoles.cs", "ReadComposeStatementTimeoutAsync", 1, 0, 0, 0),
        ("DarlingDeltaCalculator.cs", "SeedWaitStatsAsync", 1, 0, 0, 0),
        ("DarlingDeltaCalculator.cs", "SeedFileIoStatsAsync", 1, 0, 0, 0),
        ("DarlingDeltaCalculator.cs", "SeedPerfmonStatsAsync", 1, 0, 0, 0),
        ("DarlingDeltaCalculator.cs", "SeedMemoryGrantStatsAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "WarnAboutFileOnlyServersAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "ReadRegisteredServersForComparisonAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "CountAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "SeedServiceRowAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "SeedAlertSettingsAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "SeedNotificationAsync", 1, 0, 0, 0),
        ("StoreConfigProvider.cs", "SeedMonitoredServersAsync", 1, 0, 0, 0),

        /* The serial collection-loop thread: the control-plane reload body plus the disk-check store-size
           read. Seven of these nine ALSO run once on the bootstrap path above, and take this constant
           rather than BootstrapSeconds because a dual-caller site has to take the tighter of its two
           bounds. */
        ("StoreConfigProvider.cs", "ReadServiceRowAsync", 0, 0, 1, 0),
        ("StoreConfigProvider.cs", "ReadAlertSettingsAsync", 0, 0, 1, 0),
        ("StoreConfigProvider.cs", "ReadNotificationAsync", 0, 0, 1, 0),
        ("StoreConfigProvider.cs", "ReadMonitoredServersAsync", 0, 0, 1, 0),
        ("StoreConfigProvider.cs", "ReadScheduleOverridesAsync", 0, 0, 1, 0),
        ("DarlingObservability.cs", "SyncServerEnabledStatesAsync", 0, 0, 2, 0),
        ("DarlingManagedRoles.cs", "ReassertComposeStatementTimeoutAsync", 0, 0, 1, 0),
        ("DarlingWorker.cs", "ReadStoreSizeBytesAsync", 0, 0, 1, 0),
    };

    /// <summary>The group's own totals, so a member that stops creating commands fails loudly.</summary>
    private const int ExpectedBootstrapSites = 26;

    private const int ExpectedConnectProbeSites = 2;

    private const int ExpectedSerialLoopSites = 9;

    /// <summary>
    /// Members whose command sites must NOT carry either bootstrap deadline — the exclusions above,
    /// asserted rather than merely documented so a later pass cannot sweep them in by adjacency. This is
    /// a NEGATIVE, so <see cref="TheExclusionScan_CanSeeABootstrapStamp"/> positive-controls it through
    /// the identical scan: a negative that passes by matching nothing is not a guard.
    /// </summary>
    private static readonly (string File, string Member)[] s_excludedMembers =
    {
        ("StoreConfigProvider.cs", "ReadConfigVersionAsync"),
        ("DarlingObservability.cs", "LogRetentionRunAsync"),
        ("DarlingWorker.cs", "ReadLatestCpuAsync"),
        ("DarlingWorker.cs", "RunTestHypotheticalIndexAsync"),
        ("DarlingWorker.cs", "RunExecuteActualPlanAsync"),
    };

    /// <summary>
    /// All FIVE construction shapes this sweep now knows about, including the qualified type name
    /// <c>new Npgsql.NpgsqlCommand(</c> that #2931 found because a red-first variant came back green.
    /// The bare method group is kept even though every census has found zero of it in this project,
    /// because "absent today" is not a guard.
    /// </summary>
    private static readonly Regex s_commandCtor = new(
        @"new\s+(?:Npgsql\.)?NpgsqlCommand\s*\(|\.CreateCommand\s*\(|\.CreateCommand\s*[,);]",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_bootstrapDeadline = new(
        @"CommandTimeout\s*=\s*ServiceCommandDeadlines\.BootstrapSeconds\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_connectProbeDeadline = new(
        @"CommandTimeout\s*=\s*ServiceCommandDeadlines\.BootstrapConnectProbeSeconds\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_serialLoopDeadline = new(
        @"CommandTimeout\s*=\s*ServiceCommandDeadlines\.SerialLoopSeconds\b",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The pre-existing 1 h bridge bound, matched so it counts as timed-but-not-ours.</summary>
    private static readonly Regex s_bridgeDeadline = new(
        @"CommandTimeout\s*=\s*\(int\)s_bridgeTimeout\.TotalSeconds",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryBootstrapCommand_CarriesItsOwnStartupDeadline()
    {
        var offenders = new List<string>();
        var wrongConstant = new List<string>();
        int bootstrap = 0, probe = 0, loop = 0, other = 0;

        foreach (var (file, member, wantBootstrap, wantProbe, wantLoop, wantOther) in s_startupMembers)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var body = MemberBody(text, member, path);
            var bodyCode = CSharpSourceWalker.StripCommentsAndStrings(body);
            int gotBootstrap = 0, gotProbe = 0, gotLoop = 0, gotOther = 0;

            foreach (Match ctor in s_commandCtor.Matches(bodyCode))
            {
                var span = ConstructionSpan(bodyCode, ctor.Index);

                if (s_bootstrapDeadline.IsMatch(span))
                {
                    gotBootstrap++;
                }
                else if (s_connectProbeDeadline.IsMatch(span))
                {
                    gotProbe++;
                }
                else if (s_serialLoopDeadline.IsMatch(span))
                {
                    gotLoop++;
                }
                else if (s_bridgeDeadline.IsMatch(span))
                {
                    gotOther++;
                }
                else
                {
                    offenders.Add($"{file} {member} +{LineOf(body, ctor.Index)}");
                }
            }

            if (gotBootstrap != wantBootstrap || gotProbe != wantProbe
                || gotLoop != wantLoop || gotOther != wantOther)
            {
                wrongConstant.Add(
                    $"{file} {member}: bootstrap {gotBootstrap}/{wantBootstrap}, "
                    + $"connect-probe {gotProbe}/{wantProbe}, serial-loop {gotLoop}/{wantLoop}, "
                    + $"other {gotOther}/{wantOther}");
            }

            bootstrap += gotBootstrap;
            probe += gotProbe;
            loop += gotLoop;
            other += gotOther;
        }

        /* Offenders BEFORE the census, so a real defect reports as a defect rather than as an
           arithmetic mismatch that says nothing about the deadline it is missing. */
        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} startup command(s) inherit Npgsql's undocumented 30s default instead of "
            + $"setting {nameof(ServiceCommandDeadlines)}.{nameof(ServiceCommandDeadlines.BootstrapSeconds)} "
            + $"{nameof(ServiceCommandDeadlines.BootstrapConnectProbeSeconds)} or "
            + $"{nameof(ServiceCommandDeadlines.SerialLoopSeconds)}: "
            + string.Join(", ", offenders));

        /* The retried pair and the un-retried remainder are DIFFERENT budgets — 6 x deadline against
           1 x deadline — so a site drifting from one constant to the other is a real defect that no
           total would catch. Per-member expectations are what see it. */
        Assert.True(
            wrongConstant.Count == 0,
            "startup command(s) carry the wrong deadline for their sub-regime: "
            + string.Join("; ", wrongConstant));

        Assert.Equal(ExpectedBootstrapSites, bootstrap);
        Assert.Equal(ExpectedConnectProbeSites, probe);
        Assert.Equal(ExpectedSerialLoopSites, loop);
    }

    /// <summary>
    /// The exclusions, asserted. Each of these members shares a FILE with this group and belongs to
    /// another budget, and the failure mode being guarded is a later pass stamping a bootstrap constant
    /// onto one of them because it was nearby — which is precisely how #2882 came to miss
    /// <c>ReadLatestCpuAsync</c> and how #2928's first pass came to mis-place
    /// <c>WriteAnalysisStateAsync</c>, in opposite directions.
    /// </summary>
    [Fact]
    public void ExcludedMembers_DoNotCarryAStartupDeadline()
    {
        var offenders = new List<string>();
        var scanned = 0;

        foreach (var (file, member) in s_excludedMembers)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var body = MemberBody(text, member, path);
            var bodyCode = CSharpSourceWalker.StripCommentsAndStrings(body);

            foreach (Match ctor in s_commandCtor.Matches(bodyCode))
            {
                scanned++;
                var span = ConstructionSpan(bodyCode, ctor.Index);

                if (s_bootstrapDeadline.IsMatch(span)
                    || s_connectProbeDeadline.IsMatch(span)
                    || s_serialLoopDeadline.IsMatch(span))
                {
                    offenders.Add($"{file} {member} +{LineOf(body, ctor.Index)}");
                }
            }
        }

        /* The scan has to have SEEN something, or the negative below passes vacuously — the failure this
           sweep has already hit once, where a negative grep matched nothing and read as clean. */
        Assert.True(scanned >= s_excludedMembers.Length, $"the exclusion scan found only {scanned} command sites");

        Assert.True(
            offenders.Count == 0,
            $"{offenders.Count} command(s) outside the startup regime carry a startup deadline — they run "
            + "on the serial collection-loop thread, the disk-check cadence, the alert pass, the daily "
            + $"purge or the command plane, each with its own bound: {string.Join(", ", offenders)}");
    }

    /// <summary>
    /// <see cref="ExcludedMembers_DoNotCarryAStartupDeadline"/> is a negative, so its scan is proven able
    /// to FIRE through the identical code path. <c>DoesNotContain</c>-shaped assertions that can only pass
    /// are the failure mode this control exists for.
    /// </summary>
    [Fact]
    public void TheExclusionScan_CanSeeABootstrapStamp()
    {
        const string Fixture =
            "void M()\n"
            + "{\n"
            + "    using var command = new NpgsqlCommand(Sql, connection) "
            + "{ CommandTimeout = ServiceCommandDeadlines.BootstrapSeconds };\n"
            + "}\n";

        var code = CSharpSourceWalker.StripCommentsAndStrings(Fixture);
        var ctor = s_commandCtor.Match(code);
        Assert.True(ctor.Success, "the control fixture did not contain a command construction");

        var span = ConstructionSpan(code, ctor.Index);
        Assert.True(
            s_bootstrapDeadline.IsMatch(span),
            "the exclusion scan cannot see a bootstrap stamp it is supposed to reject, so its negative "
            + "would pass on any input");
    }

    /// <summary>
    /// The un-retried bound, two-sided — see <see cref="ServiceCommandDeadlines.BootstrapSeconds"/> for
    /// the derivation. A band rather than an equality, following the precedent .Storage and .Viewer set:
    /// the claim being defended is that the value sits between two bounds, not that it is one number.
    /// </summary>
    [Fact]
    public void TheBootstrapDeadline_StaysInsideItsJustifiedBand()
    {
        var seconds = ServiceCommandDeadlines.BootstrapSeconds;

        Assert.True(
            seconds > 30,
            $"bootstrap deadline {seconds}s is at or below Npgsql's inherited 30s default, so it makes "
            + "this path fail no later than it already does — and the failure here is LogCritical plus "
            + "return, which ends collection for the life of the process with no retry. #1772 is the "
            + "field case: the delta seed could not finish inside that default on a 276 GB store, and "
            + "restart continuity silently degraded on every start. Waiting is the cheap direction on "
            + "this path; the point of the change is to make the number chosen, not to make it tighter");

        Assert.True(
            seconds >= 20 * 3,
            $"bootstrap deadline {seconds}s leaves under 400x headroom over the measured cold worst case "
            + "(151.6 ms for the post-upgrade count(*) over a 0.69 GB collection_log), and the "
            + "measurement is local NVMe on Linux while the path runs on Windows storage against a store "
            + "at its least warm");

        Assert.True(
            seconds < MigrationRungBudgetSeconds(),
            $"bootstrap deadline {seconds}s reaches the {MigrationRungBudgetSeconds()}s that "
            + "PgMigrations gives ONE migration rung. That budget bounds data-MOVING DDL on a cold busy "
            + "store; these sites are catalog reads, single-row config seeds, one count(*) and one "
            + "idempotent grant batch, so taking the rung's number would be borrowing rather than "
            + "deriving");
    }

    /// <summary>
    /// The retried bound, and the assertion that makes it a bound rather than a preference: whatever it
    /// is, <c>FirstConnectionAttempts</c> multiplies it, and the product plus the retry pauses is the
    /// worst-case startup delay before <c>LogCritical</c>-and-exit. Pinned RELATIONALLY against the retry
    /// policy so raising the attempt count cannot silently push that product past the window an operator
    /// is waiting through.
    /// </summary>
    [Fact]
    public void TheConnectProbeDeadline_SurvivesItsOwnRetryMultiplier()
    {
        var seconds = ServiceCommandDeadlines.BootstrapConnectProbeSeconds;
        var attempts = DarlingManagedPostgres.FirstConnectionAttempts;
        var pauses = (attempts - 1) * DarlingManagedPostgres.s_firstConnectionRetryDelay.TotalSeconds;
        var worstCase = (attempts * seconds) + pauses;

        Assert.True(
            seconds >= 5,
            $"connect-probe deadline {seconds}s leaves too little over the measured worst case — a 12-14 ms "
            + "pg_database probe and a 19-68 ms CREATE DATABASE, on a Windows box where antivirus scans "
            + "every file the template copy creates");

        Assert.True(
            seconds < ServiceCommandDeadlines.BootstrapSeconds,
            $"connect-probe deadline {seconds}s is not tighter than the un-retried "
            + $"{ServiceCommandDeadlines.BootstrapSeconds}s, but it is the only site set with a retry "
            + "above it, so it is the only one whose deadline multiplies");

        Assert.True(
            worstCase <= 120,
            $"{attempts} attempts at {seconds}s plus {pauses}s of pauses is {worstCase}s of startup delay "
            + "before the bootstrap gives up and exits the service — past the installers' 2-minute "
            + "WaitForStatus('Running'), i.e. past the window an operator is already waiting through");
    }

    /// <summary>
    /// The serial-loop bound — see <see cref="ServiceCommandDeadlines.SerialLoopSeconds"/>. The
    /// interesting assertion is the CHAIN one: these nine run sequentially on one thread, so what has to
    /// stay bounded is their sum, not any single deadline. Pinned RELATIONALLY against
    /// <c>DarlingWorker.SweepWatchdogSeconds</c> and against the site count, so adding a tenth site to
    /// this regime forces the value to be re-derived rather than silently stretching the chain.
    /// </summary>
    [Fact]
    public void TheSerialLoopDeadline_KeepsItsWholeChainInsideTheWatchdog()
    {
        var seconds = ServiceCommandDeadlines.SerialLoopSeconds;

        Assert.True(
            seconds >= 2,
            $"serial-loop deadline {seconds}s leaves too little over the measured cold worst case — "
            + "LoadViewAsync's five commands at 16.1 ms together, SyncServerEnabledStatesAsync's two at "
            + "3.8 ms, pg_database_size at 6.2 ms — on a thread whose reload body silently LOSES an "
            + "operator's config change if it fires, because the beacon advances _lastConfigVersion "
            + "before the reload runs");

        Assert.True(
            seconds < 30,
            $"serial-loop deadline {seconds}s is at or above Npgsql's inherited 30s default, so it buys "
            + "nothing on a thread that blocks the whole fleet's cycle while it runs");

        Assert.True(
            ExpectedSerialLoopSites * seconds < DarlingWorker.SweepWatchdogSeconds,
            $"{ExpectedSerialLoopSites} sequential commands at {seconds}s is "
            + $"{ExpectedSerialLoopSites * seconds}s, which reaches the "
            + $"{DarlingWorker.SweepWatchdogSeconds}s watchdog — a merely-slow control-plane reload would "
            + "then report as a hang, the #1581/#2170 warning herd #2928 also had to avoid");

        Assert.True(
            seconds < ServiceCommandDeadlines.BootstrapSeconds,
            $"serial-loop deadline {seconds}s is not tighter than the bootstrap's "
            + $"{ServiceCommandDeadlines.BootstrapSeconds}s. Seven of these nine sites ALSO run once on "
            + "the bootstrap path, and a dual-caller site must take the tighter of its two bounds — if "
            + "this is not the tighter one, those seven are on the wrong constant");
    }

    /// <summary>
    /// <see cref="ConstructionSpan"/> is what thirty-seven sites' correctness is asserted through, so its
    /// own edges are pinned. The fixtures are shapes that OCCUR in the members above, and the fourth is
    /// the one that matters: the <c>using (...) { ...; }</c> block whose neighbour a two-statement window
    /// would borrow, which is the real shape of <c>BridgeTimescaleAsync</c>. The last fixture covers the
    /// other direction — a deadline written in a COMMENT inside the initializer, which every landed pin
    /// in this sweep counts as set because it matches its value pattern over raw source (#2940).
    /// </summary>
    [Theory]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection) { CommandTimeout = D.BootstrapSeconds };\n",
        true)]
    [InlineData(
        "using (var exists = new NpgsqlCommand(Sql, connection) { CommandTimeout = D.BootstrapSeconds })\n"
        + "{\n"
        + "    await exists.ExecuteScalarAsync(ct);\n"
        + "}\n",
        true)]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection);\n"
        + "command.Parameters.AddWithValue(x);\n",
        false)]
    [InlineData(
        "using (var update = new NpgsqlCommand(Sql, connection)\n"
        + "{\n"
        + "    CommandTimeout = 3600,\n"
        + "})\n"
        + "{\n"
        + "    await update.ExecuteNonQueryAsync(ct);\n"
        + "}\n"
        + "using var verify = new NpgsqlCommand(Other, connection) { CommandTimeout = D.BootstrapSeconds };\n",
        false)]
    [InlineData(
        "/* CommandTimeout = D.BootstrapSeconds is what this one is MISSING. */\n"
        + "using var command = new NpgsqlCommand(Sql, connection);\n",
        false)]
    [InlineData(
        "using var command = new NpgsqlCommand(Sql, connection)\n"
        + "{\n"
        + "    /* CommandTimeout = D.BootstrapSeconds -- a deadline described, not set. */\n"
        + "    Parameters = { p },\n"
        + "};\n",
        false)]
    public void TheConstructionScanner_ReadsOnlyItsOwnCommandsDeadline(string source, bool expectedStamped)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        var ctor = s_commandCtor.Match(code);
        Assert.True(ctor.Success, "the fixture did not contain a command construction");

        var span = ConstructionSpan(code, ctor.Index);
        var stamped = new Regex(@"CommandTimeout\s*=\s*D\.BootstrapSeconds\b").IsMatch(span);

        Assert.Equal(expectedStamped, stamped);
    }

    /// <summary>
    /// The member extractor, pinned separately for the reason #2928 records: a body that resolves to the
    /// WRONG span, or to an empty one, would report clean on whatever it failed to read.
    /// </summary>
    [Fact]
    public void TheMemberExtractor_ReturnsOnlyTheNamedMembersBody()
    {
        const string Source =
            "class C\n"
            + "{\n"
            + "    void Before() { var a = new NpgsqlCommand(X); }\n"
            + "    async Task TargetAsync(int id)\n"
            + "    {\n"
            + "        var command = new NpgsqlCommand(Y);\n"
            + "    }\n"
            + "    void After() { var b = new NpgsqlCommand(Z); }\n"
            + "}\n";

        var body = MemberBody(Source, "TargetAsync", "fixture");

        Assert.Contains("new NpgsqlCommand(Y)", body);
        Assert.DoesNotContain("new NpgsqlCommand(X)", body);
        Assert.DoesNotContain("new NpgsqlCommand(Z)", body);
        Assert.Single(s_commandCtor.Matches(body));
    }

    /// <summary>
    /// STORE versus MONITORED TARGET, the distinction none of the pins before #2931 had to make.
    /// <c>.Storage</c> and <c>.Viewer</c> are Npgsql-store-only projects; <c>.Service</c> is not, and it
    /// builds commands against monitored PostgreSQL targets in the same files.
    ///
    /// <para>All thirty sites in the members above are STORE commands, classified individually: twenty-six
    /// against the <c>darling</c> store (six on a data-source connection, twenty on a non-pooled
    /// <c>NpgsqlConnection</c> built from a connection-string builder), two against the MAINTENANCE
    /// database <c>postgres</c> on the same cluster (the connect probe), and two against each
    /// TimescaleDB-carrying database on the store cluster (the extension bridge). None is a monitored
    /// target: no <c>SqlConnection</c>, no <c>TargetProviders.For</c>, and no use of a
    /// <c>runtime.Target</c> connection anywhere in the thirty.</para>
    ///
    /// <para>The classification is encoded as a NEGATIVE against the two real target shapes, so a future
    /// target command placed in one of these members fails asking for a decision rather than inheriting a
    /// store bound: <c>ITargetProvider.CreateCommand(plan, connection, commandTimeoutSeconds)</c>, where
    /// the deadline is threaded as a parameter and is therefore stronger than a constant, and the
    /// qualified <c>new Npgsql.NpgsqlCommand(</c> that <c>DarlingWorker</c> uses for its
    /// <c>pg_stat_statements</c> text fetch against a monitored Aurora/PostgreSQL target.</para>
    /// </summary>
    [Fact]
    public void NoStartupMember_BuildsAMonitoredTargetCommand()
    {
        var targetShapes = new Regex(
            @"ITargetProvider|TargetProviders\.For|new\s+SqlCommand\s*\(|new\s+SqlConnection\s*\(|"
            + @"runtime\.Target|new\s+Npgsql\.NpgsqlCommand\s*\(",
            RegexOptions.Compiled | RegexOptions.CultureInvariant);

        var offenders = new List<string>();

        foreach (var (file, member, _, _, _, _) in s_startupMembers)
        {
            var path = SourcePath(file);
            var text = File.ReadAllText(path);
            var body = CSharpSourceWalker.StripCommentsAndStrings(MemberBody(text, member, path));

            foreach (Match hit in targetShapes.Matches(body))
            {
                offenders.Add($"{file} {member}: {hit.Value}");
            }
        }

        Assert.True(
            offenders.Count == 0,
            "a startup member now builds a command against a MONITORED TARGET rather than the store, so "
            + "the bootstrap bound derived from store behaviour no longer applies to it: "
            + string.Join(", ", offenders));

        /* Positive control on the negative above, and it is not decoration: the two shapes named there
           BOTH exist in this project and both would be flagged, which is what proves the scan is looking
           at real text rather than passing on an empty set. */
        Assert.Equal(2, targetShapes.Matches("ITargetProvider x; new Npgsql.NpgsqlCommand(y);").Count);
    }

    /// <summary>
    /// From a construction's match index through the end of its argument list AND its optional trailing
    /// object initializer — this command's OWN text and nothing after it.
    ///
    /// <para>Both walked over and RETURNED FROM <see cref="CSharpSourceWalker.StripCommentsAndStrings"/>'s
    /// output, which is the third of this sweep's pin defects and the reason the value regexes below cannot
    /// be fooled: every landed pin matches its value pattern over the RAW span, so a <c>CommandTimeout</c>
    /// written in a comment inside the construction makes an untimed site read clean (#2940 measured that
    /// gap and it survives #2938's span fix). Returning the stripped slice closes it, and it is safe
    /// because the stripper is character-ALIGNED with its input — newlines and offsets survive, so the
    /// offender line numbers computed against the raw body still match.</para>
    ///
    /// <para>Walking the stripped text is separately load-bearing rather than defensive: these members
    /// embed verbatim and raw-string SQL carrying braces, parentheses and quote characters, so a brace in
    /// a literal is exactly what would unbalance the walk. The pin strips before EVERY scan in this file,
    /// including the construction scan, which the landed pins do not: a <c>.CreateCommand(</c> in running
    /// prose would otherwise be reported as an offender at a line no edit could fix.</para>
    /// </summary>
    private static string ConstructionSpan(string code, int start)
    {
        var open = code.IndexOf('(', start);

        if (open < 0)
        {
            return code[start..];
        }

        var depth = 0;
        var close = open;

        for (; close < code.Length; close++)
        {
            if (code[close] == '(')
            {
                depth++;
            }
            else if (code[close] == ')' && --depth == 0)
            {
                break;
            }
        }

        var next = close + 1;

        while (next < code.Length && char.IsWhiteSpace(code[next]))
        {
            next++;
        }

        /* No object initializer: the span is the construction expression alone. A deadline set as a
           SEPARATE statement afterwards is therefore not accepted by this pin, deliberately — every one
           of this group's sites uses the initializer form, which is what makes the deadline
           unrelocatable and the span unborrowable. */
        if (next >= code.Length || code[next] != '{')
        {
            return code[start..Math.Min(close + 1, code.Length)];
        }

        depth = 0;

        for (var i = next; i < code.Length; i++)
        {
            if (code[i] == '{')
            {
                depth++;
            }
            else if (code[i] == '}' && --depth == 0)
            {
                return code[start..(i + 1)];
            }
        }

        return code[start..];
    }

    /// <summary>
    /// The per-rung migration budget, read out of <c>PgMigrations</c>' own source because the constant is
    /// <c>private</c>. Read rather than duplicated: a literal here would silently stop tracking the value
    /// it is supposed to be bounded by, which is the drift this whole sweep keeps finding.
    /// </summary>
    private static int MigrationRungBudgetSeconds()
    {
        var path = Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");

        Assert.True(File.Exists(path), $"PgMigrations source not found: {path}");

        var declaration = new Regex(
            @"MigrationCommandTimeoutSeconds\s*=\s*(\d+)\s*;",
            RegexOptions.CultureInvariant);

        var match = declaration.Match(CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path)));

        Assert.True(match.Success, "MigrationCommandTimeoutSeconds could not be read from PgMigrations.cs");

        return int.Parse(match.Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The text of one member's body, from its signature to the matching close brace — brace-matched over
    /// the stripped text with the offsets applied to the original, for the reason in
    /// <see cref="ConstructionSpan"/>.
    /// </summary>
    private static string MemberBody(string text, string member, string path)
    {
        var code = CSharpSourceWalker.StripCommentsAndStrings(text);

        var signature = new Regex(
            @"\b" + Regex.Escape(member) + @"\s*(?:<[^<>()]*>)?\s*\(",
            RegexOptions.CultureInvariant);

        var declarations = signature.Matches(code)
            .Where(m => BodyStart(code, m.Index) >= 0)
            .ToArray();

        Assert.True(
            declarations.Length == 1,
            $"expected exactly one declaration of {member} in {path}, found {declarations.Length} — "
            + "an overload or a rename has moved this member out from under the guard");

        var open = BodyStart(code, declarations[0].Index);
        var body = CSharpSourceWalker.BraceBalanced(code, open);

        Assert.EndsWith("}", body);

        return text[declarations[0].Index..(open + body.Length)];
    }

    /// <summary>
    /// Index of the <c>{</c> opening the body of the declaration at <paramref name="at"/>, or -1 when
    /// what follows the parameter list is not a block — which is how a CALL to the member, or an
    /// expression-bodied delegation, is told apart from its declaration.
    /// </summary>
    private static int BodyStart(string code, int at)
    {
        var depth = 0;

        for (var i = code.IndexOf('(', at); i >= 0 && i < code.Length; i++)
        {
            if (code[i] == '(')
            {
                depth++;
            }
            else if (code[i] == ')' && --depth == 0)
            {
                for (var j = i + 1; j < code.Length; j++)
                {
                    if (char.IsWhiteSpace(code[j]))
                    {
                        continue;
                    }

                    return code[j] == '{' ? j : -1;
                }

                return -1;
            }
        }

        return -1;
    }

    private static int LineOf(string text, int index) => text.Take(index).Count(c => c == '\n') + 1;

    private static string SourcePath(string relative)
    {
        var path = Path.Combine(
            RepoRoot(),
            "Darling",
            "PerformanceMonitor.Darling.Service",
            relative.Replace('/', Path.DirectorySeparatorChar));

        /* A moved or renamed startup source must fail loudly rather than silently shrinking the scan to
           the files that still resolve — an empty sweep is how a guard starts reporting clean. */
        Assert.True(File.Exists(path), $"startup source not found: {path}");

        return path;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
