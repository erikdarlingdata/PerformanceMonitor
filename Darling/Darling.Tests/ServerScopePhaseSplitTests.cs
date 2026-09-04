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
using System.Reflection;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Pins the SERVER-SCOPED phase split (#2851) — the <c>open: + drain: + other:</c> breakdown for
/// collectors that read a whole server in one query rather than enumerating databases.</para>
///
/// <para><b>Why it exists.</b> The breakdown was emitted only behind <c>PerItemOpenMs &gt; 0</c>, which the
/// per-database path sets, so the largest collector on the fleet could not be attributed at all.
/// procedure_stats runs 4,900ms p50 on use1 while the same shipped query, run from the same box against the
/// same target, takes 247ms — an 18.8x gap measured with the box at 4% CPU. Same shape as the store probe's
/// ~40x (36ms in a harness, ~1,451ms in production), and that one was only tractable because #2811/#2816 had
/// split its phases.</para>
///
/// <para><b>Two pins, because one cannot see what the other misses.</b> The arithmetic pin below holds the
/// residual honest. The IL pin holds the STAMPS reachable — and #2816 is the reason both are needed: the
/// probe's arithmetic pin passed throughout that defect, because the arithmetic was never wrong. A plain
/// assignment after a throwing <c>await</c> left the phase at zero and the residual silently absorbed the
/// cost, which for a timed-out store round trip is precisely inverted.</para>
///
/// <para><b>#2854 widened the IL pin to every phase stamp, both paths.</b> Naming only the two server-scoped
/// stamps left four ENUMERATED ones bare, and two of those — <c>PerItemPlanFetchMs</c> and
/// <c>PerItemTextFetchMs</c> — are the parents of the sub-split #2816 fixed. A throwing fetch therefore
/// printed a zero parent above non-zero children that stamp from their own handlers, which is not merely
/// missing but arithmetically impossible, and <c>PlanFetchOtherMs</c> clamped the negative residual to zero
/// so the line still read as precise. <c>DrainMsFrom</c> made it worse again: it subtracts the phases from
/// the item total, so a timed-out open reported its whole cost as <c>drain:</c> — blaming row streaming for a
/// statement that never returned a row.</para>
///
/// <para><b>#2855 added the third path, and with it the third decomposition.</b> The Azure per-database
/// branch emitted no split at all — not a mis-stamped one, none — so a slow database could not be attributed
/// even in principle. It opens a connection PER DATABASE, which neither other path does, so its split is
/// <c>connect: + open: + drain:</c> and <c>OpenDatabaseConnectionAsync</c> is a phase in its own right: on
/// Azure SQL DB that is a fresh login per database per cycle, the exact kind of recurring term that hides
/// inside a blended number. The stamps took a <c>PerDatabase*Ms</c> prefix and the derivation above was
/// widened to match, because a stamp named outside the pattern is a stamp nothing guards.</para>
///
/// <para>The split is emitted and logged only. Persisting it is <b>not</b> settled: it is N:1 against
/// <c>collection_log</c> — a run visits every database — where the server-scoped split V108 stores is 1:1,
/// so there is no row for it to land on. That shape is the open decision in #2860, and nothing here writes
/// to the store or moves the schema off V109.</para>
///
/// <para><b>What these pins do NOT cover.</b> The log site is on the success path, so a database whose
/// connect times out stamps its phases, sets its flag, and then reaches a catch arm that prints no split.
/// The stamps below are still pinned reachable from the handler — that is what makes the emission a later
/// addition rather than a re-instrumentation — but no test here asserts a fault-path LINE, because there
/// is not one to assert. The obstacle is that the parent the line decomposes is out of scope in the catch;
/// see the runner's own note at the log site.</para>
/// </summary>
public sealed class ServerScopePhaseSplitTests
{
    /* EVERY phase stamp, derived from the type rather than listed (#2854). The first cut of this pin named
       the two server-scoped stamps, which is why it did not notice that four stamps on the ENUMERATED path
       were still trailing assignments — including PerItemPlanFetchMs and PerItemTextFetchMs, the parents of
       the sub-split #2816 had already fixed. An enumerated list is how a sibling has survived a green suite
       four separate times in this repo; a derived one covers a stamp added tomorrow without anyone
       remembering to come back here.

       Derivation cannot silently SHRINK, which is the objection to deriving: MinimumPhaseStamps below fails
       loudly if a stamp is deleted or renamed out of the pattern, so the set can grow on its own but cannot
       quietly cover less.

       #2855 WIDENED the pattern to a third prefix. The Azure per-database branch opens a connection per
       database, so its split is connect: + open: + drain: rather than open: + drain:, and it took
       PerDatabase*Ms rather than reusing PerItem*Ms — the enumerated log site keys on its own flag and
       prints an item name, so sharing those fields would make an Azure run print as an enumerated one. That
       is the same collision ServerScopeOpenMs's doc comment records as the reason the server-scoped path got
       its own prefix, so the precedent decided it. The cost of the honest name is that the derivation had to
       be widened HERE in the same change, because a stamp outside the pattern is a stamp nothing guards —
       worse than no stamp, since this file's whole claim is that a new one is covered the day it appears. */
    private static readonly string[] PhaseSetters =
        typeof(CollectorContext)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(long)
                        && p.CanWrite
                        && p.Name.EndsWith("Ms", StringComparison.Ordinal)
                        && (p.Name.StartsWith("PerItem", StringComparison.Ordinal)
                            || p.Name.StartsWith("ServerScope", StringComparison.Ordinal)
                            || p.Name.StartsWith("PerDatabase", StringComparison.Ordinal)))
            .Select(p => "set_" + p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

    /* The TRUE count today, not a round number: 10 enumerated + 3 server-scoped + 3 per-database. Deleting a
       stamp, or renaming one out of the pattern above, drops the derived set and fails HERE with a number
       rather than silently checking less.

       Two of the four raised it. #2855 added the three PerDatabase*Ms stamps. The fourth server-scoped one
       is #2864's ServerScopeLastReadMs, which matched the pattern the day it landed and so was covered
       automatically — exactly as designed — but left the floor one behind the real count at 12, where a
       deletion could have gone unnoticed. Set to the exact count so it cannot drift silently again; a
       legitimate new stamp raises it in the same change, which is the two-line price of the guarantee. */
    private const int MinimumPhaseStamps = 16;

    /* The MEASURED FLAGS, derived on the same rule and scanned by the same IL walk (#2855). They were not
       covered before, and the omission is structural rather than an oversight in any one change: the
       derivation above selects on `long`, so a bool could never appear in it however it was named.

       That left the flag's placement guarded by prose only, and the flag is half the fix. #2854's whole
       lesson is that a stamp and its flag must be set in the SAME finally — a flag assigned after the await
       leaves a faulting phase declaring itself unmeasured, which suppresses the split line for exactly the
       run worth reading, and the *Ms pin above would stay green throughout because the number is stamped
       correctly. Two flags live on the context today (the enumerated one and #2855's per-database one); the
       server-scoped path's is a method local, so it is out of reach here and is covered instead by the
       ServerPhasesMeasured arithmetic tests. */
    private static readonly string[] MeasuredFlagSetters =
        typeof(CollectorContext)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(bool)
                        && p.CanWrite
                        && p.Name.EndsWith("PhasesMeasured", StringComparison.Ordinal))
            .Select(p => "set_" + p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

    private const int MinimumMeasuredFlags = 2;

    /* A setter assigned WITHOUT an exception handler, in the same assembly, resolved through the same
       metadata tables and the same IL walk. Two jobs: if the scanner stops resolving tokens its Total goes to
       zero and says so, and if handler-detection degenerates to "true for everything" its InHandler stops
       being zero. A control that cannot fail in the same way as the thing it guards proves nothing — the
       first control tried here (set_PerItemTextBudgetExceeded) resolved to zero call sites because it is
       assigned in the Collectors assembly, not this one, and would have failed for the wrong reason.
       set_Watermark is plain data assignment with six call sites, none of them in a handler, so it is
       unlikely to migrate into one and quietly stop discriminating. */
    private const string ControlSetter = "set_Watermark";

    [Fact]
    public void TheSplitSumsToItsParent_SoTheResidualIsAResidualAndNotASlushFund()
    {
        /* Ordinary run: open and drain are measured, other absorbs query building, command construction,
           the optional probe-failure rowset and the supplemental query. */
        var result = new CollectorRunResult(
            Rows: 150,
            SqlMs: 4644,
            StorageMs: 184,
            ServerPhasesMeasured: true,
            ServerOpenMs: 3900,
            ServerDrainMs: 700,
            ServerWatermarkMs: 12);

        Assert.Equal(44, result.ServerOtherMs);
        Assert.Equal(
            result.SqlMs,
            result.ServerOpenMs + result.ServerDrainMs + result.ServerOtherMs);
    }

    [Fact]
    public void TheResidualClampsAtZero_SoStopwatchSkewNeverPrintsNegative()
    {
        /* open + drain measured on separate stopwatches can exceed the parent by a millisecond or two. That
           must surface as zero, not as a negative term that makes the whole line look broken. */
        var skewed = new CollectorRunResult(
            Rows: 1,
            SqlMs: 100,
            StorageMs: 0,
            ServerPhasesMeasured: true,
            ServerOpenMs: 60,
            ServerDrainMs: 45);

        Assert.Equal(0, skewed.ServerOtherMs);
    }

    [Fact]
    public void AnUnmeasuredRunReportsNotMeasured_RatherThanAZeroThatLooksLikeAnInstantOpen()
    {
        /* The enumerated and Azure branches leave the flag false. The log site gates on the FLAG, so their
           zeros never print as a split — which is the distinction `PerItemOpenMs > 0` cannot make, because
           it reads a genuinely instant open and a path that measures nothing as the same thing. */
        var enumerated = new CollectorRunResult(Rows: 3956, SqlMs: 316065, StorageMs: 41);

        Assert.False(enumerated.ServerPhasesMeasured);
        Assert.Equal(0, enumerated.ServerOpenMs);
        Assert.Equal(0, enumerated.ServerDrainMs);

        /* And a measured run whose open really was instant still reports measured, with a zero that means
           what it says. */
        var instant = new CollectorRunResult(
            Rows: 0, SqlMs: 0, StorageMs: 0, ServerPhasesMeasured: true);

        Assert.True(instant.ServerPhasesMeasured);
        Assert.Equal(0, instant.ServerOtherMs);
    }

    [Fact]
    public void TheWatermarkIsCarriedButExcludedFromTheSum_BecauseItIsNotInsideSqlOnThisPath()
    {
        /* The server-scoped watermark read runs BEFORE the sql: stopwatch starts, so it is not part of
           SqlMs. #2851's own text assumes it is ("sql_duration_ms is wm: + open: + drain:"), and folding it
           into the decomposition would have printed a permanent wm:0ms — teaching every future reader that a
           store read #2796 clocked at 50s cold is free. It is carried and reported, outside the sum. */
        var result = new CollectorRunResult(
            Rows: 10,
            SqlMs: 1000,
            StorageMs: 5,
            ServerPhasesMeasured: true,
            ServerOpenMs: 600,
            ServerDrainMs: 300,
            ServerWatermarkMs: 5000);

        Assert.Equal(5000, result.ServerWatermarkMs);
        Assert.Equal(100, result.ServerOtherMs);
        Assert.Equal(
            result.SqlMs,
            result.ServerOpenMs + result.ServerDrainMs + result.ServerOtherMs);
    }

    [Fact]
    public void TheEnumeratedResidualIsHonest_WhenEveryPhaseOnThatPathIsStamped()
    {
        /* #2854. The enumerated arithmetic has two consumers and both mis-attribute when a phase is missing,
           so pin the shipped expressions rather than a copy: DrainMsFrom subtracts the non-streaming phases
           from the item total, and PlanFetchOtherMs subtracts the sub-phases from their parent. */
        var context = new CollectorContext
        {
            ServerId = 1, ServerName = "s", CollectionTime = new DateTime(2026, 9, 3, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            PerItemPhasesMeasured = true,
            PerItemWatermarkMs = 120,
            PerItemOpenMs = 900,
            PerItemPlanFetchMs = 1_500,
            PerItemTextFetchMs = 400,
            PerItemPlanProbeMs = 1_100,
            PerItemPlanTargetMs = 250,
            PerItemPlanWriteMs = 100,
        };

        /* 4,000 total: 120 wm + 900 open + 1,500 plan + 400 text leaves 1,080 genuinely streaming. */
        Assert.Equal(1_080, context.DrainMsFrom(4_000));

        /* And the sub-split sums to its parent: 1,500 - 1,100 - 250 - 100. */
        Assert.Equal(50, context.PlanFetchOtherMs);
        Assert.Equal(
            context.PerItemPlanFetchMs,
            context.PerItemPlanProbeMs + context.PerItemPlanTargetMs
                + context.PerItemPlanWriteMs + context.PlanFetchOtherMs);
    }

    [Fact]
    public void AZeroParentAboveNonZeroChildren_IsTheShapeTheStampFixPrevents()
    {
        /* The defect this issue fixed, expressed as arithmetic. If PerItemPlanFetchMs is skipped by a
           throwing await while the #2816-fixed sub-phases stamp from their handlers, the parent is zero and
           the children are not. The clamp then hides it: a negative residual prints as 0, so the line reads
           as a precise decomposition of a parent that is smaller than its own parts.

           Pinned as a NEGATIVE — this asserts the clamp still protects the log line from printing nonsense,
           and documents why a zero parent can never be trusted as "instant". The IL pin below is what stops
           the state arising; this one records what it looks like if it ever does. */
        var skipped = new CollectorContext
        {
            ServerId = 1, ServerName = "s", CollectionTime = new DateTime(2026, 9, 3, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new CollectorDeltaCalculator(),
            PerItemPhasesMeasured = true,
            PerItemPlanFetchMs = 0,
            PerItemPlanProbeMs = 1_100,
            PerItemPlanTargetMs = 250,
            PerItemPlanWriteMs = 100,
        };

        Assert.Equal(0, skipped.PlanFetchOtherMs);
        Assert.True(
            skipped.PerItemPlanProbeMs + skipped.PerItemPlanTargetMs + skipped.PerItemPlanWriteMs
                > skipped.PerItemPlanFetchMs,
            "The children exceeding the parent is the tell. If this ever stops holding, the arithmetic " +
            "changed and the IL pin below is the only thing left guarding the stamps.");
    }

    /* ── #2855: the Azure per-database split ── */

    [Fact]
    public void ThePerDatabaseSplitSumsToItsParent_SoTheResidualIsAResidualAndNotASlushFund()
    {
        /* Three phases here, not two, and the third is the point: the connect is a real per-cycle cost on
           Azure SQL DB because this branch logs in once per database. Pin the SHIPPED expressions — the
           residual method and the gate the log site calls — rather than a copy of the arithmetic.

           The figures are INVENTED, and deliberately not borrowed from any of the measurements quoted in
           this file's header: nothing has yet measured this branch against a live Azure SQL DB target, and a
           fixture that echoed a real number from another path would read as if something had. */
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 1_200;
        context.PerDatabaseOpenMs = 400;
        context.PerDatabaseDrainMs = 150;

        /* 1,800 total: 1,200 connect + 400 open + 150 drain leaves 50 in command construction, the trailing
           probe-failure rowset, and the teardown of the reader, command and connection — which on this path
           is one close per database and sits inside the parent stopwatch. */
        Assert.Equal(50, context.PerDatabaseOtherMsFrom(1_800));

        var split = Assert.NotNull(context.PerDatabasePhasesFrom(1_800));
        Assert.Equal(
            1_800,
            split.ConnectMs + split.OpenMs + split.DrainMs + split.OtherMs);
    }

    [Fact]
    public void ThePerDatabaseResidualClampsAtZero_SoStopwatchSkewNeverPrintsNegative()
    {
        /* THREE stopwatches against one parent now, so the skew this clamps is a millisecond larger than the
           server-scoped case, not smaller. A negative term would make the whole line look broken. */
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 60;
        context.PerDatabaseOpenMs = 45;
        context.PerDatabaseDrainMs = 10;

        Assert.Equal(0, context.PerDatabaseOtherMsFrom(100));
        Assert.Equal(0, Assert.NotNull(context.PerDatabasePhasesFrom(100)).OtherMs);
    }

    [Fact]
    public void AnUnmeasuredDatabaseEmitsNoSplit_SoTheLogLineCannotPrintOneItDidNotMeasure()
    {
        /* Lite does not measure this path. The gate returns null, the log site's `is { }` pattern fails, and
           no line prints — which is the honest answer for a host that measured nothing.

           The values are set NON-ZERO here on purpose: they stand in for a stale split left by a previous
           database in the same loop, and the assertion is that the FLAG alone decides, so a leftover figure
           can never be printed as this database's own. */
        var context = PerDatabaseContext();
        context.PerDatabaseConnectMs = 4_100;
        context.PerDatabaseOpenMs = 700;
        context.PerDatabaseDrainMs = 90;

        Assert.False(context.PerDatabasePhasesMeasured);
        Assert.Null(context.PerDatabasePhasesFrom(5_000));
    }

    [Fact]
    public void AMeasuredZeroConnectStillEmitsItsSplit_WhichIsWhyTheGateIsAFlagAndNotAGreaterThanZero()
    {
        /* THE case a `connect: > 0` gate loses, and the #2854 mistake one path over. A pooled or
           already-warm connect really does measure 0ms while the read behind it costs seconds — and that is
           precisely the database whose open: and drain: are worth reading, because the connect has been
           ruled out. Gating on the value would suppress the entire line for it, and a reader could not tell
           that from a host that emits no split at all. */
        var pooled = PerDatabaseContext();
        pooled.PerDatabasePhasesMeasured = true;
        pooled.PerDatabaseConnectMs = 0;
        pooled.PerDatabaseOpenMs = 1_600;
        pooled.PerDatabaseDrainMs = 150;

        var split = Assert.NotNull(pooled.PerDatabasePhasesFrom(1_800));
        Assert.Equal(0, split.ConnectMs);
        Assert.Equal(50, split.OtherMs);

        /* And a database where every phase legitimately measured zero still declares itself measured, with
           zeros that mean what they say rather than an absent line. */
        var instant = PerDatabaseContext();
        instant.PerDatabasePhasesMeasured = true;

        var zeroes = Assert.NotNull(instant.PerDatabasePhasesFrom(0));
        Assert.Equal(new PerDatabasePhaseSplit(0, 0, 0, 0), zeroes);
    }

    [Fact]
    public void TheThreePathsDoNotMasqueradeAsEachOther_WhichIsWhyTheThirdPrefixExists()
    {
        /* The reason PerDatabase*Ms is not PerItem*Ms. An enumerated run stamps its own fields and must not
           acquire a per-database line it has no connect for; a per-database run must not acquire the
           enumerated one, which would print an item name it does not have. Reusing PerItemOpenMs would have
           made both happen at once. */
        var enumerated = PerDatabaseContext();
        enumerated.PerItemPhasesMeasured = true;
        enumerated.PerItemOpenMs = 900;

        Assert.Null(enumerated.PerDatabasePhasesFrom(4_000));

        var perDatabase = PerDatabaseContext();
        perDatabase.PerDatabasePhasesMeasured = true;
        perDatabase.PerDatabaseConnectMs = 1_200;

        Assert.NotNull(perDatabase.PerDatabasePhasesFrom(1_800));
        Assert.False(perDatabase.PerItemPhasesMeasured);
        Assert.Equal(0, perDatabase.PerItemOpenMs);

        /* And neither of them is the server-scoped path, whose own flag stays false on both. */
        Assert.False(new CollectorRunResult(Rows: 1, SqlMs: 1, StorageMs: 0).ServerPhasesMeasured);
    }

    [Fact]
    public void TheAzureLogSiteGatesOnTheSharedExpression_NotOnAFigureBeingNonZero()
    {
        /* The emission itself takes a live ILogger, a live server and a live Azure connection, so it is
           pinned at source the way the probe-failure log lines are: the gate must be the shared
           PerDatabasePhasesFrom expression the tests above exercise, never a hand-written comparison that
           could drift back to the `> 0` form #2854 removed. */
        var source = ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        Assert.Contains("context.PerDatabasePhasesFrom(dbSqlMs) is { } dbPhases", source, StringComparison.Ordinal);
        Assert.Contains("connect:{ConnectMs}ms + open:{OpenMs}ms + drain:{DrainMs}ms + other:{OtherMs}ms", source, StringComparison.Ordinal);

        Assert.DoesNotContain("PerDatabaseConnectMs > 0", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PerDatabaseOpenMs > 0", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PerDatabaseDrainMs > 0", source, StringComparison.Ordinal);
    }

    private static CollectorContext PerDatabaseContext() => new()
    {
        ServerId = 1,
        ServerName = "alpha",
        CollectionTime = new DateTime(2026, 9, 4, 0, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
    };

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }

    [Fact]
    public void PhaseStamps_AreReachableFromExceptionHandlers_SoAThrowingPhaseStillReportsItsTime()
    {
        Assert.True(
            PhaseSetters.Length >= MinimumPhaseStamps,
            $"Only {PhaseSetters.Length} phase stamps were derived from CollectorContext, expected at least " +
            $"{MinimumPhaseStamps}. A stamp was deleted, or renamed out of the PerItem*Ms / ServerScope*Ms / " +
            "PerDatabase*Ms pattern, so this pin is now guarding less than it was written to guard.");

        var counts = ScanServiceAssembly();

        var (controlTotal, controlInHandler) = counts[ControlSetter];
        Assert.True(
            controlTotal > 0,
            $"The control setter {ControlSetter} resolved to zero call sites — the scanner read nothing, so " +
            "the assertions below would pass or fail for reasons unrelated to the defect.");
        Assert.Equal(0, controlInHandler);

        foreach (var setter in PhaseSetters)
        {
            var (total, inHandler) = counts[setter];

            Assert.True(
                total > 0,
                $"{setter} was not called anywhere in the service assembly — either the stamp was removed or " +
                "the scanner resolved nothing. Either way this test can say nothing about reachability.");

            Assert.True(
                inHandler > 0,
                $"{setter} is never invoked from inside an exception handler ({total} call site(s), all on " +
                "success paths). A phase that throws — a command timeout on the open, a budget expiry mid-drain — " +
                "will report 0ms and its cost lands in the other: residual, which is documented as time spent " +
                "in neither the target nor the store. That is the #2816 defect, on a new path.");
        }
    }

    [Fact]
    public void MeasuredFlags_AreReachableFromExceptionHandlers_SoAFaultingPhaseStillDeclaresItselfMeasured()
    {
        Assert.True(
            MeasuredFlagSetters.Length >= MinimumMeasuredFlags,
            $"Only {MeasuredFlagSetters.Length} measured flags were derived from CollectorContext, expected " +
            $"at least {MinimumMeasuredFlags}. A flag was deleted or renamed out of the *PhasesMeasured " +
            "pattern, so this pin is now guarding less than it was written to guard.");

        var counts = ScanServiceAssembly();

        foreach (var setter in MeasuredFlagSetters)
        {
            var (total, inHandler) = counts[setter];

            Assert.True(
                total > 0,
                $"{setter} was not called anywhere in the service assembly — the flag is never set, so its " +
                "log line can never print and the split is dead code.");

            Assert.True(
                inHandler > 0,
                $"{setter} is never set from inside an exception handler ({total} call site(s), all on " +
                "success paths). The flag GATES the split line, so a phase that faults would declare itself " +
                "unmeasured and print nothing at all — losing the reading for the timed-out connect or the " +
                "expired budget, which is the only case the split was added to explain. #2854, one flag over.");
        }
    }

    /// <summary>
    /// For each tracked setter, how many times it is called in the built service assembly and how many of
    /// those calls sit inside an exception-handler region. The walk itself lives in
    /// <see cref="IlCallSiteScanner"/> — this pin carried its own copy until #2898, and that copy advanced its
    /// cursor four bytes past a match, which can step over a genuine call instruction's own token and report a
    /// stamp that IS called from a handler as never called from one. It also could not see a call to a GENERIC
    /// member at all, which is a live trap for this pin specifically: the tracked set here is DERIVED, so a
    /// generic stamp or flag added tomorrow would be picked up by the derivation and then read as uncalled.
    /// </summary>
    private static Dictionary<string, (int Total, int InHandler)> ScanServiceAssembly()
    {
        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        return IlCallSiteScanner.CountCalls(
            assemblyPath,
            PhaseSetters.Concat(MeasuredFlagSetters).Append(ControlSetter));
    }
}
