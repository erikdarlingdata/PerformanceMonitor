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
       quietly cover less. */
    private static readonly string[] PhaseSetters =
        typeof(CollectorContext)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.PropertyType == typeof(long)
                        && p.CanWrite
                        && p.Name.EndsWith("Ms", StringComparison.Ordinal)
                        && (p.Name.StartsWith("PerItem", StringComparison.Ordinal)
                            || p.Name.StartsWith("ServerScope", StringComparison.Ordinal)))
            .Select(p => "set_" + p.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

    /* The count at the time this pin was written: 10 enumerated + 2 server-scoped. Deleting a stamp, or
       renaming one out of the pattern above, drops the derived set and fails HERE with a number rather than
       silently checking less. */
    private const int MinimumPhaseStamps = 12;

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

    [Fact]
    public void PhaseStamps_AreReachableFromExceptionHandlers_SoAThrowingPhaseStillReportsItsTime()
    {
        Assert.True(
            PhaseSetters.Length >= MinimumPhaseStamps,
            $"Only {PhaseSetters.Length} phase stamps were derived from CollectorContext, expected at least " +
            $"{MinimumPhaseStamps}. A stamp was deleted or renamed out of the PerItem*Ms / ServerScope*Ms " +
            "pattern, so this pin is now guarding less than it was written to guard.");

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

    /// <summary>
    /// For each tracked setter, how many times it is called in the built service assembly and how many of
    /// those calls sit inside an exception-handler region. The walk itself lives in
    /// <see cref="IlCallSiteScanner"/> — this pin carried its own copy until #2898, and that copy advanced its
    /// cursor four bytes past a match, which can step over a genuine call instruction's own token and report a
    /// stamp that IS called from a handler as never called from one.
    /// </summary>
    private static Dictionary<string, (int Total, int InHandler)> ScanServiceAssembly()
    {
        var assemblyPath = typeof(DarlingCollectorRunner).Assembly.Location;
        Assert.True(File.Exists(assemblyPath), $"Service assembly not found at '{assemblyPath}'.");

        return IlCallSiteScanner.CountCalls(assemblyPath, PhaseSetters.Append(ControlSetter));
    }
}
