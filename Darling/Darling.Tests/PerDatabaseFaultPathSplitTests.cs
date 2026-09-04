/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para>Pins the per-database FAULT path's phase line (#2896) — the split #2855 measured on every path and
/// printed on one.</para>
///
/// <para><b>Why it exists.</b> #2855 stamped <c>PerDatabaseConnectMs</c> and
/// <c>PerDatabasePhasesMeasured</c> from a <c>finally</c>, so a database whose connect times out really does
/// record its phases. Its only log site sat on the success path past the flush, and a fault reaches one of
/// three catch arms instead — so the one case the split was added to attribute, "this database is bound by
/// connect", produced an error string and nothing else. The generic arm's is at Debug, so on a default log
/// level it produced only the error string.</para>
///
/// <para><b>The obstacle, and why the obvious fix was wrong.</b> The line decomposes <c>dbSqlMs</c>, and
/// <c>dbSqlMs</c> is not log-only: it feeds <c>sqlMs</c>, <c>fanout.Observe(...)</c> and
/// <c>collection_log.sql_duration_ms</c>. The stopwatch was declared inside the <c>try</c>, below the
/// watermark read and <c>BuildQuery</c>, so hoisting it to reach the catch would have started it earlier and
/// silently widened all three — a persisted metric moved to make a log line reachable.</para>
///
/// <para><b>What was done instead</b>, and it is neither of the two options #2896 recorded. Only the
/// DECLARATION moved above the <c>try</c>; the START stayed on the same statement. The interval is byte-for-
/// byte the one it always was, so <c>dbSqlMs</c> is unchanged and no second stopwatch measures a second
/// interval a reader would have to reconcile with the first. The two options both had a cost this does not
/// pay: a second stopwatch started above the <c>try</c> would not have measured the same interval at all —
/// <c>PerDatabaseOtherMsFrom</c>'s contract says the watermark read and <c>BuildQuery</c> are OUTSIDE the
/// parent, so its residual would have meant something different from the success line's under the same name.
/// A second, parentless line shape needed nothing hoisted but taught the log parsers outside this repo a
/// second shape, which is the #2811/#2851 objection.</para>
///
/// <para><b>The clear had to move too, and that is the part no measurement suggested.</b> #2855 cleared the
/// stamps just below the stopwatch — before the CONNECT, which was sufficient while only the success path
/// read them. The catch arms read them now, and a fault in the watermark read or <c>BuildQuery</c> reaches
/// those arms WITHOUT having passed the old clear, so the flag and the three figures would still hold the
/// PREVIOUS database's values and the fault line would have confidently attributed another database's
/// connect to this one. That is the exact failure #2855's clear exists to prevent, on the half of the
/// iteration it did not cover.</para>
///
/// <para><b>What this does NOT cover.</b> No Azure SQL DB target exists in the monitored fleet, so nothing
/// here has been exercised against the branch's SQL Server arm end to end; only Windows CI runs the suite,
/// and only a live Azure or Aurora target can settle the emission's shape in production. The seven
/// PostgreSQL collectors that return <c>RunsPerDatabase = true</c> unconditionally do reach this loop on
/// every Aurora target, which is why the branch is not dead code — but the log volume that follows from the
/// level decision below is asserted here as a level, not measured as a rate.</para>
/// </summary>
public sealed class PerDatabaseFaultPathSplitTests
{
    /* Parsed out of the EMITTED line rather than compared against a rebuilt string, so the assertion is
       against what an operator would actually read. A template-shaped comparison would pass while the
       rendered numbers disagreed with each other, which is the failure mode that matters here. */
    private static readonly Regex SplitShape = new(
        @"sql:(?<sql>\d+)ms = connect:(?<connect>\d+)ms \+ open:(?<open>\d+)ms \+ drain:(?<drain>\d+)ms \+ other:(?<other>\d+)ms",
        RegexOptions.CultureInvariant);

    [Fact]
    public void TheFaultPathEmitsTheSplit_WhichIsTheWholeOfWhatWasMissing()
    {
        var logger = new Recorder();
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 1;
        context.PerDatabaseOpenMs = 1;
        context.PerDatabaseDrainMs = 1;

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Warning, context, RunningSlice(), "alpha", "query_store", "beta", "budget expired");

        var record = Assert.Single(logger.Records);
        Assert.Equal(LogLevel.Warning, record.Level);
        Assert.Contains("[beta]", record.Message, StringComparison.Ordinal);
        Assert.Contains("query_store", record.Message, StringComparison.Ordinal);
        Assert.Contains("budget expired", record.Message, StringComparison.Ordinal);
        Assert.Matches(SplitShape, record.Message);

        /* The tail is NOT the success line's. There were no rows and there was no flush, so "(0 rows,
           pg:0ms)" would make a failed database read as a quiet one — the same mistake in the opposite
           direction from the one #2854 undid. */
        Assert.DoesNotContain("rows, pg:", record.Message, StringComparison.Ordinal);
        Assert.Contains("nothing stored", record.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheLevelIsTheCallersNotTheMethods_SoTheSplitCanRideItsArm()
    {
        /* The budget arm is a Warning and the generic arm is Debug, and the split must not be louder than
           the error it decomposes. A method that picked its own level could not honour both. */
        var logger = new Recorder();
        var context = MeasuredContext();

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Debug, context, RunningSlice(), "alpha", "pg_index_bloat", "beta", "failed");

        Assert.Equal(LogLevel.Debug, Assert.Single(logger.Records).Level);
    }

    [Fact]
    public void AnUnmeasuredDatabaseEmitsNothing_SoTheLastDatabasesSplitCannotBePrintedAsThisOnes()
    {
        /* THE hazard the moved clear exists for, expressed at the emission. The figures are non-zero and the
           flag is false: that is exactly the state a fault in the watermark read leaves behind if the clear
           sits below the stopwatch, because the previous iteration's finally set all three and its own flag.
           Gating on the flag is what makes that unprintable. */
        var logger = new Recorder();
        var context = PerDatabaseContext();
        context.PerDatabaseConnectMs = 4_100;
        context.PerDatabaseOpenMs = 700;
        context.PerDatabaseDrainMs = 90;

        Assert.False(context.PerDatabasePhasesMeasured);

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Warning, context, RunningSlice(), "alpha", "query_store", "beta", "budget expired");

        Assert.Empty(logger.Records);
    }

    [Fact]
    public void AFaultBeforeTheTimedRegionEmitsNothing_BecauseThereIsNoParentToDecompose()
    {
        /* The watermark read, the adaptive shrink's store write and BuildQuery all run before the stopwatch
           starts, so a fault in any of them arrives with a null slice. The flag is set TRUE here on purpose:
           the null test has to hold on its own, because the day the clear moves back the flag stops being a
           sufficient guard and this is the only thing standing between a null and a NullReferenceException
           inside a catch handler. */
        var logger = new Recorder();
        var context = MeasuredContext();

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Warning, context, null, "alpha", "query_store", "beta", "budget expired");

        Assert.Empty(logger.Records);
    }

    [Fact]
    public void ThePrintedTermsSumToThePrintedParent_BecauseTheSliceIsReadExactlyOnce()
    {
        /* #2472's read-once rule, on the fault path. The stopwatch is STILL RUNNING in the handler, so a
           second read a few statements later returns a larger number: the residual would have been computed
           against the smaller parent and the printed terms would not sum to the printed total. A line that
           does not add up is the one outcome worse than no line, because it looks precise.

           The stamps are deliberately tiny against a slice spun past them, so `other` is comfortably
           positive and the clamp is not what is being tested here. */
        var logger = new Recorder();
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 2;
        context.PerDatabaseOpenMs = 1;
        context.PerDatabaseDrainMs = 1;

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Warning, context, RunningSlice(minimumElapsedMs: 25),
            "alpha", "query_store", "beta", "budget expired");

        var match = SplitShape.Match(Assert.Single(logger.Records).Message);
        Assert.True(match.Success, "The emitted line did not carry a parseable split.");

        var sql = Group(match, "sql");
        var connect = Group(match, "connect");
        var open = Group(match, "open");
        var drain = Group(match, "drain");
        var other = Group(match, "other");

        Assert.Equal(2, connect);
        Assert.Equal(1, open);
        Assert.Equal(1, drain);
        Assert.Equal(
            sql,
            connect + open + drain + other);
    }

    [Fact]
    public void TheResidualStillClampsOnTheFaultPath_SoAShortFaultNeverPrintsNegative()
    {
        /* Three stopwatches against one parent can overshoot it, and that has to print as other:0ms rather
           than as a negative term that makes the whole line look broken. Shared with the success path
           through PerDatabaseOtherMsFrom, asserted here because the fault path is where the overshoot is
           likeliest: the parent stops accumulating at the throw while the stamps are already in.

           The stamps are absurdly large against the parent ON PURPOSE — a realistic millisecond of skew
           would make this test flaky on a loaded CI runner, and what is under test is the clamp, not the
           plausibility of the figures. */
        var logger = new Recorder();
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 600_000;
        context.PerDatabaseOpenMs = 600_000;
        context.PerDatabaseDrainMs = 600_000;

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Warning, context, Stopwatch.StartNew(), "alpha", "query_store", "beta", "failed");

        var match = SplitShape.Match(Assert.Single(logger.Records).Message);
        Assert.True(match.Success, "The emitted line did not carry a parseable split.");
        Assert.Equal(0, Group(match, "other"));
    }

    [Fact]
    public void AStoppedSliceFreezesTheParent_SoAFlushTimeFaultPrintsWhatTheMetricAlreadyRecorded()
    {
        /* The #2896 REVIEW catch, and the one case the tests above could not see. The runner captures
           dbSqlMs and folds it into sqlMs and fanout.Observe, and THEN flushes — and the flush runs on
           cancellationToken rather than the per-database budget, so a store write that throws lands in a
           fault arm holding the same stopwatch. While that stopwatch was still running, the arm re-read it
           and printed a parent LARGER than the one already persisted for this database, with the whole
           difference falling into other:, because connect/open/drain are fixed by their own finally blocks.
           Storage latency reported as unattributed SQL-side residual is the precise mis-attribution this
           instrumentation exists to prevent.

           Stopping the slice at the capture point is what makes the two paths agree by construction. The
           assertion is that a STOPPED slice still prints, and prints the frozen figure — measured across a
           real delay after the stop, because the defect was that time kept accruing. */
        var logger = new Recorder();
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 2;
        context.PerDatabaseOpenMs = 1;
        context.PerDatabaseDrainMs = 1;

        var slice = RunningSlice(minimumElapsedMs: 25);
        slice.Stop();
        var frozen = slice.ElapsedMilliseconds;

        /* Stand in for the flush: real elapsed time between the capture and the fault arm's read. */
        Thread.Sleep(60);

        DarlingCollectorRunner.LogPerDatabaseFaultSplit(
            logger, LogLevel.Debug, context, slice, "alpha", "query_store", "beta", "failed");

        var match = SplitShape.Match(Assert.Single(logger.Records).Message);
        Assert.True(match.Success, "The emitted line did not carry a parseable split.");

        Assert.Equal(frozen, Group(match, "sql"));
        Assert.Equal(
            frozen,
            Group(match, "connect") + Group(match, "open") + Group(match, "drain") + Group(match, "other"));

        /* The control the assertion needs: the delay really was long enough that a RUNNING slice would have
           reported a different, larger parent. Without this the equality above could hold simply because no
           measurable time passed, and the pin would guard nothing. */
        var stillRunning = RunningSlice(minimumElapsedMs: 25);
        var firstRead = stillRunning.ElapsedMilliseconds;
        Thread.Sleep(60);
        Assert.True(
            stillRunning.ElapsedMilliseconds > firstRead,
            "A running stopwatch did not advance across the same delay, so this test cannot distinguish a " +
            "frozen slice from a live one and proves nothing.");
    }

    /* ── the shape pins: what the runner must keep true for the behaviour above to be reachable ── */

    [Fact]
    public void BothFaultArmsEmitTheSplit_AtTheirOwnLevel()
    {
        var branch = PerDatabaseBranch();

        /* One call per arm, and the LEVEL is what distinguishes them. Warning for the budget arm because a
           collector that could not finish is not routine; Debug for the generic arm because one offline
           database is. Losing an arm, or flipping a level, changes exactly one of these counts. */
        Assert.Equal(1, Occurrences(branch, "_logger, LogLevel.Warning, context, sqlSlice,"));
        Assert.Equal(1, Occurrences(branch, "_logger, LogLevel.Debug, context, sqlSlice,"));

        /* Two call sites and no third, counted on a needle with NO LINE BREAK in it — an anchor spanning one
           would match on an LF checkout and silently stop matching on a CRLF one, and a count that quietly
           becomes zero reads as a pass. */
        Assert.Equal(2, Occurrences(branch, ", context, sqlSlice,"));

        /* The outcome word each arm passes, so the two lines are distinguishable in a log without diffing
           their numbers. */
        Assert.Contains("databaseName, \"budget expired\");", branch, StringComparison.Ordinal);
        Assert.Contains("databaseName, \"failed\");", branch, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGenericArmStaysAtDebug_BecauseThePersistedNoteIsWhatMakesARepeatFailureVisible()
    {
        /* The level question #2896 raises, answered rather than left open. The per-database arm is NOT what
           makes a database that has failed every cycle for a week visible: BuildPartialFailureNote is, and it
           lands in collection_log where it is queryable and where get_collection_health reports it as
           last_note / note_count. Raising this arm would emit one Warning per database per cycle per
           collector, which on a target with many databases is the flood #1875's one-capped-burst rule exists
           to stop — and it would not add a signal that is not already persisted.

           Pinned as the SHAPE of the skip line, so a later change that raises it has to come back through
           this reasoning rather than doing it by reflex. */
        var runner = RunnerSource();

        Assert.Contains(
            "_logger?.LogDebug(\"Skipping database '{Database}' for {Collector}: {Error}\"",
            runner,
            StringComparison.Ordinal);

        /* And the aggregate channel it defers to still exists, on this branch, with the names in it. If this
           ever stops holding, the Debug level above stops being defensible and both should change together. */
        Assert.Contains("EnumeratedCollectorDriver.BuildPartialFailureNote(", runner, StringComparison.Ordinal);
        Assert.Contains("failed, attempted, failedDatabases, firstFailure?.Message)", runner, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyTheDeclarationWasHoisted_SoDbSqlMsIsNotWidened()
    {
        /* THE pin this change exists to earn. dbSqlMs feeds sqlMs, fanout.Observe and
           collection_log.sql_duration_ms, so starting the slice above the watermark read and BuildQuery to
           make it reachable from the catch would widen three persisted numbers to satisfy a log line. The
           declaration is hoisted; the START is not, and the ORDER is what proves it.

           Scoped to the per-database branch, because the PLAIN branch further down declares its own
           `var sqlSlice = Stopwatch.StartNew();` — and the first cut of this pin counted whole-file, matched
           that one as well, and failed at 2. An anchor that matches somewhere you were not looking is the
           same defect as one that matches nothing; it just fails louder. */
        var branch = PerDatabaseBranch();

        var declaration = branch.IndexOf("Stopwatch? sqlSlice = null;", StringComparison.Ordinal);
        var tryOpen = branch.IndexOf("context.CurrentDatabaseName = databaseName;", StringComparison.Ordinal);
        var watermarkRead = branch.IndexOf(
            "context.Watermark = await GetLastCollectedTimeForDatabaseAsync(", StringComparison.Ordinal);
        var buildQuery = branch.IndexOf("dbPlan = definition.BuildQuery(context);", StringComparison.Ordinal);
        var start = branch.IndexOf("sqlSlice = Stopwatch.StartNew();", StringComparison.Ordinal);

        Assert.True(declaration >= 0, "The hoisted declaration is gone — the catch arms cannot see the slice.");
        Assert.True(start >= 0, "The slice is never started.");
        Assert.True(watermarkRead >= 0 && buildQuery >= 0 && tryOpen >= 0, "The ordering anchors moved.");

        Assert.True(declaration < tryOpen, "The declaration must sit ABOVE the try to be visible in the catch.");
        Assert.True(
            start > watermarkRead,
            "The slice is started ABOVE the per-database watermark read, so dbSqlMs now includes a STORE " +
            "read — and dbSqlMs is not log-only: it feeds sqlMs, fanout.Observe and " +
            "collection_log.sql_duration_ms. That is a persisted metric widened to make a log line reachable.");
        Assert.True(
            start > buildQuery,
            "The slice is started ABOVE BuildQuery, so dbSqlMs now includes query construction — see above; " +
            "PerDatabaseOtherMsFrom's contract states both are OUTSIDE the parent.");

        /* And there is exactly ONE stopwatch over that interval, not the second one #2896 offered as
           option 1: two would have to be kept in step by hand, and the day they drift the fault line's
           other: means something different from the success line's under the same name. */
        Assert.Equal(1, Occurrences(branch, "sqlSlice = Stopwatch.StartNew();"));

        /* And it is an ASSIGNMENT to the hoisted local, not a fresh declaration shadowing it — which is how
           a well-meaning simplification would put the slice back out of the catch's reach while leaving
           every other assertion in this test green. */
        Assert.Equal(0, Occurrences(branch, "var sqlSlice = Stopwatch.StartNew();"));

        /* And it is STOPPED before the parent is captured (the review catch). The ORDER is the whole
           assertion: stopping AFTER the read leaves the read live, and stopping at all is what keeps a
           flush-time fault from printing a parent larger than the one already folded into sqlMs. */
        var stop = branch.IndexOf("sqlSlice.Stop();", StringComparison.Ordinal);
        var capture = branch.IndexOf("var dbSqlMs = sqlSlice.ElapsedMilliseconds;", StringComparison.Ordinal);
        var accumulate = branch.IndexOf("sqlMs += dbSqlMs;", StringComparison.Ordinal);

        Assert.True(stop >= 0, "The slice is never stopped, so it keeps accruing into the fault arms' read.");
        Assert.True(capture >= 0 && accumulate >= 0, "The dbSqlMs capture or its accumulator moved.");
        Assert.True(
            stop > start,
            "The slice is stopped before it is started.");
        Assert.True(
            stop < capture,
            "The slice is stopped AFTER dbSqlMs is captured, so the captured value is a live read and a " +
            "fault during the flush still prints a larger parent than sqlMs recorded.");
        Assert.True(
            capture < accumulate,
            "dbSqlMs is accumulated before it is captured.");

        /* Positive control for that zero: the identical counter, in the identical string, against the form
           that IS there. A zero from a needle that could never have matched anything proves nothing. */
        Assert.Equal(1, Occurrences(branch, "Stopwatch? sqlSlice = null;"));
    }

    [Fact]
    public void TheClearPrecedesTheWatermarkRead_SoAPreConnectFaultCannotInheritAStaleSplit()
    {
        /* #2855 cleared below the stopwatch, which covered every fault the SUCCESS path could see. The catch
           arms see more: a fault in the watermark read, in the adaptive shrink's store write, or in
           BuildQuery reaches them without passing the old position, and would arrive holding the previous
           database's flag and figures. */
        var branch = PerDatabaseBranch();

        var flagClear = branch.IndexOf("context.PerDatabasePhasesMeasured = false;", StringComparison.Ordinal);
        var connectClear = branch.IndexOf("context.PerDatabaseConnectMs = 0;", StringComparison.Ordinal);
        var watermarkRead = branch.IndexOf(
            "context.Watermark = await GetLastCollectedTimeForDatabaseAsync(", StringComparison.Ordinal);

        Assert.True(flagClear >= 0 && connectClear >= 0, "The per-iteration clear is gone.");
        Assert.True(watermarkRead >= 0, "The per-database watermark read moved.");

        Assert.True(
            connectClear < watermarkRead,
            "The per-database stamp clear now runs AFTER the watermark read, so a fault in that read reaches " +
            "a catch arm holding the PREVIOUS database's connect/open/drain and its flag — and the fault " +
            "line would print them as this database's own.");
        Assert.True(
            flagClear < watermarkRead,
            "The measured FLAG is cleared after the watermark read, so a pre-connect fault declares itself " +
            "measured on the strength of the last database's run. The flag is the whole gate.");
    }

    [Fact]
    public void TheSuccessLineAndTheAccumulatorAreUntouched_SoTheFaultPathFeedsNoPersistedMetric()
    {
        var runner = RunnerSource();

        /* The success site keeps its own shape and its own gate. */
        Assert.Contains("context.PerDatabasePhasesFrom(dbSqlMs) is { } dbPhases", runner, StringComparison.Ordinal);
        Assert.Contains("({Rows} rows, pg:{PgMs}ms)", runner, StringComparison.Ordinal);

        /* And dbSqlMs reaches sqlMs and the fan-out from ONE place each, both on the success path past the
           flush. The fault path prints a reading and feeds nothing. */
        Assert.Equal(1, Occurrences(runner, "sqlMs += dbSqlMs;"));
        Assert.Equal(1, Occurrences(runner, "fanout.Observe(databaseName, dbSqlMs + dbStorageMs);"));

        /* Positive control for the count assertions above: the identical counter finds a string the file
           really does carry more than once, so a mistyped needle cannot make them pass by matching nothing
           the way a DoesNotContain sweep can. */
        Assert.True(
            Occurrences(runner, "context.PerDatabaseConnectMs") > 1,
            "The occurrence counter itself found nothing it should have found — the assertions above would " +
            "have passed for the wrong reason.");
    }

    [Fact]
    public void TheGateIsTheSharedExpression_NeverAHandWrittenGreaterThanZero()
    {
        var runner = RunnerSource();
        var helper = HelperBody(runner);

        Assert.Contains("context.PerDatabasePhasesFrom(databaseSqlMs) is not { } phases", helper, StringComparison.Ordinal);

        /* The #2854 form, on the new path. A `> 0` gate suppresses the whole line for a pooled connect that
           really did cost nothing, which on the fault path is the reading most worth having. */
        Assert.DoesNotContain("PerDatabaseConnectMs > 0", helper, StringComparison.Ordinal);
        Assert.DoesNotContain("PerDatabaseOpenMs > 0", helper, StringComparison.Ordinal);
        Assert.DoesNotContain("PerDatabaseDrainMs > 0", helper, StringComparison.Ordinal);

        /* Positive control for the three sweeps above: the identical containment check, in the identical
           substring, against a needle the helper really does contain. Without it all three would pass on a
           helper this test failed to locate at all — which is how a negative-proving check reads as clean
           while proving nothing. */
        Assert.Contains("PerDatabasePhasesFrom", helper, StringComparison.Ordinal);
    }

    private static int Group(Match match, string name) =>
        int.Parse(match.Groups[name].Value, CultureInfo.InvariantCulture);

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal);
             i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    /// <summary>
    /// The helper's own text, sliced out so the <c>DoesNotContain</c> sweeps above cannot pass by looking at
    /// the wrong part of a 4,000-line file — and cannot fail against a COMMENT elsewhere that happens to
    /// quote the form being ruled out.
    /// </summary>
    private static string HelperBody(string runner)
    {
        var start = runner.IndexOf(
            "internal static void LogPerDatabaseFaultSplit(", StringComparison.Ordinal);
        Assert.True(start >= 0, "LogPerDatabaseFaultSplit is gone from the runner.");

        var end = runner.IndexOf("public async Task<CollectorRunResult> RunAsync<TRow>(", start, StringComparison.Ordinal);
        Assert.True(end > start, "Could not find the end of the helper.");

        return runner[start..end];
    }

    /// <summary>
    /// A stopwatch left RUNNING, as it is in the handler, spun past a floor so the parent is comfortably
    /// larger than the stamps under test. Spun rather than slept: a sleep asserts the scheduler's behaviour,
    /// a spin asserts the stopwatch's.
    /// </summary>
    private static Stopwatch RunningSlice(int minimumElapsedMs = 5)
    {
        var slice = Stopwatch.StartNew();
        while (slice.ElapsedMilliseconds < minimumElapsedMs)
        {
            Thread.SpinWait(2_000);
        }

        return slice;
    }

    private static CollectorContext MeasuredContext()
    {
        var context = PerDatabaseContext();
        context.PerDatabasePhasesMeasured = true;
        context.PerDatabaseConnectMs = 1;

        return context;
    }

    private static CollectorContext PerDatabaseContext() => new()
    {
        ServerId = 1,
        ServerName = "alpha",
        CollectionTime = new DateTime(2026, 9, 4, 0, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
    };

    private static string RunnerSource() =>
        ReadRepoFile(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

    /// <summary>
    /// Just the per-database branch, so an ordering or counting assertion cannot be answered by the PLAIN
    /// branch further down — which declares its own <c>sqlSlice</c> stopwatch and would otherwise be counted
    /// as a second one on this path.
    /// </summary>
    private static string PerDatabaseBranch()
    {
        var runner = RunnerSource();

        var start = runner.IndexOf("if (definition.RunsPerDatabase(context.Target))", StringComparison.Ordinal);
        Assert.True(start >= 0, "The per-database branch gate moved — this slicer is looking at nothing.");

        var end = runner.IndexOf("context.CurrentDatabaseName = null;", start, StringComparison.Ordinal);
        Assert.True(end > start, "The per-database branch's closing anchor moved.");

        return runner[start..end];
    }

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

    private sealed class Recorder : ILogger
    {
        public List<(LogLevel Level, string Message)> Records { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Records.Add((logLevel, formatter(state, exception)));
    }
}
