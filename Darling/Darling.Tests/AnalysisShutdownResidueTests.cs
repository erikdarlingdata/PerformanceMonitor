/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A clean stop must not read as seven faults (#2299).
///
/// <para><b>Observed on the dogfood box, 2026-08-16, on two separate stops.</b> The analysis pass is
/// started per sweep but was neither awaited nor cancellable, so <c>Stop-Service</c> disposed the loop's
/// data source underneath it and then <c>pg_ctl stop -m fast</c>-ed the managed postmaster. The abandoned
/// pass's next store reads logged five <c>Failed to compute baselines</c>, one anomaly-detection failure
/// and one <c>FilterMutedFindingsAsync failed</c> — all ERROR, all after "collection loop stopped", and
/// 7 of that day's 9 ERROR lines. The two genuine errors were the needles.</para>
///
/// <para>The repair has two halves and BOTH are pinned here: shutdown residue with the stopping token
/// signalled collapses to one Information line, and the SAME exceptions with the token NOT signalled stay
/// ERRORs — because a data source disposed while the service is meant to be running is a real bug whose
/// only evidence is exactly this text.</para>
/// </summary>
public sealed class AnalysisShutdownResidueTests
{
    private static readonly CancellationToken s_fired = new(canceled: true);

    private static PostgresException SqlState(string state) => new(
        messageText: "terminating connection due to administrator command",
        severity: "FATAL",
        invariantSeverity: "FATAL",
        sqlState: state);

    /// <summary>
    /// The shapes a stop actually produces: the token observed properly, the disposed data source
    /// (bare and Npgsql-wrapped), and the postmaster going away server-side (the 57P0x trio
    /// <c>PostgresTargetProvider</c> already classifies as connection-fatal).
    /// </summary>
    [Fact]
    public void EveryShutdownShapeIsAbandonedOnceTheTokenFires()
    {
        Assert.True(AnalysisShutdown.IsExpectedAbandon(new OperationCanceledException(), s_fired));
        Assert.True(AnalysisShutdown.IsExpectedAbandon(new ObjectDisposedException("NpgsqlDataSource"), s_fired));
        Assert.True(AnalysisShutdown.IsExpectedAbandon(
            new NpgsqlException("wrapper", new ObjectDisposedException("NpgsqlDataSource")), s_fired));
        Assert.True(AnalysisShutdown.IsExpectedAbandon(SqlState("57P01"), s_fired));
        Assert.True(AnalysisShutdown.IsExpectedAbandon(SqlState("57P02"), s_fired));
        Assert.True(AnalysisShutdown.IsExpectedAbandon(SqlState("57P03"), s_fired));
    }

    /// <summary>
    /// The other half of the agreement: with the token NOT signalled, the identical exceptions mean a
    /// data source was disposed (or a connection administratively killed) mid-run — a real bug — and
    /// must keep their ERROR. Quieting them unconditionally would erase that bug's only evidence.
    /// </summary>
    [Fact]
    public void TheSameShapesStayErrorsWhileTheServiceIsRunning()
    {
        Assert.False(AnalysisShutdown.IsExpectedAbandon(new OperationCanceledException(), CancellationToken.None));
        Assert.False(AnalysisShutdown.IsExpectedAbandon(new ObjectDisposedException("NpgsqlDataSource"), CancellationToken.None));
        Assert.False(AnalysisShutdown.IsExpectedAbandon(SqlState("57P01"), CancellationToken.None));
    }

    /// <summary>
    /// A command timeout coinciding with a stop is still a query that outgrew its deadline — the growth
    /// signal #2294 made visible must survive the coincidence, so a timeout is never relabelled shutdown.
    /// </summary>
    [Fact]
    public void ATimeoutIsNeverRelabelledAsShutdown()
    {
        Assert.False(AnalysisShutdown.IsExpectedAbandon(new TimeoutException("deadline"), s_fired));
        Assert.False(AnalysisShutdown.IsExpectedAbandon(
            new NpgsqlException("Exception while reading from stream", new TimeoutException()), s_fired));
        Assert.False(AnalysisShutdown.IsExpectedAbandon(SqlState("57014"), s_fired));
    }

    /// <summary>
    /// #2430. Once the pass token is a BUDGET linked from the stopping token, "the token fired" stops
    /// meaning "we are stopping" — and this is the pin that keeps the two apart. Arming the token
    /// without this split is what would have made every ordinary overrun on a healthy service report
    /// itself as "abandoned at shutdown", at Information, on exactly the signal someone would use to
    /// decide the budget needs raising.
    /// </summary>
    [Fact]
    public void ABudgetExpiryIsATimeoutAndNotAShutdown()
    {
        Assert.Equal(
            AnalysisAbandonKind.Timeout,
            AnalysisShutdown.Classify(new OperationCanceledException(), CancellationToken.None, s_fired));

        /* And a stop is still a stop, including when it lands on a pass that had already overrun —
           reporting that as a timeout would invent an incident out of a clean Stop-Service. */
        Assert.Equal(
            AnalysisAbandonKind.Shutdown,
            AnalysisShutdown.Classify(new OperationCanceledException(), s_fired, s_fired));
        Assert.Equal(
            AnalysisAbandonKind.Shutdown,
            AnalysisShutdown.Classify(new ObjectDisposedException("NpgsqlDataSource"), s_fired, s_fired));
    }

    /// <summary>
    /// The timeout arm is narrower than the shutdown arm on purpose, and this is why. A disposed data
    /// source and a 57P0x mean the STORE went away, which a budget expiring on a running service does
    /// not cause — so during the window after any pass overruns, those must keep the ERROR #2299 gave
    /// them rather than being relabelled as something we asked for. Widening this arm to the whole
    /// residue set would erase that bug's only evidence for every server that ever times out.
    /// </summary>
    [Fact]
    public void AStoreThatVanishesIsNeverExcusedByTheBudget()
    {
        Assert.Equal(
            AnalysisAbandonKind.None,
            AnalysisShutdown.Classify(new ObjectDisposedException("NpgsqlDataSource"), CancellationToken.None, s_fired));
        Assert.Equal(
            AnalysisAbandonKind.None,
            AnalysisShutdown.Classify(SqlState("57P01"), CancellationToken.None, s_fired));

        /* Nothing at all fired: a fault is a fault. */
        Assert.Equal(
            AnalysisAbandonKind.None,
            AnalysisShutdown.Classify(new OperationCanceledException(), CancellationToken.None, CancellationToken.None));
    }

    /// <summary>Ordinary faults during a stop are still faults — structural shapes only, never "anything goes".</summary>
    [Fact]
    public void OrdinaryFaultsAreNeverShutdownResidueEvenMidStop()
    {
        Assert.False(AnalysisShutdown.IsExpectedAbandon(SqlState("42703"), s_fired));
        Assert.False(AnalysisShutdown.IsExpectedAbandon(
            new NpgsqlException("Exception while reading from stream", new IOException("reset")), s_fired));
        Assert.False(AnalysisShutdown.IsExpectedAbandon(new InvalidOperationException("something else"), s_fired));
    }

    /// <summary>
    /// The CATEGORY pin, learned from finding the same defect shape nine times in one file: every
    /// ERROR-logging catch on the analysis pass must classify shutdown, or the next detector quietly
    /// reintroduces the noise. Counted from the shipped source so a new bare <c>catch (Exception ex)</c>
    /// in the detector goes red here with instructions, not silently at the next dogfood stop.
    /// </summary>
    [Fact]
    public void EveryErrorLoggingCatchOnTheAnalysisPassClassifiesShutdown()
    {
        const string contextFilter = "when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))";

        /* The detector: NO bare catch is permitted at all — its nine identical per-detector catches were
           the bulk of the burst, and a tenth detector must arrive classified. Every `catch (Exception ex)`
           must therefore BE one of the two filtered forms (the detectors carry the context token; the
           baseline-data gate carries its own parameter), so the counts are equal by construction. */
        var detector = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "PgAnomalyDetector.cs"));
        Assert.Equal(
            Count(detector, "catch (Exception ex)"),
            Count(detector, "catch (Exception ex) " + contextFilter)
                + Count(detector, "catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))"));
        Assert.True(Count(detector, contextFilter) >= 9, "a detector catch lost its shutdown classification");

        /* The baseline provider: its single catch produced five of the seven lines. */
        var baseline = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "PgBaselineProvider.cs"));
        Assert.Equal(1, Count(baseline, "when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))"));

        /* The finding store: only its two PASS methods run under the worker's token; its read-back
           surfaces serve other lifetimes and are deliberately untouched. */
        var findingStore = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "PgFindingStore.cs"));
        Assert.Equal(2, Count(findingStore, contextFilter));

        /* The drill-down: one per-finding catch, plus the between-findings abandon point. */
        var drillDown = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "PgDrillDownCollector.cs"));
        Assert.Equal(1, Count(drillDown, contextFilter));
        Assert.Contains("context.CancellationToken.ThrowIfCancellationRequested();", drillDown, StringComparison.Ordinal);

        /* The service: the ONE Information line a stop is allowed to cost, and the data-span probe must
           not convert shutdown residue into a bogus "0 hours of history" skip. */
        var service = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs"));
        Assert.Equal(1, Count(service, "AnalysisShutdown.Classify(ex, context.ShutdownToken, context.CancellationToken)"));
        Assert.Equal(1, Count(service, "when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))"));
        Assert.Contains("Analysis abandoned at shutdown", service, StringComparison.Ordinal);

        /* The worker: the pass must RECEIVE the stopping token (an uncancellable pass makes every filter
           above unreachable), and the stop path must hold the sweep open for the unwind grace with the
           already-fired token deliberately not forwarded. */
        var worker = ReadSource(Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        Assert.Contains("serverId, storageName, hoursBack: 4, cts.Token, stoppingToken", worker, StringComparison.Ordinal);
        Assert.Contains("WaitAsync(s_analysisShutdownGrace, CancellationToken.None)", worker, StringComparison.Ordinal);
    }

    private static int Count(string source, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = source.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }

    private static string ReadSource(string relative) => File.ReadAllText(Path.Combine(RepoRoot(), relative));
}
