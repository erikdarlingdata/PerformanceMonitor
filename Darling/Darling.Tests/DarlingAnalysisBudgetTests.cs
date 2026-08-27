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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2430. Darling's scheduled analysis had Lite's #2412 defect in the same shape: the per-pass timeout
/// raced a <c>Task.Delay</c> and cancelled nothing, while the in-flight marker was released only on
/// true completion — so a pass that never finished left that server skipped, silently, for the life of
/// the service.
///
/// <para><b>Why the fix could not be a transplant, which is what these tests are really about.</b> Arming
/// the pass token with the budget is one line. But <c>DarlingAnalysisService</c> classifies its abandon
/// arm from that token, and the classifier's question was "are we shutting down?". Arm the token and the
/// answer becomes yes on every ordinary overrun of a perfectly healthy service — so each one would report
/// itself as "abandoned at shutdown", at Information, on precisely the signal a person would use to
/// decide the budget needs raising. A wrong answer that reads as a calm one is worse than a loud wrong
/// answer, and it is the same mistake review caught on #2419's first round.</para>
///
/// <para>These run offline and in milliseconds: the token is observed before any socket work, so a data
/// source pointed at a closed port never dials it. That is deliberate — the behaviour under test is a
/// classification, and making it depend on a live store would have made it a source pin instead.</para>
/// </summary>
public sealed class DarlingAnalysisBudgetTests
{
    /// <summary>
    /// A data source that will never connect. Nothing here reaches the network: every case cancels
    /// before the first read, which is the whole point — the classification happens on the way out.
    /// </summary>
    private static NpgsqlDataSource DeadStore() =>
        new NpgsqlDataSourceBuilder("Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=2")
            .Build();

    private sealed class CapturedLog : ILogger
    {
        public List<(LogLevel Level, string Message)> Lines { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) =>
            Lines.Add((logLevel, formatter(state, exception)));

        public (LogLevel Level, string Message) Only(string containing)
        {
            var matches = Lines.Where(l => l.Message.Contains(containing, StringComparison.Ordinal)).ToList();
            Assert.True(matches.Count == 1,
                $"expected exactly one log line containing '{containing}', got {matches.Count}: "
                    + string.Join(" | ", Lines.Select(l => l.Level + ": " + l.Message)));
            return matches[0];
        }
    }

    /// <summary>
    /// The defect the port would have introduced. The pass token has fired and the host is NOT stopping,
    /// which after this change is the ordinary case — a server whose analysis outgrew its budget — and it
    /// must not be reported as a clean stop.
    /// </summary>
    [Fact]
    public async Task ABudgetCancellationIsNotReportedAsAShutdown()
    {
        var log = new CapturedLog();
        await using var store = DeadStore();
        var service = new DarlingAnalysisService(store, planFetcher: null, logger: log);

        using var fired = new CancellationTokenSource();
        await fired.CancelAsync();

        var findings = await service.AnalyzeAsync(new AnalysisContext
        {
            ServerId = 1,
            ServerName = "budget-only",
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
            CancellationToken = fired.Token

            /* ShutdownToken deliberately left unset: nothing is stopping. */
        });

        Assert.Empty(findings);
        Assert.DoesNotContain(log.Lines, l => l.Message.Contains("abandoned at shutdown", StringComparison.Ordinal));

        var reported = log.Only("cancelled at its per-pass budget");
        Assert.Equal(LogLevel.Warning, reported.Level);

        /* And the answer leaves the pass, so the scheduler reads it instead of inferring one. */
        Assert.Equal(AnalysisAbandonKind.Timeout, service.EndedEarlyAs);
    }

    /// <summary>
    /// The other direction, and the one #2299 bought: a genuine stop still collapses to ONE Information
    /// line. If this goes red, the split has been made by quieting shutdown rather than by separating it,
    /// and a clean Stop-Service is back to reading as an incident.
    /// </summary>
    [Fact]
    public async Task AShutdownIsStillOneQuietLine()
    {
        var log = new CapturedLog();
        await using var store = DeadStore();
        var service = new DarlingAnalysisService(store, planFetcher: null, logger: log);

        using var fired = new CancellationTokenSource();
        await fired.CancelAsync();

        var findings = await service.AnalyzeAsync(new AnalysisContext
        {
            ServerId = 1,
            ServerName = "stopping",
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow,
            CancellationToken = fired.Token,
            ShutdownToken = fired.Token
        });

        Assert.Empty(findings);

        var reported = log.Only("Analysis abandoned at shutdown");
        Assert.Equal(LogLevel.Information, reported.Level);
        Assert.Equal(AnalysisAbandonKind.Shutdown, service.EndedEarlyAs);
    }

    /// <summary>
    /// The entry point the worker actually calls. Dropping the fifth argument at that one call site would
    /// leave the context's shutdown token unset and put every real stop back on the timeout wording — a
    /// one-token omission with no compiler consequence, so it is pinned by behaviour rather than by shape:
    /// the same cancelled token produces two different lines depending only on whether it was also
    /// declared to be a shutdown.
    /// </summary>
    [Fact]
    public async Task TheShutdownTokenArgumentIsWhatDecidesTheWording()
    {
        using var fired = new CancellationTokenSource();
        await fired.CancelAsync();

        var asTimeout = new CapturedLog();
        await using (var store = DeadStore())
        {
            await new DarlingAnalysisService(store, planFetcher: null, logger: asTimeout)
                .AnalyzeAsync(1, "four-args", hoursBack: 4, cancellationToken: fired.Token);
        }

        var asShutdown = new CapturedLog();
        await using (var store = DeadStore())
        {
            await new DarlingAnalysisService(store, planFetcher: null, logger: asShutdown)
                .AnalyzeAsync(1, "five-args", hoursBack: 4, cancellationToken: fired.Token, shutdownToken: fired.Token);
        }

        Assert.Equal(LogLevel.Warning, asTimeout.Only("cancelled at its per-pass budget").Level);
        Assert.Equal(LogLevel.Information, asShutdown.Only("Analysis abandoned at shutdown").Level);
    }
}

/// <summary>
/// The scheduler half of #2430, which no test in this suite can reach behaviourally — the pass lives in
/// <c>DarlingWorker</c>, a BackgroundService that needs a whole configured host, a store and a fleet
/// before a single line of it runs. So these read the shipped source.
///
/// <para>They are shape pins, not text pins, and each one guards a property whose absence is invisible:
/// a token that is linked but never armed, a budget the sweep does not actually wait out, a skip branch
/// that returns in silence. Every one of those compiles, and every one of them puts the defect back.</para>
/// </summary>
public sealed class DarlingWedgedServerReportingTests
{
    private static string PassBody() =>
        MethodBody(Worker(), "private async Task<AnalysisPassResult> RunAnalysisPassAsync(");

    /// <summary>
    /// The marker used to be a <c>byte</c>, which is exactly enough to skip a server and not enough to
    /// ever say so. Reporting needs to know when the pass started and how often it has been mentioned.
    /// </summary>
    [Fact]
    public void TheInFlightMarkerCarriesEnoughToReportAStuckPass()
    {
        var worker = Worker();

        Assert.Contains("ConcurrentDictionary<int, AnalysisPassState> _analysisInFlight", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("ConcurrentDictionary<int, byte> _analysisInFlight", worker, StringComparison.Ordinal);
    }

    /// <summary>
    /// The budget has to CANCEL, not merely be waited on. A linked source that nobody arms is the shape
    /// the defect wore for its whole life: a timeout that moved the loop on while the work carried
    /// blithely on behind it, holding the marker.
    /// </summary>
    [Fact]
    public void ThePassTokenIsLinkedToShutdownAndArmedWithTheBudget()
    {
        var body = PassBody();

        Assert.Contains("CancellationTokenSource.CreateLinkedTokenSource(stoppingToken)", body, StringComparison.Ordinal);
        Assert.Contains("CancelAfter(s_analysisTimeout)", body, StringComparison.Ordinal);

        /* Both tokens on ONE call. Two independent Contains checks would pass with the pass still
           receiving only the stopping token, which is the whole defect. */
        Assert.Contains("serverId, storageName, hoursBack: 4, cts.Token, stoppingToken", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sweep must wait PAST the budget, or it is racing its own cancellation and losing that race
    /// carries no information — the pass might have stopped exactly as asked half a millisecond later.
    /// </summary>
    [Fact]
    public void TheSweepWaitsTheBudgetPlusTheUnwindGrace()
    {
        var body = PassBody();

        Assert.Contains("Task.Delay(s_analysisTimeout + s_analysisShutdownGrace, stoppingToken)", body, StringComparison.Ordinal);
        Assert.DoesNotContain("Task.Delay(s_analysisTimeout, stoppingToken)", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// The user-visible half. A server whose pass is wedged is skipped on every later cycle, and it used
    /// to be skipped with nothing said — indistinguishable from a server with nothing wrong with it. The
    /// skip is still correct; the silence was not.
    /// </summary>
    [Fact]
    public void TheSkipBranchReportsInsteadOfReturningInSilence()
    {
        var body = PassBody();

        var guard = body.IndexOf("if (!_analysisInFlight.TryAdd(", StringComparison.Ordinal);
        Assert.True(guard >= 0, "the in-flight guard moved; this pin's anchor is stale.");

        var report = body.IndexOf("ReportStuckAnalysis(", guard, StringComparison.Ordinal);
        var skipReturn = body.IndexOf("AnalysisPassStatus.Skipped", guard, StringComparison.Ordinal);

        Assert.True(report >= 0, "the skip branch says nothing at all, which is #2430.");
        Assert.True(report < skipReturn, "the report has to happen before the branch returns.");
    }

    /// <summary>
    /// Found in review on the first round of #2430. The pass already classifies its own ending and logs
    /// the one line for it, so the scheduler must READ that answer rather than infer a second one from
    /// "no findings and the budget token has fired" — which is equally true of a genuine fault that
    /// landed after the budget expired, and would have buried its ERROR under a Warning saying the pass
    /// merely ran out of time, then reported it to analyze_now as a timeout.
    /// </summary>
    [Fact]
    public void TheSchedulerReadsThePassesOwnEnding_RatherThanInferringOne()
    {
        var body = PassBody();

        Assert.Contains("analysisService.EndedEarlyAs", body, StringComparison.Ordinal);
        Assert.DoesNotContain("findings.Count == 0 && cts.IsCancellationRequested", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// And the report itself has to be findable and quantified. "Analysis skipped" would be true and
    /// useless: what makes this actionable is that it is permanent until something restarts, and that it
    /// names how many cycles that has already cost.
    /// </summary>
    [Fact]
    public void TheStuckReportIsAnErrorThatNamesTheCost()
    {
        var body = MethodBody(Worker(), "private void ReportStuckAnalysis(");

        Assert.Contains("_logger.LogError", body, StringComparison.Ordinal);
        Assert.Contains("SkippedCycles", body, StringComparison.Ordinal);

        /* Backed off rather than repeated per cycle: a line every cycle forever is the spam that makes
           a log unreadable, and an unread log reports nothing. */
        Assert.Contains("StuckAnalysisMaxBackoffDoublings", body, StringComparison.Ordinal);
    }

    /// <summary>
    /// Only the never-launched path may clear the marker by hand. Clearing it after the pass is running
    /// pulls the token out from under live work and re-admits that server next cycle on top of it —
    /// which is how an in-flight guard turns into two concurrent passes against one server.
    /// </summary>
    [Fact]
    public void OnlyANeverLaunchedPassClearsItsMarkerByHand()
    {
        var body = PassBody();

        var manual = body.IndexOf("_analysisInFlight.TryRemove(serverId, out _);\r\n                passCts?.Dispose();", StringComparison.Ordinal);
        if (manual < 0)
        {
            manual = body.IndexOf("_analysisInFlight.TryRemove(serverId, out _);\n                passCts?.Dispose();", StringComparison.Ordinal);
        }

        Assert.True(manual >= 0, "the catch no longer releases a never-launched pass, so a constructor "
            + "failure now strands that server's marker forever — the exact defect, arrived at from the "
            + "other direction.");
        Assert.Contains("if (!passStarted)", body, StringComparison.Ordinal);
    }

    private static string Worker() => ReadSource(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

    /// <summary>
    /// Everything between a method's signature and the first line closing it at method indentation.
    /// Crude on purpose: enough to stop an assertion being satisfied by a match elsewhere in a
    /// four-thousand-line worker, which is the only thing that would make these pins vacuous.
    /// </summary>
    private static string MethodBody(string source, string signature)
    {
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"'{signature}' was not found; this pin's anchor is stale.");

        var end = source.IndexOf("\n    }", start, StringComparison.Ordinal);
        Assert.True(end > start, $"no close found for '{signature}'.");

        return source.Substring(start, end - start);
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

    private static string ReadSource(string relative) => File.ReadAllText(Path.Combine(RepoRoot(), relative));
}
