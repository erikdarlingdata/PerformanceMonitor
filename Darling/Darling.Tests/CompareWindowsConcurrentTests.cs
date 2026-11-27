/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: <c>compare_analysis</c> collects its baseline and comparison windows concurrently
/// (<see cref="DarlingAnalysisService.CollectBothWindowsAsync"/>), so the call costs the slower window rather than
/// the sum of both. The collector here is a fake whose baseline read cannot finish until the comparison read has
/// started: a helper that ran the windows in turn never starts the comparison and fails at the bounded wait.
/// </summary>
public sealed class CompareWindowsConcurrentTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private static AnalysisContext Window(DateTime end) => new()
    {
        ServerId = 1,
        ServerName = "s",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
    };

    private static DarlingAnalysisService.AnalysisEngineSet EngineWith(IFactCollector collector) =>
        new(collector, null!, null!, null!, null!, string.Empty);

    private static List<Fact> FactsFor(string key) => [new Fact { Source = "test", Key = key }];

    /// <summary>Each read runs the delegate the test gave its window.</summary>
    private sealed class ScriptedCollector(Func<AnalysisContext, Task<List<Fact>>> baseline, Func<AnalysisContext, Task<List<Fact>>> comparison, AnalysisContext baselineContext) : IFactCollector
    {
        public Task<List<Fact>> CollectFactsAsync(AnalysisContext context) =>
            ReferenceEquals(context, baselineContext) ? baseline(context) : comparison(context);
    }

    [Fact]
    public async Task TheBaselineWindow_IsStillReading_WhenTheComparisonWindowStarts()
    {
        var baselineContext = Window(DateTime.UtcNow.AddHours(-4));
        var comparisonContext = Window(DateTime.UtcNow);
        var comparisonStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var collector = new ScriptedCollector(
            baseline: async _ =>
            {
                await comparisonStarted.Task.WaitAsync(Bound);
                return FactsFor("BASELINE");
            },
            comparison: _ =>
            {
                comparisonStarted.TrySetResult();
                return Task.FromResult(FactsFor("COMPARISON"));
            },
            baselineContext);

        var (baselineFacts, comparisonFacts) = await DarlingAnalysisService.CollectBothWindowsAsync(EngineWith(collector), baselineContext, comparisonContext);

        Assert.Equal("BASELINE", Assert.Single(baselineFacts).Key);
        Assert.Equal("COMPARISON", Assert.Single(comparisonFacts).Key);
    }

    [Fact]
    public async Task AFailedWindow_DoesNotEndTheCall_WhileTheOtherWindowIsStillReading()
    {
        var baselineContext = Window(DateTime.UtcNow.AddHours(-4));
        var comparisonContext = Window(DateTime.UtcNow);
        var releaseComparison = new TaskCompletionSource<List<Fact>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var collector = new ScriptedCollector(
            baseline: _ => Task.FromException<List<Fact>>(new InvalidOperationException("baseline failed")),
            comparison: _ => releaseComparison.Task,
            baselineContext);

        var call = DarlingAnalysisService.CollectBothWindowsAsync(EngineWith(collector), baselineContext, comparisonContext);
        Assert.False(call.IsCompleted);

        releaseComparison.SetResult(FactsFor("COMPARISON"));
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => call.WaitAsync(Bound));
        Assert.Equal("baseline failed", thrown.Message);
    }

    [Fact]
    public async Task ACancelledWindow_ReachesTheCallerAsCancellation()
    {
        var baselineContext = Window(DateTime.UtcNow.AddHours(-4));
        var comparisonContext = Window(DateTime.UtcNow);

        var collector = new ScriptedCollector(
            baseline: _ => Task.FromCanceled<List<Fact>>(new CancellationToken(canceled: true)),
            comparison: _ => Task.FromResult(FactsFor("COMPARISON")),
            baselineContext);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            DarlingAnalysisService.CollectBothWindowsAsync(EngineWith(collector), baselineContext, comparisonContext).WaitAsync(Bound));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ACancelledWindow_ReachesTheCallerAsCancellation_WhenTheOtherWindowFaulted(bool baselineCancelled)
    {
        var baselineContext = Window(DateTime.UtcNow.AddHours(-4));
        var comparisonContext = Window(DateTime.UtcNow);
        Task<List<Fact>> Cancelled(AnalysisContext _) => Task.FromCanceled<List<Fact>>(new CancellationToken(canceled: true));
        Task<List<Fact>> Faulted(AnalysisContext _) => Task.FromException<List<Fact>>(new InvalidOperationException("window failed"));

        var collector = new ScriptedCollector(
            baseline: baselineCancelled ? Cancelled : Faulted,
            comparison: baselineCancelled ? Faulted : Cancelled,
            baselineContext);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            DarlingAnalysisService.CollectBothWindowsAsync(EngineWith(collector), baselineContext, comparisonContext).WaitAsync(Bound));
    }
}
