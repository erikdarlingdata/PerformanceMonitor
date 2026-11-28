/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879: an on-demand <c>analyze_server</c> pass gives its heavy family reads a stated time. A read the deadline stops is
/// named and adds nothing, every other family finishes, and a pass with no deadline is the pass it was. The deadline here
/// is a token the test fires by hand while the read is held, or one already fired: no test waits on a clock.
/// </summary>
public sealed class AnalyzeServerFamilyTimeBudgetTests
{
    private readonly TaskCompletionSource _slowReadStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private int _slowReadCalls;

    private static Func<AnalysisContext, List<Fact>, Task> Quick(string key) =>
        (context, facts) =>
        {
            facts.Add(new Fact { Source = "test", Key = key });
            return Task.CompletedTask;
        };

    /// <summary>A family read that adds a fact, records a failure, and then waits on the token its context hands it.</summary>
    private async Task CollectSlowThingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        Interlocked.Increment(ref _slowReadCalls);
        facts.Add(new Fact { Source = "test", Key = "SLOW_PARTIAL" });
        context.RecordCollectionFailure("slow_thing", nameof(CollectSlowThingFactsAsync), CollectionFailureOutcome.Error, new InvalidOperationException("first read"));
        _slowReadStarted.TrySetResult();
        await Task.Delay(Timeout.Infinite, context.CancellationToken);
    }

    private bool IsTheSlowFamily(Func<AnalysisContext, List<Fact>, Task> family) => family.Target == this;

    private Func<AnalysisContext, List<Fact>, Task>[] Families() =>
        [Quick("A"), CollectSlowThingFactsAsync, Quick("B"), Quick("C")];

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AReadTheDeadlineStops_IsNamed_AddsNoFactOrFailure_AndTheOtherFamiliesFinish(int parallelism)
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { FamilyParallelism = parallelism, FamilyDeadline = deadline.Token };
        var facts = new List<Fact>();

        var pass = PgFactCollector.RunFamiliesAsync(context, facts, Families(), IsTheSlowFamily);
        await _slowReadStarted.Task.WaitAsync(TestContext.Current.CancellationToken);
        Assert.False(pass.IsCompleted);
        deadline.Cancel();

        /* The bound only turns a stage that ignores the deadline into a failure, where it would otherwise never return. */
        await pass.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "A", "B", "C" }, facts.Select(f => f.Key));
        Assert.Equal(new[] { CollectionFailure.FamilyOf(nameof(CollectSlowThingFactsAsync)) }, context.FamiliesLeftOutForTime);
        Assert.Empty(context.CollectionFailures);

        /* The pass's own flow is back on the pass's token and list once the stage ends. */
        Assert.Equal(context.PassToken, context.CancellationToken);
        context.RecordCollectionFailure("later", "later", CollectionFailureOutcome.Error, new InvalidOperationException("after"));
        Assert.Single(context.CollectionFailures);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AReadNotStartedWhenTheDeadlineHasFired_IsLeftOutUnread(int parallelism)
    {
        using var deadline = new CancellationTokenSource();
        deadline.Cancel();
        var context = new AnalysisContext { FamilyParallelism = parallelism, FamilyDeadline = deadline.Token };
        var facts = new List<Fact>();

        await PgFactCollector.RunFamiliesAsync(context, facts, Families(), IsTheSlowFamily)
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

        Assert.Equal(0, _slowReadCalls);
        Assert.Equal(new[] { "A", "B", "C" }, facts.Select(f => f.Key));
        Assert.Single(context.FamiliesLeftOutForTime);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AStopFromThePassToken_IsRethrown_AndIsNotABudgetCut(int parallelism)
    {
        using var deadline = new CancellationTokenSource();
        using var passStop = new CancellationTokenSource();
        var context = new AnalysisContext { FamilyParallelism = parallelism, FamilyDeadline = deadline.Token, CancellationToken = passStop.Token };
        var facts = new List<Fact>();

        var pass = PgFactCollector.RunFamiliesAsync(context, facts, Families(), IsTheSlowFamily);
        await _slowReadStarted.Task.WaitAsync(TestContext.Current.CancellationToken);
        passStop.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pass);
        Assert.Empty(context.FamiliesLeftOutForTime);
    }

    [Fact]
    public async Task WithNoDeadline_TheSameFamilyReadsThroughThePassToken_AndKeepsItsFactsAndFailures()
    {
        using var passStop = new CancellationTokenSource();
        var context = new AnalysisContext { FamilyParallelism = 4, CancellationToken = passStop.Token };
        var facts = new List<Fact>();

        var pass = PgFactCollector.RunFamiliesAsync(context, facts,
            [Quick("A"), async (c, f) => { try { await CollectSlowThingFactsAsync(c, f); } catch (OperationCanceledException) { } }, Quick("B")],
            _ => true);
        await _slowReadStarted.Task.WaitAsync(TestContext.Current.CancellationToken);
        passStop.Cancel();
        await pass.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "A", "SLOW_PARTIAL", "B" }, facts.Select(f => f.Key));
        Assert.Single(context.CollectionFailures);
        Assert.Empty(context.FamiliesLeftOutForTime);
    }

    [Fact]
    public void TheReply_NamesEachFamilyLeftOut_AndIsUntouchedWhenNoneWas()
    {
        var payload = new { status = "findings" };

        Assert.Same(payload, FamilyTimeBudget.Attach(payload, [], TimeSpan.FromSeconds(3), JsonSerializerOptions.Default));
        Assert.Null(FamilyTimeBudget.Describe([], TimeSpan.FromSeconds(3)));

        var reply = Assert.IsType<JsonObject>(FamilyTimeBudget.Attach(payload, ["bad_actor", "plan_advisory"], TimeSpan.FromSeconds(3), JsonSerializerOptions.Default));
        var entries = reply["families_left_out"]!.AsArray();
        Assert.Equal(new[] { "bad_actor", "plan_advisory" }, entries.Select(e => e!["family"]!.GetValue<string>()));
        Assert.All(entries, e => Assert.Equal(FamilyTimeBudget.Reason, e!["reason"]!.GetValue<string>()));
        Assert.All(entries, e => Assert.Equal(3.0, e!["budget_seconds"]!.GetValue<double>()));
        Assert.Equal("findings", reply["status"]!.GetValue<string>());
        Assert.NotNull(FamilyTimeBudget.Describe(["bad_actor"], TimeSpan.FromSeconds(3)));
    }
}
