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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9535: the bounds on query_stats' and procedure_stats' deferred plan fetch. A fake target answers each key with its
/// own name, raises a lock timeout for any command that holds a waiting key, and advances a clock by a fixed cost per
/// command, so the chunking, the single-key retry, the skip and the budget stop are pinned without a server or a wait.
/// </summary>
public sealed class BoundedPlanFetchTests
{
    private sealed class LockTimeout : Exception
    {
    }

    private sealed class Target
    {
        public HashSet<int> Waiting { get; } = [];

        public List<int[]> Commands { get; } = [];

        public Dictionary<int, string> Applied { get; } = [];

        public TimeSpan CostPerCommand { get; init; }

        public TimeSpan Clock { get; private set; }

        public Func<int, Exception?>? FailCommand { get; init; }

        public Task<Dictionary<int, string>> Fetch(IReadOnlyList<int> keys, CancellationToken cancellationToken)
        {
            Commands.Add(keys.ToArray());
            Clock += CostPerCommand;
            if (FailCommand?.Invoke(Commands.Count) is { } failure)
            {
                throw failure;
            }

            if (keys.Any(Waiting.Contains))
            {
                throw new LockTimeout();
            }

            var answers = new Dictionary<int, string>();
            for (var ord = 0; ord < keys.Count; ord++)
            {
                answers[ord] = "plan-" + keys[ord];
            }

            return Task.FromResult(answers);
        }

        public Task<BoundedPlanFetch.Outcome> RunAsync(int keyCount, TimeSpan? budget = null, CancellationToken cancellationToken = default)
            => BoundedPlanFetch.RunAsync(
                Enumerable.Range(0, keyCount).ToList(),
                Fetch,
                (ord, plan) => Applied.Add(ord, plan),
                ex => ex is LockTimeout,
                budget ?? TimeSpan.FromSeconds(5),
                () => Clock,
                cancellationToken);
    }

    [Fact]
    public async Task KeysGoOutInChunksOfTwenty_AndEachAnswerLandsOnItsOwnKey()
    {
        var target = new Target();

        var outcome = await target.RunAsync(45);

        Assert.Equal(new[] { 20, 20, 5 }, target.Commands.Select(c => c.Length).ToArray());
        Assert.Equal(45, target.Applied.Count);
        Assert.All(target.Applied, a => Assert.Equal("plan-" + a.Key, a.Value));
        Assert.Equal(45, outcome.Answered);
        Assert.Equal(0, outcome.Skipped);
        Assert.Equal(0, outcome.NotAttempted);
        Assert.Equal(0, outcome.ChunkRetries);
    }

    [Fact]
    public async Task OneWaitingPlan_SendsItsChunkAgainOneKeyAtATime_AndOnlyThatPlanIsSkipped()
    {
        var target = new Target();
        target.Waiting.Add(27);

        var outcome = await target.RunAsync(45);

        /* Chunk 0, chunk 1 (lock timeout), its 20 keys alone, chunk 2. */
        Assert.Equal(23, target.Commands.Count);
        Assert.Equal(Enumerable.Range(20, 20).ToArray(), target.Commands[1]);
        Assert.All(target.Commands.Skip(2).Take(20), c => Assert.Single(c));
        Assert.Equal(new[] { 40, 41, 42, 43, 44 }, target.Commands[22]);

        Assert.Equal(44, target.Applied.Count);
        Assert.DoesNotContain(27, target.Applied.Keys);
        Assert.All(target.Applied, a => Assert.Equal("plan-" + a.Key, a.Value));
        Assert.Equal(44, outcome.Answered);
        Assert.Equal(1, outcome.Skipped);
        Assert.Equal(1, outcome.ChunkRetries);
        Assert.Equal(0, outcome.NotAttempted);
    }

    [Fact]
    public async Task AChunkOfOneKey_IsNotAskedTwice()
    {
        var target = new Target();
        target.Waiting.Add(20);

        var outcome = await target.RunAsync(21);

        Assert.Equal(2, target.Commands.Count);
        Assert.Equal(20, target.Applied.Count);
        Assert.Equal(1, outcome.Skipped);
        Assert.Equal(0, outcome.ChunkRetries);
    }

    [Fact]
    public async Task TheBudget_StopsThePhase_AndTheKeysItDidNotReachAreCounted()
    {
        var target = new Target { CostPerCommand = TimeSpan.FromSeconds(2) };

        var outcome = await target.RunAsync(100, TimeSpan.FromSeconds(5));

        /* Commands start at 0 s, 2 s and 4 s. At 6 s the budget is spent. */
        Assert.Equal(3, target.Commands.Count);
        Assert.Equal(60, outcome.Answered);
        Assert.Equal(40, outcome.NotAttempted);
        Assert.Equal(0, outcome.Skipped);
        Assert.Equal(Enumerable.Range(0, 60).ToArray(), target.Applied.Keys.Order().ToArray());
    }

    [Fact]
    public async Task TheBudget_StopsASingleKeyRetryToo_AndEveryKeyIsAccountedFor()
    {
        var target = new Target { CostPerCommand = TimeSpan.FromSeconds(1) };
        foreach (var key in Enumerable.Range(0, 20))
        {
            target.Waiting.Add(key);
        }

        var outcome = await target.RunAsync(20, TimeSpan.FromSeconds(5));

        /* The chunk, then four keys alone: the fifth second is spent before the fifth key. */
        Assert.Equal(5, target.Commands.Count);
        Assert.Empty(target.Applied);
        Assert.Equal(4, outcome.Skipped);
        Assert.Equal(16, outcome.NotAttempted);
        Assert.Equal(20, outcome.Answered + outcome.Skipped + outcome.NotAttempted);
    }

    [Fact]
    public async Task AFailureThatIsNotATimeout_Propagates_AndKeepsTheAnswersAlreadyApplied()
    {
        var target = new Target { FailCommand = n => n == 2 ? new InvalidOperationException("not a timeout") : null };

        await Assert.ThrowsAsync<InvalidOperationException>(() => target.RunAsync(45));

        Assert.Equal(2, target.Commands.Count);
        Assert.Equal(Enumerable.Range(0, 20).ToArray(), target.Applied.Keys.Order().ToArray());
    }

    [Fact]
    public async Task ATimeoutAfterTheRunIsCancelled_IsNotRetried()
    {
        using var stop = new CancellationTokenSource();
        var target = new Target
        {
            FailCommand = n =>
            {
                stop.Cancel();
                return new LockTimeout();
            },
        };

        await Assert.ThrowsAsync<LockTimeout>(() => target.RunAsync(45, cancellationToken: stop.Token));

        Assert.Single(target.Commands);
        Assert.Empty(target.Applied);
    }

    [Fact]
    public void TheLockTimeoutStatements_SetTheBoundAndPutTheDefaultBack()
    {
        Assert.Equal(20, BoundedPlanFetch.ChunkKeys);
        Assert.Equal("SET LOCK_TIMEOUT 1000;", BoundedPlanFetch.SetLockTimeout.Text);
        Assert.Equal("SET LOCK_TIMEOUT -1;", BoundedPlanFetch.RestoreLockTimeout.Text);
        Assert.True(BoundedPlanFetch.CommandTimeoutSeconds < DarlingCollectorRunner.CommandTimeoutSeconds);
    }

    [Fact]
    public void TheRunNote_CarriesASkipCountOnlyWhenSomethingWasSkipped()
    {
        var clean = NewContext();
        DarlingCollectorRunner.StampPlanFetchBounds(clean, new BoundedPlanFetch.Outcome { Answered = 12 });
        DarlingCollectorRunner.StampPlanFetchBounds(clean, null);
        Assert.Empty(clean.Measurements);

        var skipped = NewContext();
        DarlingCollectorRunner.StampPlanFetchBounds(skipped, new BoundedPlanFetch.Outcome { Answered = 17, Skipped = 1, NotAttempted = 40 });
        Assert.Equal(1, skipped.Measurements.Single(m => m.Label == "plan_fetch_skipped").Value);
        Assert.Equal(40, skipped.Measurements.Single(m => m.Label == "plan_fetch_not_attempted").Value);
    }

    private static CollectorContext NewContext()
        => new() { ServerId = 1, ServerName = "s", CollectionTime = new DateTime(2031, 5, 1, 12, 0, 0, DateTimeKind.Utc), Deltas = new NoDeltas() };

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }
}
