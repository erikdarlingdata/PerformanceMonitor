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
/// own name, raises a lock timeout for any command that holds a waiting key and a command timeout for any command that
/// holds a slow key, and advances a clock by a fixed cost per command, so the chunking, the single-key retry, the skip,
/// the skipped key going last and the budget stop are pinned without a server or a wait.
/// </summary>
public sealed class BoundedPlanFetchTests
{
    private sealed class LockTimeout : Exception
    {
    }

    private sealed class CommandTimeout : Exception
    {
    }

    private sealed class Target
    {
        public HashSet<int> Waiting { get; } = [];

        /// <summary>Keys whose plan takes longer than the command timeout to render: a command holding one costs <see cref="SlowCost"/> and times out.</summary>
        public HashSet<int> Slow { get; } = [];

        public TimeSpan SlowCost { get; init; } = TimeSpan.FromSeconds(BoundedPlanFetch.CommandTimeoutSeconds);

        /// <summary>What the next runs remember between them, as the runner's per-server set does.</summary>
        public PlanFetchLaterKeys<int> Later { get; } = new(150);

        public List<int[]> Commands { get; } = [];

        public Dictionary<int, string> Applied { get; } = [];

        public TimeSpan CostPerCommand { get; init; }

        public TimeSpan Clock { get; private set; }

        public Func<int, Exception?>? FailCommand { get; init; }

        public Task<Dictionary<int, string>> Fetch(IReadOnlyList<int> keys, CancellationToken cancellationToken)
        {
            Commands.Add(keys.ToArray());
            if (keys.Any(Slow.Contains))
            {
                Clock += SlowCost;
                throw new CommandTimeout();
            }

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
                ex => ex is LockTimeout or CommandTimeout,
                budget ?? TimeSpan.FromSeconds(5),
                () => Clock,
                Later,
                key => key,
                cancellationToken);

        /// <summary>The next run over the same keys: the clock, the log of commands and the answers start over, the remembered keys stay.</summary>
        public void NextRun()
        {
            Commands.Clear();
            Applied.Clear();
            Clock = TimeSpan.Zero;
        }
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
    public async Task ACommandTimeoutInTheFirstChunk_StillLetsTheChunksHealthyKeysGetTheirPlans_InTheSameRun()
    {
        var target = new Target();
        target.Slow.Add(3);

        var outcome = await target.RunAsync(45);

        /* The chunk costs the command timeout, its 20 keys go out alone (the slow one costs the timeout again), and the
           two later chunks still run inside the budget: 4 s of the 5 are spent before they start. */
        Assert.Equal(Enumerable.Range(0, 20).ToArray(), target.Commands[0]);
        Assert.All(target.Commands.Skip(1).Take(20), c => Assert.Single(c));
        Assert.Equal(23, target.Commands.Count);
        Assert.Equal(Enumerable.Range(0, 45).Where(k => k != 3).ToArray(), target.Applied.Keys.Order().ToArray());
        Assert.All(target.Applied, a => Assert.Equal("plan-" + a.Key, a.Value));
        Assert.Equal(44, outcome.Answered);
        Assert.Equal(1, outcome.Skipped);
        Assert.Equal(1, outcome.ChunkRetries);
        Assert.Equal(0, outcome.NotAttempted);
        Assert.True(target.Later.Contains(3), "a key skipped on its own is remembered");
        Assert.Equal(1, target.Later.Count);
    }

    [Fact]
    public async Task TheNextRun_DoesNotStallOnTheSlowKey_ItIsAskedLast_AfterEveryOtherKey()
    {
        var target = new Target();
        target.Slow.Add(3);
        await target.RunAsync(45);
        target.NextRun();

        var outcome = await target.RunAsync(45);

        /* The 44 other keys go out in chunks of 20 first, with no retry; the slow key goes last and alone. */
        Assert.Equal(new[] { 20, 20, 4, 1 }, target.Commands.Select(c => c.Length).ToArray());
        Assert.All(target.Commands.Take(3), c => Assert.DoesNotContain(3, c));
        Assert.Equal(new[] { 3 }, target.Commands[3]);
        Assert.Equal(Enumerable.Range(0, 45).Where(k => k != 3).ToArray(), target.Applied.Keys.Order().ToArray());
        Assert.All(target.Applied, a => Assert.Equal("plan-" + a.Key, a.Value));
        Assert.Equal(44, outcome.Answered);
        Assert.Equal(1, outcome.Skipped);
        Assert.Equal(0, outcome.ChunkRetries);
        Assert.Equal(0, outcome.NotAttempted);
        Assert.True(target.Later.Contains(3), "a key that is skipped again stays at the back");
    }

    [Fact]
    public async Task ASlowKeyIsAskedLastOnlyWhileBudgetRemains()
    {
        var target = new Target { CostPerCommand = TimeSpan.FromSeconds(2) };
        target.Later.Remember(3);

        var outcome = await target.RunAsync(45, TimeSpan.FromSeconds(5));

        /* Three chunks of the other keys cost 6 s, so the budget is spent before the slow key's turn. */
        Assert.Equal(new[] { 20, 20, 4 }, target.Commands.Select(c => c.Length).ToArray());
        Assert.DoesNotContain(3, target.Applied.Keys);
        Assert.Equal(44, outcome.Answered);
        Assert.Equal(1, outcome.NotAttempted);
        Assert.Equal(0, outcome.Skipped);
        Assert.True(target.Later.Contains(3), "a key the budget did not reach stays remembered");
    }

    [Fact]
    public async Task ARememberedKeyThatIsAnswered_IsDroppedFromTheSet()
    {
        var target = new Target();
        target.Later.Remember(3);
        target.Later.Remember(900);

        var outcome = await target.RunAsync(45);

        Assert.Equal(new[] { 3 }, target.Commands[^1]);
        Assert.Equal(45, outcome.Answered);
        Assert.Equal("plan-3", target.Applied[3]);
        Assert.False(target.Later.Contains(3), "an answered key leaves the set");
        Assert.True(target.Later.Contains(900), "a key this run did not carry is left alone");
    }

    [Fact]
    public async Task WhenTheBudgetStopsTheRetryBeforeAnySingleKeyAsk_NoChunkRetryIsCounted()
    {
        var target = new Target { SlowCost = TimeSpan.FromSeconds(5) };
        target.Slow.Add(3);

        var outcome = await target.RunAsync(45, TimeSpan.FromSeconds(5));

        Assert.Single(target.Commands);
        Assert.Empty(target.Applied);
        Assert.Equal(0, outcome.ChunkRetries);
        Assert.Equal(0, outcome.Skipped);
        Assert.Equal(45, outcome.NotAttempted);
        Assert.Equal(0, target.Later.Count);
    }

    [Fact]
    public void TheSetOfSkippedKeys_HoldsNoMoreThanItsCapacity_AndDropsTheOldestFirst()
    {
        var later = new PlanFetchLaterKeys<int>(3);

        foreach (var key in new[] { 1, 2, 3, 4, 5 })
        {
            later.Remember(key);
        }

        Assert.Equal(3, later.Count);
        Assert.Equal(new[] { false, false, true, true, true }, new[] { 1, 2, 3, 4, 5 }.Select(later.Contains).ToArray());

        later.Remember(3);
        later.Remember(6);

        Assert.True(later.Contains(3), "a key remembered again is the newest, not the oldest");
        Assert.False(later.Contains(4), "the oldest left");
        later.Forget(3);
        Assert.Equal(2, later.Count);
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
