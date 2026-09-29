/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using Xunit.Sdk;

namespace Darling.Tests;

/// <summary>
/// #4807: the chunk-count comparison in <c>DarlingMcpStoreHostToolsLiveTests</c> needs a running store, so the
/// rules it applies are pinned here against fake reads that need none. The rules: the owner's count is read
/// before and after the tool call; two agreeing owner reads make the attempt count and the tool's count must
/// equal them; disagreeing reads mean a chunk changed during the call and the attempt is repeated, up to five
/// times; five disagreeing attempts fail and name every attempt's counts.
/// </summary>
public sealed class StoreHostChunkCountReadsTests
{
    /// <summary>Hands out queued values and records the order the reads were made in.</summary>
    private sealed class FakeReads(IEnumerable<long> ownerCounts, IEnumerable<long> toolCounts)
    {
        private readonly Queue<long> _owner = new(ownerCounts);
        private readonly Queue<long> _tool = new(toolCounts);

        public List<string> Calls { get; } = [];

        public int OwnerReadsLeft => _owner.Count;

        public int ToolCallsLeft => _tool.Count;

        public Task<long> OwnerRead()
        {
            Calls.Add("owner");
            return Task.FromResult(_owner.Dequeue());
        }

        public Task<long> ToolCall()
        {
            Calls.Add("tool");
            return Task.FromResult(_tool.Dequeue());
        }
    }

    [Fact]
    public async Task AgreeingOwnerReadsAndMatchingToolCount_Pass()
    {
        var reads = new FakeReads(ownerCounts: [225, 225], toolCounts: [225]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        var only = Assert.Single(attempts);
        Assert.Equal(new StoreHostChunkCountReads.Attempt(225, 225, 225), only);
        Assert.Equal(["owner", "tool", "owner"], reads.Calls);
        StoreHostChunkCountReads.AssertToolMatchesOwner(attempts);
    }

    /// <summary>The permanent RED plant: a tool count that is off by one, in either direction, against two
    /// agreeing owner reads must fail the comparison. Loosening the equality check makes this test fail.</summary>
    [Theory]
    [InlineData(224)]
    [InlineData(226)]
    public async Task ToolCountOffByOneFromAgreeingOwnerReads_Fails(long toolCount)
    {
        var reads = new FakeReads(ownerCounts: [225, 225], toolCounts: [toolCount]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        Assert.Single(attempts);
        Assert.Throws<EqualException>(() => StoreHostChunkCountReads.AssertToolMatchesOwner(attempts));
    }

    [Fact]
    public async Task OwnerReadsThatNeverAgree_FailAndNameEveryAttempt()
    {
        // Every owner read differs from the one before it: a chunk changed during every tool call.
        var reads = new FakeReads(
            ownerCounts: [100, 101, 102, 103, 104, 105, 106, 107, 108, 109],
            toolCounts: [201, 202, 203, 204, 205]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        Assert.Equal(5, StoreHostChunkCountReads.MaxAttempts);
        Assert.Equal(StoreHostChunkCountReads.MaxAttempts, attempts.Count);
        Assert.Equal(0, reads.OwnerReadsLeft);
        Assert.Equal(0, reads.ToolCallsLeft);

        var failure = Assert.Throws<FailException>(() => StoreHostChunkCountReads.AssertToolMatchesOwner(attempts));
        Assert.Contains("attempt 1: owner before=100, tool=201, owner after=101", failure.Message);
        Assert.Contains("attempt 2: owner before=102, tool=202, owner after=103", failure.Message);
        Assert.Contains("attempt 3: owner before=104, tool=203, owner after=105", failure.Message);
        Assert.Contains("attempt 4: owner before=106, tool=204, owner after=107", failure.Message);
        Assert.Contains("attempt 5: owner before=108, tool=205, owner after=109", failure.Message);
    }

    /// <summary>The flake this comparison was written for: the tool saw 229 and the owner's single later read
    /// saw 225. Now the first attempt is thrown away because its owner reads disagree, and the retry, whose
    /// owner reads agree, is the one compared.</summary>
    [Fact]
    public async Task UnstableReadsThatSettle_UseTheAttemptThatSettled()
    {
        var reads = new FakeReads(
            ownerCounts: [229, 225, 225, 225],
            toolCounts: [229, 225]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        Assert.Equal(2, attempts.Count);
        Assert.False(attempts[0].OwnerReadsAgree);
        Assert.True(attempts[1].OwnerReadsAgree);
        Assert.Equal(0, reads.OwnerReadsLeft);
        Assert.Equal(0, reads.ToolCallsLeft);
        StoreHostChunkCountReads.AssertToolMatchesOwner(attempts);
    }

    [Fact]
    public async Task UnstableReadsThatSettle_StopAtTheFirstAttemptThatSettled()
    {
        // Three attempts are queued; the second settles, so the third must never run.
        var reads = new FakeReads(
            ownerCounts: [229, 225, 225, 225, 300, 300],
            toolCounts: [229, 225, 300]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        Assert.Equal(2, attempts.Count);
        Assert.Equal(2, reads.OwnerReadsLeft);
        Assert.Equal(1, reads.ToolCallsLeft);
    }

    /// <summary>An earlier, unsettled attempt whose tool count happens to equal its first owner read must not
    /// rescue a settled attempt whose tool count is off by one: the settled attempt alone decides.</summary>
    [Fact]
    public async Task UnstableReadsThatSettle_AnOffByOneToolCountOnTheSettledAttemptFails()
    {
        var reads = new FakeReads(
            ownerCounts: [225, 229, 229, 229],
            toolCounts: [225, 230]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        Assert.Equal(2, attempts.Count);
        Assert.Equal(attempts[0].OwnerBefore, attempts[0].Tool);
        Assert.False(attempts[0].OwnerReadsAgree);
        Assert.Throws<EqualException>(() => StoreHostChunkCountReads.AssertToolMatchesOwner(attempts));
    }

    [Fact]
    public async Task SettlingOnTheLastAllowedAttempt_Passes()
    {
        var reads = new FakeReads(
            ownerCounts: [1, 2, 3, 4, 5, 6, 7, 8, 9, 9],
            toolCounts: [1, 3, 5, 7, 9]);

        var attempts = await StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall);

        Assert.Equal(5, attempts.Count);
        Assert.True(attempts[^1].OwnerReadsAgree);
        StoreHostChunkCountReads.AssertToolMatchesOwner(attempts);
    }

    [Fact]
    public async Task FewerThanOneAttempt_IsRefused()
    {
        var reads = new FakeReads(ownerCounts: [], toolCounts: []);

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(
            () => StoreHostChunkCountReads.ReadAsync(reads.OwnerRead, reads.ToolCall, maxAttempts: 0));
        Assert.Empty(reads.Calls);
    }
}
