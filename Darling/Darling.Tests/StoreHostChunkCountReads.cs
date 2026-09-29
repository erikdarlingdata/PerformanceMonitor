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
using System.Threading.Tasks;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4807: compares the uncompressed chunk count <c>get_store_host</c> reports against the owner's own read of
/// the same query, but only across a stretch in which no chunk changed. The two reads happen at different
/// moments, and other live test classes share the test database, so chunks can appear or disappear between
/// them. The owner's count is read before AND after the tool call: when the two owner reads agree, no chunk
/// changed while the tool ran, and the tool's count must equal them. When they differ, the attempt says
/// nothing about the tool and is repeated, up to <see cref="MaxAttempts"/> times.
/// </summary>
internal static class StoreHostChunkCountReads
{
    internal const int MaxAttempts = 5;

    /// <summary>One try: the owner's count before the tool call, the tool's count, the owner's count after.</summary>
    internal readonly record struct Attempt(long OwnerBefore, long Tool, long OwnerAfter)
    {
        /// <summary>True when no chunk changed while the tool ran: both owner reads saw the same count.</summary>
        internal bool OwnerReadsAgree => OwnerBefore == OwnerAfter;

        public override string ToString()
            => $"owner before={OwnerBefore}, tool={Tool}, owner after={OwnerAfter}";
    }

    /// <summary>
    /// Runs attempts until one has two agreeing owner reads, or <paramref name="maxAttempts"/> attempts have
    /// run. Returns every attempt made, oldest first; a settled run always ends on the attempt that settled,
    /// so the last element is the one to compare (see <see cref="AssertToolMatchesOwner"/>). The tool call
    /// runs once per attempt and the owner read twice, in the order before, tool, after.
    /// </summary>
    internal static async Task<IReadOnlyList<Attempt>> ReadAsync(
        Func<Task<long>> ownerRead, Func<Task<long>> toolCall, int maxAttempts = MaxAttempts)
    {
        ArgumentNullException.ThrowIfNull(ownerRead);
        ArgumentNullException.ThrowIfNull(toolCall);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxAttempts, 1);

        var attempts = new List<Attempt>(maxAttempts);
        for (var i = 0; i < maxAttempts; i++)
        {
            var ownerBefore = await ownerRead();
            var tool = await toolCall();
            var ownerAfter = await ownerRead();

            var attempt = new Attempt(ownerBefore, tool, ownerAfter);
            attempts.Add(attempt);
            if (attempt.OwnerReadsAgree)
            {
                break;
            }
        }

        return attempts;
    }

    /// <summary>
    /// Fails when no attempt had two agreeing owner reads, naming every attempt's counts; otherwise asserts
    /// the tool's count equals the owner's count on the attempt that settled.
    /// </summary>
    internal static void AssertToolMatchesOwner(IReadOnlyList<Attempt> attempts)
    {
        ArgumentNullException.ThrowIfNull(attempts);

        var settled = attempts.Count > 0 && attempts[^1].OwnerReadsAgree;
        if (!settled)
        {
            Assert.Fail(
                $"No attempt read the same owner uncompressed chunk count before and after the get_store_host call "
                + $"({attempts.Count} attempt(s) made), so the tool's count could not be compared with a count "
                + "read while no chunk changed. "
                + string.Join("; ", attempts.Select((a, i) => $"attempt {i + 1}: {a}")) + ".");
        }

        var last = attempts[^1];
        Assert.Equal(last.OwnerBefore, last.Tool);
    }
}
