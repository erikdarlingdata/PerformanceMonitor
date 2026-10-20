/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>The confirm values <c>purge_now</c> hands out (#6230): issued by the first call, good for 5 minutes and once, and only for the
/// scope and plan they were issued for. Pure logic with an injected clock, so nothing waits.</summary>
public sealed class McpConfirmTokensTests
{
    private const string Plan = "purge_now|30=collector_a,collector_b;";

    [Fact]
    public void AValueComputedWithoutTheFirstCall_IsRefused()
    {
        var tokens = new McpConfirmTokens(new ManualTimeProvider());
        tokens.Issue("purge", "store", Plan);

        /* The shape the earlier build derived from the plan alone: anyone can compute it, so it must not confirm anything. */
        var derived = "purge-" + Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(Plan)), 0, 6).ToLowerInvariant();
        Assert.False(tokens.TryRedeem(derived, "store", Plan));
        Assert.False(tokens.TryRedeem("purge-000000000000", "store", Plan));
        Assert.False(tokens.TryRedeem("", "store", Plan));

        /* A value issued by another instance (another process, or a restart) is no better. */
        var other = new McpConfirmTokens(new ManualTimeProvider()).Issue("purge", "store", Plan);
        Assert.False(tokens.TryRedeem(other, "store", Plan));
    }

    [Fact]
    public void AValueFromTheFirstCall_WorksOnce()
    {
        var tokens = new McpConfirmTokens(new ManualTimeProvider());
        var value = tokens.Issue("purge", "store", Plan);
        Assert.StartsWith("purge-", value, StringComparison.Ordinal);

        Assert.True(tokens.TryRedeem(value, "store", Plan));
        Assert.False(tokens.TryRedeem(value, "store", Plan));
    }

    [Fact]
    public void AValueOlderThanFiveMinutes_IsRefused()
    {
        var clock = new ManualTimeProvider();
        var tokens = new McpConfirmTokens(clock);
        var fresh = tokens.Issue("purge", "store", Plan);
        var stale = tokens.Issue("purge", "store", Plan);

        clock.Advance(TimeSpan.FromMinutes(5) - TimeSpan.FromSeconds(1));
        Assert.True(tokens.TryRedeem(fresh, "store", Plan));

        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.False(tokens.TryRedeem(stale, "store", Plan));
    }

    [Fact]
    public void AValueIssuedForOneServerOrPlan_IsRefusedForAnother_AndStaysGoodForItsOwn()
    {
        var tokens = new McpConfirmTokens(new ManualTimeProvider());
        var value = tokens.Issue("purge", "server-a", Plan);

        Assert.False(tokens.TryRedeem(value, "server-b", Plan));
        Assert.False(tokens.TryRedeem(value, "server-a", "purge_now|7=collector_a;"));
        Assert.True(tokens.TryRedeem(value, "server-a", Plan));
    }

    [Fact]
    public void TheSamePlanAskedTwice_GetsTwoDifferentValues_EachGoodOnce()
    {
        var tokens = new McpConfirmTokens(new ManualTimeProvider());
        var first = tokens.Issue("purge", "store", Plan);
        var second = tokens.Issue("purge", "store", Plan);

        Assert.NotEqual(first, second);
        Assert.True(tokens.TryRedeem(second, "store", Plan));
        Assert.True(tokens.TryRedeem(first, "store", Plan));
    }

    [Fact]
    public void TheHeldValues_AreBounded()
    {
        var tokens = new McpConfirmTokens(new ManualTimeProvider());
        var oldest = tokens.Issue("purge", "store", Plan);
        for (var i = 0; i < 300; i++)
        {
            tokens.Issue("purge", "store", Plan);
        }

        Assert.False(tokens.TryRedeem(oldest, "store", Plan));
    }
}
