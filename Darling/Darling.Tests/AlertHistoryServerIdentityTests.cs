/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3456: the two halves of the server-key-to-<c>server_id</c> mapping, pinned where they live. The write
/// half (<see cref="AlertHistoryServerIdentity.StorageId"/>) collapses every unparseable key into the 0
/// bucket, which is tolerable because a row must be recorded somewhere; the read half
/// (<see cref="AlertHistoryServerIdentity.SeedScope"/>) must never READ that bucket, because its rows are
/// indistinguishable by key and an answer from it is another key's history — the mechanism that made one
/// server's <c>Collector Cost Regression</c> send throttle every sibling's, first notices included. The
/// real stores are pinned against DuckDB in Lite's <c>StoreRoundTripTests</c>; these are the pure mapping's
/// own pins, so a regression names the mapping rather than a store.
/// </summary>
public sealed class AlertHistoryServerIdentityTests
{
    /// <summary>
    /// An integer key answers the same id on both sides — the engine population (deterministic
    /// storage-name hashes rendered as strings, including negative FNV outputs) keeps its #1145/#981
    /// restart seed unchanged.
    /// </summary>
    [Theory]
    [InlineData("42", 42)]
    [InlineData("-2026", -2026)]
    [InlineData("2147483647", int.MaxValue)]
    public void AnIntegerKey_ReadsAndWritesTheSameId(string key, int expected)
    {
        Assert.Equal(expected, AlertHistoryServerIdentity.StorageId(key));
        Assert.Equal(expected, AlertHistoryServerIdentity.SeedScope(key));
    }

    /// <summary>
    /// The self-alert family's shapes: composite keys, sentinels, the empty key the deprecated shell
    /// passes. All write into the 0 bucket and none may read out of it.
    /// </summary>
    [Theory]
    [InlineData("cost:12:wait_stats")]
    [InlineData("ag:5:replica")]
    [InlineData("disk")]
    [InlineData("")]
    [InlineData("12:RO")]
    [InlineData("1154bad")]
    public void AnUnparseableKey_WritesToTheBucket_AndTheSeedDeclines(string key)
    {
        Assert.Equal(0, AlertHistoryServerIdentity.StorageId(key));
        Assert.Null(AlertHistoryServerIdentity.SeedScope(key));
    }

    /// <summary>
    /// The bucket is write-only even for a key that genuinely IS "0": its rows are mixed with every
    /// collapsed key's by construction, so an answer from them is exactly the cross-key history the
    /// invariant forbids. The cost is one hypothetical server's restart continuity — a post; the
    /// alternative was #3456.
    /// </summary>
    [Fact]
    public void TheLiteralZeroKey_WritesToTheBucket_AndTheSeedStillDeclines()
    {
        Assert.Equal(0, AlertHistoryServerIdentity.StorageId("0"));
        Assert.Null(AlertHistoryServerIdentity.SeedScope("0"));
    }

    /// <summary>
    /// The invariant as a property: over any key, the seed either declines or answers with the exact
    /// non-zero id the write side used — never a different id, and never the bucket. This is the statement
    /// "the seed never answers one key's question with another key's history" in executable form, so a
    /// future edit that lets the two halves drift apart fails here by name.
    /// </summary>
    [Theory]
    [InlineData("42")]
    [InlineData("-7")]
    [InlineData("0")]
    [InlineData("cost:12:wait_stats")]
    [InlineData("ag:5:replica")]
    [InlineData("")]
    public void TheSeedEitherDeclines_OrAnswersWithTheWriteSidesOwnNonZeroId(string key)
    {
        var scope = AlertHistoryServerIdentity.SeedScope(key);

        if (scope is null)
        {
            return;
        }

        Assert.Equal(AlertHistoryServerIdentity.StorageId(key), scope.Value);
        Assert.NotEqual(0, scope.Value);
    }
}
