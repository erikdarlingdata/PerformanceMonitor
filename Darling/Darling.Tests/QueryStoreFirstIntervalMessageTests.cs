/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6181: which sentence a Query Store read gives when no Query Store sample has ever been stored. A server just added stores
/// nothing until its first interval closes, so "Query Store may be OFF" is wrong whenever the product's own latest evidence says
/// Query Store is READ_WRITE and the collector's last run succeeded. Both products and every read share
/// <see cref="CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage"/>, so the choice is tested here once.
/// </summary>
public sealed class QueryStoreFirstIntervalMessageTests
{
    private const string Server = "TestServer";

    private static CollectorRuntimePrecondition.QueryStoreDatabaseState Db(string state, long? interval = null) =>
        new("AppDb", state, interval);

    [Fact]
    public void QueryStoreOnAndCollectorSucceeded_SaysItIsOnAndNamesTheDefaultInterval()
    {
        var message = CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, new[] { Db("READ_WRITE") }, "SUCCESS", DateTime.UtcNow);

        Assert.NotNull(message);
        Assert.Contains("Query Store is on", message, StringComparison.Ordinal);
        Assert.Contains("interval closes", message, StringComparison.Ordinal);
        Assert.Contains("within the Query Store interval (60 minutes by default)", message, StringComparison.Ordinal);
        Assert.DoesNotContain("may be OFF", message, StringComparison.Ordinal);
    }

    [Fact]
    public void StoredIntervalLength_IsNamedInsteadOfTheDefault()
    {
        var message = CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, new[] { Db("READ_WRITE", 15), Db("READ_WRITE", 30) }, "success", null);

        Assert.NotNull(message);
        Assert.Contains("30 minutes", message, StringComparison.Ordinal);
        Assert.DoesNotContain("60 minutes", message, StringComparison.Ordinal);
    }

    [Fact]
    public void ZeroOrMissingStoredInterval_FallsBackToTheDefault()
    {
        var message = CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, new[] { Db("READ_WRITE", 0) }, "SUCCESS", null);

        Assert.NotNull(message);
        Assert.Contains("60 minutes by default", message, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyTheCollectingDatabasesSetTheInterval()
    {
        var message = CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, new[] { Db("READ_WRITE", 15), Db("OFF", 120) }, "SUCCESS", null);

        Assert.NotNull(message);
        Assert.Contains("15 minutes", message, StringComparison.Ordinal);
        Assert.DoesNotContain("120", message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("OFF")]
    [InlineData("READ_ONLY")]
    [InlineData("ERROR")]
    [InlineData("")]
    [InlineData(null)]
    public void QueryStoreNotCollecting_KeepsTheMayBeOffText(string? state)
    {
        Assert.Null(CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, new[] { Db(state!) }, "SUCCESS", DateTime.UtcNow));
    }

    [Fact]
    public void UnknownState_KeepsTheMayBeOffText()
    {
        Assert.Null(CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, Array.Empty<CollectorRuntimePrecondition.QueryStoreDatabaseState>(), "SUCCESS", DateTime.UtcNow));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("ERROR")]
    [InlineData("YIELDED")]
    [InlineData("SESSION_MISSING")]
    public void CollectorLastRunNotSuccessful_KeepsTheMayBeOffText(string? status)
    {
        Assert.Null(CollectorRuntimePrecondition.QueryStoreFirstIntervalMessage(
            Server, new[] { Db("READ_WRITE", 60) }, status, DateTime.UtcNow));
    }
}
