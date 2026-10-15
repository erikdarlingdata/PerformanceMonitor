/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5813: the Query Store read budget's shared pieces, and its wiring into all four per-database read sites (Darling's
/// enumerated readItem and Azure loop, Lite's twins). <see cref="QueryStoreReadBudgetLiveTests"/> drives one site end to
/// end; these pins keep the other three from drifting off the same shape.
/// </summary>
public sealed class QueryStoreReadBudgetWiringPinTests
{
    [Fact]
    public void TheBudgetIsThirtySeconds_ATenthOfTheFiveMinuteCadence()
    {
        Assert.Equal(TimeSpan.FromSeconds(30), QueryStoreReadBudget.Budget);
        Assert.True(QueryStoreReadBudget.Budget < QueryStoreCollector.PerDatabaseWallClockBudget);
        Assert.True(QueryStoreReadBudget.Budget.TotalSeconds < QueryStoreCollector.Instance.CommandTimeoutSecondsOverride);
    }

    [Fact]
    public void TheWarningKey_IsOncePerServerDatabaseAndUtcDay()
    {
        var log = new QueryStoreReadTimeoutLog();
        var day = new DateTime(2026, 10, 14, 0, 30, 0, DateTimeKind.Utc);

        Assert.True(log.ShouldWarn(1, "alpha", day));
        Assert.False(log.ShouldWarn(1, "alpha", day.AddHours(23)));
        Assert.True(log.ShouldWarn(1, "zeta", day.AddHours(1)));
        Assert.True(log.ShouldWarn(2, "alpha", day.AddHours(1)));
        Assert.True(log.ShouldWarn(1, "alpha", day.AddDays(1).Date));
        Assert.False(log.ShouldWarn(1, "alpha", day.AddDays(1).AddHours(5)));
    }

    [Fact]
    public void ATimedOutRead_IsTheSlowestItem_WithItsMarkerAndTheBudget()
    {
        var fanout = new FanoutCostAccumulator();
        fanout.Observe("fast", 900);
        Assert.True(QueryStoreReadBudget.ObserveTimeout(
            new QueryStoreReadTimeoutException("slow", QueryStoreReadBudget.Budget, null), fanout));

        var result = fanout.Result!.Value;
        Assert.Equal("slow (timed out)", result.SlowestItem);
        Assert.Equal(30_000, result.SlowestItemMs);
        Assert.Equal(2, result.ItemCount);
    }

    [Fact]
    public void AnyOtherFailure_IsNotObserved()
    {
        var fanout = new FanoutCostAccumulator();
        Assert.False(QueryStoreReadBudget.ObserveTimeout(new TimeoutException("the 10-minute budget"), fanout));
        Assert.False(QueryStoreReadBudget.ObserveTimeout(new InvalidOperationException("offline"), fanout));
        Assert.Null(fanout.Result);
    }

    [Fact]
    public void TheMessage_NamesTheDatabaseTheBudgetAndTheCarry()
    {
        var ex = new QueryStoreReadTimeoutException("alpha", QueryStoreReadBudget.Budget, null);
        Assert.Equal(
            "Query Store read for database [alpha] timed out at its 30-second budget; the database keeps its watermark and the next pass reads it from there.",
            ex.Message);
        Assert.IsAssignableFrom<TimeoutException>(ex);
    }

    [Fact]
    public void TheBudget_StartsForQueryStoreOnly()
    {
        using var queryStore = QueryStoreReadBudget.Start("query_store", TimeSpan.FromSeconds(30), CancellationToken.None);
        Assert.NotNull(queryStore);
        Assert.Null(QueryStoreReadBudget.Start("procedure_stats", TimeSpan.FromSeconds(30), CancellationToken.None));
        Assert.Null(QueryStoreReadBudget.Start("plan_correction", TimeSpan.FromSeconds(30), CancellationToken.None));
    }

    [Fact]
    public void AShutdown_IsNotABudgetExpiry()
    {
        using var outer = new CancellationTokenSource();
        using var budget = QueryStoreReadBudget.Start("query_store", TimeSpan.FromMinutes(5), outer.Token);
        outer.Cancel();
        Assert.False(QueryStoreReadBudget.Expired(budget, outer.Token));
    }

    [Theory]
    [InlineData("Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs")]
    [InlineData("Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs")]
    public void BothFanOutArms_BudgetTheRead_AndNoteTheTimeout(string a, string b, string c)
    {
        var code = RepoFile.ReadRepoFileLf(a, b, c);

        /* Two reads, two translations to the timeout, two error arms that note it: the enumerated arm and the Azure arm. */
        Assert.Equal(2, Regex.Matches(code, @"QueryStoreReadBudget\.Start\(definition\.Name, QueryStoreReadBudgetSpan, ").Count);
        Assert.True(Regex.Matches(code, @"when \(ex is not OutOfMemoryException && QueryStoreReadBudget\.Expired\(readBudget, ").Count >= 2);
        Assert.Equal(2, Regex.Matches(code, @"NoteQueryStoreReadTimeout\(server, ").Count);

        /* The reads run on the budget's token, never the item token, so only the read is cut. */
        Assert.True(code.Contains("ExecuteReaderAsync(readToken)", StringComparison.Ordinal));
        Assert.True(Regex.Matches(code, @"Read(Item)?Async\([^;]*readToken\)").Count >= 2);
    }
}
