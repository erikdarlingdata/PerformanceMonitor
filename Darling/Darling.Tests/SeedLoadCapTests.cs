/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7295 round 3: the cap on a failing restart-seed read. The engine and the PostgreSQL worker both lean on it, and their own tests
/// drive the full passes. These pin the counter itself: when it says "seed empty", that it warns once with the store, the key and
/// the step named, that keys and steps count apart, and that cancellation is never counted as a failed read.
/// </summary>
public sealed class SeedLoadCapTests
{
    private static readonly Exception Failure = new InvalidOperationException("permission denied for table config_edge_trigger_watermarks");

    [Fact]
    public void TheThirdFailedLoadInARowReachesTheCap_AndTheEarlierOnesDoNot()
    {
        var cap = new SeedLoadCap();
        for (var failed = 1; failed < SeedLoadCap.MaxFailedLoads; failed++)
        {
            Assert.False(cap.RecordFailedLoad(null, "PgAlertStateStore", "101", "deadlock count watermark", Failure));
        }

        Assert.True(cap.RecordFailedLoad(null, "PgAlertStateStore", "101", "deadlock count watermark", Failure));
    }

    [Fact]
    public void TheCapWarnsOnce_NamingTheStoreTheKeyAndTheStep_AndSayingADuplicatePageIsPossible()
    {
        var cap = new SeedLoadCap();
        var log = new CapturingTestLogger();
        for (var failed = 1; failed <= SeedLoadCap.MaxFailedLoads; failed++)
        {
            cap.RecordFailedLoad(log, "PgAlertStateStore", "101", "deadlock count watermark", Failure);
        }

        var warning = Assert.Single(log.Lines, line => line.StartsWith("Warning:", StringComparison.Ordinal));
        Assert.Contains("PgAlertStateStore", warning, StringComparison.Ordinal);
        Assert.Contains("101", warning, StringComparison.Ordinal);
        Assert.Contains("deadlock count watermark", warning, StringComparison.Ordinal);
        Assert.Contains("may be sent once more", warning, StringComparison.Ordinal);
        Assert.Contains("permission denied", warning, StringComparison.Ordinal);
    }

    [Fact]
    public void EachKeyAndStepCountsApart()
    {
        var cap = new SeedLoadCap();
        for (var failed = 1; failed < SeedLoadCap.MaxFailedLoads; failed++)
        {
            Assert.False(cap.RecordFailedLoad(null, "s", "101", "deadlock count watermark", Failure));
            Assert.False(cap.RecordFailedLoad(null, "s", "202", "deadlock count watermark", Failure));
            Assert.False(cap.RecordFailedLoad(null, "s", "101", "CPU persistence record", Failure));
        }

        Assert.True(cap.RecordFailedLoad(null, "s", "101", "deadlock count watermark", Failure));
        Assert.False(cap.RecordFailedLoad(null, "s", "303", "deadlock count watermark", Failure));
    }

    [Fact]
    public async Task ALoadThatFailsRethrowsUntilTheCap_ThenAnswersNothing_AndCountsOnlyThatFailure()
    {
        /* The rethrown failures are counted by the caller's own catch, so the cap counts none of them. The one it swallows is counted
           under the name the caller gave, for the server key, so alert_read_health does not lose the failure that reached the cap. */
        var counter = new AlertReadFailureCounter();
        var cap = new SeedLoadCap(counter);
        var attempts = 0;

        Task<int?> Load()
        {
            attempts++;
            throw Failure;
        }

        for (var failed = 1; failed < SeedLoadCap.MaxFailedLoads; failed++)
        {
            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(
                () => cap.LoadOrSeedEmptyAsync(Load, null, "PgAlertStateStore", "101", "blocking count watermark", "PostgreSQL blocking watermark seed"));
            Assert.Same(Failure, thrown);
        }

        Assert.Equal(0, counter.ReadFor("101").ServerReadFailures);

        var seeded = await cap.LoadOrSeedEmptyAsync(Load, null, "PgAlertStateStore", "101", "blocking count watermark", "PostgreSQL blocking watermark seed");
        Assert.Null(seeded);
        Assert.Equal(1, counter.ReadFor("101").ServerReadFailures);
        Assert.Equal("PostgreSQL blocking watermark seed", counter.ReadFor("101").LastFailureRead);
        Assert.Equal(SeedLoadCap.MaxFailedLoads, attempts);
    }

    [Fact]
    public async Task ALoadThatWorks_AnswersItsValue_AndCountsNothing()
    {
        var counter = new AlertReadFailureCounter();
        var cap = new SeedLoadCap(counter);
        Assert.Equal(7, await cap.LoadOrSeedEmptyAsync<int?>(() => Task.FromResult<int?>(7), null, "s", "101", "blocking count watermark", "read"));
        Assert.Null(await cap.LoadOrSeedEmptyAsync<int?>(() => Task.FromResult<int?>(null), null, "s", "101", "blocking count watermark", "read"));
        Assert.Equal(0, counter.ReadFor("101").ServerReadFailures);
    }

    [Fact]
    public async Task ACancelledLoad_PropagatesAndIsNotCountedAsAFailedRead()
    {
        var cap = new SeedLoadCap();
        for (var cancelled = 0; cancelled < SeedLoadCap.MaxFailedLoads + 1; cancelled++)
        {
            await Assert.ThrowsAsync<OperationCanceledException>(
                () => cap.LoadOrSeedEmptyAsync<int?>(() => throw new OperationCanceledException(), null, "s", "101", "blocking count watermark", "read"));
        }

        /* None of those counted: a real failure still needs its full run of failed loads before the cap says seed empty. */
        for (var failed = 1; failed < SeedLoadCap.MaxFailedLoads; failed++)
        {
            Assert.False(cap.RecordFailedLoad(null, "s", "101", "blocking count watermark", Failure));
        }

        Assert.True(cap.RecordFailedLoad(null, "s", "101", "blocking count watermark", Failure));
    }
}
