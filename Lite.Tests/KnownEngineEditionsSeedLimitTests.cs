/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The startup seed of the stored engine editions (<see cref="KnownEngineEditions.SeedFromStoreAsync"/>). Collection,
/// the alert engine, the MCP server and the server list start after it, so it waits for the read at most its limit.
/// A read that stalls, fails, or fails after the limit is logged as a warning and never stops the start. A result
/// that arrives after the limit still seeds. The log assertions drain AppLogger's process-wide buffer, so the class
/// runs in the app-logger-statics collection.
/// </summary>
[Collection("app-logger-statics")]
public sealed class KnownEngineEditionsSeedLimitTests
{
    private const int AzureSqlDatabase = CollectorEngineCapability.AzureSqlDatabaseEngineEdition;
    private const int Enterprise = 3;
    private const int Blank = CollectorEngineCapability.UnknownEngineEdition;

    /* The test's own bound: a seed that waits with no limit fails here instead of hanging the run. */
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(5);

    public KnownEngineEditionsSeedLimitTests()
    {
        AppLogger.SetMinimumLevel(AppLogger.DefaultMinimumLevel);
        AppLogger.DrainBufferedLines();
    }

    private static List<string> SeedLines() =>
        AppLogger.DrainBufferedLines().Where(l => l.Contains("[MasterScope]", StringComparison.Ordinal)).ToList();

    private static Task<IReadOnlyDictionary<int, int>> Editions(params (int ServerId, int Edition)[] rows) =>
        Task.FromResult<IReadOnlyDictionary<int, int>>(rows.ToDictionary(r => r.ServerId, r => r.Edition));

    [Fact]
    public void TheStartupLimit_IsBounded()
    {
        Assert.InRange(KnownEngineEditions.StartupSeedLimit, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(30));
    }

    [Fact]
    public async Task AReadWithinTheLimit_SeedsBeforeTheStartGoesOn()
    {
        var editions = new KnownEngineEditions();

        var seeded = await editions.SeedFromStoreAsync(() => Editions((1, AzureSqlDatabase)), KnownEngineEditions.StartupSeedLimit).WaitAsync(Guard);

        Assert.True(seeded);
        Assert.Equal(AzureSqlDatabase, editions.Resolve(1, Blank));
        Assert.Contains("[INFO ]", Assert.Single(SeedLines()), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AStalledRead_DoesNotHoldUpTheStartPastTheLimit_AndIsLoggedAsAWarning()
    {
        var editions = new KnownEngineEditions();
        var stalled = new TaskCompletionSource<IReadOnlyDictionary<int, int>>();

        var seeded = await editions.SeedFromStoreAsync(() => stalled.Task, TimeSpan.FromMilliseconds(200)).WaitAsync(Guard);

        Assert.False(seeded);
        var warning = Assert.Single(SeedLines());
        Assert.Contains("[WARN ]", warning, StringComparison.Ordinal);
        Assert.Contains("were not read within 0.2 s", warning, StringComparison.Ordinal);
        Assert.Equal(Blank, editions.Resolve(1, Blank));
    }

    [Fact]
    public async Task AResultAfterTheLimit_StillSeeds_AndKeepsAnEditionThatALiveStatusReportedMeanwhile()
    {
        var editions = new KnownEngineEditions();
        var stalled = new TaskCompletionSource<IReadOnlyDictionary<int, int>>();
        Assert.False(await editions.SeedFromStoreAsync(() => stalled.Task, TimeSpan.FromMilliseconds(100)).WaitAsync(Guard));
        Assert.Equal(Enterprise, editions.Resolve(2, Enterprise));
        SeedLines();

        stalled.SetResult(new Dictionary<int, int> { [1] = AzureSqlDatabase, [2] = AzureSqlDatabase });

        Assert.Equal(AzureSqlDatabase, editions.Resolve(1, Blank));
        Assert.Equal(Enterprise, editions.Resolve(2, Blank));
        Assert.Contains("[INFO ]", Assert.Single(SeedLines()), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AReadThatFailsAfterTheLimit_IsObservedAndLoggedAsAWarning()
    {
        var editions = new KnownEngineEditions();
        var stalled = new TaskCompletionSource<IReadOnlyDictionary<int, int>>();
        Assert.False(await editions.SeedFromStoreAsync(() => stalled.Task, TimeSpan.FromMilliseconds(100)).WaitAsync(Guard));
        SeedLines();

        stalled.SetException(new InvalidOperationException("the archive scan failed"));

        var warning = Assert.Single(SeedLines());
        Assert.Contains("[WARN ]", warning, StringComparison.Ordinal);
        Assert.Contains("the archive scan failed", warning, StringComparison.Ordinal);
        Assert.Equal(Blank, editions.Resolve(1, Blank));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AReadThatFails_IsLoggedAsAWarning_AndSeedsNothing(bool throwsBeforeItReturnsATask)
    {
        var editions = new KnownEngineEditions();
        Func<Task<IReadOnlyDictionary<int, int>>> read = throwsBeforeItReturnsATask
            ? () => throw new InvalidOperationException("the store is unavailable")
            : () => Task.FromException<IReadOnlyDictionary<int, int>>(new InvalidOperationException("the store is unavailable"));

        var seeded = await editions.SeedFromStoreAsync(read, KnownEngineEditions.StartupSeedLimit).WaitAsync(Guard);

        Assert.False(seeded);
        var warning = Assert.Single(SeedLines());
        Assert.Contains("[WARN ]", warning, StringComparison.Ordinal);
        Assert.Contains("the store is unavailable", warning, StringComparison.Ordinal);
        Assert.Equal(Blank, editions.Resolve(1, Blank));
    }
}
