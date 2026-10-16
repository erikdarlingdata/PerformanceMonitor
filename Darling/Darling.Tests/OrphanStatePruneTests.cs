/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#4660: the orphaned per-database state prune runs at most hourly per server, through one shared rule.</summary>
public class OrphanStatePruneTests
{
    private static readonly DateTime Now = new(2026, 9, 28, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void IsDue_NeverPruned_IsDue() => Assert.True(OrphanStatePrune.IsDue(null, Now));

    [Fact]
    public void IsDue_FiftyNineMinutes_IsNotDue() => Assert.False(OrphanStatePrune.IsDue(Now.AddMinutes(-59), Now));

    [Fact]
    public void IsDue_ExactlyAnHour_IsDue() => Assert.True(OrphanStatePrune.IsDue(Now.AddMinutes(-60), Now));

    [Fact]
    public void IsDue_TwoHours_IsDue() => Assert.True(OrphanStatePrune.IsDue(Now.AddHours(-2), Now));

    [Fact]
    public void IsDue_ClockStepBack_IsDue() => Assert.True(OrphanStatePrune.IsDue(Now.AddMinutes(5), Now));

    /// <summary>The wrapper's success-only recording, through the product's own entry: a prune that fails (the store is
    /// unreachable) is not recorded, so the next call tries again instead of waiting an hour.</summary>
    [Fact]
    public async Task Wrapper_FailedPrune_IsNotRecorded_AndTheNextCallRunsAgain()
    {
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1;Pooling=false");
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        Assert.False(await runner.PruneOrphanedQueryStoreDatabaseStateAsync(7, CancellationToken.None));
        await runner.PruneOrphanedQueryStoreDatabaseStateIfDueAsync(7, CancellationToken.None);

        var field = typeof(DarlingCollectorRunner).GetField("_lastOrphanStatePruneUtc", BindingFlags.Instance | BindingFlags.NonPublic)!;
        var recorded = (System.Collections.Concurrent.ConcurrentDictionary<int, DateTime>)field.GetValue(runner)!;
        Assert.Empty(recorded);

        /* A success recorded a moment ago suppresses the next call: pre-seed it and the wrapper must not touch the store. */
        recorded[7] = DateTime.UtcNow;
        await runner.PruneOrphanedQueryStoreDatabaseStateIfDueAsync(7, CancellationToken.None);
        Assert.Single(recorded);
    }

    [Fact]
    public void BothSkus_CallSitesUseTheWrapper_AndTheWrapperRecordsOnlySuccess()
    {
        var root = FindRoot();
        var darling = File.ReadAllText(Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));
        var lite = File.ReadAllText(Path.Combine(root, "Lite", "Services", "RemoteCollectorService.DefinitionRunner.cs"));
        var liteImpl = File.ReadAllText(Path.Combine(root, "Lite", "Services", "RemoteCollectorService.QueryStoreBackfill.cs"));

        Assert.Contains("await PruneOrphanedQueryStoreDatabaseStateIfDueAsync(server.ServerId,", darling, StringComparison.Ordinal);
        Assert.DoesNotContain("await PruneOrphanedQueryStoreDatabaseStateAsync(server.ServerId", darling, StringComparison.Ordinal);
        Assert.Contains("await PruneOrphanedQueryStoreDatabaseStateIfDueAsync(serverId,", lite, StringComparison.Ordinal);
        Assert.DoesNotContain("await PruneOrphanedQueryStoreDatabaseStateAsync(", lite, StringComparison.Ordinal);

        foreach (var source in new[] { darling, liteImpl })
        {
            var start = source.IndexOf("Task PruneOrphanedQueryStoreDatabaseStateIfDueAsync(", StringComparison.Ordinal);
            Assert.True(start >= 0);
            var body = source.Substring(start, 700);
            Assert.Contains("OrphanStatePrune.IsDue(", body, StringComparison.Ordinal);
            Assert.Contains("if (await PruneOrphanedQueryStoreDatabaseStateAsync(", body, StringComparison.Ordinal);
        }
    }

    private static string FindRoot()
    {
        var dir = AppContext.BaseDirectory;
        while (dir != null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("repo root not found");
    }
}
