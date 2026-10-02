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
using Npgsql;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A cancelled caller must see <see cref="OperationCanceledException"/> out of the two on-demand analysis
/// reads that have no per-pass budget of their own — <see cref="DarlingAnalysisService.CollectAndScoreFactsAsync"/>
/// and <see cref="DarlingAnalysisService.ComparePeriodsAsync"/>. Before this fix, both caught every
/// exception unconditionally and logged an ERROR instead of letting the cancellation propagate, the same
/// defect <see cref="DarlingAnalysisService.CollectConfigAuditFactsAsync"/>'s catch already excludes
/// (<c>ex is not OperationCanceledException</c>, #4203) — a cancelled `get_analysis_facts` or
/// `compare_analysis` MCP read would come back as an empty, successful-looking result instead of the
/// caller seeing its own cancellation.
///
/// <para>The store never has to answer: <see cref="DarlingAnalysisService.ResolveEngineAsync"/> opens the
/// first connection with the SAME token before either read touches a collector, and an already-cancelled
/// token throws <see cref="OperationCanceledException"/> at that first <c>OpenConnectionAsync</c> await —
/// Npgsql observes the token before it ever dials, exactly as <see cref="WebReadCancellationPinTests"/>'s
/// <c>DeadStore</c> documents for the same connection string. So this pin proves the WIRING (the real
/// public method, not a private catch site called directly), with no rig and no store, on a port nothing
/// listens on.</para>
/// </summary>
public sealed class AnalysisReadCancellationPinTests
{
    private static NpgsqlDataSource DeadStore() =>
        new NpgsqlDataSourceBuilder("Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=2")
            .Build();

    [Fact]
    public async Task CollectAndScoreFactsAsync_WithACancelledToken_ThrowsInsteadOfReturningAnEmptyResult()
    {
        await using var store = DeadStore();
        var service = new DarlingAnalysisService(store);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            service.CollectAndScoreFactsAsync(serverId: 1, serverName: "s", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task ComparePeriodsAsync_WithACancelledToken_ThrowsInsteadOfReturningAnEmptyResult()
    {
        await using var store = DeadStore();
        var service = new DarlingAnalysisService(store);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var now = DateTime.UtcNow;
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            service.ComparePeriodsAsync(
                serverId: 1, serverName: "s",
                baselineStart: now.AddHours(-8), baselineEnd: now.AddHours(-4),
                comparisonStart: now.AddHours(-4), comparisonEnd: now,
                cancellationToken: cts.Token));
    }
}
