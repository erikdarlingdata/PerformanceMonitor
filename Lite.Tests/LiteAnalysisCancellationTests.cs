/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A cancelled on-demand analysis request stops instead of reading DuckDB to the end, mirroring
/// Darling's <c>DarlingAnalysisService</c> twins. A PRE-cancelled token exercises the store-read
/// checkpoints these methods now reach without needing to race a real long-running collection: every
/// DuckDB command inside <see cref="DuckDbFactCollector"/> and <see cref="BaselineProvider"/> observes
/// <c>AnalysisContext.CancellationToken</c>, so an already-cancelled token throws on the very first read
/// rather than completing and being swallowed into an empty result.
/// </summary>
public sealed class LiteAnalysisCancellationTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;

    public LiteAnalysisCancellationTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _serverManager = new ServerManager(System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), "LiteAnalysisCancellationTests_" + Guid.NewGuid().ToString("N")[..8], "config"));
    }

    [Fact]
    public async Task CollectAndScoreFactsAsync_WithACancelledToken_ThrowsInsteadOfReturningAnEmptyResult()
    {
        var service = new AnalysisService(_duckDb);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.CollectAndScoreFactsAsync(1, "TestServer", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task CollectConfigAuditFactsAsync_WithACancelledToken_ThrowsInsteadOfReturningAnEmptyResult()
    {
        var service = new AnalysisService(_duckDb);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.CollectConfigAuditFactsAsync(1, "TestServer", cancellationToken: cts.Token));
    }

    [Fact]
    public async Task ComparePeriodsAsync_WithACancelledToken_ThrowsInsteadOfReturningAnEmptyResult()
    {
        var service = new AnalysisService(_duckDb);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        var now = DateTime.UtcNow;
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => service.ComparePeriodsAsync(
                1, "TestServer",
                now.AddHours(-8), now.AddHours(-4),
                now.AddHours(-4), now,
                cts.Token));
    }
}
