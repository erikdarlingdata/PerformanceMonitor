/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5097: the ambient <see cref="ReadScope"/> a recorded read runs inside, and the readers that note a silent
/// raw fallback into it. No database: every fault is a store that cannot be reached.
/// </summary>
public sealed class ReadScopeTests
{
    /// <summary>A port nothing listens on, so the open is refused at once. Never <c>localhost</c>: a shard may
    /// have a live Postgres there.</summary>
    private const string RefusedConnectionString = "Host=127.0.0.1;Port=1;Username=u;Password=p;Database=d;Timeout=2;Command Timeout=2;Pooling=false";

    private static async Task NoteInChildAsync(ReadFallback kind)
    {
        await Task.Yield();
        ReadScope.NoteFallback(kind, "child", null);
    }

    [Fact]
    public async Task AFallbackNotedInAChildAsyncMethod_IsVisibleToTheParentAfterTheAwait()
    {
        using var scope = ReadScope.Open(null);
        await NoteInChildAsync(ReadFallback.FallbackRaw);

        Assert.Equal(ReadFallback.FallbackRaw, scope.Fallback);
    }

    [Fact]
    public async Task TwoConcurrentScopes_DoNotCrossNotes()
    {
        async Task<ReadFallback?> RunAsync(ReadFallback? note)
        {
            using var scope = ReadScope.Open(null);
            await Task.Yield();
            if (note is { } kind)
            {
                await NoteInChildAsync(kind);
            }

            await Task.Delay(20, TestContext.Current.CancellationToken);
            return scope.Fallback;
        }

        var results = await Task.WhenAll(RunAsync(ReadFallback.GateFailed), RunAsync(null), RunAsync(ReadFallback.FallbackRaw));

        Assert.Equal(new ReadFallback?[] { ReadFallback.GateFailed, null, ReadFallback.FallbackRaw }, results);
    }

    [Fact]
    public void Resolve_ReplacesOnlyOk_AndGateFailedOutranksFallbackRaw()
    {
        Assert.Equal(ReadOutcome.FallbackRaw, ReadScope.Resolve(ReadOutcome.Ok, ReadFallback.FallbackRaw));
        Assert.Equal(ReadOutcome.GateFailed, ReadScope.Resolve(ReadOutcome.Ok, ReadFallback.GateFailed));
        Assert.Equal(ReadOutcome.Ok, ReadScope.Resolve(ReadOutcome.Ok, null));
        foreach (var bigger in new[] { ReadOutcome.Timeout, ReadOutcome.Cancelled, ReadOutcome.Error, ReadOutcome.Limit })
        {
            Assert.Equal(bigger, ReadScope.Resolve(bigger, ReadFallback.GateFailed));
            Assert.Equal(bigger, ReadScope.Resolve(bigger, ReadFallback.FallbackRaw));
        }

        using var scope = ReadScope.Open(null);
        ReadScope.NoteFallback(ReadFallback.GateFailed, "a", null);
        ReadScope.NoteFallback(ReadFallback.FallbackRaw, "b", null);
        Assert.Equal(ReadOutcome.GateFailed, ReadScope.Resolve(ReadOutcome.Ok, scope.Fallback));
    }

    [Fact]
    public void ANoteWithNoScope_DoesNotThrow_AndDisposeRestoresThePreviousScope()
    {
        Assert.Null(ReadScope.Current);
        ReadScope.NoteFallback(ReadFallback.GateFailed, "unscoped", new InvalidOperationException("x"));
        ReadScope.Warn("unscoped", null);

        using var outer = ReadScope.Open(null);
        using (var inner = ReadScope.Open(null))
        {
            Assert.Same(inner.Scope, ReadScope.Current);
        }

        Assert.Same(outer.Scope, ReadScope.Current);
    }

    [Fact]
    public void ANoteReachesTheScopeLoggerOnce_AtWarning_WithTheException()
    {
        var logger = new CapturingTestLogger();
        using var scope = ReadScope.Open(logger);

        ReadScope.NoteFallback(ReadFallback.FallbackRaw, "a site", new InvalidOperationException("boom"));

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains("a site", logger.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheGateFaulting_ReturnsAPlanThatSaysTheDecisionFailed_AndWarnsOnce()
    {
        var logger = new CapturingTestLogger();
        await using var connection = new NpgsqlConnection(RefusedConnectionString);
        var end = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);

        var plan = await QueryStoreIntervalWide.ResolveReadAsync(
            connection, 1, end.AddHours(-48), end, end, TimeSpan.FromHours(24), 5, logger, TestContext.Current.CancellationToken);

        Assert.False(plan.UseTable);
        Assert.True(plan.DecisionFailed);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task TheMcpTopQueriesRead_OverAnUnreachableStore_NotesGateFailed_AndLogsThroughTheScope()
    {
        var logger = new CapturingTestLogger();
        await using var postgres = NpgsqlDataSource.Create(RefusedConnectionString);
        var end = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);

        using var scope = ReadScope.Open(logger);
        await Assert.ThrowsAnyAsync<Exception>(() => DarlingDataReader.GetQueryStoreTopWithReachAsync(
            postgres, 1, end.AddHours(-25), end, 10, null, null, null, TestContext.Current.CancellationToken));

        Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
        Assert.True(logger.CountAtLevel(LogLevel.Warning) >= 1, logger.Joined);
    }

    [Fact]
    public async Task TheComposeWideGate_OverAnUnreachableStore_NotesGateFailed()
    {
        await using var postgres = NpgsqlDataSource.Create(RefusedConnectionString);
        var end = new DateTime(2026, 9, 15, 0, 0, 0, DateTimeKind.Utc);

        using var scope = ReadScope.Open(new CapturingTestLogger());
        var resolution = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(
            postgres, null, end.AddDays(-3), end, null, TestContext.Current.CancellationToken);

        Assert.False(resolution.Eligible);
        Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
    }

    [Fact]
    public async Task TheHourlyEdgesGuard_OnAConnectionThatCannotRun_NotesGateFailed_AndRethrowsTheRollbackFault()
    {
        await using var connection = new NpgsqlConnection(RefusedConnectionString);
        var hour = new DateTime(2026, 9, 15, 3, 0, 0, DateTimeKind.Utc);
        var candidate = new ComposeHourlyEdgesCandidate("query_store_stats", "query_store_stats_hourly", hour, hour.AddHours(1));

        using var scope = ReadScope.Open(new CapturingTestLogger());
        await Assert.ThrowsAnyAsync<Exception>(() => DarlingWebEndpoints.ResolveHourlyEdgesVerdictAsync(
            connection, candidate, null, 30, TestContext.Current.CancellationToken));

        Assert.Equal(ReadFallback.GateFailed, scope.Fallback);
    }
}
