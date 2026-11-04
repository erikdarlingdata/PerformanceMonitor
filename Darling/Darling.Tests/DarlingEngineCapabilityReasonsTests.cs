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
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7374: a cancelled request must not be turned into "no claim" by the engine capability reads. <c>NotCollectedStatusAsync</c> and
/// <c>NotCollectedReasonsAsync</c> answer null / nothing when the registry read FAILS, but a cancellation propagates, so the caller does not
/// carry on with answers it can no longer deliver. No database is needed: the token is cancelled before the read starts.
/// </summary>
public sealed class DarlingEngineCapabilityReasonsTests
{
    private static NpgsqlDataSource Unreachable() => NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Database=x;Timeout=1");

    [Fact]
    public async Task NotCollectedStatusAsync_PropagatesACancellation()
    {
        await using var data = Unreachable();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => DarlingEngineCapability.NotCollectedStatusAsync(data, 1, "srv", "query_stats", cts.Token));
    }

    [Fact]
    public async Task NotCollectedReasonsAsync_PropagatesACancellation()
    {
        await using var data = Unreachable();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => DarlingEngineCapability.NotCollectedReasonsAsync(data, [(1, "srv")], "query_stats", cts.Token));
    }

    [Fact]
    public async Task NotCollectedReasonsAsync_MakesNoClaimWhenTheRegistryReadFails()
    {
        await using var data = Unreachable();

        var reasons = await DarlingEngineCapability.NotCollectedReasonsAsync(data, [(1, "srv")], "query_stats", CancellationToken.None);

        Assert.Empty(reasons);
    }
}
