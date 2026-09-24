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
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgLogFormatCapability"/>'s cache discipline (#4053 review L1): a target's verdict is checked
/// at most once per <see cref="PgLogFormatCapability.CacheTtl"/>, except when <see cref="PgLogFormatCapability.Invalidate"/>
/// drops it early on evidence the cached verdict is stale.
/// </summary>
[Collection("pg-log-format-capability-statics")]
public sealed class PgLogFormatCapabilityTests : IDisposable
{
    /// <summary>Every test starts and ends clean \u2014 this is process-wide static state.</summary>
    public PgLogFormatCapabilityTests()
    {
        PgLogFormatCapability.Reset();
        PgLogFormatCapability.CacheTtl = TimeSpan.FromHours(1);
    }

    public void Dispose()
    {
        PgLogFormatCapability.Reset();
        PgLogFormatCapability.CacheTtl = TimeSpan.FromHours(1);
    }

    [Fact]
    public async Task IsCsvlogEnabledAsync_CachesAcrossCalls_UntilInvalidated()
    {
        using var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = true };

        var first = await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        Assert.True(first);
        Assert.Equal(1, connection.ExecuteCount);

        /* Cached: a second call with the setting now false must still return the CACHED true, and must not
           round-trip again. */
        connection.Scalar = false;
        var second = await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        Assert.True(second);
        Assert.Equal(1, connection.ExecuteCount);

        /* #4053 review L1: Invalidate drops the stale entry, so the next call re-probes and picks up the
           now-current setting. */
        PgLogFormatCapability.Invalidate("target-a");

        var third = await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        Assert.False(third);
        Assert.Equal(2, connection.ExecuteCount);
    }

    [Fact]
    public async Task InvalidateDropsOneTargetsVerdictOnly()
    {
        using var connectionA = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = true };
        using var connectionB = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = true };

        await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionA, "target-a", CancellationToken.None);
        await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionB, "target-b", CancellationToken.None);

        PgLogFormatCapability.Invalidate("target-a");

        /* target-b's cached verdict survives target-a's invalidation: no round trip needed to answer it. */
        connectionB.Scalar = false;
        var stillCached = await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionB, "target-b", CancellationToken.None);
        Assert.True(stillCached);
        Assert.Equal(1, connectionB.ExecuteCount);

        /* target-a re-probes. */
        connectionA.Scalar = false;
        var reprobed = await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionA, "target-a", CancellationToken.None);
        Assert.False(reprobed);
        Assert.Equal(2, connectionA.ExecuteCount);
    }
}
