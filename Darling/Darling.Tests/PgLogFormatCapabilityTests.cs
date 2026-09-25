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
/// <see cref="PgLogFormatCapability"/>'s cache discipline (#4053 review L1, part a2): a target's verdict is
/// checked at most once per <see cref="PgLogFormatCapability.CacheTtl"/>, except when
/// <see cref="PgLogFormatCapability.Invalidate"/> drops it early on evidence the cached verdict is stale.
/// The probe returns BOTH booleans in one scalar (#4053 part a2), so <c>IsCsvlogEnabledAsync</c> and
/// <c>IsJsonlogEnabledAsync</c> share the one cache entry and the one round trip.
/// </summary>
[Collection("pg-log-format-capability-statics")]
public sealed class PgLogFormatCapabilityTests : IDisposable
{
    /// <summary>Every test starts and ends clean — this is process-wide static state.</summary>
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

    private static string Scalar(bool csvlog, bool jsonlog) =>
        (csvlog ? "true" : "false") + ":" + (jsonlog ? "true" : "false");

    [Fact]
    public async Task IsCsvlogEnabledAsync_CachesAcrossCalls_UntilInvalidated()
    {
        using var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(true, false) };

        var first = await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        Assert.True(first);
        Assert.Equal(1, connection.ExecuteCount);

        /* Cached: a second call with the setting now false must still return the CACHED true, and must not
           round-trip again. */
        connection.Scalar = Scalar(false, false);
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
        using var connectionA = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(true, false) };
        using var connectionB = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(true, false) };

        await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionA, "target-a", CancellationToken.None);
        await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionB, "target-b", CancellationToken.None);

        PgLogFormatCapability.Invalidate("target-a");

        /* target-b's cached verdict survives target-a's invalidation: no round trip needed to answer it. */
        connectionB.Scalar = Scalar(false, false);
        var stillCached = await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionB, "target-b", CancellationToken.None);
        Assert.True(stillCached);
        Assert.Equal(1, connectionB.ExecuteCount);

        /* target-a re-probes. */
        connectionA.Scalar = Scalar(false, false);
        var reprobed = await PgLogFormatCapability.IsCsvlogEnabledAsync(connectionA, "target-a", CancellationToken.None);
        Assert.False(reprobed);
        Assert.Equal(2, connectionA.ExecuteCount);
    }

    /// <summary>
    /// #4053 part a2: one probe scalar answers both flags — every combination, including both true at once
    /// (an operator who lists both destinations).
    /// </summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task IsJsonlogEnabledAsync_AndIsCsvlogEnabledAsync_ReadTheSameOneRoundTripProbe(bool csvlog, bool jsonlog)
    {
        using var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(csvlog, jsonlog) };

        var csvlogResult = await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        var jsonlogResult = await PgLogFormatCapability.IsJsonlogEnabledAsync(connection, "target-a", CancellationToken.None);

        Assert.Equal(csvlog, csvlogResult);
        Assert.Equal(jsonlog, jsonlogResult);
        /* One round trip serves both calls: the second reads the cache the first one filled. */
        Assert.Equal(1, connection.ExecuteCount);
    }

    /// <summary>#4053 a2 review W1: only a stderr-only verdict is contradicted by a missing stderr file, so
    /// only that verdict lets the stderr arm clear the cache. A jsonlog or csvlog verdict, or no verdict, does
    /// not.</summary>
    [Theory]
    [InlineData(false, false, true)]
    [InlineData(true, false, false)]
    [InlineData(false, true, false)]
    [InlineData(true, true, false)]
    public async Task CachedVerdictIsStderrOnly_OnlyForAVerdictOfNeither(bool csvlog, bool jsonlog, bool expected)
    {
        Assert.False(PgLogFormatCapability.CachedVerdictIsStderrOnly("target-a"));

        using var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(csvlog, jsonlog) };
        await PgLogFormatCapability.IsJsonlogEnabledAsync(connection, "target-a", CancellationToken.None);

        Assert.Equal(expected, PgLogFormatCapability.CachedVerdictIsStderrOnly("target-a"));
    }

    /// <summary>#4053 part a2: Invalidate drops BOTH cached booleans, not just the csvlog one.</summary>
    [Fact]
    public async Task InvalidateDropsBothCachedFlags()
    {
        using var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(true, false) };

        await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        await PgLogFormatCapability.IsJsonlogEnabledAsync(connection, "target-a", CancellationToken.None);
        Assert.Equal(1, connection.ExecuteCount);

        PgLogFormatCapability.Invalidate("target-a");

        connection.Scalar = Scalar(false, true);

        var csvlogResult = await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);
        var jsonlogResult = await PgLogFormatCapability.IsJsonlogEnabledAsync(connection, "target-a", CancellationToken.None);

        Assert.False(csvlogResult);
        Assert.True(jsonlogResult);
        Assert.Equal(2, connection.ExecuteCount);
    }

    /// <summary>#4053 part a2: Reset drops every cached verdict, both flags, for every target.</summary>
    [Fact]
    public async Task ResetClearsBothCachedFlagsForEveryTarget()
    {
        using var connection = new PgReadBinaryFileCapabilityTests.FakeScalarConnection { Scalar = Scalar(true, true) };
        await PgLogFormatCapability.IsCsvlogEnabledAsync(connection, "target-a", CancellationToken.None);

        PgLogFormatCapability.Reset();

        connection.Scalar = Scalar(false, false);
        var afterReset = await PgLogFormatCapability.IsJsonlogEnabledAsync(connection, "target-a", CancellationToken.None);

        Assert.False(afterReset);
        Assert.Equal(2, connection.ExecuteCount);
    }
}
