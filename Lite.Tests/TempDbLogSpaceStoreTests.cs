/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8881, Lite's half: the tempdb log columns through the real DuckDB store and the real read. A row that carries the log reads back its
/// percent, reasons and holder. A row stored without it (the shape every row had before v83, and an engine without the function) reads back
/// null, never zero.
/// </summary>
public sealed class TempDbLogSpaceStoreTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public TempDbLogSpaceStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ALogRow_RoundTripsItsPercentReasonsAndHolder()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(reservedMb: 100, unallocatedMb: 900, logUsedPercent: 87.5, logHoldup: "ACTIVE_TRANSACTION", holderSessionId: 61, holderLogin: "etl_login");

        var info = await new LocalDataService(_duckDb).GetLatestTempDbSpaceAsync(TestDataSeeder.TestServerId);

        Assert.NotNull(info);
        Assert.Equal(87.5, info!.LogUsedPercent!.Value, precision: 2);
        Assert.Equal(1000d, info.LogTotalMb!.Value, precision: 2);
        Assert.Equal("ACTIVE_TRANSACTION", info.LogReuseWaitDesc);
        Assert.Equal("ACTIVE_TRANSACTION", info.LogTruncationHoldupReason);
        Assert.Equal(61, info.HolderSessionId);
        Assert.Equal("etl_login", info.HolderLoginName);
    }

    [Fact]
    public async Task ARowWithoutTheLog_ReadsNull_NeverZero_AndKeepsTheSpace()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(reservedMb: 300, unallocatedMb: 700);

        var info = await new LocalDataService(_duckDb).GetLatestTempDbSpaceAsync(TestDataSeeder.TestServerId);

        Assert.NotNull(info);
        Assert.Equal(300d, info!.TotalReservedMb, precision: 2);
        Assert.Null(info.LogUsedPercent);
        Assert.Null(info.LogTruncationHoldupReason);
        Assert.Null(info.HolderSessionId);
    }
}
