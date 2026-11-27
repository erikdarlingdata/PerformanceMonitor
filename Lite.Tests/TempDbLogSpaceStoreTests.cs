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
using PerformanceMonitor.Common;
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

    /* #8881 second step: the tempdb chart's "Log Used" series and the holder line, through the real DuckDB store and the real chart read. */

    [Fact]
    public async Task TheChartRead_CarriesLogUsedMb_AndARowStoredBeforeTheLogColumnsIsNullNotZero()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        /* Rows 0 to 7 are stored the way every row was before v83 (no log columns). Rows 8 to 15 carry a log 60 percent used of 1000 MB. */
        await seeder.SeedTempDbAsync(reservedMb: 100, unallocatedMb: 900, logUsedPercent: 60, logFromRow: 8);

        var rows = await new LocalDataService(_duckDb).GetTempDbTrendAsync(
            TestDataSeeder.TestServerId, fromDate: TestDataSeeder.TestPeriodStart.AddMinutes(-1), toDate: TestDataSeeder.TestPeriodEnd);

        Assert.Equal(16, rows.Count);
        Assert.All(rows.Take(8), r => Assert.Null(r.LogUsedMb));
        Assert.All(rows.Skip(8), r => Assert.Equal(600d, r.LogUsedMb!.Value, precision: 2));

        /* What the chart plots: the old rows are gaps (NaN), never a zero that would read as an empty log. */
        var plotted = TempDbLogSeries.Values(rows.Select(r => r.LogUsedMb).ToList());
        Assert.NotNull(plotted);
        Assert.All(plotted!.Take(8), v => Assert.True(double.IsNaN(v)));
        Assert.All(plotted.Skip(8), v => Assert.Equal(600d, v, precision: 2));
        Assert.Equal(600d, TempDbLogSeries.Max(plotted), precision: 2);
    }

    [Fact]
    public async Task TheChartRead_OfAServerWithNoLogAnywhere_DrawsNoLogSeriesAtAll()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(reservedMb: 300, unallocatedMb: 700);

        var rows = await new LocalDataService(_duckDb).GetTempDbTrendAsync(
            TestDataSeeder.TestServerId, fromDate: TestDataSeeder.TestPeriodStart.AddMinutes(-1), toDate: TestDataSeeder.TestPeriodEnd);

        Assert.Equal(16, rows.Count);
        Assert.All(rows, r => Assert.Null(r.LogUsedMb));
        Assert.Null(TempDbLogSeries.Values(rows.Select(r => r.LogUsedMb).ToList()));
    }

    [Fact]
    public async Task TheHolderLine_NamesTheOldestTransactionOfTheNewestCollection()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        await seeder.SeedTempDbAsync(
            reservedMb: 100, unallocatedMb: 900, logUsedPercent: 70, logHoldup: "ACTIVE_TRANSACTION", holderSessionId: 87, holderLogin: "app_user",
            holderProgram: "MyApp", holderOpenMinutes: 14, holderLogBytesUsed: 3435973837);

        var holder = await new LocalDataService(_duckDb).GetNewestTempDbLogHolderAsync(
            TestDataSeeder.TestServerId, fromDate: TestDataSeeder.TestPeriodStart.AddMinutes(-1), toDate: TestDataSeeder.TestPeriodEnd);

        Assert.Equal(
            "Oldest tempdb log transaction: session 87, login app_user, program MyApp, open 14 minutes, 3.2 GB of log used. Log truncation is held up by ACTIVE_TRANSACTION.",
            TempDbLogHolderLine.Build(holder));
    }

    [Fact]
    public async Task TheHolderLine_IsAbsentWhenTheNewestCollectionHasNoHolder_AndWhenTheWindowIsEmpty()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.ClearTestDataAsync();
        await seeder.SeedTestServerAsync();
        /* A log 20 percent used: the collector captures no holder below 50 percent. */
        await seeder.SeedTempDbAsync(reservedMb: 100, unallocatedMb: 900, logUsedPercent: 20, logHoldup: "NOTHING");
        var service = new LocalDataService(_duckDb);

        var noHolder = await service.GetNewestTempDbLogHolderAsync(
            TestDataSeeder.TestServerId, fromDate: TestDataSeeder.TestPeriodStart.AddMinutes(-1), toDate: TestDataSeeder.TestPeriodEnd);
        Assert.NotNull(noHolder);
        Assert.Null(TempDbLogHolderLine.Build(noHolder));

        var empty = await service.GetNewestTempDbLogHolderAsync(
            TestDataSeeder.TestServerId, fromDate: TestDataSeeder.TestPeriodEnd.AddDays(-30), toDate: TestDataSeeder.TestPeriodEnd.AddDays(-29));
        Assert.Null(empty);
        Assert.Null(TempDbLogHolderLine.Build(empty));
    }
}
