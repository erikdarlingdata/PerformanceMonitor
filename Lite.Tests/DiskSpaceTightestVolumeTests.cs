/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7179: the DISK_SPACE fact scores the volume with the lowest percent free, and its card must quote the free MB
/// of that same volume. The fact used to take the smallest percent and the smallest MB as two separate minimums, so
/// a small healthy volume could supply the MB figure for a large tight one.
/// </summary>
public sealed class DiskSpaceTightestVolumeTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -717_901;

    private readonly DuckDbInitializer _duckDb;

    public DiskSpaceTightestVolumeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task TheCard_QuotesTheFreeMbOfTheVolumeThatScored_NotTheSmallestFigureOnAnotherVolume()
    {
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            /* C: 100 GB with 25 GB free (25%). D: 2 TB with 150 GB free (7.5%): D: scores, C: has the smaller MB. */
            await LatestValueSeed.InsertSizeAsync(connection, ServerId, end.AddMinutes(-5), "master", 1, "ROWS",
                "master_data", 100, false, "C:\\", 100_000, 25_000);
            await LatestValueSeed.InsertSizeAsync(connection, ServerId, end.AddMinutes(-5), "SalesDb", 1, "ROWS",
                "SalesDb_data", 100_000, false, "D:\\", 2_000_000, 150_000);
        }

        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(ServerId, end)))
            .ToDictionary(f => f.Key);

        var disk = facts["DISK_SPACE"];
        Assert.Equal(0.075, disk.Value, precision: 6);
        Assert.Equal(150_000, disk.Metadata["min_free_mb"]);
        Assert.Equal("D:\\", disk.ObjectName);

        var card = FactAdvice.Compose("DISK_SPACE", facts);
        Assert.NotNull(card);
        Assert.Contains("(D:\\) is down to 150,000 MB free", card!.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("25,000", card.Investigation, StringComparison.Ordinal);
        Assert.Contains("150,000 MB", card.Headline, StringComparison.Ordinal);
    }
}
