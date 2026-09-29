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
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: Lite reads the server's clock from the newest <c>server_properties</c> row that carries an offset,
/// with the <c>time_zone_id</c> of that same row, and reads it again each time a tab refreshes.
/// </summary>
public sealed class ServerClockReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 8801;
    private const int OtherServerId = 8802;
    private const string EasternZone = "Eastern Standard Time";

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public ServerClockReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, -300);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task SeedServerPropertiesAsync(int serverId, DateTime collectionTime, int? offset, string? zoneId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, 'TestSrv', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, $5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = -_nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)offset ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)zoneId ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    // ── The clock read ──

    [Fact]
    public async Task GetServerClockAsync_ResolvesTheNewestRowsZone_SoBothSidesOfTheChangeAreRight()
    {
        var service = new LocalDataService(_duckDb);

        /* An older snapshot from before the engine reported a zone (offset only), then the newest one with both. */
        await SeedServerPropertiesAsync(ServerId, At(2026, 1, 10, 0, 0), -300, null);
        await SeedServerPropertiesAsync(ServerId, At(2026, 2, 10, 0, 0), -300, EasternZone);

        var clock = await service.GetServerClockAsync(ServerId);

        Assert.NotNull(clock);
        Assert.Equal(-300, clock!.OffsetMinutesAt(At(2026, 3, 1, 12, 0)));   /* standard time */
        Assert.Equal(-240, clock.OffsetMinutesAt(At(2026, 7, 1, 12, 0)));    /* daylight time */
    }

    [Fact]
    public async Task GetServerClockAsync_ServerWithNoZoneId_KeepsItsFixedOffset()
    {
        var service = new LocalDataService(_duckDb);
        await SeedServerPropertiesAsync(ServerId, At(2026, 2, 10, 0, 0), -300, null);

        var clock = await service.GetServerClockAsync(ServerId);

        Assert.NotNull(clock);
        Assert.Equal(-300, clock!.OffsetMinutesAt(At(2026, 3, 1, 12, 0)));
        Assert.Equal(-300, clock.OffsetMinutesAt(At(2026, 7, 1, 12, 0)));
    }

    [Fact]
    public async Task GetServerClockAsync_NothingCollectedYet_ReturnsNull_SoTheCallerKeepsItsClock()
    {
        var service = new LocalDataService(_duckDb);
        await SeedServerPropertiesAsync(OtherServerId, At(2026, 2, 10, 0, 0), -300, EasternZone);

        /* Another server's row does not count, and a row with a NULL offset is skipped, not read as UTC. */
        Assert.Null(await service.GetServerClockAsync(ServerId));
        await SeedServerPropertiesAsync(ServerId, At(2026, 2, 11, 0, 0), null, null);
        Assert.Null(await service.GetServerClockAsync(ServerId));
    }

    [Fact]
    public async Task GetServerClockAsync_ReadsAgain_AndPicksUpAZoneReportedLater()
    {
        var service = new LocalDataService(_duckDb);
        await SeedServerPropertiesAsync(ServerId, At(2026, 2, 10, 0, 0), -300, null);
        Assert.Equal(-300, (await service.GetServerClockAsync(ServerId))!.OffsetMinutesAt(At(2026, 7, 1, 12, 0)));

        /* The engine is upgraded to a version that reports a zone id: the next read sees daylight time in July. */
        await SeedServerPropertiesAsync(ServerId, At(2026, 2, 11, 0, 0), -300, EasternZone);
        Assert.Equal(-240, (await service.GetServerClockAsync(ServerId))!.OffsetMinutesAt(At(2026, 7, 1, 12, 0)));
    }
}
