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
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round trip for Lite's file-growth read (#2349), replaying the #3636 shape: one file whose
/// <c>database_size_stats</c> row went 100 GB → 120 GB across two hourly collections. The read must return the
/// growth as the difference and the window as the measured span (the pre-#3636 contract), and must carry the
/// NEWEST sample's <c>collection_time</c> as the observation stamp — the fact the engine keys its
/// once-per-observation guard on, so that the same two rows re-read on every alert pass for the rest of the hour
/// produce one card rather than up to twelve.
///
/// <para>Two things the text pin cannot prove and this does: that DuckDB's <c>TIMESTAMP</c> comes back through
/// <c>GetDateTime(14)</c> at the ordinal the reader binds, and that the value round-trips to the tick — the engine
/// compares one file's stamps for equality, so a stamp that came back shifted or truncated would either never
/// match (cooldown-repeat returns) or match a different collection. <c>ForcePlanFailuresReadTests</c> is the
/// #3579 twin of this file.</para>
/// </summary>
public sealed class FileGrowthReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 3636;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public FileGrowthReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /* Whole minutes by construction: DuckDB TIMESTAMP is microsecond-resolution, so raw DateTime ticks would
       not survive the round trip and the tick-equality below would be testing the wrong thing. Kind
       Unspecified, the store's naive-UTC convention. Inside a 120-minute lookback. */
    private static readonly DateTime Collection0 = MinuteFloor(DateTime.UtcNow.AddMinutes(-70));
    private static readonly DateTime Collection1 = Collection0.AddMinutes(60);

    private static DateTime MinuteFloor(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedFileAsync(DateTime collectionTime, string fileName, string fileType, double totalSizeMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name,
     database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb,
     volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, $4, 'tempdb', 2, $5, $6, $7, $8, $9, $10, 'D:\', 4096000, 3000000)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "GrowthSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileName == "tempdev" ? 1 : 2 });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileType });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = @"D:\data\" + fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalSizeMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalSizeMb * 0.8 });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TheRise_IsStampedWithTheNewestCollection_ToTheTick_AndTheStampMovesWithTheNextOne()
    {
        var service = new LocalDataService(_duckDb);

        /* Top of the hour: 100 GB. An hour later: 120 GB. templog has one sample only. */
        await SeedFileAsync(Collection0, "tempdev", "ROWS", 102_400);
        await SeedFileAsync(Collection1, "tempdev", "ROWS", 122_880);
        await SeedFileAsync(Collection1, "templog", "LOG", 4_096);

        var files = await service.GetDatabaseFileGrowthAsync(ServerId, lookbackMinutes: 120);
        Assert.Equal(2, files.Count);

        var tempdev = Assert.Single(files, f => f.FileName == "tempdev");
        Assert.Equal(122_880d, tempdev.TotalSizeMb, precision: 3);
        Assert.Equal(20_480d, tempdev.GrowthMb, precision: 3);
        Assert.Equal(60d, tempdev.GrowthWindowMinutes, precision: 3);
        /* #3636: the observation's identity is the newest sample's collection_time, to the tick, Kind Utc. */
        Assert.Equal((DateTime?)Collection1, tempdev.ObservedAtUtc);
        Assert.Equal(DateTimeKind.Utc, tempdev.ObservedAtUtc!.Value.Kind);

        /* The single-sample neighbour: no rise observed (not "the whole file appeared"), and a stamp all the same. */
        var templog = Assert.Single(files, f => f.FileName == "templog");
        Assert.Equal(0d, templog.GrowthMb, precision: 3);
        Assert.Equal((DateTime?)Collection1, templog.ObservedAtUtc);

        /* Re-reading between collections is the same observation: same row, same stamp. This is the read the
           engine used to fire on twelve times; the stamp is what lets it recognise the repeat. */
        var reread = await service.GetDatabaseFileGrowthAsync(ServerId, lookbackMinutes: 120);
        Assert.Equal(tempdev.ObservedAtUtc, Assert.Single(reread, f => f.FileName == "tempdev").ObservedAtUtc);

        /* The next collection lands: the stamp moves with it. */
        var collection2 = Collection1.AddMinutes(8);
        await SeedFileAsync(collection2, "tempdev", "ROWS", 122_880);
        var after = await service.GetDatabaseFileGrowthAsync(ServerId, lookbackMinutes: 120);
        Assert.Equal((DateTime?)collection2, Assert.Single(after, f => f.FileName == "tempdev").ObservedAtUtc);
    }
}
