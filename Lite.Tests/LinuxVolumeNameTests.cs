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
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7704: SQL Server on Linux returns NULL for the volume name, and the volume reads dropped those rows, so a Linux
/// server got no Volume Free Space alert, no DISK_SPACE fact and a dash in the Database Sizes Volume column. Each read
/// now names such a volume by the directory of its files. A Windows server with names keeps its present answer.
/// </summary>
public sealed class LinuxVolumeNameTests : IClassFixture<SharedDuckDbFixture>
{
    private const int LinuxServer = -770_401;
    private const int WindowsServer = -770_402;
    private const string LinuxDirectory = "/var/opt/mssql/data";

    private readonly DuckDbInitializer _duckDb;

    public LinuxVolumeNameTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static int s_nextId = -770_000_000;

    private static async Task InsertAsync(DuckDBConnection connection, int serverId, DateTime at, string database, int fileId,
        string physicalName, string? volumeName, decimal volumeTotalMb, decimal volumeFreeMb)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, 'linux-volume-7704', $4, $5, 'ROWS', $6, $7, 100, false, 0, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = System.Threading.Interlocked.Decrement(ref s_nextId) });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database + "_data" + fileId });
        cmd.Parameters.Add(new DuckDBParameter { Value = physicalName });
        cmd.Parameters.Add(new DuckDBParameter { Value = volumeName ?? (object)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = volumeTotalMb });
        cmd.Parameters.Add(new DuckDBParameter { Value = volumeFreeMb });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task ALinuxServerWithNoVolumeNames_GetsItsAlertRow_Fact_AndGridName_FromTheFileDirectory()
    {
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        var at = end.AddMinutes(-5);
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            /* NULL names, files of two databases under one directory, a 1,000,000 MB volume with 30,000 MB free (97% full). */
            await InsertAsync(connection, LinuxServer, at, "master", 1, LinuxDirectory + "/master.mdf", null, 1_000_000, 30_000);
            await InsertAsync(connection, LinuxServer, at, "SalesDb", 1, LinuxDirectory + "/SalesDb.mdf", null, 1_000_000, 30_000);
            await InsertAsync(connection, LinuxServer, at, "SalesDb", 2, LinuxDirectory + "/SalesDb_log.ldf", null, 1_000_000, 30_000);
        }

        var data = new LocalDataService(_duckDb);

        /* The Volume Free Space alert read. */
        var volume = Assert.Single(await new LiteAlertReadAdapter(data).GetVolumeFreeSpaceAsync(LinuxServer.ToString()));
        Assert.Equal(LinuxDirectory, volume.MountPoint);
        Assert.Equal(1_000_000, volume.TotalMb);
        Assert.Equal(30_000, volume.FreeMb);

        /* The DISK_SPACE fact. */
        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(LinuxServer, end)))
            .ToDictionary(f => f.Key);
        var disk = facts["DISK_SPACE"];
        Assert.Equal(0.03, disk.Value, precision: 6);
        Assert.Equal(LinuxDirectory, disk.ObjectName);
        Assert.Equal(1, disk.Metadata["volume_count"]);
        Assert.Equal(30_000, disk.Metadata["min_free_mb"]);

        /* The Database Sizes grid, the server-info file list and the file growth read name the volume the same way. */
        var sizes = await data.GetDatabaseSizeLatestAsync(LinuxServer);
        Assert.Equal(3, sizes.Count);
        Assert.All(sizes, s => Assert.Equal(LinuxDirectory, s.VolumeMountPoint));

        var info = await data.GetLatestDatabaseSizeStatsAsync(LinuxServer);
        Assert.Equal(3, info.Count);
        Assert.All(info, s => Assert.Equal(LinuxDirectory, s.VolumeMountPoint));

        var growth = await data.GetDatabaseFileGrowthAsync(LinuxServer, 60);
        Assert.Equal(3, growth.Count);
        Assert.All(growth, g => Assert.Equal(LinuxDirectory, g.VolumeMountPoint));
    }

    [Fact]
    public async Task AWindowsServerWithVolumeNames_KeepsItsNamesAndOneRowPerVolume()
    {
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        var at = end.AddMinutes(-5);
        using (var readLock = _duckDb.AcquireReadLock())
        using (var connection = _duckDb.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await InsertAsync(connection, WindowsServer, at, "master", 1, "C:\\Data\\master.mdf", "C:\\", 100_000, 25_000);
            await InsertAsync(connection, WindowsServer, at, "SalesDb", 1, "D:\\Data\\SalesDb.mdf", "D:\\", 2_000_000, 150_000);
            await InsertAsync(connection, WindowsServer, at, "SalesDb", 2, "D:\\Logs\\SalesDb.ldf", "D:\\", 2_000_000, 150_000);
        }

        var volumes = await new LiteAlertReadAdapter(new LocalDataService(_duckDb)).GetVolumeFreeSpaceAsync(WindowsServer.ToString());
        Assert.Equal(new[] { "D:\\", "C:\\" }, volumes.Select(v => v.MountPoint).ToArray());
        Assert.Equal(150_000, volumes[0].FreeMb);

        var disk = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(WindowsServer, end)))
            .Single(f => f.Key == "DISK_SPACE");
        Assert.Equal(0.075, disk.Value, precision: 6);
        Assert.Equal("D:\\", disk.ObjectName);
        Assert.Equal(2, disk.Metadata["volume_count"]);
    }
}
