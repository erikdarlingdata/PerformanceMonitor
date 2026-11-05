/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7704: SQL Server on Linux returns NULL for the volume name, and the volume reads dropped those rows, so a Linux
/// server got no Volume Free Space alert, no DISK_SPACE fact, no Daily Health Report volume row and a dash in the
/// Database Sizes Volume column. Each read now names such a volume by the directory of its files. Rows are seeded the
/// way the collector stores a Linux server (NULL name, a volume total, a path under /var/opt/mssql/data), and a
/// Windows server with names keeps its present answer.
/// </summary>
[Collection("live-postgres")]
public sealed class LinuxVolumeNameLiveTests
{
    private const int LinuxServer = -7704, WindowsServer = -7705;
    private const int LiveTimeoutSeconds = 60;
    private const string LinuxDirectory = "/var/opt/mssql/data";

    private static async Task<NpgsqlConnection?> OpenAsync(string? connectionString, CancellationToken ct)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live Linux volume test.");
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelsAsync(connection, ct);
        return connection;
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            string.Create(CultureInfo.InvariantCulture, $"DELETE FROM database_size_stats WHERE server_id IN ({LinuxServer}, {WindowsServer});"), connection)
        { CommandTimeout = LiveTimeoutSeconds };
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, int serverId, DateTime at, string database, int fileId,
        string physicalName, string? volumeName, decimal volumeTotalMb, decimal volumeFreeMb, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, 'linux-volume-7704', $4, $5, 'ROWS', $6, $7, 100, false, 0, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = at });
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(fileId);
        command.Parameters.AddWithValue(database + "_data" + fileId.ToString(CultureInfo.InvariantCulture));
        command.Parameters.AddWithValue(physicalName);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)volumeName ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Varchar });
        command.Parameters.AddWithValue(volumeTotalMb);
        command.Parameters.AddWithValue(volumeFreeMb);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A Linux server: NULL names, two databases' files under one directory, a 1,000,000 MB volume with 30,000 MB free (97% full).</summary>
    private static async Task SeedLinuxAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        await InsertAsync(connection, LinuxServer, at, "master", 1, LinuxDirectory + "/master.mdf", null, 1_000_000, 30_000, ct);
        await InsertAsync(connection, LinuxServer, at, "SalesDb", 1, LinuxDirectory + "/SalesDb.mdf", null, 1_000_000, 30_000, ct);
        await InsertAsync(connection, LinuxServer, at, "SalesDb", 2, LinuxDirectory + "/SalesDb_log.ldf", null, 1_000_000, 30_000, ct);
    }

    private static string Key(int serverId) => serverId.ToString(CultureInfo.InvariantCulture);

    private static DateTime Now() => new(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond, DateTimeKind.Utc);

    [Fact]
    public async Task ALinuxServerWithNoVolumeNames_GetsItsAlertRow_Fact_ReportRow_AndGridName_FromTheFileDirectory()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = Now();
            var at = now.AddMinutes(-5);
            await SeedLinuxAsync(connection, at, ct);

            /* The Volume Free Space alert read: one volume, named by the directory, with the tight figure. */
            var adapter = new DarlingAlertReadAdapter(dataSource);
            var volume = Assert.Single(await adapter.GetVolumeFreeSpaceAsync(Key(LinuxServer), ct));
            Assert.Equal(LinuxDirectory, volume.MountPoint);
            Assert.Equal(1_000_000, volume.TotalMb);
            Assert.Equal(30_000, volume.FreeMb);

            /* The DISK_SPACE fact. */
            var context = new AnalysisContext
            {
                ServerId = LinuxServer,
                ServerName = "linux-volume-7704",
                TimeRangeStart = now.AddHours(-4),
                TimeRangeEnd = now,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var facts = (await new PgFactCollector(dataSource).CollectFactsAsync(context)).ToDictionary(f => f.Key);
            var disk = facts["DISK_SPACE"];
            Assert.Equal(0.03, disk.Value, precision: 6);
            Assert.Equal(LinuxDirectory, disk.ObjectName);
            Assert.Equal(1, disk.Metadata["volume_count"]);
            Assert.Equal(30_000, disk.Metadata["min_free_mb"]);

            /* The Daily Health Report volume read. */
            await using (var report = new NpgsqlCommand(DailyHealthReportBuilder.PeriodVolumesSql, connection))
            {
                report.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = now.AddHours(-1) });
                report.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = now });
                await using var reader = await report.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(LinuxServer, reader.GetInt32(0));
                Assert.Equal(LinuxDirectory, reader.GetString(1));
                Assert.False(await reader.ReadAsync(ct));
            }

            /* The Database Sizes grid names every file's volume the same way. */
            var files = await DarlingFinOpsDatabaseSizesReader.GetLatestAsync(dataSource, LinuxServer, LiveTimeoutSeconds, ct);
            Assert.Equal(3, files.Count);
            Assert.All(files, f => Assert.Equal(LinuxDirectory, f.VolumeMountPoint));

            /* The file growth read carries the same name. */
            await using (var growth = new NpgsqlCommand(DarlingAlertReadAdapter.DatabaseFileGrowthSql, connection))
            {
                growth.Parameters.AddWithValue(LinuxServer);
                growth.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = now.AddHours(-1) });
                await using var reader = await growth.ExecuteReaderAsync(ct);
                var names = new List<string?>();
                while (await reader.ReadAsync(ct)) names.Add(reader.IsDBNull(7) ? null : reader.GetString(7));
                Assert.Equal(3, names.Count);
                Assert.All(names, n => Assert.Equal(LinuxDirectory, n));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }

    [Fact]
    public async Task AWindowsServerWithVolumeNames_KeepsItsNamesAndOneRowPerVolume()
    {
        var ct = TestContext.Current.CancellationToken;
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        using var connection = (await OpenAsync(connectionString, ct))!;
        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var now = Now();
            var at = now.AddMinutes(-5);
            await InsertAsync(connection, WindowsServer, at, "master", 1, "C:\\Data\\master.mdf", "C:\\", 100_000, 25_000, ct);
            await InsertAsync(connection, WindowsServer, at, "SalesDb", 1, "D:\\Data\\SalesDb.mdf", "D:\\", 2_000_000, 150_000, ct);
            await InsertAsync(connection, WindowsServer, at, "SalesDb", 2, "D:\\Logs\\SalesDb.ldf", "D:\\", 2_000_000, 150_000, ct);

            var adapter = new DarlingAlertReadAdapter(dataSource);
            var volumes = await adapter.GetVolumeFreeSpaceAsync(Key(WindowsServer), ct);
            Assert.Equal(new[] { "D:\\", "C:\\" }, volumes.Select(v => v.MountPoint).ToArray());
            Assert.Equal(150_000, volumes[0].FreeMb);

            var context = new AnalysisContext
            {
                ServerId = WindowsServer,
                ServerName = "linux-volume-7704",
                TimeRangeStart = now.AddHours(-4),
                TimeRangeEnd = now,
                ServerUtcOffset = TimeSpan.Zero,
            };
            var disk = (await new PgFactCollector(dataSource).CollectFactsAsync(context)).Single(f => f.Key == "DISK_SPACE");
            Assert.Equal(0.075, disk.Value, precision: 6);
            Assert.Equal("D:\\", disk.ObjectName);
            Assert.Equal(2, disk.Metadata["volume_count"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, (c, token) => DeleteSentinelsAsync(c, token));
        }
    }
}
