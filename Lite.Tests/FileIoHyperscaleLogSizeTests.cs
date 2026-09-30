/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The log file of an Azure SQL Database Hyperscale database lives in the log service, so the collector stores
/// NO size for it (a NULL <c>size_mb</c>). This proves the read side against a real DuckDB through the real
/// <see cref="LocalDataService"/> and the real <c>get_file_io_stats</c> tool: the NULL survives the read (it is not
/// mapped to 0), the row says "n/a (log service)" in place of a size, and a data file beside it keeps its size.
///
/// <para>The text comes from one constant, <see cref="FileIoStatsCollector.NoSizeLabel"/>, so Darling's payload
/// and the web table say the same thing; this class holds Lite's half of that agreement.</para>
/// </summary>
public sealed class FileIoHyperscaleLogSizeTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "HyperscaleLogSizeServer";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public FileIoHyperscaleLogSizeTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "FileIoHyperscaleLog_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);

        /* The derived id, not a literal: seeding under a hand-picked number makes every read return nothing. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task TheLatestRead_KeepsANullSizeNull_AndSaysWhyInsteadOfANumber()
    {
        var at = Truncate(DateTime.UtcNow.AddMinutes(-5));
        await SeedFileAsync(at, "AppDb_data", "ROWS", sizeMb: 112.0);
        await SeedFileAsync(at, "AppDb_log", "LOG", sizeMb: null);

        var rows = await _dataService.GetLatestFileIoStatsAsync(_serverId);

        Assert.Equal(2, rows.Count);
        var data = Assert.Single(rows, r => r.FileName == "AppDb_data");
        var log = Assert.Single(rows, r => r.FileName == "AppDb_log");

        /* The Hyperscale log file: no size, not a 0. */
        Assert.Null(log.SizeMb);
        Assert.Equal("n/a (log service)", log.SizeNote);
        Assert.Equal("n/a (log service)", log.SizeFormatted);
        Assert.Equal(FileIoStatsCollector.NoSizeLabel, log.SizeNote);

        /* The data file beside it keeps its size and has nothing to explain. */
        Assert.Equal(112.0, data.SizeMb);
        Assert.Null(data.SizeNote);
        Assert.Equal("112 MB", data.SizeFormatted);
    }

    [Fact]
    public async Task TheMcpTool_ReportsANullSizeWithTheNote_AndADataFileKeepsItsSize()
    {
        var at = Truncate(DateTime.UtcNow.AddMinutes(-5));
        await SeedFileAsync(at, "AppDb_data", "ROWS", sizeMb: 112.04);
        await SeedFileAsync(at, "AppDb_log", "LOG", sizeMb: null);

        var json = await McpIoTools.GetFileIoStats(_dataService, _serverManager, ServerName);
        var files = JsonDocument.Parse(json).RootElement.GetProperty("files").EnumerateArray().ToList();

        Assert.Equal(2, files.Count);
        var data = files.Single(f => f.GetProperty("file_name").GetString() == "AppDb_data");
        var log = files.Single(f => f.GetProperty("file_name").GetString() == "AppDb_log");

        /* null plus a short note, the same two fields Darling's payload carries. */
        Assert.Equal(JsonValueKind.Null, log.GetProperty("size_mb").ValueKind);
        Assert.Equal("n/a (log service)", log.GetProperty("size_note").GetString());

        Assert.Equal(112.0, data.GetProperty("size_mb").GetDouble());
        Assert.Equal(JsonValueKind.Null, data.GetProperty("size_note").ValueKind);
    }

    private static DateTime Truncate(DateTime utc) =>
        DateTime.SpecifyKind(new DateTime(utc.Year, utc.Month, utc.Day, utc.Hour, utc.Minute, 0), DateTimeKind.Unspecified);

    private async Task SeedFileAsync(DateTime at, string fileName, string fileType, double? sizeMb)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
     delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AppDb', $5, $6, 'F:\AppDb', $7, 10, 5, 81920, 40960, 50, 10, 60)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileType });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)sizeMb ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
