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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6651 (#5633 item 4): Lite's Top Procedures grid ranks without the databases the server dropped, the way its own
/// get_top_procedures_by_cpu tool does, and says what it left out. The grid reads the left-out set through
/// <see cref="LocalDataService.GetDatabasesLeftOutOfRankingAsync"/> and hands it to <see cref="LocalDataService.GetTopProceduresByCpuAsync"/>,
/// which is the path these tests run; the banner text is the pure <see cref="DatabasesLeftOut.ProcedureBanner"/>.
/// </summary>
public sealed class TopProceduresGridDroppedDatabaseTests : IDisposable
{
    private const int ServerId = 6651;
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public TopProceduresGridDroppedDatabaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    private async Task SnapshotAsync(TimeSpan age, params string[] databases)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var at = DateTime.UtcNow - age;
        foreach (var database in databases)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = $"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', 5, 'ONLINE', false)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task ProcedureAsync(TimeSpan age, string database, string objectName, long cpuUs)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        var at = DateTime.UtcNow - age;
        cmd.CommandText = $"INSERT INTO procedure_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type, last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time) VALUES ({_nextId++}, TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', {ServerId}, 'S', '{database}', 'dbo', '{objectName}', 'SQL_STORED_PROCEDURE', TIMESTAMP '{at:yyyy-MM-dd HH:mm:ss}', 10, {cpuUs}, {cpuUs})";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>The grid's two reads, as ServerTab chains them: the left-out set, then the ranking with it excluded.</summary>
    private async Task<(string[] Ranked, string[]? LeftOut)> GridReadAsync(LocalDataService service, string[]? chosen, DateTime startUtc, DateTime endUtc, int top = 50)
    {
        var leftOut = await service.GetDatabasesLeftOutOfRankingAsync(ServerId, chosen, startUtc, endUtc);
        var rows = await service.GetTopProceduresByCpuAsync(ServerId, 0, top, startUtc, endUtc, databaseNames: chosen, excludeDatabaseNames: leftOut);
        return (rows.Select(r => r.ObjectName).ToArray(), leftOut?.ToArray());
    }

    [Fact]
    public async Task AllDatabases_RanksWithoutADroppedOne_AndTopNIsNotFilledByIt()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);
        await SnapshotAsync(TimeSpan.FromHours(2), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(10), "LiveDb");
        await ProcedureAsync(TimeSpan.FromHours(1), "GoneDb", "usp_Gone", 900_000);
        await ProcedureAsync(TimeSpan.FromHours(1), "LiveDb", "usp_Live", 100_000);
        var end = DateTime.UtcNow;
        var start = end.AddHours(-24);

        var (ranked, leftOut) = await GridReadAsync(service, null, start, end);
        Assert.Equal(new[] { "usp_Live" }, ranked);
        Assert.Equal(new[] { "GoneDb" }, leftOut);

        /* The dropped database's rows would have taken the only slot. */
        var (top1, _) = await GridReadAsync(service, null, start, end, top: 1);
        Assert.Equal(new[] { "usp_Live" }, top1);
    }

    [Fact]
    public async Task ANamedDatabase_IsReadAsAsked_EvenWhenItWasDropped()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);
        await SnapshotAsync(TimeSpan.FromHours(2), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(10), "LiveDb");
        await ProcedureAsync(TimeSpan.FromHours(1), "GoneDb", "usp_Gone", 900_000);
        var end = DateTime.UtcNow;

        var (ranked, leftOut) = await GridReadAsync(service, new[] { "GoneDb" }, end.AddHours(-24), end);
        Assert.Equal(new[] { "usp_Gone" }, ranked);
        Assert.Null(leftOut);
    }

    [Fact]
    public async Task APastWindow_IsAnchoredAtItsEnd_SoADatabaseDroppedSinceStillRanksForIt()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);
        await SnapshotAsync(TimeSpan.FromHours(22), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromHours(21), "GoneDb", "LiveDb");
        await SnapshotAsync(TimeSpan.FromMinutes(10), "LiveDb");
        await ProcedureAsync(TimeSpan.FromHours(21), "GoneDb", "usp_Gone", 900_000);
        await ProcedureAsync(TimeSpan.FromHours(21), "LiveDb", "usp_Live", 100_000);
        var now = DateTime.UtcNow;

        /* A window that ended 20 hours ago: both databases existed then. */
        var (past, pastLeftOut) = await GridReadAsync(service, null, now.AddHours(-24), now.AddHours(-20));
        Assert.Equal(new[] { "usp_Gone", "usp_Live" }, past);
        Assert.Null(pastLeftOut);

        /* The same days read up to now: the database is gone as of the newest snapshot. */
        var (current, currentLeftOut) = await GridReadAsync(service, null, now.AddHours(-24), now);
        Assert.Equal(new[] { "usp_Live" }, current);
        Assert.Equal(new[] { "GoneDb" }, currentLeftOut);
    }

    [Fact]
    public async Task NoSnapshot_IsUnknown_AndLeavesNothingOut()
    {
        await _duckDb.InitializeFromTemplateAsync();
        var service = new LocalDataService(_duckDb);
        await ProcedureAsync(TimeSpan.FromHours(1), "AnyDb", "usp_Any", 100_000);
        var end = DateTime.UtcNow;

        var (ranked, leftOut) = await GridReadAsync(service, null, end.AddHours(-24), end);
        Assert.Equal(new[] { "usp_Any" }, ranked);
        Assert.Null(leftOut);
    }

    [Fact]
    public void Banner_IsAbsentWhenNothingWasLeftOut()
    {
        Assert.Null(DatabasesLeftOut.ProcedureBanner(null, 3, null, 24));
        Assert.Null(DatabasesLeftOut.ProcedureBanner(Array.Empty<string>(), 0, CollectorRunFacts.None, 24));
    }

    [Fact]
    public void Banner_WithRows_NamesTheDatabasesAndCarriesTheSharedNote()
    {
        var text = DatabasesLeftOut.ProcedureBanner(new[] { "A", "B" }, 5, null, 24);
        Assert.Equal(DatabasesLeftOut.Note(new[] { "A", "B" }) + " Left out: A, B.", text);
    }

    [Fact]
    public void Banner_OnAnEmptyPage_ClaimsOnlyTheDatabasesTheServerStillHas_AndDropsTheToolKeyPointer()
    {
        var facts = new CollectorRunFacts(5, 12, DateTime.UtcNow, null);
        var text = DatabasesLeftOut.ProcedureBanner(new[] { "GoneDb" }, 0, facts, 24);

        Assert.StartsWith("No procedure ran in the last 24h in a database this server still has", text, StringComparison.Ordinal);
        Assert.DoesNotContain("databases_left_out", text, StringComparison.Ordinal);
        Assert.EndsWith("Left out: GoneDb.", text, StringComparison.Ordinal);
    }

    [Fact]
    public void Banner_ListsAtMostTheCap_AndCountsTheRest()
    {
        var many = Enumerable.Range(0, DatabasesLeftOut.MaxNamed + 5).Select(i => "Db" + i.ToString("D3", System.Globalization.CultureInfo.InvariantCulture)).ToArray();
        var text = DatabasesLeftOut.ProcedureBanner(many, 3, null, 24);

        Assert.EndsWith(" and 5 more.", text, StringComparison.Ordinal);
        Assert.DoesNotContain(many[^1], text, StringComparison.Ordinal);
    }
}
