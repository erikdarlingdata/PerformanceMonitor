/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4231: Lite's twin of Darling's #2364. <c>query_stats</c>, <c>procedure_stats</c> and
/// <c>query_store_stats</c> are raw-only (no rollup fallback), and Lite's default 30-day
/// <c>retention_days</c> is per-collector and user-settable — lower it, or run a young install, and a
/// "Last 7 days" ask can be served from far less. Pins the shared floor helper
/// (<see cref="LocalDataService.GetQueryWindowFloorAsync"/>) and the three MCP tools' disclosure.
/// Own <see cref="DuckDbInitializer"/> per test (not <c>SharedDuckDbFixture</c>) because the archive
/// tests need control of the database's archive directory, to COPY hot rows out to parquet exactly like
/// <c>ArchiveViewDedupTests</c> does.
///
/// <para>The banner tests hand <c>ServerTab.SetWindowTruncatedBanner</c> the zone to word the time in, and one of
/// them sets the active server's clock and the display mode, two settings shared by the whole test process, to
/// something else to show the text does not read them (#4766). This class joins the <c>server-time-helper</c>
/// collection so a class that changes either setting cannot run in the middle of it (#4776), and restores both in
/// <c>Dispose</c>.</para>
/// </summary>
[Collection("server-time-helper")]
public sealed class QueryWindowTruncationTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly int ServerId;
    private readonly string _tempDir;
    private readonly string _archivePath;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private long _nextId = 1;

    public QueryWindowTruncationTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QueryWindowTrunc_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(_tempDir, "config"));
        _archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archivePath);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));

        _serverManager = new ServerManager(Path.Combine(_tempDir, "config"));
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        ServerId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    private async Task<DuckDBConnection> OpenSeedConnectionAsync()
    {
        var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        return connection;
    }

    private async Task SeedQueryStatsAsync(DuckDBConnection connection, DateTime collected, string queryHash, int? serverId = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_hash, sql_handle, last_execution_time, delta_execution_count,
     delta_worker_time, delta_elapsed_time, query_text)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId ?? ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xH" + queryHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT " + queryHash });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedProcedureStatsAsync(DuckDBConnection connection, DateTime collected, string objectName)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     schema_name, object_name, object_type, last_execution_time,
     delta_execution_count, delta_worker_time, delta_elapsed_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "dbo" });
        cmd.Parameters.Add(new DuckDBParameter { Value = objectName });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SQL_STORED_PROCEDURE" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedQueryStoreStatsAsync(DuckDBConnection connection, DateTime collected, long queryId)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     module_name, query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     query_plan_hash, is_forced_plan, force_failure_count,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestServer" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestDb" });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = queryId * 10 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Regular" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Adhoc" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "SELECT " + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xQ" + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 5_000L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = "0xP" + queryId });
        cmd.Parameters.Add(new DuckDBParameter { Value = false });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0L });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task GetTopQueriesByCpu_ReportsTruncation_WhenRawStartsAfterTheWindow()
    {
        await _duckDb.InitializeAsync();
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedQueryStatsAsync(connection, collected, "0xTRUNC");

        var json = await McpQueryTools.GetTopQueriesByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = ParseEffectiveStart(root);
        Assert.True(Math.Abs((effectiveStart - collected).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should track the seeded floor {collected:o}");
        Assert.InRange(root.GetProperty("effective_hours_back").GetDouble(), 46, 50);
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    [Fact]
    public async Task GetTopProceduresByCpu_ReportsTruncation_WhenRawStartsAfterTheWindow()
    {
        await _duckDb.InitializeAsync();
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedProcedureStatsAsync(connection, collected, "usp_Trunc");

        var json = await McpQueryTools.GetTopProceduresByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = ParseEffectiveStart(root);
        Assert.True(Math.Abs((effectiveStart - collected).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should track the seeded floor {collected:o}");
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    [Fact]
    public async Task GetQueryStoreTop_ReportsTruncation_WhenRawStartsAfterTheWindow()
    {
        await _duckDb.InitializeAsync();
        var collected = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-2), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedQueryStoreStatsAsync(connection, collected, 900001);

        var json = await McpQueryTools.GetQueryStoreTop(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 168);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.True(root.GetProperty("window_truncated").GetBoolean());
        var effectiveStart = ParseEffectiveStart(root);
        Assert.True(Math.Abs((effectiveStart - collected).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} should track the seeded floor {collected:o}");
        Assert.NotEqual(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    /// <summary>
    /// #4231 ruling: "a floor inside the slack shows no note." The seeded floor sits 60 minutes after the
    /// requested start — inside McpQueryTools.TruncationSlack's 90-minute allowance — so this is a normal raw
    /// series opening a collection cadence or two late, not a retention cut.
    /// </summary>
    [Fact]
    public async Task GetTopQueriesByCpu_NoNote_WhenFloorIsInsideTheSlack()
    {
        await _duckDb.InitializeAsync();
        var requestedStart = DateTime.UtcNow.AddHours(-24);
        var collected = DateTime.SpecifyKind(requestedStart.AddMinutes(60), DateTimeKind.Unspecified);
        using (var connection = await OpenSeedConnectionAsync())
            await SeedQueryStatsAsync(connection, collected, "0xINSLACK");

        var json = await McpQueryTools.GetTopQueriesByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 24);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
    }

    /// <summary>
    /// Ruling 1: the floor "comes from that view" when the grid/tool reads a view over the hot table and the
    /// archived parquet files — never just the hot table. Archives an OLDER set to parquet (mirroring
    /// ArchiveViewDedupTests' staging), keeps a NEWER set in the hot table, and confirms the probe returns the
    /// archived (older) floor. Also times the probe, per the #4231 ruling to measure it on a store that has
    /// archived files.
    /// </summary>
    [Fact]
    public async Task FloorHelper_ReadsTheArchivedFloor_NotJustTheHotTable()
    {
        await _duckDb.InitializeAsync();
        var archivedFloor = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-6), DateTimeKind.Unspecified);
        var hotStart = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-12), DateTimeKind.Unspecified);

        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction per 500-row loop (#5208): 1,000 auto-committed single-row INSERTs were 1,000 WAL
               commits. Each batch commits before the COPY / DELETE that follows, which see only committed rows. */
            using (var archivedBatch = new SeedBatch(_duckDb, connection))
            {
                for (var i = 0; i < 500; i++)
                    await SeedQueryStatsAsync(connection, archivedFloor.AddMinutes(i), $"0xARCH{i}");
                archivedBatch.Commit();
            }

            var parquetPath = Path.Combine(_archivePath, "20260101_0000_query_stats.parquet").Replace("\\", "/");
            using (var readLock = _duckDb.AcquireReadLock())
            using (var copyCmd = connection.CreateCommand())
            {
                copyCmd.CommandText = $"COPY query_stats TO '{parquetPath}' (FORMAT PARQUET)";
                await copyCmd.ExecuteNonQueryAsync();
            }
            using (var readLock = _duckDb.AcquireReadLock())
            using (var deleteCmd = connection.CreateCommand())
            {
                deleteCmd.CommandText = "DELETE FROM query_stats";
                await deleteCmd.ExecuteNonQueryAsync();
            }

            using (var hotBatch = new SeedBatch(_duckDb, connection))
            {
                for (var i = 0; i < 500; i++)
                    await SeedQueryStatsAsync(connection, hotStart.AddMinutes(i), $"0xHOT{i}");
                hotBatch.Commit();
            }
        }

        await _duckDb.CreateArchiveViewsAsync();

        var service = new LocalDataService(_duckDb);
        var requestedStart = DateTime.UtcNow.AddDays(-7);
        var windowEnd = DateTime.UtcNow;

        var stopwatch = Stopwatch.StartNew();
        var floor = await service.GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);
        stopwatch.Stop();
        Console.WriteLine($"#4231 GetQueryWindowFloorAsync (500 hot + 500 archived rows): {stopwatch.ElapsedMilliseconds} ms");

        Assert.NotNull(floor);
        Assert.True(Math.Abs((floor!.Value - archivedFloor).TotalMinutes) < 2,
            $"floor {floor:o} should be the ARCHIVED start {archivedFloor:o}, not the hot table's {hotStart:o}");
    }

    private static DateTime NaiveUtc(DateTime instant) => DateTime.SpecifyKind(instant, DateTimeKind.Unspecified);

    /// <summary>
    /// #4966: a Queries tool's effective_start as a UTC instant, asserting its trailing Z on the way. A cut window (the
    /// floor the store held) and a covered one (the requested start) both name it with the Z, so the seeded naive-UTC
    /// floors compare against it directly and the answer does not depend on the machine's time zone. These tests used
    /// to parse the text as local time and shift the seeded floor the same way, which only agreed because the
    /// payload carried no Z on the cut path.
    /// </summary>
    private static DateTime ParseEffectiveStart(JsonElement root)
    {
        var text = root.GetProperty("effective_start").GetString()!;
        Assert.EndsWith("Z", text, StringComparison.Ordinal);
        return DateTime.Parse(
            text,
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.AdjustToUniversal | System.Globalization.DateTimeStyles.AssumeUniversal);
    }

    /// <summary>
    /// Quiet-start guard (Lite twin of #4953's data-start rule): a server with an old row before the window and
    /// then a quiet stretch at the window's start (nothing collected until two hours ago) is not a window the
    /// store failed to hold, so it must get no banner. A server that holds a row before the window has had the
    /// window served whole, so the probe answers the REQUESTED START: not the first row inside the window (a probe
    /// bounded below at the window's start reads the row from two hours ago and raises a false banner), and not
    /// the oldest row the server holds either (finding that reads every archived row for the server, and no
    /// caller needs it: <c>IsWindowTruncated</c> and <c>EffectiveWindowStart</c> give the same answer for any
    /// floor at or before the start).
    /// </summary>
    [Fact]
    public async Task FloorHelper_QuietStartInsideTheWindow_WithOlderRowsBeforeIt_IsNotTruncated()
    {
        await _duckDb.InitializeAsync();
        var windowEnd = DateTime.UtcNow;
        var requestedStart = windowEnd.AddDays(-7);
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            await SeedQueryStatsAsync(connection, NaiveUtc(requestedStart.AddDays(-20)), "0xOLD");
            await SeedQueryStatsAsync(connection, NaiveUtc(windowEnd.AddHours(-2)), "0xRECENT");
        }

        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);

        Assert.NotNull(floor);
        Assert.True(floor!.Value == requestedStart,
            $"floor {floor:o} must be the requested start {requestedStart:o} (the server holds a row before it), not the window's first row");
        Assert.Equal(requestedStart, McpQueryTools.EffectiveWindowStart(floor, requestedStart));
        Assert.False(McpQueryTools.IsWindowTruncated(floor, requestedStart),
            "a quiet start inside the window, with older rows in the store, must not raise the data-start banner");
    }

    /// <summary>
    /// A server whose rows all end before the window: the window holds nothing, so the probe says NULL ("nothing
    /// was read"), never an old row or the requested start, either of which would read as the whole window being
    /// served. The older-row check runs only after the window's own first row is found, so it never answers for
    /// a window with no row in it.
    /// </summary>
    [Fact]
    public async Task FloorHelper_NoRowInsideTheWindow_ReturnsNull_EvenWhenOlderRowsExist()
    {
        await _duckDb.InitializeAsync();
        var windowEnd = DateTime.UtcNow;
        var requestedStart = windowEnd.AddDays(-7);
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            await SeedQueryStatsAsync(connection, NaiveUtc(requestedStart.AddDays(-20)), "0xOLD1");
            await SeedQueryStatsAsync(connection, NaiveUtc(requestedStart.AddDays(-10)), "0xOLD2");
        }

        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);

        Assert.Null(floor);
    }

    /// <summary>A range that starts before the oldest stored row (retention, or a server added recently) is the case the banner exists for.</summary>
    [Fact]
    public async Task FloorHelper_RangeStartsBeforeTheOldestStoredRow_IsTruncated_AtTheOldestRow()
    {
        await _duckDb.InitializeAsync();
        var windowEnd = DateTime.UtcNow;
        var requestedStart = windowEnd.AddDays(-7);
        var oldest = windowEnd.AddDays(-2);
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            await SeedQueryStatsAsync(connection, NaiveUtc(oldest), "0xFIRST");
            await SeedQueryStatsAsync(connection, NaiveUtc(windowEnd.AddHours(-1)), "0xLATER");
        }

        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);

        Assert.NotNull(floor);
        Assert.True(Math.Abs((floor!.Value - oldest).TotalMinutes) < 2, $"floor {floor:o} should be the oldest row {oldest:o}");
        Assert.True(McpQueryTools.IsWindowTruncated(floor, requestedStart));
    }

    /// <summary>
    /// A window the store fully covers (the server holds a row before the window's start) gets no banner, and the
    /// probe answers the requested start.
    /// </summary>
    [Fact]
    public async Task FloorHelper_WindowInsideTheStoredRows_IsNotTruncated_AndGivesTheStart()
    {
        await _duckDb.InitializeAsync();
        var windowEnd = DateTime.UtcNow;
        var requestedStart = windowEnd.AddDays(-7);
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            for (var day = 10; day >= 0; day--)
                await SeedQueryStatsAsync(connection, NaiveUtc(windowEnd.AddDays(-day).AddMinutes(-5)), $"0xDAY{day}");
        }

        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);

        Assert.NotNull(floor);
        Assert.True(floor!.Value == requestedStart, $"floor {floor:o} must be the requested start {requestedStart:o}");
        Assert.False(McpQueryTools.IsWindowTruncated(floor, requestedStart));
    }

    /// <summary>
    /// A row exactly at the window's start is inside the window (the bound is inclusive): the probe answers that
    /// start, and the window is not truncated. Pins the boundary of the first, bounded step. The start is a whole
    /// second so DuckDB's microsecond precision cannot move the row off it.
    /// </summary>
    [Fact]
    public async Task FloorHelper_RowExactlyAtTheWindowStart_GivesTheStart_NotTruncated()
    {
        await _duckDb.InitializeAsync();
        var now = DateTime.UtcNow;
        var windowEnd = new DateTime(now.Ticks - now.Ticks % TimeSpan.TicksPerSecond, DateTimeKind.Utc);
        var requestedStart = windowEnd.AddDays(-7);
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            await SeedQueryStatsAsync(connection, NaiveUtc(requestedStart), "0xATSTART");
            await SeedQueryStatsAsync(connection, NaiveUtc(windowEnd.AddHours(-1)), "0xLATER");
        }

        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);

        Assert.NotNull(floor);
        Assert.True(floor!.Value == requestedStart, $"floor {floor:o} must be the row at the window's start {requestedStart:o}");
        Assert.False(McpQueryTools.IsWindowTruncated(floor, requestedStart));
    }

    /// <summary>
    /// Another server's older rows are not this server's coverage: the older-row step is filtered on the server,
    /// so a server whose rows begin late in the window still reports that first row and is truncated, however
    /// much older history a neighbour holds.
    /// </summary>
    [Fact]
    public async Task FloorHelper_OnlyAnotherServerHoldsOlderRows_ThisServerIsTruncated_AtItsFirstRow()
    {
        await _duckDb.InitializeAsync();
        var windowEnd = DateTime.UtcNow;
        var requestedStart = windowEnd.AddDays(-7);
        var firstRow = windowEnd.AddDays(-2);
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            await SeedQueryStatsAsync(connection, NaiveUtc(requestedStart.AddDays(-20)), "0xNEIGHBOUR", serverId: ServerId + 1);
            await SeedQueryStatsAsync(connection, NaiveUtc(firstRow), "0xFIRST");
            await SeedQueryStatsAsync(connection, NaiveUtc(windowEnd.AddHours(-1)), "0xLATER");
        }

        var floor = await new LocalDataService(_duckDb).GetQueryWindowFloorAsync(QueryWindowRelation.QueryStats, ServerId, requestedStart, windowEnd);

        Assert.NotNull(floor);
        Assert.True(Math.Abs((floor!.Value - firstRow).TotalMinutes) < 2, $"floor {floor:o} should be this server's first row {firstRow:o}, not the start");
        Assert.True(McpQueryTools.IsWindowTruncated(floor, requestedStart));
    }

    /// <summary>
    /// The tool's twin of the quiet-start guard, and the clamp: with older rows before the window the probe answers
    /// the requested start, and the tool must report the window it was asked for (effective_start never earlier
    /// than the requested start, effective_hours_back never longer than hours_back), not the whole stored
    /// history.
    /// </summary>
    [Fact]
    public async Task GetTopQueriesByCpu_OlderRowsBeforeTheWindow_ReportsTheWholeWindow_NotTruncated()
    {
        await _duckDb.InitializeAsync();
        var nowUtc = DateTime.UtcNow;
        using (var connection = await OpenSeedConnectionAsync())
        {
            /* One transaction for this block's rows (#5208), committed when the block ends, before the read. */
            using var seedBatch = new SeedBatch(_duckDb, connection);
            await SeedQueryStatsAsync(connection, NaiveUtc(nowUtc.AddDays(-20)), "0xOLDER");
            await SeedQueryStatsAsync(connection, NaiveUtc(nowUtc.AddHours(-2)), "0xQUIETSTART");
        }

        var json = await McpQueryTools.GetTopQueriesByCpu(new LocalDataService(_duckDb), _serverManager, "TestServer", hours_back: 24);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.False(root.GetProperty("window_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("truncation_note").ValueKind);
        var effectiveStart = ParseEffectiveStart(root);
        Assert.True(Math.Abs((effectiveStart - nowUtc.AddHours(-24)).TotalMinutes) < 2,
            $"effective_start {effectiveStart:o} must be the requested start, never earlier ({nowUtc.AddHours(-24):o})");
        Assert.InRange(root.GetProperty("effective_hours_back").GetDouble(), 23.9, 24.0);
    }

    /// <summary>
    /// #4231 ruling: "the WPF Top Queries, Top Procedures and Query Store grids show 'Showing since &lt;time&gt;'
    /// in the header when the window is cut short." Pins <see cref="ServerTab.SetWindowTruncatedBanner"/> --
    /// the same text/visibility plumbing every one of the three grids' refresh paths and their matching
    /// OnXSlicerChanged handler call -- directly, rather than through ServerTab's full UI (no InitializeComponent,
    /// no server connection needed to pin the banner text). WPF objects need an STA thread to construct even
    /// off-screen; same shape as MainWindowAccessKeyTests/ThemeColorOverrideTests' OnStaThread.
    /// </summary>
    [Fact]
    public void SetWindowTruncatedBanner_Truncated_ShowsSinceEffectiveStart()
    {
        var effectiveStart = new DateTime(2026, 1, 15, 8, 30, 0, DateTimeKind.Unspecified);

        var (visibility, text) = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.SetWindowTruncatedBanner(banner, truncated: true, effectiveStart, TimeZoneInfo.Utc);
            return (banner.Visibility, banner.Text);
        });

        Assert.Equal(System.Windows.Visibility.Visible, visibility);
        Assert.Equal("Showing since 2026-01-15 08:30:00", text);
    }

    /// <summary>
    /// #4766: the zone the banner is given decides its text, not the active server's clock. Another server nine hours
    /// ahead is the active one in Server mode, and the same instant still reads 08:30 in a UTC zone and 03:30 in a
    /// US Eastern zone. Through the autumn repeated hour, 05:30Z and 06:30Z both read 01:30 in Eastern and the
    /// offset tells them apart.
    /// </summary>
    [Fact]
    public void SetWindowTruncatedBanner_WordsTheInstantInTheZoneItIsGiven_NotTheActiveClock()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.Resolve(null, 540);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var eastern = ServerClock.Resolve("Eastern Standard Time", -300).AsTimeZone();

        string BannerText(DateTime instant, TimeZoneInfo zone) => OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            ServerTab.SetWindowTruncatedBanner(banner, truncated: true, instant, zone);
            return banner.Text;
        });

        var winter = new DateTime(2026, 1, 15, 8, 30, 0, DateTimeKind.Unspecified);
        Assert.Equal("Showing since 2026-01-15 08:30:00", BannerText(winter, TimeZoneInfo.Utc));
        Assert.Equal("Showing since 2026-01-15 03:30:00", BannerText(winter, eastern));

        Assert.Equal(
            "Showing since 2026-11-01 01:30:00 -04:00",
            BannerText(new DateTime(2026, 11, 1, 5, 30, 0, DateTimeKind.Unspecified), eastern));
        Assert.Equal(
            "Showing since 2026-11-01 01:30:00 -05:00",
            BannerText(new DateTime(2026, 11, 1, 6, 30, 0, DateTimeKind.Unspecified), eastern));
    }

    /// <summary>#4231: a floor inside the slack (or no truncation at all) must hide the banner and clear stale text.</summary>
    [Fact]
    public void SetWindowTruncatedBanner_NotTruncated_HidesBanner()
    {
        var (visibility, text) = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock
            {
                Visibility = System.Windows.Visibility.Visible,
                Text = "Showing since 2020-01-01 00:00:00"
            };
            ServerTab.SetWindowTruncatedBanner(banner, truncated: false, DateTime.UtcNow, TimeZoneInfo.Utc);
            return (banner.Visibility, banner.Text);
        });

        Assert.Equal(System.Windows.Visibility.Collapsed, visibility);
        Assert.Equal(string.Empty, text);
    }

    /// <summary>WPF objects require STA; same shape as MainWindowAccessKeyTests/ThemeColorOverrideTests.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }

    /// <summary>
    /// #4231 ruling 6's source pin: every Queries-tab grid read of the three raw-only relations routes through
    /// the ONE shared <see cref="LocalDataService.GetQueryWindowFloorAsync"/> probe -- never a second hand-rolled
    /// copy -- and every grid-refresh call site (sub-tab switch, full refresh, and the matching slicer handler,
    /// which re-reads the same grid over a narrower window) calls the shared banner helper. Text-scans SOURCE,
    /// not a loaded assembly, matching ServerTabCapabilityPinTests' mechanism.
    /// </summary>
    [Fact]
    public void QueriesTabGridReads_RouteThroughSharedWindowFloorHelper()
    {
        var refreshSource = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs"));
        var combined = refreshSource + "\n" + File.ReadAllText(ControlsFile("ServerTab.Slicers.cs"));

        // The raw probe itself must have exactly ONE call site -- the shared RefreshWindowTruncatedBannerAsync
        // helper (ServerTab.Refresh.cs), which takes `relation` as a parameter rather than repeating the
        // literal per table, so every grid/slicer read forwards through the same probe call.
        var floorCalls = Regex.Matches(refreshSource, @"_dataService\.GetQueryWindowFloorAsync\(").Count;
        Assert.True(floorCalls == 1,
            $"expected exactly one _dataService.GetQueryWindowFloorAsync(...) call site in ServerTab.Refresh.cs " +
            $"(found {floorCalls}) -- every Queries-tab grid/slicer read must route through the ONE shared " +
            "RefreshWindowTruncatedBannerAsync helper, not a second copy of the probe (#4231).");

        foreach (var relation in new[] { "QueryStats", "ProcedureStats", "QueryStoreStats" })
        {
            var bannerCalls = Regex.Matches(combined, $@"RefreshWindowTruncatedBannerAsync\(\s*QueryWindowRelation\.{relation}\b").Count;
            Assert.True(bannerCalls >= 3,
                $"expected at least 3 RefreshWindowTruncatedBannerAsync(QueryWindowRelation.{relation}...) call " +
                $"sites (sub-tab switch + full refresh + slicer handler), found {bannerCalls} -- a Queries-tab " +
                "grid read of that relation is missing its window-truncated banner refresh (#4231).");
        }

        foreach (var file in Directory.EnumerateFiles(ControlsDir(), "ServerTab*.cs"))
        {
            Assert.False(File.ReadAllText(file).Contains("MIN(collection_time)", StringComparison.OrdinalIgnoreCase),
                $"{Path.GetFileName(file)} hand-rolls a MIN(collection_time) query -- route it through " +
                "LocalDataService.GetQueryWindowFloorAsync instead (#4231).");
        }
    }

    /// <summary>
    /// #4279: <c>collection_time</c> is UTC and <see cref="LocalDataService.GetQueryWindowFloorAsync"/> compares
    /// straight against it, no offset conversion. Pins <see cref="LocalDataService.GetQueriesTabWindowUtc"/> --
    /// the SAME <c>GetTimeRange</c> custom-range branch <c>GetTopQueriesByCpuAsync</c>/etc. use for the grid's
    /// OWN window -- against a non-zero offset, so a caller that stops converting (or converts the wrong
    /// direction) fails loudly rather than only on a server that happens to run UTC.
    /// </summary>
    [Fact]
    public void GetQueriesTabWindowUtc_CustomRange_IsTheHeldUtcPairUnchanged()
    {
        var fromDate = new DateTime(2026, 1, 15, 12, 0, 0, DateTimeKind.Unspecified);
        var toDate = new DateTime(2026, 1, 15, 14, 0, 0, DateTimeKind.Unspecified);

        var (startUtc, endUtc) = LocalDataService.GetQueriesTabWindowUtc(24, fromDate, toDate);

        Assert.Equal(fromDate, startUtc);
        Assert.Equal(toDate, endUtc);
    }

    /// <summary>
    /// #4279: OnXSlicerChanged (ServerTab.Slicers.cs) passes <c>e.StartUtc</c>/<c>e.EndUtc</c> to the banner
    /// untouched, and the grid read beside it takes the SAME bounds as they are
    /// (<see cref="LocalDataService.GetQueriesTabWindowUtc"/>'s custom-range branch; #4766 took out the
    /// server-local round trip that used to sit between them). This pins that the banner's UTC bounds equal what
    /// the grid actually reads, without mutating the process-global <c>ServerTimeHelper.UtcOffsetMinutes</c>,
    /// which parallel test classes also read.
    /// </summary>
    [Fact]
    public void SlicerBannerWindow_MatchesTheGridsUtcWindow_ForANonUtcServer()
    {
        var startUtc = new DateTime(2026, 1, 15, 8, 0, 0, DateTimeKind.Unspecified);
        var endUtc = new DateTime(2026, 1, 15, 10, 0, 0, DateTimeKind.Unspecified);

        /* The slicer hands e.StartUtc/e.EndUtc to the grid read as they are (#4766): no conversion either way. */
        var (gridStartUtc, gridEndUtc) = LocalDataService.GetQueriesTabWindowUtc(24, startUtc, endUtc);

        Assert.Equal(startUtc, gridStartUtc);
        Assert.Equal(endUtc, gridEndUtc);
    }

    /// <summary>
    /// #4279 revert-proof: text-scans SOURCE (matching <see cref="QueriesTabGridReads_RouteThroughSharedWindowFloorHelper"/>'s
    /// mechanism) so a future edit that quietly goes back to feeding the banner server-local fromServer/toServer
    /// fails a test even though those names still compile fine (they are plain <c>DateTime</c>s either way).
    /// Confirmed by reverting ServerTab.Slicers.cs and ServerTab.Refresh.cs to 3b8d9e12 (pre-#4279-fix): both
    /// assertions below failed before that fix.
    ///
    /// <para>#4284 updated the ServerTab.Refresh.cs half: the banner call now shares its windowStart/windowEnd
    /// tuple (formerly bannerStart/bannerEnd) with the comparison call beside it, rather than the comparison
    /// keeping its own server-local cStart -- see <see cref="ComparisonWindowUtcTests.ComparisonCallSites_TakeUtcBounds_NotServerLocalOnes"/>
    /// for that half's pin.</para>
    /// </summary>
    [Fact]
    public void WindowTruncatedBannerCallSites_TakeUtcBounds_NotServerLocalOnes()
    {
        var slicersSource = File.ReadAllText(ControlsFile("ServerTab.Slicers.cs"));
        var slicerBannerCallsOnUtc = Regex.Matches(slicersSource,
            @"(?:RefreshWindowTruncatedBannerAsync|RefreshCappedGridBannerAsync)\(\s*QueryWindowRelation\.\w+,\s*\w+,\s*e\.StartUtc,\s*e\.EndUtc[,)]").Count;
        /* 3 -> 4: Active Queries' slicer handler joined the three Queries grids' (the Lite twin of #4953's data-start notice).
           4 -> 6: the Blocked Process Reports and Deadlocks slicer handlers joined them (#4966). */
        Assert.True(slicerBannerCallsOnUtc == 6,
            $"expected all 6 OnXSlicerChanged banner calls to pass e.StartUtc, e.EndUtc (found {slicerBannerCallsOnUtc}) " +
            "-- fromServer/toServer are server-local and GetQueryWindowFloorAsync compares them straight against " +
            "UTC collection_time (#4279).");
        Assert.False(Regex.IsMatch(slicersSource, @"RefreshWindowTruncatedBannerAsync\([^)]*fromServer,\s*toServer\)"),
            "a slicer banner call still passes server-local fromServer/toServer (#4279).");

        var refreshSource = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs"));
        var refreshBannerCallsOnHelperOutput = Regex.Matches(refreshSource,
            @"(?:RefreshWindowTruncatedBannerAsync|RefreshCappedGridBannerAsync)\(\s*QueryWindowRelation\.\w+,\s*\w+,\s*windowStart\d?,\s*windowEnd\d?[,)]").Count;
        /* 6 -> 10: Active Queries (sub-tab switch + full refresh) and Current Waits (sub-tab switch + full refresh)
           each take their banner window from GetQueriesTabWindowUtc too (see DataStartBannerTests).
           10 -> 14, and 10 -> 13 declarations: the Blocked Process Reports and Deadlocks banners (#4966) add two calls
           to the sub-tab switch (one declaration each) and two to the full refresh (one shared declaration). */
        /* 14 -> 15 (the pattern now counts the cap-aware step too): the Blocked Process Reports and Deadlocks banners (4 of the 14)
           moved from RefreshWindowTruncatedBannerAsync to RefreshCappedGridBannerAsync (#4966), and the Collection Log's cap-aware
           call (#4989) is one more call on a GetQueriesTabWindowUtc pair. */
        Assert.True(refreshBannerCallsOnHelperOutput == 15,
            $"expected all 15 ServerTab.Refresh.cs banner calls to pass a GetQueriesTabWindowUtc result " +
            $"(windowStart/windowEnd) (found {refreshBannerCallsOnHelperOutput}) -- a server-local cStart must " +
            "not feed the banner (#4279/#4284).");
        /* 10 -> 11: the Collection Log's cap-aware notice (RefreshCappedGridBannerAsync, #4989) takes its window from
           GetQueriesTabWindowUtc too; it is not one of the RefreshWindowTruncatedBannerAsync calls counted above.
           11 -> 14: the Blocked Process Reports and Deadlocks banners (#4966) add the three declarations noted above. */
        Assert.Equal(14, Regex.Matches(refreshSource, @"LocalDataService\.GetQueriesTabWindowUtc\(").Count);
    }

    private static string ControlsFile(string name) => Path.Combine(ControlsDir(), name);

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
