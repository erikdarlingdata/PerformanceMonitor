/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4793: four Lite MCP tools return a server-local stamp converted to UTC. They converted every stamp with
/// the ONE offset the server last reported, so a stamp from before a daylight saving change came back an
/// hour off. They now convert each stamp with the server's clock (its time zone, when one was collected).
///
/// <para>The tools under test: <c>get_blocked_process_reports</c> (six blocked_/blocking_ transaction and batch
/// stamps), <c>get_running_jobs</c> (<c>start_time</c>), <c>get_index_usage</c> (<c>last_user_access</c>) and
/// <c>get_pvs_stats</c> (the four cleaner times). Every test calls the tool itself and reads the JSON it returns.</para>
///
/// <para>The server is US Eastern, and its newest collected offset is the POST-change one (-240, daylight
/// time), so one-offset conversion is right for a stamp after the change and an hour off before it. The 2026
/// spring-forward is 8 March: 02:00 EST becomes 03:00 EDT at 07:00 UTC. So 01:30 EST is 06:30 UTC and
/// 03:30 EDT is 07:30 UTC, the same pair <c>DefaultTraceServerClockTests</c> uses.</para>
///
/// <para>The last two tools under test, <c>get_top_queries_by_cpu</c> and <c>get_top_procedures_by_cpu</c>, use the
/// clock the other way round: they floor <c>last_execution_time</c>, a server-local stamp, at the window's start,
/// so the floor has to be the window start on THE SERVER'S clock. They passed no clock and so floored at the UTC
/// number: on a server west of UTC a query last run early in the window was dropped, and on one east of UTC a query
/// last run before the window was kept.</para>
/// </summary>
public sealed class LiteMcpServerClockTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string EasternZone = "Eastern Standard Time";
    private const string EasternName = "EasternSrv";
    private const string FixedName = "FixedOffsetSrv";
    private const string NoOffsetName = "NoOffsetSrv";
    private const string EastName = "EastSrv";

    /* Spring forward, 2026-03-08: a server-local stamp before 02:00 is EST (UTC-5), from 03:00 it is EDT (UTC-4). */
    private const int HoursToUtcBeforeChange = 5;
    private const int HoursToUtcAfterChange = 4;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _easternId;
    private readonly int _fixedId;
    private readonly int _noOffsetId;
    private readonly int _eastId;

    /* One snapshot time for every latest-snapshot read (jobs, index usage, PVS), so the rows seeded under it
       come back together. */
    private readonly DateTime _snapshot = DateTime.UtcNow.AddMinutes(-5);
    private long _nextId = 1;
    private DuckDBConnection? _seedConn;

    public LiteMcpServerClockTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "LiteMcpClockTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        /* Names that are not substrings of one another: ServerResolver falls back to a Contains match. */
        _easternId = Register(EasternName);
        _fixedId = Register(FixedName);
        _noOffsetId = Register(NoOffsetName);
        _eastId = Register(EastName);
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    private int Register(string name)
    {
        var server = new ServerConnection { ServerName = name, DisplayName = name };
        _serverManager.AddServer(server);

        /* The DERIVED id, not a literal: seeding under a hand-picked number makes every read return nothing. */
        return RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    // ── get_blocked_process_reports ──

    private static readonly string[] BlockingStampFields =
    [
        "blocked_last_tran_started", "blocking_last_tran_started",
        "blocked_last_batch_started", "blocking_last_batch_started",
        "blocked_last_batch_completed", "blocking_last_batch_completed",
    ];

    [Fact]
    public async Task BlockedProcessReports_StampsOnBothSidesOfSpringForward_ComeBackWithTheRightUtcTimes()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);

        var before = StampsAt(1, BlockingStampFields.Length);
        var after = StampsAt(3, BlockingStampFields.Length);
        await SeedBlockedReportAsync(_easternId, EasternName, blockedSpid: 51, before);
        await SeedBlockedReportAsync(_easternId, EasternName, blockedSpid: 52, after);
        await SeedBlockedReportAsync(_easternId, EasternName, blockedSpid: 53, new DateTime?[BlockingStampFields.Length]);

        var json = await McpBlockingTools.GetBlockedProcessReports(_dataService, _serverManager, EasternName);
        var reports = RowsOf(json, "reports").ToDictionary(r => r.GetProperty("blocked_spid").GetInt32());

        for (var i = 0; i < BlockingStampFields.Length; i++)
        {
            var field = BlockingStampFields[i];
            Assert.Equal(before[i]!.Value.AddHours(HoursToUtcBeforeChange), StampOf(reports[51], field));
            Assert.Equal(after[i]!.Value.AddHours(HoursToUtcAfterChange), StampOf(reports[52], field));
            Assert.Null(StampOf(reports[53], field));
        }

        /* The row's 01:30 stamp is the pair from the class summary: 06:30 UTC, not the 05:30 a -240 offset gives. */
        Assert.Equal(At(2026, 3, 8, 6, 30), StampOf(reports[51], "blocked_last_batch_completed"));
    }

    // ── get_running_jobs ──

    [Fact]
    public async Task RunningJobs_StartTimesOnBothSidesOfSpringForward_ComeBackWithTheRightUtcTimes()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);
        await SeedRunningJobAsync(_easternId, EasternName, "job-before", At(2026, 3, 8, 1, 30));
        await SeedRunningJobAsync(_easternId, EasternName, "job-after", At(2026, 3, 8, 3, 30));

        var json = await McpJobTools.GetRunningJobs(_dataService, _serverManager, EasternName);
        var jobs = RowsOf(json, "jobs").ToDictionary(j => j.GetProperty("job_name").GetString()!);

        Assert.Equal(At(2026, 3, 8, 6, 30), StampOf(jobs["job-before"], "start_time"));   /* 01:30 EST */
        Assert.Equal(At(2026, 3, 8, 7, 30), StampOf(jobs["job-after"], "start_time"));    /* 03:30 EDT */
    }

    /// <summary>
    /// A stored server-local time can land on either odd hour of the change: 02:30 on the spring-forward day
    /// never happened, and 01:30 on the fall-back day happened twice. The read answers both, never fails, and
    /// reads them the way <c>ServerClock.ToUtc</c> documents: the skipped time as 03:30 EDT, the repeated time
    /// as its first occurrence (the daylight offset).
    /// </summary>
    [Fact]
    public async Task RunningJobs_SkippedAndRepeatedLocalStartTimes_ReadWithoutFailing()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);
        await SeedRunningJobAsync(_easternId, EasternName, "job-skipped", At(2026, 3, 8, 2, 30));
        await SeedRunningJobAsync(_easternId, EasternName, "job-repeated", At(2026, 11, 1, 1, 30));

        var json = await McpJobTools.GetRunningJobs(_dataService, _serverManager, EasternName);
        var jobs = RowsOf(json, "jobs").ToDictionary(j => j.GetProperty("job_name").GetString()!);

        Assert.Equal(At(2026, 3, 8, 7, 30), StampOf(jobs["job-skipped"], "start_time"));     /* reads as 03:30 EDT */
        Assert.Equal(At(2026, 11, 1, 5, 30), StampOf(jobs["job-repeated"], "start_time"));   /* first 01:30, EDT */
    }

    /// <summary>
    /// A server that reported no time zone keeps its fixed offset all year, and a server that reported nothing
    /// at all keeps reading as UTC: the clock changes what a zone-aware server gets, not these two.
    /// </summary>
    [Fact]
    public async Task RunningJobs_ServerWithoutAZone_KeepsItsFixedOffset_AndAServerWithNoOffsetReadsAsUtc()
    {
        await SeedServerClockAsync(_fixedId, FixedName, -300, zoneId: null);
        await SeedRunningJobAsync(_fixedId, FixedName, "fixed-winter", At(2026, 1, 15, 12, 0));
        await SeedRunningJobAsync(_fixedId, FixedName, "fixed-summer", At(2026, 7, 15, 12, 0));
        await SeedRunningJobAsync(_noOffsetId, NoOffsetName, "no-offset", At(2026, 7, 15, 12, 0));

        var fixedJobs = RowsOf(await McpJobTools.GetRunningJobs(_dataService, _serverManager, FixedName), "jobs")
            .ToDictionary(j => j.GetProperty("job_name").GetString()!);
        var noOffsetJobs = RowsOf(await McpJobTools.GetRunningJobs(_dataService, _serverManager, NoOffsetName), "jobs")
            .ToDictionary(j => j.GetProperty("job_name").GetString()!);

        Assert.Equal(At(2026, 1, 15, 17, 0), StampOf(fixedJobs["fixed-winter"], "start_time"));
        Assert.Equal(At(2026, 7, 15, 17, 0), StampOf(fixedJobs["fixed-summer"], "start_time"));
        Assert.Equal(At(2026, 7, 15, 12, 0), StampOf(noOffsetJobs["no-offset"], "start_time"));
    }

    // ── get_index_usage ──

    [Fact]
    public async Task IndexUsage_LastUserAccessOnBothSidesOfSpringForward_ComesBackWithTheRightUtcTime()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);
        await SeedIndexAsync(_easternId, EasternName, "ix_before", At(2026, 3, 8, 1, 30));
        await SeedIndexAsync(_easternId, EasternName, "ix_after", At(2026, 3, 8, 3, 30));
        await SeedIndexAsync(_easternId, EasternName, "ix_never_read", lastUserSeek: null);

        var json = await McpObjectStatsTools.GetIndexUsage(_dataService, _serverManager, EasternName, database_name: "AppDb");
        var indexes = RowsOf(json, "indexes").ToDictionary(i => i.GetProperty("index_name").GetString()!);

        Assert.Equal(At(2026, 3, 8, 6, 30), StampOf(indexes["ix_before"], "last_user_access"));   /* 01:30 EST */
        Assert.Equal(At(2026, 3, 8, 7, 30), StampOf(indexes["ix_after"], "last_user_access"));    /* 03:30 EDT */
        Assert.Null(StampOf(indexes["ix_never_read"], "last_user_access"));
    }

    // ── get_pvs_stats ──

    private static readonly string[] PvsStampFields =
    [
        "aborted_version_cleaner_start_time", "aborted_version_cleaner_end_time",
        "offrow_version_cleaner_start_time", "offrow_version_cleaner_end_time",
    ];

    [Fact]
    public async Task PvsStats_CleanerTimesOnBothSidesOfSpringForward_ComeBackWithTheRightUtcTimes()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);

        var before = StampsAt(1, PvsStampFields.Length);
        var after = StampsAt(3, PvsStampFields.Length);
        await SeedPvsAsync(_easternId, EasternName, "PvsBefore", before);
        await SeedPvsAsync(_easternId, EasternName, "PvsAfter", after);
        await SeedPvsAsync(_easternId, EasternName, "PvsNever", new DateTime?[PvsStampFields.Length]);

        var json = await McpPvsTools.GetPvsStats(_dataService, _serverManager, EasternName);
        var databases = RowsOf(json, "databases").ToDictionary(d => d.GetProperty("database_name").GetString()!);

        for (var i = 0; i < PvsStampFields.Length; i++)
        {
            var field = PvsStampFields[i];
            Assert.Equal(before[i]!.Value.AddHours(HoursToUtcBeforeChange), StampOf(databases["PvsBefore"], field));
            Assert.Equal(after[i]!.Value.AddHours(HoursToUtcAfterChange), StampOf(databases["PvsAfter"], field));
            Assert.Null(StampOf(databases["PvsNever"], field));
        }

        Assert.Equal(At(2026, 3, 8, 6, 15), StampOf(databases["PvsBefore"], "aborted_version_cleaner_end_time"));
    }

    // ── get_top_queries_by_cpu / get_top_procedures_by_cpu ──

    /* The window is pinned with as_of so the offset in force at its start is known. In mid-July US Eastern is on
       daylight time (UTC-4), so the 4-hour window ending 16:00 UTC starts at 12:00 UTC, which is 08:00 on the server's
       clock. collection_time is UTC and sits inside the window for every row below; last_execution_time is the
       server's own wall clock, the column under test. */
    private const string SummerAsOf = "2026-07-15T16:00:00Z";
    private static readonly DateTime SummerCollected = At(2026, 7, 15, 15, 30);

    [Fact]
    public async Task TopQueries_ServerWestOfUtc_KeepsAQueryLastRunOneHourIntoTheWindow_AndDropsOneBeforeIt()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);
        await SeedQueryStatsAsync(_easternId, EasternName, "0xINSIDE", At(2026, 7, 15, 9, 0));    /* 13:00 UTC: 1 hour in */
        await SeedQueryStatsAsync(_easternId, EasternName, "0xBEFORE", At(2026, 7, 15, 7, 0));    /* 11:00 UTC: before the window */

        var json = await McpQueryTools.GetTopQueriesByCpu(_dataService, _serverManager, EasternName, hours_back: 4, as_of: SummerAsOf);
        var hashes = ValuesOf(json, "queries", "query_hash");

        Assert.Contains("0xINSIDE", hashes);
        Assert.DoesNotContain("0xBEFORE", hashes);
    }

    [Fact]
    public async Task TopQueries_ServerEastOfUtc_DropsAQueryLastRunBeforeTheWindow_AndKeepsOneInsideIt()
    {
        await SeedServerClockAsync(_eastId, EastName, 330, null);
        await SeedQueryStatsAsync(_eastId, EastName, "0xINSIDE", At(2026, 7, 15, 18, 30));   /* 13:00 UTC: 1 hour in */
        await SeedQueryStatsAsync(_eastId, EastName, "0xBEFORE", At(2026, 7, 15, 13, 0));    /* 07:30 UTC: before the window */

        var json = await McpQueryTools.GetTopQueriesByCpu(_dataService, _serverManager, EastName, hours_back: 4, as_of: SummerAsOf);
        var hashes = ValuesOf(json, "queries", "query_hash");

        Assert.Contains("0xINSIDE", hashes);
        Assert.DoesNotContain("0xBEFORE", hashes);
    }

    [Fact]
    public async Task TopProcedures_ServerWestOfUtc_KeepsAProcedureLastRunOneHourIntoTheWindow_AndDropsOneBeforeIt()
    {
        await SeedServerClockAsync(_easternId, EasternName, -240, EasternZone);
        await SeedProcedureStatsAsync(_easternId, EasternName, "ProcInside", At(2026, 7, 15, 9, 0));
        await SeedProcedureStatsAsync(_easternId, EasternName, "ProcBefore", At(2026, 7, 15, 7, 0));

        var json = await McpQueryTools.GetTopProceduresByCpu(_dataService, _serverManager, EasternName, hours_back: 4, as_of: SummerAsOf);
        var names = ValuesOf(json, "procedures", "full_name");

        Assert.Contains("dbo.ProcInside", names);
        Assert.DoesNotContain("dbo.ProcBefore", names);
    }

    [Fact]
    public async Task TopProcedures_ServerEastOfUtc_DropsAProcedureLastRunBeforeTheWindow_AndKeepsOneInsideIt()
    {
        await SeedServerClockAsync(_eastId, EastName, 330, null);
        await SeedProcedureStatsAsync(_eastId, EastName, "ProcInside", At(2026, 7, 15, 18, 30));
        await SeedProcedureStatsAsync(_eastId, EastName, "ProcBefore", At(2026, 7, 15, 13, 0));

        var json = await McpQueryTools.GetTopProceduresByCpu(_dataService, _serverManager, EastName, hours_back: 4, as_of: SummerAsOf);
        var names = ValuesOf(json, "procedures", "full_name");

        Assert.Contains("dbo.ProcInside", names);
        Assert.DoesNotContain("dbo.ProcBefore", names);
    }

    // ── reading the tool's answer ──

    private static DateTime At(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    /* Five-minute steps from :10, so no two stamps on a row match and a stamp read from the wrong column shows. */
    private static DateTime?[] StampsAt(int hour, int count)
        => Enumerable.Range(0, count).Select(i => (DateTime?)At(2026, 3, 8, hour, 10 + (5 * i))).ToArray();

    /* Asserts the tool answered with data, and puts what it did answer in the failure when it did not. */
    private static IReadOnlyList<JsonElement> RowsOf(string json, string arrayName)
    {
        using var doc = JsonDocument.Parse(json);
        Assert.True(
            doc.RootElement.TryGetProperty(arrayName, out var rows),
            $"the tool answered without '{arrayName}': {json[..Math.Min(json.Length, 300)]}");

        /* Cloned: the elements must outlive the document that parsed them. */
        return rows.EnumerateArray().Select(r => r.Clone()).ToList();
    }

    /* One field across the rows of an answer; empty when the tool answered with a status and no rows. */
    private static List<string> ValuesOf(string json, string arrayName, string field)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty(arrayName, out var rows)
            ? rows.EnumerateArray().Select(r => r.GetProperty(field).GetString()!).ToList()
            : [];
    }

    private static DateTime? StampOf(JsonElement row, string field)
    {
        Assert.True(row.TryGetProperty(field, out var value), $"the row has no '{field}'");
        return value.ValueKind == JsonValueKind.Null
            ? null
            : DateTime.Parse(value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
    }

    // ── seeding: every stamp is written on the server's own wall clock, the way the collectors store it ──

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        return _seedConn;
    }

    private async Task ExecuteAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value ?? DBNull.Value });
        }

        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>The server's clock as the collectors leave it: the offset in force at the newest snapshot, plus the zone id.</summary>
    private Task SeedServerClockAsync(int serverId, string serverName, int utcOffsetMinutes, string? zoneId)
        => ExecuteAsync(@"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, $4, 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $5, $6)",
            -_nextId++, DateTime.UtcNow, serverId, serverName, utcOffsetMinutes, zoneId);

    private Task SeedBlockedReportAsync(int serverId, string serverName, int blockedSpid, DateTime?[] stamps)
        => ExecuteAsync(@"INSERT INTO blocked_process_reports
            (blocked_report_id, collection_time, server_id, server_name, event_time, database_name,
             blocked_spid, blocking_spid,
             blocked_last_tran_started, blocking_last_tran_started,
             blocked_last_batch_started, blocking_last_batch_started,
             blocked_last_batch_completed, blocking_last_batch_completed)
            VALUES ($1, $2, $3, $4, $5, 'AppDb', $6, 99, $7, $8, $9, $10, $11, $12)",
            -_nextId++, _snapshot, serverId, serverName, _snapshot, blockedSpid,
            stamps[0], stamps[1], stamps[2], stamps[3], stamps[4], stamps[5]);

    private Task SeedRunningJobAsync(int serverId, string serverName, string jobName, DateTime startTimeServerLocal)
        => ExecuteAsync(@"INSERT INTO running_jobs
            (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time,
             current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count,
             is_running_long, percent_of_average)
            VALUES ($1, $2, $3, $4, $5, true, $6, 60, 60, 120, 10, false, 100.0)",
            _snapshot, serverId, serverName, jobName, Guid.NewGuid().ToString(), startTimeServerLocal);

    private Task SeedIndexAsync(int serverId, string serverName, string indexName, DateTime? lastUserSeek)
    {
        var objectId = (int)(_nextId++ + 1000);
        return ExecuteAsync(@"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc,
             reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, last_user_seek)
            VALUES ($1, $2, $3, $4, 'AppDb', 7, 'dbo', $5, $6, 2, $7, 'NONCLUSTERED', 10, 10, 1000, $8, 0, 0, 0, $9)",
            -_nextId++, _snapshot, serverId, serverName, objectId, "T" + objectId, indexName,
            lastUserSeek.HasValue ? 5 : 0, lastUserSeek);
    }

    private Task SeedPvsAsync(int serverId, string serverName, string databaseName, DateTime?[] stamps)
        => ExecuteAsync(@"INSERT INTO pvs_stats
            (collection_id, collection_time, server_id, server_name, database_name, database_id,
             is_accelerated_database_recovery_on, persistent_version_store_size_mb, database_data_size_mb,
             aborted_version_cleaner_start_time, aborted_version_cleaner_end_time,
             offrow_version_cleaner_start_time, offrow_version_cleaner_end_time)
            VALUES ($1, $2, $3, $4, $5, $6, true, 50, 1000, $7, $8, $9, $10)",
            -_nextId++, _snapshot, serverId, serverName, databaseName, 20 + (int)_nextId,
            stamps[0], stamps[1], stamps[2], stamps[3]);

    private Task SeedQueryStatsAsync(int serverId, string serverName, string queryHash, DateTime lastExecutionServerLocal)
        => ExecuteAsync(@"INSERT INTO query_stats
            (collection_id, collection_time, server_id, server_name, database_name,
             query_hash, sql_handle, last_execution_time, delta_execution_count,
             delta_worker_time, delta_elapsed_time, query_text)
            VALUES ($1, $2, $3, $4, 'AppDb', $5, $6, $7, 5, 200000, 200000, 'SELECT 1')",
            -_nextId++, SummerCollected, serverId, serverName, queryHash, queryHash + "H", lastExecutionServerLocal);

    private Task SeedProcedureStatsAsync(int serverId, string serverName, string objectName, DateTime lastExecutionServerLocal)
        => ExecuteAsync(@"INSERT INTO procedure_stats
            (collection_id, collection_time, server_id, server_name, database_name,
             schema_name, object_name, object_type, last_execution_time,
             delta_execution_count, delta_worker_time, delta_elapsed_time)
            VALUES ($1, $2, $3, $4, 'AppDb', 'dbo', $5, 'SQL_STORED_PROCEDURE', $6, 10, 200000, 200000)",
            -_nextId++, SummerCollected, serverId, serverName, objectName, lastExecutionServerLocal);
}
