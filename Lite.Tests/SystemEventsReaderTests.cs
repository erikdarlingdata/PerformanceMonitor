/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for the System Events tab's readers (LocalDataService.SystemEvents). The
/// Common SystemHealthParser shred and the shared SystemHealthSignificance / DefaultTraceEventSignificance
/// predicates are already unit-tested; these prove the LITE glue end-to-end against a freshly initialized
/// store: seed the v_ archive views' base tables with the same fixture XML shapes Darling uses, read back
/// through the reader, and assert (1) the reader keeps exactly the significant set, (2) the column
/// projection, (3) the Severe-Errors database_id -> name resolution (DuckDB QUALIFY), (4) the #1319 database
/// filter, (5) the event_time window bounding, and (6) the Default Trace server-local -> UTC de-skew.
/// </summary>
/* ServerTimeHelper.UtcOffsetMinutes / CurrentDisplayMode are process-wide mutable statics, and the
   server-time projection test below SETS them for its duration. xUnit runs test CLASSES in parallel, so
   without this shared collection that write lands underneath any sibling class reading the offset —
   CollectionHealthWindowTests computes a query window from it and would silently look for its seeded rows
   hours away, failing "expected 2, actual 0" at random. The three classes that touch ServerTimeHelper share
   one collection so they serialize against each other (and only each other). */
[Collection("server-time-helper")]
public sealed class SystemEventsReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 7777;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public SystemEventsReaderTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    // ── End-to-end significant set across every system_health category ──

    [Fact]
    public async Task SystemHealthCategories_ReadThroughVView_KeepOnlyTheSignificantSet()
    {
        var service = new LocalDataService(_duckDb);

        /* Store each fixture at a recent event_time so a 24h window includes it. The WINDOW filters on the
           stored event_time column; the ROW's own EventTimeLocal comes from the XML @timestamp (a Stage-2a
           parser concern, tested there). */
        var eventTime = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedHealthEventAsync(SystemHealthParser.SchedulerMonitorEvent, LoadFixture("scheduler_monitor_high_sql_cpu.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.ErrorReportedEvent, LoadFixture("error_reported.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_system.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_resource.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_query_processing.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_query_processing_warning.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_io_subsystem.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.MemoryBrokerEvent, LoadFixture("memory_broker.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.MemoryNodeOomEvent, LoadFixture("memory_node_oom.xml"), eventTime);
        await SeedHealthEventAsync(SystemHealthParser.WaitInfoEvent, LoadFixture("wait_info.xml"), eventTime);

        // Scheduler (WARNING), Severe Error (sev 24), Memory Node OOM (ungated), and the wait pass.
        Assert.Single(await service.GetSchedulerIssuesAsync(ServerId));
        Assert.Single(await service.GetSevereErrorsAsync(ServerId));
        Assert.Single(await service.GetMemoryNodeOomAsync(ServerId));
        Assert.Single(await service.GetSignificantWaitsAsync(ServerId));
        // The memory-conditions / broker fixtures are RESOURCE_MEMPHYSICAL_HIGH — warnings_only drops them.
        Assert.Empty(await service.GetMemoryConditionsAsync(ServerId));
        Assert.Empty(await service.GetMemoryBrokerAsync(ServerId));
        // CPU: the WARNING+15-pending event passes, the CLEAN one is dropped. I/O: both WARNING file rows pass.
        Assert.Single(await service.GetCpuTasksAsync(ServerId));
        Assert.Equal(2, (await service.GetIoIssuesAsync(ServerId)).Count);
        // System Health (Corruption + Contention charts) has NO significance filter — the CLEAN SYSTEM
        // snapshot is kept so the counter charts plot it (contrast with the dropped HIGH memory rows).
        Assert.Single(await service.GetSystemHealthAsync(ServerId));
    }

    [Fact]
    public async Task SevereErrors_ProjectColumns_AndResolveDatabaseNameFromSizeStats()
    {
        var service = new LocalDataService(_duckDb);

        var eventTime = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedHealthEventAsync(SystemHealthParser.ErrorReportedEvent, LoadFixture("error_reported.xml"), eventTime);
        /* error_reported.xml carries database_id 6; map it via the latest size-stats snapshot (newest name
           per id, the QUALIFY ROW_NUMBER dedup). A second, older row for the same id proves "latest wins". */
        await SeedDatabaseSizeAsync(databaseId: 6, databaseName: "OldName", collectionTime: eventTime.AddHours(-2));
        await SeedDatabaseSizeAsync(databaseId: 6, databaseName: "ProdDB", collectionTime: eventTime);

        var rows = await service.GetSevereErrorsAsync(ServerId);
        var row = Assert.Single(rows);
        Assert.Equal(823, row.ErrorNumber);
        Assert.Equal(24, row.Severity);
        Assert.Equal(6, row.DatabaseId);
        Assert.Equal("ProdDB", row.DatabaseName);
        Assert.Contains("operating system returned error", row.Message);
    }

    [Fact]
    public async Task SevereErrors_UnmappedDatabaseId_SurfacesTheRawId()
    {
        var service = new LocalDataService(_duckDb);

        // No size-stats rows at all — database_id 6 can't resolve, so it surfaces as "database_id 6".
        await SeedHealthEventAsync(SystemHealthParser.ErrorReportedEvent, LoadFixture("error_reported.xml"), Truncate(DateTime.UtcNow.AddHours(-1)));

        var row = Assert.Single(await service.GetSevereErrorsAsync(ServerId));
        Assert.Equal("database_id 6", row.DatabaseName);
    }

    [Fact]
    public async Task SevereErrors_DatabaseFilter_1319_FiltersOnResolvedName()
    {
        var service = new LocalDataService(_duckDb);

        var eventTime = Truncate(DateTime.UtcNow.AddHours(-1));
        await SeedHealthEventAsync(SystemHealthParser.ErrorReportedEvent, LoadFixture("error_reported.xml"), eventTime);
        await SeedDatabaseSizeAsync(databaseId: 6, databaseName: "ProdDB", collectionTime: eventTime);

        // Selecting the resolved name keeps the row; selecting a different DB filters it out.
        Assert.Single(await service.GetSevereErrorsAsync(ServerId, databaseNames: new[] { "ProdDB" }));
        Assert.Empty(await service.GetSevereErrorsAsync(ServerId, databaseNames: new[] { "SomethingElse" }));
    }

    [Fact]
    public async Task IoIssues_ProjectWarningRows_WithFilePathAndSummedDuration()
    {
        var service = new LocalDataService(_duckDb);

        await SeedHealthEventAsync(SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_io_subsystem.xml"), Truncate(DateTime.UtcNow.AddHours(-1)));

        var rows = await service.GetIoIssuesAsync(ServerId);
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r => Assert.Equal("WARNING", r.State));
        Assert.All(rows, r => Assert.False(string.IsNullOrEmpty(r.LongestPendingRequestsFilePath)));
    }

    [Fact]
    public async Task SystemHealthWindow_ExcludesEventsOutsideTheEventTimeBounds()
    {
        var service = new LocalDataService(_duckDb);

        // One in-window scheduler warning, one stored 48h ago — a 24h read must return only the recent one.
        await SeedHealthEventAsync(SystemHealthParser.SchedulerMonitorEvent, LoadFixture("scheduler_monitor_high_sql_cpu.xml"), Truncate(DateTime.UtcNow.AddHours(-1)));
        await SeedHealthEventAsync(SystemHealthParser.SchedulerMonitorEvent, LoadFixture("scheduler_monitor_high_sql_cpu.xml"), Truncate(DateTime.UtcNow.AddHours(-48)));

        Assert.Single(await service.GetSchedulerIssuesAsync(ServerId, hoursBack: 24));
    }

    // ── Default Trace: significant set, projection, de-skew, DB filter ──

    [Fact]
    public async Task DefaultTrace_KeepsSignificantSet_ProjectsCategoryAndGrowth()
    {
        var service = new LocalDataService(_duckDb);

        var offset = ServerTimeHelper.UtcOffsetMinutes;
        var serverLocal = Truncate(DateTime.UtcNow.AddMinutes(offset).AddHours(-1)); // in the server-local window

        // Auto-grow stall (256 pages -> 2 MB), severe ErrorLog (sev 20, kept), routine ErrorLog (sev 10, DROPPED).
        await SeedDefaultTraceAsync(serverLocal, "Data File Auto Grow", databaseName: "SalesDB", durationUs: 1_500_000, integerData: 256, severity: null, errorNumber: null, textData: null);
        await SeedDefaultTraceAsync(serverLocal, "ErrorLog", databaseName: null, durationUs: null, integerData: null, severity: 20, errorNumber: 9002, textData: "severe");
        await SeedDefaultTraceAsync(serverLocal, "ErrorLog", databaseName: null, durationUs: null, integerData: null, severity: 10, errorNumber: 50000, textData: "routine");

        var rows = await service.GetDefaultTraceEventsAsync(ServerId);
        Assert.Equal(2, rows.Count);   // the sev-10 ErrorLog is dropped by the severity floor
        Assert.DoesNotContain(rows, r => r.Category == "ErrorLog" && r.Severity == 10);

        var grow = Assert.Single(rows, r => r.Category == "AutoGrowShrink");
        Assert.Equal("Data File Auto Grow", grow.EventName);
        Assert.Equal("SalesDB", grow.DatabaseName);
        Assert.Equal(1500m, grow.DurationMs);   // 1_500_000 us / 1000
        Assert.Equal(2.0m, grow.GrowthMb);       // 256 pages * 8 KB / 1024
    }

    [Fact]
    public async Task DefaultTrace_DeSkewsServerLocalStartTimeToUtc_RendersConsistentlyWithSystemHealth()
    {
        var service = new LocalDataService(_duckDb);

        /* Force a known non-zero offset + server-time display so the de-skew is genuinely exercised: the
           stored server-local StartTime is de-skewed to UTC on read, then FormatServerTime renders it back
           to the server-local wall clock — so EventTimeLocal must equal the stored server-local time. */
        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        var savedMode = ServerTimeHelper.CurrentDisplayMode;
        try
        {
            ServerTimeHelper.UtcOffsetMinutes = 330; // UTC+5:30
            ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

            var serverLocal = Truncate(DateTime.UtcNow.AddMinutes(330).AddHours(-1));
            await SeedDefaultTraceAsync(serverLocal, "Server Memory Change", databaseName: null, durationUs: null, integerData: null, severity: null, errorNumber: null, textData: "mem");

            var row = Assert.Single(await service.GetDefaultTraceEventsAsync(ServerId));
            Assert.Equal(serverLocal.ToString("yyyy-MM-dd HH:mm:ss"), row.EventTimeLocal);
            Assert.Equal("MemoryChange", row.Category);
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
            ServerTimeHelper.CurrentDisplayMode = savedMode;
        }
    }

    [Fact]
    public async Task DefaultTrace_DatabaseFilter_1319_FiltersOnDatabaseNameInSql()
    {
        var service = new LocalDataService(_duckDb);

        var offset = ServerTimeHelper.UtcOffsetMinutes;
        var serverLocal = Truncate(DateTime.UtcNow.AddMinutes(offset).AddHours(-1));
        await SeedDefaultTraceAsync(serverLocal, "Object:Altered", databaseName: "SalesDB", durationUs: null, integerData: null, severity: null, errorNumber: null, textData: "ALTER");
        await SeedDefaultTraceAsync(serverLocal, "Object:Altered", databaseName: "OtherDB", durationUs: null, integerData: null, severity: null, errorNumber: null, textData: "ALTER");

        Assert.Equal(2, (await service.GetDefaultTraceEventsAsync(ServerId)).Count);
        var filtered = await service.GetDefaultTraceEventsAsync(ServerId, databaseNames: new[] { "SalesDB" });
        Assert.All(filtered, r => Assert.Equal("SalesDB", r.DatabaseName));
        Assert.Single(filtered);
    }

    // ── Pure helpers: database-name resolution (no store), mirroring the shared contract ──

    [Fact]
    public void ResolveDatabaseName_MappedZeroNullAndUnmapped()
    {
        var map = new Dictionary<int, string> { [5] = "AdventureWorks" };
        Assert.Equal("AdventureWorks", LocalDataService.ResolveDatabaseName(5, map));
        Assert.Equal(string.Empty, LocalDataService.ResolveDatabaseName(0, map));
        Assert.Equal(string.Empty, LocalDataService.ResolveDatabaseName(null, map));
        Assert.Equal("database_id 7", LocalDataService.ResolveDatabaseName(7, map));
    }

    // ── Row VM projection (machine-local time, shared UTC frame) ──

    [Fact]
    public void Rows_EventTimeLocal_SharesTheSameUtcFrame_AndNullRendersEmpty()
    {
        var utc = new DateTime(2026, 7, 9, 12, 0, 5, DateTimeKind.Utc);
        var expected = ServerTimeHelper.FormatServerTime(utc, "yyyy-MM-dd HH:mm:ss");

        var scheduler = new SchedulerIssueRow(new SchedulerIssueRecord { EventTime = utc });
        var defaultTrace = new DefaultTraceEventRow(
            utc, DefaultTraceEventCategory.AutoGrowShrink, "Data File Auto Grow", "SalesDB", null,
            "app", "APPHOST", "SqlClient", 55, 1_500_000, 256, null, null, null);

        Assert.Equal(expected, scheduler.EventTimeLocal);
        Assert.Equal(scheduler.EventTimeLocal, defaultTrace.EventTimeLocal);

        Assert.Equal(string.Empty, new MemoryBrokerRow(new MemoryBrokerRecord { EventTime = null }).EventTimeLocal);
        Assert.Equal(string.Empty, new DefaultTraceEventRow(
            null, DefaultTraceEventCategory.ErrorLog, "ErrorLog", null, null, null, null, null, null, null, null, 20, 823, "x").EventTimeLocal);
    }

    // ── Seeding helpers (raw INSERT via the shared initializer's connection, closed before each read) ──

    private async Task SeedHealthEventAsync(string eventType, string eventXml, DateTime eventTimeUtc)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO system_health_events
    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
VALUES ($1, $2, $3, $4, $5, $6, $7)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTimeUtc });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventType });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventXml });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDefaultTraceAsync(
        DateTime eventTimeServerLocal, string eventName, string? databaseName, long? durationUs,
        long? integerData, int? severity, int? errorNumber, string? textData)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name,
     database_name, duration_us, integer_data, severity, error_number, text_data)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventTimeServerLocal });
        cmd.Parameters.Add(new DuckDBParameter { Value = eventName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)databaseName ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)durationUs ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)integerData ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)severity ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)errorNumber ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)textData ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDatabaseSizeAsync(int databaseId, string databaseName, DateTime collectionTime)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        /* database_size_stats is the id -> name mapping source; provide its NOT NULL columns. */
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = databaseId });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1 });
        cmd.Parameters.Add(new DuckDBParameter { Value = "ROWS" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "data" });
        cmd.Parameters.Add(new DuckDBParameter { Value = @"D:\data\prod.mdf" });
        cmd.Parameters.Add(new DuckDBParameter { Value = 1024.00m });
        await cmd.ExecuteNonQueryAsync();
    }
}
