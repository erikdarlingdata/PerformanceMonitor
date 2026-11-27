using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9340: the alert engine's blocking read leaves out a pair whose two sessions are both the monitor's own (the monitor's collectors queued
/// behind each other), on the blocked-process-report arm and the DMV snapshot arm. A pair with a user's session on either side, or with no
/// program name recorded, still alerts. The blocking grids still show every pair.
/// </summary>
public sealed class MonitorOwnBlockingAlertReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = -9340_02;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private DuckDBConnection? _conn;
    private long _nextId = -1;

    public MonitorOwnBlockingAlertReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose() => _conn?.Dispose();

    /* Blocked spid is 100 + n on the report arm and 200 + n on the snapshot arm, so the two arms never cover the same pair. */
    private async Task SeedAsync(bool report, int n, string? blockedApp, string? blockingApp)
    {
        var at = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-10 - n), DateTimeKind.Unspecified);
        using var readLock = _duckDb.AcquireReadLock();
        _conn ??= _duckDb.CreateConnection();
        if (_conn.State != System.Data.ConnectionState.Open) await _conn.OpenAsync();
        using var cmd = _conn.CreateCommand();
        cmd.CommandText = report
            ? @"INSERT INTO blocked_process_reports (blocked_report_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app)
                VALUES ($1,$2,$2,$3,'TestServer',12000,$4,50,$5,$6)"
            : @"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, event_time, server_id, server_name, wait_time_ms, blocked_spid, blocking_spid, blocked_client_app, blocking_client_app, monitor_loop)
                VALUES ($1,$2,$2,$3,'TestServer',12000,$4,50,$5,$6,1)";
        foreach (var arg in new object?[] { _nextId--, at, ServerId, (report ? 100 : 200) + n, blockedApp, blockingApp })
            cmd.Parameters.Add(new DuckDBParameter { Value = arg ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<List<int>> AlertBlockedSpidsAsync() =>
        (await new LiteAlertReadAdapter(_service).GetRecentBlockedProcessReportsAsync(ServerId.ToString(), 4))
            .Select(r => r.BlockedSpid).OrderBy(s => s).ToList();

    [Fact]
    public async Task OnlyTheMonitorsOwnPairs_RaiseNoAlertRows_ButTheGridStillShowsThem()
    {
        await SeedAsync(true, 0, "PerformanceMonitorDarling", "PerformanceMonitorDarling");
        await SeedAsync(true, 1, "PerformanceMonitorDarling-Remediation", "PerformanceMonitorDarling");
        await SeedAsync(false, 0, "PerformanceMonitorLite", "PerformanceMonitorLite");

        Assert.Empty(await AlertBlockedSpidsAsync());
        Assert.Equal(3, (await _service.GetRecentBlockedProcessReportsAsync(ServerId, 4)).Count);
    }

    [Fact]
    public async Task APairWithAUserSession_OrNoProgramName_StillRaisesAnAlertRow()
    {
        foreach (var report in new[] { true, false })
        {
            await SeedAsync(report, 0, "PerformanceMonitorDarling", "PerformanceMonitorDarling");   // left out
            await SeedAsync(report, 1, "PerformanceMonitorDarling", "OrderEntry");                 // monitor blocked by a user
            await SeedAsync(report, 2, "OrderEntry", "PerformanceMonitorDarling");                 // user blocked by the monitor
            await SeedAsync(report, 3, null, null);                                               // no name recorded: never hidden
            await SeedAsync(report, 4, "PerformanceMonitorDarling", null);
            await SeedAsync(report, 5, "MyPerformanceMonitorTool", "MyPerformanceMonitorTool");    // contains the words, does not begin with them
        }

        Assert.Equal(new[] { 101, 102, 103, 104, 105, 201, 202, 203, 204, 205 }, await AlertBlockedSpidsAsync());
    }
}
