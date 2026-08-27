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
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2495, Lite half: <c>as_of</c> moves the END of a windowed read off "now" so a caller can ask about a
/// past incident. Darling's twin (<c>AsOfWindowAnchorLivePostgresTests</c>) pins the same behaviour against
/// live Postgres; this pins it against a real DuckDB through the real tool methods.
///
/// <para>These run through the SERVICE, not just the helper, because the anchor has to survive the whole
/// path — tool parameter, shared validator, <c>LocalDataService</c>'s window helper, and the SQL's two
/// bounds. A helper-only test would pass with the plumbing missing, which is exactly the shape of bug the
/// #2213 review found four of.</para>
/// </summary>
/* The server-local half below reads ServerTimeHelper.UtcOffsetMinutes, a process-wide mutable static that a
   sibling class SETS for its duration. xUnit runs test classes in parallel, so this joins the collection the
   other offset-reading classes already use rather than racing them into a window hours away from its rows. */
[Collection("server-time-helper")]
public sealed class AsOfWindowAnchorTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    /* Outside every default window on the surface (the widest default is 24 hours). The two windows are
       told apart by CONTENT rather than by row count, so a read returning the wrong one cannot look right. */
    private const string IncidentWait = "PAGEIOLATCH_SH";
    private const string RecentWait = "CXPACKET";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -1;
    private DuckDBConnection? _seedConn;

    public AsOfWindowAnchorTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "AsOfAnchorTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);

        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        /* The derived id, not a literal: seeding under a hand-picked number makes the read return nothing
           and the test pass for the wrong reason. */
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

    /// <summary>
    /// The whole point of the issue: the same read, anchored at a past incident, returns the incident —
    /// and on the default anchor it does not.
    /// </summary>
    [Fact]
    public async Task AnAnchoredRead_SeesThePastWindow_AndTheDefaultAnchorDoesNot()
    {
        var now = DateTime.UtcNow;
        var incident = now.AddHours(-30);

        await SeedWaitAsync(incident, IncidentWait, 900_000);
        await SeedWaitAsync(incident.AddMinutes(5), IncidentWait, 800_000);
        await SeedWaitAsync(now.AddMinutes(-10), RecentWait, 1_000);

        var anchor = incident.AddMinutes(30).ToString("o");

        var live = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer");
        Assert.Contains(RecentWait, WaitTypesIn(live));
        Assert.DoesNotContain(IncidentWait, WaitTypesIn(live));

        var anchored = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 4, 20, anchor);
        Assert.Contains(IncidentWait, WaitTypesIn(anchored));
        Assert.DoesNotContain(RecentWait, WaitTypesIn(anchored));
    }

    /// <summary>
    /// Widening <c>hours_back</c> is not the workaround it looks like. A window wide enough to reach the
    /// incident reaches everything since it too, so the aggregate now describes both periods — a different
    /// answer, not the same answer with more rows.
    /// </summary>
    [Fact]
    public async Task WideningTheWindow_IsADifferentAnswer_NotTheSameOneWithMoreRows()
    {
        var now = DateTime.UtcNow;
        var incident = now.AddHours(-30);

        await SeedWaitAsync(incident, IncidentWait, 900_000);
        await SeedWaitAsync(now.AddMinutes(-10), RecentWait, 1_000);

        var anchored = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 4, 20, incident.AddMinutes(30).ToString("o"));
        var widened = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 48);

        Assert.Equal(new[] { IncidentWait }, WaitTypesIn(anchored));
        Assert.Equal(2, WaitTypesIn(widened).Length);
        Assert.Contains(IncidentWait, WaitTypesIn(widened));
        Assert.Contains(RecentWait, WaitTypesIn(widened));
    }

    /// <summary>
    /// Backward compatibility, stated as a test rather than as a claim: a caller that sends only
    /// <c>hours_back</c> gets the window it always got. Seeded either side of the boundary so a window that
    /// silently moved would show up as content, not as a timing wobble.
    /// </summary>
    [Fact]
    public async Task WithNoAnchor_TheWindowIsStillTheLastHoursBackHours()
    {
        var now = DateTime.UtcNow;
        await SeedWaitAsync(now.AddHours(-2), RecentWait, 5_000);
        await SeedWaitAsync(now.AddHours(-10), IncidentWait, 5_000);

        Assert.Equal(new[] { RecentWait }, WaitTypesIn(await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 4)));

        var both = WaitTypesIn(await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 24));
        Assert.Contains(RecentWait, both);
        Assert.Contains(IncidentWait, both);
    }

    /// <summary>
    /// An anchor the read cannot use is refused with a message. It must NOT quietly answer as of now — that
    /// answer is indistinguishable from a correct one, which is the failure this parameter exists to remove.
    /// </summary>
    [Fact]
    public async Task AnUnusableAnchor_IsRefused_RatherThanAnsweredAsOfNow()
    {
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-10), RecentWait, 1_000);

        var garbage = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 4, 20, "last tuesday");
        Assert.StartsWith("Invalid as_of", garbage, StringComparison.Ordinal);
        Assert.DoesNotContain(RecentWait, garbage, StringComparison.Ordinal);

        /* The one a general date parser would let through: "01/02/2026" is M/d/yyyy under the invariant
           culture, so a caller who meant 1 February would silently get a window around 2 January. */
        var ambiguous = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 4, 20, "01/02/2026");
        Assert.StartsWith("Invalid as_of", ambiguous, StringComparison.Ordinal);

        var future = await McpWaitTools.GetWaitStats(
            _dataService, _serverManager, "TestServer", 4, 20, DateTime.UtcNow.AddDays(1).ToString("o"));
        Assert.Contains("future", future, StringComparison.Ordinal);
        Assert.DoesNotContain(RecentWait, future, StringComparison.Ordinal);
    }

    /// <summary>
    /// An anchor older than anything the store holds is NOT refused — the store's earliest row is
    /// per-server and per-collector, so a floor would be a guess. It comes back as the read's own honest
    /// miss, which is unambiguous because the caller chose the window.
    /// </summary>
    [Fact]
    public async Task AnAnchorBeforeAnyData_ReturnsTheReadsOwnMiss_NotARefusal()
    {
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-10), RecentWait, 1_000);

        var ancient = await McpWaitTools.GetWaitStats(_dataService, _serverManager, "TestServer", 4, 20, "1999-01-01T00:00:00Z");

        using var doc = JsonDocument.Parse(ancient);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
    }

    /// <summary>
    /// The anchor reaches a read whose window is built by the SERVER-LOCAL helper, not just the UTC one.
    /// Two families, two time bases, one instant — a bug here would shift the window by the monitored
    /// server's offset and still look plausible.
    ///
    /// <para><b>The offset is SET, not read</b> (review catch). Reading whatever
    /// <c>ServerTimeHelper.UtcOffsetMinutes</c> happens to be means running at 0 on any normal machine, and
    /// at 0 a correct conversion and no conversion at all produce identical results — the test would pass
    /// vacuously for exactly the bug its own summary names. UTC+5:30 is used because a half-hour offset also
    /// catches an implementation that rounds to whole hours. Saved and restored, and this class shares the
    /// <c>server-time-helper</c> collection so the write cannot land under a sibling class mid-read.</para>
    /// </summary>
    [Fact]
    public async Task TheAnchorAlsoMoves_AServerLocalWindow()
    {
        var savedOffset = ServerTimeHelper.UtcOffsetMinutes;
        try
        {
            ServerTimeHelper.UtcOffsetMinutes = 330; // UTC+5:30

            var now = DateTime.UtcNow;
            var incident = now.AddHours(-30);

            /* sample_time is the monitored server's LOCAL wall clock, so the fixture is seeded in that base —
               the read's window is server-local and the anchor arrives in UTC, and the point of the test is
               that the conversion between them happens exactly once. */
            var offset = ServerTimeHelper.UtcOffsetMinutes;
            await SeedCpuAsync(incident.AddMinutes(offset), 91);
            await SeedCpuAsync(now.AddMinutes(-10).AddMinutes(offset), 12);

            var anchored = await McpCpuTools.GetCpuUtilization(
                _dataService, _serverManager, "TestServer", 4, incident.AddMinutes(30).ToString("o"));
            Assert.Equal(new[] { 91 }, SqlCpuIn(anchored));

            var live = await McpCpuTools.GetCpuUtilization(_dataService, _serverManager, "TestServer");
            Assert.Equal(new[] { 12 }, SqlCpuIn(live));
        }
        finally
        {
            ServerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    // ── helpers ──

    private static string[] WaitTypesIn(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("waits").EnumerateArray()
            .Select(w => w.GetProperty("wait_type").GetString()!)
            .ToArray();
    }

    private static int[] SqlCpuIn(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("samples").EnumerateArray()
            .Select(s => s.GetProperty("sql_server_cpu").GetInt32())
            .ToArray();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task SeedWaitAsync(DateTime at, string waitType, long deltaMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO wait_stats
            (collection_id, collection_time, server_id, server_name, wait_type,
             waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
             delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
            VALUES ($1, $2, $3, 'TestServer', $4, 0, 0, 0, 50, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(waitType);
        P(deltaMs);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedCpuAsync(DateTime at, int sqlCpu)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time,
             sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ($1, $2, $3, 'TestServer', $4, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(at);
        P(sqlCpu);
        await cmd.ExecuteNonQueryAsync();
    }
}
