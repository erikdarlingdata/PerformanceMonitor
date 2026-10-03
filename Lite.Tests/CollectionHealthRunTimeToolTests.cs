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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4938: <c>get_collection_health</c> shows a collector's run time. A collector with a run time carries
/// <c>run_at</c> (24-hour <c>HH:MM</c> on the monitored server's clock) and <c>next_run_utc</c> (the next time it is due,
/// UTC, from the shared rules). A collector without one carries null for both on a full row, and a compact row leaves
/// the keys out. The health band is read from the shipped cadence and a run time does not move it, so a day that was
/// skipped reads STALE before the next run. A server with no clock yet reads the run time as UTC. These are the same
/// two fields Darling's tool publishes.
/// </summary>
public sealed class CollectionHealthRunTimeToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RunTimeHealthSrv";
    private const string Daily = "index_object_stats";
    private const string Frequent = "wait_stats";

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly ServerConnection _server;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public CollectionHealthRunTimeToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-collhealthruntime-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        _server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(_server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(_server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /* A run time half a day away from now, on UTC (the test server has no clock row), so the slot is never inside the
       hour the collector would read as due now, whatever hour the test runs in. */
    private static (int Minute, string Text) RunTimeAwayFromNow()
    {
        var minute = (DateTime.UtcNow.Hour + 12) % 24 * 60;
        return (minute, CollectorRunTime.Format(minute));
    }

    private McpCollectorRunTimes RunTimes(params (string Name, int Frequency, string? RunAt, bool Enabled)[] collectors)
    {
        var scheduler = new ScheduleManager(_configDir);
        scheduler.SetScheduleForServer(_server.Id, collectors.Select(c => new CollectorSchedule
        {
            Name = c.Name,
            Enabled = c.Enabled,
            FrequencyMinutes = c.Frequency,
            RetentionDays = 30,
            RunAt = c.RunAt,
        }).ToList());
        return new McpCollectorRunTimes(scheduler, _serverManager);
    }

    private async Task<(Dictionary<string, JsonElement> Rows, JsonElement Root)> CallAsync(McpCollectorRunTimes? runTimes, bool fullDetail)
    {
        var json = await McpHealthTools.GetCollectionHealth(
            new LocalDataService(_duckDb), _serverManager, ServerName, full_detail: fullDetail, runTimes: runTimes);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement.Clone();
        var rows = root.GetProperty("collectors").EnumerateArray()
            .ToDictionary(e => e.GetProperty("collector").GetString()!, e => e.Clone());
        return (rows, root);
    }

    private static DateTime ParseUtc(JsonElement value) =>
        DateTime.Parse(value.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    [Fact]
    public async Task ACollectorWithARunTime_ShowsRunAtAndNextRunUtc_AndACollectorWithoutOneShowsNullForBoth()
    {
        var now = DateTime.UtcNow;
        var (minute, text) = RunTimeAwayFromNow();
        await SeedLogAsync(Daily, now.AddMinutes(-1), "SUCCESS", 5000, 10);
        await SeedLogAsync(Frequent, now.AddMinutes(-1), "SUCCESS", 80, 40);
        var runTimes = RunTimes((Daily, 1440, text, true), (Frequent, 1, null, true));

        var (rows, root) = await CallAsync(runTimes, fullDetail: true);

        var daily = rows[Daily];
        Assert.Equal(text, daily.GetProperty("run_at").GetString());
        var next = ParseUtc(daily.GetProperty("next_run_utc"));
        Assert.True(next > now, $"the next run {next:o} must be ahead of {now:o}");
        Assert.True(next <= now + CollectorRunTime.MaxStampAhead(1440), $"the next run {next:o} is more than a day and the spread away");

        /* No server_properties row, so the clock is unknown and the run time reads as UTC: the time plus the server's fixed spread. */
        Assert.Equal(TimeSpan.FromTicks((TimeSpan.FromMinutes(minute) + CollectorRunTime.Spread(_serverId)).Ticks % TimeSpan.TicksPerDay), next.TimeOfDay);

        Assert.Equal(JsonValueKind.Null, rows[Frequent].GetProperty("run_at").ValueKind);
        Assert.Equal(JsonValueKind.Null, rows[Frequent].GetProperty("next_run_utc").ValueKind);

        /* The heaviest-collectors list names the run time beside the shipped cadence, and null for a collector without one. */
        var heaviest = root.GetProperty("sweep_pressure").GetProperty("heaviest_collectors").EnumerateArray()
            .ToDictionary(e => e.GetProperty("collector").GetString()!, e => e.Clone());
        Assert.Equal(text, heaviest[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.Null, heaviest[Frequent].GetProperty("run_at").ValueKind);
    }

    [Fact]
    public void TheRunTimeIsOnTheServersClock_AndAServerWithNoClockReadsItAsUtc()
    {
        var settings = new[] { new ScheduleManager.RunTimeSetting(Daily, 120, 1440, true) };
        var neverRan = new Dictionary<string, DateTime?>();
        var now = new DateTime(2026, 7, 15, 12, 0, 0, DateTimeKind.Utc);

        var plain = McpCollectorRunTimes.Compute(_serverId, settings, neverRan, clock: null, now)[Daily];
        var fiveBehind = McpCollectorRunTimes.Compute(_serverId, settings, neverRan, ServerClock.Resolve(null, -300), now)[Daily];

        Assert.Equal("02:00", plain.RunAt);
        Assert.Equal("02:00", fiveBehind.RunAt);
        Assert.Equal(TimeSpan.FromHours(2) + CollectorRunTime.Spread(_serverId), plain.NextRunUtc!.Value.TimeOfDay);

        /* Five hours behind UTC, a fixed offset: its 02:00 is 07:00 UTC. */
        Assert.Equal(TimeSpan.FromHours(7) + CollectorRunTime.Spread(_serverId), fiveBehind.NextRunUtc!.Value.TimeOfDay);
    }

    [Fact]
    public async Task ADisabledCollectorWithARunTime_ShowsItsRunTime_AndNoNextRun()
    {
        var (_, text) = RunTimeAwayFromNow();
        await SeedLogAsync(Daily, DateTime.UtcNow.AddMinutes(-1), "SUCCESS", 5000, 10);

        var (rows, _) = await CallAsync(RunTimes((Daily, 1440, text, false)), fullDetail: true);

        Assert.Equal(text, rows[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.Null, rows[Daily].GetProperty("next_run_utc").ValueKind);
    }

    [Fact]
    public async Task ACompactRow_CarriesTheRunTimeOnlyWhenTheCollectorHasOne()
    {
        var now = DateTime.UtcNow;
        var (_, text) = RunTimeAwayFromNow();
        await SeedLogAsync(Daily, now.AddMinutes(-30), "SUCCESS", 5000, 10);
        await SeedLogAsync(Frequent, now.AddMinutes(-1), "SUCCESS", 80, 40);

        var (rows, _) = await CallAsync(RunTimes((Daily, 1440, text, true), (Frequent, 1, null, true)), fullDetail: false);

        Assert.True(rows[Daily].GetProperty("compact").GetBoolean());
        Assert.Equal(text, rows[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.String, rows[Daily].GetProperty("next_run_utc").ValueKind);

        Assert.True(rows[Frequent].GetProperty("compact").GetBoolean());
        Assert.False(rows[Frequent].TryGetProperty("run_at", out _), "a compact row without a run time stays seven fields");
        Assert.False(rows[Frequent].TryGetProperty("next_run_utc", out _));
    }

    [Fact]
    public async Task ASkippedDay_CrossesTheStaleLineBeforeTheNextRun_AndTheBandIsTheSameWithoutARunTime()
    {
        var now = DateTime.UtcNow;
        var (_, text) = RunTimeAwayFromNow();

        /* A daily collector that last ran 40 hours ago is past the 36-hour stale line: a day was skipped. The next run is
           still hours away, so the row reads STALE before it. The run time changes nothing about the band. */
        await SeedLogAsync(Daily, now.AddHours(-40), "SUCCESS", 5000, 10);

        var (withRunTime, _) = await CallAsync(RunTimes((Daily, 1440, text, true)), fullDetail: true);
        var (without, _) = await CallAsync(runTimes: null, fullDetail: true);

        Assert.Equal("STALE", withRunTime[Daily].GetProperty("status").GetString());
        Assert.Equal(without[Daily].GetProperty("status").GetString(), withRunTime[Daily].GetProperty("status").GetString());
        Assert.Equal(JsonValueKind.Null, without[Daily].GetProperty("run_at").ValueKind);
        Assert.True(ParseUtc(withRunTime[Daily].GetProperty("next_run_utc")) > now.AddHours(1),
            "the skipped day's collector waits for its next slot, not for the read");

        /* The default shape of a stale row is the partial one: it carries the run time when the collector has one and
           leaves the keys out when it has none. */
        var (partial, _) = await CallAsync(RunTimes((Daily, 1440, text, true)), fullDetail: false);
        Assert.True(partial[Daily].GetProperty("partial_detail").GetBoolean());
        Assert.Equal(text, partial[Daily].GetProperty("run_at").GetString());
        Assert.Equal(JsonValueKind.String, partial[Daily].GetProperty("next_run_utc").ValueKind);

        var (partialWithout, _) = await CallAsync(runTimes: null, fullDetail: false);
        Assert.False(partialWithout[Daily].TryGetProperty("run_at", out _));
        Assert.False(partialWithout[Daily].TryGetProperty("next_run_utc", out _));
    }

    private async Task SeedLogAsync(string collector, DateTime collectionTimeUtc, string status, double durationMs, int? rowsCollected)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)rowsCollected ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.8 });
        cmd.Parameters.Add(new DuckDBParameter { Value = durationMs * 0.2 });
        await cmd.ExecuteNonQueryAsync();
    }
}
