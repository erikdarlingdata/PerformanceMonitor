/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Real-DuckDB round-trips for the two columns #1725 added to
/// <c>LocalDataService.GetAgentStatusAsync</c>: <c>collection_time</c> and the derived
/// <c>ever_seen_running</c>.
///
/// <para><c>AgentStatusHeaderHonestyTests</c> pins the DECISION — what a row means once you have it — against
/// constructed rows, which is the right shape for that half. Nothing yet proved the SQL that produces those
/// rows, and it is not trivial SQL: <c>ever_seen_running</c> is a window aggregate that must see the WHOLE
/// retained partition while the surrounding query collapses to the newest row per server. The two ways that
/// goes wrong both return a plausible boolean and both pass every model-level test — it silently reports the
/// newest row's own <c>agent_running</c> (so a server that ran Agent yesterday and stopped today reads "never
/// ran Agent", and a genuine outage renders neutral), or the partition leaks (so an Agent-less container
/// inherits a real server's history and goes back to being called "Stopped"). Each case below fails against
/// one of those.</para>
/// </summary>
public sealed class AgentStatusReaderSqlTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AgentStatusReaderSqlTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private static DateTime Truncate(DateTime t) =>
        new(t.Year, t.Month, t.Day, t.Hour, t.Minute, t.Second, DateTimeKind.Unspecified);

    private async Task SeedAsync(int serverId, string serverName, DateTime collectionTimeUtc, bool agentRunning, string? statusDesc = null, bool nullStartup = false)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO agent_status
    (collection_id, collection_time, server_id, server_name, agent_running, agent_status_desc, agent_startup_desc, next_scheduled_run)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Truncate(collectionTimeUtc) });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverName });
        cmd.Parameters.Add(new DuckDBParameter { Value = agentRunning });
        cmd.Parameters.Add(new DuckDBParameter { Value = statusDesc ?? (agentRunning ? "Running" : "Stopped") });
        cmd.Parameters.Add(new DuckDBParameter { Value = nullStartup ? (object)DBNull.Value : "Automatic" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EverSeenRunning_ReadsTheHistory_NotJustTheNewestRow()
    {
        /* Agent ran, then stopped — the case the whole gate turns on. The newest row says false; the retained
           history says this server genuinely runs Agent, so it must still render as a real problem. A window
           aggregate that collapsed to the newest row would call this "not present" and paint it neutral,
           silently swallowing a genuine outage. */
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5101, "REAL", DateTime.UtcNow.AddHours(-4), agentRunning: true);
        await SeedAsync(5101, "REAL", DateTime.UtcNow.AddMinutes(-3), agentRunning: false);

        var row = Assert.Single(await service.GetAgentStatusAsync(5101));

        Assert.False(row.AgentRunning);
        Assert.True(row.EverSeenRunning);
        Assert.False(row.IsStale);
        Assert.True(row.IsAgentProblem);
        Assert.Equal("Stopped", row.StatusDisplay);
    }

    [Fact]
    public async Task AManagedInstanceSnapshot_ReadsRunning_NotNotPresent()
    {
        /* #6886: Managed Instance returns no sys.dm_server_services rows, so the collector used to store the
           Agent-less shape (false, NULL descriptions) and Lite drew "not present". Its query now stores running
           with the platform-managed description, and the header reads Running with no problem treatment. */
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5105, "MI", DateTime.UtcNow.AddMinutes(-3), agentRunning: true,
            statusDesc: "Running (managed by Azure SQL Managed Instance)", nullStartup: true);

        var row = Assert.Single(await service.GetAgentStatusAsync(5105));

        Assert.True(row.AgentRunning);
        Assert.Equal("Running (managed by Azure SQL Managed Instance)", row.AgentStatusDesc);
        Assert.Null(row.AgentStartupDesc);
        Assert.Equal("Running", row.StatusDisplay);
        Assert.False(row.IsAgentProblem);
    }

    [Fact]
    public async Task EverSeenRunning_IsFalse_WhenNoRetainedRowEverRan()
    {
        /* A container built without Agent, Express, Azure SQL DB: every retained row is false. */
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5102, "CONTAINER", DateTime.UtcNow.AddHours(-4), agentRunning: false);
        await SeedAsync(5102, "CONTAINER", DateTime.UtcNow.AddHours(-1), agentRunning: false);
        await SeedAsync(5102, "CONTAINER", DateTime.UtcNow.AddMinutes(-3), agentRunning: false);

        var row = Assert.Single(await service.GetAgentStatusAsync(5102));

        Assert.False(row.EverSeenRunning);
        Assert.False(row.IsAgentProblem);
        Assert.Equal("not present", row.StatusDisplay);
    }

    [Fact]
    public async Task EverSeenRunning_IsPerServer_AndDoesNotLeakAcrossThePartition()
    {
        /* Without PARTITION BY server_id the aggregate spans every row in the table, and one real server's
           history would make every Agent-less target look like it runs Agent — putting the red "Stopped"
           straight back on the containers this was meant to stop nagging about. */
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5103, "USES-AGENT", DateTime.UtcNow.AddHours(-2), agentRunning: true);
        await SeedAsync(5103, "USES-AGENT", DateTime.UtcNow.AddMinutes(-2), agentRunning: false);
        await SeedAsync(5104, "NO-AGENT", DateTime.UtcNow.AddHours(-2), agentRunning: false);
        await SeedAsync(5104, "NO-AGENT", DateTime.UtcNow.AddMinutes(-2), agentRunning: false);

        var rows = await service.GetAgentStatusAsync();

        var usesAgent = Assert.Single(rows, r => r.ServerId == 5103);
        var noAgent = Assert.Single(rows, r => r.ServerId == 5104);

        Assert.True(usesAgent.EverSeenRunning);
        Assert.True(usesAgent.IsAgentProblem);

        Assert.False(noAgent.EverSeenRunning);
        Assert.False(noAgent.IsAgentProblem);
    }

    [Fact]
    public async Task CollectionTime_IsProjected_AndTheNewestRowWins()
    {
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5105, "LAGGED", DateTime.UtcNow.AddDays(-2), agentRunning: true);
        await SeedAsync(5105, "LAGGED", DateTime.UtcNow.AddHours(-6), agentRunning: false);

        var row = Assert.Single(await service.GetAgentStatusAsync(5105));

        Assert.NotNull(row.CollectionTime);
        Assert.True(row.IsStale);
        Assert.False(row.IsAgentProblem);
        Assert.Equal("unknown (stale)", row.StatusDisplay);
    }

    [Fact]
    public async Task FreshReadingIsNotStale_SoTheGateOnlyBitesOnOldData()
    {
        /* The other side of the staleness gate: a current reading must NOT be suppressed. */
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5106, "FRESH", DateTime.UtcNow.AddMinutes(-1), agentRunning: true);

        var row = Assert.Single(await service.GetAgentStatusAsync(5106));

        Assert.False(row.IsStale);
        Assert.Equal("Running", row.StatusDisplay);
    }

    [Fact]
    public async Task FleetRead_ReturnsNewestRowPerServer_WithBothColumnsPopulated()
    {
        var service = new LocalDataService(_duckDb);

        await SeedAsync(5107, "SQL-A", DateTime.UtcNow.AddHours(-3), agentRunning: true);
        await SeedAsync(5107, "SQL-A", DateTime.UtcNow.AddMinutes(-1), agentRunning: true);
        await SeedAsync(5108, "SQL-B", DateTime.UtcNow.AddMinutes(-1), agentRunning: false);

        var rows = await service.GetAgentStatusAsync();

        var a = Assert.Single(rows, r => r.ServerId == 5107);
        var b = Assert.Single(rows, r => r.ServerId == 5108);

        Assert.True(a.AgentRunning);
        Assert.True(a.EverSeenRunning);
        Assert.NotNull(a.CollectionTime);
        Assert.False(a.IsAgentProblem);

        Assert.False(b.AgentRunning);
        Assert.False(b.EverSeenRunning);
        Assert.NotNull(b.CollectionTime);
        Assert.False(b.IsAgentProblem);
    }

    [Fact]
    public void StaleWindow_StaysLongerThanTheCollectorsOwnCadence()
    {
        /* A stale window at or under the collector's cadence would render a perfectly healthy Agent as
           "unknown (stale)" most of the time. This is a live trap rather than a hypothetical: the shared
           ServerHealthThresholds.StaleThreshold is two minutes, derived from the FASTEST collector's
           one-minute cadence, and looks like the obvious thing to unify this with — but agent_status collects
           every five, so adopting it would break the header. Pinned against the schedule default so the guard
           follows the cadence if it is ever retuned. */
        var cadence = TimeSpan.FromMinutes(CollectorScheduleDefaults.All["agent_status"].FrequencyMinutes);

        Assert.True(
            AgentStatusRow.StaleWindow > cadence,
            $"StaleWindow ({AgentStatusRow.StaleWindow}) must exceed the agent_status cadence ({cadence}).");
    }
}
