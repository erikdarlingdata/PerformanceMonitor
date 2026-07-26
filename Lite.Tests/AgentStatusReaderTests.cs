/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Real-DuckDB round-trip pins for <c>LocalDataService.GetAgentStatusAsync</c>'s two #1720 additions:
/// <c>collection_time</c> (so the header can refuse to judge a stale reading) and the derived
/// <c>ever_running</c> flag (so a server that has never run Agent is never called "stopped").
///
/// <para>The classifier is unit-tested in <see cref="AgentHeaderStatusTests"/> against constructed rows;
/// these exist because that cannot prove the SQL. The window aggregate that derives ever_running has to
/// look across the WHOLE retained partition while the surrounding query collapses to the newest row per
/// server, which is exactly the kind of thing that silently returns the newest row's own value instead.</para>
/// </summary>
public sealed class AgentStatusReaderTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AgentStatusReaderTests(SharedDuckDbFixture fixture)
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

    private async Task SeedAsync(int serverId, string serverName, DateTime collectionTimeUtc, bool agentRunning)
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
        cmd.Parameters.Add(new DuckDBParameter { Value = agentRunning ? "Running" : "Stopped" });
        cmd.Parameters.Add(new DuckDBParameter { Value = "Automatic" });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task EverSeenRunning_IsTrue_WhenAnEarlierRowRan_EvenThoughTheNewestIsStopped()
    {
        /* The case the whole gate turns on: Agent ran, then stopped. The newest row says false; the history
           says this server genuinely uses Agent, so the header must still call it Stopped. */
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;

        await SeedAsync(4101, "REAL", now.AddHours(-4), agentRunning: true);
        await SeedAsync(4101, "REAL", now.AddMinutes(-3), agentRunning: false);

        var rows = await service.GetAgentStatusAsync(4101);

        var row = Assert.Single(rows);
        Assert.False(row.AgentRunning);
        Assert.True(row.EverSeenRunning);
        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Stopped, AgentHeaderStatus.Classify(row, now));
    }

    [Fact]
    public async Task EverSeenRunning_IsFalse_WhenNoRetainedRowEverRan()
    {
        /* A container built without Agent, Express, Azure SQL DB: every retained row is false. */
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;

        await SeedAsync(4102, "CONTAINER", now.AddHours(-4), agentRunning: false);
        await SeedAsync(4102, "CONTAINER", now.AddHours(-1), agentRunning: false);
        await SeedAsync(4102, "CONTAINER", now.AddMinutes(-3), agentRunning: false);

        var rows = await service.GetAgentStatusAsync(4102);

        var row = Assert.Single(rows);
        Assert.False(row.EverSeenRunning);
        Assert.Equal(AgentHeaderStatus.AgentHeaderState.NeverObserved, AgentHeaderStatus.Classify(row, now));
    }

    [Fact]
    public async Task CollectionTime_IsProjected_AndDrivesStaleness()
    {
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;

        await SeedAsync(4103, "LAGGED", now.AddDays(-2), agentRunning: true);
        await SeedAsync(4103, "LAGGED", now.AddHours(-6), agentRunning: false);

        var rows = await service.GetAgentStatusAsync(4103);

        var row = Assert.Single(rows);
        Assert.NotNull(row.CollectionTime);
        /* Newest row wins, and it is far outside the stale window. */
        Assert.True(now - row.CollectionTime!.Value > AgentHeaderStatus.StaleWindow);
        Assert.Equal(AgentHeaderStatus.AgentHeaderState.Unknown, AgentHeaderStatus.Classify(row, now));
    }

    [Fact]
    public async Task EverSeenRunning_IsPerServer_NotLeakedAcrossThePartition()
    {
        /* The window aggregate must partition by server: one server's running history must not make a
           different, Agent-less server look like it uses Agent. */
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;

        await SeedAsync(4104, "USES-AGENT", now.AddHours(-2), agentRunning: true);
        await SeedAsync(4104, "USES-AGENT", now.AddMinutes(-2), agentRunning: false);
        await SeedAsync(4105, "NO-AGENT", now.AddHours(-2), agentRunning: false);
        await SeedAsync(4105, "NO-AGENT", now.AddMinutes(-2), agentRunning: false);

        var rows = await service.GetAgentStatusAsync();

        var usesAgent = Assert.Single(rows, r => r.ServerId == 4104);
        var noAgent = Assert.Single(rows, r => r.ServerId == 4105);

        Assert.True(usesAgent.EverSeenRunning);
        Assert.False(noAgent.EverSeenRunning);
    }

    [Fact]
    public async Task FleetRead_ReturnsNewestRowPerServer_WithBothNewColumns()
    {
        var service = new LocalDataService(_duckDb);
        var now = DateTime.UtcNow;

        await SeedAsync(4106, "SQL-A", now.AddHours(-3), agentRunning: true);
        await SeedAsync(4106, "SQL-A", now.AddMinutes(-1), agentRunning: true);
        await SeedAsync(4107, "SQL-B", now.AddMinutes(-1), agentRunning: false);

        var rows = await service.GetAgentStatusAsync();

        var a = Assert.Single(rows, r => r.ServerId == 4106);
        var b = Assert.Single(rows, r => r.ServerId == 4107);

        Assert.True(a.AgentRunning);
        Assert.True(a.EverSeenRunning);
        Assert.NotNull(a.CollectionTime);

        Assert.False(b.AgentRunning);
        Assert.False(b.EverSeenRunning);
        Assert.NotNull(b.CollectionTime);

        /* One never ran Agent and the other is fine, so the roll-up must report no problems. */
        var (text, isAlert) = AgentHeaderStatus.DescribeFleet(rows, now);
        Assert.False(isAlert);
        Assert.DoesNotContain("stopped", text, StringComparison.OrdinalIgnoreCase);
    }
}
