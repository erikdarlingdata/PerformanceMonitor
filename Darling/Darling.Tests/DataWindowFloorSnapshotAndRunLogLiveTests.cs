/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact reads the one scratch database the fixture mints through ScratchPostgres, seeds once, and
   never writes again, so no fact sees a row another class planted, and the scratch database is dropped with the fixture. */
public sealed class SnapshotAndRunLogFloorStore : IAsyncLifetime
{
    public static readonly string[] Relations = ["server_config", "database_config", "collection_log"];

    private ScratchPostgres? _scratch;

    public NpgsqlDataSource? DataSource { get; private set; }

    /// <summary>The minute the store was seeded at: every relative time below is measured back from it.</summary>
    public DateTime SeededAt { get; private set; }

    /// <summary>Days the table keeps its rows: the config snapshots follow the fleet schedule (30), the run log keeps 60.</summary>
    public static int HorizonDays(string relation) => relation == "collection_log" ? 60 : 30;

    public static string TimeColumn(string relation) => relation == "collection_log" ? "collection_time" : "capture_time";

    public static int ServerId(string relation, string scenario) =>
        -496600 - (Array.IndexOf(Relations, relation) * 10 + Array.IndexOf(Scenarios, scenario));

    public static readonly string[] Scenarios = ["new", "covered", "edge", "stale"];

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var now = DateTime.UtcNow;
        SeededAt = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc);
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, CancellationToken.None);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, CancellationToken.None);
            foreach (var relation in Relations)
            {
                await SeedAsync(connection, relation);
            }
        }

        DataSource = NpgsqlDataSource.Create(_scratch.ConnectionString);
    }

    public async ValueTask DisposeAsync()
    {
        if (DataSource is not null)
        {
            await DataSource.DisposeAsync();
        }

        if (_scratch is not null)
        {
            await _scratch.DisposeAsync();
        }
    }

    public static string ServerName(string relation, string scenario) => "floor-" + relation.Replace('_', '-') + "-" + scenario;

    /* One server per scenario. The config snapshots arrive once a day, the run log every half hour. */
    private async Task SeedAsync(NpgsqlConnection connection, string relation)
    {
        var log = relation == "collection_log";
        var step = log ? "30 minutes" : "1 day";
        var edge = SeededAt.AddDays(-HorizonDays(relation));

        /* Added 2 days ago: its first collection is minutes after the registry row, its start is the registry row. */
        await AddServerAsync(connection, relation, "new", SeededAt.AddDays(-2));
        await InsertAsync(connection, relation, "new", log ? SeededAt.AddDays(-2) : SeededAt.AddDays(-2).AddMinutes(1), SeededAt, step);

        /* Monitored for months, rows all the way back to the edge, and a quiet start: the first row inside a 7-day
           window comes 5 hours after the window starts. */
        await AddServerAsync(connection, relation, "covered", SeededAt.AddDays(-120));
        if (log)
        {
            await InsertAsync(connection, relation, "covered", SeededAt.AddDays(-(HorizonDays(relation) - 1)), SeededAt.AddDays(-7).AddHours(-1), step);
            await InsertAsync(connection, relation, "covered", SeededAt.AddDays(-7).AddHours(5), SeededAt, step);
        }
        else
        {
            await InsertAsync(connection, relation, "covered", SeededAt.AddDays(-(HorizonDays(relation) - 1)).AddHours(5), SeededAt, step);
        }

        /* Monitored for 90 days, its oldest surviving row 3 days after the purge edge. */
        await AddServerAsync(connection, relation, "edge", SeededAt.AddDays(-90));
        await InsertAsync(connection, relation, "edge", SeededAt.AddDays(-(HorizonDays(relation) - 3)), SeededAt, step);

        /* The same, plus one row 20 hours older than the edge: the purge drops whole chunks and runs once a day, so
           a row older than the edge can still be there, and the grids still show it. */
        await AddServerAsync(connection, relation, "stale", SeededAt.AddDays(-90));
        await InsertAsync(connection, relation, "stale", edge.AddHours(-20), edge.AddHours(-20), step);
        await InsertAsync(connection, relation, "stale", SeededAt.AddDays(-(HorizonDays(relation) - 3)), SeededAt, step);
    }

    private async Task AddServerAsync(NpgsqlConnection connection, string relation, string scenario, DateTime createdUtc)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId(relation, scenario), ServerName(relation, scenario), CancellationToken.None);
        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None, "UPDATE collect.servers SET created_date = $2 WHERE server_id = $1",
            ServerId(relation, scenario), DarlingMcpTestData.Naive(createdUtc));
    }

    private static async Task InsertAsync(
        NpgsqlConnection connection, string relation, string scenario, DateTime firstUtc, DateTime lastUtc, string step)
    {
        var columns = relation switch
        {
            "server_config" => "INSERT INTO collect.server_config (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use, is_dynamic, is_advanced) "
                + "SELECT row_number() OVER (), t, $1, $2, 'max degree of parallelism', 0, 0, TRUE, TRUE",
            "database_config" => "INSERT INTO collect.database_config (config_id, capture_time, server_id, server_name, database_name) "
                + "SELECT row_number() OVER (), t, $1, $2, 'FloorDb'",
            _ => "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                + "SELECT row_number() OVER (), $1, $2, 'wait_stats', t, 12, 'SUCCESS', 0",
        };

        await DarlingMcpTestData.ExecAsync(
            connection, CancellationToken.None,
            columns + $" FROM generate_series($3::timestamp, $4::timestamp, interval '{step}') AS t",
            ServerId(relation, scenario), ServerName(relation, scenario), DarlingMcpTestData.Naive(firstUtc), DarlingMcpTestData.Naive(lastUtc));
    }
}

/// <summary>
/// Where the config snapshots and the collection log start, for the grids that show them. Every fact checks the
/// answer against the earliest row the grid would show in the same window: a notice never names a time later than
/// that row.
/// </summary>
public sealed class DataWindowFloorSnapshotAndRunLogLiveTests : IClassFixture<SnapshotAndRunLogFloorStore>
{
    private const int TimeoutSeconds = 30;

    private readonly SnapshotAndRunLogFloorStore _store;

    public DataWindowFloorSnapshotAndRunLogLiveTests(SnapshotAndRunLogFloorStore store) => _store = store;

    /// <summary>A server added 2 days ago, read over 7 days: the floor is its start, the registry row.</summary>
    [Theory]
    [InlineData("server_config")]
    [InlineData("database_config")]
    [InlineData("collection_log")]
    public async Task AServerAddedTwoDaysAgo_ReadOverSevenDays_HasItsStartAsTheFloor_AgainstDevPostgres(string relation)
    {
        var (floor, start, end, earliest) = await ProbeAsync(relation, "new", days: 7);

        Assert.NotNull(floor);
        Assert.Equal(_store.SeededAt.AddDays(-2), floor!.Value);
        Assert.True(RawWindowFloor.IsTruncated(floor, start));
        Assert.True(floor <= earliest, $"the floor {floor:O} is later than the earliest row {earliest:O}");

        var byName = await DataWindowFloor.GetAsync(
            _store.DataSource!, [SourceFor(relation)], [SnapshotAndRunLogFloorStore.ServerName(relation, "new")], start, end, TimeoutSeconds, TestContext.Current.CancellationToken);
        Assert.Equal(floor, byName);
    }

    /// <summary>A server monitored for months whose first row in the window comes 5 hours in: the store covered the
    /// whole window, so the floor is at or before its start and nothing is truncated.</summary>
    [Theory]
    [InlineData("server_config")]
    [InlineData("database_config")]
    [InlineData("collection_log")]
    public async Task ACoveredRange_HasNoFloorInsideIt_AgainstDevPostgres(string relation)
    {
        var (floor, start, _, earliest) = await ProbeAsync(relation, "covered", days: 7);

        Assert.NotNull(floor);
        Assert.True(floor <= start, $"the floor {floor:O} is after the window's start {start:O}");
        Assert.False(RawWindowFloor.IsTruncated(floor, start));
        Assert.True(floor <= earliest);
    }

    /// <summary>The purge edge lies inside the range and the oldest surviving row is 3 days after it: the floor is the
    /// edge, the later of the registry row and the schedule's cutoff, not the first row.</summary>
    [Theory]
    [InlineData("server_config")]
    [InlineData("database_config")]
    [InlineData("collection_log")]
    public async Task APurgeEdgeInsideTheRange_IsTheFloor_AgainstDevPostgres(string relation)
    {
        var horizon = SnapshotAndRunLogFloorStore.HorizonDays(relation);
        var (floor, start, _, earliest) = await ProbeAsync(relation, "edge", days: horizon + 15);

        Assert.NotNull(floor);
        Assert.True(floor >= _store.SeededAt.AddDays(-horizon) && floor <= DateTime.UtcNow.AddDays(-horizon), $"the floor {floor:O} is not the edge");
        Assert.True(floor > start);
        Assert.True(floor <= earliest);
    }

    /// <summary>A row older than the edge is still there (whole chunks drop, once a day) and the grids show it: the
    /// floor moves back to that row, so it is never later than the earliest row the surface shows.</summary>
    [Theory]
    [InlineData("server_config")]
    [InlineData("database_config")]
    [InlineData("collection_log")]
    public async Task ARowOlderThanTheEdge_MovesTheFloorBackToIt_AgainstDevPostgres(string relation)
    {
        var horizon = SnapshotAndRunLogFloorStore.HorizonDays(relation);
        var (floor, _, _, earliest) = await ProbeAsync(relation, "stale", days: horizon + 15);

        Assert.NotNull(floor);
        Assert.Equal(_store.SeededAt.AddDays(-horizon).AddHours(-20), floor!.Value);
        Assert.True(floor < DateTime.UtcNow.AddDays(-horizon));
        Assert.Equal(earliest, floor);
    }

    private static DataWindowFloor.Source SourceFor(string relation) =>
        relation == "collection_log" ? DataWindowFloor.Source.ForCollectionLog() : DataWindowFloor.Source.ForCollectorTable(relation);

    private async Task<(DateTime? Floor, DateTime Start, DateTime End, DateTime? Earliest)> ProbeAsync(string relation, string scenario, int days)
    {
        Assert.SkipWhen(_store.DataSource is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live data-start tests.");
        var ct = TestContext.Current.CancellationToken;
        var end = DateTime.UtcNow.AddMinutes(1);
        var start = end.AddDays(-days);
        var serverId = SnapshotAndRunLogFloorStore.ServerId(relation, scenario);

        var floor = await DataWindowFloor.GetForServerAsync(_store.DataSource!, SourceFor(relation), serverId, start, end, TimeoutSeconds, ct);

        /* The earliest row the grid shows in the window: its own read, bounded the same way. */
        var column = SnapshotAndRunLogFloorStore.TimeColumn(relation);
        await using var command = _store.DataSource!.CreateCommand(
            $"SELECT MIN({column}) FROM collect.{relation} WHERE server_id = $1 AND {column} >= $2 AND {column} <= $3");
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(start));
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(end));
        var value = await command.ExecuteScalarAsync(ct);
        return (floor, start, end, value is DateTime earliest ? earliest : null);
    }
}
