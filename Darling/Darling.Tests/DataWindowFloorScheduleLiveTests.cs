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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the one scratch database this fixture mints through ScratchPostgres holds two servers and the run-log rows
   that make them count; every fact below rewrites only the fleet schedule row of the collector it probes, and the scratch
   database is dropped with the fixture. */
public sealed class ScheduleFloorStore : IAsyncLifetime
{
    public const int OldServerId = -496801;
    public const int NewServerId = -496802;

    public static readonly string[] Tables = ["waiting_tasks", "query_snapshots", "server_config", "database_config"];

    private ScratchPostgres? _scratch;

    public NpgsqlDataSource? DataSource { get; private set; }

    /// <summary>The "old" server was registered 4500 days ago (older than the retention cap), so only the schedule's horizon
    /// can set its coverage start. The "new" server was registered 2 days ago.</summary>
    public DateTime NewServerCreated { get; private set; }

    public async ValueTask InitializeAsync()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        if (string.IsNullOrEmpty(baseConnectionString))
        {
            return;
        }

        var ct = CancellationToken.None;
        var now = DateTime.UtcNow;
        NewServerCreated = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, 0, DateTimeKind.Utc).AddDays(-2);
        _scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
        await using (var connection = new NpgsqlConnection(_scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await DarlingMcpTestData.RegisterServerAsync(connection, OldServerId, "schedule-floor-old", ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.servers SET created_date = $2 WHERE server_id = $1",
                OldServerId, DarlingMcpTestData.Naive(now.AddDays(-4500)));
            await DarlingMcpTestData.RegisterServerAsync(connection, NewServerId, "schedule-floor-new", ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, "UPDATE collect.servers SET created_date = $2 WHERE server_id = $1",
                NewServerId, DarlingMcpTestData.Naive(NewServerCreated));

            /* A logged run of the collector an hour ago makes each server count for each table, with no row in the table itself. */
            var logId = 0;
            foreach (var table in Tables)
            {
                foreach (var serverId in new[] { OldServerId, NewServerId })
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct,
                        "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) "
                        + "VALUES ($1, $2, 'schedule-floor', $3, $4, 12, 'SUCCESS', 0)",
                        -496800 - (++logId), serverId, table, DarlingMcpTestData.Naive(now.AddHours(-1)));
                }
            }

            /* The retention cap check is NOT VALID (it guards new writes only); a row stored before it existed can be over the cap, so
               the over-cap fact drops it to seed one. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "ALTER TABLE config.config_collector_schedules DROP CONSTRAINT IF EXISTS ck_config_collector_schedules_retention_cap");
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
}

/// <summary>
/// The data-window probe's edge for a table the collector schedule governs, read from a real store: the fleet schedule
/// row's retention sets how many days back the edge lies (capped at <see cref="CollectorScheduleDefaults.MaxRetentionDays"/>,
/// as the purge caps it), the shipped default applies with no row, and the server's registration is the other bound.
/// </summary>
public sealed class DataWindowFloorScheduleLiveTests : IClassFixture<ScheduleFloorStore>
{
    private const int TimeoutSeconds = 30;
    private static readonly TimeSpan Slack = TimeSpan.FromMinutes(5);

    private readonly ScheduleFloorStore _store;

    public DataWindowFloorScheduleLiveTests(ScheduleFloorStore store) => _store = store;

    [Theory]
    [InlineData("waiting_tasks", 7)]
    [InlineData("query_snapshots", 7)]
    [InlineData("server_config", 30)]
    [InlineData("database_config", 30)]
    public async Task WithNoScheduleRow_TheEdgeIsTheShippedDefaultNumberOfDaysBack_AgainstDevPostgres(string table, int defaultDays)
    {
        await SetFleetRetentionAsync(table, null);

        var floor = await ProbeAsync(table, ScheduleFloorStore.OldServerId);

        AssertDaysBack(defaultDays, floor);
    }

    [Theory]
    [InlineData("waiting_tasks")]
    [InlineData("query_snapshots")]
    [InlineData("server_config")]
    [InlineData("database_config")]
    public async Task AFleetRetentionRow_MovesTheEdgeToThatManyDaysBack_AgainstDevPostgres(string table)
    {
        await SetFleetRetentionAsync(table, 3);
        AssertDaysBack(3, await ProbeAsync(table, ScheduleFloorStore.OldServerId));

        await SetFleetRetentionAsync(table, 90);
        AssertDaysBack(90, await ProbeAsync(table, ScheduleFloorStore.OldServerId));
    }

    [Theory]
    [InlineData("waiting_tasks")]
    [InlineData("server_config")]
    public async Task AServerRegisteredAfterTheEdge_StartsAtItsRegistration_AgainstDevPostgres(string table)
    {
        await SetFleetRetentionAsync(table, 3);

        var floor = await ProbeAsync(table, ScheduleFloorStore.NewServerId);

        Assert.Equal(_store.NewServerCreated, floor);
    }

    [Theory]
    [InlineData("waiting_tasks")]
    [InlineData("database_config")]
    public async Task ARetentionAboveTheCap_MovesTheEdgeNoFartherBackThanTheCap_AgainstDevPostgres(string table)
    {
        /* 5000 days is past the cap and past the old server's registration (4500 days back): an edge that used the stored value
           would land on the registration, one that caps it lands exactly the cap back. */
        await SetFleetRetentionAsync(table, 5000);

        var floor = await ProbeAsync(table, ScheduleFloorStore.OldServerId);

        AssertDaysBack(CollectorScheduleDefaults.MaxRetentionDays, floor);
    }

    private static void AssertDaysBack(int days, DateTime? floor)
    {
        Assert.NotNull(floor);
        var expected = DateTime.UtcNow.AddDays(-days);
        Assert.True(
            (floor!.Value - expected).Duration() < Slack,
            $"the edge {floor:O} is not {days} days back (about {expected:O})");
    }

    private async Task SetFleetRetentionAsync(string table, int? days)
    {
        Assert.SkipWhen(_store.DataSource is null, "Set DARLING_TEST_PG to a Postgres connection string to run the live data-start tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await _store.DataSource!.OpenConnectionAsync(ct);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "DELETE FROM config.config_collector_schedules WHERE server_id IS NULL AND collector_name = $1", table);
        if (days is int retention)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO config.config_collector_schedules (server_id, collector_name, enabled, retention_days) VALUES (NULL, $1, TRUE, $2)",
                table, retention);
        }
    }

    /* A window of ten days: the server counts through its logged run an hour ago, and the table holds no row, so the answer is the
       coverage start the schedule and the registration give. */
    private Task<DateTime?> ProbeAsync(string table, int serverId)
    {
        var end = DateTime.UtcNow.AddMinutes(1);
        return DataWindowFloor.GetForServerAsync(
            _store.DataSource!, DataWindowFloor.Source.ForCollectorTable(table), serverId, end.AddDays(-10), end, TimeoutSeconds,
            TestContext.Current.CancellationToken);
    }
}
