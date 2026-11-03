/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7196: a SQL Server's days-old ring-buffer CPU reading is not shown or banded as current. Two SQL Server
/// targets stay Online (a fresh <c>collection_log</c> row, as any other running collector would leave), one with
/// a CPU sample a minute old and one whose <c>cpu_utilization</c> collector stopped three days ago, at a CPU that
/// would band Critical. Two more targets hold a reading 20 minutes old: one on a 30-minute schedule still shows
/// it (the bound is twice the interval), the control on a 5-minute schedule does not. The server summary read
/// (<c>get_server_summary</c>) is held to the same answers. The fleet card and the Overview card must both carry the fresh one as before and give the
/// stale one no number, <see cref="FleetCpuSource.NotCollected"/> and an Unknown CPU band.
/// </summary>
[Collection("live-postgres")]
public sealed class FleetCardStaleSqlServerCpuLivePostgresTests
{
    /* NEGATIVE, per this family's convention (see FleetCardPostgresCpuLivePostgresTests). */
    private const int FreshServerId = -993_7196;
    private const int StaleServerId = -993_7197;
    private const int SlowScheduleServerId = -993_7198;
    private const int FastScheduleServerId = -993_7199;
    private const string FreshName = "zz-stale-cpu-fresh";
    private const string SlowScheduleName = "mm-stale-cpu-slow-schedule";
    private const string FastScheduleName = "bb-stale-cpu-fast-schedule";
    private const string StaleName = "aa-stale-cpu-stale";

    private static readonly int[] SentinelIds =
        [FreshServerId, StaleServerId, SlowScheduleServerId, FastScheduleServerId];

    [Fact]
    public async Task ADaysOldSqlServerCpuReading_IsNotShownOrBanded_OnTheFleetCardOrTheOverviewCard()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live stale-CPU test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            foreach (var (id, name) in new[]
            {
                (FreshServerId, FreshName), (StaleServerId, StaleName),
                (SlowScheduleServerId, SlowScheduleName), (FastScheduleServerId, FastScheduleName),
            })
            {
                await InsertServerAsync(connection, id, name, ct);
                /* Online: some OTHER collector reported a moment ago. */
                await InsertCollectionLogAsync(connection, id, name, now.AddSeconds(-30), ct);
            }

            await InsertSqlServerCpuAsync(connection, FreshServerId, FreshName, now.AddMinutes(-1), 30, 4, ct);
            /* 95 + 3 would band Critical if it were shown. */
            await InsertSqlServerCpuAsync(connection, StaleServerId, StaleName, now.AddDays(-3), 95, 3, ct);

            /* #7196: the bound is the larger of 15 minutes and twice the server's own interval. Both servers
               have a 34% reading 20 minutes old. One is scheduled every 30 minutes (bound 60) and still shows
               it. The control is scheduled every 5 (bound 15, the floor) and does not. */
            await InsertSqlServerCpuAsync(connection, SlowScheduleServerId, SlowScheduleName, now.AddMinutes(-20), 30, 4, ct);
            await InsertSqlServerCpuAsync(connection, FastScheduleServerId, FastScheduleName, now.AddMinutes(-20), 30, 4, ct);
            await InsertCpuScheduleAsync(connection, SlowScheduleServerId, 30, ct);
            await InsertCpuScheduleAsync(connection, FastScheduleServerId, 5, ct);

            var result = await DarlingFleetReader.GetFleetOverviewAsync(
                postgres, now.AddHours(-1), now, now, cancellationToken: ct);
            var fresh = result.Cards.Single(c => c.ServerId == FreshServerId);
            var stale = result.Cards.Single(c => c.ServerId == StaleServerId);

            Assert.Equal(34.0, fresh.TotalCpuPercent);
            Assert.Equal(FleetCpuSource.RingBuffer, fresh.CpuSource);
            Assert.Equal(HealthSeverity.Healthy, fresh.CpuSeverity);

            Assert.Null(stale.CpuPercent);
            Assert.Null(stale.TotalCpuPercent);
            Assert.Equal(FleetCpuSource.NotCollected, stale.CpuSource);
            Assert.Equal(HealthSeverity.Unknown, stale.CpuSeverity);

            var slow = result.Cards.Single(c => c.ServerId == SlowScheduleServerId);
            var fast = result.Cards.Single(c => c.ServerId == FastScheduleServerId);
            Assert.Equal(34.0, slow.TotalCpuPercent);
            Assert.Equal(FleetCpuSource.RingBuffer, slow.CpuSource);
            Assert.Null(fast.TotalCpuPercent);
            Assert.Equal(FleetCpuSource.NotCollected, fast.CpuSource);

            var freshCard = await viewer.GetServerSummaryAsync(FreshServerId, FreshName, cancellationToken: ct);
            var staleCard = await viewer.GetServerSummaryAsync(StaleServerId, StaleName, cancellationToken: ct);
            Assert.Equal(34.0, freshCard.TotalCpuPercent);
            Assert.Equal(FleetCpuSource.RingBuffer, freshCard.CpuSource);
            Assert.Null(staleCard.CpuPercent);
            Assert.Null(staleCard.TotalCpuPercent);
            Assert.Equal(FleetCpuSource.NotCollected, staleCard.CpuSource);
            Assert.Equal(34.0, (await viewer.GetServerSummaryAsync(SlowScheduleServerId, SlowScheduleName, cancellationToken: ct)).TotalCpuPercent);
            Assert.Null((await viewer.GetServerSummaryAsync(FastScheduleServerId, FastScheduleName, cancellationToken: ct)).TotalCpuPercent);

            /* get_server_summary: the same bound, and a reading that is shown keeps its own collection_time. */
            var freshSummary = await DarlingHealthReader.GetServerSummaryAsync(postgres, FreshServerId, cancellationToken: ct);
            var staleSummary = await DarlingHealthReader.GetServerSummaryAsync(postgres, StaleServerId, cancellationToken: ct);
            var slowSummary = await DarlingHealthReader.GetServerSummaryAsync(postgres, SlowScheduleServerId, cancellationToken: ct);
            var fastSummary = await DarlingHealthReader.GetServerSummaryAsync(postgres, FastScheduleServerId, cancellationToken: ct);
            Assert.Equal(30.0, freshSummary.CpuPercent);
            Assert.NotNull(freshSummary.CpuCapturedAt);
            Assert.Null(staleSummary.CpuPercent);
            Assert.Null(staleSummary.CpuCapturedAt);
            Assert.Equal(30.0, slowSummary.CpuPercent);
            Assert.NotNull(slowSummary.CpuCapturedAt);
            Assert.Null(fastSummary.CpuPercent);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string name, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES ($1, $2, $2, TRUE, 0, $3)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(MonitoredEngineKind.SqlServer);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCpuScheduleAsync(
        NpgsqlConnection connection, int serverId, int frequencyMinutes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes)
VALUES ($1, 'cpu_utilization', $2)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(frequencyMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSqlServerCpuAsync(
        NpgsqlConnection connection, int serverId, string name,
        DateTime collectionTime, int sqlCpu, int otherCpu, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.AddWithValue(otherCpu);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", SentinelIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        foreach (var table in new[] { "cpu_utilization_stats", "collection_log", "config.config_collector_schedules", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
