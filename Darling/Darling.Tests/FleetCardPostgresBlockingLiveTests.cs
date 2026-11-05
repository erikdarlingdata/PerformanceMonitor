/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
/// #7616 / #7654: a PostgreSQL target's fleet card (and so the web Fleet card, the server header and <c>get_fleet_overview</c>)
/// reads its Blocking from the per-minute blocking captures that <c>get_pg_blocking</c> counts: how many captures in the window saw
/// any blocking, banded by captures per hour. A target with captures and none with blocking is a measured 0. A target with no capture
/// in the window is not measured: Unknown, and left out of the fleet total rather than added as 0.
/// </summary>
[Collection("live-postgres")]
public sealed class FleetCardPostgresBlockingLiveTests
{
    /* NEGATIVE, per this family's convention (see FleetCardPostgresCpuLivePostgresTests). */
    private const int CriticalServerId = -993_76161;
    private const int WarningServerId = -993_76162;
    private const int QuietServerId = -993_76163;
    private const int NoCaptureServerId = -993_76164;
    private const int SqlServerId = -993_76165;
    private const int YoungServerId = -993_76166;
    private const int SlowCadenceServerId = -993_76167;

    private static readonly int[] SentinelIds = [CriticalServerId, WarningServerId, QuietServerId, NoCaptureServerId, SqlServerId, YoungServerId, SlowCadenceServerId];

    [Fact]
    public async Task ThePostgresCard_ReadsItsBlockingFromTheCaptures_AndNameTheTargetWithNoCaptureAsNotMeasured()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live fleet PostgreSQL-blocking test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSentinelRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            foreach (var (id, name, kind) in new[]
            {
                (CriticalServerId, "pgblk-critical", "postgres"),
                (WarningServerId, "pgblk-warning", "postgres"),
                (QuietServerId, "pgblk-quiet", "postgres"),
                (NoCaptureServerId, "pgblk-nocapture", "postgres"),
                (SqlServerId, "sqlblk", "sqlserver"),
                (YoungServerId, "pgblk-young", "postgres"),
                (SlowCadenceServerId, "pgblk-slow", "postgres"),
            })
            {
                await InsertServerAsync(connection, id, name, kind, ct);
            }

            /* 25 captures in the past hour, every one with blocking: 25 an hour, past the Critical bar of 20. */
            for (var i = 0; i < 25; i++)
            {
                await InsertCaptureAsync(connection, CriticalServerId, "pgblk-critical", now.AddMinutes(-2 - i), 3, ct: ct);
            }

            /* 6 of 26 captures saw blocking: 6 an hour, past the Warning bar of 5 and short of 20. */
            for (var i = 0; i < 26; i++)
            {
                await InsertCaptureAsync(connection, WarningServerId, "pgblk-warning", now.AddMinutes(-2 - i), i < 6 ? 2 : 0, ct: ct);
            }

            /* Captures ran and none saw blocking: a measured 0, Healthy. A FAILED capture is not a capture. */
            for (var i = 0; i < 30; i++)
            {
                await InsertCaptureAsync(connection, QuietServerId, "pgblk-quiet", now.AddMinutes(-2 - i), 0, ct: ct);
            }

            await InsertCaptureAsync(connection, QuietServerId, "pgblk-quiet", now.AddMinutes(-40), 5, status: "ERROR", ct: ct);

            /* No pg_blocking capture at all: other collectors ran, and the only blocking capture is outside the window. */
            await InsertCaptureAsync(connection, NoCaptureServerId, "pgblk-nocapture", now.AddHours(-5), 4, ct: ct);
            await InsertCaptureAsync(connection, NoCaptureServerId, "pgblk-nocapture", now.AddMinutes(-3), 0, collector: "pg_cpu_utilization", ct: ct);

            /* A target registered a minute ago: 2 captures, both blocked. 2 of 2 would scale to 60 an hour, so fewer than 3 captures is Unknown (the count still shows). */
            await InsertCaptureAsync(connection, YoungServerId, "pgblk-young", now.AddMinutes(-1), 2, ct: ct);
            await InsertCaptureAsync(connection, YoungServerId, "pgblk-young", now.AddMinutes(-2), 3, ct: ct);

            /* A 5-minute cadence, blocked in every capture: 12 captures in the hour, 12 of 12 is 60 an hour, Critical. A nominal-hour division
               would read 12 an hour, Warning, and never reach Critical. */
            for (var i = 0; i < 12; i++)
            {
                await InsertCaptureAsync(connection, SlowCadenceServerId, "pgblk-slow", now.AddMinutes(-2 - (5 * i)), 1, ct: ct);
            }

            var result = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
            var cards = result.Cards.Where(c => SentinelIds.Contains(c.ServerId)).ToDictionary(c => c.ServerId);
            Assert.Equal(SentinelIds.Length, cards.Count);

            var critical = cards[CriticalServerId];
            Assert.True(critical.BlockingMeasured);
            Assert.Equal(25, critical.BlockingCount);
            Assert.Equal(25L, critical.BlockingCaptures);
            Assert.Equal("captures", critical.BlockingUnit);
            Assert.Equal(HealthSeverity.Critical, critical.BlockingSeverity);
            Assert.Equal(25, critical.ToHealthMetrics().BlockingCount);

            var warning = cards[WarningServerId];
            Assert.Equal(6, warning.BlockingCount);
            Assert.Equal(26L, warning.BlockingCaptures);
            Assert.Equal(HealthSeverity.Warning, warning.BlockingSeverity);

            var quiet = cards[QuietServerId];
            Assert.True(quiet.BlockingMeasured);
            Assert.Equal(0, quiet.BlockingCount);
            Assert.Equal(30L, quiet.BlockingCaptures);
            Assert.Equal(HealthSeverity.Healthy, quiet.BlockingSeverity);

            var none = cards[NoCaptureServerId];
            Assert.False(none.BlockingMeasured);
            Assert.Equal(0L, none.BlockingCaptures);
            Assert.Equal(HealthSeverity.Unknown, none.BlockingSeverity);
            Assert.Null(none.BlockingRatePerHour);
            Assert.Null(none.ToHealthMetrics().BlockingCount);

            var young = cards[YoungServerId];
            Assert.True(young.BlockingMeasured);
            Assert.Equal(2, young.BlockingCount);
            Assert.Equal(HealthSeverity.Unknown, young.BlockingSeverity);
            Assert.Null(young.BlockingRatePerHour);
            Assert.Equal("fewer than 3 captures in this window", young.BlockingDetail);
            Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.MetricSeverities(young.ToHealthMetrics()).ElementAt(3));

            var slow = cards[SlowCadenceServerId];
            Assert.Equal(12, slow.BlockingCount);
            Assert.Equal(12L, slow.BlockingCaptures);
            Assert.Equal(60.0, slow.BlockingRatePerHour);
            Assert.Equal(HealthSeverity.Critical, slow.BlockingSeverity);
            Assert.Null(slow.BlockingDetail);

            var sql = cards[SqlServerId];
            Assert.True(sql.BlockingMeasured);
            Assert.Equal("waits", sql.BlockingUnit);
            Assert.Null(sql.BlockingCaptures);

            /* The fleet total adds the measured servers and leaves the unmeasured one out (#7654). */
            var rollup = DarlingFleetReader.BuildRollup(cards.Values.ToList(), now, now.AddHours(-1), now);
            Assert.Equal(45, rollup.TotalBlockingEvents);
            Assert.Equal(HealthSeverity.Critical, rollup.TotalBlockingSeverity);
            Assert.Contains("SQL Server blocked waits", rollup.TotalBlockingDetail, StringComparison.Ordinal);
            Assert.Contains("PostgreSQL blocking captures", rollup.TotalBlockingDetail, StringComparison.Ordinal);

            /* The Viewer's fleet total is the same number: the captures that saw blocking, 25 + 6 + 2 + 12 (the quiet target adds 0, the target
               with no capture in the window adds nothing). It used to count SQL Server sources only, so it read 0 here. */
            using var totals = new NpgsqlCommand("SELECT total_blocking_events FROM (" + ViewerDataService.FleetTotalsSql + ") AS t", connection);
            totals.Parameters.AddWithValue(DateTime.SpecifyKind(now.AddHours(-1), DateTimeKind.Unspecified));
            totals.Parameters.AddWithValue(DateTime.SpecifyKind(now, DateTimeKind.Unspecified));
            totals.Parameters.AddWithValue(EventWindowFloor.For(now.AddHours(-1)));
            Assert.Equal(45L, Convert.ToInt64(await totals.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelRowsAsync);
        }
    }

    private static async Task InsertServerAsync(
        NpgsqlConnection connection, int serverId, string name, string engineKind, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES ($1, $2, $2, TRUE, 0, $3)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(engineKind);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCaptureAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, int rows,
        string collector = "pg_blocking", string status = "SUCCESS", CancellationToken ct = default)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status, rows_collected)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(rows);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", SentinelIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        foreach (var table in new[] { "collection_log", "servers" })
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids});", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }
}
