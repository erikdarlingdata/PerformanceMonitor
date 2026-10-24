/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6629: the bytes a collector run receives and sends. The meter counts a run's connections and only that run's growth, reads NULL
/// (not 0) when nothing opened, and the reads show a collector with no figure as "not measured". The live test runs the migration
/// on a fresh store, the hourly rollup over planted bytes, and the per-day read across the rolled and the raw parts of the window.
/// </summary>
/* #1776 own-store: the live test mints its own scratch database. */
[Collection("live-postgres")]
public sealed class CollectorBytesTests
{
    private const int ServerId = -6629001;
    private const string ServerName = "collector-bytes";

    private sealed class FakeSource : CollectorByteMeter.ByteSource
    {
        public FakeSource(bool opened, long received, long sent)
        {
            Opened = opened;
            Current = new CollectorBytes(received, sent);
        }

        public bool Opened { get; }

        public CollectorBytes Current { get; set; }

        public override bool EverOpened => Opened;

        public override CollectorBytes? Read() => Current;
    }

    [Fact]
    public void ARun_SumsTheGrowthOfEveryConnection_AndIgnoresWhatAConnectionHeldBeforeTheRun()
    {
        var meter = new CollectorByteMeter();
        var shared = new FakeSource(opened: true, received: 5_000, sent: 700);
        meter.Add(shared);
        var perDatabase1 = new FakeSource(opened: true, received: 0, sent: 0);
        var perDatabase2 = new FakeSource(opened: true, received: 0, sent: 0);
        meter.Add(perDatabase1);
        meter.Add(perDatabase2);

        shared.Current = new CollectorBytes(5_250, 760);
        perDatabase1.Current = new CollectorBytes(1_000, 100);
        perDatabase2.Current = new CollectorBytes(2_000, 200);

        Assert.Equal(new CollectorBytes(3_250, 360), meter.Snapshot());
    }

    [Fact]
    public void ARunWhoseConnectionNeverOpened_IsNullNotZero_AndOneThatOpenedThenFailedKeepsWhatItMoved()
    {
        var neverOpened = new CollectorByteMeter();
        neverOpened.Add(new FakeSource(opened: false, received: 0, sent: 0));
        Assert.Null(neverOpened.Snapshot());
        Assert.Null(new CollectorByteMeter().Snapshot());

        var failedMidRun = new CollectorByteMeter();
        var source = new FakeSource(opened: true, received: 0, sent: 0);
        failedMidRun.Add(source);
        source.Current = new CollectorBytes(4_096, 512);
        Assert.Equal(new CollectorBytes(4_096, 512), failedMidRun.Snapshot());
    }

    [Fact]
    public async Task TheMeterBelongsToTheRunInScope_AndAConnectionMadeOutsideOneIsLeftAlone()
    {
        using var outside = new SqlConnection("Server=tcp:127.0.0.1,1;Database=master;Connect Timeout=1");
        CollectorByteMeter.Track(outside);
        Assert.False(outside.StatisticsEnabled);
        Assert.Null(CollectorByteMeter.Current);

        using var inside = new SqlConnection("Server=tcp:127.0.0.1,1;Database=master;Connect Timeout=1");
        CollectorBytes? snapshot;
        using (var scope = CollectorByteMeter.Begin())
        {
            CollectorByteMeter.Track(inside);
            Assert.True(inside.StatisticsEnabled);

            /* The meter follows the run into a task it starts. */
            Assert.Same(scope.Meter, await Task.Run(() => CollectorByteMeter.Current));
            snapshot = scope.Meter.Snapshot();
        }

        Assert.Null(CollectorByteMeter.Current);
        Assert.Null(snapshot);
    }

    [Fact]
    public void TheReply_ShowsBytesPerDay_NullAndNotMeasuredForAnUnmeasuredCollector_AndNamesTheTopFive()
    {
        var rows = new List<DarlingCollectorBytes.Row>
        {
            new("query_store", 700_000, 7_000, 7),
            new("wait_stats", 70_000, 700, 7),
            new("blocking", null, null),
        };
        var json = """
            {"collectors":[{"collector":"query_store"},{"collector":"wait_stats"},{"collector":"blocking"},{"collector":"pg_only"}]}
            """;

        var merged = JsonNode.Parse(DarlingCollectorBytes.Merge(json, rows, 7, "collector", explicitStatus: true))!;
        var collectors = merged["collectors"]!.AsArray();
        Assert.Equal(100_000, (long)collectors[0]!["bytes_received_per_day"]!);
        Assert.Equal(1_000, (long)collectors[0]!["bytes_sent_per_day"]!);
        Assert.Equal("measured", (string)collectors[0]!["bytes_status"]!);
        Assert.Equal(168, (double)collectors[0]!["bytes_measured_hours"]!);
        Assert.Equal(10_000, (long)collectors[1]!["bytes_received_per_day"]!);

        foreach (var unmeasured in new[] { collectors[2]!, collectors[3]! })
        {
            Assert.Null(unmeasured["bytes_received_per_day"]);
            Assert.Null(unmeasured["bytes_sent_per_day"]);
            Assert.Equal("not measured", (string)unmeasured["bytes_status"]!);
        }

        var top = merged["network_bytes"]!["top_collectors_by_bytes_received_per_day"]!.AsArray();
        Assert.Equal(new[] { "query_store", "wait_stats" }, top.Select(t => (string)t!["collector"]!).ToArray());
        Assert.Equal(168, (double)top[0]!["bytes_measured_hours"]!);
        Assert.Equal(7, (double)merged["network_bytes"]!["window_days"]!);
        Assert.Equal(2, (int)merged["network_bytes"]!["collectors_not_measured"]!);

        /* The size-budgeted form (get_collection_health) leaves an unmeasured collector's row alone and counts it. */
        var compact = JsonNode.Parse(DarlingCollectorBytes.Merge(json, rows, 7, "collector", explicitStatus: false))!;
        var compactRows = compact["collectors"]!.AsArray();
        Assert.Equal(100_000, (long)compactRows[0]!["bytes_received_per_day"]!);
        Assert.Null(compactRows[0]!["bytes_status"]);
        Assert.Null(compactRows[2]!["bytes_received_per_day"]);
        Assert.Null(compactRows[2]!["bytes_status"]);
        Assert.Null(compactRows[2]!["bytes_measured_hours"]);
        Assert.Equal(2, (int)compact["network_bytes"]!["collectors_not_measured"]!);
    }

    [Fact]
    public void TheTopFive_AreOrderedByTheCorrectedPerDayFigure_NotTheRawSum()
    {
        /* A young collector with a smaller sum but a shorter span out-ranks an old one: 1,000 over 2 hours is 12,000 a day, 7,000 over 7 days is 1,000. */
        var rows = new List<DarlingCollectorBytes.Row>
        {
            new("old", 7_000, 70, 7),
            new("young", 1_000, 10, 2.0 / 24),
        };
        var merged = JsonNode.Parse(DarlingCollectorBytes.Merge("""{"collectors":[{"collector":"old"},{"collector":"young"}]}""", rows, 7, "collector", explicitStatus: true))!;
        var top = merged["network_bytes"]!["top_collectors_by_bytes_received_per_day"]!.AsArray();
        Assert.Equal(new[] { "young", "old" }, top.Select(t => (string)t!["collector"]!).ToArray());
        Assert.Equal(12_000, (long)top[0]!["bytes_received_per_day"]!);
        Assert.Equal(2, (double)top[0]!["bytes_measured_hours"]!);
    }

    [Fact]
    public void TheLogPage_CarriesBothFigures_PerRun_WithNullForARunThatMeasuredNothing()
    {
        var json = """{"runs":[{"collector":"a"},{"collector":"b"}]}""";
        var merged = JsonNode.Parse(DarlingCollectorBytes.WithRunBytes(json, new (long?, long?)[] { (1_234, 56), (null, null) }))!;
        var runs = merged["runs"]!.AsArray();
        Assert.Equal(1_234, (long)runs[0]!["bytes_received"]!);
        Assert.Equal(56, (long)runs[0]!["bytes_sent"]!);
        Assert.Null(runs[1]!["bytes_received"]);
        Assert.Null(runs[1]!["bytes_sent"]);
    }

    [Fact]
    public async Task TheRung_TheHourlyRollup_AndThePerDayRead_AgreeOnPlantedBytes_AgainstDevPostgres()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the V219 test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            /* The rung is re-runnable over its own columns. */
            await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 219;");
            await PgMigrations.MigrateAsync(connection, ct);

            var now = new DateTime(2026, 10, 1, 12, 30, 0, DateTimeKind.Utc);
            var hour = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Unspecified);
            /* alpha: three closed hours (rolled) and one run in the open hour (raw); beta ran and measured nothing. */
            await PlantAsync(connection, ct, "alpha", hour.AddHours(-3).AddMinutes(5), 100, 10);
            await PlantAsync(connection, ct, "alpha", hour.AddHours(-2).AddMinutes(5), 200, 20);
            await PlantAsync(connection, ct, "alpha", hour.AddHours(-1).AddMinutes(5), 300, 30);
            await PlantAsync(connection, ct, "alpha", hour.AddMinutes(10), 400, 40);
            await PlantAsync(connection, ct, "beta", hour.AddHours(-1).AddMinutes(5), null, null);
            await PlantAsync(connection, ct, "beta", hour.AddMinutes(10), null, null);
            /* gamma: a young store, measured only in the last 2.5 hours (the 10:00 and 11:00 hours roll up, 100 + 100). */
            await PlantAsync(connection, ct, "gamma", hour.AddHours(-2).AddMinutes(35), 100, 10);
            await PlantAsync(connection, ct, "gamma", hour.AddHours(-1).AddMinutes(5), 100, 10);

            await CollectionLogHourly.RollUpAsync(connection, now, null, ct);

            await using var command = new NpgsqlCommand(
                "SELECT collector_name, sum(bytes_received_sum)::bigint, sum(bytes_sent_sum)::bigint FROM collect.collection_log_hourly GROUP BY collector_name ORDER BY 1", connection);
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(("alpha", 600L, 60L), (reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2)));
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal("beta", reader.GetString(0));
                Assert.True(reader.IsDBNull(1));
                Assert.True(reader.IsDBNull(2));
            }

            await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
            var rows = await DarlingCollectorBytes.ReadAsync(source, ServerId, now, 1, ct);
            var alpha = Assert.Single(rows, r => r.Collector == "alpha");
            Assert.Equal(1_000L, alpha.Received);
            Assert.Equal(100L, alpha.Sent);
            /* alpha's first measured run is the rolled 09:00 hour (planted at 09:05), 3.5 hours before 12:30. */
            Assert.Equal(3.5, CollectorBytesSpan.Hours(alpha.MeasuredDays!.Value));
            Assert.Null(Assert.Single(rows, r => r.Collector == "beta").MeasuredDays);

            /* #6629: the young collector's 200 bytes were measured over 2.5 hours (its first rolled hour starts at 10:00), so a day is 200 x 9.6,
               not 200 spread over the 1-day window. */
            var gamma = Assert.Single(rows, r => r.Collector == "gamma");
            Assert.Equal(200L, gamma.Received);
            Assert.Equal(2.5, CollectorBytesSpan.Hours(gamma.MeasuredDays!.Value));
            Assert.Equal(1_920L, DarlingCollectorBytes.PerDay(gamma.Received, gamma.MeasuredDays));
            var merged = JsonNode.Parse(DarlingCollectorBytes.Merge("""{"collectors":[{"collector":"gamma"}]}""", rows, 1, "collector", explicitStatus: true))!;
            Assert.Equal(1_920L, (long)merged["collectors"]![0]!["bytes_received_per_day"]!);
            Assert.Equal(2.5, (double)merged["collectors"]![0]!["bytes_measured_hours"]!);
            Assert.Null(Assert.Single(rows, r => r.Collector == "beta").Received);

            /* A window that starts at 09:03 reads that partial hour from the raw log (the 09:05 run, 100) and the closed hours from the rollup
               (10:00 and 11:00), without counting the 09:00 rolled hour twice. Same total as the whole window. */
            var partial = await DarlingCollectorBytes.ReadAsync(source, ServerId, now, 3.45 / 24, ct);
            Assert.Equal(1_000L, Assert.Single(partial, r => r.Collector == "alpha").Received);
            var later = await DarlingCollectorBytes.ReadAsync(source, ServerId, now, 3.0 / 24, ct);
            Assert.Equal(900L, Assert.Single(later, r => r.Collector == "alpha").Received);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, DeleteRowsAsync);
        }
    }

    private static async Task PlantAsync(NpgsqlConnection connection, CancellationToken ct, string collector, DateTime time, long? received, long? sent)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected, bytes_received, bytes_sent)
VALUES ($1, $2, $3, $4, $5, 10, 'SUCCESS', 1, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint, Value = received.HasValue ? received.Value : DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint, Value = sent.HasValue ? sent.Value : DBNull.Value });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await ExecAsync(connection, ct, $"DELETE FROM collect.collection_log_hourly WHERE server_id = {ServerId}");
        await ExecAsync(connection, ct, $"DELETE FROM collect.collection_log WHERE server_id = {ServerId}");
    }
}
