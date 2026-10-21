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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one, so the class is
   not serialized against the live-postgres collection. */

/// <summary>
/// The relative Client Timeouts bars (#5736) end to end on a real PostgreSQL store: seeded <c>collection_log</c> and <c>attention_events</c>,
/// the real <see cref="DarlingAlertReadAdapter"/> computing the baseline, and the real <see cref="AlertEngine"/> grading with it. A busy server
/// whose normal rate is 150 a minute does not fire at 150 and does fire above 3 times that. A quiet server fires over the floor of 10. A server
/// with under a day of capture is graded on the floors. The Critical arm works the same way.
/// </summary>
public sealed class ClientTimeoutsRelativeBarsLiveTests
{
    private const int ServerId = 5736001;

    private static string? Pg => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Now()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
    }

    /// <summary>One successful run a minute from <paramref name="days"/> days ago to now, and a count row 2 seconds after each run:
    /// <paramref name="normal"/> attentions a minute until 6 minutes ago, <paramref name="recent"/> after it.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime now, double days, int normal, int recent, CancellationToken ct)
    {
        var from = DateTime.SpecifyKind(now.AddMinutes(-(int)(days * 1440)), DateTimeKind.Unspecified);
        var to = DateTime.SpecifyKind(now, DateTimeKind.Unspecified);
        var recentFrom = DateTime.SpecifyKind(now.AddMinutes(-6), DateTimeKind.Unspecified);
        var id = ServerId.ToString(CultureInfo.InvariantCulture);
        await using var log = new NpgsqlCommand(
            $@"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
               SELECT row_number() OVER (ORDER BY t), {id}, 'busy-live', 'attention', t, 5, 'SUCCESS'
               FROM generate_series($1::timestamp, $2::timestamp, interval '1 minute') AS t", connection);
        log.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, from);
        log.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, to);
        await log.ExecuteNonQueryAsync(ct);

        await using var counts = new NpgsqlCommand(
            $@"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count)
               SELECT row_number() OVER (ORDER BY t), t + interval '2 seconds', {id}, 'busy-live', 'count',
                      CASE WHEN t > $3::timestamp THEN $4 ELSE $5 END
               FROM generate_series($1::timestamp, $2::timestamp, interval '1 minute') AS t", connection);
        counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, from);
        counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, to);
        counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, recentFrom);
        counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, (long)recent);
        counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, (long)normal);
        await counts.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(AlertEngineTests.Harness Harness, AlertOutcome? Fired)> EvaluateAsync(
        NpgsqlDataSource postgres, DateTime now)
    {
        var h = new AlertEngineTests.Harness();
        h.Settings.ClientTimeoutsEnabled = true;
        var engine = new AlertEngine(h.Settings, new DarlingAlertReadAdapter(postgres), h.StateStore, h.Deliverer,
            isAlertMuted: _ => false, utcNow: () => now);
        var key = ServerId.ToString(CultureInfo.InvariantCulture);
        await engine.EvaluateServerAsync(new AlertServerSnapshot(key, "busy-live", true, null, null, false, false, null) { ServerId = ServerId });
        return (h, h.Deliverer.Outcomes.SingleOrDefault(o => o.MetricName == AttentionEvaluator.MetricName));
    }

    private static async Task RunAsync(
        double days, int normal, int recent, Func<NpgsqlDataSource, DateTime, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to a Postgres connection string to run the live relative-bars tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Pg!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(ownerString);
        var now = Now();
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            await SeedAsync(connection, now, days, normal, recent, ct);
        }

        await body(postgres, now);
    }

    [Fact]
    public async Task TheBaseline_IsReadFromTheStoredMinutes_AndIgnoresSamplesAndMinutesWithNoSuccessfulRun()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to a Postgres connection string to run the live relative-bars tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Pg!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(ownerString);
        var now = Now();
        await using (var connection = new NpgsqlConnection(ownerString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
            await SeedAsync(connection, now, 2, normal: 150, recent: 150, ct);

            /* Noise that is not part of the definition: ten minutes holding a huge count under a FAILED run (the collector did not look), a
               sample row per minute, and another server's rows. None of it may move the baseline. */
            await Exec(connection, ct, $@"UPDATE collection_log SET status = 'FAILED'
                WHERE server_id = {ServerId} AND collection_time BETWEEN $1 AND $2",
                now.AddHours(-30).AddMinutes(-10), now.AddHours(-30));
            await Exec(connection, ct, $@"UPDATE attention_events SET attention_count = 100000
                WHERE server_id = {ServerId} AND collection_time BETWEEN $1 AND $2",
                now.AddHours(-30).AddMinutes(-10), now.AddHours(-30).AddSeconds(5));
            await Exec(connection, ct, $@"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count, database_name)
                SELECT 9000000 + row_number() OVER (ORDER BY collection_time), collection_time, server_id, server_name, 'sample', 0, 'db1'
                FROM attention_events WHERE server_id = {ServerId} AND event_kind = 'count'");
            await Exec(connection, ct, $@"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count)
                SELECT 19000000 + attention_event_id, collection_time, {ServerId + 1}, 'other', event_kind, 5000
                FROM attention_events WHERE server_id = {ServerId} AND event_kind = 'count'");
        }

        var baseline = await new DarlingAlertReadAdapter(postgres).GetAttentionBaselineAsync(ServerId.ToString(CultureInfo.InvariantCulture), now, ct);

        Assert.NotNull(baseline);
        Assert.Equal(150d, baseline!.P999ThreeMinuteAverage, 6);
        Assert.InRange(baseline.MinutesObserved, 2_850, 2_890);
    }

    private static async Task Exec(NpgsqlConnection connection, CancellationToken ct, string sql, params DateTime[] times)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var time in times)
        {
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(time, DateTimeKind.Unspecified));
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task ABusyServer_DoesNotFireAtItsNormalRate()
    {
        await RunAsync(2, normal: 150, recent: 150, async (postgres, now) =>
        {
            var (h, fired) = await EvaluateAsync(postgres, now);
            Assert.Null(fired);
            Assert.Empty(h.Deliverer.Outcomes);
        });
    }

    [Fact]
    public async Task ABusyServer_FiresWarningAboveThreeTimesItsBaseline_AndSaysWhy()
    {
        await RunAsync(2, normal: 150, recent: 600, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.NotNull(fired);
            Assert.Equal(AlertSeverityLevel.Warning, fired!.Severity);
            Assert.Equal("450 per minute", fired.ThresholdValue);
            Assert.Contains("Baseline for this server: 150.0 per minute", fired.ShortMessage, StringComparison.Ordinal);
            Assert.Contains("Warning is 3 x baseline = 450 per minute", fired.ShortMessage, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ABusyServer_JustUnderThreeTimesItsBaseline_DoesNotFire()
    {
        await RunAsync(2, normal: 150, recent: 440, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.Null(fired);
        });
    }

    [Fact]
    public async Task ABusyServer_FiresCriticalAboveTenTimesItsBaseline()
    {
        await RunAsync(2, normal: 150, recent: 1600, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.NotNull(fired);
            Assert.Equal(AlertSeverityLevel.Critical, fired!.Severity);
            Assert.Equal("1500 per minute", fired.ThresholdValue);
            Assert.Contains("Critical is 10 x baseline = 1500 per minute", fired.ShortMessage, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ABusyServer_BetweenTheTwoBars_IsWarningNotCritical()
    {
        await RunAsync(2, normal: 150, recent: 1400, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.Equal(AlertSeverityLevel.Warning, fired!.Severity);
        });
    }

    [Fact]
    public async Task AQuietServer_FiresOverTheFloorOfTen_AndCriticalOverTheFloorOfSixty()
    {
        await RunAsync(2, normal: 2, recent: 20, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.Equal(AlertSeverityLevel.Warning, fired!.Severity);
            Assert.Equal("10 per minute", fired.ThresholdValue);
            Assert.Contains("floor", fired.ShortMessage, StringComparison.Ordinal);
        });
        await RunAsync(2, normal: 2, recent: 70, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.Equal(AlertSeverityLevel.Critical, fired!.Severity);
            Assert.Equal("60 per minute", fired.ThresholdValue);
        });
    }

    [Fact]
    public async Task AQuietServer_UnderTheFloor_DoesNotFire()
    {
        await RunAsync(2, normal: 2, recent: 8, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.Null(fired);
        });
    }

    [Fact]
    public async Task AServerWithUnderADayOfCapture_UsesTheFloors_AndSaysSo()
    {
        /* Eight hours of capture at 150 a minute: no baseline yet, so 150 is over both floors and fires Critical at 60. */
        await RunAsync(8d / 24, normal: 150, recent: 150, async (postgres, now) =>
        {
            var (_, fired) = await EvaluateAsync(postgres, now);
            Assert.NotNull(fired);
            Assert.Equal(AlertSeverityLevel.Critical, fired!.Severity);
            Assert.Equal("60 per minute", fired.ThresholdValue);
            Assert.Contains("under 1 day of capture", fired.ShortMessage, StringComparison.Ordinal);
        });
    }
}
