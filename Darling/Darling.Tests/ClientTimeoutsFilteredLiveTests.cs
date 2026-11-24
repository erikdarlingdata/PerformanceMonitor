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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test mints its own scratch database through ScratchPostgres and touches nothing on the shared one, so the class is
   not serialized against the live-postgres collection. */

/// <summary>
/// #8971 end to end on a real PostgreSQL store: seeded <c>collection_log</c> runs and <c>attention_events</c> count and sample rows, the real
/// <see cref="DarlingAlertReadAdapter"/>, the real <see cref="AlertEngine"/>, and the real <see cref="DarlingAlertDeliverer"/> writing through
/// <see cref="PgAlertHistoryStore"/>. A storm whose exact rate is cancels under the 1 s duration floor, crossing the bar and dipping under it, writes no
/// <c>config_alert_log</c> row. A storm a mute rule silences after the filters writes one Muted row per cooldown.
/// </summary>
public sealed class ClientTimeoutsFilteredLiveTests
{
    private const int ServerId = 8971001;
    private const string ServerName = "filtered-live";

    private long _nextId = 1;

    private static string? Pg => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime WholeSecondUtcNow()
    {
        var now = DateTime.UtcNow;
        return new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Utc);
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    /// <summary>One successful attention run at <paramref name="run"/>, its count row and its sample rows 2 seconds later: <paramref name="shortSamples"/>
    /// events of 30 us from a driver and <paramref name="realSamples"/> events of 90 s from <paramref name="realApp"/>.</summary>
    private async Task SeedRunAsync(
        NpgsqlConnection connection, DateTime run, long count, int shortSamples, int realSamples, string realApp, CancellationToken ct)
    {
        var id = ServerId.ToString(CultureInfo.InvariantCulture);
        var stamped = Naive(run.AddSeconds(2));

        await using (var log = new NpgsqlCommand(
            $@"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
               VALUES ($1, {id}, '{ServerName}', 'attention', $2, 5, 'SUCCESS')", connection))
        {
            log.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, _nextId++);
            log.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, Naive(run));
            await log.ExecuteNonQueryAsync(ct);
        }

        await using (var counts = new NpgsqlCommand(
            $@"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_kind, attention_count)
               VALUES ($1, $2, {id}, '{ServerName}', 'count', $3)", connection))
        {
            counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, _nextId++);
            counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, stamped);
            counts.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, count);
            await counts.ExecuteNonQueryAsync(ct);
        }

        foreach (var (n, app, durationUs) in new[] { (shortSamples, "Core Microsoft SqlClient Data Provider", 4_000L), (realSamples, realApp, 90_000_000L) })
        {
            if (n == 0)
            {
                continue;
            }

            await using var samples = new NpgsqlCommand(
                $@"INSERT INTO attention_events (attention_event_id, collection_time, server_id, server_name, event_time, event_kind, database_name,
                                                 client_app_name, interrupt_duration_us)
                   SELECT $1 + g, $2, {id}, '{ServerName}', $2, 'sample', 'sales', $3, $4
                   FROM generate_series(1, $5) AS g", connection);
            samples.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, _nextId);
            samples.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, stamped);
            samples.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Text, app);
            samples.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, durationUs);
            samples.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, n);
            await samples.ExecuteNonQueryAsync(ct);
            _nextId += n + 1;
        }
    }

    private static async Task<List<(string Metric, string Type, bool Muted)>> AlertLogAsync(NpgsqlDataSource postgres, CancellationToken ct)
    {
        var rows = new List<(string, string, bool)>();
        await using var read = postgres.CreateCommand(
            "SELECT metric_name, notification_type, muted FROM config.config_alert_log WHERE metric_name LIKE 'Client Timeouts%' ORDER BY alert_time");
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.GetBoolean(2)));
        }

        return rows;
    }

    /// <summary>Runs <paramref name="minutes"/> sweeps a minute apart on a scratch store, seeding one collector run before each, and returns the
    /// Client Timeouts rows the real deliverer wrote. <paramref name="minuteShape"/> gives each minute's count, short and real samples.</summary>
    private async Task<List<(string Metric, string Type, bool Muted)>> SweepAsync(
        int minutes, Func<int, (long Count, int Short, int Real)> minuteShape, string realApp, Func<AlertMuteContext, bool> isMuted)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Pg!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(ownerString);
        await using var connection = new NpgsqlConnection(ownerString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        /* The window read is floored on the wall clock (60 minutes back), so the runs end a minute before it and start inside it. */
        var start = WholeSecondUtcNow().AddMinutes(-(minutes + 1));
        for (var warmup = 3; warmup >= 1; warmup--)
        {
            await SeedRunAsync(connection, start.AddMinutes(-warmup), 0, 0, 0, realApp, ct);
        }

        var h = new AlertEngineTests.Harness();
        h.Settings.ClientTimeoutsEnabled = true;
        var settings = new DarlingAlertSettings(new DarlingConfig());
        var history = new PgAlertHistoryStore(postgres, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds, NullLogger.Instance);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var now = start;
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, isAlertMuted: isMuted, utcNow: () => now);
        var engine = new AlertEngine(h.Settings, new DarlingAlertReadAdapter(postgres), h.StateStore, deliverer,
            isAlertMuted: isMuted,
            resolutionCallback: (resolution, token) => deliverer.DeliverResolutionAsync(resolution, cancellationToken: token),
            utcNow: () => now);
        var snapshot = new AlertServerSnapshot(ServerId.ToString(CultureInfo.InvariantCulture), ServerName, true, null, null, false, false, null) { ServerId = ServerId };

        for (var minute = 0; minute < minutes; minute++)
        {
            var run = start.AddMinutes(minute);
            var (count, shortSamples, realSamples) = minuteShape(minute);
            await SeedRunAsync(connection, run, count, shortSamples, realSamples, realApp, ct);
            now = run.AddSeconds(5);
            await engine.EvaluateServerAsync(snapshot);
        }

        return await AlertLogAsync(postgres, ct);
    }

    [Fact]
    public async Task AFilteredStormThatCrossesAndDips_WritesNoAlertLogRow()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to a Postgres connection string to run the live filtered Client Timeouts tests.");

        /* Eight-minute cycles: five minutes of 155 attentions a minute, 150 of every 155 sampled events under 1 s, then three quiet
           minutes, so the 3-minute rate crosses the Critical bar on the exact count and falls to zero once a cycle. The filters leave
           5 a minute. A mute rule that would match everything is in place and must not turn the crossings into Muted rows. */
        var rows = await SweepAsync(
            40, minute => minute % 8 < 5 ? (155, 150, 5) : (0, 0, 0), realApp: "web", isMuted: _ => true);

        Assert.Empty(rows);
    }

    [Fact]
    public async Task AStormAMuteRuleSilencesAfterTheFilters_WritesOneMutedRowPerCooldown()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to a Postgres connection string to run the live filtered Client Timeouts tests.");

        /* 155 a minute, 50 of every 155 sampled events under 1 s and 105 from a muted backup app: the filters leave 105 a minute, the
           mute takes it to zero. Eleven minutes, a run a minute, the default five-minute cooldown: three Muted rows, as for any muted storm. */
        var rows = await SweepAsync(
            11, _ => (155, 50, 105), realApp: "backup", isMuted: ctx => ctx.ClientAppName == "backup");

        Assert.Equal(3, rows.Count);
        Assert.All(rows, row =>
        {
            Assert.Equal(AttentionEvaluator.MetricName, row.Metric);
            Assert.Equal(AlertDelivery.ChannelMuted, row.Type);
            Assert.True(row.Muted);
        });
    }
}
