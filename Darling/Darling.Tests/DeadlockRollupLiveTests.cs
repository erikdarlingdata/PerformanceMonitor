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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6005 on a live store: the daily rollup the alert pass writes, the agent gate over it and the 4-hour digest. The gate holds
/// back only the agent request; the human page's rows are written whatever it answers.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and never touches another test's rows,
   so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class DeadlockRollupLiveTests
{
    private const int ServerId = 42;
    private const string Server = "42";

    private sealed record Harness(
        DarlingDeadlockRollup Rollup, DarlingAlertDeliverer Deliverer, NpgsqlConnection Connection, Func<DateTime> Clock, DateTime[] Now);

    private static DeadlockAlertRow Row(DateTime collected, string proc = "dbo.usp_pay", int line = 12) =>
        new() { DeadlockGraphXml = DeadlockSignatureTests.Graph(proc: proc, line: line), NewestSampleUtc = collected };

    private static string Sig(string proc = "dbo.usp_pay") => DeadlockSignature.FromGraphXml(DeadlockSignatureTests.Graph(proc: proc))!;

    private static AlertOutcome Outcome(string metric) =>
        new(Server, "srv", metric, "3", "1", null, "detail", NumericCurrentValue: 3, NumericThresholdValue: 1, Muted: false, Severity: null);

    private static Task<string?> ScalarAsync(NpgsqlConnection c, string sql, CancellationToken ct) => Scalar(c, sql, ct);

    private static async Task<string?> Scalar(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task SeedDayAsync(NpgsqlConnection c, string signature, int daysAgo, int count, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.investigation_signature_days (server_id, signature, day, count) VALUES ($1, $2, (now() AT TIME ZONE 'UTC')::date - $3, $4)", c);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(signature);
        command.Parameters.AddWithValue(daysAgo);
        command.Parameters.AddWithValue(count);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Func<Harness, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the deadlock rollup live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var storeString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var connection = new NpgsqlConnection(storeString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var historySource = NpgsqlDataSource.Create(storeString);
        await using var investigationSource = NpgsqlDataSource.Create(InvestigationDispatcher.WithLockTimeout(storeString));
        var now = new[] { DateTime.UtcNow };
        Func<DateTime> clock = () => now[0];
        var store = new InvestigationRequestStore(investigationSource, 30);
        var rollup = new DarlingDeadlockRollup(new DeadlockSignatureStore(investigationSource, 30), clock);

        var config = new DarlingConfig();
        config.Alerts.DeliveryMode = AlertNotificationMode.Summary;
        config.NotificationRoutes = new[]
        {
            new NotificationRoute(1, AlertFamily.Performance, "", "", "", "", "", Enabled: true) { AgentLevel = NotificationRoute.AgentLevelInvestigate },
        };
        var settings = new DarlingAlertSettings(config);
        var history = new PgAlertHistoryStore(historySource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds);
        var webhooks = new WebhookAlertService(settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, history);
        var dispatcher = new InvestigationDispatcher(store, settings, NullLogger.Instance, agentGate: rollup.AllowsAgentRequestAsync);
        var deliverer = new DarlingAlertDeliverer(settings, history, webhooks, NullLogger.Instance, investigationDispatcher: dispatcher);

        await body(new Harness(rollup, deliverer, connection, clock, now), ct);
    }

    [Fact]
    public async Task ANewSignature_OpensARequest_AndTheSameSignatureLater_DoesNotJoinOne()
    {
        await RunAsync(async (h, ct) =>
        {
            var t = h.Clock().AddSeconds(1);
            await h.Rollup.RecordAsync(Server, [Row(t)], t, ct);
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected"), ct);
            Assert.Equal("1|1", await ScalarAsync(h.Connection,
                "SELECT count(*) || '|' || max(alert_count) FROM config.investigation_requests", ct));

            await h.Rollup.RecordAsync(Server, [Row(t.AddSeconds(5))], t, ct);
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected"), ct);
            /* Gated: the second alert did not join (a join would make alert_count 2). */
            Assert.Equal("1|1", await ScalarAsync(h.Connection,
                "SELECT count(*) || '|' || max(alert_count) FROM config.investigation_requests", ct));
        });
    }

    [Fact]
    public async Task ACountOverThreeTimesTheSevenDayAverage_OpensARequest_AndAnOrdinaryDay_DoesNot()
    {
        await RunAsync(async (h, ct) =>
        {
            /* A week of 1 a day (7 rows of history: the average is 1) for a signature that is not new. */
            for (var d = 1; d <= 7; d++)
            {
                await SeedDayAsync(h.Connection, Sig(), d, 1, ct);
            }

            var t = h.Clock().AddSeconds(1);
            await h.Rollup.RecordAsync(Server, [Row(t), Row(t.AddSeconds(1))], t, ct);
            Assert.False(await h.Rollup.AllowsAgentRequestAsync(Server, "Deadlocks Detected", ct));

            await h.Rollup.RecordAsync(Server, [Row(t.AddSeconds(2)), Row(t.AddSeconds(3)), Row(t.AddSeconds(4))], t, ct);
            Assert.True(await h.Rollup.AllowsAgentRequestAsync(Server, "Deadlocks Detected", ct));
        });
    }

    [Fact]
    public async Task TheGate_ReadsTheDailyRows_NotTheGraphXml()
    {
        await RunAsync(async (h, ct) =>
        {
            /* No graph is ever handed to the rollup: only day rows exist, and the gate's answer follows them. */
            for (var d = 1; d <= 7; d++)
            {
                await SeedDayAsync(h.Connection, Sig(), d, 7, ct);
            }

            /* 13 today plus at most yesterday's 7 (the 24-hour count prorates yesterday by the hour of day) stays under 3 x 7 = 21
               at any hour; 20 passed only late in the UTC day. */
            await SeedDayAsync(h.Connection, Sig(), 0, 13, ct);
            Assert.False(await h.Rollup.AllowsAgentRequestAsync(Server, "Deadlocks Detected", ct));
            await SeedDayAsync(h.Connection, Sig("dbo.usp_other"), 3, 1, ct);
            await SeedDayAsync(h.Connection, Sig("dbo.usp_other"), 0, 9, ct);
            Assert.True(await h.Rollup.AllowsAgentRequestAsync(Server, "Deadlocks Detected", ct));
        });
    }

    [Fact]
    public async Task TheRest_ArriveInOneDigestEvent_PerFourHours()
    {
        await RunAsync(async (h, ct) =>
        {
            var t = h.Clock().AddSeconds(1);
            await h.Rollup.RecordAsync(Server, [Row(t)], t, ct);
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected"), ct); /* opens the request the digest attaches to */
            await h.Rollup.RecordAsync(Server, [Row(t.AddSeconds(5)), Row(t.AddSeconds(6))], t, ct);
            Assert.False(await h.Rollup.AllowsAgentRequestAsync(Server, "Deadlocks Detected", ct));

            Assert.Equal(0, await h.Rollup.RunDigestAsync(ct)); /* same 4-hour bucket: nothing yet */

            h.Now[0] = h.Now[0].AddHours(4);
            Assert.Equal(1, await h.Rollup.RunDigestAsync(ct));
            Assert.Equal(0, await h.Rollup.RunDigestAsync(ct)); /* the bucket is spent: one event per 4 hours */
            Assert.Equal("1|2|" + Sig(), await ScalarAsync(h.Connection, @"
SELECT count(*) || '|' || max(detail ->> 'total') || '|' || max(detail -> 'signatures' -> 0 ->> 'signature')
FROM config.investigation_journal WHERE event = 'deadlock_digest'", ct));
        });
    }

    [Fact]
    public async Task TheHumanPage_IsWrittenWhateverTheGateAnswers()
    {
        await RunAsync(async (h, ct) =>
        {
            for (var d = 1; d <= 7; d++)
            {
                await SeedDayAsync(h.Connection, Sig(), d, 7, ct);
            }

            await SeedDayAsync(h.Connection, Sig(), 0, 1, ct);
            await h.Deliverer.DeliverAsync(Outcome("Deadlocks Detected"), ct);
            await h.Deliverer.DeliverAsync(Outcome("Blocking Detected"), ct);

            /* Both pages are in the alert log; the deadlock's agent request was held, the blocking one opened. */
            Assert.Equal("2|1|Blocking Detected", await ScalarAsync(h.Connection, @"
SELECT (SELECT count(*) FROM config_alert_log) || '|' || (SELECT count(*) FROM config.investigation_requests)
       || '|' || (SELECT first_metric FROM config.investigation_requests)", ct));
        });
    }
}
