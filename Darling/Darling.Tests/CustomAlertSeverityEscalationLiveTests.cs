/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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

/// <summary>
/// #3341 gated live round-trip (DARLING_TEST_PG): the severity-escalation FIX through the real sweep + store.
/// A two-tier rule fires at Warning, then the metric climbs into the Critical band while the incident stays
/// open; the sweep must deliver a SECOND alert at Critical on the SAME incident (metric_name Custom:&lt;id&gt;)
/// and persist FiredSeverity = Critical. This is the wiring the pure <c>CustomAlertSeverityEscalationTests</c>
/// cannot prove: that the evaluator's <c>None</c> arm actually re-computes the band, delivers the change, and
/// saves it. A Gauge metric (avg over the window) is used so ONE seeded row is a deterministic value, and the
/// evaluator is built with a zero cadence so the second sweep is immediately due.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertSeverityEscalationLiveTests
{
    private sealed class CapturingDeliverer : IAlertDeliverer
    {
        public List<AlertOutcome> Delivered { get; } = new();

        public Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            lock (Delivered)
            {
                Delivered.Add(outcome);
            }

            return Task.CompletedTask;
        }
    }

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the severity-escalation live test.");
        return connectionString!;
    }

    private static async Task<NpgsqlDataSource> MigrateAndOpenAsync(string connectionString, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(connectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        var dataSourceConnectionString = new NpgsqlConnectionStringBuilder(connectionString)
        {
            SearchPath = "collect,config,public",
        }.ConnectionString;

        return NpgsqlDataSource.Create(dataSourceConnectionString);
    }

    private static async Task RegisterServerAsync(NpgsqlDataSource dataSource, int serverId, string storageName, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(
            "INSERT INTO servers (server_id, server_name, display_name) VALUES ($1, $2, $3)");
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storageName });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storageName });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Seeds one raw cpu_utilization_stats row for the server with a CURRENT collection_time (server-side
    /// <c>now() AT TIME ZONE 'UTC'</c>, avoiding the Kind=Utc-&gt;timestamptz zone-shift trap), so the gauge metric's
    /// avg over the window is exactly <paramref name="cpuPercent"/>.</summary>
    private static async Task SeedCpuAsync(NpgsqlDataSource dataSource, int serverId, string storageName, int cpuPercent, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, (now() AT TIME ZONE 'UTC'), $2, $3, (now() AT TIME ZONE 'UTC'), $4, 0)");
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = DateTime.UtcNow.Ticks });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = storageName });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = cpuPercent });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteCpuAsync(NpgsqlDataSource dataSource, int serverId, CancellationToken ct)
    {
        await using var command = dataSource.CreateCommand("DELETE FROM cpu_utilization_stats WHERE server_id = $1");
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task OpenIncident_ClimbingPastCritical_DeliversCriticalOnTheSameIncident()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        const int serverId = 991341;
        var storage = "car_esc_srv_" + Guid.NewGuid().ToString("N");
        var ruleName = "car_esc_" + Guid.NewGuid().ToString("N");
        // Gauge metric (avg over 15m) so one seeded row = a deterministic value. warn >= 25, critical >= 40,
        // fire on the first breach, scoped to exactly this server.
        var definition =
            "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
            "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
            "\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + storage + "\"]}}";

        var deliverer = new CapturingDeliverer();
        var rules = new CustomAlertRuleStore(dataSource);
        var state = new CustomAlertStateStore(dataSource);
        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(dataSource, serverId, storage, ct);

            var created = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(ruleName, null, definition, true, "test", ct));
            var ruleId = created.Rule!.Id;
            var version = created.Rule!.Version;
            var metricName = "Custom:" + ruleId;

            // defaultIntervalSeconds 0 so the second sweep is immediately due (no 60s cadence wait); cacheTtl 0 so
            // each sweep re-reads the rule.
            var evaluator = new CustomAlertEvaluator(
                rules, state, dataSource, deliverer, isAlertMuted: null,
                new PgAlertHistoryStore(dataSource), defaultIntervalSeconds: 0,
                cacheTtl: TimeSpan.Zero, NullLogger.Instance);

            // Sweep 1: a Warning-band value (30) fires the incident at Warning.
            await SeedCpuAsync(dataSource, serverId, storage, 30, ct);
            await evaluator.EvaluateServerAsync(serverId, storage, storage, ct);

            var warnDelivery = Assert.Single(deliverer.Delivered);
            Assert.Equal(AlertSeverityLevel.Warning, warnDelivery.Severity);
            Assert.Equal(metricName, warnDelivery.MetricName);
            var warnState = await state.LoadAsync(ruleId, serverId, version, ct);
            Assert.Equal("Warning", warnState.FiredSeverity);
            Assert.True(warnState.Persistence.Firing);

            // The evaluator clamps its per-rule cadence to a 30s minimum (too long to wait in a test), so clear
            // the due-gate while KEEPING the open Firing state + delivered Warning band, making sweep 2 due now.
            await state.SaveAsync(ruleId, serverId, warnState with { NextDueAt = null }, ct);

            // Sweep 2: the value climbs into the Critical band (50) while the incident stays OPEN. Pre-fix this
            // returned None with an empty break and never re-paged; now it must deliver a Critical escalation.
            await DeleteCpuAsync(dataSource, serverId, ct);
            await SeedCpuAsync(dataSource, serverId, storage, 50, ct);
            await evaluator.EvaluateServerAsync(serverId, storage, storage, ct);

            Assert.Equal(2, deliverer.Delivered.Count);
            var escalation = deliverer.Delivered[1];
            Assert.Equal(AlertSeverityLevel.Critical, escalation.Severity);
            Assert.Equal(metricName, escalation.MetricName); // SAME incident (Custom:<id>), not a new one
            var critState = await state.LoadAsync(ruleId, serverId, version, ct);
            Assert.Equal("Critical", critState.FiredSeverity);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                using (var setPath = new NpgsqlCommand("SET search_path TO collect, config, public", cleanup))
                {
                    await setPath.ExecuteNonQueryAsync(cleanupCt);
                }

                using (var rule = new NpgsqlCommand("DELETE FROM custom_alert_rules WHERE name = $1", cleanup))
                {
                    rule.Parameters.AddWithValue(ruleName);
                    await rule.ExecuteNonQueryAsync(cleanupCt);
                }

                using (var cpu = new NpgsqlCommand("DELETE FROM cpu_utilization_stats WHERE server_id = $1", cleanup))
                {
                    cpu.Parameters.AddWithValue(serverId);
                    await cpu.ExecuteNonQueryAsync(cleanupCt);
                }

                using var server = new NpgsqlCommand("DELETE FROM servers WHERE server_id = $1", cleanup);
                server.Parameters.AddWithValue(serverId);
                await server.ExecuteNonQueryAsync(cleanupCt);
            });
        }
    }
}
