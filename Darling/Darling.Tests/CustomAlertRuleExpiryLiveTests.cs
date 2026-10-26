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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6235 gated live round-trips (DARLING_TEST_PG): a custom alert rule's optional expiry. A rule past its expiry is not evaluated and
/// does not fire; a rule before it fires as before; clearing the expiry through update turns the rule back on; an expiry in the past is
/// refused on create; and the reads (list, get) say whether a rule has expired. The sweep is the real evaluator over the real store.
/// </summary>
[Collection("live-postgres")]
public sealed class CustomAlertRuleExpiryLiveTests
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

        public async Task<AlertDelivery?> DeliverAndReportAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            await DeliverAsync(outcome, cancellationToken);
            return null;
        }
    }

    private static string RequireLivePostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (owner/superuser) to run the custom-alert expiry live tests.");
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

    private static string DefinitionFor(string storage) =>
        "{\"metric\":{\"source\":\"cpu_utilization_stats\",\"measure\":\"sqlserver_cpu_utilization\",\"aggregate\":\"avg\",\"hours\":0.25}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25}," +
        "\"hysteresis\":{\"breachSamples\":1,\"clearSamples\":1}," +
        "\"scope\":{\"mode\":\"servers\",\"servers\":[\"" + storage + "\"]}}";

    private static async Task CleanAsync(string connectionString, bool bodySucceeded, string prefix, int serverId)
    {
        await LiveStoreCleanup.RunAsync(connectionString, bodySucceeded, async (cleanup, cleanupCt) =>
        {
            using (var setPath = new NpgsqlCommand("SET search_path TO collect, config, public", cleanup))
            {
                await setPath.ExecuteNonQueryAsync(cleanupCt);
            }

            using (var rule = new NpgsqlCommand("DELETE FROM custom_alert_rules WHERE name LIKE $1", cleanup))
            {
                rule.Parameters.AddWithValue(prefix + "%");
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

    [Fact]
    public async Task Sweep_DoesNotFireAnExpiredRule_FiresOneBeforeItsExpiry_AndClearingTheExpiryTurnsTheRuleBackOn()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        const int serverId = 996235;
        var storage = "car_exp_srv_" + Guid.NewGuid().ToString("N");
        var prefix = "car_exp_" + Guid.NewGuid().ToString("N") + "_";
        var definition = DefinitionFor(storage);
        var deliverer = new CapturingDeliverer();
        var rules = new CustomAlertRuleStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
        var state = new CustomAlertStateStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
        var bodySucceeded = false;
        try
        {
            await RegisterServerAsync(dataSource, serverId, storage, ct);

            // Both rules watch the same breaching metric. One expired an hour ago, one expires in an hour. The store takes a past
            // expiry (the tools refuse it on create), which is the state a rule is in once its expiry passes.
            var expired = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(prefix + "expired", null, definition, true, "test", DateTime.UtcNow.AddHours(-1), ct)).Rule!;
            var live = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(prefix + "live", null, definition, true, "test", DateTime.UtcNow.AddHours(1), ct)).Rule!;

            var evaluator = new CustomAlertEvaluator(
                rules, state, dataSource, deliverer, isAlertMuted: null, alertsEnabled: static () => true,
                new PgAlertHistoryStore(dataSource, DarlingAlertReadAdapter.AlertPassCommandTimeoutSeconds), defaultIntervalSeconds: 0,
                cacheTtl: TimeSpan.Zero, NullLogger.Instance);

            await SeedCpuAsync(dataSource, serverId, storage, 30, ct);
            await evaluator.EvaluateServerAsync(serverId, storage, storage, ct);

            // The rule before its expiry fires as it always did; the rule past its expiry does not.
            var delivery = Assert.Single(deliverer.Delivered);
            Assert.Equal("Custom:" + live.Id, delivery.MetricName);
            Assert.DoesNotContain(deliverer.Delivered, d => d.MetricName == "Custom:" + expired.Id);

            // Clearing the expiry through update turns the expired rule back on: the next sweep fires it.
            var cleared = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.UpdateAsync(expired.Id, expired.Name, null, definition, true, expired.Version, "test", null, ct)).Rule!;
            Assert.Null(cleared.ExpiresAtUtc);
            await evaluator.EvaluateServerAsync(serverId, storage, storage, ct);

            Assert.Equal(2, deliverer.Delivered.Count);
            Assert.Contains(deliverer.Delivered, d => d.MetricName == "Custom:" + expired.Id);

            bodySucceeded = true;
        }
        finally
        {
            await CleanAsync(connectionString, bodySucceeded, prefix, serverId);
        }
    }

    [Fact]
    public async Task Tools_RefuseAPastExpiryOnCreate_AndTheReadsShowTheExpiryAndWhetherTheRuleHasExpired()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var prefix = "car_expt_" + Guid.NewGuid().ToString("N") + "_";
        var definition = DefinitionFor("no_such_server_" + prefix);
        var bodySucceeded = false;
        try
        {
            // An expiry in the past is refused on create, and nothing is saved.
            var refused = JsonNode.Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(
                dataSource, prefix + "past", definition, expires_at_utc: DateTime.UtcNow.AddMinutes(-5).ToString("o", CultureInfo.InvariantCulture)))!;
            Assert.Equal("invalid", (string?)refused["status"]);
            Assert.Contains("in the past", (string?)refused["message"] ?? (string?)refused["error"] ?? refused.ToJsonString());
            Assert.DoesNotContain(await new CustomAlertRuleStore(dataSource, StorageCommandDeadlines.McpReadSeconds).ListAsync(ct), r => r.Name == prefix + "past");

            // A future expiry saves, and the create answer says when the rule expires.
            var expiry = DateTime.UtcNow.AddDays(3);
            var created = JsonNode.Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(
                dataSource, prefix + "watch", definition, expires_at_utc: expiry.ToString("o", CultureInfo.InvariantCulture)))!;
            var id = (long)created["id"]!;
            Assert.NotNull((string?)created["expires_at_utc"]);
            Assert.False((bool)created["expired"]!);
            Assert.Contains("expires at", (string?)created["note"]);

            // The reads show the expiry and that the rule has not expired.
            var listed = JsonNode.Parse(await DarlingMcpCustomAlertTools.ListCustomAlertRules(dataSource))!.AsArray();
            var summary = Assert.Single(listed, n => (long)n!["id"]! == id)!;
            Assert.Equal((string?)created["expires_at_utc"], (string?)summary["expires_at_utc"]);
            Assert.False((bool)summary["expired"]!);

            // Make the rule expire (the clock passing, as the store sees it), then the reads say it has expired.
            await using (var age = dataSource.CreateCommand(
                "UPDATE custom_alert_rules SET expires_at_utc = (now() AT TIME ZONE 'UTC') - interval '1 minute' WHERE id = $1"))
            {
                age.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
                await age.ExecuteNonQueryAsync(ct);
            }

            var got = JsonNode.Parse(await DarlingMcpCustomAlertTools.GetCustomAlertRule(dataSource, id))!;
            Assert.True((bool)got["expired"]!);
            var listedAgain = JsonNode.Parse(await DarlingMcpCustomAlertTools.ListCustomAlertRules(dataSource))!.AsArray();
            Assert.True((bool)Assert.Single(listedAgain, n => (long)n!["id"]! == id)!["expired"]!);

            // enabled:true alone does not revive it, and the answer says what does.
            var version = (int)got["version"]!;
            var stillExpired = JsonNode.Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                dataSource, id, version, enabled: true))!;
            Assert.True((bool)stillExpired["expired"]!);
            Assert.Contains("expires_at_utc", (string?)stillExpired["note"]);

            // Moving the expiry into the past is refused; clearing it turns the rule back on, and the answer gives the undo.
            version = (int)stillExpired["version"]!;
            var movedBack = JsonNode.Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                dataSource, id, version, expires_at_utc: DateTime.UtcNow.AddHours(-2).ToString("o", CultureInfo.InvariantCulture)))!;
            Assert.Equal("invalid", (string?)movedBack["status"]);

            var revived = JsonNode.Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                dataSource, id, version, expires_at_utc: string.Empty))!;
            Assert.False((bool)revived["expired"]!);
            Assert.Null((string?)revived["expires_at_utc"]);
            Assert.NotNull((string?)revived["previous_expires_at_utc"]);
            Assert.Equal("update_custom_alert_rule", (string?)revived["undo"]!["tool"]);

            // The undo must be a call the tool accepts: the expiry it undoes had already passed, which the tool refuses to set, so the
            // undo restores "no expiry" and the answer says so (replaying a past instant answers "invalid" and writes nothing).
            Assert.Contains("already passed", (string?)revived["undo_note"]);
            var undoArguments = revived["undo"]!["arguments"]!;
            Assert.Equal(string.Empty, (string?)undoArguments["expires_at_utc"]);
            var replayed = JsonNode.Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                dataSource, (long)undoArguments["rule_id"]!, (int)undoArguments["version"]!, expires_at_utc: (string?)undoArguments["expires_at_utc"]))!;
            Assert.NotEqual("invalid", (string?)replayed["status"]);
            Assert.False((bool)replayed["expired"]!);

            bodySucceeded = true;
        }
        finally
        {
            await CleanAsync(connectionString, bodySucceeded, prefix, 0);
        }
    }

    [Fact]
    public async Task UpdateUndo_OnAnUnchangedResendOfAPassedExpiry_RestoresThatSameExpiry_WithNoNote()
    {
        // #6514 round 2: an update that resends the rule's own already-passed expiry unchanged (the tool accepts it, it is the current
        // value) while it changes another field did not touch the expiry. Its undo must put the same expiry back and carry no
        // "already passed" note; the clear-and-note path is only for a call that actually changed the expiry.
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var prefix = "car_expu_" + Guid.NewGuid().ToString("N") + "_";
        var definition = DefinitionFor("no_such_server_" + prefix);
        var bodySucceeded = false;
        try
        {
            var created = JsonNode.Parse(await DarlingMcpCustomAlertTools.CreateCustomAlertRule(
                dataSource, prefix + "resend", definition,
                expires_at_utc: DateTime.UtcNow.AddDays(3).ToString("o", CultureInfo.InvariantCulture)))!;
            var id = (long)created["id"]!;
            await using (var age = dataSource.CreateCommand(
                "UPDATE custom_alert_rules SET expires_at_utc = (now() AT TIME ZONE 'UTC') - interval '1 minute' WHERE id = $1"))
            {
                age.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
                await age.ExecuteNonQueryAsync(ct);
            }

            var got = JsonNode.Parse(await DarlingMcpCustomAlertTools.GetCustomAlertRule(dataSource, id))!;
            Assert.True((bool)got["expired"]!);
            var passedExpiry = (string)got["expires_at_utc"]!;

            // Resend the same passed expiry and change the description.
            var resent = JsonNode.Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                dataSource, id, (int)got["version"]!, description: "resent", expires_at_utc: passedExpiry))!;
            Assert.NotEqual("invalid", (string?)resent["status"]);
            Assert.Equal(passedExpiry, (string?)resent["expires_at_utc"]);
            Assert.Null((string?)resent["undo_note"]);
            var undoArguments = resent["undo"]!["arguments"]!;
            Assert.Equal(passedExpiry, (string?)undoArguments["expires_at_utc"]);

            // The undo replays cleanly and the expiry is unchanged.
            var replayed = JsonNode.Parse(await DarlingMcpCustomAlertTools.UpdateCustomAlertRule(
                dataSource, (long)undoArguments["rule_id"]!, (int)undoArguments["version"]!,
                description: (string?)undoArguments["description"], expires_at_utc: (string?)undoArguments["expires_at_utc"]))!;
            Assert.NotEqual("invalid", (string?)replayed["status"]);
            var after = JsonNode.Parse(await DarlingMcpCustomAlertTools.GetCustomAlertRule(dataSource, id))!;
            Assert.Equal(passedExpiry, (string?)after["expires_at_utc"]);

            bodySucceeded = true;
        }
        finally
        {
            await CleanAsync(connectionString, bodySucceeded, prefix, 0);
        }
    }

    [Fact]
    public async Task EnabledCap_DoesNotCountAnExpiredRule()
    {
        var connectionString = RequireLivePostgres();
        var ct = TestContext.Current.CancellationToken;
        await using var dataSource = await MigrateAndOpenAsync(connectionString, ct);

        var prefix = "car_expc_" + Guid.NewGuid().ToString("N") + "_";
        var rules = new CustomAlertRuleStore(dataSource, StorageCommandDeadlines.McpReadSeconds);
        var bodySucceeded = false;
        try
        {
            var expired = Assert.IsType<CustomAlertRuleResult.Ok>(
                await rules.CreateAsync(prefix + "a", null, DefinitionFor("x_" + prefix), true, "test", DateTime.UtcNow.AddHours(-1), ct)).Rule!;

            // The evaluator's read leaves the expired rule out, though it is still enabled.
            Assert.DoesNotContain(await rules.ListEnabledAsync(ct), r => r.Id == expired.Id);
            var summary = Assert.Single(await rules.ListAsync(ct), r => r.Id == expired.Id);
            Assert.True(summary.Enabled);
            Assert.True(summary.IsExpiredAt(DateTime.UtcNow));

            await using var count = dataSource.CreateCommand(
                "SELECT count(*) FROM custom_alert_rules WHERE " + CustomAlertRuleStore.ActiveRuleSql + " AND id = $1");
            count.Parameters.Add(new NpgsqlParameter<long> { TypedValue = expired.Id });
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));

            bodySucceeded = true;
        }
        finally
        {
            await CleanAsync(connectionString, bodySucceeded, prefix, 0);
        }
    }
}
