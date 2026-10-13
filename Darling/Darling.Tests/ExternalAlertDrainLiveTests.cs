/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

#pragma warning disable CS0618 // the fake deliverer states each send disposition directly; FromFanout needs a real fan-out

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The external alert drain against a real store with a fake deliverer that writes the history row the way the real one
/// does, after its send (#5654): a fire is sent once and leaves one history row, a fire and its clear arrive in order, a muted
/// fire is recorded as muted and sends nothing, a fire every channel refused is marked failed and not sent again, an unregistered server lands in bucket 0,
/// a restart finds a claimed row delivered when the history has its row and interrupted when not (neither is sent again), and
/// the PagerDuty resolve is asked for only on a clear of a firing key.
/// </summary>
public sealed class ExternalAlertDrainLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AFire_IsSentOnce_AndLeavesOneHistoryRow()
    {
        await WithDrainAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01", value: 91.5);
            var pass = await rig.Drain.RunPassAsync(rig.Ct);
            var again = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Delivered);
            Assert.Equal(0, again.Claimed);
            var sent = Assert.Single(rig.Sent);
            Assert.Equal("External:nagios:disk", sent.MetricName);
            Assert.Equal(AlertFamily.External, AlertFamily.Of(sent.MetricName));
            Assert.Equal(id, sent.Context!.External!.PendingId);
            Assert.Equal("delivered", await rig.StatusAsync(id));
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log WHERE metric_name = 'External:nagios:disk'"));
            Assert.Equal("nagios", AlertContextSerializer.TryReadExternalSource(await rig.ScalarAsync("SELECT context_json FROM config.config_alert_log LIMIT 1")));
        });
    }

    [Fact]
    public async Task AFireThenItsClear_ArriveInOrder_AndTheClearWritesAResolutionRow()
    {
        await WithDrainAsync(async rig =>
        {
            var fire = await rig.SubmitAsync("fire", "cpu", server: "SQL01");
            var clear = await rig.SubmitAsync("clear", "cpu", server: "SQL01");

            var first = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, first.Claimed);
            Assert.Equal("pending", await rig.StatusAsync(clear));
            var second = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, second.Claimed);

            Assert.Equal("delivered", await rig.StatusAsync(fire));
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Single(rig.Sent);
            Assert.Equal(new[] { "cpu" }, rig.PagerDutyResolves);
            Assert.Equal("External:nagios:cpu Resolved",
                await rig.ScalarAsync("SELECT metric_name FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log"));
            Assert.True(AlertMetricClassifier.IsResolution("External:nagios:cpu Resolved"));
            Assert.False(AlertMetricClassifier.IsResolution("External:nagios:backup_resolved"));
        });
    }

    [Fact]
    public async Task OneKeyFiredForTwoUnregisteredNames_IsTwoIncidents_AndEachClearResolvesItsOwn()
    {
        await WithDrainAsync(async rig =>
        {
            var fireA = await rig.SubmitAsync("fire", "disk", server: "BOX-A");
            var fireB = await rig.SubmitAsync("fire", "disk", server: "BOX-B");
            var clearA = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            var clearB = await rig.SubmitAsync("clear", "disk", server: "BOX-B");

            for (var pass = 0; pass < 4; pass++)
            {
                await rig.Drain.RunPassAsync(false, rig.Ct);
            }

            Assert.Equal("delivered", await rig.StatusAsync(fireA));
            Assert.Equal("delivered", await rig.StatusAsync(fireB));
            Assert.Equal("delivered", await rig.StatusAsync(clearA));
            Assert.Equal("delivered", await rig.StatusAsync(clearB));
            Assert.Equal(new[] { "BOX-A|external:BOX-A", "BOX-B|external:BOX-B" }, rig.PagerDutyIdentities.OrderBy(i => i, StringComparer.Ordinal).ToArray());
            Assert.Equal("2", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            Assert.Equal("BOX-A,BOX-B", await rig.ScalarAsync("SELECT string_agg(server_name, ',' ORDER BY server_name) FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM config.external_alert_state WHERE firing"));
        });
    }

    [Fact]
    public async Task AClearForOneNameWhenOnlyAnotherFired_IsIgnored_AndLeavesTheOtherFiring()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "disk", server: "BOX-B");
            await rig.Drain.RunPassAsync(false, rig.Ct);
            var clearA = await rig.SubmitAsync("clear", "disk", server: "BOX-A");
            await rig.Drain.RunPassAsync(false, rig.Ct);

            Assert.Equal("ignored", await rig.StatusAsync(clearA));
            Assert.Empty(rig.PagerDutyResolves);
            Assert.Equal("1", await rig.ScalarAsync("SELECT count(*) FROM config.external_alert_state WHERE firing"));
        });
    }

    [Fact]
    public async Task AServerRegisteredBetweenAFireAndItsClear_StillResolvesTheIncidentTheFireOpened()
    {
        await WithDrainAsync(async rig =>
        {
            var fire = await rig.SubmitAsync("fire", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(false, rig.Ct);
            Assert.Equal("NEWBOX|external:NEWBOX", rig.Sent.Select(o => o.ServerName + "|" + o.ServerKey).Single());

            rig.Registry["NEWBOX"] = new ExternalAlertServer(9, "NEWBOX");
            var clear = await rig.SubmitAsync("clear", "cpu", server: "NEWBOX");
            await rig.Drain.RunPassAsync(false, rig.Ct);

            Assert.Equal("delivered", await rig.StatusAsync(fire));
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "NEWBOX|external:NEWBOX" }, rig.PagerDutyIdentities.ToArray());
            Assert.Equal("0", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
        });
    }

    [Fact]
    public async Task AServerRenamedOrRemovedBetweenAFireAndItsClear_StillResolvesTheIncidentTheFireOpened()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "cpu", server: "sql01");
            await rig.Drain.RunPassAsync(false, rig.Ct);

            rig.Registry.Clear();
            var clear = await rig.SubmitAsync("clear", "cpu", server: "SQL01");
            await rig.Drain.RunPassAsync(false, rig.Ct);

            /* The clear differs from the fire in case and in what the registry says; it resolves under the fire's key and name. */
            Assert.Equal("delivered", await rig.StatusAsync(clear));
            Assert.Equal(new[] { "SQL01|7" }, rig.PagerDutyIdentities.ToArray());
            Assert.Equal("7", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name LIKE '% Resolved'"));
        });
    }

    [Fact]
    public async Task AClearWithNoFire_IsIgnored_AndAsksForNoResolve()
    {
        await WithDrainAsync(async rig =>
        {
            var clear = await rig.SubmitAsync("clear", "never-fired");
            await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal("ignored", await rig.StatusAsync(clear));
            Assert.Empty(rig.PagerDutyResolves);
            Assert.Equal("0", await rig.ScalarAsync("SELECT count(*) FROM config.config_alert_log"));
        });
    }

    [Fact]
    public async Task AMutedFire_IsRecordedAsMuted_AndSendsNothing()
    {
        await WithDrainAsync(async rig =>
        {
            rig.Muted = true;
            var id = await rig.SubmitAsync("fire", "disk", server: "SQL01");
            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Muted);
            Assert.Equal("muted", await rig.StatusAsync(id));
            Assert.DoesNotContain(rig.Sent, s => !s.Muted);
            Assert.Equal("true", (await rig.ScalarAsync("SELECT muted::text FROM config.config_alert_log LIMIT 1")));
        });
    }

    [Fact]
    public async Task AFireEveryChannelFailed_IsMarkedFailedAtOnce_AndNotSentAgain()
    {
        /* #5654: a webhook that timed out after its request was sent, or answered with an error, reads the same as a
           refused connection, so a failed fire is never put back in the queue. */
        await WithDrainAsync(async rig =>
        {
            rig.Answer = AlertDelivery.FromLegacyStoredColumns(false, "failed", "boom");
            var id = await rig.SubmitAsync("fire", "disk");

            var pass = await rig.Drain.RunPassAsync(rig.Ct);
            var again = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.Claimed);
            Assert.Equal(1, pass.Failed);
            Assert.Equal(0, pass.Retried);
            Assert.Equal(0, again.Claimed);
            Assert.Equal("failed", await rig.StatusAsync(id));
            Assert.Equal("0", await rig.ScalarAsync($"SELECT attempts::text FROM config.external_alert_pending WHERE pending_id = {id}"));
            Assert.Single(rig.Sent);
        });
    }

    [Fact]
    public async Task AnUnregisteredServer_LandsInBucketZero_AndARegisteredOneInItsOwnId()
    {
        await WithDrainAsync(async rig =>
        {
            await rig.SubmitAsync("fire", "a", server: "NOT-MONITORED");
            await rig.SubmitAsync("fire", "b", server: "sql01");
            await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal("0", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name = 'External:nagios:a'"));
            Assert.Equal("NOT-MONITORED", await rig.ScalarAsync("SELECT server_name FROM config.config_alert_log WHERE metric_name = 'External:nagios:a'"));
            Assert.Equal("7", await rig.ScalarAsync("SELECT server_id::text FROM config.config_alert_log WHERE metric_name = 'External:nagios:b'"));
            Assert.Equal("SQL01", await rig.ScalarAsync("SELECT server_name FROM config.config_alert_log WHERE metric_name = 'External:nagios:b'"));
        });
    }

    [Fact]
    public async Task ARestartMidDelivery_FindsTheRowDeliveredWithAHistoryRow_AndInterruptedWithout_AndSendsNeither()
    {
        await WithDrainAsync(async rig =>
        {
            var logged = await rig.SubmitAsync("fire", "logged");
            var lost = await rig.SubmitAsync("fire", "lost");
            /* A dead run claimed both; only the first got as far as its history row. */
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET status = 'claimed', claimed_at = now() - interval '10 minutes' WHERE pending_id IN ({logged}, {lost})");
            var loggedContext = AlertContextSerializer.Serialize(new AlertContext { External = new AlertExternalOrigin("nagios", "logged", logged) });
            await rig.History.RecordAlertAsync(new AlertHistoryRecord(
                "7", "SQL01", "External:nagios:logged", "1", "", 1, null,
                AlertDelivery.FromLegacyStoredColumns(true, "webhook", null), false, "x", loggedContext));

            var pass = await rig.Drain.RunPassAsync(rig.Ct);

            Assert.Equal(1, pass.ReapedDelivered);
            Assert.Equal(1, pass.ReapedInterrupted);
            Assert.Equal("delivered", await rig.StatusAsync(logged));
            Assert.Equal("interrupted", await rig.StatusAsync(lost));
            Assert.Empty(rig.Sent);

            /* A row 12 must not be found by a history row for 123 (the closing brace anchors the id). */
            Assert.Equal("%\"PendingId\":12}%", AlertContextSerializer.BuildPendingIdLikePattern(12));
        });
    }

    [Fact]
    public async Task ADisabledMasterSwitch_ClaimsNothing_ButExpiryStillRuns()
    {
        await WithDrainAsync(async rig =>
        {
            var id = await rig.SubmitAsync("fire", "disk");
            await rig.ExecAsync($"UPDATE config.external_alert_pending SET expires_at = now() - interval '1 minute' WHERE pending_id = {id}");
            rig.Enabled = false;
            var pass = await rig.Drain.RunPassAsync(rig.Ct);
            Assert.Equal(1, pass.Expired);
            Assert.Equal(0, pass.Claimed);
            Assert.Equal("expired", await rig.StatusAsync(id));
        });
    }

    private static async Task WithDrainAsync(Func<Rig, Task> body)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var seed = new NpgsqlConnection(scratch.ConnectionString);
        await seed.OpenAsync(ct);
        await PgMigrations.MigrateAsync(seed, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var rig = new Rig(seed, dataSource, ct);
        await rig.InitAsync();
        var bodySucceeded = false;
        try
        {
            await body(rig);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private sealed class Rig
    {
        private readonly NpgsqlConnection _seed;
        private int _sourceId;

        public Rig(NpgsqlConnection seed, NpgsqlDataSource dataSource, CancellationToken ct)
        {
            _seed = seed;
            Ct = ct;
            History = new PgAlertHistoryStore(dataSource, NullLogger.Instance);
            Drain = new ExternalAlertDrain(
                new ExternalAlertStore(dataSource, 30),
                () => Enabled,
                async (outcome, token) =>
                {
                    Sent.Add(outcome);
                    var delivery = outcome.Muted ? AlertDelivery.FromLegacyStoredColumns(false, AlertDelivery.ChannelMuted, null) : Answer;
                    await History.RecordAlertAsync(new AlertHistoryRecord(
                        outcome.ServerKey, outcome.ServerName, outcome.MetricName, outcome.CurrentValue, outcome.ThresholdValue,
                        outcome.NumericCurrentValue, outcome.NumericThresholdValue, delivery, outcome.Muted, outcome.DetailText,
                        AlertContextSerializer.Serialize(outcome.Context!)));
                    return delivery;
                },
                (metric, serverName, serverKey, _, _) =>
                {
                    PagerDutyResolves.Add(metric.Split(':')[2]);
                    PagerDutyIdentities.Add(serverName + "|" + serverKey);
                    return Task.FromResult(AlertChannelOutcome.Delivered);
                },
                History,
                _ => Muted,
                name => Registry.TryGetValue(name, out var server) ? server : null,
                "test:1",
                NullLogger.Instance);
        }

        public CancellationToken Ct { get; }
        public PgAlertHistoryStore History { get; }
        public ExternalAlertDrain Drain { get; }
        public List<AlertOutcome> Sent { get; } = new();
        public List<string> PagerDutyResolves { get; } = new();

        /// <summary>The server name and server key each PagerDuty resolve was asked to close, as <c>name|key</c>.</summary>
        public List<string> PagerDutyIdentities { get; } = new();

        /// <summary>The registered servers, by the name the drain looks up (case-insensitive); a test edits it between a fire and its clear.</summary>
        public Dictionary<string, ExternalAlertServer> Registry { get; } =
            new(StringComparer.OrdinalIgnoreCase) { ["SQL01"] = new ExternalAlertServer(7, "SQL01") };
        public bool Muted { get; set; }
        public bool Enabled { get; set; } = true;
        public AlertDelivery Answer { get; set; } = AlertDelivery.FromLegacyStoredColumns(true, "webhook", null);

        public async Task InitAsync() =>
            _sourceId = Convert.ToInt32(
                await ScalarAsync("INSERT INTO config.external_alert_source (source_name, role_name) VALUES ('nagios', 'role_nagios') RETURNING source_id"),
                CultureInfo.InvariantCulture);

        public async Task<long> SubmitAsync(string kind, string key, string? server = null, double? value = null)
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO config.external_alert_pending (source_id, kind, alert_key, severity, title, server_name, value, expires_at)"
                + " VALUES ($1, $2, $3, 'warning', 'a title', $4, $5, now() + interval '15 minutes') RETURNING pending_id", _seed);
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = _sourceId });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = kind });
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)server ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = (object?)value ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Double });
            return Convert.ToInt64(await command.ExecuteScalarAsync(Ct), CultureInfo.InvariantCulture);
        }

        public Task<string?> StatusAsync(long id) => ScalarAsync($"SELECT status FROM config.external_alert_pending WHERE pending_id = {id}");

        public async Task<string?> ScalarAsync(string sql)
        {
            await using var command = new NpgsqlCommand(sql, _seed);
            var value = await command.ExecuteScalarAsync(Ct);
            return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        public async Task ExecAsync(string sql)
        {
            await using var command = new NpgsqlCommand(sql, _seed);
            await command.ExecuteNonQueryAsync(Ct);
        }
    }
}
