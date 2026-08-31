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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins Darling's Phase-5 slice-D pieces. Ungated: <see cref="DarlingAlertSettings"/> mirrors
/// Lite's App defaults member-for-member (cpu 80/Total, blocking 1, deadlock 1, poison 500,
/// LRQ 30 + the hardcoded 5/all-filters read shape, tempdb 80, low disk 10%/5GB, multiplier 3,
/// lookback 60, cooldown 5, email cooldown 15) with Lite's load-time clamps, and the V3
/// "alerting-stores" migration creates the three Lite-twin tables. Gated on DARLING_TEST_PG:
/// the full engine E2E against a dev Postgres — plant a deadlock + a poison wait, evaluate with
/// REAL PG-backed state/history/mute stores and the real deliverer (SMTP unconfigured → the
/// history row must still be recorded), assert the fires + fingerprint + watermark, and prove
/// neither the same engine (cooldown + watermark) nor a FRESH engine (persisted watermark seed,
/// #1145) re-fires — plus the mute-rule store CRUD round-trip.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so
   cross-test row churn (inserts/purges/deletes) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingAlertingTests
{
    /// <summary>Distinctive fake id — a real server_id is a storage-name hash, never this.</summary>
    private const int TestServerId = -808080;
    private static readonly string TestServerKey = TestServerId.ToString(CultureInfo.InvariantCulture);
    private const string TestServerName = "alert-engine-e2e";

    /* ---------------- ungated: settings adapter pins ---------------- */

    [Fact]
    public void AlertSettings_DefaultsMirrorLitesAppDefaults()
    {
        var settings = new DarlingAlertSettings(new DarlingConfig());

        Assert.True(settings.AlertsEnabled);
        Assert.True(settings.CpuEnabled);
        Assert.True(settings.BlockingEnabled);
        Assert.True(settings.DeadlockEnabled);
        Assert.True(settings.PoisonWaitEnabled);
        Assert.True(settings.LongRunningQueryEnabled);
        Assert.True(settings.TempDbSpaceEnabled);
        Assert.True(settings.LowDiskEnabled);
        Assert.True(settings.LongRunningJobEnabled);
        Assert.True(settings.FailedJobEnabled);

        /* Lite App.xaml.cs:93-120, member-for-member. */
        Assert.Equal(80, settings.CpuThresholdPercent);
        Assert.Equal(CpuAlertMode.TotalServer, settings.CpuAlertMode); /* Lite default CpuAlertMode.Total */
        Assert.Equal(1, settings.BlockingCountThreshold);
        /* #1839 ships OFF (0) in both SKUs — a new alert must not start firing on upgrade. */
        Assert.Equal(0, settings.BlockingWaitSecondsThreshold);
        Assert.Equal(1, settings.DeadlockCountThreshold);
        Assert.Equal(500, settings.PoisonWaitThresholdMs);
        Assert.Equal(30, settings.LongRunningQueryThresholdMinutes);
        Assert.Equal(80, settings.TempDbSpaceThresholdPercent);
        Assert.Equal(10, settings.LowDiskThresholdPercent);
        Assert.Equal(5, settings.LowDiskThresholdGb);
        Assert.Equal(3, settings.LongRunningJobMultiplier);
        Assert.Equal(60, settings.FailedJobLookbackMinutes);
        Assert.Equal(5, settings.CooldownMinutes);
        Assert.Empty(settings.ExcludedDatabases);

        /* The LRQ read shape — Lite's defaults hardcoded (App.xaml.cs:104-109). */
        Assert.Equal(5, settings.LongRunningQueryMaxResults);
        Assert.True(settings.LongRunningQueryExcludeSpServerDiagnostics);
        Assert.True(settings.LongRunningQueryExcludeWaitFor);
        Assert.True(settings.LongRunningQueryExcludeBackups);
        Assert.True(settings.LongRunningQueryExcludeMiscWaits);
        Assert.True(settings.LongRunningQueryExcludeCdc);

        /* Delivery defaults (Lite App.xaml.cs:121,220-223): SMTP off until configured,
           port 587, SSL on, email cooldown 15; webhooks off until a URL is set. */
        Assert.False(settings.SmtpEnabled);
        Assert.Equal(587, settings.SmtpPort);
        Assert.True(settings.SmtpUseSsl);
        Assert.Equal(15, settings.EmailCooldownMinutes);
        Assert.Null(settings.GetSmtpPassword());
        Assert.False(settings.TeamsWebhookEnabled);
        Assert.False(settings.SlackWebhookEnabled);
    }

    [Fact]
    public void AlertSettings_CpuModeMapping_EnabledDerivations_AndLiteClamps()
    {
        var config = new DarlingConfig();

        /* "sql" → SqlProcess; anything else (default "total", garbage) → TotalServer. */
        config.Alerts.CpuMode = "sql";
        Assert.Equal(CpuAlertMode.SqlProcess, new DarlingAlertSettings(config).CpuAlertMode);
        config.Alerts.CpuMode = "banana";
        Assert.Equal(CpuAlertMode.TotalServer, new DarlingAlertSettings(config).CpuAlertMode);

        /* SMTP enabled only when host + from + to are ALL set (no speculative flag). */
        config.Smtp.Host = "mail.example.com";
        Assert.False(new DarlingAlertSettings(config).SmtpEnabled);
        config.Smtp.From = "darling@example.com";
        Assert.False(new DarlingAlertSettings(config).SmtpEnabled);
        config.Smtp.To = "dba@example.com";
        Assert.True(new DarlingAlertSettings(config).SmtpEnabled);

        /* A webhook channel is enabled by its URL. */
        config.Webhooks.TeamsUrl = "https://example.webhook.office.com/x";
        Assert.True(new DarlingAlertSettings(config).TeamsWebhookEnabled);
        Assert.False(new DarlingAlertSettings(config).SlackWebhookEnabled);

        /* Lite's settings-load clamps (App.xaml.cs:527-533,617): cooldowns 1-120, lookback
           1-1440, low-disk % 0-100 / GB >= 0. */
        config.Alerts.CooldownMinutes = 0;
        config.Alerts.FailedJobLookbackMinutes = 9999;
        config.Alerts.LowDiskThresholdPercent = 250;
        config.Alerts.LowDiskThresholdGb = -3;
        config.Smtp.EmailCooldownMinutes = 0;
        var clamped = new DarlingAlertSettings(config);
        Assert.Equal(1, clamped.CooldownMinutes);
        Assert.Equal(1440, clamped.FailedJobLookbackMinutes);
        Assert.Equal(100, clamped.LowDiskThresholdPercent);
        Assert.Equal(0, clamped.LowDiskThresholdGb);
        Assert.Equal(1, clamped.EmailCooldownMinutes);
    }

    [Fact]
    public void AlertSettings_SmtpPassword_DpapiRoundTrip()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "DPAPI requires Windows.");

        var config = new DarlingConfig();
        config.Smtp.EncryptedPassword = DarlingSecrets.Protect("smtp-s3cret");
        Assert.Equal("smtp-s3cret", new DarlingAlertSettings(config).GetSmtpPassword());
    }

    /* ---------------- ungated: V3 migration pins ---------------- */

    [Fact]
    public void V3Migration_CreatesTheThreeLiteTwinAlertingTables()
    {
        var v3 = PgMigrations.Scripts.Single(m => m.Version == 3);
        Assert.Equal("alerting-stores", v3.Name);

        /* config_alert_log — Lite Schema.cs CreateAlertLogTable, column-for-column. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config_alert_log (", v3.Sql, StringComparison.Ordinal);
        foreach (var column in new[]
        {
            "alert_time", "server_id", "server_name", "metric_name", "current_value",
            "threshold_value", "alert_sent", "notification_type", "send_error", "dismissed",
            "muted", "detail_text", "context_json"
        })
        {
            Assert.Contains(column, v3.Sql, StringComparison.Ordinal);
        }

        /* config_edge_trigger_watermarks — Lite's shape including the upsert's conflict key. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config_edge_trigger_watermarks (", v3.Sql, StringComparison.Ordinal);
        Assert.Contains("watermark_time timestamp", v3.Sql, StringComparison.Ordinal);
        Assert.Contains("PRIMARY KEY (server_id, metric_name)", v3.Sql, StringComparison.Ordinal);

        /* config_mute_rules — Lite's shape. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config_mute_rules (", v3.Sql, StringComparison.Ordinal);
        foreach (var column in new[]
        {
            "expires_at_utc", "database_pattern", "query_text_pattern", "wait_type_pattern", "job_name_pattern"
        })
        {
            Assert.Contains(column, v3.Sql, StringComparison.Ordinal);
        }
    }

    /* ---------------- gated: full engine E2E over real PG stores ---------------- */

    private const string DeadlockGraphXml = @"<deadlock><victim-list><victimProcess id=""process1""/></victim-list><process-list><process id=""process1"" spid=""55"" currentdbname=""StackOverflow""><inputbuf>UPDATE Users SET Reputation = 1</inputbuf></process><process id=""process2"" spid=""60"" currentdbname=""StackOverflow""><inputbuf>UPDATE Badges SET Name = 'x'</inputbuf></process></process-list><resource-list><keylock objectname=""StackOverflow.dbo.Users""><owner id=""process2"" mode=""X""/><waiter id=""process1"" mode=""U""/></keylock></resource-list></deadlock>";

    private sealed class RecordingDeliverer : IAlertDeliverer
    {
        private readonly IAlertDeliverer _inner;
        public List<AlertOutcome> Outcomes { get; } = new();

        public RecordingDeliverer(IAlertDeliverer inner) => _inner = inner;

        public async Task DeliverAsync(AlertOutcome outcome, CancellationToken cancellationToken = default)
        {
            Outcomes.Add(outcome);
            await _inner.DeliverAsync(outcome, cancellationToken);
        }
    }

    [Fact]
    public async Task EndToEnd_EngineOverPgStores_FiresOnce_PersistsEverything_NeverRefires()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-engine test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Clear leftovers from an earlier aborted run so the assertions below are deterministic. */
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* All timestamps Kind-Unspecified — naive-UTC storage, see PgCollectorRowWriter. */
            var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            var collectionTime = utcNow.AddMinutes(-1);

            /* Plant one deadlock + one poison wait for the fake server. */
            await InsertAsync(connection,
                "INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time, victim_process_id, victim_sql_text, deadlock_graph_xml) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                1L, collectionTime, TestServerId, TestServerName, utcNow.AddMinutes(-4),
                "process1", "UPDATE Users SET Reputation = 1", DeadlockGraphXml);

            await InsertAsync(connection,
                "INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                1L, collectionTime, TestServerId, TestServerName, "THREADPOOL", 50L, 100000L);

            AlertEngine BuildEngine(RecordingDeliverer deliverer, MuteRuleService muteRuleService)
            {
                var config = new DarlingConfig(); /* alert defaults; SMTP/webhooks unconfigured */
                return new AlertEngine(
                    new DarlingAlertSettings(config),
                    new DarlingAlertReadAdapter(postgres),
                    new PgAlertStateStore(postgres),
                    deliverer,
                    muteRuleService.IsAlertMuted);
            }

            async Task<(RecordingDeliverer Deliverer, AlertEngine Engine)> BuildStackAsync()
            {
                var config = new DarlingConfig();
                var settings = new DarlingAlertSettings(config);
                var historyStore = new PgAlertHistoryStore(postgres);
                var webhooks = new WebhookAlertService(
                    settings, DarlingAlertDeliverer.Branding, NullLogger<WebhookAlertService>.Instance, historyStore);
                var deliverer = new RecordingDeliverer(
                    new DarlingAlertDeliverer(settings, historyStore, webhooks, NullLogger.Instance));
                var muteRuleService = new MuteRuleService(
                    new PgMuteRuleStore(postgres), NullLogger<MuteRuleService>.Instance);
                await muteRuleService.LoadAsync(); /* empty store = nothing muted */
                return (deliverer, BuildEngine(deliverer, muteRuleService));
            }

            var snapshot = new AlertServerSnapshot(
                TestServerKey, TestServerName,
                IsOnline: true, SqlCpuPercent: null, TotalCpuPercent: null,
                IsAzureSqlDb: false, Suppressed: false);

            /* --- first sweep: the deadlock and the poison wait fire, unmuted --- */
            var (deliverer, engine) = await BuildStackAsync();
            await engine.EvaluateServerAsync(snapshot, ct);

            var deadlockFire = Assert.Single(deliverer.Outcomes, o => o.MetricName == "Deadlocks Detected");
            Assert.Equal("1", deadlockFire.CurrentValue);
            Assert.False(deadlockFire.Muted);
            /* #1140: the graph's involved objects produced a dedup fingerprint. */
            Assert.NotNull(deadlockFire.Context);
            var incident = Assert.Single(deadlockFire.Context!.Incidents!);
            Assert.False(string.IsNullOrEmpty(incident.DedupKey));
            Assert.Contains("StackOverflow.dbo.Users", incident.InvolvedObjects);

            Assert.Single(deliverer.Outcomes, o => o.MetricName == "Poison Wait");
            Assert.Equal(2, deliverer.Outcomes.Count);

            /* --- the deliverer recorded history rows even with NO channel configured --- */
            using (var read = new NpgsqlCommand(@"
SELECT notification_type, alert_sent, muted, context_json
FROM config_alert_log
WHERE server_id = $1 AND metric_name = 'Deadlocks Detected'", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct), "config_alert_log row missing for the deadlock fire");
                Assert.Equal("tray", reader.GetString(0)); /* Lite's taxonomy: no email/webhook attempted */
                Assert.False(reader.GetBoolean(1));
                Assert.False(reader.GetBoolean(2));
                Assert.Contains("DedupKey", reader.GetString(3), StringComparison.Ordinal);
                Assert.False(await reader.ReadAsync(ct), "expected exactly one deadlock history row");
            }

            /* --- the edge-trigger watermark advanced and persisted (#1091/#1145) --- */
            using (var read = new NpgsqlCommand(@"
SELECT watermark
FROM config_edge_trigger_watermarks
WHERE server_id = $1 AND metric_name = $2", connection))
            {
                read.Parameters.AddWithValue(TestServerId);
                read.Parameters.AddWithValue(AlertEngine.DeadlockWatermarkMetric);
                Assert.Equal(1, await read.ExecuteScalarAsync(ct));
            }

            /* --- the history store's unfiltered cooldown seed sees the row --- */
            var historyStore = new PgAlertHistoryStore(postgres);
            Assert.NotNull(await historyStore.GetLastAlertTimeAsync(TestServerKey, "Deadlocks Detected"));
            Assert.Null(await historyStore.GetLastEmailSentUtcAsync(TestServerKey, "Deadlocks Detected"));

            /* --- second sweep on the SAME engine: cooldown + in-memory watermark → no re-fire --- */
            await engine.EvaluateServerAsync(snapshot, ct);
            Assert.Equal(2, deliverer.Outcomes.Count);

            /* --- a FRESH engine (fresh in-memory state — a service restart) seeds the persisted
                   watermark from Postgres and still does not re-fire the same deadlock (#1145).
                   The poison wait DOES re-fire: it is cooldown-gated only, and the new engine's
                   in-memory cooldown starts empty — exactly Lite's post-restart behavior. --- */
            var (restartDeliverer, restartEngine) = await BuildStackAsync();
            await restartEngine.EvaluateServerAsync(snapshot, ct);
            Assert.DoesNotContain(restartDeliverer.Outcomes, o => o.MetricName == "Deadlocks Detected");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    /// <summary>
    /// #2716: the primitive Darling's Postgres Tier-0-predictor cooldown (wraparound/xmin/replication
    /// slot — DarlingWorker's <c>_lastPostgresAlert</c>, keyed per server|metric|subject) restart-seeds
    /// itself from. Those three alerts have no rolling-COUNT watermark to persist through
    /// <see cref="IAlertStateStore"/> — they are a plain last-alerted TIME per subject — so the fix
    /// reuses <see cref="IAlertHistoryStore.GetLastAlertTimeAsync"/>'s dedupKey filter (already proven
    /// for the #1154 email/webhook cooldowns) instead of adding new schema. This proves the filter
    /// actually isolates by subject rather than degenerating to the metric-level MAX: two subjects
    /// alerted at different times on the SAME (server, metric) must each seed from their OWN row, not
    /// the newer one's.
    /// </summary>
    [Fact]
    public async Task PgAlertHistoryStore_GetLastAlertTimeAsync_FiltersByDedupKey_ForRestartSeeding()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-history test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteTestRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            var historyStore = new PgAlertHistoryStore(postgres);
            const string metricName = "Wraparound Risk";

            /* Real serializer, not hand-built JSON — the same path AlertOutcome.Context takes before
               RecordAlertAsync ever sees it (AlertContextBuilders/AlertContextSerializer.Serialize). */
            string ContextFor(string subject) =>
                AlertContextSerializer.Serialize(new AlertContext
                {
                    Incidents = new List<AlertIncident> { new(subject, new[] { subject }) },
                });

            /* "walnut" alerted first (older), "cashew" alerted second (newer) — same server, same
               metric, different subjects. An unfiltered MAX would report cashew's time for BOTH. */
            await historyStore.RecordAlertAsync(new AlertHistoryRecord(
                TestServerKey, TestServerName, metricName,
                "92%", "90%", 92, 90,
                AlertSent: true, NotificationType: "tray", SendError: null,
                Muted: false, DetailText: null, ContextJson: ContextFor("walnut")));

            await Task.Delay(TimeSpan.FromMilliseconds(50), ct); /* force a distinguishable alert_time */

            await historyStore.RecordAlertAsync(new AlertHistoryRecord(
                TestServerKey, TestServerName, metricName,
                "95%", "90%", 95, 90,
                AlertSent: true, NotificationType: "tray", SendError: null,
                Muted: false, DetailText: null, ContextJson: ContextFor("cashew")));

            var walnutSeed = await historyStore.GetLastAlertTimeAsync(TestServerKey, metricName, dedupKey: "walnut");
            var cashewSeed = await historyStore.GetLastAlertTimeAsync(TestServerKey, metricName, dedupKey: "cashew");
            var unfiltered = await historyStore.GetLastAlertTimeAsync(TestServerKey, metricName);
            var missingSubjectSeed = await historyStore.GetLastAlertTimeAsync(TestServerKey, metricName, dedupKey: "pistachio");

            Assert.NotNull(walnutSeed);
            Assert.NotNull(cashewSeed);
            /* The whole point of the fix: walnut's seed must be its OWN (older) time, not cashew's. */
            Assert.True(walnutSeed < cashewSeed,
                $"walnut ({walnutSeed:O}) must seed from its own row, strictly before cashew's ({cashewSeed:O})");
            /* Unfiltered stays the pre-#2716 behavior: the newest row across all subjects. */
            Assert.Equal(cashewSeed, unfiltered);
            /* A subject with no history at all seeds nothing — a restart must not invent a cooldown. */
            Assert.Null(missingSubjectSeed);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteTestRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task EndToEnd_PgMuteRuleStore_CrudRoundTrip()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-store test.");

        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var store = new PgMuteRuleStore(postgres);

        var rule = new MuteRule
        {
            Id = "mute-e2e-" + Guid.NewGuid().ToString("N"),
            ServerName = TestServerName,
            MetricName = "Deadlocks Detected",
            Reason = "e2e round-trip",
            ExpiresAtUtc = DateTime.UtcNow.AddHours(1)
        };

        var bodySucceeded = false;
        try
        {
            await store.InsertAsync(rule);

            var loaded = (await store.LoadAllAsync()).Single(r => r.Id == rule.Id);
            Assert.Equal(TestServerName, loaded.ServerName);
            Assert.Equal("Deadlocks Detected", loaded.MetricName);
            Assert.Equal("e2e round-trip", loaded.Reason);
            Assert.True(loaded.Enabled);
            Assert.NotNull(loaded.ExpiresAtUtc);
            Assert.True(loaded.Matches(new AlertMuteContext { ServerName = TestServerName, MetricName = "Deadlocks Detected" }));

            await store.SetEnabledAsync(rule.Id, false);
            Assert.False((await store.LoadAllAsync()).Single(r => r.Id == rule.Id).Enabled);

            rule.Reason = "updated";
            await store.UpdateAsync(rule);
            Assert.Equal("updated", (await store.LoadAllAsync()).Single(r => r.Id == rule.Id).Reason);

            bodySucceeded = true;
        }
        finally
        {
            /* RunOwnedAsync rather than RunAsync (#1902): the removal has to go through the STORE, which is
               the API under test and owns its own NpgsqlDataSource — so it already opens a fresh connection
               per call and was never exposed to the closed-body-connection half of this defect. Only the
               masking half applies, and that is what this borrows. */
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () => await store.DeleteAsync(rule.Id));
        }

        Assert.DoesNotContain(await store.LoadAllAsync(), r => r.Id == rule.Id);
    }

    private static async Task InsertAsync(NpgsqlConnection connection, string sql, params object[] values)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value);
        }
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task DeleteTestRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM deadlocks WHERE server_id = {TestServerId};" +
            $"DELETE FROM wait_stats WHERE server_id = {TestServerId};" +
            $"DELETE FROM config_alert_log WHERE server_id = {TestServerId};" +
            $"DELETE FROM config_edge_trigger_watermarks WHERE server_id = {TestServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
