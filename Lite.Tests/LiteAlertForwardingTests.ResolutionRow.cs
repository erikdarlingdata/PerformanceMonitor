/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #6010 part 2: Lite records a resolution history row, in Darling's shape, when the engine reports an alert cleared, so the
/// investigation sweep can journal <c>resolved</c>. Every reader of <c>config_alert_log</c> is checked here against that row.
/// </summary>
public partial class LiteAlertForwardingTests
{
    private static string TempDb(out DuckDbInitializer db)
    {
        var dir = Path.Combine(Path.GetTempPath(), "LiteResolutionRow_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        db = new DuckDbInitializer(Path.Combine(dir, "lite.duckdb"));
        db.InitializeAsync().GetAwaiter().GetResult();
        return dir;
    }

    private static string? Scalar(DuckDbInitializer db, string sql)
    {
        using var connection = db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    private static void TearDown(DuckDbInitializer db, string dir)
    {
        db.Dispose();
        try { Directory.Delete(dir, recursive: true); } catch (IOException) { /* best effort */ }
    }

    [Fact]
    public async Task TheResolutionCallback_WritesOneHistoryRow_NamingTheMetricItResolves()
    {
        var dir = TempDb(out var db);
        try
        {
            var store = new DuckDbAlertHistoryStore(db);
            var resolution = new AlertResolution(Key, Name, "High CPU", "CPU Resolved", "SRV-A: Total CPU back to 12%");
            var after = 0;
            var callback = LiteAlertResolutionRecorder.Callback(store, (_, _) => { after++; return Task.CompletedTask; });

            await callback(resolution, CancellationToken.None);

            Assert.Equal(1, after);
            Assert.Equal("1", Scalar(db, "SELECT count(*) FROM config_alert_log"));
            Assert.Equal("CPU Resolved", Scalar(db, "SELECT metric_name FROM config_alert_log"));
            Assert.Equal("none", Scalar(db, "SELECT notification_type FROM config_alert_log"));
            Assert.Equal("false", Scalar(db, "SELECT lower(CAST(alert_sent AS VARCHAR)) FROM config_alert_log"));
            Assert.Equal("false", Scalar(db, "SELECT lower(CAST(muted AS VARCHAR)) FROM config_alert_log"));
            Assert.Equal("SRV-A: Total CPU back to 12%", Scalar(db, "SELECT detail_text FROM config_alert_log"));
            var context = JsonNode.Parse(Scalar(db, "SELECT context_json FROM config_alert_log")!)!;
            Assert.Equal("High CPU", (string)context[AlertResolutionRecords.ResolvesMember]!);
        }
        finally { TearDown(db, dir); }
    }

    [Fact]
    public async Task AFailedHistoryWrite_NeverCostsTheToastHalf()
    {
        var after = 0;
        var callback = LiteAlertResolutionRecorder.Callback(new ThrowingHistoryStore(), (_, _) => { after++; return Task.CompletedTask; });

        await callback(new AlertResolution(Key, Name, "High CPU", "CPU Resolved", "m"), CancellationToken.None);

        Assert.Equal(1, after);
    }

    private sealed class ThrowingHistoryStore : IAlertHistoryStore
    {
        public Task RecordAlertAsync(AlertHistoryRecord record) => throw new InvalidOperationException("store down");
        public Task<DateTime?> GetLastEmailSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastWebhookSentUtcAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastAlertTimeAsync(string serverId, string metricName, string? dedupKey = null) => Task.FromResult<DateTime?>(null);
        public Task<DateTime?> GetLastDeliveredPageUtcAsync(string serverId, string metricName) => Task.FromResult<DateTime?>(null);
    }

    [Fact]
    public async Task EndToEnd_AnAlertOpensARequest_ItClears_AndTheNextSweepJournalsResolvedOnce()
    {
        DisableAllChecks();
        App.AlertCpuEnabled = true;
        var dir = TempDb(out var db);
        try
        {
            var history = new DuckDbAlertHistoryStore(db);
            var investigations = new DuckDbInvestigationStore(db, () => DateTime.UtcNow, DuckDbInvestigationStoreTests.TestLockBudget);
            var h = new Harness { ResolutionHook = LiteAlertResolutionRecorder.Callback(history, (_, _) => Task.CompletedTask) };
            var engine = h.Build();

            var at = await DriveCpuAsync(engine, sqlCpu: 70, totalCpu: 92, samples: AlertEngine.SqlCpuBreachSamples, from: Harness.SampleBase);
            Assert.Single(h.Deliverer.Outcomes);

            /* The fire opens a request for the same server and metric the engine named. */
            var alert = new InvestigationAlert(1, int.Parse(Key, CultureInfo.InvariantCulture), Name, "resources", "High CPU",
                DateTime.UtcNow.AddMinutes(-5), new[] { "k-cpu" }, null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240);
            var opened = await investigations.OpenOrJoinAsync(alert, TestContext.Current.CancellationToken);
            Assert.Equal(InvestigationOpenOutcome.Opened, opened.Outcome);

            Assert.Equal(0, (await investigations.SweepAsync(TestContext.Current.CancellationToken)).Resolved);

            h.Now = h.Now.AddMinutes(6);
            await DriveCpuAsync(engine, sqlCpu: 10, totalCpu: 12, samples: ((int)AlertEngine.SqlCpuClearHold.TotalMinutes + 1), from: at);
            Assert.Single(h.Resolutions);

            Assert.Equal(1, (await investigations.SweepAsync(TestContext.Current.CancellationToken)).Resolved);
            Assert.Equal(0, (await investigations.SweepAsync(TestContext.Current.CancellationToken)).Resolved);
            Assert.Equal("1", Scalar(db, "SELECT count(*) FROM investigation_journal WHERE event = 'resolved'"));
        }
        finally { TearDown(db, dir); }
    }

    [Fact]
    public async Task EveryReaderOfTheAlertLog_ReadsAResolutionRowAsAClearAndNotAsAFire()
    {
        var dir = TempDb(out var db);
        try
        {
            var history = new DuckDbAlertHistoryStore(db);
            var since = DateTime.UtcNow.AddHours(-1);

            /* A critical fire, then the clear: the clear is written by the real recorder, so it is the row the app writes. */
            Exec(db, $"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted, context_json) VALUES ('{DateTime.UtcNow.AddMinutes(-10):yyyy-MM-dd HH:mm:ss.ffffff}', {Key}, '{Name}', 'High CPU', 97, 80, true, 'email', false, '{{\"Severity\":\"Critical\"}}')");
            Assert.Equal(AlertSeverityLevel.Critical, await history.GetLastFiredTierAsync(Key, "High CPU", "CPU Resolved", since));

            await LiteAlertResolutionRecorder.RecordAsync(history, new AlertResolution(Key, Name, "High CPU", "CPU Resolved", "back to 12%"));

            /* Escalation seed: the clear closes the fire, as it does in Darling's store. */
            Assert.Null(await history.GetLastFiredTierAsync(Key, "High CPU", "CPU Resolved", since));
            /* Cooldown and delivered-page seeds: no channel applies, so the clear is neither a send nor a delivered page under its own name. */
            Assert.Null(await history.GetLastEmailSentUtcAsync(Key, "CPU Resolved"));
            Assert.Null(await history.GetLastWebhookSentUtcAsync(Key, "CPU Resolved"));
            Assert.Null(await history.GetLastDeliveredPageUtcAsync(Key, "CPU Resolved"));
            Assert.NotNull(await history.GetLastEmailSentUtcAsync(Key, "High CPU"));

            /* History tab and get_alert_history: the row is shown, and reads as resolved with no delivery claim. */
            var rows = await new LocalDataService(db).GetAlertHistoryAsync(hoursBack: 24, limit: 50);
            Assert.Equal(2, rows.Count);
            var resolved = Assert.Single(rows, r => r.MetricName == "CPU Resolved");
            Assert.True(resolved.IsResolved);
            Assert.False(resolved.AlertSent);
            Assert.Equal(AlertDeliveryStatus.NoChannel, resolved.StatusDisplay);
        }
        finally { TearDown(db, dir); }
    }

    [Fact]
    public async Task TheDailySummaryAlertCount_CountsTheFire_AndNotItsResolutionRow()
    {
        var dir = TempDb(out var db);
        try
        {
            /* A fixed past day, so the pin holds at any hour. */
            var day = new DateTime(2026, 6, 15, 0, 0, 0, DateTimeKind.Utc);
            var history = new DuckDbAlertHistoryStore(db);
            Exec(db, $"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, muted) VALUES ('{day.AddHours(3):yyyy-MM-dd HH:mm:ss.ffffff}', {Key}, '{Name}', 'High CPU', 97, 80, true, 'email', false)");
            await history.RecordAlertAsync(AlertResolutionRecords.BuildResolutionRecord(
                new AlertResolution(Key, Name, "High CPU", "CPU Resolved", "back")) with { AlertTimeUtc = day.AddHours(4) });
            Assert.Equal("2", Scalar(db, "SELECT count(*) FROM config_alert_log"));

            var summary = await new LocalDataService(db).GetDailySummaryAsync(int.Parse(Key, CultureInfo.InvariantCulture), day);

            Assert.NotNull(summary);
            Assert.Equal(1L, summary!.AlertCount);
        }
        finally { TearDown(db, dir); }
    }

    private static void Exec(DuckDbInitializer db, string sql)
    {
        using var connection = db.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
