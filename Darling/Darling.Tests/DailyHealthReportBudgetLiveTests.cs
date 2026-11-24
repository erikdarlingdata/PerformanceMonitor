/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9398: get_daily_health_report held no response budget, so a busy day (many servers with failed Agent jobs and alerts) answered more than
/// twice the shared 32 KB with no budget keys. The servers are worst first, so the cut drops the healthiest tail, and summary and text
/// still count every server.
/// </summary>
[Collection("live-postgres")]
public sealed class DailyHealthReportBudgetLiveTests
{
    private const int FirstServerId = -939_800;
    private const int ServerCount = 40;
    private const int JobsPerServer = 14;

    [Fact]
    public async Task BusyDay_McpToolStaysUnderTheBudget_AndTheWebReadKeepsEveryServer()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #9398 Daily Health Report budget live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            /* The period the default read resolves, so the seeded rows sit inside it. The day is read once here and passed to both reads
               below, so a run that crosses a period boundary cannot send the tool to a period the rows are not in. */
            var config = await DarlingAlertReader.GetAlertConfigurationAsync(dataSource, ct);
            var sendAt = DailyHealthReportSettings.ResolveSendAt(config.Settings?.DailyReportSendAt);
            var zone = DailyHealthReportSettings.ResolveZone(config.Settings?.DailyReportTimeZone);
            var day = DailyHealthReportPeriod.LatestEndedDate(DateTime.UtcNow, sendAt, zone);
            var dayText = day.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture);
            var periodEnd = DailyHealthReportPeriod.EndUtcFor(day, sendAt, zone);
            var anchor = DarlingMcpTestData.Naive(periodEnd.AddHours(-3));

            for (var s = 0; s < ServerCount; s++)
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, FirstServerId - s, $"darling-9398-{s:00}", ct);
            }

            /* Every server: JobsPerServer failed Agent jobs with a message, and alerts of three metrics, a different total per server so the
               worst-first order is not just the name order. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $@"
INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                         job_id, job_name, job_enabled, category_name, step_id, step_name, run_status,
                         run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
SELECT 9398000000 + s * 100 + j, $1::timestamp - make_interval(mins => j), {FirstServerId} - s, 'darling-9398-' || lpad(s::text, 2, '0'), 9398000000 + s * 100 + j,
       'job ' || j, 'Nightly maintenance job number ' || j, true, 'cat', 0, 'step', 0,
       'The job failed.', $1::timestamp - make_interval(mins => j), 5, 0, repeat('The step failed with an error the job could not recover from. ', 3)
FROM generate_series(0, {ServerCount - 1}) AS s, generate_series(1, {JobsPerServer}) AS j", anchor);

            await DarlingMcpTestData.ExecAsync(connection, ct, $@"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
SELECT $1::timestamp - make_interval(secs => a * 10 + m), {FirstServerId} - s, 'darling-9398-' || lpad(s::text, 2, '0'),
       (ARRAY['High CPU', 'Blocking', 'Deadlocks'])[m], 1, 1, FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL
FROM generate_series(0, {ServerCount - 1}) AS s, generate_series(1, 3) AS m, generate_series(1, 2 + (s * 7) % 11) AS a", anchor);

            /* The whole report, in the order the builder gives it: the reference for what the cut must keep. */
            var data = await new DailyHealthReportBuilder(dataSource).BuildDataAsync(periodEnd, ct);
            var rendered = DailyHealthReportBuilder.Render(data);

            var reply = await DarlingMcpDailyReportTools.GetDailyHealthReport(dataSource, dayText, ct);

            Assert.False(McpHelpers.IsErrorEnvelope(reply), reply);
            Assert.True(Encoding.UTF8.GetByteCount(reply) <= McpResponseBudget.DefaultBytes,
                $"the reply is {Encoding.UTF8.GetByteCount(reply):N0} bytes, over the {McpResponseBudget.DefaultBytes:N0}-byte budget");

            using var doc = JsonDocument.Parse(reply);
            var root = doc.RootElement;
            var kept = root.GetProperty("servers").EnumerateArray().Select(e => e.GetProperty("server_id").GetInt32()).ToList();
            var total = data.Servers.Count;
            Assert.True(total >= ServerCount, "the seeded servers are all in the report");
            Assert.InRange(kept.Count, 2, total - 1);

            Assert.True(root.GetProperty("budget_truncated").GetBoolean());
            var leftOff = total - kept.Count;
            Assert.Contains(leftOff.ToString("#,0", System.Globalization.CultureInfo.InvariantCulture), root.GetProperty("budget_note").GetString());

            /* The kept entries are the worst-first prefix of the whole report. */
            Assert.Equal(data.Servers.Take(kept.Count).Select(s => s.ServerId), kept);

            /* summary and text still count every server, not just the ones kept. */
            var summary = root.GetProperty("summary");
            Assert.Equal(total, summary.GetProperty("servers").GetInt32());
            Assert.Equal(data.AlertsFired, summary.GetProperty("alerts_fired").GetInt32());
            Assert.Equal(total, summary.GetProperty("healthy_count").GetInt32() + summary.GetProperty("warning_count").GetInt32()
                + summary.GetProperty("critical_count").GetInt32() + summary.GetProperty("offline_count").GetInt32());
            Assert.Equal(rendered.Text, root.GetProperty("text").GetString());

            /* The web read of the same day passes no budget: every server, in the same order, and no budget keys saying it was cut. */
            var webReply = await DarlingMcpDailyReportTools.GetDailyHealthReportWithin(dataSource, dayText, null, ct);
            Assert.False(McpHelpers.IsErrorEnvelope(webReply), webReply);
            Assert.True(Encoding.UTF8.GetByteCount(webReply) > McpResponseBudget.DefaultBytes, "the seeded day is bigger than the budget, or this test proves nothing");
            using var webDoc = JsonDocument.Parse(webReply);
            var webRoot = webDoc.RootElement;
            var webServers = webRoot.GetProperty("servers").EnumerateArray().Select(e => e.GetProperty("server_id").GetInt32()).ToList();
            Assert.Equal(data.Servers.Select(x => x.ServerId), webServers);
            Assert.False(webRoot.TryGetProperty("budget_truncated", out _));
            Assert.False(webRoot.TryGetProperty("budget_note", out _));
            Assert.Equal(rendered.Text, webRoot.GetProperty("text").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var low = FirstServerId - ServerCount;
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_id > $1 AND server_id <= $2", low, FirstServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM job_history WHERE server_id > $1 AND server_id <= $2", low, FirstServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id > $1 AND server_id <= $2", low, FirstServerId);
    }
}
