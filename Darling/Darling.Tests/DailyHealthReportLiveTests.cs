/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Reports;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7041: the Daily Health Report builds from rows the store holds. One SQL Server target with alerts, failed jobs and disk snapshots
/// inside and outside the 24-hour period: the report counts only the period's rows, leaves out resolutions, the report's own delivery rows
/// and step-level job failures, and lists only the volume under the low-disk bars.
/// </summary>
[Collection("live-postgres")]
public sealed class DailyHealthReportLiveTests
{
    private const int ServerId = -704_101;
    private const string ServerName = "darling-7041-sql";

    [Fact]
    public async Task Build_CountsOnlyThePeriod_AndReportsAlertsJobsAndLowDisk()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #7041 Daily Health Report live test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(connectionString!);
        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);

            /* Alerts: two High CPU and one Blocking inside the period. Outside or not counted: a High CPU 30 hours ago, a resolution row,
               and the report's own delivery row. */
            await InsertAlertAsync(connection, ct, "High CPU", now.AddHours(-2));
            await InsertAlertAsync(connection, ct, "High CPU", now.AddHours(-5));
            await InsertAlertAsync(connection, ct, "Blocking", now.AddHours(-3));
            await InsertAlertAsync(connection, ct, "High CPU", now.AddHours(-30));
            await InsertAlertAsync(connection, ct, "High CPU Cleared", now.AddHours(-1));
            await InsertAlertAsync(connection, ct, DailyHealthReportBuilder.DeliveryMetricName, now.AddHours(-1));

            /* Jobs: "Nightly load" failed three times in the period, "Old job" 30 hours ago, "Fine job" succeeded, and "Step only" has a failed
               step but its job outcome (step 0) did not fail. */
            await InsertJobAsync(connection, ct, 1, "Nightly load", 0, 0, now.AddHours(-2));
            await InsertJobAsync(connection, ct, 2, "Nightly load", 0, 0, now.AddHours(-3));
            await InsertJobAsync(connection, ct, 3, "Nightly load", 0, 0, now.AddHours(-4));
            await InsertJobAsync(connection, ct, 4, "Old job", 0, 0, now.AddHours(-30));
            await InsertJobAsync(connection, ct, 5, "Fine job", 1, 0, now.AddHours(-1));
            await InsertJobAsync(connection, ct, 6, "Step only", 0, 1, now.AddHours(-1));

            /* Disk: at the newest snapshot in the period C: is half free and D: is 2 percent free. An older snapshot with E: nearly full is
               outside the period and must not appear. */
            await InsertVolumeAsync(connection, ct, 1, now.AddHours(-1), "C:\\", 100_000, 50_000);
            await InsertVolumeAsync(connection, ct, 2, now.AddHours(-1), "D:\\", 100_000, 2_000);
            await InsertVolumeAsync(connection, ct, 3, now.AddHours(-30), "E:\\", 100_000, 100);

            var data = await new DailyHealthReportBuilder(dataSource).BuildDataAsync(now, ct);

            var server = Assert.Single(data.Servers, s => s.ServerId == ServerId);
            Assert.Contains(ServerName, server.Name, StringComparison.Ordinal);
            Assert.Equal(
                new[] { ("High CPU", 2), ("Blocking", 1) },
                server.Alerts.Select(a => (a.Metric, a.Count)).ToArray());
            var job = Assert.Single(server.FailedJobs);
            Assert.Equal(("Nightly load", 3), (job.JobName, job.Failures));
            Assert.Equal(now.AddHours(-2), job.LastFailureUtc);
            Assert.Equal("D:\\", Assert.Single(server.LowDisk).Volume);

            var rendered = DailyHealthReportBuilder.Render(data);
            Assert.Equal(data.Servers.Count, rendered.ServerCount);
            Assert.Contains(ServerName, rendered.Text, StringComparison.Ordinal);
            Assert.Contains("Nightly load", rendered.Html, StringComparison.Ordinal);
            Assert.StartsWith(data.Title + ": " + data.Servers.Count, rendered.Subject, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, CleanupAsync);
        }
    }

    private static Task InsertAlertAsync(NpgsqlConnection connection, CancellationToken ct, string metric, DateTime at) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_alert_log
    (alert_time, server_id, server_name, metric_name, current_value, threshold_value,
     alert_sent, notification_type, send_error, muted, dismissed, detail_text, context_json)
VALUES ($1, $2, $3, $4, 1, 1, FALSE, 'none', NULL, FALSE, FALSE, NULL, NULL)",
            DarlingMcpTestData.Naive(at), ServerId, ServerName, metric);

    private static Task InsertJobAsync(NpgsqlConnection connection, CancellationToken ct, long id, string job, int runStatus, int stepId, DateTime at) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id,
                                       job_id, job_name, job_enabled, category_name, step_id, step_name, run_status,
                                       run_status_desc, run_datetime, run_duration_seconds, retries_attempted, message)
              VALUES ($1,$2,$3,$4,$1,$5,$5,true,'cat',$6,'step',$7,$8,$9,5,0,NULL)",
            7_041_000L + id, at, ServerId, ServerName, job, stepId, runStatus, runStatus == 0 ? "The job failed." : "The job succeeded.", at);

    private static Task InsertVolumeAsync(NpgsqlConnection connection, CancellationToken ct, long id, DateTime at, string mount, double totalMb, double freeMb) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, volume_mount_point, volume_total_mb, volume_free_mb)
              VALUES ($1, $2, $3, $4, $5, $6, $7)",
            7_041_000L + id, at, ServerId, ServerName, mount, totalMb, freeMb);

    private static async Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_alert_log WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM job_history WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM database_size_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
    }
}
