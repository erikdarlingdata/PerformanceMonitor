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
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE its own
   scratch database (ScratchPostgres) and drops it, so it shares no table with another class. */

/// <summary>
/// #7688: the Viewer's Scheduler Issues grid read on SQL Server on Linux. Idle reads 0 there, so Other-Process CPU (100 minus SQL CPU
/// minus idle) read 100 minus the SQL figure on every minute and every sample passed the significance test. The read clears Other-Process
/// CPU and Idle for a server whose stored host platform is not Windows, so a quiet sample is no row and a busy SQL sample is a row with
/// blank Other-Process CPU and Idle. A Windows or unknown platform keeps the figures. Lite twin: <c>SystemHealthHostCpuToolTests</c>.
/// </summary>
public sealed class ViewerSchedulerIssuesHostCpuLiveTests
{
    private const int ServerId = 7688;

    private static async Task<(int? SqlCpu, int? OtherProcessCpu, int? SystemIdle)[]> ReadAsync(string? hostOs, int sqlCpu, int idle, CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Viewer scheduler tests.");

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var end = DateTime.UtcNow;
        var eventTime = end.AddHours(-2);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "viewer-sched-hostcpu", ct);

            var xml = $"""
                <event name="scheduler_monitor_system_health_ring_buffer_recorded" package="sqlos" timestamp="{eventTime:yyyy-MM-dd'T'HH:mm:ss.fff}Z">
                  <data name="process_utilization"><value>{sqlCpu}</value></data>
                  <data name="system_idle"><value>{idle}</value></data>
                  <data name="memory_utilization"><value>100</value></data>
                </event>
                """;
            await using (var insert = new NpgsqlCommand("""
                INSERT INTO collect.system_health_events
                    (system_health_event_id, collection_time, server_id, server_name, event_time, event_type, event_xml)
                VALUES (1, $1, $2, 'viewer-sched-hostcpu', $1, 'scheduler_monitor_system_health_ring_buffer_recorded', $3)
                """, connection))
            {
                insert.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
                insert.Parameters.AddWithValue(ServerId);
                insert.Parameters.AddWithValue(xml);
                await insert.ExecuteNonQueryAsync(ct);
            }

            if (hostOs is not null)
            {
                await using var properties = new NpgsqlCommand(
                    "INSERT INTO collect.server_properties (collection_id, collection_time, server_id, server_name, host_os_version) VALUES (1, $1, $2, 'viewer-sched-hostcpu', $3)", connection);
                properties.Parameters.AddWithValue(DateTime.SpecifyKind(eventTime, DateTimeKind.Unspecified));
                properties.Parameters.AddWithValue(ServerId);
                properties.Parameters.AddWithValue(hostOs);
                await properties.ExecuteNonQueryAsync(ct);
            }
        }

        var rows = await new ViewerDataService(scratch.ConnectionString).GetSchedulerIssuesAsync(ServerId, end.AddDays(-1), end, ct);
        return rows.Select(r => (r.SqlCpuUtilization, r.OtherProcessCpu, r.SystemIdle)).ToArray();
    }

    [Fact]
    public async Task OnLinux_AQuietSampleIsNotAnIssue()
    {
        Assert.Empty(await ReadAsync("Ubuntu", sqlCpu: 0, idle: 0, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task OnLinux_ABusySqlSampleIsAnIssue_WithBlankOtherProcessAndIdle()
    {
        var row = Assert.Single(await ReadAsync("Ubuntu", sqlCpu: 94, idle: 0, TestContext.Current.CancellationToken));

        Assert.Equal(94, row.SqlCpu);
        Assert.Null(row.OtherProcessCpu);
        Assert.Null(row.SystemIdle);
    }

    [Theory]
    [InlineData("Windows")]
    [InlineData(null)]
    public async Task OnWindowsOrAnUnknownPlatform_AHighOtherProcessSampleIsKept(string? hostOs)
    {
        var row = Assert.Single(await ReadAsync(hostOs, sqlCpu: 5, idle: 0, TestContext.Current.CancellationToken));

        Assert.Equal(95, row.OtherProcessCpu);
        Assert.Equal(0, row.SystemIdle);
    }
}
