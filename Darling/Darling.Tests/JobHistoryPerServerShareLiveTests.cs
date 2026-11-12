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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8133 F5 on a live store: the fleet job-history read with the per-server share guarantees each server with rows its share of the
/// cap before the newest runs of the rest, so one busy server cannot fill the page and a fleet under the cap comes back whole,
/// through the Storage reader and the Viewer's read alike.
/// </summary>
[Collection("live-postgres")]
public sealed class JobHistoryPerServerShareLiveTests
{
    private const int ServerA = 813301;
    private const int ServerB = 813302;
    private const string NameA = "pm-8133-busy";
    private const string NameB = "pm-8133-quiet";
    private static readonly DateTime Since = new(2026, 3, 10, 0, 0, 0, DateTimeKind.Utc);
    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task FleetRead_WithAPerServerLimit_KeepsThatManyOfEachServersNewestRuns()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live job-history tests.");
        var ct = TestContext.Current.CancellationToken;

        var property = typeof(JobHistoryFilter).GetProperty("ShareCapPerServer");
        Assert.True(property is not null, "JobHistoryFilter has no ShareCapPerServer.");
        var viewerSwitch = typeof(ViewerDataService).GetProperty("ShareJobHistoryCapPerServer", BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(viewerSwitch is not null, "ViewerDataService has no ShareJobHistoryCapPerServer.");

        var ok = false;
        await using var connection = new NpgsqlConnection(Cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);
            /* The busy server ran six times in the window, the quiet one twice; both clocks read as UTC (no offset row). */
            for (var i = 1; i <= 6; i++)
            {
                await InsertAsync(connection, ct, i, ServerA, NameA, new DateTime(2026, 3, 10, 6 + i, 0, 0));
            }

            /* On the half hour, so no quiet run ties a busy one and the plain read's tie-break on instance id decides nothing. */
            await InsertAsync(connection, ct, 7, ServerB, NameB, new DateTime(2026, 3, 10, 7, 30, 0));
            await InsertAsync(connection, ct, 8, ServerB, NameB, new DateTime(2026, 3, 10, 8, 30, 0));

            var filter = FilterWithShare();
            await using var dataSource = NpgsqlDataSource.Create(Cs!);
            async Task<List<DarlingJobHistoryRow>> ReadAsync(int cap, JobHistoryFilter? f) =>
                (await DarlingJobHistoryReader.GetAsync(dataSource, Since, null, cap, 30, f, ct)).Where(r => r.ServerId is ServerA or ServerB).ToList();

            /* A fleet that fits under the cap comes back whole. */
            Assert.Equal(8, (await ReadAsync(10, filter)).Count);
            /* Over the cap, every server is guaranteed its share first (the cap over the servers with rows, rounded up), then the
               newest of the rest fill the room: at five the share is three, the quiet server has two, so the busy server's third
               newest run takes the last place. */
            var five = await ReadAsync(5, filter);
            Assert.Equal(3, five.Count(r => r.ServerId == ServerA));
            Assert.Equal(2, five.Count(r => r.ServerId == ServerB));

            var rows = await ReadAsync(4, filter);

            /* Without the share the page would be the busy server's four newest runs and none of the quiet server's. */
            Assert.Equal(2, rows.Count(r => r.ServerId == ServerA));
            Assert.Equal(2, rows.Count(r => r.ServerId == ServerB));
            Assert.Equal(
                new DateTime?[] { new DateTime(2026, 3, 10, 12, 0, 0), new DateTime(2026, 3, 10, 11, 0, 0) },
                rows.Where(r => r.ServerId == ServerA).Select(r => r.RunDateTimeUtc).ToArray());
            Assert.Equal(rows.OrderByDescending(r => r.RunDateTimeUtc).Select(r => r.InstanceId), rows.Select(r => r.InstanceId));

            /* A plain fleet read with the same cap is still the newest four of the fleet. */
            var plain = await ReadAsync(4, null);
            Assert.Equal(4, plain.Count(r => r.ServerId == ServerA));

            /* The Viewer's read shares only when its switch is set, the way the tab sets it; off, it is the plain fleet read. */
            await using var viewer = new ViewerDataService(Cs!);
            async Task<List<ViewerJobHistoryRow>> ViewerReadAsync() =>
                (await viewer.GetJobHistoryAsync(Since, null, 4, cancellationToken: ct)).Where(r => r.ServerId is ServerA or ServerB).ToList();
            Assert.Equal(4, (await ViewerReadAsync()).Count(r => r.ServerId == ServerA));
            viewerSwitch!.SetValue(viewer, true);
            var viewerRows = await ViewerReadAsync();
            Assert.Equal(2, viewerRows.Count(r => r.ServerId == ServerA));
            Assert.Equal(2, viewerRows.Count(r => r.ServerId == ServerB));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    private static JobHistoryFilter FilterWithShare()
    {
        var ctor = typeof(JobHistoryFilter).GetConstructors().Single();
        var args = ctor.GetParameters().Select(p => p.Name == "ShareCapPerServer" ? (object?)true : p.HasDefaultValue ? p.DefaultValue : null).ToArray();
        return (JobHistoryFilter)ctor.Invoke(args);
    }

    private static Task InsertAsync(NpgsqlConnection c, CancellationToken ct, long id, int serverId, string serverName, DateTime localRun) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO job_history (job_history_id, collection_time, server_id, server_name, instance_id, job_id, job_name, job_enabled,
                                       category_name, step_id, step_name, run_status, run_status_desc, run_datetime, run_duration_seconds,
                                       retries_attempted, message)
              VALUES ($1,$2,$3,$4,$5,$6,$7,true,$8,0,'(Job outcome)',1,'The job succeeded.',$9,$10,0,NULL)",
            8_133_000_000L + id, new DateTime(2026, 3, 10, 13, 0, 0, DateTimeKind.Unspecified), serverId, serverName, 8_133_000_000L + id,
            "job-8133", "Every Minute", "Maintenance", localRun, 5L);

    private static async Task CleanupAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var ids = string.Join(", ", new[] { ServerA, ServerB }.Select(i => i.ToString(CultureInfo.InvariantCulture)));
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM job_history WHERE server_id IN ({ids})");
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM server_properties WHERE server_id IN ({ids})");
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM config_monitored_servers WHERE server_id IN ({ids})");
        await DarlingMcpTestData.ExecAsync(c, ct, $"DELETE FROM servers WHERE server_id IN ({ids})");
    }
}
