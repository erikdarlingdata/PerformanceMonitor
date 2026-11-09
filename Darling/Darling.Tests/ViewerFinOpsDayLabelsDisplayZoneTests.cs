/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's FinOps day tables (the 7-Day Provisioning Trend on Utilization and Memory Grant Efficiency on Optimization) count
/// their days in the selected time display, as every other time on the page reads. With Times set to Server Time on a server eight
/// hours behind UTC, a sample at 03:00 UTC belongs to the evening of the day before, so its row reads that day; in UTC it reads the
/// UTC day. The rows are read the way the tab reads them, through its own loaders over a store holding one server whose collected
/// clock is on the US west coast.
/// </summary>
/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/* Writes ViewerTimeHelper.CurrentDisplayMode, a process-wide static, and loads the Viewer's theme at application scope. The
   collection runs alone, which is what the application-theme guard (#8395) asks of a theme writer. */
[Collection("viewer-time-statics")]
public sealed class ViewerFinOpsDayLabelsDisplayZoneTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string ServerName = "darling-finops-day-labels";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public Task TheProvisioningTrend_LabelsEachRowWithTheSelectedDisplaysDay() => WithStoreAsync(async (connection, connectionString, ct) =>
    {
        var sample = SampleUtc(DateTime.UtcNow);
        await SeedCpuAsync(connection, sample, ct);
        await SeedCpuAsync(connection, sample.AddMinutes(1), ct);
        /* The page loads the trend only for a server whose summary read has a row, and that read joins the server's newest memory
           sample; the trend's rows come from the CPU samples alone, so this row adds no day. */
        await SeedMemoryAsync(connection, sample, ct);

        var serverTimeDay = Assert.Single(LoadRows(connectionString, TimeDisplayMode.ServerTime, "LoadFinOpsUtilizationAsync", "FinOpsProvisioningTrendGrid").Cast<ProvisioningTrendRow>()).Day;
        var utcDay = Assert.Single(LoadRows(connectionString, TimeDisplayMode.UTC, "LoadFinOpsUtilizationAsync", "FinOpsProvisioningTrendGrid").Cast<ProvisioningTrendRow>()).Day;

        Assert.Equal(sample.Date.AddDays(-1), serverTimeDay);
        Assert.Equal(sample.Date, utcDay);
    });

    [Fact]
    public Task MemoryGrantEfficiency_LabelsEachRowWithTheSelectedDisplaysDay() => WithStoreAsync(async (connection, connectionString, ct) =>
    {
        var sample = SampleUtc(DateTime.UtcNow);
        await SeedGrantAsync(connection, sample, ct);
        await SeedGrantAsync(connection, sample.AddMinutes(1), ct);

        var serverTimeDay = Assert.Single(LoadRows(connectionString, TimeDisplayMode.ServerTime, "LoadFinOpsMemoryGrantEfficiencyAsync", "FinOpsMemoryGrantEfficiencyDataGrid").Cast<MemoryGrantEfficiencyRow>()).Day;
        var utcDay = Assert.Single(LoadRows(connectionString, TimeDisplayMode.UTC, "LoadFinOpsMemoryGrantEfficiencyAsync", "FinOpsMemoryGrantEfficiencyDataGrid").Cast<MemoryGrantEfficiencyRow>()).Day;

        Assert.Equal(sample.Date.AddDays(-1), serverTimeDay);
        Assert.Equal(sample.Date, utcDay);
    });

    /// <summary>
    /// An instant inside the last 24 hours (so both day reads hold it) whose UTC date and west-coast date differ: a UTC hour before
    /// 07:00, which is the evening before on the west coast in summer and in winter.
    /// </summary>
    private static DateTime SampleUtc(DateTime nowUtc)
    {
        var t = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, nowUtc.Hour, nowUtc.Minute, 0, DateTimeKind.Unspecified).AddHours(-1);
        return t.Hour < 7 ? t : t.Date.AddHours(3);
    }

    /// <summary>Runs the tab's own loader over the store in the given display mode and returns the rows its grid then holds.</summary>
    private static List<object> LoadRows(string connectionString, TimeDisplayMode mode, string loader, string gridName)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var rows = new List<object>();
        StaTestThread.Run(async () =>
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("NumericCell"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }
            await using var dataService = new ViewerDataService(connectionString);
            var tab = new FinOpsTab();
            tab.Initialize(dataService);
            tab.SetServers([new DarlingServer(ServerId, ServerName, ServerName, true, null)], ServerId);
            var window = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                StaTestThread.Settle(window);
                await (Task)typeof(FinOpsTab).GetMethod(loader, Instance)!.Invoke(tab, [])!;
                StaTestThread.Settle(window);
                rows.AddRange(((DataGrid)tab.FindName(gridName)).Items.Cast<object>());
            }
            finally
            {
                window.Close();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
                ViewerTimeHelper.CurrentDisplayMode = savedMode;
            }
        });
        return rows;
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live FinOps day-label test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        /* The server's collected clock: the US west coast, eight hours behind UTC in winter and seven in summer. Server Time on this
           server is that zone; the tab reads it from the store, as it does for every other FinOps row. */
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes, time_zone_id) VALUES ($1, $2, $3, $4, -480, 'Pacific Standard Time')",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddHours(-1)), ServerId, ServerName);
        await body(connection, scratch.ConnectionString, ct);
    }

    private static Task SeedCpuAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, 40, 5)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName);

    private static Task SeedMemoryAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 16384, 12000, 12288, 10000, 512, 50)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName);

    private static Task SeedGrantAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, target_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta) VALUES ($1, $2, $3, $4, 1000, 100, 50, 1, 0, 0, 0)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName);
}
