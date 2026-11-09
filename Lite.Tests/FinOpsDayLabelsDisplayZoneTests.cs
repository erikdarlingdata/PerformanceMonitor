/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The FinOps day tables (the 7-Day Provisioning Trend on Utilization and Memory Grant Efficiency on Optimization) count their
/// days in the selected time display, as every other time on the page reads. With Times set to Server Time on a server eight
/// hours behind UTC, a sample at 03:00 UTC belongs to the evening of the day before, so the row for it reads that day; in UTC it
/// reads the UTC day. The rows are read the way the tab reads them, over a store holding one sample, with the selected server's
/// clock coming from its open tab, which is where the tab takes it from when the store has not collected one.
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class FinOpsDayLabelsDisplayZoneTests : IDisposable
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const int ServerId = 8389;
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "FinOpsDayLabels_" + Guid.NewGuid().ToString("N")[..8]);
    private long _nextId = 1;

    public FinOpsDayLabelsDisplayZoneTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>The US west coast: seven or eight hours behind UTC, so a UTC small hour is the evening of the day before.</summary>
    private static ServerClock Pacific() => ServerClock.Resolve("Pacific Standard Time", -480);

    /// <summary>
    /// 03:00 UTC today once that hour has passed, else 03:00 UTC yesterday: an instant inside the last 24 hours (so both day reads
    /// hold it) whose UTC date and Pacific date differ, since 03:00 UTC is the evening before on the west coast in summer and in
    /// winter. The second seed, a minute later, shares its day in both clocks whatever the wall clock reads, so each read returns
    /// one row; an instant taken from the wall clock could straddle a Pacific midnight with its neighbour.
    /// </summary>
    private static DateTime SampleUtc(DateTime nowUtc)
    {
        var today = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, 3, 0, 0, DateTimeKind.Unspecified);
        return nowUtc.Hour >= 3 ? today : today.AddDays(-1);
    }

    [Fact]
    public async Task TheProvisioningTrend_LabelsEachRowWithTheSelectedDisplaysDay()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "trend.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        var sample = SampleUtc(DateTime.UtcNow);
        await SeedCpuAsync(duckDb, sample);
        await SeedCpuAsync(duckDb, sample.AddMinutes(1));
        /* The page loads the trend only for a server with a memory sample (the summary's point-in-time read joins its newest memory
           row), and the trend's rows come from the CPU samples alone, so this row adds no day. */
        await SeedMemoryAsync(duckDb, sample);

        var pacificDay = Assert.Single(LoadRows(duckDb, TimeDisplayMode.ServerTime, "LoadUtilizationAsync", "ProvisioningTrendGrid").Cast<ProvisioningTrendRow>()).Day;
        var utcDay = Assert.Single(LoadRows(duckDb, TimeDisplayMode.UTC, "LoadUtilizationAsync", "ProvisioningTrendGrid").Cast<ProvisioningTrendRow>()).Day;

        Assert.Equal(sample.Date.AddDays(-1), pacificDay);
        Assert.Equal(sample.Date, utcDay);
    }

    [Fact]
    public async Task MemoryGrantEfficiency_LabelsEachRowWithTheSelectedDisplaysDay()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "grants.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        var sample = SampleUtc(DateTime.UtcNow);
        await SeedGrantAsync(duckDb, sample);
        await SeedGrantAsync(duckDb, sample.AddMinutes(1));

        var pacificDay = Assert.Single(LoadRows(duckDb, TimeDisplayMode.ServerTime, "LoadMemoryGrantEfficiencyAsync", "MemoryGrantEfficiencyDataGrid").Cast<MemoryGrantEfficiencyRow>()).Day;
        var utcDay = Assert.Single(LoadRows(duckDb, TimeDisplayMode.UTC, "LoadMemoryGrantEfficiencyAsync", "MemoryGrantEfficiencyDataGrid").Cast<MemoryGrantEfficiencyRow>()).Day;

        Assert.Equal(sample.Date.AddDays(-1), pacificDay);
        Assert.Equal(sample.Date, utcDay);
    }

    /// <summary>Runs the tab's own loader over the store in the given display mode and returns the rows its grid then holds.</summary>
    private static List<object> LoadRows(DuckDbInitializer duckDb, TimeDisplayMode mode, string loader, string gridName)
    {
        var dataService = new LocalDataService(duckDb);
        var rows = new List<object>();
        StaTestThread.Run(async () =>
        {
            ServerTimeHelper.CurrentDisplayMode = mode;
            ServerTimeHelper.ActiveServerClock = Pacific();
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("DataGridCellText"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }
            var tab = new FinOpsTab();
            typeof(FinOpsTab).GetField("_dataService", Instance)!.SetValue(tab, dataService);
            /* The selected server's clock: the store has collected none for it, so the tab takes its open tab's. */
            typeof(FinOpsTab).GetField("_openTabClock", Instance)!.SetValue(tab, new Func<int, ServerClock?>(_ => Pacific()));
            var window = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                ClippedTextProbe.Settle(window);
                await (Task)typeof(FinOpsTab).GetMethod(loader, Instance)!.Invoke(tab, [ServerId])!;
                ClippedTextProbe.Settle(window);
                rows.AddRange(((DataGrid)tab.FindName(gridName)).Items.Cast<object>());
            }
            finally
            {
                window.Close();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
        return rows;
    }

    private async Task SeedCpuAsync(DuckDbInitializer duckDb, DateTime at)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, 'day-labels', $2, 40, 5)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedMemoryAsync(DuckDbInitializer duckDb, DateTime at)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
VALUES ($1, $2, $3, 'day-labels', 16384, 4096, 12288, 12000, 10000)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedGrantAsync(DuckDbInitializer duckDb, DateTime at)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, resource_semaphore_id, waiter_count, grantee_count,
     timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb, used_memory_mb)
VALUES ($1, $2, $3, 'day-labels', 0, 0, 1, 0, 0, 100, 50)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        await cmd.ExecuteNonQueryAsync();
    }
}
