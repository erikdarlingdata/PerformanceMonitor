/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The FinOps Storage Growth drill buckets its Object Growth Heatmap's days in the selected time display, the rule the FinOps day
/// tables count their days by. With Times set to Server Time on a server eight hours behind UTC, a sample at 03:00 UTC belongs to
/// the evening of the day before, so its column is that day; in UTC it is the UTC day. The columns are read the way the tab reads
/// them, over a store holding one object's samples, with the selected server's clock coming from its open tab, which is where the
/// tab takes it from when the store has not collected one.
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class FinOpsObjectHeatmapDisplayZoneTests : IDisposable
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const int ServerId = 8450;
    private const string DatabaseName = "AppDb";
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "FinOpsHeatmapDays_" + Guid.NewGuid().ToString("N")[..8]);
    private long _nextId = 1;

    public FinOpsObjectHeatmapDisplayZoneTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        try { Directory.Delete(_dir, recursive: true); } catch { /* best-effort cleanup */ }
    }

    /// <summary>The US west coast: seven or eight hours behind UTC, so a UTC small hour is the evening of the day before.</summary>
    private static ServerClock Pacific() => ServerClock.Resolve("Pacific Standard Time", -480);

    /// <summary>
    /// 03:00 UTC today once that hour has passed, else 03:00 UTC yesterday: an instant inside the heatmap's window whose UTC date
    /// and Pacific date differ, since 03:00 UTC is the evening before on the west coast in summer and in winter. The second seed, a
    /// minute later, shares its day in both clocks whatever the wall clock reads, so each read returns one column; an instant taken
    /// from the wall clock could straddle a Pacific midnight with its neighbour.
    /// </summary>
    private static DateTime SampleUtc(DateTime nowUtc)
    {
        var today = new DateTime(nowUtc.Year, nowUtc.Month, nowUtc.Day, 3, 0, 0, DateTimeKind.Unspecified);
        return nowUtc.Hour >= 3 ? today : today.AddDays(-1);
    }

    [Fact]
    public async Task TheObjectGrowthHeatmap_BucketsItsDaysInTheSelectedDisplay()
    {
        using var duckDb = new DuckDbInitializer(Path.Combine(_dir, "heatmap.duckdb"));
        await duckDb.InitializeFromTemplateAsync();
        var sample = SampleUtc(DateTime.UtcNow);
        await SeedObjectAsync(duckDb, sample, 100m);
        await SeedObjectAsync(duckDb, sample.AddMinutes(1), 110m);

        var pacificDay = Assert.Single(LoadHeatmapDays(duckDb, TimeDisplayMode.ServerTime));
        var utcDay = Assert.Single(LoadHeatmapDays(duckDb, TimeDisplayMode.UTC));

        Assert.Equal(sample.Date.AddDays(-1), pacificDay);
        Assert.Equal(sample.Date, utcDay);
    }

    /// <summary>Runs the tab's own object-growth loader over the store in the given display mode and returns the heatmap's day columns.</summary>
    private static DateTime[] LoadHeatmapDays(DuckDbInitializer duckDb, TimeDisplayMode mode)
    {
        var dataService = new LocalDataService(duckDb);
        var days = Array.Empty<DateTime>();
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
                await (Task)typeof(FinOpsTab).GetMethod("LoadObjectGrowthAsync", Instance)!.Invoke(tab, [ServerId, DatabaseName])!;
                ClippedTextProbe.Settle(window);
                var matrix = (FinOpsHeatmapMatrix?)typeof(FinOpsTab).GetField("_objHeatmapMatrix", Instance)!.GetValue(tab);
                days = matrix?.Days ?? [];
            }
            finally
            {
                window.Close();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
            }
        });
        return days;
    }

    /// <summary>One object's one index at <paramref name="at"/>: the heatmap's series sums an object's indexes per day.</summary>
    private async Task SeedObjectAsync(DuckDbInitializer duckDb, DateTime at, decimal reservedMb)
    {
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
     schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
     user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1, $2, $3, 'heatmap-days', $4, $5, 7, 'dbo', 1001, 'Orders', 1, 'PK_Orders', 'CLUSTERED', $6, $6, 1000, 0, 0, 0, 0, 0, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = at.AddDays(-30) });
        cmd.Parameters.Add(new DuckDBParameter { Value = DatabaseName });
        cmd.Parameters.Add(new DuckDBParameter { Value = reservedMb });
        await cmd.ExecuteNonQueryAsync();
    }
}
