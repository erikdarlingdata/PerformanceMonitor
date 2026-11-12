/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Viewer's FinOps Storage Growth drill buckets its Object Growth Heatmap's days in the selected time display, the rule the
/// FinOps day tables count their days by. With Times set to Server Time on a server eight hours behind UTC, a sample at 03:00 UTC
/// belongs to the evening of the day before, so its column is that day; in UTC it is the UTC day. The columns are read the way the
/// tab reads them, through its own loader over a store holding one server whose collected clock is on the US west coast.
/// </summary>
/* #1776 own-store: each test seeds its own scratch database, so nothing here shares rows with another test. */
/* Writes ViewerTimeHelper.CurrentDisplayMode, a process-wide static, and loads the Viewer's theme at application scope. The
   collection runs alone, which is what the application-theme guard (#8395) asks of a theme writer. */
[Collection("viewer-time-statics")]
public sealed class ViewerFinOpsObjectHeatmapDisplayZoneTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const string ServerName = "darling-finops-heatmap-days";
    private const string DatabaseName = "HeatDb";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public Task TheObjectGrowthHeatmap_BucketsItsDaysInTheSelectedDisplay() => WithStoreAsync(async (connection, connectionString, ct) =>
    {
        var sample = SampleUtc(DateTime.UtcNow);
        await SeedObjectAsync(connection, sample, 100m, ct);
        await SeedObjectAsync(connection, sample.AddMinutes(1), 110m, ct);

        var serverTimeDay = Assert.Single(LoadHeatmapDays(connectionString, TimeDisplayMode.ServerTime));
        var utcDay = Assert.Single(LoadHeatmapDays(connectionString, TimeDisplayMode.UTC));

        Assert.Equal(sample.Date.AddDays(-1), serverTimeDay);
        Assert.Equal(sample.Date, utcDay);
    });

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

    /// <summary>Runs the tab's own object-growth loader over the store in the given display mode and returns the heatmap's day columns.</summary>
    private static DateTime[] LoadHeatmapDays(string connectionString, TimeDisplayMode mode)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var days = Array.Empty<DateTime>();
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
                await (Task)typeof(FinOpsTab).GetMethod("LoadFinOpsObjectGrowthAsync", Instance)!.Invoke(tab, [DatabaseName])!;
                StaTestThread.Settle(window);
                var matrix = (FinOpsHeatmapMatrix?)typeof(FinOpsTab).GetField("_finopsObjHeatmapMatrix", Instance)!.GetValue(tab);
                days = matrix?.Days ?? [];
            }
            finally
            {
                window.Close();
                if (added is not null) app.Resources.MergedDictionaries.Remove(added);
                ViewerTimeHelper.CurrentDisplayMode = savedMode;
            }
        });
        return days;
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live FinOps heatmap day test.");
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

    /// <summary>One object's one index at <paramref name="at"/>: the heatmap's series sums an object's indexes per day.</summary>
    private static Task SeedObjectAsync(NpgsqlConnection connection, DateTime at, decimal reservedMb, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, object_id, schema_name, table_name, index_id, index_name, reserved_mb, used_mb, total_rows, row_lock_wait_in_ms, index_lock_promotion_count) VALUES ($1, $2, $3, $4, $5, 1001, 'dbo', 'Orders', 1, 'PK_Orders', $6, $6, 1000, 0, 0)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, DatabaseName, reservedMb);
}
