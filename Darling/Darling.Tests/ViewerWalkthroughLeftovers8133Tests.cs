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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8133: the Viewer walkthrough items #8115 left out. The Job History fleet read takes a share of its cap from each server
/// and its count says which servers are at that share; a Fleet Sweeps watch row says its hits are the current run and its
/// first-seen column says it is the first ever sighting; the fleet Overview says it is loading during its first read. The
/// members the fix adds are reached by name, so this class builds against a tree without them and fails at the lookup.
/// </summary>
public sealed class ViewerWalkthroughLeftovers8133Tests
{
    private static readonly Assembly ViewerAssembly = typeof(FleetRollup).Assembly;

    internal static Type ViewerType(string name)
    {
        var type = ViewerAssembly.GetType("PerformanceMonitor.Darling.Viewer." + name);
        Assert.True(type is not null, $"The Viewer has no {name} type.");
        return type!;
    }

    internal static MethodInfo Method(Type type, string name, params Type[] args)
    {
        var method = type.GetMethod(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance, args);
        Assert.True(method is not null, $"{type.Name} has no {name}({string.Join(", ", args.Select(a => a.Name))}).");
        return method!;
    }

    private static string Constant(Type type, string name)
    {
        var field = type.GetField(name, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
        Assert.True(field is not null, $"{type.Name} has no {name} constant.");
        return (string)field!.GetRawConstantValue()!;
    }

    // ── F5: the fleet read's cap is shared out per server ──

    [Theory]
    [InlineData(2000, 1, 2000)]
    [InlineData(2000, 10, 200)]
    [InlineData(2000, 3, 667)]
    [InlineData(2000, 0, 2000)]
    [InlineData(2000, 5000, 1)]
    public void JobHistoryPerServerShare_SplitsTheCapAcrossTheServersOffered(int cap, int servers, int expected)
    {
        var method = Method(typeof(JobHistoryCap), "PerServerShare", typeof(int), typeof(int));
        Assert.Equal(expected, (int)method.Invoke(null, new object[] { cap, servers })!);
    }

    [Theory]
    [InlineData(350, new[] { 200, 150 }, 200, "350 run(s) (showing the newest 200 per server; 1 of 2 server(s) at that cap)")]
    [InlineData(400, new[] { 200, 200 }, 200, "400 run(s) (showing the newest 200 per server; 2 of 2 server(s) at that cap)")]
    [InlineData(270, new[] { 120, 150 }, 200, "270 run(s)")]
    [InlineData(0, new[] { 200, 200 }, 200, "")]
    public void JobHistoryFleetCountText_NamesTheServersAtTheirShare(int shown, int[] rowsPerServer, int share, string expected)
    {
        var method = Method(typeof(JobHistoryCap), "FleetCountText", typeof(int), typeof(IReadOnlyCollection<int>), typeof(int));
        Assert.Equal(expected, (string)method.Invoke(null, new object[] { shown, rowsPerServer, share })!);
    }

    // ── S2: a watch row's hits are the current run; the first-seen column is the first ever sighting ──

    [Theory]
    [InlineData(12, "12 consecutive hits in the current run")]
    [InlineData(1, "1 consecutive hit in the current run")]
    public void FleetSweepPosition_SaysTheHitsAreTheCurrentRun(int hits, string expected)
    {
        var node = new JsonObject
        {
            ["entry_bar_sweeps"] = 2,
            ["exit_bar_sweeps"] = 2,
            ["items"] = new JsonArray(new JsonObject
            {
                ["server_id"] = 7,
                ["server"] = "srv",
                ["item"] = "deadlocks",
                ["condition"] = "deadlocks above the bar",
                ["state"] = "carried",
                ["consecutive_hits"] = hits,
                ["consecutive_misses"] = 0,
                ["first_seen_at"] = "2026-01-01T00:00:00Z",
                ["last_seen_at"] = "2026-02-21T00:00:00Z",
            }),
        };

        var row = Assert.Single(FleetSweepsRows.WatchItems(node));
        Assert.Equal(expected, row.Position);
    }

    /// <summary>
    /// The real Fleet Sweeps tab, rendered on the shared STA thread with the app's theme, bound to one carried watch row: the
    /// first-seen column's header reads "First ever seen" and the row's Position cell reads the current-run sentence.
    /// </summary>
    [Fact]
    public void FleetSweepsGrid_RendersFirstEverSeenAndTheCurrentRun()
    {
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var theme = new ResourceDictionary { Source = new Uri(ThemeUri) };
            app.Resources.MergedDictionaries.Add(theme);
            var window = new Window { Width = 1200, Height = 700, ShowActivated = false, WindowStartupLocation = WindowStartupLocation.Manual, Left = -4000 };
            try
            {
                var tab = new FleetSweepsTab();
                window.Content = tab;
                var grid = (DataGrid)tab.FindName("WatchItemsDataGrid")!;
                grid.ItemsSource = FleetSweepsRows.WatchItems(new JsonObject
                {
                    ["entry_bar_sweeps"] = 2,
                    ["exit_bar_sweeps"] = 2,
                    ["items"] = new JsonArray(new JsonObject
                    {
                        ["server_id"] = 7, ["server"] = "srv", ["item"] = "deadlocks", ["condition"] = "deadlocks above the bar",
                        ["state"] = "carried", ["consecutive_hits"] = 12, ["consecutive_misses"] = 0,
                        ["first_seen_at"] = "2026-01-01T00:00:00Z", ["last_seen_at"] = "2026-02-21T00:00:00Z",
                    }),
                });
                StaTestThread.Settle(window);
                grid.UpdateLayout();

                var firstSeen = grid.Columns.Single(c => (c as DataGridBoundColumn)?.Binding is Binding { Path.Path: "FirstSeen" });
                Assert.Equal("First ever seen", firstSeen.Header as string);

                var position = grid.Columns.Single(c => (c as DataGridBoundColumn)?.Binding is Binding { Path.Path: "Position" });
                var row = (DataGridRow)grid.ItemContainerGenerator.ContainerFromIndex(0);
                Assert.True(row is not null, "The watch grid realized no row for its one item.");
                var cell = position.GetCellContent(row!) as TextBlock;
                Assert.True(cell is not null, "The Position cell realized no TextBlock.");
                Assert.Equal("12 consecutive hits in the current run", cell!.Text);
            }
            finally
            {
                window.Close();
                app.Resources.MergedDictionaries.Remove(theme);
            }
        });
    }

    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    // ── F8(a): the fleet Overview says it is loading while its first read runs ──

    /// <summary>
    /// The text and the decision behind the Overview's loading state. The main window itself is not rendered here: it opens a
    /// store and the sidebar on construction, which no test fixture stands in for.
    /// </summary>
    [Fact]
    public void FleetOverview_HasALoadingTextForItsFirstRead()
    {
        var text = ViewerType("FleetOverviewLoadingText");
        Assert.Equal("Loading the fleet…", Constant(text, "Loading"));

        var shows = Method(text, "Shows", typeof(bool), typeof(int));
        Assert.True((bool)shows.Invoke(null, new object[] { true, 0 })!);
        Assert.False((bool)shows.Invoke(null, new object[] { false, 0 })!);
        Assert.False((bool)shows.Invoke(null, new object[] { true, 3 })!);
    }
}

/// <summary>
/// #8133 F5 on a live store: the fleet job-history read with a per-server limit keeps that many of each server's newest runs,
/// so one busy server cannot fill the page, through the Storage reader and the Viewer's read alike.
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

        var property = typeof(JobHistoryFilter).GetProperty("PerServerLimit");
        Assert.True(property is not null, "JobHistoryFilter has no PerServerLimit.");
        var viewerRead = typeof(ViewerDataService).GetMethod(nameof(ViewerDataService.GetJobHistoryAsync));
        Assert.True(viewerRead is not null && viewerRead.GetParameters().Any(p => p.Name == "perServerLimit"),
            "ViewerDataService.GetJobHistoryAsync has no perServerLimit parameter.");

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

            var filter = FilterWithPerServerLimit(2);
            await using var dataSource = NpgsqlDataSource.Create(Cs!);
            var rows = await DarlingJobHistoryReader.GetAsync(dataSource, Since, null, 4, 30, filter, ct);
            rows = rows.Where(r => r.ServerId is ServerA or ServerB).ToList();

            /* Without the share the page would be the busy server's four newest runs and none of the quiet server's. */
            Assert.Equal(2, rows.Count(r => r.ServerId == ServerA));
            Assert.Equal(2, rows.Count(r => r.ServerId == ServerB));
            Assert.Equal(
                new DateTime?[] { new DateTime(2026, 3, 10, 12, 0, 0), new DateTime(2026, 3, 10, 11, 0, 0) },
                rows.Where(r => r.ServerId == ServerA).Select(r => r.RunDateTimeUtc).ToArray());
            Assert.Equal(rows.OrderByDescending(r => r.RunDateTimeUtc).Select(r => r.InstanceId), rows.Select(r => r.InstanceId));

            /* A plain fleet read with the same cap is still the newest four of the fleet. */
            var plain = (await DarlingJobHistoryReader.GetAsync(dataSource, Since, null, 4, 30, null, ct)).Where(r => r.ServerId is ServerA or ServerB).ToList();
            Assert.Equal(4, plain.Count(r => r.ServerId == ServerA));

            await using var viewer = new ViewerDataService(Cs!);
            var args = viewerRead!.GetParameters().Select(p => p.Name switch
            {
                "sinceUtc" => (object?)Since,
                "limit" => 4,
                "perServerLimit" => 2,
                "cancellationToken" => ct,
                _ => p.HasDefaultValue ? p.DefaultValue : null,
            }).ToArray();
            var viewerRows = await (Task<List<ViewerJobHistoryRow>>)viewerRead.Invoke(viewer, args)!;
            viewerRows = viewerRows.Where(r => r.ServerId is ServerA or ServerB).ToList();
            Assert.Equal(2, viewerRows.Count(r => r.ServerId == ServerA));
            Assert.Equal(2, viewerRows.Count(r => r.ServerId == ServerB));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(Cs!, ok, async (cleanup, cct) => await CleanupAsync(cleanup, cct));
        }
    }

    private static JobHistoryFilter FilterWithPerServerLimit(int share)
    {
        var ctor = typeof(JobHistoryFilter).GetConstructors().Single();
        var args = ctor.GetParameters().Select(p => p.Name == "PerServerLimit" ? (object?)share : p.HasDefaultValue ? p.DefaultValue : null).ToArray();
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
