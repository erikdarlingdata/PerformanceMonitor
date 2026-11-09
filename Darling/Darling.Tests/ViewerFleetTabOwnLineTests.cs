/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8379: a fleet tab that owns its status line keeps it true after the window's refresh as well. A Fleet Sweeps load run by one of the
/// tab's own handlers (here the closed-items box) replaces the failure line it answers with the refreshed stamp once every load has
/// finished, and an Alert History load that a newer load superseded raises no failure line over that newer load's stamp. The theme is
/// the host window's own, so the process-wide application resources are left as they are for the classes that run beside this one.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live test reaches DARLING_TEST_PG only to CREATE and DROP its own
   database through ScratchPostgres, then works entirely inside it. */
[Collection("viewer-time-statics")]
public sealed class ViewerFleetTabOwnLineTests
{
    private const string ThemeUri = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml";

    /// <summary>A store nothing listens on: every read fails after its connection attempt, asynchronously.</summary>
    private const string ClosedStore = "Host=127.0.0.1;Port=1;Database=darling;Username=darling;Timeout=5";

    private static readonly BindingFlags Instance = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    [Fact]
    public async Task FleetSweeps_AWatchItemsLoadThatSucceedsFromItsBox_ReplacesTheFailureLine_WithTheRefreshedStamp()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live Fleet Sweeps status-line test.");
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, CancellationToken.None);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync();
            await PgMigrations.MigrateAsync(connection, CancellationToken.None);
        }

        var live = scratch.ConnectionString;
        var findings = new List<string>();
        StaTestThread.Run(async () =>
        {
            var tab = new FleetSweepsTab();
            var lines = new List<string>();
            tab.StatusChanged += lines.Add;
            var host = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStartupLocation = WindowStartupLocation.Manual, ShowInTaskbar = false };
            host.Resources.MergedDictionaries.Add(Theme());
            StaTestThread.ShowOffScreen(host);
            ClippedTextProbe.Settle(host);
            try
            {
                var box = (CheckBox)tab.FindName("ShowClosedCheckBox");
                var grid = (DataGrid)tab.FindName("WatchItemsDataGrid");

                /* The timer's refresh against a store that answers: an empty store has no sweeps, every read finishes, and the tab stamps. */
                tab.Initialize(new ViewerDataService(live));
                await tab.RefreshAsync();
                ClippedTextProbe.Settle(host);
                if (lines.Count == 0 || !lines[^1].StartsWith("fleet sweeps — refreshed", StringComparison.Ordinal))
                {
                    findings.Add($"after a refresh against an answering store the last line is \"{Last(lines)}\"");
                }

                /* The store stops answering and the operator ticks the closed-items box: the watch items read fails and says so. */
                tab.Initialize(new ViewerDataService(ClosedStore));
                var failed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                void OnFailure(string line)
                {
                    if (line.StartsWith("failed to load the fleet sweep watch items", StringComparison.Ordinal))
                    {
                        failed.TrySetResult();
                    }
                }

                tab.StatusChanged += OnFailure;
                box.IsChecked = !(box.IsChecked ?? false);
                await failed.Task;
                tab.StatusChanged -= OnFailure;
                ClippedTextProbe.Settle(host);

                /* The store answers again and the operator ticks the box back: the read finishes and binds a new list to the grid. */
                tab.Initialize(new ViewerDataService(live));
                var bound = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var itemsSource = DependencyPropertyDescriptor.FromProperty(ItemsControl.ItemsSourceProperty, typeof(DataGrid));
                EventHandler onBound = (_, _) => bound.TrySetResult();
                itemsSource.AddValueChanged(grid, onBound);
                box.IsChecked = !(box.IsChecked ?? false);
                await bound.Task;
                itemsSource.RemoveValueChanged(grid, onBound);
                ClippedTextProbe.Settle(host);

                if (lines.Count == 0 || !lines[^1].StartsWith("fleet sweeps — refreshed", StringComparison.Ordinal))
                {
                    findings.Add($"after the box's load finished against an answering store the last line is \"{Last(lines)}\", not the refreshed stamp");
                }
            }
            finally
            {
                host.Close();
            }
        });

        Assert.True(findings.Count == 0, string.Join(Environment.NewLine, findings));
    }

    [Fact]
    public void AlertHistory_ALoadThatANewerLoadSuperseded_RaisesNoLineWhenItFails()
    {
        var raised = new List<string>();
        StaTestThread.Run(async () =>
        {
            var tab = new AlertsHistoryTab();
            tab.StatusChanged += raised.Add;
            tab.Initialize(new ViewerDataService(ClosedStore));

            /* The read starts and parks on its connection attempt; a newer load claims the generation before it fails, as a Server
               filter pick does while the timer's read is in flight. */
            var older = tab.RefreshAlertsAsync();
            var loads = typeof(AlertsHistoryTab).GetField("_loads", Instance)!.GetValue(tab)!;
            loads.GetType().GetMethod("Claim", Instance)!.Invoke(loads, new object[] { "LoadAlertsAsync" });
            await older;
        });

        Assert.True(raised.Count == 0, "a superseded load raised: " + string.Join(" | ", raised));
    }

    private static string Last(List<string> lines) => lines.Count == 0 ? "(nothing)" : lines[^1];

    /// <summary>The Dark theme, merged into one host window's own resources. Nothing is added to Application.Current.Resources, which
    /// other test classes read at the same time: a card bound while a theme sat there would take that theme's border brush.</summary>
    private static ResourceDictionary Theme() => new() { Source = new Uri(ThemeUri) };
}
