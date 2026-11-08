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
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite's live screens, as a reader meets them: the plan viewer's empty state names the menu items that exist, the sidebar rail's
/// tooltip names the server and asks for a gesture the rail supports, Alert History's Dismiss All is off over an empty grid, the
/// Add SQL Server form keeps a form's width when the window is wide, and the plan's Server Context card gets the server's name
/// from the store. Built from the real controls on the shared STA thread, in hosts that fit a 1024x768 screen.
/// </summary>
[Collection("credential-manager")]
public sealed class LiveScreenTextAndLayoutTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void ThePlanViewerEmptyState_NamesTheMenuItemsThatExist()
    {
        var dir = ScratchDir("PlanEmptyState");
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var text = StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "plan-empty-test", DisplayName = "plan-empty-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                return LogicalDescendants(tab).OfType<TextBlock>()
                    .Select(t => t.Text)
                    .Single(t => t.StartsWith("Right-click a query row", StringComparison.Ordinal));
            }));

            Assert.Contains("'View Stored Plan'", text, StringComparison.Ordinal);
            Assert.Contains("'Get Actual Plan (re-run)'", text, StringComparison.Ordinal);
            Assert.DoesNotContain("'View Plan'", text, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public void TheRailTooltip_NamesTheServer_AndAsksForAGestureTheRailSupports()
    {
        var connection = new ServerConnection { Id = "s1", ServerName = "sql-a.example", DisplayName = "Orders", IsEnabled = true, IsOnline = true };

        var lines = connection.DotTooltip.Split('\n');

        Assert.Equal("Orders", lines[0]);
        Assert.StartsWith("Online — the last connection check succeeded", lines[1], StringComparison.Ordinal);
        Assert.Equal("Double-click to open this server's tab", lines[^1]);
        Assert.DoesNotContain(lines, line => line.Contains("the row", StringComparison.Ordinal));
    }

    [Fact]
    public void DismissAll_IsOffOverAnEmptyGrid_AndOnWhenRowsShow()
    {
        var (empty, withRows) = StaTestThread.Run(() => WithTheme(() =>
        {
            var tab = new AlertsHistoryTab();
            var button = (Button)tab.FindName("DismissAllButton");
            tab.ShowAlerts(new List<AlertHistoryRow>());
            var whenEmpty = button.IsEnabled;
            tab.ShowAlerts(new List<AlertHistoryRow> { new() { AlertTime = DateTime.UtcNow, ServerId = 1, ServerName = "sql-a", MetricName = "High CPU", CurrentValue = 95, ThresholdValue = 80 } });
            return (whenEmpty, button.IsEnabled);
        }));

        Assert.False(empty);
        Assert.True(withRows);
    }

    [Fact]
    public void TheAddServerForm_KeepsAFormsWidth_WhenTheWindowIsWide()
    {
        var dir = ScratchDir("AddServerWide");
        try
        {
            var width = StaTestThread.Run(() => WithTheme(() =>
            {
                var servers = new ServerManager(dir);
                var profiles = new ProfileManager(servers);
                var dialog = new AddServerDialog(servers, profiles);
                dialog.SizeToContent = SizeToContent.Manual;
                dialog.Width = 1000;
                dialog.Height = 700;
                StaTestThread.ShowOffScreen(dialog);
                try
                {
                    dialog.UpdateLayout();
                    return ((TextBox)dialog.FindName("ServerNameBox")).ActualWidth;
                }
                finally
                {
                    dialog.Close();
                }
            }));

            Assert.True(width > 300 && width <= 560, $"the Server Name box is {width:F0} px wide in a 1000 px window; a form box stays at a form's width");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task ThePlanServerContext_ReadsTheServersName()
    {
        var dir = ScratchDir("PlanServerName");
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            await duckDb.InitializeAsync();
            using (var connection = duckDb.CreateConnection())
            {
                await connection.OpenAsync();
                using var command = connection.CreateCommand();
                /* One row with every payload column the collector writes, since the table declares them NOT NULL. */
                command.CommandText = "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, product_update_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered, enterprise_features, service_objective, vcore_count, lock_pages_in_memory, instant_file_initialization_enabled, memory_dump_count) "
                    + "VALUES (1, current_timestamp, 7, 'SQL01', 'Enterprise Edition (64-bit)', '16.0.1000.6', 'RTM', 'CU5', 3, 8, 1, 65536, 1, 8, false, false, '', '', 0, false, false, 0)";
                await command.ExecuteNonQueryAsync();
            }

            var metadata = await new LocalDataService(duckDb).GetServerMetadataForPlanAnalysisAsync(7);

            Assert.NotNull(metadata);
            Assert.Equal("SQL01", metadata!.ServerName);
            Assert.Equal("Enterprise Edition (64-bit)", metadata.Edition);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    private static string ScratchDir(string stem)
    {
        var dir = Path.Combine(Path.GetTempPath(), stem + "_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static IEnumerable<DependencyObject> LogicalDescendants(DependencyObject root)
    {
        foreach (var child in LogicalTreeHelper.GetChildren(root).OfType<DependencyObject>())
        {
            yield return child;
            foreach (var grandchild in LogicalDescendants(child))
                yield return grandchild;
        }
    }

    /// <summary>The controls' markup names the theme's static resources, so the theme is merged for the build and removed after it.</summary>
    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null) app.Resources.MergedDictionaries.Remove(added);
        }
    }
}
