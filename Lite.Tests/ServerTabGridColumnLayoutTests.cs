/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using Darling.Tests;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8099: the server tab's grids keep the figures a reader opens them for inside a 1,536 px viewport (a maximized window
/// on a 3840 px display at 250 % scaling). Top Queries stands for the query grids, which share one Query Text cell
/// shape: a 500 px column of wrapped SQL, 90 px tall, pushed Executions and Total CPU past the right edge. Query Store
/// Health reserved 190 px for a Read-Only Reason that is blank for every READ_WRITE store and 200 px for a one-word
/// State. The real <see cref="ServerTab"/> is built with the theme loaded, the grid is shown at that width, rows are
/// bound, and the realized cells are measured (<see cref="GridColumnLayoutProbe"/>). The Viewer's twin is
/// <c>ViewerServerTabGridColumnLayoutTests</c>.
/// </summary>
[Collection("credential-manager")]
public sealed class ServerTabGridColumnLayoutTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    [Fact]
    public void TopQueries_ShowsQueryTextAsOneTrimmedLine_WithExecutionsAndTotalCpuInsideTheViewport()
    {
        var rows = new List<QueryStatsRow>
        {
            new() { DatabaseName = "tenant_a", QueryText = GridColumnLayoutProbe.LongQueryText, TotalExecutions = 1200 },
            new() { DatabaseName = "tenant_b", QueryText = GridColumnLayoutProbe.LongQueryText, TotalExecutions = 80 },
            new() { DatabaseName = "tenant_c", QueryText = GridColumnLayoutProbe.LongQueryText, TotalExecutions = 7 },
        };

        var found = WithServerTab(tab => GridColumnLayoutProbe.Shown(
            GridColumnLayoutProbe.Detach(tab, "QueryStatsGrid"), rows,
            grid => GridColumnLayoutProbe.MeasureQueryText(grid, rows, GridColumnLayoutProbe.LongQueryText, "Executions", "Total CPU (ms)", "Total Duration (ms)")));

        Assert.True(found.Count == 0, $"Top Queries at {GridColumnLayoutProbe.Viewport} px:{Environment.NewLine}" + string.Join(Environment.NewLine, found));
    }

    [Fact]
    public void QueryStoreHealth_KeepsTheSettingsInsideTheViewport_AndGivesTheBlankColumnsOnlyTheirHeader()
    {
        var rows = new List<QueryStoreHealthRow>();
        foreach (var database in new[] { "tenant_a", "tenant_b", "tenant_c" })
        {
            rows.Add(new QueryStoreHealthRow
            {
                CaptureTime = DateTime.UtcNow,
                DatabaseName = database,
                ActualState = "READ_WRITE",
                DesiredState = "READ_WRITE",
                ReadonlyReason = 0,
                CurrentStorageMb = 512,
                MaxStorageMb = 1024,
                SizeBasedCleanupMode = "AUTO",
                StaleQueryThresholdDays = 30,
                MaxPlansPerQuery = 200,
                IntervalLengthMinutes = 60,
                QueryCaptureMode = "AUTO",
                WaitStatsCaptureMode = "ON",
            });
        }

        var found = WithServerTab(tab => GridColumnLayoutProbe.Shown(
            GridColumnLayoutProbe.Detach(tab, "QueryStoreHealthGrid"), rows,
            grid => GridColumnLayoutProbe.MeasureSettings(grid, rows, "Read-Only Reason", "State", 120, "Stale Days", "Max Plans/Query", "Interval Min")));

        Assert.True(found.Count == 0, $"Query Store Health at {GridColumnLayoutProbe.Viewport} px:{Environment.NewLine}" + string.Join(Environment.NewLine, found));
    }

    /// <summary>
    /// #8101: the Collection Health grid's newest error and its time are the two columns after Status, and both end inside the
    /// viewport, so a HEALTHY row with Errors 1 says what the error was and when without a sideways scroll. The Viewer's twin is
    /// in <c>ViewerFillColumnTests</c>.
    /// </summary>
    [Fact]
    public void CollectionHealth_ShowsLastErrorAtAndLastError_RightAfterStatus_InsideTheViewport()
    {
        var rows = new List<CollectorHealthRow>
        {
            new()
            {
                CollectorName = "wait_stats", TotalRuns = 10080, SuccessCount = 10079, ErrorCount = 1,
                LastRunTime = DateTime.UtcNow, LastSuccessTime = DateTime.UtcNow, LastErrorTime = DateTime.UtcNow.AddMinutes(-30),
                LastError = "A connection was successfully established with the server, but then an error occurred during the pre-login handshake.",
            },
        };

        var found = WithServerTab(tab => GridColumnLayoutProbe.Shown(
            GridColumnLayoutProbe.Detach(tab, "CollectionHealthGrid"), rows,
            grid =>
            {
                var failures = new List<string>();
                var ordered = grid.Columns.OrderBy(c => c.DisplayIndex).ToList();
                var keys = ordered.Select(ColumnKey).ToList();
                var status = keys.IndexOf("DisplayStatus");
                if (status < 0 || status + 2 >= keys.Count || keys[status + 1] != "LastErrorTime" || keys[status + 2] != "LastError")
                {
                    failures.Add("the columns after Status are not Last Error At and Last Error: " + string.Join(", ", keys));
                    return failures;
                }

                var rightEdge = ordered.Take(status + 3).Sum(c => c.ActualWidth);
                var viewport = ((ScrollViewer)grid.Template.FindName("DG_ScrollViewer", grid)).ViewportWidth;
                if (rightEdge > viewport)
                {
                    failures.Add($"Last Error ends at {rightEdge} in a viewport of {viewport}");
                }

                return failures;
            }));

        Assert.True(found.Count == 0, $"Collection Health at {GridColumnLayoutProbe.Viewport} px:{Environment.NewLine}" + string.Join(Environment.NewLine, found));
    }

    /// <summary>What a column shows: its sort member, else the path its cells bind to, else its header.</summary>
    private static string ColumnKey(DataGridColumn column) =>
        !string.IsNullOrEmpty(column.SortMemberPath) ? column.SortMemberPath
        : column is DataGridBoundColumn { Binding: System.Windows.Data.Binding binding } ? binding.Path.Path
        : column.Header?.ToString() ?? "";

    /// <summary>Builds the real server tab against an empty local store, on the STA thread with the theme loaded, and hands it to <paramref name="body"/>.</summary>
    private static List<string> WithServerTab(Func<ServerTab, List<string>> body)
    {
        var dir = Path.Combine(Path.GetTempPath(), "ServerTabGridColumnLayout_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            return StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "grid-layout-test", DisplayName = "grid-layout-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                /* Switching to a tab would start that tab's load against a server that is not there. */
                typeof(ServerTab).GetField("_suppressActiveQueriesAutoRefresh", Instance)!.SetValue(tab, true);
                return body(tab);
            }));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

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
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    }
}
