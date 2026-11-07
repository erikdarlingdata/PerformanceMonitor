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
using System.Reflection;
using System.Windows;
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
            grid => GridColumnLayoutProbe.MeasureQueryText(grid, rows, GridColumnLayoutProbe.LongQueryText, "Executions", "Total CPU (ms)")));

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
