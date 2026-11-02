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
using System.Threading.Tasks;
using System.Windows;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using PerformanceMonitorLite.Tests.Helpers;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #7160: the Alerts History tab dismisses the archived rows it shows. The window write has always put them in the
/// dismissed_archive_alerts sidecar; the buttons that send the keyed write used to filter them out first and tell the operator
/// they "will remain visible" or "cannot be dismissed". Each button now dismisses every row it counts.
/// </summary>
public sealed class AlertHistoryDismissArchivedTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;

    private DuckDbInitializer? _initializer;

    public AlertHistoryDismissArchivedTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        _initializer?.Dispose();

        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>Two live CPU alerts on server 1, one live Blocking alert on server 1 and one archived CPU alert on server 2.</summary>
    private async Task<LocalDataService> SeedAsync()
    {
        _initializer = new DuckDbInitializer(_dbPath);
        await _initializer.InitializeFromTemplateAsync();
        var helper = new TestAlertDataHelper(_dbPath);
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddHours(-1), 1, "example-sql-01", "CPU");
            await helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddHours(-2), 1, "example-sql-01", "CPU");
            await helper.InsertLiveAlertAsync(connection, DateTime.UtcNow.AddHours(-3), 1, "example-sql-01", "Blocking");
            await helper.CreateArchivedAlertsParquetAsync(connection, new List<TestAlertRecord>
            {
                TestAlertDataHelper.CreateAlert(alertTime: DateTime.UtcNow.AddDays(-14), serverId: 2, serverName: "example-sql-02", metricName: "CPU")
            });
        }

        await helper.RefreshArchiveViewsAsync();
        return new LocalDataService(_initializer);
    }

    private static async Task<List<AlertHistoryRow>> ReadAsync(LocalDataService service)
        => await service.GetAlertHistoryWindowAsync(DateTime.UtcNow.AddDays(-30), null, null, 500);

    private async Task<long> SidecarCountAsync()
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(1) FROM dismissed_archive_alerts";
        return Convert.ToInt64(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task DismissAll_OverAFilteredGrid_DismissesTheArchivedRowAndAsksAboutEveryRowItDismisses()
    {
        var service = await SeedAsync();
        var rows = await ReadAsync(service);
        Assert.Equal(4, rows.Count);
        Assert.Single(rows, r => r.IsArchived);

        var prompts = new List<string>();
        var dismissedServers = new List<int?>();
        StaTestThread.Run(async () =>
        {
            var tab = new AlertsHistoryTab();
            tab.Initialize(service, _ => null);
            tab.AlertsDismissed += id => dismissedServers.Add(id);
            tab.ConfirmPrompt = (message, _, _, _) => { prompts.Add(message); return MessageBoxResult.Yes; };
            tab.ShowAlerts(rows);
            /* The CPU filter leaves three rows on screen, one of them archived, so Dismiss All sends the keyed write. */
            tab.ApplyColumnFilter(new ColumnFilterState { ColumnName = "MetricName", Operator = FilterOperator.Equals, Value = "CPU" });
            Assert.True(tab.DismissAllUsesShownRows());

            await tab.DismissAllAsync();
        });

        var prompt = Assert.Single(prompts);
        Assert.StartsWith("Dismiss 3 alert(s)?", prompt);
        Assert.DoesNotContain("remain visible", prompt);
        Assert.Equal(1, await SidecarCountAsync());
        var left = await ReadAsync(service);
        Assert.Equal("Blocking", Assert.Single(left).MetricName);
        Assert.Equal(new int?[] { 1, 2 }, dismissedServers.OrderBy(i => i).ToArray());
    }

    [Fact]
    public async Task DismissSelected_DismissesTheArchivedRowWithoutAPrompt()
    {
        var service = await SeedAsync();
        var rows = await ReadAsync(service);
        var archived = Assert.Single(rows, r => r.IsArchived);
        var live = rows.First(r => !r.IsArchived && r.MetricName == "CPU");

        var prompts = new List<string>();
        StaTestThread.Run(async () =>
        {
            var tab = new AlertsHistoryTab();
            tab.Initialize(service, _ => null);
            tab.ConfirmPrompt = (message, _, _, _) => { prompts.Add(message); return MessageBoxResult.Yes; };
            tab.ShowAlerts(rows);
            tab.AlertsDataGrid.SelectedItems.Add(archived);
            tab.AlertsDataGrid.SelectedItems.Add(live);

            await tab.DismissSelectedAsync();
        });

        /* The archived row is no longer set aside with a "cannot be dismissed" box: nothing is asked, and both rows go. */
        Assert.Empty(prompts);
        Assert.Equal(1, await SidecarCountAsync());
        var left = await ReadAsync(service);
        Assert.Equal(2, left.Count);
        Assert.DoesNotContain(left, r => r.IsArchived);
    }
}
