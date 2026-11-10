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
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8531: the Database box on FinOps > Index Analysis, filled by the real tab from a seeded store. It lists the databases of the
/// newest <c>database_states</c> snapshot when the store holds a recent one, and otherwise the databases of the newest
/// <c>database_size_stats</c> snapshot, so a store with no recent state snapshot still offers the databases it knows. A name
/// already typed in the box stays.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class FinOpsIndexAnalysisDatabaseBoxTests : IDisposable
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;
    private const int ServerId = 8531;

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteFinOpsDbBox_" + Guid.NewGuid().ToString("N")[..8]);
    private DuckDbInitializer? _store;
    private long _nextId = -1;

    public FinOpsIndexAnalysisDatabaseBoxTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        _store?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    [Fact]
    public async Task WithNoRecentStateSnapshot_TheBoxListsTheNewestSizeSnapshotsDatabases()
    {
        var initializer = await StoreAsync();
        /* An older size snapshot holds a database that is gone from the newest one, and a state snapshot too old to be current. */
        await SizeSnapshotAsync(TimeSpan.FromDays(2), "HrDb", "RetiredDb", "SalesDb");
        await SizeSnapshotAsync(TimeSpan.FromHours(30), "HrDb", "SalesDb", "WarehouseDb");
        await StateSnapshotAsync(TimeSpan.FromDays(3), "HrDb", "RetiredDb");

        var listed = OnTheBox(new LocalDataService(initializer), typed: "SalesDb");

        Assert.Equal(new[] { "HrDb", "SalesDb", "WarehouseDb" }, listed.Items);
        Assert.Equal("SalesDb", listed.Text);

        /* The list is a day old and may name a database dropped since, so the box says when it is from. */
        Assert.NotNull(listed.Note);
        Assert.StartsWith("Databases as of ", listed.Note, StringComparison.Ordinal);
        Assert.Contains("no recent database list", listed.Note, StringComparison.Ordinal);
    }

    /// <summary>A size snapshot older than a week says too little about what the server holds now: the box lists nothing from it
    /// and still takes a typed name.</summary>
    [Fact]
    public async Task WithOnlyASizeSnapshotOlderThanAWeek_TheBoxListsNothing_AndKeepsTheTypedName()
    {
        var initializer = await StoreAsync();
        await SizeSnapshotAsync(TimeSpan.FromDays(21), "HrDb", "RetiredDb", "SalesDb");

        var listed = OnTheBox(new LocalDataService(initializer), typed: "SalesDb");

        Assert.Empty(listed.Items);
        Assert.Equal("SalesDb", listed.Text);
        Assert.Null(listed.Note);
    }

    [Fact]
    public async Task WithARecentStateSnapshot_TheBoxListsItsDatabases()
    {
        var initializer = await StoreAsync();
        await SizeSnapshotAsync(TimeSpan.FromHours(30), "HrDb", "RetiredDb", "SalesDb");
        await StateSnapshotAsync(TimeSpan.FromMinutes(5), "HrDb", "SalesDb");

        var listed = OnTheBox(new LocalDataService(initializer), typed: "");

        Assert.Equal(new[] { "HrDb", "SalesDb" }, listed.Items);
        Assert.Equal("", listed.Text);
        /* A current list needs no date. */
        Assert.Null(listed.Note);
    }

    private async Task<DuckDbInitializer> StoreAsync()
    {
        _store = new DuckDbInitializer(Path.Combine(_tempDir, "box.duckdb"));
        await _store.InitializeFromTemplateAsync();
        return _store;
    }

    private async Task SizeSnapshotAsync(TimeSpan age, params string[] databases)
    {
        using var conn = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "box.duckdb")}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var at = DateTime.UtcNow - age;
        foreach (var database in databases)
        {
            /* A data file and a log file: a database is listed once however many files it has. */
            foreach (var (fileId, type, suffix) in new[] { (1, "ROWS", ".mdf"), (2, "LOG", ".ldf") })
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = @"INSERT INTO database_size_stats
                                      (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
                                       file_name, physical_name, total_size_mb, used_size_mb)
                                    VALUES ($1, $2, $3, 'BoxSrv', $4, 7, $5, $6, $7, $7, 100, 40)";
                cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
                cmd.Parameters.Add(new DuckDBParameter { Value = at });
                cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = database });
                cmd.Parameters.Add(new DuckDBParameter { Value = fileId });
                cmd.Parameters.Add(new DuckDBParameter { Value = type });
                cmd.Parameters.Add(new DuckDBParameter { Value = database + suffix });
                await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }
    }

    private async Task StateSnapshotAsync(TimeSpan age, params string[] databases)
    {
        using var conn = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "box.duckdb")}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var at = DateTime.UtcNow - age;
        foreach (var database in databases)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby)
                                VALUES ($1, $2, $3, 'BoxSrv', $4, 5, 'ONLINE', false)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = at });
            cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = database });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Opens the FinOps tab over <paramref name="dataService"/> on the STA thread, types <paramref name="typed"/> in the Index Analysis
    /// Database box, runs the tab's own fill of that box once, and returns what the box then lists (in ordinal order) and holds.
    /// </summary>
    private static (string[] Items, string Text, string? Note) OnTheBox(LocalDataService dataService, string typed)
    {
        (string[] Items, string Text, string? Note) result = (Array.Empty<string>(), "", null);
        StaTestThread.Run(async () =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary>();
            if (!app.Resources.Contains("NumericCell"))
            {
                added.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") });
                added.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }
            }

            try
            {
                var tab = new FinOpsTab();
                /* The tab's data service, without the server picker's own loads: this test drives the one fill of the box. */
                typeof(FinOpsTab).GetField("_dataService", Instance)!.SetValue(tab, dataService);

                var window = new Window { Content = tab, Width = 900, Height = 600, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var box = tab.FindName("IndexAnalysisDatabaseInput") as ComboBox ?? throw new Xunit.Sdk.XunitException("IndexAnalysisDatabaseInput is not on the tab");
                    for (DependencyObject? at = box; at is not null; at = LogicalTreeHelper.GetParent(at))
                    {
                        if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
                        {
                            owner.SelectedItem = item;
                        }
                    }

                    window.UpdateLayout();
                    box.Text = typed;

                    await (Task)typeof(FinOpsTab).GetMethod("PopulateIndexAnalysisDatabasesAsync", Instance)!.Invoke(tab, new object[] { ServerId })!;
                    window.UpdateLayout();

                    result = (box.Items.OfType<string>().OrderBy(name => name, StringComparer.Ordinal).ToArray(), box.Text, box.ToolTip as string);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }
            }
        });
        return result;
    }
}
