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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The FinOps Server Inventory, rendered on the real tab over a seeded store, keeps a server it cannot reach: its row is built
/// from the newest <c>server_properties</c> row the collector stored for it, its Last Updated cell reads "last known" with that
/// row's collection time, and the count beside Refresh counts it. A server that cannot be reached and has no collected row stays
/// out, as it always did. The sentence under an empty grid says what is true: that nothing was collected when the store holds no
/// row for any listed server, and that the listed servers could not be reached when that is why the grid is empty.
///
/// <para>Each live read goes to a loopback port nothing listens on, which refuses the connection at once, so the read fails the
/// way an offline server's does and without a timeout. Collection <c>credential-manager</c>: the live read resolves each server's
/// connection string through a <see cref="CredentialResolver"/> over the process-wide credential service, which the other tests
/// that do the same share.</para>
/// </summary>
[Collection("credential-manager")]
public sealed class FinOpsServerInventoryLastKnownTests : IDisposable
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /* The collected row's collection time, the naive UTC instant the store holds. The grid shows it on the machine's clock, as it
       shows the time a live row was read. */
    private static readonly DateTime CollectedAtUtc = new(2026, 11, 7, 9, 30, 0, DateTimeKind.Utc);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteFinOpsLastKnown_" + Guid.NewGuid().ToString("N")[..8]);

    public FinOpsServerInventoryLastKnownTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    /// <summary>A server nothing listens for: a closed loopback port refuses the connection at once. Each server gets its own
    /// port, so the two have different storage ids.</summary>
    private static ServerConnection Unreachable(string displayName, int port) => new()
    {
        Id = Guid.NewGuid().ToString(),
        ServerName = "127.0.0.1," + port,
        DisplayName = displayName,
        MonthlyCostUsd = 250m,
    };

    private static int IdOf(ServerConnection server) =>
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

    [Fact]
    public async Task AServerThatCannotBeReached_IsListedFromItsCollectedRow_MarkedLastKnown()
    {
        var collected = Unreachable("inv-offline-collected", 1);
        var neverCollected = Unreachable("inv-offline-never-collected", 2);
        var expectedTime = CollectedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm");

        using var initializer = await StoreAsync(collected);
        var problems = OnTheInventory(new LocalDataService(initializer), new[] { collected, neverCollected }, (grid, message, count) =>
        {
            var found = new List<string>();
            if (grid.Items.Count != 1)
            {
                found.Add($"the inventory lists {grid.Items.Count} servers; the one with a collected row should be listed and the one without left out");
                return found;
            }

            if (grid.Items[0] is not ServerPropertyRow row)
            {
                found.Add("the listed item is not an inventory row");
                return found;
            }

            if (row.ServerName != collected.DisplayName)
            {
                found.Add($"the row is for {row.ServerName}, not {collected.DisplayName}");
            }
            if (row.Edition != "Enterprise Edition: Core-based Licensing" || row.CpuCount != 16 || row.PhysicalMemoryMb != 65536 || row.SocketCount != 2 || row.CoresPerSocket != 8)
            {
                found.Add($"the row reads {row.Edition} / {row.CpuCount} CPUs / {row.PhysicalMemoryMb} MB / {row.SocketCount} sockets / {row.CoresPerSocket} cores per socket, not the collected row");
            }
            if (row.LastUpdated != CollectedAtUtc.ToLocalTime())
            {
                found.Add($"Last Updated is {row.LastUpdated:O}, not the collected row's collection time");
            }
            if (row.MonthlyCost != 250m)
            {
                found.Add($"the monthly cost is {row.MonthlyCost}, not the server's 250");
            }

            var cells = CellTexts(grid, row);
            if (!cells.Contains("last known, " + expectedTime))
            {
                found.Add($"no cell of the row reads \"last known, {expectedTime}\"; the cells read: {string.Join(" | ", cells)}");
            }
            if (message.Visibility == Visibility.Visible)
            {
                found.Add($"the empty sentence \"{message.Text}\" is shown over a listed server");
            }
            if (count.Text != "1 server")
            {
                found.Add($"the count reads \"{count.Text}\", not \"1 server\"");
            }
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public async Task WhenNoListedServerCanBeReachedAndNoneWasCollected_TheEmptySentenceSaysSo()
    {
        var first = Unreachable("inv-offline-first", 1);
        var second = Unreachable("inv-offline-second", 2);

        using var initializer = await StoreAsync();
        var problems = OnTheInventory(new LocalDataService(initializer), new[] { first, second }, (grid, message, count) =>
        {
            var found = new List<string>();
            if (grid.Items.Count != 0)
            {
                found.Add($"the inventory lists {grid.Items.Count} servers with nothing collected and nothing reachable");
            }
            if (message.Visibility != Visibility.Visible)
            {
                found.Add("the empty sentence is not shown over an empty grid");
            }
            if (message.Text != "2 servers could not be reached and have no collected properties yet")
            {
                found.Add($"the empty sentence reads \"{message.Text}\"");
            }
            if (count.Text != "")
            {
                found.Add($"the count reads \"{count.Text}\" over an empty grid");
            }
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public async Task WithNoServerListed_TheEmptySentenceSaysNothingWasCollected()
    {
        using var initializer = await StoreAsync();
        var problems = OnTheInventory(new LocalDataService(initializer), Array.Empty<ServerConnection>(), (grid, message, count) =>
        {
            var found = new List<string>();
            if (grid.Items.Count != 0)
            {
                found.Add($"the inventory lists {grid.Items.Count} servers with none listed");
            }
            if (message.Visibility != Visibility.Visible)
            {
                found.Add("the empty sentence is not shown over an empty grid");
            }
            if (message.Text != "No server property data collected yet")
            {
                found.Add($"the empty sentence reads \"{message.Text}\"");
            }
            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A fresh store from the product's template, with one collected <c>server_properties</c> row per server in <paramref name="withRows"/>.</summary>
    private async Task<DuckDbInitializer> StoreAsync(params ServerConnection[] withRows)
    {
        var dbPath = Path.Combine(_tempDir, "inventory.duckdb");
        var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeFromTemplateAsync();

        if (withRows.Length == 0) return initializer;

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync();
        long nextId = -1;
        foreach (var server in withRows)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name,
                                  edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket)
                                VALUES ($1, $2, $3, $4, 'Enterprise Edition: Core-based Licensing', '16.0.4150.1', 'RTM', 3, 16, 2, 65536, 2, 8)";
            cmd.Parameters.Add(new DuckDBParameter { Value = nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = CollectedAtUtc });
            cmd.Parameters.Add(new DuckDBParameter { Value = IdOf(server) });
            cmd.Parameters.Add(new DuckDBParameter { Value = server.ServerName });
            await cmd.ExecuteNonQueryAsync();
        }
        return initializer;
    }

    /// <summary>
    /// Opens the FinOps tab over <paramref name="dataService"/> and <paramref name="servers"/> in a Dark-themed window on the STA thread,
    /// runs the Server Inventory load once, and hands the grid, its empty sentence and its count to <paramref name="check"/>.
    /// </summary>
    private List<string> OnTheInventory(LocalDataService dataService, IReadOnlyList<ServerConnection> servers, Func<DataGrid, TextBlock, TextBlock, List<string>> check)
    {
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        var serverManager = new ServerManager(configDir);
        foreach (var server in servers)
        {
            serverManager.AddServer(server);
        }

        var problems = new List<string>();
        StaTestThread.Run(async () =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var saved = app.Resources.MergedDictionaries.ToList();
            app.Resources.MergedDictionaries.Clear();
            app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") });
            app.Resources.MergedDictionaries.Add(new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitor.Ui;component/Themes/SharedStyles.xaml") });
            try
            {
                var tab = new FinOpsTab();
                /* The tab's services, without the server picker's own loads: this test drives one Server Inventory load. The live read
                   resolves each connection string the way the app does, through a resolver with no confirmed-host list. */
                typeof(FinOpsTab).GetField("_dataService", Instance)!.SetValue(tab, dataService);
                typeof(FinOpsTab).GetField("_serverManager", Instance)!.SetValue(tab, serverManager);
                typeof(FinOpsTab).GetField("_credentialResolver", Instance)!.SetValue(tab, new CredentialResolver(new CredentialService(), null));

                var window = new Window { Content = tab, Width = 1270, Height = 925, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var grid = Find<DataGrid>(tab, "ServerInventoryDataGrid");
                    Reveal(grid);
                    Settle(window);

                    await (Task)typeof(FinOpsTab).GetMethod("LoadServerInventoryAsync", Instance)!.Invoke(tab, new object[] { true })!;
                    Settle(window);

                    problems.AddRange(check(grid, Find<TextBlock>(tab, "NoServerInventoryMessage"), Find<TextBlock>(tab, "ServerInventoryCountIndicator")));
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                app.Resources.MergedDictionaries.Clear();
                foreach (var dictionary in saved)
                {
                    app.Resources.MergedDictionaries.Add(dictionary);
                }
            }
        });
        return problems;
    }

    /// <summary>The text of every realized cell of <paramref name="item"/>'s row, as the grid draws it.</summary>
    private static List<string> CellTexts(DataGrid grid, object item)
    {
        grid.ScrollIntoView(item);
        grid.UpdateLayout();
        if (grid.ItemContainerGenerator.ContainerFromItem(item) is not DataGridRow row)
        {
            return new List<string> { "(the row is not realized)" };
        }
        return ClippedTextProbe.Descendants(row).OfType<TextBlock>().Select(t => t.Text).ToList();
    }

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

    private static T Find<T>(FrameworkElement tab, string name) where T : FrameworkElement =>
        tab.FindName(name) as T ?? throw new Xunit.Sdk.XunitException($"{name} is not on the tab");

    /// <summary>Selects every tab on the way up to <paramref name="element"/>, so it is laid out and drawn.</summary>
    private static void Reveal(FrameworkElement element)
    {
        for (DependencyObject? at = element; at is not null; at = LogicalTreeHelper.GetParent(at))
        {
            if (at is TabItem item && ItemsControl.ItemsControlFromItemContainer(item) is TabControl owner)
            {
                owner.SelectedItem = item;
            }
        }
    }
}
