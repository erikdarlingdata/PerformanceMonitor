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
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8531: the FinOps Server Inventory, rendered on the real tab over a seeded store. The Status and Health cells of a server
/// with CPU and memory samples read its verdict and its score, and those of a server with no CPU sample in the last 24 hours
/// read "Not collected", never an empty cell or a bare dash. The Last Updated cell reads the selected time display: each
/// server's own collected clock in Server Time, UTC in UTC, and this machine's clock in Local Time. The tab's first show after
/// start-up reads the inventory again, so cells the start-up load filled before any collection do not stay as they were.
///
/// <para>Each live read goes to a loopback port nothing listens on, which refuses the connection at once, so every row is built
/// from the store. Collection <c>credential-manager</c>: the live read resolves each connection string through the process-wide
/// credential service, the collection runs alone, and the time display statics this class sets are put back in Dispose.</para>
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class FinOpsServerInventoryCellsTests : IDisposable
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /* The collected rows' collection time, the naive UTC instant the store holds. */
    private static readonly DateTime CollectedAtUtc = new(2026, 11, 7, 9, 30, 0, DateTimeKind.Utc);

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteFinOpsCells_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private DuckDbInitializer? _store;
    private long _nextId = -1;

    public FinOpsServerInventoryCellsTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        _store?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static ServerConnection Unreachable(string displayName, int port) => new()
    {
        Id = Guid.NewGuid().ToString(),
        ServerName = "127.0.0.1," + port,
        DisplayName = displayName,
    };

    private static int IdOf(ServerConnection server) =>
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

    [Fact]
    public async Task StatusAndHealth_ReadTheCollectedValues_OrNotCollected_NeverBlank()
    {
        var sampled = Unreachable("inv-cells-sampled", 1);
        var unsampled = Unreachable("inv-cells-unsampled", 2);

        var initializer = await StoreAsync((sampled, null), (unsampled, null));
        await SeedCpuAndMemoryAsync(sampled);

        var problems = OnTheInventory(new LocalDataService(initializer), new[] { sampled, unsampled }, grid =>
        {
            var found = new List<string>();
            var rows = grid.Items.OfType<ServerPropertyRow>().ToDictionary(r => r.ServerName);
            if (!rows.TryGetValue(sampled.DisplayName, out var sampledRow) || !rows.TryGetValue(unsampled.DisplayName, out var unsampledRow))
            {
                found.Add($"the inventory lists {string.Join(", ", rows.Keys)}, not both servers");
                return found;
            }

            /* The server with samples: the overlay is keyed to it, so its cells carry the verdict and the score. */
            var status = CellText(grid, sampledRow, "Status");
            if (string.IsNullOrEmpty(sampledRow.ProvisioningStatus) || status != ProvisioningVerdict.StatusLabel(sampledRow.ProvisioningStatus))
            {
                found.Add($"the sampled server's Status cell reads \"{status}\" for the verdict \"{sampledRow.ProvisioningStatus}\"");
            }

            var health = CellText(grid, sampledRow, "Health");
            if (sampledRow.HealthScore is not int score || health != score.ToString(System.Globalization.CultureInfo.CurrentCulture))
            {
                found.Add($"the sampled server's Health cell reads \"{health}\" for the score {sampledRow.HealthScore?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "(none)"}");
            }

            /* The server with no CPU sample in the last 24 hours has no verdict and no score, and its cells say so. */
            foreach (var header in new[] { "Status", "Health" })
            {
                var text = CellText(grid, unsampledRow, header);
                if (text != "Not collected")
                {
                    found.Add($"the unsampled server's {header} cell reads \"{text}\", not \"Not collected\"");
                }
            }

            /* The Status cell says what was not collected, as the Health cell's tooltip does; a cell with a verdict has none. */
            var statusColumn = grid.Columns.First(c => HeaderText(c) == "Status");
            var unsampledTip = (statusColumn.GetCellContent(unsampledRow) as TextBlock)?.ToolTip as string;
            if (unsampledTip != "No CPU sample in the last 24 hours")
            {
                found.Add($"the unsampled server's Status cell has the tooltip \"{unsampledTip}\", not \"No CPU sample in the last 24 hours\"");
            }

            if ((statusColumn.GetCellContent(sampledRow) as TextBlock)?.ToolTip is string sampledTip)
            {
                found.Add($"the sampled server's Status cell has the tooltip \"{sampledTip}\" beside a verdict");
            }

            /* Idle DBs keeps its dash for a server with no query-stats coverage. */
            var idle = CellText(grid, unsampledRow, "Idle DBs");
            if (idle != "-")
            {
                found.Add($"the unsampled server's Idle DBs cell reads \"{idle}\", not \"-\"");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public async Task LastUpdated_ReadsTheSelectedTimeDisplay_OnEachServersOwnClock()
    {
        var kathmandu = Unreachable("inv-cells-plus-0545", 1);
        var pacific = Unreachable("inv-cells-minus-0800", 2);

        var initializer = await StoreAsync((kathmandu, 345), (pacific, -480));

        /* A third clock on the active tab: a row reads its own server's clock, never the tab that happens to be open. */
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(60);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        var problems = OnTheInventory(new LocalDataService(initializer), new[] { kathmandu, pacific }, grid =>
        {
            var found = new List<string>();
            var rows = grid.Items.OfType<ServerPropertyRow>().ToDictionary(r => r.ServerName);
            if (!rows.TryGetValue(kathmandu.DisplayName, out var kathmanduRow) || !rows.TryGetValue(pacific.DisplayName, out var pacificRow))
            {
                found.Add($"the inventory lists {string.Join(", ", rows.Keys)}, not both servers");
                return found;
            }

            void Expect(string mode, ServerPropertyRow row, string expected)
            {
                var text = CellText(grid, row, "Last Updated");
                if (text != expected)
                {
                    found.Add($"{mode}: {row.ServerName}'s Last Updated cell reads \"{text}\", not \"{expected}\"");
                }
            }

            /* In Server Time each row names the offset of the clock it is worded on. */
            Expect("Server Time", kathmanduRow, "last known, 2026-11-07 15:15 (UTC+05:45)");
            Expect("Server Time", pacificRow, "last known, 2026-11-07 01:30 (UTC-08:00)");

            /* The setting changes while the grid is on screen: the cells follow it with no reload and no refresh from here. */
            ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            grid.UpdateLayout();
            Expect("UTC", kathmanduRow, "last known, 2026-11-07 09:30");
            Expect("UTC", pacificRow, "last known, 2026-11-07 09:30");

            ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
            grid.UpdateLayout();
            var local = "last known, " + CollectedAtUtc.ToLocalTime().ToString("yyyy-MM-dd HH:mm");
            Expect("Local Time", kathmanduRow, local);
            Expect("Local Time", pacificRow, local);

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    [Fact]
    public async Task TheFirstShowAfterStartUp_ReadsTheInventoryAgain()
    {
        var server = Unreachable("inv-cells-first-show", 1);
        var initializer = await StoreAsync((server, null));

        /* Start-up reads the inventory before the tab has ever been shown and before any collection: the store holds no CPU sample.
           The first collection then lands, and the tab is shown for the first time. */
        var problems = OnTheInventory(new LocalDataService(initializer), new[] { server }, (_, _, grid) =>
        {
            var found = new List<string>();
            if (grid.Items.Count != 1 || grid.Items[0] is not ServerPropertyRow shown)
            {
                found.Add($"after the show the inventory lists {grid.Items.Count} servers, not the one");
                return Task.FromResult(found);
            }

            var status = CellText(grid, shown, "Status");
            if (string.IsNullOrEmpty(shown.ProvisioningStatus) || status != ProvisioningVerdict.StatusLabel(shown.ProvisioningStatus))
            {
                found.Add($"after the show the Status cell reads \"{status}\" for the verdict \"{shown.ProvisioningStatus}\"; the samples the collection brought were not read");
            }

            var health = CellText(grid, shown, "Health");
            if (shown.HealthScore is not int score || health != score.ToString(System.Globalization.CultureInfo.CurrentCulture))
            {
                found.Add($"after the show the Health cell reads \"{health}\"; the samples the collection brought were not read");
            }

            return Task.FromResult(found);
        }, betweenStartUpAndFirstShow: _ => SeedCpuAndMemoryAsync(server));

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A show reads the collected overlay again and lays it on the rows already loaded. It does not build the rows
    /// again, which would open a connection to every monitored server each time the tab is shown.</summary>
    [Fact]
    public async Task AShow_LaysTheOverlayOnTheRowsAlreadyLoaded_AndDoesNotBuildThemAgain()
    {
        var server = Unreachable("inv-cells-overlay-only", 1);
        var initializer = await StoreAsync((server, null));
        ServerPropertyRow? atStartUp = null;

        var problems = OnTheInventory(new LocalDataService(initializer), new[] { server }, (_, _, grid) =>
        {
            var found = new List<string>();
            if (grid.Items.Count != 1 || grid.Items[0] is not ServerPropertyRow shown)
            {
                found.Add($"after the show the inventory lists {grid.Items.Count} servers, not the one");
                return Task.FromResult(found);
            }

            if (!ReferenceEquals(atStartUp, shown))
            {
                found.Add("the show built the inventory's rows again instead of laying the overlay on the ones start-up loaded");
            }

            if (string.IsNullOrEmpty(shown.ProvisioningStatus))
            {
                found.Add("the row has no verdict after the show; the samples the collection brought were not read");
            }

            return Task.FromResult(found);
        }, betweenStartUpAndFirstShow: tab =>
        {
            atStartUp = (typeof(FinOpsTab).GetField("_serverInventoryCache", Instance)!.GetValue(tab) as List<ServerPropertyRow>)?.FirstOrDefault();
            return SeedCpuAndMemoryAsync(server);
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A re-read of the collected overlay, and a change of the time display, change the cells of the rows on screen. The
    /// selected row stays selected and current, the grid keeps the rows it has drawn (rebuilding them drops the keyboard's place
    /// and the scroll position), and the horizontal scroll stays where the user left it.</summary>
    [Fact]
    public async Task AnOverlayReRead_AndATimeDisplayChange_KeepTheSelectedRowAndTheScrollPosition()
    {
        var first = Unreachable("inv-cells-selected-a", 1);
        var second = Unreachable("inv-cells-selected-b", 2);
        var initializer = await StoreAsync((first, 345), (second, -480));
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(60);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;

        var problems = OnTheInventory(new LocalDataService(initializer), new[] { first, second }, async (tab, window, grid) =>
        {
            var found = new List<string>();
            var row = grid.Items.OfType<ServerPropertyRow>().FirstOrDefault(r => r.ServerName == second.DisplayName);
            var status = grid.Columns.FirstOrDefault(c => HeaderText(c) == "Status");
            var lastUpdated = grid.Columns.FirstOrDefault(c => HeaderText(c) == "Last Updated");
            if (row is null || status is null || lastUpdated is null)
            {
                found.Add("the inventory does not list the second server with a Status and a Last Updated column");
                return found;
            }

            grid.SelectedItem = row;
            grid.CurrentCell = new DataGridCellInfo(row, status);
            grid.ScrollIntoView(row, grid.Columns[grid.Columns.Count - 1]);
            Settle(window);

            var drawn = grid.ItemContainerGenerator.ContainerFromItem(row);
            var scroller = Descendants(grid).OfType<ScrollViewer>().FirstOrDefault();
            if (drawn is null || scroller is null)
            {
                found.Add("the selected row is not drawn, or the grid has no scroll viewer");
                return found;
            }

            var offset = scroller.HorizontalOffset;
            if (offset <= 0)
            {
                found.Add("the grid is not scrolled sideways at this width, so the test cannot read a kept scroll position");
            }

            /* A grid that redraws itself prepares its rows again, which is what takes the keyboard's place with it. */
            var rowsPrepared = 0;
            grid.LoadingRow += (_, _) => rowsPrepared++;

            void ExpectKept(string after)
            {
                Settle(window);
                if (rowsPrepared != 0)
                {
                    found.Add($"after {after} the grid prepared {rowsPrepared} of its rows again instead of changing the cells in place");
                    rowsPrepared = 0;
                }

                if (!ReferenceEquals(grid.SelectedItem, row))
                {
                    found.Add($"after {after} the selected server is {(grid.SelectedItem as ServerPropertyRow)?.ServerName ?? "none"}, not {row.ServerName}");
                }

                if (!ReferenceEquals(grid.CurrentCell.Item, row) || !ReferenceEquals(grid.CurrentCell.Column, status))
                {
                    found.Add($"after {after} the current cell is no longer the selected row's Status cell");
                }

                if (!ReferenceEquals(grid.ItemContainerGenerator.ContainerFromItem(row), drawn))
                {
                    found.Add($"after {after} the grid built its rows again; the selected row's place under the keyboard and the scroll position go with them");
                }

                if (Math.Abs(scroller.HorizontalOffset - offset) > 0.5)
                {
                    found.Add($"after {after} the grid is scrolled to {scroller.HorizontalOffset:0.#} px, not the {offset:0.#} px it was left at");
                }
            }

            /* The collection brings the second server's first samples, and the overlay is read again. */
            await SeedCpuAndMemoryAsync(second);
            await (Task)typeof(FinOpsTab).GetMethod("ReReadServerInventoryOverlayAsync", Instance)!.Invoke(tab, null)!;
            ExpectKept("an overlay re-read");

            var statusText = status.GetCellContent(row) is TextBlock statusCell ? statusCell.Text : "(the cell is not realized)";
            if (string.IsNullOrEmpty(row.ProvisioningStatus) || statusText != ProvisioningVerdict.StatusLabel(row.ProvisioningStatus))
            {
                found.Add($"after an overlay re-read the Status cell reads \"{statusText}\" for the verdict \"{row.ProvisioningStatus}\"");
            }

            ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            ExpectKept("a time display change");

            var updatedText = lastUpdated.GetCellContent(row) is TextBlock updatedCell ? updatedCell.Text : "(the cell is not realized)";
            if (updatedText != "last known, 2026-11-07 01:30 (UTC-08:00)")
            {
                found.Add($"after a time display change the Last Updated cell reads \"{updatedText}\", not the server's own clock");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A server's CPU samples leave the 24-hour window while its row is on screen, as they do when its collection stops.
    /// The next overlay re-read takes the verdict and the score off the row, and both cells read "Not collected", as a load of
    /// the same store shows them.</summary>
    [Fact]
    public async Task AnOverlayReRead_AfterTheCpuSamplesLeaveTheWindow_ReadsNotCollected()
    {
        var server = Unreachable("inv-cells-aged-out", 1);
        var initializer = await StoreAsync((server, null));
        await SeedCpuAndMemoryAsync(server);

        var problems = OnTheInventory(new LocalDataService(initializer), new[] { server }, async (tab, window, grid) =>
        {
            var found = new List<string>();
            if (grid.Items.Count != 1 || grid.Items[0] is not ServerPropertyRow row)
            {
                found.Add($"the inventory lists {grid.Items.Count} servers, not the one");
                return found;
            }

            if (string.IsNullOrEmpty(row.ProvisioningStatus) || row.HealthScore is null)
            {
                found.Add($"the load brought the verdict \"{row.ProvisioningStatus}\" and the score {row.HealthScore?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "(none)"}, so the test cannot read both leaving the row");
                return found;
            }

            await MoveCpuSamplesOutOfTheWindowAsync(server);
            await (Task)typeof(FinOpsTab).GetMethod("ReReadServerInventoryOverlayAsync", Instance)!.Invoke(tab, null)!;
            Settle(window);

            foreach (var header in new[] { "Status", "Health" })
            {
                var text = CellText(grid, row, header);
                if (text != "Not collected")
                {
                    found.Add($"after the re-read the {header} cell reads \"{text}\", not \"Not collected\": the last 24 hours hold no CPU sample for the server");
                }
            }

            var statusColumn = grid.Columns.First(c => HeaderText(c) == "Status");
            var tip = (statusColumn.GetCellContent(row) as TextBlock)?.ToolTip as string;
            if (tip != "No CPU sample in the last 24 hours")
            {
                found.Add($"after the re-read the Status cell has the tooltip \"{tip}\", not \"No CPU sample in the last 24 hours\"");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A row's Last Updated is an instant in UTC. With no clock of its own (a row nothing stamped) it reads the active
    /// tab's clock in Server Time and UTC in UTC, as the other FinOps rows that word their own times do.</summary>
    [Fact]
    public void LastUpdated_IsAUtcInstant_ShownOnTheSelectedTimeDisplay()
    {
        var row = new ServerPropertyRow { LastUpdated = CollectedAtUtc };

        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(345);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-07 15:15 (UTC+05:45)", row.LastUpdatedDisplay);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-07 09:30", row.LastUpdatedDisplay);
    }

    /// <summary>#8634: Start Time and Last Updated are worded the same way: both carry the server's offset in Server Time and neither
    /// in UTC. Start Time is the instant the server's tick counts give, not its own clock reading, so UTC shows the UTC time.</summary>
    [Fact]
    public void StartTime_IsLabelledLikeLastUpdated_OnTheSelectedTimeDisplay()
    {
        var row = new ServerPropertyRow
        {
            LastUpdated = CollectedAtUtc,
            SqlServerStartUtc = new DateTime(2026, 9, 9, 4, 41, 0, DateTimeKind.Unspecified),
            SqlServerStartTime = new DateTime(2026, 9, 9, 0, 41, 0, DateTimeKind.Unspecified),
        };

        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(-240);
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-09-09 00:41 (UTC-04:00)", row.StartTimeDisplay);
        Assert.EndsWith("(UTC-04:00)", row.LastUpdatedDisplay);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-09-09 04:41", row.StartTimeDisplay);

        /* A row with no start instant shows the server's own reading, which carries no label it cannot back. */
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-09-09 00:41", new ServerPropertyRow { SqlServerStartTime = row.SqlServerStartTime }.StartTimeDisplay);
        Assert.Equal("", new ServerPropertyRow().StartTimeDisplay);
    }

    /// <summary>A fresh store from the product's template with one collected <c>server_properties</c> row per server, each with the
    /// UTC offset its server reported (null for a row collected without one).</summary>
    private async Task<DuckDbInitializer> StoreAsync(params (ServerConnection Server, int? UtcOffsetMinutes)[] servers)
    {
        var dbPath = Path.Combine(_tempDir, "inventory.duckdb");
        _store = new DuckDbInitializer(dbPath);
        await _store.InitializeFromTemplateAsync();

        using var conn = new DuckDBConnection($"Data Source={dbPath}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var (server, offset) in servers)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name,
                                  edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb, socket_count, cores_per_socket, utc_offset_minutes)
                                VALUES ($1, $2, $3, $4, 'Enterprise Edition: Core-based Licensing', '16.0.4150.1', 'RTM', 3, 16, 2, 65536, 2, 8, $5)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cmd.Parameters.Add(new DuckDBParameter { Value = CollectedAtUtc });
            cmd.Parameters.Add(new DuckDBParameter { Value = IdOf(server) });
            cmd.Parameters.Add(new DuckDBParameter { Value = server.ServerName });
            cmd.Parameters.Add(new DuckDBParameter { Value = offset is int minutes ? minutes : DBNull.Value });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        return _store;
    }

    /// <summary>Twenty CPU samples over the last hours and one memory row for <paramref name="server"/>: enough for a verdict and a score.</summary>
    private async Task SeedCpuAndMemoryAsync(ServerConnection server)
    {
        using var conn = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "inventory.duckdb")}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);
        var now = DateTime.UtcNow;
        for (var i = 0; i < 20; i++)
        {
            using var cpu = conn.CreateCommand();
            cpu.CommandText = @"INSERT INTO cpu_utilization_stats
                                  (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
                                VALUES ($1, $2, $3, $4, $2, $2, 40, 3)";
            cpu.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cpu.Parameters.Add(new DuckDBParameter { Value = now.AddMinutes(-10 * (i + 1)) });
            cpu.Parameters.Add(new DuckDBParameter { Value = IdOf(server) });
            cpu.Parameters.Add(new DuckDBParameter { Value = server.ServerName });
            await cpu.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        using var memory = conn.CreateCommand();
        memory.CommandText = @"INSERT INTO memory_stats
                                 (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb,
                                  target_server_memory_mb, total_server_memory_mb, buffer_pool_mb)
                               VALUES ($1, $2, $3, $4, 65536, 8192, 49152, 49152, 40960)";
        memory.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
        memory.Parameters.Add(new DuckDBParameter { Value = now.AddMinutes(-5) });
        memory.Parameters.Add(new DuckDBParameter { Value = IdOf(server) });
        memory.Parameters.Add(new DuckDBParameter { Value = server.ServerName });
        await memory.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>Replaces <paramref name="server"/>'s CPU samples with twenty that are all older than 24 hours: what the store holds a
    /// day after the server's collection stopped. Its memory row stays.</summary>
    private async Task MoveCpuSamplesOutOfTheWindowAsync(ServerConnection server)
    {
        using var conn = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "inventory.duckdb")}");
        await conn.OpenAsync(TestContext.Current.CancellationToken);

        using (var remove = conn.CreateCommand())
        {
            remove.CommandText = "DELETE FROM cpu_utilization_stats WHERE server_id = $1";
            remove.Parameters.Add(new DuckDBParameter { Value = IdOf(server) });
            await remove.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var dayAgo = DateTime.UtcNow.AddHours(-25);
        for (var i = 0; i < 20; i++)
        {
            using var cpu = conn.CreateCommand();
            cpu.CommandText = @"INSERT INTO cpu_utilization_stats
                                  (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization)
                                VALUES ($1, $2, $3, $4, $2, $2, 40, 3)";
            cpu.Parameters.Add(new DuckDBParameter { Value = _nextId-- });
            cpu.Parameters.Add(new DuckDBParameter { Value = dayAgo.AddMinutes(-10 * (i + 1)) });
            cpu.Parameters.Add(new DuckDBParameter { Value = IdOf(server) });
            cpu.Parameters.Add(new DuckDBParameter { Value = server.ServerName });
            await cpu.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    /// <summary>
    /// Opens the FinOps tab over <paramref name="dataService"/> and <paramref name="servers"/> in a Dark-themed window on the STA thread,
    /// runs the Server Inventory load once, and hands the grid to <paramref name="check"/>.
    /// </summary>
    private List<string> OnTheInventory(LocalDataService dataService, IReadOnlyList<ServerConnection> servers, Func<DataGrid, List<string>> check) =>
        OnTheInventory(dataService, servers, (_, _, grid) => Task.FromResult(check(grid)));

    /// <summary>The same, for a check that also takes the tab and its window. With <paramref name="betweenStartUpAndFirstShow"/> the
    /// order is the app's at start-up instead: the inventory is read while the tab has never been shown, the callback runs, the
    /// window is shown (the tab's first show, which runs its own show handler), and the load that show starts is awaited.</summary>
    private List<string> OnTheInventory(
        LocalDataService dataService, IReadOnlyList<ServerConnection> servers, Func<FinOpsTab, Window, DataGrid, Task<List<string>>> check,
        Func<FinOpsTab, Task>? betweenStartUpAndFirstShow = null)
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
                /* The tab's services, without the server picker's own loads: this test drives one Server Inventory load. */
                typeof(FinOpsTab).GetField("_dataService", Instance)!.SetValue(tab, dataService);
                typeof(FinOpsTab).GetField("_serverManager", Instance)!.SetValue(tab, serverManager);
                typeof(FinOpsTab).GetField("_credentialResolver", Instance)!.SetValue(tab, new CredentialResolver(new CredentialService(), null));

                var load = typeof(FinOpsTab).GetMethod("LoadServerInventoryAsync", Instance)!;
                if (betweenStartUpAndFirstShow is not null)
                {
                    await (Task)load.Invoke(tab, new object[] { false })!;
                    await betweenStartUpAndFirstShow(tab);
                }

                var window = new Window { Content = tab, Width = 1000, Height = 700, Left = 0, Top = 0, WindowStyle = WindowStyle.None, ResizeMode = ResizeMode.NoResize };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    var grid = tab.FindName("ServerInventoryDataGrid") as DataGrid ?? throw new Xunit.Sdk.XunitException("ServerInventoryDataGrid is not on the tab");
                    Reveal(grid);
                    Settle(window);

                    if (betweenStartUpAndFirstShow is null)
                    {
                        await (Task)load.Invoke(tab, new object[] { true })!;
                    }
                    else if (typeof(FinOpsTab).GetProperty("ServerInventoryReload", Instance | BindingFlags.Public)?.GetValue(tab) is Task reload)
                    {
                        await reload;
                    }

                    Settle(window);

                    problems.AddRange(await check(tab, window, grid));
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

    /// <summary>The text the grid draws in <paramref name="item"/>'s cell under the column headed <paramref name="header"/>.</summary>
    private static string CellText(DataGrid grid, object item, string header)
    {
        var column = grid.Columns.FirstOrDefault(c => HeaderText(c) == header)
            ?? throw new Xunit.Sdk.XunitException($"the grid has no {header} column");
        grid.ScrollIntoView(item, column);
        grid.UpdateLayout();
        return column.GetCellContent(item) is TextBlock text ? text.Text : "(the cell is not realized)";
    }

    private static string HeaderText(DataGridColumn column) => column.Header switch
    {
        string text => text,
        Panel panel => panel.Children.OfType<TextBlock>().FirstOrDefault()?.Text ?? "",
        _ => "",
    };

    private static IEnumerable<DependencyObject> Descendants(DependencyObject root)
    {
        for (var i = 0; i < System.Windows.Media.VisualTreeHelper.GetChildrenCount(root); i++)
        {
            var child = System.Windows.Media.VisualTreeHelper.GetChild(root, i);
            yield return child;
            foreach (var below in Descendants(child))
            {
                yield return below;
            }
        }
    }

    private static void Settle(Window window)
    {
        window.UpdateLayout();
        ClippedTextProbe.Settle(window);
    }

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
