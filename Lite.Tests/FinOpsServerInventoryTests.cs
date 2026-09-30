/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The FinOps Server Inventory's collected overlay is an optional extra that had never run on a real store
/// (its read was driven from a table nothing fills), so it must never be able to cost the grid a row or break
/// the tab. These pin that through the same assembly path the tab uses, with a failure planted at each place
/// the overlay can fail: its read, and its per-server merge.
///
/// <para>They also pin which servers can appear at all: the inventory is built from the monitor's own list, so
/// a server that was removed from the monitor — whose collected rows stay in the store until retention purges
/// them, and so stay in the overlay's read — never shows up.</para>
///
/// <para>Collection <c>app-logger-statics</c>: the log assertions drain the process-wide buffer, which the
/// other tests that do the same share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class FinOpsServerInventoryTests : IDisposable
{
    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteFinOpsInv_" + Guid.NewGuid().ToString("N")[..8]);

    public FinOpsServerInventoryTests() => Directory.CreateDirectory(_tempDir);

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    private static ServerConnection Server(string name) => new()
    {
        Id = Guid.NewGuid().ToString(),
        ServerName = name,
        DisplayName = name + " (display)",
        MonthlyCostUsd = 100m,
    };

    private static int IdOf(ServerConnection server) =>
        RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));

    private static Task<ServerPropertyRow> Live(ServerConnection server) =>
        Task.FromResult(new ServerPropertyRow { Edition = "Enterprise Edition", CpuCount = 8 });

    private static void AssertOverlayBlank(ServerPropertyRow row)
    {
        Assert.Null(row.AvgCpuPct);
        Assert.Null(row.StorageTotalGb);
        Assert.Null(row.IdleDbCount);
        Assert.Null(row.ProvisioningStatus);
    }

    /// <summary>A planted throw in the overlay's read - thrown before the read returns a task, and from a
    /// faulted task - leaves every server's overlay blank, logs it, and still returns every live row.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AFailingOverlayRead_LeavesTheOverlayBlank_AndEveryLiveRowStillComesBack(bool faultsTheTask)
    {
        var first = Server("inv-read-first");
        var second = Server("inv-read-second");

        Func<Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>> failingRead = faultsTheTask
            ? () => Task.FromException<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>(new InvalidOperationException("planted overlay read failure"))
            : () => throw new InvalidOperationException("planted overlay read failure");

        AppLogger.DrainBufferedLines();
        var rows = await FinOpsServerInventory.BuildAsync(new[] { first, second }, Live, failingRead);
        var log = AppLogger.DrainBufferedLines();

        Assert.Equal(new[] { first.DisplayName, second.DisplayName }, rows.Select(r => r.ServerName));
        Assert.All(rows, AssertOverlayBlank);
        Assert.All(rows, r => Assert.Equal(100m, r.MonthlyCost));
        Assert.Contains(log, l => l.Contains("[FinOps]", StringComparison.Ordinal)
            && l.Contains("planted overlay read failure", StringComparison.Ordinal)
            && l.Contains("overlay stays blank", StringComparison.Ordinal));
    }

    /// <summary>A planted throw in the per-server merge leaves that overlay blank and logs it. The merge used
    /// to share the live read's catch, where the same throw dropped the whole server from the grid.</summary>
    [Fact]
    public async Task AFailingMerge_LeavesTheOverlayBlank_AndTheServerKeepsItsLiveRow()
    {
        var first = Server("inv-merge-first");
        var second = Server("inv-merge-second");

        AppLogger.DrainBufferedLines();
        var rows = await FinOpsServerInventory.BuildAsync(
            new[] { first, second },
            Live,
            () => Task.FromResult<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>(new MetricsThatThrowWhenLookedUp()));
        var log = AppLogger.DrainBufferedLines();

        Assert.Equal(new[] { first.DisplayName, second.DisplayName }, rows.Select(r => r.ServerName));
        Assert.All(rows, AssertOverlayBlank);
        Assert.All(rows, r => Assert.Equal("Enterprise Edition", r.Edition));
        Assert.Contains(log, l => l.Contains("Failed to overlay collected metrics for " + first.DisplayName, StringComparison.Ordinal)
            && l.Contains("planted merge failure", StringComparison.Ordinal));
        Assert.Contains(log, l => l.Contains("Failed to overlay collected metrics for " + second.DisplayName, StringComparison.Ordinal));
    }

    /// <summary>The overlay lands on the right server by its storage-name id, and a server whose live read
    /// fails is left out and logged exactly as before while the others keep their overlay.</summary>
    [Fact]
    public async Task TheOverlayLandsByStorageNameId_AndAServerWhoseLiveReadFailsIsLeftOut()
    {
        var healthy = Server("inv-healthy");
        var unreachable = Server("inv-unreachable");

        var metrics = new Dictionary<int, LocalDataService.ServerMetricsRow>
        {
            [IdOf(healthy)] = new(AvgCpuPct: 42m, StorageTotalGb: 7m, IdleDbCount: 3, ProvisioningStatus: "right_sized"),
            [IdOf(unreachable)] = new(AvgCpuPct: 99m, StorageTotalGb: 1m, IdleDbCount: 0, ProvisioningStatus: "over_provisioned"),
        };

        AppLogger.DrainBufferedLines();
        var rows = await FinOpsServerInventory.BuildAsync(
            new[] { healthy, unreachable },
            server => server == unreachable
                ? throw new InvalidOperationException("planted live failure")
                : Live(server),
            () => Task.FromResult<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>(metrics));
        var log = AppLogger.DrainBufferedLines();

        var only = Assert.Single(rows);
        Assert.Equal(healthy.DisplayName, only.ServerName);
        Assert.Equal(42m, only.AvgCpuPct);
        Assert.Equal(7m, only.StorageTotalGb);
        Assert.Equal(3, only.IdleDbCount);
        Assert.Equal("right_sized", only.ProvisioningStatus);
        Assert.Contains(log, l => l.Contains("Failed to query " + unreachable.DisplayName, StringComparison.Ordinal));
    }

    /// <summary>
    /// A server removed from the monitor keeps its <c>server_properties</c> rows until retention purges them,
    /// so the overlay's read still returns an entry for it. The inventory is built from the monitor's list, so
    /// that entry is never read: the removed server does not appear, and the listed one gets its own figures.
    /// </summary>
    [Fact]
    public async Task ACollectedServerThatIsNoLongerListed_NeverAppearsInTheInventory()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "inventory.duckdb"));
        await initializer.InitializeAsync();

        var listed = Server("inv-still-listed");
        var removed = Server("inv-removed-from-monitor");
        var now = DateTime.UtcNow;

        using (var conn = new DuckDBConnection($"Data Source={Path.Combine(_tempDir, "inventory.duckdb")}"))
        {
            await conn.OpenAsync();
            long nextId = -1;
            foreach (var (server, cpu) in new[] { (listed, 35), (removed, 80) })
            {
                Exec(conn, @"INSERT INTO server_properties (collection_id, collection_time, server_id, server_name,
                              edition, product_version, product_level, engine_edition, cpu_count, hyperthread_ratio, physical_memory_mb)
                             VALUES ($1,$2,$3,$4,'Test Edition','16.0.4150.1','RTM',3,8,1,16384)",
                    nextId--, now, IdOf(server), server.ServerName);
                Exec(conn, @"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
                             VALUES ($1,$2,$3,$4,$2,$5,5)",
                    nextId--, now, IdOf(server), server.ServerName, cpu);
            }
        }

        var service = new LocalDataService(initializer);

        /* Not vacuous: the overlay's read really does carry the removed server. */
        var collected = await service.GetServerMetricsAsync();
        Assert.True(collected.ContainsKey(IdOf(removed)));
        Assert.Equal(80m, collected[IdOf(removed)].AvgCpuPct);

        var rows = await FinOpsServerInventory.BuildAsync(
            new[] { listed },
            Live,
            async () => await service.GetServerMetricsAsync());

        var only = Assert.Single(rows);
        Assert.Equal(listed.DisplayName, only.ServerName);
        Assert.Equal(35m, only.AvgCpuPct);
        Assert.DoesNotContain(rows, r => r.ServerName == removed.DisplayName);
    }

    private static void Exec(DuckDBConnection conn, string sql, params object[] values)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values) cmd.Parameters.Add(new DuckDBParameter { Value = value });
        cmd.ExecuteNonQuery();
    }

    /// <summary>An overlay whose every lookup throws: the merge's failure, planted where the merge reads.</summary>
    private sealed class MetricsThatThrowWhenLookedUp : IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>
    {
        private static InvalidOperationException Planted() => new("planted merge failure");

        public LocalDataService.ServerMetricsRow this[int key] => throw Planted();
        public IEnumerable<int> Keys => throw Planted();
        public IEnumerable<LocalDataService.ServerMetricsRow> Values => throw Planted();
        public int Count => 1;
        public bool ContainsKey(int key) => throw Planted();
        public bool TryGetValue(int key, out LocalDataService.ServerMetricsRow value) => throw Planted();
        public IEnumerator<KeyValuePair<int, LocalDataService.ServerMetricsRow>> GetEnumerator() => throw Planted();
        IEnumerator IEnumerable.GetEnumerator() => throw Planted();
    }
}
