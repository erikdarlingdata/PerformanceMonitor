/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5643 part B: the collection loop's late cycles reach a reader. The loop writes one <c>collection_cycle</c>/<c>SKIPPED</c>
/// row per enabled server for a cycle that overran its slot; the Collection Health tab prints a summary line over the last
/// 24 h and <c>get_collection_health</c> carries a <c>skipped_cycles</c> block. The rows are seeded here in the shape the loop
/// writes, against a REAL DuckDB through the real reader.
/// </summary>
public sealed class SkippedCycleSurfaceTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerA = 4301;
    private const int ServerB = 4302;

    /// <summary>A fixed "now", so the 24 h window's edges are exact.</summary>
    private static readonly DateTime AsOf = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Unspecified);

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public SkippedCycleSurfaceTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    [Fact]
    public async Task TheSummary_CountsCyclesAndSlots_AndTheNewestRowSuppliesTheLastFields()
    {
        var service = new LocalDataService(_duckDb);

        /* Seeded newest FIRST, so "last" is the newest by time, not the last inserted. */
        await SeedCycleAsync(ServerA, AsOf.AddHours(-1), slots: 3, step: "collectors", stepMs: 412_300, totalMs: 480_000, message: "newest late cycle");
        await SeedCycleAsync(ServerA, AsOf.AddHours(-5), slots: 2, step: "archival", stepMs: 90_000, totalMs: 130_000, message: "older late cycle");
        await SeedCycleAsync(ServerA, AsOf.AddHours(-9), slots: 1, step: "wait", stepMs: 70_000, totalMs: 70_000, message: "oldest late cycle");

        var summary = await service.GetSkippedCycleSummaryAsync(ServerA, AsOf);

        Assert.Equal(3, summary.Count);
        Assert.Equal(6, summary.SlotsSkipped);
        Assert.Equal(AsOf.AddHours(-1), summary.LastAtUtc);
        Assert.Equal("collectors", summary.LastSlowestStep);
        Assert.Equal(412_300, summary.LastSlowestStepMs);
        Assert.Equal(480_000, summary.LastDurationMs);
        Assert.Equal("newest late cycle", summary.LastMessage);

        var line = summary.SummaryLine();
        Assert.Contains("3 late cycle(s)", line, StringComparison.Ordinal);
        Assert.Contains("6 slot(s) skipped", line, StringComparison.Ordinal);
        Assert.Contains("slowest step collectors, 412.3 s", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheWindow_IsTwentyFourHours_InclusiveAtBothEnds()
    {
        var service = new LocalDataService(_duckDb);

        await SeedCycleAsync(ServerA, AsOf.AddHours(-24).AddSeconds(-1), slots: 100, step: "wait", stepMs: 1, totalMs: 1, message: "just outside");
        await SeedCycleAsync(ServerA, AsOf.AddHours(-24), slots: 1, step: "wait", stepMs: 1, totalMs: 1, message: "on the lower edge");
        await SeedCycleAsync(ServerA, AsOf, slots: 2, step: "wait", stepMs: 1, totalMs: 1, message: "on the upper edge");
        await SeedCycleAsync(ServerA, AsOf.AddSeconds(1), slots: 1000, step: "wait", stepMs: 1, totalMs: 1, message: "after the as-of");

        var summary = await service.GetSkippedCycleSummaryAsync(ServerA, AsOf);

        Assert.Equal(2, summary.Count);
        Assert.Equal(3, summary.SlotsSkipped);
        Assert.Equal("on the upper edge", summary.LastMessage);
    }

    [Fact]
    public async Task NoRows_IsTheAllClear_AndOnlyTheCycleRowsOfThisServerCount()
    {
        var service = new LocalDataService(_duckDb);

        /* Not late-cycle rows: a collector's own SKIPPED run, the cycle name with another status, and another server's cycle. */
        await SeedAsync(ServerA, "wait_stats", AsOf.AddHours(-1), "SKIPPED", slots: 9, step: "x", stepMs: 1, totalMs: 1, message: "a collector's skip");
        await SeedAsync(ServerA, "collection_cycle", AsOf.AddHours(-1), "SUCCESS", slots: 9, step: "x", stepMs: 1, totalMs: 1, message: "not a skip");
        await SeedCycleAsync(ServerB, AsOf.AddHours(-1), slots: 4, step: "analysis", stepMs: 5_000, totalMs: 9_000, message: "server B");

        var none = await service.GetSkippedCycleSummaryAsync(ServerA, AsOf);

        Assert.Equal(0, none.Count);
        Assert.Equal(0, none.SlotsSkipped);
        Assert.Null(none.LastAtUtc);
        Assert.Null(none.LastSlowestStep);
        Assert.Equal("No skipped cycles in 24 h", none.SummaryLine());
    }

    [Fact]
    public async Task EachServer_ReadsItsOwnCycles()
    {
        var service = new LocalDataService(_duckDb);

        await SeedCycleAsync(ServerA, AsOf.AddHours(-2), slots: 3, step: "collectors", stepMs: 1_000, totalMs: 2_000, message: "A");
        await SeedCycleAsync(ServerB, AsOf.AddHours(-3), slots: 1, step: "retention", stepMs: 2_000, totalMs: 3_000, message: "B1");
        await SeedCycleAsync(ServerB, AsOf.AddHours(-4), slots: 5, step: "analysis", stepMs: 3_000, totalMs: 4_000, message: "B2");

        var a = await service.GetSkippedCycleSummaryAsync(ServerA, AsOf);
        var b = await service.GetSkippedCycleSummaryAsync(ServerB, AsOf);

        Assert.Equal((1L, 3L, "A"), (a.Count, a.SlotsSkipped, a.LastMessage));
        Assert.Equal((2L, 6L, "B1"), (b.Count, b.SlotsSkipped, b.LastMessage));
        Assert.Equal("retention", b.LastSlowestStep);
    }

    [Fact]
    public async Task TheCycleRow_IsNotACollectorRow_InTheCollectionHealthGrid()
    {
        var service = new LocalDataService(_duckDb);

        await SeedAsync(ServerA, "wait_stats", DateTime.UtcNow.AddMinutes(-5), "SUCCESS", slots: null, step: null, stepMs: null, totalMs: 100, message: null);
        await SeedCycleAsync(ServerA, DateTime.UtcNow.AddMinutes(-3), slots: 3, step: "collectors", stepMs: 1_000, totalMs: 2_000, message: "late");

        var rows = await service.GetCollectionHealthAsync(ServerA);

        Assert.Equal(new[] { "wait_stats" }, rows.Select(r => r.CollectorName).ToArray());
    }

    [Fact]
    public async Task TheTabsLine_WordsTheTimeWithTheTabsOwnFormatter()
    {
        var service = new LocalDataService(_duckDb);
        await SeedCycleAsync(ServerA, AsOf.AddHours(-1), slots: 1, step: "wait", stepMs: 1_500, totalMs: 2_000, message: "late");

        var summary = await service.GetSkippedCycleSummaryAsync(ServerA, AsOf);

        /* Shaped like the Collection Health tab's other instants, from the same formatter. */
        Assert.Contains(CollectionHealthTime.Format(summary.LastAtUtc!.Value, null), summary.SummaryLine(), StringComparison.Ordinal);
        Assert.Contains("slowest step wait, 1.5 s", summary.SummaryLine(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheMcpBlock_CarriesTheContractFields_AndZeroIsARealAnswer()
    {
        var some = JsonSerializer.SerializeToElement(McpHealthTools.SkippedCyclesBlock(new SkippedCycleSummary
        {
            Count = 3,
            SlotsSkipped = 6,
            LastAtUtc = AsOf,
            LastSlowestStep = "collectors",
            LastSlowestStepMs = 412_300,
            LastMessage = "late"
        }));

        Assert.Equal(
            new[] { "count_24h", "last_at", "last_message", "last_slowest_step", "last_slowest_step_ms", "slots_skipped_24h" },
            some.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(3, some.GetProperty("count_24h").GetInt64());
        Assert.Equal(6, some.GetProperty("slots_skipped_24h").GetInt64());
        Assert.Equal("collectors", some.GetProperty("last_slowest_step").GetString());
        Assert.Equal(412_300, some.GetProperty("last_slowest_step_ms").GetInt64());
        Assert.Equal(AsOf.ToString("o"), some.GetProperty("last_at").GetString());

        var none = JsonSerializer.SerializeToElement(McpHealthTools.SkippedCyclesBlock(SkippedCycleSummary.None));
        Assert.Equal(0, none.GetProperty("count_24h").GetInt64());
        Assert.Equal(0, none.GetProperty("slots_skipped_24h").GetInt64());
        Assert.Equal(JsonValueKind.Null, none.GetProperty("last_at").ValueKind);
        Assert.Equal(JsonValueKind.Null, none.GetProperty("last_slowest_step_ms").ValueKind);
    }

    /// <summary>
    /// The wiring no behavioural test reaches: the tool body and the tab's refresh are UI/host code. Both must read the
    /// summary and print it, and the tab must have a place for the line above the grids.
    /// </summary>
    [Fact]
    public void TheToolAndTheTab_AreWiredToTheSummary()
    {
        var root = RepoRoot();
        var tool = File.ReadAllText(Path.Combine(root, "Lite", "Mcp", "McpHealthTools.cs"));
        var refresh = File.ReadAllText(Path.Combine(root, "Lite", "Controls", "ServerTab.Refresh.cs"));
        var xaml = File.ReadAllText(Path.Combine(root, "Lite", "Controls", "ServerTab.xaml"));
        var health = File.ReadAllText(Path.Combine(root, "Lite", "Services", "LocalDataService.CollectionHealth.cs"));

        Assert.Contains("skipped_cycles = SkippedCyclesBlock(skippedCycles),", tool, StringComparison.Ordinal);
        Assert.Contains("dataService.GetSkippedCycleSummaryAsync(resolved.ServerId)", tool, StringComparison.Ordinal);
        Assert.Contains("SkippedCyclesSummary.Text = ", refresh, StringComparison.Ordinal);
        Assert.Contains(".SummaryLine(tabClock)", refresh, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"SkippedCyclesSummary\"", xaml, StringComparison.Ordinal);
        Assert.Contains("AND   collector_name <> 'collection_cycle'", health, StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }

    private Task SeedCycleAsync(int serverId, DateTime at, int slots, string step, int stepMs, int totalMs, string message) =>
        SeedAsync(serverId, "collection_cycle", at, "SKIPPED", slots, step, stepMs, totalMs, message);

    private async Task SeedAsync(
        int serverId, string collector, DateTime at, string status, int? slots, string? step, int? stepMs, int totalMs, string? message)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
        {
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message,
     rows_collected, fanout_item_count, slowest_item, slowest_item_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = "TestSrv" });
        cmd.Parameters.Add(new DuckDBParameter { Value = collector });
        cmd.Parameters.Add(new DuckDBParameter { Value = at });
        cmd.Parameters.Add(new DuckDBParameter { Value = totalMs });
        cmd.Parameters.Add(new DuckDBParameter { Value = status });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)message ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)slots ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)step ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)stepMs ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }
}
