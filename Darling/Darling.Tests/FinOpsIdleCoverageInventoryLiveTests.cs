/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The idle-database coverage rule read through the Server Inventory statement (<c>GetServerMetricsAsync</c>, the fleet read whose
/// <c>idle_db_count</c> is NULL for a server without coverage), on a plain store, on a TimescaleDB store without rollups used, and
/// on a TimescaleDB store with the hourly rollups routed (#5492 round 2). The scenarios are the three the per-server statement is
/// tested on (six and a half days, a missing D-3, a full seven and a half) plus the two day-boundary samples: one exactly at
/// 00:00:00.000000 UTC of D-3 (that day is covered by it alone), and one at D-4 23:59:59.999999 with D-2's 00:00:00.000000
/// (neither is on D-3, so D-3 stays uncovered).
/// </summary>
/* #1776 own-store: the scenarios share one scratch database, and each server's rows are its own, so nothing here shares rows with another test. */
public sealed class FinOpsIdleCoverageInventoryLiveTests
{
    private const int TimeoutSeconds = 30;
    private static readonly DateTime Day = new(2026, 10, 7, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly double[] HoursOfDay = { 0.5, 12, 23.5 };

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private enum Scenario { SixAndAHalfDays, GapAtDMinus3, FullSevenAndAHalf, MidnightSampleCoversDMinus3, NeighbouringEdgesLeaveDMinus3Empty }

    /// <summary>Whether the scenario's server holds coverage, so a known never-executed database reads as idle (count 1) rather than NULL.</summary>
    private static bool Covered(Scenario s) => s is Scenario.FullSevenAndAHalf or Scenario.MidnightSampleCoversDMinus3;

    private static async Task<List<(int Id, DateTime Now, Scenario Scenario)>> SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var seeded = new List<(int, DateTime, Scenario)>();
        foreach (var hour in HoursOfDay)
        {
            var now = Day.AddHours(hour);
            var dMinus3 = now.Date.AddDays(-3);
            foreach (var scenario in Enum.GetValues<Scenario>())
            {
                var (id, name) = await FinOpsIdleCoverageLiveTests.RegisterAsync(c, $"darling-idle-inv-{scenario}-{hour}", ct);
                var days = scenario == Scenario.SixAndAHalfDays ? 6.5 : 7.5;
                var gap = scenario is Scenario.GapAtDMinus3 or Scenario.MidnightSampleCoversDMinus3 or Scenario.NeighbouringEdgesLeaveDMinus3Empty;
                await FinOpsIdleCoverageLiveTests.SeedHalfDaysAsync(c, id, name, now, days, at => gap && at.Date == dMinus3, ct);

                if (scenario == Scenario.MidnightSampleCoversDMinus3)
                {
                    await FinOpsIdleCoverageSeed.InsertAsync(c, ct, id, name, dMinus3, "mid");
                }
                else if (scenario == Scenario.NeighbouringEdgesLeaveDMinus3Empty)
                {
                    await FinOpsIdleCoverageSeed.InsertAsync(c, ct, id, name, dMinus3.AddTicks(-10), "prevEnd");
                    await FinOpsIdleCoverageSeed.InsertAsync(c, ct, id, name, dMinus3.AddDays(1), "nextStart");
                }

                /* One known database that never executed: idle when the server is covered, no count at all when it is not. */
                await DarlingMcpTestData.ExecAsync(c, ct,
                    "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb) VALUES ($1, $2, $3, $4, 'NeverRunDb', 100)",
                    CollectionIdGenerator.Next(), now.AddHours(-1), id, name);
                seeded.Add((id, now, scenario));
            }
        }

        return seeded;
    }

    private static async Task AssertInventoryAsync(
        NpgsqlDataSource dataSource, RollupAvailability rollups, RollupCoverage coverage, List<(int Id, DateTime Now, Scenario Scenario)> seeded, string path, CancellationToken ct)
    {
        foreach (var (id, now, scenario) in seeded)
        {
            var metrics = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, TimeoutSeconds, now, ct);
            var expected = Covered(scenario) ? 1 : (int?)null;
            Assert.True(metrics.TryGetValue(id, out var row), $"{path}: no row for {scenario} at {now:HH:mm}");
            Assert.True(expected == row.IdleDbCount, $"{path}: {scenario} at {now:HH:mm}: idle_db_count {row.IdleDbCount?.ToString() ?? "NULL"}, expected {expected?.ToString() ?? "NULL"}");
            Assert.Equal(Covered(scenario), await DarlingFinOpsOptimizationReader.HasIdleCoverageAtAsync(dataSource, id, now, TimeoutSeconds, ct));
        }
    }

    [Fact]
    public async Task InventoryIdleCount_FollowsTheCoverageRule_OnAPlainStore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live idle-coverage inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var seeded = await SeedAsync(c, ct);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.False(rollups.DbGrainHourly);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        await AssertInventoryAsync(dataSource, rollups, coverage, seeded, "plain store", ct);
    }

    [Fact]
    public async Task InventoryIdleCount_FollowsTheCoverageRule_OnATimescaleStore_RawAndRollupRouted()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live idle-coverage inventory test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(c, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The routed idle-coverage read needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(c, null, ct));

        var seeded = await SeedAsync(c, ct);

        Assert.True(await TimescaleSupport.EnsureContinuousAggregatesAsync(c, null, ct) > 0);
        /* Legacy hourly view for the first days, the interval view after it, both up to 23:00 on the last day; the last half
           hour of that day is read raw. */
        var degraded = new RefreshDisclosure(message => Assert.Fail($"the refresh degraded unexpectedly: {message}"));
        var start = Day.AddDays(-9);
        var split = Day.AddDays(-4);
        var watermarkTarget = Day.AddHours(23);
        await RollupBackfill.RunSliceAsync(c, TimescaleSupport.QueryStatsDbHourlyView, start, split, degraded, ct);
        await RollupBackfill.RunSliceAsync(c, TimescaleSupport.QueryStatsDbIntervalHourlyView, split, watermarkTarget, degraded, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        Assert.True(rollups.DbGrainHourly, "the routed path needs the database-grain hourly rollup");
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

        /* The same statement unrouted on the hypertable layout (no rollup used), then routed through the rollups. */
        await AssertInventoryAsync(dataSource, RollupAvailability.None, coverage, seeded, "hypertable, no rollups", ct);
        await AssertInventoryAsync(dataSource, rollups, coverage, seeded, "hypertable, routed through the rollups", ct);
    }
}
