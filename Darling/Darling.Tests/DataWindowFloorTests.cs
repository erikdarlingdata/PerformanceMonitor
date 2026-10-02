/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The data-start probe's SQL, the relations it accepts, which relations a compiled panel probes, and the notice's
/// words. The live classes (<see cref="ComposeDataFloorLiveTests"/>, <see cref="ComposeRollupDataFloorLiveTests"/>)
/// run the probe against PostgreSQL; these pin what it is allowed to read and how.
/// </summary>
public sealed class DataWindowFloorTests
{
    [Fact]
    public void ThePanelProbe_AsksEachServerThroughItsOwnIndex_NeverFiltersTheFactTableByName()
    {
        var sql = DataWindowFloor.FloorSql([DataWindowFloor.Source.ForCollectorTable("waiting_tasks")], DataWindowFloor.Scope.ServerNames);

        Assert.Contains("FROM collect.servers AS s", sql, StringComparison.Ordinal);
        Assert.Contains("CROSS JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE f.server_id = s.server_id", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE s.server_name = ANY($2)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("f.server_name", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY f.collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    /// <summary>Unbounded below: the oldest row at or before the window's end, never the oldest row inside the
    /// window, which a quiet first hour pushes past the window's start.</summary>
    [Fact]
    public void ThePanelProbe_BoundsOnlyTheEnd()
    {
        var raw = DataWindowFloor.FloorSql([DataWindowFloor.Source.ForCollectorTable("waiting_tasks")], DataWindowFloor.Scope.Fleet);
        Assert.Contains("AND   f.collection_time <= $1", raw, StringComparison.Ordinal);
        Assert.DoesNotContain(">=", raw, StringComparison.Ordinal);
        Assert.DoesNotContain("$2", raw, StringComparison.Ordinal);

        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsHourlyView, out var hourly));
        var rollup = DataWindowFloor.FloorSql([hourly], DataWindowFloor.Scope.ServerId);
        Assert.Contains("AND   f.bucket < $1", rollup, StringComparison.Ordinal);
        Assert.Contains("WHERE s.server_id = $2", rollup, StringComparison.Ordinal);
    }

    [Fact]
    public void TwoSources_AreProbedSideBySide_AndTheOldestWins()
    {
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsHourlyView, out var legacy));
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsIntervalHourlyView, out var successor));

        var sql = DataWindowFloor.FloorSql([legacy, successor], DataWindowFloor.Scope.Fleet);

        Assert.StartsWith("SELECT MIN(u.t)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect." + TimescaleSupport.QueryStatsHourlyView + " AS f", sql, StringComparison.Ordinal);
        Assert.Contains("FROM collect." + TimescaleSupport.QueryStatsIntervalHourlyView + " AS f", sql, StringComparison.Ordinal);
        Assert.Contains("UNION ALL", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("waiting_tasks", true)]
    [InlineData("query_snapshots", true)]
    [InlineData("wait_stats", true)]
    [InlineData("index_object_stats", false)]
    [InlineData("server_config", false)]
    [InlineData("database_config", false)]
    [InlineData("query_stats; DROP TABLE x", false)]
    [InlineData("not_a_table", false)]
    public void CollectorTables_AreProbedOnlyWhenTheirIndexLeadsWithServerAndTime(string table, bool accepted)
    {
        Assert.Equal(accepted, DataWindowFloor.Source.TryForCollectorTable(table, out _));
    }

    [Theory]
    [InlineData("query_stats_hourly", true)]
    [InlineData("query_store_stats_corrected_daily", true)]
    [InlineData("wait_stats", false)]
    [InlineData("query_stats_hourly; DROP TABLE x", false)]
    [InlineData("", false)]
    public void Rollups_AreProbedOnlyWhenTheRegistryKnowsThem(string view, bool accepted)
    {
        Assert.Equal(accepted, DataWindowFloor.Source.TryForRollup(view, out _));
    }

    /// <summary>Every rollup a Custom Views panel can be routed to is one the probe accepts, so no rollup-routed
    /// panel goes without a start.</summary>
    [Fact]
    public void EveryRollupAPanelCanRead_IsOneTheProbeAccepts()
    {
        foreach (var table in new[] { "query_stats", "procedure_stats", "query_store_stats" })
        {
            var info = ComposeCaggCatalog.For(table)!;
            var names = new[] { info.HourlyView, info.DailyView, info.LegacyHourlyView, info.LegacyDailyView, info.DayGrainDailyView }
                .Where(n => n is not null)
                .Select(n => n!)
                .ToList();
            names.AddRange(names.Select(TimescaleSupport.SuccessorOf).Where(n => n is not null).Select(n => n!).ToList());
            names.AddRange(TimescaleSupport.SupersededDailyRollups
                .Where(p => names.Contains(p.LegacyDaily))
                .Select(p => p.SuccessorDaily)
                .ToList());

            foreach (var name in names.Distinct())
            {
                Assert.True(DataWindowFloor.Source.TryForRollup(name, out _), name + " is a rollup a panel can read, but the probe refuses it");
            }
        }
    }

    [Fact]
    public void ARawRoute_ProbesItsSourceTable()
    {
        var sources = ComposeStoreAvailability.DataStartSources("waiting_tasks", ComposeRoute.Raw);

        Assert.Equal("waiting_tasks", Assert.Single(sources).Relation);
    }

    [Fact]
    public void ARawRoute_OverATableTheProbeCannotRead_ProbesNothing()
    {
        Assert.Empty(ComposeStoreAvailability.DataStartSources("index_object_stats", ComposeRoute.Raw));
    }

    [Fact]
    public void ARollupRoute_ProbesTheRollupItRead()
    {
        var route = new ComposeRoute(
            ComposeSourceTier.Hourly, TimescaleSupport.QueryStatsHourlyView, "collect." + TimescaleSupport.QueryStatsHourlyView + " AS f");

        var source = Assert.Single(ComposeStoreAvailability.DataStartSources("query_stats", route));

        Assert.Equal(TimescaleSupport.QueryStatsHourlyView, source.Relation);
        Assert.Equal("bucket", source.TimeColumn);
        Assert.True(source.EndExclusive);
    }

    /// <summary>A route that stitches a superseded rollup to its successor reads both, so both are probed.</summary>
    [Fact]
    public void AStitchedRollupRoute_ProbesBothHalves()
    {
        var from = "(SELECT bucket, server_id FROM collect." + TimescaleSupport.QueryStatsHourlyView
            + " WHERE bucket < TIMESTAMP '2026-09-01 00:00:00.000000' UNION ALL SELECT bucket, server_id FROM collect."
            + TimescaleSupport.QueryStatsIntervalHourlyView + " WHERE bucket >= TIMESTAMP '2026-09-01 00:00:00.000000') AS f";
        var route = new ComposeRoute(ComposeSourceTier.Hourly, TimescaleSupport.QueryStatsIntervalHourlyView, from);

        var relations = ComposeStoreAvailability.DataStartSources("query_stats", route).Select(s => s.Relation).ToArray();

        Assert.Equal(new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView }, relations);
    }

    [Fact]
    public void TheNotice_NamesTheStartAndTheWindowItCovers_InUtc()
    {
        var notice = ComposeStoreAvailability.BuildDataStartNotice(
            new DateTime(2026, 9, 25, 14, 2, 0, DateTimeKind.Utc),
            new DateTime(2026, 9, 2, 14, 0, 0, DateTimeKind.Utc),
            new DateTime(2026, 10, 2, 14, 0, 0, DateTimeKind.Utc));

        Assert.Equal(
            "partial window: this panel's data starts at 2026-09-25 14:02 UTC, after the window's start at 2026-09-02 14:00 UTC. "
                + "The panel covers 2026-09-25 14:02 to 2026-10-02 14:00 UTC.",
            notice);
    }
}
