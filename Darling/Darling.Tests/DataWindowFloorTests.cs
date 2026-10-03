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
        Assert.Contains("LEFT JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CROSS JOIN LATERAL", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE f.server_id = s.server_id", sql, StringComparison.Ordinal);
        Assert.Contains("AND   s.server_name = ANY($4)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("f.server_name", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY f.collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }

    /// <summary>The scope is the fourth parameter (the window's end, its start and the probe's clock come first), and
    /// a fleet probe carries none.</summary>
    [Fact]
    public void TheScope_IsTheFourthParameter_AndAFleetProbeHasNone()
    {
        var waitingTasks = new[] { DataWindowFloor.Source.ForCollectorTable("waiting_tasks") };

        Assert.Contains("AND   s.server_id = $4", DataWindowFloor.FloorSql(waitingTasks, DataWindowFloor.Scope.ServerId), StringComparison.Ordinal);
        Assert.Contains("AND   s.server_name = ANY($4)", DataWindowFloor.FloorSql(waitingTasks, DataWindowFloor.Scope.ServerNames), StringComparison.Ordinal);
        Assert.DoesNotContain("$4", DataWindowFloor.FloorSql(waitingTasks, DataWindowFloor.Scope.Fleet), StringComparison.Ordinal);
    }

    /// <summary>The read inside the window has both bounds: it asks whether the server holds a row in the window and
    /// where its first one is, so the earliest instant a row can move coverage to is the window's start. Before this,
    /// the probe took the oldest row at or before the window's end, a row a purge may already have dropped.</summary>
    [Fact]
    public void TheWindowRead_IsBoundedOnBothSides_ForEverySource()
    {
        var raw = DataWindowFloor.FloorSql([DataWindowFloor.Source.ForCollectorTable("waiting_tasks")], DataWindowFloor.Scope.Fleet);
        Assert.Contains("AND   f.collection_time >= $2", raw, StringComparison.Ordinal);
        Assert.Contains("AND   f.collection_time <= $1", raw, StringComparison.Ordinal);

        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsHourlyView, out var hourly));
        var rollup = DataWindowFloor.FloorSql([hourly], DataWindowFloor.Scope.ServerId);
        Assert.Contains("AND   f.bucket >= $2", rollup, StringComparison.Ordinal);
        Assert.Contains("AND   f.bucket < $1", rollup, StringComparison.Ordinal);
        Assert.Contains("AND   s.server_id = $4", rollup, StringComparison.Ordinal);
    }

    /// <summary>
    /// A collector table the schedule's retention governs (waiting_tasks and query_snapshots keep 7 days) takes its
    /// edge from the purge cutoff, so the probe reads nothing older than the window: no row of the table is read
    /// without the window's lower bound, and the first collection (the registry's created_date) is the other bound.
    /// A quiet start, or a server whose first row came late, cannot raise a notice the store does not owe.
    /// </summary>
    [Theory]
    [InlineData("waiting_tasks", 7)]
    [InlineData("query_snapshots", 7)]
    public void AScheduleGovernedTable_ReadsItsEdgeFromTheScheduleNotFromAWalk(string table, int defaultDays)
    {
        var source = DataWindowFloor.Source.ForCollectorTable(table);
        Assert.Equal(defaultDays, source.RetentionDefaultDays);

        var sql = DataWindowFloor.FloorSql([source], DataWindowFloor.Scope.Fleet);

        Assert.Contains("GREATEST($3 - make_interval(days => COALESCE((SELECT o.retention_days FROM config.config_collector_schedules AS o", sql, StringComparison.Ordinal);
        Assert.Contains($"lower(o.collector_name) = '{table}' AND o.retention_days >= 1 LIMIT 1), {defaultDays})), s.created_date)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain(" AS h", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A source the schedule gives no edge (the raw relations the gated purge owns, the collectors whose purge is
    /// floored at the baseline window, and every rollup) takes its edge from the oldest row the server holds at or
    /// before the window's end, the rule before the probe read coverage. These tables are dense, so the oldest
    /// row is where coverage starts; reading coverage from the first collection alone would give an old server no
    /// notice however long before anything the table keeps the window starts. The walk is unbounded below and
    /// sits in the select list, so it runs only for a server that counts.
    /// </summary>
    [Theory]
    [InlineData("cpu_utilization_stats", "collection_time")]
    [InlineData("file_io_stats", "collection_time")]
    [InlineData("pg_wait_stats", "collection_time")]
    [InlineData("query_stats", "collection_time")]
    [InlineData("procedure_stats", "collection_time")]
    [InlineData("query_store_stats", "collection_time")]
    public void ASourceTheScheduleGivesNoEdge_WalksToTheOldestRowAtOrBeforeTheEnd(string table, string timeColumn)
    {
        Assert.True(DataWindowFloor.Source.TryForCollectorTable(table, out var source));
        Assert.Null(source.RetentionDefaultDays);

        var sql = DataWindowFloor.FloorSql([source], DataWindowFloor.Scope.Fleet);

        var walk = $"COALESCE((SELECT h.{timeColumn} FROM collect.{table} AS h WHERE h.server_id = s.server_id AND h.{timeColumn} <= $1 ORDER BY h.{timeColumn} LIMIT 1), s.created_date)";
        Assert.Contains(walk, sql, StringComparison.Ordinal);
        Assert.DoesNotContain("config.config_collector_schedules", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("make_interval", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);

        /* In the select list, ahead of FROM: the planner runs it on the rows the WHERE keeps, never on a server
           that holds nothing in the window. */
        Assert.True(sql.IndexOf(walk, StringComparison.Ordinal) < sql.IndexOf("FROM collect.servers AS s", StringComparison.Ordinal));
        Assert.True(sql.IndexOf(walk, StringComparison.Ordinal) < sql.IndexOf("WHERE (w.t IS NOT NULL", StringComparison.Ordinal));
    }

    [Fact]
    public void ARollup_WalksToItsOldestBucketBeforeTheEnd_AndTheEndIsExclusive()
    {
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsHourlyView, out var hourly));
        Assert.Null(hourly.RetentionDefaultDays);

        var sql = DataWindowFloor.FloorSql([hourly], DataWindowFloor.Scope.Fleet);

        Assert.Contains(
            $"COALESCE((SELECT h.bucket FROM collect.{TimescaleSupport.QueryStatsHourlyView} AS h WHERE h.server_id = s.server_id AND h.bucket < $1 ORDER BY h.bucket LIMIT 1), s.created_date)",
            sql, StringComparison.Ordinal);
    }

    /// <summary>A server counts only with a row, or a logged run of the collector, inside the window, so a server that
    /// stopped before it (its registry row and history stay) cannot hide a newer server's late start on a fleet
    /// panel. A rollup has no collector to log a run, so it counts by its rows alone.</summary>
    [Fact]
    public void AServerCounts_OnlyWithARowOrALoggedRunInTheWindow()
    {
        var collector = DataWindowFloor.FloorSql([DataWindowFloor.Source.ForCollectorTable("waiting_tasks")], DataWindowFloor.Scope.Fleet);
        Assert.Contains("WHERE (w.t IS NOT NULL", collector, StringComparison.Ordinal);
        Assert.Contains(
            "OR EXISTS (SELECT 1 FROM collect.collection_log AS c WHERE c.server_id = s.server_id AND c.collector_name = 'waiting_tasks' AND c.collection_time >= $2 AND c.collection_time <= $1))",
            collector, StringComparison.Ordinal);

        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsHourlyView, out var hourly));
        var rollup = DataWindowFloor.FloorSql([hourly], DataWindowFloor.Scope.Fleet);
        Assert.Contains("WHERE (w.t IS NOT NULL)", rollup, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_log", rollup, StringComparison.Ordinal);
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

    /// <summary>A relation the panel reads only from an instant up (the successor half of a stitched read) starts both
    /// its window read and its walk there, with the instant a bind parameter numbered after the scope's, never a
    /// literal. A relation read whole keeps the text it had.</summary>
    [Fact]
    public void ALowerBound_StartsTheWindowReadAndTheWalk_AsABindParameter()
    {
        var bound = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsHourlyView, out var legacy));
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsIntervalHourlyView, out var successor, bound));
        Assert.Null(legacy.LowerBoundUtc);
        Assert.Equal(bound, successor.LowerBoundUtc);

        var fleet = DataWindowFloor.FloorSql([legacy, successor], DataWindowFloor.Scope.Fleet);

        Assert.Contains("AND   f.bucket >= $4\n", fleet, StringComparison.Ordinal);
        Assert.Contains(
            $"FROM collect.{TimescaleSupport.QueryStatsIntervalHourlyView} AS h WHERE h.server_id = s.server_id AND h.bucket < $1 AND h.bucket >= $4 ORDER BY h.bucket LIMIT 1",
            fleet, StringComparison.Ordinal);
        Assert.Contains(
            $"FROM collect.{TimescaleSupport.QueryStatsHourlyView} AS h WHERE h.server_id = s.server_id AND h.bucket < $1 ORDER BY h.bucket LIMIT 1",
            fleet, StringComparison.Ordinal);
        Assert.DoesNotContain("2026-09-01", fleet, StringComparison.Ordinal);
        Assert.DoesNotContain("$5", fleet, StringComparison.Ordinal);

        var scoped = DataWindowFloor.FloorSql([legacy, successor], DataWindowFloor.Scope.ServerNames);
        Assert.Contains("AND   f.bucket >= $5\n", scoped, StringComparison.Ordinal);
        Assert.Contains("AND h.bucket >= $5 ORDER BY", scoped, StringComparison.Ordinal);
        Assert.DoesNotContain("$6", scoped, StringComparison.Ordinal);
    }

    [Fact]
    public void SeveralLowerBounds_AreNumberedAfterTheScope_InSourceOrder()
    {
        var bound = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsIntervalHourlyView, out var first, bound));
        Assert.True(DataWindowFloor.Source.TryForRollup(TimescaleSupport.QueryStatsDbIntervalHourlyView, out var second, bound));

        var sql = DataWindowFloor.FloorSql([first, second], DataWindowFloor.Scope.ServerId);

        Assert.Contains("AND   s.server_id = $4", sql, StringComparison.Ordinal);
        Assert.True(sql.IndexOf("f.bucket >= $5", StringComparison.Ordinal) < sql.IndexOf("f.bucket >= $6", StringComparison.Ordinal));
        Assert.Contains("h.bucket >= $5 ORDER BY", sql, StringComparison.Ordinal);
        Assert.Contains("h.bucket >= $6 ORDER BY", sql, StringComparison.Ordinal);
    }

    /// <summary>A stitched route reads the superseded half below the boundary and the successor from it up, so the
    /// probe reads the successor from the boundary up and the superseded half whole, on both tiers, whichever order
    /// the clause lists them in: the successor is the one the registry names, and the boundary is the route's value.</summary>
    [Fact]
    public void AStitchedRoute_ProbesTheSuccessorFromTheBoundaryUp_AndTheSupersededHalfWhole()
    {
        var boundary = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Unspecified);
        var pairs = new[]
        {
            (Legacy: TimescaleSupport.QueryStatsHourlyView, Successor: TimescaleSupport.QueryStatsIntervalHourlyView, Tier: ComposeSourceTier.Hourly),
            (Legacy: TimescaleSupport.QueryStatsDailyView, Successor: TimescaleSupport.QueryStatsIntervalDailyView, Tier: ComposeSourceTier.Daily),
        };

        foreach (var pair in pairs)
        {
            foreach (var order in new[] { new[] { pair.Legacy, pair.Successor }, new[] { pair.Successor, pair.Legacy } })
            {
                var from = "(SELECT bucket FROM collect." + order[0] + " WHERE bucket < TIMESTAMP '2026-09-01 00:00:00.000000' UNION ALL SELECT bucket FROM collect."
                    + order[1] + " WHERE bucket >= TIMESTAMP '2026-09-01 00:00:00.000000') AS f";
                var route = new ComposeRoute(pair.Tier, pair.Legacy, from, boundary);

                var sources = ComposeStoreAvailability.DataStartSources("query_stats", route).ToDictionary(s => s.Relation, StringComparer.Ordinal);

                Assert.Equal(2, sources.Count);
                Assert.Null(sources[pair.Legacy].LowerBoundUtc);
                Assert.Equal(boundary, sources[pair.Successor].LowerBoundUtc);
            }
        }
    }

    [Fact]
    public void AnUnstitchedRollupRoute_ProbesItsRollupWhole()
    {
        var route = new ComposeRoute(
            ComposeSourceTier.Hourly, TimescaleSupport.QueryStatsIntervalHourlyView, "collect." + TimescaleSupport.QueryStatsIntervalHourlyView + " AS f");

        Assert.Null(Assert.Single(ComposeStoreAvailability.DataStartSources("query_stats", route)).LowerBoundUtc);
    }

    /// <summary>The router sets the FROM clause on every rollup route it builds, so a rollup route without one is a
    /// router defect: the probe refuses it rather than guess which relation the panel read.</summary>
    [Fact]
    public void ARollupRouteWithoutAFromClause_IsRefused()
    {
        var route = new ComposeRoute(ComposeSourceTier.Hourly, TimescaleSupport.QueryStatsIntervalHourlyView);

        Assert.Throws<ArgumentException>(() => ComposeStoreAvailability.DataStartSources("query_stats", route));
    }
}
