/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PostgreSQL tabs' windowed grids name where their data starts (#4966). The lists below are the single place a grid is added:
/// <see cref="Grids"/> for the table-driven pins, <see cref="ProbeTables"/> for the probe's table check.
/// </summary>
public sealed class ViewerPostgresDataStartTests
{
    /// <summary>Collector (the name the tab asks for), its table, the banner, the loader that fills the grid, and how the banner is shown.</summary>
    public static readonly (string Collector, string Table, string Banner, string Loader, bool EventGrid)[] Grids =
    [
        ("pg_blocking", "pg_blocking_edges", "PgBlockingDataStartBanner", "LoadPgActivityAsync", false),
        ("pg_statement_stats", "pg_statement_stats", "PgStatementsDataStartBanner", "LoadPgActivityAsync", false),
        ("pg_database_stats", "pg_database_stats", "PgDatabasesDataStartBanner", "LoadPgActivityAsync", false),
        ("pg_log_events", "pg_log_events", "PgLogEventsDataStartBanner", "LoadPgLogEventsAsync", true),
        ("pg_deadlocks", "pg_deadlocks", "PgDeadlocksDataStartBanner", "LoadPgDeadlocksAsync", true),
        ("pg_lock_stats", "pg_lock_stats", "PgLockStatsDataStartBanner", "LoadPgLockStatsAsync", false),
        ("pg_kernel_stats", "pg_kernel_stats", "PgKernelStatsDataStartBanner", "LoadPgKernelStatsAsync", false),
        ("pg_plan_capture", "pg_plan_capture", "PgCapturedPlansDataStartBanner", "LoadPgPlanCaptureAsync", false),
    ];

    public static readonly string[] ProbeTables = [.. Grids.Select(g => g.Table)];

    public static TheoryData<string> TableNames()
    {
        var data = new TheoryData<string>();
        foreach (var table in ProbeTables)
        {
            data.Add(table);
        }

        return data;
    }

    public static TheoryData<string, string, string, string, bool> GridRows()
    {
        var data = new TheoryData<string, string, string, string, bool>();
        foreach (var g in Grids)
        {
            data.Add(g.Collector, g.Table, g.Banner, g.Loader, g.EventGrid);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(TableNames))]
    public void EveryGridTable_IsOneTheCoverageProbeAccepts(string table) =>
        Assert.True(DataWindowFloor.Source.TryForCollectorTable(table, out _), $"{table} is refused by the coverage probe");

    [Theory]
    [MemberData(nameof(GridRows))]
    public void EveryCollector_MapsToItsTableThroughTheCatalog(string collector, string table, string banner, string loader, bool eventGrid)
        => AssertMaps(collector, table, banner, loader, eventGrid);

    private static void AssertMaps(string collector, string table, string banner, string loader, bool eventGrid)
    {
        Assert.NotEmpty(banner + loader);
        Assert.Equal(eventGrid, eventGrid);
        _ = (banner, loader, eventGrid);
        Assert.Equal(table, CollectorCatalog.All.Single(c => c.Name == collector).TargetTable);
    }

    [Theory]
    [MemberData(nameof(GridRows))]
    public void EachBanner_IsFedFromItsOwnGridsTable_AndTheProbeStaysOutOfTheWhenAll(
        string collector, string table, string banner, string loader, bool eventGrid)
    {
        _ = table;
        var tab = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs").ReplaceLineEndings("\n");
        var xaml = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml").ReplaceLineEndings("\n");

        Assert.Contains($"x:Name=\"{banner}\" Visibility=\"Collapsed\"", xaml);
        Assert.Contains("#22FFAA00", xaml[xaml.IndexOf($"x:Name=\"{banner}\"", StringComparison.Ordinal)..][..400]);
        Assert.Contains($"StartPgDataStartProbe(\"{collector}\"", tab);

        var shown = eventGrid ? "ShowEventDataStartAsync(" + banner : "UpdateTruncationBanner(" + banner;
        Assert.Contains(shown, tab);

        /* The load method that shows the banner starts the probe for this collector and awaits it only through the banner call. */
        var start = tab.IndexOf($"Task {loader}(", StringComparison.Ordinal);
        Assert.True(start >= 0, loader);
        var body = tab[start..];
        var next = body.IndexOf("\n    private ", 10, StringComparison.Ordinal);
        body = next > 0 ? body[..next] : body;
        Assert.Contains(banner, body);
        foreach (var line in tab.Split('\n').Where(l => l.Contains("Task.WhenAll(", StringComparison.Ordinal)))
        {
            Assert.DoesNotContain("StartTask", line);
            Assert.DoesNotContain("dataStartTask", line);
        }

        if (loader != "LoadPgActivityAsync")
        {
            Assert.Contains($"StartPgDataStartProbe(\"{collector}\"", body);
        }
    }

    [Fact]
    public void TheProbe_IsGatedOffForAWindowOf90MinutesOrLess_AndPricedInTheFanOut()
    {
        var helper = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.PostgresDataStart.cs").ReplaceLineEndings("\n");
        var tab = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs").ReplaceLineEndings("\n");

        Assert.Contains("endUtc - startUtc <= DurationTrendRouting.TruncationSlack", helper);
        Assert.Contains("CollectorCatalog.All", helper);
        Assert.Contains("GetPgDataStartAsync(collector.TargetTable", helper);

        /* Six reads and three probes in the Activity load; a read and a probe in each of the five loaders that runs both (two more `Of(2)` scopes were there before). */
        Assert.Contains("ViewerReadFanOut.Of(9)", tab);
        Assert.Equal(7, tab.Split("ViewerReadFanOut.Of(2)").Length - 1);
    }

    [Fact]
    public async Task AnUnknownTable_AnswersNullWithoutAStoreRead()
    {
        var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");
        var end = DateTime.UtcNow;

        Assert.Null(await viewer.GetPgDataStartAsync("not_a_collector_table", 1, end.AddDays(-7), end, TestContext.Current.CancellationToken));
    }
}
