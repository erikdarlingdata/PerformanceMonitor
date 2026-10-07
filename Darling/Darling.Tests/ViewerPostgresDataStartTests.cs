/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
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
        ("pg_cpu_utilization", "pg_cpu_utilization", "PgCpuDataStartBanner", "LoadPgCpuUtilizationAsync", false),
        ("pg_wait_sampling", "pg_wait_sampling", "PgWaitSamplingDataStartBanner", "LoadPgWaitSamplingAsync", false),
        ("pg_session_states", "pg_session_states", "PgSessionStatesDataStartBanner", "LoadPgVacuumAsync", false),
        ("pg_xmin_horizon", "pg_xmin_horizon", "PgXminDataStartBanner", "LoadPgVacuumAsync", false),
        ("pg_autovacuum_stats", "pg_autovacuum_stats", "PgAutovacuumDataStartBanner", "LoadPgVacuumAsync", false),
        ("pg_wait_stats", "pg_wait_stats", "PgWaitStatsDataStartBanner", "LoadPgWaitsAsync", false),
        ("pg_io_stats", "pg_io_stats", "PgIoStatsDataStartBanner", "LoadPgIoAsync", false),
        ("pg_replication_stats", "pg_replication_stats", "PgReplicationStatsDataStartBanner", "LoadPgReplicationStatsAsync", false),
        ("pg_table_bloat_stats", "pg_table_bloat_stats", "PgTableBloatDataStartBanner", "LoadPgStorageAsync", false),
        ("pg_index_usage_stats", "pg_index_usage_stats", "PgIndexUsageDataStartBanner", "LoadPgStorageAsync", false),
    ];

    /// <summary>Write Stats is the one grid with no probe: its row carries the window's first sample, which is the coverage start.</summary>
    public const string WriteStatsTable = "pg_write_stats";

    public static readonly string[] ProbeTables = [.. Grids.Select(g => g.Table), WriteStatsTable];

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

    public static TheoryData<string, string> CollectorTables()
    {
        var data = new TheoryData<string, string>();
        foreach (var g in Grids)
        {
            data.Add(g.Collector, g.Table);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(CollectorTables))]
    public void EveryCollector_MapsToItsTableThroughTheCatalog(string collector, string table)
        => Assert.Equal(table, CollectorCatalog.All.Single(c => c.Name == collector).TargetTable);

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

        /* The probe starts inside this loader, into a variable of its own, and THAT variable feeds this banner: a loader with several
           banners (Activity, Vacuum, Storage) fails if two banners swap their probe tasks. */
        var task = Regex.Match(body, $@"var (\w+) = [^;]*StartPgDataStartProbe\(""{collector}""").Groups[1].Value;
        Assert.NotEmpty(task);
        Assert.Matches(
            eventGrid
                ? $@"ShowEventDataStartAsync\({banner}, {task},"
                : $@"UpdateTruncationBanner\({banner}, await DataStartOrNullAsync\({task},",
            body);

        /* #5022: the same variable is also watched while the read is awaited, so a read that throws does not leave it unobserved. */
        Assert.Matches($@"AwaitReadWatchingProbeAsync\([^;]*?\b{task},\s*""", body);
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
        Assert.Equal(11, tab.Split("ViewerReadFanOut.Of(2)").Length - 1);
        Assert.Contains("ViewerReadFanOut.Of(8)", tab);
        Assert.Contains("ViewerReadFanOut.Of(4)", tab);
    }

    [Fact]
    public void WriteStats_UsesTheRowsWindowStart_WithNoProbe()
    {
        var tab = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.Postgres.cs").ReplaceLineEndings("\n");
        var xaml = RepoFile.ReadRepoFile("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.xaml").ReplaceLineEndings("\n");
        var start = tab.IndexOf("Task LoadPgWriteStatsAsync(", StringComparison.Ordinal);
        var body = tab[start..];
        body = body[..body.IndexOf("\n    private ", 10, StringComparison.Ordinal)];

        Assert.Contains("x:Name=\"PgWriteStatsDataStartBanner\" Visibility=\"Collapsed\"", xaml);
        Assert.Contains("UpdateTruncationBanner(PgWriteStatsDataStartBanner, row?.WindowStartUtc, startUtc)", body);
        Assert.DoesNotContain("StartPgDataStartProbe", body);
    }

    [Fact]
    public async Task AnUnknownTable_AnswersNullWithoutAStoreRead()
    {
        var viewer = new PerformanceMonitor.Darling.Viewer.ViewerDataService("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=1");
        var end = DateTime.UtcNow;

        Assert.Null(await viewer.GetPgDataStartAsync("not_a_collector_table", 1, end.AddDays(-7), end, TestContext.Current.CancellationToken));
    }
}
