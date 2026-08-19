/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Perfmon tab's two ported reads against the Darling store contract (no live Postgres): the
/// distinct-counter population read and the batched per-counter trend read, both copied from Lite's
/// LocalDataService.Perfmon.cs and run on the <c>v_perfmon_stats</c> passthrough view. The one
/// deviation from Lite's SQL text is the <c>CAST(SUM(...) AS bigint)</c> wrap, because Postgres'
/// <c>SUM(bigint)</c> returns numeric while the typed <c>GetInt64</c> reader (and DuckDB) want an
/// integral type.
/// </summary>
public sealed class ViewerPerfmonSqlTests
{
    [Fact]
    public void DistinctPerfmonCountersSql_ListsEveryCounter_OverTheWindow()
    {
        Assert.Contains("SELECT DISTINCT counter_name", ViewerDataService.DistinctPerfmonCountersSql, StringComparison.Ordinal);
        Assert.Contains("FROM v_perfmon_stats", ViewerDataService.DistinctPerfmonCountersSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.DistinctPerfmonCountersSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", ViewerDataService.DistinctPerfmonCountersSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", ViewerDataService.DistinctPerfmonCountersSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY counter_name", ViewerDataService.DistinctPerfmonCountersSql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonTrendsSql_SumsValueAndDelta_ButTakesTheIntervalAsMax()
    {
        var sql = ViewerDataService.PerfmonTrendsSql(3);

        Assert.Contains("FROM v_perfmon_stats", sql, StringComparison.Ordinal);
        /* Every aggregate is CAST to bigint for the typed GetInt64 reader (Postgres SUM(bigint)
           returns numeric). */
        Assert.Contains("CAST(SUM(cntr_value) AS bigint)", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(delta_cntr_value) AS bigint)", sql, StringComparison.Ordinal);
        /* The interval is NOT additive across a counter's instance rows — it is one measured sweep gap
           repeated per instance, so SUM would multiply the denominator by the instance count (12-17 for
           Transactions/sec on the fleet). Same pin as the MCP read carries, #2234. */
        Assert.Contains("CAST(MAX(sample_interval_seconds) AS bigint)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("SUM(sample_interval_seconds)", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY counter_name, collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY counter_name, collection_time", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "$4", "IN ($4)")]
    [InlineData(3, "$6", "IN ($4, $5, $6)")]
    public void PerfmonTrendsSql_BuildsDynamicInListFromFour(int count, string highestParam, string inClause)
    {
        var sql = ViewerDataService.PerfmonTrendsSql(count);
        /* server_id/start/end take $1/$2/$3; the counter-name IN list starts at $4 (Lite's numbering). */
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains(inClause, sql, StringComparison.Ordinal);
        Assert.Contains(highestParam, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PerfmonSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        foreach (var sql in new[] { ViewerDataService.DistinctPerfmonCountersSql, ViewerDataService.PerfmonTrendsSql(2) })
        {
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
            Assert.Contains("$2", sql, StringComparison.Ordinal);
            Assert.Contains("$3", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PerfmonSql_ReadsColumnsThatExistInTheGeneratedPerfmonTable()
    {
        Assert.Equal("perfmon_stats", PerfmonStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(PerfmonStatsCollector.Instance);
        foreach (var column in new[] { "collection_time", "counter_name", "cntr_value", "delta_cntr_value" })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Pins the Running Jobs tab's two ported reads (no live Postgres): the latest-snapshot grid read
/// (copied from Lite's GetRunningJobsAsync, run on <c>v_running_jobs</c>) and the msdb-warning
/// derivation read that stands in for Lite's live <c>_hasMsdbAccess</c> probe — the running_jobs
/// collector's most recent collection_log status.
/// </summary>
public sealed class ViewerRunningJobsSqlTests
{
    [Fact]
    public void RunningJobsSql_ReturnsLatestSnapshotOnly_OrderedByCurrentDurationDesc()
    {
        Assert.Contains("FROM v_running_jobs", ViewerDataService.RunningJobsSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.RunningJobsSql, StringComparison.Ordinal);
        /* Latest snapshot: collection_time = MAX(collection_time) self-subquery for the same server. */
        Assert.Contains("collection_time = (", ViewerDataService.RunningJobsSql, StringComparison.Ordinal);
        Assert.Contains("SELECT MAX(collection_time)", ViewerDataService.RunningJobsSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY current_duration_seconds DESC", ViewerDataService.RunningJobsSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RunningJobsCollectorStatusSql_ReadsLatestRunningJobsStatus_FromCollectionLog()
    {
        Assert.Contains("FROM collection_log", ViewerDataService.RunningJobsCollectorStatusSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.RunningJobsCollectorStatusSql, StringComparison.Ordinal);
        Assert.Contains("collector_name = 'running_jobs'", ViewerDataService.RunningJobsCollectorStatusSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", ViewerDataService.RunningJobsCollectorStatusSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", ViewerDataService.RunningJobsCollectorStatusSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RunningJobsSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        foreach (var sql in new[] { ViewerDataService.RunningJobsSql, ViewerDataService.RunningJobsCollectorStatusSql })
        {
            Assert.DoesNotContain("now(", sql.ToLowerInvariant());
            Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
            Assert.Contains("$1", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void RunningJobsSql_ReadsColumnsThatExistInTheGeneratedRunningJobsTable()
    {
        Assert.Equal("running_jobs", RunningJobsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(RunningJobsCollector.Instance);
        foreach (var column in new[]
        {
            "collection_time", "job_name", "job_id", "job_enabled", "start_time",
            "current_duration_seconds", "avg_duration_seconds", "p95_duration_seconds",
            "successful_run_count", "is_running_long", "percent_of_average",
        })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("PERMISSIONS", true)]
    [InlineData("permissions", true)]   // case-insensitive
    [InlineData("Permissions", true)]
    [InlineData("ERROR", false)]        // a transient ERROR is NOT a permission problem — banner must stay hidden
    [InlineData("error", false)]
    [InlineData("SUCCESS", false)]
    [InlineData("SKIPPED", false)]
    [InlineData("", false)]
    [InlineData(null, false)]           // collector never ran
    public void MsdbBanner_ShowsOnlyForPermissionsStatus_NotTransientError(string? status, bool expectVisible)
    {
        /* Item 6: the banner is restricted to the running_jobs collector's PERMISSIONS outcome (the store
           stand-in for Lite's real _hasMsdbAccess probe). A generic ERROR (timeout/blip) must no longer
           raise the misleading "grant msdb access" guidance. Calls the real production decision. */
        Assert.Equal(expectVisible, ViewerServerTab.ShouldShowMsdbBanner(status));
    }

    [Theory]
    [InlineData(30, "30s")]
    [InlineData(90, "1m 30s")]
    [InlineData(3720, "1h 2m")]
    public void RunningJobRow_FormatsDuration_LikeLite(long seconds, string expected)
    {
        var row = new RunningJobRow { CurrentDurationSeconds = seconds };
        Assert.Equal(expected, row.CurrentDurationFormatted);
    }

    [Fact]
    public void RunningJobRow_PercentOfAverage_NullShowsNa()
    {
        Assert.Equal("N/A", new RunningJobRow { PercentOfAverage = null }.PercentOfAverageFormatted);
        Assert.Equal("150%", new RunningJobRow { PercentOfAverage = 150m }.PercentOfAverageFormatted);
    }
}

/// <summary>
/// Pins the perfmon picker logic copied from Lite's ServerTab.Pickers.cs. The picker methods are
/// WPF-bound (they touch the ComboBox / search box / ListBox) so they aren't headlessly invokable;
/// these are behavior specs mirroring the load-bearing rules — the General-Throughput default set, the
/// 12-counter cap on pack fills / "Select All", the "All Counters" → General-Throughput mapping, and
/// the clear-only-the-filtered-visible-set semantics — pinned against the SHARED
/// <see cref="PerfmonPacks"/> parity data the real picker reads.
/// </summary>
public sealed class ViewerPerfmonPickerLogicTests
{
    private static readonly HashSet<string> GeneralThroughput =
        new(PerfmonPacks.Packs["General Throughput"], StringComparer.OrdinalIgnoreCase);

    [Fact]
    public void GeneralThroughputPack_IsTheDefaultCounterSet_AndNonEmpty()
    {
        Assert.NotEmpty(GeneralThroughput);
        /* The curated default counters Lite seeds the picker with. */
        Assert.Contains("Batch Requests/sec", GeneralThroughput);
        Assert.Contains("SQL Compilations/sec", GeneralThroughput);
    }

    [Fact]
    public void DefaultSelection_SelectsGeneralThroughputCounters_WhenNoPriorSelection()
    {
        /* Mirrors PopulatePerfmonPicker: with no previously-selected counters, an item is checked iff
           it is in the General Throughput default set. */
        var available = new List<string> { "Batch Requests/sec", "SQL Compilations/sec", "Page reads/sec", "Lock Waits/sec" };
        var previouslySelected = new HashSet<string>();

        var selected = available
            .Where(c => previouslySelected.Contains(c) || (previouslySelected.Count == 0 && GeneralThroughput.Contains(c)))
            .ToList();

        Assert.Contains("Batch Requests/sec", selected);
        Assert.Contains("SQL Compilations/sec", selected);
        Assert.DoesNotContain("Page reads/sec", selected);   // not in General Throughput
    }

    [Fact]
    public void PackFill_SelectsAtMostTwelveMatchingCounters()
    {
        /* Mirrors PerfmonPack_SelectionChanged's 12-cap. "I/O Pressure" ships 15 counters — a full-fill
           against an available list that contains them all must stop at 12. */
        var packCounters = PerfmonPacks.Packs["I/O Pressure"];
        Assert.True(packCounters.Length > 12, "the I/O Pressure pack should exceed the 12-counter cap to exercise it");

        var available = packCounters.ToList();   // every pack counter is collected
        var packSet = new HashSet<string>(packCounters, StringComparer.OrdinalIgnoreCase);

        int count = 0;
        var selected = new List<string>();
        foreach (var item in available)
        {
            if (count >= 12) break;
            if (packSet.Contains(item)) { selected.Add(item); count++; }
        }

        Assert.Equal(12, selected.Count);
    }

    [Fact]
    public void AllCounters_SelectsTheGeneralThroughputDefaults()
    {
        /* Mirrors the AllCounters branch: "All Counters" checks exactly the General Throughput defaults
           present in the available list (not literally every counter). */
        var available = new List<string> { "Batch Requests/sec", "Page reads/sec", "SQL Compilations/sec", "Lock Waits/sec" };

        var selected = available.Where(c => GeneralThroughput.Contains(c)).ToList();

        Assert.Equal(new[] { "Batch Requests/sec", "SQL Compilations/sec" }, selected);
    }

    [Fact]
    public void ClearAll_ClearsOnlyTheFilteredVisibleSet_LeavingHiddenSelectionsIntact()
    {
        /* Behavior spec mirroring ApplyPerfmonFilter's case-insensitive Contains filter +
           PerfmonClearAll_Click clearing only the currently-visible (filtered) items. */
        var items = new List<SelectableItem>
        {
            new() { DisplayName = "Batch Requests/sec", IsSelected = true },
            new() { DisplayName = "Page reads/sec", IsSelected = true },
            new() { DisplayName = "Page writes/sec", IsSelected = true },
            new() { DisplayName = "Lock Waits/sec", IsSelected = true },
        };

        const string search = "page";
        var visible = items.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
        foreach (var item in visible)
        {
            item.IsSelected = false;
        }

        /* Only the two "Page*" (the filtered set) are cleared; the others persist. */
        Assert.True(items.Single(i => i.DisplayName == "Batch Requests/sec").IsSelected);
        Assert.True(items.Single(i => i.DisplayName == "Lock Waits/sec").IsSelected);
        Assert.False(items.Single(i => i.DisplayName == "Page reads/sec").IsSelected);
        Assert.False(items.Single(i => i.DisplayName == "Page writes/sec").IsSelected);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the Perfmon + Running Jobs reads. Shares the serialized
/// "live-postgres" collection; uses negative sentinel server_ids and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerPerfmonRunningJobsLivePostgresTests
{
    private const int PerfmonServerId = -949501;
    private const int JobsServerId = -949502;
    private const string ServerName = "viewer-w1h-e2e";

    [Fact]
    public async Task Perfmon_DistinctCounters_AndBatchedTrends_SumAcrossInstances_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live perfmon test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeletePerfmonRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* AAA_Counter at t1 across two instances (delta 10 + 5 = 15, cntr 100 + 50 = 150) and once
               at t2 (delta 20). ZZZ_Counter must NOT appear when only AAA is requested (the IN filter). */
            await InsertPerfmonRowAsync(connection, 1, t1, "AAA_Counter", "", cntr: 100, delta: 10);
            await InsertPerfmonRowAsync(connection, 2, t1, "AAA_Counter", "db2", cntr: 50, delta: 5);
            await InsertPerfmonRowAsync(connection, 3, t2, "AAA_Counter", "", cntr: 200, delta: 20);
            await InsertPerfmonRowAsync(connection, 4, t2, "ZZZ_Counter", "", cntr: 999, delta: 99);

            var counters = await viewer.GetDistinctPerfmonCountersAsync(PerfmonServerId, t1.AddMinutes(-1), t2.AddMinutes(1));
            Assert.Equal(new[] { "AAA_Counter", "ZZZ_Counter" }, counters);

            var trends = await viewer.GetPerfmonTrendsByCountersAsync(
                PerfmonServerId, new List<string> { "AAA_Counter" }, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.True(trends.ContainsKey("AAA_Counter"));
            Assert.False(trends.ContainsKey("ZZZ_Counter"));   // the IN filter excluded it

            var aaa = trends["AAA_Counter"];
            Assert.Equal(2, aaa.Count);

            /* t1: summed across the two instances → delta 15, value 150. */
            Assert.Equal(t1.Ticks, aaa[0].CollectionTime.Ticks);
            Assert.Equal(15, aaa[0].DeltaValue);
            Assert.Equal(150, aaa[0].Value);

            /* t2: single instance → delta 20, value 200. */
            Assert.Equal(t2.Ticks, aaa[1].CollectionTime.Ticks);
            Assert.Equal(20, aaa[1].DeltaValue);
            Assert.Equal(200, aaa[1].Value);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeletePerfmonRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task RunningJobs_ReturnsLatestSnapshotOnly_OrderedByDuration_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live running-jobs test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteJobRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-20));
            var latest = older.AddMinutes(10);

            /* An older snapshot that must NOT appear (only the latest collection_time is returned). */
            await InsertJobRowAsync(connection, older, "OldSnapshotJob", currentDuration: 999, isRunningLong: false, percentOfAverage: 50m);
            /* Latest snapshot: two jobs — the read orders by current duration descending. */
            await InsertJobRowAsync(connection, latest, "JobA", currentDuration: 300, isRunningLong: false, percentOfAverage: 120m);
            await InsertJobRowAsync(connection, latest, "JobB", currentDuration: 600, isRunningLong: true, percentOfAverage: 250m);

            var jobs = await viewer.GetRunningJobsAsync(JobsServerId);

            Assert.Equal(2, jobs.Count);
            Assert.DoesNotContain(jobs, j => j.JobName == "OldSnapshotJob");
            Assert.Equal("JobB", jobs[0].JobName);   // 600s first
            Assert.Equal("JobA", jobs[1].JobName);   // 300s second
            Assert.True(jobs[0].IsRunningLong);
            Assert.Equal(250m, jobs[0].PercentOfAverage);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteJobRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task RunningJobsCollectorStatus_ReturnsLatestStatus_ForMsdbBanner_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live collector-status test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteJobLogRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* Latest running_jobs run wins: an earlier SUCCESS then a newer PERMISSIONS returns
               PERMISSIONS (the banner shows). A stray SUCCESS for a DIFFERENT collector at t2 must not
               leak in (collector_name filter). */
            await InsertCollectionLogAsync(connection, 1, "running_jobs", t1, "SUCCESS");
            await InsertCollectionLogAsync(connection, 2, "running_jobs", t2, "PERMISSIONS");
            await InsertCollectionLogAsync(connection, 3, "wait_stats", t2.AddMinutes(1), "SUCCESS");

            var status = await viewer.GetLatestRunningJobsCollectorStatusAsync(JobsServerId);

            Assert.Equal("PERMISSIONS", status);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteJobLogRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertPerfmonRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc,
        string counterName, string instanceName, long cntr, long delta)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO perfmon_stats
    (collection_id, collection_time, server_id, server_name, object_name, counter_name,
     instance_name, cntr_value, delta_cntr_value, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'SQLServer:General', $5, $6, $7, $8, 60)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(PerfmonServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(counterName);
        command.Parameters.AddWithValue(instanceName);
        command.Parameters.AddWithValue(cntr);
        command.Parameters.AddWithValue(delta);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertJobRowAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string jobName,
        long currentDuration, bool isRunningLong, decimal percentOfAverage)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO running_jobs
    (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time,
     current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count,
     is_running_long, percent_of_average)
VALUES ($1, $2, $3, $4, $5, true, $6, $7, 100, 200, 42, $8, $9)", connection);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(JobsServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(jobName);
        command.Parameters.AddWithValue(Guid.NewGuid().ToString());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(currentDuration);
        command.Parameters.AddWithValue(isRunningLong);
        command.Parameters.AddWithValue(percentOfAverage);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, long logId, string collectorName, DateTime collectionTimeUtc, string status)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.AddWithValue(logId);
        command.Parameters.AddWithValue(JobsServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(status);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeletePerfmonRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM perfmon_stats WHERE server_id = {PerfmonServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteJobRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM running_jobs WHERE server_id = {JobsServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteJobLogRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM collection_log WHERE server_id = {JobsServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
