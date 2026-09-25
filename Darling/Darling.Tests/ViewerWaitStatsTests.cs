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
using Xunit;

using PerformanceMonitor.Ui;

namespace Darling.Tests;

/// <summary>
/// Pins the Wait Stats tab's two ported reads against the Darling store contract (no live Postgres):
/// the distinct-wait-types population read and the batched per-type trend read, both copied from
/// Lite's LocalDataService.WaitStats.cs and run VERBATIM on the <c>v_wait_stats</c> passthrough view.
/// The trend keeps Lite's per-type <c>LAG</c> per-second math and the dynamic <c>IN ($4, $5, ...)</c>
/// list; the population read drops Lite's per-user ignored-wait exclusion clause (no viewer equivalent).
/// </summary>
public sealed class ViewerWaitStatsSqlTests
{
    [Fact]
    public void DistinctWaitTypesSql_RanksEveryTypeByTotalDelta_OverTheWindow()
    {
        Assert.Contains("FROM v_wait_stats", ViewerDataService.DistinctWaitTypesSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", ViewerDataService.DistinctWaitTypesSql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", ViewerDataService.DistinctWaitTypesSql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", ViewerDataService.DistinctWaitTypesSql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY wait_type", ViewerDataService.DistinctWaitTypesSql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_wait_time_ms) DESC", ViewerDataService.DistinctWaitTypesSql, StringComparison.Ordinal);
    }

    [Fact]
    public void DistinctWaitTypesSql_DropsLitesIgnoredWaitExclusionClause()
    {
        /* Lite splices an "AND wait_type NOT IN (...)" ignored-wait exclusion; the viewer has no
           per-user ignore config, so the ported read must carry no NOT IN clause. */
        Assert.DoesNotContain("NOT IN", ViewerDataService.DistinctWaitTypesSql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void WaitTrendsSql_KeepsLitesPerTypeLagPerSecondMath()
    {
        var sql = ViewerDataService.WaitTrendsSql(3);

        Assert.Contains("WITH raw AS", sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_wait_stats", sql, StringComparison.Ordinal);
        /* #3540: the STORED interval first — 0 (the calculator's unknowable marker) mapped to NULL — and the
           per-type LAG window (the truncate-then-diff epoch idiom proven value-identical between DuckDB and
           Postgres) only for pre-V127 rows that never recorded one. */
        ViewerLatchSpinlockSqlTests.AssertStoredIntervalIdiom(sql, "wait_type");
        /* #4234: bucketed — a bucket's rate is its summed rated wait over its summed rated seconds (the
           "rated" CTE), not a per-row division. Neither rate CASE carries an ELSE, so an all-unrated bucket
           sums to NULL/NULL = NULL and the reader drops the row, same as the pre-#4234 per-row read did. */
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN delta_wait_time_ms END AS rated_wait_ms", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN delta_signal_wait_time_ms END AS rated_signal_ms", sql, StringComparison.Ordinal);
        Assert.Contains("CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(rated_wait_ms) AS DOUBLE PRECISION) / SUM(rated_seconds) AS wait_time_ms_per_second", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(rated_signal_ms) AS DOUBLE PRECISION) / SUM(rated_seconds) AS signal_wait_time_ms_per_second", sql, StringComparison.Ordinal);
        /* avg_ms_per_wait is the one metric with a deliberate, EXPLICIT "ELSE 0" — a bucket whose rated
           collections all logged zero waiting tasks would otherwise raise division_by_zero, so the guard is
           spelled out rather than left to NULL propagation like the two rate columns above. */
        Assert.Contains("CASE WHEN SUM(rated_tasks) > 0 THEN CAST(SUM(rated_wait_ms) AS DOUBLE PRECISION) / SUM(rated_tasks) ELSE 0 END AS avg_ms_per_wait", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY wait_type, 2", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "$4", "IN ($4)")]
    [InlineData(3, "$6", "IN ($4, $5, $6)")]
    public void WaitTrendsSql_BuildsDynamicInListFromFour(int count, string highestParam, string inClause)
    {
        var sql = ViewerDataService.WaitTrendsSql(count);
        /* server_id/start/end take $1/$2/$3; the wait-type IN list starts at $4 (Lite's numbering). */
        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains(inClause, sql, StringComparison.Ordinal);
        Assert.Contains(highestParam, sql, StringComparison.Ordinal);
    }

    [Fact]
    public void WaitStatsSql_PgDialect_PositionalParams_NoBareNow_NoNLiterals()
    {
        foreach (var sql in new[] { ViewerDataService.DistinctWaitTypesSql, ViewerDataService.WaitTrendsSql(2) })
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
    public void WaitStatsSql_ReadsColumnsThatExistInTheGeneratedWaitTable()
    {
        Assert.Equal("wait_stats", WaitStatsCollector.Instance.TargetTable);

        var ddl = PgSchemaGenerator.CreateTable(WaitStatsCollector.Instance);
        foreach (var column in new[] { "collection_time", "wait_type", "delta_wait_time_ms", "delta_signal_wait_time_ms", "delta_waiting_tasks" })
        {
            Assert.Contains(column, ddl, StringComparison.Ordinal);
        }
    }
}

/// <summary>
/// Pins the picker logic copied from Lite's ServerTab.Pickers.cs: the default-selection composition
/// (poison + usual-suspect + PAGELATCH_ prefix, presence-gated), the fill cap, and the
/// clear-only-the-filtered-visible-set semantics. <see cref="ViewerServerTab.GetDefaultWaitTypes"/>
/// is the real (internal) method; the filter/clear pin is a behavior spec mirroring the inline
/// (WPF-bound, not headlessly invokable) <c>ApplyWaitTypeFilter</c> + <c>WaitTypeClearAll_Click</c>.
/// </summary>
public sealed class ViewerWaitPickerLogicTests
{
    private static readonly string[] Fixed =
    {
        "THREADPOOL", "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE",   // poison
        "SOS_SCHEDULER_YIELD", "CXPACKET", "CXCONSUMER", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX", "WRITELOG", // usual suspects
    };

    [Fact]
    public void GetDefaultWaitTypes_IncludesPoisonUsualSuspectsAndPagelatchPrefix_WhenPresent()
    {
        var available = new List<string>(Fixed) { "PAGELATCH_UP", "PAGELATCH_EX", "SLEEP_TASK", "BROKER_RECEIVE_WAITFOR" };

        var defaults = ViewerServerTab.GetDefaultWaitTypes(available);

        foreach (var w in Fixed)
        {
            Assert.Contains(w, defaults);
        }
        Assert.Contains("PAGELATCH_UP", defaults);
        Assert.Contains("PAGELATCH_EX", defaults);
    }

    [Fact]
    public void GetDefaultWaitTypes_DoesNotAddAbsentPoisonOrUsualSuspects()
    {
        /* Only THREADPOOL + CXPACKET are actually collected; the rest of the fixed set must not appear. */
        var available = new List<string> { "THREADPOOL", "CXPACKET", "SLEEP_TASK" };

        var defaults = ViewerServerTab.GetDefaultWaitTypes(available);

        Assert.Contains("THREADPOOL", defaults);
        Assert.Contains("CXPACKET", defaults);
        Assert.DoesNotContain("RESOURCE_SEMAPHORE", defaults);
        Assert.DoesNotContain("WRITELOG", defaults);
        /* SLEEP_TASK rides in only as fill (there is room), which is fine — the point is absent
           fixed types stay out. */
    }

    [Fact]
    public void GetDefaultWaitTypes_FillAddsAtMostTenNonFixedTypes()
    {
        /* 9 fixed present, no PAGELATCH_ prefix, plus 30 "other" types — the fill loop stops at 10
           added, so the result is 9 + 10 = 19 and never sweeps in all 30 others. */
        var available = new List<string>(Fixed);
        available.AddRange(Enumerable.Range(0, 30).Select(i => $"OTHER_{i:D2}"));

        var defaults = ViewerServerTab.GetDefaultWaitTypes(available);

        Assert.Equal(19, defaults.Count);
        var others = defaults.Where(d => d.StartsWith("OTHER_", StringComparison.Ordinal)).ToList();
        Assert.Equal(10, others.Count);
    }

    [Fact]
    public void GetDefaultWaitTypes_FillRespectsThirtyCap_WhenPrefixSetAlreadyLarge()
    {
        /* 3 poison + 6 usual + 20 PAGELATCH_ = 29 before fill; the fill's ">= 30" guard lets exactly
           one more in, then stops — so a huge PAGELATCH_ family can't run the default set away. */
        var available = new List<string>(Fixed);
        available.AddRange(Enumerable.Range(0, 20).Select(i => $"PAGELATCH_{i:D2}"));
        available.AddRange(Enumerable.Range(0, 5).Select(i => $"OTHER_{i:D2}"));

        var defaults = ViewerServerTab.GetDefaultWaitTypes(available);

        Assert.Equal(30, defaults.Count);
        Assert.Equal(1, defaults.Count(d => d.StartsWith("OTHER_", StringComparison.Ordinal)));
    }

    [Fact]
    public void ClearAll_ClearsOnlyTheFilteredVisibleSet_LeavingHiddenSelectionsIntact()
    {
        /* Behavior spec mirroring ApplyWaitTypeFilter's case-insensitive Contains filter +
           WaitTypeClearAll_Click clearing only the currently-visible (filtered) items. */
        var items = new List<SelectableItem>
        {
            new() { DisplayName = "CXPACKET", IsSelected = true },
            new() { DisplayName = "PAGEIOLATCH_SH", IsSelected = true },
            new() { DisplayName = "PAGEIOLATCH_EX", IsSelected = true },
            new() { DisplayName = "WRITELOG", IsSelected = true },
        };

        const string search = "pageio";
        var visible = items.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
        foreach (var item in visible)
        {
            item.IsSelected = false;
        }

        /* Only the two PAGEIOLATCH_* (the filtered set) are cleared; CXPACKET and WRITELOG persist. */
        Assert.True(items.Single(i => i.DisplayName == "CXPACKET").IsSelected);
        Assert.True(items.Single(i => i.DisplayName == "WRITELOG").IsSelected);
        Assert.False(items.Single(i => i.DisplayName == "PAGEIOLATCH_SH").IsSelected);
        Assert.False(items.Single(i => i.DisplayName == "PAGEIOLATCH_EX").IsSelected);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the two Wait Stats reads: the distinct-types ranking
/// and the batched per-type trend (per-second wait rate + avg ms per wait computed in SQL via the
/// per-type LAG window). Shares the serialized "live-postgres" collection; uses a negative sentinel
/// server_id and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerWaitStatsLivePostgresTests
{
    private const int WaitServerId = -949494;
    private const string WaitServerName = "viewer-waitstats-e2e";

    [Fact]
    public async Task DistinctWaitTypes_RanksByTotalDeltaDescending_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live distinct-wait-types test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteWaitRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* CXPACKET total delta = 100 + 600 = 700; WRITELOG total = 50 → CXPACKET ranks first. */
            await InsertWaitRowAsync(connection, 1, t1, "CXPACKET", deltaWait: 100, deltaSignal: 10, deltaTasks: 5);
            await InsertWaitRowAsync(connection, 2, t2, "CXPACKET", deltaWait: 600, deltaSignal: 60, deltaTasks: 3);
            await InsertWaitRowAsync(connection, 1, t1, "WRITELOG", deltaWait: 50, deltaSignal: 5, deltaTasks: 2);

            var types = await viewer.GetDistinctWaitTypesAsync(WaitServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.Equal(new[] { "CXPACKET", "WRITELOG" }, types);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWaitRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task WaitTrends_ComputePerSecondAndAvgPerWait_ByType_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live wait-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteWaitRowsAsync(connection, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-20));
            var t2 = t1.AddMinutes(5);   // 300 seconds later
            var t3 = t2.AddMinutes(5);
            var t4 = t3.AddMinutes(5);

            /* CXPACKET across four collections; WRITELOG is a second type that must NOT appear when
               only CXPACKET is requested (the IN filter). t1/t2 are pre-V127 rows (NULL interval); t3
               stores interval 0 — the calculator's "no delta knowable" marker, a restart (#3540); t4
               stores a measured 120 s. */
            await InsertWaitRowAsync(connection, 1, t1, "CXPACKET", deltaWait: 100, deltaSignal: 10, deltaTasks: 5);
            await InsertWaitRowAsync(connection, 2, t2, "CXPACKET", deltaWait: 600, deltaSignal: 60, deltaTasks: 3);
            await InsertWaitRowAsync(connection, 2, t2, "WRITELOG", deltaWait: 900, deltaSignal: 90, deltaTasks: 9);
            await InsertWaitRowAsync(connection, 3, t3, "CXPACKET", deltaWait: 0, deltaSignal: 0, deltaTasks: 0, sampleIntervalSeconds: 0);
            await InsertWaitRowAsync(connection, 4, t4, "CXPACKET", deltaWait: 1200, deltaSignal: 120, deltaTasks: 4, sampleIntervalSeconds: 120);

            var trends = await viewer.GetWaitStatsTrendsByTypesAsync(
                WaitServerId, new List<string> { "CXPACKET" }, t1.AddMinutes(-1), t4.AddMinutes(1));

            Assert.True(trends.ContainsKey("CXPACKET"));
            Assert.False(trends.ContainsKey("WRITELOG"));   // the IN filter excluded it

            var cx = trends["CXPACKET"];

            /* Two points, not four: t1 has no prior collection and no stored interval (it used to plot as
               0.00 ms/sec — the fabricated first point), and t3 is the unknowable marker, which must be
               ABSENT rather than a confident 0.00 at exactly the moment (a restart) nothing is knowable. */
            Assert.Equal(new[] { t2.Ticks, t4.Ticks }, cx.Select(p => p.CollectionTime.Ticks).ToArray());

            /* t2 (pre-V127): 600 ms over the LAG's 300 s = 2.0 ms/sec; signal 60/300 = 0.2; avg 600/3 = 200. */
            Assert.Equal(2.0, cx[0].WaitTimeMsPerSecond, precision: 3);
            Assert.Equal(0.2, cx[0].SignalWaitTimeMsPerSecond, precision: 3);
            Assert.Equal(200.0, cx[0].AvgMsPerWait, precision: 3);

            /* t4: the STORED 120 s wins over the LAG's 300 s — 1200/120 = 10.0 ms/sec, signal 1.0, avg 300. */
            Assert.Equal(10.0, cx[1].WaitTimeMsPerSecond, precision: 3);
            Assert.Equal(1.0, cx[1].SignalWaitTimeMsPerSecond, precision: 3);
            Assert.Equal(300.0, cx[1].AvgMsPerWait, precision: 3);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteWaitRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertWaitRowAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTimeUtc,
        string waitType, long deltaWait, long deltaSignal, long deltaTasks, int? sampleIntervalSeconds = null)
    {
        /* sample_interval_seconds NULL by default — a pre-V127 row; pass 0 (unknowable) or a measured value
           for the V127 contract (#3540). */
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(collectionId);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(WaitServerId);
        command.Parameters.AddWithValue(WaitServerName);
        command.Parameters.AddWithValue(waitType);
        command.Parameters.AddWithValue(deltaTasks);
        command.Parameters.AddWithValue(deltaWait);
        command.Parameters.AddWithValue(deltaSignal);
        command.Parameters.Add(new NpgsqlParameter { Value = (object?)sampleIntervalSeconds ?? DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteWaitRowsAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {WaitServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
