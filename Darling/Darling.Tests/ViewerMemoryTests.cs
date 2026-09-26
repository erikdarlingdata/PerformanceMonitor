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
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

using PerformanceMonitor.Ui;

namespace Darling.Tests;

/// <summary>
/// Pins the Memory-tab reads (W1j viewer copy-parity) against the Darling store contract, no live
/// Postgres. Every read runs against a v_* passthrough view (v_memory_stats / v_memory_grant_stats /
/// v_memory_clerks / v_memory_pressure_events) mirroring Lite. The numeric(18,2) MB columns are CAST to
/// double precision for the typed GetDouble reader; the grant-chart count SUMs are CAST to bigint (a
/// Postgres SUM widens integer→bigint and bigint→numeric, so the CAST keeps GetInt64 valid either way);
/// the Overview summary read is a latest-snapshot (ORDER BY collection_time DESC LIMIT 1, no window); the
/// clerk trend batches its selected types into a dynamic IN list from $4; and the pressure read windows
/// on the payload's own sample_time (its dedicated index), not collection_time.
/// </summary>
public sealed class ViewerMemorySqlTests
{
    [Fact]
    public void LatestMemoryStatsSql_IsLatestSnapshot_CastsMbToDouble_SelectsStateStrings()
    {
        var sql = ViewerDataService.LatestMemoryStatsSql;

        Assert.Contains("FROM v_memory_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);

        /* The snapshot takes no window — only $1 (server_id). */
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);

        /* Every numeric(18,2) MB metric is CAST to double precision for the typed GetDouble reader. */
        foreach (var col in new[]
        {
            "total_physical_memory_mb", "available_physical_memory_mb", "total_page_file_mb",
            "available_page_file_mb", "target_server_memory_mb", "total_server_memory_mb",
            "buffer_pool_mb", "plan_cache_mb",
        })
        {
            Assert.Contains($"CAST({col} AS double precision)", sql, StringComparison.Ordinal);
        }

        /* The two state strings are text — selected raw (GetString), never cast. */
        Assert.Contains("system_memory_state", sql, StringComparison.Ordinal);
        Assert.Contains("sql_memory_model", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CAST(system_memory_state", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryGrantTrendSql_SumsGrantedMb_PerCollection_OverTheWindow()
    {
        var sql = ViewerDataService.MemoryGrantTrendSql;

        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("CAST(SUM(granted_memory_mb) AS double precision)", sql, StringComparison.Ordinal);
        /* per_collection still sums per collection_time before the outer bucket AVERAGES the gauge (#4349). */
        Assert.Contains("GROUP BY collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("AVG(total_granted_mb) AS total_granted_mb", sql, StringComparison.Ordinal);
        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY 1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DistinctMemoryClerkTypesSql_RanksEveryTypeByTotalMemory_OverTheWindow()
    {
        var sql = ViewerDataService.DistinctMemoryClerkTypesSql;

        Assert.Contains("FROM v_memory_clerks", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY clerk_type", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(memory_mb) DESC", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "$4", "clerk_type IN ($4)")]
    [InlineData(2, "$5", "clerk_type IN ($4, $5)")]
    [InlineData(4, "$7", "clerk_type IN ($4, $5, $6, $7)")]
    public void MemoryClerkTrendsSql_BuildsDynamicInListFromFour_CastsMemoryToDouble(int count, string highestParam, string inClause)
    {
        var sql = ViewerDataService.MemoryClerkTrendsSql(count);

        /* server_id/start/end take $1/$2/$3; the clerk-type IN list starts at $4 (Lite's numbering). */
        Assert.Contains(inClause, sql, StringComparison.Ordinal);
        Assert.Contains(highestParam, sql, StringComparison.Ordinal);
        Assert.Contains("FROM v_memory_clerks", sql, StringComparison.Ordinal);
        /* #4234: a gauge — AVG per bucket, not the raw per-collection value. */
        Assert.Contains("CAST(AVG(memory_mb) AS double precision)", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY clerk_type, 2", sql, StringComparison.Ordinal);
    }

    /// <summary>#4234 ruling item 6 (source check): the clerk trend carries a bucket width and projects the
    /// singleton-detection columns, mirroring <c>WaitTrendsSql</c>. Proven once by hand against the pre-#4234
    /// text: it had no <c>date_bin</c> anywhere (a per-collection read has no bucket width), so this assert
    /// fails there.</summary>
    [Fact]
    public void MemoryClerkTrendsSql_CarriesABucketWidth_AndProjectsSingletonDetectionColumns()
    {
        /* 1 clerk type = $4, so the width is the trailing $5. */
        var sql = ViewerDataService.MemoryClerkTrendsSql(1);
        Assert.Contains("date_bin(CAST($5 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);
        Assert.Contains(TrendBucketSql.OriginSql, sql, StringComparison.Ordinal);
        Assert.Contains("MIN(collection_time) AS first_collection_time", sql, StringComparison.Ordinal);
        Assert.Contains("COUNT(*) AS collection_count", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryGrantChartDataSql_GroupsPerPool_CastsMbToDouble_CountsToBigint()
    {
        var sql = ViewerDataService.MemoryGrantChartDataSql;

        Assert.Contains("FROM v_memory_grant_stats", sql, StringComparison.Ordinal);
        /* per_collection still groups per collection_time + pool_id (#4349); the outer bucket then AVERAGES
           the sizing/count gauges per pool over the date_bin grid. */
        Assert.Contains("GROUP BY collection_time, pool_id", sql, StringComparison.Ordinal);
        Assert.Contains("GROUP BY pool_id, 2", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY pool_id, 2", sql, StringComparison.Ordinal);
        Assert.Contains("date_bin(CAST($4 AS integer) * INTERVAL '1 minute'", sql, StringComparison.Ordinal);

        /* pool_id is the integer group key, selected raw (GetInt32). */
        Assert.Contains("pool_id", sql, StringComparison.Ordinal);

        /* The three sizing MB SUMs plus the workspace-memory ceiling (target / max target — item 2) all
           CAST to double precision inside per_collection; the outer bucket AVERAGES them (a gauge). */
        foreach (var col in new[] { "available_memory_mb", "granted_memory_mb", "used_memory_mb", "target_memory_mb", "max_target_memory_mb" })
        {
            Assert.Contains($"CAST(SUM({col}) AS double precision)", sql, StringComparison.Ordinal);
            Assert.Contains($"AVG({col})", sql, StringComparison.Ordinal);
        }

        /* The four activity count SUMs CAST to bigint (integer→bigint / bigint→numeric both land on
           GetInt64). */
        foreach (var col in new[]
        {
            "grantee_count", "waiter_count", "timeout_error_count_delta", "forced_grant_count_delta",
        })
        {
            Assert.Contains($"CAST(SUM({col}) AS bigint)", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void MemoryPressureEventsSql_WindowsOnSampleTime_SelectsIndicators()
    {
        var sql = ViewerDataService.MemoryPressureEventsSql;

        Assert.Contains("FROM v_memory_pressure_events", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        /* Windowed on sample_time (the payload's own clock + its dedicated index), not collection_time for
           the answer itself — #4229 adds a collection_time >= $4 FLOOR beside it (EventWindowFloor), so the
           table is a hypertable partitioned on collection_time and this sample_time window alone gave the
           planner nothing to exclude a chunk on. The floor is a second predicate, not a replacement. */
        Assert.Contains("sample_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("sample_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY sample_time", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);

        foreach (var col in new[]
        {
            "sample_time", "memory_notification", "memory_indicators_process", "memory_indicators_system",
        })
        {
            Assert.Contains(col, sql, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("latest")]
    [InlineData("granttrend")]
    [InlineData("distinctclerks")]
    [InlineData("grantchart")]
    [InlineData("pressure")]
    public void MemoryReads_PgDialect_PositionalParams_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "latest" => ViewerDataService.LatestMemoryStatsSql,
            "granttrend" => ViewerDataService.MemoryGrantTrendSql,
            "distinctclerks" => ViewerDataService.DistinctMemoryClerkTypesSql,
            "grantchart" => ViewerDataService.MemoryGrantChartDataSql,
            _ => ViewerDataService.MemoryPressureEventsSql,
        };

        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void MemoryReads_ReadColumnsThatExistInTheGeneratedTables()
    {
        Assert.Equal("memory_stats", MemoryStatsCollector.Instance.TargetTable);
        Assert.Equal("memory_clerks", MemoryClerksCollector.Instance.TargetTable);
        Assert.Equal("memory_grant_stats", MemoryGrantsCollector.Instance.TargetTable);
        Assert.Equal("memory_pressure_events", MemoryPressureEventsCollector.Instance.TargetTable);

        var statsDdl = PgSchemaGenerator.CreateTable(MemoryStatsCollector.Instance);
        foreach (var col in new[]
        {
            "total_physical_memory_mb", "available_physical_memory_mb", "total_page_file_mb",
            "available_page_file_mb", "system_memory_state", "sql_memory_model",
            "target_server_memory_mb", "total_server_memory_mb", "buffer_pool_mb", "plan_cache_mb",
        })
        {
            Assert.Contains(col, statsDdl, StringComparison.Ordinal);
        }

        var clerksDdl = PgSchemaGenerator.CreateTable(MemoryClerksCollector.Instance);
        foreach (var col in new[] { "clerk_type", "memory_mb" })
        {
            Assert.Contains(col, clerksDdl, StringComparison.Ordinal);
        }

        var grantDdl = PgSchemaGenerator.CreateTable(MemoryGrantsCollector.Instance);
        foreach (var col in new[]
        {
            "pool_id", "available_memory_mb", "granted_memory_mb", "used_memory_mb",
            "grantee_count", "waiter_count", "timeout_error_count_delta", "forced_grant_count_delta",
            /* Item 2 verification: the workspace-memory ceiling columns the viewer now surfaces are collected. */
            "target_memory_mb", "max_target_memory_mb",
        })
        {
            Assert.Contains(col, grantDdl, StringComparison.Ordinal);
        }

        var pressureDdl = PgSchemaGenerator.CreateTable(MemoryPressureEventsCollector.Instance);
        foreach (var col in new[]
        {
            "sample_time", "memory_notification", "memory_indicators_process", "memory_indicators_system",
        })
        {
            Assert.Contains(col, pressureDdl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void MemoryClerksAndPressureViews_AreCreatedByAMigration()
    {
        /* The clerk + pressure reads hit v_memory_clerks / v_memory_pressure_events, which V4/V5 left
           out — a migration must create them (V6 here; the concat is robust to a V6→V7 renumber if the
           plan-capture migration merges first). The other two views (v_memory_stats /
           v_memory_grant_stats) already ship in V4. */
        var allMigrationSql = string.Concat(PgMigrations.Scripts.Select(m => m.Sql));
        Assert.Contains("CREATE OR REPLACE VIEW v_memory_clerks AS SELECT * FROM memory_clerks;", allMigrationSql, StringComparison.Ordinal);
        Assert.Contains("CREATE OR REPLACE VIEW v_memory_pressure_events AS SELECT * FROM memory_pressure_events;", allMigrationSql, StringComparison.Ordinal);
    }
}

/// <summary>
/// Pins the Memory-tab picker + pressure-bucketing logic copied from Lite (ServerTab.Pickers.cs /
/// ServerTab.Charts.cs). The clerk picker's Top-5 / default composition and its clear-only-the-filtered
/// semantics, and the pressure chart's medium/severe classification, are all inline in the WPF-bound
/// control methods (not headlessly invokable), so these are behavior specs mirroring that logic — the
/// same approach ViewerWaitPickerLogicTests uses for the wait picker's filter/clear.
/// </summary>
public sealed class ViewerMemoryPickerLogicTests
{
    [Fact]
    public void MemoryClerkDefault_SelectsTopFive_WhenNoPriorSelection()
    {
        /* PopulateMemoryClerkPicker: with no previously-selected clerks, the default is the first 5 of
           the store-ranked (total-memory-descending) list. */
        var ranked = new List<string>
        {
            "MEMORYCLERK_SQLBUFFERPOOL", "CACHESTORE_SQLCP", "MEMORYCLERK_SQLGENERAL",
            "OBJECTSTORE_LOCK_MANAGER", "MEMORYCLERK_SQLLOGPOOL", "CACHESTORE_OBJCP", "MEMORYCLERK_XTP",
        };

        var previouslySelected = new HashSet<string>();
        var topClerks = previouslySelected.Count == 0 ? new HashSet<string>(ranked.Take(5)) : null;
        var selected = ranked
            .Where(c => previouslySelected.Contains(c) || (topClerks != null && topClerks.Contains(c)))
            .ToList();

        Assert.Equal(ranked.Take(5), selected);
        Assert.DoesNotContain("CACHESTORE_OBJCP", selected);
        Assert.DoesNotContain("MEMORYCLERK_XTP", selected);
    }

    [Fact]
    public void MemoryClerkTop_SelectsExactlyTheFirstFive_ByRankedOrder()
    {
        /* MemoryClerkSelectTop_Click: select the first 5 of the current (ranked) list, deselect the rest. */
        var items = new List<SelectableItem>
        {
            new() { DisplayName = "MEMORYCLERK_SQLBUFFERPOOL", IsSelected = false },
            new() { DisplayName = "CACHESTORE_SQLCP", IsSelected = true },
            new() { DisplayName = "MEMORYCLERK_SQLGENERAL", IsSelected = false },
            new() { DisplayName = "OBJECTSTORE_LOCK_MANAGER", IsSelected = false },
            new() { DisplayName = "MEMORYCLERK_SQLLOGPOOL", IsSelected = false },
            new() { DisplayName = "CACHESTORE_OBJCP", IsSelected = true },
        };

        var topClerks = new HashSet<string>(items.Take(5).Select(x => x.DisplayName));
        foreach (var item in items)
        {
            item.IsSelected = topClerks.Contains(item.DisplayName);
        }

        Assert.Equal(5, items.Count(i => i.IsSelected));
        Assert.True(items.Single(i => i.DisplayName == "MEMORYCLERK_SQLBUFFERPOOL").IsSelected);
        Assert.True(items.Single(i => i.DisplayName == "MEMORYCLERK_SQLLOGPOOL").IsSelected);
        /* The sixth clerk (outside the top 5) is deselected even though it started selected. */
        Assert.False(items.Single(i => i.DisplayName == "CACHESTORE_OBJCP").IsSelected);
    }

    [Fact]
    public void MemoryClerkClearAll_ClearsOnlyTheFilteredVisibleSet_LeavingHiddenSelectionsIntact()
    {
        /* Behavior spec mirroring ApplyMemoryClerkFilter's case-insensitive Contains filter +
           MemoryClerkClearAll_Click clearing only the currently-visible (filtered) items. */
        var items = new List<SelectableItem>
        {
            new() { DisplayName = "MEMORYCLERK_SQLBUFFERPOOL", IsSelected = true },
            new() { DisplayName = "CACHESTORE_SQLCP", IsSelected = true },
            new() { DisplayName = "CACHESTORE_OBJCP", IsSelected = true },
            new() { DisplayName = "MEMORYCLERK_XTP", IsSelected = true },
        };

        const string search = "cachestore";
        var visible = items.Where(i => i.DisplayName.Contains(search, StringComparison.OrdinalIgnoreCase)).ToList();
        foreach (var item in visible)
        {
            item.IsSelected = false;
        }

        /* Only the two CACHESTORE_* (the filtered set) are cleared; the buffer pool + XTP persist. */
        Assert.True(items.Single(i => i.DisplayName == "MEMORYCLERK_SQLBUFFERPOOL").IsSelected);
        Assert.True(items.Single(i => i.DisplayName == "MEMORYCLERK_XTP").IsSelected);
        Assert.False(items.Single(i => i.DisplayName == "CACHESTORE_SQLCP").IsSelected);
        Assert.False(items.Single(i => i.DisplayName == "CACHESTORE_OBJCP").IsSelected);
    }

    [Fact]
    public void MemoryClerkChart_TakesAtMostTwentySelected()
    {
        /* UpdateMemoryClerksChartFromPickerAsync caps the plotted series at 20 selected clerks. */
        var selectedAll = Enumerable.Range(0, 30)
            .Select(i => new SelectableItem { DisplayName = $"CLERK_{i:D2}", IsSelected = true })
            .ToList();

        var plotted = selectedAll.Where(i => i.IsSelected).Take(20).ToList();

        Assert.Equal(20, plotted.Count);
    }

    [Fact]
    public void PressureBucketing_FiltersBelowMedium_ClassifiesMediumSevere_PerHour_ProcessVsSystem()
    {
        /* RenderMemoryPressureEventsChart: keep rows where process>=2 OR system>=2, bucket by the hour of
           sample_time, and per hour count SQL Server (process) medium (==2) vs severe (>=3) and OS
           (system) medium vs severe. */
        var h9 = new DateTime(2026, 6, 1, 9, 0, 0, DateTimeKind.Unspecified);
        var h10 = new DateTime(2026, 6, 1, 10, 0, 0, DateTimeKind.Unspecified);

        var data = new List<MemoryPressureEventRow>
        {
            /* 09:xx — SQL: one medium (2) + one severe (4); OS: one medium (2). */
            new(h9.AddMinutes(5), "RESOURCE_MEMPHYSICAL_LOW", 2, 0),
            new(h9.AddMinutes(20), "RESOURCE_MEMPHYSICAL_LOW", 4, 2),
            /* below-threshold on both axes — must be filtered out entirely. */
            new(h9.AddMinutes(40), "RESOURCE_MEMPHYSICAL_HIGH", 1, 1),
            /* 10:xx — SQL: one severe (3); OS: one severe (5). */
            new(h10.AddMinutes(15), "RESOURCE_MEMPHYSICAL_LOW", 3, 5),
        };

        var pressureRows = data
            .Where(d => d.MemoryIndicatorsProcess >= 2 || d.MemoryIndicatorsSystem >= 2)
            .ToList();
        Assert.Equal(3, pressureRows.Count);   // the 1/1 row dropped

        var buckets = pressureRows
            .GroupBy(d => new DateTime(d.SampleTime.Year, d.SampleTime.Month, d.SampleTime.Day, d.SampleTime.Hour, 0, 0))
            .OrderBy(g => g.Key)
            .Select(g => new
            {
                g.Key,
                SqlMedium = g.Count(d => d.MemoryIndicatorsProcess == 2),
                SqlSevere = g.Count(d => d.MemoryIndicatorsProcess >= 3),
                OsMedium = g.Count(d => d.MemoryIndicatorsSystem == 2),
                OsSevere = g.Count(d => d.MemoryIndicatorsSystem >= 3),
            })
            .ToList();

        Assert.Equal(2, buckets.Count);

        var nine = buckets[0];
        Assert.Equal(h9, nine.Key);
        Assert.Equal(1, nine.SqlMedium);   // the process==2 row
        Assert.Equal(1, nine.SqlSevere);   // the process==4 row
        Assert.Equal(1, nine.OsMedium);    // the system==2 row
        Assert.Equal(0, nine.OsSevere);

        var ten = buckets[1];
        Assert.Equal(h10, ten.Key);
        Assert.Equal(0, ten.SqlMedium);
        Assert.Equal(1, ten.SqlSevere);    // process==3
        Assert.Equal(0, ten.OsMedium);
        Assert.Equal(1, ten.OsSevere);     // system==5
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the six NEW Memory-tab reads: the Overview latest-snapshot
/// (MB-as-double + the two state strings, newest by collection_time), the grant overlay (granted-MB SUM
/// per collection), the clerk ranking (by total memory descending), the batched clerk trend (dynamic IN +
/// per-type grouping + MB-as-double), the grant chart aggregation (per-pool sizing MB-as-double + the
/// load-bearing bigint count SUMs), and the pressure samples (windowed on sample_time). The reused
/// GetMemoryTrendAsync is already covered by ViewerOverviewLanesTests. Shares the serialized
/// "live-postgres" collection; uses negative sentinel server_ids and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerMemoryLivePostgresTests
{
    private const int StatsServerId = -930001;
    private const string StatsServerName = "viewer-memory-stats-e2e";

    private const int GrantTrendServerId = -930002;
    private const string GrantTrendServerName = "viewer-memory-granttrend-e2e";

    private const int ClerkServerId = -930003;
    private const string ClerkServerName = "viewer-memory-clerks-e2e";

    private const int GrantChartServerId = -930004;
    private const string GrantChartServerName = "viewer-memory-grantchart-e2e";

    private const int PressureServerId = -930005;
    private const string PressureServerName = "viewer-memory-pressure-e2e";

    private const int ClerkBudgetServerId = -930006;
    private const int ClerkSingletonServerId = -930007;
    private const int ClerkPickerServerId = -930008;

    /// <summary>#4234 ruling items 2/6: a 7-day window at the real 1-minute memory_clerks cadence used to
    /// return every collection — now capped to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES
    /// (clerk type). Bulk-seeded server-side (generate_series) rather than one round trip per minute.</summary>
    [Fact]
    public async Task MemoryClerkTrend_SevenDayWindow_ReturnsAtMostBudgetTimesSeriesRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-clerk budget-cap test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_clerks", ClerkBudgetServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var end = new DateTime(2026, 3, 10, 0, 0, 0);
            var start = end.AddDays(-7);
            var clerkTypes = new List<string> { "MEMORYCLERK_SQLBUFFERPOOL", "MEMORYCLERK_SQLGENERAL" };
            var baseId = CollectionIdGenerator.Next() * 1_000_000L;
            foreach (var clerkType in clerkTypes)
            {
                await BulkSeedMemoryClerkAsync(connection, TestContext.Current.CancellationToken, baseId, start, end, clerkType);
                baseId += 20_000;
            }

            var trends = await viewer.GetMemoryClerkTrendsByTypesAsync(ClerkBudgetServerId, clerkTypes, start, end);

            var totalRows = trends.Values.Sum(list => list.Count);
            var budget = TrendBudget.Chart.AutoPoints * clerkTypes.Count;
            Assert.True(totalRows > 0 && totalRows <= budget, $"{totalRows} rows over a {clerkTypes.Count}-series budget of {budget}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_clerks", ClerkBudgetServerId, cleanupCt));
        }
    }

    /// <summary>#4234 ruling item 3/6: when the budget covers every collection in the window, the clerk trend
    /// returns the raw points unchanged, stamped at their own raw collection time — proven off the minute
    /// grid (:37 seconds), which the pre-#4234 date_bin-always-floors code would have lost.</summary>
    [Fact]
    public async Task MemoryClerkTrend_BudgetCoversEveryCollection_ReturnsRawTimestampsAndAverageUnchanged()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-clerk singleton-bucket test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_clerks", ClerkSingletonServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var baseTime = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));
            var t1 = baseTime.AddSeconds(37 - baseTime.Second);
            var t2 = t1.AddMinutes(5);
            var t3 = t2.AddMinutes(5);

            await InsertMemoryClerkAtServerAsync(connection, ClerkSingletonServerId, t1, "MEMORYCLERK_SQLBUFFERPOOL", 500.00m);
            await InsertMemoryClerkAtServerAsync(connection, ClerkSingletonServerId, t2, "MEMORYCLERK_SQLBUFFERPOOL", 520.00m);
            await InsertMemoryClerkAtServerAsync(connection, ClerkSingletonServerId, t3, "MEMORYCLERK_SQLBUFFERPOOL", 540.00m);

            var trends = await viewer.GetMemoryClerkTrendsByTypesAsync(
                ClerkSingletonServerId, new List<string> { "MEMORYCLERK_SQLBUFFERPOOL" }, t1.AddMinutes(-1), t3.AddMinutes(1));

            var points = trends["MEMORYCLERK_SQLBUFFERPOOL"];
            Assert.Equal(new[] { t1, t2, t3 }, points.Select(p => p.CollectionTime).ToArray());
            Assert.Equal(new[] { 500.00, 520.00, 540.00 }, points.Select(p => p.MemoryMb).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_clerks", ClerkSingletonServerId, cleanupCt));
        }
    }

    /// <summary>#4234 ruling items 4/6: the clerk picker (<c>GetDistinctMemoryClerkTypesAsync</c>) is cached
    /// per (server, window length) for <see cref="ViewerNameListCache.Ttl"/> — a second call inside 15 minutes
    /// returns the stale list without re-running the DISTINCT (proven by mutating the store between calls and
    /// observing the new type is invisible until the cache is bypassed), and a window whose END has moved past
    /// the TTL misses even though the wall clock hasn't.</summary>
    [Fact]
    public async Task MemoryClerkPicker_SecondCallWithinTtl_ReturnsStaleList_WindowEndPastTtl_Misses()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-clerk picker cache test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_clerks", ClerkPickerServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var now = new DateTime(2026, 3, 10, 12, 0, 0);
            var start = now.AddHours(-1);

            await InsertMemoryClerkAtServerAsync(connection, ClerkPickerServerId, now.AddMinutes(-30), "MEMORYCLERK_SQLBUFFERPOOL", 500.00m);

            var first = await viewer.GetDistinctMemoryClerkTypesAsync(ClerkPickerServerId, start, now, now);
            Assert.Equal(new[] { "MEMORYCLERK_SQLBUFFERPOOL" }, first);

            /* A new clerk type lands in the store, but a call 14 minutes later, same window end, still
               answers from the cache — proof no DISTINCT ran. */
            await InsertMemoryClerkAtServerAsync(connection, ClerkPickerServerId, now.AddMinutes(-20), "MEMORYCLERK_XTP", 999.00m);
            var stillCached = await viewer.GetDistinctMemoryClerkTypesAsync(ClerkPickerServerId, start, now, now.AddMinutes(14));
            Assert.Equal(new[] { "MEMORYCLERK_SQLBUFFERPOOL" }, stillCached);

            /* The window END moves forward past the TTL (a live auto-refresh), even though the wall clock
               above hadn't reached 15 minutes yet on its own — this is a MISS, and the fresh DISTINCT sees
               the new type. */
            var movedEnd = now.AddMinutes(20);
            var afterWindowMoved = await viewer.GetDistinctMemoryClerkTypesAsync(ClerkPickerServerId, start.AddMinutes(20), movedEnd, now.AddMinutes(14));
            Assert.Equal(new[] { "MEMORYCLERK_XTP", "MEMORYCLERK_SQLBUFFERPOOL" }, afterWindowMoved);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_clerks", ClerkPickerServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task LatestMemoryStats_ReturnsNewestSnapshot_MbAsDouble_StateStrings_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-stats test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_stats", StatsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-20));
            var newer = older.AddMinutes(10);

            await InsertMemoryStatsAsync(connection, older,
                totalPhys: 111.00m, availPhys: 22.00m, totalPage: 200.00m, availPage: 40.00m,
                state: "Steady", model: "CONVENTIONAL", target: 90.00m, total: 88.00m, buffer: 70.00m, plan: 5.00m);
            await InsertMemoryStatsAsync(connection, newer,
                totalPhys: 8192.00m, availPhys: 1024.50m, totalPage: 16384.00m, availPage: 4096.25m,
                state: "Available physical memory is high", model: "LOCK_PAGES", target: 6000.00m, total: 5800.75m, buffer: 5000.00m, plan: 250.50m);

            var stats = await viewer.GetLatestMemoryStatsAsync(StatsServerId);

            Assert.NotNull(stats);
            /* Newest snapshot wins (ORDER BY collection_time DESC LIMIT 1). */
            Assert.Equal(newer.Ticks, stats!.CollectionTime.Ticks);
            Assert.Equal(8192.00, stats.TotalPhysicalMemoryMb, precision: 2);
            Assert.Equal(1024.50, stats.AvailablePhysicalMemoryMb, precision: 2);
            Assert.Equal(16384.00, stats.TotalPageFileMb, precision: 2);
            Assert.Equal(4096.25, stats.AvailablePageFileMb, precision: 2);
            Assert.Equal(6000.00, stats.TargetServerMemoryMb, precision: 2);
            Assert.Equal(5800.75, stats.TotalServerMemoryMb, precision: 2);
            Assert.Equal(5000.00, stats.BufferPoolMb, precision: 2);
            Assert.Equal(250.50, stats.PlanCacheMb, precision: 2);
            Assert.Equal("Available physical memory is high", stats.SystemMemoryState);
            Assert.Equal("LOCK_PAGES", stats.SqlMemoryModel);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_stats", StatsServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task MemoryGrantTrend_SumsGrantedMbPerCollection_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live grant-trend test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_grant_stats", GrantTrendServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* t1 has two pools (100 + 25 = 125 granted); t2 has one pool (300). */
            await InsertMemoryGrantAsync(connection, GrantTrendServerId, GrantTrendServerName, t1, poolId: 1,
                availMb: 900m, grantedMb: 100m, usedMb: 80m, grantee: 2, waiter: 0, timeoutDelta: 0, forcedDelta: 0);
            await InsertMemoryGrantAsync(connection, GrantTrendServerId, GrantTrendServerName, t1, poolId: 2,
                availMb: 400m, grantedMb: 25m, usedMb: 20m, grantee: 1, waiter: 0, timeoutDelta: 0, forcedDelta: 0);
            await InsertMemoryGrantAsync(connection, GrantTrendServerId, GrantTrendServerName, t2, poolId: 1,
                availMb: 700m, grantedMb: 300m, usedMb: 250m, grantee: 5, waiter: 1, timeoutDelta: 0, forcedDelta: 0);

            var trend = await viewer.GetMemoryGrantTrendAsync(GrantTrendServerId, t1.AddMinutes(-1), t2.AddMinutes(1));

            Assert.Equal(2, trend.Count);
            Assert.Equal(t1.Ticks, trend[0].CollectionTime.Ticks);
            Assert.Equal(125.0, trend[0].TotalGrantedMb, precision: 2);
            Assert.Equal(t2.Ticks, trend[1].CollectionTime.Ticks);
            Assert.Equal(300.0, trend[1].TotalGrantedMb, precision: 2);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_grant_stats", GrantTrendServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task MemoryClerks_RankByTotalMemory_AndBatchedTrendByType_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live memory-clerks test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_clerks", ClerkServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var t2 = t1.AddMinutes(5);

            /* BUFFERPOOL total = 500 + 520 = 1020; GENERAL total = 30 + 40 = 70 → BUFFERPOOL ranks first.
               THIRD is a type NOT requested in the batched trend, to prove the IN filter excludes it. */
            await InsertMemoryClerkAsync(connection, t1, "MEMORYCLERK_SQLBUFFERPOOL", 500.00m);
            await InsertMemoryClerkAsync(connection, t2, "MEMORYCLERK_SQLBUFFERPOOL", 520.00m);
            await InsertMemoryClerkAsync(connection, t1, "MEMORYCLERK_SQLGENERAL", 30.00m);
            await InsertMemoryClerkAsync(connection, t2, "MEMORYCLERK_SQLGENERAL", 40.00m);
            await InsertMemoryClerkAsync(connection, t2, "MEMORYCLERK_XTP", 999.00m);

            var window = (t1.AddMinutes(-1), t2.AddMinutes(1));

            var ranked = await viewer.GetDistinctMemoryClerkTypesAsync(ClerkServerId, window.Item1, window.Item2);
            /* Ranked by SUM(memory_mb) DESC: BUFFERPOOL's two rows total 1020, XTP's single row is 999,
               GENERAL totals 70 — so BUFFERPOOL leads despite XTP's larger single value (proves the
               ranking sums across collections, not per-row). */
            Assert.Equal(new[] { "MEMORYCLERK_SQLBUFFERPOOL", "MEMORYCLERK_XTP", "MEMORYCLERK_SQLGENERAL" }, ranked);

            var trends = await viewer.GetMemoryClerkTrendsByTypesAsync(
                ClerkServerId, new List<string> { "MEMORYCLERK_SQLBUFFERPOOL", "MEMORYCLERK_SQLGENERAL" },
                window.Item1, window.Item2);

            Assert.True(trends.ContainsKey("MEMORYCLERK_SQLBUFFERPOOL"));
            Assert.True(trends.ContainsKey("MEMORYCLERK_SQLGENERAL"));
            Assert.False(trends.ContainsKey("MEMORYCLERK_XTP"));   // excluded by the IN filter

            var bp = trends["MEMORYCLERK_SQLBUFFERPOOL"];
            Assert.Equal(2, bp.Count);
            Assert.Equal(t1.Ticks, bp[0].CollectionTime.Ticks);
            Assert.Equal(500.00, bp[0].MemoryMb, precision: 2);
            Assert.Equal(520.00, bp[1].MemoryMb, precision: 2);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_clerks", ClerkServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task MemoryClerkTrends_EmptySelection_ReturnsEmpty_WithoutTouchingStore()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live empty-clerk-selection test.");

        await using var viewer = new ViewerDataService(connectionString!);

        var trends = await viewer.GetMemoryClerkTrendsByTypesAsync(
            ClerkServerId, new List<string>(), DateTime.UtcNow.AddHours(-1), DateTime.UtcNow);

        Assert.Empty(trends);
    }

    [Fact]
    public async Task MemoryGrantChart_AggregatesPerPool_MbAsDouble_BigintCounts_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live grant-chart test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_grant_stats", GrantChartServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var t1 = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));

            /* Pool 1 at t1 has TWO resource semaphores that must SUM together; pool 2 is a second group.
               forcedDelta on pool 1 deliberately exceeds int.MaxValue so a GetInt32 reader would throw —
               the load-bearing bigint check for the delta counts. */
            const long bigForced = 5_000_000_000L;
            await InsertMemoryGrantAsync(connection, GrantChartServerId, GrantChartServerName, t1, poolId: 1,
                availMb: 600.00m, grantedMb: 100.00m, usedMb: 90.00m, grantee: 2, waiter: 1, timeoutDelta: 3, forcedDelta: bigForced);
            await InsertMemoryGrantAsync(connection, GrantChartServerId, GrantChartServerName, t1, poolId: 1,
                availMb: 400.00m, grantedMb: 50.00m, usedMb: 40.00m, grantee: 1, waiter: 0, timeoutDelta: 4, forcedDelta: 1);
            await InsertMemoryGrantAsync(connection, GrantChartServerId, GrantChartServerName, t1, poolId: 2,
                availMb: 200.00m, grantedMb: 20.00m, usedMb: 10.00m, grantee: 3, waiter: 2, timeoutDelta: 0, forcedDelta: 0);

            var rows = await viewer.GetMemoryGrantChartDataAsync(GrantChartServerId, t1.AddMinutes(-1), t1.AddMinutes(1));

            /* Two groups: (t1, pool 1) and (t1, pool 2), ordered by collection_time, pool_id. */
            Assert.Equal(2, rows.Count);

            var pool1 = rows[0];
            Assert.Equal(1, pool1.PoolId);
            Assert.Equal(1000.00, pool1.AvailableMemoryMb, precision: 2);   // 600 + 400
            Assert.Equal(150.00, pool1.GrantedMemoryMb, precision: 2);      // 100 + 50
            Assert.Equal(130.00, pool1.UsedMemoryMb, precision: 2);         // 90 + 40
            Assert.Equal(3, pool1.GranteeCount);                            // 2 + 1
            Assert.Equal(1, pool1.WaiterCount);                            // 1 + 0
            Assert.Equal(7L, pool1.TimeoutErrorCountDelta);                 // 3 + 4
            Assert.Equal(bigForced + 1L, pool1.ForcedGrantCountDelta);      // bigint SUM

            var pool2 = rows[1];
            Assert.Equal(2, pool2.PoolId);
            Assert.Equal(20.00, pool2.GrantedMemoryMb, precision: 2);
            Assert.Equal(3, pool2.GranteeCount);
            Assert.Equal(2, pool2.WaiterCount);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_grant_stats", GrantChartServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task MemoryPressureEvents_WindowedOnSampleTime_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live pressure-events test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "memory_pressure_events", PressureServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var collUtc = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-30));
            var inWindowOld = collUtc.AddMinutes(1);
            var inWindowNew = collUtc.AddMinutes(9);
            var beforeWindow = collUtc.AddMinutes(-120);   // outside the [start, end] sample_time filter

            await InsertMemoryPressureAsync(connection, collUtc, inWindowNew, "RESOURCE_MEMPHYSICAL_LOW", process: 3, system: 2);
            await InsertMemoryPressureAsync(connection, collUtc, inWindowOld, "RESOURCE_MEMPHYSICAL_LOW", process: 2, system: 0);
            await InsertMemoryPressureAsync(connection, collUtc, beforeWindow, "RESOURCE_MEMPHYSICAL_HIGH", process: 0, system: 0);

            var events = await viewer.GetMemoryPressureEventsAsync(
                PressureServerId, collUtc, collUtc.AddMinutes(10));

            /* Only the two in-window samples survive the sample_time window; ordered by sample_time. */
            Assert.Equal(2, events.Count);
            Assert.Equal(inWindowOld.Ticks, events[0].SampleTime.Ticks);
            Assert.Equal(2, events[0].MemoryIndicatorsProcess);
            Assert.Equal(0, events[0].MemoryIndicatorsSystem);
            Assert.Equal(inWindowNew.Ticks, events[1].SampleTime.Ticks);
            Assert.Equal(3, events[1].MemoryIndicatorsProcess);
            Assert.Equal(2, events[1].MemoryIndicatorsSystem);
            Assert.Equal("RESOURCE_MEMPHYSICAL_LOW", events[1].MemoryNotification);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "memory_pressure_events", PressureServerId, cleanupCt));
        }
    }

    private static async Task InsertMemoryStatsAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc,
        decimal totalPhys, decimal availPhys, decimal totalPage, decimal availPage,
        string state, string model, decimal target, decimal total, decimal buffer, decimal plan)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, available_physical_memory_mb, total_page_file_mb, available_page_file_mb,
     system_memory_state, sql_memory_model,
     target_server_memory_mb, total_server_memory_mb, buffer_pool_mb, plan_cache_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(StatsServerId);
        command.Parameters.AddWithValue(StatsServerName);
        command.Parameters.AddWithValue(totalPhys);
        command.Parameters.AddWithValue(availPhys);
        command.Parameters.AddWithValue(totalPage);
        command.Parameters.AddWithValue(availPage);
        command.Parameters.AddWithValue(state);
        command.Parameters.AddWithValue(model);
        command.Parameters.AddWithValue(target);
        command.Parameters.AddWithValue(total);
        command.Parameters.AddWithValue(buffer);
        command.Parameters.AddWithValue(plan);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertMemoryClerkAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, string clerkType, decimal memoryMb)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_clerks
    (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ClerkServerId);
        command.Parameters.AddWithValue(ClerkServerName);
        command.Parameters.AddWithValue(clerkType);
        command.Parameters.AddWithValue(memoryMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>#4234 test helper: <see cref="InsertMemoryClerkAsync"/> hardcodes <see cref="ClerkServerId"/>;
    /// the budget/singleton/picker live tests each need their own server id so they cannot race each other's
    /// row churn under the shared "live-postgres" collection.</summary>
    private static async Task InsertMemoryClerkAtServerAsync(
        NpgsqlConnection connection, int serverId, DateTime collectionTimeUtc, string clerkType, decimal memoryMb)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_clerks
    (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(ClerkServerName);
        command.Parameters.AddWithValue(clerkType);
        command.Parameters.AddWithValue(memoryMb);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One row per minute from <paramref name="start"/> to <paramref name="end"/> inclusive,
    /// generated server-side (the real memory_clerks cadence) — the #4234 budget-cap live test's seed,
    /// avoiding a round trip per minute over a 7-day window. Mirrors
    /// <c>ViewerTrendBucketingReviewTests.BulkSeedWaitAsync</c>.</summary>
    private static async Task BulkSeedMemoryClerkAsync(
        NpgsqlConnection connection, System.Threading.CancellationToken ct, long baseId, DateTime start, DateTime end, string clerkType)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_clerks
    (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
SELECT $1 + row_number() OVER (), g, $2, $3, $4, 500.00
FROM generate_series($5::timestamp, $6::timestamp, interval '1 minute') AS g", connection);
        command.Parameters.AddWithValue(baseId);
        command.Parameters.AddWithValue(ClerkBudgetServerId);
        command.Parameters.AddWithValue(ClerkServerName);
        command.Parameters.AddWithValue(clerkType);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(DateTime.SpecifyKind(end, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMemoryGrantAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime collectionTimeUtc, int poolId,
        decimal availMb, decimal grantedMb, decimal usedMb, int grantee, int waiter, long timeoutDelta, long forcedDelta)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_grant_stats
    (collection_id, collection_time, server_id, server_name, pool_id,
     available_memory_mb, granted_memory_mb, used_memory_mb,
     grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(serverName);
        command.Parameters.AddWithValue(poolId);
        command.Parameters.AddWithValue(availMb);
        command.Parameters.AddWithValue(grantedMb);
        command.Parameters.AddWithValue(usedMb);
        command.Parameters.AddWithValue(grantee);
        command.Parameters.AddWithValue(waiter);
        command.Parameters.AddWithValue(timeoutDelta);
        command.Parameters.AddWithValue(forcedDelta);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertMemoryPressureAsync(
        NpgsqlConnection connection, DateTime collectionTimeUtc, DateTime sampleTimeUtc,
        string notification, int process, int system)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_pressure_events
    (collection_id, collection_time, server_id, server_name,
     sample_time, memory_notification, memory_indicators_process, memory_indicators_system)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(PressureServerId);
        command.Parameters.AddWithValue(PressureServerName);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(sampleTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(notification);
        command.Parameters.AddWithValue(process);
        command.Parameters.AddWithValue(system);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
