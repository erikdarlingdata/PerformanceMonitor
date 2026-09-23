/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3896: the analysis "latest value" reads — each series' newest sample, for the database-size, autogrowth,
/// disk-space, memory-clerk, plan-cache and memory_stats facts and the autogrowth drill-down — are bounded to
/// <see cref="AnalysisContext.LatestValueLookback"/> before the window's end, not merely capped at it.
///
/// <para>Both halves of the defect are pinned. The cost: capped only above, the window functions numbered every
/// row the server had retained to keep a few dozen, and TimescaleDB could exclude no chunk — 1,167 ms of
/// execution for <c>DatabaseSizeSql</c> alone on DARLING01's 17-day store, against 56 ms bounded. The answer:
/// "latest EVER" kept five dropped databases' files in <c>DATABASE_TOTAL_SIZE_MB</c> (239,068 MB against the
/// real 182,496), and their ALTER DATABASE statements in the autogrowth drill-down, until retention aged the
/// rows out. The same ghosts sat in <c>DB_CONFIG</c>, whose read now anchors on the newest on-load capture
/// instead.</para>
///
/// <para>The regression is quiet in both directions — the unbounded read returns the same number on any store
/// small enough for a test, and a ghost looks like data — so the shape is pinned on the text, the Lite twin is
/// pinned to the same text, and the gated arm seeds ghosts and asks the planner.</para>
/// </summary>
public sealed class LatestValueLookbackSqlTests
{
    /// <summary>Every lookback-bounded read, by name: the six the issue inventoried plus memory_stats, the same
    /// shape with an ORDER BY … LIMIT 1 in place of the window function.</summary>
    public static TheoryData<string> LatestValueReadNames => new()
    {
        nameof(PgFactCollector.DatabaseSizeSql),
        nameof(PgFactCollector.FileAutogrowthSql),
        nameof(PgFactCollector.DiskSpaceSql),
        nameof(PgFactCollector.MemoryClerkSql),
        nameof(PgFactCollector.PlanCacheStatsSql),
        nameof(PgFactCollector.MemoryStatsSql),
        nameof(PgDrillDownCollector.AutogrowthPercentFilesSql),
    };

    internal static string SqlFor(string name) => name switch
    {
        nameof(PgFactCollector.DatabaseSizeSql) => PgFactCollector.DatabaseSizeSql,
        nameof(PgFactCollector.FileAutogrowthSql) => PgFactCollector.FileAutogrowthSql,
        nameof(PgFactCollector.DiskSpaceSql) => PgFactCollector.DiskSpaceSql,
        nameof(PgFactCollector.MemoryClerkSql) => PgFactCollector.MemoryClerkSql,
        nameof(PgFactCollector.PlanCacheStatsSql) => PgFactCollector.PlanCacheStatsSql,
        nameof(PgFactCollector.MemoryStatsSql) => PgFactCollector.MemoryStatsSql,
        nameof(PgDrillDownCollector.AutogrowthPercentFilesSql) => PgDrillDownCollector.AutogrowthPercentFilesSql,
        nameof(PgFactCollector.DatabaseConfigSql) => PgFactCollector.DatabaseConfigSql,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

    /// <summary>
    /// $2 is the lookback start and $3 the window end, and nothing else binds. A bare parameter on each side is
    /// the form TimescaleDB excludes chunks on reliably (the <c>PlanRegressionSql</c> lesson), and it keeps the
    /// lookback in C#, on <see cref="AnalysisContext.LatestValueStart"/>, where its reason is written down.
    /// </summary>
    [Theory]
    [MemberData(nameof(LatestValueReadNames))]
    public void EachLatestValueRead_IsBoundedBelowByTheLookback_AndAboveByTheWindowEnd(string name)
    {
        var sql = SqlFor(name);

        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time <= $3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);

        /* Not an interval spelled into the SQL: the lookback is one number, in one place. */
        Assert.DoesNotMatch(new Regex(@"INTERVAL", RegexOptions.IgnoreCase), sql);
    }

    [Fact]
    public void TheLookback_IsADay_AnchoredOnTheWindowsEnd_NotOnNow()
    {
        Assert.Equal(TimeSpan.FromHours(24), AnalysisContext.LatestValueLookback);

        /* A historical window reads the state as it stood at ITS end — the anchored analyze_server and
           compare_analysis's baseline window both depend on that. */
        var end = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var context = new AnalysisContext { TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
        Assert.Equal(end.AddHours(-24), context.LatestValueStart);

        /* The lookback reaches further back than the analyze_server default window, so a series sampled once
           in the window is never lost to the bound. */
        Assert.True(AnalysisContext.LatestValueLookback > TimeSpan.FromHours(4));
    }

    /// <summary>
    /// database_config is an ON-LOAD snapshot, so a lookback would lose it on any server connected for longer
    /// than a day. The ghosts go instead by anchoring on the newest capture — the rule its own drill-down
    /// (<see cref="PgDrillDownCollector.ConfigIssuesSql"/>) already followed, so the count and the list now
    /// agree.
    /// </summary>
    [Fact]
    public void TheDatabaseConfigRead_AnchorsOnTheNewestCapture_AndTakesNoLookback()
    {
        var sql = PgFactCollector.DatabaseConfigSql;

        Assert.Contains(
            "AND   capture_time = (SELECT MAX(capture_time) FROM database_config WHERE server_id = $1)",
            sql,
            StringComparison.Ordinal);
        Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time = (SELECT MAX(capture_time) FROM v_database_config WHERE server_id = $1)",
            PgDrillDownCollector.ConfigIssuesSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The enforcement for the NEXT read of this shape. Every statement in the fact collector that picks a
    /// series' newest row — a window function ordered on <c>collection_time DESC</c>, or an
    /// <c>ORDER BY collection_time DESC LIMIT 1</c> — over a CADENCED collector's table must bound
    /// <c>collection_time</c> from below. The on-load snapshots (<see cref="CollectorScheduleDefaults"/> frequency
    /// 0) are exempt by their schedule, because their newest capture is as old as the last connect.
    /// </summary>
    [Fact]
    public void EveryNewestPerSeriesRead_OfACadencedCollector_CarriesALowerBound()
    {
        var reads = PgFactCollector.AllSql.Select(sql => StripComments(sql)).Where(IsNewestPerSeriesRead).ToList();

        /* Nine today: the six lookback reads in this collector, plus session_stats, perfmon_stats and the
           runnable-task read, which were window-bounded already. A floor, not an exact count, so a tenth is
           allowed; finding NONE would mean the scan broke, and a broken scan passes everything. */
        Assert.True(reads.Count >= 9, $"found {reads.Count} newest-per-series reads; the scan is broken, not the tree clean");

        var checkedCount = 0;
        foreach (var sql in reads)
        {
            var cadenced = TablesRead(sql).Where(IsCadencedCollectorTable).ToList();
            if (cadenced.Count == 0)
            {
                continue;
            }

            checkedCount++;
            Assert.True(
                Regex.IsMatch(sql, @"\bcollection_time\s*>=?\s*\$\d"),
                $"A newest-per-series read of {string.Join(", ", cadenced)} carries no lower bound on collection_time, "
                + "so it numbers the server's whole retained history and keeps series that stopped reporting (#3896). "
                + $"Bind AnalysisContext.LatestValueStart:\n{sql}");
        }

        Assert.True(checkedCount >= 9, $"only {checkedCount} cadenced newest-per-series reads were checked");
    }

    /// <summary>The scan's own positive control, both directions: the shipped-before shape is caught, and an
    /// on-load snapshot is not dragged in.</summary>
    [Theory]
    [InlineData(true, "WITH latest AS (SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY collection_time DESC) AS rn FROM file_io_stats WHERE server_id = $1 AND collection_time <= $2) SELECT 1 FROM latest")]
    [InlineData(true, "SELECT a FROM v_memory_stats WHERE server_id = $1 AND collection_time <= $2 ORDER BY collection_time DESC\nLIMIT 1")]
    [InlineData(true, "WITH r AS (SELECT a, DENSE_RANK() OVER (ORDER BY collection_time DESC) AS rnk FROM plan_cache_stats WHERE server_id = $1) SELECT 1 FROM r")]
    [InlineData(false, "SELECT a FROM server_properties WHERE server_id = $1 ORDER BY collection_time DESC LIMIT 1")]
    [InlineData(false, "WITH latest AS (SELECT a, ROW_NUMBER() OVER (PARTITION BY a ORDER BY collection_time DESC) AS rn FROM file_io_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) SELECT 1 FROM latest")]
    public void TheScan_FlagsAnUnboundedCadencedRead_AndOnlyThat(bool violates, string sql)
    {
        Assert.True(IsNewestPerSeriesRead(sql));
        var cadenced = TablesRead(sql).Where(IsCadencedCollectorTable).Any();
        var bounded = Regex.IsMatch(sql, @"\bcollection_time\s*>=?\s*\$\d");
        Assert.Equal(violates, cadenced && !bounded);
    }

    /// <summary>
    /// The Lite twin carries each statement verbatim — Darling reads the base table where Lite reads the
    /// <c>v_</c> archive view over the same table, and that is the only difference. The twin matters more on
    /// Lite than here: its <c>v_</c> views UNION the parquet archive, and without the lower bound DuckDB could
    /// prune no parquet row group, so these reads scanned the whole archive.
    /// </summary>
    [Theory]
    [InlineData(nameof(PgFactCollector.DatabaseSizeSql), "Lite/Analysis/DuckDbFactCollector.Storage.cs")]
    [InlineData(nameof(PgFactCollector.FileAutogrowthSql), "Lite/Analysis/DuckDbFactCollector.Storage.cs")]
    [InlineData(nameof(PgFactCollector.DiskSpaceSql), "Lite/Analysis/DuckDbFactCollector.Storage.cs")]
    [InlineData(nameof(PgFactCollector.MemoryClerkSql), "Lite/Analysis/DuckDbFactCollector.Resources.cs")]
    [InlineData(nameof(PgFactCollector.PlanCacheStatsSql), "Lite/Analysis/DuckDbFactCollector.Resources.cs")]
    [InlineData(nameof(PgFactCollector.MemoryStatsSql), "Lite/Analysis/DuckDbFactCollector.Resources.cs")]
    [InlineData(nameof(PgFactCollector.DatabaseConfigSql), "Lite/Analysis/DuckDbFactCollector.Config.cs")]
    [InlineData(nameof(PgDrillDownCollector.AutogrowthPercentFilesSql), "Lite/Analysis/DrillDownCollector.Storage.cs")]
    public void TheLiteTwin_CarriesTheSameStatement(string name, string liteFile)
    {
        var darling = Normalize(SqlFor(name)).Trim();
        var lite = Normalize(File.ReadAllText(Path.Combine(RepoRoot(), liteFile)));

        /* Map Darling's base-table references onto the v_ views Lite reads; a v_ reference is left alone. */
        var asLite = Regex.Replace(darling, @"\bFROM (?!v_)(\w+)", m =>
            CollectorCatalog.All.Any(s => s.TargetTable == m.Groups[1].Value) ? "FROM v_" + m.Groups[1].Value : m.Value);

        Assert.Contains(asLite, lite, StringComparison.Ordinal);
    }

    internal static bool IsNewestPerSeriesRead(string sql) =>
        Regex.IsMatch(sql, @"OVER\s*\([^)]*?ORDER\s+BY\s+collection_time\s+DESC\s*\)", RegexOptions.IgnoreCase)
        || Regex.IsMatch(sql, @"ORDER\s+BY\s+collection_time\s+DESC\s+LIMIT\s+1\b", RegexOptions.IgnoreCase);

    private static IEnumerable<string> TablesRead(string sql) =>
        Regex.Matches(sql, @"\b(?:FROM|JOIN)\s+(\w+)", RegexOptions.IgnoreCase)
            .Select(m => m.Groups[1].Value)
            .Select(t => t.StartsWith("v_", StringComparison.Ordinal) ? t[2..] : t)
            .Distinct(StringComparer.Ordinal);

    private static bool IsCadencedCollectorTable(string table)
    {
        var collector = CollectorCatalog.All.FirstOrDefault(s => s.TargetTable == table);
        return collector is not null
               && CollectorScheduleDefaults.All.TryGetValue(collector.Name, out var entry)
               && entry.FrequencyMinutes > 0;
    }

    private static string StripComments(string sql)
    {
        var stripped = Regex.Replace(sql, @"/\*.*?\*/", " ", RegexOptions.Singleline);
        return Regex.Replace(stripped, @"--[^\n]*", " ");
    }

    private static string Normalize(string text) => text.Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null
               && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln"))
               && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) round-trips for the #3896 reads through the real collectors, against seeded ghosts:
/// a database dropped a little over a day before the window's end, its files, its percent-autogrowth file, its
/// volume and its config row, beside a live database sampled minutes ago. Serialized through the
/// "live-postgres" collection with sentinel server ids, cleaned up through <see cref="LiveStoreCleanup"/>.
///
/// <para>Three answers are pinned, because each is a behavior someone will one day ask about: a live server
/// reports only what it still has; a server whose newest sample is older than the lookback reports NO
/// latest-value fact — the collector is down or the server is gone, and a stale number would read as current —
/// but keeps its on-load config; and a historical window reports the state as it stood at its own end, ghost
/// included, because the ghost existed then.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class LatestValueLookbackLivePostgresTests
{
    /// <summary>Sentinel ids — a real server_id is a storage-name hash, never these.</summary>
    private const int LiveServerId = -389_601;
    private const int StaleServerId = -389_602;
    private const int PlanShapeServerId = -389_603;

    private static readonly int[] s_serverIds = { LiveServerId, StaleServerId, PlanShapeServerId };

    private static readonly string[] s_tables =
    {
        "file_io_stats", "database_size_stats", "memory_clerks", "plan_cache_stats", "memory_stats", "database_config",
    };

    [Fact]
    public async Task ALiveServer_ReportsOnlyWhatItStillHas_AndTheDrillDownListsTheSameFiles_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value lookback test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            await SeedLiveAndGhostAsync(connection, LiveServerId, end, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = Context(LiveServerId, end);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(context)).ToDictionary(f => f.Key);

            /* 1,000 + 200: the live database's newest data-file size and its log. Not 51,200 — the dropped
               database's 50 GB file, last seen 25 hours ago, is gone; not 7,777, the sample after the window. */
            Assert.Equal(1200.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);

            var autogrowth = facts["FILE_AUTOGROWTH_PERCENT"];
            Assert.Equal(1, autogrowth.Value);
            Assert.Equal(1, autogrowth.Metadata["database_count"]);

            /* The live volume only: 15% free, not the ghost volume's 2%. */
            var disk = facts["DISK_SPACE"];
            Assert.Equal(0.15, disk.Value, precision: 6);
            Assert.Equal(1, disk.Metadata["volume_count"]);
            Assert.Equal(1_000_000, disk.Metadata["total_volume_mb"]);

            var clerks = facts["MEMORY_CLERKS"];
            Assert.Equal(900, clerks.Value, precision: 6);
            Assert.False(clerks.Metadata.ContainsKey("CACHESTORE_GHOST"), "a clerk last seen 25 hours ago was reported");

            Assert.Equal(60.0, facts["PLAN_CACHE_BLOAT"].Value, precision: 6);
            Assert.Equal(65_536, facts["MEMORY_TOTAL_PHYSICAL_MB"].Value, precision: 6);

            /* The newest capture — five days old, which is ordinary for an on-load snapshot — holds the live
               database only; the older capture's dropped database (auto_shrink ON, RCSI OFF) is not counted. */
            var dbConfig = facts["DB_CONFIG"];
            Assert.Equal(1, dbConfig.Metadata["database_count"]);
            Assert.Equal(0, dbConfig.Metadata["auto_shrink_on_count"]);
            Assert.Equal(0, dbConfig.Metadata["rcsi_off_count"]);

            /* The seam: the drill-down that renders the fix names exactly the file the count counted. */
            var files = await AutogrowthFilesAsync(postgres, context);
            var file = Assert.Single(files);
            Assert.Equal("LiveDb", file.GetProperty("database").GetString());
            Assert.Equal("LiveDb_data", file.GetProperty("logical_file_name").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    [Fact]
    public async Task AServerWhoseNewestSampleIsOlderThanTheLookback_EmitsNoLatestValueFact_ButKeepsItsOnLoadConfig_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value lookback test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            /* Everything a day and an hour old: the collectors stopped, or the server went away. */
            var end = TruncateToSeconds(DateTime.UtcNow);
            var stale = end.AddHours(-25);
            await InsertFileIoAsync(connection, StaleServerId, stale, "StaleDb", "StaleDb_data", 4096, ct);
            await InsertSizeAsync(connection, StaleServerId, stale, "StaleDb", 1, "ROWS", "StaleDb_data", 20_480, percentGrowth: true, "D:\\", 1_000_000, 10_000, ct);
            await InsertClerkAsync(connection, StaleServerId, stale, "MEMORYCLERK_SQLBUFFERPOOL", 800, ct);
            await InsertPlanCacheAsync(connection, StaleServerId, stale, totalPlans: 100, singleUse: 90, ct);
            await InsertMemoryStatsAsync(connection, StaleServerId, stale, 32_768, ct);
            await InsertDatabaseConfigAsync(connection, StaleServerId, end.AddDays(-5), "StaleDb", autoShrink: false, rcsiOn: true, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(Context(StaleServerId, end))).ToDictionary(f => f.Key);

            foreach (var key in new[]
                     {
                         "DATABASE_TOTAL_SIZE_MB", "FILE_AUTOGROWTH_PERCENT", "DISK_SPACE", "MEMORY_CLERKS",
                         "PLAN_CACHE_BLOAT", "MEMORY_TOTAL_PHYSICAL_MB", "MEMORY_BUFFER_POOL_MB", "MEMORY_TARGET_MB",
                     })
            {
                Assert.False(facts.ContainsKey(key), $"{key} was emitted from a sample 25 hours older than the window's end");
            }

            /* The on-load config is not a latest-value read: its newest capture is as old as the last connect. */
            Assert.Equal(1, facts["DB_CONFIG"].Metadata["database_count"]);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    [Fact]
    public async Task AHistoricalWindow_ReadsTheStateAsItStoodAtItsOwnEnd_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value lookback test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            await SeedLiveAndGhostAsync(connection, LiveServerId, end, ct);

            /* Twenty hours ago the dropped database was still there (last seen five hours before that), and none
               of the live database's recent samples had been taken yet. */
            var historicalEnd = end.AddHours(-20);
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = Context(LiveServerId, historicalEnd);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(context)).ToDictionary(f => f.Key);

            Assert.Equal(50_000.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);
            Assert.Equal(0.02, facts["DISK_SPACE"].Value, precision: 6);
            Assert.Equal(1, facts["FILE_AUTOGROWTH_PERCENT"].Value);

            var file = Assert.Single(await AutogrowthFilesAsync(postgres, context));
            Assert.Equal("GhostDb", file.GetProperty("database").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// The evidence no string pin can give: that the planner prunes to the lookback. Builds the store the way
    /// the service does (ladder, then the hypertable conversion where TimescaleDB is present), seeds one server's
    /// files once an hour across ten days — eleven one-day chunks — and EXPLAINs the shipped statement with its
    /// real bound parameters: at most the two chunks a 24-hour span can touch appear in the plan. The
    /// pre-#3896 shape, kept here as the oracle, plans every one of them, and both return the same total
    /// because every file in this seed is still live.
    /// </summary>
    [Fact]
    public async Task TheShippedDatabaseSizeRead_PlansOnlyTheLookbacksChunks_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value plan-shape test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* #1922: probe on its own connection. */
        var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(connectionString!, ct);
        if (timescaleEnabled)
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            using (var seed = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 9_389_603_000 + (EXTRACT(EPOCH FROM t)::bigint % 1_000_000) * 10 + f, t, $1, 'latest-value-plan-shape',
       'Db' || f, 'Db' || f || '_data', 'ROWS', 100 * f
FROM generate_series($2::timestamp - interval '10 days', $2::timestamp, interval '1 hour') AS t
CROSS JOIN generate_series(1, 3) AS f", connection))
            {
                seed.Parameters.AddWithValue(PlanShapeServerId);
                seed.Parameters.AddWithValue(end);
                await seed.ExecuteNonQueryAsync(ct);
            }

            using (var analyze = new NpgsqlCommand("ANALYZE collect.file_io_stats", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            var context = Context(PlanShapeServerId, end);
            var shipped = await ExplainAsync(connection, PgFactCollector.DatabaseSizeSql,
                [PlanShapeServerId, context.LatestValueStart, context.TimeRangeEnd], ct);
            var oracle = await ExplainAsync(connection, UnboundedDatabaseSizeSql,
                [PlanShapeServerId, context.TimeRangeEnd], ct);

            if (timescaleEnabled)
            {
                var shippedChunks = PlanChunkScans.Count(shipped);
                var oracleChunks = PlanChunkScans.Count(oracle);
                Assert.True(oracleChunks >= 11,
                    $"the pre-#3896 shape should plan every seeded chunk (eleven days of data), planned {oracleChunks}:\n{oracle}");
                Assert.True(shippedChunks is >= 1 and <= 2,
                    $"the shipped read planned {shippedChunks} chunks; a 24-hour lookback touches at most two:\n{shipped}");
            }

            /* Same answer both ways, because nothing in this seed is a ghost: 100 + 200 + 300. */
            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            Assert.Equal(600.0, Assert.Single(facts, f => f.Key == "DATABASE_TOTAL_SIZE_MB").Value, precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>The read as shipped before #3896 — capped at the window's end, bounded nowhere else. The oracle
    /// the plan-shape test compares against, never a read the product runs.</summary>
    private const string UnboundedDatabaseSizeSql = @"
WITH latest AS (
    SELECT database_name, file_name, size_mb,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_name ORDER BY collection_time DESC) AS rn
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time <= $2
    AND   size_mb > 0
)
SELECT SUM(size_mb) AS total_size_mb
FROM latest
WHERE rn = 1";

    /// <summary>
    /// The live database, sampled minutes before <paramref name="end"/>, beside a database dropped 25 hours
    /// before it: every table a #3896 read touches, ghost and live, plus one sample AFTER the window's end that
    /// the upper bound must keep out.
    /// </summary>
    private static async Task SeedLiveAndGhostAsync(NpgsqlConnection connection, int serverId, DateTime end, CancellationToken ct)
    {
        var ghost = end.AddHours(-25);

        await InsertFileIoAsync(connection, serverId, end.AddMinutes(-65), "LiveDb", "LiveDb_data", 900, ct);
        await InsertFileIoAsync(connection, serverId, end.AddMinutes(-5), "LiveDb", "LiveDb_data", 1000, ct);
        await InsertFileIoAsync(connection, serverId, end.AddMinutes(-5), "LiveDb", "LiveDb_log", 200, ct);
        await InsertFileIoAsync(connection, serverId, ghost, "GhostDb", "GhostDb_data", 50_000, ct);
        await InsertFileIoAsync(connection, serverId, end.AddMinutes(30), "LiveDb", "LiveDb_data", 7777, ct);

        await InsertSizeAsync(connection, serverId, end.AddMinutes(-30), "LiveDb", 1, "ROWS", "LiveDb_data", 20_480, percentGrowth: true, "D:\\", 1_000_000, 150_000, ct);
        await InsertSizeAsync(connection, serverId, end.AddMinutes(-30), "LiveDb", 2, "LOG", "LiveDb_log", 512, percentGrowth: false, "D:\\", 1_000_000, 150_000, ct);
        await InsertSizeAsync(connection, serverId, ghost, "GhostDb", 1, "ROWS", "GhostDb_data", 40_960, percentGrowth: true, "E:\\", 500_000, 10_000, ct);

        await InsertClerkAsync(connection, serverId, end.AddMinutes(-5), "MEMORYCLERK_SQLBUFFERPOOL", 800, ct);
        await InsertClerkAsync(connection, serverId, end.AddMinutes(-5), "CACHESTORE_SQLCP", 100, ct);
        await InsertClerkAsync(connection, serverId, ghost, "CACHESTORE_GHOST", 5000, ct);

        await InsertPlanCacheAsync(connection, serverId, end.AddMinutes(-10), totalPlans: 100, singleUse: 60, ct);
        await InsertMemoryStatsAsync(connection, serverId, end.AddMinutes(-2), 65_536, ct);

        /* Two on-load captures: ten days ago with both databases, five days ago with the live one alone. */
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-10), "LiveDb", autoShrink: false, rcsiOn: true, ct);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-10), "GhostDb", autoShrink: true, rcsiOn: false, ct);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-5), "LiveDb", autoShrink: false, rcsiOn: true, ct);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-5), "master", autoShrink: false, rcsiOn: false, ct);
    }

    private static AnalysisContext Context(int serverId, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = "latest-value-lookback-e2e",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
        ServerUtcOffset = TimeSpan.Zero,
    };

    private static async Task<List<JsonElement>> AutogrowthFilesAsync(NpgsqlDataSource postgres, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            ServerId = context.ServerId,
            RootFactKey = "FILE_AUTOGROWTH_PERCENT",
            StoryPath = "FILE_AUTOGROWTH_PERCENT",
            PathKeys = ["FILE_AUTOGROWTH_PERCENT"],
            Severity = 0.3,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.NotNull(finding.DrillDown);
        if (!finding.DrillDown.TryGetValue("autogrowth_percent_files", out var raw))
        {
            return [];
        }

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, string sql, object[] parameters, CancellationToken ct)
    {
        using var explain = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + sql, connection);
        foreach (var parameter in parameters)
        {
            explain.Parameters.AddWithValue(parameter is DateTime dt ? DateTime.SpecifyKind(dt, DateTimeKind.Unspecified) : parameter);
        }

        var plan = new StringBuilder();
        using var reader = await explain.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            plan.AppendLine(reader.GetString(0));
        }

        return plan.ToString();
    }

    private static async Task InsertFileIoAsync(NpgsqlConnection connection, int serverId, DateTime at,
        string database, string file, decimal sizeMb, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', $4, $5, 'ROWS', $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(file);
        command.Parameters.AddWithValue(sizeMb);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSizeAsync(NpgsqlConnection connection, int serverId, DateTime at,
        string database, int fileId, string fileType, string file, decimal totalMb, bool percentGrowth,
        string volume, decimal volumeTotalMb, decimal volumeFreeMb, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name,
     total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(fileId);
        command.Parameters.AddWithValue(fileType);
        command.Parameters.AddWithValue(file);
        command.Parameters.AddWithValue(totalMb);
        command.Parameters.AddWithValue(percentGrowth);
        command.Parameters.AddWithValue(percentGrowth ? 10 : 0);
        command.Parameters.AddWithValue(volume);
        command.Parameters.AddWithValue(volumeTotalMb);
        command.Parameters.AddWithValue(volumeFreeMb);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertClerkAsync(NpgsqlConnection connection, int serverId, DateTime at,
        string clerk, decimal memoryMb, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', $4, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(clerk);
        command.Parameters.AddWithValue(memoryMb);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertPlanCacheAsync(NpgsqlConnection connection, int serverId, DateTime at,
        int totalPlans, int singleUse, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
     total_plans, single_use_plans, total_size_mb, single_use_size_mb)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', 'Compiled Plan', 'Adhoc', $4, $5, 400, 300)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(totalPlans);
        command.Parameters.AddWithValue(singleUse);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMemoryStatsAsync(NpgsqlConnection connection, int serverId, DateTime at,
        decimal physicalMb, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, buffer_pool_mb, target_server_memory_mb)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', $4, $5, $6)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(physicalMb);
        command.Parameters.AddWithValue(physicalMb / 2);
        command.Parameters.AddWithValue(physicalMb * 3 / 4);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertDatabaseConfigAsync(NpgsqlConnection connection, int serverId, DateTime capturedAt,
        string database, bool autoShrink, bool rcsiOn, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name, recovery_model,
     is_auto_shrink_on, is_auto_close_on, is_read_committed_snapshot_on, is_auto_create_stats_on,
     is_auto_update_stats_on, page_verify_option, is_query_store_on)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', $4, 'FULL', $5, false, $6, true, true, 'CHECKSUM', true)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(capturedAt);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(database);
        command.Parameters.AddWithValue(autoShrink);
        command.Parameters.AddWithValue(rcsiOn);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", s_serverIds.Select(id => id.ToString(CultureInfo.InvariantCulture)));
        foreach (var table in s_tables)
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids})", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
