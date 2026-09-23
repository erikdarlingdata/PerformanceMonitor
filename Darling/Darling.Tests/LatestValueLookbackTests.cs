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
using PerformanceMonitor.Darling.Service;
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
    /// lookback in C#, on <see cref="AnalysisContext.LatestValueStartFor"/>, where its reason is written down.
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

    /// <summary>
    /// A day, or twice the collector's interval if that is longer. Cadences are operator-editable and a
    /// steady-state collection advances by exactly the interval, so a flat day would drop a daily collector's
    /// fact on every pass between the day and its next run — a flap that resolves, then re-fires, the findings
    /// built on it. Every shipped cadence is under twelve hours, so the default behavior is the flat day.
    /// </summary>
    [Theory]
    [InlineData(1, 24 * 60)]
    [InlineData(60, 24 * 60)]
    [InlineData(720, 24 * 60)]
    [InlineData(721, 2 * 721)]
    [InlineData(1440, 48 * 60)]
    [InlineData(10_080, 2 * 10_080)]
    public void TheLookback_IsADayOrTwiceTheInterval(int frequencyMinutes, int expectedMinutes)
    {
        Assert.Equal(TimeSpan.FromMinutes(expectedMinutes), AnalysisContext.LatestValueLookbackFor(frequencyMinutes));
    }

    [Fact]
    public void AnOnLoadCollector_TakesNoLookback_AndEveryShippedCadenceTakesTheFlatDay()
    {
        /* On load only (0): the read anchors on the newest capture instead, however old. */
        Assert.Null(AnalysisContext.LatestValueLookbackFor(0));

        foreach (var collector in PgLatestValueBounds.Collectors)
        {
            var shipped = CollectorScheduleDefaults.All[collector.Name].FrequencyMinutes;
            Assert.Equal(AnalysisContext.LatestValueLookback, AnalysisContext.LatestValueLookbackFor(shipped));
        }
    }

    [Fact]
    public void TheBound_IsAnchoredOnTheWindowsEnd_AndTheStampWins()
    {
        Assert.Equal(TimeSpan.FromHours(24), AnalysisContext.LatestValueLookback);

        /* A historical window reads the state as it stood at ITS end — the anchored analyze_server and
           compare_analysis's baseline window both depend on that. Unstamped, every read takes the day. */
        var end = new DateTime(2026, 3, 1, 12, 0, 0, DateTimeKind.Unspecified);
        var context = new AnalysisContext { TimeRangeStart = end.AddHours(-4), TimeRangeEnd = end };
        Assert.Equal(end.AddHours(-24), context.LatestValueStartFor("database_size_stats"));

        context.LatestValueStarts = new Dictionary<string, DateTime> { ["database_size_stats"] = end.AddHours(-48) };
        Assert.Equal(end.AddHours(-48), context.LatestValueStartFor("database_size_stats"));
        Assert.Equal(end.AddHours(-24), context.LatestValueStartFor("memory_stats"));

        /* The lookback reaches further back than the analyze_server default window, so a series sampled once
           in the window is never lost to the bound. */
        Assert.True(AnalysisContext.LatestValueLookback > TimeSpan.FromHours(4));
    }

    /// <summary>
    /// The analysis pass must bound a read by the cadence the scheduler RUNS the collector at, or it bounds a
    /// daily collector by a day and flaps. One rule serves both: <c>StoreConfigProvider.ResolveSchedule</c>
    /// delegates to <see cref="CollectorScheduleDefaults.ResolveFrequencyMinutes"/>, so they cannot drift.
    /// </summary>
    [Theory]
    [InlineData("database_size_stats", null, null, 60)]
    [InlineData("database_size_stats", 1440, null, 1440)]
    [InlineData("database_size_stats", null, 720, 720)]
    [InlineData("database_size_stats", 1440, 720, 1440)]
    [InlineData("database_size_stats", -5, 720, 720)]
    [InlineData("memory_stats", 0, null, 0)]
    [InlineData("file_io_stats", 1440, null, 1)]
    [InlineData("file_io_stats", 30, null, 30)]
    public void TheSchedulerAndTheAnalysis_ResolveOneCadence(string collector, int? perServer, int? fleet, int expected)
    {
        Assert.Equal(expected, CollectorScheduleDefaults.ResolveFrequencyMinutes(collector, perServer, fleet));

        const int ServerId = 7;
        var overrides = new List<ScheduleOverride>();
        if (perServer is not null) overrides.Add(new ScheduleOverride(ServerId, collector, perServer, null, true));
        if (fleet is not null) overrides.Add(new ScheduleOverride(null, collector, fleet, null, true));
        Assert.Equal(expected, StoreConfigProvider.ResolveSchedule(collector, ServerId, overrides).FrequencyMinutes);
    }

    /// <summary>
    /// The seam between a read and its bound: each latest-value command binds the lower bound of the collector
    /// whose table its SQL reads. Bound by the wrong collector, a daily database_size_stats override would widen
    /// the memory_stats read and leave the size reads at a day — both wrong, and invisible on default cadences.
    /// </summary>
    [Theory]
    [InlineData("PgFactCollector.Storage.cs", nameof(PgFactCollector.DatabaseSizeSql))]
    [InlineData("PgFactCollector.Storage.cs", nameof(PgFactCollector.FileAutogrowthSql))]
    [InlineData("PgFactCollector.Storage.cs", nameof(PgFactCollector.DiskSpaceSql))]
    [InlineData("PgFactCollector.Resources.cs", nameof(PgFactCollector.MemoryClerkSql))]
    [InlineData("PgFactCollector.Resources.cs", nameof(PgFactCollector.PlanCacheStatsSql))]
    [InlineData("PgFactCollector.Resources.cs", nameof(PgFactCollector.MemoryStatsSql))]
    [InlineData("PgDrillDownCollector.Storage.cs", nameof(PgDrillDownCollector.AutogrowthPercentFilesSql))]
    public void EachRead_BindsTheBoundOfTheCollectorWhoseTableItReads(string file, string name)
    {
        var table = Assert.Single(TablesRead(StripComments(SqlFor(name))), t => t != "latest" && t != "ranked");
        var collector = Assert.Single(PgLatestValueBounds.Collectors, c => c.TargetTable == table);

        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Analysis", file));
        var construction = source.IndexOf($"new NpgsqlCommand({name}, connection)", StringComparison.Ordinal);
        Assert.True(construction >= 0, $"{name}'s command construction was not found in {file}");
        var bindings = source[construction..source.IndexOf("ExecuteReaderAsync", construction, StringComparison.Ordinal)];

        Assert.Contains($"context.LatestValueStartFor({collector.GetType().Name}.Instance.Name)", bindings, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(bindings, @"LatestValueStartFor\("));
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
    /// #3929: unlike database_config's exact-capture-time anchor above, trace_flags CANNOT anchor on
    /// MAX(capture_time) - a capture that finds every flag off writes ZERO rows, so MAX would silently fall
    /// back to an older, stale-ON capture. It takes the same cadence-aware lookback the six collection_time
    /// reads do instead, just spelled over capture_time (the config-snapshot family's time column, shared with
    /// database_config above) and fed by CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes rather
    /// than the raw resolved frequency, since trace_flags's own resolved frequency is 0 (on-load).
    /// </summary>
    [Fact]
    public void TheTraceFlagsRead_IsBoundedByCaptureTime_BothBelowAndAboveTheWindow()
    {
        var sql = PgFactCollector.TraceFlagsSql;

        Assert.Contains("server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time <= $3", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$4", sql, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"INTERVAL", RegexOptions.IgnoreCase), sql);
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
                + $"Bind AnalysisContext.LatestValueStartFor:\n{sql}");
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
    private const int DailyServerId = -389_605;
    private const int DailyStaleServerId = -389_606;
    private const int DefaultCadenceServerId = -389_607;
    private const int OnLoadServerId = -389_608;
    private const int FleetServerId = -389_609;
    private const int FleetOverriddenServerId = -389_610;
    private const int PlanShapeDailyServerId = -389_611;
    private const int TraceFlagClearServerId = -389_612;
    private const int TraceFlagAllOffServerId = -389_613;

    private static readonly int[] s_serverIds =
    {
        LiveServerId, StaleServerId, PlanShapeServerId, DailyServerId, DailyStaleServerId, DefaultCadenceServerId,
        OnLoadServerId, FleetServerId, FleetOverriddenServerId, PlanShapeDailyServerId,
        TraceFlagClearServerId, TraceFlagAllOffServerId,
    };

    /// <summary>The collector the fleet-wide override test takes over for the whole store, and hands back.</summary>
    private const string FleetOverrideCollector = "memory_clerks";

    private static readonly string[] s_tables =
    {
        "file_io_stats", "database_size_stats", "memory_clerks", "plan_cache_stats", "memory_stats", "database_config",
        "trace_flags",
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

    /// <summary>
    /// #3929: a trace flag turned off drops out once it is missing from the whole on-load-aware lookback
    /// window (two OnLoadRecaptureMinutes cycles - 48h at the shipped daily default), NOT after the full
    /// 30-day retention the OLD unbounded-per-flag read waited out. Plants three captures: 3 days ago (flag
    /// 3604 on - before the window, so its stale ON row must not resurface), 1 day ago (3604 already off -
    /// DBCC TRACESTATUS omits it, so no row is written for it at all; flag 2371 is on), and 6 hours ago
    /// (2371 still on). 3604 has been missing from two consecutive captures (1 day ago and 6 hours ago) and
    /// its only row is older than the 48h bound, so it must not appear; 2371 must.
    /// </summary>
    [Fact]
    public async Task TraceFlagTurnedOffAfterLastConnect_DropsOutOfTheOnLoadWindow_WhileAStillOnFlagStays_AgainstDevPostgres()
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
            await InsertTraceFlagAsync(connection, TraceFlagClearServerId, end.AddDays(-3), 3604, status: true, ct);
            await InsertTraceFlagAsync(connection, TraceFlagClearServerId, end.AddDays(-1), 2371, status: true, ct);
            await InsertTraceFlagAsync(connection, TraceFlagClearServerId, end.AddHours(-6), 2371, status: true, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(Context(TraceFlagClearServerId, end))).ToDictionary(f => f.Key);

            var traceFlags = facts["TRACE_FLAGS"];
            Assert.Equal(1, traceFlags.Value);
            Assert.Equal(1, traceFlags.Metadata["flag_count"]);
            Assert.True(traceFlags.Metadata.ContainsKey("TF_2371"), "flag 2371 (still on 6 hours ago) should be reported");
            Assert.False(traceFlags.Metadata.ContainsKey("TF_3604"), "flag 3604 (off for two full captures, last ON row 3 days old) should have cleared");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// #3929: the flags are the window's NEWEST capture, not each flag's newest row. Both flags were on in
    /// yesterday's capture, inside the window; this morning's lists only 2371. A per-flag read would keep 3604
    /// until yesterday's row left the window, for up to two more days. The newest capture drops it now.
    /// </summary>
    [Fact]
    public async Task AFlagTurnedOffWhileAnotherStaysOn_DropsOutAtTheNextCapture_NotWhenItsLastRowLeavesTheWindow_AgainstDevPostgres()
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
            await InsertTraceFlagAsync(connection, TraceFlagClearServerId, end.AddDays(-1), 3604, status: true, ct);
            await InsertTraceFlagAsync(connection, TraceFlagClearServerId, end.AddDays(-1), 2371, status: true, ct);
            await InsertTraceFlagAsync(connection, TraceFlagClearServerId, end.AddHours(-6), 2371, status: true, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(Context(TraceFlagClearServerId, end))).ToDictionary(f => f.Key);

            var traceFlags = facts["TRACE_FLAGS"];
            Assert.Equal(1, traceFlags.Metadata["flag_count"]);
            Assert.True(traceFlags.Metadata.ContainsKey("TF_2371"), "flag 2371 is on in the newest capture");
            Assert.False(traceFlags.Metadata.ContainsKey("TF_3604"),
                "flag 3604 is missing from the newest capture, so it is off, even though yesterday's ON row is still inside the window");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// #3929's sharpest edge case: a capture that finds EVERY flag off writes ZERO rows (DBCC TRACESTATUS(-1)
    /// only ever lists flags that are on), so a naive MAX(capture_time) anchor - the fix DB_CONFIG uses - would
    /// silently fall back to the older capture that still had one on, reporting a flag that is actually off.
    /// The bounded read has no such fallback: with every row for this server older than the 48h window, the
    /// read returns zero rows and no TRACE_FLAGS fact is emitted at all - a real "nothing is on", not a stale one.
    /// </summary>
    [Fact]
    public async Task ServerWhoseOnlyTraceFlagRowIsOutsideTheWindow_EmitsNoTraceFlagsFact_AgainstDevPostgres()
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
            await InsertTraceFlagAsync(connection, TraceFlagAllOffServerId, end.AddDays(-3), 1222, status: true, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(Context(TraceFlagAllOffServerId, end))).ToDictionary(f => f.Key);

            Assert.False(facts.ContainsKey("TRACE_FLAGS"), "a flag last seen on 3 days ago, outside the 48h window, must not resurface as currently on");

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
                [PlanShapeServerId, context.LatestValueStartFor(FileIoStatsCollector.Instance.Name), context.TimeRangeEnd], ct);
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

    /// <summary>
    /// The cadence-aware half: an operator schedules database_size_stats once a day. A flat day would lose its
    /// facts on every pass between the day and the next run; twice the interval keeps them — a sample 25 hours
    /// old still counts, one 49 hours old does not — while a server left at the hourly default loses the same
    /// 25-hour-old sample exactly as before. The drill-down, handed the fact collector's context, lists the same
    /// file the count counted.
    /// </summary>
    [Fact]
    public async Task ACollectorSlowedToDaily_KeepsItsFactForTwoIntervals_ThenLosesIt_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value cadence test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            await InsertOverrideAsync(connection, DailyServerId, "database_size_stats", 1440, ct);
            await InsertOverrideAsync(connection, DailyStaleServerId, "database_size_stats", 1440, ct);

            foreach (var (serverId, age) in new[]
                     {
                         (DailyServerId, TimeSpan.FromHours(25)),
                         (DailyStaleServerId, TimeSpan.FromHours(49)),
                         (DefaultCadenceServerId, TimeSpan.FromHours(25)),
                     })
            {
                await InsertSizeAsync(connection, serverId, end - age, "LiveDb", 1, "ROWS", "LiveDb_data", 20_480, percentGrowth: true, "D:\\", 1_000_000, 150_000, ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var collector = new PgFactCollector(postgres);

            var dailyContext = Context(DailyServerId, end);
            var daily = (await collector.CollectFactsAsync(dailyContext)).ToDictionary(f => f.Key);
            Assert.Equal(end.AddHours(-48), dailyContext.LatestValueStartFor("database_size_stats"));
            Assert.Equal(end.AddHours(-24), dailyContext.LatestValueStartFor("memory_stats"));
            Assert.Equal(1, daily["FILE_AUTOGROWTH_PERCENT"].Value);
            Assert.Equal(0.15, daily["DISK_SPACE"].Value, precision: 6);
            var file = Assert.Single(await AutogrowthFilesAsync(postgres, dailyContext));
            Assert.Equal("LiveDb_data", file.GetProperty("logical_file_name").GetString());

            var stale = (await collector.CollectFactsAsync(Context(DailyStaleServerId, end))).ToDictionary(f => f.Key);
            Assert.False(stale.ContainsKey("FILE_AUTOGROWTH_PERCENT"), "a daily collector's sample two intervals and an hour old was counted");
            Assert.False(stale.ContainsKey("DISK_SPACE"));
            Assert.Empty(await AutogrowthFilesAsync(postgres, Context(DailyStaleServerId, end)));

            var defaults = (await collector.CollectFactsAsync(Context(DefaultCadenceServerId, end))).ToDictionary(f => f.Key);
            Assert.False(defaults.ContainsKey("FILE_AUTOGROWTH_PERCENT"), "the hourly default no longer takes the flat day");
            Assert.False(defaults.ContainsKey("DISK_SPACE"));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// A collector an operator moved to on-load only (frequency 0) writes one capture per connect, so its newest
    /// capture can be days old on a healthy server. Its reads anchor on that capture — the files present at the
    /// last connect, the dropped database's file from the capture before excluded — rather than on a lookback
    /// that would lose the fact a day after every connect; a capture after the window's end stays out.
    /// </summary>
    [Fact]
    public async Task AnOnLoadOnlyCollector_AnchorsOnItsNewestCapture_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value on-load test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            await InsertOverrideAsync(connection, OnLoadServerId, "file_io_stats", 0, ct);
            await InsertOverrideAsync(connection, OnLoadServerId, "memory_stats", 0, ct);

            var older = end.AddDays(-5);
            var newest = end.AddDays(-3);
            await InsertFileIoAsync(connection, OnLoadServerId, older, "LiveDb", "LiveDb_data", 1000, ct);
            await InsertFileIoAsync(connection, OnLoadServerId, older, "GhostDb", "GhostDb_data", 50_000, ct);
            await InsertFileIoAsync(connection, OnLoadServerId, newest, "LiveDb", "LiveDb_data", 1100, ct);
            await InsertFileIoAsync(connection, OnLoadServerId, newest, "LiveDb", "LiveDb_log", 200, ct);
            await InsertFileIoAsync(connection, OnLoadServerId, end.AddHours(1), "LiveDb", "LiveDb_data", 7777, ct);
            await InsertMemoryStatsAsync(connection, OnLoadServerId, newest, 32_768, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = Context(OnLoadServerId, end);
            var facts = (await new PgFactCollector(postgres).CollectFactsAsync(context)).ToDictionary(f => f.Key);

            Assert.Equal(newest, context.LatestValueStartFor("file_io_stats"));
            Assert.Equal(1300.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);
            Assert.Equal(32_768, facts["MEMORY_TOTAL_PHYSICAL_MB"].Value, precision: 6);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// The override read takes both levels the scheduler layers: a fleet-wide row reaches every server, and a
    /// server's own row wins over it — the rule <c>StoreConfigProvider.ResolveSchedule</c> runs the collectors by.
    /// </summary>
    [Fact]
    public async Task AFleetWideOverride_WidensEveryServer_AndAServersOwnRowWins_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value fleet-override test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DeleteSentinelsAsync(connection, ct);

            var end = TruncateToSeconds(DateTime.UtcNow);
            await InsertOverrideAsync(connection, null, FleetOverrideCollector, 1440, ct);
            await InsertOverrideAsync(connection, FleetOverriddenServerId, FleetOverrideCollector, 60, ct);
            await InsertClerkAsync(connection, FleetServerId, end.AddHours(-30), "MEMORYCLERK_SQLBUFFERPOOL", 800, ct);
            await InsertClerkAsync(connection, FleetOverriddenServerId, end.AddHours(-30), "MEMORYCLERK_SQLBUFFERPOOL", 800, ct);

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var collector = new PgFactCollector(postgres);

            var fleet = (await collector.CollectFactsAsync(Context(FleetServerId, end))).ToDictionary(f => f.Key);
            Assert.Equal(800, fleet["MEMORY_CLERKS"].Value, precision: 6);

            var own = (await collector.CollectFactsAsync(Context(FleetOverriddenServerId, end))).ToDictionary(f => f.Key);
            Assert.False(own.ContainsKey("MEMORY_CLERKS"), "the fleet-wide row outranked the server's own");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>
    /// The plan shape at a non-default lookback: database_size_stats scheduled daily, seeded hourly over ten days,
    /// and the shipped read EXPLAINed with the bound the pass stamped — twice the interval, still a plain bound
    /// parameter, so TimescaleDB still excludes every chunk outside it at plan time. Two days span at most three
    /// one-day chunks; the pre-#3896 read, which had no time bound at all, plans every one.
    /// </summary>
    [Fact]
    public async Task ADailyCollectorsRead_StillPlansOnlyTheChunksItsLookbackSpans_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live latest-value plan-shape test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

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
            await InsertOverrideAsync(connection, PlanShapeDailyServerId, "database_size_stats", 1440, ct);
            using (var seed = new NpgsqlCommand(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name,
     total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb)
SELECT 9_389_611_000 + (EXTRACT(EPOCH FROM t)::bigint % 1_000_000) * 10 + f, t, $1, 'latest-value-plan-shape',
       'Db' || f, f, 'ROWS', 'Db' || f || '_data', 20480, true, 10, 'D:\', 1000000, 150000
FROM generate_series($2::timestamp - interval '10 days', $2::timestamp, interval '1 hour') AS t
CROSS JOIN generate_series(1, 3) AS f", connection))
            {
                seed.Parameters.AddWithValue(PlanShapeDailyServerId);
                seed.Parameters.AddWithValue(end);
                await seed.ExecuteNonQueryAsync(ct);
            }

            using (var analyze = new NpgsqlCommand("ANALYZE collect.database_size_stats", connection))
            {
                await analyze.ExecuteNonQueryAsync(ct);
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var context = Context(PlanShapeDailyServerId, end);
            await PgLatestValueBounds.EnsureAsync(postgres, context, null);
            var start = context.LatestValueStartFor("database_size_stats");
            Assert.Equal(end.AddHours(-48), start);

            var shipped = await ExplainAsync(connection, PgFactCollector.FileAutogrowthSql,
                [PlanShapeDailyServerId, start, context.TimeRangeEnd], ct);
            var oracle = await ExplainAsync(connection, UnboundedFileAutogrowthSql, [PlanShapeDailyServerId], ct);

            if (timescaleEnabled)
            {
                var shippedChunks = PlanChunkScans.Count(shipped);
                var oracleChunks = PlanChunkScans.Count(oracle);
                Assert.True(oracleChunks >= 11,
                    $"the pre-#3896 read should plan every seeded chunk (eleven days of data), planned {oracleChunks}:\n{oracle}");
                Assert.True(shippedChunks is >= 1 and <= 3,
                    $"the shipped read planned {shippedChunks} chunks; a 48-hour lookback touches at most three:\n{shipped}");
            }

            var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);
            Assert.Equal(3, Assert.Single(facts, f => f.Key == "FILE_AUTOGROWTH_PERCENT").Value);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteSentinelsAsync);
        }
    }

    /// <summary>The autogrowth read as shipped before #3896 — no time bound at all. An oracle, never run by the
    /// product.</summary>
    private const string UnboundedFileAutogrowthSql = @"
WITH latest AS (
    SELECT database_name, file_id, total_size_mb, is_percent_growth,
           ROW_NUMBER() OVER (PARTITION BY database_name, file_id ORDER BY collection_time DESC) AS rn
    FROM database_size_stats
    WHERE server_id = $1
)
SELECT
    COUNT(*) AS file_count,
    COUNT(DISTINCT database_name) AS database_count
FROM latest
WHERE rn = 1
AND   is_percent_growth = true
AND   total_size_mb >= 10240
AND   database_name NOT IN ('master', 'msdb', 'model', 'tempdb')";

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

        /* An enrichment that found nothing leaves DrillDown null, which is the empty list here. */
        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("autogrowth_percent_files", out var raw))
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

    private static async Task InsertTraceFlagAsync(NpgsqlConnection connection, int serverId, DateTime capturedAt,
        int traceFlag, bool status, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, 'latest-value-lookback-e2e', $4, $5, true, false)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(capturedAt);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(traceFlag);
        command.Parameters.AddWithValue(status);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A sparse schedule override row — <paramref name="serverId"/> null is the fleet-wide one.</summary>
    private static async Task InsertOverrideAsync(NpgsqlConnection connection, int? serverId, string collector,
        int frequencyMinutes, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO config_collector_schedules (server_id, collector_name, frequency_minutes)
VALUES ($1, $2, $3)", connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = (object?)serverId ?? DBNull.Value });
        command.Parameters.AddWithValue(collector);
        command.Parameters.AddWithValue(frequencyMinutes);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteSentinelsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var ids = string.Join(", ", s_serverIds.Select(id => id.ToString(CultureInfo.InvariantCulture)));

        /* The sentinels' own schedule rows, and the one fleet-wide row the fleet test takes over: darlingtest
           belongs to the suite, and a fleet row left behind would widen that collector for every server. */
        using (var schedules = new NpgsqlCommand(
            $"DELETE FROM config_collector_schedules WHERE server_id IN ({ids}) OR (server_id IS NULL AND collector_name = '{FleetOverrideCollector}')",
            connection))
        {
            await schedules.ExecuteNonQueryAsync(ct);
        }

        foreach (var table in s_tables)
        {
            using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id IN ({ids})", connection);
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
}
