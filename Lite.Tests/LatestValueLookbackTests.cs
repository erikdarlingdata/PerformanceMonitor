/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3896, Lite's half: the "latest value" reads — database size, percent autogrowth, disk space, memory clerks,
/// plan cache, memory_stats, and the autogrowth drill-down — take each series' newest sample within
/// <see cref="AnalysisContext.LatestValueLookback"/> of the window's end, and <c>DB_CONFIG</c> counts the newest
/// on-load capture only. The same scenarios as Darling's <c>LatestValueLookbackLivePostgresTests</c>, answered
/// the same way: the Darling suite pins the two SKUs' statements to one text, and this pins that DuckDB gives
/// that text the same answer.
/// </summary>
public sealed class LatestValueLookbackTests : IClassFixture<SharedDuckDbFixture>
{
    private const int LiveServerId = -389_601;
    private const int StaleServerId = -389_602;

    private readonly DuckDbInitializer _duckDb;

    public LatestValueLookbackTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    [Fact]
    public async Task ALiveServer_ReportsOnlyWhatItStillHas_AndTheDrillDownListsTheSameFiles()
    {
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(connection => LatestValueSeed.SeedLiveAndGhostAsync(connection, LiveServerId, end));

        var context = LatestValueSeed.Context(LiveServerId, end);
        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context)).ToDictionary(f => f.Key);

        /* 1,000 + 200, not 51,200 (the dropped database's file, last seen 25 hours ago) and not 7,777 (the
           sample after the window's end). */
        Assert.Equal(1200.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);

        var autogrowth = facts["FILE_AUTOGROWTH_PERCENT"];
        Assert.Equal(1, autogrowth.Value);
        Assert.Equal(1, autogrowth.Metadata["database_count"]);

        var disk = facts["DISK_SPACE"];
        Assert.Equal(0.15, disk.Value, precision: 6);
        Assert.Equal(1, disk.Metadata["volume_count"]);
        Assert.Equal(1_000_000, disk.Metadata["total_volume_mb"]);

        var clerks = facts["MEMORY_CLERKS"];
        Assert.Equal(900, clerks.Value, precision: 6);
        Assert.False(clerks.Metadata.ContainsKey("CACHESTORE_GHOST"), "a clerk last seen 25 hours ago was reported");

        Assert.Equal(60.0, facts["PLAN_CACHE_BLOAT"].Value, precision: 6);
        Assert.Equal(65_536, facts["MEMORY_TOTAL_PHYSICAL_MB"].Value, precision: 6);

        /* The newest on-load capture (five days old) holds the live database only. */
        var dbConfig = facts["DB_CONFIG"];
        Assert.Equal(1, dbConfig.Metadata["database_count"]);
        Assert.Equal(0, dbConfig.Metadata["auto_shrink_on_count"]);
        Assert.Equal(0, dbConfig.Metadata["rcsi_off_count"]);

        var file = Assert.Single(await LatestValueSeed.AutogrowthFilesAsync(new DrillDownCollector(_duckDb), context));
        Assert.Equal("LiveDb", file.GetProperty("database").GetString());
        Assert.Equal("LiveDb_data", file.GetProperty("logical_file_name").GetString());
    }

    [Fact]
    public async Task AServerWhoseNewestSampleIsOlderThanTheLookback_EmitsNoLatestValueFact_ButKeepsItsOnLoadConfig()
    {
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(connection => LatestValueSeed.SeedStaleAsync(connection, StaleServerId, end));

        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(StaleServerId, end)))
            .ToDictionary(f => f.Key);

        foreach (var key in LatestValueSeed.LatestValueFactKeys)
        {
            Assert.False(facts.ContainsKey(key), $"{key} was emitted from a sample 25 hours older than the window's end");
        }

        Assert.Equal(1, facts["DB_CONFIG"].Metadata["database_count"]);
    }

    [Fact]
    public async Task AHistoricalWindow_ReadsTheStateAsItStoodAtItsOwnEnd()
    {
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(connection => LatestValueSeed.SeedLiveAndGhostAsync(connection, LiveServerId, end));

        var context = LatestValueSeed.Context(LiveServerId, end.AddHours(-20));
        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context)).ToDictionary(f => f.Key);

        Assert.Equal(50_000.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);
        Assert.Equal(0.02, facts["DISK_SPACE"].Value, precision: 6);
        Assert.Equal(1, facts["FILE_AUTOGROWTH_PERCENT"].Value);

        var file = Assert.Single(await LatestValueSeed.AutogrowthFilesAsync(new DrillDownCollector(_duckDb), context));
        Assert.Equal("GhostDb", file.GetProperty("database").GetString());
    }

    /// <summary>
    /// The cadence-aware half, through the resolver production wires (<see cref="ScheduleManager.GetFrequencyForStorageServer"/>
    /// over a real <see cref="ServerManager"/>, so the storage-hash to connection-GUID match is under test too).
    /// A server whose database_size_stats an operator scheduled daily keeps its facts for two intervals — a
    /// sample 25 hours old still counts, one 49 hours old does not — while a server left at the hourly default
    /// loses the 25-hour-old sample exactly as before.
    /// </summary>
    [Fact]
    public async Task ACollectorSlowedToDaily_KeepsItsFactForTwoIntervals_ThenLosesIt()
    {
        using var schedules = new ScheduleFixture();
        var daily = schedules.AddServer("DailyServer", ("database_size_stats", 1440));
        var dailyStale = schedules.AddServer("DailyStaleServer", ("database_size_stats", 1440));
        var defaults = schedules.AddServer("DefaultCadenceServer");

        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(async connection =>
        {
            await LatestValueSeed.SeedSizeAsync(connection, daily, end.AddHours(-25));
            await LatestValueSeed.SeedSizeAsync(connection, dailyStale, end.AddHours(-49));
            await LatestValueSeed.SeedSizeAsync(connection, defaults, end.AddHours(-25));
        });

        var collector = new DuckDbFactCollector(_duckDb, schedules.Resolver);

        var dailyContext = LatestValueSeed.Context(daily, end);
        var dailyFacts = (await collector.CollectFactsAsync(dailyContext)).ToDictionary(f => f.Key);
        Assert.Equal(end.AddHours(-48), dailyContext.LatestValueStartFor("database_size_stats"));
        Assert.Equal(end.AddHours(-24), dailyContext.LatestValueStartFor("memory_stats"));
        Assert.Equal(1, dailyFacts["FILE_AUTOGROWTH_PERCENT"].Value);
        Assert.Equal(0.15, dailyFacts["DISK_SPACE"].Value, precision: 6);
        var file = Assert.Single(await LatestValueSeed.AutogrowthFilesAsync(new DrillDownCollector(_duckDb, null, schedules.Resolver), dailyContext));
        Assert.Equal("LiveDb_data", file.GetProperty("logical_file_name").GetString());

        var staleFacts = (await collector.CollectFactsAsync(LatestValueSeed.Context(dailyStale, end))).ToDictionary(f => f.Key);
        Assert.False(staleFacts.ContainsKey("FILE_AUTOGROWTH_PERCENT"), "a daily collector's sample two intervals and an hour old was counted");
        Assert.False(staleFacts.ContainsKey("DISK_SPACE"));

        var defaultFacts = (await collector.CollectFactsAsync(LatestValueSeed.Context(defaults, end))).ToDictionary(f => f.Key);
        Assert.False(defaultFacts.ContainsKey("FILE_AUTOGROWTH_PERCENT"), "the hourly default no longer takes the flat day");
        Assert.False(defaultFacts.ContainsKey("DISK_SPACE"));
    }

    /// <summary>
    /// A collector moved to on-load only (frequency 0) anchors on its newest capture at or before the window's
    /// end, however old: the files present at the last connect, not a ghost from the capture before, and not
    /// "no fact" a day after every connect.
    /// </summary>
    [Fact]
    public async Task AnOnLoadOnlyCollector_AnchorsOnItsNewestCapture()
    {
        using var schedules = new ScheduleFixture();
        var onLoad = schedules.AddServer("OnLoadServer", ("file_io_stats", 0), ("memory_stats", 0));

        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        var older = end.AddDays(-5);
        var newest = end.AddDays(-3);
        await SeedAsync(async connection =>
        {
            await LatestValueSeed.InsertFileIoAsync(connection, onLoad, older, "LiveDb", "LiveDb_data", 1000);
            await LatestValueSeed.InsertFileIoAsync(connection, onLoad, older, "GhostDb", "GhostDb_data", 50_000);
            await LatestValueSeed.InsertFileIoAsync(connection, onLoad, newest, "LiveDb", "LiveDb_data", 1100);
            await LatestValueSeed.InsertFileIoAsync(connection, onLoad, newest, "LiveDb", "LiveDb_log", 200);
            await LatestValueSeed.InsertFileIoAsync(connection, onLoad, end.AddHours(1), "LiveDb", "LiveDb_data", 7777);
            await LatestValueSeed.InsertMemoryStatsAsync(connection, onLoad, newest, 32_768);
        });

        var context = LatestValueSeed.Context(onLoad, end);
        var facts = (await new DuckDbFactCollector(_duckDb, schedules.Resolver).CollectFactsAsync(context)).ToDictionary(f => f.Key);

        Assert.Equal(newest, context.LatestValueStartFor("file_io_stats"));
        Assert.Equal(1300.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);
        Assert.Equal(32_768, facts["MEMORY_TOTAL_PHYSICAL_MB"].Value, precision: 6);
    }

    /// <summary>
    /// #3929, Lite's half: mirrors Darling's TraceFlagTurnedOffAfterLastConnect live test. trace_flags is
    /// on-load by default (frequency 0) but cannot take the newest-capture anchor AnOnLoadOnlyCollector above
    /// uses - a capture that finds every flag off writes ZERO rows, so anchoring on the newest capture would
    /// fall back to an older, stale-ON one. It takes the on-load-aware lookback instead (two
    /// OnLoadRecaptureMinutes cycles - 48h at the shipped daily default): flag 3604's only row is 3 days old
    /// (outside the window, and missing from the two most recent captures at -1 day and -6 hours) so it must
    /// not appear; flag 2371, on 6 hours ago, must.
    /// </summary>
    [Fact]
    public async Task TraceFlagTurnedOffAfterLastConnect_DropsOutOfTheOnLoadWindow_WhileAStillOnFlagStays()
    {
        const int ServerId = -389_612;
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(async connection =>
        {
            await LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddDays(-3), 3604, status: true);
            await LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddDays(-1), 2371, status: true);
            await LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddHours(-6), 2371, status: true);
        });

        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(ServerId, end)))
            .ToDictionary(f => f.Key);

        var traceFlags = facts["TRACE_FLAGS"];
        Assert.Equal(1, traceFlags.Metadata["flag_count"]);
        Assert.True(traceFlags.Metadata.ContainsKey("TF_2371"), "flag 2371 (still on 6 hours ago) should be reported");
        Assert.False(traceFlags.Metadata.ContainsKey("TF_3604"), "flag 3604 (off for two full captures, last ON row 3 days old) should have cleared");
    }

    /// <summary>
    /// #3929: the flags are the window's NEWEST capture, not each flag's newest row. Both flags were on in
    /// yesterday's capture, inside the window; this morning's lists only 2371. A per-flag read would keep 3604
    /// until yesterday's row left the window. The newest capture drops it now.
    /// </summary>
    [Fact]
    public async Task AFlagTurnedOffWhileAnotherStaysOn_DropsOutAtTheNextCapture_NotWhenItsLastRowLeavesTheWindow()
    {
        const int ServerId = -389_613;
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(async connection =>
        {
            await LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddDays(-1), 3604, status: true);
            await LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddDays(-1), 2371, status: true);
            await LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddHours(-6), 2371, status: true);
        });

        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(ServerId, end)))
            .ToDictionary(f => f.Key);

        var traceFlags = facts["TRACE_FLAGS"];
        Assert.Equal(1, traceFlags.Metadata["flag_count"]);
        Assert.True(traceFlags.Metadata.ContainsKey("TF_2371"), "flag 2371 is on in the newest capture");
        Assert.False(traceFlags.Metadata.ContainsKey("TF_3604"),
            "flag 3604 is missing from the newest capture, so it is off, even though yesterday's ON row is still inside the window");
    }

    /// <summary>
    /// #3929's sharpest edge case, Lite's half: a capture that finds every flag off writes ZERO rows, so a
    /// MAX(capture_time) anchor would silently fall back to an older capture that still had one on. With every
    /// row for this server older than the 48h window, the bounded read returns zero rows and no TRACE_FLAGS
    /// fact is emitted at all.
    /// </summary>
    [Fact]
    public async Task ServerWhoseOnlyTraceFlagRowIsOutsideTheWindow_EmitsNoTraceFlagsFact()
    {
        const int ServerId = -389_613;
        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        await SeedAsync(connection => LatestValueSeed.InsertTraceFlagAsync(connection, ServerId, end.AddDays(-3), 1222, status: true));

        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(LatestValueSeed.Context(ServerId, end)))
            .ToDictionary(f => f.Key);

        Assert.False(facts.ContainsKey("TRACE_FLAGS"), "a flag last seen on 3 days ago, outside the 48h window, must not resurface as currently on");
    }

    private async Task SeedAsync(Func<DuckDBConnection, Task> seed)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await seed(connection);
    }

    /// <summary>
    /// A throwaway config directory holding a real <see cref="ServerManager"/> and <see cref="ScheduleManager"/>,
    /// and the resolver production builds over them. Windows-auth servers, so nothing touches the credential store.
    /// </summary>
    private sealed class ScheduleFixture : IDisposable
    {
        private readonly string _configDir;
        private readonly ServerManager _servers;
        private readonly ScheduleManager _schedules;

        public ScheduleFixture()
        {
            _configDir = Path.Combine(Path.GetTempPath(), "LatestValueCadence_" + Guid.NewGuid().ToString("N")[..8]);
            Directory.CreateDirectory(_configDir);
            _servers = new ServerManager(_configDir);
            _schedules = new ScheduleManager(_configDir);
            Resolver = (serverId, collector) => _schedules.GetFrequencyForStorageServer(_servers, serverId, collector);
        }

        public Func<int, string, int?> Resolver { get; }

        /// <summary>Adds a server, with a per-server schedule when any cadence is overridden, and returns the
        /// storage id the analysis keys it by.</summary>
        public int AddServer(string name, params (string Collector, int FrequencyMinutes)[] overrides)
        {
            var server = new ServerConnection { ServerName = name, DisplayName = name };
            _servers.AddServer(server);

            if (overrides.Length > 0)
            {
                var schedule = ScheduleManager.GetDefaultSchedules();
                foreach (var (collector, frequency) in overrides)
                {
                    schedule.First(s => s.Name == collector).FrequencyMinutes = frequency;
                }

                _schedules.SetScheduleForServer(server.Id, schedule);
            }

            return RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
        }

        public void Dispose()
        {
            try { if (Directory.Exists(_configDir)) Directory.Delete(_configDir, recursive: true); }
            catch { /* best-effort cleanup */ }
        }
    }
}

/// <summary>
/// The same reads across the parquet archive, which is where Lite's version of the defect lived: the
/// <c>v_</c> views UNION the hot table with every archived parquet file, so an unbounded latest-value read scanned
/// the server's whole archive and a dropped database's files counted until the archive aged them out. Its own
/// database and archive folder, like <c>AnalysisDataSpanTests</c>, because the archive views are created from
/// the files present when <see cref="DuckDbInitializer.CreateArchiveViewsAsync"/> runs.
/// </summary>
public sealed class LatestValueLookbackArchiveTests : IDisposable
{
    private const int ServerId = -389_604;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archivePath;

    public LatestValueLookbackArchiveTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archivePath = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archivePath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    /// <summary>
    /// The 512 MB reset shape (#1809): everything archived to parquet and the hot tables emptied. The live
    /// database's recent samples are now parquet rows too, and must still count; the dropped database's must
    /// not, from parquet any more than from the hot table.
    /// </summary>
    [Fact]
    public async Task AGhostInTheArchive_IsNotCounted_AndALiveSeriesArchivedMinutesAgoStillIs()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        var end = LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            await LatestValueSeed.SeedLiveAndGhostAsync(connection, ServerId, end);

            foreach (var table in LatestValueSeed.Tables)
            {
                var parquet = Path.Combine(_archivePath, $"20260101_0000_{table}.parquet").Replace("\\", "/");
                await LatestValueSeed.ExecuteAsync(connection, $"COPY {table} TO '{parquet}' (FORMAT PARQUET)");
                await LatestValueSeed.ExecuteAsync(connection, $"DELETE FROM {table}");
            }
        }

        await initializer.CreateArchiveViewsAsync();

        var context = LatestValueSeed.Context(ServerId, end);
        var facts = (await new DuckDbFactCollector(initializer).CollectFactsAsync(context)).ToDictionary(f => f.Key);

        Assert.Equal(1200.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);
        Assert.Equal(0.15, facts["DISK_SPACE"].Value, precision: 6);
        Assert.Equal(1, facts["FILE_AUTOGROWTH_PERCENT"].Value);
        Assert.False(facts["MEMORY_CLERKS"].Metadata.ContainsKey("CACHESTORE_GHOST"));
        Assert.Equal(65_536, facts["MEMORY_TOTAL_PHYSICAL_MB"].Value, precision: 6);
        Assert.Equal(1, facts["DB_CONFIG"].Metadata["database_count"]);

        var file = Assert.Single(await LatestValueSeed.AutogrowthFilesAsync(new DrillDownCollector(initializer), context));
        Assert.Equal("LiveDb", file.GetProperty("database").GetString());
    }
}

/// <summary>
/// The #3896 scenario, shared by both classes above: a live database sampled minutes before the window's end,
/// beside a database dropped 25 hours before it — every table a latest-value read touches — plus one sample
/// AFTER the window's end that the upper bound must keep out, and two on-load config captures.
/// </summary>
internal static class LatestValueSeed
{
    public static readonly string[] Tables =
    {
        "file_io_stats", "database_size_stats", "memory_clerks", "plan_cache_stats", "memory_stats", "database_config",
    };

    public static readonly string[] LatestValueFactKeys =
    {
        "DATABASE_TOTAL_SIZE_MB", "FILE_AUTOGROWTH_PERCENT", "DISK_SPACE", "MEMORY_CLERKS",
        "PLAN_CACHE_BLOAT", "MEMORY_TOTAL_PHYSICAL_MB", "MEMORY_BUFFER_POOL_MB", "MEMORY_TARGET_MB",
    };

    private static long s_nextId = -38_960_000;

    public static AnalysisContext Context(int serverId, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = "latest-value-lookback",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
    };

    public static async Task SeedLiveAndGhostAsync(DuckDBConnection connection, int serverId, DateTime end)
    {
        var ghost = end.AddHours(-25);

        await InsertFileIoAsync(connection, serverId, end.AddMinutes(-65), "LiveDb", "LiveDb_data", 900);
        await InsertFileIoAsync(connection, serverId, end.AddMinutes(-5), "LiveDb", "LiveDb_data", 1000);
        await InsertFileIoAsync(connection, serverId, end.AddMinutes(-5), "LiveDb", "LiveDb_log", 200);
        await InsertFileIoAsync(connection, serverId, ghost, "GhostDb", "GhostDb_data", 50_000);
        await InsertFileIoAsync(connection, serverId, end.AddMinutes(30), "LiveDb", "LiveDb_data", 7777);

        await InsertSizeAsync(connection, serverId, end.AddMinutes(-30), "LiveDb", 1, "ROWS", "LiveDb_data", 20_480, true, "D:\\", 1_000_000, 150_000);
        await InsertSizeAsync(connection, serverId, end.AddMinutes(-30), "LiveDb", 2, "LOG", "LiveDb_log", 512, false, "D:\\", 1_000_000, 150_000);
        await InsertSizeAsync(connection, serverId, ghost, "GhostDb", 1, "ROWS", "GhostDb_data", 40_960, true, "E:\\", 500_000, 10_000);

        await InsertClerkAsync(connection, serverId, end.AddMinutes(-5), "MEMORYCLERK_SQLBUFFERPOOL", 800);
        await InsertClerkAsync(connection, serverId, end.AddMinutes(-5), "CACHESTORE_SQLCP", 100);
        await InsertClerkAsync(connection, serverId, ghost, "CACHESTORE_GHOST", 5000);

        await InsertPlanCacheAsync(connection, serverId, end.AddMinutes(-10), totalPlans: 100, singleUse: 60);
        await InsertMemoryStatsAsync(connection, serverId, end.AddMinutes(-2), 65_536);

        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-10), "LiveDb", autoShrink: false, rcsiOn: true);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-10), "GhostDb", autoShrink: true, rcsiOn: false);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-5), "LiveDb", autoShrink: false, rcsiOn: true);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-5), "master", autoShrink: false, rcsiOn: false);
    }

    /// <summary>One large percent-growth file on a 15%-free volume, sampled at <paramref name="at"/>.</summary>
    public static Task SeedSizeAsync(DuckDBConnection connection, int serverId, DateTime at) =>
        InsertSizeAsync(connection, serverId, at, "LiveDb", 1, "ROWS", "LiveDb_data", 20_480, true, "D:\\", 1_000_000, 150_000);

    /// <summary>Every cadenced series last sampled 25 hours before the window's end, and an on-load capture from
    /// five days before it.</summary>
    public static async Task SeedStaleAsync(DuckDBConnection connection, int serverId, DateTime end)
    {
        var stale = end.AddHours(-25);
        await InsertFileIoAsync(connection, serverId, stale, "StaleDb", "StaleDb_data", 4096);
        await InsertSizeAsync(connection, serverId, stale, "StaleDb", 1, "ROWS", "StaleDb_data", 20_480, true, "D:\\", 1_000_000, 10_000);
        await InsertClerkAsync(connection, serverId, stale, "MEMORYCLERK_SQLBUFFERPOOL", 800);
        await InsertPlanCacheAsync(connection, serverId, stale, totalPlans: 100, singleUse: 90);
        await InsertMemoryStatsAsync(connection, serverId, stale, 32_768);
        await InsertDatabaseConfigAsync(connection, serverId, end.AddDays(-5), "StaleDb", autoShrink: false, rcsiOn: true);
    }

    public static async Task<List<JsonElement>> AutogrowthFilesAsync(DrillDownCollector collector, AnalysisContext context)
    {
        var finding = new AnalysisFinding
        {
            ServerId = context.ServerId,
            RootFactKey = "FILE_AUTOGROWTH_PERCENT",
            StoryPath = "FILE_AUTOGROWTH_PERCENT",
            PathKeys = ["FILE_AUTOGROWTH_PERCENT"],
            Severity = 0.3,
        };

        await collector.EnrichFindingsAsync([finding], context);

        /* An enrichment that found nothing leaves DrillDown null, which is the empty list here. */
        if (finding.DrillDown is null || !finding.DrillDown.TryGetValue("autogrowth_percent_files", out var raw))
        {
            return [];
        }

        return [.. JsonSerializer.SerializeToElement(raw).EnumerateArray()];
    }

    public static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    public static async Task ExecuteAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertAsync(DuckDBConnection connection, string sql, params object[] values)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    public static Task InsertFileIoAsync(DuckDBConnection connection, int serverId, DateTime at,
        string database, string file, decimal sizeMb) =>
        InsertAsync(connection, @"
INSERT INTO file_io_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES ($1, $2, $3, 'latest-value-lookback', $4, $5, 'ROWS', $6)",
            Interlocked.Decrement(ref s_nextId), at, serverId, database, file, sizeMb);

    private static Task InsertSizeAsync(DuckDBConnection connection, int serverId, DateTime at,
        string database, int fileId, string fileType, string file, decimal totalMb, bool percentGrowth,
        string volume, decimal volumeTotalMb, decimal volumeFreeMb) =>
        InsertAsync(connection, @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, file_id, file_type_desc, file_name,
     total_size_mb, is_percent_growth, growth_pct, volume_mount_point, volume_total_mb, volume_free_mb)
VALUES ($1, $2, $3, 'latest-value-lookback', $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)",
            Interlocked.Decrement(ref s_nextId), at, serverId, database, fileId, fileType, file, totalMb, percentGrowth, percentGrowth ? 10 : 0,
            volume, volumeTotalMb, volumeFreeMb);

    private static Task InsertClerkAsync(DuckDBConnection connection, int serverId, DateTime at, string clerk, decimal memoryMb) =>
        InsertAsync(connection, @"
INSERT INTO memory_clerks (collection_id, collection_time, server_id, server_name, clerk_type, memory_mb)
VALUES ($1, $2, $3, 'latest-value-lookback', $4, $5)",
            Interlocked.Decrement(ref s_nextId), at, serverId, clerk, memoryMb);

    private static Task InsertPlanCacheAsync(DuckDBConnection connection, int serverId, DateTime at, int totalPlans, int singleUse) =>
        InsertAsync(connection, @"
INSERT INTO plan_cache_stats
    (collection_id, collection_time, server_id, server_name, cacheobjtype, objtype,
     total_plans, single_use_plans, total_size_mb, single_use_size_mb)
VALUES ($1, $2, $3, 'latest-value-lookback', 'Compiled Plan', 'Adhoc', $4, $5, 400, 300)",
            Interlocked.Decrement(ref s_nextId), at, serverId, totalPlans, singleUse);

    public static Task InsertMemoryStatsAsync(DuckDBConnection connection, int serverId, DateTime at, decimal physicalMb) =>
        InsertAsync(connection, @"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_physical_memory_mb, buffer_pool_mb, target_server_memory_mb)
VALUES ($1, $2, $3, 'latest-value-lookback', $4, $5, $6)",
            Interlocked.Decrement(ref s_nextId), at, serverId, physicalMb, physicalMb / 2, physicalMb * 3 / 4);

    private static Task InsertDatabaseConfigAsync(DuckDBConnection connection, int serverId, DateTime capturedAt,
        string database, bool autoShrink, bool rcsiOn) =>
        InsertAsync(connection, @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name, recovery_model,
     is_auto_shrink_on, is_auto_close_on, is_read_committed_snapshot_on, is_auto_create_stats_on,
     is_auto_update_stats_on, page_verify_option, is_query_store_on)
VALUES ($1, $2, $3, 'latest-value-lookback', $4, 'FULL', $5, false, $6, true, true, 'CHECKSUM', true)",
            Interlocked.Decrement(ref s_nextId), capturedAt, serverId, database, autoShrink, rcsiOn);

    public static Task InsertTraceFlagAsync(DuckDBConnection connection, int serverId, DateTime capturedAt, int traceFlag, bool status) =>
        InsertAsync(connection, @"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, 'latest-value-lookback', $4, $5, true, false)",
            Interlocked.Decrement(ref s_nextId), capturedAt, serverId, traceFlag, status);
}
