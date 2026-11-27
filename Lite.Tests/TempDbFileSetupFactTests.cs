/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8883 on Lite's DuckDB store: the TEMPDB_FILE_SETUP fact. Its value is how many tempdb file setup checks the server fails (too
/// few data files for the schedulers, unequal data file sizes, unequal data file growth, percent growth, AUTOGROW_ALL_FILES off,
/// more than one log file), and there is no fact when it fails none. The same scenario table runs on the Darling store in
/// <c>TempDbFileSetupFactLiveTests</c>.
/// </summary>
public sealed class TempDbFileSetupFactTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;
    private int _nextServerId = 888_300;

    public TempDbFileSetupFactTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>One file of a tempdb collection. Fixed 256 MB growth and AUTOGROW_ALL_FILES on, unless the scenario says otherwise.</summary>
    private sealed record File(string Type, double Size, double Step = 256, bool Percent = false, double? Pct = null, bool? AllFiles = true, int DatabaseId = 2);

    private static File Data(double size = 1024, double step = 256, bool percent = false, double? pct = null, bool? allFiles = true) =>
        new("ROWS", size, step, percent, pct, allFiles);

    private static File Log(bool percent = false) => percent ? new("LOG", 512, 64, true, 10, null) : new("LOG", 512, 64, false, null, null);

    private static IEnumerable<File> DataFiles(int count) => Enumerable.Range(0, count).Select(_ => Data());

    private static double Unix(DateTime naiveUtc) =>
        new DateTimeOffset(DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc)).ToUnixTimeSeconds();

    private static DateTime Clock(int minutesAgo)
    {
        var t = DateTime.UtcNow.AddMinutes(-minutesAgo);
        return DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
    }

    private async Task<DuckDBConnection> ConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync(TestContext.Current.CancellationToken);
        }
        return _seedConn;
    }

    private async Task SeedFilesAsync(int serverId, DateTime collectionTime, IEnumerable<File> files)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await ConnectionAsync();
        var fileId = 1;
        foreach (var f in files)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb, auto_growth_mb, max_size_mb, is_percent_growth, growth_pct, is_autogrow_all_files)
VALUES ($1, $2, $3, 'srv', $4, $5, $6, $7, $8, $9, $10, 1, $11, -1, $12, $13, $14)";
            cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
            cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = f.DatabaseId == 2 ? "tempdb" : "userdb" });
            cmd.Parameters.Add(new DuckDBParameter { Value = f.DatabaseId });
            cmd.Parameters.Add(new DuckDBParameter { Value = fileId });
            cmd.Parameters.Add(new DuckDBParameter { Value = f.Type });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"file{fileId}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = $"T:\\file{fileId}" });
            cmd.Parameters.Add(new DuckDBParameter { Value = f.Size });
            cmd.Parameters.Add(new DuckDBParameter { Value = f.Step });
            cmd.Parameters.Add(new DuckDBParameter { Value = f.Percent });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)f.Pct ?? DBNull.Value });
            cmd.Parameters.Add(new DuckDBParameter { Value = (object?)f.AllFiles ?? DBNull.Value });
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            fileId++;
        }
    }

    private async Task SeedSchedulersAsync(int serverId, DateTime collectionTime, int schedulers)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await ConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO cpu_scheduler_stats
            (collection_id, collection_time, server_id, server_name,
             max_workers_count, scheduler_count, cpu_count,
             total_runnable_tasks_count, total_work_queue_count, total_current_workers_count,
             avg_runnable_tasks_count, total_active_request_count, total_queued_request_count,
             total_blocked_task_count, total_active_parallel_thread_count,
             runnable_request_count, total_request_count, runnable_percent,
             worker_thread_exhaustion_warning, runnable_tasks_warning, blocked_tasks_warning,
             queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb,
             system_memory_state_desc, physical_memory_pressure_warning,
             total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
            VALUES ($1, $2, $3, 'srv', 512, $4, $4, 0, 0, 128, 0.5, 2, 0, 0, 0, 3, 20, 12.5,
                    false, false, false, false, 67108864, 8388608, 'ok', false, 2, 2, 0, false)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = collectionTime });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = schedulers });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static AnalysisContext Context(int serverId, DateTime end) => new()
    {
        ServerId = serverId,
        ServerName = "tempdb-file-setup",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
    };

    /// <summary>Seeds one tempdb collection (and a scheduler count unless it is null) for a fresh server, and collects its facts.</summary>
    private async Task<(Fact? Fact, int ServerId, AnalysisContext Context)> CollectAsync(IEnumerable<File> files, int? schedulers)
    {
        var serverId = _nextServerId++;
        var end = Clock(0);
        await SeedFilesAsync(serverId, Clock(30), files);
        if (schedulers is not null)
            await SeedSchedulersAsync(serverId, Clock(20), schedulers.Value);

        var context = Context(serverId, end);
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        return (facts.SingleOrDefault(f => f.Key == "TEMPDB_FILE_SETUP"), serverId, context);
    }

    private static void AssertOnlyFails(Fact? fact, params string[] failed)
    {
        Assert.NotNull(fact);
        Assert.Equal("config", fact!.Source);
        Assert.Equal(failed.Length, fact.Value);
        var all = new[]
        {
            "check_data_file_count", "check_data_file_sizes", "check_data_file_growth", "check_percent_growth",
            "check_autogrow_all_files", "check_log_file_count",
        };
        foreach (var check in all)
        {
            Assert.Equal(failed.Contains(check) ? 1.0 : 0.0, fact.Metadata.GetValueOrDefault(check, 0.0));
        }
    }

    [Fact]
    public async Task ASetupThatPassesEveryCheck_HasNoFact()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(8).Append(Log()), schedulers: 16);
        Assert.Null(fact);
    }

    [Fact]
    public async Task FewerDataFilesThanTheLesserOfEightAndTheSchedulers_FailsOnlyTheCountCheck()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(4).Append(Log()), schedulers: 16);

        AssertOnlyFails(fact, "check_data_file_count");
        Assert.Equal(4, fact!.Metadata["data_file_count"]);
        Assert.Equal(16, fact.Metadata["scheduler_count"]);
        Assert.Equal(8, fact.Metadata["recommended_data_file_count"]);
    }

    [Fact]
    public async Task OnAServerWithTwoSchedulers_TwoDataFilesAreEnough()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(2).Append(Log()), schedulers: 2);
        Assert.Null(fact);
    }

    [Fact]
    public async Task MoreSchedulersThanEight_StillAsksForEight()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(8).Append(Log()), schedulers: 64);
        Assert.Null(fact);
    }

    [Fact]
    public async Task DataFilesOfUnequalSize_FailTheSizeCheckAlone_AndOnePercentOfTheLargestIsTheLine()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(7).Append(Data(size: 2048)).Append(Log()), schedulers: 8);

        AssertOnlyFails(fact, "check_data_file_sizes");
        Assert.Equal(2048, fact!.Metadata["data_file_largest_mb"]);
        Assert.Equal(1024, fact.Metadata["data_file_smallest_mb"]);

        /* 1,000 against 1,009 MB is under one percent of the largest: the same size for this check. */
        var (close, _, _) = await CollectAsync(new[] { Data(size: 1000), Data(size: 1009), Log() }, schedulers: 2);
        Assert.Null(close);
    }

    [Fact]
    public async Task DataFilesWithDifferentGrowth_FailTheGrowthCheckAlone()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(7).Append(Data(step: 512)).Append(Log()), schedulers: 8);

        AssertOnlyFails(fact, "check_data_file_growth");
        Assert.Equal(2, fact!.Metadata["data_file_growth_setting_count"]);
    }

    [Fact]
    public async Task PercentGrowthOnTheLog_FailsThePercentCheckAlone()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(8).Append(Log(percent: true)), schedulers: 8);

        AssertOnlyFails(fact, "check_percent_growth");
        Assert.Equal(1, fact!.Metadata["percent_growth_file_count"]);
    }

    [Fact]
    public async Task PercentGrowthOnEveryDataFile_IsOneSettingAndOneFailedCheck()
    {
        var (fact, _, _) = await CollectAsync(Enumerable.Range(0, 8).Select(_ => Data(percent: true, pct: 10, step: 0)).Append(Log()), schedulers: 8);

        AssertOnlyFails(fact, "check_percent_growth");
        Assert.Equal(8, fact!.Metadata["percent_growth_file_count"]);
        Assert.Equal(1, fact.Metadata["data_file_growth_setting_count"]);
    }

    [Fact]
    public async Task AutogrowAllFilesOff_FailsThatCheckAlone()
    {
        var (fact, _, _) = await CollectAsync(Enumerable.Range(0, 8).Select(_ => Data(allFiles: false)).Append(Log()), schedulers: 8);

        AssertOnlyFails(fact, "check_autogrow_all_files");
        Assert.Equal(8, fact!.Metadata["autogrow_all_files_off_count"]);
    }

    [Fact]
    public async Task AnUnknownAutogrowFlag_IsSkipped()
    {
        var (fact, _, _) = await CollectAsync(Enumerable.Range(0, 8).Select(_ => Data(allFiles: null)).Append(Log()), schedulers: 8);
        Assert.Null(fact);
    }

    [Fact]
    public async Task MoreThanOneLogFile_FailsThatCheckAlone()
    {
        var (fact, _, _) = await CollectAsync(DataFiles(8).Append(Log()).Append(Log()), schedulers: 8);

        AssertOnlyFails(fact, "check_log_file_count");
        Assert.Equal(2, fact!.Metadata["log_file_count"]);
    }

    [Fact]
    public async Task AMissingSchedulerCount_SkipsTheCountCheckOnly()
    {
        /* Two data files would fail the count check against 16 schedulers; with none collected it passes. */
        var (passing, _, _) = await CollectAsync(DataFiles(2).Append(Log()), schedulers: null);
        Assert.Null(passing);

        /* The other checks still run, and the fact does not claim a scheduler count. */
        var (failing, _, _) = await CollectAsync(DataFiles(2).Append(Log()).Append(Log()), schedulers: null);
        AssertOnlyFails(failing, "check_log_file_count");
        Assert.False(failing!.Metadata.ContainsKey("scheduler_count"));
        Assert.False(failing.Metadata.ContainsKey("check_data_file_count"));
    }

    [Fact]
    public async Task EveryCheckFailing_CountsSix_AndTheEngineRootsAnAdvisoryCardWithOneFixPerCheck()
    {
        var files = new[] { Data(size: 1024, step: 256), Data(size: 2048, step: 512, percent: true, pct: 10, allFiles: false), Log(), Log() };
        var (fact, _, _) = await CollectAsync(files, schedulers: 16);

        AssertOnlyFails(fact, "check_data_file_count", "check_data_file_sizes", "check_data_file_growth", "check_percent_growth", "check_autogrow_all_files", "check_log_file_count");

        var facts = new List<Fact> { fact! };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.3, fact!.BaseSeverity, precision: 6);

        var story = Assert.Single(new InferenceEngine(new RelationshipGraph()).BuildStories(facts));
        Assert.Equal("TEMPDB_FILE_SETUP", story.RootFactKey);

        var advice = FactAdvice.Compose("TEMPDB_FILE_SETUP", facts.ToDictionary(f => f.Key));
        Assert.NotNull(advice);
        Assert.Contains("tempdb has 2 data files and the server has 16 schedulers. Add data files up to 8, all the same size and growth.", advice!.Remediation);
        Assert.Contains("the largest is 2,048 MB and the smallest is 1,024 MB", advice.Remediation);
        Assert.Contains("2 different growth settings", advice.Remediation);
        Assert.Contains("1 tempdb file grows by a percentage", advice.Remediation);
        Assert.Contains("AUTOGROW_ALL_FILES is off on 1 tempdb data file", advice.Remediation);
        Assert.Contains("tempdb has 2 log files", advice.Remediation);
        Assert.Contains("fails 6 checks", advice.Headline);
    }

    [Fact]
    public async Task AServerWithNoTempDbRows_HasNoFact()
    {
        /* An Azure SQL Database stores none. Another database's files in the store are not tempdb's. */
        var (fact, _, _) = await CollectAsync(new[] { new File("ROWS", 1024, DatabaseId: 7), new File("LOG", 512, DatabaseId: 7), new File("LOG", 512, DatabaseId: 7) }, schedulers: 16);
        Assert.Null(fact);
    }

    [Fact]
    public async Task OnlyTheNewestTempDbCollectionCounts()
    {
        var serverId = _nextServerId++;
        var end = Clock(0);
        await SeedFilesAsync(serverId, Clock(90), DataFiles(1).Append(Log()).Append(Log()));
        await SeedFilesAsync(serverId, Clock(30), DataFiles(8).Append(Log()));
        await SeedSchedulersAsync(serverId, Clock(20), 8);

        var healed = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(serverId, end))).SingleOrDefault(f => f.Key == "TEMPDB_FILE_SETUP");
        Assert.Null(healed);

        var brokenId = _nextServerId++;
        await SeedFilesAsync(brokenId, Clock(90), DataFiles(8).Append(Log()));
        var newestFiles = Clock(30);
        var newestSchedulers = Clock(20);
        await SeedFilesAsync(brokenId, newestFiles, DataFiles(8).Append(Log()).Append(Log()));
        await SeedSchedulersAsync(brokenId, newestSchedulers, 8);

        var broken = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(brokenId, end))).SingleOrDefault(f => f.Key == "TEMPDB_FILE_SETUP");
        AssertOnlyFails(broken, "check_log_file_count");

        /* The fact says WHEN: the newest collections' times, not the older one's. */
        Assert.Equal(Unix(newestFiles), broken!.Metadata[TempDbFileSetup.MetaFilesCollectedUnix]);
        Assert.Equal(Unix(newestSchedulers), broken.Metadata[TempDbFileSetup.MetaSchedulerCollectedUnix]);
    }

    [Fact]
    public async Task TheSchedulerCount_IsBoundedByItsOwnCollectorsLookback_NotTheDatabaseSizeCollectors()
    {
        /* The database size collector runs weekly, so its lookback is two weeks. A scheduler count from two days ago is outside
           the scheduler collector's own day, and the read must not accept it: with no count the file-count check is skipped. */
        var serverId = _nextServerId++;
        var end = Clock(0);
        await SeedFilesAsync(serverId, Clock(30), DataFiles(1).Append(Log()));
        await SeedSchedulersAsync(serverId, Clock(48 * 60), 16);

        var context = Context(serverId, end);
        context.LatestValueStarts = new Dictionary<string, DateTime>
        {
            ["database_size_stats"] = end.AddDays(-14),
            ["cpu_scheduler_stats"] = end.AddHours(-24),
        };

        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        Assert.Null(facts.SingleOrDefault(f => f.Key == "TEMPDB_FILE_SETUP"));
    }

    [Fact]
    public async Task TheDrillDown_ListsEveryTempDbFileWithItsSizeAndGrowth()
    {
        var files = new[] { Data(size: 1024, step: 256), Data(size: 2048, step: 0, percent: true, pct: 10), Log() };
        var (_, serverId, context) = await CollectAsync(files, schedulers: 16);

        var finding = new AnalysisFinding
        {
            ServerId = serverId,
            RootFactKey = "TEMPDB_FILE_SETUP",
            StoryPath = "TEMPDB_FILE_SETUP",
            PathKeys = ["TEMPDB_FILE_SETUP"],
            Severity = 0.3,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        var rows = JsonSerializer.SerializeToElement(finding.DrillDown!["tempdb_files"]).EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);
        Assert.Equal("ROWS", rows[0].GetProperty("file_type").GetString());
        Assert.Equal("256 MB", rows[0].GetProperty("growth").GetString());
        Assert.Equal(2048, rows[1].GetProperty("total_size_mb").GetDouble());
        Assert.Equal("10%", rows[1].GetProperty("growth").GetString());
        Assert.Equal("LOG", rows[2].GetProperty("file_type").GetString());
    }

    [Fact]
    public async Task TheDrillDown_SaysWhichCollectionItListsFilesFrom()
    {
        var serverId = _nextServerId++;
        await SeedFilesAsync(serverId, Clock(90), DataFiles(1).Append(Log()));
        var newest = Clock(30);
        await SeedFilesAsync(serverId, newest, DataFiles(2).Append(Log()));
        var context = Context(serverId, Clock(0));

        var finding = new AnalysisFinding
        {
            ServerId = serverId,
            RootFactKey = "TEMPDB_FILE_SETUP",
            StoryPath = "TEMPDB_FILE_SETUP",
            PathKeys = ["TEMPDB_FILE_SETUP"],
            Severity = 0.3,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        var rows = JsonSerializer.SerializeToElement(finding.DrillDown!["tempdb_files"]).EnumerateArray().ToList();
        Assert.Equal(3, rows.Count);
        Assert.All(rows, row => Assert.Equal(newest, row.GetProperty("collected_at").GetDateTime()));
    }
}
