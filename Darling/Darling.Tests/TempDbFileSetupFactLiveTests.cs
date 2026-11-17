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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres. */

/// <summary>
/// #8883 on a live store: the TEMPDB_FILE_SETUP fact. Its value is how many tempdb file setup checks the server fails (too few data
/// files for the schedulers, unequal data file sizes, unequal data file growth, percent growth, AUTOGROW_ALL_FILES off, more than one
/// log file), and there is no fact when it fails none. The same scenario table runs on Lite's DuckDB store in
/// <c>TempDbFileSetupFactTests</c>.
/// </summary>
public sealed class TempDbFileSetupFactLiveTests
{
    private const int FirstServerId = -888300;

    private static readonly string[] AllChecks =
    {
        "check_data_file_count", "check_data_file_sizes", "check_data_file_growth", "check_percent_growth",
        "check_autogrow_all_files", "check_log_file_count",
    };

    private sealed record File(string Type, double Size, double Step = 256, bool Percent = false, double? Pct = null, bool? AllFiles = true, int DatabaseId = 2);

    private static File Data(double size = 1024, double step = 256, bool percent = false, double? pct = null, bool? allFiles = true) =>
        new("ROWS", size, step, percent, pct, allFiles);

    private static File Log(bool percent = false) => percent ? new("LOG", 512, 64, true, 10, null) : new("LOG", 512, 64, false, null, null);

    private static IEnumerable<File> DataFiles(int count) => Enumerable.Range(0, count).Select(_ => Data());

    private static double Unix(DateTime naiveUtc) =>
        new DateTimeOffset(DateTime.SpecifyKind(naiveUtc, DateTimeKind.Utc)).ToUnixTimeSeconds();

    private static DateTime Clock(int minutesAgo)
    {
        var raw = DateTime.UtcNow.AddMinutes(-minutesAgo);
        return DateTime.SpecifyKind(new DateTime(raw.Ticks - (raw.Ticks % 10)), DateTimeKind.Unspecified);
    }

    private static long s_nextId = 8_883_000;

    private static async Task SeedFilesAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, IEnumerable<File> files, CancellationToken ct)
    {
        var fileId = 1;
        var collectionId = s_nextId++;
        foreach (var f in files)
        {
            using var command = new NpgsqlCommand(@"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc, file_name, physical_name,
     total_size_mb, used_size_mb, auto_growth_mb, max_size_mb, is_percent_growth, growth_pct, is_autogrow_all_files)
VALUES ($1, $2, $3, 'srv', $4, $5, $6, $7, $8, $9, $10, 1, $11, -1, $12, $13, $14)", connection);
            command.Parameters.AddWithValue(collectionId);
            command.Parameters.AddWithValue(collectionTime);
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(f.DatabaseId == 2 ? "tempdb" : "userdb");
            command.Parameters.AddWithValue(f.DatabaseId);
            command.Parameters.AddWithValue(fileId);
            command.Parameters.AddWithValue(f.Type);
            command.Parameters.AddWithValue($"file{fileId}");
            command.Parameters.AddWithValue($"T:\\file{fileId}");
            command.Parameters.AddWithValue(f.Size);
            command.Parameters.AddWithValue(f.Step);
            command.Parameters.AddWithValue(f.Percent);
            command.Parameters.AddWithValue(f.Pct.HasValue ? f.Pct.Value : DBNull.Value);
            command.Parameters.AddWithValue(f.AllFiles.HasValue ? f.AllFiles.Value : DBNull.Value);
            await command.ExecuteNonQueryAsync(ct);
            fileId++;
        }
    }

    private static async Task SeedSchedulersAsync(NpgsqlConnection connection, int serverId, DateTime collectionTime, int schedulers, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_scheduler_stats
    (collection_id, collection_time, server_id, server_name, max_workers_count, scheduler_count, cpu_count, total_runnable_tasks_count,
     total_work_queue_count, total_current_workers_count, avg_runnable_tasks_count, total_active_request_count, total_queued_request_count,
     total_blocked_task_count, total_active_parallel_thread_count, runnable_percent, worker_thread_exhaustion_warning, runnable_tasks_warning,
     blocked_tasks_warning, queued_requests_warning, total_physical_memory_kb, available_physical_memory_kb, physical_memory_pressure_warning,
     total_node_count, nodes_online_count, offline_cpu_count, offline_cpu_warning)
VALUES ($1, $2, $3, 'srv', 512, $4, $4, 0, 0, 128, 0.5, 2, 0, 0, 0, 12.5, false, false, false, false, 67108864, 8388608, false, 2, 2, 0, false)", connection);
        command.Parameters.AddWithValue(s_nextId++);
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(schedulers);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static AnalysisContext Context(int serverId) => new()
    {
        ServerId = serverId,
        ServerName = "tempdb-file-setup",
        TimeRangeStart = DateTime.UtcNow.AddHours(-4),
        TimeRangeEnd = DateTime.UtcNow,
    };

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the tempdb file setup live test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            await body(connection, dataSource, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<Fact?> FactAsync(NpgsqlDataSource dataSource, int serverId, CancellationToken ct)
    {
        var facts = await new PgFactCollector(dataSource).CollectFactsAsync(Context(serverId));
        return facts.SingleOrDefault(f => f.Key == "TEMPDB_FILE_SETUP");
    }

    private static void AssertFails(string scenario, Fact? fact, params string[] failed)
    {
        if (failed.Length == 0)
        {
            Assert.True(fact is null, $"{scenario}: expected no fact");
            return;
        }

        Assert.True(fact is not null, $"{scenario}: expected a fact");
        Assert.Equal("config", fact!.Source);
        Assert.True(failed.Length == fact.Value, $"{scenario}: value {fact.Value}, expected {failed.Length}");
        foreach (var check in AllChecks)
        {
            Assert.True(
                (failed.Contains(check) ? 1.0 : 0.0) == fact.Metadata.GetValueOrDefault(check, 0.0),
                $"{scenario}: {check} should be {(failed.Contains(check) ? "failed" : "passing")}");
        }
    }

    [Fact]
    public async Task EachCheck_FailsAlone_AndASetupThatPassesEveryCheckHasNoFact()
    {
        await RunAsync(async (connection, dataSource, ct) =>
        {
            var serverId = FirstServerId;

            async Task<Fact?> Run(IEnumerable<File> files, int? schedulers)
            {
                var id = serverId--;
                await SeedFilesAsync(connection, id, Clock(30), files, ct);
                if (schedulers is not null)
                    await SeedSchedulersAsync(connection, id, Clock(20), schedulers.Value, ct);
                return await FactAsync(dataSource, id, ct);
            }

            AssertFails("passes", await Run(DataFiles(8).Append(Log()), 16));
            AssertFails("two schedulers, two files", await Run(DataFiles(2).Append(Log()), 2));
            AssertFails("more than eight schedulers asks for eight", await Run(DataFiles(8).Append(Log()), 64));

            var fewFiles = await Run(DataFiles(4).Append(Log()), 16);
            AssertFails("too few files", fewFiles, "check_data_file_count");
            Assert.Equal(4, fewFiles!.Metadata["data_file_count"]);
            Assert.Equal(16, fewFiles.Metadata["scheduler_count"]);
            Assert.Equal(8, fewFiles.Metadata["recommended_data_file_count"]);

            var sizes = await Run(DataFiles(7).Append(Data(size: 2048)).Append(Log()), 8);
            AssertFails("unequal sizes", sizes, "check_data_file_sizes");
            Assert.Equal(2048, sizes!.Metadata["data_file_largest_mb"]);
            Assert.Equal(1024, sizes.Metadata["data_file_smallest_mb"]);

            AssertFails("within one percent", await Run(new[] { Data(size: 1000), Data(size: 1009), Log() }, 2));

            AssertFails("unequal growth", await Run(DataFiles(7).Append(Data(step: 512)).Append(Log()), 8), "check_data_file_growth");
            AssertFails("percent growth on the log", await Run(DataFiles(8).Append(Log(percent: true)), 8), "check_percent_growth");
            AssertFails("percent growth on every data file", await Run(Enumerable.Range(0, 8).Select(_ => Data(percent: true, pct: 10, step: 0)).Append(Log()), 8), "check_percent_growth");
            AssertFails("autogrow off", await Run(Enumerable.Range(0, 8).Select(_ => Data(allFiles: false)).Append(Log()), 8), "check_autogrow_all_files");
            AssertFails("autogrow unknown", await Run(Enumerable.Range(0, 8).Select(_ => Data(allFiles: null)).Append(Log()), 8));
            AssertFails("two logs", await Run(DataFiles(8).Append(Log()).Append(Log()), 8), "check_log_file_count");

            AssertFails("no scheduler count, passing", await Run(DataFiles(2).Append(Log()), null));
            var noSchedulers = await Run(DataFiles(2).Append(Log()).Append(Log()), null);
            AssertFails("no scheduler count, two logs", noSchedulers, "check_log_file_count");
            Assert.False(noSchedulers!.Metadata.ContainsKey("scheduler_count"));

            AssertFails("no tempdb rows", await Run(new[] { new File("ROWS", 1024, DatabaseId: 7), new File("LOG", 512, DatabaseId: 7), new File("LOG", 512, DatabaseId: 7) }, 16));

            AssertFails(
                "everything fails",
                await Run(new[] { Data(size: 1024, step: 256), Data(size: 2048, step: 512, percent: true, pct: 10, allFiles: false), Log(), Log() }, 16),
                AllChecks);
        });
    }

    [Fact]
    public async Task OnlyTheNewestCollectionCounts_TheDrillDownListsItsFiles_AndTheEngineRootsAnAdvisoryCard()
    {
        await RunAsync(async (connection, dataSource, ct) =>
        {
            var healed = FirstServerId - 100;
            await SeedFilesAsync(connection, healed, Clock(90), DataFiles(1).Append(Log()).Append(Log()), ct);
            await SeedFilesAsync(connection, healed, Clock(30), DataFiles(8).Append(Log()), ct);
            await SeedSchedulersAsync(connection, healed, Clock(20), 8, ct);
            AssertFails("older collection is ignored", await FactAsync(dataSource, healed, ct));

            var broken = FirstServerId - 101;
            await SeedFilesAsync(connection, broken, Clock(90), DataFiles(8).Append(Log()), ct);
            var newestFiles = Clock(30);
            var newestSchedulers = Clock(20);
            await SeedFilesAsync(connection, broken, newestFiles, new[] { Data(size: 1024, step: 256), Data(size: 2048, step: 0, percent: true, pct: 10), Log() }, ct);
            await SeedSchedulersAsync(connection, broken, newestSchedulers, 16, ct);
            var fact = await FactAsync(dataSource, broken, ct);
            AssertFails("newest collection counts", fact, "check_data_file_count", "check_data_file_sizes", "check_data_file_growth", "check_percent_growth");

            /* The fact says WHEN: the newest collections' times, not the older one's. */
            Assert.Equal(Unix(newestFiles), fact!.Metadata[TempDbFileSetup.MetaFilesCollectedUnix]);
            Assert.Equal(Unix(newestSchedulers), fact.Metadata[TempDbFileSetup.MetaSchedulerCollectedUnix]);

            var facts = new List<Fact> { fact! };
            new FactScorer().ScoreAll(facts);
            Assert.Equal(0.3, fact!.BaseSeverity, precision: 6);
            var story = Assert.Single(new InferenceEngine(new RelationshipGraph()).BuildStories(facts));
            Assert.Equal("TEMPDB_FILE_SETUP", story.RootFactKey);

            var advice = FactAdvice.Compose("TEMPDB_FILE_SETUP", facts.ToDictionary(f => f.Key));
            Assert.Contains("tempdb has 2 data files and the server has 16 schedulers. Add data files up to 8, all the same size and growth.", advice!.Remediation);

            var finding = new AnalysisFinding
            {
                ServerId = broken,
                RootFactKey = "TEMPDB_FILE_SETUP",
                StoryPath = "TEMPDB_FILE_SETUP",
                PathKeys = ["TEMPDB_FILE_SETUP"],
                Severity = 0.3,
            };
            await new PgDrillDownCollector(dataSource).EnrichFindingsAsync([finding], Context(broken));

            var rows = JsonSerializer.SerializeToElement(finding.DrillDown!["tempdb_files"]).EnumerateArray().ToList();
            Assert.Equal(3, rows.Count);
            Assert.Equal("ROWS", rows[0].GetProperty("file_type").GetString());
            Assert.Equal("256 MB", rows[0].GetProperty("growth").GetString());
            Assert.Equal(2048, rows[1].GetProperty("total_size_mb").GetDouble());
            Assert.Equal("10%", rows[1].GetProperty("growth").GetString());
            Assert.Equal("LOG", rows[2].GetProperty("file_type").GetString());
            Assert.All(rows, row => Assert.Equal(newestFiles, row.GetProperty("collected_at").GetDateTime()));
        });
    }

    [Fact]
    public async Task TheSchedulerCount_IsBoundedByItsOwnCollectorsLookback_NotTheDatabaseSizeCollectors()
    {
        await RunAsync(async (connection, dataSource, ct) =>
        {
            /* The database size collector runs weekly, so its lookback is two weeks. A scheduler count from two days ago is outside
               the scheduler collector's own day, and the read must not accept it: with no count the file-count check is skipped. */
            var serverId = FirstServerId - 102;
            await SeedFilesAsync(connection, serverId, Clock(30), DataFiles(1).Append(Log()), ct);
            await SeedSchedulersAsync(connection, serverId, Clock(48 * 60), 16, ct);

            var context = Context(serverId);
            context.LatestValueStarts = new Dictionary<string, DateTime>
            {
                ["database_size_stats"] = context.TimeRangeEnd.AddDays(-14),
                ["cpu_scheduler_stats"] = context.TimeRangeEnd.AddHours(-24),
            };
            var facts = await new PgFactCollector(dataSource).CollectFactsAsync(context);
            AssertFails("the stale scheduler count is not read", facts.SingleOrDefault(f => f.Key == "TEMPDB_FILE_SETUP"));
        });
    }
}
