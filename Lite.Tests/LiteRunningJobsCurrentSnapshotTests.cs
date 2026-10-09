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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Running Jobs tab and get_running_jobs list only jobs from the collector's latest run. The collector writes no
/// row when no job is running, so the newest running_jobs row can be days older than the last run that looked: the tab
/// used to list a job collected weeks ago as running now. These drive the real read against a real DuckDB store.
/// </summary>
public sealed class LiteRunningJobsCurrentSnapshotTests : IDisposable
{
    private const int ServerId = 7;

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;

    public LiteRunningJobsCurrentSnapshotTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    /// <summary>One collection_log row: the run's time, its status, and how many rows it stored.</summary>
    private readonly record struct LogRow(TimeSpan Age, string Status, int Rows);

    private async Task<LocalDataService> SeedAsync(TimeSpan? snapshotAge, params LogRow[] log)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);

            if (snapshotAge is { } age)
            {
                using var insert = connection.CreateCommand();
                insert.CommandText = @"
INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time, current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ($1, $2, 'S1', 'Nightly ETL', 'job-1', true, $3, 111780, 900, 1200, 42, true, 400.0)";
                var snapshotTime = DateTime.UtcNow - age;
                insert.Parameters.Add(new DuckDBParameter { Value = snapshotTime });
                insert.Parameters.Add(new DuckDBParameter { Value = ServerId });
                insert.Parameters.Add(new DuckDBParameter { Value = snapshotTime.AddHours(-31) });
                await insert.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            var logId = 1;
            foreach (var row in log)
            {
                using var insertLog = connection.CreateCommand();
                insertLog.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected)
VALUES ($1, $2, 'S1', 'running_jobs', $3, 12, $4, $5)";
                insertLog.Parameters.Add(new DuckDBParameter { Value = (long)logId++ });
                insertLog.Parameters.Add(new DuckDBParameter { Value = ServerId });
                insertLog.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow - row.Age });
                insertLog.Parameters.Add(new DuckDBParameter { Value = row.Status });
                insertLog.Parameters.Add(new DuckDBParameter { Value = row.Rows });
                await insertLog.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }
        }

        await initializer.CreateArchiveViewsAsync();
        return new LocalDataService(initializer);
    }

    [Fact]
    public async Task NewestSnapshotDaysOlderThanTheLastSuccessfulRun_ReadsAsNoJobs()
    {
        /* The walk's case: a job collected days ago, and every run since found nothing and logged SUCCESS with zero rows. */
        var data = await SeedAsync(
            TimeSpan.FromDays(35),
            new LogRow(TimeSpan.FromDays(35), "SUCCESS", 1),
            new LogRow(TimeSpan.FromMinutes(10), "SUCCESS", 0),
            new LogRow(TimeSpan.FromMinutes(5), "SUCCESS", 0));

        var jobs = await data.GetRunningJobsAsync(ServerId);

        Assert.Empty(jobs);
    }

    [Fact]
    public async Task SnapshotFromTheLastSuccessfulRun_StillReadsItsJobs()
    {
        /* Lite stamps the log row with the run's start, so its snapshot is at or just after that instant. */
        var data = await SeedAsync(
            TimeSpan.FromMinutes(1),
            new LogRow(TimeSpan.FromMinutes(6), "SUCCESS", 0),
            new LogRow(TimeSpan.FromMinutes(1.01), "SUCCESS", 1));

        var jobs = await data.GetRunningJobsAsync(ServerId);

        var job = Assert.Single(jobs);
        Assert.Equal("Nightly ETL", job.JobName);
    }

    [Fact]
    public async Task SnapshotFromARunWhoseLogRowIsNotWrittenYet_StillReadsItsJobs()
    {
        /* The rows are stored before the log row is, so for a moment the newest log row is the previous, empty run. */
        var data = await SeedAsync(
            TimeSpan.FromSeconds(20),
            new LogRow(TimeSpan.FromMinutes(5), "SUCCESS", 0));

        var jobs = await data.GetRunningJobsAsync(ServerId);

        Assert.Single(jobs);
    }

    [Fact]
    public async Task FailedRunsJustAfterTheLastSuccess_DoNotMakeTheLastSuccessfulSnapshotVanish()
    {
        /* Only a SUCCESS run says what is running. A later PERMISSIONS or ERROR run looked at nothing, so it neither clears nor
           confirms the list while the snapshot is still inside the freshness bound (three missed cycles, 15 minutes at the shipped 5). */
        var data = await SeedAsync(
            TimeSpan.FromMinutes(8),
            new LogRow(TimeSpan.FromMinutes(8.01), "SUCCESS", 1),
            new LogRow(TimeSpan.FromMinutes(5), "ERROR", 0),
            new LogRow(TimeSpan.FromMinutes(2), "PERMISSIONS", 0));

        var read = await data.ReadRunningJobsAsync(ServerId);

        Assert.Single(read.Jobs);
        Assert.Null(read.LastGoodCollection);
    }

    [Fact]
    public async Task SuccessWithRowsThenOnlyFailedRunsForHours_ReadsNoJobsAndSaysCollectionIsNotCurrent()
    {
        /* The offline-after-rows case: the server went away while a job ran, and three hours of ERROR runs followed. The job
           must not read as running (its "current duration" frozen at the last good run) and the answer says when collection last worked. */
        var data = await SeedAsync(
            TimeSpan.FromHours(3),
            new LogRow(TimeSpan.FromHours(3), "SUCCESS", 1),
            new LogRow(TimeSpan.FromHours(2), "ERROR", 0),
            new LogRow(TimeSpan.FromHours(1), "ERROR", 0),
            new LogRow(TimeSpan.FromMinutes(5), "ERROR", 0));

        var read = await data.ReadRunningJobsAsync(ServerId);

        Assert.Empty(read.Jobs);
        Assert.NotNull(read.LastGoodCollection);
        Assert.True(DateTime.UtcNow - read.LastGoodCollection!.Value > TimeSpan.FromHours(2.9));
        Assert.Empty(await data.GetRunningJobsAsync(ServerId));
    }

    [Fact]
    public async Task CollectorSwitchedOffAfterARunThatStoredRows_ReadsNoJobsAndSaysCollectionIsNotCurrent()
    {
        /* No later log row exists at all: the user turned the collector off in the schedule editor. */
        var data = await SeedAsync(TimeSpan.FromHours(5), new LogRow(TimeSpan.FromHours(5), "SUCCESS", 1));

        var read = await data.ReadRunningJobsAsync(ServerId);

        Assert.Empty(read.Jobs);
        Assert.NotNull(read.LastGoodCollection);
    }

    [Fact]
    public async Task HealthyCollectorThatFoundNoJobs_ReadsNoJobsAndDoesNotClaimCollectionIsStale()
    {
        var data = await SeedAsync(
            TimeSpan.FromDays(2),
            new LogRow(TimeSpan.FromDays(2), "SUCCESS", 1),
            new LogRow(TimeSpan.FromMinutes(3), "SUCCESS", 0));

        var read = await data.ReadRunningJobsAsync(ServerId);

        Assert.Empty(read.Jobs);
        Assert.Null(read.LastGoodCollection);
    }

    [Fact]
    public async Task StoreWithNoRunLogForTheCollector_KeepsTheNewestSnapshotWhileItIsCurrent()
    {
        /* A log row can be gone (retention) while the data row is not: with nothing to decide by, keep what was read before, while it is recent. */
        var data = await SeedAsync(TimeSpan.FromMinutes(4));

        var jobs = await data.GetRunningJobsAsync(ServerId);

        Assert.Single(jobs);
    }

    [Fact]
    public async Task StoreWithNoRunLogForTheCollector_StopsListingASnapshotOlderThanTheBound()
    {
        var data = await SeedAsync(TimeSpan.FromHours(3));

        var read = await data.ReadRunningJobsAsync(ServerId);

        Assert.Empty(read.Jobs);
        Assert.NotNull(read.LastGoodCollection);
    }

    [Fact]
    public async Task ServerWithNoSnapshotAtAll_ReadsAsNoJobs()
    {
        var data = await SeedAsync(null, new LogRow(TimeSpan.FromMinutes(2), "SUCCESS", 0));

        var jobs = await data.GetRunningJobsAsync(ServerId);

        Assert.Empty(jobs);
    }
}
