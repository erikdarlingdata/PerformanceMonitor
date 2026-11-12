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
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: the windowed reads of <c>v_query_stats</c> and <c>v_query_store_stats</c> must not open an archive file when the
/// archive holds nothing at or after the window. Each case caches the archive span with one real read, overwrites the one
/// archive file with garbage, and reads again: a read that still opens the file throws.
/// </summary>
public sealed class ArchiveWindowReadScalingMoreTests : IDisposable
{
    private const int ServerId = 7256;
    private static readonly DateTime NowUtc = DateTime.UtcNow;
    private static readonly DateTime Start = NowUtc.AddHours(-6);

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private long _nextId = 1;

    public ArchiveWindowReadScalingMoreTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
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

    private static DateTime Whole(DateTime t) =>
        DateTime.SpecifyKind(new DateTime(t.Ticks - (t.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static string Ts(DateTime t) => $"TIMESTAMP '{t:yyyy-MM-dd HH:mm:ss}'";

    /* Synchronous on purpose: the database lock is thread-affine (#2463), so no await may sit between taking it and releasing it (#7256). */
    private Task ExecuteAsync(params string[] statements)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
        return Task.CompletedTask;
    }

    private string StatsRow(DateTime collected, string hash, long cpu) =>
        $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, max_dop, query_text)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', '{hash}', '{hash}', {Ts(collected)}, 10, {cpu}, {cpu}, 1, 'SELECT {hash}')";

    private string StoreRow(DateTime collected) =>
        $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, runtime_stats_interval_id, interval_start_time_utc)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', 11, 22, 'Regular', {Ts(collected.AddMinutes(-10))}, {Ts(collected)},
     'SELECT 1', '0x0000000000000011', 5, 100, 200, 1, 1, 1, 1, {Ts(collected.AddMinutes(-10))})";

    /// <summary>Moves every hot row of <paramref name="table"/> older than a day into one Parquet file and rebuilds the views.</summary>
    private async Task<string> ArchiveAsync(string table)
    {
        var before = Ts(NowUtc.AddDays(-1));
        var path = Path.Combine(_archiveDir, $"20260101_000000_{table}.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM {table} WHERE collection_time < {before}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM {table} WHERE collection_time < {before}");
        await _duckDb.CreateArchiveViewsAsync();
        return path;
    }

    /// <summary>Case name, then the table whose archive the read must leave alone and the read itself.</summary>
    private static readonly Dictionary<string, (string Table, Func<LocalDataService, Task> Read)> Cases = new()
    {
        ["QueryStatsSlicer"] = ("query_stats", s => s.GetQueryStatsSlicerDataAsync(ServerId, 6)),
        ["QueryStatsComparison"] = ("query_stats", s => s.GetQueryStatsComparisonAsync(ServerId, NowUtc.AddHours(-3), NowUtc, Start, NowUtc.AddHours(-3))),
        ["QueryStatsHistory"] = ("query_stats", s => s.GetQueryStatsHistoryAsync(ServerId, "Db", "recent", 6)),
        ["ProcedureStatsComparison"] = ("query_stats", s => s.GetProcedureStatsComparisonAsync(ServerId, NowUtc.AddHours(-3), NowUtc, Start, NowUtc.AddHours(-3))),
        ["ExecutionCountTrend"] = ("query_stats", s => s.GetExecutionCountTrendAsync(ServerId, 6)),
        ["QueryDurationTrend"] = ("query_stats", s => s.GetQueryDurationTrendAsync(ServerId, 6)),
        ["QueryHeatmap"] = ("query_stats", s => s.GetQueryHeatmapAsync(ServerId, HeatmapMetric.Cpu, 6)),
        ["QueryTagRollupPlanCache"] = ("query_stats", s => s.GetQueryTagRollupDataAsync([ServerId], Start, NowUtc)),
        ["QueryStoreSlicer"] = ("query_store_stats", s => s.GetQueryStoreSlicerDataAsync(ServerId, 6)),
        ["QueryStoreComparison"] = ("query_store_stats", s => s.GetQueryStoreComparisonAsync(ServerId, NowUtc.AddHours(-3), NowUtc, Start, NowUtc.AddHours(-3))),
        ["QueryStoreItemTimeline"] = ("query_store_stats", s => s.GetQueryStoreItemTimelineAsync(ServerId, "Db", 11, 22, 6)),
        ["QueryStoreHistory"] = ("query_store_stats", s => s.GetQueryStoreHistoryAsync(ServerId, "Db", 11, 6)),
        ["QueryStoreDurationTrend"] = ("query_store_stats", s => s.GetQueryStoreDurationTrendAsync(ServerId, 6)),
        ["QueryStoreRegressions"] = ("query_store_stats", s => s.GetQueryStoreRegressionsAsync(ServerId, 6, dataStartUtc: Start)),
        ["QueryStoreRegressionCoverage"] = ("query_store_stats", s => s.GetQueryStoreRegressionCoverageAsync(ServerId, 6)),
        ["QueryTagRollupQueryStore"] = ("query_store_stats", s => s.GetQueryTagRollupDataAsync([ServerId], Start, NowUtc)),
    };

    public static IEnumerable<TheoryDataRow<string>> CaseNames() => Cases.Keys.Select(k => new TheoryDataRow<string>(k));

    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task AWindowNewerThanTheArchive_OpensNoArchiveFile(string name)
    {
        var (table, read) = Cases[name];
        await ExecuteAsync(
            StatsRow(Whole(NowUtc.AddDays(-10)), "archived", 999),
            StatsRow(Whole(NowUtc.AddHours(-2)), "recent", 100),
            StoreRow(Whole(NowUtc.AddDays(-10))),
            StoreRow(Whole(NowUtc.AddHours(-2))));
        var archiveFile = await ArchiveAsync(table);

        await read(_service);

        /* The archive file can no longer be read. A read that still opens it throws. */
        File.WriteAllText(archiveFile, "not a parquet file");

        await read(_service);
    }

    /// <summary>The force-plan state read goes through the analysis service's own handle (no LocalDataService entry point), so it gets its own case.</summary>
    [Fact]
    public async Task ForcePlanTargetStates_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(
            StoreRow(Whole(NowUtc.AddDays(-10))),
            StoreRow(Whole(NowUtc.AddHours(-2))));
        var archiveFile = await ArchiveAsync("query_store_stats");
        var finding = new AnalysisFinding { Remediation = new RemediationAction("PLAN_REGRESSION", "force", [new ForcePlanTarget("Db", 11, 22)]) };
        var analysis = new PerformanceMonitorLite.Analysis.AnalysisService(_duckDb);

        var (_, firstReason) = await analysis.TryGetForcePlanTargetStatesAsync(ServerId, [finding]);
        Assert.Null(firstReason);

        File.WriteAllText(archiveFile, "not a parquet file");

        /* A read that still opens the file comes back as an unavailable reason, not as an exception. */
        var (_, secondReason) = await analysis.TryGetForcePlanTargetStatesAsync(ServerId, [finding]);
        Assert.Null(secondReason);
    }
}
