/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: with the diagnostic switch on, the anomaly detector, the plan regression read and the Query Store top read each write
/// DuckDB's own JSON operator profile per statement, and turn profiling off again on the connection. Off, nothing is written. Each
/// test runs the real read over a store the test seeds itself, with the switch held for this test's async flow alone
/// (<see cref="DuckDbReadProfile.ScopeForTests"/>), so tests that run side by side never see it.
/// </summary>
public sealed class DuckDbReadProfileTests : IDisposable
{
    private const int ServerId = 9195;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _profiles;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private long _nextId = 1;

    public DuckDbReadProfileTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _profiles = Path.Combine(_tempDir, "duckdb-profiles");
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

    /* Synchronous on purpose: the database lock is thread-affine (#2463), so no await may sit between taking it and releasing it. */
    private void Execute(params string[] statements)
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
    }

    private string StoreRow(DateTime collected, long queryId, string text) =>
        $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, runtime_stats_interval_id, interval_start_time_utc)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', {queryId}, {queryId}, 'Regular', {Ts(collected.AddMinutes(-10))}, {Ts(collected)},
     '{text}', '0x00000000000000{queryId}', 5, 100, 200, 1, 1, 1, 1, {Ts(collected.AddMinutes(-10))})";

    private void SeedQueryStore() =>
        Execute(
            StoreRow(Whole(NowUtc.AddHours(-2)), 11, "SELECT one"),
            StoreRow(Whole(NowUtc.AddHours(-1)), 12, "SELECT two"),
            StoreRow(Whole(NowUtc.AddMinutes(-30)), 11, "SELECT one"));

    /// <summary>The start of the baseline day <paramref name="daysBack"/> before <paramref name="windowStart"/>, floored to the hour so
    /// the seeded rows stay in the window's hour-of-week bucket.</summary>
    private static DateTime BaselineDay(DateTime windowStart, int daysBack)
    {
        var day = windowStart.AddDays(-daysBack);
        return day.Date.AddHours(day.Hour);
    }

    private string IoRow(DateTime time, long stallReadMs, long reads) =>
        $@"INSERT INTO file_io_stats
            (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, physical_name, size_mb,
             delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms, sample_interval_seconds)
            VALUES ({_nextId++}, {Ts(time)}, {ServerId}, 'TestServer', 'AppDb', 'AppDb_data', 'ROWS', 'D:\AppDb.mdf', 100,
                    {reads}, 0, 81920, 0, {stallReadMs}, 0, 60)";

    private string CpuRow(DateTime time, int cpu) =>
        $@"INSERT INTO cpu_utilization_stats
            (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
            VALUES ({_nextId++}, {Ts(time)}, {ServerId}, 'TestServer', {Ts(time)}, {cpu}, 2)";

    /// <summary>14 baseline days of 2 ms read latency in the window's hour, a quiet CPU canary for the baseline gate, and a window of
    /// 40 ms reads.</summary>
    private void SeedAnomalies(DateTime windowStart)
    {
        var statements = new List<string>();
        for (var day = 1; day <= 14; day++)
        {
            var baseDay = BaselineDay(windowStart, day);
            for (var i = 0; i < 4; i++)
            {
                statements.Add(IoRow(Whole(baseDay.AddMinutes(i * 3)), 20, 10));
                statements.Add(CpuRow(Whole(baseDay.AddMinutes(i * 3)), 10));
            }
        }

        for (var i = 0; i < 16; i++)
            statements.Add(IoRow(Whole(windowStart.AddMinutes(i * 15)), 800, 20));
        Execute(statements.ToArray());
    }

    private static AnalysisContext Context(DateTime start, DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = "TestServer",
        TimeRangeStart = start,
        TimeRangeEnd = end,
    };

    private string[] Files(string readName) =>
        Directory.Exists(_profiles)
            ? Directory.GetFiles(_profiles, readName + "-*.json").OrderBy(f => f, StringComparer.Ordinal).ToArray()
            : [];

    /// <summary>Whether the profile JSON holds an operator with a timing field: DuckDB names it operator_timing (timing before 1.2).</summary>
    private static bool HasOperatorTiming(string file)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(file));
        return Find(doc.RootElement);

        static bool Find(JsonElement e)
        {
            switch (e.ValueKind)
            {
                case JsonValueKind.Object:
                    foreach (var p in e.EnumerateObject())
                    {
                        if (p.Name is "operator_timing" or "timing" && p.Value.ValueKind == JsonValueKind.Number) return true;
                        if (Find(p.Value)) return true;
                    }
                    return false;
                case JsonValueKind.Array:
                    return e.EnumerateArray().Any(Find);
                default:
                    return false;
            }
        }
    }

    private void AssertProfiled(string readName)
    {
        var files = Files(readName);
        Assert.NotEmpty(files);
        Assert.All(files, f => Assert.True(HasOperatorTiming(f), $"{Path.GetFileName(f)} has no operator timing"));
    }

    [Fact]
    public async Task AnomalyDetector_WritesAProfilePerStatement_WithOperatorTimings()
    {
        var end = DateTime.UtcNow;
        var start = end.AddHours(-4);
        SeedAnomalies(start);

        using var scope = DuckDbReadProfile.ScopeForTests(on: true, _profiles);
        var facts = await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(Context(start, end));

        Assert.Contains(facts, f => f.Key == "ANOMALY_READ_LATENCY");
        AssertProfiled("anomaly-baseline-gate");
        AssertProfiled("anomaly-cpu-window");
        AssertProfiled("anomaly-io-window");
        Assert.All(Directory.GetFiles(_profiles, "*.json"), f => Assert.True(HasOperatorTiming(f), Path.GetFileName(f)));
    }

    [Fact]
    public async Task PlanRegressionRead_WritesAProfileForTheCostAndFailuresReads_AndTheTrailNamesThem()
    {
        SeedQueryStore();
        var end = Whole(NowUtc);

        using var scope = DuckDbReadProfile.ScopeForTests(on: true, _profiles);
        using var trail = DuckDbProfileTrail.Begin();
        await new DuckDbFactCollector(_duckDb).CollectPlanRegressionFactsAsync(Context(end.AddHours(-4), end), new List<Fact>());

        AssertProfiled("plan-regression-cost");
        AssertProfiled("plan-regression-failures");
        var named = trail.Describe();
        Assert.Contains(Path.GetFileName(Files("plan-regression-cost").Single()), named);
        Assert.Contains(Path.GetFileName(Files("plan-regression-failures").Single()), named);
    }

    [Fact]
    public async Task QueryStoreTop_WritesAProfileForTheRankingAndTheTwoLookups_AndTheResultIsTheSame()
    {
        SeedQueryStore();
        var plain = await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6);

        List<QueryStoreRow> profiled;
        using (DuckDbReadProfile.ScopeForTests(on: true, _profiles))
        {
            profiled = await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6);
        }

        Assert.Equal(2, plain.Count);
        Assert.Equal(plain.Select(r => (r.QueryId, r.QueryText)), profiled.Select(r => (r.QueryId, r.QueryText)));
        AssertProfiled("query-store-top-rank");
        AssertProfiled("query-store-top-latest-rows");
        AssertProfiled("query-store-top-text-by-row");
    }

    [Fact]
    public async Task SwitchOff_CreatesNoProfileFolderAndNoFile_AndTheReadsAnswerTheSame()
    {
        SeedQueryStore();
        var end = DateTime.UtcNow;
        var start = end.AddHours(-4);
        SeedAnomalies(start);

        using var scope = DuckDbReadProfile.ScopeForTests(on: false, _profiles);
        using var trail = DuckDbProfileTrail.Begin();
        var rows = await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6);
        var facts = await new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb)).DetectAnomaliesAsync(Context(start, end));

        Assert.Equal(2, rows.Count);
        Assert.Contains(facts, f => f.Key == "ANOMALY_READ_LATENCY");
        Assert.False(Directory.Exists(_profiles));
        Assert.Equal("", trail.Describe());
    }

    [Fact]
    public async Task AfterAProfiledRead_TheSameConnectionHasProfilingOff()
    {
        using var scope = DuckDbReadProfile.ScopeForTests(on: true, _profiles);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);

        using (var profiled = connection.CreateCommand())
        {
            profiled.CommandText = "SELECT count(*) FROM query_store_stats";
            using var reader = await profiled.ExecuteProfiledReaderAsync("probe", TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        }

        var file = Assert.Single(Files("probe"));
        var before = File.ReadAllText(file);

        using (var plain = connection.CreateCommand())
        {
            plain.CommandText = "SELECT count(*) FROM wait_stats";
            using var reader = await plain.ExecuteReaderAsync(TestContext.Current.CancellationToken);
            Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken));
        }

        Assert.Equal(new[] { file }, Directory.GetFiles(_profiles));
        Assert.Equal(before, File.ReadAllText(file));
        Assert.True(HasOperatorTiming(file));
    }

    [Fact]
    public async Task OnlyTheTwentyNewestFilesOfAReadAreKept()
    {
        SeedQueryStore();
        Directory.CreateDirectory(_profiles);
        for (var i = 0; i < 25; i++)
            File.WriteAllText(Path.Combine(_profiles, $"query-store-top-rank-20200101T0000{i:D2}000.json"), "{}");
        /* Another read whose name starts with this one's is not this read's file. */
        var other = Path.Combine(_profiles, "query-store-top-rank-x-20200101T000000000.json");
        File.WriteAllText(other, "{}");

        using (DuckDbReadProfile.ScopeForTests(on: true, _profiles))
        {
            await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6);
        }

        var kept = Files("query-store-top-rank").Select(Path.GetFileName).Where(n => !n!.StartsWith("query-store-top-rank-x-", StringComparison.Ordinal)).ToList();
        Assert.Equal(DuckDbReadProfile.KeepPerRead, kept.Count);
        Assert.DoesNotContain("query-store-top-rank-20200101T000000000.json", kept);
        Assert.Contains("query-store-top-rank-20200101T000024000.json", kept);
        Assert.Contains(kept, n => !n!.Contains("20200101", StringComparison.Ordinal));
        Assert.True(File.Exists(other));
    }

    [Fact]
    public async Task AFolderThatCannotBeWritten_StillLetsTheReadReturnItsResult()
    {
        SeedQueryStore();
        File.WriteAllText(_profiles, "a file where the folder should be");

        List<QueryStoreRow> rows;
        using (DuckDbReadProfile.ScopeForTests(on: true, _profiles))
        {
            rows = await _service.GetQueryStoreTopQueriesAsync(ServerId, hoursBack: 6);
        }

        Assert.Equal(2, rows.Count);
        Assert.True(File.Exists(_profiles));
    }
}
