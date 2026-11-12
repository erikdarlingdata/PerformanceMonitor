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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: the blocking, storage, wait and resource reads of the drill-downs and fact collectors. A window that starts after the
/// newest archived row must read the hot table alone, so an unreadable archive file cannot fail it. A window that reaches the
/// archive must still read it. Each test archives older rows to a Parquet file, caches the span with one real read, and makes
/// the file unreadable for the hot-window case: a read that still opens the file fails and its result goes missing.
/// </summary>
public sealed class ArchiveWindowCollectorReadScalingTests : IDisposable
{
    private const int ServerId = 7256;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public ArchiveWindowCollectorReadScalingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
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

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

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

    /// <summary>Moves the rows of <paramref name="table"/> older than <paramref name="before"/> into a Parquet file, rebuilds the
    /// views (which bumps the archive view generation) and reads the table's span once, so it is cached. Returns the file.</summary>
    private async Task<string> ArchiveAsync(string table, DateTime before)
    {
        var path = Path.Combine(_archiveDir, $"20260101_000000_{table}.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM {table} WHERE collection_time < {Ts(before)}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM {table} WHERE collection_time < {Ts(before)}");
        await _duckDb.CreateArchiveViewsAsync();
        Assert.NotNull(await _duckDb.GetArchiveSpanAsync(table, "collection_time", Ct));
        return path;
    }

    private static void Break(string archiveFile) => File.WriteAllText(archiveFile, "not a parquet file");

    private static AnalysisContext Context(DateTime start) => new()
    {
        ServerId = ServerId,
        ServerName = "TestSrv",
        TimeRangeStart = start,
        TimeRangeEnd = Whole(NowUtc),
    };

    private string WaitRow(DateTime collected, long deltaWaitMs, string waitType = "LCK_M_X") =>
        $@"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, waiting_tasks_count, wait_time_ms,
     signal_wait_time_ms, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', '{waitType}', 10, 10, 0, 10, {deltaWaitMs}, 0)";

    private string CpuRow(DateTime collected, int sqlCpu) =>
        $@"INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sample_time_utc,
     sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', {Ts(collected)}, {Ts(collected)}, {sqlCpu}, 3)";

    private string TempdbRow(DateTime collected, double userMb) =>
        $@"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb,
     internal_object_reserved_mb, version_store_reserved_mb, unallocated_mb)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', {userMb}, 1, 1, 500)";

    private string DmvBlockingRow(DateTime collected, long waitMs) =>
        $@"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, blocked_spid, blocking_spid,
     wait_time_ms, monitor_loop)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 51, 52, {waitMs}, 1)";

    /// <summary>A snapshot pair with its event time and statement text, which the reconstructed chains read by event key.</summary>
    private string DmvChainRow(DateTime collected, int blocked, int blocking, long waitMs) =>
        $@"INSERT INTO dmv_blocking_snapshots (collection_id, collection_time, server_id, server_name, event_time, blocked_spid, blocking_spid,
     wait_time_ms, blocked_sql_text, blocking_sql_text, blocked_ecid, blocking_ecid, monitor_loop)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', {Ts(collected)}, {blocked}, {blocking}, {waitMs},
     'SELECT blocked_{blocked}', 'UPDATE blocking_{blocking}', 0, 0, 1)";

    private async Task<JsonArray?> DrillDownAsync(string pathKey, string section, DateTime start)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = pathKey,
            StoryPath = pathKey,
            PathKeys = [pathKey],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(start));
        return finding.DrillDown is not null && finding.DrillDown.TryGetValue(section, out var raw)
            ? JsonSerializer.SerializeToNode(raw)!.AsArray()
            : null;
    }

    /* ---- DrillDownCollector.Blocking.cs ---- */

    [Fact]
    public async Task LockModeDrillDown_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(WaitRow(Whole(NowUtc.AddDays(-10)), 5000), WaitRow(Whole(NowUtc.AddHours(-2)), 1000));
        Break(await ArchiveAsync("wait_stats", NowUtc.AddDays(-1)));

        var rows = await DrillDownAsync("LCK", "lock_mode_breakdown", Whole(NowUtc.AddHours(-6)));

        Assert.Equal(1000, Assert.Single(rows!)!["total_wait_ms"]!.GetValue<double>());
    }

    [Fact]
    public async Task LockModeDrillDown_AWindowThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        await ExecuteAsync(WaitRow(Whole(NowUtc.AddDays(-10)), 5000), WaitRow(Whole(NowUtc.AddHours(-2)), 1000));
        await ArchiveAsync("wait_stats", NowUtc.AddDays(-1));

        var rows = await DrillDownAsync("LCK", "lock_mode_breakdown", Whole(NowUtc.AddDays(-12)));

        Assert.Equal(6000, Assert.Single(rows!)!["total_wait_ms"]!.GetValue<double>());
    }

    [Fact]
    public async Task BlockingChainsDrillDown_AWindowNewerThanTheArchive_OpensNoSnapshotArchiveFile()
    {
        await ExecuteAsync(DmvBlockingRow(Whole(NowUtc.AddDays(-10)), 9000), DmvBlockingRow(Whole(NowUtc.AddHours(-2)), 100));
        Break(await ArchiveAsync("dmv_blocking_snapshots", NowUtc.AddDays(-1)));

        var rows = await DrillDownAsync("BLOCKING_EVENTS", "top_blocking_chains", Whole(NowUtc.AddHours(-6)));

        Assert.Equal(100, Assert.Single(rows!)!["wait_time_ms"]!.GetValue<long>());
    }

    [Fact]
    public async Task BlockingChainsDrillDown_AWindowThatReachesIntoTheArchive_StillReadsTheSnapshotArchive()
    {
        await ExecuteAsync(DmvBlockingRow(Whole(NowUtc.AddDays(-10)), 9000), DmvBlockingRow(Whole(NowUtc.AddHours(-2)), 100));
        await ArchiveAsync("dmv_blocking_snapshots", NowUtc.AddDays(-1));

        var rows = await DrillDownAsync("BLOCKING_EVENTS", "top_blocking_chains", Whole(NowUtc.AddDays(-12)));

        Assert.Equal([9000L, 100L], rows!.Select(r => r!["wait_time_ms"]!.GetValue<long>()).ToArray());
    }

    [Fact]
    public async Task ReconstructedBlockingChains_AWindowNewerThanTheArchive_OpensNoSnapshotArchiveFile()
    {
        await ExecuteAsync(DmvChainRow(Whole(NowUtc.AddDays(-10)), 61, 62, 9000), DmvChainRow(Whole(NowUtc.AddHours(-2)), 51, 52, 100));
        Break(await ArchiveAsync("dmv_blocking_snapshots", NowUtc.AddDays(-1)));

        var chains = await DrillDownAsync("BLOCKING_CHAIN", "reconstructed_blocking_chains", Whole(NowUtc.AddHours(-6)));

        /* The window fetch and the by-key text fetch both ran on the hot table: the one hot pair, with its text. */
        var chain = Assert.Single(chains!)!;
        Assert.Equal(52, chain["apex_spid"]!.GetValue<int>());
        var level = Assert.Single(chain["levels"]!.AsArray())!;
        Assert.Contains("UPDATE blocking_52", level["blocking_sql"]!.GetValue<string>());
        Assert.Contains("SELECT blocked_51", level["blocked_sql"]!.GetValue<string>());
    }

    [Fact]
    public async Task ReconstructedBlockingChains_AWindowThatReachesIntoTheArchive_StillReadsTheSnapshotArchive()
    {
        await ExecuteAsync(DmvChainRow(Whole(NowUtc.AddDays(-10)), 61, 62, 9000), DmvChainRow(Whole(NowUtc.AddHours(-2)), 51, 52, 100));
        await ArchiveAsync("dmv_blocking_snapshots", NowUtc.AddDays(-1));

        var chains = await DrillDownAsync("BLOCKING_CHAIN", "reconstructed_blocking_chains", Whole(NowUtc.AddDays(-12)));

        Assert.Equal([52, 62], chains!.Select(c => c!["apex_spid"]!.GetValue<int>()).Order().ToArray());
        var archived = chains!.Single(c => c!["apex_spid"]!.GetValue<int>() == 62)!;
        Assert.Contains("UPDATE blocking_62", archived["levels"]![0]!["blocking_sql"]!.GetValue<string>());
    }

    /* ---- DrillDownCollector.Storage.cs ---- */

    [Fact]
    public async Task TempDbDrillDown_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(TempdbRow(Whole(NowUtc.AddDays(-10)), 900), TempdbRow(Whole(NowUtc.AddHours(-2)), 10));
        Break(await ArchiveAsync("tempdb_stats", NowUtc.AddDays(-1)));

        var rows = await DrillDownAsync("TEMPDB_USAGE", "tempdb_breakdown", Whole(NowUtc.AddHours(-6)));

        Assert.Equal(10, Assert.Single(rows!)!["user_objects_mb"]!.GetValue<double>());
    }

    [Fact]
    public async Task TempDbDrillDown_AWindowThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        await ExecuteAsync(TempdbRow(Whole(NowUtc.AddDays(-10)), 900), TempdbRow(Whole(NowUtc.AddHours(-2)), 10));
        await ArchiveAsync("tempdb_stats", NowUtc.AddDays(-1));

        var rows = await DrillDownAsync("TEMPDB_USAGE", "tempdb_breakdown", Whole(NowUtc.AddDays(-12)));

        Assert.Equal([900.0, 10.0], rows!.Select(r => r!["user_objects_mb"]!.GetValue<double>()).ToArray());
    }

    /* ---- DuckDbFactCollector.Waits.cs ---- */

    private async Task<Fact?> WaitFactAsync(DateTime start)
    {
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(start));
        return facts.FirstOrDefault(f => f.Source == "waits" && f.Key == Yield);
    }

    /* A wait the fact collector keeps under its own name; a LCK_M_ wait is folded into the LCK group. */
    private const string Yield = "SOS_SCHEDULER_YIELD";

    private string[] HotWaitSamples() =>
    [
        WaitRow(Whole(NowUtc.AddMinutes(-3)), 1000, Yield),
        WaitRow(Whole(NowUtc.AddMinutes(-2)), 1000, Yield),
        WaitRow(Whole(NowUtc.AddMinutes(-1)), 1000, Yield),
    ];

    [Fact]
    public async Task WaitFacts_AWindowNewerThanTheArchive_OpenNoArchiveFile()
    {
        await ExecuteAsync([WaitRow(Whole(NowUtc.AddDays(-10)), 5000, Yield), .. HotWaitSamples()]);
        Break(await ArchiveAsync("wait_stats", NowUtc.AddDays(-1)));

        var fact = await WaitFactAsync(Whole(NowUtc.AddHours(-6)));

        Assert.NotNull(fact);
        Assert.Equal(3000, fact.Metadata!["wait_time_ms"]);
    }

    [Fact]
    public async Task WaitFacts_AWindowThatReachesIntoTheArchive_StillCountTheArchivedWaits()
    {
        await ExecuteAsync([WaitRow(Whole(NowUtc.AddDays(-10)), 5000, Yield), .. HotWaitSamples()]);
        await ArchiveAsync("wait_stats", NowUtc.AddDays(-1));

        var fact = await WaitFactAsync(Whole(NowUtc.AddDays(-12)));

        Assert.NotNull(fact);
        Assert.Equal(8000, fact.Metadata!["wait_time_ms"]);
    }

    /* ---- DuckDbFactCollector.Resources.cs ---- */

    private async Task<Fact?> CpuFactAsync(DateTime start)
    {
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(start));
        return facts.FirstOrDefault(f => f.Key == "CPU_SQL_PERCENT");
    }

    [Fact]
    public async Task CpuFact_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(CpuRow(Whole(NowUtc.AddDays(-10)), 80), CpuRow(Whole(NowUtc.AddHours(-2)), 20));
        Break(await ArchiveAsync("cpu_utilization_stats", NowUtc.AddDays(-1)));

        var fact = await CpuFactAsync(Whole(NowUtc.AddHours(-6)));

        Assert.NotNull(fact);
        Assert.Equal(20, fact.Value);
    }

    [Fact]
    public async Task CpuFact_AWindowThatReachesIntoTheArchive_StillAveragesTheArchivedSamples()
    {
        await ExecuteAsync(CpuRow(Whole(NowUtc.AddDays(-10)), 80), CpuRow(Whole(NowUtc.AddHours(-2)), 20));
        await ArchiveAsync("cpu_utilization_stats", NowUtc.AddDays(-1));

        var fact = await CpuFactAsync(Whole(NowUtc.AddDays(-12)));

        Assert.NotNull(fact);
        Assert.Equal(50, fact.Value);
    }
}
