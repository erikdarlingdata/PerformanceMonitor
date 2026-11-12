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
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Investigations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: the analysis drill-downs and fact collectors, the investigation evidence capture and the force-plan state read hold a
/// <see cref="DuckDbInitializer"/> and an open connection, not a <c>LocalDataService</c>. A window that starts after the newest
/// archived row must read the hot table alone for them too. Each test caches the archive span with one real read, then makes the
/// archive file unreadable: a read that still opens it fails, and the section it feeds comes back empty or missing.
/// </summary>
public sealed class ArchiveWindowAnalysisReadScalingTests : IDisposable
{
    private const int ServerId = 7256;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public ArchiveWindowAnalysisReadScalingTests()
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

    private string QueryStatsRow(DateTime collected, string hash, long spills = 0) =>
        $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     last_execution_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_spills, max_dop, query_text)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', '{hash}', '{hash}', {Ts(collected)}, 10, 100000, 100000, {spills}, 1, 'SELECT {hash}')";

    /// <summary>Moves the rows of <paramref name="table"/> older than <paramref name="before"/> into a Parquet file, rebuilds the
    /// views (which bumps the archive view generation) and reads the table's span once, so it is cached. Returns the file.</summary>
    private async Task<string> ArchiveAsync(string table, DateTime before, string timeColumn = "collection_time")
    {
        var path = Path.Combine(_archiveDir, $"20260101_000000_{table}.parquet").Replace("\\", "/");
        await ExecuteAsync(
            $"COPY (SELECT * FROM {table} WHERE {timeColumn} < {Ts(before)}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM {table} WHERE {timeColumn} < {Ts(before)}");
        await _duckDb.CreateArchiveViewsAsync();
        Assert.NotNull(await _duckDb.GetArchiveSpanAsync(table, timeColumn, Ct));
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

    /* ---- The shared check on the store ---- */

    [Fact]
    public async Task Plan_AWindowNewerThanTheArchive_ReadsTheHotTable_AndAWindowThatReachesItReadsTheView()
    {
        await ExecuteAsync(QueryStatsRow(Whole(NowUtc.AddDays(-10)), "archived"), QueryStatsRow(Whole(NowUtc.AddHours(-2)), "recent"));
        var archiveFile = await ArchiveAsync("query_stats", NowUtc.AddDays(-1));
        Break(archiveFile);

        var inside = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", Whole(NowUtc.AddHours(-6)), Ct);
        Assert.Equal("SELECT 1 FROM query_stats", inside.Apply("SELECT 1 FROM v_query_stats"));

        var reaching = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", Whole(NowUtc.AddDays(-12)), Ct);
        Assert.Equal("SELECT 1 FROM v_query_stats", reaching.Apply("SELECT 1 FROM v_query_stats"));
    }

    [Fact]
    public async Task Plan_AnArchiveMoveAfterThePlan_SendsTheStatementBackToTheView()
    {
        await ExecuteAsync(QueryStatsRow(Whole(NowUtc.AddDays(-10)), "archived"), QueryStatsRow(Whole(NowUtc.AddHours(-2)), "recent"));
        await ArchiveAsync("query_stats", NowUtc.AddDays(-1));
        var plan = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", Whole(NowUtc.AddHours(-6)), Ct);

        _duckDb.BumpArchiveViewGeneration();

        Assert.Equal("SELECT 1 FROM v_query_stats", plan.Apply("SELECT 1 FROM v_query_stats"));
    }

    /* ---- DrillDownCollector.Queries.cs ---- */

    private async Task<JsonNode?> TopCpuAsync(DateTime start)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "CPU_SQL_PERCENT",
            StoryPath = "CPU_SQL_PERCENT",
            PathKeys = ["CPU_SQL_PERCENT"],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(start));
        return finding.DrillDown is not null && finding.DrillDown.TryGetValue("top_cpu_queries", out var raw)
            ? JsonSerializer.SerializeToNode(raw)
            : null;
    }

    [Fact]
    public async Task TopCpuDrillDown_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(QueryStatsRow(Whole(NowUtc.AddDays(-10)), "archived"), QueryStatsRow(Whole(NowUtc.AddHours(-2)), "recent"));
        Break(await ArchiveAsync("query_stats", NowUtc.AddDays(-1)));

        var rows = Assert.IsType<JsonArray>(await TopCpuAsync(Whole(NowUtc.AddHours(-6))));

        Assert.Equal("recent", Assert.Single(rows)!["query_hash"]!.GetValue<string>());
    }

    [Fact]
    public async Task TopCpuDrillDown_AWindowThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        await ExecuteAsync(QueryStatsRow(Whole(NowUtc.AddDays(-10)), "archived"), QueryStatsRow(Whole(NowUtc.AddHours(-2)), "recent"));
        await ArchiveAsync("query_stats", NowUtc.AddDays(-1));

        var rows = Assert.IsType<JsonArray>(await TopCpuAsync(Whole(NowUtc.AddDays(-12))));

        Assert.Equal(["archived", "recent"], rows.Select(r => r!["query_hash"]!.GetValue<string>()).OrderBy(h => h, StringComparer.Ordinal).ToArray());
    }

    /* ---- DuckDbFactCollector.QueryPerf.cs ---- */

    private async Task<Fact?> SpillsFactAsync(DateTime start)
    {
        var facts = await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(start));
        return facts.FirstOrDefault(f => f.Key == "QUERY_SPILLS");
    }

    [Fact]
    public async Task SpillsFact_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(QueryStatsRow(Whole(NowUtc.AddDays(-10)), "archived", spills: 500), QueryStatsRow(Whole(NowUtc.AddHours(-2)), "recent", spills: 7));
        Break(await ArchiveAsync("query_stats", NowUtc.AddDays(-1)));

        var fact = await SpillsFactAsync(Whole(NowUtc.AddHours(-6)));

        Assert.NotNull(fact);
        Assert.Equal(7, fact.Value);
    }

    [Fact]
    public async Task SpillsFact_AWindowThatReachesIntoTheArchive_StillCountsTheArchivedSpills()
    {
        await ExecuteAsync(QueryStatsRow(Whole(NowUtc.AddDays(-10)), "archived", spills: 500), QueryStatsRow(Whole(NowUtc.AddHours(-2)), "recent", spills: 7));
        await ArchiveAsync("query_stats", NowUtc.AddDays(-1));

        var fact = await SpillsFactAsync(Whole(NowUtc.AddDays(-12)));

        Assert.NotNull(fact);
        Assert.Equal(507, fact.Value);
    }

    /* ---- LiteInvestigationEvidenceCapture.cs ---- */

    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);

    private async Task<JsonNode> TopStatementsEvidenceAsync(DateTime archiveBefore, Func<string, Task> beforeCapture)
    {
        var now = T0;
        var store = new DuckDbInvestigationStore(_duckDb, () => now, DuckDbInvestigationStoreTests.TestLockBudget);
        var opened = await store.OpenOrJoinAsync(
            new InvestigationAlert(1, ServerId, "srv", "resources", "High CPU", T0, ["k-cpu"], null, 20, 100, 15, 240), Ct);
        await ExecuteAsync(
            QueryStatsRow(Whole(T0.AddDays(-10)), "archived"),
            QueryStatsRow(Whole(T0.AddMinutes(-5)), "recent"));
        var file = await ArchiveAsync("query_stats", archiveBefore);
        await beforeCapture(file);

        var id = 0;
        foreach (var collector in new[] { "query_stats", "query_snapshots", "query_store_health", "running_jobs", "wait_stats" })
        {
            await ExecuteAsync($"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ({++id}, {ServerId}, 'srv', '{collector}', {Ts(T0.AddMinutes(2))}, 'SUCCESS')");
        }

        now = T0.AddMinutes(3);
        await new LiteInvestigationEvidenceCapture(_duckDb, store, null, () => now).CaptureOnceAsync(Ct);
        var sections = await store.ReadEvidenceAsync(opened.RequestId!.Value, Ct);
        var top = Assert.Single(sections, s => s.Section == "top_statements");
        return JsonNode.Parse(top.ContentJson!)!;
    }

    [Fact]
    public async Task EvidenceTopStatements_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        var content = await TopStatementsEvidenceAsync(T0.AddDays(-1), file => { Break(file); return Task.CompletedTask; });

        Assert.Equal("recent", Assert.Single(content["rows"]!.AsArray())!["query_hash"]!.GetValue<string>());
    }

    [Fact]
    public async Task EvidenceTopStatements_AWindowThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        /* The row inside the window is archived too, so only the archive holds it. */
        var content = await TopStatementsEvidenceAsync(T0.AddMinutes(-2), _ => Task.CompletedTask);

        Assert.Equal("recent", Assert.Single(content["rows"]!.AsArray())!["query_hash"]!.GetValue<string>());
    }

    /* ---- ForcePlanTargetStateReader (AnalysisService and the investigation brief reader) ---- */

    private string QueryStoreRow(DateTime collected, long queryId, long planId) =>
        $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, query_plan_hash, is_forced_plan,
     force_failure_count, plan_forcing_type, last_force_failure_reason, runtime_stats_interval_id, interval_start_time_utc)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', {queryId}, {planId}, 'Regular', {Ts(collected.AddMinutes(-10))}, {Ts(collected)},
     'SELECT 1', '0xQ', 100, 500, 900, 40, 0, 0, '0xP{planId}', true, 0, 'MANUAL', NULL, 1, {Ts(collected)})";

    private async Task<ForcePlanTargetState?> ForceStateAsync(DateTime nowUtc)
    {
        var plan = await _duckDb.PlanArchiveReadAsync(
            Ct, [.. PerformanceMonitorLite.Services.ForcePlanTargetStateReader.ArchivedTables
                .Select(t => new ArchiveWindow(t, "collection_time", nowUtc - ForcePlanTargetState.Lookback))]);
        /* The read lock is thread-affine (#2463): take it, read and release it on one dedicated thread, never across an await (#7256). */
        return await DuckDbLockThread.RunAsync(() =>
        {
            using var readLock = _duckDb.AcquireReadLock(Ct);
            using var connection = _duckDb.CreateConnection();
            connection.Open();
            using var command = connection.CreateCommand();
            var target = new ForcePlanTarget("Db", 5, 6);
            var states = PerformanceMonitorLite.Services.ForcePlanTargetStateReader.ReadAsync(command, ServerId, [target], nowUtc, plan, Ct).GetAwaiter().GetResult();
            return states.TryGetValue(ForcePlanTargetKey.Of(target), out var state) ? state : null;
        });
    }

    [Fact]
    public async Task ForcePlanState_ALookbackNewerThanBothArchives_OpensNoArchiveFile()
    {
        await ExecuteAsync(QueryStoreRow(Whole(NowUtc.AddHours(-30)), 5, 6), QueryStoreRow(Whole(NowUtc.AddHours(-2)), 5, 6));
        var storeFile = await ArchiveAsync("query_store_stats", NowUtc.AddHours(-12));
        var correctionFile = await ArchiveAsync("plan_correction", NowUtc.AddHours(-12));
        /* The second archive bumped the generation: read the first table's span again before the files go bad. */
        Assert.NotNull(await _duckDb.GetArchiveSpanAsync("query_store_stats", "collection_time", Ct));
        Break(storeFile);
        Break(correctionFile);

        var state = await ForceStateAsync(NowUtc);

        Assert.True(state?.PlanIsForced);
    }

    [Fact]
    public async Task ForcePlanState_ALookbackThatReachesIntoTheArchive_StillReadsTheArchive()
    {
        await ExecuteAsync(QueryStoreRow(Whole(NowUtc.AddHours(-20)), 5, 6));
        await ArchiveAsync("query_store_stats", NowUtc.AddHours(-10));
        await ArchiveAsync("plan_correction", NowUtc.AddHours(-10));

        var state = await ForceStateAsync(NowUtc);

        Assert.True(state?.PlanIsForced);
    }
}
