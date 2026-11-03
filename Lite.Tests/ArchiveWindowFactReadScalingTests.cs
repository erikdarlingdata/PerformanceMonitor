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
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: the activity, config and storage fact collectors (<c>DuckDbFactCollector.Activity.cs</c>, <c>.Config.cs</c>,
/// <c>.Storage.cs</c>, <c>.PlanRegressionFailures.cs</c>) and the plan advisory drill-down hold a <see cref="DuckDbInitializer"/>
/// and an open connection, not a <c>LocalDataService</c>. A window (or lookback) that starts after the newest archived row must
/// read the hot table alone for them. Each test archives an old row to a real Parquet file, caches the span with one read, then
/// makes the file unreadable: a read that still opens it fails, and every one of these collectors swallows that failure, so the
/// fact comes back missing.
/// </summary>
public sealed class ArchiveWindowFactReadScalingTests : IDisposable
{
    private const int ServerId = 7256;

    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly DuckDbInitializer _duckDb;
    private long _nextId = 1;

    public ArchiveWindowFactReadScalingTests()
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

    private static DateTime Old => Whole(NowUtc.AddDays(-40));

    private static DateTime Recent => Whole(NowUtc.AddHours(-2));

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

    /// <summary>Moves the rows of <paramref name="table"/> older than a day into a Parquet file, rebuilds the views (which bumps the
    /// archive view generation), reads the table's span once so it is cached, and makes the file unreadable.</summary>
    private async Task ArchiveAndBreakAsync(string table, string timeColumn = "collection_time")
    {
        var path = Path.Combine(_archiveDir, $"20260101_000000_{table}.parquet").Replace("\\", "/");
        var before = Ts(NowUtc.AddDays(-1));
        await ExecuteAsync(
            $"COPY (SELECT * FROM {table} WHERE {timeColumn} < {before}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM {table} WHERE {timeColumn} < {before}");
        await _duckDb.CreateArchiveViewsAsync();
        Assert.NotNull(await _duckDb.GetArchiveSpanAsync(table, timeColumn, Ct));
        File.WriteAllText(path, "not a parquet file");
    }

    /// <summary>Archives (without breaking the file) so a read that reaches into the archive still sees it.</summary>
    private async Task ArchiveAsync(string table, string timeColumn = "collection_time")
    {
        var path = Path.Combine(_archiveDir, $"20260101_000000_{table}.parquet").Replace("\\", "/");
        var before = Ts(NowUtc.AddDays(-1));
        await ExecuteAsync(
            $"COPY (SELECT * FROM {table} WHERE {timeColumn} < {before}) TO '{DuckDbInitializer.EscapeSqlPath(path)}' (FORMAT PARQUET)",
            $"DELETE FROM {table} WHERE {timeColumn} < {before}");
        await _duckDb.CreateArchiveViewsAsync();
        Assert.NotNull(await _duckDb.GetArchiveSpanAsync(table, timeColumn, Ct));
    }

    private static AnalysisContext Context(DateTime start) => new()
    {
        ServerId = ServerId,
        ServerName = "TestSrv",
        TimeRangeStart = start,
        TimeRangeEnd = Whole(NowUtc),
    };

    private async Task<List<Fact>> FactsAsync(DateTime windowStart) =>
        await new DuckDbFactCollector(_duckDb).CollectFactsAsync(Context(windowStart));

    private static Fact? Find(List<Fact> facts, string key) => facts.FirstOrDefault(f => f.Key == key);

    private static DateTime ShortWindow => Whole(NowUtc.AddHours(-6));

    private static DateTime ReachingWindow => Whole(NowUtc.AddDays(-45));

    /* ---- DuckDbFactCollector.Activity.cs ---- */

    private string RunningJob(DateTime collected, string name, bool runningLong) =>
        $@"INSERT INTO running_jobs (collection_time, server_id, server_name, job_name, job_id, job_enabled, start_time,
     current_duration_seconds, avg_duration_seconds, p95_duration_seconds, successful_run_count, is_running_long, percent_of_average)
VALUES ({Ts(collected)}, {ServerId}, 'TestSrv', '{name}', '{name}-id', true, {Ts(collected)}, 600, 60, 90, 20, {(runningLong ? "true" : "false")}, 1000.0)";

    [Fact]
    public async Task RunningJobsFact_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(RunningJob(Old, "OldJob", true), RunningJob(Recent, "RecentJob", true));
        await ArchiveAndBreakAsync("running_jobs");

        var fact = Find(await FactsAsync(ShortWindow), "RUNNING_JOBS");

        Assert.NotNull(fact);
        Assert.Equal(1, fact.Metadata["running_count"]);
        Assert.Equal("RecentJob", fact.ObjectName);
    }

    [Fact]
    public async Task RunningJobsFact_AWindowThatReachesIntoTheArchive_StillCountsTheArchivedRows()
    {
        await ExecuteAsync(RunningJob(Old, "OldJob", true), RunningJob(Recent, "RecentJob", true));
        await ArchiveAsync("running_jobs");

        var fact = Find(await FactsAsync(ReachingWindow), "RUNNING_JOBS");

        Assert.NotNull(fact);
        Assert.Equal(2, fact.Metadata["running_count"]);
    }

    private string SessionRow(DateTime collected, string program, int connections) =>
        $@"INSERT INTO session_stats (collection_id, collection_time, server_id, server_name, program_name,
     connection_count, running_count, sleeping_count, dormant_count)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', '{program}', {connections}, 0, 0, 0)";

    [Fact]
    public async Task SessionStatsFact_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(SessionRow(Old, "OldApp", 50), SessionRow(Recent, "NewApp", 3));
        await ArchiveAndBreakAsync("session_stats");

        var fact = Find(await FactsAsync(ShortWindow), "SESSION_STATS");

        Assert.NotNull(fact);
        Assert.Equal(3, fact.Value);
    }

    [Fact]
    public async Task SessionStatsFact_AWindowThatReachesIntoTheArchive_StillCountsTheArchivedRows()
    {
        await ExecuteAsync(SessionRow(Old, "OldApp", 50), SessionRow(Recent, "NewApp", 3));
        await ArchiveAsync("session_stats");

        var fact = Find(await FactsAsync(ReachingWindow), "SESSION_STATS");

        Assert.NotNull(fact);
        Assert.Equal(53, fact.Value);
    }

    /* ---- DuckDbFactCollector.Config.cs ---- */

    private string TraceFlagRow(DateTime captured, int flag) =>
        $@"INSERT INTO trace_flags (config_id, capture_time, server_id, server_name, trace_flag, status, is_global, is_session)
VALUES ({_nextId++}, {Ts(captured)}, {ServerId}, 'TestSrv', {flag}, true, true, false)";

    [Fact]
    public async Task TraceFlagsFact_ALookbackNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(TraceFlagRow(Old, 1117), TraceFlagRow(Recent, 1118));
        await ArchiveAndBreakAsync("trace_flags", "capture_time");

        var fact = Find(await FactsAsync(ShortWindow), "TRACE_FLAGS");

        Assert.NotNull(fact);
        Assert.Equal(1, fact.Metadata["TF_1118"]);
        Assert.False(fact.Metadata.ContainsKey("TF_1117"));
    }

    /* ---- DuckDbFactCollector.Storage.cs ---- */

    private string FileIoRow(DateTime collected, double sizeMb, long reads, long stallReadMs) =>
        $@"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type,
     physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes, delta_stall_read_ms, delta_stall_write_ms,
     sample_interval_seconds)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'AppDb', 'AppDb_data', 'ROWS', 'D:\AppDb.mdf', {sizeMb}, {reads}, 0, 0, 0, {stallReadMs}, 0, 60)";

    [Fact]
    public async Task IoLatencyAndDatabaseSizeFacts_AWindowNewerThanTheArchive_OpenNoArchiveFile()
    {
        await ExecuteAsync(FileIoRow(Old, 1000, 10, 1000), FileIoRow(Recent, 50, 10, 20));
        await ArchiveAndBreakAsync("file_io_stats");

        var facts = await FactsAsync(ShortWindow);

        var latency = Find(facts, "IO_READ_LATENCY_MS");
        Assert.NotNull(latency);
        Assert.Equal(2, latency.Value);
        var size = Find(facts, "DATABASE_TOTAL_SIZE_MB");
        Assert.NotNull(size);
        Assert.Equal(50, size.Value);
    }

    [Fact]
    public async Task IoLatencyFact_AWindowThatReachesIntoTheArchive_StillCountsTheArchivedRows()
    {
        await ExecuteAsync(FileIoRow(Old, 1000, 10, 1000), FileIoRow(Recent, 50, 10, 20));
        await ArchiveAsync("file_io_stats");

        var latency = Find(await FactsAsync(ReachingWindow), "IO_READ_LATENCY_MS");

        Assert.NotNull(latency);
        Assert.Equal(51, latency.Value);
    }

    private string TempDbRow(DateTime collected, double totalReservedMb) =>
        $@"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb,
     internal_object_reserved_mb, version_store_reserved_mb, total_reserved_mb, unallocated_mb, total_sessions_using_tempdb,
     top_session_id, top_session_tempdb_mb)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 1, 1, 1, {totalReservedMb}, 10, 3, 55, 1)";

    [Fact]
    public async Task TempDbFact_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(TempDbRow(Old, 900), TempDbRow(Recent, 20));
        await ArchiveAndBreakAsync("tempdb_stats");

        var fact = Find(await FactsAsync(ShortWindow), "TEMPDB_USAGE");

        Assert.NotNull(fact);
        Assert.Equal(20, fact.Metadata["max_reserved_mb"]);
    }

    [Fact]
    public async Task TempDbFact_AWindowThatReachesIntoTheArchive_StillCountsTheArchivedRows()
    {
        await ExecuteAsync(TempDbRow(Old, 900), TempDbRow(Recent, 20));
        await ArchiveAsync("tempdb_stats");

        var fact = Find(await FactsAsync(ReachingWindow), "TEMPDB_USAGE");

        Assert.NotNull(fact);
        Assert.Equal(900, fact.Metadata["max_reserved_mb"]);
    }

    private string DatabaseSizeRow(DateTime collected, string database, double totalSizeMb, string volume, double volumeFreeMb) =>
        $@"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id,
     file_type_desc, file_name, physical_name, total_size_mb, used_size_mb, volume_mount_point, volume_total_mb, volume_free_mb,
     is_percent_growth)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', '{database}', 5, 1, 'ROWS', 'f', 'D:\f.mdf', {totalSizeMb}, 50,
     '{volume}', 1000, {volumeFreeMb}, true)";

    [Fact]
    public async Task DiskSpaceAndAutogrowthFacts_ALookbackNewerThanTheArchive_OpenNoArchiveFile()
    {
        await ExecuteAsync(DatabaseSizeRow(Old, "OldDb", 20000, "C:\\", 10), DatabaseSizeRow(Recent, "BigDb", 20000, "D:\\", 400));
        await ArchiveAndBreakAsync("database_size_stats");

        var facts = await FactsAsync(ShortWindow);

        var disk = Find(facts, "DISK_SPACE");
        Assert.NotNull(disk);
        Assert.Equal(1, disk.Metadata["volume_count"]);
        Assert.Equal(400, disk.Metadata["min_free_mb"]);
        var growth = Find(facts, "FILE_AUTOGROWTH_PERCENT");
        Assert.NotNull(growth);
        Assert.Equal(1, growth.Metadata["file_count"]);
    }

    /* ---- DuckDbFactCollector.PlanRegressionFailures.cs ---- */

    private string AbortedRun(DateTime collected, DateTime lastExecution, long queryId) =>
        $@"INSERT INTO query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
     execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads, query_plan_hash, is_forced_plan,
     force_failure_count, plan_forcing_type, last_force_failure_reason, runtime_stats_interval_id, interval_start_time_utc)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', {queryId}, {queryId + 1000}, 'Aborted', {Ts(lastExecution.AddMinutes(-10))},
     {Ts(lastExecution)}, 'SELECT 1', '0xQ', 5, 500, 900, 40, 0, 0, '0xP{queryId}', false, 0, 'NONE', NULL, 1, {Ts(lastExecution)})";

    private async Task<IReadOnlyList<PlanRegressionFailureRule.Failure>?> PlanRegressionFailuresAsync(DateTime windowStart)
    {
        var context = Context(windowStart);
        await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context);
        return context.PlanRegressionFailures;
    }

    [Fact]
    public async Task PlanRegressionFailures_AWindowNewerThanTheArchive_OpensNoArchiveFile()
    {
        await ExecuteAsync(AbortedRun(Old, Old, 11), AbortedRun(Recent, Recent, 77));
        await ArchiveAndBreakAsync("query_store_stats", "last_execution_time");

        var failures = await PlanRegressionFailuresAsync(ShortWindow);

        Assert.NotNull(failures);
        Assert.Equal(77, Assert.Single(failures).QueryId);
    }

    /* ---- DrillDownCollector.Plans.cs: CollectPlanAdvisoryDetail ---- */

    private static string WarningPlan() =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + WebUtility.HtmlEncode("SELECT * FROM dbo.t WHERE UPPER(c) = 1") + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"upper([db].[dbo].[t].[c])=(1)\"><Const ConstValue=\"(1)\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private string QueryStatsRow(DateTime collected, string hash, string? planXml) =>
        $@"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, plan_handle,
     creation_time, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_spills, query_text, query_plan_xml)
VALUES ({_nextId++}, {Ts(collected)}, {ServerId}, 'TestSrv', 'Db', '{hash}', '{hash}', '{hash}-handle', {Ts(collected.AddDays(-1))},
     10, 500000, 1000000, 1000, 0, 'SELECT {hash}', {(planXml is null ? "NULL" : "'" + planXml.Replace("'", "''") + "'")})";

    private async Task<object?> PlanWarningsAsync(DateTime windowStart)
    {
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_WARNING",
            StoryPath = "PLAN_WARNING",
            PathKeys = ["PLAN_WARNING"],
            Severity = 1.0,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], Context(windowStart));
        return finding.DrillDown is not null && finding.DrillDown.TryGetValue("plan_warnings", out var raw) ? raw : null;
    }

    [Fact]
    public async Task PlanAdvisoryDetail_AWindowNewerThanTheArchive_ProducesTheDetailWithoutOpeningAnArchiveFile()
    {
        await ExecuteAsync(QueryStatsRow(Old, "archived", null), QueryStatsRow(Recent, "recent", WarningPlan()));
        await ArchiveAndBreakAsync("query_stats");

        var warnings = await PlanWarningsAsync(ShortWindow);

        /* The detail swallows a failed read, so an unreadable archive file that the read still opened would leave this null. */
        Assert.NotNull(warnings);
        Assert.NotEqual("[]", JsonSerializer.Serialize(warnings));
    }
}
