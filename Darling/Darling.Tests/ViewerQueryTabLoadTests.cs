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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Release-walk finding D15: the Viewer's Top Queries and Query Store tabs took 10 to 27 seconds against a store reached over
/// a network. Measured on a local PostgreSQL behind a delayed link, the 7 day Top Queries load (the hourly rollup route) made
/// 52 sequential store calls, 50 of them the one-text-per-row lookup; the grid's slicer read started only after the grid's
/// own read finished; and the status bar measured the live database directory every five minutes. These pins hold the fixes.
/// </summary>
/* #1776 own-store: each live fact mints its own scratch database through ScratchPostgres and never touches the shared store. */
public sealed class ViewerQueryTabLoadTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string Source(string file) =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", file).Replace("\r\n", "\n");

    /* ------------------------------------------------------------------ offline pins */

    [Theory]
    [InlineData("LoadTopQueriesAsync", "GetQueryStatsSlicerDataAsync", "AwaitReadWatchingProbeAsync(dataReadTask, floorTask, \"Query Stats\")")]
    [InlineData("LoadTopProceduresAsync", "GetProcStatsSlicerDataAsync", "AwaitReadWatchingProbeAsync(dataReadTask, floorTask, \"Procedure Stats\")")]
    [InlineData("LoadQueryStoreAsync", "GetQueryStoreSlicerDataAsync", "AwaitReadWatchingProbeAsync(dataReadTask, floorTask, \"Query Store\")")]
    public void EachGridLoad_StartsItsSlicerRead_BesideTheGridRead_NotAfterIt(string loader, string slicerRead, string gridAwait)
    {
        var source = Source("ViewerServerTab.Queries.cs");
        var start = source.IndexOf($"private async Task {loader}(", StringComparison.Ordinal);
        Assert.True(start >= 0, loader);
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        var body = source[start..end];

        var slicerStarted = body.IndexOf($"_dataService.{slicerRead}(", StringComparison.Ordinal);
        var gridAwaited = body.IndexOf(gridAwait, StringComparison.Ordinal);
        Assert.True(slicerStarted >= 0, $"{loader} must start the slicer read");
        Assert.True(gridAwaited >= 0, $"{loader} must await the grid read");
        Assert.True(slicerStarted < gridAwaited,
            $"{loader}: the slicer read must be started before the grid read is awaited, so the two round trips overlap");
        /* A failed grid read still observes the slicer's task, so one failure is not reported twice. */
        Assert.Contains("ViewerDataService.ObserveAsync(slicerTask)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void HourlyTopQueries_ReadsEveryRowsTextInOneStatement_NotOnePerRow()
    {
        var source = Source("ViewerDataService.QueryStats.cs");
        var start = source.IndexOf("private async Task<ViewerRoutedRead<ViewerQueryStatsRow>> GetTopQueriesByCpuHourlyAsync(", StringComparison.Ordinal);
        var end = source.IndexOf("internal const string HourlyTextBatchSql", start, StringComparison.Ordinal);
        var body = source[start..end];
        Assert.Contains("ReadHourlyQueryTextsAsync(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("FROM v_query_stats WHERE server_id = $1 AND database_name = $2 AND query_hash = $3", body, StringComparison.Ordinal);
        /* The only per-row work left is shaping: no statement is created inside the loop over the ranked rows. */
        var loop = body.IndexOf("for (var i = 0; i < ranked.Count; i++)", StringComparison.Ordinal);
        Assert.True(loop >= 0);
        var loopBody = body[loop..];
        Assert.DoesNotContain("CreateCommand", loopBody, StringComparison.Ordinal);
    }

    [Fact]
    public void EveryInnerTabLoad_IsTimed_AndASlowOneLogsItsPhaseSplit()
    {
        var shell = Source("ViewerServerTab.xaml.cs");
        Assert.Contains("var timer = _loadTimer = new ViewerLoadTimer();", shell, StringComparison.Ordinal);
        Assert.Contains("timer.Finish(", shell, StringComparison.Ordinal);
        Assert.Contains("ViewerLogger.Warn(\"SlowLoad\"", shell, StringComparison.Ordinal);
    }

    [Fact]
    public void StatusBarStoreSize_ReadsTheRecordedFigure_BeforeMeasuringTheLiveDirectory()
    {
        var source = Source("ViewerDataService.ServerStatus.cs");
        var start = source.IndexOf("private async Task<long?> FetchStoreSizeBytesAsync()", StringComparison.Ordinal);
        var body = source[start..];
        var recorded = body.IndexOf("StoreSelfMetrics.LatestStoreSizeSql", StringComparison.Ordinal);
        var live = body.IndexOf("CreateCommand(StoreSizeSql)", StringComparison.Ordinal);
        Assert.True(recorded >= 0 && live > recorded, "the recorded figure is read first; the live directory only when none is recorded");
    }

    [Fact]
    public void LoadTimer_LogsNothingForAQuickLoad()
    {
        var phases = new List<(string Phase, long StartedMs, long EndedMs)> { ("grid read", 0, 400) };
        Assert.Null(ViewerLoadTimer.Describe("Queries > Top Queries by Duration", ViewerLoadTimer.SlowLoadThresholdMs - 1, phases));
    }

    [Fact]
    public void LoadTimer_NamesTheTab_EachPhase_AndTheClientShare_ForASlowLoad()
    {
        var phases = new List<(string Phase, long StartedMs, long EndedMs)>
        {
            ("data start", 0, 300), ("grid read", 0, 5200), ("slicer read", 0, 900),
        };
        var line = ViewerLoadTimer.Describe("Queries > Top Queries by Duration", 5900, phases);
        Assert.NotNull(line);
        Assert.Contains("Queries > Top Queries by Duration", line, StringComparison.Ordinal);
        Assert.Contains("took 5900 ms", line, StringComparison.Ordinal);
        Assert.Contains("grid read 5200 ms", line, StringComparison.Ordinal);
        Assert.Contains("slicer read 900 ms", line, StringComparison.Ordinal);
        /* 5900 total, the last read finished at 5200: 700 ms was the client's. */
        Assert.Contains("700 ms of client work", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task LoadTimer_Track_RecordsAPhase_AndHandsTheTaskBackUnchanged()
    {
        var timer = new ViewerLoadTimer();
        var task = Task.FromResult(7);
        Assert.Same(task, timer.Track("grid read", task));
        await task;
        await Task.Delay(50, TestContext.Current.CancellationToken);
        timer.Surface = "x";
        Assert.Null(timer.Finish("fallback"));
    }

    private static string LoaderBody(string file, string signature)
    {
        var source = Source(file);
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, signature);
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        return source[start..end];
    }

    [Fact]
    public void LongQueries_StartsItsTraceCheck_BesideTheGridReads_NotBeforeThem()
    {
        var body = LoaderBody("ViewerServerTab.LongQueries.cs", "private async Task LoadLongQueriesAsync(");
        var traceStarted = body.IndexOf("_dataService.GetLongQueryTraceEnabledAsync(", StringComparison.Ordinal);
        var dataStarted = body.IndexOf("_dataService.GetRecentLongQueryCompletionsAsync(", StringComparison.Ordinal);
        var gridAwaited = body.IndexOf("AwaitReadWatchingProbeAsync(dataReadTask, dataStartTask, \"Long Queries\")", StringComparison.Ordinal);
        Assert.True(traceStarted >= 0 && dataStarted >= 0 && gridAwaited >= 0);
        Assert.True(traceStarted < gridAwaited, "the trace check must be started before the grid read is awaited, so the round trips overlap");
        Assert.DoesNotContain("await _dataService.GetLongQueryTraceEnabledAsync(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("await Timed(\"trace check\"", body, StringComparison.Ordinal);
        /* The note for a trace that is off is still driven by the check's answer. */
        Assert.Contains("LongQueriesDisabledWarning.Visibility = await traceTask", body, StringComparison.Ordinal);
        Assert.Contains("ViewerDataService.ObserveAsync(traceTask)", body, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("GetQueryStatsComparisonAsync")]
    [InlineData("GetProcedureStatsComparisonAsync")]
    [InlineData("GetQueryStoreComparisonAsync")]
    public void ComparisonReads_AreTimed_SoSlowLoadDoesNotCallThemClientWork(string read)
    {
        var source = Source("ViewerServerTab.QueriesComparison.cs");
        Assert.Contains($"Timed(\"comparison read\", _dataService.{read}(", source, StringComparison.Ordinal);
    }

    [Fact]
    public void LoadTimer_CountsALateComparisonRead_AsAStoreRead_NotAsClientWork()
    {
        var phases = new List<(string Phase, long StartedMs, long EndedMs)>
        {
            ("grid read", 0, 4000), ("slicer read", 0, 1000), ("comparison read", 4100, 6800),
        };
        var line = ViewerLoadTimer.Describe("Queries > Top Queries by Duration", 7000, phases);
        Assert.NotNull(line);
        Assert.Contains("comparison read 2700 ms", line, StringComparison.Ordinal);
        Assert.Contains("200 ms of client work", line, StringComparison.Ordinal);
    }

    [Fact]
    public void InnerTabLoad_ClearsItsTimer_WhenItEnds()
    {
        var shell = Source("ViewerServerTab.xaml.cs");
        var finallyAt = shell.IndexOf("var slow = timer.Finish(", StringComparison.Ordinal);
        Assert.True(finallyAt >= 0);
        Assert.Contains("if (ReferenceEquals(_loadTimer, timer))", shell[finallyAt..], StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(7, "Wait Stats", "Wait Stats")]
    [InlineData(7, "  Memory  ", "Memory")]
    [InlineData(7, null, "inner tab 7")]
    [InlineData(7, "", "inner tab 7")]
    public void SlowLoadLine_NamesTheTab_ByItsHeader_AndByIndexOnlyWithoutOne(int index, string? header, string expected)
    {
        Assert.Equal(expected, ViewerServerTab.InnerTabLoadName(index, header));
    }

    [Fact]
    public void SlowLoadLine_FallsBackToTheIndex_WhenTheHeaderIsNotText()
    {
        Assert.Equal("inner tab 3", ViewerServerTab.InnerTabLoadName(3, new object()));
    }

    [Theory]
    [InlineData("private async Task LoadTopQueriesAsync(")]
    [InlineData("private async Task LoadTopProceduresAsync(")]
    [InlineData("private async Task LoadQueryStoreAsync(")]
    public void AGridReadFailure_DoesNotWaitForTheSlicerRead(string loader)
    {
        var body = LoaderBody("ViewerServerTab.Queries.cs", loader);
        Assert.DoesNotContain("await ViewerDataService.ObserveAsync(slicerTask)", body, StringComparison.Ordinal);
        Assert.Contains("_ = ViewerDataService.ObserveAsync(slicerTask)", body, StringComparison.Ordinal);
    }

    [Fact]
    public void StatusBarStoreSize_FallsBackToTheLiveDirectory_WhenTheRecordedReadFails()
    {
        var source = Source("ViewerDataService.ServerStatus.cs");
        var start = source.IndexOf("private async Task<long?> FetchStoreSizeBytesAsync()", StringComparison.Ordinal);
        var body = source[start..];
        var recorded = body.IndexOf("StoreSelfMetrics.LatestStoreSizeSql", StringComparison.Ordinal);
        var guard = body.IndexOf("catch (Exception)", recorded, StringComparison.Ordinal);
        var live = body.IndexOf("CreateCommand(StoreSizeSql)", StringComparison.Ordinal);
        Assert.True(recorded >= 0 && guard > recorded && live > guard, "a failed recorded read must fall through to the live read");
    }

    /* ------------------------------------------------------------------ live pins */

    private static async Task<long> StartStatementCountingAsync(string connectionString, CancellationToken ct)
    {
        await using var probe = new NpgsqlConnection(connectionString);
        await probe.OpenAsync(ct);
        await using (var preloadCmd = new NpgsqlCommand("SELECT current_setting('shared_preload_libraries')", probe))
        {
            var preload = (string)(await preloadCmd.ExecuteScalarAsync(ct))!;
            Assert.SkipWhen(!preload.Split(',').Select(s => s.Trim()).Contains("pg_stat_statements"),
                $"pg_stat_statements is not in shared_preload_libraries ('{preload}').");
        }

        await using (var ext = new NpgsqlCommand("CREATE EXTENSION IF NOT EXISTS pg_stat_statements", probe))
        {
            await ext.ExecuteNonQueryAsync(ct);
        }

        await using var oidCmd = new NpgsqlCommand("SELECT oid FROM pg_database WHERE datname = current_database()", probe);
        var dbId = (uint)(await oidCmd.ExecuteScalarAsync(ct))!;
        await using var reset = new NpgsqlCommand("SELECT pg_stat_statements_reset(0, $1, 0)", probe);
        reset.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = dbId });
        await reset.ExecuteNonQueryAsync(ct);
        return dbId;
    }

    private static async Task<long> CountCallsAsync(string connectionString, long dbId, string likePattern, CancellationToken ct)
    {
        await using var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new NpgsqlCommand("SELECT COALESCE(SUM(calls), 0)::bigint FROM pg_stat_statements WHERE dbid = $1 AND query LIKE $2", conn);
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Oid, Value = (uint)dbId });
        cmd.Parameters.AddWithValue(likePattern);
        return (long)(await cmd.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task HourlyTextLookup_IsOneStatement_ForAnyNumberOfRows_AndAnswersEachRowsOwnText()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live query-tab load pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            await using var seed = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (7001, 'tabload', 'tabload', TRUE, 15, now() at time zone 'utc', now() at time zone 'utc');
INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, delta_execution_count)
SELECT 1000 + q, (now() at time zone 'utc') - interval '5 minutes', 7001, 'tabload', 'db' || (q % 3), '0xH' || q,
       CASE WHEN q = 7 THEN NULL ELSE 'SELECT ' || q END, 1
FROM generate_series(1, 30) q", setup);
            await seed.ExecuteNonQueryAsync(ct);
        }

        var dbId = await StartStatementCountingAsync(scratch.ConnectionString, ct);
        var bodySucceeded = false;
        await using var service = new ViewerDataService(scratch.ConnectionString);
        try
        {
            var databases = Enumerable.Range(1, 31).Select(q => "db" + (q % 3)).ToArray();
            var hashes = Enumerable.Range(1, 31).Select(q => "0xH" + q).ToArray();
            var texts = await service.ReadHourlyQueryTextsAsync(7001, databases, hashes, ct);

            Assert.Equal(31, texts.Length);
            Assert.Equal("SELECT 1", texts[0]);
            Assert.Equal("SELECT 30", texts[29]);
            Assert.Equal("", texts[6]);   /* q = 7 has no text anywhere */
            Assert.Equal("", texts[30]);  /* q = 31 has no row at all */

            var calls = await CountCallsAsync(scratch.ConnectionString, dbId, "%v_query_stats%", ct);
            Assert.True(calls == 1, $"31 rows' text must take ONE store call, not one each; saw {calls}");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task StatusBarStoreSize_ComesFromTheSweepsRecordedRow_WithoutMeasuringTheLiveDirectory()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live query-tab load pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            await using var seed = new NpgsqlCommand(
                "INSERT INTO collect.store_metrics (metric_time, object_name, object_kind, total_bytes) VALUES (now() at time zone 'utc', 'store', $1, 123456789)", setup);
            seed.Parameters.AddWithValue(StoreSelfMetrics.StoreObjectKind);
            await seed.ExecuteNonQueryAsync(ct);
        }

        var dbId = await StartStatementCountingAsync(scratch.ConnectionString, ct);
        var bodySucceeded = false;
        await using var service = new ViewerDataService(scratch.ConnectionString);
        try
        {
            Assert.Equal(123456789L, await service.GetStoreSizeBytesAsync(ct));
            var live = await CountCallsAsync(scratch.ConnectionString, dbId, "%pg_database_size%current_database%", ct);
            Assert.True(live == 0, $"a recorded size must not trigger the live directory walk; saw {live} calls");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task HourlyTextLookup_KeepsTheNewestTextPerKey_AndKeepsDatabasesApart()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live query-tab load pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            await using var seed = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES (7002, 'tabtext', 'tabtext', TRUE, 15, now() at time zone 'utc', now() at time zone 'utc');
INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text, delta_execution_count)
SELECT v.id, (now() at time zone 'utc') - v.age, 7002, 'tabtext', v.db, v.hash, v.txt, 1
FROM (VALUES
    (2001, interval '3 hours', 'dbA', '0xA', 'old text'),
    (2002, interval '1 hour',  'dbA', '0xA', 'new text'),
    (2003, interval '3 hours', 'dbA', '0xB', 'kept text'),
    (2004, interval '1 hour',  'dbA', '0xB', NULL),
    (2005, interval '2 hours', 'dbB', '0xA', 'other database text')
) AS v(id, age, db, hash, txt)", setup);
            await seed.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        await using var service = new ViewerDataService(scratch.ConnectionString);
        try
        {
            var texts = await service.ReadHourlyQueryTextsAsync(7002, new[] { "dbA", "dbA", "dbB", "dbB" }, new[] { "0xA", "0xB", "0xA", "0xB" }, ct);
            Assert.Equal("new text", texts[0]);           /* two texts for one key: the newest wins, as the per-row read did */
            Assert.Equal("kept text", texts[1]);          /* a newer row with no text does not blank an older text */
            Assert.Equal("other database text", texts[2]); /* the same hash in another database is its own key */
            Assert.Equal("", texts[3]);                   /* no row for the key */
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task StatusBarStoreSize_MeasuresTheLiveDirectoryOnce_WhenNoSizeIsRecorded()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live query-tab load pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
        }

        var dbId = await StartStatementCountingAsync(scratch.ConnectionString, ct);
        var bodySucceeded = false;
        await using var service = new ViewerDataService(scratch.ConnectionString);
        try
        {
            var size = await service.GetStoreSizeBytesAsync(ct);
            Assert.True(size > 0, "a store with no recorded size reads its live size");
            var live = await CountCallsAsync(scratch.ConnectionString, dbId, "%pg_database_size%current_database%", ct);
            Assert.True(live == 1, $"one live directory walk expected; saw {live}");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task StatusBarStoreSize_StillAnswers_WhenTheRecordedRowCannotBeRead()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the live query-tab load pins.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var setup = new NpgsqlConnection(scratch.ConnectionString))
        {
            await setup.OpenAsync(ct);
            await PgMigrations.MigrateAsync(setup, ct);
            /* A store the viewer's role cannot read the self-metrics table of fails the recorded read the same way a missing table does. */
            await using var drop = new NpgsqlCommand("DROP TABLE collect.store_metrics CASCADE", setup);
            await drop.ExecuteNonQueryAsync(ct);
        }

        var bodySucceeded = false;
        await using var service = new ViewerDataService(scratch.ConnectionString);
        try
        {
            var size = await service.GetStoreSizeBytesAsync(ct);
            Assert.True(size > 0, "a failed recorded read falls back to the live size and does not leave the field blank");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
