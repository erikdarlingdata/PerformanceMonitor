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
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5653: the startup repair's cost and its failure behaviour. A reporter's start ran the archive survey twice,
/// rewrote a 199 MiB file with a 2.7 GiB spill, and failed with the disk full.
/// </summary>
public sealed class SliceRepairStartupTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 9653;

    private readonly DuckDbInitializer _duckDb;
    private readonly string _archivePath;
    private readonly Func<string, long?> _originalFreeBytesProvider;

    public SliceRepairStartupTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _originalFreeBytesProvider = _duckDb.AvailableFreeBytesProvider;
        _archivePath = Path.Combine(Path.GetTempPath(), $"pm-5653-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_archivePath);

        /* The marker and attempt tables survive ResetData by design; drop them so each test starts unrepaired. */
        using var connection = _duckDb.CreateConnection();
        connection.Open();
        using var drop = connection.CreateCommand();
        drop.CommandText =
            $"DROP TABLE IF EXISTS {QueryStoreSliceRepairService.MarkerTable}; " +
            $"DROP TABLE IF EXISTS {QueryStoreSliceRepairService.AttemptTable}";
        drop.ExecuteNonQuery();
    }

    public void Dispose()
    {
        _duckDb.AvailableFreeBytesProvider = _ => 1L << 40;
        try
        {
            if (Directory.Exists(_archivePath))
            {
                Directory.Delete(_archivePath, recursive: true);
            }
        }
        catch (IOException)
        {
            /* A scratch directory that outlives one test run is noise, not a failure. */
        }
    }

    /// <summary>
    /// A startup that has work to do surveys exactly once. The survey is a hot-table GROUP BY plus three scans of
    /// every archive file, and the repair used to run it again as its own first step.
    /// </summary>
    [Fact]
    public async Task StartupRepair_SurveysOnce_NotOncePerStep()
    {
        await WriteSplitArchiveAsync(Path.Combine(_archivePath, "202605_query_store_stats.parquet"));

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.True(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, service.SurveysRun);
    }

    /// <summary>The public API still surveys for itself: a caller that hands over no survey gets a fresh one.</summary>
    [Fact]
    public async Task RepairAsync_WithoutASurvey_StillSurveysForItself()
    {
        await WriteSplitArchiveAsync(Path.Combine(_archivePath, "202605_query_store_stats.parquet"));

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        var result = await service.RepairAsync(cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(result.FullyRepaired);
        Assert.Equal(1, service.SurveysRun);
    }

    /// <summary>
    /// The survey handed in at startup can be stale by the time the repair uses it: archiving or compaction may
    /// have removed a file in between. A vanished file is skipped, not reported as a repair failure that would
    /// withhold the completion marker for good.
    /// </summary>
    [Fact]
    public async Task RepairWithAHandedInSurvey_SkipsAFileThatHasVanished()
    {
        var file = Path.Combine(_archivePath, "202605_query_store_stats.parquet");
        await WriteSplitArchiveAsync(file);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        var survey = await service.SurveyAsync(TestContext.Current.CancellationToken);
        Assert.True(survey.HasWork);
        File.Delete(file);

        var result = await service.RepairAsync(survey: survey, cancellationToken: TestContext.Current.CancellationToken);

        Assert.True(result.FullyRepaired);
        Assert.Equal(1, service.SurveysRun);
    }

    /// <summary>
    /// #5693: the startup repair reports its progress lines before it returns. They used to go through
    /// <c>Progress&lt;T&gt;</c>, which posts each report to the captured synchronization context (the thread pool
    /// when there is none), so they were logged late and on another thread. The test runs the repair under a context
    /// that records every post made by a <c>Progress&lt;T&gt;</c>: the old code posts at least one, the inline
    /// reporter posts none. That makes it fail every run, not only when the pool is slow.
    /// </summary>
    [Fact]
    public async Task StartupRepair_LogsItsProgressLines_BeforeItReturns()
    {
        await WriteSplitArchiveAsync(Path.Combine(_archivePath, "202605_query_store_stats.parquet"));

        var log = new CapturingLogger();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, log);
        var context = new ProgressPostCountingContext();
        var previous = SynchronizationContext.Current;
        SynchronizationContext.SetSynchronizationContext(context);
        try
        {
            await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        Assert.Equal(0, context.ProgressPosts);

        var progress = log.Entries
            .Where(l => l.Level == LogLevel.Information && l.Message.StartsWith("#1912 ", StringComparison.Ordinal))
            .Select(l => l.Message).ToList();
        var archive = progress.FindIndex(m => m.Contains("Archive 202605_query_store_stats.parquet:", StringComparison.Ordinal));
        var views = progress.FindIndex(m => m.Contains("Archive views rebuilt", StringComparison.Ordinal));
        Assert.True(archive >= 0 && archive < views, "the archive line and the views line must both be logged, in that order");

        var countAtReturn = log.Entries.Count;
        await Task.Delay(250, TestContext.Current.CancellationToken);
        Assert.Equal(countAtReturn, log.Entries.Count);
    }

    /// <summary>Forwards every post to the thread pool, and counts the ones that come from a <c>Progress&lt;T&gt;</c>.</summary>
    private sealed class ProgressPostCountingContext : SynchronizationContext
    {
        private int _progressPosts;

        public int ProgressPosts => Volatile.Read(ref _progressPosts);

        public override void Post(SendOrPostCallback d, object? state)
        {
            var owner = d.Target?.GetType();
            if (owner is { IsGenericType: true } && owner.GetGenericTypeDefinition() == typeof(Progress<>))
            {
                Interlocked.Increment(ref _progressPosts);
            }

            ThreadPool.QueueUserWorkItem(_ =>
            {
                SetSynchronizationContext(this);
                d(state);
            });
        }
    }

    /// <summary>
    /// #5653: with too little free disk for the rewrite, the file is left alone and the run says why, instead of
    /// starting a COPY that dies part way through with the disk full.
    /// </summary>
    [Fact]
    public async Task Rewrite_WithTooLittleFreeDisk_SkipsTheFile_AndSaysWhy()
    {
        var file = Path.Combine(_archivePath, "202605_query_store_stats.parquet");
        await WriteSplitArchiveAsync(file);
        var before = File.ReadAllBytes(file);
        _duckDb.AvailableFreeBytesProvider = _ => 1024 * 1024;

        var log = new CapturingLogger();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, log);
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.Equal(before, File.ReadAllBytes(file));
        Assert.False(File.Exists(file + ".repair-tmp"));
        Assert.False(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Contains(log.Entries, l => l.Level == LogLevel.Warning && l.Message.Contains("free disk space", StringComparison.Ordinal)
            && l.Message.Contains("reserve", StringComparison.Ordinal) && l.Message.Contains("rewritten copy", StringComparison.Ordinal)
            && l.Message.Contains("temporary spill", StringComparison.Ordinal) && l.Message.Contains("attempt was used", StringComparison.Ordinal));
    }

    /// <summary>
    /// #5653: free space that the old check refused (it asked for 512 MiB of spill on top of the copy and the
    /// reserve) but that leaves the cap above its floor: the file is rewritten. Each skip used one of two startup
    /// attempts, so a roomy-enough disk that was skipped twice never got its repair.
    /// </summary>
    [Fact]
    public async Task Rewrite_WithRoomForTheCapAboveItsFloor_RewritesTheFile()
    {
        const long MiB = 1024 * 1024;
        var file = Path.Combine(_archivePath, "202605_query_store_stats.parquet");
        await WriteSplitArchiveAsync(file);
        var fileBytes = new FileInfo(file).Length;
        var reserve = DataVolumeSpace.CompactionReserveBytes(_duckDb.DatabasePath);
        _duckDb.AvailableFreeBytesProvider = _ => reserve + fileBytes + (fileBytes / 2) + (300 * MiB);

        var schemaBefore = await ParquetSchemaAsync(file);

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.True(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.False(File.Exists(file + ".repair-tmp"));

        /* This file is an old-era one (no runtime_stats_interval_id, no replica_role): the rewrite must hand back
           exactly the columns and parquet types it was given. */
        Assert.DoesNotContain("runtime_stats_interval_id", schemaBefore, StringComparison.Ordinal);
        Assert.Equal(schemaBefore, await ParquetSchemaAsync(file));
    }

    private async Task<string> ParquetSchemaAsync(string file)
    {
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT string_agg(name || ':' || type, ', ') FROM parquet_schema('" +
            file.Replace("\\", "/", StringComparison.Ordinal).Replace("'", "''", StringComparison.Ordinal) +
            "') WHERE name <> 'duckdb_schema'";
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture) ?? "";
    }

    /// <summary>
    /// #5653: the warning after an unfinished repair tells the truth about what happens next. After attempt 1 the
    /// next start retries; after the last attempt it does not, and the text says so and what the user can do.
    /// </summary>
    [Fact]
    public async Task UnfinishedRepair_WarningSaysWhetherItWillBeRetried_AndTheLastAttemptSaysWhatToDo()
    {
        await WriteSplitArchiveAsync(Path.Combine(_archivePath, "202605_query_store_stats.parquet"));
        _duckDb.AvailableFreeBytesProvider = _ => 1024 * 1024;

        var log = new CapturingLogger();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, log);

        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        var first = log.Entries.Single(l => l.Level == LogLevel.Warning && l.Message.Contains("file(s) unrepaired", StringComparison.Ordinal)).Message;
        Assert.Contains("will be retried on the next start", first, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT run again", first, StringComparison.Ordinal);

        log.Clear();
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        var last = log.Entries.Single(l => l.Level == LogLevel.Warning && l.Message.Contains("file(s) unrepaired", StringComparison.Ordinal)).Message;
        Assert.Contains("the last one", last, StringComparison.Ordinal);
        Assert.Contains("will NOT run again", last, StringComparison.Ordinal);
        Assert.Contains("free up disk space", last, StringComparison.Ordinal);
        Assert.Contains(QueryStoreSliceRepairService.AttemptTable, last, StringComparison.Ordinal);
        Assert.DoesNotContain("will be retried on the next start", last, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5653: a start that fails before it records its attempt row was not counted, and the next start runs the
    /// repair again. The log must not call it the last attempt or tell the user to drop the attempt table, which
    /// would reset a cap this start never used. One attempt is already recorded here, so attempt number 2 is the
    /// number this start would have had.
    /// </summary>
    [Fact]
    public async Task StartThatFailsBeforeRecordingItsAttempt_IsNotCalledTheLastOne_AndTheNextStartRunsTheRepair()
    {
        await WriteSplitArchiveAsync(Path.Combine(_archivePath, "202605_query_store_stats.parquet"));
        _duckDb.AvailableFreeBytesProvider = _ => 1024 * 1024;

        var log = new CapturingLogger();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, log);

        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, await service.AttemptCountAsync(TestContext.Current.CancellationToken));

        log.Clear();
        service.BeforeSurveyHook = () => throw new InvalidOperationException("the survey ran out of memory");
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        var failed = log.Entries.Single(l => l.Level == LogLevel.Error && l.Message.Contains("could not run", StringComparison.Ordinal)).Message;
        Assert.DoesNotContain("the last one", failed, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT run again", failed, StringComparison.Ordinal);
        Assert.DoesNotContain(QueryStoreSliceRepairService.AttemptTable, failed, StringComparison.Ordinal);
        Assert.Contains("not counted", failed, StringComparison.Ordinal);
        Assert.Contains("will run again on the next start", failed, StringComparison.Ordinal);
        Assert.Equal(1, await service.AttemptCountAsync(TestContext.Current.CancellationToken));

        /* The next start, with the survey healthy and the disk roomy, does the repair. */
        service.BeforeSurveyHook = null;
        _duckDb.AvailableFreeBytesProvider = _ => 1L << 40;
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        Assert.True(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, await service.AttemptCountAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>An unreadable volume (null) counts as enough, as it does for compaction: the repair goes ahead.</summary>
    [Fact]
    public async Task Rewrite_WhenFreeSpaceCannotBeRead_StillRuns()
    {
        await WriteSplitArchiveAsync(Path.Combine(_archivePath, "202605_query_store_stats.parquet"));
        _duckDb.AvailableFreeBytesProvider = _ => null;

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.True(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
    }

    /// <summary>The spill cap is what the volume can give after the copy and the reserve, within the ceiling.</summary>
    [Fact]
    public void SpillCap_IsWhatTheVolumeCanGive_WithinTheCeiling()
    {
        const long MiB = 1024 * 1024;

        /* free 3.58 GiB, a 199 MiB file (copy 298.5 MiB), a 64 MiB reserve: the cap is the rest. */
        var free = 3666L * MiB;
        Assert.Equal(
            free - (199 * MiB) - (199 * MiB / 2) - (64 * MiB),
            QueryStoreSliceRepairService.RewriteSpillCapBytes(free, 199 * MiB, 64 * MiB));

        Assert.Equal(4096 * MiB, QueryStoreSliceRepairService.RewriteSpillCapBytes(100_000 * MiB, 199 * MiB, 64 * MiB));
        Assert.Equal(4096 * MiB, QueryStoreSliceRepairService.RewriteSpillCapBytes(null, 199 * MiB, 64 * MiB));
        Assert.True(QueryStoreSliceRepairService.RewriteSpillCapBytes(100 * MiB, 199 * MiB, 64 * MiB) < 0);

        Assert.Equal(150 * MiB, QueryStoreSliceRepairService.RewriteCopyBytes(100 * MiB));
        Assert.Equal(256 * MiB, QueryStoreSliceRepairService.RewriteSpillFloorBytes);
    }

    /// <summary>
    /// #5653: the rewrite runs out of its temp allowance. DuckDB fails the statement; the original is byte for byte
    /// as it was, no sibling is left behind, the file counts as a failure (so the marker is withheld), and the log
    /// line names temp space.
    /// </summary>
    [Fact]
    public async Task Rewrite_WhenItRunsOutOfTempSpace_LeavesTheOriginal_AndCountsAFailure()
    {
        const long MiB = 1024 * 1024;
        var file = Path.Combine(_archivePath, "202605_query_store_stats.parquet");
        await WriteSplitArchiveAsync(file);
        var before = File.ReadAllBytes(file);
        var reserve = DataVolumeSpace.CompactionReserveBytes(_duckDb.DatabasePath);

        /* A 2 MiB allowance (floor lowered so the file starts), then a statement that must spill far more. */
        _duckDb.AvailableFreeBytesProvider = _ => reserve + RewriteCopy(before.Length) + (2 * MiB);
        var log = new CapturingLogger();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, log)
        {
            SpillFloorBytes = 1 * MiB,
            InsideSpillCapHook = async connection =>
            {
                using var cmd = connection.CreateCommand();
                cmd.CommandText = "SET memory_limit = '16MB'";
                await cmd.ExecuteNonQueryAsync();
                cmd.CommandText = "SELECT count(*) FROM (SELECT i, hash(i) AS h, repeat('x', 100) AS pad FROM range(5000000) t(i) ORDER BY h)";
                await cmd.ExecuteScalarAsync();
            },
        };

        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.Equal(before, File.ReadAllBytes(file));
        Assert.False(File.Exists(file + ".repair-tmp"));
        Assert.False(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Contains(log.Entries, l => l.Level == LogLevel.Error && l.Message.Contains("202605_query_store_stats", StringComparison.Ordinal));
        Assert.Contains(log.Entries, l => l.Level == LogLevel.Warning && l.Message.Contains("temporary space", StringComparison.Ordinal));
    }

    /// <summary>
    /// #5653 round 1: the temp cap is a property of the rewrite's OWN DuckDB instance. The repair runs un-awaited at
    /// startup while the UI keeps reading, and <c>max_temp_directory_size</c> is instance-wide, so a cap set on the main
    /// instance would hold every other query to it (as low as 256 MiB) for as long as a rewrite runs. This reads the
    /// main instance's setting from inside the rewrite, while the cap is in force.
    /// </summary>
    [Fact]
    public async Task Rewrite_LeavesTheMainInstancesTempCapAlone_WhileItRuns()
    {
        const long MiB = 1024 * 1024;
        var file = Path.Combine(_archivePath, "202605_query_store_stats.parquet");
        await WriteSplitArchiveAsync(file);
        var reserve = DataVolumeSpace.CompactionReserveBytes(_duckDb.DatabasePath);
        _duckDb.AvailableFreeBytesProvider = _ => reserve + RewriteCopy(new FileInfo(file).Length) + (300 * MiB);

        string mainBefore;
        using (var before = _duckDb.CreateConnection())
        {
            await before.OpenAsync(TestContext.Current.CancellationToken);
            mainBefore = await SettingAsync(before);
        }

        string? mainDuring = null;
        string? rewriteDuring = null;
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath)
        {
            InsideSpillCapHook = async rewriteConnection =>
            {
                rewriteDuring = await SettingAsync(rewriteConnection);
                using var main = _duckDb.CreateConnection();
                await main.OpenAsync(TestContext.Current.CancellationToken);
                mainDuring = await SettingAsync(main);
            },
        };

        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.True(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Equal(mainBefore, mainDuring);
        Assert.Equal("300.0 MiB", rewriteDuring);
        Assert.False(Directory.Exists(Path.Combine(_archivePath, QueryStoreSliceRepairService.RewriteSpillFolderName)));
    }

    private static long RewriteCopy(long fileBytes) => QueryStoreSliceRepairService.RewriteCopyBytes(fileBytes);

    /// <summary>
    /// The rewrite instance carries the cap, the temp folder and the memory limit, and they stay on it: another
    /// instance in the same process (the store's) keeps its own.
    /// </summary>
    [Fact]
    public async Task RewriteInstance_CarriesItsOwnCap_AndAnotherInstanceKeepsItsOwn()
    {
        using var other = new DuckDBConnection("DataSource=:memory:");
        await other.OpenAsync(TestContext.Current.CancellationToken);
        var otherBefore = await SettingAsync(other);

        var spill = Path.Combine(_archivePath, "unit_spill");
        using (var rewrite = await QueryStoreSliceRepairService.OpenRewriteInstanceAsync(
            spill, 700L * 1024 * 1024, TestContext.Current.CancellationToken))
        {
            Assert.Equal("700.0 MiB", await SettingAsync(rewrite));
            Assert.Equal(otherBefore, await SettingAsync(other));
            Assert.Equal(spill.Replace('\\', '/'), (await ScalarAsync(rewrite, "current_setting('temp_directory')")).Replace('\\', '/'));

            /* DuckDB prints GB limits in GiB, so compare against what it prints for the same SET on a third instance. */
            using var expected = new DuckDBConnection("DataSource=:memory:");
            await expected.OpenAsync(TestContext.Current.CancellationToken);
            using (var set = expected.CreateCommand())
            {
                set.CommandText = $"SET memory_limit = '{ArchiveService.MainConnectionCopyMemoryLimit}'";
                await set.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            }

            Assert.Equal(
                await ScalarAsync(expected, "current_setting('memory_limit')"),
                await ScalarAsync(rewrite, "current_setting('memory_limit')"));
        }

        Assert.True(Directory.Exists(spill));
    }

    /// <summary>
    /// A setup step that fails after the connection opened: the error reaches the caller and the connection is
    /// disposed, not left open with nobody holding it.
    /// </summary>
    [Fact]
    public async Task RewriteInstance_WhenSetupFails_DisposesTheConnection_AndTheErrorSurfaces()
    {
        DuckDBConnection? handedOut = null;
        var openWhenHandedOut = false;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            using var instance = await QueryStoreSliceRepairService.OpenRewriteInstanceAsync(
                Path.Combine(_archivePath, "failing_spill"),
                700L * 1024 * 1024,
                TestContext.Current.CancellationToken,
                afterOpen: connection =>
                {
                    handedOut = connection;
                    openWhenHandedOut = connection.State == System.Data.ConnectionState.Open;
                    throw new InvalidOperationException("setup failed on purpose");
                });
        });

        Assert.Equal("setup failed on purpose", ex.Message);
        Assert.NotNull(handedOut);
        Assert.True(openWhenHandedOut);
        Assert.Equal(System.Data.ConnectionState.Closed, handedOut.State);
    }

    private static async Task<string> ScalarAsync(DuckDBConnection connection, string expression)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT CAST({expression} AS VARCHAR)";
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture) ?? "";
    }

    private static async Task<string> SettingAsync(DuckDBConnection connection)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT CAST(current_setting('max_temp_directory_size') AS VARCHAR)";
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture) ?? "";
    }

    private async Task WriteSplitArchiveAsync(string file)
    {
        using var readLock = _duckDb.AcquireReadLock(TestContext.Current.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $@"
COPY (
    SELECT * FROM (VALUES
        (1::BIGINT, TIMESTAMP '2026-05-10 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-05-10 09:55:00', TIMESTAMP '2026-05-10 10:00:00', 'SELECT 1', '0xH', 100::BIGINT, 1778::BIGINT, 998::BIGINT, 4708::BIGINT),
        (2::BIGINT, TIMESTAMP '2026-05-10 10:00:00', {ServerId}::INTEGER, 'SRV', 'DB', 1::BIGINT, 11::BIGINT, 'Regular',
         TIMESTAMP '2026-05-10 09:55:00', TIMESTAMP '2026-05-10 10:00:00', 'SELECT 1', '0xH', 25::BIGINT, 2245::BIGINT, 998::BIGINT, 5100::BIGINT),
        (3::BIGINT, TIMESTAMP '2026-05-10 10:05:00', {ServerId}::INTEGER, 'SRV', 'DB', 2::BIGINT, 22::BIGINT, 'Regular',
         TIMESTAMP '2026-05-10 10:01:00', TIMESTAMP '2026-05-10 10:05:00', 'SELECT 2', '0xH2', 55::BIGINT, 500::BIGINT, 400::BIGINT, 600::BIGINT)
    ) AS t(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
           execution_type_desc, first_execution_time, last_execution_time, query_text, query_hash,
           execution_count, avg_duration_us, min_duration_us, max_duration_us)
) TO '{file.Replace("'", "''", StringComparison.Ordinal)}' (FORMAT PARQUET, COMPRESSION ZSTD)";
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>
    /// Captures what the service logs. The list is written under a lock and read as a snapshot under the same lock
    /// (#5693), so a line logged from another thread cannot make a reader fail with "Collection was modified".
    /// </summary>
    internal sealed class CapturingLogger : ILogger<QueryStoreSliceRepairService>
    {
        private readonly List<(LogLevel Level, string Message)> _entries = [];
        private readonly object _gate = new();

        public IReadOnlyList<(LogLevel Level, string Message)> Entries
        {
            get { lock (_gate) { return [.. _entries]; } }
        }

        public void Clear() { lock (_gate) { _entries.Clear(); } }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            lock (_gate) { _entries.Add((logLevel, message)); }
        }
    }
}
