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
        _duckDb.AvailableFreeBytesProvider = _originalFreeBytesProvider;
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

        var log = new List<(LogLevel Level, string Message)>();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, new CapturingLogger(log));
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.Equal(before, File.ReadAllBytes(file));
        Assert.False(File.Exists(file + ".repair-tmp"));
        Assert.False(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.Contains(log, l => l.Level == LogLevel.Warning && l.Message.Contains("free disk space", StringComparison.Ordinal)
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

        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath);
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);

        Assert.True(await service.AlreadyRepairedAsync(TestContext.Current.CancellationToken));
        Assert.False(File.Exists(file + ".repair-tmp"));
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

        var log = new List<(LogLevel Level, string Message)>();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, new CapturingLogger(log));

        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        var first = log.Single(l => l.Level == LogLevel.Warning && l.Message.Contains("file(s) unrepaired", StringComparison.Ordinal)).Message;
        Assert.Contains("will be retried on the next start", first, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT run again", first, StringComparison.Ordinal);

        log.Clear();
        await service.RepairOnStartupAsync(TestContext.Current.CancellationToken);
        var last = log.Single(l => l.Level == LogLevel.Warning && l.Message.Contains("file(s) unrepaired", StringComparison.Ordinal)).Message;
        Assert.Contains("the last one", last, StringComparison.Ordinal);
        Assert.Contains("will NOT run again", last, StringComparison.Ordinal);
        Assert.Contains("free up disk space", last, StringComparison.Ordinal);
        Assert.Contains(QueryStoreSliceRepairService.AttemptTable, last, StringComparison.Ordinal);
        Assert.DoesNotContain("will be retried on the next start", last, StringComparison.Ordinal);
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
        var log = new List<(LogLevel Level, string Message)>();
        var service = new QueryStoreSliceRepairService(_duckDb, _archivePath, new CapturingLogger(log))
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
        Assert.Contains(log, l => l.Level == LogLevel.Error && l.Message.Contains("202605_query_store_stats", StringComparison.Ordinal));
        Assert.Contains(log, l => l.Level == LogLevel.Warning && l.Message.Contains("temporary space", StringComparison.Ordinal));
    }

    private static long RewriteCopy(long fileBytes) => QueryStoreSliceRepairService.RewriteCopyBytes(fileBytes);

    /// <summary>The cap is in force while the action runs and the setting is put back after, even when the action throws.</summary>
    [Fact]
    public async Task SpillCap_AppliesDuringTheAction_AndIsRestoredAfter()
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        var original = await SettingAsync(connection);
        string? during = null;

        await QueryStoreSliceRepairService.WithRewriteSpillCap(connection, 700L * 1024 * 1024, async () =>
        {
            during = await SettingAsync(connection);
        });

        Assert.NotEqual(original, during);
        Assert.Contains("MiB", during, StringComparison.Ordinal);
        Assert.Equal(original, await SettingAsync(connection));

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            QueryStoreSliceRepairService.WithRewriteSpillCap(
                connection, 700L * 1024 * 1024, () => throw new InvalidOperationException("boom")));
        Assert.Equal(original, await SettingAsync(connection));
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

    internal sealed class CapturingLogger(List<(LogLevel Level, string Message)> sink) : ILogger<QueryStoreSliceRepairService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => sink.Add((logLevel, formatter(state, exception)));
    }
}
