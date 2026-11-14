using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8788: the Store Health read, the housekeeping history and <c>get_store_health</c>. Every test runs on a temp folder.
/// The reset test runs the real <see cref="ArchiveService.ArchiveAllAndResetAsync"/>, which touches the process-wide reset gate
/// and archive lock, so the class joins the collection the other reset tests use.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class StoreHealthTests : IDisposable
{
    /* Bounds the hand-off spins below, so a failed hand-off ends the test instead of hanging it. */
    private const int SpinLimit = 2_000_000;

    private static readonly DateTime T1 = new(2026, 5, 1, 10, 0, 0, DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly string _historyPath;
    private readonly DuckDbInitializer _duckDb;

    public StoreHealthTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _historyPath = StoreHousekeepingHistory.PathFor(_dbPath);
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        CollectionResetGate.ResetForTests();
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

    private async Task SeedAsync(params string[] statements)
    {
        await _duckDb.InitializeFromTemplateAsync();
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        foreach (var sql in statements)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = sql;
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private static string Deadlock(long id) =>
        $"INSERT INTO deadlocks (deadlock_id, collection_time, server_id, server_name, deadlock_time) VALUES ({id}, TIMESTAMP '2026-05-01 10:10:00', 1, 'S1', TIMESTAMP '{T1:yyyy-MM-dd HH:mm:ss}')";

    private StoreHealthReader NewReader() => new(_duckDb, _archiveDir, new StoreHousekeepingHistory(_historyPath));

    private void WriteArchiveFile(string name, int bytes) => File.WriteAllBytes(Path.Combine(_archiveDir, name), new byte[bytes]);

    [Fact]
    public async Task Read_ReportsUsedMbFileSizeAndRowsPerTableLargestFirst()
    {
        await SeedAsync(Deadlock(1), Deadlock(2), Deadlock(3),
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) " +
            "VALUES (1, 1, 'S1', 'wait_stats', TIMESTAMP '2026-05-01 10:00:00', 'SUCCESS')");

        var snapshot = NewReader().Read();

        Assert.True(snapshot.Store.FileMb > 0, "the store file has a size on disk");
        Assert.NotNull(snapshot.Store.UsedMb);
        Assert.True(snapshot.Store.UsedMb > 0);
        Assert.Equal(512, snapshot.Store.ResetAtMb);
        Assert.Equal(snapshot.Store.FileMb / 512 * 100, snapshot.Store.FilePercentOfReset, 6);
        Assert.Null(snapshot.StoreNote);

        Assert.NotNull(snapshot.Tables);
        var deadlocks = Assert.Single(snapshot.Tables, t => t.Name == "deadlocks");
        var log = Assert.Single(snapshot.Tables, t => t.Name == "collection_log");
        Assert.Equal(3, deadlocks.EstimatedRows);
        Assert.Equal(1, log.EstimatedRows);
        Assert.True(snapshot.Tables.ToList().IndexOf(deadlocks) < snapshot.Tables.ToList().IndexOf(log), "largest first");
        Assert.Equal(snapshot.Tables.OrderByDescending(t => t.EstimatedRows).Select(t => t.EstimatedRows), snapshot.Tables.Select(t => t.EstimatedRows));
    }

    [Fact]
    public async Task Read_CountsTheWalBesideTheStoreFile()
    {
        await SeedAsync();
        File.WriteAllBytes(_dbPath + ".wal", new byte[2 * 1024 * 1024]);

        var snapshot = NewReader().Read();

        Assert.Equal(2.0, snapshot.Store.WalMb, 3);
    }

    [Fact]
    public void Read_WithNoStoreFile_SaysSoAndStillReadsTheArchiveFolder()
    {
        WriteArchiveFile("202601_wait_stats.parquet", 10);

        var snapshot = NewReader().Read();

        Assert.Equal(0, snapshot.Store.FileMb);
        Assert.Null(snapshot.Tables);
        Assert.Contains("does not exist", snapshot.StoreNote);
        Assert.Equal(1, snapshot.Archives.FileCount);
    }

    [Fact]
    public async Task Read_WhenTheStoreIsBusy_KeepsTheFileFactsAndSaysTablesAreMissing()
    {
        await SeedAsync(Deadlock(1));
        var reader = NewReader();
        reader.ReadLockBudget = TimeSpan.FromMilliseconds(50);

        /* A write lock is released on the thread that took it, so a helper thread holds it while this one reads. The two threads
           hand off through flags, not waits: the suite's lock-hold scan counts a wait as a hold, and this one lasts milliseconds. */
        var held = false;
        var release = false;
        var holder = Task.Run(() =>
        {
            using (_duckDb.AcquireWriteLock())
            {
                System.Threading.Volatile.Write(ref held, true);
                for (var i = 0; i < SpinLimit && !System.Threading.Volatile.Read(ref release); i++)
                {
                    System.Threading.Thread.Yield();
                }
            }
        }, TestContext.Current.CancellationToken);
        for (var i = 0; i < SpinLimit && !System.Threading.Volatile.Read(ref held); i++)
        {
            System.Threading.Thread.Yield();
        }

        var snapshot = reader.Read();
        System.Threading.Volatile.Write(ref release, true);
        await holder;

        Assert.True(snapshot.Store.FileMb > 0);
        Assert.Null(snapshot.Store.UsedMb);
        Assert.Null(snapshot.Tables);
        Assert.Contains("busy", snapshot.StoreNote);
    }

    [Fact]
    public void ReadArchiveFolder_CountsFilesBytesAndTheMonthRangeFromTheNames()
    {
        WriteArchiveFile("202601_wait_stats.parquet", 100);
        WriteArchiveFile("20260315_1200_query_stats.parquet", 200);
        WriteArchiveFile("20260402_deadlocks.parquet", 300);
        WriteArchiveFile("2025-11_wait_stats.parquet", 400);
        WriteArchiveFile("imported_202512_memory_stats.parquet", 500);
        WriteArchiveFile("notes.parquet", 600);
        /* Not archive data: a half-written export, a journal and a file in a subfolder are outside *.parquet at the top level. */
        WriteArchiveFile("20260402_deadlocks.parquet.tmp", 9999);
        WriteArchiveFile("20260402_deadlocks.archive-pending", 9999);
        Directory.CreateDirectory(Path.Combine(_archiveDir, "sub"));
        File.WriteAllBytes(Path.Combine(_archiveDir, "sub", "202001_old.parquet"), new byte[9999]);

        var info = StoreHealthReader.ReadArchiveFolder(_archiveDir);

        Assert.Equal(6, info.FileCount);
        Assert.Equal(100 + 200 + 300 + 400 + 500 + 600, info.TotalBytes);
        Assert.Equal(new DateTime(2025, 11, 1), info.OldestMonth);
        Assert.Equal(new DateTime(2026, 4, 1), info.NewestMonth);
        Assert.Equal(1, info.UndatedFiles);
        Assert.Equal(RetentionService.ArchiveRetentionMonths, info.RetentionMonths);
    }

    [Fact]
    public void ReadArchiveFolder_EmptyOrMissingFolder_HasNoMonths()
    {
        var empty = StoreHealthReader.ReadArchiveFolder(_archiveDir);
        var missing = StoreHealthReader.ReadArchiveFolder(Path.Combine(_tempDir, "nope"));

        Assert.Equal(0, empty.FileCount);
        Assert.Null(empty.OldestMonth);
        Assert.Null(empty.NewestMonth);
        Assert.Equal(0, missing.FileCount);
    }

    [Fact]
    public void History_RecordsNewestFirstAndKeepsAtMostTheCap()
    {
        var history = new StoreHousekeepingHistory(_historyPath);
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        for (var i = 0; i < StoreHousekeepingHistory.MaxEvents + 25; i++)
        {
            history.Record(new StoreHousekeepingEvent(start.AddMinutes(i), StoreHousekeepingHistory.KindArchive,
                StoreHousekeepingHistory.OutcomeOk, Rows: i + 1, Files: 1));
        }

        var read = history.Read();

        Assert.Equal(StoreHousekeepingHistory.MaxEvents, read.Count);
        Assert.Equal(StoreHousekeepingHistory.MaxEvents + 25, read[0].Rows);
        Assert.True(read.Zip(read.Skip(1)).All(p => p.First.TimeUtc > p.Second.TimeUtc));
    }

    [Fact]
    public void History_FoldsPassesThatDidNothingAndRepeatedFailuresButNotRealWorkOrNewErrors()
    {
        var history = new StoreHousekeepingHistory(_historyPath);
        var t = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        StoreHousekeepingEvent Archive(int minute, string outcome, long rows, string? message = null) =>
            new(t.AddMinutes(minute), StoreHousekeepingHistory.KindArchive, outcome, rows, rows > 0 ? 1 : 0, Message: message);

        history.Record(Archive(0, StoreHousekeepingHistory.OutcomeOk, 500));
        history.Record(Archive(1, StoreHousekeepingHistory.OutcomeOk, 0));
        history.Record(Archive(2, StoreHousekeepingHistory.OutcomeOk, 0));
        history.Record(Archive(3, StoreHousekeepingHistory.OutcomeFailed, 0, "disk full"));
        history.Record(Archive(4, StoreHousekeepingHistory.OutcomeFailed, 0, "disk full"));
        history.Record(Archive(5, StoreHousekeepingHistory.OutcomeFailed, 0, "locked"));

        var read = history.Read();

        Assert.Equal(4, read.Count);
        Assert.Equal("locked", read[0].Message);
        Assert.Equal(1, read[0].Count);
        Assert.Equal("disk full", read[1].Message);
        Assert.Equal(2, read[1].Count);
        Assert.Equal(t.AddMinutes(4), read[1].TimeUtc);
        Assert.Equal(StoreHousekeepingHistory.OutcomeOk, read[2].Outcome);
        Assert.Equal(2, read[2].Count);
        Assert.Equal(500, read[3].Rows);
    }

    [Fact]
    public void History_DamagedFile_ReadsEmptyAndTheNextRecordStartsOver()
    {
        File.WriteAllText(_historyPath, "{ not json");
        var history = new StoreHousekeepingHistory(_historyPath);

        Assert.Empty(history.Read());

        history.Record(new StoreHousekeepingEvent(DateTime.UtcNow, StoreHousekeepingHistory.KindRetention, StoreHousekeepingHistory.OutcomeOk, Files: 2));
        Assert.Single(history.Read());
    }

    [Fact]
    public async Task SizeReset_KeepsEarlierHistoryAndWritesItsOwnRecord()
    {
        await SeedAsync(Deadlock(1), Deadlock(2), Deadlock(3));
        var history = new StoreHousekeepingHistory(_historyPath);
        var before = new StoreHousekeepingEvent(
            new DateTime(2026, 4, 1, 3, 0, 0, DateTimeKind.Utc), StoreHousekeepingHistory.KindArchive, StoreHousekeepingHistory.OutcomeFailed, Message: "an earlier failure");
        history.Record(before);

        await new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance) { History = history }.ArchiveAllAndResetAsync();

        /* The reset replaced the store: the rows are in the archive now, not in the table. */
        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM main.deadlocks";
            Assert.Equal(0L, Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
        }

        var read = new StoreHousekeepingHistory(_historyPath).Read();
        Assert.Equal(2, read.Count);
        var reset = read[0];
        Assert.Equal(StoreHousekeepingHistory.KindSizeReset, reset.Kind);
        Assert.Equal(StoreHousekeepingHistory.OutcomeOk, reset.Outcome);
        Assert.True(reset.Rows >= 3, "the reset names the rows it moved");
        Assert.True(reset.Files >= 1);
        Assert.NotNull(reset.UsedMb);
        Assert.Equal(before, read[1]);
    }

    [Fact]
    public async Task Retention_RecordsTheFilesItDeleted()
    {
        await SeedAsync();
        WriteArchiveFile("202001_wait_stats.parquet", 10);
        WriteArchiveFile("202002_wait_stats.parquet", 10);
        var history = new StoreHousekeepingHistory(_historyPath);

        var deleted = await new RetentionService(_archiveDir, NullLogger<RetentionService>.Instance) { History = history }
            .CleanupOldArchivesAndRefreshViewsAsync(_duckDb);

        Assert.Equal(2, deleted);
        var entry = Assert.Single(history.Read());
        Assert.Equal(StoreHousekeepingHistory.KindRetention, entry.Kind);
        Assert.Equal(StoreHousekeepingHistory.OutcomeOk, entry.Outcome);
        Assert.Equal(2, entry.Files);
    }

    [Fact]
    public async Task TimedArchive_RecordsWhatItMoved()
    {
        await SeedAsync(Deadlock(1), Deadlock(2));
        var history = new StoreHousekeepingHistory(_historyPath);

        /* The seeded rows are from 2026-05-01, older than the hot window whenever this test runs after that date. */
        var complete = await new ArchiveService(_duckDb, _archiveDir, NullLogger<ArchiveService>.Instance) { History = history }.ArchiveOldDataAsync();

        Assert.True(complete);
        var entry = Assert.Single(history.Read());
        Assert.Equal(StoreHousekeepingHistory.KindArchive, entry.Kind);
        Assert.Equal(StoreHousekeepingHistory.OutcomeOk, entry.Outcome);
        Assert.True(entry.Rows >= 2);
        Assert.True(entry.Files >= 1);
    }

    [Fact]
    public async Task GetStoreHealthTool_ReturnsTheFourSections()
    {
        await SeedAsync(Deadlock(1));
        WriteArchiveFile("202601_wait_stats.parquet", 10);
        new StoreHousekeepingHistory(_historyPath).Record(new StoreHousekeepingEvent(
            new DateTime(2026, 4, 1, 3, 0, 0, DateTimeKind.Utc), StoreHousekeepingHistory.KindSizeReset, StoreHousekeepingHistory.OutcomeFailed, Message: "the export failed"));

        var text = await McpStoreHealthTools.GetStoreHealth(NewReader());

        Assert.Contains("STORE", text);
        Assert.Contains("starts a fresh store when the store file reaches 512 MB", text);
        Assert.Contains("ARCHIVES", text);
        Assert.Contains("1 parquet files", text);
        Assert.Contains("Oldest month: 2026-01", text);
        Assert.Contains("HOUSEKEEPING HISTORY", text);
        Assert.Contains("Size reset  FAILED", text);
        Assert.Contains("the export failed", text);
        Assert.Contains("TABLES", text);
        Assert.Contains("deadlocks: 1", text);
    }
}
