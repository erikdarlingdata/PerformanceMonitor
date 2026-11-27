using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9425: the build history, one row per Lite start. Every test runs on a temp folder with a store the test seeds itself.
/// </summary>
// BuildHistory reads collection_log under TryAcquireReadLock(2 s), the store lock is process-wide, and another class holding it makes that read give null (#9465).
[Collection("CollectionResetGate")]
public sealed class BuildHistoryTests : IDisposable
{
    private static readonly DateTime Start1 = new(2026, 5, 1, 10, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Start2 = new(2026, 5, 2, 9, 0, 0, DateTimeKind.Utc);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public BuildHistoryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
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

    private BuildHistory NewHistory() => new(BuildHistory.PathFor(_dbPath));

    private static StoreHousekeepingHistory NewHousekeeping(string dbPath) => new(StoreHousekeepingHistory.PathFor(dbPath));

    /// <summary>One Lite start: open the store the way Lite does, then record the row the way MainWindow does.</summary>
    private async Task StartAsync(string full, DateTime nowUtc)
    {
        using var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();
        await BuildHistory.RecordStartAsync(
            duckDb, NewHistory(), NewHousekeeping(_dbPath), BuildHistory.StripHash(full), full, nowUtc);
    }

    private static void Exec(string dbPath, string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={dbPath}");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task AStart_WritesOneRowWithTheBuildTheFullVersionAndTheSchemaVersions()
    {
        File.WriteAllBytes(Path.Combine(_archiveDir, "202601_wait_stats.parquet"), new byte[10]);

        await StartAsync("3.13.0+0123abcd", Start1);

        var row = Assert.Single(NewHistory().Read());
        Assert.Equal("3.13.0", row.Build);
        Assert.Equal("3.13.0+0123abcd", row.FullVersion);
        Assert.Equal(Start1, row.StartedUtc);
        Assert.Equal(0, row.SchemaBefore);
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion, row.SchemaAfter);
        Assert.Equal(1, row.ArchiveFiles);
        Assert.Null(row.PreviousBuild);
    }

    [Fact]
    public async Task AStartThatMigrates_RecordsTheVersionBeforeAndAfter()
    {
        await StartAsync("3.12.0", Start1);
        Exec(_dbPath, $"UPDATE schema_version SET version = {DuckDbInitializer.CurrentSchemaVersion - 1}");

        await StartAsync("3.13.0", Start2);

        var newest = NewHistory().Read()[0];
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion - 1, newest.SchemaBefore);
        Assert.Equal(DuckDbInitializer.CurrentSchemaVersion, newest.SchemaAfter);
        Assert.True(newest.MigrationMs >= 0);
    }

    [Fact]
    public async Task ASecondStart_WritesASecondRowNamingTheFirstBuildAndWhenTheFirstRunWasLastSeen()
    {
        await StartAsync("3.12.0+aaaa", Start1);
        var lastHousekeeping = Start1.AddMinutes(20);
        var lastCollection = Start1.AddMinutes(30);
        NewHousekeeping(_dbPath).Record(new StoreHousekeepingEvent(lastHousekeeping, StoreHousekeepingHistory.KindArchive, StoreHousekeepingHistory.OutcomeOk));
        Exec(_dbPath,
            $"INSERT INTO collection_log (log_id, server_id, collector_name, collection_time, status) VALUES (1, 1, 'wait_stats', TIMESTAMP '{lastCollection:yyyy-MM-dd HH:mm:ss}', 'SUCCESS')");

        await StartAsync("3.13.0+bbbb", Start2);

        var rows = NewHistory().Read();
        Assert.Equal(2, rows.Count);
        Assert.Equal("3.13.0", rows[0].Build);
        Assert.Equal("3.12.0", rows[0].PreviousBuild);
        Assert.Null(rows[0].LastSeenUtc);
        Assert.Equal("3.12.0", rows[1].Build);
        Assert.Equal(lastCollection, rows[1].LastSeenUtc);
    }

    [Fact]
    public async Task ASecondStart_WithOnlyAHousekeepingEntry_UsesItAsTheLastSeenTime()
    {
        await StartAsync("3.12.0", Start1);
        var lastHousekeeping = Start1.AddHours(2);
        NewHousekeeping(_dbPath).Record(new StoreHousekeepingEvent(lastHousekeeping, StoreHousekeepingHistory.KindRetention, StoreHousekeepingHistory.OutcomeOk));

        await StartAsync("3.12.0", Start2);

        var rows = NewHistory().Read();
        Assert.Equal(lastHousekeeping, rows[1].LastSeenUtc);
        Assert.Equal("3.12.0", rows[0].PreviousBuild);
    }

    [Fact]
    public void TheHistory_KeepsTheNewest200Rows()
    {
        var history = NewHistory();
        for (var i = 1; i <= BuildHistory.MaxEntries + 5; i++)
        {
            history.Record(new BuildStartEntry(Start1.AddMinutes(i), $"3.0.{i}", $"3.0.{i}", 85, 85, 0, 0), null);
        }

        var rows = history.Read();
        Assert.Equal(200, rows.Count);
        Assert.Equal($"3.0.{BuildHistory.MaxEntries + 5}", rows[0].Build);
        Assert.Equal("3.0.6", rows[^1].Build);
    }

    [Fact]
    public async Task AFailedRowWrite_DoesNotFailTheStart()
    {
        /* A folder where the history file belongs: the write cannot replace it. */
        Directory.CreateDirectory(BuildHistory.PathFor(_dbPath));

        await StartAsync("3.13.0", Start1);

        var history = NewHistory();
        history.Record(new BuildStartEntry(Start1, "3.13.0", "3.13.0", 85, 85, 0, 0), null);
        Assert.Empty(history.Read());
    }

    [Fact]
    public async Task GetStoreHealth_ShowsTheBuildHistorySectionNewestFirstAndCappedAt20()
    {
        using var duckDb = new DuckDbInitializer(_dbPath);
        await duckDb.InitializeAsync();
        var history = NewHistory();
        for (var i = 1; i <= 25; i++)
        {
            history.Record(new BuildStartEntry(Start1.AddDays(i), $"3.0.{i}", $"3.0.{i}+x", 84, 85, 12, 3), Start1.AddDays(i).AddHours(1));
        }

        var reader = new StoreHealthReader(duckDb, _archiveDir, NewHousekeeping(_dbPath));
        var text = await McpStoreHealthTools.GetStoreHealth(reader);

        Assert.Contains("BUILD HISTORY", text);
        var section = text[text.IndexOf("BUILD HISTORY", StringComparison.Ordinal)..];
        var lines = section.Split(Environment.NewLine).Where(l => l.Contains(" UTC  build ")).ToList();
        Assert.Equal(20, lines.Count);
        Assert.Contains("build 3.0.25 ", lines[0]);
        Assert.Contains("build 3.0.6 ", lines[^1]);
        Assert.Contains("schema v84 to v85, migrations 12 ms, 3 archive files at start", lines[0]);
        Assert.Contains("... and 5 older starts.", section);
    }

    [Fact]
    public void TheArchiveFreeRead_ReturnsTheRowsWithoutAStoreOrAnArchiveFolder()
    {
        var history = NewHistory();
        history.Record(new BuildStartEntry(Start1, "3.12.0", "3.12.0", 84, 85, 5, 0), null);
        history.Record(new BuildStartEntry(Start2, "3.13.0", "3.13.0", 85, 85, 0, 0), Start1.AddHours(1));

        using var duckDb = new DuckDbInitializer(_dbPath);
        var rows = new StoreHealthReader(duckDb, Path.Combine(_tempDir, "no-such-archive"), NewHousekeeping(_dbPath)).ReadBuildHistory();

        Assert.Equal(["3.13.0", "3.12.0"], rows.Select(r => r.Build).ToArray());
        Assert.Equal("3.12.0", rows[0].PreviousBuild);
    }
}
