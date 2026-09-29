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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The periodic archive exports a table's old rows to parquet, promotes the file, then deletes those rows. A
/// process killed between the promote and the DELETE left the rows in both places, and the next run exported
/// them again, so the archive held them twice for good. Each export now writes a per-table journal before the
/// promote, and the next run finishes or discards it before exporting or compacting anything. The same run
/// also removes the partial <c>.tmp</c> files a killed COPY leaves, and rebuilds the archive views even when
/// compaction throws. A seam that throws right after (or right before) the promote stands in for the kill.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveInterruptedRunTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveInterruptedRunTests()
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

    private string P(string fileName) => Path.Combine(_archiveDir, fileName).Replace("\\", "/");

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<long> ScalarAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    private static void ExecInMemory(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private void MakeParquet(string fileName, long from, long to) =>
        ExecInMemory($"COPY (SELECT i AS id, md5(i::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' (FORMAT PARQUET)");

    /* 50 old collection_log rows and 20 old config_alert_log rows, all far older than the 7-day cutoff. */
    private async Task<DuckDbInitializer> SeedAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 51) t(i)");
        await ExecAsync(@"
INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value)
SELECT TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 1, 'S1', 'Blocking Detected', i, 10 FROM range(1, 21) t(i)");

        return initializer;
    }

    /* Rows and distinct keys in the table's archive files (whole-month, per-cycle and part files alike). */
    private (long Rows, long Distinct) Archived(string table, string keyColumn)
    {
        var globs = new[] { $"*_{table}.parquet", $"*_{table}_pt???.parquet" }
            .Where(pattern => Directory.GetFiles(_archiveDir, pattern).Length > 0)
            .Select(pattern => $"'{P(pattern)}'")
            .ToList();
        if (globs.Count == 0)
        {
            return (0, 0);
        }

        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT count(*), count(DISTINCT {keyColumn}) FROM read_parquet([{string.Join(", ", globs)}])";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        return (Convert.ToInt64(reader.GetValue(0)), Convert.ToInt64(reader.GetValue(1)));
    }

    private static string NextHourTimestamp() => DateTime.UtcNow.AddHours(1).ToString("yyyyMMdd_HHmm");

    private string[] Journals() =>
        Directory.GetFiles(_archiveDir, "*.archive-pending*").Select(f => Path.GetFileName(f)).Order(StringComparer.Ordinal).ToArray();

    private string[] TempFiles() =>
        Directory.GetFiles(_archiveDir).Select(f => Path.GetFileName(f))
            .Where(f => f.EndsWith(".tmp", StringComparison.OrdinalIgnoreCase)).Order(StringComparer.Ordinal).ToArray();

    [Fact]
    public async Task RunKilledBetweenThePromoteAndTheDelete_IsFinishedByTheNextRun_WithEachRowKeptOnce()
    {
        var initializer = await SeedAsync();

        var killed = new ArchiveService(initializer, _archiveDir)
        {
            AfterPromoteForTests = table =>
            {
                if (table == "collection_log")
                {
                    throw new ArchiveService.SimulatedKillException();
                }
            }
        };
        await Assert.ThrowsAsync<ArchiveService.SimulatedKillException>(() => killed.ArchiveOldDataAsync(hotDataDays: 7));

        /* The state a kill leaves: the file is promoted and the rows are still in the table. */
        Assert.Equal(50, Archived("collection_log", "log_id").Rows);
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));

        /* The next hourly run: a later minute, so it names its files differently from the dead one. */
        var log = new CapturingLogger();
        await new ArchiveService(initializer, _archiveDir, log) { TimestampForTests = NextHourTimestamp() }.ArchiveOldDataAsync(hotDataDays: 7);

        Assert.Equal((50, 50), Archived("collection_log", "log_id"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));
        Assert.Equal((20, 20), Archived("config_alert_log", "alert_time"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM config_alert_log"));
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
        Assert.Empty(Journals());

        var recovered = log.Entries.Where(e => e.Level == LogLevel.Information
            && e.Message.Contains("collection_log", StringComparison.Ordinal)
            && e.Message.Contains("50 row", StringComparison.Ordinal)).ToList();
        Assert.Single(recovered);
    }

    [Fact]
    public async Task RunKilledBeforeThePromote_LeavesNoJournalAfterTheNextRun_AndLosesNoRow()
    {
        var initializer = await SeedAsync();

        var killed = new ArchiveService(initializer, _archiveDir)
        {
            BeforePromoteForTests = table =>
            {
                if (table == "collection_log")
                {
                    throw new ArchiveService.SimulatedKillException();
                }
            }
        };
        await Assert.ThrowsAsync<ArchiveService.SimulatedKillException>(() => killed.ArchiveOldDataAsync(hotDataDays: 7));

        /* Nothing was promoted, so the table still holds every row. */
        Assert.Equal(0, Archived("collection_log", "log_id").Rows);
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));

        await new ArchiveService(initializer, _archiveDir) { TimestampForTests = NextHourTimestamp() }.ArchiveOldDataAsync(hotDataDays: 7);

        Assert.Equal((50, 50), Archived("collection_log", "log_id"));
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
        Assert.Empty(Journals());
    }

    [Fact]
    public async Task ArchiveThatFailsItsDelete_LeavesNeitherTheFileNorAJournal()
    {
        var initializer = await SeedAsync();

        /* A view over the rows cannot be deleted from, so the DELETE after a good export and promote fails. */
        await ExecAsync("CREATE TABLE collection_log_rows AS SELECT * FROM collection_log");
        await ExecAsync("DROP TABLE collection_log");
        await ExecAsync("CREATE VIEW collection_log AS SELECT * FROM collection_log_rows");

        await new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));
        Assert.Equal(0, Archived("collection_log", "log_id").Rows);
        Assert.Empty(Journals());
    }

    [Fact]
    public async Task StaleTempFiles_AreRemovedAtTheStartOfTheNextRun()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        /* What a process killed inside a COPY, a compaction merge or a journal write leaves behind. */
        File.WriteAllText(P("20260901_0000_wait_stats.parquet.tmp"), "partial COPY");
        File.WriteAllText(P("202609_wait_stats.parquet.tmp"), "partial merge");
        File.WriteAllText(P("collection_log.archive-pending.tmp"), "partial journal");
        MakeParquet("202609_t.parquet", 0, 10);

        await new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);

        Assert.Empty(TempFiles());
        Assert.True(File.Exists(P("202609_t.parquet")), "a real archive file went with the temps");
    }

    [Fact]
    public async Task ATempNamedByASwapJournalThatIsStillLive_IsKept_WhileOtherTempsGo()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        MakeParquet("202609_t.parquet", 0, 1_000);              /* merged output, in place */
        MakeParquet("20260928_1400_t.parquet", 500, 1_000);      /* folded into the output, not yet deleted */
        File.WriteAllLines(P("202609_t.swap"),
        [
            "state|swapped",
            "output|replacing|202609_t.parquet",
            "input|20260928_1400_t.parquet"
        ]);
        File.WriteAllText(P("202609_t.parquet.tmp"), "named by the journal");
        File.WriteAllText(P("20260901_0000_wait_stats.parquet.tmp"), "partial COPY");

        /* An input that cannot be deleted keeps the journal alive through this run. */
        using (new FileStream(P("20260928_1400_t.parquet").Replace("/", "\\"), FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
        {
            await new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7);
        }

        Assert.True(File.Exists(P("202609_t.swap")), "the journal was dropped although its input could not be deleted");
        Assert.Equal(["202609_t.parquet.tmp"], TempFiles());
    }

    [Fact]
    public async Task CompactionThatThrows_StillRebuildsTheArchiveViews()
    {
        var initializer = await SeedAsync();

        /* Compaction creates its spill folder before it merges anything; a file of that name makes it throw. */
        File.WriteAllText(Path.Combine(_archiveDir, "duckdb_tmp"), "not a folder");

        await Assert.ThrowsAnyAsync<IOException>(() => new ArchiveService(initializer, _archiveDir).ArchiveOldDataAsync(hotDataDays: 7));

        /* The rows are only in the archive now, so a view that was not rebuilt does not see them. */
        Assert.Equal(0, await ScalarAsync("SELECT COUNT(*) FROM collection_log"));
        Assert.Equal(50, await ScalarAsync("SELECT COUNT(*) FROM v_collection_log"));
    }

    private sealed class CapturingLogger : ILogger<ArchiveService>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }
}
