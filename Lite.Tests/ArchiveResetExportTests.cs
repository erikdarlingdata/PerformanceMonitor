/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The size-triggered <see cref="ArchiveService.ArchiveAllAndResetAsync"/> exports every table to parquet and
/// then deletes the database. A failed export (out of memory, a full disk, an I/O error) used to be logged
/// and the reset ran anyway, so that table's hot window was gone; and the exports wrote straight to their
/// final names, so a process killed mid-COPY left a truncated file that matched the archive glob, failed the
/// table's view at bind, and hid every archived month of that table. Now nothing is promoted until every
/// export succeeded, a failure leaves the database and the archive exactly as they were, and the next attempt
/// waits out a backoff instead of failing again a minute later.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class ArchiveResetExportTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveResetExportTests()
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

    private async Task ExecAsync(string sql)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<long> CountAsync(string table)
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken));
    }

    /* Two archivable tables with rows, plus a mute rule the reset must carry across. */
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
        await ExecAsync(@"
INSERT INTO config_mute_rules (id, enabled, created_at_utc, expires_at_utc, reason, server_name, metric_name)
VALUES ('rule-1', true, TIMESTAMP '2026-09-01 00:00:00', NULL, 'test', 'S1', 'Blocking Detected')");

        return initializer;
    }

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    [Fact]
    public async Task FailedExport_LeavesTheDatabaseAndTheArchiveUntouched_AndBacksOff()
    {
        var initializer = await SeedAsync();
        var service = new ArchiveService(initializer, _archiveDir);

        /* collection_log is the last archivable table, so config_alert_log's export is already on disk as a
           temp when this throws: the failure must take that temp with it. */
        var exportsStarted = 0;
        service.BeforeTableExportForTests = table =>
        {
            exportsStarted++;
            if (table == "collection_log")
                throw new IOException("There is not enough space on the disk.");
        };

        await service.ArchiveAllAndResetAsync();

        Assert.Equal(50, await CountAsync("collection_log"));
        Assert.Equal(20, await CountAsync("config_alert_log"));
        Assert.Equal(1, await CountAsync("config_mute_rules"));
        Assert.Empty(ArchiveFileNames());
        Assert.True(service.ResetRetryNotBeforeUtc > DateTime.UtcNow.AddMinutes(10), "the failed attempt did not schedule a backoff");

        /* The size check re-runs every minute; the failed attempt is not repeated until the backoff elapses. */
        var before = exportsStarted;
        await service.ArchiveAllAndResetAsync();
        Assert.Equal(before, exportsStarted);
        Assert.Equal(50, await CountAsync("collection_log"));

        /* Once the failure clears and the backoff is over, the reset goes through. */
        service.BeforeTableExportForTests = null;
        service.ResetRetryNotBeforeUtc = DateTime.MinValue;
        await service.ArchiveAllAndResetAsync();

        Assert.Equal(0, await CountAsync("collection_log"));
        Assert.Equal(0, await CountAsync("config_alert_log"));
        Assert.Equal(1, await CountAsync("config_mute_rules"));
        var files = ArchiveFileNames().Where(f => f.EndsWith(".parquet", StringComparison.Ordinal)).ToArray();
        Assert.Single(files, f => f.EndsWith("_collection_log.parquet", StringComparison.Ordinal));
        Assert.Single(files, f => f.EndsWith("_config_alert_log.parquet", StringComparison.Ordinal));
    }

    /// <summary>
    /// While later tables are still exporting, an earlier table's export exists only as a .tmp, which no
    /// archive glob matches: a kill at this point leaves nothing a view could bind to.
    /// </summary>
    [Fact]
    public async Task ExportsStayAsTempFiles_UntilEveryTableHasExported()
    {
        var initializer = await SeedAsync();
        var service = new ArchiveService(initializer, _archiveDir);

        string[]? filesWhenLastExportStarted = null;
        service.BeforeTableExportForTests = table =>
        {
            if (table == "collection_log")
                filesWhenLastExportStarted = ArchiveFileNames();
        };

        await service.ArchiveAllAndResetAsync();

        Assert.NotNull(filesWhenLastExportStarted);
        Assert.DoesNotContain(filesWhenLastExportStarted, f => f.EndsWith(".parquet", StringComparison.Ordinal));
        Assert.Contains(filesWhenLastExportStarted, f => f.EndsWith("_config_alert_log.parquet.tmp", StringComparison.Ordinal));

        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".tmp", StringComparison.Ordinal) || f == "archive_reset_pending.txt");
    }

    /// <summary>
    /// A failure after the files are promoted but before the database is reset leaves every row in the
    /// database; the promoted files would count the whole hot window twice, and again on every retry.
    /// </summary>
    [Fact]
    public async Task FailureBeforeTheReset_RemovesThePromotedFiles_SoARetryAddsNoSecondCopy()
    {
        var initializer = await SeedAsync();
        var service = new ArchiveService(initializer, _archiveDir);

        service.BeforeDatabaseResetForTests = () => throw new InvalidOperationException("reset refused");
        await service.ArchiveAllAndResetAsync();

        Assert.Equal(50, await CountAsync("collection_log"));
        Assert.Equal(20, await CountAsync("config_alert_log"));
        Assert.Empty(ArchiveFileNames());

        service.BeforeDatabaseResetForTests = null;
        service.ResetRetryNotBeforeUtc = DateTime.MinValue;
        await service.ArchiveAllAndResetAsync();

        Assert.Equal(0, await CountAsync("collection_log"));
        var files = ArchiveFileNames().Where(f => f.EndsWith(".parquet", StringComparison.Ordinal)).ToArray();
        Assert.Equal(2, files.Length);
    }

    /// <summary>
    /// The process died between promoting the files and resetting the database. The marker written before
    /// the promotion names them, and the next archival run removes them because the database still holds
    /// every row they contain.
    /// </summary>
    [Fact]
    public async Task FilesLeftByAKilledAttempt_AreRemovedOnTheNextRun()
    {
        var initializer = await SeedAsync();
        File.WriteAllText(Path.Combine(_archiveDir, "20260901_0000_collection_log.parquet"), "duplicate of rows still in the database");
        File.WriteAllLines(Path.Combine(_archiveDir, "archive_reset_pending.txt"), ["20260901_0000_collection_log.parquet"]);

        /* A cutoff nothing is older than: this run archives no rows of its own, only cleans up. */
        var service = new ArchiveService(initializer, _archiveDir);
        await service.ArchiveOldDataAsync(hotDataDays: 3650);

        Assert.DoesNotContain("20260901_0000_collection_log.parquet", ArchiveFileNames());
        Assert.DoesNotContain("archive_reset_pending.txt", ArchiveFileNames());
        Assert.Equal(50, await CountAsync("collection_log"));
    }
}
