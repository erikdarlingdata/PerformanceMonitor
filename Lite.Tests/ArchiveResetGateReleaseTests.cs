/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5733: the size-triggered reset took <see cref="CollectionResetGate"/> and kept it through the monthly parquet
/// compaction that follows the reset, so every collection round waited for a merge that grows through the month
/// (seconds on the 1st, minutes by month end). The gate is now released the moment the database is reset and
/// reinitialized, before compaction, and every exit path releases it exactly once.
///
/// <para>The scaling pin counts instead of timing: compaction work seen while the gate was still held is zero for
/// a small and a large backlog alike.</para>
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection the other reset tests use. */
[Collection("CollectionResetGate")]
public sealed class ArchiveResetGateReleaseTests : IDisposable
{
    private static readonly TimeSpan GateProbe = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan SeamWait = TimeSpan.FromSeconds(90);

    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;
    private readonly string _month = DateTime.UtcNow.ToString("yyyyMM", CultureInfo.InvariantCulture);

    /* The reset exports the live tables as yyyyMMdd_HHmm_<table>.parquet at the current minute. A seeded per-cycle file on the
       same day with a stamp at or before that minute (the 50 file run seeds 0000 to 0049) can take that very name, the export
       then fails, the reset gives up before compaction and the test waits for a compaction that never comes (#7055: a run at
       00:22 UTC on the 1st). The seeded day is therefore never today. */
    private readonly string _seedDay = DateTime.UtcNow.Day == 1 ? "02" : "01";

    public ArchiveResetGateReleaseTests()
    {
        CollectionResetGate.ResetForTests();
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        CollectionResetGate.ResetForTests();
        foreach (var initializer in _initializers)
        {
            initializer.Dispose();
        }

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

    /* The current month's group of collection_log: <paramref name="perCycleFiles"/> per-cycle files and the month's
       existing merged file, which compaction merges the new files with. The live table keeps rows too, so the reset
       exports a fresh per-cycle file into the same group. */
    private async Task<(TestLogger Log, ArchiveService Service)> SeedAsync(int perCycleFiles)
    {
        var initializer = new DuckDbInitializer(_dbPath);
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();

        await ExecAsync(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 11) t(i)");

        var mergedPath = Path.Combine(_archiveDir, $"{_month}_collection_log.parquet").Replace("\\", "/");
        await ExecAsync($"COPY collection_log TO '{mergedPath}' (FORMAT PARQUET)");
        var seedPath = Path.Combine(_archiveDir, $"{_month}{_seedDay}_0000_collection_log.parquet").Replace("\\", "/");
        await ExecAsync($"COPY collection_log TO '{seedPath}' (FORMAT PARQUET)");
        for (var i = 1; i < perCycleFiles; i++)
        {
            File.Copy(seedPath, Path.Combine(_archiveDir, $"{_month}{_seedDay}_{i / 60:D2}{i % 60:D2}_collection_log.parquet"));
        }

        await initializer.CreateArchiveViewsAsync();

        var log = new TestLogger();
        return (log, new ArchiveService(initializer, _archiveDir, log));
    }

    /* True when a collection can register within the probe window, i.e. the gate is not held. */
    private static async Task<bool> CollectionCanStartAsync()
    {
        using var cts = new CancellationTokenSource(GateProbe);
        try
        {
            (await CollectionResetGate.BeginCollectionAsync(cts.Token)).Dispose();
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    private string[] CollectionLogFiles() =>
        Directory.GetFiles(_archiveDir, "*_collection_log*.parquet").Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    [Theory]
    [InlineData(5)]
    [InlineData(50)]
    public async Task CompactionRunsAfterTheGateIsReleased_SoNoCompactionWorkHappensWhileCollectionIsHeld(int perCycleFiles)
    {
        var (_, service) = await SeedAsync(perCycleFiles);

        using var compactionReached = new ManualResetEventSlim(false);
        using var letCompactionFinish = new ManualResetEventSlim(false);
        var filesRewrittenWhileGateHeld = 0;
        var swapsWhileGateHeld = 0;
        var tempsSeen = 0;

        service.OnCompactionTempsReadyForTests = temps =>
        {
            tempsSeen += temps.Count;
            if (!CollectionCanStartAsync().GetAwaiter().GetResult())
                filesRewrittenWhileGateHeld += temps.Count;
            compactionReached.Set();
            Assert.True(letCompactionFinish.Wait(SeamWait), "the test never released compaction");
        };
        service.AfterCompactionSwapForTests = _ =>
        {
            if (!CollectionCanStartAsync().GetAwaiter().GetResult())
                swapsWhileGateHeld++;
        };

        var reset = Task.Run(() => service.ArchiveAllAndResetAsync(), TestContext.Current.CancellationToken);

        try
        {
            Assert.True(compactionReached.Wait(SeamWait), "compaction was never reached");

            /* Compaction is blocked mid-merge: a collection round must start now. */
            Assert.True(await CollectionCanStartAsync(), "collection is held while the reset compacts the archive");
            Assert.True(ArchiveService.IsArchiving, "the reset is still archiving until compaction ends");
        }
        finally
        {
            /* Also when an assertion above failed: a reset left running would hold ArchiveService's process-wide
               lock and turn every later test in the collection into a skipped reset. */
            letCompactionFinish.Set();
            await reset;
        }

        Assert.Equal(0, filesRewrittenWhileGateHeld);
        Assert.Equal(0, swapsWhileGateHeld);
        Assert.True(tempsSeen >= 1, "compaction did not run");
        Assert.False(ArchiveService.IsArchiving);

        /* Compaction still merges the month after the release: the merged file, no per-cycle file left. */
        var files = CollectionLogFiles();
        Assert.Contains($"{_month}_collection_log.parquet", files);
        Assert.DoesNotContain(files, f => Regex.IsMatch(f, @"^\d{8}_\d{4}_collection_log"));
    }

    [Theory]
    [InlineData("success")]
    [InlineData("export-fails")]
    [InlineData("fails-before-reset")]
    [InlineData("fails-after-reset-began")]
    [InlineData("compaction-throws")]
    public async Task EveryExitPath_ReleasesTheGateExactlyOnce(string path)
    {
        var (log, service) = await SeedAsync(3);

        switch (path)
        {
            case "export-fails":
                service.BeforeTableExportForTests = _ => throw new IOException("There is not enough space on the disk.");
                break;
            case "fails-before-reset":
                service.BeforeDatabaseResetForTests = () => throw new InvalidOperationException("reset refused");
                break;
            case "fails-after-reset-began":
                service.AfterDatabaseResetForTests = () => throw new InvalidOperationException("restore refused");
                break;
            case "compaction-throws":
                service.OnCompactionTempsReadyForTests = _ => throw new IOException("compaction refused");
                break;
        }

        /* A second release logs the measurement line twice, which Assert.Single below catches. (The gate's own
           ResetScope.Dispose is idempotent, so a double release would not throw.) */
        await service.ArchiveAllAndResetAsync();

        /* The gate is free at once: taken, it is held (a second take is refused), and free again after dispose. */
        var first = await CollectionResetGate.TryBeginResetAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(first);
        Assert.Null(await CollectionResetGate.TryBeginResetAsync(TestContext.Current.CancellationToken));
        first!.Dispose();
        var again = await CollectionResetGate.TryBeginResetAsync(TestContext.Current.CancellationToken);
        Assert.NotNull(again);
        again!.Dispose();
        Assert.Equal(0, CollectionResetGate.CollectionsInFlight);
        Assert.True(await CollectionCanStartAsync());
        Assert.False(ArchiveService.IsArchiving);

        /* The new field-measurement line: written once per reset that took the gate, with a non-negative number. */
        var lines = log.Entries.Where(e => e.Message.StartsWith("Database reset held collection for ", StringComparison.Ordinal)).ToList();
        var line = Assert.Single(lines);
        Assert.Equal(LogLevel.Information, line.Level);
        var match = Regex.Match(line.Message, @"for (\d+(?:[.,]\d+)?) s \((\d+) tables, (\d+) rows\)");
        Assert.True(match.Success, line.Message);
        Assert.True(double.Parse(match.Groups[1].Value.Replace(',', '.'), CultureInfo.InvariantCulture) >= 0);
        if (path == "success")
        {
            Assert.Equal(10, long.Parse(match.Groups[3].Value, CultureInfo.InvariantCulture));
        }

        /* The start line operators grep for is unchanged. */
        Assert.Contains(log.Entries, e => e.Message.StartsWith("Archiving ALL data to Parquet (prefix: ", StringComparison.Ordinal)
            && e.Message.EndsWith(") and resetting database", StringComparison.Ordinal));
    }

    private sealed class TestLogger : ILogger<ArchiveService>
    {
        private readonly object _gate = new();
        private readonly List<(LogLevel Level, string Message)> _entries = [];

        public List<(LogLevel Level, string Message)> Entries
        {
            get { lock (_gate) { return [.. _entries]; } }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (_gate) { _entries.Add((logLevel, formatter(state, exception))); }
        }
    }
}
