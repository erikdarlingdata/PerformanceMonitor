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
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9188: a <c>--stop</c> that arrives while Parquet compaction runs. Compaction takes the shutdown token, checks it before
/// each group and before each batch's merge (never inside one), logs its outcome in every case, and never leaves a state a
/// read or the next run trips on: a merged file is written to a <c>.tmp</c> name the archive views' globs do not match,
/// the group's inputs are removed only after every merged file is in place, and a cancelled run leaves the inputs as they
/// were. The tests drive <see cref="ArchiveService.CompactParquetFiles"/> over real parquet files with a token that a
/// test seam cancels at an exact point, so no test waits on a clock, and count rows through the globs the views use.
/// <para>In the reset-gate collection for the same reason as <see cref="ArchiveCompactionSwapTests"/>: each group's swap waits
/// for the process-wide write lock.</para>
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveCompactionShutdownTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];

    public ArchiveCompactionShutdownTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
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

    private string P(string fileName) => Path.Combine(_archiveDir, fileName).Replace("\\", "/");

    private (ArchiveService Service, CapturingLogger Log) NewService()
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _initializers.Add(initializer);
        var log = new CapturingLogger();
        return (new ArchiveService(initializer, _archiveDir, log) { WriteLockBudget = DuckDbInvestigationStoreTests.TestLockBudget }, log);
    }

    private static void Exec(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long Scalar(string sql)
    {
        using var connection = new DuckDBConnection("DataSource=:memory:");
        connection.Open();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private void MakeParquet(string fileName, long from, long to) =>
        Exec($"COPY (SELECT i AS id, md5(i::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)");

    /* Every row visible through the archive views' two globs for table, and how many of them are distinct ids. Each glob is
       used only while it matches a file, as the views do. A .tmp file matches neither glob, so a half-written merge is not
       counted here, and that is the point. */
    private (long Rows, long DistinctIds) Visible(string table)
    {
        var globs = new List<string>();
        foreach (var pattern in new[] { $"*_{table}.parquet", $"*_{table}_pt???.parquet" })
        {
            if (Directory.GetFiles(_archiveDir, pattern).Length > 0)
                globs.Add(P(pattern));
        }
        Assert.NotEmpty(globs);
        var source = "[" + string.Join(", ", globs.Select(g => $"'{g}'")) + "]";
        return (Scalar($"SELECT count(*) FROM read_parquet({source})"),
                Scalar($"SELECT count(DISTINCT id) FROM read_parquet({source})"));
    }

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    /* Two tables, each a month file and two per-cycle files: two compaction groups of 300 rows each. */
    private void MakeTwoTables()
    {
        foreach (var t in new[] { "ta", "tb" })
        {
            MakeParquet($"202609_{t}.parquet", 0, 100);
            MakeParquet($"20260928_1400_{t}.parquet", 100, 200);
            MakeParquet($"20260928_1500_{t}.parquet", 200, 300);
        }
    }

    /// <summary>
    /// The stop arrives while the first group's merges are done and its swap has not started. That group finishes its swap
    /// (its merged files are already written, and the swap is short), the second group is not started, the log says the
    /// run was cancelled, and every row of both tables is served exactly once. The next run, with no stop, merges the
    /// second group. Before this change compaction took no token, so the run was never cancelled and logged no outcome.
    /// </summary>
    [Fact]
    public void StopDuringTheFirstGroup_FinishesThatGroup_LeavesTheNext_AndTheNextRunCompletes()
    {
        MakeTwoTables();
        var (service, log) = NewService();
        using var stop = new CancellationTokenSource();
        service.OnCompactionTempsReadyForTests = _ => stop.Cancel();

        var finished = service.CompactParquetFiles(cancellationToken: stop.Token);

        Assert.False(finished, "a cancelled compaction reported that it finished");
        var cancelledLine = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction cancelled by shutdown", StringComparison.Ordinal));
        Assert.Contains("merged 1 groups, removed 2 files", cancelledLine.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Warning);
        Assert.DoesNotContain(log.Entries, e => e.Message.StartsWith("Parquet compaction complete", StringComparison.Ordinal));

        /* One table is merged to its month file, the other is exactly as it was, and no temp is left behind. */
        Assert.Equal(1, new[] { "ta", "tb" }.Count(t => !File.Exists(P($"20260928_1400_{t}.parquet"))));
        Assert.Equal(4, ArchiveFileNames().Count(f => f.EndsWith(".parquet", StringComparison.Ordinal)));
        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".tmp", StringComparison.Ordinal) || f.EndsWith(".swap", StringComparison.Ordinal) || f.EndsWith(".replaced", StringComparison.Ordinal));
        foreach (var t in new[] { "ta", "tb" })
        {
            var (rows, distinct) = Visible(t);
            Assert.Equal(300, rows);
            Assert.Equal(300, distinct);
        }

        /* The next run has no stop: it finishes what was left. */
        service.OnCompactionTempsReadyForTests = null;
        log.Entries.Clear();
        Assert.True(service.CompactParquetFiles());
        Assert.Equal(["202609_ta.parquet", "202609_tb.parquet"], ArchiveFileNames().Where(f => f.EndsWith(".parquet", StringComparison.Ordinal)));
        foreach (var t in new[] { "ta", "tb" })
        {
            var (rows, distinct) = Visible(t);
            Assert.Equal(300, rows);
            Assert.Equal(300, distinct);
        }

        var done = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction complete", StringComparison.Ordinal));
        Assert.Contains("merged 1 groups, removed 2 files", done.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The stop is already there when compaction starts: no group is started, no file is touched, and the run says it was
    /// cancelled with nothing merged.
    /// </summary>
    [Fact]
    public void StopBeforeAnyGroup_TouchesNothing_AndSaysSo()
    {
        MakeTwoTables();
        var before = ArchiveFileNames();
        var (service, log) = NewService();
        using var stop = new CancellationTokenSource();
        stop.Cancel();

        Assert.False(service.CompactParquetFiles(cancellationToken: stop.Token));

        Assert.Equal(before, ArchiveFileNames());
        var line = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction cancelled by shutdown", StringComparison.Ordinal));
        Assert.Contains("merged 0 groups, removed 0 files", line.Message, StringComparison.Ordinal);

        Assert.True(service.CompactParquetFiles());
        Assert.Equal(["202609_ta.parquet", "202609_tb.parquet"], ArchiveFileNames());
    }

    /// <summary>
    /// A group that splits into two batches is stopped after its first batch's merge. The first batch's temp file is
    /// removed, the group's inputs are untouched and every row is served once. A merged file is only ever visible after
    /// the whole group is merged, so a stop between batches cannot leave a month half merged. The next run merges the
    /// group.
    /// </summary>
    [Fact]
    public void StopBetweenTwoBatches_DropsTheTemp_AndLeavesTheGroupAsItWas()
    {
        MakeParquet("20260928_1400_ta.parquet", 0, 60_000);
        MakeParquet("20260928_1500_ta.parquet", 60_000, 120_000);
        var before = ArchiveFileNames();
        var (service, log) = NewService();

        /* A budget of one file: every per-cycle file is a batch of its own. */
        service.CompactionBatchInputBytes = new FileInfo(P("20260928_1400_ta.parquet").Replace("/", "\\")).Length;
        using var stop = new CancellationTokenSource();
        var tempSeenAfterFirstBatch = false;
        service.AfterCompactionBatchMergedForTests = i =>
        {
            if (i == 0)
            {
                tempSeenAfterFirstBatch = Directory.GetFiles(_archiveDir, "*.tmp").Length == 1;
                stop.Cancel();
            }
        };

        Assert.False(service.CompactParquetFiles(cancellationToken: stop.Token));

        Assert.True(tempSeenAfterFirstBatch, "the first batch's merge left no temp file, so the test did not stop between two batches");
        Assert.Equal(before, ArchiveFileNames());
        var (rows, distinct) = Visible("ta");
        Assert.Equal(120_000, rows);
        Assert.Equal(120_000, distinct);
        var line = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction cancelled by shutdown", StringComparison.Ordinal));
        Assert.Contains("merged 0 groups, removed 0 files", line.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Warning);

        service.AfterCompactionBatchMergedForTests = null;
        Assert.True(service.CompactParquetFiles());
        (rows, distinct) = Visible("ta");
        Assert.Equal(120_000, rows);
        Assert.Equal(120_000, distinct);
        Assert.DoesNotContain(ArchiveFileNames(), f => f.EndsWith(".tmp", StringComparison.Ordinal));
        Assert.False(File.Exists(P("20260928_1400_ta.parquet")), "the per-cycle input survived the merge that followed the stop");
    }

    /// <summary>
    /// What a process that is killed inside a merge leaves: a partial <c>.tmp</c> file at the name the next merge writes
    /// to, and the group's inputs untouched. A read does not serve the partial file (the views' globs do not match
    /// <c>.parquet.tmp</c>), and the next compaction writes over it and completes, every row once. This is the case a
    /// <c>--stop</c> hits when a merge is still running at the end of the 5 seconds the window waits for it.
    /// </summary>
    [Fact]
    public void MergeKilledMidWrite_LeavesNothingAReadServes_AndTheNextCompactionOverwritesIt()
    {
        MakeParquet("202609_ta.parquet", 0, 100);
        MakeParquet("20260928_1400_ta.parquet", 100, 200);

        /* The leftover holds rows that overlap the group's inputs, so serving it would show as duplicates. */
        MakeParquet("202609_ta.parquet.tmp", 0, 150);
        File.WriteAllBytes(P("202609_ta.parquet.tmp"), File.ReadAllBytes(P("202609_ta.parquet.tmp"))[..120]);

        var (rows, distinct) = Visible("ta");
        Assert.Equal(200, rows);
        Assert.Equal(200, distinct);

        var (service, log) = NewService();
        Assert.True(service.CompactParquetFiles());

        Assert.Equal(["202609_ta.parquet"], ArchiveFileNames());
        (rows, distinct) = Visible("ta");
        Assert.Equal(200, rows);
        Assert.Equal(200, distinct);
        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Warning);
    }

    /// <summary>
    /// The reset path says how compaction ended even when there was nothing to merge, so the "Compacting parquet files"
    /// line it logs first is always followed by an outcome. The hourly pass does not log an idle run.
    /// </summary>
    [Fact]
    public void ARunWithNothingToMerge_IsLoggedOnlyWhenTheCallerAsks()
    {
        MakeParquet("202609_ta.parquet", 0, 100);
        var (service, log) = NewService();

        Assert.True(service.CompactParquetFiles());
        Assert.DoesNotContain(log.Entries, e => e.Level >= LogLevel.Information);

        Assert.True(service.CompactParquetFiles(reportIdle: true));
        var line = Assert.Single(log.Entries, e => e.Level >= LogLevel.Information);
        Assert.StartsWith("Parquet compaction complete: nothing to merge", line.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A group whose merge throws is logged as it was, and the outcome line then says how many groups failed instead of
    /// reading as a clean finish. The failed group's inputs stay and the other group still merges.
    /// </summary>
    [Fact]
    public void AGroupThatFails_IsCountedInTheOutcomeLine()
    {
        MakeTwoTables();
        /* A per-cycle file that is not parquet makes that group's merge throw. */
        File.WriteAllText(P("20260928_1600_ta.parquet"), "not a parquet file");
        var (service, log) = NewService();

        Assert.True(service.CompactParquetFiles());

        Assert.Single(log.Entries, e => e.Level == LogLevel.Error && e.Message.StartsWith("Failed to compact", StringComparison.Ordinal));
        var line = Assert.Single(log.Entries, e => e.Message.StartsWith("Parquet compaction failed for 1 group(s)", StringComparison.Ordinal));
        Assert.Contains("merged 1 groups, removed 2 files", line.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(log.Entries, e => e.Message.StartsWith("Parquet compaction complete", StringComparison.Ordinal));
        Assert.True(File.Exists(P("20260928_1600_ta.parquet")));
        Assert.Equal(300, Visible("tb").Rows);
    }

    private sealed class CapturingLogger : ILogger<ArchiveService>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }
}
