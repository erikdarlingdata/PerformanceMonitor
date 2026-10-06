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
using System.Security.Cryptography;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5377. A reporter's Lite had 518 archive files, 6.92 GiB, on a volume with about 2.2 GiB free. Compaction
/// rewrote a whole month of a wide table to fold in a few MB of new rows, needed room for all of it at once,
/// and when the volume could not hold it the merge failed part way with an I/O error that named no numbers.
/// These tests pin the smaller step (merge only what changes, only what fits), the one warning that names the
/// table, the bytes it needed and the bytes free, and the warning for the data folder's volume at startup and
/// once per archive pass. A seam on the initializer stands in for the disk's free space.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class ArchiveCompactionLowDiskTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _archiveDir;
    private readonly List<DuckDbInitializer> _initializers = [];
    private readonly List<(LogLevel Level, string Message)> _log = [];
    private readonly List<(LogLevel Level, string Message)> _initializerLog = [];

    public ArchiveCompactionLowDiskTests()
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

    private (ArchiveService Service, DuckDbInitializer Initializer) NewService(long? freeBytes = null)
    {
        var initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"), new CapturingLogger<DuckDbInitializer>(_initializerLog));
        initializer.AvailableFreeBytesProvider = _ => freeBytes;
        _initializers.Add(initializer);
        return (new ArchiveService(initializer, _archiveDir, new CapturingLogger<ArchiveService>(_log)), initializer);
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
        return Convert.ToInt64(cmd.ExecuteScalar(), System.Globalization.CultureInfo.InvariantCulture);
    }

    /* Rows [from, to) with a payload wide enough that the file sizes follow the row counts. */
    private void MakeParquet(string fileName, long from, long to) =>
        Exec($"COPY (SELECT i AS id, md5(i::VARCHAR) AS payload FROM range({from}, {to}) t(i)) TO '{P(fileName)}' (FORMAT PARQUET, ROW_GROUP_SIZE 10000)");

    private long SizeOf(string fileName) => new FileInfo(P(fileName).Replace("/", "\\")).Length;

    /* Every row visible through the archive views' two globs for table t. */
    private (long Rows, long DistinctIds) Visible()
    {
        var globs = new List<string>();
        foreach (var pattern in new[] { "*_t.parquet", "*_t_pt???.parquet" })
        {
            if (Directory.GetFiles(_archiveDir, pattern).Length > 0)
                globs.Add(P(pattern));
        }
        var source = "[" + string.Join(", ", globs.Select(g => $"'{g}'")) + "]";
        return (Scalar($"SELECT count(*) FROM read_parquet({source})"),
                Scalar($"SELECT count(DISTINCT id) FROM read_parquet({source})"));
    }

    private string[] ArchiveFileNames() =>
        Directory.GetFiles(_archiveDir).Select(Path.GetFileName).Order(StringComparer.Ordinal).ToArray()!;

    private string Hash(string fileName) =>
        Convert.ToHexString(SHA256.HashData(File.ReadAllBytes(P(fileName).Replace("/", "\\"))));

    /* The archive service's warnings (a compaction that could not merge everything). */
    private List<string> Warnings() => _log.Where(e => e.Level == LogLevel.Warning).Select(e => e.Message).ToList();

    /* The initializer's warnings: the data folder's volume, at startup and once per archive pass. */
    private List<string> VolumeWarnings() =>
        _initializerLog.Where(e => e.Level == LogLevel.Warning && e.Message.StartsWith("Low disk space", StringComparison.Ordinal)).Select(e => e.Message).ToList();

    [Fact]
    public void AMonthFileThatIsAloneInItsBatch_IsLeftAsItIs_WhileTheNewFileMerges()
    {
        MakeParquet("202609_t_pt001.parquet", 0, 80_000);
        MakeParquet("20260928_1400_t.parquet", 1_000_000, 1_000_100);
        var untouched = Hash("202609_t_pt001.parquet");

        var (service, _) = NewService();
        /* The budget is the existing part's size: nothing fits beside it, so it is a batch of one. It used to be
           read and written back in full on every pass. */
        service.CompactionBatchInputBytes = SizeOf("202609_t_pt001.parquet");
        List<string> temps = [];
        service.OnCompactionTempsReadyForTests = t => temps = t.ToList();

        service.CompactParquetFiles();

        Assert.Equal([P("202609_t_pt002.parquet.tmp")], temps);
        Assert.Equal(["202609_t_pt001.parquet", "202609_t_pt002.parquet"], ArchiveFileNames());
        Assert.Equal(untouched, Hash("202609_t_pt001.parquet"));
        Assert.Equal((80_100L, 80_100L), Visible());

        /* The next pass folds its new file into pt002 and still leaves pt001 alone. */
        MakeParquet("20260928_1500_t.parquet", 2_000_000, 2_000_100);
        temps = [];
        service.CompactParquetFiles();

        Assert.Equal([P("202609_t_pt002.parquet.tmp")], temps);
        Assert.Equal(["202609_t_pt001.parquet", "202609_t_pt002.parquet"], ArchiveFileNames());
        Assert.Equal(untouched, Hash("202609_t_pt001.parquet"));
        Assert.Equal((80_200L, 80_200L), Visible());
        Assert.Equal(200, Scalar($"SELECT count(*) FROM read_parquet('{P("202609_t_pt002.parquet")}')"));
    }

    [Fact]
    public void WithoutEnoughFreeSpace_NothingMerges_AndOneWarningNamesTheTableTheBytesNeededAndTheBytesFree()
    {
        MakeParquet("20260928_1400_t.parquet", 0, 50_000);
        MakeParquet("20260928_1500_t.parquet", 50_000, 100_000);
        var inputBytes = SizeOf("20260928_1400_t.parquet") + SizeOf("20260928_1500_t.parquet");
        const long free = 1_000_000;

        var (service, _) = NewService(freeBytes: free);
        var tempsRan = false;
        service.OnCompactionTempsReadyForTests = _ => tempsRan = true;

        service.CompactParquetFiles();

        Assert.False(tempsRan, "a merge ran although the volume could not hold its output");
        Assert.Equal(["20260928_1400_t.parquet", "20260928_1500_t.parquet"], ArchiveFileNames());
        Assert.Equal((100_000L, 100_000L), Visible());

        var warning = Assert.Single(Warnings());
        Assert.Contains("t (202609)", warning, StringComparison.Ordinal);
        Assert.Contains((inputBytes + DataVolumeSpace.CompactionHeadroomBytes).ToString("N0", System.Globalization.CultureInfo.InvariantCulture) + " bytes", warning, StringComparison.Ordinal);
        Assert.Contains(free.ToString("N0", System.Globalization.CultureInfo.InvariantCulture) + " are free", warning, StringComparison.Ordinal);
        Assert.Contains("nothing merged", warning, StringComparison.Ordinal);
        Assert.Contains(_archiveDir, warning, StringComparison.Ordinal);
        Assert.Equal(inputBytes, service.LastCompactionLargestNeedBytes);

        /* One warning per pass: the next pass says it again, once. */
        service.CompactParquetFiles();
        Assert.Equal(2, Warnings().Count);
    }

    [Fact]
    public void WhenOnlyOneBatchFits_ItMerges_TheRestStaysForALaterPass_AndTheWarningSaysSo()
    {
        MakeParquet("20260928_1400_t.parquet", 0, 50_000);
        MakeParquet("20260928_1500_t.parquet", 50_000, 100_000);

        var (service, initializer) = NewService();
        /* A budget of one file: each per-cycle file is a batch of its own, two merge batches. The free space fits
           the first with the headroom and not the second. */
        service.CompactionBatchInputBytes = 1;
        var fitsOne = Math.Max(SizeOf("20260928_1400_t.parquet"), SizeOf("20260928_1500_t.parquet")) + DataVolumeSpace.CompactionHeadroomBytes + 1;
        initializer.AvailableFreeBytesProvider = _ => fitsOne;
        List<string> temps = [];
        service.OnCompactionTempsReadyForTests = t => temps = t.ToList();

        service.CompactParquetFiles();

        Assert.Single(temps);
        Assert.Equal((100_000L, 100_000L), Visible());
        var left = ArchiveFileNames();
        Assert.Equal(2, left.Length);
        Assert.Contains("202609_t_pt001.parquet", left);
        Assert.Single(left, f => f.StartsWith("20260928_", StringComparison.Ordinal));
        var warning = Assert.Single(Warnings());
        Assert.Contains("merged anyway", warning, StringComparison.Ordinal);

        /* With room again the rest merges, and nothing warns. */
        _log.Clear();
        initializer.AvailableFreeBytesProvider = _ => long.MaxValue;
        service.CompactionBatchInputBytes = ParquetCompaction.DefaultBatchInputBytes;
        service.CompactParquetFiles();

        Assert.Equal(["202609_t.parquet"], ArchiveFileNames());
        Assert.Equal((100_000L, 100_000L), Visible());
        Assert.Empty(Warnings());
    }

    [Fact]
    public void WhenTheFreeSpaceIsUnknown_CompactionMergesAsBefore_AndDoesNotWarn()
    {
        MakeParquet("20260928_1400_t.parquet", 0, 1_000);
        MakeParquet("20260928_1500_t.parquet", 1_000, 2_000);

        var (service, _) = NewService(freeBytes: null);
        service.CompactParquetFiles();

        Assert.Equal(["202609_t.parquet"], ArchiveFileNames());
        Assert.Equal((2_000L, 2_000L), Visible());
        Assert.Empty(Warnings());
    }

    [Fact]
    public void WarnIfLow_NamesTheFolderTheBytesFreeAndTheBytesNeeded_OnlyWhenThereIsLess()
    {
        var logger = new CapturingLogger<ArchiveService>(_log);

        Assert.True(DataVolumeSpace.WarnIfLow(logger, @"C:\Lite\data", 600L * 1024 * 1024, "a CHECKPOINT", _ => 100L * 1024 * 1024));
        var warning = Assert.Single(Warnings());
        Assert.Contains(@"C:\Lite\data", warning, StringComparison.Ordinal);
        Assert.Contains((100L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture), warning, StringComparison.Ordinal);
        Assert.Contains((600L * 1024 * 1024).ToString(System.Globalization.CultureInfo.InvariantCulture), warning, StringComparison.Ordinal);
        Assert.Contains("100.0 MiB", warning, StringComparison.Ordinal);
        Assert.Contains("600.0 MiB", warning, StringComparison.Ordinal);
        Assert.Contains("a CHECKPOINT", warning, StringComparison.Ordinal);

        _log.Clear();
        Assert.False(DataVolumeSpace.WarnIfLow(logger, "x", 600, "r", _ => 600));
        Assert.False(DataVolumeSpace.WarnIfLow(logger, "x", 600, "r", _ => 601));
        Assert.False(DataVolumeSpace.WarnIfLow(logger, "x", 600, "r", _ => null));
        Assert.False(DataVolumeSpace.WarnIfLow(logger, "x", 0, "r", _ => 0));
        Assert.Empty(Warnings());
    }

    [Fact]
    public void GetAvailableFreeBytes_ReadsTheRealVolume_AndAnswersNullWhenItCannot()
    {
        var free = DataVolumeSpace.GetAvailableFreeBytes(_tempDir);
        Assert.NotNull(free);
        Assert.True(free > 0);

        /* A share has no drive letter: unknown, so nothing warns on a guess. */
        Assert.Null(DataVolumeSpace.GetAvailableFreeBytes(@"\\no-such-host-5377\share\lite"));
    }

    [Fact]
    public async Task TheDataVolumeWarning_FiresAtStartup_AndOncePerArchivePass_NamingTheFolder()
    {
        var (service, initializer) = NewService(freeBytes: 1);

        await initializer.InitializeAsync();

        var startup = Assert.Single(VolumeWarnings());
        Assert.Contains(_tempDir, startup, StringComparison.Ordinal);
        Assert.Contains("1 bytes free", startup, StringComparison.Ordinal);
        Assert.Contains("CHECKPOINT", startup, StringComparison.Ordinal);

        await service.ArchiveOldDataAsync(hotDataDays: 7);
        Assert.Equal(2, VolumeWarnings().Count);

        await service.ArchiveOldDataAsync(hotDataDays: 7);
        Assert.Equal(3, VolumeWarnings().Count);

        /* Enough room: silent. */
        _initializerLog.Clear();
        initializer.AvailableFreeBytesProvider = _ => long.MaxValue;
        await service.ArchiveOldDataAsync(hotDataDays: 7);
        Assert.Empty(VolumeWarnings());
    }

    /// <summary>
    /// DuckDB caches a parquet file's state against its PATH at INSTANCE scope, and a test with separate
    /// in-memory instances cannot see it. Compaction writes the merged month over the existing
    /// <c>YYYYMM_table.parquet</c>, the very path an earlier read went through, so this reads it on the store's
    /// own instance (the initializer's connection string, sentinel open), lets compaction replace it, and reads
    /// it again on a fresh connection of the same instance. It runs with <c>parquet_metadata_cache</c> off, as
    /// Lite ships it, and on, the setting evaluated and declined in #5377: both must read the new bytes.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ACompactedMonthFile_ReadsItsNewBytes_OnTheSameInstanceThatReadItBefore(bool metadataCache)
    {
        MakeParquet("202609_t.parquet", 0, 100_000);
        var (service, initializer) = NewService();
        await initializer.InitializeAsync();

        if (metadataCache)
        {
            await using var set = initializer.CreateConnection();
            await set.OpenAsync(TestContext.Current.CancellationToken);
            await using var setCmd = set.CreateCommand();
            /* GLOBAL: a plain SET of this setting is per connection, and the point is the instance's cache. */
            setCmd.CommandText = "SET GLOBAL parquet_metadata_cache=true";
            await setCmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        Assert.Equal(metadataCache, await SettingAsync(initializer, "parquet_metadata_cache"));
        Assert.Equal(100_000L, await CountAsync(initializer));

        /* Fold new rows in: the merged month replaces the file at the same name, and is a different size. */
        MakeParquet("20260928_1400_t.parquet", 100_000, 160_000);
        service.CompactParquetFiles();
        Assert.Equal(["202609_t.parquet"], ArchiveFileNames());

        Assert.Equal(160_000L, await CountAsync(initializer));

        /* And shrinking it, as the Query Store repair does. */
        Exec($"COPY (SELECT i AS id, md5(i::VARCHAR) AS payload FROM range(0, 1000) t(i)) TO '{P("202609_t.parquet")}.tmp' (FORMAT PARQUET)");
        File.Delete(P("202609_t.parquet").Replace("/", "\\"));
        File.Move(P("202609_t.parquet.tmp").Replace("/", "\\"), P("202609_t.parquet").Replace("/", "\\"));
        Assert.Equal(1_000L, await CountAsync(initializer));
    }

    /// <summary>
    /// The decision #5377 made on measured numbers, pinned so a change to it is deliberate: the metadata cache is
    /// left at DuckDB's default, off. See the comment on <see cref="DuckDbInitializer.ConnectionString"/>.
    /// </summary>
    [Fact]
    public async Task ParquetMetadataCache_IsOff_OnTheStoresConnections()
    {
        var (_, initializer) = NewService();
        await initializer.InitializeAsync();

        Assert.False(await SettingAsync(initializer, "parquet_metadata_cache"));
        Assert.DoesNotContain("parquet_metadata_cache", initializer.ConnectionString, StringComparison.OrdinalIgnoreCase);
    }

    private async Task<long> CountAsync(DuckDbInitializer initializer)
    {
        await using var connection = initializer.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT count(*) FROM read_parquet(['{P("*_t.parquet")}'])";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task<bool> SettingAsync(DuckDbInitializer initializer, string name)
    {
        await using var connection = initializer.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = $"SELECT current_setting('{name}')";
        return Convert.ToBoolean(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    private sealed class CapturingLogger<T>(List<(LogLevel Level, string Message)> sink) : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => sink.Add((logLevel, formatter(state, exception)));
    }
}
