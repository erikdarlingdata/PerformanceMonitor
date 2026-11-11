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
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8535: the memory line Lite writes on a server tab's open and close and on every trim tick, and the trim target it was
/// found with. A field walk showed private bytes growing by hundreds of MB with the trim never logging, because the trim logs
/// at Debug and, once the store's ART indexes held more than the trim's 64 MB target, DuckDB refused to lower memory_limit
/// ("could not free up enough memory for the new limit") on every tick, a failure that also only reached Debug.
/// </summary>
[Collection("CollectionResetGate")]
public sealed class DuckDbMemoryDiagnosticsTests : IDisposable
{
    private static readonly TimeSpan LockHangGuard = TimeSpan.FromSeconds(30);

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly List<DuckDbInitializer> _initializers = [];

    public DuckDbMemoryDiagnosticsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        foreach (var initializer in _initializers)
            initializer.Dispose();

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

    private async Task<(DuckDbInitializer Initializer, CapturingLogger Log)> OpenStoreAsync()
    {
        var log = new CapturingLogger();
        var initializer = new DuckDbInitializer(_dbPath, log) { TrimLockWaitForTests = LockHangGuard };
        _initializers.Add(initializer);
        await initializer.InitializeFromTemplateAsync();
        return (initializer, log);
    }

    private static DuckDbInitializer.MemoryTag Tag(string tag, long memoryMb, long temporaryMb = 0) =>
        new(tag, memoryMb * 1024 * 1024, temporaryMb * 1024 * 1024);

    [Fact]
    public void TrimTargetFor_WithNoTags_IsTheFloor() =>
        Assert.Equal(DuckDbInitializer.TrimTargetMemoryLimit, DuckDbInitializer.TrimTargetFor(null));

    [Fact]
    public void TrimTargetFor_AddsWhatTheBufferManagerCannotGiveUp_ButNotTheCaches()
    {
        var target = DuckDbInitializer.TrimTargetFor(
        [
            Tag("BASE_TABLE", 900), Tag("EXTERNAL_FILE_CACHE", 300), Tag("ART_INDEX", 71), Tag("OBJECT_CACHE", 9),
        ]);

        /* 64 MB floor plus 80 MB held by the indexes and the object cache. The 1,200 MB of caches do not raise it. */
        Assert.Equal("144MB", target);
    }

    [Fact]
    public void DescribeMemory_NamesEachTagInUse_TheTemporaryStorage_ThePrivateBytesAndTheGcHeap()
    {
        var text = DuckDbInitializer.DescribeMemory(
            [Tag("BASE_TABLE", 100), Tag("ART_INDEX", 50, temporaryMb: 8), Tag("OBJECT_CACHE", 0)],
            privateBytes: 700L * 1024 * 1024, gcHeapBytes: 40L * 1024 * 1024);

        Assert.Equal(
            "DuckDB holds 150.0 MB (BASE_TABLE 100.0 MB, ART_INDEX 50.0 MB), temporary storage 8.0 MB. " +
            "Process private bytes 700.0 MB, gc heap 40.0 MB.",
            text);
    }

    [Fact]
    public void ServerTabEventName_NamesTheServerAndTheEvent()
    {
        Assert.Equal("Server tab opened (Alpha)", DuckDbInitializer.ServerTabEventName("Alpha", opened: true));
        Assert.Equal("Server tab closed (Alpha)", DuckDbInitializer.ServerTabEventName("Alpha", opened: false));
    }

    [Fact]
    public async Task LogMemory_OnAnOpenStore_WritesOneInformationLineFromLitesOwnInstance()
    {
        var (initializer, log) = await OpenStoreAsync();
        var before = log.Entries.Count;

        var line = initializer.LogMemory(DuckDbInitializer.ServerTabEventName("Alpha", opened: true));

        Assert.NotNull(line);
        var added = log.Entries.Skip(before).ToList();
        var entry = Assert.Single(added);
        Assert.Equal(LogLevel.Information, entry.Level);
        Assert.StartsWith("Server tab opened (Alpha): DuckDB holds ", entry.Message, StringComparison.Ordinal);
        Assert.Contains("Process private bytes ", entry.Message, StringComparison.Ordinal);
        Assert.Contains("gc heap ", entry.Message, StringComparison.Ordinal);

        /* The reading is Lite's instance: a table written through one connection is held in the tags a second one reads. */
        using var connection = initializer.CreateConnection();
        connection.Open();
        var tags = DuckDbInitializer.ReadMemoryTags(connection);
        Assert.NotEmpty(tags);
    }

    [Fact]
    public void LogMemory_BeforeTheStoreIsOpen_WritesNothing()
    {
        var log = new CapturingLogger();
        using var initializer = new DuckDbInitializer(_dbPath, log);

        Assert.Null(initializer.LogMemory("Server tab opened (Alpha)"));
        Assert.Empty(log.Entries);
    }

    [Fact]
    public async Task RunMemoryTrimCycle_UnderTheThreshold_WritesNothingAndDoesNotTrim()
    {
        var (initializer, log) = await OpenStoreAsync();
        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = long.MaxValue;
        try
        {
            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            Assert.DoesNotContain(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
            Assert.Equal(0, initializer.CompletedTrimCycleCount);
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
    }

    [Fact]
    public async Task RunMemoryTrimCycle_WhenTheLocalDatabaseIsBusy_WritesOneTickLineAndSkips()
    {
        var (initializer, log) = await OpenStoreAsync();
        initializer.TrimLockWaitForTests = TimeSpan.Zero;
        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 0;
        var readerReady = new ManualResetEventSlim();
        var releaseReader = new ManualResetEventSlim();
        try
        {
            var readerTask = Task.Run(() =>
            {
                using var readLock = initializer.AcquireReadLock();
                readerReady.Set();
                releaseReader.Wait(LockHangGuard);
            });
            Assert.True(readerReady.Wait(LockHangGuard));

            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            var tick = Assert.Single(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Information, tick.Level);
            Assert.Contains("the local database was busy, so the trim was skipped.", tick.Message, StringComparison.Ordinal);
            Assert.Equal(0, initializer.CompletedTrimCycleCount);

            releaseReader.Set();
            await readerTask;
        }
        finally
        {
            releaseReader.Set();
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
    }

    /// <summary>
    /// A lowering DuckDB refuses writes one tick line with the reason and leaves the configured limit in place. The real target is
    /// built to be accepted, so the test puts the old fixed 64 MB target back through the seam, over a store whose indexes hold more.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_WhenDuckDbRefusesTheLowering_WritesOneTickLineAndRestoresTheLimit()
    {
        var (initializer, log) = await OpenStoreAsync();
        await FillIndexedTableAsync(initializer);
        var limitBefore = await ReadMemoryLimitAsync(initializer);

        initializer.TrimTargetOverrideForTests = "64MB";
        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 0;
        try
        {
            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            var tick = Assert.Single(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Information, tick.Level);
            Assert.Contains("DuckDB would not lower memory_limit to 64MB", tick.Message, StringComparison.Ordinal);
            Assert.Equal(0, initializer.CompletedTrimCycleCount);
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }

        Assert.Equal(limitBefore, await ReadMemoryLimitAsync(initializer));
    }

    /// <summary>
    /// The threshold counts only what a trim can free. A store whose pinned tags (ART indexes) alone pass the threshold, with a
    /// small cache, used to trip the trim every minute and empty the caches for nothing. The threshold here sits between the
    /// store's reclaimable bytes and its total, which is where a store with 7 million rows sits against the real 256 MB: the old
    /// comparison trimmed, the new one does not and writes no line.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_WhenOnlyPinnedTagsPassTheThreshold_DoesNotTrim()
    {
        var (initializer, log) = await OpenStoreAsync();
        await FillIndexedTableAsync(initializer);

        IReadOnlyList<DuckDbInitializer.MemoryTag> tags;
        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            tags = DuckDbInitializer.ReadMemoryTags(connection);
        }

        var total = tags.Sum(t => t.MemoryBytes);
        var pinned = tags.Where(t => t.Tag is not ("BASE_TABLE" or "EXTERNAL_FILE_CACHE")).Sum(t => t.MemoryBytes);
        Assert.True(pinned > 64L * 1024 * 1024, $"pinned {pinned / (1024 * 1024)} MB, not over the 64 MB floor");

        /* Half the pinned memory of margin each way, so the store's drift between this read and the cycle cannot flip it. */
        var threshold = total - pinned / 2;
        Assert.True(total - pinned < threshold, "the reclaimable bytes must be under the threshold for this test to mean anything");

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = threshold;
        try
        {
            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            Assert.Equal(0, initializer.CompletedTrimCycleCount);
            Assert.DoesNotContain(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
    }

    [Fact]
    public void ReclaimableBytes_AgainstTheRealThreshold_IgnoresPinnedTagsAndCountsTheCaches()
    {
        var threshold = DuckDbInitializer.TrimThresholdBytes;

        /* 7 million rows of ART_INDEX is about 250 MB: pinned alone passes 256 MB, and there is nothing to trim. */
        var pinnedOnly = new[] { Tag("BASE_TABLE", 20), Tag("EXTERNAL_FILE_CACHE", 5), Tag("ART_INDEX", 300), Tag("OBJECT_CACHE", 9) };
        var pinnedOnlyTotal = pinnedOnly.Sum(t => t.MemoryBytes);
        Assert.True(pinnedOnlyTotal >= threshold);
        Assert.True(DuckDbInitializer.ReclaimableBytes(pinnedOnlyTotal, pinnedOnly) < threshold);

        /* A cache over 256 MB still trims, with or without indexes beside it. */
        var bigCache = new[] { Tag("BASE_TABLE", 280), Tag("EXTERNAL_FILE_CACHE", 40), Tag("ART_INDEX", 71) };
        Assert.True(DuckDbInitializer.ReclaimableBytes(bigCache.Sum(t => t.MemoryBytes), bigCache) >= threshold);

        /* A store with nothing pinned, or with no tags read, compares its total as before. */
        var cachesOnly = new[] { Tag("BASE_TABLE", 300) };
        Assert.Equal(300d * 1024 * 1024, DuckDbInitializer.ReclaimableBytes(cachesOnly.Sum(t => t.MemoryBytes), cachesOnly));
        Assert.Equal(300d * 1024 * 1024, DuckDbInitializer.ReclaimableBytes(300d * 1024 * 1024, null));
    }

    /// <summary>A store whose cache holds more than the threshold still trims, even with indexes pinned beside it.</summary>
    [Fact]
    public async Task RunMemoryTrimCycle_WhenTheCacheIsOverTheThreshold_StillTrims()
    {
        var (initializer, log) = await OpenStoreAsync();
        await FillIndexedTableAsync(initializer);

        IReadOnlyList<DuckDbInitializer.MemoryTag> tags;
        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            tags = DuckDbInitializer.ReadMemoryTags(connection);
        }

        var reclaimable = DuckDbInitializer.ReclaimableBytes(tags.Sum(t => t.MemoryBytes), tags);
        Assert.True(reclaimable > 1, "the store holds no cache to trim");

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = (long)(reclaimable / 2);
        try
        {
            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            Assert.Equal(1, initializer.CompletedTrimCycleCount);
            var tick = Assert.Single(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
            Assert.Contains("Trimmed from ", tick.Message, StringComparison.Ordinal);
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
    }

    private async Task FillIndexedTableAsync(DuckDbInitializer initializer)
    {
        using var connection = initializer.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var create = connection.CreateCommand();
        create.CommandText = "CREATE TABLE pinned_8535 (id BIGINT PRIMARY KEY, v INTEGER)";
        await create.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        using var fill = connection.CreateCommand();
        fill.CommandText = "INSERT INTO pinned_8535 SELECT (hash(i) >> 1)::BIGINT, i::INTEGER FROM range(0, 3000000) t(i)";
        fill.CommandTimeout = 300;
        await fill.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task<string> ReadMemoryLimitAsync(DuckDbInitializer initializer)
    {
        using var connection = initializer.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        return await ScalarTextAsync(connection, "SELECT current_setting('memory_limit')");
    }

    /// <summary>
    /// The defect. A table with a primary key and enough rows puts more than the trim's 64 MB target into ART_INDEX, which
    /// DuckDB cannot evict. The old target made <c>SET memory_limit='64MB'</c> throw "could not free up enough memory for the
    /// new limit", so the cycle never completed (and the timer's catch only wrote a Debug line). The target now sits above the
    /// pinned memory, the cycle completes, the configured limit is back after it, and the tick line says what it did.
    /// </summary>
    [Fact]
    public async Task RunMemoryTrimCycle_WhenTheIndexesHoldMoreThanTheFloor_StillTrimsAndRestoresTheLimit()
    {
        var (initializer, log) = await OpenStoreAsync();

        await FillIndexedTableAsync(initializer);

        string limitBefore;
        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            var art = DuckDbInitializer.ReadMemoryTags(connection).Where(t => t.Tag == "ART_INDEX").Sum(t => t.MemoryBytes);

            /* The precondition: without more than the floor pinned, this test would pass on the old target too. */
            Assert.True(art > 64L * 1024 * 1024, $"ART_INDEX holds {art / (1024 * 1024)} MB, not over the 64 MB floor");

            limitBefore = await ScalarTextAsync(connection, "SELECT current_setting('memory_limit')");
        }

        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = 1;
        try
        {
            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            Assert.Equal(1, initializer.CompletedTrimCycleCount);
            var tick = Assert.Single(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Information, tick.Level);
            Assert.Contains("Trimmed from ", tick.Message, StringComparison.Ordinal);
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }

        using var after = initializer.CreateConnection();
        await after.OpenAsync(TestContext.Current.CancellationToken);
        Assert.Equal(limitBefore, await ScalarTextAsync(after, "SELECT current_setting('memory_limit')"));
    }

    private static async Task<string> ScalarTextAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToString(await cmd.ExecuteScalarAsync(TestContext.Current.CancellationToken)) ?? "";
    }

    private sealed class CapturingLogger : ILogger<DuckDbInitializer>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Entries)
                Entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
