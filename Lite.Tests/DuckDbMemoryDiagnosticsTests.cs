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
    public async Task RunMemoryTrimCycle_UnderTheThreshold_WritesOneTickLineAndDoesNotTrim()
    {
        var (initializer, log) = await OpenStoreAsync();
        var originalThreshold = DuckDbInitializer.TrimThresholdBytes;
        DuckDbInitializer.TrimThresholdBytes = long.MaxValue;
        try
        {
            var before = log.Entries.Count;
            initializer.RunMemoryTrimCycle();

            var tick = Assert.Single(log.Entries.Skip(before), e => e.Message.StartsWith("Memory trim tick:", StringComparison.Ordinal));
            Assert.Equal(LogLevel.Information, tick.Level);
            Assert.Contains("DuckDB holds ", tick.Message, StringComparison.Ordinal);
            Assert.Contains("Process private bytes ", tick.Message, StringComparison.Ordinal);
            Assert.Contains("so no trim.", tick.Message, StringComparison.Ordinal);
            Assert.Equal(0, initializer.CompletedTrimCycleCount);
        }
        finally
        {
            DuckDbInitializer.TrimThresholdBytes = originalThreshold;
        }
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

        using (var connection = initializer.CreateConnection())
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var create = connection.CreateCommand();
            create.CommandText = "CREATE TABLE pinned_8535 (id BIGINT PRIMARY KEY, v INTEGER)";
            await create.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
            using var fill = connection.CreateCommand();
            fill.CommandText = "INSERT INTO pinned_8535 SELECT (hash(i) >> 1)::BIGINT, i::INTEGER FROM range(0, 3000000) t(i)";
            fill.CommandTimeout = 300;
            await fill.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

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
