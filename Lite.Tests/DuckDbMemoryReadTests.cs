/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9423: <c>DuckDbInitializer.TryReadDuckDbMemoryBytes</c>, the read the slow-call record takes after an MCP call. Its contract
/// is DuckDB's memory total now, or null when the store lock is busy, without waiting. The call timing tests hand the record a
/// reading of their own, so this is the only test that reaches the real read.
/// </summary>
/* The store lock is one static field shared by every DuckDbInitializer in the process, so a parallel class that holds it makes the
   free-lock test answer null. The serialized collection CollectionResetGateTests defines runs alone, with no other class in flight. */
[Collection("CollectionResetGate")]
public sealed class DuckDbMemoryReadTests : IDisposable
{
    /* A hang backstop for the busy-lock read, not a timing budget: a read that waited for the lock would not finish until the
       writer let go, which this test does only after the read has finished. */
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _initializer;

    public DuckDbMemoryReadTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _initializer = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
    }

    public void Dispose()
    {
        _initializer.Dispose();

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

    [Fact]
    public async Task OnAnOpenStoreWithTheLockFree_ReturnsDuckDbsMemoryTotal()
    {
        await _initializer.InitializeFromTemplateAsync();

        var bytes = _initializer.TryReadDuckDbMemoryBytes();

        Assert.NotNull(bytes);
        Assert.True(bytes > 0, $"DuckDB's memory total on an open store was {bytes}");
    }

    [Fact]
    public async Task WhileAnotherThreadHoldsTheStoreLock_ReturnsNullWithoutWaiting()
    {
        await _initializer.InitializeFromTemplateAsync();

        using var writerHasIt = new ManualResetEventSlim(false);
        using var releaseWriter = new ManualResetEventSlim(false);

        /* A dedicated thread: a ReaderWriterLockSlim write hold is released on the thread that took it. */
        var writer = new Thread(() =>
        {
            using var write = _initializer.AcquireWriteLock(HangGuard);
            writerHasIt.Set();
            releaseWriter.Wait(HangGuard);
        })
        { IsBackground = true };
        writer.Start();

        try
        {
            Assert.True(writerHasIt.Wait(HangGuard), "the writer never took the store lock");

            /* The writer stays inside its hold until releaseWriter is set below, so a read that finishes at all has not waited
               for it. A read that waited would sit here until the hang guard ran out. */
            var read = Task.Run(() => _initializer.TryReadDuckDbMemoryBytes());
            var finished = await Task.WhenAny(read, Task.Delay(HangGuard));

            Assert.Same(read, finished);
            Assert.Null(await read);
        }
        finally
        {
            releaseWriter.Set();
            writer.Join(HangGuard);
        }
    }
}
