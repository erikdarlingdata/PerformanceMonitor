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
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9407: retention deletes an expired archive file under the store write lock only, so a sort or a read that has the file open
/// makes the delete fail on Windows. That is logged as information, not as an error, and the file is deleted by the next run, once it is free (Store Health still records the run's failure).
/// </summary>
public sealed class RetentionFileInUseTests : IDisposable
{
    private readonly string _archiveDir;

    public RetentionFileInUseTests()
    {
        _archiveDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8], "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(Path.GetDirectoryName(_archiveDir)!, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public void AnExpiredFileInUse_IsNotLoggedAsAnError_AndIsDeletedByTheNextRun()
    {
        var path = Path.Combine(_archiveDir, "20200101_query_store_stats.parquet");
        File.WriteAllText(path, "x");
        var levels = new List<LogLevel>();
        var retention = new RetentionService(_archiveDir, new RecordingLogger(levels));

        /* A handle without FileShare.Delete, as a sort reading the file holds. */
        using (new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
        {
            Assert.Equal(0, retention.CleanupOldArchives());
            Assert.True(File.Exists(path));
            Assert.Contains("20200101_query_store_stats.parquet", retention.LastCleanupError);
            Assert.DoesNotContain(LogLevel.Error, levels);
        }

        Assert.Equal(1, retention.CleanupOldArchives());
        Assert.False(File.Exists(path));
    }

    private sealed class RecordingLogger(List<LogLevel> levels) : ILogger<RetentionService>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            levels.Add(logLevel);
    }
}
