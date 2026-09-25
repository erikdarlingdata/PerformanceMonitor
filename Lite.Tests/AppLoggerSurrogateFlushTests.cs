/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4286/#4281 review parity: <see cref="AppLogger"/>'s <c>Flush</c> used the no-arg
/// <c>File.AppendAllText</c> overload, whose strict UTF-8 encoder throws <c>EncoderFallbackException</c> on a
/// lone surrogate and writes ZERO bytes — dropping every OTHER line already dequeued into that batch, not
/// just the offending one. Mirrors <c>DarlingFileLoggerProviderTests.Flush_ALoneSurrogateInOneLine_StillWritesTheBatchsOtherLines</c>.
///
/// <para>Drives the write through <see cref="AppLogger.FlushTo"/> rather than <see cref="AppLogger.Flush"/>:
/// the public overload only writes once <see cref="AppLogger.Initialize"/> has run, and
/// <c>Initialize</c> is not usable from a test — it repoints the whole process's logging and starts a 5s
/// timer, the same hazard <c>AppLoggerRetentionTests</c> documents. <c>FlushTo</c> is the write half of
/// <c>Flush</c> with the directory as a parameter, so the batch really goes through disk without touching
/// the process-wide static.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class AppLoggerSurrogateFlushTests : IDisposable
{
    private readonly string _logDir =
        Path.Combine(Path.GetTempPath(), "lite-applog-surrogate-tests", Guid.NewGuid().ToString("N"));

    public AppLoggerSurrogateFlushTests() => Directory.CreateDirectory(_logDir);

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_logDir))
            {
                Directory.Delete(_logDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    [Fact]
    public void FlushTo_ALoneSurrogateInOneLine_StillWritesTheBatchsOtherLines()
    {
        /* Tagged so concurrently-running tests logging into the same process-wide buffer cannot be mistaken
           for this one's lines, and so this one's cannot be mistaken for theirs. */
        var tag = Guid.NewGuid().ToString("N");

        AppLogger.DrainBufferedLines();
        AppLogger.Info("Test", $"bad line {new string('a', 255)}\uD83D");
        AppLogger.Info("Test", $"the other line in the same batch {tag}");

        AppLogger.FlushTo(_logDir);

        var logFile = Path.Combine(_logDir, $"lite_{DateTime.Now:yyyyMMdd}.log");
        Assert.True(File.Exists(logFile), "the batch's surviving line never reached disk");
        var written = File.ReadAllText(logFile);
        Assert.Contains($"the other line in the same batch {tag}", written, StringComparison.Ordinal);
    }
}
