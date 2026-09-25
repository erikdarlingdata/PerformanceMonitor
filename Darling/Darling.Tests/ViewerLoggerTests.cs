/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4286/#4281 review parity: <see cref="ViewerLogger"/>'s <c>Flush</c> used the no-arg
/// <c>File.AppendAllText</c> overload, whose strict UTF-8 encoder throws <c>EncoderFallbackException</c> on a
/// lone surrogate and writes ZERO bytes — dropping every OTHER line already dequeued into that batch, not
/// just the offending one. Mirrors <c>DarlingFileLoggerProviderTests.Flush_ALoneSurrogateInOneLine_StillWritesTheBatchsOtherLines</c>.
///
/// <para><c>[Collection("viewer-logger-statics")]</c>: like <c>ViewerLogger</c> itself, this is a process-wide
/// static singleton, so this is the named place a future class touching it should join. Nothing else does
/// today. One test method by design: <see cref="ViewerLogger.Shutdown"/> disposes the static flush timer, so
/// a second <see cref="ViewerLogger.Initialize"/> in the same class would land in its catch and silently
/// no-op the logger.</para>
/// </summary>
[Collection("viewer-logger-statics")]
public sealed class ViewerLoggerTests : IDisposable
{
    private readonly string _logDir =
        Path.Combine(Path.GetTempPath(), "darling-viewerlog-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            ViewerLogger.Shutdown();
        }
        catch
        {
            /* Best-effort: Shutdown never throws today, but this is test cleanup, not the pin. */
        }

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
    public void Flush_ALoneSurrogateInOneLine_StillWritesTheBatchsOtherLines()
    {
        /* Tagged so a concurrently-running test elsewhere in the process cannot be mistaken for this one's
           line. Initialize() also enqueues its own "Logging initialized" line -- assert on the tag, not on
           how many lines the file holds. */
        var tag = Guid.NewGuid().ToString("N");

        ViewerLogger.Initialize(_logDir);

        ViewerLogger.Info("Test", $"bad line {new string('a', 255)}\uD83D");
        ViewerLogger.Info("Test", $"the other line in the same batch {tag}");
        ViewerLogger.Flush();

        var logFile = ViewerLogger.GetCurrentLogFile();
        Assert.True(File.Exists(logFile), "the batch's surviving line never reached disk");
        var written = File.ReadAllText(logFile);
        Assert.Contains($"the other line in the same batch {tag}", written, StringComparison.Ordinal);
    }
}
