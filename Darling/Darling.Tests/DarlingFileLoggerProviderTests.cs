/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.IO;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #1581 field finding 1: the rolling file log must fail LOUDLY, not silently. On the field box the log
/// directory turned unwritable (an ACL artifact) and <see cref="DarlingFileLoggerProvider"/> went silent — the
/// flush catch swallowed every write, and nobody learned until they went to tail it. The provider now surfaces a
/// file-logging failure ONCE through an injectable sink (production: a best-effort Windows Event Log Warning),
/// latched so a persistently-broken log emits a single event, not one per 5s flush. These pins drive the latch
/// through the injected sink and the internal <see cref="DarlingFileLoggerProvider.Flush"/> seam — no real Event
/// Log needed.
/// </summary>
public sealed class DarlingFileLoggerProviderTests : IDisposable
{
    private readonly string _tempRoot =
        Path.Combine(Path.GetTempPath(), "darling-filelog-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempRoot))
            {
                Directory.Delete(_tempRoot, recursive: true);
            }
            else if (File.Exists(_tempRoot))
            {
                File.Delete(_tempRoot);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    /// <summary>
    /// A healthy provider — a writable log directory, every flush succeeds — NEVER surfaces the failure
    /// fallback. The sink must stay untouched across construction and repeated flushes so a working box is never
    /// spuriously paged about its own logging.
    /// </summary>
    [Fact]
    public void HealthyProvider_NeverSurfacesFailure()
    {
        var reports = new ConcurrentQueue<string>();
        var logDir = Path.Combine(_tempRoot, "logs");

        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        for (var i = 0; i < 5; i++)
        {
            logger.LogInformation("healthy line {Index}", i);
            provider.Flush();
        }

        Assert.Empty(reports);
        /* Sanity: the flushes actually wrote — otherwise "never surfaced" would be a vacuous pass. */
        Assert.True(File.Exists(provider.CurrentLogFile()), "a healthy provider must have written its log file");
    }

    /// <summary>
    /// A constructor that cannot create/enable the log directory surfaces the failure EXACTLY once, and the
    /// disabled provider never surfaces it again on later flushes. The directory is unmakeable because its parent
    /// is a FILE — a deterministic, cross-platform <see cref="Directory.CreateDirectory(string)"/> failure.
    /// </summary>
    [Fact]
    public void ConstructorDirectoryFailure_SurfacesOnce()
    {
        var reports = new ConcurrentQueue<string>();
        Directory.CreateDirectory(_tempRoot);
        var blocker = Path.Combine(_tempRoot, "blocker");
        File.WriteAllText(blocker, "not a directory");
        var unmakeableDir = Path.Combine(blocker, "logs");   /* parent is a file → CreateDirectory throws */

        using var provider = new DarlingFileLoggerProvider(unmakeableDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        /* A disabled provider drops enqueues and short-circuits flush, so later flushes must not add a second
           report. */
        for (var i = 0; i < 3; i++)
        {
            logger.LogInformation("line {Index}", i);
            provider.Flush();
        }

        Assert.Single(reports);
        Assert.True(reports.TryPeek(out var message), "the constructor failure must have surfaced a message");
        Assert.Contains("File logging is disabled", message);
    }

    /// <summary>
    /// The core latch pin: a provider that constructed healthy but whose directory then turned unwritable (the
    /// field's mid-run ACL artifact) surfaces the failure ONCE across MANY failing flushes — not one event per 5s
    /// flush. Each iteration enqueues a fresh line so every flush genuinely attempts (and fails) a write; the
    /// latch, not an emptied buffer, is what bounds the report to one.
    /// </summary>
    [Fact]
    public void RepeatedFlushFailures_SurfaceAtMostOnce()
    {
        var reports = new ConcurrentQueue<string>();
        var logDir = Path.Combine(_tempRoot, "logs");

        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        /* Break the directory the provider enabled against: replace it with a FILE so every subsequent
           File.AppendAllText(CurrentLogFile()) throws (its parent is no longer a directory). */
        Directory.Delete(logDir, recursive: true);
        File.WriteAllText(logDir, "now a file");

        for (var i = 0; i < 6; i++)
        {
            logger.LogInformation("failing line {Index}", i);
            provider.Flush();
        }

        Assert.Single(reports);
    }

    /* ---------------- #4281 review, finding 1 (Medium): a lone surrogate must not cost the batch ---------------- */

    /// <summary>
    /// File.AppendAllText's default UTF-8 encoder throws EncoderFallbackException on a lone surrogate and
    /// writes ZERO bytes -- so one malformed line used to cost the WHOLE 5-second batch, including every
    /// other line queued in the same flush. The surrogate is injected directly here, bypassing
    /// DarlingHttpRefusalLog.Sanitize entirely, to isolate this fix (Flush's encoding) from that one: Flush
    /// itself must never throw or drop a batch, whatever put the surrogate there.
    /// </summary>
    [Fact]
    public void Flush_ALoneSurrogateInOneLine_StillWritesTheBatchsOtherLines()
    {
        var reports = new ConcurrentQueue<string>();
        var logDir = Path.Combine(_tempRoot, "logs");

        using var provider = new DarlingFileLoggerProvider(logDir, reports.Enqueue);
        var logger = provider.CreateLogger("Test");

        logger.LogInformation("bad line {Tail}", new string('a', 255) + '\uD83D');
        logger.LogInformation("the other line in the same batch");
        provider.Flush();

        Assert.Empty(reports);
        var written = File.ReadAllText(provider.CurrentLogFile());
        Assert.Contains("the other line in the same batch", written, StringComparison.Ordinal);
    }

    /// <summary>#4281 review, finding 5: exception.Message can repeat request text (a PostgreSQL cast error
    /// echoes the bad value; KeyNotFoundException echoes the key) and reached the file log unsanitized -- CR/LF
    /// in it could forge a second line the same way an unsanitized route could. The FORMATTED message is
    /// deliberately left alone (DarlingWorker's "Store host profile" line embeds '\n' on purpose); only
    /// exception.Message is sanitized.</summary>
    [Fact]
    public void Log_ExceptionMessageCarriesCrLf_SanitizesSoNoForgedLineReachesTheFile()
    {
        var logDir = Path.Combine(_tempRoot, "logs");
        using var provider = new DarlingFileLoggerProvider(logDir, _ => { });
        var logger = provider.CreateLogger("Test");

        var ex = new InvalidOperationException("bad value\r\n2026-08-21 12:00:00 WARN  Forged line");
        logger.LogError(ex, "operation failed");
        provider.Flush();

        var written = File.ReadAllText(provider.CurrentLogFile());
        Assert.DoesNotContain("\r\n2026-08-21", written, StringComparison.Ordinal);
        Assert.Contains("bad value..2026-08-21 12:00:00 WARN  Forged line", written, StringComparison.Ordinal);
    }

    /* ---------------- #1652 gap 3: the RECURRING retention sweep ---------------- */

    /// <summary>
    /// The sweep the worker's daily maintenance tick calls. It used to run only from the constructor, so a
    /// service up for months — which is the normal case for a service — swept once at startup and never again
    /// while writing a file a day. Static and directory-taking so the tick needs no reference to the provider
    /// the host owns.
    /// </summary>
    [Fact]
    public void SweepOldFiles_DeletesPastRetention_KeepsRecentAndForeignFiles()
    {
        var logDir = Path.Combine(_tempRoot, "logs");
        Directory.CreateDirectory(logDir);

        var expired = WriteAged(logDir, "darling-service_20240101.log", ageDays: 30);
        var justOutside = WriteAged(logDir, "darling-service_20240102.log", ageDays: 15);
        var justInside = WriteAged(logDir, "darling-service_20240103.log", ageDays: 13);
        var foreign = WriteAged(logDir, "pg.log", ageDays: 30);

        DarlingFileLoggerProvider.SweepOldFiles(logDir);

        Assert.False(File.Exists(expired), "a 30-day-old service log survived the sweep");
        Assert.False(File.Exists(justOutside), "a 15-day-old log survived the 14-day window");
        Assert.True(File.Exists(justInside), "a 13-day-old log was deleted inside the 14-day window");
        Assert.True(File.Exists(foreign), "the sweep deleted a file that is not a service log");
    }

    /// <summary>
    /// Running on a maintenance tick means it runs forever, so it must never throw — a missing directory (the
    /// provider failed to create it, or an operator removed it mid-run) must not take down the tick that also
    /// carries the store's retention purge.
    /// </summary>
    [Fact]
    public void SweepOldFiles_MissingDirectory_DoesNotThrow() =>
        DarlingFileLoggerProvider.SweepOldFiles(Path.Combine(_tempRoot, "never-created"));

    /// <summary>14 days, the window an operator needs to reconstruct an incident they noticed last week.</summary>
    [Fact]
    public void RetentionWindow_IsFourteenDays() =>
        Assert.Equal(14, DarlingFileLoggerProvider.RetentionDays);

    /// <summary>
    /// Writes a file and backdates it. The sweep compares <see cref="File.GetLastWriteTime(string)"/>, so that
    /// is the stamp the fixture has to move.
    /// </summary>
    private static string WriteAged(string directory, string name, int ageDays)
    {
        var path = Path.Combine(directory, name);
        File.WriteAllText(path, "line");
        File.SetLastWriteTime(path, DateTime.Now.AddDays(-ageDays));
        return path;
    }
}
