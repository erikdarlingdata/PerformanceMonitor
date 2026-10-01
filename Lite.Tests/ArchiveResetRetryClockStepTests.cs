/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4732: after a size-triggered archive-and-reset fails, the next attempt waits <c>ResetRetryBackoff</c> (15 minutes): the
/// failed attempt stamps <c>ResetRetryNotBeforeUtc</c> at now plus the backoff, and the check every minute skipped the
/// attempt while the clock was before that stamp. A wall clock that stepped backwards after the stamp was taken left it
/// further ahead than the backoff ever writes it, so every attempt was skipped until the clock caught up, while the database
/// kept growing past the threshold. A stamp more than one backoff ahead now counts as due, and a wait up to one backoff is
/// honoured.
/// </summary>
/* ArchiveAllAndResetAsync takes CollectionResetGate and ArchiveService's process-wide s_archiveLock, so this
   class joins the serialized collection CollectionResetGateTests defines. */
[Collection("CollectionResetGate")]
public sealed class ArchiveResetRetryClockStepTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public ArchiveResetRetryClockStepTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
    }

    public void Dispose()
    {
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

    /* A service whose every table export fails, so a reset attempt that goes ahead ends in the failure path that stamps
       the backoff, and one that is skipped never reaches an export. */
    private async Task<(ArchiveService Service, Func<int> ExportsStarted)> FailingServiceAsync()
    {
        var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeAsync();

        using (var connection = new DuckDBConnection($"Data Source={_dbPath}"))
        {
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
SELECT i, 1, 'S1', 'wait_stats', TIMESTAMP '2026-09-01 00:00:00' + INTERVAL (i) MINUTE, 'SUCCESS' FROM range(1, 11) t(i)";
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }

        var service = new ArchiveService(initializer, _archiveDir);
        var exportsStarted = 0;
        service.BeforeTableExportForTests = _ =>
        {
            exportsStarted++;
            throw new IOException("There is not enough space on the disk.");
        };

        return (service, () => exportsStarted);
    }

    [Fact]
    public async Task AStampFarPastTheBackoffAhead_DoesNotSkipTheAttempt()
    {
        var (service, exportsStarted) = await FailingServiceAsync();

        /* The failed attempt stamped now + 15 minutes; the clock then stepped back three hours, so the stamp is
           three hours and 15 minutes ahead. */
        service.ResetRetryNotBeforeUtc = DateTime.UtcNow.AddHours(3);
        await service.ArchiveAllAndResetAsync();

        Assert.True(exportsStarted() > 0, "a stamp far past one backoff ahead can only be a clock step: the attempt must run");

        /* It failed again, so it stamped the next retry one backoff from the clock's own reading. */
        Assert.True(service.ResetRetryNotBeforeUtc < DateTime.UtcNow.AddMinutes(20));
        Assert.True(service.ResetRetryNotBeforeUtc > DateTime.UtcNow.AddMinutes(10));
    }

    [Fact]
    public async Task AStampInsideTheBackoff_StillSkipsTheAttempt_AndLeavesTheStampAlone()
    {
        var (service, exportsStarted) = await FailingServiceAsync();

        var stamp = DateTime.UtcNow.AddMinutes(5);
        service.ResetRetryNotBeforeUtc = stamp;
        await service.ArchiveAllAndResetAsync();

        Assert.Equal(0, exportsStarted());
        Assert.Equal(stamp, service.ResetRetryNotBeforeUtc);
    }

    [Fact]
    public async Task TheClampUsesTheServicesOwnBackoff()
    {
        var (service, exportsStarted) = await FailingServiceAsync();
        service.ResetRetryBackoff = TimeSpan.FromMinutes(2);

        /* Ten minutes ahead is well inside the default 15 minute backoff, but the writer of this service stamps two
           minutes ahead at most, so ten can only be a step. */
        service.ResetRetryNotBeforeUtc = DateTime.UtcNow.AddMinutes(10);
        await service.ArchiveAllAndResetAsync();
        Assert.True(exportsStarted() > 0);

        /* One minute ahead is a wait under a two minute backoff. */
        var before = exportsStarted();
        var stamp = DateTime.UtcNow.AddMinutes(1);
        service.ResetRetryNotBeforeUtc = stamp;
        await service.ArchiveAllAndResetAsync();
        Assert.Equal(before, exportsStarted());
        Assert.Equal(stamp, service.ResetRetryNotBeforeUtc);
    }

    [Fact]
    public void TheArithmetic_ClampsAtOneBackoff_AndTheDefaultBackoffIsFifteenMinutes()
    {
        var now = new DateTime(2026, 9, 29, 12, 0, 0, DateTimeKind.Utc);
        var backoff = TimeSpan.FromMinutes(15);

        Assert.Equal(backoff, new ArchiveService(new DuckDbInitializer(Path.Combine(_tempDir, "unused.duckdb")), _archiveDir).ResetRetryBackoff);

        /* The stamp a failed attempt writes is the longest lead there is, and is a wait. */
        Assert.Equal(now + backoff, CollectorCadence.ClampDue(now + backoff, now, backoff));

        /* One tick past it, or a stamp hours ahead, counts as due now. */
        Assert.Equal(now, CollectorCadence.ClampDue(now + backoff + TimeSpan.FromTicks(1), now, backoff));
        Assert.Equal(now, CollectorCadence.ClampDue(now.AddHours(3), now, backoff));

        /* An unset stamp, and one already past, are left alone. */
        Assert.Equal(DateTime.MinValue, CollectorCadence.ClampDue(DateTime.MinValue, now, backoff));
        Assert.Equal(now.AddMinutes(-1), CollectorCadence.ClampDue(now.AddMinutes(-1), now, backoff));
    }

    [Fact]
    public void TheCheck_GoesThroughTheClamp_WithTheBackoffEveryWriterStampsWith()
    {
        var source = ReadRepoFile("Lite/Services/ArchiveService.cs");

        Assert.DoesNotContain("DateTime.UtcNow < ResetRetryNotBeforeUtc", source, StringComparison.Ordinal);
        Assert.Contains(
            "resetNow < CollectorCadence.ClampDue(ResetRetryNotBeforeUtc, resetNow, ResetRetryBackoff)", source, StringComparison.Ordinal);

        /* The interval passed to the clamp is the one every writer of the stamp adds to the clock: every
           assignment must be the backoff form, however many writers there are. */
        var correct = Regex.Matches(source, Regex.Escape("ResetRetryNotBeforeUtc = DateTime.UtcNow + ResetRetryBackoff;")).Count;
        var all = Regex.Matches(source, @"\bResetRetryNotBeforeUtc\s*=[^=]").Count;
        Assert.True(all >= 2, "expected at least two writers of the reset retry stamp");
        Assert.Equal(all, correct);
    }

    /* Locate the repo from this file: no build-output copying. */
    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
