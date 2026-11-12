/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Retention, the findings purge and the dismissed-alert purge each run on a 24-hour interval from an in-memory stamp
/// that is never saved. A desktop session is usually shorter than a day, and a laptop sleeps, so the three must count as
/// due on a fresh service and run on its first housekeeping tick; the next run is then a day away as before. Each step
/// is invoked the way the housekeeping loop invokes it, against a real template database, and the stamp is read back:
/// the stamp is the one record that a step ran.
/// </summary>
public sealed class HousekeepingFirstRunTests : IDisposable
{
    private static readonly BindingFlags NonPublic = BindingFlags.NonPublic | BindingFlags.Instance;

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly string _archiveDir;

    public HousekeepingFirstRunTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        _archiveDir = Path.Combine(_tempDir, "archive");
        Directory.CreateDirectory(_archiveDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Theory]
    [InlineData("RunRetentionIfDueAsync", "_lastRetentionTime")]
    [InlineData("RunFindingsCleanupIfDueAsync", "_lastFindingsCleanupTime")]
    [InlineData("RunDismissedAlertsCleanupIfDueAsync", "_lastDismissedAlertsCleanupTime")]
    public async Task AFreshService_CountsItsDailyCleanupAsDue_AndRunsItOnTheFirstTick(string stepName, string stampName)
    {
        using var initializer = new DuckDbInitializer(_dbPath);
        await initializer.InitializeFromTemplateAsync();
        var expired = Path.Combine(_archiveDir, "200001_collection_log.parquet");
        File.WriteAllText(expired, "x");
        var service = new CollectionBackgroundService(null!, initializer, retentionService: new RetentionService(_archiveDir));
        var stampField = typeof(CollectionBackgroundService).GetField(stampName, NonPublic)!;
        var step = typeof(CollectionBackgroundService).GetMethod(stepName, NonPublic)!;

        var seed = (DateTime)stampField.GetValue(service)!;
        Assert.True(
            CollectionBackgroundService.HousekeepingIsDue(seed, CollectionBackgroundService.RetentionInterval, DateTime.UtcNow),
            $"a fresh service reads {stampName} as {seed:O}, which is not due for a day");

        await (Task)step.Invoke(service, null)!;

        var ran = (DateTime)stampField.GetValue(service)!;
        Assert.True(ran > seed, $"{stepName} left {stampName} at its seed, so the step did not run");
        Assert.True(DateTime.UtcNow - ran < TimeSpan.FromMinutes(1), "the stamp after the run is the clock at the time it ran");
        if (stepName == "RunRetentionIfDueAsync")
        {
            Assert.False(File.Exists(expired), "the first retention pass deleted the expired archive");
        }
    }
}
