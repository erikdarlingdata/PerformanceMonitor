/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9862: a query tag sighting must outlive the oldest archived usage row that can still exist, or the last days of an
/// archive file read with no tag. Both ends are built from the real constants and the real archive cutoff, on a fake clock.
/// </summary>
public sealed class QueryTagSightingWindowTests
{
    /// <summary>
    /// The latest instant an archived row can still be read, given when it was collected. The row stays hot for
    /// <see cref="ArchiveService.HotDataDays"/>, so the first hourly archive run after that writes it, into a file
    /// named for that run's date. <see cref="RetentionService.CleanupOldArchives"/> then deletes the file at the first
    /// daily run after the real <see cref="RetentionService.ArchiveCutoff"/> passes the file's date. Both runs are taken
    /// at their worst phase: the archive run just missed, and the cleanup run just missed.
    /// </summary>
    private static DateTime LastInstantAnArchivedRowCanBeRead(DateTime collectedUtc)
    {
        var written = collectedUtc.AddDays(ArchiveService.HotDataDays) + CollectionBackgroundService.ArchiveInterval;
        var fileDate = written.Date;

        /* The cutoff is calendar months, so the crossing is found by stepping, not by adding a fixed day count. */
        var crossing = fileDate.AddDays(RetentionService.ArchiveRetentionMonths * 28);
        while (!(fileDate < RetentionService.ArchiveCutoff(crossing)))
        {
            crossing = crossing.AddHours(1);
        }

        return crossing + CollectionBackgroundService.RetentionInterval;
    }

    [Fact]
    public void ASighting_OutlivesTheOldestArchivedRowItTags_OnEveryDayOfTheYear()
    {
        var sightingDays = LocalDataService.QueryTagSightingRetentionDays;
        var worstShortfall = TimeSpan.Zero;
        DateTime? worstCollected = null;

        /* Two years of hourly collection times, so every month-length combination is crossed, the long July to September
           span included. A sighting is last seen when its row is collected, and goes at last_seen + the window. */
        for (var collected = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
             collected < new DateTime(2028, 1, 1, 0, 0, 0, DateTimeKind.Utc);
             collected = collected.AddHours(1))
        {
            var rowGoneAt = LastInstantAnArchivedRowCanBeRead(collected);
            var sightingGoneAt = collected.AddDays(sightingDays);
            var shortfall = rowGoneAt - sightingGoneAt;
            if (shortfall > worstShortfall)
            {
                worstShortfall = shortfall;
                worstCollected = collected;
            }
        }

        Assert.True(worstShortfall <= TimeSpan.Zero,
            $"The sighting window ({sightingDays} days) is shorter than the archive's longest life: a row collected {worstCollected:u} is still archived {worstShortfall.TotalHours:F0} hours after its sighting is gone.");
    }
}
