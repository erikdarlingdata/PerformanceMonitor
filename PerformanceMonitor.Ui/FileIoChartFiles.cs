/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Which files the File I/O tab's two latency charts draw. The latency read returns up to twenty files: the ten with the
/// most reads and the ten with the most writes. The read chart draws the ten busiest by reads and the write chart the
/// ten busiest by writes, each ranked on its own count. Cutting the combined list back to one shared ten by summed
/// latency let twelve busy log files (3 ms writes, no reads) push both data files off the Read Latency chart, and drew
/// every write-only log file as a flat 0 ms read line.
/// </summary>
public static class FileIoChartFiles
{
    public const int SeriesPerChart = 10;

    /// <summary>
    /// The groups (one per file key) for the read chart: files with reads, the ten with the most, a tie going to the file
    /// with more writes and then to the key, the order the read ranks by. Each group's points are left as given.
    /// </summary>
    public static IReadOnlyList<IGrouping<string, T>> ReadChartFiles<T>(IEnumerable<T> points, Func<T, string> fileKey, Func<T, long> reads, Func<T, long> writes)
        => Top(points, fileKey, reads, writes);

    /// <summary>The write chart's files: files with writes, the ten with the most, a tie going to the file with more reads and then to the key.</summary>
    public static IReadOnlyList<IGrouping<string, T>> WriteChartFiles<T>(IEnumerable<T> points, Func<T, string> fileKey, Func<T, long> reads, Func<T, long> writes)
        => Top(points, fileKey, writes, reads);

    private static List<IGrouping<string, T>> Top<T>(IEnumerable<T> points, Func<T, string> fileKey, Func<T, long> primary, Func<T, long> secondary)
        => points
            .GroupBy(fileKey)
            .Select(g => (Group: g, Primary: g.Sum(primary), Secondary: g.Sum(secondary)))
            .Where(x => x.Primary > 0)
            .OrderByDescending(x => x.Primary)
            .ThenByDescending(x => x.Secondary)
            .ThenBy(x => x.Group.Key, StringComparer.Ordinal)
            .Take(SeriesPerChart)
            .Select(x => x.Group)
            .ToList();
}
