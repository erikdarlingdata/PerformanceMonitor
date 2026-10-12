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
/// most reads and the ten with the most writes, and it says which ten each file is in (two flag columns,
/// <c>read_rank &lt;= 10 AND total_reads &gt; 0</c> and <c>write_rank &lt;= 10 AND total_writes &gt; 0</c>). The read chart draws the
/// files flagged for reads and the write chart the files flagged for writes, exactly as the read ranked them. Cutting the
/// combined list back to one shared ten by summed latency let twelve busy log files (3 ms writes, no reads) push both data files off
/// the Read Latency chart, and drew every write-only log file as a flat 0 ms read line. Ranking again here on the points' own
/// counts was no better: those counts leave out the rows with no interval, so a chart's ten could differ from the read's ten,
/// and a tie broke on the joined "db.file" text, not on (database, file) the way the read breaks it.
/// </summary>
public static class FileIoChartFiles
{
    /// <summary>The groups (one per file key) for the read chart: the files the read flagged as in its ten by reads. Each group's points are left as given.</summary>
    public static IReadOnlyList<IGrouping<string, T>> ReadChartFiles<T>(IEnumerable<T> points, Func<T, string> fileKey, Func<T, bool> inReadTen)
        => Flagged(points, fileKey, inReadTen);

    /// <summary>The write chart's files: the files the read flagged as in its ten by writes.</summary>
    public static IReadOnlyList<IGrouping<string, T>> WriteChartFiles<T>(IEnumerable<T> points, Func<T, string> fileKey, Func<T, bool> inWriteTen)
        => Flagged(points, fileKey, inWriteTen);

    private static List<IGrouping<string, T>> Flagged<T>(IEnumerable<T> points, Func<T, string> fileKey, Func<T, bool> flag)
        => points
            .Where(flag)
            .GroupBy(fileKey)
            .ToList();
}
