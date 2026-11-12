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

namespace PerformanceMonitor.Common;

/// <summary>One database's I/O totals over the storage-tier window.</summary>
public sealed record StorageTierIo(
    string DatabaseName, long TotalReads, long TotalStallReadMs, long TotalWrites, long TotalStallWriteMs,
    DateTime? FirstSample, DateTime? LastSample, long WindowSamples);

/// <summary>The words of the storage-tier recommendation: the finding line and the detail sentence.</summary>
public sealed record StorageTierText(string Finding, string Detail);

/// <summary>
/// #7706: the low IO latency (storage-tier) recommendation, as one definition for both products. Darling's
/// <c>FinOpsRecommendationFigures.StorageTier</c> and Lite's recommendations read both call it, so the qualifying test, the
/// "no writes" wording and the judged-on-reads sentence cannot drift apart.
/// </summary>
public static class StorageTierFigures
{
    /// <summary>
    /// The storage-tier advice for the databases whose average read latency is under 5 ms and write latency under 3 ms, or null when
    /// none qualify. A database with no writes is not a 0 ms write latency: it stays null, still qualifies on its reads, prints as
    /// "no writes", and the detail says it was judged on reads only. The window and sample totals cover the qualifying databases only.
    /// </summary>
    public static StorageTierText? Evaluate(IEnumerable<StorageTierIo> rows)
    {
        var lowLatencyDbs = new List<(string Name, decimal AvgReadMs, decimal? AvgWriteMs)>();
        var storageMin = DateTime.MaxValue;
        var storageMax = DateTime.MinValue;
        long storageSamples = 0;

        foreach (var row in rows)
        {
            var avgReadMs = row.TotalReads > 0 ? (decimal)row.TotalStallReadMs / row.TotalReads : 0m;
            decimal? avgWriteMs = row.TotalWrites > 0 ? (decimal)row.TotalStallWriteMs / row.TotalWrites : null;

            if (avgReadMs < 5m && (avgWriteMs ?? 0m) < 3m)
            {
                lowLatencyDbs.Add((row.DatabaseName, avgReadMs, avgWriteMs));
                storageSamples += row.WindowSamples;
                if (row.FirstSample.HasValue && row.LastSample.HasValue)
                {
                    storageMin = row.FirstSample.Value < storageMin ? row.FirstSample.Value : storageMin;
                    storageMax = row.LastSample.Value > storageMax ? row.LastSample.Value : storageMax;
                }
            }
        }

        if (lowLatencyDbs.Count == 0)
            return null;

        var storageWindow = RightSizingWindow.Describe(storageSamples, storageMax > storageMin ? storageMax - storageMin : TimeSpan.Zero);
        var detail = string.Join("; ", lowLatencyDbs.Take(10)
            .Select(d => $"{d.Name} (read {d.AvgReadMs:N1}ms, {(d.AvgWriteMs is { } w ? $"write {w:N1}ms" : "no writes")})"));
        var noWritesNote = lowLatencyDbs.Take(10).Any(d => d.AvgWriteMs is null) ? " A database with no writes was judged on reads only." : "";
        return new StorageTierText(
            $"{lowLatencyDbs.Count} {(lowLatencyDbs.Count == 1 ? "database" : "databases")} with low IO latency, so standard storage may suffice",
            $"These databases have avg read latency under 5ms and write under 3ms across {storageWindow}: {detail}" +
            (lowLatencyDbs.Count > 10 ? $" and {lowLatencyDbs.Count - 10} more" : "") +
            ". Premium/high-performance storage may not be needed." + noWritesNote);
    }
}
