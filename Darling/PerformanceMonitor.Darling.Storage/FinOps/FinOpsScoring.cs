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

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>Pure health-score math (Utilization + Server Inventory). Copied verbatim from Lite.</summary>
public static class FinOpsHealthCalculator
{
    public static int CpuScore(decimal p95Pct)
    {
        if (p95Pct <= 70) return (int)(100 - p95Pct * 50 / 70);
        return (int)Math.Max(0, 50 - (p95Pct - 70) * 50 / 30);
    }

    public static int MemoryScore(decimal bufferPoolRatio)
    {
        if (bufferPoolRatio <= 0.30m) return 60;
        if (bufferPoolRatio <= 0.85m) return 100;
        if (bufferPoolRatio <= 0.95m) return (int)(100 - (bufferPoolRatio - 0.85m) * 800);
        return (int)Math.Max(0, 20 - (bufferPoolRatio - 0.95m) * 400);
    }

    public static int StorageScore(decimal freeSpacePct)
    {
        if (freeSpacePct >= 30) return 100;
        if (freeSpacePct >= 10) return (int)(50 + (freeSpacePct - 10) * 2.5m);
        return (int)(freeSpacePct * 5);
    }

    /// <summary>
    /// The overall score: CPU 40%, memory 30%, storage 30%. A null <paramref name="cpu"/> means the window held no CPU
    /// sample: there is nothing to score, and scoring the 0 it reads as would be a full 100 made from nothing. The term is
    /// then left out, not scored as zero and not scored as a default, and memory and storage keep their weights over their
    /// own total (30:30 over 60).
    /// </summary>
    public static int Overall(int? cpu, int memory, int storage)
    {
        if (cpu is int cpuScore)
            return (int)(cpuScore * 0.40 + memory * 0.30 + storage * 0.30);

        /* integer weights, so no floating-point error can truncate 100 to 99 */
        return (memory * 30 + storage * 30) / 60;
    }

    public static string ScoreColor(int score) => score switch
    {
        >= 80 => "#27AE60",
        >= 60 => "#F39C12",
        _ => "#E74C3C"
    };
}

/// <summary>
/// Identifies top-N queries per resource dimension, computes PERCENT_RANK and share percentages, and
/// returns the "interesting" set sorted by impact score. Copied verbatim from Lite's HighImpactScorer.
/// </summary>
public static class HighImpactScorer
{
    /// <summary>The band for an impact score: high at 80 or more, medium at 60 or more, else low. These are the
    /// cut points the desktop viewer colors the score by.</summary>
    public static string HighImpactBand(int score) => score >= 80 ? "high" : score >= 60 ? "medium" : "low";

    public static List<HighImpactQuery> Score(List<HighImpactQuery> allRows, int topN = 10)
    {
        if (allRows.Count == 0) return allRows;

        var interesting = new HashSet<string>();
        foreach (var hash in allRows.OrderByDescending(r => r.TotalCpuMs).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalDurationMs).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalReads).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalWrites).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalMemoryMb).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);
        foreach (var hash in allRows.OrderByDescending(r => r.TotalExecutions).Take(topN).Select(r => r.QueryHash)) interesting.Add(hash);

        var filtered = allRows.Where(r => interesting.Contains(r.QueryHash)).ToList();

        if (filtered.Count == 0) return filtered;

        var cpuValues = filtered.Select(r => r.TotalCpuMs).OrderBy(v => v).ToList();
        var durationValues = filtered.Select(r => r.TotalDurationMs).OrderBy(v => v).ToList();
        var readsValues = filtered.Select(r => (decimal)r.TotalReads).OrderBy(v => v).ToList();
        var writesValues = filtered.Select(r => (decimal)r.TotalWrites).OrderBy(v => v).ToList();
        var memoryValues = filtered.Select(r => r.TotalMemoryMb).OrderBy(v => v).ToList();
        var execValues = filtered.Select(r => (decimal)r.TotalExecutions).OrderBy(v => v).ToList();

        var totalCpu = filtered.Sum(r => r.TotalCpuMs);
        var totalDuration = filtered.Sum(r => r.TotalDurationMs);
        var totalReads = filtered.Sum(r => (decimal)r.TotalReads);
        var totalWrites = filtered.Sum(r => (decimal)r.TotalWrites);
        var totalMemory = filtered.Sum(r => r.TotalMemoryMb);
        var totalExecs = filtered.Sum(r => (decimal)r.TotalExecutions);

        foreach (var row in filtered)
        {
            var cpuPctl = PercentRank(cpuValues, row.TotalCpuMs);
            var durationPctl = PercentRank(durationValues, row.TotalDurationMs);
            var readsPctl = PercentRank(readsValues, (decimal)row.TotalReads);
            var writesPctl = PercentRank(writesValues, (decimal)row.TotalWrites);
            var memoryPctl = PercentRank(memoryValues, row.TotalMemoryMb);
            var execsPctl = PercentRank(execValues, (decimal)row.TotalExecutions);

            row.CpuShare = totalCpu > 0 ? Math.Round(100m * row.TotalCpuMs / totalCpu, 1) : 0;
            row.DurationShare = totalDuration > 0 ? Math.Round(100m * row.TotalDurationMs / totalDuration, 1) : 0;
            row.ReadsShare = totalReads > 0 ? Math.Round(100m * row.TotalReads / totalReads, 1) : 0;
            row.WritesShare = totalWrites > 0 ? Math.Round(100m * row.TotalWrites / totalWrites, 1) : 0;
            row.MemoryShare = totalMemory > 0 ? Math.Round(100m * row.TotalMemoryMb / totalMemory, 1) : 0;
            row.ExecutionsShare = totalExecs > 0 ? Math.Round(100m * row.TotalExecutions / totalExecs, 1) : 0;

            var pctlSum = cpuPctl + durationPctl + readsPctl + writesPctl + memoryPctl + execsPctl;
            row.ImpactScore = (int)(pctlSum / 6m * 100m);
        }

        return filtered.OrderByDescending(r => r.ImpactScore).ToList();
    }

    internal static decimal PercentRank(List<decimal> sortedValues, decimal value)
    {
        if (sortedValues.Count <= 1) return 0;
        int rank = sortedValues.Count(v => v < value);
        return Math.Min(1.0m, (decimal)rank / (sortedValues.Count - 1));
    }
}
