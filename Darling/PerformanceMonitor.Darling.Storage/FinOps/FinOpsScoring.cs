/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

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
