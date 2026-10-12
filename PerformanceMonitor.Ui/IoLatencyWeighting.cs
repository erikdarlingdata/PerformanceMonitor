/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The one-figure-per-time latency the Overview "I/O Latency ms" lane draws from several files' points. Each
/// file's point is already a ratio (summed stall over summed operations), so the lane's figure is the I/O-weighted
/// mean of those ratios: total stall over total operations, counting only files that had operations at that time.
/// A plain average of the ratios counted a log file with no reads as a 0 ms read and pulled the read figure down.
/// </summary>
public static class IoLatencyWeighting
{
    /// <summary>
    /// The latency over the files in <paramref name="points"/> that had operations: the sum of latency times
    /// operations over the sum of operations. A file with no operations does not take part. Returns 0 when no file
    /// had any (nothing was read, so there is nothing to average).
    /// </summary>
    public static double Weighted(IEnumerable<(double LatencyMs, long Operations)> points)
    {
        double stall = 0;
        double operations = 0;
        foreach (var (latencyMs, ops) in points)
        {
            if (ops <= 0)
            {
                continue;
            }

            stall += latencyMs * ops;
            operations += ops;
        }

        return operations > 0 ? stall / operations : 0;
    }
}
