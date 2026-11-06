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
using System.Threading.Tasks;

namespace PerformanceMonitor.Ui;

/// <summary>
/// What the Overview says when one of its lane reads fails. The ten reads run as one batch and each lane is drawn from its own
/// read, so one failure costs one lane and the others draw as usual. The failed lane is marked as failed, and the tab's status line
/// names the reads that failed instead of saying the tab refreshed.
/// </summary>
public static class OverviewLaneReadFailures
{
    /// <summary>
    /// Names the reads that did not complete, in the order given, with the first failure's message; null when every read completed.
    /// </summary>
    public static string? Summary(IReadOnlyList<(string Read, Task Task)> reads)
    {
        ArgumentNullException.ThrowIfNull(reads);

        var failed = reads.Where(r => !r.Task.IsCompletedSuccessfully).ToList();
        if (failed.Count == 0)
            return null;

        var names = string.Join(", ", failed.Select(r => r.Read));
        var message = failed
            .Select(r => r.Task.Exception?.GetBaseException().Message)
            .FirstOrDefault(m => !string.IsNullOrWhiteSpace(m));

        return message is null
            ? $"Overview lanes did not read {names}"
            : $"Overview lanes did not read {names}: {message}";
    }
}
