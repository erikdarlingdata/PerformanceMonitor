/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Walk finding D15: times one inner-tab load by phase, so a slow tab leaves one log line saying where the time went
/// (the store reads, in the order they finished, against the time left over once the last one was in: the client's
/// shaping and binding) instead of a bare "it took 20 seconds". A load that finishes inside
/// <see cref="SlowLoadThresholdMs"/> logs nothing.
///
/// <para>Phases may overlap (the grid read, the slicer read and the data-start probe run side by side), so each one is
/// timed from the moment it is handed to <see cref="Track{T}"/> to the moment it completes, and the client share is
/// whatever is left after the LAST phase completed, not a sum.</para>
/// </summary>
internal sealed class ViewerLoadTimer
{
    /// <summary>A load slower than this is logged. The walk called anything over three seconds slow.</summary>
    internal const int SlowLoadThresholdMs = 3000;

    private readonly Stopwatch _total = Stopwatch.StartNew();
    private readonly List<(string Phase, long StartedMs, long EndedMs)> _phases = new();
    private readonly object _gate = new();

    /// <summary>What the load is: set by a loader that knows its sub-tab ("Queries > Top Queries by Duration").</summary>
    internal string Surface { get; set; } = "";

    /// <summary>Times <paramref name="task"/> (already started) as <paramref name="phase"/> and hands it back unchanged, so a
    /// caller awaits it exactly as before. A faulted or cancelled task is still recorded; its own awaiter sees the error.</summary>
    internal Task<T> Track<T>(string phase, Task<T> task)
    {
        var started = _total.ElapsedMilliseconds;
        _ = task.ContinueWith(
            _ => Record(phase, started),
            System.Threading.CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
    }

    /// <summary><see cref="Track{T}"/> for a task with no result.</summary>
    internal Task Track(string phase, Task task)
    {
        var started = _total.ElapsedMilliseconds;
        _ = task.ContinueWith(
            _ => Record(phase, started),
            System.Threading.CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        return task;
    }

    private void Record(string phase, long startedMs)
    {
        var ended = _total.ElapsedMilliseconds;
        lock (_gate)
        {
            _phases.Add((phase, startedMs, ended));
        }
    }

    /// <summary>The log line for a load of <paramref name="totalMs"/>, or null when it was not slow. Pure, so a test reads the text.</summary>
    internal static string? Describe(string surface, long totalMs, IReadOnlyList<(string Phase, long StartedMs, long EndedMs)> phases)
    {
        if (totalMs < SlowLoadThresholdMs)
        {
            return null;
        }

        var lastReadEnded = phases.Count == 0 ? 0 : phases.Max(p => p.EndedMs);
        var clientMs = Math.Max(0, totalMs - lastReadEnded);
        var split = phases.Count == 0
            ? "no store phase timed"
            : string.Join(", ", phases.OrderBy(p => p.EndedMs).Select(p => $"{p.Phase} {p.EndedMs - p.StartedMs} ms"));
        return $"slow load: {surface} took {totalMs} ms: store reads [{split}], then {clientMs} ms of client work after the last read";
    }

    /// <summary>Stops the clock and returns the slow-load line, or null when the load was quick.</summary>
    internal string? Finish(string fallbackSurface)
    {
        _total.Stop();
        lock (_gate)
        {
            return Describe(string.IsNullOrEmpty(Surface) ? fallbackSurface : Surface, _total.ElapsedMilliseconds, _phases.ToList());
        }
    }
}
