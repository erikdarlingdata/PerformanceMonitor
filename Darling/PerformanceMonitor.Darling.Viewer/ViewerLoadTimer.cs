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
public sealed class ViewerLoadTimer
{
    /// <summary>A load slower than this is logged. The walk called anything over three seconds slow.</summary>
    internal const int SlowLoadThresholdMs = 3000;

    private readonly Stopwatch _total = Stopwatch.StartNew();
    private readonly List<(string Phase, long StartedMs, long EndedMs)> _phases = new();
    private readonly List<(string Step, long Ms)> _clientSteps = new();
    private readonly object _gate = new();

    /// <summary>What the load is: set by a loader that knows its sub-tab ("Queries > Top Queries by Duration").</summary>
    internal string Surface { get; set; } = "";

    /// <summary>Times <paramref name="task"/> (already started) as <paramref name="phase"/> and hands it back unchanged, so a
    /// caller awaits it exactly as before. A faulted or cancelled task is still recorded; its own awaiter sees the error.</summary>
    internal Task<T> Track<T>(string phase, Task<T> task)
    {
        _ = Watch(phase, _total.ElapsedMilliseconds, task);
        return task;
    }

    /// <summary><see cref="Track{T}"/> for a task with no result.</summary>
    internal Task Track(string phase, Task task)
    {
        _ = Watch(phase, _total.ElapsedMilliseconds, task);
        return task;
    }

    /// <summary>#8549: times a stretch of the client's own work (a grid build, a chart draw, a converter) as <paramref name="step"/> and
    /// runs it, so a slow load's line names the step that kept the UI thread busy instead of one lump of "client work". Steps run one
    /// after another on the UI thread, so they are summed per name and listed after the client share. A step that throws is still booked.</summary>
    internal void Step(string step, Action work)
    {
        var started = _total.ElapsedMilliseconds;
        try
        {
            work();
        }
        finally
        {
            RecordStep(step, _total.ElapsedMilliseconds - started);
        }
    }

    /// <summary><see cref="Step(string, Action)"/> for work that answers a value.</summary>
    internal T Step<T>(string step, Func<T> work)
    {
        var result = default(T);
        Action run = () => result = work();
        Step(step, run);
        return result!;
    }

    private void RecordStep(string step, long ms)
    {
        lock (_gate)
        {
            _clientSteps.Add((step, ms));
        }
    }

    /// <summary>Records <paramref name="phase"/> when <paramref name="task"/> ends, however it ends. Written as an await rather than a
    /// <c>ContinueWith</c>, which the Viewer's concurrency census keeps out of the project (a continuation is a fan-out the width
    /// pin does not model). A faulted task's error is its awaiter's to see, so it is swallowed here.</summary>
    private async Task Watch(string phase, long startedMs, Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception)
        {
            /* The awaiter of the task sees the failure; this watcher only needs to know when it ended. */
        }

        Record(phase, startedMs);
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
    internal static string? Describe(string surface, long totalMs, IReadOnlyList<(string Phase, long StartedMs, long EndedMs)> phases,
        IReadOnlyList<(string Step, long Ms)>? clientSteps = null)
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
        var line = $"slow load: {surface} took {totalMs} ms: store reads [{split}], then {clientMs} ms of client work after the last read";
        if (clientSteps is { Count: > 0 })
        {
            /* #8549: the client's share by named step, in the order the load first ran each one. */
            var steps = clientSteps.GroupBy(s => s.Step).Select(g => $"{g.Key} {g.Sum(s => s.Ms)} ms");
            line += $" [client steps: {string.Join(", ", steps)}]";
        }

        return line;
    }

    /// <summary>The names of the store reads this load has booked, in the order they finished (a test reads it).</summary>
    internal IReadOnlyList<string> PhaseNames
    {
        get
        {
            lock (_gate)
            {
                return _phases.OrderBy(p => p.EndedMs).Select(p => p.Phase).ToList();
            }
        }
    }

    /// <summary>The names of the client steps this load has run, in order (a test reads it).</summary>
    internal IReadOnlyList<string> StepNames
    {
        get
        {
            lock (_gate)
            {
                return _clientSteps.Select(s => s.Step).ToList();
            }
        }
    }

    /// <summary>How many store reads this load has booked so far (a test reads it; the slow-load line is the product's read).</summary>
    internal int PhaseCount
    {
        get
        {
            lock (_gate)
            {
                return _phases.Count;
            }
        }
    }

    /// <summary>#8549: stops the clock and writes the slow-load line to the Viewer log (the "SlowLoad" source the server tab's loads use) when
    /// the load was slow. For a fleet tab, which has no inner-tab shell to do it. <paramref name="surface"/> names the tab.</summary>
    internal void FinishAndLog(string surface)
    {
        if (Finish(surface) is { } slow)
        {
            ViewerLogger.Warn("SlowLoad", slow);
        }
    }

    /// <summary>Stops the clock and returns the slow-load line, or null when the load was quick.</summary>
    internal string? Finish(string fallbackSurface)
    {
        _total.Stop();
        lock (_gate)
        {
            return Describe(string.IsNullOrEmpty(Surface) ? fallbackSurface : Surface, _total.ElapsedMilliseconds, _phases.ToList(), _clientSteps.ToList());
        }
    }
}
