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
using System.Windows.Threading;

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
    /// <summary>#9426: a load whose total (store reads plus the client's render) reaches this is logged. Two seconds is where a panel stops
    /// feeling instant to the reader, so a timing check reads the log instead of a click-through with a stopwatch.</summary>
    internal const int SlowLoadThresholdMs = 2000;

    private readonly Stopwatch _total = Stopwatch.StartNew();
    private readonly List<(string Phase, long StartedMs, long EndedMs)> _phases = new();
    private readonly List<(string Step, long Ms)> _clientSteps = new();
    /* #8549: reads handed to Track that have not ended (the key is a running number), and reads that ended cancelled. A load that is
       finished before such a read's watcher has run must still name it: with only the ended reads booked, the stretch the read ran
       was left to "client work".
       #9249: the entry keeps the read's task. The loader awaits the same task on the UI thread and then calls Finish, and nothing orders
       that continuation against the watcher's, so Finish can run while a read that has already completed is still here. Finish reads the
       task's state to tell that read from one still running. */
    private readonly Dictionary<int, (string Phase, long StartedMs, Task Task)> _inFlight = new();
    private readonly List<(string Phase, long StartedMs, long EndedMs)> _cancelled = new();
    /* #9365: every read handed to Track, kept for the load's life, so a failed load can name the read whose task carries the exception. */
    private readonly List<(string Phase, long StartedMs, Task Task, Func<long?> Rows)> _tracked = new();
    private readonly Dictionary<string, string> _phaseNotes = new();
    private int _nextReadId;
    private bool _superseded;
    private readonly object _gate = new();
    private readonly int _slowThresholdMs;
    private readonly long _alreadyElapsedMs;

    public ViewerLoadTimer() : this(SlowLoadThresholdMs)
    {
    }

    /// <summary>A clock with its own slow-load threshold, so a test reads the line a load would log without waiting three seconds (#8549).</summary>
    internal ViewerLoadTimer(int slowThresholdMs) => _slowThresholdMs = slowThresholdMs;

    /// <summary>#8549: a clock that starts <paramref name="alreadyElapsedMs"/> in, for a load whose first stretch (a tab's construction)
    /// ran before the clock could exist. The line's total and every phase's time count from the load's true start.</summary>
    internal ViewerLoadTimer(int slowThresholdMs, long alreadyElapsedMs) : this(slowThresholdMs) => _alreadyElapsedMs = alreadyElapsedMs;

    /// <summary>#9249: test-only. When set, a watcher waits on it after its read ends and before it books the read, so a test holds the
    /// watcher back and runs <see cref="Finish"/> in the gap the loader's own continuation can hit. Null in the product.</summary>
    internal Task? BookingGate { get; set; }

    /// <summary>Milliseconds on the load's clock, counting any stretch the load ran before this clock started.</summary>
    internal long ElapsedMs => _alreadyElapsedMs + _total.ElapsedMilliseconds;

    /// <summary>#8549: text put before the surface in the slow-load line, so a tab's first open reads "server tab first open > Overview"
    /// whichever sub-tab loader then names its own surface.</summary>
    internal string Lead { get; set; } = "";

    /// <summary>#9426: the time range the load asked for, as the text its slow-load line shows. Empty for a load with no range of its own.</summary>
    internal string Window { get; set; } = "";

    /// <summary>What the load is: set by a loader that knows its sub-tab ("Queries > Top Queries by Duration").</summary>
    internal string Surface { get; set; } = "";

    /// <summary>Times <paramref name="task"/> (already started) as <paramref name="phase"/> and hands it back unchanged, so a
    /// caller awaits it exactly as before. A faulted or cancelled task is still recorded; its own awaiter sees the error. #9249: the
    /// task handed back is the caller's own, not a wrapper, so the watcher and the caller's await are two unordered continuations. <see cref="Finish"/> covers
    /// the gap by treating a read whose task has ended as booked.</summary>
    internal Task<T> Track<T>(string phase, Task<T> task)
    {
        /* #9426: the rows the read returned, counted once it has finished. Only a result that is a collection has a count. */
        _ = Watch(phase, StartRead(phase, task, () => task.IsCompletedSuccessfully && task.Result is System.Collections.ICollection rows ? rows.Count : null), task);
        return task;
    }

    /// <summary><see cref="Track{T}"/> for a read whose result wraps its rows (a record that carries a list beside other facts), so
    /// <paramref name="rowsOf"/> says how many rows it holds and the slow-load line counts them (#9887).</summary>
    internal Task<T> Track<T>(string phase, Task<T> task, Func<T, long?> rowsOf)
    {
        _ = Watch(phase, StartRead(phase, task, () => task.IsCompletedSuccessfully ? rowsOf(task.Result) : null), task);
        return task;
    }

    /// <summary><see cref="Track{T}"/> for a task with no result.</summary>
    internal Task Track(string phase, Task task)
    {
        _ = Watch(phase, StartRead(phase, task, static () => null), task);
        return task;
    }

    /// <summary>#9287: says what a read read, shown after the read's time in the slow-load line (a Query Store read names the days it took
    /// from the rollups and the days it read from the interval table). The latest note for a phase name wins.</summary>
    internal void NotePhase(string phase, string note)
    {
        lock (_gate)
        {
            _phaseNotes[phase] = note;
        }
    }

    /// <summary>#9365: the tracked read whose task failed with <paramref name="failure"/>, and the milliseconds from its start to now (the caller
    /// reports at once, so that is the read's own time). Null when no tracked read carries that exception, for a failure the loader raised
    /// itself or wrapped.</summary>
    internal (string Phase, long Ms)? FailedRead(Exception failure)
    {
        lock (_gate)
        {
            foreach (var (phase, startedMs, task, _) in _tracked)
            {
                if (task.IsFaulted && task.Exception!.InnerExceptions.Contains(failure))
                {
                    return (phase, Math.Max(0, ElapsedMs - startedMs));
                }
            }
        }

        return null;
    }

    /// <summary>#8549: the load was stopped by a newer one (a tab that cancels its superseded load calls this from its cancel path). A read
    /// still in flight, or cancelled by that newer load, then reads "stopped by a newer pick" in the slow-load line. Without this call a read
    /// still out when the load finishes stays off the line, as before.</summary>
    internal void MarkSuperseded()
    {
        lock (_gate)
        {
            _superseded = true;
        }
    }

    private (int Id, long StartedMs) StartRead(string phase, Task task, Func<long?> rows)
    {
        var started = ElapsedMs;
        lock (_gate)
        {
            var id = ++_nextReadId;
            _inFlight[id] = (phase, started, task);
            _tracked.Add((phase, started, task, rows));
            return (id, started);
        }
    }

    /// <summary>#8549: times a stretch of the client's own work (a grid build, a chart draw, a converter) as <paramref name="step"/> and
    /// runs it, so a slow load's line names the step that kept the UI thread busy instead of one lump of "client work". Steps run one
    /// after another on the UI thread, so they are summed per name and listed after the client share. A step that throws is still booked.</summary>
    internal void Step(string step, Action work)
    {
        var started = ElapsedMs;
        try
        {
            work();
        }
        finally
        {
            RecordStep(step, ElapsedMs - started);
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

    /// <summary>#8549: waits for the dispatcher to finish the layout and render pass the load's binds queued, and books the wait as the step
    /// "layout and render". A grid's rows are measured and drawn after the load's own code has returned, at a lower priority than the code that set
    /// the items, so a load's clock stopped before that pass and the first open of a tab with a big grid wrote no line for the seconds it took. It
    /// waits at <see cref="DispatcherPriority.Background"/>, behind the render pass and ahead of idle work. Off the UI thread, or once the
    /// dispatcher is shutting down, there is no pass to wait for and it returns at once.</summary>
    internal async Task SettleLayoutAsync(Dispatcher dispatcher)
    {
        if (!dispatcher.CheckAccess() || dispatcher.HasShutdownStarted)
        {
            return;
        }

        var started = ElapsedMs;
        try
        {
            await dispatcher.InvokeAsync(static () => { }, DispatcherPriority.Background);
        }
        finally
        {
            RecordStep("layout and render", ElapsedMs - started);
        }
    }

    /// <summary>#8549: books a stretch of client work that ran before this clock could time it (a tab's construction) as <paramref name="step"/>.</summary>
    internal void BookStep(string step, long ms) => RecordStep(step, ms);

    /// <summary>#8549: books a wait that has no task to hand to <see cref="Track{T}"/> (a refresh queued behind another one) as a phase
    /// from <paramref name="startedMs"/> to now on the load's clock.</summary>
    internal void BookPhaseEndingNow(string phase, long startedMs) => Record(phase, startedMs);

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
    private async Task Watch(string phase, (int Id, long StartedMs) read, Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception)
        {
            /* The awaiter of the task sees the failure; this watcher only needs to know when it ended. */
        }

        if (BookingGate is { } gate)
        {
            await gate.ConfigureAwait(false);
        }

        var ended = ElapsedMs;
        lock (_gate)
        {
            /* #9249: booking and leaving _inFlight happen together under the gate, so Finish sees a read in one list or the other, never
               both. Finish only copies the lists, so a line it already built cannot gain this read afterwards. */
            _inFlight.Remove(read.Id);
            (task.IsCanceled ? _cancelled : _phases).Add((phase, read.StartedMs, ended));
        }
    }

    private void Record(string phase, long startedMs)
    {
        var ended = ElapsedMs;
        lock (_gate)
        {
            _phases.Add((phase, startedMs, ended));
        }
    }

    /// <summary>The log line for a load of <paramref name="totalMs"/>, or null when it was not slow. Pure, so a test reads the text.</summary>
    internal static string? Describe(string surface, long totalMs, IReadOnlyList<(string Phase, long StartedMs, long EndedMs)> phases,
        IReadOnlyList<(string Step, long Ms)>? clientSteps = null, int slowThresholdMs = SlowLoadThresholdMs,
        IReadOnlyList<(string Phase, long StartedMs, long EndedMs, string Marker)>? stoppedReads = null,
        IReadOnlyDictionary<string, string>? phaseNotes = null, string? window = null, long? rows = null)
    {
        if (totalMs < slowThresholdMs)
        {
            return null;
        }

        /* #8549: a read the load did not see through (still running when the load was finished, or cancelled) is listed with its time and
           a marker, and counts as ended where it stopped: the client's share starts after the last read that ended or was stopped. */
        var reads = phases.Select(p => (p.Phase, p.StartedMs, p.EndedMs, Marker: ""))
            .Concat(stoppedReads ?? Array.Empty<(string Phase, long StartedMs, long EndedMs, string Marker)>())
            .OrderBy(p => p.EndedMs).ToList();
        var lastReadEnded = reads.Count == 0 ? 0 : reads.Max(p => p.EndedMs);
        var clientMs = Math.Max(0, totalMs - lastReadEnded);
        var split = reads.Count == 0
            ? "no store phase timed"
            : string.Join(", ", reads.Select(p => $"{p.Phase} {p.EndedMs - p.StartedMs} ms{(p.Marker.Length == 0 ? "" : $" ({p.Marker})")}"
                + (phaseNotes is not null && phaseNotes.TryGetValue(p.Phase, out var note) ? $" ({note})" : "")));
        /* #9426: the load in two parts. The store share runs from the load's start to the last read in hand, the render share is what is left
           (shaping, binding and the layout pass the load waited for). Rows and the window come after, when the load knows them. */
        var summary = $"store {lastReadEnded} ms, render {clientMs} ms"
            + (rows is { } n ? $", {n} rows" : "")
            + (string.IsNullOrEmpty(window) ? "" : $", window {window}");
        var line = $"slow load: {surface} took {totalMs} ms ({summary}): store reads [{split}], then {clientMs} ms of client work after the last read";
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
    /// the load was slow. For a fleet tab, which has no inner-tab shell to do it. <paramref name="surface"/> names the tab. A <paramref name="sink"/> takes the line in place of the log (a test reads it).</summary>
    internal void FinishAndLog(string surface, Action<string>? sink = null)
    {
        try
        {
            if (Finish(surface) is { } slow)
            {
                (sink ?? (line => ViewerLogger.Warn("SlowLoad", line)))(slow);
            }
        }
        catch (Exception ex)
        {
            /* #9426: the line is a diagnostic. A sink that fails (a log file that cannot be written) must not fail the load it describes. */
            Debug.WriteLine($"slow-load line not written: {ex.GetType().Name}");
        }
    }

    /// <summary>Stops the clock and returns the slow-load line, or null when the load was quick.</summary>
    internal string? Finish(string fallbackSurface)
    {
        _total.Stop();
        lock (_gate)
        {
            /* A load nothing stopped lists only the reads that ended (see below for the ones not yet booked): a side read still out when the load finished is not part of its time.
               A load a newer one stopped lists the reads that were in flight or cancelled too, marked, and counts them as ended where
               they stopped, so their time is not booked as client work. */
            var now = ElapsedMs;
            var phases = _phases.ToList();
            List<(string Phase, long StartedMs, long EndedMs, string Marker)>? stopped = null;
            if (_superseded)
            {
                const string marker = "stopped by a newer pick";
                stopped = _cancelled.Select(c => (c.Phase, c.StartedMs, c.EndedMs, Marker: marker))
                    .Concat(_inFlight.Values.Select(f => (f.Phase, f.StartedMs, EndedMs: now, Marker: marker)))
                    .ToList();
            }
            else
            {
                phases.AddRange(_cancelled);

                /* #9249: a read whose task has ended but whose watcher has not run yet is part of the load's time. The loader's own await of
                   the task can resume first and call Finish, and without this the read's whole run was booked as client work. It is booked as
                   ended now (the watcher's own timestamp is not in yet). A read still running stays off the line, as above. */
                phases.AddRange(_inFlight.Values.Where(f => f.Task.IsCompleted).Select(f => (f.Phase, f.StartedMs, now)));
            }

            long? rows = null;
            foreach (var read in _tracked)
            {
                if (read.Rows() is { } count)
                {
                    rows = (rows ?? 0) + count;
                }
            }

            return Describe(Lead + (string.IsNullOrEmpty(Surface) ? fallbackSurface : Surface), now, phases, _clientSteps.ToList(), _slowThresholdMs, stopped, new Dictionary<string, string>(_phaseNotes), Window, rows);
        }
    }
}
