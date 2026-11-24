/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// #9232: the first load of a tab or sub-tab, written to the log once with its elapsed milliseconds. Before this a slow first
/// open (30 to 45 s on Performance Trends in the field) left no line, so it could not be read from the log. Only the FIRST
/// completed load of a key is written: the timer's later ticks and a return to the tab are not first opens and stay quiet.
/// A load a newer request abandoned is not a first open, so its replay is the one that gets the line.
///
/// <para>#9366: the line's total runs from the PICK (the moment the selection changed, see <see cref="Pick"/>) to the first
/// drawn row, and splits that into the wait before the refresh started, the steps before the read, the read with its read lock
/// wait, and the draw. The line used to time only the read, so a screen that took 21.9 s showed a line that said 3.3 s and the
/// rest could not be placed.</para>
/// </summary>
internal sealed class TabFirstLoad
{
    private static readonly AsyncLocal<FirstLoadMarks?> s_current = new();

    private readonly HashSet<string> _loaded = new(StringComparer.Ordinal);
    private readonly Dictionary<string, long> _picked = new(StringComparer.Ordinal);
    private readonly Action<string> _write;
    private readonly TimeProvider _time;
    private readonly Func<Task> _afterRender;

    /// <param name="write">Receives the line.</param>
    /// <param name="time">The clock. A test moves it by hand.</param>
    /// <param name="afterRender">Completes once the UI has had its next render pass, so rows handed to a grid are on screen.
    /// The server tab passes a Dispatcher pass at Loaded priority, which runs after the pending layout and render.</param>
    public TabFirstLoad(Action<string> write, TimeProvider? time = null, Func<Task>? afterRender = null)
    {
        _write = write;
        _time = time ?? TimeProvider.System;
        _afterRender = afterRender ?? (() => Task.CompletedTask);
    }

    /// <summary>The clock's now, for the caller to hand back as <c>passStartedAt</c> when its refresh pass starts.</summary>
    public long Timestamp() => _time.GetTimestamp();

    /// <summary>
    /// The user picked <paramref name="key"/> (the tab or sub-tab selection changed, or the server tab opened on it). A newer
    /// pick of the same key replaces the stamp, the way an abandoned load is replaced by its replay. A key that already had its
    /// first load is not tracked again.
    /// </summary>
    public void Pick(string? key)
    {
        if (key is null || _loaded.Contains(key))
        {
            return;
        }

        _picked[key] = _time.GetTimestamp();
    }

    /// <summary>
    /// Called by a tab's load the moment its rows are handed to the grid, so the read ends there and the draw starts there,
    /// rather than at the end of the load, which can go on to read a comparison and a banner. A no-op outside a first load.
    /// A load that never calls it hands its rows at its end.
    /// </summary>
    public static void RowsHanded() => s_current.Value?.RowsHanded();

    /// <summary>
    /// Runs <paramref name="load"/>. The first time <paramref name="key"/> completes it writes
    /// <c>First load of {label}: {ms} ms (queued .., before read .., read .., draw ..)</c>, and later runs of the same key write
    /// nothing. A null key is not tracked. <paramref name="abandoned"/> is asked once the load ends: true means a newer request
    /// superseded it, so it is neither counted nor logged. <paramref name="passStartedAt"/> is <see cref="Timestamp"/> from when
    /// the refresh pass started, so the steps before this call are told apart from the wait before the pass.
    /// </summary>
    public async Task<T> RunAsync<T>(string? key, string label, Func<Task<T>> load, Func<bool>? abandoned = null, long? passStartedAt = null)
    {
        if (key is null || _loaded.Contains(key))
        {
            return await load();
        }

        var readStarted = _time.GetTimestamp();
        long? picked = _picked.TryGetValue(key, out var stamp) ? stamp : null;
        FirstLoadMarks marks;
        T result;
        using (var tally = ReadLockWaitTally.Begin())
        {
            marks = new FirstLoadMarks(_time, _afterRender, tally, readStarted);
            s_current.Value = marks;
            try
            {
                result = await load();
            }
            finally
            {
                s_current.Value = null;
            }

            marks.RowsHanded();
        }

        if (abandoned?.Invoke() == true)
        {
            return result;
        }

        await marks.Drawn;
        if (_loaded.Add(key))
        {
            _picked.Remove(key);
            _write(marks.Describe(label, picked, passStartedAt ?? readStarted));
        }

        return result;
    }
}

/// <summary>#9366: the stamps of one first load, from the read starting to the first row drawn.</summary>
internal sealed class FirstLoadMarks
{
    private readonly TimeProvider _time;
    private readonly Func<Task> _afterRender;
    private readonly ReadLockWaitTally _tally;
    private readonly long _readStarted;
    private long _handed;
    private long _drawn;
    private TimeSpan _lockWait;
    private int _connections;
    private Task? _drawnTask;

    internal FirstLoadMarks(TimeProvider time, Func<Task> afterRender, ReadLockWaitTally tally, long readStarted)
    {
        _time = time;
        _afterRender = afterRender;
        _tally = tally;
        _readStarted = readStarted;
    }

    /// <summary>Completes after the render pass that follows the rows being handed to the grid.</summary>
    internal Task Drawn => _drawnTask ?? Task.CompletedTask;

    internal void RowsHanded()
    {
        if (_drawnTask is not null)
        {
            return;
        }

        _handed = _time.GetTimestamp();
        _lockWait = _tally.LockWait;
        _connections = _tally.Connections;
        _drawnTask = ObserveDrawnAsync();
    }

    private async Task ObserveDrawnAsync()
    {
        await _afterRender();
        _drawn = _time.GetTimestamp();
    }

    internal string Describe(string label, long? picked, long passStarted)
    {
        /* A pick that came after the pass started (the user moved on while the steps ran) leaves nothing queued. */
        var start = picked ?? passStarted;
        var beforePass = Math.Max(start, passStarted);
        var queued = picked is null ? "" : $"queued {Math.Max(0, Ms(start, passStarted))} ms, ";
        return string.Create(CultureInfo.InvariantCulture,
            $"First load of {label}: {Ms(start, _drawn)} ms ({queued}before read {Ms(beforePass, _readStarted)} ms, read {Ms(_readStarted, _handed)} ms, read lock wait {(long)_lockWait.TotalMilliseconds} ms over {_connections} connections, draw {Ms(_handed, _drawn)} ms)");
    }

    private long Ms(long from, long to) => (long)_time.GetElapsedTime(from, to).TotalMilliseconds;
}

/// <summary>
/// #9232: draws each panel of a tab as ITS OWN reads land. The tab used to wait for every read of the sub-tab and then draw
/// them all in a row, so the slowest read held back the chart that was ready first.
/// </summary>
internal static class LandedPanels
{
    /// <summary>Awaits <paramref name="read"/>, then runs <paramref name="draw"/> with its result. Nothing else is awaited.</summary>
    public static async Task DrawAsync<T>(Task<T> read, Action<T> draw) => draw(await read);

    /// <summary>The same for a panel that needs two reads (a chart and the discontinuity markers it is marked with).</summary>
    public static async Task DrawAsync<T1, T2>(Task<T1> read, Task<T2> alsoRead, Action<T1, T2> draw)
    {
        var first = await read;
        draw(first, await alsoRead);
    }
}
