/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// #9232: the first load of a tab or sub-tab, written to the log once with its elapsed milliseconds. Before this a slow first
/// open (30 to 45 s on Performance Trends in the field) left no line, so it could not be read from the log. Only the FIRST
/// completed load of a key is written: the timer's later ticks and a return to the tab are not first opens and stay quiet.
/// A load a newer request abandoned is not a first open, so its replay is the one that gets the line.
/// </summary>
internal sealed class TabFirstLoad
{
    private readonly HashSet<string> _loaded = new(StringComparer.Ordinal);
    private readonly Action<string> _write;
    private readonly TimeProvider _time;

    public TabFirstLoad(Action<string> write, TimeProvider? time = null)
    {
        _write = write;
        _time = time ?? TimeProvider.System;
    }

    /// <summary>
    /// Runs <paramref name="load"/>. The first time <paramref name="key"/> completes it writes
    /// <c>First load of {label}: {ms} ms</c>, and later runs of the same key write nothing. A null key is not tracked.
    /// <paramref name="abandoned"/> is asked once the load ends: true means a newer request superseded it, so it is neither
    /// counted nor logged.
    /// </summary>
    public async Task<T> RunAsync<T>(string? key, string label, Func<Task<T>> load, Func<bool>? abandoned = null)
    {
        if (key is null || _loaded.Contains(key))
        {
            return await load();
        }

        var started = _time.GetTimestamp();
        var result = await load();
        if (abandoned?.Invoke() == true)
        {
            return result;
        }

        if (_loaded.Add(key))
        {
            var ms = (long)_time.GetElapsedTime(started).TotalMilliseconds;
            _write($"First load of {label}: {ms} ms");
        }

        return result;
    }
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
