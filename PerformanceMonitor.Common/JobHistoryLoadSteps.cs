/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>
/// The sequencing of one Job History load (#5703), shared by the Darling viewer's tab and Lite's so the two keep the same order and
/// the order is tested as behaviour. Pure — no WPF — so a test drives it with tasks it completes by hand.
/// <para><b>The rows bind with no await after the last supersede check.</b> A load that waits on its read, then checks whether a
/// newer load has claimed the tab, must bind in the same synchronous run. Round 1 put an <c>await</c> (the choices read) between the
/// check and the bind, so a load that was current when it checked could be superseded while it waited and then bind its older rows
/// over the newer load's. <see cref="BindWhenCurrentAsync{T}"/> is the one place that check and the bind sit together.</para>
/// <para><b>The grid never waits on the choices.</b> The Server and Category choices that do not depend on the runs come from reads of
/// their own. The grid binds first; <see cref="ApplyWhenCurrentAsync{T}"/> then takes the choices when they arrive, and only a load
/// that is still current applies them (a stale load's choices may be for another window).</para>
/// </summary>
public static class JobHistoryLoadSteps
{
    /// <summary>
    /// Waits for the load's read, then — if no newer load has claimed the tab — runs <paramref name="bind"/> in the same synchronous
    /// run: nothing awaits between the check and the bind. Returns false (and does not bind) for a superseded load.
    /// </summary>
    /// <param name="read">The load's read.</param>
    /// <param name="superseded">True when a newer load has claimed the tab (the tab's <c>_loads.Superseded</c>).</param>
    /// <param name="bind">Puts the rows on the grid. Must not await.</param>
    public static async Task<bool> BindWhenCurrentAsync<T>(Task<T> read, Func<bool> superseded, Action<T> bind)
    {
        ArgumentNullException.ThrowIfNull(read);
        ArgumentNullException.ThrowIfNull(superseded);
        ArgumentNullException.ThrowIfNull(bind);

        var rows = await read;
        if (superseded())
        {
            return false;
        }

        bind(rows);
        return true;
    }

    /// <summary>
    /// Waits for the choices read (null when this load did not start one) and applies it only if no newer load has claimed the tab by
    /// the time it arrives. Call it after the grid has bound and do not await it before the rest of the load. Returns whether
    /// <paramref name="apply"/> ran.
    /// </summary>
    public static async Task<bool> ApplyWhenCurrentAsync<T>(Task<T>? choices, Func<bool> superseded, Action<T> apply)
    {
        ArgumentNullException.ThrowIfNull(superseded);
        ArgumentNullException.ThrowIfNull(apply);

        if (choices is null)
        {
            return false;
        }

        var read = await choices;
        if (superseded())
        {
            return false;
        }

        apply(read);
        return true;
    }
}

/// <summary>
/// Says when a failed choices read is written to the log (#5703): once for a window, not on every refresh. The tab retries a failed
/// read on every load until it succeeds, so a store that is down for an hour would write a line per refresh. A success clears the
/// source, so a failure after it logs again.
/// </summary>
/// <typeparam name="TKey">The window the read was for.</typeparam>
public sealed class ChoicesFailureLogGate<TKey> where TKey : notnull
{
    private readonly Dictionary<string, TKey> _logged = [];

    /// <summary>True the first time <paramref name="source"/> fails for <paramref name="key"/> since it last succeeded.</summary>
    public bool ShouldLog(string source, TKey key)
    {
        lock (_logged)
        {
            if (_logged.TryGetValue(source, out var last) && EqualityComparer<TKey>.Default.Equals(last, key))
            {
                return false;
            }

            _logged[source] = key;
            return true;
        }
    }

    /// <summary>Records that <paramref name="source"/> read successfully, so its next failure is logged.</summary>
    public void Succeeded(string source)
    {
        lock (_logged)
        {
            _logged.Remove(source);
        }
    }
}
