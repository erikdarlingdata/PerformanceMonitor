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

/// <summary>
/// What identifies the window and server the Job History choices were read for (#5703). The choices read is an uncapped DISTINCT
/// over the fleet, so the tab reads it once per window and again only after a lifetime, not on every auto-refresh. The key must
/// therefore stay the same across refreshes of one window and change when the window or the server changes:
/// <list type="bullet">
/// <item>A <b>relative</b> preset ("Past 24 hours") slides its start with now, so its identity is its length.</item>
/// <item>A window with a <b>fixed start</b> (a calendar period such as Today or Month to Date, or "since X") is identified by that start.
/// Its end is part of the key only when the window is not live: a live window's end is now, and keying on it (or on the span, which
/// grows with it) would make the key new on every load and read the choices every time.</item>
/// <item>The <b>server</b> picked in the Server drop-down is part of the key: the Category read is that server's.</item>
/// </list>
/// </summary>
/// <param name="StartUtc">The window's start, for a window whose start is fixed; null for a relative preset.</param>
/// <param name="Span">The window's length, for a relative preset; null otherwise.</param>
/// <param name="FixedEndUtc">The window's end, for a window that has ended; null for a live window.</param>
/// <param name="ServerId">The picked server, or null at All Servers.</param>
public readonly record struct JobHistoryChoicesKey(DateTime? StartUtc, TimeSpan? Span, DateTime? FixedEndUtc, int? ServerId)
{
    /// <summary>The key for one load's window.</summary>
    /// <param name="startSlides">True when the start moves with now (a relative preset), false when it is fixed.</param>
    /// <param name="startUtc">The window's start for this load.</param>
    /// <param name="endUtc">The window's end for this load (now, for a live window).</param>
    /// <param name="endIsLive">True when the window's end is now.</param>
    /// <param name="serverId">The picked server, or null at All Servers.</param>
    public static JobHistoryChoicesKey For(bool startSlides, DateTime startUtc, DateTime endUtc, bool endIsLive, int? serverId) =>
        startSlides
            ? new JobHistoryChoicesKey(null, endUtc - startUtc, null, serverId)
            : new JobHistoryChoicesKey(startUtc, null, endIsLive ? null : endUtc, serverId);
}

/// <summary>
/// When the Job History choices are read again (#5703): when the window or server changed (a different
/// <see cref="JobHistoryChoicesKey"/>) or after <c>lifetime</c> since the last read that fully succeeded. The clock is passed in, so
/// a test drives several refreshes without waiting.
/// </summary>
public sealed class JobHistoryChoicesFreshness
{
    private readonly TimeSpan _lifetime;
    private JobHistoryChoicesKey? _key;
    private DateTime _readAtUtc = DateTime.MinValue;

    /// <param name="lifetime">How long a read stays current for an unchanged key.</param>
    public JobHistoryChoicesFreshness(TimeSpan lifetime) => _lifetime = lifetime;

    /// <summary>True when this load should start the choices reads.</summary>
    public bool NeedsRead(JobHistoryChoicesKey key, DateTime nowUtc) =>
        _key != key || nowUtc - _readAtUtc > _lifetime;

    /// <summary>Records that every half of the choices was read for <paramref name="key"/> at <paramref name="nowUtc"/>.</summary>
    public void Record(JobHistoryChoicesKey key, DateTime nowUtc)
    {
        _key = key;
        _readAtUtc = nowUtc;
    }
}

/// <summary>
/// The Job History tab's category list, remembered per window and server for the session, with one read in flight per key (#8549).
/// <para><b>Why a cache of results, not only a "read again after" clock.</b> <see cref="JobHistoryChoicesFreshness"/> remembers one
/// key and is told a read succeeded only by a load that is still current when the read arrives. The category read is the slowest
/// statement on a large store (several seconds, more on a first read), so a load that was superseded while it ran (a Status pick, a
/// Refresh click, a range change) threw the answer away and left the next load to start the same read again; so did going back to a
/// window already read. A field store logged five runs of it in an hour of light use. This cache keeps the answer for every key it has
/// seen and hands every caller of a key that is still running the same task, so the store runs the read once per key per
/// <c>lifetime</c>, whoever asks and however many loads come and go.</para>
/// <para><b>What a cached list can miss.</b> A category that appears after the read is added to the drop-down anyway: the drop-down
/// merges the categories of the runs the grid just read (<see cref="JobHistoryFilterChoices.MergeCategories"/>), and a new run is among the newest.
/// What a cached list can still hold is a category whose last run has since left the window, which costs an empty grid for that pick,
/// as the hour of slack in the read already can.</para>
/// <para>A read that failed is never kept: the next caller reads again.</para>
/// </summary>
public sealed class JobHistoryCategoryCache
{
    private sealed class Entry(Task<List<string>> task, DateTime startedUtc)
    {
        public Task<List<string>> Task { get; } = task;

        public DateTime StartedUtc { get; } = startedUtc;
    }

    private readonly TimeSpan _lifetime;
    private readonly int _capacity;
    private readonly object _gate = new();
    private readonly Dictionary<JobHistoryChoicesKey, Entry> _entries = [];

    /// <param name="lifetime">How long a finished read answers for its key.</param>
    /// <param name="capacity">How many keys are remembered; the oldest finished read goes first.</param>
    public JobHistoryCategoryCache(TimeSpan lifetime, int capacity = 16)
    {
        _lifetime = lifetime;
        _capacity = capacity < 1 ? 1 : capacity;
    }

    /// <summary>How many reads <see cref="GetAsync"/> has started (not counting the callers it answered from memory).</summary>
    public int ReadsStarted { get; private set; }

    /// <summary>
    /// The categories for <paramref name="key"/>: the finished answer if it is younger than the lifetime, the read already running for
    /// the key if there is one, else a new read through <paramref name="read"/>. The caller must not change the returned list.
    /// A read that throws faults the returned task, for every caller that was waiting on it, and is forgotten.
    /// </summary>
    public Task<List<string>> GetAsync(JobHistoryChoicesKey key, DateTime nowUtc, Func<Task<List<string>>> read)
    {
        ArgumentNullException.ThrowIfNull(read);

        Entry entry;
        TaskCompletionSource<List<string>> source;
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var existing))
            {
                if (!existing.Task.IsCompleted
                    || (existing.Task.IsCompletedSuccessfully && nowUtc - existing.StartedUtc <= _lifetime))
                {
                    return existing.Task;
                }

                _entries.Remove(key);
            }

            source = new TaskCompletionSource<List<string>>(TaskCreationOptions.RunContinuationsAsynchronously);
            entry = new Entry(source.Task, nowUtc);
            _entries[key] = entry;
            ReadsStarted++;
            Trim(key);
        }

        _ = RunAsync(key, entry, source, read);
        return source.Task;
    }

    private async Task RunAsync(JobHistoryChoicesKey key, Entry entry, TaskCompletionSource<List<string>> source, Func<Task<List<string>>> read)
    {
        try
        {
            source.SetResult(await read().ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            lock (_gate)
            {
                if (_entries.TryGetValue(key, out var current) && ReferenceEquals(current, entry))
                {
                    _entries.Remove(key);
                }
            }

            source.SetException(ex);
        }
    }

    /// <summary>Drops the oldest finished reads past the capacity, never the one just started and never one still running.</summary>
    private void Trim(JobHistoryChoicesKey keep)
    {
        while (_entries.Count > _capacity)
        {
            JobHistoryChoicesKey? oldest = null;
            var oldestAt = DateTime.MaxValue;
            foreach (var (key, entry) in _entries)
            {
                if (key.Equals(keep) || !entry.Task.IsCompleted || entry.StartedUtc >= oldestAt)
                {
                    continue;
                }

                oldest = key;
                oldestAt = entry.StartedUtc;
            }

            if (oldest is null)
            {
                return;
            }

            _entries.Remove(oldest.Value);
        }
    }
}
