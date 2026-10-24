/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The expanded notification routes the alert path reads (#5750), loaded lazily and held until something that can change
/// them calls <see cref="Invalidate"/>. <c>AppAlertSettings</c> is a stateless pass-through built in several places, so the
/// cache is static: <see cref="Current"/> is what <c>IAlertSettings.NotificationRoutes</c> returns.
///
/// <para><b>One invalidation method, called from</b>: every write of <see cref="DuckDbNotificationRouteStore"/> (insert,
/// update, set-enabled, delete); every <c>LocalDataService</c> tag writer that changes the tree or the assignments (reparent,
/// delete, assign, unassign, clear-for-server), which is the path ManageTagsWindow and any other caller take; and
/// <c>ServerManager.AddServer</c>, <c>UpdateServer</c>, <c>UpdateServerSettings</c>, <c>DeleteServer</c> and
/// <c>ImportServersFromFile</c> (a rename changes the server's hash id). Not <c>SaveServers</c>, which also runs on
/// last-connected and favorite updates.</para>
///
/// <para><b>Invalidate does no I/O itself</b>: it bumps a counter and starts ONE background reload, so it is safe to call
/// under the DuckDB write lock the writers hold and an alert after an edit rarely waits. The background reload runs outside
/// any lock of ours and takes the DuckDB read lock, so it waits for the write that invalidated it. It is single-flight: an
/// Invalidate while a reload is running starts nothing new, and the running one loads once more after it finishes, so any
/// number of edits during a reload cost one extra load. Invalidate never takes the cache's gate (a reload holds it while it
/// waits for the read lock, and the writer holding the write lock calls Invalidate: taking it there would deadlock). When
/// <see cref="Current"/> still finds the routes stale (the background reload has not finished, or the retry window is open),
/// it reloads on the calling thread as before, joining a reload already in flight through the same gate.</para>
///
/// <para><b>The WPF dispatcher thread never loads and never takes the gate</b> (#5750 review). The 30 second status timer
/// (<c>WebhookAlertService.GetChannelFailureCounts</c> reads the routes) and the synchronous start of an alert send both run
/// on it, and the load takes the DuckDB read lock with no timeout. On that thread <see cref="Current"/> returns the last
/// snapshot, and starts the background reload if one is due. <see cref="Configure"/> primes the first load the same way. An
/// alert must still not read the empty pre-load list, which would send a scoped server's alert to the app-wide destinations:
/// <c>EmailAlertService</c> awaits <see cref="WaitForFirstLoadAsync"/> (bounded, no thread blocked) before the send core
/// reads the routes, and every other thread reloads inline as before.</para>
///
/// <para><b>A failed reload keeps the routes already in force</b> (the same posture as MuteRuleService), logs, and retries
/// no sooner than 30 seconds later, so a broken store costs one failed read per half minute, not one per alert. A store whose
/// tag tables cannot be read is handled inside the loader (tag-scoped routes match nothing; server-scoped ones still expand).</para>
/// </summary>
public static class NotificationRouteCache
{
    private static readonly NotificationRouteCacheCore s_core = new();

    /// <summary>Installs the loader (null removes it, and the cache serves no routes). Resets the held routes.</summary>
    public static void Configure(Func<IReadOnlyList<NotificationRoute>>? loader) => s_core.Configure(loader);

    /// <summary>Runs <paramref name="initializeStore"/> (the one that creates <c>notification_routes</c>), then installs the loader
    /// and primes the first load. See <see cref="NotificationRouteCacheCore.InitializeStoreThenConfigureAsync"/> (#6458).</summary>
    public static Task InitializeStoreThenConfigureAsync(Func<Task> initializeStore, Func<IReadOnlyList<NotificationRoute>>? loader) =>
        s_core.InitializeStoreThenConfigureAsync(initializeStore, loader);

    /// <summary>Marks the held routes stale and starts one background reload (none when one is already running: that one
    /// loads again when it finishes). No I/O on the calling thread and no lock of ours.</summary>
    public static void Invalidate() => s_core.Invalidate();

    /// <summary>The expanded routes in force, reloading first when something invalidated them.</summary>
    public static IReadOnlyList<NotificationRoute> Current => s_core.Current;

    /// <summary>Completes once the first load attempt since <see cref="Configure"/> has finished, or after <paramref name="timeout"/>.
    /// The alert path awaits this before it reads <see cref="Current"/>, so an alert sent right after start does not read
    /// the empty pre-load list and fall through to the app-wide destinations. Never blocks a thread.</summary>
    public static Task WaitForFirstLoadAsync(TimeSpan timeout) => s_core.WaitForFirstLoadAsync(timeout);

    /// <summary>Whether a background reload is running; tests wait for it to finish.</summary>
    internal static bool BackgroundReloadRunning => s_core.BackgroundReloadRunning;
}

/// <summary>
/// The cache's logic as an instance, so a test can exercise the single-flight reload on its own instance: the static
/// <see cref="NotificationRouteCache"/> is invalidated by every server and tag writer in the process, and a test that
/// counts loads on it would count another test's Invalidate.
/// </summary>
internal sealed class NotificationRouteCacheCore
{
    private readonly object _gate = new();
    private readonly TimeSpan _retryAfterFailure = TimeSpan.FromSeconds(30);
    private readonly Func<bool> _isUiThread;
    /* Read without the gate (WaitForFirstLoadAsync): the gate is held across a load, so a read under it would block behind the load. */
    private volatile TaskCompletionSource<bool> _firstLoad = NewFirstLoad();

    private Func<IReadOnlyList<NotificationRoute>>? _loader;
    private IReadOnlyList<NotificationRoute> _routes = Array.Empty<NotificationRoute>();
    private long _version;
    private long _loadedVersion = -1;
    private long _retryAfterTicks;
    private int _backgroundRunning;

    /// <param name="isUiThread">Whether the calling thread is one that must never wait on a load (the WPF dispatcher thread).
    /// Null: the application dispatcher, and false when there is none (a test, a console host).</param>
    public NotificationRouteCacheCore(Func<bool>? isUiThread = null)
    {
        _isUiThread = isUiThread ?? OnApplicationDispatcher;
    }

    private static bool OnApplicationDispatcher() =>
        System.Windows.Application.Current?.Dispatcher?.CheckAccess() == true;

    private static TaskCompletionSource<bool> NewFirstLoad() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Installs the loader (null removes it, and the cache serves no routes). Resets the held routes.</summary>
    public void Configure(Func<IReadOnlyList<NotificationRoute>>? loader)
    {
        lock (_gate)
        {
            _loader = loader;
            _routes = Array.Empty<NotificationRoute>();
            _loadedVersion = -1;
            Volatile.Write(ref _retryAfterTicks, 0);
            Interlocked.Increment(ref _version);
            _firstLoad = NewFirstLoad();
        }

        /* Prime the first load now, so the first status tick and the first alert find it done or in flight. */
        StartBackgroundReload();
    }

    /// <summary>Creates the store's tables, and only then installs the loader. <see cref="Configure"/> primes the first load at once, so
    /// calling it before the store exists made a first start on an empty data root read a <c>notification_routes</c> table that was
    /// not there yet, and log a WARN that read like a fault (#6458). Until this returns no loader is installed: the cache serves no
    /// routes and nothing waits on a first load. A table that is missing AFTER this returns is a real fault, and the loader's own
    /// failure path still logs it.</summary>
    public async Task InitializeStoreThenConfigureAsync(Func<Task> initializeStore, Func<IReadOnlyList<NotificationRoute>>? loader)
    {
        try
        {
            await initializeStore().ConfigureAwait(false);
        }
        finally
        {
            /* Installed even when the store failed to initialize: that is a real fault, and the loader's failure path logs it. */
            Configure(loader);
        }
    }

    /// <summary>Completes once the first load attempt since <see cref="Configure"/> has finished, or after the timeout; at once when no
    /// loader is installed or the routes are already loaded. Starts the background load if it is not running. No thread blocks.</summary>
    public async Task WaitForFirstLoadAsync(TimeSpan timeout)
    {
        if (Volatile.Read(ref _loader) is null)
        {
            return;
        }

        /* Not under _gate: a load holds the gate for as long as the loader takes (it can wait on the DuckDB write lock with no
           timeout), and this method promises to hold no thread. Reading the field under the gate blocked the caller behind
           the very load it was about to wait for. Configure (called once, at start) swaps the field; a caller still holding the old one waits out its bound at most. */
        var first = _firstLoad;

        if (first.Task.IsCompleted)
        {
            return;
        }

        StartBackgroundReload();
        if (await Task.WhenAny(first.Task, Task.Delay(timeout)).ConfigureAwait(false) != first.Task)
        {
            AppLogger.Warn("NotificationRoutes", $"The notification routes had not finished their first load after {timeout.TotalSeconds:0} seconds; this alert reads the routes loaded so far.");
        }
    }

    /// <summary>Marks the held routes stale and starts one background reload (none when one is already running: that one
    /// loads again when it finishes). No I/O on the calling thread and no lock of ours.</summary>
    public void Invalidate()
    {
        Interlocked.Increment(ref _version);
        StartBackgroundReload();
    }

    private void StartBackgroundReload()
    {
        if (Volatile.Read(ref _loader) is null || Interlocked.CompareExchange(ref _backgroundRunning, 1, 0) != 0)
        {
            return;
        }

        _ = Task.Run(BackgroundReloadLoop);
    }

    private void BackgroundReloadLoop()
    {
        do
        {
            /* Current reloads when stale and logs a failed load itself, so nothing here can throw past a loader's catch. The flag
               stays set while an Invalidate during a load still needs one more: clearing it between loads let a reader see the
               reload as finished with that load still to come. */
            do
            {
                _ = Current;
            }
            while (MoreToLoad());

            /* A full fence, not a release store: the look below must not be read before the clear is seen, or an Invalidate in
               between would start nothing and this loop would stop with its change unloaded. */
            Interlocked.Exchange(ref _backgroundRunning, 0);

            /* Look again after clearing the flag: an Invalidate that landed between the last look and the clear saw the flag set
               and started nothing, so this loop goes on (or that Invalidate won the race and started its own loop). */
        }
        while (MoreToLoad() && Interlocked.CompareExchange(ref _backgroundRunning, 1, 0) == 0);
    }

    /* Stale with a loader set, and not inside the 30 second retry window, or a broken store would spin. */
    private bool MoreToLoad() =>
        Volatile.Read(ref _loader) is not null
        && Volatile.Read(ref _loadedVersion) != Volatile.Read(ref _version)
        && DateTime.UtcNow.Ticks >= Volatile.Read(ref _retryAfterTicks);

    /// <summary>Whether a background reload is running; tests wait for it to finish.</summary>
    internal bool BackgroundReloadRunning => Volatile.Read(ref _backgroundRunning) != 0;

    /// <summary>The expanded routes in force, reloading first when something invalidated them.</summary>
    public IReadOnlyList<NotificationRoute> Current
    {
        get
        {
            var loader = Volatile.Read(ref _loader);
            if (loader is null)
            {
                return Array.Empty<NotificationRoute>();
            }

            if (Volatile.Read(ref _loadedVersion) == Volatile.Read(ref _version))
            {
                return Volatile.Read(ref _routes);
            }

            /* The dispatcher thread reads what is held and never loads or takes the gate: a load can wait on the DuckDB write
               lock with no timeout. The background reload brings the routes up to date. */
            if (_isUiThread())
            {
                StartBackgroundReload();
                return Volatile.Read(ref _routes);
            }

            lock (_gate)
            {
                var version = Volatile.Read(ref _version);
                if (_loadedVersion == version || _loader is null)
                {
                    return _routes;
                }

                if (DateTime.UtcNow.Ticks < Volatile.Read(ref _retryAfterTicks))
                {
                    return _routes;
                }

                try
                {
                    _routes = loader();
                    _loadedVersion = version;
                    Volatile.Write(ref _retryAfterTicks, 0);
                    _firstLoad.TrySetResult(true);
                }
                catch (Exception ex)
                {
                    AppLogger.Warn("NotificationRoutes", $"Reloading the notification routes failed; the routes already loaded stay in force: {ex.Message}");
                    Volatile.Write(ref _retryAfterTicks, (DateTime.UtcNow + _retryAfterFailure).Ticks);

                    /* A failed first attempt still ends the wait: the 30 second retry window, not each alert, paces the retries. */
                    _firstLoad.TrySetResult(true);
                }

                return _routes;
            }
        }
    }
}
