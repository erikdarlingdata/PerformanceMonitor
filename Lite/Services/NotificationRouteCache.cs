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
/// <para><b>Invalidate only bumps a counter</b> (no I/O), so it is safe to call under the DuckDB write lock the writers
/// hold. The reload runs on the next read of <see cref="Current"/>, outside any lock of ours, and takes the read lock, so it
/// waits for the write that invalidated it. A write that lands while a reload is running leaves the counter ahead of the
/// loaded one, so the next read reloads again.</para>
///
/// <para><b>A failed reload keeps the routes already in force</b> (the same posture as MuteRuleService), logs, and retries
/// no sooner than 30 seconds later, so a broken store costs one failed read per half minute, not one per alert. A store whose
/// tag tables cannot be read is handled inside the loader (scoped routes match nothing).</para>
/// </summary>
public static class NotificationRouteCache
{
    private static readonly object s_gate = new();
    private static readonly TimeSpan s_retryAfterFailure = TimeSpan.FromSeconds(30);

    private static Func<IReadOnlyList<NotificationRoute>>? s_loader;
    private static IReadOnlyList<NotificationRoute> s_routes = Array.Empty<NotificationRoute>();
    private static long s_version;
    private static long s_loadedVersion = -1;
    private static DateTime s_retryAfterUtc = DateTime.MinValue;

    /// <summary>Installs the loader (null removes it, and the cache serves no routes). Resets the held routes.</summary>
    public static void Configure(Func<IReadOnlyList<NotificationRoute>>? loader)
    {
        lock (s_gate)
        {
            s_loader = loader;
            s_routes = Array.Empty<NotificationRoute>();
            s_loadedVersion = -1;
            s_retryAfterUtc = DateTime.MinValue;
            Interlocked.Increment(ref s_version);
        }
    }

    /// <summary>Marks the held routes stale; the next <see cref="Current"/> reloads them. No I/O.</summary>
    public static void Invalidate() => Interlocked.Increment(ref s_version);

    /// <summary>The expanded routes in force, reloading first when something invalidated them.</summary>
    public static IReadOnlyList<NotificationRoute> Current
    {
        get
        {
            var loader = Volatile.Read(ref s_loader);
            if (loader is null)
            {
                return Array.Empty<NotificationRoute>();
            }

            if (Volatile.Read(ref s_loadedVersion) == Volatile.Read(ref s_version))
            {
                return Volatile.Read(ref s_routes);
            }

            lock (s_gate)
            {
                var version = Volatile.Read(ref s_version);
                if (s_loadedVersion == version || s_loader is null)
                {
                    return s_routes;
                }

                if (DateTime.UtcNow < s_retryAfterUtc)
                {
                    return s_routes;
                }

                try
                {
                    s_routes = loader();
                    s_loadedVersion = version;
                    s_retryAfterUtc = DateTime.MinValue;
                }
                catch (Exception ex)
                {
                    AppLogger.Warn("NotificationRoutes", $"Reloading the notification routes failed; the routes already loaded stay in force: {ex.Message}");
                    s_retryAfterUtc = DateTime.UtcNow + s_retryAfterFailure;
                }

                return s_routes;
            }
        }
    }
}
