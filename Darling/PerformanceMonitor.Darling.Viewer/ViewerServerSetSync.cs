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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Decides whether the viewer's server list needs reloading because a server was added or removed outside
/// this window — another viewer, the web viewer, the MCP add and remove tools — and runs the reload when it
/// does. The fleet refresh tick calls it; nothing else does.
///
/// <para><b>Compared by server id, and only the set.</b> Both sides come from the same managed list read
/// (<c>GetManagedServersAsync</c>), so an id that differs is a real add or remove and never two sources
/// disagreeing, which is what keeps an unchanged registry from reloading on every tick. Order, favorites
/// and the observed facts each row carries are not compared: they are not what this is for, and comparing
/// them would rebuild the sidebar after every collection.</para>
///
/// <para><b>A server removed elsewhere gets what a local remove gives it.</b> The caller hands in the
/// cleanup the context-menu remove runs, and this runs it for each removed server before the reload. A
/// local remove leaves that server's open tab where it is, showing the history already collected, so this
/// leaves it too.</para>
/// </summary>
internal static class ViewerServerSetSync
{
    /// <summary>
    /// Whether the id sets of <paramref name="loaded"/> and <paramref name="registered"/> differ, and the
    /// loaded servers whose ids the registry no longer has.
    /// </summary>
    internal static (bool Changed, IReadOnlyList<DarlingServer> Removed) Compare(
        IReadOnlyList<DarlingServer> loaded,
        IReadOnlyList<DarlingServer> registered)
    {
        ArgumentNullException.ThrowIfNull(loaded);
        ArgumentNullException.ThrowIfNull(registered);

        var loadedIds = new HashSet<int>(loaded.Select(server => server.ServerId));
        var registeredIds = new HashSet<int>(registered.Select(server => server.ServerId));
        var removed = loaded.Where(server => !registeredIds.Contains(server.ServerId)).ToList();

        return (!loadedIds.SetEquals(registeredIds), removed);
    }

    /// <summary>
    /// One pass: when the id sets match, nothing happens and this returns false. When they differ, runs
    /// <paramref name="forgetRemoved"/> for each loaded server the registry no longer has, then
    /// <paramref name="reload"/> once, and returns true.
    /// </summary>
    internal static async Task<bool> ApplyAsync(
        IReadOnlyList<DarlingServer> loaded,
        IReadOnlyList<DarlingServer> registered,
        Action<DarlingServer> forgetRemoved,
        Func<Task> reload)
    {
        ArgumentNullException.ThrowIfNull(forgetRemoved);
        ArgumentNullException.ThrowIfNull(reload);

        var (changed, removed) = Compare(loaded, registered);
        if (!changed)
        {
            return false;
        }

        foreach (var server in removed)
        {
            forgetRemoved(server);
        }

        await reload();

        return true;
    }
}
