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
/// does. The fleet refresh tick calls <see cref="ApplyAsync"/>; nothing else does.
///
/// <para><b>Compared by server id, and only the set.</b> The registry side is the config list only
/// (<c>GetConfigManagedServersAsync</c>), which on a seeded store is the list the loaded side came from, so
/// an id that differs is a real add or remove and never two sources disagreeing. That is what keeps an
/// unchanged registry from reloading on every tick. Order, favorites and the observed facts each row
/// carries are not compared: they are not what this is for, and comparing them would rebuild the sidebar
/// after every collection.</para>
///
/// <para><b>No config list, no change.</b> When the store is not seeded, or the seeded check failed this
/// tick (a dead pooled connection after a store restart, say), the read reports null and the pass does
/// nothing. The load falls back to the observed list in that case, but the observed list is a display
/// fallback, not the registry: it lacks every configured server that has never collected, and comparing it
/// would forget each of them.</para>
///
/// <para><b>A server removed elsewhere gets what a local remove gives it.</b> The caller hands in the
/// cleanup the context-menu remove runs, and this runs it for each removed server before the reload. A
/// local remove leaves that server's open tab where it is, showing the history already collected, so this
/// leaves it too.</para>
///
/// <para><b>A rebuild keeps what each picker shows.</b> <see cref="PickerSelectionAfterReload"/> picks the server
/// that the Recommendations and FinOps pickers show after their lists are rebuilt. The reload calls it for both,
/// and the FinOps tab calls it when it re-reads the registry on opening.</para>
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
    /// One pass. A null <paramref name="registered"/> is a read with no config list to report: nothing
    /// happens and this returns false. When the id sets match, nothing happens either. When they differ, runs
    /// <paramref name="forgetRemoved"/> for each loaded server the registry no longer has, then
    /// <paramref name="reload"/> once, and returns true.
    /// </summary>
    internal static async Task<bool> ApplyAsync(
        IReadOnlyList<DarlingServer> loaded,
        IReadOnlyList<DarlingServer>? registered,
        Action<DarlingServer> forgetRemoved,
        Func<Task> reload)
    {
        ArgumentNullException.ThrowIfNull(forgetRemoved);
        ArgumentNullException.ThrowIfNull(reload);

        if (registered is null)
        {
            return false;
        }

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

    /// <summary>
    /// The server a picker shows after the server list is rebuilt: the server it showed before, while that one is
    /// still in <paramref name="servers"/>; otherwise the sidebar's server; otherwise the first server. Null when
    /// the list is empty. A rebuild is not the user choosing a server, so only a picker whose server is gone
    /// moves. The initial load passes no previous server, so its pickers start on the sidebar's server.
    /// </summary>
    internal static DarlingServer? PickerSelectionAfterReload(
        IReadOnlyList<DarlingServer> servers,
        int? previousServerId,
        int? sidebarServerId)
    {
        ArgumentNullException.ThrowIfNull(servers);

        if (servers.Count == 0)
        {
            return null;
        }

        return servers.FirstOrDefault(server => server.ServerId == previousServerId)
            ?? servers.FirstOrDefault(server => server.ServerId == sidebarServerId)
            ?? servers[0];
    }
}
