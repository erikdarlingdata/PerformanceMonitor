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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Which server the FinOps tab shows, how its picker names it, and whether its reads apply to it. The web FinOps page
/// opens on the first SQL Server target, labels a PostgreSQL target "(PostgreSQL)" in its server list, and says "Not
/// collected for PostgreSQL" on a panel a PostgreSQL target cannot answer (<c>finops/gate.js</c>); this is the same
/// rule for the Viewer, kept out of the window so it can be tested without one.
///
/// <para>Almost every FinOps panel reads SQL Server data (query stats, wait stats, database files, memory grants).
/// Run for a PostgreSQL target, each of those reads came back empty and the tab showed "No Data" or an empty grid with
/// nothing saying why. Server Inventory is the exception: it lists the whole fleet, PostgreSQL targets included,
/// whichever server the picker shows, so it keeps loading.</para>
/// </summary>
internal static class FinOpsServerChoice
{
    /// <summary>The line a panel shows in place of its data for a PostgreSQL target. Same words as the web page.</summary>
    internal const string PostgresNotCollected = "Not collected for PostgreSQL";

    /// <summary>The picker's text for a server: its display name, and " (PostgreSQL)" after a PostgreSQL target's.</summary>
    internal static string Label(DarlingServer server)
    {
        ArgumentNullException.ThrowIfNull(server);
        return server.IsPostgres ? server.DisplayName + " (PostgreSQL)" : server.DisplayName;
    }

    /// <summary>
    /// The server the FinOps picker shows after its list is built or rebuilt: the server it showed before while that
    /// one is still listed (a rebuild is not the user choosing a server, and a PostgreSQL target the user picked stays);
    /// else the sidebar's server when that is a SQL Server target; else the first SQL Server target in the shell's
    /// order. A list with no SQL Server target falls back to the sidebar's server, then the first one, and the tab says
    /// "Not collected for PostgreSQL" for it. Null for an empty list.
    /// </summary>
    internal static DarlingServer? Selection(IReadOnlyList<DarlingServer> servers, int? previousServerId, int? sidebarServerId)
    {
        ArgumentNullException.ThrowIfNull(servers);

        if (servers.Count == 0)
        {
            return null;
        }

        var previous = previousServerId is null ? null : servers.FirstOrDefault(s => s.ServerId == previousServerId);
        if (previous is not null)
        {
            return previous;
        }

        var sidebar = sidebarServerId is null ? null : servers.FirstOrDefault(s => s.ServerId == sidebarServerId);
        if (sidebar is { IsPostgres: false })
        {
            return sidebar;
        }

        return servers.FirstOrDefault(s => !s.IsPostgres) ?? sidebar ?? servers[0];
    }

    /// <summary>
    /// The line to show in place of a sub-tab's data, or null when its reads run. A PostgreSQL target gets
    /// <see cref="PostgresNotCollected"/> for every sub-tab that reads one server's SQL Server data; a sub-tab that
    /// reads the whole fleet (<paramref name="crossServer"/>, Server Inventory) always runs.
    /// </summary>
    internal static string? NotCollectedLine(DarlingServer? server, bool crossServer)
    {
        return !crossServer && server != null && server.IsPostgres ? PostgresNotCollected : null;
    }

    /// <summary>The reader's <c>include_removed</c> flag for the Server Inventory "Show removed servers" checkbox: on only when checked, off when unchecked or unset.</summary>
    internal static bool IncludeRemoved(bool? showRemovedChecked) => showRemovedChecked == true;

    /// <summary>The Server Inventory "N server(s)" line, with the web page's "(removed servers included)" when the box was ticked for the read that produced the list. Blank for an empty list.</summary>
    internal static string InventoryCountText(int count, bool removedIncluded) =>
        count > 0 ? $"{count} server(s)" + (removedIncluded ? " (removed servers included)" : "") : "";
}

/// <summary>
/// Newest request wins (#5492 round 2), the Viewer's form of the web FinOps tab's <c>seq</c> guard. Each load takes a
/// token before its reads; when the reads finish it applies its result only if no later load has begun, so a slow read
/// for "Show removed servers" ticked that finishes after the box was cleared cannot put removed servers back in the grid.
/// </summary>
internal sealed class FinOpsLoadSequence
{
    private int _latest;

    /// <summary>Starts a load and returns its token. Every earlier token stops being current.</summary>
    internal int Begin() => System.Threading.Interlocked.Increment(ref _latest);

    /// <summary>True while no load newer than <paramref name="token"/> has begun.</summary>
    internal bool IsCurrent(int token) => System.Threading.Volatile.Read(ref _latest) == token;
}
