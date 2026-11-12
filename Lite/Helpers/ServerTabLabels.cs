/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitorLite.Models;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Which open server tabs show a label that no longer matches their server (#7212). The Edit Server dialog changes the
/// live <see cref="ServerConnection"/> in place, so a tab's own record is already the edited one and cannot say what it
/// shows: the comparison is against the label text the tab header holds.
/// </summary>
internal static class ServerTabLabels
{
    /// <summary>The header text for a server: its display name, with " (RO)" when it connects read-only.</summary>
    internal static string HeaderLabel(ServerConnection server)
    {
        ArgumentNullException.ThrowIfNull(server);
        return server.ReadOnlyIntent ? $"{server.DisplayName} (RO)" : server.DisplayName;
    }

    /// <summary>
    /// The loaded servers whose open tab shows a different header label than <see cref="HeaderLabel"/> gives now.
    /// <paramref name="shownLabels"/> maps each open tab's server id to the header text it shows. A tab whose server is not
    /// in <paramref name="loaded"/> is left alone (a removed server's tab stays open), and a tab already showing the right
    /// label needs no change. Matched by server id, so a rename never makes a second tab.
    /// </summary>
    internal static IReadOnlyList<ServerConnection> NeedingNewLabel(
        IReadOnlyDictionary<string, string> shownLabels,
        IReadOnlyList<ServerConnection> loaded)
    {
        ArgumentNullException.ThrowIfNull(shownLabels);
        ArgumentNullException.ThrowIfNull(loaded);

        return loaded
            .Where(server => shownLabels.TryGetValue(server.Id, out var shown)
                && !string.Equals(shown, HeaderLabel(server), StringComparison.Ordinal))
            .ToList();
    }
}
