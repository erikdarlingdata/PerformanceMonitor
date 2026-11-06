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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7706: the one sentence for a database this server holds only as an availability group secondary copy and that has no PVS row in
/// the snapshot (the DMV reports nothing for it here), so the Version Store grid does not look as if the database were forgotten.
/// One copy for the MCP tool (<c>secondary_replica_note</c> on <c>get_pvs_stats</c>, which the web page shows) and the Viewer's grid.
/// </summary>
public static class PvsSecondaryNote
{
    /// <summary>
    /// The sentence for the secondary databases that have no row in <paramref name="rowDatabases"/> and are among the databases the
    /// <paramref name="filter"/> chose. A name is matched exactly, as <see cref="AgReplicaScope"/> does. Null when none.
    /// </summary>
    public static string? WithoutRow(IEnumerable<string> secondaryDatabases, IEnumerable<string> rowDatabases, DatabaseFilter filter)
    {
        var withRow = new HashSet<string>(rowDatabases, StringComparer.Ordinal);
        var count = secondaryDatabases.Count(d => !withRow.Contains(d)
            && (filter.IsAll || filter.Names.Contains(d, StringComparer.Ordinal))
            && !filter.ExcludedNames.Contains(d, StringComparer.Ordinal));
        return count switch
        {
            0 => null,
            1 => "1 database has no row here: this server holds a secondary copy of it in an availability group. Check the primary replica for its persistent version store.",
            _ => string.Create(System.Globalization.CultureInfo.InvariantCulture,
                $"{count} databases have no row here: this server holds a secondary copy of them in an availability group. Check the primary replica for their persistent version store."),
        };
    }
}
