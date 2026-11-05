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
using System.Text.Json.Nodes;

namespace PerformanceMonitor.Common;

/// <summary>
/// #7725: the mark and the envelope names of a top-N read that fills from the databases the server no longer lists. The databases it
/// still lists rank first; what the limit leaves is filled from the left-out (dropped) databases, ranked the same way. Each row from a
/// left-out database carries <see cref="Key"/> = true (the key is left off every other row), and the envelope's
/// <c>databases_left_out</c> names only the databases that have no row in the reply. Shared by the Darling service and Lite.
/// </summary>
public static class DroppedDatabaseMark
{
    /// <summary>The row key that marks a row from a database the server no longer lists.</summary>
    public const string Key = "database_dropped";

    /// <summary>The left-out names that have NO row in the reply (ordinal), or null when every one has a row.</summary>
    public static IReadOnlyList<string>? WithoutRows(IReadOnlyList<string>? leftOut, IEnumerable<string?> rowDatabases)
    {
        if (leftOut is null || leftOut.Count == 0)
        {
            return leftOut;
        }

        var represented = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in rowDatabases)
        {
            if (name is not null)
            {
                represented.Add(name);
            }
        }

        return DatabasesLeftOut.Distinct(leftOut.Where(name => !represented.Contains(name)));
    }

    /// <summary>Sets <see cref="Key"/> on each object of <paramref name="rows"/> (in the order of <paramref name="rowDatabases"/>)
    /// whose database is one of <paramref name="leftOut"/>.</summary>
    public static void Mark(JsonArray rows, IReadOnlyList<string?> rowDatabases, IReadOnlyList<string>? leftOut)
    {
        if (leftOut is null || leftOut.Count == 0)
        {
            return;
        }

        var dropped = new HashSet<string>(leftOut, StringComparer.Ordinal);
        for (var i = 0; i < rows.Count && i < rowDatabases.Count; i++)
        {
            if (rowDatabases[i] is { } name && dropped.Contains(name) && rows[i] is JsonObject row)
            {
                row[Key] = true;
            }
        }
    }
}
