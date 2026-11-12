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
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #7725: the order of a top-N read that left databases out (#5633 item 4). The databases the server still lists rank first, exactly as
/// before. What the limit leaves is filled from the left-out (dropped) databases, ranked the same way, so a dropped database's rows can
/// never push out a listed database's row, and the grid is no longer empty when the work is on record. The mark and the envelope names
/// are <c>DroppedDatabaseMark</c> (shared with Lite).
/// </summary>
internal static class DroppedDatabaseRows
{
    /// <summary>
    /// <paramref name="listed"/> (the ranking over the databases the server still lists) followed by the fill: when it is short of
    /// <paramref name="top"/> and databases were left out, <paramref name="readLeftOut"/> reads the left-out databases for the rest.
    /// </summary>
    public static async Task<List<T>> FillAsync<T>(
        List<T> listed, int top, IReadOnlyList<string>? leftOut, Func<DatabaseFilter, int, Task<List<T>>> readLeftOut)
    {
        if (leftOut is null || leftOut.Count == 0 || listed.Count >= top)
        {
            return listed;
        }

        var fill = await readLeftOut(DatabaseFilter.Of(leftOut), top - listed.Count);
        if (fill.Count == 0)
        {
            return listed;
        }

        var combined = new List<T>(listed.Count + fill.Count);
        combined.AddRange(listed);
        combined.AddRange(fill);
        return combined;
    }

    /// <summary>
    /// #7725 (FinOps): marks each row object of the arrays <paramref name="arrayKeys"/> of <paramref name="payload"/> whose
    /// <c>database_name</c> is in <paramref name="dropped"/> (<see cref="CurrentDatabaseScope.ReadDroppedAsync"/>, the one rule
    /// #5633 uses) with <see cref="DroppedDatabaseMark.Key"/>. Mark only: the FinOps lists keep their own order.
    /// </summary>
    public static void MarkByName(JsonObject payload, IReadOnlySet<string>? dropped, params string[] arrayKeys)
    {
        if (dropped is null || dropped.Count == 0)
        {
            return;
        }

        foreach (var key in arrayKeys)
        {
            if (payload[key] is not JsonArray rows)
            {
                continue;
            }

            foreach (var row in rows.OfType<JsonObject>())
            {
                if (row["database_name"] is JsonValue value && value.TryGetValue<string>(out var name) && dropped.Contains(name))
                {
                    row[DroppedDatabaseMark.Key] = true;
                }
            }
        }
    }
}
