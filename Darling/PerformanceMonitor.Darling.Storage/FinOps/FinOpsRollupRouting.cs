/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage.FinOps;

/// <summary>
/// The substitution the FinOps readers use to swap a raw-table fragment for its rollup form. It throws when the
/// fragment is not found: a raw fragment that has drifted from the SQL must fail loudly rather than leave the
/// panel quietly reading the raw tier (#1661).
/// </summary>
internal static class FinOpsRollupRouting
{
    internal static string RouteOrThrow(string sql, string from, string to, string what)
    {
        var routed = sql.Replace(from, to, StringComparison.Ordinal);
        if (string.Equals(routed, sql, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"FinOps {what} CAGG routing found nothing to replace — its raw fragment has drifted from the SQL (#1661).");
        }

        return routed;
    }
}
