/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Alerting;

/// <summary>One configured target, reduced to the fields the master-scope rule reads.</summary>
public sealed record AlertTargetIdentity(string Id, string Host, string? Database, bool Enabled, bool ReadOnlyIntent);

/// <summary>
/// On Azure SQL Database a target on the logical server's <c>master</c> collects blocked-process reports and
/// deadlocks for EVERY database on that server. When a database is also monitored as its own target, the same
/// event would alert on both. This names the databases the master target should skip.
/// </summary>
public static class AzureMasterScope
{
    /// <summary>The note shown beside a master target's own Blocking and Deadlocks lists when <see cref="SeparatelyMonitoredDatabases"/> is non-empty: those lists keep master's server-wide rows, while the counts skip them.</summary>
    public const string SeparatelyMonitoredListNote =
        "Events from databases monitored as their own servers are listed here and counted under those servers.";

    /// <summary>
    /// The databases whose blocking and deadlock events a master target skips because they alert on their
    /// own targets. Empty unless <paramref name="isAzureSqlDb"/> and this target's database is blank or
    /// <c>master</c> (the same rule as <c>AzureSweepScope.OwnDatabaseOrEmpty</c> in Collectors, which
    /// Alerting cannot reference). Otherwise the distinct databases of the OTHER enabled targets on the same
    /// host. Read-only-intent targets do not count: they monitor a secondary replica, so master's
    /// server-wide events for that database would be lost.
    /// </summary>
    public static IReadOnlyList<string> SeparatelyMonitoredDatabases(
        bool isAzureSqlDb, string selfId, string host, string? database, IEnumerable<AlertTargetIdentity> targets)
    {
        if (!isAzureSqlDb || !IsBlankOrMaster(database))
        {
            return Array.Empty<string>();
        }

        var hostKey = host.Trim();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var result = new List<string>();
        foreach (var t in targets)
        {
            if (t.Id == selfId || !t.Enabled || t.ReadOnlyIntent
                || !string.Equals(t.Host.Trim(), hostKey, StringComparison.OrdinalIgnoreCase)
                || IsBlankOrMaster(t.Database))
            {
                continue;
            }

            if (seen.Add(t.Database!))
            {
                result.Add(t.Database!);
            }
        }

        return result;
    }

    private static bool IsBlankOrMaster(string? database) =>
        string.IsNullOrEmpty(database) || string.Equals(database, "master", StringComparison.OrdinalIgnoreCase);
}
