/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The metric names a mute rule editor offers (#8350): the alert types the engines judge mute rules against, read from the
/// one census that names them (<see cref="AlertFamily.MetricFamilies"/>), so the editors and the engines hold one list and an
/// alert of any censused type can be muted by type alone. An editor takes the families its app raises: the Viewer every family,
/// Lite the performance and agent-job families without the PostgreSQL names, since only the Darling service raises the rest and
/// a rule for a name an app never raises matches nothing.
/// </summary>
public static class MuteRuleMetricList
{
    /// <summary>
    /// Every censused metric name, grouped by family in <see cref="AlertFamily.All"/>'s order and in census order within a
    /// family. The dynamic shapes (<c>Custom:</c>, <c>External:</c>, <c>Analysis:</c>) have no fixed names to list; an
    /// editor opened from such an alert adds the alert's own name instead.
    /// </summary>
    public static IReadOnlyList<string> Names { get; } = For(AlertFamily.All, sqlServerOnly: false);

    /// <summary>
    /// The censused names of <paramref name="families"/>, in <see cref="AlertFamily.All"/>'s family order and census order
    /// within a family. <paramref name="sqlServerOnly"/> leaves out the PostgreSQL alerts, which the census keeps in the
    /// performance family and only the Darling service raises.
    /// </summary>
    public static IReadOnlyList<string> For(IReadOnlyList<string> families, bool sqlServerOnly)
    {
        var names = new List<string>(AlertFamily.MetricFamilies.Count);
        foreach (var family in AlertFamily.All)
        {
            if (!families.Contains(family))
                continue;
            foreach (var pair in AlertFamily.MetricFamilies)
            {
                if (!string.Equals(pair.Value, family, System.StringComparison.Ordinal))
                    continue;
                if (sqlServerOnly && pair.Key.StartsWith("PostgreSQL ", System.StringComparison.Ordinal))
                    continue;
                names.Add(pair.Key);
            }
        }

        return names;
    }
}
