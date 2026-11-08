/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// The metric names a mute rule editor offers (#8350): every alert type the engines judge mute rules against, read from
/// the one census that names them (<see cref="AlertFamily.MetricFamilies"/>), so the editors cannot drift from the
/// engines. A type missing from an editor's list is not a cosmetic gap: "Mute This Alert" on such an alert fell back to
/// "(any)" and saved a rule that muted every alert from the server.
/// </summary>
public static class MuteRuleMetricList
{
    /// <summary>
    /// Every censused metric name, grouped by family in <see cref="AlertFamily.All"/>'s order and in census order within a
    /// family. The dynamic shapes (<c>Custom:</c>, <c>External:</c>, <c>Analysis:</c>) have no fixed names to list; an
    /// editor opened from such an alert adds the alert's own name instead.
    /// </summary>
    public static IReadOnlyList<string> Names { get; } = Build();

    private static List<string> Build()
    {
        var names = new List<string>(AlertFamily.MetricFamilies.Count);
        foreach (var family in AlertFamily.All)
        {
            foreach (var pair in AlertFamily.MetricFamilies)
            {
                if (string.Equals(pair.Value, family, System.StringComparison.Ordinal))
                {
                    names.Add(pair.Key);
                }
            }
        }

        return names;
    }
}
