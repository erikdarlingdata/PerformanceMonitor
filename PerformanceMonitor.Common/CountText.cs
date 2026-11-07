/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;

namespace PerformanceMonitor.Common;

/// <summary>
/// A count with its noun, the way a person says it: "1 event", "3 events", "12 recommendations". A label that counts rows
/// uses this instead of the "N event(s)" form, so the same count reads the same on every tab.
/// </summary>
public static class CountText
{
    /// <summary>
    /// <paramref name="count"/> followed by <paramref name="singular"/>, with an "s" added when the count is not one. The nouns
    /// the apps count (event, run, alert, recommendation, database) all take a plain "s".
    /// </summary>
    public static string Plural(int count, string singular) =>
        count == 1 ? $"1 {singular}" : $"{count.ToString(CultureInfo.InvariantCulture)} {singular}s";
}
