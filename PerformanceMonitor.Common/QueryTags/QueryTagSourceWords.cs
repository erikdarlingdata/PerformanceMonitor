/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Common.QueryTags;

/// <summary>
/// What each Resource Use By Tag section's data is called in words, for the sentence a window shows when the section's source is missing
/// (#6850). Lite and the Darling Viewer both build "This store has no ... data yet." from here, so the two read the same and neither
/// shows a table or view name. Keyed by the section's name; every section has an entry and an unknown name throws, so a new section
/// cannot silently read as another one.
/// </summary>
public static class QueryTagSourceWords
{
    private static readonly Dictionary<string, string> Words = new(StringComparer.Ordinal)
    {
        ["plan_cache"] = "query stats",
        ["query_store"] = "Query Store",
        ["procedures"] = "procedure stats",
        ["blocking_reports"] = "blocked process report",
        ["blocking_snapshots"] = "blocking snapshot",
        ["deadlocks"] = "deadlock",
    };

    /// <summary>The words for one section, for example "query stats". Throws for a section with no entry.</summary>
    public static string For(string sectionName) =>
        Words.TryGetValue(sectionName, out var words)
            ? words
            : throw new ArgumentOutOfRangeException(nameof(sectionName), sectionName, "This query tag section has no source words.");

    /// <summary>"This store has no query stats data yet.": the section's source is missing, said without the source's name.</summary>
    public static string Missing(string sectionName) => $"This store has no {For(sectionName)} data yet.";
}
