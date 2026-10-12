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

namespace PerformanceMonitor.Common;

/// <summary>
/// The one every-process rule for deadlock graphs: a deadlock belongs to a set of databases only when
/// EVERY process in its graph ran in one of them. Shared by the alert sweep's excluded-database check
/// and the analysis facts for an Azure SQL Database master target.
/// </summary>
public static class DeadlockGraphDatabases
{
    /// <summary>
    /// True when every non-empty <c>process/@currentdbname</c> in the graph is in <paramref name="databases"/>
    /// (case-insensitive). An empty list, a graph with no database names, an empty graph or unparseable
    /// XML is never "all in".
    /// </summary>
    public static bool AllIn(string? graphXml, IReadOnlyList<string> databases)
    {
        if (string.IsNullOrEmpty(graphXml)) return false;
        try
        {
            var doc = System.Xml.Linq.XElement.Parse(graphXml);
            var dbNames = doc.Descendants("process")
                .Select(p => p.Attribute("currentdbname")?.Value)
                .Where(n => !string.IsNullOrEmpty(n))
                .Cast<string>()
                .ToList();
            if (dbNames.Count == 0) return false;
            return dbNames.All(db => databases.Any(e =>
                string.Equals(e, db, StringComparison.OrdinalIgnoreCase)));
        }
        catch { return false; }
    }
}
