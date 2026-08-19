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
using System.Xml.Linq;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Extracts the distinct fully-qualified object names (<c>database.schema.object</c>) from a deadlock
/// graph's <c>&lt;resource-list&gt;</c> for the #1140 fingerprint. Mirrors the object extraction the
/// Lite deadlock detail UI already does (LocalDataService deadlock parse) so Lite's live "Deadlocks
/// Detected" builder can feed <see cref="DeadlockIncidentGrouper"/> without re-querying. Dashboard
/// already carries the equivalent on <c>DeadlockItem.ObjectNames</c> and need not parse XML.
/// </summary>
public static class DeadlockObjectExtractor
{
    // Lock resource elements that carry an objectname attribute (matches the Lite UI parser).
    private static readonly string[] s_lockTypes =
        { "objectlock", "pagelock", "keylock", "ridlock", "rowgrouplock" };

    /// <summary>
    /// Returns the distinct database names across the graph's processes (<c>currentdbname</c>), or an
    /// empty list when the XML is blank, unparseable, or carries none. Never throws. The same attribute
    /// the excluded-database filter reads — this is the discrete "Database" fact's source (#2109), kept
    /// separate from the object-name fingerprint parse because a graph can name objects in databases no
    /// process was running in (cross-database deadlocks), and the fact answers "where did this happen",
    /// not "what was locked".
    /// </summary>
    public static IReadOnlyList<string> DatabasesFromGraphXml(string? graphXml)
    {
        if (string.IsNullOrWhiteSpace(graphXml))
            return Array.Empty<string>();

        try
        {
            var doc = XElement.Parse(graphXml);
            var names = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var process in doc.Descendants("process"))
            {
                var db = process.Attribute("currentdbname")?.Value;
                if (!string.IsNullOrWhiteSpace(db))
                    names.Add(db.Trim());
            }

            return names.Count == 0 ? Array.Empty<string>() : names.ToList();
        }
        catch
        {
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Returns the distinct object names across all lock resources in the graph, or an empty list when
    /// the XML is blank, unparseable, or carries no named objects. Never throws.
    /// </summary>
    public static IReadOnlyList<string> FromGraphXml(string? graphXml)
    {
        if (string.IsNullOrWhiteSpace(graphXml))
            return Array.Empty<string>();

        try
        {
            var doc = XElement.Parse(graphXml);
            var resourceList = doc.Descendants("resource-list").FirstOrDefault();
            if (resourceList is null)
                return Array.Empty<string>();

            var names = new SortedSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var lockType in s_lockTypes)
            {
                foreach (var node in resourceList.Elements(lockType))
                {
                    var objectName = node.Attribute("objectname")?.Value;
                    if (!string.IsNullOrWhiteSpace(objectName))
                        names.Add(objectName.Trim());
                }
            }

            return names.Count == 0 ? Array.Empty<string>() : names.ToList();
        }
        catch
        {
            return Array.Empty<string>();
        }
    }
}
