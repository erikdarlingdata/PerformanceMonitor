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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The version lines Manage Servers shows once above its grid (#5655): the version each service host recorded in the
/// store, and the viewer's own. Display only; a viewer older or newer than the service gets no warning here.
/// </summary>
public static class ServiceVersionText
{
    /// <summary>
    /// What a store with no row, or without the table at all, shows for the service version. A service writes its row
    /// from 3.11 on, so a missing row means a service older than that, or one that has not started since the upgrade.
    /// </summary>
    public const string Unknown = "unknown (older than 3.11)";

    /// <summary>What the service line shows when the read itself failed, which is not the same as no row.</summary>
    public const string Unavailable = "unavailable";

    /// <summary>The viewer's own version, as its line shows it.</summary>
    public static string ViewerLine(string viewerVersion) => "Viewer version: " + viewerVersion;

    /// <summary>
    /// The service's lines, one per service host in host order. One host reads "Service version: 3.11.0 (HOST)": the
    /// host name stays, so an operator can tell which machine runs it. Several hosts read one "HOST: version" line each
    /// under a "Service versions:" heading. No host reads <see cref="Unknown"/>.
    /// </summary>
    public static string ServiceLines(IReadOnlyList<ServiceVersionRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        if (rows.Count == 0)
        {
            return "Service version: " + Unknown;
        }

        var ordered = rows.OrderBy(r => r.ServiceHost, StringComparer.OrdinalIgnoreCase).ToList();
        if (ordered.Count == 1)
        {
            return $"Service version: {ordered[0].ProductVersion} ({ordered[0].ServiceHost})";
        }

        return "Service versions:" + Environment.NewLine
            + string.Join(Environment.NewLine, ordered.Select(r => $"{r.ServiceHost}: {r.ProductVersion}"));
    }
}
