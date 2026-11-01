/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The application names the Darling service and Lite connect to a monitored SQL Server with (#7086), and the
/// test for "this session is the monitor's own". Both connection-string builders take their name from here, so
/// the deadlock alert's own-session filter cannot drift from what the collectors actually send.
/// </summary>
public static class MonitorApplicationNames
{
    /// <summary>The Darling service's name on a monitored SQL Server connection. Its helper connections
    /// (<c>-Remediation</c>, <c>-HypotheticalIndex</c>) extend it with a suffix.</summary>
    public const string Darling = "PerformanceMonitorDarling";

    /// <summary>Lite's name on a monitored SQL Server connection.</summary>
    public const string Lite = "PerformanceMonitorLite";

    /// <summary>True when <paramref name="applicationName"/> is the Darling service's or Lite's (or one of their
    /// suffixed helper connections), compared ordinal and case-insensitive.</summary>
    public static bool IsMonitorApplication(string? applicationName)
    {
        if (string.IsNullOrEmpty(applicationName)) return false;
        return applicationName.StartsWith(Darling, StringComparison.OrdinalIgnoreCase)
            || applicationName.StartsWith(Lite, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// True when ANY process in the deadlock graph connected with a monitor application name (the graph's
    /// <c>process/@clientapp</c>). The collectors read DMVs and the system_health session, and the monitored
    /// workload can pick one as a deadlock victim: that deadlock is still stored and shown, but it is not the
    /// operator's to act on, so the alert does not count it. Unparseable or empty XML is never the monitor's.
    /// </summary>
    public static bool GraphHasMonitorSession(string? graphXml)
    {
        if (string.IsNullOrEmpty(graphXml)) return false;
        try
        {
            var doc = System.Xml.Linq.XElement.Parse(graphXml);
            return doc.Descendants("process")
                .Select(p => p.Attribute("clientapp")?.Value)
                .Any(IsMonitorApplication);
        }
        catch { return false; }
    }
}
