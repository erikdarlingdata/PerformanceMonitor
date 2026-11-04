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

    /// <summary>
    /// The prefix every SQL Server connection the monitor opens carries: the Darling service
    /// (<see cref="Darling"/> and its <c>-Remediation</c> and <c>-HypotheticalIndex</c> helpers) and Lite
    /// (<see cref="Lite"/>). The desktop viewer, web host and MCP host connect to the monitoring store, never to a
    /// monitored SQL Server, so they never appear in the monitored server's sessions. This is the ONE value the
    /// collectors that leave the monitor's own sessions out of a read build their predicate from (#7416).
    /// </summary>
    public const string SqlProgramNamePrefix = "PerformanceMonitor";

    /// <summary>
    /// The T-SQL <c>LIKE</c> pattern for <see cref="SqlProgramNamePrefix"/>: a PREFIX match, so a user's
    /// application whose name merely contains the words (<c>MyPerformanceMonitorTool</c>) is kept, and one that
    /// begins with them is the monitor's. Case follows the monitored server's collation, as it always did in
    /// <c>SessionStatsCollector</c>.
    /// </summary>
    public const string SqlProgramNameLikePattern = SqlProgramNamePrefix + "%";

    /// <summary>
    /// The T-SQL predicate that keeps a session that is NOT the monitor's: <paramref name="alias"/>.program_name
    /// is NULL (a NULL program name is never the monitor's, and a bare <c>NOT LIKE</c> would drop it as unknown) or
    /// does not match <see cref="SqlProgramNameLikePattern"/>.
    /// </summary>
    public static string SqlNotMonitorSessionPredicate(string alias)
        => $"({alias}.program_name IS NULL OR {alias}.program_name NOT LIKE N'{SqlProgramNameLikePattern}')";

    /// <summary>The C# twin of the <c>LIKE</c> pattern above: true when the program name begins with
    /// <see cref="SqlProgramNamePrefix"/> (ordinal, ignoring case), so a test can check each name the monitor
    /// connects under against the same prefix the SQL uses.</summary>
    public static bool MatchesSqlProgramNamePattern(string? programName)
        => programName is not null && programName.StartsWith(SqlProgramNamePrefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>True when <paramref name="applicationName"/> is the Darling service's or Lite's (or one of their
    /// suffixed helper connections), compared ordinal and case-insensitive.</summary>
    public static bool IsMonitorApplication(string? applicationName)
    {
        if (string.IsNullOrEmpty(applicationName)) return false;
        return applicationName.StartsWith(Darling, StringComparison.OrdinalIgnoreCase)
            || applicationName.StartsWith(Lite, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// True when the deadlock's VICTIM process connected with a monitor application name (the graph's
    /// <c>process/@clientapp</c>). The collectors read DMVs and the system_health session, and the monitored
    /// workload can pick one as the victim: that deadlock is still stored and shown, but it is not the operator's
    /// to act on, so the alert does not count it. A deadlock where a user session was the victim counts even when
    /// a monitor session sits in the graph, because the monitor then cost the operator real work. The victim is
    /// the stored <paramref name="victimProcessId"/> plus any <c>victim-list/victimProcess/@id</c> in the graph
    /// (a parallel deadlock can list several). Every victim process must be the monitor's. Unparseable or empty
    /// XML, or a graph whose victim cannot be found, is never the monitor's.
    /// </summary>
    public static bool VictimIsMonitorSession(string? graphXml, string? victimProcessId)
    {
        if (string.IsNullOrEmpty(graphXml)) return false;
        try
        {
            var doc = System.Xml.Linq.XElement.Parse(graphXml);
            var victimIds = new System.Collections.Generic.HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (!string.IsNullOrEmpty(victimProcessId)) victimIds.Add(victimProcessId);
            foreach (var v in doc.Descendants("victimProcess"))
            {
                var id = v.Attribute("id")?.Value;
                if (!string.IsNullOrEmpty(id)) victimIds.Add(id);
            }

            var victims = doc.Descendants("process")
                .Where(p => p.Attribute("id")?.Value is { Length: > 0 } id && victimIds.Contains(id))
                .ToList();
            return victims.Count > 0
                && victims.All(p => IsMonitorApplication(p.Attribute("clientapp")?.Value));
        }
        catch { return false; }
    }
}
