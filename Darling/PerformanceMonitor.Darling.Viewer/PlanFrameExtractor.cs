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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Pulls the (sql_handle, statement-start, statement-end) frames out of a deadlock graph's process node or a
/// blocked-process report's blocked/blocking side — the keys the <c>fetch_plan</c> command needs to read a
/// process's plan from the live cache. The shared <see cref="PerformanceMonitor.Common.DeadlockGraphParser"/>
/// deliberately drops the raw <c>sqlhandle</c> (it exposes the parsed SqlText / SPID / procname for display),
/// so this walks the raw XML the store rows carry directly — Lite's <c>ServerTab.Plans.cs</c> frame-walk lifted
/// to a pure, testable helper (no UI coupling). The all-zero dynamic-SQL / system-context handle is skipped
/// (SQL Server records it when there is no persistent sql_handle, and no plan can be recovered from it).
/// </summary>
public static class PlanFrameExtractor
{
    /// <summary>SQL Server's 42-byte all-zero sql_handle sentinel for dynamic SQL / system contexts (no plan). Sample data
    /// only: the extractors below skip a zero handle of any length through <see cref="DeadlockGraphParser.IsZeroSqlHandle"/>.</summary>
    public static readonly string ZeroSqlHandle = "0x" + new string('0', 84);

    /// <summary>
    /// The candidate frames for ONE deadlock-graph process (matched by its graph node id): the process-level
    /// sqlhandle first (deadlock graphs frequently put it on <c>&lt;process&gt;</c>), then the executionStack
    /// <c>&lt;frame&gt;</c> handles top-down. Empty when the graph / id is blank, the process is not found, or
    /// every handle is the zero sentinel.
    /// </summary>
    public static IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> ExtractDeadlockProcessFrames(
        string graphXml, string processId)
    {
        var empty = Array.Empty<(string, int, int)>();
        if (string.IsNullOrWhiteSpace(graphXml) || string.IsNullOrWhiteSpace(processId)) return empty;
        try
        {
            var doc = XElement.Parse(graphXml);
            var process = doc.Descendants("process")
                .FirstOrDefault(p => string.Equals(p.Attribute("id")?.Value, processId, StringComparison.OrdinalIgnoreCase));
            if (process == null) return empty;

            var frames = new List<(string, int, int)>();

            var procHandle = process.Attribute("sqlhandle")?.Value;
            if (!string.IsNullOrWhiteSpace(procHandle) &&
                !DeadlockGraphParser.IsZeroSqlHandle(procHandle))
            {
                int ps = 0, pe = -1;
                int.TryParse(process.Attribute("stmtstart")?.Value, out ps);
                if (int.TryParse(process.Attribute("stmtend")?.Value, out var peParsed)) pe = peParsed;
                frames.Add((procHandle!, ps, pe));
            }

            var stack = process.Element("executionStack");
            if (stack != null)
            {
                foreach (var frame in stack.Elements("frame"))
                {
                    var handle = frame.Attribute("sqlhandle")?.Value;
                    if (string.IsNullOrWhiteSpace(handle)) continue;
                    if (DeadlockGraphParser.IsZeroSqlHandle(handle)) continue;

                    int fs = 0, fe = -1;
                    int.TryParse(frame.Attribute("stmtstart")?.Value, out fs);
                    if (int.TryParse(frame.Attribute("stmtend")?.Value, out var feParsed)) fe = feParsed;
                    frames.Add((handle!, fs, fe));
                }
            }

            return frames;
        }
        catch
        {
            return empty;
        }
    }

    /// <summary>
    /// The candidate frames for the blocked OR blocking side of a blocked-process report — the executionStack
    /// <c>&lt;frame&gt;</c> handles, zero sentinel skipped. Searches by descendant (matching the Dashboard) so
    /// it works regardless of how deep the process node sits. Empty when the XML is blank / has no matching
    /// executionStack, or every handle is the zero sentinel.
    /// </summary>
    public static IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> ExtractBlockedProcessFrames(
        string bprXml, bool blockingSide)
    {
        var empty = Array.Empty<(string, int, int)>();
        if (string.IsNullOrWhiteSpace(bprXml)) return empty;
        try
        {
            var doc = XElement.Parse(bprXml);
            var processContainer = doc
                .Descendants(blockingSide ? "blocking-process" : "blocked-process")
                .FirstOrDefault();
            var stack = processContainer?.Descendants("executionStack").FirstOrDefault();
            if (stack == null) return empty;

            var frames = new List<(string, int, int)>();
            foreach (var frame in stack.Elements("frame"))
            {
                var handle = frame.Attribute("sqlhandle")?.Value;
                if (string.IsNullOrWhiteSpace(handle)) continue;
                if (DeadlockGraphParser.IsZeroSqlHandle(handle)) continue;

                int stmtStart = 0;
                int stmtEnd = -1;
                int.TryParse(frame.Attribute("stmtstart")?.Value, out stmtStart);
                if (int.TryParse(frame.Attribute("stmtend")?.Value, out var se)) stmtEnd = se;

                frames.Add((handle!, stmtStart, stmtEnd));
            }
            return frames;
        }
        catch
        {
            return empty;
        }
    }
}
