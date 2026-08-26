/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Parses PostgreSQL deadlock reports out of the server log (#2661).
///
/// <para>PostgreSQL writes a deadlock as an <c>ERROR: deadlock detected</c> line followed by a
/// <c>DETAIL:</c> block that carries the whole graph: one wait edge per participant, then each
/// participant's full statement text. Measured on a real 17.11 target, that is MORE than SQL Server's
/// deadlock graph gives — it names every participant's SQL, where the SQL Server graph frequently leaves the
/// non-victim side as a handle.</para>
///
/// <para><b>Always available, unlike plan capture.</b> There is no setting that suppresses a deadlock report
/// — <c>log_lock_waits</c> governs ordinary lock waits, not this — so unlike <c>auto_explain</c> this needs
/// nothing configured on the target and works on a managed fleet today.</para>
/// </summary>
public static class PgDeadlockLogParser
{
    /// <param name="GraphText">The DETAIL block verbatim, tabs stripped. Stored losslessly because the
    /// parsed fields are an interpretation and the raw block is the evidence: a shape this parser does not
    /// recognise today is still readable by a person.</param>
    /// <param name="DeadlockHash">Identity across overlapping log reads. Both transports read a bounded
    /// TAIL of the log on a schedule, so the same deadlock is seen again on the next cycle and must not be
    /// stored twice.</param>
    public readonly record struct ParsedDeadlock(
        DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string DeadlockHash,
        string? LockModes,
        string? Resources,
        string? VictimStatement,
        string GraphText);

    /* The report as PostgreSQL writes it. Two traps are encoded here rather than discovered later:

       %Q puts the query id immediately before the severity with NO separator — measured output reads
       `[1549] 322048460535975151ERROR:  deadlock detected` — so this must not require whitespace there.
       [^\n]* between the pid and ERROR: covers the query id whether the prefix carries one or not.

       The DETAIL block is the first line plus every TAB-INDENTED line after it. It ends at the next line
       carrying a log prefix, which is what (?:\t[^\n]*\n)* expresses: a statement inside the block can
       itself contain newlines, and each continuation arrives tab-indented, so a line-count rule or a
       blank-line rule would truncate multi-line SQL silently. */
    private static readonly Regex s_deadlockBlock = new(
        @"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d+) (?<zone>\w+) \[(?<pid>\d+)\][^\n]*ERROR:  deadlock detected\s*\n"
        + @"[^\n]*DETAIL:  (?<detail>(?:[^\n]*\n)(?:\t[^\n]*\n)*)",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /* `Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.`
       The resource is captured WHOLE rather than decomposed: it is `transaction N` here but also
       `relation N of database N`, `tuple (b,o) of relation N`, `advisory lock ...` and more, and an
       enumeration would silently drop the shapes it did not anticipate. */
    private static readonly Regex s_edge = new(
        @"Process (?<waiter>\d+) waits for (?<mode>[A-Za-z ]+) on (?<resource>[^;]+); blocked by process (?<blocker>\d+)\.",
        RegexOptions.Compiled);

    /* `Process 1549: ` then the statement, which runs to the next such header or the end of the block. */
    private static readonly Regex s_participant = new(
        @"^Process (?<pid>\d+): ?[ \t]*\n?",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /// <summary>
    /// Every deadlock in a raw slab of server log.
    ///
    /// <para>A block that will not parse is skipped rather than reported, matching
    /// <see cref="PgPlanLogParser"/>: both transports read a bounded window, so a report cut in half at the
    /// edge is an ordinary consequence of not reading the whole file rather than a fault worth surfacing
    /// every cycle. It will be read whole on the next pass, because the window overlaps.</para>
    /// </summary>
    public static List<ParsedDeadlock> Extract(string? logBody)
    {
        var results = new List<ParsedDeadlock>();

        if (string.IsNullOrEmpty(logBody))
        {
            return results;
        }

        foreach (Match match in s_deadlockBlock.Matches(logBody))
        {
            var parsed = FromBlock(
                match.Groups[1].Value,
                match.Groups["pid"].Value,
                match.Groups["detail"].Value);

            if (parsed is not null)
            {
                results.Add(parsed.Value);
            }
        }

        return results;
    }

    /// <summary>
    /// One report, from its timestamp, victim pid and DETAIL block. Null when the block carries no wait
    /// edge, which is the one thing that makes it a deadlock report rather than some other DETAIL.
    /// </summary>
    public static ParsedDeadlock? FromBlock(string timestampText, string victimPidText, string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail))
        {
            return null;
        }

        if (!DateTime.TryParse(
                timestampText,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                out var occurredAt))
        {
            return null;
        }

        if (!int.TryParse(victimPidText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var victimPid))
        {
            return null;
        }

        /* Tabs out, so the stored block reads as text rather than as log formatting. Newlines stay: they
           separate the edges from the statements and are inside the statements too. */
        var graph = detail.Replace("\t", string.Empty, StringComparison.Ordinal).TrimEnd('\n');

        var edges = s_edge.Matches(graph);

        if (edges.Count == 0)
        {
            return null;
        }

        var modes = edges
            .Select(e => e.Groups["mode"].Value.Trim())
            .Where(m => m.Length > 0)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(m => m, StringComparer.Ordinal)
            .ToList();

        var resources = edges
            .Select(e => e.Groups["resource"].Value.Trim())
            .Where(r => r.Length > 0)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(r => r, StringComparer.Ordinal)
            .ToList();

        /* Participants are counted from the EDGES rather than from the `Process N:` headers, because the
           statement headers are absent when the server could not recover the text — a participant with no
           statement is still a participant, and counting headers would under-report the cycle. */
        var participants = edges
            .SelectMany(e => new[] { e.Groups["waiter"].Value, e.Groups["blocker"].Value })
            .Distinct(StringComparer.Ordinal)
            .Count();

        var statements = ParseStatements(graph);
        statements.TryGetValue(victimPid, out var victimStatement);

        return new ParsedDeadlock(
            OccurredAtUtc: occurredAt,
            VictimPid: victimPid,
            ParticipantCount: participants,
            DeadlockHash: HashOf(graph),
            LockModes: modes.Count > 0 ? string.Join(", ", modes) : null,
            Resources: resources.Count > 0 ? string.Join(", ", resources) : null,
            VictimStatement: victimStatement,
            GraphText: graph);
    }

    /// <summary>
    /// Each participant's statement, keyed by pid. A statement runs from its <c>Process N:</c> header to the
    /// next header or the end of the block, because arbitrary user SQL contains newlines and taking one line
    /// would truncate every multi-line statement to its first.
    /// </summary>
    public static Dictionary<int, string> ParseStatements(string graph)
    {
        var statements = new Dictionary<int, string>();
        var headers = s_participant.Matches(graph);

        for (var i = 0; i < headers.Count; i++)
        {
            if (!int.TryParse(headers[i].Groups["pid"].Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var pid))
            {
                continue;
            }

            var start = headers[i].Index + headers[i].Length;
            var end = i + 1 < headers.Count ? headers[i + 1].Index : graph.Length;

            if (end <= start)
            {
                continue;
            }

            var text = graph[start..end].Trim();

            if (text.Length > 0)
            {
                statements[pid] = text;
            }
        }

        return statements;
    }

    /// <summary>
    /// Identity for a report, over the graph text. Both transports read a bounded TAIL on a schedule, so the
    /// same deadlock appears in consecutive reads and would otherwise be stored once per cycle for as long
    /// as it stays inside the window.
    ///
    /// <para>Over the graph rather than over (timestamp, pid): two deadlocks in the same millisecond with
    /// the same victim pid are vanishingly unlikely, but the graph is what actually distinguishes them, and
    /// hashing the thing itself needs no argument about how unlikely a collision is.</para>
    /// </summary>
    public static string HashOf(string graph)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(graph));
        return Convert.ToHexString(bytes)[..32];
    }
}
