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
/// <para><b>Nothing to ENABLE, unlike plan capture.</b> A deadlock report needs no preload, no restart and
/// no extension — <c>log_lock_waits</c> governs ordinary lock waits, not this — so unlike
/// <c>auto_explain</c> this needs nothing turned on at the target and works on a managed fleet
/// today.</para>
///
/// <para><b>Which is not the same as unsuppressable, and the difference is where this parser's silence
/// comes from.</b> <c>log_error_verbosity = terse</c> drops the DETAIL field, and DETAIL is the whole
/// graph — every wait edge and every participant's SQL. The <c>ERROR:  deadlock detected</c> line is still
/// written, so the log LOOKS like it holds deadlocks while the block pattern, which requires the DETAIL
/// group, matches nothing. Zero rows from a server that is deadlocking, in the shape of a server that is
/// not. Check the verbosity parameter before believing an empty result (#3030).</para>
/// </summary>
public static class PgDeadlockLogParser
{
    /// <param name="GraphText">The DETAIL block verbatim, tabs stripped. Stored losslessly because the
    /// parsed fields are an interpretation and the raw block is the evidence: a shape this parser does not
    /// recognise today is still readable by a person.</param>
    /// <param name="DeadlockHash">Identity across repeated log reads, needed on both transports but for
    /// different reasons. The <c>pg_read_file</c> route re-reads a bounded TAIL every cycle, so the same
    /// deadlock arrives on every cycle it stays inside the window. The RDS route is consume-once and does
    /// NOT re-offer it cycle to cycle — its repeats come from a restart discarding the in-process resume
    /// marker, or from #3008 leaving that marker in place after a write that did not land. Either way the
    /// report must not be stored twice.</param>
    public readonly record struct ParsedDeadlock(
        DateTime OccurredAtUtc,
        int VictimPid,
        int ParticipantCount,
        string DeadlockHash,
        string? LockModes,
        string? Resources,
        string? VictimStatement,
        string GraphText);

    /* The report as PostgreSQL writes it, under either family of log_line_prefix this parser meets. The
       two families are ALTERNATIVES rather than one relaxed pattern, and that is the load-bearing part.

       PostgreSQL's own default is '%m [%p] ', rendering `2026-08-26 22:25:24.100 UTC [1549] `: the zone
       runs to a SPACE, and a space is the one character it cannot contain, so [^ \n]+ reads it whole
       whatever it is. The system default on a managed parameter group is '%t:%r:%u@%d:[%p]:', rendering
       `2026-08-26 22:25:24 UTC:<host>(<port>):<user>@<db>:[1549]:`: the zone runs to a COLON, %r and
       %u@%d follow it, and %r renders EMPTY on a line with no client connection, so that run can collapse
       to consecutive colons. [^\n]*? is lazy, so the pid is the FIRST bracketed run of digits after the
       delimiter - an %r rendering an IPv6 address brings its own brackets, and an all-digit pid is what
       tells them apart.

       ONE pattern spanning both, with the delimiter as [ :] and the zone as an abbreviation-or-offset
       alternation, is WRONG, and the way it is wrong is a wrong answer rather than no answer. A numeric
       offset carries colons of its own (-03:30), so a zone allowed to keep them cannot tell its own colon
       from the prefix's: on `+00:2001:db8::1(...)` — a zero-offset zone and an IPv6 %r — the offset loop
       eats `:2001` as another of its groups, IPv6's own colon then satisfies the delimiter, nothing
       backtracks, and the zone reads `+00:2001`. IsZeroOffsetLogZone refuses that, so a target that IS
       UTC has every window abandoned with a message naming a setting already correct. Bounding the
       offset's groups to two digits only moves the boundary: `+00:20:db8::1(...)` breaks it again.

       Splitting by delimiter removes the ambiguity instead of narrowing it, because the colon family's
       zone then excludes ':' outright. The trade is that a numeric offset under a colon-delimited prefix
       is read up to its first colon, so a non-zero one is refused as `-03` rather than `-03:30`. The
       VERDICT is unchanged in every case; only the token in the message is shorter, and only on a prefix
       family that no measured target combines with a numeric zone. Reading a UTC server as non-UTC is the
       failure worth designing against; a terser refusal message is not.

       The FRACTION is optional in both, because %m renders fractional seconds and %t does not.

       %Q puts the query id immediately before the severity with NO separator — measured output reads
       `[1549] 322048460535975151ERROR:  deadlock detected` — so this must not require whitespace there.
       [^\n]* between the pid and ERROR: covers the query id whether the prefix carries one or not.

       The DETAIL block is the first line plus every TAB-INDENTED line after it. It ends at the next line
       carrying a log prefix, which is what (?:\t[^\n]*\n)* expresses: a statement inside the block can
       itself contain newlines, and each continuation arrives tab-indented, so a line-count rule or a
       blank-line rule would truncate multi-line SQL silently.

       The zone is captured and READ (#2993). PostgreSQL renders the stamp in log_timezone and prints that
       zone's abbreviation beside it, so this token is the setting's own rendered value for THIS line, and
       it is what decides whether the naive timestamp next to it is already UTC. See IsZeroOffsetLogZone
       for why that question is answerable from an abbreviation when "which zone is this" is not. Both
       families name the group `zone` and the group `pid`, which .NET allows across alternatives and which
       keeps the reads below indifferent to which family matched.

       Not \w+ for either zone: \w matches neither a sign nor a colon, and a prefix the pattern cannot
       match produces no block at all, which reads as a server with no deadlocks.

       PgDeadlocksCollector holds this pattern's counterpart for the pg_read_file route, as SQL and
       narrower: that one carries the space family only, which PostgreSQL's own default renders. */
    private static readonly Regex s_deadlockBlock = new(
        @"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?) "
        + @"(?:(?<zone>[^ \n]+) \[(?<pid>\d+)\]|(?<zone>[^ :\n]+):[^\n]*?\[(?<pid>\d+)\])"
        + @"[^\n]*ERROR:  deadlock detected\s*\n"
        + @"[^\n]*DETAIL:  (?<detail>(?:[^\n]*\n)(?:\t[^\n]*\n)*)",
        RegexOptions.Compiled | RegexOptions.Multiline);

    /* `Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.`
       The resource is captured WHOLE rather than decomposed: it is `transaction N` here but also
       `relation N of database N`, `tuple (b,o) of relation N`, `advisory lock ...` and more, and an
       enumeration would silently drop the shapes it did not anticipate. */
    private static readonly Regex s_edge = new(
        @"Process (?<waiter>\d+) waits for (?<mode>[A-Za-z ]+) on (?<resource>[^;]+); blocked by process (?<blocker>\d+)\.",
        RegexOptions.Compiled);

    /* A numeric log-prefix offset that is zero: +00, -00, +00:00, -0000. Anchored, so a non-zero offset
       sharing a prefix with a zero one (+0030) cannot match part of itself and pass. */
    private static readonly Regex s_zeroOffset = new(
        @"^[+-]0+(?::0+)*$",
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
    /// every cycle.</para>
    ///
    /// <para><b>Whether that skip is ever recovered depends on the transport, and on the managed one it is
    /// not (#3009).</b> <see cref="PgDeadlocksCollector"/>'s <c>pg_read_file</c> route re-reads a
    /// byte-window tail every cycle that overlaps the previous one <b>while the log grows by less than
    /// that tail between cycles</b>, and a report cut at one read's edge arrives whole in the next only
    /// inside that condition. The tail is 4 MB and the cadence is five minutes, so the condition is a
    /// write rate under roughly 14 KB/s. Above it consecutive reads stop touching and the bytes between
    /// them are read by nobody — reports lost entire rather than cut, which is a different outcome from
    /// the one this paragraph is about and is not recovered by anything. Neither collector measures the
    /// write rate, so neither can distinguish a quiet server from a truncated view of a loud one. The RDS log-API route is CONSUME-ONCE: <c>RdsLogSource</c>
    /// holds a resume marker per (instance, file) and <c>DownloadDBLogFilePortion</c> starts the next call
    /// where the last one stopped, so there is no overlap and no next pass, and a report straddling a
    /// chunk boundary is not completed for the life of that marker. The paragraph above is not a recovery
    /// guarantee — it says truncation is expected on both transports, not that it clears itself on
    /// both.</para>
    ///
    /// <para><b>Two shapes on that route, and the second is the dangerous one.</b> A chunk ending before
    /// <c>DETAIL:</c> matches nothing, because the pattern requires that group, and the report is lost
    /// whole. A chunk ending mid-DETAIL still MATCHES — <c>(?:\t[^\n]*\n)*</c> takes however many
    /// continuation lines arrived, including none — so <see cref="FromBlock"/> finds a wait edge and
    /// stores a row carrying only the participants, resources and statements that were inside the chunk.
    /// That row is indistinguishable from a genuinely smaller deadlock, which is worse than the absence.
    /// It cannot be checked against its own graph either: <c>ParticipantCount</c> is derived from the same
    /// edges <c>GraphText</c> holds, so those two agree by construction. What a fragment does leave is an
    /// edge list that is not a CYCLE — a whole report carries one edge per participant, so a participant
    /// count EXCEEDING the edge count is a shape the server never writes.</para>
    ///
    /// <para>The one exception is a log stamped in a non-UTC zone, which throws
    /// <see cref="PgLogTimezoneUnsupportedException"/> instead: see <see cref="FromBlock"/>.</para>
    ///
    /// <para><b>That throw abandons the WHOLE read, siblings included.</b> One log has one
    /// <c>log_timezone</c> at any instant, so a window holding both a non-UTC and a UTC stamp only
    /// happens while a change to that setting straddles the read. Storing the UTC half and refusing the
    /// rest would leave a partial history from a target we have just declared unreadable, with nothing in
    /// the data marking what is missing — the reader could not then tell a quiet server from a
    /// half-collected one. A whole-read refusal says one thing, and the runner's row says it.</para>
    ///
    /// <para><b>What that refusal costs is the same on both transports, and it is #3008 that made it so
    /// rather than anything here.</b> <see cref="PgDeadlocksCollector"/> re-reads an overlapping
    /// byte-window tail every cycle, so a straddled window there loses nothing permanently — the same
    /// text arrives again next cycle, and once the non-UTC lines age out of the window the readable
    /// siblings store. On the RDS log-API route the throw leaves the ingestor WITHOUT its resume marker
    /// being committed, because that commit sits after the write rather than inside the fetch, so the
    /// next cycle asks for the same window and the readable siblings survive there too.</para>
    ///
    /// <para><b>Which is why the truncation above is a different problem, and not one the
    /// marker-ordering fix closed.</b> A refusal FAILS, so the marker is withheld and the bytes come
    /// back. A truncated block parses, stores and commits, so the marker advances past the fragment on
    /// the strength of a cycle that succeeded by every signal the ingestor has. Do not read #3008 as
    /// covering both.</para>
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
                match.Groups["zone"].Value,
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
    /// One report, from its log-prefix timestamp and zone, its victim pid and its DETAIL block. Null when
    /// the block carries no wait edge, which is the one thing that makes it a deadlock report rather than
    /// some other DETAIL.
    /// </summary>
    /// <param name="zoneText">The zone token from the log prefix, which is <c>log_timezone</c> as the
    /// server rendered it for this line. Required, and required to mean an offset of zero.</param>
    /// <exception cref="PgLogTimezoneUnsupportedException">The prefix zone is not a zero-offset one, so the
    /// timestamp beside it is local rather than UTC and the report cannot be stored (#2993).</exception>
    public static ParsedDeadlock? FromBlock(string timestampText, string zoneText, string victimPidText, string? detail)
    {
        if (string.IsNullOrWhiteSpace(detail))
        {
            return null;
        }

        /* THROWN rather than skipped, and it is the only thing in this parser that is not tolerant.
           Everything else a block can be wrong about is tolerated because the pg_read_file route reads it
           again on the next overlapping pass, so skipping it costs nothing THERE. The consume-once RDS
           route has no such pass, so a skip on it stands for the life of its resume marker — see
           Extract's remarks and #3009.
           This one neither clears itself nor announces itself: the timestamp
           below parses PERFECTLY under a non-UTC prefix, AssumeUniversal takes it for UTC, and the row
           lands in the wrong hour with nothing anywhere disagreeing. Returning null instead would store no
           deadlocks and read as a server that has none, which is the same silent-wrong one layer up. A
           refusal the runner can classify is the only outcome that names the setting.

           Per BLOCK, so it abandons the whole read from inside Extract's loop and the collector's row
           loop alike. See Extract's remarks for why losing the readable siblings is preferred to storing
           a partial history nothing marks as partial — and for why that trade is cheap on the
           re-reading transport and NOT cheap on the consume-once one. */
        if (!IsZeroOffsetLogZone(zoneText))
        {
            throw new PgLogTimezoneUnsupportedException(zoneText);
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
    /// Whether a log-prefix zone token means an offset of exactly zero, so the naive timestamp printed
    /// beside it is already UTC and needs no conversion.
    ///
    /// <para><b>Detection, never conversion.</b> An abbreviation cannot be converted from — <c>CST</c> is
    /// US Central, China Standard and Cuba Standard — and that is the good reason the captured zone was
    /// never wired up. But "is this offset zero" is a far weaker question than "which zone is this", and
    /// the token answers it exactly: three abbreviations are zero by definition, a numeric offset says so
    /// arithmetically, and everything else either has an offset or is ambiguous about having one, which for
    /// this purpose is the same answer.</para>
    ///
    /// <para><b>Per LINE rather than per server, which is why <c>log_timezone</c> is not read from the
    /// target.</b> <c>current_setting('log_timezone')</c> describes the collector's connection now; this
    /// token was written by the server into the line being parsed, under whatever the setting was then, so
    /// it is the better witness of the two for the row it is attached to. On <c>Europe/London</c> they
    /// disagree for half the year — winter lines are stamped <c>GMT</c> and really are UTC, and a GUC read
    /// would refuse them — and on the managed transport there is no connection to ask at all, because that
    /// route receives log TEXT and runs no SQL.</para>
    /// </summary>
    public static bool IsZeroOffsetLogZone(string? zone)
    {
        if (string.IsNullOrWhiteSpace(zone))
        {
            /* Nothing said the timestamp was UTC. Unreachable through Extract, whose pattern requires
               the token, and reachable through the collector, which reads the zone from a result column
               that can come back NULL. */
            return false;
        }

        var token = zone.Trim();

        /* UT is deliberately absent: it is not a zone PostgreSQL renders, and an over-long allowlist is
           the same silent-wrong defect as no check at all, arriving one entry at a time. */
        if (string.Equals(token, "UTC", StringComparison.OrdinalIgnoreCase)
            || string.Equals(token, "GMT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(token, "UCT", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        /* A zone with no abbreviation renders as a numeric offset, and a numerically zero one is UTC by
           another spelling. Matched as a SHAPE rather than enumerated, because the rendering carries only
           as much precision as the offset needs (+00, +00:00, -0000) and an allowlist that missed a form
           would refuse a server that is perfectly UTC. */
        return s_zeroOffset.IsMatch(token);
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
    /// Identity for a report, over the graph text. The same report reaches this more than once on either
    /// transport and must be stored once, though not for the same reason: the <c>pg_read_file</c> route
    /// re-reads a bounded TAIL on a schedule, so a report inside the window arrives every cycle, while the
    /// consume-once RDS route re-offers only a window whose write did not land (#3008) or whose
    /// in-process marker a restart discarded. See the <c>DeadlockHash</c> parameter above.
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
