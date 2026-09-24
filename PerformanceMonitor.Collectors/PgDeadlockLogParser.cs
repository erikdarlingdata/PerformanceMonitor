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
    /// <param name="GraphText">The DETAIL block as PostgreSQL wrote it, its tab indenting stripped and each
    /// participant's query normalized (#4005): every literal is <c>?</c>, and a query that cannot be read to its
    /// end is withheld (<see cref="PgLogTextRedactor.RedactDetail"/>, the same reading #3944 gave the DETAIL of a
    /// stored log event). The wait-for lines are prose and are kept as written, because the parsed fields are an
    /// interpretation and the block is the evidence: a shape this parser does not recognise today is still
    /// readable by a person.</param>
    /// <param name="VictimStatement">The victim's query as <paramref name="GraphText"/> holds it, so normalized
    /// or withheld the same way.</param>
    /// <param name="DeadlockHash">Identity across repeated log reads, needed on both transports but for
    /// different reasons. The <c>pg_read_file</c> route re-reads a bounded TAIL every cycle, so the same
    /// deadlock arrives on every cycle it stays inside the window. The RDS route is consume-once and does
    /// NOT re-offer it cycle to cycle — its repeats come from a restart discarding the in-process resume
    /// marker, or from #3008 leaving that marker in place after a write that did not land. Either way the
    /// report must not be stored twice. See <see cref="IdentityOf"/> for what it is computed over.</param>
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
       The gap between the pid and ERROR: covers the query id whether the prefix carries one or not (it may
       not cross a field label; see the narrowing below).

       The DETAIL block is the first line plus every TAB-INDENTED line after it. It ends at the next line
       carrying a log prefix, which is what (?:\t[^\n]*\n)* expresses: a statement inside the block can
       itself contain newlines, and each continuation arrives tab-indented, so a line-count rule or a
       blank-line rule would truncate multi-line SQL silently.

       The zone is READ (#2993), by the assembler FromReport hands the candidate to, which splits the same
       two families the same way. PostgreSQL renders the stamp in log_timezone and prints that zone's
       abbreviation beside it, so this token is the setting's own rendered value for THIS line, and it is
       what decides whether the naive timestamp next to it is already UTC. See IsZeroOffsetLogZone for why
       that question is answerable from an abbreviation when "which zone is this" is not.

       Not \w+ for either zone: \w matches neither a sign nor a colon, and a prefix the pattern cannot
       match produces no block at all, which reads as a server with no deadlocks.

       PgDeadlocksCollector holds this pattern's counterpart for the pg_read_file route, as SQL. It carried
       the space family only, with the fraction required, until #4041 gave it this prefix clause whole.

       A CANDIDATE, not a verdict (#4005). `ERROR:  deadlock detected` and `DETAIL:  ` are found anywhere on
       their lines, so a statement's literal holding those words matched as a report, and under a translated
       lc_messages it is the only thing that can. What a candidate is, is decided by PgLogEntryAssembler, the
       reader #3996 hardened for exactly that (FromReport): its label is the line's own, never one inside the
       text, and its DETAIL is the same backend's. The pattern only has to never miss a report the assembler
       would accept.

       The candidate runs one line past the DETAIL, when that line carries a prefix: DeadLockReport always
       writes a HINT after the DETAIL, and that line is the proof the DETAIL arrived whole, which
       PgLogTextRedactor.RedactDetail needs to trust a query after one that does not read to its end. Never a
       line that opens another report, so a candidate cannot take the next report's first line from it.

       The candidate is narrowed to the assembler's own rule as well (#4014), so the pattern does not hand it
       text a real report never looks like:
       - Between the pid and ERROR:, and before DETAIL:, the gap may not contain a field label's ":  ". Every
         real line carries exactly one label, so the ERROR: matched is the line's own and never an echo inside a
         STATEMENT or a LOG line's text. The %Q query id directly before ERROR: has no ":  ", so it still fits.
         A prefix that itself renders ":  " (an application_name holding one, under %a) would hide that
         line's report; that is the rare side, and it can only hide a report, never forge one.
       - The managed family's gap to the pid bracket excludes '[', as plan capture's does since #4008, so it
         cannot slide past the line's real bracket to one inside the text. The lazy '[^\n]*?' it replaces
         could, when the rest of the pattern failed at the real one.
       - The space family admits fields between the zone and the pid (#4041), as '%m %u@%d [%p] ' renders them,
         because the assembler does: a report under that prefix matched nothing here and read as a server with no
         deadlocks. Its gap excludes '[' for the same reason as the managed family's, and may not contain a ":  "
         either, the assembler's label rule in the form this pattern spells it. Both only ever narrow the
         candidate toward the assembler's reading, so no report the assembler accepts is missed: on every line it
         reads, the first bracket after the zone is the pid and no label comes before it.

       PgDeadlocksCollector carries this prefix clause into its SQL as written (#4041), both families and the
       optional fraction, so the pg_read_file route offers every candidate this one does. */
    private static readonly Regex s_deadlockBlock = new(
        @"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)? "
        + @"(?:[^ \n]+ (?:(?!:  )[^\[\n])*\[\d+\]|[^ :\n]+:[^\[\n]*\[\d+\])"
        + @"(?:(?!:  )[^\n])*ERROR:  deadlock detected\s*\n"
        + @"(?:(?!:  )[^\n])*DETAIL:  (?:[^\n]*\n)(?:\t[^\n]*\n)*"
        + @"(?:(?![^\n]*ERROR:  deadlock detected)\d{4}-\d\d-\d\d [^\n]*\n)?",
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
    /// continuation lines arrived, including none — so <see cref="FromReport"/> finds a wait edge and
    /// stores a row carrying only the participants, resources and statements that were inside the chunk.
    /// That row is indistinguishable from a genuinely smaller deadlock, which is worse than the absence.
    /// It cannot be checked against its own graph either: <c>ParticipantCount</c> is derived from the same
    /// edges <c>GraphText</c> holds, so those two agree by construction. What a fragment does leave is an
    /// edge list that is not a CYCLE — a whole report carries one edge per participant, so a participant
    /// count EXCEEDING the edge count is a shape the server never writes.</para>
    ///
    /// <para>The one exception is a log stamped in a non-UTC zone, which throws
    /// <see cref="PgLogTimezoneUnsupportedException"/> instead: see <see cref="FromReport"/>.</para>
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
    public static List<ParsedDeadlock> Extract(string? logBody) => Extract(logBody, logTimezoneIsUtc: false, out _);

    /// <summary>
    /// The managed-route twin (#4046 part 1b): when the target's own <c>log_timezone</c> renders UTC
    /// (<see cref="IsUtcLogTimezoneSetting"/>), a candidate block in another zone is not the server's own —
    /// planted the same way a self-hosted target's is (#4046) — and is skipped and counted in
    /// <paramref name="foreignZoneLines"/> instead of throwing the slab's whole read out. False (the
    /// convenience overload above) keeps today's refusal, for a caller that could not read the setting.
    /// </summary>
    public static List<ParsedDeadlock> Extract(string? logBody, bool logTimezoneIsUtc, out int foreignZoneLines)
    {
        var results = new List<ParsedDeadlock>();
        foreignZoneLines = 0;

        if (string.IsNullOrEmpty(logBody))
        {
            return results;
        }

        foreach (Match match in s_deadlockBlock.Matches(logBody))
        {
            var parsed = FromReport(match.Value, logTimezoneIsUtc, out var blockForeignZoneLines);
            foreignZoneLines += blockForeignZoneLines;

            if (parsed is not null)
            {
                results.Add(parsed.Value);
            }
        }

        return results;
    }

    /// <summary>
    /// One report, from the log text of a candidate block: its <c>ERROR:  deadlock detected</c> line, the
    /// DETAIL block, and the line after it when there is one. Null when the text holds no deadlock report or
    /// the report carries no wait edge, which is the one thing that makes it a deadlock report rather than some
    /// other DETAIL.
    ///
    /// <para><b>Read by <see cref="PgLogEntryAssembler"/></b> (#4005), the reader every other log family shares,
    /// and not by a pattern of its own. That is what makes the line's label its own rather than a phrase inside
    /// a statement, the DETAIL the same backend's, each continuation line lose exactly the tab PostgreSQL added
    /// (a query's own tabs stay, so lexing it reads the tokens PostgreSQL ran), and a HINT after the DETAIL the
    /// proof it is whole (<see cref="PgLogEntry.DetailComplete"/>).</para>
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">The prefix zone is not a zero-offset one, so the
    /// timestamp beside it is local rather than UTC and the report cannot be stored (#2993). Thrown by the
    /// assembler, whose zone check is this parser's own <see cref="IsZeroOffsetLogZone"/>.</exception>
    public static ParsedDeadlock? FromReport(string? reportText) => FromReport(reportText, logTimezoneIsUtc: false, out _);

    /// <summary>
    /// <see cref="FromReport(string?)"/> for a caller that read the target's own <c>log_timezone</c> in the statement
    /// that returned <paramref name="reportText"/> (#4046). When that setting renders UTC
    /// (<see cref="IsUtcLogTimezoneSetting"/>), a line in another zone is not the server's own: the assembler skips it
    /// and counts it in <paramref name="foreignZoneLines"/> instead of refusing, so a report planted in another zone
    /// yields null and one planted line no longer refuses every read of the target. Otherwise this is exactly
    /// <see cref="FromReport(string?)"/>, refusal included.
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">Only when <paramref name="logTimezoneIsUtc"/> is false: see
    /// <see cref="FromReport(string?)"/>.</exception>
    public static ParsedDeadlock? FromReport(string? reportText, bool logTimezoneIsUtc, out int foreignZoneLines)
    {
        foreignZoneLines = 0;

        if (string.IsNullOrEmpty(reportText))
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
           re-reading transport and NOT cheap on the consume-once one. The check is the assembler's, per
           primary line, through IsZeroOffsetLogZone. Under a UTC log_timezone the assembler skips and counts a
           line in another zone instead (#4046): the setting says the server did not write it. */
        foreach (var entry in PgLogEntryAssembler.Assemble(reportText, logTimezoneIsUtc, out foreignZoneLines))
        {
            if (entry.Severity == "ERROR"
                && entry.Message.TrimEnd() == "deadlock detected"
                && !string.IsNullOrWhiteSpace(entry.Detail))
            {
                /* #4058: the same RAISE-shaped check FromEntry applies below, applied here to the stderr
                   route's own candidate entry. FAILS OPEN on this route in a way FromEntry does not: the
                   assembler's block pattern captures the primary line and DETAIL, but the CONTEXT line a
                   PL/pgSQL RAISE appends is not guaranteed to fall inside what this pattern captured, so a
                   forged report whose CONTEXT landed outside the match is not caught here even though the
                   csvlog route (whole assembled entry, no pattern to miss a line past) would catch it. */
                if (PgDeadlockLogParser.IsRaiseShaped(entry))
                {
                    continue;
                }

                return BuildFromEntry(entry);
            }
        }

        return null;
    }

    /// <summary>
    /// #4058: true when <paramref name="e"/> — already matched as an ERROR "deadlock detected" row with a
    /// DETAIL — was not actually written by PostgreSQL's own <c>DeadLockReport</c>. Either
    /// <see cref="PgLogEntryProvenance.RaisedByPlpgsql"/> (a PL/pgSQL <c>RAISE</c> fired this ERROR itself) or
    /// <see cref="PgLogEntryProvenance.ReportedByOther"/> against <c>"DeadLockReport"</c> (verbose
    /// <c>Location</c> names a different C function) is true. Both callers below return null for such an
    /// entry rather than storing it, so no caller of this parser can end up with a forged deadlock row.
    /// </summary>
    public static bool IsRaiseShaped(PgLogEntry e) =>
        PgLogEntryProvenance.RaisedByPlpgsql(e) || PgLogEntryProvenance.ReportedByOther(e, "DeadLockReport");

    /// <summary>
    /// One report, built directly from an already-assembled <see cref="PgLogEntry"/> (#4053 part b1) — the
    /// csvlog route's entry point, used in place of <see cref="FromReport(string?, bool, out int)"/> because
    /// <see cref="PgServerLogCsvParser"/> hands <see cref="PgDeadlocksCollector"/> already-typed entries rather
    /// than raw text for the assembler to re-derive a prefix from. Applies the same check
    /// <see cref="FromReport(string?, bool, out int)"/> applies inside its assembler loop — severity ERROR, the
    /// message the deadlock marker with its severity prefix already stripped by the entry's own field split,
    /// and a non-empty DETAIL — so an entry that is not a deadlock report (an ordinary ERROR row the csvlog
    /// tail also carries) is null here exactly as it is null there. Null does not mean a bad shape: most
    /// entries the csvlog tail yields are not deadlocks at all.
    /// </summary>
    public static ParsedDeadlock? FromEntry(PgLogEntry entry)
    {
        if (entry.Severity != "ERROR"
            || entry.Message.TrimEnd() != "deadlock detected"
            || string.IsNullOrWhiteSpace(entry.Detail)
            || IsRaiseShaped(entry))
        {
            return null;
        }

        return BuildFromEntry(entry);
    }

    private static ParsedDeadlock? BuildFromEntry(PgLogEntry entry)
    {
        /* The queries normalized before anything is read out of the block (#4005): the victim's statement is
           then the graph's own, and the identity is over text a reader of the row can see. Newlines stay: they
           separate the edges from the statements and are inside the statements too. */
        var graph = NormalizeGraph(entry.Detail, entry.DetailComplete)!.TrimEnd('\n');

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
        statements.TryGetValue(entry.Pid, out var victimStatement);

        return new ParsedDeadlock(
            OccurredAtUtc: entry.OccurredAtUtc,
            VictimPid: entry.Pid,
            ParticipantCount: participants,
            DeadlockHash: IdentityOf(entry.OccurredAtUtc, graph),
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
    /// route receives log TEXT and runs no SQL. The setting is read on the <c>pg_read_file</c> route since #4046,
    /// but only to decide what a line this check rejects MEANS (<see cref="IsUtcLogTimezoneSetting"/>), never
    /// which lines are accepted.</para>
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
    /// Whether a target's <c>log_timezone</c> setting, as <c>current_setting('log_timezone')</c> returns it, stamps
    /// EVERY line at a zero offset, all year (#4046). Then a line in another zone cannot be the server's own, and the
    /// readers skip and count it instead of refusing the target.
    ///
    /// <para><b>Why it matters.</b> PostgreSQL renders <c>%u</c> and <c>%d</c> into <c>log_line_prefix</c> from the
    /// startup packet, before authentication and unescaped, so any client that reaches the port can put a newline
    /// in a role name and write a whole line, stamp and zone included, with one failed login. Before this, one such
    /// line in another zone refused every read of the target while it stayed in the tail.</para>
    ///
    /// <para><b>An allowlist of the zero-offset zones by name, never "the offset now".</b> <c>Europe/London</c> is at
    /// zero all winter and writes genuine <c>BST</c> lines all summer; reading its current offset would skip real
    /// lines as planted. Every name here renders <c>UTC</c> or <c>GMT</c> (<c>UCT</c> under old tz data), each of
    /// which <see cref="IsZeroOffsetLogZone"/> accepts, so a genuine line is never the one skipped. Anything else,
    /// a POSIX spec like <c>UTC0</c> included, is not UTC here and keeps the refusal, which is the misconfiguration
    /// that refusal exists for. Compared ignoring case, since PostgreSQL resolves zone names that way.</para>
    ///
    /// <para><b>A majority rule was rejected</b> (refuse when most lines disagree): a client can flood failed
    /// logins to outnumber the genuine lines on a quiet server.</para>
    /// </summary>
    public static bool IsUtcLogTimezoneSetting(string? setting) =>
        !string.IsNullOrWhiteSpace(setting) && s_utcLogTimezoneSettings.Contains(setting.Trim());

    private static readonly HashSet<string> s_utcLogTimezoneSettings = new(StringComparer.OrdinalIgnoreCase)
    {
        "UTC", "Etc/UTC", "UCT", "Etc/UCT", "Universal", "Etc/Universal", "Zulu", "Etc/Zulu",
        "GMT", "Etc/GMT", "GMT0", "Etc/GMT0", "GMT+0", "Etc/GMT+0", "GMT-0", "Etc/GMT-0", "Greenwich", "Etc/Greenwich",
    };

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
    /// Identity for a report (#4005): <see cref="HashOf"/> over its timestamp and its NORMALIZED graph, which is
    /// what a reader of the stored row sees. The same report reaches this more than once on either transport
    /// and must be stored once, though not for the same reason: the <c>pg_read_file</c> route re-reads a
    /// bounded TAIL on a schedule, so a report inside the window arrives every cycle, while the consume-once
    /// RDS route re-offers only a window whose write did not land (#3008) or whose in-process marker a restart
    /// discarded. See the <c>DeadlockHash</c> parameter above.
    ///
    /// <para><b>Over the normalized graph, never the raw one.</b> An unkeyed hash of text holding a literal is
    /// a test for that literal (#4004): a reader who sees the normalized graph can rebuild everything else the
    /// hash covered, enumerate the values a <c>?</c> could have been, and check each offline. Over what the
    /// reader already sees, the hash tells them nothing more.</para>
    ///
    /// <para><b>With the timestamp</b>, because normalizing makes two different deadlocks read alike when only
    /// their literals differ, the same pids included under a connection pool. Two sightings of one report
    /// carry the same timestamp, written once on its log line. It also keeps this hash from ever equalling
    /// <see cref="HashOf"/> over the graph alone, which is how a row stored before #4005, whose hash is over its
    /// raw graph, is told apart in the store (<see cref="RawGraphHashSql"/>).</para>
    /// </summary>
    public static string IdentityOf(DateTime occurredAtUtc, string graph) =>
        HashOf(occurredAtUtc.ToString("yyyy-MM-dd HH:mm:ss.ffffff", CultureInfo.InvariantCulture) + "\n" + graph);

    /// <summary>
    /// SHA-256 over <paramref name="text"/>, the first 16 bytes as upper-case hex. Before #4005 a report's
    /// identity was this over its raw graph.
    /// </summary>
    public static string HashOf(string text)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return Convert.ToHexString(bytes)[..32];
    }

    /// <summary>
    /// A SQL predicate, true for a stored row whose <c>deadlock_hash</c> is <see cref="HashOf"/> over its
    /// <c>graph_text</c>: a row stored before #4005, when the graph was stored raw and hashed raw. That hash is
    /// a test for the literals a read normalizes away (#4004), so a read never returns it and never finds a row
    /// by it; <see cref="ReportIdentity"/> names such a row instead. A row stored since carries
    /// <see cref="IdentityOf"/>, which covers the timestamp as well and so never satisfies this.
    /// </summary>
    public static string RawGraphHashSql(string hashColumn, string graphColumn) =>
        $"(upper(left(encode(sha256(convert_to({graphColumn}, 'UTF8')), 'hex'), 32)) = {hashColumn})";

    /// <summary>A report named by its timestamp and victim pid, which every read already shows (#4005): what a
    /// read returns in place of the hash of a row <see cref="RawGraphHashSql"/> is true for, and what an analysis
    /// finding stored before #4005 names its exemplar by, since its stored hash may be one. The detail read finds
    /// the report by it whichever build stored the row.</summary>
    public static string ReportIdentity(DateTime occurredAt, int victimPid) =>
        ReportIdentityPrefix + occurredAt.ToString("yyyyMMdd'T'HHmmss.ffffff", CultureInfo.InvariantCulture)
        + "-" + victimPid.ToString(CultureInfo.InvariantCulture);

    /// <summary>The report a <see cref="ReportIdentity"/> names.</summary>
    public static bool TryParseReportIdentity(string? identity, out DateTime occurredAt, out int victimPid)
    {
        occurredAt = default;
        victimPid = 0;
        var match = identity is null ? Match.Empty : s_reportIdentity.Match(identity.Trim());

        return match.Success
            && DateTime.TryParseExact(match.Groups["at"].Value, "yyyyMMdd'T'HHmmss.ffffff", CultureInfo.InvariantCulture, DateTimeStyles.None, out occurredAt)
            && int.TryParse(match.Groups["pid"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out victimPid);
    }

    private const string ReportIdentityPrefix = "at-";

    private static readonly Regex s_reportIdentity = new(
        "^" + ReportIdentityPrefix + @"(?<at>[0-9]{8}T[0-9]{6}\.[0-9]{6})-(?<pid>[0-9]{1,10})$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A deadlock report's DETAIL with each participant's query normalized (#4005), as
    /// <see cref="PgLogTextRedactor.RedactDetail"/> reads it for a stored log event (#3944): every literal a
    /// <c>?</c>, a query that cannot be read to its end withheld, the wait-for lines kept as written.
    /// <paramref name="complete"/> is the entry's proof the DETAIL is whole; a stored graph carries none and is
    /// read the fail-closed way, which a graph this build wrote reads the same under, because its withheld
    /// queries read to their end. Null in, null out; idempotent.
    /// </summary>
    public static string? NormalizeGraph(string? graph, bool complete = false) =>
        PgLogTextRedactor.RedactDetail(graph, complete);

    /// <summary>
    /// How much of a stored statement or graph a read needs to normalize it (#4005): one character past the
    /// longest SQL-bearing field the lexer reads at all. A longer one is withheld whole, so reading more serves
    /// nothing, and a read that cuts first and normalizes after withholds a statement whose cut landed inside a
    /// literal.
    /// </summary>
    public const int NormalizeReadCap = PgLogTextRedactor.MaxSqlFieldLength + 1;

    /// <summary>
    /// A stored victim statement normalized the way <see cref="NormalizeGraph"/> normalizes it inside the graph
    /// (#4005): withheld when it cannot be read to its end or is too long to read. Null in, null out;
    /// idempotent.
    /// </summary>
    public static string? NormalizeStatement(string? statement) =>
        string.IsNullOrEmpty(statement) ? statement : PgLogTextRedactor.MaskQuery(statement);
}
