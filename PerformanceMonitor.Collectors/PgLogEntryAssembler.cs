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
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Turns a slab of PostgreSQL <c>stderr</c>-format server log into <see cref="PgLogEntry"/> records
/// (#3601): the LINE ASSEMBLY every log family used to do for itself, done once.
///
/// <para><b>What an entry is, in PostgreSQL's own terms.</b> The server writes one message as a PRIMARY
/// line — <c>&lt;prefix&gt;SEVERITY:  text</c> — followed by zero or more COMPANION lines for the same
/// message, each with its own prefix and a field label instead of a severity: <c>DETAIL:</c>,
/// <c>HINT:</c>, <c>STATEMENT:</c>, <c>CONTEXT:</c>, <c>QUERY:</c>, <c>LOCATION:</c>. Any of those lines
/// may continue onto following lines, and a continuation is marked by a leading TAB. A statement with
/// newlines in it arrives as a <c>STATEMENT:</c> line plus tab-indented continuations; a deadlock's
/// <c>DETAIL:</c> is one line plus a tab-indented graph. The entry ends at the next primary line.</para>
///
/// <para><b>Both prefix families, inherited from <see cref="PgDeadlockLogParser"/> and for its reasons.</b>
/// PostgreSQL's default <c>%m [%p] </c> runs the zone to a SPACE; the managed parameter-group default
/// <c>%t:%r:%u@%d:[%p]:</c> runs it to a COLON with <c>%r</c> and <c>%u@%d</c> between the zone and the
/// pid. The two are ALTERNATIVES in one pattern rather than a relaxed union, because a numeric offset zone
/// carries colons of its own and a union reads a UTC server as non-UTC on an IPv6 <c>%r</c> — the deadlock
/// parser's header carries the worked case. <c>%Q</c> glues the query id to the label with no separator
/// (<c>[1549] 322048460535975151ERROR:</c>), so the label is found by a LAZY run to the first
/// <c>LABEL:  </c>, never by requiring whitespace before it. The space family also reads fields between the zone
/// and the pid (#4041), as <c>%m %u@%d [%p] </c> renders them, through a gap that stops at the first bracket.</para>
///
/// <para><b>Only <c>stderr</c> format.</b> <c>csvlog</c> and <c>jsonlog</c> are different formats with the
/// same content, and neither existing log reader handles them; a target whose <c>log_destination</c> is
/// one of those yields no entries here and reads as quiet. The plan-capture readiness collector already
/// reports the log format as a facet, and #3607's settings audit is where that becomes a named finding
/// for this pipeline too.</para>
///
/// <para><b>Timezone: checked once, refused whole (#2993).</b> Every primary line's zone token goes through
/// <see cref="PgDeadlockLogParser.IsZeroOffsetLogZone"/>, and a non-zero one throws
/// <see cref="PgLogTimezoneUnsupportedException"/> out of the whole assembly — the same exception the
/// deadlock and plan routes throw, classified by the runner the same way, so an operator sees ONE sentence
/// about <c>log_timezone</c> whichever log reader met it first. Not skipped per line, because a line under
/// a non-UTC prefix parses PERFECTLY and lands in the wrong hour with nothing disagreeing; the deadlock
/// parser's <c>FromReport</c> remarks argue the whole-read refusal over the partial history.</para>
///
/// <para><b>Window edges.</b> The self-hosted tail starts at an arbitrary byte and the RDS chunk ends at
/// one. Lines before the first recognisable prefix are the cut head and are dropped; a final line with no
/// trailing newline is a cut tail and is dropped with the entry it belongs to, so a half-written primary
/// line is never stored as a shorter event. An entry cut BETWEEN its primary line and a companion is not
/// detectable and is stored as it arrived — the next overlapping read on the self-hosted route stores it
/// whole under a different <see cref="PgLogEntry.RawText"/> hash; on the consume-once RDS route it is
/// not re-offered (#3009 describes the same exposure for deadlocks).</para>
/// </summary>
public static class PgLogEntryAssembler
{
    /// <summary>The field labels a catalogue of PostgreSQL 18's writes without the two spaces after the colon that
    /// elog.c's English labels end with, as each is written up to its colon (#3996's round-2 review): Turkish
    /// <c>AYRINTI:</c>, <c>İPUCU:</c>, <c>SORGU:</c> and <c>ORTAM:</c> (DETAIL, HINT, QUERY, CONTEXT), Korean
    /// <c>쿼리:</c> (QUERY) and French <c>PILE D'APPEL :</c> (BACKTRACE). Each ends a label whatever follows its
    /// colon. PgLogCatalogueShapeTests fails when the bundled runtime's catalogues write another.</summary>
    public static readonly IReadOnlyList<string> UnpaddedLabels =
        ["AYRINTI:", "İPUCU:", "SORGU:", "ORTAM:", "쿼리:", "PILE D'APPEL :"];

    /* The prefix line: stamp, zone-and-pid in either family, whatever else the prefix carried, then the
       label. `rest` is lazy so the label is the FIRST `LABEL:  ` after the pid, which is what %Q's glued
       query id requires. The companion labels are in the same alternation as the severities because a
       companion line carries the same prefix and is told apart only by its label.

       `rest` never runs past a label of ANY language (#3996's review). Under a translated lc_messages the
       line's own label is one this does not know (`SENTENCIA:  `, `ANWEISUNG:  `), and a lazy run past it found
       `ERROR:  ` inside the statement's literal and started a new event from the middle of it. So `rest` stops at
       what a label ends with in every catalogue PostgreSQL 18 ships: a colon and two spaces, one of the labels a
       catalogue writes without them (UnpaddedLabels) whatever follows its colon, or a colon straight after a
       non-ASCII letter or three capitals and before text. A prefix's own colons pass: a time's, `]:` before the
       label, `: ` separators, an IPv6 client, pgAdmin's `DB:postgres`. A label glued to capitals (`PG_CATALOG:`)
       is not one. A line whose label is not this reader's opens nothing: fail closed. The managed family's `mid`
       is held to the same run, because it is lazy too: refused at the real pid, it slid on to a `[1]` inside the
       statement's literal and read that as the pid.

       The unpadded labels are named because their text need not start with a letter (#3996's round-2 review):
       PL/pgSQL's QUERY companion is the function's body, which opens with a space after `AS $$ BEGIN`, so
       `SORGU: BEGIN ... 'ERROR:  ...'` passed the shape rule, the run crossed it, and the ERROR inside the body
       opened an event carrying the body's password. The one thing after a named label's colon that does not make
       it a label is a pid in brackets: the managed family writes `%u@%d:[%p]:`, and a database or role whose name
       ends in one (`검색쿼리`) would otherwise lose every line.

       A THIRD alternative (#4041): the space family with fields between the zone and the pid, as
       '%m %u@%d [%p] ' renders them (`UTC app@db [5555] ERROR:  `). Its gap is s_prefixRun's step with '[' taken
       out of the class, so it holds to both rules at once. It cannot cross a label, by the run's own boundaries, so
       it never leaves the line's prefix for its text, which is where a forged bracket or label has to live. And it
       cannot consume a bracket, so the pid is the FIRST bracket after the zone's space and nothing can backtrack
       past it to a later one (plan capture's #4008 rule). `rest` after it is the unchanged run, so from the zone to
       the label the whole path crosses no label anywhere, and the label read is the line's first.

       Tried LAST, so a line the first two read is read exactly as before: the engine takes the first alternative
       that completes the line, and the old space family is this one with an empty gap.

       Its zone holds no colon and no bracket, so this alternative reads only lines whose first token is
       colon-free, and there that token is the zone. A first token with a colon in it belongs to the managed family.
       Allowed a colon, this zone ran over a managed line's prefix to its first space: tried first, it read a
       managed line whose database holds a space (`UTC:...:app@my db:[5555]:`) with the zone `UTC:...:app@my`; tried
       last, it still ran over a translated label (`UTC:...:[4503]:ANWEISUNG:`), the gap found `[1]` inside the
       statement, and either way the zone check refused every read of the target. A numeric zone with a colon
       (`+05:30`) under this prefix reads through the managed family instead, up to its first colon, with the same
       verdict; the deadlock parser's header argues that trade. */
    private static readonly string s_prefixRunStep =
        @"(?!:  )(?!(?<=" + string.Join('|', UnpaddedLabels.Select(l => Regex.Escape(l[..^1]))) + @"):(?!\[[0-9]+\]))"
        + @"(?!(?<=[\p{L}-[\x00-\x7F]]|[A-Z]{3}):[^0-9\[\s:])";

    private static readonly string s_prefixRun = @"(?:" + s_prefixRunStep + @"[^\n])*?";

    private static readonly string s_prefixGapBeforePid = @"(?:" + s_prefixRunStep + @"[^\[\n])*?";

    private static readonly Regex s_prefixLine = new(
        @"^(?<stamp>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?) "
        + @"(?:(?<zone>[^ \n]+) \[(?<pid>\d+)\]|(?<zone>[^ :\n]+):(?<mid>" + s_prefixRun + @")\[(?<pid>\d+)\]"
        + @"|(?<zone>[^ :\[\n]+) (?<mid>" + s_prefixGapBeforePid + @")\[(?<pid>\d+)\])"
        + @"(?<rest>" + s_prefixRun + ")"
        + @"(?<![A-Z_])(?<label>LOG|INFO|NOTICE|WARNING|ERROR|FATAL|PANIC|DEBUG[1-5]?|DETAIL|HINT|STATEMENT|CONTEXT|QUERY|LOCATION):  ?(?<text>.*)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* %u@%d anywhere in the prefix's non-pid text, before the pid or after it. Both halves required: a background
       process renders `@` alone under that prefix and means neither. */
    private static readonly Regex s_userAtDatabase = new(
        @"(?<![\w@.-])(?<user>[A-Za-z_][\w$.-]*)@(?<db>[A-Za-z_][\w$.-]*)(?![\w@])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* %e: five characters, digits and capitals, at least one digit (every SQLSTATE class or subclass
       carries one), not glued to an identifier character. The digit requirement is what keeps a
       five-letter upper-case host name in %r from reading as an error code. */
    private static readonly Regex s_sqlState = new(
        @"(?<![A-Za-z0-9])(?=[0-9A-Z]{0,4}\d)(?<state>[0-9A-Z]{5})(?![A-Za-z0-9])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly HashSet<string> s_primaryLabels = new(StringComparer.Ordinal)
    {
        "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL", "PANIC",
        "DEBUG", "DEBUG1", "DEBUG2", "DEBUG3", "DEBUG4", "DEBUG5",
    };

    /// <summary>
    /// Every complete entry in the slab, in log order.
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">A primary line's prefix zone is not a zero-offset
    /// one. Thrown from inside the walk, abandoning every entry assembled so far (#2993).</exception>
    public static List<PgLogEntry> Assemble(string? logBody)
    {
        var entries = new List<PgLogEntry>();

        if (string.IsNullOrEmpty(logBody))
        {
            return entries;
        }

        /* A slab that does not end in a newline ends mid-line. That last fragment, and the entry it
           belongs to, are the cut tail and are not stored: see the type header. */
        var endsClean = logBody.EndsWith('\n');
        var lines = logBody.Split('\n');
        var lastIndex = lines.Length - 1;

        /* Split leaves one empty string after a trailing newline; treat that as the end rather than as an
           orphan line. */
        if (endsClean)
        {
            lastIndex--;
        }

        Builder? current = null;

        for (var i = 0; i <= lastIndex; i++)
        {
            var line = lines[i].TrimEnd('\r');

            if (line.Length > 0 && line[0] == '\t')
            {
                /* A continuation of whatever field is open. Before the first prefix line it is part of the
                   cut head and there is nothing to attach it to. */
                current?.Continue(line[1..]);
                continue;
            }

            var match = s_prefixLine.Match(line);

            if (!match.Success)
            {
                /* Not a prefix line and not a continuation: the cut head, a line the server wrote to stderr
                   outside its own format (a loader's chatter, a crash dump), or a line whose label this reader
                   does not know, as under a translated lc_messages (#3996's review). Nothing to do with it, nor
                   with the tab lines under it, and it ends the open entry: a message's lines are written
                   together, so what follows belongs to this line's message, not the entry above (a German
                   `FEHLER:` line's untranslated `DETAIL:` would otherwise join the entry before it). */
                if (current is not null)
                {
                    entries.Add(current.Build());
                    current = null;
                }

                continue;
            }

            var label = match.Groups["label"].Value;

            if (s_primaryLabels.Contains(label))
            {
                if (current is not null)
                {
                    entries.Add(current.Build());
                }

                current = Builder.Start(match, line);
                continue;
            }

            /* A companion line. It belongs to the open entry only if the same backend wrote it; interleaved
               output from another backend between a primary line and its DETAIL is rare but real, and
               attaching it would put one session's statement under another session's error. */
            if (current is not null && current.Pid == match.Groups["pid"].Value)
            {
                current.Companion(label, match.Groups["text"].Value, line);
            }
            else
            {
                /* ...and neither do the tab lines under that backend's line (#3944's review): they are the rest of
                   ITS field, another session's multi-line statement say, and the field open here would otherwise
                   take them in, now that the columns keep their prose as written. */
                current?.CloseField();
            }
        }

        if (current is not null && endsClean)
        {
            entries.Add(current.Build());
        }

        return entries;
    }

    /// <summary>The unfinished entry, accumulating companions and continuations until the next primary line.</summary>
    private sealed class Builder
    {
        private readonly string _stamp;
        private readonly string _zone;
        private readonly DateTime _occurredAtUtc;
        private readonly string _prefixRest;
        private readonly string _severity;
        private readonly StringBuilder _raw = new();
        private readonly StringBuilder _message = new();
        private StringBuilder? _detail;
        private StringBuilder? _hint;
        private StringBuilder? _statement;
        private StringBuilder? _context;
        private StringBuilder? _open;

        /* Whether the DETAIL is proven whole (#3996's review): another companion followed it, and nothing cut
           into its lines on the way. */
        private bool _detailFollowed;
        private bool _detailInterrupted;

        public string Pid { get; }

        private Builder(Match match, string line)
        {
            _stamp = match.Groups["stamp"].Value;
            _zone = match.Groups["zone"].Value;
            Pid = match.Groups["pid"].Value;
            _severity = match.Groups["label"].Value;
            /* The fields before the pid and after it, a space between so the two never glue into one token
               (`%u@%d[%p]%e` would read the database as `app_db28P01`). */
            _prefixRest = (match.Groups["mid"].Value + " " + match.Groups["rest"].Value).Trim();

            /* THROWN rather than skipped, and it is the one intolerant thing in the assembler — see the
               type header and PgDeadlockLogParser.FromReport for why a per-line skip is the silent-wrong
               outcome here. */
            if (!PgDeadlockLogParser.IsZeroOffsetLogZone(_zone))
            {
                throw new PgLogTimezoneUnsupportedException(_zone);
            }

            if (!DateTime.TryParse(
                    _stamp,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
                    out _occurredAtUtc))
            {
                /* Unreachable through the pattern, which admits only digits in the stamp's shape; kept so a
                   future loosening of the pattern cannot store DateTime.MinValue as an occurrence. */
                _occurredAtUtc = DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);
            }

            _message.Append(match.Groups["text"].Value);
            _open = _message;
            _raw.Append(line).Append('\n');
        }

        public static Builder Start(Match match, string line) => new(match, line);

        public void Continue(string text)
        {
            _open?.Append('\n').Append(text);
            _raw.Append('\t').Append(text).Append('\n');
        }

        /// <summary>Stops tab-continuation lines from joining the field last opened: the line they continue was
        /// not this entry's. A DETAIL closed this way may have lost the rest of its lines.</summary>
        public void CloseField()
        {
            _detailInterrupted |= _open is not null && ReferenceEquals(_open, _detail);
            _open = null;
        }

        public void Companion(string label, string text, string line)
        {
            _raw.Append(line).Append('\n');
            _detailFollowed |= _detail is not null && label != "DETAIL";

            switch (label)
            {
                case "DETAIL":
                    _detail = Open(_detail, text);
                    break;
                case "HINT":
                    _hint = Open(_hint, text);
                    break;
                case "STATEMENT":
                    _statement = Open(_statement, text);
                    break;
                case "CONTEXT":
                    _context = Open(_context, text);
                    break;
                default:
                    /* QUERY and LOCATION: kept in RawText for identity, not as a field. QUERY is the
                       internal query of a PL function (literals and all) and LOCATION is a source-code
                       reference; neither has a column, and neither is worth one until a family asks. */
                    _open = null;
                    break;
            }
        }

        private StringBuilder Open(StringBuilder? field, string text)
        {
            /* A second line with the same label is appended rather than replacing: PostgreSQL does not
               write two DETAILs for one message, but a parser downstream is better served by seeing both
               than by silently keeping one. */
            field ??= new StringBuilder();

            if (field.Length > 0)
            {
                field.Append('\n');
            }

            field.Append(text);
            _open = field;
            return field;
        }

        public PgLogEntry Build()
        {
            var userMatch = s_userAtDatabase.Match(_prefixRest);
            var stateMatch = s_sqlState.Match(_prefixRest);

            return new PgLogEntry(
                TimestampText: _stamp,
                ZoneText: _zone,
                OccurredAtUtc: _occurredAtUtc,
                Pid: int.TryParse(Pid, NumberStyles.Integer, CultureInfo.InvariantCulture, out var pid) ? pid : 0,
                PrefixRest: _prefixRest,
                Severity: _severity,
                Message: _message.ToString(),
                Detail: _detail?.ToString(),
                Hint: _hint?.ToString(),
                Statement: _statement?.ToString(),
                Context: _context?.ToString(),
                UserName: userMatch.Success ? userMatch.Groups["user"].Value : null,
                DatabaseName: userMatch.Success ? userMatch.Groups["db"].Value : null,
                SqlState: stateMatch.Success ? stateMatch.Groups["state"].Value : null,
                RawText: _raw.ToString(),
                DetailComplete: _detailFollowed && !_detailInterrupted);
        }
    }
}
