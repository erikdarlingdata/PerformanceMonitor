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
/// <para><b>Unless the target's own setting says the line is not the server's (#4046).</b> <c>%u</c> and <c>%d</c>
/// let any client that reaches the port write a whole line into the log with one failed login, so the refusal
/// above let one planted line blind a target. A caller that read <c>log_timezone</c> in the statement that read the
/// text passes whether it renders UTC; if it does, a line in another zone is skipped and counted instead. The
/// decision rests on the setting, never on how many lines disagree, which a flood of failed logins could
/// decide.</para>
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

    /// <summary>Every label this reader knows, the severities and the companion fields alike, as the same
    /// alternation <see cref="s_prefixLine"/>'s own label group uses. Shared with <see cref="IsForgedLabel"/>
    /// so the "is this a label" shape is written once.</summary>
    private const string LabelAlternation =
        "LOG|INFO|NOTICE|WARNING|ERROR|FATAL|PANIC|DEBUG[1-5]?|DETAIL|HINT|STATEMENT|CONTEXT|QUERY|LOCATION";

    private static readonly Regex s_prefixLine = new(
        @"^(?<stamp>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?:\.\d+)?) "
        + @"(?:(?<zone>[^ \n]+) \[(?<pid>\d+)\]|(?<zone>[^ :\n]+):(?<mid>" + s_prefixRun + @")\[(?<pid>\d+)\]"
        + @"|(?<zone>[^ :\[\n]+) (?<mid>" + s_prefixGapBeforePid + @")\[(?<pid>\d+)\])"
        + @"(?<rest>" + s_prefixRun + ")"
        + @"(?<![A-Z_])(?<label>" + LabelAlternation + @"):  (?<text>.*)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The bounded, severity-disagreement forgery rule (#4501, replacing the plain "text opens with a label"
    /// check #4426 v17 shipped first). Finds the NEXT known label anywhere in <c>text</c> — not only at its
    /// start — so a forged label can hide behind extra <c>application_name</c> characters before the real one
    /// (K2, K6). Used by <see cref="IsForgedLabel"/>.
    /// </summary>
    private static readonly Regex s_nextLabel = new(
        @"(?<![A-Z_])(?<label>" + LabelAlternation + @"):  ",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Whether the matched label (<paramref name="matchedLabel"/>, the text already consumed as label plus
    /// its trailing <c>":  "</c>) is a forgery planted in front of the line's REAL label, found by scanning
    /// <paramref name="text"/> for the next known label (#4501, replacing #4426 v17's plain "text opens with a
    /// label" check with the bounded, severity-disagreement rule the security review specified).
    ///
    /// <para><b>The window.</b> <c>window = matchedLabel + ":  " + text[..M2.Index)</c> — everything from the
    /// forged label's own start up to (not including) the real label M2. That whole span sits inside
    /// whatever client-controlled field rendered it (<c>application_name</c> under the default managed
    /// prefix, or <c>%u</c>/<c>%d</c> under a prefix that puts a field after the pid), so it is bounded by
    /// that field's own limit: PostgreSQL caps <c>application_name</c>, <c>%u</c> and <c>%d</c> at
    /// NAMEDATALEN−1 = 63 bytes of printable ASCII 0x20–0x7E.</para>
    ///
    /// <para><b>Refuse the line iff ALL of:</b></para>
    /// <list type="number">
    /// <item>the window is at most 63 bytes plus <paramref name="separator"/>'s length (or, with no separator
    /// check — <paramref name="separator"/> is null — at most 63 bytes with no allowance for one);</item>
    /// <item>every character in the window is printable ASCII 0x20–0x7E;</item>
    /// <item>(only when <paramref name="separator"/> is not null) the window ends with it — the literal text
    /// the collected <c>log_line_prefix</c> renders right before the label, so a client field's own value
    /// cannot masquerade as the prefix's punctuation;</item>
    /// <item><paramref name="matchedLabel"/> and M2's label are DIFFERENT strings — same-severity pairs (a
    /// genuine <c>RAISE EXCEPTION 'ERROR:  x'</c> rendering <c>ERROR:  ERROR:  x</c>) read the same either
    /// way, so nothing is lost by keeping them (#4501).</item>
    /// </list>
    ///
    /// <para><paramref name="applyCheck"/> false skips the whole rule (never refuses): the collected prefix
    /// puts no client-controlled field after the pid, so there is no forgery surface to guard (#4501 —
    /// the RDS default <c>%t:%r:%u@%d:[%p]:</c> and a bare <c>%m [%p] </c> both land here).</para>
    ///
    /// <para>A refused match is treated exactly as a line the prefix regex never matched at all: dropped,
    /// ending the open entry, never opening a new one.</para>
    /// </summary>
    private static bool IsForgedLabel(string matchedLabel, string text, bool applyCheck, string? separator)
    {
        if (!applyCheck)
        {
            return false;
        }

        var m2 = s_nextLabel.Match(text);

        if (!m2.Success)
        {
            return false;
        }

        var beforeLabel = matchedLabel.Length + 3 /* ":  " */ + m2.Index;
        var sepLen = separator?.Length ?? 0;

        if (beforeLabel > 63 + sepLen)
        {
            return false;
        }

        for (var i = 0; i < m2.Index; i++)
        {
            if (text[i] < '\x20' || text[i] > '\x7E')
            {
                return false;
            }
        }

        if (separator is not null)
        {
            var window = matchedLabel + ":  " + text[..m2.Index];

            if (!window.EndsWith(separator, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return !string.Equals(matchedLabel, m2.Groups["label"].Value, StringComparison.Ordinal);
    }


    /* %u@%d anywhere in the prefix's non-pid text, before the pid or after it. Both halves required: a background
       process renders `@` alone under that prefix and means neither. */
    private static readonly Regex s_userAtDatabase = new(
        @"(?<![\w@.-])(?<user>[A-Za-z_][\w$.-]*)@(?<db>[A-Za-z_][\w$.-]*)(?![\w@])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* %e: five characters, digits and capitals, at least one digit (every SQLSTATE class or subclass
       carries one), not glued to an identifier character. The digit requirement is what keeps a
       five-letter upper-case host name in %r from reading as an error code. Nor is it %r's port: %r renders
       `host(port)`, and a five-digit port in its parentheses read as the SQLSTATE of every event under the RDS
       default `%t:%r:%u@%d:[%p]:` (#4047 review), an ephemeral port often in classes 53-58 (resources, operator
       intervention, system errors). A bare five-digit %l, %x or %v still reads as one; only the target's own
       log_line_prefix could tell those from %e (#4046). */
    private static readonly Regex s_sqlState = new(
        @"(?<![A-Za-z0-9])(?!(?<=\()[0-9A-Z]{5}\))(?=[0-9A-Z]{0,4}\d)(?<state>[0-9A-Z]{5})(?![A-Za-z0-9])",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly HashSet<string> s_primaryLabels = new(StringComparer.Ordinal)
    {
        "LOG", "INFO", "NOTICE", "WARNING", "ERROR", "FATAL", "PANIC",
        "DEBUG", "DEBUG1", "DEBUG2", "DEBUG3", "DEBUG4", "DEBUG5",
    };

    /// <summary>
    /// Every complete entry in the slab, in log order.
    ///
    /// <para><b>Prefix unknown (#4501).</b> This overload takes no <c>log_line_prefix</c>, so the bounded
    /// forgery rule (<see cref="IsForgedLabel"/>) runs with NO separator check — the fallback #4501
    /// calls for when a caller cannot state the prefix: it shrinks what the rule KEEPS rather than
    /// what it refuses, since an unknown prefix could put anything before the label. A caller that has read
    /// the target's own <c>log_line_prefix</c> should prefer <see cref="Assemble(string?, bool, string?, out int)"/>
    /// instead, which applies the rule only when a client field sits between the pid and the label, using the
    /// prefix's own separator.</para>
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">A primary line's prefix zone is not a zero-offset
    /// one. Thrown from inside the walk, abandoning every entry assembled so far (#2993).</exception>
    public static List<PgLogEntry> Assemble(string? logBody) => Assemble(logBody, logTimezoneIsUtc: false, out _);

    /// <summary>
    /// <see cref="Assemble(string?, bool, out int)"/> for a caller that also read the target's own
    /// <c>log_line_prefix</c> in the statement that returned <paramref name="logBody"/> (#4501, plumbed the
    /// same way <paramref name="logTimezoneIsUtc"/> is): <paramref name="logLinePrefix"/> null means the
    /// setting was not collected — the forgery rule still runs, but with no separator check, per the #4501
    /// fallback. See <see cref="IsForgedLabel"/> and <see cref="ForgeryCheckFor"/>.
    /// </summary>
    public static List<PgLogEntry> Assemble(
        string? logBody, bool logTimezoneIsUtc, string? logLinePrefix, out int foreignZoneLines)
    {
        var (applyCheck, separator) = ForgeryCheckFor(logLinePrefix);
        return Assemble(logBody, logTimezoneIsUtc, applyCheck, separator, out foreignZoneLines);
    }

    /// <summary>
    /// Decides, from a collected <c>log_line_prefix</c> (#4501), whether the forgery rule
    /// applies at all and what separator it requires.
    ///
    /// <list type="bullet">
    /// <item><paramref name="logLinePrefix"/> is null (not collected): apply the rule with NO separator
    /// check — the fallback shrinks what it keeps rather than what it refuses, since an unknown prefix could
    /// put anything before the label.</item>
    /// <item>the prefix has no client-controlled field (<c>%a</c>, <c>%u</c> or <c>%d</c>) between <c>%p</c>
    /// and the label: the rule does not apply at all (there is no forgery surface), matching how a bare
    /// <c>%m [%p] </c> read before #4501.</item>
    /// <item>otherwise: apply the rule, with the separator being the prefix's own literal text between that
    /// field and where the label starts — the prefix's tail after its last escape (a space for
    /// <c>'%m [%p] %a '</c>, empty for a field glued straight onto the label).</item>
    /// </list>
    /// </summary>
    internal static (bool ApplyCheck, string? Separator) ForgeryCheckFor(string? logLinePrefix)
    {
        if (logLinePrefix is null)
        {
            return (true, null);
        }

        /* %p is required for this reader to have a pid to anchor on at all; a prefix with no %p is not one
           this reader can reason about, so it is treated the same as "no client field after %p". */
        var pidIndex = logLinePrefix.IndexOf("%p", StringComparison.Ordinal);

        if (pidIndex < 0)
        {
            return (false, null);
        }

        var afterPid = logLinePrefix[(pidIndex + 2)..];
        var fieldMatch = s_clientFieldEscape.Match(afterPid);

        if (!fieldMatch.Success)
        {
            return (false, null);
        }

        /* The prefix's own literal text after the LAST escape of any kind, up to the prefix's end — that is
           what actually renders right before the label on the wire. A prefix that glues the field straight
           to the label (no trailing literal) yields an empty separator, which IsForgedLabel treats as "no
           separator required", the correct reading: there is nothing there to check. */
        var lastEscape = s_anyEscape.Matches(afterPid).Cast<Match>().LastOrDefault();
        var tail = lastEscape is null ? afterPid : afterPid[(lastEscape.Index + lastEscape.Length)..];

        return (true, tail);
    }

    /* %a, %u, %d: the three client-controlled fields (#4501). */
    private static readonly Regex s_clientFieldEscape = new("%[aud]", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* Every log_line_prefix escape (%-something), used to find the LAST one so the separator is only the
       prefix's own trailing literal, not a literal that sits between two escapes earlier in the string. */
    private static readonly Regex s_anyEscape = new("%.", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every complete entry in the slab, in log order, for a caller that read the target's own <c>log_timezone</c>
    /// in the statement that returned <paramref name="logBody"/> (#4046).
    ///
    /// <para><b>When that setting renders UTC</b> (<see cref="PgDeadlockLogParser.IsUtcLogTimezoneSetting"/>), a
    /// line whose zone is not a zero-offset one is not the server's own: a client planted it through <c>%u</c> or
    /// <c>%d</c>, or it predates a change to the setting. It is skipped and counted in
    /// <paramref name="foreignZoneLines"/>, primary or companion, and it ends the open entry the way an unrecognised
    /// line does, so neither it nor the tab lines under it join a genuine entry. Nothing is refused for it: one
    /// planted line used to refuse every read of the target for as long as it stayed in the tail.</para>
    ///
    /// <para><b>Otherwise</b> (false: the setting is another zone, or the route could not read it) this is exactly
    /// <see cref="Assemble(string?)"/>, refusal included. That refusal is for a target whose log really is local
    /// time, where a per-line skip would store a partial history nothing marks as partial (see the type
    /// header).</para>
    ///
    /// <para><b>No <c>log_line_prefix</c> parameter here either (#4501).</b> A caller on this overload has not
    /// read the target's prefix, so this is the same fallback <see cref="Assemble(string?)"/> takes: the
    /// bounded forgery rule runs with NO separator check (<see cref="IsForgedLabel"/>'s condition 3), keeping
    /// only its severity-disagreement condition (4). See <see cref="ForgeryCheckFor"/> for the prefix-aware
    /// overload a caller that has read the prefix should prefer instead.</para>
    /// </summary>
    /// <exception cref="PgLogTimezoneUnsupportedException">Only when <paramref name="logTimezoneIsUtc"/> is false: see
    /// <see cref="Assemble(string?)"/>.</exception>
    public static List<PgLogEntry> Assemble(string? logBody, bool logTimezoneIsUtc, out int foreignZoneLines) =>
        Assemble(logBody, logTimezoneIsUtc, applyForgeryCheck: true, separator: null, out foreignZoneLines);

    /// <summary>The walk itself, parameterised over the #4501 forgery rule (see <see cref="IsForgedLabel"/>)
    /// so both the pre-#4501-shaped overloads (a bare check, the store's default separator) and the
    /// prefix-aware overload above share one implementation.</summary>
    private static List<PgLogEntry> Assemble(
        string? logBody, bool logTimezoneIsUtc, bool applyForgeryCheck, string? separator, out int foreignZoneLines)
    {
        var entries = new List<PgLogEntry>();
        foreignZoneLines = 0;

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

            /* #4501: the matched label is genuine only if it is not a forgery in front of the line's real
               label — see IsForgedLabel's doc for the bounded, severity-disagreement rule. Treated exactly
               as a line the prefix never matched: dropped, closing the open entry, opening none. */
            if (!match.Success
                || IsForgedLabel(match.Groups["label"].Value, match.Groups["text"].Value, applyForgeryCheck, separator))
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

            /* #4046: the setting says this zone is not the server's, so the line is not its own. Skipped and
               counted, and it ends the open entry like an unrecognised line above. */
            if (logTimezoneIsUtc && !PgDeadlockLogParser.IsZeroOffsetLogZone(match.Groups["zone"].Value))
            {
                foreignZoneLines++;

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
               outcome here. Under a UTC log_timezone the walk skips such a line before it gets here (#4046). */
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
