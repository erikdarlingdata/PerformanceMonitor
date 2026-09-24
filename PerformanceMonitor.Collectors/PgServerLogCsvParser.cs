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
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Parses <c>csvlog</c>-format PostgreSQL server log text into <see cref="PgLogEntry"/> records (#4053, part a1).
///
/// <para><b>Why this format exists.</b> Under <c>stderr</c> format a failed login's role or database name
/// lands in the log UNESCAPED — <see cref="PgLogEntryAssembler"/> reads it off a regex prefix — so a
/// newline planted in either field forges a whole extra log line, primary or companion, that reads as the
/// server's own. Under <c>csvlog</c> every field PostgreSQL writes is RFC 4180 quoted, so a planted newline
/// stays inside its own field: it never starts a new record. This parser is the csvlog twin of
/// <see cref="PgLogEntryAssembler.Assemble(string?)"/> — same target shape, same
/// <see cref="PgLogEntry.OccurredAtUtc"/> rule — over a CSV record instead of a regex-matched prefix line.
/// Wiring it into the tail and <c>pg_log_events</c> is a later lane (a1b); jsonlog is part a2.</para>
///
/// <para><b>Column count, verified empirically (#4053's brief), not guessed.</b> PostgreSQL 14 through 18
/// both write 26 columns per csvlog record: <c>log_time, user_name, database_name, process_id,
/// connection_from, session_id, session_line_num, command_tag, session_start_time,
/// virtual_transaction_id, transaction_id, error_severity, sql_state_code, message, detail, hint,
/// internal_query, internal_query_pos, context, query, query_pos, location, application_name,
/// backend_type, leader_pid, query_id</c>. A record whose field count is not 26 is rejected and counted in
/// <see cref="Parse"/>'s <c>recordsDiscarded</c> — there is currently one supported shape, but the check is
/// a count, not an equality, so a future confirmed shape (a different supported version) can be added
/// without changing the reject path.</para>
///
/// <para><b>The tail starts mid-file, so record boundaries are found from the END.</b> Reading forward from an
/// arbitrary offset cannot recover quote parity: a window that starts inside a quoted field (most of a csvlog's
/// bytes are quoted text) inverts every quote after it, and the rest of the body glues into one record that fails
/// the shape check. The body's end is a record boundary (the syslogger writes whole records), so parity is anchored
/// there and walked backward. A newline outside quotes is a true boundary, and no text inside a quoted field, planted
/// or not, can fake one. When the backward walk reaches offset 0 outside quotes, offset 0 is offered as a boundary too (a file
/// under the tail size is read from its first byte), and the first segment is shape-checked: a cut fragment fails and is
/// discarded. Inside quotes, the head is certainly a fragment and is excluded.</para>
///
/// <para><b>Trailing partial record.</b> When the caller cannot state <see cref="CsvBodyEdges.EndsOnRecordBoundary"/>,
/// the text after the true last newline is a partial record whose quote state is unknown. A single backward pass
/// tags every newline by the parity of the quote count strictly after it, splitting them into two hypotheses —
/// H_even (the body ends outside quotes) and H_odd (it ends inside one) — and each is scored ONCE by how many of
/// its records parse; the higher score wins, a tie broken by whichever hypothesis's last mark is later (#4053
/// review M1, M2). This closes the old 8-candidate cap's hole (a trailing partial with 8 or more embedded
/// newlines made every trial an inverted-parity one) at the cost of exactly two scoring passes. Scoring cannot
/// see an edge it was never told, so a single statement bigger than the tail can still straddle the read's start
/// and let inverted parity out-score the true one (#4053 review Q2) — naming that residual risk is why
/// <see cref="CsvBodyEdges"/> exists: a caller that knows an edge should state it rather than lean on scoring.
/// The partial is not emitted. Every complete record is still shape-checked: 26 fields, a <c>log_time</c>
/// that parses, a numeric <c>process_id</c>, and a <c>session_id</c> shaped <c>hex.hex</c>.</para>
/// </summary>
public static class PgServerLogCsvParser
{
    /// <summary>
    /// What the caller already knows about <c>body</c>'s two ends (#4053 review round 2). A tail reader
    /// carries these across calls: the offset it starts a read at is always the previous call's
    /// <c>consumedLength</c>, which is by construction a true record boundary, so every read after the
    /// first can state <see cref="StartsOnRecordBoundary"/>; whether the read reached the file's current
    /// end (<see cref="EndsOnRecordBoundary"/>) is the caller's own read-size bookkeeping, not something
    /// this parser can infer honestly from the trailing byte alone — a body that happens to end in <c>\n</c>
    /// might still be raced a byte short by the syslogger's own next write.
    /// </summary>
    [Flags]
    public enum CsvBodyEdges
    {
        /// <summary>Neither end is known to be a record boundary.</summary>
        None = 0,

        /// <summary>Offset 0 is a true record boundary — parity is exact from the first byte, so a forward
        /// walk never needs to score a candidate.</summary>
        StartsOnRecordBoundary = 1,

        /// <summary>The last byte of <c>body</c> is a true record boundary — the syslogger writes whole
        /// records, so the caller's own read-size bookkeeping already knows this when it read up to the
        /// file's current end.</summary>
        EndsOnRecordBoundary = 2,
    }

    /// <summary>The number of columns PostgreSQL 14 through 18 write per csvlog record, verified against
    /// live 14 and 18 containers for #4053. See the type header for the full column list.</summary>
    private const int ExpectedColumnCount = 26;

    private static readonly Regex s_sessionId = new(
        @"^[0-9a-fA-F]+\.[0-9a-fA-F]+$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every complete, resynced record in <paramref name="body"/>, in log order. Infers <c>body</c>'s edges
    /// exactly as the original single-overload parser did — <see cref="CsvBodyEdges.EndsOnRecordBoundary"/>
    /// when <c>body</c> ends in <c>\n</c>, otherwise <see cref="CsvBodyEdges.None"/> — for the three stacked
    /// branches (#4135, #4136, the plan-capture lane) that still call this signature.
    /// </summary>
    /// <param name="body">A slab of csvlog text, as read from the tail of a <c>.csv</c> log file. May start
    /// mid-record and may end mid-record.</param>
    /// <param name="recordsDiscarded">Every record dropped during resync or for a bad shape: the cut head's
    /// records up to and including the first complete one (see the type header), plus any record later in
    /// the body whose column count is not <see cref="ExpectedColumnCount"/>. Does not count a trailing
    /// partial record, because nothing about it was rejected — it was never complete enough to judge.</param>
    public static List<PgLogEntry> Parse(string body, out int recordsDiscarded)
    {
        var edges = !string.IsNullOrEmpty(body) && body[^1] == '\n'
            ? CsvBodyEdges.EndsOnRecordBoundary
            : CsvBodyEdges.None;
        return Parse(body, edges, out recordsDiscarded, out _);
    }

    /// <summary>
    /// Every complete, resynced record in <paramref name="body"/>, in log order (#4053 review round 2: the
    /// caller states the edges it already knows instead of this parser inferring them from the trailing byte).
    /// </summary>
    /// <param name="body">A slab of csvlog text, as read from the tail of a <c>.csv</c> log file. May start
    /// mid-record and may end mid-record.</param>
    /// <param name="edges">What the caller already knows about <c>body</c>'s two ends. See the enum's own
    /// remarks for why a tail reader can state <see cref="CsvBodyEdges.StartsOnRecordBoundary"/> on every
    /// read after its first.</param>
    /// <param name="recordsDiscarded">Every record dropped during resync or for a bad shape: the cut head's
    /// records up to and including the first complete one (see the type header), plus any record later in
    /// the body whose column count is not <see cref="ExpectedColumnCount"/>. Does not count a trailing
    /// partial record, because nothing about it was rejected — it was never complete enough to judge.</param>
    /// <param name="consumedLength">The index in <c>body</c> just past the last TRUE record boundary found.
    /// A tail reader carries <c>body[consumedLength..]</c> into its next read, along with
    /// <see cref="CsvBodyEdges.StartsOnRecordBoundary"/> for that next call. That is sound only when THIS call
    /// stated an edge (a forward walk or the fast path), because then <c>consumedLength</c> is a record boundary
    /// by construction. Under <see cref="CsvBodyEdges.None"/> it is the winning hypothesis's last boundary, a
    /// guess: a caller must not claim a known start from it, since a wrong start inverts parity for every
    /// later forward walk. Zero when no boundary was found at all.</param>
    public static List<PgLogEntry> Parse(string body, CsvBodyEdges edges, out int recordsDiscarded, out int consumedLength)
    {
        var entries = new List<PgLogEntry>();
        recordsDiscarded = 0;
        consumedLength = 0;
        if (string.IsNullOrEmpty(body))
        {
            return entries;
        }

        List<int>? marks;

        if (edges.HasFlag(CsvBodyEdges.StartsOnRecordBoundary))
        {
            /* The caller already knows offset 0 is a true record boundary, so parity is exact from the
               first byte: a FORWARD walk needs no scoring, because unlike an arbitrary mid-file offset
               there is nothing to invert. Every newline seen outside quotes is a true boundary; the text
               after the LAST one found is the carried partial (unknown quote state, not emitted, not
               counted) whether or not EndsOnRecordBoundary is also set — if the caller was wrong about the
               true end also being a boundary, the walk simply finds no newline there and the tail is
               treated as a partial anyway, exactly as the brief calls for. */
            marks = ComputeMarksForward(body);
        }
        else if (edges.HasFlag(CsvBodyEdges.EndsOnRecordBoundary))
        {
            /* Today's fast path: the caller says the last byte is a true record boundary (the syslogger
               writes whole records), so parity is anchored there and walked BACKWARD — a newline outside
               quotes is a true boundary, and no text inside a quoted field, planted or not, can fake one.
               No candidate scoring is needed: the end is a boundary only because the CALLER says so, not
               because this parser inferred it from the trailing byte.

               Residual risk (Low, a race): the syslogger can split one record across more than one write
               when the record is larger than stdio's buffer. A read that lands between two such writes can
               end at a newline that is actually inside a still-open quoted field — the caller's own
               bookkeeping said "end of file as of this read", which is not the same fact as "this byte
               ends a record". Nothing in this parser can detect that case from the text alone; it is named
               here because a caller for whom this matters needs to know the flag is a stated fact, not a
               guarantee this parser re-derives. */
            var anchorMarks = ComputeMarks(body, body.Length - 1);
            marks = HasParseableRecord(body, anchorMarks) ? anchorMarks : null;
        }
        else
        {
            /* Neither edge is known (#4053 review M1/M2): two-hypothesis scoring. One backward pass over
               the whole body tags every newline by the parity of the quote count strictly after it — H_even
               takes the newlines seen with an even count (the body's true end is outside quotes) and H_odd
               takes those seen with an odd count (the body's true end is inside a quoted field). Each
               hypothesis is scored ONCE with CountParseableRecords; the higher wins, and a tie goes to the
               hypothesis whose last mark is later. This costs exactly two parses, not up to
               MaxAnchorCandidates-worth of retries, and it closes the old cap's hole: a trailing partial
               with 8 or more embedded newlines used to make every one of the old 8 trial anchors an
               inverted-parity trial, so the true parity was never even tried.

               Residual risk (#4053 review Q2): a single statement bigger than the read's own tail can
               straddle the read's start, and an inverted-parity hypothesis can then out-score the true one
               by sheer bulk. Scoring cannot fix that without knowing an edge — which is exactly why a
               caller that knows one should state it via <see cref="CsvBodyEdges"/> instead of leaving this
               parser to guess. */
            marks = FindBoundariesByScoring(body);
        }

        if (marks is null)
        {
            recordsDiscarded = 1;
            return entries;
        }

        for (var r = 0; r + 1 < marks.Count; r++)
        {
            var start = marks[r] + 1;
            var text = body.Substring(start, marks[r + 1] - start);
            if (text.EndsWith('\r'))
            {
                text = text[..^1];
            }

            if (TryParseRecord(text, out var entry))
            {
                entries.Add(entry);
            }
            else
            {
                recordsDiscarded++;
            }
        }

        consumedLength = marks.Count > 0 ? marks[^1] + 1 : 0;
        return entries;
    }

    /// <summary>
    /// Forward walk from offset 0 (#4053 review round 2), used only when the caller states
    /// <see cref="CsvBodyEdges.StartsOnRecordBoundary"/>. Parity is exact from the first byte because the
    /// caller already knows offset 0 is a true boundary, so no scoring is needed: every newline seen outside
    /// quotes is a true boundary, in the order found. Never null — a body with no boundary at all simply
    /// yields the head sentinel alone, and the caller's loop then finds nothing to emit.
    /// </summary>
    private static List<int> ComputeMarksForward(string body)
    {
        var marks = new List<int> { -1 };
        var inQuotes = false;

        for (var i = 0; i < body.Length; i++)
        {
            var c = body[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (c == '\n' && !inQuotes)
            {
                marks.Add(i);
            }
        }

        return marks;
    }

    /// <summary>
    /// Two-hypothesis scoring (#4053 review M1/M2, replacing the old 8-candidate trial loop): a single
    /// backward pass tags every newline by the parity of the quote count seen strictly after it, splitting
    /// them into H_even (the newlines consistent with the body's true end being outside quotes) and H_odd
    /// (consistent with the end being inside one). Each hypothesis is scored ONCE with
    /// <see cref="CountParseableRecords"/>; the higher wins, ties going to the later last mark. Null when
    /// neither hypothesis scores above zero, which the caller counts as one discard.
    /// </summary>
    private static List<int>? FindBoundariesByScoring(string body)
    {
        var evenMarks = new List<int>();
        var oddMarks = new List<int>();
        var inQuotes = false;

        for (var i = body.Length - 1; i >= 0; i--)
        {
            var c = body[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (c == '\n')
            {
                if (inQuotes)
                {
                    oddMarks.Add(i);
                }
                else
                {
                    evenMarks.Add(i);
                }
            }
        }

        /* Parity at offset 0 is now known exactly under H_even's own convention (the pass started with
           inQuotes = false, i.e. "the body's true end is outside quotes"). Under H_odd the two states are
           simply swapped, so exactly one hypothesis gets the -1 head mark — the same -1-at-offset-0 rule
           ComputeMarks applies for a single anchor, applied here to each hypothesis in turn. */
        if (!inQuotes)
        {
            evenMarks.Add(-1);
        }
        else
        {
            oddMarks.Add(-1);
        }

        evenMarks.Reverse();
        oddMarks.Reverse();

        var evenScore = CountParseableRecords(body, evenMarks);
        var oddScore = CountParseableRecords(body, oddMarks);

        if (evenScore == 0 && oddScore == 0)
        {
            return null;
        }

        if (evenScore != oddScore)
        {
            return evenScore > oddScore ? evenMarks : oddMarks;
        }

        var evenLast = evenMarks.Count > 0 ? evenMarks[^1] : -1;
        var oddLast = oddMarks.Count > 0 ? oddMarks[^1] : -1;
        return evenLast >= oddLast ? evenMarks : oddMarks;
    }

    /// <summary>
    /// Walks backward from <paramref name="anchor"/> (a newline, assumed to be outside quotes) to recover
    /// every earlier boundary: a newline outside quotes is a true boundary, and no text inside a quoted
    /// field, planted or not, can fake one.
    /// </summary>
    private static List<int> ComputeMarks(string body, int anchor)
    {
        var marks = new List<int> { anchor };
        var inQuotes = false;
        for (var i = anchor - 1; i >= 0; i--)
        {
            var c = body[i];
            if (c == '"')
            {
                inQuotes = !inQuotes;
            }
            else if (c == '\n' && !inQuotes)
            {
                marks.Add(i);
            }
        }

        /* Parity at offset 0 is now known exactly. Outside quotes, offset 0 may be a real record start (a
           file under the tail size is read from its first byte), so it is offered as a boundary and the
           first segment is shape-checked like any other: a cut fragment fails it and is discarded. Inside
           quotes, the head is certainly a fragment and is excluded. */
        if (!inQuotes)
        {
            marks.Add(-1);
        }

        marks.Reverse();
        return marks;
    }

    /// <summary>Whether the record ending at the last mark — the one just before the anchor — parses. Used
    /// only for the already-terminated-body path, where a single anchor is the only candidate.</summary>
    private static bool HasParseableRecord(string body, List<int> marks)
    {
        if (marks.Count < 2)
        {
            return false;
        }

        var start = marks[^2] + 1;
        var last = body.Substring(start, marks[^1] - start).TrimEnd('\r');
        return TryParseRecord(last, out _);
    }

    /// <summary>How many of the records bounded by <paramref name="marks"/> parse (#4053 review M1): the
    /// scoring signal used to choose among candidate anchors on a mid-write read.</summary>
    private static int CountParseableRecords(string body, List<int> marks)
    {
        var count = 0;

        for (var r = 0; r + 1 < marks.Count; r++)
        {
            var start = marks[r] + 1;
            var text = body.Substring(start, marks[r + 1] - start).TrimEnd('\r');
            if (TryParseRecord(text, out _))
            {
                count++;
            }
        }

        return count;
    }


    /// <summary>
    /// Parses one raw record's text into fields, checks its shape, and builds a <see cref="PgLogEntry"/> if
    /// every shape check passes: 26 fields, a <c>log_time</c> that parses, a numeric <c>process_id</c>, and
    /// a <c>session_id</c> shaped <c>hex.hex</c>.
    /// </summary>
    private static bool TryParseRecord(string recordText, out PgLogEntry entry)
    {
        entry = default;

        var fields = SplitFields(recordText);

        if (fields.Count != ExpectedColumnCount)
        {
            return false;
        }

        var logTime = fields[0];

        if (!TryParseLogTime(logTime, out var occurredAtUtc, out var zoneText))
        {
            return false;
        }

        var pidText = fields[3];

        if (!int.TryParse(pidText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var pid))
        {
            return false;
        }

        var sessionId = fields[5];

        if (!s_sessionId.IsMatch(sessionId))
        {
            return false;
        }

        var severity = fields[11];
        var sqlState = NullIfEmpty(fields[12]);
        var message = fields[13];
        var detail = NullIfEmpty(fields[14]);
        var hint = NullIfEmpty(fields[15]);
        var context = NullIfEmpty(fields[18]);
        var statement = NullIfEmpty(fields[19]);
        var userName = NullIfEmpty(fields[1]);
        var databaseName = NullIfEmpty(fields[2]);
        /* Index 21 of the 26 columns (#4058 item 2): the reporting function's own name, filled only
           under log_error_verbosity = verbose. Empty on every other verbosity, mapped to null the same
           way every other optional companion here is. */
        var location = NullIfEmpty(fields[21]);

        entry = new PgLogEntry(
            TimestampText: logTime,
            ZoneText: zoneText,
            OccurredAtUtc: occurredAtUtc,
            Pid: pid,
            PrefixRest: string.Empty,
            Severity: severity,
            Message: message,
            Detail: detail,
            Hint: hint,
            Statement: statement,
            Context: context,
            UserName: userName,
            DatabaseName: databaseName,
            SqlState: sqlState,
            RawText: recordText,
            DetailComplete: true,
            Location: location);

        return true;
    }

    /// <summary>
    /// <c>log_time</c> renders as <c>YYYY-MM-DD HH:MM:SS.mmm ZONE</c> — the same stamp-then-zone shape the
    /// stderr assembler reads. This check decides only "is this a record" — a stamp that parses — never
    /// whether the zone is UTC (#4053 review H1): a non-zero-offset zone is still a record, and the zone
    /// decision belongs to <see cref="PgLogEventsCollector"/>'s foreign-zone filter, which runs after every
    /// record has been recovered. Rejecting a foreign-zone stamp here starved that filter of the very
    /// entries it exists to judge, and a non-UTC target read as quiet instead of refused.
    /// </summary>
    private static bool TryParseLogTime(string logTime, out DateTime occurredAtUtc, out string zoneText)
    {
        occurredAtUtc = default;
        zoneText = string.Empty;

        var spaceIndex = logTime.LastIndexOf(' ');

        if (spaceIndex < 0 || spaceIndex == logTime.Length - 1)
        {
            return false;
        }

        var stamp = logTime[..spaceIndex];
        zoneText = logTime[(spaceIndex + 1)..];

        return DateTime.TryParse(
            stamp,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
            out occurredAtUtc);
    }

    private static string? NullIfEmpty(string value) => value.Length == 0 ? null : value;

    /// <summary>
    /// Splits one CSV record's raw text (already isolated from the record stream, so it holds no
    /// unescaped newline) into its fields, per RFC 4180 as PostgreSQL writes it: comma-separated, an
    /// optionally-quoted field, <c>""</c> inside a quoted field unescapes to a literal <c>"</c>. An
    /// UNQUOTED field is written back verbatim — PostgreSQL only ever leaves a field unquoted when it is
    /// empty (its csvlog columns are consistently either quoted text or bare, e.g. a numeric pid), so no
    /// unescaping applies there.
    /// </summary>
    private static List<string> SplitFields(string recordText)
    {
        var fields = new List<string>();
        var current = new StringBuilder();
        var length = recordText.Length;
        var i = 0;

        while (i <= length)
        {
            if (i < length && recordText[i] == '"')
            {
                /* A quoted field: PostgreSQL always quotes from the very first character of a
                   non-empty text field, so seeing a quote here starts one. */
                i++;
                current.Clear();

                while (i < length)
                {
                    var c = recordText[i];

                    if (c == '"')
                    {
                        if (i + 1 < length && recordText[i + 1] == '"')
                        {
                            current.Append('"');
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    current.Append(c);
                    i++;
                }

                fields.Add(current.ToString());
                current.Clear();

                /* Skip to the next comma (or end); anything between a closing quote and the comma would
                   not be well-formed csvlog output, but skip it rather than losing the field boundary. */
                while (i < length && recordText[i] != ',')
                {
                    i++;
                }

                if (i < length)
                {
                    i++;
                    continue;
                }

                break;
            }

            var commaIndex = recordText.IndexOf(',', i);

            if (commaIndex < 0)
            {
                fields.Add(recordText[i..]);
                break;
            }

            fields.Add(recordText[i..commaIndex]);
            i = commaIndex + 1;
        }

        return fields;
    }
}
