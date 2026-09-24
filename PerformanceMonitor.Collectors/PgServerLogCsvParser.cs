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
/// <para><b>Trailing partial record.</b> When the body does not end in a newline (a read that raced a write), the
/// text after the last newline is a partial record whose quote state is unknown. The latest newline whose backward
/// split validates the record just before it becomes the anchor (bounded tries); the partial is not emitted. Every
/// complete record is still shape-checked: 26 fields, a <c>log_time</c> that parses, a numeric <c>process_id</c>,
/// and a <c>session_id</c> shaped <c>hex.hex</c>.</para>
/// </summary>
public static class PgServerLogCsvParser
{
    /// <summary>The number of columns PostgreSQL 14 through 18 write per csvlog record, verified against
    /// live 14 and 18 containers for #4053. See the type header for the full column list.</summary>
    private const int ExpectedColumnCount = 26;

    private static readonly Regex s_sessionId = new(
        @"^[0-9a-fA-F]+\.[0-9a-fA-F]+$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every complete, resynced record in <paramref name="body"/>, in log order.
    /// </summary>
    /// <param name="body">A slab of csvlog text, as read from the tail of a <c>.csv</c> log file. May start
    /// mid-record and may end mid-record.</param>
    /// <param name="recordsDiscarded">Every record dropped during resync or for a bad shape: the cut head's
    /// records up to and including the first complete one (see the type header), plus any record later in
    /// the body whose column count is not <see cref="ExpectedColumnCount"/>. Does not count a trailing
    /// partial record, because nothing about it was rejected — it was never complete enough to judge.</param>
    public static List<PgLogEntry> Parse(string body, out int recordsDiscarded)
    {
        var entries = new List<PgLogEntry>();
        recordsDiscarded = 0;
        if (string.IsNullOrEmpty(body))
        {
            return entries;
        }

        /* Quote parity cannot be recovered reading FORWARD from an arbitrary offset: a window that starts
           inside a quoted field inverts every quote after it, every newline then reads as inside a field,
           and the whole rest of the body glues into one record that fails the shape check, so a read that
           happens to start mid-field (most of a csvlog's bytes are quoted text) would yield nothing. The
           END of the body is a record boundary instead (the syslogger writes whole records), so parity is
           anchored there and walked BACKWARD: a newline outside quotes is a true boundary, and no text
           inside a quoted field, planted or not, can fake one. What precedes the first boundary is the cut
           head. If the body does not end in a newline (a read that raced a write), the text after the
           candidate boundary is a partial record whose quote state is unknown, so the latest candidate
           newline whose backward split yields a valid record just before it wins; tries are bounded. */
        var boundaries = FindBoundaries(body);
        if (boundaries is null)
        {
            recordsDiscarded = 1;
            return entries;
        }

        for (var r = 0; r + 1 < boundaries.Count; r++)
        {
            var start = boundaries[r] + 1;
            var text = body.Substring(start, boundaries[r + 1] - start);
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

        return entries;
    }

    /// <summary>
    /// Positions of the newlines that end records, preceded by -1 when the body starts on a record boundary
    /// (it never does for a tail read, but a whole file would). The cut head before the first boundary is
    /// excluded. Null when no anchor validates (bounded tries), which the caller counts as one discard.
    /// </summary>
    private static List<int>? FindBoundaries(string body)
    {
        const int MaxAnchorTries = 64;
        var anchor = body.Length - 1;
        var tries = 0;
        while (anchor >= 0 && tries < MaxAnchorTries)
        {
            anchor = body.LastIndexOf('\n', anchor);
            if (anchor < 0)
            {
                return null;
            }

            tries++;
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
            /* The anchor is right when the record that ends at it parses: with inverted parity the
               "record" before it is a glued fragment that fails the shape check. A body with only one
               boundary has no complete record to validate against, so it yields nothing this cycle. */
            if (marks.Count >= 2)
            {
                var start = marks[^2] + 1;
                var last = body.Substring(start, marks[^1] - start).TrimEnd('\r');
                if (TryParseRecord(last, out _))
                {
                    return marks;
                }
            }

            anchor--;
        }

        return null;
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
            DetailComplete: true);

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
