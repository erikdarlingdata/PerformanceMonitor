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
/// <para><b>The tail starts mid-file (the same window-edge problem <see cref="PgLogEntryAssembler"/>
/// documents), so this resyncs before trusting anything:</b></para>
/// <list type="number">
/// <item>Discard everything up to the first newline: whatever preceded it is the cut head of a record this
/// window did not see the start of.</item>
/// <item>Then discard records, one at a time, until the first one that parses COMPLETELY — the right field
/// count, a <c>log_time</c> that parses, a numeric <c>process_id</c>, and a <c>session_id</c> shaped
/// <c>hex.hex</c> — because a window boundary that lands inside a quoted field could make "the first
/// record" start on attacker-planted text that only coincidentally has 26 commas in it.</item>
/// <item><b>Also discard that first genuinely complete record.</b> Landing inside a quoted field does not
/// stop the record boundary rule above from finding a false 26-column split if the planted text itself
/// contains 25 commas before a real field boundary — passing every shape check by chance is unlikely but
/// not impossible, and dropping one more record costs nothing because consecutive tail reads overlap.</item>
/// </list>
///
/// <para><b>Trailing partial record.</b> A record with no closing newline, or with a quote still open at
/// end of input, is the cut tail of a record the next overlapping tail read will offer whole; it is not
/// emitted, and it is not counted in <c>recordsDiscarded</c> (nothing was rejected — there was nothing
/// complete to reject).</para>
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

        var records = SplitRecords(body, out var endedCleanly);

        /* Rule 1: discard everything up to the first newline. A record that started before this window's
           first byte is the cut head; if the very first raw record in the split has no complete newline
           of its own preceding it (i.e. it IS the first line), it is still potentially a cut head, so it
           is handled uniformly by requiring the first accepted record to also pass the shape checks below. */
        var resynced = false;
        var i = 0;

        for (; i < records.Count; i++)
        {
            var isLast = i == records.Count - 1;

            if (isLast && !endedCleanly)
            {
                /* Trailing partial record: not emitted, not counted (see type header). */
                break;
            }

            if (!TryParseRecord(records[i], out var entry))
            {
                recordsDiscarded++;
                continue;
            }

            if (!resynced)
            {
                /* Rule 2 + rule 3: the first record that parses completely is dropped too — it may have
                   started on attacker-planted text inside a quoted field from before this window. */
                resynced = true;
                recordsDiscarded++;
                continue;
            }

            entries.Add(entry);
        }

        return entries;
    }

    /// <summary>
    /// Splits <paramref name="body"/> into raw record texts on newlines that are OUTSIDE quotes, honouring
    /// RFC 4180 quoting (a quoted field may contain literal commas and newlines, and <c>""</c> inside a
    /// quoted field is an escaped quote, not a field terminator). <paramref name="endedCleanly"/> is false
    /// when the body ends with an open quote or with no closing record terminator, in which case the last
    /// element of the result is a partial record and must not be parsed as a whole one.
    /// </summary>
    private static List<string> SplitRecords(string body, out bool endedCleanly)
    {
        var records = new List<string>();
        var current = new StringBuilder();
        var inQuotes = false;
        var length = body.Length;

        for (var i = 0; i < length; i++)
        {
            var c = body[i];

            if (inQuotes)
            {
                if (c == '"')
                {
                    if (i + 1 < length && body[i + 1] == '"')
                    {
                        /* Escaped quote: keep both characters as part of the raw record text; field-level
                           unescaping happens in TryParseRecord. */
                        current.Append('"').Append('"');
                        i++;
                        continue;
                    }

                    inQuotes = false;
                    current.Append(c);
                    continue;
                }

                current.Append(c);
                continue;
            }

            if (c == '"')
            {
                inQuotes = true;
                current.Append(c);
                continue;
            }

            if (c == '\n')
            {
                var text = current.ToString();

                if (text.EndsWith('\r'))
                {
                    text = text[..^1];
                }

                records.Add(text);
                current.Clear();
                continue;
            }

            current.Append(c);
        }

        /* Whatever is left in `current` is either empty (the body ended right after a newline: nothing
           partial) or the cut tail of an unterminated record — including one whose open quote never
           closed, which `inQuotes` still being true also signals. */
        if (current.Length > 0 || inQuotes)
        {
            records.Add(current.ToString());
            endedCleanly = false;
        }
        else
        {
            endedCleanly = true;
        }

        return records;
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
    /// stderr assembler reads, and the same rule applies to what the zone MEANS: only a zero-offset zone is
    /// trusted as already-UTC, via <see cref="PgDeadlockLogParser.IsZeroOffsetLogZone"/>.
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

        if (!PgDeadlockLogParser.IsZeroOffsetLogZone(zoneText))
        {
            return false;
        }

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
