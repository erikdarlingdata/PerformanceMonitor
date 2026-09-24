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
using System.Text.Json;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Parses <c>jsonlog</c>-format PostgreSQL server log text (PostgreSQL 15+) into <see cref="PgLogEntry"/>
/// records (#4053, part a2).
///
/// <para><b>Why this format needs no boundary walk.</b> Under <c>stderr</c> format a failed login's role
/// or database name lands in the log UNESCAPED — <see cref="PgLogEntryAssembler"/> reads it off a
/// regex-matched prefix — so a newline planted in either field forges a whole extra log line that reads as
/// the server's own. <c>jsonlog</c> writes exactly one JSON object per line, and JSON string escaping turns
/// every raw newline or quote inside a field into <c>\n</c> or <c>\"</c>, so nothing planted in a field can
/// ever contain a raw newline byte. Every raw <c>\n</c> in a jsonlog file IS a record boundary — unlike
/// <see cref="PgServerLogCsvParser"/>, which has to walk backward from the body's end to recover quote
/// parity before it can trust one. This parser is the jsonlog twin of
/// <see cref="PgServerLogCsvParser"/> and, transitively, of <see cref="PgLogEntryAssembler.Assemble(string?)"/>
/// — same target shape, same <see cref="PgLogEntry.OccurredAtUtc"/> rule — over one JSON object per line
/// instead of a CSV record or a regex-matched prefix line. Wiring it into the tail and
/// <c>pg_log_events</c> is a later lane.</para>
///
/// <para><b>Keys, verified empirically (#4053's brief), not guessed.</b> Against a live 18 container with
/// <c>log_destination=jsonlog</c>, a failed login and an ordinary <c>ERROR</c> produced records carrying
/// (among others) <c>timestamp, user, dbname, pid, remote_host, remote_port, session_id, line_num, ps,
/// session_start, vxid, txid, error_severity, state_code, message, detail, hint, statement,
/// application_name, backend_type, query_id</c>. PostgreSQL OMITS a key entirely when it has nothing to
/// report — a pre-authentication connection line carries no <c>user</c> or <c>dbname</c> at all, rather
/// than writing them as JSON <c>null</c> — so every field read here treats an absent key exactly like an
/// empty one, both mapping to a null <see cref="PgLogEntry"/> field.</para>
///
/// <para><b>A cut head can never parse.</b> A JSON object that has been cut mid-stream (a tail read that
/// starts partway through one) is not valid JSON — it is missing its opening brace or an earlier field —
/// so <see cref="JsonDocument.Parse(string, JsonDocumentOptions)"/> throws on it and it is discarded like
/// any other malformed line. No separate resync walk is needed: unlike csvlog's quoted commas, nothing in
/// jsonlog can make a fragment look complete.</para>
///
/// <para><b>Trailing partial line.</b> When the body does not end in a newline (a read that raced a
/// write), the text after the last newline is a partial record and is never emitted or counted — it was
/// never complete enough to judge, same rule as <see cref="PgServerLogCsvParser"/>.</para>
/// </summary>
public static class PgServerLogJsonParser
{
    private static readonly Regex s_sessionId = new(
        @"^[0-9a-fA-F]+\.[0-9a-fA-F]+$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// Every complete, well-formed record in <paramref name="body"/>, in log order.
    /// </summary>
    /// <param name="body">A slab of jsonlog text, as read from the tail of a <c>.json</c> log file. May
    /// start mid-record (a cut head, which can never parse and so is simply discarded) and may end
    /// mid-record (a trailing partial line, never emitted or counted).</param>
    /// <param name="recordsDiscarded">Every line that failed to parse as one complete JSON object with a
    /// usable shape: malformed JSON (including a cut head), a missing or unparseable <c>timestamp</c>, a
    /// non-numeric <c>pid</c>, or a <c>session_id</c> not shaped <c>hex.hex</c>. Does not count the
    /// trailing partial line, because nothing about it was rejected — it was never complete enough to
    /// judge.</param>
    public static List<PgLogEntry> Parse(string body, out int recordsDiscarded)
    {
        var entries = new List<PgLogEntry>();
        recordsDiscarded = 0;

        if (string.IsNullOrEmpty(body))
        {
            return entries;
        }

        var lines = body.Split('\n');

        /* body.Split('\n') on "a\nb\n" yields ["a","b",""] — the trailing empty element after the final
           newline is not a line at all, and dropping the last element also drops it correctly. On "a\nb"
           (no trailing newline) the last element IS a genuine partial, and dropping the last element
           excludes it too. Either way the last element of the split is never a complete line to parse. */
        var lineCount = lines.Length - 1;

        for (var i = 0; i < lineCount; i++)
        {
            var line = lines[i];

            if (line.EndsWith('\r'))
            {
                line = line[..^1];
            }

            if (line.Length == 0)
            {
                continue;
            }

            if (TryParseRecord(line, out var entry))
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
    /// Parses one raw JSON line into a <see cref="PgLogEntry"/> if it is well-formed JSON and its shape
    /// checks pass: <c>timestamp</c> parses, <c>pid</c> is numeric, and — when present — <c>session_id</c>
    /// is shaped <c>hex.hex</c>.
    /// </summary>
    private static bool TryParseRecord(string line, out PgLogEntry entry)
    {
        entry = default;

        JsonDocument doc;

        try
        {
            doc = JsonDocument.Parse(line);
        }
        catch (JsonException)
        {
            return false;
        }

        using (doc)
        {
            var root = doc.RootElement;

            if (root.ValueKind != JsonValueKind.Object)
            {
                return false;
            }

            if (!TryGetString(root, "timestamp", out var timestampText) ||
                !TryParseTimestamp(timestampText!, out var occurredAtUtc, out var zoneText))
            {
                return false;
            }

            if (!root.TryGetProperty("pid", out var pidElement) ||
                pidElement.ValueKind != JsonValueKind.Number ||
                !pidElement.TryGetInt32(out var pid))
            {
                return false;
            }

            if (TryGetString(root, "session_id", out var sessionId) &&
                !s_sessionId.IsMatch(sessionId!))
            {
                return false;
            }

            TryGetString(root, "error_severity", out var severity);
            TryGetString(root, "message", out var message);
            TryGetString(root, "detail", out var detail);
            TryGetString(root, "hint", out var hint);
            TryGetString(root, "statement", out var statement);
            TryGetString(root, "context", out var context);
            TryGetString(root, "user", out var userName);
            TryGetString(root, "dbname", out var databaseName);
            TryGetString(root, "state_code", out var sqlState);

            entry = new PgLogEntry(
                TimestampText: timestampText!,
                ZoneText: zoneText,
                OccurredAtUtc: occurredAtUtc,
                Pid: pid,
                PrefixRest: string.Empty,
                Severity: severity ?? string.Empty,
                Message: message ?? string.Empty,
                Detail: detail,
                Hint: hint,
                Statement: statement,
                Context: context,
                UserName: userName,
                DatabaseName: databaseName,
                SqlState: sqlState,
                RawText: line,
                DetailComplete: true);

            return true;
        }
    }

    /// <summary>
    /// Reads a string property, treating an ABSENT key exactly like PostgreSQL means it — nothing to
    /// report — the same as an empty string: both come back as <c>false</c>/null so a caller need not
    /// distinguish them. A present key whose value is JSON <c>null</c> is treated the same way.
    /// </summary>
    private static bool TryGetString(JsonElement root, string propertyName, out string? value)
    {
        if (!root.TryGetProperty(propertyName, out var element) ||
            element.ValueKind != JsonValueKind.String)
        {
            value = null;
            return false;
        }

        value = element.GetString();
        return !string.IsNullOrEmpty(value);
    }

    /// <summary>
    /// <c>timestamp</c> renders as <c>YYYY-MM-DD HH:MM:SS.mmm ZONE</c> — the same stamp-then-zone shape the
    /// stderr assembler and csvlog's <c>log_time</c> read. This check decides only "is this a record" — a
    /// stamp that parses — never whether the zone is UTC (#4053 review H1, mirrored here so the future
    /// jsonlog wiring gets it right from the start): a non-zero-offset zone is still a record, and the zone
    /// decision belongs to the caller's foreign-zone filter, which runs after every record has been
    /// recovered.
    /// </summary>
    private static bool TryParseTimestamp(string timestampText, out DateTime occurredAtUtc, out string zoneText)
    {
        occurredAtUtc = default;
        zoneText = string.Empty;

        var spaceIndex = timestampText.LastIndexOf(' ');

        if (spaceIndex < 0 || spaceIndex == timestampText.Length - 1)
        {
            return false;
        }

        var stamp = timestampText[..spaceIndex];
        zoneText = timestampText[(spaceIndex + 1)..];

        return DateTime.TryParse(
            stamp,
            CultureInfo.InvariantCulture,
            DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal,
            out occurredAtUtc);
    }
}
