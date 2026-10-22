/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Common;

/// <summary>
/// Parses the "Add Multiple Servers" paste box into a typed list of server rows + a parallel list of
/// per-line errors — shared verbatim by Lite and the Darling viewer so the two bulk dialogs can never
/// drift on grammar. Pure, synchronous, allocation-light (the Common-root pure-static precedent set by
/// <see cref="IndexCleanupAnalyzer"/>).
///
/// <para>Grammar (one server per line, <c>server[, display name][, database]</c>): lines split on CRLF or
/// LF; blank lines and lines whose trimmed text starts with <c>#</c> are skipped; a line containing a TAB
/// is tab-separated, otherwise comma-separated (so a spreadsheet paste keeps commas inside a field);
/// every field is trimmed and trailing empty fields are dropped (tolerating trailing separators and Excel's
/// trailing empty cells); the first field is the server name (empty → an error), fields two and three are
/// the optional display name and database (empty-after-trim → null, INTERIOR empties are null not errors),
/// and more than three fields is an error. The parser does NO duplicate detection and NO normalization —
/// both are app-side by design (the shared <see cref="ServerIdHelper"/> identity + each app's dedupe gate).</para>
///
/// <para>Port rule (#2027): SQL Server's own non-default-port syntax is <c>host,port</c>, which collides
/// with the comma grammar — under the plain rules <c>sql01,2433</c> parsed as server "sql01" with display
/// name "2433", silently connecting to the default port. So on a COMMA-separated line, a second field that
/// is a pure number in the valid port range (1–65535) is a PORT riding the server name, not a display name:
/// it folds back into field one and the remaining fields shift left, so
/// <c>sql01,2433, Prod POS, master</c> is server "sql01,2433" + display + database. The one thing this
/// costs is a pure-numeric display name on a comma line — use the tab form for that (the same escape the
/// grammar already prescribes for display names containing commas). Tab-separated lines are untouched:
/// their first cell already carries <c>host,port</c> intact.</para>
/// </summary>
public static class BulkServerListParser
{
    /// <summary>Parses <paramref name="text"/> into the (valid rows, errors) split. Never throws;
    /// null/empty/whitespace-only input yields an empty result.</summary>
    public static BulkServerParseResult Parse(string? text)
    {
        var servers = new List<BulkServerParseLine>();
        var errors = new List<BulkServerParseError>();

        if (string.IsNullOrEmpty(text))
        {
            return new BulkServerParseResult(servers, errors);
        }

        // Rule 1: split on CRLF or LF; "\r\n" listed first so a CRLF is consumed as one separator (no
        // stray trailing '\r' on the raw line). LineNumber is 1-based over the ORIGINAL input.
        var rawLines = text.Split(new[] { "\r\n", "\n" }, System.StringSplitOptions.None);
        for (var i = 0; i < rawLines.Length; i++)
        {
            var lineNumber = i + 1;
            var rawText = rawLines[i];
            var trimmed = rawText.Trim();

            // Rule 2: skip blank + comment (#) lines (LineNumber still advances, so later lines report correctly).
            if (trimmed.Length == 0 || trimmed[0] == '#')
            {
                continue;
            }

            // Rule 3: tab-separated when the line contains a tab (spreadsheet paste), else comma-separated
            // (hand-typed). A display name containing a comma therefore requires tab-separated input.
            var separator = rawText.Contains('\t') ? '\t' : ',';
            var fields = rawText.Split(separator);

            // Rule 4: trim every field, then drop TRAILING empty fields.
            for (var f = 0; f < fields.Length; f++)
            {
                fields[f] = fields[f].Trim();
            }

            var count = fields.Length;
            while (count > 0 && fields[count - 1].Length == 0)
            {
                count--;
            }

            // Rule 5: field 1 (server name) must be present.
            if (count == 0 || fields[0].Length == 0)
            {
                errors.Add(new BulkServerParseError(lineNumber, rawText, "server name is empty"));
                continue;
            }

            // Port rule (#2027, comma lines only — a tab line's first cell already holds "host,port"
            // intact): a pure-numeric second field is SQL Server's ",port" suffix, not a display name.
            // Fold it into the server name and shift the remaining fields left BEFORE the max-field
            // check, so "sql01,2433, Prod POS, master" is three logical fields, not four.
            if (separator == ',' && count >= 2 && IsPortNumber(fields[1]))
            {
                fields[0] = fields[0] + "," + fields[1];
                for (var f = 2; f < count; f++)
                {
                    fields[f - 1] = fields[f];
                }
                count--;
            }

            // Rule 5 continued: at most 3 (logical) fields.
            if (count > 3)
            {
                errors.Add(new BulkServerParseError(lineNumber, rawText, "too many fields (max 3: server, display name, database)"));
                continue;
            }

            // #6352: a server name SqlClient would read as a named pipe or an admin connection is a line error,
            // like the single-server dialogs refuse it, so the bulk paste cannot save what they would not.
            if (ServerHostSetting.Refusal(fields[0]) is { } hostRefusal)
            {
                errors.Add(new BulkServerParseError(lineNumber, rawText, hostRefusal));
                continue;
            }

            // Rules 5/6: absent or empty (incl. INTERIOR-empty) display/database fields → null.
            var serverName = fields[0];
            var displayName = count >= 2 && fields[1].Length > 0 ? fields[1] : null;
            var databaseName = count >= 3 && fields[2].Length > 0 ? fields[2] : null;

            servers.Add(new BulkServerParseLine(lineNumber, rawText, serverName, displayName, databaseName));
        }

        return new BulkServerParseResult(servers, errors);
    }

    /// <summary>Whether a trimmed field is a valid TCP port (all ASCII digits, 1–65535) — the #2027 port
    /// rule's gate. "0", empty, signed, and out-of-range numbers are NOT ports and stay display names.</summary>
    private static bool IsPortNumber(string field)
    {
        if (field.Length is 0 or > 5)
        {
            return false;
        }

        foreach (var c in field)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }
        }

        return int.Parse(field, System.Globalization.CultureInfo.InvariantCulture) is >= 1 and <= 65535;
    }
}

/// <summary>The parse output: the valid server rows and the per-line errors, each ordered by
/// <see cref="BulkServerParseLine.LineNumber"/> / <see cref="BulkServerParseError.LineNumber"/>. A
/// line-ordered preview is a merge of the two lists on LineNumber.</summary>
public sealed record BulkServerParseResult(
    IReadOnlyList<BulkServerParseLine> Servers,
    IReadOnlyList<BulkServerParseError> Errors);

/// <summary>One successfully parsed server row. <see cref="ServerName"/> is guaranteed non-empty;
/// <see cref="DisplayName"/> / <see cref="DatabaseName"/> are null when absent or blank.
/// <see cref="RawText"/> is the original (pre-trim) line, for the preview grid.</summary>
public sealed record BulkServerParseLine(
    int LineNumber,
    string RawText,
    string ServerName,
    string? DisplayName,
    string? DatabaseName);

/// <summary>One line that could not be parsed (empty server name, or too many fields), carrying its
/// original text and a human-readable reason for the preview grid.</summary>
public sealed record BulkServerParseError(
    int LineNumber,
    string RawText,
    string Message);
