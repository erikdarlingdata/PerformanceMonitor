/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What happens to PostgreSQL's log text before it is stored (#3601, #3944): SQL is normalized, prose is not
/// touched. Messages are shown as PostgreSQL wrote them; SQL text is normalized with literals replaced by
/// <c>?</c>.
///
/// <para><b>Why prose is left alone</b> (#3944's ruling). PostgreSQL's prose carries values in more shapes than a
/// pattern list can name: a RAISE message is the application's own text, a libxml DETAIL quotes the input line, a
/// quoted run after a noun can be a name or a typed value. #3920's four review rounds each found another shape, and
/// masking every bare number would have destroyed the evidence prose carries (pids, transaction ids, durations). So
/// the prose masking went, and what stays is the half that CAN be made complete: SQL, read by its own lexical
/// rules.</para>
///
/// <list type="bullet">
/// <item><description><see cref="RedactStatement"/> — a monitored target's <c>STATEMENT:</c> companion, which is
/// never stored: only <see cref="Fingerprint"/>'s hash of this form is, so one statement shape recurs to one
/// fingerprint whatever it ran with. It applies the plan parser's own patterns by INSTANCE
/// (<see cref="PgPlanLogParser.s_quotedLiteral"/>, <see cref="PgPlanLogParser.s_bareNumber"/>), and it is unchanged
/// since #3601 so fingerprints stay continuous with every row already stored.</description></item>
/// <item><description><see cref="RedactStoredStatement"/> — SQL text that IS stored: a single-pass lexer that
/// replaces every literal with <c>?</c> and refuses a statement it cannot read to its end
/// (<see cref="WithheldStatement"/>).</description></item>
/// <item><description><see cref="RedactDetail"/> and <see cref="RedactContext"/> — the SQL PostgreSQL writes into
/// a DETAIL or a CONTEXT, found and put through that lexer; every other line of the field is kept as
/// written.</description></item>
/// <item><description><see cref="WithholdPlan"/> — auto_explain's plan report, whose plan carries the statement's
/// text in a form the lexer cannot read, withheld below its duration line.</description></item>
/// </list>
/// </summary>
public static class PgLogTextRedactor
{
    /* The DETAIL shapes that carry SQL (#3920). A deadlock report opens with its wait-for lines, "Process N
       waits for <lock> on <object>; blocked by process M.", then writes each process's query as "Process N:
       <query>" (errdetail_log, each cut at track_activity_query_size). A crashed backend's report is one query,
       "Failed process was running: <query>", and a logged EXECUTE names its PREPARE as "prepare: <statement>"
       (errdetail_execute). The values a statement ran with are one list to the field's end, in SQL's own quoting
       (#3944's review): PL/pgSQL's "parameters: name = 'value', ..." under print_strict_params, on the ERROR a
       STRICT row count raised, and "Parameters: $1 = 'value', ..." under a logged statement (errdetail_params).
       Prose masking had covered their quoted values. A tab leads each continuation line in a stderr log, nothing
       in csvlog. Any other DETAIL is prose, kept as written. */
    private static readonly Regex s_deadlockWaitFor = new(
        @"^\t?Process (?<pid>[0-9]+) waits for .*; blocked by process (?<blocker>[0-9]+)\.$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_deadlockQueryHead = new(
        @"^\t?Process (?<pid>[0-9]+): ",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_singleQueryHead = new(
        @"^(?:Failed process was running: |prepare: |[Pp]arameters: )",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* A CONTEXT frame that quotes the statement it was running, unescaped (#3920's review): SPI's
       `SQL statement "..."` and PL/pgSQL's expression and assignment frames. */
    private static readonly Regex s_contextSqlFrame = new(
        @"^\t?(?:SQL statement|SQL expression|PL/pgSQL expression|PL/pgSQL assignment) """,
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* A CONTEXT frame that writes SQL WITHOUT quoting it, to the frame's end (#3944's review): postgres_fdw's
       `remote SQL command: <sql>`, whose pushed-down constants are literals, the values a statement was bound
       with (`unnamed portal with parameters: $1 = '...'`, `portal "p" parameter $1 = '...'`, written under
       log_parameter_max_length_on_error), which read as SQL too, and PL/pgSQL's `query: <sql>` under its
       "query did not return data" error. Prose masking had covered their quoted values. */
    private static readonly Regex s_contextSqlLine = new(
        @"^\t?(?:remote SQL command: |query: |(?:unnamed portal|portal ""[^""]*"") (?:with parameters: |parameter ))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* What can follow an SQL frame: the line after its closing quote starts one of these, or the field ends. */
    private static readonly Regex s_contextFrameStart = new(
        @"^\t?(?:PL/pgSQL function |SQL function |SQL statement ""|SQL expression ""|PL/pgSQL expression ""|PL/pgSQL assignment ""|parallel worker|while |COPY |JSON data, |remote SQL command: |query: |unnamed portal |portal "")",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>What a statement becomes when it cannot be read to its end (cut inside a literal, a quoted
    /// identifier or a block comment, or written two ways at once): no mask can be trusted to have covered what
    /// the cut hid, so no statement is kept at all.</summary>
    public const string WithheldStatement = "<statement withheld: it could not be read to its end>";

    /// <summary>PostgreSQL 18's normalized IN-list marker, <c>IN ($1 /*, ... */)</c>: a comment that is part of what
    /// a normalized statement says, and carries no value.</summary>
    public const string NormalizedInListMarker = "/*, ... */";

    /// <summary>The longest SQL-bearing field (a DETAIL's queries, a CONTEXT's SQL frames) whose SQL is read at all
    /// (#3920's fourth review); a longer one has its SQL withheld. PostgreSQL cuts each query it writes there at
    /// <c>track_activity_query_size</c>, 1 kB by default, so this is far past any field it writes itself.</summary>
    internal const int MaxSqlFieldLength = 64 * 1024;

    /// <summary>How many candidate query boundaries one field may test (#3920's fourth review). Each test re-reads
    /// the text so far, so an unbounded count made a hostile field quadratic: a 180 KB CONTEXT took 21 s, and the
    /// self-hosted tail re-reads its overlap window every cycle. Past the cap the boundary is not taken, which
    /// leaves the SQL to be withheld.</summary>
    internal const int MaxBoundaryTests = 32;

    /// <summary>
    /// A DETAIL field (#3920, #3944): the SQL PostgreSQL writes into it normalized as a stored statement is
    /// (<see cref="RedactStoredStatement"/>), and withheld when it cannot be read to its end, which is common: the
    /// server cuts each query at <c>track_activity_query_size</c>. Only these shapes carry SQL: a crash's
    /// <c>Failed process was running: query</c>, a logged EXECUTE's <c>prepare: statement</c> and the values a
    /// statement ran with (<c>parameters: ...</c>, <c>Parameters: ...</c>), each one query to the end of the field,
    /// and a deadlock report, whose wait-for lines are prose and are followed by each process's
    /// <c>Process N: query</c>. Every other DETAIL, and a deadlock report's wait-for lines, are kept as
    /// written.
    ///
    /// <para><b>Where one deadlock query ends</b> (#3920's fourth review). A <c>Process N:</c> line starts the next
    /// query only when N is one of the deadlock's own processes AND the query before it reads to its end, testing
    /// at most <see cref="MaxBoundaryTests"/> such lines; any other line, one a literal carries included, belongs to
    /// the query it follows. A report that goes on in any other shape after its wait-for lines, or one longer than
    /// <see cref="MaxSqlFieldLength"/>, has its queries withheld: whatever follows the wait-for lines is SQL. Null
    /// in, null out; idempotent.</para>
    /// </summary>
    public static string? RedactDetail(string? detail)
    {
        if (string.IsNullOrEmpty(detail))
        {
            return detail;
        }

        var single = s_singleQueryHead.Match(detail);
        if (single.Success)
        {
            return single.Value + MaskQuery(detail[single.Length..]);
        }

        var lines = detail.Split('\n');
        var waits = 0;
        var pids = new HashSet<string>(StringComparer.Ordinal);
        while (waits < lines.Length && s_deadlockWaitFor.Match(lines[waits]) is { Success: true } wait)
        {
            pids.Add(wait.Groups["pid"].Value);
            pids.Add(wait.Groups["blocker"].Value);
            waits++;
        }

        /* Prose, or a deadlock report cut before its first query: nothing here is SQL. */
        if (waits == 0 || waits == lines.Length)
        {
            return detail;
        }

        var output = new List<string>(lines.Length - waits + 1) { string.Join('\n', lines, 0, waits) };
        var head = s_deadlockQueryHead.Match(lines[waits]);
        if (!head.Success || !pids.Contains(head.Groups["pid"].Value) || detail.Length > MaxSqlFieldLength)
        {
            output.Add((head.Success ? head.Value : lines[waits].StartsWith('\t') ? "\t" : string.Empty) + WithheldStatement);
            return string.Join('\n', output);
        }

        var currentHead = head.Value;
        var query = new StringBuilder(lines[waits], head.Length, lines[waits].Length - head.Length, detail.Length);
        var tests = 0;
        for (var k = waits + 1; k < lines.Length; k++)
        {
            var next = s_deadlockQueryHead.Match(lines[k]);
            if (next.Success
                && pids.Contains(next.Groups["pid"].Value)
                && tests++ < MaxBoundaryTests
                && RedactStoredStatement(query.ToString()) is { } masked)
            {
                output.Add(currentHead + masked);
                currentHead = next.Value;
                query.Clear().Append(lines[k], next.Length, lines[k].Length - next.Length);
                continue;
            }

            query.Append('\n').Append(lines[k]);
        }

        output.Add(currentHead + MaskQuery(query.ToString()));
        return string.Join('\n', output);
    }

    /// <summary>One query a DETAIL carries, masked, or withheld when it cannot be read to its end or is too long
    /// to read at all.</summary>
    private static string MaskQuery(string query) =>
        query.Length > MaxSqlFieldLength ? WithheldStatement : RedactStoredStatement(query) ?? WithheldStatement;

    /// <summary>
    /// A CONTEXT field (#3920, #3944): a frame that quotes the statement it was running (<c>SQL statement "..."</c>,
    /// PL/pgSQL's <c>expression</c> and <c>assignment</c> frames) has that statement normalized
    /// (<see cref="RedactStoredStatement"/>), and so does a frame that writes SQL unquoted to its end (postgres_fdw's
    /// <c>remote SQL command:</c>, a portal's bound parameters, PL/pgSQL's <c>query:</c>); every other line is kept
    /// as written. PostgreSQL does not escape the quote, so a double quote inside the statement is not its end: the
    /// statement ends at the first closing quote (a line end, unquoted) before which the text reads to its end and
    /// after which the next line starts another frame, or the field ends, testing at most
    /// <see cref="MaxBoundaryTests"/> closes. A frame that no close satisfies, or one in a field longer than
    /// <see cref="MaxSqlFieldLength"/>, is withheld with everything after it, because what follows may be the rest
    /// of its statement. Null in, null out; idempotent.
    ///
    /// <para><b>An SQL frame is read as SQL on whatever line it opens</b> (#3944). #3920's fourth review read one
    /// only where a frame could start, so a COPY value running onto a line shaped like a frame stayed part of that
    /// value's prose mask. With prose kept as written that rule protects nothing, and it cost SQL: a function's
    /// <c>SQL statement "COPY t FROM '...'"</c> frame under the <c>COPY t, line N</c> frame its own data raised was
    /// not read as SQL at all. Now a line mistaken for a frame can only be normalized when it did not need to
    /// be.</para>
    /// </summary>
    public static string? RedactContext(string? context)
    {
        if (string.IsNullOrEmpty(context))
        {
            return context;
        }

        var lines = context.Split('\n');
        var output = new List<string>(lines.Length);
        var i = 0;
        while (i < lines.Length)
        {
            var sqlFrame = s_contextSqlFrame.Match(lines[i]);
            var quoted = sqlFrame.Success;
            if (!quoted)
            {
                sqlFrame = s_contextSqlLine.Match(lines[i]);
            }

            if (!sqlFrame.Success)
            {
                output.Add(lines[i]);
                i++;
                continue;
            }

            var closingQuote = quoted ? "\"" : string.Empty;
            var (masked, close) = context.Length > MaxSqlFieldLength
                ? (null, -1)
                : CloseSqlFrame(lines, i, sqlFrame.Length, quoted);
            if (masked is null)
            {
                output.Add(sqlFrame.Value + WithheldStatement + closingQuote);
                return string.Join('\n', output);
            }

            output.Add(sqlFrame.Value + masked + closingQuote);
            i = close + 1;
        }

        return string.Join('\n', output);
    }

    /// <summary>Where the SQL frame opening on <paramref name="start"/> ends: its statement masked and the line of
    /// its close (a closing quote when <paramref name="quoted"/>, a line end otherwise), testing at most
    /// <see cref="MaxBoundaryTests"/> candidate closes; (null, -1) when none reads to its end.</summary>
    private static (string? Masked, int Close) CloseSqlFrame(string[] lines, int start, int headLength, bool quoted)
    {
        var tests = 0;
        for (var end = start; end < lines.Length; end++)
        {
            var closesHere = (!quoted || (lines[end].EndsWith('"') && (end > start || lines[end].Length > headLength)))
                && (end + 1 == lines.Length || s_contextFrameStart.IsMatch(lines[end + 1]));
            if (!closesHere)
            {
                continue;
            }

            if (++tests > MaxBoundaryTests)
            {
                return (null, -1);
            }

            var body = new StringBuilder(lines[start], headLength, lines[start].Length - headLength, MaxSqlFieldLength);
            for (var k = start + 1; k <= end; k++)
            {
                body.Append('\n').Append(lines[k]);
            }

            if (quoted)
            {
                body.Length--;
            }

            if (RedactStoredStatement(body.ToString()) is { } masked)
            {
                return (masked, end);
            }
        }

        return (null, -1);
    }

    /* auto_explain's report (auto_explain.c): "duration: N ms  plan:", and the plan on the lines under it. */
    private static readonly Regex s_planHead = new(
        @"^duration: [0-9]+(?:\.[0-9]+)? ms  plan:$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>What an auto_explain plan becomes in a stored message: its statement's text rides in it.</summary>
    public const string WithheldPlan = "<plan withheld: auto_explain writes the statement's text into it>";

    /// <summary>
    /// A message as it is kept (#3944's review): as PostgreSQL wrote it, unless it is auto_explain's plan report,
    /// which reaches an error log at <c>auto_explain.log_level = warning</c> or above. Its plan carries the statement
    /// (<c>Query Text</c>) and the constants its conditions compare against, in text, JSON, YAML or XML the lexer
    /// cannot read as SQL, so the plan is withheld and the duration line kept. Null in, null out; idempotent.
    /// </summary>
    public static string? WithholdPlan(string? message)
    {
        var newline = message?.IndexOf('\n') ?? -1;
        if (newline < 0 || !s_planHead.IsMatch(message![..newline]))
        {
            return message;
        }

        var indent = newline + 1 < message.Length && message[newline + 1] == '\t' ? "\t" : string.Empty;
        return message[..(newline + 1)] + indent + WithheldPlan;
    }

    /// <summary>The form a monitored target's <c>STATEMENT:</c> companion is fingerprinted in, and never stored in:
    /// quoted literals AND bare numbers out, identifier-glued digits kept. Unchanged since #3601, so
    /// <see cref="Fingerprint"/> stays continuous with the rows already stored. Null in, null out.</summary>
    public static string? RedactStatement(string? statement)
    {
        if (string.IsNullOrEmpty(statement))
        {
            return statement;
        }

        var scrubbed = PgPlanLogParser.s_quotedLiteral.Replace(statement, "'?'");
        return PgPlanLogParser.s_bareNumber.Replace(scrubbed, "?");
    }

    /// <summary>
    /// SQL text that is STORED rather than hashed (#3899, #3915): every literal masked, in ONE left-to-right
    /// pass over PostgreSQL's own lexical rules, so no construct can make the masking lose its place. A
    /// quoted literal of any spelling (<c>'...'</c> with <c>''</c>, <c>E'...'</c> / <c>B'...'</c> /
    /// <c>X'...'</c> / <c>N'...'</c> / <c>U&amp;'...'</c>) and a dollar-quoted string (<c>$$...$$</c>,
    /// <c>$tag$...$tag$</c>) becomes <c>'?'</c>; a numeric literal (<c>42</c>, <c>1.5e3</c>, <c>0x1F</c>,
    /// <c>0o17</c>, <c>0b101</c>, <c>1_000</c>) becomes <c>?</c>; a positional parameter (<c>$1</c>), an
    /// identifier (double-quoted ones included, apostrophes and all) and every operator are kept; comments,
    /// nested ones included, are dropped; whitespace collapses to single spaces.
    ///
    /// <para><b>Backslash.</b> An escape in an <c>E''</c> literal always, never in <c>B''</c> or <c>X''</c>,
    /// and in any other literal only when the CLIENT that sent it runs with <c>standard_conforming_strings</c>
    /// off, which the text does not say. A run of backslashes decides where such a literal ends only when a
    /// quote follows it, and the two settings then disagree exactly when the run is odd: off, the quote is
    /// escaped and the literal runs on; on, the quote ends it. Neither reading can be trusted, so the text is
    /// refused. #3920's review showed that the earlier rule, reading every backslash as an escape, skipped the
    /// closing quote of <c>'C:\'</c> and unmasked the next literal. An even run, or one before any other
    /// character, reads the same both ways.</para>
    ///
    /// <para>Whitespace and comments follow PostgreSQL's lexer, not .NET's: only
    /// <c>space \t \n \r \f \v</c> separate tokens (a non-ASCII character is part of an identifier), and a
    /// line comment ends at <c>\r</c> as well as <c>\n</c>. An identifier holding a non-ASCII character that is
    /// not a letter is masked like a number, and PostgreSQL 18's normalized IN-list marker, the one comment that
    /// is part of a statement's text, is kept (#3920's fourth review).</para>
    ///
    /// <para><b>Fails closed.</b> Null when the text ends inside a literal, a quoted identifier or a block
    /// comment, which is what a statement cut at a cap or a read boundary looks like: no mask can be trusted
    /// to have covered what the cut hid, so the caller keeps no statement at all. The regex pair this
    /// replaced read <c>"owner's count"</c> as the opening of a literal and <c>/* a /* b */ it's */</c> as
    /// ending at the first <c>*/</c>, and in both the following literal was left standing (#3915's review).</para>
    ///
    /// <para>The log-event pipeline keeps only <see cref="Fingerprint"/>'s hash of a statement, so it hashes
    /// <see cref="RedactStatement"/>'s output and a <c>DO</c> body stays part of the shape it hashes; this is
    /// for a surface that keeps the TEXT. Null in, null out.</para>
    /// </summary>
    public static string? RedactStoredStatement(string? statement)
    {
        if (statement is null)
        {
            return null;
        }

        var text = statement;
        var output = new StringBuilder(text.Length);
        var pendingSpace = false;
        var i = 0;

        void Emit(string token)
        {
            if (pendingSpace && output.Length > 0)
            {
                output.Append(' ');
            }

            pendingSpace = false;
            output.Append(token);
        }

        while (i < text.Length)
        {
            var c = text[i];
            var next = i + 1 < text.Length ? text[i + 1] : '\0';

            if (c is ' ' or '\t' or '\n' or '\r' or '\f' or '\v')
            {
                pendingSpace = true;
                i++;
                continue;
            }

            if (c == '-' && next == '-')
            {
                var newline = text.IndexOfAny(['\n', '\r'], i);
                i = newline < 0 ? text.Length : newline + 1;
                pendingSpace = true;
                continue;
            }

            if (c == '/' && next == '*')
            {
                /* PostgreSQL 18's normalized IN-list marker is part of what a normalized statement says, and carries
                   no value: kept, so a normalized IN list does not read as a one-element list once the statement
                   reader's text passes through here (#3920's fourth review). */
                if (string.CompareOrdinal(text, i, NormalizedInListMarker, 0, NormalizedInListMarker.Length) == 0)
                {
                    Emit(NormalizedInListMarker);
                    i += NormalizedInListMarker.Length;
                    continue;
                }

                var depth = 1;
                i += 2;
                while (i < text.Length && depth > 0)
                {
                    if (text[i] == '/' && i + 1 < text.Length && text[i + 1] == '*')
                    {
                        depth++;
                        i += 2;
                    }
                    else if (text[i] == '*' && i + 1 < text.Length && text[i + 1] == '/')
                    {
                        depth--;
                        i += 2;
                    }
                    else
                    {
                        i++;
                    }
                }

                if (depth > 0)
                {
                    return null;
                }

                pendingSpace = true;
                continue;
            }

            /* A double-quoted identifier, U&"..." included: kept whole, quotes and all, because an apostrophe
               inside one is not a literal's opening. */
            if (c == '"' || ((c == 'U' || c == 'u') && next == '&' && i + 2 < text.Length && text[i + 2] == '"'))
            {
                var open = c == '"' ? i : i + 2;
                var close = open + 1;
                while (true)
                {
                    close = text.IndexOf('"', close);
                    if (close < 0)
                    {
                        return null;
                    }

                    if (close + 1 < text.Length && text[close + 1] == '"')
                    {
                        close += 2;
                        continue;
                    }

                    break;
                }

                Emit(text[i..(close + 1)]);
                i = close + 1;
                continue;
            }

            /* A quoted literal: a bare quote, or one of the prefixes that open a literal at a token start (an
               identifier ending in one of those letters was consumed whole below, so it never reaches here). */
            var literalOpen = c == '\'' ? i
                : (c is 'E' or 'e' or 'B' or 'b' or 'X' or 'x' or 'N' or 'n') && next == '\'' ? i + 1
                : (c == 'U' || c == 'u') && next == '&' && i + 2 < text.Length && text[i + 2] == '\'' ? i + 2
                : -1;
            if (literalOpen >= 0)
            {
                var escapeString = literalOpen == i + 1 && (c is 'E' or 'e');
                var bitString = literalOpen == i + 1 && (c is 'B' or 'b' or 'X' or 'x');
                var j = literalOpen + 1;
                var closed = false;
                while (j < text.Length)
                {
                    if (text[j] == '\\' && escapeString)
                    {
                        j += 2;
                        continue;
                    }

                    if (text[j] == '\\' && !bitString)
                    {
                        var run = j;
                        while (run < text.Length && text[run] == '\\')
                        {
                            run++;
                        }

                        if (run < text.Length && text[run] == '\'' && (run - j) % 2 == 1)
                        {
                            return null;
                        }

                        j = run;
                        continue;
                    }

                    if (text[j] == '\'')
                    {
                        if (j + 1 < text.Length && text[j + 1] == '\'')
                        {
                            j += 2;
                            continue;
                        }

                        closed = true;
                        break;
                    }

                    j++;
                }

                if (!closed)
                {
                    return null;
                }

                Emit("'?'");
                i = j + 1;
                continue;
            }

            if (c == '$')
            {
                /* $1: a positional parameter, the place a value was bound, not a value. */
                if (char.IsAsciiDigit(next))
                {
                    var end = i + 1;
                    while (end < text.Length && char.IsAsciiDigit(text[end]))
                    {
                        end++;
                    }

                    Emit(text[i..end]);
                    i = end;
                    continue;
                }

                /* $tag$ or $$: a dollar-quoted string, closed by the same tag. */
                var tagEnd = i + 1;
                while (tagEnd < text.Length && IsIdentifierChar(text[tagEnd], first: tagEnd == i + 1))
                {
                    tagEnd++;
                }

                if (tagEnd < text.Length && text[tagEnd] == '$')
                {
                    var tag = text[i..(tagEnd + 1)];
                    var close = text.IndexOf(tag, tagEnd + 1, StringComparison.Ordinal);
                    if (close < 0)
                    {
                        return null;
                    }

                    Emit("'?'");
                    i = close + tag.Length;
                    continue;
                }

                Emit("$");
                i++;
                continue;
            }

            if (char.IsAsciiDigit(c) || (c == '.' && char.IsAsciiDigit(next)))
            {
                var end = i;
                if (c == '0' && next is 'x' or 'X' or 'o' or 'O' or 'b' or 'B')
                {
                    end += 2;
                    while (end < text.Length && (char.IsAsciiHexDigit(text[end]) || text[end] == '_'))
                    {
                        end++;
                    }
                }
                else
                {
                    while (end < text.Length && (char.IsAsciiDigit(text[end]) || text[end] is '_' or '.'))
                    {
                        end++;
                    }

                    if (end < text.Length && text[end] is 'e' or 'E')
                    {
                        var exponent = end + 1;
                        if (exponent < text.Length && text[exponent] is '+' or '-')
                        {
                            exponent++;
                        }

                        if (exponent < text.Length && char.IsAsciiDigit(text[exponent]))
                        {
                            end = exponent;
                            while (end < text.Length && (char.IsAsciiDigit(text[end]) || text[end] == '_'))
                            {
                                end++;
                            }
                        }
                    }
                }

                Emit("?");
                i = end;
                continue;
            }

            if (IsIdentifierChar(c, first: true))
            {
                var end = i + 1;
                var foreign = c >= '\u0080' && !char.IsLetter(c);
                while (end < text.Length && (IsIdentifierChar(text[end], first: false) || text[end] == '$'))
                {
                    foreign |= text[end] >= '\u0080' && !char.IsLetter(text[end]);
                    end++;
                }

                /* A non-ASCII character that is not a letter (a no-break space, a full-width digit, a zero-width
                   mark) is part of an identifier to PostgreSQL, so `=<NBSP>4111...` is one token to the server. It
                   is also how a value pasted from a web page or typed through an IME reads, and keeping the token
                   kept the value (#3920's fourth review). It goes; a non-ASCII LETTER is still a name. */
                Emit(foreign ? "?" : text[i..end]);
                i = end;
                continue;
            }

            Emit(c.ToString());
            i++;
        }

        return output.ToString();
    }

    /// <summary>PostgreSQL's identifier characters: a letter, an underscore or any non-ASCII character, and
    /// after the first, a digit too.</summary>
    private static bool IsIdentifierChar(char c, bool first) =>
        char.IsAsciiLetter(c) || c == '_' || c >= '\u0080' || (!first && char.IsAsciiDigit(c));

    /// <summary>
    /// Identity of a statement SHAPE: SHA-256 over the REDACTED text, 32 hex characters, the plan hash's
    /// width. Two executions of one statement with different literals fingerprint alike; the raw text
    /// never reaches the hash, so the fingerprint cannot be reversed into a value either.
    /// </summary>
    public static string? Fingerprint(string? redactedStatement)
    {
        if (string.IsNullOrWhiteSpace(redactedStatement))
        {
            return null;
        }

        /* Whitespace-normalised so a statement re-indented by a client fingerprints with its siblings.
           Case is kept: PostgreSQL identifiers are case-sensitive when quoted, and folding would merge
           two different statements. */
        var normalised = Regex.Replace(redactedStatement.Trim(), @"\s+", " ");
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(normalised));
        return Convert.ToHexString(bytes, 0, 16);
    }

    /// <summary>
    /// Identity of one log ENTRY across sightings: SHA-256 over the entry's raw text, 32 hex characters.
    /// The self-hosted tail re-reads an overlapping window every cycle, so the same entry arrives on every
    /// cycle it stays inside the window; the reads dedupe on this the way the deadlock reads dedupe on
    /// <c>deadlock_hash</c>. Over the RAW text on purpose — two entries that store alike (the same error from
    /// one statement shape run with two different values, in the same millisecond, from the same pid) are two
    /// events, and the hash has to keep them apart. A hash of raw text discloses nothing about the text.
    /// </summary>
    public static string RawLineHash(string rawText)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(rawText ?? string.Empty));
        return Convert.ToHexString(bytes, 0, 16);
    }
}
