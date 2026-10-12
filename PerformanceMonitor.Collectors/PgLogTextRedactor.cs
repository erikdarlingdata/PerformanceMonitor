/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using TimeBoundPattern = PerformanceMonitor.Collectors.PgSettingRedactor.TimeBoundPattern;

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
/// <para><b>The one exception: row values in PostgreSQL's own fixed formats.</b> A narrow exception to that decision,
/// not a reversal of it. Some of the server's formats put row data in a fixed slot: a unique or foreign-key
/// violation's <c>Key (col)=(value) ...</c>, a NOT NULL or CHECK failure's <c>Failing row contains (...)</c>, a COPY
/// or JSON input error's CONTEXT quoting the input, and <c>invalid input syntax for type ...: "value"</c>. Unlike
/// open-ended prose these are a closed list of formats, each catalogue's msgstr verbatim (the tables below, guarded
/// like <see cref="LexerErrorFormats"/> by <c>PgLogCatalogueShapeTests</c>), so only the value slot is replaced
/// by <c>?</c> and every column, constraint and table name stays, the way the bind-parameter and PL/pgSQL
/// parameter-list masking already works. Where the log carries a SQLSTATE (csvlog, jsonlog, a <c>%e</c> prefix) it
/// names the class whose fields carry values (<see cref="ValueDetailStates"/>, <see cref="ValueMessageStates"/>),
/// and a field in that class matching no form is withheld (<see cref="WithheldValues"/>) rather than stored as
/// written; without one, a field that opens like a form and matches none whole is withheld the same way.
/// Application-authored text (a RAISE message, a value an application quotes itself) stays as written.</para>
///
/// <list type="bullet">
/// <item><description><see cref="RedactStatement"/> — a monitored target's <c>STATEMENT:</c> companion, which is
/// never stored: only its keyed fingerprint is (<see cref="PgLogHashKey.Fingerprint"/>, #4004), so one statement shape
/// recurs to one fingerprint whatever it ran with. It applies the plan parser's own patterns by INSTANCE
/// (<see cref="PgPlanLogParser.s_quotedLiteral"/>, <see cref="PgPlanLogParser.s_bareNumber"/>), and it is unchanged
/// since #3601, so a shape's fingerprint changes only when the store's key does.</description></item>
/// <item><description><see cref="RedactStoredStatement"/> — SQL text that IS stored: a single-pass lexer that
/// replaces every literal with <c>?</c> and refuses a statement it cannot read to its end
/// (<see cref="WithheldStatement"/>).</description></item>
/// <item><description><see cref="RedactDetail"/> and <see cref="RedactContext"/> — the SQL PostgreSQL writes into
/// a DETAIL or a CONTEXT, found and put through that lexer; every other line of the field is kept as
/// written.</description></item>
/// <item><description><see cref="RedactMessage"/> — a message as written, except auto_explain's plan report, whose
/// plan carries the statement's text in a form the lexer cannot read (<see cref="WithholdPlan"/>, withheld below
/// its duration line), and the SQL a syntax error quotes after <c>at or near</c>, in any catalogue's words
/// (<see cref="LexerErrorFormats"/>, #4006), put through the lexer.</description></item>
/// </list>
/// </summary>
public static class PgLogTextRedactor
{
    /// <summary>The bound every pattern here runs under. The value forms and the frame heads are linear in the
    /// field's length (the longest field they read is <see cref="MaxSqlFieldLength"/>), so this is a backstop, not
    /// a budget: a pattern that outruns it throws, and the field it was reading is withheld
    /// (<see cref="WithheldValues"/>), never kept as written and never left half masked.</summary>
    private static readonly TimeSpan s_matchTimeout = TimeSpan.FromSeconds(1);

    /* The DETAIL shapes that carry SQL (#3920). A deadlock report opens with its wait-for lines, "Process N
       waits for <lock> on <object>; blocked by process M.", then writes each process's query as "Process N:
       <query>" (errdetail_log, each cut at track_activity_query_size). A crashed backend's report is one query,
       "Failed process was running: <query>", and a logged EXECUTE names its PREPARE as "prepare: <statement>"
       (errdetail_execute). The values a statement ran with are one list to the field's end, in SQL's own quoting
       (#3944's review): PL/pgSQL's "parameters: name = 'value', ..." under print_strict_params, on the ERROR a
       STRICT row count raised, and "Parameters: $1 = 'value', ..." under a logged statement (errdetail_params).
       Prose masking had covered their quoted values. A tab leads each continuation line in a stderr log, nothing
       in csvlog. Any other DETAIL is prose, kept as written. */
    private static readonly TimeBoundPattern s_deadlockWaitFor = new(
        @"^\t?Process (?<pid>[0-9]+) waits for .*; blocked by process (?<blocker>[0-9]+)\.$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    private static readonly TimeBoundPattern s_deadlockQueryHead = new(
        @"^\t?Process (?<pid>[0-9]+): ",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    private static readonly TimeBoundPattern s_singleQueryHead = new(
        @"^(?:Failed process was running: |prepare: |(?<parameters>[Pp]arameters: ))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* What a list of values reads as once lexed (#3996's review): `name = '?'` or `name = NULL` pairs, comma
       separated, each name a plain identifier or a `$n`. PL/pgSQL writes a variable's name UNQUOTED
       (format_expr_params), so a name that needed quoting in the function (`"o'k"`, `"a""b"`) opens a literal or an
       identifier the lexer then reads out of step, and a value comes back as a word. Such a list is not this shape,
       and is withheld. */
    private static readonly TimeBoundPattern s_valueList = new(
        @"^(?:[A-Za-z_\u0080-\uFFFF][A-Za-z0-9_$\u0080-\uFFFF]*|\$[0-9]+) = (?:'\?'|NULL)(?:, (?:[A-Za-z_\u0080-\uFFFF][A-Za-z0-9_$\u0080-\uFFFF]*|\$[0-9]+) = (?:'\?'|NULL))*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* A portal's one bound value (`portal "p" parameter $1 = '...'`), or only its number when the value was not
       logged. */
    private static readonly TimeBoundPattern s_oneValue = new(
        @"^\$[0-9]+(?: = (?:'\?'|NULL))?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* A CONTEXT frame that quotes the statement it was running, unescaped (#3920's review): SPI's
       `SQL statement "..."` and PL/pgSQL's expression and assignment frames. */
    private static readonly TimeBoundPattern s_contextSqlFrame = new(
        @"^\t?(?:SQL statement|SQL expression|PL/pgSQL expression|PL/pgSQL assignment) """,
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* A CONTEXT frame that writes SQL WITHOUT quoting it, to the frame's end (#3944's review): postgres_fdw's
       `remote SQL command: <sql>`, whose pushed-down constants are literals, the values a statement was bound
       with (`unnamed portal with parameters: $1 = '...'`, `portal "p" parameter $1 = '...'`, written under
       log_parameter_max_length_on_error), which read as SQL too, and PL/pgSQL's `query: <sql>` under its
       "query did not return data" error. Prose masking had covered their quoted values. A portal's name is written
       unescaped, so it is matched lazily to the first `" with parameters: ` or `" parameter $` (#3996's review): a
       name holding a double quote had failed the match, and the frame's values were kept as prose. What a portal
       frame's values lex to is checked against their shape as well, so a name holding that text only withholds. */
    private static readonly TimeBoundPattern s_contextSqlLine = new(
        @"^\t?(?:remote SQL command: |query: |(?:unnamed portal|portal "".*?"") (?:(?<list>with parameters: )|(?<one>parameter )(?=\$)))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* What can follow an SQL frame: the line after its closing quote starts one of these, or the field ends. */
    private static readonly TimeBoundPattern s_contextFrameStart = new(
        @"^\t?(?:PL/pgSQL function |SQL function |SQL statement ""|SQL expression ""|PL/pgSQL expression ""|PL/pgSQL assignment ""|parallel worker|while |COPY |JSON data, |remote SQL command: |query: |unnamed portal |portal "")",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* The CONTEXT frames that quote no input and no statement, each in the whole of its grammar as PostgreSQL writes it
       (English; another catalogue's wording is not trusted here and is withheld where trust is required). A line is
       trusted as one of these only when all of it fits: the frame word alone does not (`while home: ...` begins like
       a lock wait's frame and is not one). The free parts are names, which PostgreSQL writes unescaped: a relation
       or function name, and a function's argument types; a relation or SQL function name holding a double quote is not trusted, so a line with one is withheld. Locks and vacuum: heapam's XactLockTableWaitErrorCb and
       vacuumlazy's vacuum_error_callback. PL/pgSQL: plpgsql_exec_error_callback, whose statement types are
       plpgsql_stmt_typename's, and whose other tails are its err_text phrases. SQL functions: sql_exec_error_callback
       and the inliner's. */
    private static readonly TimeBoundPattern s_contextPlainFrame = new(
        @"^\t?(?:"
        + @"parallel worker"
        + @"|while (?:(?:updating tuple|deleting tuple|locking tuple|inserting index tuple|checking uniqueness of tuple|rechecking updated tuple|checking exclusion constraint on tuple) \([0-9]+,[0-9]+\)|locking updated version \([0-9]+,[0-9]+\) of tuple) in relation ""[^""]+"""
        + @"|while (?:scanning block [0-9]+(?: offset [0-9]+)? of relation ""[^""]+""|scanning relation ""[^""]+""|vacuuming block [0-9]+ of relation ""[^""]+""|vacuuming relation ""[^""]+"""
        + @"|vacuuming index ""[^""]+"" of relation ""[^""]+""|cleaning up index ""[^""]+"" of relation ""[^""]+""|truncating relation ""[^""]+"" to [0-9]+ blocks)"
        + @"|SQL function ""[^""]+"" (?:statement [0-9]+|during (?:startup|inlining))"
        + @"|PL/pgSQL function (?:inline_code_block|(?>""(?:[^""]|"""")+""|[A-Za-z0-9_$.\u0080-\uFFFF])+\([A-Za-z0-9_$.,\[\] \u0080-\uFFFF""]*\))"
        + @"(?: line [0-9]+ (?:at (?:statement block|assignment|FOR with integer loop variable|FOR over SELECT rows|FOR over cursor|FOR over EXECUTE statement|FOREACH over array|EXIT|CONTINUE|RETURN|RETURN NEXT|RETURN QUERY|RAISE|ASSERT|EXECUTE|IF|CASE|LOOP|WHILE|SQL statement|GET DIAGNOSTICS|OPEN|FETCH|CLOSE|PERFORM|CALL|COMMIT|ROLLBACK|unknown)"
        + @"|during (?:statement block local variable initialization|function entry|function exit|statement block entry|statement block exit|exception cleanup)"
        + @"|while (?:storing call arguments into local variables|casting return value to function's return type)))?"
        + @")\r?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

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

    /// <summary>How many candidate closes one CONTEXT SQL frame may test (#3920's fourth review). Each test re-reads
    /// the text so far, so an unbounded count made a hostile field quadratic: a 180 KB CONTEXT took 21 s, and the
    /// self-hosted tail re-reads its overlap window every cycle. Past the cap the close is not taken, which leaves
    /// the SQL to be withheld. A deadlock report's queries need no cap: their heads are found before any is
    /// read, and each is read once (<see cref="RedactDetail"/>).</summary>
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
    /// <para><b>Where one deadlock query ends</b> (#3944's review). DeadLockReport writes one <c>Process N:</c> line
    /// per wait-for line, in the wait-for lines' order, so the query heads are known before any query is read: a
    /// line that starts with the next waiter's <c>Process N:</c> starts its query, and every other line belongs to
    /// the query above it. Each query is then read on its own, from its own first character. When the heads found
    /// are not the waiters in that order, each once (a query holding a line shaped like another process's head, a
    /// report that goes on in another shape after its wait-for lines), where one query ends is unknowable and every
    /// query is withheld; so is every query of a report longer than <see cref="MaxSqlFieldLength"/>. #3920's fourth
    /// review took a head only where the query before it read to its end, which let one query that did not (cut at
    /// <c>track_activity_query_size</c> inside a literal, or written to look that way) take in the next and read it
    /// out of step, a literal kept as a word. A report cut before its last queries keeps the ones it has.</para>
    ///
    /// <para><b>A cut report</b> (#3996's review). A report can also be cut between its lines: an RDS chunk is
    /// consume-once, and the assembler stores an entry cut there as it arrived. Cut before a waiter's real head, a
    /// line inside the query above it shaped like that head is the only one there is, so the order check passes,
    /// and when that query did not read to its end the look-alike's text is read out of step. So after a query that
    /// does not read to its end, a following head is trusted only when <paramref name="complete"/> says the caller
    /// saw a companion field after the DETAIL (a HINT, CONTEXT or STATEMENT; DeadLockReport always writes its HINT),
    /// which proves every real head is present and a look-alike a second head the order check refuses. Otherwise
    /// that query and every one after it are withheld. Null in, null out; idempotent.</para>
    /// </summary>
    /// <param name="detail">The DETAIL field's text.</param>
    /// <param name="complete">True only when a companion field followed the DETAIL in the same entry. False, the
    /// default, fails closed.</param>
    public static string? RedactDetail(string? detail, bool complete = false, string? sqlState = null)
    {
        try
        {
            return RedactDetailCore(detail, complete, sqlState);
        }
        catch (RegexMatchTimeoutException)
        {
            return WithheldValues;
        }
    }

    private static string? RedactDetailCore(string? detail, bool complete, string? sqlState)
    {
        if (string.IsNullOrEmpty(detail))
        {
            return detail;
        }

        /* The row values PostgreSQL writes into an integrity violation's DETAIL, masked in their slot (the one
           exception to #3944: a closed list of the server's own fixed formats, in every catalogue). A DETAIL whose
           SQLSTATE puts it in that class, or that opens like one of the forms, and matches none whole is withheld:
           an application's DETAIL under a RAISE with that errcode, or a shape this build does not read. */
        if (detail.Length <= MaxSqlFieldLength && MaskValues(detail, s_detailForms) is { } maskedValues)
        {
            return maskedValues;
        }

        if (InValueClass(sqlState, ValueDetailStates) || OpensValueForm(detail, s_detailForms, message: false))
        {
            return WithheldValues;
        }

        var single = s_singleQueryHead.Match(detail);
        if (single.Success)
        {
            var masked = MaskQuery(detail[single.Length..]);
            return single.Value + (single.Groups["parameters"].Success && !s_valueList.IsMatch(masked) ? WithheldStatement : masked);
        }

        var lines = detail.Split('\n');
        var waits = 0;
        var waiters = new List<string>();
        while (waits < lines.Length && s_deadlockWaitFor.Match(lines[waits]) is { Success: true } wait)
        {
            waiters.Add(wait.Groups["pid"].Value);
            waits++;
        }

        /* Prose, or a deadlock report cut before its first query: nothing here is SQL. */
        if (waits == 0 || waits == lines.Length)
        {
            return detail;
        }

        var output = new List<string>(lines.Length - waits + 1) { string.Join('\n', lines, 0, waits) };
        var first = s_deadlockQueryHead.Match(lines[waits]);
        var firstHead = first.Success ? first.Value : lines[waits].StartsWith('\t') ? "\t" : string.Empty;

        /* Too long to read at all, checked before the heads are looked for (#3996's review): a hostile DETAIL of
           many wait-for lines and many head lines made that search quadratic. */
        if (detail.Length > MaxSqlFieldLength)
        {
            output.Add(firstHead + WithheldStatement);
            return string.Join('\n', output);
        }

        /* Every line that starts with a waiter's head, in order. They must be the waiters themselves, from the
           first, each once, with the first on the line after the wait-for lines. */
        var isWaiter = new HashSet<string>(waiters, StringComparer.Ordinal);
        var heads = new List<(int Line, Match Head)>();
        var inOrder = true;
        for (var k = waits; k < lines.Length && inOrder; k++)
        {
            var head = s_deadlockQueryHead.Match(lines[k]);
            if (head.Success && isWaiter.Contains(head.Groups["pid"].Value))
            {
                inOrder = heads.Count < waiters.Count && head.Groups["pid"].Value == waiters[heads.Count];
                heads.Add((k, head));
            }
        }

        if (!inOrder || heads.Count == 0 || heads[0].Line != waits)
        {
            output.Add(firstHead + WithheldStatement);
            return string.Join('\n', output);
        }

        /* Whole means every waiter's head is here: a report that claims it and lacks one is not what DeadLockReport
           wrote, so the claim is not taken. */
        var whole = complete && heads.Count == waiters.Count;
        var outOfStep = false;
        for (var h = 0; h < heads.Count; h++)
        {
            var (line, head) = heads[h];
            if (outOfStep)
            {
                output.Add(head.Value + WithheldStatement);
                continue;
            }

            /* Sized to this query (#3996's round-2 review): sized to the whole DETAIL, a 63 KB report of 880
               waiters allocated 107 MB a call. */
            var end = h + 1 < heads.Count ? heads[h + 1].Line : lines.Length;
            var query = new StringBuilder(
                lines[line], head.Length, lines[line].Length - head.Length, LengthOf(lines, line, end) - head.Length);
            for (var k = line + 1; k < end; k++)
            {
                query.Append('\n').Append(lines[k]);
            }

            var masked = RedactStoredStatement(query.ToString());
            output.Add(head.Value + (masked ?? WithheldStatement));
            outOfStep = masked is null && !whole;
        }

        return string.Join('\n', output);
    }

    /// <summary>One query a DETAIL carries, masked, or withheld when it cannot be read to its end or is too long
    /// to read at all. Also a stored deadlock's victim statement, the same query outside its DETAIL (#4005).</summary>
    internal static string MaskQuery(string query) =>
        query.Length > MaxSqlFieldLength ? WithheldStatement : RedactStoredStatement(query) ?? WithheldStatement;

    /// <summary>The length of <paramref name="lines"/> from <paramref name="from"/> up to <paramref name="to"/>,
    /// joined by newlines.</summary>
    private static int LengthOf(string[] lines, int from, int to)
    {
        var length = to - from - 1;
        for (var k = from; k < to; k++)
        {
            length += lines[k].Length;
        }

        return length;
    }

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
    /// not read as SQL at all. Now a line mistaken for a frame can only be normalized when it did not need to be,
    /// or withheld: a frame, mistaken or not, that has not closed by the line where the next SQL frame opens is
    /// withheld with the rest of the field, so a look-alike's text never decides how a real statement is
    /// read.</para>
    /// </summary>
    public static string? RedactContext(string? context)
    {
        try
        {
            return RedactContextCore(context);
        }
        catch (RegexMatchTimeoutException)
        {
            return WithheldValues;
        }
    }

    private static string? RedactContextCore(string? context)
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
            /* A COPY frame's input line or column, and the JSON parser's input, masked in their slot (the one
               exception to #3944). A line that opens like one of those frames and matches none whole (a CSV value
               holding a newline runs onto the next line) is withheld with everything after it, which may be the rest
               of that value. */
            var tab = lines[i].StartsWith('\t') ? "\t" : string.Empty;
            var body = tab.Length == 0 ? lines[i] : lines[i][1..];
            if (context.Length <= MaxSqlFieldLength && MaskValues(body, s_contextForms, out var carriesValue) is { } maskedLine)
            {
                output.Add(tab + maskedLine);
                i++;

                /* A COPY value in CSV mode, or a JSON input, can hold a newline: its first physical line then ends
                   in a quote or is cut where the value goes on, and the form above reads that line whole. What
                   follows a value-bearing frame is another frame, or nothing. A line is another frame only when ALL
                   of it reads as one (ReadsAsFrame): beginning with a frame word is not enough, since a value's next
                   line can begin with any of them. Any other line is the rest of the value, and is withheld with
                   everything after it. */
                if (carriesValue && i < lines.Length && !ReadsAsFrame(lines[i]))
                {
                    output.Add((lines[i].StartsWith('\t') ? "\t" : string.Empty) + WithheldValues);
                    return string.Join('\n', output);
                }

                continue;
            }

            if (OpensValueForm(body, s_contextForms, message: false))
            {
                output.Add(tab + WithheldValues);
                return string.Join('\n', output);
            }

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
            if ((sqlFrame.Groups["list"].Success && masked is not null && !s_valueList.IsMatch(masked))
                || (sqlFrame.Groups["one"].Success && masked is not null && !s_oneValue.IsMatch(masked)))
            {
                masked = null;
            }

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

    /// <summary>Whether a CONTEXT line, the one after a frame that quoted a value, reads whole as a frame
    /// PostgreSQL writes: a COPY or JSON value form (<see cref="MaskValues(string, ValueForm[])"/>), an SQL frame that
    /// <see cref="CloseSqlFrame"/> then reads to its close or withholds, or a frame with no input in it that fits its
    /// grammar to the end of the line (<see cref="s_contextPlainFrame"/>). A line that only begins with a frame word
    /// (<c>while </c>, <c>parallel worker</c>, <c>PL/pgSQL function </c>, <c>SQL function </c>, <c>unnamed portal </c>,
    /// <c>portal "</c>) is not one.</summary>
    private static bool ReadsAsFrame(string line)
    {
        var body = line.StartsWith('\t') ? line[1..] : line;
        return s_contextSqlFrame.IsMatch(line)
            || s_contextSqlLine.IsMatch(line)
            || s_contextPlainFrame.IsMatch(line)
            || MaskValues(body, s_contextForms) is not null;
    }

    /// <summary>Where the SQL frame opening on <paramref name="start"/> ends: its statement masked and the line of
    /// its close (a closing quote when <paramref name="quoted"/>, a line end otherwise), testing at most
    /// <see cref="MaxBoundaryTests"/> candidate closes; (null, -1) when none reads to its end.</summary>
    private static (string? Masked, int Close) CloseSqlFrame(string[] lines, int start, int headLength, bool quoted)
    {
        var tests = 0;
        for (var end = start; end < lines.Length; end++)
        {
            /* A line that opens an SQL frame of its own is one this frame had to close before (#3944's review).
               Read on past it, a look-alike frame (a COPY value's or a JSON line's continuation) would take the
               real frame's statement into its body and lex it from whatever state the look-alike's text left,
               where a literal can read as the inside of an identifier and be kept. Nothing closed before it, so
               this frame is withheld with the rest of the field. */
            if (end > start && (s_contextSqlFrame.IsMatch(lines[end]) || s_contextSqlLine.IsMatch(lines[end])))
            {
                return (null, -1);
            }

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

            /* Sized to this body, as a deadlock query is (#3996's round-2 review): sized to the field's cap, a
               27 KB CONTEXT of 1,000 one-line frames allocated 125 MB a call. */
            var body = new StringBuilder(
                lines[start], headLength, lines[start].Length - headLength, LengthOf(lines, start, end + 1) - headLength);
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
    private static readonly TimeBoundPattern s_planHead = new(
        @"^duration: [0-9]+(?:\.[0-9]+)? ms  plan:$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /// <summary>Whether a message is auto_explain's plan head, <c>duration: N ms  plan:</c>, whose plan is on the lines
    /// under it (#5097). The one spelling of the head, shared with <see cref="WithholdPlan"/>.</summary>
    public static bool IsPlanHead(string? firstLineMessage)
    {
        try
        {
            return firstLineMessage is not null && s_planHead.IsMatch(firstLineMessage);
        }
        catch (RegexMatchTimeoutException)
        {
            // Not read to its end: withheld the way a plan is.
            return true;
        }
    }

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
        if (newline < 0)
        {
            return message;
        }

        try
        {
            if (!s_planHead.IsMatch(message![..newline]))
            {
                return message;
            }
        }
        catch (RegexMatchTimeoutException)
        {
            return WithheldPlan;
        }

        var indent = newline + 1 < message.Length && message[newline + 1] == '\t' ? "\t" : string.Empty;
        return message[..(newline + 1)] + indent + WithheldPlan;
    }

    /// <summary>Every catalogue's <c>Key %s already exists.</c> and <c>Key %s is duplicated.</c>: a unique violation's DETAIL, its one slot the index's <c>(columns)=(values)</c>.</summary>
    public static readonly IReadOnlyList<string> KeyExistsDetailFormats =
    [
        "Key %s already exists.",
        "Schlüssel »%s« existiert bereits.", // de
        "Ya existe la llave %s.", // es
        "La clé « %s » existe déjà.", // fr
        "Kunci « %s » sudah ada.", // id
        "La chiave %s esiste già.", // it
        "キー %s はすでに存在します。", // ja
        "გასაღები უკვე არსებობს: %s.", // ka
        "%s 키가 이미 있습니다.", // ko
        "Klucz %s już istnieje.", // pl
        "Chave %s já existe.", // pt_BR
        "Ключ \"%s\" уже существует.", // ru
        "Nyckeln %s existerar redan.", // sv
        "\"%s\" anahtarı zaten mevcut", // tr
        "Ключ %s вже існує.", // uk
        "键值\"%s\" 已经存在", // zh_CN
        "Key %s is duplicated.",
        "Schlüssel %s ist doppelt vorhanden.", // de
        "La llave %s está duplicada.", // es
        "La clé %s est dupliquée.", // fr
        "Key %s terduplikasi.", // id
        "La chiave %s è duplicata.", // it
        "キー%sは重複しています。", // ja
        "გასაღები %s დუბლირებულია.", // ka
        "%s 키가 중복됨", // ko
        "Klucz %s jest zdublowany.", // pl
        "Chave %s está duplicada.", // pt_BR
        "Ключ %s дублируется.", // ru
        "Nyckeln %s är duplicerad.", // sv
        "%s anahtarı (key) çoğaltılmış (duplicated).", // tr
        "Ключ %s дублюється.", // uk
        "键值%s重复了", // zh_CN
    ];

    /// <summary>Every catalogue's <c>Key %s conflicts with existing key %s.</c> and <c>Key %s conflicts with key %s.</c>: an exclusion violation's DETAIL, both slots <c>(columns)=(values)</c> of the same index.</summary>
    public static readonly IReadOnlyList<string> KeyConflictDetailFormats =
    [
        "Key %s conflicts with existing key %s.",
        "Schlüssel %s kollidiert mit vorhandenem Schlüssel %s.", // de
        "La llave %s está en conflicto con la llave existente %s.", // es
        "La clé %s est en conflit avec la clé existante %s.", // fr
        "Kunci %s bermasalah dengan kunci yang sudah ada %s.", // id
        "La chiave %s è in conflitto con la chiave esistente %s.", // it
        "キー %s が既存のキー %s と競合しています", // ja
        "გასაღები %s კონფლიქტობს არსებულ გასაღებ %s-სთან.", // ka
        "%s 키가 이미 있는 %s 키와 충돌합니다.", // ko
        "Klucz %s jest sprzeczny z istniejącym kluczem %s.", // pl
        "A chave %s está em conflito com a chave existente %s.", // pt_BR
        "Ключ %s конфликтует с существующим ключом %s.", // ru
        "Nyckel %s står i konflilkt med existerande nyckel %s.", // sv
        "%s anahtarı, mevcut %s anahtarıyla uyuşmuyor.", // tr
        "Ключ %s конфліктує з існуючим ключем %s.", // uk
        "键%s与已存在的键%s冲突", // zh_CN
        "Key %s conflicts with key %s.",
        "Schlüssel %s kollidiert mit Schlüssel %s.", // de
        "La llave %s está en conflicto con la llave %s.", // es
        "La clé %s est en conflit avec la clé %s.", // fr
        "Kunci %s bermasalah dengan kunci %s.", // id
        "La chiave %s è in conflitto con la chiave %s.", // it
        "キー %s がキー %s と競合しています", // ja
        "გასაღები %s კონფლიქტობს გასაღებ %s-სთან.", // ka
        "%s 키와 %s 가 충돌함", // ko
        "Klucz %s jest sprzeczny z kluczem %s.", // pl
        "A chave %s está em conflito com a chave %s.", // pt_BR
        "Ключ %s конфликтует с ключом %s.", // ru
        "Nyckel %s står i konflikt med nyckel %s.", // sv
        "%s anahtarı,  %s anahtarı ile uyuşmuyor.", // tr
        "Ключ %s конфліктує з ключем %s.", // uk
        "键%s与另外一个键%s冲突", // zh_CN
    ];

    /// <summary>Every catalogue's <c>Key (%s)=(%s) is not present in table "%s".</c> and <c>Key (%s)=(%s) is still referenced from table "%s".</c>: a foreign-key violation's DETAIL, the second slot the key's values.</summary>
    public static readonly IReadOnlyList<string> KeyReferenceDetailFormats =
    [
        "Key (%s)=(%s) is not present in table \"%s\".",
        "Schlüssel (%s)=(%s) ist nicht in Tabelle »%s« vorhanden.", // de
        "La llave (%s)=(%s) no está presente en la tabla «%s».", // es
        "La clé (%s)=(%s) n'est pas présente dans la table « %s ».", // fr
        "Key (%s)=(%s) tidak ada dalam table « %s ».", // id
        "La chiave (%s)=(%s) non è presente nella tabella \"%s\".", // it
        "テーブル\"%3$s\"にキー(%1$s)=(%2$s)がありません", // ja
        "გასაღები (%s)=(%s) არაა წარმოდგენილი ცხრილში \"%s\".", // ka
        "(%s)=(%s) 키가 \"%s\" 테이블에 없습니다.", // ko
        "Klucz (%s)=(%s) nie występuje w tabeli \"%s\".", // pl
        "Chave (%s)=(%s) não está presente na tabela \"%s\".", // pt_BR
        "Ключ (%s)=(%s) отсутствует в таблице \"%s\".", // ru
        "Nyckel (%s)=(%s) finns inte i tabellen \"%s\".", // sv
        "\"%3$s\" tablosunda (%1$s)=(%2$s) anahtarı mevcut değildir.", // tr
        "Ключ (%s)=(%s) не присутній в таблиці \"%s\".", // uk
        "键值对(%s)=(%s)没有在表\"%s\"中出现.", // zh_CN
        "Key (%s)=(%s) is still referenced from table \"%s\".",
        "Auf Schlüssel (%s)=(%s) wird noch aus Tabelle »%s« verwiesen.", // de
        "La llave (%s)=(%s) todavía es referida desde la tabla «%s».", // es
        "La clé (%s)=(%s) est toujours référencée à partir de la table « %s ».", // fr
        "Key (%s)=(%s) masih direferensikan dari table « %s ».", // id
        "La chiave (%s)=(%s) è ancora referenziata dalla tabella \"%s\".", // it
        "キー(%s)=(%s)はまだテーブル\"%s\"から参照されています", // ja
        "(%s)=(%s) 키가 \"%s\" 테이블에서 여전히 참조됩니다.", // ko
        "Klucz (%s)=(%s) ma wciąż odwołanie w tabeli \"%s\".", // pl
        "Chave (%s)=(%s) ainda é referenciada pela tabela \"%s\".", // pt_BR
        "На ключ (%s)=(%s) всё ещё есть ссылки в таблице \"%s\".", // ru
        "Nyckeln (%s)=(%s) refereras fortfarande till från tabell \"%s\".", // sv
        "(%s)=(%s) anahtarı \"%s\" tablosundan hala referans edilmektedir.", // tr
        "На ключ (%s)=(%s) все ще є посилання в таблиці \"%s\".", // uk
        "键值对(%s)=(%s)仍然是从表\"%s\"引用的.", // zh_CN
        "Key (%s)=(%s) is referenced from table \"%s\".",
        "Auf Schlüssel (%s)=(%s) wird aus Tabelle »%s« verwiesen.", // de
        "キー(%s)=(%s)はテーブル\"%s\"から参照されています。", // ja
        "გასაღებს (%s)=(%s) მიმართავენ ცხრილიდან \"%s\".", // ka
        "На ключ (%s)=(%s) есть ссылки в таблице \"%s\".", // ru
        "Nyckeln (%s)=(%s) refereras från tabell \"%s\".", // sv
        "Ключ (%s)=(%s) посилається з таблиці \"%s\".", // uk
    ];

    /// <summary>Every catalogue's <c>Key is not present in table "%s".</c> and <c>Key is still referenced from table "%s".</c>: the foreign-key DETAIL written when the role may not read the key's columns, which carries no value.</summary>
    public static readonly IReadOnlyList<string> KeyReferenceUnreadableDetailFormats =
    [
        "Key is not present in table \"%s\".",
        "Der Schlüssel ist nicht in Tabelle »%s« vorhanden.", // de
        "La llave no está presente en la tabla «%s».", // es
        "La clé n'est pas présente dans la table « %s ».", // fr
        "La chiave non è presente nella tabella \"%s\".", // it
        "テーブル\"%s\"にキーがありません。", // ja
        "ცხრილში \"%s\" გასაღები არ არსებობს.", // ka
        "\"%s\" 테이블에 키가 없습니다.", // ko
        "Klucz nie występuje w tabeli \"%s\".", // pl
        "A chave não está presente na tabela \"%s\".", // pt_BR
        "Ключ отсутствует в таблице \"%s\".", // ru
        "Nyckeln finns inte i tabellen \"%s\".", // sv
        "Ключ не присутній в таблиці \"%s\".", // uk
        "表\"%s\"中没有出现键。", // zh_CN
        "Key is still referenced from table \"%s\".",
        "Auf den Schlüssel wird noch aus Tabelle »%s« verwiesen.", // de
        "La llave todavía es referida desde la tabla «%s».", // es
        "La clé est toujours référencée à partir de la table « %s ».", // fr
        "La chiave è ancora referenziata dalla tabella \"%s\".", // it
        "テーブル\"%s\"からキーがまだ参照されています。", // ja
        "\"%s\" 테이블에서 키가 여전히 참조됩니다.", // ko
        "Klucz ma wciąż odwołania w tabeli \"%s\".", // pl
        "A chave ainda é referenciada pela tabela \"%s\".", // pt_BR
        "На ключ всё ещё есть ссылки в таблице \"%s\".", // ru
        "Nyckel refereras fortfarande till från tabell \"%s\".", // sv
        "На ключ все ще є посилання в таблиці \"%s\".", // uk
        "表\"%s\"中仍然有对键的引用。", // zh_CN
        "Key is referenced from table \"%s\".",
        "Auf den Schlüssel wird aus Tabelle »%s« verwiesen.", // de
        "キーがテーブル\"%s\"から参照されています。", // ja
        "გასაღებს მიმართავენ ცხრილიდან \"%s\".", // ka
        "На ключ есть ссылки в таблице \"%s\".", // ru
        "Nyckel refereras från tabell \"%s\".", // sv
        "Ключ посилається з таблиці \"%s\".", // uk
    ];

    /// <summary>Every catalogue's <c>Failing row contains %s.</c>: a NOT NULL or CHECK failure's DETAIL, its slot the whole row's <c>(values)</c>.</summary>
    public static readonly IReadOnlyList<string> FailingRowDetailFormats =
    [
        "Failing row contains %s.",
        "Fehlgeschlagene Zeile enthält %s.", // de
        "La fila que falla contiene %s.", // es
        "La ligne en échec contient %s.", // fr
        "Baris gagal berisi %s", // id
        "La riga in errore contiene %s.", // it
        "失敗した行は%sを含みます", // ja
        "შეცდომიანი მწკრივი \"%s\"-ს შეიცავს.", // ka
        "실패한 자료: %s", // ko
        "Niepoprawne ograniczenia wiersza %s.", // pl
        "Registro que falhou contém %s.", // pt_BR
        "Ошибочная строка содержит %s.", // ru
        "Misslyckande rad innehåller %s.", // sv
        "Hata veren satır %s içeriyor.", // tr
        "Помилковий рядок містить %s.", // uk
        "失败, 行包含%s.", // zh_CN
    ];

    /// <summary>Every catalogue's <c>Partition key of the failing row contains %s.</c>: a row no partition takes, its slot <c>(columns) = (values)</c>.</summary>
    public static readonly IReadOnlyList<string> PartitionKeyDetailFormats =
    [
        "Partition key of the failing row contains %s.",
        "Partitionierungsschlüssel der fehlgeschlagenen Zeile enthält %s.", // de
        "La llave de particionamiento de la fila que falla contiene %s.", // es
        "La clé de partitionnement de la ligne en échec contient %s.", // fr
        "La chiave di partizione della riga sbagliata contiene %s.", // it
        "失敗した行のパーティションキーは%sを含みます。", // ja
        "შეცდომიანი მწკრივის დანაყოფის გასაღები \"%s\"-ს შეიცავს.", // ka
        "실패한 로우의 파티션 키 값: %s", // ko
        "A chave de partição da linha com falha contém %s.", // pt_BR
        "Ключ секционирования для неподходящей строки содержит %s.", // ru
        "Partitioneringsnyckel för den trasiga raden innehåller %s.", // sv
        "Başarısız olan satırda bölümleme anahtarı %s içeriyor.", // tr
        "Ключ секціонування для невідповідного рядка містить %s.", // uk
        "失败行的分区键包含%s.", // zh_CN
    ];

    /// <summary>Every catalogue's <c>COPY %s, line %llu: "%s"</c>: a COPY frame quoting the input line.</summary>
    public static readonly IReadOnlyList<string> CopyLineContextFormats =
    [
        "COPY %s, line %llu: \"%s\"",
        "COPY %s, ligne %llu : « %s »", // fr
        "COPY %s, riga %llu: \"%s\"", // it
        "COPY %s, linha %llu: \"%s\"", // pt_BR
        "%sのCOPY、行 %llu: \"%s\"", // ja
        "%s 복사, %llu번째 줄: \"%s\"", // ko
    ];

    /// <summary>Every catalogue's <c>COPY %s, line %llu, column %s: "%s"</c>: a COPY frame quoting one column's input.</summary>
    public static readonly IReadOnlyList<string> CopyColumnContextFormats =
    [
        "COPY %s, line %llu, column %s: \"%s\"",
        "COPY %s, ligne %llu, colonne %s : « %s »", // fr
        "COPY %s, riga %llu, colonna %s: \"%s\"", // it
        "COPY %s, linha %llu, coluna %s: \"%s\"", // pt_BR
        "%sのCOPY、行 %llu、列 %s: \"%s\"", // ja
        "%s 복사, %llu번째 줄, %s 열: \"%s\"", // ko
    ];

    /// <summary>Every catalogue's COPY frames that quote no input: the relation alone, the line, the column, and a null input.</summary>
    public static readonly IReadOnlyList<string> CopyBareContextFormats =
    [
        "COPY %s",
        "%sのCOPY", // ja
        "COPY %s, line %llu",
        "COPY %s, ligne %llu", // fr
        "COPY %s, riga %llu", // it
        "COPY %s, linha %llu", // pt_BR
        "COPY %s, line %llu, column %s",
        "COPY %s, ligne %llu, colonne %s", // fr
        "COPY %s, riga %llu, colonna %s", // it
        "COPY %s, linha %llu, coluna %s", // pt_BR
        "COPY %s, line %llu, column %s: null input",
        "COPY %s, ligne %llu, colonne %s : NULL en entrée", // fr
        "COPY %s, riga %llu, colonna %s: input nullo", // it
        "COPY %s, linha %llu, coluna %s: entrada nula", // pt_BR
        "%sのCOPY、行 %llu、列 %s", // ja
        "%sのCOPY、行 %llu", // ja
        "%sのCOPY、行 %llu、列 %s: null が入力されました", // ja
        "%s 복사, %llu번째 줄, %s 열", // ko
        "%s 복사, %llu번째 줄", // ko
    ];

    /// <summary>Every catalogue's <c>JSON data, line %d: %s%s%s</c>: the JSON parser's frame, everything after the line number the input around the error.</summary>
    public static readonly IReadOnlyList<string> JsonContextFormats =
    [
        "JSON data, line %d: %s%s%s",
        "JSON-Daten, Zeile %d: %s%s%s", // de
        "Datos JSON, línea %d: %s%s%s", // es
        "données JSON, ligne %d : %s%s%s", // fr
        "data JSON, baris %d : %s%s%s", // id
        "dati JSON, riga %d: %s%s%s", // it
        "JSONデータ、%d行目: %s%s%s", // ja
        "JSON მონაცემი, ხაზი %d: %s%s%s", // ka
        "JSON 자료, %d 번째 줄: %s%s%s", // ko
        "Dane JSON, linia %d: %s%s%s", // pl
        "dado JSON, linha %d: %s%s%s", // pt_BR
        "данные JSON, строка %d: %s%s%s", // ru
        "JSON-data, rad %d: %s%s%s", // sv
        "Дані JSON, рядок %d: %s%s%s", // uk
        "JSON数据, 行 %d: %s%s%s", // zh_CN
    ];

    /// <summary>Every catalogue's <c>invalid input syntax for type %s: "%s"</c>: an input function's message, its second slot the rejected value.</summary>
    public static readonly IReadOnlyList<string> InputSyntaxMessageFormats =
    [
        "invalid input syntax for type %s: \"%s\"",
        "ungültige Eingabesyntax für Typ %s: »%s«", // de
        "la sintaxis de entrada no es válida para tipo %s: «%s»", // es
        "syntaxe en entrée invalide pour le type %s : « %s »", // fr
        "sintaks masukan tidak valid untuk tipe %s : « %s »", // id
        "sintassi di input non valida per il tipo %s: \"%s\"", // it
        "\"%s\"型の入力構文が不正です: \"%s\"", // ja
        "არასწორი შეყვანის სინტაქსი ტიპისთვის %s: \"%s\"", // ka
        "%s 자료형 대한 잘못된 입력: \"%s\"", // ko
        "nieprawidłowa składnia wejścia dla typu %s: \"%s\"", // pl
        "sintaxe de entrada é inválida para tipo %s: \"%s\"", // pt_BR
        "неверный синтаксис для типа %s: \"%s\"", // ru
        "ogiltig indatasyntax för type %s: \"%s\"", // sv
        "%s tipi için geçersiz giriş sözdizimi: \"%s\"", // tr
        "неприпустимий синтаксис для типу %s: \"%s\"", // uk
        "无效的类型 %s 输入语法: \"%s\"", // zh_CN
        "%s型の入力構文が不正です: \"%s\"", // ja
    ];

    /// <summary>Every catalogue's <c>invalid input syntax for type %s</c>: the JSON parser's message, which carries no value (its DETAIL and CONTEXT do).</summary>
    public static readonly IReadOnlyList<string> InputSyntaxBareMessageFormats =
    [
        "invalid input syntax for type %s",
        "ungültige Eingabesyntax für Typ %s", // de
        "sintaxis de entrada no válida para tipo %s", // es
        "syntaxe en entrée invalide pour le type %s", // fr
        "sintassi di input non valida per il tipo %s", // it
        "%s型に対する不正な入力構文", // ja
        "არასწორი შეყვანის სინტაქსი ტიპისთვის %s", // ka
        "%s 자료형에 대한 잘못된 입력 구문", // ko
        "nieprawidłowa składnia wejścia dla typu %s", // pl
        "sintaxe de entrada inválida para o tipo de dados %s", // pt_BR
        "неверный синтаксис для типа %s", // ru
        "ogiltig indatassyntax för typen %s", // sv
        "%s tipi için geçersiz girdi sözdizimi", // tr
        "неприпустимий вхідний синтаксис для типу %s", // uk
        "类型%s的输入语法无效", // zh_CN
    ];

    /// <summary>Every catalogue's message that quotes the value an input function or a range check rejected, the first
    /// slot the value: <c>malformed array literal: "%s"</c> (and the record, range and multirange forms),
    /// <c>value "%s" is out of range for type %s</c>, <c>"%s" is out of range for type double precision</c> (and
    /// <c>real</c>), and the date and time range checks (<c>date/time field value out of range: "%s"</c>,
    /// <c>interval field value out of range: "%s"</c>, <c>time zone displacement out of range: "%s"</c>,
    /// <c>date out of range: "%s"</c>, <c>timestamp out of range: "%s"</c>). Each major version's wording is listed.</summary>
    public static readonly IReadOnlyList<string> QuotedValueMessageFormats =
    [
        "malformed array literal: \"%s\"",
        "malformed record literal: \"%s\"",
        "malformed range literal: \"%s\"",
        "malformed multirange literal: \"%s\"",
        "value \"%s\" is out of range for type %s",
        "\"%s\" is out of range for type double precision",
        "\"%s\" is out of range for type real",
        "date/time field value out of range: \"%s\"",
        "interval field value out of range: \"%s\"",
        "time zone displacement out of range: \"%s\"",
        "date out of range: \"%s\"",
        "timestamp out of range: \"%s\"",
        "fehlerhafte Arraykonstante: »%s«", // de
        "Wert »%s« ist außerhalb des gültigen Bereichs für Typ %s", // de
        "date ist außerhalb des gültigen Bereichs: »%s«", // de
        "Datum/Zeit-Feldwert ist außerhalb des gültigen Bereichs: »%s«", // de
        "»interval«-Feldwert ist außerhalb des gültigen Bereichs: »%s«", // de
        "Zeitzonenunterschied ist außerhalb des gültigen Bereichs: »%s«", // de
        "»%s« ist außerhalb des gültigen Bereichs für Typ real", // de
        "»%s« ist außerhalb des gültigen Bereichs für Typ double precision", // de
        "fehlerhafte Multirange-Konstante: »%s«", // de
        "fehlerhafte Bereichskonstante: »%s«", // de
        "fehlerhafte Record-Konstante: »%s«", // de
        "timestamp ist außerhalb des gültigen Bereichs: »%s«", // de
        "literal de array mal formado: «%s»", // es
        "el valor «%s» está fuera de rango para el tipo %s", // es
        "fecha fuera de rango: «%s»", // es
        "valor de hora/fecha fuera de rango: «%s»", // es
        "valor de interval fuera de rango: «%s»", // es
        "desplazamiento de huso horario fuera de rango: «%s»", // es
        "«%s» está fuera de rango para el tipo real", // es
        "«%s» está fuera de rango para el tipo double precision", // es
        "literal de multirango mal formado: «%s»", // es
        "literal de rango mal formado: «%s»", // es
        "literal de record mal formado: «%s»", // es
        "timestamp fuera de rango: «%s»", // es
        "timestamp en dehors de limites : « %s »", // fr
        "tableau litéral mal formé : « %s »", // fr
        "la valeur « %s » est en dehors des limites pour le type %s", // fr
        "date en dehors des limites : « %s »", // fr
        "valeur du champ date/time en dehors des limites : « %s »", // fr
        "valeur du champ interval en dehors des limites : « %s »", // fr
        "déplacement du fuseau horaire en dehors des limites : « %s »", // fr
        "« %s » est en dehors des limites du type real", // fr
        "« %s » est en dehors des limites du type double precision", // fr
        "litéral multirange mal formé : « %s »", // fr
        "intervalle litéral mal formé : « %s »", // fr
        "enregistrement litéral invalide : « %s »", // fr
        "tableau littéral mal formé : « %s »", // fr
        "littéral multirange mal formé : « %s »", // fr
        "intervalle littéral mal formé : « %s »", // fr
        "enregistrement littéral invalide : « %s »", // fr
        "literal array salah bentuk: « %s »", // id
        "tanggal diluar jangkauan: « %s »", // id
        "nilai field date/time melebihi jangkauan : « %s »", // id
        "nilai field interval melebihi jangkauan : « %s »", // id
        "pemindahan zona waktu melebihi jangkauan : « %s »", // id
        "« %s » diluar jangkauan untuk tipe real", // id
        "« %s » diluar jangkauan untuk tipe presisi ganda", // id
        "literal rentang tidak benar: « %s »", // id
        "literal record cacad: « %s »", // id
        "timestamp di luar jangkauan: « %s »", // id
        "il letterale array non è definito in modo corretto: \"%s\"", // it
        "valore \"%s\" fuori dall'intervallo consentito per il tipo %s", // it
        "data fuori dall'intervallo consentito: \"%s\"", // it
        "valore del campo date/time fuori dall'intervallo consentito: \"%s\"", // it
        "valore del campo interval fuori dall'intervallo consentito: \"%s\"", // it
        "la differenza di fuso orario è fuori dall'intervallo consentito: \"%s\"", // it
        "\"%s\" è fuori dall'intervallo consentito per il tipo real", // it
        "\"%s\" è fuori dall'intervallo consentito per il tipo double precision", // it
        "letterale multirange non corretto: \"%s\"", // it
        "letterale di intervallo non definito correttamente: \"%s\"", // it
        "letterale record non corretto: \"%s\"", // it
        "timestamp fuori dall'intervallo consentito: \"%s\"", // it
        "配列リテラルの書式が誤っています: \"%s\"", // ja
        "値\"%s\"は型%sの範囲外です", // ja
        "日付が範囲外です: \"%s\"", // ja
        "日付時刻のフィールドが範囲外です: \"%s\"", // ja
        "intervalフィールドの値が範囲外です: \"%s\"", // ja
        "タイムゾーンの置換が範囲外です: \"%s\"", // ja
        "型realでは\"%s\"は範囲外です", // ja
        "型double precisionでは\"%s\"は範囲外です", // ja
        "不正な複範囲リテラル: \"%s\"", // ja
        "不正な範囲リテラル: \"%s\"", // ja
        "おかしなレコードリテラルです: \"%s\"", // ja
        "timestampが範囲外です: \"%s\"", // ja
        "値\"%s\"は%s型の範囲外です", // ja
        "\"%s\"はreal型の範囲外です", // ja
        "\"%s\"はdouble precision型の範囲外です", // ja
        "不正な複範囲型リテラル: \"%s\"", // ja
        "不正な形式のレコードリテラル: \"%s\"", // ja
        "თარიღი დიაპაზონს გარეთაა: \"%s\"", // ka
        "დროის/თარიღის ველის მნიშვნელობა დიაპაზონს გარეთაა: \"%s\"", // ka
        "ინტერვალის ველის მნიშვნელობა დიაპაზონს გარეთაა: \"%s\"", // ka
        "%s დიაპაზონს გარეთაა ტიპისთვის: real", // ka
        "%s დიაპაზონს გარეთაა ტიპისთვის: double precision", // ka
        "დიაპაზონის არასწორი სტრიქონი: %s", // ka
        "ჩანაწერის არასწორი სტრიქონი: %s", // ka
        "დროის შტამპი დიაპაზონს გარეთაა: \"%s\"", // ka
        "비정상적인 배열 문자: \"%s\"", // ko
        "입력한 \"%s\" 값은 %s 자료형 범위를 초과했습니다", // ko
        "날짜 범위가 벗어났음: \"%s\"", // ko
        "날짜/시간 필드의 값이 범위를 벗어남: \"%s\"", // ko
        "interval 필드의 값이 범위를 벗어남: \"%s\"", // ko
        "표준시간대 범위를 벗어남: \"%s\"", // ko
        "\"%s\"는 real 자료형의 범위를 벗어납니다.", // ko
        "\"%s\"는 double precision 자료형의 범위를 벗어납니다.", // ko
        "비정상적인 multirange 문자열: \"%s\"", // ko
        "비정상적인 range 문자: \"%s\"", // ko
        "비정상적인 레코드 문자: \"%s\"", // ko
        "타임스탬프 값이 범위를 벗어났음: \"%s\"", // ko
        "nieprawidłowy literał tablicy: \"%s\"", // pl
        "wartość \"%s\" jest poza zakresem dla typu %s", // pl
        "data poza zakresem: \"%s\"", // pl
        "wartość pola daty/czasu poza zakresem: \"%s\"", // pl
        "wartość pola interwału poza zakresem: \"%s\"", // pl
        "przesunięcie strefy czasowej poza zakresem: \"%s\"", // pl
        "\"%s\" jest poza zakresem dla typu liczb rzeczywistych", // pl
        "\"%s\" jest poza zakresem dla typu liczb podwójnej precyzji", // pl
        "nieprawidłowy literał przedziału: \"%s\"", // pl
        "nieprawidłowy literał rekordu: \"%s\"", // pl
        "znacznik czasu poza zakresem: \"%s\"", // pl
        "timestamp fora do intervalo: \"%s\"", // pt_BR
        "matriz mal formada: \"%s\"", // pt_BR
        "o valor \"%s\" está fora do intervalo para o tipo de dados %s", // pt_BR
        "date fora do intervalo: \"%s\"", // pt_BR
        "valor do campo date/time está fora do intervalo: \"%s\"", // pt_BR
        "valor do campo interval fora do intervalo: \"%s\"", // pt_BR
        "deslocamento de zona horária fora do intervalo: \"%s\"", // pt_BR
        "\"%s\" está fora do intervalo para tipo real", // pt_BR
        "\"%s\" está fora do intervalo para tipo double precision", // pt_BR
        "literal multi-intervalo malformado: \"%s\"", // pt_BR
        "range mal formado: \"%s\"", // pt_BR
        "ошибочный литерал массива: \"%s\"", // ru
        "значение \"%s\" вне диапазона для типа %s", // ru
        "дата вне диапазона: \"%s\"", // ru
        "значение поля типа date/time вне диапазона: \"%s\"", // ru
        "значение поля interval вне диапазона: \"%s\"", // ru
        "смещение часового пояса вне диапазона: \"%s\"", // ru
        "\"%s\" вне диапазона для типа real", // ru
        "\"%s\" вне диапазона для типа double precision", // ru
        "ошибочный литерал мультидиапазона: \"%s\"", // ru
        "ошибочный литерал диапазона: \"%s\"", // ru
        "ошибка в литерале записи: \"%s\"", // ru
        "timestamp вне диапазона: \"%s\"", // ru
        "felaktig array-literal: \"%s\"", // sv
        "värdet \"%s\" är utanför giltigt intervall för typen %s", // sv
        "datum utanför giltigt intervall \"%s\"", // sv
        "datum/tid-värde utanför giltigt område: \"%s\"", // sv
        "intervall-värde utanför giltigt område: \"%s\"", // sv
        "tidszonförskjutning itanför sitt intervall: \"%s\"", // sv
        "\"%s\" är utanför giltigt intervall för typen real", // sv
        "\"%s\" är utanför giltigt intervall för typen double precision", // sv
        "trasig multirange-litteral: \"%s\"", // sv
        "trasig range-litteral: \"%s\"", // sv
        "felaktig postliteral: \"%s\"", // sv
        "timestamp utanför giltigt intervall: \"%s\"", // sv
        "array literal bozuk: \"%s\"", // tr
        "date kapsam dışıdır: \"%s\"", // tr
        "date/time alan değieri kapsam dışıdır: \"%s\"", // tr
        "interval alan değeri kapsam dışında: \"%s\"", // tr
        "yer değiştirme değeri kapsam dışında: \"%s\"", // tr
        "real tipi için kapsam dışı bir değer \"%s\"", // tr
        "double precision tipi için kapsam dışı bir değer \"%s\"", // tr
        "aralık (range) literali bozuk: \"%s\"", // tr
        "hatalı değer: \"%s\"", // tr
        "timestamp kapsam dışıdır: \"%s\"", // tr
        "неправильний літерал масиву: \"%s\"", // uk
        "значення \"%s\" поза діапазоном для типу %s", // uk
        "дата поза діапазоном: \"%s\"", // uk
        "значення поля типу дата/час поза діапазоном: \"%s\"", // uk
        "значення поля типу інтервал, поза діапазоном: \"%s\"", // uk
        "зміщення часового поясу, поза діапазоном: \"%s\"", // uk
        "\"%s\" поза діапазоном для дійсного типу", // uk
        "\"%s\" поза діапазоном для типу double precision", // uk
        "неправильний багатодіапазонний літерал: \"%s\"", // uk
        "неправильний літерал діапазону: \"%s\"", // uk
        "невірно сформований літерал запису: \"%s\"", // uk
        "позначка часу поза діапазоном: \"%s\"", // uk
        "有缺陷的数组常量:\"%s\"", // zh_CN
        "值 \"%s\" 超出类型 %s 的范围", // zh_CN
        "时间戳超出范围: \"%s\"", // zh_CN
        "日期/时间值超出范围: \"%s\"", // zh_CN
        "间隔字段超出范围: \"%s\"", // zh_CN
        "时间区域置换超出范围: \"%s\"", // zh_CN
        "\"%s\" 超出实数类型的范围", // zh_CN
        "\"%s\" 超出双精度类型的范围", // zh_CN
        "有缺陷的范围字串:\"%s\"", // zh_CN
        "有缺陷的记录常量: \"%s\"", // zh_CN
    ];

    /// <summary>Every catalogue's <c>invalid input value for enum %s: "%s"</c>: the enum's name, then the rejected value.</summary>
    public static readonly IReadOnlyList<string> EnumInputMessageFormats =
    [
        "invalid input value for enum %s: \"%s\"",
        "ungültiger Eingabewert für Enum %s: »%s«", // de
        "la sintaxis de entrada no es válida para el enum %s: «%s»", // es
        "valeur en entrée invalide pour le enum %s : « %s »", // fr
        "nilai masukan tidak valid untuk enum %s : « %s »", // id
        "la sintassi per l'enumerazione %s non è valida: \"%s\"", // it
        "列挙型%sの不正な入力構文: \"%s\"", // ja
        "არასწორი შეყვანილი მნიშვნელობა ჩამონათვალისთვის %s: \"%s\"", // ka
        "%s 열거형의 입력 값이 잘못됨: \"%s\"", // ko
        "nieprawidłowa wartość wejścia dla enumeracji %s: \"%s\"", // pl
        "valor de entrada é inválido para enum %s: \"%s\"", // pt_BR
        "неверное значение для перечисления %s: \"%s\"", // ru
        "ogiltigt indata-värde för enum %s: \"%s\"", // sv
        "%s enum tipi için geçersiz değer: \"%s\"", // tr
        "неприпустиме вхідне значення для переліку %s: \"%s\"", // uk
        "对于枚举%s的输入值无效: \"%s\"", // zh_CN
    ];

    /// <summary>Every catalogue's DETAIL for a JSON parser error that quotes the token it stopped at (<c>Token "%s" is
    /// invalid.</c>, <c>Expected JSON value, but found "%s".</c> and its siblings, <c>Escape sequence "\%s" is
    /// invalid.</c>), the one slot the token. Each major version's wording is listed (the token's placeholder is
    /// <c>%s</c> in 13 through 16 and <c>%.*s</c> in 17 and 18; both write the same text).</summary>
    public static readonly IReadOnlyList<string> JsonTokenDetailFormats =
    [
        "Token \"%s\" is invalid.",
        "Expected end of input, but found \"%s\".",
        "Expected array element or \"]\", but found \"%s\".",
        "Expected \",\" or \"]\", but found \"%s\".",
        "Expected \":\", but found \"%s\".",
        "Expected JSON value, but found \"%s\".",
        "Expected string, but found \"%s\".",
        "Expected string or \"}\", but found \"%s\".",
        "Expected \",\" or \"}\", but found \"%s\".",
        "Escape sequence \"\\%s\" is invalid.",
        "Escape-Sequenz »\\%s« ist nicht gültig.", // de
        "Ende der Eingabe erwartet, aber »%s« gefunden.", // de
        "Array-Element oder »]« erwartet, aber »%s« gefunden.", // de
        "»,« oder »]« erwartet, aber »%s« gefunden.", // de
        "»:« erwartet, aber »%s« gefunden.", // de
        "JSON-Wert erwartet, aber »%s« gefunden.", // de
        "Zeichenkette oder »}« erwartet, aber »%s« gefunden.", // de
        "»,« oder »}« erwartet, aber »%s« gefunden.", // de
        "Zeichenkette erwartet, aber »%s« gefunden.", // de
        "Token »%s« ist ungültig.", // de
        "La secuencia de escape «\\%s» no es válida.", // es
        "Se esperaba el fin de la entrada, se encontró «%s».", // es
        "Se esperaba un elemento de array o «]», se encontró «%s».", // es
        "Se esperaba «,» o «]», se encontró «%s».", // es
        "Se esperaba «:», se encontró «%s».", // es
        "Se esperaba un valor JSON, se encontró «%s».", // es
        "Se esperaba una cadena o «}», se encontró «%s».", // es
        "Se esperaba «,» o «}», se encontró «%s».", // es
        "Se esperaba una cadena, se encontró «%s».", // es
        "La secuencia de escape «%s» no es válida.", // es
        "El elemento «%s» no es válido.", // es
        "La séquence d'échappement « \\%s » est invalide.", // fr
        "Attendait une fin de l'entrée, mais a trouvé « %s ».", // fr
        "Élément de tableau ou « ] » attendu, mais « %s » trouvé.", // fr
        "« , » ou « ] » attendu, mais « %s » trouvé.", // fr
        "« : » attendu, mais « %s » trouvé.", // fr
        "Valeur JSON attendue, mais « %s » trouvé.", // fr
        "Chaîne ou « } » attendu, mais « %s » trouvé.", // fr
        "« , » ou « } » attendu, mais « %s » trouvé.", // fr
        "Chaîne attendue, mais « %s » trouvé.", // fr
        "Le jeton « %s » n'est pas valide.", // fr
        "Chaîne ou « } » attendu, mais « %s » trouvé", // fr
        "urutan escape « \\%s » tidak valid.", // id
        "Diharapkan akhir masukan, tapi mendapatkan « %s ».", // id
        "Diharapkan nilai JSON, tapi mendapatkan « %s ».", // id
        "Diharapkan string, tapi didapatkan « %s ».", // id
        "Diharapkan elemen array atau « ] », namun menemukan « %s »", // id
        "« , » ou « ] » diharapkan, namun menemukan « %s »", // id
        "String « } » diharapkan, namun menemukan « %s »", // id
        "« : » diharapkan, namun menemukan « %s »", // id
        "« , » atau « } » diharapkan, namun menemukan « %s » trouvé", // id
        "Token « %s » tidak valid", // id
        "La sequenza di escape \"\\%s\" non è valida.", // it
        "Era prevista la fine dell'input, trovato \"%s\" invece.", // it
        "Era previsto un elemento di array oppure \"]\", trovato \"%s\" invece.", // it
        "Era previsto \",\" oppure \"]\", trovato \"%s\" invece.", // it
        "Era previsto \":\", trovato \"%s\" invece.", // it
        "Era previsto un valore JSON, trovato \"%s\" invece.", // it
        "Era prevista una stringa oppure \"}\", trovato \"%s\" invece.", // it
        "Era previsto \",\" oppure \"}\", trovato \"%s\" invece.", // it
        "Era prevista una stringa, trovato \"%s\" invece.", // it
        "Il token \"%s\" non è valido.", // it
        "エスケープシーケンス\"\\%s\"は不正です。", // ja
        "入力の終端を想定していましたが、\"%s\"でした。", // ja
        "配列要素または\"]\"を想定していましたが、\"%s\"でした。", // ja
        "\",\"または\"]\"を想定していましたが、\"%s\"でした。", // ja
        "\":\"を想定していましたが、\"%s\"でした。", // ja
        "JSON値を想定していましたが、\"%s\"でした。", // ja
        "文字列または\"}\"を想定していましたが、\"%s\"でした。", // ja
        "\",\"または\"}\"を想定していましたが、\"%s\"でした。", // ja
        "文字列を想定していましたが、\"%s\"でした。", // ja
        "トークン\"%s\"は不正です。", // ja
        "სპეციალური მიმდევრობა \"\\%s\" არასწორია.", // ka
        "მოველოდი შეყვანის დასასრულს, მაგრამ მივიღე \"%s\".", // ka
        "მოველოდი მასივის ელემენტს ან \"]\", მაგრამ მივიღე \"%s\".", // ka
        "მოველოდი \",\" ან \"]\", მაგრამ მივიღე \"%s\".", // ka
        "მოველოდი \":\", მაგრამ მივიღე \"%s\".", // ka
        "მოველოდი JSON მნიშვნელობას. მივიღე \"%s\".", // ka
        "მოველოდი სტრიქონს ან \"}\", მაგრამ მივიღე \"%s\".", // ka
        "მოველოდი \",\", ან \"}\", მაგრამ მივიღე \"%s\".", // ka
        "მოველოდი სტრიქონს, მაგრამ მივიღე \"%s\".", // ka
        "კოდი არასწორია: %s.", // ka
        "잘못된 이스케이프 조합: \"\\%s\"", // ko
        "입력 자료의 끝을 기대했는데, \"%s\" 값이 더 있음.", // ko
        "배열 요소나 \"]\" 가 필요한데 \"%s\"이(가) 있음", // ko
        "\",\" 또는 \"]\"가 필요한데 \"%s\"이(가) 있음", // ko
        "\":\"가 필요한데 \"%s\"이(가) 있음", // ko
        "JSON 값을 기대했는데, \"%s\" 값임", // ko
        "문자열이나 \"}\"가 필요한데 \"%s\"이(가) 있음", // ko
        "\",\" 또는 \"}\"가 필요한데 \"%s\"이(가) 있음", // ko
        "문자열 값을 기대했는데, \"%s\" 값임", // ko
        "\"]\" 가 필요한데 \"%s\"이(가) 있음", // ko
        "\"}\"가 필요한데 \"%s\"이(가) 있음", // ko
        "잘못된 토큰: \"%s\"", // ko
        "Sekwencja ucieczki \"\\%s\" nie jest poprawna.", // pl
        "Oczekiwano zakończenia na wejściu, znaleziono \"%s\".", // pl
        "Oczekiwano wartości JSON, znaleziono \"%s\".", // pl
        "Oczekiwano ciągu znaków, znaleziono \"%s\".", // pl
        "Oczekiwano elementu tablicy lub \"]\", znaleziono \"%s\".", // pl
        "Oczekiwano \",\" lub \"]\", znaleziono \"%s\".", // pl
        "Oczekiwano ciągu znaków lub \"}\", znaleziono \"%s\".", // pl
        "Oczekiwano \":\", znaleziono \"%s\".", // pl
        "Oczekiwano \",\" lub \"}\", znaleziono \"%s\".", // pl
        "Token \"%s\" jest niepoprawny.", // pl
        "A sequência de escape \"\\%s\" não é válida.", // pt_BR
        "Esperado o fim da entrada, mas encontrado \"%s\".", // pt_BR
        "Esperado elemento da matriz ou \"]\", mas encontrado \"%s\".", // pt_BR
        "Esperado \",\" ou \"]\", mas encontrado \"%s\".", // pt_BR
        "Esperado \":\", mas encontrado \"%s\".", // pt_BR
        "Esperado valor JSON, mas encontrado \"%s\".", // pt_BR
        "Esperada uma cadeia de caracteres ou \"}\", mas encontrado \"%s\".", // pt_BR
        "Esperado \",\" ou \"}\", mas encontrado \"%s\".", // pt_BR
        "Esperada uma cadeia de caracteres, mas encontrado \"%s\".", // pt_BR
        "O elemento \"%s\" não é válido.", // pt_BR
        "Неверная спецпоследовательность: \"\\%s\".", // ru
        "Ожидался конец текста, но обнаружено продолжение \"%s\".", // ru
        "Ожидался элемент массива или \"]\", но обнаружено \"%s\".", // ru
        "Ожидалась \",\" или \"]\", но обнаружено \"%s\".", // ru
        "Ожидалось \":\", но обнаружено \"%s\".", // ru
        "Ожидалось значение JSON, но обнаружено \"%s\".", // ru
        "Ожидалась строка или \"}\", но обнаружено \"%s\".", // ru
        "Ожидалась \",\" или \"}\", но обнаружено \"%s\".", // ru
        "Ожидалась строка, но обнаружено \"%s\".", // ru
        "Ошибочный элемент \"%s\".", // ru
        "Escape-sekvens \"\\%s\" är ogiltig.", // sv
        "Förväntade slut på indata, men hittade \"%s\".", // sv
        "Färväntade array-element eller \"]\", men hittade \"%s\".", // sv
        "Förväntade \",\" eller \"]\", men hittade \"%s\".", // sv
        "Förväntade sig \":\" men hittade \"%s\".", // sv
        "Förväntade JSON-värde, men hittade \"%s\".", // sv
        "Färväntade sträng eller \"}\", men hittade \"%s\".", // sv
        "Förväntade sig \",\" eller \"}\" men hittade \"%s\".", // sv
        "Förväntade sträng, men hittade \"%s\".", // sv
        "Token \"%s\" är ogiltig.", // sv
        "Неприпустима спеціальна послідовність \"\\%s\".", // uk
        "Очікувався кінець введення, але знайдено \"%s\".", // uk
        "Очікувався елемент масиву або \"]\", але знайдено \"%s\".", // uk
        "Очікувалось \",\" або \"]\", але знайдено \"%s\".", // uk
        "Очікувалось \":\", але знайдено \"%s\".", // uk
        "Очікувалось значення JSON, але знайдено \"%s\".", // uk
        "Очікувався рядок або \"}\", але знайдено \"%s\".", // uk
        "Очікувалось \",\" або \"}\", але знайдено \"%s\".", // uk
        "Очікувався рядок, але знайдено \"%s\".", // uk
        "Неприпустимий маркер \"%s\".", // uk
        "转义序列 \"\\%s\" 无效.", // zh_CN
        "期望输入结束，结果发现是\"%s\".", // zh_CN
        "期望 是JSON值, 但结果发现是\"%s\".", // zh_CN
        "期望是字符串, 但发现结果是\"%s\".", // zh_CN
        "期望为数组元素或者\"]\"，但发现结果是\"%s\".", // zh_CN
        "期望是\",\" 或 \"]\"，但发现结果是\"%s\".", // zh_CN
        "期望是字符串或\"}\"，但发现结果是\"%s\".", // zh_CN
        "期望得到 \":\"，但发现结果是\"%s\".", // zh_CN
        "期望是 \",\" 或 \"}\"，但发现结果是\"%s\".", // zh_CN
        "令牌 \"%s\" 无效.", // zh_CN
    ];

    /* ── Row values in PostgreSQL's own fixed formats: the one exception to #3944 (see the type's summary). ── */

    /// <summary>What a field becomes when PostgreSQL writes row values into fields of its kind and this build reads no
    /// form for it: its SQLSTATE names the class (<see cref="ValueDetailStates"/>, <see cref="ValueMessageStates"/>),
    /// or the text opens like one of the forms without matching it whole. Nothing of it is kept, the way a line under
    /// a field label a translated <c>lc_messages</c> writes is withheld rather than read as prose.</summary>
    public const string WithheldValues = "<withheld: this text carries row values in a shape this build does not read>";

    /// <summary>The SQLSTATEs whose DETAIL PostgreSQL writes row values into (errcodes.txt): not_null_violation,
    /// foreign_key_violation, unique_violation, check_violation and exclusion_violation, whose DETAIL is one of the
    /// <c>Key ...</c> / <c>Failing row ...</c> forms below; and invalid_text_representation, whose JSON parser writes
    /// the offending token into its DETAIL. A DETAIL under one of these that matches no form is withheld.</summary>
    public static readonly IReadOnlyList<string> ValueDetailStates = ["23502", "23503", "23505", "23514", "23P01", "22P02"];

    /// <summary>The SQLSTATE whose message PostgreSQL writes the rejected value into: invalid_text_representation
    /// (<c>invalid input syntax for type %s: "%s"</c>, the <c>malformed ... literal: "%s"</c> family, and
    /// <c>invalid input value for enum</c>). A message under it that matches no form is withheld. The messages that
    /// carry a value under other SQLSTATEs (<see cref="QuotedValueMessageFormats"/>) are read by their text alone,
    /// so a log prefix with no <c>%e</c> keeps no more of them than one with it: a message opening like one of those
    /// forms and matching none whole is withheld too.</summary>
    public static readonly IReadOnlyList<string> ValueMessageStates = ["22P02"];

    /* A placeholder in one of the value forms: %s, %d or %llu in order, or %n$s where a catalogue reorders them. */
    private static readonly Regex s_valuePlaceholder = new(
        @"%(?:(?<position>[0-9])\$)?(?<spec>llu|d|s)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /// <summary>Which of a form's slots carry values, and in what inner shape.</summary>
    private enum ValueSlot
    {
        /// <summary>Every slot is an index key, <c>(columns)=(values)</c>; the values are masked and the columns kept,
        /// and two keys must name the same columns (they are of one index).</summary>
        Key,

        /// <summary>The second slot is the key's values, the first its columns and the third the table.</summary>
        KeyPair,

        /// <summary>The one slot is the whole row, <c>(values)</c>.</summary>
        Row,

        /// <summary>The one slot is the partition key, <c>(columns) = (values)</c>.</summary>
        PartitionKey,

        /// <summary>The third slot is the input line (after the relation and the line number).</summary>
        CopyLine,

        /// <summary>The fourth slot is one column's input (after the relation, the line number and the column).</summary>
        CopyColumn,

        /// <summary>Every slot after the line number is the input around the error.</summary>
        Json,

        /// <summary>The second slot is the rejected value, the first the type's name.</summary>
        Input,

        /// <summary>The first slot is the value; a later slot, when there is one, is a name (a type's).</summary>
        Literal,

        /// <summary>No slot carries a value: the form is recognized so that it is kept, not withheld.</summary>
        None,
    }

    /// <summary>One value form, compiled: the pattern of a whole field in it, and the literal it opens with.
    ///
    /// <para><b>No slot backtracks into another.</b> Each slot before the last ends at the FIRST place the text that
    /// follows it begins (a tempered, atomic run: <c>(?&gt;(?:(?!separator).)*)</c>), and the last slot, which
    /// <c>$</c> pins to the field's end, takes the rest. A slot can therefore be read in one pass whatever the
    /// text holds, where lazy slots tried every split of every separator the text repeated. The two value slots that
    /// run to the LAST place their separator appears (<see cref="ValueSlot.KeyPair"/>, <see cref="ValueSlot.Literal"/>)
    /// are one atomic scan back from the field's end, linear for the same reason. Text that repeats a
    /// separator and so does not read as the form whole matches nothing, and is withheld (<see cref="OpensValueForm"/>,
    /// <see cref="InValueClass"/>) rather than read some other way.</para></summary>
    private sealed class ValueForm
    {
        public required TimeBoundPattern Whole { get; init; }

        /// <summary>The literal before the form's first slot, when it has a letter in it; empty otherwise (a
        /// catalogue that writes a slot first, or opens with a bare quote), so it marks nothing.</summary>
        public required string Head { get; init; }

        /// <summary>Whether the form is a message's, which a verbose log's SQLSTATE may precede.</summary>
        public required bool Message { get; init; }

        /// <summary>Two keys of one index must name the same columns; different ones mean the text is not what
        /// PostgreSQL wrote, and it is withheld.</summary>
        public required bool SameNames { get; init; }

        /// <summary>A slot that ends where <paramref name="separator"/> (a pattern) first appears.</summary>
        private static string Until(string group, string separator, string atom = ".") =>
            $"(?<{group}>(?>(?:(?!{separator}){atom})*))";

        /// <summary>The form's last slot: whatever is left of the field, up to the text that ends the form.</summary>
        private static string Rest(string group, string atom = ".") => $"(?<{group}>{atom}*)";

        /// <summary>A slot that ends where <paramref name="separator"/> (a pattern) LAST appears: one atomic run
        /// that takes everything to the end of the field and gives back only as far as the last place the separator
        /// begins (<c>(?&gt;.*(?=separator))</c>). The scan back is a single pass, O(field length x separator length),
        /// whatever the text repeats, and being atomic it never offers an earlier separator to the slots after it:
        /// a field that does not read as the form with its last separator does not read as it at all, and is withheld.
        /// The separator itself is matched by the literal that follows the slot.</summary>
        private static string Greedy(string group, string separator) =>
            $"(?<{group}>(?>.*(?={separator})))";

        /// <summary>The form's longest literal that has a letter in it, kept only for a form with no <see cref="Head"/>
        /// (the catalogues that write a slot first): text holding it is a field of this form in a wording this build
        /// does not list (another major version's, or a minor release's). Empty when the form has a head.
        ///
        /// <para><b>A fallback that can only add withholding.</b> It is tried after the forms have all failed to match
        /// whole, and what it adds is a form's text withheld, never a form's text kept: it cannot make a field that
        /// was withheld read as kept. Its coverage is the one literal, verbatim, wherever it appears. A wording of another
        /// major version that changes any character inside that literal, or a value-first message cut inside its value,
        /// does not contain it and is kept when no SQLSTATE puts the field in the class. A literal short in letters
        /// (the ko COPY form's five characters) is found in unrelated text too, and a CONTEXT line holding it is
        /// withheld with everything after it: the fallback may withhold more than the form's text, and does not
        /// cover every wording.</para></summary>
        public required string Tail { get; init; }

        /// <summary>Whether a slot of the form carries a value (false for the forms recognized only to be kept).</summary>
        public required bool Carries { get; init; }

        public static ValueForm Compile(string format, ValueSlot slot, bool message)
        {
            var placeholders = s_valuePlaceholder.Matches(format);
            var literals = new List<string>(placeholders.Count + 1);
            var from = 0;
            foreach (Match placeholder in placeholders)
            {
                literals.Add(format[from..placeholder.Index]);
                from = placeholder.Index + placeholder.Length;
            }

            literals.Add(format[from..]);

            var pattern = new StringBuilder(message ? StatePattern : "^");
            var argument = 0;
            string? head = null;
            var keys = 0;
            var valueOpened = false;
            var tail = string.Empty;
            for (var at = 0; at < placeholders.Count; at++)
            {
                var placeholder = placeholders[at];
                var literal = literals[at];
                head ??= literal;
                tail = LongerLetteredLiteral(tail, literal);
                pattern.Append(Regex.Escape(literal));

                /* What the slot ends at, in the text: the literal that follows the placeholder; the form's end when
                   no slot follows. */
                var last = at == placeholders.Count - 1;
                var next = Regex.Escape(literals[at + 1]);
                var position = placeholder.Groups["position"];
                argument = position.Success ? position.Value[0] - '0' : argument + 1;
                if (placeholder.Groups["spec"].Value != "s")
                {
                    pattern.Append("(?<name>[0-9]+)");
                }
                else
                {
                    switch (slot)
                    {
                        case ValueSlot.Key:
                            pattern.Append(@"\(").Append(Until("name", @"\)=\(")).Append(@"\)=\(")
                                .Append(last ? Rest("value") : Until("value", @"\)" + next)).Append(@"\)");
                            keys++;
                            break;
                        case ValueSlot.CopyLine when argument == 3:
                        case ValueSlot.CopyColumn when argument == 4:
                        case ValueSlot.Input when argument == 2:
                            pattern.Append(last ? Rest("value") : Until("value", next));
                            break;
                        case ValueSlot.KeyPair when argument == 2:
                            /* A value with a name slot after it (the table's) runs to the LAST place that text appears:
                               a value holding it would otherwise leave its tail in the name. */
                            pattern.Append(last ? Rest("value") : Greedy("value", next));
                            break;
                        case ValueSlot.Literal when argument == 1:
                            pattern.Append(last ? Rest("value") : Greedy("value", next));
                            break;
                        case ValueSlot.Row:
                            pattern.Append(@"\(").Append(last ? Rest("value") : Until("value", @"\)" + next)).Append(@"\)");
                            break;
                        case ValueSlot.PartitionKey:
                            pattern.Append(@"\(").Append(Until("name", @"\) = \(")).Append(@"\) = \(")
                                .Append(last ? Rest("value") : Until("value", @"\)" + next)).Append(@"\)");
                            break;
                        case ValueSlot.Json when argument >= 2:
                            /* The three slots are adjacent: one value spans them. */
                            if (!valueOpened)
                            {
                                pattern.Append(Rest("value"));
                                valueOpened = true;
                            }

                            break;
                        default:
                            /* A form with no value slot must not read a whole value-bearing line as its one name
                               (`COPY %s` over `COPY t, line 1: "..."`), so its names take no colon, quote or line
                               break; a relation named with one falls to the head rule and is withheld. */
                            var atom = slot == ValueSlot.None ? @"[^\n:""]" : ".";
                            pattern.Append(last ? Rest("name", atom) : Until("name", next, atom));
                            break;
                    }
                }
            }

            pattern.Append(Regex.Escape(literals[^1])).Append(message ? s_positionPattern : "$");
            head ??= format;
            tail = LongerLetteredLiteral(tail, literals[^1]);
            var headed = head.Any(char.IsLetter);
            return new ValueForm
            {
                Whole = new TimeBoundPattern(pattern.ToString(), RegexOptions.CultureInvariant | RegexOptions.Singleline, s_matchTimeout),
                Head = headed ? head : string.Empty,
                Message = message,
                SameNames = keys > 1,
                Tail = headed ? string.Empty : tail,
                Carries = slot != ValueSlot.None,
            };
        }

        private static string LongerLetteredLiteral(string longest, string literal) =>
            literal.Length > longest.Length && literal.Any(char.IsLetter) ? literal : longest;

        /// <summary>The text with every value slot replaced by <c>?</c>; <see cref="WithheldValues"/> when its keys
        /// name different columns; null when it is not in this form.</summary>
        public string? Apply(string text)
        {
            /* A form that opens with words is only tried on text that opens with them (after a verbose log's
               SQLSTATE, for a message): the pattern could not match otherwise, and skipping it skips the read. */
            if (Head.Length > 0 && !text.AsSpan(Message ? StatePrefixLength(text) : 0).StartsWith(Head, StringComparison.Ordinal))
            {
                return null;
            }

            var match = Whole.Match(text);
            if (!match.Success)
            {
                return null;
            }

            if (SameNames)
            {
                var names = match.Groups["name"].Captures;
                for (var k = 1; k < names.Count; k++)
                {
                    if (!string.Equals(names[k].Value, names[0].Value, StringComparison.Ordinal))
                    {
                        return WithheldValues;
                    }
                }
            }

            var values = match.Groups["value"].Captures;
            if (values.Count == 0)
            {
                return text;
            }

            var result = new StringBuilder(text.Length);
            var at = 0;
            foreach (Capture value in values)
            {
                result.Append(text, at, value.Index - at).Append('?');
                at = value.Index + value.Length;
            }

            return result.Append(text, at, text.Length - at).ToString();
        }
    }

    /// <summary>The text in the first form that matches it whole, its values masked (or withheld); null when none
    /// does.</summary>
    private static string? MaskValues(string text, ValueForm[] forms) => MaskValues(text, forms, out _);

    /// <param name="carriesValue">Whether the form that matched has a slot that carries a value.</param>
    private static string? MaskValues(string text, ValueForm[] forms, out bool carriesValue)
    {
        foreach (var form in forms)
        {
            if (form.Apply(text) is { } masked)
            {
                carriesValue = form.Carries;
                return masked;
            }
        }

        carriesValue = false;
        return null;
    }

    /// <summary>Whether the text opens with one of the forms' heads (after a verbose log's SQLSTATE, for a message)
    /// without matching any of them whole: a form this build reads, in a shape it does not. A form with no head (its
    /// catalogue writes a slot first) is recognized by its longest lettered literal anywhere in the text instead, so
    /// another major version's wording of it is withheld, not kept.</summary>
    private static bool OpensValueForm(string text, ValueForm[] forms, bool message)
    {
        var span = text.AsSpan(message ? StatePrefixLength(text) : 0);
        foreach (var form in forms)
        {
            if (form.Head.Length > 0
                ? span.StartsWith(form.Head, StringComparison.Ordinal)
                : form.Tail.Length > 0 && span.Contains(form.Tail, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static bool InValueClass(string? sqlState, IReadOnlyList<string> states) =>
        sqlState is not null && states.Contains(sqlState);

    /* A scanner's error, in every language PostgreSQL 18 ships it in (#4006). The scanner writes "<what> at or near
       "<text>"" (scanner_yyerror, plpgsql_yyerror), jsonpath's grammar "... of jsonpath input", and past the last
       token "<what> at end of input"; the server log then appends " at character N" (elog.c). Every one of these
       is translated under lc_messages, while the ERROR label stays English in es, id and ja, and in every catalogue
       that translates PL/pgSQL alone (cs, el, ro, vi, zh_TW; the label is the backend's), so a translated form
       reaches the stored message exactly as the English one does. English was the only form read before #4006,
       and a Japanese error kept its token verbatim. These are the msgstrs of every catalogue in the postgres and
       plpgsql domains, verbatim; PgLogCatalogueShapeTests reads the bundled runtime's .mo files and fails on one
       missing here, so a PostgreSQL bump cannot reopen #4006 quietly. %1$s is the head (the error's name, from a
       fixed set of messages), %2$s the token, and a catalogue may put the token first. */

    /// <summary>Every catalogue's <c>%s at or near "%s"</c> (#4006): what the token of a syntax error is written
    /// inside, in the postgres and plpgsql domains.</summary>
    public static readonly IReadOnlyList<string> LexerErrorFormats =
    [
        "%s at or near \"%s\"",
        "%s bei »%s«", // de
        "%s en o cerca de «%s»", // es
        "%s sur ou près de « %s »", // fr
        "'%s' pada atau didekat « %s »", // id
        "%s a o presso \"%s\"", // it
        "\"%2$s\"またはその近辺で%1$s", // ja
        "\"%2$s\" もしくはその近辺で %1$s", // ja, plpgsql
        "%s \"%s\"-სთან ან ახლოს", // ka
        "%s, \"%s\" 부근", // ko
        "%s w lub blisko \"%s\"", // pl
        "%s w lub pobliżu \"%s\"", // pl, plpgsql
        "%s em ou próximo a \"%s\"", // pt_BR
        "%s (примерное положение: \"%s\")", // ru
        "%s vid eller nära \"%s\"", // sv
        "\"%2$s\"  yerinde %1$s", // tr
        "%s в або поблизу \"%s\"", // uk
        "%s 在 \"%s\" 或附近的", // zh_CN
        "%s na nebo blízko \"%s\"", // cs, plpgsql only
        "%s σε ή κοντά σε «%s»", // el, plpgsql only
        "%s la sau aproape de \"%s\"", // ro, plpgsql only
        "%s tại hoặc gần\"%s\"", // vi, plpgsql only
        "\"%2$s\" 附近發生 %1$s", // zh_TW, plpgsql only
    ];

    /// <summary>Every catalogue's <c>%s at or near "%s" of jsonpath input</c> (#4006). Its token is jsonpath, not
    /// SQL: the lexer would read a jsonpath string (<c>"4111-1111"</c>) as a quoted identifier and keep it, so the
    /// token is withheld rather than read.</summary>
    public static readonly IReadOnlyList<string> JsonpathErrorFormats =
    [
        "%s at or near \"%s\" of jsonpath input",
        "%s bei »%s« in jsonpath-Eingabe", // de
        "%s en o cerca de «%s» de la entrada jsonpath", // es
        "%s sur ou près de « %s » de l'entrée jsonpath", // fr
        "%s in corrispondenza o vicino a \"%s\" dell'input jsonpath", // it
        "jsonpath 入力の\"%2$s\"または近くに %1$s があります", // ja
        "%s ზუსტად ან ახლოს jsonpath შეყვანის \"%s\"-სთან", // ka
        "%s, jsonpath 입력 \"%s\" 부근", // ko
        "%s em ou perto de \"%s\" da entrada jsonpath", // pt_BR
        "%s в строке jsonpath (примерное положение: \"%s\")", // ru
        "%s vid eller nära \"%s\" i jsonpath-indata", // sv
        "jsonpath girdisinin \"%2$s\" kısmında veya yakınında %1$s", // tr
        "%s в або біля \"%s\" введення jsonpath", // uk
        "%s位于或靠近jsonpath输入的 \"%s\"", // zh_CN
    ];

    /// <summary>Every catalogue's <c>%s at end of input</c> and <c>%s at end of jsonpath input</c> (#4006): a syntax
    /// error past the last token, which quotes nothing and is kept as written.</summary>
    public static readonly IReadOnlyList<string> EndOfInputFormats =
    [
        "%s at end of input",
        "%s at end of jsonpath input",
        "%s am Ende der Eingabe", // de
        "%s am Ende der jsonpath-Eingabe", // de
        "%s al final de la entrada", // es
        "%s al final de la entrada jsonpath", // es
        "%s à la fin de l'entrée", // fr
        "%s à la fin de l'entrée jsonpath", // fr
        "'%s' diakhir masukan", // id
        "%s alla fine dell'input", // it
        "%s alla fine dell'input di jsonpath", // it
        "入力の最後で %s", // ja
        "jsonpath の最後に %s があります", // ja
        "%s შეყვანის ბოლოს", // ka
        "%s jsonpath-ის შეყვანის ბოლოში", // ka
        "%s, 입력 끝부분", // ko
        "%s, jsonpath 입력 끝부분", // ko
        "%s na końcu danych wejściowych", // pl
        "%s no fim da entrada", // pt_BR
        "%s no final da entrada jsonpath", // pt_BR
        "%s в конце", // ru
        "%s в конце аргумента jsonpath", // ru
        "%s vid slutet av indatan", // sv
        "%s vid slutet av jsonpath-indata", // sv
        "giriş sonuna %s", // tr
        "jsonpath girdisi sonunda %s", // tr
        "%s в кінці введення", // uk
        "%s в кінці введення jsonpath", // uk
        "%s 在输入的末尾", // zh_CN
        "%s位于jsonpath输入的末尾", // zh_CN
        "\"%s\" na konci vstupu", // cs, plpgsql only
        "«%s» στο τέλος εισόδου", // el, plpgsql only
        "%s la sfârşit de intrare", // ro, plpgsql only
        "%s tại nơi kết thúc đầu vào", // vi, plpgsql only
        "輸入結尾發生 %s", // zh_TW, plpgsql only
    ];

    /// <summary>Every catalogue's <c> at character %d</c> (#4006), which the server log appends to a message that
    /// carries a cursor position. Any catalogue's is accepted after any form: a catalogue that translates PL/pgSQL
    /// alone writes PL/pgSQL's message translated and this position in English.</summary>
    public static readonly IReadOnlyList<string> PositionFormats =
    [
        " at character %d",
        " bei Zeichen %d", // de
        " en carácter %d", // es
        " au caractère %d", // fr
        " pada karakter %d", // id
        " al carattere %d", // it
        "(%d文字目)", // ja
        " სიმბოლოსთან %d", // ka
        " %d 번째 문자 부근", // ko
        " przy znaku %d", // pl
        " no caractere %d", // pt_BR
        " (символ %d)", // ru
        " vid tecken %d", // sv
        " %d karakterinde ", // tr
        " символ %d", // uk
        " 第 %d 个字符处", // zh_CN
    ];

    /// <summary>The scanner's six <c>unterminated ...</c> heads in every catalogue (#4006). An error under one quotes
    /// the input from the literal's, identifier's or comment's opening to the end of the input, so by its name it
    /// cannot be read to its end, and its token is withheld without being read.</summary>
    public static readonly IReadOnlyList<string> UnterminatedMessages =
    [
        "unterminated /* comment",
        "unterminated bit string literal",
        "unterminated dollar-quoted string",
        "unterminated hexadecimal string literal",
        "unterminated quoted identifier",
        "unterminated quoted string",
        "/*-Kommentar nicht abgeschlossen", // de
        "Bitkettenkonstante nicht abgeschlossen",
        "Dollar-Quotes nicht abgeschlossen",
        "hexadezimale Zeichenkette nicht abgeschlossen",
        "Bezeichner in Anführungszeichen nicht abgeschlossen",
        "Zeichenkette in Anführungszeichen nicht abgeschlossen",
        "un comentario /* está inconcluso", // es
        "una cadena de bits está inconclusa",
        "una cadena separada por $ está inconclusa",
        "una cadena hexadecimal está inconclusa",
        "un identificador entre comillas está inconcluso",
        "una cadena de caracteres entre comillas está inconclusa",
        "commentaire /* non terminé", // fr
        "chaîne bit litérale non terminée",
        "chaîne entre guillemets dollars non terminée",
        "chaîne hexadécimale litérale non terminée",
        "identifiant entre guillemets non terminé",
        "chaîne entre guillemets non terminée",
        "komentar /* tidak berakhiran", // id
        "'bit string literal' tidak berakhiran",
        "string dengan batas dollar ($) yang tidak berakhir",
        "'hexadecimal string literal' tidak berakhiran",
        "penunjuk (identifier) tidak memiliki akhir batas",
        "membatalkan proses terminasi string kutipan",
        "commento /* non terminato", // it
        "letterale di stringa di bit non terminato",
        "stringa delimitata da dollari non terminata",
        "letterale di stringa esadecimale non terminato",
        "identificativo tra virgolette non terminato",
        "stringa tra virgolette non terminata",
        "/*コメントが閉じていません", // ja
        "ビット列リテラルの終端がありません",
        "文字列のドル引用符が閉じていません",
        "16進数文字列リテラルの終端がありません",
        "識別子の引用符が閉じていません",
        "文字列の引用符が閉じていません",
        "დაუსრულებელი /* კომენტარი", // ka
        "გაწყვეტილი ბიტური სტრიქონი",
        "$-ით დაწყებული სტრიქონ დაუმთავრებელია",
        "გაწყვეტილი თექვსმეტობითი სტრიქონი",
        "დაუსრულებელი იდენტიფიკატორი ბრჭყალებში",
        "ბრჭყალებში ჩასმული ციტატის დაუსრულებელი სტრიქონი",
        "마무리 안된 /* 주석", // ko
        "마무리 안된 비트 문자열 문자",
        "마무리 안된 달러-따옴표 안의 문자열",
        "마무리 안된 16진수 문자열 문자",
        "마무리 안된 따옴표 안의 식별자",
        "마무리 안된 따옴표 안의 문자열",
        "nie zakończony komentarz /*", // pl
        "niezakończona stała łańcucha bitów",
        "niezakończona stała łańcuchowa cytowana znakiem dolara",
        "niezakończona stała łańcucha szesnastkowego",
        "niezakończony identyfikator cytowany",
        "niezakończona stała łańcuchowa",
        "comentário /* não foi terminado", // pt_BR
        "cadeia de bits não foi terminada",
        "cadeia de caracteres entre dólares não foi terminada",
        "cadeia de caracteres hexadecimal não foi terminada",
        "identificador entre aspas não foi terminado",
        "cadeia de caracteres entre aspas não foi terminada",
        "незавершённый комментарий /*", // ru
        "оборванная битовая строка",
        "незавершённая строка с $",
        "оборванная шестнадцатеричная строка",
        "незавершённый идентификатор в кавычках",
        "незавершённая строка в кавычках",
        "ej avslutad /*-kommentar", // sv
        "ej avslutad bitsträngslitteral",
        "icke terminerad dollarciterad sträng",
        "ej avslutad hexadecimal stränglitteral",
        "icke terminerad citerad identifierare",
        "icketerminerad citerad sträng",
        "/* açıklama sonlandırılmamış", // tr
        "sonuçlandırılmamış bit string literal",
        "sonlandırılmamış dolar işaretiyle sınırlandırılmış satır",
        "sonuçlandırılmamış hexadecimal string literal",
        "sonlandırılmamış tırnakla sınırlandırılmış tanımlayıcı",
        "sonuçlandırılmamış tırnakla sınırlandırılmış satır",
        "незавершений коментар /*", // uk
        "незавершений бітовий рядок",
        "незавершений рядок з $",
        "незавершений шістнадцятковий рядок",
        "незавершений ідентифікатор в лапках",
        "незавершений рядок в лапках",
        "/* 注释没有结束", // zh_CN
        "未结束的bit字符串常量",
        "未结束的用$符号引用的字符串",
        "未结束的16进制字符串常量",
        "未结束的引用标识符",
        "未结束的引用字符串",
    ];

    /* A placeholder in a catalogue format: %s in order, or %1$s / %2$s where a catalogue reorders them. */
    private static readonly Regex s_formatArgument = new(
        @"%(?:(?<position>[12])\$)?s",
        RegexOptions.Compiled | RegexOptions.CultureInvariant,
        s_matchTimeout);

    /* The head is one of the scanner's fixed messages, so it holds no newline and no quote a token is written
       inside. Holding one is what makes a head "not clean" (#3996's round-2 review): `improper use of "*"` puts a
       quote before the marker, and a message of one catalogue could otherwise pass for another's with its token
       inside the other's head. Such a message matches no form whole and is withheld from its marker on. */
    private const string HeadPattern = @"(?<head>[^\n""«»]*?)";

    /* log_error_verbosity = verbose writes the SQLSTATE and a colon ahead of the message. */
    private const string StatePattern = @"^(?:[0-9A-Z]{5}: )?";

    private static readonly HashSet<string> s_unterminated = new(UnterminatedMessages, StringComparer.Ordinal);

    private static readonly string s_positionPattern =
        "(?<tail>(?:"
        + string.Join('|', PositionFormats.Select(f => Regex.Escape(f).Replace("%d", "[0-9]+", StringComparison.Ordinal)))
        + ")?)$";

    private static readonly MessageShape[] s_tokenShapes =
    [
        .. JsonpathErrorFormats.Select(f => MessageShape.Compile(f, jsonpath: true)),
        .. LexerErrorFormats.Select(f => MessageShape.Compile(f, jsonpath: false)),
    ];

    private static readonly MessageShape[] s_endShapes =
        [.. EndOfInputFormats.Select(f => MessageShape.Compile(f, jsonpath: false))];

    /* Compiled after s_positionPattern, which the message forms end in: static fields initialize in order. */
    private static readonly ValueForm[] s_detailForms =
    [
        .. JsonTokenDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.Literal, message: false)),
        .. KeyExistsDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.Key, message: false)),
        .. KeyConflictDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.Key, message: false)),
        .. KeyReferenceDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.KeyPair, message: false)),
        .. KeyReferenceUnreadableDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.None, message: false)),
        .. FailingRowDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.Row, message: false)),
        .. PartitionKeyDetailFormats.Select(f => ValueForm.Compile(f, ValueSlot.PartitionKey, message: false)),
    ];

    private static readonly ValueForm[] s_contextForms =
    [
        .. CopyLineContextFormats.Select(f => ValueForm.Compile(f, ValueSlot.CopyLine, message: false)),
        .. CopyColumnContextFormats.Select(f => ValueForm.Compile(f, ValueSlot.CopyColumn, message: false)),
        .. CopyBareContextFormats.Select(f => ValueForm.Compile(f, ValueSlot.None, message: false)),
        .. JsonContextFormats.Select(f => ValueForm.Compile(f, ValueSlot.Json, message: false)),
    ];

    private static readonly ValueForm[] s_messageForms =
    [
        .. QuotedValueMessageFormats.Select(f => ValueForm.Compile(f, ValueSlot.Literal, message: true)),
        .. EnumInputMessageFormats.Select(f => ValueForm.Compile(f, ValueSlot.Input, message: true)),
        .. InputSyntaxMessageFormats.Select(f => ValueForm.Compile(f, ValueSlot.Input, message: true)),
        .. InputSyntaxBareMessageFormats.Select(f => ValueForm.Compile(f, ValueSlot.None, message: true)),
    ];

    /// <summary>One catalogue form, compiled (#4006): the pattern of a whole message in it, and the literal text on
    /// either side of its token, which is all a message cut short, or ending in a way no form knows, is recognized
    /// by.</summary>
    private sealed class MessageShape
    {
        public required TimeBoundPattern Whole { get; init; }

        /// <summary>The form's longest literal: a message without it cannot be in this form.</summary>
        public required string Anchor { get; init; }

        public required string Opening { get; init; }

        public required string Closing { get; init; }

        public required bool TokenFirst { get; init; }

        public required bool Jsonpath { get; init; }

        /// <summary>The text before the token has words in it (<c> at or near "</c>, <c> en o cerca de «</c>),
        /// not just a quote, so it marks where a token starts wherever it appears.</summary>
        public required bool OpeningNamed { get; init; }

        /// <summary>The same for the text after the token (<c>"またはその近辺で</c>, <c>" 부근</c>).</summary>
        public required bool ClosingNamed { get; init; }

        /// <summary>What closes a withheld token: the closing text up to its first word.</summary>
        public required string CloseQuote { get; init; }

        public static MessageShape Compile(string format, bool jsonpath)
        {
            var pattern = new StringBuilder(StatePattern);
            var literals = new List<string>();
            var tokenAfter = -1;
            var argument = 0;
            var from = 0;
            foreach (Match placeholder in s_formatArgument.Matches(format))
            {
                var literal = format[from..placeholder.Index];
                literals.Add(literal);
                pattern.Append(Regex.Escape(literal));
                var position = placeholder.Groups["position"];
                argument = position.Success ? position.Value[0] - '0' : argument + 1;
                if (argument == 2)
                {
                    tokenAfter = literals.Count - 1;
                    pattern.Append("(?<lexeme>.*)");
                }
                else
                {
                    pattern.Append(HeadPattern);
                }

                from = placeholder.Index + placeholder.Length;
            }

            literals.Add(format[from..]);
            pattern.Append(Regex.Escape(format[from..])).Append(s_positionPattern);
            var opening = tokenAfter < 0 ? string.Empty : literals[tokenAfter];
            var closing = tokenAfter < 0 ? string.Empty : literals[tokenAfter + 1];
            return new MessageShape
            {
                Whole = new TimeBoundPattern(pattern.ToString(), RegexOptions.CultureInvariant | RegexOptions.Singleline, s_matchTimeout),
                Anchor = literals.MaxBy(l => l.Length)!,
                Opening = opening,
                Closing = closing,
                TokenFirst = tokenAfter == 0,
                Jsonpath = jsonpath,
                OpeningNamed = opening.Any(char.IsLetter),
                ClosingNamed = closing.Any(char.IsLetter),
                CloseQuote = new string([.. closing.TakeWhile(c => !char.IsLetter(c))]).TrimEnd(),
            };
        }
    }

    /// <summary>
    /// A message as it is kept (#3944, #3996's reviews, #4006): as PostgreSQL wrote it (<see cref="WithholdPlan"/>),
    /// except the token a syntax error quotes, in any catalogue's words (<see cref="LexerErrorFormats"/>), which is
    /// SQL: the scanner writes the statement from the error token on, one token for most errors (a string literal
    /// among them, value and all), and everything to the end of the input for an unterminated one
    /// (<c>unterminated dollar-quoted string</c> carries a whole function body). It is read by
    /// <see cref="RedactStoredStatement"/>, or withheld: under an <see cref="UnterminatedMessages"/> head, which by its
    /// name cannot be read to its end; in a jsonpath error (<see cref="JsonpathErrorFormats"/>), whose token is not
    /// SQL; and when it does not read to its end.
    ///
    /// <para><b>A message that names a form but does not match it whole</b> is withheld from where its token may
    /// start: a head that is not clean (<c>improper use of "*" at or near "..."</c>), a message cut before its
    /// closing quote, or one ending in a way no catalogue writes. Where a form's opening has words in it
    /// (<c> at or near "</c>), everything after the first one is withheld; where only its closing does (Japanese,
    /// Turkish and Traditional Chinese write the token FIRST, <c>"'4111...'"またはその近辺で構文エラー</c>), the text
    /// between its opening quote and its last closing is; and a token-first message that opens with its quote and is
    /// not whole (<paramref name="cut"/>, or spanning lines, which is how an entry cut between its lines reads) is
    /// withheld from that quote on, since its closing may be what the cut took. Every form that applies is applied,
    /// from the earliest start. Null in, null out; idempotent.</para>
    /// </summary>
    /// <param name="message">The message, and its continuation lines.</param>
    /// <param name="cut">True when the caller knows the message's end may be missing (a stored sample the length cap
    /// cut on its first line).</param>
    public static string? RedactMessage(string? message, bool cut = false, string? sqlState = null)
    {
        try
        {
            return RedactMessageCore(message, cut, sqlState);
        }
        catch (RegexMatchTimeoutException)
        {
            return WithheldValues;
        }
    }

    private static string? RedactMessageCore(string? message, bool cut, string? sqlState)
    {
        var kept = WithholdPlan(message);
        if (string.IsNullOrEmpty(kept))
        {
            return kept;
        }

        /* A verbose log (log_error_verbosity) writes the SQLSTATE ahead of the message, which tells the class when the
           log's prefix gave none. */
        if (sqlState is null && StatePrefixLength(kept) > 0)
        {
            sqlState = kept[..5];
        }

        /* The rejected value an input function or a range check writes into its message, masked in its slot (the one
           exception to #3944). A message whose SQLSTATE is invalid_text_representation, or that opens like one of the
           forms, and matches none whole is withheld. */
        if (kept.Length <= MaxSqlFieldLength && MaskValues(kept, s_messageForms) is { } maskedValues)
        {
            return maskedValues;
        }

        if (InValueClass(sqlState, ValueMessageStates) || OpensValueForm(kept, s_messageForms, message: true))
        {
            return WithheldValues;
        }

        /* A form matched whole: the token lexed, or withheld. The earliest token wins, so no token is left standing
           inside the head of a form that matched further along. */
        Match? whole = null;
        MessageShape? wholeShape = null;
        foreach (var shape in s_tokenShapes)
        {
            if (kept.Contains(shape.Anchor, StringComparison.Ordinal)
                && shape.Whole.Match(kept) is { Success: true } match
                && (whole is null || match.Groups["lexeme"].Index < whole.Groups["lexeme"].Index))
            {
                (whole, wholeShape) = (match, shape);
            }
        }

        if (whole is not null)
        {
            var lexeme = whole.Groups["lexeme"];
            var head = whole.Groups["head"].Value;
            var masked = wholeShape!.Jsonpath
                || head.StartsWith("unterminated ", StringComparison.Ordinal)
                || s_unterminated.Contains(head)
                || lexeme.Length > MaxSqlFieldLength
                    ? WithheldStatement
                    : RedactStoredStatement(lexeme.Value) ?? WithheldStatement;
            return kept[..lexeme.Index] + masked + kept[(lexeme.Index + lexeme.Length)..];
        }

        /* An error past the last token names no token, and is kept. */
        foreach (var shape in s_endShapes)
        {
            if (kept.Contains(shape.Anchor, StringComparison.Ordinal) && shape.Whole.IsMatch(kept))
            {
                return kept;
            }
        }

        var start = -1;
        var end = -1;
        var closeQuote = string.Empty;
        foreach (var shape in s_tokenShapes)
        {
            var (from, to) = UnreadSpan(kept, shape, cut);
            if (from < 0)
            {
                continue;
            }

            start = start < 0 ? from : Math.Min(start, from);
            if (to > end)
            {
                (end, closeQuote) = (to, shape.CloseQuote);
            }
        }

        return start < 0 ? kept : kept[..start] + WithheldStatement + (end == kept.Length ? closeQuote : kept[end..]);
    }

    /// <summary>Where the token of a message that names <paramref name="shape"/> without matching it whole may lie
    /// (<see cref="RedactMessage"/>): from, and to (the message's end when unknown); (-1, -1) when it names no
    /// token of this form.</summary>
    private static (int From, int To) UnreadSpan(string kept, MessageShape shape, bool cut)
    {
        if (shape.OpeningNamed)
        {
            var at = kept.IndexOf(shape.Opening, StringComparison.Ordinal);
            return at < 0 ? (-1, -1) : (at + shape.Opening.Length, kept.Length);
        }

        /* A token-first form opens the message with its quote (after a verbose SQLSTATE). */
        var state = StatePrefixLength(kept);
        var opened = shape.TokenFirst && kept.AsSpan(state).StartsWith(shape.Opening, StringComparison.Ordinal)
            ? state + shape.Opening.Length
            : -1;
        var close = shape.ClosingNamed ? kept.LastIndexOf(shape.Closing, StringComparison.Ordinal) : -1;
        if (close >= 0)
        {
            /* Between the token's opening and its last closing; from the message's start when the opening is not
               found before that closing. */
            var from = opened;
            if (!shape.TokenFirst)
            {
                var opening = kept.IndexOf(shape.Opening, 0, close, StringComparison.Ordinal);
                from = opening < 0 ? -1 : opening + shape.Opening.Length;
            }

            return (from >= 0 && from <= close ? from : 0, close);
        }

        return opened >= 0 && (cut || kept.Contains('\n')) ? (opened, kept.Length) : (-1, -1);
    }

    /// <summary>The length of a verbose log's <c>SQLSTATE: </c> ahead of the message, or 0.</summary>
    private static int StatePrefixLength(string message)
    {
        if (message.Length < 7 || message[5] != ':' || message[6] != ' ')
        {
            return 0;
        }

        for (var i = 0; i < 5; i++)
        {
            if (!char.IsAsciiDigit(message[i]) && !char.IsAsciiLetterUpper(message[i]))
            {
                return 0;
            }
        }

        return 7;
    }

    /// <summary>The form a monitored target's <c>STATEMENT:</c> companion is fingerprinted in, and never stored in:
    /// quoted literals AND bare numbers out, identifier-glued digits kept. Unchanged since #3601; its output is hashed
    /// under the store's key (<see cref="PgLogHashKey.Fingerprint"/>, #4004) and never stored. Null in, null out.</summary>
    public static string? RedactStatement(string? statement)
    {
        if (string.IsNullOrEmpty(statement))
        {
            return statement;
        }

        /* A statement the patterns cannot finish reading in time is withheld whole: it is never kept as written, and
           the timeout never reaches the collector. */
        try
        {
            var scrubbed = PgPlanLogParser.s_quotedLiteral.Replace(statement, "'?'");
            return PgPlanLogParser.s_bareNumber.Replace(scrubbed, "?");
        }
        catch (RegexMatchTimeoutException)
        {
            return WithheldValues;
        }
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
    /// <para>The log-event pipeline keeps only a keyed hash of a statement (<see cref="PgLogHashKey.Fingerprint"/>), so it hashes
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

    /* The two identities a log event is stored with, raw_line_hash and statement_fingerprint, lived here as unkeyed
       SHA-256 until #4004: an offline guessing oracle for the literals they hid (#3996's review recovered a PIN from
       one in 6 ms). They are HMAC-SHA-256 under the store's own key now, in PgLogHashKey, and there is deliberately
       no unkeyed spelling left to call. */
}
