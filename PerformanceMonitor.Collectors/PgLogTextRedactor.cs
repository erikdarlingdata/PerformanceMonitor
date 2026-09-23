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
/// <item><description><see cref="RedactMessage"/> — a message as written, except auto_explain's plan report, whose
/// plan carries the statement's text in a form the lexer cannot read (<see cref="WithholdPlan"/>, withheld below
/// its duration line), and the SQL a syntax error quotes after <c>at or near</c>, in any catalogue's words
/// (<see cref="ScannerErrorFormats"/>, #4006), put through the lexer.</description></item>
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
        @"^(?:Failed process was running: |prepare: |(?<parameters>[Pp]arameters: ))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* What a list of values reads as once lexed (#3996's review): `name = '?'` or `name = NULL` pairs, comma
       separated, each name a plain identifier or a `$n`. PL/pgSQL writes a variable's name UNQUOTED
       (format_expr_params), so a name that needed quoting in the function (`"o'k"`, `"a""b"`) opens a literal or an
       identifier the lexer then reads out of step, and a value comes back as a word. Such a list is not this shape,
       and is withheld. */
    private static readonly Regex s_valueList = new(
        @"^(?:[A-Za-z_\u0080-\uFFFF][A-Za-z0-9_$\u0080-\uFFFF]*|\$[0-9]+) = (?:'\?'|NULL)(?:, (?:[A-Za-z_\u0080-\uFFFF][A-Za-z0-9_$\u0080-\uFFFF]*|\$[0-9]+) = (?:'\?'|NULL))*$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* A portal's one bound value (`portal "p" parameter $1 = '...'`), or only its number when the value was not
       logged. */
    private static readonly Regex s_oneValue = new(
        @"^\$[0-9]+(?: = (?:'\?'|NULL))?$",
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
       "query did not return data" error. Prose masking had covered their quoted values. A portal's name is written
       unescaped, so it is matched lazily to the first `" with parameters: ` or `" parameter $` (#3996's review): a
       name holding a double quote had failed the match, and the frame's values were kept as prose. What a portal
       frame's values lex to is checked against their shape as well, so a name holding that text only withholds. */
    private static readonly Regex s_contextSqlLine = new(
        @"^\t?(?:remote SQL command: |query: |(?:unnamed portal|portal "".*?"") (?:(?<list>with parameters: )|(?<one>parameter )(?=\$)))",
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
    public static string? RedactDetail(string? detail, bool complete = false)
    {
        if (string.IsNullOrEmpty(detail))
        {
            return detail;
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
    /// to read at all.</summary>
    private static string MaskQuery(string query) =>
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
    public static readonly IReadOnlyList<string> ScannerErrorFormats =
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
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

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
        .. ScannerErrorFormats.Select(f => MessageShape.Compile(f, jsonpath: false)),
    ];

    private static readonly MessageShape[] s_endShapes =
        [.. EndOfInputFormats.Select(f => MessageShape.Compile(f, jsonpath: false))];

    /// <summary>One catalogue form, compiled (#4006): the pattern of a whole message in it, and the literal text on
    /// either side of its token, which is all a message cut short, or ending in a way no form knows, is recognized
    /// by.</summary>
    private sealed class MessageShape
    {
        public required Regex Whole { get; init; }

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
                Whole = new Regex(pattern.ToString(), RegexOptions.CultureInvariant | RegexOptions.Singleline),
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
    /// except the token a syntax error quotes, in any catalogue's words (<see cref="ScannerErrorFormats"/>), which is
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
    public static string? RedactMessage(string? message, bool cut = false)
    {
        var kept = WithholdPlan(message);
        if (string.IsNullOrEmpty(kept))
        {
            return kept;
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
    /// width. Two executions of one statement with different literals fingerprint alike. Only what
    /// <see cref="RedactStatement"/> masks stays out of the hash: a dollar-quoted body (a <c>DO</c> block, a
    /// <c>$$...$$</c> literal) is hashed as written, so for such a statement the fingerprint is an offline guessing
    /// oracle like <see cref="RawLineHash"/> (#3996's review, #4004).
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
    /// events, and the hash has to keep them apart.
    ///
    /// <para><b>It is not a secret-safe digest</b> (#3996's review). An unkeyed hash of text a reader can mostly
    /// reconstruct (the stored prose, the normalized statement, the prefix) is an offline guessing oracle for the
    /// part the reader cannot: a four-digit PIN in a failed UPDATE came back from the stored hash in 6 ms. So no
    /// read surface returns it; it stays a column the reads dedupe on inside the store, and a keyed replacement is
    /// #4004.</para>
    /// </summary>
    public static string RawLineHash(string rawText)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(rawText ?? string.Empty));
        return Convert.ToHexString(bytes, 0, 16);
    }
}
