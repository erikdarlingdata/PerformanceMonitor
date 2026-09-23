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
/// The redaction every text column of <see cref="PgLogEvent"/> passes through before it is stored (#3601)
/// — the SAME discipline <see cref="PgPlanLogParser"/> applies to plan capture, applied to log text. The
/// issue's scope note is binding: the log pipeline must not become the place parameter values leak into
/// the store.
///
/// <para><b>Shared by INSTANCE, not by imitation.</b> <see cref="PgPlanLogParser.s_quotedLiteral"/> and
/// <see cref="PgPlanLogParser.s_bareNumber"/> are the plan parser's own compiled patterns, made internal
/// so this type applies them rather than a second spelling of them. Two spellings of the literal pattern
/// would eventually disagree, and that disagreement would be a value in the store.</para>
///
/// <para><b>Two strengths, because the plan parser's asymmetry applies here with a twist.</b> The plan
/// parser strips quoted literals from EVERY string and bare numbers only from condition fields, because a
/// blanket numeric strip rewrites a relation named <c>transactionitems1</c>. Log text has the same two
/// populations under different names:</para>
///
/// <list type="bullet">
/// <item><description><see cref="RedactStatement"/> — the <c>STATEMENT:</c> companion, which is the user's
/// SQL with its literals. Quoted literals AND bare numbers go, because every bare number in a statement is
/// a value (<c>WHERE id = 42</c>) or part of one; the pattern's own identifier guard is what keeps
/// <c>transactionitems1</c> whole, exactly as it does inside a plan's <c>Filter</c>. The redacted text is
/// what <see cref="Fingerprint"/> hashes, so one statement shape recurs to one fingerprint whatever it ran
/// with — the plan hash's reasoning, over SQL text. <see cref="RedactStoredStatement"/> is the stronger form
/// for a surface that keeps the text instead of a hash: a single-pass lexer that masks every literal
/// spelling and refuses a statement it cannot read to its end.</description></item>
/// <item><description><see cref="RedactMessage"/> — <c>message</c>, <c>detail</c> and <c>hint</c>, which
/// are PostgreSQL's prose. Quoted literals go. Bare numbers STAY, because in prose they are the
/// evidence rather than the value: <c>process 1549 still waiting for ShareLock on transaction 809 after
/// 1000.123 ms</c> is three numbers a reader needs and none a customer typed. One more pattern is added
/// for the one prose shape that carries an unquoted value: a unique-violation DETAIL reads
/// <c>Key (email)=(someone@example.com) already exists.</c>, and the right-hand tuple is the row's data
/// verbatim — so <c>=(...)</c> after a <c>Key (...)</c> becomes <c>=(?)</c>. Wrong in the safe direction
/// where a column name follows the same shape.</description></item>
/// </list>
///
/// <para><b>Double quotes are NOT a safe signal, and review caught the first draft treating them as one.</b>
/// PostgreSQL double-quotes identifiers in its prose — <c>relation "orders"</c>, <c>user "app_rw"</c> —
/// and those are not values; a redaction that took them would leave <c>permission denied for relation
/// "?"</c>, which names nothing. But it double-quotes the offending VALUE too, in a whole class of routine
/// errors: <c>invalid input syntax for type integer: "abc"</c>, <c>malformed array literal: "{bad"</c>,
/// <c>date/time field value out of range: "2026-13-40"</c>, <c>invalid input value for enum mood: "x"</c>,
/// <c>syntax error at or near "…"</c> — client input verbatim, at WARNING-or-worse, needing no setting on
/// the target. So the rule is an ALLOWLIST in the safe direction: a double-quoted run is kept only when
/// the word before it is one PostgreSQL uses for a named object (<see cref="s_identifierNoun"/>), and
/// every other double-quoted run — including the <c>: "…"</c> and <c>at or near "…"</c> value shapes,
/// which are taken greedily to the closing quote because a JSON value carries quotes of its own — becomes
/// <c>"?"</c>. An unknown shape is over-redacted, never leaked. The one unquoted value shape beside the
/// key tuple, <c>Failing row contains (…)</c>, goes whole for the same reason. Every value shape also FAILS
/// CLOSED: a value whose close never arrived (a read boundary, a newline inside it, a cap) is masked to the
/// end of the text rather than kept (#3920's fourth review).</para>
/// </summary>
public static class PgLogTextRedactor
{
    /* How long one prose pattern may run on one text (#3920's fourth review, M3). Every pattern here is linear or
       near it on text PostgreSQL writes; this is the backstop for a shape nobody has found yet, and a text that
       runs it out is withheld whole rather than kept. */
    private static readonly TimeSpan s_proseTimeout = TimeSpan.FromMilliseconds(500);

    /* `Key (col, col)=(val, val)` — the unique/exclusion/foreign-key-violation DETAIL. The left tuple is
       column names and stays; the right tuple is the row's values, unquoted whatever their type, and goes
       whole.

       PostgreSQL does not escape the values, so a value can contain the very character that closes the
       tuple: `Key (name)=(Acme (USA) Inc.) already exists.` A `[^)]*` value pattern stops at the FIRST `)`
       and leaks ` Inc.)` into the store — caught in review, and exactly the leak the scope note forbids.
       So the value runs GREEDILY to the LAST `)` in the text, which is the tuple's true close because every
       sentence PostgreSQL writes after the tuple (`already exists.`, `is duplicated.`, `is still referenced
       from table "t".`, `is not present in table "t".`, `conflicts with existing key ...`) carries no
       parenthesis of its own; the lookahead asserts that no `)` follows. Wrong in the safe direction if a
       future sentence ever did carry one: it would redact the sentence too, never leak the value.

       The KEY tuple is matched lazily so an expression key — `Key (lower(email))=(...)` — is read whole: the
       lazy run extends past the inner `)` until `=(` follows. It is ATOMIC: the key is the first such run and is
       never re-tried longer. #3920's fourth review found the pair pattern, re-trying every longer key against
       every value, took 7 s on 27 KB of `Key (a)=(` and minutes on 90 KB; atomic, 19 ms.

       The exclusion-violation DETAIL carries TWO tuples — `Key (during)=(...) conflicts with existing key
       (during)=(...).` — and the greedy rule alone would fold the second key's name and the sentence
       between them into the first value: a leak of nothing, but a loss of the one word that says which
       constraint fired. So that shape is matched FIRST, both values redacted, both key names and the
       sentence kept; the greedy rule then takes every single-tuple shape. The exclusion pattern's own value
       runs are greedy too, so a value that happened to contain the sentence is consumed rather than split.
       Both patterns refuse a value that is already the redaction mark `(?)`, which is what stops the general
       rule from re-reading the exclusion rule's output — `Key (a)=(?) conflicts with existing key (a)=(?).` —
       as one tuple whose value runs to the final `)`, and folding the kept sentence back into a value.

       The general rule's close is the LAST `)` that one of those sentences follows, or that ends the text. A
       value cut before its close has neither, and is masked to the end rather than kept (#3920's fourth review:
       `Key (email)=(bob@exam` matched nothing and stayed whole). `[Kk]ey`, so an exclusion violation's second
       tuple, `... existing key (a)=(...`, is read the same way when the pair pattern could not take both. */
    private static readonly Regex s_exclusionTupleValues = new(
        @"(?<key>\bKey \((?>.*?\)(?==\()))=\((?!\?\)).*\) conflicts with existing key (?<key2>\((?>.*?\)(?==\()))=\((?!\?\)).*\)(?=[^)]*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline, s_proseTimeout);

    private static readonly Regex s_keyTupleValue = new(
        @"(?<key>\b[Kk]ey \((?>.*?\)(?==\()))=\((?!\?\))(?:.*\)(?= (?:already exists|is duplicated|is still referenced|is not present|conflicts with))|.*\)(?=\.?\s*$)|.*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline, s_proseTimeout);

    /* `: "value"` and `at or near "fragment"` — PostgreSQL's two ways of quoting the thing the client sent.
       Greedy to the LAST quote in the text, because the value can carry quotes of its own (`malformed array
       literal: "{"a"}"`) and a first-quote match would leave its middle standing; both shapes end the
       message, so the value's close is a quote that ends the text, or that the log's cursor position
       (` at character N`) follows. A value cut before its close is masked to the end (#3920's fourth review:
       `json: "{"card": 4111` closed at the key's quote and kept the number after it). */
    private static readonly Regex s_quotedValueShape = new(
        @"(?<=:\s|\bat or near\s)""(?:.*""(?=(?: at character [0-9]+)?\s*$)|.*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline, s_proseTimeout);

    /* `Failing row contains (1, abc, 2026-01-01).` — the NOT NULL / CHECK violation DETAIL, the row's
       values unquoted whatever their type. Greedy to the last `)` for the key tuple's reason, which must end
       the text; otherwise masked to the end (#3920's fourth review). */
    private static readonly Regex s_failingRow = new(
        @"\bFailing row contains \((?:.*\)(?=\.?\s*$)|.*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline, s_proseTimeout);

    /* `Partition key of the failing row contains (tenant_email) = (bob@example.com).` - the no-partition-found
       DETAIL, its values unquoted like a key tuple's and taken whole for the key tuple's reasons (#3920's
       review): the key list stays, the value list goes, greedy to a last `)` that ends the text, and to the end
       when there is none. */
    private static readonly Regex s_partitionKeyValue = new(
        @"(?<key>\bPartition key of the failing row contains \((?>.*?\)(?= = \())) = \((?!\?\))(?:.*\)(?=\.?\s*$)|.*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline, s_proseTimeout);

    /* `JSON data, line 1: {"card": 4111111111111111, ...` - the json parser's CONTEXT, which quotes the input
       line up to the error with nothing escaped (#3920's review). Everything after the lead goes, to the end of
       the line the parser wrote it on. */
    private static readonly Regex s_jsonDataLine = new(
        @"(?<lead>\bJSON data, line [0-9]+: )[^\n]*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant, s_proseTimeout);

    /* A double-quoted run or a single-quoted literal, whichever opens first, so an apostrophe inside a quoted
       name (`"o'k"`) never pairs with one outside it (#3920's review). Only the single-quoted kind is masked
       here; the double-quoted kind is left for the allowlist pass below. The single-quoted half IS the plan
       parser's pattern, not a copy of it. */
    private static readonly Regex s_quotedRun = new(
        @"""[^""]*""|" + PgPlanLogParser.s_quotedLiteral,
        RegexOptions.Compiled | RegexOptions.CultureInvariant, s_proseTimeout);

    /* The DETAIL shapes that carry SQL (#3920). A deadlock report opens with its wait-for lines, "Process N
       waits for <lock> on <object>; blocked by process M.", then writes each process's query as "Process N:
       <query>" (errdetail_log, each cut at track_activity_query_size). A crashed backend's report is one query,
       "Failed process was running: <query>", and a logged EXECUTE names its PREPARE as "prepare: <statement>"
       (errdetail_execute). A tab leads each continuation line in a stderr log, nothing in csvlog. Any other
       DETAIL is prose: round 3 split at any line shaped like a head, inside a value included, which kept a
       literal's middle and cut a key tuple from its close (#3920's fourth review). */
    private static readonly Regex s_deadlockWaitFor = new(
        @"^\t?Process (?<pid>[0-9]+) waits for .*; blocked by process (?<blocker>[0-9]+)\.$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_deadlockQueryHead = new(
        @"^\t?Process (?<pid>[0-9]+): ",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Regex s_singleQueryHead = new(
        @"^(?:Failed process was running: |prepare: )",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* A CONTEXT frame that quotes the statement it was running, unescaped (#3920's review): SPI's
       `SQL statement "..."` and PL/pgSQL's expression and assignment frames. */
    private static readonly Regex s_contextSqlFrame = new(
        @"^\t?(?:SQL statement|SQL expression|PL/pgSQL expression|PL/pgSQL assignment) """,
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* A one-line CONTEXT frame with no free text of its own: the line after one may open an SQL frame, where a
       line after another frame's value may not (#3920's fourth review: a COPY value running onto a line shaped
       like `SQL statement "..."` had that line read as SQL). */
    private static readonly Regex s_contextOneLineFrame = new(
        @"^\t?(?:PL/pgSQL function |SQL function ""|while |JSON data, line [0-9]+: |parallel worker|automatic (?:vacuum|analyze) of table "")",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /* What can follow such a frame: the line after its closing quote starts one of these, or the field ends. */
    private static readonly Regex s_contextFrameStart = new(
        @"^\t?(?:PL/pgSQL function |SQL function |SQL statement ""|SQL expression ""|PL/pgSQL expression ""|PL/pgSQL assignment ""|parallel worker|while |COPY |JSON data, )",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>What a statement becomes when it cannot be read to its end (cut inside a literal, a quoted
    /// identifier or a block comment, or written two ways at once): no mask can be trusted to have covered what
    /// the cut hid, so no statement is kept at all.</summary>
    public const string WithheldStatement = "<statement withheld: it could not be read to its end>";

    /// <summary>What prose becomes when masking it runs a pattern past its time budget: no mask can be trusted to
    /// have finished, so none of the text is kept.</summary>
    public const string WithheldProse = "<text withheld: it could not be masked in time>";

    /// <summary>PostgreSQL 18's normalized IN-list marker, <c>IN ($1 /*, ... */)</c>: a comment that is part of what
    /// a normalized statement says, and carries no value.</summary>
    public const string NormalizedInListMarker = "/*, ... */";

    /// <summary>The longest SQL-bearing field (a DETAIL's queries, a CONTEXT) masked at all (#3920's fourth
    /// review); a longer one is withheld. PostgreSQL cuts each query it writes there at
    /// <c>track_activity_query_size</c>, 1 kB by default, so this is far past any field it writes itself.</summary>
    internal const int MaxSqlFieldLength = 64 * 1024;

    /// <summary>How many candidate query boundaries one field may test (#3920's fourth review). Each test re-reads
    /// the text so far, so an unbounded count made a hostile field quadratic: a 180 KB CONTEXT took 21 s, and the
    /// self-hosted tail re-reads its overlap window every cycle. Past the cap the boundary is not taken, which
    /// leaves the text to be withheld or masked whole.</summary>
    internal const int MaxBoundaryTests = 32;

    /* Every remaining double-quoted run, with what precedes it captured so the allowlist can be asked. The
       word-and-space lead is tried first so `relation "x"` reaches the allowlist with its noun; the fallback
       is ANY single character — a bare space, a newline at the head of a tab-continuation, punctuation — so
       every double-quoted run is evaluated and one preceded by nothing recognisable is redacted rather than
       skipped. Review found the first draft's fallback matched non-space only, which left a quote after two
       spaces or after a newline neither redacted nor allowlisted. */
    private static readonly Regex s_doubleQuoted = new(
        @"(?<lead>(?:[A-Za-z_]+=|\b[A-Za-z_]+\s|^|.))(?<quoted>""(?:[^""]*""|[^""]*$))",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline, s_proseTimeout);

    /* The words PostgreSQL puts before a double-quoted IDENTIFIER. A run preceded by one of these is a
       name and stays; a run preceded by anything else is treated as a value. Enumerated rather than
       inferred, and wrong in the safe direction: a noun missing from this list over-redacts one name, a
       value shape missing from a blocklist would leak. `identity=` / `method=` / `application_name=` are
       the connection-authenticated line's own key=value spellings; `path` is the temp-file line's, which
       #3602 reads. */
    private static readonly Regex s_identifierNoun = new(
        @"(?:^|\b)(?:relation|table|column|constraint|index|sequence|view|function|procedure|routine|type|schema|database|role|user|extension|parameter|tablespace|trigger|rule|policy|language|domain|collation|operator|aggregate|publication|subscription|server|wrapper|mapping|file|directory|path|option|setting|slot|partition|attribute|object|library|module|record|conversion|dictionary|template|configuration|statistics|method|namespace|catalog|cursor|portal|savepoint|prepared statement|access method|event trigger|foreign table|materialized view|composite type|enum type|range type|base type|text search configuration|text search dictionary|text search parser|text search template|application_name=|identity=|method=)\s?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase, s_proseTimeout);

    /// <summary>Prose: quoted values out (single-quoted, double-quoted after a value shape or an unknown lead, key tuples, failing rows), identifiers and bare numbers kept; a value cut before its close is masked to the end, and a text that runs a pattern past its time budget comes back as <see cref="WithheldProse"/> (#3920's fourth review). Null in, null out.</summary>
    public static string? RedactMessage(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        try
        {
            return MaskProse(text);
        }
        catch (RegexMatchTimeoutException)
        {
            return WithheldProse;
        }
    }

    private static string MaskProse(string text)
    {
        /* The value shapes before the single-quote pass (#3920's review): a key tuple's value, a partition key's,
           a JSON line, is taken whole before an apostrophe inside it, or inside the quoted column name beside it,
           can pair with another one and leave the value's middle standing. */
        var scrubbed = s_exclusionTupleValues.Replace(text, "${key}=(?) conflicts with existing key ${key2}=(?)");
        scrubbed = s_keyTupleValue.Replace(scrubbed, "${key}=(?)");
        scrubbed = s_partitionKeyValue.Replace(scrubbed, "${key} = (?)");
        scrubbed = s_failingRow.Replace(scrubbed, "Failing row contains (?)");
        scrubbed = s_jsonDataLine.Replace(scrubbed, "${lead}?");
        scrubbed = s_quotedValueShape.Replace(scrubbed, "\"?\"");
        scrubbed = s_quotedRun.Replace(scrubbed, m => m.Value[0] == '\'' ? "'?'" : m.Value);

        return s_doubleQuoted.Replace(scrubbed, m =>
            s_identifierNoun.IsMatch(m.Groups["lead"].Value) && ReadsAsAName(m.Groups["quoted"].Value)
                ? m.Value
                : m.Groups["lead"].Value + "\"?\"");
    }

    /// <summary>
    /// Whether a double-quoted run after an identifier noun reads as a NAME (#3920's fourth review): closed, with
    /// a letter in it, and no non-ASCII character that is not a letter. <c>column "&lt;NBSP&gt;4111..."</c> and
    /// <c>column "4111..."</c> are values a client typed where a name was expected, and the noun alone kept them.
    /// </summary>
    private static bool ReadsAsAName(string quoted)
    {
        if (quoted.Length < 2 || quoted[^1] != '"')
        {
            return false;
        }

        var letter = false;
        for (var i = 1; i < quoted.Length - 1; i++)
        {
            var c = quoted[i];
            if (c >= '\u0080' && !char.IsLetter(c))
            {
                return false;
            }

            letter |= char.IsLetter(c);
        }

        return letter;
    }

    /// <summary>
    /// A DETAIL field (#3920). The SQL PostgreSQL writes into one is masked as a statement is
    /// (<see cref="RedactStoredStatement"/>), and withheld when it cannot be read to its end, which is common: the
    /// server cuts each query at <c>track_activity_query_size</c>. Prose masking kept bare numbers and
    /// dollar-quoted strings, so before #3920 a deadlock kept both sessions' values. Only three shapes carry SQL:
    /// a crash's <c>Failed process was running: query</c> and a logged EXECUTE's <c>prepare: statement</c>, each
    /// one query to the end of the field, and a deadlock report. A deadlock report's wait-for lines are prose,
    /// then each process's <c>Process N: query</c>. A <c>Process N:</c> line starts the next query only when N is
    /// one of the deadlock's own processes AND the query before it reads to its end; any other line, one a literal
    /// carries included, belongs to the query it follows (#3920's fourth review). Every other DETAIL is prose,
    /// read whole. Null in, null out; idempotent.
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

        if (waits == 0)
        {
            return RedactMessage(detail);
        }

        var output = new List<string>(lines.Length) { RedactMessage(string.Join('\n', lines, 0, waits)) ?? string.Empty };
        if (waits == lines.Length)
        {
            return output[0];
        }

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
    /// A CONTEXT field (#3920): prose through <see cref="RedactMessage"/>, except a frame that quotes the
    /// statement it was running (<c>SQL statement "..."</c>, PL/pgSQL's <c>expression</c> and
    /// <c>assignment</c> frames), whose statement is masked as SQL. PostgreSQL does not escape that quote, so a
    /// double quote inside the statement is not its end: the statement ends at the first closing quote before
    /// which the text reads to its end (<see cref="RedactStoredStatement"/>) and after which the next line starts
    /// another frame, or the field ends. A frame that no closing quote satisfies is withheld, with everything
    /// after it. Null in, null out; idempotent.
    ///
    /// <para><b>Where a frame starts</b> (#3920's fourth review). An SQL frame is read as SQL only on the field's
    /// first line, after a closed SQL frame, or after a one-line frame (<c>PL/pgSQL function ...</c>,
    /// <c>while ... in relation ...</c>, <c>JSON data, line N: ...</c>) that no open value precedes. Any other
    /// frame (a COPY row, say) is a value whose text can run onto the next line, so the lines after it are its
    /// content until another one-line frame starts on even quotes, and the run is masked as one prose block.
    /// Round 3 took any line shaped like a frame for one, so a COPY value holding such a line had its content
    /// read as SQL. A field longer than <see cref="MaxSqlFieldLength"/> is withheld whole, and a frame tests at
    /// most <see cref="MaxBoundaryTests"/> closes.</para>
    /// </summary>
    public static string? RedactContext(string? context)
    {
        if (string.IsNullOrEmpty(context))
        {
            return context;
        }

        if (context.Length > MaxSqlFieldLength)
        {
            return WithheldStatement;
        }

        var lines = context.Split('\n');
        var output = new List<string>(lines.Length);
        var value = new List<string>();
        var valueQuotes = 0;
        var frameMayStart = true;
        var i = 0;
        while (i < lines.Length)
        {
            var line = lines[i];
            var sqlFrame = frameMayStart ? s_contextSqlFrame.Match(line) : Match.Empty;
            if (sqlFrame.Success)
            {
                FlushProse(value, output);
                valueQuotes = 0;
                var (masked, close) = CloseSqlFrame(lines, i, sqlFrame.Length);
                if (masked is null)
                {
                    output.Add(sqlFrame.Value + WithheldStatement + "\"");
                    return string.Join('\n', output);
                }

                output.Add(sqlFrame.Value + masked + "\"");
                i = close + 1;
                frameMayStart = true;
                continue;
            }

            if (valueQuotes % 2 == 0 && s_contextOneLineFrame.IsMatch(line))
            {
                FlushProse(value, output);
                valueQuotes = 0;
                output.Add(RedactMessage(line) ?? string.Empty);
                frameMayStart = true;
                i++;
                continue;
            }

            value.Add(line);
            valueQuotes += CountQuotes(line);
            frameMayStart = false;
            i++;
        }

        FlushProse(value, output);
        return string.Join('\n', output);
    }

    /// <summary>Where the SQL frame opening on <paramref name="start"/> ends: its statement masked and the line of
    /// its closing quote, testing at most <see cref="MaxBoundaryTests"/> candidate closes; (null, -1) when none
    /// reads to its end.</summary>
    private static (string? Masked, int Close) CloseSqlFrame(string[] lines, int start, int headLength)
    {
        var tests = 0;
        for (var end = start; end < lines.Length; end++)
        {
            var closesHere = lines[end].EndsWith('"')
                && (end > start || lines[end].Length > headLength)
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

            body.Length--;
            if (RedactStoredStatement(body.ToString()) is { } masked)
            {
                return (masked, end);
            }
        }

        return (null, -1);
    }

    private static int CountQuotes(string line)
    {
        var count = 0;
        foreach (var c in line)
        {
            if (c == '"')
            {
                count++;
            }
        }

        return count;
    }

    private static void FlushProse(List<string> prose, List<string> output)
    {
        if (prose.Count > 0)
        {
            output.Add(RedactMessage(string.Join('\n', prose)) ?? string.Empty);
            prose.Clear();
        }
    }

    /// <summary>SQL: quoted literals AND bare numbers out, identifier-glued digits kept. Null in, null out.</summary>
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
    /// <c>deadlock_hash</c>. Over the RAW text on purpose — two entries that redact alike (the same error
    /// for two different values, in the same millisecond, from the same pid) are two events, and the hash
    /// has to keep them apart. A hash of raw text discloses nothing about the text.
    /// </summary>
    public static string RawLineHash(string rawText)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(rawText ?? string.Empty));
        return Convert.ToHexString(bytes, 0, 16);
    }
}
