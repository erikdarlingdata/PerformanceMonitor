/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
/// key tuple, <c>Failing row contains (…)</c>, goes whole for the same reason.</para>
/// </summary>
public static class PgLogTextRedactor
{
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
       lazy run extends past the inner `)` until `=(` follows.

       The exclusion-violation DETAIL carries TWO tuples — `Key (during)=(...) conflicts with existing key
       (during)=(...).` — and the greedy rule alone would fold the second key's name and the sentence
       between them into the first value: a leak of nothing, but a loss of the one word that says which
       constraint fired. So that shape is matched FIRST, both values redacted, both key names and the
       sentence kept; the greedy rule then takes every single-tuple shape. The exclusion pattern's own value
       runs are greedy too, so a value that happened to contain the sentence is consumed rather than split.
       Both patterns refuse a value that is already the redaction mark `(?)`, which is what stops the general
       rule from re-reading the exclusion rule's output — `Key (a)=(?) conflicts with existing key (a)=(?).` —
       as one tuple whose value runs to the final `)`, and folding the kept sentence back into a value. */
    private static readonly Regex s_exclusionTupleValues = new(
        @"(?<key>\bKey \(.*?\))=\((?!\?\)).*\) conflicts with existing key (?<key2>\(.*?\))=\((?!\?\)).*\)(?=[^)]*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    private static readonly Regex s_keyTupleValue = new(
        @"(?<key>\bKey \(.*?\))=\((?!\?\)).*\)(?=[^)]*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    /* `: "value"` and `at or near "fragment"` — PostgreSQL's two ways of quoting the thing the client sent.
       Greedy to the LAST quote in the text, because the value can carry quotes of its own (`malformed array
       literal: "{"a"}"`) and a first-quote match would leave its middle standing; both shapes end the
       message, so the last quote is the value's close. */
    private static readonly Regex s_quotedValueShape = new(
        @"(?<=:\s|\bat or near\s)"".*""(?=[^""]*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    /* `Failing row contains (1, abc, 2026-01-01).` — the NOT NULL / CHECK violation DETAIL, the row's
       values unquoted whatever their type. Greedy to the last `)` for the key tuple's reason. */
    private static readonly Regex s_failingRow = new(
        @"\bFailing row contains \(.*\)(?=[^)]*$)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    /* Every remaining double-quoted run, with what precedes it captured so the allowlist can be asked. The
       word-and-space lead is tried first so `relation "x"` reaches the allowlist with its noun; the fallback
       is ANY single character — a bare space, a newline at the head of a tab-continuation, punctuation — so
       every double-quoted run is evaluated and one preceded by nothing recognisable is redacted rather than
       skipped. Review found the first draft's fallback matched non-space only, which left a quote after two
       spaces or after a newline neither redacted nor allowlisted. */
    private static readonly Regex s_doubleQuoted = new(
        @"(?<lead>(?:[A-Za-z_]+=|\b[A-Za-z_]+\s|^|.))(?<quoted>""[^""]*"")",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.Singleline);

    /* The words PostgreSQL puts before a double-quoted IDENTIFIER. A run preceded by one of these is a
       name and stays; a run preceded by anything else is treated as a value. Enumerated rather than
       inferred, and wrong in the safe direction: a noun missing from this list over-redacts one name, a
       value shape missing from a blocklist would leak. `identity=` / `method=` / `application_name=` are
       the connection-authenticated line's own key=value spellings; `path` is the temp-file line's, which
       #3602 reads. */
    private static readonly Regex s_identifierNoun = new(
        @"(?:^|\b)(?:relation|table|column|constraint|index|sequence|view|function|procedure|routine|type|schema|database|role|user|extension|parameter|tablespace|trigger|rule|policy|language|domain|collation|operator|aggregate|publication|subscription|server|wrapper|mapping|file|directory|path|option|setting|slot|partition|attribute|object|library|module|record|conversion|dictionary|template|configuration|statistics|method|namespace|catalog|cursor|portal|savepoint|prepared statement|access method|event trigger|foreign table|materialized view|composite type|enum type|range type|base type|text search configuration|text search dictionary|text search parser|text search template|application_name=|identity=|method=)\s?$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    /// <summary>Prose: quoted values out (single-quoted, double-quoted after a value shape or an unknown lead, key tuples, failing rows), identifiers and bare numbers kept. Null in, null out.</summary>
    public static string? RedactMessage(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        var scrubbed = PgPlanLogParser.s_quotedLiteral.Replace(text, "'?'");
        scrubbed = s_exclusionTupleValues.Replace(scrubbed, "${key}=(?) conflicts with existing key ${key2}=(?)");
        scrubbed = s_keyTupleValue.Replace(scrubbed, "${key}=(?)");
        scrubbed = s_failingRow.Replace(scrubbed, "Failing row contains (?)");
        scrubbed = s_quotedValueShape.Replace(scrubbed, "\"?\"");

        return s_doubleQuoted.Replace(scrubbed, m =>
            s_identifierNoun.IsMatch(m.Groups["lead"].Value)
                ? m.Value
                : m.Groups["lead"].Value + "\"?\"");
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
    /// <para><b>Backslash is an escape in every single-quoted literal</b>, not only in <c>E''</c>: a client
    /// with <c>standard_conforming_strings</c> off writes <c>'it\'s'</c>, and reading that backslash as a
    /// plain character would end the literal early and leave the rest standing. Wrong in the safe direction
    /// on a standard string that ends in a backslash: the mask runs on to the next quote, or the text ends
    /// inside a literal and is refused.</para>
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

            if (char.IsWhiteSpace(c))
            {
                pendingSpace = true;
                i++;
                continue;
            }

            if (c == '-' && next == '-')
            {
                var newline = text.IndexOf('\n', i);
                i = newline < 0 ? text.Length : newline + 1;
                pendingSpace = true;
                continue;
            }

            if (c == '/' && next == '*')
            {
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
                var j = literalOpen + 1;
                var closed = false;
                while (j < text.Length)
                {
                    if (text[j] == '\\')
                    {
                        j += 2;
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
                while (end < text.Length && (IsIdentifierChar(text[end], first: false) || text[end] == '$'))
                {
                    end++;
                }

                Emit(text[i..end]);
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
