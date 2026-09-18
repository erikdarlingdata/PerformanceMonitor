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
/// with — the plan hash's reasoning, over SQL text.</description></item>
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
/// <para><b>Identifiers stay.</b> PostgreSQL double-quotes identifiers in its prose — <c>relation
/// "orders"</c>, <c>user "app_rw"</c> — and those are not values; a redaction that took them would leave
/// <c>permission denied for relation "?"</c>, which names nothing and helps nobody. The single-quote
/// pattern does not touch them.</para>
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

    /// <summary>Prose: quoted literals and key-tuple values out, bare numbers kept. Null in, null out.</summary>
    public static string? RedactMessage(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        var scrubbed = PgPlanLogParser.s_quotedLiteral.Replace(text, "'?'");
        scrubbed = s_exclusionTupleValues.Replace(scrubbed, "${key}=(?) conflicts with existing key ${key2}=(?)");
        return s_keyTupleValue.Replace(scrubbed, "${key}=(?)");
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
