/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The one pattern that names a statement whose text can carry a credential (#4348), shared by every place
/// that stores PostgreSQL statement text: the store's own <c>pg_stat_statements</c> reader
/// (<c>StoreStatementStats</c>, which had it first) and the two collectors that store a monitored TARGET's
/// statement text verbatim — <c>PgStatementText</c> and <c>PgBlockingCollector</c>. One definition, applied
/// with PostgreSQL's own regex engine (the <c>~*</c> operator) everywhere it is used, so a store and a
/// collector can never drift apart on what counts as sensitive.
///
/// <para><b>Why a PostgreSQL regex, not a .NET one.</b> The pattern uses POSIX ARE syntax
/// (<c>[[:&lt;:]]</c>/<c>[[:&gt;:]]</c> word boundaries, <c>[[:space:]]</c> classes) that only PostgreSQL's
/// engine understands, and it has no backslash so it means the same thing under either
/// <c>standard_conforming_strings</c> setting. Every caller therefore evaluates it inside SQL — a
/// <c>CASE WHEN text ~* pattern THEN placeholder ELSE text END</c> in the fetch query itself — rather than
/// porting it to .NET <c>Regex</c>, which would be a second, divergent copy of the same rule.</para>
/// </summary>
public static class PgSensitiveStatementFilter
{
    /// <summary>What may separate two SQL tokens: whitespace, a block comment or a line comment. Used only by
    /// <see cref="SensitiveStatementPattern"/>, a blocklist, where reading a comment short can only add matches.
    /// </summary>
    private const string TokenGap = "([[:space:]]|/[*]([^*]|[*]+[^*/])*[*]+/|--[^[:cntrl:]]*)";

    /// <summary>
    /// The statements whose text can carry a credential, as a case-insensitive PostgreSQL regular expression:
    /// a <c>PASSWORD</c> keyword followed by a literal (covers role, user and group DDL's <c>PASSWORD</c>
    /// clause, and <c>CREATE/ALTER USER MAPPING</c>'s password option, since both always end in the literal),
    /// a libpq <c>password=</c> or <c>PGPASSWORD=</c> setting (covers a connection string wherever it appears —
    /// subscription DDL, foreign server DDL, <c>dblink</c>), and a URI's <c>user:secret@</c>, with comments
    /// allowed wherever the grammar allows whitespace.
    ///
    /// <para><b>No blanket DDL-name alternative, on purpose.</b> An earlier version also matched
    /// <c>CREATE|ALTER ROLE|USER|GROUP|SUBSCRIPTION|SERVER</c> by name alone, meaning to catch every form of
    /// each DDL statement; in practice it flagged forms that carry no credential at all (<c>ALTER ROLE app
    /// SET work_mem = '64MB'</c>) while adding nothing the literal-following alternatives below did not
    /// already catch (every credential-bearing form of that DDL ends in a literal). Removed rather than
    /// carried forward.</para>
    ///
    /// <para><b>No backslash, on purpose.</b> <c>[[:&lt;:]]</c>, <c>[[:&gt;:]]</c>, <c>[[:space:]]</c>,
    /// <c>[*]</c> and <c>[$]</c> spell what a first version wrote with backslash escapes, so the literal means
    /// the same under either <c>standard_conforming_strings</c> setting. A normalized DML parameter
    /// (<c>password = $1</c>) is not a hit: it carries no value.</para>
    /// </summary>
    public const string SensitiveStatementPattern =
        "[[:<:]](create|alter)" + TokenGap + "+(role|user|group|subscription|server)[[:>:]]"
        + "|[[:<:]]password[[:>:]]" + TokenGap + "*(=|to)?" + TokenGap + "*(e?'|u&'|[$][^0-9])"
        + "|[[:<:]](pg)?password[[:space:]]*=[[:space:]]*[^$[:space:]]"
        + "|[a-z][a-z0-9+.-]*://[^[:space:]/@:]+:[^[:space:]/@]+@";

    /// <summary>What a collector or reader stores/returns in place of a statement <see cref="SensitiveStatementPattern"/>
    /// names, in place of its text. Fixed, so a reader never has to distinguish "withheld" from "not captured
    /// yet" by anything other than this literal.</summary>
    public const string PlaceholderText = "-- statement text withheld (#4348)";
}
