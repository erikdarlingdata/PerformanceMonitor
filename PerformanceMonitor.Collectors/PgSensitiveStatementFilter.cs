/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The one pattern that names a statement whose text can carry a credential (#4348), shared by every place
/// that stores PostgreSQL statement text: the store's own <c>pg_stat_statements</c> reader
/// (<c>StoreStatementStats</c>, which had it first) and the two collectors that store a monitored TARGET's
/// statement text verbatim — <c>PgStatementText</c> and <c>PgBlockingCollector</c>. One definition, applied
/// with PostgreSQL's own regex engine (the <c>~*</c> operator) everywhere it is used, so a store and a
/// collector can never drift apart on what counts as sensitive.
///
/// <para>The pattern itself now lives in <see cref="SensitiveStatements"/>; this class keeps the PostgreSQL
/// helpers that embed it in SQL.</para>
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
    /// <summary>
    /// The statements whose text can carry a credential, as a case-insensitive PostgreSQL regular expression:
    /// an alias of the one shared definition, <see cref="SensitiveStatements.Pattern"/> (PostgreSQL's own
    /// credential forms and the T-SQL ones, #4348). A const alias, not a copy, so the string is the same
    /// instance everywhere and no second definition can drift.
    /// </summary>
    public const string SensitiveStatementPattern = SensitiveStatements.Pattern;

    /// <summary>What a collector or reader stores/returns in place of a statement <see cref="SensitiveStatementPattern"/>
    /// names, in place of its text: an alias of <see cref="SensitiveStatements.PlaceholderText"/>. Fixed, so a
    /// reader never has to distinguish "withheld" from "not captured yet" by anything other than this
    /// literal.</summary>
    public const string PlaceholderText = SensitiveStatements.PlaceholderText;

    /// <summary>
    /// A value as a single-quoted SQL string literal, with its own quote characters doubled — the one rule
    /// every caller applies to <see cref="SensitiveStatementPattern"/> and <see cref="PlaceholderText"/>
    /// before either reaches a query, so a test asserting on the built SQL and a collector building it agree
    /// on the same doubled form rather than each re-deriving it.
    /// </summary>
    public static string SqlLiteral(string value) => "'" + value.Replace("'", "''", System.StringComparison.Ordinal) + "'";

    /// <summary>
    /// The <c>CASE WHEN ... ~* pattern THEN placeholder ELSE column END</c> expression that withholds
    /// <paramref name="column"/>'s text when <see cref="SensitiveStatementPattern"/> names it, built once here
    /// so <c>PgStatementText</c> and <c>PgBlockingCollector</c> embed the identical expression rather than each
    /// composing their own copy.
    /// </summary>
    public static string SqlPredicate(string column) =>
        "CASE WHEN " + column + " ~* " + SqlLiteral(SensitiveStatementPattern) +
        " THEN " + SqlLiteral(PlaceholderText) +
        " ELSE " + column + " END";
}
