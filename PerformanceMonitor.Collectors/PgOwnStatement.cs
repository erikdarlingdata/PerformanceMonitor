/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The marker that tells the service's own statements apart from the monitored application's in
/// <c>pg_stat_statements</c> (#7567).
///
/// <para><b>Why a comment and not <c>userid</c>.</b> <c>pg_stat_statements</c> has no application name, so the session
/// reads' <c>application_name</c> exclusion (#7381) can't carry over. <c>userid</c> would hide the application's real
/// statements wherever the service monitors with a shared login (<c>postgres</c>, or the admin role on RDS and Azure):
/// a missed finding. A comment names the statement itself.</para>
///
/// <para><b>Why it trails.</b> <c>pg_stat_statements</c> cuts a LEADING comment from the stored text (measured on
/// PostgreSQL 18.6: <c>/* x */ SELECT 1</c> is stored as <c>SELECT $1</c>) and keeps one that sits inside the
/// statement's own span, so the marker goes at the end of each statement, before its semicolon. The stored text is the
/// first call's since the last <c>pg_stat_statements_reset()</c>; the comment never changes the <c>queryid</c>.</para>
///
/// <para><b>What it gets wrong.</b> If another client ran the same normalized statement first since the last reset,
/// the stored text has no marker and the service's statement reads as the application's. That errs toward showing the
/// statement. The store also learns the text hourly, so a statement is unmarked in the store until its text is read.</para>
/// </summary>
public static class PgOwnStatement
{
    /// <summary>The marker comment. One constant, used by every statement the PostgreSQL collectors send.</summary>
    public const string Marker = "/* PerformanceMonitor Darling */";

    /// <summary>
    /// A SQL predicate, true when the statement behind <c>pg_statement_stats</c> row(s) named
    /// <c>pg_statement_stats</c> is NOT one of the service's own. A statement with no stored text counts as not ours.
    /// Splice it into a <c>WHERE</c> over <c>pg_statement_stats</c> that is not aliased.
    /// </summary>
    public const string NotOwnOnStats =
        "NOT EXISTS (SELECT 1 FROM pg_statement_text AS own WHERE own.server_id = pg_statement_stats.server_id "
        + "AND own.queryid = pg_statement_stats.queryid AND right(own.query_text, 32) = '" + Marker + "')";

    /// <summary>The same predicate for a <c>pg_statement_stats</c> aliased <c>s</c>.</summary>
    public const string NotOwnOnS =
        "NOT EXISTS (SELECT 1 FROM pg_statement_text AS own WHERE own.server_id = s.server_id "
        + "AND own.queryid = s.queryid AND right(own.query_text, 32) = '" + Marker + "')";

    /// <summary>
    /// A SQL boolean expression, true when the statement text <paramref name="textExpression"/> carries the marker.
    /// </summary>
    public static string IsOwnSql(string textExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(textExpression);
        return "coalesce(right(" + textExpression + ", 32) = '" + Marker + "', false)";
    }

    /// <summary>Whether stored statement text carries the marker.</summary>
    public static bool IsOwn(string? statementText) =>
        statementText != null && statementText.EndsWith(Marker, StringComparison.Ordinal);

    /// <summary>
    /// Marks <paramref name="sql"/>: the marker goes on its own line after the last token, before a trailing
    /// semicolon. Idempotent, so a batch whose builder marked each statement is left as built.
    /// </summary>
    public static string Mark(string sql)
    {
        ArgumentNullException.ThrowIfNull(sql);
        var body = sql.TrimEnd();
        var semicolon = body.EndsWith(';');
        if (semicolon)
        {
            body = body[..^1].TrimEnd();
        }

        if (body.EndsWith(Marker, StringComparison.Ordinal))
        {
            return sql;
        }

        return body + "\n" + Marker + (semicolon ? ";" : string.Empty);
    }

    /// <summary>
    /// Marks every statement of a batch: each fragment is marked on its own, then joined with newlines. The caller
    /// passes the statements without their semicolons.
    /// </summary>
    public static string MarkBatch(System.Collections.Generic.IEnumerable<string> statements)
    {
        ArgumentNullException.ThrowIfNull(statements);
        var sb = new System.Text.StringBuilder();
        foreach (var statement in statements)
        {
            sb.Append(Mark(statement.TrimEnd().TrimEnd(';') + ";")).Append('\n');
        }

        return sb.ToString().TrimEnd('\n');
    }
}
