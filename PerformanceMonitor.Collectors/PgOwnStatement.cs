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
/// <para><b>Own-ness is per ENTRY, not per queryid.</b> <c>pg_stat_statements</c> keeps one entry per
/// <c>(userid, dbid, queryid, toplevel)</c> and stores the text of the FIRST call that created each entry, so an
/// application's <c>SELECT pg_sleep($1)</c> under its own login is a different entry from the sampler's, with its own
/// text and no marker, even though both share a <c>queryid</c> (the comment never changes it). The hourly text fetch
/// (<c>PgStatementText</c>) reads which entries' text ends with <see cref="Marker"/> and the store keeps their
/// <c>[user_id, database_id]</c> pairs per queryid (<c>pg_statement_text.own_entries</c>). A statistics row is the
/// service's only when its own <c>(user_id, database_id)</c> is in that list, so the application's rows under another
/// login stay in Analysis and the top-queries reads.</para>
///
/// <para><b>Why not <c>userid</c> alone.</b> <c>pg_stat_statements</c> has no application name, so the session reads'
/// <c>application_name</c> exclusion (#7381) can't carry over. Excluding every statement of the service's login would
/// hide the application's real statements wherever the service monitors with a shared login (<c>postgres</c>, or the
/// admin role on RDS and Azure). A comment names the statement itself, and the entry key says whose it is.</para>
///
/// <para><b>Why it trails.</b> <c>pg_stat_statements</c> cuts a LEADING comment from the stored text (measured on
/// PostgreSQL 18.6: <c>/* x */ SELECT 1</c> is stored as <c>SELECT $1</c>) and keeps one that sits inside the
/// statement's own span, so the marker goes at the end of each statement, before its semicolon.</para>
///
/// <para><b>What it still gets wrong.</b> Two limits, both measured. First, a SHARED login: when the service and the
/// application connect as the same role (<c>postgres</c>) and the service ran a statement first since the last
/// <c>pg_stat_statements_reset()</c>, the one shared entry carries the marker and ALL its calls, the application's
/// included, are left out of Analysis. Second, the store learns own entries hourly, so a statement is shown until its
/// text is read. Everything not known to be the service's is shown.</para>
/// </summary>
public static class PgOwnStatement
{
    /// <summary>The marker comment. One constant, used by every statement the PostgreSQL collectors send.</summary>
    public const string Marker = "/* PerformanceMonitor Darling */";

    /// <summary>
    /// A SQL predicate, true when the <c>pg_statement_stats</c> row is NOT one of the service's own entries: its
    /// <c>(user_id, database_id)</c> is not in the <c>own_entries</c> list the hourly text fetch stored for its queryid.
    /// A row with no stored text, or with no own entries, counts as not ours. Splice it into a <c>WHERE</c> over
    /// <c>pg_statement_stats</c> that is not aliased.
    /// </summary>
    public const string NotOwnOnStats =
        "NOT EXISTS (SELECT 1 FROM pg_statement_text AS own WHERE own.server_id = pg_statement_stats.server_id "
        + "AND own.queryid = pg_statement_stats.queryid AND jsonb_contains(own.own_entries, "
        + "jsonb_build_array(jsonb_build_array(pg_statement_stats.user_id, pg_statement_stats.database_id))))";

    /// <summary>
    /// A SQL boolean expression, true when the statement text <paramref name="textExpression"/> carries the marker.
    /// Used by the text fetch, on the monitored server, to find the service's own entries.
    /// </summary>
    public static string IsOwnSql(string textExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(textExpression);
        return "coalesce(right(" + textExpression + ", " + Marker.Length.ToString(System.Globalization.CultureInfo.InvariantCulture)
            + ") = '" + Marker + "', false)";
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
