/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Builds the parameterized database-exclusion fragment collectors splice into their queries.
/// Ported verbatim from Lite's RemoteCollectorService.BuildDatabaseExclusionFilter (Lite's copy now calls
/// this one, and DatabaseExclusionFilterTests pins that the two stay identical).
/// Each exact name is parameterized (@excl_db_N, nvarchar(128)) — works on every supported SQL Server
/// version (no STRING_SPLIT/OPENJSON compatibility-level dependency).
/// An entry holding <c>*</c> is a pattern (#9050, see <see cref="DatabaseNamePattern"/>): it becomes one
/// <c>NOT LIKE @excl_like_N ESCAPE '\'</c> term beside the <c>NOT IN</c> of the exact names. A list with no
/// pattern produces exactly the text and parameters it did before patterns existed.
/// </summary>
public static class DatabaseExclusionFilter
{
    /// <param name="excludedDatabaseNames">Names and patterns from the server's excluded-databases config.</param>
    /// <param name="columnExpression">SQL column to filter, e.g. "d.name".</param>
    public static (string Clause, List<CollectorParameter> Parameters) Build(
        IReadOnlyList<string>? excludedDatabaseNames, string columnExpression)
    {
        var (terms, parameters) = BuildTerms(excludedDatabaseNames, columnExpression);
        return (terms.Count == 0 ? string.Empty : "AND " + string.Join(" AND ", terms), parameters);
    }

    /// <summary>The exclusion as separate boolean terms, each true for a row to keep, with their parameters.
    /// <see cref="Build"/> joins them with AND; a caller that wraps them (the default-trace read's NULL arm)
    /// joins them itself.</summary>
    internal static (List<string> Terms, List<CollectorParameter> Parameters) BuildTerms(
        IReadOnlyList<string>? excludedDatabaseNames, string columnExpression)
    {
        var terms = new List<string>();
        var parameters = new List<CollectorParameter>();
        if (excludedDatabaseNames is null || excludedDatabaseNames.Count == 0)
        {
            return (terms, parameters);
        }

        var paramNames = new List<string>(excludedDatabaseNames.Count);
        var likeTerms = new List<string>();
        var likeParameters = new List<CollectorParameter>();
        foreach (var name in excludedDatabaseNames)
        {
            /* An entry made only of * is ignored (#9050), as it was before wildcards: it would drop every database. */
            if (DatabaseNamePattern.IsMatchAll(name))
            {
                continue;
            }

            if (DatabaseNamePattern.IsPattern(name))
            {
                /* NVarChar260, not 128: a 128-character pattern with every character escaped is 256 characters,
                   and a truncated pattern would match the wrong names without any error. */
                string like = $"@excl_like_{likeTerms.Count}";
                likeTerms.Add($"{columnExpression} NOT LIKE {like} ESCAPE '{DatabaseNamePattern.LikeEscape}'");
                likeParameters.Add(new CollectorParameter(
                    like, DatabaseNamePattern.ToLikePattern(name, escapeBracket: true), CollectorParameterType.NVarChar260));
                continue;
            }

            string p = $"@excl_db_{paramNames.Count}";
            paramNames.Add(p);
            parameters.Add(new CollectorParameter(p, name, CollectorParameterType.NVarChar128));
        }

        if (paramNames.Count > 0)
        {
            terms.Add($"{columnExpression} NOT IN ({string.Join(", ", paramNames)})");
        }

        terms.AddRange(likeTerms);
        parameters.AddRange(likeParameters);
        return (terms, parameters);
    }

    /// <summary>
    /// Builds a SQL fragment with database names interpolated as literal N'...' values, for
    /// dynamic-SQL paths where parameter binding is awkward (e.g. inside a string passed to
    /// sp_executesql). Names come from user-picked checklists of existing databases, so literal
    /// interpolation with single-quote escaping is safe. When <paramref name="forNestedDynamicSql"/>
    /// is true, the escape is doubled for use inside an outer T-SQL string that itself becomes a
    /// dynamic-SQL @sql variable. Ported verbatim from Lite's
    /// RemoteCollectorService.BuildDatabaseExclusionLiteralClause (identity-pinned by test).
    /// A pattern entry (#9050) becomes a <c>NOT LIKE N'...' ESCAPE '\'</c> term, quoted the same way.
    /// </summary>
    public static string BuildLiteralClause(
        IReadOnlyList<string>? excludedDatabaseNames, string columnExpression, bool forNestedDynamicSql = false)
    {
        if (excludedDatabaseNames is null || excludedDatabaseNames.Count == 0)
        {
            return string.Empty;
        }

        string escapedQuote = forNestedDynamicSql ? "''" : "'";
        string Escape(string s) => forNestedDynamicSql
            ? s.Replace("'", "''''")
            : s.Replace("'", "''");

        var quoted = new List<string>(excludedDatabaseNames.Count);
        var likeTerms = new List<string>();
        foreach (var name in excludedDatabaseNames)
        {
            /* An entry made only of * is ignored (#9050), as it was before wildcards: it would drop every database. */
            if (DatabaseNamePattern.IsMatchAll(name))
            {
                continue;
            }

            if (DatabaseNamePattern.IsPattern(name))
            {
                var pattern = DatabaseNamePattern.ToLikePattern(name, escapeBracket: true);
                likeTerms.Add($"{columnExpression} NOT LIKE N{escapedQuote}{Escape(pattern)}{escapedQuote} "
                    + $"ESCAPE {escapedQuote}{DatabaseNamePattern.LikeEscape}{escapedQuote}");
                continue;
            }

            quoted.Add($"N{escapedQuote}{Escape(name)}{escapedQuote}");
        }

        var terms = new List<string>();
        if (quoted.Count > 0)
        {
            terms.Add($"{columnExpression} NOT IN ({string.Join(", ", quoted)})");
        }

        terms.AddRange(likeTerms);
        if (terms.Count == 0)
        {
            return string.Empty;
        }

        return "AND " + string.Join(" AND ", terms);
    }
}
