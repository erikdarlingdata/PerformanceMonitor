/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The one read-side predicate that hides the monitor's own Query Store statement from every reader that ranks Query Store
/// queries (#6654, #6665).
///
/// <para><b>Why a read-side predicate.</b> The collector drops a row whose text carries <see cref="QueryStoreCollector.SelfQueryMarker"/>,
/// but it tests <c>row.QueryText</c>, and on Darling that column is NULL: the Query Store read fetches the text separately
/// (<c>FetchQueryTextSeparately</c>) and lands it once in <c>collect.query_store_text</c>, which has no marker check. So the
/// monitor's statement is stored as an ordinary fact row, and every reader that resolves text from <c>query_store_text</c> can list it
/// (confirmed on a live store: <c>get_query_store_top</c> listed it, 241 executions in 24 hours). The collector is left alone on
/// purpose: a server-side <c>NOT LIKE</c> on the text cost 75% of the read's time (#1565), and the rows already stored stay for the
/// whole retention window, so a read-side filter is needed whatever the collector does.</para>
///
/// <para><b>Fail-open.</b> A query with no known text passes: the predicate only removes a query whose text is known and carries the
/// marker.</para>
///
/// <para>The marker is a plain word, so it is spliced into the SQL as a literal (no quoting needed).</para>
/// </summary>
public static class QueryStoreSelfStatement
{
    /// <summary>The marker the monitor puts in the text of every statement it sends to a monitored server.</summary>
    public const string Marker = QueryStoreCollector.SelfQueryMarker;

    private const string Open = "strpos(COALESCE(";
    private const string Close = ", ''), '" + Marker + "')";

    /// <summary>The column <see cref="NotSelfOfLateralText"/> tests: the text a Query Store top read has already resolved
    /// in its <c>LEFT JOIN LATERAL (...) AS t</c>.</summary>
    public const string LateralTextColumn = "t.query_text";

    /// <summary>The predicate for a read that has already resolved the text as <c>t.query_text</c>, usable inside a constant SQL string.</summary>
    public const string NotSelfOfLateralText = Open + LateralTextColumn + Close + " = 0";

    /// <summary>The same test, true for the monitor's own statement, over <c>x.query_sql_text</c> of a
    /// <c>query_store_text AS x</c> row. For a read that already probes <c>query_store_text</c> by key (a <c>NOT EXISTS</c>), so it adds
    /// no second probe, and usable inside a constant SQL string.</summary>
    public const string SelfOfStoredText = Open + "x.query_sql_text" + Close + " > 0";

    /// <summary>The predicate over a text expression the read has already resolved.</summary>
    public static string NotSelfOfText(string textExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(textExpression);
        return Open + textExpression + Close + " = 0";
    }

    /// <summary>The same test, true for the monitor's own statement, over a text expression. The self-statement key writers
    /// (<see cref="QueryStoreSelfKeys"/>) use it, so the keys they record and the text probe they replace are one definition (#6750).</summary>
    public static string SelfOfText(string textExpression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(textExpression);
        return Open + textExpression + Close + " > 0";
    }

    /// <summary>
    /// The predicate for a read that has not resolved the text: the text is looked up in <c>query_store_text</c> by its key
    /// (one primary-key probe per row), falling back to <paramref name="inlineTextExpression"/> when that table has no row for the
    /// query. A read whose rows can only carry inline text from before the separate-fetch cutover passes <c>null</c>: the
    /// collector's own drop already removed those rows when they were collected.
    /// </summary>
    public static string NotSelfOfKeyedText(string serverIdExpression, string databaseExpression, string queryIdExpression, string? inlineTextExpression = null)
    {
        var lookup = $"(SELECT x.query_sql_text FROM query_store_text AS x WHERE x.server_id = {serverIdExpression} AND x.database_name = {databaseExpression} AND x.query_id = {queryIdExpression})";
        return NotSelfOfText(inlineTextExpression is null ? lookup : $"COALESCE({lookup}, {inlineTextExpression})");
    }

    /// <summary>
    /// #6750 part 3: the predicate over the key table the text writer fills (<see cref="QueryStoreSelfKeys"/>), true when the query is
    /// not one of the monitor's own. One primary-key probe of that table per row, so it reads no text. Use it only once
    /// <see cref="QueryStoreSelfKeys.ReadyAsync"/> says the backfill has run: before that the table misses the text stored earlier.
    /// </summary>
    public static string NotSelfOfKey(string serverIdExpression, string databaseExpression, string queryIdExpression) =>
        $"NOT EXISTS (SELECT 1 FROM {QueryStoreSelfKeys.TableName} AS sk WHERE sk.server_id = {serverIdExpression} AND sk.database_name = {databaseExpression} AND sk.query_id = {queryIdExpression})";
}
