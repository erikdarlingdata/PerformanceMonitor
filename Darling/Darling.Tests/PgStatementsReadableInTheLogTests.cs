/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9236: <see cref="PgLogTextRedactor.RedactStoredStatement"/> withholds a statement whole when an odd run of backslashes
/// stands right before a quote, which is the shape of <c>ESCAPE '\'</c>. A statement the product sends to PostgreSQL that
/// carries one shows as "statement withheld" in the store log and in a slow-plan report, so a timeout cannot be traced to
/// it. Each statement here is the text the product sends; the redactor must give it back as text.
/// </summary>
public sealed class PgStatementsReadableInTheLogTests
{
    public static IEnumerable<object[]> Statements()
    {
        yield return ["find_queries_by_text Query Store", DarlingMcpFindQueriesByTextTools.QueryStoreSql];
        yield return ["find_queries_by_text plan cache", DarlingMcpFindQueriesByTextTools.PlanCacheSql];
        yield return ["find_queries_by_text all servers", DarlingMcpFindQueriesByTextTools.PlanCacheAllServersSql];
        yield return ["find_queries_by_text all servers span", DarlingMcpFindQueriesByTextTools.PlanCacheAllServersSpanSql];
        yield return ["find_queries_by_text all servers lookup span", DarlingMcpFindQueriesByTextTools.PlanCacheAllServersLookupSpanSql];
        yield return ["find_queries_by_text matched digests", DarlingMcpFindQueriesByTextTools.MatchedDigestsSql];
        yield return ["find_queries_by_text sample probe", DarlingMcpFindQueriesByTextTools.PlanCacheAllServersProbeSql];
        yield return ["find_queries_by_text plan cache probe", DarlingMcpFindQueriesByTextTools.PlanCacheProbeSql];
        yield return ["collected databases search", CollectedDatabases.NamesSearchLimitedSql];
        yield return ["setting scrub candidates", PgSettingScrub.CandidateSql];
    }

    [Theory]
    [MemberData(nameof(Statements))]
    public void The_log_shows_the_statement_the_product_sent(string label, string statement)
    {
        var shown = PgLogTextRedactor.RedactStoredStatement(statement);

        Assert.True(shown is not null, $"{label}: the statement is withheld from the log");
        Assert.False(string.IsNullOrWhiteSpace(shown), $"{label}: the statement shows as empty text");
    }
}
