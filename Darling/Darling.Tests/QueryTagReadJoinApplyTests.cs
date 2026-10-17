/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5984 round 1: <see cref="QueryTagReadJoin.Apply"/> never serves unfiltered rows under a tag the caller asked for. A requested
/// tag on a statement with no <c>/*TAG:*/</c> token throws (the tool answers with its error envelope); no tag leaves the text alone.
/// </summary>
public sealed class QueryTagReadJoinApplyTests
{
    private const string WithToken = "SELECT query_hash FROM t WHERE 1 = 1 /*TAG:hash:query_hash*/ GROUP BY query_hash";
    private const string WithoutToken = "SELECT query_hash FROM t WHERE 1 = 1 GROUP BY query_hash";
    private static readonly QueryTagFilter Tagged = new(new[] { 7 }, false, false, "Prod");

    [Fact]
    public void ATagRequestedOnAStatementWithNoToken_Throws_NamingTheStatement()
    {
        var error = Assert.Throws<InvalidOperationException>(() => QueryTagReadJoin.Apply(WithoutToken, 1, Tagged));
        Assert.Contains("SELECT query_hash FROM t", error.Message, StringComparison.Ordinal);

        Assert.Throws<InvalidOperationException>(() =>
            QueryTagReadJoin.Apply(WithoutToken, 1, new QueryTagFilter(Array.Empty<int>(), true, false, "untagged")));
    }

    [Fact]
    public void NoTag_LeavesTheStatementAlone_TokenOrNot()
    {
        Assert.Equal(WithToken, QueryTagReadJoin.Apply(WithToken, 1, QueryTagFilter.None));
        Assert.Equal(WithToken, QueryTagReadJoin.Apply(WithToken, 1, null));
        Assert.Equal(WithoutToken, QueryTagReadJoin.Apply(WithoutToken, 1, QueryTagFilter.None));
    }

    [Fact]
    public void ATagOnAStatementWithAToken_ReplacesTheToken()
    {
        var sql = QueryTagReadJoin.Apply(WithToken, 1, Tagged);
        Assert.DoesNotContain("/*TAG:", sql, StringComparison.Ordinal);
        Assert.Contains("m.tag_id IN (7)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Named_IsTheTagAsGiven_Untagged_OrNullWithNoFilter()
    {
        Assert.Null(QueryTagFilter.None.Named);
        Assert.Equal("Prod", Tagged.Named);
        Assert.Equal("untagged", new QueryTagFilter(Array.Empty<int>(), true, false, "Untagged").Named);
    }
}
