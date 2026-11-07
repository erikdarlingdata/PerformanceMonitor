/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>#7643: the catalog read batches of the schema capture. They are pure text builders; the live behaviour is in SchemaCatalogLiveSqlTests.</summary>
public sealed class SchemaCatalogQueriesTests
{
    [Fact]
    public void BuildTargetedRead_InlinesOnlyTheGivenIds()
    {
        var sql = SchemaCatalogQueries.BuildTargetedRead([101, 7, 101, 3000000]);

        Assert.Contains("IN (7,101,3000000)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("TOP (10001)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildTargetedRead_RefusesMoreThanMaxTargetedIds()
    {
        var ids = Enumerable.Range(1, SchemaCatalogQueries.MaxTargetedIds + 1).ToList();

        Assert.Throws<ArgumentOutOfRangeException>(() => SchemaCatalogQueries.BuildTargetedRead(ids));
        Assert.Contains("IN (", SchemaCatalogQueries.BuildTargetedRead(ids.Take(SchemaCatalogQueries.MaxTargetedIds).ToList()), StringComparison.Ordinal);
    }

    [Fact]
    public void BuildFullRead_HasNoStringAggOrStringSplit()
    {
        var sql = SchemaCatalogQueries.BuildFullRead();

        Assert.DoesNotContain("STRING_AGG", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("STRING_SPLIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("OPENJSON", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("TOP (10001)", sql, StringComparison.Ordinal);
    }
}
