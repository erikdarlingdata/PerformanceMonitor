/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Another database on an Azure SQL Database server has one stored row, named <c>(whole database)</c>. Rows stored
/// before the allocated/used fix hold the database's USED space in <c>total_size_mb</c> and nothing in
/// <c>used_size_mb</c>. Rows stored after it hold the ALLOCATED size there and the used space beside it. At upgrade a
/// Hyperscale database that did not grow goes from 119 MB to 10,240 MB in the store, and a growth read that set the two
/// rows side by side would report a 10,121 MB rise.
///
/// <para>A growth read leaves the old-shape rows out, at read time, so the database reads like one added inside the
/// window: a blank past size and growth 0. This store is PostgreSQL, whose tests are live-only, so here the text is the
/// proof. Lite's <c>AzureSiblingGrowthTests</c> run the twin query and the same predicate on a real DuckDB store, with
/// the upgrade history seeded.</para>
/// </summary>
public sealed class AzureSiblingGrowthTests
{
    private static string Squash(string sql) => Regex.Replace(sql, @"\s+", " ").Trim();

    /// <summary>The body of one CTE of a WITH statement: the text inside its <c>name AS ( ... )</c>.</summary>
    private static string CteBody(string sql, string name)
    {
        var head = "\n" + name + " AS (";
        var start = sql.IndexOf(head, StringComparison.Ordinal);
        if (start < 0)
        {
            head = "WITH " + name + " AS (";
            start = sql.IndexOf(head, StringComparison.Ordinal);
        }

        Assert.True(start >= 0, $"The SQL has no CTE named {name}.");
        var open = start + head.Length - 1;
        var depth = 0;
        for (var i = open; i < sql.Length; i++)
        {
            if (sql[i] == '(')
            {
                depth++;
            }
            else if (sql[i] == ')' && --depth == 0)
            {
                return sql.Substring(open + 1, i - open - 1);
            }
        }

        throw new InvalidOperationException($"The CTE {name} is never closed.");
    }

    [Fact]
    public void ViewerStorageGrowth_LeavesTheOldShapeRowsOutOfTheLatestAnd7dAnd30dSums()
    {
        var sql = ViewerDataService.StorageGrowthSql;

        foreach (var sum in new[] { "latest", "past_7d", "past_30d" })
        {
            Assert.True(
                Squash(CteBody(sql, sum)).Contains(AzureSiblingDatabaseSize.ExcludePreFixRows, StringComparison.Ordinal),
                $"The {sum} sum does not leave the old-shape sibling rows out.");
        }
    }

    /// <summary>
    /// An old-shape row is one with no file id, the sibling name and no used space, all three. A real file whose
    /// used-space probe failed also has no used space and must keep counting, and a row with no file name must never
    /// drop out of a WHERE, so the name test reads through COALESCE. Lite's twin pin runs this clause over literal
    /// rows on DuckDB.
    /// </summary>
    [Fact]
    public void ExcludePreFixRows_NeedsAllThreeConditions_AndIsNullSafeOnTheFileName()
    {
        Assert.Equal(
            "NOT (file_id IS NULL AND COALESCE(file_name, '') = '(whole database)' AND used_size_mb IS NULL)",
            AzureSiblingDatabaseSize.ExcludePreFixRows);
    }
}
