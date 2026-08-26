/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2636, reported from the field: <c>get_index_usage</c> returned 200 rows, all "Unused", from a handful of
/// databases — and zero rows for the database the reporter actually asked about, whose collection was
/// healthy with full retention and no errors.
///
/// <para>
/// Nothing was broken in collection. Two decisions combined into a third nobody made: the ordering puts
/// unused indexes ahead of everything SERVER-WIDE, and the cap was a hardcoded 200 the caller could not
/// change or scope. On an instance with 200+ unused indexes in one legacy database, that database consumes
/// the entire answer and every Active index everywhere else is invisible.
/// </para>
///
/// <para>
/// The reporter's own words are the reason this is a bug and not a limit: an operator "can easily conclude
/// collection is broken, stale, or scoped to exclude that database — when in fact the data exists but was
/// truncated out". A capped answer that cannot say it was capped is indistinguishable from an empty one.
/// </para>
/// </summary>
public sealed class IndexUsageTruncationTests
{
    private static string ReaderSql => DarlingObjectStatsReaderSource.IndexUsageSql;

    /// <summary>
    /// The filter, which is what makes the reporter's question answerable at all: before this there was no
    /// way to ask about one database, so the only way to see a quiet database's indexes was to hope no
    /// louder one outranked it.
    /// </summary>
    [Fact]
    public void TheQueryTakesADatabaseFilter_AndNullMeansEveryDatabase()
    {
        Assert.Contains("($2::text IS NULL OR database_name = $2::text)", ReaderSql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $3", ReaderSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The count is a SEPARATE statement, and that is the load-bearing detail.
    ///
    /// <para><c>COUNT(*) OVER ()</c> inside the capped query would return the count of rows that survived the
    /// LIMIT — a number that always equals what was returned, reported as though it were the total. That is
    /// the exact mistake this whole issue is about, reimplemented one layer down.</para>
    /// </summary>
    [Fact]
    public void TheMatchCountIsTakenBeforeTheCap_NotOverTheReturnedRows()
    {
        var countSql = DarlingObjectStatsReaderSource.IndexUsageMatchCountSql;

        Assert.Contains("SELECT count(*)", countSql, StringComparison.Ordinal);
        Assert.Contains("($2::text IS NULL OR database_name = $2::text)", countSql, StringComparison.Ordinal);

        /* No cap on the count, or it would count the page again. */
        Assert.DoesNotMatch(new Regex(@"\bLIMIT\b", RegexOptions.IgnoreCase), countSql);
        Assert.DoesNotContain("OVER ()", countSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The count query has to apply the SAME filter as the rows, or the ratio it feeds is between two
    /// different questions and "truncated" becomes noise on every call.
    /// </summary>
    [Fact]
    public void TheCountAndTheRowsShareTheirFilter()
    {
        foreach (var clause in new[]
        {
            "WHERE server_id = $1",
            "collection_time = (SELECT MAX(collection_time) FROM v_index_object_stats WHERE server_id = $1)",
            "($2::text IS NULL OR database_name = $2::text)",
        })
        {
            Assert.Contains(clause, ReaderSql, StringComparison.Ordinal);
            Assert.Contains(clause, DarlingObjectStatsReaderSource.IndexUsageMatchCountSql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The unused-first ordering STAYS. It is right for the question the tool was built for — which indexes
    /// can I drop — and the defect was never the ordering on its own. Changing it would trade the reporter's
    /// problem for its mirror image: every drop candidate pushed off the end by active indexes.
    /// </summary>
    [Fact]
    public void UnusedStillSortsFirst_BecauseTheOrderingWasNotTheDefect()
    {
        Assert.Contains(
            "CASE WHEN COALESCE(user_seeks, 0) + COALESCE(user_scans, 0) + COALESCE(user_lookups, 0) = 0 THEN 0 ELSE 1 END",
            ReaderSql,
            StringComparison.Ordinal);
    }
}

/// <summary>Reaches the internal reader's public SQL constants from the test assembly.</summary>
internal static class DarlingObjectStatsReaderSource
{
    private static readonly Type s_reader =
        typeof(DarlingCommandExecutor).Assembly.GetType("PerformanceMonitor.Darling.Service.Mcp.DarlingObjectStatsReader")
        ?? throw new InvalidOperationException("DarlingObjectStatsReader was not found — this pin needs re-anchoring.");

    public static string IndexUsageSql => Field(nameof(IndexUsageSql));

    public static string IndexUsageMatchCountSql => Field(nameof(IndexUsageMatchCountSql));

    private static string Field(string name)
        => s_reader.GetField(name)?.GetValue(null) as string
           ?? throw new InvalidOperationException($"{name} was not found on DarlingObjectStatsReader.");
}
