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
using PerformanceMonitor.Darling.Service;
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
    /// A sibling's size is data space only, so the <c>latest</c> CTE flags a database that has the one row another database
    /// gets, with a <c>bool_or</c> of the shared row test. Lite's <c>AzureSiblingGrowthTests</c> run the twin query on a
    /// real DuckDB store. This store is PostgreSQL, whose tests are live-only, so here the text is the proof.
    /// </summary>
    [Fact]
    public void ViewerStorageGrowth_FlagsASibling_WithABoolOrOfTheSharedRowTest_InTheLatestCte()
    {
        var latest = Squash(CteBody(ViewerDataService.StorageGrowthSql, "latest"));

        Assert.Contains("bool_or(" + AzureSiblingDatabaseSize.RowPredicate + ") AS has_sibling_row", latest, StringComparison.Ordinal);
    }

    /// <summary>
    /// A Hyperscale database's size is data space only when its log file has no size. The <c>latest</c> CTE flags a
    /// database that has a row in <c>log_service_files</c>, the CTE the read already has, which binds the same <c>$2</c>
    /// snapshot, so the #4245 plan shape holds. An <c>EXISTS</c> is two-valued, so a NULL name cannot make the flag unknown.
    /// </summary>
    [Fact]
    public void ViewerStorageGrowth_FlagsALogServiceDatabase_WithAnExistsOnTheLogServiceCte_InTheLatestCte()
    {
        var latest = Squash(CteBody(ViewerDataService.StorageGrowthSql, "latest"));

        Assert.Contains(
            "EXISTS ( SELECT 1 FROM log_service_files AS ls WHERE ls.database_name = s.database_name ) AS has_log_service_file",
            latest,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The two flags are the last two columns of the read, after every older column, so no ordinal moves: the final
    /// <c>SELECT</c> ends with them and the loader reads them at 8 and 9.
    /// </summary>
    [Fact]
    public void ViewerStorageGrowth_ReadsTheTwoFlagsLast_SoNoOtherColumnMoves()
    {
        Assert.Contains(
            "growth_pct_30d, l.has_sibling_row, l.has_log_service_file FROM latest l",
            Squash(ViewerDataService.StorageGrowthSql),
            StringComparison.Ordinal);

        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Storage.cs");
        Assert.Contains("GrowthPct30d = reader.IsDBNull(7) ? 0m : Convert.ToDecimal(reader.GetValue(7)),", source, StringComparison.Ordinal);
        Assert.Contains("HasSiblingRow = !reader.IsDBNull(8) && reader.GetBoolean(8)", source, StringComparison.Ordinal);
        Assert.Contains("HasLogServiceFile = !reader.IsDBNull(9) && reader.GetBoolean(9)", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The shared row test is the file id and the file name, nothing else, and it is NULL-safe on the name: a real file
    /// has a file id, so it is never taken for a sibling. The flag and the old-shape test both splice it in.
    /// </summary>
    [Fact]
    public void RowPredicate_IsTheFileIdAndTheFileName_AndIsNullSafeOnTheFileName()
    {
        Assert.Equal("file_id IS NULL AND COALESCE(file_name, '') = '(whole database)'", AzureSiblingDatabaseSize.RowPredicate);
        Assert.Equal(AzureSiblingDatabaseSize.RowPredicate + " AND used_size_mb IS NULL", AzureSiblingDatabaseSize.PreFixRowPredicate);
    }

    /// <summary>
    /// The file-growth alert's read takes the newest row and the oldest row of a window, one CTE each. Both leave the
    /// old-shape rows out: with only one of them filtered, the newest row (10,240 MB, the allocation) would be set
    /// against an old-shape baseline (119 MB, the used space) and read as a rise.
    /// </summary>
    [Fact]
    public void DatabaseFileGrowthSql_LeavesTheOldShapeRowsOutOfBothCtes()
    {
        var sql = DarlingAlertReadAdapter.DatabaseFileGrowthSql;

        foreach (var cte in new[] { "current_files", "baseline" })
        {
            Assert.True(
                Squash(CteBody(sql, cte)).Contains(AzureSiblingDatabaseSize.ExcludePreFixRows, StringComparison.Ordinal),
                $"The {cte} CTE does not leave the old-shape sibling rows out.");
        }
    }

    /// <summary>
    /// A Hyperscale sibling row holds 10,240 MB allocated and 119 MB used. Its free space is the difference and its used
    /// share rounds to one place. It is a data row (<c>ROWS</c>, no file id), so it is not read as a log file and it adds
    /// its allocation and free space to the sums the health score takes over the latest snapshot.
    /// </summary>
    [Fact]
    public void ViewerRow_AHyperscaleSibling_ReadsItsFreeSpace_AndAddsToTheFreeSpaceSums()
    {
        var sibling = new DatabaseSizeRow
        {
            DatabaseName = "sibdb", FileTypeDesc = "ROWS", FileName = AzureSiblingDatabaseSize.FileName,
            TotalSizeMb = 10_240m, UsedSizeMb = 119m,
        };
        var real = new DatabaseSizeRow
        {
            DatabaseName = "realdb", FileTypeDesc = "ROWS", FileName = "realdb_data", TotalSizeMb = 100m, UsedSizeMb = 10m,
        };

        Assert.Equal(10_121m, sibling.FreeSpaceMb);
        Assert.Equal(1.2m, sibling.UsedPct);

        Assert.Equal(10_240m, DatabaseSizeRow.AllocatedTotalMb(new[] { sibling }));
        Assert.Equal(10_121m, DatabaseSizeRow.FreeTotalMb(new[] { sibling }));
        Assert.Equal(10_340m, DatabaseSizeRow.AllocatedTotalMb(new[] { sibling, real }));
        Assert.Equal(10_211m, DatabaseSizeRow.FreeTotalMb(new[] { sibling, real }));
    }

    /// <summary>The health score's free-space sums sit in the FinOps tab's code-behind, which a test cannot reach. They
    /// are the two row helpers the test above pins, called on the latest snapshot, so this holds the call by source.</summary>
    [Fact]
    public void FinOpsHealthScore_TakesItsFreeSpaceSums_FromTheTwoRowHelpers()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");

        Assert.Contains("var totalStorageMb = DatabaseSizeRow.AllocatedTotalMb(dbSizes);", source, StringComparison.Ordinal);
        Assert.Contains("var totalFreeMb = DatabaseSizeRow.FreeTotalMb(dbSizes);", source, StringComparison.Ordinal);
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
