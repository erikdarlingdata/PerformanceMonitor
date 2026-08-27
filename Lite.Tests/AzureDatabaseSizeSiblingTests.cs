/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2643: on Azure SQL DB this collector reported the connected database and nothing else.
///
/// <para>
/// Correct — <c>sys.database_files</c> is database-scoped — and indistinguishable from a collector that
/// managed to find only <c>master</c>. A reporter with fifty databases pointed the Viewer at <c>master</c>,
/// saw <c>master</c>'s two files on a grid headed "All Servers", and filed it. I told them the platform
/// made anything else impossible. It does not: <c>sys.resource_stats</c> is a master-only view carrying
/// <c>storage_in_megabytes</c> per database, verified against a live Azure SQL Database.
/// </para>
/// </summary>
public class AzureDatabaseSizeSiblingTests
{
    private static string AzureSql =>
        (string)typeof(DatabaseSizeStatsCollector)
            .GetField("AzureSqlDbQueryText", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

    [Fact]
    public void TheAzureQueryReadsBothTheConnectedDatabaseAndItsSiblings()
    {
        Assert.Contains("sys.database_files", AzureSql, StringComparison.Ordinal);
        Assert.Contains("sys.resource_stats", AzureSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The sibling read goes through <c>sp_executesql</c>, and that is the load-bearing detail.
    ///
    /// <para><c>sys.resource_stats</c> does not EXIST in a user database, and SQL Server resolves names at
    /// PARSE time — so a plain <c>UNION</c> guarded by <c>WHERE DB_NAME() = N'master'</c> still fails with
    /// <b>error 208 on every user database</b>, which is the common case. That is not a theory: the first
    /// version of this shipped exactly that shape, and running it from a user database returned 208
    /// immediately. Deferring the reference until the branch runs is the only thing that fixes it.</para>
    /// </summary>
    [Fact]
    public void TheSiblingReadIsDeferred_BecauseTheViewDoesNotExistInAUserDatabase()
    {
        Assert.Contains("IF DB_NAME() = N'master'", AzureSql, StringComparison.Ordinal);
        Assert.Contains("EXEC sys.sp_executesql", AzureSql, StringComparison.Ordinal);

        /* The reference must be INSIDE the deferred string, not in the outer batch where parsing reaches
           it regardless of the branch. */
        var execIndex = AzureSql.IndexOf("EXEC sys.sp_executesql", StringComparison.Ordinal);
        var viewIndex = AzureSql.IndexOf("sys.resource_stats", StringComparison.Ordinal);

        Assert.True(viewIndex > execIndex,
            "sys.resource_stats is referenced in the outer batch — parsing reaches it on a user database and fails 208 before any guard runs.");
    }

    /// <summary>
    /// A sibling row is honest about being a database rather than a file. <c>sys.resource_stats</c> has no
    /// per-file breakdown, so the row says so: a NULL <c>file_id</c> and a name that reads as a database.
    /// A fabricated file name would make the grid look complete and be wrong.
    /// </summary>
    [Fact]
    public void ASiblingRowIsLabelledAsAWholeDatabase_NotAFabricatedFile()
    {
        Assert.Contains("file_name = N''(whole database)''", AzureSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>used_size_mb</c> is not projected for a sibling, and the omission is the point: the table
    /// variable defaults it to NULL. Zero would say the database is empty, which is a measurement nobody
    /// took.
    /// </summary>
    [Fact]
    public void TheSiblingInsertOmitsWhatItCannotMeasure()
    {
        /* Sliced from the INSERT's own column list, not from the first parenthesis after the IF — that one
           belongs to DB_NAME(), and the first version of this assertion happily tested the string "(". */
        var insert = AzureSql[AzureSql.IndexOf("IF DB_NAME() = N'master'", StringComparison.Ordinal)..];
        var listStart = insert.IndexOf("@database_sizes", StringComparison.Ordinal);
        var open = insert.IndexOf('(', listStart);
        var columnList = insert[open..insert.IndexOf(')', open)];

        Assert.DoesNotContain("used_size_mb", columnList, StringComparison.Ordinal);
        Assert.DoesNotContain("auto_growth_mb", columnList, StringComparison.Ordinal);
        Assert.Contains("total_size_mb", columnList, StringComparison.Ordinal);
    }

    /// <summary>
    /// The connected database is excluded from the sibling arm — the file arm already reported it, with
    /// real files. Without this every Azure entry reports its own database twice, once properly and once
    /// as a sizeless "(whole database)" row.
    /// </summary>
    [Fact]
    public void TheConnectedDatabaseIsNotReportedTwice()
        => Assert.Contains("r.database_name <> DB_NAME()", AzureSql, StringComparison.Ordinal);

    /// <summary>
    /// Newest sample per database. <c>sys.resource_stats</c> keeps roughly fourteen days at five-minute
    /// grain, so without this every database arrives a few thousand times.
    /// </summary>
    [Fact]
    public void OnlyTheNewestSamplePerDatabaseIsTaken()
    {
        Assert.Contains("ROW_NUMBER() OVER (PARTITION BY r.database_name ORDER BY r.end_time DESC)", AzureSql, StringComparison.Ordinal);
        Assert.Contains("WHERE rs.rn = 1", AzureSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The final projection must still match <c>PayloadColumns</c> exactly — the collector writes by
    /// position, and a table variable makes it easy to reorder one and not the other.
    /// </summary>
    [Fact]
    public void TheFinalProjectionMatchesThePayloadColumnsInOrder()
    {
        var final = AzureSql[AzureSql.LastIndexOf("FROM @database_sizes", StringComparison.Ordinal)..];
        var select = AzureSql[..AzureSql.LastIndexOf("FROM @database_sizes", StringComparison.Ordinal)];
        select = select[select.LastIndexOf("SELECT", StringComparison.Ordinal)..];

        var projected = Regex.Matches(select, @"ds\.(\w+)").Select(m => m.Groups[1].Value).ToArray();
        var declared = DatabaseSizeStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray();

        Assert.Equal(declared, projected);
        Assert.NotEmpty(final);
    }
}
