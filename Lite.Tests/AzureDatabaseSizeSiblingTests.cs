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
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
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
///
/// <para>
/// A sibling row reports data space only, and both of its sizes come from that one view: the ALLOCATED data space
/// (<c>allocated_storage_in_megabytes</c>) is <c>total_size_mb</c>, like every other row in the store, and the data
/// space USED (<c>storage_in_megabytes</c>) is <c>used_size_mb</c>. The arm first stored the used figure as the
/// total, so a sibling read 119 MB where its allocation was 10,240 MB and no free space could be worked out.
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

        /* The name has one owner. The growth reads leave the old-shape sibling row out by this same name. */
        Assert.Equal("(whole database)", AzureSiblingDatabaseSize.FileName);
        Assert.DoesNotContain("'", AzureSiblingDatabaseSize.FileName, StringComparison.Ordinal);
    }

    /// <summary>
    /// A sibling row carries the used space as well as the size, because the view reports both. What it
    /// cannot measure stays out of the INSERT, where the table variable defaults it to NULL: a growth step or a
    /// ceiling of 0 would be a measurement nobody took.
    /// </summary>
    [Fact]
    public void TheSiblingInsertWritesTheSizeAndTheUsedSpace_AndOmitsWhatItCannotMeasure()
    {
        /* Sliced from the INSERT's own column list, not from the first parenthesis after the IF — that one
           belongs to DB_NAME(), and the first version of this assertion happily tested the string "(". */
        var insert = AzureSql[AzureSql.IndexOf("IF DB_NAME() = N'master'", StringComparison.Ordinal)..];
        var listStart = insert.IndexOf("@database_sizes", StringComparison.Ordinal);
        var open = insert.IndexOf('(', listStart);
        var columnList = insert[open..insert.IndexOf(')', open)];

        Assert.Contains("used_size_mb", columnList, StringComparison.Ordinal);
        Assert.DoesNotContain("auto_growth_mb", columnList, StringComparison.Ordinal);
        Assert.Contains("total_size_mb", columnList, StringComparison.Ordinal);
    }

    private static string SiblingArm()
    {
        var arm = AzureSql[AzureSql.IndexOf("EXEC sys.sp_executesql", StringComparison.Ordinal)..];
        return Regex.Replace(arm, @"\s+", " ");
    }

    /// <summary>
    /// The mapping itself: the allocated data space is the size and the used data space is the used space, the same
    /// footing as every row the per-file arm writes. The view's two columns are Microsoft Learn's "formatted file
    /// space ... made available for storing database data" and "Maximum storage size ... including database data,
    /// indexes, stored procedures, and metadata". The arm once stored the second as the total.
    /// </summary>
    [Fact]
    public void TheSiblingArmMapsAllocatedToTotal_AndStorageToUsed()
    {
        var arm = SiblingArm();

        Assert.Contains("total_size_mb = CONVERT(decimal(19,2), rs.allocated_storage_in_megabytes)", arm, StringComparison.Ordinal);
        Assert.Contains("used_size_mb = CONVERT(decimal(19,2), rs.storage_in_megabytes)", arm, StringComparison.Ordinal);
        Assert.DoesNotContain("total_size_mb = CONVERT(decimal(19,2), rs.storage_in_megabytes)", arm, StringComparison.Ordinal);

        /* INSERT ... EXEC maps by position, so the projection must list the columns in the INSERT's order. */
        var insertList = Regex.Match(AzureSql, @"IF DB_NAME\(\) = N'master'\s*BEGIN\s*INSERT\s*@database_sizes\s*\(([^)]*)\)").Groups[1].Value;
        var inserted = insertList.Split(',').Select(c => c.Trim()).ToArray();
        Assert.Equal(new[] { "database_name", "file_type_desc", "file_name", "total_size_mb", "used_size_mb", "state_desc" }, inserted);

        var projection = arm[arm.IndexOf("SELECT rs.database_name,", StringComparison.Ordinal)..arm.IndexOf(" FROM ( SELECT", StringComparison.Ordinal)];
        var projected = Regex.Matches(projection, @"(?:^|, )(?:SELECT )?(?:rs\.)?(\w+)(?= =|,|$)").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(inserted, projected);
    }

    /// <summary>
    /// The newest sample where BOTH sizes are known. A sample that has only one of them would give a size without a
    /// used space (or the reverse), and ranking before that filter could pick it over an older complete sample.
    /// Both filters sit in the WHERE of the SELECT that ranks, so they apply before the ranking.
    /// </summary>
    [Fact]
    public void TheSiblingArmTakesTheNewestSampleWhereBothSizesAreKnown()
    {
        var arm = SiblingArm();
        var ranking = arm[arm.IndexOf("FROM sys.resource_stats AS r", StringComparison.Ordinal)..arm.IndexOf(") AS rs", StringComparison.Ordinal)];

        Assert.Contains("r.storage_in_megabytes IS NOT NULL", ranking, StringComparison.Ordinal);
        Assert.Contains("r.allocated_storage_in_megabytes IS NOT NULL", ranking, StringComparison.Ordinal);
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
    /// #3262: the seam every test above leaves untested. They pin the SQL that EMITS a sibling row —
    /// including the NULLs at database_id, file_id and physical_name — and <c>ReadAsync</c> read
    /// exactly those three ordinals unguarded, so the two halves were each correct and had never been
    /// run against each other. In the field the first sibling row arrived when
    /// <c>sys.resource_stats</c> finished ingesting (about an hour after database creation, which is
    /// why every fresh-server validation window missed it), and the cast threw
    /// "Object cannot be cast from DBNull to other types." — aborting the whole read, master's own
    /// file rows included, permanently.
    ///
    /// <para>The reader is driven over the arm's documented shape: a real file row for the connected
    /// database first (the ORDER BY puts sibling rows last), then a sibling row that carries what the arm
    /// can measure: its allocated size, its used space, and nothing else. Every row must come back, and the
    /// sibling's absent measurements must arrive as nulls rather than exceptions. A third row has the shape
    /// every stored sibling row had before the sizes were mapped to allocated and used (the used space
    /// empty): history in the store, and the same shape a real file takes when its used-space probe fails,
    /// so the reader still has to read it.</para>
    /// </summary>
    [Fact]
    public async Task TheReaderSurvivesTheSiblingRow_TheArmDeliberatelyEmits()
    {
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "master", 1, 1, "ROWS", "data_0", "data_0.mdf", 4.00m, 2.63m,
                16.00m, 2097152m, "FULL", 160, "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value,
                0, DBNull.Value, DBNull.Value,
            },
            new object[]
            {
                "testdb1", DBNull.Value, DBNull.Value, "ROWS", "(whole database)", DBNull.Value,
                10240.00m, 119.00m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
            },
            new object[]
            {
                "testdb2", DBNull.Value, DBNull.Value, "ROWS", "(whole database)", DBNull.Value,
                23.00m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
            });

        var rows = await DatabaseSizeStatsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(new RecordingCollectorDeltaCalculator()), CancellationToken.None);

        Assert.Equal(3, rows.Count);

        /* The connected database's real file row is untouched by the guards. */
        Assert.Equal("master", rows[0].DatabaseName);
        Assert.Equal(1, rows[0].DatabaseId);
        Assert.Equal(1, rows[0].FileId);
        Assert.Equal("data_0.mdf", rows[0].PhysicalName);

        /* The sibling row survives, and what the arm could not measure reads as null. */
        Assert.Equal("testdb1", rows[1].DatabaseName);
        Assert.Null(rows[1].DatabaseId);
        Assert.Null(rows[1].FileId);
        Assert.Null(rows[1].PhysicalName);
        Assert.Equal("(whole database)", rows[1].FileName);
        Assert.Equal(10240.00m, rows[1].TotalSizeMb);
        Assert.Equal(119.00m, rows[1].UsedSizeMb);
        Assert.Null(rows[1].AutoGrowthMb);
        Assert.Equal("ONLINE", rows[1].StateDesc);

        /* The row in the old shape still reads: its size, and no used space. */
        Assert.Equal("testdb2", rows[2].DatabaseName);
        Assert.Equal(23.00m, rows[2].TotalSizeMb);
        Assert.Null(rows[2].UsedSizeMb);
    }

    /// <summary>
    /// The other half of the seam: the sibling row must also make it back OUT of the definition. The
    /// positional writer contract (one value per declared payload column, nulls included) is what the
    /// stores' appender / binary COPY adapters rest on, and both store schemas hold these three
    /// columns nullable — so the row's nulls must be written as nulls, not skipped and not defaulted.
    /// </summary>
    [Fact]
    public async Task TheSiblingRowWritesItsNulls_ThroughThePositionalContract()
    {
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "testdb1", DBNull.Value, DBNull.Value, "ROWS", "(whole database)", DBNull.Value,
                10240.00m, 119.00m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
            },
            new object[]
            {
                "testdb2", DBNull.Value, DBNull.Value, "ROWS", "(whole database)", DBNull.Value,
                23.00m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
            });

        var deltas = new RecordingCollectorDeltaCalculator();
        var rows = await DatabaseSizeStatsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(deltas), CancellationToken.None);
        Assert.Equal(2, rows.Count);

        var writer = new RecordingCollectorRowWriter();
        DatabaseSizeStatsCollector.Instance.WritePayload(rows[0], writer, CollectorTestContext.Make(deltas));

        Assert.Equal(DatabaseSizeStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal("testdb1", writer.Values[0]);
        Assert.Null(writer.Values[1]);    /* database_id */
        Assert.Null(writer.Values[2]);    /* file_id */
        Assert.Equal("(whole database)", writer.Values[4]);
        Assert.Null(writer.Values[5]);    /* physical_name */
        Assert.Equal(10240.00m, writer.Values[6]);    /* total_size_mb: the allocated data space */
        Assert.Equal(119.00m, writer.Values[7]);      /* used_size_mb: the data space used */

        /* A row in the old shape is written with its used space as a null, not skipped and not defaulted to 0. */
        var oldShape = new RecordingCollectorRowWriter();
        DatabaseSizeStatsCollector.Instance.WritePayload(rows[1], oldShape, CollectorTestContext.Make(deltas));
        Assert.Equal(23.00m, oldShape.Values[6]);
        Assert.Null(oldShape.Values[7]);
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
