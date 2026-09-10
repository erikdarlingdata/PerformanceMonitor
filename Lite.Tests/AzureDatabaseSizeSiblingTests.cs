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
    /// database first (the ORDER BY puts sibling rows last), then a sibling row that carries only
    /// what the arm can measure. Both rows must come back, and the sibling's absent measurements
    /// must arrive as nulls rather than exceptions.</para>
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
                23.00m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
            });

        var rows = await DatabaseSizeStatsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(new RecordingCollectorDeltaCalculator()), CancellationToken.None);

        Assert.Equal(2, rows.Count);

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
        Assert.Equal(23.00m, rows[1].TotalSizeMb);
        Assert.Null(rows[1].UsedSizeMb);
        Assert.Equal("ONLINE", rows[1].StateDesc);
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
                23.00m, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
            });

        var deltas = new RecordingCollectorDeltaCalculator();
        var rows = await DatabaseSizeStatsCollector.Instance.ReadAsync(
            reader, CollectorTestContext.Make(deltas), CancellationToken.None);

        var writer = new RecordingCollectorRowWriter();
        DatabaseSizeStatsCollector.Instance.WritePayload(Assert.Single(rows), writer, CollectorTestContext.Make(deltas));

        Assert.Equal(DatabaseSizeStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal("testdb1", writer.Values[0]);
        Assert.Null(writer.Values[1]);    /* database_id */
        Assert.Null(writer.Values[2]);    /* file_id */
        Assert.Equal("(whole database)", writer.Values[4]);
        Assert.Null(writer.Values[5]);    /* physical_name */
        Assert.Equal(23.00m, writer.Values[6]);
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
