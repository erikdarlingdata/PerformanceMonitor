/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the parity contract of the extracted file_io_stats definition: the two query variants,
/// the exclusion-filter splice, per-database execution on Azure, and the eight
/// "{database}|{file}" delta groups.
/// </summary>
public sealed class FileIoCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    [Fact]
    public void BuildQuery_OnPrem_SplicesExclusionFilter_WithParameters()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = DateTime.UtcNow,
            Deltas = s_deltas,
            ExcludedDatabases = new[] { "AdventureWorks", "StackOverflow" },
        });

        Assert.Contains("AND d.name NOT IN (@excl_db_0, @excl_db_1)", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.master_files", plan.Text, StringComparison.Ordinal);
        Assert.Equal(2, plan.Parameters.Count);
        Assert.Equal(("@excl_db_0", (object?)"AdventureWorks", CollectorParameterType.NVarChar128),
            (plan.Parameters[0].Name, plan.Parameters[0].Value, plan.Parameters[0].Type));
    }

    [Fact]
    public void BuildQuery_OnPrem_NoExclusions_RemovesToken_NoParameters()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.DoesNotContain("/*EXCLUSION_FILTER*/", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@excl_db_", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
    }

    [Fact]
    public void BuildQuery_Azure_ScopesToConnectedDatabase_AndRunsPerDatabase()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Contains("sys.dm_io_virtual_file_stats(DB_ID(), NULL)", plan.Text, StringComparison.Ordinal);
        Assert.Contains("sys.database_files", plan.Text, StringComparison.Ordinal);
        Assert.Empty(plan.Parameters);
        Assert.True(FileIoStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.False(FileIoStatsCollector.Instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = false }));
    }

    /// <summary>
    /// On a Hyperscale database, <c>sys.dm_io_virtual_file_stats.size_on_disk_bytes</c> read about 0.1 MB for the data
    /// file and for the log file, so the file size was wrong in both apps. <c>sys.database_files.size</c> (8-KB pages)
    /// is correct there and is what Database Sizes reads, so the Azure SQL Database query takes <c>size_mb</c> from it.
    /// The DMV's number stays as the fallback for a file the join misses: a NULL would be stored as 0, and the size
    /// facts skip rows where <c>size_mb</c> is 0 or less.
    /// </summary>
    [Fact]
    public void BuildQuery_Azure_SizeComesFromDatabaseFiles_WithTheDmvAsFallback()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        /* database_files is the first COALESCE operand, so it wins whenever the join matched, and the DMV's bytes
           are read only when it did not. The CONVERT keeps the payload column at decimal(18,2). Every row the
           Hyperscale log rule below does not take ends here. */
        Assert.EndsWith(
            "ELSE CONVERT(decimal(18,2), COALESCE(df.size * 8.0 / 1024.0, vfs.size_on_disk_bytes / 1048576.0)) END",
            SizeMbProjection(plan.Text),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// On a Hyperscale database the LOG file lives in the log service. Its <c>sys.database_files.size</c> is not
    /// storage the database holds, and neither is the DMV's number, so that row carries NO size: NULL, stored as
    /// NULL. Only a log file (<c>type = 1</c>) on a database whose <c>Edition</c> is Hyperscale takes this branch. The
    /// Hyperscale data file, and every file on any other Azure SQL Database tier, keeps the size from
    /// <c>sys.database_files</c>.
    /// </summary>
    [Fact]
    public void BuildQuery_Azure_HyperscaleLogRow_CarriesNoSize_ButEveryOtherRowKeepsIt()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));

        Assert.Equal(
            "CASE WHEN df.type = 1 /*LOG*/ AND CONVERT(nvarchar(64), DATABASEPROPERTYEX(DB_NAME(), N'Edition')) = N'Hyperscale' "
            + "THEN CONVERT(decimal(18,2), NULL) "
            + "ELSE CONVERT(decimal(18,2), COALESCE(df.size * 8.0 / 1024.0, vfs.size_on_disk_bytes / 1048576.0)) END",
            SizeMbProjection(plan.Text));
    }

    /// <summary>
    /// The Azure SQL Database size change must not reach the on-prem / Managed Instance query: there the DMV's
    /// <c>size_on_disk_bytes</c> is the size, and the query has no <c>sys.database_files</c> join to read one from.
    /// </summary>
    [Fact]
    public void BuildQuery_OnPrem_SizeStaysOnTheIoStatsDmv()
    {
        var plan = FileIoStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.Equal("CONVERT(decimal(18,2), vfs.size_on_disk_bytes / 1048576.0)", SizeMbProjection(plan.Text));
        Assert.DoesNotContain("sys.database_files", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("df.size", plan.Text, StringComparison.Ordinal);
        /* The Hyperscale log rule is an Azure SQL Database rule; SQL Server and Managed Instance have no such tier. */
        Assert.DoesNotContain("Hyperscale", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("DATABASEPROPERTYEX", plan.Text, StringComparison.Ordinal);
    }

    /// <summary>The expression assigned to <c>size_mb</c> in the select list, with whitespace collapsed.</summary>
    private static string SizeMbProjection(string sql)
    {
        var match = Regex.Match(sql, @"\bsize_mb\s*=\s*(?<expr>.+?),\s*num_of_reads\s*=", RegexOptions.Singleline);
        Assert.True(match.Success, "the select list has a size_mb column followed by num_of_reads");
        return Regex.Replace(match.Groups["expr"].Value, @"\s+", " ").Trim();
    }

    [Fact]
    public void PayloadColumns_MatchSchemaOrder()
    {
        Assert.Equal(
            new[]
            {
                "database_name", "file_name", "file_type", "physical_name", "size_mb",
                "num_of_reads", "num_of_writes", "read_bytes", "write_bytes",
                "io_stall_read_ms", "io_stall_write_ms", "io_stall_queued_read_ms", "io_stall_queued_write_ms",
                "delta_reads", "delta_writes", "delta_read_bytes", "delta_write_bytes",
                "delta_stall_read_ms", "delta_stall_write_ms", "delta_stall_queued_read_ms", "delta_stall_queued_write_ms",
                "sample_interval_seconds",
            },
            FileIoStatsCollector.Instance.PayloadColumns.Select(c => c.Name).ToArray());

        /* #3540: the interval is the TRAILING column, INTEGER like perfmon_stats' and query_stats'. */
        Assert.Equal(CollectorColumnType.Integer, FileIoStatsCollector.Instance.PayloadColumns[^1].Type);
    }

    [Fact]
    public async Task ReadAsync_NullRow_MapsDefaults()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value });

        var rows = await FileIoStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("Unknown", row.DatabaseName);
        Assert.Equal("", row.PhysicalName);
        Assert.Equal(0L, row.NumOfReads);
    }

    /// <summary>
    /// A row with no size stays a row with no size. The Hyperscale log file comes back from the query with a NULL
    /// <c>size_mb</c>; mapping that to 0 here would store a confident "0 MB", and the size facts would drop the
    /// row for a reason that has nothing to do with the file. A file that has a size keeps it.
    /// </summary>
    [Fact]
    public async Task ReadAsync_NullSize_StaysNull_AndASizeIsKept()
    {
        using var reader = new FakeCollectorDataReader(
            new object[] { "SO", "SO_log", "LOG", @"D:\so.ldf", DBNull.Value, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 2 },
            new object[] { "SO", "SO_data", "ROWS", @"D:\so.mdf", 112.04m, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 1 });

        var rows = await FileIoStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Null(rows[0].SizeMb);
        Assert.Equal(112.04m, rows[1].SizeMb);
    }

    /// <summary>
    /// The writer is handed NULL for a row with no size, not 0, so both stores keep the column NULL. A row with a
    /// size is written as that size.
    /// </summary>
    [Fact]
    public void WritePayload_NullSize_IsWrittenAsNull_AndASizeIsWrittenAsTheSize()
    {
        var context = CollectorTestContext.Make(new RecordingCollectorDeltaCalculator());
        var noSizeWriter = new RecordingCollectorRowWriter();
        var sizeWriter = new RecordingCollectorRowWriter();

        FileIoStatsCollector.Instance.WritePayload(
            new FileIoStatsCollector.Row("SO", "SO_log", "LOG", @"D:\so.ldf", null, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 2),
            noSizeWriter, context);
        FileIoStatsCollector.Instance.WritePayload(
            new FileIoStatsCollector.Row("SO", "SO_data", "ROWS", @"D:\so.mdf", 112.04m, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 1),
            sizeWriter, context);

        Assert.Equal(22, noSizeWriter.Values.Count);
        Assert.Null(noSizeWriter.Values[4]);
        Assert.Equal(112.04m, sizeWriter.Values[4]);
    }

    [Fact]
    public void WritePayload_EmitsSchemaOrder_AndPinsDeltaKeyAndGroups()
    {
        var deltas = new RecordingCollectorDeltaCalculator();
        var writer = new RecordingCollectorRowWriter();
        var context = CollectorTestContext.Make(deltas);
        var row = new FileIoStatsCollector.Row("SO", "SO_data", "ROWS", @"D:\so.mdf", 100.5m, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 1);

        FileIoStatsCollector.Instance.WritePayload(row, writer, context);

        Assert.Equal(22, writer.Values.Count);
        Assert.Equal("SO", writer.Values[0]);
        Assert.Equal(8L, writer.Values[12]);
        Assert.Equal(10L, writer.Values[13]);   /* delta_reads = 1 * 10 */
        Assert.Equal(80L, writer.Values[20]);   /* delta_stall_queued_write_ms = 8 * 10 */
        Assert.Equal(0, writer.Values[21]);     /* sample_interval_seconds (#3540): the fake reports 0, the unknowable marker */

        Assert.Equal(8, deltas.Calls.Count);
        Assert.All(deltas.Calls, c => Assert.Equal("SO|SO_data", c.Key));
        Assert.All(deltas.Calls, c => Assert.Equal(CollectorDeltaCalculator.DefaultMaxGapSeconds, c.MaxGap));
        Assert.Equal(
            new[]
            {
                "file_io_reads", "file_io_writes", "file_io_read_bytes", "file_io_write_bytes",
                "file_io_stall_read", "file_io_stall_write", "file_io_stall_queued_read", "file_io_stall_queued_write",
            },
            deltas.Calls.Select(c => c.Group).ToArray());
    }

    /// <summary>
    /// #3540: the measured interval reaches the payload, and it is the MINIMUM over the row's eight groups —
    /// one unknowable group marks the row unknowable, so a latency reader never divides a reset stall counter
    /// by a sibling's real reads and renders it as 0.00 ms (see WaitStatsCollectorDefinitionTests).
    /// </summary>
    [Fact]
    public void WritePayload_WritesTheMeasuredInterval_AsTheMinimumOverTheRowsGroups()
    {
        var deltas = new RecordingCollectorDeltaCalculator { ReportedInterval = 137 };
        var context = CollectorTestContext.Make(deltas);
        var row = new FileIoStatsCollector.Row("SO", "SO_data", "ROWS", @"D:\so.mdf", 100.5m, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9, 1);

        var writer = new RecordingCollectorRowWriter();
        FileIoStatsCollector.Instance.WritePayload(row, writer, context);
        Assert.Equal(137, writer.Values[^1]);

        deltas.IntervalByGroup["file_io_stall_queued_write"] = 0;
        writer = new RecordingCollectorRowWriter();
        FileIoStatsCollector.Instance.WritePayload(row, writer, context);
        Assert.Equal(0, writer.Values[^1]);
    }
}

/// <summary>
/// Pins that the shared DatabaseExclusionFilter and Lite's original builder (still used by
/// un-migrated collectors) produce identical SQL — the temporary duplication cannot drift.
/// </summary>
public sealed class DatabaseExclusionFilterTests
{
    [Fact]
    public void Empty_ReturnsEmptyClause_NoParameters()
    {
        var (clause, parameters) = DatabaseExclusionFilter.Build(null, "d.name");
        Assert.Equal(string.Empty, clause);
        Assert.Empty(parameters);
    }

    [Fact]
    public void Names_ProduceParameterizedNotIn()
    {
        var (clause, parameters) = DatabaseExclusionFilter.Build(new[] { "A", "B" }, "d.name");

        Assert.Equal("AND d.name NOT IN (@excl_db_0, @excl_db_1)", clause);
        Assert.Equal(2, parameters.Count);
        Assert.Equal("A", parameters[0].Value);
        Assert.All(parameters, p => Assert.Equal(CollectorParameterType.NVarChar128, p.Type));
    }

    [Fact]
    public void MatchesLiteOriginalBuilder_ClauseAndParameters()
    {
        var names = new[] { "AdventureWorks", "Stack Overflow" };

        var (sharedClause, sharedParams) = DatabaseExclusionFilter.Build(names, "d.name");
        var (liteClause, liteParams) = RemoteCollectorService.BuildDatabaseExclusionFilter(names, "d.name");

        Assert.Equal(liteClause, sharedClause);
        Assert.Equal(liteParams.Count, sharedParams.Count);
        for (int i = 0; i < liteParams.Count; i++)
        {
            Assert.Equal(liteParams[i].ParameterName, sharedParams[i].Name);
            Assert.Equal(liteParams[i].Value, sharedParams[i].Value);
            Assert.Equal(System.Data.SqlDbType.NVarChar, liteParams[i].SqlDbType);
            Assert.Equal(128, liteParams[i].Size);
        }
    }
}
