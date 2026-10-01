/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Another database on an Azure SQL Database server has one stored row, named <c>(whole database)</c>. Rows stored
/// before the allocated/used fix hold the database's USED space in <c>total_size_mb</c> and nothing in
/// <c>used_size_mb</c>. Rows stored after it hold the ALLOCATED size in <c>total_size_mb</c> and the used space in
/// <c>used_size_mb</c>. At upgrade a Hyperscale database that did not grow at all goes from 119 MB to 10,240 MB in
/// the store, and a growth read that set the two rows side by side would report a 10,121 MB rise.
///
/// <para>These pins hold the rule: a growth read leaves the old-shape rows out, at read time, so a database in that
/// position reads like one added inside the window (a blank past size and growth 0). A file whose used-space probe
/// failed also has no used space, and it must still count, because it is a real file with a real history.</para>
/// </summary>
public sealed class AzureSiblingGrowthTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4902;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AzureSiblingGrowthTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly DateTime Collected = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>One stored size row. A sibling row has no database id, file id or physical name.</summary>
    private async Task SeedAsync(string database, int? fileId, string fileName, double? total, double? used, DateTime? at = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open) await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
     file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1, $2, $3, 'SibSrv', $4, $5, $6, 'ROWS', $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at ?? Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileId.HasValue ? 7 : DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)fileId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileId.HasValue ? @"C:\" + fileName : DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)total ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)used ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private Task SeedSiblingAsync(string database, double total, double? used, DateTime? at = null) =>
        SeedAsync(database, null, AzureSiblingDatabaseSize.FileName, total, used, at);

    /// <summary>
    /// 31 and 8 days ago the sibling is in the old shape (its USED space, 119 MB, as the total). Now it is in the new
    /// shape (its allocation, 10,240 MB, with 119 MB used). Beside it a real file whose used-space probe failed, so
    /// its used space is empty in every row: it is not an old-shape sibling row and its growth must still count.
    /// </summary>
    private async Task SeedTheUpgradeAsync()
    {
        foreach (var daysAgo in new[] { 31, 8 })
        {
            var at = Collected.AddDays(-daysAgo);
            await SeedSiblingAsync("sibdb", 119, null, at);
            await SeedAsync("realdb", 1, "realdb_data", daysAgo == 31 ? 100 : 120, null, at);
        }

        await SeedSiblingAsync("sibdb", 10_240, 119);
        await SeedAsync("realdb", 1, "realdb_data", 150, null);
    }

    [Fact]
    public async Task StorageGrowth_TheUpgradeStepInASiblingIsNotGrowth_AndARealFileWithNoUsedSpaceStillCounts()
    {
        await SeedTheUpgradeAsync();

        var rows = await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId);

        /* The old rows are left out, so the sibling reads like a database added inside the window. */
        var sib = Assert.Single(rows, r => r.DatabaseName == "sibdb");
        Assert.Equal(0m, sib.Growth7dMb);
        Assert.Equal(0m, sib.Growth30dMb);
        Assert.Equal(0m, sib.DailyGrowthRateMb);
        Assert.Equal(0m, sib.GrowthPct30d);
        Assert.Null(sib.Size7dAgoMb);
        Assert.Null(sib.Size30dAgoMb);
        Assert.Equal(10_240m, sib.CurrentSizeMb);

        /* A real file with no used space is not an old-shape sibling row: it has a file id and a file name. */
        var real = Assert.Single(rows, r => r.DatabaseName == "realdb");
        Assert.Equal(150m, real.CurrentSizeMb);
        Assert.Equal(120m, real.Size7dAgoMb);
        Assert.Equal(100m, real.Size30dAgoMb);
        Assert.Equal(30m, real.Growth7dMb);
        Assert.Equal(50m, real.Growth30dMb);
    }

    [Fact]
    public async Task StorageGrowth_ASiblingCollectedAfterTheFix_GrowsLikeAnyOtherDatabase()
    {
        /* Once the new shape is old enough to compare against, the sibling reports its growth. */
        await SeedSiblingAsync("sibdb", 10_000, 100, Collected.AddDays(-31));
        await SeedSiblingAsync("sibdb", 10_100, 110, Collected.AddDays(-8));
        await SeedSiblingAsync("sibdb", 10_240, 119);

        var sib = Assert.Single(await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId), r => r.DatabaseName == "sibdb");

        Assert.Equal(10_240m, sib.CurrentSizeMb);
        Assert.Equal(10_100m, sib.Size7dAgoMb);
        Assert.Equal(10_000m, sib.Size30dAgoMb);
        Assert.Equal(140m, sib.Growth7dMb);
        Assert.Equal(240m, sib.Growth30dMb);
        Assert.Equal(8m, Math.Round(sib.DailyGrowthRateMb, 4));
    }

    [Fact]
    public async Task StorageGrowth_BeforeTheFirstCollectionAfterTheUpgrade_TheSiblingIsNotListedYet()
    {
        /* The newest snapshot is still in the old shape, so the sibling has no row to show until the next
           collection lands. Other databases are unaffected. */
        await SeedSiblingAsync("sibdb", 119, null, Collected.AddDays(-8));
        await SeedSiblingAsync("sibdb", 119, null);
        await SeedAsync("realdb", 1, "realdb_data", 100, 40, Collected.AddDays(-8));
        await SeedAsync("realdb", 1, "realdb_data", 110, 42);

        var rows = await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId);

        Assert.DoesNotContain(rows, r => r.DatabaseName == "sibdb");
        Assert.Equal(10m, Assert.Single(rows, r => r.DatabaseName == "realdb").Growth7dMb);
    }

    /// <summary>
    /// A sibling's size is data space only: the server reports no log size for another database. The read flags it from
    /// the latest snapshot, and the row's Note says the log size is not reported.
    /// </summary>
    [Fact]
    public async Task StorageGrowth_ASibling_IsFlagged_AndItsNoteSaysItsLogSizeIsNotReported()
    {
        await SeedSiblingAsync("sibdb", 10_240, 119);

        var sib = Assert.Single(await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId), r => r.DatabaseName == "sibdb");

        Assert.True(sib.HasSiblingRow);
        Assert.False(sib.HasLogServiceFile);
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, sib.Note);
    }

    /// <summary>
    /// A Hyperscale database whose log file lives in the log service has a log file with no size. The sums skip it, so
    /// the size is its data file alone. The read flags the database, and the row's Note says the log is not in the size.
    /// </summary>
    [Fact]
    public async Task StorageGrowth_ALogServiceDatabase_IsFlagged_AndItsNoteSaysSo()
    {
        await SeedAsync("hsdb", 1, "hsdb_data", 4_096, 1_000);
        await SeedAsync("hsdb", 2, "hsdb_log", null, 12);

        var hyperscale = Assert.Single(await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId), r => r.DatabaseName == "hsdb");

        Assert.False(hyperscale.HasSiblingRow);
        Assert.True(hyperscale.HasLogServiceFile);
        Assert.Equal(HyperscaleLogSize.LogNote, hyperscale.Note);
        Assert.Equal(4_096m, hyperscale.CurrentSizeMb);
    }

    /// <summary>
    /// A normal database counts every file, and a real file that only carries the sibling name has a file id, so it is
    /// not a sibling. Neither gets a flag or a Note, even with a sibling and a Hyperscale database in the same snapshot.
    /// </summary>
    [Fact]
    public async Task StorageGrowth_ANormalDatabase_AndARealFileWithTheSiblingName_AreNotFlagged()
    {
        await SeedSiblingAsync("sibdb", 10_240, 119);
        await SeedAsync("hsdb", 1, "hsdb_data", 4_096, 1_000);
        await SeedAsync("hsdb", 2, "hsdb_log", null, 12);
        await SeedAsync("realdb", 1, "realdb_data", 150, 50);
        await SeedAsync("realdb", 2, "realdb_log", 64, 2);
        await SeedAsync("oddb", 3, AzureSiblingDatabaseSize.FileName, 5, 1);

        var rows = await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId);

        foreach (var name in new[] { "realdb", "oddb" })
        {
            var normal = Assert.Single(rows, r => r.DatabaseName == name);
            Assert.False(normal.HasSiblingRow);
            Assert.False(normal.HasLogServiceFile);
            Assert.Null(normal.Note);
        }

        Assert.Equal(214m, Assert.Single(rows, r => r.DatabaseName == "realdb").CurrentSizeMb);
    }

    /// <summary>
    /// A sibling still in the old shape in the latest snapshot is left out of the sums, so it is not listed, and
    /// the flag is read from the rows the sums keep: a database listed with a post-fix row is flagged whatever its
    /// history holds.
    /// </summary>
    [Fact]
    public async Task StorageGrowth_ASiblingWithOldShapeHistory_IsFlaggedFromItsNewRow()
    {
        await SeedTheUpgradeAsync();

        var rows = await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId);

        Assert.Equal(AzureSiblingDatabaseSize.LogNote, Assert.Single(rows, r => r.DatabaseName == "sibdb").Note);
        Assert.Null(Assert.Single(rows, r => r.DatabaseName == "realdb").Note);
    }

    /// <summary>
    /// The file-growth alert reads the last hour of sizes, and this collector runs hourly, so right after the upgrade
    /// the window holds the last old-shape sample and the first new-shape one. Set side by side they read as a rise
    /// of the whole allocation, 10,121 MB for a database that did not grow.
    /// </summary>
    [Fact]
    public async Task FileGrowthRead_TheUpgradeStepInASiblingIsNotARise_AndARealFileWithNoUsedSpaceStillCounts()
    {
        var halfHourAgo = Collected.AddMinutes(-30);
        await SeedSiblingAsync("sibdb", 119, null, halfHourAgo);
        await SeedSiblingAsync("sibdb", 10_240, 119);
        await SeedAsync("realdb", 1, "realdb_data", 100, null, halfHourAgo);
        await SeedAsync("realdb", 1, "realdb_data", 150, null);

        var files = await new LocalDataService(_duckDb).GetDatabaseFileGrowthAsync(ServerId, 60);

        var sib = Assert.Single(files, f => f.DatabaseName == "sibdb");
        Assert.Equal(0d, sib.GrowthMb);
        Assert.Equal(10_240d, sib.TotalSizeMb);

        /* A real file with no used space is not an old-shape sibling row: it has a file id and a file name. */
        var real = Assert.Single(files, f => f.DatabaseName == "realdb");
        Assert.Equal(50d, real.GrowthMb);
        Assert.Equal(150d, real.TotalSizeMb);
    }

    /// <summary>The other side of the rule: two samples in the new shape are a real history, so a sibling that grows
    /// inside the window reads its growth like any other database.</summary>
    [Fact]
    public async Task FileGrowthRead_ASiblingCollectedAfterTheFix_RisesLikeAnyOtherDatabase()
    {
        await SeedSiblingAsync("sibdb", 10_000, 100, Collected.AddMinutes(-30));
        await SeedSiblingAsync("sibdb", 10_240, 119);

        var sib = Assert.Single(await new LocalDataService(_duckDb).GetDatabaseFileGrowthAsync(ServerId, 60), f => f.DatabaseName == "sibdb");

        Assert.Equal(240d, sib.GrowthMb);
        Assert.Equal(10_240d, sib.TotalSizeMb);
    }

    /// <summary>
    /// A Hyperscale sibling row holds 10,240 MB allocated and 119 MB used. The Database Sizes read returns it as a data
    /// row (<c>ROWS</c>, no file id), so it is not read as a log file: its free space is the difference, its used share
    /// rounds to one place, and it adds its allocation and free space to the sums the FinOps health score takes over the
    /// latest snapshot.
    /// </summary>
    [Fact]
    public async Task DatabaseSizes_AHyperscaleSibling_ReadsItsFreeSpace_AndAddsToTheFreeSpaceSums()
    {
        await SeedSiblingAsync("sibdb", 10_240, 119);
        await SeedAsync("realdb", 1, "realdb_data", 100, 10);

        var rows = await new LocalDataService(_duckDb).GetDatabaseSizeLatestAsync(ServerId);

        var sibling = Assert.Single(rows, r => r.DatabaseName == "sibdb");
        Assert.Equal("ROWS", sibling.FileTypeDesc);
        Assert.Equal(10_240m, sibling.TotalSizeMb);
        Assert.Equal(119m, sibling.UsedSizeMb);
        Assert.Equal(10_121m, sibling.FreeSpaceMb);
        Assert.Equal(1.2m, sibling.UsedPct);

        Assert.Equal(10_240m, DatabaseSizeRow.AllocatedTotalMb(new[] { sibling }));
        Assert.Equal(10_121m, DatabaseSizeRow.FreeTotalMb(new[] { sibling }));
        Assert.Equal(10_340m, DatabaseSizeRow.AllocatedTotalMb(rows));
        Assert.Equal(10_211m, DatabaseSizeRow.FreeTotalMb(rows));
    }

    /// <summary>The health score's free-space sums sit in the FinOps tab's code-behind, which a test cannot reach. They
    /// are the two row helpers the test above pins, called on the latest snapshot, so this holds the call by source.</summary>
    [Fact]
    public void FinOpsHealthScore_TakesItsFreeSpaceSums_FromTheTwoRowHelpers()
    {
        var source = ParitySource.ReadFile("Lite/Controls/FinOpsTab.xaml.cs");

        Assert.Contains("var totalStorageMb = DatabaseSizeRow.AllocatedTotalMb(dbSizes);", source, StringComparison.Ordinal);
        Assert.Contains("var totalFreeMb = DatabaseSizeRow.FreeTotalMb(dbSizes);", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The old-shape test never drops a row it should keep. A row with no file name has to stay: a bare
    /// <c>file_name = ...</c> is NULL for it, and <c>NOT (... AND NULL AND ...)</c> is NULL, which a WHERE reads as
    /// false. The store holds file names NOT NULL, so no stored row can show this, and the clause is run over literal
    /// rows here instead.
    /// </summary>
    [Fact]
    public async Task ExcludePreFixRows_DropsOnlyTheOldShapeSiblingRow_AndKeepsARowWithNoFileName()
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SELECT n
FROM (VALUES
    (1, CAST(NULL AS INTEGER), CAST(NULL AS VARCHAR), CAST(NULL AS DECIMAL(19,2))),
    (2, CAST(NULL AS INTEGER), '(whole database)', CAST(NULL AS DECIMAL(19,2))),
    (3, CAST(NULL AS INTEGER), '(whole database)', CAST(119 AS DECIMAL(19,2))),
    (4, 1, 'data_0', CAST(NULL AS DECIMAL(19,2))),
    (5, 1, '(whole database)', CAST(NULL AS DECIMAL(19,2)))
) AS t(n, file_id, file_name, used_size_mb)
WHERE " + AzureSiblingDatabaseSize.ExcludePreFixRows + @"
ORDER BY n";

        var kept = new List<int>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            kept.Add(reader.GetInt32(0));
        }

        /* Row 2 is the only old-shape sibling row: no file id, the sibling name, no used space. Row 1 has no file
           name, row 3 has its used space, row 4 is a real file, row 5 has a file id. */
        Assert.Equal(new[] { 1, 3, 4, 5 }, kept);
    }

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
    public void StorageGrowthSql_LeavesTheOldShapeRowsOutOfTheLatestAnd7dAnd30dSums()
    {
        var sql = LocalDataService.StorageGrowthSql;

        foreach (var sum in new[] { "latest", "past_7d", "past_30d" })
        {
            Assert.True(
                Squash(CteBody(sql, sum)).Contains(AzureSiblingDatabaseSize.ExcludePreFixRows, StringComparison.Ordinal),
                $"The {sum} sum does not leave the old-shape sibling rows out.");
        }
    }

    [Fact]
    public void DatabaseFileGrowthSql_LeavesTheOldShapeRowsOutOfTheWindow()
    {
        Assert.True(
            Squash(CteBody(LocalDataService.DatabaseFileGrowthSql, "windowed")).Contains(AzureSiblingDatabaseSize.ExcludePreFixRows, StringComparison.Ordinal),
            "The windowed rows do not leave the old-shape sibling rows out.");
    }
}
