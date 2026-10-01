/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Azure SQL Database Hyperscale keeps its transaction log in the log service. <c>sys.database_files</c> still lists a
/// LOG row for it, sized at about 1 TB (1,046,528 MB), which is not storage the database holds or pays for: Hyperscale
/// bills allocated data storage. The rule these pins hold: the collector stores a NULL size for that one row, every
/// reader shows it as "n/a (log service)", and it stays out of every allocated total. The data file keeps its real
/// size. SQL Server, Managed Instance and non-Hyperscale Azure SQL Database are unchanged.
/// </summary>
public sealed class HyperscaleLogSizeCollectorTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static string AzureQuery() =>
        DatabaseSizeStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas, isAzureSqlDb: true)).Text;

    [Fact]
    public void AzureSqlDb_Query_DetectsHyperscaleFromTheEditionOfTheCurrentDatabase()
    {
        var text = AzureQuery();

        /* DATABASEPROPERTYEX needs only the current database, so it adds no permission on the Azure path. It
           returns sql_variant, hence the CONVERT. */
        Assert.Contains("CONVERT(nvarchar(64), DATABASEPROPERTYEX(DB_NAME(), N'Edition')) = N'Hyperscale'", text, StringComparison.Ordinal);
    }

    [Fact]
    public void AzureSqlDb_Query_NullsTheSizeOfTheHyperscaleLogRowOnly_AndKeepsTheRealSizeOtherwise()
    {
        var text = AzureQuery();

        /* The LOG row (type 1) of a Hyperscale database is the only row the decision names. */
        Assert.Matches(new Regex(@"is_log_service =\s*CASE\s*WHEN @is_hyperscale = 1\s*AND\s+df\.type = 1 /\*LOG\*/\s*THEN CONVERT\(bit, 1\)\s*ELSE CONVERT\(bit, 0\)", RegexOptions.Singleline), text);

        /* Its size is NULL; every other file keeps the size sys.database_files reports. */
        Assert.Matches(new Regex(@"total_size_mb =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(decimal\(19,2\), NULL\)\s*ELSE CONVERT\(decimal\(19,2\), df\.size \* 8\.0 / 1024\.0\)\s*END", RegexOptions.Singleline), text);

        /* The log service has no growth setting or ceiling to report either, so they go NULL with the size and
           cannot read as a real 2 TB cap or a real growth step beside "n/a (log service)". */
        Assert.Matches(new Regex(@"auto_growth_mb =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(decimal\(19,2\), NULL\)", RegexOptions.Singleline), text);
        Assert.Matches(new Regex(@"max_size_mb =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(decimal\(19,2\), NULL\)", RegexOptions.Singleline), text);
        Assert.Matches(new Regex(@"is_percent_growth =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(bit, NULL\)", RegexOptions.Singleline), text);
        Assert.Matches(new Regex(@"growth_pct =\s*CASE\s*WHEN ls\.is_log_service = 0\s*AND\s+df\.is_percent_growth = 1", RegexOptions.Singleline), text);

        /* Used space and the VLF count stay as collected: neither feeds an allocated total. */
        Assert.Contains("CONVERT(decimal(19,2), FILEPROPERTY(df.name, N'SpaceUsed') * 8.0 / 1024.0)", text, StringComparison.Ordinal);
        Assert.Contains("vlf_count =", text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPrem_Query_IsUnchanged_NoHyperscaleBranch()
    {
        /* SQL Server and Managed Instance take the on-prem text; it has no Hyperscale branch and still reports
           every file at the size the engine gives it. */
        var plan = DatabaseSizeStatsCollector.Instance.BuildQuery(CollectorTestContext.Make(s_deltas));

        Assert.DoesNotContain("Hyperscale", plan.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("is_log_service", plan.Text, StringComparison.Ordinal);
        Assert.Contains("CONVERT(decimal(19,2), df.size * 8.0 / 1024.0)", plan.Text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ReadAsync_NullTotalSize_IsKeptNull_AndDoesNotKillTheBatch()
    {
        /* The Hyperscale log row: size, growth and ceiling NULL, used space as collected. An unguarded
           GetDecimal on ordinal 6 threw here and lost the whole batch (the #3262 shape). */
        using var reader = new FakeCollectorDataReader(
            new object[]
            {
                "hsdb", 5, 1, "ROWS", "hsdb_data", @"D:\hsdb.mdf", 10240.00m, 315.25m,
                128.00m, 4194304.00m, "FULL", DBNull.Value, "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value,
                0, DBNull.Value, DBNull.Value,
            },
            new object[]
            {
                "hsdb", 5, 2, "LOG", "hsdb_log", @"D:\hsdb.ldf", DBNull.Value, 40.50m,
                DBNull.Value, DBNull.Value, "FULL", DBNull.Value, "ONLINE", DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, 4,
            });

        var rows = await DatabaseSizeStatsCollector.Instance.ReadAsync(reader, CollectorTestContext.Make(s_deltas, isAzureSqlDb: true), CancellationToken.None);

        Assert.Equal(2, rows.Count);
        Assert.Equal(10240.00m, rows[0].TotalSizeMb);
        Assert.Null(rows[1].TotalSizeMb);
        Assert.Null(rows[1].AutoGrowthMb);
        Assert.Null(rows[1].MaxSizeMb);
        Assert.Equal(40.50m, rows[1].UsedSizeMb);
        Assert.Equal(4, rows[1].VlfCount);

        var writer = new RecordingCollectorRowWriter();
        DatabaseSizeStatsCollector.Instance.WritePayload(rows[1], writer, CollectorTestContext.Make(s_deltas, isAzureSqlDb: true));
        Assert.Equal(19, writer.Values.Count);
        Assert.Null(writer.Values[6]);
    }
}

/// <summary>
/// The readers: a Hyperscale log row reads as a null size (which the grid words as "n/a (log service)"), stays out of
/// every allocated total, and the data row counts. A non-Hyperscale database, whose log row carries a real size, is
/// unchanged. Runs against a real DuckDB, so it also proves the store accepts the NULL size on a fresh database.
/// </summary>
public sealed class HyperscaleLogSizeReadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4901;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public HyperscaleLogSizeReadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly DateTime Collected = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private async Task SeedAsync(string database, int fileId, string fileType, string fileName, double? total, double? used, double? autoGrowth, double? max, DateTime? at = null)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open) await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
     file_name, physical_name, total_size_mb, used_size_mb, auto_growth_mb, max_size_mb)
VALUES ($1, $2, $3, 'HsSrv', $4, 7, $5, $6, $7, $8, $9, $10, $11, $12)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = at ?? Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileId });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileType });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = @"C:\" + fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)total ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)used ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)autoGrowth ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)max ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedHyperscaleAndPlainAsync()
    {
        /* Hyperscale: the data file reports its real allocation; the log row's size, growth and ceiling are NULL
           (the log service), its used space as collected. */
        await SeedAsync("hsdb", 1, "ROWS", "hsdb_data", 10_240, 315, 1_024, 4_194_304);
        await SeedAsync("hsdb", 2, "LOG", "hsdb_log", null, 40, null, null);

        /* A non-Hyperscale database: both files carry a real size and count. */
        await SeedAsync("plain", 1, "ROWS", "plain_data", 100, 10, 64, -1);
        await SeedAsync("plain", 2, "LOG", "plain_log", 50, 5, 64, 2_097_152);
    }

    [Fact]
    public async Task DatabaseSizesGrid_HyperscaleLogRow_IsNull_AndOutOfTheAllocatedTotals_WhileTheDataRowCounts()
    {
        await SeedHyperscaleAndPlainAsync();
        var service = new LocalDataService(_duckDb);

        var rows = await service.GetDatabaseSizeLatestAsync(ServerId);
        var hsData = Assert.Single(rows, r => r.DatabaseName == "hsdb" && r.FileTypeDesc == "ROWS");
        var hsLog = Assert.Single(rows, r => r.DatabaseName == "hsdb" && r.FileTypeDesc == "LOG");

        Assert.Equal(10_240m, hsData.TotalSizeMb);
        Assert.Equal(10_240m - 315m, hsData.FreeSpaceMb);

        /* Null, never 0: the grid words null as n/a (log service), and 0 would read as a real empty log. */
        Assert.Null(hsLog.TotalSizeMb);
        Assert.Null(hsLog.FreeSpaceMb);
        Assert.Null(hsLog.UsedPct);
        Assert.Equal(40m, hsLog.UsedSizeMb);

        var hsRows = rows.Where(r => r.DatabaseName == "hsdb").ToList();
        Assert.Equal(10_240m, DatabaseSizeRow.AllocatedTotalMb(hsRows));
        Assert.Equal(10_240m - 315m, DatabaseSizeRow.FreeTotalMb(hsRows));

        /* Unchanged: the non-Hyperscale log row counts at its real size. */
        var plainRows = rows.Where(r => r.DatabaseName == "plain").ToList();
        Assert.Equal(150m, DatabaseSizeRow.AllocatedTotalMb(plainRows));
        Assert.All(plainRows, r => Assert.NotNull(r.TotalSizeMb));

        Assert.Equal(10_390m, DatabaseSizeRow.AllocatedTotalMb(rows));
    }

    [Fact]
    public async Task UtilizationChart_HyperscaleDatabase_AllocatesTheDataFileOnly_AndUsedStaysOnTheSameFooting()
    {
        await SeedHyperscaleAndPlainAsync();
        var service = new LocalDataService(_duckDb);

        var summary = await service.GetDatabaseSizeSummaryAsync(ServerId);

        var hs = Assert.Single(summary, r => r.DatabaseName == "hsdb");
        Assert.Equal(10_240m, hs.TotalMb);
        Assert.Equal(315m, hs.UsedMb);

        var plain = Assert.Single(summary, r => r.DatabaseName == "plain");
        Assert.Equal(150m, plain.TotalMb);
        Assert.Equal(15m, plain.UsedMb);
    }

    /* The size sys.database_files reports for a Hyperscale log file. History collected before the collector stored
       NULL for that row still holds it, and the store keeps 90 days of it. */
    private const double LogServiceMb = 1_046_528;

    /// <summary>
    /// 31 and 8 days ago, before the change: the Hyperscale log row holds the ~1 TB. Now it is NULL. Beside it, a
    /// database whose log row has a real size and the same file_id, and whose second data file was dropped this week.
    /// </summary>
    private async Task SeedGrowthHistoryAsync()
    {
        foreach (var daysAgo in new[] { 31, 8 })
        {
            var at = Collected.AddDays(-daysAgo);
            await SeedAsync("hsdb", 1, "ROWS", "hsdb_data", daysAgo == 31 ? 10_000 : 10_100, 300, 1_024, -1, at);
            await SeedAsync("hsdb", 2, "LOG", "hsdb_log", LogServiceMb, 40, 1_024, 1_048_576, at);

            await SeedAsync("plain", 1, "ROWS", "plain_data", 1_000, 10, 64, -1, at);
            await SeedAsync("plain", 3, "ROWS", "plain_data2", 500, 5, 64, -1, at);
            await SeedAsync("plain", 2, "LOG", "plain_log", 200, 5, 64, 2_097_152, at);
        }

        await SeedAsync("hsdb", 1, "ROWS", "hsdb_data", 10_240, 315, 1_024, -1);
        await SeedAsync("hsdb", 2, "LOG", "hsdb_log", null, 40, null, null);

        await SeedAsync("plain", 1, "ROWS", "plain_data", 1_000, 10, 64, -1);
        await SeedAsync("plain", 2, "LOG", "plain_log", 260, 5, 64, 2_097_152);
    }

    [Fact]
    public async Task StorageGrowth_HyperscaleHistoryHoldsTheOneTerabyteLogRow_ShowsTheDataFileGrowth_NotA99PercentDrop()
    {
        await SeedGrowthHistoryAsync();

        var hs = Assert.Single(await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId), r => r.DatabaseName == "hsdb");

        /* The log file is out of all three sums, so each side is the data file alone. Summing the old ~1 TB rows on
           the past side only read as 10,240 against 1,056,528 MB: -99%. */
        Assert.Equal(140m, hs.Growth7dMb);
        Assert.Equal(2.4m, Math.Round(hs.GrowthPct30d!.Value, 4));
        Assert.Equal(240m, hs.Growth30dMb);
        Assert.Equal(8m, Math.Round(hs.DailyGrowthRateMb!.Value, 4));
        Assert.Equal(10_240m, hs.CurrentSizeMb);
        Assert.Equal(10_100m, hs.Size7dAgoMb);
        Assert.Equal(10_000m, hs.Size30dAgoMb);
    }

    [Fact]
    public async Task StorageGrowth_AFileGoneFromTheLatestSnapshot_StillCountsAsShrinkage_AndARealLogCounts()
    {
        await SeedGrowthHistoryAsync();

        var plain = Assert.Single(await new LocalDataService(_duckDb).GetStorageGrowthAsync(ServerId), r => r.DatabaseName == "plain");

        /* plain_data2 has no row in the latest snapshot, so the rule has nothing to match and its 500 MB stays on the
           past side: the database shrank. plain_log shares hsdb_log's file_id but has a real size, so it counts on
           every side (its 60 MB of growth included). */
        Assert.Equal(1_260m, plain.CurrentSizeMb);
        Assert.Equal(1_700m, plain.Size7dAgoMb);
        Assert.Equal(1_700m, plain.Size30dAgoMb);
        Assert.Equal(-440m, plain.Growth7dMb);
        Assert.Equal(-440m, plain.Growth30dMb);
    }

    [Fact]
    public async Task StorageGrowth_LatestSnapshotStillFromBeforeTheChange_CountsTheLogRowOnBothSides_LikeTheDatabaseSizesGrid()
    {
        /* Until the first hourly collection after the upgrade, the latest snapshot still holds the ~1 TB log row. The
           Database Sizes grid shows that stored figure, and Storage Growth counts it on both sides: growth is still
           the data file's own. */
        await SeedAsync("hsdb", 1, "ROWS", "hsdb_data", 10_000, 300, 1_024, -1, Collected.AddDays(-31));
        await SeedAsync("hsdb", 2, "LOG", "hsdb_log", LogServiceMb, 40, 1_024, 1_048_576, Collected.AddDays(-31));
        await SeedAsync("hsdb", 1, "ROWS", "hsdb_data", 10_240, 315, 1_024, -1);
        await SeedAsync("hsdb", 2, "LOG", "hsdb_log", LogServiceMb, 40, 1_024, 1_048_576);

        var service = new LocalDataService(_duckDb);
        var hs = Assert.Single(await service.GetStorageGrowthAsync(ServerId), r => r.DatabaseName == "hsdb");

        Assert.Equal(10_240m + (decimal)LogServiceMb, hs.CurrentSizeMb);
        Assert.Equal(240m, hs.Growth7dMb);
        Assert.Equal(240m, hs.Growth30dMb);

        var grid = await service.GetDatabaseSizeLatestAsync(ServerId);
        Assert.Equal(hs.CurrentSizeMb, DatabaseSizeRow.AllocatedTotalMb(grid.Where(r => r.DatabaseName == "hsdb").ToList()));
    }

    [Fact]
    public void StorageGrowthSql_AppliesOnePredicateToTheLatestAnd7dAnd30dSums()
    {
        /* The behaviour pins above cannot show the latest side: there the predicate drops only the NULL rows SUM
           already skips. The text shows it, and holds all three sums to the same predicate. Darling's
           ViewerDataService.StorageGrowthSql pin is the twin. */
        var sql = LocalDataService.StorageGrowthSql;
        const string predicate = "NOT EXISTS ( SELECT 1 FROM log_service_files AS ls WHERE ls.database_name = s.database_name AND ls.file_id = s.file_id )";

        foreach (var sum in new[] { "latest", "past_7d", "past_30d" })
        {
            Assert.True(Squash(CteBody(sql, sum)).Contains(predicate, StringComparison.Ordinal), $"The {sum} sum does not leave the log-service file out.");
        }

        var files = Squash(CteBody(sql, "log_service_files"));
        Assert.Contains("collection_time = ( SELECT MAX(collection_time) FROM v_database_size_stats WHERE server_id = $1 )", files, StringComparison.Ordinal);
        Assert.Contains("AND total_size_mb IS NULL", files, StringComparison.Ordinal);
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
    public async Task GetDatabaseSizesTool_Read_PassesTheNullSizeGrowthAndCeilingThroughAsNull()
    {
        await SeedHyperscaleAndPlainAsync();
        var service = new LocalDataService(_duckDb);

        var rows = await service.GetLatestDatabaseSizeStatsAsync(ServerId);

        var hsLog = Assert.Single(rows, r => r.DatabaseName == "hsdb" && r.FileTypeDesc == "LOG");
        Assert.Null(hsLog.TotalSizeMb);
        Assert.Null(hsLog.AutoGrowthMb);
        Assert.Null(hsLog.MaxSizeMb);

        var hsData = Assert.Single(rows, r => r.DatabaseName == "hsdb" && r.FileTypeDesc == "ROWS");
        Assert.Equal(10_240d, hsData.TotalSizeMb);

        var plainLog = Assert.Single(rows, r => r.DatabaseName == "plain" && r.FileTypeDesc == "LOG");
        Assert.Equal(50d, plainLog.TotalSizeMb);
        Assert.Equal(64d, plainLog.AutoGrowthMb);
        Assert.Equal(2_097_152d, plainLog.MaxSizeMb);
    }

    [Fact]
    public void Row_WithoutAnAllocation_ReadsAsNotApplicable_WithoutThrowing()
    {
        var log = new DatabaseSizeRow { FileTypeDesc = "LOG", TotalSizeMb = null, UsedSizeMb = 40m };
        Assert.Null(log.FreeSpaceMb);
        Assert.Null(log.UsedPct);

        /* A valued row is unchanged. */
        var data = new DatabaseSizeRow { FileTypeDesc = "ROWS", TotalSizeMb = 200m, UsedSizeMb = 50m };
        Assert.Equal(150m, data.FreeSpaceMb);
        Assert.Equal(25.0m, data.UsedPct);
    }

    [Fact]
    public void TheGridWordsANullSize_AsTheSharedHyperscaleText()
    {
        Assert.Equal("n/a (log service)", HyperscaleLogSize.Display);

        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "FinOpsTab.xaml"));
        Assert.Contains("{Binding TotalSizeMb, StringFormat='{}{0:N2}', TargetNullValue='" + HyperscaleLogSize.Display + "'}", xaml, StringComparison.Ordinal);
    }

    private static DatabaseSizeStatsRow SizeRow(string database, string name, string fileType, double? total, double used, double? growth, double? max) => new()
    {
        DatabaseName = database,
        FileName = name,
        FileTypeDesc = fileType,
        TotalSizeMb = total,
        UsedSizeMb = used,
        AutoGrowthMb = growth,
        MaxSizeMb = max,
        CollectionTime = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc),
    };

    [Fact]
    public void GetDatabaseSizesPayload_HyperscaleLogFile_IsNullWithTheNote_AndOutOfTheDatabaseTotal()
    {
        var rows = new[]
        {
            SizeRow("hsdb", "hsdb_data", "ROWS", 10_240, 315, 1_024, 4_194_304),
            SizeRow("hsdb", "hsdb_log", "LOG", null, 40, null, null),
            SizeRow("plain", "plain_data", "ROWS", 100, 10, 64, -1),
            SizeRow("plain", "plain_log", "LOG", 50, 5, 64, 2_097_152),
        };

        using var doc = System.Text.Json.JsonDocument.Parse(McpServerInfoTools.DatabaseSizesPayload("srv", rows));
        var root = doc.RootElement;

        /* The one short note, in the shared words, and only because a log-service file is present. */
        Assert.Equal(HyperscaleLogSize.Note, root.GetProperty("note").GetString());

        var hs = root.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == "hsdb");
        Assert.Equal(10_240d, hs.GetProperty("total_size_mb").GetDouble());
        /* Used sums over the same files as the total, so the log file's own used space (40) stays out of it. */
        Assert.Equal(315d, hs.GetProperty("used_size_mb").GetDouble());
        var hsLog = hs.GetProperty("files").EnumerateArray().Single(f => f.GetProperty("file_type").GetString() == "LOG");
        Assert.Equal(System.Text.Json.JsonValueKind.Null, hsLog.GetProperty("total_size_mb").ValueKind);
        Assert.Equal(System.Text.Json.JsonValueKind.Null, hsLog.GetProperty("auto_growth_mb").ValueKind);
        Assert.Equal(System.Text.Json.JsonValueKind.Null, hsLog.GetProperty("max_size_mb").ValueKind);
        Assert.Equal(40d, hsLog.GetProperty("used_size_mb").GetDouble());
        var hsData = hs.GetProperty("files").EnumerateArray().Single(f => f.GetProperty("file_type").GetString() == "ROWS");
        Assert.Equal(10_240d, hsData.GetProperty("total_size_mb").GetDouble());

        /* Unchanged: a database whose log row carries a real size counts it. */
        var plain = root.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == "plain");
        Assert.Equal(150d, plain.GetProperty("total_size_mb").GetDouble());
    }

    [Fact]
    public void GetDatabaseSizesPayload_NoLogServiceFile_HasNoNote_AndTheSameShapeAsBefore()
    {
        var rows = new[]
        {
            SizeRow("plain", "plain_data", "ROWS", 100, 10, 64, -1),
            SizeRow("plain", "plain_log", "LOG", 50, 5, 64, 2_097_152),
        };

        using var doc = System.Text.Json.JsonDocument.Parse(McpServerInfoTools.DatabaseSizesPayload("srv", rows));
        var root = doc.RootElement;

        Assert.False(root.TryGetProperty("note", out _));
        Assert.Equal(new[] { "server", "captured_at", "file_count", "databases" }, root.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal(150d, root.GetProperty("databases")[0].GetProperty("total_size_mb").GetDouble());
    }

    [Fact]
    public void TheSharedNote_NamesTheWordsTheGridShows()
    {
        Assert.Contains("Azure SQL Database Hyperscale", HyperscaleLogSize.Note, StringComparison.Ordinal);
        Assert.Contains(HyperscaleLogSize.Display, HyperscaleLogSize.Note, StringComparison.Ordinal);

        /* People read it above the web Database Sizes table, so it names no JSON field (every one has an underscore). */
        Assert.DoesNotContain("_", HyperscaleLogSize.Note, StringComparison.Ordinal);
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
