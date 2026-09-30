/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Azure SQL Database Hyperscale keeps its transaction log in the log service. <c>sys.database_files</c> still lists a
/// LOG row for it, sized at about 1 TB (1,046,528 MB), which is not storage the database holds or pays for: Hyperscale
/// bills allocated data storage. The rule these pins hold, in Darling's half: the collector (shared with Lite) stores a
/// NULL size for that one row, the Postgres store keeps it NULL, every reader shows it as "n/a (log service)", and it
/// stays out of every allocated total. The data file keeps its real size. SQL Server, Managed Instance and
/// non-Hyperscale Azure SQL Database are unchanged. Lite's <c>HyperscaleLogSize*Tests</c> are the twins.
/// </summary>
public sealed class HyperscaleLogSizeTests
{
    private static CollectorContext Context(bool azure) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
    };

    [Fact]
    public void AzureSqlDbQuery_NullsTheSizeOfTheHyperscaleLogRowOnly_AndKeepsTheRealSizeOtherwise()
    {
        var text = DatabaseSizeStatsCollector.Instance.BuildQuery(Context(azure: true)).Text;

        Assert.Contains("CONVERT(nvarchar(64), DATABASEPROPERTYEX(DB_NAME(), N'Edition')) = N'Hyperscale'", text, StringComparison.Ordinal);
        Assert.Matches(new Regex(@"is_log_service =\s*CASE\s*WHEN @is_hyperscale = 1\s*AND\s+df\.type = 1 /\*LOG\*/\s*THEN CONVERT\(bit, 1\)\s*ELSE CONVERT\(bit, 0\)"), text);
        Assert.Matches(new Regex(@"total_size_mb =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(decimal\(19,2\), NULL\)\s*ELSE CONVERT\(decimal\(19,2\), df\.size \* 8\.0 / 1024\.0\)\s*END"), text);
        Assert.Matches(new Regex(@"max_size_mb =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(decimal\(19,2\), NULL\)"), text);
        Assert.Matches(new Regex(@"auto_growth_mb =\s*CASE\s*WHEN ls\.is_log_service = 1\s*THEN CONVERT\(decimal\(19,2\), NULL\)"), text);
        Assert.Contains("CONVERT(decimal(19,2), FILEPROPERTY(df.name, N'SpaceUsed') * 8.0 / 1024.0)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPremQuery_IsUnchanged_NoHyperscaleBranch()
    {
        var text = DatabaseSizeStatsCollector.Instance.BuildQuery(Context(azure: false)).Text;

        Assert.DoesNotContain("Hyperscale", text, StringComparison.Ordinal);
        Assert.DoesNotContain("is_log_service", text, StringComparison.Ordinal);
        Assert.Contains("CONVERT(decimal(19,2), df.size * 8.0 / 1024.0)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void PostgresStore_KeepsTotalSizeNullable()
    {
        /* Postgres emits every payload column nullable, so the NULL size needs no migration here; this holds it
           that way (Lite's DuckDB store needed its v67 rung for the same row). */
        var ddl = PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance);

        var line = ddl.Split('\n').Single(l => l.TrimStart().StartsWith("total_size_mb ", StringComparison.Ordinal));
        Assert.DoesNotContain("NOT NULL", line, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerRow_HyperscaleLogRow_ReadsAsNotApplicable_AndOutOfTheAllocatedTotals_WhileTheDataRowCounts()
    {
        var rows = new List<DatabaseSizeRow>
        {
            new() { DatabaseName = "hsdb", FileTypeDesc = "ROWS", FileName = "hsdb_data", TotalSizeMb = 10_240m, UsedSizeMb = 315m },
            new() { DatabaseName = "hsdb", FileTypeDesc = "LOG", FileName = "hsdb_log", TotalSizeMb = null, UsedSizeMb = 40m },
        };

        Assert.Null(rows[1].FreeSpaceMb);
        Assert.Null(rows[1].UsedPct);
        Assert.Equal(40m, rows[1].UsedSizeMb);

        Assert.Equal(10_240m, DatabaseSizeRow.AllocatedTotalMb(rows));
        Assert.Equal(10_240m - 315m, DatabaseSizeRow.FreeTotalMb(rows));

        /* Unchanged: a non-Hyperscale database (or a SQL Server one) has a real size on its log row, which counts. */
        var plain = new List<DatabaseSizeRow>
        {
            new() { FileTypeDesc = "ROWS", TotalSizeMb = 100m, UsedSizeMb = 10m },
            new() { FileTypeDesc = "LOG", TotalSizeMb = 50m, UsedSizeMb = 5m },
        };
        Assert.Equal(150m, DatabaseSizeRow.AllocatedTotalMb(plain));
        Assert.Equal(135m, DatabaseSizeRow.FreeTotalMb(plain));
        Assert.Equal(10.0m, plain[0].UsedPct);
    }

    [Fact]
    public void ViewerGrid_WordsANullSize_AsTheSharedHyperscaleText_AndSortsNullSizesLast()
    {
        Assert.Equal("n/a (log service)", HyperscaleLogSize.Display);

        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml"));
        Assert.Contains("{Binding TotalSizeMb, StringFormat=N2, TargetNullValue='" + HyperscaleLogSize.Display + "'}", xaml, StringComparison.Ordinal);

        /* Postgres sorts NULL first under DESC, which would put the log-service row at the top of a grid whose
           lead is the biggest files. */
        Assert.Contains("ORDER BY total_size_mb DESC NULLS LAST,", ViewerDataService.DatabaseSizeLatestSql, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerUtilizationChart_UsedIsSummedOnlyOverTheFilesWhoseSizeCounts()
    {
        Assert.Contains("SUM(CASE WHEN total_size_mb IS NOT NULL THEN used_size_mb END) AS used_mb", ViewerDataService.DatabaseSizeSummarySql, StringComparison.Ordinal);
    }

    private static DarlingObjectStatsReader.DatabaseSizeRow SizeRow(string database, string name, string fileType, double? total, double used, double? growth, double? max) =>
        new(new DateTime(2026, 9, 30, 12, 0, 0, DateTimeKind.Utc), database, name, fileType, total, used, growth, max, null, null, null);

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

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));
        var root = doc.RootElement;

        Assert.Equal(HyperscaleLogSize.Note, root.GetProperty("note").GetString());

        var hs = root.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == "hsdb");
        Assert.Equal(10_240d, hs.GetProperty("total_size_mb").GetDouble());
        /* Used sums over the same files as the total, so the log file's own used space (40) stays out of it. */
        Assert.Equal(315d, hs.GetProperty("used_size_mb").GetDouble());
        var hsLog = hs.GetProperty("files").EnumerateArray().Single(f => f.GetProperty("file_type").GetString() == "LOG");
        Assert.Equal(JsonValueKind.Null, hsLog.GetProperty("total_size_mb").ValueKind);
        Assert.Equal(JsonValueKind.Null, hsLog.GetProperty("auto_growth_mb").ValueKind);
        Assert.Equal(JsonValueKind.Null, hsLog.GetProperty("max_size_mb").ValueKind);
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

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));
        var root = doc.RootElement;

        Assert.False(root.TryGetProperty("note", out _));
        Assert.Equal(new[] { "server", "captured_at", "file_count", "databases" }, root.EnumerateObject().Select(p => p.Name).ToArray());
        Assert.Equal(150d, root.GetProperty("databases")[0].GetProperty("total_size_mb").GetDouble());
    }

    [Fact]
    public void WebDatabaseSizesTable_RendersTheReadsNote()
    {
        var js = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

        /* The table() helper's ninth argument is the noteKey: the read's own note rides above the rows. */
        var block = js.Substring(js.IndexOf("\"Database Sizes\",", StringComparison.Ordinal));
        block = block.Substring(0, block.IndexOf("),", StringComparison.Ordinal));
        Assert.Contains("\"get_database_sizes\"", block, StringComparison.Ordinal);
        Assert.Matches(new Regex("1,\\s*\"note\""), block);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
