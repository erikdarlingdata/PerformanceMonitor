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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The FinOps Database Sizes grid for the other databases on an Azure SQL Database server. Each one is a single row
/// that holds its data size only; the server reports no log size for it. The row says so in a Note column
/// (<see cref="AzureSiblingDatabaseSize.LogNote"/>), and the caption over the grid says those rows are data space only
/// (<see cref="AzureSiblingDatabaseSize.GridCaption"/>). A normal file row has no note, and a grid with no such row keeps
/// its caption. Darling's <c>AzureSiblingGridNoteTests</c> is the twin.
/// </summary>
public sealed class AzureSiblingGridNoteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4921;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AzureSiblingGridNoteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly DateTime Collected = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private async Task SeedAsync(string database, int? fileId, string fileName, double total, double? used)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open) await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO database_size_stats
    (collection_id, collection_time, server_id, server_name, database_name, database_id, file_id, file_type_desc,
     file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1, $2, $3, 'GridSrv', $4, $5, $6, 'ROWS', $7, $8, $9, $10)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = Collected });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerId });
        cmd.Parameters.Add(new DuckDBParameter { Value = database });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileId is null ? (object)DBNull.Value : 7 });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)fileId ?? DBNull.Value });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = fileId is null ? (object)DBNull.Value : @"C:\" + fileName });
        cmd.Parameters.Add(new DuckDBParameter { Value = total });
        cmd.Parameters.Add(new DuckDBParameter { Value = (object?)used ?? DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task TheGridRead_GivesASiblingRowTheLogNote_AndAFileRowNone_WithoutChangingTheSizeMath()
    {
        /* The new shape, the old shape (no used space), a real file, and a real file that carries the sibling name:
           it has a file id, so it is not a sibling. */
        await SeedAsync("newdb", null, AzureSiblingDatabaseSize.FileName, 10_240, 119);
        await SeedAsync("olddb", null, AzureSiblingDatabaseSize.FileName, 119, null);
        await SeedAsync("appdb", 1, "appdb_data", 100, 10);
        await SeedAsync("oddb", 3, AzureSiblingDatabaseSize.FileName, 5, 1);

        var rows = await new LocalDataService(_duckDb).GetDatabaseSizeLatestAsync(ServerId);

        var current = Assert.Single(rows, r => r.DatabaseName == "newdb");
        Assert.True(current.IsAzureSiblingRow);
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, current.Note);
        Assert.Equal(10_240m - 119m, current.FreeSpaceMb);
        Assert.Equal(1.2m, current.UsedPct);

        var old = Assert.Single(rows, r => r.DatabaseName == "olddb");
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, old.Note);
        Assert.Null(old.FreeSpaceMb);

        var app = Assert.Single(rows, r => r.DatabaseName == "appdb");
        Assert.Null(app.Note);
        Assert.Equal(90m, app.FreeSpaceMb);

        Assert.Null(Assert.Single(rows, r => r.DatabaseName == "oddb").Note);
    }

    private static DatabaseSizeRow Row(int? fileId, string fileName) => new() { DatabaseName = "d", FileId = fileId, FileName = fileName, TotalSizeMb = 10m };

    [Fact]
    public void TheCaption_SaysDataSpaceOnly_OnlyWhenTheGridHoldsASiblingRow()
    {
        const string scope = " - Azure SQL Database: scope words";
        var file = Row(1, "d_data");
        var sibling = Row(null, AzureSiblingDatabaseSize.FileName);

        var withSibling = DatabaseSizeRow.Caption([file, sibling], scope);
        Assert.Equal("2 file(s). " + AzureSiblingDatabaseSize.GridCaption, withSibling);
        /* The scope note says a connection sees one database; a grid with a sibling row shows more, so it is left out. */
        Assert.DoesNotContain("scope words", withSibling, StringComparison.Ordinal);

        Assert.Equal("1 file(s)" + scope, DatabaseSizeRow.Caption([file], scope));
        Assert.Equal("1 file(s)", DatabaseSizeRow.Caption([file], ""));
        Assert.Equal("", DatabaseSizeRow.Caption([], scope));
    }

    [Fact]
    public void TheCaptionAndTheNote_AreTheSharedWords_AndThePageBindsAndUsesThem()
    {
        Assert.StartsWith("Rows named (whole database) hold data space only", AzureSiblingDatabaseSize.GridCaption, StringComparison.Ordinal);
        Assert.EndsWith("their log size is not reported.", AzureSiblingDatabaseSize.GridCaption, StringComparison.Ordinal);

        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "FinOpsTab.xaml"));
        var grid = xaml[xaml.IndexOf("x:Name=\"DatabaseSizesDataGrid\"", StringComparison.Ordinal)..];
        grid = grid[..grid.IndexOf("</DataGrid>", StringComparison.Ordinal)];
        Assert.Contains("Header=\"Note\" Binding=\"{Binding Note}\"", grid, StringComparison.Ordinal);

        var code = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Controls", "FinOpsTab.xaml.cs"));
        Assert.Contains("DbSizeCountIndicator.Text = DatabaseSizeRow.Caption(data, scopeNote);", code, StringComparison.Ordinal);
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
