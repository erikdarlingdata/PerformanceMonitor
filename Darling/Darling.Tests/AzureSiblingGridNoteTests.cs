/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The FinOps Database Sizes grid for the other databases on an Azure SQL Database server. Each one is a single row
/// that holds its data size only; the server reports no log size for it. The row says so in a Note column
/// (<see cref="AzureSiblingDatabaseSize.LogNote"/>), and the caption over the grid says those rows are data space only
/// (<see cref="AzureSiblingDatabaseSize.GridCaption"/>). A normal file row has no note, and a grid with no such row keeps
/// its caption. Lite.Tests' <c>AzureSiblingGridNoteTests</c> is the twin.
/// </summary>
public sealed class AzureSiblingGridNoteTests
{
    private static DatabaseSizeRow Row(int? fileId, string fileName, decimal? total = 10m, decimal? used = null) => new()
    {
        DatabaseName = "d",
        FileId = fileId,
        FileName = fileName,
        TotalSizeMb = total,
        UsedSizeMb = used,
    };

    [Fact]
    public void TheGridRead_SelectsTheFileId_AndReadsItLast_SoNoOtherColumnMoves()
    {
        var sql = ViewerDataService.DatabaseSizeLatestSql;
        Assert.Contains("vlf_count,\n    file_id\nFROM v_database_size_stats", sql.Replace("\r\n", "\n", StringComparison.Ordinal), StringComparison.Ordinal);

        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.FinOps.Storage.cs");
        Assert.Contains("FileId = reader.IsDBNull(13) ? null : Convert.ToInt32(reader.GetValue(13))", source, StringComparison.Ordinal);
    }

    [Fact]
    public void ASiblingRow_CarriesTheLogNote_AndAFileRowDoesNot_WithoutChangingTheSizeMath()
    {
        var current = Row(null, AzureSiblingDatabaseSize.FileName, 10_240m, 119m);
        Assert.True(current.IsAzureSiblingRow);
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, current.Note);
        Assert.Equal(10_240m - 119m, current.FreeSpaceMb);
        Assert.Equal(1.2m, current.UsedPct);

        var old = Row(null, AzureSiblingDatabaseSize.FileName, 119m, null);
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, old.Note);
        Assert.Null(old.FreeSpaceMb);

        var file = Row(1, "d_data", 100m, 10m);
        Assert.False(file.IsAzureSiblingRow);
        Assert.Null(file.Note);
        Assert.Equal(90m, file.FreeSpaceMb);

        /* A real file that carries the sibling name has a file id, so it is not a sibling. */
        Assert.Null(Row(3, AzureSiblingDatabaseSize.FileName).Note);
    }

    [Fact]
    public void TheCaption_SaysDataSpaceOnly_OnlyWhenTheGridHoldsASiblingRow()
    {
        var file = Row(1, "d_data");
        var sibling = Row(null, AzureSiblingDatabaseSize.FileName);

        Assert.Equal("2 file(s). " + AzureSiblingDatabaseSize.GridCaption, DatabaseSizeRow.Caption([file, sibling]));
        Assert.Equal("1 file(s)", DatabaseSizeRow.Caption([file]));
        Assert.Equal("", DatabaseSizeRow.Caption([]));
    }

    [Fact]
    public void TheCaptionAndTheNote_AreTheSharedWords_AndThePageBindsAndUsesThem()
    {
        Assert.StartsWith("Rows named (whole database) hold data space only", AzureSiblingDatabaseSize.GridCaption, StringComparison.Ordinal);
        Assert.EndsWith("their log size is not reported.", AzureSiblingDatabaseSize.GridCaption, StringComparison.Ordinal);

        var xaml = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.xaml");
        var grid = xaml[xaml.IndexOf("x:Name=\"FinOpsDatabaseSizesDataGrid\"", StringComparison.Ordinal)..];
        grid = grid[..grid.IndexOf("</DataGrid>", StringComparison.Ordinal)];
        Assert.Contains("Header=\"Note\" Binding=\"{Binding Note}\"", grid, StringComparison.Ordinal);

        var code = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "FinOpsTab.Loaders.cs");
        Assert.Contains("FinOpsDbSizeCountIndicator.Text = DatabaseSizeRow.Caption(data);", code, StringComparison.Ordinal);
    }
}
