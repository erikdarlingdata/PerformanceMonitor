/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The log file of an Azure SQL Database Hyperscale database lives in the log service, so the collector stores NO
/// size for it (a NULL <c>size_mb</c>) and every surface that shows a File I/O size says "n/a (log service)" in
/// place of a number. This is Darling's half: the <c>get_file_io_stats</c> row, and the web table that renders it.
/// Lite's half is <c>FileIoHyperscaleLogSizeTests</c> in Lite.Tests, and both take the text from
/// <see cref="FileIoStatsCollector.NoSizeLabel"/>.
///
/// <para>The row projection is built without a Postgres store, so these run everywhere. The web table is pinned as
/// source, like the other page pins.</para>
/// </summary>
public sealed class FileIoHyperscaleLogSizeTests
{
    private static readonly string[] PanelsJsPath =
        ["Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "panels.js"];

    private static readonly string[] ServerTabsJsPath =
        ["Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"];

    private static DarlingDataReader.FileIoRow Row(string file, string type, double? sizeMb) => new(
        "AppDb", file, type, "F:\\AppDb", sizeMb, DeltaReads: 10, DeltaWrites: 5, DeltaReadBytes: 81920,
        DeltaWriteBytes: 40960, DeltaStallReadMs: 50, DeltaStallWriteMs: 10, SampleIntervalSeconds: 60);

    private static JsonElement Payload(DarlingDataReader.FileIoRow row) =>
        JsonDocument.Parse(JsonSerializer.Serialize(DarlingMcpDataTools.FileIoRowPayload(row), McpHelpers.JsonOptions)).RootElement;

    /// <summary>
    /// A log row with no size reports <c>size_mb</c> as null beside a short <c>size_note</c>, and a data row beside it
    /// keeps its size and has a null note. The same two fields Lite's payload carries.
    /// </summary>
    [Fact]
    public void TheMcpRow_ForAHyperscaleLogFile_HasNoSizeAndTheNote_AndADataFileKeepsItsSize()
    {
        var log = Payload(Row("AppDb_log", "LOG", sizeMb: null));
        var data = Payload(Row("AppDb_data", "ROWS", sizeMb: 112.04));

        Assert.Equal(JsonValueKind.Null, log.GetProperty("size_mb").ValueKind);
        Assert.Equal("n/a (log service)", log.GetProperty("size_note").GetString());

        Assert.Equal(112.0, data.GetProperty("size_mb").GetDouble());
        Assert.Equal(JsonValueKind.Null, data.GetProperty("size_note").ValueKind);
    }

    [Fact]
    public void TheRowNote_IsTheSharedLabel_AndOnlyWhenThereIsNoSize()
    {
        Assert.Equal(FileIoStatsCollector.NoSizeLabel, Row("AppDb_log", "LOG", sizeMb: null).SizeNote);
        Assert.Equal("n/a (log service)", FileIoStatsCollector.NoSizeLabel);

        Assert.Null(Row("AppDb_data", "ROWS", sizeMb: 112).SizeNote);
        /* A genuine size of 0 is still a size: only a missing one says why. */
        Assert.Null(Row("AppDb_data", "ROWS", sizeMb: 0).SizeNote);
    }

    /// <summary>
    /// The latest-snapshot statement passes a NULL size through as NULL. Mapping it to 0 would print a confident
    /// "0 MB" for the log file and hide the reason, and the size facts would drop the row for the wrong cause.
    /// This pins the statement only. The C# read of that column is pinned by
    /// <c>DarlingMcpDataToolsLivePostgresTests</c>, which plants a log row with no size and needs a Postgres store
    /// (<c>DARLING_TEST_PG</c>), so it runs in CI.
    /// </summary>
    [Fact]
    public void TheLatestSnapshotSql_PassesANullSizeThrough()
    {
        var sql = DarlingDataReader.LatestFileIoStatsSql;

        /* NULL numeric casts to NULL double precision. The size is selected on a line of its own and is only
           cast: a COALESCE or CASE around it would turn the missing size back into a number. */
        var sizeLine = Assert.Single(sql.Split('\n'), l => l.Contains("size_mb", StringComparison.Ordinal));
        Assert.Equal("CAST(size_mb AS double precision),", sizeLine.Trim());
    }

    /// <summary>
    /// The web File I/O table shows the server's note in place of the bare dash. The Size column names
    /// <c>size_note</c> as its <c>nullKey</c>, and the cell renderer reads that field of the same row only when the
    /// column's own value is null. The page writes no sentence of its own: the text arrives in the payload.
    /// </summary>
    [Fact]
    public void TheWebFileIoTable_ShowsTheServersNote_WhereTheSizeIsNull()
    {
        var tabs = ReadRepoFile(ServerTabsJsPath);
        var columns = Slice(tabs, "const FILE_IO_COLUMNS = [", "];");
        Assert.Contains("key: \"size_mb\"", columns, StringComparison.Ordinal);
        Assert.Contains("nullKey: \"size_note\"", columns, StringComparison.Ordinal);
        Assert.DoesNotContain("n/a (log service)", tabs, StringComparison.Ordinal);

        var panels = ReadRepoFile(PanelsJsPath);
        var cell = Slice(panels, "function cell(row, c) {", "function vizStat(");
        Assert.Contains("c.nullKey", cell, StringComparison.Ordinal);
        Assert.Contains("raw == null && c.nullKey", cell, StringComparison.Ordinal);
    }

    /// <summary>
    /// No File I/O total includes the row: the database-size fact sums the latest <c>size_mb</c> and keeps only rows
    /// where it is above zero, which a NULL is not.
    /// </summary>
    [Fact]
    public void TheDatabaseSizeFact_SkipsARowWithNoSize()
    {
        Assert.Contains("size_mb > 0", PgFactCollector.DatabaseSizeSql, StringComparison.Ordinal);
    }

    private static string Slice(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found: {start}");
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor not found after {start}: {end}");
        return source[from..to];
    }
}
