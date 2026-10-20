/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The other databases on an Azure SQL Database server each store one row for the whole database: its data size, with
/// no log size, because the server reports none for them. <c>get_database_sizes</c> says so on that row, on its
/// database's entry, and in the top-level note, in the shared words (<see cref="AzureSiblingDatabaseSize.LogNote"/>).
/// No other row has the key. Lite's <c>AzureSiblingSizePayloadTests</c> is the twin.
/// </summary>
public sealed class AzureSiblingSizePayloadTests
{
    private static DarlingObjectStatsReader.DatabaseSizeRow Row(string database, int? fileId, string name, string type, double? total, double? used) =>
        new(new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc), database, name, type, total, used, null, null, null, null, null, fileId);

    private static JsonElement Database(JsonDocument doc, string name) =>
        doc.RootElement.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == name);

    private static string? TopLevelNote(params DarlingObjectStatsReader.DatabaseSizeRow[] rows)
    {
        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));
        return doc.RootElement.TryGetProperty("note", out var note) ? note.GetString() : null;
    }

    [Fact]
    public void ASiblingRow_CarriesTheLogNoteAndItsUsedSize_OnItsFileAndItsDatabase_AndANormalFileRowDoesNot()
    {
        var rows = new[]
        {
            Row("sibdb", null, AzureSiblingDatabaseSize.FileName, "ROWS", 10_240, 119),
            Row("appdb", 1, "appdb_data", "ROWS", 100, 10),
            Row("appdb", 2, "appdb_log", "LOG", 50, 5),
        };

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));

        var sibling = Database(doc, "sibdb");
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, sibling.GetProperty(AzureSiblingDatabaseSize.RowNoteKey).GetString());
        Assert.Equal(119d, sibling.GetProperty("used_size_mb").GetDouble());
        var siblingFile = Assert.Single(sibling.GetProperty("files").EnumerateArray());
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, siblingFile.GetProperty(AzureSiblingDatabaseSize.RowNoteKey).GetString());
        Assert.Equal(10_240d, siblingFile.GetProperty("total_size_mb").GetDouble());
        Assert.Equal(119d, siblingFile.GetProperty("used_size_mb").GetDouble());

        /* A database with real files gets no new key, on itself or on any of its files. */
        var normal = Database(doc, "appdb");
        Assert.False(normal.TryGetProperty(AzureSiblingDatabaseSize.RowNoteKey, out _));
        Assert.All(normal.GetProperty("files").EnumerateArray(), f => Assert.False(f.TryGetProperty(AzureSiblingDatabaseSize.RowNoteKey, out _)));
    }

    [Fact]
    public void TheTopLevelNote_SaysTheLogIsNotAvailable_AndJoinsTheHyperscaleSentence_WhenBothApply()
    {
        var sibling = Row("sibdb", null, AzureSiblingDatabaseSize.FileName, "ROWS", 10_240, 119);
        var hyperscaleData = Row("hsdb", 1, "hsdb_data", "ROWS", 10_240, 315);
        var hyperscaleLog = Row("hsdb", 2, "hsdb_log", "LOG", null, 40);
        var plain = Row("appdb", 1, "appdb_data", "ROWS", 100, 10);

        Assert.Equal(AzureSiblingDatabaseSize.LogNote, TopLevelNote(sibling, plain));
        Assert.Equal(HyperscaleLogSize.Note, TopLevelNote(hyperscaleData, hyperscaleLog, plain));
        Assert.Equal(HyperscaleLogSize.Note + " " + AzureSiblingDatabaseSize.LogNote, TopLevelNote(hyperscaleData, hyperscaleLog, sibling));
        Assert.Null(TopLevelNote(plain));
    }

    [Fact]
    public void ANullUsedSize_IsNullOnTheFileAndOnItsDatabase_NotZero()
    {
        /* The old shape: the used space sits in the total and the used size is empty. A file with an id and the
           sibling name is a real file, so it is neither noted nor matched. */
        var rows = new[]
        {
            Row("olddb", null, AzureSiblingDatabaseSize.FileName, "ROWS", 119, null),
            Row("oddb", 3, AzureSiblingDatabaseSize.FileName, "ROWS", 5, 1),
        };

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));

        var old = Database(doc, "olddb");
        Assert.Equal(JsonValueKind.Null, old.GetProperty("used_size_mb").ValueKind);
        Assert.Equal(JsonValueKind.Null, old.GetProperty("files")[0].GetProperty("used_size_mb").ValueKind);
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, old.GetProperty(AzureSiblingDatabaseSize.RowNoteKey).GetString());
        Assert.False(Database(doc, "oddb").TryGetProperty(AzureSiblingDatabaseSize.RowNoteKey, out _));
        Assert.True(rows[0].IsAzureSiblingRow);
        Assert.False(rows[1].IsAzureSiblingRow);
    }

    [Fact]
    public void TheLatestSnapshotSql_SelectsTheFileId_AsTheColumnTheReaderMapsLast()
    {
        var sql = DarlingObjectStatsReader.DatabaseSizeLatestSql;

        /* #5633 item 6 appended the percent-growth pair AFTER file_id, so file_id is no longer the last column. The
           reader maps by ordinal (file_id at 11, then is_percent_growth 12, growth_pct 13); this pins that exact
           order, so a column slipped in before file_id would shift the mapping and fail here. */
        Assert.Matches(new Regex(@"volume_free_mb,\s*file_id,\s*is_percent_growth,\s*CAST\(growth_pct AS double precision\) AS growth_pct\s*FROM v_database_size_stats"), sql);
    }

    [Fact]
    public void TheSharedWords_AreThePlainTextOfTheFileIoNoteKey_AndNameNoJsonField()
    {
        Assert.Equal("size_note", AzureSiblingDatabaseSize.RowNoteKey);
        Assert.StartsWith("Log size: n/a", AzureSiblingDatabaseSize.LogNote, StringComparison.Ordinal);
        /* People read it on the web page, so no JSON field name (every one has an underscore). */
        Assert.DoesNotContain("_", AzureSiblingDatabaseSize.LogNote, StringComparison.Ordinal);
    }

    /// <summary>#6178: on Hyperscale the data file's max size is a per-file cap. Its row says so under the same note key the log
    /// and sibling rows use; a data file of any other database, and the log row, carry no such note.</summary>
    [Fact]
    public void AHyperscaleDataFile_SaysItsMaxSizeIsAPerFileCap_AndNoOtherFileDoes()
    {
        var rows = new[]
        {
            Row("hsdb", 1, "hsdb_data", "ROWS", 10_240, 315) with { MaxSizeMb = 131_072 },
            Row("hsdb", 2, "hsdb_log", "LOG", null, 40) with { MaxSizeMb = 2_097_152 },
            Row("appdb", 1, "appdb_data", "ROWS", 100, 10) with { MaxSizeMb = 131_072 },
        };

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));

        var hsFiles = Database(doc, "hsdb").GetProperty("files").EnumerateArray().ToList();
        Assert.Equal(HyperscaleLogSize.DataFileMaxSizeNote, hsFiles[0].GetProperty(AzureSiblingDatabaseSize.RowNoteKey).GetString());
        Assert.False(hsFiles[1].TryGetProperty(AzureSiblingDatabaseSize.RowNoteKey, out _));
        Assert.All(Database(doc, "appdb").GetProperty("files").EnumerateArray(), f => Assert.False(f.TryGetProperty(AzureSiblingDatabaseSize.RowNoteKey, out _)));
        Assert.Contains("one file", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
    }
}
