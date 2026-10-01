/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Lite.Tests.Helpers;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The other databases on an Azure SQL Database server each store one row for the whole database: its data size, with
/// no log size, because the server reports none for them. <c>get_database_sizes</c> says so on that row, on its
/// database's entry, and in the top-level note, in the shared words (<see cref="AzureSiblingDatabaseSize.LogNote"/>).
/// No other row has the key. Darling's <c>AzureSiblingSizePayloadTests</c> is the twin.
/// </summary>
public sealed class AzureSiblingSizePayloadTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int ServerId = 4911;

    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public AzureSiblingSizePayloadTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    private static readonly DateTime Collected = DateTime.SpecifyKind(
        new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    private static DatabaseSizeStatsRow Row(string database, int? fileId, string name, string type, double? total, double? used) => new()
    {
        DatabaseName = database,
        FileId = fileId,
        FileName = name,
        FileTypeDesc = type,
        TotalSizeMb = total,
        UsedSizeMb = used,
        CollectionTime = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc),
    };

    private static JsonElement Database(JsonDocument doc, string name) =>
        doc.RootElement.GetProperty("databases").EnumerateArray().Single(d => d.GetProperty("database_name").GetString() == name);

    private static string? TopLevelNote(params DatabaseSizeStatsRow[] rows)
    {
        using var doc = JsonDocument.Parse(McpServerInfoTools.DatabaseSizesPayload("srv", rows));
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

        using var doc = JsonDocument.Parse(McpServerInfoTools.DatabaseSizesPayload("srv", rows));

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
    public void TheSharedWords_AreThePlainTextOfTheFileIoNoteKey_AndNameNoJsonField()
    {
        Assert.Equal("size_note", AzureSiblingDatabaseSize.RowNoteKey);
        Assert.StartsWith("Log size: n/a", AzureSiblingDatabaseSize.LogNote, StringComparison.Ordinal);
        /* People read it on the web page, so no JSON field name (every one has an underscore). */
        Assert.DoesNotContain("_", AzureSiblingDatabaseSize.LogNote, StringComparison.Ordinal);
    }

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
VALUES ($1, $2, $3, 'SibSrv', $4, $5, $6, 'ROWS', $7, $8, $9, $10)";
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
    public async Task TheStoreRead_KeepsANullUsedSizeNull_AndTakesOnlyAFileWithNoIdForASibling()
    {
        /* The old shape (the used space in the total, no used size), the new shape, a real file, and a real file
           that happens to carry the sibling name: it has a file id, so it is not a sibling. */
        await SeedAsync("olddb", null, AzureSiblingDatabaseSize.FileName, 119, null);
        await SeedAsync("newdb", null, AzureSiblingDatabaseSize.FileName, 10_240, 119);
        await SeedAsync("appdb", 1, "appdb_data", 100, 10);
        await SeedAsync("oddb", 3, AzureSiblingDatabaseSize.FileName, 5, 1);

        var rows = await new LocalDataService(_duckDb).GetLatestDatabaseSizeStatsAsync(ServerId);

        var old = Assert.Single(rows, r => r.DatabaseName == "olddb");
        Assert.Null(old.UsedSizeMb);
        Assert.Null(old.FileId);
        Assert.True(old.IsAzureSiblingRow);
        var current = Assert.Single(rows, r => r.DatabaseName == "newdb");
        Assert.Equal(119d, current.UsedSizeMb);
        Assert.True(current.IsAzureSiblingRow);
        var app = Assert.Single(rows, r => r.DatabaseName == "appdb");
        Assert.Equal(1, app.FileId);
        Assert.False(app.IsAzureSiblingRow);
        var odd = Assert.Single(rows, r => r.DatabaseName == "oddb");
        Assert.Equal(3, odd.FileId);
        Assert.False(odd.IsAzureSiblingRow);

        /* An unknown used size is null on the file and on its database, never 0 MB. */
        using var doc = JsonDocument.Parse(McpServerInfoTools.DatabaseSizesPayload("srv", rows));
        var oldDb = Database(doc, "olddb");
        Assert.Equal(JsonValueKind.Null, oldDb.GetProperty("used_size_mb").ValueKind);
        Assert.Equal(JsonValueKind.Null, oldDb.GetProperty("files")[0].GetProperty("used_size_mb").ValueKind);
        Assert.False(Database(doc, "oddb").TryGetProperty(AzureSiblingDatabaseSize.RowNoteKey, out _));
    }
}
