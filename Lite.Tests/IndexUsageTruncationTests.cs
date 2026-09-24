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
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2636, brought to Lite parity with Darling: <c>get_index_usage</c>'s ordering puts Unused/Write-only
/// indexes ahead of Active ones SERVER-WIDE, and the cap used to be a hardcoded 200 with no
/// <c>database_name</c> filter and no signal that a full page had been cut. On an instance with 200+ unused
/// indexes in one legacy database, that database could consume the entire answer while every Active index
/// elsewhere stayed invisible — indistinguishable from a collection failure.
///
/// <para>Darling's twin is <c>Darling.Tests/IndexUsageTruncationTests</c>, split into a SQL-text-pin half and
/// a live-Postgres behavioral half. Lite's reader builds its optional filter as literal SQL (the
/// <see cref="LocalDataService.GetIndexLockingAsync"/> pattern) rather than a parameterized statement a
/// reflection pin can read, and the shared DuckDB fixture needs no live server and no skip-when-absent, so
/// this pins the same four claims — the filter, the cap, truncated, and the count — directly against it.</para>
/// </summary>
public sealed class IndexUsageTruncationTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "IndexUsageTruncationSrv";
    private const string Asked = "AskedAbout";
    private const string Louder = "LouderNeighbour";

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly DateTime _startTime = DateTime.UtcNow.AddDays(-10);
    private DuckDBConnection? _seedConn;
    private long _nextId = -1;

    public IndexUsageTruncationTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-indexusage-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    /// <summary>One capture, two databases, three indexes: a never-read index in the alphabetically-first,
    /// SMALLEST database (so size cannot be what puts it first), and two actively-read ones in the other —
    /// Darling's <c>PlantTwoDatabasesAsync</c> fixture, ported. ALL THREE share one <c>collection_time</c>
    /// (a single capture, like Darling's <c>capture</c> reused across its <c>PlantIndexAsync</c> calls) —
    /// the "latest snapshot" read is a correlated <c>MAX(collection_time)</c>, so three rows minted a
    /// microsecond apart at <see cref="DateTime.UtcNow"/> each would leave only the newest as "latest".</summary>
    private async Task PlantTwoDatabasesAsync()
    {
        var capture = DateTime.UtcNow;
        await InsertIndexAsync(capture, Asked, "IX_NeverRead", objectId: 100, indexId: 2, reservedMb: 1m, seeks: 0);
        await InsertIndexAsync(capture, Louder, "IX_HotOne", objectId: 200, indexId: 1, reservedMb: 5000m, seeks: 900_000);
        await InsertIndexAsync(capture, Louder, "IX_HotTwo", objectId: 201, indexId: 1, reservedMb: 4000m, seeks: 800_000);
    }

    private async Task InsertIndexAsync(DateTime capture, string db, string indexName, int objectId, int indexId, decimal? reservedMb, long seeks)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"INSERT INTO index_object_stats
            (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
             schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
             user_seeks, user_scans, user_lookups, user_updates)
            VALUES ($1,$2,$3,$4,$5,$6,7,'dbo',$7,$8,$9,$10,'NONCLUSTERED',$11,$11,1000000,$12,0,0,0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--); P(capture); P(_serverId); P(ServerName); P(_startTime); P(db);
        P(objectId); P("T" + objectId.ToString(System.Globalization.CultureInfo.InvariantCulture));
        P(indexId); P(indexName); P((object?)reservedMb ?? DBNull.Value); P(seeks);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /* ---- the filter ---- */

    [Fact]
    public async Task NullDatabaseNameAnswersAboutEveryDatabase_ANameAnswersAboutOne()
    {
        await PlantTwoDatabasesAsync();

        var everyDatabase = await _dataService.GetIndexUsageAsync(_serverId, topN: 100, databaseName: null);
        Assert.Equal(
            new[] { Asked, Louder },
            everyDatabase.Select(r => r.DatabaseName).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());

        var oneDatabase = await _dataService.GetIndexUsageAsync(_serverId, topN: 100, databaseName: Asked);
        Assert.Equal(new[] { Asked }, oneDatabase.Select(r => r.DatabaseName).Distinct().ToArray());
        Assert.NotEmpty(oneDatabase);

        /* The row sets differ, which is the whole claim — a filter that silently did nothing would return
           the same set for both calls and satisfy every assertion above except this one. */
        Assert.True(
            oneDatabase.Count < everyDatabase.Count,
            $"the filtered read returned {oneDatabase.Count} of {everyDatabase.Count} rows — the database "
            + "filter is not narrowing anything");

        /* And the count the truncation notice divides by is the count of the SAME question. */
        Assert.Equal(oneDatabase.Count, await _dataService.GetIndexUsageMatchCountAsync(_serverId, Asked));
        Assert.Equal(everyDatabase.Count, await _dataService.GetIndexUsageMatchCountAsync(_serverId, null));
    }

    /* ---- the ordering + the cap ---- */

    [Fact]
    public async Task AnUnusedIndexComesBackAheadOfAUsedOne_AndTheCapTruncatesToTheUnusedEnd()
    {
        await PlantTwoDatabasesAsync();

        var rows = await _dataService.GetIndexUsageAsync(_serverId, topN: 100, databaseName: null);
        Assert.Equal("Unused", rows[0].Classification);
        Assert.Equal("IX_NeverRead", rows[0].IndexName);

        /* The cap is what made the reporter's database invisible, so assert it truncates to the unused end
           rather than to whatever the store happened to return first. */
        var capped = await _dataService.GetIndexUsageAsync(_serverId, topN: 1, databaseName: null);
        Assert.Equal("IX_NeverRead", Assert.Single(capped).IndexName);
    }

    /* ---- ties (#4134): the order, and so what a cap keeps, must not depend on scan order ---- */

    [Fact]
    public async Task TiedSizesBreakByName_AndAMissingSizeSortsLast_SoACappedPageIsStable()
    {
        var capture = DateTime.UtcNow;
        /* All three are Active (seeks > 0) and inserted out of order: two tie on reserved_mb, one has no size. */
        await InsertIndexAsync(capture, Louder, "IX_Unsized", objectId: 302, indexId: 4, reservedMb: null, seeks: 10);
        await InsertIndexAsync(capture, Louder, "IX_TieB", objectId: 300, indexId: 3, reservedMb: 50m, seeks: 10);
        await InsertIndexAsync(capture, Asked, "IX_TieA", objectId: 301, indexId: 3, reservedMb: 50m, seeks: 10);

        var rows = await _dataService.GetIndexUsageAsync(_serverId, topN: 100, databaseName: null);
        Assert.Equal(new[] { "IX_TieA", "IX_TieB", "IX_Unsized" }, rows.Select(r => r.IndexName).ToArray());

        /* A cap of 1 keeps the same row on every call: the first by name among the tied sizes. */
        var capped = await _dataService.GetIndexUsageAsync(_serverId, topN: 1, databaseName: null);
        Assert.Equal("IX_TieA", Assert.Single(capped).IndexName);
    }

    /* ---- the count, taken before the cap ---- */

    [Fact]
    public async Task TheMatchCountIsTakenBeforeTheCap_NotOverTheReturnedRows()
    {
        await PlantTwoDatabasesAsync();

        var capped = await _dataService.GetIndexUsageAsync(_serverId, topN: 1, databaseName: null);
        Assert.Single(capped);

        /* A count taken over the returned rows would equal what was returned on every call — the exact
           mistake #2636 reports, reimplemented one layer down. */
        var matching = await _dataService.GetIndexUsageMatchCountAsync(_serverId, null);
        Assert.Equal(3, matching);
        Assert.True(matching > capped.Count);
    }

    /* ---- the tool: truncated, matching_index_count, and the empty-vs-unavailable status routes ---- */

    [Fact]
    public async Task Tool_ReportsMatchingCountAndTruncated_WhenTheCapCutsTheAnswer()
    {
        await PlantTwoDatabasesAsync();

        var capped = Root(await McpObjectStatsTools.GetIndexUsage(_dataService, _serverManager, ServerName, limit: 1));
        Assert.Equal(1, capped.GetProperty("returned_index_count").GetInt32());
        Assert.Equal(3, capped.GetProperty("matching_index_count").GetInt32());
        Assert.True(capped.GetProperty("truncated").GetBoolean());
        Assert.Contains("TRUNCATED", capped.GetProperty("note").GetString(), StringComparison.Ordinal);

        var complete = Root(await McpObjectStatsTools.GetIndexUsage(_dataService, _serverManager, ServerName, limit: 100));
        Assert.Equal(3, complete.GetProperty("returned_index_count").GetInt32());
        Assert.False(complete.GetProperty("truncated").GetBoolean());
        Assert.StartsWith("Complete", complete.GetProperty("note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Tool_DatabaseFilterMatchingNothing_IsStatusEmpty_NotUnavailable_WhenServerHasDataElsewhere()
    {
        await PlantTwoDatabasesAsync();

        var root = Root(await McpObjectStatsTools.GetIndexUsage(_dataService, _serverManager, ServerName, database_name: "NoSuchDatabase"));
        Assert.Equal("empty", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("NoSuchDatabase", message, StringComparison.Ordinal);
        Assert.Contains("get_database_sizes", message, StringComparison.Ordinal);
    }

    private static JsonElement Root(string json) => JsonDocument.Parse(json).RootElement;
}
