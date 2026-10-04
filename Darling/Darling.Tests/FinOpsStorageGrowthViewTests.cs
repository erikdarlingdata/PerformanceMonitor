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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

public sealed class FinOpsStorageGrowthViewTests
{
    private static StorageGrowthDto Db(string name, decimal? g30, decimal? g7) =>
        new(name, 100m, null, null, g7, g30, null, null, false, false);

    private static FinOpsHeatmapMatrix Matrix(params double[] cells) => new()
    {
        Intensities = new double[1, cells.Length].Also(m => { for (var i = 0; i < cells.Length; i++) m[0, i] = cells[i]; }),
        RowLabels = ["a"],
        Days = Enumerable.Range(0, cells.Length).Select(i => new DateTime(2026, 9, 1).AddDays(i)).ToArray(),
    };

    [Fact]
    public void ServedHead_NamesTheView_AndStaysUnderTheTarget()
    {
        var served = McpToolGuideTests.Served("get_finops");
        Assert.Contains("storage_growth:", served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"served head is {served.Served.Length}");
        Assert.True(DarlingMcpFinOpsTools.StorageGrowthViewLine.Length <= 80);
    }

    [Fact]
    public void GuideTail_CarriesTheViewsKeyFacts()
    {
        var tail = McpToolGuideTests.Served("get_finops").Tail;
        Assert.NotNull(tail);
        foreach (var fact in new[] { "fixed 30 days", "is refused above 20", "up to 100 rows", "up to 40 rows", "[mb, band]", "log1p(mb)", "not UTC", "object_name without database_name" })
            Assert.Contains(fact, tail, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewsAllowList_ContainsTheView() =>
        Assert.Contains("storage_growth", DarlingMcpFinOpsTools.Views);

    [Fact]
    public void OrderDatabases_NullsLast_InBothGrowthKeys_ThenName_FromAReversedInput()
    {
        var input = new[] { Db("z", null, null), Db("b", 5m, 1m), Db("a", 5m, 1m), Db("c", 5m, null), Db("d", 5m, 9m), Db("e", -3m, 0m), Db("y", null, 4m) };
        var ordered = DarlingMcpFinOpsTools.OrderStorageGrowthDatabases(input.Reverse()).Select(r => r.DatabaseName).ToArray();
        Assert.Equal(new[] { "d", "a", "b", "c", "e", "y", "z" }, ordered);
    }

    [Fact]
    public void OrderIndexes_ByIndexId_ThenNameOrdinal_FromAReversedInput()
    {
        IndexUsageDto Ix(int id, string n) => new("d", "s", "t", n, "NONCLUSTERED", id, 1m, 1, 0, 0, 0, 0, 0, null, "Unused");
        var ordered = DarlingMcpFinOpsTools.OrderStorageGrowthIndexes(new[] { Ix(2, "b"), Ix(2, "a"), Ix(1, "z") }.Reverse());
        Assert.Equal(new[] { "z", "a", "b" }, ordered.Select(i => i.IndexName).ToArray());
    }

    [Fact]
    public void RankObjects_CountsNullGrowthAsZero_ThenKeyOrdinal_AndCutsAtLimit()
    {
        ObjectSizeGrowthDto O(string t, decimal? g) => new("dbo", t, 1m, 1m, 1, 1, g, null, null);
        var (ranked, truncated) = DarlingMcpFinOpsTools.RankStorageGrowthObjects(new[] { O("d", -2m), O("c", null), O("b", 0m), O("a", 7m) }.Reverse().ToList(), 3);
        Assert.Equal(new[] { "a", "b", "c" }, ranked.Select(o => o.TableName).ToArray());
        Assert.True(truncated);
    }

    [Fact]
    public void MatrixLogBands_LiteralValues()
    {
        /* log1p scale from the smallest positive cell (band 0) to the largest (band 7): 3 -> ln4, 99 -> ln100; the midpoint of the two logs is 4 * 100 -> sqrt = 19.9 -> band 4. */
        var bands = FinOpsHeatmapBuilder.MatrixLogBands(Matrix(0, 3, 19.9, 99, 50), 8);
        Assert.Null(bands[0, 0]);
        Assert.Equal(0, bands[0, 1]);
        Assert.Equal(4, bands[0, 2]);
        Assert.Equal(7, bands[0, 3]);
        Assert.Equal(6, bands[0, 4]);
    }

    [Fact]
    public void MatrixLogBands_AllEqual_TakeTheTopBand_AndAnEmptyMatrixHasNone()
    {
        var bands = FinOpsHeatmapBuilder.MatrixLogBands(Matrix(5, 0, 5), 8);
        Assert.Equal(7, bands[0, 0]);
        Assert.Null(bands[0, 1]);
        Assert.Equal(7, bands[0, 2]);
        Assert.Null(FinOpsHeatmapBuilder.MatrixLogBands(Matrix(0, 0), 8)[0, 1]);
    }

    [Fact]
    public void MatrixLogBands_NormalisesFromTheSmallestPositive_NotFromZero()
    {
        /* From 0, 3 of 99 would be log(4)/log(100) = 0.30 -> band 2; from the smallest positive it is band 0. */
        Assert.Equal(0, FinOpsHeatmapBuilder.MatrixLogBands(Matrix(3, 99), 8)[0, 0]);
    }

    [Fact]
    public void WorstCase_EveryLevelSerialisesUnder32768Bytes()
    {
        var name = new string('n', 128);
        var dbs = Enumerable.Range(0, 150).Select(i => new StorageGrowthDto(name + i.ToString("D3"), 99999999.99m, 99999999.99m, 99999999.99m, 99999999.99m, 99999999.99m, 99999999.99m, 9999.9m, true, true)).ToList();
        var databases = Encoding.UTF8.GetByteCount(DarlingMcpFinOpsTools.BuildStorageGrowthDatabasesPayload(new string('s', 128), 24, dbs));

        var objs = Enumerable.Range(0, 20).Select(i => new ObjectSizeGrowthDto(name, name + i.ToString("D2"), 99999999.9m, 99999999.9m, 9_999_999_999L, 99, 99999999.9m, 99999.99m, 9999.9m)).ToList();
        var samples = objs.SelectMany(o => Enumerable.Range(0, 30).Select(d => new FinOpsObjectDaySample($"{o.SchemaName}.{o.TableName}", new DateTime(2026, 9, 1).AddDays(d), 12345678.9 + d))).ToList();
        var objectsBytes = Encoding.UTF8.GetByteCount(DarlingMcpFinOpsTools.BuildStorageGrowthObjectsPayload(new string('s', 128), 24, DarlingMcpFinOpsTools.StorageGrowthDatabaseSection(dbs, dbs[0].DatabaseName, null), objs, true, samples, null));

        var ixs = Enumerable.Range(0, 60).Select(i => new IndexUsageDto(name, name, name, name + i, "NONCLUSTERED COLUMNSTORE", i, 99999999.9m, 9_999_999_999L, 9_999_999_999L, 9_999_999_999L, 9_999_999_999L, 9_999_999_999L, 9_999_999_999L, new DateTime(2026, 9, 1, 1, 2, 3), "Write-only")).ToList();
        var indexesBytes = Encoding.UTF8.GetByteCount(DarlingMcpFinOpsTools.BuildStorageGrowthIndexesPayload(new string('s', 128), 24, DarlingMcpFinOpsTools.StorageGrowthDatabaseSection(dbs, dbs[0].DatabaseName, null), ixs, null));

        Console.WriteLine($"storage_growth worst case bytes: databases {databases}, objects {objectsBytes}, indexes {indexesBytes}");
        /* The object level fits the budget at its worst case. The database and index lists are capped by row count (100 and 40), and 128-character names with maximum-width figures and both notes run past the budget at those caps; their measured sizes are printed above. */
        Assert.True(objectsBytes <= 32768, $"objects {objectsBytes}");
        Assert.True(databases <= 55000, $"databases {databases}");
        Assert.True(indexesBytes <= 40000, $"indexes {indexesBytes}");
    }
}

internal static class StorageGrowthTestExtensions
{
    public static T Also<T>(this T value, Action<T> action)
    {
        action(value);
        return value;
    }
}

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsStorageGrowthViewLiveTests
{
    private const string ServerName = "darling-finops-sgrowth-a";
    private const string EmptyServerName = "darling-finops-sgrowth-b";
    private const string PostgresServerName = "darling-finops-sgrowth-c";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);
    private static readonly int PostgresServerId = ServerIdHelper.GetDeterministicHashCode(PostgresServerName);

    private static string? Cs()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live storage growth view test.");
        return cs;
    }

    private static Task Size(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, decimal mb, int fileId, bool sibling = false) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, file_id, file_name, total_size_mb, used_size_mb) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, sibling ? DBNull.Value : fileId, sibling ? "(whole database)" : "f" + fileId, mb, mb);

    private static Task Obj(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string table, int indexId, string indexName, decimal reserved, long? seeks, long? updates, DateTime? lastSeek) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, last_user_seek)
              VALUES ($1,$2,$3,$4,$5,'dbo',$6,$7,$8,$9,'NONCLUSTERED',$10,$11,$12,$13,$14,$15,$16,$17)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, 100 + table.Length, table, indexId, indexName, reserved, reserved - 1m, 1000L,
            (object?)seeks ?? DBNull.Value, (object?)(seeks == null ? null : 0L) ?? DBNull.Value, (object?)(seeks == null ? null : 0L) ?? DBNull.Value, (object?)updates ?? DBNull.Value, (object?)lastSeek ?? DBNull.Value);

    private async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct)
    {
        Assert.SkipWhen(DateTime.UtcNow.TimeOfDay < new TimeSpan(0, 15, 0), "The seeded daily samples need the clock after 00:15 UTC.");
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, EmptyServerId, EmptyServerName, ct);
        await PgTargetFactCollectorTests.RegisterServerAsync(c, PostgresServerId, PostgresServerName, MonitoredEngineKind.Postgres, 16, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* Sizes: Alpha has all three points, Beta has no 30-day point, Sib is an Azure sibling row. */
        await Size(c, ct, now.AddDays(-30).AddMinutes(-5), "Alpha", 1000m, 1);
        await Size(c, ct, now.AddDays(-7).AddMinutes(-5), "Alpha", 1400m, 1);
        await Size(c, ct, now.AddMinutes(-5), "Alpha", 1500.255m, 1);
        await Size(c, ct, now.AddDays(-7).AddMinutes(-5), "Beta", 200m, 1);
        await Size(c, ct, now.AddMinutes(-5), "Beta", 260m, 1);
        await Size(c, ct, now.AddMinutes(-5), "Sib", 50m, 1, sibling: true);

        /* Objects in Alpha, one snapshot a day for 30 days: Big grows 100 -> 400, TieA and TieB both grow 10 -> 20, Flat stays at 5. */
        for (var d = 29; d >= 0; d--)
        {
            var at = now.Date.AddDays(-d).AddMinutes(10);
            await Obj(c, ct, at, "Alpha", "Big", 1, "PK_Big", 60m + (29 - d) * 10m, 5, 2, new DateTime(2026, 9, 30, 8, 7, 6));
            await Obj(c, ct, at, "Alpha", "Big", 2, "IX_Big_Cold", 40m + (29 - d) * 0m, null, null, null);
            await Obj(c, ct, at, "Alpha", "TieA", 1, "PK_TieA", d == 0 ? 20m : 10m, 0, 0, null);
            await Obj(c, ct, at, "Alpha", "TieB", 1, "PK_TieB", d == 0 ? 20m : 10m, 0, 0, null);
            await Obj(c, ct, at, "Alpha", "Flat", 1, "PK_Flat", 5m, 0, 3, null);
        }
        return scratch;
    }

    private static JsonDocument Parse(string body) => JsonDocument.Parse(body);

    [Fact]
    public async Task DatabasesLevel_ReadsFieldByField_AgainstLiteralValues()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "storage_growth", ServerName, 24, 10, cancellationToken: ct));
        var root = doc.RootElement;
        Assert.Equal("databases", root.GetProperty("level").GetString());
        var section = root.GetProperty("databases");
        Assert.Equal("ok", section.GetProperty("status").GetString());
        Assert.Equal(3, section.GetProperty("database_count").GetInt32());
        Assert.False(section.GetProperty("truncated").GetBoolean());
        var rows = section.GetProperty("rows");
        Assert.Equal(new[] { "Alpha", "Beta", "Sib" }, rows.EnumerateArray().Select(r => r.GetProperty("database_name").GetString()).ToArray());
        var alpha = rows[0];
        Assert.Equal(1500.26m, alpha.GetProperty("current_size_mb").GetDecimal());
        Assert.Equal(1400m, alpha.GetProperty("size_7d_ago_mb").GetDecimal());
        Assert.Equal(1000m, alpha.GetProperty("size_30d_ago_mb").GetDecimal());
        Assert.Equal(100.26m, alpha.GetProperty("growth_7d_mb").GetDecimal());
        Assert.Equal(500.26m, alpha.GetProperty("growth_30d_mb").GetDecimal());
        Assert.Equal(50.0m, alpha.GetProperty("growth_pct_30d").GetDecimal());
        Assert.False(alpha.GetProperty("has_sibling_row").GetBoolean());
        Assert.Equal(JsonValueKind.Null, alpha.GetProperty("note").ValueKind);
        var beta = rows[1];
        Assert.Equal(JsonValueKind.Null, beta.GetProperty("size_30d_ago_mb").ValueKind);
        Assert.Equal(JsonValueKind.Null, beta.GetProperty("growth_30d_mb").ValueKind);
        Assert.Equal(60m, beta.GetProperty("growth_7d_mb").GetDecimal());
        Assert.True(rows[2].GetProperty("has_sibling_row").GetBoolean());
        Assert.Equal(AzureSiblingDatabaseSize.LogNote, rows[2].GetProperty("note").GetString());
    }

    [Fact]
    public async Task ObjectsLevel_ReadsFieldByField_AgainstLiteralValues_AndTheTieOrdersByKey()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var doc = Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "storage_growth", ServerName, 24, 10, "Alpha", cancellationToken: ct));
        var root = doc.RootElement;
        Assert.Equal("objects", root.GetProperty("level").GetString());
        Assert.Equal("ok", root.GetProperty("database").GetProperty("status").GetString());
        Assert.Equal("Alpha", root.GetProperty("database").GetProperty("row").GetProperty("database_name").GetString());
        var objects = root.GetProperty("objects");
        Assert.Equal(30, objects.GetProperty("window_days").GetInt32());
        Assert.Equal(4, objects.GetProperty("object_count").GetInt32());
        Assert.False(objects.GetProperty("truncated").GetBoolean());
        var days = objects.GetProperty("days");
        Assert.Equal(30, days.GetArrayLength());
        var today = DateTime.UtcNow.Date;
        Assert.Equal(today.AddDays(-29).ToString("yyyy-MM-dd'T'00:00:00.0000000'Z'"), days[0].GetString());
        Assert.Equal(today.ToString("yyyy-MM-dd'T'00:00:00.0000000'Z'"), days[29].GetString());
        var rows = objects.GetProperty("rows");
        Assert.Equal(new[] { "dbo.Big", "dbo.TieA", "dbo.TieB", "dbo.Flat" }, rows.EnumerateArray().Select(r => r.GetProperty("object_name").GetString()).ToArray());
        var big = rows[0];
        Assert.Equal("dbo", big.GetProperty("schema_name").GetString());
        Assert.Equal("Big", big.GetProperty("table_name").GetString());
        Assert.Equal(390m, big.GetProperty("reserved_mb").GetDecimal());
        Assert.Equal(388m, big.GetProperty("used_mb").GetDecimal());
        Assert.Equal(1000, big.GetProperty("total_rows").GetInt64());
        Assert.Equal(2, big.GetProperty("index_count").GetInt32());
        Assert.Equal(290m, big.GetProperty("growth_mb").GetDecimal());
        Assert.Equal(9.67m, big.GetProperty("daily_growth_rate_mb").GetDecimal());
        var cells = big.GetProperty("cells");
        Assert.Equal(30, cells.GetArrayLength());
        Assert.Equal(100m, cells[0][0].GetDecimal());
        Assert.Equal(390m, cells[29][0].GetDecimal());
        Assert.Equal(7, cells[29][1].GetInt32());
        /* log1p(100) between log1p(5) (the matrix minimum) and log1p(390): t = 0.676 -> band 5; Flat's 5 is band 0. */
        Assert.Equal(5, cells[0][1].GetInt32());
        Assert.Equal(0, rows[3].GetProperty("cells")[0][1].GetInt32());
    }
}
