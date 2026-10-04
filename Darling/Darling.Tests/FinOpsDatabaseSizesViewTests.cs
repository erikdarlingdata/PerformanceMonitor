/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

public sealed class FinOpsDatabaseSizesViewTests
{
    private static DatabaseSizeFileDto File(int i) => new(
        new DateTime(2026, 10, 4, 12, 0, 0, DateTimeKind.Unspecified), "LongerDatabaseName_" + i, i % 2 == 0 ? "ROWS" : "LOG",
        "LongerDatabaseName_" + i + (i % 2 == 0 ? ".mdf" : "_log.ldf"), 12345.67m + i, 9876.54m, -1m,
        "D:\\SQLData\\", 1_048_576.00m, 524_288.25m, "FULL", 1024.00m, false, null, i % 2 == 0 ? null : 48, i);

    [Fact]
    public void ServedHead_StaysUnderTheTarget_AndTheTailDescribesTheView()
    {
        var served = McpToolGuideTests.Served("get_finops");
        Assert.True(served.Served.Length <= 620, $"served head is {served.Served.Length}");
        Assert.Contains("database_sizes gives one row per database file", served.Tail, StringComparison.Ordinal);
        Assert.Contains("database_sizes", DarlingMcpFinOpsTools.SetBValid, StringComparison.Ordinal);
        Assert.Contains("database_sizes", DarlingMcpFinOpsTools.Views);
    }

    [Fact]
    public void ADefaultCall_OnALargeStore_StaysUnderTheResponseTarget()
    {
        var files = Enumerable.Range(0, 600).Select(File).ToList();
        var json = DarlingMcpFinOpsTools.DatabaseSizesPayload("srv", files, 1234.50m);
        using var doc = JsonDocument.Parse(json);
        Assert.Equal(DarlingMcpFinOpsTools.DefaultDatabaseSizeRows, doc.RootElement.GetProperty("rows").GetArrayLength());
        Assert.Equal(600, doc.RootElement.GetProperty("file_count").GetInt32());
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes, $"{Encoding.UTF8.GetByteCount(json)} bytes");
    }

    [Fact]
    public void ACallAtTheCeiling_ListsEveryFileUpToTheCeiling_AndSaysWhenItStillCuts()
    {
        var files = Enumerable.Range(0, 600).Select(File).ToList();
        var cap = DarlingMcpFinOpsTools.DatabaseSizeRowCap(DarlingMcpFinOpsTools.MaxDatabaseSizeRows);
        Assert.Equal(500, cap);
        using var doc = JsonDocument.Parse(DarlingMcpFinOpsTools.DatabaseSizesPayload("srv", files, 1234.50m, cap));
        Assert.Equal(500, doc.RootElement.GetProperty("rows").GetArrayLength());
        Assert.Equal(600, doc.RootElement.GetProperty("file_count").GetInt32());
        Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
        using var all = JsonDocument.Parse(DarlingMcpFinOpsTools.DatabaseSizesPayload("srv", files.Take(300).ToList(), 1234.50m, cap));
        Assert.Equal(300, all.RootElement.GetProperty("rows").GetArrayLength());
        Assert.False(all.RootElement.GetProperty("truncated").GetBoolean());
    }

    [Fact]
    public void TheDefaultLimit_MeansTheDefaultRowCount_AndAnyOtherLimitIsTheCount()
    {
        Assert.Equal(DarlingMcpFinOpsTools.DefaultDatabaseSizeRows, DarlingMcpFinOpsTools.DatabaseSizeRowCap(10));
        Assert.Equal(11, DarlingMcpFinOpsTools.DatabaseSizeRowCap(11));
    }

    [Fact]
    public void CapturedAt_IsAUtcStamp_LikeTheSiblingViews()
    {
        using var doc = JsonDocument.Parse(DarlingMcpFinOpsTools.DatabaseSizesPayload("srv", [File(0)], 0m));
        Assert.EndsWith("Z", doc.RootElement.GetProperty("captured_at").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void NoCost_LeavesEveryRowCostNull_AndNamesTheReason()
    {
        using var doc = JsonDocument.Parse(DarlingMcpFinOpsTools.DatabaseSizesPayload("srv", [File(0), File(1)], 0m));
        Assert.Equal("monthly cost not set", doc.RootElement.GetProperty("cost_reason").GetString());
        Assert.All(doc.RootElement.GetProperty("rows").EnumerateArray(),
            r => Assert.Equal(JsonValueKind.Null, r.GetProperty("monthly_cost_usd").ValueKind));
    }

    [Fact]
    public void ARowCarriesTheDesktopDerivedColumns_AndVlfOnlyOnALogFile()
    {
        using var doc = JsonDocument.Parse(DarlingMcpFinOpsTools.DatabaseSizesPayload("srv", [File(0), File(1)], 100m));
        var rows = doc.RootElement.GetProperty("rows").EnumerateArray().ToList();
        Assert.Equal(JsonValueKind.Null, rows[0].GetProperty("vlf_count").ValueKind);
        Assert.Equal(48, rows[1].GetProperty("vlf_count").GetInt32());
        Assert.Equal(12345.67m - 9876.54m, rows[0].GetProperty("free_space_mb").GetDecimal());
        Assert.Equal(80.0m, rows[0].GetProperty("used_pct").GetDecimal());
        Assert.Equal("FULL", rows[0].GetProperty("recovery_model").GetString());
    }
}

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsDatabaseSizesViewLiveTests
{
    private const string ServerName = "darling-finops-dbsizes-view-a";
    private const string EmptyServerName = "darling-finops-dbsizes-view-b";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly int EmptyServerId = ServerIdHelper.GetDeterministicHashCode(EmptyServerName);

    private static async Task<ScratchPostgres> SeedAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, EmptyServerId, EmptyServerName, ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET monthly_cost_usd = 1000 WHERE server_id = $1", ServerId);
        var old = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-3);
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-1);
        /* An older snapshot that must be ignored, then the latest: a percent-growth data file, a MB-growth log
           file with VLFs, a no-growth file, and a Hyperscale-style log file with no size. */
        await FileRowAsync(c, ct, old, "OldDb", "o.mdf", "ROWS", 9999m, 1m, 1m, false, null, "FULL", null, 1);
        await FileRowAsync(c, ct, at, "SalesDb", "sales.mdf", "ROWS", 800m, 600m, 64m, false, null, "FULL", null, 1);
        await FileRowAsync(c, ct, at, "SalesDb", "sales_log.ldf", "LOG", 200m, 20m, 10m, true, 10, "FULL", 37, 2);
        await FileRowAsync(c, ct, at, "ArchiveDb", "archive.mdf", "ROWS", 500m, 500m, 0m, false, null, "SIMPLE", null, 1);
        await FileRowAsync(c, ct, at, "ArchiveDb", "archive_log.ldf", "LOG", null, null, 0m, false, null, "SIMPLE", 4, 2);
        return scratch;
    }

    private static Task FileRowAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string file, string type,
        decimal? total, decimal? used, decimal growthMb, bool pct, int? growthPct, string recovery, int? vlf, int fileId) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, file_id, file_name,
                file_type_desc, total_size_mb, used_size_mb, auto_growth_mb, is_percent_growth, growth_pct, recovery_model_desc, vlf_count,
                volume_mount_point, volume_total_mb, volume_free_mb)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'D:\',100000,40000)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, fileId, file, type, total, used, growthMb, pct, growthPct, recovery, vlf);

    private static string? Cs()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live database_sizes view test.");
        return cs;
    }

    [Fact]
    public async Task ToolRows_MatchTheDesktopViewersRowsAndCostShares()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);

        var expected = await viewer.GetDatabaseSizeLatestAsync(ServerId, ct);
        var allocated = DatabaseSizeRow.AllocatedTotalMb(expected);
        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_sizes", ServerName, cancellationToken: ct));
        var rows = tool.RootElement.GetProperty("rows").EnumerateArray().ToList();

        Assert.Equal(expected.Count, rows.Count);
        Assert.Equal(4, tool.RootElement.GetProperty("file_count").GetInt32());
        Assert.False(tool.RootElement.GetProperty("truncated").GetBoolean());
        for (var i = 0; i < expected.Count; i++)
        {
            var e = expected[i];
            var r = rows[i];
            Assert.Equal(e.DatabaseName, r.GetProperty("database_name").GetString());
            Assert.Equal(e.FileName, r.GetProperty("file_name").GetString());
            Assert.Equal(e.RecoveryModel, r.GetProperty("recovery_model").GetString());
            Assert.Equal(e.IsPercentGrowth, r.GetProperty("is_percent_growth").GetBoolean());
            Assert.Equal(e.AutoGrowthMb, r.GetProperty("auto_growth_mb").GetDecimal());
            if (e.UsedPct is { } pctUsed) Assert.Equal(pctUsed, r.GetProperty("used_pct").GetDecimal());
            else Assert.Equal(JsonValueKind.Null, r.GetProperty("used_pct").ValueKind);
            if (e.FreeSpaceMb is { } free) Assert.Equal(free, r.GetProperty("free_space_mb").GetDecimal());
            if (e.VlfCountDisplay == "N/A") Assert.Equal(JsonValueKind.Null, r.GetProperty("vlf_count").ValueKind);
            else Assert.Equal(e.VlfCount, r.GetProperty("vlf_count").GetInt32());
            if (e.TotalSizeMb is null) Assert.Equal(JsonValueKind.Null, r.GetProperty("monthly_cost_usd").ValueKind);
            else Assert.Equal(Math.Round(FinOpsCost.StorageShare(e.TotalSizeMb.Value, allocated, 1000m), 2, MidpointRounding.AwayFromZero),
                r.GetProperty("monthly_cost_usd").GetDecimal());
        }

        Assert.DoesNotContain(rows, r => r.GetProperty("database_name").GetString() == "OldDb");
        Assert.Equal(1000m, tool.RootElement.GetProperty("monthly_cost_usd").GetDecimal());
    }

    [Fact]
    public async Task ALimitAtTheCeiling_ReturnsMoreThanTheDefaultRows_AndOverTheCeilingIsRefused()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddMinutes(-30);
            for (var i = 0; i < 120; i++)
                await FileRowAsync(c, ct, at, "BulkDb" + (i / 2), "bulk" + i + ".mdf", "ROWS", 100m + i, 50m, 1m, false, null, "FULL", null, i + 1);
        }

        using var dflt = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_sizes", ServerName, cancellationToken: ct));
        Assert.Equal(DarlingMcpFinOpsTools.DefaultDatabaseSizeRows, dflt.RootElement.GetProperty("rows").GetArrayLength());
        Assert.True(dflt.RootElement.GetProperty("truncated").GetBoolean());

        using var max = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_sizes", ServerName,
            limit: DarlingMcpFinOpsTools.MaxDatabaseSizeRows, cancellationToken: ct));
        Assert.Equal(120, max.RootElement.GetProperty("rows").GetArrayLength());
        Assert.Equal(120, max.RootElement.GetProperty("file_count").GetInt32());
        Assert.False(max.RootElement.GetProperty("truncated").GetBoolean());

        using var over = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_sizes", ServerName, limit: 501, cancellationToken: ct));
        Assert.Contains("1 to 500", over.RootElement.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWithNoSnapshot_GetsAStatus_NotAnError()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(Cs()!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);

        using var tool = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "database_sizes", EmptyServerName, cancellationToken: ct));
        Assert.Equal("unavailable", tool.RootElement.GetProperty("status").GetString());
    }
}
