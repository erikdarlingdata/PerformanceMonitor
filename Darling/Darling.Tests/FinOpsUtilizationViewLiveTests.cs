/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>utilization</c> view of <c>get_finops</c>, on the same five-server seed the utilization golden test uses
/// (A: CPU, memory, grants; B: Azure SQL Database with a week of days; C: Azure SQL Database; D: memory and no CPU;
/// E: CPU and no memory), plus a monthly cost on A and database sizes on A. Own-store: the seed lives in a scratch database.
/// </summary>
/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsUtilizationViewLiveTests
{
    private static async Task<ScratchPostgres> SeedScratchAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var anchor = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        await FinOpsUtilizationGoldenLiveTests.SeedAsync(connection, anchor, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET monthly_cost_usd = 1500.00 WHERE server_id = $1", FinOpsUtilizationGoldenLiveTests.ServerIdA);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "UPDATE servers SET monthly_cost_usd = 0 WHERE server_id = $1", FinOpsUtilizationGoldenLiveTests.ServerIdC);
        foreach (var (file, total, used) in new[] { (1, 600m, 400m), (2, 400m, 200m) })
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, database_id,
     file_id, file_type_desc, file_name, physical_name, total_size_mb, used_size_mb)
VALUES ($1, $2, $3, $4, 'Db', NULL, $5, 'ROWS', $6, NULL, $7, $8)",
                CollectionIdGenerator.Next(), now.AddHours(-3), FinOpsUtilizationGoldenLiveTests.ServerIdA,
                FinOpsUtilizationGoldenLiveTests.ServerNameA, file, "f" + file, total, used);
        }
        await SeedExtraServersAsync(connection, now, ct);
        return scratch;
    }

    internal const string ServerNameVcoreLess = "darling-finops-util-view-g";
    internal const string ServerNameMaster = "darling-finops-util-view-h";
    internal const string ServerNameNoProps = "darling-finops-util-view-i";

    /* G: Azure SQL Database whose service objective names no vCores. H: a logical server's master database.
       I: CPU and memory with no server_properties row. */
    private static async Task SeedExtraServersAsync(NpgsqlConnection connection, DateTime now, CancellationToken ct)
    {
        foreach (var (name, edition, engine) in new (string, string?, int?)[]
        {
            (ServerNameVcoreLess, "Standard", 5),
            (ServerNameMaster, "Azure SQL Database (System)", 5),
            (ServerNameNoProps, null, null),
        })
        {
            var id = ServerIdHelper.GetDeterministicHashCode(name);
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, 20, 1)",
                CollectionIdGenerator.Next(), now.AddHours(-1), id, name, now.AddHours(-1));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 8192, 3000, 4000, 2000, 512, 50)",
                CollectionIdGenerator.Next(), now.AddHours(-1), id, name);
            if (engine is { } e)
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, cpu_count, vcore_count) VALUES ($1, $2, $3, $4, $5, $6, 4, NULL)",
                    CollectionIdGenerator.Next(), now.AddDays(-1), id, name, edition!, e);
        }
    }

    private static async Task<JsonDocument> CallAsync(NpgsqlDataSource ds, string name, int hours = 24, int limit = 10, CancellationToken ct = default) =>
        JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(ds, "utilization", name, hours, limit, ct));

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live utilization view test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedScratchAsync(cs!, ct);
        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(ds, ct);
    }

    [Fact]
    public Task ServerA_MatchesTheStoragePieces_AndPinsMoneyScoreAndBand() => WithStoreAsync(async (ds, ct) =>
    {
        var id = FinOpsUtilizationGoldenLiveTests.ServerIdA;
        using var doc = await CallAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerNameA, ct: ct);
        var r = doc.RootElement;
        var dto = (await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(ds, id, 30, ct))!;
        var totals = await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(ds, id, 30, ct);
        var free = FinOpsUtilizationFigures.FreeSpacePct(totals!.Value.AllocatedMb, totals.Value.FreeMb);
        var score = FinOpsUtilizationFigures.HealthScore(dto, free);

        Assert.Equal("utilization", r.GetProperty("view").GetString());
        Assert.Equal(24, r.GetProperty("window_hours").GetInt32());
        Assert.Equal(7, r.GetProperty("trend_days").GetInt32());
        Assert.Equal(dto.ProvisioningStatus, r.GetProperty("verdict").GetString());
        Assert.Equal(FinOpsUtilizationFigures.Explanation(dto, CultureInfo.InvariantCulture), r.GetProperty("verdict_reason").GetString());
        Assert.Equal(score, r.GetProperty("health_score").GetInt32());
        Assert.Equal(FinOpsUtilizationFigures.HealthBand(score), r.GetProperty("health_band").GetString());
        Assert.Equal(40m, r.GetProperty("free_space_pct").GetDecimal());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("health_score_note").ValueKind);
        var mem = r.GetProperty("memory");
        Assert.Equal(Math.Round(FinOpsUtilizationFigures.StolenMemoryPct(dto.TotalMemoryMb, dto.BufferPoolMb), 1), mem.GetProperty("stolen_memory_pct").GetDouble());
        Assert.Equal(Math.Round(FinOpsUtilizationFigures.BufferPoolPct(dto.BufferPoolMb, dto.PhysicalMemoryMb), 1), mem.GetProperty("buffer_pool_pct").GetDouble());
        Assert.Equal("physical", mem.GetProperty("memory_basis").GetString());
        Assert.Equal(dto.MaxCpuPct, r.GetProperty("cpu").GetProperty("max_cpu_pct").GetInt32());
        Assert.Equal(8, r.GetProperty("cpu").GetProperty("cpu_count").GetInt32());

        /* Hand-computed: p95 of 10,20,30,95 is 85.25 -> CPU score 24; buffer pool 6000/16000 -> memory 100; free 40% -> storage 100. */
        Assert.Equal(69, score);
        Assert.Equal(38.8m, r.GetProperty("cpu").GetProperty("avg_cpu_pct").GetDecimal());
        Assert.Equal(85.3m, r.GetProperty("cpu").GetProperty("p95_cpu_pct").GetDecimal());
        Assert.Contains("85.3%", r.GetProperty("verdict_reason").GetString(), StringComparison.Ordinal);
        Assert.Equal("fair", r.GetProperty("health_band").GetString());
        Assert.Equal(3, r.GetProperty("engine_edition").GetInt32());
        Assert.Equal(1500.00m, r.GetProperty("monthly_cost_usd").GetDecimal());
        Assert.Equal(18000.00m, r.GetProperty("annual_cost_usd").GetDecimal());
        Assert.Equal(FinOpsCost.Annual(1500.00m), r.GetProperty("annual_cost_usd").GetDecimal());
    });

    [Fact]
    public Task ServerWithoutACost_HasNullCostAndAReason_AndEditionFiveFacts() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerNameC, ct: ct);
        var r = doc.RootElement;
        Assert.Equal(JsonValueKind.Null, r.GetProperty("monthly_cost_usd").ValueKind);
        Assert.Equal(JsonValueKind.Null, r.GetProperty("annual_cost_usd").ValueKind);
        Assert.Equal("monthly cost not set", r.GetProperty("cost_reason").GetString());
        Assert.Equal("memory_limit", r.GetProperty("memory").GetProperty("memory_basis").GetString());
        Assert.Equal(JsonValueKind.Null, r.GetProperty("cpu").GetProperty("current_workers").ValueKind);
        Assert.Equal(2, r.GetProperty("cpu").GetProperty("cpu_count").GetInt32());
        Assert.Equal("vcores", r.GetProperty("cpu").GetProperty("cpu_count_unit").GetString());
        Assert.Equal("no database size snapshot", r.GetProperty("free_space_pct_reason").GetString());
    });

    [Fact]
    public Task ServerWithoutCpu_HasNullVerdictAndTheScoreNote() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerNameD, ct: ct);
        var r = doc.RootElement;
        Assert.Equal(JsonValueKind.Null, r.GetProperty("verdict").ValueKind);
        Assert.Equal(ServerHardwareScope.HealthScoreWithoutCpuNote, r.GetProperty("health_score_note").GetString());
        Assert.Contains("no verdict", r.GetProperty("verdict_reason").GetString(), StringComparison.Ordinal);

        /* Hand-computed: buffer pool 2000/8192 is 0.24 -> memory 60; no database size snapshot -> free 100% -> storage 100;
           no CPU, so (60 * 30 + 100 * 30) / 60 = 80, which is the good band. */
        Assert.Equal(80, r.GetProperty("health_score").GetInt32());
        Assert.Equal("good", r.GetProperty("health_band").GetString());
        var dto = (await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerIdD, 30, ct))!;
        Assert.Equal(FinOpsUtilizationFigures.HealthScore(dto, 100m), r.GetProperty("health_score").GetInt32());
    });

    [Fact]
    public Task EditionFiveServerWithoutVcores_HasNullCpuCountAndAReason() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, ServerNameVcoreLess, ct: ct);
        var cpu = doc.RootElement.GetProperty("cpu");
        Assert.Equal(JsonValueKind.Null, cpu.GetProperty("cpu_count").ValueKind);
        Assert.Equal("service objective names no vCores", cpu.GetProperty("cpu_count_reason").GetString());
        Assert.Equal("vcores", cpu.GetProperty("cpu_count_unit").GetString());
        Assert.Equal(5, doc.RootElement.GetProperty("engine_edition").GetInt32());
    });

    [Fact]
    public Task LogicalServerMaster_HasTheNotApplicableVerdict() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, ServerNameMaster, ct: ct);
        Assert.Equal("NOT_APPLICABLE", doc.RootElement.GetProperty("verdict").GetString());
        Assert.Equal(ProvisioningVerdict.NotApplicableExplanation, doc.RootElement.GetProperty("verdict_reason").GetString());
    });

    [Fact]
    public Task ServerWithoutAPropertiesRow_HasNullEngineEdition() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, ServerNameNoProps, ct: ct);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("engine_edition").ValueKind);
    });

    [Fact]
    public Task PayloadKeys_AreExactlyTheDocumentedSets() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerNameB, ct: ct);
        var trend = doc.RootElement.GetProperty("provisioning_trend")[0];
        Assert.Equal(
            new[] { "avg_cpu_pct", "day", "max_cpu_pct", "memory_ratio", "p95_cpu_pct", "verdict" },
            trend.EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.Equal(
            new[]
            {
                "buffer_pool_mb", "buffer_pool_pct", "forced_grants", "grant_timeouts", "grant_utilization_pct", "max_grant_waiters",
                "memory_basis", "memory_ratio", "physical_memory_mb", "stolen_memory_pct", "target_memory_mb", "total_memory_mb",
            },
            doc.RootElement.GetProperty("memory").EnumerateObject().Select(p => p.Name).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    });

    [Fact]
    public Task ServerWithoutMemory_AnswersEmpty() => WithStoreAsync(async (ds, ct) =>
    {
        using var doc = await CallAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerNameE, ct: ct);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal("No memory statistics were collected for this server in the last 24 hours, so there is no utilization summary.",
            doc.RootElement.GetProperty("message").GetString());
    });

    [Fact]
    public Task Trend_IsInReaderOrder_WithIsoDays() => WithStoreAsync(async (ds, ct) =>
    {
        var expected = await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerIdB, 30, ct);
        using var doc = await CallAsync(ds, FinOpsUtilizationGoldenLiveTests.ServerNameB, ct: ct);
        var trend = doc.RootElement.GetProperty("provisioning_trend");
        Assert.Equal(expected.Count, trend.GetArrayLength());
        Assert.True(expected.Count > 0);
        for (var i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Day.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture), trend[i].GetProperty("day").GetString());
            Assert.Equal(expected[i].Status, trend[i].GetProperty("verdict").GetString());
            Assert.Equal(expected[i].MaxCpuPct, trend[i].GetProperty("max_cpu_pct").GetInt32());
        }
    });

    [Fact]
    public Task FixedWindowParameters_AreRefused_AndExplicitDefaultsAccepted() => WithStoreAsync(async (ds, ct) =>
    {
        var name = FinOpsUtilizationGoldenLiveTests.ServerNameA;
        using (var hours = await CallAsync(ds, name, hours: 48, ct: ct))
        {
            Assert.Equal("invalid", hours.RootElement.GetProperty("status").GetString());
            Assert.Equal("hours_back", hours.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.Contains("utilization", hours.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }
        using (var limit = await CallAsync(ds, name, limit: 5, ct: ct))
        {
            Assert.Equal("invalid", limit.RootElement.GetProperty("status").GetString());
            Assert.Equal("limit", limit.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
            Assert.Contains("utilization", limit.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        }
        using (var ok = await CallAsync(ds, name, 24, 10, ct))
            Assert.Equal("utilization", ok.RootElement.GetProperty("view").GetString());
        using var unknown = await CallAsync(ds, "no-such-server-zz", ct: ct);
        Assert.Equal("invalid", unknown.RootElement.GetProperty("status").GetString());
    });

    [Fact]
    public Task VerdictReason_IsTheSameUnderGermanCulture() => WithStoreAsync(async (ds, ct) =>
    {
        var name = FinOpsUtilizationGoldenLiveTests.ServerNameA;
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("en-US");
            using var en = await CallAsync(ds, name, ct: ct);
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            using var de = await CallAsync(ds, name, ct: ct);
            var enText = en.RootElement.GetProperty("verdict_reason").GetString();
            Assert.False(string.IsNullOrEmpty(enText));
            Assert.Equal(enText, de.RootElement.GetProperty("verdict_reason").GetString());
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    });

    [Fact]
    public Task WebRoute_ReturnsTheToolsBody() => WithStoreAsync(async (ds, ct) =>
    {
        var name = FinOpsUtilizationGoldenLiveTests.ServerNameA;
        var tool = await DarlingMcpFinOpsTools.GetFinOps(ds, "utilization", name, 24, 10, ct);
        var (status, body) = await FinOpsWebReadParityLiveTests.GetAsync(ds, $"/api/read/get_finops?server={name}&view=utilization", ct);
        Assert.Equal(200, status);
        using var expected = JsonDocument.Parse(tool);
        using var actual = JsonDocument.Parse(body);
        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), body);
    });
}
