/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Equality pin for the high-impact read after it moved into Darling.Storage: the exact figures below were
/// captured from the viewer's own read BEFORE the move, on this same seed, so the storage read and the
/// viewer delegate must both reproduce them value for value and in order.
/// </summary>
[Collection("live-postgres")]
public sealed class FinOpsHighImpactReaderLiveTests
{
    private const string ServerName = "darling-finops-high-impact-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* Captured from the pre-move viewer read: hash, database, executions, cpu ms, duration ms, reads, writes,
       memory MB, shares (cpu, duration, reads, writes, memory, executions), impact score. */
    private static readonly (string Hash, string Db, long Exec, decimal Cpu, decimal Dur, long Reads, long Writes, decimal Mem, decimal[] Shares, int Score)[] Expected =
    [
        ("0xHI03", "Db0", 22L, 1150m, 1200m, 3500L, 310L, 21m, new[] { 12.4m, 9.3m, 6.9m, 12.9m, 15.8m, 15.6m }, 75),
        ("0xHI06", "Db0", 16L, 400m, 1500m, 5000L, 150L, 18m, new[] { 4.3m, 11.6m, 9.9m, 6.2m, 13.5m, 11.3m }, 62),
        ("0xHI02", "Db2", 13L, 200m, 1800m, 7000L, 200L, 14m, new[] { 2.2m, 14m, 13.9m, 8.3m, 10.5m, 9.2m }, 60),
        ("0xHI09", "Db0", 14L, 1200m, 450m, 7000L, 0L, 16m, new[] { 13m, 3.5m, 13.9m, 0m, 12m, 9.9m }, 57),
        ("0xHI08", "Db2", 9L, 500m, 1350m, 4000L, 350L, 10m, new[] { 5.4m, 10.5m, 7.9m, 14.5m, 7.5m, 6.4m }, 53),
        ("0xHI04", "Db1", 6L, 300m, 1650m, 6000L, 400L, 6m, new[] { 3.2m, 12.8m, 11.9m, 16.6m, 4.5m, 4.3m }, 50),
        ("0xHI12", "Db0", 12L, 700m, 1050m, 2000L, 300L, 14m, new[] { 7.6m, 8.1m, 4m, 12.4m, 10.5m, 8.5m }, 46),
        ("0xHI11", "Db2", 7L, 1300m, 300m, 6000L, 200L, 8m, new[] { 14.1m, 2.3m, 11.9m, 8.3m, 6m, 5m }, 43),
        ("0xHI10", "Db1", 19L, 600m, 1200m, 3000L, 100L, 2m, new[] { 6.5m, 9.3m, 5.9m, 4.1m, 1.5m, 13.5m }, 37),
        ("0xHI05", "Db2", 11L, 1000m, 750m, 2000L, 50L, 12m, new[] { 10.8m, 5.8m, 4m, 2.1m, 9m, 7.8m }, 34),
        ("0xHI01", "Db1", 8L, 800m, 1050m, 4000L, 100L, 8m, new[] { 8.6m, 8.1m, 7.9m, 4.1m, 6m, 5.7m }, 34),
        ("0xHI07", "Db1", 4L, 1100m, 600m, 1000L, 250L, 4m, new[] { 11.9m, 4.7m, 2m, 10.4m, 3m, 2.8m }, 27),
    ];

    private static async Task<ScratchPostgres> SeedAsync(string cs, System.Threading.CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
        var at = DarlingMcpTestData.Naive(DateTime.UtcNow).AddHours(-1);
        for (var i = 1; i <= 12; i++)
        {
            await Plant(connection, ct, at, "0xHI" + i.ToString("D2"), "Db" + (i % 3),
                exec: 3 + (i * 5) % 17, cpuUs: ((i * 7) % 13 + 1) * 100_000L, elapsedUs: ((i * 5) % 11 + 2) * 150_000L,
                reads: ((i * 3) % 7 + 1) * 1000L, writes: ((i * 11) % 9) * 50L, grantKb: ((i * 13) % 10 + 1) * 2048L);
        }
        await Plant(connection, ct, at.AddMinutes(-5), "0xHI03", "Db0", exec: 4, cpuUs: 250_000L, elapsedUs: 300_000L, reads: 500L, writes: 10L, grantKb: 1024L);
        await Plant(connection, ct, at.AddDays(-3), "0xHIOLD", "Db9", exec: 99, cpuUs: 900_000_000L, elapsedUs: 900_000_000L, reads: 9_000_000L, writes: 9_000L, grantKb: 99999L);

        return scratch;
    }

    [Fact]
    public async Task StorageRead_AndViewerDelegate_ReproducePreMoveFigures_Exactly()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact reader test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(cs!, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var stored = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, 24, 60, ct);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var viewed = await viewer.GetHighImpactQueriesAsync(ServerId, 24, ct);

        Assert.Equal(Expected.Length, stored.Count);
        Assert.Equal(Expected.Length, viewed.Count);
        for (var i = 0; i < Expected.Length; i++)
        {
            var e = Expected[i];
            AssertRow(e, stored[i].QueryHash, stored[i].DatabaseName, stored[i].TotalExecutions, stored[i].TotalCpuMs,
                stored[i].TotalDurationMs, stored[i].TotalReads, stored[i].TotalWrites, stored[i].TotalMemoryMb,
                [stored[i].CpuShare, stored[i].DurationShare, stored[i].ReadsShare, stored[i].WritesShare, stored[i].MemoryShare, stored[i].ExecutionsShare],
                stored[i].ImpactScore, stored[i].SampleQueryText, stored[i].FullQueryText);
            AssertRow(e, viewed[i].QueryHash, viewed[i].DatabaseName, viewed[i].TotalExecutions, viewed[i].TotalCpuMs,
                viewed[i].TotalDurationMs, viewed[i].TotalReads, viewed[i].TotalWrites, viewed[i].TotalMemoryMb,
                [viewed[i].CpuShare, viewed[i].DurationShare, viewed[i].ReadsShare, viewed[i].WritesShare, viewed[i].MemoryShare, viewed[i].ExecutionsShare],
                viewed[i].ImpactScore, viewed[i].SampleQueryText, viewed[i].FullQueryText);
            Assert.Null(stored[i].QueryPlanXml);
            Assert.Null(viewed[i].QueryPlanXml);
        }
    }

    [Fact]
    public async Task StorageRead_HonoursTheWindow_AndTheOutOfWindowRowIsAbsent()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact reader test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(cs!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var narrow = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, 24, 60, ct);
        Assert.DoesNotContain(narrow, q => q.QueryHash == "0xHIOLD");

        var wide = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, 24 * 7, 60, ct);
        Assert.Contains(wide, q => q.QueryHash == "0xHIOLD");
    }

    [Fact]
    public async Task StorageRead_ForAnUnknownServer_ReturnsNothing()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live high-impact reader test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await SeedAsync(cs!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        Assert.Empty(await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId + 1, 24, 60, ct));
    }

    private static void AssertRow(
        (string Hash, string Db, long Exec, decimal Cpu, decimal Dur, long Reads, long Writes, decimal Mem, decimal[] Shares, int Score) e,
        string hash, string db, long exec, decimal cpu, decimal dur, long reads, long writes, decimal mem,
        decimal[] shares, int score, string sample, string full)
    {
        Assert.Equal(e.Hash, hash);
        Assert.Equal(e.Db, db);
        Assert.Equal(e.Exec, exec);
        Assert.Equal(e.Cpu, cpu);
        Assert.Equal(e.Dur, dur);
        Assert.Equal(e.Reads, reads);
        Assert.Equal(e.Writes, writes);
        Assert.Equal(e.Mem, mem);
        Assert.Equal(e.Shares, shares);
        Assert.Equal(e.Score, score);
        Assert.Equal("SELECT " + e.Hash + ";", sample);
        Assert.Equal("SELECT " + e.Hash + ";", full);
    }

    private static async Task Plant(NpgsqlConnection c, System.Threading.CancellationToken ct, DateTime at, string hash, string db,
        long exec, long cpuUs, long elapsedUs, long reads, long writes, long grantKb)
    {
        await DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name,
                query_hash, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                delta_logical_writes, max_grant_kb, sample_interval_seconds, last_execution_time, query_text)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, hash, exec, cpuUs, elapsedUs, reads, writes, grantKb, 60, at, "SELECT " + hash + ";");
    }
}
