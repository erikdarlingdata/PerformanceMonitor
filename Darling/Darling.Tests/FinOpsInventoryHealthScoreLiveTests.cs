/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// One server, one health score: the Server Inventory's Health figure (the fleet read, <c>GetServerMetricsAsync</c>) equals the
/// Utilization view's score for the same server, from the same inputs over the same window. The inventory used to score the 24-hour
/// average CPU against a fixed memory and storage term, so servers with different buffer pools and free space all read the same number.
/// </summary>
/* #1776 own-store: the servers here live in one scratch database of this test's own, so nothing shares rows with another test. */
public sealed class FinOpsInventoryHealthScoreLiveTests
{
    private const int TimeoutSeconds = 30;

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task InventoryHealthScore_EqualsTheUtilizationScore_PerServer()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live inventory health-score test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = DateTime.UtcNow;
        // (name, cpu samples, physical MB, buffer pool MB, total MB, used MB)
        var servers = new (string Name, int[] Cpu, int PhysMb, int BpMb, int TotalMb, int UsedMb)[]
        {
            ("darling-health-healthy", [10, 12, 14], 65536, 40000, 100000, 40000),
            ("darling-health-strained", [60, 85, 95], 16384, 15800, 100000, 97000),
        };

        var ids = new List<int>();
        long collectionId = 1;
        foreach (var sv in servers)
        {
            var (id, name) = await FinOpsIdleCoverageLiveTests.RegisterAsync(c, sv.Name, ct);
            ids.Add(id);
            var at = DateTime.SpecifyKind(now.AddHours(-1), DateTimeKind.Unspecified);
            foreach (var cpu in sv.Cpu)
            {
                await Exec(c, ct,
                    "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, $5, 1)",
                    collectionId++, at, id, name, cpu);
            }

            await Exec(c, ct,
                "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, available_physical_memory_mb, target_server_memory_mb, total_server_memory_mb, buffer_pool_mb) VALUES ($1, $2, $3, $4, $5, 1000, $5, $5, $6)",
                collectionId++, at, id, name, sv.PhysMb, sv.BpMb);
            await Exec(c, ct,
                "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb, used_size_mb) VALUES ($1, $2, $3, $4, 'DbOne', $5, $6)",
                collectionId++, at, id, name, sv.TotalMb, sv.UsedMb);
        }

        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);
        var fleet = await DarlingFinOpsInventoryReader.GetServerMetricsAsync(dataSource, rollups, coverage, TimeoutSeconds, null, ct);

        var scores = new List<int?>();
        foreach (var id in ids)
        {
            var util = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(dataSource, id, TimeoutSeconds, ct);
            Assert.NotNull(util);
            var totals = await FinOpsUtilizationFigures.GetLatestStorageTotalsAsync(dataSource, id, TimeoutSeconds, ct);
            var free = totals is { } t ? FinOpsUtilizationFigures.FreeSpacePct(t.AllocatedMb, t.FreeMb) : 100m;

            Assert.Equal(FinOpsUtilizationFigures.HealthScoreOrNull(util, free), fleet[id].HealthScore);
            scores.Add(fleet[id].HealthScore);
        }

        Assert.NotNull(scores[0]);
        Assert.NotEqual(scores[0], scores[1]);
    }

    private static async Task Exec(NpgsqlConnection c, CancellationToken ct, string sql, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, c);
        foreach (var value in values)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
