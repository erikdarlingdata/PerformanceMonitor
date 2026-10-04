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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Golden-fixture pin for the viewer's <c>GetRecommendationsAsync</c> over a deterministic seeded store, at a monthly
/// cost of 1000 and at 0. The rows are serialized field by field (declaration order, decimals at their stored scale)
/// and compared byte for byte with <c>Fixtures/FinOpsRecommendations/golden.json</c>. Set <c>DARLING_WRITE_GOLDEN=1</c>
/// to regenerate it.
///
/// <para>Server A is an Enterprise 14.x standalone server with 16 cores, two TDE databases, two dev/test database
/// names, a CPU series whose mean sits just above 20% with a small spread, a memory series at about a fifth of the
/// RAM and two indexes (one uncompressed 2 GB, one compressed). Its server_properties row carries a memory size that
/// differs from the memory_stats one, so a swapped source changes the output. Server B is an Azure SQL Database
/// (engine edition 5) with the same series: memory and VM advice stand down. Server C has no rows.</para>
///
/// <para>The seeded series are all inside the last 24 hours, so they are far from the 7-day cutoff and inside the
/// utilization read's own 24-hour window.</para>
/// </summary>
public sealed class FinOpsRecommendationsGoldenLiveTests
{
    internal const string ServerNameA = "darling-finops-rec-golden-a";
    internal const string ServerNameB = "darling-finops-rec-golden-b";
    internal const string ServerNameC = "darling-finops-rec-golden-c";
    internal static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    internal static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
    internal static readonly int ServerIdC = ServerIdHelper.GetDeterministicHashCode(ServerNameC);

    [Fact]
    public async Task GetRecommendationsAsync_MatchesGolden_ThroughTheViewer()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps recommendations golden test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var anchor = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, ct);
        }

        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var map = new Dictionary<string, object?>();
        foreach (var (key, id) in new[] { ("a", ServerIdA), ("b", ServerIdB), ("c", ServerIdC) })
        {
            map[key] = new Dictionary<string, object?>
            {
                ["monthly1000"] = await viewer.GetRecommendationsAsync(id, 1000m, ct),
                ["monthly0"] = await viewer.GetRecommendationsAsync(id, 0m, ct),
            };
        }

        AssertGolden(FinOpsOptimizationGoldenLiveTests.Serialize(anchor, map));
    }

    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdB, ServerNameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdC, ServerNameC, ct);
        /* One "now", captured once; every seed time is an offset from it. */
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* Feeds the edition audit (Enterprise, 14.x, standalone, 16 cores) and the VM right-sizing physical-memory
           column; the physical memory here (98304) differs from memory_stats (65536) on purpose. */
        await Props(c, ct, ServerIdA, ServerNameA, now.AddHours(-2), "Enterprise Edition (64-bit)", "14.0.3456.2", 3, 16, 98304);
        /* Engine edition 5: memory and VM advice stand down. */
        await Props(c, ct, ServerIdB, ServerNameB, now.AddHours(-2), "SQL Azure", "12.0.2000.8", 5, 8, 98304);

        /* Feeds the edition audit's TDE branch (two encrypted ONLINE databases) and the dev/test detection. */
        await Config(c, ct, now.AddHours(-3), "SalesA", true);
        await Config(c, ct, now.AddHours(-3), "OrdersA", true);
        await Config(c, ct, now.AddHours(-3), "app_dev_a", false);
        await Config(c, ct, now.AddHours(-3), "qa1_a", false);

        /* Feeds CPU right-sizing, the VM CPU advice and the reserved-capacity check: 30 samples about 45 minutes apart
           over the last 22 hours, mean just above 20% with a spread well under 15% of the mean. */
        int[] cpu = { 20, 22, 24, 21, 23, 22, 20, 24, 22, 21 };
        for (var i = 0; i < 30; i++)
        {
            var at = now.AddMinutes(-(60 + 45 * i));
            await Cpu(c, ct, ServerIdA, ServerNameA, at, cpu[i % cpu.Length]);
            await Cpu(c, ct, ServerIdB, ServerNameB, at, cpu[i % cpu.Length]);
        }

        /* Feeds memory right-sizing and the VM memory advice: 20 samples, SQL memory about a fifth of 65536 MB. */
        for (var i = 0; i < 20; i++)
        {
            var at = now.AddMinutes(-(60 + 60 * i));
            var total = 13000m + 20 * (i % 5);
            await Memory(c, ct, ServerIdA, ServerNameA, at, total);
            await Memory(c, ct, ServerIdB, ServerNameB, at, total);
        }

        /* Feeds the compression candidates: a 2 GB uncompressed index and a compressed one. */
        await Index(c, ct, now.AddHours(-2), 1, "BigUncompressed", 2048m, "NONE");
        await Index(c, ct, now.AddHours(-2), 2, "BigCompressed", 3072m, "PAGE");
    }

    private static Task Props(NpgsqlConnection c, CancellationToken ct, int id, string name, DateTime at, string edition,
        string version, int engineEdition, int cpuCount, int physicalMemoryMb) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, engine_edition, cpu_count, physical_memory_mb, is_hadr_enabled, ag_replica_role) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,FALSE,'Standalone')",
            CollectionIdGenerator.Next(), at, id, name, edition, version, engineEdition, cpuCount, physicalMemoryMb);

    private static Task Config(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, bool encrypted) =>
        DarlingMcpTestData.ExecAsync(c, ct, @"
INSERT INTO database_config
    (config_id, capture_time, server_id, server_name, database_name,
     state_desc, compatibility_level, collation_name, recovery_model, is_read_only,
     is_auto_close_on, is_auto_shrink_on, is_auto_create_stats_on, is_auto_update_stats_on,
     is_auto_update_stats_async_on, is_read_committed_snapshot_on, snapshot_isolation_state,
     is_parameterization_forced, is_query_store_on, is_encrypted, is_trustworthy_on, is_db_chaining_on,
     is_broker_enabled, is_cdc_enabled, is_mixed_page_allocation_on, log_reuse_wait_desc, page_verify_option,
     target_recovery_time_seconds, delayed_durability, is_accelerated_database_recovery_on,
     is_memory_optimized_enabled, is_optimized_locking_on)
VALUES ($1, $2, $3, $4, $5,
        'ONLINE', 140, 'SQL_Latin1_General_CP1_CI_AS', 'FULL', FALSE,
        FALSE, FALSE, TRUE, TRUE,
        FALSE, TRUE, 'OFF',
        FALSE, TRUE, $6, FALSE, FALSE,
        FALSE, FALSE, FALSE, 'NOTHING', 'CHECKSUM',
        60, 'DISABLED', FALSE,
        FALSE, FALSE)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, db, encrypted);

    private static Task Cpu(NpgsqlConnection c, CancellationToken ct, int id, string name, DateTime at, int pct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, 1)",
            CollectionIdGenerator.Next(), at, id, name, at, pct);

    private static Task Memory(NpgsqlConnection c, CancellationToken ct, int id, string name, DateTime at, decimal total) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 65536, $5, 60000, 9000, 512, 100)",
            CollectionIdGenerator.Next(), at, id, name, total);

    private static Task Index(NpgsqlConnection c, CancellationToken ct, DateTime at, int n, string index, decimal reservedMb, string compression) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, key_columns, is_unique, is_primary_key, reserved_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, partition_count, sqlserver_start_time, data_compression_desc) VALUES ($1,$2,$3,$4,'SalesA',5,'dbo',$5,$6,1,$7,'CLUSTERED','[Id]',TRUE,FALSE,$8,1000000,10,10,0,10,0,0,1,$9,$10)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, 1000 + n, "T" + n, index, reservedMb, at.Date.AddDays(-400), compression);

    private static string GoldenSourcePath([CallerFilePath] string testFile = "") =>
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsRecommendations", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
        {
            Directory.CreateDirectory(Path.GetDirectoryName(GoldenSourcePath())!);
            File.WriteAllText(GoldenSourcePath(), actual);
        }

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsRecommendations", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
