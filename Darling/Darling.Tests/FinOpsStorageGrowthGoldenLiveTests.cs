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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Golden-fixture pin for the FinOps Storage Growth reads (<c>GetStorageGrowthAsync</c>,
/// <c>GetObjectGrowthHeatmapDataAsync</c> and <c>GetObjectIndexDetailAsync</c>). A deterministic seeded store is read
/// and the results are serialized (declaration-order public properties, decimals at their stored scale, times as
/// offsets from today's UTC midnight) and compared byte for byte with <c>Fixtures/FinOpsStorageGrowth/golden.json</c>.
/// Set <c>DARLING_WRITE_GOLDEN=1</c> to regenerate it. Every ORDER BY in these reads ends in a unique key, so a tied
/// sort key still has one order and the rows are compared in that order.
///
/// <para>Server A: a latest snapshot, a 7-day and a 30-day at-or-before snapshot (each with a newer decoy that must
/// be ignored), one database with no 30-day snapshot, one with no past snapshot, a Hyperscale-style log row with no
/// size, and two databases tied on both growth figures; plus the object-growth data for one database (tables across
/// several days, two snapshots on the latest day, a tie on growth, a table that exists only on the latest day and
/// more tables than the top-N) and the index detail for tables with two indexes, a heap and NULL usage columns.
/// Server B: one snapshot, five days old, so the latest probe takes the fallback. Server C: a latest snapshot ten
/// days old and one forty days old, so both at-or-before probes take the fallback. Server D has no rows.</para>
/// </summary>
public sealed class FinOpsStorageGrowthGoldenLiveTests
{
    internal const string ServerNameA = "darling-finops-sg-golden-a";
    internal const string ServerNameB = "darling-finops-sg-golden-b";
    internal const string ServerNameC = "darling-finops-sg-golden-c";
    internal const string ServerNameD = "darling-finops-sg-golden-d";
    internal static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    internal static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
    internal static readonly int ServerIdC = ServerIdHelper.GetDeterministicHashCode(ServerNameC);
    internal static readonly int ServerIdD = ServerIdHelper.GetDeterministicHashCode(ServerNameD);
    internal const string HeatDb = "HeatDb";

    [Fact]
    public Task StorageGrowthReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var map = new Dictionary<string, object?>();
            foreach (var (key, id) in new[] { ("a", ServerIdA), ("b", ServerIdB), ("c", ServerIdC), ("d", ServerIdD) })
            {
                var heat = await viewer.GetObjectGrowthHeatmapDataAsync(id, HeatDb, 30, 4, ct);
                map[key] = new Dictionary<string, object?>
                {
                    ["storageGrowth"] = await viewer.GetStorageGrowthAsync(id, ct),
                    ["heatmapObjects"] = heat.Objects,
                    ["heatmapSamples"] = heat.Samples,
                    ["indexDetailOrders"] = await viewer.GetObjectIndexDetailAsync(id, HeatDb, "dbo", "Orders", ct),
                    ["indexDetailHeap"] = await viewer.GetObjectIndexDetailAsync(id, HeatDb, "dbo", "Heap", ct),
                    ["indexDetailMissing"] = await viewer.GetObjectIndexDetailAsync(id, HeatDb, "dbo", "NoSuchTable", ct),
                };
            }
            return FinOpsOptimizationGoldenLiveTests.Serialize(anchor, map);
        });

    [Fact]
    public Task StorageGrowthReads_MatchGoldenFixture_ThroughTheStorageReaderAndRowMappers() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var map = new Dictionary<string, object?>();
            foreach (var (key, id) in new[] { ("a", ServerIdA), ("b", ServerIdB), ("c", ServerIdC), ("d", ServerIdD) })
            {
                var windowStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-30), DateTimeKind.Unspecified);
                var heat = await DarlingFinOpsStorageGrowthReader.GetObjectGrowthHeatmapDataAsync(dataSource, id, HeatDb, windowStart, 30, 4, 30, ct);
                map[key] = new Dictionary<string, object?>
                {
                    ["storageGrowth"] = (await DarlingFinOpsStorageGrowthReader.GetStorageGrowthAsync(dataSource, id, DateTime.UtcNow, 30, ct)).ConvertAll(StorageGrowthRow.From),
                    ["heatmapObjects"] = heat.Objects.ConvertAll(o => ObjectSizeGrowthRow.From(o, HeatDb)),
                    ["heatmapSamples"] = heat.Samples,
                    ["indexDetailOrders"] = (await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(dataSource, id, HeatDb, "dbo", "Orders", 30, ct)).ConvertAll(IndexUsageRow.From),
                    ["indexDetailHeap"] = (await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(dataSource, id, HeatDb, "dbo", "Heap", 30, ct)).ConvertAll(IndexUsageRow.From),
                    ["indexDetailMissing"] = (await DarlingFinOpsStorageGrowthReader.GetObjectIndexDetailAsync(dataSource, id, HeatDb, "dbo", "NoSuchTable", 30, ct)).ConvertAll(IndexUsageRow.From),
                };
            }
            return FinOpsOptimizationGoldenLiveTests.Serialize(anchor, map);
        });

    internal static async Task RunAsync(Func<string, DateTime, CancellationToken, Task<string>> read)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps storage growth golden test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var anchor = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, anchor, ct);
        }

        AssertGolden(await read(scratch.ConnectionString, anchor, ct));
    }

    private static async Task SeedAsync(NpgsqlConnection c, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdB, ServerNameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdC, ServerNameC, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, ServerIdD, ServerNameD, ct);

        /* Server A, latest snapshot at today's midnight. The 7-day point is the newest snapshot at or before now-7d (anchor-8d);
           anchor-6d is a decoy that is newer than that cutoff and must be ignored. The 30-day point is anchor-31d, decoy anchor-29d. */
        var latest = anchor;
        var p7 = anchor.AddDays(-8);
        var p30 = anchor.AddDays(-31);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "GrowAlpha", 1, 1000.5m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "GrowAlpha", 2, 250m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "GrowBeta", 1, 800m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "NewGamma", 1, 40.25m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "TieDelta", 1, 500m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "TieEpsilon", 1, 500m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "HyperDb", 1, 300m);
        await Size(c, ct, ServerIdA, ServerNameA, latest, "HyperDb", 2, null);
        await Size(c, ct, ServerIdA, ServerNameA, p7, "GrowAlpha", 1, 900m);
        await Size(c, ct, ServerIdA, ServerNameA, p7, "GrowAlpha", 2, 200m);
        await Size(c, ct, ServerIdA, ServerNameA, p7, "GrowBeta", 1, 780m);
        await Size(c, ct, ServerIdA, ServerNameA, p7, "TieDelta", 1, 400m);
        await Size(c, ct, ServerIdA, ServerNameA, p7, "TieEpsilon", 1, 400m);
        await Size(c, ct, ServerIdA, ServerNameA, p7, "HyperDb", 1, 290m);
        await Size(c, ct, ServerIdA, ServerNameA, p30, "GrowAlpha", 1, 700m);
        await Size(c, ct, ServerIdA, ServerNameA, p30, "GrowAlpha", 2, 100m);
        await Size(c, ct, ServerIdA, ServerNameA, p30, "TieDelta", 1, 300m);
        await Size(c, ct, ServerIdA, ServerNameA, p30, "TieEpsilon", 1, 300m);
        await Size(c, ct, ServerIdA, ServerNameA, anchor.AddDays(-6), "GrowAlpha", 1, 99999m);
        await Size(c, ct, ServerIdA, ServerNameA, anchor.AddDays(-29), "GrowAlpha", 1, 88888m);

        /* Azure sibling database on server A: one whole-database row per snapshot (file_id NULL, file_name = the sibling name),
           so HasSiblingRow is true. The p30 row is an old-shape row (no used_size_mb); ExcludePreFixRows drops it, so the
           30-day point and growth are blank. Were it counted, they would be 100 MB and 500 MB. */
        await Sibling(c, ct, ServerIdA, ServerNameA, latest, "SiblingOmega", 600m, 500m);
        await Sibling(c, ct, ServerIdA, ServerNameA, p7, "SiblingOmega", 560m, 450m);
        await Sibling(c, ct, ServerIdA, ServerNameA, p30, "SiblingOmega", 100m, null);

        /* Server B: one snapshot, so the latest probe's two-day window is empty and the fallback finds it. */
        await Size(c, ct, ServerIdB, ServerNameB, anchor.AddDays(-5), "OnlyBeta", 1, 123.75m);

        /* Server C: both at-or-before probes take the fallback; the 7-day point is the latest itself and is dropped. */
        await Size(c, ct, ServerIdC, ServerNameC, anchor.AddDays(-10), "StaleGamma", 1, 600m);
        await Size(c, ct, ServerIdC, ServerNameC, anchor.AddDays(-40), "StaleGamma", 1, 450m);

        /* Object growth for HeatDb on server A. Earliest instant anchor-5d+3h, then -3d+3h, then two on the latest day. */
        var e = anchor.AddDays(-5).AddHours(3);
        var m = anchor.AddDays(-3).AddHours(3);
        var l1 = anchor.AddDays(-1).AddHours(3);
        var l2 = anchor.AddDays(-1).AddHours(15);
        foreach (var (t, orders, lines, tie1, tie2, flat) in new[]
                 {
                     (e, 100m, 50m, 10m, 20m, 30m),
                     (m, 120m, 70m, 15m, 25m, 30m),
                     (l1, 150m, 90m, 20m, 30m, 30m),
                     (l2, 160m, 100m, 20m, 30m, 30m),
                 })
        {
            await Idx(c, ct, t, "Orders", 1, "PK_Orders", "CLUSTERED", orders * 0.75m, 800_000, 5000, 100, 20, 400, anchor.AddDays(-1), null, anchor.AddHours(-2));
            await Idx(c, ct, t, "Orders", 2, "IX_Orders_Nc", "NONCLUSTERED", orders * 0.25m, 800_000, null, null, null, null, null, null, null);
            await Idx(c, ct, t, "Lines", 1, "PK_Lines", "CLUSTERED", lines, 2_000_000, 10, 0, 0, 5, null, null, null);
            await Idx(c, ct, t, "TiedOne", 1, "PK_TiedOne", "CLUSTERED", tie1, 1000, 0, 0, 0, 0, null, null, null);
            await Idx(c, ct, t, "TiedTwo", 1, "PK_TiedTwo", "CLUSTERED", tie2, 1000, 0, 0, 0, 0, null, null, null);
            await Idx(c, ct, t, "Flat", 1, "PK_Flat", "CLUSTERED", flat, 10, 1, 1, 1, 1, null, null, null);
            await Idx(c, ct, t, "Heap", 0, null, "HEAP", orders / 10m, 77, 3, 0, 0, 0, null, null, null);
        }
        await Idx(c, ct, l2, "NewTable", 1, "PK_NewTable", "CLUSTERED", 12.5m, 123, 0, 0, 0, 0, null, null, null);
        await Idx(c, ct, l2, "Orders", 3, "IX_Orders_Extra", "NONCLUSTERED", 7m, 800_000, 0, 0, 0, 9, null, null, null);
    }

    private static Task Size(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime at, string db, int fileId, decimal? sizeMb) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, file_id, total_size_mb) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            CollectionIdGenerator.Next(), at, serverId, serverName, db, fileId, sizeMb);

    private static Task Sibling(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime at, string db, decimal sizeMb, decimal? usedMb) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, file_id, file_name, total_size_mb, used_size_mb) VALUES ($1, $2, $3, $4, $5, NULL, $6, $7, $8)",
            CollectionIdGenerator.Next(), at, serverId, serverName, db, AzureSiblingDatabaseSize.FileName, sizeMb, usedMb);

    private static Task Idx(NpgsqlConnection c, CancellationToken ct, DateTime at, string table, int indexId, string? indexName, string type,
        decimal reserved, long rows, long? seeks, long? scans, long? lookups, long? updates,
        DateTime? lastSeek, DateTime? lastScan, DateTime? lastUpdate) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_id, table_name,
                index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates,
                last_user_seek, last_user_scan, last_user_update)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, HeatDb, "dbo", 1000 + table.Sum(ch => (int)ch), table,
            indexId, indexName, type, reserved, reserved * 0.9m, rows, seeks, scans, lookups, updates, lastSeek, lastScan, lastUpdate);

    private static string GoldenSourcePath([CallerFilePath] string testFile = "") =>
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsStorageGrowth", "golden.json");

    internal static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
        {
            Directory.CreateDirectory(Path.GetDirectoryName(GoldenSourcePath())!);
            File.WriteAllText(GoldenSourcePath(), actual);
        }

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsStorageGrowth", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
