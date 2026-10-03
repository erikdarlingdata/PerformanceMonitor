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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
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

/// <summary>
/// Golden-fixture pin for the three FinOps Utilization reads (<c>GetUtilizationEfficiencyAsync</c>,
/// <c>GetProvisioningTrendAsync</c>, <c>GetMemoryGrantEfficiencyAsync</c>). A deterministic seeded store is read
/// through the reads and the results are serialized (declaration-order properties with a public setter, decimals at
/// their stored scale, day values as offsets from the seed anchor) and compared byte-for-byte to
/// <c>Fixtures/FinOpsUtilization/golden.json</c>. Set <c>DARLING_WRITE_GOLDEN=1</c> to regenerate the fixture.
///
/// <para>Five servers are seeded. Server A has rows relative to now (all inside the 24-hour window) for the
/// point-in-time efficiency read. Server B has rows on fixed days before today's UTC midnight at fixed hours, for the
/// two per-day reads, so the day buckets never depend on the time of day the test runs; its days reach UNDER_PROVISIONED,
/// OVER_PROVISIONED and RIGHT_SIZED. Server C is an Azure SQL Database (engine edition 5, a vCore count that differs from
/// its CPU count, no worker count). Server D has memory rows and no CPU rows (the empty status). Server E has CPU rows and
/// no memory rows (the efficiency read returns null).</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FinOpsUtilizationGoldenLiveTests
{
    private const string ServerNameA = "darling-finops-util-golden-a";
    private const string ServerNameB = "darling-finops-util-golden-b";
    private const string ServerNameC = "darling-finops-util-golden-c";
    private const string ServerNameD = "darling-finops-util-golden-d";
    private const string ServerNameE = "darling-finops-util-golden-e";
    private static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    private static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
    private static readonly int ServerIdC = ServerIdHelper.GetDeterministicHashCode(ServerNameC);
    private static readonly int ServerIdD = ServerIdHelper.GetDeterministicHashCode(ServerNameD);
    private static readonly int ServerIdE = ServerIdHelper.GetDeterministicHashCode(ServerNameE);

    [Fact]
    public Task UtilizationReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var efficiency = new Dictionary<string, object?>
            {
                ["a"] = await viewer.GetUtilizationEfficiencyAsync(ServerIdA, ct),
                ["c"] = await viewer.GetUtilizationEfficiencyAsync(ServerIdC, ct),
                ["d"] = await viewer.GetUtilizationEfficiencyAsync(ServerIdD, ct),
                ["e"] = await viewer.GetUtilizationEfficiencyAsync(ServerIdE, ct),
            };
            var trend = await viewer.GetProvisioningTrendAsync(ServerIdB, ct);
            var grants = await viewer.GetMemoryGrantEfficiencyAsync(ServerIdB, hoursBack: 24 * 6, ct);
            return Serialize(anchor, efficiency, trend, grants);
        });

    [Fact]
    public Task UtilizationReads_MatchGoldenFixture_ThroughTheStorageReaderAndRowMappers() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var efficiency = new Dictionary<string, object?>();
            foreach (var (key, id) in new[] { ("a", ServerIdA), ("c", ServerIdC), ("d", ServerIdD), ("e", ServerIdE) })
            {
                var dto = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(dataSource, id, 30, ct);
                efficiency[key] = dto is null ? null : UtilizationEfficiencyRow.From(dto);
            }
            var trend = (await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(dataSource, ServerIdB, 30, ct))
                .ConvertAll(ProvisioningTrendRow.From);
            var grants = (await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(dataSource, ServerIdB, 24 * 6, 30, ct))
                .ConvertAll(MemoryGrantEfficiencyRow.From);
            return Serialize(anchor, efficiency, trend, grants);
        });

    private static async Task RunAsync(Func<string, DateTime, CancellationToken, Task<string>> read)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps utilization golden test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupAsync(connection, ct);

        var succeeded = false;
        try
        {
            var anchor = DateTime.SpecifyKind(DateTime.UtcNow.Date, DateTimeKind.Unspecified);
            await SeedAsync(connection, anchor, ct);

            var actual = await read(connectionString!, anchor, ct);
            AssertGolden(actual);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanup, cleanupCt) =>
                await CleanupAsync(cleanup, cleanupCt));
        }
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdC, ServerNameC, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdD, ServerNameD, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdE, ServerNameE, ct);

        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* Server A: four CPU samples, two memory snapshots (the newest wins), grant pressure, an Enterprise 8-core row. */
        var cpu = new[] { 10, 20, 30, 95 };
        for (var i = 0; i < cpu.Length; i++)
            await InsertCpuAsync(connection, ServerIdA, ServerNameA, now.AddHours(-1 - i), cpu[i], ct);
        await InsertMemoryAsync(connection, ServerIdA, ServerNameA, now.AddHours(-5), 6000m, 8000m, 16000m, 5000m, 512, 80, ct);
        await InsertMemoryAsync(connection, ServerIdA, ServerNameA, now.AddHours(-2), 7000m, 8000m, 16000m, 6000m, 512, 100, ct);
        await InsertGrantAsync(connection, ServerIdA, ServerNameA, now.AddHours(-3), 100m, 40m, 1000m, 4, 2, 1, 3, ct);
        await InsertGrantAsync(connection, ServerIdA, ServerNameA, now.AddHours(-2), 150m, 60m, 1000m, 6, 0, null, 2, ct);
        await InsertPropertiesAsync(connection, ServerIdA, ServerNameA, now.AddDays(-1), "Enterprise Edition", 3, 8, null, ct);

        /* Server B: four fixed days at 12:00 and 13:00 UTC, varied load, an Azure SQL Database row. */
        var avgCpu = new[] { 5, 22, 45, 80 };
        for (var d = 0; d < 4; d++)
        {
            var day = anchor.AddDays(-4 + d);
            await InsertCpuAsync(connection, ServerIdB, ServerNameB, day.AddHours(12), avgCpu[d], ct);
            await InsertCpuAsync(connection, ServerIdB, ServerNameB, day.AddHours(13), avgCpu[d] + 7, ct);
            await InsertMemoryAsync(connection, ServerIdB, ServerNameB, day.AddHours(12), 3000m + (d * 400m), 4000m, 8192m, 2000m, 300, 40 + d, ct);
            await InsertGrantAsync(connection, ServerIdB, ServerNameB, day.AddHours(12), 200m + (d * 50m), 90m, 2000m, 5 + d, d, d == 2 ? null : 1, d, ct);
            await InsertGrantAsync(connection, ServerIdB, ServerNameB, day.AddHours(13), 300m + (d * 25m), 120m, 2000m, 3, d, 0, d + 1, ct);
        }
        await InsertPropertiesAsync(connection, ServerIdB, ServerNameB, anchor.AddDays(-2), "SQL Azure", 5, 4, 2, ct);

        /* Server B, two earlier days. Day -5 has low CPU and no memory pressure (OVER_PROVISIONED). Day -6 sits in the middle
           band with no pressure and no grant rows (RIGHT_SIZED). */
        var overDay = anchor.AddDays(-5);
        await InsertCpuAsync(connection, ServerIdB, ServerNameB, overDay.AddHours(12), 4, ct);
        await InsertCpuAsync(connection, ServerIdB, ServerNameB, overDay.AddHours(13), 9, ct);
        await InsertMemoryAsync(connection, ServerIdB, ServerNameB, overDay.AddHours(12), 2500m, 4000m, 8192m, 1500m, 300, 30, ct);
        await InsertGrantAsync(connection, ServerIdB, ServerNameB, overDay.AddHours(12), 100m, 40m, 2000m, 2, 0, 0, 0, ct);
        await InsertGrantAsync(connection, ServerIdB, ServerNameB, overDay.AddHours(13), 120m, 50m, 2000m, 3, 0, 0, 0, ct);
        var middleDay = anchor.AddDays(-6);
        await InsertCpuAsync(connection, ServerIdB, ServerNameB, middleDay.AddHours(12), 30, ct);
        await InsertCpuAsync(connection, ServerIdB, ServerNameB, middleDay.AddHours(13), 50, ct);
        await InsertMemoryAsync(connection, ServerIdB, ServerNameB, middleDay.AddHours(12), 3200m, 4000m, 8192m, 2200m, 300, 60, ct);

        /* Server C: an Azure SQL Database. Its vCore count (2) differs from the scheduler count (4), the workers are not collected. */
        await InsertCpuAsync(connection, ServerIdC, ServerNameC, now.AddHours(-2), 12, ct);
        await InsertCpuAsync(connection, ServerIdC, ServerNameC, now.AddHours(-1), 25, ct);
        await InsertMemoryAsync(connection, ServerIdC, ServerNameC, now.AddHours(-1), 1800m, 2000m, 4096m, 900m, 0, null, ct);
        await InsertPropertiesAsync(connection, ServerIdC, ServerNameC, now.AddDays(-1), "Standard", 5, 4, 2, ct);

        /* Server D: memory rows, no CPU rows. */
        await InsertMemoryAsync(connection, ServerIdD, ServerNameD, now.AddHours(-1), 3000m, 4000m, 8192m, 2000m, 512, 50, ct);
        await InsertPropertiesAsync(connection, ServerIdD, ServerNameD, now.AddDays(-1), "Standard Edition", 2, 4, null, ct);

        /* Server E: CPU rows, no memory rows. */
        await InsertCpuAsync(connection, ServerIdE, ServerNameE, now.AddHours(-1), 20, ct);
    }

    private static Task InsertCpuAsync(NpgsqlConnection c, int id, string name, DateTime at, int pct, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, 1)",
            CollectionIdGenerator.Next(), at, id, name, at, pct);

    private static Task InsertMemoryAsync(NpgsqlConnection c, int id, string name, DateTime at, decimal total, decimal target,
        decimal physical, decimal bufferPool, int maxWorkers, int? currentWorkers, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            CollectionIdGenerator.Next(), at, id, name, physical, total, target, bufferPool, maxWorkers, currentWorkers);

    private static Task InsertGrantAsync(NpgsqlConnection c, int id, string name, DateTime at, decimal granted, decimal used,
        decimal target, int grantees, int waiters, long? timeoutDelta, long forcedDelta, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, target_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            CollectionIdGenerator.Next(), at, id, name, target, granted, used, grantees, waiters, timeoutDelta, forcedDelta);

    private static Task InsertPropertiesAsync(NpgsqlConnection c, int id, string name, DateTime at, string edition,
        int engineEdition, int cpuCount, int? vcores, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, cpu_count, vcore_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), at, id, name, edition, engineEdition, cpuCount, vcores);

    private static Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM cpu_utilization_stats WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC},{ServerIdD},{ServerIdE}); " +
            $"DELETE FROM memory_stats WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC},{ServerIdD},{ServerIdE}); " +
            $"DELETE FROM memory_grant_stats WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC},{ServerIdD},{ServerIdE}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC},{ServerIdD},{ServerIdE}); " +
            $"DELETE FROM servers WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC},{ServerIdD},{ServerIdE})");

    /// <summary>Serializes the three results as indented JSON: public settable properties in declaration order,
    /// decimals at their stored scale, and every DateTime as a whole-day offset from <paramref name="anchor"/>.</summary>
    private static string Serialize(DateTime anchor, object efficiency, object trend, object grants)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("utilizationEfficiency");
            WriteValue(writer, efficiency, anchor);
            writer.WritePropertyName("provisioningTrend");
            WriteValue(writer, trend, anchor);
            writer.WritePropertyName("memoryGrantEfficiency");
            WriteValue(writer, grants, anchor);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n") + "\n";
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, DateTime anchor)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case DateTime t:
                var offset = t - anchor;
                writer.WriteStringValue($"anchor{(offset < TimeSpan.Zero ? "-" : "+")}{Math.Abs(offset.TotalDays).ToString("0.####", System.Globalization.CultureInfo.InvariantCulture)}d");
                break;
            case System.Collections.IDictionary map:
                writer.WriteStartObject();
                foreach (System.Collections.DictionaryEntry entry in map)
                {
                    writer.WritePropertyName((string)entry.Key);
                    WriteValue(writer, entry.Value, anchor);
                }
                writer.WriteEndObject();
                break;
            case System.Collections.IEnumerable list:
                writer.WriteStartArray();
                foreach (var item in list) WriteValue(writer, item, anchor);
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStartObject();
                foreach (var p in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.SetMethod is { IsPublic: true })
                             .OrderBy(p => p.MetadataToken))
                {
                    writer.WritePropertyName(p.Name);
                    WriteValue(writer, p.GetValue(value), anchor);
                }
                writer.WriteEndObject();
                break;
        }
    }

    private static string GoldenSourcePath([CallerFilePath] string testFile = "") =>
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsUtilization", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
            File.WriteAllText(GoldenSourcePath(), actual);

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsUtilization", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
