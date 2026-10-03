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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Golden-fixture pin for the two FinOps Inventory reads (<c>GetServerMetricsAsync</c>, <c>GetServerInventoryAsync</c>).
/// A deterministic seeded store is read through the reads and the results are serialized (declaration-order properties
/// with a public setter, decimals at their stored scale, times as offsets from the seed anchor) and compared
/// byte-for-byte to <c>Fixtures/FinOpsInventory/golden.json</c>. Set <c>DARLING_WRITE_GOLDEN=1</c> to regenerate it.
///
/// <para>Three servers are seeded, with letter-suffixed names: A is busy and enabled, with a size snapshot, a recent and
/// an old query, grant pressure and collection-log rows; B is a disabled Azure SQL Database with a vCore count; C has
/// no CPU samples and no hardware columns. Only these servers are serialized, so other rows in a shared store do not
/// change the result.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class FinOpsInventoryGoldenLiveTests
{
    private const string NameA = "darling-finops-inv-golden-a";
    private const string NameB = "darling-finops-inv-golden-b";
    private const string NameC = "darling-finops-inv-golden-c";
    private static readonly int IdA = ServerIdHelper.GetDeterministicHashCode(NameA);
    private static readonly int IdB = ServerIdHelper.GetDeterministicHashCode(NameB);
    private static readonly int IdC = ServerIdHelper.GetDeterministicHashCode(NameC);
    private static readonly int[] Ids = [IdA, IdB, IdC];

    [Fact]
    public Task InventoryReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var metrics = (await viewer.GetServerMetricsAsync(ct)).Where(kv => Ids.Contains(kv.Key))
                .OrderBy(kv => NameOf(kv.Key)).Select(kv => new MetricsEntry { Server = NameOf(kv.Key), Metrics = kv.Value }).ToList();
            var inventory = (await viewer.GetServerInventoryAsync(ct)).Where(r => Ids.Contains(r.ServerId)).ToList();
            return Serialize(anchor, metrics, inventory);
        });

    public sealed class MetricsEntry
    {
        public string Server { get; set; } = "";
        public ViewerDataService.ServerMetricsRow Metrics { get; set; }
    }

    private static string NameOf(int id) => id == IdA ? NameA : id == IdB ? NameB : NameC;

    private static async Task RunAsync(Func<string, DateTime, CancellationToken, Task<string>> read)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps inventory golden test.");

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

    private static async Task SeedAsync(NpgsqlConnection c, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, IdA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, IdB, NameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(c, IdC, NameC, ct);
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET monthly_cost_usd = 1234.5 WHERE server_id = $1", IdA);
        await DarlingMcpTestData.ExecAsync(c, ct, "UPDATE servers SET is_enabled = FALSE, display_name = 'Golden Disabled B' WHERE server_id = $1", IdB);

        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* A: CPU samples and grants inside the 24-hour window, two size snapshots (the newest wins), one database active
           and one never executed. */
        var cpu = new[] { 10, 20, 30, 95 };
        for (var i = 0; i < cpu.Length; i++)
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, 1)",
                CollectionIdGenerator.Next(), now.AddHours(-1 - i), IdA, NameA, now.AddHours(-1 - i), cpu[i]);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 512, 100)",
            CollectionIdGenerator.Next(), now.AddHours(-2), IdA, NameA);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, waiter_count, timeout_error_count_delta, forced_grant_count_delta, granted_memory_mb, target_memory_mb) VALUES ($1, $2, $3, $4, 3, 1, 2, 150, 1000)",
            CollectionIdGenerator.Next(), now.AddHours(-2), IdA, NameA);
        foreach (var (db, size, at) in new[] { ("UserDbBusy", 2048m, now.AddHours(-3)), ("UserDbIdle", 1024m, now.AddHours(-3)), ("UserDbBusy", 1000m, now.AddDays(-2)), ("master", 50m, now.AddHours(-3)) })
            await DarlingMcpTestData.ExecAsync(c, ct,
                "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb) VALUES ($1, $2, $3, $4, $5, $6)",
                CollectionIdGenerator.Next(), at, IdA, NameA, db, size);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle, delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds) VALUES ($1, $2, $3, $4, 'UserDbBusy', '0xHASHGOLD', '0xHANDLEGOLD', 1000, 1000, 5, 300)",
            CollectionIdGenerator.Next(), now.AddDays(-3), IdA, NameA);
        await InsertPropertiesAsync(c, IdA, NameA, anchor.AddDays(-1).AddHours(3), "Enterprise Edition", "16.0.4100.1", "RTM", "CU9", 3, 8, 65536L, 2, 4, true, false, anchor.AddDays(-3).AddHours(7), "Windows Server 2022", "PRIMARY", null, ct);
        await InsertPropertiesAsync(c, IdA, NameA, anchor.AddDays(-9), "Developer Edition", "15.0.2000.5", "RTM", null, 3, 4, 8192L, 1, 4, false, false, null, "Windows Server 2019", null, null, ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status) VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')",
            CollectionIdGenerator.Next(), IdA, NameA, anchor.AddHours(-5));

        /* B: disabled Azure SQL Database, vCore-named, no samples. */
        await InsertPropertiesAsync(c, IdB, NameB, anchor.AddDays(-2), "SQL Azure", "12.0.2000.8", "RTM", null, 5, 2, 913000L, 0, 32, null, null, null, null, null, 4, ct);

        /* C: a monitored server that has no CPU samples and no hardware columns. */
        await InsertPropertiesAsync(c, IdC, NameC, anchor.AddDays(-4), "Standard Edition", "14.0.1000.169", "SP1", "", 2, null, null, null, null, null, null, null, null, null, null, ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 256, NULL)",
            CollectionIdGenerator.Next(), now.AddHours(-4), IdC, NameC);
    }

    private static Task InsertPropertiesAsync(NpgsqlConnection c, int id, string name, DateTime at, string edition, string version,
        string level, string? updateLevel, int engineEdition, int? cpuCount, long? memoryMb, int? sockets, int? coresPerSocket,
        bool? hadr, bool? clustered, DateTime? start, string? os, string? role, int? vcores, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, product_level, product_update_level, engine_edition, cpu_count, physical_memory_mb, socket_count, cores_per_socket, is_hadr_enabled, is_clustered, sqlserver_start_time, host_os_version, ag_replica_role, vcore_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)",
            CollectionIdGenerator.Next(), at, id, name, edition, version, level, updateLevel, engineEdition, cpuCount, memoryMb, sockets, coresPerSocket, hadr, clustered, start, os, role, vcores);

    private static Task CleanupAsync(NpgsqlConnection c, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            $"DELETE FROM cpu_utilization_stats WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM memory_stats WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM memory_grant_stats WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM database_size_stats WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM query_stats WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM collection_log WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({IdA},{IdB},{IdC}); " +
            $"DELETE FROM servers WHERE server_id IN ({IdA},{IdB},{IdC})");

    /// <summary>Serializes the three results as indented JSON: public settable properties in declaration order,
    /// decimals at their stored scale, and every DateTime as a whole-day offset from <paramref name="anchor"/>.</summary>
    private static string Serialize(DateTime anchor, object metrics, object inventory)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("serverMetrics");
            WriteValue(writer, metrics, anchor);
            writer.WritePropertyName("serverInventory");
            WriteValue(writer, inventory, anchor);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n") + "\n";
    }

    /* The three row properties the viewer converts for display depend on the machine's zone and the display mode, so the pin
       reads the UTC instants (InventoryAsOfUtc, LastCollectedUtc) that they are converted from instead. */
    private static readonly HashSet<string> DisplayConverted = ["Clock", "InventoryAsOf", "LastCollected"];

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
            case System.Collections.IEnumerable list:
                writer.WriteStartArray();
                foreach (var item in list) WriteValue(writer, item, anchor);
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStartObject();
                foreach (var p in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.SetMethod is { IsPublic: true } && !DisplayConverted.Contains(p.Name))
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
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsInventory", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
            File.WriteAllText(GoldenSourcePath(), actual);

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsInventory", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
