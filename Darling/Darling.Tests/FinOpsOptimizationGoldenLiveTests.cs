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

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Golden-fixture pin for the four FinOps Optimization reads (<c>GetIdleDatabasesAsync</c>, <c>GetTempdbSummaryAsync</c>,
/// <c>GetWaitCategorySummaryAsync</c> and <c>GetExpensiveQueriesAsync</c>). A deterministic seeded store is read and
/// the results are serialized (declaration-order public properties, decimals at their stored scale, times as offsets from
/// today's UTC midnight) and compared byte for byte with <c>Fixtures/FinOpsOptimization/golden.json</c>. Set
/// <c>DARLING_WRITE_GOLDEN=1</c> to regenerate it.
///
/// <para>Server A holds: a size snapshot with an idle database that has no query rows at all (no last execution), an idle
/// database whose only query rows are older than the window (a last execution before the cutoff), a busy database, a
/// database whose rows are inside the window but ran nothing and last ran before the cutoff, and a system database;
/// tempdb samples with a newer and an older-than-24-hours row; waits stored with and without the trailing space, in
/// several categories with two categories tied on total time; and query rows that group into more statements than the
/// top-N of 3, with a tie on CPU and one statement carrying a plan. Server B has no rows at all.</para>
/// </summary>
public sealed class FinOpsOptimizationGoldenLiveTests
{
    internal const string ServerNameA = "darling-finops-opt-golden-a";
    internal const string ServerNameB = "darling-finops-opt-golden-b";
    internal static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    internal static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);

    [Fact]
    public Task OptimizationReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var map = new Dictionary<string, object?>();
            foreach (var (key, id) in new[] { ("a", ServerIdA), ("b", ServerIdB) })
            {
                map[key] = new Dictionary<string, object?>
                {
                    ["idleDatabases"] = await viewer.GetIdleDatabasesAsync(id, 7, ct),
                    ["tempdbSummary"] = await viewer.GetTempdbSummaryAsync(id, ct),
                    ["waitCategories"] = await viewer.GetWaitCategorySummaryAsync(id, 24, ct),
                    ["expensiveQueries"] = await viewer.GetExpensiveQueriesAsync(id, 24, 3, ct),
                };
            }
            return Serialize(anchor, map);
        });

    internal static async Task RunAsync(Func<string, DateTime, CancellationToken, Task<string>> read)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps optimization golden test.");

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

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* Size snapshots: an older snapshot (ignored, the newest wins) and the latest one. Two files for IdleNoRows. */
        foreach (var (db, size) in new[] { ("IdleNoRows", 100m), ("BusyAlpha", 100m), ("master", 10m) })
            await Size(connection, ct, now.AddHours(-30), db, size);
        await Size(connection, ct, now.AddHours(-3), "IdleNoRows", 500.25m);
        await Size(connection, ct, now.AddHours(-3), "IdleNoRows", 250.5m);
        await Size(connection, ct, now.AddHours(-3), "IdleOld", 1024m);
        await Size(connection, ct, now.AddHours(-3), "IdleZero", 64m);
        await Size(connection, ct, now.AddHours(-3), "BusyAlpha", 9999m);
        await Size(connection, ct, now.AddHours(-3), "master", 5000m);

        /* Activity. BusyAlpha ran inside the window. IdleOld's only rows are 8 days old (just outside 7), and its last execution
           is on the server-local clock, read verbatim. IdleZero has rows inside the window that ran nothing, last run before
           the cutoff. */
        await Query(connection, ct, now.AddDays(-2), "BusyAlpha", "0xBH", "SELECT busy", 2_000_000, 12, 100, 60, anchor.AddDays(-1).AddHours(5), null);
        await Query(connection, ct, now.AddDays(-8), "IdleOld", "0xOH", "SELECT old", 1_000_000, 4, 10, 60, anchor.AddDays(-8).AddHours(7).AddMinutes(30), null);
        await Query(connection, ct, now.AddDays(-2), "IdleZero", "0xZH", "SELECT zero", 0, 0, 0, 60, anchor.AddDays(-9).AddHours(2), null);

        /* tempdb: the newest sample is the latest; the 30-hour-old sample is outside the peak window. */
        await Tempdb(connection, ct, now.AddHours(-30), 9000m, 9000m, 9000m, 27000m);
        await Tempdb(connection, ct, now.AddHours(-10), 1500.5m, 300m, 2500m, 4300.5m);
        await Tempdb(connection, ct, now.AddHours(-1), 800m, 1200.25m, 100m, 2100.25m);

        /* Waits. CXPACKET is stored with and without the trailing space and merges. CPU 2300, Storage 1000, Locks 700 and
           Memory 700 tie, Other 100. A wait with no delta, and one with a zero delta, are dropped. */
        await Wait(connection, ct, now.AddHours(-2), "CXPACKET", 1000, 10);
        await Wait(connection, ct, now.AddHours(-3), "CXPACKET ", 500, 5);
        await Wait(connection, ct, now.AddHours(-2), "SOS_SCHEDULER_YIELD", 800, 20);
        await Wait(connection, ct, now.AddHours(-2), "PAGEIOLATCH_SH", 700, 7);
        await Wait(connection, ct, now.AddHours(-2), "WRITELOG", 300, 3);
        await Wait(connection, ct, now.AddHours(-2), "LCK_M_X", 700, 7);
        await Wait(connection, ct, now.AddHours(-2), "RESOURCE_SEMAPHORE", 700, 9);
        await Wait(connection, ct, now.AddHours(-2), "BROKER_TASK_STOP", 100, 1);
        await Wait(connection, ct, now.AddHours(-2), "ZERO_WAIT", 0, 4);
        await Wait(connection, ct, now.AddHours(-2), "NULL_WAIT", null, 4);
        await Wait(connection, ct, now.AddHours(-30), "OUTSIDE_WAIT", 5000, 50);

        /* Expensive queries: five statements, top-N 3. Two tie on CPU (Beta and Gamma, 4000 ms). One carries a plan, one has
           a long text, one has a zero sample interval and is dropped, and one has only a tiny CPU figure. */
        await Query(connection, ct, now.AddHours(-2), "Alpha", "0xE1", "SELECT alpha", 9_000_000, 30, 1000, 60, null, "<ShowPlanXML />");
        await Query(connection, ct, now.AddHours(-3), "Alpha", "0xE1", "SELECT alpha", 2_500_000, 7, 400, 60, null, null);
        await Query(connection, ct, now.AddHours(-2), "Beta", "0xE2", "SELECT beta", 4_000_000, 8, 300, 60, null, null);
        await Query(connection, ct, now.AddHours(-2), "Gamma", "0xE3", "SELECT gamma", 4_000_000, 8, 200, 60, null, null);
        await Query(connection, ct, now.AddHours(-2), "Delta", "0xE4", "SELECT " + new string('d', 260), 3_000_000, 3, 50, 60, null, null);
        await Query(connection, ct, now.AddHours(-2), "Echo", "0xE5", "SELECT echo", 1_000, 1, 1, 60, null, null);
        await Query(connection, ct, now.AddHours(-2), "Foxtrot", "0xE6", "SELECT foxtrot", 99_000_000, 999, 9999, 0, null, null);
    }

    private static Task Size(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, decimal sizeMb) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO database_size_stats (collection_id, collection_time, server_id, server_name, database_name, total_size_mb) VALUES ($1, $2, $3, $4, $5, $6)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, db, sizeMb);

    private static Task Query(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string handle, string text,
        long cpuUs, long exec, long logical, int intervalSeconds, DateTime? lastExecution, string? planXml) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                query_text, delta_worker_time, delta_execution_count, delta_logical_reads, sample_interval_seconds,
                last_execution_time, query_plan_xml)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, db, "0xQ" + handle, handle, text, cpuUs, exec, logical,
            intervalSeconds, lastExecution, planXml);

    private static Task Tempdb(NpgsqlConnection c, CancellationToken ct, DateTime at, decimal user, decimal internalObjects,
        decimal versionStore, decimal total) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO tempdb_stats (collection_id, collection_time, server_id, server_name, user_object_reserved_mb,
                internal_object_reserved_mb, version_store_reserved_mb, total_reserved_mb)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, user, internalObjects, versionStore, total);

    private static Task Wait(NpgsqlConnection c, CancellationToken ct, DateTime at, string type, long? waitMs, long tasks) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_wait_time_ms, delta_waiting_tasks)
              VALUES ($1,$2,$3,$4,$5,$6,$7)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, type, waitMs, tasks);

    /// <summary>Serializes the results as indented JSON: public settable properties in declaration order, decimals at their
    /// stored scale, and every DateTime as an offset in days from <paramref name="anchor"/>.</summary>
    internal static string Serialize(DateTime anchor, object results)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            WriteValue(writer, results, anchor);
        }
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n") + "\n";
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value, DateTime anchor)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s.ReplaceLineEndings("\n")); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case DateTime t:
                var offset = t - anchor;
                writer.WriteStringValue($"anchor{(offset < TimeSpan.Zero ? "-" : "+")}{Math.Abs(offset.TotalDays).ToString("0.######", System.Globalization.CultureInfo.InvariantCulture)}d");
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
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsOptimization", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
        {
            Directory.CreateDirectory(Path.GetDirectoryName(GoldenSourcePath())!);
            File.WriteAllText(GoldenSourcePath(), actual);
        }

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsOptimization", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
