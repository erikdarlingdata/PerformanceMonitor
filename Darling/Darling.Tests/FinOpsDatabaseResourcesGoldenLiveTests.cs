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

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Golden-fixture pin for the two FinOps Database Resources reads (<c>GetDatabaseResourceUsageAsync</c> and
/// <c>GetTopResourceConsumersAsync</c>) and for the SQL text of their routed forms. A deterministic seeded store is
/// read and the results are serialized (declaration-order public properties, decimals at their stored scale) and
/// compared byte for byte with <c>Fixtures/FinOpsDatabaseResources/golden.json</c>. Set <c>DARLING_WRITE_GOLDEN=1</c>
/// to regenerate it.
///
/// <para>Server A holds: a database with several query rows (one with a zero sample interval, which the
/// interval-honest filter drops); two databases tied on CPU and on average CPU; a database with zero executions
/// (no average, so absent from the by-average list); a row with a NULL database name (dropped from the by-total list,
/// kept in the by-average list); more databases than the top-N of 3; a database with file I/O and no query rows; and
/// I/O byte counts whose megabytes round at the stored precision. Server B has no rows at all.</para>
///
/// <para>The fresh store has no rollups, so every read routes to raw. The routed forms never execute here, so their
/// SQL text is pinned directly: all four <c>...SqlFor</c> overloads, for each tier, with unknown coverage and with a
/// coverage built to stitch a successor in (hourly and daily).</para>
/// </summary>
public sealed class FinOpsDatabaseResourcesGoldenLiveTests
{
    internal const string ServerNameA = "darling-finops-dbres-golden-a";
    internal const string ServerNameB = "darling-finops-dbres-golden-b";
    internal static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    internal static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);

    [Fact]
    public Task DatabaseResourceReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var usage = new Dictionary<string, object?>
            {
                ["a"] = await viewer.GetDatabaseResourceUsageAsync(ServerIdA, 24, ct),
                ["b"] = await viewer.GetDatabaseResourceUsageAsync(ServerIdB, 24, ct),
            };
            var top = new Dictionary<string, object?>();
            foreach (var (key, id) in new[] { ("a", ServerIdA), ("b", ServerIdB) })
            {
                var (byTotal, byAvg) = await viewer.GetTopResourceConsumersAsync(id, 24, 3, ct);
                top[key] = new Dictionary<string, object?> { ["byTotal"] = byTotal, ["byAvg"] = byAvg };
            }
            return Serialize(usage, top, SqlSnapshots(
                ViewerDataService.DatabaseResourceUsageSqlFor, ViewerDataService.DatabaseResourceUsageSqlFor,
                ViewerDataService.TopResourceConsumersSqlFor, ViewerDataService.TopResourceConsumersSqlFor));
        });

    internal static readonly DateTime StitchWindowStart = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>A coverage whose hourly successors start on 2026-02-10 and whose daily successors start on 2026-02-20, so
    /// a window starting 2026-01-01 stitches at those boundaries (legacy below, successor at and above).</summary>
    internal static RollupCoverage StitchingCoverage()
    {
        DateTime At(int month, int day) => new(2026, month, day, 0, 0, 0, DateTimeKind.Unspecified);
        var floors = new Dictionary<string, DateTime>(StringComparer.Ordinal)
        {
            [TimescaleSupport.QueryStatsDbHourlyView] = At(1, 1),
            [TimescaleSupport.QueryStatsDbIntervalHourlyView] = At(2, 10),
            [TimescaleSupport.QueryStatsDbDailyView] = At(1, 1),
            [TimescaleSupport.QueryStatsDbIntervalDailyView] = At(2, 20),
            [TimescaleSupport.QueryStatsHourlyView] = At(1, 1),
            [TimescaleSupport.QueryStatsIntervalHourlyView] = At(2, 10),
            [TimescaleSupport.QueryStatsDailyView] = At(1, 1),
            [TimescaleSupport.QueryStatsIntervalDailyView] = At(2, 20),
        };
        return new RollupCoverage(floors, new Dictionary<string, DateTime>(StringComparer.Ordinal), RollupAvailability.All);
    }

    /// <summary>The SQL each overload returns, per tier, for unknown coverage and for the stitching coverage.</summary>
    internal static Dictionary<string, object?> SqlSnapshots(
        Func<RetentionTier, string> usageOne, Func<RetentionTier, RollupCoverage, DateTime, string> usageThree,
        Func<RetentionTier, string> topOne, Func<RetentionTier, RollupCoverage, DateTime, string> topThree)
    {
        var map = new Dictionary<string, object?>();
        var stitching = StitchingCoverage();
        foreach (var tier in new[] { RetentionTier.Raw, RetentionTier.Hourly, RetentionTier.Daily })
        {
            map[$"usage_{tier}_oneArg"] = usageOne(tier);
            map[$"usage_{tier}_unknown"] = usageThree(tier, RollupCoverage.Unknown, StitchWindowStart);
            map[$"usage_{tier}_stitched"] = usageThree(tier, stitching, StitchWindowStart);
            map[$"top_{tier}_oneArg"] = topOne(tier);
            map[$"top_{tier}_unknown"] = topThree(tier, RollupCoverage.Unknown, StitchWindowStart);
            map[$"top_{tier}_stitched"] = topThree(tier, stitching, StitchWindowStart);
        }
        return map;
    }

    internal static async Task RunAsync(Func<string, CancellationToken, Task<string>> read)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps database resources golden test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedAsync(connection, ct);
        }

        AssertGolden(await read(scratch.ConnectionString, ct));
    }

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);

        /* cpu is in microseconds (the read divides by 1000). Alpha: three rows, one with a zero sample interval (dropped). */
        await Query(connection, ct, now.AddHours(-2), "Alpha", 9_000_000, 30, 1000, 100, 50, 60);
        await Query(connection, ct, now.AddHours(-3), "Alpha", 2_500_000, 7, 400, 40, 20, 60);
        await Query(connection, ct, now.AddHours(-4), "Alpha", 99_000_000, 999, 9999, 999, 999, 0);
        /* Bravo and Charlie tie on CPU (5000 ms) and on average CPU (500 ms). */
        await Query(connection, ct, now.AddHours(-2), "Bravo", 5_000_000, 10, 300, 30, 10, 60);
        await Query(connection, ct, now.AddHours(-2), "Charlie", 5_000_000, 10, 200, 20, 5, 60);
        /* Delta: CPU but zero executions, so no average. */
        await Query(connection, ct, now.AddHours(-2), "Delta", 3_000_000, 0, 50, 5, 1, 60);
        /* An unattributed row. */
        await Query(connection, ct, now.AddHours(-2), null, 4_000_000, 8, 70, 7, 3, 60);
        await Query(connection, ct, now.AddHours(-2), "Echo", 1_000_000, 4, 10, 1, 0, 60);
        await Query(connection, ct, now.AddHours(-2), "Foxtrot", 2_000_000, 5, 20, 2, 1, 60);

        /* File I/O: bytes chosen so the megabyte figures round at DECIMAL(19,2) and the shares at DECIMAL(5,2). */
        await FileIo(connection, ct, now.AddHours(-2), "Alpha", "a.mdf", 1_054_000L, 3_333_333L, 120, 30);
        await FileIo(connection, ct, now.AddHours(-3), "Alpha", "a.ldf", 7_777_777L, 1_048_577L, 15, 5);
        await FileIo(connection, ct, now.AddHours(-2), "Bravo", "b.mdf", 524_288L, 1_572_864L, 8, 2);
        await FileIo(connection, ct, now.AddHours(-2), "Golf", "g.mdf", 123_456_789L, 98_765_432L, 77, 33);
    }

    private static Task Query(NpgsqlConnection c, CancellationToken ct, DateTime at, string? db, long cpuUs, long exec,
        long logical, long physical, long writes, int intervalSeconds) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash,
                delta_worker_time, delta_execution_count, delta_logical_reads, delta_physical_reads, delta_logical_writes,
                sample_interval_seconds)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, db, "0xQ" + CollectionIdGenerator.Next(), cpuUs, exec, logical, physical, writes, intervalSeconds);

    private static Task FileIo(NpgsqlConnection c, CancellationToken ct, DateTime at, string db, string file,
        long readBytes, long writeBytes, long stallRead, long stallWrite) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            @"INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name,
                file_type, physical_name, size_mb, delta_reads, delta_writes, delta_read_bytes, delta_write_bytes,
                delta_stall_read_ms, delta_stall_write_ms)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, db, file, "ROWS", "D:\\data\\" + file, 100m, 10L, 10L,
            readBytes, writeBytes, stallRead, stallWrite);

    /// <summary>Serializes the results as indented JSON: public settable properties in declaration order and decimals at
    /// their stored scale.</summary>
    internal static string Serialize(object usage, object top, object sql)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping }))
        {
            writer.WriteStartObject();
            writer.WritePropertyName("databaseResourceUsage");
            WriteValue(writer, usage);
            writer.WritePropertyName("topResourceConsumers");
            WriteValue(writer, top);
            writer.WritePropertyName("sqlText");
            WriteValue(writer, sql);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n") + "\n";
    }

    private static void WriteValue(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null: writer.WriteNullValue(); break;
            case string s: writer.WriteStringValue(s.ReplaceLineEndings("\n")); break;
            case bool b: writer.WriteBooleanValue(b); break;
            case int i: writer.WriteNumberValue(i); break;
            case long l: writer.WriteNumberValue(l); break;
            case decimal m: writer.WriteNumberValue(m); break;
            case System.Collections.IDictionary map:
                writer.WriteStartObject();
                foreach (System.Collections.DictionaryEntry entry in map)
                {
                    writer.WritePropertyName((string)entry.Key);
                    WriteValue(writer, entry.Value);
                }
                writer.WriteEndObject();
                break;
            case System.Collections.IEnumerable list:
                writer.WriteStartArray();
                foreach (var item in list) WriteValue(writer, item);
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStartObject();
                foreach (var p in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.SetMethod is { IsPublic: true })
                             .OrderBy(p => p.MetadataToken))
                {
                    writer.WritePropertyName(p.Name);
                    WriteValue(writer, p.GetValue(value));
                }
                writer.WriteEndObject();
                break;
        }
    }

    private static string GoldenSourcePath([CallerFilePath] string testFile = "") =>
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsDatabaseResources", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
            File.WriteAllText(GoldenSourcePath(), actual);

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsDatabaseResources", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
