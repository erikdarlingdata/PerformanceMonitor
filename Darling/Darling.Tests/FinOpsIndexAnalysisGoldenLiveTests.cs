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
/// Golden-fixture pin for the FinOps Index Analysis reads (<c>GetIndexCleanupInputsAsync</c>,
/// <c>GetIndexCleanupOptionsAsync</c>, <c>GetIndexAnalysisAsync</c> and the row projections). A deterministic seeded
/// store is read and the results are serialized (public readable properties in declaration order, decimals at their
/// stored scale, times as offsets from the seed anchor with their <c>Kind</c>, an uptime as whole days) and compared
/// byte-for-byte to <c>Fixtures/FinOpsIndexAnalysis/golden.json</c>. Set <c>DARLING_WRITE_GOLDEN=1</c> to regenerate it.
/// </summary>
[Collection("live-postgres")]
public sealed class FinOpsIndexAnalysisGoldenLiveTests
{
    private const string ServerNameA = "darling-finops-ia-golden-a";
    private const string ServerNameB = "darling-finops-ia-golden-b";
    private static readonly int ServerIdA = ServerIdHelper.GetDeterministicHashCode(ServerNameA);
    private static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);

    [Fact]
    public Task IndexAnalysisReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var inputs = await viewer.GetIndexCleanupInputsAsync(ServerIdA, ct);
            var options = await viewer.GetIndexCleanupOptionsAsync(ServerIdA, ct);
            var noProps = await viewer.GetIndexCleanupOptionsAsync(ServerIdB, ct);
            var result = await viewer.GetIndexAnalysisAsync(ServerIdA, ct);
            var empty = await viewer.GetIndexAnalysisAsync(ServerIdB, ct);
            return Serialize(anchor, inputs, options, noProps, result,
                ViewerDataService.ProjectRecommendations(result), ViewerDataService.ProjectRollups(result),
                ViewerDataService.ProjectRollups(empty));
        });

    [Fact]
    public Task IndexAnalysisReads_MatchGoldenFixture_ThroughTheStorageReaderAndRowProjections() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var inputs = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(dataSource, ServerIdA, 30, ct);
            var options = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(dataSource, ServerIdA, 30, cancellationToken: ct);
            var noProps = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(dataSource, ServerIdB, 30, cancellationToken: ct);
            var result = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisAsync(dataSource, ServerIdA, 30, ct);
            var empty = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisAsync(dataSource, ServerIdB, 30, ct);
            return Serialize(anchor, inputs, options, noProps, result,
                ViewerDataService.ProjectRecommendations(result), ViewerDataService.ProjectRollups(result),
                ViewerDataService.ProjectRollups(empty));
        });

    private static async Task RunAsync(Func<string, DateTime, CancellationToken, Task<string>> read)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps index analysis golden test.");

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

    /// <summary>Server A: an older and a newer snapshot of one index (the newest wins; its start time is five days back), one index with two rows at the same time, a clustered key, two exact
    /// duplicates, an unused index and a second database. Server B has no collected data at all.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);

        var old = anchor.AddDays(-3).AddHours(2);
        var recent = anchor.AddDays(-1).AddHours(2);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, product_version, sqlserver_start_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), old, ServerIdA, ServerNameA, "Standard Edition", 2, "12.0.2000.8", anchor.AddDays(-400));
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, product_version, sqlserver_start_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), recent, ServerIdA, ServerNameA, "Standard Edition", 2, "16.0.1000.6", DateTime.Now.AddDays(-5).AddHours(-1));

        await InsertIndexAsync(connection, old, "SalesDb", 1, 100, 1, "PK_Orders", "CLUSTERED", "[OrderId]", null, true, true, 900m, 50000, 10, 5, 0, 700, ct);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 1, "PK_Orders", "CLUSTERED", "[OrderId]", null, true, true, 910.5m, 51000, 12, 6, 1, 800, ct);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 2, "IX_Orders_Cust", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 120.25m, 51000, 400, 20, 0, 800, ct);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 2, "IX_Orders_Cust", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 121m, 51000, 77, 9, 3, 801, ct);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 3, "IX_Orders_Cust_Dup", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 118m, 51000, 0, 0, 0, 800, ct);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 4, "IX_Orders_Unused", "NONCLUSTERED", "[ShipDate]", null, false, false, 64m, 51000, 0, 0, 0, 800, ct);
        await InsertIndexAsync(connection, recent, "OpsDb", 2, 200, 1, "PK_Jobs", "CLUSTERED", "[JobId]", null, true, true, 5m, 900, 3, 1, 0, 40, ct);
        await InsertIndexAsync(connection, recent, "OpsDb", 2, 200, 2, "IX_Jobs_State", "NONCLUSTERED", "[State]", "[Owner]", false, false, 2.5m, 900, 0, 0, 0, 40, ct);
    }

    private static Task InsertIndexAsync(NpgsqlConnection c, DateTime at, string db, int dbId, int objectId, int indexId,
        string index, string type, string keys, string? included, bool unique, bool primary, decimal reservedMb,
        long rows, long seeks, long scans, long lookups, long updates, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, key_columns, included_columns, is_unique, is_primary_key, reserved_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, partition_count, sqlserver_start_time) VALUES ($1,$2,$3,$4,$5,$6,'dbo',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,1,$24)",
            CollectionIdGenerator.Next(), at, ServerIdA, ServerNameA, db, dbId, objectId, "T" + objectId, indexId, index, type, keys,
            (object?)included ?? DBNull.Value, unique, primary, reservedMb, rows, seeks, scans, lookups, updates, indexId * 3L, indexId * 11L,
            at.Date.AddDays(-400));

    private static Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM index_object_stats WHERE server_id IN ({ServerIdA},{ServerIdB}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({ServerIdA},{ServerIdB}); " +
            $"DELETE FROM servers WHERE server_id IN ({ServerIdA},{ServerIdB})");

    /// <summary>Serializes the results as indented JSON, one named section per argument.</summary>
    private static string Serialize(DateTime anchor, params object[] sections)
    {
        var names = new[] { "inputs", "options", "optionsNoProperties", "analysis", "recommendationRows", "rollupRows", "rollupRowsNoData" };
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            for (var i = 0; i < sections.Length; i++)
            {
                writer.WritePropertyName(names[i]);
                WriteValue(writer, sections[i], anchor);
            }
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
            case Enum e: writer.WriteStringValue(e.ToString()); break;
            case double dd: writer.WriteNumberValue(Math.Floor(dd)); break;
            case DateTime t:
                var offset = t - anchor;
                writer.WriteStringValue($"anchor{(offset < TimeSpan.Zero ? "-" : "+")}{Math.Abs(offset.TotalDays).ToString("0.####", System.Globalization.CultureInfo.InvariantCulture)}d[{t.Kind}]");
                break;
            case System.Collections.IEnumerable list:
                writer.WriteStartArray();
                foreach (var item in list) WriteValue(writer, item, anchor);
                writer.WriteEndArray();
                break;
            default:
                writer.WriteStartObject();
                foreach (var p in value.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.GetMethod is { IsPublic: true } && p.GetIndexParameters().Length == 0)
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
        Path.Combine(Path.GetDirectoryName(testFile)!, "Fixtures", "FinOpsIndexAnalysis", "golden.json");

    private static void AssertGolden(string actual)
    {
        if (Environment.GetEnvironmentVariable("DARLING_WRITE_GOLDEN") == "1")
        {
            File.WriteAllText(GoldenSourcePath(), actual);
            Assert.Skip("Regenerated the golden fixture source copy; run again without DARLING_WRITE_GOLDEN to compare.");
        }

        var golden = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "FinOpsIndexAnalysis", "golden.json"))
            .ReplaceLineEndings("\n");
        Assert.Equal(golden, actual);
    }
}
