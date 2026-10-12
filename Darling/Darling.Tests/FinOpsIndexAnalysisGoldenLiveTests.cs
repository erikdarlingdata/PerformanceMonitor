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
    private const string ServerNameC = "darling-finops-ia-golden-c";
    private static readonly int ServerIdB = ServerIdHelper.GetDeterministicHashCode(ServerNameB);
    private static readonly int ServerIdC = ServerIdHelper.GetDeterministicHashCode(ServerNameC);

    [Fact]
    public Task IndexAnalysisReads_MatchGoldenFixture_ThroughTheViewer() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var viewer = new ViewerDataService(connectionString);
            var inputs = await viewer.GetIndexCleanupInputsAsync(ServerIdA, ct);
            var options = await viewer.GetIndexCleanupOptionsAsync(ServerIdA, ct);
            var noProps = await viewer.GetIndexCleanupOptionsAsync(ServerIdB, ct);
            var timed = await viewer.GetIndexAnalysisWithSnapshotTimesAsync(ServerIdA, ct);
            var result = timed.Result;
            var empty = await viewer.GetIndexAnalysisAsync(ServerIdB, ct);
            var longInputs = await viewer.GetIndexCleanupInputsAsync(ServerIdC, ct);
            var longOptions = await viewer.GetIndexCleanupOptionsAsync(ServerIdC, ct);
            var longTimed = await viewer.GetIndexAnalysisWithSnapshotTimesAsync(ServerIdC, ct);
            var longResult = longTimed.Result;
            return Serialize(anchor,
                new object[] { inputs, options, noProps, result,
                    ViewerDataService.ProjectRecommendations(result, timed.SnapshotTimes), ViewerDataService.ProjectRollups(result, timed.SnapshotTimes),
                    ViewerDataService.ProjectRollups(empty) },
                new object[] { longInputs, longOptions, longResult,
                    ViewerDataService.ProjectRecommendations(longResult, longTimed.SnapshotTimes), ViewerDataService.ProjectRollups(longResult, longTimed.SnapshotTimes) });
        });

    [Fact]
    public Task IndexAnalysisReads_MatchGoldenFixture_ThroughTheStorageReaderAndRowProjections() =>
        RunAsync(async (connectionString, anchor, ct) =>
        {
            await using var dataSource = NpgsqlDataSource.Create(connectionString);
            var inputs = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(dataSource, ServerIdA, 30, ct);
            var options = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(dataSource, ServerIdA, 30, cancellationToken: ct);
            var noProps = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(dataSource, ServerIdB, 30, cancellationToken: ct);
            var timed = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(dataSource, ServerIdA, 30, ct);
            var result = timed.Result;
            var empty = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisAsync(dataSource, ServerIdB, 30, ct);
            var longInputs = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupInputsAsync(dataSource, ServerIdC, 30, ct);
            var longOptions = await DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync(dataSource, ServerIdC, 30, cancellationToken: ct);
            var longTimed = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(dataSource, ServerIdC, 30, ct);
            var longResult = longTimed.Result;
            return Serialize(anchor,
                new object[] { inputs, options, noProps, result,
                    ViewerDataService.ProjectRecommendations(result, timed.SnapshotTimes), ViewerDataService.ProjectRollups(result, timed.SnapshotTimes),
                    ViewerDataService.ProjectRollups(empty) },
                new object[] { longInputs, longOptions, longResult,
                    ViewerDataService.ProjectRecommendations(longResult, longTimed.SnapshotTimes), ViewerDataService.ProjectRollups(longResult, longTimed.SnapshotTimes) });
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
    /// duplicates, an unused index and a second database. Server B has no collected data at all. Server C is analyzed with a 400-day uptime and the plain index set
    /// (no same-time tie row), so the long-uptime recommendations stay pinned next to server A's short-uptime output.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime anchor, CancellationToken ct)
    {
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdA, ServerNameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdB, ServerNameB, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerIdC, ServerNameC, ct);

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
        await SeedLongUptimeServerAsync(connection, anchor, ct);
    }

    private static async Task SeedLongUptimeServerAsync(NpgsqlConnection connection, DateTime anchor, CancellationToken ct)
    {
        var old = anchor.AddDays(-3).AddHours(2);
        var recent = anchor.AddDays(-1).AddHours(2);
        /* sqlserver_start_time is the target's LOCAL wall clock and the reader compares it with the host's local now
           (DarlingFinOpsIndexAnalysisReader.GetIndexCleanupOptionsAsync), so seed it on the local calendar day. Seeding it from
           the UTC anchor made the uptime 399 between 00:00 UTC and local midnight on a zone behind UTC. */
        var longUptimeStart = DateTime.SpecifyKind(DateTime.Now.Date.AddDays(-400), DateTimeKind.Unspecified);
        const string props = "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, product_version, sqlserver_start_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)";
        await DarlingMcpTestData.ExecAsync(connection, ct, props,
            CollectionIdGenerator.Next(), old, ServerIdC, ServerNameC, "Standard Edition", 2, "12.0.2000.8", longUptimeStart);
        await DarlingMcpTestData.ExecAsync(connection, ct, props,
            CollectionIdGenerator.Next(), recent, ServerIdC, ServerNameC, "Standard Edition", 2, "16.0.1000.6", longUptimeStart);

        await InsertIndexAsync(connection, old, "SalesDb", 1, 100, 1, "PK_Orders", "CLUSTERED", "[OrderId]", null, true, true, 900m, 50000, 10, 5, 0, 700, ct, ServerIdC, ServerNameC);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 1, "PK_Orders", "CLUSTERED", "[OrderId]", null, true, true, 910.5m, 51000, 12, 6, 1, 800, ct, ServerIdC, ServerNameC);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 2, "IX_Orders_Cust", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 120.25m, 51000, 400, 20, 0, 800, ct, ServerIdC, ServerNameC);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 3, "IX_Orders_Cust_Dup", "NONCLUSTERED", "[CustomerId]", "[Total]", false, false, 118m, 51000, 0, 0, 0, 800, ct, ServerIdC, ServerNameC);
        await InsertIndexAsync(connection, recent, "SalesDb", 1, 100, 4, "IX_Orders_Unused", "NONCLUSTERED", "[ShipDate]", null, false, false, 64m, 51000, 0, 0, 0, 800, ct, ServerIdC, ServerNameC);
        await InsertIndexAsync(connection, recent, "OpsDb", 2, 200, 1, "PK_Jobs", "CLUSTERED", "[JobId]", null, true, true, 5m, 900, 3, 1, 0, 40, ct, ServerIdC, ServerNameC);
        await InsertIndexAsync(connection, recent, "OpsDb", 2, 200, 2, "IX_Jobs_State", "NONCLUSTERED", "[State]", "[Owner]", false, false, 2.5m, 900, 0, 0, 0, 40, ct, ServerIdC, ServerNameC);
    }

    private static Task InsertIndexAsync(NpgsqlConnection c, DateTime at, string db, int dbId, int objectId, int indexId,
        string index, string type, string keys, string? included, bool unique, bool primary, decimal reservedMb,
        long rows, long seeks, long scans, long lookups, long updates, CancellationToken ct,
        int? serverId = null, string? serverName = null) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, key_columns, included_columns, is_unique, is_primary_key, reserved_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_count, row_lock_wait_in_ms, partition_count, sqlserver_start_time) VALUES ($1,$2,$3,$4,$5,$6,'dbo',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,1,$24)",
            CollectionIdGenerator.Next(), at, serverId ?? ServerIdA, serverName ?? ServerNameA, db, dbId, objectId, "T" + objectId, indexId, index, type, keys,
            (object?)included ?? DBNull.Value, unique, primary, reservedMb, rows, seeks, scans, lookups, updates, indexId * 3L, indexId * 11L,
            at.Date.AddDays(-400));

    private static Task CleanupAsync(NpgsqlConnection connection, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            $"DELETE FROM index_object_stats WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC}); " +
            $"DELETE FROM server_properties WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC}); " +
            $"DELETE FROM servers WHERE server_id IN ({ServerIdA},{ServerIdB},{ServerIdC})");

    /// <summary>Serializes the results as indented JSON: the short-uptime server's sections at the top level, then the
    /// long-uptime server's sections under <c>longUptime</c>.</summary>
    private static string Serialize(DateTime anchor, object[] shortUptime, object[] longUptime)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            WriteSections(writer, anchor, new[] { "inputs", "options", "optionsNoProperties", "analysis", "recommendationRows", "rollupRows", "rollupRowsNoData" }, shortUptime);
            writer.WritePropertyName("longUptime");
            writer.WriteStartObject();
            WriteSections(writer, anchor, new[] { "inputs", "options", "analysis", "recommendationRows", "rollupRows" }, longUptime);
            writer.WriteEndObject();
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray()).ReplaceLineEndings("\n") + "\n";
    }

    private static void WriteSections(Utf8JsonWriter writer, DateTime anchor, string[] names, object[] sections)
    {
        for (var i = 0; i < sections.Length; i++)
        {
            writer.WritePropertyName(names[i]);
            WriteValue(writer, sections[i], anchor);
        }
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
                             /* #5082: the Collected text is the display-zone rendering of CollectionTimeUtc, which is pinned anchor-relative. */
                             .Where(p => p.Name != "CollectionTime" || p.PropertyType != typeof(string))
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
