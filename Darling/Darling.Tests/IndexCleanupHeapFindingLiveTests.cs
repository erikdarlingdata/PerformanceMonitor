/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact seeds its own scratch database. */
/// <summary>
/// #8880, live: heap rows as the collector stores them (index_id 0, type HEAP, no index name) reach Index Analysis through the
/// Storage reader and get_finops index_analysis. A database holding only a heap gets its roll-up with the Heaps count; the note is
/// dated from the collected server start time; a heap under the read floor is not listed.
/// </summary>
public sealed class IndexCleanupHeapFindingLiveTests
{
    private const string ServerName = "darling-finops-heap";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private static string? Cs => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task<(ScratchPostgres Scratch, DateTime Start)> SeedAsync(string cs, CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(cs, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var at = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-1).AddHours(2), DateTimeKind.Unspecified);
        /* The target's local wall clock, 30 days back: past the 14-day caveat, and the date the note must carry. */
        var start = DateTime.SpecifyKind(DateTime.Now.Date.AddDays(-30).AddHours(4), DateTimeKind.Unspecified);
        await DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, engine_edition, product_version, sqlserver_start_time) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, "Enterprise Edition", 3, "16.0.1000.6", start);

        /* heap_only: one big, busy heap with no index, and one heap nobody reads (a staging table). */
        await InsertAsync(connection, at, start, "heap_only", 5, 500, 0, null, "HEAP", 3_000_000, 1536m, 0, 2_500, 400, 90_000, ct);
        await InsertAsync(connection, at, start, "heap_only", 5, 501, 0, null, "HEAP", 9_000_000, 4096m, 0, 0, 0, 50_000, ct);
        /* mixed: a busy heap with one nonclustered index, beside an ordinary clustered table. */
        await InsertAsync(connection, at, start, "mixed", 6, 600, 0, null, "HEAP", 2_000_000, 700m, 0, 1_200, 3_300, 10_000, ct);
        await InsertAsync(connection, at, start, "mixed", 6, 600, 2, "IX_Mixed_Customer", "NONCLUSTERED", 2_000_000, 90m, 3_300, 0, 0, 10_000, ct);
        await InsertAsync(connection, at, start, "mixed", 6, 601, 1, "PK_Orders", "CLUSTERED", 50_000, 40m, 10, 5, 0, 700, ct);
        return (scratch, start);
    }

    private static Task InsertAsync(NpgsqlConnection c, DateTime at, DateTime start, string db, int dbId, int objectId, int indexId,
        string? index, string type, long rows, decimal reservedMb, long seeks, long scans, long lookups, long updates, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id, index_name, index_type_desc, key_columns, is_unique, is_primary_key, reserved_mb, total_rows, user_seeks, user_scans, user_lookups, user_updates, partition_count, data_compression_desc, sqlserver_start_time) VALUES ($1,$2,$3,$4,$5,$6,'dbo',$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,1,'NONE',$21)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName, db, dbId, objectId, "T" + objectId, indexId,
            (object?)index ?? DBNull.Value, type, indexId == 0 ? DBNull.Value : "[Col" + indexId + "]",
            indexId == 1, indexId == 1, reservedMb, rows, seeks, scans, lookups, updates, start);

    [Fact]
    public async Task CollectedHeaps_ReachTheReaderAndTheTool_WithTheirRollupAndNote()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cs), "Set DARLING_TEST_PG to run the live heap finding test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, start) = await SeedAsync(Cs!, ct);
        await using var _ = scratch;
        await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

        var read = (await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(pg, ServerId, 30, cancellationToken: ct)).Result;

        var heaps = read.Recommendations.Where(r => r.ConsolidationRule == IndexCleanupRules.Heap).OrderBy(r => r.DatabaseName, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { ("heap_only", "T500"), ("mixed", "T600") }, heaps.Select(h => (h.DatabaseName, h.TableName)).ToArray());
        var since = " since " + start.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + ". ";
        Assert.StartsWith("Heap with 1.5 GB and 3,000,000 rows, read 2,900 times (2,500 scans, 400 RID lookups) and written 90,000 times" + since,
            heaps[0].AdditionalInfo, StringComparison.Ordinal);
        Assert.StartsWith("Heap with 700 MB and 2,000,000 rows, read 4,500 times (1,200 scans, 3,300 RID lookups) and written 10,000 times" + since,
            heaps[1].AdditionalInfo, StringComparison.Ordinal);
        Assert.EndsWith(" It has 1 nonclustered index. Its lookups reach the heap by row locator and follow any forwarded record.", heaps[1].AdditionalInfo, StringComparison.Ordinal);

        var heapOnly = read.DatabaseRollups.Single(d => d.DatabaseName == "heap_only");
        Assert.Equal(new IndexCleanupRollup { DatabaseName = "heap_only", DatabaseId = 5, Heaps = 1 }, heapOnly);
        var mixed = read.DatabaseRollups.Single(d => d.DatabaseName == "mixed");
        Assert.Equal(1, mixed.Heaps);
        Assert.Equal(2, mixed.IndexCount);   // the nonclustered index and PK_Orders; the heap is not an index here
        Assert.Equal(2, mixed.TablesAnalyzed);
        Assert.Equal(2, read.OverallRollup.Heaps);

        using var doc = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(pg, "index_analysis", ServerName, 24, 50, null, false, cancellationToken: ct));
        var root = doc.RootElement;
        Assert.Equal(2, root.GetProperty("overall").GetProperty("heaps").GetInt32());
        var dbs = root.GetProperty("databases").EnumerateArray().ToDictionary(d => d.GetProperty("database_name").GetString()!);
        Assert.Equal(1, dbs["heap_only"].GetProperty("heaps").GetInt32());
        Assert.Equal(1, dbs["mixed"].GetProperty("heaps").GetInt32());
        var toolHeaps = root.GetProperty("recommendations").EnumerateArray().Where(r => r.GetProperty("consolidation_rule").GetString() == "Heap").ToList();
        Assert.Equal(2, toolHeaps.Count);
        Assert.All(toolHeaps, h =>
        {
            Assert.Equal("REVIEW", h.GetProperty("action").GetString());
            Assert.Equal("HEAP", h.GetProperty("index_name").GetString());
            Assert.Equal("", h.GetProperty("script").GetString());
        });
        Assert.Contains(root.GetProperty("notes").EnumerateArray(), n => n.GetString()!.StartsWith("Heap rows are REVIEW rows", StringComparison.Ordinal));

        /* The database filter reads a heap-only database too: it has a roll-up now. */
        using var filtered = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(pg, "index_analysis", ServerName, 24, 50, "heap_only", false, cancellationToken: ct));
        Assert.Equal(1, filtered.RootElement.GetProperty("databases")[0].GetProperty("heaps").GetInt32());
        Assert.Equal(1, filtered.RootElement.GetProperty("recommendation_count").GetInt32());
    }
}
