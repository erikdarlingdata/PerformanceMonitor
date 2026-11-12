/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7701: a database whose newest index snapshot is more than two days older than the server's newest cycle (a dropped and re-created
/// database's old id, a tempdb row from before the collector left temp tables out) stays in the grid, marked, but is in no total and no
/// recommendation. The rule lives in <see cref="DarlingFinOpsIndexAnalysisReader.LeftOverDatabaseIds"/>, so the web page, get_finops
/// and the Viewer agree. Fixed anchors, never the wall clock.
/// </summary>
/* #1776 own-store: the live fact mints its own scratch database through ScratchPostgres. */
public sealed class FinOpsIndexAnalysisLeftOverTests
{
    private const int Server = 7701001;
    private static readonly DateTime Cycle = new(2026, 11, 4, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingFinOpsIndexAnalysisReader.IndexObjectStatsDto Row(int dbId, TimeSpan age) =>
        new() { DatabaseId = dbId, DatabaseName = "db" + dbId, CollectionTime = Cycle - age };

    [Fact]
    public void ADatabaseTwoDaysBehindStaysIn_AndOneSecondPastTwoDaysIsLeftOver()
    {
        var rows = new[]
        {
            Row(1, TimeSpan.Zero),
            Row(2, TimeSpan.FromDays(1)),
            Row(3, DarlingFinOpsIndexAnalysisReader.LeftOverAfter),
            Row(4, DarlingFinOpsIndexAnalysisReader.LeftOverAfter + TimeSpan.FromSeconds(1)),
            Row(5, TimeSpan.FromDays(60)),
        };
        Assert.Equal(new[] { 4, 5 }, DarlingFinOpsIndexAnalysisReader.LeftOverDatabaseIds(rows).OrderBy(i => i).ToArray());
        Assert.Empty(DarlingFinOpsIndexAnalysisReader.LeftOverDatabaseIds(Array.Empty<DarlingFinOpsIndexAnalysisReader.IndexObjectStatsDto>()));
    }

    [Fact]
    public void AServerThatStoppedCollecting_LeavesNoDatabaseLeftOver_BecauseTheRuleIsRelativeToItsNewestCycle()
    {
        var rows = new[] { Row(1, TimeSpan.Zero), Row(2, TimeSpan.FromHours(20)) };
        Assert.Empty(DarlingFinOpsIndexAnalysisReader.LeftOverDatabaseIds(rows));
    }

    [Fact]
    public async Task TwoIdsOnOneName_OneSixtyDaysOld_AndATempdbRow_AreMarkedAndLeftOutOfTheTotals_OnTheReaderGetFinOpsAndTheViewer()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the left-over snapshot live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, Server, "srv", ct);

        var t = new DateTime(2026, 11, 4, 12, 0, 0, DateTimeKind.Unspecified);
        await Insert(connection, 7, "Shared", t.AddDays(-60), 1, ct);   // the old id of a re-created database
        await Insert(connection, 8, "Shared", t, 2, ct);                // its live twin
        await Insert(connection, 8, "Shared", t, 2, ct, indexId: 2, indexName: "ix_dup"); // a second index on the same key: the analyzer recommends on it
        await Insert(connection, 9, "Yesterday", t.AddDays(-1), 3, ct); // a live database one day behind
        await Insert(connection, 2, "tempdb", t.AddDays(-25), 4, ct);   // from before the collector left temp tables out
        await using (var body = new NpgsqlCommand(
            "UPDATE index_object_stats SET index_type_desc = 'NONCLUSTERED', key_columns = '[a]', reserved_mb = 100, total_rows = 1000, "
            + "is_unique = false, is_primary_key = false, is_disabled = false, user_seeks = 0, user_scans = 0, user_lookups = 0, user_updates = 5", connection))
        {
            await body.ExecuteNonQueryAsync(ct);
        }

        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        var read = await DarlingFinOpsIndexAnalysisReader.GetIndexAnalysisWithSnapshotTimesAsync(ds, Server, 30, cancellationToken: ct);

        Assert.Equal(new[] { 2, 7 }, read.LeftOverDatabaseIds!.OrderBy(i => i).ToArray());
        /* The rows still come back: four rollups, one per id. The overall row counts the two current databases only. */
        Assert.Equal(4, read.Result.DatabaseRollups.Count);
        Assert.Equal(3, read.Result.OverallRollup.IndexCount);
        Assert.Equal(read.Result.DatabaseRollups.Where(r => r.DatabaseId is 8 or 9).Sum(r => r.TotalSizeGb), read.Result.OverallRollup.TotalSizeGb);
        Assert.DoesNotContain(read.Result.Recommendations, r => r.DatabaseName == "tempdb");
        Assert.Equal(read.Result.DatabaseRollups.Where(r => r.DatabaseId is 8 or 9).Sum(r => r.CompressableIndexes), read.Result.OverallRollup.CompressableIndexes);
        Assert.Equal(
            read.Result.OverallRollup.CompressableIndexes,
            read.Result.Recommendations.Count(r => r.ResultKind == IndexCleanupResultKind.Compress));

        var viewerRows = ViewerDataService.ProjectRollups(read.Result, read.SnapshotTimes, read.LeftOverDatabaseIds);
        var marked = viewerRows.Where(r => r.Status.StartsWith("Not collected since ", StringComparison.Ordinal)).Select(r => r.Scope).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "Shared", "tempdb" }, marked);
        Assert.Equal("", viewerRows.Single(r => r.Scope == "Yesterday").Status);
        Assert.Equal("", viewerRows.Single(r => r.Scope == "ALL DATABASES").Status);
        /* The live "Shared" keeps its own Collected time on its recommendations even though a left-over id shares the name. */
        var recs = ViewerDataService.ProjectRecommendations(read.Result, read.SnapshotTimes, read.LeftOverDatabaseIds);
        Assert.Contains(recs, r => r.DatabaseName == "Shared");
        Assert.All(recs.Where(r => r.DatabaseName == "Shared"), r => Assert.Equal(new DateTime(2026, 11, 4, 12, 0, 0, DateTimeKind.Utc), r.CollectionTimeUtc));

        var payload = await DarlingMcpFinOpsTools.GetFinOps(ds, "index_analysis", "srv", 24, 500, cancellationToken: ct);
        using var doc = JsonDocument.Parse(payload);
        var root = doc.RootElement;
        Assert.Equal(3, root.GetProperty("overall").GetProperty("index_count").GetInt32());
        var dbs = root.GetProperty("databases").EnumerateArray().ToList();
        Assert.Equal(4, dbs.Count);
        var leftOver = dbs.Where(IsMarked).Select(d => d.GetProperty("database_name").GetString()).OrderBy(s => s, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "Shared", "tempdb" }, leftOver);
        /* Left-over rows sort after the current ones. */
        Assert.True(dbs.Take(2).All(d => !IsMarked(d)), payload);
        Assert.Equal(read.Result.Recommendations.Count, root.GetProperty("recommendation_count").GetInt32());
    }

    /* GetIndexCleanupInputsAsync feeds the FinOps "uncompressed object" recommendation too, so a left-over database (a dropped one's
       stale snapshot) must not be named there while a current one still is. */
    [Fact]
    public async Task TheUncompressedObjectRecommendation_NamesACurrentDatabase_AndNotALeftOverOne()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the left-over snapshot live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, Server, "srv", ct);

        var t = new DateTime(2026, 11, 4, 12, 0, 0, DateTimeKind.Unspecified);
        await Insert(connection, 7, "OldDb", t.AddDays(-60), 1, ct);
        await Insert(connection, 8, "CurrentDb", t, 2, ct);
        await using (var body = new NpgsqlCommand(
            "UPDATE index_object_stats SET index_type_desc = 'NONCLUSTERED', key_columns = '[a]', reserved_mb = 2048, total_rows = 1000, "
            + "data_compression_desc = 'NONE', is_unique = false, is_primary_key = false, is_disabled = false", connection))
        {
            await body.ExecuteNonQueryAsync(ct);
        }

        await using var ds = NpgsqlDataSource.Create(scratch.ConnectionString);
        var failed = new List<string>();
        var recs = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(
            ds, Server, 1000m, 30, onCheckFailed: (name, _) => failed.Add(name), cancellationToken: ct);

        Assert.DoesNotContain("Compression", failed);
        var rec = Assert.Single(recs, r => r.Finding.Contains("uncompressed object", StringComparison.Ordinal));
        Assert.StartsWith("1 uncompressed object over", rec.Finding, StringComparison.Ordinal);
        Assert.Contains("CurrentDb.", rec.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("OldDb", rec.Detail, StringComparison.Ordinal);
    }

    /* An unmarked database carries left_over as null, so only a true value marks one. */
    private static bool IsMarked(JsonElement d) => d.TryGetProperty("left_over", out var l) && l.ValueKind == JsonValueKind.True;

    private static async Task Insert(NpgsqlConnection c, int dbId, string databaseName, DateTime at, long collectionId, CancellationToken ct, int indexId = 1, string indexName = "ix")
    {
        await using var insert = new NpgsqlCommand(
            "INSERT INTO index_object_stats (collection_id, collection_time, server_id, server_name, database_name, database_id, "
            + "schema_name, object_id, table_name, index_id, index_name, user_seeks, partition_count) "
            + "VALUES ($1, $2, $3, 'srv', $4, $5, 'dbo', 100, 'T', $6, $7, 0, 1)", c);
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = collectionId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = at });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Server });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar, Value = databaseName });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = dbId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = indexId });
        insert.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Varchar, Value = indexName });
        await insert.ExecuteNonQueryAsync(ct);
    }
}
