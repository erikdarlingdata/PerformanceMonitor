/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store table health read (#8787) against a real store: a plain table emptied by deletes reads as mostly empty and
/// as a rewrite candidate, a dense one does not, a table with no statistics gets a NULL estimate with a reason and not
/// zero, chunks and partitions roll up to one row, and the MCP tool says what its figure is.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to
   CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class DarlingStoreTableHealthLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /* Statistics counters reach the shared stats a moment after the statement that made them, and an ANALYZE that runs
       first is followed by the older modification counts, which reads as stale statistics. Forcing the flush first makes
       the order the one a real store sees. */
    private static async Task FlushThenAsync(NpgsqlConnection connection, string[] maintenance, CancellationToken ct)
    {
        await ExecAsync(connection, "SELECT pg_stat_force_next_flush()", ct);
        await ExecAsync(connection, "SELECT 1", ct);
        foreach (var statement in maintenance)
        {
            await ExecAsync(connection, statement, ct);
        }
    }

    [Fact]
    public async Task AnEmptiedTable_IsARewriteCandidate_ADenseOneIsNot_AndNoStatisticsMeansNullWithAReason()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_churn (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_churn SELECT g, repeat('x', 200) FROM generate_series(1, 450000) AS g;
DELETE FROM collect.zz_churn WHERE id % 10 <> 0;
CREATE TABLE collect.zz_dense (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_dense SELECT g, repeat('x', 200) FROM generate_series(1, 20000) AS g;
CREATE TABLE collect.zz_nostats (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_nostats SELECT g, repeat('x', 200) FROM generate_series(1, 20000) AS g;", ct);
        await FlushThenAsync(connection, new[] { "VACUUM collect.zz_churn", "ANALYZE collect.zz_churn", "VACUUM ANALYZE collect.zz_dense" }, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);

        var churn = Assert.Single(rows, r => r.SchemaName == "collect" && r.TableName == "zz_churn");
        Assert.Equal(DarlingStoreTableHealthReader.KindTable, churn.Kind);
        Assert.NotNull(churn.EstimatedEmptyShare);
        Assert.True(churn.EstimatedEmptyShare > 0.8, $"share {churn.EstimatedEmptyShare}");
        Assert.True(churn.EstimatedEmptyBytes > DarlingStoreTableHealthReader.RewriteCandidateMinEmptyBytes);
        Assert.True(churn.RewriteCandidate);
        Assert.True(churn.AutovacuumDisabled);
        Assert.Equal(churn.HeapBytes + churn.IndexBytes + churn.ToastBytes, churn.TotalBytes);

        var dense = Assert.Single(rows, r => r.TableName == "zz_dense");
        Assert.NotNull(dense.EstimatedEmptyShare);
        Assert.True(dense.EstimatedEmptyShare < 0.1, $"share {dense.EstimatedEmptyShare}");
        Assert.False(dense.RewriteCandidate);

        var nostats = Assert.Single(rows, r => r.TableName == "zz_nostats");
        Assert.Null(nostats.EstimatedEmptyBytes);
        Assert.Null(nostats.EstimatedEmptyShare);
        Assert.Equal(DarlingStoreTableHealthReader.ReasonNeverAnalyzed, nostats.EstimateUnavailableReason);
        Assert.False(nostats.RewriteCandidate);

        /* Order: candidates first. */
        Assert.True(rows[0].RewriteCandidate);
        Assert.Equal("zz_churn", rows[0].TableName);

        /* The tool: the header and the sentence that says the figure is an estimate. */
        using var doc = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, cancellationToken: ct));
        var header = doc.RootElement.GetProperty("header").GetString()!;
        Assert.Contains("store tables", header, StringComparison.Ordinal);
        Assert.Contains("1 rewrite candidates", header, StringComparison.Ordinal);
        Assert.Contains("never analyzed", header, StringComparison.Ordinal);
        var note = doc.RootElement.GetProperty("estimate_note").GetString()!;
        Assert.Contains("ESTIMATES", note, StringComparison.Ordinal);
        Assert.Contains("pgstattuple", note, StringComparison.Ordinal);

        using var only = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, candidates_only: true, cancellationToken: ct));
        var names = only.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()).ToList();
        Assert.Equal(new[] { "zz_churn" }, names);
    }

    [Fact]
    public async Task HypertableChunks_RollUpToOneRow_WithTheChunkCount()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_ht (collection_time timestamp NOT NULL, v integer, payload text);
SELECT create_hypertable('collect.zz_ht', 'collection_time', chunk_time_interval => interval '1 day');
INSERT INTO collect.zz_ht
SELECT timestamp '2026-01-01' + (d || ' days')::interval + (g || ' seconds')::interval, g, repeat('y', 100)
FROM generate_series(0, 2) AS d, generate_series(1, 30000) AS g;
CREATE TABLE collect.zz_part (d date NOT NULL, v text) PARTITION BY RANGE (d);
CREATE TABLE collect.zz_part_1 PARTITION OF collect.zz_part FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE collect.zz_part_2 PARTITION OF collect.zz_part FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE collect.zz_part_3 PARTITION OF collect.zz_part FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
INSERT INTO collect.zz_part SELECT date '2026-01-01' + (g % 85), repeat('p', 100) FROM generate_series(1, 30000) AS g;", ct);
        var chunkNames = await ChunkNamesAsync(connection, ct);
        await FlushThenAsync(connection, new[] { "ANALYZE collect.zz_ht", "ANALYZE collect.zz_part_1", "ANALYZE collect.zz_part_2", "ANALYZE collect.zz_part_3" }, ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);

        var ht = Assert.Single(rows, r => r.TableName == "zz_ht");
        Assert.Equal(DarlingStoreTableHealthReader.KindHypertable, ht.Kind);
        Assert.Equal(3, ht.ChunkCount);
        Assert.Equal(0, ht.LeafCount);
        Assert.Equal(3, ht.EstimateRelations);
        Assert.Equal(3, ht.EstimateRelationsUsable);
        Assert.NotNull(ht.EstimatedEmptyBytes);
        Assert.Null(ht.EstimateUnavailableReason);
        Assert.Equal(3, chunkNames.Count);
        Assert.DoesNotContain(rows, r => chunkNames.Contains(r.TableName));

        var part = Assert.Single(rows, r => r.TableName == "zz_part");
        Assert.Equal(DarlingStoreTableHealthReader.KindPartitioned, part.Kind);
        Assert.Equal(3, part.LeafCount);
        Assert.Equal(0, part.ChunkCount);
        Assert.DoesNotContain(rows, r => r.TableName.StartsWith("zz_part_", StringComparison.Ordinal));
    }

    private static async Task<System.Collections.Generic.HashSet<string>> ChunkNamesAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var names = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = 'zz_ht'", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }
}
