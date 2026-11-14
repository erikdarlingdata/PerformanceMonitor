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
        Assert.True(churn.PastRewriteFloors);
        Assert.True(churn.TailOccupied);
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

    [Fact]
    public async Task VisibilityMap_ReadsHighAfterVacuum_AndLowAfterEveryRowIsUpdated()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_vis (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_vis SELECT g, repeat('x', 100) FROM generate_series(1, 60000) AS g;", ct);
        await FlushThenAsync(connection, new[] { "VACUUM collect.zz_vis" }, ct);

        var afterVacuum = Assert.Single(await DarlingStoreTableHealthReader.GetAsync(dataSource, ct), r => r.TableName == "zz_vis");
        Assert.True(afterVacuum.RelPages > 100);
        Assert.NotNull(afterVacuum.PctAllVisible);
        Assert.True(afterVacuum.PctAllVisible >= 90, $"pct all visible after vacuum {afterVacuum.PctAllVisible}");
        Assert.Equal(afterVacuum.RelPages, afterVacuum.RelAllVisible);

        await ExecAsync(connection, "UPDATE collect.zz_vis SET payload = payload || 'y'", ct);
        await FlushThenAsync(connection, new[] { "ANALYZE collect.zz_vis" }, ct);

        var afterUpdate = Assert.Single(await DarlingStoreTableHealthReader.GetAsync(dataSource, ct), r => r.TableName == "zz_vis");
        Assert.NotNull(afterUpdate.PctAllVisible);
        Assert.True(afterUpdate.PctAllVisible < 10, $"pct all visible after updating every row {afterUpdate.PctAllVisible}");

        /* The churn counters count the update, and the window they cover is named. */
        Assert.True(afterUpdate.TuplesUpdated >= 60000, $"updates {afterUpdate.TuplesUpdated}");
        Assert.True(afterUpdate.TuplesInserted >= 60000);
        Assert.NotNull(afterUpdate.HotUpdateShare);
        Assert.NotNull(afterUpdate.StatsSince);
        Assert.Contains(afterUpdate.StatsSinceSource, new[] { DarlingStoreTableHealthReader.StatsSourceReset, DarlingStoreTableHealthReader.StatsSourcePostmasterStart });
        Assert.True(afterUpdate.StatsHours > 0);
        Assert.NotNull(afterUpdate.UpdatesPerDay);
    }

    [Fact]
    public async Task VacuumTrigger_ComesFromTheTablesOwnSettings_AndFromTheServerWhenNoneAreSet()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_trig (id integer) WITH (autovacuum_enabled = false, autovacuum_vacuum_threshold = 10, autovacuum_vacuum_scale_factor = 0);
INSERT INTO collect.zz_trig SELECT g FROM generate_series(1, 100) AS g;
DELETE FROM collect.zz_trig WHERE id <= 50;
CREATE TABLE collect.zz_default (id integer);", ct);
        await FlushThenAsync(connection, Array.Empty<string>(), ct);

        double serverThreshold;
        await using (var setting = new NpgsqlCommand("SELECT current_setting('autovacuum_vacuum_threshold')::float8", connection))
        {
            serverThreshold = (double)(await setting.ExecuteScalarAsync(ct))!;
        }

        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);

        var trig = Assert.Single(rows, r => r.TableName == "zz_trig");
        Assert.Equal(10d, trig.VacuumTriggerRows);
        Assert.Equal(50, trig.DeadRows);
        Assert.Equal(1, trig.RelationsPastVacuumTrigger);
        Assert.Equal(50, trig.LiveRows);
        Assert.Equal(50, trig.TuplesDeleted);
        Assert.NotNull(trig.TurnoverPerDay);
        Assert.Equal(trig.DeletesPerDay!.Value / 50d, trig.TurnoverPerDay!.Value, 9);

        /* Never analyzed counts as zero rows, so the trigger is the server's bare threshold. */
        var plain = Assert.Single(rows, r => r.TableName == "zz_default");
        Assert.Equal(serverThreshold, plain.VacuumTriggerRows);
        Assert.Equal(0, plain.RelationsPastVacuumTrigger);
    }

    private const string BigChurnTable = @"
CREATE TABLE collect.{0} (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.{0} SELECT g, repeat('x', 200) FROM generate_series(1, 450000) AS g;";

    [Fact]
    public async Task TheTailRule_AMostlyEmptyTableWithALiveRowAtTheEndIsACandidate_OneWithAnEmptyTailIsNot()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Both tables lose nine rows in ten. The second also loses its last rows, a few hundred pages' worth, which is under
           the point where VACUUM truncates a file, so after a plain VACUUM the last pages are empty and the file keeps its size. */
        await ExecAsync(connection, string.Format(System.Globalization.CultureInfo.InvariantCulture, BigChurnTable, "zz_tail_live")
            + string.Format(System.Globalization.CultureInfo.InvariantCulture, BigChurnTable, "zz_tail_empty")
            + @"
DELETE FROM collect.zz_tail_live WHERE id % 10 <> 0;
DELETE FROM collect.zz_tail_empty WHERE id % 10 <> 0 OR id > 440000;", ct);
        await FlushThenAsync(connection, new[] { "VACUUM collect.zz_tail_live", "ANALYZE collect.zz_tail_live", "VACUUM collect.zz_tail_empty", "ANALYZE collect.zz_tail_empty" }, ct);

        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);

        var live = Assert.Single(rows, r => r.TableName == "zz_tail_live");
        Assert.True(live.PastRewriteFloors);
        Assert.True(live.TailOccupied);
        Assert.Equal(1, live.TailRelationsChecked);
        Assert.Equal(1, live.TailRelationsOccupied);
        Assert.True(live.RewriteCandidate);

        var empty = Assert.Single(rows, r => r.TableName == "zz_tail_empty");
        Assert.True(empty.PastRewriteFloors, $"share {empty.EstimatedEmptyShare}, empty bytes {empty.EstimatedEmptyBytes}");
        Assert.False(empty.TailOccupied);
        Assert.Equal(1, empty.TailRelationsChecked);
        Assert.Equal(0, empty.TailRelationsOccupied);
        Assert.False(empty.RewriteCandidate);

        /* Candidates first, then the table past the floors whose tail is empty. */
        Assert.Equal("zz_tail_live", rows[0].TableName);

        using var only = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, candidates_only: true, cancellationToken: ct));
        Assert.Equal(new[] { "zz_tail_live" }, only.RootElement.GetProperty("tables").EnumerateArray().Select(t => t.GetProperty("table_name").GetString()).ToArray());
    }

    [Fact]
    public async Task Indexes_AnIndexLargerThanItsHeapIsFlagged_AChunkAndALeafRollUp_AndAMissingLeafIndexShowsInTheCoverage()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_idx (id integer) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_idx SELECT g FROM generate_series(1, 300000) AS g;
CREATE INDEX zz_idx_wide ON collect.zz_idx (id, id, id, id, id, id, id, id, id, id, id, id, id, id, id, id);
CREATE TABLE collect.zz_slim (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_slim SELECT g, repeat('x', 200) FROM generate_series(1, 20000) AS g;
CREATE UNIQUE INDEX zz_slim_pk ON collect.zz_slim (id);
CREATE INDEX zz_slim_small ON collect.zz_slim (id) WHERE id < 100;
CREATE TABLE collect.zz_part (d date NOT NULL, v text) PARTITION BY RANGE (d);
CREATE TABLE collect.zz_part_1 PARTITION OF collect.zz_part FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE collect.zz_part_2 PARTITION OF collect.zz_part FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE collect.zz_part_3 PARTITION OF collect.zz_part FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE INDEX zz_part_v_idx ON ONLY collect.zz_part (v);
CREATE INDEX zz_part_1_v_idx ON collect.zz_part_1 (v);
CREATE INDEX zz_part_2_v_idx ON collect.zz_part_2 (v);
ALTER INDEX collect.zz_part_v_idx ATTACH PARTITION collect.zz_part_1_v_idx;
ALTER INDEX collect.zz_part_v_idx ATTACH PARTITION collect.zz_part_2_v_idx;
CREATE INDEX zz_part_d_idx ON collect.zz_part (d);", ct);
        await FlushThenAsync(connection, new[] { "ANALYZE collect.zz_idx", "ANALYZE collect.zz_slim" }, ct);

        var indexes = await DarlingStoreTableHealthReader.GetIndexesAsync(dataSource, ct);

        var wide = Assert.Single(indexes, i => i.IndexName == "zz_idx_wide");
        Assert.True(wide.LargerThanHeap);
        Assert.Equal(1, wide.LargerThanHeapCount);
        Assert.True(wide.IndexToHeapRatio > 1, $"ratio {wide.IndexToHeapRatio}");
        Assert.True(wide.Valid);
        Assert.False(wide.RarelyUsed, "the statistics window is days younger than the seven the flag needs");

        var slim = Assert.Single(indexes, i => i.IndexName == "zz_slim_pk");
        Assert.False(slim.LargerThanHeap);
        Assert.True(slim.IsUnique);
        Assert.False(slim.RarelyUsed);
        Assert.Equal(0, wide.IndexScans);

        /* Partition leaf indexes roll up under the parent index they are attached to: the two attached ones, and the leaf the
           parent index created itself on the third. */
        var attached = Assert.Single(indexes, i => i.TableName == "zz_part" && i.IndexName == "zz_part_v_idx");
        Assert.Equal(DarlingStoreTableHealthReader.KindPartitioned, attached.Kind);
        Assert.Equal(2, attached.LeafCount);
        var auto = Assert.Single(indexes, i => i.TableName == "zz_part" && i.IndexName == "zz_part_d_idx");
        Assert.Equal(3, auto.LeafCount);
        Assert.DoesNotContain(indexes, i => i.IndexName.StartsWith("zz_part_1", StringComparison.Ordinal));

        var coverage = await DarlingStoreTableHealthReader.GetIndexCoverageAsync(dataSource, ct);
        var missing = Assert.Single(coverage, c => c.IndexName == "zz_part_v_idx");
        Assert.Equal(3, missing.LeafCount);
        Assert.Equal(2, missing.LeavesWithIndex);
        Assert.Equal(1, missing.LeavesMissingIndex);
        Assert.Equal(new[] { "collect.zz_part_3" }, missing.MissingLeaves.ToArray());
        Assert.False(missing.ParentIndexValid);
        var whole = Assert.Single(coverage, c => c.IndexName == "zz_part_d_idx");
        Assert.Equal(3, whole.LeavesWithIndex);
        Assert.Equal(0, whole.LeavesMissingIndex);
        Assert.Empty(whole.MissingLeaves);
        Assert.Equal(0, whole.LeavesInvalidIndex);

        /* The gap sorts first, and the tool carries it. */
        Assert.Equal("zz_part_v_idx", coverage[0].IndexName);
        using var doc = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, section: "indexes", cancellationToken: ct));
        Assert.False(doc.RootElement.TryGetProperty("tables", out _));
        Assert.Contains("larger than their heap", doc.RootElement.GetProperty("index_header").GetString(), StringComparison.Ordinal);
        Assert.Equal("zz_part_v_idx", doc.RootElement.GetProperty("partition_index_coverage")[0].GetProperty("index_name").GetString());
        Assert.Equal("zz_idx_wide", doc.RootElement.GetProperty("indexes")[0].GetProperty("index_name").GetString());

        using var tablesOnly = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, section: "tables", cancellationToken: ct));
        Assert.False(tablesOnly.RootElement.TryGetProperty("indexes", out _));
        Assert.True(tablesOnly.RootElement.TryGetProperty("tables", out _));
        Assert.Contains("Invalid section", await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, section: "bogus", cancellationToken: ct), StringComparison.Ordinal);
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
