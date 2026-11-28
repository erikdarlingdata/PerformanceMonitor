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
        Assert.Contains("1 rewrite candidate,", header, StringComparison.Ordinal);
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

        /* Per-day rates need a counters' start the store knows (#8823): a reset of this test database's own statistics gives one. */
        await ExecAsync(connection, "SELECT pg_stat_reset()", ct);
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_vis (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_vis SELECT g, repeat('x', 100) FROM generate_series(1, 60000) AS g;", ct);
        await FlushThenAsync(connection, new[] { "VACUUM collect.zz_vis" }, ct);

        var afterVacuum = Assert.Single(await DarlingStoreTableHealthReader.GetAsync(dataSource, ct), r => r.TableName == "zz_vis");
        Assert.True(afterVacuum.RelPages > 100);
        Assert.NotNull(afterVacuum.PctAllVisible);
        Assert.True(afterVacuum.PctAllVisible >= 90, $"pct all visible after vacuum {afterVacuum.PctAllVisible}");
        /* A plain VACUUM passes over a page whose buffer another process holds pinned at that moment, and leaves it unmarked, so on a busy
           runner one page can stay off the map (1034 of 1035 seen in CI). The reader still has to report the map's own count. */
        Assert.InRange(afterVacuum.RelAllVisible, afterVacuum.RelPages - 2, afterVacuum.RelPages);

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
        Assert.Equal(DarlingStoreTableHealthReader.StatsSourceReset, afterUpdate.StatsSinceSource);
        Assert.True(afterUpdate.StatsHours > 0);
        Assert.NotNull(afterUpdate.UpdatesPerDay);
    }

    /* #8823: PostgreSQL keeps table statistics across a restart, so with no statistics reset the counters are older than the postmaster start by an unknown
       amount. Dividing them by the time since that start gave per-day rates too high by the ratio of the two ages. A fresh test database has stats_reset NULL,
       which is the field's "never reset" case. */
    [Fact]
    public async Task WhenTheStatisticsWereNeverReset_TheRatesAreNull_TheCountersStay_AndTheReplySaysWhy()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8823 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_rates (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_rates SELECT g, repeat('x', 50) FROM generate_series(1, 2000) AS g;
DELETE FROM collect.zz_rates WHERE id <= 1000;", ct);
        await FlushThenAsync(connection, new[] { "ANALYZE collect.zz_rates" }, ct);

        var row = Assert.Single(await DarlingStoreTableHealthReader.GetAsync(dataSource, ct), r => r.TableName == "zz_rates");
        Assert.Equal(DarlingStoreTableHealthReader.StatsSourcePostmasterStart, row.StatsSinceSource);
        Assert.False(row.RatesKnown);
        Assert.Null(row.InsertsPerDay);
        Assert.Null(row.UpdatesPerDay);
        Assert.Null(row.DeletesPerDay);
        Assert.Null(row.TurnoverPerDay);
        Assert.Equal(2000, row.TuplesInserted);
        Assert.Equal(1000, row.TuplesDeleted);

        using var doc = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, section: "tables", cancellationToken: ct));
        var root = doc.RootElement;
        Assert.False(root.GetProperty("rates_known").GetBoolean());
        var note = root.GetProperty("rates_note").GetString()!;
        Assert.Contains("never reset", note, StringComparison.Ordinal);
        Assert.Contains("older than the last start", note, StringComparison.Ordinal);
        var tableJson = root.GetProperty("tables").EnumerateArray().Single(t => t.GetProperty("table_name").GetString() == "zz_rates");
        Assert.Equal("postmaster_start", tableJson.GetProperty("stats_since_source").GetString());
        Assert.Equal(JsonValueKind.Null, tableJson.GetProperty("inserts_per_day").ValueKind);
        Assert.Equal(JsonValueKind.Null, tableJson.GetProperty("deletes_per_day").ValueKind);
        Assert.Equal(JsonValueKind.Null, tableJson.GetProperty("turnover_per_day").ValueKind);
        Assert.Equal(2000, tableJson.GetProperty("tuples_inserted").GetInt64());
        Assert.Equal(1000, tableJson.GetProperty("tuples_deleted").GetInt64());
    }

    [Fact]
    public async Task AfterAStatisticsReset_TheRatesAreWorkedOut_AndTheReplyHasNoNote()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8823 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* pg_stat_reset() resets only the database the connection is on: this test's own database. */
        await ExecAsync(connection, "SELECT pg_stat_reset()", ct);
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_rates (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_rates SELECT g, repeat('x', 50) FROM generate_series(1, 2000) AS g;
DELETE FROM collect.zz_rates WHERE id <= 1000;", ct);
        await FlushThenAsync(connection, new[] { "ANALYZE collect.zz_rates" }, ct);

        var row = Assert.Single(await DarlingStoreTableHealthReader.GetAsync(dataSource, ct), r => r.TableName == "zz_rates");
        Assert.Equal(DarlingStoreTableHealthReader.StatsSourceReset, row.StatsSinceSource);
        Assert.True(row.RatesKnown);
        Assert.NotNull(row.InsertsPerDay);
        Assert.NotNull(row.DeletesPerDay);
        Assert.NotNull(row.TurnoverPerDay);

        using var doc = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, section: "tables", cancellationToken: ct));
        var root = doc.RootElement;
        Assert.True(root.GetProperty("rates_known").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("rates_note").ValueKind);
        var tableJson = root.GetProperty("tables").EnumerateArray().Single(t => t.GetProperty("table_name").GetString() == "zz_rates");
        Assert.Equal("stats_reset", tableJson.GetProperty("stats_since_source").GetString());
        Assert.Equal(JsonValueKind.Number, tableJson.GetProperty("inserts_per_day").ValueKind);
        Assert.Equal(JsonValueKind.Number, tableJson.GetProperty("turnover_per_day").ValueKind);
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

        /* Per-day rates need a counters' start the store knows (#8823): a reset of this test database's own statistics gives one. */
        await ExecAsync(connection, "SELECT pg_stat_reset()", ct);
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

    [Fact]
    public async Task TheTailRule_ALiveRowInTheWindowIsACandidate_AnEmptyRunVacuumCanCutIsNot_AndTruncateOffIsACandidate()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* zz_tail_live and zz_tail_empty lose nine rows in ten. zz_tail_empty also loses its last rows, a few hundred pages' worth,
           under the 1000 pages (a sixteenth of the file) VACUUM needs before it truncates, so after a plain VACUUM the last pages
           are empty and the file keeps its size: roughly 90 MB sits free inside pages and VACUUM gives nothing back, so it IS a
           candidate. zz_tail_free loses the top 60 percent of its ids and is analyzed without a VACUUM, so the file still holds
           those pages and no live row sits in the window at the end: VACUUM can cut that run. zz_tail_off is zz_tail_free with
           vacuum_truncate off, which stops VACUUM cutting it. */
        var fmt = System.Globalization.CultureInfo.InvariantCulture;
        const string bigTable = @"
CREATE TABLE collect.{0} (id integer, payload text) WITH (autovacuum_enabled = false{1});
INSERT INTO collect.{0} SELECT g, repeat('x', 200) FROM generate_series(1, {2}) AS g;";
        await ExecAsync(connection, string.Format(fmt, bigTable, "zz_tail_live", "", 450000)
            + string.Format(fmt, bigTable, "zz_tail_empty", "", 450000)
            + string.Format(fmt, bigTable, "zz_tail_free", "", 700000)
            + string.Format(fmt, bigTable, "zz_tail_off", ", vacuum_truncate = off", 700000)
            + @"
DELETE FROM collect.zz_tail_live WHERE id % 10 <> 0;
DELETE FROM collect.zz_tail_empty WHERE id % 10 <> 0 OR id > 440000;
DELETE FROM collect.zz_tail_free WHERE id > 280000;
DELETE FROM collect.zz_tail_off WHERE id > 280000;", ct);
        await FlushThenAsync(connection, new[]
        {
            "VACUUM collect.zz_tail_live", "ANALYZE collect.zz_tail_live", "VACUUM collect.zz_tail_empty", "ANALYZE collect.zz_tail_empty",
            "ANALYZE collect.zz_tail_free", "ANALYZE collect.zz_tail_off",
        }, ct);

        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);

        var live = Assert.Single(rows, r => r.TableName == "zz_tail_live");
        Assert.True(live.PastRewriteFloors);
        Assert.True(live.TailOccupied);
        Assert.Equal(1, live.TailRelationsChecked);
        Assert.Equal(1, live.TailRelationsOccupied);
        Assert.True(live.RewriteCandidate);

        var empty = Assert.Single(rows, r => r.TableName == "zz_tail_empty");
        Assert.True(empty.PastRewriteFloors, $"share {empty.EstimatedEmptyShare}, empty bytes {empty.EstimatedEmptyBytes}");
        Assert.True(empty.TailOccupied);
        Assert.Equal(1, empty.TailRelationsOccupied);
        Assert.True(empty.RewriteCandidate);

        var free = Assert.Single(rows, r => r.TableName == "zz_tail_free");
        Assert.True(free.PastRewriteFloors, $"share {free.EstimatedEmptyShare}, empty bytes {free.EstimatedEmptyBytes}");
        Assert.False(free.TailOccupied);
        Assert.Equal(1, free.TailRelationsChecked);
        Assert.Equal(0, free.TailRelationsOccupied);
        Assert.False(free.RewriteCandidate);

        var off = Assert.Single(rows, r => r.TableName == "zz_tail_off");
        Assert.True(off.PastRewriteFloors);
        Assert.True(off.TailOccupied);
        Assert.True(off.RewriteCandidate);

        /* Candidates first, then the table past the floors whose empty run VACUUM can cut. */
        Assert.Equal(new[] { "zz_tail_empty", "zz_tail_live", "zz_tail_off" }, rows.Where(r => r.RewriteCandidate && r.TableName.StartsWith("zz_tail", StringComparison.Ordinal)).Select(r => r.TableName).OrderBy(n => n, StringComparer.Ordinal).ToArray());
        Assert.True(rows.ToList().FindIndex(r => r.TableName == "zz_tail_free") > rows.ToList().FindLastIndex(r => r.RewriteCandidate));

        using var only = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, candidates_only: true, cancellationToken: ct));
        Assert.DoesNotContain(only.RootElement.GetProperty("tables").EnumerateArray(), t => t.GetProperty("table_name").GetString() == "zz_tail_free");
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

    /// <summary>
    /// #8897: the estimate runs in the same statement as the size read, and both sizes are pg_relation_size, so a row whose copies were
    /// all estimated reads a basis equal to its size, for a plain index and for a rolled-up one. The basis is what the grid names when
    /// only some copies were estimated.
    /// </summary>
    [Fact]
    public async Task AnEstimatedIndexRow_ReadsABasisEqualToItsSize_ForOneIndexAndForARolledUpOne()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Heaps past the 1 MB estimate floor. The index is built on the full table, then most rows go, so it holds far more pages than
           the statistics say it needs and the estimate is a number and not a skip. */
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_est (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_est SELECT g, repeat('x', 40) FROM generate_series(1, 100000) AS g;
CREATE INDEX zz_est_id ON collect.zz_est (id);
DELETE FROM collect.zz_est WHERE id % 10 <> 0;
CREATE TABLE collect.zz_est_part (d date NOT NULL, id integer, payload text) PARTITION BY RANGE (d);
CREATE TABLE collect.zz_est_part_1 PARTITION OF collect.zz_est_part FOR VALUES FROM ('2026-01-01') TO ('2026-02-01') WITH (autovacuum_enabled = false);
CREATE TABLE collect.zz_est_part_2 PARTITION OF collect.zz_est_part FOR VALUES FROM ('2026-02-01') TO ('2026-03-01') WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_est_part SELECT CASE WHEN g % 20 < 10 THEN DATE '2026-01-10' ELSE DATE '2026-02-10' END, g, repeat('x', 40) FROM generate_series(1, 100000) AS g;
CREATE INDEX zz_est_part_id ON collect.zz_est_part (id);
DELETE FROM collect.zz_est_part WHERE id % 10 <> 0;", ct);
        await FlushThenAsync(connection, new[] { "ANALYZE collect.zz_est", "ANALYZE collect.zz_est_part" }, ct);

        var indexes = await DarlingStoreTableHealthReader.GetIndexesAsync(dataSource, ct);

        var single = Assert.Single(indexes, i => i.TableName == "zz_est" && i.IndexName == "zz_est_id");
        Assert.Equal(1, single.EstimateIndexesUsable);
        Assert.NotNull(single.EstimatedReclaimableBytes);
        Assert.NotNull(single.EstimatedBasisBytes);
        Assert.Equal(single.Bytes, single.EstimatedBasisBytes);

        var rolled = Assert.Single(indexes, i => i.TableName == "zz_est_part" && i.IndexName == "zz_est_part_id");
        Assert.Equal(2, rolled.IndexCount);
        Assert.Equal(2, rolled.EstimateIndexesUsable);
        Assert.NotNull(rolled.EstimatedReclaimableBytes);
        Assert.NotNull(rolled.EstimatedBasisBytes);
        Assert.Equal(rolled.Bytes, rolled.EstimatedBasisBytes);
    }

    /// <summary>
    /// The MCP tool holds its reply to the response budget: a store with many tables asked for the most rows is cut to whole
    /// rows that fit, says so, and next_offset reads on from the first row left out. The web route asks for no budget and
    /// gets every row it asked for.
    /// </summary>
    [Fact]
    public async Task AReplyOverTheByteBudget_IsCutToWholeRows_AndNextOffsetReadsOn()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8787 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var all = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);
        Assert.True(all.Count > 40, "the migrated store should hold enough tables to be cut: " + all.Count);

        using var first = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, top: 1000, section: "tables", cancellationToken: ct));
        var firstRoot = first.RootElement;
        var returned = firstRoot.GetProperty("tables_returned").GetInt32();
        Assert.True(returned > 0 && returned < all.Count, "the first page holds some rows and not all: " + returned);
        Assert.True(firstRoot.GetProperty("budget_truncated").GetBoolean());
        Assert.True(firstRoot.GetProperty("has_more").GetBoolean());
        Assert.Contains("32,768-byte response budget", firstRoot.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(returned, firstRoot.GetProperty("next_offset").GetInt32());
        Assert.Equal(all[0].TableName, firstRoot.GetProperty("tables")[0].GetProperty("table_name").GetString());

        using var second = JsonDocument.Parse(await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, top: 1000, section: "tables", offset: returned, cancellationToken: ct));
        Assert.Equal(all[returned].TableName, second.RootElement.GetProperty("tables")[0].GetProperty("table_name").GetString());
        Assert.Equal(returned, second.RootElement.GetProperty("offset").GetInt32());

        var uncapped = await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, 1000, false, "tables", 0, null, ct);
        using var whole = JsonDocument.Parse(uncapped);
        Assert.Equal(all.Count, whole.RootElement.GetProperty("tables_returned").GetInt32());
        Assert.False(whole.RootElement.GetProperty("budget_truncated").GetBoolean());
        Assert.False(whole.RootElement.GetProperty("has_more").GetBoolean());

        Assert.Contains("Invalid offset", await DarlingMcpStoreTableHealthTools.GetStoreTableHealth(dataSource, offset: -1, cancellationToken: ct), StringComparison.Ordinal);
    }

    [Fact]
    public async Task ManySmallChunks_AndAFewLargeOnes_ReadTheSameAsTheCollectorAndTheSizeFunctions()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #8806 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        /* 200 one-row chunks under the 1 MB floor (one compressed), two chunks over it (one emptied by deletes), a plain
           table over the floor that is emptied the same way, and a small plain table. */
        await ExecAsync(connection, @"
CREATE TABLE collect.zz_many (collection_time timestamp NOT NULL, v integer, payload text);
SELECT create_hypertable('collect.zz_many', 'collection_time', chunk_time_interval => interval '1 day');
ALTER TABLE collect.zz_many SET (timescaledb.compress);
INSERT INTO collect.zz_many SELECT timestamp '2010-01-01' + (d || ' days')::interval, d, 'x' FROM generate_series(0, 199) AS d;
INSERT INTO collect.zz_many SELECT timestamp '2030-01-01' + (g || ' seconds')::interval, g, repeat('y', 100) FROM generate_series(1, 30000) AS g;
INSERT INTO collect.zz_many SELECT timestamp '2031-01-01' + (g || ' seconds')::interval, g, repeat('y', 100) FROM generate_series(1, 30000) AS g;
DELETE FROM collect.zz_many WHERE collection_time >= timestamp '2031-01-01' AND v % 10 <> 0;
SELECT compress_chunk(c) FROM show_chunks('collect.zz_many', older_than => timestamp '2010-01-02') AS c;
CREATE TABLE collect.zz_plain_big (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_plain_big SELECT g, repeat('x', 200) FROM generate_series(1, 45000) AS g;
DELETE FROM collect.zz_plain_big WHERE id % 10 <> 0;
CREATE TABLE collect.zz_plain_small (id integer, payload text) WITH (autovacuum_enabled = false);
INSERT INTO collect.zz_plain_small SELECT g, 'x' FROM generate_series(1, 10) AS g;", ct);
        var chunkNames = await ChunkNamesAsync(connection, "zz_many", ct);
        var maintenance = new System.Collections.Generic.List<string> { "ANALYZE collect.zz_plain_big", "ANALYZE collect.zz_plain_small" };
        maintenance.AddRange(chunkNames.Select(n => "ANALYZE _timescaledb_internal." + n));
        await FlushThenAsync(connection, maintenance.ToArray(), ct);

        /* What the collector reports for the same database, with no help from the read. */
        var collector = new System.Collections.Generic.Dictionary<string, (long Empty, bool Unavailable)>(StringComparer.Ordinal);
        await using (var command = new NpgsqlCommand(PerformanceMonitor.Collectors.PgTableBloatStatsCollector.QueryText, connection))
        await using (var collected = await command.ExecuteReaderAsync(ct))
        {
            while (await collected.ReadAsync(ct))
            {
                collector[collected.GetString(0) + "." + collected.GetString(1)] =
                    (collected.GetInt64(collected.GetOrdinal("bloat_bytes_estimate")), collected.GetBoolean(collected.GetOrdinal("estimate_unavailable")));
            }
        }

        var estimatedChunks = chunkNames.Where(n => collector.TryGetValue("_timescaledb_internal." + n, out var e) && !e.Unavailable).ToList();
        Assert.Equal(2, estimatedChunks.Count);
        Assert.True(collector["collect.zz_plain_big"].Empty > 0);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);

        var many = Assert.Single(rows, r => r.TableName == "zz_many");
        Assert.Equal(DarlingStoreTableHealthReader.KindHypertable, many.Kind);
        Assert.Equal(202, many.ChunkCount);
        Assert.Equal(202, many.EstimateRelations);
        Assert.Equal(2, many.EstimateRelationsUsable);
        Assert.Equal(estimatedChunks.Sum(n => collector["_timescaledb_internal." + n].Empty), many.EstimatedEmptyBytes);
        Assert.DoesNotContain(rows, r => chunkNames.Contains(r.TableName));

        var big = Assert.Single(rows, r => r.TableName == "zz_plain_big");
        Assert.Equal(1, big.EstimateRelationsUsable);
        Assert.Equal(collector["collect.zz_plain_big"].Empty, big.EstimatedEmptyBytes);

        var small = Assert.Single(rows, r => r.TableName == "zz_plain_small");
        Assert.Equal(0, small.EstimateRelationsUsable);
        Assert.Null(small.EstimatedEmptyBytes);
        Assert.Equal(DarlingStoreTableHealthReader.ReasonBelowSizeFloor, small.EstimateUnavailableReason);

        /* The sizes: the hypertable, every chunk and every compressed chunk table, summed straight from the size functions. */
        await using var sizes = new NpgsqlCommand(@"
WITH rels AS (
    SELECT 'collect.zz_many'::regclass::oid AS oid
    UNION ALL
    SELECT pg_catalog.format('%I.%I', chunk_schema, chunk_name)::regclass::oid
    FROM timescaledb_information.chunks WHERE hypertable_name = 'zz_many'
    UNION ALL
    SELECT cs.compress_relid::oid
    FROM _timescaledb_catalog.compression_settings AS cs
    WHERE cs.compress_relid IS NOT NULL AND cs.relid::oid <> 'collect.zz_many'::regclass::oid
)
SELECT sum(pg_relation_size(r.oid))::bigint, sum(pg_indexes_size(r.oid))::bigint,
       sum(COALESCE(pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0))::bigint
FROM rels AS r JOIN pg_class AS c ON c.oid = r.oid", connection);
        await using var sizeReader = await sizes.ExecuteReaderAsync(ct);
        Assert.True(await sizeReader.ReadAsync(ct));
        Assert.Equal(sizeReader.GetInt64(0), many.HeapBytes);
        Assert.Equal(sizeReader.GetInt64(1), many.IndexBytes);
        Assert.Equal(sizeReader.GetInt64(2), many.ToastBytes);
    }

    private static Task<System.Collections.Generic.HashSet<string>> ChunkNamesAsync(NpgsqlConnection connection, CancellationToken ct)
        => ChunkNamesAsync(connection, "zz_ht", ct);

    private static async Task<System.Collections.Generic.HashSet<string>> ChunkNamesAsync(NpgsqlConnection connection, string hypertable, CancellationToken ct)
    {
        var names = new System.Collections.Generic.HashSet<string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(
            "SELECT chunk_name FROM timescaledb_information.chunks WHERE hypertable_name = @h", connection);
        command.Parameters.AddWithValue("h", hypertable);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    /* The size read runs on several connections once a store has this many relations (#9170). The statement it feeds is
       compared with the unsplit one, run on one connection, over every relation: hypertables with uncompressed, compressed
       and dropped chunks, rows with out-of-line (TOAST) values, indexes, and a plain table. */
    [Fact]
    public async Task ASizedReadOnSeveralConnections_ReturnsTheSizesTheUnsplitStatementReturns()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9170 live tests.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        foreach (var name in new[] { "zz_sz_a", "zz_sz_b", "zz_sz_c" })
        {
            await ExecAsync(connection, $@"
CREATE TABLE collect.{name} (collection_time timestamp NOT NULL, v integer, payload text);
SELECT create_hypertable('collect.{name}', 'collection_time', chunk_time_interval => interval '1 day');
ALTER TABLE collect.{name} SET (timescaledb.compress);
CREATE INDEX ON collect.{name} (v);
INSERT INTO collect.{name} SELECT timestamp '2020-01-01' + (d || ' days')::interval, d, 'x' FROM generate_series(0, 299) AS d;
INSERT INTO collect.{name}
    SELECT timestamp '2020-01-01' + (d || ' days')::interval + interval '1 hour', d,
           (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 200))
    FROM generate_series(0, 19) AS d;
SELECT compress_chunk(c) FROM show_chunks('collect.{name}', older_than => timestamp '2020-03-01') AS c;
SELECT drop_chunks('collect.{name}', older_than => timestamp '2020-01-04', newer_than => timestamp '2020-01-01');", ct);
        }

        await ExecAsync(connection, @"
CREATE TABLE collect.zz_sz_plain (id integer PRIMARY KEY, payload text);
INSERT INTO collect.zz_sz_plain SELECT g, repeat('x', 200) FROM generate_series(1, 5000) AS g;", ct);

        /* More relations than the read gives one connection each to, so it splits. */
        await using (var count = new NpgsqlCommand(
            "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_name LIKE 'zz_sz_%'", connection))
        {
            Assert.True((long)(await count.ExecuteScalarAsync(ct))! > 800);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);

        var reference = new System.Collections.Generic.Dictionary<string, (long Total, long Heap, long Index, long Toast, int Chunks)>(StringComparer.Ordinal);
        /* The statement reads relallfrozen only from 18, so the oracle is built for the version this connection reports. */
        await using var versionCommand = new NpgsqlCommand("SELECT current_setting('server_version_num')::int", connection);
        var serverVersion = (int)(await versionCommand.ExecuteScalarAsync(ct))!;
        await using (var command = new NpgsqlCommand(DarlingStoreTableHealthReader.BuildSql(true, serverVersion), connection) { CommandTimeout = 300 })
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                reference[reader.GetString(0) + "." + reader.GetString(1)] =
                    (reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetInt64(6), reader.GetInt32(8));
            }
        }

        var rows = await DarlingStoreTableHealthReader.GetAsync(dataSource, ct);
        Assert.Equal(reference.Count, rows.Count);
        foreach (var row in rows)
        {
            var expected = reference[row.SchemaName + "." + row.TableName];
            Assert.Equal(expected, (row.TotalBytes, row.HeapBytes, row.IndexBytes, row.ToastBytes, row.ChunkCount));
        }

        var a = Assert.Single(rows, r => r.TableName == "zz_sz_a");
        Assert.True(a.ToastBytes > 0);
        Assert.True(a.ChunkCount > 250);

        var indexReference = new System.Collections.Generic.Dictionary<string, (long Bytes, long Heap, int Count)>(StringComparer.Ordinal);
        await using (var command = new NpgsqlCommand(DarlingStoreTableHealthReader.BuildIndexSql(true), connection) { CommandTimeout = 300 })
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                indexReference[reader.GetString(0) + "." + reader.GetString(1) + "." + reader.GetString(3) + "." + reader.GetBoolean(4)] =
                    (reader.GetInt64(7), reader.GetInt64(8), reader.GetInt32(5));
            }
        }

        var indexes = await DarlingStoreTableHealthReader.GetIndexesAsync(dataSource, ct);
        Assert.Equal(indexReference.Count, indexes.Count);
        foreach (var index in indexes)
        {
            var expected = indexReference[index.SchemaName + "." + index.TableName + "." + index.IndexName + "." + index.OnCompressedData];
            Assert.Equal(expected, (index.Bytes, index.HeapBytes, index.IndexCount));
        }

        Assert.Contains(indexes, i => i.TableName == "zz_sz_a" && i.IndexCount > 250 && i.Bytes > 0);
    }
}
