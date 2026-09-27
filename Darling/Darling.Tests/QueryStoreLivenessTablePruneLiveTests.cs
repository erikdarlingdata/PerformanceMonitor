/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The live loop test for #4250 item 3's row-capped prune (<see cref="DarlingRetention.UnorderedRowCappedDeleteSql"/>),
/// through the real purge path (<see cref="DarlingRetention.PurgeAsync"/>) rather than calling the builder or
/// <see cref="DarlingRetention.DrainBatchesAsync"/> directly. Seeds <c>collect.query_store_plan_map</c> and
/// <c>collect.query_store_text</c> with more than 2x a SMALL test cap of expired rows plus a handful of
/// in-cutoff rows, runs the product's own sweep, and asserts every expired row is gone, every in-cutoff row
/// remains, and the batch count matches the drain contract (ceil(expired / cap) full batches, the drain
/// loop stops on the first batch that clears fewer than the cap).
///
/// <para>Uses the <see cref="DarlingRetention.PurgeAsync"/> test seam (<c>livenessTouchedTablePruneRowCap</c>)
/// rather than seeding ~650k rows per table: the production call sites omit the parameter, so production
/// always runs the shipped 300,000 constant unchanged; only this test passes a small cap (1,000), pinned
/// separately below against the shipped default.</para>
/// </summary>
/* #1776 own-store: mints its own scratch database through ScratchPostgres and never touches the shared
   store's tables, so it cannot race the live collection. */
public sealed class QueryStoreLivenessTablePruneLiveTests
{
    private const int TestCap = 1_000;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The seam defaults to the shipped production constant — a test that forgets to pass a cap still
    /// exercises the real 300,000-row shape, not a silently-different one.
    /// </summary>
    [Fact]
    public void TheSeamDefaultsToTheShippedProductionCap()
    {
        var method = typeof(DarlingRetention).GetMethod(
            nameof(DarlingRetention.PurgeAsync), System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static)!;
        var parameter = Array.Find(method.GetParameters(), p => p.Name == "livenessTouchedTablePruneRowCap")!;
        Assert.NotNull(parameter);
        Assert.Equal(DarlingRetention.LivenessTouchedTablePruneRowCap, (int)parameter.DefaultValue!);
    }

    /// <summary>
    /// The loop test itself. Seeds <see cref="TestCap"/> * 2 + a short remainder of EXPIRED rows in each
    /// table (so the drain needs 3 batches: two full at the cap, one short), plus 5 IN-CUTOFF rows that must
    /// survive. Runs <see cref="DarlingRetention.PurgeAsync"/> — the same product entry point the daily
    /// sweep and the on-demand <c>purge_now</c> command call — with the small test cap, then asserts:
    /// every expired row gone, every in-cutoff row kept, and the logged batch count for each table matches
    /// the drain contract via the <c>{Batches}</c> field of <see cref="CapturingTestLogger"/>'s captured
    /// "Retention purge drained ... in {Batches} batch(es)" line (batchSize &gt; 1 is required for that line
    /// to be written — see <c>PurgeOneAsync</c> — which both callers satisfy).
    ///
    /// <para>A stop-after-one-batch mutation at the drain loop for these callers (temporarily forcing
    /// <see cref="DarlingRetention.DrainBatchesAsync"/> to return after its FIRST execution) turns this RED:
    /// see the PR body for the exact revert-and-rerun. That mutation is not committed here — it is a
    /// temporary product-code edit, verified and reverted by hand.</para>
    /// </summary>
    [Fact]
    public async Task PurgeAsync_DrainsBothLivenessTouchedTables_ClearingExpiredKeepingCurrent_InTheContractedBatchCount()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4250 item 3 live loop test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var utcNow = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);

        /* Both cutoffs are widestFactRetentionDays (the widest SHARED retention of any dim-feeding
           collector on this store, not a fixed 1-day floor — the scratch store's own catalog can widen it
           well past 1 day) plus each table's own margin. Measured live above: the actual map cutoff landed
           at 31 days back on this scratch store. 400 days back is comfortably past every horizon any
           collector's shared RetentionDays can produce (the widest of them, plan_force_actions, is 365
           days), matching the margin the existing live retention E2Es already use for their own
           "definitely expired" rows. A 1-hour-back row is comfortably inside every one of them. */
        var expiredStamp = utcNow.AddDays(-400);
        var keptStamp = utcNow.AddHours(-1);

        const int expiredPerTable = TestCap * 2 + 137; // 2 full batches + 1 short batch = 3 batches, drain contract.
        const int keptPerTable = 5;

        await SeedMapRowsAsync(connection, expiredPerTable, keptPerTable, expiredStamp, keptStamp, ct);
        await SeedTextRowsAsync(connection, expiredPerTable, keptPerTable, expiredStamp, keptStamp, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var purgeLog = new CapturingTestLogger();

        await DarlingRetention.PurgeAsync(
            postgres, timescaleAvailable: false, purgeLog, ct,
            livenessTouchedTablePruneRowCap: TestCap);

        // Every expired row is gone; every in-cutoff row survives, on BOTH tables.
        await AssertMapSurvivorsAsync(connection, keptPerTable, ct, purgeLog);
        await AssertTextSurvivorsAsync(connection, keptPerTable, ct, purgeLog);

        // The drain contract: ceil(expiredPerTable / cap) = 3 batches for each table (2 full + 1 short).
        var mapBatches = ExtractBatchCount(purgeLog, "query_store_plan_map");
        var textBatches = ExtractBatchCount(purgeLog, "query_store_text");
        Assert.Equal(3, mapBatches);
        Assert.Equal(3, textBatches);
    }

    private static async Task SeedMapRowsAsync(
        NpgsqlConnection connection, int expiredCount, int keptCount, DateTime expiredStamp, DateTime keptStamp, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) " +
            "SELECT 1, 'db', gs, ('\\x' || lpad(to_hex(gs), 8, '0'))::bytea, 'h' || gs, $2 " +
            "FROM generate_series(1, $1) gs", connection);
        command.Parameters.AddWithValue(expiredCount);
        command.Parameters.AddWithValue(expiredStamp);
        await command.ExecuteNonQueryAsync(ct);

        await using var keptCommand = new NpgsqlCommand(
            "INSERT INTO collect.query_store_plan_map (server_id, database_name, plan_id, digest, plan_hash, last_seen) " +
            "SELECT 2, 'db', gs, ('\\x' || lpad(to_hex(gs), 8, '0'))::bytea, 'h' || gs, $2 " +
            "FROM generate_series(1, $1) gs", connection);
        keptCommand.Parameters.AddWithValue(keptCount);
        keptCommand.Parameters.AddWithValue(keptStamp);
        await keptCommand.ExecuteNonQueryAsync(ct);
    }

    private static async Task SeedTextRowsAsync(
        NpgsqlConnection connection, int expiredCount, int keptCount, DateTime expiredStamp, DateTime keptStamp, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) " +
            "SELECT 1, 'db', gs, 'SELECT ' || gs, $2 " +
            "FROM generate_series(1, $1) gs", connection);
        command.Parameters.AddWithValue(expiredCount);
        command.Parameters.AddWithValue(expiredStamp);
        await command.ExecuteNonQueryAsync(ct);

        await using var keptCommand = new NpgsqlCommand(
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) " +
            "SELECT 2, 'db', gs, 'SELECT ' || gs, $2 " +
            "FROM generate_series(1, $1) gs", connection);
        keptCommand.Parameters.AddWithValue(keptCount);
        keptCommand.Parameters.AddWithValue(keptStamp);
        await keptCommand.ExecuteNonQueryAsync(ct);
    }

    private static async Task AssertMapSurvivorsAsync(
        NpgsqlConnection connection, int expectedKept, System.Threading.CancellationToken ct, CapturingTestLogger purgeLog)
    {
        await using var expiredCount = new NpgsqlCommand(
            "SELECT COUNT(*) FROM collect.query_store_plan_map WHERE server_id = 1", connection);
        Assert.True(0L == (long)(await expiredCount.ExecuteScalarAsync(ct))!, purgeLog.Joined);

        await using var keptCount = new NpgsqlCommand(
            "SELECT COUNT(*) FROM collect.query_store_plan_map WHERE server_id = 2", connection);
        Assert.True((long)expectedKept == (long)(await keptCount.ExecuteScalarAsync(ct))!, purgeLog.Joined);
    }

    private static async Task AssertTextSurvivorsAsync(
        NpgsqlConnection connection, int expectedKept, System.Threading.CancellationToken ct, CapturingTestLogger purgeLog)
    {
        await using var expiredCount = new NpgsqlCommand(
            "SELECT COUNT(*) FROM collect.query_store_text WHERE server_id = 1", connection);
        Assert.True(0L == (long)(await expiredCount.ExecuteScalarAsync(ct))!, purgeLog.Joined);

        await using var keptCount = new NpgsqlCommand(
            "SELECT COUNT(*) FROM collect.query_store_text WHERE server_id = 2", connection);
        Assert.True((long)expectedKept == (long)(await keptCount.ExecuteScalarAsync(ct))!, purgeLog.Joined);
    }

    /// <summary>
    /// Pulls the batch count out of <c>PurgeOneAsync</c>'s "Retention purge drained {Rows} row(s) from
    /// {Table} in {Batches} batch(es) (cap {Cap}), cutoff ..." line for the given table's unqualified name.
    /// </summary>
    private static int ExtractBatchCount(CapturingTestLogger purgeLog, string tableSuffix)
    {
        foreach (var line in purgeLog.Lines)
        {
            if (line.Contains("Retention purge drained", StringComparison.Ordinal)
                && line.Contains("from collect." + tableSuffix + " in", StringComparison.Ordinal))
            {
                var marker = " in ";
                var afterIn = line.IndexOf(marker, line.IndexOf("from collect." + tableSuffix, StringComparison.Ordinal), StringComparison.Ordinal) + marker.Length;
                var spaceIndex = line.IndexOf(' ', afterIn);
                return int.Parse(line[afterIn..spaceIndex]);
            }
        }

        Assert.Fail($"no drain-batch log line found for collect.{tableSuffix}; {purgeLog.Joined}");
        return -1;
    }
}
