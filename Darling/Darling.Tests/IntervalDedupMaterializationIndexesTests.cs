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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3597: the interval-dedup L1 aggregate (<see cref="TimescaleSupport.QueryStoreStatsIntervalHourlyView"/>)
/// carries no per-GROUP-BY-column index on its materialization, and an existing store is brought to that
/// shape at startup.
///
/// <para><b>What the rig measured, restated here because the pins below only make sense against it.</b>
/// TimescaleDB's default <c>create_group_indexes</c> built eleven <c>(column, bucket DESC)</c> btrees on
/// L1's materialization beside the bucket index. Nothing reads L1 by any of those columns — its three
/// child aggregates refresh over it by <c>bucket</c> range, the coverage probe and the arming gate read
/// <c>min(bucket)</c>, retention drops chunks — but every hourly refresh re-materializes a bucket by
/// DELETE + INSERT, and each inserted row cost twelve index inserts. <c>EXPLAIN (ANALYZE, BUFFERS, WAL)</c>
/// of one bucket's materialization INSERT at one tenth of the largest store's scale: 45.5 MB of WAL and
/// 1.64 M buffer touches with the group indexes, 10.7 MB and 447 K with the bucket index alone. The
/// refresh, the child refreshes, <c>compress_chunk</c> and <c>decompress_chunk</c> all ran unchanged
/// without them.</para>
///
/// <para><b>Why the pins are scoped to L1 and only L1.</b> The option is earned by that measurement, not
/// applied for symmetry: the composer-grain rollups ARE read through their group indexes (by
/// <c>server_id</c>, by <c>query_hash</c>), and the day-grain L2 refreshes once a day and was not measured.
/// A second aggregate wanting <c>create_group_indexes = false</c> should arrive with its own rig figures and
/// move the scope pin deliberately.</para>
/// </summary>
public sealed class IntervalDedupMaterializationIndexesTests
{
    private const string NoGroupIndexes = "timescaledb.create_group_indexes = false";

    [Fact]
    public void L1_IsCreatedWithoutGroupIndexes()
    {
        var sql = TimescaleSupport.CreateQueryStoreStatsIntervalHourlySql;

        Assert.Contains("WITH (timescaledb.continuous, " + NoGroupIndexes + ") AS", sql, StringComparison.Ordinal);
        /* Still materialized-only (#1759): the option rides beside `continuous`, it does not displace the
           absence TimescaleContinuousAggregateTests pins for the query-acceleration tier. */
        Assert.DoesNotContain("materialized_only", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The scope pin: every OTHER registered aggregate keeps TimescaleDB's default. Enumerated from the three
    /// registries the ensure sweep builds from, so a new aggregate is covered the day it is registered.
    /// </summary>
    [Fact]
    public void EveryOtherAggregate_KeepsTheDefaultGroupIndexes_UntilMeasured()
    {
        var others = TimescaleSupport.HourlyAggregates
            .Concat(TimescaleSupport.DailyAggregates)
            .Concat(TimescaleSupport.BaselineAggregates)
            .Where(a => !string.Equals(a.View, TimescaleSupport.QueryStoreStatsIntervalHourlyView, StringComparison.Ordinal))
            .ToList();

        Assert.NotEmpty(others);
        foreach (var (createSql, view) in others)
        {
            Assert.DoesNotContain("create_group_indexes", createSql, StringComparison.Ordinal);
            Assert.True(view.Length > 0);
        }

        /* And L1 is in the hourly registry, so the converge below finds a materialization to act on. */
        Assert.Contains(TimescaleSupport.HourlyAggregates, a => string.Equals(a.View, TimescaleSupport.QueryStoreStatsIntervalHourlyView, StringComparison.Ordinal));
    }

    /// <summary>
    /// The converge's catalog read selects by SHAPE — one column then <c>bucket DESC</c> — on L1's
    /// materialization only, so the bucket index (<c>(bucket DESC)</c> alone) can never match, and a
    /// materialization other than L1's is never touched. Pinned as text because the regex runs in
    /// PostgreSQL; the gated test below asks the server.
    /// </summary>
    [Fact]
    public void TheGroupIndexRead_SelectsByShape_OnL1Only()
    {
        var sql = TimescaleSupport.IntervalDedupMaterializationGroupIndexesSql;

        Assert.Contains($"ca.view_name = '{TimescaleSupport.QueryStoreStatsIntervalHourlyView}'", sql, StringComparison.Ordinal);
        Assert.Contains(@"i.indexdef ~ 'USING btree \([a-z_]+, bucket DESC\)$'", sql, StringComparison.Ordinal);
        Assert.Contains("i.tablename = ca.materialization_hypertable_name", sql, StringComparison.Ordinal);
        /* Resolved from the view name, never a hard-coded _materialized_hypertable_N. */
        Assert.DoesNotContain("_materialized_hypertable_", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The drop yields to a refresh in flight rather than queueing behind it (the queued-exclusive convoy
    /// <c>HourlyRefreshStartOffset</c> documents): a bounded lock timeout, short against the grid's hour and
    /// long against an idle lock.
    /// </summary>
    [Fact]
    public void TheIndexDrop_WaitsABoundedTimeForItsLock()
    {
        var timeout = TimescaleSupport.IntervalDedupIndexDropLockTimeout;

        Assert.EndsWith("s", timeout, StringComparison.Ordinal);
        var seconds = int.Parse(timeout.TrimEnd('s'), System.Globalization.CultureInfo.InvariantCulture);
        Assert.InRange(seconds, 1, 60);
    }

    /// <summary>
    /// The evidence no string pin can give, on a scratch database with TimescaleDB: (1) TimescaleDB honours the
    /// option — a freshly created L1 materialization carries exactly one index, on <c>(bucket DESC)</c>;
    /// (2) the converge finds a pre-#3597 store's group index (planted by hand in TimescaleDB's own shape and
    /// name), drops it, reports one, and reports zero on the next call; (3) the bucket index is never
    /// selected; (4) the refresh still materializes rows afterwards, through the same policy window the
    /// product uses, and the child corrected hourly still reads them.
    /// </summary>
    [Fact]
    public async Task OnAFreshStore_L1HasOnlyTheBucketIndex_AndTheConvergeDropsAPlantedGroupIndex_AgainstDevPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #3597 materialization-index test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
            "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

        /* Manual refreshes below assert on exact ranges; strip the policies so a background first run
           cannot race them (the QueryStoreTrendRoutingLiveTests discipline). */
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            await using var remove = new NpgsqlCommand(
                $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)", connection);
            await remove.ExecuteNonQueryAsync(ct);
        }

        var (matSchema, matName) = await MaterializationOfAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, ct);

        /* (1) A fresh L1 honours the option: the bucket index and nothing else. */
        var fresh = await IndexDefinitionsAsync(connection, matSchema, matName, ct);
        var only = Assert.Single(fresh);
        Assert.EndsWith("USING btree (bucket DESC)", only, StringComparison.Ordinal);

        /* A settled store: nothing to find, nothing dropped. */
        Assert.Equal(0, await TimescaleSupport.EnsureIntervalDedupMaterializationIndexesAsync(connection, null, ct));

        /* (2) Plant what an earlier build's CREATE left behind, in TimescaleDB's own name and shape. */
        var planted = matName + "_server_id_bucket_idx";
        await using (var plant = new NpgsqlCommand(
            $"CREATE INDEX \"{planted}\" ON \"{matSchema}\".\"{matName}\" (server_id, bucket DESC)", connection))
        {
            await plant.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(2, (await IndexDefinitionsAsync(connection, matSchema, matName, ct)).Count);

        Assert.Equal(1, await TimescaleSupport.EnsureIntervalDedupMaterializationIndexesAsync(connection, null, ct));
        var afterDrop = await IndexDefinitionsAsync(connection, matSchema, matName, ct);
        var survivor = Assert.Single(afterDrop);
        /* (3) The bucket index survived, by shape. */
        Assert.EndsWith("USING btree (bucket DESC)", survivor, StringComparison.Ordinal);

        Assert.Equal(0, await TimescaleSupport.EnsureIntervalDedupMaterializationIndexesAsync(connection, null, ct));

        /* (4) The aggregate still materializes without the group indexes. Fixed instants, not now-relative. */
        var hour = new DateTime(2026, 3, 4, 10, 0, 0, DateTimeKind.Unspecified);
        await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT 1, $1 + (k * interval '10 minutes'), -735970, 'interval-dedup-index-e2e', 'db', 100 + i, 1000 + i,
    'Regular', $1, 'mod', md5('q' || i), 10 * (k + 1), 500, 300, 900, 700, 'PRIMARY', 77, $1
FROM generate_series(0, 9) AS i CROSS JOIN generate_series(0, 2) AS k", connection))
        {
            seed.Parameters.AddWithValue(hour);
            await seed.ExecuteNonQueryAsync(ct);
        }

        await RefreshAsync(connection, TimescaleSupport.QueryStoreStatsIntervalHourlyView, hour, hour.AddHours(1), ct);
        await RefreshAsync(connection, TimescaleSupport.QueryStoreStatsCorrectedHourlyView, hour, hour.AddHours(1), ct);

        await using (var l1 = new NpgsqlCommand(
            $"SELECT count(*), sum(execution_count) FROM collect.{TimescaleSupport.QueryStoreStatsIntervalHourlyView} WHERE server_id = -735970", connection))
        await using (var reader = await l1.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct));
            /* Ten interval identities, each deduped to its LAST snapshot (30 executions). */
            Assert.Equal(10L, reader.GetInt64(0));
            Assert.Equal(300L, Convert.ToInt64(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture));
        }

        await using (var corrected = new NpgsqlCommand(
            $"SELECT sum(execution_count_sum) FROM collect.{TimescaleSupport.QueryStoreStatsCorrectedHourlyView} WHERE server_id = -735970", connection))
        {
            /* Ten query_hash groups, one per identity; the child sums L1's deduped counts, never the raw snapshots. */
            Assert.Equal(300L, Convert.ToInt64(await corrected.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture));
        }
    }

    private static async Task<(string Schema, string Name)> MaterializationOfAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT materialization_hypertable_schema, materialization_hypertable_name FROM timescaledb_information.continuous_aggregates WHERE view_schema = 'collect' AND view_name = $1", connection);
        command.Parameters.AddWithValue(view);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), $"{view} was not created");
        return (reader.GetString(0), reader.GetString(1));
    }

    private static async Task<List<string>> IndexDefinitionsAsync(NpgsqlConnection connection, string schema, string table, CancellationToken ct)
    {
        var definitions = new List<string>();
        await using var command = new NpgsqlCommand(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = $1 AND tablename = $2 ORDER BY indexname", connection);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            definitions.Add(reader.GetString(0));
        }

        return definitions;
    }

    /// <summary>Bounded retry on 55P03 — the same reason QueryStoreTrendRoutingLiveTests retries: a policy's
    /// creation-time first run can still be finishing when the manual refresh lands.</summary>
    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var refresh = new NpgsqlCommand(
                    $"CALL refresh_continuous_aggregate('collect.{view}', $1::timestamp, $2::timestamp)", connection);
                refresh.Parameters.AddWithValue(from);
                refresh.Parameters.AddWithValue(to);
                await refresh.ExecuteNonQueryAsync(ct);
                return;
            }
            catch (PostgresException ex) when (ex.SqlState == "55P03" && attempt < 12)
            {
                await Task.Delay(TimeSpan.FromSeconds(1), ct);
            }
        }
    }
}
