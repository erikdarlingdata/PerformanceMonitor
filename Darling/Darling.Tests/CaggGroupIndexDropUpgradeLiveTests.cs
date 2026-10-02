/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4503's upgrade-equivalence pin: a store already at V151, with its six Query Store rollups
/// materialized and compressed, migrates to V152 and every reader query returns the SAME rows before and
/// after. The rung only drops indexes — it must never change what a reader sees.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (it creates the continuous aggregates the
/// shared fixture deliberately leaves to the tests, compresses their materialization chunks, and rolls the
/// version stamp back to plant a pre-upgrade store), so it cannot race the shared store and is not
/// serialized against the <c>live-postgres</c> collection. Same shape as
/// <c>IntervalDedupMaterializationIndexesTests</c> and <c>QueryStoreCorrectedRollupLiveTests</c>.</para>
/// </summary>
public sealed class CaggGroupIndexDropUpgradeLiveTests
{
    private const int RungVersion = 152;
    private const int PreviousVersion = 151;

    /// <summary>Distinctive fake ids — a real server_id is a storage-name hash, never these.</summary>
    private const int ServerA = -415203;
    private const int ServerB = -415204;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The six rollups this rung touches, in the shape the migration's own <c>DO</c> block enumerates them.
    /// </summary>
    private static readonly string[] SixRollups =
    {
        TimescaleSupport.QueryStoreStatsHourlyView,
        TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
        TimescaleSupport.QueryStoreStatsDailyView,
        TimescaleSupport.QueryStoreStatsCorrectedDailyView,
        TimescaleSupport.QueryStoreStatsIntervalDailyView,
        TimescaleSupport.QueryStoreStatsDayGrainDailyView,
    };

    /// <summary>
    /// Pin (b): a V151 store with materialized, COMPRESSED Query Store rollups migrates to V152, and every
    /// reader query returns identical rows before and after — proving the rung's index drop changes no
    /// answer, only how it is served.
    ///
    /// <para><b>RED on <c>origin/dev</c> (pre-#4503):</b> the index-shape assertions at the end fail at
    /// runtime — the group indexes this rung drops are still present, and the kept-index names this test
    /// checks for do not exist yet (dev never creates them), so <c>Assert.Empty</c> on the leftover set and
    /// <c>Assert.Contains</c> on the kept names both fail. See the PR body for the captured line.</para>
    /// </summary>
    [Fact]
    public async Task AStoreAtV151WithMaterializedCompressedRollups_MigratesToV152_AndEveryReaderReturnsTheSameRows()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the V152 upgrade-equivalence pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* Climb to the top first (as every rung test does) — the CAGGs and their rollups need the rest
               of the schema (servers, config) present. */
            await PgMigrations.MigrateAsync(connection, ct);

            var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the V152 upgrade-equivalence pin");

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

            /* With the group indexes: roll the version stamp back to V151 first, then create the rollups the
               way a store on THAT build did — the default create_group_indexes shape, no create_group_indexes
               option and no kept-index statements — rather than through this build's ensure sweep (which
               already withholds them). This is what makes the fixture a genuine "already at V151" store
               instead of a fresh migrate that happens to look like one. */
            await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion);
            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(PreviousVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct)));
            }

            await CreateV151ShapeRollupsAsync(connection, ct);

            /* Plant two servers x two days x a few databases/queries of query_store_stats. */
            var day1 = new DateTime(2026, 4, 6, 0, 0, 0, DateTimeKind.Unspecified);
            var day2 = day1.AddDays(1);
            await SeedQueryStoreStatsAsync(connection, ServerA, day1, ct);
            await SeedQueryStoreStatsAsync(connection, ServerA, day2, ct);
            await SeedQueryStoreStatsAsync(connection, ServerB, day1, ct);
            await SeedQueryStoreStatsAsync(connection, ServerB, day2, ct);

            /* Refresh every one of the six views over both seeded days. */
            var from = day1;
            var to = day2.AddDays(1);
            foreach (var view in SixRollups)
            {
                await RefreshAsync(connection, view, from, to, ct);
            }

            /* Compress the materialization chunks, the product's own way (ALTER MATERIALIZED VIEW ... SET
               (timescaledb.compress ...) then compress_chunk over every chunk). */
            foreach (var view in SixRollups)
            {
                await CompressMaterializationAsync(connection, view, ct);
            }

            /* Capture every reader's rows BEFORE the V152 step. */
            var before = await CaptureReaderRowsAsync(connection, ct);

            /* The upgrade: apply every rung above V151 — V152 plus whichever later rungs this build
               also carries (V153 (#4608) as of this build's top). */
            var applied = await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= RungVersion), applied);

            /* Capture every reader's rows AFTER. */
            var after = await CaptureReaderRowsAsync(connection, ct);

            Assert.Equal(before.Count, after.Count);
            for (var i = 0; i < before.Count; i++)
            {
                Assert.Equal(before[i].Name, after[i].Name);
                Assert.Equal(before[i].Rows, after[i].Rows);
            }

            /* The index-shape assertions: dropped indexes gone, kept indexes present, matched by key
               columns — this half is RED on origin/dev (see the doc comment above). */
            foreach (var view in SixRollups)
            {
                var (matSchema, matTable) = await MaterializationOfAsync(connection, view, ct);
                var indexes = await IndexColumnsAsync(connection, matSchema, matTable, ct);

                var keptColumns = KeptFirstColumnsFor(view);
                var droppedColumns = DroppedFirstColumnsFor(view);

                foreach (var kept in keptColumns)
                {
                    Assert.Contains(indexes, idx => idx.Count == 2 && idx[0] == kept && idx[1] == "bucket");
                }

                foreach (var dropped in droppedColumns)
                {
                    Assert.DoesNotContain(indexes, idx => idx.Count == 2 && idx[0] == dropped && idx[1] == "bucket");
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                /* Own-store scratch database is dropped by ScratchPostgres.DisposeAsync; nothing else to
                   restore on a database nobody else shares. */
                await Task.CompletedTask;
            });
        }
    }

    /// <summary>
    /// #4503: V152's <c>SET LOCAL lock_timeout</c> is derived from
    /// <see cref="PgMigrations.MigrationCommandTimeoutSeconds"/> so the rung WAITS OUT a running rollup
    /// refresh's <c>AccessExclusiveLock</c> on a materialization hypertable rather than failing on it.
    /// A second connection locks one rollup's materialization for about 8 s; the migration, started on
    /// another task, must still finish once that lock is released.
    ///
    /// <para><b>RED at the pre-#4503 head:</b> that build's lock_timeout is a flat <c>'5s'</c>, shorter
    /// than the 8 s hold, so the migration's <c>DROP INDEX</c> statement fails with <c>55P03</c>
    /// ("canceling statement due to lock timeout") before the lock is ever released. See the PR body for
    /// the captured exception.</para>
    /// </summary>
    [Fact]
    public async Task ARunningRollupRefreshHoldingTheMaterializationLockForEightSeconds_IsWaitedOut_AndTheMigrationReachesTheTop()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the V152 lock-wait pin.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the V152 lock-wait pin");

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

            /* Stop this scratch database's TimescaleDB scheduler (the CollectionHealthAggregateTests
               precedent): the refresh policy CreateV151ShapeRollupsAsync installs below would otherwise
               launch its own first run and contend with this test's own ACCESS SHARE lock, which measured
               as a 40P01 deadlock rather than the clean queue-and-wait this pin means to exercise. */
            await ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");

            /* Roll the version stamp back and create the rollups the V151-era way (the default
               create_group_indexes shape) — the same fixture CreateV151ShapeRollupsAsync builds for the
               upgrade-equivalence pin above. Only that shape has a group index on the hourly view's
               materialization for V152's DROP INDEX to actually take an AccessExclusiveLock on; the
               current build's already-pruned kept-index shape has nothing there for a lock to contend
               over, so the wait this pin measures would never happen against it. */
            await ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= " + RungVersion);
            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(PreviousVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct)));
            }

            await CreateV151ShapeRollupsAsync(connection, ct);

            var (matSchema, matTable) = await MaterializationOfAsync(connection, TimescaleSupport.QueryStoreStatsHourlyView, ct);

            await using var lockConnection = new NpgsqlConnection(scratch.ConnectionString);
            await lockConnection.OpenAsync(ct);
            await using var lockTransaction = await lockConnection.BeginTransactionAsync(ct);
            await using (var lockCommand = new NpgsqlCommand(
                $"LOCK TABLE {matSchema}.{matTable} IN ACCESS SHARE MODE", lockConnection, lockTransaction))
            {
                await lockCommand.ExecuteNonQueryAsync(ct);
            }

            var releaseAfter = Task.Delay(TimeSpan.FromSeconds(8), ct);

            var started = DateTime.UtcNow;
            var migrateTask = Task.Run(async () =>
            {
                await using var migrateConnection = new NpgsqlConnection(scratch.ConnectionString);
                await migrateConnection.OpenAsync(ct);
                return await PgMigrations.MigrateAsync(migrateConnection, ct);
            }, ct);

            await releaseAfter;
            await lockTransaction.CommitAsync(ct);

            var applied = await migrateTask;
            var elapsed = DateTime.UtcNow - started;

            Assert.Equal(PgMigrations.Scripts.Count(m => m.Version >= RungVersion), applied);
            Assert.True(elapsed >= TimeSpan.FromSeconds(7), $"expected the migration to have waited out the lock (~8s hold); it finished in {elapsed}");

            /* The final version is the build's TOP rung, not the literal 152 — a later rung (V153, #4608)
               may also apply once the wait is over. V152's OWN effect (the group index drop on this
               materialization) is what this pin is actually about, so assert that directly below rather
               than pinning the final version to a rung number that will drift again the next time a rung
               is added above this one. */
            using (var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await version.ExecuteScalarAsync(ct)));
            }

            var indexes = await IndexColumnsAsync(connection, matSchema, matTable, ct);
            foreach (var dropped in DroppedFirstColumnsFor(TimescaleSupport.QueryStoreStatsHourlyView))
            {
                Assert.DoesNotContain(indexes, idx => idx.Count == 2 && idx[0] == dropped && idx[1] == "bucket");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await Task.CompletedTask;
            });
        }
    }

    /// <summary>
    /// The keep/drop decision (#4503, restated for this pin): <c>bucket_idx</c> and
    /// <c>(server_id, bucket)</c> on all six, plus <c>(server_name, bucket)</c> on the two hourly views.
    /// </summary>
    private static string[] KeptFirstColumnsFor(string view) =>
        view == TimescaleSupport.QueryStoreStatsHourlyView || view == TimescaleSupport.QueryStoreStatsCorrectedHourlyView
            ? new[] { "server_id", "server_name" }
            : new[] { "server_id" };

    private static string[] DroppedFirstColumnsFor(string view) => view switch
    {
        _ when view == TimescaleSupport.QueryStoreStatsHourlyView || view == TimescaleSupport.QueryStoreStatsCorrectedHourlyView =>
            new[] { "database_name", "module_name", "query_hash" },
        _ when view == TimescaleSupport.QueryStoreStatsIntervalDailyView =>
            new[] { "database_name", "execution_type_desc", "first_execution_time", "module_name", "plan_id", "query_hash", "query_id", "replica_role", "runtime_stats_interval_id", "server_name" },
        _ => new[] { "database_name", "module_name", "query_hash", "server_name" },
    };

    /// <summary>
    /// Creates the six rollups the way a V151-era build did: TimescaleDB's DEFAULT
    /// <c>create_group_indexes</c> (no option at all — every group column gets its own
    /// <c>(column, bucket DESC)</c> index), and none of this build's explicit kept-index statements. Built
    /// by stripping <c>, timescaledb.create_group_indexes = false</c> out of this build's own CREATE text
    /// rather than hand-copying the SELECT, so the fixture's columns can never drift from the real view.
    /// </summary>
    private static async Task CreateV151ShapeRollupsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var createSqls = new[]
        {
            TimescaleSupport.CreateQueryStoreStatsHourlySql,
            TimescaleSupport.CreateQueryStoreStatsIntervalHourlySql,
            TimescaleSupport.CreateQueryStoreStatsCorrectedHourlySql,
            TimescaleSupport.CreateQueryStoreStatsDailySql,
            TimescaleSupport.CreateQueryStoreStatsCorrectedDailySql,
            TimescaleSupport.CreateQueryStoreStatsIntervalDailySql,
            TimescaleSupport.CreateQueryStoreStatsDayGrainDailySql,
        };

        foreach (var createSql in createSqls)
        {
            var v151Shape = createSql.Replace(", timescaledb.create_group_indexes = false", string.Empty, StringComparison.Ordinal);
            Assert.DoesNotContain("create_group_indexes", v151Shape, StringComparison.Ordinal);

            await using var create = new NpgsqlCommand(v151Shape, connection);
            await create.ExecuteNonQueryAsync(ct);
        }

        foreach (var view in SixRollups)
        {
            await using var policy = new NpgsqlCommand(
                view.EndsWith("_daily", StringComparison.Ordinal)
                    ? TimescaleSupport.AddDailyRefreshPolicySql(view)
                    : TimescaleSupport.AddHourlyRefreshPolicySql(view), connection);
            await policy.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task SeedQueryStoreStatsAsync(NpgsqlConnection connection, int serverId, DateTime day, CancellationToken ct)
    {
        await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us)
SELECT
    (extract(epoch FROM $1)::bigint * 1000) + (d.n * 100) + q.n,
    $1 + (d.n * interval '1 hour'),
    $2, 'upgrade-eq-server-' || $2::text, 'db' || d.n::text, 'dbo.proc' || q.n::text, md5('q' || d.n::text || q.n::text),
    100 + q.n, 1000 + q.n, 'Regular', 'PRIMARY',
    77, $1, $1,
    10 * (q.n + 1), 500, 300, 900, 700
FROM generate_series(0, 5) AS d(n) CROSS JOIN generate_series(0, 3) AS q(n)", connection);
        insert.Parameters.AddWithValue(day);
        insert.Parameters.AddWithValue(serverId);
        await insert.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Captures the rows every named reader query returns, ordered so the before/after comparison is
    /// stable. Named readers: <see cref="QueryStoreTrendRouting.BuildRollupTrendSql"/> with and without the
    /// database filter, <see cref="QueryStoreTrendRouting.RollupBoundsSql"/>, and one composer-shaped SELECT
    /// per view filtering on server_id + a bucket range and on database_name.
    /// </summary>
    private static async Task<List<(string Name, List<string> Rows)>> CaptureReaderRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var results = new List<(string Name, List<string> Rows)>();

        var from = new DateTime(2026, 4, 6, 0, 0, 0, DateTimeKind.Unspecified);
        var to = new DateTime(2026, 4, 8, 0, 0, 0, DateTimeKind.Unspecified);
        var future = to.AddYears(1);

        foreach (var serverId in new[] { ServerA, ServerB })
        {
            results.Add(($"trend-no-db-filter:{serverId}", await CaptureRowsAsync(connection,
                QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: false),
                cmd =>
                {
                    cmd.Parameters.AddWithValue(serverId);
                    cmd.Parameters.AddWithValue(from);
                    cmd.Parameters.AddWithValue(to);
                    cmd.Parameters.AddWithValue(future);
                }, ct)));

            results.Add(($"trend-with-db-filter:{serverId}", await CaptureRowsAsync(connection,
                QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true),
                cmd =>
                {
                    cmd.Parameters.AddWithValue(serverId);
                    cmd.Parameters.AddWithValue(from);
                    cmd.Parameters.AddWithValue(to);
                    cmd.Parameters.AddWithValue(future);
                    cmd.Parameters.AddWithValue(new[] { "db0", "db1" });
                }, ct)));
        }

        results.Add(("rollup-bounds", await CaptureRowsAsync(connection, QueryStoreTrendRouting.RollupBoundsSql, _ => { }, ct)));

        foreach (var view in SixRollups)
        {
            foreach (var serverId in new[] { ServerA, ServerB })
            {
                results.Add(($"composer-server:{view}:{serverId}", await CaptureRowsAsync(connection,
                    $"SELECT * FROM collect.{view} WHERE server_id = $1 AND bucket >= $2 AND bucket <= $3 ORDER BY bucket",
                    cmd =>
                    {
                        cmd.Parameters.AddWithValue(serverId);
                        cmd.Parameters.AddWithValue(from);
                        cmd.Parameters.AddWithValue(to);
                    }, ct)));

                results.Add(($"composer-database:{view}:{serverId}", await CaptureRowsAsync(connection,
                    $"SELECT * FROM collect.{view} WHERE server_id = $1 AND database_name = 'db0' AND bucket >= $2 AND bucket <= $3 ORDER BY bucket",
                    cmd =>
                    {
                        cmd.Parameters.AddWithValue(serverId);
                        cmd.Parameters.AddWithValue(from);
                        cmd.Parameters.AddWithValue(to);
                    }, ct)));
            }
        }

        return results;
    }

    private static async Task<List<string>> CaptureRowsAsync(
        NpgsqlConnection connection, string sql, Action<NpgsqlCommand> bind, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection);
        bind(command);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture) ?? "<null>";
            }
            rows.Add(string.Join("|", cells));
        }
        return rows;
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

    /// <summary>The first two key columns of every btree on the materialization, one list per index —
    /// a single-column bucket index reads as a one-element list, never matching the 2-column checks above.</summary>
    private static async Task<List<List<string>>> IndexColumnsAsync(NpgsqlConnection connection, string schema, string table, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT array_agg(a.attname ORDER BY k.ord)
FROM pg_index i
JOIN pg_class c ON c.oid = i.indrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
WHERE n.nspname = $1 AND c.relname = $2
GROUP BY i.indexrelid", connection);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);

        var indexes = new List<List<string>>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            indexes.Add(((string[])reader.GetValue(0)).ToList());
        }
        return indexes;
    }

    private static async Task CompressMaterializationAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using (var alter = new NpgsqlCommand(
            $"ALTER MATERIALIZED VIEW collect.{view} SET (timescaledb.compress, timescaledb.compress_segmentby = 'server_id')", connection))
        {
            await alter.ExecuteNonQueryAsync(ct);
        }

        var chunks = await ReadMaterializationChunksAsync(connection, view, ct);
        foreach (var chunk in chunks)
        {
            await using var compress = new NpgsqlCommand($"SELECT compress_chunk('{chunk}'::regclass, if_not_compressed => true)", connection);
            await compress.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<List<string>> ReadMaterializationChunksAsync(NpgsqlConnection connection, string view, CancellationToken ct)
    {
        await using var read = new NpgsqlCommand($@"
SELECT format('%I.%I', c.chunk_schema, c.chunk_name)
FROM timescaledb_information.chunks AS c
JOIN timescaledb_information.continuous_aggregates AS ca
  ON  c.hypertable_schema = ca.materialization_hypertable_schema
  AND c.hypertable_name = ca.materialization_hypertable_name
WHERE ca.view_schema = 'collect' AND ca.view_name = '{view}'
ORDER BY c.range_start", connection);
        var chunks = new List<string>();
        await using var reader = await read.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            chunks.Add(reader.GetString(0));
        }
        return chunks;
    }

    /// <summary>Bounded retry on 55P03, the same reason IntervalDedupMaterializationIndexesTests retries: a
    /// policy's creation-time first run can still be finishing when the manual refresh lands.</summary>
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

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
