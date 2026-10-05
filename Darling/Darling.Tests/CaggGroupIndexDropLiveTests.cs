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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4503: on a FRESH migrate, the six Query Store rollups' materializations carry EXACTLY the group
/// indexes an UPGRADED store keeps after V152's <c>DO</c> block drops the unread ones — the same key
/// columns, the same <c>bucket DESC</c> order, and the same PostgreSQL-assigned name — and nothing else.
///
/// <para><b>#1776 own-store</b> — mints its own scratch database (<see cref="ScratchPostgres"/>) so the
/// index catalog it reads is not shared with any other live class, the same reason
/// <see cref="IntervalDedupMaterializationIndexesTests"/> does.</para>
/// </summary>
public sealed class CaggGroupIndexDropLiveTests
{
    /// <summary>The six views #4503 touches, with the key column(s) each KEEPS.</summary>
    private static readonly (string View, string[] KeptColumns)[] Views =
    {
        (TimescaleSupport.QueryStoreStatsHourlyView, new[] { "server_id", "server_name" }),
        (TimescaleSupport.QueryStoreStatsCorrectedHourlyView, new[] { "server_id", "server_name" }),
        (TimescaleSupport.QueryStoreStatsDailyView, new[] { "server_id" }),
        (TimescaleSupport.QueryStoreStatsCorrectedDailyView, new[] { "server_id" }),
        (TimescaleSupport.QueryStoreStatsIntervalDailyView, new[] { "server_id" }),
        (TimescaleSupport.QueryStoreStatsDayGrainDailyView, new[] { "server_id" }),
    };

    /// <summary>Every column any of the six views' default <c>create_group_indexes</c> would have built,
    /// beyond what <see cref="Views"/> keeps — the DROPPED set this pin proves absent.</summary>
    private static readonly IReadOnlyDictionary<string, string[]> DroppedColumns = new Dictionary<string, string[]>(StringComparer.Ordinal)
    {
        [TimescaleSupport.QueryStoreStatsHourlyView] = new[] { "database_name", "module_name", "query_hash" },
        [TimescaleSupport.QueryStoreStatsCorrectedHourlyView] = new[] { "database_name", "module_name", "query_hash" },
        [TimescaleSupport.QueryStoreStatsDailyView] = new[] { "database_name", "module_name", "query_hash", "server_name" },
        [TimescaleSupport.QueryStoreStatsCorrectedDailyView] = new[] { "database_name", "module_name", "query_hash", "server_name" },
        [TimescaleSupport.QueryStoreStatsIntervalDailyView] = new[]
        {
            "database_name", "execution_type_desc", "first_execution_time", "module_name", "plan_id",
            "query_hash", "query_id", "replica_role", "runtime_stats_interval_id", "server_name",
        },
        [TimescaleSupport.QueryStoreStatsDayGrainDailyView] = new[] { "database_name", "module_name", "query_hash", "server_name" },
    };

    /// <summary>
    /// (a) A fresh migrate to the top, then the aggregates ensured: on each of the six, every DROPPED
    /// column's group index is absent, every KEPT column's is present, and the plain <c>bucket_idx</c>
    /// survives too. RED on <c>origin/dev</c> — a fresh store there still carries the dropped indexes,
    /// because <c>create_group_indexes = false</c> and the kept-index follow-up did not exist before #4503.
    /// </summary>
    [Fact]
    public async Task FreshMigrate_KeepsOnlyTheSurvivingGroupIndexes_OnAllSixRollups()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4503 fresh-vs-upgraded index test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct),
                "the dev fixture is expected to have TimescaleDB installed");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            foreach (var (view, keptColumns) in Views)
            {
                var (matSchema, matName) = await MaterializationOfAsync(connection, view, ct);
                var indexes = await GroupIndexColumnsAsync(connection, matSchema, matName, ct);

                foreach (var dropped in DroppedColumns[view])
                {
                    Assert.DoesNotContain(dropped, indexes);
                }

                foreach (var kept in keptColumns)
                {
                    Assert.Contains(kept, indexes);
                }

                /* The plain bucket index — never a group index, never touched by #4503 either side. */
                var plainBucket = await ScalarBoolAsync(connection,
                    $"SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = '{matSchema}' AND tablename = '{matName}' AND indexdef ~ 'USING btree \\(bucket DESC\\)$')",
                    ct);
                Assert.True(plainBucket, $"{view}'s materialization lost its plain bucket index");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// (c) The kept-index build is idempotent: running <see cref="TimescaleSupport.EnsureContinuousAggregatesAsync"/>
    /// a second time issues no error and leaves the SAME <c>pg_indexes</c> rows on every one of the six
    /// materializations — the follow-up's own catalog read finds each kept index already present and
    /// creates nothing more.
    /// </summary>
    [Fact]
    public async Task EnsureContinuousAggregates_RunTwice_IsIdempotent_OnTheKeptIndexes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4503 idempotency test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct));
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            var before = new Dictionary<string, List<string>>(StringComparer.Ordinal);
            foreach (var (view, _) in Views)
            {
                var (matSchema, matName) = await MaterializationOfAsync(connection, view, ct);
                before[view] = await IndexDefinitionsAsync(connection, matSchema, matName, ct);
            }

            /* Second pass — no error. */
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);

            foreach (var (view, _) in Views)
            {
                var (matSchema, matName) = await MaterializationOfAsync(connection, view, ct);
                var after = await IndexDefinitionsAsync(connection, matSchema, matName, ct);
                Assert.Equal(before[view], after);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// V152 on a store WITHOUT TimescaleDB is a no-op: a plain migrate (never calling
    /// <see cref="LiveTimescaleProbe.TryEnableAsync"/>) reaches the top with no exception, and the
    /// recorded schema version is 152. RED at bab1c4114 (pre-guard): the DO block reaches
    /// timescaledb_information unconditionally and raises 42P01, relation "timescaledb_information.
    /// continuous_aggregates" does not exist.
    /// </summary>
    [Fact]
    public async Task V152_OnAStoreWithoutTimescaleDb_CompletesAsANoOp()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V152 no-TimescaleDB no-op pin (it mints its own scratch database and never enables TimescaleDB on it).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            /* Deliberately never calls LiveTimescaleProbe.TryEnableAsync — this is the plain-PostgreSQL
               case the migration guard must no-op on. */
            await PgMigrations.MigrateAsync(connection, ct);

            using var version = new NpgsqlCommand("SELECT MAX(version) FROM darling_schema_version", connection);
            var recorded = Convert.ToInt32(await version.ExecuteScalarAsync(ct));
            Assert.Equal(StorageVersion.SchemaVersion, recorded);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// The reviewer's false-positive case: a store at V150 with no rollup materialized at all does NOT
    /// map to 152 — V151's own sentinel (the <c>ag_replica_states.group_id</c> column) is false at V150,
    /// so the V152 arm's AND fails and the version falls through correctly.
    /// </summary>
    [Fact]
    public async Task AStoreAtV150WithNoRollup_DoesNotMapToV152()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the V150 false-positive pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            /* Roll the version stamp back to V150 — no rollup, no group_id column, plain PostgreSQL,
               exactly the reviewer's false-positive shape. */
            await using (var rollback = new NpgsqlCommand("DELETE FROM darling_schema_version WHERE version >= 151", connection))
            {
                await rollback.ExecuteNonQueryAsync(ct);
            }

            await using (var dropColumn = new NpgsqlCommand(
                "ALTER TABLE collect.ag_replica_states DROP COLUMN IF EXISTS group_id", connection))
            {
                await dropColumn.ExecuteNonQueryAsync(ct);
            }

            await DropArtifactsOfLaterRungsAsync(connection, 150, ct);

            var version = await ProbedVersionAsync(connection, ct);
            Assert.Equal(150, version);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>
    /// A TimescaleDB store fully migrated to V151 (the index present, before V152's drop rung has run)
    /// maps to exactly 151 — the group index is still there, so the negative NOT EXISTS half of the V152
    /// sentinel is false.
    /// </summary>
    [Fact]
    public async Task ATimescaleDbStoreAtV151WithTheGroupIndexPresent_MapsToV151()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the V151 probe pin (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);

        var bodySucceeded = false;
        try
        {
            await PgMigrations.MigrateAsync(connection, ct);

            var timescaleEnabled = await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
            Assert.True(timescaleEnabled, "TimescaleDB must be available on CI for the V151 probe pin");

            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);

            /* Roll back to V151, then create the rollups the V151-era way — default create_group_indexes,
               the group index still present — the same fixture shape CaggGroupIndexDropUpgradeLiveTests uses. */
            await using (var rollback = new NpgsqlCommand("DELETE FROM darling_schema_version WHERE version >= 152", connection))
            {
                await rollback.ExecuteNonQueryAsync(ct);
            }

            await DropArtifactsOfLaterRungsAsync(connection, 151, ct);

            var v151Shape = TimescaleSupport.CreateQueryStoreStatsHourlySql.Replace(
                ", timescaledb.create_group_indexes = false", string.Empty, StringComparison.Ordinal);
            await using (var create = new NpgsqlCommand(v151Shape, connection))
            {
                await create.ExecuteNonQueryAsync(ct);
            }

            var version = await ProbedVersionAsync(connection, ct);
            Assert.Equal(151, version);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>Removes the on-disk artifacts of every rung ABOVE <paramref name="simulatedVersion"/> so a
    /// version-stamp rollback (<c>DELETE FROM darling_schema_version WHERE version >= N + 1</c>) actually
    /// simulates an older store for the schema PROBE too, not just the version row — a later rung's own
    /// sentinel object would otherwise still be on disk and make the probe map past the simulated version.
    /// A future rung that adds a new probe sentinel must add its artifact's removal here.</summary>
    private static async Task DropArtifactsOfLaterRungsAsync(NpgsqlConnection connection, int simulatedVersion, CancellationToken ct)
    {
        if (simulatedVersion < 153)
        {
            /* V153 (#4608, split #4615) — the interval-tables-latest first_execution_time index. */
            await using var dropLatest = new NpgsqlCommand(
                "DROP INDEX IF EXISTS collect.idx_query_store_interval_latest_first_exec", connection);
            await dropLatest.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < 154)
        {
            /* V154 (#4608, split #4615) — the interval-tables-wide first_execution_time index. */
            await using var dropWide = new NpgsqlCommand(
                "DROP INDEX IF EXISTS collect.idx_query_store_interval_wide_first_exec", connection);
            await dropWide.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < 155)
        {
            /* V155 (#4765) - the interval end. The interval-wide table's column is the probe's sentinel; the
               stats table's column is also created by a fresh store's generated schema, so it is not one. */
            await using var dropEnd = new NpgsqlCommand(
                "ALTER TABLE collect.query_store_interval_wide DROP COLUMN IF EXISTS interval_end_time_utc", connection);
            await dropEnd.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < SlowReadsRungTests.RungVersion)
        {
            /* V162 (#5097) - the slow-read record; the table is the probe's sentinel. */
            await using var dropSlowReads = new NpgsqlCommand(
                "DROP TABLE IF EXISTS collect.slow_reads", connection);
            await dropSlowReads.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < QueryStoreTopDailyRungTests.RungVersion)
        {
            /* V161 (#5094) - the Query Store top daily summary tables; the first is the probe's sentinel. */
            await using var dropTopDaily = new NpgsqlCommand(
                "DROP TABLE IF EXISTS collect.query_store_top_daily, collect.query_store_top_daily_built", connection);
            await dropTopDaily.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < CollectorRunTimeRungTests.RungVersion)
        {
            /* V160 (#4938) - the collector run time, in a table of its own; the table is the probe's sentinel. */
            await using var dropRunAt = new NpgsqlCommand(
                "DROP TABLE IF EXISTS config.config_collector_run_times", connection);
            await dropRunAt.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < InstallIdTableOidRungTests.RungVersion)
        {
            /* V159 (#4961) - the install id table's own OID and the server's major version, which arrive in the one
               statement; the OID's column is the probe's sentinel. IF EXISTS on the table, because a store simulated below
               the install id's own rung has had the table dropped already. */
            await using var dropTableOid = new NpgsqlCommand(
                "ALTER TABLE IF EXISTS config.config_install_id DROP COLUMN IF EXISTS table_oid, DROP COLUMN IF EXISTS server_major", connection);
            await dropTableOid.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < InstallIdRungTests.RungVersion)
        {
            /* V158 (#4961) - the install id's one-row table; the table is the probe's sentinel. */
            await using var dropInstallId = new NpgsqlCommand(
                "DROP TABLE IF EXISTS config.config_install_id", connection);
            await dropInstallId.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < MuteRuleServerIdRungTests.RungVersion)
        {
            /* V157 - the mute rule's store server id; the column is the probe's sentinel. */
            await using var dropServerId = new NpgsqlCommand(
                "ALTER TABLE config.config_mute_rules DROP COLUMN IF EXISTS server_id", connection);
            await dropServerId.ExecuteNonQueryAsync(ct);
        }

        if (simulatedVersion < CheckpointLongestSyncRungTests.RungVersion)
        {
            /* V156 (#4834) - the hour's longest checkpoint sync. Both columns arrive in the one statement; the
               milliseconds column is the probe's sentinel. */
            await using var dropLongest = new NpgsqlCommand(
                "ALTER TABLE collect.store_metrics DROP COLUMN IF EXISTS checkpoint_longest_sync_ms, DROP COLUMN IF EXISTS checkpoint_longest_sync_at", connection);
            await dropLongest.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));

        var sentinels = new object[arity];
        for (var i = 0; i < arity; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)method.Invoke(null, sentinels)!;
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

    /// <summary>The set of columns carrying a two-key <c>(column, bucket)</c> group index on the given materialization.</summary>
    private static async Task<HashSet<string>> GroupIndexColumnsAsync(NpgsqlConnection connection, string schema, string table, CancellationToken ct)
    {
        var columns = new HashSet<string>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(@"
SELECT a1.attname
FROM pg_index i
JOIN pg_attribute a1 ON a1.attrelid = i.indrelid AND a1.attnum = i.indkey[0]
JOIN pg_attribute a2 ON a2.attrelid = i.indrelid AND a2.attnum = i.indkey[1]
WHERE i.indrelid = format('%I.%I', $1, $2)::regclass
AND   i.indnatts = 2
AND   a2.attname = 'bucket'", connection);
        command.Parameters.AddWithValue(schema);
        command.Parameters.AddWithValue(table);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static async Task<bool> ScalarBoolAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        var result = await command.ExecuteScalarAsync(ct);
        return result is bool value && value;
    }
}
