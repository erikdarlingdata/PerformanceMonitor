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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #7609 part B: the Query Store source of <c>get_query_fleet_summary</c> and the V246 index under it. The Query Store rows come
/// from <c>query_store_stats_corrected_hourly</c> below the rollup's ceiling and from the deduped raw rows above it, a window past
/// seven days answers the plan cache in full and says the Query Store part was cut, and the index exists on a fresh store and
/// after an upgrade from V245.
/// </summary>
public sealed class QueryFleetSummaryQueryStoreLiveTests
{
    private const string Hash = "0xQS7609";
    private const string IndexName = "ix_qs_corrected_hourly_hash_bucket";
    private static readonly (int Id, string Name)[] Servers = [(-760911, "b7609-one"), (-760912, "b7609-two")];

    private static async Task SeedSnapshotAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName, string db,
        long intervalId, long queryId, DateTime intervalStart, DateTime collected, long executions, long avgCpuUs = 1000, long avgDurationUs = 2000)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_stats
                  (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
                   query_id, plan_id, execution_type_desc, replica_role,
                   runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
                   execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
              VALUES ((extract(epoch FROM $1)::bigint * 100000) + $2, $1, $3, $4, $5, 'dbo.Proc', $9,
                      $6, $6, 'Regular', 'PRIMARY', $2, $7, $7, $8, $10, $11, 900, 400, $7 + interval '1 hour')",
            collected, intervalId, serverId, serverName, db, queryId, intervalStart, executions, Hash, avgDurationUs, avgCpuUs);
    }

    private static async Task InsertPlanCacheAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string serverName,
        string db, DateTime at, long executions, long cpuUs)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                  delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
              VALUES ($1, $2, $3, $4, $5, $6, '0xH1', $7, $8, $8, 100, 1000, 3600)",
            CollectionIdGenerator.Next(), at, serverId, serverName, db, Hash, executions, cpuUs);
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, CancellationToken ct, string view, DateTime from, DateTime to)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}', $1::timestamp, $2::timestamp)", connection);
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

    private static async Task<NpgsqlConnection> OpenTimescaleStoreAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var enabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!enabled, "This live test needs TimescaleDB.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
        await DarlingMcpTestData.ExecAsync(connection, ct, "SELECT _timescaledb_functions.stop_background_workers()");
        return connection;
    }

    private static async Task<bool> IndexExistsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_index i ON i.indexrelid = c.oid JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = i.indkey[0]
              WHERE c.relname = $1 AND a.attname = 'query_hash')", connection);
        command.Parameters.AddWithValue(IndexName);
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    [Fact]
    public async Task TheQueryStoreRows_CarryTheirSource_AreCutToSevenDays_AndNoHourIsCountedTwice()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live fleet summary Query Store test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);
        foreach (var (id, name) in Servers)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        foreach (var (view, _, _, _, _) in TimescaleSupport.RollupViews)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, $"SELECT remove_continuous_aggregate_policy('collect.{view}', if_exists => true)");
        }

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var thisHour = new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified);
            var rolled = thisHour.AddDays(-3);
            var old = thisHour.AddDays(-10);

            /* Rollup side. Server one/DbQ: one interval, 7 then 21 executions (the honest answer is 21, once). Server two/DbQ: 4. Ten days
               back, server two/DbOld: 8, which the seven-day cut leaves out. */
            await SeedSnapshotAsync(connection, ct, Servers[0].Id, Servers[0].Name, "DbQ", 7001, 1, rolled, rolled.AddMinutes(5), 7);
            await SeedSnapshotAsync(connection, ct, Servers[0].Id, Servers[0].Name, "DbQ", 7001, 1, rolled, rolled.AddMinutes(30), 21);
            await SeedSnapshotAsync(connection, ct, Servers[1].Id, Servers[1].Name, "DbQ", 7002, 2, rolled, rolled.AddMinutes(10), 4);
            await SeedSnapshotAsync(connection, ct, Servers[1].Id, Servers[1].Name, "DbOld", 7003, 3, old, old.AddMinutes(10), 8);
            foreach (var view in new[] { TimescaleSupport.QueryStoreStatsIntervalHourlyView, TimescaleSupport.QueryStoreStatsCorrectedHourlyView })
            {
                await RefreshAsync(connection, ct, view, old.AddHours(-1), rolled.AddHours(2));
            }

            /* A different interval that started in the rolled-up hour and reached the store after the refresh: that hour is the rollup's,
               so the raw side must not add these 999 again. */
            await SeedSnapshotAsync(connection, ct, Servers[0].Id, Servers[0].Name, "DbQ", 7004, 4, rolled.AddMinutes(40), rolled.AddMinutes(50), 999);

            /* Raw tail past the rollup's end: server one/DbQ 3 then 5 executions of one interval. */
            var recent = thisHour.AddHours(-2);
            await SeedSnapshotAsync(connection, ct, Servers[0].Id, Servers[0].Name, "DbQ", 7005, 5, recent, recent.AddMinutes(5), 3);
            await SeedSnapshotAsync(connection, ct, Servers[0].Id, Servers[0].Name, "DbQ", 7005, 5, recent, recent.AddMinutes(25), 5);

            /* The plan cache sees the same hash: a recent raw row on server one/DbQ, and one ten days back in the plan-cache rollup. */
            await InsertPlanCacheAsync(connection, ct, Servers[0].Id, Servers[0].Name, "DbQ", thisHour.AddHours(-1).AddMinutes(5), 2, 400_000);
            await InsertPlanCacheAsync(connection, ct, Servers[0].Id, Servers[0].Name, "PlanOld", old.AddMinutes(10), 6, 1_200_000);
            await RefreshAsync(connection, ct, TimescaleSupport.QueryStatsIntervalHourlyView, old.AddHours(-1), old.AddHours(2));
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collect.query_stats WHERE collection_time < $1", thisHour.AddDays(-1));

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            /* 90 days: the plan cache answers in full, the Query Store part is cut to seven days and says so. */
            var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 2160, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(json))
            {
                Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
                var rows = doc.RootElement.GetProperty("servers").EnumerateArray().ToArray();
                var qs = rows.Where(r => r.GetProperty("source").GetString() == "query_store").ToArray();
                var plan = rows.Where(r => r.GetProperty("source").GetString() == "plan_cache").ToArray();

                /* Rollup 21 + raw tail 5 = 26 executions on one/DbQ and 26 ms of CPU: the 999 in the rolled-up hour is not added, and the
                   raw tail's 3-then-5 is one interval counted once. */
                Assert.Equal(2, qs.Length);
                var oneQ = qs.Single(r => r.GetProperty("server_name").GetString() == "b7609-one");
                Assert.Equal("DbQ", oneQ.GetProperty("database_name").GetString());
                Assert.Equal(26, oneQ.GetProperty("execution_count").GetInt64());
                Assert.Equal(26.0, oneQ.GetProperty("cpu_ms").GetDouble());
                Assert.Equal(52.0, oneQ.GetProperty("elapsed_ms").GetDouble());
                Assert.StartsWith(rolled.ToString("yyyy-MM-ddTHH:00:00"), oneQ.GetProperty("first_hour").GetString());
                Assert.StartsWith(recent.ToString("yyyy-MM-ddTHH:00:00"), oneQ.GetProperty("last_hour").GetString());
                var twoQ = qs.Single(r => r.GetProperty("server_name").GetString() == "b7609-two");
                Assert.Equal(4, twoQ.GetProperty("execution_count").GetInt64());

                /* The ten-day-old Query Store pair is out of the Query Store part, the ten-day-old plan-cache pair is in the plan-cache part. */
                Assert.DoesNotContain(qs, r => r.GetProperty("database_name").GetString() == "DbOld");
                Assert.Equal(new[] { "DbQ", "PlanOld" }, plan.Select(r => r.GetProperty("database_name").GetString()!).OrderBy(x => x, StringComparer.Ordinal).ToArray());

                var qsPart = doc.RootElement.GetProperty("query_store");
                Assert.True(qsPart.GetProperty("cut_to_7_days").GetBoolean());
                Assert.Equal(168, qsPart.GetProperty("hours_read").GetInt32());
                Assert.Equal(2, qsPart.GetProperty("rows").GetInt32());
                Assert.Contains("cut to 168 hours", qsPart.GetProperty("note").GetString());
                Assert.Equal(4, doc.RootElement.GetProperty("rows_returned").GetInt32());
            }

            /* Seven days: no cut, no note, the same two Query Store rows, and the plan-cache rollup's old pair is gone. */
            var week = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 168, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(week))
            {
                var qsPart = doc.RootElement.GetProperty("query_store");
                Assert.False(qsPart.GetProperty("cut_to_7_days").GetBoolean());
                Assert.Equal(JsonValueKind.Null, qsPart.GetProperty("note").ValueKind);
                Assert.Equal(2, qsPart.GetProperty("rows").GetInt32());
                Assert.Equal(3, doc.RootElement.GetProperty("rows_returned").GetInt32());
            }

            /* The cap covers both sources: three rows of four are shown and the note says three of four. */
            var capped = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(postgres, Hash, 2160, limit: 3, cancellationToken: ct);
            using (var doc = JsonDocument.Parse(capped))
            {
                Assert.True(doc.RootElement.GetProperty("truncated").GetBoolean());
                Assert.Equal(4, doc.RootElement.GetProperty("server_database_pairs").GetInt32());
                Assert.Contains("3 of 4", doc.RootElement.GetProperty("note").GetString());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheKeptIndexStep_OnASettledStore_TakesNoLockOnTheMaterialization_AndStillBuildsAMissingIndex()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live kept-index lock test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            Assert.True(await IndexExistsAsync(connection, ct), "a fresh store has the (query_hash, bucket) index");

            string materialization;
            string materializationSchema;
            await using (var find = new NpgsqlCommand(
                @"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name), materialization_hypertable_schema
                  FROM timescaledb_information.continuous_aggregates
                  WHERE view_schema = 'collect' AND view_name = 'query_store_stats_corrected_hourly'", connection))
            {
                await using var found = await find.ExecuteReaderAsync(ct);
                Assert.True(await found.ReadAsync(ct));
                materialization = found.GetString(0);
                materializationSchema = found.GetString(1);
            }

            var keptIndexStep = TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedHourlyView];

            /* A session holding what the view's refresh holds: ROW EXCLUSIVE on the materialization. CREATE INDEX asks for SHARE, which conflicts. */
            await using var refresh = new NpgsqlConnection(scratch.ConnectionString);
            await refresh.OpenAsync(ct);
            await using var refreshTransaction = await refresh.BeginTransactionAsync(ct);
            await DarlingMcpTestData.ExecAsync(refresh, ct, $"LOCK TABLE {materialization} IN ROW EXCLUSIVE MODE");

            /* The premise: CREATE INDEX IF NOT EXISTS on the existing index still waits for the lock, even though it would build nothing. */
            await using (var premise = new NpgsqlConnection(scratch.ConnectionString))
            {
                await premise.OpenAsync(ct);
                await DarlingMcpTestData.ExecAsync(premise, ct, "SET lock_timeout = '500ms'");
                var blocked = await Assert.ThrowsAsync<PostgresException>(() => DarlingMcpTestData.ExecAsync(
                    premise, ct, $"CREATE INDEX IF NOT EXISTS {IndexName} ON {materialization} (query_hash, bucket DESC)"));
                Assert.Equal("55P03", blocked.SqlState);
            }

            /* The step on a settled store reads the catalog and asks for no lock: it returns at once instead of waiting out its own 280 s lock timeout. */
            await using (var step = new NpgsqlCommand(keptIndexStep, connection) { CommandTimeout = 20 })
            {
                await step.ExecuteNonQueryAsync(ct);
            }

            await refreshTransaction.RollbackAsync(ct);

            /* With the index missing, the same step builds it. */
            await DarlingMcpTestData.ExecAsync(connection, ct, $"DROP INDEX \"{materializationSchema}\".{IndexName}");
            Assert.False(await IndexExistsAsync(connection, ct));
            await using (var step = new NpgsqlCommand(keptIndexStep, connection) { CommandTimeout = 60 })
            {
                await step.ExecuteNonQueryAsync(ct);
            }

            Assert.True(await IndexExistsAsync(connection, ct), "the step builds a missing index");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task TheV246Index_ExistsOnAFreshStore_AndIsRebuiltByTheUpgradeFromV245()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live V246 index test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = await OpenTimescaleStoreAsync(scratch, ct);

        var bodySucceeded = false;
        try
        {
            /* A fresh store: the view is created, then its kept indexes, and the hash index is one of them. */
            Assert.False(await IndexExistsAsync(connection, ct), "no view yet, so no index");
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            Assert.True(await IndexExistsAsync(connection, ct), "a fresh store has the (query_hash, bucket) index");
            Assert.True(await ProbeSentinelAsync(connection, ct));

            /* A store at V245: the index and the stamp are gone, and V247's column with them (the probe's newest sentinel is V247's now, so a store
               at V245 has neither), the probe reads rung 245, and the migration puts V246 and V247 back. */
            await DarlingMcpTestData.ExecAsync(connection, ct, RungRewindArtifacts.Ladder.Single(r => r.Version == 247).DropSql!);
            await DarlingMcpTestData.ExecAsync(connection, ct, RungRewindArtifacts.Ladder.Single(r => r.Version == 246).DropSql!);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM darling_schema_version WHERE version >= 246");
            Assert.False(await IndexExistsAsync(connection, ct));
            Assert.False(await ProbeSentinelAsync(connection, ct));

            Assert.Equal(2, await PgMigrations.MigrateAsync(connection, ct));
            Assert.True(await IndexExistsAsync(connection, ct), "the V245 to V246 upgrade builds the index");
            Assert.True(await ProbeSentinelAsync(connection, ct));
            await using (var stamp = new NpgsqlCommand("SELECT max(version) FROM darling_schema_version", connection))
            {
                Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await stamp.ExecuteScalarAsync(ct)));
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>The Viewer's schema probe, the V246 sentinel: the column before the last, now that V247's sentinel (#7639) is the last.</summary>
    private static async Task<bool> ProbeSentinelAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        /* The V246 sentinel is no longer the last column: V247's (#7639) and V248's (#7731) follow it. */
        return reader.GetBoolean(reader.FieldCount - 3);
    }
}
