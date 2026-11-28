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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch databases through ScratchPostgres and never touches another test's rows. */

/// <summary>
/// #9149 (V284): the covering hash indexes under <c>get_query_fleet_summary</c>. The reply must not change when the indexes are there (ties and
/// a NULL database_name pair included), the statements must read fewer pages with them, and the migration must be idempotent and replace
/// V246's narrow Query Store index in place.
/// </summary>
public sealed class QueryFleetSummaryCoveringIndexLiveTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9149 covering index tests.";
    private const string Hash = "0xCOV9149";
    private const string Other = "0xOTHER9149";
    private const int ServerCount = 8;
    private const int Groups = 40;
    private static readonly string[] CoveringIndexNames =
    [
        TimescaleSupport.QueryStatsIntervalHourlyHashCoveringIndexName,
        TimescaleSupport.QueryStatsHourlyHashCoveringIndexName,
        TimescaleSupport.QueryStoreCorrectedHourlyHashIndexName,
    ];

    private sealed record Store(ScratchPostgres Scratch, NpgsqlConnection Connection);

    private sealed record Timeline(DateTime End, int HoursBack)
    {
        public DateTime Start => End.AddHours(-HoursBack);
    }

    private static Timeline NewTimeline()
    {
        var now = LiveClock.Now();
        return new Timeline(new DateTime(now.Year, now.Month, now.Day, now.Hour, 0, 0, DateTimeKind.Unspecified), 168);
    }

    private static async Task Exec(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<Store> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The live #9149 tests need TimescaleDB.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            Assert.True(await TimescaleSupport.EnsureCollectionLogHypertableAsync(connection, null, ct));
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            await Exec(connection, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            return new Store(scratch, connection);
        }
        catch
        {
            await connection.DisposeAsync();
            await scratch.DisposeAsync();
            throw;
        }
    }

    private static async Task RefreshAsync(NpgsqlConnection connection, string view, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(from, DateTimeKind.Unspecified));
        refresh.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(to, DateTimeKind.Unspecified));
        await refresh.ExecuteNonQueryAsync(ct);
    }

    /// <summary>No autovacuum or autoanalyze on the relations the pages are counted on, so the page counts do not move with a background pass (#8725).
    /// The test vacuums the rollups itself once they are built: an index-only scan needs the visibility map, which a vacuum sets.</summary>
    private static async Task DisableAutovacuumAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var tables = new List<string>();
        await using (var relations = new NpgsqlCommand(@"SELECT 'collect.query_stats' UNION ALL SELECT 'collect.query_store_stats'
            UNION ALL SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name) FROM timescaledb_information.continuous_aggregates", c))
        await using (var reader = await relations.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                tables.Add(reader.GetString(0));
            }
        }

        foreach (var table in tables)
        {
            await Exec(c, $"ALTER TABLE {table} SET (autovacuum_enabled = false)", ct);
        }
    }

    /// <summary>Puts the rows of the three rollups in random physical order. A refresh writes a rollup in its aggregate's output order, and the order the
    /// planner picks differs between PostgreSQL versions: PostgreSQL 17 sorts by the hash and writes the statement's rows side by side, about seven to a
    /// heap page, where 18 spreads them. The statement then reads 192 pages on 17, below the 250 the assertion below needs (#9943). A shuffle puts the
    /// hash's rows on about a page each on both, the shape the page counts below are asserted on.</summary>
    private static async Task ScatterRollupsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var chunks = new List<string>();
        await using (var relations = new NpgsqlCommand(@"SELECT format('%I.%I', ch.chunk_schema, ch.chunk_name)
            FROM timescaledb_information.continuous_aggregates ca
            JOIN timescaledb_information.chunks ch ON ch.hypertable_schema = ca.materialization_hypertable_schema AND ch.hypertable_name = ca.materialization_hypertable_name
            WHERE ca.view_name IN ('query_stats_interval_hourly', 'query_stats_hourly', 'query_store_stats_corrected_hourly')", c))
        await using (var reader = await relations.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                chunks.Add(reader.GetString(0));
            }
        }

        foreach (var chunk in chunks)
        {
            await Exec(c, $"CREATE TEMP TABLE pm9943_scatter AS SELECT * FROM {chunk} ORDER BY random()", ct);
            await Exec(c, $"TRUNCATE {chunk}", ct);
            await Exec(c, $"INSERT INTO {chunk} SELECT * FROM pm9943_scatter", ct);
            await Exec(c, "DROP TABLE pm9943_scatter", ct);
        }
    }

    private static async Task VacuumRollupsAsync(NpgsqlConnection c, CancellationToken ct)
    {
        var tables = new List<string>();
        await using (var relations = new NpgsqlCommand(@"SELECT format('%I.%I', materialization_hypertable_schema, materialization_hypertable_name)
            FROM timescaledb_information.continuous_aggregates WHERE view_name IN ('query_stats_interval_hourly', 'query_stats_hourly', 'query_store_stats_corrected_hourly')", c))
        await using (var reader = await relations.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                tables.Add(reader.GetString(0));
            }
        }

        foreach (var table in tables)
        {
            await Exec(c, $"VACUUM (ANALYZE) {table}", ct);
        }

        await Exec(c, "VACUUM (ANALYZE) collect.query_stats", ct);
        await Exec(c, "VACUUM (ANALYZE) collect.query_store_stats", ct);
    }

    /// <summary>The seed: <see cref="ServerCount"/> servers, <see cref="Groups"/> statements per server, every hour of the window, both sources. Statement 1
    /// is <see cref="Hash"/>, so its hourly rows sit one to a heap page among the rows of the other statements. Whole-number averages, so the Query Store
    /// weighted sums are exact. On top: a NULL database_name pair, two pairs that tie on CPU, and rows of another hash.</summary>
    private static async Task SeedAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
    {
        for (var i = 1; i <= ServerCount; i++)
        {
            await DarlingMcpTestData.RegisterServerAsync(c, -914900 - i, "c9149-" + i, ct);
        }

        /* #9491: the read window is hours_back back from now, so a read after the hour turns starts an hour later. Seeding from two hours inside keeps every row in both reads. */
        var from = t.Start.AddHours(2);
        var to = t.End.AddHours(-1);
        await Exec(c, $@"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
              delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
            SELECT 700000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', -914900 - s, 'c9149-' || s, 'db' || (g % 5),
              CASE WHEN g = 1 THEN '{Hash}' ELSE '0x' || g END, 'h' || g, 1 + (extract(hour FROM h.ts))::int % 5, 1000 + g, 2000 + g, 100, 1000, 3600
            FROM generate_series(TIMESTAMP '{from:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{to:yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
            CROSS JOIN generate_series(1, {ServerCount}) s CROSS JOIN generate_series(1, {Groups}) g", ct);
        await Exec(c, $@"INSERT INTO collect.query_store_stats
              (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash, query_id, plan_id, execution_type_desc, replica_role,
               runtime_stats_interval_id, interval_start_time_utc, first_execution_time, execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
            SELECT 900000000::bigint + row_number() OVER (), h.ts + interval '10 minutes', -914900 - s, 'c9149-' || s, 'db' || (g % 5), 'dbo.P',
              CASE WHEN g = 1 THEN '{Hash}' ELSE '0x' || g END, g, g, 'Regular', 'PRIMARY', g, h.ts, h.ts, 1 + (extract(hour FROM h.ts))::int % 5, 2000 + g, 1000 + g, 900, 400, h.ts + interval '1 hour'
            FROM generate_series(TIMESTAMP '{from:yyyy-MM-dd HH:mm:ss}', TIMESTAMP '{to:yyyy-MM-dd HH:mm:ss}', interval '1 hour') AS h(ts)
            CROSS JOIN generate_series(1, {ServerCount}) s CROSS JOIN generate_series(1, {Groups}) g", ct);

        /* A NULL database_name pair, two pairs that tie exactly (server 1, 'TieA' and 'TieB'), and a pair of another hash, in both sources. */
        var specials = new (string? Db, string Hash, long Executions, long Cpu)[]
        {
            (null, Hash, 7, 4_321), ("TieA", Hash, 10, 999_000_000), ("TieB", Hash, 10, 999_000_000), ("Elsewhere", Other, 3, 55),
        };
        var at = t.Start.AddDays(2).AddHours(7);
        var snapshotIndex = 0;
        foreach (var (db, hash, executions, cpu) in specials)
        {
            snapshotIndex++;
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                      delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, total_logical_reads, sample_interval_seconds)
                  VALUES ($1, $2, -914901, 'c9149-1', $3, $4, '0xHS', $5, $6, $6, 100, 1000, 3600)",
                CollectionIdGenerator.Next(), at.AddMinutes(20), (object?)db ?? DBNull.Value, hash, executions, cpu);
            await DarlingMcpTestData.ExecAsync(c, ct,
                @"INSERT INTO collect.query_store_stats
                      (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
                       query_id, plan_id, execution_type_desc, replica_role,
                       runtime_stats_interval_id, interval_start_time_utc, first_execution_time,
                       execution_count, avg_duration_us, avg_cpu_time_us, max_duration_us, max_cpu_time_us, interval_end_time_utc)
                  VALUES ($1, $2, -914901, 'c9149-1', $3, 'dbo.Proc', $4, $5, $5, 'Regular', 'PRIMARY', $5, $6, $6, $7, 2000, $8, 900, 400, $6 + interval '1 hour')",
                CollectionIdGenerator.Next(), at.AddMinutes(20), (object?)db ?? DBNull.Value, hash, 90_000L + snapshotIndex, at, executions, cpu);
        }
    }

    private static async Task RefreshAllAsync(NpgsqlConnection c, CancellationToken ct, Timeline t)
    {
        var from = t.Start.Date.AddDays(-1);
        foreach (var view in new[]
        {
            TimescaleSupport.QueryStatsIntervalHourlyView, TimescaleSupport.QueryStatsHourlyView,
            TimescaleSupport.QueryStoreStatsIntervalHourlyView, TimescaleSupport.QueryStoreStatsCorrectedHourlyView,
        })
        {
            await RefreshAsync(c, view, from, t.End, ct);
        }
    }

    /// <summary>True when the index of this name on the rollup's materialization has included columns.</summary>
    private static async Task<bool?> IsCoveringAsync(NpgsqlConnection c, string indexName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT i.indnatts > i.indnkeyatts FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class t ON t.oid = i.indrelid
              WHERE ic.relname = $1 AND t.relname LIKE '_materialized_hypertable_%'", c);
        command.Parameters.AddWithValue(indexName);
        return await command.ExecuteScalarAsync(ct) as bool?;
    }

    private static async Task<int> IndexCountAsync(NpgsqlConnection c, string indexNameLike, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"SELECT count(*)::int FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid JOIN pg_class t ON t.oid = i.indrelid
              WHERE ic.relname LIKE $1 AND t.relname LIKE '_materialized_hypertable_%'", c);
        command.Parameters.AddWithValue(indexNameLike);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The state before V284: the covering plan cache indexes gone and the Query Store index narrow again, built by V246's own statement.</summary>
    private static async Task DowngradeToNarrowAsync(NpgsqlConnection c, CancellationToken ct)
    {
        await Exec(c, @"DO $$ DECLARE v regclass; BEGIN
            FOR v IN SELECT c.oid::regclass FROM pg_class c WHERE c.relkind = 'i' AND c.relname IN
                ('ix_query_stats_interval_hourly_hash_cov', 'ix_query_stats_hourly_hash_cov', 'ix_qs_corrected_hourly_hash_bucket')
            LOOP EXECUTE format('DROP INDEX %s', v); END LOOP; END $$;", ct);
        await using var tx = await c.BeginTransactionAsync(ct);
        await Exec(c, TimescaleSupport.CreateQueryStoreCorrectedHourlyHashIndexSql, ct);
        await tx.CommitAsync(ct);
        foreach (var name in CoveringIndexNames)
        {
            Assert.NotEqual(true, await IsCoveringAsync(c, name, ct));
        }
    }

    private static async Task RunV284Async(NpgsqlConnection c, CancellationToken ct)
    {
        var sql = PgMigrations.Scripts.Single(m => m.Version == 284).Sql;
        await using var tx = await c.BeginTransactionAsync(ct);
        await Exec(c, sql, ct);
        await tx.CommitAsync(ct);
    }

    private static async Task<string> ServersOfReplyAsync(NpgsqlDataSource pg, Timeline t, CancellationToken ct)
    {
        var json = await DarlingMcpQueryFleetSummaryTools.GetQueryFleetSummary(pg, Hash, t.HoursBack, limit: 200, cancellationToken: ct);
        using var doc = JsonDocument.Parse(json);
        Assert.False(doc.RootElement.TryGetProperty("error", out _), json);
        return doc.RootElement.GetProperty("servers").GetRawText();
    }

    private static void AddParameters(NpgsqlCommand command, Timeline t, DateTime rawFrom)
    {
        command.Parameters.AddWithValue(NpgsqlDbType.Text, Hash);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.Start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(t.End, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(rawFrom, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 200);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Integer, DBNull.Value);
    }

    /// <summary>The data pages the one-relation plan cache and Query Store statements read over the whole window, as the fleet summary runs them
    /// when no daily tier serves the days between the edges.</summary>
    private static async Task<(long PlanCache, long QueryStore)> PagesAsync(NpgsqlDataSource pg, Timeline t, CancellationToken ct)
    {
        var (_, coverage) = await ComposeStoreAvailability.GetRollupsAsync(pg, ct);
        var hourlyRelation = coverage.StitchedRelationSql(TimescaleSupport.QueryStatsHourlyView, "f", t.Start, RollupCoverage.StitchTier.Hourly);
        var ceiling = coverage.HourlyEndCeiling(TimescaleSupport.QueryStatsHourlyView, t.Start);
        Assert.NotNull(ceiling);
        var planCacheSql = DarlingMcpQueryFleetSummaryTools.BuildSql(hourlyRelation, RollupCoverage.HourlyCeilingClause(4));
        var route = await QueryStoreTrendRouting.ResolveAsync(coverage, pg, ct);
        Assert.True(route.UseRollup);
        var queryStoreSql = DarlingMcpQueryFleetSummaryTools.BuildQueryStoreSql(true);

        var planCache = await ExplainPagesAsync(pg, planCacheSql, t, ceiling!.Value > t.Start ? ceiling.Value : t.Start, ct);
        var queryStore = await ExplainPagesAsync(pg, queryStoreSql, t, route.RawStartUtc, ct);
        return (planCache, queryStore);
    }

    private static async Task<long> ExplainPagesAsync(NpgsqlDataSource pg, string sql, Timeline t, DateTime rawFrom, CancellationToken ct)
    {
        await using var command = pg.CreateCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql);
        AddParameters(command, t, rawFrom);
        var json = await command.ExecuteScalarAsync(ct);
        return ExplainBuffers.DataPages(json);
    }

    /// <summary>The same reply with and without the covering indexes, a NULL database_name pair and two exact ties included, and the statements read fewer
    /// pages with them. Red when V284's indexes are not built: with and without are then the same store.</summary>
    [Fact]
    public async Task CoveringIndexes_LeaveTheReplyAlone_AndReadFewerPages()
    {
        var ct = TestContext.Current.CancellationToken;
        var t = NewTimeline();
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            await DisableAutovacuumAsync(c, ct);
            await SeedAsync(c, ct, t);
            await RefreshAllAsync(c, ct, t);
            await Exec(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
            foreach (var name in CoveringIndexNames)
            {
                Assert.True(await IsCoveringAsync(c, name, ct), name + " should be covering on a fresh store");
            }

            await ScatterRollupsAsync(c, ct);
            await VacuumRollupsAsync(c, ct);
            await using var pg = NpgsqlDataSource.Create(scratch.ConnectionString);

            var withReply = await ServersOfReplyAsync(pg, t, ct);
            var withPages = await PagesAsync(pg, t, ct);
            using (var parsed = JsonDocument.Parse(withReply))
            {
                var rows = parsed.RootElement.EnumerateArray().ToArray();
                Assert.Contains(rows, r => r.GetProperty("database_name").ValueKind == JsonValueKind.Null || r.GetProperty("database_name").GetString() == "");
                Assert.Equal(4, rows.Count(r => r.GetProperty("database_name").GetString() is "TieA" or "TieB"));
            }

            await DowngradeToNarrowAsync(c, ct);
            await VacuumRollupsAsync(c, ct);
            var withoutReply = await ServersOfReplyAsync(pg, t, ct);
            var withoutPages = await PagesAsync(pg, t, ct);

            TestContext.Current.SendDiagnosticMessage($"pages plan cache {withoutPages.PlanCache} -> {withPages.PlanCache}, Query Store {withoutPages.QueryStore} -> {withPages.QueryStore}");
            Assert.Equal(withoutReply, withReply);
            Assert.True(withoutPages.PlanCache > 250, $"the seed must put the hash on about a page per hourly row: the plan cache statement read {withoutPages.PlanCache} pages");
            Assert.True(withoutPages.QueryStore > 100, $"the seed must put the hash on about a page per hourly row: the Query Store statement read {withoutPages.QueryStore} pages");
            Assert.True(withPages.PlanCache * 2 < withoutPages.PlanCache, $"plan cache: {withoutPages.PlanCache} pages without the covering index, {withPages.PlanCache} with it");
            Assert.True(withPages.QueryStore * 2 < withoutPages.QueryStore, $"Query Store: {withoutPages.QueryStore} pages without the covering index, {withPages.QueryStore} with it");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>V284 on a store that has V246's narrow Query Store index and no plan cache covering index: all three indexes end covering, the narrow index is
    /// gone (one index of that name, no temporary one left), and a second run changes nothing.</summary>
    [Fact]
    public async Task TheMigration_ReplacesTheNarrowIndex_BuildsTheCoveringOnes_AndRunsTwice()
    {
        var ct = TestContext.Current.CancellationToken;
        var store = await CreateStoreAsync(ct);
        await using var scratch = store.Scratch;
        await using var c = store.Connection;
        var bodySucceeded = false;
        try
        {
            await DowngradeToNarrowAsync(c, ct);
            Assert.Equal(false, await IsCoveringAsync(c, TimescaleSupport.QueryStoreCorrectedHourlyHashIndexName, ct));
            Assert.Null(await IsCoveringAsync(c, TimescaleSupport.QueryStatsIntervalHourlyHashCoveringIndexName, ct));

            for (var run = 1; run <= 2; run++)
            {
                await RunV284Async(c, ct);
                foreach (var name in CoveringIndexNames)
                {
                    Assert.True(await IsCoveringAsync(c, name, ct), $"{name} after run {run}");
                    Assert.Equal(1, await IndexCountAsync(c, name, ct));
                }

                Assert.Equal(0, await IndexCountAsync(c, "%\\_new", ct));
            }

            /* The Query Store index is still the one the convergence pass and a fresh store build, so a settled store finds it covering and builds nothing. */
            var convergence = TimescaleSupport.QueryStoreRollupKeptIndexesSql[TimescaleSupport.QueryStoreStatsCorrectedHourlyView];
            await using (var tx = await c.BeginTransactionAsync(ct))
            {
                await Exec(c, convergence, ct);
                await tx.CommitAsync(ct);
            }

            Assert.True(await IsCoveringAsync(c, TimescaleSupport.QueryStoreCorrectedHourlyHashIndexName, ct));
            Assert.Equal(1, await IndexCountAsync(c, TimescaleSupport.QueryStoreCorrectedHourlyHashIndexName, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, (_, _) => Task.CompletedTask);
        }
    }
}
