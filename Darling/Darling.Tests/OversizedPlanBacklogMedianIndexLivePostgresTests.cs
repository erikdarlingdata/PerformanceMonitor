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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8570: the median columns of <c>get_oversized_plan_backlog</c> are read through two indexes (rung V260) instead of by sorting
/// every row of every group. The rollup takes the median of a server's <c>observed_bytes</c> from
/// <c>idx_oversized_plan_backlog_server_bytes</c>, and the fleet census takes each collector's from
/// <c>idx_oversized_plan_backlog_collector_bytes</c>. The medians must stay the discrete percentile the aggregate returned, for odd and
/// even row counts, a single row and ties; the plans must read the indexes.
/// </summary>
public sealed class OversizedPlanBacklogMedianIndexLivePostgresTests
{
    private const int RungVersion = 260;

    private const string ServerIndex = "idx_oversized_plan_backlog_server_bytes";
    private const string CollectorIndex = "idx_oversized_plan_backlog_collector_bytes";

    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8570 backlog median tests (they mint their own scratch databases).";

    /// <summary>The fixed instant the last-day counts start at, so no read here depends on the clock.</summary>
    private static readonly DateTime LastDaySince = new(2026, 6, 1, 0, 0, 0, DateTimeKind.Unspecified);

    /// <summary>
    /// One server per row count: 1, 2, 3, 4, 7 and 10 rows (the even counts are where a median of an interpolating kind would differ from
    /// the discrete one), over two collectors, with tied sizes. Sizes come from the row number so every group's median is a known value.
    /// </summary>
    private const string SeedSql = @"
INSERT INTO collect.servers (server_id, server_name, is_enabled)
VALUES (5001, 'median-1', TRUE), (5002, 'median-2', TRUE), (5003, 'median-3', TRUE), (5004, 'median-4', TRUE), (5005, 'median-7', TRUE);

INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT s.server_id,
       CASE WHEN n % 3 = 0 THEN 'procedure_stats' ELSE 'query_stats' END,
       'PLAN_' || s.server_id || '_' || n, 'SQL_' || s.server_id || '_' || n, 0, 400, 'median_db', 'HASH_' || n,
       CASE WHEN n % 4 = 0 THEN 700000 ELSE 600000 + ((n * 7919) % 100000) END,
       TIMESTAMP '2026-06-01 12:00', TIMESTAMP '2026-06-01 12:00'
FROM (VALUES (5001, 1), (5002, 2), (5003, 3), (5004, 4), (5005, 7), (5006, 10)) AS s (server_id, cnt)
CROSS JOIN LATERAL generate_series(1, s.cnt) AS n;";

    private const string RollupOracleSql = @"
SELECT server_id, percentile_disc(0.5) WITHIN GROUP (ORDER BY observed_bytes)
FROM collect.oversized_plan_backlog
GROUP BY server_id
ORDER BY server_id;";

    private const string CensusOracleSql = @"
SELECT collector_name, count(DISTINCT server_id), percentile_disc(0.5) WITHIN GROUP (ORDER BY observed_bytes)
FROM collect.oversized_plan_backlog
GROUP BY collector_name
ORDER BY collector_name;";

    /// <summary>A store the ladder migrated has both indexes on their two columns, the first is the probe's sentinel for the rung, and
    /// running the rung again is harmless.</summary>
    [Fact]
    public async Task AMigratedStore_HasTheTwoMedianIndexes_AndTheProbeNamesTheirRung()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);

        Assert.Equal(new[] { "server_id", "observed_bytes" }, await KeyColumnsAsync(connection, ServerIndex, ct));
        Assert.Equal(new[] { "collector_name", "observed_bytes" }, await KeyColumnsAsync(connection, CollectorIndex, ct));
        // Newer rungs move the probe past this one: drop their sentinels so the probe names this rung.
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion, ct);
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        Assert.Null(await KeyColumnsAsync(connection, ServerIndex, ct));
        Assert.Null(await KeyColumnsAsync(connection, CollectorIndex, ct));
        Assert.Equal(RungVersion - 1, await ProbedVersionAsync(connection, ct));

        var rung = PgMigrations.Scripts.Single(m => m.Version == RungVersion).Sql;
        for (var run = 0; run < 2; run++)
        {
            await using var again = new NpgsqlCommand(rung, connection);
            await again.ExecuteNonQueryAsync(ct);
        }

        Assert.NotNull(await KeyColumnsAsync(connection, ServerIndex, ct));
        Assert.NotNull(await KeyColumnsAsync(connection, CollectorIndex, ct));
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));
    }

    /// <summary>The medians the reads return are the aggregate's discrete percentile, group for group, whether the read covers the fleet or one
    /// server, and with or without the indexes.</summary>
    [Fact]
    public async Task TheRollupAndTheCensus_ReturnTheDiscretePercentile_WithAndWithoutTheIndexes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);
        await ExecuteAsync(connection, SeedSql, ct);
        await ExecuteAsync(connection, "ANALYZE collect.oversized_plan_backlog", ct);
        await using var postgres = NpgsqlDataSource.Create(connection.ConnectionString);

        for (var pass = 0; pass < 2; pass++)
        {
            if (pass == 1)
            {
                await ExecuteAsync(connection, $"DROP INDEX collect.{ServerIndex}; DROP INDEX collect.{CollectorIndex}", ct);
            }

            var oracle = await ReadPairsAsync(connection, RollupOracleSql, ct);
            Assert.Equal(6, oracle.Count);

            var fleet = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, null, LastDaySince, ct);
            Assert.Equal(oracle.Select(o => (int)o.Key).ToArray(), fleet.Select(r => r.ServerId).OrderBy(id => id).ToArray());
            foreach (var row in fleet)
            {
                Assert.Equal(oracle[row.ServerId], row.MedianObservedBytes);
            }

            foreach (var server in oracle.Keys)
            {
                var one = await DarlingOversizedPlanBacklogReader.GetPerServerRollupAsync(postgres, (int)server, LastDaySince, ct);
                var only = Assert.Single(one);
                Assert.Equal(oracle[server], only.MedianObservedBytes);
                Assert.Equal(fleet.Single(r => r.ServerId == server).TotalRows, only.TotalRows);
            }

            var censusOracle = await ReadCensusOracleAsync(connection, ct);
            var census = await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, null, ct);
            Assert.Equal(censusOracle.Keys.OrderBy(k => k, StringComparer.Ordinal).ToArray(), census.Select(c => c.CollectorName).OrderBy(k => k, StringComparer.Ordinal).ToArray());
            foreach (var row in census)
            {
                Assert.Equal(censusOracle[row.CollectorName].Servers, row.Servers);
                Assert.Equal(censusOracle[row.CollectorName].Median, row.MedianObservedBytes);
            }

            var forOne = await DarlingOversizedPlanBacklogReader.GetCollectorCensusAsync(postgres, 5005, ct);
            Assert.NotEmpty(forOne);
            Assert.All(forOne, c => Assert.Equal(1, c.Servers));
            Assert.Equal(7, forOne.Sum(c => c.TotalRows));
        }
    }

    /// <summary>
    /// 120,000 rows over three servers: the rollup reads each server's median from the server index, and the fleet census reads each
    /// collector's from the collector index, with no sort of the table's rows for either (the old statements sorted every row and no index
    /// could serve them).
    /// </summary>
    [Fact]
    public async Task TheRollupAndTheFleetCensus_ReadTheirMedianThroughTheIndexes()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);
        await ExecuteAsync(connection, @"
INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (5101, 'plan-1', TRUE), (5102, 'plan-2', TRUE), (5103, 'plan-3', TRUE);
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
SELECT 5101 + (n % 3), CASE WHEN n % 5 = 0 THEN 'procedure_stats' ELSE 'query_stats' END,
       md5('p' || n) || md5('q' || n), md5('s' || n) || md5('t' || n), 0, 400, 'plan_db', 'H' || (n % 500),
       2000000 + (n * 7919) % 60000000, TIMESTAMP '2026-06-01 12:00', TIMESTAMP '2026-06-01 12:00'
FROM generate_series(1, 120000) AS n;", ct);
        await ExecuteAsync(connection, "VACUUM (ANALYZE) collect.oversized_plan_backlog", ct);

        var rollup = await PlanNodesAsync(connection, DarlingOversizedPlanBacklogReader.PerServerRollupSql, withLastDay: true, ct);
        Assert.Contains(rollup, n => n.NodeType == "Index Only Scan" && n.IndexName == ServerIndex);

        var census = await PlanNodesAsync(connection, DarlingOversizedPlanBacklogReader.CollectorCensusSql, withLastDay: false, ct);
        Assert.Contains(census, n => n.NodeType == "Index Only Scan" && n.IndexName == CollectorIndex);
    }

    private sealed record PlanNode(string NodeType, string? IndexName);

    private static async Task<List<PlanNode>> PlanNodesAsync(NpgsqlConnection connection, string sql, bool withLastDay, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection);
        if (withLastDay)
        {
            command.Parameters.AddWithValue(NpgsqlDbType.Integer, DBNull.Value);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, LastDaySince);
            command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DarlingOversizedPlanBacklogReader.DayStartHour(LastDaySince));
        }

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var nodes = new List<PlanNode>();
        Walk(document.RootElement[0].GetProperty("Plan"), nodes);
        return nodes;
    }

    private static void Walk(JsonElement plan, List<PlanNode> nodes)
    {
        nodes.Add(new PlanNode(
            plan.GetProperty("Node Type").GetString()!,
            plan.TryGetProperty("Index Name", out var index) ? index.GetString() : null));
        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, nodes);
            }
        }
    }

    private static async Task<Dictionary<long, long>> ReadPairsAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        var result = new Dictionary<long, long>();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetInt32(0)] = reader.GetInt64(1);
        }

        return result;
    }

    private static async Task<Dictionary<string, (long Servers, long Median)>> ReadCensusOracleAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var result = new Dictionary<string, (long, long)>(StringComparer.Ordinal);
        await using var command = new NpgsqlCommand(CensusOracleSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result[reader.GetString(0)] = (reader.GetInt64(1), reader.GetInt64(2));
        }

        return result;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string[]?> KeyColumnsAsync(NpgsqlConnection connection, string indexName, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT ARRAY
(
    SELECT a.attname::text
    FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS k (attnum, ordinal)
    JOIN pg_attribute AS a
      ON  a.attrelid = i.indrelid
      AND a.attnum = k.attnum
    ORDER BY k.ordinal
)
FROM pg_index AS i
JOIN pg_class AS c
  ON c.oid = i.indexrelid
WHERE i.indrelid = 'collect.oversized_plan_backlog'::regclass
AND   c.relname = $1;", connection);
        command.Parameters.AddWithValue(indexName);
        return (string[]?)await command.ExecuteScalarAsync(ct);
    }

    private static async Task<NpgsqlConnection> MigratedConnectionAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var map = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var sentinels = new object[map.GetParameters().Length];

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(sentinels.Length, reader.FieldCount);
        for (var i = 0; i < sentinels.Length; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)map.Invoke(null, sentinels)!;
    }
}
