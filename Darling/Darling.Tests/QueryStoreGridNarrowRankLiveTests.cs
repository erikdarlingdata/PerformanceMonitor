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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8549: the Query Store by Duration grid's interval-table read groups and ranks on the narrow group key, then aggregates the wide
/// columns for the groups it kept. Grouped in one step, the sort carried every wide column of every interval row (about 450 bytes
/// each) and wrote gigabytes of temp files on a 30 day window. The test runs the read on a store of about 240,000 wide rows under a
/// temp file limit the one-step read exceeds, and checks the rows it returns against an independent grouping, including two heavy
/// groups that differ only in a NULL replica role.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreGridNarrowRankLiveTests
{
    private const int Groups = 60_000;
    private const int RowsPerGroup = 4;
    private const int Top = 50;
    private const int Candidates = 250;

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string[] Metrics = (
        "avg_duration_us min_duration_us max_duration_us avg_cpu_time_us min_cpu_time_us max_cpu_time_us avg_logical_io_reads min_logical_io_reads max_logical_io_reads "
        + "avg_logical_io_writes min_logical_io_writes max_logical_io_writes avg_physical_io_reads min_physical_io_reads max_physical_io_reads avg_clr_time_us min_clr_time_us max_clr_time_us "
        + "min_dop max_dop avg_query_max_used_memory min_query_max_used_memory max_query_max_used_memory avg_rowcount min_rowcount max_rowcount "
        + "avg_num_physical_io_reads min_num_physical_io_reads max_num_physical_io_reads avg_log_bytes_used min_log_bytes_used max_log_bytes_used "
        + "avg_tempdb_space_used min_tempdb_space_used max_tempdb_space_used").Split(' ');

    [Fact]
    public async Task TheGridTableRead_RanksOnTheNarrowKey_StaysUnderATempLimitTheWideSortExceeds_AndReturnsTheGroupsExactly()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var metricList = string.Join(", ", Metrics);
        var metricValues = string.Join(", ", Metrics.Select((m, i) => m.StartsWith("avg_duration", StringComparison.Ordinal)
            ? "((r.g * 7919 + r.n * 31) % 9000 + 1)::bigint * CASE WHEN r.g % 1000 = 0 THEN 1000 ELSE 1 END"
            : m.StartsWith("avg", StringComparison.Ordinal) ? $"((r.g * 7919 + r.n * 31 + {i}) % 9000 + 1)::bigint"
            : m.StartsWith("min", StringComparison.Ordinal) ? $"((r.g + {i}) % 50)::bigint" : $"((r.g + {i}) % 5000 + 100)::bigint"));

        /* Groups 1000, 2000, ... carry a heavy twin pair that differs only in replica_role: NULL, and 'Secondary'. */
        await ExecuteAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, query_hash,
 execution_count, {metricList}, plan_type, is_forced_plan, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT now() AT TIME ZONE 'utc' - interval '1 hour' + (r.n * interval '1 second'), 1, 'db' || (r.g % 5), r.g, r.g, 'Regular',
       now() AT TIME ZONE 'utc' - interval '2 hours' - (r.n * interval '1 minute'), now() AT TIME ZONE 'utc' - interval '1 hour', '0x' || md5(r.g::text),
       1 + (r.g * 13 + r.n) % 400, {metricValues}, 'Query', false, '0x' || md5((r.g + 1)::text), r.role, 1000 + r.n,
       now() AT TIME ZONE 'utc' - interval '3 hours' - (r.n * interval '1 minute')
FROM (SELECT g, n, CASE WHEN t = 1 THEN 'Secondary' END AS role
      FROM generate_series(1, {Groups}) AS g
      CROSS JOIN generate_series(1, {RowsPerGroup}) AS n
      CROSS JOIN generate_series(0, 1) AS t
      WHERE t = 0 OR g % 1000 = 0) AS r;
ANALYZE collect.query_store_interval_wide;", ct);

        /* A temp limit the one-step read's wide sort (about 240,000 rows of about 450 bytes) exceeds, and the narrow rows stay well under. */
        await ExecuteAsync(connection, "SET temp_file_limit = '64MB'; SET max_parallel_workers_per_gather = 0; SET work_mem = '4MB'", ct);

        await using var top = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableSql, connection) { CommandTimeout = 300 };
        top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
        top.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-30), DateTimeKind.Unspecified) });
        top.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DBNull.Value });
        top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        top.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        top.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Candidates });

        var read = new List<(long QueryId, string? Replica, long Executions, double AvgDurationMs, long Candidates)>();
        await using (var reader = await top.ExecuteReaderAsync(ct))
        {
            var replica = reader.GetOrdinal("replica_role");
            var candidates = reader.GetOrdinal("candidate_count");
            while (await reader.ReadAsync(ct))
            {
                read.Add((reader.GetInt64(reader.GetOrdinal("query_id")), reader.IsDBNull(replica) ? null : reader.GetString(replica),
                    reader.GetInt64(reader.GetOrdinal("total_executions")), Convert.ToDouble(reader.GetValue(reader.GetOrdinal("avg_duration_ms"))),
                    reader.GetInt64(candidates)));
            }
        }

        Assert.Equal(Top, read.Count);
        Assert.All(read, r => Assert.Equal((long)Candidates, r.Candidates));

        /* The independent grouping: the same ranking written in one step, the page ordered as the read's tail orders it. */
        var expected = new List<(long QueryId, string? Replica, long Executions, double AvgDurationMs)>();
        await using (var expectedCommand = new NpgsqlCommand(@"
SELECT query_id, replica_role, SUM(execution_count)::bigint AS total_executions,
       SUM(avg_duration_us::double precision * execution_count) / NULLIF(SUM(execution_count), 0) / 1000.0 AS avg_ms
FROM collect.query_store_interval_wide
WHERE server_id = 1
GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
ORDER BY SUM(avg_duration_us::double precision * execution_count) DESC, database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
LIMIT 250", connection) { CommandTimeout = 300 })
        await using (var reader = await expectedCommand.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                expected.Add((reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetInt64(2), Convert.ToDouble(reader.GetValue(3))));
            }
        }

        var expectedPage = expected
            .OrderByDescending(e => e.Executions * e.AvgDurationMs)
            .ThenBy(e => e.QueryId)
            .ThenBy(e => e.Replica is null ? 1 : 0)
            .ThenBy(e => e.Replica, StringComparer.Ordinal)
            .Take(Top)
            .ToList();
        Assert.Equal(expectedPage.Select(e => (e.QueryId, e.Replica, e.Executions)), read.Select(r => (r.QueryId, r.Replica, r.Executions)));
        for (var i = 0; i < Top; i++)
        {
            Assert.Equal(expectedPage[i].AvgDurationMs, read[i].AvgDurationMs, 6);
        }

        /* The heavy twins rank in the page as two groups, the NULL replica role and 'Secondary', never merged and never dropped. */
        Assert.Contains(read, r => r.QueryId == 1000 && r.Replica is null);
        Assert.Contains(read, r => r.QueryId == 1000 && r.Replica == "Secondary");
    }

    /// <summary>
    /// #8549: the grid's two-statement read (the kept keys first, then the rows of those keys bound as arrays, one Bitmap Heap Scan per daily
    /// leaf) returns exactly the rows of the one-statement read, on keys that stress the key match: a key with rows in several leaves, a
    /// key whose query_id also has a sibling plan that is not kept, a key with NULL database_name and query_id, two keys that differ only in
    /// replica_role (NULL and a value), and a row of a kept key from before the read start.
    /// </summary>
    [Fact]
    public async Task TheGridTwoStatementRead_ReturnsTheOneStatementReadsRows_OnKeysThatStressTheMatch()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #8549 live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var metricList = string.Join(", ", Metrics);
        var metricValues = string.Join(", ", Metrics.Select((m, i) => m.StartsWith("avg_duration", StringComparison.Ordinal)
            ? "CASE WHEN r.hot THEN 5000000 + r.n ELSE 100 + r.n END"
            : m.StartsWith("avg", StringComparison.Ordinal) ? $"((r.g * 7919 + r.n * 31 + {i}) % 9000 + 1)::bigint"
            : m.StartsWith("min", StringComparison.Ordinal) ? $"((r.g + {i}) % 50)::bigint" : $"((r.g + {i}) % 5000 + 100)::bigint"));

        /* Group g 1..40 are the hot keys (rows on four days, g 1 and 2 are replica twins of one plan: NULL and 'Secondary'), g 41..400 are
           cold. Group 0 has NULL database_name and query_id. Group 1000 is a sibling plan of query 1 that is too cold to be kept, and one
           row of group 1 is 60 days old, before the read start. */
        await ExecuteAsync(connection, $@"
INSERT INTO collect.query_store_interval_wide
(collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time, query_hash,
 execution_count, {metricList}, plan_type, is_forced_plan, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT t + interval '30 minutes', 1,
       CASE WHEN r.g = 0 THEN NULL ELSE 'db' || (r.g % 2) END,
       CASE WHEN r.g = 0 THEN NULL WHEN r.g = 1000 THEN 1 WHEN r.g = 2 THEN 1 ELSE r.g END,
       CASE WHEN r.g = 0 THEN 7 ELSE r.g END, 'Regular', t, t, '0x' || md5(r.g::text),
       1 + (r.g * 13 + r.n) % 400, {metricValues}, 'Query', false, '0x' || md5((r.g + 1)::text),
       CASE WHEN r.g = 2 THEN 'Secondary' END, r.g * 1000 + r.n, date_trunc('hour', t)
FROM (SELECT g, n, g BETWEEN 0 AND 40 AS hot,
             CASE WHEN g = 1 AND n = 9 THEN now() AT TIME ZONE 'utc' - interval '60 days'
                  ELSE now() AT TIME ZONE 'utc' - ((n % 4) * interval '1 day') - (n * interval '5 minutes') - interval '2 hours' END AS t
      FROM (SELECT g FROM generate_series(0, 400) AS g UNION ALL SELECT 1000) AS gs
      CROSS JOIN generate_series(1, 9) AS n) AS r;
ANALYZE collect.query_store_interval_wide;", ct);

        var readStart = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-30), DateTimeKind.Unspecified);
        const int top = 100;
        const int candidates = 45;

        void AddShared(NpgsqlCommand command)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
            command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = readStart });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = DBNull.Value });
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = top });
            command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(null));
        }

        async Task<List<string>> ReadAllAsync(NpgsqlCommand command)
        {
            var rows = new List<string>();
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => reader.IsDBNull(i) ? "~" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
            }

            return rows;
        }

        await using var oneStatement = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableSql, connection) { CommandTimeout = 300 };
        AddShared(oneStatement);
        oneStatement.Parameters.Add(new NpgsqlParameter<int> { TypedValue = candidates });
        var expected = await ReadAllAsync(oneStatement);

        var keys = new ViewerDataService.TableReadKeys();
        await using (var keysCommand = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableKeysSql, connection) { CommandTimeout = 300 })
        {
            AddShared(keysCommand);
            keysCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = candidates });
            await using var keyReader = await keysCommand.ExecuteReaderAsync(ct);
            while (await keyReader.ReadAsync(ct))
            {
                keys.Add(keyReader);
            }
        }

        await using var twoStatement = new NpgsqlCommand(ViewerDataService.QueryStoreTopTableByKeysSql, connection) { CommandTimeout = 300 };
        AddShared(twoStatement);
        keys.AddParameters(twoStatement);
        var actual = await ReadAllAsync(twoStatement);

        Assert.True(expected.Count > 40, $"the page holds the kept keys: {expected.Count}");
        Assert.Equal(expected, actual);

        /* The seed does stress the match: the NULL-key group and both replica twins are in the page, and the sibling plan is not. */
        Assert.Contains(actual, r => r.StartsWith("~|~|7|", StringComparison.Ordinal));
        Assert.DoesNotContain(actual, r => r.StartsWith("db0|1|1000|", StringComparison.Ordinal));
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync(ct);
    }
}
