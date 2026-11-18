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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>
/// #8570: the readable-secondary facts read takes the newest <c>query_store_health</c> capture once, not once per part. It must
/// answer exactly what the read it replaced answered, on every shape of store a server can have (no capture, a capture of
/// databases with Query Store off, tempdb, secondary bits, collector runs older and newer than the capture), and it must reach
/// the health relation fewer times than the old read did.
/// </summary>
public sealed class QueryStoreReadableSecondaryFactsLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The read as it stood before #8570: the newest capture is looked up again by each of its four parts.</summary>
    private const string ReferenceFactsSql = @"
SELECT
    (SELECT sp.ag_replica_role FROM server_properties AS sp WHERE sp.server_id = $1 ORDER BY sp.collection_time DESC LIMIT 1),
    (SELECT COALESCE(NULLIF(btrim(c.query_store_replica), ''), NULL) FROM config.config_monitored_servers AS c WHERE c.server_id = $1),
    (SELECT COUNT(*) FROM v_query_store_health AS h
      WHERE h.server_id = $1 AND h.capture_time = (SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1)
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'),
    (SELECT COUNT(*) FROM v_query_store_health AS h
      WHERE h.server_id = $1 AND (h.readonly_reason & 8) <> 0
        AND h.capture_time = (SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1)
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'),
    run.fanout_item_count::bigint,
    run.log_id IS NOT NULL,
    hints.rows_collected::bigint,
    hints.age_seconds,
    hints.note
FROM (SELECT 1) AS one
LEFT JOIN LATERAL (
    SELECT cl.log_id, cl.fanout_item_count
    FROM collection_log AS cl
    WHERE cl.server_id = $1 AND cl.collector_name = 'query_store' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE((SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1), '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS run ON TRUE
LEFT JOIN LATERAL (
    SELECT cl.rows_collected, EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - cl.collection_time))::double precision AS age_seconds, cl.error_message AS note
    FROM collection_log AS cl
    WHERE cl.server_id = $1 AND cl.collector_name = 'query_store_hints' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE((SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1), '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS hints ON TRUE";

    /// <summary>The fleet read as it stood before #8570: the newest capture of every server is a grouping of every capture the store holds.</summary>
    private const string ReferenceFleetFactsSql = @"
WITH ids AS (SELECT unnest($1::integer[]) AS server_id),
role AS (
    SELECT DISTINCT ON (sp.server_id) sp.server_id, sp.ag_replica_role
    FROM server_properties AS sp
    JOIN ids ON ids.server_id = sp.server_id
    ORDER BY sp.server_id, sp.collection_time DESC),
newest AS (
    SELECT h.server_id, MAX(h.capture_time) AS capture_time
    FROM v_query_store_health AS h
    JOIN ids ON ids.server_id = h.server_id
    GROUP BY h.server_id),
counts AS (
    SELECT h.server_id,
           COUNT(*) AS health_databases,
           COUNT(*) FILTER (WHERE (h.readonly_reason & 8) <> 0) AS health_secondary_databases
    FROM v_query_store_health AS h
    JOIN newest AS n ON n.server_id = h.server_id AND n.capture_time = h.capture_time
    WHERE h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'
    GROUP BY h.server_id)
SELECT ids.server_id,
       role.ag_replica_role,
       EXISTS (SELECT 1 FROM config.config_monitored_servers AS c WHERE c.server_id = ids.server_id AND NULLIF(btrim(c.query_store_replica), '') IS NOT NULL) AS replica_configured,
       COALESCE(counts.health_databases, 0),
       COALESCE(counts.health_secondary_databases, 0),
       run.fanout_item_count::bigint,
       run.log_id IS NOT NULL,
       hints.rows_collected::bigint,
       hints.age_seconds,
       hints.note
FROM ids
LEFT JOIN role ON role.server_id = ids.server_id
LEFT JOIN newest AS n ON n.server_id = ids.server_id
LEFT JOIN counts ON counts.server_id = ids.server_id
LEFT JOIN LATERAL (
    SELECT cl.log_id, cl.fanout_item_count
    FROM collection_log AS cl
    WHERE cl.server_id = ids.server_id AND cl.collector_name = 'query_store' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE(n.capture_time, '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS run ON TRUE
LEFT JOIN LATERAL (
    SELECT cl.rows_collected, EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - cl.collection_time))::double precision AS age_seconds, cl.error_message AS note
    FROM collection_log AS cl
    WHERE cl.server_id = ids.server_id AND cl.collector_name = 'query_store_hints' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE(n.capture_time, '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS hints ON TRUE";

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] args)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var a in args)
        {
            cmd.Parameters.AddWithValue(a);
        }

        cmd.CommandTimeout = 600;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Seeds the shape the read runs against: a big server (<paramref name="bigDatabases"/> databases, one capture an hour for
    /// <paramref name="days"/> days, the collector's own cadence), three smaller servers, and the <c>query_store</c> and
    /// <c>query_store_hints</c> runs every five minutes over the same days beside <paramref name="noiseCollectors"/> other
    /// collectors' runs every minute on the big server. The newest capture on every server is the current hour. Every third
    /// database of the big server and every database of server 3 carries the readable-secondary bit, database 2 of each capture
    /// is OFF and database 1 is tempdb.
    /// </summary>
    internal static async Task SeedAsync(NpgsqlConnection c, int bigDatabases, int days, int noiseCollectors, CancellationToken ct)
    {
        await ExecAsync(c,
            """
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date)
            SELECT s, 'qs-facts-' || s, 'qs-facts-' || s, TRUE, $1, now()::timestamp, now()::timestamp FROM generate_series(1, 4) AS s
            """, ct, MonitoredEngineKind.SqlServer);

        await ExecAsync(c,
            """
            INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
                                            current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days,
                                            max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
            SELECT hr.h::bigint * 1000000 + sv.server_id * 100000 + db.n,
                   date_trunc('hour', now() AT TIME ZONE 'UTC') - make_interval(hours => hr.h),
                   sv.server_id, 'qs-facts-' || sv.server_id, db.name,
                   CASE WHEN db.n = 2 THEN 'OFF' WHEN db.secondary THEN 'READ_ONLY' ELSE 'READ_WRITE' END,
                   'READ_WRITE',
                   CASE WHEN db.secondary THEN 8 ELSE 0 END,
                   100, 1000, 'AUTO', 30, 200, 60, 'AUTO', 'ON'
            FROM (VALUES (1, $1::int), (2, 40), (3, 20), (4, 5)) AS sv(server_id, databases)
            CROSS JOIN generate_series(0, $2::int * 24 - 1) AS hr(h)
            CROSS JOIN LATERAL (
                SELECT g AS n,
                       CASE WHEN g = 1 THEN 'tempdb' ELSE 'db' || g END AS name,
                       g > 2 AND (sv.server_id = 3 OR (sv.server_id = 1 AND g % 3 = 0)) AS secondary
                FROM generate_series(1, sv.databases) AS g) AS db
            """, ct, bigDatabases, days);

        await ExecAsync(c,
            """
            INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, fanout_item_count)
            SELECT m.k::bigint * 1000000000 + sv.server_id * 100000000 + (extract(epoch FROM m.t)::bigint % 100000000),
                   sv.server_id, 'qs-facts-' || sv.server_id, m.name, m.t, 120,
                   CASE WHEN m.k = 3 AND m.x % 50 = 0 THEN 'ERROR' ELSE 'SUCCESS' END,
                   CASE WHEN m.k = 2 THEN 'hints note' END,
                   CASE WHEN m.k = 2 THEN 4 ELSE 0 END,
                   CASE WHEN m.k = 1 THEN 250 END
            FROM generate_series(1, 4) AS sv(server_id)
            CROSS JOIN LATERAL (
                SELECT 1 AS k, 'query_store' AS name, x, t FROM (SELECT x, date_trunc('minute', now() AT TIME ZONE 'UTC') - make_interval(mins => x * 5) AS t FROM generate_series(0, $1::int * 24 * 12 - 1) AS x) AS q
                UNION ALL
                SELECT 2, 'query_store_hints', x, t FROM (SELECT x, date_trunc('minute', now() AT TIME ZONE 'UTC') - make_interval(mins => x * 5) AS t FROM generate_series(0, $1::int * 24 * 12 - 1) AS x) AS q
                UNION ALL
                SELECT 10 + n, 'noise_' || n, x, t FROM generate_series(1, CASE WHEN sv.server_id = 1 THEN $2::int ELSE 0 END) AS n
                CROSS JOIN LATERAL (SELECT x, date_trunc('minute', now() AT TIME ZONE 'UTC') - make_interval(mins => x) AS t FROM generate_series(0, $1::int * 24 * 60 - 1) AS x) AS q
            ) AS m
            """, ct, days, noiseCollectors);

        await ExecAsync(c,
            """
            INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, ag_replica_role)
            SELECT x::bigint * 10 + sv.server_id, date_trunc('hour', now() AT TIME ZONE 'UTC') - make_interval(hours => x), sv.server_id, 'qs-facts-' || sv.server_id,
                   CASE WHEN sv.server_id = 3 THEN 'SECONDARY' ELSE 'PRIMARY' END
            FROM generate_series(1, 4) AS sv(server_id)
            CROSS JOIN generate_series(0, $1::int * 24 - 1) AS x
            """, ct, days);

        await ExecAsync(c, "ANALYZE query_store_health; ANALYZE collection_log; ANALYZE server_properties;", ct);
    }

    private static async Task<List<object?[]>> ReadRowsAsync(NpgsqlConnection c, string sql, object parameter, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        cmd.Parameters.Add(new NpgsqlParameter { Value = parameter });
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        var rows = new List<object?[]>();
        while (await reader.ReadAsync(ct))
        {
            var row = new object?[reader.FieldCount];
            for (var i = 0; i < row.Length; i++)
            {
                row[i] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }

            rows.Add(row);
        }

        return rows;
    }

    private static void AssertSameRows(List<object?[]> expected, List<object?[]> actual, string what)
    {
        Assert.True(expected.Count == actual.Count, $"{what}: {expected.Count} rows expected, {actual.Count} read");
        for (var r = 0; r < expected.Count; r++)
        {
            for (var i = 0; i < expected[r].Length; i++)
            {
                var (e, a) = (expected[r][i], actual[r][i]);
                if (e is double ed && a is double ad)
                {
                    /* An age on the store's clock: the two reads run a moment apart. */
                    Assert.True(Math.Abs(ed - ad) < 30, $"{what}: row {r} column {i} is {ed} then {ad}");
                }
                else
                {
                    Assert.True(Equals(e, a), $"{what}: row {r} column {i} is {e ?? "NULL"} then {a ?? "NULL"}");
                }
            }
        }
    }

    /// <summary>Visits of the <c>query_store_health</c> relation in the plan of <paramref name="sql"/> run for server 1, each counted once per loop.</summary>
    private static async Task<long> HealthVisitsAsync(NpgsqlConnection c, string sql, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, c);
        cmd.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
        var json = (string)(await cmd.ExecuteScalarAsync(ct))!;
        using var doc = JsonDocument.Parse(json);

        static long Walk(JsonElement node)
        {
            long total = 0;
            if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "query_store_health")
            {
                total += node.TryGetProperty("Actual Loops", out var loops) ? loops.GetInt64() : 1;
            }

            if (node.TryGetProperty("Plans", out var plans))
            {
                foreach (var child in plans.EnumerateArray())
                {
                    total += Walk(child);
                }
            }

            return total;
        }

        return Walk(doc.RootElement[0].GetProperty("Plan"));
    }

    [Fact]
    public async Task TheFactsRead_AnswersWhatTheOldReadAnswered_OnEveryShapeOfStore_AndReadsTheCaptureOnce()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the readable-secondary facts read against a seeded store (each test mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var bodySucceeded = false;
        try
        {
            await SeedAsync(connection, bigDatabases: 30, days: 3, noiseCollectors: 2, ct);
            await ExecAsync(connection,
                """
                -- 5: a frozen capture (the collector stopped a day and a half ago) with runs on both sides of it
                INSERT INTO query_store_health SELECT config_id + 500000, capture_time, 5, 'qs-facts-5', database_name, actual_state, desired_state, readonly_reason,
                       current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query,
                       interval_length_minutes, query_capture_mode, wait_stats_capture_mode
                FROM query_store_health WHERE server_id = 2 AND capture_time < date_trunc('hour', now() AT TIME ZONE 'UTC') - INTERVAL '36 hours';
                INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, fanout_item_count)
                SELECT log_id + 700000000000, 5, 'qs-facts-5', collector_name, collection_time, duration_ms, status, error_message, rows_collected, fanout_item_count
                FROM collection_log WHERE server_id = 2 AND collector_name IN ('query_store', 'query_store_hints');
                -- 6: runs and no capture at all
                INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, fanout_item_count)
                SELECT log_id + 800000000000, 6, 'qs-facts-6', collector_name, collection_time, duration_ms, status, error_message, rows_collected, fanout_item_count
                FROM collection_log WHERE server_id = 2 AND collector_name IN ('query_store', 'query_store_hints');
                -- 7: a capture that holds only tempdb, a database with no state and a database whose Query Store is OFF
                INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
                                                current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query,
                                                interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
                VALUES (9000001, date_trunc('hour', now() AT TIME ZONE 'UTC'), 7, 'qs-facts-7', 'tempdb', 'READ_WRITE', 'READ_WRITE', 8, 1, 1, 'AUTO', 30, 200, 60, 'AUTO', 'ON'),
                       (9000002, date_trunc('hour', now() AT TIME ZONE 'UTC'), 7, 'qs-facts-7', 'off_db', 'OFF', 'READ_WRITE', 8, 1, 1, 'AUTO', 30, 200, 60, 'AUTO', 'ON'),
                       (9000003, date_trunc('hour', now() AT TIME ZONE 'UTC'), 7, 'qs-facts-7', 'unknown_state', NULL, 'READ_WRITE', NULL, 1, 1, 'AUTO', 30, 200, 60, 'AUTO', 'ON');
                -- 8: a capture newer than every run, and a failed run of the other collector
                INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
                                                current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query,
                                                interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
                VALUES (9000004, date_trunc('hour', now() AT TIME ZONE 'UTC'), 8, 'qs-facts-8', 'db1', 'READ_ONLY', 'READ_WRITE', 8, 1, 1, 'AUTO', 30, 200, 60, 'AUTO', 'ON');
                INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, fanout_item_count)
                VALUES (9100001, 8, 'qs-facts-8', 'query_store', (now() AT TIME ZONE 'UTC') - INTERVAL '2 hours', 1, 'SUCCESS', NULL, 0, 3),
                       (9100002, 8, 'qs-facts-8', 'query_store_hints', (now() AT TIME ZONE 'UTC') - INTERVAL '1 minute', 1, 'ERROR', 'failed', NULL, NULL);
                ANALYZE query_store_health; ANALYZE collection_log;
                """, ct);

            /* Server 999 is not in the store at all. */
            var ids = new[] { 1, 2, 3, 4, 5, 6, 7, 8, 999 };
            foreach (var serverId in ids)
            {
                var expected = await ReadRowsAsync(connection, ReferenceFactsSql, serverId, ct);
                var actual = await ReadRowsAsync(connection, DarlingQueryStoreReadableSecondary.FactsSql, serverId, ct);
                Assert.Single(expected);
                AssertSameRows(expected, actual, $"facts for server {serverId}");
            }

            var big = await ReadRowsAsync(connection, DarlingQueryStoreReadableSecondary.FactsSql, 1, ct);
            Assert.True((long)big[0][2]! > 0 && (long)big[0][3]! > 0, "the seeded big server has databases and readable secondaries in its newest capture");
            var tempdbOnly = await ReadRowsAsync(connection, DarlingQueryStoreReadableSecondary.FactsSql, 7, ct);
            Assert.Equal(1L, tempdbOnly[0][2]);

            var expectedFleet = await ReadRowsAsync(connection, ReferenceFleetFactsSql, ids, ct);
            var actualFleet = await ReadRowsAsync(connection, DarlingQueryStoreReadableSecondary.FleetFactsSql, ids, ct);
            Assert.Equal(ids.Length, actualFleet.Count);
            AssertSameRows(expectedFleet.OrderBy(r => (int)r[0]!).ToList(), actualFleet.OrderBy(r => (int)r[0]!).ToList(), "fleet facts");

            var oldVisits = await HealthVisitsAsync(connection, ReferenceFactsSql, ct);
            var newVisits = await HealthVisitsAsync(connection, DarlingQueryStoreReadableSecondary.FactsSql, ct);
            Assert.True(newVisits * 2 <= oldVisits, $"the facts read reaches query_store_health {newVisits} times, the read it replaced {oldVisits}: the newest capture is found once, not once per part");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }
}
