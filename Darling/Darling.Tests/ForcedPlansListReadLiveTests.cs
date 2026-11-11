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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Darling.Tests;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres, so its seed is the only data the plan sees. */

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #8570: the force list of <c>get_forced_plans</c> (<see cref="DarlingForcedPlanStateReader.ForcesSql(DatabaseFilter, int)"/>) built its newest-row-per-force step by
/// sorting every capture row of the window, then sorted the force-by-marker pairs again for each force's first sighting (440,000 capture rows of 24 hours: 28 MB
/// and 14 MB written to disk, 2,300 ms, against the count read's 120 ms after the earlier fix). The test seeds forces of four shapes (present throughout, started
/// mid-window, with a gap in the middle, removed before the newest capture) over two databases and holds the read to the rows the old statement returned,
/// including each force's first sighting, and to the sort space its plan reports.
/// </summary>
public sealed class ForcedPlansListReadLiveTests
{
    private const int Forces = 40;
    private const int Captures = 400;

    /// <summary>The statement as it was before #8570, kept here only so the new read is held to its results and the seed to its shape.</summary>
    private static readonly string OldSql = $@"
WITH win AS (
    SELECT database_name, collection_time, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason,
           variant_plan_ids, plan_guide_name, plan_guide_enabled, plan_guide_scope
    FROM collect.query_store_forced_plans
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3{DatabaseFilter.All.Clause("database_name", 4)}),
plans AS (
    SELECT * FROM win WHERE plan_id IS NOT NULL AND ($5::bigint IS NULL OR query_id = $5) AND ($6::bigint IS NULL OR plan_id = $6)),
newest AS (
    SELECT database_name, MAX(collection_time) AS newest_capture FROM win WHERE plan_id IS NULL GROUP BY database_name),
latest AS (
    SELECT DISTINCT ON (database_name, plan_id) database_name, plan_id, query_id, plan_forcing_type, force_failure_count, last_force_failure_reason, collection_time,
           variant_plan_ids, plan_guide_name, plan_guide_enabled, plan_guide_scope
    FROM plans ORDER BY database_name, plan_id, collection_time DESC),
absent AS (
    SELECT p.database_name, p.plan_id, MAX(m.collection_time) AS last_absent
    FROM (SELECT DISTINCT database_name, plan_id FROM plans) AS p
    JOIN win AS m ON m.database_name = p.database_name AND m.plan_id IS NULL
    WHERE NOT EXISTS (SELECT 1 FROM plans AS x WHERE x.database_name = p.database_name AND x.plan_id = p.plan_id AND x.collection_time = m.collection_time)
    GROUP BY p.database_name, p.plan_id),
firsts AS (
    SELECT x.database_name, x.plan_id, MIN(x.collection_time) AS first_seen
    FROM plans AS x
    LEFT JOIN absent AS a ON a.database_name = x.database_name AND a.plan_id = x.plan_id
    WHERE a.last_absent IS NULL OR x.collection_time > a.last_absent
    GROUP BY x.database_name, x.plan_id)
SELECT l.database_name, l.plan_id, l.query_id, l.plan_forcing_type, l.force_failure_count, l.last_force_failure_reason, l.collection_time,
       f.first_seen,
       n.newest_capture,
       l.variant_plan_ids, l.plan_guide_name, l.plan_guide_enabled, l.plan_guide_scope
FROM latest AS l
LEFT JOIN firsts AS f ON f.database_name = l.database_name AND f.plan_id = l.plan_id
LEFT JOIN newest AS n ON n.database_name = l.database_name
ORDER BY CASE WHEN l.collection_time >= n.newest_capture AND (l.force_failure_count > 0 OR btrim(COALESCE(l.last_force_failure_reason, '')) <> '') THEN 0
              WHEN l.collection_time >= n.newest_capture THEN 1
              ELSE 2 END,
         l.database_name, l.plan_id
LIMIT 2001";

    [Fact]
    public Task TheForceList_ReturnsTheOldStatementsRows_AndSortsLittle() =>
        WithSeedAsync(async (connection, serverId, from, to, ct) =>
        {
            var expected = await RowsAsync(connection, OldSql, serverId, from, to, null, null, ct);
            var actual = await RowsAsync(connection, DarlingForcedPlanStateReader.ForcesSql(DatabaseFilter.All, 2000), serverId, from, to, null, null, ct);

            Assert.Equal(Forces, expected.Count);
            /* The same forces in the same order, each with its newest row, its first sighting and its database's newest capture. */
            Assert.Equal(expected, actual);
            /* A narrowed read (one query) agrees as well. */
            Assert.Equal(
                await RowsAsync(connection, OldSql, serverId, from, to, 5003L, null, ct),
                await RowsAsync(connection, DarlingForcedPlanStateReader.ForcesSql(DatabaseFilter.All, 2000), serverId, from, to, 5003L, null, ct));

            var oldSort = await SortKilobytesAsync(connection, OldSql, serverId, from, to, ct);
            var newSort = await SortKilobytesAsync(connection, DarlingForcedPlanStateReader.ForcesSql(DatabaseFilter.All, 2000), serverId, from, to, ct);
            Assert.True(oldSort > 800, $"the seed's old sort held only {oldSort} kB, so it does not show the defect");
            Assert.True(newSort < 250, $"the force list sorted {newSort} kB: it sorts the window's capture rows again (the old statement sorted {oldSort} kB)");
        });

    /// <summary>
    /// A force present in every capture from its first row on has that row as its first sighting, and the new read skips the absence step for it. Plan 1004
    /// is in all 400 captures, plan 1001 started 100 minutes ago and was never absent after, plan 1002 has a gap (so the absence step must still run for it:
    /// its first sighting is the capture after the gap) and plan 1003 was removed (no row after the last capture it missed, so no first sighting). The
    /// reply equals the old statement's for each. Query Store writes one row per plan per capture (<c>plan_id</c> is unique in a database's
    /// <c>sys.query_store_plan</c> and the collector takes one marker row per database per pass), so a force never has two rows in one capture and the
    /// count of marker matches over the force's rows is its count of captures.
    /// </summary>
    [Fact]
    public Task AForcePresentFromItsFirstRowOn_HasThatRowAsItsFirstSighting_AndTheReplyEqualsTheOldStatements() =>
        WithSeedAsync(async (connection, serverId, from, to, ct) =>
        {
            var expected = await RowsAsync(connection, OldSql, serverId, from, to, null, null, ct);
            var actual = await RowsAsync(connection, DarlingForcedPlanStateReader.ForcesSql(DatabaseFilter.All, 2000), serverId, from, to, null, null, ct);
            Assert.Equal(expected, actual);

            string FirstSeen(string planId)
            {
                var row = Assert.Single(actual, r => r.Split('|')[1] == planId);
                return row.Split('|')[7];
            }

            string At(int minutesBeforeNewestCapture) => Convert.ToString(to.AddMinutes(-1 - minutesBeforeNewestCapture), CultureInfo.InvariantCulture)!;

            Assert.Equal(At(Captures - 1), FirstSeen("1004"));
            Assert.Equal(At(100), FirstSeen("1001"));
            Assert.Equal(At(49), FirstSeen("1002"));
            Assert.Equal("<null>", FirstSeen("1003"));
        });

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, int serverId, DateTime from, DateTime to, long? queryId, long? planId, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, serverId, from, to, queryId, planId);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var cells = new string[reader.FieldCount];
            for (var i = 0; i < cells.Length; i++)
            {
                cells[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join("|", cells));
        }

        return rows;
    }

    private static async Task<double> SortKilobytesAsync(NpgsqlConnection connection, string sql, int serverId, DateTime from, DateTime to, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        AddParameters(command, serverId, from, to, null, null);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        return SortKilobytes(document.RootElement[0].GetProperty("Plan"));
    }

    private static double SortKilobytes(JsonElement node)
    {
        var total = node.TryGetProperty("Sort Space Used", out var used) ? used.GetDouble() : 0;
        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                total += SortKilobytes(child);
            }
        }

        return total;
    }

    private static void AddParameters(NpgsqlCommand command, int serverId, DateTime from, DateTime to, long? queryId, long? planId)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(from, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(to, DateTimeKind.Unspecified) });
        command.Parameters.Add(DatabaseFilter.All.Parameter());
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId.HasValue ? queryId.Value : DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = planId.HasValue ? planId.Value : DBNull.Value });
    }

    /// <summary>
    /// 40 forces over two databases (the first 20 in DbA), one capture a minute for 400 minutes, each capture a marker row per database. By plan number mod 4:
    /// 0 present in every capture, 1 first captured 100 minutes ago, 2 missing from the captures 50 to 60 minutes ago, 3 removed 20 minutes ago (its newest
    /// row is the capture before). Every seventh force has a failure count and reason, which moves it up the order.
    /// </summary>
    private static async Task WithSeedAsync(Func<NpgsqlConnection, int, DateTime, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live plan tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        const int serverId = 85701;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var succeeded = false;
        try
        {
            var to = LiveClock.Now();
            var from = to.AddHours(-24);
            await ExecAsync(connection, ct,
                @"INSERT INTO collect.query_store_forced_plans
                      (collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type,
                       force_failure_count, last_force_failure_reason)
                  SELECT $1 + p * 100000 + m, $2 - m * interval '1 minute', $3, 'a8570', CASE WHEN p <= 20 THEN 'DbA' ELSE 'DbB' END, 1000 + p, 5000 + p, '0xPH' || p, 'MANUAL',
                         (p % 7 = 0)::int, CASE WHEN p % 7 = 0 THEN 'GENERAL_FAILURE' END
                  FROM generate_series(1, $4) p CROSS JOIN generate_series(0, $5 - 1) m
                  WHERE CASE p % 4 WHEN 0 THEN true
                                   WHEN 1 THEN m <= 100
                                   WHEN 2 THEN m < 50 OR m > 60
                                   ELSE m >= 20 END",
                CollectionIdGenerator.Next(), to.AddMinutes(-1), serverId, Forces, Captures);
            await ExecAsync(connection, ct,
                @"INSERT INTO collect.query_store_forced_plans (collection_id, collection_time, server_id, server_name, database_name)
                  SELECT $1 + 9000000 + d * 100000 + m, $2 - m * interval '1 minute', $3, 'a8570', CASE WHEN d = 0 THEN 'DbA' ELSE 'DbB' END
                  FROM generate_series(0, 1) d CROSS JOIN generate_series(0, $4 - 1) m",
                CollectionIdGenerator.Next(), to.AddMinutes(-1), serverId, Captures);
            await ExecAsync(connection, ct, "ANALYZE collect.query_store_forced_plans");

            await body(connection, serverId, from, to, ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await ExecAsync(cleanupConnection, cleanupCt, "DELETE FROM collect.query_store_forced_plans WHERE server_id = 85701"));
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
