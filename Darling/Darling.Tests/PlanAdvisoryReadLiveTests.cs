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
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres, so its seed is the only data the plan sees. */

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #8570: the plan fetch of <c>get_analysis_facts</c> (<see cref="PgFactCollector.PlanAdvisorySql"/>, and the drill-down's byte-equal twin) ranks every
/// in-window row of a server, and it read them through <c>v_query_stats</c>, whose join to <c>query_plan_dim</c> carried each row's plan content into
/// the rank's sort. A plan under the TOAST limit sits inline in the dimension row, so the sort held every snapshot row of every plan with the whole plan
/// (67,200 rows of 1.4 KB plans: 26 MB and 21 MB written to disk, 4.5 MB since). The test seeds that shape and pins two things by what the read does:
/// the sort holds thin rows (the sort space the plan reports), and the rows returned are exactly the rows the view-based statement returned.
/// </summary>
public sealed class PlanAdvisoryReadLiveTests
{
    private const int Keys = 120;
    private const int Snapshots = 10;

    /// <summary>The statement as it was before #8570, kept here only so the new read is held to its results and the seed is held to its shape.</summary>
    private const string ViewBasedSql = @"
SELECT query_plan_xml, query_plan_gz
FROM
(
    SELECT
        query_plan_xml,
        query_plan_gz,
        delta_worker_time,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_hash, COALESCE(query_plan_hash, plan_handle) ORDER BY delta_worker_time DESC) AS plan_rank
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   (query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)
) AS ranked
WHERE plan_rank = 1
ORDER BY delta_worker_time DESC
LIMIT 10";

    [Fact]
    public Task TheFetch_SortsThinRows_AndReturnsTheViewBasedStatementsRows() =>
        WithSeedAsync(async (connection, serverId, start, end, ct) =>
        {
            var expected = await RowsAsync(connection, ViewBasedSql, serverId, start, end, ct);
            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, serverId, start, end, ct);

            Assert.Equal(10, expected.Count);
            /* The same ten plans in the same order, content included (inline text, gzip bytes, and the legacy inline column). */
            Assert.Equal(expected, actual);
            Assert.Equal(PgFactCollector.PlanAdvisorySql, PgDrillDownCollector.PlanAdvisoryXmlSql);

            var oldSort = await SortKilobytesAsync(connection, ViewBasedSql, serverId, start, end, ct);
            var newSort = await SortKilobytesAsync(connection, PgFactCollector.PlanAdvisorySql, serverId, start, end, ct);

            /* The seed must have the shape that costs: the view-based sort carries the inline plans (about 0.9 MB here). */
            Assert.True(oldSort > 600, $"the seed's view-based sort held only {oldSort} kB, so it does not show the defect");
            Assert.True(newSort < 250, $"the plan fetch sorted {newSort} kB: it carries plan content through the rank (the view-based sort held {oldSort} kB)");
        });

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, int serverId, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, serverId, start, end);
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var text = reader.IsDBNull(0) ? "<null>" : reader.GetString(0);
            var gz = reader.IsDBNull(1) ? "<null>" : Convert.ToHexString(reader.GetFieldValue<byte[]>(1));
            rows.Add(text + "|" + gz);
        }

        return rows;
    }

    private static async Task<double> SortKilobytesAsync(NpgsqlConnection connection, string sql, int serverId, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        AddParameters(command, serverId, start, end);
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

    private static void AddParameters(NpgsqlCommand command, int serverId, DateTime start, DateTime end)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
    }

    /// <summary>
    /// 120 query keys, ten snapshots each, every row's delta_worker_time distinct so the ranking has no ties. Keys 1-60 resolve to 1.4 KB gzip plans that sit
    /// inline in the dimension (key 1 to a text plan instead), keys 61-120 to 40 KB plans that go to TOAST. Three keys cover the guard's other arms: one row
    /// carries its plan in its own column with no digest (the pre-#1767 shape), one points at a digest the dimension no longer holds, one at a dimension row with
    /// no content. The three are the costliest keys, so a wrong guard changes the ten returned.
    /// </summary>
    private static async Task WithSeedAsync(Func<NpgsqlConnection, int, DateTime, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live plan tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;
        const int serverId = 8570;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var succeeded = false;
        try
        {
            await ExecAsync(connection, ct,
                @"INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen)
                  SELECT sha256(convert_to('a8570-plan' || g, 'UTF8')),
                         CASE WHEN g = 1 THEN '<ShowPlanXML>text plan</ShowPlanXML>' END,
                         CASE WHEN g = 1 THEN NULL
                              ELSE decode((SELECT string_agg(md5(g::text || '-' || k::text), '') FROM generate_series(1, CASE WHEN g <= 60 THEN 88 ELSE 2500 END) k), 'hex') END,
                         now() AT TIME ZONE 'UTC'
                  FROM generate_series(1, 120) g");
            /* A dimension row with no content at all: the digest of key 122. */
            await ExecAsync(connection, ct,
                "INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen) VALUES (sha256(convert_to('a8570-plan122', 'UTF8')), NULL, NULL, now() AT TIME ZONE 'UTC')");

            var end = LiveClock.Now();
            var start = end.AddHours(-24);
            /* Keys 121 (own column, no digest), 122 (dimension row without content) and 123 (digest with no dimension row) outrank the rest. */
            await ExecAsync(connection, ct,
                @"INSERT INTO query_stats
                      (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                       query_text, query_plan_xml, query_plan_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                       delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                       min_dop, max_dop, sample_interval_seconds)
                  SELECT $1 + g * 100 + r, $2 - (r * interval '1 hour'), $3, 'a8570', 'PlanDb', '0xH' || g, '0xP' || g, '0xS' || g, '0xL' || g,
                         NULL,
                         CASE WHEN g = 121 THEN '<ShowPlanXML>legacy inline plan</ShowPlanXML>' END,
                         CASE WHEN g = 121 THEN NULL ELSE sha256(convert_to('a8570-plan' || g, 'UTF8')) END,
                         10, CASE WHEN g > 120 THEN 9000000 ELSE 20000 END + g * 100 + r, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(1, 123) g CROSS JOIN generate_series(0, $4 - 1) r",
                CollectionIdGenerator.Next(), end.AddMinutes(-30), serverId, Snapshots);

            await body(connection, serverId, start, end.AddMinutes(5), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await ExecAsync(cleanupConnection, cleanupCt, "DELETE FROM query_stats WHERE server_id = 8570"));
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
