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
/// #8570: the plan fetch of <c>get_analysis_facts</c> (<see cref="PgFactCollector.PlanAdvisorySql"/>) guards each in-window row with "its own plan column, or a
/// dimension row with content". That guard was a correlated EXISTS in the rank's WHERE, so it probed <c>query_plan_dim</c> once per snapshot row (85,451
/// probes for one field window) before the rank kept ten. The statement now probes once per distinct digest. The test seeds many snapshot rows over few
/// digests and pins two things by what the read does: the ten rows returned are exactly the rows the per-row statement returned (the guard's meaning is
/// unchanged, including a plan key whose costliest row has no plan but a cheaper row does), and the dimension is probed per digest, never per row and never
/// scanned whole.
/// </summary>
public sealed class PlanAdvisoryDigestProbeLiveTests
{
    private const int ServerId = 85701;
    private const int Digests = 200;

    /// <summary>The statement as it was before this change (a correlated EXISTS per snapshot row), kept here only so the new read is held to its results.</summary>
    private const string PerRowGuardSql = @"
SELECT COALESCE(r.query_plan_xml, d.query_plan_xml) AS query_plan_xml, d.query_plan_gz
FROM
(
    SELECT query_plan_xml, query_plan_digest, delta_worker_time
    FROM
    (
        SELECT
            f.query_plan_xml,
            f.query_plan_digest,
            f.delta_worker_time,
            ROW_NUMBER() OVER (PARTITION BY f.database_name, f.query_hash, COALESCE(f.query_plan_hash, f.plan_handle) ORDER BY f.delta_worker_time DESC) AS plan_rank
        FROM query_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time >= $2
        AND   f.collection_time <= $3
        AND   (f.query_plan_xml IS NOT NULL
               OR EXISTS (SELECT 1 FROM query_plan_dim AS p
                          WHERE p.digest = f.query_plan_digest
                          AND   (p.query_plan_xml IS NOT NULL OR p.query_plan_gz IS NOT NULL)))
    ) AS ranked
    WHERE plan_rank = 1
    ORDER BY delta_worker_time DESC
    LIMIT 10
) AS r
LEFT JOIN query_plan_dim AS d ON d.digest = r.query_plan_digest
ORDER BY r.delta_worker_time DESC";

    [Fact]
    public Task TheFetch_ReturnsThePerRowStatementsRows_AndProbesTheDimensionOncePerDigest() =>
        WithSeedAsync(async (connection, start, end, ct) =>
        {
            var expected = await RowsAsync(connection, PerRowGuardSql, start, end, ct);
            var actual = await RowsAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);

            Assert.Equal(10, expected.Count);
            /* The same ten plans in the same order, content included. */
            Assert.Equal(expected, actual);

            /* A plan key whose costliest row has no plan stands for itself through its cheaper row, which does (key 1001: its plan is plan 3). */
            Assert.Contains("<ShowPlanXML>plan 3</ShowPlanXML>|<null>", actual);
            /* A gzip-only dimension row counts (key 1003: digest 12). */
            Assert.Contains(actual, row => row.StartsWith("<null>|", StringComparison.Ordinal) && !row.EndsWith("<null>", StringComparison.Ordinal));
            /* A row's own column counts with no digest at all (key 1005). */
            Assert.Contains("<ShowPlanXML>own column plan</ShowPlanXML>|<null>", actual);
            /* A digest whose dimension row is gone (key 1002) and one whose dimension row has no content (key 1004) are among the costliest keys but never appear. */
            Assert.DoesNotContain("<null>|<null>", actual);

            var oldProbes = await DimensionProbesAsync(connection, PerRowGuardSql, start, end, ct);
            var newProbes = await DimensionProbesAsync(connection, PgFactCollector.PlanAdvisorySql, start, end, ct);

            /* The seed must have the shape that costs: the per-row guard probes the dimension once per snapshot row that has no plan of its own. */
            Assert.True(oldProbes.Loops > 10_000, $"the seed's per-row statement probed the dimension {oldProbes.Loops} times, so it does not show the defect");
            /* One probe per distinct digest plus the ten winners' content read; never a row-count multiple. */
            Assert.True(newProbes.Loops <= Digests + 10 + 6, $"the plan fetch probed query_plan_dim {newProbes.Loops} times for {Digests} distinct digests (the per-row statement probed {oldProbes.Loops} times)");
            Assert.False(newProbes.WholeScan, "the plan fetch scans query_plan_dim whole instead of probing it by digest");
        });

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        AddParameters(command, start, end);
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

    /// <summary>The loops every query_plan_dim node of the statement's executed plan ran, and whether any of them read the whole table.</summary>
    private static async Task<(double Loops, bool WholeScan)> DimensionProbesAsync(NpgsqlConnection connection, string sql, DateTime start, DateTime end, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        AddParameters(command, start, end);
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        if (Environment.GetEnvironmentVariable("PLAN_EXPLAIN_OUT") is { Length: > 0 } outPath)
        {
            await System.IO.File.AppendAllTextAsync(outPath, json + "\n", ct);
        }

        using var document = JsonDocument.Parse(json);
        var loops = 0.0;
        var whole = false;
        Walk(document.RootElement[0].GetProperty("Plan"), ref loops, ref whole);
        return (loops, whole);
    }

    private static void Walk(JsonElement node, ref double loops, ref bool whole)
    {
        if (node.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "query_plan_dim")
        {
            loops += node.GetProperty("Actual Loops").GetDouble();
            var type = node.GetProperty("Node Type").GetString();
            if (type is "Seq Scan" or "Parallel Seq Scan")
            {
                whole = true;
            }
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, ref loops, ref whole);
            }
        }
    }

    private static void AddParameters(NpgsqlCommand command, DateTime start, DateTime end)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(start, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DateTime.SpecifyKind(end, DateTimeKind.Unspecified) });
    }

    /// <summary>
    /// 400 bulk keys of 50 snapshots over 200 digests, so a digest backs two keys and each key's rows share it. A digest by its number: no dimension row (a multiple
    /// of 10), a dimension row with no content (one past), gzip bytes only (two past), else text. Five special keys outrank the bulk and cover the guard's arms:
    /// 1001 has a costliest row whose digest is gone and cheaper rows on a digest with text (it must stand for itself through them), 1002 only gone digests,
    /// 1003 a gzip-only digest, 1004 a content-less digest, 1005 its own plan column with no digest. 50,000 filler dimension rows give the planner a real primary key to choose against.
    /// </summary>
    private static async Task WithSeedAsync(Func<NpgsqlConnection, DateTime, DateTime, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live plan tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connectionString = scratch.ConnectionString;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var succeeded = false;
        try
        {
            await ExecAsync(connection, ct,
                @"INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen)
                  SELECT sha256(convert_to('a8570p-d' || g, 'UTF8')),
                         CASE WHEN g % 10 >= 3 THEN '<ShowPlanXML>plan ' || g || '</ShowPlanXML>' END,
                         CASE WHEN g % 10 = 2 THEN decode(md5('gz' || g), 'hex') END,
                         now() AT TIME ZONE 'UTC'
                  FROM generate_series(1, 200) g
                  WHERE g % 10 <> 0");
            await ExecAsync(connection, ct,
                @"INSERT INTO query_plan_dim (digest, query_plan_xml, query_plan_gz, last_seen)
                  SELECT sha256(convert_to('a8570p-f' || g, 'UTF8')), '<ShowPlanXML>filler</ShowPlanXML>', NULL, now() AT TIME ZONE 'UTC'
                  FROM generate_series(1, 50000) g");
            await ExecAsync(connection, ct, "ANALYZE query_plan_dim");
            /* The field's dimension holds 52 million rows, far over any work_mem the planner would build a hash of, so it answers a per-row EXISTS with one index probe per row.
               A small seed would be hashed whole instead, so the connection gets the smallest work_mem to put the seed in the field's position. */
            await ExecAsync(connection, ct, "SET work_mem = '64kB'");

            var end = LiveClock.Now();
            var start = end.AddHours(-4);
            const string insertHead = @"INSERT INTO query_stats
                      (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                       query_text, query_plan_xml, query_plan_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                       delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                       min_dop, max_dop, sample_interval_seconds)
                  ";
            var id = CollectionIdGenerator.Next();
            /* Bulk: key g (1..400), snapshot r (0..49), digest ((g - 1) % 200) + 1. */
            await ExecAsync(connection, ct,
                insertHead + @"SELECT $1 + g * 100 + r, $2 - (r * interval '4 minutes'), $3, 'a8570', 'PlanDb', '0xH' || g, '0xP' || g, '0xS' || g, '0xL' || g,
                         NULL, NULL, sha256(convert_to('a8570p-d' || (((g - 1) % 200) + 1), 'UTF8')),
                         10, 1000 + g * 100 + r, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(1, 400) g CROSS JOIN generate_series(0, 49) r",
                id, end.AddMinutes(-10), ServerId);
            /* Special keys. Key 1001: row 0 is the costliest and its digest (10) has no dimension row; rows 1+ carry digest 3 (text). */
            await ExecAsync(connection, ct,
                insertHead + @"SELECT $1 + 200000 + k * 100 + r, $2 - (r * interval '4 minutes'), $3, 'a8570', 'PlanDb', '0xH' || k, '0xP' || k, '0xS' || k, '0xL' || k,
                         NULL,
                         CASE WHEN k = 1005 THEN '<ShowPlanXML>own column plan</ShowPlanXML>' END,
                         CASE k WHEN 1001 THEN sha256(convert_to('a8570p-d' || CASE WHEN r = 0 THEN 10 ELSE 3 END, 'UTF8'))
                                WHEN 1002 THEN sha256(convert_to('a8570p-d20', 'UTF8'))
                                WHEN 1003 THEN sha256(convert_to('a8570p-d12', 'UTF8'))
                                WHEN 1004 THEN sha256(convert_to('a8570p-d11', 'UTF8'))
                                END,
                         10, (10000000 - (k - 1001) * 100000) - r * 1000, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                  FROM generate_series(1001, 1005) k CROSS JOIN generate_series(0, 9) r",
                id, end.AddMinutes(-10), ServerId);
            await ExecAsync(connection, ct, "ANALYZE query_stats");

            await body(connection, start, end.AddMinutes(5), ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await ExecAsync(cleanupConnection, cleanupCt, "DELETE FROM query_stats WHERE server_id = 85701"));
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
