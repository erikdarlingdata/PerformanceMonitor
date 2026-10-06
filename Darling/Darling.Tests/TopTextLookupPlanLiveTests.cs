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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #5309 and #5313 (inside #5299): the PLAN of the text lookup, read from EXPLAIN (ANALYZE, FORMAT JSON) on a small seed.
/// Two defects the large-store timing found, each pinned here by what the plan does, not by what the text says.
///
/// <para><b>The raw lookup runs once.</b> <c>latest_text</c> is joined to the ranked rows NULL-safe
/// (<c>IS NOT DISTINCT FROM</c>), which PostgreSQL cannot hash, so a plain CTE is inlined into a nested loop and re-run for
/// every ranked row (a 120-candidate round scanned the 20,000-row text dimension 10,000 times, 9,975 ms against 805 ms
/// materialized). With WAITFOR shells planted above the real groups the round has to read past them, which is the case
/// that made it expensive. The seed here is 125 candidates over unanalyzed tables, so the planner estimates one row and
/// takes the nested loop on the plain CTE; the test asserts the lookup is its own CTE node that ran one time.</para>
///
/// <para><b>The hourly lookup reads the ranked keys, not the window.</b> <c>latest_in_window</c> read every raw row of the
/// window and joined it to the ranked keys (1,439,400 rows at 168 hours to find 25 texts). It now probes
/// <c>idx_query_stats_server_hash_time</c> once per ranked key. The test asserts the rows the lookup reads from raw are
/// bounded by the ranked keys, not by the window's row count.</para>
/// </summary>
[Collection("live-postgres")]
public sealed class TopTextLookupPlanLiveTests
{
    private const string ServerName = "a5299-text-lookup-plan";
    private const int Shells = 120;
    private const int Real = 30;
    private const int RowsPerGroup = 12;
    private const int Candidates = 125;
    private const int Top = 5;

    [Fact]
    public Task TheRawLookup_RunsOnce_NotOncePerRankedRow() =>
        WithSeedAsync(async (connection, serverId, now, ct) =>
        {
            var start = now.AddHours(-24);
            var end = now.AddMinutes(5);

            foreach (var name in new[] { "TopQueriesSql", "TopQueriesByHostObjectSql" })
            {
                var sql = TopRankings.Apply(name == "TopQueriesSql" ? DarlingDataReader.TopQueriesSql : DarlingDataReader.TopQueriesByHostObjectSql,
                    TopRanking.Cpu, hourly: false);
                var plan = await ExplainAsync(connection, sql, ct, serverId, start, end,
                    P(NpgsqlDbType.Integer, Top), P(NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, 0), P(NpgsqlDbType.Integer, Candidates));
                AssertLookupRanOnce(name, plan);
            }

            var viewerPlan = await ExplainAsync(connection, ViewerDataService.TopQueriesSql, ct, serverId, start, end,
                P(NpgsqlDbType.Integer, Top), P(NpgsqlDbType.Array | NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, Candidates));
            AssertLookupRanOnce("ViewerTopQueriesSql", viewerPlan);
        });

    [Fact]
    public Task TheHourlyLookup_ReadsTheRankedKeys_NotTheWholeWindow() =>
        WithSeedAsync(async (connection, serverId, now, ct) =>
        {
            /* The index the lookup probes is created by the service at start (PgTableTuning.ApplyAsync), not by a migration, so a
               test store may lack it: make sure it is there, and give the planner statistics, as a store that has run a while has.
               (Without the statistics the unanalyzed seed makes the planner walk the time index and filter on the hash.) */
            const string indexName = "idx_query_stats_server_hash_time";
            await using var probe = new NpgsqlCommand(
                "SELECT count(*) FROM pg_indexes WHERE schemaname = 'collect' AND indexname = '" + indexName + "'", connection);
            var created = (long)(await probe.ExecuteScalarAsync(ct))! == 0;
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "CREATE INDEX IF NOT EXISTS " + indexName + " ON collect.query_stats (server_id, query_hash, collection_time DESC)");
            try
            {
                await DarlingMcpTestData.ExecAsync(connection, ct, "ANALYZE collect.query_stats");
                await AssertHourlyLookupAsync(connection, serverId, now, ct);
            }
            finally
            {
                if (created)
                {
                    await DarlingMcpTestData.ExecAsync(connection, ct, "DROP INDEX IF EXISTS collect." + indexName);
                }
            }
        });

    private static async Task AssertHourlyLookupAsync(NpgsqlConnection connection, int serverId, DateTime now, CancellationToken ct)
    {
        /* The rollup is stood in for by a plain relation over the raw rows, so the test reads the TEXT lookup's plan
           and not the router's: ranked reads it, the lookup reads query_stats. */
        const string standIn = "(SELECT server_id, collection_time AS bucket, database_name, query_hash, sql_handle, " +
            "delta_execution_count AS execution_count_sum, delta_worker_time AS worker_time_sum, " +
            "delta_elapsed_time AS elapsed_time_sum FROM collect.query_stats) AS f";
        var sql = TopRankings.Apply(DarlingDataReader.TopQueriesHourlySql, TopRanking.Cpu, hourly: true)
            .Replace("$FROM$", standIn, StringComparison.Ordinal)
            .Replace("$CEIL$", "", StringComparison.Ordinal);
        var plan = await ExplainAsync(connection, sql, ct, serverId, now.AddHours(-24), now.AddMinutes(5),
            P(NpgsqlDbType.Integer, Top), P(NpgsqlDbType.Text, null), P(NpgsqlDbType.Integer, Candidates));

        var lookup = Nodes(plan.Root, null).Where(n => n.Cte == "latest_in_window" && n.Relation == "query_stats").ToList();
        Assert.NotEmpty(lookup);
        /* Rows the lookup touched in raw: what each scan returned plus what its filter threw away, over every loop. */
        var read = lookup.Sum(n => (n.Rows + n.Removed) * n.Loops);
        var windowRows = (long)(Shells + Real) * RowsPerGroup;
        var rankedKeys = Candidates;
        Assert.True(read <= rankedKeys * 4,
            $"the hourly text lookup read {read} raw rows [{string.Join("; ", lookup.Select(n => $"{n.NodeType} loops={n.Loops} rows={n.Rows} removed={n.Removed}"))}] for {rankedKeys} ranked keys over a window of {windowRows}; it must read about one per key, not the window");
    }

    [Fact]
    public Task TheHourlyLookup_StillFindsTheText_ForANullKey_ATextlessNewestRow_AndAKeyWithNoRowInTheWindow() =>
        WithSeedAsync(async (connection, serverId, now, ct) =>
        {
            async Task Plant(string? database, string? hash, DateTime at, string? text) =>
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats
                          (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                           query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)
                      VALUES ($1, $2, $3, $4, $5, $6, '0xP', '0xS', '0xL', $7, 10, $8, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600)",
                    CollectionIdGenerator.Next(), at, serverId, "a5299-hourly-keys", database, hash, text, 1_000_000L - (long)(now - at).TotalMinutes);

            /* The newest row in the window has no text: the lookup takes the newest row that does, inside the window. */
            await Plant("D1", "0xH1", now.AddHours(-3), "older text in the window");
            await Plant("D1", "0xH1", now.AddHours(-1), null);
            /* A group with a NULL database and a NULL hash still gets its text (strict equality would have lost it). */
            await Plant(null, null, now.AddHours(-2), "text of the null-keyed group");
            /* A group the window holds no text for: the rollup outlives raw, so the fallback finds the newest text raw has at all. */
            await Plant("D3", "0xH3", now.AddHours(-2), null);
            await Plant("D3", "0xH3", now.AddDays(-10), "text outside the window");

            var standIn = "(SELECT server_id, collection_time AS bucket, database_name, query_hash, sql_handle, " +
                "delta_execution_count AS execution_count_sum, delta_worker_time AS worker_time_sum, " +
                "delta_elapsed_time AS elapsed_time_sum FROM collect.query_stats WHERE collection_time >= '" +
                now.AddDays(-2).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "') AS f";
            /* The stand-in keeps the window rows only, so the fallback's ten-day-old row is read by latest_any from raw, as in life. */
            var sql = TopRankings.Apply(DarlingDataReader.TopQueriesHourlySql, TopRanking.Cpu, hourly: true)
                .Replace("$FROM$", standIn, StringComparison.Ordinal)
                .Replace("$CEIL$", "", StringComparison.Ordinal);
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.Add(P(NpgsqlDbType.Integer, serverId));
            command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(now.AddHours(-24), DateTimeKind.Unspecified)));
            command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(now.AddMinutes(5), DateTimeKind.Unspecified)));
            command.Parameters.Add(P(NpgsqlDbType.Integer, 10));
            command.Parameters.Add(P(NpgsqlDbType.Text, null));
            command.Parameters.Add(P(NpgsqlDbType.Integer, 10));
            var texts = new Dictionary<string, string?>();
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    if (!reader.IsDBNull(7))
                    {
                        texts[(reader.IsDBNull(0) ? "(null)" : reader.GetString(0)) + "/" + (reader.IsDBNull(1) ? "(null)" : reader.GetString(1))] =
                            reader.IsDBNull(6) ? null : reader.GetString(6);
                    }
                }
            }

            Assert.Equal(3, texts.Count);
            Assert.Equal("older text in the window", texts["D1/0xH1"]);
            Assert.Equal("text of the null-keyed group", texts["(null)/(null)"]);
            Assert.Equal("text outside the window", texts["D3/0xH3"]);
        }, "a5299-hourly-keys", bulk: false);

    // ------------------------------------------------------------------ plan reading

    private static void AssertLookupRanOnce(string name, Plan plan)
    {
        var all = Nodes(plan.Root, null).ToList();
        /* The defect, as the plan shows it: a sequential scan of the text dimension that ran once per ranked row. */
        var rescans = all.Where(n => n.Relation == "query_text_dim" && n.NodeType == "Seq Scan" && n.Loops > 1).ToList();
        var lookup = all.Where(n => n.Cte == "latest_text").ToList();
        Assert.True(rescans.Count == 0 && lookup.Count > 0,
            $"{name}: latest_text is not its own CTE node (found {lookup.Count} nodes), so PostgreSQL inlined it into the join and re-runs it per ranked row; " +
            $"query_text_dim was scanned {string.Join(", ", rescans.Select(n => n.Loops + " times"))} (add MATERIALIZED)");
        var root = lookup.OrderBy(n => n.Depth).First();
        Assert.True(root.Loops == 1, $"{name}: the text lookup ran {root.Loops} times; it must run once");
    }

    private sealed record Plan(JsonElement Root, double Ms);

    private sealed record PlanNode(string NodeType, string? Relation, string? Cte, long Loops, long Rows, long Removed, int Depth);

    /// <summary>Walks the plan. <c>Cte</c> is the CTE a node sits under ("CTE name" in Subplan Name), or null.</summary>
    private static IEnumerable<PlanNode> Nodes(JsonElement node, string? cte, int depth = 0)
    {
        if (node.TryGetProperty("Subplan Name", out var sub) && sub.GetString() is { } s && s.StartsWith("CTE ", StringComparison.Ordinal))
        {
            cte = s[4..];
        }

        long Get(string p) => node.TryGetProperty(p, out var v) && v.ValueKind == JsonValueKind.Number ? (long)v.GetDouble() : 0;
        yield return new PlanNode(
            node.GetProperty("Node Type").GetString()!,
            node.TryGetProperty("Relation Name", out var r) ? r.GetString() : null,
            cte, Get("Actual Loops"), Get("Actual Rows"),
            Get("Rows Removed by Filter") + Get("Rows Removed by Index Recheck"), depth);

        if (node.TryGetProperty("Plans", out var plans))
        {
            foreach (var child in plans.EnumerateArray())
            {
                foreach (var n in Nodes(child, cte, depth + 1))
                {
                    yield return n;
                }
            }
        }
    }

    private static NpgsqlParameter P(NpgsqlDbType type, object? value) => new() { NpgsqlDbType = type, Value = value ?? DBNull.Value };

    private static async Task<Plan> ExplainAsync(NpgsqlConnection connection, string sql, CancellationToken ct,
        int serverId, DateTime start, DateTime end, params NpgsqlParameter[] rest)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, connection) { CommandTimeout = 120 };
        command.Parameters.Add(P(NpgsqlDbType.Integer, serverId));
        command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified)));
        command.Parameters.Add(P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified)));
        foreach (var p in rest)
        {
            command.Parameters.Add(p);
        }

        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        var doc = JsonDocument.Parse(json).RootElement[0];
        return new Plan(doc.GetProperty("Plan").Clone(), doc.GetProperty("Execution Time").GetDouble());
    }

    // ------------------------------------------------------------------ the seed

    /// <summary>
    /// 120 WAITFOR shells that outrank 30 real groups, twelve rows each, texts only in the dimension (the modern shape:
    /// the stat row keeps a digest, not the text). The tables are NOT analyzed, which is what makes the planner estimate
    /// one row for the lookup on a store the collectors have just filled.
    /// </summary>
    private static async Task WithSeedAsync(Func<NpgsqlConnection, int, DateTime, CancellationToken, Task> body, string serverName = ServerName, bool bulk = true)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to run the live plan tests.");
        var ct = TestContext.Current.CancellationToken;
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var cleanup = string.Format(CultureInfo.InvariantCulture,
            "DELETE FROM query_stats WHERE server_id = {0}; DELETE FROM query_text_dim WHERE query_text LIKE '%a5299-plan%'; DELETE FROM servers WHERE server_id = {0}", serverId);

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.ExecAsync(connection, ct, cleanup);
        var succeeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, serverId, serverName, ct);
            var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
            if (bulk)
            {
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_text_dim (digest, query_text, last_seen)
                      SELECT sha256(convert_to(t.text, 'UTF8')), t.text, now() AT TIME ZONE 'UTC'
                      FROM (SELECT CASE WHEN g <= $1 THEN 'WAITFOR DELAY ''00:00:30'' /* a5299-plan ' || g || ' */'
                                        ELSE 'SELECT real /* a5299-plan ' || g || ' */' END AS text
                            FROM generate_series(1, $1 + $2) AS g) AS t
                      ON CONFLICT (digest) DO NOTHING",
                    Shells, Real);
                await DarlingMcpTestData.ExecAsync(connection, ct,
                    @"INSERT INTO query_stats
                          (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, sql_handle, plan_handle,
                           query_text, query_text_digest, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads,
                           delta_logical_writes, delta_physical_reads, min_worker_time, max_worker_time, min_elapsed_time, max_elapsed_time,
                           min_dop, max_dop, sample_interval_seconds)
                      SELECT $1 + g * 100 + r, $2 - (r * interval '1 hour'), $3, $4, 'PlanDb', '0xH' || g, '0xP' || g, '0xS' || g, '0xL' || g,
                             NULL,
                             sha256(convert_to(CASE WHEN g <= $5 THEN 'WAITFOR DELAY ''00:00:30'' /* a5299-plan ' || g || ' */'
                                                    ELSE 'SELECT real /* a5299-plan ' || g || ' */' END, 'UTF8')),
                             10, (CASE WHEN g <= $5 THEN 2000000 ELSE 20000 END) - g * 100, 100, 100, 0, 0, 1, 100, 1, 100, 1, 1, 3600
                      FROM generate_series(1, $5 + $6) AS g CROSS JOIN generate_series(1, $7) AS r",
                    CollectionIdGenerator.Next(), now.AddMinutes(-30), serverId, serverName, Shells, Real, RowsPerGroup);
            }

            await body(connection, serverId, now, ct);
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, succeeded, async (cleanupConnection, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanupConnection, cleanupCt, cleanup));
        }
    }
}
