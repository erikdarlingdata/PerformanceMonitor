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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it.

   #9151: an all-raw Query Store RankedTimeSeries panel built the de-duplicated rows twice, once for the rank and once for the series, each
   with its own scan to find the matching keys. It now builds them once into a shared CTE that both read. The answer must not change: the
   new statement returns the old statement's rows, in the old statement's order, and it reads fewer pages. The old statement is
   ComposeCompiler.CompileCore(..., sharedQueryStoreDedupe: false). */
public sealed class ComposeQueryStoreSharedDedupeLiveTests
{
    private const string SkipMessage = "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 shared dedupe live tests.";

    private static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(4);

    /// <summary>Three servers; every query id exists on each. One collection every 5 minutes over the window and 30 minutes before it.
    /// A query's interval is the hour, re-read each collection with a growing count, as the collector re-reads the open interval.
    /// module_name is NULL for a fifth of the queries (the NULL-module case), 'usp_Order%' for a quarter (the narrow filter), and the six tie_* modules
    /// carry identical measures larger than any other, so a top-4 cuts through a six-way tie. The edge rows below each need a
    /// key column of the dedupe to keep their rows apart (the role, the interval id) or the dedupe's pick (a mid-window rename).</summary>
    private const string Seed = @"
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT row_number() OVER (ORDER BY t.ct, s.n, q.n), t.ct, s.n, 'srv' || s.n,
       CASE WHEN q.n % 29 = 0 THEN NULL ELSE 'db' || (q.n % 4) END,
       q.n, q.n * 10, 'Regular', date_trunc('hour', t.ct),
       CASE WHEN q.n % 5 = 0 THEN NULL WHEN q.n % 4 = 0 THEN 'usp_Order' || (q.n % 11) ELSE 'proc' || (q.n % 17) END,
       md5('h' || q.n),
       1 + (extract(minute FROM t.ct)::int / 5) + (q.n % 7) + s.n,
       100 + (q.n % 13) * 10, 50 + (q.n % 19) * 5, 900 + (q.n % 11), 400 + (q.n % 17),
       'PRIMARY', (extract(epoch FROM t.ct)::bigint / 3600), date_trunc('hour', t.ct)
FROM generate_series($1::timestamp - interval '30 minutes', $2::timestamp, interval '5 minutes') AS t(ct)
CROSS JOIN generate_series(1, 3) AS s(n)
CROSS JOIN generate_series(1, 400) AS q(n);
INSERT INTO collect.query_store_stats (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id,
    execution_type_desc, first_execution_time, module_name, query_hash, execution_count, avg_duration_us, avg_cpu_time_us,
    max_duration_us, max_cpu_time_us, replica_role, runtime_stats_interval_id, interval_start_time_utc)
SELECT 100000000 + e.n, $1::timestamp + e.at, 1, 'srv1', e.db, e.qid, e.qid * 10, 'Regular', $1::timestamp, e.module, md5('e' || e.qid),
       e.executions, 300, 200, 900, 400, e.role, e.interval_id, $1::timestamp
FROM (VALUES
    (1, interval '1 hour 5 minutes', 'dbEdge', 9001, 'usp_OrderRole', 10, 'PRIMARY', 1),
    (2, interval '1 hour 20 minutes', 'dbEdge', 9001, 'usp_OrderRole', 500, 'SECONDARY', 1),
    (3, interval '1 hour 25 minutes', 'dbEdge', 9001, 'usp_OrderRole', 60, NULL, 1),
    (4, interval '1 hour 5 minutes', 'dbEdge', 9002, 'usp_OrderInterval', 7, 'PRIMARY', 1),
    (5, interval '1 hour 5 minutes', 'dbEdge', 9002, 'usp_OrderInterval', 700, 'PRIMARY', 2),
    (6, interval '1 hour 5 minutes', 'dbEdge', 9003, 'usp_OrderOld', 4, 'PRIMARY', 1),
    (7, interval '1 hour 25 minutes', 'dbEdge', 9003, 'proc_renamed', 5000, 'PRIMARY', 1),
    (8, interval '1 hour 5 minutes', 'dbEdge', 9004, NULL, 800, 'PRIMARY', 1),
    (9, interval '1 hour 25 minutes', 'dbEdge', 9004, NULL, 900, 'PRIMARY', 1),
    (10, interval '2 hours 5 minutes', 'dbTie', 9101, 'tie_a', 1000000, 'PRIMARY', 1),
    (11, interval '2 hours 5 minutes', 'dbTie', 9102, 'tie_b', 1000000, 'PRIMARY', 1),
    (12, interval '2 hours 5 minutes', 'dbTie', 9103, 'tie_c', 1000000, 'PRIMARY', 1),
    (13, interval '2 hours 5 minutes', 'dbTie', 9104, 'tie_d', 1000000, 'PRIMARY', 1),
    (14, interval '2 hours 5 minutes', 'dbTie', 9105, 'tie_e', 1000000, 'PRIMARY', 1),
    (15, interval '2 hours 5 minutes', 'dbTie', 9106, 'tie_f', 1000000, 'PRIMARY', 1)
) AS e(n, at, db, qid, module, executions, role, interval_id);
ANALYZE collect.query_store_stats;";

    private sealed record Filter(string Name, string? Json);

    private static readonly Filter[] Filters =
    {
        new("narrow module filter", "[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"%Order%\"}]"),
        new("broad module filter", "[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"%e%\"}]"),
        new("partition column filter", "[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"db1\"}]"),
        new("module and partition column filters", "[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"%o%\"},{\"dimension\":\"database_name\",\"op\":\"neq\",\"value\":\"db0\"}]"),
        new("no filter", null),
    };

    private static readonly (string Measure, string Aggregate)[] Measures =
    {
        ("qs_executions", "sum"), ("qs_avg_cpu_us", ""),
    };

    private static string PanelJson(string measure, string aggregate, string[] groups, int topN, bool includeOther, string? filters) =>
        "{\"source\":\"query_store_stats\",\"" + (aggregate.Length == 0 ? "ratio" : "measure") + "\":\"" + measure + "\""
        + (aggregate.Length == 0 ? string.Empty : ",\"aggregate\":\"" + aggregate + "\"")
        + ",\"timeBucket\":\"hour\",\"topN\":" + topN.ToString(CultureInfo.InvariantCulture)
        + ",\"groupBy\":[" + string.Join(",", groups.Select(g => "\"" + g + "\"")) + "],\"viz\":\"line\""
        + (includeOther ? ",\"includeOther\":true" : string.Empty) + (filters is null ? string.Empty : ",\"filters\":" + filters) + "}";

    private static ComposeRunContext Context(IReadOnlyList<string>? servers) =>
        new(servers, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown, QueryStoreWideEligible: false);

    private static ComposeCompiled Compile(string panel, ComposeRunContext context, bool shared)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var (compiled, compileError) = ComposeCompiler.CompileCore(plan!, context, singleScanRankedTimeSeries: true, sharedQueryStoreDedupe: shared);
        Assert.True(compileError is null, compileError);
        return compiled!;
    }

    private static async Task<NpgsqlConnection> OpenSeededAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        for (var id = 1; id <= 3; id++)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "srv" + id, ct);
        }

        /* one statement per INSERT: a statement with parameters cannot hold several commands */
        foreach (var statement in System.Text.RegularExpressions.Regex.Split(Seed, @";\s*(?=INSERT INTO|ANALYZE)"))
        {
            await using var command = new NpgsqlCommand(statement, connection) { CommandTimeout = 600 };
            if (statement.Contains("$1", StringComparison.Ordinal))
            {
                command.Parameters.AddWithValue(WindowStart);
                command.Parameters.AddWithValue(WindowEnd);
            }

            await command.ExecuteNonQueryAsync(ct);
        }

        return connection;
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, ComposeCompiled compiled)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = p.NpgsqlDbType, Value = p.Value });
        }

        return command;
    }

    /// <summary>The rows exactly as returned: every column of every row, in order.</summary>
    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, ComposeCompiled compiled, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = Command(connection, compiled.Sql, compiled);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture))));
        }

        return rows;
    }

    /// <summary>Null when the two result sets are the same rows in the same order. The statement orders by bucket alone (its final
    /// ORDER BY), so the rows of one bucket come out in the aggregate's own order, which no statement text promises: the buckets
    /// must come out in the same sequence and each bucket must hold the same rows.</summary>
    private static string? Difference(List<string> rows, List<string> oldRows)
    {
        if (rows.Count != oldRows.Count)
        {
            return $"{rows.Count} rows against {oldRows.Count}";
        }

        var buckets = rows.Select(r => r.Split('|')[0]).ToList();
        if (!buckets.SequenceEqual(oldRows.Select(r => r.Split('|')[0])))
        {
            return "the buckets come out in a different order";
        }

        var sorted = rows.OrderBy(r => r, StringComparer.Ordinal).ToList();
        var oldSorted = oldRows.OrderBy(r => r, StringComparer.Ordinal).ToList();
        var at = Enumerable.Range(0, sorted.Count).Select(i => (int?)i).FirstOrDefault(i => sorted[i!.Value] != oldSorted[i.Value]);
        return at is null ? null : $"row {sorted[at.Value]} against {oldSorted[at.Value]}";
    }

    [Fact]
    public async Task SharedDedupe_ReturnsTheRowsOfTheTwoDedupeStatement_InTheSameOrder()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, ct);
            var groupings = new[] { new[] { "module_name" }, new[] { "server" } };
            var compared = 0;
            var withRows = 0;
            var withNullModule = 0;
            foreach (var servers in new IReadOnlyList<string>?[] { null, new[] { "srv2" } })
            {
                var context = Context(servers);
                foreach (var filter in Filters)
                {
                    foreach (var (measure, aggregate) in Measures)
                    {
                        foreach (var groups in groupings)
                        {
                            foreach (var includeOther in new[] { false, true })
                            {
                                var panel = PanelJson(measure, aggregate, groups, 4, includeOther, filter.Json);
                                var shared = Compile(panel, context, shared: true);
                                var old = Compile(panel, context, shared: false);
                                Assert.NotEqual(old.Sql, shared.Sql);

                                var rows = await RowsAsync(connection, shared, ct);
                                var oldRows = await RowsAsync(connection, old, ct);
                                var difference = Difference(rows, oldRows);
                                Assert.True(difference is null, $"{filter.Name} | servers {(servers is null ? "fleet" : "srv2")} | {panel}: {difference}");
                                compared++;
                                withRows += rows.Count > 0 ? 1 : 0;
                                withNullModule += rows.Any(r => r.Contains("<null>", StringComparison.Ordinal)) ? 1 : 0;
                            }
                        }
                    }
                }
            }

            Assert.True(compared == 2 * Filters.Length * Measures.Length * 2 * 2, $"compared {compared}");
            Assert.True(withRows * 10 >= compared * 8, $"the seed must give most cases rows: {withRows} of {compared}");
            Assert.True(withNullModule > 0, "the seed must reach the NULL module group");
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task SharedDedupe_KeepsTheRowsAKeyColumnSeparatesAndThePickOfTheNewestSnapshot()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, ct);
            var context = Context(null);
            var panel = PanelJson("qs_executions", "sum", new[] { "module_name" }, 50, false,
                "[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"dbEdge\"}]");
            var rows = await RowsAsync(connection, Compile(panel, context, shared: true), ct);
            var oldRows = await RowsAsync(connection, Compile(panel, context, shared: false), ct);
            Assert.Null(Difference(rows, oldRows));

            /* The newest snapshot per key, summed by module. Read as the compiled statement returns it: the bucket, then the module, then the value. */
            long Executions(string module) => rows.Where(r => r.Split('|')[1] == module).Sum(r => (long)double.Parse(r.Split('|')[2], CultureInfo.InvariantCulture));
            /* 9001: three keys (PRIMARY 10, SECONDARY 500, NULL role 60) each survive. */
            Assert.Equal(570, Executions("usp_OrderRole"));
            /* 9002: two intervals, one snapshot each. */
            Assert.Equal(707, Executions("usp_OrderInterval"));
            /* 9003: one key, the newest snapshot is the renamed one, and the older name keeps nothing. */
            Assert.Equal(5000, Executions("proc_renamed"));
            Assert.Equal(0, Executions("usp_OrderOld"));
            /* 9004: one key, the newest snapshot (900) and not the older. */
            Assert.Equal(900, Executions("<null>"));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task SharedDedupe_ReadsFewerSharedBlocksThanTheTwoDedupeStatement()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), SkipMessage);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = await OpenSeededAsync(scratch, ct);
            var context = Context(null);
            foreach (var filter in Filters.Where(f => f.Json is not null && f.Json.Contains("module_name", StringComparison.Ordinal)))
            {
                var panel = PanelJson("qs_executions", "sum", new[] { "module_name" }, 25, false, filter.Json);
                var shared = Compile(panel, context, shared: true);
                var old = Compile(panel, context, shared: false);
                var sharedPages = await DataPagesAsync(connection, shared, ct);
                var oldPages = await DataPagesAsync(connection, old, ct);
                TestContext.Current.SendDiagnosticMessage($"{filter.Name}: {sharedPages} blocks shared, {oldPages} blocks before");
                Assert.True(sharedPages > 0, $"{filter.Name}: the shared statement read no blocks");
                Assert.True(sharedPages < oldPages, $"{filter.Name}: the shared statement read {sharedPages} blocks, the old one {oldPages}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>The blocks the statement reads (the second, warm run: the first rebuilds catalog caches).</summary>
    private static async Task<long> DataPagesAsync(NpgsqlConnection connection, ComposeCompiled compiled, CancellationToken ct)
    {
        long pages = 0;
        for (var run = 0; run < 2; run++)
        {
            await using var command = Command(connection, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + compiled.Sql, compiled);
            pages = ExplainBuffers.DataPages(await command.ExecuteScalarAsync(ct));
        }

        return pages;
    }
}
