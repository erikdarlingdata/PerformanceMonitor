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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9960: the live exactness proof for the module pre-aggregate. A <c>query_stats</c> panel that joins modules (a group or a filter
/// on <c>object_name</c> or <c>statement</c>) aggregates its fact rows to one row per (server_name, sql_handle, kept fact columns)
/// before the join, and combines the partials after it. Every fact plants its own scratch store (four server names, one of them
/// with no registry row, handles shared across servers, handles with no module row, NULL handles, restart rows, NULL deltas, NULL
/// intervals, ties, and rows on both raw edges and in the whole hours between), then compiles each panel twice with the same
/// context: the pre-aggregated statement, and the statement that joins every fact row (<c>moduleJoinPreAggregate: false</c>). The
/// two must bind the same parameters and return the same rows, values and order. Each comparison also asserts that the first
/// statement really is the pre-aggregated one and the second is not, so the proof never compares a statement with itself.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every fact reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposeModulePreAggregateLiveTests
{
    private const string ServerA = "PreAggServerA";
    private const string ServerB = "PreAggServerB";
    private const string ServerC = "PreAggServerC";
    private const string Ghost = "PreAggGhost";

    private static readonly Dictionary<string, int> s_serverIds = new(StringComparer.Ordinal)
    {
        [ServerA] = -49601, [ServerB] = -49602, [ServerC] = -49603, [Ghost] = -49604,
    };

    /// <summary>The marker of the pre-aggregated statement: the inner GROUP BY on the join key.</summary>
    private const string PreAggregated = "GROUP BY f.server_name, " + AdHocFold;

    /// <summary>The pre-aggregate's handle key: the fact's handle when m holds its (server_name, sql_handle), NULL otherwise.</summary>
    private const string AdHocFold = "CASE WHEN (f.server_name, f.sql_handle) IN (SELECT mk.server_name, mk.sql_handle FROM m AS mk) THEN f.sql_handle END";
    private const string Hybrid = "query_stats_interval_hourly AS mid";
    private const string MapFirst = "module_map AS mm";

    /// <summary>The one instant every fact runs at, 37 minutes and 21 seconds into an hour.</summary>
    private static readonly DateTime Anchor = new(2026, 1, 5, 12, 37, 21, DateTimeKind.Utc);

    private static long s_collectionId = Anchor.Ticks;

    private static readonly string[] s_aggregates = { "sum", "avg", "min", "max" };

    private static readonly string[] s_groupings =
    {
        "[\"object_name\"]", "[\"database_name\",\"object_name\"]", "[\"statement\"]", "[\"server\",\"object_name\"]",
    };

    [Fact]
    public async Task ARankedPanel_ReadsTheSameRows_ForEveryAggregate_Grouping_RouteAndScope()
    {
        await RunLiveAsync(async store =>
        {
            var compared = 0;
            foreach (var run in store.Runs())
            {
                foreach (var grouping in s_groupings)
                {
                    foreach (var aggregate in s_aggregates)
                    {
                        var panel = $"{{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"{aggregate}\",\"topN\":50,\"groupBy\":{grouping},\"viz\":\"table\"}}";
                        await AssertSameRowsAsync(store, run, panel);
                        compared++;
                    }

                    /* the Sum-mode ratio, a different unit, and a short top N whose last place is decided by the tie-break */
                    await AssertSameRowsAsync(store, run, $"{{\"source\":\"query_stats\",\"ratio\":\"query_avg_cpu_us\",\"topN\":50,\"groupBy\":{grouping},\"viz\":\"table\"}}");
                    await AssertSameRowsAsync(store, run, $"{{\"source\":\"query_stats\",\"measure\":\"query_elapsed_us\",\"aggregate\":\"sum\",\"unit\":\"s\",\"topN\":3,\"groupBy\":{grouping},\"viz\":\"table\"}}");
                    await AssertSameRowsAsync(store, run, $"{{\"source\":\"query_stats\",\"measure\":\"query_executions\",\"aggregate\":\"sum\",\"topN\":4,\"groupBy\":{grouping},\"viz\":\"table\"}}");
                    compared += 3;
                }
            }

            Assert.Equal(store.Runs().Count * s_groupings.Length * (s_aggregates.Length + 3), compared);
        });
    }

    [Fact]
    public async Task ASeries_AScalar_AFilter_AndAnOverlay_ReadTheSameRows_OnEveryRouteAndScope()
    {
        string[] panels =
        {
            /* a time series: the pre-aggregate keeps the bucket */
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"groupBy\":[\"object_name\"],\"viz\":\"table\"}",
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}",
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"avg\",\"timeBucket\":\"minute\",\"groupBy\":[\"statement\"],\"viz\":\"table\"}",
            /* a ranked time series, with and without the residual series */
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":3,\"groupBy\":[\"object_name\"],\"viz\":\"line\"}",
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":3,\"includeOther\":true,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"line\"}",
            "{\"source\":\"query_stats\",\"ratio\":\"query_avg_cpu_us\",\"timeBucket\":\"hour\",\"topN\":2,\"groupBy\":[\"object_name\"],\"viz\":\"line\"}",
            /* a scalar whose only module reference is a filter, and one whose filter matches nothing */
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"viz\":\"stat\",\"filters\":[{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"usp_%\"}]}",
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"min\",\"viz\":\"stat\",\"filters\":[{\"dimension\":\"object_name\",\"op\":\"eq\",\"value\":\"usp_NoSuchModule\"}]}",
            /* a fact filter (inside the pre-aggregate) beside module filters (outside the join), one with a fallback column */
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"object_name\"],\"viz\":\"table\",\"filters\":[{\"dimension\":\"database_name\",\"op\":\"neq\",\"value\":\"TieDb\"},{\"dimension\":\"statement\",\"op\":\"neq\",\"value\":\"usp_Pay\"},{\"dimension\":\"object_name\",\"op\":\"neq\",\"value\":\"usp_RestartOnly\"}]}",
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"max\",\"topN\":50,\"groupBy\":[\"database_name\"],\"viz\":\"table\",\"filters\":[{\"dimension\":\"statement\",\"op\":\"like\",\"value\":\"H%\"}]}",
            /* an overlay: both values decompose, over the same pre-aggregated rows */
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"filters\":[{\"dimension\":\"object_name\",\"op\":\"neq\",\"value\":\"(ad hoc)\"}],\"overlay\":{\"measure\":\"query_executions\",\"aggregate\":\"max\"}}",
            "{\"source\":\"query_stats\",\"ratio\":\"query_avg_elapsed_us\",\"timeBucket\":\"hour\",\"viz\":\"line\",\"filters\":[{\"dimension\":\"object_name\",\"op\":\"neq\",\"value\":\"(ad hoc)\"}],\"overlay\":{\"measure\":\"query_worker_us\",\"aggregate\":\"avg\"}}",
        };

        await RunLiveAsync(async store =>
        {
            foreach (var run in store.Runs())
            {
                foreach (var panel in panels)
                {
                    await AssertSameRowsAsync(store, run, panel, allowNoRows: true);
                }
            }
        });
    }

    /// <summary>A ranked series with no residual whose module filter gives the outer statement a WHERE of its own: the top-N membership
    /// test is joined to it with AND. The statement text is asserted to have that shape (the module filter's WHERE on the outer query,
    /// then the membership on the next line), and its rows equal the join-first statement's on every route and scope.</summary>
    [Fact]
    public async Task ARankedSeries_WithAModuleFilter_AndNoResidual_AddsTheTopNTestToTheFilter_AndReadsTheSameRows()
    {
        string[] panels =
        {
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":3,\"groupBy\":[\"object_name\"],\"viz\":\"line\",\"filters\":[{\"dimension\":\"object_name\",\"op\":\"neq\",\"value\":\"usp_Shared\"}]}",
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"max\",\"timeBucket\":\"hour\",\"topN\":2,\"groupBy\":[\"database_name\",\"statement\"],\"viz\":\"line\",\"filters\":[{\"dimension\":\"database_name\",\"op\":\"neq\",\"value\":\"TieDb\"},{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"usp_%\"}]}",
        };

        await RunLiveAsync(async store =>
        {
            foreach (var run in store.Runs())
            {
                foreach (var panel in panels)
                {
                    var (_, sql, _) = await AssertSameRowsAsync(store, run, panel);
                    Assert.True(
                        System.Text.RegularExpressions.Regex.IsMatch(sql, "\nWHERE [^\n]*m\\.object_name[^\n]*\n  AND EXISTS \\(SELECT 1 FROM topn AS t WHERE "),
                        $"the outer statement does not add the top-N test to its module filter: [{run.Name}] {panel}\n{sql}");
                }
            }
        });
    }

    /// <summary>The hash limit guard of the ad hoc fold. The store's map gets more module handles than a 64kB work_mem's hash limit holds
    /// by the planner's own estimate, and the panel runs in a transaction at that work_mem. Without the runner's prelude the fold's IN is a
    /// plain SubPlan (a scan of the keys per fact row); after it, on the same connection and transaction, the same EXPLAIN shows a
    /// hashed SubPlan, and the runner's rows on that connection equal the join-first statement's. The runner puts the multiplier back on a
    /// connection it shares, never lowers it, and leaves a statement without the fold alone.</summary>
    [Fact]
    public async Task TheAdHocFold_RunsUnderTheRunnersHashLimit_AndIsAHashedSubPlan_AtASmallWorkMem()
    {
        const string panel = "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";
        await RunLiveAsync(bulkModules: 6000, body: async store =>
        {
            var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panel)!, Array.Empty<string>());
            Assert.True(parseError is null, parseError);
            foreach (var run in new[] { "fleet 4 h, raw, map first", "fleet 24 h, hourly edges, map first" }.Select(name => store.Runs().Single(r => r.Name == name)))
            {
                var (folded, foldedError) = ComposeCompiler.Compile(plan!, store.Context(plan!, run));
                Assert.True(foldedError is null, foldedError);
                var (joinedFirst, joinedError) = ComposeCompiler.CompileCore(plan!, store.Context(plan!, run), singleScanRankedTimeSeries: true, moduleJoinPreAggregate: false);
                Assert.True(joinedError is null, joinedError);
                Assert.True(folded!.FoldsAdHocHandles, run.Name);
                Assert.Contains(AdHocFold, folded.Sql, StringComparison.Ordinal);
                Assert.False(joinedFirst!.FoldsAdHocHandles, run.Name);

                var expected = await ComposePanelRunner.RunComposedQueryAsync(store.DataSource, joinedFirst, 120, store.Ct);
                Assert.True(expected.Count > 0, run.Name);

                await using var connection = await store.DataSource.OpenConnectionAsync(store.Ct);
                await using var transaction = await connection.BeginTransactionAsync(store.Ct);
                await ExecuteAsync(connection, "SET LOCAL work_mem = '64kB'", store.Ct);
                var prior = await SettingAsync(connection, store.Ct);

                var unguarded = await ExplainAsync(connection, folded, store.Ct);
                Assert.True(unguarded.Contains("SubPlan", StringComparison.Ordinal) && !unguarded.Contains("hashed SubPlan", StringComparison.Ordinal),
                    $"the plant does not pass the 64kB hash limit, so the guard is not exercised: [{run.Name}]\n{unguarded}");

                var rows = await ComposePanelRunner.RunComposedQueryAsync(store.DataSource, folded, 120, store.Ct, connection);
                Assert.True(string.Equals(expected.ToJsonString(), rows.ToJsonString(), StringComparison.Ordinal),
                    $"rows differ: [{run.Name}]\nexpected {expected.ToJsonString()}\nactual   {rows.ToJsonString()}");
                Assert.Equal(prior, await SettingAsync(connection, store.Ct));

                Assert.Equal(prior, await ComposePanelRunner.RaiseModuleKeyHashLimitAsync(connection, folded, store.Ct));
                var guarded = await ExplainAsync(connection, folded, store.Ct);
                Assert.True(guarded.Contains("hashed SubPlan", StringComparison.Ordinal), $"the guarded fold is not a hashed SubPlan: [{run.Name}]\n{guarded}");
                Assert.Equal("1000", await SettingAsync(connection, store.Ct));

                /* a statement without the fold changes nothing, and a work_mem that already gives the limit keeps the multiplier */
                await ExecuteAsync(connection, "SET LOCAL hash_mem_multiplier = 2", store.Ct);
                Assert.Null(await ComposePanelRunner.RaiseModuleKeyHashLimitAsync(connection, joinedFirst, store.Ct));
                Assert.Equal("2", await SettingAsync(connection, store.Ct));
                await ExecuteAsync(connection, "SET LOCAL work_mem = '1GB'", store.Ct);
                Assert.Equal("2", await ComposePanelRunner.RaiseModuleKeyHashLimitAsync(connection, folded, store.Ct));
                Assert.Equal("2", await SettingAsync(connection, store.Ct));
                await ExecuteAsync(connection, "SET LOCAL work_mem = '4MB'", store.Ct);
                Assert.Equal("2", await ComposePanelRunner.RaiseModuleKeyHashLimitAsync(connection, folded, store.Ct));
                Assert.Equal("16", await SettingAsync(connection, store.Ct));
            }
        });
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string> SettingAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT current_setting('hash_mem_multiplier')", connection);
        return (string)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, ComposeCompiled compiled, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("EXPLAIN (VERBOSE) " + compiled.Sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter.Clone());
        }

        var lines = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            lines.Add(reader.GetString(0));
        }

        return string.Join("\n", lines);
    }

    [Fact]
    public async Task TheFleetPanel_ByDatabaseAndObject_CompilesThePreAggregatedShape_AndReadsThePlantedValues()
    {
        const string panel = "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":20,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";
        await RunLiveAsync(async store =>
        {
            var runs = store.Runs();
            var day = runs.Single(r => r.Name == "fleet 24 h, hourly edges, map first");
            var fourHours = runs.Single(r => r.Name == "fleet 4 h, raw, map first");

            foreach (var run in new[] { day, fourHours })
            {
                var (rows, sql, parameters) = await AssertSameRowsAsync(store, run, panel);
                Assert.Contains(MapFirst, sql, StringComparison.Ordinal);
                Assert.Equal(run == day, sql.Contains(Hybrid, StringComparison.Ordinal));
                /* the fact rows are grouped to the join key and the kept group column, the join reads those rows, and the value
                   combines the partial */
                Assert.Contains("GROUP BY f.server_name, " + AdHocFold + ", f.database_name\n) AS f\nLEFT JOIN m ON m.sql_handle = f.sql_handle AND m.server_name = f.server_name\nGROUP BY f.database_name, COALESCE(m.object_name, '(ad hoc)')", sql, StringComparison.Ordinal);
                Assert.Contains("SELECT f.server_name, " + AdHocFold + " AS sql_handle, f.database_name, ", sql, StringComparison.Ordinal);
                Assert.Contains("SUM(f.delta_worker_time) FILTER (WHERE f.sample_interval_seconds IS DISTINCT FROM 0) AS p0", sql, StringComparison.Ordinal);
                Assert.Contains("(CAST(SUM(f.p0) AS double precision)) * 1.0 / 1000.0 AS value", sql, StringComparison.Ordinal);

                var output = TestContext.Current.TestOutputHelper;
                output?.WriteLine($"-- 9960-sql-begin [{run.Name}]");
                output?.WriteLine(sql);
                output?.WriteLine($"-- 9960-sql-params [{run.Name}]");
                for (var i = 0; i < parameters.Count; i++)
                {
                    output?.WriteLine($"-- ${i + 1} {parameters[i].NpgsqlDbType}");
                }

                output?.WriteLine($"-- 9960-sql-end [{run.Name}]");

                /* reference values computed from the plant, in the panel's unit (ms), so two statements that were both wrong
                   cannot agree: the shared handle reads each server's own module name, every handle with no module row (several
                   in one server and database, the shared handle on the servers that have no module for it, an empty and a NULL
                   handle) reads '(ad hoc)', and a group with only restart rows is NULL and sorts last */
                Assert.Equal(store.ExpectedWorkerUs(run, "DbOne", "usp_Shared") / 1000.0, ValueOf(rows, "DbOne", "usp_Shared"));
                Assert.Equal(store.ExpectedWorkerUs(run, "DbOne", "usp_SharedOnB") / 1000.0, ValueOf(rows, "DbOne", "usp_SharedOnB"));
                Assert.Equal(store.ExpectedWorkerUs(run, "DbOne", "usp_Pay") / 1000.0, ValueOf(rows, "DbOne", "usp_Pay"));
                Assert.Equal(store.ExpectedWorkerUs(run, "DbOne", "(ad hoc)") / 1000.0, ValueOf(rows, "DbOne", "(ad hoc)"));
                Assert.Equal(store.ExpectedWorkerUs(run, "DbTwo", "usp_New") / 1000.0, ValueOf(rows, "DbTwo", "usp_New"));
                var restartModule = (JsonObject)rows[rows.Count - 2]!;
                Assert.Equal("usp_RestartOnly", restartModule["object_name"]!.GetValue<string>());
                Assert.Null(restartModule["value"]);
                /* an ad hoc handle whose every row is a restart row: its folded group exists, with no value */
                var last = (JsonObject)rows[rows.Count - 1]!;
                Assert.Equal("RestartDb", last["database_name"]!.GetValue<string>());
                Assert.Equal("(ad hoc)", last["object_name"]!.GetValue<string>());
                Assert.Null(last["value"]);
                /* the two tied groups keep the tie-break's order */
                var names = rows.OfType<JsonObject>().Select(r => r["object_name"]!.GetValue<string>()).ToList();
                Assert.Equal(names.IndexOf("usp_TieA") + 1, names.IndexOf("usp_TieB"));
            }
        });
    }

    [Fact]
    public async Task ANamedScope_KeepsTheScopedServersOnly_AndAnUnregisteredNameReadsItsOwnModule()
    {
        const string panel = "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"server\",\"object_name\"],\"viz\":\"table\"}";
        await RunLiveAsync(async store =>
        {
            var runs = store.Runs();
            var (scoped, scopedSql, _) = await AssertSameRowsAsync(store, runs.Single(r => r.Name == "servers A and B, 24 h"), panel);
            Assert.Contains(PreAggregated, scopedSql, StringComparison.Ordinal);
            Assert.Equal(new[] { ServerA, ServerB }, scoped.OfType<JsonObject>().Select(r => r["server"]!.GetValue<string>()).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());

            var (ghost, ghostSql, _) = await AssertSameRowsAsync(store, runs.Single(r => r.Name == "server A and an unregistered name, 24 h"), panel);
            Assert.Contains("m_unreg", ghostSql, StringComparison.Ordinal);
            Assert.Equal(new[] { ServerA, Ghost }.OrderBy(n => n, StringComparer.Ordinal).ToArray(), ghost.OfType<JsonObject>().Select(r => r["server"]!.GetValue<string>()).Distinct().OrderBy(n => n, StringComparer.Ordinal).ToArray());
            Assert.Contains(ghost.OfType<JsonObject>(), r => r["server"]!.GetValue<string>() == Ghost && r["object_name"]!.GetValue<string>() == "usp_Ghost");
        });
    }

    /* ---- helpers ---- */

    /// <summary>One compile context of the plant: a name, the server scope, the window start, and whether the run has a module-map
    /// watermark and a count-guard verdict.</summary>
    private sealed record Run(string Name, string[]? Servers, string[]? Unregistered, DateTime Start, bool Watermark, bool Verdict);

    private static double? ValueOf(JsonArray rows, string database, string objectName) =>
        rows.OfType<JsonObject>().Single(r => r["database_name"]?.GetValue<string>() == database && r["object_name"]?.GetValue<string>() == objectName)["value"]?.GetValue<double>();

    /// <summary>Compiles <paramref name="panelJson"/> for <paramref name="run"/> with the pre-aggregate and without it, checks which
    /// statement is which and that both bind the same parameters, runs both, and asserts the same rows in the same order. Returns the
    /// pre-aggregated statement's rows, text and parameters.</summary>
    private static async Task<(JsonArray Rows, string Sql, IReadOnlyList<NpgsqlParameter> Parameters)> AssertSameRowsAsync(Store store, Run run, string panelJson, bool allowNoRows = false)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(panelJson)!, Array.Empty<string>());
        Assert.True(parseError is null, $"{parseError} [{panelJson}]");
        Assert.True(plan!.UsesModuleJoin, panelJson);

        var context = store.Context(plan, run);
        var (rewritten, rewrittenError) = ComposeCompiler.Compile(plan, context);
        Assert.True(rewrittenError is null, rewrittenError);
        var (joinedFirst, joinedError) = ComposeCompiler.CompileCore(plan, store.Context(plan, run), singleScanRankedTimeSeries: true, moduleJoinPreAggregate: false);
        Assert.True(joinedError is null, joinedError);

        var label = $"[{run.Name}] {panelJson}";
        Assert.True(rewritten!.Sql.Contains(PreAggregated, StringComparison.Ordinal), $"not pre-aggregated: {label}\n{rewritten.Sql}");
        Assert.False(joinedFirst!.Sql.Contains(PreAggregated, StringComparison.Ordinal), $"the baseline is pre-aggregated: {label}");
        Assert.Equal(rewritten.Route.Tier, joinedFirst.Route.Tier);
        Assert.Equal(run.Verdict && rewritten.Route.Tier == ComposeSourceTier.HourlyRawEdges, rewritten.Sql.Contains(Hybrid, StringComparison.Ordinal));
        Assert.Equal(run.Watermark, rewritten.Sql.Contains(MapFirst, StringComparison.Ordinal));
        Assert.Equal(
            joinedFirst.Parameters.Select(p => (p.NpgsqlDbType, Text: ParameterText(p))).ToArray(),
            rewritten.Parameters.Select(p => (p.NpgsqlDbType, Text: ParameterText(p))).ToArray());

        var expected = await ReadRowsAsync(store, joinedFirst.Sql, joinedFirst.Parameters);
        var actual = await ReadRowsAsync(store, rewritten.Sql, rewritten.Parameters);
        Assert.True(allowNoRows || expected.Count > 0, $"the baseline returned no rows: {label}");
        Assert.True(string.Equals(expected.ToJsonString(), actual.ToJsonString(), StringComparison.Ordinal),
            $"rows differ: {label}\nexpected {expected.ToJsonString()}\nactual   {actual.ToJsonString()}\n{rewritten.Sql}");
        return (actual, rewritten.Sql, rewritten.Parameters);
    }

    private static string ParameterText(NpgsqlParameter parameter) => parameter.Value switch
    {
        null => "null",
        string[] names => string.Join("|", names),
        int[] ids => string.Join("|", ids),
        DateTime instant => instant.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture),
        var other => Convert.ToString(other, System.Globalization.CultureInfo.InvariantCulture) ?? "null",
    };

    private static async Task<JsonArray> ReadRowsAsync(Store store, string sql, IReadOnlyList<NpgsqlParameter> parameters)
    {
        await using var connection = await store.DataSource.OpenConnectionAsync(store.Ct);
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        var rows = new JsonArray();
        await using var reader = await command.ExecuteReaderAsync(store.Ct);
        while (await reader.ReadAsync(store.Ct))
        {
            var row = new JsonObject();
            for (var i = 0; i < reader.FieldCount; i++)
            {
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : DarlingWebEndpoints.DbValueToJson(reader.GetValue(i));
            }

            rows.Add(row);
        }

        return rows;
    }

    private static async Task RunLiveAsync(Func<Store, Task> body, int bulkModules = 0)
    {
        await using var store = await StoreAsync(bulkModules);
        var bodySucceeded = false;
        try
        {
            await body(store);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(store.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    private static async Task<Store> StoreAsync(int bulkModules)
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #9960 module pre-aggregate live test.");
        var ct = TestContext.Current.CancellationToken;

        var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct, emptyStore: true);
        NpgsqlConnection? connection = null;
        NpgsqlDataSource? dataSource = null;
        try
        {
            connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
            Assert.SkipWhen(!timescaleEnabled, "The hourly-raw-edges route reads a continuous aggregate: TimescaleDB is required.");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
            foreach (var name in new[] { ServerA, ServerB, ServerC })
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, s_serverIds[name], name, ct);
            }

            await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
            Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));

            dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
            var store = new Store(scratch, connection, dataSource, ct);
            await store.PlantAsync(bulkModules);
            return store;
        }
        catch
        {
            if (dataSource is not null)
            {
                await dataSource.DisposeAsync();
            }

            if (connection is not null)
            {
                await connection.DisposeAsync();
            }

            await scratch.DisposeAsync();
            throw;
        }
    }

    private static DateTime FloorHour(DateTime value) => new(value.Year, value.Month, value.Day, value.Hour, 0, 0, DateTimeKind.Utc);

    /// <summary>One planted query_stats row, kept so a fact can compute a reference value from the plant.</summary>
    private sealed record Planted(string Server, string Database, string? Handle, DateTime At, long? Worker, int? Interval);

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;
        private readonly List<Planted> _rows = new();
        private readonly Dictionary<(string Server, string Handle), (string Name, DateTime At)> _modules = new();
        private RollupAvailability _rollups;
        private RollupCoverage? _coverage;
        private DateTime? _watermark;

        public Store(ScratchPostgres scratch, NpgsqlConnection connection, NpgsqlDataSource dataSource, CancellationToken ct)
        {
            _scratch = scratch;
            Connection = connection;
            DataSource = dataSource;
            Ct = ct;
            WindowEnd = Anchor;
            DayStart = Anchor.AddHours(-24);
            FourHourStart = Anchor.AddHours(-4);
            H2 = FloorHour(Anchor);
            H1 = FloorHour(DayStart).AddHours(1);
        }

        public string ConnectionString => _scratch.ConnectionString;
        public NpgsqlConnection Connection { get; }
        public NpgsqlDataSource DataSource { get; }
        public CancellationToken Ct { get; }
        public DateTime WindowEnd { get; }
        public DateTime DayStart { get; }
        public DateTime FourHourStart { get; }
        public DateTime H1 { get; }
        public DateTime H2 { get; }

        /// <summary>The compile contexts every comparison runs under.</summary>
        public IReadOnlyList<Run> Runs() => new[]
        {
            new Run("fleet 24 h, hourly edges, map first", null, null, DayStart, Watermark: true, Verdict: true),
            new Run("fleet 24 h, hourly edges, window ranking", null, null, DayStart, Watermark: false, Verdict: true),
            new Run("fleet 24 h, raw, map first", null, null, DayStart, Watermark: true, Verdict: false),
            new Run("fleet 4 h, raw, map first", null, null, FourHourStart, Watermark: true, Verdict: false),
            new Run("fleet 4 h, raw, window ranking", null, null, FourHourStart, Watermark: false, Verdict: false),
            new Run("servers A and B, 24 h", new[] { ServerA, ServerB }, null, DayStart, Watermark: true, Verdict: true),
            new Run("server A and an unregistered name, 24 h", new[] { ServerA, Ghost }, new[] { Ghost }, DayStart, Watermark: true, Verdict: false),
        };

        /// <summary>The run context of <paramref name="run"/> for <paramref name="plan"/>. A run with a verdict carries the router's own
        /// candidate for the plan as a passed count guard, when the plan has one (a SUM, MAX or MIN of a delta measure).</summary>
        public ComposeRunContext Context(PanelPlan plan, Run run)
        {
            ComposeHourlyEdgesVerdict? verdict = null;
            if (run.Verdict
                && ComposeSourceRouter.HourlyRawEdgesCandidate(plan, Anchor, run.Start, WindowEnd, _rollups, _coverage!, ComposeSourceRouter.IsFleetScope(run.Servers), out _) is { } candidate)
            {
                verdict = new ComposeHourlyEdgesVerdict(candidate.SourceTable, candidate.HourStartUtc, candidate.HourEndUtc, run.Servers);
            }

            return new ComposeRunContext(run.Servers, run.Start, WindowEnd, ComposeRunContext.NoVariables, _rollups, Anchor, _coverage!,
                HourlyEdges: verdict, ModuleMapThrough: run.Watermark ? _watermark : null, UnregisteredServers: run.Unregistered);
        }

        /// <summary>The planted sum of delta_worker_time (microseconds) the panel reads for (<paramref name="database"/>, <paramref name="objectName"/>)
        /// in <paramref name="run"/>'s window and scope: measured rows only, each row named by its own server's module for its handle
        /// when that module's newest procedure_stats row is inside the window, and '(ad hoc)' otherwise.</summary>
        public double ExpectedWorkerUs(Run run, string database, string objectName) =>
            _rows.Where(r => r.At >= run.Start && r.At <= WindowEnd
                    && (run.Servers is null || run.Servers.Contains(r.Server, StringComparer.Ordinal))
                    && r.Database == database
                    && r.Interval != 0
                    && r.Worker is not null
                    && (r.Handle is not null && _modules.TryGetValue((r.Server, r.Handle), out var module) && module.At >= run.Start ? module.Name : "(ad hoc)") == objectName)
                .Sum(r => (double)r.Worker!.Value);

        public async Task PlantAsync(int bulkModules)
        {
            /* one measured row per whole hour, so the hourly rollup has a bucket at h1 and a measured ceiling at h2 */
            for (var h = H1; h < H2; h = h.AddHours(1))
            {
                await QsAsync(ServerA, "AnchorDb", "HA", "0xA", h.AddMinutes(30), 1000, 300);
            }

            /* the modules the map learns at its refresh, inside both windows: one handle shared by three servers (two names and no
               module row), a module on the unregistered name, two tied modules and one that only ever restarts. usp_Pay is one name
               on two servers, and server B's row is older than the 4 h window, so that window reads its handle as '(ad hoc)'. */
            var known = H2.AddHours(-2);
            await ProcAsync(ServerA, "DbOne", "usp_Shared", "0xS", known);
            await ProcAsync(ServerB, "DbOne", "usp_SharedOnB", "0xS", known);
            await ProcAsync(ServerA, "DbOne", "usp_Pay", "0xP", known);
            await ProcAsync(ServerB, "DbOne", "usp_Pay", "0xP", H1.AddHours(2));
            await ProcAsync(Ghost, "GhostDb", "usp_Ghost", "0xG", known);
            await ProcAsync(ServerA, "TieDb", "usp_TieA", "0xT1", known);
            await ProcAsync(ServerA, "TieDb", "usp_TieB", "0xT2", known);
            await ProcAsync(ServerC, "DbTwo", "usp_RestartOnly", "0xR", known);
            await ProcAsync(ServerC, "DbTwo", "usp_OldName", "0xN", known);
            await ProcAsync(ServerA, "DbOne", "usp_Filler", "0xW", H2.AddHours(-1));

            /* both raw edges of the 24 h window, whole hours between them, and three instants inside the 4 h window */
            DateTime[] instants =
            {
                DayStart.AddMinutes(5), DayStart.AddMinutes(20), H1.AddMinutes(7), H1.AddHours(3).AddMinutes(7), H1.AddHours(10).AddMinutes(7),
                H1.AddHours(19).AddMinutes(7), H1.AddHours(21).AddMinutes(7), H1.AddHours(22).AddMinutes(7), H2.AddMinutes(10), H2.AddMinutes(30),
            };
            var n = 0;
            foreach (var at in instants)
            {
                foreach (var server in new[] { ServerA, ServerB, ServerC, Ghost })
                {
                    n++;
                    await QsAsync(server, "DbOne", "HS", "0xS", at, 1000 + 37 * n, 300);
                    await QsAsync(server, "DbOne", "HS2", "0xS", at.AddSeconds(30), 90 + n, 60);
                    await QsAsync(server, "DbOne", "HP", "0xP", at, 500 + 11 * n, 300);
                    await QsAsync(server, "DbOne", "HNone", "0xNone", at, 70 + 3 * n, 300);
                    await QsAsync(server, "DbTwo", "HS", "0xS", at, 20 + n, null);
                    await QsAsync(server, "DbTwo", "HN", "0xN", at, 300 + 7 * n, 300);
                }

                /* a NULL handle, a NULL delta, a restart row that carries a value the filter must leave out, a handle on the
                   unregistered name, and a module whose every row is a restart row */
                await QsAsync(ServerA, "DbOne", "HNull", null, at, 40 + n, 300);
                await QsAsync(ServerB, "DbOne", "HP", "0xP", at.AddSeconds(45), null, 300);
                await QsAsync(ServerA, "DbOne", "HS", "0xS", at.AddSeconds(50), 999_999, 0);
                await QsAsync(Ghost, "GhostDb", "HG", "0xG", at, 800 + n, 300);
                await QsAsync(ServerC, "DbTwo", "HR", "0xR", at, 0, 0);

                /* the ad hoc fold: three more ad hoc handles in one (server, database), two of them under one query_hash, an
                   empty handle, and an ad hoc handle whose every row is a restart row, alone in its database */
                await QsAsync(ServerA, "DbOne", "HAd1", "0xAd1", at, 210 + n, 300);
                await QsAsync(ServerA, "DbOne", "HAd2", "0xAd2", at, 310 + (2 * n), 300);
                await QsAsync(ServerA, "DbOne", "HAd2", "0xAd3", at.AddSeconds(5), 410 + (3 * n), 60);
                await QsAsync(ServerB, "DbOne", "HEmpty", string.Empty, at, 55 + n, 300);
                await QsAsync(ServerB, "RestartDb", "HRA", "0xRA", at, 777, 0);
            }

            /* two modules with the same total, in the 4 h window */
            await QsAsync(ServerA, "TieDb", "HT1", "0xT1", H2.AddMinutes(12), 5000, 300);
            await QsAsync(ServerA, "TieDb", "HT2", "0xT2", H2.AddMinutes(13), 2500, 300);
            await QsAsync(ServerA, "TieDb", "HT2", "0xT2", H2.AddMinutes(14), 2500, 300);

            /* the map's refresh, with the anchor as its clock; its watermark is the filler row's hour */
            if (bulkModules > 0)
            {
                await PlantManyModulesAsync(bulkModules, known);
            }

            await DarlingModuleMap.RefreshRecentAsync(Connection, null, Anchor, Ct);
            _watermark = await DarlingModuleMap.ReadWatermarkAsync(Connection, Ct);
            Assert.Equal(H2.AddHours(-1), _watermark);

            /* after the refresh: a rename the map has not seen (the overlay carries it) */
            await ProcAsync(ServerC, "DbTwo", "usp_New", "0xN", H2.AddMinutes(20));

            await using (var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{TimescaleSupport.QueryStatsIntervalHourlyView}'::regclass, $1::timestamp, $2::timestamp)", Connection))
            {
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(H1.AddHours(-2), DateTimeKind.Unspecified));
                refresh.Parameters.AddWithValue(DateTime.SpecifyKind(H2, DateTimeKind.Unspecified));
                await refresh.ExecuteNonQueryAsync(Ct);
            }

            if (bulkModules > 0)
            {
                await using var analyze = new NpgsqlCommand("ANALYZE collect.procedure_stats; ANALYZE collect.query_stats; ANALYZE collect.module_map", Connection);
                await analyze.ExecuteNonQueryAsync(Ct);
            }

            (_rollups, _coverage) = await ComposeStoreAvailability.GetRollupsAsync(DataSource, Ct);
        }

        /// <summary>Plants <paramref name="count"/> module handles on server A at <paramref name="known"/>, before the map's refresh (so
        /// the map learns them and the planner estimates m's map arm from the map's statistics), each with a fact row half an hour later,
        /// plus as many ad hoc fact rows. The caller refreshes the statistics after the map refresh.</summary>
        private async Task PlantManyModulesAsync(int count, DateTime known)
        {
            var at = DateTime.SpecifyKind(known, DateTimeKind.Unspecified);
            var factAt = DateTime.SpecifyKind(known.AddMinutes(30), DateTimeKind.Unspecified);
            var firstId = Interlocked.Add(ref s_collectionId, 3L * count) - (3L * count);
            await using (var procs = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT $1 + g, $2, $3, $4, 'BulkDb', 'dbo', 'usp_Bulk' || g, '0xB' || g, 1, 1, 1, 300
FROM generate_series(1, $5) AS g", Connection))
            {
                procs.Parameters.AddWithValue(firstId);
                procs.Parameters.AddWithValue(at);
                procs.Parameters.AddWithValue(s_serverIds[ServerA]);
                procs.Parameters.AddWithValue(ServerA);
                procs.Parameters.AddWithValue(count);
                await procs.ExecuteNonQueryAsync(Ct);
            }

            await using (var facts = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT $1 + $5 + g, $2, $3, $4, 'BulkDb', 'HB' || (g % 40), CASE WHEN g <= $5 THEN '0xB' || g ELSE '0xBAd' || g END, 10 + g, (3 * g) + 1, 1, 300
FROM generate_series(1, 2 * $5) AS g", Connection))
            {
                facts.Parameters.AddWithValue(firstId);
                facts.Parameters.AddWithValue(factAt);
                facts.Parameters.AddWithValue(s_serverIds[ServerA]);
                facts.Parameters.AddWithValue(ServerA);
                facts.Parameters.AddWithValue(count);
                await facts.ExecuteNonQueryAsync(Ct);
            }
        }

        private async Task QsAsync(string server, string database, string hash, string? handle, DateTime at, long? worker, int? interval)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)", Connection);
            insert.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(s_serverIds[server]);
            insert.Parameters.AddWithValue(server);
            insert.Parameters.AddWithValue(database);
            insert.Parameters.AddWithValue(hash);
            insert.Parameters.Add(new NpgsqlParameter { Value = handle is null ? DBNull.Value : handle, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Text });
            insert.Parameters.Add(new NpgsqlParameter { Value = worker is long w ? w : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            insert.Parameters.Add(new NpgsqlParameter { Value = worker is long e ? (e * 3) + 1 : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Bigint });
            insert.Parameters.AddWithValue(worker is long x ? (x % 7) + 1 : 1L);
            insert.Parameters.Add(new NpgsqlParameter { Value = interval is int i ? i : DBNull.Value, NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer });
            await insert.ExecuteNonQueryAsync(Ct);
            _rows.Add(new Planted(server, database, handle, at, worker, interval));
        }

        private async Task ProcAsync(string server, string database, string objectName, string handle, DateTime at)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, 'dbo', $6, $7, 1, 1, 1, 300)", Connection);
            insert.Parameters.AddWithValue(Interlocked.Increment(ref s_collectionId));
            insert.Parameters.AddWithValue(DateTime.SpecifyKind(at, DateTimeKind.Unspecified));
            insert.Parameters.AddWithValue(s_serverIds[server]);
            insert.Parameters.AddWithValue(server);
            insert.Parameters.AddWithValue(database);
            insert.Parameters.AddWithValue(objectName);
            insert.Parameters.AddWithValue(handle);
            await insert.ExecuteNonQueryAsync(Ct);
            /* the newest row of a (server, handle) names it */
            if (!_modules.TryGetValue((server, handle), out var newest) || at >= newest.At)
            {
                _modules[(server, handle)] = (objectName, at);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await DataSource.DisposeAsync();
            await Connection.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }
}
