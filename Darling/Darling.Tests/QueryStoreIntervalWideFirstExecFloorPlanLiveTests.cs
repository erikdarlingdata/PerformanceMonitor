/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4605: the plan pin for the <c>first_execution_time</c> floor. The compiled ELIGIBLE compose read of
/// <c>collect.query_store_interval_wide</c> filters <c>collection_time</c>, which neither of the table's indexes
/// serves (the unique key leads with <c>server_id</c>), so on a fleet-wide day read the planner had nothing but a
/// seq scan of the whole table. The floor gives it <c>idx_query_store_interval_wide_first_exec</c>. This seeds a
/// month of intervals, one per minute, reads the last twelve hours (the floor is a lower bound on
/// <c>first_execution_time</c>, so it is selective for a recent window), <c>ANALYZE</c>s, and <c>EXPLAIN</c>s the
/// compiled SQL with its binds.
/// <para><b>The form used is the forced one</b>: <c>SET LOCAL enable_seqscan = off</c> inside a transaction, so the
/// pin does not depend on the planner's cost model at whatever table size CI affords. Under it the plan must read
/// the table through the first_exec index (Index Scan, Index Only Scan or Bitmap Index Scan) and hold no Seq Scan
/// on it. The control is the same statement with the floor predicate cut out: nothing can use the first_exec
/// index then, so a plan that still names it would mean the pin proves nothing.</para>
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it, so it cannot race
   live collection. */
public sealed class QueryStoreIntervalWideFirstExecFloorPlanLiveTests
{
    private const int ServerId = -46053;
    private const string ServerName = "qswide-plan-1";
    private const string FirstExecIndex = "idx_query_store_interval_wide_first_exec";
    private const string WideTable = "query_store_interval_wide";

    private static readonly DateTime WindowEnd = new(2026, 9, 30, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = WindowEnd.AddHours(-12);

    private const string PanelJson =
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"viz\":\"line\"}";

    [Fact]
    public async Task EligibleComposeRead_ReadsTheWideTableThroughTheFirstExecIndex_AndTheFloorlessControlCannot()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 first_execution_time floor plan pin.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        /* A month of intervals ending at the window's end, one per minute (43,200 rows): the floor keeps the last
           twelve hours plus the margin, about 5% of them. */
        await using (var seed = new NpgsqlCommand(@"
INSERT INTO collect.query_store_interval_wide
(
    collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
    last_execution_time, query_hash, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc
)
SELECT
    g.t + INTERVAL '5 minutes', $1, 'qsPlan', row_number() OVER (ORDER BY g.t), 1, 'Regular', g.t,
    g.t + INTERVAL '4 minutes', '0x00000001', 1, 100, row_number() OVER (ORDER BY g.t), g.t
FROM generate_series($2::timestamp, $3::timestamp, INTERVAL '1 minute') AS g(t);", connection))
        {
            seed.Parameters.AddWithValue(ServerId);
            seed.Parameters.AddWithValue(DateTime.SpecifyKind(WindowEnd.AddDays(-30), DateTimeKind.Unspecified));
            seed.Parameters.AddWithValue(DateTime.SpecifyKind(WindowEnd.AddMinutes(-5), DateTimeKind.Unspecified));
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using (var analyze = new NpgsqlCommand("ANALYZE collect.query_store_interval_wide", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        var floorPredicate = " AND w.first_execution_time >= $1 - " + QueryStoreIntervalWide.PurgeEdgeMarginSql;
        var withFloor = Compile().Sql;
        Assert.Contains(floorPredicate, withFloor, StringComparison.Ordinal);

        var plan = await ExplainAsync(connection, Compile(), withFloor, ct);
        Assert.Contains(plan, n => n.Index == FirstExecIndex && (n.Type is "Index Scan" or "Index Only Scan" or "Bitmap Index Scan"));
        Assert.DoesNotContain(plan, n => n.Type == "Seq Scan" && n.Relation == WideTable);

        /* The control: the same statement without the floor. Nothing can use the first_exec index for it. */
        var floorless = withFloor.Replace(floorPredicate, string.Empty, StringComparison.Ordinal);
        Assert.NotEqual(withFloor, floorless);
        var controlPlan = await ExplainAsync(connection, Compile(), floorless, ct);
        Assert.DoesNotContain(controlPlan, n => n.Index == FirstExecIndex);
    }

    private static ComposeCompiled Compile()
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(PanelJson)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var context = new ComposeRunContext(
            null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown,
            QueryStoreWideEligible: true);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
        Assert.True(compileError is null, compileError);
        return compiled!;
    }

    /// <summary>EXPLAINs <paramref name="sql"/> with <paramref name="compiled"/>'s binds under
    /// <c>SET LOCAL enable_seqscan = off</c> and returns every plan node's type, relation and index name.</summary>
    private static async Task<List<(string Type, string? Relation, string? Index)>> ExplainAsync(
        NpgsqlConnection connection, ComposeCompiled compiled, string sql, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await using (var set = new NpgsqlCommand("SET LOCAL enable_seqscan = off", connection, transaction))
        {
            await set.ExecuteNonQueryAsync(ct);
        }

        string json;
        await using (var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection, transaction))
        {
            foreach (var parameter in compiled.Parameters)
            {
                explain.Parameters.Add(parameter);
            }

            json = (string)(await explain.ExecuteScalarAsync(ct))!;
        }

        await transaction.RollbackAsync(ct);

        using var document = JsonDocument.Parse(json);
        var nodes = new List<(string Type, string? Relation, string? Index)>();
        Collect(document.RootElement[0].GetProperty("Plan"), nodes);
        return nodes;
    }

    private static void Collect(JsonElement node, List<(string Type, string? Relation, string? Index)> nodes)
    {
        nodes.Add((
            node.GetProperty("Node Type").GetString()!,
            node.TryGetProperty("Relation Name", out var relation) ? relation.GetString() : null,
            node.TryGetProperty("Index Name", out var index) ? index.GetString() : null));

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Collect(child, nodes);
            }
        }
    }
}
