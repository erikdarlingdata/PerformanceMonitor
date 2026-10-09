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
using System.Text;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

public sealed class ComposeQueryStoreStampPlanLiveTests
{
    internal sealed record PlanNode(string Type, string? Relation, string? Index, long Loops, double Rows, string Conditions, int Depth, string Path);

    /// <summary>Compiles a panel on the stamp route and runs it under EXPLAIN (ANALYZE, FORMAT JSON); returns every plan node, flattened.</summary>
    internal static async Task<(string Sql, IReadOnlyList<PlanNode> Nodes)> ExplainAsync(
        NpgsqlConnection connection, string planJson, ComposeRunContext context, CancellationToken ct)
    {
        var plan = QueryStoreRankedHarness.Parse(planJson);
        var (compiled, error) = QueryStoreRankedHarness.Product(plan, context);
        Assert.True(error is null, error);
        await using var command = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + compiled!.Sql, connection) { CommandTimeout = 120 };
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter);
        }

        var text = Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture)!;
        var root = (JsonArray)JsonNode.Parse(text)!;
        var nodes = new List<PlanNode>();
        Walk(root[0]!["Plan"]!, 0, string.Empty, nodes);
        return (compiled.Sql, nodes);
    }

    private static void Walk(JsonNode node, int depth, string parentPath, List<PlanNode> into)
    {
        var type = node["Node Type"]!.GetValue<string>();
        var relation = node["Relation Name"]?.GetValue<string>();
        var path = parentPath + "/" + type + (relation is null ? string.Empty : "(" + relation + ")");
        var conditions = string.Join(" ;; ", new[] { "Index Cond", "Filter", "Recheck Cond", "Join Filter", "Hash Cond", "Merge Cond" }
            .Select(k => node[k]?.GetValue<string>() is { } v ? k + ": " + v : null).Where(x => x is not null));
        into.Add(new PlanNode(type, relation, node["Index Name"]?.GetValue<string>(), node["Actual Loops"]?.GetValue<long>() ?? 1,
            node["Actual Rows"]?.GetValue<double>() ?? 0, conditions, depth, path));
        if (node["Plans"] is JsonArray children)
        {
            foreach (var child in children)
            {
                Walk(child!, depth + 1, path, into);
            }
        }
    }

    internal static string Dump(IReadOnlyList<PlanNode> nodes)
    {
        var text = new StringBuilder();
        foreach (var n in nodes)
        {
            text.Append(new string(' ', n.Depth * 2)).Append(n.Type).Append(' ').Append(n.Relation).Append(' ').Append(n.Index)
                .Append(" loops=").Append(n.Loops).Append(" rows=").Append(n.Rows).Append(" | ").AppendLine(n.Conditions);
        }

        return text.ToString();
    }

    private const string Panel = "{\"source\":\"query_store_stats\",\"ratio\":\"qs_avg_duration_us\",\"topN\":4,\"groupBy\":[\"module_name\"],\"timeBucket\":\"hour\",\"includeOther\":true,\"viz\":\"line\"}";

    private static bool IsWide(PlanNode n) => n.Relation is not null && n.Relation.StartsWith("query_store_interval_wide", StringComparison.Ordinal);

    private static string Literal(DateTime value) => "'" + value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) + "'";

    /// <summary>The wide-table scans of the TAIL arm: the ones that carry the lower bound at the stamp-through instant.</summary>
    private static List<PlanNode> TailScans(IReadOnlyList<PlanNode> nodes, DateTime through) =>
        nodes.Where(n => IsWide(n) && n.Conditions.Contains("collection_time >= " + Literal(through), StringComparison.Ordinal)).ToList();

    [Fact]
    public async Task TheCompiledRoute_ReadsTheRollupThroughItsIndex_TheTailIsBoundedBelowByStampThrough_AndTheStaleArmReadsNothingWhenNoPairIsStale()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            await ComposeStampLiveSupport.ExecAsync(connection, "ANALYZE collect.query_store_interval_wide; ANALYZE collect.query_store_compose_stamp; ANALYZE collect.query_store_compose_stamp_built; ANALYZE collect.query_store_compose_stamp_hours; ANALYZE collect.servers", ct);
            Assert.True(await ComposeStampLiveSupport.CountAsync(connection, "SELECT count(*) FROM collect.query_store_compose_stamp_hours", ct) >= 10, "the seed has 10 or more built hours");
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = await ComposeStampLiveSupport.StampThroughAsync(connection, start, end, ct);
            Assert.Equal(hourNow.AddHours(-2), through);
            var tailRows = await ComposeStampLiveSupport.CountAsync(connection,
                $"SELECT count(*) FROM collect.query_store_interval_wide WHERE collection_time >= TIMESTAMP '{ComposeStampLiveSupport.At(through!.Value)}' AND collection_time <= TIMESTAMP '{ComposeStampLiveSupport.At(end)}'", ct);
            Assert.True(tailRows > 100);

            foreach (var scope in new IReadOnlyList<string>?[] { null, new[] { "srv1" } })
            {
                var label = scope is null ? "fleet" : "srv1";
                var context = QueryStoreRankedHarness.WideContext(start, end, scope) with { QueryStoreStampThrough = through };

                /* The natural plan: no stale pair, so the stale arm's wide scan never starts (zero loops), and the tail scans the wide table
                   from the stamp-through instant only. */
                var (sql, nodes) = await ExplainAsync(connection, Panel, context, ct);
                Assert.Contains("query_store_compose_stamp", sql, StringComparison.Ordinal);
                var tail = TailScans(nodes, through.Value);
                Assert.True(tail.Count > 0, $"{label}: no wide-table scan carries the stamp-through lower bound\n{Dump(nodes)}");
                var wideScans = nodes.Where(n => IsWide(n) && n.Type.Contains("Scan", StringComparison.Ordinal)).ToList();
                var stale = wideScans.Except(tail).ToList();
                Assert.True(stale.Count > 0, $"{label}: the stale arm's wide scan is missing from the plan\n{Dump(nodes)}");
                Assert.True(stale.All(n => n.Loops == 0), $"{label}: the stale arm read the wide table although no pair is stale\n{Dump(nodes)}");
                Assert.True(tail.All(n => n.Loops == 1));
                Assert.Equal(scope is null ? tailRows : await ComposeStampLiveSupport.CountAsync(connection,
                    $"SELECT count(*) FROM collect.query_store_interval_wide WHERE server_id = 1 AND collection_time >= TIMESTAMP '{ComposeStampLiveSupport.At(through.Value)}' AND collection_time <= TIMESTAMP '{ComposeStampLiveSupport.At(end)}'", ct),
                    (long)tail.Sum(n => n.Rows));

                /* The rollup arm: with a sequential scan ruled out (the seed's rollup is a few thousand rows, where a seq scan is the cheaper
                   plan), its predicates are served by the (collection_time, server_id) index, with both bounds in the index condition. */
                await ComposeStampLiveSupport.ExecAsync(connection, "SET enable_seqscan = off", ct);
                try
                {
                    var (_, forced) = await ExplainAsync(connection, Panel, context, ct);
                    var rollup = forced.Where(n => n.Index == "ix_query_store_compose_stamp_time_server").ToList();
                    Assert.True(rollup.Count > 0, $"{label}: the rollup arm does not read through ix_query_store_compose_stamp_time_server\n{Dump(forced)}");
                    Assert.True(rollup.Any(n => n.Conditions.Contains("collection_time >= " + Literal(start.AddSeconds(0)), StringComparison.Ordinal)
                        && n.Conditions.Contains("collection_time < " + Literal(through.Value), StringComparison.Ordinal)), $"{label}: the index condition lacks a bound\n{Dump(forced)}");
                }
                finally
                {
                    await ComposeStampLiveSupport.ExecAsync(connection, "RESET enable_seqscan", ct);
                }
            }

            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }

    [Fact]
    public async Task TheStaleArm_ProbesTheWideTableOncePerStalePairInScope_NotOncePerPair()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ComposeStampLiveSupport.BaseConnectionString), ComposeStampLiveSupport.SkipReason);
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection, source, hourNow) = await ComposeStampLiveSupport.ArrangeAsync(ct);
        var bodySucceeded = false;
        try
        {
            /* Two pairs turn stale after the lookup: server 1 ten hours ago, server 3 fifteen hours ago. */
            await ComposeStampLiveSupport.InsertRowAsync(connection, 1, hourNow.AddHours(-10).AddMinutes(55), 9_000_001, ct);
            await ComposeStampLiveSupport.InsertRowAsync(connection, 3, hourNow.AddHours(-15).AddMinutes(55), 9_000_002, ct);
            await ComposeStampLiveSupport.ExecAsync(connection, "ANALYZE collect.query_store_interval_wide; ANALYZE collect.query_store_compose_stamp; ANALYZE collect.query_store_compose_stamp_built; ANALYZE collect.servers", ct);
            var start = hourNow.AddHours(-28).AddMinutes(17);
            var end = hourNow.AddMinutes(-40);
            var through = hourNow.AddHours(-2);

            foreach (var (scope, expectedProbes) in new[] { ((IReadOnlyList<string>?)null, 2L), (new[] { "srv1" }, 1L), (new[] { "srv2" }, 0L) })
            {
                var context = QueryStoreRankedHarness.WideContext(start, end, scope) with { QueryStoreStampThrough = through };
                var (_, nodes) = await ExplainAsync(connection, Panel, context, ct);
                var stale = nodes.Where(n => IsWide(n) && n.Type.Contains("Scan", StringComparison.Ordinal)).Except(TailScans(nodes, through)).ToList();
                Assert.True(stale.Count > 0, Dump(nodes));
                Assert.True(stale.All(n => n.Loops == expectedProbes),
                    $"scope {(scope is null ? "fleet" : scope[0])}: expected {expectedProbes} probe(s) of the wide table, saw {string.Join(",", stale.Select(n => n.Loops))}\n{Dump(nodes)}");
            }

            bodySucceeded = true;
        }
        finally
        {
            await ComposeStampLiveSupport.CleanupAsync(scratch, connection, source, bodySucceeded);
        }
    }
}
