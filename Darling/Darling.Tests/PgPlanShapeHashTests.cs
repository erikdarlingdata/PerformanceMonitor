/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5114: <see cref="PgPlanLogParser.ParsedPlan.PlanHash"/> covers the plan's SHAPE, not its estimates. Two captures
/// of one plan that differ only in costs, row estimates, runtime counters, buffers, settings or JIT are one hash
/// (and keep their own stored JSON); a change to a node type, a join order, a join method, an index or the parallel
/// awareness is a different hash, and so is a key this parser has never heard of (a denylist over-splits, which is
/// the safe direction).
/// </summary>
public sealed class PgPlanShapeHashTests
{
    /// <summary>An Aggregate over an Index Scan, the tree of the real auto_explain fixture, with the estimates as
    /// parameters.</summary>
    private static JsonObject AggregateOverIndexScan(double totalCost = 7.59, long planRows = 1)
    {
        return JsonNode.Parse($$"""
            {
              "Plan": {
                "Node Type": "Aggregate", "Strategy": "Plain", "Partial Mode": "Simple",
                "Parallel Aware": false, "Async Capable": false,
                "Startup Cost": 7.58, "Total Cost": {{totalCost.ToString(System.Globalization.CultureInfo.InvariantCulture)}},
                "Plan Rows": {{planRows}}, "Plan Width": 8,
                "Plans": [
                  {
                    "Node Type": "Index Scan", "Parent Relationship": "Outer",
                    "Parallel Aware": false, "Async Capable": false, "Scan Direction": "Forward",
                    "Index Name": "t13_status", "Relation Name": "t13", "Alias": "t13",
                    "Startup Cost": 0.29, "Total Cost": {{totalCost.ToString(System.Globalization.CultureInfo.InvariantCulture)}},
                    "Plan Rows": {{planRows}}, "Plan Width": 0,
                    "Index Cond": "(status = 'ACME Holdings Ltd'::text)",
                    "Filter": "(amount > 4321.55)"
                  }
                ]
              },
              "Query Identifier": 4242
            }
            """)!.AsObject();
    }

    private static PgPlanLogParser.ParsedPlan Parse(JsonObject block)
    {
        var parsed = PgPlanLogParser.FromBlock(1, 1, block.ToJsonString());
        Assert.NotNull(parsed);
        return parsed!.Value;
    }

    private static JsonObject ChildOf(JsonObject root) =>
        root["Plan"]!["Plans"]![0]!.AsObject();

    [Fact]
    public void TwoBlocksDifferingOnlyInCosts_HashTheSame_AndKeepTheirOwnEstimates()
    {
        var first = Parse(AggregateOverIndexScan(totalCost: 7.59, planRows: 1));
        var second = Parse(AggregateOverIndexScan(totalCost: 91234.5, planRows: 880000));

        Assert.Equal(first.PlanHash, second.PlanHash);
        Assert.NotEqual(first.PlanJson, second.PlanJson);
        Assert.Contains("\"Total Cost\":7.59", first.PlanJson, StringComparison.Ordinal);
        Assert.Contains("\"Total Cost\":91234.5", second.PlanJson, StringComparison.Ordinal);
        Assert.Contains("\"Plan Rows\":880000", second.PlanJson, StringComparison.Ordinal);
    }

    [Fact]
    public void SettingsJitAndTimings_DoNotSplitTheHash()
    {
        var plain = Parse(AggregateOverIndexScan());

        var noisy = AggregateOverIndexScan();
        noisy["Settings"] = JsonNode.Parse("""{"work_mem": "64MB", "random_page_cost": "1.1"}""");
        noisy["JIT"] = JsonNode.Parse("""{"Functions": 3, "Options": {"Inlining": false}, "Timing": {"Total": 4.2}}""");
        noisy["Planning Time"] = 0.123;
        noisy["Execution Time"] = 45.6;
        noisy["Planning"] = JsonNode.Parse("""{"Shared Hit Blocks": 12}""");
        noisy["Triggers"] = JsonNode.Parse("""[{"Trigger Name": "trg", "Relation": "t13", "Time": 0.2, "Calls": 1}]""");

        var withNoise = Parse(noisy);

        Assert.Equal(plain.PlanHash, withNoise.PlanHash);
        Assert.Contains("\"JIT\"", withNoise.PlanJson, StringComparison.Ordinal);
        Assert.Contains("\"Settings\"", withNoise.PlanJson, StringComparison.Ordinal);
    }

    /// <summary>One representative key per pattern family, placed on a CHILD node to prove the walk recurses.</summary>
    [Theory]
    [InlineData("Actual Rows", "12")]
    [InlineData("Actual Loops", "3")]
    [InlineData("Actual Total Time", "1.5")]
    [InlineData("Rows Removed by Filter", "9000")]
    [InlineData("Shared Hit Blocks", "44")]
    [InlineData("Temp Written Blocks", "7")]
    [InlineData("I/O Read Time", "0.5")]
    [InlineData("Shared I/O Read Time", "0.25")]
    [InlineData("WAL Bytes", "8192")]
    [InlineData("Sort Method", "\"quicksort\"")]
    [InlineData("Hash Batches", "4")]
    [InlineData("Workers Launched", "2")]
    [InlineData("Workers", "[{\"Worker Number\": 0, \"Actual Rows\": 5}]")]
    [InlineData("Heap Fetches", "17")]
    [InlineData("Cache Hits", "100")]
    [InlineData("Index Searches", "3")]
    [InlineData("WAL Buffers Full", "5")]
    [InlineData("Estimated Capacity", "1024")]
    public void LogAnalyzeCounters_DoNotSplitTheHash(string key, string valueJson)
    {
        var plain = Parse(AggregateOverIndexScan());

        var counted = AggregateOverIndexScan();
        ChildOf(counted)[key] = JsonNode.Parse(valueJson);

        Assert.Equal(plain.PlanHash, Parse(counted).PlanHash);
    }

    [Fact]
    public void ADifferentNodeType_HashesDifferently()
    {
        var indexScan = Parse(AggregateOverIndexScan());

        var seqScan = AggregateOverIndexScan();
        var child = ChildOf(seqScan);
        child["Node Type"] = "Seq Scan";
        child.Remove("Index Name");
        child.Remove("Scan Direction");

        Assert.NotEqual(indexScan.PlanHash, Parse(seqScan).PlanHash);
    }

    private static JsonObject HashJoinOver(string outer, string inner)
    {
        return JsonNode.Parse($$"""
            {
              "Plan": {
                "Node Type": "Hash Join", "Join Type": "Inner", "Hash Cond": "(a.id = b.id)",
                "Total Cost": 10.0, "Plan Rows": 5,
                "Plans": [
                  { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "{{outer}}", "Alias": "{{outer}}", "Total Cost": 3.0 },
                  { "Node Type": "Hash", "Parent Relationship": "Inner", "Total Cost": 4.0,
                    "Plans": [ { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "{{inner}}", "Alias": "{{inner}}", "Total Cost": 2.0 } ] }
                ]
              }
            }
            """)!.AsObject();
    }

    [Fact]
    public void ADifferentJoinOrder_HashesDifferently()
    {
        Assert.NotEqual(Parse(HashJoinOver("a", "b")).PlanHash, Parse(HashJoinOver("b", "a")).PlanHash);
    }

    [Fact]
    public void ADifferentJoinMethodOrIndex_HashesDifferently()
    {
        var hashJoin = Parse(HashJoinOver("a", "b"));

        var nested = HashJoinOver("a", "b");
        nested["Plan"]!["Node Type"] = "Nested Loop";
        Assert.NotEqual(hashJoin.PlanHash, Parse(nested).PlanHash);

        var x = AggregateOverIndexScan();
        var y = AggregateOverIndexScan();
        ChildOf(y)["Index Name"] = "t13_other";
        Assert.NotEqual(Parse(x).PlanHash, Parse(y).PlanHash);
    }

    [Fact]
    public void ParallelAwareness_IsShape()
    {
        var serial = AggregateOverIndexScan();
        var parallel = AggregateOverIndexScan();
        ChildOf(parallel)["Parallel Aware"] = true;

        Assert.NotEqual(Parse(serial).PlanHash, Parse(parallel).PlanHash);
    }

    private static JsonObject GatherWith(int workersPlanned)
    {
        return JsonNode.Parse($$"""
            {
              "Plan": {
                "Node Type": "Gather", "Parallel Aware": false, "Workers Planned": {{workersPlanned}},
                "Total Cost": 1000.0, "Plan Rows": 100,
                "Plans": [ { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Parallel Aware": true, "Relation Name": "big", "Alias": "big", "Total Cost": 900.0 } ]
              }
            }
            """)!.AsObject();
    }

    /// <summary>The number of workers is a degree the planner derives from relation size and a setting, so it drifts
    /// like an estimate: Gather over two workers and Gather over four are one hash.</summary>
    [Fact]
    public void WorkersPlanned_IsNotShape()
    {
        Assert.Equal(Parse(GatherWith(2)).PlanHash, Parse(GatherWith(4)).PlanHash);
    }

    private static JsonObject DeepPlan(double cost, long rows)
    {
        var c = cost.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return JsonNode.Parse($$"""
            {
              "Plan": {
                "Node Type": "Nested Loop", "Total Cost": {{c}}, "Plan Rows": {{rows}},
                "Plans": [
                  {
                    "Node Type": "Hash Join", "Parent Relationship": "Outer", "Total Cost": {{c}}, "Plan Rows": {{rows}},
                    "Plans": [
                      { "Node Type": "Seq Scan", "Parent Relationship": "Outer", "Relation Name": "a", "Alias": "a",
                        "Total Cost": {{c}}, "Plan Rows": {{rows}}, "Actual Rows": {{rows}} },
                      { "Node Type": "Result", "Parent Relationship": "SubPlan", "Subplan Name": "SubPlan 1",
                        "Total Cost": {{c}}, "Plan Rows": {{rows}}, "Actual Rows": {{rows}} }
                    ]
                  }
                ]
              }
            }
            """)!.AsObject();
    }

    /// <summary>Estimates and actuals are dropped at every depth, grandchildren and SubPlan nodes included.</summary>
    [Fact]
    public void EstimatesBelowTheFirstChild_DoNotSplitTheHash()
    {
        Assert.Equal(Parse(DeepPlan(10.5, 1)).PlanHash, Parse(DeepPlan(99999.5, 777)).PlanHash);
    }

    /// <summary>The denylist's safe direction: a key this parser has never heard of is part of the shape.</summary>
    [Fact]
    public void AnUnknownNodeKey_StillSplits()
    {
        var one = AggregateOverIndexScan();
        ChildOf(one)["Future Field"] = 1;
        var two = AggregateOverIndexScan();
        ChildOf(two)["Future Field"] = 2;

        Assert.NotEqual(Parse(one).PlanHash, Parse(two).PlanHash);
    }

    /// <summary>The projection runs on a clone: the stored JSON is the redacted block with every estimate in it.</summary>
    [Fact]
    public void TheProjection_NeverAltersPlanJson()
    {
        var block = AggregateOverIndexScan();
        block["Query Text"] = "SELECT 1";
        block["Execution Time"] = 45.6;

        var parsed = Parse(block);

        var kept = JsonNode.Parse(parsed.PlanJson)!.AsObject();
        Assert.False(kept.ContainsKey("Query Text"));
        Assert.Equal(45.6, kept["Execution Time"]!.GetValue<double>());
        Assert.Equal(7.59, kept["Plan"]!["Total Cost"]!.GetValue<double>());
        Assert.Equal(1, kept["Plan"]!["Plan Rows"]!.GetValue<long>());
        Assert.Equal(0.29, ChildOf(kept)["Startup Cost"]!.GetValue<double>());
        Assert.Contains("'?'", parsed.PlanJson.Replace("\\u0027", "'"), StringComparison.Ordinal);
    }

    /// <summary>32 uppercase hex characters, so the 48-bit prefix the plan-flip SQL takes of it stays valid.</summary>
    [Fact]
    public void ShapeHash_StaysThirtyTwoHex()
    {
        Assert.Matches(new Regex("^[0-9A-F]{32}$"), Parse(AggregateOverIndexScan()).PlanHash);
    }
}
