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
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7886: the plan advisory facts and their drill-down count each plan once, however many snapshots it has
/// in the window. <c>v_query_stats</c> holds one row per query per snapshot, and every snapshot row of a cached
/// plan resolves to the same plan XML, so a hot plan seen in six collections is six rows at the top of the
/// delta_worker_time ordering. PLAN_WARNING's <c>warning_count</c> and <c>critical_count</c> are the warnings
/// over the window's distinct top plans, and the drill-down's <c>plan_warnings</c> never lists one plan's
/// warning twice. The Lite twin is <c>PlanAdvisoryDistinctPlansTests</c>.
///
/// <para>A plan is a statement's plan, not a plan shape: <c>query_plan_hash</c> is a shape hash that two
/// statements with one shape share, so two such statements with different plans are two plans and both are
/// counted.</para>
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>), never the shared
/// <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class PlanAdvisoryDistinctPlansLiveTests
{
    private const int ServerId = -788_601;
    private const string ServerName = "PlanDedupSrv";

    /// <summary>A plan whose one statement wraps a column in a function, so the analyzer raises a warning that quotes the predicate.</summary>
    private static string WarningPlan(string column) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + WebUtility.HtmlEncode($"SELECT * FROM dbo.t WHERE UPPER({column}) = 1") + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"" + WebUtility.HtmlEncode($"upper([db].[dbo].[t].[{column}])=(1)") + "\"><Const ConstValue=\"(1)\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static readonly string HotPlan = WarningPlan("c_hot");
    private static readonly string SecondPlan = WarningPlan("c_second");
    private static readonly string ThirdPlan = WarningPlan("c_third");
    private static readonly string FirstStatementPlan = WarningPlan("c_first_statement");
    private static readonly string SecondStatementPlan = WarningPlan("c_second_statement");

    private sealed record DrillDownWarning(string type, string severity, string message);

    [Fact]
    public async Task PlanWarningFactAndDrillDown_TwoStatementsWithOnePlanShape_CountBothPlans()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plan advisory dedup test.");
        var ct = TestContext.Current.CancellationToken;

        var periodEnd = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* Two statements of one batch with one plan shape: the same query_plan_hash and plan_handle, a
               different query_hash, and a different plan each, in one snapshot. Both plans raise a warning of their own. */
            await SeedAsync(connection, periodEnd.AddMinutes(-5), "0xSHAPE", FirstStatementPlan, workerTime: 900_000, ct, queryHash: "0xSTMT1", planHandle: "0xBATCH");
            await SeedAsync(connection, periodEnd.AddMinutes(-5), "0xSHAPE", SecondStatementPlan, workerTime: 800_000, ct, queryHash: "0xSTMT2", planHandle: "0xBATCH");
        }

        var both = PlanAdvisoryAggregator.Summarize([FirstStatementPlan, SecondStatementPlan]);
        Assert.True(both.WarningCount > PlanAdvisoryAggregator.Summarize([FirstStatementPlan]).WarningCount,
            "the second statement's plan must add a warning of its own");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = periodEnd.AddHours(-4),
            TimeRangeEnd = periodEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };

        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

        var fact = Assert.Single(facts, f => f.Key == "PLAN_WARNING");
        Assert.Equal(both.WarningCount, fact.Value);
        Assert.Equal(both.WarningCount, fact.Metadata["warning_count"]);
        Assert.Equal(both.CriticalCount, fact.Metadata["critical_count"]);

        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_WARNING",
            StoryPath = "PLAN_WARNING",
            PathKeys = ["PLAN_WARNING"],
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.True(finding.DrillDown!.TryGetValue("plan_warnings", out var raw), "plan_warnings was not collected");
        var warnings = JsonSerializer.Deserialize<List<DrillDownWarning>>(JsonSerializer.Serialize(raw))!;
        var bothDetail = PlanAdvisoryAggregator.Extract([FirstStatementPlan, SecondStatementPlan]).Warnings.Count;
        Assert.Equal(Math.Min(5, bothDetail), warnings.Count);
        Assert.Equal(warnings.Count, warnings.Select(w => (w.type, w.message)).Distinct().Count());
    }

    [Fact]
    public async Task PlanWarningFactAndDrillDown_APlanInSixSnapshots_CountItsWarningsOnce()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plan advisory dedup test.");
        var ct = TestContext.Current.CancellationToken;

        var periodEnd = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            /* The hot plan in six snapshots, each costlier than the two single-snapshot plans, so the six copies
               lead the delta_worker_time ordering the read takes its ten rows from. */
            for (var i = 0; i < 6; i++)
                await SeedAsync(connection, periodEnd.AddMinutes(-5 * (i + 1)), "0xPLANHOT", HotPlan, workerTime: 900_000 - i, ct);
            await SeedAsync(connection, periodEnd.AddMinutes(-10), "0xPLANSECOND", SecondPlan, workerTime: 400_000, ct);
            await SeedAsync(connection, periodEnd.AddMinutes(-10), "0xPLANTHIRD", ThirdPlan, workerTime: 300_000, ct);
        }

        var distinct = PlanAdvisoryAggregator.Summarize([HotPlan, SecondPlan, ThirdPlan]);
        Assert.True(PlanAdvisoryAggregator.Summarize([HotPlan]).WarningCount >= 1, "the fixture plan must raise at least one warning");

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = periodEnd.AddHours(-4),
            TimeRangeEnd = periodEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };

        var facts = await new PgFactCollector(postgres).CollectFactsAsync(context);

        var fact = Assert.Single(facts, f => f.Key == "PLAN_WARNING");
        Assert.Equal(distinct.WarningCount, fact.Value);
        Assert.Equal(distinct.WarningCount, fact.Metadata["warning_count"]);
        Assert.Equal(distinct.CriticalCount, fact.Metadata["critical_count"]);

        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_WARNING",
            StoryPath = "PLAN_WARNING",
            PathKeys = ["PLAN_WARNING"],
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.True(finding.DrillDown!.TryGetValue("plan_warnings", out var raw), "plan_warnings was not collected");
        var warnings = JsonSerializer.Deserialize<List<DrillDownWarning>>(JsonSerializer.Serialize(raw))!;
        Assert.NotEmpty(warnings);
        var distinctDetail = PlanAdvisoryAggregator.Extract([HotPlan, SecondPlan, ThirdPlan]).Warnings.Count;
        Assert.Equal(Math.Min(5, distinctDetail), warnings.Count);
        Assert.Equal(warnings.Count, warnings.Select(w => (w.type, w.message)).Distinct().Count());
    }

    /// <summary>One query_stats snapshot row. The query hash and plan handle derive from the plan hash unless given.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, DateTime collectionTime, string planHash, string planXml, long workerTime, CancellationToken ct,
        string? queryHash = null, string? planHandle = null)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, plan_handle, query_plan_xml, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("PlanDedupDb");
        command.Parameters.AddWithValue(queryHash ?? planHash + "-query");
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue(planHandle ?? planHash + "-handle");
        command.Parameters.AddWithValue(planXml);
        command.Parameters.AddWithValue(workerTime);
        await command.ExecuteNonQueryAsync(ct);
    }
}
