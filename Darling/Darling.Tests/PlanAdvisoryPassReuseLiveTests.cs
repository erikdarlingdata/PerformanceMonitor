/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570: an analysis pass reads <see cref="PgFactCollector.PlanAdvisorySql"/> once. The MISSING_INDEX and PLAN_WARNING
/// drill-downs of that pass render from the plans the fact read returned, so the pass runs the statement once whatever
/// number of plan-advisory findings it enriches, and each drill-down is the one a drill-down with its own read renders.
/// The statements are counted from Npgsql's own command log (<see cref="CommandCountingLoggerFactory"/>).
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>).</para>
/// </summary>
public sealed class PlanAdvisoryPassReuseLiveTests
{
    private const int ServerId = -857_001;
    private const string ServerName = "PlanAdvisoryReuseSrv";

    /// <summary>Text only <see cref="PgFactCollector.PlanAdvisorySql"/> carries among the pass's statements.</summary>
    private const string PlanAdvisoryMarker = "WITH grp AS";

    /// <summary>A one-statement plan that wraps a column in a function (a plan warning) and carries one missing index.</summary>
    private static string Plan(string column, double impact) =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + "<StmtSimple StatementText=\"" + WebUtility.HtmlEncode($"SELECT * FROM dbo.t WHERE UPPER({column}) = 1") + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<MissingIndexes><MissingIndexGroup Impact=\"" + impact.ToString(System.Globalization.CultureInfo.InvariantCulture) + "\">"
        + "<MissingIndex Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\"><ColumnGroup Usage=\"EQUALITY\"><Column Name=\"[" + column + "]\" ColumnId=\"1\"/></ColumnGroup></MissingIndex>"
        + "</MissingIndexGroup></MissingIndexes>"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"" + WebUtility.HtmlEncode($"upper([db].[dbo].[t].[{column}])=(1)") + "\"><Const ConstValue=\"(1)\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>"
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    private static AnalysisFinding Finding(string key) => new()
    {
        RootFactKey = key,
        StoryPath = key,
        PathKeys = [key],
        Severity = 1.0,
    };

    [Fact]
    public async Task AnalysisPass_TwoPlanAdvisoryFindings_ReadsThePlansOnceAndRendersTheStandaloneDrillDowns()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plan advisory reuse test.");
        var ct = TestContext.Current.CancellationToken;

        var periodEnd = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedAsync(connection, periodEnd.AddMinutes(-30), "0xPLAN1", Plan("c_one", 91.5), 900_000, ct);
            await SeedAsync(connection, periodEnd.AddMinutes(-20), "0xPLAN2", Plan("c_two", 62.25), 700_000, ct);
            await SeedAsync(connection, periodEnd.AddMinutes(-10), "0xPLAN3", Plan("c_three", 40), 500_000, ct);
        }

        var counter = new CommandCountingLoggerFactory();
        await using var countedStore = new NpgsqlDataSourceBuilder(scratch.ConnectionString).UseLoggerFactory(counter).Build();

        AnalysisContext Context() => new()
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = periodEnd.AddHours(-4),
            TimeRangeEnd = periodEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };

        /* The pass: the facts, then the drill-downs of both plan-advisory findings on the same context. */
        var pass = Context();
        var facts = await new PgFactCollector(countedStore).CollectFactsAsync(pass);
        Assert.Contains(facts, f => f.Key == "MISSING_INDEX");
        Assert.Contains(facts, f => f.Key == "PLAN_WARNING");
        Assert.Equal(1, counter.Provider.CountContaining(PlanAdvisoryMarker));

        var passMissingIndex = Finding("MISSING_INDEX");
        var passPlanWarning = Finding("PLAN_WARNING");
        await new PgDrillDownCollector(countedStore).EnrichFindingsAsync([passMissingIndex, passPlanWarning], pass);
        Assert.Equal(1, counter.Provider.CountContaining(PlanAdvisoryMarker));

        /* The same drill-downs with no fact pass before them read the plans themselves, once per finding. */
        counter.Provider.Reset();
        var ownMissingIndex = Finding("MISSING_INDEX");
        var ownPlanWarning = Finding("PLAN_WARNING");
        await new PgDrillDownCollector(countedStore).EnrichFindingsAsync([ownMissingIndex, ownPlanWarning], Context());
        Assert.Equal(2, counter.Provider.CountContaining(PlanAdvisoryMarker));

        Assert.True(ownMissingIndex.DrillDown!.ContainsKey("missing_indexes"), "missing_indexes was not collected");
        Assert.True(ownPlanWarning.DrillDown!.ContainsKey("plan_warnings"), "plan_warnings was not collected");
        Assert.Equal(JsonSerializer.Serialize(ownMissingIndex.DrillDown), JsonSerializer.Serialize(passMissingIndex.DrillDown));
        Assert.Equal(JsonSerializer.Serialize(ownPlanWarning.DrillDown), JsonSerializer.Serialize(passPlanWarning.DrillDown));
    }

    [Fact]
    public async Task AnalysisPass_NoPlanInTheWindow_DrillDownReadsNothingAndRendersNothing()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live plan advisory reuse test.");
        var ct = TestContext.Current.CancellationToken;

        var periodEnd = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        var counter = new CommandCountingLoggerFactory();
        await using var countedStore = new NpgsqlDataSourceBuilder(scratch.ConnectionString).UseLoggerFactory(counter).Build();
        var pass = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = periodEnd.AddHours(-4),
            TimeRangeEnd = periodEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };

        var facts = await new PgFactCollector(countedStore).CollectFactsAsync(pass);
        Assert.DoesNotContain(facts, f => f.Key is "MISSING_INDEX" or "PLAN_WARNING");

        var finding = Finding("PLAN_WARNING");
        await new PgDrillDownCollector(countedStore).EnrichFindingsAsync([finding], pass);

        Assert.Equal(1, counter.Provider.CountContaining(PlanAdvisoryMarker));
        Assert.Null(finding.DrillDown);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, DateTime collectionTime, string planHash, string planXml, long workerTime, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash, plan_handle, query_plan_xml, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue("PlanAdvisoryReuseDb");
        command.Parameters.AddWithValue(planHash + "-query");
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue(planHash + "-handle");
        command.Parameters.AddWithValue(planXml);
        command.Parameters.AddWithValue(workerTime);
        await command.ExecuteNonQueryAsync(ct);
    }
}
