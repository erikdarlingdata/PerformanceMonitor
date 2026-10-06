/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Net;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, L8: a drill-down plan warning quotes up to 200 characters of a predicate from the plan it analyzed, and the
/// finding carrying it is persisted and read back by <c>analyze_server</c> and <c>get_analysis_findings</c>. The collector
/// judges each stored plan before the analyzer sees it, so a predicate that belongs to a withheld statement is withheld
/// with it, while the warning for a statement that is kept still quotes its predicate.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>), never the shared
/// <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StatementFilterDrillDownWarningsLiveTests
{
    private const int ServerId = -453_201;
    private const string ServerName = "SsfDrillDownSrv";

    private static string Statement(string text, string predicate, string constant) =>
        "<StmtSimple StatementText=\"" + WebUtility.HtmlEncode(text) + "\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" "
        + "StatementSubTreeCost=\"5\" StatementOptmLevel=\"FULL\"><QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"104\">"
        + "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"100\" EstimateIO=\"1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" "
        + "EstimatedTotalSubtreeCost=\"5\" TableCardinality=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">"
        + "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>"
        + "<Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" IndexKind=\"Heap\" Storage=\"RowStore\"/>"
        + "<Predicate><ScalarOperator ScalarString=\"" + WebUtility.HtmlEncode(predicate) + "\"><Const ConstValue=\"" + constant + "\"/></ScalarOperator></Predicate>"
        + "</TableScan></RelOp></QueryPlan></StmtSimple>";

    private static string TwoStatementPlan() =>
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">"
        + "<BatchSequence><Batch><Statements>"
        + Statement(StatementScrubCanary.CanaryStatement, "upper([db].[dbo].[t].[c])=N'const-secret-ssf'", "N'const-secret-ssf'")
        + Statement(StatementScrubCanary.PlainStatement, "upper([db].[dbo].[t].[c])=N'kept-predicate-ssf'", "N'kept-predicate-ssf'")
        + "</Statements></Batch></BatchSequence></ShowPlanXML>";

    [Fact]
    public async Task APredicateQuotedInAPlanWarning_IsWithheldWithItsStatement_AndKeptWithAKeptOne()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live drill-down warning filter test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_xml, delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
            command.Parameters.AddWithValue(CollectionIdGenerator.Next());
            command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue("SsfDb");
            command.Parameters.AddWithValue("0xSSF0DRILL");
            command.Parameters.AddWithValue(TwoStatementPlan());
            command.Parameters.AddWithValue(500_000L);
            await command.ExecuteNonQueryAsync(ct);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var periodEnd = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var context = new AnalysisContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            TimeRangeStart = periodEnd.AddHours(-4),
            TimeRangeEnd = periodEnd,
            ServerUtcOffset = TimeSpan.Zero,
        };
        var finding = new AnalysisFinding
        {
            RootFactKey = "PLAN_WARNING",
            StoryPath = "PLAN_WARNING",
            PathKeys = ["PLAN_WARNING"],
            Severity = 1.0,
        };

        await new PgDrillDownCollector(postgres).EnrichFindingsAsync([finding], context);

        Assert.True(finding.DrillDown!.TryGetValue("plan_warnings", out var warnings), "plan_warnings was not collected");
        var text = JsonSerializer.Serialize(warnings);
        Assert.Contains("kept-predicate-ssf", text, StringComparison.Ordinal);
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, text, StringComparison.Ordinal);
        }
    }
}
