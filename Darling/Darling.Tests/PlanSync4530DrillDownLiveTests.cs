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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4530 — the drill-down half of the wiring. <see cref="PgDrillDownCollector.CollectPlanAdvisoryDetail"/>
/// reads <see cref="DarlingServerMetadataReader"/> and passes the result into
/// <c>PlanAdvisoryAggregator.ExtractCancellable</c>, so a PLAN_WARNING finding's drill-down should carry
/// rule 38's Warning (not its Info branch) when the server is seeded as Standard Edition / MAXDOP 8.
///
/// <para>#1776 own-store: mints its own scratch database (<see cref="ScratchPostgres"/>), never the
/// shared <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class PlanSync4530DrillDownLiveTests
{
    private const int TestServerId = -453_100;
    private const string ServerName = "PlanSync4530DrillDownSrv";
    private const string Db = "PlanSync4530Db";

    /// <summary>Same DOP-2 batch-mode plan XML the reader/entry-point pins use.</summary>
    private const string BatchModeDop2Xml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT SUM(a) FROM dbo.t" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5" StatementOptmLevel="FULL">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104" DegreeOfParallelism="2">
            <RelOp NodeId="0" PhysicalOp="Hash Match" LogicalOp="Aggregate" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="0" Parallel="1" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Batch">
              <OutputList/>
              <RelOp NodeId="1" PhysicalOp="Columnstore Index Scan" LogicalOp="Columnstore Index Scan" EstimateRows="1000" EstimateIO="5" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="1000" Parallel="1" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Batch">
                <OutputList/>
              </RelOp>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    private static async Task SeedQueryStatsPlanAsync(NpgsqlConnection connection)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_xml,
     delta_worker_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue("0x4530DOP2HASH");
        command.Parameters.AddWithValue(BatchModeDop2Xml);
        command.Parameters.AddWithValue(500_000L);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedServerMetadataAsync(NpgsqlConnection connection)
    {
        await using (var props = new NpgsqlCommand(@"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition)
VALUES ($1, $2, $3, $4, $5)", connection))
        {
            props.Parameters.AddWithValue(CollectionIdGenerator.Next());
            props.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            props.Parameters.AddWithValue(TestServerId);
            props.Parameters.AddWithValue(ServerName);
            props.Parameters.AddWithValue("Standard Edition (64-bit)");
            await props.ExecuteNonQueryAsync();
        }

        await using (var cfg = new NpgsqlCommand(@"
INSERT INTO server_config (config_id, capture_time, server_id, server_name, configuration_name, value_in_use)
VALUES ($1, $2, $3, $4, $5, $6)", connection))
        {
            cfg.Parameters.AddWithValue(CollectionIdGenerator.Next());
            cfg.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
            cfg.Parameters.AddWithValue(TestServerId);
            cfg.Parameters.AddWithValue(ServerName);
            cfg.Parameters.AddWithValue("max degree of parallelism");
            cfg.Parameters.AddWithValue(8L);
            await cfg.ExecuteNonQueryAsync();
        }
    }

    [Fact]
    public async Task PlanAdvisoryDrillDown_OnAStandardEditionMaxDop8Server_SurfacesRule38Warning()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4530 drill-down test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedQueryStatsPlanAsync(connection);
            await SeedServerMetadataAsync(connection);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var periodEnd = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var context = new AnalysisContext
        {
            ServerId = TestServerId,
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

        Assert.NotNull(finding.DrillDown);
        Assert.True(finding.DrillDown!.TryGetValue("plan_warnings", out var raw), "plan_warnings was not collected");

        var warnings = JsonSerializer.SerializeToElement(raw);
        var rule38 = warnings.EnumerateArray()
            .Where(w => w.GetProperty("type").GetString() == "Standard Edition DOP Limitation")
            .ToList();

        Assert.Single(rule38);
        Assert.Equal("Warning", rule38[0].GetProperty("severity").GetString());
        Assert.Contains("MAXDOP is set to 8", rule38[0].GetProperty("message").GetString());
    }
}
