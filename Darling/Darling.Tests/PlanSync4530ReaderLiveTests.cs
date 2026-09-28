/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.PlanAnalysis;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4530 — the untested half of the wiring: that <see cref="DarlingServerMetadataReader"/> actually
/// reads the edition and MAXDOP off a migrated store rather than a hand-built <see cref="ServerMetadata"/>.
/// <see cref="PlanSync4530Tests"/> and <see cref="PlanSync4530EntryPointsTests"/> only ever pass a
/// literal <c>ServerMetadata</c> into the analyzer; a wrong column or table name in
/// <see cref="DarlingServerMetadataReader.ServerMetadataSql"/> would return <c>null</c> silently and rule
/// 38 would stay on its Info branch on every real store, with nothing here to catch it.
///
/// <para>Seeds <c>collect.server_properties</c> (edition "Standard Edition (64-bit)") and
/// <c>collect.server_config</c> (<c>max degree of parallelism</c> = 8) with the store's real migrated
/// columns, reads them back through the reader, and runs the SAME plan XML
/// <see cref="McpPlanAnalysisFormatter.BuildAnalysisResult"/> uses through the MCP entry point
/// (<c>DarlingMcpPlanTools.AnalyzeQueryPlan</c>) to confirm rule 38 gives its Warning end to end.</para>
///
/// <para>#1776 own-store: this uses <c>ScratchPostgres</c>, not the shared <c>live-postgres</c>
/// collection, and never touches another test's rows.</para>
/// </summary>
public sealed class PlanSync4530ReaderLiveTests
{
    private const int TestServerId = -453_000;

    /// <summary>Same DOP-2 batch-mode plan XML <see cref="PlanSync4530EntryPointsTests"/> uses.</summary>
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

    private static async Task SeedServerPropertiesAsync(NpgsqlConnection connection, string edition)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name, edition, product_version, product_level,
     cpu_count, physical_memory_mb)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue("PlanSync4530Srv");
        command.Parameters.AddWithValue(edition);
        command.Parameters.AddWithValue("16.0.4215.2");
        command.Parameters.AddWithValue("RTM");
        command.Parameters.AddWithValue(4);
        command.Parameters.AddWithValue(16384L);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedMaxDopAsync(NpgsqlConnection connection, int maxDop)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name, value_configured, value_in_use)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue("PlanSync4530Srv");
        command.Parameters.AddWithValue("max degree of parallelism");
        command.Parameters.AddWithValue((long)maxDop);
        command.Parameters.AddWithValue((long)maxDop);
        await command.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task DarlingServerMetadataReader_ReadsEditionAndMaxDop_FromAMigratedStore_AndRule38Warns()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #4530 reader test.");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct);

        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);

            await SeedServerPropertiesAsync(connection, "Standard Edition (64-bit)");
            await SeedMaxDopAsync(connection, 8);
        }

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* ---- (1) the reader itself, direct call, against the migrated store. ---- */
        var metadata = await DarlingServerMetadataReader.ReadAsync(postgres, TestServerId, ct);
        Assert.NotNull(metadata);
        Assert.Contains("Standard", metadata!.Edition, StringComparison.Ordinal);
        Assert.Equal(8, metadata.MaxDop);

        /* ---- (2) through the product's own analysis path: rule 38 gives its Warning. ---- */
        var withMetadata = McpPlanAnalysisFormatter.BuildAnalysisResult(
            BatchModeDop2Xml, "PlanSync4530Srv", "xml", null, metadata, ct);
        using var withDoc = JsonDocument.Parse(withMetadata);
        var warning = withDoc.RootElement.GetProperty("statements")[0].GetProperty("warnings")
            .EnumerateArray().Single(w => w.GetProperty("type").GetString() == "Standard Edition DOP Limitation");
        Assert.Equal("Warning", warning.GetProperty("severity").GetString());
        Assert.Contains("MAXDOP is set to 8", warning.GetProperty("message").GetString());

        /* ---- (3) a server with no rows at all: reader returns null, rule 38 stays Info. ---- */
        var noRowsMetadata = await DarlingServerMetadataReader.ReadAsync(postgres, TestServerId - 1, ct);
        Assert.Null(noRowsMetadata);

        var withoutMetadata = McpPlanAnalysisFormatter.BuildAnalysisResult(
            BatchModeDop2Xml, "PlanSync4530Srv", "xml", null, noRowsMetadata, ct);
        using var withoutDoc = JsonDocument.Parse(withoutMetadata);
        var info = withoutDoc.RootElement.GetProperty("statements")[0].GetProperty("warnings")
            .EnumerateArray().Single(w => w.GetProperty("type").GetString() == "Standard Edition DOP Limitation");
        Assert.Equal("Info", info.GetProperty("severity").GetString());
    }

    /// <summary>
    /// Source-census pin: Lite's <c>GetServerMetadataForPlanAnalysisAsync</c> SQL (<c>Lite.Tests</c>
    /// can't run on macOS, so this is read by hand there — see the RED note in the PR) names the same
    /// views and columns as Darling's migrated schema, so both SKUs answer rule 38 off equivalent data.
    /// </summary>
    [Fact]
    public void LiteReaderSql_NamesTheSameViewAndColumns_AsDarlingsMigratedSchema()
    {
        var liteSource = ReadRepoFile(Path.Combine("Lite", "Services", "LocalDataService.PlanServerMetadata.cs"));

        Assert.Contains("FROM v_server_properties", liteSource, StringComparison.Ordinal);
        Assert.Contains("FROM v_server_config", liteSource, StringComparison.Ordinal);
        Assert.Contains("edition", liteSource, StringComparison.Ordinal);
        Assert.Contains("'max degree of parallelism'", liteSource, StringComparison.Ordinal);
        Assert.Contains("value_in_use", liteSource, StringComparison.Ordinal);

        /* Darling's own reader SQL, so a rename on one side without the other fails here. */
        Assert.Contains("edition", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("max degree of parallelism", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
        Assert.Contains("value_in_use", DarlingServerMetadataReader.ServerMetadataSql, StringComparison.Ordinal);
    }
}
