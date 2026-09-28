/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using System.Threading;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4535 (config loading step): pins that the per-rule analyzer config actually reaches the
/// analyzer through each host's own config file, not just that the shape parses (that was
/// <see cref="PlanSync4535ConfigPlumbingTests"/>). <see cref="ConfigLoader.Parse"/> and
/// <see cref="ConfigLoader.LoadFile"/>, the <c>darling.json</c> "analyzer" section binding
/// (<see cref="DarlingConfig.Analyzer"/>), the end-to-end MCP formatter path with a disabled rule
/// and a severity override, and the viewer control's <c>AnalyzerConfig</c> property reaching
/// <see cref="PlanAnalysisPipeline.Run(ParsedPlan, AnalyzerConfig?, ServerMetadata?, CancellationToken)"/>.
/// </summary>
public sealed class PlanSync4535ConfigLoadingTests
{
    /// <summary>A statement with an un-sniffed local variable and cost above the rule-20 floor,
    /// so the analyzer emits rule 20 (Local Variables) by default.</summary>
    private const string LocalVariablePlanXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="DECLARE @b INT = 1; SELECT a FROM dbo.t WHERE b = @b" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5" StatementOptmLevel="FULL">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1000" EstimateIO="5" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="1000" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableScan Storage="RowStore">
                <Object Database="[Repro]" Schema="[dbo]" Table="[t]" Storage="RowStore"/>
              </TableScan>
            </RelOp>
            <ParameterList>
              <ColumnReference Column="@b" ParameterDataType="int" ParameterRuntimeValue="1"/>
            </ParameterList>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    /* ---------------- ConfigLoader.Parse / LoadFile ---------------- */

    [Fact]
    public void Parse_PsShapedJson_RoundTrips()
    {
        const string json = """{"rules":{"disabled":[20],"severity_overrides":{"5":"Low"}}}""";

        var cfg = ConfigLoader.Parse(json);

        Assert.True(cfg.IsRuleDisabled(20));
        Assert.Equal("Low", cfg.GetSeverityOverride(5));
    }

    [Fact]
    public void Parse_Null_ReturnsDefault()
    {
        var cfg = ConfigLoader.Parse(null);

        Assert.False(cfg.IsRuleDisabled(20));
        Assert.Null(cfg.GetSeverityOverride(5));
    }

    [Fact]
    public void Parse_Malformed_ReturnsDefault()
    {
        var cfg = ConfigLoader.Parse("{ not valid json ]");

        Assert.False(cfg.IsRuleDisabled(20));
        Assert.Null(cfg.GetSeverityOverride(5));
    }

    [Fact]
    public void LoadFile_MissingFile_ReturnsDefault()
    {
        var cfg = ConfigLoader.LoadFile(System.IO.Path.Combine(
            System.IO.Path.GetTempPath(), "does-not-exist-4535-" + System.Guid.NewGuid() + ".json"));

        Assert.False(cfg.IsRuleDisabled(20));
    }

    [Fact]
    public void LoadFile_RealFile_Binds()
    {
        var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "analyzer-4535-" + System.Guid.NewGuid() + ".json");
        System.IO.File.WriteAllText(path, """{"rules":{"disabled":[20]}}""");
        try
        {
            var cfg = ConfigLoader.LoadFile(path);
            Assert.True(cfg.IsRuleDisabled(20));
        }
        finally
        {
            System.IO.File.Delete(path);
        }
    }

    /* ---------------- darling.json "analyzer" section binding ---------------- */

    [Fact]
    public void DarlingConfig_AnalyzerSection_Binds()
    {
        const string json = """
            {
              "postgres": { "managed": true },
              "analyzer": { "rules": { "disabled": [20], "severity_overrides": { "5": "Low" } } }
            }
            """;

        var config = JsonSerializer.Deserialize<DarlingConfig>(json,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        Assert.NotNull(config);
        Assert.NotNull(config!.Analyzer);
        Assert.True(config.Analyzer!.IsRuleDisabled(20));
        Assert.Equal("Low", config.Analyzer.GetSeverityOverride(5));
    }

    [Fact]
    public void DarlingConfig_NoAnalyzerSection_IsNullAndTreatedAsDefault()
    {
        const string json = """{ "postgres": { "managed": true } }""";

        var config = JsonSerializer.Deserialize<DarlingConfig>(json,
            new JsonSerializerOptions { PropertyNameCaseInsensitive = true });

        Assert.NotNull(config);
        Assert.Null(config!.Analyzer);

        // Every consumer feeds Analyzer through "?? AnalyzerConfig.Default" (DarlingMcpHostService,
        // DarlingWorker etc.), so a null section behaves exactly like an explicit Default.
        var effective = config.Analyzer ?? AnalyzerConfig.Default;
        Assert.False(effective.IsRuleDisabled(20));
    }

    /* ---------------- end to end through the MCP formatter ---------------- */

    [Fact]
    public void BuildAnalysisResult_DefaultConfig_EmitsLocalVariablesFinding()
    {
        var result = McpPlanAnalysisFormatter.BuildAnalysisResult(
            LocalVariablePlanXml, "TESTSRV", "query_stats", "0xTEST", AnalyzerConfig.Default, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        var statement = doc.RootElement.GetProperty("statements")[0];
        var warningTypes = statement.GetProperty("warnings").EnumerateArray()
            .Select(w => w.GetProperty("type").GetString())
            .ToList();

        Assert.Contains("Local Variables", warningTypes);
    }

    [Fact]
    public void BuildAnalysisResult_Disabled20_RemovesTheFinding()
    {
        var cfg = ConfigLoader.Parse("""{"rules":{"disabled":[20]}}""");

        var result = McpPlanAnalysisFormatter.BuildAnalysisResult(
            LocalVariablePlanXml, "TESTSRV", "query_stats", "0xTEST", cfg, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        var statement = doc.RootElement.GetProperty("statements")[0];
        var warningTypes = statement.GetProperty("warnings").EnumerateArray()
            .Select(w => w.GetProperty("type").GetString())
            .ToList();

        Assert.DoesNotContain("Local Variables", warningTypes);
    }

    [Fact]
    public void BuildAnalysisResult_SeverityOverride_ChangesReportedSeverity()
    {
        var cfg = ConfigLoader.Parse("""{"rules":{"severity_overrides":{"20":"Info"}}}""");

        var result = McpPlanAnalysisFormatter.BuildAnalysisResult(
            LocalVariablePlanXml, "TESTSRV", "query_stats", "0xTEST", cfg, CancellationToken.None);

        using var doc = JsonDocument.Parse(result);
        var statement = doc.RootElement.GetProperty("statements")[0];
        var localVarWarning = statement.GetProperty("warnings").EnumerateArray()
            .First(w => w.GetProperty("type").GetString() == "Local Variables");

        Assert.Equal("Info", localVarWarning.GetProperty("severity").GetString());
    }

    /* ---------------- the viewer control's helper seam ---------------- */

    [Fact]
    public void ViewerSeam_AnalyzerConfig_ReachesRun_DisablesTheFinding()
    {
        // Mirrors PlanViewerControl.LoadPlan's body: ShowPlanParser.Parse then
        // PlanAnalysisPipeline.Run(plan, analyzerConfig, serverMetadata: null, ct) — the same
        // call the control makes once its AnalyzerConfig property is set by the host.
        var cfg = ConfigLoader.Parse("""{"rules":{"disabled":[20]}}""");

        var plan = ShowPlanParser.Parse(LocalVariablePlanXml);
        PlanAnalysisPipeline.Run(plan, cfg, serverMetadata: null, CancellationToken.None);

        var warnings = plan.Batches.SelectMany(b => b.Statements).SelectMany(s => s.PlanWarnings).ToList();
        Assert.DoesNotContain(warnings, w => w.WarningType == "Local Variables");
    }

    [Fact]
    public void ViewerSeam_NullAnalyzerConfig_KeepsDefaultBehavior()
    {
        var plan = ShowPlanParser.Parse(LocalVariablePlanXml);
        PlanAnalysisPipeline.Run(plan, null, serverMetadata: null, CancellationToken.None);

        var warnings = plan.Batches.SelectMany(b => b.Statements).SelectMany(s => s.PlanWarnings).ToList();
        Assert.Contains(warnings, w => w.WarningType == "Local Variables");
    }
}
