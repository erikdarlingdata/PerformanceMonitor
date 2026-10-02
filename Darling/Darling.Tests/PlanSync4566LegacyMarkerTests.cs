/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4566 (part of #4511, ported from erikdarlingdata/PerformanceStudio dev (85492a1) commit aabbaa2):
/// PS tags analyzer warnings from rules that predate its benefit-scoring framework with
/// <see cref="PlanWarning.IsLegacy"/>, so reviewers know which findings to hold to a higher bar
/// vs which are known-legacy. The set of legacy types, the marking pass, the MCP <c>is_legacy</c>
/// field and the viewer's " [legacy]" header tag are all ported here.
/// </summary>
public sealed class PlanSync4566LegacyMarkerTests
{
    private static ParsedPlan Analyze(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        return plan;
    }

    // (a) a legacy-listed analyzer warning type ("Local Variables", rule 20) is tagged.
    [Fact]
    public void AnalyzerWarning_OfLegacyType_IsTaggedLegacy()
    {
        var stmt = new PlanStatement
        {
            StatementText = "SELECT a FROM dbo.t WHERE a = @p",
            StatementSubTreeCost = 5.0,
            Parameters = [new PlanParameter { Name = "@p", CompiledValue = null }]
        };
        Analyze(stmt);

        var warning = Assert.Single(stmt.PlanWarnings, w => w.WarningType == "Local Variables");
        Assert.True(warning.IsLegacy);
    }

    // (b) a non-legacy analyzer warning type ("Bare Scan") is not tagged.
    [Fact]
    public void AnalyzerWarning_OfNonLegacyType_IsNotTaggedLegacy()
    {
        var scan = new PlanNode
        {
            PhysicalOp = "Table Scan",
            LogicalOp = "Table Scan",
            HasActualStats = true,
            ActualElapsedMs = 50,
            OutputColumns = "[dbo].[t].[a],[dbo].[t].[b]"
        };
        var stmt = new PlanStatement { StatementText = "SELECT a, b FROM dbo.t", RootNode = scan };
        Analyze(stmt);

        var warning = Assert.Single(scan.Warnings, w => w.WarningType == "Bare Scan");
        Assert.False(warning.IsLegacy);
    }

    // (c) the type name "Implicit Conversion" is shared between SQL Server's own record (never
    // tagged, regardless of type) and the analyzer's rule 29 finding of the same name (tagged,
    // since MarkLegacyWarnings gates on Source, not on which rule produced the finding).
    [Fact]
    public void SqlServerImplicitConversion_IsNotLegacy_AnalyzerImplicitConversion_IsLegacy()
    {
        var engineWarning = new PlanWarning
        {
            WarningType = "Implicit Conversion",
            Message = "Cardinality Estimate: CONVERT_IMPLICIT(...)",
            Source = PlanWarningSource.SqlServer
        };
        var analyzerWarning = new PlanWarning
        {
            WarningType = "Implicit Conversion",
            Message = "Seek Plan: CONVERT_IMPLICIT(...)",
            Source = PlanWarningSource.Analyzer
        };
        var stmt = new PlanStatement
        {
            StatementText = "SELECT a FROM dbo.t",
            PlanWarnings = [engineWarning, analyzerWarning]
        };
        Analyze(stmt);

        Assert.False(engineWarning.IsLegacy);
        Assert.True(analyzerWarning.IsLegacy);
    }

    // (d) the MCP formatter carries is_legacy on each warning.
    private const string LocalVariablesReproXml = """
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT a FROM dbo.t WHERE a = @p" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104">
            <ParameterList>
              <ColumnReference Column="@p" ParameterDataType="int"/>
            </ParameterList>
            <RelOp NodeId="0" PhysicalOp="Table Scan" LogicalOp="Table Scan" EstimateRows="1" EstimateIO="0.003" EstimateCPU="0.0001" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="100" Parallel="0" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Row">
              <OutputList/>
              <TableScan Ordered="0" ForcedIndex="0" ForceScan="0" NoExpandHint="0" Storage="RowStore">
                <DefinedValues/>
                <Predicate><ScalarOperator ScalarString="[dbo].[t].[a]=@p"/></Predicate>
                <Object Database="[db]" Schema="[dbo]" Table="[t]" IndexKind="Heap" Storage="RowStore"/>
              </TableScan>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    [Fact]
    public void McpFormatter_CarriesIsLegacyOnEachWarning()
    {
        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(LocalVariablesReproXml, "test-server", "showplan", null);

        Assert.Contains("\"is_legacy\"", json);

        using var doc = JsonDocument.Parse(json);
        var statements = doc.RootElement.GetProperty("statements");
        var warnings = statements[0].GetProperty("warnings");
        var localVarWarning = warnings.EnumerateArray().Single(w => w.GetProperty("type").GetString() == "Local Variables");
        Assert.True(localVarWarning.GetProperty("is_legacy").GetBoolean());
    }

    // (e) the viewer header carries " [legacy]" in dev's position: source tag, then legacy tag,
    // then the benefit suffix.
    [Fact]
    public void ViewerHeader_AppendsLegacyTag_AfterSourceTag()
    {
        var legacyWarning = new PlanWarning
        {
            WarningType = "Local Variables",
            Source = PlanWarningSource.Analyzer,
            IsLegacy = true,
            MaxBenefitPercent = 25.0
        };

        var header = PlanWarningDisplay.PlanWarningHeader(legacyWarning);

        Assert.Equal("\u26A0 Local Variables [legacy] \u2014 up to 25.0% benefit", header);
    }

    [Fact]
    public void ViewerHeader_EngineWarning_NeverCarriesLegacyTag()
    {
        var engineWarning = new PlanWarning
        {
            WarningType = "Implicit Conversion",
            Source = PlanWarningSource.SqlServer,
            IsLegacy = false
        };

        var header = PlanWarningDisplay.PlanWarningHeader(engineWarning);

        Assert.Equal("\u26A0 Implicit Conversion [SQL Server]", header);
    }
}
