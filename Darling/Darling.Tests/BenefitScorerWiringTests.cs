/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4546: <see cref="BenefitScorer.Score"/> existed but nothing in the product called it, so
/// <c>PlanWarning.MaxBenefitPercent</c> was always null everywhere. <see cref="PlanAnalysisPipeline.Run"/>
/// is now the one place that runs <see cref="PlanAnalyzer.Analyze"/> then <see cref="BenefitScorer.Score"/>,
/// and every entry point that used to call the analyzer directly calls the pipeline instead.
/// </summary>
public sealed class BenefitScorerWiringTests
{
    /// <summary>
    /// Census: no production file outside <see cref="PlanAnalysisPipeline"/> calls
    /// <c>PlanAnalyzer.Analyze(</c> directly. A direct call bypasses the scorer again, the exact #4546 bug.
    /// Same walker shape as the repo's other production-file censuses (deprecated/, bin/, obj/ and
    /// *Tests directories excluded; PlanAnalysisPipeline.cs itself is the one legitimate caller).
    /// </summary>
    [Fact]
    public void NoProductionCallerBypassesThePipeline()
    {
        var offenders = new List<string>();

        foreach (var file in ProductionCSharpFiles())
        {
            var fileName = Path.GetFileName(file);
            if (fileName is "PlanAnalysisPipeline.cs" or "BenefitScorer.cs")
                continue; // BenefitScorer.cs's own doc comment names PlanAnalyzer.Analyze() in prose, not a call

            var code = File.ReadAllText(file);
            if (code.Contains("PlanAnalyzer.Analyze("))
                offenders.Add(file);
        }

        Assert.True(offenders.Count == 0,
            "Call PlanAnalysisPipeline.Run(plan) instead of PlanAnalyzer.Analyze(plan) directly so " +
            "BenefitScorer.Score always runs too. Offenders: " + string.Join(", ", offenders));
    }

    /// <summary>
    /// End to end through the product's own MCP entry point: a Serial Plan finding on a plan with
    /// actual runtime stats gets a non-null MaxBenefitPercent in the JSON envelope. Before #4546 this
    /// was always null — the scorer that computes it never ran.
    /// </summary>
    [Fact]
    public void McpAnalysisResult_SerialPlanWarning_HasMaxBenefitPercent()
    {
        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(SerialPlanWithActualStats(), null, "test", null);
        using var doc = JsonDocument.Parse(json);

        var statements = doc.RootElement.GetProperty("statements");
        Assert.True(statements.GetArrayLength() > 0);

        var warnings = statements[0].GetProperty("warnings");
        var serialWarning = warnings.EnumerateArray()
            .FirstOrDefault(w => w.GetProperty("type").GetString() == "Serial Plan");

        Assert.True(serialWarning.ValueKind != JsonValueKind.Undefined, "Expected a Serial Plan warning.");

        var benefit = serialWarning.GetProperty("max_benefit_percent");
        Assert.NotEqual(JsonValueKind.Null, benefit.ValueKind);
        Assert.True(benefit.GetDouble() > 0);
    }

    /// <summary>
    /// #4546 follow-up: the MCP envelope orders a statement's <c>warnings</c> by <c>max_benefit_percent</c>
    /// descending, with the unscored (null) finding last — the same ordering PerformanceStudio's viewer and
    /// advice builder apply. The fixture carries a Serial Plan finding (scored, benefit &gt; 0) and a Local
    /// Variables finding (Rule 20, never quantified — <see cref="BenefitScorer.Score"/> leaves it null), so
    /// the ordering has something real to sort.
    /// </summary>
    [Fact]
    public void McpAnalysisResult_Warnings_OrderedByMaxBenefitPercentDescending_NullsLast()
    {
        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(SerialPlanWithUnsniffedLocalVariable(), null, "test", null);
        using var doc = JsonDocument.Parse(json);

        var warnings = doc.RootElement.GetProperty("statements")[0].GetProperty("warnings")
            .EnumerateArray().ToList();

        Assert.True(warnings.Count >= 2, "Expected at least the Serial Plan and Local Variables findings.");

        var types = warnings.Select(w => w.GetProperty("type").GetString()).ToList();
        var serialIndex = types.IndexOf("Serial Plan");
        var localVarIndex = types.IndexOf("Local Variables");
        Assert.True(serialIndex >= 0, "Expected a Serial Plan warning.");
        Assert.True(localVarIndex >= 0, "Expected a Local Variables warning.");

        Assert.True(
            warnings[serialIndex].GetProperty("max_benefit_percent").GetDouble() > 0,
            "Serial Plan should carry a positive scored benefit.");
        Assert.Equal(JsonValueKind.Null, warnings[localVarIndex].GetProperty("max_benefit_percent").ValueKind);

        // The scored finding comes before the unscored (null) one.
        Assert.True(serialIndex < localVarIndex,
            $"Expected the scored Serial Plan finding before the null Local Variables finding; got order {string.Join(", ", types)}");

        // A strict descending-by-benefit check across every scored entry (nulls treated as -1, so they sort last).
        var benefits = warnings
            .Select(w => w.GetProperty("max_benefit_percent").ValueKind == JsonValueKind.Null
                ? -1.0
                : w.GetProperty("max_benefit_percent").GetDouble())
            .ToList();
        for (var i = 1; i < benefits.Count; i++)
            Assert.True(benefits[i - 1] >= benefits[i], $"warnings not ordered by max_benefit_percent descending at index {i}.");
    }

    /// <summary>A plan with no statements — the pipeline's early-return arm — doesn't throw and leaves the plan unchanged.</summary>
    [Fact]
    public void Run_EmptyPlan_DoesNotThrow_AndLeavesPlanUnchanged()
    {
        var plan = new ParsedPlan();

        var result = PlanAnalysisPipeline.Run(plan);

        Assert.Same(plan, result);
        Assert.Empty(result.Batches);
    }

    private static IEnumerable<string> ProductionCSharpFiles()
    {
        foreach (var file in Directory.EnumerateFiles(RepoFile.Root, "*.cs", SearchOption.AllDirectories))
        {
            var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
            if (segments.Contains("bin") || segments.Contains("obj")
                || segments.Contains("deprecated")
                || segments.Any(s => s.EndsWith("Tests", StringComparison.Ordinal)))
            {
                continue;
            }

            yield return file;
        }
    }

    /// <summary>A single-statement actual plan: forced serial (CouldNotGenerateValidParallelPlan), cost
    /// above the analyzer's floor, real QueryTimeStats so the scorer's CPU-bound Serial Plan rule fires.</summary>
    private static string SerialPlanWithActualStats()
    {
        return
            "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
            "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
            "<BatchSequence><Batch><Statements>" +
            "<StmtSimple StatementText=\"SELECT * FROM dbo.t\" StatementId=\"1\" StatementType=\"SELECT\" " +
            "StatementSubTreeCost=\"100\" StatementEstRows=\"10\" StatementOptmLevel=\"FULL\">" +
            "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" " +
            "NonParallelPlanReason=\"CouldNotGenerateValidParallelPlan\" DegreeOfParallelism=\"1\">" +
            "<QueryTimeStats CpuTime=\"1000\" ElapsedTime=\"1000\" />" +
            "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
            "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
            "EstimatedTotalSubtreeCost=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" " +
            "EstimatedExecutionMode=\"Row\">" +
            "<OutputList />" +
            "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"10\" " +
            "ActualElapsedms=\"1000\" ActualCPUms=\"1000\" ActualLogicalReads=\"10\" /></RunTimeInformation>" +
            "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
            "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" " +
            "IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
            "</IndexScan>" +
            "</RelOp>" +
            "</QueryPlan></StmtSimple>" +
            "</Statements></Batch></BatchSequence></ShowPlanXML>";
    }

    /// <summary>Same shape as <see cref="SerialPlanWithActualStats"/>, plus an unsniffed local variable
    /// (no ParameterCompiledValue, no OPTION (RECOMPILE)) so PlanAnalyzer.Rule 20 also fires. Two findings,
    /// one scored and one not — enough to pin the ordering.</summary>
    private static string SerialPlanWithUnsniffedLocalVariable()
    {
        return
            "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
            "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
            "<BatchSequence><Batch><Statements>" +
            "<StmtSimple StatementText=\"SELECT * FROM dbo.t WHERE id = @id\" StatementId=\"1\" StatementType=\"SELECT\" " +
            "StatementSubTreeCost=\"100\" StatementEstRows=\"10\" StatementOptmLevel=\"FULL\">" +
            "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" " +
            "NonParallelPlanReason=\"CouldNotGenerateValidParallelPlan\" DegreeOfParallelism=\"1\">" +
            "<QueryTimeStats CpuTime=\"1000\" ElapsedTime=\"1000\" />" +
            "<ParameterList><ColumnReference Column=\"@id\" ParameterDataType=\"int\" /></ParameterList>" +
            "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
            "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
            "EstimatedTotalSubtreeCost=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" " +
            "EstimatedExecutionMode=\"Row\">" +
            "<OutputList />" +
            "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"10\" " +
            "ActualElapsedms=\"1000\" ActualCPUms=\"1000\" ActualLogicalReads=\"10\" /></RunTimeInformation>" +
            "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
            "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" " +
            "IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
            "</IndexScan>" +
            "</RelOp>" +
            "</QueryPlan></StmtSimple>" +
            "</Statements></Batch></BatchSequence></ShowPlanXML>";
    }
}
